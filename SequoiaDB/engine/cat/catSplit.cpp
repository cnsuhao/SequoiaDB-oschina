/*******************************************************************************


   Copyright (C) 2011-2014 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

   Source File Name = catSplit.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          19/07/2013  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "catSplit.hpp"
#include "catCommon.hpp"
#include "pdTrace.hpp"
#include "catTrace.hpp"
#include "rtn.hpp"

using namespace bson ;

namespace engine
{

   static BOOLEAN isGroupInCataSet( INT32 groupID, clsCatalogSet &cataSet )
   {
      BOOLEAN findGroup = FALSE ;

      VEC_GROUP_ID vecGroups ;
      cataSet.getAllGroupID( vecGroups ) ;
      VEC_GROUP_ID::iterator itVec = vecGroups.begin() ;
      while ( itVec != vecGroups.end() )
      {
         if ( *itVec == (UINT32)groupID )
         {
            findGroup = TRUE ;
            break ;
         }
         ++itVec ;
      }

      return findGroup ;
   }

   static INT32 _checkRangeSplitKey( const BSONObj &splitKey,
                                     clsCatalogSet &cataSet,
                                     BOOLEAN allowEmpty )
   {
      INT32 rc = SDB_OK ;

      if ( splitKey.nFields() != cataSet.getShardingKey().nFields() )
      {
         PD_LOG( PDWARNING, "Split key fields not match sharding key fields" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   static INT32 _checkHashSplitKey( const BSONObj &splitKey,
                                    clsCatalogSet &cataSet,
                                    BOOLEAN allowEmpty )
   {
      INT32 rc = SDB_OK ;

      if ( 1 != splitKey.nFields() ||
           NumberInt != splitKey.firstElement().type() )
      {
         PD_LOG( PDWARNING, "Split key field not 1 or field type not Int, "
                 "split key: %s", splitKey.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   static INT32 _checkSplitKey( const BSONObj &splitKey,
                                clsCatalogSet &cataSet,
                                INT32 groupID,
                                BOOLEAN allowEmpty,
                                BOOLEAN allowOnBound )
   {
      INT32 rc = SDB_OK ;

      if ( splitKey.isEmpty() && allowEmpty )
      {
         goto done ;
      }

      if ( cataSet.isRangeSharding() )
      {
         rc = _checkRangeSplitKey( splitKey, cataSet, allowEmpty ) ;
      }
      else
      {
         rc = _checkHashSplitKey( splitKey, cataSet, allowEmpty ) ;
      }

      if ( rc )
      {
         goto error ;
      }

      if ( !cataSet.isKeyInGroup( splitKey, (UINT32)groupID ) )
      {
         rc = SDB_CLS_BAD_SPLIT_KEY ;

         if ( allowOnBound &&
              cataSet.isKeyOnBoundary( splitKey, (UINT32*)&groupID ) )
         {
            rc = SDB_OK ;
         }

         if ( rc )
         {
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      PD_LOG( PDWARNING, "Split key: %s, catalog info: %s, group id: %d",
              splitKey.toString().c_str(),
              cataSet.toCataInfoBson().toString().c_str(),
              groupID ) ;
      goto done ;
   }

   static INT32 _checkDstGroupInCSDomain( const CHAR * groupName,
                                          const CHAR * clFullName,
                                          BOOLEAN & existed,
                                          pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      CHAR szSpace [ DMS_COLLECTION_SPACE_NAME_SZ + 1 ]  = {0} ;
      CHAR szCollection [ DMS_COLLECTION_NAME_SZ + 1 ] = {0} ;

      existed = FALSE ;

      BSONObj csObj ;
      BOOLEAN csExist = FALSE ;

      const CHAR *domainName = NULL ;

      rc = catResolveCollectionName( clFullName, ossStrlen( clFullName ),
                                     szSpace, DMS_COLLECTION_SPACE_NAME_SZ,
                                     szCollection, DMS_COLLECTION_NAME_SZ ) ;
      PD_RC_CHECK( rc, PDERROR, "Resolve collection name[%s] failed, rc: %d",
                   clFullName, rc ) ;

      rc = catCheckSpaceExist( szSpace, csExist, csObj, cb ) ;
      PD_RC_CHECK( rc, PDWARNING, "Check collection space[%s] exist failed, "
                   "rc: %d", szSpace, rc ) ;
      PD_CHECK( csExist, SDB_DMS_CS_NOTEXIST, error, PDWARNING,
                "Collection space[%s] is not exist", szSpace ) ;

      rc = rtnGetStringElement( csObj, CAT_DOMAIN_NAME, &domainName ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         existed = TRUE ;
         rc = SDB_OK ;
         goto done ;
      }
      else if ( SDB_OK == rc )
      {
         BSONObj domainObj ;
         map<string, INT32> groups ;
         rc = catGetDomainObj( domainName, domainObj, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Get domain[%s] failed, rc: %d",
                      domainName, rc ) ;

         rc = catGetDomainGroups( domainObj,  groups ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get groups from domain info[%s], "
                      "rc: %d", domainObj.toString().c_str(), rc ) ;

         if ( groups.find( groupName ) != groups.end() )
         {
            existed = TRUE ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catSplitPrepare( const BSONObj &splitInfo, const CHAR *clFullName,
                          clsCatalogSet *cataSet, INT32 &groupID,
                          pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;

      BSONObj splitQuery ;
      BOOLEAN existQuery = TRUE ;
      FLOAT64 percent = 0.0 ;
      const CHAR* sourceName = NULL ;
      const CHAR* dstName = NULL ;
      groupID = CAT_INVALID_GROUPID ;
      BOOLEAN dstInCSDomain = FALSE ;

      rc = rtnGetObjElement( splitInfo, CAT_SPLITQUERY_NAME, splitQuery ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         existQuery = FALSE ;
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc , PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_SPLITQUERY_NAME, rc ) ;

      percent = splitInfo.getField( CAT_SPLITPERCENT_NAME ).numberDouble() ;

      rc = rtnGetStringElement( splitInfo, CAT_SOURCE_NAME, &sourceName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_SOURCE_NAME, rc ) ;
      rc = catGroupNameValidate( sourceName, FALSE ) ;
      PD_RC_CHECK( rc, PDERROR, "Source group name[%s] is not valid, rc: %d",
                   sourceName, rc ) ;

      rc = rtnGetStringElement( splitInfo, CAT_TARGET_NAME, &dstName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_TARGET_NAME, rc ) ;
      rc = catGroupNameValidate( dstName, FALSE ) ;
      PD_RC_CHECK( rc, PDERROR, "Target group name[%s] is not valid, rc: %d",
                   dstName, rc ) ;

      PD_CHECK( 0 != ossStrcmp( dstName, sourceName ), SDB_INVALIDARG, error,
                PDERROR, "Target group name can not the same with source group"
                " name" ) ;

      rc = _checkDstGroupInCSDomain( dstName, clFullName, dstInCSDomain, cb ) ;
      PD_RC_CHECK( rc, PDWARNING, "Check destination group in collection space"
                   "domain failed, rc: %d", rc ) ;
      PD_CHECK( dstInCSDomain, SDB_CAT_GROUP_NOT_IN_DOMAIN, error, PDWARNING,
                "Split target group[%s] is not in collection space domain",
                dstName ) ;

      if ( FALSE == existQuery )
      {
         PD_CHECK( percent > 0.0 && percent <= 100.0, SDB_INVALIDARG, error,
                   PDERROR, "Split percent value[%f] error", percent ) ;
      }

      rc = catGroupName2ID( sourceName, groupID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Group name[%s] to id failed, rc: %d",
                   sourceName, rc ) ;

      PD_CHECK( isGroupInCataSet( groupID, *cataSet ), SDB_CL_NOT_EXIST_ON_GROUP,
                error, PDWARNING, "The collection[%s] does not exist on source "
                "group[%s]", clFullName, sourceName ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catSplitReady( const BSONObj & splitInfo, const CHAR * clFullName,
                        clsCatalogSet * cataSet, INT32 & groupID,
                        clsTaskMgr &taskMgr, pmdEDUCB * cb, INT16 w,
                        UINT64 *pTaskID )
   {
      INT32 rc = SDB_OK ;

      BSONObj bKey ;
      BSONObj eKey ;
      BOOLEAN usePercent = FALSE ;
      FLOAT64 percent = 0.0 ;
      const CHAR* sourceName = NULL ;
      const CHAR* dstName = NULL ;
      groupID = CAT_INVALID_GROUPID ;
      INT32 sourceID = CAT_INVALID_GROUPID ;
      BOOLEAN dstInCSDomain = FALSE ;

      rc = rtnGetObjElement( splitInfo, CAT_SPLITVALUE_NAME, bKey ) ;
      if ( SDB_FIELD_NOT_EXIST == rc ||
           ( SDB_OK == rc && bKey.isEmpty() ) )
      {
         usePercent = TRUE ;
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_SPLITVALUE_NAME, rc ) ;

      percent = splitInfo.getField( CAT_SPLITPERCENT_NAME ).numberDouble() ;

      if ( !usePercent )
      {
         rc = rtnGetObjElement( splitInfo, CAT_SPLITENDVALUE_NAME, eKey ) ;
         if ( SDB_FIELD_NOT_EXIST == rc )
         {
            rc = SDB_OK ;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                      CAT_SPLITENDVALUE_NAME, rc ) ;
      }
      else
      {
         PD_CHECK( percent > 0.0 && percent <= 100.0, SDB_INVALIDARG, error,
                   PDERROR, "Split percent value[%f] error", percent ) ;
         PD_CHECK( cataSet->isHashSharding(), SDB_SYS, error,
                   PDERROR, "Split by percent must be hash sharding" ) ;
      }

      rc = rtnGetStringElement( splitInfo, CAT_SOURCE_NAME, &sourceName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_SOURCE_NAME, rc ) ;
      rc = catGroupNameValidate( sourceName, FALSE ) ;
      PD_RC_CHECK( rc, PDERROR, "Source group name[%s] is not valid, rc: %d",
                   sourceName, rc ) ;

      rc = rtnGetStringElement( splitInfo, CAT_TARGET_NAME, &dstName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_TARGET_NAME, rc ) ;
      rc = catGroupNameValidate( dstName, FALSE ) ;
      PD_RC_CHECK( rc, PDERROR, "Target group name[%s] is not valid, rc: %d",
                   dstName, rc ) ;

      PD_CHECK( 0 != ossStrcmp( dstName, sourceName ), SDB_INVALIDARG, error,
                PDERROR, "Target group name can not the same with source group"
                " name" ) ;

      rc = _checkDstGroupInCSDomain( dstName, clFullName, dstInCSDomain, cb ) ;
      PD_RC_CHECK( rc, PDWARNING, "Check destination group in collection space"
                   "domain failed, rc: %d", rc ) ;
      PD_CHECK( dstInCSDomain, SDB_CAT_GROUP_NOT_IN_DOMAIN, error, PDWARNING,
                "Split target group[%s] is not in collection space domain",
                dstName ) ;

      rc = catGroupName2ID( sourceName, sourceID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Group name[%s] to id failed, rc: %d",
                   sourceName, rc ) ;

      PD_CHECK( isGroupInCataSet( sourceID, *cataSet ),
                SDB_CL_NOT_EXIST_ON_GROUP,
                error, PDWARNING, "The collection[%s] does not exist on source "
                "group[%s]", clFullName, sourceName ) ;

      rc = catGroupName2ID( dstName, groupID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Group name[%s] to id failed, rc: %d",
                   dstName, rc ) ;

      if ( !usePercent  )
      {
         rc = _checkSplitKey( bKey, *cataSet, sourceID, FALSE, FALSE ) ;
         PD_RC_CHECK( rc, PDERROR, "Check split key failed, rc: %d", rc ) ;

         rc = _checkSplitKey( eKey, *cataSet, sourceID, TRUE, TRUE ) ;
         PD_RC_CHECK( rc, PDERROR, "Check split end key failed, rc: %d", rc ) ;
      }

      {
         BSONObj match ;
         BSONObj taskObj ;
         BOOLEAN conflict = FALSE ;
         clsSplitTask splitTask( taskMgr.getTaskID() ) ;
         if ( usePercent )
         {
            rc = splitTask.calcHashPartition( *cataSet, sourceID, percent,
                                              bKey, eKey ) ;
            PD_RC_CHECK( rc, PDERROR, "Calc hash percent partition split key "
                         "falied, rc: %d", rc ) ;
         }
         rc = splitTask.init( clFullName, sourceID, sourceName, groupID,
                              dstName, bKey, eKey, percent, *cataSet ) ;
         PD_RC_CHECK( rc, PDERROR, "Init split task failed, rc: %d", rc ) ;

         match = splitTask.toBson( CLS_SPLIT_MASK_CLNAME ) ;
         rc = catSplitCheckConflict( match, splitTask, conflict, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Check task conflict failed, rc: %d", rc ) ;
         PD_CHECK( FALSE == conflict, SDB_CLS_MUTEX_TASK_EXIST, error, PDERROR,
                   "Exist task not compatible with the new task" ) ;

         taskObj = splitTask.toBson( CLS_MASK_ALL ) ;
         rc = catAddTask( taskObj, cb, w ) ;
         PD_RC_CHECK( rc, PDERROR, "Add split task failed, rc: %d", rc ) ;

         if ( pTaskID )
         {
            *pTaskID = splitTask.taskID() ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   static INT32 _checkSplitTaskConflict( rtnObjBuff *pBuffObj,
                                         BOOLEAN &conflict,
                                         clsSplitTask *pTask )
   {
      INT32 rc = 0 ;
      BSONObj taskObj ;

      while ( TRUE )
      {
         rc = pBuffObj->nextObj( taskObj ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
            }
            break ;
         }
         clsSplitTask tmpTask( CLS_INVALID_TASKID ) ;
         rc = tmpTask.init( taskObj.objdata() ) ;
         PD_RC_CHECK( rc, PDWARNING, "Init split task failed, rc: %d, obj: "
                      "%s", rc, taskObj.toString().c_str() ) ;
         if ( pTask->taskID() == tmpTask.taskID() ||
              pTask->muteXOn( &tmpTask ) || tmpTask.muteXOn( pTask ) )
         {
            conflict = TRUE ;
            PD_LOG( PDWARNING, "Exist task[%s] conflict with current "
                    "task[%s]", tmpTask.toBson().toString().c_str(),
                    pTask->toBson().toString().c_str() ) ;
            break ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catSplitCheckConflict( BSONObj & match, clsSplitTask & splitTask,
                                BOOLEAN & conflict, pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
      SDB_RTNCB *rtnCB = krcb->getRTNCB() ;
      BSONObj dummyObj ;
      INT64 contextID = -1 ;

      rtnContextBuf buffObj ;

      rc = rtnQuery( CAT_TASK_INFO_COLLECTION, dummyObj, match, dummyObj,
                     dummyObj, 0, cb, 0, -1, dmsCB, rtnCB, contextID ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to perform query, rc = %d", rc ) ;

      while ( SDB_OK == rc )
      {
         rc = rtnGetMore( contextID, -1, buffObj, cb, rtnCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               contextID = -1 ;
               rc = SDB_OK ;
               break ;
            }
            PD_LOG( PDERROR, "Failed to retreive record, rc = %d", rc ) ;
            goto error ;
         }

         rc = _checkSplitTaskConflict( &buffObj, conflict, &splitTask ) ;
         PD_RC_CHECK( rc, PDERROR, "Check split task conflict error, rc: %d",
                      rc ) ;

         if ( conflict )
         {
            break ;
         }
      }

   done:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
      }
      return rc ;
      return rc ;
   error:
      goto done ;
   }

   INT32 catSplitStart( const BSONObj & splitInfo, pmdEDUCB * cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      UINT64 taskID = 0 ;
      INT32 status = CLS_TASK_STATUS_READY ;

      BSONElement ele = splitInfo.getField( CAT_TASKID_NAME ) ;
      PD_CHECK( ele.isNumber(), SDB_INVALIDARG, error, PDERROR,
                "Failed to get field[%s], type: %d", CAT_TASKID_NAME,
                ele.type() ) ;
      taskID = ( UINT64 )ele.numberLong() ;

      rc = catGetTaskStatus( taskID, status, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( CLS_TASK_STATUS_READY == status ||
           CLS_TASK_STATUS_PAUSE == status )
      {
         rc = catUpdateTaskStatus( taskID, CLS_TASK_STATUS_RUN, cb, w ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( CLS_TASK_STATUS_CANCELED == status )
      {
         rc = SDB_TASK_HAS_CANCELED ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catSplitChgMeta( const BSONObj & splitInfo, const CHAR * clFullName,
                          clsCatalogSet * cataSet, pmdEDUCB * cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = krcb->getDPSCB() ;
      UINT64 taskID = 0 ;
      BSONObj taskObj ;
      BSONObj cataInfo ;
      clsSplitTask splitTask( CLS_INVALID_TASKID ) ;

      BSONElement ele = splitInfo.getField( CAT_TASKID_NAME ) ;
      PD_CHECK( ele.isNumber(), SDB_INVALIDARG, error, PDERROR,
                "Failed to get field[%s], type: %d", CAT_TASKID_NAME,
                ele.type() ) ;
      taskID = ( UINT64 )ele.numberLong() ;

      rc = catGetTask( taskID, taskObj, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Get task[%lld] failed, rc: %d", taskID, rc ) ;

      rc = splitTask.init( taskObj.objdata() ) ;
      PD_RC_CHECK( rc, PDERROR, "Init task failed, rc: %d, obj: %s",
                   rc, taskObj.toString().c_str() ) ;

      if ( CLS_TASK_STATUS_META == splitTask.status() ||
           CLS_TASK_STATUS_FINISH == splitTask.status() )
      {
         goto done ;
      }
      else if ( CLS_TASK_STATUS_CANCELED == splitTask.status() )
      {
         rc = SDB_TASK_HAS_CANCELED ;
         goto error ;
      }
      else if ( CLS_TASK_STATUS_RUN == splitTask.status() &&
                !cataSet->isKeyInGroup( splitTask.splitKeyObj(),
                                        splitTask.dstID() ) )
      {
         rc = _checkSplitKey( splitTask.splitKeyObj(), *cataSet,
                              splitTask.sourceID(), FALSE, FALSE ) ;
         PD_RC_CHECK( rc, PDSEVERE, "Check split key failed, rc: %d, there's "
                      "possible data corruption, obj: %s", rc,
                      taskObj.toString().c_str() ) ;

         rc = _checkSplitKey( splitTask.splitEndKeyObj(), *cataSet,
                              splitTask.sourceID(), TRUE, TRUE ) ;
         PD_RC_CHECK( rc, PDSEVERE, "Check split end key failed, rc: %d, "
                      "there's possible data corruption, obj: %s", rc,
                      taskObj.toString().c_str() ) ;


         rc = cataSet->split( splitTask.splitKeyObj(),
                              splitTask.splitEndKeyObj(),
                              splitTask.dstID(), splitTask.dstName() ) ;
         PD_RC_CHECK( rc, PDERROR, "Catalog split failed, rc: %d, catalog: %s, "
                      "task obj: %s", rc ,
                      cataSet->toCataInfoBson().toString().c_str(),
                      taskObj.toString().c_str() ) ;

         cataInfo = cataSet->toCataInfoBson() ;
         rc = catUpdateCatalog( clFullName, cataInfo, cb, w ) ;
         PD_RC_CHECK( rc, PDSEVERE, "Failed to update collection catalog, "
                      "rc: %d", rc ) ;

         {
            std::string strMainCLName = cataSet->getMainCLName();
            if ( !strMainCLName.empty() )
            {
               BSONObj emptyObj;
               rc = catUpdateCatalog( strMainCLName.c_str(), emptyObj, cb, w );
            }
         }
      }
      else
      {
         PD_CHECK( CLS_TASK_STATUS_RUN == splitTask.status(), SDB_SYS, error,
                   PDERROR, "Split task status error, task: %s",
                   taskObj.toString().c_str() ) ;
      }

      rc = catUpdateTaskStatus( taskID, CLS_TASK_STATUS_META, cb, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to update task status, rc: %d", rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catSplitCleanup( const BSONObj & splitInfo, pmdEDUCB * cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      UINT64 taskID = 0 ;
      INT32 status = CLS_TASK_STATUS_READY ;

      BSONElement ele = splitInfo.getField( CAT_TASKID_NAME ) ;
      PD_CHECK( ele.isNumber(), SDB_INVALIDARG, error, PDERROR,
                "Failed to get field[%s], type: %d", CAT_TASKID_NAME,
                ele.type() ) ;
      taskID = ( UINT64 )ele.numberLong() ;

      rc = catGetTaskStatus( taskID, status, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( CLS_TASK_STATUS_META == status )
      {
         rc = catUpdateTaskStatus( taskID, CLS_TASK_STATUS_FINISH, cb, w ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( CLS_TASK_STATUS_FINISH == status )
      {
      }
      else
      {
         PD_LOG( PDERROR, "Task[%ll] status error in clean up step",
                 taskID, status ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catSplitFinish( const BSONObj & splitInfo, pmdEDUCB * cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      UINT64 taskID = 0 ;

      BSONElement ele = splitInfo.getField( CAT_TASKID_NAME ) ;
      PD_CHECK( ele.isNumber(), SDB_INVALIDARG, error, PDERROR,
                "Failed to get field[%s], type: %d", CAT_TASKID_NAME,
                ele.type() ) ;
      taskID = ( UINT64 )ele.numberLong() ;

      rc = catRemoveTask( taskID, cb, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Remove task[%lld] failed, rc: %d",
                   taskID, rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catSplitCancel( const BSONObj & splitInfo, pmdEDUCB * cb,
                         INT32 &groupID, INT16 w )
   {
      INT32 rc = SDB_OK ;
      UINT64 taskID = 0 ;

      BSONElement ele = splitInfo.getField( CAT_TASKID_NAME ) ;


      if ( !ele.eoo() )
      {
         INT32 status = CLS_TASK_STATUS_READY ;
         BSONObj taskObj ;

         PD_CHECK( ele.isNumber(), SDB_INVALIDARG, error, PDERROR,
                   "Failed to get field[%s], type: %d", CAT_TASKID_NAME,
                   ele.type() ) ;
         taskID = ( UINT64 )ele.numberLong() ;

         rc = catGetTask( taskID, taskObj, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get task[%lld], rc: %d",
                      taskID, rc ) ;

         rc = rtnGetIntElement( taskObj, CAT_STATUS_NAME, status ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                      CAT_STATUS_NAME, rc ) ;

         rc = rtnGetIntElement( taskObj, CAT_TARGETID_NAME, groupID ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                      CAT_TARGETID_NAME, rc ) ;

         if ( CLS_TASK_STATUS_META == status ||
              CLS_TASK_STATUS_FINISH == status )
         {
            rc = SDB_TASK_ALREADY_FINISHED ;
            goto error ;
         }
         else if ( CLS_TASK_STATUS_READY == status )
         {
            rc = catRemoveTask( taskID, cb ,w ) ;
            PD_RC_CHECK( rc, PDERROR, "Remove task[%lld] failed, rc: %d",
                         taskID, rc ) ;
         }
         else if ( CLS_TASK_STATUS_CANCELED != status )
         {
            rc = catUpdateTaskStatus( taskID, CLS_TASK_STATUS_CANCELED,
                                      cb, w ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to update data task[%lld] to "
                         "canceled, rc: %d", taskID, rc ) ;
         }
      }
      else
      {
         BSONObjBuilder matchBuilder ;
         matchBuilder.append( CAT_TASKTYPE_NAME, CLS_TASK_SPLIT ) ;
         matchBuilder.append( splitInfo.getField( CAT_COLLECTION_NAME ) ) ;
         matchBuilder.append( splitInfo.getField( CAT_SOURCE_NAME ) ) ;
         matchBuilder.append( splitInfo.getField( CAT_TARGET_NAME ) ) ;

         BSONElement splitKeyEle = splitInfo.getField( CAT_SPLITVALUE_NAME ) ;
         if ( splitKeyEle.eoo() ||
              splitKeyEle.embeddedObject().isEmpty() )
         {
            matchBuilder.append( splitInfo.getField( CAT_SPLITPERCENT_NAME ) ) ;
         }
         else
         {
            matchBuilder.append( splitKeyEle ) ;
         }

         BSONObj match = matchBuilder.obj() ;
         rc = catRemoveTask( match, cb, w ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove task[%s] failed, rc: %d",
                      splitInfo.toString().c_str(), rc ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

}


