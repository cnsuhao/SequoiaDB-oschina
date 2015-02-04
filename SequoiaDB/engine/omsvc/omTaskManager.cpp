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

   Source File Name = omTaskManager.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/12/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omTaskManager.hpp"
#include "omDef.hpp"
#include "rtn.hpp"
#include "msgMessage.hpp"
#include "ossVer.hpp"

using namespace bson ;

namespace engine
{
   static BOOLEAN isSubSet( const BSONObj &source, const BSONObj &find ) ;
   static BOOLEAN isInElement( const BSONObj &source, const string eleKey,
                               const BSONObj &find ) ;
   static INT32 queryOneTask( const BSONObj &selector, const BSONObj &matcher,
                              const BSONObj &orderBy, const BSONObj &hint , 
                              BSONObj &oneTask ) ;

   BOOLEAN isSubSet( const BSONObj &source, const BSONObj &find )
   {
      BSONObjIterator iter( find ) ;
      while ( iter.more() )
      {
         BSONElement ele       = iter.next() ;
         BSONElement sourceEle = source.getField( ele.fieldName() ) ;
         if ( ele.woCompare( sourceEle, false ) != 0 )
         {
            return FALSE ;
         }
      }

      return TRUE ;
   }

   BOOLEAN isInElement( const BSONObj &source, const string eleKey,
                        const BSONObj &find )
   {
      BSONElement eleSource = source.getField( eleKey ) ;
      if ( eleSource.type() != Array )
      {
         return FALSE ;
      }

      BSONObj obj = eleSource.embeddedObject() ;
      BSONObjIterator iter( obj ) ;
      while ( iter.more() )
      {
         BSONObj oneObj ;
         BSONElement ele = iter.next() ;
         if ( ele.type() != Object )
         {
            continue ;
         }

         oneObj = ele.embeddedObject() ;
         if ( isSubSet( oneObj, find ) )
         {
            return TRUE ;
         }
      }

      return FALSE ;
   }

   INT32 queryOneTask( const BSONObj &selector, const BSONObj &matcher,
                       const BSONObj &orderBy, const BSONObj &hint , 
                       BSONObj &oneTask ) 
   {
      INT32 rc = SDB_OK ;
      pmdEDUCB *cb       = pmdGetThreadEDUCB() ;
      pmdKRCB *pKRCB     = pmdGetKRCB() ;
      _SDB_DMSCB *pdmsCB = pKRCB->getDMSCB() ;
      _SDB_RTNCB *pRtnCB = pKRCB->getRTNCB() ;
      SINT64 contextID = -1 ;

      rc = rtnQuery( OM_CS_DEPLOY_CL_TASKINFO, selector, matcher, orderBy, 
                     hint, 0, cb, 0, 1, pdmsCB, pRtnCB, contextID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "query table failed:table=%s,rc=%d", 
                     OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
         goto error ;
      }

      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, cb, pRtnCB ) ;
         if ( rc )
         {
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         oneTask = result.copy() ;
         goto done ;
      }

   done:
      if ( -1 != contextID )
      {
         pRtnCB->contextDelete ( contextID, cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   omTaskBase::omTaskBase()
   {
   }

   omTaskBase::~omTaskBase()
   {
   }

   INT32 omTaskBase::checkUpdateInfo(const BSONObj & updateInfo)
   {
      INT32 rc = SDB_OK ;
      if ( !updateInfo.hasField( OM_TASKINFO_FIELD_RESULTINFO ) 
           || !updateInfo.hasField( OM_TASKINFO_FIELD_PROGRESS ) 
           || !updateInfo.hasField( OM_TASKINFO_FIELD_STATUS ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "updateinfo miss field:fields=[%s,%s,%s],updateInfo"
                 "=%s", OM_TASKINFO_FIELD_RESULTINFO, 
                 OM_TASKINFO_FIELD_PROGRESS, OM_TASKINFO_FIELD_STATUS, 
                 updateInfo.toString().c_str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   omAddHostTask::omAddHostTask( INT64 taskID )
   {
      _taskID   = taskID ;
      _taskType = OM_TASK_TYPE_ADD_HOST ;
   }

   omAddHostTask::~omAddHostTask()
   {

   }

   INT32 omAddHostTask::_getSuccessHost( BSONObj &resultInfo, 
                                         set<string> &successHostSet )
   {
      INT32 rc = SDB_OK ;
      BSONObj hosts ;
      hosts = resultInfo.getObjectField( OM_TASKINFO_FIELD_RESULTINFO ) ;
      BSONObjIterator iter( hosts ) ;
      while ( iter.more() )
      {
         BSONElement ele = iter.next() ;
         BSONObj tmpHost = ele.embeddedObject() ;
         string hostName = tmpHost.getStringField( OM_HOST_FIELD_NAME ) ;
         INT32 retCode   = tmpHost.getIntField( OM_REST_RES_RETCODE ) ;
         if ( SDB_OK == retCode )
         {
            successHostSet.insert( hostName ) ;
         }
      }

      return rc ;
   }

   INT32 omAddHostTask::finish( BSONObj &resultInfo )
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      string clusterName ;
      BSONObj taskInfoValue ;
      BSONObj hosts ;
      set<string> successHostSet ;

      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      BSONObj taskInfo ;

      rc = _getSuccessHost( resultInfo, successHostSet ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get success host failed:rc=%d", rc ) ;
         goto error ;
      }

      selector = BSON( OM_TASKINFO_FIELD_INFO << 1 ) ;
      matcher  = BSON( OM_TASKINFO_FIELD_TASKID << _taskID ) ;
      rc = queryOneTask( selector, matcher, orderBy, hint, taskInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get task info failed:taskID="OSS_LL_PRINT_FORMAT
                 ",rc=%d", _taskID, rc ) ;
         goto error ;
      }

      taskInfoValue = taskInfo.getObjectField( OM_TASKINFO_FIELD_INFO ) ;
      hosts = taskInfoValue.getObjectField( OM_BSON_FIELD_HOST_INFO ) ;
      SDB_ASSERT( !hosts.isEmpty(), "" ) ;
      {
         BSONObjIterator iter( hosts ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            BSONObj oneHost = ele.embeddedObject() ;
            string hostName = oneHost.getStringField( OM_HOST_FIELD_NAME ) ;
            if ( successHostSet.find( hostName ) == successHostSet.end() )
            {
               continue ;
            }

            rc = rtnInsert( OM_CS_DEPLOY_CL_HOST, oneHost, 1, 0, cb ) ;
            if ( rc )
            {
               if ( SDB_IXM_DUP_KEY != rc )
               {
                  PD_LOG( PDERROR, "insert into table failed:%s,rc=%d", 
                          OM_CS_DEPLOY_CL_HOST, rc ) ;
                  goto error ;
               }
            }
         }
      }

      clusterName = taskInfo.getStringField( OM_BSON_FIELD_CLUSTER_NAME ) ;
      sdbGetOMManager()->updateClusterVersion( clusterName ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostTask::getType()
   {
      return _taskType ;
   }

   INT64 omAddHostTask::getTaskID()
   {
      return _taskID ;
   }

   INT32 omAddHostTask::checkUpdateInfo( const BSONObj &updateInfo )
   {
      INT32 rc = SDB_OK ;
      BSONElement resultInfoEle ;
      BSONObj agentResultInfo ;

      BSONObj localTask ;
      BSONObj localResultInfo ;

      rc = omTaskBase::checkUpdateInfo( updateInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "omTaskBase::checkUpdateInfo failed:rc=%d", rc ) ;
         goto error ;
      }

      {
         BSONObj filterResult = BSON( OM_TASKINFO_FIELD_RESULTINFO << "" ) ;
         BSONObj selector ;
         BSONObj matcher = BSON( OM_TASKINFO_FIELD_TASKID << _taskID ) ;
         BSONObj orderBy ;
         BSONObj hint ;
         rc = queryOneTask( selector, matcher, orderBy, hint, localTask ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "query task failed:taskID="OSS_LL_PRINT_FORMAT
                    ",rc=%d", _taskID, rc ) ;
            goto error ;
         }

         localResultInfo = localTask.filterFieldsUndotted( filterResult, 
                                                           true ) ;
      }

      resultInfoEle = updateInfo.getField( OM_TASKINFO_FIELD_RESULTINFO ) ;
      if ( Array != resultInfoEle.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s is not Array type", 
                 OM_TASKINFO_FIELD_RESULTINFO ) ;
         goto error ;
      }
      agentResultInfo = resultInfoEle.embeddedObject() ;
      {
         BSONObj filter = BSON( OM_HOST_FIELD_NAME << "" ) ;
         BSONObjIterator iterResult( agentResultInfo ) ;
         while ( iterResult.more() )
         {
            BSONObj find ;
            BSONObj oneResult ;
            BSONElement ele = iterResult.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG( PDERROR, "%s's element is not Object type", 
                       OM_TASKINFO_FIELD_RESULTINFO ) ;
               goto error ;
            }

            oneResult = ele.embeddedObject() ;
            find      = oneResult.filterFieldsUndotted( filter, true ) ;
            if ( !isInElement( localResultInfo, OM_TASKINFO_FIELD_RESULTINFO, 
                               find ) )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG( PDERROR, "agent's result is not in localTask:"
                       "agentResult=%s", find.toString().c_str() ) ;
               goto error ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   omAddBusinessTask::omAddBusinessTask( INT64 taskID )
   {
      _taskID   = taskID ;
      _taskType = OM_TASK_TYPE_ADD_BUSINESS ;
   }

   omAddBusinessTask::~omAddBusinessTask()
   {

   }

   BOOLEAN omAddBusinessTask::_isHostConfExist( const string &hostName, 
                                                const string &businessName )
   {
      INT32 rc         = SDB_OK ;
      pmdEDUCB *cb     = pmdGetThreadEDUCB() ;
      BOOLEAN flag     = FALSE ;
      SINT64 contextID = -1 ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      pmdKRCB *pKRCB = pmdGetKRCB() ;

      matcher = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName 
                      << OM_CONFIGURE_FIELD_HOSTNAME << hostName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CONFIGURE, selector, matcher, orderBy, 
                     hint, 0, cb, 0, -1, pKRCB->getDMSCB(), pKRCB->getRTNCB(), 
                     contextID ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_CONFIGURE, rc ) ;
         goto done ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, cb, pKRCB->getRTNCB() ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               goto done ;
            }

            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
            goto done ;
         }

         flag = TRUE ;
         goto done ;
      }

   done:
      if ( -1 != contextID )
      {
         pKRCB->getRTNCB()->contextDelete ( contextID, cb ) ;
      }
      return flag ;
   }

   INT32 omAddBusinessTask::_appendConfigure( const string &hostName,
                                              const string &businessName,
                                              BSONObj &oneNode )
   {
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      INT32 rc     = SDB_OK ;
      BSONArrayBuilder arrayBuilder ;
      BSONObj filter  = BSON( OM_BSON_FIELD_HOST_NAME << "" 
                              << OM_BSON_FIELD_HOST_USER << "" 
                              << OM_BSON_FIELD_HOST_PASSWD << "" 
                              << OM_BSON_FIELD_HOST_SSHPORT << "" ) ;
      BSONObj oneConf = oneNode.filterFieldsUndotted( filter, false ) ;
      arrayBuilder.append( oneConf ) ;

      BSONObj selector = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName 
                               << OM_CONFIGURE_FIELD_HOSTNAME << hostName );
      BSONObj tmp      = BSON( OM_CONFIGURE_FIELD_CONFIG 
                               << arrayBuilder.arr() ) ;
      BSONObj updator  = BSON( "$addtoset" << tmp ) ;
      {
         BSONObj hint ;
         rc = rtnUpdate( OM_CS_DEPLOY_CL_CONFIGURE, selector, updator, hint,
                         0, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to update config for %s in %s:rc=%d", 
                    hostName.c_str(), OM_CS_DEPLOY_CL_CONFIGURE, rc ) ;
            goto error ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddBusinessTask::_insertConfigure( const string &hostName,
                                              const string &businessName ,
                                              BSONObj &oneNode )
   {
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      INT32 rc     = SDB_OK ;
      BSONArrayBuilder arrayBuilder ;
      BSONObj filter  = BSON( OM_BSON_FIELD_HOST_NAME << "" 
                              << OM_BSON_FIELD_HOST_USER << "" 
                              << OM_BSON_FIELD_HOST_PASSWD << ""
                              << OM_BSON_FIELD_HOST_SSHPORT << "" ) ;
      BSONObj oneConf = oneNode.filterFieldsUndotted( filter, false ) ;
      arrayBuilder.append( oneConf ) ;
      BSONObj obj = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName 
                          << OM_CONFIGURE_FIELD_HOSTNAME << hostName 
                          << OM_CONFIGURE_FIELD_CONFIG << arrayBuilder.arr() ) ;
      rc = rtnInsert( OM_CS_DEPLOY_CL_CONFIGURE, obj, 1, 0, cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to store config into table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_CONFIGURE, rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   void omAddBusinessTask::_updateHostOMVersion( const string &hostName )
   {
      
   }

   INT32 omAddBusinessTask::_storeBusinessInfo( BSONObj &taskInfoValue )
   {
      INT32 rc = SDB_OK ;
      string businessName ;
      string deployMod ;
      string businessType ;
      string clusterName ;
      BSONObj obj ;
      BSONObj configs ;
      pmdEDUCB *cb  = pmdGetThreadEDUCB() ;

      businessName  = taskInfoValue.getStringField( OM_BSON_BUSINESS_NAME );
      deployMod     = taskInfoValue.getStringField( OM_BSON_DEPLOY_MOD ) ;
      businessType  = taskInfoValue.getStringField( OM_BSON_BUSINESS_TYPE );
      clusterName   = taskInfoValue.getStringField( OM_BSON_FIELD_CLUSTER_NAME ) ;

      obj = BSON( OM_BUSINESS_FIELD_NAME << businessName 
                  << OM_BSON_BUSINESS_TYPE << businessType 
                  << OM_BSON_DEPLOY_MOD << deployMod
                  << OM_BSON_FIELD_CLUSTER_NAME << clusterName ) ;
      rc = rtnInsert( OM_CS_DEPLOY_CL_BUSINESS, obj, 1, 0, cb );
      if ( rc )
      {
         if ( SDB_IXM_DUP_KEY != rc )
         {
            PD_LOG_MSG( PDERROR, "failed to store business into table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_BUSINESS, rc ) ;
            goto error ;
         }

         rc = SDB_OK ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddBusinessTask::_storeConfigInfo( BSONObj &taskInfoValue )
   {
      string businessName ;
      BSONObj configs ;
      INT32 rc      = SDB_OK ;
      businessName  = taskInfoValue.getStringField( OM_BSON_BUSINESS_NAME ) ;
      configs       = taskInfoValue.getObjectField( OM_BSON_FIELD_CONFIG ) ;
      {
         BSONObjIterator iter( configs ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            BSONObj oneNode = ele.embeddedObject() ;
            string hostName ;
            hostName = oneNode.getStringField( OM_BSON_FIELD_HOST_NAME ) ;
            if ( _isHostConfExist( hostName, businessName ) )
            {
               rc = _appendConfigure( hostName, businessName, oneNode ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "append configure failed:host=%s,"
                          "business=%s, node=%s, rc=%d", 
                          hostName.c_str(), businessName.c_str(), 
                          oneNode.toString().c_str(), rc ) ;
                  goto error ;
               }
            }
            else
            {
               rc = _insertConfigure( hostName, businessName, oneNode ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "insert configure failed:host=%s,"
                          "business=%s, node=%s, rc=%d", 
                          hostName.c_str(), businessName.c_str(), 
                          oneNode.toString().c_str(), rc ) ;
                  goto error ;
               }
            }

            _updateHostOMVersion( hostName ) ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddBusinessTask::finish( BSONObj &resultInfo )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      BSONObj taskInfo ;
      BSONObj taskInfoValue ;

      selector = BSON( OM_TASKINFO_FIELD_INFO << 1 ) ;
      matcher  = BSON( OM_TASKINFO_FIELD_TASKID << _taskID ) ;
      rc = queryOneTask( selector, matcher, orderBy, hint, taskInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get task info failed:taskID="OSS_LL_PRINT_FORMAT
                 ",rc=%d", _taskID, rc ) ;
         goto error ;
      }

      taskInfoValue = taskInfo.getObjectField( OM_TASKINFO_FIELD_INFO ) ;

      rc = _storeBusinessInfo( taskInfoValue ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "store business info failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = _storeConfigInfo( taskInfoValue ) ;
      {
         PD_LOG( PDERROR, "store configure info failed:rc=%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddBusinessTask::getType()
   {
      return _taskType ;
   }

   INT64 omAddBusinessTask::getTaskID()
   {
      return _taskID ;
   }

   INT32 omAddBusinessTask::checkUpdateInfo( const BSONObj &updateInfo )
   {
      INT32 rc = SDB_OK ;
      BSONElement resultInfoEle ;
      BSONObj agentResultInfo ;

      BSONObj localTask ;
      BSONObj localResultInfo ;

      rc = omTaskBase::checkUpdateInfo( updateInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "omTaskBase::checkUpdateInfo failed:rc=%d", rc ) ;
         goto error ;
      }

      {
         BSONObj selector ;
         BSONObj matcher = BSON( OM_TASKINFO_FIELD_TASKID << _taskID ) ;
         BSONObj orderBy ;
         BSONObj hint ;
         rc = queryOneTask( selector, matcher, orderBy, hint, localTask ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "query task failed:taskID="OSS_LL_PRINT_FORMAT
                    ",rc=%d", _taskID, rc ) ;
            goto error ;
         }

         localResultInfo = localTask.getObjectField( 
                                              OM_TASKINFO_FIELD_RESULTINFO ) ;
      }

      resultInfoEle = updateInfo.getField( OM_TASKINFO_FIELD_RESULTINFO ) ;
      if ( Array != resultInfoEle.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s is not Array type", 
                 OM_TASKINFO_FIELD_RESULTINFO ) ;
         goto error ;
      }
      agentResultInfo = resultInfoEle.embeddedObject() ;
      {
         BSONObj filter = BSON( OM_BSON_FIELD_HOST_NAME << ""
                                << OM_CONF_DETAIL_SVCNAME << "" 
                                << OM_CONF_DETAIL_ROLE << "" 
                                << OM_CONF_DETAIL_DATAGROUPNAME << "" ) ;
         BSONObjIterator iterResult( agentResultInfo ) ;
         while ( iterResult.more() )
         {
            BSONObj find ;
            BSONObj oneResult ;
            BSONElement ele = iterResult.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG( PDERROR, "%s's element is not Object type", 
                       OM_TASKINFO_FIELD_RESULTINFO ) ;
               goto error ;
            }

            oneResult = ele.embeddedObject() ;
            find      = oneResult.filterFieldsUndotted( filter, true ) ;
            if ( !isInElement( localResultInfo, OM_TASKINFO_FIELD_RESULTINFO, 
                               find ) )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG( PDERROR, "agent's result is not in localTask:"
                       "agentResult=%s", find.toString().c_str() ) ;
               goto error ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   omRemoveBusinessTask::omRemoveBusinessTask( INT64 taskID )
   {
      _taskID   = taskID ;
      _taskType = OM_TASK_TYPE_REMOVE_BUSINESS;
   }

   omRemoveBusinessTask::~omRemoveBusinessTask()
   {

   }

   INT32 omRemoveBusinessTask::_removeBusinessInfo( BSONObj &taskInfoValue )
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = NULL ;
      string businessName ;
      BSONObj condition ;
      BSONObj hint ;
      businessName = taskInfoValue.getStringField( OM_BSON_BUSINESS_NAME ) ;

      cb           = pmdGetThreadEDUCB() ;
      condition    = BSON( OM_BUSINESS_FIELD_NAME << businessName ) ;

      rc = rtnDelete( OM_CS_DEPLOY_CL_BUSINESS, condition, hint, 0, cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to delete business from table:%s,"
                     "business=%s,rc=%d", OM_CS_DEPLOY_CL_BUSINESS, 
                     businessName.c_str(), rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveBusinessTask::_removeConfigInfo( BSONObj &taskInfoValue )
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = NULL ;
      string businessName ;
      BSONObj condition ;
      BSONObj hint ;
      businessName = taskInfoValue.getStringField( OM_BSON_BUSINESS_NAME ) ;
      cb           = pmdGetThreadEDUCB() ;
      condition    = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName ) ;

      rc = rtnDelete( OM_CS_DEPLOY_CL_CONFIGURE, condition, hint, 0, cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to delete configure from table:%s,"
                     "business=%s,rc=%d", OM_CS_DEPLOY_CL_CONFIGURE, 
                     businessName.c_str(), rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveBusinessTask::finish( BSONObj &resultInfo )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      BSONObj taskInfo ;
      BSONObj taskInfoValue ;

      selector = BSON( OM_TASKINFO_FIELD_INFO << 1 ) ;
      matcher  = BSON( OM_TASKINFO_FIELD_TASKID << _taskID ) ;
      rc = queryOneTask( selector, matcher, orderBy, hint, taskInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get task info failed:taskID="OSS_LL_PRINT_FORMAT
                 ",rc=%d", _taskID, rc ) ;
         goto error ;
      }

      taskInfoValue = taskInfo.getObjectField( OM_TASKINFO_FIELD_INFO ) ;

      rc = _removeBusinessInfo( taskInfoValue ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "store business info failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = _removeConfigInfo( taskInfoValue ) ;
      {
         PD_LOG( PDERROR, "store configure info failed:rc=%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveBusinessTask::getType()
   {
      return _taskType ;
   }

   INT64 omRemoveBusinessTask::getTaskID()
   {
      return _taskID ;
   }

   INT32 omRemoveBusinessTask::checkUpdateInfo( const BSONObj &updateInfo )
   {
      INT32 rc = SDB_OK ;
      BSONElement resultInfoEle ;
      BSONObj agentResultInfo ;

      BSONObj localTask ;
      BSONObj localResultInfo ;

      rc = omTaskBase::checkUpdateInfo( updateInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "omTaskBase::checkUpdateInfo failed:rc=%d", rc ) ;
         goto error ;
      }

      {
         BSONObj selector ;
         BSONObj matcher = BSON( OM_TASKINFO_FIELD_TASKID << _taskID ) ;
         BSONObj orderBy ;
         BSONObj hint ;
         rc = queryOneTask( selector, matcher, orderBy, hint, localTask ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "query task failed:taskID="OSS_LL_PRINT_FORMAT
                    ",rc=%d", _taskID, rc ) ;
            goto error ;
         }

         localResultInfo = localTask.getObjectField( 
                                              OM_TASKINFO_FIELD_RESULTINFO ) ;
      }

      resultInfoEle = updateInfo.getField( OM_TASKINFO_FIELD_RESULTINFO ) ;
      if ( Array != resultInfoEle.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s is not Array type", 
                 OM_TASKINFO_FIELD_RESULTINFO ) ;
         goto error ;
      }
      agentResultInfo = resultInfoEle.embeddedObject() ;
      {
         BSONObj filter = BSON( OM_BSON_FIELD_HOST_NAME << ""
                                << OM_CONF_DETAIL_SVCNAME << "" 
                                << OM_CONF_DETAIL_ROLE << "" 
                                << OM_CONF_DETAIL_DATAGROUPNAME << "" ) ;
         BSONObjIterator iterResult( agentResultInfo ) ;
         while ( iterResult.more() )
         {
            BSONObj find ;
            BSONObj oneResult ;
            BSONElement ele = iterResult.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG( PDERROR, "%s's element is not Object type", 
                       OM_TASKINFO_FIELD_RESULTINFO ) ;
               goto error ;
            }

            oneResult = ele.embeddedObject() ;
            find      = oneResult.filterFieldsUndotted( filter, true ) ;
            if ( !isInElement( localResultInfo, OM_TASKINFO_FIELD_RESULTINFO, 
                               find ) )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG( PDERROR, "agent's result is not in localTask:"
                       "agentResult=%s", find.toString().c_str() ) ;
               goto error ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   omTaskManager::omTaskManager()
   {
   }

   omTaskManager::~omTaskManager()
   {
   }

   INT32 omTaskManager::_updateTask( INT64 taskID, 
                                     const BSONObj &taskUpdateInfo )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      builder.appendElements( taskUpdateInfo ) ;
      time_t now = time( NULL ) ;
      builder.appendTimestamp( OM_TASKINFO_FIELD_END_TIME, now * 1000, 0 ) ;
      BSONObj selector = BSON( OM_TASKINFO_FIELD_TASKID << taskID ) ;
      BSONObj updator  = BSON( "$set" << builder.obj() ) ;
      BSONObj hint ;
      INT64 updateNum = 0 ;
      rc = rtnUpdate( OM_CS_DEPLOY_CL_TASKINFO, selector, updator, hint, 0, 
                      pmdGetThreadEDUCB(), &updateNum ) ;
      if ( SDB_OK != rc || 0 == updateNum )
      {
         PD_LOG( PDERROR, "update task failed:table=%s,updateNum=%d,taskID="
                 OSS_LL_PRINT_FORMAT",updator=%s,selector=%s,rc=%d",
                 OM_CS_DEPLOY_CL_TASKINFO, updateNum, taskID, 
                 updator.toString().c_str(), selector.toString().c_str(), rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::_getTaskType( INT64 taskID, INT32 &taskType )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      BSONObj task ;

      selector = BSON( OM_TASKINFO_FIELD_TYPE << 1 ) ;
      matcher  = BSON( OM_TASKINFO_FIELD_TASKID << taskID ) ;
      rc = queryOneTask( selector, matcher, orderBy, hint, task ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get task info faild:rc=%d", rc ) ;
         goto error ;
      }

      taskType = task.getIntField( OM_TASKINFO_FIELD_TYPE ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::queryOneTask( const BSONObj &selector, 
                                      const BSONObj &matcher,
                                      const BSONObj &orderBy,
                                      const BSONObj &hint, BSONObj &oneTask ) 
   {
      return engine::queryOneTask( selector, matcher, orderBy, hint, oneTask ) ;
   }

   INT32 omTaskManager::_getTaskFlag( INT64 taskID, BOOLEAN &existFlag, 
                                      BOOLEAN &isFinished, INT32 &taskType )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      BSONObj task ;
      INT32 status ;
      existFlag  = FALSE ;
      isFinished = FALSE ;
      taskType   = OM_TASK_TYPE_END ;

      matcher  = BSON( OM_TASKINFO_FIELD_TASKID << taskID ) ;
      rc = queryOneTask( selector, matcher, orderBy, hint, task ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_OK ;
            existFlag = FALSE ;
            goto done ;
         }

         PD_LOG( PDERROR, "get task info faild:rc=%d", rc ) ;
         goto error ;
      }

      existFlag = TRUE ;
      status = task.getIntField( OM_TASKINFO_FIELD_STATUS ) ;
      if ( OM_TASK_STATUS_FINISH == status )
      {
         isFinished = TRUE ;
      }

      taskType = task.getIntField( OM_TASKINFO_FIELD_TYPE ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::updateTask( INT64 taskID, 
                                    const BSONObj &taskUpdateInfo )
   {
      INT32 rc = SDB_OK ;
      INT32 status ;
      INT32 errFlag ;
      BOOLEAN isExist   = FALSE ;
      BOOLEAN isFinish  = FALSE ;
      INT32 taskType    = OM_TASK_TYPE_END ;
      omTaskBase *pTask = NULL ;

      rc = _getTaskFlag( taskID, isExist, isFinish, taskType ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get task flag failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( !isExist || isFinish )
      {
         rc = SDB_OM_TASK_NOT_EXIST ;
         PD_LOG( PDERROR, "task status error:taskID="OSS_LL_PRINT_FORMAT
                 ",isExist[%d],isFinish[%d]",  taskID, isExist, isFinish ) ;
         goto error ;
      }

      switch ( taskType )
      {
      case OM_TASK_TYPE_ADD_HOST :
         pTask = SDB_OSS_NEW omAddHostTask( taskID ) ;
         break ;

      case OM_TASK_TYPE_ADD_BUSINESS :
         pTask = SDB_OSS_NEW omAddBusinessTask( taskID ) ;
         break ;

      case OM_TASK_TYPE_REMOVE_BUSINESS :
         pTask = SDB_OSS_NEW omRemoveBusinessTask( taskID ) ;
         break ;

      default :
         PD_LOG( PDERROR, "unknown task type:taskID="OSS_LL_PRINT_FORMAT
                 ",taskType=%d", taskID, taskType ) ;
         SDB_ASSERT( FALSE, "unknown task type" ) ;
      }

      rc = pTask->checkUpdateInfo( taskUpdateInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "check update info failed:updateInfo=%s,rc=%d", 
                 taskUpdateInfo.toString().c_str(), rc ) ;
         goto error ;
      }

      status = taskUpdateInfo.getIntField( OM_TASKINFO_FIELD_STATUS ) ;
      if ( OM_TASK_STATUS_FINISH == status )
      {
         isFinish = TRUE ;
      }
      else
      {
         isFinish = FALSE ;
      }

      errFlag = taskUpdateInfo.getIntField( OM_TASKINFO_FIELD_ERRNO ) ;
      if ( SDB_OK == errFlag )
      {
         if ( isFinish )
         {
            BSONObj resultInfo ;
            BSONObj tmpFilter = BSON( OM_TASKINFO_FIELD_RESULTINFO << 1 ) ;
            resultInfo = taskUpdateInfo.filterFieldsUndotted( tmpFilter, 
                                                              true ) ;
            rc = pTask->finish( resultInfo ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "finish task failed:taskID="OSS_LL_PRINT_FORMAT
                       ",rc=%d", pTask->getTaskID(), rc ) ;
               goto error ;
            }
         }
      }

      rc = _updateTask( taskID, taskUpdateInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "update task failed:taskID="OSS_LL_PRINT_FORMAT
                 ",rc=%d", taskID, rc ) ;
         goto error ;
      }

   done:
      if ( NULL != pTask )
      {
         SDB_OSS_DEL pTask ;
         pTask = NULL ;
      }
      return rc ;
   error:
      goto done ;
   }
}

