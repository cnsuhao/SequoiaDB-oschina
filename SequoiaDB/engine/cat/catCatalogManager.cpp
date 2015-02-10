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

   Source File Name = catCatalogManager.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "core.hpp"
#include "pmdCB.hpp"
#include "pd.hpp"
#include "rtn.hpp"
#include "catDef.hpp"
#include "catCatalogManager.hpp"
#include "rtnPredicate.hpp"
#include "msgMessage.hpp"
#include "ixmIndexKey.hpp"
#include "pdTrace.hpp"
#include "catTrace.hpp"
#include "catCommon.hpp"
#include "clsCatalogAgent.hpp"

using namespace bson;

namespace engine
{

   /*
      catCatalogueManager implement
   */
   catCatalogueManager::catCatalogueManager()
   {
      _pEduCB     = NULL ;
      _pDpsCB     = NULL ;
      _pCatCB     = NULL ;
      _pDmsCB     = NULL ;
   }

   INT32 catCatalogueManager::active()
   {
      _taskMgr.setTaskID( catGetMaxTaskID( _pEduCB ) ) ;
      return SDB_OK ;
   }

   INT32 catCatalogueManager::deactive()
   {
      return SDB_OK ;
   }

   INT32 catCatalogueManager::init()
   {
      pmdKRCB *krcb  = pmdGetKRCB();
      _pDmsCB        = krcb->getDMSCB();
      _pDpsCB        = krcb->getDPSCB();
      _pCatCB        = krcb->getCATLOGUECB();
      return SDB_OK ;
   }

   void catCatalogueManager::attachCB( pmdEDUCB * cb )
   {
      _pEduCB = cb ;
   }

   void catCatalogueManager::detachCB( pmdEDUCB * cb )
   {
      _pEduCB = NULL ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_DROPCS, "catCatalogueManager::processCmdDropCollectionSpace" )
   INT32 catCatalogueManager::processCmdDropCollectionSpace( const CHAR *pQuery )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_DROPCS ) ;

      try
      {
         BSONObj boQuery( pQuery ) ;
         BSONElement beSpaceName =
            boQuery.getField ( CAT_COLLECTION_SPACE_NAME ) ;
         PD_CHECK ( beSpaceName.type() == String, SDB_INVALIDARG, error,
                    PDERROR, "Field[%s] type[%d] is not String",
                    CAT_COLLECTION_SPACE_NAME, beSpaceName.type() ) ;
         PD_TRACE1 ( SDB_CATALOGMGR_DROPCS,
                     PD_PACK_STRING ( beSpaceName.valuestr() ) ) ;
         rc = catRemoveCSEx( beSpaceName.valuestr(), _pEduCB, _pDmsCB, _pDpsCB,
                             _majoritySize() ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to drop collection space %s, "
                       "rc = %d", beSpaceName.valuestr(), rc ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_DROPCS, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_CRT_PROCEDURES, "catCatalogueManager::processCmdCrtProcedures")
   INT32 catCatalogueManager::processCmdCrtProcedures( void *pMsg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATALOGMGR_CRT_PROCEDURES ) ;
      try
      {
         BSONObj func( (const CHAR *)pMsg ) ;
         BSONObj parsed ;
         rc = catPraseFunc( func, parsed ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to parse store procedures:%s",
                    func.toString().c_str() ) ;
            goto error ;
         }

         rc = rtnInsert( CAT_PROCEDURES_COLLECTION,
                         parsed, 1, 0,
                         _pEduCB, _pDmsCB, _pDpsCB, _majoritySize() ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to add func:%s",
                    parsed.toString().c_str() ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s",e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_CATALOGMGR_CRT_PROCEDURES, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_RM_PROCEDURES, "catCatalogueManager::processCmdRmProcedures")
   INT32 catCatalogueManager::processCmdRmProcedures( void *pMsg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATALOGMGR_RM_PROCEDURES ) ;
      try
      {
         BSONObj obj( (const CHAR *)pMsg ) ;
         BSONElement name = obj.getField( FIELD_NAME_FUNC ) ;
         if ( name.eoo() || String != name.type() )
         {
            PD_LOG( PDERROR, "invalid type of func name[%s:%d]",
                    name.toString().c_str(), name.type()  ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         {
         BSONObjBuilder builder ;
         BSONObj deletor ;
         BSONObj dummy ;
         BSONObj func ;
         builder.appendAs( name, FMP_FUNC_NAME ) ;
         deletor = builder.obj() ;

         rc = catGetOneObj( CAT_PROCEDURES_COLLECTION,
                            dummy, deletor, dummy,
                            _pEduCB, func ) ;
         if ( SDB_DMS_EOC == rc )
         {
            PD_LOG( PDERROR, "func %s is not exist",
                    deletor.toString().c_str() ) ;
            rc = SDB_FMP_FUNC_NOT_EXIST ;
            goto error ;
         }
         else if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get func:%s, rc=%d",
                    deletor.toString().c_str(), rc ) ; 
            goto error ;
         }

         rc = rtnDelete( CAT_PROCEDURES_COLLECTION, 
                         deletor, BSONObj(),
                         0, _pEduCB, _pDmsCB, _pDpsCB, _majoritySize() ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to rm func:%s",
                    deletor.toString().c_str() ) ;
            goto error ;
         }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_CATALOGMGR_RM_PROCEDURES, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_QUERYSPACEINFO, "catCatalogueManager::processCmdQuerySpaceInfo" )
   INT32 catCatalogueManager::processCmdQuerySpaceInfo( const CHAR * pQuery,
                                                        CHAR * * ppReplyBody,
                                                        UINT32 & replyBodyLen,
                                                        INT32 & returnNum )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_QUERYSPACEINFO ) ;
      const CHAR *csName = NULL ;
      BSONObj boSpace ;
      BOOLEAN isExist = FALSE ;
      vector< INT32 > groups ;
      BSONObjBuilder builder ;
      BSONObj retObj ;

      try
      {
         BSONObj boQuery( pQuery ) ;
         rtnGetStringElement( boQuery,  CAT_COLLECTION_SPACE_NAME, &csName ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                      CAT_COLLECTION_SPACE_NAME, rc ) ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
         goto error ;
      }

      PD_TRACE1 ( SDB_CATALOGMGR_QUERYSPACEINFO, PD_PACK_STRING ( csName ) ) ;

      rc = catCheckSpaceExist( csName, isExist, boSpace, _pEduCB ) ;
      PD_RC_CHECK( rc, PDERROR, "Check collection space[%s] exist failed, "
                   "rc: %d", csName, rc ) ;
      PD_TRACE1 ( SDB_CATALOGMGR_QUERYSPACEINFO,PD_PACK_INT ( isExist ) ) ;

      if ( !isExist )
      {
         rc = SDB_DMS_CS_NOTEXIST ;
         goto error ;
      }

      rc = catGetCSGroupsFromCLs( csName, _pEduCB, groups ) ;
      PD_RC_CHECK( rc, PDERROR, "Get collection space[%s] all groups failed, "
                   "rc: %d", csName, rc ) ;

      builder.appendElements( boSpace ) ;
      {
         string groupName ;
         BSONArrayBuilder sub( builder.subarrayStart( CAT_GROUP_NAME ) ) ;
         for ( UINT32 i = 0 ; i < groups.size() ; ++i )
         {
            catGroupID2Name( groups[ i ], groupName, _pEduCB ) ;
            sub.append( BSON( CAT_GROUPID_NAME << groups[ i ] <<
                              CAT_GROUPNAME_NAME << groupName ) ) ;
         }
         sub.done() ;
      }
      retObj = builder.obj() ;

      returnNum = 1 ;
      replyBodyLen = retObj.objsize() ;
      *ppReplyBody = ( CHAR* )SDB_OSS_MALLOC( replyBodyLen ) ;
      PD_CHECK( *ppReplyBody, SDB_OOM, error, PDERROR,
                "Failed to alloc memry, size: %d", replyBodyLen ) ;

      ossMemcpy( *ppReplyBody, retObj.objdata(), replyBodyLen ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_QUERYSPACEINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_QUERYCATALOG, "catCatalogueManager::processQueryCatalogue" )
   INT32 catCatalogueManager::processQueryCatalogue ( const NET_HANDLE &handle,
                                                      MsgHeader *pMsg )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_QUERYCATALOG ) ;
      MsgCatQueryCatReq *pCatReq       = (MsgCatQueryCatReq*)pMsg ;
      MsgOpReply *pReply               = NULL;

      PD_CHECK ( pmdIsPrimary(), SDB_CLS_NOT_PRIMARY, error, PDWARNING,
                 "service deactive but received query catalogue request" ) ;

      PD_CHECK ( pCatReq->header.messageLength >=
                 (INT32)sizeof(MsgCatQueryCatReq),
                 SDB_INVALIDARG, error, PDERROR,
                 "recived unexpected query catalogue request, "
                 "message length(%d) is invalied",
                 pCatReq->header.messageLength ) ;
      try
      {
         CHAR *pCollectionName = NULL ;
         SINT32 flag           = 0 ;
         SINT64 numToSkip      = 0 ;
         SINT64 numToReturn    = -1 ;
         CHAR *pQuery          = NULL ;
         CHAR *pFieldSelector  = NULL ;
         CHAR *pOrderBy        = NULL ;
         CHAR *pHint           = NULL ;
         rc = msgExtractQuery  ( (CHAR *)pCatReq, &flag, &pCollectionName,
                                 &numToSkip, &numToReturn, &pQuery,
                                 &pFieldSelector, &pOrderBy, &pHint ) ;
         BSONObj matcher(pQuery);
         BSONObj selector(pFieldSelector);
         BSONObj orderBy(pOrderBy);
         BSONObj hint(pHint);
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to extract message, rc = %d", rc ) ;
         rc = catQueryAndGetMore ( &pReply, CAT_COLLECTION_INFO_COLLECTION,
                                   selector, matcher, orderBy, hint, flag,
                                   _pEduCB, numToSkip, numToReturn ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to query from catalog, rc = %d", rc ) ;
         PD_CHECK ( pReply->numReturned >= 1, SDB_DMS_NOTEXIST, error,
                    PDWARNING, "Collection does not exist:%s",
                    matcher.toString().c_str() ) ;
         PD_CHECK ( pReply->numReturned <= 1, SDB_CAT_CORRUPTION, error,
                    PDSEVERE,
                    "More than one records returned for query, "
                    "possible catalog corruption" ) ;
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR,
                       "Exception during query catalogue request:%s",
                       e.what() ) ;
      }
      pReply->header.opCode        = MSG_CAT_QUERY_CATALOG_RSP ;
      pReply->header.TID           = pCatReq->header.TID ;
      pReply->header.requestID     = pCatReq->header.requestID ;
      pReply->header.routeID.value = 0 ;
   done :
      if ( SDB_OK == rc && NULL != pReply )
      {
         rc = _pCatCB->netWork()->syncSend ( handle, pReply );
         SDB_OSS_FREE ( pReply );
      }
      else
      {
         MsgOpReply replyMsg;
         replyMsg.header.messageLength = sizeof( MsgOpReply );
         replyMsg.header.opCode        = MSG_CAT_QUERY_CATALOG_RSP;
         replyMsg.header.TID           = pCatReq->header.TID;
         replyMsg.header.routeID.value = 0;
         replyMsg.header.requestID     = pCatReq->header.requestID;
         replyMsg.numReturned          = 0;
         replyMsg.flags                = rc;
         replyMsg.contextID            = -1 ;
         PD_TRACE1 ( SDB_CATALOGMGR_QUERYCATALOG,
                     PD_PACK_INT ( rc ) ) ;
         rc = _pCatCB->netWork()->syncSend ( handle, &replyMsg );
      }
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_QUERYCATALOG, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_DROPCOLLECTION, "catCatalogueManager::processCmdDropCollection" )
   INT32 catCatalogueManager::processCmdDropCollection( const CHAR *pQuery,
                                                        INT32 version )
   {
      INT32 rc                         = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_DROPCOLLECTION ) ;

      const CHAR *strName              = NULL ;

      try
      {
         BSONObj boDeletor = BSONObj ( pQuery ) ;
         BSONElement beName = boDeletor.getField( CAT_COLLECTION_NAME ) ;
         PD_CHECK ( String == beName.type(), SDB_INVALIDARG, error,
                    PDERROR, "Failed to drop the collection, get collection "
                    "name failed, type: %d", beName.type() ) ;
         strName = beName.valuestr() ;
         PD_TRACE1 ( SDB_CATALOGMGR_DROPCOLLECTION,
                     PD_PACK_STRING ( strName ) ) ;
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = catRemoveCLEx( strName, _pEduCB, _pDmsCB, _pDpsCB,
                          _majoritySize(), TRUE, version ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to drop collection %s from catalog, "
                    "rc = %d", strName, rc ) ;

   done :
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_DROPCOLLECTION, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_QUERYTASK, "catCatalogueManager::processQueryTask" )
   INT32 catCatalogueManager::processQueryTask ( const NET_HANDLE &handle,
                                                 MsgHeader *pMsg )
   {
      INT32 rc                         = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_QUERYTASK ) ;
      MsgCatQueryTaskReq *pTaskRequest = (MsgCatQueryTaskReq*)pMsg ;
      MsgCatQueryTaskRes *pReply       = NULL ;
      INT32 flag                       = 0 ;
      SINT64 numToSkip                 = 0 ;
      SINT64 numToReturn               = 0 ;
      CHAR *pQuery                     = NULL ;
      CHAR *pFieldSelector             = NULL ;
      CHAR *pOrderBy                   = NULL ;
      CHAR *pHint                      = NULL ;
      CHAR *pCollectionName            = NULL ;

      PD_CHECK ( pmdIsPrimary(), SDB_CLS_NOT_PRIMARY, error, PDWARNING,
                 "service deactive but received query catalogue request" ) ;

      PD_CHECK ( pTaskRequest->header.messageLength >=
                 (INT32)sizeof(MsgCatQueryTaskReq),
                 SDB_INVALIDARG, error, PDERROR,
                 "received unexpected query task request, "
                 "message length(%d) is invalid",
                 pTaskRequest->header.messageLength ) ;

      try
      {
         rc = msgExtractQuery ( (CHAR*)pTaskRequest, &flag, &pCollectionName,
                                &numToSkip, &numToReturn, &pQuery,
                                &pFieldSelector, &pOrderBy, &pHint ) ;
         BSONObj matcher ( pQuery ) ;
         BSONObj selector ( pFieldSelector ) ;
         BSONObj orderBy ( pOrderBy );
         BSONObj hint ( pHint ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to extract message, rc = %d", rc ) ;
         rc = catQueryAndGetMore ( &pReply, CAT_TASK_INFO_COLLECTION,
                                   selector, matcher, orderBy, hint, flag,
                                   _pEduCB, numToSkip, numToReturn ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to perform query from catalog, rc = %d", rc ) ;

         PD_CHECK ( pReply->numReturned >= 1, SDB_CAT_TASK_NOTFOUND, error,
                    PDINFO, "Task does not exist" ) ;
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR,
                       "Exception when extracting query task: %s",
                       e.what() ) ;
      }
      pReply->header.opCode        = MSG_CAT_QUERY_TASK_RSP ;
      pReply->header.TID           = pTaskRequest->header.TID ;
      pReply->header.requestID     = pTaskRequest->header.requestID ;
      pReply->header.routeID.value = 0 ;

   done :
      if ( SDB_OK == rc && pReply )
      {
         rc = _pCatCB->netWork()->syncSend ( handle, pReply );
      }
      else
      {
         MsgOpReply replyMsg;
         replyMsg.header.messageLength = sizeof( MsgOpReply );
         replyMsg.header.opCode        = MSG_CAT_QUERY_TASK_RSP ;
         replyMsg.header.TID           = pTaskRequest->header.TID;
         replyMsg.header.routeID.value = 0;
         replyMsg.header.requestID     = pTaskRequest->header.requestID;
         replyMsg.numReturned          = 0;
         replyMsg.flags                = rc;
         replyMsg.contextID            = -1 ;
         PD_TRACE1 ( SDB_CATALOGMGR_QUERYTASK, PD_PACK_INT ( rc ) ) ;
         rc = _pCatCB->netWork()->syncSend ( handle, &replyMsg );
      }
      if ( pReply )
      {
         SDB_OSS_FREE ( pReply ) ;
      }
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_QUERYTASK, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_ALTERCOLLECTION, "catCatalogueManager::processAlterCollection" )
   INT32 catCatalogueManager::processAlterCollection ( void *pMsg,
                                                       CHAR **ppReplyBody,
                                                       UINT32 &replyBodyLen,
                                                       INT32 &returnNum )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_ALTERCOLLECTION ) ;
      const CHAR *strName              = NULL ;
      BSONObj options ;
      BOOLEAN isCollectionExist        = FALSE ;
      BSONObj boCollectionRecord ;
      BSONObj matcher ;
      BSONObj updater ;
      BSONObj hint ;
      BSONObj optionsObj ;
      UINT32 mask = 0 ;
      catCollectionInfo clInfo ;
      returnNum = 0 ;
      _clsCatalogSet catSet( "" ) ; 

      try
      {
         BSONObj boAlterObj ( ( CHAR * )pMsg ) ;
         BSONElement beName = boAlterObj.getField ( CAT_COLLECTION_NAME ) ;
         BSONElement beOptions = boAlterObj.getField ( CAT_OPTIONS_NAME ) ;
         PD_CHECK ( String == beName.type(), SDB_INVALIDARG, error, PDERROR,
                    "Invalid field %s", CAT_COLLECTION_NAME ) ;
         strName = beName.valuestr() ;
         PD_TRACE1 ( SDB_CATALOGMGR_ALTERCOLLECTION,
                     PD_PACK_STRING ( strName ) ) ;
         PD_CHECK ( Object == beOptions.type(), SDB_INVALIDARG, error, PDERROR,
                    "Invalid field %s", CAT_OPTIONS_NAME ) ;

         rc = catCheckCollectionExist ( strName, isCollectionExist,
                                        boCollectionRecord, _pEduCB ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to detect collection existence, rc = %d", rc ) ;

         if ( !isCollectionExist )
         {
            PD_LOG( PDERROR, "collection[%s] does not exist", strName ) ;
            rc = SDB_DMS_NOTEXIST ;
            goto error ;
         }

         optionsObj = beOptions.embeddedObject() ;
         if ( optionsObj.isEmpty() )
         {
            PD_LOG( PDERROR, "empty options object in alter request" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         rc = _checkAndBuildCataRecord( optionsObj,
                                        mask, clInfo, FALSE ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "invalid alter request:%s",
                    boAlterObj.toString().c_str() ) ;
            goto error ;
         }

         rc = catSet.updateCatSet( boCollectionRecord ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to save json[%s] to catalogset:%d",
                    boCollectionRecord.toString(FALSE, TRUE).c_str(), rc ) ;
            goto error ;
         }
         rc = _buildAlterObjWithMetaAndObj( catSet,
                                            mask,
                                            clInfo,
                                            updater ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build alter object, alter req[%s], rc:%d",
                    boAlterObj.toString(FALSE, TRUE).c_str(), rc ) ;
            goto error ;
         }

         matcher = BSON ( CAT_COLLECTION_NAME << strName ) ;
         rc = rtnUpdate ( CAT_COLLECTION_INFO_COLLECTION,
                          matcher, BSON( "$set" << updater ), hint,
                          0, _pEduCB, _pDmsCB, _pDpsCB,
                          _majoritySize() );
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to alter collection, rc = %d", rc ) ;

         if ( !catSet.isMainCL() )
         {
            BSONArrayBuilder arrBuilder ;
            BSONElement gpInfo = boCollectionRecord.getField( CAT_CATALOGINFO_NAME ) ;
            if ( Array == gpInfo.type() ||
                 Object == gpInfo.type() )
            {
               BSONObjIterator itr( gpInfo.embeddedObject() ) ;
               while ( itr.more() )
               {
                  arrBuilder << itr.next() ;
               }
               BSONObj replyObj = BSON( CAT_GROUP_NAME << arrBuilder.arr() ) ;
               replyBodyLen = replyObj.objsize() ;
               *ppReplyBody = ( CHAR * )SDB_OSS_MALLOC( replyBodyLen ) ;
               if ( NULL == *ppReplyBody )
               {
                  PD_LOG( PDERROR, "failed to allocate mem." ) ;
                  rc = SDB_OOM ;
                  goto error ;
               }

               ossMemcpy( *ppReplyBody, replyObj.objdata(), replyBodyLen ) ;
               returnNum = 1 ;
            }
         }
         else
         {
            BSONObj replyObj ;
            vector<string> subCLLst ;
            rc = catSet.getSubCLList( subCLLst ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get sub cl list:%d", rc ) ;
               goto error ;
            }

            if ( !subCLLst.empty() )
            {
               rc = _getGroupsOfCollections( subCLLst, replyObj ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "failed to get groups of sub cl:%d", rc ) ;
                  goto error ;
               }

               *ppReplyBody = ( CHAR * )SDB_OSS_MALLOC( replyObj.objsize() ) ;
               if ( NULL == *ppReplyBody )
               {
                  PD_LOG( PDERROR, "failed to allocate mem." ) ;
                  rc = SDB_OOM ;
                  goto error ;
               }

               replyBodyLen = replyObj.objsize() ;
               ossMemcpy( *ppReplyBody, replyObj.objdata(), replyBodyLen ) ;
               returnNum = 1 ;
            }
         }
         
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR,
                       "Exception hit when parsing alter collection: %s",
                       e.what() ) ;
      }
   done :
      PD_TRACE1 ( SDB_CATALOGMGR_ALTERCOLLECTION, PD_PACK_INT ( rc ) ) ;
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_ALTERCOLLECTION, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_CREATECS, "catCatalogueManager::processCmdCreateCS" )
   INT32 catCatalogueManager::processCmdCreateCS( const CHAR * pQuery,
                                                  CHAR * * ppReplyBody,
                                                  UINT32 & replyBodyLen,
                                                  INT32 & returnNum )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_CREATECS ) ;
      INT32 groupID = CAT_INVALID_GROUPID ;

      try
      {
         BSONObj groupObj ;
         BSONObj query( pQuery ) ;
         rc = _createCS( query, groupID ) ;
         PD_RC_CHECK( rc, PDERROR, "Create collection space failed, rc: %d",
                      rc ) ;
      }
      catch( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Occurred exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_CREATECS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_CREATECL, "catCatalogueManager::processCmdCreateCL" )
   INT32 catCatalogueManager::processCmdCreateCL( const CHAR *pQuery,
                                                  CHAR **ppReplyBody,
                                                  UINT32 &replyBodyLen,
                                                  INT32 &returnNum )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_CREATECL ) ;
      INT32 groupID = CAT_INVALID_GROUPID ; 
      std::vector<UINT64> taskIDs ;
      try
      {
         BSONObj query( pQuery ) ;
         rc = _createCL( query, groupID, taskIDs ) ;
         PD_RC_CHECK( rc, PDERROR, "Create collection failed, rc: %d", rc ) ;

         {
            returnNum = 1 ;

            BSONObjBuilder replyBuild ;
            replyBuild.append( CAT_CATALOGVERSION_NAME, CAT_VERSION_BEGIN ) ;
            BSONObjBuilder sub( replyBuild.subarrayStart( CAT_GROUP_NAME ) ) ;
            sub.append( "0",
                        BSON( CAT_GROUPID_NAME << groupID ) ) ;
            sub.done() ;

            if ( !taskIDs.empty() )
            {
               BSONObjBuilder task( replyBuild.subarrayStart( CAT_TASKID_NAME ) ) ;
               for ( UINT32 i = 0; i < taskIDs.size(); i++ )
               {
                  std::stringstream ss ;
                  ss << i ;
                  UINT64 taskID = taskIDs.at( i ) ;
                  task.append( ss.str(), (long long int)(taskID) ) ;
               }

               task.done() ;
            }

            BSONObj replyObj = replyBuild.obj() ;

            replyBodyLen = replyObj.objsize() ;
            *ppReplyBody = (CHAR*)SDB_OSS_MALLOC( replyBodyLen ) ;

            PD_CHECK( *ppReplyBody, SDB_OOM, error, PDERROR,
                      "Failed to alloc memry, size: %d", replyBodyLen ) ;

            ossMemcpy( *ppReplyBody, replyObj.objdata(), replyBodyLen ) ;
         }
      }
      catch( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Occurred exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_CREATECL, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_CMDSPLIT, "catCatalogueManager::processCmdSplit" )
   INT32 catCatalogueManager::processCmdSplit( const CHAR * pQuery,
                                               INT32 opCode,
                                               CHAR * * ppReplyBody,
                                               UINT32 & replyBodyLen,
                                               INT32 & returnNum )
   {
      INT32 rc = SDB_OK ;
      const CHAR *clFullName = NULL ;
      clsCatalogSet *pCataSet = NULL ;
      INT32 groupID = CAT_INVALID_GROUPID ;
      UINT64 taskID = CLS_INVALID_TASKID ;

      PD_TRACE_ENTRY ( SDB_CATALOGMGR_CMDSPLIT ) ;
      try
      {
         BSONObj cataObj ;
         BOOLEAN clExist = FALSE ;
         BSONObj query( pQuery ) ;

         if ( MSG_CAT_SPLIT_PREPARE_REQ == opCode ||
              MSG_CAT_SPLIT_READY_REQ == opCode ||
              MSG_CAT_SPLIT_CHGMETA_REQ == opCode )
         {
            rc = rtnGetStringElement( query, CAT_COLLECTION_NAME,
                                      &clFullName ) ;
            PD_RC_CHECK( rc, PDERROR, "Get split collection name failed, "
                         "rc: %d, info: %s", rc, query.toString().c_str() ) ;

            rc = catCheckCollectionExist( clFullName, clExist, cataObj,
                                          _pEduCB ) ;
            PD_RC_CHECK( rc, PDERROR, "Check collection exist failed, rc: %d",
                         rc ) ;
            PD_CHECK( clExist, SDB_DMS_NOTEXIST, error, PDWARNING,
                      "Collection[%s] is no longer existed", clFullName ) ;

            pCataSet = SDB_OSS_NEW clsCatalogSet( clFullName, TRUE ) ;
            PD_CHECK( pCataSet, SDB_OOM, error, PDERROR, "Alloc failed" ) ;
            rc = pCataSet->updateCatSet( cataObj, 0 ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to update catalog set, cata "
                         "info: %s, rc: %d", cataObj.toString().c_str(), rc ) ;

            PD_CHECK( pCataSet->isSharding(), SDB_COLLECTION_NOTSHARD, error,
                      PDERROR, "Collection[%s] is not sharding, can't split",
                      clFullName ) ;

            PD_CHECK( !pCataSet->isMainCL(), SDB_MAIN_CL_OP_ERR, error, 
                      PDERROR, "Collection[%s] is MainCL, can't split", 
                      clFullName ) ;
         }

         switch ( opCode )
         {
            case MSG_CAT_SPLIT_PREPARE_REQ :
               rc = catSplitPrepare( query, clFullName, pCataSet,
                                     groupID, _pEduCB ) ;
               break ;
            case MSG_CAT_SPLIT_READY_REQ :
               rc = catSplitReady( query, clFullName, pCataSet, groupID,
                                   _taskMgr, _pEduCB, _majoritySize(),
                                   &taskID ) ;
               break ;
            case MSG_CAT_SPLIT_CANCEL_REQ :
               rc = catSplitCancel( query, _pEduCB, groupID, _majoritySize() ) ;
               break ;
            case MSG_CAT_SPLIT_START_REQ :
               rc = catSplitStart( query, _pEduCB, _majoritySize() ) ;
               break ;
            case MSG_CAT_SPLIT_CHGMETA_REQ :
               rc = catSplitChgMeta( query, clFullName, pCataSet, _pEduCB,
                                     _majoritySize() ) ;
               break ;
            case MSG_CAT_SPLIT_CLEANUP_REQ :
               rc = catSplitCleanup( query, _pEduCB, _majoritySize() ) ;
               break ;
            case MSG_CAT_SPLIT_FINISH_REQ :
               rc = catSplitFinish( query, _pEduCB, _majoritySize() ) ;
               break ;
            default :
               rc = SDB_INVALIDARG ;
               break ;
         }

         PD_RC_CHECK( rc, PDERROR, "Split collection failed, opCode: %d, "
                      "rc: %d", opCode, rc ) ;

         if ( CAT_INVALID_GROUPID != groupID )
         {
            returnNum = 1 ;

            BSONObjBuilder replyBuild ;
            if ( pCataSet )
            {
               replyBuild.append( CAT_CATALOGVERSION_NAME,
                                  pCataSet->getVersion() ) ;
            }
            if ( CLS_INVALID_TASKID != taskID )
            {
               replyBuild.append( CAT_TASKID_NAME, (long long)taskID ) ;
            }
            BSONObjBuilder sub( replyBuild.subarrayStart( CAT_GROUP_NAME ) ) ;
            sub.append( "0", BSON( CAT_GROUPID_NAME << groupID ) ) ;
            sub.done() ;
            BSONObj replyObj = replyBuild.obj() ;

            replyBodyLen = replyObj.objsize() ;
            *ppReplyBody = (CHAR*)SDB_OSS_MALLOC( replyBodyLen ) ;

            PD_CHECK( *ppReplyBody, SDB_OOM, error, PDERROR,
                      "Failed to alloc memry, size: %d", replyBodyLen ) ;

            ossMemcpy( *ppReplyBody, replyObj.objdata(), replyBodyLen ) ;
         }
      }
      catch( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Occurred exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      if ( pCataSet )
      {
         SDB_OSS_DEL pCataSet ;
      }
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_CMDSPLIT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__CHECKCSOBJ, "catCatalogueManager::_checkCSObj" )
   INT32 catCatalogueManager::_checkCSObj( const BSONObj & infoObj,
                                           catCSInfo & csInfo )
   {
      INT32 rc = SDB_OK ;

      csInfo._pCSName = NULL ;
      csInfo._domainName = NULL ;
      csInfo._pageSize = DMS_PAGE_SIZE_DFT ;
      csInfo._lobPageSize = DMS_DEFAULT_LOB_PAGE_SZ ;
      INT32 expected = 0 ;

      PD_TRACE_ENTRY ( SDB_CATALOGMGR__CHECKCSOBJ ) ;
      BSONObjIterator it( infoObj ) ;
      while ( it.more() )
      {
         BSONElement ele = it.next() ;

         if ( 0 == ossStrcmp( ele.fieldName(), CAT_COLLECTION_SPACE_NAME ) )
         {
            PD_CHECK( String == ele.type(), SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] type[%d] error", CAT_COLLECTION_NAME,
                      ele.type() ) ;
            csInfo._pCSName = ele.valuestr() ;
            ++expected ;
         }
         else if ( 0 == ossStrcmp( ele.fieldName(), CAT_PAGE_SIZE_NAME ) )
         {
            PD_CHECK( ele.isNumber(), SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] type[%d] error", CAT_PAGE_SIZE_NAME,
                      ele.type() ) ;
            if ( 0 != ele.numberInt() )
            {
               csInfo._pageSize = ele.numberInt() ;
            }

            PD_CHECK ( csInfo._pageSize == DMS_PAGE_SIZE4K ||
                       csInfo._pageSize == DMS_PAGE_SIZE8K ||
                       csInfo._pageSize == DMS_PAGE_SIZE16K ||
                       csInfo._pageSize == DMS_PAGE_SIZE32K ||
                       csInfo._pageSize == DMS_PAGE_SIZE64K, SDB_INVALIDARG,
                       error, PDERROR, "PageSize must be 4K/8K/16K/32K/64K" ) ;
            ++expected ;
         }
         else if ( 0 == ossStrcmp( ele.fieldName(), CAT_DOMAIN_NAME ) )
         {
            PD_CHECK( String == ele.type(), SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] type[%d] error", CAT_DOMAIN_NAME,
                      ele.type() ) ;
            csInfo._domainName = ele.valuestr() ;
            ++expected ;
         }
         else if ( 0 == ossStrcmp( ele.fieldName(), CAT_LOB_PAGE_SZ_NAME ) )
         {
            PD_CHECK( ele.isNumber(), SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] type[%d] error", CAT_LOB_PAGE_SZ_NAME,
                      ele.type() ) ;
            if ( 0 != ele.numberInt() )
            {
               csInfo._lobPageSize = ele.numberInt() ;
            }

            PD_CHECK ( csInfo._lobPageSize == DMS_PAGE_SIZE4K ||
                       csInfo._lobPageSize == DMS_PAGE_SIZE8K ||
                       csInfo._lobPageSize == DMS_PAGE_SIZE16K ||
                       csInfo._lobPageSize == DMS_PAGE_SIZE32K ||
                       csInfo._lobPageSize == DMS_PAGE_SIZE64K ||
                       csInfo._lobPageSize == DMS_PAGE_SIZE128K ||
                       csInfo._lobPageSize == DMS_PAGE_SIZE256K ||
                       csInfo._lobPageSize == DMS_PAGE_SIZE512K, SDB_INVALIDARG,
                       error, PDERROR, "PageSize must be 4K/8K/16K/32K/64K/128K/256K/512K" ) ;
            ++expected ;
         }
         else
         {
            PD_RC_CHECK ( SDB_INVALIDARG, PDERROR,
                          "Unexpected field[%s] in create collection space "
                          "command", ele.toString().c_str() ) ;
         }
      }

      PD_CHECK( csInfo._pCSName, SDB_INVALIDARG, error, PDERROR,
                "Collection space name not set" ) ;

      PD_CHECK( infoObj.nFields() == expected, SDB_INVALIDARG, error, PDERROR,
                "unexpected fields exsit." ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR__CHECKCSOBJ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__CHECKANDBUILDCATARECORD, "catCatalogueManager::_checkAndBuildCataRecord" )
   INT32 catCatalogueManager::_checkAndBuildCataRecord( const BSONObj &infoObj,
                                                        UINT32 &fieldMask,
                                                        catCollectionInfo &clInfo,
                                                        BOOLEAN clNameIsNecessary )
   {
      INT32 rc = SDB_OK ;

      clInfo._pCLName            = NULL ;
      clInfo._replSize           = 1 ;
      clInfo._enSureShardIndex   = true ;
      clInfo._pShardingType      = CAT_SHARDING_TYPE_RANGE ;
      clInfo._shardPartition     = CAT_SHARDING_PARTITION_DEFAULT ;
      clInfo._isHash             = FALSE ;
      clInfo._isSharding         = FALSE ;
      clInfo._isMainCL           = false;
      clInfo._assignType         = ASSIGN_RANDOM ;

      fieldMask = 0 ;

      PD_TRACE_ENTRY ( SDB_CATALOGMGR__CHECKANDBUILDCATARECORD ) ;
      BSONObjIterator it( infoObj ) ;
      while ( it.more() )
      {
         BSONElement eleTmp = it.next() ;

         if ( ossStrcmp( eleTmp.fieldName(), CAT_COLLECTION_NAME ) == 0 )
         {
            PD_CHECK( String == eleTmp.type(), SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] type[%d] error", CAT_COLLECTION_NAME,
                      eleTmp.type() ) ;
            clInfo._pCLName = eleTmp.valuestr() ;
            fieldMask |= CAT_MASK_CLNAME ;
         }
         else if ( ossStrcmp( eleTmp.fieldName(),
                              CAT_SHARDINGKEY_NAME ) == 0 )
         {
            PD_CHECK( Object == eleTmp.type(), SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] type[%d] error", CAT_SHARDINGKEY_NAME,
                      eleTmp.type() ) ;
            clInfo._shardingKey = eleTmp.embeddedObject() ;
            PD_CHECK( _ixmIndexKeyGen::validateKeyDef( clInfo._shardingKey ),
                      SDB_INVALIDARG, error, PDERROR,
                      "Sharding key[%s] definition is invalid",
                      clInfo._shardingKey.toString().c_str() ) ;
            fieldMask |= CAT_MASK_SHDKEY ;
            clInfo._isSharding = TRUE ;
         }
         else if ( ossStrcmp( eleTmp.fieldName(), CAT_CATALOG_W_NAME ) == 0 )
         {
            PD_CHECK( NumberInt == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error", CAT_CATALOG_W_NAME,
                      eleTmp.type() ) ;
            clInfo._replSize = eleTmp.numberInt() ;
            if ( clInfo._replSize <= 0 )
            {
               clInfo._replSize = CLS_REPLSET_MAX_NODE_SIZE ;
            }

            PD_CHECK( clInfo._replSize <= CLS_REPLSET_MAX_NODE_SIZE,
                      SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] value should less than or equal to %d",
                      CAT_CATALOG_W_NAME, CLS_REPLSET_MAX_NODE_SIZE ) ;
            fieldMask |= CAT_MASK_REPLSIZE ;
         }
         else if ( ossStrcmp( eleTmp.fieldName(), CAT_ENSURE_SHDINDEX ) == 0 )
         {
            PD_CHECK( Bool == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error", CAT_ENSURE_SHDINDEX,
                      eleTmp.type() ) ;
            clInfo._enSureShardIndex = eleTmp.Bool() ;
            fieldMask |= CAT_MASK_SHDIDX ;
         }
         else if ( ossStrcmp( eleTmp.fieldName(), CAT_SHARDING_TYPE ) == 0 )
         {
            PD_CHECK( String == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error", CAT_SHARDING_TYPE,
                      eleTmp.type() ) ;

            clInfo._pShardingType = eleTmp.valuestr() ;
            PD_CHECK( 0 == ossStrcmp( clInfo._pShardingType,
                                      CAT_SHARDING_TYPE_HASH ) ||
                      0 == ossStrcmp( clInfo._pShardingType,
                                      CAT_SHARDING_TYPE_RANGE ),
                      SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] value[%s] should be[%s/%s]",
                      CAT_SHARDING_TYPE, clInfo._pShardingType,
                      CAT_SHARDING_TYPE_HASH, CAT_SHARDING_TYPE_RANGE ) ;
            fieldMask |= CAT_MASK_SHDTYPE ;

            if ( 0 == ossStrcmp( clInfo._pShardingType,
                                 CAT_SHARDING_TYPE_HASH ) )
            {
               clInfo._isHash = TRUE ;
            }
         }
         else if ( ossStrcmp( eleTmp.fieldName(),
                              CAT_SHARDING_PARTITION ) == 0 )
         {
            PD_CHECK( NumberInt == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error",
                      CAT_SHARDING_PARTITION, eleTmp.type() ) ;
            clInfo._shardPartition = eleTmp.numberInt() ;
            PD_CHECK( ossIsPowerOf2( (UINT32)clInfo._shardPartition ),
                      SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] value must be power of 2",
                      CAT_SHARDING_PARTITION ) ;
            PD_CHECK( clInfo._shardPartition >= CAT_SHARDING_PARTITION_MIN &&
                      clInfo._shardPartition <= CAT_SHARDING_PARTITION_MAX,
                      SDB_INVALIDARG, error, PDERROR, "Field[%s] value[%d] "
                      "should between in[%d, %d]", CAT_SHARDING_PARTITION,
                      clInfo._shardPartition, CAT_SHARDING_PARTITION_MIN,
                      CAT_SHARDING_PARTITION_MAX ) ;
            fieldMask |= CAT_MASK_SHDPARTITION ;
         }
         else if ( ossStrcmp ( eleTmp.fieldName(),
                               CAT_COMPRESSED ) == 0 )
         {
            PD_CHECK( Bool == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error",
                      CAT_COMPRESSED, eleTmp.type() ) ;
            clInfo._isCompressed = eleTmp.boolean() ;
            fieldMask |= CAT_MASK_COMPRESSED ;
         }
         else if ( ossStrcmp( eleTmp.fieldName(),
                              CAT_IS_MAINCL ) == 0 )
         {
            PD_CHECK( Bool == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error",
                      CAT_IS_MAINCL, eleTmp.type() ) ;
            clInfo._isMainCL = eleTmp.boolean() ;
            fieldMask |= CAT_MASK_ISMAINCL;
         }
         else if ( 0 == ossStrcmp( eleTmp.fieldName(),
                                   CAT_GROUP_NAME ) )
         {
            PD_CHECK( String == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error",
                      CAT_GROUP_NAME, eleTmp.type() ) ;
            if ( 0 == ossStrcasecmp( eleTmp.valuestr(),
                                     CAT_ASSIGNGROUP_FOLLOW ) )
            {
               clInfo._assignType = ASSIGN_FOLLOW ;
            }
            else if ( 0 == ossStrcasecmp( eleTmp.valuestr(),
                                          CAT_ASSIGNGROUP_RANDOM ) )
            {
               clInfo._assignType = ASSIGN_RANDOM ;
            }
            else
            {
               clInfo._gpSpecified = eleTmp.valuestr() ;
            }
         }
         else if ( 0 == ossStrcmp( eleTmp.fieldName(),
                                   CAT_DOMAIN_AUTO_SPLIT ) )
         {
            PD_CHECK( Bool == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error",
                      CAT_DOMAIN_AUTO_SPLIT, eleTmp.type() ) ;
            clInfo._autoSplit = eleTmp.Bool() ;
            fieldMask |= CAT_MASK_AUTOASPLIT ;
         }
         else if ( 0 == ossStrcmp( eleTmp.fieldName(),
                                   CAT_DOMAIN_AUTO_REBALANCE ) )
         {
            PD_CHECK( Bool == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error",
                      CAT_DOMAIN_AUTO_REBALANCE, eleTmp.type() ) ;
            clInfo._autoRebalance = eleTmp.Bool() ;
            fieldMask |= CAT_MASK_AUTOREBALAN ;
         }
         else if ( 0 == ossStrcmp( eleTmp.fieldName(),
                                   CAT_AUTO_INDEX_ID ) )
         {
            PD_CHECK( Bool == eleTmp.type(), SDB_INVALIDARG, error,
                      PDERROR, "Field[%s] type[%d] error",
                      CAT_AUTO_INDEX_ID, eleTmp.type() ) ;
            clInfo._autoIndexId = eleTmp.Bool() ;
            fieldMask |= CAT_MASK_AUTOINDEXID ;
         }
         else
         {
            PD_RC_CHECK ( SDB_INVALIDARG, PDERROR,
                          "Unexpected field[%s] in create collection command",
                          eleTmp.toString().c_str() ) ;
         }
      }
      if ( clInfo._isMainCL )
      {
         PD_CHECK ( clInfo._isSharding,
                    SDB_NO_SHARDINGKEY, error, PDERROR,
                    "main-collection must have ShardingKey!" );
         PD_CHECK ( !clInfo._isHash,
                    SDB_INVALID_MAIN_CL_TYPE, error, PDERROR,
                    "the sharding-type of main-collection must be range!" );
      }
      if ( clInfo._autoSplit || clInfo._autoRebalance )
      {
         PD_CHECK ( clInfo._isSharding,
                    SDB_NO_SHARDINGKEY, error, PDERROR,
                    "can not do split or rebalance with out ShardingKey!" );

         PD_CHECK ( NULL == clInfo._gpSpecified,
                    SDB_INVALIDARG, error, PDERROR,
                    "can not do split or rebalance with out more than one group" );

         PD_CHECK( clInfo._isHash,
                   SDB_INVALIDARG, error, PDERROR,
                   "auto options only can be set when shard type is hash" ) ;
      }

      if ( fieldMask & CAT_MASK_SHDIDX ||
           fieldMask & CAT_MASK_SHDTYPE ||
           fieldMask & CAT_MASK_SHDPARTITION )
      {
         PD_CHECK( fieldMask & CAT_MASK_SHDKEY,
                   SDB_INVALIDARG, error, PDERROR,
                   "these arguments are legal only when sharding key is specified." ) ;
      }

      if ( clNameIsNecessary )
      {
         PD_CHECK( clInfo._pCLName, SDB_INVALIDARG, error, PDERROR,
                   "Collection name not set" ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR__CHECKANDBUILDCATARECORD, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__ASSIGNGROUP, "catCatalogueManager::_assignGroup" )
   INT32 catCatalogueManager::_assignGroup( vector < INT32 > * pGoups,
                                            INT32 & groupID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR__ASSIGNGROUP ) ;
      if ( !pGoups || pGoups->size() == 0 )
      {
         rc = _pCatCB->getAGroupRand( groupID ) ;
      }
      else
      {
         UINT32 size = pGoups->size() ;
         groupID = (*pGoups)[ ossRand() % size ] ;
      }
      PD_TRACE_EXITRC ( SDB_CATALOGMGR__ASSIGNGROUP, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__CHECKGROUPINDOMAIN, "catCatalogueManager::_checkGroupInDomain" )
   INT32 catCatalogueManager::_checkGroupInDomain( const CHAR * groupName,
                                                   const CHAR * domainName,
                                                   BOOLEAN & existed,
                                                   INT32 *pGroupID )
   {
      INT32 rc = SDB_OK ;
      existed = FALSE ;

      PD_TRACE_ENTRY ( SDB_CATALOGMGR__CHECKGROUPINDOMAIN ) ;
      BSONObj groupInfo ;

      rc = catGetGroupObj( groupName, TRUE, groupInfo, _pEduCB ) ;
      PD_RC_CHECK( rc, PDERROR, "Get group[%s] info failed, rc: %d",
                   groupName, rc ) ;

      if ( pGroupID )
      {
         rtnGetIntElement( groupInfo, CAT_GROUPID_NAME, *pGroupID ) ;
      }

      if ( 0 == ossStrcmp( domainName, CAT_SYS_DOMAIN_NAME ) )
      {
         existed = TRUE ;
      }
      else
      {
         BSONObj domainObj ;
         map<string, INT32> groups ;
         rc = catGetDomainObj( domainName, domainObj, _pEduCB ) ;
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
      PD_TRACE_EXITRC ( SDB_CATALOGMGR__CHECKGROUPINDOMAIN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__CREATECS, "catCatalogueManager::_createCS" )
   INT32 catCatalogueManager::_createCS( BSONObj & createObj,
                                         INT32 & groupID )
   {
      INT32 rc               = SDB_OK ;
      string strGroupName ;

      const CHAR *csName     = NULL ;
      const CHAR *domainName = NULL ;
      BOOLEAN isSpaceExist   = FALSE ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR__CREATECS ) ;

      catCSInfo csInfo ;
      BSONObj spaceObj ;
      BSONObj domainObj ;
      vector< INT32 >  domainGroups ;

      rc = _checkCSObj( createObj, csInfo ) ;
      PD_RC_CHECK( rc, PDERROR, "Check create collection space obj[%s] failed,"
                   "rc: %d", createObj.toString().c_str(), rc ) ;
      csName = csInfo._pCSName ;
      domainName = csInfo._domainName ;

      rc = dmsCheckCSName( csName ) ;
      PD_RC_CHECK( rc, PDERROR, "Check collection space name[%s] failed, rc: "
                   "%d", csName, rc ) ;

      rc = catCheckSpaceExist( csName, isSpaceExist, spaceObj, _pEduCB ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to check collection space existed, rc: "
                   "%d", rc ) ;
      PD_TRACE1 ( SDB_CATALOGMGR_CREATECS, PD_PACK_INT ( isSpaceExist ) ) ;
      PD_CHECK( FALSE == isSpaceExist, SDB_DMS_CS_EXIST, error, PDERROR,
                "Collection space[%s] is already existed", csName ) ;

      if ( domainName )
      {
         rc = catGetDomainObj( domainName, domainObj, _pEduCB ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get domain[%s] obj, rc: %d",
                      domainName, rc ) ;
         rc = catGetDomainGroups( domainObj, domainGroups ) ;
         PD_RC_CHECK( rc, PDERROR, "Get domain[%s] groups failed, rc: %d",
                      domainObj.toString().c_str(), rc ) ;
      }

      rc = _assignGroup( &domainGroups, groupID ) ;
      PD_RC_CHECK( rc, PDERROR, "Assign group for collection space[%s] "
                   "failed, rc: %d", csName, rc ) ;
      catGroupID2Name( groupID, strGroupName, _pEduCB ) ;

      {
         BSONObjBuilder newBuilder ;
         newBuilder.appendElements( csInfo.toBson() ) ;
         BSONObjBuilder sub1( newBuilder.subarrayStart( CAT_COLLECTION ) ) ;
         sub1.done() ;

         BSONObj newObj = newBuilder.obj() ;

         rc = rtnInsert( CAT_COLLECTION_SPACE_COLLECTION, newObj, 1, 0,
                         _pEduCB, _pDmsCB, _pDpsCB, _majoritySize() ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert collection space obj[%s] "
                      " to collection[%s], rc: %d", newObj.toString().c_str(),
                      CAT_COLLECTION_SPACE_COLLECTION, rc ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR__CREATECS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_CREATECOLLECTION, "catCatalogueManager::_createCL" )
   INT32 catCatalogueManager::_createCL( BSONObj & createObj,
                                         INT32 &groupID,
                                         std::vector<UINT64> &taskIDs )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_CREATECOLLECTION ) ;

      UINT32 fieldMask = 0 ;
      catCollectionInfo clInfo ;
      const CHAR *collectionName = NULL ;
      BSONObj newCLRecordObj ;

      CHAR szSpace[ DMS_COLLECTION_SPACE_NAME_SZ + 1 ] = {0} ;
      CHAR szCollection[ DMS_COLLECTION_NAME_SZ + 1 ] = {0} ;

      BOOLEAN isSpaceExist = FALSE ;
      BSONObj boSpaceRecord ;
      BOOLEAN isCollectionExist = FALSE ;
      BSONObj boCollectionRecord ;
      BSONObj domainObj ;
      std::string strGroupName ;
      groupID = CAT_INVALID_GROUPID ; 
      std::map<string, INT32> range ;

      rc = _checkAndBuildCataRecord( createObj, fieldMask, clInfo ) ;
      PD_RC_CHECK( rc, PDERROR, "Check create collection obj[%s] failed, rc: %d",
                   createObj.toString().c_str(), rc ) ;
      collectionName = clInfo._pCLName ;

      clInfo._version = catGetBucketVersion( collectionName, _pEduCB ) ;

      PD_TRACE1 ( SDB_CATALOGMGR_CREATECOLLECTION,
                  PD_PACK_STRING ( collectionName ) ) ;

      rc = catResolveCollectionName( collectionName,
                                     ossStrlen ( collectionName ),
                                     szSpace, DMS_COLLECTION_SPACE_NAME_SZ,
                                     szCollection, DMS_COLLECTION_NAME_SZ );
      PD_RC_CHECK ( rc, PDERROR, "Failed to resolve collection name: %s",
                    collectionName ) ;

      rc = dmsCheckCLName( szCollection, FALSE ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to check collection name: %s, rc = %d",
                    szCollection, rc ) ;

      rc = catCheckSpaceExist( szSpace, isSpaceExist,
                               boSpaceRecord, _pEduCB ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to check if collection space exist, "
                    "rc = %d", rc );
      PD_CHECK ( isSpaceExist, SDB_DMS_CS_NOTEXIST, error, PDERROR,
                 "Create failed, the collection space(%s) is not exist",
                 szSpace ) ;

      PD_TRACE1 ( SDB_CATALOGMGR_CREATECOLLECTION,
                  PD_PACK_INT ( isSpaceExist ) ) ;

      rc = catCheckCollectionExist( collectionName, isCollectionExist,
                                    boCollectionRecord, _pEduCB ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to check if collection exist, rc = %d", rc ) ;
      PD_CHECK ( !isCollectionExist, SDB_DMS_EXIST, error, PDERROR,
                 "Create failed, the collection(%s) exists",
                 collectionName ) ;

      PD_TRACE1 ( SDB_CATALOGMGR_CREATECOLLECTION,
                  PD_PACK_INT ( isCollectionExist ) ) ;

      {
         BSONElement domainEle = boSpaceRecord.getField( CAT_DOMAIN_NAME ) ;
         if ( String == domainEle.type() )
         {
            rc = catGetDomainObj( domainEle.valuestr(), domainObj, _pEduCB ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "Failed to get domain obj os cs[%s], rc:%d",
                       szSpace, rc ) ;
               goto error ;
            }
         }
      }

      rc = _combineOptions( domainObj, boSpaceRecord, fieldMask, clInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Failed to combine options, domainObj[%s],"
                 "create cl options[%s], rc:%d", domainObj.toString().c_str(),
                 createObj.toString().c_str(), rc ) ;
         goto error ;
      }

      rc = _chooseGroupOfCl( domainObj, boSpaceRecord, clInfo,
                             strGroupName, groupID, range ) ;
      PD_RC_CHECK( rc, PDERROR, "failed to choose group for cl[%s], rc: %d",
                   collectionName, rc ) ;

      rc = _buildCatalogRecord( clInfo, fieldMask, groupID,
                                strGroupName.c_str(),
                                newCLRecordObj ) ;
      PD_RC_CHECK( rc, PDERROR, "Build new collection catalog record failed, "
                   "rc: %d", rc ) ;

      PD_TRACE1 ( SDB_CATALOGMGR_CREATECOLLECTION,
                  PD_PACK_STRING ( newCLRecordObj.toString().c_str() ) ) ;

      rc = rtnInsert( CAT_COLLECTION_INFO_COLLECTION, newCLRecordObj,
                      1, 0, _pEduCB, _pDmsCB, _pDpsCB, _majoritySize() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed insert record[%s] to collection[%s], "
                   "rc: %d", newCLRecordObj.toString().c_str(),
                   CAT_COLLECTION_INFO_COLLECTION, rc ) ;

      rc = catAddCL2CS( szSpace, szCollection, _pEduCB, _pDmsCB,
                        _pDpsCB, _majoritySize() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Update collection to space failed, rc: %d", rc ) ;
         goto rollback ;
      }

      if ( clInfo._autoSplit ) 
      {
         rc = _autoHashSplit( newCLRecordObj, taskIDs,
                              strGroupName.c_str(), &range ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to split collection[%s], rc:%d",
                    collectionName, rc ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_CREATECOLLECTION, rc ) ;
      return rc ;
   error :
      goto done ;
   rollback:
      catRemoveCL( collectionName , _pEduCB, _pDmsCB, _pDpsCB,
                   _majoritySize() ) ;
      groupID = CAT_INVALID_GROUPID ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_BUILDCATALOGRECORD, "catCatalogueManager::_buildCatalogRecord" )
   INT32 catCatalogueManager::_buildCatalogRecord( const catCollectionInfo & clInfo,
                                                   UINT32 mask,
                                                   INT32 groupID,
                                                   const CHAR *groupName,
                                                   BSONObj & catRecord )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_BUILDCATALOGRECORD ) ;

      BSONObjBuilder builder ;

      if ( mask & CAT_MASK_CLNAME )
      {
         builder.append( CAT_CATALOGNAME_NAME, clInfo._pCLName ) ;
      }

      builder.append( CAT_CATALOGVERSION_NAME,
                      0 == clInfo._version ?
                      CAT_VERSION_BEGIN :
                      clInfo._version ) ;

      if ( mask & CAT_MASK_REPLSIZE )
      {
         builder.append( CAT_CATALOG_W_NAME, clInfo._replSize ) ;
      }

      if ( mask & CAT_MASK_COMPRESSED )
      {
         UINT32 attr = 0 ;
         if ( clInfo._isCompressed )
         {
            attr |= DMS_MB_ATTR_COMPRESSED ;
         }
         builder.append( CAT_ATTRIBUTE_NAME, attr ) ;
      }
      if ( mask & CAT_MASK_SHDKEY )
      {
         builder.append( CAT_SHARDINGKEY_NAME, clInfo._shardingKey ) ;
         builder.appendBool( CAT_ENSURE_SHDINDEX, clInfo._enSureShardIndex ) ;
         builder.append( CAT_SHARDING_TYPE, clInfo._pShardingType ) ;
         if( clInfo._isHash )
         {
            builder.append( CAT_SHARDING_PARTITION, clInfo._shardPartition ) ;
         }
      }
      if ( clInfo._isMainCL )
      {
         builder.appendBool( CAT_IS_MAINCL, clInfo._isMainCL );
         BSONObjBuilder sub( builder.subarrayStart( CAT_CATALOGINFO_NAME ) ) ;
         sub.done() ;
      }
      else
      {
         BSONObjBuilder sub( builder.subarrayStart( CAT_CATALOGINFO_NAME ) ) ;
         BSONObjBuilder cataItemBd ( sub.subobjStart ( sub.numStr(0) ) ) ;
         cataItemBd.append ( CAT_CATALOGGROUPID_NAME, groupID ) ;
         if ( groupName )
         {
            cataItemBd.append ( CAT_GROUPNAME_NAME, groupName ) ;
         }
         if ( clInfo._isSharding )
         {
            BSONObj lowBound, upBound ;

            if ( !clInfo._isHash )
            {
               Ordering order = Ordering::make( clInfo._shardingKey ) ;
               rc = _buildInitBound ( clInfo._shardingKey.nFields(), order ,
                                      lowBound, upBound ) ;
            }
            else
            {
               rc =_buildHashBound( lowBound, upBound, clInfo._shardPartition ) ;
            }
            PD_RC_CHECK( rc, PDERROR, "Build cata info bound failed, rc: %d", rc ) ;

            cataItemBd.append ( CAT_LOWBOUND_NAME, lowBound ) ;
            cataItemBd.append ( CAT_UPBOUND_NAME, upBound ) ;
         }
         cataItemBd.done () ;
         sub.done () ;
      }

      if ( mask & CAT_MASK_AUTOASPLIT )
      {
         builder.appendBool ( CAT_DOMAIN_AUTO_SPLIT, clInfo._autoSplit ) ;
      }

      if ( mask & CAT_MASK_AUTOREBALAN )
      {
         builder.appendBool ( CAT_DOMAIN_AUTO_REBALANCE, clInfo._autoRebalance ) ;
      }

      if ( mask & CAT_MASK_AUTOINDEXID )
      {
         builder.appendBool( CAT_AUTO_INDEX_ID, clInfo._autoIndexId ) ;
      }

      catRecord = builder.obj () ;
   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_BUILDCATALOGRECORD, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_BUILDINITBOUND, "catCatalogueManager::_buildInitBound" )
   INT32 catCatalogueManager::_buildInitBound ( UINT32 fieldNum,
                                                const Ordering & order,
                                                BSONObj & lowBound,
                                                BSONObj & upBound )
   {
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_BUILDINITBOUND ) ;
      PD_TRACE1 ( SDB_CATALOGMGR_BUILDINITBOUND,
                  PD_PACK_UINT ( fieldNum ) ) ;
      BSONObjBuilder lowBoundBD ;
      BSONObjBuilder upBoundBD ;

      UINT32 index = 0 ;
      while ( index < fieldNum )
      {
         if ( order.get( (int)index ) == 1 )
         {
            lowBoundBD.appendMinKey ( "" ) ;
            upBoundBD.appendMaxKey ( "" ) ;
         }
         else
         {
            lowBoundBD.appendMaxKey ( "" ) ;
            upBoundBD.appendMinKey ( "" ) ;
         }
         ++index ;
      }
      lowBound = lowBoundBD.obj () ;
      upBound = upBoundBD.obj () ;
      PD_TRACE_EXIT ( SDB_CATALOGMGR_BUILDINITBOUND ) ;
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_PROCESSMSG, "catCatalogueManager::processMsg" )
   INT32 catCatalogueManager::processMsg( const NET_HANDLE &handle,
                                          MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_PROCESSMSG ) ;
      PD_TRACE1 ( SDB_CATALOGMGR_PROCESSMSG,
                  PD_PACK_INT ( pMsg->opCode ) ) ;

      switch ( pMsg->opCode )
      {
      case MSG_CAT_CREATE_COLLECTION_REQ :
      case MSG_CAT_CREATE_COLLECTION_SPACE_REQ :
      case MSG_CAT_SPLIT_PREPARE_REQ :
      case MSG_CAT_SPLIT_READY_REQ :
      case MSG_CAT_SPLIT_CANCEL_REQ :
      case MSG_CAT_SPLIT_START_REQ :
      case MSG_CAT_SPLIT_CHGMETA_REQ :
      case MSG_CAT_SPLIT_CLEANUP_REQ :
      case MSG_CAT_SPLIT_FINISH_REQ :
      case MSG_CAT_QUERY_SPACEINFO_REQ :
      case MSG_CAT_DROP_COLLECTION_REQ :
      case MSG_CAT_CRT_PROCEDURES_REQ :
      case MSG_CAT_RM_PROCEDURES_REQ :
      case MSG_CAT_DROP_SPACE_REQ :
      case MSG_CAT_LINK_CL_REQ :
      case MSG_CAT_UNLINK_CL_REQ :
      case MSG_CAT_CREATE_DOMAIN_REQ :
      case MSG_CAT_DROP_DOMAIN_REQ :
      case MSG_CAT_ALTER_DOMAIN_REQ :
      case MSG_CAT_ALTER_COLLECTION_REQ:
         {
            rc = processCommandMsg( handle, pMsg, TRUE ) ;
            break;
         }
      case MSG_CAT_QUERY_CATALOG_REQ:
         {
            rc = processQueryCatalogue( handle, pMsg ) ;
            break;
         }
      case MSG_CAT_QUERY_TASK_REQ:
         {
            rc = processQueryTask ( handle, pMsg ) ;
            break ;
         }
      default:
         {
            rc = SDB_UNKNOWN_MESSAGE;
            PD_LOG( PDWARNING, "received unknown message (opCode: [%d]%u)",
                    IS_REPLY_TYPE(pMsg->opCode),
                    GET_REQUEST_TYPE(pMsg->opCode) ) ;
            break;
         }
      }
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_PROCESSMSG, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_PROCESSCOMMANDMSG, "catCatalogueManager::processCommandMsg" )
   INT32 catCatalogueManager::processCommandMsg( const NET_HANDLE &handle,
                                                 MsgHeader *pMsg,
                                                 BOOLEAN writable )
   {
      INT32 rc = SDB_OK ;
      MsgOpQuery *pQueryReq = (MsgOpQuery *)pMsg ;

      PD_TRACE_ENTRY ( SDB_CATALOGMGR_PROCESSCOMMANDMSG ) ;
      MsgOpReply replyHeader ;
      INT32      opCode = pQueryReq->header.opCode ;
      CHAR       *replyData = NULL ;
      UINT32     replyDataLen = 0 ;
      INT32      returnNum    = 0 ;
      BOOLEAN    fillPeerRouteID = FALSE ;

      INT32 flag = 0 ;
      CHAR *pCMDName = NULL ;
      INT64 numToSkip = 0 ;
      INT64 numToReturn = 0 ;
      CHAR *pQuery = NULL ;
      CHAR *pFieldSelector = NULL ;
      CHAR *pOrderBy = NULL ;
      CHAR *pHint = NULL ;

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;
      _fillRspHeader( &(replyHeader.header), &(pQueryReq->header) ) ;

      if ( MSG_CAT_SPLIT_START_REQ == opCode ||
           MSG_CAT_SPLIT_CHGMETA_REQ == opCode ||
           MSG_CAT_SPLIT_CLEANUP_REQ == opCode ||
           MSG_CAT_SPLIT_FINISH_REQ == opCode )
      {
         fillPeerRouteID = TRUE ;
      }

      rc = msgExtractQuery( (CHAR*)pMsg, &flag, &pCMDName, &numToSkip,
                            &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to extract query msg, rc: %d", rc ) ;

      if ( writable && !pmdIsPrimary() )
      {
         rc = SDB_CLS_NOT_PRIMARY ;
         PD_LOG ( PDWARNING, "Service deactive but received command: %s,"
                  "opCode: %d", pCMDName, pQueryReq->header.opCode ) ;
         goto error ;
      }

      switch ( pQueryReq->header.opCode )
      {
         case MSG_CAT_CREATE_COLLECTION_REQ :
            rc = processCmdCreateCL( pQuery, &replyData,
                                     replyDataLen, returnNum ) ;
            break ;
         case MSG_CAT_CREATE_COLLECTION_SPACE_REQ :
            rc = processCmdCreateCS( pQuery, &replyData,
                                     replyDataLen, returnNum ) ;
            break ;
         case MSG_CAT_SPLIT_PREPARE_REQ :
         case MSG_CAT_SPLIT_READY_REQ :
         case MSG_CAT_SPLIT_CANCEL_REQ :
         case MSG_CAT_SPLIT_START_REQ :
         case MSG_CAT_SPLIT_CHGMETA_REQ :
         case MSG_CAT_SPLIT_CLEANUP_REQ :
         case MSG_CAT_SPLIT_FINISH_REQ :
            rc = processCmdSplit( pQuery, pQueryReq->header.opCode,
                                  &replyData, replyDataLen, returnNum ) ;
            break ;
         case MSG_CAT_QUERY_SPACEINFO_REQ :
            rc = processCmdQuerySpaceInfo( pQuery, &replyData, replyDataLen,
                                           returnNum ) ;
            break ;
         case MSG_CAT_DROP_COLLECTION_REQ :
            rc = processCmdDropCollection( pQuery, pQueryReq->version ) ;
            break ;
         case MSG_CAT_DROP_SPACE_REQ :
            rc = processCmdDropCollectionSpace( pQuery ) ;
            break ;
         case MSG_CAT_ALTER_COLLECTION_REQ :
            rc = processAlterCollection( pQuery, &replyData,
                                         replyDataLen, returnNum ) ;
            break ;
         case MSG_CAT_CRT_PROCEDURES_REQ :
            rc = processCmdCrtProcedures( pQuery ) ;
            break ;
         case MSG_CAT_RM_PROCEDURES_REQ :
            rc = processCmdRmProcedures( pQuery ) ;
            break ;
         case MSG_CAT_LINK_CL_REQ :
            rc = processCmdLinkCollection( pQuery, &replyData,
                                          replyDataLen, returnNum );
            break;
         case MSG_CAT_UNLINK_CL_REQ :
            rc = processCmdUnlinkCollection( pQuery, &replyData,
                                          replyDataLen, returnNum );
            break;
         case MSG_CAT_CREATE_DOMAIN_REQ :
            rc = processCmdCreateDomain ( pQuery ) ;
            break ;
         case MSG_CAT_DROP_DOMAIN_REQ :
            rc = processCmdDropDomain ( pQuery ) ;
            break ;
         case MSG_CAT_ALTER_DOMAIN_REQ :
            rc = processCmdAlterDomain ( pQuery ) ;
            break ;
         default :
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "Recieved unknow command: %s, opCode: %d",
                    pCMDName, pQueryReq->header.opCode ) ;
            break ;
      }

      PD_RC_CHECK( rc, PDERROR, "Process command[%s] failed, opCode: %d, "
                   "rc: %d", pCMDName, pQueryReq->header.opCode, rc ) ;

   done:
      if ( fillPeerRouteID )
      {
         replyHeader.header.routeID.value = pQueryReq->header.routeID.value ;
      }

      if ( 0 == replyDataLen )
      {
         rc = _pCatCB->netWork()->syncSend( handle, (void*)&replyHeader ) ;
      }
      else
      {
         replyHeader.header.messageLength += replyDataLen ;
         replyHeader.numReturned = returnNum ;
         rc = _pCatCB->netWork()->syncSend( handle, &(replyHeader.header),
                                            (void*)replyData, replyDataLen ) ;
      }
      if ( replyData )
      {
         SDB_OSS_FREE( replyData ) ;
      }
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_PROCESSCOMMANDMSG, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   void catCatalogueManager::_fillRspHeader( MsgHeader * rspMsg,
                                             const MsgHeader * reqMsg )
   {
      rspMsg->opCode = MAKE_REPLY_TYPE( reqMsg->opCode ) ;
      rspMsg->requestID = reqMsg->requestID ;
      rspMsg->routeID.value = 0 ;
      rspMsg->TID = reqMsg->TID ;
   }

   INT32 catCatalogueManager::_sendFailedRsp( NET_HANDLE handle,
                                              INT32 res,
                                              MsgHeader * reqMsg )
   {
      MsgInternalReplyHeader reply ;
      reply.res = res ;
      reply.header.messageLength = sizeof( MsgInternalReplyHeader ) ;
      _fillRspHeader( &(reply.header), reqMsg ) ;
      return _pCatCB->netWork()->syncSend( handle, (void*)&reply ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__BUILDHASHBOUND, "catCatalogueManager::_buildHashBound" )
   INT32 catCatalogueManager::_buildHashBound( BSONObj& lowBound,
                                               BSONObj& upBound,
                                               INT32 paritition )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATALOGMGR__BUILDHASHBOUND ) ;

      lowBound = BSON("" << CAT_HASH_LOW_BOUND ) ;
      upBound = BSON("" << paritition )  ;

      PD_TRACE_EXITRC( SDB_CATALOGMGR__BUILDHASHBOUND, rc ) ;
      return rc ;
   }

   INT16 catCatalogueManager::_majoritySize()
   {
      return _pCatCB->majoritySize() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_CMDLINKCOLLECTION, "catCatalogueManager::processCmdLinkCollection" )
   INT32 catCatalogueManager::processCmdLinkCollection( const CHAR *pQuery,
                                                        CHAR **ppReplyBody,
                                                        UINT32 &replyBodyLen,
                                                        INT32 &returnNum )
   {
      INT32 rc = SDB_OK;
      std::string strMainCLName;
      std::string strSubCLName;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_CMDLINKCOLLECTION ) ;
      BSONObj boLowBound;
      BSONObj boUpBound;
      std::vector<UINT32>  groupList;
      try
      {
         BSONObj boQuery( pQuery );
         BSONElement beMainCLName = boQuery.getField( CAT_COLLECTION_NAME );
         PD_CHECK( beMainCLName.type() == String, SDB_INVALIDARG, error,
                   PDERROR, "failed to link the collection, get field(%s) "
                   "failed!", CAT_COLLECTION_NAME );
         strMainCLName = beMainCLName.str();
         PD_CHECK( !strMainCLName.empty(), SDB_INVALIDARG, error, PDERROR,
                   "invalid field:%s", CAT_COLLECTION_NAME );

         {
         BSONElement beSubCLName = boQuery.getField( CAT_SUBCL_NAME );
         PD_CHECK( beSubCLName.type() == String, SDB_INVALIDARG, error, PDERROR,
                   "failed to link the collection, get field(%s) failed!",
                   CAT_SUBCL_NAME );
         strSubCLName = beSubCLName.str();
         PD_CHECK( !strSubCLName.empty(), SDB_INVALIDARG, error, PDERROR,
                   "invalid field:%s", CAT_SUBCL_NAME );
         }

         {
         BSONElement beLowBound = boQuery.getField( CAT_LOWBOUND_NAME );
         PD_CHECK( beLowBound.type() == Object, SDB_INVALIDARG, error, PDERROR,
                   "invalid field:%s", CAT_LOWBOUND_NAME );
         boLowBound = beLowBound.embeddedObject();
         }

         {
         BSONElement beUpBound = boQuery.getField( CAT_UPBOUND_NAME );
         PD_CHECK( beUpBound.type() == Object, SDB_INVALIDARG, error, PDERROR,
                   "invalid field:%s", CAT_UPBOUND_NAME );
         boUpBound = beUpBound.embeddedObject();
         }

         rc = catLinkCL( strMainCLName.c_str(), strSubCLName.c_str(),
                        boLowBound, boUpBound, _pEduCB, _pDmsCB,
                        _pDpsCB, _majoritySize(), groupList );
         PD_RC_CHECK( rc, PDERROR,
                      "failed to link the sub-collection(%s) "
                      "to main-collection(%s)(rc=%d)",
                      strMainCLName.c_str(), strSubCLName.c_str(), rc );

         {
         returnNum = 1;
         BSONArrayBuilder babGroup;
         UINT32 i = 0;
         for( ; i < groupList.size(); i++ )
         {
            BSONObj boTmp = BSON( CAT_GROUPID_NAME << groupList[i] );
            babGroup.append( boTmp );
         }
         BSONObjBuilder bobGroup;
         bobGroup.appendArray( CAT_GROUP_NAME, babGroup.arr() );
         BSONObj boGroup = bobGroup.obj();
         replyBodyLen = boGroup.objsize();
         *ppReplyBody = ( CHAR *)SDB_OSS_MALLOC( replyBodyLen );
         PD_CHECK( *ppReplyBody, SDB_OOM, error, PDERROR,
                   "malloc failed(size=%d)", replyBodyLen );
         ossMemcpy( *ppReplyBody, boGroup.objdata(), replyBodyLen );
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_CMDLINKCOLLECTION, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_CMDUNLINKCOLLECTION, "catCatalogueManager::processCmdUnlinkCollection" )
   INT32 catCatalogueManager::processCmdUnlinkCollection( const CHAR *pQuery,
                                                          CHAR **ppReplyBody,
                                                          UINT32 &replyBodyLen,
                                                          INT32 &returnNum )
   {
      INT32 rc = SDB_OK;
      std::string strMainCLName;
      std::string strSubCLName;
      std::vector<UINT32>  groupList;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_CMDUNLINKCOLLECTION ) ;
      try
      {
         BSONObj boQuery( pQuery );
         BSONElement beMainCLName = boQuery.getField( CAT_COLLECTION_NAME );
         PD_CHECK( beMainCLName.type() == String, SDB_INVALIDARG, error,
                   PDERROR, "failed to link the collection, get field(%s) "
                   "failed!", CAT_COLLECTION_NAME );
         strMainCLName = beMainCLName.str();
         PD_CHECK( !strMainCLName.empty(), SDB_INVALIDARG, error, PDERROR,
                   "invalid field:%s", CAT_COLLECTION_NAME );

         {
         BSONElement beSubCLName = boQuery.getField( CAT_SUBCL_NAME );
         PD_CHECK( beSubCLName.type() == String, SDB_INVALIDARG, error, PDERROR,
                   "failed to link the collection, get field(%s) failed!",
                   CAT_SUBCL_NAME );
         strSubCLName = beSubCLName.str();
         PD_CHECK( !strSubCLName.empty(), SDB_INVALIDARG, error, PDERROR,
                   "invalid field:%s", CAT_SUBCL_NAME );
         }

         rc = catUnlinkCL( strMainCLName.c_str(), strSubCLName.c_str(),
                           _pEduCB, _pDmsCB, _pDpsCB, _majoritySize(),
                           groupList );
         PD_RC_CHECK( rc, PDERROR,
                      "failed to unlink the sub-collection(%s) "
                      "from main-collection(%s)(rc=%d)",
                      strMainCLName.c_str(), strSubCLName.c_str(), rc );

         {
         returnNum = 1;
         BSONArrayBuilder babGroup;
         UINT32 i = 0;
         for( ; i < groupList.size(); i++ )
         {
            BSONObj boTmp = BSON( CAT_GROUPID_NAME << groupList[i] );
            babGroup.append( boTmp );
         }
         BSONObjBuilder bobGroup;
         bobGroup.appendArray( CAT_GROUP_NAME, babGroup.arr() );
         BSONObj boGroup = bobGroup.obj();
         replyBodyLen = boGroup.objsize();
         *ppReplyBody = ( CHAR *)SDB_OSS_MALLOC( replyBodyLen );
         PD_CHECK( *ppReplyBody, SDB_OOM, error, PDERROR,
                   "malloc failed(size=%d)", replyBodyLen );
         ossMemcpy( *ppReplyBody, boGroup.objdata(), replyBodyLen );
         }

      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_CMDUNLINKCOLLECTION, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_CREATEDOMAIN, "catCatalogueManager::processCmdCreateDomain" )
   INT32 catCatalogueManager::processCmdCreateDomain ( const CHAR *pQuery )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_CREATEDOMAIN ) ;
      try
      {
         BSONObj tempObj ;
         BSONObj queryObj ;
         BSONObj insertObj ;
         BSONObj boQuery( pQuery );
         BSONObjBuilder ob ;
         BSONElement beDomainOptions ;
         const CHAR *pDomainName = NULL ;
         INT32 expectedObjSize   = 0 ;
         BSONElement beDomainName = boQuery.getField ( CAT_DOMAINNAME_NAME ) ;
         PD_CHECK( beDomainName.type() == String, SDB_INVALIDARG, error,
                   PDERROR, "failed to create domain, get field(%s) "
                   "failed!", CAT_DOMAINNAME_NAME );
         pDomainName = beDomainName.valuestr() ;
         PD_TRACE1 ( SDB_CATALOGMGR_CREATEDOMAIN, PD_PACK_STRING(pDomainName) ) ;
         rc = catDomainNameValidate ( pDomainName ) ;
         PD_CHECK ( SDB_OK == rc, rc, error, PDERROR,
                    "Invalid domain name: %s, rc = %d", pDomainName, rc ) ;
         ob.append ( CAT_DOMAINNAME_NAME, pDomainName ) ;
         expectedObjSize ++ ;
         beDomainOptions = boQuery.getField ( CAT_OPTIONS_NAME ) ;
         if ( !beDomainOptions.eoo() && beDomainOptions.type() != Object )
         {
            PD_LOG ( PDERROR,
                     "Invalid options type, expected eoo or object" ) ;
            rc = SDB_INVALIDARG ;
         }
         if ( beDomainOptions.type() == Object )
         {
            rc = catDomainOptionsExtract ( beDomainOptions.embeddedObject(),
                                            _pEduCB,
                                           &ob ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to validate domain options, rc = %d",
                        rc ) ;
               goto error ;
            }
            expectedObjSize ++ ;
         }
         if ( boQuery.nFields() != expectedObjSize )
         {
            PD_LOG ( PDERROR, "Actual input doesn't match expected opt size, "
                     "there could be one or more invalid arguments" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         insertObj = ob.obj () ;
         rc = rtnInsert ( CAT_DOMAIN_COLLECTION, insertObj, 1,
                          0, _pEduCB ) ;
         if ( rc )
         {
            if ( SDB_IXM_DUP_KEY == rc )
            {
               PD_LOG ( PDERROR, "Domain %s is already exist",
                        pDomainName ) ;
               rc = SDB_CAT_DOMAIN_EXIST ;
               goto error ;
            }
            else
            {
               PD_LOG ( PDERROR,
                        "Failed to insert domain object into %s, rc = %d",
                        CAT_DOMAIN_COLLECTION, rc ) ;
               goto error ;
            }
         }

         PD_LOG( PDEVENT, "create domain[%s]",
                 insertObj.toString( FALSE, TRUE ).c_str() ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_CREATEDOMAIN, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_DROPDOMAIN, "catCatalogueManager::processCmdDropDomain" )
   INT32 catCatalogueManager::processCmdDropDomain ( const CHAR *pQuery )
   {
      INT32 rc                = SDB_OK ;
      const CHAR *pDomainName = NULL ;
      INT64 numDeleted        = 0 ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_DROPDOMAIN ) ;
      try
      {
         BSONObj tempObj ;
         BSONObj queryObj ;
         BSONObj resultObj ;
         BSONObj boQuery( pQuery );
         BSONElement beDomainName = boQuery.getField( CAT_DOMAINNAME_NAME );
         PD_CHECK( beDomainName.type() == String, SDB_INVALIDARG, error,
                   PDERROR, "failed to drop domain, get field(%s) "
                   "failed!", CAT_DOMAINNAME_NAME );
         pDomainName = beDomainName.valuestr() ;
         PD_TRACE1 ( SDB_CATALOGMGR_DROPDOMAIN, PD_PACK_STRING(pDomainName) ) ;
         queryObj = BSON ( CAT_DOMAIN_NAME << pDomainName ) ;
         rc = catGetOneObj ( CAT_COLLECTION_SPACE_COLLECTION, tempObj,
                             queryObj, tempObj, _pEduCB, resultObj ) ;
         if ( SDB_DMS_EOC != rc )
         {
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get object from %s, rc = %d",
                        CAT_COLLECTION_SPACE_COLLECTION, rc ) ;
               goto error ;
            }
            else
            {
               rc = SDB_DOMAIN_IS_OCCUPIED ;
               PD_LOG ( PDERROR, "There are one or more collection spaces "
                        "are using the domain, rc = %d", rc ) ;
               goto error ;
            }
         }
         queryObj = BSON ( CAT_DOMAINNAME_NAME << pDomainName ) ;
         rc = rtnDelete ( CAT_DOMAIN_COLLECTION, queryObj,
                          tempObj, 0, _pEduCB, &numDeleted ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to drop domain %s, rc = %d",
                     pDomainName, rc ) ;
            goto error ;
         }
         if ( 0 == numDeleted )
         {
            PD_LOG ( PDERROR, "Domain %s does not exist",
                     pDomainName ) ;
            rc = SDB_CAT_DOMAIN_NOT_EXIST ;
            goto error ;
         }

         PD_LOG( PDEVENT, "drop domain[%s]", pDomainName ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_DROPDOMAIN, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_ALTERDOMAIN, "catCatalogueManager::processCmdAlterDomain" )
   INT32 catCatalogueManager::processCmdAlterDomain ( const CHAR *pQuery )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGMGR_ALTERDOMAIN ) ;
      BSONObj alterObj( pQuery ) ;
      BSONElement eleDomainName ;
      BSONObj domainObj ;
      BSONElement eleOptions ;
      BSONObjBuilder alterBuilder ;
      BSONObjBuilder reqBuilder ;
      BSONObj objReq ;


      eleDomainName = alterObj.getField( CAT_DOMAINNAME_NAME ) ;
      if ( String != eleDomainName.type() )
      {
         PD_LOG( PDERROR, "can not find valid [%s] in alter req [%s]",
                 CAT_DOMAINNAME_NAME, alterObj.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      eleOptions = alterObj.getField( CAT_OPTIONS_NAME ) ;
      if ( Object != eleOptions.type() )
      {
         PD_LOG( PDERROR, "can not find valid [%s] in alter req[%s]",
                 CAT_OPTIONS_NAME, alterObj.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = catGetDomainObj( eleDomainName.valuestr(),
                            domainObj,
                            _pEduCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get domain[%s], rc:",
                 eleDomainName.valuestr(), rc  ) ;
         goto error ;
      }

      rc = catDomainOptionsExtract( eleOptions.embeddedObject(),
                                    _pEduCB,
                                    &reqBuilder ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to validate options object:%d", rc ) ;
         goto error ;
      }

      objReq = reqBuilder.obj() ;

      {
      BSONElement groups = objReq.getField( CAT_GROUPS_NAME ) ;
      if ( !groups.eoo() )
      {
         rc = _buildAlterGroups( domainObj, groups, alterBuilder ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to add groups to builder:%d", rc ) ;
            goto error ;
         }
      }
      }

      {
      BSONElement autoSplit = objReq.getField( CAT_DOMAIN_AUTO_SPLIT ) ;
      if ( !autoSplit.eoo() )
      {
         alterBuilder.append( autoSplit ) ;
      }
      }

      {
      BSONElement autoRebalance = objReq.getField( CAT_DOMAIN_AUTO_REBALANCE ) ;
      if ( !autoRebalance.eoo() )
      {
         alterBuilder.append( autoRebalance ) ;
      }
      }

      {
      BSONObjBuilder matchBuilder ;
      matchBuilder.append( eleDomainName ) ;
      BSONObj alterObj = alterBuilder.obj() ;
      BSONObj dummy ;
      rc = rtnUpdate( CAT_DOMAIN_COLLECTION,
                      matchBuilder.obj(),
                      BSON( "$set" << alterObj ),
                      dummy,
                      0, _pEduCB, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to update cata info:%d", rc ) ;
         goto error ;
      }

      PD_LOG( PDEVENT, "alter domain[%s] to[%s]",
              eleDomainName.valuestr(),
              alterObj.toString( FALSE, TRUE ).c_str() ) ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CATALOGMGR_ALTERDOMAIN, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   static INT32 _findGroupWillBeRemoved( const map<string, INT32> &groupsInDomain,
                                         const BSONElement &groupsInReq,
                                         map<string, INT32> &removed )
   {
      INT32 rc = SDB_OK ;
      map<string, INT32>::const_iterator itr = groupsInDomain.begin() ;
      for ( ; itr != groupsInDomain.end(); itr++ )
      {
         BOOLEAN found = FALSE ;
         BSONObjIterator i( groupsInReq.embeddedObject() ) ;
         while ( i.more() )
         {
            BSONElement ele = i.next() ;
            if ( Object != ele.type() )
            {
               PD_LOG( PDERROR, "invalid groups info[%s]. it should be like",
                       " {GroupID:int, GroupName:string}",
                       groupsInReq.toString().c_str() ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            {
            BSONElement groupID =
                    ele.embeddedObject().getField( CAT_GROUPID_NAME ) ;
            if ( NumberInt != groupID.type() )
            {
               PD_LOG( PDERROR, "invalid groups info[%s]. it should be like",
                       " {GroupID:int, GroupName:string}",
                       groupsInReq.toString().c_str() ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            if ( groupID.Int() ==  itr->second )
            {
               found = TRUE ;
               break ;
            }
            }
         }
         
         if ( !found )
         {
            removed.insert( std::make_pair( itr->first, itr->second ) ) ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__BUILDALTERGROUPS, "catCatalogueManager::_buildAlterGroups" )
   INT32 catCatalogueManager::_buildAlterGroups( const BSONObj &domain,
                                                 const BSONElement &ele,
                                                 BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATALOGMGR__BUILDALTERGROUPS ) ;
      map<string, INT32> groupsInDomain ;
      map<string, INT32> toBeRemoved ;
      BSONObj objToBeRemoved ;
      BSONArrayBuilder arrBuilder ;
      BSONObjBuilder inBuilder ;
      BSONObj condition ;
      BSONObj dummy ;
      BSONObj res ;

      rc = catGetDomainGroups( domain, groupsInDomain ) ;
      if ( SDB_CAT_NO_GROUP_IN_DOMAIN == rc )
      {
         rc = SDB_OK ;
      }
      else if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get groups from domain object:%d", rc ) ;
         goto error ;
      }

      rc = _findGroupWillBeRemoved( groupsInDomain,
                                    ele,
                                    toBeRemoved ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get the groups those to be removed:%d", rc) ;
         goto error ;
      }

      for ( map<string, INT32>::const_iterator itr = toBeRemoved.begin();
            itr != toBeRemoved.end();
            itr++ )
      {
         arrBuilder << itr->second ;
      }

      if ( !toBeRemoved.empty() )
      {
         objToBeRemoved = arrBuilder.arr() ;
         inBuilder.appendArray( "$in", objToBeRemoved ) ;
         condition = BSON( CAT_DOMAIN_NAME <<
                           domain.getField( CAT_DOMAINNAME_NAME ).valuestrsafe() <<
                           CAT_GROUP_NAME"."CAT_GROUPID_NAME <<
                           inBuilder.obj()  ) ;
         rc = catGetOneObj( CAT_COLLECTION_SPACE_COLLECTION,
                            dummy, condition, dummy, _pEduCB, res ) ;
         if ( SDB_OK == rc )
         {
            PD_LOG( PDERROR, "clear data(of this domain) before remove it "
                    "from domain. groups to be removed[%s]",
                    objToBeRemoved.toString( TRUE, TRUE ).c_str() ) ;
            rc = SDB_DOMAIN_IS_OCCUPIED ;
            goto error ;
         }
         else if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_OK ;
         }
         else
         {
            PD_LOG( PDERROR, "unexpected err happened:%d", rc ) ;
            goto error ;
         }
      }

      builder.append( ele ) ;
   done:
      PD_TRACE_EXITRC( SDB_CATALOGMGR__BUILDALTERGROUPS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__CHOOSEFGROUPOFCL, "catCatalogueManager::_chooseGroupOfCl" )
   INT32 catCatalogueManager::_chooseGroupOfCl( const BSONObj &domainObj,
                                                const BSONObj &csObj,
                                                const catCollectionInfo &clInfo,
                                                std::string &groupName,
                                                INT32 &groupID,
                                                std::map<string, INT32> &splitRange )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATALOGMGR__CHOOSEFGROUPOFCL ) ;
      BSONObj gpObj ;
      const CHAR *domain = NULL ;
      BOOLEAN isSysDomain = domainObj.isEmpty() ;
      std::map<string, INT32> groupsOfDomain ;

      if ( NULL != clInfo._gpSpecified )
      {
         if ( !isSysDomain )
         {
            rc = catGetDomainGroups( domainObj, groupsOfDomain ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to get groups from domain "
                         "info[%s], rc: %d", domainObj.toString().c_str(),
                         rc ) ;
            {
               map<string, INT32>::const_iterator itr =
                  groupsOfDomain.find( clInfo._gpSpecified ) ;
               if ( groupsOfDomain.end() == itr )
               {
                  PD_LOG( PDERROR, "[%s] is not a group of domain [%s]",
                          clInfo._gpSpecified, domain ) ;
                  rc = SDB_CAT_GROUP_NOT_IN_DOMAIN ;
                  goto error ;
               }

               groupID = itr->second ;
            }
         }
         else
         {
            rc = catGetGroupObj( clInfo._gpSpecified, TRUE, gpObj, _pEduCB ) ;
            PD_RC_CHECK( rc, PDERROR, "Get group[%s] info failed, rc: %d",
                         clInfo._gpSpecified, rc ) ;
            rc = rtnGetIntElement( gpObj, CAT_GROUPID_NAME, groupID ) ;
            PD_RC_CHECK( rc, PDERROR, "Get groupid of group[%s] info failed, "
                         "rc: %d", clInfo._gpSpecified, rc ) ;
         }

         groupName.assign( clInfo._gpSpecified ) ;
      }
      else
      {
         vector< INT32 > vecGroupID ;

         if ( ASSIGN_FOLLOW == clInfo._assignType )
         {
            BSONElement ele = csObj.getField( CAT_COLLECTION_SPACE_NAME ) ;
            rc = catGetCSGroupsFromCLs( ele.valuestrsafe(), _pEduCB,
                                        vecGroupID ) ;
            if ( rc )
            {
               PD_LOG( PDERROR, "Get collection space[%s] all groups failed, "
                       "rc: %d", csObj.toString().c_str(), rc ) ;
               goto error ;
            }
         }

         if ( 0 == vecGroupID.size() && !isSysDomain )
         {
            rc = catGetDomainGroups( domainObj, vecGroupID ) ;
            if ( rc )
            {
               PD_LOG( PDERROR, "Get groups from domain obj[%s] failed, "
                       "rc: %d", domainObj.toString().c_str(), rc ) ;
               goto error ;
            }
         }

         rc = _assignGroup( &vecGroupID, groupID ) ;
         PD_RC_CHECK( rc, PDERROR, "Assign group for collection[%s] "
                      "failed, rc: %d", clInfo._pCLName, rc ) ;

         rc = catGroupID2Name( groupID, groupName, _pEduCB ) ;
         PD_RC_CHECK( rc, PDERROR, "Group id[%d] to group name failed, "
                      "rc: %d", groupID, rc ) ;

         if ( !isSysDomain )
         {
            rc = catGetDomainGroups( domainObj, splitRange ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to get groups from domain "
                         "info[%s], rc: %d", domainObj.toString().c_str(),
                         rc ) ;
         }
      }

      SDB_ASSERT( CAT_INVALID_GROUPID != groupID, "can not be invalid" ) ;
   done:
      PD_TRACE_EXITRC( SDB_CATALOGMGR__CHOOSEFGROUPOFCL, rc ) ;
      return rc ;
   error:
      groupID = CAT_INVALID_GROUPID ;
      groupName.clear() ;
      splitRange.clear() ; 
      goto done ;
   }

   static INT32 getBoundFromClObj( const BSONObj &clObj,
                                   UINT32 &totalBound )
   {
      INT32 rc = SDB_OK ;
      BSONElement upBound =
            clObj.getFieldDotted(CAT_CATALOGINFO_NAME".0."CAT_UPBOUND_NAME);
      if ( Object != upBound.type() )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      {
      BSONElement first = upBound.embeddedObject().firstElement() ;
      if ( NumberInt != first.type() )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      totalBound = first.Int() ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR_AUTOHASHSPLIT, "catCatalogueManager::_autoHashSplit" )
   INT32 catCatalogueManager::_autoHashSplit( const BSONObj &clObj,
                                              std::vector<UINT64> &taskIDs,
                                              const CHAR *srcGroupName,
                                              const map<string, INT32> *range )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATALOGMGR_AUTOHASHSPLIT ) ;
      const CHAR *srcGroup = NULL ;
      const map<string, INT32> *dstGroups = NULL ;
      BSONObj splitInfo ;
      const CHAR *fullName = NULL ;
      UINT32 totalBound = 0 ;


      BSONElement eleName = clObj.getField( CAT_CATALOGNAME_NAME ) ;
      if ( String != eleName.type() )
      {
         SDB_ASSERT( FALSE, "impossible" ) ;
         PD_LOG( PDERROR, "invalid collection record:%s",
                 clObj.toString().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      fullName = eleName.valuestr() ;

      rc = getBoundFromClObj( clObj, totalBound ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get bound from cl obj[%s]",
                 clObj.toString().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      

      if ( NULL == srcGroupName )
      {
         SDB_ASSERT( FALSE, "impossible" ) ;
      }
      else
      {
         SDB_ASSERT( NULL != range, "can not be NULL" ) ;
         srcGroup = srcGroupName ;
         dstGroups = range ;
      }

      if ( 1 < dstGroups->size() )
      {
         INT32 tmpID ;
         UINT64 taskID = CLS_INVALID_TASKID ;
         clsCatalogSet catSet( fullName ) ;
         UINT32 avgBound = totalBound / dstGroups->size() ;
         UINT32 endBound = totalBound ;
         UINT32 beginBound = totalBound - avgBound ;

         rc = catSet.updateCatSet( clObj ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to update catlogset:%d", rc ) ;
            goto error ;
         }

         {
         map<string, INT32>::const_iterator itr = dstGroups->begin() ;
         for ( ; itr != dstGroups->end(); itr++ )
         {
            if ( 0 == ossStrcmp( srcGroup, itr->first.c_str() ) )
            {
               continue ;
            }

            splitInfo = _crtSplitInfo( fullName,
                                       srcGroup,
                                       itr->first.c_str(),
                                       beginBound,
                                       endBound ) ;

            rc = catSplitReady( splitInfo, fullName,
                                &catSet, tmpID, _taskMgr,
                                _pEduCB, _majoritySize(),
                                &taskID ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to split collections[%s], rc:%d",
                       fullName, rc ) ;
               goto error ;
            }

            endBound = beginBound ;
            beginBound = endBound - avgBound ;
            taskIDs.push_back( taskID ) ;   
         }
         }
      }
      else
      {
         PD_LOG( PDINFO, "split range size:%d, do nothing.", dstGroups->size() ) ;
      }
   done:
      PD_TRACE_EXITRC( SDB_CATALOGMGR_AUTOHASHSPLIT, rc ) ;
      return rc ;
   error:
      {
      std::vector<UINT64>::const_iterator itr = taskIDs.begin() ;
      for ( ; itr != taskIDs.end(); itr++ )
      {
         catRemoveTask( *itr, _pEduCB, _majoritySize() ) ;
      }

      taskIDs.clear() ;
      }
      goto done ;
   }

   BSONObj catCatalogueManager::_crtSplitInfo( const CHAR *fullName,
                                               const CHAR *src,
                                               const CHAR *dst,
                                               UINT32 begin,
                                               UINT32 end )
   {
      SDB_ASSERT( NULL != fullName && NULL != src && NULL != dst,
                  "can not be NULL" ) ;
      BSONObj obj = BSON ( CAT_COLLECTION_NAME << fullName <<
                           CAT_SOURCE_NAME << src <<
                           CAT_TARGET_NAME << dst <<
                           CAT_SPLITVALUE_NAME << BSON( "" << begin ) <<
                           CAT_SPLITENDVALUE_NAME << BSON( "" << end ) ) ;
      return obj ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__COMBINEOPTIONS, "catCatalogueManager::_combineOptions" )
   INT32 catCatalogueManager::_combineOptions( const BSONObj &domain,
                                               const BSONObj &cs,
                                               UINT32 &mask,
                                               catCollectionInfo &options  )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATALOGMGR__COMBINEOPTIONS ) ;
      if ( domain.isEmpty() )
      {
         goto done ;
      }

      if ( !( CAT_MASK_AUTOASPLIT & mask ) )
      {
         if ( options._isSharding && options._isHash )
         {
            BSONElement split = domain.getField( CAT_DOMAIN_AUTO_SPLIT ) ;
            if ( Bool == split.type() )
            {
               options._autoSplit = split.Bool() ;
               mask |= CAT_MASK_AUTOASPLIT ;
            }
         }
      }

      if ( !( CAT_MASK_AUTOREBALAN & mask ) )
      {
         if ( options._isSharding && options._isHash )
         {
            BSONElement rebalance = domain.getField( CAT_DOMAIN_AUTO_REBALANCE ) ;
            if ( Bool == rebalance.type() )
            {
               options._autoRebalance = rebalance.Bool() ;
               mask |= CAT_MASK_AUTOREBALAN ;
            }
         }
      }
   done:
      PD_TRACE_EXITRC( SDB_CATALOGMGR__COMBINEOPTIONS, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__BUILDALTEROBJWITHMETAANDOBJ, "catCatalogueManager::_buildAlterObjWithMetaAndObj" )
   INT32 catCatalogueManager::_buildAlterObjWithMetaAndObj( _clsCatalogSet &catSet,
                                                            UINT32 mask,
                                                            catCollectionInfo &alterInfo,
                                                            BSONObj &alterObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY(  SDB_CATALOGMGR__BUILDALTEROBJWITHMETAANDOBJ) ;
      BSONElement groupID ;
      BSONElement groupName ;
      BSONObj groupObj ;
      _clsCatalogSet::POSITION pos ;
      clsCatalogItem *item = NULL ;

      if ( ( CAT_MASK_SHDKEY & mask ) &&
           catSet.isSharding() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "can not alter a sharding collection's shardingkey" ) ;
         goto error ;
      }

      if ( CAT_MASK_ISMAINCL & mask )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "can not change a collection to a main cl" ) ;
         goto error ;
      }

     if ( CAT_MASK_COMPRESSED & mask )
     {
        rc = SDB_INVALIDARG ;
        PD_LOG( PDERROR, "can not alter attribute \"compressed\"" ) ;
        goto error ;
     }

      alterInfo._version = catSet.getVersion() ;
      ++alterInfo._version ;

      if ( catSet.isSharding() || catSet.isMainCL() )
      {
         BSONObjBuilder builder ;
         builder.append( CAT_CATALOGVERSION_NAME, alterInfo._version ) ;
         if ( mask & CAT_MASK_REPLSIZE )
         {
            builder.append( CAT_CATALOG_W_NAME, alterInfo._replSize ) ;
         }
         if ( mask & CAT_MASK_AUTOREBALAN )
         {
            builder.appendBool( CAT_DOMAIN_AUTO_REBALANCE,
                                alterInfo._autoRebalance ) ;
         }

         alterObj = builder.obj() ; 
         goto done ;
      }

      pos = catSet.getFirstItem() ;
      item = catSet.getNextItem( pos ) ;
      if ( NULL == item )
      {
         PD_LOG( PDERROR, "failed to get first item from catalogset" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = _buildCatalogRecord( alterInfo, mask, item->getGroupID(),
                                item->getGroupName().c_str(),
                                alterObj ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to build cata record:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_CATALOGMGR__BUILDALTEROBJWITHMETAANDOBJ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGMGR__GETGROUPSOFCOLLECTIONS, "catCatalogueManager::_getGroupsOfCollections" )
   INT32 catCatalogueManager::_getGroupsOfCollections(
                              const std::vector<string> &clNames,
                              BSONObj &groups )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATALOGMGR__GETGROUPSOFCOLLECTIONS ) ;
      BSONArrayBuilder builder ;
      set<INT32> pushed ;
      vector<string>::const_iterator itr = clNames.begin() ;
      for ( ; itr != clNames.end(); ++itr )
      {
         BSONObj clInfo ;
         BOOLEAN exist = FALSE ;
         BSONElement cataInfo ;
         rc = catCheckCollectionExist( itr->c_str(),
                                       exist,
                                       clInfo,
                                       _pEduCB ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to detect collection[%s]"
                    ", rc:%d", itr->c_str(), rc ) ;
            goto error ;
         }

         if ( !exist )
         {
            PD_LOG( PDERROR, "collection[%s] does not exist" ) ;
            rc = SDB_DMS_NOTEXIST ;
            goto error ;
         }

         cataInfo = clInfo.getField( CAT_CATALOGINFO_NAME ) ;
         if ( Array != cataInfo.type() )
         {
            PD_LOG( PDERROR, "invalid cl info:%s",
                    clInfo.toString( FALSE, FALSE ).c_str()) ;
            rc = SDB_SYS ;
            goto error ;
         }

         {
         BSONObjIterator i( cataInfo.embeddedObject() ) ;
         while ( i.more() )
         {
            BSONElement ele = i.next() ;
            if ( Object == ele.type() )
            {
               BSONElement groupID = ele.embeddedObject().getField( CAT_GROUPID_NAME ) ;
               if ( NumberInt == groupID.type() )
               {
                  if ( pushed.find( groupID.Int() ) == pushed.end() )
                  {
                     builder << ele ;
                     pushed.insert( groupID.Int() ) ;
                  }
               }
            }
         }
         }
      }

      groups = BSON( CAT_GROUP_NAME << builder.arr() ) ;   
   done:
      PD_TRACE_EXITRC( SDB_CATALOGMGR__GETGROUPSOFCOLLECTIONS, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

