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

   Source File Name = rtnCoordUpdate.cpp

   Descriptive Name = Runtime Coord Update

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   update operation on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoordUpdate.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "msgMessage.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "mthModifier.hpp"

using namespace bson;

namespace engine
{
   //PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOUPDATE_EXECUTE, "rtnCoordUpdate::execute" )
   INT32 rtnCoordUpdate::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                  CHAR **ppResultBuffer, pmdEDUCB *cb,
                                  MsgOpReply &replyHeader,
                                  BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      rtnCoordOperator *pRollbackOperator = NULL;
      BOOLEAN isNeedRefresh = FALSE ;
      BOOLEAN hasRefresh = FALSE ;
      CoordGroupList sendGroupLst ;
      INT64 updateNum = 0;
      BSONObj newUpdator ;

      MsgHeader*pHeader                = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_UPDATE_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      CHAR *pSelector = NULL;
      CHAR *pUpdator = NULL;
      CHAR *pHint = NULL;
      BSONObj boSelector;
      BSONObj boHint;
      BSONObj boUpdator;
      rc = msgExtractUpdate( pReceiveBuffer, &flag, &pCollectionName,
                             &pSelector, &pUpdator, &pHint );
      PD_RC_CHECK( rc, PDERROR, "failed to parse update request(rc=%d)", rc );
      try
      {
         boSelector = BSONObj( pSelector );
         boHint = BSONObj( pHint );
         boUpdator = BSONObj( pUpdator );

         if ( boUpdator.isEmpty() )
         {
            PD_LOG( PDERROR, "modifier can't be empty" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                      "Update failed, received unexpected error:%s",
                      e.what() );
      }

      do
      {
         hasRefresh = isNeedRefresh ;
         CoordCataInfoPtr cataInfo ;
         BOOLEAN hasShardingKey = FALSE ;
         CHAR *pNewMsg = NULL ;
         INT32 bufferSize = 0 ;
         INT64 numTmp = 0 ;
         newUpdator = boUpdator ;
         MsgOpUpdate *pMsgReq = (MsgOpUpdate *)pReceiveBuffer;
         rc = rtnCoordGetCataInfo( cb, pCollectionName, isNeedRefresh,
                                   cataInfo ) ;
         PD_RC_CHECK( rc, PDERROR, "Update failed, failed to get the "
                      "catalogue info(collection name:%s)", pCollectionName ) ;

         rc = kickShardingKey( cataInfo, boUpdator, newUpdator,
                               hasShardingKey ) ;
         PD_RC_CHECK( rc, PDERROR, "Update failed, failed to kick the "
                      "sharding-key field(rc=%d)", rc ) ;

         if ( cataInfo->isMainCL() )
         {
            std::set< INT32 > emptyRCList;
            CoordSubCLlist subCLList;
            BSONObj newSubCLUpdator;
            rc = cataInfo->getMatchSubCLs( boSelector, subCLList );
            PD_RC_CHECK( rc, PDERROR,"Failed to get match "
                         "sub-collection(rc=%d)", rc ) ;

            rc = kickShardingKeyForSubCL( subCLList, newUpdator,
                                          newSubCLUpdator,
                                          hasShardingKey, cb ) ;
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to kick the sharding-key field "
                         "for sub-collection(rc=%d)",
                         rc );
            newUpdator = newSubCLUpdator;
            if ( hasShardingKey )
            {
               if ( newUpdator.isEmpty() )
               {
                  if ( flag & FLG_UPDATE_UPSERT )
                  {
                     newUpdator = BSON( "$null" << BSON( "null" << 1 ) ) ;
                  }
                  else
                  {
                     goto done ;
                  }
               }
               MsgOpUpdate *pUpdateReq = (MsgOpUpdate *)pReceiveBuffer;
               rc = msgBuildUpdateMsg( &pNewMsg, &bufferSize, pUpdateReq->name,
                                       flag, 0, &boSelector,
                                       &newUpdator, &boHint );
               PD_RC_CHECK( rc, PDERROR,
                            "Failed to build update request(rc=%d)", rc ) ;
               pMsgReq = (MsgOpUpdate *)pNewMsg;
            }
            if ( pMsgReq->flags | FLG_UPDATE_UPSERT )
            {
               pMsgReq->flags &= ~FLG_UPDATE_UPSERT ;
               pMsgReq->flags |= FLG_UPDATE_RETURNNUM;
            }
            rc = modifyOpOnMainCL( cataInfo, subCLList,
                                   (MsgHeader *)pMsgReq,
                                   pRouteAgent, cb, isNeedRefresh,
                                   emptyRCList, sendGroupLst,
                                   ((flag & FLG_UPDATE_RETURNNUM)
                                   | (flag & FLG_UPDATE_UPSERT)) ?
                                   &numTmp : NULL );
         }
         else
         {
            if ( hasShardingKey )
            {
               if ( newUpdator.isEmpty() )
               {
                  if ( flag & FLG_UPDATE_UPSERT )
                  {
                     newUpdator = BSON( "$null" << BSON( "null" << 1 ) ) ;
                  }
                  else
                  {
                     goto done ;
                  }
               }
               MsgOpUpdate *pUpdateReq = (MsgOpUpdate *)pReceiveBuffer;
               rc = msgBuildUpdateMsg( &pNewMsg, &bufferSize, pUpdateReq->name,
                                       flag, 0, &boSelector,
                                       &newUpdator, &boHint );
               PD_RC_CHECK( rc, PDERROR,
                            "Failed to build update request(rc=%d)", rc );
               pMsgReq = (MsgOpUpdate *)pNewMsg;
            }
            if ( pMsgReq->flags | FLG_UPDATE_UPSERT )
            {
               pMsgReq->flags &= ~FLG_UPDATE_UPSERT ;
               pMsgReq->flags |= FLG_UPDATE_RETURNNUM;
            }
            rc = updateNormalCL( cataInfo, boSelector, pMsgReq,
                                 pRouteAgent, cb, sendGroupLst,
                                 ((flag & FLG_UPDATE_RETURNNUM)
                                 | (flag & FLG_UPDATE_UPSERT)) ?
                                 &numTmp : NULL );
         }
         updateNum += numTmp;
         if ( pNewMsg )
         {
            SDB_OSS_FREE( pNewMsg );
            pNewMsg = NULL;
         }

         if ( !hasRefresh && rtnCoordWriteRetryRC( rc ) )
         {
            isNeedRefresh = TRUE;
         }
         else
         {
            isNeedRefresh = FALSE;
         }
      }while( isNeedRefresh );
      if ( cb->isTransaction() )
      {
         rc = rc ? rc : cb->getTransRC();
      }
      PD_RC_CHECK( rc, PDERROR, "Update failed(rc=%d)", rc ) ;

      if ( flag & FLG_UPDATE_UPSERT && 0 == updateNum )
      {
         mthModifier modifier;
         vector< INT64 > dollarList;
         BSONObj source;
         BSONObj target;
         CHAR *pBuffer = NULL;
         INT32 bufferSize = 0;
         MsgOpUpdate *pUpdateMsg = NULL;
         rc = modifier.loadPattern( boUpdator, &dollarList );
         PD_RC_CHECK( rc, PDERROR, "Invalid pattern is detected for updator:%s",
                     boUpdator.toString().c_str() );
         rc = modifier.modify( source, target );
         PD_RC_CHECK( rc, PDERROR, "failed to generate upsertor record(rc=%d)",
                     rc );
         rtnCoordProcesserFactory *pProcesserFactory
                                          = pCoordcb->getProcesserFactory();
         rtnCoordOperator *pOpProcesser = NULL ;
         pOpProcesser = pProcesserFactory->getOperator( MSG_BS_INSERT_REQ );
         SDB_ASSERT( pOpProcesser , "pCmdProcesser can't be NULL" );
         pUpdateMsg = (MsgOpUpdate *)pReceiveBuffer;
         rc = msgBuildInsertMsg( &pBuffer, &bufferSize, pUpdateMsg->name, 0,
                                 0, &target );
         PD_RC_CHECK( rc, PDERROR, "failed to build insert message(rc=%d)",
                      rc );
         rc = pOpProcesser->execute( pBuffer, 0, ppResultBuffer, cb,
                                     replyHeader, ppErrorObj );
         if ( pBuffer != NULL )
         {
            SDB_OSS_FREE( pBuffer );
            pBuffer = NULL;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to insert the data(rc=%d)", rc ) ;
      }

   done:
      if ( flag & FLG_UPDATE_RETURNNUM )
      {
         replyHeader.contextID = updateNum;
      }
      return rc;
   error:
      if ( rc && cb->isTransaction() )
      {
         pRollbackOperator = pCoordcb->getProcesserFactory()->getOperator(
            MSG_BS_TRANS_ROLLBACK_REQ );
         if ( pRollbackOperator )
         {
            pRollbackOperator->execute( pReceiveBuffer, packSize,
                                        ppResultBuffer, cb, replyHeader,
                                        ppErrorObj );
            SDB_ASSERT( NULL == *ppErrorObj, "impossible" ) ;
         }
      }
      replyHeader.flags = rc;
      goto done;
   }

   INT32 rtnCoordUpdate::updateNormalCL( CoordCataInfoPtr cataInfo,
                                         BSONObj &boSelector,
                                         MsgOpUpdate *pUpdateMsg,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb,
                                         CoordGroupList &sendGroupLst,
                                         INT64 *updateNum )
   {
      INT32 rc = SDB_OK;
      CoordGroupList groupLst;
      rc = getNodeGroups( cataInfo, boSelector, sendGroupLst, groupLst );
      PD_RC_CHECK( rc, PDERROR,
                  "update failed, couldn't get the match sharding(rc=%d)",
                  rc );

      rc = buildTransSession( groupLst, pRouteAgent, cb );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to build transaction session(rc=%d)",
                  rc );

      pUpdateMsg->version = cataInfo->getVersion();
      pUpdateMsg->header.routeID.value = 0;
      pUpdateMsg->header.TID = cb->getTID();
      rc = updateToDataNodeGroup( (CHAR *)pUpdateMsg, groupLst, sendGroupLst,
                                  pRouteAgent, cb, updateNum );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to update on data-node(rc=%d)", rc );
   done:
      return rc;
   error:
      adjustTransSession( sendGroupLst, pRouteAgent, cb );
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOUPDATE_GETNODEGROUPS, "rtnCoordUpdate::getNodeGroups" )
   INT32 rtnCoordUpdate::getNodeGroups( const CoordCataInfoPtr &cataInfo,
                                        bson::BSONObj &selectObj,
                                        CoordGroupList &sendGroupLst,
                                        CoordGroupList &groupLst )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOUPDATE_GETNODEGROUPS ) ;
      cataInfo->getGroupByMatcher( selectObj, groupLst );
      if ( groupLst.size() <= 0 )
      {
         rc = SDB_CAT_NO_MATCH_CATALOG;
      }
      else
      {
         CoordGroupList::iterator iter = sendGroupLst.begin();
         while( iter != sendGroupLst.end() )
         {
            groupLst.erase( iter->first );
            ++iter;
         }
      }
      PD_TRACE_EXITRC ( SDB_RTNCOUPDATE_GETNODEGROUPS, rc ) ;
      return rc;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOUPDATE_UPTODNG, "rtnCoordUpdate::updateToDataNodeGroup" )
   INT32 rtnCoordUpdate::updateToDataNodeGroup( CHAR *pBuffer,
                                                CoordGroupList &groupLst,
                                                CoordGroupList &sendGroupLst,
                                                netMultiRouteAgent *pRouteAgent,
                                                pmdEDUCB *cb,
                                                INT64 *updateNum )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOUPDATE_UPTODNG ) ;
      BOOLEAN isNeedRetry = FALSE;
      BOOLEAN hasRetry = FALSE;
      MsgOpUpdate *pUpdateMsg = (MsgOpUpdate *)pBuffer;
      do
      {
         hasRetry = isNeedRetry;
         isNeedRetry = FALSE;
         REQUESTID_MAP sendNodes;
         if ( cb->isTransaction() )
         {
            pUpdateMsg->header.opCode = MSG_BS_TRANS_UPDATE_REQ;
         }
         rc = rtnCoordSendRequestToNodeGroups( pBuffer, groupLst, TRUE,
                                               pRouteAgent, cb, sendNodes );
         if ( rc != SDB_OK )
         {
            rtnCoordClearRequest( cb, sendNodes ) ;
            PD_LOG ( PDERROR, "Failed to update on data-node,"
                     "send request failed(rc=%d)" );
            break;
         }
         REPLY_QUE replyQue;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                                MAKE_REPLY_TYPE( pUpdateMsg->header.opCode ) ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDWARNING, "Failed to update on data-node,"
                     "get reply failed(rc=%d)", rc );
            break;
         }
         while ( !replyQue.empty() )
         {
            MsgOpReply *pReply = NULL;
            pReply = (MsgOpReply *)(replyQue.front());
            replyQue.pop();
            INT32 rcTmp = pReply->flags;
            if ( SDB_OK == rc || SDB_CLS_COORD_NODE_CAT_VER_OLD == rc )
            {
               if ( SDB_OK != rcTmp )
               {
                  if ( SDB_CLS_NOT_PRIMARY == rcTmp
                     && !hasRetry )
                  {
                     CoordGroupInfoPtr groupInfoTmp;
                     rcTmp = rtnCoordGetGroupInfo( cb,
                        pReply->header.routeID.columns.groupID,
                        TRUE, groupInfoTmp ) ;
                     if ( SDB_OK == rcTmp )
                     {
                        isNeedRetry = TRUE;
                     }
                  }
                  if ( rcTmp )
                  {
                     rc = rcTmp ;
                     PD_LOG ( PDERROR, "Failed to update on data node"
                              "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                              pReply->header.routeID.columns.groupID,
                              pReply->header.routeID.columns.nodeID,
                              pReply->header.routeID.columns.serviceID,
                              rc );
                  }
               }
               else
               {
                  if ( updateNum && pReply->contextID > 0 )
                  {
                     *updateNum += pReply->contextID;
                  }
                  UINT32 groupID = pReply->header.routeID.columns.groupID;
                  sendGroupLst[groupID] = groupID;
                  groupLst.erase( groupID );
               }
               rc = rcTmp ? rcTmp : rc;
            }
            if ( NULL != pReply )
            {
               SDB_OSS_FREE( pReply );
            }
         }
      }while( isNeedRetry );
      PD_TRACE_EXITRC ( SDB_RTNCOUPDATE_UPTODNG, rc ) ;
      return rc;
   }
   
   //PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOUPDATE_CKIFINSHKEY, "rtnCoordUpdate::checkIfIncludeShardingKey" )
   INT32 rtnCoordUpdate::checkIfIncludeShardingKey ( const CoordCataInfoPtr &cataInfo,
                                                     const CHAR *pUpdator,
                                                     BOOLEAN &isInclude,
                                                     pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      isInclude = FALSE;
      try
      {
         BSONObj boUpdator( pUpdator );
         BSONObjIterator iter( boUpdator );
         while ( iter.more() )
         {
            BSONElement beTmp = iter.next();
            BSONObj boTmp = beTmp.Obj();
            isInclude = cataInfo->isIncludeShardingKey( boTmp );
            if ( isInclude )
            {
               goto done;
            }
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR, "Failed to check the record is include sharding-key,"
                  "occured unexpected error:%s", e.what() );
         goto error;
      }
      done :
         return rc;
      error :
         goto done;
   }

   INT32 rtnCoordUpdate::buildOpMsg( const CoordCataInfoPtr &cataInfo,
                                     const CoordSubCLlist &subCLList,
                                     CHAR *pSrcMsg, CHAR *&pDstMsg,
                                     INT32 &bufferSize )
   {
      INT32 rc = SDB_OK;
      INT32 flag;
      CHAR *pCollectionName = NULL;
      CHAR *pSelector = NULL;
      CHAR *pUpdator = NULL;
      CHAR *pHint = NULL;
      BSONObj boSelector;
      BSONObj boHint;
      BSONObj boUpdator;
      rc = msgExtractUpdate( pSrcMsg, &flag, &pCollectionName,
                           &pSelector, &pUpdator, &pHint );
      PD_RC_CHECK( rc, PDERROR, "Failed to parse update request(rc=%d)", rc );
      try
      {
         boSelector = BSONObj( pSelector );
         boUpdator = BSONObj( pUpdator );
         boHint = BSONObj( pHint );
         BSONArrayBuilder babSubCL;
         CoordSubCLlist::const_iterator iterCL = subCLList.begin();
         while( iterCL != subCLList.end() )
         {
            babSubCL.append( *iterCL );
            ++iterCL;
         }
         BSONObjBuilder bobNewSelector;
         bobNewSelector.appendElements( boSelector );
         bobNewSelector.appendArray( CAT_SUBCL_NAME, babSubCL.arr() );
         BSONObj boNewSelector = bobNewSelector.obj();
         rc = msgBuildUpdateMsg( &pDstMsg, &bufferSize, pCollectionName,
                                 flag, 0, &boNewSelector, &boUpdator,
                                 &boHint );
         PD_RC_CHECK( rc, PDERROR, "failed to build update request(rc=%d)",
                      rc );
         {
         MsgOpUpdate *pReqMsg = (MsgOpUpdate *)pDstMsg;
         MsgOpUpdate *pSrcReq = (MsgOpUpdate *)pSrcMsg;
         pReqMsg->version = cataInfo->getVersion();
         pReqMsg->w = pSrcReq->w;
         }
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR, "occur unexpected error:%s",
                      e.what() );
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordUpdate::checkModifierForSubCL ( const CoordSubCLlist &subCLList,
                                                 const CHAR *pUpdator,
                                                 pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      BOOLEAN isInclude;
      CoordSubCLlist::const_iterator iterCL = subCLList.begin() ;
      while( iterCL != subCLList.end() )
      {
         CoordCataInfoPtr subCataInfo ;
         rc = rtnCoordGetCataInfo( cb, (*iterCL).c_str(), FALSE,
                                   subCataInfo ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "get catalog of sub-collection(%s) failed(rc=%d)",
                      (*iterCL).c_str(), rc ) ;
         rc = checkIfIncludeShardingKey( subCataInfo, pUpdator,
                                         isInclude, cb ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "failed to check if include sharding-key(rc=%d)",
                      rc ) ;
         if ( isInclude )
         {
            rc = SDB_UPDATE_SHARD_KEY ;
            goto done ;
         }
         ++iterCL ;
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordUpdate::kickShardingKey( const CoordCataInfoPtr &cataInfo,
                                          const BSONObj &boUpdator,
                                          bson::BSONObj &boNewUpdator,
                                          BOOLEAN &hasShardingKey )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj boShardingKey ;
         cataInfo->getShardingKey( boShardingKey ) ;
         BSONObjBuilder bobNewUpdator ;
         BSONObjIterator iter( boUpdator ) ;
         while ( iter.more() )
         {
            BSONElement beTmp = iter.next() ;
            if ( beTmp.type() != Object )
            {
               rc = SDB_INVALIDARG;
               PD_LOG( PDERROR, "updator's element must be an Object type:"
                       "updator=%s", boUpdator.toString().c_str() ) ;
               goto error;
            }
            BSONObj boTmp = beTmp.Obj() ;
            BSONObjBuilder bobFields;
            BSONObjIterator iterField( boTmp ) ;
            while( iterField.more() )
            {
               BSONElement beField = iterField.next() ;
               BSONObjIterator iterKey( boShardingKey ) ;
               BOOLEAN isKey = FALSE ;
               while( iterKey.more() )
               {
                  BSONElement beKey = iterKey.next();
                  const CHAR *pKey = beKey.fieldName();
                  const CHAR *pField = beField.fieldName();
                  while( *pKey == *pField && *pKey != '\0' )
                  {
                     ++pKey;
                     ++pField;
                  }

                  if ( *pKey == *pField
                     || ( '\0' == *pKey && '.' == *pField )
                     || ( '\0' == *pField && '.' == *pKey ) )
                  {
                     isKey = TRUE;
                     break;
                  }
               }
               if ( isKey )
               {
                  hasShardingKey = TRUE;
               }
               else
               {
                  bobFields.append( beField );
               }
            }
            BSONObj boFields = bobFields.obj();
            if ( !boFields.isEmpty() )
            {
               bobNewUpdator.appendObject( beTmp.fieldName(),
                                           boFields.objdata() );
            }
         }
         boNewUpdator = bobNewUpdator.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR,"Failed to check the record is include sharding-key,"
                  "occured unexpected error: %s", e.what() ) ;
         goto error;
      }

   done:
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordUpdate::kickShardingKeyForSubCL( const CoordSubCLlist &subCLList,
                                                  const BSONObj &boUpdator,
                                                  BSONObj &boNewUpdator,
                                                  BOOLEAN &hasShardingKey,
                                                  pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      CoordSubCLlist::const_iterator iterCL = subCLList.begin();
      BSONObj boCur = boUpdator;
      BSONObj boNew = boUpdator;

      while( iterCL != subCLList.end() )
      {
         CoordCataInfoPtr subCataInfo;
         rc = rtnCoordGetCataInfo( cb, (*iterCL).c_str(), FALSE,
                                   subCataInfo ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "get catalog of sub-collection(%s) failed(rc=%d)",
                      (*iterCL).c_str(), rc ) ;
         rc = kickShardingKey( subCataInfo, boCur, boNew, hasShardingKey ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to kick sharding-key for "
                      "sub-collection(rc=%d)", rc ) ;
         boCur = boNew ;
         ++iterCL ;
      }
      boNewUpdator = boNew ;

   done:
      return rc;
   error:
      goto done;
   }
}
