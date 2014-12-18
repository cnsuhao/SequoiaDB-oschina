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

   Source File Name = rtnCoordInsert.cpp

   Descriptive Name = Runtime Coord Insert

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   insert options on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoordInsert.hpp"
#include "msgMessage.hpp"
#include "coordCB.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

using namespace bson;

namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_EXECUTE, "rtnCoordInsert::execute" )
   INT32 rtnCoordInsert::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                  CHAR **ppResultBuffer, pmdEDUCB *cb,
                                  MsgOpReply &replyHeader,
                                  BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_EXECUTE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      rtnCoordOperator *pRollbackOperator = NULL;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_INSERT_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;
      MsgOpInsert *pSrcMsg = (MsgOpInsert *)pReceiveBuffer;
      BOOLEAN isNeedRefreshCata = FALSE;
      GroupObjsMap groupObjsMap;
      BOOLEAN hasSendSomeData = FALSE;
      GroupSubCLMap groupSubCLMap;

      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      CHAR *pInsertor = NULL;
      INT32 count = 0;
      rc = msgExtractInsert( pReceiveBuffer, &flag,
                             &pCollectionName, &pInsertor, count ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to parse insert request" );
      pSrcMsg->header.TID = cb->getTID();
      if ( cb->isTransaction() )
      {
         pSrcMsg->header.opCode = MSG_BS_TRANS_INSERT_REQ;
      }
      else
      {
         pSrcMsg->header.opCode = MSG_BS_INSERT_REQ;
      }

      while ( TRUE )
      {
         CoordCataInfoPtr cataInfo;
         rc = rtnCoordGetCataInfo( cb, pCollectionName, isNeedRefreshCata,
                                   cataInfo );
         PD_RC_CHECK( rc, PDERROR,
                      "Failed to get the catalog info(collection name:%s)",
                      pCollectionName );

         pSrcMsg->header.routeID.value = 0;
         pSrcMsg->version = cataInfo->getVersion();

         if ( !cataInfo->isSharded() )
         {
            CoordGroupList groupLst;
            cataInfo->getGroupLst( groupLst );
            PD_CHECK( groupLst.size() > 0, SDB_SYS, error,
                      PDERROR, "invalid catalog-info, no group-info" );
            rc = buildTransSession( groupLst, pRouteAgent, cb );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to build transaction session(rc=%d)",
                         rc );

            CoordGroupList::iterator iterLst = groupLst.begin();
            rc = insertToAGroup( pReceiveBuffer, iterLst->first, pRouteAgent, cb );
            if ( rc )
            {
               CoordGroupList emptyGroupLst;
               adjustTransSession( emptyGroupLst, pRouteAgent, cb );
            }
         }//end of if ( !cataInfo->isSharded() )
         else if( !cataInfo->isMainCL() )
         {
            rc = insertToNormalCL( cataInfo, pReceiveBuffer, pInsertor,
                                   count, pRouteAgent, cb, groupObjsMap,
                                   hasSendSomeData ) ;
         }//end of else if( !cataInfo->isMainCL() )
         else
         {
            rc = insertToMainCL( cataInfo, pReceiveBuffer, pInsertor,
                                 count, pRouteAgent, cb, groupSubCLMap ) ;
         }
         if ( SDB_OK != rc )
         {
            if ( !isNeedRefreshCata && rtnCoordWriteRetryRC( rc ) )
            {
               isNeedRefreshCata = TRUE;
               continue;
            }
            if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == rc )
            {
               rc = SDB_CAT_NO_MATCH_CATALOG;
            }
            PD_RC_CHECK ( rc, PDERROR, "Failed to insert the record to "
                          "data-node, rc = %d", rc ) ;
         }
         break;
      }
      if ( cb->isTransaction() )
      {
         rc = rc ? rc : cb->getTransRC();
      }
      if ( rc )
      {
         goto error;
      }
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOINS_EXECUTE, rc ) ;
      return rc;
   error:
      if ( cb->isTransaction() )
      {
         pRollbackOperator
               = pCoordcb->getProcesserFactory()->getOperator( MSG_BS_TRANS_ROLLBACK_REQ );
         if ( pRollbackOperator )
         {
            pRollbackOperator->execute( pReceiveBuffer, packSize, ppResultBuffer,
                                       cb, replyHeader, ppErrorObj );
            SDB_ASSERT( NULL == *ppErrorObj, "impossible" ) ;
         }
      }
      replyHeader.flags = rc;
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_SHARDDBGROUP, "rtnCoordInsert::shardDataByGroup" )
   INT32 rtnCoordInsert::shardDataByGroup( const CoordCataInfoPtr &cataInfo,
                                           INT32 count,
                                           CHAR *pInsertor,
                                           GroupObjsMap &groupObjsMap )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_SHARDDBGROUP ) ;
      while ( count > 0 )
      {
         rc = shardAnObj( pInsertor, cataInfo, groupObjsMap );
         PD_RC_CHECK( rc, PDERROR, "Failed to shard the obj(rc=%d)", rc );

         BSONObj boInsertor ;
         try
         {
            boInsertor = BSONObj( pInsertor );
         }
         catch ( std::exception &e )
         {
            PD_CHECK( FALSE, SDB_INVALIDARG, error, PDERROR,
                      "Failed to parse the insert-obj:%s", e.what() ) ;
         }
         --count ;
         pInsertor += ossRoundUpToMultipleX( boInsertor.objsize(), 4 ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOINS_SHARDDBGROUP, rc ) ;
      return rc;
   error:
      groupObjsMap.clear() ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_RESHARDDATA, "rtnCoordInsert::reshardData" )
   INT32 rtnCoordInsert::reshardData( const CoordCataInfoPtr &cataInfo,
                                      GroupObjsMap &groupObjsMap )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_RESHARDDATA ) ;
      GroupObjsMap groupObjsMapNew ;
      GroupObjsMap::iterator iterMap = groupObjsMap.begin() ;
      while ( iterMap != groupObjsMap.end() )
      {
         ObjQueue objQueueOld = iterMap->second ;
         while ( !objQueueOld.empty() )
         {
            CHAR *pInsertor = objQueueOld.back();
            rc = shardAnObj( pInsertor, cataInfo, groupObjsMapNew ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to shard the obj(rc=%d)", rc ) ;

            objQueueOld.pop_back();
         }
         ++iterMap ;
      }
      groupObjsMap = groupObjsMapNew ;

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOINS_RESHARDDATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_SHARDANOBJ, "rtnCoordInsert::shardAnObj" )
   INT32 rtnCoordInsert::shardAnObj( CHAR *pInsertor,
                                     const CoordCataInfoPtr &cataInfo,
                                     GroupObjsMap &groupObjsMap )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_SHARDANOBJ ) ;
      try
      {
         BSONObj insertObj( pInsertor );
         UINT32 groupID = 0;
         rc = cataInfo->getGroupByRecord( insertObj, groupID );
         PD_RC_CHECK( rc, PDERROR, "Failed to get the group(rc=%d)", rc );

         groupObjsMap[ groupID ].push_back( pInsertor ) ;
      }
      catch ( std::exception &e )
      {
         PD_CHECK( FALSE, SDB_INVALIDARG, error, PDERROR,
                   "Failed to shard the data, received unexpected error:%s",
                   e.what() );
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOINS_SHARDANOBJ, rc ) ;
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordInsert::shardAnObj( CHAR *pInsertor,
                                     const CoordCataInfoPtr &cataInfo,
                                     pmdEDUCB * cb,
                                     GroupSubCLMap &groupSubCLMap )
   {
      INT32 rc = SDB_OK;
      std::string subCLName ;
      UINT32 groupID = CAT_INVALID_GROUPID;

      try
      {
         BSONObj insertObj( pInsertor ) ;
         CoordCataInfoPtr subClCataInfo;
         rc = cataInfo->getSubCLNameByRecord( insertObj, subCLName ) ;
         PD_CHECK( SDB_OK == rc, SDB_CLS_COORD_NODE_CAT_VER_OLD, error,
                  PDWARNING, "couldn't find the match sub-collection(rc=%d)",
                  rc ) ;
         rc = rtnCoordGetCataInfo( cb, subCLName.c_str(), FALSE,
                                   subClCataInfo );
         PD_CHECK( SDB_OK == rc, SDB_CLS_COORD_NODE_CAT_VER_OLD, error,
                  PDWARNING, "failed to get catalog of sub-collection(%s)",
                  subCLName.c_str() );
         rc = subClCataInfo->getGroupByRecord( insertObj, groupID );
         PD_CHECK( SDB_OK == rc, SDB_CLS_COORD_NODE_CAT_VER_OLD, error,
                  PDWARNING, "couldn't find the match catalog of "
                  "sub-collection(%s)", subCLName.c_str() );
         (groupSubCLMap[ groupID ])[ subCLName ].push_back( pInsertor );
      }
      catch ( std::exception &e )
      {
         PD_CHECK( FALSE, SDB_INVALIDARG, error, PDERROR,
                   "Failed to shard the data, occur unexpected error:%s",
                   e.what() );
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_INSTOGROUP, "rtnCoordInsert::insertToAGroup" )
   INT32 rtnCoordInsert::insertToAGroup( CHAR *pBuffer,
                                         UINT32 grpID,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_INSTOGROUP ) ;
      BOOLEAN isNeedRetry = FALSE;
      BOOLEAN hasRetry = FALSE;
      CoordGroupList groupLst;
      CoordGroupList successGroupLst;
      groupLst[grpID] = grpID;
      MsgHeader *pHead = (MsgHeader *)pBuffer ;

      if ( cb->isTransaction() )
      {
         pHead->opCode = MSG_BS_TRANS_INSERT_REQ;
      }
      do
      {
         hasRetry = isNeedRetry;
         isNeedRetry = FALSE;
         REQUESTID_MAP sendNodes;
         rc = rtnCoordSendRequestToNodeGroups( pBuffer, groupLst, TRUE,
                                               pRouteAgent, cb, sendNodes );
         if ( rc )
         {
            rtnCoordClearRequest( cb, sendNodes );
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to insert on data-node, "
                      "send request failed(rc=%d)", rc ) ;

         REPLY_QUE replyQue;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                                MAKE_REPLY_TYPE( pHead->opCode ) );
         PD_RC_CHECK( rc, PDWARNING, "Failed to insert on data-node, "
                      "get reply failed(rc=%d)", rc );

         rc = processReply( replyQue, successGroupLst, cb );
         if ( SDB_CLS_NOT_PRIMARY == rc && !hasRetry )
         {
            isNeedRetry = TRUE;
            rc = SDB_OK;
         }
         PD_RC_CHECK( rc, PDWARNING, "Failed to process the reply(rc=%d)",
                      rc );
      } while ( isNeedRetry );

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOINS_INSTOGROUP, rc ) ;
      return rc;
   error:
      goto done;
   }

/*   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_INSTOGROUPS, "rtnCoordInsert::insertToGroups" )
   INT32 rtnCoordInsert::insertToGroups( GroupObjsMap &groupObjsMap,
                           CHAR *pInputBuf,
                           netMultiRouteAgent *pRouteAgent,
                           pmdEDUCB *cb,
                           BOOLEAN &hasSendSomeData )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_INSTOGROUPS ) ;
      BOOLEAN isNeedRetry = FALSE;
      BOOLEAN hasRetry = FALSE;
      MsgOpInsert *pSrcMsg = (MsgOpInsert *)pInputBuf;
      UINT32 totalGroups = groupObjsMap.size();
      do
      {
         hasRetry = isNeedRetry;
         isNeedRetry = FALSE;
         REQUESTID_MAP sendNodes;
         REPLY_QUE replyQue;
         rc = sendToGroups( groupObjsMap, pSrcMsg,
                        pRouteAgent, cb, sendNodes );
         if ( rc )
         {
            rtnCoordClearRequest( cb, sendNodes );
         }
         PD_RC_CHECK( rc, PDERROR,
                     "failed to send the request(rc=%d)",
                     rc );
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                     MAKE_REPLY_TYPE( pSrcMsg->header.opCode ) );
         PD_RC_CHECK( rc, PDWARNING,
                     "failed to insert on data-node, "
                     "get reply failed(rc=%d)", rc );
         rc = processReply( replyQue, groupObjsMap, cb );
         if ( SDB_CLS_NOT_PRIMARY == rc && !hasRetry )
         {
            isNeedRetry = TRUE;
            rc = SDB_OK;
         }
         PD_RC_CHECK( rc, PDWARNING,
                     "failed to process the reply(rc=%d)", rc );
      }while ( isNeedRetry );
   done:
      if ( totalGroups != groupObjsMap.size() )
      {
         hasSendSomeData = TRUE;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOINS_INSTOGROUPS, rc ) ;
      return rc;
   error:
      goto done;
   }*/

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_SENDTOGROUPS, "sendToGroups" )
   INT32 rtnCoordInsert::sendToGroups( const GroupInsertMsgMap &groupMsgMap,
                                       MsgOpInsert *pSrcMsg,
                                       netMultiRouteAgent *pRouteAgent,
                                       pmdEDUCB *cb,
                                       REQUESTID_MAP &sendNodes )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_SENDTOGROUPS ) ;
      SDB_ASSERT( pSrcMsg != NULL, "pSrcMsg can't be null!" );

      GroupInsertMsgMap::const_iterator iterMap = groupMsgMap.begin();
      INT32 headLen = ossRoundUpToMultipleX( offsetof(MsgOpInsert, name) +
                                             ossStrlen ( pSrcMsg->name ) + 1,
                                             4 ) ;
      while( iterMap != groupMsgMap.end() )
      {
         BSONObj boInsertor;
         CoordGroupList groupLst;
         groupLst[iterMap->first] = iterMap->first;
         if ( iterMap->second.dataList.size() == 0 )
         {
            ++iterMap ;
            continue ;
         }

         pSrcMsg->header.messageLength = headLen + iterMap->second.dataLen ;
         pSrcMsg->header.routeID.value = 0;
         rc = rtnCoordSendRequestToNodeGroups( (MsgHeader *)pSrcMsg,
                                               groupLst, TRUE, pRouteAgent,
                                               cb, iterMap->second.dataList,
                                               sendNodes ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to send the insert request to "
                      "group(goupID=%u)", iterMap->first ) ;
         ++iterMap ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOINS_SENDTOGROUPS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_PROREPLY, "rtnCoordInsert::processReply" )
   INT32 rtnCoordInsert::processReply( REPLY_QUE &replyQue,
                                       CoordGroupList &successGroupList,
                                       pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_PROREPLY ) ;
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = (MsgOpReply *)(replyQue.front());
         replyQue.pop();
         if ( NULL == pReply )
         {
            PD_LOG ( PDWARNING, "reply is null");
            continue;
         }
         INT32 rcTmp = pReply->flags;
         if ( SDB_OK == rcTmp )
         {
            successGroupList[pReply->header.routeID.columns.groupID]
                           = pReply->header.routeID.columns.groupID;
         }
         else
         {
            PD_LOG ( PDERROR, "Failed to execute on data node"
                     "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                     pReply->header.routeID.columns.groupID,
                     pReply->header.routeID.columns.nodeID,
                     pReply->header.routeID.columns.serviceID,
                     rcTmp ) ;

            if ( SDB_CLS_NOT_PRIMARY == rcTmp )
            {
               rc = rc ? rc : rcTmp ;

               CoordGroupInfoPtr groupInfoTmp ;
               rcTmp = rtnCoordGetGroupInfo( cb, pReply->header.routeID.columns.groupID,
                                             TRUE, groupInfoTmp ) ;
               if ( rcTmp )
               {
                  rc = rcTmp ;
                  PD_LOG ( PDERROR, "Failed to update group "
                           "info(groupID=%u, rc=%d)",
                           pReply->header.routeID.columns.groupID,
                           rc ) ;
               }
            }
            else if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == rcTmp )
            {
               if ( SDB_OK == rc || SDB_CLS_NOT_PRIMARY == rc )
               {
                  rc = SDB_CLS_COORD_NODE_CAT_VER_OLD ;
               }
            }
            else
            {
               rc = rcTmp ;
            }
         }
         SDB_OSS_FREE( pReply ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOINS_PROREPLY, rc ) ;
      return rc ;
   }

   INT32 rtnCoordInsert::insertToNormalCL( const CoordCataInfoPtr &cataInfo,
                                           CHAR *pReceiveBuffer,
                                           CHAR *pInsertor, INT32 count,
                                           netMultiRouteAgent *pRouteAgent,
                                           pmdEDUCB *cb,
                                           GroupObjsMap &groupObjsMap,
                                           BOOLEAN &hasSendSomeData )
   {
      INT32 rc = SDB_OK;
      INT32 filler = 0;
      GroupInsertMsgMap groupMsgMap;
      CoordGroupList successGroupList;

      if ( groupObjsMap.size() == 0 )
      {
         rc = shardDataByGroup( cataInfo, count, pInsertor, groupObjsMap ) ;
      }
      else
      {
         rc = reshardData( cataInfo, groupObjsMap ) ;
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to shard the data by group(rc=%d)",
                   rc ) ;

      if ( cb->isTransaction() )
      {
         CoordGroupList groupLst ;
         GroupObjsMap::iterator iterGroup ;
         iterGroup = groupObjsMap.begin();
         while( iterGroup != groupObjsMap.end() )
         {
            groupLst[ iterGroup->first ] = iterGroup->first ;
            ++iterGroup ;
         }
         rc = buildTransSession( groupLst, pRouteAgent, cb ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "Failed to build transaction session(rc=%d)",
                      rc ) ;
      }

      if ( groupObjsMap.size() == 1 && !hasSendSomeData )
      {
         GroupObjsMap::iterator iterMap = groupObjsMap.begin();
         rc = insertToAGroup( pReceiveBuffer, iterMap->first,
                              pRouteAgent, cb );
         PD_RC_CHECK( rc, PDERROR,
                      "Failed to insert on group(groupID:%u, rc=%d)",
                      iterMap->first, rc );
         successGroupList[iterMap->first] = iterMap->first ;
      }
      else
      {
         CHAR *pHeadRemain = pReceiveBuffer + sizeof( MsgHeader );
         INT32 remainLen = pInsertor - pHeadRemain;
         rc = buildInsertMsg( pHeadRemain, remainLen, groupObjsMap,
                              &filler, groupMsgMap ) ;
         PD_RC_CHECK( rc, PDERROR,"Failed to build the message(rc=%d)", rc );

         rc = insertToGroups( groupMsgMap, (MsgOpInsert *)pReceiveBuffer,
                              pRouteAgent, cb, successGroupList ) ;
         if ( successGroupList.size() != 0 )
         {
            hasSendSomeData = TRUE ;
         }
         if ( rc != SDB_OK )
         {
            CoordGroupList::iterator iterSuc = successGroupList.begin();
            while( iterSuc != successGroupList.end() )
            {
               groupObjsMap.erase( iterSuc->first ) ;
               ++iterSuc ;
            }
            PD_LOG( PDWARNING, "Failed to insert the data(rc=%d)", rc ) ;
            goto error ;
         }
         else
         {
            groupObjsMap.clear() ;
         }
      }

   done:
      return rc ;
   error:
      adjustTransSession( successGroupList, pRouteAgent, cb ) ;
      goto done ;
   }

   INT32 rtnCoordInsert::insertToMainCL( const CoordCataInfoPtr &cataInfo,
                                         CHAR *pReceiveBuffer,
                                         CHAR *pInsertor, INT32 count,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb,
                                         GroupSubCLMap &groupSubCLMap )
   {
      INT32 rc = SDB_OK ;
      INT32 filler = 0 ;
      GroupInsertMsgMap groupMsgMap ;
      CoordGroupList successGroupList ;
      std::vector< BSONObj > subCLInfoCache ;

      if ( groupSubCLMap.size() == 0 )
      {
         rc = shardDataByGroup( cataInfo, count, pInsertor, cb,
                                groupSubCLMap ) ;
      }
      else
      {
         rc = reshardData( cataInfo, cb, groupSubCLMap );
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to shard the data(rc=%d)", rc ) ;

      if ( cb->isTransaction() )
      {
         CoordGroupList groupLst;
         GroupSubCLMap::iterator iterMap = groupSubCLMap.begin();
         while( iterMap != groupSubCLMap.end() )
         {
            groupLst[ iterMap->first ] = iterMap->first;
            ++iterMap ;
         }
         rc = buildTransSession( groupLst, pRouteAgent, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build transaction "
                      "session(rc=%d)", rc ) ;
      }

      {
         CHAR *pHeadRemain = pReceiveBuffer + sizeof( MsgHeader );
         INT32 remainLen = pInsertor - pHeadRemain ;
         rc = buildInsertMsg( pHeadRemain, remainLen, groupSubCLMap,
                              subCLInfoCache, &filler, groupMsgMap );
         PD_RC_CHECK( rc, PDERROR, "Failed to build the message(rc=%d)",
                      rc ) ;
      }

      rc = insertToGroups( groupMsgMap, (MsgOpInsert *)pReceiveBuffer,
                           pRouteAgent, cb, successGroupList ) ;
      if ( rc != SDB_OK )
      {
         CoordGroupList::iterator iterSuc = successGroupList.begin() ;
         while( iterSuc != successGroupList.end() )
         {
            groupSubCLMap.erase( iterSuc->first );
            ++iterSuc ;
         }
         PD_LOG( PDWARNING, "failed to insert the data(rc=%d)", rc );
         goto error ;
      }
      else
      {
         groupSubCLMap.clear();
      }

   done:
      return rc;
   error:
      adjustTransSession( successGroupList, pRouteAgent, cb );
      goto done;
   }

   INT32 rtnCoordInsert::shardDataByGroup( const CoordCataInfoPtr &cataInfo,
                                           INT32 count,
                                           CHAR *pInsertor,
                                           pmdEDUCB *cb,
                                           GroupSubCLMap &groupSubCLMap )
   {
      INT32 rc = SDB_OK;
      std::string subCLName ;

      while ( count > 0 )
      {
         rc = shardAnObj( pInsertor, cataInfo, cb, groupSubCLMap );
         PD_RC_CHECK( rc, PDERROR, "Failed to shard the obj(rc=%d)", rc );

         try
         {
            BSONObj boInsertor( pInsertor );
            pInsertor += ossRoundUpToMultipleX( boInsertor.objsize(), 4 ) ;
            --count ;
         }
         catch ( std::exception &e )
         {
            PD_CHECK( FALSE, SDB_INVALIDARG, error, PDERROR,
                      "Failed to parse the insert-obj, "
                      "occur unexpected error:%s", e.what() ) ;
         }
      }

   done:
      return rc;
   error:
      groupSubCLMap.clear() ;
      goto done ;
   }

   INT32 rtnCoordInsert::reshardData( const CoordCataInfoPtr &cataInfo,
                                      pmdEDUCB *cb,
                                      GroupSubCLMap &groupSubCLMap )
   {
      INT32 rc = SDB_OK;
      GroupSubCLMap groupSubCLMapNew;
      GroupSubCLMap::iterator iterGroup = groupSubCLMap.begin(); 
      while ( iterGroup != groupSubCLMap.end() )
      {
         SubCLObjsMap::iterator iterCL = iterGroup->second.begin() ;
         while( iterCL != iterGroup->second.end() )
         {
            while( !iterCL->second.empty() )
            {
               CHAR *pInsertor = iterCL->second.back();
               rc = shardAnObj( pInsertor, cataInfo, cb, groupSubCLMapNew );
               PD_RC_CHECK( rc, PDWARNING, "Failed to shard the obj(rc=%d)",
                            rc ) ;
               iterCL->second.pop_back() ;
            }
            ++iterCL ;
         }
         ++iterGroup ;
      }
      groupSubCLMap = groupSubCLMapNew ;

   done:
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINS_INSTOGROUPS, "rtnCoordInsert::insertToGroups" )
   INT32 rtnCoordInsert::insertToGroups( const GroupInsertMsgMap &groupMsgMap,
                                         MsgOpInsert *pSrcMsg,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb,
                                         CoordGroupList &successGroupList )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINS_INSTOGROUPS ) ;
      SINT32 opCode;
      if ( cb->isTransaction() )
      {
         opCode = MSG_BS_TRANS_INSERT_REQ;
      }
      else
      {
         opCode = MSG_BS_INSERT_REQ;
      }
      REQUESTID_MAP sendNodes;
      REPLY_QUE replyQue;
      rc = sendToGroups( groupMsgMap, pSrcMsg, pRouteAgent,
                         cb, sendNodes ) ;
      if ( rc )
      {
         rtnCoordClearRequest( cb, sendNodes ) ;
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to send the request(rc=%d)", rc );

      rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                             MAKE_REPLY_TYPE( opCode ) ) ;
      PD_RC_CHECK( rc, PDWARNING, "Failed to insert on data-node, "
                   "get reply failed(rc=%d)", rc ) ;

      rc = processReply( replyQue, successGroupList, cb ) ;
      PD_RC_CHECK( rc, PDWARNING, "Failed to process the reply(rc=%d)", rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOINS_INSTOGROUPS, rc ) ;
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordInsert::buildInsertMsg( CHAR *pHeadRemain,
                                         INT32 remainLen,
                                         const GroupObjsMap &groupObjsMap,
                                         void *pFiller,
                                         GroupInsertMsgMap &groupMsgMap )
   {
      INT32 rc = SDB_OK;
      GroupObjsMap::const_iterator iterGroup = groupObjsMap.begin();
      while( iterGroup != groupObjsMap.end() )
      {
         UINT32 i = 0;
         UINT32 groupID = iterGroup->first;
         groupMsgMap[groupID].dataList.clear();
         groupMsgMap[groupID].dataLen = 0;
         netIOV ioHead;
         ioHead.iovBase = (void *)pHeadRemain;
         ioHead.iovLen = remainLen;
         groupMsgMap[groupID].dataList.push_back( ioHead );
         for( ; i < iterGroup->second.size(); i++ )
         {
            try
            {
               BSONObj boInsertor( (iterGroup->second)[i] );
               netIOV ioObj;
               ioObj.iovBase = (iterGroup->second)[i] ;
               ioObj.iovLen = boInsertor.objsize();
               groupMsgMap[groupID].dataList.push_back( ioObj );

               SINT32 usedLen = ossRoundUpToMultipleX( boInsertor.objsize(),
                                                       4 );
               SINT32 fillLen = usedLen - boInsertor.objsize();
               if ( fillLen > 0 )
               {
                  netIOV ioFiller;
                  ioFiller.iovBase = pFiller;
                  ioFiller.iovLen = fillLen;
                  groupMsgMap[groupID].dataList.push_back( ioFiller );
               }
               groupMsgMap[groupID].dataLen += usedLen;
            }
            catch( std::exception &e )
            {
               PD_CHECK( FALSE, SDB_INVALIDARG, error, PDERROR,
                         "Failed to build insert-message, "
                         "occur unexpected error:%s", e.what() );
            }
         }
         ++iterGroup ;
      }

   done:
      return rc;
   error:
      groupMsgMap.clear() ;
      goto done;
   }

   INT32 rtnCoordInsert::buildInsertMsg( CHAR *pHeadRemain,
                                         INT32 remainLen,
                                         const GroupSubCLMap &groupSubCLMap,
                                         std::vector< bson::BSONObj > &subClInfoLst,
                                         void *pFiller,
                                         GroupInsertMsgMap &groupMsgMap )
   {
      INT32 rc = SDB_OK;
      GroupSubCLMap::const_iterator iterGroup = groupSubCLMap.begin();
      while ( iterGroup != groupSubCLMap.end() )
      {
         UINT32 groupID = iterGroup->first;
         groupMsgMap[groupID].dataList.clear();
         groupMsgMap[groupID].dataLen = 0;
         netIOV ioHead;
         ioHead.iovBase = (void *)pHeadRemain;
         ioHead.iovLen = remainLen;
         groupMsgMap[groupID].dataList.push_back( ioHead );
         SubCLObjsMap::const_iterator iterCL = iterGroup->second.begin();
         while ( iterCL != iterGroup->second.end() )
         {
            UINT32 i = 0;
            SINT32 dataLen = 0;
            SINT32 objNum = 0;
            UINT32 curPos = groupMsgMap[groupID].dataList.size();
            netIOV ioCLInfo;
            ioCLInfo.iovBase = NULL;
            ioCLInfo.iovLen = 0;
            groupMsgMap[groupID].dataList.push_back( ioCLInfo );
            netIOV ioCLInfoFiller;
            ioCLInfoFiller.iovBase = NULL;
            ioCLInfoFiller.iovLen = 0;
            groupMsgMap[groupID].dataList.push_back( ioCLInfoFiller );
            try
            {
               for( ; i < iterCL->second.size(); i++ )
               {
                  BSONObj boInsertor( (iterCL->second)[i] );
                  netIOV ioObj;
                  ioObj.iovBase = (iterCL->second)[i] ;
                  ioObj.iovLen = boInsertor.objsize();
                  groupMsgMap[groupID].dataList.push_back( ioObj );

                  SINT32 usedLen = ossRoundUpToMultipleX( boInsertor.objsize(),
                                                          4 );
                  SINT32 fillLen = usedLen - boInsertor.objsize();
                  if ( fillLen > 0 )
                  {
                     netIOV ioFiller;
                     ioFiller.iovBase = pFiller;
                     ioFiller.iovLen = fillLen;
                     groupMsgMap[groupID].dataList.push_back( ioFiller );
                  }
                  dataLen += usedLen ;
                  ++objNum ;
               }
               BSONObjBuilder subCLInfoBuild;
               subCLInfoBuild.append( FIELD_NAME_SUBOBJSNUM, objNum );
               subCLInfoBuild.append( FIELD_NAME_SUBOBJSSIZE, dataLen );
               subCLInfoBuild.append( FIELD_NAME_SUBCLNAME, iterCL->first );
               UINT32 objPos = subClInfoLst.size();
               subClInfoLst.push_back( subCLInfoBuild.obj() );
               groupMsgMap[groupID].dataList[curPos].iovBase
                                    = subClInfoLst[objPos].objdata();
               groupMsgMap[groupID].dataList[curPos].iovLen
                                    = subClInfoLst[objPos].objsize();
               SINT32 clInfoUsedLen = ossRoundUpToMultipleX(
                  subClInfoLst[objPos].objsize(), 4 ) ;
               SINT32 clInfoFillLen = clInfoUsedLen -
                                      subClInfoLst[objPos].objsize();
               if( clInfoFillLen > 0 )
               {
                  groupMsgMap[groupID].dataList[curPos + 1].iovBase = pFiller;
                  groupMsgMap[groupID].dataList[curPos + 1].iovLen = clInfoFillLen;
               }
               else
               {
                  groupMsgMap[groupID].dataList.erase(
                           groupMsgMap[groupID].dataList.begin() + curPos + 1 ) ;
               }
               groupMsgMap[groupID].dataLen += ( dataLen + clInfoUsedLen ) ;
            }
            catch( std::exception &e )
            {
               PD_CHECK( FALSE, SDB_INVALIDARG, error, PDERROR,
                         "Failed to build insert-message, "
                         "occur unexpected error:%s", e.what() );
            }
            ++iterCL ;
         }
         ++iterGroup ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

}

