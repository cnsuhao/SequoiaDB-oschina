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

   Source File Name = rtnCoordOperator.cpp

   Descriptive Name = Runtime Coord Operator

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   general operations on coordniator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoordOperator.hpp"
#include "ossErr.h"
#include "rtnCoord.hpp"
#include "rtnCoordInsert.hpp"
#include "rtnCoordQuery.hpp"
#include "rtnCoordDelete.hpp"
#include "rtnCoordUpdate.hpp"
#include "../bson/bson.h"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "msgMessage.hpp"
#include "msgCatalog.hpp"
#include "rtnCoordCommon.hpp"
#include "rtnCoordInterrupt.hpp"
#include "coordSession.hpp"
#include "rtnCoordAuth.hpp"
#include "rtnCoordAuthCrt.hpp"
#include "rtnCoordAuthDel.hpp"
#include "rtn.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "rtnCoordTransaction.hpp"
#include "rtnCoordSql.hpp"
#include "rtnCoordAggregate.hpp"
#include "coordDef.hpp"
#include "rtnCoordLob.hpp"
#include <stdlib.h>

using namespace bson;

namespace engine
{
   RTN_COORD_OP_BEGIN
   RTN_COORD_OP_ADD( MSG_BS_INSERT_REQ, rtnCoordInsert )
   RTN_COORD_OP_ADD( MSG_BS_QUERY_REQ, rtnCoordQuery )
   RTN_COORD_OP_ADD( MSG_BS_DELETE_REQ, rtnCoordDelete )
   RTN_COORD_OP_ADD( MSG_BS_UPDATE_REQ, rtnCoordUpdate )
   RTN_COORD_OP_ADD( MSG_BS_AGGREGATE_REQ, rtnCoordAggregate )
   RTN_COORD_OP_ADD( MSG_BS_INTERRUPTE, rtnCoordInterrupt )
   RTN_COORD_OP_ADD( MSG_BS_KILL_CONTEXT_REQ, rtnCoordKillContext )
   RTN_COORD_OP_ADD( MSG_AUTH_VERIFY_REQ, rtnCoordAuth )
   RTN_COORD_OP_ADD( MSG_AUTH_CRTUSR_REQ, rtnCoordAuthCrt )
   RTN_COORD_OP_ADD( MSG_AUTH_DELUSR_REQ, rtnCoordAuthDel )
   RTN_COORD_OP_ADD( MSG_BS_TRANS_BEGIN_REQ, rtnCoordTransBegin )
   RTN_COORD_OP_ADD( MSG_BS_TRANS_COMMIT_REQ, rtnCoordTransCommit )
   RTN_COORD_OP_ADD( MSG_BS_TRANS_ROLLBACK_REQ, rtnCoordTransRollback )
   RTN_COORD_OP_ADD( MSG_BS_SQL_REQ, rtnCoordSql )
   RTN_COORD_OP_ADD( MSG_BS_MSG_REQ, rtnCoordMsg )
   RTN_COORD_OP_ADD( MSG_BS_LOB_OPEN_REQ, rtnCoordOpenLob )
   RTN_COORD_OP_ADD( MSG_BS_LOB_WRITE_REQ, rtnCoordWriteLob )
   RTN_COORD_OP_ADD( MSG_BS_LOB_READ_REQ, rtnCoordReadLob )
   RTN_COORD_OP_ADD( MSG_BS_LOB_CLOSE_REQ, rtnCoordCloseLob )
   RTN_COORD_OP_ADD( MSG_BS_LOB_REMOVE_REQ, rtnCoordRemoveLob )
   RTN_COORD_OP_ADD( MSG_NULL, rtnCoordOperatorDefault )
   RTN_COORD_OP_END


   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOOPDEFAULT_EXECUTE, "rtnCoordOperatorDefault::execute" )
   INT32 rtnCoordOperatorDefault::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                           CHAR **ppResultBuffer, pmdEDUCB *cb,
                           MsgOpReply &replyHeader,
                           BSONObj **ppErrorObj )
   {
      PD_TRACE_ENTRY ( SDB_RTNCOOPDEFAULT_EXECUTE ) ;
      MsgHeader *pHeader = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof(MsgOpReply);
      replyHeader.header.opCode        = MAKE_REPLY_TYPE(pHeader->opCode);
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_COORD_UNKNOWN_OP_REQ;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;
      PD_TRACE_EXIT ( SDB_RTNCOOPDEFAULT_EXECUTE ) ;
      return SDB_OK;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOKILLCONTEXT_EXECUTE, "rtnCoordKillContext::execute" )
   INT32 rtnCoordKillContext::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                       CHAR **ppResultBuffer, pmdEDUCB *cb,
                                       MsgOpReply &replyHeader,
                                       BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOKILLCONTEXT_EXECUTE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      SDB_RTNCB *rtnCB                 = pKrcb->getRTNCB () ;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_KILL_CONTEXT_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 contextCount     = 0;
      INT64 *contextList     = NULL ;

      rc = msgExtractKillContexts ( pReceiveBuffer, &contextCount,
                                    &contextList ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to parse killcontext request ");
         goto error ;
      }
      rc = rtnKillContexts ( contextCount, contextList, cb, rtnCB ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to kill contexts, rc = %d", rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_RTNCOKILLCONTEXT_EXECUTE, rc ) ;
      return rc;
   error :
      goto done ;
   }

   INT32 rtnCoordTransOperator::buildTransSession( CoordGroupList &groupLst,
                                                   netMultiRouteAgent *pRouteAgent,
                                                   pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      DpsTransNodeMap *pTransNodeLst = NULL;
      DpsTransNodeMap::iterator iterTrans;
      CoordGroupList newTransGroupLst;
      CoordGroupList::iterator iterGroup;
      MsgOpTransBegin msgReq;
      REQUESTID_MAP sendNodes;
      REPLY_QUE replyQue;
      BOOLEAN hasRetry = FALSE;
      BOOLEAN isNeedRetry = FALSE;
      CoordGroupInfoPtr groupInfoTmp;

      if ( !cb->isTransaction() )
      {
         goto done;
      }

      msgReq.header.messageLength = sizeof( MsgOpTransBegin );
      msgReq.header.opCode = MSG_BS_TRANS_BEGIN_REQ;
      msgReq.header.routeID.value = 0;
      msgReq.header.TID = cb->getTID();

      pTransNodeLst = cb->getTransNodeLst();
      iterGroup = groupLst.begin();
      while ( iterGroup != groupLst.end() )
      {
         iterTrans = pTransNodeLst->find( iterGroup->first );
         if ( pTransNodeLst->end() == iterTrans )
         {
            newTransGroupLst[iterGroup->first] = iterGroup->second;
         }
         ++iterGroup;
      }

   retry:
      isNeedRetry = FALSE;
      rc = rtnCoordSendRequestToNodeGroups( (CHAR *)(&msgReq), newTransGroupLst,
                                            TRUE, pRouteAgent, cb, sendNodes ) ;
      if ( rc )
      {
         rtnCoordClearRequest( cb, sendNodes );
      }
      PD_CHECK( SDB_OK == rc, rc, errorrollback, PDERROR,
               "failed to build transaction session, "
               "send the request failed(rc=%d)", rc );
      rc = rtnCoordGetReply( cb, sendNodes,
                             replyQue, MSG_BS_TRANS_BEGIN_RSP );
      PD_CHECK( SDB_OK == rc, rc, errorrollback, PDERROR,
               "failed to build transaction session, "
               "get reply failed(rc=%d)", rc );
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = (MsgOpReply *)(replyQue.front());
         replyQue.pop();
         INT32 rcTmp = pReply->flags;
         if ( SDB_OK == rcTmp )
         {
            newTransGroupLst.erase( pReply->header.routeID.columns.groupID );
            cb->addTransNode( pReply->header.routeID );
         }
         else if ( !hasRetry && rtnCoordWriteRetryRC( rc ) )
         {
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
            rc = rc ? rc : rcTmp;
            PD_LOG ( PDERROR,
                     "failed to build transaction session on data node"
                     "(groupID=%u, nodeID=%u, rc=%d)",
                     pReply->header.routeID.columns.groupID,
                     pReply->header.routeID.columns.nodeID,
                     rcTmp );
         }

         SDB_OSS_FREE( pReply );
      }
      PD_CHECK( SDB_OK == rc, rc, errorrollback, PDERROR,
               "build transaction session failed(rc=%d)",
               rc );
      if ( isNeedRetry )
      {
         hasRetry = TRUE;
         goto retry;
      }
   done:
      return rc;
   errorrollback:
      releaseTransSession( newTransGroupLst, pRouteAgent, cb );
      goto done;
   }

   INT32 rtnCoordTransOperator::releaseTransSession( CoordGroupList & groupLst,
                                                   netMultiRouteAgent * pRouteAgent,
                                                   pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK;
      CoordGroupInfoPtr groupInfo;
      CoordGroupList::iterator iter = groupLst.begin();
      MsgOpTransRollback msgReq;
      REQUESTID_MAP sendNodes;
      REPLY_QUE replyQue;
      msgReq.header.messageLength = sizeof( MsgOpTransRollback );
      msgReq.header.opCode = MSG_BS_TRANS_ROLLBACK_REQ;
      msgReq.header.routeID.value = 0;
      msgReq.header.TID = cb->getTID();
      while ( iter != groupLst.end() )
      {
         rc = rtnCoordGetGroupInfo( cb, iter->first, FALSE, groupInfo );
         if ( rc != SDB_OK )
         {
            ++iter;
            PD_LOG ( PDERROR,
                     "failed to release transaction session, "
                     "get groupInfo failed(groupID=%u, rc=%d)",
                     iter->first, rc );
            continue;
         }
         rc = rtnCoordSendRequestToPrimary( (CHAR *)(&msgReq), groupInfo, sendNodes,
                                    pRouteAgent, MSG_ROUTE_SHARD_SERVCIE,cb );
         if ( rc != SDB_OK )
         {
            ++iter;
            PD_LOG ( PDWARNING,
                     "failed to send release transaction session "
                     "request to the group(groupID=%u, rc=%d)."
                     "the primary node will perform release transaction "
                     "session automatically" );
            continue;
         }
         ++iter;
      }

      rc = rtnCoordGetReply( cb, sendNodes, replyQue, MSG_BS_TRANS_ROLLBACK_RSP );
      PD_RC_CHECK( rc, PDWARNING,
                  "failed to get reply(rc=%d)", rc );
      while( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = (MsgOpReply *)(replyQue.front());
         replyQue.pop();
         if ( pReply->flags )
         {
            PD_LOG ( PDWARNING,
                     "failed to release transaction session on data node"
                     "(groupID=%u, nodeID=%u, rc=%d)",
                     pReply->header.routeID.columns.groupID,
                     pReply->header.routeID.columns.nodeID,
                     pReply->flags );
         }
         SDB_OSS_FREE( pReply );
      }
   done:
      return rc;
   error:
      goto done;
   }
   INT32 rtnCoordTransOperator::modifyOpOnMainCL( CoordCataInfoPtr &cataInfo,
                                             const CoordSubCLlist &subCLList,
                                             MsgHeader *pSrcMsg,
                                             netMultiRouteAgent *pRouteAgent,
                                             pmdEDUCB *cb,
                                             BOOLEAN isNeedRefresh,
                                             std::set<INT32> &ignoreRCList,
                                             CoordGroupList &sendGroupList,
                                             INT64 *modifyNum )
   {
      INT32 rc = SDB_OK;
      INT32 rcTmp = SDB_OK;
      CoordGroupSubCLMap groupSubCLMap;
      CoordGroupSubCLMap::iterator iterGroup;
      CHAR *pBuffer = NULL;
      INT32 bufferSize = 0;
      REPLY_QUE replyQue;
      REQUESTID_MAP sendNodes;

      rc = rtnCoordGetSubCLsByGroups( subCLList, sendGroupList, cb,
                                      groupSubCLMap );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get sub-collection info(rc=%d)",
                  rc );

      if ( cb->isTransaction() )
      {
         CoordGroupList groupLst;
         CoordGroupSubCLMap::iterator iterMap = groupSubCLMap.begin();
         while( iterMap != groupSubCLMap.end() )
         {
            groupLst[ iterMap->first ] = iterMap->first;
            ++iterMap;
         }
         rc = buildTransSession( groupLst, pRouteAgent, cb );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to build transaction session(rc=%d)",
                     rc );
      }

      iterGroup = groupSubCLMap.begin();
      while( iterGroup != groupSubCLMap.end() )
      {
         CoordGroupInfoPtr groupInfo;
         rc = buildOpMsg( cataInfo, iterGroup->second,
                        (CHAR *)pSrcMsg, pBuffer, bufferSize );
         PD_CHECK( SDB_OK == rc, rc, RECV_MSG, PDERROR,
                  "failed to build query message(rc=%d)", rc );
         rc = rtnCoordGetGroupInfo( cb, iterGroup->first, isNeedRefresh, groupInfo );
         PD_CHECK( SDB_OK == rc, rc, RECV_MSG, PDERROR,
                  "failed to get group info(groupId=%u, rc=%d)",
                  iterGroup->first, rc );
         rc = rtnCoordSendRequestToPrimary( pBuffer, groupInfo, sendNodes,
                                       pRouteAgent, MSG_ROUTE_SHARD_SERVCIE, cb );
         PD_CHECK( SDB_OK == rc, rc, RECV_MSG, PDERROR,
                  "failed to send request(rc=%d)", rc );
         ++iterGroup;
      }

   RECV_MSG:
      rcTmp = rtnCoordGetReply( cb, sendNodes, replyQue,
                                MAKE_REPLY_TYPE( pSrcMsg->opCode ) ) ;
      if ( SDB_OK != rcTmp )
      {
         PD_LOG( PDWARNING, "failed to get reply(rcTmp=%d)", rcTmp );
         if ( SDB_APP_INTERRUPT == rcTmp || SDB_OK == rc )
         {
            rc = rcTmp;
         }
      }
      while( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = (MsgOpReply *)(replyQue.front());
         replyQue.pop();
         rcTmp = pReply->flags;
         UINT32 groupID = pReply->header.routeID.columns.groupID;
         if ( rcTmp != SDB_OK
            && ignoreRCList.find( rcTmp ) == ignoreRCList.end() )
         {
            if ( SDB_OK == rc )
            {
               rc = rcTmp;
            }
         }
         else
         {
            if ( modifyNum && pReply->contextID > 0 )
            {
               *modifyNum += pReply->contextID;
            }
            sendGroupList[ groupID ] = groupID;
            groupSubCLMap.erase( groupID );
         }
         if ( NULL != pReply )
         {
            SDB_OSS_FREE( pReply );
            pReply = NULL;
         }
      }
   done:
      if ( pBuffer != NULL )
      {
         SDB_OSS_FREE( pBuffer );
         pBuffer = NULL;
      }
      return rc;
   error:
      adjustTransSession( sendGroupList, pRouteAgent, cb );
      goto done;
   }

   void rtnCoordTransOperator::adjustTransSession( CoordGroupList &transGroupLst,
                                                   netMultiRouteAgent *pRouteAgent,
                                                   pmdEDUCB *cb )
   {
      DpsTransNodeMap *pTransNodeList = cb->getTransNodeLst();
      if ( pTransNodeList )
      {
         DpsTransNodeMap::iterator iter = pTransNodeList->begin();
         while ( iter != pTransNodeList->end() )
         {
            if ( transGroupLst.find( iter->first )
               == transGroupLst.end() )
            {
               pTransNodeList->erase( iter++ );
               continue;
            }
            ++iter;
         }
      }
   }

   /*
      rtnCoordMsg implement
   */
   INT32 rtnCoordMsg::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                               CHAR **ppResultBuffer, pmdEDUCB *cb,
                               MsgOpReply &replyHeader, BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *pKrcb = pmdGetKRCB() ;
      CoordCB *pCoordcb = pKrcb->getCoordCB() ;
      netMultiRouteAgent *pRouteAgent = pCoordcb->getRouteAgent() ;

      INT32 rcTmp = SDB_OK ;
      REPLY_QUE replyQue ;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode        = MSG_BS_MSG_RES ;
      replyHeader.header.requestID     = pHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID           = pHeader->TID ;
      replyHeader.contextID            = -1 ;
      replyHeader.flags                = SDB_OK ;
      replyHeader.numReturned          = 0 ;
      replyHeader.startFrom            = 0 ;
      pHeader->TID = cb->getTID() ;

      CoordGroupList groupLst ;

      ROUTE_SET sendNodes ;
      REQUESTID_MAP successNodes ;
      ROUTE_RC_MAP failedNodes ;

      rtnMsg( (MsgOpMsg *)pReceiveBuffer ) ;

      rc = rtnCoordGetAllGroupList( cb, groupLst, NULL, FALSE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get all group list, rc: %d", rc ) ;

      rc = rtnCoordGetGroupNodes( cb, BSONObj(), NODE_SEL_ALL,
                                  groupLst, sendNodes ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get nodes, rc: %d", rc ) ;
      if ( sendNodes.size() == 0 )
      {
         PD_LOG( PDWARNING, "Not found any node" ) ;
         rc = SDB_CLS_NODE_NOT_EXIST ;
         goto error ;
      }

      rtnCoordSendRequestToNodes( pReceiveBuffer, sendNodes, 
                                  pRouteAgent, cb, successNodes,
                                  failedNodes ) ;
      rcTmp = rtnCoordGetReply( cb, successNodes, replyQue, MSG_BS_MSG_RES,
                                TRUE, FALSE ) ;
      if ( rcTmp != SDB_OK )
      {
         PD_LOG( PDERROR, "Failed to get the reply, rc", rcTmp ) ;
      }

      if ( failedNodes.size() != 0 )
      {
         rc = rcTmp ? rcTmp : failedNodes.begin()->second ;
         goto error ;
      }

   done:
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL ;
         pReply = ( MsgOpReply *)( replyQue.front() ) ;
         replyQue.pop() ;
         SDB_OSS_FREE( pReply ) ;
      }
      return rc ;
   error:
      rtnCoordClearRequest( cb, successNodes ) ;
      replyHeader.flags = rc ;
      goto done ;
   }

}

