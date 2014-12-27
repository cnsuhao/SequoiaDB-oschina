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

   Source File Name = rtnCoordTransaction.cpp

   Descriptive Name = Runtime Coord Transaction

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   transaction management on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoordTransaction.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "msgMessage.hpp"

namespace engine
{
   INT32 rtnCoordTransBegin::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                    CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                    MsgOpReply & replyHeader,
                                    BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_TRANS_BEGIN_RSP;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;
      rc = cb->createTransaction();
      PD_RC_CHECK( rc, PDERROR,
                  "create transaction failed(rc=%d)",
                  rc );
   done:
      return rc;
   error:
      replyHeader.flags = rc;
      goto done;
   }

   INT32 rtnCoord2PhaseCommit::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                    CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                    MsgOpReply & replyHeader,
                                    BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      INT32 rcTmp = SDB_OK;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_TRANS_COMMIT_RSP;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      if ( !cb->isTransaction() )
      {
         rc = SDB_DPS_TRANS_NO_TRANS;
         goto error;
      }

      rc = doPhase1( pReceiveBuffer, packSize, ppResultBuffer, cb, replyHeader );
      PD_CHECK( SDB_OK == rc, rc, errorcancel, PDERROR,
               "execute failed on phase1(rc=%d)",
               rc );

      rc = doPhase2( pReceiveBuffer, packSize, ppResultBuffer, cb, replyHeader );
      PD_RC_CHECK( rc, PDERROR,
                  "execute failed on phase2(rc=%d)",
                  rc );
   done:
      return rc;

   errorcancel:
      rcTmp = cancelOp( pReceiveBuffer, packSize, ppResultBuffer, cb, replyHeader );
      if ( rcTmp )
      {
         PD_LOG ( PDERROR,
                  "failed to cancel the operate(rc=%d)",
                  rcTmp );
      }

   error:
      replyHeader.flags = rc;
      goto done;
   }

   INT32 rtnCoord2PhaseCommit::doPhase1( CHAR * pReceiveBuffer, SINT32 packSize,
                                 CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                 MsgOpReply & replyHeader )
   {
      INT32 rc = SDB_OK;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      CHAR *pMsgReq                    = NULL;
      MsgHeader *pMsgHead              = NULL;
      CoordGroupList groupLst;
      CoordGroupList sendGroupLst;

      rc = buildPhase1Msg( pReceiveBuffer, &pMsgReq );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to build the message on phase1(rc=%d)",
                  rc );

      pMsgHead = (MsgHeader *)pMsgReq;
      pMsgHead->TID = cb->getTID();

      rc = executeOnDataGroup( pMsgReq, pRouteAgent, cb );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to execute on data-group on phase1(rc=%d)",
                  rc );
   done:
      if ( pMsgReq )
      {
         SDB_OSS_FREE( pMsgReq );
         pMsgReq = NULL;
      }
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoord2PhaseCommit::doPhase2( CHAR * pReceiveBuffer, SINT32 packSize,
                                         CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                         MsgOpReply & replyHeader )
   {
      INT32 rc = SDB_OK;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      CHAR *pMsgReq                    = NULL;
      MsgHeader *pMsgHead              = NULL;
      CoordGroupList groupLst;
      CoordGroupList sendGroupLst;

      rc = buildPhase2Msg( pReceiveBuffer, &pMsgReq );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to build the message on phase1(rc=%d)",
                  rc );

      pMsgHead = (MsgHeader *)pMsgReq;
      pMsgHead->TID = cb->getTID();

      rc = executeOnDataGroup( pMsgReq, pRouteAgent, cb );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to execute on data-group on phase1(rc=%d)",
                  rc );
   done:
      if ( pMsgReq )
      {
         SDB_OSS_FREE( pMsgReq );
         pMsgReq = NULL;
      }
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoord2PhaseCommit::cancelOp( CHAR * pReceiveBuffer, SINT32 packSize,
                                         CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                         MsgOpReply & replyHeader )
   {
      return SDB_OK;
   }

   INT32 rtnCoordTransCommit::executeOnDataGroup( CHAR * pMsg,
                                                  netMultiRouteAgent * pRouteAgent,
                                                  pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK;
      REQUESTID_MAP requestIdMap;
      REPLY_QUE replyQue;
      MsgHeader *pMsgHead = (MsgHeader *)pMsg;
      DpsTransNodeMap *pNodeMap = cb->getTransNodeLst();
      DpsTransNodeMap::iterator iterMap = pNodeMap->begin();
      while( iterMap != pNodeMap->end() )
      {
         rc = rtnCoordSendRequestToNode( (void *)pMsg, iterMap->second,
                                         pRouteAgent, cb, requestIdMap );
         if ( rc )
         {
            rtnCoordClearRequest( cb, requestIdMap );
         }
         PD_RC_CHECK( rc, PDERROR,
                     "failed to send the request to the node"
                     "(groupID=%u, nodeID=%u, rc=%d). ",
                     iterMap->second.columns.groupID,
                     iterMap->second.columns.nodeID,
                     rc );
         ++iterMap;
      }
      rc = rtnCoordGetReply( cb, requestIdMap, replyQue,
                             MAKE_REPLY_TYPE( pMsgHead->opCode ) ) ;
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get the reply(rc=%d)",
                  rc );
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = (MsgOpReply *)(replyQue.front());
         replyQue.pop();
         INT32 rcTmp = pReply->flags;
         if ( rcTmp != SDB_OK )
         {
            rc = rc ? rc : rcTmp;
            PD_LOG( PDERROR,
                  "failed to execute on data node(rc=%d, groupID=%u, nodeID=%u)",
                  rcTmp, pReply->header.routeID.columns.groupID,
                  pReply->header.routeID.columns.nodeID );
         }
         SDB_OSS_FREE( pReply );
      }
      if ( rc )
      {
         goto error;
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordTransCommit::buildPhase1Msg( CHAR * pReceiveBuffer, CHAR **pMsg )
   {
      SDB_ASSERT( pMsg, "pMsg can't be NULL" ) ;
      INT32 bufferSize = 0;
      if ( *pMsg != NULL )
      {
         SDB_OSS_FREE( pMsg );
         *pMsg = NULL;
      }
      return msgBuildTransCommitPreMsg( pMsg, &bufferSize );
   }

   INT32 rtnCoordTransCommit::buildPhase2Msg( CHAR * pReceiveBuffer, CHAR **pMsg )
   {
      SDB_ASSERT( pMsg, "pMsg can't be NULL" ) ;
      INT32 bufferSize = 0;
      if ( *pMsg != NULL )
      {
         SDB_OSS_FREE( pMsg );
         *pMsg = NULL;
      }
      return msgBuildTransCommitMsg( pMsg, &bufferSize );
   }

   INT32 rtnCoordTransCommit::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                    CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                    MsgOpReply & replyHeader,
                                    BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      rtnCoordOperator *pRollbackOperator = NULL;
      rc = rtnCoord2PhaseCommit::execute( pReceiveBuffer, packSize, ppResultBuffer,
                                       cb, replyHeader, ppErrorObj );
      SDB_ASSERT( NULL == *ppErrorObj, "impossible" ) ;
      PD_RC_CHECK( rc, PDERROR,
                  "failed to commit the transaction(rc=%d)",
                  rc );
      cb->delTransaction();
   done:
      return rc;
   error:
      pRollbackOperator = pmdGetKRCB()->getCoordCB()->getProcesserFactory(
         )->getOperator( MSG_BS_TRANS_ROLLBACK_REQ );
      if ( pRollbackOperator )
      {
         pRollbackOperator->execute( pReceiveBuffer, packSize, ppResultBuffer,
                                    cb, replyHeader, ppErrorObj );
         SDB_ASSERT( NULL == *ppErrorObj, "impossible" ) ;
      }
      goto done;
   }

   INT32 rtnCoordTransRollback::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                         CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                         MsgOpReply & replyHeader,
                                         BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      CHAR *pMsgReq                    = NULL;
      MsgHeader *pMsgHead              = NULL;
      INT32 bufferSize                 = 0;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_TRANS_ROLLBACK_RSP;
      replyHeader.header.routeID.value = 0;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      if ( pHeader )
      {
         replyHeader.header.requestID     = pHeader->requestID;
         replyHeader.header.TID           = pHeader->TID;
      }

      if ( !cb->isTransaction() )
      {
         rc = SDB_DPS_TRANS_NO_TRANS;
         goto error;
      }

      cb->startRollback();

      rc = msgBuildTransRollbackMsg( &pMsgReq, &bufferSize );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to build the message(rc=%d)",
                  rc );

      pMsgHead = (MsgHeader *)pMsgReq;
      pMsgHead->TID = cb->getTID();
      rc = executeOnDataGroup( pMsgReq, pRouteAgent, cb );
      cb->delTransaction();
      PD_RC_CHECK( rc, PDERROR,
                  "failed to rollback(rc=%d)",
                  rc );
   done:
      if ( pMsgReq )
      {
         SDB_OSS_FREE( pMsgReq );
      }
      cb->stopRollback();
      return rc;
   error:
      replyHeader.flags = rc;
      goto done;
   }

   INT32 rtnCoordTransRollback::executeOnDataGroup( CHAR * pMsg,
                                                    netMultiRouteAgent * pRouteAgent,
                                                    pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK;
      REQUESTID_MAP requestIdMap;
      REPLY_QUE replyQue;
      MsgHeader *pMsgHead = (MsgHeader *)pMsg;
      DpsTransNodeMap *pNodeMap = cb->getTransNodeLst();
      DpsTransNodeMap::iterator iterMap;
      if ( NULL == pNodeMap )
      {
         goto done;
      }
      iterMap = pNodeMap->begin();
      while( iterMap != pNodeMap->end() )
      {
         rc = rtnCoordSendRequestToNode( (void *)pMsg, iterMap->second,
                                         pRouteAgent, cb, requestIdMap );
         if ( rc )
         {
            rtnCoordClearRequest( cb, requestIdMap );
            PD_LOG ( PDWARNING,
                     "failed to send rollback request to the node"
                     "(groupID=%u, nodeID=%u, rc=%d). "
                     "the node will perform rollback automatically.",
                     iterMap->second.columns.groupID,
                     iterMap->second.columns.nodeID,
                     rc );
         }
         ++iterMap;
      }
      rc = rtnCoordGetReply( cb, requestIdMap, replyQue,
                             MAKE_REPLY_TYPE( pMsgHead->opCode ));
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get the reply(rc=%d)",
                  rc );
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = (MsgOpReply *)(replyQue.front());
         replyQue.pop();
         INT32 rcTmp = pReply->flags;
         if ( rcTmp != SDB_OK )
         {
            rc = rc ? rc : rcTmp;
            PD_LOG( PDERROR,
                  "failed to execute on data node(rc=%d, groupID=%u, nodeID=%u)",
                  rc, pReply->header.routeID.columns.groupID,
                  pReply->header.routeID.columns.nodeID );
         }
         SDB_OSS_FREE( pReply );
      }
      if ( rc )
      {
         goto error;
      }
   done:
      return rc;
   error:
      goto done;
   }
}
