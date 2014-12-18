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

   Source File Name = pmdAgent.cpp

   Descriptive Name = Process MoDel Agent

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include <stdio.h>
#include "pd.hpp"
#include "pmdEDUMgr.hpp"
#include "pmdEDU.hpp"
#include "msgMessage.hpp"
#include "oss.hpp"
#include "ossSocket.hpp"
#include "ossMem.hpp"
#include "ossUtil.hpp"
#include "rtn.hpp"
#include "../bson/bson.h"
#include "pmd.hpp"
#include "restAdaptorold.hpp"
#include "rtnCoord.hpp"
#include "rtnCoordCommands.hpp"
#include "coordSession.hpp"
#include "pmdSession.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "pmdCB.hpp"
#include "pmdProcessor.hpp"

using namespace bson ;

namespace engine
{
#define PMD_AGENT_RECIEVE_BUFFER_SZ 4096

#define PMD_AGENT_SOCKET_DFT_TIMEOUT 10

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDHANDLESYSINFOREQUEST, "pmdHandleSysInfoRequest" )
   static INT32 pmdHandleSysInfoRequest ( CHAR *pReceiveBuffer,
                                          INT32 receiveBufferSize,
                                          ossSocket &sock,
                                          pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN endianConvert ;
      MsgSysInfoReply reply ;
      PD_TRACE_ENTRY ( SDB_PMDHANDLESYSINFOREQUEST ) ;
      MsgSysInfoReply *pReply = &reply ;
      INT32 replySize = sizeof(reply) ;
      INT32 recvSize    = (INT32)sizeof( MsgSysInfoRequest ) ;

      SDB_ASSERT ( recvSize < receiveBufferSize,
                   "receive buffer size should not be smaller "
                   "than msg info request" ) ;

      rc = pmdRecv ( &pReceiveBuffer[sizeof(SINT32)],
                     recvSize - sizeof(SINT32),
                     &sock, cb ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to receive packet, rc = %d", rc ) ;

      rc = msgExtractSysInfoRequest ( pReceiveBuffer, endianConvert ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to extract sys info request, rc = %d", rc ) ;

      rc = msgBuildSysInfoReply ( (CHAR**)&pReply, &replySize ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to build sys info reply, rc = %d", rc ) ;
      rc = pmdSend ( (CHAR*)pReply, replySize, &sock, cb ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to send packet, rc = %d", rc ) ;
   done :
      PD_TRACE_EXITRC ( SDB_PMDHANDLESYSINFOREQUEST, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   /*
    * This is the master function to process coord agent request
    * This function interpret the incoming request and break the packet into
    * different variables, and call Runtime component to execute the request
    */
   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDPROCCOORDAGENTREQ, "pmdProcessCoordAgentRequest" )
   static INT32 pmdProcessCoordAgentRequest( CHAR *pReceiveBuffer,
                                             SINT32 packetSize,
                                             rtnContextBuf &buffObj,
                                             BOOLEAN *disconnect,
                                             pmdEDUCB *cb,
                                             MsgOpReply &replyHeader,
                                             BSONObj **ppErrorObj )
   {
      SDB_ASSERT ( disconnect, "disconnect can't be NULL" ) ;
      SDB_ASSERT ( pReceiveBuffer, "pReceivedBuffer is NULL" ) ;
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_PMDPROCCOORDAGENTREQ ) ;

      MsgHeader *pHead  = (MsgHeader *)pReceiveBuffer;
      CHAR *pResultBuff = NULL ;

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode = MAKE_REPLY_TYPE( pHead->opCode ) ;
      replyHeader.header.requestID = pHead->requestID ;
      replyHeader.header.routeID.value = pmdGetNodeID().value ;
      replyHeader.header.TID = pHead->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      do
      {
         if ( (UINT32)(pHead->messageLength) < sizeof(MsgHeader) )
         {
            PD_LOG ( PDERROR, "Invalid message length: %d",
                     pHead->messageLength ) ;
            rc = SDB_INVALIDARG;
            break;
         }
         pmdKRCB *pKrcb                   = pmdGetKRCB();
         CoordCB *pCoordcb                = pKrcb->getCoordCB();
         SDB_RTNCB *pRTNCB                = pKrcb->getRTNCB() ;
         rtnCoordProcesserFactory *pProcesserFactory
                                    = pCoordcb->getProcesserFactory();
         PD_TRACE1 ( SDB_PMDPROCCOORDAGENTREQ, PD_PACK_INT(pHead->opCode) );
         switch ( pHead->opCode )
         {
         case MSG_BS_GETMORE_REQ :
            rc = SDB_COORD_UNKNOWN_OP_REQ ;
            break ;
         case MSG_BS_QUERY_REQ:
            {
               MsgOpQuery *pQueryMsg   = (MsgOpQuery *)pReceiveBuffer ;
               CHAR *pQueryName        = pQueryMsg->name ;
               SINT32 queryNameLen     = pQueryMsg->nameLength ;
               if ( queryNameLen > 0 && '$' == pQueryName[0] )
               {
                  rtnCoordOperator *pCmdProcesser =
                     pProcesserFactory->getCommandProcesser( pQueryMsg );
                  rc = pCmdProcesser->execute( pReceiveBuffer,
                                               packetSize,
                                               &pResultBuff,
                                               cb,
                                               replyHeader,
                                               ppErrorObj ) ;
                  break ;
               }
            }
         default:
            {
               rtnContextBase *pContext = NULL ;
               rtnCoordOperator *pOperator =
                  pProcesserFactory->getOperator( pHead->opCode );
               rc = pOperator->execute( pReceiveBuffer,
                                        packetSize,
                                        &pResultBuff,
                                        cb,
                                        replyHeader,
                                        ppErrorObj ) ;
               if ( MSG_BS_QUERY_REQ == pHead->opCode &&
                    -1 != replyHeader.contextID &&
                    NULL != ( pContext = pRTNCB->contextFind(
                              replyHeader.contextID ) ) &&
                    ( ((MsgOpQuery*)pHead)->flags & FLG_QUERY_WITH_RETURNDATA ) )
               {
                  rc = pContext->getMore( -1, buffObj, cb ) ;
                  if ( rc || pContext->eof() )
                  {
                     pRTNCB->contextDelete( replyHeader.contextID, cb ) ;
                     replyHeader.contextID = -1 ;
                  }
                  replyHeader.startFrom = ( INT32 )buffObj.getStartFrom() ;
                  replyHeader.header.messageLength = sizeof( MsgOpReply ) ;

                  if ( SDB_DMS_EOC == rc )
                  {
                     rc = SDB_OK ;
                  }
                  else if ( rc )
                  {
                     PD_LOG( PDERROR, "Failed to query with return data, "
                             "rc: %d", rc ) ;
                  }
                  replyHeader.flags = rc ;
               }
            }
            break;
         }
      }while ( FALSE ) ;

      if ( ( MSG_BS_LOB_OPEN_REQ == pHead->opCode ||
           MSG_BS_LOB_READ_REQ == pHead->opCode ) &&
           NULL != pResultBuff )
      {
         INT32 dataLen = replyHeader.header.messageLength -
                         sizeof( MsgOpReply ) ;
         buffObj = rtnContextBuf( pResultBuff, dataLen, 1 ) ;
      }
      else
      {
         SDB_ASSERT( pResultBuff == NULL, "Result must be NULL" ) ;
      }

      if ( rc && buffObj.size() == 0 )
      {
         if ( *ppErrorObj )
         {
            buffObj = rtnContextBuf( **ppErrorObj ) ;
         }
         else
         {
            BSONObj obj = utilGetErrorBson( rc,
                                            cb->getInfo( EDU_INFO_ERROR ) ) ;
            buffObj = rtnContextBuf( obj ) ;
         }
      }

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.flags = rc ;

      PD_TRACE_EXITRC ( SDB_PMDPROCCOORDAGENTREQ, rc );
      return rc;
   }

   /*
    * This is the master function to process agent request
    * This function interpret the incoming request and break the packet into
    * different variables, and call Runtime component to execute the request
    */
   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDPROCAGENTREQ, "pmdProcessAgentRequest" )
   static INT32 pmdProcessAgentRequest ( CHAR *pReceiveBuffer,
                                         SINT32 packetSize,
                                         rtnContextBuf &buffObj,
                                         BOOLEAN *disconnect, pmdEDUCB *cb,
                                         MsgOpReply &replyHeader )
   {
      INT32 rc                 = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDPROCAGENTREQ ) ;
      UINT64 requestID         = 0 ;
      SINT32 opCode            = 0 ;
      UINT32 probe             = 0 ;
      BOOLEAN isNeedRollback   = FALSE ;
      pmdKRCB *krcb        = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB     = krcb->getDMSCB () ;
      SDB_RTNCB *rtnCB     = krcb->getRTNCB () ;
      SDB_DPSCB *dpsCB     = krcb->getDPSCB () ;
      SDB_ASSERT ( disconnect, "disconnect can't be NULL" ) ;
      SDB_ASSERT ( pReceiveBuffer, "pReceivedBuffer is NULL" ) ;
      *disconnect          = FALSE ;
      MsgHeader *header    = (MsgHeader *)pReceiveBuffer ;
      INT32 flags          = 0 ;
      SINT64 numToSkip     = -1 ;
      SINT64 numToReturn   = -1 ;
      SINT64 contextID     = -1 ;   // contextID
      SINT32 numToRead     = 0 ;
      CHAR *pCollectionName= NULL ;
      CHAR *pMatcherBuffer = NULL ;
      CHAR *pSelectorBuffer= NULL ;
      CHAR *pUpdatorBuffer = NULL ;
      CHAR *pInsertorBuffer= NULL ;
      CHAR *pDeletorBuffer = NULL ;
      CHAR *pOrderByBuffer = NULL ;
      CHAR *pHintBuffer    = NULL ;
      _rtnCommand *pCommand = NULL ;

      MON_START_OP( cb->getMonAppCB() ) ;

      if ( dpsCB && cb->isFromLocal() && !dpsCB->isLogLocal() )
      {
         dpsCB = NULL ;
      }

      if ( (UINT32)header->messageLength < sizeof (MsgHeader) )
      {
         probe = 5 ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      requestID = header->requestID ;
      opCode = header->opCode ;
      PD_TRACE2 ( SDB_PMDPROCAGENTREQ, PD_PACK_ULONG(requestID),
                  PD_PACK_INT(opCode) ) ;

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode = MAKE_REPLY_TYPE( opCode ) ;
      replyHeader.header.requestID = requestID ;
      replyHeader.header.routeID.value = pmdGetNodeID().value ;
      replyHeader.header.TID = header->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      try
      {
         if ( MSG_BS_INTERRUPTE == opCode )
         {
            PD_LOG ( PDEVENT, "Recieve interrupt msg in session[%lld, %s]", 
                     cb->getID(), cb->getName() ) ;

            while ( -1 != (contextID = cb->contextPeek() ))
               rtnCB->contextDelete( contextID, NULL ) ;

            rc = SDB_OK ;
         }
         else if ( MSG_BS_MSG_REQ == opCode )
         {
            rc = rtnMsg ( (MsgOpMsg *)header ) ;
         }
         else if ( MSG_BS_UPDATE_REQ == opCode )
         {
            isNeedRollback = TRUE ;
            PD_LOG ( PDDEBUG, "Update request received" ) ;
            rc = msgExtractUpdate ( pReceiveBuffer, &flags, &pCollectionName,
                                    &pMatcherBuffer, &pUpdatorBuffer,
                                    &pHintBuffer ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to read update packet" ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            try
            {
               BSONObj matcher( pMatcherBuffer ) ;
               BSONObj updator ( pUpdatorBuffer ) ;
               BSONObj hint ( pHintBuffer ) ;
               MON_SAVE_OP_DETAIL( cb->getMonAppCB(), MSG_BS_UPDATE_REQ,
                           "CL:%s, Match:%s, Updator:%s, Hint:%s",
                           pCollectionName,
                           matcher.toString().c_str(),
                           updator.toString().c_str(),
                           hint.toString().c_str() ) ;
#if defined (_DEBUG)
               PD_LOG ( PDDEBUG,
                       "Update: matcher: %s\nupdator: %s\nhint: %s",
                       matcher.toString().c_str(),
                       updator.toString().c_str(),
                       hint.toString().c_str() ) ;
#endif
               rc = rtnUpdate ( pCollectionName,
                                matcher,
                                updator,
                                hint,
                                flags, cb,
                                dmsCB,
                                dpsCB ) ;
            }
            catch ( std::exception &e )
            {
               PD_LOG ( PDERROR, "Failed to create matcher and updator for "
                        "UPDATE: %s", e.what() ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
         }
         else if ( MSG_BS_INSERT_REQ == opCode )
         {
            isNeedRollback = TRUE ;
            INT32 recordNum = 0 ;
            PD_LOG ( PDDEBUG, "Insert request received" ) ;
            rc = msgExtractInsert ( pReceiveBuffer, &flags, &pCollectionName,
                                    &pInsertorBuffer, recordNum ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to read insert packet" ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }

            try
            {
               BSONObj insertor ( pInsertorBuffer ) ;
               MON_SAVE_OP_DETAIL( cb->getMonAppCB(), MSG_BS_INSERT_REQ,
                           "CL:%s, Insertor:%s",
                           pCollectionName,
                           insertor.toString( false, false ).c_str() ) ;
#if defined (_DEBUG)
               PD_LOG ( PDDEBUG,
                       "Insert: insertor: %s\nCollection: %s",
                       insertor.toString().c_str(),
                       pCollectionName ) ;
#endif
               rc = rtnInsert ( pCollectionName, insertor, recordNum,
                                flags, cb, dmsCB, dpsCB ) ;
            }
            catch ( std::exception &e )
            {
               PD_LOG ( PDERROR, "Failed to create insertor for INSERT: %s",
                        e.what() ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
         }
         else if ( MSG_BS_QUERY_REQ == opCode )
         {
            PD_LOG ( PDDEBUG, "Query request received" ) ;
            rc = msgExtractQuery ( pReceiveBuffer, &flags, &pCollectionName,
                                   &numToSkip, &numToReturn, &pMatcherBuffer,
                                   &pSelectorBuffer, &pOrderByBuffer,
                                   &pHintBuffer ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to read query packet" ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }

            if ( rtnIsCommand( pCollectionName ) )
            {
               PD_LOG ( PDDEBUG, "Command: %s", pCollectionName ) ;

               rc = rtnParserCommand ( pCollectionName, &pCommand ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG ( PDERROR, "parse command[%s] failed[rc:%d]",
                     pCollectionName, rc ) ;
                  goto error ;
               }
               rc = rtnInitCommand( pCommand, flags, numToSkip, numToReturn,
                                    pMatcherBuffer, pSelectorBuffer,
                                    pOrderByBuffer, pHintBuffer ) ;
               if ( SDB_OK != rc )
               {
                  goto error ;
               }
               rc = rtnRunCommand( pCommand, CMD_SPACE_SERVICE_LOCAL,
                                   cb, dmsCB, rtnCB,
                                   dpsCB, 1, &contextID ) ;
            }
            else
            {
               try
               {
                  BSONObj matcher ( pMatcherBuffer ) ;
                  BSONObj selector ( pSelectorBuffer ) ;
                  BSONObj orderBy ( pOrderByBuffer ) ;
                  BSONObj hint ( pHintBuffer ) ;
                  MON_SAVE_OP_DETAIL( cb->getMonAppCB(), MSG_BS_QUERY_REQ,
                              "CL:%s, Match:%s, Selector:%s, OrderBy:%s, Hint:%s",
                              pCollectionName,
                              matcher.toString().c_str(),
                              selector.toString().c_str(),
                              orderBy.toString().c_str(),
                              hint.toString().c_str() ) ;

#if defined (_DEBUG)
                  PD_LOG ( PDDEBUG,
                           "Query: matcher: %s\nselector: %s\n"
                           "orderBy: %s\nhint: %s",
                           matcher.toString().c_str(),
                           selector.toString().c_str(),
                           orderBy.toString().c_str(),
                           hint.toString().c_str() ) ;
#endif
                  rc = rtnQuery ( pCollectionName, // collection name
                                  selector, // selector
                                  matcher, // matcher
                                  orderBy, // orderBy
                                  hint,    // hint
                                  flags, // query flag
                                  cb,    // EDU control block
                                  numToSkip, // number of element to skip
                                  numToReturn, // number of element to return
                                  dmsCB, // dms control block
                                  rtnCB, // runtime control block
                                  contextID,
                                  NULL, TRUE ); // output: newly created context
               }
               catch ( std::exception &e )
               {
                  PD_LOG ( PDERROR, "Failed to create matcher and selector "
                           "for QUERY: %s", e.what() ) ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
            }
         }
         else if ( MSG_BS_GETMORE_REQ == opCode )
         {
            PD_LOG ( PDDEBUG, "GetMore request received" ) ;
            rc = msgExtractGetMore ( pReceiveBuffer, &numToRead, &contextID );
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to read getmore packet" ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            MON_SAVE_OP_DETAIL( cb->getMonAppCB(), MSG_BS_GETMORE_REQ,
                        "ContextID:%lld, NumToRead:%d",
                        contextID, numToRead ) ;
            rc = rtnGetMore ( contextID, numToRead, buffObj, cb, rtnCB ) ;
         }
         else if ( MSG_BS_DELETE_REQ == opCode )
         {
            isNeedRollback = TRUE ;
            PD_LOG ( PDDEBUG, "Delete request received" ) ;
            rc = msgExtractDelete ( pReceiveBuffer, &flags, &pCollectionName,
                                    &pDeletorBuffer, &pHintBuffer ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to read delete packet" ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            try
            {
               BSONObj deletor ( pDeletorBuffer ) ;
               BSONObj hint ( pHintBuffer ) ;
               MON_SAVE_OP_DETAIL( cb->getMonAppCB(), MSG_BS_DELETE_REQ,
                           "CL:%s, Deletor:%s, Hint:%s",
                           pCollectionName,
                           deletor.toString( false, false ).c_str(),
                           hint.toString( false, false ).c_str() );
#if defined (_DEBUG)
               PD_LOG( PDDEBUG, "Delete: deletor: %s\nhint: %s",
                       deletor.toString().c_str(),
                       hint.toString().c_str() ) ;
#endif
               rc = rtnDelete ( pCollectionName, deletor,
                                hint, flags, cb, dmsCB, dpsCB ) ;
            }
            catch ( std::exception &e )
            {
               PD_LOG ( PDERROR, "Failed to create deletor for DELETE: %s",
                        e.what() ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
         }
         else if ( MSG_BS_KILL_CONTEXT_REQ == opCode )
         {
            PD_LOG ( PDDEBUG, "Contexts request received" ) ;
            SINT32 numContexts ;
            SINT64 *pContextIDs ;
            rc = msgExtractKillContexts ( pReceiveBuffer, &numContexts,
                                          &pContextIDs ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to read kill contexts packet" ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            rc = rtnKillContexts ( numContexts, pContextIDs, cb, rtnCB ) ;
         }
         else if ( MSG_BS_DISCONNECT == opCode )
         {
            PD_LOG ( PDEVENT, "Recieve disconnect msg in session[%lld, %s]", 
                     cb->getID(), cb->getName() ) ;
            *disconnect = TRUE ;
         }
         else if ( MSG_BS_SQL_REQ == opCode )
         {
            CHAR *sql = NULL ;
            rc = msgExtractSql( pReceiveBuffer, &sql ) ;
            SQL_CB *sqlcb = krcb->getSqlCB() ;
            rc = sqlcb->exec( sql, cb, contextID ) ;
         }
         else if ( MSG_BS_TRANS_BEGIN_REQ == opCode )
         {
            if ( krcb->getDBRole() != SDB_ROLE_STANDALONE )
            {
               rc = SDB_PERM;
               PD_LOG( PDERROR, "In sharding mode, couldn't execute "
                       "transaction operation from local service" ) ;
            }
            else
            {
               rc = rtnTransBegin( cb );
            }
         }
         else if ( MSG_BS_TRANS_COMMIT_REQ == opCode )
         {
            isNeedRollback = TRUE ;
            if ( krcb->getDBRole() != SDB_ROLE_STANDALONE )
            {
               rc = SDB_PERM;
               PD_LOG( PDERROR, "In sharding mode, couldn't execute "
                       "transaction operation from local service") ;
            }
            else
            {
               rc = rtnTransCommit( cb, dpsCB );
            }
         }
         else if ( MSG_BS_TRANS_ROLLBACK_REQ == opCode )
         {
            if ( krcb->getDBRole() != SDB_ROLE_STANDALONE )
            {
               rc = SDB_PERM;
               PD_LOG( PDERROR, "In sharding mode, couldn't execute "
                       "transaction operation from local service" ) ;
            }
            else
            {
               rc = rtnTransRollback( cb, dpsCB );
            }
         }
         else if ( MSG_BS_AGGREGATE_REQ == opCode )
         {
            CHAR *pObjs = NULL;
            bson::BSONObj objs;
            INT32 count = 0;
            rc = msgExtractAggrRequest( pReceiveBuffer, &pCollectionName,
                                        &pObjs, count ) ;
            if ( rc )
            {
               PD_LOG( PDERROR, "Failed to parse aggregate-request(rc=%d)", rc ) ;
               goto error;
            }

            objs = bson::BSONObj( pObjs );
            rc = rtnAggregate( pCollectionName, objs, count, flags, cb,
                               dmsCB, contextID ) ;
         }
         else
         {
            probe = 150 ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Error happened during performing operation: %s",
                  e.what() ) ;
         probe = 160 ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG ( PDERROR, "Failed to perform operation, rc: %d", rc ) ;
         }
         goto error ;
      }

      replyHeader.contextID = contextID ;
      replyHeader.startFrom = (INT32)buffObj.getStartFrom() ;

   done :
      if ( pCommand )
      {
         rtnReleaseCommand( &pCommand ) ;
      }
      replyHeader.flags = rc ;
      if ( rc && buffObj.size() == 0 )
      {
         BSONObj obj = utilGetErrorBson( rc, cb->getInfo( EDU_INFO_ERROR ) ) ;
         buffObj = rtnContextBuf( obj ) ;
      }

      MON_END_OP( cb->getMonAppCB() ) ;
      PD_TRACE_EXITRC ( SDB_PMDPROCAGENTREQ, rc ) ;
      return rc ;
   error :
      if ( isNeedRollback )
      {
         INT32 rcTmp = rtnTransRollback( cb, dpsCB );
         if ( rcTmp )
         {
            PD_LOG( PDERROR, "Failed to rollback(rc=%d)", rc ) ;
         }
      }

      switch ( rc )
      {
      case SDB_INVALIDARG :
         PD_LOG ( PDERROR, "Invalid argument is received, probe: %d", probe ) ;
         break ;
      case SDB_DMS_EOC :
         PD_LOG ( PDDEBUG, "Hit end of resultset" ) ;
         break ;
      default :
         PD_LOG ( PDERROR, "System error, probe: %d", probe ) ;
         break ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDHTTPAGENTENTPNT, "pmdHTTPAgentEntryPoint" )
   INT32 pmdHTTPAgentEntryPoint ( pmdEDUCB * cb, void * pData )
   {
      INT32 rc                 = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDHTTPAGENTENTPNT );
      BSONObj *pErrorObj       = NULL ;
      INT32 sendRC             = SDB_OK ;
      BOOLEAN needFetch        = 0 ;
      UINT32 probe             = 0 ;
      EDUID myEDUID            = cb->getID () ;
      pmdEDUMgr * eduMgr       = cb->getEDUMgr() ;
      BOOLEAN disconnect       = FALSE ;
      CHAR *pReceiveBuffer     = NULL ;
      rtnContextBuf  buffObj ;
      BOOLEAN isSendHeader     = FALSE ;
      MsgOpReply replyHeader ;
      ossMemset ( &replyHeader, 0, sizeof( replyHeader ) ) ;
      SINT32 receiveBufferSize = 0 ;
      SINT32 packetLength = 0 ;
      SDB_ASSERT ( pData, "Null pointer passed to pmdAgentEntryPoint" ) ;
      SOCKET s = *(( SOCKET *) &pData ) ;
      CHAR clientName [ PMD_EDU_NAME_LENGTH + 1 ] = {0} ;
      UINT16 clientPort = 0 ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      monDBCB *mondbcb = krcb->getMonDBCB () ;
      SDB_ROLE dbrole = krcb->getDBRole () ;

      ossSocket sock ( &s, PMD_AGENT_SOCKET_DFT_TIMEOUT ) ;
      sock.disableNagle () ;

      restAdaptoro httpAdaptor ;

      clientPort = sock.getPeerPort () ;
      cb->setClientSock( &sock ) ;
      cb->resetInterrupt() ;
      cb->resetInfo ( EDU_INFO_ERROR ) ;

      rc = sock.getPeerAddress ( clientName, PMD_EDU_NAME_LENGTH ) ;
      if ( rc )
      {
         PD_LOG ( PDWARNING, "Failed to get address from socket" ) ;
      }
      else
      {
         cb->setClientInfo ( clientName, clientPort ) ;
         PD_LOG ( PDEVENT, "Connection is received from %s port %d",
                  clientName, clientPort ) ;
      }
      if ( SDB_ROLE_COORD == dbrole )
      {
         netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->getCoordCB()->getRouteAgent();
         rc = pRouteAgent->addSession( cb );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to add session(rc=%d)",
                     rc );
      }
      mondbcb->addReceiveNum () ;
      rc = httpAdaptor.getRequest ( sock, &pReceiveBuffer, &receiveBufferSize,
                                    needFetch ) ;
      if ( rc )
      {
         BSONObj errorObj = utilGetErrorBson( rc, NULL ) ;
         PD_LOG ( PDERROR, "Failed to get http request, rc = %d", rc ) ;
         if ( SDB_NETWORK == rc || SDB_NETWORK_CLOSE == rc )
         {
            goto error ;
         }
         replyHeader.numReturned = 1 ;
         replyHeader.flags = rc ;
         sendRC = httpAdaptor.sendReply ( replyHeader, errorObj.objdata(),
                                          errorObj.objsize(), sock, needFetch,
                                          isSendHeader ) ;
         if ( sendRC )
         {
            rc = sendRC ;
            if ( SDB_APP_FORCED == sendRC )
            {
               goto done ;
            }
            probe = 70 ;
            goto error ;
         }
         goto error ;
      }
      SDB_ASSERT ( pReceiveBuffer, "pReceiveBuffer can't be NULL" ) ;
      packetLength = *(SINT32*)(pReceiveBuffer) ;
      PD_LOG ( PDDEBUG, "Received packet size = %d", packetLength ) ;
      if ( packetLength < (SINT32)sizeof (SINT32) )
      {
         probe = 30 ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( SDB_OK != ( rc = eduMgr->activateEDU ( cb )) )
      {
         goto error ;
      }

   start :
      if ( SDB_ROLE_COORD == dbrole )
      {
         rc = pmdProcessCoordAgentRequest ( pReceiveBuffer, packetLength,
                                            buffObj, &disconnect,
                                            cb, replyHeader, &pErrorObj ) ;
      }

      if ( SDB_ROLE_STANDALONE == dbrole ||
           SDB_ROLE_CATALOG == dbrole ||
           SDB_ROLE_DATA == dbrole ||
           SDB_COORD_UNKNOWN_OP_REQ == rc )
      {
         rc = pmdProcessAgentRequest ( pReceiveBuffer, packetLength,
                                       buffObj, &disconnect, cb,
                                       replyHeader ) ;
      }

      if ( rc )
      {
         if ( SDB_APP_INTERRUPT == rc )
         {
            PD_LOG ( PDINFO, "Agent is interrupt" ) ;
         }
         else if ( SDB_DMS_EOC != rc )
         {
            PD_LOG ( PDERROR, "Error processing Agent request, rc = %d", rc ) ;
         }
      }

      sendRC = httpAdaptor.sendReply ( replyHeader, buffObj.data(),
                                       buffObj.size(),
                                       sock, needFetch, isSendHeader ) ;
      buffObj.release() ; //release lock for data prepare
      if ( sendRC )
      {
         rc = sendRC ;
         if ( SDB_APP_FORCED == sendRC )
         {
            goto done ;
         }
         probe = 70 ;
         goto error ;
      }
      if ( needFetch )
      {
         rc = msgBuildGetMoreMsg ( &pReceiveBuffer, &receiveBufferSize,
                                   -1, replyHeader.contextID,
                                   replyHeader.header.requestID ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to build get more request, rc = %d",
                     rc ) ;
            probe = 80 ;
            goto error ;
         }
         goto start ;
      }

      if ( SDB_OK != ( rc = eduMgr->waitEDU ( cb )) )
      {
         goto error ;
      }

   done :
      if ( pReceiveBuffer )
      {
         SDB_OSS_FREE ( pReceiveBuffer ) ;
      }

      if ( SDB_ROLE_STANDALONE == dbrole )
      {
         rc = rtnTransRollback( cb, pmdGetKRCB()->getDPSCB() );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to rollback(rc=%d)", rc );
         }
      }

      {
         pmdKRCB *krcb = pmdGetKRCB() ;
         SDB_RTNCB *rtnCB = krcb->getRTNCB() ;
         SINT64 contextID = -1 ;
         while ( -1 != (contextID = cb->contextPeek() ) )
            rtnCB->contextDelete( contextID, NULL ) ;
      }

      if ( SDB_ROLE_COORD == dbrole )
      {
         netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->getCoordCB(
            )->getRouteAgent() ;
         pRouteAgent->delSession( cb->getTID() );
      }

      cb->setClientSock(NULL) ;
      sock.close () ;

      if ( pErrorObj )
      {
         SDB_OSS_DEL pErrorObj ;
         pErrorObj = NULL ;
      }
      PD_TRACE_EXITRC ( SDB_PMDHTTPAGENTENTPNT, rc );
      return rc ;
   error :
      switch ( rc )
      {
      case SDB_SYS :
         PD_LOG ( PDSEVERE, "EDU id %d cannot be found, probe %d", myEDUID,
                  probe ) ;
         break ;
      case SDB_EDU_INVAL_STATUS :
         PD_LOG ( PDSEVERE, "EDU status is not valid, probe %d", probe ) ;
         break ;
      case SDB_INVALIDARG :
         PD_LOG ( PDSEVERE, "Invalid argument receieved by agent, probe %d",
                  probe ) ;
         break ;
      case SDB_OOM :
         PD_LOG ( PDSEVERE, "Failed to allocate memory by agent, probe %d",
                  probe ) ;
         break ;
      case SDB_NETWORK :
         PD_LOG ( PDSEVERE, "Network error occured, probe %d", probe ) ;
         break ;
      case SDB_NETWORK_CLOSE :
         PD_LOG ( PDDEBUG, "Remote connection closed" ) ;
         rc = SDB_OK ;
         break ;
      default :
         PD_LOG ( PDSEVERE, "Internal error, probe %d", probe ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDAUTHENTICATE, "pmdAuthenticate" )
   static INT32 pmdAuthenticate( ossSocket &sock, CHAR *&pBuffer,
                                 pmdEDUCB *cb,
                                 INT32 &receiveBufferSize,
                                 MsgOpReply &reply,
                                 SDB_ROLE role )
   {
      INT32 rc                         = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDAUTHENTICATE );
      UINT32 probe                     = 0 ;
      MsgHeader *header                = NULL ;
      SDB_ASSERT ( pBuffer, "pBuffer can't be NULL" ) ;
      SDB_ASSERT ( cb, "cb can't be NULL" ) ;
      cb->resetInterrupt () ;
      cb->resetInfo ( EDU_INFO_ERROR ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      shardCB *shdCB                   = NULL ;
      rtnCoordOperator *pOperator      = NULL ;
      rtnCoordProcesserFactory *pProcesserFactory
                                 = pCoordcb->getProcesserFactory();
      CHAR **ppResultBuffer = NULL ;
      INT32 packetLength = 0 ;
      MsgHeader *authRes = NULL ;

      reply.header.TID = 0 ;
      reply.header.requestID = 0 ;
      reply.header.opCode = MSG_AUTH_VERIFY_RES ;
      reply.header.messageLength = sizeof( reply ) ;

   retry :
      rc = pmdRecv( pBuffer, sizeof(SINT32), &sock,
                    cb ) ;
      if ( SDB_APP_FORCED == rc )
      {
         probe = 10 ;
         goto error ;
      }
      else if ( SDB_OK != rc )
      {
         probe = 20 ;
         goto error ;
      }
      else
      {
      }

      if ( *(UINT32*)(pBuffer) == MSG_SYSTEM_INFO_LEN )
      {
         rc = pmdHandleSysInfoRequest ( pBuffer,
                                        receiveBufferSize,
                                        sock, cb ) ;
         if ( rc )
         {
            probe = 25 ;
            goto error ;
         }
         goto retry ;
      }

      packetLength = *((SINT32*)pBuffer) ;
      if ( packetLength < (SINT32)sizeof (SINT32) )
      {
         probe = 30 ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( receiveBufferSize < packetLength+1 )
      {
         INT32 newSize = ossRoundUpToMultipleX ( packetLength+1,
                                                 SDB_PAGE_SIZE ) ;
         if ( newSize < 0 )
         {
            probe = 40 ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         SDB_OSS_FREE ( pBuffer ) ;
         pBuffer = (CHAR*)SDB_OSS_MALLOC ( sizeof(CHAR) * (newSize) ) ;
         if ( !pBuffer )
         {
            rc = SDB_OOM ;
            probe = 50 ;
            goto error ;
         }
         *(SINT32*)(pBuffer) = packetLength ;
         receiveBufferSize = newSize ;
      }

      rc = pmdRecv ( &pBuffer[sizeof (SINT32)],
                     packetLength-sizeof (SINT32),
                     &sock, cb ) ;
      if ( SDB_APP_FORCED == rc )
      {
         goto error ;
      }
      else if ( SDB_OK != rc )
      {
         probe = 55 ;
         goto error ;
      }
      else
      {
      }

      cb->incEventCount () ;

      header = (MsgHeader *)pBuffer ;
      reply.header.TID = header->TID ;
      reply.header.requestID = header->requestID ;

      if ( (UINT32)(header->messageLength) < sizeof (MsgHeader) )
      {
         probe = 60 ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( MSG_AUTH_VERIFY_REQ != header->opCode )
      {
         probe = 70 ;
         rc = SDB_AUTH_AUTHORITY_FORBIDDEN ;
         goto error ;
      }

      if ( SDB_ROLE_STANDALONE == role )
      {
         rc = SDB_OK ;
         goto done ;
      }
      else if ( SDB_ROLE_COORD != role )
      {
         shdCB = sdbGetShardCB() ;
         rc = shdCB->syncSend( (MsgHeader *)pBuffer,
                               CATALOG_GROUPID,
                               TRUE,
                               &authRes ) ;
         if ( SDB_OK != rc )
         {
            rc = shdCB->syncSend( (MsgHeader *)pBuffer,
                               CATALOG_GROUPID,
                               FALSE,
                               &authRes ) ;
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to send auth req to catalog, rc=%d",
                         rc ) ;
         }

         if ( NULL == authRes )
         {
            rc = SDB_SYS ;
            SDB_ASSERT( FALSE, "impossible" ) ;
            PD_LOG( PDERROR, "syncsend return ok but res is NULL" ) ;
            goto error ;
         }

         rc = (( MsgInternalReplyHeader *)authRes)->res ;
         if ( SDB_CLS_NOT_PRIMARY == rc )
         {
            SDB_OSS_FREE( authRes ) ;
            authRes = NULL ;
            rc = shdCB->syncSend( (MsgHeader *)pBuffer,
                                   CATALOG_GROUPID,
                                   FALSE,
                                   &authRes ) ;
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to send auth req to catalog, rc=%d",
                         rc ) ;
            if ( NULL == authRes )
            {
               rc = SDB_SYS ;
               SDB_ASSERT( FALSE, "impossible" ) ;
               PD_LOG( PDERROR, "syncsend return ok but res is NULL" ) ;
               goto error ;
            }
            rc = (( MsgInternalReplyHeader *)authRes)->res ;
         }

         if ( rc )
         {
            goto error ;
         }
      }
      else
      {
         BSONObj *err = NULL ;
         pOperator = pProcesserFactory->getOperator( header->opCode );
         rc = pOperator->execute( pBuffer,
                                  packetLength,
                                  ppResultBuffer,
                                  cb,
                                  reply,
                                  &err );
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( MSG_AUTH_VERIFY_REQ == header->opCode &&
              SDB_CAT_NO_ADDR_LIST == rc )
         {
            rc = SDB_OK ;
         }
         if ( SDB_OK != rc )
         {
            probe = 80 ;
            goto error ;
         }
      }

   done:
      if ( NULL != authRes )
      {
         SDB_OSS_FREE( authRes ) ;
      }
      reply.flags = rc ;
      PD_TRACE_EXITRC ( SDB_PMDAUTHENTICATE, rc );
      return rc ;
   error:
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Failed to execute authenticate, probe=%d, rc=%d",
                 probe, rc ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDAGENTENTPNT, "pmdAgentEntryPoint" )
   INT32 pmdAgentEntryPoint ( pmdEDUCB *cb, void *pData )
   {
      INT32 rc                   = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDAGENTENTPNT );
      BSONObj *pErrorObj         = NULL ;
      INT32 sendRC               = SDB_OK ;
      UINT32 probe               = 0 ;
      EDUID myEDUID              = cb->getID () ;
      pmdEDUMgr * eduMgr         = cb->getEDUMgr() ;
      BOOLEAN disconnect         = FALSE ;
      CHAR *pReceiveBuffer       = NULL ;
      rtnContextBuf buffObj ;
      MsgOpReply replyHeader ;
      MsgOpReply authReply ;
      ossMemset ( &replyHeader, 0, sizeof(replyHeader)) ;
      SINT32 receiveBufferSize   = ossRoundUpToMultipleX (
            PMD_AGENT_RECIEVE_BUFFER_SZ, SDB_PAGE_SIZE ) ;
      SINT32 packetLength        = 0 ;
      SDB_ASSERT ( pData, "Null pointer passed to pmdAgentEntryPoint" ) ;
      SOCKET s                   = *(( SOCKET *) &pData ) ;
      CHAR clientName [ PMD_EDU_NAME_LENGTH + 1] = {0} ;
      UINT16 clientPort          = 0 ;
      pmdKRCB *krcb              = pmdGetKRCB() ;
      monDBCB *mondbcb           = krcb->getMonDBCB () ;
      SDB_ROLE dbrole            = krcb->getDBRole () ;

      ossSocket sock ( &s, PMD_AGENT_SOCKET_DFT_TIMEOUT ) ;
      sock.disableNagle () ;

      clientPort = sock.getPeerPort () ;
      cb->setClientSock ( &sock ) ;
      rc = sock.getPeerAddress ( clientName, PMD_EDU_NAME_LENGTH ) ;
      if ( rc )
      {
         PD_LOG ( PDWARNING, "Failed to get address from socket" ) ;
      }
      else
      {
         cb->setClientInfo ( clientName, clientPort ) ;
         PD_LOG ( PDEVENT, "Connection is received from %s port %d",
                  clientName, clientPort ) ;
      }

      pReceiveBuffer = (CHAR*)SDB_OSS_MALLOC ( sizeof(CHAR) *
                                               receiveBufferSize ) ;
      if ( !pReceiveBuffer )
      {
         rc = SDB_OOM ;
         probe = 10 ;
         goto error ;
      }

      if ( SDB_ROLE_COORD == dbrole )
      {
         CoordCB *pCoordCB = krcb->getCoordCB() ;
         netMultiRouteAgent *pRouteAgent = pCoordCB->getRouteAgent() ;
         rc = pRouteAgent->addSession( cb );
         PD_RC_CHECK( rc, PDERROR,"failed to add session(rc=%d)", rc );
      }
      mondbcb->addReceiveNum () ;

      ossMemset ( &authReply, 0, sizeof(authReply)) ;
      rc = pmdAuthenticate( sock, pReceiveBuffer,
                            cb, receiveBufferSize, authReply, dbrole ) ;
      if ( SDB_APP_FORCED == rc )
      {
         goto done ;
      }
      else
      {
         INT32 sendRC = pmdSend ( (CHAR*)&authReply, sizeof(authReply),
                                   &sock, cb ) ;
         if ( sendRC )
         {
            rc = sendRC ;
            if ( SDB_APP_FORCED != sendRC )
            {
               probe = 70 ;
            }
         }
         if ( rc )
         {
            goto error ;
         }
      }

      while ( !disconnect )
      {
         cb->resetInterrupt () ;
         cb->resetInfo ( EDU_INFO_ERROR ) ;

         rc = pmdRecv ( pReceiveBuffer, sizeof (SINT32),
                        &sock, cb ) ;
         if ( rc )
         {
            if ( SDB_APP_FORCED == rc )
            {
               disconnect = TRUE ;
               cb->disconnect() ;
               continue ;
            }
            probe = 20 ;
            goto error ;
         }

         if ( *(UINT32*)(pReceiveBuffer) == MSG_SYSTEM_INFO_LEN )
         {
            rc = pmdHandleSysInfoRequest ( pReceiveBuffer,
                                           receiveBufferSize,
                                           sock, cb ) ;
            if ( rc )
            {
               if ( SDB_APP_FORCED == rc )
               {
                  disconnect = TRUE ;
                  continue ;
               }
               probe = 25 ;
               goto error ;
            }
            continue ;
         }
         packetLength = *(SINT32*)(pReceiveBuffer) ;
         PD_LOG ( PDDEBUG, "Received packet size = %d", packetLength ) ;
         if ( packetLength < (SINT32)sizeof (SINT32) )
         {
            probe = 30 ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         if ( receiveBufferSize < packetLength + 1 )
         {
            PD_LOG ( PDDEBUG, "Receive buffer size is too small: %d vs %d, "
                     "increasing...", receiveBufferSize, packetLength ) ;
            INT32 newSize = ossRoundUpToMultipleX ( packetLength+1,
                                                    SDB_PAGE_SIZE ) ;
            if ( newSize < 0 )
            {
               probe = 40 ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            SDB_OSS_FREE ( pReceiveBuffer ) ;
            pReceiveBuffer = (CHAR*)SDB_OSS_MALLOC ( newSize ) ;
            if ( !pReceiveBuffer )
            {
               rc = SDB_OOM ;
               probe = 50 ;
               goto error ;
            }
            *(SINT32*)(pReceiveBuffer) = packetLength ;
            receiveBufferSize = newSize ;
         }

         rc = pmdRecv ( &pReceiveBuffer[sizeof (SINT32)],
                        packetLength-sizeof (SINT32),
                        &sock, cb ) ;
         if ( rc )
         {
            if ( SDB_APP_FORCED == rc )
            {
               disconnect = TRUE ;
               continue ;
            }
            probe = 55 ;
            goto error ;
         }

         cb->incEventCount () ;

         pReceiveBuffer[packetLength] = 0 ;
         if ( SDB_OK != ( rc = eduMgr->activateEDU ( cb )) )
         {
            goto error ;
         }

         if ( SDB_ROLE_COORD == dbrole )
         {
            rc = pmdProcessCoordAgentRequest ( pReceiveBuffer, packetLength,
                                               buffObj, &disconnect, cb,
                                               replyHeader, &pErrorObj ) ;
         }

         if ( SDB_ROLE_STANDALONE == dbrole ||
              SDB_ROLE_CATALOG == dbrole ||
              SDB_ROLE_DATA == dbrole ||
              SDB_COORD_UNKNOWN_OP_REQ == rc )
         {
            rc = pmdProcessAgentRequest ( pReceiveBuffer, packetLength,
                                          buffObj, &disconnect, cb,
                                          replyHeader ) ;
         }

         if ( rc )
         {
            if ( SDB_APP_INTERRUPT == rc )
            {
               PD_LOG ( PDINFO, "Agent is interrupt" ) ;
            }
            else if ( SDB_DMS_EOC != rc )
            {
               PD_LOG ( PDERROR, "Error processing Agent request, rc=%d", rc ) ;
            }
         }

         if ( MSG_BS_INTERRUPTE != GET_REQUEST_TYPE(replyHeader.header.opCode)
              && !disconnect )
         {
            if ( buffObj.recordNum() > 0 )
            {
               replyHeader.numReturned = buffObj.recordNum() ;
               replyHeader.header.messageLength += buffObj.size() ;
            }

            sendRC = SDB_OK ;
            sendRC = pmdSend ( (CHAR*)&replyHeader, sizeof(replyHeader),
                               &sock, cb ) ;
            if ( sendRC )
            {
               rc = sendRC ;
               if ( SDB_APP_FORCED == sendRC )
               {
                  disconnect = TRUE ;
                  continue ;
               }
               probe = 70 ;
               goto error ;
            }

            if ( buffObj.size() > 0 )
            {
               sendRC = pmdSend ( buffObj.data(), buffObj.size(), &sock, cb ) ;
               buffObj.release() ; // release lock for data prepare
               if ( sendRC )
               {
                  rc = sendRC ;
                  if ( SDB_APP_FORCED == rc )
                  {
                     disconnect = TRUE ;
                     continue ;
                  }
                  probe = 90 ;
                  goto error ;
               }
            }
         }
         if ( SDB_OK != ( rc = eduMgr->waitEDU ( cb )) )
         {
            goto error ;
         }
         if ( pErrorObj )
         {
            SDB_OSS_DEL pErrorObj ;
            pErrorObj = NULL ;
         }
      }

   done :
      if ( pReceiveBuffer )
      {
         SDB_OSS_FREE ( pReceiveBuffer ) ;
      }

      if ( SDB_ROLE_STANDALONE == dbrole )
      {
         rc = rtnTransRollback( cb, krcb->getDPSCB() );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to rollback(rc=%d)", rc );
         }
      }
      else if ( SDB_ROLE_COORD == dbrole )
      {
         rtnCoordOperator *pRollbackOperator = NULL;
         BSONObj *err = NULL ;
         pRollbackOperator = pmdGetKRCB()->getCoordCB()->getProcesserFactory(
            )->getOperator( MSG_BS_TRANS_ROLLBACK_REQ ) ;
         if ( pRollbackOperator )
         {
            CHAR *pResultBuff = NULL ;
            pRollbackOperator->execute( NULL, 0, &pResultBuff, cb,
                                        replyHeader, &err ) ;
            SDB_ASSERT( pResultBuff == NULL, "Result buff must be NULL" ) ;
            SDB_ASSERT( NULL == err, "impossible" ) ;
         }
      }

      {
         pmdKRCB *krcb = pmdGetKRCB() ;
         SDB_RTNCB *rtnCB = krcb->getRTNCB() ;
         SINT64 contextID = -1 ;
         while ( -1 != (contextID = cb->contextPeek() ) )
            rtnCB->contextDelete( contextID, NULL ) ;
      }

      if ( SDB_ROLE_COORD == dbrole )
      {
         netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->getCoordCB(
            )->getRouteAgent() ;
         pRouteAgent->delSession( cb->getTID() ) ;
      }

      cb->setClientSock( NULL ) ;
      sock.close () ;

      if ( pErrorObj )
      {
         SDB_OSS_DEL pErrorObj ;
         pErrorObj = NULL ;
      }

      PD_TRACE_EXITRC ( SDB_PMDAGENTENTPNT, rc );
      return rc;
   error :
      switch ( rc )
      {
      case SDB_SYS :
         PD_LOG ( PDERROR, "EDU id %d cannot be found, probe %d", myEDUID,
                  probe ) ;
         break ;
      case SDB_EDU_INVAL_STATUS :
         PD_LOG ( PDERROR, "EDU status is not valid, probe %d", probe ) ;
         break ;
      case SDB_INVALIDARG :
         PD_LOG ( PDERROR, "Invalid argument receieved by agent, probe %d",
                  probe ) ;
         break ;
      case SDB_OOM :
         PD_LOG ( PDERROR, "Failed to allocate memory by agent, probe %d", probe ) ;
         break ;
      case SDB_NETWORK :
         PD_LOG ( PDERROR, "Network error occured[%s:%u], probe %d",
                  clientName, clientPort, probe ) ;
         break ;
      case SDB_NETWORK_CLOSE :
         PD_LOG ( PDDEBUG, "Remote connection closed[%s:%u]",
                  clientName, clientPort ) ;
         rc = SDB_OK ;
         break ;
      default :
         PD_LOG ( PDERROR, "Internal error, probe %d", probe ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDLOCALAGENTENTPNT, "pmdLocalAgentEntryPoint" )
   INT32 pmdLocalAgentEntryPoint( pmdEDUCB *cb, void *arg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDLOCALAGENTENTPNT );

      SOCKET s = *(( SOCKET *) &arg ) ;

      pmdLocalSession localSession( s ) ;
      localSession.attach( cb ) ;

      pmdDataProcessor dataProcessor ;
      dataProcessor.attachSession( &localSession ) ;
      localSession.attachProcessor( &dataProcessor ) ;

      rc = localSession.run() ;

      localSession.detachProcessor() ;
      dataProcessor.detachSession() ;

      localSession.detach() ;

      PD_TRACE_EXITRC ( SDB_PMDLOCALAGENTENTPNT, rc );
      return rc ;
   }

}

