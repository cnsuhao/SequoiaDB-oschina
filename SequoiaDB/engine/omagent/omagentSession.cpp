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

   Source File Name = omagentSession.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omagentSession.hpp"
#include "omagentMgr.hpp"
#include "utilCommon.hpp"
#include "msgMessage.hpp"
#include "omagentHelper.hpp"
#include "omagentMsgDef.hpp"
#include "omagentUtil.hpp"
#include "msgAuth.hpp"
#include "../bson/bson.h"
#include "../bson/lib/md5.hpp"

using namespace bson ;

namespace engine
{
   /*
      Local define
   */
   #define OMAGENT_SESESSION_TIMEOUT         ( 120 )

   /*
      _omaSession implement
   */
   BEGIN_OBJ_MSG_MAP( _omaSession, _pmdAsyncSession )
      ON_MSG( MSG_CM_REMOTE, _onNodeMgrReq )
      ON_MSG( MSG_AUTH_VERIFY_REQ, _onAuth )
      ON_MSG( MSG_BS_QUERY_REQ, _onOMAgentReq )
   END_OBJ_MSG_MAP()

   _omaSession::_omaSession( UINT64 sessionID )
   :_pmdAsyncSession( sessionID )
   {
      ossMemset( (void*)&_replyHeader, 0, sizeof(_replyHeader) ) ;
      _pNodeMgr   = NULL ;
      sdbGetOMAgentMgr()->incSession() ;
   }

   _omaSession::~_omaSession()
   {
      sdbGetOMAgentMgr()->decSession() ;
   }

   SDB_SESSION_TYPE _omaSession::sessionType() const
   {
      return SDB_SESSION_OMAGENT ;
   }

   EDU_TYPES _omaSession::eduType() const
   {
      return EDU_TYPE_AGENT ;
   }

   void _omaSession::onRecieve( const NET_HANDLE netHandle, MsgHeader * msg )
   {
      ossGetCurrentTime( _lastRecvTime ) ;
      sdbGetOMAgentMgr()->resetNoMsgTimeCounter() ;
   }

   BOOLEAN _omaSession::timeout ( UINT32 interval )
   {
      BOOLEAN ret = FALSE ;
      ossTimestamp curTime ;
      ossGetCurrentTime ( curTime ) ;

      if ( sdbGetOMAgentOptions()->isStandAlone() )
      {
         ret = TRUE ;
         goto done ;
      }
      else if ( curTime.time - _lastRecvTime.time > OMAGENT_SESESSION_TIMEOUT )
      {
         ret = TRUE ;
         goto done ;
      }

   done :
      return ret ;
   }

   void _omaSession::onTimer( UINT64 timerID, UINT32 interval )
   {
   }

   void _omaSession::_onDetach()
   {
   }

   void _omaSession::_onAttach()
   {
      _pNodeMgr = sdbGetOMAgentMgr()->getNodeMgr() ;
   }

   INT32 _omaSession::_defaultMsgFunc( NET_HANDLE handle, MsgHeader * msg )
   {
      PD_LOG( PDWARNING, "Session[%s] Recieve unknow msg[type:[%d]%u, len:%u]",
              sessionName(), IS_REPLY_TYPE( msg->opCode ) ? 1 : 0,
              GET_REQUEST_TYPE( msg->opCode ), msg->messageLength ) ;

      return _reply( SDB_CLS_UNKNOW_MSG, msg ) ;
   }

   INT32 _omaSession::_reply( MsgOpReply *header, const CHAR *pBody,
                              INT32 bodyLen )
   {
      INT32 rc = SDB_OK ;

      if ( (UINT32)(header->header.messageLength) !=
           sizeof (MsgOpReply) + bodyLen )
      {
         PD_LOG ( PDERROR, "Session[%s] reply message length error[%u != %u]",
                  sessionName() ,header->header.messageLength,
                  sizeof ( MsgOpReply ) + bodyLen ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( bodyLen > 0 )
      {
         rc = routeAgent()->syncSend ( _netHandle, (MsgHeader *)header,
                                       (void*)pBody, bodyLen ) ;
      }
      else
      {
         rc = routeAgent()->syncSend ( _netHandle, (void *)header ) ;
      }

      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Session[%s] send reply message failed[rc:%d]",
                  sessionName(), rc ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaSession::_reply( INT32 flags, MsgHeader * pSrcReqMsg )
   {
      const CHAR *pBody = NULL ;
      INT32 bodyLen     = 0 ;

      _replyHeader.header.opCode = MAKE_REPLY_TYPE( pSrcReqMsg->opCode ) ;
      _replyHeader.header.messageLength = sizeof ( MsgOpReply ) ;
      _replyHeader.header.requestID = pSrcReqMsg->requestID ;
      _replyHeader.header.TID = pSrcReqMsg->TID ;
      _replyHeader.header.routeID.value = 0 ;
      _replyHeader.flags = flags ;
      _replyHeader.contextID = -1 ;
      _replyHeader.numReturned = 0 ;
      _replyHeader.startFrom = 0 ;

      if ( flags )
      {
         _errorInfo = utilGetErrorBson( flags, _pEDUCB->getInfo(
                                        EDU_INFO_ERROR ) ) ;
         bodyLen  = _errorInfo.objsize() ;
         pBody    = _errorInfo.objdata() ;
         _replyHeader.header.messageLength += bodyLen ;
         _replyHeader.numReturned = 1 ;
      }

      return _reply( &_replyHeader, pBody, bodyLen ) ;
   }

   INT32 _omaSession::_onAuth( const NET_HANDLE & handle, MsgHeader * pMsg )
   {
      INT32 rc = SDB_OK ;
      BSONObj obj ;
      BSONElement user, pass ;

      rc = extractAuthMsg( pMsg, obj ) ;
      PD_RC_CHECK( rc, PDERROR, "Extrace auth msg failed, rc: %d", rc ) ;

      user = obj.getField( SDB_AUTH_USER ) ;
      pass = obj.getField( SDB_AUTH_PASSWD ) ;

      if ( 0 != ossStrcmp( user.valuestrsafe(), SDB_OMA_USER ) )
      {
         PD_LOG( PDERROR, "User name[%s] is not support",
                 user.valuestrsafe() ) ;
         rc = SDB_AUTH_AUTHORITY_FORBIDDEN ;
         goto error ;
      }

      if ( md5::md5simpledigest( string( SDB_OMA_USERPASSWD ) ) !=
           string( pass.valuestrsafe() ) )
      {
         PD_LOG( PDERROR, "User password[%s] is not error",
                 pass.valuestrsafe() ) ;
         rc = SDB_AUTH_AUTHORITY_FORBIDDEN ;
         goto error ;
      }
      eduCB()->setUserInfo( user.valuestrsafe(), pass.valuestrsafe() ) ;

   done:
      return _reply( rc, pMsg ) ;
   error:
      goto done ;
   }

   INT32 _omaSession::_onNodeMgrReq( const NET_HANDLE & handle,
                                     MsgHeader * pMsg )
   {
      INT32 rc = SDB_OK ;
      MsgCMRequest *pCMReq = ( MsgCMRequest* )pMsg ;
      INT32 remoteCode = 0 ;
      CHAR *arg1 = NULL ;
      CHAR *arg2 = NULL ;
      CHAR *arg3 = NULL ;
      CHAR *arg4 = NULL ;

      if ( sdbGetOMAgentOptions()->isStandAlone() )
      {
         rc = SDB_PERM ;
         goto done ;
      }

      if ( pMsg->messageLength < (SINT32)sizeof (MsgCMRequest) )
      {
         PD_LOG( PDERROR, "Session[%s] recieve invalid msg[opCode: %d, "
                 "len: %d]", sessionName(), pMsg->opCode,
                 pMsg->messageLength ) ;
         goto done ;
      }

      rc = msgExtractCMRequest ( ( CHAR*)pMsg, &remoteCode, &arg1, &arg2,
                                 &arg3, &arg4 ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Session[%s]failed to extract cm request, rc: %d",
                  sessionName(), rc ) ;
         goto done ;
      }

      switch( pCMReq->remoCode )
      {
         case SDBSTART :
            rc = _pNodeMgr->startANode( arg1 ) ;
            break ;
         case SDBSTOP :
            rc = _pNodeMgr->stopANode( arg1 ) ;
            break ;
         case SDBADD :
            rc = _pNodeMgr->addANode( arg1, arg2 ) ;
            break ;
         case SDBMODIFY :
            rc = _pNodeMgr->mdyANode( arg1 ) ;
            break ;
         case SDBRM :
            rc = _pNodeMgr->rmANode( arg1, arg2 ) ;
            break ;
         case SDBSTARTALL :
            rc = _pNodeMgr->startAllNodes( NODE_START_CLIENT ) ;
            break ;
         case SDBSTOPALL :
            rc = _pNodeMgr->stopAllNodes() ;
            break ;
         default :
            PD_LOG( PDERROR, "Unknow remote code[%d] in session[%s]",
                    pCMReq->remoCode, sessionName() ) ;
            rc = SDB_INVALIDARG ;
            break ;
      }

      if ( rc )
      {
         PD_LOG( PDERROR, "Session[%s] process remote code[%d] failed, rc: %d",
                 sessionName(), pCMReq->remoCode, rc ) ;
      }

   done:
      return _reply( rc, pMsg ) ;
   }

   INT32 _omaSession::_onOMAgentReq( const NET_HANDLE &handle,
                                     MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      INT32 flags               = 0 ;
      CHAR *pCollectionName     = NULL ;
      CHAR *pQuery              = NULL ;
      CHAR *pFieldSelector      = NULL ;
      CHAR *pOrderByBuffer      = NULL ;
      CHAR *pHintBuffer         = NULL ;
      SINT64 numToSkip          = -1 ;
      SINT64 numToReturn        = -1 ;
      _omaCommand *pCommand     = NULL ;
      INT64 sec                 = 0 ;
      INT64 microSec            = 0 ;
      INT64 tkTime              = 0 ;
      ossTimestamp tmBegin ;
      ossTimestamp tmEnd ;
      BSONObj retObj ;
      BSONObjBuilder builder ;

      PD_LOG ( PDDEBUG, "Omagent receive requset from omsvc" ) ;
      ossGetCurrentTime( tmBegin ) ;
      _buildReplyHeader( pMsg ) ;
      rc = msgExtractQuery ( (CHAR *)pMsg, &flags, &pCollectionName,
                             &numToSkip, &numToReturn, &pQuery,
                             &pFieldSelector, &pOrderByBuffer,
                             &pHintBuffer ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Session[%s] extract omsvc's command msg failed, "
                  "rc: %d", sessionName(), rc ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( omaIsCommand ( pCollectionName ) )
      {
         PD_LOG( PDDEBUG, "Omagent receive command: %s, argument: %s",
                 pCollectionName, BSONObj(pQuery).toString().c_str() ) ;

         rc = omaParseCommand ( pCollectionName, &pCommand ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "Failed to parse omsvc's command[%s], rc:%d",
                    pCollectionName, rc ) ;
            goto error ;
         }
         rc = omaInitCommand( pCommand, flags, numToSkip, numToReturn,
                              pQuery, pFieldSelector, pOrderByBuffer,
                              pHintBuffer ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "Failed to init omsvc's command[%s] for omagent, "
                    "rc: %d", pCollectionName, rc ) ;
            goto error ;
         }
         rc = omaRunCommand( pCommand, retObj ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "Failed to run omsvc's command[%s], rc: %d",
                    pCollectionName, rc ) ;
            goto error ;
         }
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Omsvc's request[%s] is not a command",
                  pCollectionName ) ;
         goto error ;
      }

      builder.appendElements( retObj ) ;

   done :
      if ( pCommand )
      {
         omaReleaseCommand( &pCommand ) ;
      }
      retObj = builder.obj() ;
      _replyHeader.header.messageLength += retObj.objsize() ;
      _replyHeader.numReturned = 1 ;
      _replyHeader.flags = rc ;

      ossGetCurrentTime ( tmEnd ) ;
      tkTime = ( tmEnd.time * 1000000 + tmEnd.microtm ) -
               ( tmBegin.time * 1000000 + tmBegin.microtm ) ;
      sec = tkTime/1000000 ;
      microSec = tkTime%1000000 ;
      PD_LOG ( PDDEBUG, "Excute command[%s] return: %s",
               pCollectionName, retObj.toString(FALSE, TRUE).c_str() ) ;
      PD_LOG ( PDDEBUG, "Excute command[%s] takes %lld.%llds.",
               pCollectionName, sec, microSec ) ;

      return _reply( &_replyHeader, retObj.objdata(), retObj.objsize() ) ;
   error :
      if ( rc < -SDB_MAX_ERROR || rc > SDB_MAX_WARNING )
      {
         PD_LOG ( PDERROR, "Error code is invalid[rc:%d]", rc ) ;
         rc = SDB_SYS ;
      }
      if ( eduCB()->getInfo( EDU_INFO_ERROR ) &&
           0 != *( eduCB()->getInfo( EDU_INFO_ERROR ) ) )
      {
         builder.append( OMA_FIELD_DETAIL,
                         eduCB()->getInfo( EDU_INFO_ERROR ) ) ;
      }
      else
      {
/*
#if defined (_DEBUG)
         ossPanic() ;
#endif
*/
         builder.append( OMA_FIELD_DETAIL, getErrDesp( rc ) ) ;
      }

      goto done ;
   }

   INT32 _omaSession::_buildReplyHeader( MsgHeader *pMsg )
   {
      _replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      _replyHeader.header.opCode        = MAKE_REPLY_TYPE(pMsg->opCode) ;
      _replyHeader.header.TID           = pMsg->TID ;
      _replyHeader.header.routeID.value = 0 ;
      _replyHeader.header.requestID     = pMsg->requestID ;

      _replyHeader.contextID            = -1 ;
      _replyHeader.flags                = 0 ;
      _replyHeader.numReturned          = 0 ;
      _replyHeader.startFrom            = 0 ;
      _replyHeader.numReturned          = 0 ;

      return SDB_OK ;
   }

} // namespace engine

