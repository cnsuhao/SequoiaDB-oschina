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

   Source File Name = mongoSession.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/27/2015  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#include "util.hpp"
#include "mongodef.hpp"
#include "mongoConverter.hpp"
#include "mongoSession.hpp"
#include "pmdEDUMgr.hpp"
#include "pmdEDU.hpp"
#include "pmdEnv.hpp"
#include "monCB.hpp"
#include "msg.hpp"
#include "../../bson/bson.hpp"
#include "rtnCommandDef.hpp"
#include "rtn.hpp"
#include "pmd.hpp"
#include "sdbInterface.hpp"
#include "mongoReplyHelper.hpp"

_mongoSession::_mongoSession( SOCKET fd, engine::IResource *resource )
   : engine::pmdSession( fd ), _masterRead( FALSE ), _resource( resource )
{
   _converter = SDB_OSS_NEW mongoConverter() ;
}

_mongoSession::~_mongoSession()
{
   if ( NULL != _converter )
   {
      SDB_OSS_DEL( _converter ) ;
      _converter = NULL ;
   }

   _resource = NULL ;
}

void _mongoSession::_resetBuffers()
{
   if ( 0 != _contextBuff.size() )
   {
      _contextBuff.release() ;
   }

   if ( !_inBuffer.empty() )
   {
      _inBuffer.zero() ;
   }

   if ( !_outBuffer.empty() )
   {
      _outBuffer.zero() ;
   }
}

UINT64 _mongoSession::identifyID()
{
   return ossPack32To64( _socket.getLocalIP(), _socket.getLocalPort() ) ;
}

INT32 _mongoSession::getServiceType() const
{
   return CMD_SPACE_SERVICE_LOCAL ;
}

engine::SDB_SESSION_TYPE _mongoSession::sessionType() const
{
   return engine::SDB_SESSION_PROTOCOL ;
}

INT32 _mongoSession::run()
{
   INT32 rc                     = SDB_OK ;
   BOOLEAN bigEndian            = FALSE ;
   UINT32 msgSize               = 0 ;
   UINT32  headerLen            = sizeof( mongoMsgHeader ) - sizeof( INT32 ) ;
   INT32 bodyLen                = 0 ;
   engine::pmdEDUMgr *pmdEDUMgr = NULL ;
   CHAR *pBuff                  = NULL ;
   const CHAR *pBody            = NULL ;
   const CHAR *pInMsg           = NULL ;

   if ( !_pEDUCB )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   pmdEDUMgr = _pEDUCB->getEDUMgr() ;
   bigEndian = checkBigEndian() ;
   while ( !_pEDUCB->isDisconnected() && !_socket.isClosed() )
   {
      _pEDUCB->resetInterrupt() ;
      _pEDUCB->resetInfo( engine::EDU_INFO_ERROR ) ;
      _pEDUCB->resetLsn() ;

      rc = _setSeesionAttr() ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = recvData( (CHAR*)&msgSize, sizeof(UINT32) ) ;
      if ( rc )
      {
         if ( SDB_APP_FORCED != rc )
         {
            PD_LOG( PDERROR, "Session[%s] failed to recv msg size, "
                    "rc: %d", sessionName(), rc ) ;
         }
         break ;
      }

      if ( bigEndian )
      {
      }

      if ( msgSize < headerLen || msgSize > SDB_MAX_MSG_LENGTH )
      {
         PD_LOG( PDERROR, "Session[%s] recv msg size[%d] is less than "
                 "mongoMsgHeader size[%d] or more than max msg size[%d]",
                 sessionName(), msgSize, sizeof( mongoMsgHeader ),
                 SDB_MAX_MSG_LENGTH ) ;
         rc = SDB_INVALIDARG ;
         break ;
      }
      else
      {
         pBuff = getBuff( msgSize + 1 ) ;
         if ( !pBuff )
         {
            rc = SDB_OOM ;
            break ;
         }
         *(UINT32*)pBuff = msgSize ;
         rc = recvData( pBuff + sizeof(UINT32), msgSize - sizeof(UINT32) ) ;
         if ( rc )
         {
            if ( SDB_APP_FORCED != rc )
            {
               PD_LOG( PDERROR, "Session failed to recv rest msg, rc: %d",
                       sessionName(), rc ) ;
            }
            break ;
         }
         pBuff[ msgSize ] = 0 ;
         {
            _resetBuffers() ;
            _converter->loadFrom( pBuff, msgSize ) ;
            rc = _converter->convert( _inBuffer ) ;
            if ( SDB_OK != rc && SDB_OPTION_NOT_SUPPORT != rc)
            {
               goto error ;
            }

            if ( _preProcessMsg( _converter->getParser(),
                                 _resource, _contextBuff ) )
            {
               _pEDUCB->incEventCount() ;
               goto reply ;
            }

            _pEDUCB->incEventCount() ;
            if ( SDB_OK != ( rc = pmdEDUMgr->activateEDU( _pEDUCB ) ) )
            {
               PD_LOG( PDERROR, "Session[%s] activate edu failed, rc: %d",
                       sessionName(), rc ) ;
               goto error ;
            }

            pInMsg = _inBuffer.data() ;
            while ( NULL != pInMsg )
            {
               rc = _processMsg( pInMsg ) ;
               rc = _converter->reConvert( _inBuffer, &_replyHeader ) ;
               if ( SDB_OK != rc )
               {
                  goto reply ;
               }
               else
               {
                  if ( !_inBuffer.empty() )
                  {
                     _contextBuff.release() ;
                     pInMsg = _inBuffer.data() ;
                  }
                  else
                  {
                     pInMsg = NULL ;
                  }
               }
            }
         reply:
            _handleResponse( _converter->getOpType(), _contextBuff ) ;
            pBody = _contextBuff.data() ;
            bodyLen = _contextBuff.size() ;
            INT32 rcTmp = _reply( &_replyHeader, pBody, bodyLen ) ;
            if ( rcTmp )
            {
               PD_LOG( PDERROR, "Session[%s] failed to send response, rc: %d",
                       sessionName(), rcTmp ) ;
               goto error ;
            }
            pBody = NULL ;
            bodyLen = 0 ;
            _contextBuff.release() ;

            if ( SDB_OK != ( rc = pmdEDUMgr->waitEDU( _pEDUCB ) ) )
            {
               PD_LOG( PDERROR, "Session[%s] wait edu failed, rc: %d",
                       sessionName(), rc ) ;
               goto error ;
            }
         }
      }
   } // end while
done:
   disconnect() ;
   return rc ;
error:
   goto done ;
}

INT32 _mongoSession::_processMsg( const CHAR *pMsg )
{
   INT32 rc  = SDB_OK ;
   INT32 tmp = SDB_OK ;
   INT32 bodyLen = 0 ;
   BOOLEAN needReply = FALSE ;
   bson::BSONObjBuilder bob ;

   rc = _onMsgBegin( (MsgHeader *) pMsg ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   {
      rc = getProcessor()->processMsg( (MsgHeader *) pMsg,
                                       _contextBuff, _replyHeader.contextID,
                                       needReply ) ;
      if ( SDB_OK != rc )
      {
         _errorInfo = engine::utilGetErrorBson( rc,
                      _pEDUCB->getInfo( engine::EDU_INFO_ERROR ) ) ;

         tmp = _errorInfo.getIntField( OP_ERRNOFIELD ) ;
         bob.append( "ok", FALSE ) ;
         bob.append( "code",  tmp ) ;
         bob.append( "errmsg", _errorInfo.getStringField( OP_ERRDESP_FIELD) ) ;
         _contextBuff = engine::rtnContextBuf( bob.obj() ) ;
      }
      bodyLen = _contextBuff.size() ;
      _replyHeader.numReturned = _contextBuff.recordNum() ;
      _replyHeader.startFrom = (INT32)_contextBuff.getStartFrom() ;
      _replyHeader.flags = rc ;
   }

   if ( _converter->getParser().withCmd )
   {
      if ( 0 == bodyLen )
      {
         _errorInfo = engine::utilGetErrorBson( rc,
                      _pEDUCB->getInfo( engine::EDU_INFO_ERROR ) ) ;

         tmp = _errorInfo.getIntField( OP_ERRNOFIELD ) ;
         if ( SDB_OK != rc )
         {
            bob.append( "ok", FALSE ) ;
            bob.append( "code",  tmp ) ;
            bob.append( "errmsg", _errorInfo.getStringField( OP_ERRDESP_FIELD) ) ;
         }
         else
         {
            bob.append( "ok", TRUE ) ;
         }

         _contextBuff = engine::rtnContextBuf( bob.obj() ) ;
         _replyHeader.flags = rc ;
      }
   }

   _onMsgEnd( rc, (MsgHeader *) pMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 _mongoSession::_onMsgBegin( MsgHeader *msg )
{
   _replyHeader.contextID          = -1 ;
   _replyHeader.numReturned        = 0 ;
   _replyHeader.startFrom          = 0 ;
   _replyHeader.header.opCode      = MAKE_REPLY_TYPE(msg->opCode) ;
   _replyHeader.header.requestID   = msg->requestID ;
   _replyHeader.header.TID         = msg->TID ;
   _replyHeader.header.routeID     = engine::pmdGetNodeID() ;

   MON_START_OP( _pEDUCB->getMonAppCB() ) ;

   return SDB_OK ;
}

INT32 _mongoSession::_onMsgEnd( INT32 result, MsgHeader *msg )
{

   if ( result && SDB_DMS_EOC != result )
   {
      PD_LOG( PDWARNING, "Session[%s] process msg[opCode=%d, len: %d, "
              "TID: %d, requestID: %llu] failed, rc: %d",
              sessionName(), msg->opCode, msg->messageLength, msg->TID,
              msg->requestID, result ) ;
   }

   MON_END_OP( _pEDUCB->getMonAppCB() ) ;

   return SDB_OK ;
}

INT32 _mongoSession::_reply( MsgOpReply *replyHeader,
                             const CHAR *pBody,
                             const INT32 len )
{
   INT32 rc         = SDB_OK ;
   INT32 offset     = 0 ;
   mongoMsgReply reply ;
   bson::BSONObjBuilder bob ;
   bson::BSONObj bsonBody ;
   bson::BSONObj objToSend ;

   reply.header.id = 0 ;
   reply.header.responseTo = replyHeader->header.requestID ;
   reply.header.opCode = dbReply ;
   reply.header._flags = 0 ;
   reply.header._version = 0 ;
   reply.header.reservedFlags = 0 ;
   if ( SDB_OK != replyHeader->flags )
   {
      reply.cursorId = 0 ;
   }
   else
   {
      reply.cursorId = replyHeader->contextID + 1 ;
   }
   reply.startingFrom = replyHeader->startFrom ;
   if ( _converter->getParser().withCmd &&
        OP_GETMORE != _converter->getOpType() )
   {
      reply.nReturned = ( replyHeader->numReturned > 0 ?
                          replyHeader->numReturned : 1 ) ;
   }
   else
   {
      reply.nReturned = replyHeader->numReturned ;
   }

   if ( reply.nReturned > 1 )
   {
         while ( offset < len )
         {
            bsonBody.init( pBody + offset ) ;
            _outBuffer.write( bsonBody.objdata(), bsonBody.objsize() ) ;
            offset += ossRoundUpToMultipleX( bsonBody.objsize(), 4 ) ;
         }

   }
   else
   {
      if ( pBody )
      {
         if ( 0 == reply.cursorId &&
             ( SDB_OK == _replyHeader.flags &&
               OP_QUERY != _converter->getOpType() ) )
         {
            bsonBody.init( pBody ) ;
            if ( !bsonBody.hasField( "ok" ) )
            {
               bob.append( "ok", 0 == replyHeader->flags ? TRUE : FALSE ) ;
               bob.append( "code", replyHeader->flags ) ;
               bob.appendElements( bsonBody ) ;
               objToSend = bob.obj() ;
               _outBuffer.write( objToSend ) ;
            }
            _outBuffer.write( bsonBody ) ;
         }
         else
         {
            bsonBody.init( pBody ) ;
            _outBuffer.write( bsonBody ) ;
         }
      }
      else
      {
         if ( OP_GETMORE != _converter->getOpType() &&
              OP_CMD_GET_INDEX == _converter->getOpType()  )
         {
            bob.append( "ok", 1.0 ) ;
            objToSend = bob.obj() ;
            _outBuffer.write( objToSend ) ;
         }
      }
   }

   if ( !_outBuffer.empty() )
   {
      pBody = _outBuffer.data() ;
   }
   reply.header.len = sizeof( mongoMsgReply ) + _outBuffer.size() ;

   rc = sendData( (CHAR *)&reply, sizeof( mongoMsgReply ) ) ;
   if ( rc )
   {
      PD_LOG( PDERROR, "Session[%s] failed to send response header, rc: %d",
              sessionName(), rc ) ;
      goto error ;
   }

   if ( pBody )
   {
      rc = sendData( pBody, reply.header.len - sizeof( mongoMsgReply ) ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Session[%s] failed to send response body, rc: %d",
                          sessionName(), rc ) ;
         goto error ;
      }
   }

done:
   return rc ;
error:
   goto done ;
}

BOOLEAN _mongoSession::_preProcessMsg( const mongoParser &parser,
                                       engine::IResource *resource,
                                       engine::rtnContextBuf &buff )
{
   BOOLEAN handled = FALSE ;

   if ( OP_CMD_ISMASTER == parser.opType )
   {
      handled = TRUE ;
      fap::mongo::buildIsMasterReplyMsg( resource, buff ) ;
   }
   else if ( OP_CMD_GETNONCE == parser.opType )
   {
      handled = TRUE ;
      fap::mongo::buildGetNonceReplyMsg( buff ) ;
   }
   else if ( OP_CMD_GETLASTERROR == parser.opType )
   {
      handled = TRUE ;
      fap::mongo::buildGetLastErrorReplyMsg( _errorInfo, buff ) ;
   }
   else if ( OP_CMD_NOT_SUPPORTED == parser.opType )
   {
      handled = TRUE ;
      fap::mongo::buildNotSupportReplyMsg( _contextBuff, parser.cmdName ) ;
   }
   else if ( OP_CMD_PING == parser.opType )
   {
       handled = TRUE ;
       fap::mongo::buildPingReplyMsg( _contextBuff ) ;
   }

   if ( handled )
   {
      _replyHeader.contextID            = -1 ;
      _replyHeader.numReturned          = 1 ;
      _replyHeader.startFrom            = 0 ;
      _replyHeader.header.opCode        = MAKE_REPLY_TYPE(parser.opCode) ;
      _replyHeader.header.requestID     = parser.id ;
      _replyHeader.header.TID           = 0 ;
      _replyHeader.header.routeID.value = 0 ;
      _replyHeader.flags         = SDB_OK ;
   }

   return handled ;
}

void _mongoSession::_handleResponse( const INT32 opType,
                                    engine::rtnContextBuf &buff )
{
   if ( OP_CMD_COUNT_MORE == opType )
   {
      bson::BSONObjBuilder bob ;
      bson::BSONObj obj( buff.data() ) ;
      bob.append( "n", obj.getIntField( "Total" ) ) ;
      buff = engine::rtnContextBuf( bob.obj() ) ;
      _replyHeader.contextID = -1 ;
   }

   if ( SDB_DMS_EOC == _replyHeader.flags )
   {
      buff = engine::rtnContextBuf() ;
      _replyHeader.numReturned = 0 ;
      _replyHeader.contextID = -1 ;
   }
}

INT32 _mongoSession::_setSeesionAttr()
{
   INT32 rc = SDB_OK ;
   const CHAR *cmd = CMD_ADMIN_PREFIX CMD_NAME_SETSESS_ATTR ;
   MsgOpQuery *set = NULL ;
   bson::BSONObj obj ;
   bson::BSONObj emptyObj ;

   msgBuffer msgSetAttr ;
   if ( _masterRead )
   {
      goto done ;
   }

   msgSetAttr.reverse( sizeof( MsgOpQuery ) ) ;
   msgSetAttr.advance( sizeof( MsgOpQuery ) - 4 ) ;
   obj = BSON( FIELD_NAME_PREFERED_INSTANCE << PREFER_REPL_MASTER ) ;
   set = (MsgOpQuery *)msgSetAttr.data() ;

   set->header.opCode = MSG_BS_QUERY_REQ ;
   set->header.TID = 0 ;
   set->header.routeID.value = 0 ;
   set->header.requestID = 0 ;
   set->version = 0 ;
   set->w = 0 ;
   set->padding = 0 ;
   set->flags = 0 ;
   set->nameLength = ossStrlen(cmd) ;
   set->numToSkip = 0 ;
   set->numToReturn = -1 ;

   msgSetAttr.write( cmd, set->nameLength + 1, TRUE ) ;
   msgSetAttr.write( obj, TRUE ) ;
   msgSetAttr.write( emptyObj, TRUE ) ;
   msgSetAttr.write( emptyObj, TRUE ) ;
   msgSetAttr.write( emptyObj, TRUE ) ;
   msgSetAttr.doneLen() ;
   rc = _processMsg( msgSetAttr.data() ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   _masterRead = TRUE ;

done:
   return rc ;
error:
   goto done ;
}
