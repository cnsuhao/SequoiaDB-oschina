#include "util.hpp"
#include "mongodef.hpp"
#include "mongoConverter.hpp"
#include "pmdMongoSession.hpp"
#include "pmdEDUMgr.hpp"
#include "pmdEDU.hpp"
#include "pmdEnv.hpp"
#include "monCB.hpp"
#include "msg.h"
#include "rtnCommandDef.hpp"
#include "rtn.hpp"
#include "pmd.hpp"
#include "sdbInterface.hpp"

_pmdMongoSession::_pmdMongoSession( SOCKET fd ) : engine::pmdSession( fd )
{
   _converter = SDB_OSS_NEW mongoConverter() ;
}

_pmdMongoSession::~_pmdMongoSession()
{
   if ( NULL != _converter )
   {
      SDB_OSS_DEL( _converter ) ;
      _converter = NULL ;
   }
}

UINT64 _pmdMongoSession::identifyID()
{
   return ossPack32To64( _socket.getLocalIP(), _socket.getLocalPort() ) ;
}

INT32 _pmdMongoSession::getServiceType() const
{
   return CMD_SPACE_SERVICE_LOCAL ;
}

engine::SDB_SESSION_TYPE _pmdMongoSession::sessionType() const
{
   return engine::SDB_SESSION_MONGO ;
}

INT32 _pmdMongoSession::attachProcessor( engine::_IProcessor *processor )
{
   SDB_ASSERT( NULL != processor, "" ) ;
   _processor = processor ;
   return SDB_OK ;
}

void _pmdMongoSession::detachProcessor()
{
   SDB_ASSERT( NULL != _processor, "" ) ;
   _processor = NULL ;
}

INT32 _pmdMongoSession::run()
{
   INT32 rc                     = SDB_OK ;
   INT32 buffSize               = 0 ;
   UINT32 msgSize               = 0 ;
   CHAR *pBuff                  = NULL ;
   engine::pmdEDUMgr *pmdEDUMgr = NULL ;

   if ( !_pEDUCB )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   pmdEDUMgr = _pEDUCB->getEDUMgr() ;

   while ( !_pEDUCB->isDisconnected() && !_socket.isClosed() )
   {
      _pEDUCB->resetInterrupt() ;
      _pEDUCB->resetInfo( engine::EDU_INFO_ERROR ) ;

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

      if ( msgSize < sizeof( mongoMsgHeader ) || msgSize > SDB_MAX_MSG_LENGTH )
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
         buffSize = getBuffLen() ;
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

         _pEDUCB->incEventCount() ;
         pBuff[ msgSize ] = 0 ;
         if ( SDB_OK != ( rc = pmdEDUMgr->activateEDU( _pEDUCB ) ) )
         {
            PD_LOG( PDERROR, "Session[%s] activate edu failed, rc: %d",
                    sessionName(), rc ) ;
            break ;
         }
         rc = _processMsg( pBuff, msgSize ) ;
         if ( rc )
         {
            break ;
         }
         if ( SDB_OK != ( rc = pmdEDUMgr->waitEDU( _pEDUCB ) ) )
         {
            PD_LOG( PDERROR, "Session[%s] wait edu failed, rc: %d",
                    sessionName(), rc ) ;
            break ;
         }
      }
   } // end while

done:
   disconnect() ;
   return rc ;
error:
   goto done ;
}

INT32 _pmdMongoSession::_processMsg( const CHAR *pMsg, const INT32 len )
{
   INT32 rc             = SDB_OK ;
   CONVERT_ERROR con_rc = CON_OK ;
   const CHAR *pBody    = NULL ;
   INT32 bodyLen        = 0 ;

   _converter->loadFrom( pMsg, len ) ;
   con_rc = _converter->convert( _inStream ) ;
   if ( CON_OK != con_rc )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = _onMsgBegin( (MsgHeader *) _inStream.data() ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( _converter->isGetLastError()  )
   {
      _replyHeader.numReturned = 1 ;
      _replyHeader.contextID = 0 ;
      _replyHeader.startFrom = 0 ;
      _replyHeader.flags = rc ;
      _replyHeader.header.messageLength = sizeof( _replyHeader ) +
                                          _errorInfo.objsize() ;

      pBody = _errorInfo.objdata() ;
      bodyLen = _errorInfo.objsize() ;
   }
   else
   {
      rc = _processor->processMsg( (MsgHeader *) _inStream.data(),
                                    _pDPSCB, _contextBuff, 
                                    _replyHeader.contextID, _needReply ) ;
      pBody     = _contextBuff.data() ;
      bodyLen   = _contextBuff.size() ;
      _replyHeader.numReturned = _contextBuff.recordNum() ;
      _replyHeader.startFrom = (INT32)_contextBuff.getStartFrom() ;
      _replyHeader.flags = rc ;
      _replyHeader.header.messageLength = sizeof( _replyHeader ) + bodyLen ;
   }

   if ( rc && bodyLen == 0 )
   {
      _errorInfo = engine::utilGetErrorBson( rc,
                   _pEDUCB->getInfo( engine::EDU_INFO_ERROR ) ) ;
      pBody = _errorInfo.objdata() ;
      bodyLen = _errorInfo.objsize() ;

      _replyHeader.numReturned = 1 ;
      _replyHeader.contextID = 0 ;
      _replyHeader.startFrom = 0 ;
      _replyHeader.flags = rc ;
      _replyHeader.header.messageLength = sizeof( _replyHeader ) + bodyLen ;
   }

   if ( _needReply )
   {
      INT32 rcTmp = _reply( &_replyHeader, pBody, bodyLen ) ;
      if ( rcTmp )
      {
         PD_LOG( PDERROR, "Session[%s] failed to send response, rc: %d",
                 sessionName(), rcTmp ) ;
         disconnect() ;
      }
   }

   rc = _onMsgEnd( rc, (MsgHeader *) _inStream.data() ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

done:
   _zeroStream() ;
   return rc ;
error:
   goto done ;
}

INT32 _pmdMongoSession::_onMsgBegin( MsgHeader *msg )
{
   _replyHeader.contextID          = -1 ;
   _replyHeader.numReturned        = 0 ;
   _replyHeader.startFrom          = 0 ;
   _replyHeader.header.opCode      = MAKE_REPLY_TYPE(msg->opCode) ;
   _replyHeader.header.requestID   = msg->requestID ;
   _replyHeader.header.TID         = msg->TID ;
   _replyHeader.header.routeID     = engine::pmdGetNodeID() ;

   if ( MSG_BS_INTERRUPTE == msg->opCode ||
        MSG_BS_INTERRUPTE_SELF == msg->opCode ||
        MSG_BS_DISCONNECT == msg->opCode  )
   {
      _needReply = FALSE ;
   }
   else if ( MSG_BS_INSERT_REQ == msg->opCode ||
             MSG_BS_DELETE_REQ == msg->opCode ||
             MSG_BS_UPDATE_REQ == msg->opCode )
   {
      _needReply = FALSE ;
   }
   else
   {
      _needReply = TRUE ;
   }

   MON_START_OP( _pEDUCB->getMonAppCB() ) ;

   return SDB_OK ;
}

INT32 _pmdMongoSession::_onMsgEnd( INT32 result, MsgHeader *msg )
{
   _contextBuff.release() ;

   if ( result && SDB_DMS_EOC != result )
   {
      PD_LOG( PDWARNING, "Session[%s] process msg[opCode=%d, len: %d, "
              "TID: %d, requestID: %llu] failed, rc: %d",
              sessionName(), msg->opCode, msg->messageLength, msg->TID,
              msg->requestID, result ) ;
   }

   MON_END_OP( _pEDUCB->getMonAppCB() ) ;
}

INT32 _pmdMongoSession::_reply( MsgOpReply *replyHeader,
                                const CHAR *pBody,
                                const INT32 len )
{
   INT32 rc         = SDB_OK ;
   SINT16 opCode    = 0 ;
   INT32 responseTo = 0 ;
   INT32 nToReturn  = 0 ;
   INT32 startFrom  = 0 ;
   INT32 msgLen     = 0 ;
   SINT64 cursorID  = 0 ;
   mongoMsgReply header ;
   bson::BSONObjBuilder bob ;
   bson::BSONObj bsonBody ;
   BOOLEAN bigEndian = _converter->isBigEndian() ;

   header.id = 0 ;
   opCode = dbReply ;
   ossEndianConvertIf( opCode, header.opCode, bigEndian ) ;
   responseTo = replyHeader->header.requestID ;
   ossEndianConvertIf( responseTo,  header.responseTo, bigEndian ) ;
   header._flags = 0 ;
   header._version = 0 ;
   header.reservedFlags = 0 ;
   cursorID = replyHeader->contextID ;
   ossEndianConvertIf( cursorID, header.cursorId, bigEndian ) ;
   startFrom = replyHeader->startFrom ;
   ossEndianConvertIf( startFrom, header.startingFrom, bigEndian ) ;
   nToReturn = replyHeader->startFrom ;
   ossEndianConvertIf( nToReturn, header.nReturned, bigEndian ) ;

   if ( pBody )
   {
      bsonBody.init( pBody ) ;
      rc = bsonBody.getBoolField( OP_ERRNOFIELD ) ? 0.0 : 1.0 ;
      bob.append( "ok", rc ) ;
      bob.append( "code",  rc ) ;
      bob.append( "errmsg", bsonBody.getStringField( OP_ERRDESP_FIELD) ) ;

      _outStream.write( bob.obj().objdata(), bob.obj().objsize() ) ;
   }

   msgLen = sizeof( mongoMsgReply ) + bob.obj().objsize() ;
   ossEndianConvertIf( msgLen, header.len, bigEndian ) ;

   rc = sendData( (CHAR *)&header, sizeof( mongoMsgHeader ) ) ;
   if ( rc )
   {
      PD_LOG( PDERROR, "Session[%s] failed to send response header, rc: %d",
              sessionName(), rc ) ;
      goto error ;
   }

   if ( pBody )
   {
      rc = sendData(  _outStream.data(), _outStream.size() ) ;
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

void _pmdMongoSession::_onAttach( )
{
   engine::pmdKRCB *krcb = engine::pmdGetKRCB() ;
   _pDPSCB = krcb->getDPSCB() ;

   if ( _pDPSCB && !_pDPSCB->isLogLocal() )
   {
      _pDPSCB = NULL ;
   }
}

void _pmdMongoSession::_onDetach()
{
}

void _pmdMongoSession::_zeroStream()
{
   if ( !_inStream.empty() )
   {
      _inStream.zero() ;
   }

   if ( !_outStream.empty() )
   {
      _outStream.zero() ;
   }
}
