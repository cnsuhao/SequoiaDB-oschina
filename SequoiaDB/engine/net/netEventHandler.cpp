/******************************************************************************

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

   Source File Name = netEventHandler.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-motionatted
   versions of PD component. This file contains declare of PD functions.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "core.hpp"
#include "netEventHandler.hpp"
#include "netFrame.hpp"
#include "ossMem.hpp"
#include "msgDef.h"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "netTrace.hpp"
#include <boost/bind.hpp>

using namespace boost::asio::ip ;
namespace engine
{
   _netEventHandler::_netEventHandler( _netFrame *frame ):
                                       _sock(frame->ioservice()),
                                       _buf(NULL),
                                       _bufLen(0),
                                       _state(NET_EVENT_HANDLER_STATE_HEADER),
                                       _frame(frame),
                                       _handle(_frame->allocateHandle())
   {
      _id.value      = MSG_INVALID_ROUTEID ;
      _isConnected   = FALSE ;
      _isInAsync     = FALSE ;
      _hasRecvMsg    = FALSE ;
   }

   _netEventHandler::~_netEventHandler()
   {
      close() ;
      if ( NULL != _buf )
      {
         SDB_OSS_FREE( _buf ) ;
      }
   }

   string _netEventHandler::localAddr() const
   {
      string addr ;
      try
      {
         addr = _sock.local_endpoint().address().to_string() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "get local address occurred exception: %s",
                 e.what() ) ;
      }
      return addr ;
   }

   string _netEventHandler::remoteAddr() const
   {
      string addr ;
      try
      {
         addr = _sock.remote_endpoint().address().to_string() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "get remote address occurred exception: %s",
                 e.what() ) ;
      }
      return addr ;
   }

   UINT16 _netEventHandler::localPort () const
   {
      UINT16 port = 0 ;
      try
      {
         port = _sock.local_endpoint().port() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "get local port occurred exception: %s", e.what() ) ;
      }
      return port ;
   }

   UINT16 _netEventHandler::remotePort () const
   {
      UINT16 port = 0 ;
      try
      {
         port = _sock.remote_endpoint().port() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "get remote port occurred exception: %s", e.what() ) ;
      }
      return port ;
   }

   BOOLEAN _netEventHandler::isLocalConnection() const
   {
      return localAddr() == remoteAddr() ? TRUE : FALSE ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__NETEVNHND_SETOPT, "_netEventHandler::setOpt" )
   void _netEventHandler::setOpt()
   {
      PD_TRACE_ENTRY ( SDB__NETEVNHND_SETOPT );

      _isConnected = TRUE ;

      try
      {
         _sock.set_option( tcp::no_delay(TRUE) ) ;
         _sock.set_option( tcp::socket::keep_alive(TRUE) ) ;
#if defined (_LINUX)
         INT32 keepAlive = 1 ;
         INT32 keepIdle = 15 ;
         INT32 keepInterval = 5 ;
         INT32 keepCount = 3 ;
         INT32 res = SDB_OK ;
         struct timeval sendtimeout ;
         sendtimeout.tv_sec = 1 ;
         sendtimeout.tv_usec = 0 ;
         SOCKET nativeSock = _sock.native() ;
         res = setsockopt( nativeSock, SOL_SOCKET, SO_KEEPALIVE,
                     ( void *)&keepAlive, sizeof(keepAlive) ) ;
         if ( SDB_OK != res )
         {
            PD_LOG( PDERROR, "failed to set keepalive of sock[%d],"
                    "err:%d", nativeSock, res ) ;
         }
         res = setsockopt( nativeSock, SOL_TCP, TCP_KEEPIDLE,
                     ( void *)&keepIdle, sizeof(keepIdle) ) ;
         if ( SDB_OK != res )
         {
            PD_LOG( PDERROR, "failed to set keepidle of sock[%d],"
                    "err:%d", nativeSock, res ) ;
         }
         res = setsockopt( nativeSock, SOL_TCP, TCP_KEEPINTVL,
                     ( void *)&keepInterval, sizeof(keepInterval) ) ;
         if ( SDB_OK != res )
         {
            PD_LOG( PDERROR, "failed to set keepintvl of sock[%d],"
                    "err:%d", nativeSock, res ) ;
         }
         res = setsockopt( nativeSock, SOL_TCP, TCP_KEEPCNT,
                     ( void *)&keepCount, sizeof(keepCount) ) ;
         if ( SDB_OK != res )
         {
            PD_LOG( PDERROR, "failed to set keepcnt of sock[%d],"
                    "err:%d", nativeSock, res ) ;
         }
         res = setsockopt( nativeSock, SOL_SOCKET, SO_SNDTIMEO,
                           ( CHAR * )&sendtimeout, sizeof(struct timeval) ) ;
         if ( SDB_OK != res )
         {
            PD_LOG( PDERROR, "failed to set sndtimeout of sock[%d],"
                    "err:%d", nativeSock, res ) ;
         }
#endif
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "failed to set no delay:%s", e.what() ) ;
      }
      PD_TRACE_EXIT ( SDB__NETEVNHND_SETOPT );
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__NETEVNHND_SYNCCONN, "_netEventHandler::syncConnect" )
   INT32 _netEventHandler::syncConnect( const CHAR *hostName,
                                        const CHAR *serviceName )
   {
      SDB_ASSERT( NULL != hostName, "hostName should not be NULL" ) ;
      SDB_ASSERT( NULL != serviceName, "serviceName should not be NULL" ) ;

      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__NETEVNHND_SYNCCONN );

      if ( _isConnected )
      {
         close() ;
      }

/*
      try
      {

         boost::system::error_code ec ;
         tcp::resolver::query query ( tcp::v4(), hostName, serviceName ) ;
         tcp::resolver resolver ( _frame->ioservice() ) ;
         tcp::resolver::iterator itr = resolver.resolve ( query ) ;
         ip::tcp::endpoint endpoint = *itr ;
         _sock.open( tcp::v4()) ;

         _sock.connect( endpoint, ec ) ;
         if ( ec )
         {
            if ( boost::asio::error::would_block ==
                 ec )
            {
            rc = _complete( _sock.native() ) ;
            if ( SDB_OK != rc )
            {
               _sock.close() ;
               PD_LOG ( PDWARNING,
                  "Failed to connect to %s: %s: timeout",
                  hostName, serviceName ) ;
               goto error ;
            }
            }
            else
            {
               PD_LOG ( PDWARNING,
                  "Failed to connect to %s: %s: %s", hostName, serviceName,
                  ec.message().c_str()) ;
               rc = SDB_NET_CANNOT_CONNECT ;
               _sock.close() ;
               goto error ;
            }
         }
      }
      catch ( boost::system::system_error &e )
      {
         PD_LOG ( PDWARNING,
                  "Failed to connect to %s: %s: %s", hostName, serviceName,
                  e.what() ) ;
         rc = SDB_NET_CANNOT_CONNECT ;
         _sock.close() ;
         goto error ;
      }
*/
      UINT16 port = 0 ;
      rc = ossGetPort( serviceName, port ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get port :%s", serviceName ) ;
         goto error ;
      }

      {
         _ossSocket sock( hostName, port ) ;
         rc = sock.initSocket() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to init socket:%d", rc ) ;
            goto error ;
         }
         sock.closeWhenDestruct( FALSE ) ;
         rc = sock.connect() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to connect remote[%s:%s], rc:%d",
                    hostName, serviceName, rc ) ;
            goto error ;
         }

         try
         {
            _sock.assign( tcp::v4(), sock.native() ) ; 
         }
         catch ( std::exception &e )
         {
            PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
            rc = SDB_SYS ;
            sock.close() ;
            _sock.close() ;
            goto error ;
         }
      }

      setOpt() ;
   done:
      PD_TRACE_EXITRC ( SDB__NETEVNHND_SYNCCONN, rc );
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__NETEVNHND_ASYNCRD, "_netEventHandler::asyncRead" )
   void _netEventHandler::asyncRead()
   {
      PD_TRACE_ENTRY ( SDB__NETEVNHND_ASYNCRD ) ;

      if ( NET_EVENT_HANDLER_STATE_HEADER == _state ||
           NET_EVENT_HANDLER_STATE_HEADER_LAST == _state )
      {
         _isInAsync = TRUE ;
         if ( !_isConnected )
         {
            PD_LOG( PDWARNING, "Connection[routeID: %u,%u,%u; Handel: %u] "
                    "already closed", _id.columns.groupID, _id.columns.nodeID,
                    _id.columns.serviceID, _handle ) ;
            goto error ;
         }

         if ( NET_EVENT_HANDLER_STATE_HEADER_LAST == _state )
         {
            async_read( _sock, buffer(
                        (CHAR*)&_header + sizeof(MsgSysInfoRequest),
                        sizeof(_MsgHeader) - sizeof(MsgSysInfoRequest) ),
                        boost::bind(&_netEventHandler::_readCallback,
                                    shared_from_this(),
                                    boost::asio::placeholders::error ) ) ;
         }
         else if ( FALSE == _hasRecvMsg )
         {
            async_read( _sock, buffer(&_header, sizeof(MsgSysInfoRequest)),
                        boost::bind(&_netEventHandler::_readCallback,
                                    shared_from_this(),
                                    boost::asio::placeholders::error )) ;
         }
         else
         {
            async_read( _sock, buffer(&_header, sizeof(_MsgHeader)),
                        boost::bind(&_netEventHandler::_readCallback,
                                    shared_from_this(),
                                    boost::asio::placeholders::error )) ;
         }
      }
      else
      {
         UINT32 len = _header.messageLength ;
         if ( SDB_OK != _allocateBuf( len ) )
         {
            goto error ;
         }
         ossMemcpy( _buf, &_header, sizeof( _MsgHeader ) ) ;

         _isInAsync = TRUE ;
         if ( !_isConnected )
         {
            PD_LOG( PDWARNING, "Connection[routeID: %u,%u,%u; Handel: %u] "
                    "already closed", _id.columns.groupID, _id.columns.nodeID,
                    _id.columns.serviceID, _handle ) ;
            goto error ;
         }
         async_read( _sock, buffer(
                     (CHAR *)((ossValuePtr)_buf + sizeof(_MsgHeader)),
                     len - sizeof(_MsgHeader)),
                     boost::bind( &_netEventHandler::_readCallback,
                                  shared_from_this(),
                                  boost::asio::placeholders::error ) ) ;
      }

   done:
      _isInAsync = FALSE ;
      PD_TRACE_EXIT ( SDB__NETEVNHND_ASYNCRD ) ;
      return ;
   error:
      _isInAsync = FALSE ;
      if ( _isConnected )
      {
         close() ;
      }
      _frame->handleClose( shared_from_this(), _id ) ;
      _frame->_erase( handle() ) ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__NETEVNHND_SYNCSND, "_netEventHandler::syncSend" )
   INT32 _netEventHandler::syncSend( const void *buf,
                                     UINT32 len )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__NETEVNHND_SYNCSND );
      UINT32 send = 0 ;
      try
      {
         while ( send < len )
         {
            send +=  _sock.send(buffer((const void*)((ossValuePtr)buf + send),
                                        len - send));
         }
      }
      catch ( boost::system::system_error &e )
      {
         PD_LOG( PDERROR, "Failed to send to node :%d, %d, %d, %s",
                 _id.columns.groupID, _id.columns.nodeID,
                 _id.columns.serviceID, e.what() ) ;
         rc = SDB_NET_SEND_ERR ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__NETEVNHND_SYNCSND, rc );
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__NETEVNHND__ALLOBUF, "_netEventHandler::_allocateBuf" )
   INT32 _netEventHandler::_allocateBuf( UINT32 len )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__NETEVNHND__ALLOBUF );
      if ( _bufLen < len )
      {
         if ( NULL != _buf )
         {
            SDB_OSS_FREE( _buf ) ;
            _bufLen = 0 ;
         }
         _buf = (CHAR *)SDB_OSS_MALLOC( len ) ;
         if ( NULL == _buf )
         {
            PD_LOG( PDERROR, "mem allocate failed, len: %u", len ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         _bufLen = len ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__NETEVNHND__ALLOBUF, rc );
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__NETEVNHND__RDCALLBK, "_netEventHandler::_readCallback" )
   void _netEventHandler::_readCallback( const boost::system::error_code &
                                         error )
   {
      PD_TRACE_ENTRY ( SDB__NETEVNHND__RDCALLBK ) ;

      if ( error )
      {
         if ( error.value() == boost::system::errc::operation_canceled ||
              error.value() == boost::system::errc::no_such_file_or_directory )
         {
            PD_LOG ( PDINFO, "connection aborted, node:%d, %d, %d",
                     _id.columns.groupID, _id.columns.nodeID,
                     _id.columns.serviceID ) ;
         }
         else
         {
            PD_LOG ( PDERROR, "Error received, node:%d, %d, %d, err=%d",
                     _id.columns.groupID, _id.columns.nodeID,
                     _id.columns.serviceID, error.value() ) ;
         }

         goto error_close ;
      }

      if ( NET_EVENT_HANDLER_STATE_HEADER == _state )
      {
         if ( ( UINT32 )MSG_SYSTEM_INFO_LEN == (UINT32)_header.messageLength )
         {
            if ( SDB_OK != _allocateBuf( sizeof(MsgSysInfoRequest) ))
            {
               goto error_close ;
            }
            _hasRecvMsg = TRUE ;
            ossMemcpy( _buf, &_header, sizeof( MsgSysInfoRequest ) ) ;
            _frame->handleMsg( shared_from_this() ) ;
            _state = NET_EVENT_HANDLER_STATE_HEADER ;
            asyncRead() ;
            goto done ;
         }
         else if ( sizeof(_MsgHeader) > (UINT32)_header.messageLength ||
                   SDB_MAX_MSG_LENGTH < (UINT32)_header.messageLength )
         {
            PD_LOG( PDERROR, "Error header[len: %d, opCode: (%d)%d, TID:%d] "
                    "received, node:%d, %d, %d", _header.messageLength,
                    IS_REPLY_TYPE(_header.opCode) ? 1 : 0,
                    GET_REQUEST_TYPE(_header.opCode), _header.TID,
                    _id.columns.groupID,
                    _id.columns.nodeID, _id.columns.serviceID ) ;
            goto error_close ;
         }
         else
         {
            if ( FALSE == _hasRecvMsg )
            {
               _hasRecvMsg = TRUE ;
               _state = NET_EVENT_HANDLER_STATE_HEADER_LAST ;
               asyncRead() ;
               _state = NET_EVENT_HANDLER_STATE_HEADER ;
               goto done ;
            }

            PD_LOG( PDDEBUG, "msg header: [len:%d], [opCode: [%d]%d], "
                             "[TID:%d], [groupID:%d], [nodeID:%d], "
                             "[ADDR:%s], [PORT:%d]",
                    _header.messageLength, IS_REPLY_TYPE(_header.opCode)?1:0,
                    GET_REQUEST_TYPE(_header.opCode),
                    _header.TID, _header.routeID.columns.groupID,
                    _header.routeID.columns.nodeID,
                    remoteAddr().c_str(), remotePort() ) ;
            if ( MSG_INVALID_ROUTEID == _id.value )
            {
               if ( MSG_INVALID_ROUTEID != _header.routeID.value )
               {
                  _id = _header.routeID ;
                  _frame->_addRoute( shared_from_this() ) ;
               }
            }
         }
         if ( (UINT32)sizeof(_MsgHeader) == (UINT32)_header.messageLength )
         {
            if ( SDB_OK != _allocateBuf( sizeof(_MsgHeader) ))
            {
               goto error_close ;
            }
            ossMemcpy( _buf, &_header, sizeof( _MsgHeader ) ) ;
            _frame->handleMsg( shared_from_this() ) ;
            _state = NET_EVENT_HANDLER_STATE_HEADER ;
            asyncRead() ;
            goto done ;
         }

#if defined (_LINUX)
         try
         {
            boost::asio::detail::socket_option::boolean<IPPROTO_TCP, TCP_QUICKACK>
                                                    quickack( TRUE ) ;
            _sock.set_option( quickack ) ;
         }
         catch ( boost::system::system_error &e )
         {
            PD_LOG ( PDERROR, "Failed to quick ack: %s", e.what() ) ;
         }
#endif // _LINUX

         _state = NET_EVENT_HANDLER_STATE_BODY ;
         asyncRead() ;
      }
      else
      {
         _frame->handleMsg( shared_from_this() ) ;
         _state = NET_EVENT_HANDLER_STATE_HEADER ;
         asyncRead() ;
      }

   done:
      PD_TRACE_EXIT ( SDB__NETEVNHND__RDCALLBK ) ;
      return ;
   error_close:
      if ( _isConnected )
      {
         close() ;
      }
      _frame->handleClose( shared_from_this(), _id ) ;
      _frame->_erase( handle() ) ;
      goto done ;
   }

}

