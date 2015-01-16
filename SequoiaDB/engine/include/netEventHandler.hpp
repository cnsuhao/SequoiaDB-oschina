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

   Source File Name = netEventHandler.hpp

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

#ifndef NETEVENTHANDLER_HPP_
#define NETEVENTHANDLER_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "netDef.hpp"
#include "ossLatch.hpp"

#include <string>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>
using namespace boost::asio ;
using namespace std ;

namespace engine
{
   /*
      NET_EVENT_HANDLER_STATE define
   */
   enum NET_EVENT_HANDLER_STATE
   {
      NET_EVENT_HANDLER_STATE_HEADER         = 0,
      NET_EVENT_HANDLER_STATE_HEADER_LAST,
      NET_EVENT_HANDLER_STATE_BODY
   } ;

   class _netFrame ;

   /*
      _netEventHandler define
   */
   class _netEventHandler :
         public boost::enable_shared_from_this<_netEventHandler>,
         public SDBObject
   {
      public:
         _netEventHandler( _netFrame *frame ) ;

         ~_netEventHandler() ;

      public:
         OSS_INLINE void id( const _MsgRouteID &id )
         {
            _id = id ;
         }
         OSS_INLINE const _MsgRouteID &id()
         {
            return _id ;
         }
         OSS_INLINE boost::asio::ip::tcp::socket &socket()
         {
            return _sock ;
         }
         OSS_INLINE _ossSpinXLatch &mtx()
         {
            return _mtx ;
         }
         OSS_INLINE NET_HANDLE handle()
         {
            return _handle ;
         }
         CHAR *msg()
         {
            return _buf ;
         }
         OSS_INLINE NET_EVENT_HANDLER_STATE state()
         {
            return _state ;
         }
         OSS_INLINE void close()
         {
            UINT32 timeout = 0 ;
            _mtx.get() ;
            _isConnected = FALSE ;
            while ( _isInAsync && timeout < 60000 )
            {
               ossSleep( 50 ) ;
               timeout += 50 ;
            }
            SDB_ASSERT( timeout < 60000, "socket is dead locked" ) ;
            _sock.close() ;
            _mtx.release() ;
         }

      public:
         void asyncRead() ;

         INT32 syncConnect( const CHAR *hostName,
                            const CHAR *serviceName ) ;

         INT32 syncSend( const void *buf,
                         UINT32 len ) ;

         void  setOpt() ;

         string localAddr() const ;
         string remoteAddr() const ;
         UINT16 localPort() const ;
         UINT16 remotePort() const ;

         BOOLEAN isLocalConnection() const ;

      private:
         void _readCallback(const boost::system::error_code &
                            error ) ;

         INT32 _allocateBuf( UINT32 len ) ;

      private:
         boost::asio::ip::tcp::socket     _sock ;
         _ossSpinXLatch                   _mtx ;
         _MsgHeader                       _header ;
         CHAR                             *_buf ;
         UINT32                           _bufLen ;
         NET_EVENT_HANDLER_STATE          _state ;
         _MsgRouteID                      _id ;
         _netFrame                        *_frame ;
         NET_HANDLE                       _handle ;
         BOOLEAN                          _isConnected ;
         BOOLEAN                          _isInAsync ;
         BOOLEAN                          _hasRecvMsg ;

   };

   typedef boost::shared_ptr<_netEventHandler> NET_EH ;

}

#endif // NETEVENTHANDLER_HPP_

