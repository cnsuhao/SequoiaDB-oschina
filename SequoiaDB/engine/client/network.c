/*******************************************************************************
   Copyright (C) 2012-2014 SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*******************************************************************************/

#include "network.h"
#include "ossUtil.h"
#if defined (_LINUX)
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/tcp.h>
#else
#pragma comment(lib, "Ws2_32.lib")
#endif
#if defined (_WINDOWS)
#define SOCKET_GETLASTERROR WSAGetLastError()
#else
#define SOCKET_GETLASTERROR errno
#endif

#if defined (_WINDOWS)
static BOOLEAN sockInitialized = FALSE ;
#endif
INT32 clientConnect ( const CHAR *pHostName,
                      const CHAR *pServiceName,
                      SOCKET *sock )
{
   INT32 rc = SDB_OK ;
   struct hostent *hp = NULL ;
   struct servent *servinfo ;
   struct sockaddr_in sockAddress ;
   UINT16 port ;
   if ( !sock )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   ossMemset ( &sockAddress, 0, sizeof(sockAddress) ) ;
#if defined (_WINDOWS)
   if ( !sockInitialized )
   {
      WSADATA data = {0} ;
      rc = WSAStartup ( MAKEWORD ( 2,2 ), &data ) ;
      if ( INVALID_SOCKET == rc )
      {
         rc = SDB_NETWORK ;
         goto error ;
      }
      sockInitialized = TRUE ;
   }
#endif
   sockAddress.sin_family = AF_INET ;
   if ( (hp = gethostbyname ( pHostName ) ) )
      sockAddress.sin_addr.s_addr = *((UINT32*)hp->h_addr_list[0] ) ;
   else
      sockAddress.sin_addr.s_addr = inet_addr ( pHostName ) ;
   servinfo = getservbyname ( pServiceName, "tcp" ) ;
   if ( !servinfo )
   {
      port = atoi ( pServiceName ) ;
   }
   else
   {
      port = (UINT16)ntohs(servinfo->s_port) ;
   }
   sockAddress.sin_port = htons ( port ) ;
   *sock = socket ( AF_INET, SOCK_STREAM, IPPROTO_TCP ) ;
   if ( -1 == *sock )
   {
      rc = SDB_NETWORK ;
      goto error ;
   }
   rc = connect ( *sock, (struct sockaddr *) &sockAddress,
                    sizeof( sockAddress ) ) ;
   if ( rc )
   {
      clientDisconnect ( *sock ) ;
      *sock = -1 ;
      rc = SDB_NETWORK ;
      goto error ;
   }

   disableNagle( *sock ) ;

done :
   return rc ;
error :
   goto done ;
}

INT32 disableNagle( SOCKET sock )
{
   INT32 rc = SDB_OK ;
   INT32 temp = 1 ;
   rc = setsockopt ( sock, IPPROTO_TCP, TCP_NODELAY, (CHAR *) &temp,
                     sizeof ( INT32 ) ) ;
   if ( rc )
   {
      goto error ;
   }

   rc = setsockopt ( sock, SOL_SOCKET, SO_KEEPALIVE, (CHAR *) &temp,
                     sizeof ( INT32 ) ) ;
   if ( rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

void clientDisconnect ( SOCKET sock )
{
#if defined (_WINDOWS)
      closesocket ( sock ) ;
#else
      close ( sock ) ;
#endif
}

INT32 clientSend ( SOCKET sock, const CHAR *pMsg, INT32 len, INT32 timeout )
{
   INT32 rc = SDB_OK ;
   SOCKET maxFD = sock ;
   struct timeval maxSelectTime ;
   fd_set fds ;
   if ( !pMsg || !len )
      goto done ;
   maxSelectTime.tv_sec = timeout / 1000000 ;
   maxSelectTime.tv_usec = timeout % 1000000 ;
   while ( TRUE )
   {
      FD_ZERO ( &fds ) ;
      FD_SET ( sock, &fds ) ;
      rc = select ( maxFD + 1, NULL, &fds, NULL,
                    timeout>=0?&maxSelectTime:NULL ) ;
      if ( 0 == rc )
      {
         rc = SDB_TIMEOUT ;
         goto done ;
      }
      if ( 0 > rc )
      {
         rc = SOCKET_GETLASTERROR ;
         if (
#if defined (_WINDOWS)
            WSAEINTR
#else
            EINTR
#endif
            == rc )
         {
            continue ;
         }
         rc = SDB_NETWORK ;
         goto error ;
      }

      if ( FD_ISSET ( sock, &fds ) )
      {
         break ;
      }
   }
   while ( len > 0 )
   {
#if defined (_WINDOWS)
      rc = send ( sock, pMsg, len, 0 ) ;
      if ( SOCKET_ERROR == rc )
#else
      rc = send ( sock, pMsg, len, MSG_NOSIGNAL ) ;
      if ( -1 == rc )
#endif
      {
         rc = SDB_NETWORK ;
         goto error ;
      }
      len -= rc ;
      pMsg += rc ;
   }
   rc = SDB_OK ;
done :
   return rc ;
error :
   goto done ;
}
#define MAX_RECV_RETRIES 5
INT32 clientRecv ( SOCKET sock, CHAR *pMsg, INT32 len, INT32 timeout )
{
   INT32 rc = SDB_OK ;
   UINT32 retries = 0 ;
   SOCKET maxFD = sock ;
   struct timeval maxSelectTime ;
   fd_set fds ;
   if ( !pMsg || !len )
      goto done ;
   maxSelectTime.tv_sec = timeout / 1000000 ;
   maxSelectTime.tv_usec = timeout % 1000000 ;
   while ( TRUE )
   {
      FD_ZERO ( &fds ) ;
      FD_SET ( sock, &fds ) ;
      rc = select ( maxFD + 1, &fds, NULL, NULL,
                    timeout>=0?&maxSelectTime:NULL ) ;
      if ( 0 == rc )
      {
         rc = SDB_TIMEOUT ;
         goto done ;
      }
      if ( 0 > rc )
      {
         rc = SOCKET_GETLASTERROR ;
         if (
#if defined (_WINDOWS)
               WSAEINTR
#else
               EINTR
#endif
               == rc )
         {
            continue ;
         }
         rc = SDB_NETWORK ;
         goto error ;
      }
      if ( FD_ISSET ( sock, &fds ) )
      {
         break ;
      }
   }
   while ( len > 0 )
   {
#if defined (_WINDOWS)
      rc = recv ( sock, pMsg, len, 0 ) ;
#else
      rc = recv ( sock, pMsg, len, MSG_NOSIGNAL ) ;
#endif
      if ( rc > 0 )
      {
         len -= rc ;
         pMsg += rc ;
      }
      else if ( rc == 0 )
      {
         rc = SDB_NETWORK_CLOSE ;
         goto error ;
      }
      else
      {
         rc = SOCKET_GETLASTERROR ;
#if defined (_WINDOWS)
         if ( WSAETIMEDOUT == rc )
#else
         if ( (EAGAIN == rc || EWOULDBLOCK == rc ) )
#endif
         {
            rc = SDB_NETWORK ;
            goto error ;
         }
         if ( (
#if defined ( _WINDOWS )
              WSAEINTR
#else
              EINTR
#endif
              == rc ) && ( retries < MAX_RECV_RETRIES ) )
         {
            ++retries ;
            continue ;
         }
         rc = SDB_NETWORK ;
         goto error ;
      }
   }
   rc = SDB_OK ;
done :
   return rc ;
error :
   goto done ;
}

