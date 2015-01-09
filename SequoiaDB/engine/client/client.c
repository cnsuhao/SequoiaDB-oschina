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
#include "client_internal.h"
#include "bson/bson.h"
#include "ossUtil.h"
#include "ossMem.h"
#include "msg.h"
#include "msgDef.h"
#include "network.h"
#include "common.h"
#include "pmdOptions.h"
#include "msgCatalogDef.h"
#include "../bson/lib/md5.h"
#include "fmpDef.h"

#if defined( _LINUX )
#include <arpa/inet.h>
#include <netinet/tcp.h>
#endif // _LINUX

SDB_EXTERN_C_START
BOOLEAN g_disablePassEncode = FALSE ;
SDB_EXTERN_C_END

#define CLIENT_UNUSED( p )         \
   do { \
      p = p ; \
   } while ( 0 )

#define HANDLE_CHECK( handle, interhandle, handletype ) \
do                                                      \
{                                                       \
   if ( SDB_INVALID_HANDLE == handle )                  \
   {                                                    \
      rc = SDB_INVALIDARG ;                             \
      goto error ;                                      \
   }                                                    \
   if ( !interhandle ||                                 \
        handletype != interhandle->_handleType )        \
   {                                                    \
      rc = SDB_CLT_INVALID_HANDLE ;                     \
      goto error ;                                      \
   }                                                    \
}while( FALSE )

#define CHECK_RET_MSGHEADER( pSendBuf, pRecvBuf, connHandle ) \
do                                                            \
{                                                             \
   rc = clientCheckRetMsgHeader( pSendBuf, pRecvBuf ) ;       \
   if ( SDB_OK != rc )                                        \
   {                                                          \
      if ( SDB_UNEXPECTED_RESULT == rc )                      \
      {                                                       \
         _sdbDisconnect_inner( connHandle ) ;                 \
      }                                                       \
      goto error ;                                            \
   }                                                          \
}while( FALSE )

#define ALLOC_HANDLE( handle, type )                      \
do                                                        \
{                                                         \
   handle = ( type* ) SDB_OSS_MALLOC ( sizeof( type ) ) ; \
   if ( !handle )                                         \
   {                                                      \
      rc = SDB_OOM ;                                      \
      goto error ;                                        \
   }                                                      \
   ossMemset ( handle, 0, sizeof( type ) ) ;              \
}while( FALSE )

#define INIT_CURSOR( cursor, conn, handle, contextID )      \
do                                                          \
{                                                           \
   cursor->_handleType = SDB_HANDLE_TYPE_CURSOR ;           \
   cursor->_connection=                                     \
               ( sdbConnectionHandle )conn ;                \
   cursor->_sock            = handle->_sock ;               \
   cursor->_contextID       = contextID ;                   \
   cursor->_offset          = -1 ;                          \
   cursor->_endianConvert   = handle->_endianConvert ;      \
}while( FALSE )

#define BSON_INIT( bsonobj )              \
do                                        \
{                                         \
   bson_init( &bsonobj );                 \
   bsoninit = TRUE ;                      \
}while ( FALSE )                            

#define BSON_APPEND_NULL( bsonobj, key )                 \
do                                                       \
{                                                        \
   rc = bson_append_null( &bsonobj, key ) ;              \
   if ( rc )                                             \
   {                                                     \
      rc = SDB_SYS ;                                     \
      goto error ;                                       \
   }                                                     \
}while ( FALSE ) 

#define BSON_APPEND( bsonobj, key, val, type )       \
do                                                   \
{                                                    \
   rc = bson_append_##type( &bsonobj, key, val ) ;   \
   if ( rc )                                         \
   {                                                 \
      rc = SDB_SYS ;                                 \
      goto error ;                                   \
   }                                                 \
}while( FALSE )


#define BSON_FINISH( bson )        \
do                                 \
{                                  \
   rc = bson_finish ( &bson ) ;    \
   if ( rc )                       \
   {                               \
      rc = SDB_SYS ;               \
      goto error ;                 \
   }                               \
}while ( FALSE )

#define BSON_DESTROY( bson )       \
do                                 \
{                                  \
   if ( bsoninit )                 \
   {                               \
      bson_destroy( &bson ) ;      \
   }                               \
}while( FALSE )

#define SET_INVALID_HANDLE( handle ) \
if ( handle )                        \
{                                    \
   *handle = SDB_INVALID_HANDLE ;    \
}

#define LOB_INIT( lob, conn, handle )\
        do\
        {\
           lob->_handleType = SDB_HANDLE_TYPE_LOB ;\
           lob->_connection = ( sdbConnectionHandle )conn ;\
           lob->_sock = handle->_sock ;\
           lob->_contextID = -1 ;\
           lob->_offset = -1 ; \
           lob->_endianConvert = handle->_endianConvert ;\
           lob->_cachedOffset = -1 ;\
        } while ( FALSE )

#define LOB_ALIGNED_LEN 524288

static BOOLEAN _sdbIsSrand = FALSE ;

void  _sdbDisconnect_inner ( sdbConnectionHandle handle ) ;


#if defined (_LINUX)
static UINT32 _sdbRandSeed = 0 ;
#endif
static void _sdbSrand ()
{
   if ( !_sdbIsSrand )
   {
#if defined (_WINDOWS)
      srand ( (UINT32) time ( NULL ) ) ;
#elif defined (_LINuX)
      _sdbRandSeed = time ( NULL ) ;
#endif
      _sdbIsSrand = TRUE ;
   }
}

static UINT32 _sdbRand ()
{
   UINT32 randVal = 0 ;
   if ( !_sdbIsSrand )
   {
      _sdbSrand () ;
   }
#if defined (_WINDOWS)
   rand_s ( &randVal ) ;
#elif defined (_LINUX)
   randVal = rand_r ( &_sdbRandSeed ) ;
#endif
   return randVal ;
}

/***********************************************************
 *** note: internal function's parmeters don't check     ***
 ***       because interface function already check it   ***
 ***********************************************************/
#define SDB_CLIENT_DFT_NETWORK_TIMEOUT -1
static INT32 _setRGName ( sdbReplicaGroupHandle handle,
                          const CHAR *pGroupName )
{
   INT32 rc       = SDB_OK ;
   INT32 len      = 0 ;
   sdbRGStruct *r = (sdbRGStruct*)handle ;

   if ( ( len = ossStrlen ( pGroupName ) ) > CLIENT_RG_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   ossMemset ( r->_replicaGroupName, 0, sizeof(r->_replicaGroupName) ) ;
   ossMemcpy ( r->_replicaGroupName, pGroupName, len ) ;
   r->_isCatalog = ( 0 == ossStrcmp ( pGroupName, CAT_CATALOG_GROUPNAME ) ) ;
done :
   return rc ;
error :
   goto done ;
}

static INT32 _setCSName ( sdbCSHandle handle,
                          const CHAR *pCollectionSpaceName )
{
   INT32 rc       = SDB_OK ;
   INT32 len      = 0 ;
   sdbCSStruct *s = (sdbCSStruct*)handle ;

   if ( ( len = ossStrlen ( pCollectionSpaceName) ) > CLIENT_CS_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   ossMemset ( s->_CSName, 0, sizeof(s->_CSName) ) ;
   ossMemcpy ( s->_CSName, pCollectionSpaceName, len ) ;
done :
   return rc ;
error :
   goto done ;
}

#define CL_FULLNAME_LEN (CLIENT_CS_NAMESZ + CLIENT_COLLECTION_NAMESZ +1)
static INT32 _setCollectionName ( sdbCollectionHandle handle,
                                  const CHAR *pCollectionFullName )
{
   INT32 rc                 = SDB_OK ;
   INT32 collectionSpaceLen = 0 ;
   INT32 collectionLen      = 0 ;
   INT32 fullLen            = 0 ;
   CHAR *pDot               = NULL ;
   CHAR *pDot1              = NULL ;
   CHAR collectionFullName [ CL_FULLNAME_LEN  + 1 ] = {0} ;
   sdbCollectionStruct *s  = (sdbCollectionStruct*)handle ;

   ossMemset ( s->_CSName, 0, sizeof ( s->_CSName ) );
   ossMemset ( s->_collectionName, 0, sizeof ( s->_collectionName ) ) ;
   ossMemset ( s->_collectionFullName, 0, sizeof ( s->_collectionFullName ) ) ;
   if ( (fullLen = ossStrlen ( pCollectionFullName )) >
        CL_FULLNAME_LEN )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   ossStrncpy ( collectionFullName, pCollectionFullName, fullLen ) ;
   pDot = (CHAR*)ossStrchr ( (CHAR*)collectionFullName, '.' ) ;
   pDot1 = (CHAR*)ossStrrchr ( (CHAR*)collectionFullName, '.' ) ;
   if ( !pDot || ( pDot != pDot1 ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   *pDot++ = 0 ;

   collectionSpaceLen = ossStrlen ( collectionFullName ) ;
   collectionLen      = ossStrlen ( pDot ) ;
   if ( collectionSpaceLen <= CLIENT_CS_NAMESZ &&
        collectionLen <= CLIENT_COLLECTION_NAMESZ )
   {
      ossMemcpy ( s->_CSName, collectionFullName, collectionSpaceLen ) ;
      ossMemcpy ( s->_collectionName, pDot, collectionLen ) ;
      ossMemcpy ( s->_collectionFullName, pCollectionFullName, fullLen ) ;
   }
   else
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

static INT32 _reallocBuffer ( CHAR **ppBuffer, INT32 *buffersize,
                              INT32 newSize )
{
   INT32 rc              = SDB_OK ;
   CHAR *pOriginalBuffer = NULL ;

   if ( *buffersize < newSize )
   {
      pOriginalBuffer = *ppBuffer ;
      *ppBuffer = (CHAR*)SDB_OSS_REALLOC ( *ppBuffer, sizeof(CHAR)*newSize );
      if ( !*ppBuffer )
      {
         *ppBuffer = pOriginalBuffer ;
         rc = SDB_OOM ;
         goto error ;
      }
      *buffersize = newSize ;
   }
done :
   return rc ;
error :
   goto done ;
}

static INT32 _send1 ( sdbConnectionHandle cHandle, SOCKET sock,
                      const CHAR *pMsg, INT32 len )
{
   INT32 rc = SDB_OK ;

   if ( -1 == sock )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientSend ( sock, pMsg, len, SDB_CLIENT_DFT_NETWORK_TIMEOUT ) ;
   if ( SDB_OK != rc )
   {
      if ( SDB_NETWORK == rc || SDB_NETWORK_CLOSE == rc )
      {
         _sdbDisconnect_inner( cHandle ) ;
      }
      goto error ;
   }
done:
   return rc ;
error :
   goto done ;
}

static INT32 _send ( sdbConnectionHandle cHandle, SOCKET sock,
                     const MsgHeader *msg, BOOLEAN endianConvert )
{
   INT32 rc  = SDB_OK ;
   INT32 len = 0 ;

   ossEndianConvertIf4 ( msg->messageLength, len, endianConvert ) ;
   rc = _send1 ( cHandle, sock, (const CHAR*)msg, len ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error :
   goto done ;
}

static INT32 _recv ( sdbConnectionHandle cHandle, SOCKET sock,
                     MsgHeader **msg, INT32 *size,
                     BOOLEAN endianConvert )
{
   INT32 rc        = SDB_OK ;
   INT32 len       = 0 ;
   INT32 realLen   = 0 ;
   CHAR **ppBuffer = (CHAR**)msg ;

   if ( -1 == sock )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   while ( TRUE )
   {
      rc = clientRecv ( sock, (CHAR*)&len, sizeof(len),
                        SDB_CLIENT_DFT_NETWORK_TIMEOUT ) ;
      if ( SDB_TIMEOUT == rc )
         continue ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

#if defined( _LINUX )
      {
         INT32 i = 0 ;
         setsockopt( sock, IPPROTO_TCP, TCP_QUICKACK, (void*)&i, sizeof(i) ) ;
      }
#endif // _LINUX
      break ;
   }
   ossEndianConvertIf4 ( len, realLen, endianConvert ) ;
   rc = _reallocBuffer ( ppBuffer, size, realLen+1 ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *(SINT32*)(*ppBuffer) = len ;
   while ( TRUE )
   {
      rc = clientRecv ( sock, &(*ppBuffer)[sizeof(realLen)],
                        realLen - sizeof(realLen),
                        SDB_CLIENT_DFT_NETWORK_TIMEOUT ) ;
      if ( SDB_TIMEOUT == rc )
         continue ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      break ;
   }
done :
   return rc ;
error :
   if ( SDB_NETWORK_CLOSE == rc || SDB_NETWORK == rc )
   {
      _sdbDisconnect_inner( cHandle ) ;
   }
   goto done ;
}

static INT32 _recvExtract ( sdbConnectionHandle cHandle, SOCKET sock,
                            MsgHeader **msg, INT32 *size,
                            SINT64 *contextID, BOOLEAN *result,
                            BOOLEAN endianConvert )
{
   INT32 rc          = SDB_OK ;
   INT32 replyFlag   = -1 ;
   INT32 numReturned = -1 ;
   INT32 startFrom   = -1 ;
   CHAR **ppBuffer   = (CHAR**)msg ;

   rc = _recv ( cHandle, sock, msg, size, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = clientExtractReply ( *ppBuffer, &replyFlag, contextID,
                             &startFrom, &numReturned, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( SDB_OK != replyFlag )
   {
      *result = FALSE ;
      rc = replyFlag ;
   }
   else
   {
      *result = TRUE ;
   }

done :
   return rc ;
error :
   goto done ;
}

static INT32 _recvExtractEval ( sdbConnectionHandle cHandle, SOCKET sock,
                                MsgHeader **msg, INT32 *size,
                                SINT64 *contextID, SDB_SPD_RES_TYPE *type,
                                BOOLEAN *result, bson *errmsg,
                                BOOLEAN endianConvert )
{
   INT32 rc          = SDB_OK ;
   INT32 replyFlag   = -1 ;
   INT32 startFrom   = -1 ;
   CHAR **ppBuffer   = (CHAR**)msg ;
   MsgOpReply *replyHeader = NULL ;

   rc = _recv ( cHandle, sock, msg, size, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = clientExtractReply ( *ppBuffer, &replyFlag, contextID,
                             &startFrom, (SINT32 *)type, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( SDB_OK != replyFlag )
   {
      *result = FALSE ;
      rc = replyFlag ;
      replyHeader = (MsgOpReply *)(*ppBuffer) ;
      if ( errmsg && sizeof( MsgOpReply ) != replyHeader->header.messageLength )
      {
         bson_init_finished_data( errmsg, *ppBuffer + sizeof(MsgOpReply) ) ;
      }
   }
   else
   {
      *result = TRUE ;
   }
done :
   return rc ;
error :
   goto done ;
}

static INT32 _readNextBuffer ( sdbCursorHandle cursor )
{
   INT32 rc          = SDB_OK ;
   BOOLEAN lresult   = FALSE ;
   SINT64 lcontextID = 0 ;
   sdbCursorStruct *pCursor = ( sdbCursorStruct* )cursor ;

   HANDLE_CHECK( cursor, pCursor, SDB_HANDLE_TYPE_CURSOR ) ;

   if ( -1 == pCursor->_contextID )
   {
      rc = SDB_DMS_EOC ;
      goto error ;
   }

   rc = clientBuildGetMoreMsg ( &pCursor->_pSendBuffer,
                                &pCursor->_sendBufferSize, -1,
                                pCursor->_contextID,
                                0, pCursor->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( pCursor->_connection, pCursor->_sock,
                (MsgHeader*)pCursor->_pSendBuffer,
                pCursor->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( pCursor->_connection, pCursor->_sock,
                       (MsgHeader**)&pCursor->_pReceiveBuffer,
                       &pCursor->_receiveBufferSize, &lcontextID,
                       &lresult, pCursor->_endianConvert ) ;
   if ( SDB_OK != rc || lcontextID != pCursor->_contextID )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( pCursor->_pSendBuffer, pCursor->_pReceiveBuffer,
                        pCursor->_connection ) ;
done :
   return rc ;
error :
   if ( SDB_DMS_EOC != rc )
   {
      sdbCloseCursor( cursor ) ;
   }
   goto done ;
}


static INT32 _runCommand ( sdbConnectionHandle cHandle, SOCKET sock,
                           CHAR **ppSendBuffer, INT32 *sendBufferSize,
                           CHAR **ppReceiveBuffer, INT32 *receiveBufferSize,
                           BOOLEAN endianConvert, const CHAR *pString,
                           BOOLEAN *result, bson *arg1,
                           bson *arg2, bson *arg3, bson *arg4 )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;

   rc = clientBuildQueryMsg ( ppSendBuffer,
                              sendBufferSize,
                              pString, 0, 0, -1, -1,
                              arg1, arg2, arg3, arg4, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( cHandle, sock, (MsgHeader*)(*ppSendBuffer), endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cHandle, sock,
                       (MsgHeader**)ppReceiveBuffer,
                       receiveBufferSize,
                       &contextID, result, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( *ppSendBuffer, *ppReceiveBuffer, cHandle ) ;

done :
   return rc ;
error :
   goto done ;
}

static INT32 requestSysInfo ( sdbConnectionStruct *connection )
{
   INT32 rc               = SDB_OK ;
   MsgSysInfoReply reply ;

   connection->_endianConvert = FALSE;
   rc = clientBuildSysInfoRequest ( (CHAR**)&connection->_pSendBuffer,
                                    &connection->_sendBufferSize ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send1 ( (sdbConnectionHandle)connection, connection->_sock,
                 connection->_pSendBuffer, sizeof( MsgSysInfoRequest ) ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   while ( TRUE )
   {
      rc = clientRecv ( connection->_sock, (CHAR*)&reply,
                        sizeof(MsgSysInfoReply),
                        SDB_CLIENT_DFT_NETWORK_TIMEOUT ) ;
      if ( SDB_TIMEOUT == rc )
         continue ;
      if ( SDB_OK != rc )
      {
         _sdbDisconnect_inner( (sdbConnectionHandle)connection ) ;
         goto error ;
      }
      break ;
   }

   rc = clientExtractSysInfoReply ( (CHAR*)&reply,
                                    &(connection->_endianConvert),
                                    NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

static INT32 _addHandle ( Node **ptr, ossValuePtr handle )
{
   INT32 rc = SDB_OK ;
   Node *p  = NULL ;

   p = (Node*)SDB_OSS_MALLOC( sizeof(Node) ) ;
   if ( !p )
   {
      rc = SDB_OOM ;
      goto error ;
   }
   ossMemset ( p, 0, sizeof(Node) ) ;
   p->data = handle ;
   p->next = NULL ;

   if ( !(*ptr) )
      *ptr = p ;
   else
   {
      p->next = *ptr ;
      *ptr = p ;
   }
done :
   return rc ;
error :
   goto done ;
}

static INT32 _removeHandle ( Node **ptr, ossValuePtr handle,
                             Node **ptrRemoved )
{
   Node *pcurrent  = NULL ;
   Node *pprevious = NULL ;

   pcurrent = *ptr ;
   while( pcurrent )
   {
      if ( handle == pcurrent->data )
      {
         break;
      }

      pprevious = pcurrent ;
      pcurrent = pcurrent->next ;
   }

   if ( !pcurrent )
   {
      goto done ;
   }
   if ( !pprevious )
   {
      *ptr = pcurrent->next ;
   }
   else
   {
      pprevious->next = pcurrent->next ;
   }

   if ( !ptrRemoved )
   {
      SDB_OSS_FREE ( pcurrent ) ;
   }
   else
   {
      *ptrRemoved = pcurrent ;
   }

done :
   return SDB_OK ;
}

static INT32 _regSocket( ossValuePtr cHandle, SOCKET *pSock )
{
   INT32 rc                        = SDB_OK ;
   sdbConnectionStruct *connection = (sdbConnectionStruct *)cHandle ;

   if ( -1 == *pSock )
   {
      goto done ;
   }

   rc = _addHandle ( &connection->_sockets, (ossValuePtr)pSock ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

done :
   return rc ;
error :
   goto done ;
}

static INT32 _unregSocket( ossValuePtr cHandle, SOCKET *pSock )
{
   INT32 rc                        = SDB_OK ;
   Node *ptrRemoved                = NULL ;
   sdbConnectionStruct *connection = (sdbConnectionStruct *)cHandle ;

   if ( -1 == *pSock )
   {
      goto done ;
   }

   rc = _removeHandle ( &connection->_sockets, (ossValuePtr)pSock,
                        &ptrRemoved ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( ptrRemoved )
   {
      *(SOCKET*)ptrRemoved->data = -1 ;
      SDB_OSS_FREE( ptrRemoved ) ;
   }

done :
   return rc ;
error :
   goto done ;
}

static INT32 _regCursor ( ossValuePtr cHandle, sdbCursorHandle cursorHandle )
{
   INT32 rc                        = SDB_OK ;
   sdbConnectionStruct *connection = (sdbConnectionStruct *)cHandle ;

   _regSocket( cHandle, &((sdbCursorStruct*)cursorHandle)->_sock ) ;

   rc = _addHandle ( &connection->_cursors, cursorHandle ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

done :
   return rc ;
error :
   goto done ;
}

static INT32 _unregCursor ( ossValuePtr cHandle, sdbCursorHandle cursorHandle )
{
   INT32 rc                        = SDB_OK ;
   sdbConnectionStruct *connection = (sdbConnectionStruct *)cHandle ;

   _unregSocket( cHandle, &((sdbCursorStruct*)cursorHandle)->_sock ) ;

   rc = _removeHandle ( &connection->_cursors, cursorHandle, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

done :
   return rc ;
error :
   goto done ;
}

#define TRACE_FIELD_SEP ','
static INT32 sdbTraceStrtok ( bson *obj, CHAR *pLine )
{
   INT32 rc     = SDB_OK ;
   INT32 len    = 0 ;
   CHAR *pStart = pLine ;
   CHAR *pStop  = pLine ;
   if ( !pLine )
   {
      goto done ;
   }
   len = ossStrlen ( pLine ) ;
   while ( pStart - pLine <= len &&
           pStop  - pLine <= len )
   {
      if ( ( pStart == pStop ) &&
             isspace( *pStop ) )
         ++pStart ;
      else if ( TRACE_FIELD_SEP == *pStop ||
                !*pStop )
      {
         *pStop = '\0' ;
         if ( pStart != pStop )
         {
            CHAR *pTmp = pStop - 1 ;
            while ( ( pTmp > pStart ) &&
                    isspace ( *pTmp ) )
            {
               *pTmp = '\0' ;
               --pTmp ;
            }
            BSON_APPEND( *obj, "", pStart, string ) ;
         }
         pStart = pStop + 1 ;
      }
      ++pStop ;
   }
done :
   return rc ;
error :
   goto done ;
}

static INT32 __sdbUpdate ( sdbCollectionHandle cHandle,
                           SINT32 flag,
                           bson *rule,
                           bson *condition,
                           bson *hint )
{
   INT32 rc                = SDB_OK ;
   SINT64 contextID        = -1 ;
   BOOLEAN result          = FALSE ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;

   rc = clientBuildUpdateMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                                cs->_collectionFullName, flag, 0, condition,
                                rule, hint, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   return rc ;
error :
   goto done ;
}

static INT32 _sdbStartStopNode ( sdbNodeHandle cHandle,
                                 BOOLEAN start )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN result   = FALSE ;
   bson configuration ;
   BOOLEAN bsoninit = FALSE ;
   sdbRNStruct *r   = (sdbRNStruct*)cHandle ;

   BSON_INIT( configuration ) ;
   BSON_APPEND( configuration, CAT_HOST_FIELD_NAME,
                r->_hostName, string ) ;
   BSON_APPEND( configuration, PMD_OPTION_SVCNAME, r->_serviceName, string ) ;
   BSON_FINISH( configuration ) ;
   rc = _runCommand ( r->_connection, r->_sock, &r->_pSendBuffer,
                      &r->_sendBufferSize,
                      &r->_pReceiveBuffer,
                      &r->_receiveBufferSize,
                      r->_endianConvert,
                      start?
                         (CMD_ADMIN_PREFIX CMD_NAME_STARTUP_NODE) :
                         (CMD_ADMIN_PREFIX CMD_NAME_SHUTDOWN_NODE),
                      &result, &configuration,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( configuration ) ;
   return rc ;
error :
   goto done ;
}

static INT32 _sdbShardExtractNode ( sdbReplicaGroupHandle cHandle,
                                    sdbNodeHandle *handle,
                                    const CHAR *data,
                                    BOOLEAN endianConvert )
{
   INT32 rc       = SDB_OK ;
   sdbRNStruct *r = NULL ;
   sdbRGStruct *s = (sdbRGStruct *)cHandle ;

   ALLOC_HANDLE( r, sdbRNStruct ) ;
   r->_handleType = SDB_HANDLE_TYPE_REPLICANODE ;
   r->_connection = s->_connection ;
   r->_sock = s->_sock ;
   r->_endianConvert = endianConvert ;

   _regSocket( s->_connection, &r->_sock ) ;

   rc = clientReplicaGroupExtractNode ( data,
                                        r->_hostName,
                                        CLIENT_MAX_HOSTNAME,
                                        r->_serviceName,
                                        CLIENT_MAX_SERVICENAME,
                                        &r->_nodeID ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   ossStrncpy ( r->_nodeName, r->_hostName, CLIENT_MAX_HOSTNAME ) ;
   ossStrncat ( r->_nodeName, NODE_NAME_SERVICE_SEP, 1 ) ;
   ossStrncat ( r->_nodeName, r->_serviceName,
                CLIENT_MAX_SERVICENAME ) ;

   *handle = (sdbNodeHandle)r ;
done :
   return rc ;
error :
   if ( r )
   {
      SDB_OSS_FREE ( r ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}
static INT32 _sdbGetList ( sdbConnectionHandle cHandle,
                           INT32 listType,
                           bson *condition, bson *selector, bson *orderBy,
                           sdbCursorHandle *handle )
{
   INT32 rc                        = SDB_OK ;
   sdbCursorStruct *cursor         = NULL ;
   SINT64 contextID                = -1 ;
   BOOLEAN result                  = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct *)cHandle ;

   static char *pcmd[] = {
      CMD_ADMIN_PREFIX CMD_NAME_LIST_CONTEXTS         ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_CONTEXTS_CURRENT ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_SESSIONS         ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_SESSIONS_CURRENT ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_COLLECTIONS      ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_COLLECTIONSPACES ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_STORAGEUNITS ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_GROUPS       ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_PROCEDURES   ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_DOMAINS      ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_TASKS        ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_CS_IN_DOMAIN ,
      CMD_ADMIN_PREFIX CMD_NAME_LIST_CL_IN_DOMAIN ,

   } ;

   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                              &connection->_sendBufferSize,
                              pcmd[listType], 0, 0, 0, -1, condition, selector, orderBy,
                              NULL, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                 connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
		        cHandle ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR ( cursor, connection, connection, contextID ) ;

   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   *handle = (sdbCursorHandle)cursor ;
done :
   return rc ;
error :
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

static INT32 _sdbGetReplicaGroupDetail ( sdbReplicaGroupHandle cHandle,
                                         bson *result )
{
   INT32 rc               = SDB_OK ;
   sdbCursorHandle cursor = SDB_INVALID_HANDLE ;
   CHAR *pName            = FIELD_NAME_GROUPNAME ;
   sdbRGStruct *r         = (sdbRGStruct*)cHandle ;
   BOOLEAN bsoninit       = FALSE ;
   bson newObj ;

   BSON_INIT( newObj );
   BSON_APPEND( newObj, pName, r->_replicaGroupName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _sdbGetList ( r->_connection,
                      SDB_LIST_GROUPS, &newObj, NULL, NULL, &cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = sdbNext ( cursor, result ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   if ( SDB_INVALID_HANDLE != cursor )
   {
      sdbReleaseCursor ( cursor ) ;
   }

   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

static INT32 _sdbGetReplicaGroup( sdbConnectionHandle cHandle,
                                  bson condition,
                                  sdbReplicaGroupHandle *handle )
{
   INT32 rc                 = SDB_OK;
   sdbCursorHandle cursor   = SDB_INVALID_HANDLE ;
   sdbRGStruct *r           = NULL ;
   bson result ;
   BOOLEAN bsoninit         = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   rc = sdbGetList ( cHandle, SDB_LIST_GROUPS, &condition, NULL, NULL,
                     &cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   BSON_INIT( result ) ;
   if ( SDB_OK == ( rc = sdbNext ( cursor, &result ) ) )
   {
      bson_iterator it ;
      const CHAR *pGroupName = NULL ;
      if ( BSON_STRING != bson_find ( &it, &result, CAT_GROUPNAME_NAME ) )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      pGroupName = bson_iterator_string ( &it ) ;

      ALLOC_HANDLE( r, sdbRGStruct ) ;
      r->_handleType    = SDB_HANDLE_TYPE_REPLICAGROUP ;
      r->_connection    = cHandle ;
      r->_sock          = connection->_sock ;
      r->_endianConvert = connection->_endianConvert ;
      rc = _setRGName ( (sdbReplicaGroupHandle)r, pGroupName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      _regSocket( cHandle, &r->_sock ) ;

      if ( !ossStrcmp ( pGroupName, CATALOG_GROUPNAME ) )
      {
         r->_isCatalog = TRUE ;
      }
      *handle = (sdbReplicaGroupHandle)r ;
   }
   else if ( SDB_DMS_EOC != rc )
   {
      goto error ;
   }
   else
   {
      rc = SDB_CLS_GRP_NOT_EXIST ;
      goto error ;
   }

done :
   if ( SDB_INVALID_HANDLE != cursor )
   {
      sdbReleaseCursor ( cursor ) ;
   }

   BSON_DESTROY( result ) ;
   return rc;
error:
   if ( r )
   {
      SDB_OSS_FREE ( r ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 __sdbGetReserveSpace1 ( sdbConnectionHandle cHandle,
                                         UINT64 *space )
{
   INT32 rc                        = SDB_OK ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !space )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   *space = connection->reserveSpace1 ;
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 __sdbSetReserveSpace1 ( sdbConnectionHandle cHandle,
                                         UINT64 space )
{
   INT32 rc                        = SDB_OK ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   connection->reserveSpace1 = space ;
done :
   return rc ;
error :
   goto done ;
}

#define ENCRYTED_STR_LEN   ( SDB_MD5_DIGEST_LENGTH * 2 + 1 )
SDB_EXPORT INT32 sdbConnect ( const CHAR *pHostName, const CHAR *pServiceName,
                              const CHAR *pUsrName, const CHAR *pPasswd ,
                              sdbConnectionHandle *handle )
{
   INT32 rc                            = SDB_OK ;
   CHAR md5[ENCRYTED_STR_LEN + 1]      = {0} ;
   BOOLEAN r                           = FALSE ;
   SINT64 contextID                    = 0 ;
   sdbConnectionStruct *connection     = NULL ;

   if ( !pHostName || !pServiceName || !pUsrName || !pPasswd || !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   ALLOC_HANDLE( connection, sdbConnectionStruct ) ;
   connection->_handleType = SDB_HANDLE_TYPE_CONNECTION ;
   rc = clientConnect ( pHostName, pServiceName, &connection->_sock ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = requestSysInfo ( connection ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( FALSE == g_disablePassEncode )
   {
      rc = md5Encrypt( pPasswd, md5, ENCRYTED_STR_LEN ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   }
   else
   {
      ossStrncpy( md5, pPasswd, ENCRYTED_STR_LEN ) ;
   }

   rc = clientBuildAuthMsg( &connection->_pSendBuffer,
                            &connection->_sendBufferSize,
                            pUsrName, md5, 0, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( (sdbConnectionHandle)connection, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( (sdbConnectionHandle)connection, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize,
                       &contextID, &r,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        (sdbConnectionHandle)connection ) ;
   *handle = (sdbConnectionHandle)connection ;
done:
   return rc ;
error:
   if ( connection )
   {
      sdbDisconnect( (sdbConnectionHandle)connection );
      sdbReleaseCollection( (sdbConnectionHandle)connection );
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbConnect1 ( const CHAR **pConnAddrs, INT32 arrSize,
                               const CHAR *pUsrName, const CHAR *pPasswd ,
                               sdbConnectionHandle *handle )
{
   INT32 rc                 = SDB_OK ;
   const CHAR *pHostName    = NULL ;
   const CHAR *pServiceName = NULL ;
   const CHAR *addr         = NULL ;
   CHAR *pStr               = NULL ;
   CHAR *pTmp               = NULL ;
   INT32 mark               = 0 ;
   INT32 i                  = 0 ;

   if ( !pConnAddrs || arrSize <= 0 || !pUsrName || !pPasswd || !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   srand ( (UINT32)time(NULL) ) ;
   i = rand() % arrSize ;
   mark = i ;
   do
   {
      addr = pConnAddrs[i] ;
      i++ ;
      i = i % arrSize ;
      pTmp = ossStrchr ( addr, ':' ) ;
      if ( !pTmp )
      {
         continue ;
      }

      pStr = ossStrdup ( addr ) ;
      if ( !pStr )
      {
         rc = SDB_OOM ;
         goto error ;
      }

      pStr[pTmp - addr] = 0 ;
      pHostName = pStr ;
      pServiceName = &(pStr[pTmp - addr]) + 1;
      rc = sdbConnect ( pHostName, pServiceName, pUsrName, pPasswd, handle ) ;
      SDB_OSS_FREE ( pStr ) ;
      pStr = NULL ;
      pTmp = NULL ;
      if ( SDB_OK == rc )
         goto done ;
   } while ( mark != i ) ;
   rc = SDB_NET_CANNOT_CONNECT ;
done:
   return rc ;
error:
   if ( handle )
   {
      *handle = SDB_INVALID_HANDLE ;
   }
   goto done;
}

void _sdbDisconnect_inner ( sdbConnectionHandle handle )
{
   INT32 rc = SDB_OK ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)handle ;
   Node *cursors  = NULL ;
   Node *sockets  = NULL ;

   CLIENT_UNUSED( rc ) ;

   HANDLE_CHECK( handle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( -1 == connection->_sock )
   {
      return ;
   }

   clientDisconnect ( connection->_sock ) ;
   connection->_sock = -1 ;

   sockets = connection->_sockets ;
   while ( sockets )
   {
      *((SOCKET*)sockets->data) = -1 ;
      connection->_sockets = sockets->next ;
      SDB_OSS_FREE( sockets ) ;
      sockets = connection->_sockets ;
   }

   cursors = connection->_cursors ;
   while ( cursors )
   {
      ((sdbCursorStruct*)cursors->data)->_isClosed = TRUE ;
      ((sdbCursorStruct*)cursors->data)->_contextID = -1 ;
      connection->_cursors = cursors->next ;
      SDB_OSS_FREE( cursors ) ;
      cursors = connection->_cursors ;
   }

done :
   return ;
error :
   goto done ;
}

SDB_EXPORT void sdbDisconnect ( sdbConnectionHandle handle )
{
   INT32 rc                        = SDB_OK ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)handle ;

   CLIENT_UNUSED( rc ) ;

   HANDLE_CHECK( handle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( -1 == connection->_sock )
   {
      return ;
   }

   if ( !clientBuildDisconnectMsg ( &connection->_pSendBuffer,
                                    &connection->_sendBufferSize,
                                    0, connection->_endianConvert ))
   {
      _send ( handle, connection->_sock, (MsgHeader*)connection->_pSendBuffer,
              connection->_endianConvert ) ;
   }

   _sdbDisconnect_inner( handle ) ;

done:
   return ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbGetDataBlocks ( sdbCollectionHandle cHandle,
                                    bson *condition,
                                    bson *select,
                                    bson *orderBy,
                                    bson *hint,
                                    INT64 numToSkip,
                                    INT64 numToReturn,
                                    sdbCursorHandle *handle )
{
   INT32 rc                        = SDB_OK ;
   CHAR *p                         = CMD_ADMIN_PREFIX CMD_NAME_GET_DATABLOCKS ;
   sdbCursorStruct *cursor         = NULL ;
   SINT64 contextID                = 0 ;
   BOOLEAN result                  = FALSE ;
   sdbCollectionStruct *cs         = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !cs->_collectionFullName[0] || !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              p, 0, 0,
                              numToSkip, numToReturn, condition,
                              select, orderBy, hint, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( cs->_connection, cs->_sock, ( MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer, cHandle ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR ( cursor, cs->_connection, cs, contextID ) ;

   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   *handle = (sdbCursorHandle)cursor ;
done:
   return rc ;
error:
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetQueryMeta ( sdbCollectionHandle cHandle,
                                   bson *condition,
                                   bson *orderBy,
                                   bson *hint,
                                   INT64 numToSkip,
                                   INT64 numToReturn,
                                   sdbCursorHandle *handle )
{
   INT32 rc                        = SDB_OK ;
   CHAR *p                         = CMD_ADMIN_PREFIX CMD_NAME_GET_QUERYMETA ;
   sdbCursorStruct *cursor         = NULL ;
   SINT64 contextID                = 0 ;
   BOOLEAN result                  = FALSE ;
   sdbCollectionStruct *cs         = (sdbCollectionStruct*)cHandle ;
   BOOLEAN bsoninit                = FALSE ;
   bson hint1 ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !cs->_collectionFullName[0] || !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( hint1 ) ;
   BSON_APPEND( hint1, FIELD_NAME_COLLECTION,
                cs->_collectionFullName, string ) ;
   BSON_FINISH ( hint1 ) ;

   rc = clientBuildQueryMsg ( &cs->_pSendBuffer,
                              &cs->_sendBufferSize,
                              p, 0, 0, numToSkip, numToReturn, condition,
                              hint, orderBy, &hint1,
                              cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, ( MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer, cHandle ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, cs->_connection, cs, contextID ) ;

   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *handle = (sdbCursorHandle)cursor ;
done:
   BSON_DESTROY( hint1 ) ;
   return rc ;
error:
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetSnapshot ( sdbConnectionHandle cHandle,
                                  INT32 snapType,
                                  bson *condition,
                                  bson *selector,
                                  bson *orderBy,
                                  sdbCursorHandle *handle )
{
   INT32 rc                        = SDB_OK ;
   sdbCursorStruct *cursor         = NULL ;
   SINT64 contextID                = -1 ;
   BOOLEAN result                  = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   static char *pcmd[] = {
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CONTEXTS ,
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CONTEXTS_CURRENT ,
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_SESSIONS ,
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_SESSIONS_CURRENT ,
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_COLLECTIONS ,
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_COLLECTIONSPACES ,
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_DATABASE ,
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_SYSTEM ,
      CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CATA
   } ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( snapType >= SDB_SNAP_END || snapType < SDB_SNAP_CONTEXTS )
   {
      rc = SDB_INVALIDARG ;
       goto error ;
   }

   if ( !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                              &connection->_sendBufferSize,
                              pcmd[snapType], 0, 0, 0, -1,
                              condition, selector, orderBy,
                              NULL, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, connection, connection, contextID ) ;
   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *handle = (sdbCursorHandle)cursor ;
done :
   return rc ;
error :
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbCreateUsr( sdbConnectionHandle cHandle,
                              const CHAR *pUsrName,
                               const CHAR *pPasswd )
{
   INT32 rc                   = SDB_OK ;
   CHAR md5[ENCRYTED_STR_LEN] = {0};
   BOOLEAN r                  = FALSE ;
   SINT64 contextID           = 0 ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION );
   if ( !pUsrName || !pPasswd )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = md5Encrypt( pPasswd, md5, ENCRYTED_STR_LEN) ;
   if ( rc )
   {
      goto error ;
   }
   rc = clientBuildAuthCrtMsg( &connection->_pSendBuffer,
                               &connection->_sendBufferSize,
                               pUsrName, md5, 0, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                      (MsgHeader**)&connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize, &contextID, &r,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done:
  return rc ;
error:
  goto done ;
}

SDB_EXPORT INT32 sdbRemoveUsr( sdbConnectionHandle cHandle,
                               const CHAR *pUsrName,
                               const CHAR *pPasswd )
{
   INT32 rc                        = SDB_OK ;
   CHAR md5[ENCRYTED_STR_LEN]      = {0};
   BOOLEAN r                       = FALSE;
   SINT64 contextID                = 0 ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pUsrName || !pPasswd )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = md5Encrypt( pPasswd, md5, ENCRYTED_STR_LEN ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = clientBuildAuthDelMsg( &connection->_pSendBuffer,
                               &connection->_sendBufferSize,
                               pUsrName, md5, 0, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                      (MsgHeader**)&connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize, &contextID, &r,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done:
  return rc ;
error:
  goto done ;
}

SDB_EXPORT INT32 sdbResetSnapshot ( sdbConnectionHandle cHandle,
                                    bson *condition )
{
   INT32 rc                        = SDB_OK ;
   BOOLEAN result                  = FALSE ;
   CHAR *p                         = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_RESET ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      p, &result, condition,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

/*
static INT32 _sdbGetList ( SOCKET _sock, CHAR **_pSendBuffer,
                           INT32 *_sendBufferSize, CHAR **_pReceiveBuffer,
                           INT32 *_receiveBufferSize, BOOLEAN _endianConvert,
                           INT32 listType,
                           bson *condition, bson *selector, bson *orderBy,
                           sdbCursorHandle *handle )
*/

SDB_EXPORT INT32 sdbGetList ( sdbConnectionHandle cHandle,
                              INT32 listType,
                              bson *condition,
                              bson *selector,
                              bson *orderBy,
                              sdbCursorHandle *handle )
{
   INT32 rc                        = SDB_OK ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( listType >= SDB_LIST_END|| listType < SDB_LIST_CONTEXTS )
   {
      rc = SDB_INVALIDARG ;
     goto error ;
   }
   if ( !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = _sdbGetList ( cHandle,
                      listType,
                      condition, selector, orderBy, handle ) ;
   if ( SDB_OK != rc )
   {
      goto done ;
   }
done :
   return rc ;
error :
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetCollection ( sdbConnectionHandle cHandle,
                                    const CHAR *pCollectionFullName,
                                    sdbCollectionHandle *handle )
{
   INT32 rc               = SDB_OK ;
   BOOLEAN result         = FALSE ;
   CHAR *pTestCollection  = CMD_ADMIN_PREFIX CMD_NAME_TEST_COLLECTION ;
   CHAR *pName            = FIELD_NAME_NAME ;
   sdbCollectionStruct *s = NULL ;
   bson newObj ;
   BOOLEAN bsoninit       = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pCollectionFullName || !handle ||
        ossStrlen ( pCollectionFullName) > CL_FULLNAME_LEN )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj );
   BSON_APPEND( newObj, pName, pCollectionFullName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pTestCollection, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   ALLOC_HANDLE( s, sdbCollectionStruct ) ;
   s->_handleType    = SDB_HANDLE_TYPE_COLLECTION ;
   s->_connection    = cHandle ;
   s->_sock          = connection->_sock ;
   s->_endianConvert = connection->_endianConvert ;
   rc = _setCollectionName ( (sdbCollectionHandle)s, pCollectionFullName ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   _regSocket( cHandle, &s->_sock ) ;

   *handle = (sdbCollectionHandle)s ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   if ( s )
   {
      SDB_OSS_FREE ( s ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetCollectionSpace ( sdbConnectionHandle cHandle,
                                         const CHAR *pCollectionSpaceName,
                                         sdbCSHandle *handle )
{
   INT32 rc               = SDB_OK ;
   BOOLEAN result         = FALSE ;
   CHAR *pTestCollection  = CMD_ADMIN_PREFIX CMD_NAME_TEST_COLLECTIONSPACE ;
   CHAR *pName            = FIELD_NAME_NAME ;
   sdbCSStruct *s         = NULL ;
   BOOLEAN bsoninit       = FALSE ;
   bson newObj ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pCollectionSpaceName || !handle ||
        ossStrlen ( pCollectionSpaceName) > CLIENT_CS_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj );
   BSON_APPEND( newObj, pName, pCollectionSpaceName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pTestCollection, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   ALLOC_HANDLE( s, sdbCSStruct ) ;
   s->_handleType    = SDB_HANDLE_TYPE_CS ;
   s->_connection    = cHandle ;
   s->_sock          = connection->_sock ;
   s->_endianConvert = connection->_endianConvert ;
   rc = _setCSName ( (sdbCSHandle)s, pCollectionSpaceName ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   _regSocket( cHandle, &s->_sock ) ;

   *handle = (sdbCSHandle)s ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   if ( s )
   {
      SDB_OSS_FREE ( s ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetReplicaGroup ( sdbConnectionHandle cHandle,
                                      const CHAR *pGroupName,
                                      sdbReplicaGroupHandle *handle )
{
   INT32 rc                 = SDB_OK ;
   CHAR *pName              = CAT_GROUPNAME_NAME ;
   bson newObj ;
   BOOLEAN bsoninit         = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pGroupName || !handle ||
        ossStrlen ( pGroupName ) > CLIENT_RG_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj );
   BSON_APPEND( newObj, pName, pGroupName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _sdbGetReplicaGroup( cHandle, newObj, handle ) ;
   if (SDB_OK != rc )
   {
      goto error;
   }

done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetReplicaGroup1 ( sdbConnectionHandle cHandle,
                                       UINT32 id,
                                       sdbReplicaGroupHandle *handle )
{
   INT32 rc                 = SDB_OK ;
   CHAR *pName              = CAT_GROUPID_NAME ;
   bson newObj ;
   BOOLEAN bsoninit         = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj );
   BSON_APPEND( newObj, pName, (INT32)id, int ) ;
   BSON_FINISH ( newObj ) ;

   rc = _sdbGetReplicaGroup( cHandle, newObj, handle ) ;
   if (SDB_OK != rc )
   {
      goto error;
   }

done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetReplicaGroupName ( sdbReplicaGroupHandle cHandle,
                                          CHAR **ppShardName )
{
   INT32 rc                 = SDB_OK ;
   sdbRGStruct *r           = (sdbRGStruct*)cHandle ;

   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;

   if ( ppShardName )
   {
      *ppShardName = r->_replicaGroupName ;
   }
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT BOOLEAN sdbIsReplicaGroupCatalog ( sdbReplicaGroupHandle cHandle )
{
   INT32 rc = SDB_OK ;
   sdbRGStruct *r    = (sdbRGStruct*)cHandle ;
   BOOLEAN isCatalog = FALSE ;

   CLIENT_UNUSED( rc ) ;

   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;

   isCatalog = r->_isCatalog ;
done :
   return isCatalog ;
error :
   goto done;
}

SDB_EXPORT INT32 sdbCreateReplicaCataGroup ( sdbConnectionHandle cHandle,
                                             const CHAR *pHostName,
                                             const CHAR *pServiceName,
                                             const CHAR *pDatabasePath,
                                             bson *configure )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN result   = FALSE ;
   CHAR *pCataShard = CMD_ADMIN_PREFIX CMD_NAME_CREATE_CATA_GROUP ;
   BOOLEAN bsoninit = FALSE ;
   bson configuration ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pHostName || !pServiceName || !pDatabasePath )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( configuration ) ;
   BSON_APPEND( configuration, CAT_HOST_FIELD_NAME, pHostName, string ) ;
   BSON_APPEND( configuration, PMD_OPTION_SVCNAME, pServiceName, string ) ;
   BSON_APPEND( configuration, PMD_OPTION_DBPATH, pDatabasePath, string ) ;

   if ( configure )
   {
      bson_iterator it ;
      bson_iterator_init ( &it, configure ) ;
      while ( BSON_EOO != bson_iterator_next ( &it ) )
      {
         const CHAR *key = bson_iterator_key ( &it ) ;
         if ( !ossStrcmp ( key, PMD_OPTION_DBPATH )  ||
              !ossStrcmp ( key, PMD_OPTION_SVCNAME ) ||
              !ossStrcmp ( key, CAT_HOST_FIELD_NAME ) )
         {
            continue ;
         }

         bson_append_element( &configuration, NULL, &it ) ;
      } // while
   } // if ( configure )
   BSON_FINISH ( configuration ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pCataShard, &result, &configuration,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( configuration ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbCreateNode ( sdbReplicaGroupHandle cHandle,
                                 const CHAR *pHostName,
                                 const CHAR *pServiceName,
                                 const CHAR *pDatabasePath,
                                 bson *configure )
{
   INT32 rc = SDB_OK ;

   BOOLEAN result    = FALSE ;
   CHAR *pCreateNode = CMD_ADMIN_PREFIX CMD_NAME_CREATE_NODE ;
   sdbRGStruct *r    = (sdbRGStruct*)cHandle ;
   BOOLEAN bsoninit  = FALSE ;
   bson configuration ;

   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;
   if ( !pHostName || !pServiceName || !pDatabasePath )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( configuration ) ;
   BSON_APPEND( configuration,
                CAT_GROUPNAME_NAME, r->_replicaGroupName, string ) ;
   BSON_APPEND( configuration, CAT_HOST_FIELD_NAME, pHostName, string ) ;
   BSON_APPEND( configuration, PMD_OPTION_SVCNAME, pServiceName, string ) ;

   BSON_APPEND( configuration, PMD_OPTION_DBPATH, pDatabasePath, string ) ;
   if ( configure )
   {
      bson_iterator it ;
      bson_iterator_init ( &it, configure ) ;
      while ( BSON_EOO != bson_iterator_next ( &it ) )
      {
         const CHAR *key = bson_iterator_key ( &it ) ;
         if ( !ossStrcmp ( key, PMD_OPTION_DBPATH )   ||
              !ossStrcmp ( key, PMD_OPTION_SVCNAME )  ||
              !ossStrcmp ( key, CAT_HOST_FIELD_NAME ) ||
              !ossStrcmp ( key, CAT_GROUPNAME_NAME ) )
         {
            continue ;
         }

         bson_append_element( &configuration, NULL, &it ) ;
      } // while
   } // if ( configure )
   BSON_FINISH ( configuration ) ;

   rc = _runCommand ( r->_connection, r->_sock, &r->_pSendBuffer,
                      &r->_sendBufferSize,
                      &r->_pReceiveBuffer,
                      &r->_receiveBufferSize,
                      r->_endianConvert,
                      pCreateNode, &result, &configuration,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( configuration ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbRemoveNode ( sdbReplicaGroupHandle cHandle,
                                 const CHAR *pHostName,
                                 const CHAR *pServiceName,
                                 bson *configure )
{
   INT32 rc          = SDB_OK ;
   CHAR *pRemoveNode = CMD_ADMIN_PREFIX CMD_NAME_REMOVE_NODE ;
   sdbRGStruct *r    = (sdbRGStruct*)cHandle ;
   BOOLEAN result    = FALSE ;
   BOOLEAN bsoninit  = FALSE ;
   bson removeInfo ;

   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;
   if ( !pHostName || !pServiceName )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( removeInfo ) ;
   BSON_APPEND( removeInfo, CAT_GROUPNAME_NAME,
                r->_replicaGroupName, string ) ;

   BSON_APPEND( removeInfo, FIELD_NAME_HOST, pHostName, string ) ;

   BSON_APPEND( removeInfo, PMD_OPTION_SVCNAME, pServiceName, string ) ;
   if ( configure )
   {
      bson_iterator it ;
      bson_iterator_init ( &it, configure ) ;
      while ( BSON_EOO != bson_iterator_next ( &it ) )
      {
         const CHAR *key = bson_iterator_key ( &it ) ;
         if ( ossStrcmp ( key, FIELD_NAME_HOST ) == 0  ||
              ossStrcmp ( key, FIELD_NAME_SERVICE_NAME ) == 0 ||
              ossStrcmp ( key, CAT_GROUPNAME_NAME ) == 0 )
         {
            continue ;
         }
         else
         {
            BSON_APPEND( removeInfo, NULL, &it, element ) ;
         }
      }
   }

   BSON_FINISH( removeInfo ) ;

   rc = _runCommand ( r->_connection, r->_sock, &r->_pSendBuffer,
                      &r->_sendBufferSize,
                      &r->_pReceiveBuffer,
                      &r->_receiveBufferSize,
                      r->_endianConvert,
                      pRemoveNode, &result, &removeInfo,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   BSON_DESTROY( removeInfo ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbCreateCollectionSpaceV2 ( sdbConnectionHandle cHandle,
                                              const CHAR *pCollectionSpaceName,
                                              bson *options,
                                              sdbCSHandle *handle )
{
   INT32 rc                 = SDB_OK ;
   BOOLEAN result           = FALSE ;
   CHAR *pCreateCollection  = CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTIONSPACE ;
   sdbCSStruct *s           = NULL ;
   BOOLEAN bsoninit         = FALSE ;
   bson newObj ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pCollectionSpaceName || !handle ||
        ossStrlen ( pCollectionSpaceName) > CLIENT_CS_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, FIELD_NAME_NAME, pCollectionSpaceName, string ) ;

   if ( options )
   {
      bson_iterator itr ;
      bson_iterator_init( &itr, options ) ;
      while ( bson_iterator_more( &itr ) )
      {
         bson_iterator_next( &itr ) ;
         BSON_APPEND( newObj, NULL, &itr, element ) ;
      }
   }

   BSON_FINISH ( newObj ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pCreateCollection, &result, &newObj,
                      NULL, NULL, NULL ) ;

   if ( SDB_OK != rc )
   {
      goto error ;
   }

   ALLOC_HANDLE( s, sdbCSStruct ) ;
   s->_handleType    = SDB_HANDLE_TYPE_CS ;
   s->_connection    = cHandle ;
   s->_sock          = connection->_sock ;
   s->_endianConvert = connection->_endianConvert ;
   rc = _setCSName ( (sdbCSHandle)s, pCollectionSpaceName ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   _regSocket( cHandle, &s->_sock ) ;
   *handle = (sdbCSHandle)s ;

done:
   BSON_DESTROY( newObj ) ;
   return rc ;
error:
   if ( s )
   {
      SDB_OSS_FREE( s ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbCreateCollectionSpace ( sdbConnectionHandle cHandle,
                                            const CHAR *pCollectionSpaceName,
                                            INT32 iPageSize,
                                            sdbCSHandle *handle )
{
   INT32 rc               = SDB_OK ;
   BOOLEAN bsoninit       = FALSE ;
   bson options ;

   BSON_INIT( options );
   BSON_APPEND( options, FIELD_NAME_PAGE_SIZE, iPageSize, int ) ;
   BSON_FINISH( options ) ;
   rc = sdbCreateCollectionSpaceV2( cHandle, pCollectionSpaceName,
                                    &options, handle ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( options ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbDropCollectionSpace ( sdbConnectionHandle cHandle,
                                          const CHAR *pCollectionSpaceName )
{
   INT32 rc               = SDB_OK ;
   BOOLEAN result         = FALSE ;
   CHAR *pDropCollection  = CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTIONSPACE ;
   CHAR *pName            = FIELD_NAME_NAME ;
   BOOLEAN bsoninit       = FALSE ;
   bson newObj ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pCollectionSpaceName ||
        ossStrlen ( pCollectionSpaceName) > CLIENT_CS_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, pName, pCollectionSpaceName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pDropCollection, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbCreateReplicaGroup ( sdbConnectionHandle cHandle,
                                         const CHAR *pGroupName,
                                         sdbReplicaGroupHandle *handle )
{
   INT32 rc           = SDB_OK ;
   BOOLEAN result     = FALSE ;
   CHAR *pCreateShard = CMD_ADMIN_PREFIX CMD_NAME_CREATE_GROUP ;
   CHAR *pName        = FIELD_NAME_GROUPNAME ;
   sdbRGStruct *r     = NULL ;
   BOOLEAN bsoninit   = FALSE ;
   bson newObj ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pGroupName || !handle ||
        ossStrlen ( pGroupName ) > CLIENT_RG_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, pName, pGroupName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pCreateShard, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   ALLOC_HANDLE( r, sdbRGStruct ) ;
   r->_handleType    = SDB_HANDLE_TYPE_REPLICAGROUP ;
   r->_connection    = cHandle ;
   r->_sock          = connection->_sock ;
   r->_endianConvert = connection->_endianConvert ;
   rc = _setRGName ( (sdbReplicaGroupHandle)r, pGroupName ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *handle = (sdbReplicaGroupHandle)r ;
   _regSocket( cHandle, &r->_sock ) ;

done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   if ( r )
   {
      SDB_OSS_FREE ( r ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbRemoveReplicaGroup ( sdbConnectionHandle cHandle,
                                         const CHAR *pGroupName )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN result   = FALSE ;
   INT32 nameLength = 0 ;
   CHAR *pCommand   = CMD_ADMIN_PREFIX CMD_NAME_REMOVE_GROUP ;
   CHAR *pName      = FIELD_NAME_GROUPNAME ;
   BOOLEAN bsoninit = FALSE ;
   bson newObj ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pGroupName )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   nameLength = ossStrlen( pGroupName ) ;
   if ( 0 == nameLength || CLIENT_RG_NAMESZ < nameLength )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, pName, pGroupName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pCommand, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   BSON_DESTROY( newObj ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbStartReplicaGroup ( sdbReplicaGroupHandle cHandle )
{
   INT32 rc             = SDB_OK ;
   BOOLEAN result       = FALSE ;
   CHAR *pActivateShard = CMD_ADMIN_PREFIX CMD_NAME_ACTIVE_GROUP ;
   CHAR *pName          = FIELD_NAME_GROUPNAME ;
   sdbRGStruct *r       = (sdbRGStruct*)cHandle ;
   BOOLEAN bsoninit     = FALSE ;
   bson newObj ;

   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;
   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, pName, r->_replicaGroupName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _runCommand ( r->_connection, r->_sock, &r->_pSendBuffer,
                      &r->_sendBufferSize,
                      &r->_pReceiveBuffer,
                      &r->_receiveBufferSize,
                      r->_endianConvert,
                      pActivateShard, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbStopReplicaGroup ( sdbReplicaGroupHandle cHandle )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN result   = FALSE ;
   sdbRGStruct *r   = (sdbRGStruct*)cHandle ;
   BOOLEAN bsoninit = FALSE ;
   bson configuration ;
   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;

   BSON_INIT( configuration );
   BSON_APPEND( configuration,
                FIELD_NAME_GROUPNAME, r->_replicaGroupName, string ) ;
   BSON_FINISH ( configuration ) ;

   rc = _runCommand ( r->_connection, r->_sock, &r->_pSendBuffer,
                      &r->_sendBufferSize,
                      &r->_pReceiveBuffer,
                      &r->_receiveBufferSize,
                      r->_endianConvert,
                      CMD_ADMIN_PREFIX CMD_NAME_SHUTDOWN_GROUP,
                      &result, &configuration,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( configuration ) ;
   return rc ;
error :
   goto done ;
}

/*
static INT32 _sdbShardExtractNode ( SOCKET sock,
                                    sdbNodeHandle *handle,
                                    const CHAR *data,
                                    BOOLEAN endianConvert )
*/

SDB_EXPORT INT32 sdbGetNodeMaster ( sdbReplicaGroupHandle cHandle,
                                    sdbNodeHandle *handle )
{
   INT32 rc                = SDB_OK ;
   const CHAR *primaryData = NULL ;
   INT32 primaryNode       = -1 ;
   sdbRGStruct *r          = (sdbRGStruct*)cHandle ;
   BOOLEAN bsoninit        = FALSE ;
   bson_iterator it ;
   bson result ;
   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;
   if ( !handle )
   {
      rc = SDB_INVALIDARG;
     goto error ;
   }

   BSON_INIT( result );

   rc = _sdbGetReplicaGroupDetail ( cHandle, &result ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( BSON_INT != bson_find ( &it, &result, CAT_PRIMARY_NAME ) )
   {
      rc = SDB_CLS_NODE_NOT_EXIST ;
      goto error ;
   }
   primaryNode = bson_iterator_int ( &it ) ;
   if ( BSON_ARRAY != bson_find ( &it, &result, CAT_GROUP_NAME ) )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   {
      const CHAR *groupList = bson_iterator_value ( &it ) ;
      bson_iterator i ;
      bson_iterator_from_buffer ( &i, groupList ) ;
      while ( bson_iterator_next ( &i ) )
      {
         bson intObj ;
       BSON_INIT( intObj ) ;
         if ( BSON_OBJECT == (signed int)bson_iterator_type ( &i ) &&
              BSON_OK == bson_init_finished_data ( &intObj,
                                       (CHAR*)bson_iterator_value ( &i ) ) )
         {
            bson_iterator k ;
            if ( BSON_INT != bson_find ( &k, &intObj, CAT_NODEID_NAME ) )
            {
               rc = SDB_SYS ;
               bson_destroy ( &intObj ) ;
               goto error ;
            }
            if ( primaryNode == bson_iterator_int ( &k ) )
            {
               primaryData = intObj.data ;
               break ;
            }
         }

         bson_destroy ( &intObj ) ;
      }
   }
   if ( primaryData )
   {
      rc = _sdbShardExtractNode ( cHandle, handle, primaryData,
                                  r->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   }
   else
   {
      rc = SDB_CLS_NODE_NOT_EXIST ;
      goto error ;
   }
done :
   BSON_DESTROY( result ) ;
   return rc ;
error :
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetNodeSlave ( sdbReplicaGroupHandle cHandle,
                                   sdbNodeHandle *handle )
{
   INT32 rc                = SDB_OK ;
   const CHAR *primaryData = NULL ;
   INT32 primaryNode       = -1 ;
   sdbRGStruct *r          = (sdbRGStruct*)cHandle ;
   BOOLEAN bsoninit        = FALSE ;
   bson_iterator it ;
   bson result ;

   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;
   if ( !handle )
   {
      rc = SDB_INVALIDARG;
      goto error;
   }

   BSON_INIT( result );
   rc = _sdbGetReplicaGroupDetail ( cHandle, &result ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( BSON_INT == bson_find ( &it, &result, CAT_PRIMARY_NAME ) )
   {
      primaryNode = bson_iterator_int ( &it ) ;
   }
   if ( BSON_ARRAY != bson_find ( &it, &result, CAT_GROUP_NAME ) )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   {
      const CHAR *groupList = bson_iterator_value ( &it ) ;
      bson_iterator i ;
      BOOLEAN first = TRUE ;
      INT32 totalNum = -1 ;
      INT32 fetchNum = -1 ;
      bson intObj ;
      BOOLEAN bsoninit = FALSE ;

      do{
            totalNum = 0 ;
            bson_iterator_from_buffer ( &i, groupList ) ;
            while ( bson_iterator_next ( &i ) )
            {
               BSON_INIT( intObj );

               if ( BSON_OBJECT == (signed int)bson_iterator_type ( &i ) &&
                    BSON_OK == bson_init_finished_data ( &intObj,
                                  (CHAR*)bson_iterator_value ( &i ) ) )
               {
                  bson_iterator k ;
                  if ( BSON_INT != bson_find ( &k, &intObj, CAT_NODEID_NAME ) )
                  {
                     rc = SDB_SYS ;
                     goto error ;
                  }
                  if ( primaryNode != bson_iterator_int ( &k ) )
                  {
                     if ( !first && totalNum == fetchNum )
                     {
                        primaryData = intObj.data ;
                        break ;
                     }
                     ++totalNum ;
                  }
                  else
                  {
                     if ( !first && -1 == fetchNum )
                     {
                        primaryData = intObj.data ;
                        break ;
                     }
                  }
               } // if ( BSON_OBJECT == (signed int)bson_iterator_type ( &i ) &&

             bson_destroy ( &intObj ) ;
             bsoninit = FALSE ;
            } // while ( bson_iterator_next ( &i ) )

            if ( bsoninit )
            {
               bson_destroy ( &intObj ) ;
            }

            if ( first )
            {
               first = FALSE ;
               if ( totalNum )
               {
                  fetchNum = _sdbRand() % totalNum ;
               }
            }
            else
            {
               break;
            }
         }while(TRUE);
   }

   if ( !primaryData )
   {
      rc = SDB_CLS_NODE_NOT_EXIST ;
      goto error ;
   }

   rc = _sdbShardExtractNode ( cHandle, handle, primaryData,
                               r->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( result ) ;
   return rc ;
error :
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetNodeByName ( sdbReplicaGroupHandle cHandle,
                                    const CHAR *pNodeName,
                                    sdbNodeHandle *handle )
{
   INT32 rc           = SDB_OK ;
   CHAR *pHostName    = NULL ;
   CHAR *pServiceName = NULL ;
   sdbRGStruct *r     = (sdbRGStruct*)cHandle ;

   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;
   if ( !pNodeName || !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   pHostName = (CHAR*)SDB_OSS_MALLOC ( ossStrlen ( pNodeName + 1 ) ) ;
   if ( !pHostName )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   pServiceName = ossStrchr ( pHostName, NODE_NAME_SERVICE_SEPCHAR ) ;
   if ( !pServiceName )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   *pServiceName = '\0' ;
   pServiceName ++ ;
   rc = sdbGetNodeByHost ( cHandle, pHostName, pServiceName, handle ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   if ( pHostName )
   {
      SDB_OSS_FREE ( pHostName ) ;
   }
   return rc ;
error :
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetNodeByHost ( sdbReplicaGroupHandle cHandle,
                                    const CHAR *pHostName,
                                    const CHAR *pServiceName,
                                    sdbNodeHandle *handle )
{
   INT32 rc                = SDB_OK ;
   const CHAR *hostName    = NULL ;
   const CHAR *serviceName = NULL ;
   const CHAR *nodeName    = NULL ;
   BOOLEAN bsoninit        = FALSE ;
   bson result ;
   bson_iterator it ;
   sdbRGStruct *r          = (sdbRGStruct*)cHandle ;
   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICAGROUP ) ;
   if ( !pHostName || !pServiceName || !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( result );
   *handle = SDB_INVALID_HANDLE ;

   rc = _sdbGetReplicaGroupDetail ( cHandle, &result ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( BSON_ARRAY != bson_find ( &it, &result, CAT_GROUP_NAME ) )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   {
      INT32 nodeID = 0 ;
      const CHAR *groupList = bson_iterator_value ( &it ) ;
      bson_iterator i ;
      sdbNodeHandle interhandle;
      bson_iterator_from_buffer ( &i, groupList ) ;

      while ( BSON_EOO != bson_iterator_next ( &i ) )
      {
         rc = _sdbShardExtractNode ( cHandle, &interhandle,
                                     (CHAR*)bson_iterator_value ( &i ),
                                     r->_endianConvert ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         sdbGetNodeAddr ( interhandle, &hostName,
                          &serviceName, &nodeName,
                          &nodeID ) ;

         if ( !ossStrcmp ( hostName, pHostName ) &&
              !ossStrcmp ( serviceName, pServiceName ))
         {
            break ;
         }
         sdbReleaseNode ( interhandle ) ;
         interhandle = SDB_INVALID_HANDLE ;
      }

      *handle = interhandle ;
   }

   if ( SDB_INVALID_HANDLE == *handle )
   {
      rc = SDB_CLS_NODE_NOT_EXIST ;
      goto error ;
   }
done :
   BSON_DESTROY( result ) ;
   return rc ;
error :
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetNodeAddr ( sdbNodeHandle cHandle,
                                  const CHAR **ppHostName,
                                  const CHAR **ppServiceName,
                                  const CHAR **ppNodeName,
                                  INT32 *pNodeID )
{
   INT32 rc       = SDB_OK ;
   sdbRNStruct *r = (sdbRNStruct*)cHandle ;

   HANDLE_CHECK( cHandle, r, SDB_HANDLE_TYPE_REPLICANODE ) ;

   if ( ppHostName )
   {
      *ppHostName = r->_hostName ;
   }
   if ( ppServiceName )
   {
      *ppServiceName = r->_serviceName ;
   }
   if ( ppNodeName )
   {
      *ppNodeName = r->_nodeName ;
   }
   if ( pNodeID )
   {
      *pNodeID = r->_nodeID ;
   }
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbStartNode ( sdbNodeHandle cHandle )
{
   return _sdbStartStopNode ( cHandle, TRUE ) ;
}

SDB_EXPORT INT32 sdbStopNode ( sdbNodeHandle cHandle )
{
   return _sdbStartStopNode ( cHandle, FALSE ) ;
}

SDB_EXPORT INT32 sdbListCollectionSpaces ( sdbConnectionHandle cHandle,
                                           sdbCursorHandle *handle )
{
   return sdbGetList ( cHandle, SDB_LIST_COLLECTIONSPACES, NULL, NULL, NULL,
                       handle ) ;
}

SDB_EXPORT INT32 sdbListCollections ( sdbConnectionHandle cHandle,
                                      sdbCursorHandle *handle )
{
   return sdbGetList ( cHandle, SDB_LIST_COLLECTIONS, NULL, NULL, NULL,
                       handle ) ;
}

SDB_EXPORT INT32 sdbListReplicaGroups ( sdbConnectionHandle cHandle,
                                        sdbCursorHandle *handle )
{
   return sdbGetList ( cHandle, SDB_LIST_GROUPS, NULL, NULL, NULL,
                       handle ) ;
}

SDB_EXPORT INT32 sdbFlushConfigure( sdbConnectionHandle cHandle,
                                    bson *options )
{
   INT32 rc = SDB_OK ;
   BOOLEAN r ;
   SINT64 contextID = 0 ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   rc = clientBuildQueryMsg( &(connection->_pSendBuffer),
                             &(connection->_sendBufferSize),
                             (CMD_ADMIN_PREFIX CMD_NAME_EXPORT_CONFIG),
                             0, 0, 0, -1, options, NULL, NULL, NULL,
                             connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      ossPrintf ( "Failed to build flush msg, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)(connection->_pSendBuffer),
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &r,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done:
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbCrtJSProcedure( sdbConnectionHandle cHandle,
                                    const CHAR *code )
{
   INT32 rc = SDB_OK ;
   BOOLEAN r ;
   SINT64 contextID = 0 ;
   bson bs ;
   BOOLEAN bsoninit = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !code )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( bs );
   BSON_APPEND( bs, FIELD_NAME_FUNC, code, code ) ;
   BSON_APPEND( bs, FMP_FUNC_TYPE, FMP_FUNC_TYPE_JS, int );

   BSON_FINISH( bs ) ;

   rc = clientBuildQueryMsg( &(connection->_pSendBuffer),
                             &(connection->_sendBufferSize),
                             (CMD_ADMIN_PREFIX CMD_NAME_CRT_PROCEDURE),
                             0, 0, 0, -1, &bs, NULL, NULL, NULL,
                             connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      ossPrintf ( "Failed to build crt procedure msg, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)(connection->_pSendBuffer),
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &r,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done:
   BSON_DESTROY( bs ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbRmProcedure( sdbConnectionHandle cHandle,
                                 const CHAR *spName )
{
   INT32 rc = SDB_OK ;
   BOOLEAN r ;
   SINT64 contextID = 0 ;
   bson bs ;
   BOOLEAN bsoninit = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !spName )
   {
      rc = SDB_INVALIDARG ;
     goto error ;
   }

   BSON_INIT( bs );
   BSON_APPEND( bs, FIELD_NAME_FUNC, spName, string ) ;
   BSON_FINISH( bs ) ;
   rc = clientBuildQueryMsg( &(connection->_pSendBuffer),
                             &(connection->_sendBufferSize),
                             (CMD_ADMIN_PREFIX CMD_NAME_RM_PROCEDURE),
                             0, 0, 0, -1, &bs, NULL, NULL, NULL,
                             connection->_endianConvert ) ;

   if ( SDB_OK != rc )
   {
      ossPrintf ( "Failed to build rm procedues msg, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)(connection->_pSendBuffer),
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &r,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done:
   BSON_DESTROY( bs ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbListProcedures( sdbConnectionHandle cHandle,
                                    bson *condition,
                                    sdbCursorHandle *handle )
{
   return sdbGetList( cHandle, SDB_LIST_STOREPROCEDURES, condition, NULL, NULL,
                      handle ) ;
}

SDB_EXPORT INT32 sdbEvalJS(sdbConnectionHandle cHandle,
                           const CHAR *code,
                           SDB_SPD_RES_TYPE *type,
                           sdbCursorHandle *handle,
                           bson *errmsg )
{
   INT32 rc = SDB_OK ;
   BOOLEAN r ;
   bson bs ;
   SINT64 contextID = 0 ;
   sdbCursorStruct *cursor = NULL ;
   BOOLEAN bsoninit = FALSE;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !code || !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( *type < SDB_SPD_RES_TYPE_VOID || *type > SDB_SPD_RES_TYPE_RN )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( bs );
   BSON_APPEND( bs, FIELD_NAME_FUNC, code, code );
   BSON_APPEND( bs, FIELD_NAME_FUNCTYPE, FMP_FUNC_TYPE_JS, int ) ;
   BSON_FINISH( bs ) ;

   rc = clientBuildQueryMsg( &(connection->_pSendBuffer),
                             &(connection->_sendBufferSize),
                             (CMD_ADMIN_PREFIX CMD_NAME_EVAL),
                             0, 0, 0, -1, &bs, NULL, NULL, NULL,
                             connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      ossPrintf ( "Failed to build flush msg, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)(connection->_pSendBuffer),
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtractEval ( cHandle, connection->_sock,
                           (MsgHeader**)&connection->_pReceiveBuffer,
                           &connection->_receiveBufferSize, &contextID,
                           type, &r, errmsg, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, connection, connection, contextID );

   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *handle = (sdbCursorHandle)cursor ;
done:
   BSON_DESTROY( bs ) ;
   return rc ;
error:
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }

   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbGetCollection1 ( sdbCSHandle cHandle,
                                     const CHAR *pCollectionName,
                                     sdbCollectionHandle *handle )
{
   INT32 rc                        = SDB_OK ;
   BOOLEAN result                  = FALSE ;
   bson newObj ;
   BOOLEAN bsoninit                = FALSE ;
   CHAR *pTestCollection           = CMD_ADMIN_PREFIX CMD_NAME_TEST_COLLECTION ;
   CHAR *pName                     = FIELD_NAME_NAME ;
   sdbCollectionStruct *s          = NULL ;
   sdbCSStruct *cs                 = (sdbCSStruct*)cHandle ;
   CHAR fullCollectionName [ CLIENT_COLLECTION_NAMESZ + CLIENT_CS_NAMESZ + 2 ] = {0};

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CS ) ;
   if ( !pCollectionName || !handle ||
        ossStrlen ( pCollectionName) > CLIENT_COLLECTION_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;

   ossStrncpy ( fullCollectionName, cs->_CSName, sizeof(cs->_CSName) ) ;
   ossStrncat ( fullCollectionName, ".", 1 ) ;
   ossStrncat ( fullCollectionName, pCollectionName, CLIENT_COLLECTION_NAMESZ );
   BSON_APPEND( newObj, pName, fullCollectionName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = _runCommand ( cs->_connection, cs->_sock, &cs->_pSendBuffer,
                      &cs->_sendBufferSize,
                      &cs->_pReceiveBuffer,
                      &cs->_receiveBufferSize,
                      cs->_endianConvert,
                      pTestCollection, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   ALLOC_HANDLE( s, sdbCollectionStruct ) ;
   s->_handleType    = SDB_HANDLE_TYPE_COLLECTION ;
   s->_connection    = cs->_connection ;
   s->_sock          = cs->_sock ;
   s->_endianConvert = cs->_endianConvert ;
   rc = _setCollectionName ( (sdbCollectionHandle)s, fullCollectionName ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   _regSocket( cs->_connection, &s->_sock ) ;
   *handle = (sdbCollectionHandle)s ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   if ( s )
   {
      SDB_OSS_FREE ( s ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbCreateCollection1 ( sdbCSHandle cHandle,
                                        const CHAR *pCollectionName,
                                        bson *options,
                                        sdbCollectionHandle *handle )
{
   INT32 rc                        = SDB_OK ;
   BOOLEAN result                  = FALSE ;
   bson newObj ;
   BOOLEAN bsoninit                = FALSE ;
   CHAR *pTestCollection           = CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTION ;
   CHAR *pName                     = FIELD_NAME_NAME ;
   sdbCollectionStruct *s          = NULL ;
   sdbCSStruct *cs                 = (sdbCSStruct*)cHandle ;
   bson_iterator it ;
   CHAR fullCollectionName [ CLIENT_COLLECTION_NAMESZ + CLIENT_CS_NAMESZ + 2 ] = {0};
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CS ) ;
   if ( !pCollectionName || !handle ||
        ossStrlen ( pCollectionName) > CLIENT_COLLECTION_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;

   ossStrncpy ( fullCollectionName, cs->_CSName, sizeof(cs->_CSName) ) ;
   ossStrncat ( fullCollectionName, ".", 1 ) ;
   ossStrncat ( fullCollectionName, pCollectionName, CLIENT_COLLECTION_NAMESZ );
   BSON_APPEND( newObj, pName, fullCollectionName, string ) ;
   if ( options )
   {
      bson_iterator_init ( &it, options ) ;
      while ( BSON_EOO != bson_iterator_next ( &it ) )
      {
         BSON_APPEND ( newObj, NULL, &it, element ) ;
      }
   }
   BSON_FINISH ( newObj ) ;
   rc = _runCommand ( cs->_connection, cs->_sock, &cs->_pSendBuffer,
                      &cs->_sendBufferSize,
                      &cs->_pReceiveBuffer,
                      &cs->_receiveBufferSize,
                      cs->_endianConvert,
                      pTestCollection, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   ALLOC_HANDLE( s, sdbCollectionStruct ) ;
   s->_handleType    = SDB_HANDLE_TYPE_COLLECTION ;
   s->_connection    = cs->_connection ;
   s->_sock          = cs->_sock ;
   s->_endianConvert = cs->_endianConvert ;
   rc = _setCollectionName ( (sdbCollectionHandle)s, fullCollectionName ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   _regSocket( cs->_connection, &s->_sock ) ;
   *handle = (sdbCollectionHandle)s ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   if ( s )
   {
       SDB_OSS_FREE ( s ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbCreateCollection ( sdbCSHandle cHandle,
                                       const CHAR *pCollectionName,
                                       sdbCollectionHandle *handle )
{
   return sdbCreateCollection1 ( cHandle, pCollectionName, NULL, handle ) ;
}

SDB_EXPORT INT32 sdbAlterCollection ( sdbCollectionHandle cHandle,
                                      bson *options  )
{
   INT32 rc                        = SDB_OK ;
   BOOLEAN result                  = FALSE ;
   bson newObj ;
   SINT64 contextID                = 0 ;
   BOOLEAN bsoninit                = FALSE ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !options ||
        !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, FIELD_NAME_NAME,
                cs->_collectionFullName, string ) ;

   BSON_APPEND( newObj, FIELD_NAME_OPTIONS, options, bson ) ;
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_ALTER_COLLECTION,
                              0, 0, -1, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbDropCollection ( sdbCSHandle cHandle,
                                     const CHAR *pCollectionName )
{
   INT32 rc                        = SDB_OK ;
   BOOLEAN result                  = FALSE ;
   CHAR *pTestCollection           = CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTION ;
   CHAR *pName                     = FIELD_NAME_NAME ;
   sdbCSStruct *cs                 = (sdbCSStruct*)cHandle ;
   CHAR fullCollectionName [ CLIENT_COLLECTION_NAMESZ + CLIENT_CS_NAMESZ + 2 ] = {0};
   bson newObj ;
   BOOLEAN bsoninit = FALSE ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CS ) ;
   if ( !pCollectionName ||
        ossStrlen ( pCollectionName) > CLIENT_COLLECTION_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;

   ossStrncpy ( fullCollectionName, cs->_CSName, sizeof(cs->_CSName) ) ;
   ossStrncat ( fullCollectionName, ".", 1 ) ;
   ossStrncat ( fullCollectionName, pCollectionName, CLIENT_COLLECTION_NAMESZ );
   BSON_APPEND( newObj, pName, fullCollectionName, string ) ;
   BSON_FINISH ( newObj ) ;
   rc = _runCommand ( cs->_connection, cs->_sock, &cs->_pSendBuffer,
                      &cs->_sendBufferSize,
                      &cs->_pReceiveBuffer,
                      &cs->_receiveBufferSize,
                      cs->_endianConvert,
                      pTestCollection, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}


SDB_EXPORT INT32 sdbGetCSName ( sdbCSHandle cHandle,
                                CHAR **ppCSName )
{
   INT32 rc                        = SDB_OK ;
   sdbCSStruct *cs                 = (sdbCSStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CS ) ;
   if ( !ppCSName )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   *ppCSName = &cs->_CSName[0] ;
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbSplitCollection ( sdbCollectionHandle cHandle,
                                      const CHAR *pSourceGroup,
                                      const CHAR *pTargetGroup,
                                      const bson *pSplitCondition,
                                      const bson *pSplitEndCondition )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN result   = FALSE;
   SINT64 contextID = 0 ;
   bson newObj ;
   BOOLEAN bsoninit = FALSE ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !pSourceGroup || !pTargetGroup || !pSplitCondition ||
        !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT ( newObj ) ;
   BSON_APPEND( newObj, CAT_COLLECTION_NAME,
                cs->_collectionFullName, string ) ;
   BSON_APPEND( newObj, CAT_SOURCE_NAME, pSourceGroup, string ) ;
   BSON_APPEND( newObj, CAT_TARGET_NAME, pTargetGroup, string ) ;
   BSON_APPEND( newObj, CAT_SPLITQUERY_NAME, pSplitCondition, bson ) ;

   if ( pSplitEndCondition )
   {
      BSON_APPEND( newObj, CAT_SPLITENDQUERY_NAME, pSplitEndCondition, bson ) ;
   }

   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                              0, 0, -1, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbSplitCLAsync ( sdbCollectionHandle cHandle,
                                   const CHAR *pSourceGroup,
                                   const CHAR *pTargetGroup,
                                   const bson *pSplitCondition,
                                   const bson *pSplitEndCondition,
                                   SINT64 *taskID )
{
   INT32 rc            = SDB_OK ;
   BOOLEAN bresult     = FALSE ;
   SINT64 contextID    = 0 ;
   BOOLEAN bsoninit    = FALSE ;
   sdbCursorStruct *cursor = NULL ;
   bson_iterator it ;
   bson newObj ;
   bson result ;
   sdbCollectionStruct *cs = (sdbCollectionStruct *)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !pSourceGroup || !pTargetGroup || !pSplitCondition || !taskID ||
        !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj );
   BSON_INIT( result );
   BSON_APPEND( newObj, CAT_COLLECTION_NAME,
                cs->_collectionFullName, string ) ;
   BSON_APPEND( newObj, CAT_SOURCE_NAME, pSourceGroup, string ) ;
   BSON_APPEND( newObj, CAT_TARGET_NAME, pTargetGroup, string ) ;
   BSON_APPEND( newObj, CAT_SPLITQUERY_NAME, pSplitCondition, bson ) ;

   if ( NULL != pSplitEndCondition )
   {
      BSON_APPEND( newObj, CAT_SPLITENDQUERY_NAME,
                   pSplitEndCondition, bson ) ;
   }
   BSON_APPEND( newObj, FIELD_NAME_ASYNC, TRUE, bool ) ;
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                              0, 0, -1, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &bresult,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, cs->_connection, cs, contextID ) ;
   ossMemcpy ( cursor->_collectionFullName, cs->_collectionFullName,
               sizeof(cursor->_collectionFullName) ) ;

   rc = sdbNext ( (sdbCursorHandle)cursor, &result ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   if ( BSON_LONG == bson_find ( &it, &result, FIELD_NAME_TASKID ) )
   {
      *taskID = bson_iterator_long ( &it ) ;
   }
   else
   {
      rc = SDB_SYS ;
     goto error ;
   }

done :
   BSON_DESTROY( newObj ) ;
   BSON_DESTROY( result ) ;

   if ( cursor )
   {
      sdbReleaseCursor ( (sdbCursorHandle)cursor ) ;
   }
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbSplitCollectionByPercent( sdbCollectionHandle cHandle,
                                              const CHAR * pSourceGroup,
                                              const CHAR * pTargetGroup,
                                              double percent )
{
   INT32 rc = SDB_OK ;
   BOOLEAN result ;
   SINT64 contextID = 0 ;
   bson newObj ;
   BOOLEAN bsoninit = FALSE;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( percent <= 0.0 || percent > 100.0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( !pSourceGroup || !pTargetGroup || !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj );
   BSON_APPEND( newObj, CAT_COLLECTION_NAME,
                cs->_collectionFullName, string ) ;
   BSON_APPEND( newObj, CAT_SOURCE_NAME, pSourceGroup, string ) ;
   BSON_APPEND( newObj, CAT_TARGET_NAME, pTargetGroup, string ) ;
   BSON_APPEND( newObj, CAT_SPLITPERCENT_NAME, percent, double ) ;
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                              0, 0, -1, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}


SDB_EXPORT INT32 sdbSplitCLByPercentAsync ( sdbCollectionHandle cHandle,
                                            const CHAR *pSourceGroup,
                                            const CHAR *pTargetGroup,
                                            FLOAT64 percent,
                                            SINT64 *taskID )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN bresult  = FALSE;
   SINT64 contextID = 0 ;
   bson newObj ;
   bson result ;
   BOOLEAN bsoninit = FALSE;
   sdbCursorStruct *cursor = NULL;
   bson_iterator it ;
   sdbCollectionStruct *cs = (sdbCollectionStruct *)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( percent <= 0.0 || percent > 100.0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( !pSourceGroup || !pTargetGroup ||
        !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_INIT( result ) ;
   BSON_APPEND( newObj, CAT_COLLECTION_NAME,
                cs->_collectionFullName, string ) ;
   BSON_APPEND( newObj, CAT_SOURCE_NAME, pSourceGroup, string ) ;
   BSON_APPEND( newObj, CAT_TARGET_NAME, pTargetGroup, string ) ;
   BSON_APPEND( newObj, CAT_SPLITPERCENT_NAME, percent, double ) ;
   BSON_APPEND( newObj, FIELD_NAME_ASYNC, TRUE, bool ) ;
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                              0, 0, -1, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &bresult,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, cs->_connection, cs, contextID );
   ossMemcpy ( cursor->_collectionFullName, cs->_collectionFullName,
               sizeof(cursor->_collectionFullName) ) ;

   rc = sdbNext ( (sdbCursorHandle)cursor, &result ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   if ( BSON_LONG == bson_find ( &it, &result, FIELD_NAME_TASKID ) )
   {
      *taskID = bson_iterator_long ( &it ) ;
   }
   else
   {
      rc = SDB_SYS ;
     goto error;
   }
done :
   BSON_DESTROY( newObj ) ;
   BSON_DESTROY( result ) ;
   if ( cursor )
   {
      sdbReleaseCursor ( (sdbCursorHandle)cursor ) ;
   }
   return rc ;
error :
   goto done ;
}

/*
SDB_EXPORT INT32 sdbRenameCollection ( sdbCollectionHandle cHandle,
                                       const CHAR *pNewName )
{
   INT32 rc = SDB_OK ;
   BOOLEAN result ;
   SINT64 contextID = 0 ;
   bson obj ;
   bson_init ( &obj ) ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   if ( !cs ||
        cs->_handleType != SDB_HANDLE_TYPE_COLLECTION )
   {
      rc = SDB_CLT_INVALID_HANDLE ;
      goto error ;
   }
   if ( !pNewName )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = bson_append_string ( &obj, FIELD_NAME_COLLECTIONSPACE, cs->_CSName ) ;
   if ( rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   rc = bson_append_string ( &obj, FIELD_NAME_OLDNAME, cs->_collectionName ) ;
   if ( rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   rc = bson_append_string ( &obj, FIELD_NAME_NEWNAME, pNewName ) ;
   if ( rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   bson_finish ( &obj ) ;

   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_RENAME_COLLECTION,
                              0, 0, -1, -1, &obj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = _send ( cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_sock, (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }
done :
   bson_destroy ( &obj ) ;
   return rc ;
error :
   goto done ;
}*/

SDB_EXPORT INT32 sdbCreateIndex ( sdbCollectionHandle cHandle,
                                  bson *indexDef,
                                  const CHAR *pIndexName,
                                  BOOLEAN isUnique,
                                  BOOLEAN isEnforced )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN result   = FALSE;
   SINT64 contextID = 0 ;
   bson indexObj ;
   bson newObj ;
   BOOLEAN bsoninit = FALSE;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !cs->_collectionFullName[0] || !indexDef ||
        !pIndexName )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( indexObj ) ;
   BSON_INIT( newObj ) ;

   BSON_APPEND( indexObj, IXM_FIELD_NAME_KEY, indexDef, bson ) ;
   BSON_APPEND( indexObj, IXM_FIELD_NAME_NAME, pIndexName, string ) ;
   BSON_APPEND( indexObj, IXM_FIELD_NAME_UNIQUE, isUnique, bool ) ;
   BSON_APPEND( indexObj, IXM_FIELD_NAME_ENFORCED, isEnforced, bool ) ;
   BSON_FINISH ( indexObj ) ;

   BSON_APPEND( newObj, FIELD_NAME_COLLECTION,
                cs->_collectionFullName, string ) ;

   BSON_APPEND( newObj, FIELD_NAME_INDEX,  &indexObj, bson ) ;
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_CREATE_INDEX,
                              0, 0, -1, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   BSON_DESTROY( indexObj ) ;
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbGetIndexes ( sdbCollectionHandle cHandle,
                                 const CHAR *pIndexName,
                                 sdbCursorHandle *handle )
{
   INT32 rc = SDB_OK ;
   BOOLEAN result ;
   SINT64 contextID = 0 ;
   bson queryCond ;
   bson newObj ;
   BOOLEAN bsoninit = FALSE ;
   sdbCursorStruct *cursor = NULL ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !handle )
   {
      rc = SDB_CLT_INVALID_HANDLE ;
      goto error ;
   }
   if ( !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( queryCond ) ;
   BSON_INIT( newObj ) ;

   /* build query condition */
   if ( pIndexName )
   {
      BSON_APPEND( queryCond, IXM_FIELD_NAME_INDEX_DEF "."IXM_FIELD_NAME_NAME,
                   pIndexName, string ) ;
      BSON_FINISH ( queryCond ) ;
   }
   /* build collection name */
   BSON_APPEND( newObj, FIELD_NAME_COLLECTION,
                cs->_collectionFullName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_GET_INDEXES,
                              0, 0, -1, -1,
                              pIndexName?&queryCond:NULL,
                              NULL, NULL, &newObj, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, cs->_connection, cs, contextID ) ;
   ossMemcpy ( cursor->_collectionFullName, cs->_collectionFullName,
               sizeof(cursor->_collectionFullName) ) ;
   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   *handle                  = (sdbCursorHandle)cursor ;
done :
   BSON_DESTROY( queryCond ) ;
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbDropIndex ( sdbCollectionHandle cHandle,
                                const CHAR *pIndexName )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN result   = FALSE ;
   SINT64 contextID = 0 ;
   bson indexObj ;
   bson newObj ;
   BOOLEAN bsoninit = FALSE ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !cs->_collectionFullName[0] ||
        !pIndexName )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( indexObj ) ;
   BSON_INIT( newObj ) ;
   BSON_APPEND( indexObj, "", pIndexName, string ) ;
   BSON_FINISH ( indexObj ) ;
   BSON_APPEND( newObj, FIELD_NAME_COLLECTION,
                cs->_collectionFullName, string ) ;
   BSON_APPEND( newObj, FIELD_NAME_INDEX,  &indexObj, bson ) ;
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_DROP_INDEX,
                              0, 0, -1, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   BSON_DESTROY( indexObj ) ;
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbGetCount ( sdbCollectionHandle cHandle,
                               bson *condition,
                               SINT64 *count )
{
   return sdbGetCount1(cHandle, condition, NULL, count) ;
}

SDB_EXPORT INT32 sdbGetCount1 ( sdbCollectionHandle cHandle,
                                bson *condition,
                                bson *hint,
                                SINT64 *count )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN bresult  = FALSE ;
   SINT64 contextID = 0 ;
   bson newObj ;
   bson result ;
   BOOLEAN bsoninit = FALSE;
   sdbCursorStruct *cursor = NULL ;
   bson_iterator it ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !count )
   {
      rc = SDB_CLT_INVALID_HANDLE ;
      goto error ;
   }
   if ( !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_INIT( result ) ;
   /* build collection name */
   BSON_APPEND( newObj, FIELD_NAME_COLLECTION,
                cs->_collectionFullName, string ) ;
   if ( hint )
   {
      BSON_APPEND( newObj, FIELD_NAME_HINT, hint, bson ) ;
   }
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_GET_COUNT,
                              0, 0, -1, -1,
                              condition,
                              NULL, NULL, &newObj, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &bresult,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, cs->_connection, cs, contextID ) ;
   ossMemcpy ( cursor->_collectionFullName, cs->_collectionFullName,
               sizeof(cursor->_collectionFullName) ) ;

   rc = sdbNext ( (sdbCursorHandle)cursor, &result ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( BSON_LONG == bson_find ( &it, &result, FIELD_NAME_TOTAL ) )
   {
      *count = bson_iterator_long ( &it ) ;
   }
   else
   {
      rc = SDB_SYS ;
      goto error ;
   }
done :
   BSON_DESTROY( newObj ) ;
   BSON_DESTROY( result ) ;
   if ( cursor )
   {
      sdbReleaseCursor ( (sdbCursorHandle)cursor ) ;
   }
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbInsert ( sdbCollectionHandle cHandle,
                             bson *obj )
{
   return sdbInsert1 ( cHandle, obj, NULL ) ;
}

SDB_EXPORT INT32 sdbInsert1 ( sdbCollectionHandle cHandle,
                              bson *obj, bson_iterator *id )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;
   BOOLEAN result   = FALSE ;
   bson_iterator tempid ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !cs->_collectionFullName[0] || !obj )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientAppendOID ( obj, &tempid ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = clientBuildInsertMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                               cs->_collectionFullName, 0, 0, obj, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   if ( id )
   {
      ossMemcpy ( id, &tempid, sizeof(bson_iterator) ) ;
   }
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbBulkInsert ( sdbCollectionHandle cHandle,
                                 SINT32 flags, bson **obj, SINT32 num )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;
   BOOLEAN result   = FALSE ;
   SINT32 count     = 0 ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !cs->_collectionFullName[0] || !obj )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( num < 0)
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   else if ( !num )
   {
      goto done ;
   }

   for ( count = 0; count < num; ++count )
   {
      if ( !obj[count] )
         break ;
      rc = clientAppendOID ( obj[count], NULL ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      if ( 0 == count )
         rc = clientBuildInsertMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                                     cs->_collectionFullName, flags, 0,
                                     obj[count], cs->_endianConvert ) ;
      else
         rc = clientAppendInsertMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                                      obj[count], cs->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   return rc ;
error :
   goto done ;
}
/*
static INT32 _sdbUpdate ( SOCKET sock, CHAR *pCollectionFullName,
                          CHAR **ppSendBuffer, INT32 *sendBufferSize,
                          CHAR **ppReceiveBuffer, INT32 *receiveBufferSize,
                          BOOLEAN endianConvert,
                          bson *rule, bson *condition, bson *hint )
{
   INT32 rc = SDB_OK ;
   SINT64 contextID ;
   BOOLEAN result ;
   if ( !pCollectionFullName || !ppSendBuffer || !sendBufferSize ||
        !ppReceiveBuffer || !receiveBufferSize || !rule )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   rc = clientBuildUpdateMsg ( ppSendBuffer, sendBufferSize,
                               pCollectionFullName, 0, 0, condition,
                               rule, hint, endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = _send ( sock, (MsgHeader*)(*ppSendBuffer), endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = _recvExtract ( sock, (MsgHeader**)ppReceiveBuffer,
                       receiveBufferSize, &contextID, &result,
                       endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}
*/

SDB_EXPORT INT32 sdbUpdate ( sdbCollectionHandle cHandle,
                             bson *rule,
                             bson *condition,
                             bson *hint )
{
   return __sdbUpdate ( cHandle, 0, rule, condition, hint ) ;
}

SDB_EXPORT INT32 sdbUpsert ( sdbCollectionHandle cHandle,
                             bson *rule,
                             bson *condition,
                             bson *hint )
{
   return __sdbUpdate ( cHandle, FLG_UPDATE_UPSERT, rule, condition, hint ) ;
}
/*
static INT32 _sdbDelete ( SOCKET sock, CHAR *pCollectionFullName,
                          CHAR **ppSendBuffer, INT32 *sendBufferSize,
                          CHAR **ppReceiveBuffer, INT32 *receiveBufferSize,
                          BOOLEAN endianConvert,
                          bson *condition, bson *hint )
{
   INT32 rc = SDB_OK ;
   SINT64 contextID ;
   BOOLEAN result ;
   if ( !pCollectionFullName || !ppSendBuffer || !sendBufferSize ||
        !ppReceiveBuffer || !receiveBufferSize )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   rc = clientBuildDeleteMsg ( ppSendBuffer, sendBufferSize,
                               pCollectionFullName, 0, 0, condition,
                               hint, endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = _send ( sock, (MsgHeader*)(*ppSendBuffer), endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = _recvExtract ( sock, (MsgHeader**)ppReceiveBuffer,
                       receiveBufferSize, &contextID, &result,
                       endianConvert ) ;
   if ( rc )
   {
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}
*/

SDB_EXPORT INT32 sdbDelete ( sdbCollectionHandle cHandle,
                             bson *condition,
                             bson *hint )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;
   BOOLEAN result   = FALSE ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientBuildDeleteMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                                cs->_collectionFullName, 0, 0, condition,
                                hint, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbExplain ( sdbCollectionHandle cHandle,
                              bson *condition,
                              bson *selector,
                              bson *orderBy,
                              bson *hint,
                              INT32 flags,
                              INT64 numToSkip,
                              INT64 numToReturn,
                              bson *options,
                              sdbCursorHandle *handle )
{
   INT32 rc = SDB_OK ;
   bson newObj ;
   BOOLEAN bsoninit = FALSE ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;

   if ( !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;

   if ( hint )
   {
      BSON_APPEND( newObj, FIELD_NAME_HINT, hint, bson ) ;
   }

   if ( options )
   {
      BSON_APPEND( newObj, FIELD_NAME_OPTIONS, options, bson ) ;
   }
   BSON_FINISH ( newObj ) ;

   rc = sdbQuery1( cHandle, condition, selector, orderBy, &newObj,
                   numToSkip, numToReturn, flags | FLG_QUERY_EXPLAIN,
                   handle ) ;
   if ( rc )
   {
      goto error ;
   }

done:
   BSON_DESTROY( newObj ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbQuery ( sdbCollectionHandle cHandle,
                            bson *condition,
                            bson *select,
                            bson *orderBy,
                            bson *hint,
                            INT64 numToSkip,
                            INT64 numToReturn,
                            sdbCursorHandle *handle )
{
   return sdbQuery1 ( cHandle, condition, select, orderBy, hint,
                      numToSkip, numToReturn, 0, handle ) ;
}

SDB_EXPORT INT32 sdbQuery1 ( sdbCollectionHandle cHandle,
                             bson *condition,
                             bson *select,
                             bson *orderBy,
                             bson *hint,
                             INT64 numToSkip,
                             INT64 numToReturn,
                             INT32 flag,
                             sdbCursorHandle *handle )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;
   BOOLEAN result   = FALSE ;
   sdbCursorStruct *cursor = NULL ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !cs->_collectionFullName[0] || !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( 1 == numToReturn )
   {
      flag |= FLG_QUERY_WITH_RETURNDATA ;
   }

   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              cs->_collectionFullName, flag, 0,
                              numToSkip, numToReturn, condition,
                              select, orderBy, hint, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, cs->_connection, cs, contextID ) ;
   ossMemcpy ( cursor->_collectionFullName, cs->_collectionFullName,
               sizeof(cursor->_collectionFullName) ) ;

   if ( ((MsgHeader*)cs->_pReceiveBuffer)->messageLength >
        (INT32)ossRoundUpToMultipleX( sizeof(MsgOpReply), 4 ) )
   {
      cursor->_pReceiveBuffer = cs->_pReceiveBuffer ;
      cs->_pReceiveBuffer = NULL ;
      cursor->_receiveBufferSize = cs->_receiveBufferSize ;
      cs->_receiveBufferSize = 0 ;
   }

   if ( -1 != contextID )
   {
      rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   }
   *handle = (sdbCursorHandle)cursor ;
done :
   return rc ;
error :
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbNext ( sdbCursorHandle cHandle,
                           bson *obj )
{
   INT32 rc = SDB_OK ;
   MsgOpReply *pReply = NULL ;
   bson localobj ;
   BOOLEAN bsoninit = FALSE;
   sdbCursorStruct *cs = (sdbCursorStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CURSOR ) ;
   if ( !obj )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( cs->_isClosed )
   {
      rc = SDB_DMS_CONTEXT_IS_CLOSE ;
      goto error ;
   }

   BSON_INIT( localobj ) ;

   /*
   if ( cs->_modifiedCurrent )
   {
      bson_destroy ( cs->_modifiedCurrent ) ;
      SDB_OSS_FREE ( cs->_modifiedCurrent ) ;
      cs->_modifiedCurrent = NULL ;
   }
   */
   if ( !cs->_pReceiveBuffer )
   {
      cs->_offset = -1 ;
      rc = _readNextBuffer ( cHandle ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   }
retry :
   pReply = (MsgOpReply*)cs->_pReceiveBuffer ;
   if ( -1 == cs->_offset )
   {
      cs->_offset = ossRoundUpToMultipleX ( sizeof(MsgOpReply), 4 ) ;
   }
   else
   {
      cs->_offset += ossRoundUpToMultipleX
            ( *(INT32*)&cs->_pReceiveBuffer[cs->_offset], 4 ) ;
   }
   if ( cs->_offset >= pReply->header.messageLength ||
        cs->_offset >= cs->_receiveBufferSize )
   {
      cs->_offset = -1 ;
      rc = _readNextBuffer ( cHandle ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      goto retry ;
   }
   rc = bson_init_finished_data ( &localobj, &cs->_pReceiveBuffer [ cs->_offset] ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_CORRUPTED_RECORD ;
      goto done ;
   }
   rc = bson_copy ( obj, &localobj ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto done ;
   }
   ++ cs->_totalRead ;
done :
   BSON_DESTROY( localobj ) ;
   return rc ;
error :
   if ( SDB_DMS_EOC == rc )
   {
      INT32 ret = SDB_OK ;
      cs->_contextID = -1 ;
      cs->_isClosed = TRUE ;
      ret = _unregCursor ( cs->_connection, cHandle ) ;
      if ( ret )
      {
         rc = ret ;
      }
   }
   goto done ;
}

SDB_EXPORT INT32 sdbCurrent ( sdbCursorHandle cHandle,
                              bson *obj )
{
   INT32 rc            = SDB_OK ;
   MsgOpReply *pReply  = NULL ;
   bson localobj ;
   BOOLEAN bsoninit = FALSE;
   sdbCursorStruct *cs = (sdbCursorStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CURSOR ) ;
   if ( cs->_isClosed )
   {
      rc = SDB_DMS_CONTEXT_IS_CLOSE ;
      goto error ;
   }

   if ( !obj )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( localobj );

   /*
   if(cs->_isDeleteCurrent)
   {
      rc = SDB_CURRENT_RECORD_DELETED ;
      goto error ;
   }
   */

   /*
   if ( cs->_modifiedCurrent )
   {
      bson_copy ( obj, cs->_modifiedCurrent ) ;
      goto done ;
   }
   */
   if ( !cs->_pReceiveBuffer )
   {
      cs->_offset = -1 ;
      rc = _readNextBuffer ( cHandle ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   }
retry :
   pReply = (MsgOpReply*)cs->_pReceiveBuffer ;
   if ( -1 == cs->_offset )
   {
      cs->_offset = ossRoundUpToMultipleX ( sizeof(MsgOpReply), 4 ) ;
   }

   if ( cs->_offset >= pReply->header.messageLength ||
        cs->_offset >= cs->_receiveBufferSize )
   {
      cs->_offset = -1 ;
      rc = _readNextBuffer ( cHandle ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      goto retry ;
   }
   rc = bson_init_finished_data ( &localobj, &cs->_pReceiveBuffer [ cs->_offset] ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_CORRUPTED_RECORD ;
      goto done ;
   }
   rc = bson_copy ( obj, &localobj ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   ++ cs->_totalRead ;
done :
   BSON_DESTROY( localobj ) ;
   return rc ;
error :
   if ( SDB_DMS_EOC == rc )
   {
      INT32 ret = SDB_OK ;
      cs->_contextID = -1 ;
      cs->_isClosed = TRUE ;
      ret = _unregCursor ( cs->_connection, cHandle ) ;
      if ( ret )
      {
         rc = ret ;
      }
   }
   goto done ;
}

/*
SDB_EXPORT INT32 sdbUpdateCurrent ( sdbCursorHandle cHandle,
                                    bson *rule )
{
   INT32 rc = SDB_OK ;
   bson obj ;
   bson_init ( &obj ) ;
   bson updateCondition ;
   bson_init ( &updateCondition ) ;
   bson_iterator it ;
   bson hintObj ;
   bson_init ( &hintObj ) ;
   bson_append_string ( &hintObj, "", CLIENT_RECORD_ID_INDEX ) ;
   bson_finish ( &hintObj ) ;

   CHAR *pSendBuffer       = NULL ;
   INT32 sendBufferSize    = 0 ;
   CHAR *pReceiveBuffer    = NULL ;
   INT32 receiveBufferSize = 0 ;

   bson modifiedObj ;
   bson_init ( &modifiedObj ) ;

   sdbCursorHandle tempQuery = SDB_INVALID_HANDLE ;
   sdbCursorStruct *cs = (sdbCursorStruct*)cHandle ;
   if ( !cs || cs->_handleType != SDB_HANDLE_TYPE_CURSOR )
   {
      rc = SDB_CLT_INVALID_HANDLE ;
      goto error ;
   }
   if ( cs->_collectionFullName[0] == '\0' )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }


   rc = sdbCurrent ( cHandle, &obj ) ;
   if ( rc )
   {
      goto error ;
   }
   if ( BSON_EOO != bson_find ( &it, &obj, CLIENT_RECORD_ID_FIELD ) )
   {
      rc = bson_append_element ( &updateCondition, NULL, &it ) ;
      if ( rc )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      rc = bson_finish ( &updateCondition ) ;
      if ( rc )
      {
         rc = SDB_SYS ;
         goto error ;
      }
   }
   else
   {
      rc = SDB_CORRUPTED_RECORD ;
      goto error ;
   }
   rc = _sdbUpdate ( cs->_sock, cs->_collectionFullName,
                     &pSendBuffer, &sendBufferSize,
                     &pReceiveBuffer, &receiveBufferSize,
                      cs->_endianConvert,
                      rule,
                     &updateCondition, &hintObj ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = _sdbQuery ( cs->_sock, cs->_collectionFullName, &pSendBuffer,
                    &sendBufferSize, &pReceiveBuffer, &receiveBufferSize,
                    cs->_endianConvert,
                    &updateCondition, NULL, NULL,
                    &hintObj, 0, 1, &tempQuery ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = sdbNext ( tempQuery, &modifiedObj ) ;
   if ( rc )
   {
      goto error ;
   }
   if ( !cs->_modifiedCurrent )
   {
      cs->_modifiedCurrent = (bson*)SDB_OSS_MALLOC ( sizeof(bson) ) ;
      if ( !cs->_modifiedCurrent )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      bson_init ( cs->_modifiedCurrent ) ;
   }
   rc = bson_copy ( cs->_modifiedCurrent, &modifiedObj ) ;
   if ( BSON_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
done :
   if ( SDB_INVALID_HANDLE != tempQuery )
   {
      sdbReleaseCursor ( tempQuery ) ;
      tempQuery = SDB_INVALID_HANDLE ;
   }
   if ( pSendBuffer )
   {
      SDB_OSS_FREE ( pSendBuffer ) ;
   }
   if ( pReceiveBuffer )
   {
      SDB_OSS_FREE ( pReceiveBuffer ) ;
   }
   bson_destroy ( &updateCondition ) ;
   bson_destroy ( &hintObj ) ;
   bson_destroy ( &modifiedObj ) ;
   return rc ;
error :
   goto done ;
}


SDB_EXPORT INT32 sdbDeleteCurrent ( sdbCursorHandle cHandle )
{
   INT32 rc = SDB_OK ;
   bson obj ;
   bson_init ( &obj ) ;
   bson updateCondition ;
   bson_init ( &updateCondition ) ;
   bson_iterator it ;
   bson hintObj ;
   bson_init ( &hintObj ) ;
   bson_append_string ( &hintObj, "", CLIENT_RECORD_ID_INDEX ) ;
   bson_finish ( &hintObj ) ;
   CHAR *pSendBuffer = NULL ;
   INT32 sendBufferSize = 0 ;
   CHAR *pReceiveBuffer = NULL ;
   INT32 receiveBufferSize = 0 ;
   sdbCursorStruct *cs = (sdbCursorStruct*)cHandle ;
   if ( !cs || cs->_handleType != SDB_HANDLE_TYPE_CURSOR )
   {
      rc = SDB_CLT_INVALID_HANDLE ;
      goto error ;
   }
   if ( cs->_collectionFullName[0] == '\0' )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if(cs->_isDeleteCurrent)
   {
      rc = SDB_CURRENT_RECORD_DELETED ;
      goto error ;
   }
   rc = sdbCurrent ( cHandle, &obj ) ;
   if ( rc )
   {
      goto error ;
   }
   if ( BSON_EOO != bson_find ( &it, &obj, CLIENT_RECORD_ID_FIELD ) )
   {
      rc = bson_append_element ( &updateCondition, NULL, &it ) ;
      if ( rc )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      rc = bson_finish ( &updateCondition ) ;
      if ( rc )
      {
         rc = SDB_SYS ;
         goto error ;
      }
   }
   else
   {
      rc = SDB_CORRUPTED_RECORD ;
      goto error ;
   }
   rc = _sdbDelete ( cs->_sock, cs->_collectionFullName,
                     &pSendBuffer, &sendBufferSize,
                     &pReceiveBuffer, &receiveBufferSize,
                     cs->_endianConvert,
                     &updateCondition, &hintObj ) ;
   if ( rc )
   {
      goto error ;
   }
   if ( cs->_modifiedCurrent )
   {
      bson_destroy ( cs->_modifiedCurrent ) ;
      SDB_OSS_FREE ( cs->_modifiedCurrent ) ;
      cs->_modifiedCurrent = NULL ;
   }
done :
   if ( pSendBuffer )
   {
      SDB_OSS_FREE ( pSendBuffer ) ;
   }
   if ( pReceiveBuffer )
   {
      SDB_OSS_FREE ( pReceiveBuffer ) ;
   }
   bson_destroy ( &updateCondition ) ;
   bson_destroy ( &hintObj ) ;
   cs->_isDeleteCurrent = TRUE ;
   return rc ;
error :
   goto done ;
}
*/

SDB_EXPORT INT32 sdbCloseCursor ( sdbCursorHandle cHandle )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN result   = FALSE ;
   SINT64 contextID = -1 ;
   sdbCursorStruct *cs = (sdbCursorStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CURSOR ) ;

   if ( cs->_isClosed )
   {
      goto done ;
   }
   if ( -1 == cs->_sock || -1 == cs->_contextID )
   {
      cs->_isClosed = TRUE ;
      goto done ;
   }

   rc = clientBuildKillContextsMsg ( &cs->_pSendBuffer,
                                     &cs->_sendBufferSize, 0, 1,
                                     &cs->_contextID, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock,
                (MsgHeader*)cs->_pSendBuffer, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID,
                       &result, cs->_endianConvert ) ;
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
   _unregCursor ( cs->_connection, cHandle ) ;
   cs->_contextID = -1 ;
   cs->_isClosed = TRUE ;

   if ( SDB_OK != rc )
   {
      goto error ;
   }

done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbCloseAllCursors ( sdbConnectionHandle cHandle )
{
   INT32 rc            = SDB_OK ;
   Node *cursorHandles = NULL ;
   Node *p             = NULL ;
   sdbConnectionStruct *cs = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CONNECTION ) ;

   cursorHandles = cs->_cursors ;
   while ( cursorHandles )
   {
      p = cursorHandles ;
      rc = sdbCloseCursor( p->data ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      cursorHandles = cursorHandles->next ;
   }
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbIsValid( sdbConnectionHandle cHandle, BOOLEAN *result )
{
   INT32 rc    = SDB_OK ;
   INT32 ret   = SDB_OK ;
   SOCKET sock = 0 ;
   fd_set fds ;
   struct timeval maxSelectTime = { 0, 1000 };
   sdbConnectionStruct *connection = (sdbConnectionStruct *)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   if ( !result )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   sock = connection->_sock ;
   if ( sock < 0 )
   {
      *result = FALSE ;
      goto done ;
   }
   while ( TRUE )
   {
      FD_ZERO ( &fds ) ;
      FD_SET ( sock, &fds ) ;
      ret = select ( sock+1, &fds, NULL,  NULL, &maxSelectTime ) ;
      if ( !ret )
      {
         *result = TRUE ;
         goto done ;
      }
      else if ( ret < 0 )
      {
         if (
#if defined (_WINDOWS)
         WSAEINTR
#else
         EINTR
#endif
         == errno )
         {
            continue ;
         }
         rc = SDB_NETWORK ;
         goto error ;
      }
      if ( FD_ISSET ( sock, &fds ) )
      {

         *result = FALSE ;
         goto done ;
      }
      else
      {
          rc = SDB_NETWORK ;
          goto error ;
      }
      break ;
   }
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbTraceStart ( sdbConnectionHandle cHandle,
                                 UINT32 traceBufferSize,
                                 CHAR * comp,
                                 CHAR * breakPoint )
{
   INT32 rc       = SDB_OK ;
   BOOLEAN result = FALSE ;
   bson obj ;
   BOOLEAN bsoninit = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   BSON_INIT( obj );
   BSON_APPEND( obj, FIELD_NAME_SIZE, (INT64)traceBufferSize, long ) ;

   if ( comp )
   {
      rc = bson_append_start_array ( &obj, FIELD_NAME_COMPONENTS ) ;
      if( rc )
      {
         rc = SDB_SYS;
         goto error;
      }
      rc = sdbTraceStrtok ( &obj, comp ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      rc = bson_append_finish_array( &obj );
      if ( rc )
      {
         rc = SDB_SYS ;
         goto error ;
      }
   }

   if ( breakPoint )
   {
      rc = bson_append_start_array( &obj, FIELD_NAME_BREAKPOINTS );
      if( rc )
      {
         rc = SDB_SYS;
         goto error;
      }
      rc = sdbTraceStrtok ( &obj, breakPoint ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      rc = bson_append_finish_array( &obj );
      if ( rc )
      {
         rc = SDB_SYS ;
         goto error ;
      }
   }
   BSON_FINISH ( obj ) ;
   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      CMD_ADMIN_PREFIX CMD_NAME_TRACE_START, &result, &obj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( obj ) ;
   return rc ;
error :
   goto done ;
}


SDB_EXPORT INT32 sdbTraceResume ( sdbConnectionHandle cHandle )
{
   INT32 rc       = SDB_OK ;
   BOOLEAN result = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      CMD_ADMIN_PREFIX CMD_NAME_TRACE_RESUME, &result, NULL,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbTraceStop ( sdbConnectionHandle cHandle,
                                const CHAR *pDumpFileName )
{
   INT32 rc       = SDB_OK ;
   BOOLEAN result = FALSE ;
   bson obj ;
   BOOLEAN bsoninit = FALSE;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   BSON_INIT( obj ) ;
   if ( pDumpFileName )
   {
      BSON_APPEND( obj, FIELD_NAME_FILENAME, pDumpFileName, string ) ;
   }
   BSON_FINISH ( obj ) ;
   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      CMD_ADMIN_PREFIX CMD_NAME_TRACE_STOP, &result, &obj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( obj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbTraceStatus ( sdbConnectionHandle cHandle,
                                  sdbCursorHandle *handle )
{
   INT32 rc         = SDB_OK ;
   BOOLEAN r        = FALSE;
   SINT64 contextID = 0 ;
   sdbCursorStruct *cursor = NULL;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                              &connection->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_TRACE_STATUS,
                              0, 0, 0, -1, NULL, NULL, NULL,
                              NULL, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize,
                       &contextID, &r,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, connection, connection, contextID );

   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *handle = (sdbCursorHandle)cursor ;
done :
   return rc ;
error :
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}


SDB_EXPORT INT32 sdbExecUpdate( sdbConnectionHandle cHandle,
                                const CHAR *sql )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;
   BOOLEAN result   = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !sql )
   {
      rc = SDB_INVALIDARG ;
     goto error ;
   }

   rc = clientValidateSql( sql, FALSE ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = clientBuildSqlMsg( &connection->_pSendBuffer,
                           &connection->_sendBufferSize, sql,
                           0, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize,
                       &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbExec( sdbConnectionHandle cHandle,
                          const CHAR *sql,
                          sdbCursorHandle *result )
{
   INT32 rc                = SDB_OK ;
   SINT64 contextID        = 0 ;
   sdbCursorStruct *cursor = NULL ;
   BOOLEAN r               = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !result || !sql )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientValidateSql( sql, TRUE ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = clientBuildSqlMsg( &connection->_pSendBuffer,
                           &connection->_sendBufferSize, sql,
                           0, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize,
                       &contextID, &r,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, connection, connection, contextID ) ;
   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *result = (sdbCursorHandle)cursor ;
done :
   return rc ;
error :
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   if ( result )
   {
      *result = SDB_INVALID_HANDLE ;
   }
   goto done ;
}

SDB_EXPORT INT32 sdbTransactionBegin( sdbConnectionHandle cHandle )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;
   BOOLEAN result   = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   rc = clientBuildTransactionBegMsg( &connection->_pSendBuffer,
                                      &connection->_sendBufferSize,
                                      0, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize,
                       &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbTransactionCommit( sdbConnectionHandle cHandle )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;
   BOOLEAN result   = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   rc = clientBuildTransactionCommitMsg( &connection->_pSendBuffer,
                                         &connection->_sendBufferSize,
                                         0, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize,
                       &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbTransactionRollback( sdbConnectionHandle cHandle )
{
   INT32 rc         = SDB_OK ;
   SINT64 contextID = 0 ;
   BOOLEAN result   = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   rc = clientBuildTransactionRollbackMsg( &connection->_pSendBuffer,
                                           &connection->_sendBufferSize,
                                           0, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize,
                       &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   return rc ;
error :
   goto done ;
}

SDB_EXPORT void sdbReleaseConnection ( sdbConnectionHandle cHandle )
{
   INT32 rc = SDB_OK ;
   sdbConnectionStruct *cs = (sdbConnectionStruct*)cHandle ;

   CLIENT_UNUSED( rc ) ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CONNECTION ) ;

   if ( cs->_pSendBuffer )
   {
      SDB_OSS_FREE ( cs->_pSendBuffer ) ;
   }
   if ( cs->_pReceiveBuffer )
   {
      SDB_OSS_FREE ( cs->_pReceiveBuffer ) ;
   }
   SDB_OSS_FREE ( (sdbConnectionStruct*)cHandle ) ;

done :
   return ;
error :
   goto done ;
}

SDB_EXPORT void sdbReleaseCollection ( sdbCollectionHandle cHandle )
{
   INT32 rc = SDB_OK ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;

   CLIENT_UNUSED( rc ) ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;

   _unregSocket( cs->_connection, &cs->_sock ) ;

   if ( cs->_pSendBuffer )
   {
      SDB_OSS_FREE ( cs->_pSendBuffer ) ;
   }
   if ( cs->_pReceiveBuffer )
   {
      SDB_OSS_FREE ( cs->_pReceiveBuffer ) ;
   }
   SDB_OSS_FREE ( (sdbCollectionStruct*)cHandle ) ;

done :
   return ;
error :
   goto done ;
}

SDB_EXPORT void sdbReleaseCS ( sdbCSHandle cHandle )
{
   INT32 rc = SDB_OK ;
   sdbCSStruct *cs = (sdbCSStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CS ) ;

   CLIENT_UNUSED( rc ) ;

   _unregSocket( cs->_connection, &cs->_sock ) ;

   if ( cs->_pSendBuffer )
   {
      SDB_OSS_FREE (cs->_pSendBuffer ) ;
   }
   if ( cs->_pReceiveBuffer )
   {
      SDB_OSS_FREE ( cs->_pReceiveBuffer ) ;
   }
   SDB_OSS_FREE ( (sdbCSStruct*)cHandle ) ;
done :
   return ;
error :
   goto done ;
}

SDB_EXPORT void sdbReleaseReplicaGroup ( sdbReplicaGroupHandle cHandle )
{
   INT32 rc = SDB_OK ;
   sdbRGStruct *rg = (sdbRGStruct*)cHandle ;
   HANDLE_CHECK( cHandle, rg, SDB_HANDLE_TYPE_REPLICAGROUP ) ;

   CLIENT_UNUSED( rc ) ;

   _unregSocket( rg->_connection, &rg->_sock ) ;

   if ( rg->_pSendBuffer )
   {
      SDB_OSS_FREE ( rg->_pSendBuffer ) ;
   }
   if ( rg->_pReceiveBuffer )
   {
      SDB_OSS_FREE ( rg->_pReceiveBuffer ) ;
   }
   SDB_OSS_FREE ( (sdbRGStruct*)cHandle ) ;
done :
   return ;
error :
   goto done ;
}

SDB_EXPORT void sdbReleaseNode ( sdbNodeHandle cHandle )
{
   INT32 rc = SDB_OK ;
   sdbRNStruct *rn = (sdbRNStruct*)cHandle ;
   HANDLE_CHECK( cHandle, rn, SDB_HANDLE_TYPE_REPLICANODE ) ;

   CLIENT_UNUSED( rc ) ;

   _unregSocket( rn->_connection, &rn->_sock ) ;

   if ( rn->_pSendBuffer )
   {
      SDB_OSS_FREE ( rn->_pSendBuffer ) ;
   }
   if ( rn->_pReceiveBuffer )
   {
      SDB_OSS_FREE ( rn->_pReceiveBuffer ) ;
   }
   SDB_OSS_FREE ( (sdbRNStruct*)cHandle ) ;
done :
   return ;
error :
   goto done ;
}

SDB_EXPORT void sdbReleaseDomain ( sdbDomainHandle cHandle )
{
   INT32 rc = SDB_OK ;
   sdbDomainStruct *s = (sdbDomainStruct*)cHandle ;

   CLIENT_UNUSED( rc ) ;

   HANDLE_CHECK( cHandle, s, SDB_HANDLE_TYPE_DOMAIN ) ;

   _unregSocket( s->_connection, &s->_sock ) ;

   if ( s->_pSendBuffer )
   {
      SDB_OSS_FREE ( s->_pSendBuffer ) ;
   }
   if ( s->_pReceiveBuffer )
   {
      SDB_OSS_FREE ( s->_pReceiveBuffer ) ;
   }
   SDB_OSS_FREE ( (sdbDomainStruct*)cHandle ) ;
done :
   return ;
error :
   goto done ;
}

SDB_EXPORT void sdbReleaseCursor ( sdbCursorHandle cHandle )
{
   INT32 rc = SDB_OK ;
   sdbCursorStruct *cs = (sdbCursorStruct*)cHandle ;

   CLIENT_UNUSED( rc ) ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_CURSOR ) ;

   sdbCloseCursor( cHandle ) ;

   if ( !cs->_isClosed )
   {
      _unregCursor ( cs->_connection, cHandle ) ;
   }
   if ( cs->_pSendBuffer )
   {
      SDB_OSS_FREE ( cs->_pSendBuffer ) ;
   }
   if ( cs->_pReceiveBuffer )
   {
      SDB_OSS_FREE ( cs->_pReceiveBuffer ) ;
   }
   /*
   if ( cs->_modifiedCurrent )
   {
      bson_destroy ( cs->_modifiedCurrent ) ;
      SDB_OSS_FREE ( cs->_modifiedCurrent ) ;
      cs->_modifiedCurrent = NULL ;
   }
   */
   /*
   if( cs->_isDeleteCurrent )
   {
     cs->_isDeleteCurrent = FALSE ;
   }
   */
   SDB_OSS_FREE ( (sdbCursorStruct*)cHandle ) ;
done :
   return ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbAggregate ( sdbCollectionHandle cHandle,
                                bson **obj, SINT32 num,
                                sdbCursorHandle *handle )
{
   INT32 rc                = SDB_OK ;
   SINT64 contextID        = -1 ;
   BOOLEAN result          = FALSE ;
   SINT32 count            = 0 ;
   sdbCursorStruct *cursor = NULL ;
   sdbCollectionStruct *sdbCL = (sdbCollectionStruct *)cHandle ;

   HANDLE_CHECK( cHandle, sdbCL, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !obj || num <=0 || !handle || !sdbCL->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   for ( count = 0; count < num; ++count )
   {
      if ( !obj[count] )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( 0 == count )
         rc = clientBuildAggrRequest ( &sdbCL->_pSendBuffer,
                                       &sdbCL->_sendBufferSize,
                                       sdbCL->_collectionFullName, obj[count],
                                       sdbCL->_endianConvert ) ;
      else
         rc = clientAppendAggrRequest ( &sdbCL->_pSendBuffer,
                                        &sdbCL->_sendBufferSize,
                                        obj[count], sdbCL->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   }
   rc = _send ( sdbCL->_connection, sdbCL->_sock,
                (MsgHeader *)sdbCL->_pSendBuffer,
                sdbCL->_endianConvert );
   if ( SDB_OK != rc )
   {
      goto error;
   }
   rc = _recvExtract( sdbCL->_connection, sdbCL->_sock,
                      (MsgHeader **)&sdbCL->_pReceiveBuffer,
                      &sdbCL->_receiveBufferSize, &contextID, &result,
                      sdbCL->_endianConvert );
   if ( SDB_OK != rc )
   {
      goto error;
   }
   CHECK_RET_MSGHEADER( sdbCL->_pSendBuffer, sdbCL->_pReceiveBuffer,
                        sdbCL->_connection ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct );
   INIT_CURSOR( cursor, sdbCL->_connection, sdbCL, contextID );
   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *handle = (sdbCursorHandle)cursor;
done :
   return rc ;
error :
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbAttachCollection ( sdbCollectionHandle cHandle,
                                       const CHAR *subClFullName,
                                       bson *options )
{
   INT32 rc                      = SDB_OK ;
   BOOLEAN result                = FALSE ;
   SINT64 contextID              = 0 ;
   bson newObj ;
   BOOLEAN bsoninit              = TRUE ;
   bson_iterator it ;
   sdbCollectionStruct *cs       = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !subClFullName || !options ||
        ossStrlen ( subClFullName) > CLIENT_COLLECTION_NAMESZ ||
        !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj );
   BSON_APPEND( newObj, FIELD_NAME_NAME, cs->_collectionFullName, string ) ;
   BSON_APPEND( newObj, FIELD_NAME_SUBCLNAME, subClFullName, string ) ;

   bson_iterator_init ( &it, options ) ;
   while ( BSON_EOO != ( bson_iterator_next ( &it ) ) )
   {
      BSON_APPEND ( newObj, NULL, &it, element ) ;
   }
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_LINK_CL,
                              0, 0, 0, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbDetachCollection( sdbCollectionHandle cHandle,
                                      const CHAR *subClFullName)
{
   INT32 rc                      = SDB_OK ;
   BOOLEAN result                = FALSE ;
   SINT64 contextID              = 0 ;
   bson newObj ;
   BOOLEAN bsoninit              = FALSE ;
   sdbCollectionStruct *cs       = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;
   if ( !subClFullName ||
        ossStrlen ( subClFullName) > CLIENT_COLLECTION_NAMESZ ||
        !cs->_collectionFullName[0] )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, FIELD_NAME_NAME, cs->_collectionFullName, string ) ;
   BSON_APPEND( newObj, FIELD_NAME_SUBCLNAME, subClFullName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_UNLINK_CL,
                              0, 0, 0, -1, &newObj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbBackupOffline ( sdbConnectionHandle cHandle,
                                    bson *options )
{
   INT32 rc                      = SDB_OK ;
   BOOLEAN result                = FALSE ;
   SINT64 contextID              = 0 ;
   bson newObj ;
   BOOLEAN bsoninit              = FALSE ;
   bson_iterator it ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   BSON_INIT( newObj ) ;
   if ( options )
   {
      bson_iterator_init ( &it, options ) ;
      while ( BSON_EOO != bson_iterator_next ( &it ) )
      {
         BSON_APPEND( newObj, NULL, &it, element ) ;
      }
   }
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                              &connection->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_BACKUP_OFFLINE,
                              0, 0, 0, -1, &newObj,
                              NULL, NULL, NULL, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbListBackup ( sdbConnectionHandle cHandle,
                                 bson *options,
                                 bson *condition,
                                 bson *selector,
                                 bson *orderBy,
                                 sdbCursorHandle *handle )
{
   INT32 rc                      = SDB_OK ;
   BOOLEAN result                = FALSE ;
   SINT64 contextID              = 0 ;
   sdbCursorStruct *cursor       = NULL ;
   bson newObj ;
   BOOLEAN bsoninit              = FALSE ;
   bson_iterator it ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !handle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   if ( options )
   {
      bson_iterator_init ( &it, options ) ;
      while ( BSON_EOO != bson_iterator_next ( &it ) )
      {
         BSON_APPEND( newObj, NULL, &it, element ) ;
      }
   }
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                              &connection->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_LIST_BACKUPS,
                              0, 0, 0, -1, condition,
                              selector, orderBy, &newObj,
                              connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
   ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
   INIT_CURSOR( cursor, connection, connection, contextID ) ;

   rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   *handle = (sdbCursorHandle)cursor ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbRemoveBackup ( sdbConnectionHandle cHandle,
                                   bson* options )
{
   INT32 rc                      = SDB_OK ;
   BOOLEAN result                = FALSE ;
   SINT64 contextID              = 0 ;
   bson newObj ;
   BOOLEAN bsoninit              = FALSE ;
   bson_iterator it ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   BSON_INIT( newObj ) ;
   if ( options )
   {
      bson_iterator_init ( &it, options ) ;
      while ( BSON_EOO != bson_iterator_next ( &it ) )
      {
         BSON_APPEND( newObj, NULL, &it, element ) ;
      }
   }
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                                &connection->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_REMOVE_BACKUP,
                              0, 0, 0, -1, &newObj,
                              NULL, NULL, NULL, connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbListTasks ( sdbConnectionHandle cHandle,
                                bson *condition,
                                bson *selector,
                                bson *orderBy,
                                bson *hint,
                                sdbCursorHandle *handle )
{
   return sdbGetList ( cHandle, SDB_LIST_TASKS, condition,
                       selector, orderBy, handle ) ;
}

SDB_EXPORT INT32 sdbWaitTasks ( sdbConnectionHandle cHandle,
                                const SINT64 *taskIDs,
                                SINT32 num )
{
   INT32 rc                      = SDB_OK ;
   BOOLEAN result                = FALSE ;
   SINT64 contextID              = 0 ;
   SINT32 i                      = 0 ;
   bson newObj ;
   BOOLEAN bsoninit              = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*) cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !taskIDs || num < 0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   rc = bson_append_start_object ( &newObj, FIELD_NAME_TASKID ) ;
   if ( rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   rc = bson_append_start_array ( &newObj, "$in" ) ;
   if ( rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   for ( i = 0; i < num; i++ )
   {
      BSON_APPEND( newObj, "", taskIDs[i], long ) ;
   }
   rc = bson_append_finish_array ( &newObj ) ;
   if ( rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   rc = bson_append_finish_object ( &newObj ) ;
   if ( rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                              &connection->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_WAITTASK,
                              0, 0, 0, -1,
                              &newObj, NULL, NULL, NULL,
                              connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbCancelTask ( sdbConnectionHandle cHandle,
                                 SINT64 taskID,
                                 BOOLEAN isAsync )
{
   INT32 rc                      = SDB_OK ;
   BOOLEAN result                = FALSE ;
   SINT64 contextID              = 0 ;
   bson newObj ;
   BOOLEAN bsoninit              = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*) cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( taskID <= 0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND ( newObj, FIELD_NAME_TASKID, taskID, long ) ;
   BSON_APPEND ( newObj, FIELD_NAME_ASYNC, isAsync, bool ) ;
   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                              &connection->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_CANCEL_TASK,
                              0, 0, 0, -1,
                              &newObj, NULL, NULL, NULL,
                              connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbSetSessionAttr ( sdbConnectionHandle cHandle,
                                     bson *options )
{
   INT32 rc              = SDB_OK ;
   BOOLEAN result        = FALSE ;
   SINT64 contextID      = 0 ;
   const CHAR *key       = NULL ;
   INT32 value           = PREFER_REPL_TYPE_MAX ;
   const CHAR *str_value = NULL ;
   bson newObj ;
   bson_iterator it ;
   BOOLEAN bsoninit      = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*) cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !options )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   bson_iterator_init ( &it, options ) ;
   while ( BSON_EOO != bson_iterator_next( &it ) )
   {
      key = bson_iterator_key( &it ) ;
      if ( strcmp( FIELD_NAME_PREFERED_INSTANCE, key ) )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      switch ( bson_iterator_type( &it ) )
      {
         case BSON_STRING :
            str_value = bson_iterator_string ( &it ) ;
            if ( !ossStrcasecmp("M", str_value) )        // master
               value = PREFER_REPL_MASTER ;
            else if ( !ossStrcasecmp( "S", str_value ) ) // slave
               value = PREFER_REPL_SLAVE ;
            else if ( !ossStrcasecmp( "A", str_value ) ) // anyone
               value = PREFER_REPL_ANYONE ;
            else
            {
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            break ;
         case BSON_INT :
            value = bson_iterator_int ( &it ) ;
            if ( value < PREFER_REPL_NODE_1 || value > PREFER_REPL_NODE_7 )
            {
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            break ;
         default :
            rc = SDB_INVALIDARG ;
            goto error ;
      }
      BSON_APPEND( newObj, key, value, int ) ;
      break ;
   } // while

   BSON_FINISH ( newObj ) ;
   rc = clientBuildQueryMsg ( &connection->_pSendBuffer,
                              &connection->_sendBufferSize,
                              CMD_ADMIN_PREFIX CMD_NAME_SETSESS_ATTR,
                              0, 0, 0, -1,
                              &newObj, NULL, NULL, NULL,
                              connection->_endianConvert) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 _sdbMsg ( sdbConnectionHandle cHandle, const CHAR *msg )
{
   INT32 rc              = SDB_OK ;
   BOOLEAN result        = FALSE ;
   SINT64 contextID      = 0 ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*) cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !msg )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientBuildTestMsg ( &connection->_pSendBuffer,
                             &connection->_sendBufferSize,
                             msg, 0, connection->_endianConvert) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cHandle, connection->_sock,
                (MsgHeader*)connection->_pSendBuffer,
                connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _recvExtract ( cHandle, connection->_sock,
                       (MsgHeader**)&connection->_pReceiveBuffer,
                       &connection->_receiveBufferSize, &contextID, &result,
                       connection->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( connection->_pSendBuffer, connection->_pReceiveBuffer,
                        cHandle ) ;
done :
   return rc ;
error :
   goto done ;
}

#define INIT_DOMAINHANDLE( dhandle, cHandle, pDoaminName )                  \
do                                                                          \
{                                                                           \
   dhandle->_handleType    = SDB_HANDLE_TYPE_DOMAIN ;                       \
   dhandle->_connection    = ( sdbConnectionHandle )cHandle ;               \
   dhandle->_sock          = cHandle->_sock ;                               \
   dhandle->_endianConvert = cHandle->_endianConvert ;                      \
   ossStrncpy ( dhandle->_domainName, pDomainName, CLIENT_DOMAIN_NAMESZ ) ; \
}while(0)

SDB_EXPORT INT32 sdbCreateDomain ( sdbConnectionHandle cHandle,
                                   const CHAR *pDomainName,
                                   bson *options,
                                   sdbDomainHandle *handle )
{
   INT32 rc                     = SDB_OK ;
   BOOLEAN result               = FALSE ;
   CHAR *pCreateDomain          = CMD_ADMIN_PREFIX CMD_NAME_CREATE_DOMAIN ;
   sdbDomainStruct *s           = NULL ;
   CHAR *pName                  = FIELD_NAME_NAME ;
   CHAR *pOptions               = FIELD_NAME_OPTIONS ;
   bson newObj ;
   BOOLEAN bsoninit             = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pDomainName || !handle ||
        ossStrlen ( pDomainName ) > CLIENT_DOMAIN_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj );
   BSON_APPEND( newObj, pName, pDomainName, string ) ;
   if ( options )
   {
      BSON_APPEND( newObj, pOptions, options, bson ) ;
   }
   BSON_FINISH ( newObj ) ;
   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pCreateDomain, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   ALLOC_HANDLE( s, sdbDomainStruct ) ;
   INIT_DOMAINHANDLE( s, connection, pDomainName ) ;
   _regSocket( cHandle, &s->_sock ) ;

   *handle = (sdbDomainHandle)s ;
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbDropDomain ( sdbConnectionHandle cHandle,
                                 const CHAR *pDomainName )
{
   INT32 rc               = SDB_OK ;
   BOOLEAN result         = FALSE ;
   CHAR *pDropDomain      = CMD_ADMIN_PREFIX CMD_NAME_DROP_DOMAIN ;
   CHAR *pName            = FIELD_NAME_NAME ;
   bson newObj ;
   BOOLEAN bsoninit       = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pDomainName ||
        ossStrlen ( pDomainName) > CLIENT_DOMAIN_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, pName, pDomainName, string );
   BSON_FINISH ( newObj ) ;
   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      pDropDomain, &result, &newObj,
                      NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   BSON_DESTROY( newObj ) ;
   return rc ;
error :
   goto done ;
}

SDB_EXPORT INT32 sdbGetDomain ( sdbConnectionHandle cHandle,
                                const CHAR *pDomainName,
                                sdbDomainHandle *handle )
{
   INT32 rc                 = SDB_OK ;
   sdbCursorHandle cursor   = SDB_INVALID_HANDLE ;
   CHAR *pName              = FIELD_NAME_NAME ;
   sdbDomainStruct *s       = NULL ;
   bson newObj ;
   bson result ;
   BOOLEAN bsoninit         = FALSE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   if ( !pDomainName || !handle ||
        ossStrlen ( pDomainName ) > CLIENT_DOMAIN_NAMESZ )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_INIT( result ) ;
   BSON_APPEND( newObj, pName, pDomainName, string ) ;
   BSON_FINISH ( newObj ) ;

   rc = sdbGetList ( cHandle, SDB_LIST_DOMAINS, &newObj, NULL, NULL,
                     &cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   if ( SDB_OK == ( rc = sdbNext ( cursor, &result ) ) )
   {
      ALLOC_HANDLE( s, sdbDomainStruct ) ;
      INIT_DOMAINHANDLE( s, connection, pDomainName ) ;
      _regSocket( cHandle, &s->_sock ) ;
      *handle = (sdbDomainHandle) s ;
   }
   else if ( SDB_DMS_EOC != rc )
   {
      goto error ;
   }
   else
   {
      rc = SDB_CAT_DOMAIN_NOT_EXIST ;
      goto error ;
   }
done :
   if ( SDB_INVALID_HANDLE != cursor )
   {
      sdbReleaseCursor ( cursor ) ;
   }

   BSON_DESTROY( newObj ) ;
   BSON_DESTROY( result ) ;
   return rc ;
error :
   if ( s )
   {
      SDB_OSS_FREE( s );
   }

   SET_INVALID_HANDLE( handle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbListDomains ( sdbConnectionHandle cHandle,
                                  bson *condition,
                                  bson *selector,
                                  bson *orderBy,
                                  sdbCursorHandle *handle )
{
   return sdbGetList ( cHandle, SDB_LIST_DOMAINS, condition,
                       selector, orderBy, handle ) ;
}

SDB_EXPORT INT32 sdbAlterDomain( sdbDomainHandle cHandle,
                                 const bson *options )
{
   INT32 rc                = SDB_OK ;
   const CHAR *command     = CMD_ADMIN_PREFIX CMD_NAME_ALTER_DOMAIN ;
   sdbDomainStruct *domain = ( sdbDomainStruct * )cHandle  ;
   BOOLEAN ret             = TRUE ;
   bson newObj ;
   BOOLEAN bsoninit        = FALSE ;

   HANDLE_CHECK( cHandle, domain, SDB_HANDLE_TYPE_DOMAIN ) ;
   if ( !options )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( newObj ) ;
   BSON_APPEND( newObj, FIELD_NAME_NAME, domain->_domainName, string ) ;
   BSON_APPEND( newObj, FIELD_NAME_OPTIONS, options, bson) ;
   BSON_FINISH ( newObj ) ;

   rc = _runCommand( domain->_connection, domain->_sock,
                     &( domain->_pSendBuffer ),
                     &( domain->_sendBufferSize ),
                     &( domain->_pReceiveBuffer ),
                     &( domain->_receiveBufferSize ),
                     domain->_endianConvert,
                     command, &ret, &newObj,
                     NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   BSON_DESTROY( newObj ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbListCollectionSpacesInDomain( sdbDomainHandle cHandle,
                                                  sdbCursorHandle *cursor )
{
   INT32 rc         = SDB_OK ;
   sdbDomainStruct *s = ( sdbDomainStruct * )cHandle ;
   bson condition ;
   bson selector ;
   BOOLEAN bsoninit = FALSE ;

   HANDLE_CHECK( cHandle, s, SDB_HANDLE_TYPE_DOMAIN ) ;
   if ( !cursor )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( condition ) ;
   BSON_INIT( selector ) ;
   BSON_APPEND( condition, FIELD_NAME_DOMAIN, s->_domainName, string ) ;
   BSON_FINISH( condition ) ;
   BSON_APPEND_NULL( selector, FIELD_NAME_NAME ) ;
   BSON_FINISH( selector ) ;

   rc = sdbGetList ( s->_connection, SDB_LIST_CS_IN_DOMAIN, &condition,
                     &selector, NULL, cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error;
   }
done:
   BSON_DESTROY( condition );
   BSON_DESTROY( selector );
   return rc ;
error:
   SET_INVALID_HANDLE( cursor ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbListCollectionsInDomain( sdbDomainHandle cHandle,
                                             sdbCursorHandle *cursor )
{
   INT32 rc           = SDB_OK ;
   sdbDomainStruct *s = ( sdbDomainStruct * )cHandle ;
   bson condition ;
   bson selector ;
   BOOLEAN bsoninit   = FALSE ;

   HANDLE_CHECK( cHandle, s, SDB_HANDLE_TYPE_DOMAIN ) ;
   if ( !cursor )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( condition ) ;
   BSON_INIT( selector ) ;
   BSON_APPEND( condition, FIELD_NAME_DOMAIN, s->_domainName, string) ;
   BSON_FINISH( condition ) ;

   BSON_APPEND_NULL( selector, FIELD_NAME_NAME ) ;
   BSON_FINISH( selector ) ;

   rc = sdbGetList ( s->_connection, SDB_LIST_CL_IN_DOMAIN, &condition,
                     &selector, NULL, cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   BSON_DESTROY( condition );
   BSON_DESTROY( selector );
   return rc ;
error:
   SET_INVALID_HANDLE( cursor ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbListGroupsInDomain( sdbDomainHandle cHandle,
                                        sdbCursorHandle *cursor )
{
   INT32 rc = SDB_OK ;
   sdbDomainStruct *s = ( sdbDomainStruct * )cHandle ;
   sdbConnectionHandle conn = SDB_INVALID_HANDLE ;
   bson condition ;
   BOOLEAN bsoninit = FALSE ;

   HANDLE_CHECK( cHandle, s, SDB_HANDLE_TYPE_DOMAIN ) ;
   if ( !cursor )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   BSON_INIT( condition ) ;
   BSON_APPEND( condition, FIELD_NAME_NAME, s->_domainName, string) ;
   BSON_FINISH( condition ) ;

   conn = s->_connection ;
   rc = sdbListDomains( conn, &condition, NULL, NULL, cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   BSON_DESTROY( condition ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbInvalidateCache( sdbConnectionHandle cHandle,
                                     bson *condition )
{
   INT32 rc = SDB_OK ;
   BOOLEAN result = TRUE ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;
   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      (CMD_ADMIN_PREFIX CMD_NAME_INVALIDATE_CACHE),
                      &result, condition, NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbForceSession( sdbConnectionHandle cHandle,
                                  SINT64 sessionID )
{
   INT32 rc = SDB_OK ;
   BOOLEAN result = TRUE ;
   BOOLEAN bsoninit = FALSE ;
   bson query ;
   sdbConnectionStruct *connection = (sdbConnectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, connection, SDB_HANDLE_TYPE_CONNECTION ) ;

   BSON_INIT( query ) ;
   BSON_APPEND( query, FIELD_NAME_SESSIONID, sessionID, long ) ;
   BSON_FINISH( query ) ;
   rc = _runCommand ( cHandle, connection->_sock, &connection->_pSendBuffer,
                      &connection->_sendBufferSize,
                      &connection->_pReceiveBuffer,
                      &connection->_receiveBufferSize,
                      connection->_endianConvert,
                      (CMD_ADMIN_PREFIX CMD_NAME_FORCE_SESSION),
                      &result, &query, NULL, NULL, NULL ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   BSON_DESTROY( query ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbOpenLob( sdbCollectionHandle cHandle,
                             const bson_oid_t *oid,
                             INT32 mode,
                             sdbLobHandle* lobHandle )
{
   INT32 rc = SDB_OK ;
   bson obj ;
   SINT64 contextID = -1 ;
   BOOLEAN result = TRUE ;
   sdbLobStruct *lobStruct = NULL ;
   const CHAR *bsonBuf = NULL ;
   bson_iterator bsonItr ;
   bson_type bType = BSON_EOO ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   bson_init( &obj ) ;

   if ( NULL == oid )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;

   if ( !cs->_collectionFullName[0] || NULL == lobHandle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( SDB_LOB_CREATEONLY != mode && 
        SDB_LOB_READ != mode )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = bson_append_string( &obj, FIELD_NAME_COLLECTION, cs->_collectionFullName ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   rc = bson_append_oid( &obj, FIELD_NAME_LOB_OID, oid ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   
   rc = bson_append_int( &obj, FIELD_NAME_LOB_OPEN_MODE, mode ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
 
   bson_finish( &obj ) ;

   rc = clientBuildOpenLobMsg( &cs->_pSendBuffer, &cs->_sendBufferSize,
                               &obj, 0, 1, 0, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
   bsonBuf = cs->_pReceiveBuffer + sizeof( MsgOpReply ) ;
   if ( BSON_OK != bson_init_finished_data( &obj, bsonBuf ) )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   ALLOC_HANDLE( lobStruct, sdbLobStruct ) ;
   LOB_INIT( lobStruct, cs->_connection, cs ) ;
   ossMemcpy( lobStruct->_oid, oid, 12 ) ;
   lobStruct->_contextID = contextID ;
   lobStruct->_mode = mode ;
   lobStruct->_lobSize = 0 ;
   lobStruct->_createTime = 0 ;

   _regSocket( cs->_connection, &lobStruct->_sock ) ;

   if ( SDB_LOB_READ & mode )
   {
      bType = bson_find( &bsonItr, &obj, FIELD_NAME_LOB_SIZE ) ;
      if ( BSON_INT == bType || BSON_LONG == bType )
      {
         lobStruct->_lobSize = bson_iterator_long( &bsonItr ) ;
      }
      else
      {
         rc = SDB_SYS ;
         goto error ;
      }

      bType = bson_find( &bsonItr, &obj, FIELD_NAME_LOB_CREATTIME ) ;
      if ( BSON_LONG == bType )
      {
         lobStruct->_createTime = bson_iterator_long( &bsonItr ) ;
      }
      else
      {
         rc = SDB_SYS ;
         goto error ;
      }

      bType = bson_find( &bsonItr, &obj, FIELD_NAME_LOB_PAGE_SIZE ) ;
      if ( BSON_INT == bType )
      {
         lobStruct->_pageSize =  bson_iterator_int( &bsonItr ) ;
      }
      else
      {
         rc = SDB_SYS ;
         goto error ;
      }
   }
   *lobHandle = (sdbLobHandle)lobStruct ;
done:
   bson_destroy( &obj ) ;
   return rc ;
error:
   if ( NULL != lobStruct )
   {
      SDB_OSS_FREE( lobStruct ) ;
   }
   *lobHandle = SDB_INVALID_HANDLE ;
   goto done ;
}

SDB_EXPORT INT32 sdbWriteLob( sdbLobHandle lobHandle,
                              const CHAR *buf,
                              UINT32 len )
{
   INT32 rc = SDB_OK ;
   sdbLobStruct *lob = ( sdbLobStruct * )lobHandle ;
   SINT64 contextID = -1 ;
   BOOLEAN result = TRUE ;
   UINT32 totalLen = 0 ;
   const UINT32 maxSendLen = 2 * 1024 * 1024 ;

   HANDLE_CHECK( lobHandle, lob, SDB_HANDLE_TYPE_LOB ) ;

   if ( NULL == buf )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( SDB_LOB_CREATEONLY != lob->_mode )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( 0 == len )
   {
      goto done ;
   }

   do
   {
      UINT32 sendLen = maxSendLen <= len - totalLen ?
                       maxSendLen : len - totalLen ;
      rc = clientBuildWriteLobMsg( &(lob->_pSendBuffer), &lob->_sendBufferSize,
                                   buf + totalLen, sendLen, -1, 0, 1,
                                   lob->_contextID, 0,
                                   lob->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _send ( lob->_connection, lob->_sock, (MsgHeader*)lob->_pSendBuffer,
                   lob->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _recvExtract ( lob->_connection, lob->_sock,
                          (MsgHeader**)&lob->_pReceiveBuffer,
                          &lob->_receiveBufferSize, &contextID, &result,
                          lob->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( lob->_pSendBuffer, lob->_pReceiveBuffer,
                           lob->_connection ) ;
      totalLen += sendLen ;
   } while ( totalLen < len ) ;
   lob->_lobSize += len ;
done:
   return rc ;
error:
   goto done ;
}

static BOOLEAN sdbDataCached( const sdbLobStruct *lob,
                              UINT32 len )
{
   return ( NULL != lob->_dataCache && 0 < lob->_cachedSize &&
            0 <= lob->_cachedOffset &&
            lob->_cachedOffset <= lob->_currentOffset &&
            lob->_currentOffset < ( lob->_cachedOffset +
                                    lob->_cachedSize ) ) ;
}

static void sdbReadInCache( sdbLobStruct *lob,
                            void *buf,
                            UINT32 len,
                            UINT32 *read )
{
   const CHAR *cache = NULL ;
   UINT32 readInCache = lob->_cachedOffset + lob->_cachedSize -
                        lob->_currentOffset ;
   readInCache = readInCache <= len ?
                 readInCache : len ;
   cache = lob->_dataCache +
           lob->_currentOffset -
           lob->_cachedOffset ;
   ossMemcpy( buf, cache, readInCache ) ;
   lob->_cachedSize -= readInCache + cache - lob->_dataCache ;

   if ( 0 == lob->_cachedSize )
   {
      lob->_dataCache = NULL ;
      lob->_cachedOffset = -1 ;
   }
   else
   {
      lob->_dataCache = cache + readInCache ;
      lob->_cachedOffset = readInCache + lob->_currentOffset ;
   }

   *read = readInCache ;
   return ;                        
}

static UINT32 sdbReviseReadLen( sdbLobStruct *lob,
                                UINT32 needLen )
{
   UINT32 pageSize = lob->_pageSize ;
   UINT32 mod = lob->_currentOffset & ( pageSize - 1 ) ;
   UINT32 alignedLen = ossRoundUpToMultipleX( needLen,
                                              LOB_ALIGNED_LEN ) ;
   alignedLen -= mod ;
   if ( alignedLen < LOB_ALIGNED_LEN )
   {
      alignedLen += LOB_ALIGNED_LEN ;
   }
   return alignedLen ;
}

static INT32 sdbOnceRead( sdbLobStruct *lob,
                          CHAR *buf,
                          UINT32 len,
                          UINT32 *read )
{
   INT32 rc = SDB_OK ;
   UINT32 needRead = len ;
   UINT32 totalRead = 0 ;
   CHAR *localBuf = buf ;
   UINT32 onceRead = 0 ;
   const MsgOpReply *reply = NULL ;
   const MsgLobTuple *tuple = NULL ;
   const CHAR *body = NULL ;
   UINT32 alignedLen = 0 ;
   SINT64 contextID = -1 ;
   BOOLEAN result = TRUE ;

   if ( sdbDataCached( lob, needRead ) )
   {
      sdbReadInCache( lob, localBuf, needRead, &onceRead ) ;

      totalRead += onceRead ;
      needRead -= onceRead ;
      lob->_currentOffset += onceRead ;
      localBuf += onceRead ;
      *read = totalRead ;
      goto done ;
   }

   lob->_cachedOffset = -1 ;
   lob->_cachedSize = 0 ;
   lob->_dataCache = NULL ;

   alignedLen = sdbReviseReadLen( lob, needRead ) ;

   rc = clientBuildReadLobMsg( &(lob->_pSendBuffer), &lob->_sendBufferSize,
                               alignedLen, lob->_currentOffset,
                               0, lob->_contextID, 0,
                               lob->_endianConvert ) ;

   rc = _send ( lob->_connection, lob->_sock, (MsgHeader*)lob->_pSendBuffer,
                lob->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( lob->_connection, lob->_sock,
                       (MsgHeader**)&lob->_pReceiveBuffer,
                       &lob->_receiveBufferSize, &contextID, &result,
                       lob->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( lob->_pSendBuffer, lob->_pReceiveBuffer,
                        lob->_connection ) ;
   reply = ( const MsgOpReply * )( lob->_pReceiveBuffer ) ;
   if ( ( UINT32 )( reply->header.messageLength ) <
        ( sizeof( MsgOpReply ) + sizeof( MsgLobTuple ) ) )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   tuple = ( const MsgLobTuple *)
           ( lob->_pReceiveBuffer + sizeof( MsgOpReply ) ) ;
   if ( lob->_currentOffset != tuple->columns.offset )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   else if ( ( UINT32 )( reply->header.messageLength ) < 
             ( sizeof( MsgOpReply ) + sizeof( MsgLobTuple ) +
             tuple->columns.len ) )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   body = lob->_pReceiveBuffer + sizeof( MsgOpReply ) + sizeof( MsgLobTuple ) ;

   if ( needRead < tuple->columns.len )
   {
      ossMemcpy( localBuf, body, needRead ) ;
      totalRead += needRead ;
      lob->_currentOffset += needRead ;
      lob->_cachedOffset = lob->_currentOffset ;
      lob->_cachedSize = tuple->columns.len - needRead ;
      lob->_dataCache = body + needRead ;
   }
   else
   {
      ossMemcpy( localBuf, body, tuple->columns.len ) ;
      totalRead += tuple->columns.len ;
      lob->_currentOffset += tuple->columns.len ;
      lob->_cachedOffset = -1 ;
      lob->_cachedSize = 0 ;
      lob->_dataCache = NULL ;
   }

   *read = totalRead ;
done:
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbReadLob( sdbLobHandle lobHandle,
                             UINT32 len,
                             CHAR *buf,
                             UINT32 *read )
{
   INT32 rc = SDB_OK ;
   sdbLobStruct *lob = ( sdbLobStruct * )lobHandle ;
   UINT32 needRead = len ;
   CHAR *localBuf = buf ;
   UINT32 onceRead = 0 ;
   UINT32 totalRead = 0 ;

   HANDLE_CHECK( lobHandle, lob, SDB_HANDLE_TYPE_LOB ) ;

   if (  NULL == buf )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( SDB_LOB_READ != lob->_mode || -1 == lob->_contextID )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( 0 == len )
   {
      *read = 0 ;
      goto done ;
   }

   if ( lob->_currentOffset == lob->_lobSize )
   {
      rc = SDB_EOF ;
      goto error ;
   }

   while ( 0 < needRead && lob->_currentOffset < lob->_lobSize )
   {
      rc = sdbOnceRead( lob, localBuf, needRead, &onceRead ) ;
      if ( SDB_EOF == rc )
      {
         if ( 0 < totalRead )
         {
            rc = SDB_OK ;
            break ;
         }
         else
         {
            goto error ;
         }
      }
      else if ( SDB_OK != rc )
      {
         goto error ;
      }

      needRead -= onceRead ;
      totalRead += onceRead ;
      localBuf += onceRead ;
      onceRead = 0 ;
   }

   *read = totalRead ;
done:
   return rc ;
error:
   *read = 0 ;
   goto done ;
}


SDB_EXPORT INT32 sdbCloseLob( sdbLobHandle *lobHandle )
{
   INT32 rc = SDB_OK ;
   sdbLobStruct *lob = ( sdbLobStruct * )( *lobHandle );
   SINT64 contextID = -1 ;
   BOOLEAN result = TRUE ;

   if ( NULL == lobHandle )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   HANDLE_CHECK( *lobHandle, lob, SDB_HANDLE_TYPE_LOB ) ;

   if ( -1 == lob->_sock )
   {
      goto done ;
   }

   rc = clientBuildCloseLobMsg( &lob->_pSendBuffer, &lob->_sendBufferSize,
                                0, 1, lob->_contextID, 0,
                                lob->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( lob->_connection, lob->_sock, (MsgHeader*)lob->_pSendBuffer,
                lob->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( lob->_connection, lob->_sock,
                       (MsgHeader**)&lob->_pReceiveBuffer,
                       &lob->_receiveBufferSize, &contextID, &result,
                       lob->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( lob->_pSendBuffer, lob->_pReceiveBuffer,
                        lob->_connection ) ;
done:
   if ( NULL != lobHandle && SDB_INVALID_HANDLE != *lobHandle )
   {
      _unregSocket( lob->_connection, &lob->_sock ) ;

      if ( lob->_pSendBuffer )
      {
         SDB_OSS_FREE ( lob->_pSendBuffer ) ;
      }
      if ( lob->_pReceiveBuffer )
      {
         SDB_OSS_FREE ( lob->_pReceiveBuffer ) ;
      }

      SDB_OSS_FREE( lob ) ;

      *lobHandle = SDB_INVALID_HANDLE ;
   }
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbRemoveLob( sdbCollectionHandle cHandle,
                               const bson_oid_t *oid )
{
   INT32 rc = SDB_OK ;
   SINT64 contextID = -1 ;
   BOOLEAN result = TRUE ;
   bson meta ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   
   bson_init( &meta ) ;

   if ( NULL == oid )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;

   if ( !cs->_collectionFullName[0] || NULL == oid )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( NULL == oid )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = bson_append_string( &meta, FIELD_NAME_COLLECTION,
                            cs->_collectionFullName ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   rc = bson_append_oid( &meta, FIELD_NAME_LOB_OID, oid ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   bson_finish( &meta ) ;

   rc = clientBuildRemoveLobMsg( &cs->_pSendBuffer, &cs->_sendBufferSize,
                                 &meta, 0, 1, 0, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
done:
   bson_destroy( &meta ) ;
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbSeekLob( sdbLobHandle lobHandle,
                             SINT64 size,
                             SDB_LOB_SEEK whence )
{
   INT32 rc = SDB_OK ;
   sdbLobStruct *lob = ( sdbLobStruct * )lobHandle ;

   HANDLE_CHECK( lobHandle, lob, SDB_HANDLE_TYPE_LOB ) ;

   if ( SDB_LOB_READ != lob->_mode || -1 == lob->_contextID )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( SDB_LOB_SEEK_SET == whence )
   {
      if ( size < 0 || lob->_lobSize < size )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      lob->_currentOffset = size ;
   }
   else if ( SDB_LOB_SEEK_CUR == whence )
   {
      if ( lob->_lobSize < size + lob->_currentOffset ||
           size + lob->_currentOffset < 0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      lob->_currentOffset += size ;
   }
   else if ( SDB_LOB_SEEK_END == whence )
   {
      if ( size < 0 || lob->_lobSize < size )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      lob->_currentOffset = lob->_lobSize - size ;
   }
   else
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbGetLobSize( sdbLobHandle lobHandle,
                                SINT64 *size )
{
   INT32 rc = SDB_OK ;
   sdbLobStruct *lob = ( sdbLobStruct * )lobHandle ;

   HANDLE_CHECK( lobHandle, lob, SDB_HANDLE_TYPE_LOB ) ;

   *size = lob->_lobSize ;
done:
   return rc ;
error:
   goto done ;
}

SDB_EXPORT INT32 sdbGetLobCreateTime( sdbLobHandle lobHandle,
                                      UINT64 *millis )
{
   INT32 rc = SDB_OK ;
   sdbLobStruct *lob = ( sdbLobStruct * )lobHandle ;

   HANDLE_CHECK( lobHandle, lob, SDB_HANDLE_TYPE_LOB ) ;

   *millis = lob->_createTime ;
done:
   return rc ;
error:
   goto done ;
}

static INT32 _sdbRunCmdOfLob( sdbCollectionHandle cHandle,
                              const CHAR *cmd,
                              const bson *obj,
                              sdbCursorHandle *cursorHandle )
{
   INT32 rc = SDB_OK ;
   BOOLEAN result = TRUE ;
   SINT64 contextID = -1 ;
   sdbCursorStruct *cursor = NULL ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;
   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;

   rc = clientBuildQueryMsg ( &cs->_pSendBuffer, &cs->_sendBufferSize,
                              cmd,
                              0, 0, -1, -1, obj,
                              NULL, NULL, NULL, cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = _send ( cs->_connection, cs->_sock, (MsgHeader*)cs->_pSendBuffer,
                cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = _recvExtract ( cs->_connection, cs->_sock,
                       (MsgHeader**)&cs->_pReceiveBuffer,
                       &cs->_receiveBufferSize, &contextID, &result,
                       cs->_endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
   CHECK_RET_MSGHEADER( cs->_pSendBuffer, cs->_pReceiveBuffer,
                        cs->_connection ) ;
   if ( -1 != contextID && NULL != cursorHandle )
   {
      ALLOC_HANDLE( cursor, sdbCursorStruct ) ;
      INIT_CURSOR( cursor, cs->_connection, cs, contextID ) ;
      ossMemcpy ( cursor->_collectionFullName, cs->_collectionFullName,
                  sizeof(cursor->_collectionFullName) ) ;
      rc = _regCursor ( cursor->_connection, (sdbCursorHandle)cursor ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      *cursorHandle = (sdbCursorHandle)cursor ;
   }
 
done:
   return rc ;
error:
   if ( cursor )
   {
      SDB_OSS_FREE ( cursor ) ;
   }
   SET_INVALID_HANDLE( cursorHandle ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbListLobs( sdbCollectionHandle cHandle,
                              sdbCursorHandle *cursor )
{
   INT32 rc = SDB_OK ;
   bson obj ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;

   if ( NULL == cursor )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   bson_init( &obj ) ;
   rc = bson_append_string( &obj, FIELD_NAME_COLLECTION, cs->_collectionFullName ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   bson_finish( &obj ) ;

   rc = _sdbRunCmdOfLob( cHandle, CMD_ADMIN_PREFIX CMD_NAME_LIST_LOBS,
                         &obj, cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   bson_destroy( &obj ) ;
   return rc ;
error:
   SET_INVALID_HANDLE( cursor ) ;
   goto done ;
}

SDB_EXPORT INT32 sdbListLobPieces( sdbCollectionHandle cHandle,
                                   sdbCursorHandle *cursor )
{
   INT32 rc = SDB_OK ;
   bson obj ;
   sdbCollectionStruct *cs = (sdbCollectionStruct*)cHandle ;

   HANDLE_CHECK( cHandle, cs, SDB_HANDLE_TYPE_COLLECTION ) ;

   if ( NULL == cursor )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }   

   bson_init( &obj ) ;
   rc = bson_append_string( &obj, FIELD_NAME_COLLECTION, cs->_collectionFullName ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   rc = bson_append_bool( &obj, FIELD_NAME_LOB_LIST_PIECES_MODE, TRUE ) ;
   if ( SDB_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   bson_finish( &obj ) ;

   rc = _sdbRunCmdOfLob( cHandle, CMD_ADMIN_PREFIX CMD_NAME_LIST_LOBS,
                         &obj, cursor ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   } 
done:
   bson_destroy( &obj ) ;
   return rc ;
error:
   SET_INVALID_HANDLE( cursor ) ;
   goto done ;
}

