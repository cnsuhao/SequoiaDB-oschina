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

   Source File Name = clientcpp.cpp

   Descriptive Name = C++ Client Driver

   When/how to use: this program may be used on binary and text-formatted
   versions of C++ Client component. This file contains functions for
   client driver.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "clientImpl.hpp"
#include "common.h"
#include "ossMem.hpp"
#include "msgCatalogDef.h"
#include "msgDef.h"
#include "pmdOptions.h"
#include <string>
#include <vector>
#include "pdTrace.hpp"
#include "clientTrace.hpp"
#include "fmpDef.hpp"
#include "../bson/lib/md5.hpp"

using namespace std ;
using namespace bson ;

#define LOB_ALIGNED_LEN 524288

namespace sdbclient
{
   static BOOLEAN _sdbIsSrand = FALSE ;
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
         _sdbSrand () ;
#if defined (_WINDOWS)
      rand_s ( &randVal ) ;
#elif defined (_LINUX)
      randVal = rand_r ( &_sdbRandSeed ) ;
#endif
      return randVal ;
   }

#define CHECK_RET_MSGHEADER( pSendBuf, pRecvBuf, pConnect )   \
do                                                            \
{                                                             \
   rc = clientCheckRetMsgHeader( pSendBuf, pRecvBuf ) ;       \
   if ( SDB_OK != rc )                                        \
   {                                                          \
      if ( SDB_UNEXPECTED_RESULT == rc )                      \
      {                                                       \
         pConnect->disconnect() ;                             \
      }                                                       \
      goto error ;                                            \
   }                                                          \
}while( FALSE )

   static INT32 clientSocketSend ( ossSocket *pSock,
                                   CHAR *pBuffer,
                                   INT32 sendSize )
   {
      INT32 rc = SDB_OK ;
      INT32 sentSize = 0 ;
      INT32 totalSentSize = 0 ;
      while ( sendSize > totalSentSize )
      {
         rc = pSock->send ( &pBuffer[totalSentSize],
                            sendSize - totalSentSize,
                            sentSize,
                            SDB_CLIENT_SOCKET_TIMEOUT_DFT ) ;
         totalSentSize += sentSize ;
         if ( SDB_TIMEOUT == rc )
            continue ;
         if ( rc  )
            goto done ;
      }
   done :
      return rc ;
   }

   static INT32 clientSocketRecv ( ossSocket *pSock,
                                   CHAR *pBuffer,
                                   INT32 receiveSize )
   {
      INT32 rc = SDB_OK ;
      INT32 receivedSize = 0 ;
      INT32 totalReceivedSize = 0 ;
      while ( receiveSize > totalReceivedSize )
      {
         rc = pSock->recv ( &pBuffer[totalReceivedSize],
                            receiveSize - totalReceivedSize,
                            receivedSize,
                            SDB_CLIENT_SOCKET_TIMEOUT_DFT ) ;
         totalReceivedSize += receivedSize ;
         if ( SDB_TIMEOUT == rc )
            continue ;
         if ( rc )
            goto done ;
      }
   done :
      return rc ;
   }
   
   /*
    * sdbCursorImpl
    * Cursor Implementation
    */
   _sdbCursorImpl::_sdbCursorImpl () :
   _connection ( NULL ),
   _collection ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ),
   _pReceiveBuffer ( NULL ),
   _receiveBufferSize ( 0 ),
   _modifiedCurrent ( NULL ),
   _isDeleteCurrent ( FALSE ),
   _contextID ( -1 ),
   _isClosed ( FALSE ),
   _totalRead ( 0 ),
   _offset ( -1 )
   {
      _hintObj = BSON ( "" << CLIENT_RECORD_ID_INDEX ) ;
   }

   _sdbCursorImpl::~_sdbCursorImpl ()
   {
      if ( _connection )
      {
         if ( !_isClosed )
         {
            if ( -1 != _contextID )
            {
               _killCursor () ;
            }
            _connection->_unregCursor ( this ) ;
         }
      }
      if ( _collection )
      {
         if ( !_isClosed )
         {
            _collection->_unregCursor ( this ) ;
         }
      }
      if ( _pSendBuffer )
      {
         SDB_OSS_FREE ( _pSendBuffer ) ;
      }
      if ( _pReceiveBuffer )
      {
         SDB_OSS_FREE ( _pReceiveBuffer ) ;
      }
      if ( _modifiedCurrent )
      {
         delete _modifiedCurrent ;
         _modifiedCurrent = NULL ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__SETCOLLECTIONINCUR, "_sdbCursorImpl::_setCollection" )
   void _sdbCursorImpl::_setCollection ( _sdbCollectionImpl *collection )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__SETCOLLECTIONINCUR ) ;
      _collection = collection ;
      if ( _collection )
         _collection->_regCursor ( this ) ;
      PD_TRACE_EXIT ( SDB_CLIENT__SETCOLLECTIONINCUR ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CURSORIMPL__SETCONNECTIONINCUR, "_sdbCursorImpl::_setConnection" )
   void _sdbCursorImpl::_setConnection ( _sdb *connection )
   {
      PD_TRACE_ENTRY ( SDB_CURSORIMPL__SETCONNECTIONINCUR ) ;
      _connection = (_sdbImpl*)connection ;
      _connection->_regCursor ( this ) ;
      PD_TRACE_EXIT ( SDB_CURSORIMPL__SETCONNECTIONINCUR ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__KILLCURSOR, "_sdbCursorImpl::_killCursor" )
   void _sdbCursorImpl::_killCursor ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__KILLCURSOR ) ;
      INT32 rc         = SDB_OK ;
      SINT64 contextID = 0 ;
      BOOLEAN result   = FALSE ;
      BOOLEAN locked   = FALSE ;

      if ( -1 == _contextID || !_connection )
      {
         goto done ;
      }
      rc = clientBuildKillContextsMsg ( &_pSendBuffer, &_sendBufferSize, 0,
                                        1, &_contextID,
                                        _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done :
      if ( locked )
      {
         _connection->unlock () ;
      }
      PD_TRACE_EXIT ( SDB_CLIENT__KILLCURSOR ) ;
      return ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__READNEXTBUF, "_sdbCursorImpl::_readNextBuffer" )
   INT32 _sdbCursorImpl::_readNextBuffer ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__READNEXTBUF ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result = FALSE ;
      BOOLEAN locked  = FALSE ;
      SINT64 contextID = 0 ;
      if ( -1 == _contextID )
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }
      if ( !_connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error ;
      }
      rc = clientBuildGetMoreMsg ( &_pSendBuffer, &_sendBufferSize, -1,
                                   _contextID, 0, _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc || contextID != _contextID )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done :
      if ( locked )
      {
         _connection->unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT__READNEXTBUF, rc ) ;
      return rc ;
   error :
      if ( SDB_DMS_EOC != rc )
      {
         _killCursor () ;
      }
      _isClosed = TRUE ;
      _contextID  = -1 ;
      _offset     = -1 ;
      if ( _connection )
      {
         _connection->_unregCursor ( this ) ;
         _connection = NULL ;
      }
      if ( _collection )
      {
         _collection->_unregCursor ( this ) ;
         _collection = NULL ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_NEXT, "_sdbCursorImpl::next" )
   INT32 _sdbCursorImpl::next ( BSONObj &obj )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_NEXT ) ;
      INT32 rc = SDB_OK ;
      BSONObj localobj ;
      MsgOpReply *pReply = NULL ;
      if ( _isClosed )
      {
         rc = SDB_DMS_CONTEXT_IS_CLOSE ;
         goto error ;
      }
      if ( _modifiedCurrent )
      {
         delete _modifiedCurrent ;
         _modifiedCurrent = NULL ;
      }
      if ( !_pReceiveBuffer && _connection )
      {
         if ( -1 == _offset )
         {
            rc = _readNextBuffer () ;
            if ( rc )
            {
               goto error ;
            }
         }
         else
         {
            rc = SDB_DMS_EOC ;
            goto error ;
         }
      }
   retry :
      pReply = (MsgOpReply*)_pReceiveBuffer ;
      if ( -1 == _offset )
      {
         _offset = ossRoundUpToMultipleX ( sizeof ( MsgOpReply ), 4 ) ;
      }
      else
      {
         _offset += ossRoundUpToMultipleX ( *(INT32*)&_pReceiveBuffer[_offset],
                                            4 ) ;
      }
      if ( _offset >= pReply->header.messageLength ||
           _offset >= _receiveBufferSize )
      {
         if ( _connection )
         {
            _offset = -1 ;
            rc = _readNextBuffer () ;
            if ( rc )
            {
               goto error ;
            }
            goto retry ;
         }
         else
         {
            rc = SDB_DMS_EOC ;
            goto error ;
         }
      }
      localobj.init ( &_pReceiveBuffer [ _offset ] ) ;
      obj = localobj.copy () ;
      ++ _totalRead ;
   done :
      _isDeleteCurrent = FALSE ;
      PD_TRACE_EXITRC ( SDB_CLIENT_NEXT, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CURRENT, "_sdbCursorImpl::current" )
   INT32 _sdbCursorImpl::current ( BSONObj &obj )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CURRENT ) ;
      INT32 rc = SDB_OK ;
      MsgOpReply *pReply = NULL ;
      BSONObj localobj ;
      if ( _isClosed )
      {
         rc = SDB_DMS_CONTEXT_IS_CLOSE ;
         goto error ;
      }
      if(_isDeleteCurrent)
      {
         rc = SDB_CURRENT_RECORD_DELETED ;
         goto error ;
      }
      if ( !&obj )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( _modifiedCurrent )
      {
         obj=_modifiedCurrent->copy() ;
         goto done ;
      }
      if ( !_pReceiveBuffer )
      {
         if ( -1 == _offset && _connection )
         {
            rc = _readNextBuffer () ;
            if ( rc )
            {
               goto error ;
            }
         }
         else
         {
            rc = SDB_DMS_EOC ;
            goto error ;
         }
      }
   retry :
      pReply = (MsgOpReply*)_pReceiveBuffer ;
      if ( -1 == _offset )
      {
         _offset = ossRoundUpToMultipleX ( sizeof ( MsgOpReply ), 4 ) ;
      }
      if ( _offset > pReply->header.messageLength ||
           _offset >= _receiveBufferSize )
      {
         if ( _connection )
         {
            _offset = -1 ;
            rc = _readNextBuffer () ;
            if ( rc )
            {
               goto error ;
            }
            goto retry ;
         }
         else
         {
            rc = SDB_DMS_EOC ;
            goto error ;
         }
      }
      localobj.init ( &_pReceiveBuffer [ _offset ] ) ;
      obj = localobj.copy () ;

   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CURRENT, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CLOSECURSOR, "_sdbCursorImpl::close" )
   INT32 _sdbCursorImpl::close()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CLOSECURSOR ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      if ( _isClosed || -1 == _contextID )
      {
         goto done ;
      }
      if ( NULL == _connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error ;
      }
      rc = clientBuildKillContextsMsg( &_pSendBuffer, &_sendBufferSize, 0, 1,
                                       &_contextID, _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      _contextID = -1 ;
      _isClosed = TRUE ;
      if ( _connection )
      {
         _connection->_unregCursor ( this ) ;
         _connection = NULL ;
      }
      if ( _collection )
      {
         _collection->_unregCursor ( this ) ;
         _collection = NULL ;
      }
   done :
      if ( locked )
      {
         _connection->unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_CLOSECURSOR, rc );
      return rc ;
   error :
      goto done ;
   }


/*
   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_UPDATECURRENT, "_sdbCursorImpl::updateCurrent" )
   INT32 _sdbCursorImpl::updateCurrent ( BSONObj &rule )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_UPDATECURRENT ) ;
      INT32 rc = SDB_OK ;
      BSONObj obj ;
      BSONObj updateCondition ;
      BSONObj modifiedObj ;
      BSONElement it ;
      _sdbCursor *tempQuery = NULL ;
      if(_isDeleteCurrent)
      {
         rc = SDB_CURRENT_RECORD_DELETED ;
         goto error ;
      }
      if ( !_collection )
      {
         rc = SDB_CLT_OBJ_NOT_EXIST ;
         goto error ;
      }
      rc = current ( obj ) ;
      if ( rc )
      {
         goto error ;
      }
      it = obj.getField ( CLIENT_RECORD_ID_FIELD ) ;
      if ( it.eoo() )
      {
         rc = SDB_CORRUPTED_RECORD ;
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
      rc = _collection->update ( &rule, &updateCondition,
                                 &_hintObj ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _collection->query(&tempQuery, updateCondition,
               _sdbStaticObject, _sdbStaticObject, _hintObj, 0, 1) ;
      if ( rc )
      {
         goto error ;
      }
      rc = tempQuery->next( modifiedObj ) ;
      if ( rc )
      {
         goto error ;
      }
   if ( !_modifiedCurrent )
   {
      _modifiedCurrent = (bson*)SDB_OSS_MALLOC ( sizeof(bson) ) ;
      if ( !_modifiedCurrent )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      bson_init ( _modifiedCurrent ) ;
   }
   rc = bson_copy ( _modifiedCurrent, &modifiedObj ) ;
   if ( BSON_OK != rc )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   done :
      if ( tempQuery )
      {
         delete  tempQuery ;
         tempQuery = NULL ;
      }
      bson_destroy ( &updateCondition ) ;
      bson_destroy ( &hintObj ) ;
      bson_destroy ( &modifiedObj ) ;
      PD_TRACE_EXITRC ( SDB_CLIENT_UPDATECURRENT, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DELCURRENT, "_sdbCursorImpl::delCurrent" )
   INT32 _sdbCursorImpl::delCurrent ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DELCURRENT ) ;
      INT32 rc = SDB_OK ;
      bson obj ;
      bson_init ( &obj ) ;
      bson deleteCondition ;
      bson_init ( &deleteCondition ) ;
      bson_iterator it ;
      if ( !_collection )
      {
         rc = SDB_CLT_OBJ_NOT_EXIST ;
         goto error ;
      }
      if(_isDeleteCurrent)
      {
         rc = SDB_CURRENT_RECORD_DELETED ;
         goto error ;
      }
      rc = current ( obj ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( BSON_EOO != bson_find ( &it, &obj, CLIENT_RECORD_ID_FIELD ) )
      {
         rc = bson_append_element ( &deleteCondition, NULL, &it ) ;
         if ( rc )
         {
            rc = SDB_SYS ;
            goto error ;
         }
         rc = bson_finish ( &deleteCondition ) ;
         if ( rc )
         {
            rc = SDB_SYS ;
            goto error ;
         }
      }
      rc = _collection->del ( &deleteCondition, &_hintObj ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      bson_destroy ( &deleteCondition ) ;
      _isDeleteCurrent = TRUE ;
      PD_TRACE_EXITRC ( SDB_CLIENT_DELCURRENT, rc ) ;
      return rc ;
   error :
      goto done ;
   }*/


   /*
    * sdbCollectionImpl
    * Collection Implementation
    */
   _sdbCollectionImpl::_sdbCollectionImpl () :
   _connection ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ),
   _pReceiveBuffer ( NULL ),
   _receiveBufferSize ( 0 ),
   _pAppendOIDBuffer ( NULL ),
   _appendOIDBufferSize ( 0 )
   {
      ossMemset ( _collectionSpaceName, 0, sizeof ( _collectionSpaceName ) ) ;
      ossMemset ( _collectionName, 0, sizeof ( _collectionName ) ) ;
      ossMemset ( _collectionFullName, 0, sizeof ( _collectionFullName ) ) ;
   }

   _sdbCollectionImpl::_sdbCollectionImpl ( CHAR *pCollectionFullName ) :
   _connection ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ),
   _pReceiveBuffer ( NULL ),
   _receiveBufferSize ( 0 ),
   _pAppendOIDBuffer ( NULL ),
   _appendOIDBufferSize ( 0 )
   {
      _setName ( pCollectionFullName ) ;
   }

   _sdbCollectionImpl::_sdbCollectionImpl ( CHAR *pCollectionSpaceName,
                                            CHAR *pCollectionName ) :
   _connection ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ),
   _pReceiveBuffer ( NULL ),
   _receiveBufferSize ( 0 ),
   _pAppendOIDBuffer ( NULL ),
   _appendOIDBufferSize ( 0 )
   {
      INT32 collectionSpaceLen = ossStrlen ( pCollectionSpaceName ) ;
      INT32 collectionLen      = ossStrlen ( pCollectionName ) ;
      ossMemset ( _collectionSpaceName, 0, sizeof ( _collectionSpaceName ) );
      ossMemset ( _collectionName, 0, sizeof ( _collectionName ) ) ;
      ossMemset ( _collectionFullName, 0, sizeof ( _collectionFullName ) ) ;
      if ( collectionSpaceLen <= CLIENT_CS_NAMESZ &&
           collectionLen <= CLIENT_COLLECTION_NAMESZ )
      {
         ossStrncpy ( _collectionSpaceName, pCollectionSpaceName,
                      collectionSpaceLen ) ;
         ossStrncpy ( _collectionName, pCollectionName, collectionLen ) ;
         ossStrncpy ( _collectionFullName, pCollectionSpaceName,
                      collectionSpaceLen ) ;
         ossStrncat ( _collectionFullName, ".", 1 ) ;
         ossStrncat ( _collectionFullName, pCollectionName, collectionLen ) ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIMPL__SETNAME, "_sdbCollectionImpl::_setName" )
   INT32 _sdbCollectionImpl::_setName ( const CHAR *pCollectionFullName )
   {
      PD_TRACE_ENTRY ( SDB_CLIMPL__SETNAME ) ;
      INT32 rc                 = SDB_OK ;
      INT32 collectionSpaceLen = 0 ;
      INT32 collectionLen      = 0 ;
      INT32 fullLen            = 0 ;
      CHAR *pDot               = NULL ;
      CHAR *pDot1              = NULL ;
      CHAR collectionFullName [ CLIENT_COLLECTION_NAMESZ +
                                CLIENT_CS_NAMESZ +
                                1 ] ;
      ossMemset ( _collectionSpaceName, 0, sizeof ( _collectionSpaceName ) );
      ossMemset ( _collectionName, 0, sizeof ( _collectionName ) ) ;
      ossMemset ( _collectionFullName, 0, sizeof ( _collectionFullName ) ) ;
      if ( !pCollectionFullName ||
           (fullLen = ossStrlen ( pCollectionFullName )) >
           CLIENT_COLLECTION_NAMESZ + CLIENT_CS_NAMESZ + 1 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossMemset ( collectionFullName, 0, sizeof ( collectionFullName ) ) ;
      ossStrncpy ( collectionFullName, pCollectionFullName, fullLen ) ;
      pDot = (CHAR*)ossStrchr ( (CHAR*)collectionFullName, '.' ) ;
      pDot1 = (CHAR*)ossStrrchr ( (CHAR*)collectionFullName, '.' ) ;
      if ( !pDot || (pDot != pDot1) )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      *pDot = 0 ;
      ++pDot ;

      collectionSpaceLen = ossStrlen ( collectionFullName ) ;
      collectionLen      = ossStrlen ( pDot ) ;
      if ( collectionSpaceLen <= CLIENT_CS_NAMESZ &&
           collectionLen <= CLIENT_COLLECTION_NAMESZ )
      {
         ossMemcpy ( _collectionSpaceName, collectionFullName,
                     collectionSpaceLen ) ;
         ossMemcpy ( _collectionName, pDot, collectionLen ) ;
         ossMemcpy ( _collectionFullName, pCollectionFullName, fullLen ) ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIMPL__SETNAME, rc );
      return rc ;
   error :
      goto done ;
   }

   _sdbCollectionImpl::~_sdbCollectionImpl ()
   {
      std::set<ossValuePtr>::iterator it ;
      for ( it = _cursors.begin(); it != _cursors.end(); ++it )
      {
         ((_sdbCursorImpl*)(*it))->_setCollection ( NULL ) ;
      }
      _cursors.clear() ;
      if ( _connection )
      {
         _connection->_unregCollection ( this ) ;
      }
      if ( _pSendBuffer )
      {
         SDB_OSS_FREE ( _pSendBuffer ) ;
      }
      if ( _pReceiveBuffer )
      {
         SDB_OSS_FREE ( _pReceiveBuffer ) ;
      }
      if ( _pAppendOIDBuffer )
      {
         SDB_OSS_FREE ( _pAppendOIDBuffer ) ;
      }
   }

#pragma pack(1)
   class _IDToInsert
   {
   public :
      CHAR _type ;
      CHAR _id[4] ; // _id + '\0'
      OID _oid ;
      _IDToInsert ()
      {
         _type = (CHAR)jstOID ;
         _id[0] = '_' ;
         _id[1] = 'i' ;
         _id[2] = 'd' ;
         _id[3] = 0 ;
         SDB_ASSERT ( sizeof ( _IDToInsert) == 17,
                      "IDToInsert should be 17 bytes" ) ;
      }
   } ;
   typedef class _IDToInsert _IDToInsert ;
   class _idToInsert : public BSONElement
   {
   public :
      _idToInsert( CHAR* x ) : BSONElement((CHAR*) ( x )){}
   } ;
   typedef class _idToInsert _idToInsert ;
#pragma pack()
   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIMPL__APPENDOID, "_sdbCollectionImpl::_appendOID" )
   INT32 _sdbCollectionImpl::_appendOID ( const BSONObj &input, BSONObj &output )
   {
      INT32 rc = SDB_OK ;
      _IDToInsert oid ;
      _idToInsert oidEle((CHAR*)(&oid)) ;
      INT32 oidLen = 0 ;
      if ( !input.getField( CLIENT_RECORD_ID_FIELD ).eoo() )
      {
         output = input ;
         goto done ;
      }
      oid._oid.init() ;
      oidLen = oidEle.size() ;
      if ( _appendOIDBufferSize < input.objsize() + oidLen )
      {
         CHAR *pOld = _pAppendOIDBuffer ;
         INT32 newSize = ossRoundUpToMultipleX ( input.objsize() +
                                                 oidLen,
                                                 SDB_PAGE_SIZE ) ;
         if ( newSize < 0 )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         _pAppendOIDBuffer = (CHAR*)SDB_OSS_REALLOC(_pAppendOIDBuffer,
                                                    sizeof(CHAR)*newSize) ;
         if ( !_pAppendOIDBuffer )
         {
            rc = SDB_OOM ;
            _pAppendOIDBuffer = pOld ;
            goto error ;
         }
         _appendOIDBufferSize = newSize ;
      }
      *(INT32*)(_pAppendOIDBuffer) = input.objsize() + oidLen ;
      ossMemcpy ( &_pAppendOIDBuffer[sizeof(INT32)], oidEle.rawdata(),
                  oidEle.size() ) ;
      ossMemcpy ( &_pAppendOIDBuffer[sizeof(INT32)+oidLen],
                  (CHAR*)(input.objdata()+sizeof(INT32)),
                  input.objsize()-sizeof(INT32) ) ;
      output.init ( _pAppendOIDBuffer ) ;
   done :
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIMPL__SETCONNECTIONINCL, "_sdbCollectionImpl::_setConnection" )
   void _sdbCollectionImpl::_setConnection ( _sdb *connection )
   {
      PD_TRACE_ENTRY ( SDB_CLIMPL__SETCONNECTIONINCL ) ;
      _connection = (_sdbImpl*)connection ;
      _connection->_regCollection ( this ) ;
      PD_TRACE_EXIT ( SDB_CLIMPL__SETCONNECTIONINCL );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIMPL__GETCONNECTIONINCL, "_sdbCollectionImpl::_getConnection" )
   void* _sdbCollectionImpl::_getConnection ()
   {
      PD_TRACE_ENTRY ( SDB_CLIMPL__GETCONNECTIONINCL ) ;
      return _connection ;
      PD_TRACE_EXIT ( SDB_CLIMPL__GETCONNECTIONINCL );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETCOUNT, "_sdbCollectionImpl::getCount" )
   INT32 _sdbCollectionImpl::getCount ( SINT64 &count,
                                        const BSONObj &condition )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETCOUNT ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      SINT64 contextID    = -1 ;
      _sdbCursor *cursor  = NULL ;
      BSONObj newObj = BSON ( FIELD_NAME_COLLECTION << _collectionFullName ) ;
      BSONObj countObj ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_GET_COUNT,
                                    0, 0, 0, -1,
                                    condition.objdata(), NULL, NULL,
                                    newObj.objdata(),
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto exit ;
      }
      _connection->lock () ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      cursor = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl () ) ;
      if ( !cursor )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)cursor)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)cursor)->_setConnection ( _connection ) ;

      rc = cursor->next ( countObj ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_UNEXPECTED_RESULT ;
         }
      }
      else
      {
         BSONElement ele = countObj.getField ( FIELD_NAME_TOTAL ) ;
         if ( ele.type() != NumberLong )
         {
            rc = SDB_UNEXPECTED_RESULT ;
         }
         else
         {
            count = ele.numberLong() ;
         }
      }
      delete ( cursor ) ;
   exit:
      PD_TRACE_EXITRC ( SDB_CLIENT_GETCOUNT, rc );
      return rc ;
   done :
      _connection->unlock () ;
      goto exit ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_BULKINSERT, "_sdbCollectionImpl::bulkInsert" )
   INT32 _sdbCollectionImpl::bulkInsert ( SINT32 flags,
                                          vector<BSONObj> &obj
                                        )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_BULKINSERT ) ;
      INT32 rc = SDB_OK ;
      SINT64 contextID = 0 ;
      BOOLEAN result ;
      SINT32 count = 0 ;
      SINT32 num = obj.size() ;

      if ( _collectionFullName[0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      if ( num <= 0 )
      {
         goto exit ;
      }
      for ( count = 0; count < num; ++count )
      {
         BSONObj temp ;
         rc = _appendOID ( obj[count], temp ) ;
         if ( rc )
         {
            goto exit ;
         }
         if ( 0 == count )
            rc = clientBuildInsertMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                           _collectionFullName, flags, 0,
                                           temp.objdata(),
                                           _connection->_endianConvert ) ;
         else
            rc = clientAppendInsertMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                            temp.objdata(),
                                            _connection->_endianConvert ) ;
         if ( rc )
         {
            goto exit ;
         }
      }
      _connection->lock () ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer,
                                       &_receiveBufferSize,
                                       contextID,
                                       result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   exit:
      PD_TRACE_EXITRC ( SDB_CLIENT_BULKINSERT, rc );
      return rc ;
   done :
      _connection->unlock () ;
      goto exit ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_INSERT, "_sdbCollectionImpl::insert" )
   INT32 _sdbCollectionImpl::insert ( const BSONObj &obj, OID *id )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_INSERT ) ;
      INT32 rc = SDB_OK ;
      SINT64 contextID = 0 ;
      BOOLEAN result ;
      BSONObj temp ;
      if ( _collectionFullName [0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      rc = _appendOID ( obj, temp ) ;
      if ( rc )
      {
         goto exit ;
      }
      rc = clientBuildInsertMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     _collectionFullName, 0, 0, temp.objdata(),
                                     _connection->_endianConvert ) ;
      if ( rc )
      {
         goto exit ;
      }
      _connection->lock () ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      if ( id )
      {
         *id = temp.getField ( CLIENT_RECORD_ID_FIELD ).__oid();
      }
   exit :
      PD_TRACE_EXITRC ( SDB_CLIENT_INSERT, rc );
      return rc ;
   done :
      _connection->unlock () ;
      goto exit ;
   error :
      goto done ;
   }

   INT32 _sdbCollectionImpl::update ( const BSONObj &rule,
                                      const BSONObj &condition,
                                      const BSONObj &hint )
   {
      return _update ( rule, condition, hint, 0 ) ;
   }

   INT32 _sdbCollectionImpl::upsert ( const BSONObj &rule,
                                      const BSONObj &condition,
                                      const BSONObj &hint )
   {
      return _update ( rule, condition, hint, FLG_UPDATE_UPSERT ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__UPDATE, "_sdbCollectionImpl::_update" )
   INT32 _sdbCollectionImpl::_update ( const BSONObj &rule,
                                       const BSONObj &condition,
                                       const BSONObj &hint,
                                       INT32 flag )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__UPDATE ) ;
      INT32 rc = SDB_OK ;
      SINT64 contextID = 0 ;
      BOOLEAN result ;
      if ( _collectionFullName [0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      rc = clientBuildUpdateMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     _collectionFullName, flag, 0,
                                     condition.objdata(),
                                     rule.objdata(),
                                     hint.objdata(),
                                     _connection->_endianConvert ) ;
      if ( rc )
      {
         goto exit ;
      }
      _connection->lock () ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   exit:
      PD_TRACE_EXITRC ( SDB_CLIENT__UPDATE, rc );
      return rc ;
   done :
      _connection->unlock () ;
      goto exit ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DEL, "_sdbCollectionImpl::del" )
   INT32 _sdbCollectionImpl::del ( const BSONObj &condition,
                                   const BSONObj &hint )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DEL ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      if ( _collectionFullName [0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      rc = clientBuildDeleteMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     _collectionFullName, 0, 0,
                                     condition.objdata(),
                                     hint.objdata(),
                                     _connection->_endianConvert ) ;
      if ( rc )
      {
         goto exit ;
      }
      _connection->lock () ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   exit:
      PD_TRACE_EXITRC ( SDB_CLIENT_DEL, rc );
      return rc ;
   done :
      _connection->unlock () ;
      goto exit ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_QUERY, "_sdbCollectionImpl::query" )
   INT32 _sdbCollectionImpl::query ( _sdbCursor **cursor,
                                     const BSONObj &condition,
                                     const BSONObj &selected,
                                     const BSONObj &orderBy,
                                     const BSONObj &hint,
                                     INT64 numToSkip,
                                     INT64 numToReturn,
                                     INT32 flag )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_QUERY ) ;
      PD_TRACE3 ( SDB_CLIENT_QUERY,
                  PD_PACK_LONG(numToSkip),
                  PD_PACK_LONG(numToReturn),
                  PD_PACK_INT( flag ) );
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BOOLEAN locked = FALSE ;
      if ( _collectionFullName [0] == '\0' || !_connection || !cursor )
      {
         rc = SDB_INVALIDARG ;
         goto done;
      }
      if ( 1 == numToReturn )
      {
         flag |= FLG_QUERY_WITH_RETURNDATA ;
      }
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    _collectionFullName, flag, 0, numToSkip,
                                    numToReturn,
                                    condition.objdata(),
                                    selected.objdata(),
                                    orderBy.objdata(),
                                    hint.objdata(),
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto done ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      if ( *cursor )
      {
         delete *cursor ;
         *cursor = NULL ;
      }
      *cursor = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl () ) ;
      if ( !*cursor )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*cursor)->_setCollection ( this ) ;
      ((_sdbCursorImpl*)*cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)*cursor)->_setConnection ( _connection ) ;
      if ( ((UINT32)((MsgHeader*)_pReceiveBuffer)->messageLength) >
           ossRoundUpToMultipleX( sizeof(MsgOpReply), 4 ) )
      {
         ((_sdbCursorImpl*)*cursor)->_pReceiveBuffer = _pReceiveBuffer ;
         _pReceiveBuffer = NULL ;
         ((_sdbCursorImpl*)*cursor)->_receiveBufferSize = _receiveBufferSize ;
         _receiveBufferSize = 0 ;
      }
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_QUERY, rc );
      return rc ;
   error :
      goto done;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETQUERYMETA, "_sdbCollectionImpl::getQueryMeta" )
   INT32 _sdbCollectionImpl::getQueryMeta ( _sdbCursor **cursor,
                                     const BSONObj &condition,
                                     const BSONObj &orderBy,
                                     const BSONObj &hint,
                                     INT64 numToSkip,
                                     INT64 numToReturn )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETQUERYMETA ) ;
      PD_TRACE2 ( SDB_CLIENT_GETQUERYMETA,
                  PD_PACK_LONG(numToSkip),
                  PD_PACK_LONG(numToReturn) );
      INT32 rc = SDB_OK ;
      const CHAR *p = NULL ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObjBuilder bo ;
      BSONObj hint1 ;
      if ( _collectionFullName [0] == '\0' || !_connection || !cursor )
      {
         rc = SDB_INVALIDARG ;
         goto done;
      }
      bo.append( FIELD_NAME_COLLECTION, _collectionFullName ) ;
      hint1 = bo.obj() ;
      p = CMD_ADMIN_PREFIX CMD_NAME_GET_QUERYMETA ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    p, 0, 0, numToSkip,
                                    numToReturn,
                                    condition.objdata(),
                                    hint.objdata(),
                                    orderBy.objdata(),
                                    hint1.objdata(),
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto done ;
      }
      _connection->lock () ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      if ( *cursor )
      {
         delete *cursor ;
         *cursor = NULL ;
      }
      *cursor = (_sdbCursor*)( new(std::nothrow) _sdbCursorImpl () ) ;
      if ( !*cursor )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*cursor)->_setCollection ( this ) ;
      ((_sdbCursorImpl*)*cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)*cursor)->_setConnection ( _connection ) ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETQUERYMETA, rc );
      return rc ;
   error :
      _connection->unlock () ;
      goto done;
   }

   /*PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__RENAMEATTEMP, "_sdbCollectionImpl::_renameAttempt" )
   void _sdbCollectionImpl::_renameAttempt ( const CHAR *pOldName,
                                             const CHAR *pNewName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__RENAMEATTEMP ) ;
      INT32 newNameLen = ossStrlen ( pNewName ) ;
      if ( (UINT32)newNameLen > sizeof(_collectionName) )
         goto exit ;
      if ( ossStrncmp ( _collectionName, pOldName,
                        CLIENT_COLLECTION_NAMESZ ) == 0 )
      {
         ossMemset ( _collectionName, 0,
                     sizeof ( _collectionName ) ) ;
         ossMemset ( _collectionFullName, 0,
                     sizeof ( _collectionFullName ) ) ;
         ossStrncpy ( _collectionName, pNewName,
                      CLIENT_COLLECTION_NAMESZ ) ;
         ossStrncpy ( _collectionFullName, _collectionSpaceName,
                      CLIENT_CS_NAMESZ ) ;
         ossStrncat ( _collectionFullName, ".", 1 ) ;
         ossStrncat ( _collectionFullName, pNewName,
                      newNameLen ) ;
      }
   exit:
      PD_TRACE_EXIT ( SDB_CLIENT__RENAMEATTEMP );
      return ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_RENAME, "_sdbCollectionImpl::rename" )
   INT32 _sdbCollectionImpl::rename ( const CHAR *pNewName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_RENAME ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObj obj ;
      BOOLEAN locked = FALSE ;
      if ( !pNewName || _collectionSpaceName[0] == '\0' ||
           _collectionName[0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      obj = BSON ( FIELD_NAME_COLLECTIONSPACE << _collectionSpaceName <<
                   FIELD_NAME_OLDNAME << _collectionName <<
                   FIELD_NAME_NEWNAME << pNewName ) ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_RENAME_COLLECTION,
                                    0, 0, -1, -1, obj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->_changeCollectionName ( _collectionSpaceName,
                                           _collectionName,
                                           pNewName ) ;
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_RENAME, rc );
      return rc ;
   error :
      goto done ;

   }*/

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATEINDEX, "_sdbCollectionImpl::createIndex" )
   INT32 _sdbCollectionImpl::createIndex ( const BSONObj &indexDef,
                                           const CHAR *pName,
                                           BOOLEAN isUnique,
                                           BOOLEAN isEnforced )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATEINDEX ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObj indexObj ;
      BSONObj newObj ;
      BOOLEAN locked = FALSE ;
      if ( _collectionFullName [0] == '\0' || !_connection ||
           !pName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      indexObj = BSON ( IXM_FIELD_NAME_KEY << indexDef <<
                        IXM_FIELD_NAME_NAME << pName <<
                        IXM_FIELD_NAME_UNIQUE << isUnique <<
                        IXM_FIELD_NAME_ENFORCED << isEnforced ) ;

      newObj = BSON ( FIELD_NAME_COLLECTION << _collectionFullName <<
                      FIELD_NAME_INDEX << indexObj ) ;

      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_CREATE_INDEX,
                                    0, 0, -1, -1,
                                    newObj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATEINDEX, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETINDEXES, "_sdbCollectionImpl::getIndexes" )
   INT32 _sdbCollectionImpl::getIndexes ( _sdbCursor **cursor,
                                          const CHAR *pName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETINDEXES ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObj queryCond ;
      BSONObj newObj ;
      if ( _collectionFullName [0] == '\0' || !_connection || !cursor )
      {
         rc = SDB_INVALIDARG ;
         return rc ;
      }
      /* build query condition */
      if ( pName )
      {
         queryCond = BSON ( IXM_FIELD_NAME_INDEX_DEF "." IXM_FIELD_NAME_NAME <<
                            pName ) ;
      }
      /* build collection name */
      newObj = BSON ( FIELD_NAME_COLLECTION << _collectionFullName ) ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_GET_INDEXES,
                                    0, 0, -1,
                                    -1, pName?queryCond.objdata():NULL,
                                    NULL, NULL,
                                    newObj.objdata(),
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto exit ;
      }
      _connection->lock () ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      if ( *cursor )
      {
         delete *cursor ;
         *cursor = NULL ;
      }
      *cursor = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl () ) ;
      if ( !*cursor )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*cursor)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)*cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)*cursor)->_setConnection ( _connection ) ;
   exit:
      PD_TRACE_EXITRC ( SDB_CLIENT_GETINDEXES, rc );
      return rc ;
   done :
      _connection->unlock () ;
      goto exit ;
   error :
      goto done ;

   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DROPINDEX, "_sdbCollectionImpl::dropIndex" )
   INT32 _sdbCollectionImpl::dropIndex ( const CHAR *pName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DROPINDEX ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObj indexObj ;
      BSONObj newObj ;
      BOOLEAN locked = FALSE ;
      if ( _collectionFullName [0] == '\0' || !_connection ||
           !pName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      indexObj = BSON ( "" << pName ) ;
      newObj = BSON ( FIELD_NAME_COLLECTION << _collectionFullName <<
                      FIELD_NAME_INDEX << indexObj ) ;

      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_DROP_INDEX,
                                    0, 0, -1, -1, newObj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
    done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_DROPINDEX, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATEINCL, "_sdbCollectionImpl::create" )
   INT32 _sdbCollectionImpl::create ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATEINCL ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObj newObj ;
      BOOLEAN locked = FALSE ;
      if ( _collectionFullName [0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( FIELD_NAME_NAME << _collectionFullName ) ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTION,
                                    0, 0, -1, -1,
                                    newObj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATEINCL, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DROPINCL, "_sdbCollectionImpl::drop" )
   INT32 _sdbCollectionImpl::drop ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DROPINCL ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObj newObj ;
      BOOLEAN locked = FALSE ;
      if ( _collectionFullName [0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( FIELD_NAME_NAME << _collectionFullName ) ;

      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTION,
                                    0, 0, -1, -1,
                                    newObj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_DROPINCL, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_SPLIT, "_sdbCollectionImpl::split" )
   INT32 _sdbCollectionImpl::split ( const CHAR *pSourceGroupName,
                                     const CHAR *pTargetGroupName,
                                     const BSONObj &splitCondition,
                                     const BSONObj &splitEndCondition)
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_SPLIT ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObj newObj ;
      BOOLEAN locked = FALSE ;
      if ( _collectionFullName [0] == '\0' || !_connection ||
         !pSourceGroupName || 0 == ossStrcmp ( pSourceGroupName, "" ) ||
         !pTargetGroupName || 0 == ossStrcmp ( pTargetGroupName, "" ) )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( CAT_COLLECTION_NAME << _collectionFullName <<
                      CAT_SOURCE_NAME << pSourceGroupName <<
                      CAT_TARGET_NAME << pTargetGroupName <<
                      CAT_SPLITQUERY_NAME << splitCondition <<
                      CAT_SPLITENDQUERY_NAME << splitEndCondition) ;

      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                                    0, 0, -1, -1,
                                    newObj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_SPLIT, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_SPLIT2, "_sdbCollectionImpl::split" )
   INT32 _sdbCollectionImpl::split ( const CHAR *pSourceGroupName,
                                     const CHAR *pTargetGroupName,
                                     double percent )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_SPLIT2 ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObj newObj ;
      BOOLEAN locked = FALSE ;
      if ( _collectionFullName [0] == '\0' || !_connection ||
         !pSourceGroupName || 0 == ossStrcmp ( pSourceGroupName, "" ) ||
         !pTargetGroupName || 0 == ossStrcmp ( pTargetGroupName, "" ) )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( percent <= 0.0 || percent > 100.0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( CAT_COLLECTION_NAME << _collectionFullName <<
                      CAT_SOURCE_NAME << pSourceGroupName <<
                      CAT_TARGET_NAME << pTargetGroupName <<
                      CAT_SPLITPERCENT_NAME << percent ) ;

      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                                    0, 0, -1, -1,
                                    newObj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_SPLIT2, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_SPLITASYNC, "_sdbCollectionImpl::splitAsync" )
   INT32 _sdbCollectionImpl::splitAsync ( SINT64 &taskID,
                    const CHAR *pSourceGroupName,
                    const CHAR *pTargetGroupName,
                    const bson::BSONObj &splitCondition,
                    const bson::BSONObj &splitEndCondition )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_SPLITASYNC ) ;
      INT32 rc                 = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID    = 0 ;
      _sdbCursor *cursor  = NULL ;
      BOOLEAN locked      = FALSE ;
      BSONObjBuilder bob ;
      BSONObj newObj  ;
      BSONObj countObj ;
      if ( _collectionFullName[0] == '\0' || !_connection ||
            !pSourceGroupName || 0 == ossStrcmp ( pSourceGroupName, "" ) ||
            !pTargetGroupName || 0 == ossStrcmp ( pTargetGroupName, "" ) )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         bob.append ( CAT_COLLECTION_NAME, _collectionFullName ) ;
         bob.append ( CAT_SOURCE_NAME, pSourceGroupName ) ;
         bob.append ( CAT_TARGET_NAME, pTargetGroupName ) ;
         bob.append ( CAT_SPLITQUERY_NAME, splitCondition ) ;
         bob.append ( CAT_SPLITENDQUERY_NAME, splitEndCondition ) ;
         bob.appendBool ( FIELD_NAME_ASYNC, TRUE ) ;
         newObj = bob.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                                    0, 0, 0, -1,
                                    newObj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      cursor = (_sdbCursor*) (new(std::nothrow) sdbCursorImpl () ) ;
      if ( !cursor  )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)cursor)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)cursor)->_setConnection ( _connection ) ;
      rc = cursor->next ( countObj ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_UNEXPECTED_RESULT ;
         }
      }
      else
      {
         BSONElement ele = countObj.getField ( FIELD_NAME_TASKID ) ;
         if ( ele.type() != NumberLong )
         {
            rc = SDB_UNEXPECTED_RESULT ;
         }
         else
         {
            taskID = ele.numberLong () ;
         }
      }
      delete ( cursor ) ;
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_SPLITASYNC, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_SPLITASYNC2, "_sdbCollectionImpl::splitAsync2" )
   INT32 _sdbCollectionImpl::splitAsync ( const CHAR *pSourceGroupName,
                                          const CHAR *pTargetGroupName,
                                          FLOAT64 percent,
                                          SINT64 &taskID )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_SPLITASYNC2 ) ;
      INT32 rc                 = SDB_OK ;
      BOOLEAN result ;
      SINT64 contextID    = 0 ;
      _sdbCursor *cursor  = NULL ;
      BSONObjBuilder bob ;
      BSONObj newObj ;
      BSONObj countObj ;
      BOOLEAN locked = FALSE ;
      if ( _collectionFullName [0] == '\0' || !_connection ||
         !pSourceGroupName || 0 == ossStrcmp ( pSourceGroupName, "" ) ||
         !pTargetGroupName || 0 == ossStrcmp ( pTargetGroupName, "" ) )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( percent <= 0.0 || percent > 100.0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         bob.append ( CAT_COLLECTION_NAME, _collectionFullName ) ;
         bob.append ( CAT_SOURCE_NAME, pSourceGroupName ) ;
         bob.append ( CAT_TARGET_NAME, pTargetGroupName ) ;
         bob.append ( CAT_SPLITPERCENT_NAME, percent ) ;
         bob.appendBool ( FIELD_NAME_ASYNC, TRUE ) ;
         newObj = bob.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                                    0, 0, -1, -1,
                                    newObj.objdata(),
                                    NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      cursor = (_sdbCursor*) (new(std::nothrow) sdbCursorImpl () ) ;
      if ( !cursor  )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)cursor)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)cursor)->_setConnection ( _connection ) ;
      rc = cursor->next ( countObj ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_UNEXPECTED_RESULT ;
         }
      }
      else
      {
         BSONElement ele = countObj.getField ( FIELD_NAME_TASKID ) ;
         if ( ele.type() != NumberLong )
         {
            rc = SDB_UNEXPECTED_RESULT ;
         }
         else
         {
            taskID = ele.numberLong () ;
         }
      }
      delete ( cursor ) ;
   done :
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_SPLITASYNC2, rc ) ;
   return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_AGGREGATE,"_sdbCollectionImpl::aggregate" )
   INT32 _sdbCollectionImpl::aggregate ( _sdbCursor **cursor,
                            std::vector<bson::BSONObj> &obj )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_AGGREGATE ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      SINT32 count = 0 ;
      SINT32 num = obj.size() ;

      if ( _collectionFullName[0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      for ( count = 0; count < num; ++count )
      {
         if ( 0 == count )
         rc = clientBuildAggrRequestCpp( &_pSendBuffer, &_sendBufferSize,
                                           _collectionFullName, obj[count].objdata(),
                                           _connection->_endianConvert ) ;
         else
         rc = clientAppendAggrRequestCpp ( &_pSendBuffer, &_sendBufferSize,
                                           obj[count].objdata(), _connection->_endianConvert ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                    contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      if ( *cursor )
      {
         delete *cursor ;
         *cursor = NULL ;
      }
      *cursor = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl() ) ;
      if ( !*cursor )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*cursor)->_setCollection( this ) ;
      ((_sdbCursorImpl*)*cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)*cursor)->_setConnection( _connection ) ;
   done:
      if ( locked )
      {
         _connection->unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_AGGREGATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_ATTACHCOLLECTION,"_sdbCollectionImpl::attachCollection" )
   INT32 _sdbCollectionImpl::attachCollection ( const CHAR *subClFullName,
                                                const bson::BSONObj &options)
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_ATTACHCOLLECTION ) ;
      INT32 rc         = SDB_OK ;
      BOOLEAN locked   = FALSE ;
      SINT64 contextID = 0 ;
      INT32 nameLength = 0 ;
      BOOLEAN result ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_LINK_CL ) ;
      if ( !subClFullName || !_connection ||
         ( nameLength = ossStrlen ( subClFullName) ) >
           CLIENT_COLLECTION_NAMESZ ||
           _collectionFullName[0] == '\0' )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ob.append ( FIELD_NAME_NAME, _collectionFullName ) ;
      ob.append ( FIELD_NAME_SUBCLNAME, subClFullName ) ;
      {
         BSONObjIterator it( options ) ;
         while ( it.more() )
         {
            ob.append ( it.next() ) ;
         }
      }
      newObj = ob.obj() ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     command.c_str(), 0, 0, 0,
                                     -1, newObj.objdata(),
                                     NULL, NULL, NULL,
                                     _connection->_endianConvert ) ;
      if ( rc )
      {
         goto done ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                        contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done:
      if ( locked )
      {
         _connection->unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_ATTACHCOLLECTION, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DETACHCOLLECTION,"_sdbCollectionImpl::detachCollection" )
   INT32 _sdbCollectionImpl::detachCollection ( const CHAR *subClFullName)
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DETACHCOLLECTION ) ;
      INT32 rc         = SDB_OK ;
      BOOLEAN locked   = FALSE ;
      SINT64 contextID = 0 ;
      INT32 nameLength = 0 ;
      BOOLEAN result ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_UNLINK_CL ) ;
      if ( !subClFullName || !_connection ||
            (nameLength = ossStrlen ( subClFullName) ) >
            CLIENT_COLLECTION_NAMESZ ||
            _collectionFullName[0] == '\0' )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ob.append ( FIELD_NAME_NAME, _collectionFullName ) ;
      ob.append ( FIELD_NAME_SUBCLNAME, subClFullName ) ;
      newObj = ob.obj() ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     command.c_str(), 0, 0, 0,
                                     -1, newObj.objdata(),
                                     NULL, NULL, NULL,
                                     _connection->_endianConvert ) ;
      if ( rc )
      {
         goto done ;
      }
      _connection->lock () ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                        contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done:
      if ( locked )
      {
         _connection->unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_DETACHCOLLECTION, rc ) ;
      return rc ;
   error:
      goto done ;
   }

//    PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_ALTERCOLLECTION, "_sdbCollectionImpl::alterCollection" )
   INT32 _sdbCollectionImpl::alterCollection ( const bson::BSONObj &options )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_ALTERCOLLECTION ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      BSONObjBuilder bob ;
      BSONObj newObj ;
      string collectionS ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_ALTER_COLLECTION ) ;
      if ( '\0' == _collectionFullName[0] || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      collectionS = string (_collectionFullName) ;
      try
      {
         bob.append ( FIELD_NAME_NAME, collectionS ) ;
         bob.append ( FIELD_NAME_OPTIONS, options ) ;
         newObj = bob.obj() ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = _connection->_runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_ALTERCOLLECTION, rc );
      return rc ;
   error :
      goto done ;

   }

//    PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_EXPLAIN, "_sdbCollectionImpl::explain" )
   INT32 _sdbCollectionImpl::explain ( 
                              _sdbCursor **cursor,
                              const bson::BSONObj &condition,
                              const bson::BSONObj &select,
                              const bson::BSONObj &orderBy,
                              const bson::BSONObj &hint,
                              INT64 numToSkip,
                              INT64 numToReturn,
                              INT32 flag,
                              const bson::BSONObj &options )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_EXPLAIN ) ;
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;
      BSONObj newObj ;

      if ( '\0' == _collectionFullName[0] || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         bob.append( FIELD_NAME_HINT, hint ) ;
         bob.append( FIELD_NAME_OPTIONS, options ) ;
         newObj= bob.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = query( cursor, condition, select, orderBy, newObj,
                  numToSkip, numToReturn, flag | FLG_QUERY_EXPLAIN ) ;
      if ( rc )
      {
         goto error ;
      }  

   done:
      PD_TRACE_EXITRC ( SDB_CLIENT_EXPLAIN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATELOB, "_sdbCollectionImpl::createLob" )
   INT32 _sdbCollectionImpl::createLob( _sdbLob **lob, const bson::OID *oid )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATELOB ) ;
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;
      BSONObj obj ;
      SINT64 contextID = -1 ;
      BOOLEAN result = FALSE ;
      BOOLEAN locked = FALSE ;
      const CHAR *bsonBuf = NULL ;
      OID oidObj ;

      if ( '\0' == _collectionFullName[0] || NULL == _connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( oid )
      {
         oidObj = *oid ;
      }
      else
      {
         oidObj = OID::gen() ;
      }
      try
      {
         bob.append( FIELD_NAME_COLLECTION, _collectionFullName ) ;
         bob.appendOID( FIELD_NAME_LOB_OID, &oidObj ) ;
         bob.append( FIELD_NAME_LOB_OPEN_MODE, SDB_LOB_CREATEONLY ) ;
         obj = bob.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = clientBuildOpenLobMsgCpp( &_pSendBuffer, &_sendBufferSize,
                                     obj.objdata(), 0, 1, 0,
                                     _connection->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      _connection->lock() ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      bsonBuf = _pReceiveBuffer + sizeof( MsgOpReply ) ;
      try
      {
         obj = BSONObj( bsonBuf ).getOwned() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      if ( *lob )
      {
         delete *lob ;
         *lob = NULL ;
      }
      *lob = (_sdbLob*)( new(std::nothrow) _sdbLobImpl() ) ;
      if ( !(*lob) )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbLobImpl*)*lob)->_setConnection( _connection ) ;
      ((_sdbLobImpl*)*lob)->_setCollection( this ) ;
      ((_sdbLobImpl*)*lob)->_oid = oidObj ;      
      ((_sdbLobImpl*)*lob)->_contextID = contextID ;
      ((_sdbLobImpl*)*lob)->_isOpen = TRUE ;
      ((_sdbLobImpl*)*lob)->_mode = SDB_LOB_CREATEONLY ;
      ((_sdbLobImpl*)*lob)->_lobSize = 0 ;
      ((_sdbLobImpl*)*lob)->_createTime = 0 ;

   done:
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATELOB, rc ) ;
      return rc ;
   error:
      if ( *lob )
      {
         delete *lob ;
         *lob = NULL ;
      }
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_REMOVELOB, "_sdbCollectionImpl::removeLob" )
   INT32 _sdbCollectionImpl::removeLob( const bson::OID &oid )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_REMOVELOB ) ;
      INT32 rc = SDB_OK ;
      SINT64 contextID = -1 ;
      BOOLEAN result = FALSE ;
      BOOLEAN locked = FALSE ;
      BSONObjBuilder bob ;
      BSONObj meta ;
      
      if ( '\0' == _collectionFullName[0] || NULL == _connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         bob.append( FIELD_NAME_COLLECTION, _collectionFullName ) ;
         bob.appendOID( FIELD_NAME_LOB_OID, (OID *)(&oid) ) ;
         meta = bob.obj() ;         
      }
      catch ( std::exception &e )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = clientBuildRemoveLobMsgCpp( &_pSendBuffer, &_sendBufferSize,
                                       meta.objdata(), 0, 1, 0,
                                       _connection->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      _connection->lock() ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
   done:
      if ( locked )
         _connection->unlock() ;
      PD_TRACE_EXITRC ( SDB_CLIENT_REMOVELOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_OPENLOB, "_sdbCollectionImpl::openLob" )
   INT32 _sdbCollectionImpl::openLob( _sdbLob **lob, const bson::OID &oid )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_OPENLOB ) ;
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;
      BSONObj obj ;
      BSONElement ele ;
      BSONType bType ;
      SINT64 contextID = -1 ;
      BOOLEAN result = FALSE ;
      BOOLEAN locked = FALSE ;
      const CHAR *bsonBuf = NULL ;

      if ( '\0' == _collectionFullName[0] || NULL == _connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         bob.append( FIELD_NAME_COLLECTION, _collectionFullName ) ;
         bob.appendOID( FIELD_NAME_LOB_OID, (bson::OID *)&oid ) ;
         bob.append( FIELD_NAME_LOB_OPEN_MODE, SDB_LOB_READ ) ;
         obj = bob.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = clientBuildOpenLobMsgCpp( &_pSendBuffer, &_sendBufferSize,
                                     obj.objdata(), 0, 1, 0,
                                     _connection->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      _connection->lock() ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      bsonBuf = _pReceiveBuffer + sizeof( MsgOpReply ) ;
      try
      {
         obj = BSONObj( bsonBuf ).getOwned() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      if ( *lob )
      {
         delete *lob ;
         *lob = NULL ;
      }
      *lob = (_sdbLob*)( new(std::nothrow) _sdbLobImpl() ) ;
      if ( !(*lob) )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbLobImpl*)*lob)->_setConnection( _connection ) ;
      ((_sdbLobImpl*)*lob)->_setCollection( this ) ;
      ((_sdbLobImpl*)*lob)->_oid = oid ;    
      ((_sdbLobImpl*)*lob)->_contextID = contextID ;
      ((_sdbLobImpl*)*lob)->_isOpen = TRUE ;
      ((_sdbLobImpl*)*lob)->_mode = SDB_LOB_READ ;
      ele = obj.getField( FIELD_NAME_LOB_SIZE ) ;
      bType = ele.type() ;
      if ( NumberInt == bType || NumberLong == bType )
      {
         ((_sdbLobImpl*)*lob)->_lobSize = ele.numberLong() ;
      }
      else
      {
         rc = SDB_SYS ;
         goto error ;
      }
      ele = obj.getField( FIELD_NAME_LOB_CREATTIME ) ;
      bType = ele.type() ;
      if ( NumberLong == bType )
      {
         ((_sdbLobImpl*)*lob)->_createTime = ele.numberLong() ;
      }
      else
      {
         rc = SDB_SYS ;
         goto error ;
      }
      ele = obj.getField( FIELD_NAME_LOB_PAGE_SIZE ) ;
      bType = ele.type() ;
      if ( NumberInt == bType )
      {
         ((_sdbLobImpl*)*lob)->_pageSize =  ele.numberInt() ;
      }
      else
      {
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      if ( locked )
         _connection->unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_OPENLOB, rc ) ;
      return rc ;
   error:
      if ( *lob )
      {
         delete *lob ;
         *lob = NULL ;
      }
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_LISTLOBS, "_sdbCollectionImpl::listLobs" )
   INT32 _sdbCollectionImpl::listLobs ( _sdbCursor **cursor )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_REMOVELOB ) ;
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;
      BSONObj obj ;
      
      if ( '\0' == _collectionFullName[0] || NULL == _connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         bob.append( FIELD_NAME_COLLECTION, _collectionFullName ) ;
         obj = bob.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = _runCmdOfLob( CMD_ADMIN_PREFIX CMD_NAME_LIST_LOBS, obj, cursor ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CLIENT_REMOVELOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_RUNCMDOFLOB, "_sdbCollectionImpl::_runCmdOfLob" )
   INT32 _sdbCollectionImpl::_runCmdOfLob ( const CHAR *cmd, const BSONObj &obj,
                                            _sdbCursor **cursor )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_RUNCMDOFLOB ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result = FALSE ;
      BOOLEAN locked = FALSE ;
      SINT64 contextID = -1 ;

      if ( '\0' == _collectionFullName[0] || NULL == _connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( NULL == cursor )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    cmd, 0, 0, -1, -1,
                                    obj.objdata(), NULL, NULL, NULL,
                                    _connection->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      _connection->lock() ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      if ( -1 != contextID )
      {
         if ( *cursor )
         {
            delete *cursor ;
            *cursor = NULL ;
         }
         *cursor = (_sdbCursor*)( new(std::nothrow) _sdbCursorImpl () ) ;
         if ( NULL == *cursor )
         {
            rc = SDB_OOM ;
            goto error ;
         }
         ((_sdbCursorImpl*)*cursor)->_setCollection ( this ) ;
         ((_sdbCursorImpl*)*cursor)->_contextID = contextID ;
         ((_sdbCursorImpl*)*cursor)->_setConnection ( _connection ) ;
      }

   done:
      if ( locked )
         _connection->unlock() ;
      PD_TRACE_EXITRC ( SDB_CLIENT_RUNCMDOFLOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }
/*
   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETLOBOBJ, "_sdbCollectionImpl::getLobObj" )
   _sdbLob* _sdbCollectionImpl::getLobObj ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETLOBOBJ ) ;
      _sdbLobImpl* pLob = NULL ;
      pLob = new(std::nothrow) _sdbLobImpl () ;
      pLob->_setCollection ( this ) ;
      pLob->_setConnection ( _connection ) ;
      return (_sdbLob*)( pLob ) ;
      PD_TRACE_EXIT ( SDB_CLIENT_GETLOBOBJ );
   }
*/

   /*
    * _sdbNodeImpl
    * Sdb Node Implementation
    */
   _sdbNodeImpl::_sdbNodeImpl () :
   _connection ( NULL )
   {
      ossMemset ( _hostName, 0, sizeof(_hostName) ) ;
      ossMemset ( _serviceName, 0, sizeof(_serviceName) ) ;
      ossMemset ( _nodeName, 0, sizeof(_nodeName) ) ;
      _nodeID = SDB_NODE_INVALID_NODEID ;
   }

   _sdbNodeImpl::~_sdbNodeImpl ()
   {
      if ( _connection )
      {
         _connection->_unregNode ( this ) ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CONNECTINRN, "_sdbNodeImpl::connect" )
   INT32 _sdbNodeImpl::connect ( _sdb **dbConn )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CONNECTINRN ) ;
      INT32 rc = SDB_OK ;
      _sdbImpl *connection = NULL ;
      if ( !_connection || !dbConn || ossStrlen ( _hostName ) == 0 ||
           ossStrlen ( _serviceName ) == 0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      connection = new (std::nothrow) _sdbImpl () ;
      if ( !connection )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      rc = connection->connect ( _hostName, _serviceName ) ;
      if ( rc )
      {
         goto error ;
      }
      *dbConn = connection ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CONNECTINRN, rc );
      return rc ;
   error :
      if ( connection )
         delete connection ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETSTATUS, "_sdbNodeImpl::getStatus" )
   sdbNodeStatus _sdbNodeImpl::getStatus ( )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETSTATUS ) ;
      sdbNodeStatus sns     = SDB_NODE_UNKNOWN ;
      INT32 rc              = SDB_OK ;
      SINT64 contextID      = 0 ;
      BOOLEAN r             = FALSE ;
      CHAR *_pSendBuffer    = NULL ;
      CHAR *_pReceiveBuffer = NULL ;
      INT32 _sendBufferSize = 0 ;
      INT32 _receiveBufferSize = 0 ;
      BSONObj condition ;
      condition = BSON ( CAT_GROUPID_NAME << _replicaGroupID <<
                         CAT_NODEID_NAME << _nodeID ) ;

      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_DATABASE,
                                    0, 0, 0, -1, condition.objdata(), NULL, NULL,
                                    NULL,
                                    _connection->_endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, r ) ;
      if ( rc )
      {
         if ( SDB_NET_CANNOT_CONNECT == rc )
         {
            sns = SDB_NODE_INACTIVE ;
         }
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      sns = SDB_NODE_ACTIVE ;
   done :
      if ( _pSendBuffer )
      {
         SDB_OSS_FREE ( _pSendBuffer ) ;
      }
      if ( _pReceiveBuffer )
      {
         SDB_OSS_FREE ( _pReceiveBuffer ) ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_GETSTATUS, sns );
      return sns ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__STOPSTART, "_sdbNodeImpl::_stopStart" )
   INT32 _sdbNodeImpl::_stopStart ( BOOLEAN start )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__STOPSTART ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result = FALSE ;
      BSONObj configuration ;
      if ( !_connection || ossStrlen ( _hostName ) == 0 ||
           ossStrlen ( _serviceName ) == 0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      configuration = BSON ( CAT_HOST_FIELD_NAME << _hostName <<
                             PMD_OPTION_SVCNAME << _serviceName ) ;
      rc = _connection->_runCommand ( start?
                                      (CMD_ADMIN_PREFIX CMD_NAME_STARTUP_NODE) :
                                      (CMD_ADMIN_PREFIX CMD_NAME_SHUTDOWN_NODE),
                                      result,
                                      &configuration );
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT__STOPSTART, rc );
      return rc ;
   error :
      goto done ;
   }

/*   INT32 _sdbNodeImpl::modifyConfig ( std::map<std::string,std::string>
                                             &config )
   {
      INT32 rc = SDB_OK ;
      if ( !_connection || _nodeID == SDB_NODE_INVALID_NODEID )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = _connection->modifyConfig ( _nodeID, config ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      return rc ;
   error :
      goto done ;
   } */

   _sdbReplicaGroupImpl::_sdbReplicaGroupImpl () :
   _connection ( NULL )
   {
      ossMemset ( _replicaGroupName, 0, sizeof(_replicaGroupName) ) ;
      _isCatalog = FALSE ;
      _replicaGroupID = 0 ;
   }

   _sdbReplicaGroupImpl::~_sdbReplicaGroupImpl ()
   {
      if ( _connection )
      {
         _connection->_unregReplicaGroup ( this ) ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETNODENUM, "_sdbReplicaGroupImpl::getNodeNum" )
   INT32 _sdbReplicaGroupImpl::getNodeNum ( sdbNodeStatus status, INT32 *num )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETNODENUM ) ;
      INT32 rc = SDB_OK ;
      BSONObj result ;
      BSONElement ele ;
      INT32 totalCount = 0 ;
      if ( !num )
      {
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      rc = getDetail ( result ) ;
      if ( rc )
      {
         goto error ;
      }
      ele = result.getField ( CAT_GROUP_NAME ) ;
      if ( ele.type() == Array )
      {
         BSONObjIterator it ( ele.embeddedObject() ) ;
         while ( it.more() )
         {
            it.next () ;
            ++totalCount ;
         }
      }
      *num = totalCount ;
   exit:
      PD_TRACE_EXITRC ( SDB_CLIENT_GETNODENUM, rc );
      return rc ;
   done :
      goto exit ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATENODE, "_sdbReplicaGroupImpl::createNode" )
   INT32 _sdbReplicaGroupImpl::createNode ( const CHAR *pHostName,
                                            const CHAR *pServiceName,
                                            const CHAR *pDatabasePath,
                                            map<string,string> &config )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATENODE ) ;
      INT32 rc = SDB_OK ;
      BSONObj configuration ;
      BSONObjBuilder ob ;
      BOOLEAN result ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_CREATE_NODE ) ;
      map<string,string>::iterator it ;
      if ( !_connection || ossStrlen ( _replicaGroupName ) == 0 ||
           !pHostName || !pServiceName || !pDatabasePath )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ob.append ( CAT_GROUPNAME_NAME, _replicaGroupName ) ;
      config.erase ( CAT_GROUPNAME_NAME ) ;

      ob.append ( CAT_HOST_FIELD_NAME, pHostName ) ;
      config.erase ( CAT_HOST_FIELD_NAME ) ;

      ob.append ( PMD_OPTION_SVCNAME, pServiceName ) ;
      config.erase ( PMD_OPTION_SVCNAME ) ;

      ob.append ( PMD_OPTION_DBPATH, pDatabasePath ) ;
      config.erase ( PMD_OPTION_DBPATH ) ;

      for ( it = config.begin(); it != config.end(); ++it )
      {
         ob.append ( it->first.c_str(),
                     it->second.c_str() ) ;
      }
      configuration = ob.obj () ;

      rc = _connection->_runCommand ( command.c_str(), result, &configuration );
      if ( rc )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATENODE, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_REMOVENODE, "_sdbReplicaGroupImpl::removeNode" )
   INT32 _sdbReplicaGroupImpl::removeNode ( const CHAR *pHostName,
                                                   const CHAR *pServiceName,
                                                   const BSONObj &configure )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_REMOVENODE ) ;
      INT32 rc = SDB_OK ;
      BSONObj removeInfo ;
      BSONObjBuilder ob ;
      const CHAR *pRemoveNode = CMD_ADMIN_PREFIX CMD_NAME_REMOVE_NODE ;
      BOOLEAN result = FALSE ;
      if ( !pHostName || !pServiceName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      ob.append ( CAT_GROUPNAME_NAME, _replicaGroupName ) ;
      ob.append ( FIELD_NAME_HOST, pHostName ) ;

      ob.append ( PMD_OPTION_SVCNAME, pServiceName ) ;

      {
         BSONObjIterator it ( configure ) ;
         while ( it.more() )
         {
            BSONElement ele = it.next () ;
            const CHAR *key = ele.fieldName() ;
            if ( ossStrcmp ( key, CAT_GROUPNAME_NAME ) == 0 ||
                 ossStrcmp ( key, FIELD_NAME_HOST ) == 0  ||
                 ossStrcmp ( key, PMD_OPTION_SVCNAME ) == 0 )
            {
               continue ;
            }
            ob.append ( ele ) ;
         } // while
      } // if ( configure )
      removeInfo = ob.obj () ;

      rc = _connection->_runCommand ( pRemoveNode, result, &removeInfo ) ;
      if ( rc )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_REMOVENODE, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETDETAIL, "_sdbReplicaGroupImpl::getDetail" )
   INT32 _sdbReplicaGroupImpl::getDetail ( BSONObj &result )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETDETAIL ) ;
      INT32 rc = SDB_OK ;
      BSONObj newObj ;
      sdbCursor cursor ;
      if ( !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( CAT_GROUPNAME_NAME << _replicaGroupName ) ;
      rc = _connection->getList ( &cursor.pCursor, SDB_LIST_GROUPS, newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = cursor.next ( result ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETDETAIL, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__EXTRACTNODE, "_sdbReplicaGroupImpl::_extractNode" )
   INT32 _sdbReplicaGroupImpl::_extractNode ( _sdbNode **node,
                                            const CHAR *primaryData )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__EXTRACTNODE ) ;
      INT32 rc = SDB_OK ;
      _sdbNodeImpl *pNode = NULL ;
      if ( !_connection || !node || !primaryData )
      {
         rc = SDB_INVALIDARG ;
         goto error1 ;
      }
      pNode = new ( std::nothrow) _sdbNodeImpl () ;
      if ( !pNode )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      pNode->_replicaGroupID = _replicaGroupID ;
      pNode->_connection = this->_connection ;
      _connection->_regNode ( pNode ) ;

      rc = clientReplicaGroupExtractNode ( primaryData,
                                           pNode->_hostName,
                                           OSS_MAX_HOSTNAME,
                                           pNode->_serviceName,
                                           OSS_MAX_SERVICENAME,
                                           &pNode->_nodeID ) ;
      if ( rc )
      {
         goto error ;
      }
      ossStrcpy ( pNode->_nodeName, pNode->_hostName ) ;
      ossStrncat ( pNode->_nodeName, NODE_NAME_SERVICE_SEP, 1 ) ;
      ossStrncat ( pNode->_nodeName, pNode->_serviceName,
                   OSS_MAX_SERVICENAME ) ;
   done :
      *node = pNode ;
      PD_TRACE_EXITRC ( SDB_CLIENT__EXTRACTNODE, rc );
      return rc ;
   error :
      if ( pNode )
      {
         delete pNode ;
         pNode = NULL ;
      }
   error1 :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETMASETER, "_sdbReplicaGroupImpl::getMaster" )
   INT32 _sdbReplicaGroupImpl::getMaster ( _sdbNode **node )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETMASETER ) ;
      INT32 rc = SDB_OK ;
      INT32 primaryNode = -1 ;
      const CHAR *primaryData = NULL ;
      BSONObj result ;
      BSONElement ele ;
      if ( !_connection || !node )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = getDetail ( result ) ;
      if ( rc )
      {
         goto error ;
      }
      ele = result.getField ( CAT_PRIMARY_NAME ) ;
      if ( ele.type() != NumberInt )
      {
         rc = SDB_CLS_NODE_NOT_EXIST ;
         goto error ;
      }
      primaryNode = ele.numberInt () ;
      ele = result.getField ( CAT_GROUP_NAME ) ;
      if ( ele.type() != Array )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      {
         BSONObjIterator it ( ele.embeddedObject() ) ;
         while ( it.more() )
         {
            BSONObj embObj ;
            BSONElement embEle = it.next() ;
            if ( Object == embEle.type() )
            {
               embObj = embEle.embeddedObject() ;
               BSONElement embEle1 = embObj.getField ( CAT_NODEID_NAME ) ;
               if ( embEle1.type() != NumberInt )
               {
                  rc = SDB_SYS ;
                  goto error ;
               }
               if ( primaryNode == embEle1.numberInt() )
               {
                  primaryData = embObj.objdata() ;
                  break ;
               }
            }
         }
      }
      if ( primaryData )
      {
         rc = _extractNode ( node, primaryData ) ;
         if ( rc )
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
      PD_TRACE_EXITRC ( SDB_CLIENT_GETMASETER, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETSLAVE, "_sdbReplicaGroupImpl::getSlave" )
   INT32 _sdbReplicaGroupImpl::getSlave ( _sdbNode **node )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETSLAVE ) ;
      INT32 rc = SDB_OK ;
      INT32 primaryNode = -1 ;
      const CHAR *primaryData = NULL ;
      BSONObj result ;
      BSONElement ele ;
      vector<const CHAR*> slaveElements ;
      if ( !_connection || !node )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = getDetail ( result ) ;
      if ( rc )
      {
         goto error ;
      }
      ele = result.getField ( CAT_PRIMARY_NAME ) ;
      if ( ele.type() == NumberInt )
      {
         primaryNode = ele.numberInt () ;
      }
      ele = result.getField ( CAT_GROUP_NAME ) ;
      if ( ele.type() != Array )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      {
         BSONObj objReplicaGroupList = ele.embeddedObject() ;
         BSONObjIterator it ( objReplicaGroupList ) ;
         while ( it.more() )
         {
            BSONObj embObj ;
            BSONElement embEle ;
            embEle = it.next() ;
            if ( embEle.type() == Object )
            {
               embObj = embEle.embeddedObject() ;
               BSONElement embEle1 = embObj.getField ( CAT_NODEID_NAME ) ;
               if ( embEle1.type() != NumberInt )
               {
                  rc = SDB_SYS ;
                  goto error ;
               }
               if ( primaryNode != embEle1.numberInt() )
               {
                  slaveElements.push_back ( embObj.objdata() ) ;
               }
               else
               {
                  primaryData = embObj.objdata() ;
               }
            } // if ( BSON_OBJECT == bson_iterator ( &i )
         }
      }
      if ( slaveElements.size() != 0 )
      {
         INT32 slaveID = _sdbRand() % slaveElements.size() ;
         rc = _extractNode ( node, slaveElements[slaveID] ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( primaryData )
      {
         rc = _extractNode ( node, primaryData ) ;
         if ( rc )
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
      PD_TRACE_EXITRC ( SDB_CLIENT_GETSLAVE, rc ); 
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETNODE1, "_sdbReplicaGroupImpl::getNode" )
   INT32 _sdbReplicaGroupImpl::getNode ( const CHAR *pHostName,
                                         const CHAR *pServiceName,
                                         _sdbNode **node )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETNODE1 ) ;
      INT32 rc = SDB_OK ;
      BSONObj result ;
      BSONElement ele ;
      if ( !_connection || !pHostName || !pServiceName || !node )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      *node = NULL ;
      rc = getDetail ( result ) ;
      if ( rc )
      {
         goto error ;
      }
      ele = result.getField ( CAT_GROUP_NAME ) ;
      if ( ele.type() != Array )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      {
         BSONObjIterator it ( ele.embeddedObject() ) ;
         while ( it.more() )
         {
            BSONElement embEle ;
            BSONObj embObj ;
            embEle = it.next() ;
            if ( embEle.type() == Object )
            {
               embObj = embEle.embeddedObject() ;
               rc = _extractNode ( node, embObj.objdata() ) ;
               if ( rc )
               {
                  goto error ;
               }
               if ( ossStrcmp ( ((_sdbNodeImpl*)(*node))->_hostName,
                                pHostName ) == 0 &&
                    ossStrcmp ( ((_sdbNodeImpl*)(*node))->_serviceName,
                                pServiceName ) == 0 )
                  break ;
               SDB_OSS_DEL ( (*node ) ) ;
               *node = NULL ;
            }
         }
      }
      if ( !(*node) )
      {
         rc = SDB_CLS_NODE_NOT_EXIST ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETNODE1, rc );
      return rc ;
   error :
      if ( node && *node )
      {
         SDB_OSS_FREE ( (*node) ) ;
         *node = NULL ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETNODE2, "_sdbReplicaGroupImpl::getNode" )
   INT32 _sdbReplicaGroupImpl::getNode ( const CHAR *pNodeName,
                                  _sdbNode **node )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETNODE2 ) ;
      INT32 rc = SDB_OK ;
      CHAR *pHostName = NULL ;
      CHAR *pServiceName = NULL ;
      INT32 nodeNameLen = 0 ;
      if ( !_connection || !pNodeName || !node )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      nodeNameLen = ossStrlen ( pNodeName ) ;
      pHostName = (CHAR*)SDB_OSS_MALLOC ( nodeNameLen +1 ) ;
      if ( !pHostName )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ossMemset ( pHostName, 0, nodeNameLen + 1 ) ;
      ossStrncpy ( pHostName, pNodeName, nodeNameLen + 1 ) ;
      pServiceName = ossStrchr ( pHostName, NODE_NAME_SERVICE_SEPCHAR ) ;
      if ( !pServiceName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      *pServiceName = '\0' ;
      pServiceName ++ ;
      rc = getNode ( pHostName, pServiceName, node ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      if ( pHostName )
         SDB_OSS_FREE ( pHostName ) ;
      PD_TRACE_EXITRC ( SDB_CLIENT_GETNODE2, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_STARTRS, "_sdbReplicaGroupImpl::start" )
   INT32 _sdbReplicaGroupImpl::start ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_STARTRS ) ;
      INT32 rc = SDB_OK ;
      BSONObj replicaGroupName ;
      BOOLEAN result = FALSE ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_ACTIVE_GROUP ) ;
      if ( !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      replicaGroupName = BSON ( CAT_GROUPNAME_NAME << _replicaGroupName ) ;
      rc = _connection->_runCommand ( command.c_str(), result, &replicaGroupName ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_STARTRS, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_STOPRS, "_sdbReplicaGroupImpl::stop" )
   INT32 _sdbReplicaGroupImpl::stop ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_STOPRS ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result = FALSE ;
      BSONObj configuration ;
      if ( !_connection || ossStrlen ( _replicaGroupName ) == 0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      configuration = BSON ( CAT_GROUPNAME_NAME << _replicaGroupName ) ;
      rc = _connection->_runCommand ( CMD_ADMIN_PREFIX CMD_NAME_SHUTDOWN_GROUP,
                                      result,
                                      &configuration );
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_STOPRS, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__SETNAME, "_sdbCollectionSpaceImpl::_setName" )
   INT32 _sdbCollectionSpaceImpl::_setName ( const CHAR *pCollectionSpaceName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__SETNAME ) ;
      INT32 rc = SDB_OK ;
      INT32 nameLength = ossStrlen ( pCollectionSpaceName ) ;
      ossMemset ( _collectionSpaceName, 0, sizeof ( _collectionSpaceName ) ) ;
      if ( nameLength > CLIENT_CS_NAMESZ )
      {
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      ossMemcpy ( _collectionSpaceName, pCollectionSpaceName,
                  nameLength );
   exit:
      PD_TRACE_EXITRC ( SDB_CLIENT__SETNAME, rc );
      return rc ;
   }

   /*
    * sdbCollectionSpaceImpl
    * Collection Space Implementation
    */
   _sdbCollectionSpaceImpl::_sdbCollectionSpaceImpl () :
   _connection ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ),
   _pReceiveBuffer ( NULL ),
   _receiveBufferSize ( 0 )
   {
      ossMemset ( _collectionSpaceName, 0, sizeof ( _collectionSpaceName ) ) ;
   }

   _sdbCollectionSpaceImpl::_sdbCollectionSpaceImpl (
         CHAR *pCollectionSpaceName ) :
   _connection ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ),
   _pReceiveBuffer ( NULL ),
   _receiveBufferSize ( 0 )
   {
      _setName ( pCollectionSpaceName ) ;
   }

   _sdbCollectionSpaceImpl::~_sdbCollectionSpaceImpl ()
   {
      if ( _connection )
      {
         _connection->_unregCollectionSpace ( this ) ;
      }
      if ( _pSendBuffer )
      {
         SDB_OSS_FREE ( _pSendBuffer ) ;
      }
      if ( _pReceiveBuffer )
      {
         SDB_OSS_FREE ( _pReceiveBuffer ) ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_SDBCSIMPL__SETCONNECTIONINCS, "_sdbCollectionSpaceImpl::_setConnection" )
   void _sdbCollectionSpaceImpl::_setConnection ( _sdb *connection )
   {
      PD_TRACE_ENTRY ( SDB_SDBCSIMPL__SETCONNECTIONINCS ) ;
      _connection = (_sdbImpl*)connection ;
      _connection->_regCollectionSpace ( this ) ;
      PD_TRACE_EXIT ( SDB_SDBCSIMPL__SETCONNECTIONINCS );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETCOLLECTIONINCS, "_sdbCollectionSpaceImpl::getCollection" )
   INT32 _sdbCollectionSpaceImpl::getCollection ( const CHAR *pCollectionName,
                                                  _sdbCollection **collection )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETCOLLECTIONINCS ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      BSONObj newObj ;
      string collectionS = string ("") ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_TEST_COLLECTION ) ;
      if ( !pCollectionName || ossStrlen ( pCollectionName ) >
                               CLIENT_COLLECTION_NAMESZ )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( _collectionSpaceName [0] == '\0' || !_connection || !collection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      collectionS = string (_collectionSpaceName) + "." +
                    string (pCollectionName) ;
      newObj = BSON ( FIELD_NAME_NAME << collectionS.c_str() ) ;

      rc = _connection->_runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( NULL != (*collection) )
      {
         delete *collection ;
         *collection = NULL ;
      }
      *collection = (_sdbCollection*)( new(std::nothrow) sdbCollectionImpl () ) ;
      if ( !*collection )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((sdbCollectionImpl*)*collection)->_setConnection ( _connection ) ;
      ((sdbCollectionImpl*)*collection)->_setName ( collectionS.c_str() ) ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETCOLLECTIONINCS, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _sdbCollectionSpaceImpl::createCollection ( const CHAR *pCollectionName,
                                                     _sdbCollection **collection )
   {
      return createCollection ( pCollectionName, _sdbStaticObject, collection ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATECOLLECTION, "_sdbCollectionSpaceImpl::createCollection" )
   INT32 _sdbCollectionSpaceImpl::createCollection ( const CHAR *pCollectionName,
                                                     const BSONObj &options,
                                                     _sdbCollection **collection )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATECOLLECTION ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string collectionS = string ("") ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTION ) ;
      if ( !pCollectionName || ossStrlen ( pCollectionName ) >
                               CLIENT_COLLECTION_NAMESZ )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( _collectionSpaceName [0] == '\0' || !_connection || !collection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      collectionS = string (_collectionSpaceName) + "." +
                    string (pCollectionName) ;
      ob.append ( FIELD_NAME_NAME, collectionS ) ;
      {
         BSONObjIterator it ( options ) ;
         while ( it.more() )
         {
            ob.append ( it.next() ) ;
         }
      }
      newObj = ob.obj () ;

      rc = _connection->_runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( *collection )
      {
         delete *collection ;
         *collection = NULL ;
      }
      *collection = (_sdbCollection*)( new(std::nothrow) sdbCollectionImpl () ) ;
      if ( !*collection )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((sdbCollectionImpl*)*collection)->_setConnection ( _connection ) ;
      ((sdbCollectionImpl*)*collection)->_setName ( collectionS.c_str() ) ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATECOLLECTION, rc );
      return rc ;
   error :
      goto done ;
   }

//   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DROPCOLLECTION, "_sdbCollectionSpaceImpl::dropCollection" )
   INT32 _sdbCollectionSpaceImpl::dropCollection ( const CHAR *pCollectionName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DROPCOLLECTION ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      BSONObj newObj ;
      string collectionS = string ("") ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTION ) ;
      if ( !pCollectionName || ossStrlen ( pCollectionName ) >
                               CLIENT_COLLECTION_NAMESZ )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( _collectionSpaceName [0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      collectionS = string (_collectionSpaceName) + "." +
                    string (pCollectionName) ;
      newObj = BSON ( FIELD_NAME_NAME << collectionS ) ;

      rc = _connection->_runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_DROPCOLLECTION, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATECS, "_sdbCollectionSpaceImpl::create" )
   INT32 _sdbCollectionSpaceImpl::create ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATECS ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      BSONObj newObj ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTIONSPACE ) ;
      if ( _collectionSpaceName [0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( FIELD_NAME_NAME << _collectionSpaceName ) ;

      rc = _connection->_runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATECS, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DROPCS, "_sdbCollectionSpaceImpl::drop" )
   INT32 _sdbCollectionSpaceImpl::drop ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DROPCS ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      BSONObj newObj ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTIONSPACE ) ;
      if ( _collectionSpaceName [0] == '\0' || !_connection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( FIELD_NAME_NAME << _collectionSpaceName ) ;
      rc = _connection->_runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_DROPCS, rc );
      return rc ;
   error :
      goto done ;
   }

   /*
    * sdbDomainImpl
    * SequoiaDB Domain Implementation
    */
   _sdbDomainImpl::_sdbDomainImpl () :
   _connection ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ) ,
   _pReceiveBuffer ( NULL ) ,
   _receiveBufferSize ( 0 )
   {
      ossMemset( _domainName, 0, sizeof ( _domainName ) ) ;
   }

   _sdbDomainImpl::_sdbDomainImpl ( const CHAR *pDomainName ) :
   _connection ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ) ,
   _pReceiveBuffer ( NULL ) ,
   _receiveBufferSize ( 0 )
   {
      _setName( pDomainName ) ;
   }

   _sdbDomainImpl::~_sdbDomainImpl ()
   {
      if ( _connection )
      {
         _connection->_unregDomain ( this ) ;
      }
      if ( _pSendBuffer )
      {
         SDB_OSS_FREE ( _pSendBuffer ) ;
      }
      if ( _pReceiveBuffer )
      {
         SDB_OSS_FREE ( _pReceiveBuffer ) ;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__DOMAINSETCONNECTION, "_sdbDomainImpl::_setConnection" )
   void _sdbDomainImpl::_setConnection ( _sdb *connection )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__DOMAINSETCONNECTION ) ;
      _connection = (_sdbImpl*)connection ;
      _connection->_regDomain ( this ) ;
      PD_TRACE_EXIT ( SDB_CLIENT__DOMAINSETCONNECTION );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__DOMAINSETNAME, "_sdbDomainImpl::_setName" )
   INT32 _sdbDomainImpl::_setName ( const CHAR *pDomainName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__DOMAINSETNAME ) ;
      INT32 rc = SDB_OK ;
      INT32 nameLength = ossStrlen ( pDomainName ) ;
      ossMemset ( _domainName, 0, sizeof ( _domainName ) ) ;
      if ( nameLength > CLIENT_CS_NAMESZ )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossMemcpy ( _domainName, pDomainName,
                  nameLength );
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT__DOMAINSETNAME, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_ALTERDOMAIN, "_sdbDomainImpl::alterDomain" )
   INT32 _sdbDomainImpl::alterDomain ( const bson::BSONObj &options )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_ALTERDOMAIN ) ;
      INT32 rc       = SDB_OK ;
      BOOLEAN result = FALSE ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_ALTER_DOMAIN ) ;
      if ( !_connection || '\0' == _domainName[0] )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         ob.append ( FIELD_NAME_NAME, _domainName ) ;
         ob.append ( FIELD_NAME_OPTIONS, options ) ;
         newObj = ob.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = _connection->_runCommand(command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_ALTERDOMAIN, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_LISTCSINDOMAIN, "_sdbDomainImpl::listCollectionSpacesInDomain" )
   INT32 _sdbDomainImpl::listCollectionSpacesInDomain ( _sdbCursor **cursor )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_LISTCSINDOMAIN ) ;
      INT32 rc = SDB_OK ;
      BSONObj condition ;
      BSONObj selector ;
      BSONObjBuilder ob1 ;
      BSONObjBuilder ob2 ;

      if ( !_connection || '\0' == _domainName[0] || !cursor )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         ob1.append ( FIELD_NAME_DOMAIN, _domainName ) ;
         condition = ob1.obj () ;
         ob2.appendNull ( FIELD_NAME_NAME ) ;
         selector = ob2.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = _connection->getList( cursor, SDB_LIST_CS_IN_DOMAIN, condition, selector ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_LISTCSINDOMAIN, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_LISTCLINDOMAIN, "_sdbDomainImpl::listCollectionsInDomain" )
   INT32 _sdbDomainImpl::listCollectionsInDomain ( _sdbCursor **cursor )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_LISTCLINDOMAIN ) ;
      INT32 rc = SDB_OK ;
      BSONObj condition ;
      BSONObj selector ;
      BSONObjBuilder ob1 ;
      BSONObjBuilder ob2 ;

      if ( !_connection || '\0' == _domainName[0] || !cursor )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         ob1.append ( FIELD_NAME_DOMAIN, _domainName ) ;
         condition = ob1.obj () ;
         ob2.appendNull ( FIELD_NAME_NAME ) ;
         selector = ob2.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = _connection->getList( cursor, SDB_LIST_CL_IN_DOMAIN, condition, selector ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_LISTCLINDOMAIN, rc );
      return rc ;
   error :
      goto done ;
   }

   /*
    * sdbLobImpl
    * SequoiaDB large object Implementation
    */
   _sdbLobImpl::_sdbLobImpl () :
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ),
   _pReceiveBuffer ( NULL ),
   _receiveBufferSize ( 0 ),
   _isOpen( FALSE ),
   _contextID ( -1 ),
   _mode( -1 ),
   _createTime( -1 ),
   _lobSize( -1 ),
   _currentOffset( 0 ),
   _cachedOffset( 0 ),
   _cachedSize( 0 ),
   _pageSize( 0 ),
   _dataCache( NULL )
   {
      _oid = OID() ;
   }

   _sdbLobImpl::~_sdbLobImpl ()
   {
      if ( _connection )
      {
         if ( _isOpen )
         {
            close() ;  
         }
         _connection->_unregLob ( this ) ;
         _connection = NULL ;
      }
      if ( _pSendBuffer )
      {
         SDB_OSS_FREE ( _pSendBuffer ) ;
      }
      if ( _pReceiveBuffer )
      {
         SDB_OSS_FREE ( _pReceiveBuffer ) ;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__SETCONNECTIONINLOB, "_setLobImpl::_setConnection" )
   void _sdbLobImpl::_setConnection ( _sdb *connection )
   {
      PD_TRACE_ENTRY ( SDB_SDBCSIMPL__SETCONNECTIONINCS ) ;
      _connection = (_sdbImpl*)connection ;
      if ( _connection )
      {
         _connection->_regLob ( this ) ;
      }
      PD_TRACE_EXIT ( SDB_SDBCSIMPL__SETCONNECTIONINCS );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__SETCOLLECTIONINLOB, "_sdbLobImpl::_setCollection" )
   void _sdbLobImpl::_setCollection ( _sdbCollectionImpl *collection )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__SETCOLLECTIONINLOB ) ;
      _collection = collection ;
      PD_TRACE_EXIT ( SDB_CLIENT__SETCOLLECTIONINLOB ) ;
   }

   void _sdbLobImpl::_cleanup()
   {
      _collection = NULL ;
      _contextID = -1 ;
      _mode = -1 ;
      _oid = bson::OID() ;
      _createTime = -1 ;
      _lobSize = -1 ;
      _currentOffset = 0 ;
      _cachedOffset = 0 ;
      _cachedSize = 0 ;
      _pageSize = 0 ;
      _dataCache = NULL ;
      if ( _pSendBuffer )
      {
         SAFE_OSS_FREE ( _pSendBuffer ) ;
         _sendBufferSize = 0 ;
      }
      if ( _pReceiveBuffer )
      {
         SAFE_OSS_FREE ( _pReceiveBuffer ) ;
         _receiveBufferSize = 0 ;
      }
   }

   BOOLEAN _sdbLobImpl::_dataCached()
   {
      return ( NULL != _dataCache && 0 < _cachedSize &&
               0 <= _cachedOffset &&
               _cachedOffset <= _currentOffset &&
               _currentOffset < ( _cachedOffset + _cachedSize ) ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__READINCACHE, "_setLobImpl::_readInCache" )
   void _sdbLobImpl::_readInCache( void *buf, UINT32 len, UINT32 *read )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__READINCACHE ) ;
      const CHAR *cache = NULL ;
      UINT32 readInCache = _cachedOffset + _cachedSize - _currentOffset ;
      readInCache = readInCache <= len ?
                    readInCache : len ;
      cache = _dataCache + _currentOffset - _cachedOffset ;
      ossMemcpy( buf, cache, readInCache ) ;
      _cachedSize -= readInCache + cache - _dataCache ;

      if ( 0 == _cachedSize )
      {
         _dataCache = NULL ;
         _cachedOffset = -1 ;
      }
      else
      {
         _dataCache = cache + readInCache ;
         _cachedOffset = readInCache + _currentOffset ;
      }

      *read = readInCache ;
      PD_TRACE_EXIT ( SDB_CLIENT__READINCACHE );
      return ;                        
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__REVISEREADLEN, "_setLobImpl::_reviseReadLen" )
   UINT32 _sdbLobImpl::_reviseReadLen( UINT32 needLen )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__REVISEREADLEN ) ;
      UINT32 pageSize = _pageSize ;
      UINT32 mod = _currentOffset & ( pageSize - 1 ) ;
      UINT32 alignedLen = ossRoundUpToMultipleX( needLen,
                                                 LOB_ALIGNED_LEN ) ;
      alignedLen -= mod ;
      if ( alignedLen < LOB_ALIGNED_LEN )
      {
         alignedLen += LOB_ALIGNED_LEN ;
      }
      PD_TRACE_EXIT ( SDB_CLIENT__REVISEREADLEN );
      return alignedLen ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__ONCEREAD, "_setLobImpl::_onceRead" )
   INT32 _sdbLobImpl::_onceRead( CHAR *buf, UINT32 len, UINT32 *read )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__ONCEREAD ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked ;
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

      if ( _dataCached() )
      {
         _readInCache( localBuf, needRead, &onceRead ) ;

         totalRead += onceRead ;
         needRead -= onceRead ;
         _currentOffset += onceRead ;
         localBuf += onceRead ;
         *read = totalRead ;
         goto done ;
      }

      _cachedOffset = -1 ;
      _cachedSize = 0 ;
      _dataCache = NULL ;

      alignedLen = _reviseReadLen( needRead ) ;

      rc = clientBuildReadLobMsg( &_pSendBuffer, &_sendBufferSize,
                                  alignedLen, _currentOffset,
                                  0, _contextID, 0,
                                  _connection->_endianConvert ) ;
      _connection->lock() ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      reply = ( const MsgOpReply * )( _pReceiveBuffer ) ;
      if ( ( UINT32 )( reply->header.messageLength ) <
           ( sizeof( MsgOpReply ) + sizeof( MsgLobTuple ) ) )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      tuple = ( const MsgLobTuple *)
              ( _pReceiveBuffer + sizeof( MsgOpReply ) ) ;
      if ( _currentOffset != tuple->columns.offset )
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

      body = _pReceiveBuffer + sizeof( MsgOpReply ) + sizeof( MsgLobTuple ) ;

      if ( needRead < tuple->columns.len )
      {
         ossMemcpy( localBuf, body, needRead ) ;
         totalRead += needRead ;
         _currentOffset += needRead ;
         _cachedOffset = _currentOffset ;
         _cachedSize = tuple->columns.len - needRead ;
         _dataCache = body + needRead ;
      }
      else
      {
         ossMemcpy( localBuf, body, tuple->columns.len ) ;
         totalRead += tuple->columns.len ;
         _currentOffset += tuple->columns.len ;
         _cachedOffset = -1 ;
         _cachedSize = 0 ;
         _dataCache = NULL ;
      }

      *read = totalRead ;
   done:
      if ( locked )
         _connection->unlock() ;
      PD_TRACE_EXIT ( SDB_CLIENT__ONCEREAD );
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CLOSE, "_sdbLobImpl::close" )
   INT32 _sdbLobImpl::close ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CLIENT_CLOSE ) ;
      SINT64 contextID = -1 ;
      BOOLEAN result = FALSE ;
      BOOLEAN locked = FALSE ;

      if (  !_connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error;
      }
      _connection->lock() ;
      locked = TRUE ;
      if ( !_isOpen )
      {
         goto done ;
      }
      locked = FALSE;
      _connection->unlock() ;
      rc = clientBuildCloseLobMsg( &_pSendBuffer, &_sendBufferSize,
                                   0, 1, _contextID, 0,
                                   _connection->_endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      _connection->lock() ;
      locked = TRUE ;
      rc = _connection->_send ( _pSendBuffer ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
      _cleanup() ;
      _isOpen = FALSE ;

   done:
      if ( locked )
      {
         _connection->unlock() ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_CLOSE, rc );
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_READ, "_sdbLobImpl::read" )
   INT32 _sdbLobImpl::read ( UINT32 len, CHAR *buf, UINT32 *read )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_READ ) ;
      INT32 rc = SDB_OK ;
      UINT32 needRead = len ;
      CHAR *localBuf = buf ;
      UINT32 onceRead = 0 ;
      UINT32 totalRead = 0 ;
      BOOLEAN locked = FALSE ;
      
      if (  !_connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error;
      }
      _connection->lock() ;
      locked = TRUE ;
      if ( !_isOpen )
      {
         rc = SDB_LOB_NOT_OPEN ;
         goto error ;
      }
      locked = FALSE;
      _connection->unlock() ;
      if (  NULL == buf )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( SDB_LOB_READ != _mode || -1 == _contextID )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( 0 == len )
      {
         *read = 0 ;
         goto done ;
      }
      
      if ( _currentOffset == _lobSize )
      {
         rc = SDB_EOF ;
         goto error ;
      }

      while ( 0 < needRead && _currentOffset < _lobSize )
      {
         rc = _onceRead( localBuf, needRead, &onceRead ) ;
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
      if ( locked )
         _connection->unlock() ;
      PD_TRACE_EXITRC ( SDB_CLIENT_READ, rc );
      return rc ;
   error:
      *read = 0 ;
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_WRITE, "_sdbLobImpl::write" )
   INT32 _sdbLobImpl::write ( const CHAR *buf, UINT32 len )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_WRITE ) ;
      INT32 rc = SDB_OK ;
      SINT64 contextID = -1 ;
      BOOLEAN result = FALSE ;
      BOOLEAN locked = FALSE ;
      UINT32 totalLen = 0 ;
      const UINT32 maxSendLen = 2 * 1024 * 1024 ;
      
      if (  !_connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error;
      }
      _connection->lock() ;
      locked = TRUE ;
      if ( !_isOpen )
      {
         rc = SDB_LOB_NOT_OPEN ;
         goto error ;
      }
      locked = FALSE;
      _connection->unlock() ;
      if ( NULL == buf )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( SDB_LOB_CREATEONLY != _mode )
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
         rc = clientBuildWriteLobMsg( &_pSendBuffer, &_sendBufferSize,
                                      buf + totalLen, sendLen, -1, 0, 1,
                                      _contextID, 0,
                                      _connection->_endianConvert ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         _connection->lock() ;
         locked = TRUE ;
         rc = _connection->_send ( _pSendBuffer ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         rc = _connection->_recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                          contextID, result ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, _connection ) ;
         locked = FALSE ;
         _connection->unlock() ;
         
         totalLen += sendLen ;
      } while ( totalLen < len ) ;
      _lobSize += len ;
   done:
      if ( locked )
         _connection->unlock() ;
      PD_TRACE_EXITRC ( SDB_CLIENT_WRITE, rc );
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_SEEK, "_sdbLobImpl::seek" )
   INT32 _sdbLobImpl::seek ( SINT64 size, SDB_LOB_SEEK whence )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_SEEK ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      
      if (  !_connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error;
      }
      _connection->lock() ;
      locked = TRUE ;
      if ( !_isOpen )
      {
         rc = SDB_LOB_NOT_OPEN ;
         goto error ;
      }
      locked = FALSE;
      _connection->unlock() ;
      if ( SDB_LOB_READ != _mode || -1 == _contextID )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( SDB_LOB_SEEK_SET == whence )
      {
         if ( size < 0 || _lobSize < size )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         _currentOffset = size ;
      }
      else if ( SDB_LOB_SEEK_CUR == whence )
      {
         if ( _lobSize < size + _currentOffset ||
              size + _currentOffset < 0 )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         _currentOffset += size ;
      }
      else if ( SDB_LOB_SEEK_END == whence )
      {
         if ( size < 0 || _lobSize < size )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         _currentOffset = _lobSize - size ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      if ( locked )
         _connection->unlock() ;
      PD_TRACE_EXITRC ( SDB_CLIENT_SEEK, rc );
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_ISCLOSED2, "_sdbLobImpl::isClosed" )
    INT32 _sdbLobImpl::isClosed( BOOLEAN &flag )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_ISCLOSED2 ) ;
      INT32 rc = SDB_OK ;
      flag = isClosed();
      PD_TRACE_EXIT ( SDB_CLIENT_ISCLOSED2 );
      return rc ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETOID2, "_sdbLobImpl::getOid" )
   INT32 _sdbLobImpl::getOid( bson::OID &oid )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETOID2 ) ;
      INT32 rc = SDB_OK ;

      if (  !_connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error;
      }
      oid = getOid() ;
   done:
      PD_TRACE_EXITRC ( SDB_CLIENT_GETOID2, rc );
      return rc ;
   error:
      goto done ;
   }
   
   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETSIZE2, "_sdbLobImpl::getSize" )
   INT32 _sdbLobImpl::getSize( SINT64 *size )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETSIZE2 ) ;
      INT32 rc = SDB_OK ;

      if (  !_connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error;
      }
      *size = getSize() ;
   done:
      PD_TRACE_EXITRC ( SDB_CLIENT_GETSIZE2, rc );
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETCREATETIME2, "_sdbLobImpl::getCreateTime" )
   INT32 _sdbLobImpl::getCreateTime ( UINT64 *millis )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETCREATETIME2 ) ;
      INT32 rc = SDB_OK ;

      if (  !_connection )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error;
      }
      *millis = getCreateTime() ;
   done:
      PD_TRACE_EXITRC ( SDB_CLIENT_GETCREATETIME2, rc );
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_ISCLOSED, "_sdbLobImpl::isClosed" )
    BOOLEAN _sdbLobImpl::isClosed()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_ISCLOSED ) ;
      BOOLEAN flag = TRUE ;
      if ( NULL == _connection )
      {
         return flag ;
      }
      _connection->lock() ;
      flag = !_isOpen ;
      _connection->unlock() ;
      PD_TRACE_EXIT ( SDB_CLIENT_ISCLOSED ) ;
      return flag ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETOID, "_sdbLobImpl::getOid" )
   bson::OID _sdbLobImpl::getOid()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETOID ) ;
      bson::OID oid = bson::OID() ;
      if (  !_connection )
      {
         return oid ;
      }
      _connection->lock() ;
      oid = _oid ;
      _connection->unlock() ;
      PD_TRACE_EXIT ( SDB_CLIENT_GETOID ) ;
      return oid ;
   }
   
   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETSIZE, "_sdbLobImpl::getSize" )
   SINT64 _sdbLobImpl::getSize()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETSIZE ) ;
      SINT64 size = 0 ;

      if ( !_connection )
      {
         return -1 ;
      }
      _connection->lock() ;
      size = _lobSize ;
      _connection->unlock() ;
      PD_TRACE_EXIT ( SDB_CLIENT_GETSIZE );
      return size ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETCREATETIME, "_sdbLobImpl::getCreateTime" )
   UINT64 _sdbLobImpl::getCreateTime ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETCREATETIME ) ;
      UINT64 millis = 0 ;
      if ( !_connection )
      {
         return -1 ;
      }
      _connection->lock() ;
      millis = _createTime ;
      _connection->unlock() ;
      PD_TRACE_EXIT ( SDB_CLIENT_GETCREATETIME );
      return millis ;
   }

   /*
    * sdbImpl
    * SequoiaDB Connection Implementation
    */
   _sdbImpl::_sdbImpl () :
   _sock ( NULL ),
   _pSendBuffer ( NULL ),
   _sendBufferSize ( 0 ),
   _pReceiveBuffer ( NULL ),
   _receiveBufferSize ( 0 )
   {
   }

   _sdbImpl::~_sdbImpl ()
   {
      std::set<ossValuePtr>::iterator it ;
      for ( it = _cursors.begin(); it != _cursors.end(); ++it )
      {
         ((_sdbCursorImpl*)(*it))->_dropConnection () ;
      }
      for ( it = _collections.begin(); it != _collections.end(); ++it )
      {
         ((_sdbCollectionImpl*)(*it))->_dropConnection () ;
      }
      for ( it = _collectionspaces.begin(); it != _collectionspaces.end(); ++it)
      {
         ((_sdbCollectionSpaceImpl*)(*it))->_dropConnection () ;
      }
      for ( it = _nodes.begin(); it != _nodes.end(); ++it )
      {
         ((_sdbNodeImpl*)(*it))->_dropConnection () ;
      }
      for ( it = _replicaGroups.begin(); it != _replicaGroups.end(); ++it )
      {
         ((_sdbReplicaGroupImpl*)(*it))->_dropConnection () ;
      }
      for ( it = _domains.begin(); it != _domains.end(); ++it )
      {
         ((_sdbDomainImpl*)(*it))->_dropConnection () ;
      }
      for ( it = _lobs.begin(); it != _lobs.end(); ++it )
      {
         ((_sdbLobImpl*)(*it))->_dropConnection () ;
      }
      if ( _sock )
         _disconnect () ;
      if ( _pSendBuffer )
         SDB_OSS_FREE ( _pSendBuffer ) ;
      if ( _pReceiveBuffer )
         SDB_OSS_FREE ( _pReceiveBuffer ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__DISCONNECT, "_sdbImpl::_disconnect" )
   void _sdbImpl::_disconnect ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__DISCONNECT ) ;
      INT32 rc = SDB_OK ;
      CHAR buffer [ sizeof ( MsgOpDisconnect ) ] ;
      CHAR *pBuffer = &buffer[0] ;
      INT32 bufferSize = sizeof ( buffer ) ;
      if ( _sock )
      {
         rc = clientBuildDisconnectMsg ( &pBuffer, &bufferSize, 0,
                                         _endianConvert ) ;
         if ( !rc )
         {
            clientSocketSend ( _sock, pBuffer, bufferSize ) ;
         }
         if ( pBuffer != &buffer[0] )
         {
            SDB_OSS_FREE ( pBuffer ) ;
         }
         _sock->close () ;
         delete _sock ;
         _sock = NULL ;
      }
      PD_TRACE_EXIT ( SDB_CLIENT__DISCONNECT );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__CONNECT, "_sdbImpl::_connect" )
   INT32 _sdbImpl::_connect ( const CHAR *pHostName,
                             UINT16 port )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__CONNECT ) ;
      INT32 rc = SDB_OK ;
      if ( _sock )
      {
         _disconnect () ;
      }
      _sock = new(std::nothrow) ossSocket ( pHostName, port ) ;
      if ( !_sock )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      rc = _sock->initSocket () ;
      if ( rc )
      {
         goto error ;
      }
      rc = _sock->connect ( SDB_CLIENT_SOCKET_TIMEOUT_DFT ) ;
      if ( rc )
      {
         goto error ;
      }
      _sock->disableNagle () ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT__CONNECT, rc );
      return rc ;
   error :
      if ( _sock )
         delete _sock ;
      _sock = NULL ;
      goto done ;
   }

   INT32 _sdbImpl::connect( const CHAR *pHostName,
                            UINT16 port )
   {
      return connect( pHostName, port, "", "" ) ;
   }

   INT32 _sdbImpl::_requestSysInfo ()
   {
      INT32 rc = SDB_OK ;
      MsgSysInfoRequest request ;
      MsgSysInfoRequest *prq = &request ;
      INT32 requestSize = sizeof(request) ;
      MsgSysInfoReply reply ;
      MsgSysInfoReply *pReply = &reply ;
      rc = clientBuildSysInfoRequest ( (CHAR**)&prq, &requestSize ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = clientSocketSend ( _sock, (CHAR *)prq, requestSize ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = clientSocketRecv ( _sock, (CHAR *)pReply, sizeof(MsgSysInfoReply) ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = clientExtractSysInfoReply ( (CHAR*)pReply, &_endianConvert, NULL ) ;
      if ( rc )
      {
         goto error ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CONNECTWITHPORT, "_sdbImpl::connect" )
   INT32 _sdbImpl::connect ( const CHAR *pHostName,
                             UINT16 port,
                             const CHAR *pUsrName,
                             const CHAR *pPasswd )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CONNECTWITHPORT ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      CHAR md5[SDB_MD5_DIGEST_LENGTH*2+1] ;
      BOOLEAN r ;
      SINT64 contextID = 0 ;

      rc = _connect( pHostName, port ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _requestSysInfo() ;
      if ( rc )
      {
         goto error ;
      }

      rc = md5Encrypt( pPasswd, md5, SDB_MD5_DIGEST_LENGTH*2+1) ;
      if ( rc )
      {
         goto error ;
      }

      rc = clientBuildAuthMsg( &_pSendBuffer, &_sendBufferSize,
                               pUsrName, md5, 0, _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }

      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_CONNECTWITHPORT, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CONNECTWITHSERVALADDR, "_sdbImpl::connect" )
   INT32 _sdbImpl::connect ( const CHAR **pConnAddrs,
                             INT32 arrSize,
                             const CHAR *pUsrName,
                             const CHAR *pPasswd )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CONNECTWITHSERVALADDR ) ;
      INT32 rc = SDB_OK ;
      const CHAR *pHostName = NULL ;
      const CHAR *pServiceName = NULL ;
      const CHAR *addr = NULL ;
      CHAR *pStr = NULL ;
      CHAR *pTmp = NULL ;
      INT32 mark = 0 ;
      INT32 i = 0 ;
      INT32 tmp = 0 ;
      if ( !pConnAddrs || arrSize <= 0 || !pUsrName || !pPasswd )
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
         tmp = (++i) % arrSize ;
         i = tmp ;
         pStr = ossStrdup ( addr ) ;
         if ( pStr == NULL )
         {
            rc = SDB_OOM ;
            goto error ;
         }
         pTmp = ossStrchr ( pStr, ':' ) ;
         if ( pTmp == NULL )
         {
            SDB_OSS_FREE ( pStr ) ;
            continue ;
         }
         *pTmp = 0 ;
         pHostName = pStr ;
         pServiceName = pTmp + 1 ;
         rc = connect ( pHostName, pServiceName, pUsrName, pPasswd ) ;
         SDB_OSS_FREE ( pStr ) ;
         pStr = NULL ;
         pTmp = NULL ;
         if ( rc == SDB_OK)
            goto done ;
      } while ( mark != i ) ;
      rc = SDB_NET_CANNOT_CONNECT ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CONNECTWITHSERVALADDR, rc );
      return rc ;
   error :
      goto done ;

   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATEUSR, "_sdbImpl::createUsr" )
   INT32 _sdbImpl::createUsr( const CHAR *pUsrName,
                              const CHAR *pPasswd )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATEUSR ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      CHAR md5[SDB_MD5_DIGEST_LENGTH*2+1] ;
      BOOLEAN r ;
      SINT64 contextID = 0 ;

      if ( NULL == pUsrName ||
           NULL == pPasswd )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = md5Encrypt( pPasswd, md5, SDB_MD5_DIGEST_LENGTH*2+1) ;
      if ( rc )
      {
         goto error ;
      }

      rc = clientBuildAuthCrtMsg( &_pSendBuffer, &_sendBufferSize,
                               pUsrName, md5, 0,
                               _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }

      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATEUSR, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_REMOVEUSR, "_sdbImpl::removeUsr" )
   INT32 _sdbImpl::removeUsr( const CHAR *pUsrName,
                              const CHAR *pPasswd )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_REMOVEUSR ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      CHAR md5[SDB_MD5_DIGEST_LENGTH*2+1] ;
      BOOLEAN r ;
      SINT64 contextID = 0 ;

      if ( NULL == pUsrName ||
           NULL == pPasswd )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = md5Encrypt( pPasswd, md5, SDB_MD5_DIGEST_LENGTH*2+1) ;
      if ( rc )
      {
         goto error ;
      }

      rc = clientBuildAuthDelMsg( &_pSendBuffer, &_sendBufferSize,
                                  pUsrName, md5, 0,
                                  _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }

      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_REMOVEUSR, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETSNAPSHOT, "_sdbImpl::getSnapshot" )
   INT32 _sdbImpl::getSnapshot ( _sdbCursor **result,
                                 INT32 snapType,
                                 const BSONObj &condition,
                                 const BSONObj &selector,
                                 const BSONObj &orderBy
                               )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETSNAPSHOT ) ;
      INT32 rc                        = SDB_OK ;
      const CHAR *p                   = NULL ;
      SINT64 contextID                = 0 ;
      BOOLEAN r                       = FALSE ;
      if ( !result )
      {
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      switch ( snapType )
      {
      case SDB_SNAP_CONTEXTS :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CONTEXTS ;
         break ;
      case SDB_SNAP_CONTEXTS_CURRENT :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CONTEXTS_CURRENT ;
         break ;
      case SDB_SNAP_SESSIONS :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_SESSIONS ;
         break ;
      case SDB_SNAP_SESSIONS_CURRENT :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_SESSIONS_CURRENT ;
         break ;
      case SDB_SNAP_COLLECTIONS :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_COLLECTIONS ;
         break ;
      case SDB_SNAP_COLLECTIONSPACES :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_COLLECTIONSPACES ;
         break ;
      case SDB_SNAP_DATABASE :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_DATABASE ;
         break ;
      case SDB_SNAP_SYSTEM :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_SYSTEM ;
         break ;
      case SDB_SNAP_CATALOG :
         p = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CATA ;
         break;
      default :
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      lock () ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                    p, 0, 0, 0, -1,
                                    condition.objdata(),
                                    selector.objdata(),
                                    orderBy.objdata(),
                                    NULL, _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
      if ( *result )
      {
         delete *result ;
         *result = NULL ;
      }
      *result = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl () ) ;
      if ( !*result )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*result)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)*result)->_contextID = contextID ;
      ((_sdbCursorImpl*)*result)->_setConnection ( this ) ;
   exit :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETSNAPSHOT, rc );
      return rc ;
   done :
      unlock () ;
      goto exit ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_RESETSNAPSHOT, "_sdbImpl::resetSnapshot" )
   INT32 _sdbImpl::resetSnapshot ( const BSONObj &condition )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_RESETSNAPSHOT ) ;
      INT32 rc                = SDB_OK ;
      BOOLEAN r               = FALSE ;
      const CHAR *p           = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_RESET ;
      if ( !_sock )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error ;
      }
      lock () ;
      rc = _runCommand ( p, r, &condition,
                         NULL, NULL, NULL ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_RESETSNAPSHOT, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETLIST, "_sdbImpl::getList" )
   INT32 _sdbImpl::getList ( _sdbCursor **result,
                             INT32 listType,
                             const BSONObj &condition,
                             const BSONObj &selector,
                             const BSONObj &orderBy
                           )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETLIST ) ;
      INT32 rc                        = SDB_OK ;
      const CHAR *p                   = NULL ;
      SINT64 contextID                = 0 ;
      BOOLEAN r                       = FALSE ;
      if ( !result )
      {
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      switch ( listType )
      {
      case SDB_LIST_CONTEXTS :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_CONTEXTS ;
         break ;
      case SDB_LIST_CONTEXTS_CURRENT :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_CONTEXTS_CURRENT ;
         break ;
      case SDB_LIST_SESSIONS :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_SESSIONS ;
         break ;
      case SDB_LIST_SESSIONS_CURRENT :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_SESSIONS_CURRENT ;
         break ;
      case SDB_LIST_COLLECTIONS :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_COLLECTIONS ;
         break ;
      case SDB_LIST_COLLECTIONSPACES :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_COLLECTIONSPACES ;
         break ;
      case SDB_LIST_STORAGEUNITS :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_STORAGEUNITS ;
         break ;
      case SDB_LIST_GROUPS :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_GROUPS ;
         break ;
      case SDB_LIST_STOREPROCEDURES :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_PROCEDURES ;
         break ;
      case SDB_LIST_DOMAINS :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_DOMAINS ;
         break ;
      case SDB_LIST_TASKS :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_TASKS ;
         break ;
      case SDB_LIST_CS_IN_DOMAIN :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_CS_IN_DOMAIN ;
         break ;
      case SDB_LIST_CL_IN_DOMAIN :
         p = CMD_ADMIN_PREFIX CMD_NAME_LIST_CL_IN_DOMAIN ;
         break ;
      default :
         rc = SDB_INVALIDARG ;
         goto exit ;
      }
      lock () ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer,
                                    &_sendBufferSize,
                                    p, 0, 0, 0, -1,
                                    condition.objdata(),
                                    selector.objdata(),
                                    orderBy.objdata(),
                                    NULL, _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer,
                          &_receiveBufferSize,
                          contextID,
                          r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
      if ( *result )
      {
         delete *result ;
         *result = NULL ;
      }
      *result = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl () ) ;
      if ( !*result )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*result)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)*result)->_contextID = contextID ;
      ((_sdbCursorImpl*)*result)->_setConnection ( this ) ;
   exit:
      PD_TRACE_EXITRC ( SDB_CLIENT_GETLIST, rc );
   return rc ;
   done :
      unlock () ;
      goto exit ;
   error :
      goto done ;
   }

   INT32 _sdbImpl::connect ( const CHAR *pHostName,
                             const CHAR *pServiceName
                           )
   {
      return connect( pHostName, pServiceName, "", "" ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CONNECTWITHSERVERNAME, "_sdbImpl::connect" )
   INT32 _sdbImpl::connect( const CHAR *pHostName,
                            const CHAR *pServiceName,
                            const CHAR *pUsrName,
                            const CHAR *pPasswd )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CONNECTWITHSERVERNAME ) ;
      INT32 rc = SDB_OK ;
      UINT16 port ;
      rc = ossSocket::getPort ( pServiceName, port ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = connect( pHostName, port, pUsrName, pPasswd ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CONNECTWITHSERVERNAME, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DISCONNECT, "_sdbImpl::disconnect" )
   void _sdbImpl::disconnect ()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DISCONNECT ) ;
      if ( _sock )
      {
         _disconnect () ;
      }
      PD_TRACE_EXIT ( SDB_CLIENT_DISCONNECT );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__REALLOCBUFFER, "_sdbImpl::_reallocBuffer" )
   INT32 _sdbImpl::_reallocBuffer ( CHAR **ppBuffer, INT32 *buffersize,
                                    INT32 newSize )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__REALLOCBUFFER ) ;
      INT32 rc = SDB_OK ;
      CHAR *pOriginalBuffer = NULL ;
      if ( !ppBuffer || !buffersize )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      pOriginalBuffer = *ppBuffer ;
      if ( *buffersize < newSize )
      {
         *ppBuffer = (CHAR*)SDB_OSS_REALLOC ( *ppBuffer, sizeof(CHAR)*newSize);
         if ( !*ppBuffer )
         {
            *ppBuffer = pOriginalBuffer ;
            rc = SDB_OOM ;
            goto error ;
         }
         *buffersize = newSize ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT__REALLOCBUFFER, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__SEND, "_sdbImpl::_send" )
   INT32 _sdbImpl::_send ( CHAR *pBuffer )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__SEND ) ;
      INT32 rc = SDB_OK ;
      INT32 len = 0 ;
      if ( !isConnected() )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error ;
      }
      ossEndianConvertIf4 ( *(SINT32*)pBuffer, len, _endianConvert ) ;
      rc = clientSocketSend ( _sock, pBuffer, len ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT__SEND, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__RECV, "_sdbImpl::_recv" )
   INT32 _sdbImpl::_recv ( CHAR **ppBuffer, INT32 *size )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__RECV ) ;
      INT32 rc = SDB_OK ;
      INT32 length = 0 ;
      INT32 realLen = 0 ;
      if ( !isConnected () )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error ;
      }
      rc = clientSocketRecv ( _sock,
                              (CHAR*)&length,
                              sizeof(length) ) ;
      if ( rc )
      {
         goto error ;
      }
      _sock->quickAck() ;

      ossEndianConvertIf4 (length, realLen, _endianConvert ) ;
      rc = _reallocBuffer ( ppBuffer, size, realLen+1 ) ;
      if ( rc )
      {
         goto error ;
      }

      *(SINT32*)(*ppBuffer) = length ;
      rc = clientSocketRecv ( _sock,
                              &(*ppBuffer)[sizeof(realLen)],
                              realLen - sizeof(realLen) ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT__RECV, rc );
      return rc ;
   error :
      if ( SDB_NETWORK_CLOSE == rc )
      {
         delete (_sock) ;
         _sock = NULL ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__RECVEXTRACT, "_sdbImpl::_recvExtract" )
   INT32 _sdbImpl::_recvExtract ( CHAR **ppBuffer, INT32 *size,
                                  SINT64 &contextID, BOOLEAN &result )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__RECVEXTRACT ) ;
      INT32 rc          = SDB_OK ;
      INT32 replyFlag   = -1 ;
      INT32 numReturned = -1 ;
      INT32 startFrom   = -1 ;
      rc = _recv ( ppBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = clientExtractReply ( *ppBuffer, &replyFlag, &contextID,
                                &startFrom, &numReturned, _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( replyFlag != SDB_OK )
      {
         result = FALSE ;
         rc = replyFlag ;
         goto done ;
      }

      result = TRUE ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT__RECVEXTRACT, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT__RUNCOMMAND, "_sdbImpl::_runCommand" )
   INT32 _sdbImpl::_runCommand ( const CHAR *pString, BOOLEAN &result,
                                 const BSONObj *arg1,
                                 const BSONObj *arg2,
                                 const BSONObj *arg3,
                                 const BSONObj *arg4 )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT__RUNCOMMAND ) ;
      INT32 rc            = SDB_OK ;
      SINT64 contextID    = 0 ;
      lock () ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize, pString,
                                    0, 0, -1, -1,
                                    arg1?arg1->objdata():NULL,
                                    arg2?arg2->objdata():NULL,
                                    arg3?arg3->objdata():NULL,
                                    arg4?arg4->objdata():NULL,
                                    _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT__RUNCOMMAND, rc );
      return rc ;
   error :
      goto done ;

   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETCOLLECTIONINSDB, "_sdbImpl::getCollection" )
   INT32 _sdbImpl::getCollection ( const CHAR *pCollectionFullName,
                                   _sdbCollection **collection )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETCOLLECTIONINSDB ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      INT32 nameLength    = ossStrlen ( pCollectionFullName ) ;
      BSONObj newObj ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_TEST_COLLECTION ) ;
      if ( !_sock )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error ;
      }
      if ( !pCollectionFullName || nameLength > CLIENT_CS_NAMESZ +
           CLIENT_COLLECTION_NAMESZ + 1 || !collection )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      newObj = BSON ( FIELD_NAME_NAME << pCollectionFullName ) ;
      rc = _runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      *collection = (_sdbCollection*)( new(std::nothrow) sdbCollectionImpl ()) ;
      if ( !*collection )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((sdbCollectionImpl*)*collection)->_setConnection ( this ) ;
      ((sdbCollectionImpl*)*collection)->_setName ( pCollectionFullName ) ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETCOLLECTIONINSDB, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETCOLLECTIONSPACE, "_sdbImpl::getCollectionSpace" )
   INT32 _sdbImpl::getCollectionSpace ( const CHAR *pCollectionSpaceName,
                                        _sdbCollectionSpace **cs )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETCOLLECTIONSPACE ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      INT32 nameLength = ossStrlen ( pCollectionSpaceName ) ;
      BSONObj newObj ;
      string command = string ( CMD_ADMIN_PREFIX
                                CMD_NAME_TEST_COLLECTIONSPACE ) ;
      if ( !_sock )
      {
         rc = SDB_NOT_CONNECTED ;
         goto error ;
      }
      if ( nameLength > CLIENT_CS_NAMESZ || !cs )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( FIELD_NAME_NAME << pCollectionSpaceName ) ;
      rc = _runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      *cs = (_sdbCollectionSpace*)( new(std::nothrow) sdbCollectionSpaceImpl());
      if ( !*cs )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((sdbCollectionSpaceImpl*)*cs)->_setConnection ( this ) ;
      ((sdbCollectionSpaceImpl*)*cs)->_setName ( pCollectionSpaceName ) ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETCOLLECTIONSPACE, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATECOLLECTIONSPACE, "_sdbImpl::createCollectionSpace" )
   INT32 _sdbImpl::createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                           INT32 iPageSize,
                                           _sdbCollectionSpace **cs )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATECOLLECTIONSPACE ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      INT32 nameLength = ossStrlen ( pCollectionSpaceName ) ;
      BSONObj newObj ;
      string command = string ( CMD_ADMIN_PREFIX
                                CMD_NAME_CREATE_COLLECTIONSPACE ) ;
      if ( nameLength > CLIENT_CS_NAMESZ || !cs )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( FIELD_NAME_NAME << pCollectionSpaceName <<
                      FIELD_NAME_PAGE_SIZE << iPageSize ) ;
      rc = _runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      *cs = (_sdbCollectionSpace*)( new(std::nothrow) sdbCollectionSpaceImpl());
      if ( !*cs )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((sdbCollectionSpaceImpl*)*cs)->_setConnection ( this ) ;
      ((sdbCollectionSpaceImpl*)*cs)->_setName ( pCollectionSpaceName ) ;
   done :
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATECOLLECTIONSPACE ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATECOLLECTIONSPACE2, "_sdbImpl::createCollectionSpace" )
   INT32 _sdbImpl::createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                           const bson::BSONObj &options,
                                           _sdbCollectionSpace **cs )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATECOLLECTIONSPACE2 ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      INT32 nameLength = ossStrlen ( pCollectionSpaceName ) ;
      BSONObjBuilder bob ;
      BSONObj newObj ;
      string command = string ( CMD_ADMIN_PREFIX
                                CMD_NAME_CREATE_COLLECTIONSPACE ) ;
      if ( nameLength > CLIENT_CS_NAMESZ || !cs )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }


      try
      {
         bob.append ( FIELD_NAME_NAME, pCollectionSpaceName ) ;
         BSONObjIterator it( options ) ;
         while ( it.more() )
         {
            bob.append ( it.next() ) ;
         }
         newObj = bob.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
/*
      newObj = BSON ( FIELD_NAME_NAME << pCollectionSpaceName <<
                      FIELD_NAME_PAGE_SIZE << iPageSize ) ;
*/
      rc = _runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      *cs = (_sdbCollectionSpace*)( new(std::nothrow) sdbCollectionSpaceImpl());
      if ( !*cs )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((sdbCollectionSpaceImpl*)*cs)->_setConnection ( this ) ;
      ((sdbCollectionSpaceImpl*)*cs)->_setName ( pCollectionSpaceName ) ;
   done :
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATECOLLECTIONSPACE2 ) ;
      return rc ;
   error :
      goto done ;
   }


   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DROPCOLLECTIONSPACE, "_sdbImpl::dropCollectionSpace" )
   INT32 _sdbImpl::dropCollectionSpace ( const CHAR *pCollectionSpaceName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DROPCOLLECTIONSPACE ) ;
      INT32 rc            = SDB_OK ;
      BOOLEAN result      = FALSE ;
      INT32 nameLength = ossStrlen ( pCollectionSpaceName ) ;
      BSONObj newObj ;
      string command = string ( CMD_ADMIN_PREFIX
                                CMD_NAME_DROP_COLLECTIONSPACE ) ;
      if ( nameLength > CLIENT_CS_NAMESZ )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      newObj = BSON ( FIELD_NAME_NAME << pCollectionSpaceName ) ;
      rc = _runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_DROPCOLLECTIONSPACE, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _sdbImpl::listCollectionSpaces ( _sdbCursor **result )
   {
      return getList ( result, SDB_LIST_COLLECTIONSPACES ) ;
   }

   INT32 _sdbImpl::listCollections ( _sdbCursor **result )
   {
      return getList ( result, SDB_LIST_COLLECTIONS ) ;
   }

   INT32 _sdbImpl::listReplicaGroups ( _sdbCursor **result )
   {
      return getList ( result, SDB_LIST_GROUPS ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETRGWITHNAME, "_sdbImpl::getReplicaGroup" )
   INT32 _sdbImpl::getReplicaGroup ( const CHAR *pName, _sdbReplicaGroup **result )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETRGWITHNAME ) ;
      INT32 rc = SDB_OK ;
      sdbCursor resultCursor ;
      BOOLEAN found = FALSE ;
      BSONObj record ;
      BSONObj condition ;
      BSONElement ele ;
      if ( !pName || !result )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      condition = BSON ( CAT_GROUPNAME_NAME << pName ) ;
      rc = getList ( &resultCursor.pCursor, SDB_LIST_GROUPS, condition ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( SDB_OK == ( rc = resultCursor.next ( record ) ) )
      {
         _sdbReplicaGroupImpl *replset =
                     new(std::nothrow) _sdbReplicaGroupImpl () ;
         if ( !replset )
         {
            rc = SDB_OOM ;
            goto error ;
         }
         ele = record.getField ( CAT_GROUPID_NAME ) ;
         if ( ele.type() == NumberInt )
         {
            replset->_replicaGroupID = ele.numberInt() ;
         }
         replset->_connection = this ;
         _regReplicaGroup ( replset ) ;
         ossStrncpy ( replset->_replicaGroupName, pName, CLIENT_REPLICAGROUP_NAMESZ ) ;
         if ( ossStrcmp ( pName, CAT_CATALOG_GROUPNAME ) == 0 )
         {
            replset->_isCatalog = TRUE ;
         }
         found = TRUE ;
         *result = replset ;
      }
      else if ( SDB_DMS_EOC != rc )
      {
         goto error ;
      }
      if ( !found )
      {
         rc = SDB_CLS_GRP_NOT_EXIST ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETRGWITHNAME, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETRGWITHID, "_sdbImpl::getReplicaGroup" )
   INT32 _sdbImpl::getReplicaGroup ( SINT32 id, _sdbReplicaGroup **result )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETRGWITHID ) ;
      INT32 rc = SDB_OK ;
      sdbCursor resultCursor ;
      BOOLEAN found = FALSE ;
      BSONObj record ;
      BSONObj condition ;
      BSONElement ele ;
      if ( !result )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      condition = BSON ( CAT_GROUPID_NAME << id ) ;
      rc = getList ( &resultCursor.pCursor, SDB_LIST_GROUPS, condition ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( SDB_OK == ( rc = resultCursor.next ( record ) ) )
      {
         ele = record.getField ( CAT_GROUPNAME_NAME ) ;
         if ( ele.type() == String )
         {
            const CHAR *pReplicaGroupName = ele.valuestr() ;
            _sdbReplicaGroupImpl *replset =
                  new(std::nothrow) _sdbReplicaGroupImpl () ;
            if ( !replset )
            {
               rc = SDB_OOM ;
               goto error ;
            }
            replset->_connection = this ;
            _regReplicaGroup ( replset ) ;
            ossStrncpy ( replset->_replicaGroupName, pReplicaGroupName,
                         CLIENT_REPLICAGROUP_NAMESZ ) ;
            replset->_replicaGroupID = id ;
            if ( ossStrcmp ( pReplicaGroupName, CAT_CATALOG_GROUPNAME ) == 0 )
            {
               replset->_isCatalog = TRUE ;
            }
            *result = replset ;
            found = TRUE ;
         } // if ( ele.type() == String )
      } // while ( SDB_OK == result.next ( record ) )
      else if ( SDB_DMS_EOC != rc )
      {
         goto error ;
      }
      if ( !found )
      {
         rc = SDB_CLS_GRP_NOT_EXIST ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETRGWITHID, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATERG, "_sdbImpl::createReplicaGroup" )
   INT32 _sdbImpl::createReplicaGroup ( const CHAR *pName,
                                        _sdbReplicaGroup **rg )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATERG ) ;
      INT32 rc = SDB_OK ;
      BSONObj replicaGroupName ;
      BOOLEAN result = FALSE ;
      _sdbReplicaGroupImpl *replset = NULL ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_CREATE_GROUP ) ;

      if ( ossStrlen ( pName ) > CLIENT_REPLICAGROUP_NAMESZ || !rg )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      replicaGroupName = BSON ( CAT_GROUPNAME_NAME << pName ) ;
      rc = _runCommand ( command.c_str(), result, &replicaGroupName ) ;
      if ( rc )
      {
         goto error ;
      }
      replset = new(std::nothrow) _sdbReplicaGroupImpl () ;
      if ( !replset )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      replset->_connection = this ;
      _regReplicaGroup ( replset ) ;
      ossStrncpy ( replset->_replicaGroupName, pName,
                   CLIENT_REPLICAGROUP_NAMESZ ) ;
      if ( ossStrcmp ( pName, CAT_CATALOG_GROUPNAME ) == 0 )
      {
         replset->_isCatalog = TRUE ;
      }
      *rg = replset ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATERG, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_REMOVERG, "_sdbImpl::removeReplicaGroup" )
   INT32 _sdbImpl::removeReplicaGroup ( const CHAR *pReplicaGroupName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_REMOVERG ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN result = FALSE ;
      INT32 nameLength = 0 ;
      BSONObjBuilder ob ;
      BSONObj newObj ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_REMOVE_GROUP ;
      const CHAR *pName = FIELD_NAME_GROUPNAME ;
      if ( !pReplicaGroupName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      nameLength = ossStrlen( pReplicaGroupName ) ;
      if ( 0 == nameLength || CLIENT_REPLICAGROUP_NAMESZ < nameLength )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ob.append ( pName, pReplicaGroupName ) ;
      newObj = ob.obj() ;
      rc = _runCommand ( pCommand, result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CLIENT_REMOVERG, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATECATARG, "_sdbImpl::createReplicaCataGroup" )
   INT32 _sdbImpl::createReplicaCataGroup (  const CHAR *pHostName,
                                        const CHAR *pServiceName,
                                        const CHAR *pDatabasePath,
                                        const BSONObj &configure )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATECATARG ) ;
      INT32 rc = SDB_OK ;
      BSONObj configuration ;
      BSONObjBuilder ob ;
      BOOLEAN result = FALSE ;
      const CHAR *pCreateCataRG = CMD_ADMIN_PREFIX CMD_NAME_CREATE_CATA_GROUP ;

      if ( !pHostName || !pServiceName || !pDatabasePath )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      ob.append ( CAT_HOST_FIELD_NAME, pHostName ) ;

      ob.append ( PMD_OPTION_SVCNAME, pServiceName ) ;

      ob.append ( PMD_OPTION_DBPATH, pDatabasePath ) ;

      {
         BSONObjIterator it ( configure ) ;
         while ( it.more() )
         {
            BSONElement ele = it.next () ;
            const CHAR *key = ele.fieldName() ;
            if ( ossStrcmp ( key, PMD_OPTION_DBPATH ) == 0 ||
                 ossStrcmp ( key, PMD_OPTION_SVCNAME ) == 0  ||
                 ossStrcmp ( key, CAT_HOST_FIELD_NAME ) == 0 )
            {
               continue ;
            }
            ob.append ( ele ) ;
         } // while
      } // if ( configure )
      configuration = ob.obj () ;

      rc = _runCommand ( pCreateCataRG, result, &configuration ) ;
      if ( rc )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATECATARG, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_ACTIVATERG, "_sdbImpl::activateReplicaGroup" )
   INT32 _sdbImpl::activateReplicaGroup ( const CHAR *pName,
                                   _sdbReplicaGroup **rg )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_ACTIVATERG ) ;
      INT32 rc = SDB_OK ;
      BSONObj replicaGroupName ;
      BOOLEAN result = FALSE ;
      _sdbReplicaGroupImpl *replset = NULL ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_ACTIVE_GROUP ) ;

      if ( ossStrlen ( pName ) > CLIENT_REPLICAGROUP_NAMESZ || !rg )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      replicaGroupName = BSON ( CAT_GROUPNAME_NAME << pName ) ;
      rc = _runCommand ( command.c_str(), result, &replicaGroupName ) ;
      if ( rc )
      {
         goto error ;
      }
      replset = new(std::nothrow) _sdbReplicaGroupImpl () ;
      if ( !replset )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      replset->_connection = this ;
      _regReplicaGroup ( replset ) ;
      ossStrncpy ( replset->_replicaGroupName, pName,
                   CLIENT_REPLICAGROUP_NAMESZ ) ;
      if ( ossStrcmp ( pName, CAT_CATALOG_GROUPNAME ) == 0 )
      {
         replset->_isCatalog = TRUE ;
      }
      *rg = replset ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_ACTIVATERG, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_EXECUPDATE, "_sdbImpl::execUpdate" )
   INT32 _sdbImpl::execUpdate( const CHAR *sql )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_EXECUPDATE ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;

      rc = clientValidateSql( sql, FALSE ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = clientBuildSqlMsg( &_pSendBuffer, &_sendBufferSize,
                              sql, 0, _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_EXECUPDATE, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_EXEC, "_sdbImpl::exec" )
   INT32 _sdbImpl::exec( const CHAR *sql,
                         _sdbCursor **result )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_EXEC ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN r ;
      SINT64 contextID = 0 ;

      rc = clientValidateSql( sql, TRUE ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = clientBuildSqlMsg( &_pSendBuffer, &_sendBufferSize,
                              sql, 0,
                              _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                           contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
      if ( *result )
      {
         delete *result ;
         *result = NULL ;
      }
      *result = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl () ) ;
      if ( !*result )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*result)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)*result)->_contextID = contextID ;
      ((_sdbCursorImpl*)*result)->_setConnection ( this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_EXEC, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_TRANSBEGIN, "_sdbImpl::transactionBegin" )
   INT32 _sdbImpl:: transactionBegin()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_TRANSBEGIN ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      rc = clientBuildTransactionBegMsg( &_pSendBuffer, &_sendBufferSize, 0,
                                         _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_TRANSBEGIN, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_TRANSCOMMIT, "_sdbImpl::transactionCommit" )
   INT32 _sdbImpl:: transactionCommit()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_TRANSCOMMIT ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      rc = clientBuildTransactionCommitMsg( &_pSendBuffer, &_sendBufferSize, 0,
                                            _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_TRANSCOMMIT, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_TRANSROLLBACK, "_sdbImpl::transactionRollback" )
   INT32 _sdbImpl:: transactionRollback()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_TRANSROLLBACK ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      rc = clientBuildTransactionRollbackMsg( &_pSendBuffer, &_sendBufferSize, 0,
                                              _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_TRANSROLLBACK, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_FLUSHCONGIGURE, "_sdbImpl::flushConfigure" )
   INT32 _sdbImpl::flushConfigure( const bson::BSONObj &options )
   {
      PD_TRACE_ENTRY( SDB_CLIENT_FLUSHCONGIGURE ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN r ;
      SINT64 contextID = 0 ;
      rc = clientBuildQueryMsgCpp( &_pSendBuffer, &_sendBufferSize,
                                (CMD_ADMIN_PREFIX CMD_NAME_EXPORT_CONFIG),
                                0, 0, 0, -1, options.objdata(), NULL, NULL, NULL,
                              _endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done:
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_FLUSHCONGIGURE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_CRTJSPROCEDURE, "_sdbImpl::crtJSProcedure" )
   INT32 _sdbImpl::crtJSProcedure ( const CHAR *code )
   {
      PD_TRACE_ENTRY( SDB_CLIENT_CRTJSPROCEDURE ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN r ;
      SINT64 contextID = 0 ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      if ( !code )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ob.appendCode ( FIELD_NAME_FUNC, code ) ;
      ob.append ( FMP_FUNC_TYPE, FMP_FUNC_TYPE_JS ) ;
      newObj = ob.obj() ;

      rc = clientBuildQueryMsgCpp( &_pSendBuffer, &_sendBufferSize,
                                   (CMD_ADMIN_PREFIX CMD_NAME_CRT_PROCEDURE),
                                   0, 0, 0, -1, newObj.objdata(), NULL, NULL, NULL,
                                   _endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                          contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done:
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_CRTJSPROCEDURE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_RMPROCEDURE, "_sdbImpl::rmProcedure" )
   INT32 _sdbImpl::rmProcedure( const CHAR *spName )
   {
      PD_TRACE_ENTRY( SDB_CLIENT_RMPROCEDURE ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN r ;
      SINT64 contextID = 0 ;
      BSONObj newObj ;
      BSONObjBuilder ob ;

      ob.append ( FIELD_NAME_FUNC, spName ) ;
      newObj = ob.obj() ;

      rc = clientBuildQueryMsgCpp( &_pSendBuffer, &_sendBufferSize,
                                (CMD_ADMIN_PREFIX CMD_NAME_RM_PROCEDURE),
                                0, 0, 0, -1, newObj.objdata(), NULL, NULL, NULL,
                                 _endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                           contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done:
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_RMPROCEDURE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_LISTPROCEDURES, "_sdbImpl::listProcedures" )
   INT32 _sdbImpl::listProcedures( _sdbCursor **cursor, const bson::BSONObj &condition )
   {
      return getList ( cursor, SDB_LIST_STOREPROCEDURES, condition ) ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_EVALJS, "_sdbImpl::evalJS" )
   INT32 _sdbImpl::evalJS( _sdbCursor **cursor,
                           const CHAR *code,
                           SDB_SPD_RES_TYPE *type,
                           const bson::BSONObj &errmsg )
   {
      PD_TRACE_ENTRY( SDB_CLIENT_EVALJS ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN r ;
      SINT64 contextID = 0 ;
      BSONObj newObj ;
      BSONObjBuilder ob ;

      ob.appendCode ( FIELD_NAME_FUNC, code ) ;
      ob.appendIntOrLL ( FIELD_NAME_FUNCTYPE, FMP_FUNC_TYPE_JS ) ;
      newObj = ob.obj() ;

      rc = clientBuildQueryMsgCpp( &_pSendBuffer, &_sendBufferSize,
                                (CMD_ADMIN_PREFIX CMD_NAME_EVAL),
                                0, 0, 0, -1, newObj.objdata(), NULL, NULL, NULL,
                                 _endianConvert ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                        contextID, r ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
      if ( *cursor )
      {
         delete *cursor ;
         *cursor = NULL ;
      }
      *cursor = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl () ) ;
      if ( !*cursor )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*cursor)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)*cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)*cursor)->_setConnection ( this ) ;
   done:
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_EVALJS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_BACKUPOFFLINE, "_sdbImpl::backupOffline" )
   INT32 _sdbImpl::backupOffline ( const bson::BSONObj &options)
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_BACKUPOFFLINE ) ;
      INT32 rc          = SDB_OK ;
      BOOLEAN result    = FALSE ;
      SINT64 contextID  = 0 ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_BACKUP_OFFLINE ) ;
      {
      BSONObjIterator it ( options ) ;
      while ( it.more() )
      {
         ob.append ( it.next() ) ;
      }
      }
      newObj = ob.obj() ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     command.c_str(), 0, 0, 0, -1,
                                     newObj.objdata(), NULL, NULL, NULL,
                                    _endianConvert ) ;
      if ( rc )
      {
         goto done ;
      }
      lock () ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_BACKUPOFFLINE, rc ) ;
      return rc ;
   error :
      unlock () ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_LISTBACKUP, "_sdbImpl::listBackup" )
   INT32 _sdbImpl::listBackup ( _sdbCursor **cursor,
                                 const bson::BSONObj &options,
                                 const bson::BSONObj &condition,
                                 const bson::BSONObj &selector,
                              const bson::BSONObj &orderBy)
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_LISTBACKUP ) ;
      INT32 rc          = SDB_OK ;
      BOOLEAN result    = FALSE ;
      BOOLEAN locked    = FALSE ;
      SINT64 contextID  = 0 ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_LIST_BACKUPS ) ;
      {
      BSONObjIterator it ( options ) ;
      while ( it.more() )
      {
         ob.append ( it.next() ) ;
      }
      }
      newObj = ob.obj() ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     command.c_str(), 0, 0, 0, -1,
                                     condition.objdata(), selector.objdata(),
                                     orderBy.objdata(), newObj.objdata(),
                                     _endianConvert ) ;
      if ( rc )
      {
         goto done ;
      }
      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
      if ( *cursor )
      {
         delete *cursor ;
         *cursor = NULL ;
      }
      *cursor = (_sdbCursor*)( new(std::nothrow) sdbCursorImpl () ) ;
      if ( !*cursor )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((_sdbCursorImpl*)*cursor)->_setCollection ( NULL ) ;
      ((_sdbCursorImpl*)*cursor)->_contextID = contextID ;
      ((_sdbCursorImpl*)*cursor)->_setConnection ( this ) ;
   done :
      if ( locked )
          unlock () ;
      PD_TRACE_EXITRC ( SDB_CLIENT_LISTBACKUP, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_REMOVEBACKUP, "_sdbImpl::removeBackup" )
   INT32 _sdbImpl::removeBackup ( const bson::BSONObj &options )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_REMOVEBACKUP ) ;
      INT32 rc          = SDB_OK ;
      BOOLEAN result    = FALSE ;
      SINT64 contextID  = 0 ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_REMOVE_BACKUP ) ;
      {
      BSONObjIterator it ( options ) ;
      while ( it.more() )
      {
         ob.append ( it.next() ) ;
      }
      }
      newObj = ob.obj() ;
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     command.c_str(), 0, 0, 0, -1,
                                     newObj.objdata(), NULL, NULL, NULL,
                                     _endianConvert ) ;
      if ( rc )
      {
          goto done ;
      }
      lock () ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_REMOVEBACKUP, rc ) ;
      return rc ;
   error :
      unlock () ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_LISTTASKS, "_sdbImpl::listTasks" )
   INT32 _sdbImpl::listTasks ( _sdbCursor **cursor,
                           const bson::BSONObj &condition,
                           const bson::BSONObj &selector,
                           const bson::BSONObj &orderBy,
                           const bson::BSONObj &hint)

   {
      return getList ( cursor, SDB_LIST_TASKS, condition, selector, orderBy ) ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_WAITTASKS, "_sdbImpl::waitTasks" )
   INT32 _sdbImpl::waitTasks ( const SINT64 *taskIDs,
                         SINT32 num )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_WAITTASKS ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObjBuilder bob ;
      BSONObjBuilder subBob ;
      BSONArrayBuilder bab ;
      BSONObj newObj ;
      BSONObj subObj ;
      INT32 i = 0 ;

      if ( !taskIDs || num < 0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      try
      {
         for ( i = 0 ; i < num; i++ )
         {
            bab.append(taskIDs[i]) ;
         }
         subBob.appendArray ( "$in", bab.arr() ) ;
         subObj = subBob.obj () ;
         bob.append ( FIELD_NAME_TASKID, subObj ) ;
         newObj = bob.obj () ;
      }
      catch (std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }

      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     CMD_ADMIN_PREFIX CMD_NAME_WAITTASK,
                                     0, 0, 0, -1,
                                     newObj.objdata(), NULL,
                                     NULL, NULL,
                                     _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_WAITTASKS, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_CANCELTASK, "_sdbImpl::cancelTask" )
   INT32 _sdbImpl::cancelTask ( SINT64 taskID,
                            BOOLEAN isAsync )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CANCELTASK ) ;
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN result ;
      SINT64 contextID = 0 ;
      BSONObjBuilder it ;
      BSONObj newObj ;

      if ( taskID <= 0 )
      {
         rc = SDB_INVALIDARG ;
       goto error ;
      }

      try
      {
         it.appendIntOrLL ( FIELD_NAME_TASKID, taskID ) ;
         it.appendBool( FIELD_NAME_ASYNC, isAsync ) ;
         newObj = it.obj () ;
      }
      catch (std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = clientBuildQueryMsgCpp ( &_pSendBuffer, &_sendBufferSize,
                                     CMD_ADMIN_PREFIX CMD_NAME_CANCEL_TASK,
                                     0, 0, 0, -1,
                                     newObj.objdata(), NULL,
                                     NULL, NULL,
                                     _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      lock () ;
      locked = TRUE ;
      rc = _send ( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                                       contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
      {
         unlock () ;
      }
      PD_TRACE_EXITRC ( SDB_CLIENT_CANCELTASK, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB_CLIENT_SETSESSIONATTR, "_sdbImpl::setSessionAttr" )
   INT32 _sdbImpl::setSessionAttr ( const bson::BSONObj &options )
   {
      PD_TRACE_ENTRY( SDB_CLIENT_SETSESSIONATTR ) ;
      INT32 rc         = SDB_OK ;
      BOOLEAN result   = FALSE ;
      BOOLEAN locked   = FALSE ;
      SINT64 contextID = 0 ;
      const CHAR *key  = NULL ;
      const CHAR *str_value = NULL ;
      INT32 int_value  = 0 ;
      INT32 value      = PREFER_REPL_TYPE_MAX ;
      BSONType type    = EOO ;
      BSONElement ele ;
      BSONObjBuilder bob ;
      BSONObj newObj ;
      string command =string( CMD_ADMIN_PREFIX CMD_NAME_SETSESS_ATTR ) ;
      BSONObjIterator it ( options ) ;

      if ( !it.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      while ( it.more() )
      {
         ele = it.next() ;
         key = ele.fieldName() ;
         if ( strcmp( FIELD_NAME_PREFERED_INSTANCE, key ) == 0 )
         {
            type = ele.type() ;
            switch ( type )
            {
               case String :
                  try
                  {
                     str_value = ele.String().c_str() ;
                     if ( strcmp( "M", str_value ) == 0 ||
                          strcmp( "m", str_value ) == 0 ) // master
                        value = PREFER_REPL_MASTER ;
                     else if ( strcmp( "S", str_value ) == 0 ||
                               strcmp( "s", str_value ) == 0 ) // slave
                        value = PREFER_REPL_SLAVE ;
                     else if ( strcmp( "A", str_value ) == 0 ||
                               strcmp( "a", str_value ) == 0 ) //anyone
                        value = PREFER_REPL_ANYONE ;
                     else
                     {
                        rc = SDB_INVALIDARG ;
                        goto error ;
                     }
                  }
                  catch( std::exception )
                  {
                     rc = SDB_SYS ;
                     goto error ;
                  }
                  break ;
               case NumberInt :
                  try
                  {
                     int_value = ele.Int() ;
                     if ( 1 <= int_value && int_value <= 7 )
                        value = int_value ;
                     else
                     {
                        rc = SDB_INVALIDARG ;
                        goto error ;
                     }
                  }
                  catch( std::exception )
                  {
                     rc = SDB_SYS ;
                     goto error ;
                  }
                  break ;
               default :
                  rc = SDB_INVALIDARG ;
                  goto error ;
            } // switch
            bob.append( key, value ) ;
            break ;
         } // if
         else
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      } // while()
      newObj = bob.obj() ;
      rc = clientBuildQueryMsgCpp( &_pSendBuffer, &_sendBufferSize,
                                   command.c_str(), 0, 0, 0, -1,
                                   newObj.objdata(), NULL,
                                   NULL, NULL, _endianConvert ) ;
      if ( rc )
      {
         goto error ;
      }
      lock() ;
      locked = TRUE ;
      rc = _send( _pSendBuffer ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _recvExtract ( &_pReceiveBuffer, &_receiveBufferSize,
                          contextID, result ) ;
      if ( rc )
      {
         goto error ;
      }
      CHECK_RET_MSGHEADER( _pSendBuffer, _pReceiveBuffer, this ) ;
   done :
      if ( locked )
         unlock() ;
      PD_TRACE_EXITRC( SDB_CLIENT_SETSESSIONATTR, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CLOSE_ALL_CURSORS, "_sdbImpl::closeAllCursors" )
   INT32 _sdbImpl::closeAllCursors()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CLOSE_ALL_CURSORS ) ;
      INT32 rc = SDB_OK ;

      for ( std::set<ossValuePtr>::iterator it = _cursors.begin();
            it != _cursors.end(); ++it )
      {
         rc = ((_sdbCursorImpl *)(*it))->close() ;
         if ( rc )
         {
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CLOSE_ALL_CURSORS, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_IS_VALID2, "_sdbImpl::isValid" )
   INT32 _sdbImpl::isValid( BOOLEAN *result )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_IS_VALID2 ) ;
      INT32 rc = SDB_OK ;
      if ( result == NULL )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      *result = isValid() ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_IS_VALID2, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_IS_VALID, "_sdbImpl::isValid" )
   BOOLEAN _sdbImpl::isValid()
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_IS_VALID ) ;
      BOOLEAN flag = FALSE ;
      if ( _sock == NULL )
      {
         flag = FALSE ;
      }
      else
      {
         flag =  _sock->isConnected() ;
      }
      PD_TRACE_EXIT ( SDB_CLIENT_IS_VALID );
      return flag ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_CREATEDOMAIN, "_sdbImpl::createDomain" )
   INT32 _sdbImpl::createDomain ( const CHAR *pDomainName,
                                  const bson::BSONObj &options,
                                  _sdbDomain **domain )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_CREATEDOMAIN ) ;
      INT32 rc       = SDB_OK ;
      BOOLEAN result = FALSE ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_CREATE_DOMAIN ) ;
      if ( !pDomainName || ossStrlen ( pDomainName ) >
                               CLIENT_COLLECTION_NAMESZ )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         ob.append ( FIELD_NAME_NAME, pDomainName ) ;
         ob.append ( FIELD_NAME_OPTIONS, options ) ;
         newObj = ob.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }

      rc = _runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( *domain )
      {
         delete *domain ;
         *domain = NULL ;
      }
      *domain = (_sdbDomain*)( new(std::nothrow) sdbDomainImpl () ) ;
      if ( !(*domain) )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ((sdbDomainImpl*)*domain)->_setConnection ( this ) ;
      ((sdbDomainImpl*)*domain)->_setName ( pDomainName ) ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_CREATEDOMAIN, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_DROPDOMAIN, "_sdbImpl::dropDomain" )
   INT32 _sdbImpl::dropDomain ( const CHAR *pDomainName )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_DROPDOMAIN ) ;
      INT32 rc       = SDB_OK ;
      BOOLEAN result = FALSE ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_DROP_DOMAIN ) ;
      if ( !pDomainName || ossStrlen ( pDomainName ) >
                               CLIENT_COLLECTION_NAMESZ )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         ob.append ( FIELD_NAME_NAME, pDomainName ) ;
         newObj = ob.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }

      rc = _runCommand ( command.c_str(), result, &newObj ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_DROPDOMAIN, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_GETDOMAIN, "_sdbImpl::getDomain" )
   INT32 _sdbImpl::getDomain ( const CHAR *pDomainName,
                               _sdbDomain **domain )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_GETDOMAIN ) ;
      INT32 rc       = SDB_OK ;
      BSONObj result ;
      BSONObj newObj ;
      BSONObjBuilder ob ;
      sdbCursor cursor ;
      if ( !pDomainName || ossStrlen ( pDomainName ) > CLIENT_COLLECTION_NAMESZ
            || !domain )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      try
      {
         ob.append ( FIELD_NAME_NAME, pDomainName ) ;
         newObj = ob.obj () ;
      }
      catch ( std::exception )
      {
         rc = SDB_DRIVER_BSON_ERROR ;
         goto error ;
      }
      rc = getList ( &cursor.pCursor, SDB_LIST_DOMAINS, newObj ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( SDB_OK == ( rc = cursor.next( result ) ) )
      {
         *domain = (_sdbDomain*)( new(std::nothrow) sdbDomainImpl() ) ;
         if ( !(*domain) )
         {
            rc = SDB_OOM ;
            goto error ;
         }
         ((sdbDomainImpl*)*domain)->_setConnection ( this ) ;
         ((sdbDomainImpl*)*domain)->_setName ( pDomainName ) ;
      }
      else if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_CAT_DOMAIN_NOT_EXIST ;
         goto done ;
      }
      else
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_GETDOMAIN, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_CLIENT_LISTDOMAINS, "_sdbImpl::listDomains" )
   INT32 _sdbImpl::listDomains ( _sdbCursor **cursor,
                       const bson::BSONObj &condition,
                       const bson::BSONObj &selector,
                       const bson::BSONObj &orderBy,
                       const bson::BSONObj &hint
                      )
   {
      PD_TRACE_ENTRY ( SDB_CLIENT_LISTDOMAINS ) ;
      INT32 rc = SDB_OK ;
      rc = getList ( cursor, SDB_LIST_DOMAINS,
                     condition, selector, orderBy ) ;
      if ( rc )
         goto error ;
   done :
      PD_TRACE_EXITRC ( SDB_CLIENT_LISTDOMAINS, rc );
      return rc ;
   error :
      goto done ;
   }


/*   INT32 _sdbImpl::modifyConfig ( INT32 nodeID,
                                  std::map<std::string,std::string> &config )
   {
      INT32 rc = SDB_OK ;
      bson nodeObj ;
      bson configObj ;
      BOOLEAN result = FALSE ;
      bson_init ( &nodeObj ) ;
      bson_init ( &configObj ) ;
      map<string,string>::iterator it ;
      string command = string ( CMD_ADMIN_PREFIX CMD_NAME_UPDATE_CONFIG ) ;

      rc = bson_append_int ( &nodeObj, CAT_NODEID_NAME, nodeID ) ;
      if ( rc )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      bson_finish ( &nodeObj ) ;

      for ( it = config.begin(); it != config.end(); ++it )
      {
         rc = bson_append_string ( &configObj,
                                   it->first.c_str(),
                                   it->second.c_str() ) ;
         if ( rc )
         {
            rc = SDB_SYS ;
            goto error ;
         }
      }
      bson_finish ( &configObj ) ;

      rc = _runCommand ( command.c_str(), result, &nodeObj, &configObj ) ;
      if ( rc )
      {
         goto error ;
      }
   done :
      bson_destroy ( &nodeObj ) ;
      bson_destroy ( &configObj ) ;
      return rc ;
   error :
      goto done ;
   }

   INT32 _sdbImpl::getConfig ( INT32 nodeID,
                               std::map<std::string,std::string> &config )
   {
      return SDB_OK ;
   }*/

   _sdb *_sdb::getObj ()
   {
      return (_sdb*)(new(std::nothrow) sdbImpl ()) ;
   }
}
