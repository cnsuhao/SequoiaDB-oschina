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

   Source File Name = rtnLobStream.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnLobStream.hpp"
#include "pmdEDU.hpp"
#include "msgDef.h"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "rtnContext.hpp"

using namespace bson ;

#define ALLOC_MEM( needLen, len, buf, rc ) \
        do\
        {\
           if ( needLen <= len )\
           {\
           }\
           else if ( NULL == buf )\
           {\
              buf = ( CHAR * )SDB_OSS_MALLOC( needLen ) ;\
              if ( NULL == buf )\
              {\
                 rc = SDB_OOM ;\
              }\
              len = needLen ;\
           }\
           else\
           {\
              SDB_OSS_FREE( buf ) ;\
              buf = NULL ;\
              len = 0 ;\
              buf = ( CHAR * )SDB_OSS_MALLOC( needLen ) ;\
              if ( NULL == buf )\
              {\
                 rc = SDB_OOM ;\
              }\
              len = needLen ;\
           }\
        } while ( FALSE )

namespace engine
{
   _rtnLobStream::_rtnLobStream()
   :_dpsCB( NULL ),
    _opened( FALSE ),
    _mode( 0 ),
    _lobPageSz( DMS_DO_NOT_CREATE_LOB ),
    _offset( 0 )
   {
      ossMemset( _fullName, 0, DMS_COLLECTION_SPACE_NAME_SZ +
                               DMS_COLLECTION_NAME_SZ + 2 ) ;
   }

   _rtnLobStream::~_rtnLobStream()
   {

   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM_OPEN, "_rtnLobStream::open" )
   INT32 _rtnLobStream::open( const CHAR *fullName,
                              const bson::OID &oid,
                              INT32 mode,
                              _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM_OPEN ) ;

      ossMemcpy( _fullName, fullName, ossStrlen( fullName ) ) ;
      ossMemcpy( &_oid, &oid, sizeof( oid ) ) ;
      _mode = mode ;

      rc = _prepare( fullName, oid, mode, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to open lob[%s]"
                 " in cl[%s], rc:%d",
                 oid.str().c_str(), fullName, rc ) ;
         goto error ;
      }

      if ( SDB_LOB_MODE_R == mode )
      {
         rc = _open4Read( cb ) ;
      }
      else if ( SDB_LOB_MODE_CREATEONLY == mode )
      {
         rc = _open4Create( cb ) ;
      }
      else if ( SDB_LOB_MODE_REMOVE == mode )
      {
         rc = _open4Remove( cb ) ;
      }
      else
      {
         PD_LOG( PDERROR, "unknown open mode:%d", mode ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob[%s], rc:%d",
                 oid.str().c_str(), rc ) ;
         goto error ;
      }

      rc = _getLobPageSize( _lobPageSz ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get page size of lob:%d", rc ) ;
         goto error ;
      }

      rc = _lw.init( _lobPageSz ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to init stream window:%d", rc ) ;
         goto error ;
      }

      _opened = TRUE ;
   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM_OPEN, rc ) ;
      return rc ;
   error:
      if ( _opened )
      {
         closeWithException( cb ) ;
      }
      goto done ;
   }

   INT32 _rtnLobStream::getMetaData( bson::BSONObj &meta )
   {
      INT32 rc = SDB_OK ;
      if ( !isOpened() )
      {
         rc = SDB_INVALIDARG ;
         goto done ;
      }

      if ( _metaObj.isEmpty() )
      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_LOB_SIZE, (long long)_meta._lobLen ) ;
         builder.append( FIELD_NAME_LOB_PAGE_SIZE, _lobPageSz ) ;
         builder.append( FIELD_NAME_LOB_CREATTIME, (long long)_meta._createTime ) ;
         _metaObj = builder.obj() ;
      }

      meta = _metaObj ;
   done:
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM_CLOSE, "_rtnLobStream::close" )
   INT32 _rtnLobStream::close( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM_CLOSE ) ;
      if ( !isOpened() )
      {
         goto done ;
      }

      if ( SDB_LOB_MODE_CREATEONLY & _mode )
      {
         ossTimestamp t ;
         _rtnLobTuple tuple ;
         if ( _lw.getCachedData( tuple ) )
         {
            rc = _write( tuple, cb ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to write lob[%s], rc:%d",
                       _oid.str().c_str(), rc ) ;
                goto error ;
            }
         }

         ossGetCurrentTime( t ) ;
         _meta._lobLen = _offset ;
         _meta._createTime = t.time * 1000 + t.microtm / 1000 ;
         _meta._status = DMS_LOB_COMPLETE ;

         rc = _completeLob( _meta, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to complete lob:%d", rc ) ;
            goto error ;
         }

         PD_LOG( PDDEBUG, "lob [%s] is closed, len:%lld",
                 getOID().str().c_str(), _offset ) ;
      }
      else if ( SDB_LOB_MODE_REMOVE & _mode )
      {
         _rtnLobTuple tuple( 0, DMS_LOB_META_SEQUENCE, 0, NULL ) ;
         RTN_LOB_TUPLES tuples ;
         tuples.push_back( tuple ) ;
         rc = _removev( tuples, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to remove meta data of lob:%d", rc ) ;
            goto error ;
         }
         PD_LOG( PDDEBUG, "lob [%s] is removed",
                 getOID().str().c_str() ) ;
      }

      rc = _close( cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to close lob:%d", rc ) ;
         goto error ;
      }
      _opened = FALSE ;
   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM_CLOSE, rc ) ;
      return rc ;
   error:
      closeWithException( cb ) ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM_CLOSEWITHEXCEPTION, "_rtnLobStream::closeWithException" )
   INT32 _rtnLobStream::closeWithException( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM_CLOSEWITHEXCEPTION ) ;
      if ( !isOpened() )
      {
         goto done ;
      }

      if ( SDB_LOB_MODE_CREATEONLY & _mode ) 
      {
         PD_LOG( PDERROR, "lob[%s] is closed with exception, rollback",
                 getOID().str().c_str() ) ;
         rc = _rollback( cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to rollback lob[%s], rc:%d",
                    _oid.str().c_str(), rc ) ;
            goto error ;
         }
      }

      _opened = FALSE ;
   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM_CLOSEWITHEXCEPTION, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM_WRITE, "_rtnLobStream::write" )
   INT32 _rtnLobStream::write ( UINT32 len,
                                const CHAR *buf,
                                _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM_WRITE ) ;
      RTN_LOB_TUPLES tuples ;

      if ( !isOpened() )
      {
         PD_LOG( PDERROR, "lob[%s] is not opened yet",
                 _oid.str().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( !SDB_LOB_MODE_CREATEONLY & _mode )
      {
         PD_LOG( PDERROR, "open mode[%d] does not support this operation",
                 _mode ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _lw.prepare2Write( _offset, len, buf ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to add piece to window:%d", rc ) ;
         goto error ;
      }

      _offset += len ;

      while ( _lw.getNextWriteSequences( tuples )  )
      {
         if ( cb->isInterrupted() )
         {
            rc = SDB_INTERRUPT ;
            goto error ;
         }

         rc = _writev( tuples, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to write lob[%s], rc:%d",
                    _oid.str().c_str(), rc ) ;
            goto error ;
         }

         tuples.clear() ;
      }

      _lw.cacheLastDataOrClearCache() ;

   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM_WRITE, rc ) ;
      return rc ;
   error:
      closeWithException( cb ) ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM_READ, "_rtnLobStream::read" )
   INT32 _rtnLobStream::read( UINT32 len,
                              _rtnContextBase *context,
                              _pmdEDUCB *cb,
                              UINT32 &read )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM_READ ) ;
      UINT32 readLen = 0 ;
      RTN_LOB_TUPLES tuples ;
      
      SDB_ASSERT( _meta.isDone(), "lob has not been completed yet" ) ;

      if ( !isOpened() )
      {
         PD_LOG( PDERROR, "lob[%s] is not opened yet",
                 _oid.str().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( !( SDB_LOB_MODE_R & _mode ) )
      {
         PD_LOG( PDERROR, "open mode[%d] does not support this operation",
                 _mode ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( 0 == len )
      {
         goto done ;
         read = 0 ;
      }

      if ( _meta._lobLen == _offset )
      {
         rc = SDB_EOF ;
         goto error ;
      }

      if ( _pool.match( _offset ) )
      {
         rc = _readFromPool( len, context, cb, readLen ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to read data from pool:%d", rc ) ;
            goto error ;
         }

         _offset += readLen ;
         goto done ;
      }

      _pool.clear() ;

      rc = _lw.prepare2Read( _meta._lobLen,
                             _offset, len,
                             tuples ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to read:%d", rc ) ;
         goto error ;      
      }

      rc = _readv( tuples, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read lob[%s], rc:%d",
                    _oid.str().c_str(), rc ) ;
         goto error ;
      }

      rc = _readFromPool( len, context, cb, readLen ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read data from pool:%d", rc ) ;
         goto error ;
      }

      _offset += len ;
      read = readLen ;
   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM_READ, rc ) ;
      return rc ;
   error:
      closeWithException( cb ) ;
      goto done ;
   }

   INT32 _rtnLobStream::seek( SINT64 offset, _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      if ( !isOpened() )
      {
         PD_LOG( PDERROR, "lob[%s] is not opened yet",
                 _oid.str().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( SDB_LOB_MODE_R != _mode  )
      {
         PD_LOG( PDERROR, "open mode[%d] does not support this operation",
                 _mode ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( _meta._lobLen < offset )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _offset = offset ;
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM_TRUNCATE, ""_rtnLobStream::truncate" )
   INT32 _rtnLobStream::truncate( SINT64 len,
                                  _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM_TRUNCATE ) ;
      SDB_ASSERT( SDB_LOB_MODE_REMOVE == _mode && 0 == len,
                  "do not support other params now" ) ;

      RTN_LOB_TUPLES tuples ;
      UINT32 pieceNum = 0 ;
      UINT32 oneLoopNum = 0 ;

      RTN_LOB_GET_SEQUENCE_NUM( _meta._lobLen, _lobPageSz, pieceNum ) ;
      while ( 1 < pieceNum-- )
      {
         tuples.push_back( _rtnLobTuple( 0, pieceNum, 0, NULL ) ) ;
         ++oneLoopNum ;

         if ( 1000 == oneLoopNum )
         {
            rc = _removev( tuples, cb ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to truncate lob:%d", rc ) ;
               goto error ;
            }

            oneLoopNum = 0 ;
            tuples.clear() ;
         }
      }

      if ( !tuples.empty() )
      {
         rc = _removev( tuples, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to truncate lob:%d", rc ) ;
            goto error ;
         }
         tuples.clear() ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM_TRUNCATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM__READFROMPOOL, "_rtnLobStream::_readFromPool" )
   INT32 _rtnLobStream::_readFromPool( UINT32 len,
                                       _rtnContextBase *context,
                                       _pmdEDUCB *cb,
                                       UINT32 &readLen )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM__READFROMPOOL ) ;
      const CHAR *data = NULL ;
      _MsgLobTuple tuple ;
      tuple.columns.len = len <= _pool.getLastDataSize() ?
                          len : _pool.getLastDataSize() ;
      tuple.columns.offset = _offset ;
      tuple.columns.sequence = 0 ; /// it is useless column now.
      UINT32 needLen = tuple.columns.len ;

      SDB_ASSERT( _pool.match( _offset ), "impossible" ) ;

      rc = context->appendObjs( tuple.data, sizeof( tuple.data ), 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to append data to context%d", rc ) ;
         goto error ;
      }

      while ( 0 < needLen )
      {
         UINT32 dataLen = 0 ;
         if ( _pool.next( needLen, &data, dataLen ) )
         {
            needLen -= dataLen ;
            readLen += dataLen ;
            rc = context->appendObjs( data, dataLen, 0, FALSE ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to append data to context%d", rc ) ;
               goto error ;
            }
         }
         else
         {
            break ;
         }
      }

      SDB_ASSERT( readLen == tuple.columns.len, "impossible" ) ;
      rc = context->appendObjs( NULL, 0, 1 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to append data to context%d", rc ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM__READFROMPOOL, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM__OPEN4READ, "_rtnLobStream::_open4Read" )
   INT32 _rtnLobStream::_open4Read( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM__OPEN4READ ) ;

      rc = _queryLobMeta( cb, _meta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob[%s] in collection[%s], rc:%d",
                 _oid.str().c_str(), _fullName, rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM__OPEN4READ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM__OPEN4CREATE, "_rtnLobStream::_open4Create" )
   INT32 _rtnLobStream::_open4Create( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM__OPEN4CREATE ) ;
      BOOLEAN isNew = TRUE ;
      rc = _ensureLob( cb, _meta, isNew ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob[%s] in collection[%s], rc:%d",
                 _oid.str().c_str(), _fullName, rc ) ;
         goto error ;
      }

      if ( !isNew )
      {
         PD_LOG( PDERROR, "lob[%s] exists in collection[%s]",
                 _oid.str().c_str(), _fullName ) ;
         rc = SDB_FE ;
         goto error ;
      }

      PD_LOG( PDDEBUG, "lob[%s] in [%s] is created, wait to be completed",
              getOID().str().c_str(), _fullName ) ;
   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM__OPEN4CREATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBSTREAM__OPEN4REMOVE, "_rtnLobStream::_open4Remove" )
   INT32 _rtnLobStream::_open4Remove( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBSTREAM__OPEN4REMOVE ) ;

      rc = _queryAndInvalidateMetaData( cb, _meta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob[%s] in collection[%s], rc:%d",
                 _oid.str().c_str(), _fullName, rc ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB_RTNLOBSTREAM__OPEN4REMOVE, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

