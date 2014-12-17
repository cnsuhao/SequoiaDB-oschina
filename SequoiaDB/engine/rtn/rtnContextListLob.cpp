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

   Source File Name = rtnContextListLob.cpp

   Descriptive Name = N/A

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/19/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnContextListLob.hpp"
#include "rtnTrace.hpp"
#include "rtnLob.hpp"

namespace engine
{
   _rtnContextListLob::_rtnContextListLob( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID ),
    _buf( NULL ),
    _bufLen( 0 ),
    _fetchLobHead( TRUE )
   {

   }

   _rtnContextListLob::~_rtnContextListLob()
   {
      if ( NULL != _buf )
      {
         SDB_OSS_FREE( _buf ) ;
         _buf = NULL ;
         _bufLen = 0 ;
      }
   }

   _dmsStorageUnit* _rtnContextListLob::getSU()
   {
      return _fetcher.getSu() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLISTLOB_OPEN, "_rtnContextListLob::open" )
   INT32 _rtnContextListLob::open( const BSONObj &condition,
                                   _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLISTLOB_OPEN ) ;
      BSONElement fullName ;

      fullName = condition.getField( FIELD_NAME_COLLECTION ) ;
      if ( String != fullName.type() )
      {
         PD_LOG( PDERROR, "invalid collection name in condition:%s",
                 condition.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _fetchLobHead = condition.getField( FIELD_NAME_LOB_LIST_PIECES_MODE ).eoo() ;

      rc = _fetcher.init( fullName.valuestr(),
                          _fetchLobHead ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to init lob fetcher:%d", rc ) ;
         goto error ;
      }

      _fullName.assign( fullName.valuestr() ) ;

      _isOpened = TRUE ;
      _hitEnd = FALSE ;
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLISTLOB_OPEN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLISTLOB__PREPAGEDATA, "_rtnContextListLob::_prepareData" )
   INT32 _rtnContextListLob::_prepareData( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLISTLOB__PREPAGEDATA ) ;
      BSONObj obj ;
      for ( INT32 i = 0; i < 100; ++i )
      {
         rc = _fetchLobHead ?_getMetaInfo( cb, obj ) :
                             _getSequenceInfo( cb, obj ) ;
         if ( SDB_OK == rc )
         {
            rc = append( obj ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to append data to context:%d", rc ) ;
               goto error ;
            }
         }
         else if ( SDB_DMS_EOC == rc )
         {
            _hitEnd = TRUE ;
            goto error ;   
         }
         else if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get lob data:%d", rc ) ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLISTLOB__PREPAGEDATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLISTLOB__GETMETAINFO, "_rtnContextListLob::_getMetaInfo" )
   INT32 _rtnContextListLob::_getMetaInfo( _pmdEDUCB *cb, BSONObj &obj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLISTLOB__GETMETAINFO ) ;
      _dmsLobInfoOnPage info ;
      UINT32 read = 0 ;
      const _dmsLobMeta *meta = NULL ;
      BSONObjBuilder builder ;

      rc = _fetcher.fetch( cb, info ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "failed to fetch lob:%d", rc ) ;
         }
         goto error ;
      }

      rc = _reallocate( info._len ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to reallocate buf:%d", rc ) ;
         goto error ;
      }

      rc = rtnReadLob( _fullName.c_str(), info._oid,
                       info._sequence, 0, info._len,
                       cb, _buf, read ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read lob[%s], rc:%d",
                 info._oid.str().c_str(), rc ) ;
         goto error ;
      }

      SDB_ASSERT( read == info._len, "impossible" ) ;

      meta = ( const _dmsLobMeta* )_buf ;
      builder.append( FIELD_NAME_LOB_SIZE, meta->_lobLen ) ;
      builder.appendOID( FIELD_NAME_LOB_OID, &( info._oid ) ) ;
      builder.appendTimestamp( FIELD_NAME_LOB_CREATTIME,
                               meta->_createTime,
                               (meta->_createTime - ( meta->_createTime / 1000 * 1000 ) ) * 1000) ;
      builder.appendBool( FIELD_NAME_LOB_AVAILABLE, meta->isDone() ) ;
      obj = builder.obj() ;
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLISTLOB__GETMETAINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLISTLOB__GETSEQUENCEINFO, "_rtnContextListLob::_getSequenceInfo" )
   INT32 _rtnContextListLob::_getSequenceInfo( _pmdEDUCB *cb, BSONObj &obj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLISTLOB__GETSEQUENCEINFO ) ;
      _dmsLobInfoOnPage info ;
      BSONObjBuilder builder ;

      rc = _fetcher.fetch( cb, info ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "failed to fetch lob:%d", rc ) ;
         }
         goto error ;
      }

      builder.appendOID( FIELD_NAME_LOB_OID, &( info._oid ) ) ;
      builder.append( "Sequence", info._sequence ) ;
      obj = builder.obj() ;
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLISTLOB__GETSEQUENCEINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextListLob::_reallocate( UINT32 len )
   {
      INT32 rc = SDB_OK ;
      if ( len <= _bufLen )
      {
         goto done ;
      }
      else if ( NULL != _buf )
      {
         SDB_OSS_FREE( _buf ) ;
         _bufLen = 0 ;
         _buf = NULL ;
      }

      _buf = ( CHAR * )SDB_OSS_MALLOC( len ) ;
      if ( NULL == _buf )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }
   done:
      return rc; 
   error:
      goto done ;
   }
}

