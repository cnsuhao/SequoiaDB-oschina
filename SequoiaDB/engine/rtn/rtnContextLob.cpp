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

   Source File Name = rtnContextLob.cpp

   Descriptive Name = N/A

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/19/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnContextLob.hpp"
#include "pmd.hpp"
#include "rtnLocalLobStream.hpp"
#include "rtnCoordLobStream.hpp"
#include "rtnTrace.hpp"

using namespace bson ;

namespace engine
{
   _rtnContextLob::_rtnContextLob( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID ),
    _stream( NULL ),
    _offset( -1 ),
    _readLen( 0 )
   {

   }

   _rtnContextLob::~_rtnContextLob()
   {
      if ( NULL != _stream && _stream->isOpened() )
      {
         pmdKRCB *krcb = pmdGetKRCB() ;
         pmdEDUMgr *eduMgr = krcb->getEDUMgr() ;
         pmdEDUCB *cb = eduMgr->getEDUByID( eduID() ) ;
         _stream->closeWithException( cb ) ;
      }

      SAFE_OSS_DELETE( _stream ) ;
   }

   _dmsStorageUnit* _rtnContextLob::getSU()
   {
      return NULL == _stream ?
            NULL : _stream->getSU() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLOB_OPEN, "_rtnContextLob::open" )
   INT32 _rtnContextLob::open( const BSONObj &lob,
                               BOOLEAN isLocal, 
                               _pmdEDUCB *cb,
                               SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLOB_OPEN ) ;
      BSONElement mode ;
      BSONElement oid ;
      BSONElement fullName = lob.getField( FIELD_NAME_COLLECTION ) ;
      if ( String != fullName.type() )
      {
         PD_LOG( PDERROR, "can not find collection name in lob[%s]",
                 lob.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      oid = lob.getField( FIELD_NAME_LOB_OID ) ;
      if ( jstOID != oid.type() )
      {
         PD_LOG( PDERROR, "invalid oid in meta bsonobj:%s",
                 lob.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      mode = lob.getField( FIELD_NAME_LOB_OPEN_MODE ) ;
      if ( NumberInt != mode.type() )
      {
         PD_LOG( PDERROR, "invalid mode in meta bsonobj:%s",
                 lob.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( isLocal )
      {
         _stream = SDB_OSS_NEW _rtnLocalLobStream() ;
      }
      else
      {
         _stream = SDB_OSS_NEW _rtnCoordLobStream() ;
      }

      if ( NULL == _stream )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      _stream->setDPSCB( dpsCB ) ;
      rc = _stream->open( fullName.valuestr(),
                          oid.OID(),
                          mode.Int(), cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob stream:%d", rc ) ;
         goto error ;
      }

      _isOpened = TRUE ;
      _hitEnd = FALSE ;
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLOB_OPEN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLOB_WRITE, "_rtnContextLob::write" )
   INT32 _rtnContextLob::write( UINT32 len,
                                const CHAR *buf,
                                _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLOB_WRITE ) ;
      SDB_ASSERT( NULL != _stream, "can not be null" ) ;
      rc = _stream->write( len, buf, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLOB_WRITE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLOB_GETLOBMETADATA, "_rtnContextLob::getLobMetaData" )
   INT32 _rtnContextLob::getLobMetaData( BSONObj &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLOB_GETLOBMETADATA ) ;
      SDB_ASSERT( NULL != _stream, "can not be null" ) ;
      rc = _stream->getMetaData( meta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get lob meta data:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLOB_GETLOBMETADATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLOB_READ, "_rtnContextLob::read" )
   INT32 _rtnContextLob::read( UINT32 len,
                               SINT64 offset,
                               _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLOB_READ ) ;
      _readLen = len ;
      _offset = offset ;
      if ( -1 != _offset && _offset != _stream->curOffset() )
      {
         _empty() ;  /// clear data in context.
         rc = _stream->seek( _offset, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to seek lob:%d", rc ) ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLOB_READ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLOB_CLOSE, "_rtnContextLob::close" )
   INT32 _rtnContextLob::close( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLOB_CLOSE ) ;
      rc = _stream->close( cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to close lob:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLOB_CLOSE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTLOB__PREPAGEDATA, "_rtnContextLob::_prepareData" )
   INT32 _rtnContextLob::_prepareData( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTLOB__PREPAGEDATA ) ;
      UINT32 read = 0 ;
      if ( 0 == _readLen )
      {
         goto done ;
      }
/*
      if ( -1 != _offset && _offset != _stream->curOffset() )
      {
         _empty() ;  /// clear data in context.
         rc = _stream->seek( _offset, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to seek lob:%d", rc ) ;
            goto error ;
         }
      }
*/
      rc = _stream->read( _readLen, this, cb, read ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read lob:%d", rc ) ;
         goto error ;
      }

      if ( read < _readLen )
      {
         _readLen = DMS_PAGE_SIZE512K ;
         _offset += read ;
      }
      else
      {
         _readLen = 0 ;
         _offset = -1 ;
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTLOB__PREPAGEDATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

