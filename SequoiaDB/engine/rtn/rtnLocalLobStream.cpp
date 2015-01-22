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

   Source File Name = rtnLocalLobStream.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnLocalLobStream.hpp"
#include "rtnTrace.hpp"
#include "dmsStorageUnit.hpp"
#include "dmsCB.hpp"
#include "rtn.hpp"
#include "rtnLob.hpp"

namespace engine
{
   _rtnLocalLobStream::_rtnLocalLobStream()
   :_mbContext( NULL ),
    _su( NULL )
   {

   }

   _rtnLocalLobStream::~_rtnLocalLobStream()
   {
      if ( _mbContext && _su )
      {
         _su->data()->releaseMBContext( _mbContext ) ;
         _mbContext = NULL ;
      }

      if ( _su )
      {
         sdbGetDMSCB()->suUnlock ( _su->CSID() ) ;
         _su = NULL ;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__PREPARE, "_rtnLocalLobStream::_prepare" )
   INT32 _rtnLocalLobStream::_prepare( const CHAR *fullName,
                                       const bson::OID &oid,
                                       INT32 mode,
                                       _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__PREPARE ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      dmsStorageUnitID suID = DMS_INVALID_CS ;
      const CHAR *clName = NULL ;
      
      rc = rtnResolveCollectionNameAndLock( fullName, dmsCB,
                                            &_su, &clName, suID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to resolve collection name:%s",
                 fullName ) ;
         goto error ;
      }

      rc = _su->data()->getMBContext( &_mbContext, clName, -1 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to resolve collection name:%s",
                 clName ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__PREPARE, rc ) ;
      return rc ;
   error:
      if ( _mbContext && _su )
      {
         _su->data()->releaseMBContext( _mbContext ) ;
         _mbContext = NULL ;
      }

      if ( _su )
      {
         dmsCB->suUnlock ( _su->CSID() ) ;
         _su = NULL ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__QUERYLOBMETA, "_rtnLocalLobStream::_queryLobMeta" )
   INT32 _rtnLocalLobStream::_queryLobMeta(  _pmdEDUCB *cb,
                                             _dmsLobMeta &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__QUERYLOBMETA ) ;
      rc = _su->lob()->getLobMeta( getOID(), _mbContext,
                                   cb, meta ) ;
      if ( SDB_OK == rc )
      {
         if ( !meta.isDone() )
         {
            rc = SDB_LOB_IS_NOT_AVAILABLE ;
            goto error ;
         }

         goto done ;
      }
      else if ( SDB_FNE != rc )
      {
         PD_LOG( PDERROR, "failed to get meta of lob:%d", rc ) ;
         goto error ;
      }
      else
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__QUERYLOBMETA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__ENSURELOB, "_rtnLocalLobStream::_ensureLob" )
   INT32 _rtnLocalLobStream::_ensureLob( _pmdEDUCB *cb,
                                         _dmsLobMeta &meta,
                                         BOOLEAN &isNew )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__ENSURELOB ) ;
      BOOLEAN lockMbBlock = FALSE ;
      BOOLEAN lockDms = FALSE ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;

      rc = _mbContext->mbLock( EXCLUSIVE ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;
      lockMbBlock = TRUE ;

      rc = _su->lob()->getLobMeta( getOID(), _mbContext,
                                   cb, meta ) ;
      if ( SDB_OK == rc )
      {
         if ( !meta.isDone() )
         {
            rc = SDB_LOB_IS_NOT_AVAILABLE ;
            goto error ;
         }
         isNew = FALSE ;
         goto done ;
      }
      else if ( SDB_FNE != rc )
      {
         PD_LOG( PDERROR, "failed to get meta of lob:%d", rc ) ;
         goto error ;
      }
      else
      {
         rc = SDB_OK ;
         rc = dmsCB->writable( cb ) ;
         if ( SDB_OK !=rc )
         {
            PD_LOG ( PDERROR, "database is not writable, rc = %d", rc ) ;
            goto error ;
         }
         lockDms = TRUE ;

         if ( !dmsAccessAndFlagCompatiblity ( _mbContext->mb()->_flag,
                                              DMS_ACCESS_TYPE_INSERT ) )
         {
            PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                     _mbContext->mb()->_flag ) ;
            rc = SDB_DMS_INCOMPATIBLE_MODE ;
            goto error ;
         }

         meta.clear() ;
         rc = _su->lob()->writeLobMeta( getOID(), _mbContext,
                                        cb, meta, TRUE, _getDPSCB() ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to ensure meta:%d", rc ) ;
            goto error ;
         }

         isNew = TRUE ;
      }

   done:
      if ( lockMbBlock )
      {
         _mbContext->mbUnlock() ;
      }
      if ( lockDms )
      {
         dmsCB->writeDown( cb ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__ENSURELOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__COMPLETELOB, "_rtnLocalLobStream::_completeLob" )
   INT32 _rtnLocalLobStream::_completeLob( const _dmsLobMeta &meta,
                                           _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__COMPLETELOB ) ;
      SDB_ASSERT( meta.isDone(), "impossible" ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      dmsLobRecord record ;
      BOOLEAN lockDms = FALSE ;

      rc = dmsCB->writable( cb ) ;
      if ( SDB_OK !=rc )
      {
         PD_LOG ( PDERROR, "database is not writable, rc = %d", rc ) ;
         goto error ;
      }
      lockDms = TRUE ;

      record.set( &getOID(), DMS_LOB_META_SEQUENCE, 0,
                  sizeof( dmsLobMeta ), ( const CHAR * )( &meta ) ) ;

      rc = _su->lob()->update( record, _mbContext, cb, _getDPSCB() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write to lob:%d", rc ) ;
         goto error ;
      }
   done:
      if ( lockDms )
      {
         dmsCB->writeDown( cb );
      }
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__COMPLETELOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnLocalLobStream::_getLobPageSize( INT32 &pageSize )
   {
      SDB_ASSERT( NULL != _su, "can not be null" ) ;
      pageSize = _su->getLobPageSize() ;
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__WRITEV, "_rtnLocalLobStream::_writev" )
   INT32 _rtnLocalLobStream::_writev( const RTN_LOB_TUPLES &tuples,
                                      _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__WRITEV ) ;
      for ( RTN_LOB_TUPLES::const_iterator itr = tuples.begin() ;
            itr != tuples.end() ;
            ++itr ) 
      {
         SDB_ASSERT( !itr->empty(), "can not be empty" ) ;
         if ( cb->isInterrupted() )
         {
            rc = SDB_INTERRUPT ;
            goto error ;
         }

         rc = _write( *itr, cb ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__WRITEV, rc ) ;
      return rc ;
   error:
      goto done ;
   }


   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__WRITE, "_rtnLocalLobStream::_write" )
   INT32 _rtnLocalLobStream::_write( const _rtnLobTuple &tuple,
                                     _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__WRITE ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      BOOLEAN lockDms = FALSE ;
      _dmsLobRecord record ;

      rc = dmsCB->writable( cb ) ;
      if ( SDB_OK !=rc )
      {
         PD_LOG ( PDERROR, "database is not writable, rc = %d", rc ) ;
         goto error ;
      }
      lockDms = TRUE ;

      record.set( &getOID(),
                  tuple.tuple.columns.sequence,
                  tuple.tuple.columns.offset,
                  tuple.tuple.columns.len,
                  tuple.data ) ;
      rc = _su->lob()->write( record, _mbContext, cb,
                              _getDPSCB() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write lob[%s],"
                 "sequence:%d, rc:%d", record._oid->str().c_str(),
                 record._sequence, rc ) ;
         goto error ;
      }
   done:
      if ( lockDms )
      {
         dmsCB->writeDown( cb ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__WRITE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__READV, "_rtnLocalLobStream::_readv" )
   INT32 _rtnLocalLobStream::_readv( const RTN_LOB_TUPLES &tuples,
                                     _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__READV ) ;
      SDB_ASSERT( !tuples.empty(), "can not be empty" ) ;

      CHAR *buf = NULL ;
      SINT64 readSize = 0 ;
      INT32 pageSize =  _su->getLobPageSize() ;
      UINT32 needLen = 0 ;
      _getPool().clear() ;

      for ( RTN_LOB_TUPLES::const_iterator itr = tuples.begin() ;
            itr != tuples.end() ;
            ++itr )
      {
         needLen += itr->tuple.columns.len ;
      }

      rc = _getPool().allocate( needLen, &buf ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to allocate buf:%d", rc ) ;
         goto error ;
      }

      for ( RTN_LOB_TUPLES::const_iterator itr = tuples.begin() ;
            itr != tuples.end() ;
            ++itr )
      {
         rc = _read( *itr, cb, buf + readSize ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         readSize += itr->tuple.columns.len ;
      }

      SDB_ASSERT( readSize == needLen, "impossible" ) ;
      rc = _getPool().push( buf, readSize,
                            RTN_LOB_GET_OFFSET_OF_LOB(
                                pageSize,
                                tuples.begin()->tuple.columns.sequence,
                                tuples.begin()->tuple.columns.offset ) ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to push data to pool:%d", rc ) ;
         goto error ;
      }

      _getPool().pushDone() ;
   done:
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__READV, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnLocalLobStream::_read( const _rtnLobTuple &tuple,
                                    _pmdEDUCB *cb,
                                    CHAR *buf )
   {
      INT32 rc = SDB_OK ;
      UINT32 len = 0 ;
      dmsLobRecord record ;
      record.set( &getOID(),
                  tuple.tuple.columns.sequence,
                  tuple.tuple.columns.offset,
                  tuple.tuple.columns.len,
                  tuple.data ) ;
      rc = _su->lob()->read( record, _mbContext, cb,
                             buf, len ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read lob[%s], sequence[%d], rc:%d",
                 record._oid->str().c_str(), record._sequence, rc ) ;
         goto error ;
      }

      SDB_ASSERT( len == record._dataLen, "impossible" ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__ROLLBACK, "_rtnLocalLobStream::_rooback" )
   INT32 _rtnLocalLobStream::_rollback( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__ROLLBACK ) ;
      dmsLobRecord piece ;
      INT32 num = 0 ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      BOOLEAN lockDms = FALSE ;

      rc = dmsCB->writable( cb ) ;
      if ( SDB_OK !=rc )
      {
         PD_LOG ( PDERROR, "database is not writable, rc = %d", rc ) ;
         goto error ;
      }
      lockDms = TRUE ;

      RTN_LOB_GET_SEQUENCE_NUM( curOffset(), _getPageSz(), num ) ;
      
      while ( 0 < num )
      {
         --num ;
         piece.set( &getOID(), num, 0, 0, NULL ) ;
         rc = _su->lob()->remove( piece, _mbContext, cb,
                                  _getDPSCB() ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to remove lob[%s],"
                    "sequence:%d, rc:%d", piece._oid->str().c_str(),
                    piece._sequence, rc ) ;
            if ( SDB_LOB_SEQUENCE_NOT_EXIST != rc )
            {
               goto error ;
            }
            rc = SDB_OK ;
         }
      }
   done:
      if ( lockDms )
      {
         dmsCB->writeDown( cb ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__ROLLBACK, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__QUERYANDINVALIDATEMETADATA, "_rtnLocalLobStream::_queryAndInvalidateMetaData" )
   INT32 _rtnLocalLobStream::_queryAndInvalidateMetaData( _pmdEDUCB *cb,
                                                          _dmsLobMeta &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__QUERYANDINVALIDATEMETADATA ) ;
      rc = rtnQueryAndInvalidateLob( getFullName(), getOID(),
                                     cb, 1, _getDPSCB(), meta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to invalidate lob:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__QUERYANDINVALIDATEMETADATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOCALLOBSTREAM__REMOVEV, "_rtnLocalLobStream::_removev" )
   INT32 _rtnLocalLobStream::_removev( const RTN_LOB_TUPLES &tuples,
                                       _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOCALLOBSTREAM__REMOVEV ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      BOOLEAN lockDms = FALSE ;
      dmsLobRecord record ;

      rc = dmsCB->writable( cb ) ;
      if ( SDB_OK !=rc )
      {
         PD_LOG ( PDERROR, "database is not writable, rc = %d", rc ) ;
         goto error ;
      }
      lockDms = TRUE ;

      for ( RTN_LOB_TUPLES::const_iterator itr = tuples.begin() ;
            itr != tuples.end() ;
            ++itr )
      {
         record.set( &getOID(),
                     itr->tuple.columns.sequence,
                     itr->tuple.columns.offset,
                     itr->tuple.columns.len,
                     itr->data ) ;
         rc = _su->lob()->remove( record, _mbContext, cb,
                                  _getDPSCB() ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to remove lob[%s],"
                    "sequence:%d, rc:%d", record._oid->str().c_str(),
                    record._sequence, rc ) ;
            goto error ;
         }
      }
   done:
      if ( lockDms )
      {
         dmsCB->writeDown( cb ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNLOCALLOBSTREAM__REMOVEV, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

