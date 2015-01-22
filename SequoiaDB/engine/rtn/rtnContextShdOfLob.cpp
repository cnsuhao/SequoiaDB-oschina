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

   Source File Name = rtnContextShdOfLob.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnContextShdOfLob.hpp"
#include "pmdEDU.hpp"
#include "rtnTrace.hpp"
#include "rtnLob.hpp"
#include "clsMgr.hpp"

using namespace bson ;

namespace engine
{
   _rtnContextShdOfLob::_rtnContextShdOfLob( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID ),
    _mode( SDB_LOB_MODE_R ),
    _isMainShd( FALSE ),
    _w( 1 ),
    _version( 0 ),
    _dpsCB( NULL ),
    _closeWithException( TRUE ),
    _buf( NULL ),
    _bufLen( 0 ),
    _su( NULL )
   {

   }

   _rtnContextShdOfLob::~_rtnContextShdOfLob()
   {
      if ( _closeWithException &&
           SDB_LOB_MODE_CREATEONLY == _mode )
      {
         _pmdEDUCB *cb = pmdGetThreadEDUCB() ;
         SDB_ASSERT( cb->getID() == eduID(), "impossible" ) ;
         _rollback( cb ) ;
      }

      SAFE_OSS_FREE( _buf ) ;
      if ( NULL != _su )
      {
         sdbGetDMSCB()->suUnlock ( _su->CSID() ) ;
         _su = NULL ;
      }
   }

   _dmsStorageUnit* _rtnContextShdOfLob::getSU ()
   {
      return _su ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTSHDOFLOB_OPEN, "_rtnContextShdOfLob::open" )
   INT32 _rtnContextShdOfLob::open( const BSONObj &lob,
                                    SINT32 version,
                                    SINT16 w,
                                    SDB_DPSCB *dpsCB,
                                    _pmdEDUCB *cb,
                                    BSONObj &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTSHDOFLOB_OPEN ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      const CHAR *clName = NULL ;
      dmsStorageUnitID suID = DMS_INVALID_CS ;
      BSONElement ele = lob.getField( FIELD_NAME_COLLECTION ) ;
      if ( String != ele.type() )
      {
         PD_LOG( PDERROR, "invalid full name type:%d", ele.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _fullName.assign( ele.valuestr() ) ;

      rc = rtnResolveCollectionNameAndLock( _fullName.c_str(),
                                            dmsCB, &_su,
                                            &clName, suID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get cs lock:%s, rc:%d",
                 _fullName.c_str(), rc ) ;
         goto error ;
      }

      ele = lob.getField( FIELD_NAME_LOB_OPEN_MODE ) ;
      if ( NumberInt != ele.type() )
      {
         PD_LOG( PDERROR, "invalid mode type:%d", ele.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _mode = ele.Int() ;

      ele = lob.getField( FIELD_NAME_LOB_OID ) ;
      if ( jstOID != ele.type() )
      {
         PD_LOG( PDERROR, "invalid oid type:%d", ele.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossMemcpy( &_oid, &( ele.__oid() ), sizeof( _oid ) ) ;

      ele = lob.getField( FIELD_NAME_LOB_IS_MAIN_SHD ) ;
      if ( Bool != ele.type() )
      {
         PD_LOG( PDERROR, "invalid \"isMainShd\" type:%d", ele.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _isMainShd = ele.Bool() ;

      ele = lob.getField( FIELD_NAME_LOB_META_DATA ) ;
      if ( Object == ele.type() )
      {
         _metaObj = ele.embeddedObject() ;
      }
      else if ( !ele.eoo() )
      {
         PD_LOG( PDERROR, "invalid meta obj type:%d", ele.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
      {
      }

      _w = w ;
      _dpsCB = dpsCB ;
      _version = version ;

      rc = _open( cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob:%d", rc ) ;
         goto error ;
      }

      meta = _metaObj ;
      _isOpened = TRUE ;
      _hitEnd = FALSE ;
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTSHDOFLOB_OPEN, rc ) ;
      return rc ;
   error:
      if ( NULL != _su )
      {
         dmsCB->suUnlock( _su->CSID() ) ;
         _su = NULL ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTSHDOFLOB_WRITE, "_rtnContextShdOfLob::write" )
   INT32 _rtnContextShdOfLob::write( UINT32 sequence,
                                     UINT32 offset,
                                     UINT32 len,
                                     const CHAR *data,
                                     _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTSHDOFLOB_WRITE ) ;
      rc = rtnWriteLob( _fullName.c_str(),
                        _oid, sequence,
                        offset, len,
                        data, cb, _w, _dpsCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
         goto error ;
      }

      _written.insert( sequence ) ;
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTSHDOFLOB_WRITE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTSHDOFLOB_UPDATE, "_rtnContextShdOfLob::update" )
   INT32 _rtnContextShdOfLob::update( UINT32 sequence,
                                      UINT32 offset,
                                      UINT32 len,
                                      const CHAR *data,
                                      _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTSHDOFLOB_UPDATE ) ;
      rc = rtnUpdateLob( _fullName.c_str(),
                         _oid, sequence,
                         offset, len,
                         data, cb, _w, _dpsCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTSHDOFLOB_UPDATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextShdOfLob::_prepareData( _pmdEDUCB *cb )
   {
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTSHDOFLOB__OPEN, "_rtnContextShdOfLob::_open" )
   INT32 _rtnContextShdOfLob::_open( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTSHDOFLOB__OPEN ) ;
      if ( _isMainShd && SDB_LOB_MODE_R == _mode )
      {
         rc = rtnGetLobMetaData( _fullName.c_str(),
                                 _oid, cb, _meta ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get meta data of lob:%d", rc ) ;
            goto error ;
         }

      }
      else if ( SDB_LOB_MODE_CREATEONLY == _mode &&
           _isMainShd )
      {
         rc = rtnCreateLob( _fullName.c_str(),
                            _oid, cb, _w, _dpsCB ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to create lob:%d", rc ) ;
            goto error ;
         }
      }
      else if ( _isMainShd && SDB_LOB_MODE_REMOVE == _mode )
      {
         rc = rtnQueryAndInvalidateLob( _fullName.c_str(),
                                        _oid, cb, _w,
                                        _dpsCB, _meta ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to invalidate lob:%d", rc ) ;
            goto error ;
         }
      }
      else
      {
      }

      if ( _isMainShd )
      {
         _meta2Obj( _metaObj ) ;
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTSHDOFLOB__OPEN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTSHDOFLOB_READV, "_rtnContextShdOfLob::readv" )
   INT32 _rtnContextShdOfLob::readv( const MsgLobTuple *tuples,
                                     UINT32 cnt,
                                     _pmdEDUCB *cb,
                                     const CHAR **data,
                                     UINT32 &read )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTSHDOFLOB_READV ) ;
      SDB_ASSERT( NULL != tuples && 0 < cnt, "can not be null" ) ;
      UINT32 totalRead = 0 ;

      for ( UINT32 i = 0; i < cnt; ++i )
      {
         UINT32 onceRead = 0 ;
         CHAR *dataOfTuple = NULL ;
         MsgLobTuple *rt = NULL ;
         const MsgLobTuple &t = tuples[i] ;
         rc = _extendBuf( sizeof( MsgLobTuple ) + t.columns.len + totalRead ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to extend buf:%d", rc ) ;
            goto error ;
         }

         dataOfTuple = _buf + totalRead ;
         rt = ( MsgLobTuple * )dataOfTuple ;
         dataOfTuple += sizeof( MsgLobTuple ) ;

         rc = rtnReadLob( _fullName.c_str(),
                          _oid, t.columns.sequence,
                          t.columns.offset, t.columns.len,
                          cb, dataOfTuple, onceRead ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to read lob[%s:%d]:%d",
                    _oid.str().c_str(), t.columns.sequence, rc ) ;
            goto error ;
         }

         rt->columns.sequence = t.columns.sequence ;
         rt->columns.offset = t.columns.offset ;
         rt->columns.len = onceRead ;
         onceRead += sizeof( MsgLobTuple ) ; /// | tuple | data | tuple | data |
         totalRead += onceRead ;
      }

      *data = _buf ;
      read = totalRead ;
   done:
      PD_TRACE_EXITRC( SDB__RTNCONTEXTSHDOFLOB_READV, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextShdOfLob::remove( UINT32 sequence,
                                      _pmdEDUCB *cb )
   {
      return rtnRemoveLobPiece( _fullName.c_str(),
                                _oid, sequence, cb,
                                _w, _dpsCB ) ;
   }

   INT32 _rtnContextShdOfLob::close( _pmdEDUCB *cb )
   {
      _closeWithException = FALSE ;
      if ( NULL != _su )
      {
         sdbGetDMSCB()->suUnlock( _su->CSID() ) ;
         _su = NULL ;
      }
      return SDB_OK ; 
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCONTEXTSHDOFLOB__ROLLBACK, "_rtnContextShdOfLob::_rollback" )
   INT32 _rtnContextShdOfLob::_rollback( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCONTEXTSHDOFLOB__ROLLBACK ) ;
      UINT64 failedNum = 0 ;
      std::set<UINT32>::reverse_iterator itr = _written.rbegin() ;
      for ( ; itr != _written.rend(); ++itr )
      {
         if ( !sdbGetReplCB()->primaryIsMe() )
         {
            PD_LOG( PDERROR, "we are not primary any more, stop to rollback" ) ;
            break ;
         }

         rc = rtnRemoveLobPiece( _fullName.c_str(),
                                 _oid, *itr, cb,
                                 _w, _dpsCB ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to remove piece[%d] of lob, rc:%d",
                    *itr, rc ) ;
            ++failedNum ;
            rc = SDB_OK ;
         }
      }

      PD_LOG( PDEVENT, "rollback[%s]: we removed %d pieces, failed:%d",
              _oid.str().c_str(),
              _written.size(), failedNum ) ;
      _written.clear() ;

      PD_TRACE_EXITRC( SDB__RTNCONTEXTSHDOFLOB__ROLLBACK, rc ) ;
      return rc ;
   }

   void _rtnContextShdOfLob::_meta2Obj( bson::BSONObj &obj )
   {
      BSONObjBuilder builder ;
      builder.append( FIELD_NAME_LOB_SIZE, (long long)_meta._lobLen ) ;
      builder.append( FIELD_NAME_LOB_PAGE_SIZE,
                      NULL != _su ?
                     _su->getLobPageSize() : 0 ) ;
      builder.append( FIELD_NAME_LOB_CREATTIME, (long long)_meta._createTime ) ;
      obj = builder.obj() ;
      return ;
   }

   INT32 _rtnContextShdOfLob::_extendBuf( UINT32 len )
   {
      INT32 rc = SDB_OK ;
      if ( _bufLen < len )
      {
         CHAR *buf = _buf ;
         _buf = ( CHAR * )SDB_OSS_REALLOC( _buf, len ) ;
         if ( NULL == _buf )
         {
            PD_LOG( PDERROR, "failed to allocate mem." ) ;
            rc = SDB_OOM ;
            _buf = buf ;
            goto error ;
         }
         _bufLen = len ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }
}

