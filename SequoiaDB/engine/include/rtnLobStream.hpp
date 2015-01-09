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

   Source File Name = rtnLobStream.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          05/08/2014  YW Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_LOBSTREAM_HPP_
#define RTN_LOBSTREAM_HPP_

#include "dmsLobDef.hpp"
#include "rtnLobWindow.hpp"
#include "rtnLobDataPool.hpp"
#include "../bson/bson.hpp"

namespace engine
{
#define SDB_LOB_MODE_REMOVE 0x00000010

   class _pmdEDUCB ;
   class _dmsStorageUnit ;
   class _dpsLogWrapper ;
   class _rtnContextBase ;

   class _rtnLobStream : public SDBObject
   {
   public:
      _rtnLobStream() ;
      virtual ~_rtnLobStream() ;

   public:

      INT32 open( const CHAR *fullName,
                  const bson::OID &oid,
                  INT32 mode,
                  _pmdEDUCB *cb ) ;

      INT32 close( _pmdEDUCB *cb ) ;

      INT32 write( UINT32 len,
                   const CHAR *buf,
                   _pmdEDUCB *cb ) ;

      INT32 read( UINT32 len,
                  _rtnContextBase *context,
                  _pmdEDUCB *cb,
                  UINT32 &read ) ;
 
      INT32 next( _pmdEDUCB *cb,
                  const CHAR **buf,
                  UINT32 &len ) ;

      INT32 seek( SINT64 offset,
                  _pmdEDUCB *cb ) ;

      INT32 truncate( SINT64 len,
                      _pmdEDUCB *cb ) ;

      INT32 closeWithException( _pmdEDUCB *cb ) ;

      INT32 getMetaData( bson::BSONObj &meta ) ;

      OSS_INLINE const bson::OID &getOID() const
      {
         return _oid ;
      }

      OSS_INLINE BOOLEAN isOpened() const
      {
         return _opened ;
      }

      OSS_INLINE SINT64 curOffset() const
      {
         return _offset ; 
      }

      virtual _dmsStorageUnit *getSU() = 0 ;

      OSS_INLINE void setDPSCB( _dpsLogWrapper *dpsCB )
      {
         _dpsCB = dpsCB ;
         return ;
      }

      OSS_INLINE const CHAR *getFullName() const
      {
         return _fullName ;
      }

   protected:
      OSS_INLINE _dmsLobMeta &_getMeta()
      {
         return _meta ;
      }

      OSS_INLINE INT32 _getPageSz() const
      {
         return _lobPageSz ;
      }

   protected:
      _dpsLogWrapper *_getDPSCB()
      {
         return _dpsCB ;
      }

      _rtnLobDataPool &_getPool()
      {
         return _pool ;
      }

      INT32 _getMode() const
      {
         return _mode ;
      }

   private:
      virtual INT32 _prepare( const CHAR *fullName,
                              const bson::OID &oid,
                              INT32 mode,
                              _pmdEDUCB *cb ) = 0 ;

      virtual INT32 _queryLobMeta( _pmdEDUCB *cb,
                                   _dmsLobMeta &meta ) = 0 ;

      virtual INT32 _ensureLob( _pmdEDUCB *cb,
                                _dmsLobMeta &meta,
                                BOOLEAN &isNew ) = 0 ;

      virtual INT32 _getLobPageSize( INT32 &pageSize ) = 0 ;

      virtual INT32 _write( const _rtnLobTuple &tuple,
                            _pmdEDUCB *cb ) = 0 ;

      virtual INT32 _writev( const RTN_LOB_TUPLES &tuples,
                             _pmdEDUCB *cb ) = 0 ;

      virtual INT32 _readv( const RTN_LOB_TUPLES &tuples,
                            _pmdEDUCB *cb ) = 0 ;

      virtual INT32 _completeLob( const _dmsLobMeta &meta,
                                  _pmdEDUCB *cb ) = 0 ;

      virtual INT32 _close( _pmdEDUCB *cb ) = 0 ;

      virtual INT32 _rollback( _pmdEDUCB *cb ) { return SDB_SYS ; }

      virtual INT32 _queryAndInvalidateMetaData( _pmdEDUCB *cb,
                                                 _dmsLobMeta &meta ) = 0 ;

      virtual INT32 _removev( const RTN_LOB_TUPLES &tuples,
                              _pmdEDUCB *cb ) = 0 ;

   private:
      INT32 _readFromPool( UINT32 len,
                           _rtnContextBase *context,
                           _pmdEDUCB *cb,
                           UINT32 &readLen ) ;

      INT32 _open4Read( _pmdEDUCB *cb ) ;

      INT32 _open4Create( _pmdEDUCB *cb ) ;

      INT32 _open4Remove( _pmdEDUCB *cb ) ;

   private:
      CHAR _fullName[ DMS_COLLECTION_SPACE_NAME_SZ +
                      DMS_COLLECTION_NAME_SZ + 2 ] ;
      _dpsLogWrapper *_dpsCB ;
      bson::OID _oid ;
      _dmsLobMeta _meta ;
      bson::BSONObj _metaObj ;
      _rtnLobDataPool _pool ;
      BOOLEAN _opened ;
      _rtnLobWindow _lw ;
      UINT32 _mode ;
      SINT32 _lobPageSz ;
      SINT64 _offset ;
   } ;
   typedef class _rtnLobStream rtnLobStream ;
}

#endif

