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

   Source File Name = rtnLocalLobStream.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_LOCALLOBSTREAM_HPP_
#define RTN_LOCALLOBSTREAM_HPP_

#include "rtnLobStream.hpp"

namespace engine
{
   class _dmsMBContext ;
   class _dmsStorageUnit ;
   class _dpsLogWrapper ;

   class _rtnLocalLobStream : public rtnLobStream
   {
   public:
      _rtnLocalLobStream() ;
      virtual ~_rtnLocalLobStream() ;

   public:
      virtual _dmsStorageUnit *getSU()
      {
         return _su ;
      }
   private:
      virtual INT32 _prepare( const CHAR *fullName,
                              const bson::OID &oid,
                              INT32 mode,
                              _pmdEDUCB *cb ) ;

      virtual INT32 _queryLobMeta( _pmdEDUCB *cb,
                                   _dmsLobMeta &meta ) ;

      virtual INT32 _ensureLob( _pmdEDUCB *cb,
                                _dmsLobMeta &meta,
                                BOOLEAN &isNew ) ;

      virtual INT32 _getLobPageSize( INT32 &pageSize ) ;

      virtual INT32 _write( const _rtnLobTuple &tuple, 
                            _pmdEDUCB *cb ) ;

      virtual INT32 _writev( const RTN_LOB_TUPLES &tuples,
                             _pmdEDUCB *cb ) ;

      virtual INT32 _completeLob( const _dmsLobMeta &meta,
                                  _pmdEDUCB *cb ) ;

      virtual INT32 _rollback( _pmdEDUCB *cb ) ;

      virtual INT32 _readv( const RTN_LOB_TUPLES &tuples,
                            _pmdEDUCB *cb ) ;

      virtual INT32 _queryAndInvalidateMetaData( _pmdEDUCB *cb,
                                                 _dmsLobMeta &meta ) ;

      virtual INT32 _removev( const RTN_LOB_TUPLES &tuples,
                              _pmdEDUCB *cb ) ;

      virtual INT32 _close( _pmdEDUCB *cb ) { return SDB_OK ;}

      INT32 _read( const _rtnLobTuple &tuple,
                   _pmdEDUCB *cb,
                   CHAR *buf ) ;
   private:
      _dmsMBContext *_mbContext ;
      _dmsStorageUnit *_su ;
   } ;
   typedef class _rtnLocalLobStream rtnLocalLobStream ;
}

#endif

