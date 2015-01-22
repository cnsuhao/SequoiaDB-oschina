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

   Source File Name = rtnContextLob.hpp

   Descriptive Name = N/A

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/19/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_CONTEXTLOB_HPP_
#define RTN_CONTEXTLOB_HPP_

#include "rtnContext.hpp"

using namespace bson ;

namespace engine
{
   class _rtnLobStream ;

   class _rtnContextLob : public _rtnContextBase
   {
   public:
      _rtnContextLob( INT64 contextID, UINT64 eduID ) ;
      virtual ~_rtnContextLob() ;

   public:
      virtual RTN_CONTEXT_TYPE getType() const { return RTN_CONTEXT_LOB ; }
      virtual _dmsStorageUnit*  getSU () ;

   public:
      INT32 open( const BSONObj &lob,
                  BOOLEAN isLocal,
                  _pmdEDUCB *cb,
                  SDB_DPSCB *dpsCB ) ;

      INT32 read( UINT32 len,
                  SINT64 offset,
                  _pmdEDUCB *cb ) ;

      INT32 write( UINT32 len,
                   const CHAR *buf,
                   _pmdEDUCB *cb ) ;

      INT32 close( _pmdEDUCB *cb ) ;

      INT32 getLobMetaData( BSONObj &meta ) ;

   protected:
      virtual INT32 _prepareData( _pmdEDUCB *cb ) ;
      virtual BOOLEAN   _canPrefetch () const { return TRUE ; }

   private:
      rtnLobStream *_stream ;
      SINT64 _offset ;
      UINT32 _readLen ;
   } ;
   typedef class _rtnContextLob rtnContextLob ;
}

#endif

