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

   Source File Name = rtnDataSet.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains structure for Runtime
   Context.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          24/06/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_BSONSET_HPP_
#define RTN_BSONSET_HPP_

#include "rtnContext.hpp"

namespace engine
{
   class _rtnDataSet : public SDBObject
   {
   public:
      _rtnDataSet( const rtnQueryOptions &options,
                   _pmdEDUCB *cb ) ;

      _rtnDataSet( SINT64 contextID,
                   _pmdEDUCB *cb ) ;

      virtual ~_rtnDataSet() ;

   public:
      INT32 next( BSONObj &obj ) ;

   private:
      rtnContextBuf     _contextBuf ;
      SINT64            _contextID ;
      _pmdEDUCB         *_cb ;
      INT32             _lastErr ;
      BOOLEAN           _fetchFromContext ;
      _SDB_RTNCB        *_rtnCB ;
   } ;
   typedef class _rtnDataSet rtnDataSet ;
}

#endif // RTN_BSONSET_HPP_

