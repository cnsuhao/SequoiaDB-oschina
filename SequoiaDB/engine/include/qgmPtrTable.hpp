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

   Source File Name = qgmPtrTable.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains declare for QGM operators

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/09/2013  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef QGMPTRTABLE_HPP_
#define QGMPTRTABLE_HPP_

#include "qgmDef.hpp"
#include <set>
#include "ossMem.hpp"

namespace engine
{
   typedef std::set<qgmField> PTR_TABLE ;
   typedef std::vector<CHAR*> STR_TABLE ;

   class _qgmPtrTable : public SDBObject
   {
   public:
      _qgmPtrTable() ;
      virtual ~_qgmPtrTable() ;

   public:
      INT32 getField( const SQL_CON_ITR &itr,
                      qgmField &field )
      {
         const CHAR *begin = NULL ;
         UINT32 size = 0 ;
         QGM_VALUE_PTR( itr, begin, size )
         return getField( begin, size, field ) ;
      }

      INT32 getField( const CHAR *begin, UINT32 size,
                      qgmField &field ) ;

      INT32 getOwnField( const CHAR *begin, qgmField &field ) ;

      INT32 getAttr( const SQL_CON_ITR &itr,
                     qgmDbAttr &attr ) ;

      INT32 getAttr( const CHAR *begin, UINT32 size,
                     qgmDbAttr &attr ) ;

      INT32 getUniqueFieldAlias( qgmField &field ) ;
      INT32 getUniqueTableAlias( qgmField &field ) ;

   private:
      PTR_TABLE _table ;
      STR_TABLE _stringTable ;
      UINT32    _uniqueFieldID ;
      UINT32    _uniqueTableID ;

   } ;

   typedef class _qgmPtrTable qgmPtrTable ;
}

#endif

