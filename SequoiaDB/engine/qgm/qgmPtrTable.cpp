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

   Source File Name = qgmPtrTable.cpp

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

#include "qgmPtrTable.hpp"
#include "qgmUtil.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"

namespace engine
{
   _qgmPtrTable::_qgmPtrTable()
   {
      _uniqueFieldID = 0 ;
      _uniqueTableID = 0 ;
   }

   _qgmPtrTable::~_qgmPtrTable()
   {
      _table.clear() ;

      STR_TABLE::iterator it = _stringTable.begin() ;
      while ( it != _stringTable.end() )
      {
         SDB_OSS_FREE( *it ) ;
         ++it ;
      }
      _stringTable.clear() ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPTRTABLE_GETFIELD, "_qgmPtrTable::getField" )
   INT32 _qgmPtrTable::getField( const CHAR *begin, UINT32 size,
                                 qgmField &field )
   {
      PD_TRACE_ENTRY( SDB__QGMPTRTABLE_GETFIELD ) ;
      INT32 rc = SDB_OK ;
      if ( NULL == begin || 0 == size )
      {
         field._begin = NULL ;
         field._size = 0 ;
         goto done ;
      }
      {
      qgmField f ;
      f._begin = begin ;
      f._size = size ;
      PTR_TABLE::const_iterator itr = _table.find( f ) ;
      if ( _table.end() == itr )
      {
         field = f ;
         _table.insert( f ) ;
      }
      else
      {
         field = *itr;
      }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPTRTABLE_GETFIELD, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPTRTABLE_GETOWNFIELD, "_qgmPtrTable::getOwnField" )
   INT32 _qgmPtrTable::getOwnField( const CHAR * begin, qgmField & field )
   {
      PD_TRACE_ENTRY( SDB__QGMPTRTABLE_GETOWNFIELD ) ;
      SDB_ASSERT( NULL != begin, "impossible" ) ;
      INT32 rc = SDB_OK ;
      UINT32 size = ossStrlen( begin ) ;
      if ( NULL == begin || 0 == size )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      {
      qgmField f ;
      f._begin = begin ;
      f._size = size ;
      PTR_TABLE::const_iterator itr = _table.find( f ) ;
      if ( _table.end() == itr )
      {
         CHAR *newStr = ossStrdup( begin ) ;
         _stringTable.push_back( newStr ) ;

         field._begin = ( const CHAR*)newStr ;
         field._size = size ;
         _table.insert( field ) ;
      }
      else
      {
         field = *itr;
      }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPTRTABLE_GETOWNFIELD, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPTRTABLE_GETATTR, "_qgmPtrTable::getAttr" )
   INT32 _qgmPtrTable::getAttr( const CHAR *begin, UINT32 size,
                                qgmDbAttr &attr )
   {
      PD_TRACE_ENTRY( SDB__QGMPTRTABLE_GETATTR ) ;
      INT32 rc = SDB_OK ;
      UINT32 num = 0 ;
      BOOLEAN hasDot = qgmUtilFirstDot( begin, size, num ) ;
      if ( 1 == num || size == num )
      {
         PD_LOG( PDERROR,
                 "the first char and the last char can not be '.'" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( hasDot )
      {
         rc = getField( begin, num - 1, attr.relegation() ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }

      rc = getField( begin + num, size - num, attr.attr() ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMPTRTABLE_GETATTR, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _qgmPtrTable::getAttr( const SQL_CON_ITR &itr,
                                qgmDbAttr &attr )
   {
      const CHAR *begin = NULL ;
      UINT32 size = 0 ;
      QGM_VALUE_PTR( itr, begin, size )
      return getAttr( begin, size, attr ) ;
   }

   INT32 _qgmPtrTable::getUniqueFieldAlias( qgmField &field )
   {
      CHAR uniqueName[20] = {0} ;
      ossSnprintf( uniqueName, 19, "$SYS_F%d", ++_uniqueFieldID ) ;
      return getOwnField( uniqueName, field ) ;
   }

   INT32 _qgmPtrTable::getUniqueTableAlias( qgmField & field )
   {
      CHAR uniqueName[20] = {0} ;
      ossSnprintf( uniqueName, 19, "$SYS_T%d", ++_uniqueTableID ) ;
      return getOwnField( uniqueName, field ) ;
   }

}

