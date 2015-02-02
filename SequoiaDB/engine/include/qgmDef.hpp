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

   Source File Name = qgmDef.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef QGMDEF_HPP_
#define QGMDEF_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "sqlGrammar.hpp"
#include "ossUtil.hpp"
#include "../bson/bson.h"
#include "mthCommon.hpp"
#include "utilStr.hpp"

using namespace bson ;

#define QGM_VALUE_PTR( itr, ptr, size )\
        { ptr = &(*(itr->value.begin()));\
          size = itr->value.end() - itr->value.begin() ;}

namespace engine
{
   class _qgmField : public SDBObject
   {
   private:
      const CHAR *_begin ;
      UINT32 _size ;

   public:
      _qgmField()
      :_begin( NULL ),
       _size( 0 )
      {

      }

      _qgmField( const _qgmField &field )
      :_begin( field._begin ),
       _size( field._size)
      {

      }

      _qgmField &operator=(const _qgmField &field )
      {
         _begin = field._begin ;
         _size = field._size ;
         return *this ;
      }

      ~_qgmField()
      {
         _begin = NULL ;
         _size = 0 ;
      }

      BOOLEAN empty() const
      {
         return NULL == _begin ;
      }

      void clear()
      {
         _begin = NULL ;
         _size = 0 ;
      }

      BOOLEAN operator==( const _qgmField &field )const
      {
         return this->_begin == field._begin
                && this->_size == field._size ;
      }

      BOOLEAN operator!=( const _qgmField &field )const
      {
         return !( this->_begin == field._begin
                   && this->_size == field._size ) ;
      }

      BOOLEAN operator<( const _qgmField &field )const
      {
         UINT32 i = 0 ;
         while ( i < this->_size && i < field._size )
         {
            if ( _begin[i] < field._begin[i] )
            {
               return TRUE ;
            }
            else if ( _begin[i] > field._begin[i] )
            {
               return FALSE ;
            }
            else
            {
               ++i ;
            }
         }

         return this->_size < field._size ?
                TRUE : FALSE ;
      }

      const CHAR *begin() const
      {
         return _begin ;
      }

      UINT32 size() const
      {
         return _size ;
      }

      string toString() const
      {
         if ( _begin )
         {
            return string( _begin, _size ) ;
         }
         return "" ;
      }

      string toFieldName() const
      {
         stringstream ss ;
         string str = toString() ;
         if ( !str.empty() )
         {
            utilSplitIterator i( ( CHAR * )( str.c_str() ) ) ;
            while ( i.more() )
            {
               const CHAR *left = i.next() ;
               if ( '$' == *left && '[' == *(left + 1) )
               {
                  INT32 n = 0 ;
                  mthConvertSubElemToNumeric( left, n ) ;
                  ss << n << '.';
               }
               else
               {
                  ss << left << '.' ;
               }
            }
            ss.seekp( (INT32)ss.tellp() - 1 ) ;
            ss << '\0' ;
         }
         return ss.str() ;
      }

      friend class _qgmPtrTable ;
   } ;
   typedef class _qgmField qgmField ;

   class _qgmDbAttr : public SDBObject
   {
   public:
      _qgmDbAttr( const qgmField &relegation,
                  const qgmField &attr )
      :_relegation(relegation),
       _attr(attr)
      {

      }

      _qgmDbAttr(){}

      _qgmDbAttr( const _qgmDbAttr &attr )
      :_relegation( attr._relegation),
       _attr( attr._attr )
       {

       }

      _qgmDbAttr &operator=( const _qgmDbAttr &attr )
      {
         _relegation = attr._relegation ;
         _attr = attr._attr ;
         return *this ;
      }

      virtual ~_qgmDbAttr(){}

   public:
      OSS_INLINE const qgmField &relegation() const { return _relegation ;}

      OSS_INLINE const qgmField &attr() const { return _attr ;}

      OSS_INLINE qgmField &relegation() { return _relegation ;}

      OSS_INLINE qgmField &attr() { return _attr ;}

      OSS_INLINE BOOLEAN empty()const
      {
         return _relegation.empty() && _attr.empty() ;
      }

      OSS_INLINE BOOLEAN operator==( const _qgmDbAttr &attr ) const
      {
         return _relegation == attr._relegation
                && _attr == attr._attr ;
      }

      OSS_INLINE string toString() const
      {
         if ( !_relegation.empty() && !_attr.empty() )
         {
            stringstream ss ;
            ss << _relegation.toString()
               << "."
               << _attr.toString() ;
            return ss.str() ;
         }
         else
         {
            return _relegation.empty() ?
                   _attr.toString() : _relegation.toString() ;
         }
      }

      OSS_INLINE string toFieldName() const
      {
         if ( !_relegation.empty() && !_attr.empty() )
         {
            stringstream ss ;
            ss << _relegation.toString()
               << "."
               << _attr.toFieldName() ;
            return ss.str() ;
         }
         else
         {
            return _relegation.empty() ?
                   _attr.toFieldName() : _relegation.toString() ;
         }
      }

      OSS_INLINE BOOLEAN operator<( const _qgmDbAttr &attr ) const
      {
         if ( _relegation < attr._relegation )
         {
            return TRUE ;
         }
         else if ( _relegation == attr._relegation )
         {
            return _attr < attr._attr ;
         }
         else
         {
            return FALSE ;
         }
      }
   private:
      qgmField _relegation ;
      qgmField _attr ;
   } ;
   typedef class _qgmDbAttr qgmDbAttr ;
   typedef vector< qgmDbAttr* > qgmDbAttrPtrVec ;
   typedef vector< qgmDbAttr >  qgmDbAttrVec ;

   struct _qgmOpField : public SDBObject
   {
      qgmDbAttr value ;
      qgmField alias ;
      INT32 type ;

      _qgmOpField()
      :type(SQL_GRAMMAR::SQLMAX)
      {}

      _qgmOpField( const _qgmOpField &field )
      :value( field.value ),
       alias( field.alias ),
       type( field.type )
      {

      }

      _qgmOpField &operator=( const _qgmOpField &field )
      {
         value = field.value ;
         alias = field.alias ;
         type = field.type ;
         return *this ;
      }

      _qgmOpField( const qgmDbAttr &attr, INT32 attrType )
      :value( attr ), type( attrType )
      {
      }

      virtual ~_qgmOpField(){}

      BOOLEAN operator==( const _qgmOpField &field )
      {
         return value == field.value
                && alias == field.alias
                && type == field.type ;
      }


      BOOLEAN isFrom( const _qgmOpField &right, BOOLEAN useAlias = TRUE ) const
      {
         if ( value == right.value || right.type == SQL_GRAMMAR::WILDCARD
              || ( useAlias && value.attr() == right.alias ) )
         {
            return TRUE ;
         }
         return FALSE ;
      }

      BOOLEAN empty() const
      {
         return value.empty() && alias.empty() ;
      }

      string toString() const
      {
         stringstream ss ;
         ss << "value:" << value.toString() ;
         ss << ",type:" << type ;
         if ( !alias.empty() )
         {
            ss << ",alias:" << alias.toString() ;
         }
         return ss.str() ;
      }
   } ;
   typedef struct _qgmOpField qgmOpField ;
   typedef std::vector< qgmOpField >  qgmOPFieldVec ;
   typedef std::vector< qgmOpField* > qgmOPFieldPtrVec ;


   struct _qgmFetchOut : public SDBObject
   {
      _qgmField alias ;
      BSONObj obj ;
      _qgmFetchOut *next ;

      _qgmFetchOut()
      :next( NULL )
      {

      }

      virtual ~_qgmFetchOut()
      {
         next = NULL ;
      }

      INT32 element( const _qgmDbAttr &attr, BSONElement &ele )const ;

      void elements( std::vector<BSONElement> &eles ) const ;

      BSONObj mergedObj() const ;
   } ;
   typedef struct _qgmFetchOut qgmFetchOut ;

   typedef struct _varItem
   {
      qgmDbAttr      _fieldName ;
      qgmDbAttr      _varName ;
   }varItem ;

   typedef vector< varItem >     QGM_VARLIST ;

   struct _qgmHint : public SDBObject
   {
      qgmField value ;
      vector<qgmOpField> param ;
   } ;
   typedef struct _qgmHint qgmHint ;

   typedef vector<qgmHint>  QGM_HINS ;

   enum QGM_JOIN_ACHIEVE
   {
      QGM_JOIN_ACHIEVE_NL = 0,
      QGM_JOIN_ACHIEVE_HASH,
      QGM_JOIN_ACHIEVE_MERGE,
   } ;
}

#endif

