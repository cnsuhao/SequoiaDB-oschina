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

   Source File Name = utilArray.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef UTIL_ARRAY_HPP_
#define UTIL_ARRAY_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossMem.hpp"
#include "ossUtil.hpp"

#define UTIL_ARRAY_DEFAULT_SIZE 16

namespace engine
{
   template <typename T>
   class _utilArray : public SDBObject
   {
   public:
      _utilArray( UINT32 size = 0 )
      :_dynamicBuf( NULL ),
       _eles( _staticBuf ),
       _bufSize( UTIL_ARRAY_DEFAULT_SIZE ),
       _eleSize( 0 )
      {
         if ( UTIL_ARRAY_DEFAULT_SIZE < size )
         {
            _dynamicBuf = ( T * )SDB_OSS_MALLOC( sizeof( T ) * size ) ;
            if ( NULL != _dynamicBuf )
            {
               _eles = _dynamicBuf ;
               _bufSize = size ;
            }
         }
      }

      ~_utilArray()
      {
         SAFE_OSS_FREE( _dynamicBuf ) ;
      }

   public:
      class iterator
      {
      public:
         iterator( _utilArray<T> &t )
         :_t( &t ),
          _now( 0 )
         {

         }

         ~iterator(){}

      public:
         BOOLEAN more() const
         {
            return _now < _t->_eleSize ;
         }

         void next( T &t )
         {
            if ( more() )
            {
               t = _t->_eles[_now++] ;
            }
            return ;
         }

      private:
         _utilArray<T> *_t ;
         UINT32 _now ;
      } ;

   public:
      OSS_INLINE const T &operator[]( UINT32 i ) const
      {
         return _eles[i] ;
      }

      OSS_INLINE T &operator[]( UINT32 i )
      {
         return _eles[i] ;
      }

      OSS_INLINE UINT32 size() const
      {
         return _eleSize ;
      }

      OSS_INLINE BOOLEAN empty() const
      {
         return 0 == _eleSize ;
      }

      OSS_INLINE void clear()
      {
         _eleSize = 0 ;
         return ;
      }

      OSS_INLINE INT32 append( const T &t )
      {
         INT32 rc = SDB_OK ;
         if ( _eleSize < _bufSize )
         {
            _eles[_eleSize++] = t ;
         }
         else if ( UTIL_ARRAY_DEFAULT_SIZE == _bufSize )
         {
            _dynamicBuf = ( T * )SDB_OSS_MALLOC( sizeof( T ) * ( _bufSize * 2 ) ) ;
            if ( NULL == _dynamicBuf )
            {
               rc = SDB_OOM ;
               goto error ;
            }
            _bufSize *= 2 ;
            ossMemcpy( _dynamicBuf, _staticBuf, sizeof( T ) * UTIL_ARRAY_DEFAULT_SIZE ) ;
            _eles = _dynamicBuf ;
            _eles[_eleSize++] = t ;
         }
         else
         {
            T *tmp = _dynamicBuf ;
            _dynamicBuf = ( T * )SDB_OSS_REALLOC( _dynamicBuf,
                                                  sizeof( T ) * _bufSize * 2 ) ;
            if ( NULL == _dynamicBuf )
            {
               _dynamicBuf = tmp ;
               rc = SDB_OOM ;
               goto error ;
            }
            _bufSize *= 2 ;
            _eles = _dynamicBuf ;
            _eles[_eleSize++] = t ;
         }
      done:
         return rc ;
      error:
         goto done ;
      }

      INT32 resize( UINT32 size )
      {
         INT32 rc = SDB_OK ;
         if ( size <= _bufSize ||
              size <= _eleSize )
         {
            goto done ;
         }

         else if ( UTIL_ARRAY_DEFAULT_SIZE == _bufSize )
         {
            _dynamicBuf = ( T * )SDB_OSS_MALLOC( sizeof( T ) * size ) ;
            if ( NULL == _dynamicBuf )
            {
               rc = SDB_OOM ;
               goto error ;
            }
            _bufSize = size ;
            ossMemcpy( _dynamicBuf, _staticBuf, sizeof( T ) * UTIL_ARRAY_DEFAULT_SIZE ) ;
            _eles = _dynamicBuf ;
         }
         else
         {
            T *tmp = _dynamicBuf ;
            _dynamicBuf = ( T * )SDB_OSS_REALLOC( _dynamicBuf,
                                                  sizeof( T ) * size ) ;
            if ( NULL == _dynamicBuf )
            {
               _dynamicBuf = tmp ;
               rc = SDB_OOM ;
               goto error ;
            }
            _bufSize = size ;
            _eles = _dynamicBuf ;
         } 
      done:
         return rc ;
      error:
         goto done ;
      }

      OSS_INLINE INT32 copy( _utilArray<T> &arr )
      {
         INT32 rc = SDB_OK ;
         rc = arr.resize( _eleSize ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         ossMemcpy( arr._eles, _eles, _eleSize * sizeof( T ) ) ;
         arr._eleSize = _eleSize ;
      done:
         return rc ;
      error:
         goto done ;
      }
   private:
      T _staticBuf[UTIL_ARRAY_DEFAULT_SIZE] ;
      T *_dynamicBuf ;
      T *_eles ;
      UINT32 _bufSize ;
      UINT32 _eleSize ;
   } ;
}

#endif

