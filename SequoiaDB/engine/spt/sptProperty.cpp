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

   Source File Name = sptProperty.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptProperty.hpp"
#include "pd.hpp"
#include "ossUtil.hpp"
#include "sptBsonobj.hpp"
#include "sptBsonobjArray.hpp"

using namespace bson ;

namespace engine
{
   _sptProperty::_sptProperty()
   :_value( 0 ),
    _type( EOO )
   {
      _pReleaseFunc = NULL ;
   }

   _sptProperty::_sptProperty( const _sptProperty &other )
   {
      _name = other._name ;
      _type = other._type ;
      _value = 0 ;
      if ( String == other._type )
      {
         UINT32 size = ossStrlen( ( const CHAR * )other._value ) ;
         CHAR *p = ( CHAR * )SDB_OSS_MALLOC( size + 1 );
         if ( NULL != p )
         {
            ossMemcpy( p, ( const CHAR * )( other._value ), size + 1 ) ;
            _value = ( UINT64 )p ;
         }
      }
      else
      {
         _value = other._value ;
         _pReleaseFunc = other._pReleaseFunc ;
      }
   }

   _sptProperty::~_sptProperty()
   {
      _name.clear() ;

      if ( String == _type )
      {
         CHAR *p = ( CHAR * )_value ;
         SDB_OSS_FREE( p ) ;
         _value = 0 ;
      }

      _type = EOO ;
   }

   _sptProperty &_sptProperty::operator=( const _sptProperty &other )
   {
      _name = other._name ;
      _type = other._type ;
      _value = 0 ;
      if ( String == other._type )
      {
         UINT32 size = ossStrlen( ( const CHAR * )other._value ) ;
         CHAR *p = ( CHAR * )SDB_OSS_MALLOC( size + 1 );
         if ( NULL != p )
         {
            ossMemcpy( p, ( const CHAR * )( other._value ), size ) ;
            p[size] = '\0' ;
            _value = ( UINT64 )p ;
         }
      }
      else
      {
         _value = other._value ;
         _pReleaseFunc = other._pReleaseFunc ;
      }

      return *this ;
   }

   INT32 _sptProperty::assignNative( const CHAR *name,
                                     bson::BSONType type,
                                     const void *value )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NumberDouble == type ||
                  Bool == type ||
                  NumberInt == type, "invalid value type" ) ;
      SDB_ASSERT( NULL != value, "can not be NULL" ) ;
      SDB_ASSERT( EOO == _type, "can not be reassigned" ) ;

      _value = 0 ;

      if ( NumberDouble == type )
      {
         FLOAT64 *v = (FLOAT64 *)(&_value) ;
         *v = *((const FLOAT64 *)value) ;
      }
      else if ( Bool == type )
      {
         BOOLEAN *v = (BOOLEAN *)(&_value);
         *v = *((const BOOLEAN *)value) ;
      }
      else
      {
         INT32 *v = (INT32 *)(&_value) ;
         *v = *((const INT32 *)value) ;
      }

      _name.assign( name ) ;
      _type = type ;
      return rc ;
   }

   INT32 _sptProperty::assignString( const CHAR *name,
                                     const CHAR *value )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != name && NULL != value, "can not be null" ) ;
      SDB_ASSERT( EOO == _type, "can not be reassigned" ) ;

      _value = 0 ;
      UINT32 size = ossStrlen( value ) ;
      CHAR *p = ( CHAR * )SDB_OSS_MALLOC( size + 1 ) ; /// +1 for \0
      if ( NULL == p )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      ossMemcpy( p, value, size + 1 ) ;
      _value = (UINT64)p ;
      _name.assign( name ) ;
      _type = String ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptProperty::assignBsonobj( const CHAR *name,
                                      const bson::BSONObj &value )
   {
      INT32 rc = SDB_OK ;
      releaseObj() ;

      _sptBsonobj *bs = SDB_OSS_NEW _sptBsonobj( value ) ;
      if ( NULL == bs )
      {
         PD_LOG( PDERROR, "failed to allocate mem.") ;
         rc = SDB_OOM ;
         goto error ;
      }
      _pReleaseFunc = (SPT_RELEASE_OBJ_FUNC)_sptBsonobj::releaseInstance ;

      rc = assignUsrObject( name, bs ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptProperty::assignBsonArray( const CHAR *name,
                                        const std::vector < BSONObj > &vecObj )
   {
      INT32 rc = SDB_OK ;
      releaseObj() ;

      _sptBsonobjArray *bsonarray = SDB_OSS_NEW _sptBsonobjArray( vecObj ) ;
      if ( NULL == bsonarray )
      {
         PD_LOG( PDERROR, "failed to allocate mem for bsonarray") ;
         rc = SDB_OOM ;
         goto error ;
      }
      _pReleaseFunc = (SPT_RELEASE_OBJ_FUNC)_sptBsonobjArray::releaseInstance ;

      rc = assignUsrObject( name, bsonarray ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptProperty::assignUsrObject( const CHAR *name,
                                        void *value )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != name, "can no be null" ) ;
      SDB_ASSERT( EOO == _type, "can not be reassigned" ) ;

      _value = 0 ;
      _name.assign(name);
      _type = Object ;
      _value = ( UINT64 )value ;
      return rc ;
   }

   INT32 _sptProperty::getNative( bson::BSONType type,
                                  void *value ) const
   {
      SDB_ASSERT( NULL != value, "can not be null" ) ;
      SDB_ASSERT( NumberDouble == type ||
                  Bool == type ||
                  NumberInt == type, "invalid value type" ) ;

      if ( NumberDouble == type )
      {
         FLOAT64 *v = ( FLOAT64 * )value ;
         *v = *(( FLOAT64 *)( &_value )) ;
      }
      else if ( Bool == type )
      {
         BOOLEAN *v = ( BOOLEAN * )value ;
         *v = *(( BOOLEAN *)( &_value )) ;
      }
      else
      {
         INT32 *v = ( INT32 * )value ;
         *v = *(( INT32 *)( &_value )) ;
      }

      return SDB_OK ;
   }

   const CHAR *_sptProperty::getString() const
   {
      SDB_ASSERT( String == _type, "type must be string" ) ;
      return ( CHAR * )_value ;
   }

}

