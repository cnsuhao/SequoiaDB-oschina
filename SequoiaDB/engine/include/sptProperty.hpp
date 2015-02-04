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

   Source File Name = sptProperty.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_PROPERTY_HPP_
#define SPT_PROPERTY_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "../bson/bson.hpp"
#include <vector>

namespace engine
{
   typedef void (*SPT_RELEASE_OBJ_FUNC)(void *instance) ;

   /*
      _sptProperty define
   */
   class _sptProperty : public SDBObject
   {
   public:
      _sptProperty() ;
      _sptProperty( const _sptProperty &other ) ;
      _sptProperty &operator=(const _sptProperty &other) ;
      virtual ~_sptProperty() ;

   public:
      INT32 assignNative( const CHAR *name,
                          bson::BSONType type,
                          const void *value ) ;

      INT32 assignString( const CHAR *name,
                          const CHAR *value ) ;

      INT32 assignBsonobj( const CHAR *name,
                           const bson::BSONObj &value ) ;

      INT32 assignBsonArray( const CHAR *name,
                             const std::vector< bson::BSONObj > &vecObj ) ;

      INT32 assignUsrObject( const CHAR *name,
                             void *value ) ;

      INT32 getNative( bson::BSONType type,
                       void *value ) const ;

      const CHAR *getString() const ;

      inline bson::BSONType getType() const
      {
         return _type ;
      }

      inline void *getValue() const
      {
         return ( void * )_value ;
      }

      inline const std::string &getName() const
      {
         return _name ;
      }

      inline void releaseObj()
      {
         if ( bson::Object == _type && 0 != _value && _pReleaseFunc )
         {
            _pReleaseFunc( (void*)_value ) ;
            _value = 0 ;
            _pReleaseFunc = NULL ;
            _type = bson::EOO ;
         }
      }

   private:
      std::string _name ;
      UINT64 _value ;
      bson::BSONType _type ;
      SPT_RELEASE_OBJ_FUNC _pReleaseFunc ;
      
   } ;
   typedef class _sptProperty sptProperty ;
}

#endif // SPT_PROPERTY_HPP_

