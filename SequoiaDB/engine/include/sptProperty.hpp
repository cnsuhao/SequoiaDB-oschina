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

namespace engine
{
   class _sptProperty : public SDBObject
   {
   public:
      _sptProperty() ;
      _sptProperty( const _sptProperty &other ) ;
      _sptProperty &operator=(const _sptProperty &other) ;
      virtual ~_sptProperty() ;

   public:
      /// BOOLEAN, INT32, FLOAT64
      INT32 assignNative( const CHAR *name,
                          bson::BSONType type,
                          const void *value ) ;

      /// value should be base64 coded when
      /// it is a binary data.
      INT32 assignString( const CHAR *name,
                          const CHAR *value ) ;

      INT32 assignBsonobj( const CHAR *name,
                           const bson::BSONObj &value ) ;

      /// WARNING: value will be registered in
      /// engine and released in JS_Destructor.
      INT32 assignUsrObject( const CHAR *name,
                             void *value ) ;

      INT32 getNative( bson::BSONType type,
                       void *value ) const ;

      /// copy value if u want to modify or keep it.
      const CHAR *getString() const ;

      /// copy value if u want to modify or keep it.
      INT32 getBsonobj( bson::BSONObj &value ) const ;

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

   private:
      std::string _name ;
      UINT64 _value ;
      bson::BSONType _type ;
      
   } ;
   typedef class _sptProperty sptProperty ;
}

#endif

