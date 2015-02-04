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

   Source File Name = sptReturnVal.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_RETURNVAL_HPP_
#define SPT_RETURNVAL_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "sptProperty.hpp"
#include <vector>

namespace engine
{
   typedef std::vector<sptProperty> SPT_PROPERTIES ;

   class _sptReturnVal : public SDBObject
   {
   public:
      _sptReturnVal()
      : _classDef(NULL)
      {}

      virtual ~_sptReturnVal()
      {
         _classDef = NULL ;
      }

      INT32 setNativeVal( const CHAR *name,
                          bson::BSONType type,
                          const void *value ) ;

      INT32 setStringVal( const CHAR *name,
                          const CHAR *value ) ;

      INT32 setUsrObjectVal( const CHAR *name,
                             void *value,
                             const void *classDef ) ;

      INT32 setBSONObj( const CHAR *name,
                        const bson::BSONObj &obj ) ;

      INT32 setBSONArray( const CHAR *name,
                          const std::vector< bson::BSONObj > &vecObj ) ;

      const sptProperty &getVal() const
      {
         return _property ;
      }

      const void *getClassDef()const
      {
         return _classDef ;
      }

      void addReturnValProperty( const sptProperty &property )
      {
         _properties.push_back( property ) ;
      }
    
      const SPT_PROPERTIES &getValProperties()const
      {
         return _properties ;
      }

      void releaseObj()
      {
         _property.releaseObj() ;
      }

   private:
      sptProperty _property ;
      const void *_classDef ;

      SPT_PROPERTIES _properties ;
   } ;

   typedef class _sptReturnVal sptReturnVal ;
}

#endif

