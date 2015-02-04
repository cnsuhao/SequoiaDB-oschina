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

   Source File Name = sptReturnVal.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptReturnVal.hpp"
#include "sptBsonobj.hpp"
#include "sptBsonobjArray.hpp"
#include "ossUtil.hpp"
#include "pd.hpp"

using namespace bson ;

namespace engine
{
   INT32 _sptReturnVal::setNativeVal( const CHAR *name,
                                      bson::BSONType type,
                                      const void *value )
   {
      INT32 rc = SDB_OK ;
      rc = _property.assignNative( name, type, value ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to assign property:%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptReturnVal::setUsrObjectVal( const CHAR *name,
                                         void *value,
                                         const void *classDef )
   {
      SDB_ASSERT( NULL != classDef, "class def can not be NULL" ) ;
      INT32 rc = SDB_OK ;
      rc = _property.assignUsrObject( name, value ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      _classDef = classDef ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptReturnVal::setStringVal( const CHAR *name,
                                      const CHAR *value )
   {
      return _property.assignString( name, value ) ;
   }

   INT32 _sptReturnVal::setBSONObj( const CHAR *name,
                                    const bson::BSONObj &obj )
   {
      INT32 rc = SDB_OK ;
      rc = _property.assignBsonobj( name, obj ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      _classDef = _sptBsonobj::__desc.getClassDef() ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptReturnVal::setBSONArray( const CHAR *name,
                                      const std::vector< BSONObj > &vecObj )
   {
      INT32 rc = SDB_OK ;
      rc = _property.assignBsonArray( name, vecObj ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      _classDef = _sptBsonobjArray::__desc.getClassDef() ;
   done:
      return rc ;
   error:
      goto done ;
   }

}

