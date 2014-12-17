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

   Source File Name = sptScope.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptScope.hpp"
#include "pd.hpp"
#include "sptObjDesc.hpp"
#include "ossUtil.hpp"
#include "sptCommon.hpp"

namespace engine
{
   _sptScope::OBJ_DESCS _sptScope::_descs ;

   _sptScope::_sptScope()
   {

   }

   _sptScope::~_sptScope()
   {

   }

   INT32 _sptScope::getLastError()
   {
      return sdbGetErrno() ;
   }

   const CHAR* _sptScope::getLastErrMsg()
   {
      return sdbGetErrMsg() ;
   }

   INT32 _sptScope::loadUsrDefObj( _sptObjDesc *desc )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != desc, "desc can not be NULL" ) ;
      SDB_ASSERT( NULL != desc->getJSClassName(),
                  "obj name can not be empty" ) ;
      rc = _loadUsrDefObj( desc ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to load object defined by user:%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }
}
