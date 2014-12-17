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

   Source File Name = rtnQueryOptions.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   user command processing on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          27/05/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnQueryOptions.hpp"
#include "ossUtil.hpp"

namespace engine
{
   _rtnQueryOptions::~_rtnQueryOptions()
   {
      if ( NULL != _fullNameBuf )
      {
         SDB_OSS_FREE( _fullNameBuf ) ;
      }

      _fullName = NULL ;
      _fullNameBuf = NULL ;
   }

   INT32 _rtnQueryOptions::getOwned()
   {
      INT32 rc = SDB_OK ;
      if ( NULL != _fullNameBuf )
      {
         SDB_OSS_FREE( _fullNameBuf ) ;
         _fullNameBuf = NULL ;
      }

      if ( NULL != _fullName )
      {
         _fullNameBuf = ossStrdup( _fullName ) ;
         if ( NULL == _fullNameBuf )
         {
            rc = SDB_OOM ;
            goto error ;
         }
      }

      _fullName = _fullNameBuf ;
      _query = _query.getOwned() ;
      _selector = _selector.getOwned() ;
      _orderBy = _orderBy.getOwned() ;
      _hint = _hint.getOwned() ;
   done:
      return rc ;
   error:
      goto done ;
   }
}
