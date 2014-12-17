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

   Source File Name = sptUtil.cpp

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

#include "sptUtil.hpp"
#include "sptConvertor.hpp"


namespace engine
{
   INT32 utilGetBsonRawFromCtx( JSContext *cx, JSObject *obj, CHAR **raw )
   {
      INT32 rc = SDB_OK ;
      sptConvertor convertor( cx ) ;
      bson *bs = NULL ;
      rc = convertor.toBson( obj, &bs ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      bs->ownmem = FALSE ;
      *raw = bs->data ;
   done:
      if ( NULL != bs )
      {
         bson_dispose( bs ) ;
      }
      return rc ;
   error:
      goto done ;
   }   
}
