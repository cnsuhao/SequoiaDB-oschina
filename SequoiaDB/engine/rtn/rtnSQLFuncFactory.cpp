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

   Source File Name = rtnSQLFuncFactory.cpp

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

#include "rtnSQLFuncFactory.hpp"
#include "pd.hpp"
#include "rtnSQLFunc.hpp"
#include "ossUtil.hpp"
#include "rtnSQLCount.hpp"
#include "rtnSQLSum.hpp"
#include "rtnSQLMin.hpp"
#include "rtnSQLMax.hpp"
#include "rtnSQLAvg.hpp"
#include "rtnSQLFirst.hpp"
#include "utilStr.hpp"
#include "rtnSQLLast.hpp"
#include "rtnSQLPush.hpp"
#include "rtnSQLAddToSet.hpp"
#include "rtnSQLBuildObj.hpp"
#include "rtnSQLMergeArraySet.hpp"

namespace engine
{
   INT32  _rtnSQLFuncFactory::create( const CHAR *funcName,
                                      UINT32 paramNum,
                                       _rtnSQLFunc *&func )
   {
      INT32 rc = SDB_OK ;
      CHAR *name = NULL ;
      rc = utilStrToUpper( funcName, name ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      if ( 0 == ossStrcmp("COUNT", name) && 1 == paramNum )
      {
         func = SDB_OSS_NEW _rtnSQLCount() ;
      }
      else if ( 0 == ossStrcmp( "SUM", name ) && 1 == paramNum )
      {
         func = SDB_OSS_NEW _rtnSQLSum() ;
      }
      else if ( 0 == ossStrcmp( "MIN", name ) && 1 == paramNum )
      {
         func = SDB_OSS_NEW _rtnSQLMin() ;
      }
      else if ( 0 == ossStrcmp( "MAX", name) && 1 == paramNum )
      {
         func = SDB_OSS_NEW _rtnSQLMax() ;
      }
      else if ( 0 == ossStrcmp( "AVG", name) && 1 == paramNum )
      {
         func = SDB_OSS_NEW _rtnSQLAvg() ;
      }
      else if ( 0 == ossStrcmp( "FIRST", name ) && 1 == paramNum )
      {
         func = SDB_OSS_NEW _rtnSQLFirst() ;
      }
      else if ( 0 == ossStrcmp( "LAST", name ) && 1 == paramNum )
      {
         func = SDB_OSS_NEW rtnSQLLast() ;
      }
      else if ( 0 == ossStrcmp( "PUSH", name ) && 1 == paramNum )
      {
         func = SDB_OSS_NEW rtnSQLPush() ;
      }
      else if ( 0 == ossStrcmp( "ADDTOSET", name ) && 1 == paramNum )
      {
         func = SDB_OSS_NEW rtnSQLAddToSet() ;
      }
      else if ( 0 == ossStrcmp( "BUILDOBJ", name ) && paramNum > 0 )
      {
         func = SDB_OSS_NEW rtnSQLBuildObj() ;
      }
      else if ( 0 == ossStrcmp( "MERGEARRAYSET", name ) && 1 == paramNum )
      {
         func = SDB_OSS_NEW rtnSQLMergeArraySet() ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( NULL == func )
      {
         PD_LOG( PDERROR, "failed to allocated mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

   done:
      if ( NULL != name )
      {
         SDB_OSS_FREE( name ) ;
         name = NULL ;
      }
      return rc ;
   error:
      goto done ;
   }
}
