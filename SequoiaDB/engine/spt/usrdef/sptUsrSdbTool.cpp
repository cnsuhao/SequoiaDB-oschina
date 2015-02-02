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

   Source File Name = sptUsrSdbTool.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          18/08/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptUsrSdbTool.hpp"
#include "ossUtil.hpp"
#include "utilStr.hpp"
#include "pmdOptions.h"
#include "../bson/bsonobj.h"

using namespace bson ;

namespace engine
{
   /*
      Function Define
   */
   JS_STATIC_FUNC_DEFINE(_sptUsrSdbTool, help)
   JS_STATIC_FUNC_DEFINE(_sptUsrSdbTool, listNodes)
   JS_STATIC_FUNC_DEFINE(_sptUsrSdbTool, getNodeConfig)

   /*
      Function Map
   */
   JS_BEGIN_MAPPING( _sptUsrSdbTool, "Sdbtool" )
      JS_ADD_STATIC_FUNC("help", help)
      JS_ADD_STATIC_FUNC("listNodes", listNodes)
      JS_ADD_STATIC_FUNC("getNodeConfig", getNodeConfig)
   JS_MAPPING_END()

   /*
      _sptUsrSdbTool Implement
   */
   _sptUsrSdbTool::_sptUsrSdbTool()
   {
   }

   _sptUsrSdbTool::~_sptUsrSdbTool()
   {
   }

   INT32 _sptUsrSdbTool::help( const _sptArguments & arg,
                               _sptReturnVal & rval,
                               BSONObj & detail )
   {
      stringstream ss ;
      ss << "SdbTool functions:" << endl
         << " SdbTool.listNodes( [option obj], [filter obj] )" << endl
         << " SdbTool.getNodeConfig( svcname )" << endl ;
      rval.setStringVal( "", ss.str().c_str() ) ;
      return SDB_OK ;
   }

   INT32 _sptUsrSdbTool::listNodes( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      BSONObj optObj ;
      BSONObj filterObj ;

      if ( arg.argc() > 0 )
      {
         rc = arg.getBsonobj( 0, optObj ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "option must be bson obj" ) ;
            goto error ;
         }
      }
      if ( arg.argc() > 1 )
      {
         rc = arg.getBsonobj( 1, filterObj ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "filter must be bson obj" ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSdbTool::getNodeConfig( const _sptArguments &arg,
                                        _sptReturnVal &rval,
                                        BSONObj &detail )
   {
      INT32 rc = SDB_OK ;

   done:
      return rc ;
   error:
      goto done ;
   }

}


