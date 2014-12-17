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

   Source File Name = sptGlobalFunc.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptGlobalFunc.hpp"
#include "ossUtil.hpp"
#include "sptCommon.hpp"

using namespace bson ;

namespace engine
{
JS_GLOBAL_FUNC_DEFINE( _sptGlobalFunc, getLastErrorMsg )
JS_GLOBAL_FUNC_DEFINE( _sptGlobalFunc, setLastErrorMsg )
JS_GLOBAL_FUNC_DEFINE( _sptGlobalFunc, getLastError )
JS_GLOBAL_FUNC_DEFINE( _sptGlobalFunc, setLastError )
JS_GLOBAL_FUNC_DEFINE( _sptGlobalFunc, sleep )

JS_BEGIN_MAPPING( _sptGlobalFunc, "" )
   JS_ADD_GLOBAL_FUNC( "getLastErrMsg", getLastErrorMsg )
   JS_ADD_GLOBAL_FUNC( "setLastErrMsg", setLastErrorMsg )
   JS_ADD_GLOBAL_FUNC( "getLastError", getLastError )
   JS_ADD_GLOBAL_FUNC( "setLastError", setLastError )
   JS_ADD_GLOBAL_FUNC( "sleep", sleep )
JS_MAPPING_END()

   INT32 _sptGlobalFunc::getLastErrorMsg( const _sptArguments &arg,
                                          _sptReturnVal &rval,
                                          bson::BSONObj &detail )
   {
      sdbSetNeedClearErrorInfo( FALSE ) ;
      if ( NULL != sdbGetErrMsg() )
      {
         rval.setStringVal( "", sdbGetErrMsg() ) ;
         sdbSetErrMsg( NULL ) ;
      }
      return SDB_OK ;
   }

   INT32 _sptGlobalFunc::setLastErrorMsg( const _sptArguments & arg,
                                          _sptReturnVal & rval,
                                          BSONObj & detail )
   {
      string msg ;
      sdbSetNeedClearErrorInfo( FALSE ) ;
      if ( SDB_OK == arg.getString( 0, msg ) )
      {
         sdbSetErrMsg( msg.c_str() ) ;
      }
      return SDB_OK ;
   }

   INT32 _sptGlobalFunc::getLastError( const _sptArguments & arg,
                                       _sptReturnVal & rval,
                                       BSONObj & detail )
   {
      sdbSetNeedClearErrorInfo( FALSE ) ;
      INT32 error = sdbGetErrno() ;
      rval.setNativeVal( "",  NumberInt, (const void*)&error ) ;
      sdbSetErrno( SDB_OK ) ;
      return SDB_OK ;
   }

   INT32 _sptGlobalFunc::setLastError( const _sptArguments & arg,
                                       _sptReturnVal & rval,
                                       BSONObj & detail )
   {
      INT32 errNum = SDB_OK ;
      sdbSetNeedClearErrorInfo( FALSE ) ;
      if( SDB_OK == arg.getNative( 0, (void*)&errNum, SPT_NATIVE_INT32 ) )
      {
         sdbSetErrno( errNum ) ;
      }
      return SDB_OK ;
   }

   INT32 _sptGlobalFunc::sleep( const _sptArguments &arg,
                                _sptReturnVal &rval,
                                bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 time = 0 ;
      rc = arg.getNative( 0, &time, SPT_NATIVE_INT32 ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      ossSleepmillis( time ) ;
   done:
      return SDB_OK ;
   error:
      goto done ;
   }
}

