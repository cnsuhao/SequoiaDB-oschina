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

   Source File Name = sptUsrCmd.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptUsrCmd.hpp"
#include "ossCmdRunner.hpp"
#include "ossMem.hpp"
#include "ossUtil.hpp"
#include "utilStr.hpp"

using namespace bson ;

namespace engine
{
   JS_MEMBER_FUNC_DEFINE( _sptUsrCmd, exec )
   JS_STATIC_FUNC_DEFINE( _sptUsrCmd, help )
   JS_CONSTRUCT_FUNC_DEFINE( _sptUsrCmd, construct )
   JS_DESTRUCT_FUNC_DEFINE( _sptUsrCmd, destruct )
   JS_MEMBER_FUNC_DEFINE( _sptUsrCmd, toString )
   JS_MEMBER_FUNC_DEFINE( _sptUsrCmd, getLastRet )
   JS_MEMBER_FUNC_DEFINE( _sptUsrCmd, getLastOut )
   JS_MEMBER_FUNC_DEFINE( _sptUsrCmd, start )

   JS_BEGIN_MAPPING( _sptUsrCmd, "Cmd" )
      JS_ADD_STATIC_FUNC( "help", help )
      JS_ADD_CONSTRUCT_FUNC( construct )
      JS_ADD_DESTRUCT_FUNC( destruct )
      JS_ADD_MEMBER_FUNC( "toString", toString )
      JS_ADD_MEMBER_FUNC( "getLastRet", getLastRet )
      JS_ADD_MEMBER_FUNC( "getLastOut", getLastOut )
      JS_ADD_MEMBER_FUNC( "run", exec )
      JS_ADD_MEMBER_FUNC( "start", start )
   JS_MAPPING_END()

   _sptUsrCmd::_sptUsrCmd()
   {
      _retCode    = 0 ;
   }

   _sptUsrCmd::~_sptUsrCmd()
   {
   }

   INT32 _sptUsrCmd::construct( const _sptArguments &arg,
                                _sptReturnVal &rval,
                                bson::BSONObj &detail )
   {
      return SDB_OK ;
   }

   INT32 _sptUsrCmd::destruct()
   {
      return SDB_OK ;
   }

   INT32 _sptUsrCmd::toString( const _sptArguments & arg,
                               _sptReturnVal & rval,
                               BSONObj & detail )
   {
      rval.setStringVal( "", "CommandRunner" ) ;
      return SDB_OK ;
   }

   INT32 _sptUsrCmd::getLastRet( const _sptArguments & arg,
                                 _sptReturnVal & rval,
                                 BSONObj & detail )
   {
      rval.setNativeVal( "", NumberInt, (const void*)&_retCode ) ;
      return SDB_OK ;
   }

   INT32 _sptUsrCmd::getLastOut( const _sptArguments & arg,
                                 _sptReturnVal & rval,
                                 BSONObj & detail )
   {
      rval.setStringVal( "", _strOut.c_str() ) ;
      return SDB_OK ;
   }

   INT32 _sptUsrCmd::exec( const _sptArguments &arg,
                           _sptReturnVal &rval,
                           bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      string cmd ;
      string ev ;
      UINT32 timeout = 0 ;
      UINT32 useShell = TRUE ;
      ossCmdRunner runner ;

      rc = arg.getString( 0, cmd ) ;
      if ( SDB_OK != rc )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "cmd must be config" ) ;
         goto error ;
      }
      utilStrTrim( cmd ) ;

      rc = arg.getString( 1, ev ) ;
      if ( SDB_OK != rc && SDB_OUT_OF_BOUND != rc )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "environment should be a string" ) ;
         goto error ;
      }
      else if ( SDB_OK == rc && !ev.empty() )
      {
         cmd += " " ;
         cmd += ev ;
      }

      rc = arg.getNative( 2, (void*)&timeout, SPT_NATIVE_INT32 ) ;
      if ( SDB_OK != rc && SDB_OUT_OF_BOUND != rc )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "timeout should be a number" ) ;
         goto error ;
      }
      rc = SDB_OK ;

      rc = arg.getNative( 3, (void*)&useShell, SPT_NATIVE_INT32 ) ;
      if ( SDB_OK != rc && SDB_OUT_OF_BOUND != rc )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "useShell should be a number" ) ;
         goto error ;
      }
      rc = SDB_OK ;

      _strOut = "" ;
      _retCode = 0 ;
      rc = runner.exec( cmd.c_str(), _retCode, FALSE,
                        0 == timeout ? -1 : (INT64)timeout,
                        FALSE, NULL, useShell ? TRUE : FALSE ) ;
      if ( SDB_OK != rc )
      {
         stringstream ss ;
         ss << "run[" << cmd << "] failed" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }
      else
      {
         rc = runner.read( _strOut ) ;
         if ( rc )
         {
            stringstream ss ;
            ss << "read run command[" << cmd << "] result failed" ;
            detail = BSON( SPT_ERR << ss.str() ) ;
            goto error ;
         }
         else if ( SDB_OK != _retCode )
         {
            detail = BSON( SPT_ERR << _strOut ) ;
            rc = _retCode ;
            goto error ;
         }

         rval.setStringVal( "", _strOut.c_str() ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrCmd::start( const _sptArguments & arg,
                            _sptReturnVal & rval,
                            BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string cmd ;
      string ev ;
      ossCmdRunner runner ;
      UINT32 useShell = TRUE ;

      rc = arg.getString( 0, cmd ) ;
      if ( SDB_OK != rc )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "cmd must be config" ) ;
         goto error ;
      }
      utilStrTrim( cmd ) ;

      rc = arg.getString( 1, ev ) ;
      if ( SDB_OK != rc && SDB_OUT_OF_BOUND != rc )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "environment should be a string" ) ;
         goto error ;
      }
      else if ( SDB_OK == rc )
      {
         cmd += " " ;
         cmd += ev ;
      }

      rc = arg.getNative( 2, (void*)&useShell, SPT_NATIVE_INT32 ) ;
      if ( SDB_OK != rc && SDB_OUT_OF_BOUND != rc )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "useShell should be a number" ) ;
         goto error ;
      }
      rc = SDB_OK ;

      _strOut = "" ;
      _retCode = 0 ;
      rc = runner.exec( cmd.c_str(), _retCode, TRUE, -1, FALSE, NULL,
                        useShell ? TRUE : FALSE ) ;
      if ( SDB_OK != rc )
      {
         stringstream ss ;
         ss << "run[" << cmd << "] failed" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }
      else
      {
         OSSPID pid = runner.getPID() ;
         rval.setNativeVal( "", NumberInt, (const void*)&pid ) ;

         ossSleep( 100 ) ;
         if ( !ossIsProcessRunning( pid ) )
         {
            rc = runner.read( _strOut ) ;
            if ( rc )
            {
               stringstream ss ;
               ss << "read run command[" << cmd << "] result failed" ;
               detail = BSON( SPT_ERR << ss.str() ) ;
               goto error ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrCmd::help( const _sptArguments & arg,
                           _sptReturnVal & rval,
                           BSONObj & detail )
   {
      stringstream ss ;
      ss << "Cmd functions:" << endl
         << " var cmd = new Cmd()" << endl
         << "   run( cmd, [args], [timeout], [useShell] )  timeout(ms), default 0: never timeout" << endl
         << "        useShell 0/1, default 1" << endl
         << "   start( cmd, [args], [useShell] )  useShell 0/1, default 1" << endl
         << "   getLastRet()" << endl
         << "   getLastOut()" << endl ;
      rval.setStringVal( "", ss.str().c_str() ) ;
      return SDB_OK ;
   }

   INT32 _sptUsrCmd::_setRVal( _ossCmdRunner *runner,
                               _sptReturnVal &rval,
                               BOOLEAN setToRVal,
                               BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      string outStr ;

      rc = runner->read( outStr ) ;
      if ( rc )
      {
         detail = BSON( SPT_ERR << "read run result failed" ) ;
         goto error ;
      }

      if ( setToRVal )
      {
         rval.setStringVal( "", outStr.c_str() ) ;
      }
      else
      {
         detail = BSON( SPT_ERR << outStr ) ;
      }

   done:
      return rc  ;
   error:
      goto done ;
   }
}

