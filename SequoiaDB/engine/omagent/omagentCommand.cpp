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

   Source File Name = omagentCommand.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omagentCommand.hpp"
#include "omagentUtil.hpp"
#include "omagentHelper.hpp"
#include "ossProc.hpp"
#include "utilPath.hpp"
#include "ossPath.h"
#include "omagentJob.hpp"
#include "omagentMgr.hpp"

using namespace bson ;

namespace engine
{
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaScanHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaBasicCheckHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaInstallRemoteAgent )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaCheckHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaUninstallRemoteAgent )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaAddHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaRemoveHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaInsDBBus )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaUninsDBBus )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaQueryTaskProgress )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaUpdateHostsInfo )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaQueryHostStatus )


   /*
      _omaCommand
   */
   _omaCommand::_omaCommand ()
   {
      _scope      = NULL ;
      _fileBuff   = NULL ;
      _buffSize   = 0 ;
      _readSize   = 0 ;
      ossMemset( _jsFileName, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _jsFileArgs, 0, JS_ARG_LEN + 1 ) ;
      prime() ;
   }

   _omaCommand::~_omaCommand ()
   {
      if ( _scope )
      {
         sdbGetOMAgentMgr()->releaseScope( _scope ) ;
         _scope = NULL ;
      }
      if ( _fileBuff )
      {
         SAFE_OSS_FREE ( _fileBuff ) ;
      }
   }

   INT32 _omaCommand::setJsFile( const CHAR *fileName )
   {
      INT32 rc = SDB_OK ;
      const CHAR *tmp = NULL ;
      if ( NULL == fileName )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Invalid js file name" ) ;
         goto error ;
      }
      tmp = sdbGetOMAgentOptions()->getScriptPath() ;
      ossStrncpy ( _jsFileName, tmp, OSS_MAX_PATHSIZE ) ;
      rc = utilCatPath ( _jsFileName, OSS_MAX_PATHSIZE, fileName ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to build js file full path, rc = %d",
                      rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaCommand::addJsFile( const CHAR *fileName,
                                 const CHAR *bus,
                                 const CHAR *sys,
                                 const CHAR *env,
                                 const CHAR *other )
   {
      INT32 rc = SDB_OK ;
      string name( fileName ) ;
      string para ;
      vector< pair<string, string> >::iterator it = _jsFiles.begin() ;

      for ( ; it != _jsFiles.end(); it++ )
      {
         if ( it->first == name )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG ( PDWARNING, "Js file[%s] already exit", fileName ) ;
            goto error ;
         }
      }
      if ( bus ) para += bus ;
      if ( sys ) para += sys ;
      if ( env ) para += env ;
      if ( other ) para += other ; 
      _jsFiles.push_back( pair<string, string>( name, para ) ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaCommand::getExcuteJsContent( string &content )
   {
      INT32 rc = SDB_OK ;
      vector< pair<string, string> >::iterator it = _jsFiles.begin() ;

      if ( it == _jsFiles.end() )
      {
         goto done ;
      }
      content.clear() ;
      for ( ; it != _jsFiles.end(); it++ )
      {
         rc = setJsFile( it->first.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to set js file[%s], rc = %d", 
                         it->first.c_str(), rc ) ;
            goto error ;
         }
         rc = readFile ( _jsFileName, &_fileBuff,
                         &_buffSize, &_readSize ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to read js file[%s], rc = %d",
                         _jsFileName, rc ) ;
            goto error ;
         }
         content += it->second ;
         content += OSS_NEWLINE ;  
         content += _fileBuff ;
         content += OSS_NEWLINE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaCommand::prime()
   {
      addJsFile ( FILE_DEFINE ) ;
      addJsFile ( FILE_ERROR ) ;
      addJsFile ( FILE_COMMON ) ;
      addJsFile ( FILE_LOG ) ;
      addJsFile ( FILE_FUNC ) ;
      return SDB_OK ;
   }

   INT32 _omaCommand::init ( const CHAR *pIndtallInfo )
   {
      INT32 rc = SDB_OK ;

      return rc ;
   }

   INT32 _omaCommand::doit ( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      BSONObj detail ;
      BSONObj rval ;

      rc = getExcuteJsContent( _content ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to get js file's content to "
                      "excute, rc = %d", rc ) ;
         goto error ;
      }
      _scope = sdbGetOMAgentMgr()->getScope() ;
      if ( !_scope )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG ( PDERROR, "Failed to get scope, rc = %d", rc ) ;
         goto error ;
      }
      rc = _scope->eval( _content.c_str(), _content.size(),
                         "", 1, SPT_EVAL_FLAG_NONE, rval, detail ) ;
      if ( rc )
      {
         string errmsg = _scope->getLastErrMsg() ;
         PD_LOG_MSG ( PDERROR, "%s", errmsg.c_str() ) ;
         PD_LOG ( PDDEBUG, "Failed to eval js file for command[%s]: "
                  "%s, rc = %d", name(), errmsg.c_str(), rc ) ;
         rc = _scope->getLastError() ;
         BSONObjBuilder bob ;
         bob.append( OMA_FIELD_DETAIL, errmsg.c_str() ) ;
         retObj = bob.obj() ;
         goto error ;
      }
      rc = final ( rval, retObj ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to extract result for command[%s], "
                  "rc = %d", name(), rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaCommand::final ( BSONObj &rval, BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;
      BSONObj subObj ;

      PD_LOG ( PDDEBUG, "Js return raw result for command[%s]: %s",
               name(), rval.toString(FALSE, TRUE).c_str() ) ;
      rc = omaGetObjElement( rval, "", subObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get the nameless field from the js"
                  "return object, rc: %d", rc ) ;
         goto error ;
      }
      bob.appendElements( subObj ) ;
      retObj = bob.obj() ;
   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _omaCmdAssit
   */
   _omaCmdAssit::_omaCmdAssit ( OA_NEW_FUNC pFunc )
   {
      if ( pFunc )
      {
         _omaCommand *pCommand = (*pFunc)() ;
         if ( pCommand )
         {
            getOmaCmdBuilder()->_register ( pCommand->name(), pFunc ) ;
            SDB_OSS_DEL pCommand ;
            pCommand = NULL ;
         }
      }
   }

   _omaCmdAssit::~_omaCmdAssit ()
   {
   }

   /*
      _omaCmdBuilder
   */
   _omaCmdBuilder::_omaCmdBuilder ()
   {
   }

   _omaCmdBuilder::~_omaCmdBuilder ()
   {
   }

   _omaCommand* _omaCmdBuilder::create ( const CHAR *command )
   {
      OA_NEW_FUNC pFunc = _find ( command ) ;
      if ( pFunc )
      {
         return (*pFunc)() ;
      }
      return NULL ;
   }

   void _omaCmdBuilder::release ( _omaCommand *&pCommand )
   {
      if ( pCommand )
      {
         SDB_OSS_DEL pCommand ;
         pCommand = NULL ;
      }
   }

   INT32 _omaCmdBuilder::_register ( const CHAR *name, OA_NEW_FUNC pFunc )
   {
      INT32 rc = SDB_OK ;

      pair< MAP_OACMD_IT, BOOLEAN > ret ;
      ret = _cmdMap.insert( pair<const CHAR*, OA_NEW_FUNC>(name, pFunc) ) ;
      if ( FALSE == ret.second )
      {
         PD_LOG_MSG ( PDERROR, "Failed to register omagent command[%s], "
                      "already exist", name ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   OA_NEW_FUNC _omaCmdBuilder::_find ( const CHAR *name )
   {
      if ( name )
      {
         MAP_OACMD_IT it = _cmdMap.find( name ) ;
         if ( it != _cmdMap.end() )
         {
            return it->second ;
         }
      }
      return NULL ;
   }

   /*
      get omagent command builder
   */
   _omaCmdBuilder* getOmaCmdBuilder()
   {
      static _omaCmdBuilder cmdBuilder ;
      return &cmdBuilder ;
   }

   /******************************* scan host ********************************/
   /*
      _omaScanHost
   */
   _omaScanHost::_omaScanHost()
   {
   }

   _omaScanHost::~_omaScanHost()
   {
   }

   INT32 _omaScanHost::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj obj( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; "
                      "var %s = %s; var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, obj.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, "{}",
                      JS_ARG_ENV, "{}",
                      JS_ARG_OTHER, "{}" ) ;
         PD_LOG ( PDDEBUG, "Scan host passes argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_SCAN_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_SCAN_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   /******************************* basic check *********************/
   _omaBasicCheckHost::_omaBasicCheckHost ()
   {
   }

   _omaBasicCheckHost::~_omaBasicCheckHost ()
   {
   }

   INT32 _omaBasicCheckHost::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj obj( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; "
                      "var %s = %s; var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, obj.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, "{}",
                      JS_ARG_ENV, "{}",
                      JS_ARG_OTHER, "{}" ) ;
         PD_LOG ( PDDEBUG, "Basic check passes argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_BASIC_CHECK_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_BASIC_CHECK_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
     goto done ;
   }

   /******************************* install remote agent *********************/
   /*
      _omaInstallRemoteAgent
   */
   _omaInstallRemoteAgent::_omaInstallRemoteAgent ()
   {
   }

   _omaInstallRemoteAgent::~_omaInstallRemoteAgent ()
   {
   }

   INT32 _omaInstallRemoteAgent::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      CHAR prog_path[ OSS_MAX_PATHSIZE + 1 ] = { 0 };

      try
      {
         BSONObj bus( pInstallInfo ) ;
         BSONObj sys ;
         rc = _getProgPath ( prog_path, OSS_MAX_PATHSIZE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get sdbcm program path, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         sys = BSON( OMA_FIELD_PROG_PATH << prog_path ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; "
                      "var %s = %s; var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str(),
                      JS_ARG_ENV, "{}",
                      JS_ARG_OTHER, "{}" ) ;
         PD_LOG ( PDDEBUG, "Install remote agent passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_INSTALL_REMOTE_AGENT, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_INSTALL_REMOTE_AGENT, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstallRemoteAgent::_getProgPath ( CHAR *path, INT32 len )
   {
      INT32 rc = SDB_OK ;
      std::string str ;
      std::string key ;
      UINT32 found = 0 ;
      CHAR tmp[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;

      rc = ossGetEWD ( tmp, OSS_MAX_PATHSIZE ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR,
                      "Failed to get program's work directory, rc = %d", rc ) ;
         goto error ;
      }
      rc = utilCatPath ( tmp, OSS_MAX_PATHSIZE, SDBSDBCMPROG ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR,
                      "Failed to build program's full directory, rc = %d",
                      rc ) ;
         goto error ;
      }
      str = tmp ;
      key = SDBSDBCMPROG ;
      found = str.rfind( key ) ;
      if ( found != std::string::npos )
      {
         str.replace( found, key.length(), "\0" ) ;
         ossStrncpy( path, str.c_str(), len ) ;
      }
      else
      {
         rc = SDB_SYS ;
         PD_LOG_MSG ( PDERROR, "Failed to set program's path" ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /******************************* check host ********************************/
   /*
      _omaCheckHost
   */
   _omaCheckHost::_omaCheckHost ()
   {
   }

   _omaCheckHost::~_omaCheckHost ()
   {
   }

   INT32 _omaCheckHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;

      try
      {
         BSONObj bus( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Check host info passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_CHECK_HOST_ITEM ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_CHECK_HOST_ITEM, rc ) ;
            goto error ;
         }
         rc = addJsFile( FILE_CHECK_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_CHECK_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   /******************************* uninstall remote agent *******************/
   /*
      _omaUninstallRemoteAgent
   */
   _omaUninstallRemoteAgent::_omaUninstallRemoteAgent ()
   {
   }

   _omaUninstallRemoteAgent::~_omaUninstallRemoteAgent ()
   {
   }

   INT32 _omaUninstallRemoteAgent::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Uninstall remote sdbcm passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_UNINSTALL_REMOTE_AGENT, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_UNINSTALL_REMOTE_AGENT, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   /******************************* add host ********************************/
   /*
      _omaAddHost
   */
   _omaAddHost::_omaAddHost ()
   {
   }

   _omaAddHost::~_omaAddHost ()
   {
      _transactionID = -1 ;
   }

   INT32 _omaAddHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;
         _addHostInfo = bus.getOwned() ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Add hosts passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_ADD_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_ADD_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   INT32 _omaAddHost::final ( BSONObj &rval, BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      INT32 retErrno = SDB_OK ;
      BSONObjBuilder bob ;
      BSONObj result ;
      BSONObj addHostResult ;
      BSONObj rollbackInfo ;
      BSONObj rollbackResult ;
      CHAR detail[OMA_BUFF_SIZE + 1] = { 0 } ;

      PD_LOG ( PDDEBUG, "Add hosts return raw result: %s",
               rval.toString(FALSE, TRUE).c_str() ) ;
      rc = omaGetObjElement( rval, "", addHostResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Get field[%s] failed, rc: %d", "", rc ) ;
         goto error ;
      } 
      rc = omaGetIntElement ( addHostResult, OMA_FIELD_ERRNO, retErrno ) ; 
      if ( rc )
      {
         PD_LOG ( PDERROR, "Get field[%s] failed, rc: %d",
                  OMA_FIELD_ERRNO, rc ) ;
         goto error ;
      }
      if ( retErrno )
      {
         PD_LOG ( PDERROR, "Failed to add all the hosts, "
                  "going to rollback, rc = %d", retErrno ) ;
         rc = _getRollbackInfo( addHostResult , rollbackInfo ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get add hosts roolback info, "
                     "failed to rollback, rc = %d", rc ) ;
            goto error ;
         }
         rc = _addHostRollback( rollbackInfo, rollbackResult ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to roolback add hosts, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         rc = _buildErrDetail( addHostResult, rollbackResult,
                               detail, OMA_BUFF_SIZE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to build return detail, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         bob.append( OMA_FIELD_DETAIL, detail ) ;
         result = bob.obj() ;
         rc = retErrno ;
         goto done ;
      }

   done:
       _buildRetResult( result, retObj ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHost::_getRollbackInfo( BSONObj &addHostResult,
                                        BSONObj &rollbackInfo )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      set<string> ips ;

      try
      {
         ele = addHostResult.getField ( OMA_FIELD_HOSTINFO ) ;
         if ( Array != ele.type() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "Js return wrong format result" ) ;
            goto error ;
         }
         else
         {
            BSONArrayBuilder bab ;
            BSONObjIterator itr( ele.embeddedObject() ) ;
            while ( itr.more() )
            {
               BOOLEAN value = FALSE ;
               const CHAR *ip = NULL ;
               ele = itr.next() ;
               if ( Object != ele.type() )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG ( PDERROR, "Js return wrong format result" ) ;
                  goto error ;
               }
               BSONObj temp = ele.embeddedObject() ;
               rc = omaGetBooleanElement( temp, OMA_FIELD_HASINSTALL, value ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                           "rc = %d", OMA_FIELD_HASINSTALL, rc ) ;
                  goto error ;
               }
               if ( value )
               {
                  rc = omaGetStringElement( temp, OMA_FIELD_IP, &ip ) ;
                  if( rc )
                  {
                     PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                              "rc = %d", OMA_FIELD_IP, rc ) ;
                     goto error ;
                  }
                  ips.insert( string(ip) ) ;
               }
            }
         }
         ele = _addHostInfo.getField ( OMA_FIELD_HOSTINFO ) ;
         if ( Array != ele.type() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "Wrong format input info" ) ;
            goto error ;
         }
         else
         {
            BSONObjBuilder bob ;
            BSONArrayBuilder bab ;
            BSONObjIterator itr( ele.embeddedObject() ) ;
            while ( itr.more() )
            {
               const CHAR *ip = NULL ;
               set<string>::iterator it ;
               ele = itr.next() ;
               if ( Object != ele.type() )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG ( PDERROR, "Wrong format input info" ) ;
                  goto error ;
               }
               BSONObj temp = ele.embeddedObject() ;
               rc = omaGetStringElement( temp, OMA_FIELD_IP, &ip ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                           "rc = %d", OMA_FIELD_IP, rc ) ;
                  goto error ;
               }
               it = ips.find( string(ip) ) ;
               if ( it != ips.end() )
               {
                  bab.append( temp ) ;
               }
            }
            bob.appendArray( OMA_FIELD_HOSTINFO, bab.arr() ) ;
            rollbackInfo = bob.obj() ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaAddHost::_addHostRollback ( BSONObj &rollbackInfo,
                                         BSONObj &rollbackResult )
   {
      INT32 rc = SDB_OK ;

      _omaAddHostRollbackInternal rollbackInternal ;
      rc = rollbackInternal.init( rollbackInfo.objdata() ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init rollback in add hosts, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = rollbackInternal.doit( rollbackResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to do rollback in add hosts, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      PD_LOG ( PDEVENT, "Rollback add hosts result is: %s",
               rollbackResult.toString(FALSE, TRUE).c_str() ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHost::_buildErrDetail ( BSONObj &addHostResult,
                                        BSONObj &rollbackResult,
                                        CHAR *pBuf,
                                        INT32 bufSize )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      BSONArrayBuilder bab ;
      BSONArray arr ;
      const CHAR *pDetail = NULL ;
      string str = "" ;

      rc = omaGetStringElement( addHostResult, OMA_FIELD_DETAIL, &pDetail ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Get field[%s] failed, rc: %d",
                  OMA_FIELD_DETAIL, rc ) ;
         goto error ;
      }
      ele = rollbackResult.getField ( OMA_FIELD_HOSTINFO ) ;
      if ( Array != ele.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Js return Wrong format bson" ) ;
         goto error ;
      }
      else
      {
         BSONObjIterator itr( ele.embeddedObject() ) ;
         while ( itr.more() )
         {
            const CHAR *ip = "" ;
            BOOLEAN hasUninstall = FALSE ;
            ele = itr.next() ;
            if ( Object != ele.type() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG ( PDERROR, "Js return Wrong format bson" ) ;
               goto error ;
            }
            BSONObj temp = ele.embeddedObject() ;
            rc = omaGetStringElement( temp, OMA_FIELD_IP, &ip ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                        "rc = %d", OMA_FIELD_IP, rc ) ;
            }
            rc = omaGetBooleanElement( temp, OMA_FIELD_HASUNINSTALL,
                                       hasUninstall ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                        "rc = %d", OMA_FIELD_HASUNINSTALL, rc ) ;
            }
            if ( !hasUninstall )
            {
               bab.append ( ip ) ;
            }
         }
         arr = bab.arr() ;
         if ( !arr.isEmpty() )
         {
            str = ", need to uninstall db packet in these hosts manually: " ;
            str += arr.toString( TRUE, FALSE ).c_str() ;
         }
      }
      ossSnprintf( pBuf, OMA_BUFF_SIZE, "%s%s", pDetail, str.c_str() ) ;  
      PD_LOG_MSG ( PDERROR, pBuf ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHost::_buildRetResult( BSONObj &obj,
                                       BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObjBuilder bob ;
         bob.append( OMA_FIELD_TRANSACTION_ID, _transactionID ) ;
         bob.appendElements( obj ) ;
         retObj = bob.obj() ; 
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   /******************************* add host2 ********************************/
   /*
      _omaAddHost2
   */
   _omaAddHost2::_omaAddHost2 ()
   {
   }

   _omaAddHost2::~_omaAddHost2 ()
   {
   }

   INT32 _omaAddHost2::init( const CHAR *pAddHostInfo )
   {
      INT32 rc = SDB_OK ;
      EDUID startaddHostTaskJobID = PMD_INVALID_EDUID ;
      rc = startAddHostTaskJob ( pAddHostInfo, &startaddHostTaskJobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start add hosts task "
                 "job, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHost2::doit( BSONObj &retObj )
   {
      return SDB_OK ;
   }

   /******************************* remove host ********************************/
   /*
      _omaRemoveHost
   */
   _omaRemoveHost::_omaRemoveHost ()
   {
   }

   _omaRemoveHost::~_omaRemoveHost ()
   {
   }

   INT32 _omaRemoveHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove hosts passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_REMOVE_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   /******************************* add db business **************************/
   _omaInsDBBus::_omaInsDBBus ()
   {
   }

   _omaInsDBBus::~_omaInsDBBus ()
   {
   }

   INT32 _omaInsDBBus::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      EDUID startDBTaskJobID = PMD_INVALID_EDUID ;
      rc = startInsDBBusTaskJob ( pInstallInfo, &startDBTaskJobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start install db business task "
                 "job, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBus::doit( BSONObj &retObj )
   {
      return SDB_OK ;
   }

   /******************************* uninstall db business *********************/
   _omaUninsDBBus::_omaUninsDBBus ()
   {
   }

   _omaUninsDBBus::~_omaUninsDBBus ()
   {
   }

   INT32 _omaUninsDBBus::init( const CHAR *pUninstallInfo )
   {
      INT32 rc = SDB_OK ;
      EDUID removeDBTaskJobID = PMD_INVALID_EDUID ;
      rc = startRmDBBusTaskJob ( pUninstallInfo, &removeDBTaskJobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start remove db business task "
                 "job, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaUninsDBBus::doit( BSONObj &retObj )
   {
      return SDB_OK ;
   }

   /******************************* query progress status *********************/
   /*
      _omaQueryTaskProgress
   */
   _omaQueryTaskProgress::_omaQueryTaskProgress ()
   {
      _taskID = OMA_INVALID_TASKID ;
   }

   _omaQueryTaskProgress::~_omaQueryTaskProgress ()
   {
   }

   INT32 _omaQueryTaskProgress::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      BSONObj arg( pInstallInfo ) ;
      try
      {
         ele = arg.getField ( OMA_FIELD_TASKID ) ;
         if ( NumberInt != ele.type() && NumberLong != ele.type() )
         {
            rc = SDB_UNEXPECTED_RESULT ;
            PD_LOG_MSG ( PDERROR, "Failed to get taskID, rc = %d", rc ) ;
            goto error ;
         }
         _taskID = ele.numberLong () ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG_MSG ( PDERROR,  "Failed to get taskID, "
                      "received exception: %s", e.what() ) ;
         goto error ;
      }
      _taskMgr = getTaskMgr() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaQueryTaskProgress::doit ( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      _omaTask *pTask  = NULL ;
      pTask = _taskMgr->findTask( _taskID ) ;
      if ( NULL == pTask )
      {
         rc = SDB_OM_TASK_NOT_EXIST ;
         PD_LOG_MSG ( PDERROR, "No such task with id[%ld], "
                      "failed to query task's progress", (INT64)_taskID ) ;
         goto error ;
      }
      rc = pTask->queryProgress( retObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to query task's progress, "
                  "rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   /*************************** update hosts table info **********************/
   /*
      _omaUpdateHostsInfo
   */
   _omaUpdateHostsInfo::_omaUpdateHostsInfo ()
   {
   }

   _omaUpdateHostsInfo::~_omaUpdateHostsInfo ()
   {
   }

   INT32 _omaUpdateHostsInfo::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      BSONObj bus( pInstallInfo ) ;
      ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                   JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
      PD_LOG ( PDDEBUG, "Update hosts info passes argument: %s",
               _jsFileArgs ) ;
      rc = addJsFile ( FILE_UPDATE_HOSTS_INFO, _jsFileArgs ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to add js file[%s]", FILE_UPDATE_HOSTS_INFO ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*************************** query host status **************************/
   /*
      _omaQueryHostStatus
   */
   _omaQueryHostStatus::_omaQueryHostStatus()
   {
   }

   _omaQueryHostStatus::~_omaQueryHostStatus()
   {
   }

   INT32 _omaQueryHostStatus::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "_omaQueryHostStatus passes argument: %s",
                  _jsFileArgs ) ;

         rc = addJsFile( FILE_QUERY_HOSTSTATUS_ITEM ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_QUERY_HOSTSTATUS_ITEM, rc ) ;
            goto error ;
         }

         rc = addJsFile( FILE_QUERY_HOSTSTATUS, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_QUERY_HOSTSTATUS, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error :
      goto done ;
   }

   _omaCreateVirtualCoord::_omaCreateVirtualCoord ()
   {
   }

   _omaCreateVirtualCoord::~_omaCreateVirtualCoord ()
   {
   }

   INT32 _omaCreateVirtualCoord::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus ;
         if ( NULL == pInstallInfo )
         {
            BSONObjBuilder bob ;
            BSONArrayBuilder bab ;
            bob.appendArray( OMA_FIELD_CATAADDR, bab.arr() ) ;
            bus = bob.obj() ;
         }
         else
         {
            bus = BSONObj(pInstallInfo).getOwned() ;
         }
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Create temp coord passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_CREATE_TEMP_COORD, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_CREATE_TEMP_COORD, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaCreateVirtualCoord::createVirtualCoord( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      rc = init( NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init for creating "
                  "temp coord, rc = %d", rc ) ;
         goto error ;
      }
      rc = doit( retObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to create temp coord, rc = %d", rc ) ;
         goto error ;
      }     
   done:
      return rc ;
   error:
      goto done ;
   }

   _omaRemoveVirtualCoord::_omaRemoveVirtualCoord (
                                              const CHAR *pVCoordSvcName )
   {
      ossStrncpy( _vCoordSvcName, pVCoordSvcName, OSS_MAX_SERVICENAME ) ;
   }

   _omaRemoveVirtualCoord::~_omaRemoveVirtualCoord ()
   {
   }

   INT32 _omaRemoveVirtualCoord::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj sys = BSON (
                 OMA_FIELD_VCOORDSVCNAME << _vCoordSvcName ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove temp coord passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_TEMP_COORD, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_REMOVE_TEMP_COORD, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaRemoveVirtualCoord::removeVirtualCoord( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      rc = init( NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init for creating temp coord, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = doit( retObj ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to create temp coord, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   _omaAddHostRollbackInternal::_omaAddHostRollbackInternal()
   {
   }

   _omaAddHostRollbackInternal::~_omaAddHostRollbackInternal()
   {
   }

   INT32 _omaAddHostRollbackInternal::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Rollback add hosts passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_ADDHOST_ROLLBACK, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_ADDHOST_ROLLBACK, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error :
      goto done ;
   }


} // namespace engine

