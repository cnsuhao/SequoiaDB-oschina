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

   Source File Name = sdbstart.cpp

   Descriptive Name = sdbstart Main

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for sdbstart,
   which is used to start SequoiaDB engine.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "core.hpp"
#include "ossUtil.hpp"
#include "ossMem.hpp"
#include "pd.hpp"
#include "ossPath.hpp"
#include "ossProc.hpp"
#include "pmdDef.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "utilCommon.hpp"
#include "utilNodeOpr.hpp"
#include "pmdOptions.h"
#include "utilParam.hpp"
#include "ossVer.h"
#include "omagentDef.hpp"
#include "ossIO.hpp"
#include "ossCmdRunner.hpp"

#include <vector>
#include <string>
#include <list>
#include <boost/algorithm/string.hpp>

using namespace std ;
using namespace boost::algorithm ;

namespace engine
{

   #define PMD_OPTION_FORCE         "force"
   #define PMD_OPTION_OPTIONS       "options"

   #define COMMANDS_OPTIONS \
       ( PMD_COMMANDS_STRING( PMD_OPTION_HELP, ",h"), "help" ) \
       ( PMD_OPTION_VERSION, "version" ) \
       ( PMD_COMMANDS_STRING( PMD_OPTION_CONFPATH, ",c"), po::value<string>(), "configure file path" ) \
       ( PMD_COMMANDS_STRING( PMD_OPTION_SVCNAME, ",p"), po::value<string>(), "service name, separated by comma (',')" ) \
       ( PMD_COMMANDS_STRING( PMD_OPTION_TYPE, ",t"), po::value<string>(), "node type: db/om/all, default: db" ) \
       ( PMD_COMMANDS_STRING( PMD_OPTION_ROLE, ",r" ), po::value<string>(), "role type: coord/data/catalog/om" ) \
       ( PMD_OPTION_FORCE, "force start when the config not exist" ) \
       ( PMD_COMMANDS_STRING( PMD_OPTION_OPTIONS, ",o" ), po::value<string>(), "SequoiaDB start arguments, but not use '-c/--confpath/-p/--svcname'" ) \


   #define COMMANDS_HIDE_OPTIONS \
      ( PMD_OPTION_HELPFULL, "help all configs" ) \
      ( PMD_OPTION_CURUSER, "inherite the current user" ) \


   void init ( po::options_description &desc,
               po::options_description &all )
   {
      PMD_ADD_PARAM_OPTIONS_BEGIN ( desc )
         COMMANDS_OPTIONS
      PMD_ADD_PARAM_OPTIONS_END

      PMD_ADD_PARAM_OPTIONS_BEGIN ( all )
         COMMANDS_OPTIONS
         COMMANDS_HIDE_OPTIONS
      PMD_ADD_PARAM_OPTIONS_END
   }

   void displayArg ( po::options_description &desc )
   {
      std::cout << desc << std::endl ;
   }

   BOOLEAN serviceExists ( const CHAR *pServiceName,
                           utilNodeInfo &info )
   {
      UTIL_VEC_NODES nodes ;
      INT32 rc = utilListNodes( nodes, -1, pServiceName ) ;
      if ( SDB_OK == rc && nodes.size() > 0 &&
           SDB_TYPE_OMA != (*nodes.begin())._type )
      {
         info = *nodes.begin() ;
         return TRUE ;
      }
      return FALSE ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SDBSTART_RESVARG, "resolveArgument" )
   INT32 resolveArgument ( po::options_description &desc,
                           po::options_description &all,
                           po::variables_map &vm,
                           INT32 argc, CHAR **argv,
                           vector< string > &configs,
                           vector< utilNodeInfo > &nodesinfo,
                           INT32 &typeFilter, INT32 &roleFilter,
                           string &options )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_SDBSTART_RESVARG );
      string confPath ;
      utilNodeInfo info ;

      rc = utilReadCommandLine( argc, argv, all, vm, FALSE ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( vm.count ( PMD_OPTION_HELP ) )
      {
         displayArg ( desc ) ;
         rc = SDB_PMD_HELP_ONLY ;
         goto done ;
      }
      if ( vm.count( PMD_OPTION_HELPFULL ) )
      {
         displayArg( all ) ;
         rc = SDB_PMD_HELP_ONLY ;
         goto done ;
      }
      else if ( vm.count( PMD_OPTION_VERSION ) )
      {
         ossPrintVersion( "SDB Start Version" ) ;
         rc = SDB_PMD_VERSION_ONLY ;
         goto done ;
      }

      if ( vm.count ( PMD_OPTION_CONFPATH ) )
      {
         confPath = vm[PMD_OPTION_CONFPATH].as<string>() ;
         configs.push_back( confPath ) ;
         nodesinfo.push_back( info ) ;
      }

      if ( vm.count( PMD_OPTION_SVCNAME ) )
      {
         vector< string > listServices ;
         CHAR localPath[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
         CHAR path[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
         string svcname = vm[PMD_OPTION_SVCNAME].as<string>() ;
         rc = utilSplitStr( svcname, listServices, ", \t" ) ;
         if ( rc )
         {
            std::cout << "Parse svcname failed: " << rc << endl ;
            goto error ;
         }
         ossGetEWD( localPath, OSS_MAX_PATHSIZE ) ;
         utilCatPath( localPath, OSS_MAX_PATHSIZE, SDBCM_LOCAL_PATH ) ;
         for ( UINT32 i = 0 ; i < listServices.size() ; ++i )
         {
            utilBuildFullPath( localPath, listServices[ i ].c_str(),
                               OSS_MAX_PATHSIZE, path ) ;
            configs.push_back( string( path ) ) ;
            info._svcname = listServices[ i ] ;
            nodesinfo.push_back( info ) ;
         }
      }
      if ( vm.count( PMD_OPTION_TYPE ) )
      {
         string listType = vm[ PMD_OPTION_TYPE ].as<string>() ;
         if ( 0 == ossStrcasecmp( listType.c_str(),
                                  SDBLIST_TYPE_DB_STR ) )
         {
            typeFilter = SDB_TYPE_DB ;
         }
         else if ( 0 == ossStrcasecmp( listType.c_str(),
                                       SDBLIST_TYPE_OM_STR ) )
         {
            typeFilter = SDB_TYPE_OM ;
         }
         else if ( 0 == ossStrcasecmp( listType.c_str(),
                                       SDBLIST_TYPE_ALL_STR ) )
         {
            typeFilter = -1 ;
         }
         else
         {
            std::cout << "type invalid" << endl ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      if ( vm.count( PMD_OPTION_ROLE ))
      {
         string roleTemp = vm[PMD_OPTION_ROLE].as<string>() ;
         roleFilter = utilGetRoleEnum( roleTemp.c_str() ) ;
         if ( SDB_ROLE_MAX == roleFilter ||
              SDB_ROLE_OMA == roleFilter )
         {
            std::cout << "role invalid" << endl ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         typeFilter = -1 ;
      }
      if ( vm.count( PMD_OPTION_OPTIONS ) )
      {
         options = vm[ PMD_OPTION_OPTIONS ].as<string>() ;
         if ( ossStrstr( options.c_str(), "-c" ) ||
              ossStrstr( options.c_str(), "-p" ) ||
              ossStrstr( options.c_str(),
                         SDBCM_OPTION_PREFIX PMD_OPTION_SVCNAME ) ||
              ossStrstr( options.c_str(),
                         SDBCM_OPTION_PREFIX PMD_OPTION_CONFPATH ) )
         {
            std::cout << "options invalid" << std::endl ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB_SDBSTART_RESVARG, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   void buildListArgs( const CHAR * pEnginePathName,
                       BOOLEAN isForce,
                       const CHAR * pConfPath,
                       const CHAR * pOptions,
                       const CHAR * svcname,
                       string &cmd )
   {
      cmd = pEnginePathName ;

      if ( pConfPath && 0 != ossStrlen( pConfPath ) )
      {
         if ( !isForce || 0 == ossAccess( pConfPath ) )
         {
            cmd += " " ;
            cmd += SDBCM_OPTION_PREFIX PMD_OPTION_CONFPATH ;
            cmd += " " ;
            cmd += pConfPath ;
         }
      }

      if ( pOptions && 0 != ossStrlen( pOptions ) )
      {
         cmd += " " ;
         cmd += pOptions ;
      }
      if ( isForce && svcname && 0 != ossStrlen( svcname ) )
      {
         cmd += " " ;
         cmd += SDBCM_OPTION_PREFIX PMD_OPTION_SVCNAME ;
         cmd += " " ;
         cmd += svcname ;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SDBSTART_MAIN, "mainEntry" )
   INT32 mainEntry ( INT32 argc, CHAR **argv )
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRC = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_SDBSTART_MAIN ) ;
      CHAR rootPath[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
      CHAR enginePathName[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
      vector< string > configs ;
      vector< utilNodeInfo > nodesInfo ;
      vector< OSSHANDLE > handles ;
      vector< ossCmdRunner* > cmdRunners ;
      INT32 typeFilter  = SDB_TYPE_DB ;
      INT32 roleFilter  =  -1 ;
      string options ;
      BOOLEAN isForce   = FALSE ;
      INT32 total       = 0 ;
      INT32 succeedNum  = 0 ;
      INT32 failedNum   = 0 ;
      po::options_description desc ( "Command options" ) ;
      po::options_description all ( "Command options" ) ;
      po::variables_map vm ;
      string svcname ;
      string runCmd ;
      UINT32 exitCode = 0 ;

      init( desc, all ) ;

      rc = resolveArgument ( desc, all, vm, argc, argv, configs, nodesInfo,
                             typeFilter, roleFilter, options ) ;
      if ( rc )
      {
         if ( SDB_PMD_HELP_ONLY != rc && SDB_PMD_VERSION_ONLY != rc )
         {
            ossPrintf( "Error: Invalid argument: %d"OSS_NEWLINE, rc ) ;
            displayArg ( desc ) ;
         }
         else
         {
            rc = SDB_OK ;
         }
         goto done ;
      }

      if ( !vm.count( PMD_OPTION_CURUSER ) )
      {
         UTIL_CHECK_AND_CHG_USER() ;
      }
      if ( vm.count( PMD_OPTION_FORCE ) )
      {
         isForce = TRUE ;
      }

      rc = ossGetEWD( rootPath, OSS_MAX_PATHSIZE ) ;
      if ( rc )
      {
         ossPrintf( "Error: Get module self path failed:  %d"OSS_NEWLINE,
                    rc ) ;
         goto error ;
      }
      rc = utilBuildFullPath( rootPath, ENGINE_NAME, OSS_MAX_PATHSIZE,
                              enginePathName ) ;
      if ( rc )
      {
         ossPrintf( "Error: Build engine path name failed: %d"OSS_NEWLINE,
                    rc ) ;
         goto error ;
      }

      if ( configs.size() == 0 )
      {
         utilNodeInfo info ;
         CHAR localPath [ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
         rc = utilBuildFullPath( rootPath, SDBCM_LOCAL_PATH,
                                 OSS_MAX_PATHSIZE, localPath ) ;
         if ( rc )
         {
            ossPrintf( "Error: Build local config path failed: %d"OSS_NEWLINE,
                       rc ) ;
            goto error ;
         }
         rc = utilEnumNodes( localPath, nodesInfo, typeFilter,
                             NULL, roleFilter ) ;
         if ( rc )
         {
            ossPrintf( "Error: Enum [%s] sub dirs failed: %d"OSS_NEWLINE,
                       localPath, rc ) ;
            goto error ;
         }

         for ( UINT32 i = 0 ; i < nodesInfo.size() ; ++i )
         {
            configs.push_back( string( localPath ) +
                               string( OSS_FILE_SEP ) +
                               nodesInfo[ i ]._svcname ) ;
         }
      }

      SDB_ASSERT( configs.size() == nodesInfo.size(),
                  "config size must equal with node info size" ) ;

      for ( UINT32 j = 0 ; j < configs.size() ; ++j )
      {
         handles.push_back( (OSSHANDLE)0 ) ;
         cmdRunners.push_back( SDB_OSS_NEW ossCmdRunner() ) ;
      }

      for ( UINT32 j = 0 ; j < configs.size() ; ++j )
      {
         ++total ;
         utilNodeInfo &info = nodesInfo[ j ] ;
         OSSHANDLE &handle = handles[ j ] ;
         ossCmdRunner *runner = cmdRunners[ j ] ;
         rc = utilGetServiceByConfigPath( configs[ j ], svcname,
                                          info._svcname ) ;
         if ( SDB_OK == rc && !svcname.empty() &&
              serviceExists( svcname.c_str(), info ) )
         {
            ossPrintf ( "Success: %s(%s) is already started (%d)"OSS_NEWLINE,
                        utilDBTypeStr( (SDB_TYPE)info._type ),
                        info._svcname.c_str(), info._pid ) ;
            ++succeedNum ;
            continue ;
         }

         buildListArgs( enginePathName, isForce,
                        configs[ j ].c_str(),
                        options.c_str(),
                        svcname.c_str(),
                        runCmd ) ;
         tmpRC = runner->exec( runCmd.c_str(), exitCode, TRUE,
                               -1, TRUE, &handle ) ;
         if ( tmpRC )
         {
            rc = tmpRC ;
            ossPrintf( "Error: Start [%s] failed, rc: %d(%s)"OSS_NEWLINE,
                       configs[ j ].c_str(), tmpRC, getErrDesp( rc ) ) ;
            ++failedNum ;
            continue ;
         }
         info._pid = runner->getPID() ;
         info._svcname = svcname ;
      }

      for ( UINT32 j = 0 ; j < configs.size() ; ++j )
      {
         utilNodeInfo &info = nodesInfo[ j ] ;
         OSSHANDLE &handle = handles[ j ] ;
         ossCmdRunner *runner = cmdRunners[ j ] ;
         UINT32 exitCode = 0 ;
         UINT32 timeout = 0 ;

         if ( !info._orgname.empty() )
         {
            continue ;
         }
         if ( info._pid == OSS_INVALID_PID && info._svcname.empty() )
         {
            continue ;
         }

         while ( timeout < OSS_ONE_SEC )
         {
            ossSleep( 100 ) ;
            timeout += 100 ;
            if ( !ossIsProcessRunning( info._pid ) )
            {
               string outString ;
               runner->read( outString ) ;
               utilStrTrim( outString ) ;
#if defined( _WINDOWS )
               erase_all( outString, "\r" ) ;
#endif // _WINDOWS
               if ( !outString.empty() )
               {
                  ossPrintf( "%s: %u bytes out==>%s%s%s<=="OSS_NEWLINE,
                             info._svcname.c_str(),
                             (UINT32)(outString.length() + ossStrlen( OSS_NEWLINE ) * 2 ),
                             OSS_NEWLINE,
                             outString.c_str(),
                             OSS_NEWLINE ) ;
               }
               break ;
            }
         }
         runner->done() ;

         tmpRC = utilWaitNodeOK( info, info._svcname.c_str(), info._pid ) ;
         if ( SDB_OK == tmpRC )
         {
            ossPrintf ( "Success: %s(%s) is successfully started (%d)"
                        OSS_NEWLINE, utilDBTypeStr( (SDB_TYPE)info._type ),
                        info._svcname.c_str(), info._pid ) ;
            ++succeedNum ;
         }
         else
         {
            rc = tmpRC ;
            if ( !ossIsProcessRunning( info._pid ) &&
                 (OSSHANDLE)0 != handle &&
                 SDB_OK == ossGetExitCodeProcess( handle, exitCode ) )
            {
               rc = exitCode ;
            }
            ossPrintf( "Error: Start [%s] failed, rc: %d(%s)"OSS_NEWLINE,
                       configs[ j ].c_str(), rc,
                       getErrDesp( utilShellRC2RC( rc ) ) ) ;
            ++failedNum ;
         }
         ossCloseProcessHandle( handle ) ;
      }

      if ( 0 == total )
      {
         ossPrintf( "No node configs"OSS_NEWLINE ) ;
         rc = SDB_INVALIDARG ;
      }
      else
      {
         ossPrintf( "Total: %d; Succeed: %d; Failed: %d"OSS_NEWLINE,
                    total, succeedNum, failedNum ) ;
      }

   done :
      {
         vector< ossCmdRunner* >::iterator it = cmdRunners.begin() ;
         while ( it != cmdRunners.end() )
         {
            SDB_OSS_DEL *it ;
            ++it ;
         }
      }
      PD_TRACE_EXITRC ( SDB_SDBSTART_MAIN, rc );
      return SDB_OK == rc ? 0 : utilRC2ShellRC( rc ) ;
   error :
      goto done ;
   }

}

INT32 main ( INT32 argc, CHAR **argv )
{
   return engine::mainEntry( argc, argv ) ;
}


