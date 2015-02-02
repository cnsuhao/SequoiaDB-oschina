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

   Source File Name = sdbstop.cpp

   Descriptive Name = sdbstop Main

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for sdbstop,
   which is used to stop SequoiaDB engine.

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
#include "ossProc.hpp"
#include "ossMem.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "pmdDef.hpp"
#include "pmdOptions.h"
#include "utilParam.hpp"
#include "utilCommon.hpp"
#include "utilNodeOpr.hpp"
#include "omagentDef.hpp"
#include "utilStr.hpp"
#include "ossVer.h"

namespace engine
{

   #define COMMANDS_OPTIONS \
       ( PMD_COMMANDS_STRING(PMD_OPTION_HELP, ",h"), "help" )\
       ( PMD_OPTION_VERSION, "version" ) \
       ( PMD_COMMANDS_STRING( PMD_OPTION_TYPE, ",t"), po::value<string>(), "node type: db/om/all, default: db" ) \
       ( PMD_COMMANDS_STRING( PMD_OPTION_ROLE, ",r" ), po::value<string>(), "role type: coord/data/catalog/om" ) \
       ( PMD_COMMANDS_STRING(PMD_OPTION_SVCNAME, ",p"), po::value<string>(), "service name, separated by comma (',')" )

   void init ( po::options_description &desc )
   {
      PMD_ADD_PARAM_OPTIONS_BEGIN ( desc )
         COMMANDS_OPTIONS
      PMD_ADD_PARAM_OPTIONS_END
   }

   void displayArg ( po::options_description &desc )
   {
      std::cout << desc << std::endl ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SDBSTOP_RESVARG, "resolveArgument" )
   INT32 resolveArgument ( po::options_description &desc, INT32 argc,
                           CHAR **argv, vector<string> &listServices,
                           INT32 &typeFilter, INT32 &roleFilter )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_SDBSTOP_RESVARG );
      po::variables_map vm ;

      rc = utilReadCommandLine( argc, argv, desc, vm, FALSE ) ;
      if ( rc )
      {
         std::cout << "Read command line failed: " << rc << endl ;
         goto error ;
      }

      if ( vm.count ( PMD_OPTION_HELP ) )
      {
         displayArg ( desc ) ;
         rc = SDB_PMD_HELP_ONLY ;
         goto error ;
      }
      else if ( vm.count( PMD_OPTION_VERSION ) )
      {
         ossPrintVersion( "Sdb Stop Version" ) ;
         rc = SDB_PMD_VERSION_ONLY ;
         goto error ;
      }

      if ( vm.count ( PMD_OPTION_SVCNAME ) )
      {
         string svcname = vm[PMD_OPTION_SVCNAME].as<string>() ;
         rc = utilSplitStr( svcname, listServices, ", \t" ) ;
         if ( rc )
         {
            std::cout << "Parse svcname failed: " << rc << endl ;
            goto error ;
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

   done :
      PD_TRACE_EXITRC ( SDB_SDBSTOP_RESVARG, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SDBSTOP_MAIN, "mainEntry" )
   INT32 mainEntry ( INT32 argc, CHAR **argv )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_SDBSTOP_MAIN ) ;
      INT32 success = 0 ;
      INT32 total = 0 ;
      vector<string> listServices ;
      UTIL_VEC_NODES listNodes ;
      BOOLEAN bFind = TRUE ;
      INT32 typeFilter = SDB_TYPE_DB ;
      INT32 roleFilter =  -1 ;
      po::options_description desc ( "Command options" ) ;
      init ( desc ) ;

      rc = resolveArgument ( desc, argc, argv, listServices, typeFilter,
                             roleFilter ) ;
      if ( rc )
      {
         if ( SDB_PMD_HELP_ONLY != rc && SDB_PMD_VERSION_ONLY != rc )
         {
            std::cout << "Invalid argument" << endl ;
            displayArg ( desc ) ;
         }
         else
         {
            rc = SDB_OK ;
         }
         goto done ;
      }

      if ( listServices.size() > 0 )
      {
         typeFilter = -1 ;
         roleFilter = -1 ;
      }

      utilListNodes( listNodes, typeFilter, NULL, OSS_INVALID_PID,
                     roleFilter ) ;

      for ( UINT32 i = 0 ; i < listNodes.size() ; ++i )
      {
         utilNodeInfo &info = listNodes[ i ] ;

         if ( SDB_TYPE_OMA == info._type )
         {
            continue ;
         }

         if ( listServices.size() > 0 )
         {
            bFind = FALSE ;
            for ( UINT32 j = 0 ; j < listServices.size() ; ++j )
            {
               if ( 0 == ossStrcmp( info._svcname.c_str(),
                                    listServices[ j ].c_str() ) )
               {
                  bFind = TRUE ;
                  break ;
               }
            }
         }
         else
         {
            bFind = TRUE ;
         }

         if ( bFind )
         {
            ++total ;
            ossPrintf ( "Terminating process %d: %s(%s)"OSS_NEWLINE,
                        info._pid, utilDBTypeStr( (SDB_TYPE)info._type ),
                        info._svcname.c_str() ) ;

            rc = utilStopNode( info ) ;
            if ( SDB_OK == rc )
            {
               ++success ;
               ossPrintf ( "DONE"OSS_NEWLINE ) ;
            }
            else
            {
               ossPrintf ( "FAILED"OSS_NEWLINE ) ;
            }
         }
      }

      ossPrintf ( "Total: %d; Success: %d; Failed: %d"OSS_NEWLINE,
                  total, success, total - success ) ;

      if ( total == success )
      {
         rc = SDB_OK ;
      }
      else if ( success == 0 )
      {
         rc = STOPFAIL ;
      }
      else
      {
         rc = STOPPART ;
      }

   done :
      PD_TRACE_EXITRC( SDB_SDBSTOP_MAIN, rc ) ;
      return ( rc >= 0 ) ? rc : utilRC2ShellRC( rc ) ;
   }

}

INT32 main ( INT32 argc, CHAR **argv )
{
   return engine::mainEntry( argc, argv ) ;
}


