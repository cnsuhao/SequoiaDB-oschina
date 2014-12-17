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

   Source File Name = utilParam.hpp

   Descriptive Name =

   When/how to use: str util

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          26/04/2014  XJH Initial Draft

   Last Changed =

******************************************************************************/

#ifndef UTILPARAM_HPP__
#define UTILPARAM_HPP__

#include "core.hpp"
#include <string>

#include <boost/program_options.hpp>
#include <boost/program_options/parsers.hpp>

namespace po = boost::program_options ;
using namespace std ;

namespace engine
{

   INT32 utilReadConfigureFile( const CHAR *file,
                                po::options_description &desc,
                                po::variables_map &vm ) ;

   INT32 utilReadCommandLine( INT32 argc, CHAR **argv,
                              po::options_description &desc,
                              po::variables_map &vm,
                              BOOLEAN allowUnreg = TRUE ) ;

   INT32 utilWriteConfigFile( const CHAR * pFile, const CHAR * pData,
                              BOOLEAN createOnly = FALSE ) ;

   INT32 utilGetServiceByConfigPath( const string& confPath,
                                     string &svcname,
                                     const string &defaultName,
                                     BOOLEAN allowFileNotExist = TRUE ) ;

   INT32 utilGetRoleByConfigPath( const string& confPath,
                                  INT32 &role,
                                  BOOLEAN allowFileNotExist = TRUE ) ;

   INT32 utilGetDBPathByConfigPath( const string& confPath,
                                    string &dbPath,
                                    BOOLEAN allowFileNotExist = TRUE ) ;

   INT32 utilGetCMService( const string &rootPath,
                           const string &hostname,
                           string &svcname,
                           BOOLEAN allowFileNotExist = TRUE ) ;


#if defined( _LINUX )
   #define SDB_INSTALL_FILE_NAME             "/etc/default/sequoiadb"
#else
   #define SDB_INSTALL_FILE_NAME             "C:\\default\\sequoiadb"
#endif // _LINUX
   #define SDB_INSTALL_RUN_FILED             "NAME"
   #define SDB_INSTALL_USER_FIELD            "SDBADMIN_USER"
   #define SDB_INSTALL_PATH_FIELD            "INSTALL_DIR"

   struct _utilInstallInfo
   {
      string      _run ;   // SDB_INSTALL_RUN_FILED
      string      _user ;  // SDB_INSTALL_USER_FIELD
      string      _path ;  // SDB_INSTALL_PATH_FIELD
   } ;
   typedef _utilInstallInfo utilInstallInfo ;

   INT32 utilGetInstallInfo( utilInstallInfo &info ) ;

   INT32 utilCheckAndChangeUserInfo( const CHAR *curFileName ) ;

   /*
      Check start user info
   */
   #define UTIL_CHECK_AND_CHG_USER() \
      do \
      { \
         rc = utilCheckAndChangeUserInfo( argv[ 0 ] ) ; \
         if ( rc ) \
         { \
            goto error ; \
         } \
      } while ( 0 )

}

#endif // UTILPARAM_HPP__

