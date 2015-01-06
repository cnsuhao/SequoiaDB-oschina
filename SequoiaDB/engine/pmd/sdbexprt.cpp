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

   Source File Name = sdbexprt.cpp

   Descriptive Name = Export Utility

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for sdbexprt
   which is used to do data export

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/21/2013  TW  Initial Draft
          05/22/2014  JWH
   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include "migExport.hpp"
#include "pd.hpp"
#include "ossUtil.hpp"
#include "ossMem.hpp"
#include "ossSocket.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "msgDef.h"
#include "utilSdb.hpp"
#include "migCommon.hpp"
#include <iostream>

namespace engine
{
   
   #define LOGPATH "sdbexport.log"
   
   #define OPTION_DELCHAR           "delchar"
   #define OPTION_DELFIELD          "delfield"
   #define OPTION_DELRECORD         "delrecord"
   #define OPTION_FIELD             "fields"
   #define OPTION_INCLUDED          "include"
   #define OPTION_COLLECTSPACE      "csname"
   #define OPTION_COLLECTION        "clname"
   #define OPTION_FILENAME          "file"
   #define OPTION_TYPE              FIELD_NAME_LTYPE
   #define OPTION_ERRORSTOP         "errorstop"
   #define OPTION_INCLUDEBINARY     "includebinary"
   #define OPTION_INCLUDEREGEX      "includeregex"
   #define OPTION_FILTER            "filter"
   #define OPTION_SORT              "sort"
   
   #define DEFAULT_HOSTNAME         "localhost"
   #define DEFAULT_SVCNAME          "11810"
   
   
   utilSdbTemplet utilSdbObj ;
   
   const CHAR *SDBEXPORT_TYPE_STR[] =
   {
      "csv",
      "json"
   } ;
   
   INT32 on_init( void *pData )
   {
      return SDB_OK ;
   }
   
   INT32 on_preparation( void *pData )
   {
      return SDB_OK ;
   }
   
   INT32 on_main( void *pData )
   {
      INT32 rc = SDB_OK ;
      INT32 total = 0 ;
      migExport   parser ;
      migExprtArg exprtArg ;
   
      utilSdbObj.getArgSwitch( OPTION_TYPE,  (INT32 *)(&exprtArg.type) ) ;
   
      utilSdbObj.getArgString( OPTION_HOSTNAME,     &exprtArg.pHostname ) ;
      utilSdbObj.getArgString( OPTION_SVCNAME,      &exprtArg.pSvcname ) ;
      utilSdbObj.getArgString( OPTION_USER,         &exprtArg.pUser ) ;
      utilSdbObj.getArgString( OPTION_PASSWORD,     &exprtArg.pPassword ) ;
      utilSdbObj.getArgString( OPTION_COLLECTSPACE, &exprtArg.pCSName ) ;
      utilSdbObj.getArgString( OPTION_COLLECTION,   &exprtArg.pCLName ) ;
      utilSdbObj.getArgString( OPTION_FILENAME,     &exprtArg.pFile ) ;
      utilSdbObj.getArgString( OPTION_FIELD,        &exprtArg.pFields ) ;
      utilSdbObj.getArgString( OPTION_FILTER,       &exprtArg.pFiter ) ;
      utilSdbObj.getArgString( OPTION_SORT,         &exprtArg.pSort ) ;
      
      utilSdbObj.getArgChar( OPTION_DELCHAR,        &exprtArg.delChar ) ;
      utilSdbObj.getArgChar( OPTION_DELFIELD,       &exprtArg.delField ) ;
      utilSdbObj.getArgChar( OPTION_DELRECORD,      &exprtArg.delRecord ) ;
   
      utilSdbObj.getArgBool( OPTION_ERRORSTOP,      &exprtArg.errorStop ) ;
      utilSdbObj.getArgBool( OPTION_INCLUDED,       &exprtArg.include ) ;
      utilSdbObj.getArgBool( OPTION_INCLUDEBINARY,  &exprtArg.includeBinary ) ;
      utilSdbObj.getArgBool( OPTION_INCLUDEREGEX,   &exprtArg.includeRegex ) ;
   
      if ( !exprtArg.pFields && exprtArg.type == MIGEXPRT_CSV )
      {
         ossPrintf ( "CSV format must complete the --fields"OSS_NEWLINE ) ;
         PD_LOG ( PDERROR, "CSV format must complete the --fields" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   
      rc = parser.init( &exprtArg ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to initialize parser, rc = %d", rc ) ;
         goto error ;
      }
   
      rc = parser.run ( total ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to execute parser, rc = %d", rc ) ;
         goto error ;
      }
      ossPrintf ( "%d records export"OSS_NEWLINE, total ) ;
   
   done:
      return rc ;
   error:
      goto done ;
   }
   INT32 on_end( void *pData )
   {
      return SDB_OK ;
   }
   
   #define EXPLAIN_HOSTNAME         "host name, default: localhost"
   #define EXPLAIN_SVCNAME          "service name, default: 11810"
   #define EXPLAIN_USER             "username"
   #define EXPLAIN_PASSWORD         "password"
   #define EXPLAIN_DELCHAR          "string delimiter, default: '\"' ( csv only )"
   #define EXPLAIN_DELFIELD         "field delimiter, default: ','  ( csv only )"
   #define EXPLAIN_DELRECORD        "record delimiter, default: '\\n' "
   #define EXPLAIN_COLLECTSPACE     "collection space name"
   #define EXPLAIN_COLLECTION       "collection name"
   #define EXPLAIN_FIELDS           "field names, separated by command (',')"
   #define EXPLAIN_INCLUDE          "whether to include field names of the file, default: true ( csv only )"
   #define EXPLAIN_FILENAME         "output file name"
   #define EXPLAIN_TYPE             "type of file to load, default: csv (json,csv)"
   #define EXPLAIN_ERRORSTOP        "whether stop by hitting error, default false"
   #define EXPLAIN_INCLUDEBINARY    "whether to output a compelete binary, default false( csv only )"
   #define EXPLAIN_INCLUDEREGEX     "whether to output a compelete regex, default false( csv only )"
   #define EXPLAIN_FILTER           "the matching rule(e.g. --filter '{ age: 18 }')"
   #define EXPLAIN_SORT             "the ordered rule(e.g. --sort '{ name: 1 }')"
   
   INT32 mainEntry ( INT32 argc, CHAR **argv )
   {
      INT32 rc = SDB_OK ;
      util_sdb_settings setting ;
   
      setting.on_init = on_init ;
      setting.on_preparation = on_preparation ;
      setting.on_main = on_main ;
      setting.on_end = on_end ;
   
      sdbEnablePD( LOGPATH ) ;
      setPDLevel( PDINFO ) ;
   
      APPENDARGSTRING( utilSdbObj, OPTION_HOSTNAME,     OPTION_HOSTNAME ",s",     EXPLAIN_HOSTNAME,         FALSE, OSS_MAX_HOSTNAME,    DEFAULT_HOSTNAME ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_SVCNAME,      OPTION_SVCNAME ",p",      EXPLAIN_SVCNAME,          FALSE, OSS_MAX_SERVICENAME, DEFAULT_SVCNAME ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_USER,         OPTION_USER ",u",         EXPLAIN_USER,             FALSE, -1,                  "\0" ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_PASSWORD,     OPTION_PASSWORD ",w",     EXPLAIN_PASSWORD,         FALSE, -1,                  "\0" ) ;
      APPENDARGCHAR  ( utilSdbObj, OPTION_DELCHAR,      OPTION_DELCHAR ",a",      EXPLAIN_DELCHAR,          FALSE, '"'  ) ;
      APPENDARGCHAR  ( utilSdbObj, OPTION_DELFIELD,     OPTION_DELFIELD ",e",     EXPLAIN_DELFIELD,         FALSE, ','  ) ;
      APPENDARGCHAR  ( utilSdbObj, OPTION_DELRECORD,    OPTION_DELRECORD ",r",    EXPLAIN_DELRECORD,        FALSE, '\n' ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_COLLECTSPACE, OPTION_COLLECTSPACE ",c", EXPLAIN_COLLECTSPACE,     TRUE,  -1,                  NULL ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_COLLECTION,   OPTION_COLLECTION ",l",   EXPLAIN_COLLECTION,       TRUE,  -1,                  NULL ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_FILENAME,     OPTION_FILENAME,          EXPLAIN_FILENAME,         TRUE,  -1,                  NULL ) ;
      APPENDARGSWITCH( utilSdbObj, OPTION_TYPE,         OPTION_TYPE,              EXPLAIN_TYPE,             FALSE, SDBEXPORT_TYPE_STR,  2,        MIGEXPRT_CSV ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_FIELD,        OPTION_FIELD,             EXPLAIN_FIELDS,           FALSE, -1,                  NULL ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_INCLUDED,     OPTION_INCLUDED,          EXPLAIN_INCLUDE,          FALSE, TRUE ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_ERRORSTOP,    OPTION_ERRORSTOP,         EXPLAIN_ERRORSTOP,        FALSE, FALSE  ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_INCLUDEBINARY,OPTION_INCLUDEBINARY,     EXPLAIN_INCLUDEBINARY,    FALSE, FALSE  ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_INCLUDEREGEX, OPTION_INCLUDEREGEX,      EXPLAIN_INCLUDEREGEX,     FALSE, FALSE  ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_FILTER,       OPTION_FILTER,            EXPLAIN_FILTER,           FALSE, -1,                  NULL ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_SORT,         OPTION_SORT,              EXPLAIN_SORT,             FALSE, -1,                  NULL ) ;
   
      rc = utilSdbObj.init( setting, NULL ) ;
      if ( rc )
      {
         goto error ;
      }
   
      rc = utilSdbObj.run( argc, argv, "sdbexprt version" ) ;
      if ( rc )
      {
         goto error ;
      }
   
   done :
      if ( rc )
      {
         if ( rc != SDB_PMD_HELP_ONLY && rc != SDB_PMD_VERSION_ONLY )
         {
            ossPrintf ( "Export Failed"OSS_NEWLINE ) ;
         }
      }
      else
      {
         ossPrintf ( "Export Successfully"OSS_NEWLINE ) ;
      }
      if ( rc != SDB_PMD_HELP_ONLY && rc != SDB_PMD_VERSION_ONLY )
      {
         ossPrintf ( "Detail in log path: %s"OSS_NEWLINE, getDialogName() ) ;
         PD_LOG ( PDEVENT, "Export Completed" ) ;
      }
      return SDB_OK == rc ? 0 : migRC2ShellRC( rc ) ;
   error :
      goto done ;
   }
}

INT32 main ( INT32 argc, CHAR **argv )
{
   return engine::mainEntry( argc, argv ) ;
}