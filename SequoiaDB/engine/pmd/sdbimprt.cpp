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

   Source File Name = sdbimprt.cpp

   Descriptive Name = Import Utility

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for sdbimprt
   which is used to do data import

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/21/2013  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include "migImport.hpp"
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
   
   #define LOGPATH "sdbimport.log"
   
   #define OPTION_DELCHAR           "delchar"
   #define OPTION_DELFIELD          "delfield"
   #define OPTION_DELRECORD         "delrecord"
   #define OPTION_FILENAME          "file"
   #define OPTION_EXTRA             "extra"
   #define OPTION_SPARSE            "sparse"
   #define OPTION_LINEPRIORITY      "linepriority"
   #define OPTION_STRINGTYPE        "stringtype"
   #define OPTION_FIELD             FIELD_NAME_FIELDS
   #define OPTION_HEADERLINE        FIELD_NAME_HEADERLINE
   #define OPTION_COLLECTSPACE      "csname"
   #define OPTION_COLLECTION        "clname"
   #define OPTION_TYPE              FIELD_NAME_LTYPE
   #define OPTION_INSERTNUM         "insertnum"
   #define OPTION_ERRORSTOP         "errorstop"
   #define OPTION_FORCE             "force"
   
   #define DEFAULT_HOSTNAME         "localhost"
   #define DEFAULT_SVCNAME          "11810"
   
   utilSdbTemplet utilSdbObj ;
   
   const CHAR *SDBIMPORT_TYPE_STR[] =
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
      INT32 succ = 0 ;
      BOOLEAN *pIsFull = (BOOLEAN *)pData ;
      migImport   parser ;
      migImprtArg imprtArg ;
   
      utilSdbObj.getArgInt   ( OPTION_INSERTNUM, &imprtArg.insertNum ) ;
      utilSdbObj.getArgSwitch( OPTION_TYPE,  (INT32 *)(&imprtArg.type) ) ;
   
      utilSdbObj.getArgString( OPTION_HOSTNAME,     &imprtArg.pHostname ) ;
      utilSdbObj.getArgString( OPTION_SVCNAME,      &imprtArg.pSvcname ) ;
      utilSdbObj.getArgString( OPTION_USER,         &imprtArg.pUser ) ;
      utilSdbObj.getArgString( OPTION_PASSWORD,     &imprtArg.pPassword ) ;
      utilSdbObj.getArgString( OPTION_COLLECTSPACE, &imprtArg.pCSName ) ;
      utilSdbObj.getArgString( OPTION_COLLECTION,   &imprtArg.pCLName ) ;
      utilSdbObj.getArgString( OPTION_FILENAME,     &imprtArg.pFile ) ;
      utilSdbObj.getArgString( OPTION_FIELD,        &imprtArg.pFields ) ;
      
      utilSdbObj.getArgChar( OPTION_DELCHAR,   &imprtArg.delChar ) ;
      utilSdbObj.getArgChar( OPTION_DELFIELD,  &imprtArg.delField ) ;
      utilSdbObj.getArgChar( OPTION_DELRECORD, &imprtArg.delRecord ) ;
   
      utilSdbObj.getArgBool( OPTION_HEADERLINE,   &imprtArg.isHeaderline ) ;
      utilSdbObj.getArgBool( OPTION_SPARSE,       &imprtArg.autoAddField ) ;
      utilSdbObj.getArgBool( OPTION_EXTRA,        &imprtArg.autoCompletion ) ;
      utilSdbObj.getArgBool( OPTION_LINEPRIORITY, &imprtArg.linePriority ) ;
      utilSdbObj.getArgBool( OPTION_ERRORSTOP,    &imprtArg.errorStop ) ;
      utilSdbObj.getArgBool( OPTION_FORCE,        &imprtArg.force ) ;
   
      if ( imprtArg.type == MIGIMPRT_CSV &&
           !imprtArg.isHeaderline && !imprtArg.pFields )
      {
         ossPrintf ( "if not read first line,than must input fields" ) ;
         PD_LOG ( PDERROR, "if not read first line,than must input fields" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   
      rc = parser.init( &imprtArg ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to initialize parser, rc = %d", rc ) ;
         goto error ;
      }
   
      rc = parser.run ( total, succ ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to execute parser, rc = %d", rc ) ;
         goto error ;
      }
      if( total > succ )
      {
         *pIsFull = FALSE ;
      }
      ossPrintf ( "%d records in file, %d records import"OSS_NEWLINE, total, succ ) ;
   
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
   #define EXPLAIN_DELCHAR          "string delimiter, default: '\"', ( csv only )"
   #define EXPLAIN_DELFIELD         "field delimiter, default: ',' , ( csv only )"
   #define EXPLAIN_DELRECORD        "record delimiter, default: '\\n', ( csv only )"
   #define EXPLAIN_COLLECTSPACE     "collection space name"
   #define EXPLAIN_COLLECTION       "collection name"
   #define EXPLAIN_INSERTNUM        "batch insert records number, minimun 1, maximum 100000, default: 100"
   #define EXPLAIN_FILENAME         "input file name"
   #define EXPLAIN_TYPE             "type of file to load, default: csv (json,csv)"
   #define EXPLAIN_FIELDS           "field name, separated by comma (',')(e.g. \"--fields name,age\"). "\
                                    "field type and default value can be specified for csv input (e.g. \"--fields name string,age int default 18\")"
   #define EXPLAIN_HEADERLINE       "for csv input, whether the first line defines field name. if --fields is defined, the first line will be ignored if this options is true"
   #define EXPLAIN_SPARSE           "for csv input, whether to add missing field, default: true"
   #define EXPLAIN_EXTRA            "for csv input, whether to add missing value, default: false"
   #define EXPLAIN_LINEPRIORITY     "reverse the priority for record and character delimiter, default: true"
   #define EXPLAIN_ERRORSTOP        "whether stop by hitting error, default false"
   #define EXPLAIN_FORCE            "force to insert the records that are not in utf-8 format, default: false"
   
   INT32 mainEntry ( INT32 argc, CHAR **argv )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN isFull = TRUE ;
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
      APPENDARGINT   ( utilSdbObj, OPTION_INSERTNUM,    OPTION_INSERTNUM ",n",    EXPLAIN_INSERTNUM,        FALSE, 1,                   100000,   100 ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_FILENAME,     OPTION_FILENAME,          EXPLAIN_FILENAME,         TRUE,  -1,                  NULL ) ;
      APPENDARGSWITCH( utilSdbObj, OPTION_TYPE,         OPTION_TYPE,              EXPLAIN_TYPE,             FALSE, SDBIMPORT_TYPE_STR,  2,        MIGIMPRT_CSV ) ;
      APPENDARGSTRING( utilSdbObj, OPTION_FIELD,        OPTION_FIELD,             EXPLAIN_FIELDS,           FALSE, -1,                  NULL ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_HEADERLINE,   OPTION_HEADERLINE,        EXPLAIN_HEADERLINE,       FALSE, FALSE ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_SPARSE,       OPTION_SPARSE,            EXPLAIN_SPARSE,           FALSE, TRUE  ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_EXTRA,        OPTION_EXTRA,             EXPLAIN_EXTRA,            FALSE, FALSE ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_LINEPRIORITY, OPTION_LINEPRIORITY,      EXPLAIN_LINEPRIORITY,     FALSE, TRUE  ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_ERRORSTOP,    OPTION_ERRORSTOP,         EXPLAIN_ERRORSTOP,        FALSE, FALSE  ) ;
      APPENDARGBOOL  ( utilSdbObj, OPTION_FORCE,        OPTION_FORCE,             EXPLAIN_FORCE,            FALSE, FALSE  ) ;
   
      rc = utilSdbObj.init( setting, &isFull ) ;
      if ( rc )
      {
         goto error ;
      }
   
      rc = utilSdbObj.run( argc, argv, "sdbimprt version" ) ;
      if ( rc )
      {
         goto error ;
      }
   
   done :
      if ( rc )
      {
         if ( rc != SDB_PMD_HELP_ONLY && rc != SDB_PMD_VERSION_ONLY )
         {
            ossPrintf ( "Import Failed"OSS_NEWLINE ) ;
         }
      }
      else
      {
         ossPrintf ( "Import Successfully"OSS_NEWLINE ) ;
      }
      if ( rc != SDB_PMD_HELP_ONLY && rc != SDB_PMD_VERSION_ONLY )
      {
         ossPrintf ( "Detail in log path: %s"OSS_NEWLINE, getDialogName() ) ;
         PD_LOG ( PDEVENT, "Import Completed" ) ;
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