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

   Source File Name = ossPath.cpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          12/1/2014  ly  Initial Draft

   Last Changed =

*******************************************************************************/

#include "ossPath.hpp"
#include "ossErr.h"
#include "ossUtil.h"
#include "ossMem.h"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "ossTrace.hpp"

#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

namespace fs = boost::filesystem ;

#if defined (_WINDOWS)

// PD_TRACE_DECLARE_FUNCTION ( SDB_GETEXECNM, "getExecutableName" )
static INT32 getExecutableName ( const CHAR * exeName ,
                                 CHAR * buf ,
                                 UINT32 bufSize )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_GETEXECNM );

   SDB_ASSERT ( exeName && buf && bufSize > 0 , "invalid argument" ) ;

   try
   {
      string strName = exeName ;
      string strEnd  = ".exe" ;
      if ( strName.length() <= strEnd.length() ||
           0 != strName.compare ( strName.length() - strEnd.length() ,
                                  strEnd.length() , strEnd ) )
      {
         strName += strEnd ;
      }

      if ( strName.length() >= bufSize )
      {
         rc = SDB_INVALIDSIZE ;
         goto error ;
      }
      ossStrcpy ( buf , strName.c_str() ) ;
   }
   catch ( std::bad_alloc & )
   {
      rc = SDB_OOM ;
      goto error ;
   }

done :
   PD_TRACE_EXITRC ( SDB_GETEXECNM, rc );
   return rc ;
error :
   goto done ;
}

#endif

// PD_TRACE_DECLARE_FUNCTION ( SDB_OSSLCEXEC, "ossLocateExecutable" )
INT32 ossLocateExecutable ( const CHAR * refPath ,
                            const CHAR * exeName ,
                            CHAR * buf ,
                            UINT32 bufSize )
{
   INT32          rc          = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_OSSLCEXEC );
   INT32          dirLen      = 0 ;
   UINT32         exeLen      = 0 ;
   const CHAR *   separator   = NULL ;
   CHAR newExeName[ OSS_MAX_PATHSIZE + 1 ] = {0} ;

   ossMemset ( newExeName , 0 , sizeof ( newExeName ) ) ;

   if ( ! ( refPath && exeName && buf && bufSize > 0 ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

#ifdef _WINDOWS
   rc = getExecutableName ( exeName , newExeName , sizeof ( newExeName ) ) ;
   if ( rc != SDB_OK )
   {
      goto error ;
   }
#else
   if ( ossStrlen ( exeName ) >= sizeof ( newExeName ) )
   {
      rc = SDB_INVALIDSIZE ;
      goto error ;
   }
   ossStrncpy ( newExeName , exeName, sizeof ( newExeName ) ) ;
#endif

   exeLen = ossStrlen ( newExeName ) ;

   separator = ossStrrchr ( refPath , OSS_PATH_SEP_CHAR ) ;
   if ( ! separator )
   {
      if ( exeLen >= bufSize )
      {
         rc = SDB_INVALIDSIZE ;
         goto error ;
      }
      ossStrcpy ( buf , newExeName ) ;
      goto done ;
   }

   dirLen = separator - refPath ; // length without separator

   if ( dirLen + exeLen + 1 >= bufSize )
   {
      rc = SDB_INVALIDSIZE ;
      goto error ;
   }

   ossStrncpy ( buf , refPath , dirLen + 1 ) ; // 1 for separator
   buf[dirLen + 1] = '\0' ;
   ossStrncat ( buf , newExeName , exeLen ) ;

done :
   PD_TRACE_EXITRC ( SDB_OSSLCEXEC, rc );
   return rc ;
error :
   goto done ;
}

static INT32 _ossEnumFiles( const string &dirPath,
                            map<string, string> &mapFiles,
                            const CHAR *filter, UINT32 filterLen,
                            OSS_MATCH_TYPE type, UINT32 deep )
{
   INT32 rc = SDB_OK ;
   const CHAR *pFind = NULL ;

   fs::path dbDir ( dirPath ) ;
   fs::directory_iterator end_iter ;

   if ( 0 == deep )
   {
      goto done ;
   }

   if ( fs::exists ( dbDir ) && fs::is_directory ( dbDir ) )
   {
      for ( fs::directory_iterator dir_iter ( dbDir );
            dir_iter != end_iter; ++dir_iter )
      {
         if ( fs::is_regular_file ( dir_iter->status() ) )
         {
            const std::string fileName =
               dir_iter->path().filename().string() ;

            if ( ( OSS_MATCH_NULL == type ) ||
                 ( OSS_MATCH_LEFT == type &&
                   0 == ossStrncmp( fileName.c_str(), filter, filterLen ) ) ||
                 ( OSS_MATCH_MID == type &&
                   ossStrstr( fileName.c_str(), filter ) ) ||
                 ( OSS_MATCH_RIGHT == type &&
                   ( pFind = ossStrstr( fileName.c_str(), filter ) ) &&
                   pFind[filterLen] == 0 ) ||
                 ( OSS_MATCH_ALL == type &&
                   0 == ossStrcmp( fileName.c_str(), filter ) )
               )
            {
               mapFiles[ fileName ] = dir_iter->path().string() ;
            }
         }
         else if ( fs::is_directory( dir_iter->path() ) && deep > 1 )
         {
            _ossEnumFiles( dir_iter->path().string(), mapFiles,
                           filter, filterLen, type, deep - 1 ) ;
         }
      }
   }
   else
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 ossEnumFiles( const string &dirPath,
                    map< string, string > &mapFiles,
                    const CHAR *filter,
                    UINT32 deep )
{
   string newFilter ;
   OSS_MATCH_TYPE type = OSS_MATCH_NULL ;

   if ( !filter || filter[0] == 0 || 0 == ossStrcmp( filter, "*" ) )
   {
      type = OSS_MATCH_NULL ;
   }
   else if ( filter[0] != '*' && filter[ ossStrlen( filter ) - 1 ] != '*' )
   {
      type = OSS_MATCH_ALL ;
      newFilter = filter ;
   }
   else if ( filter[0] == '*' && filter[ ossStrlen( filter ) - 1 ] == '*' )
   {
      type = OSS_MATCH_MID ;
      newFilter.assign( &filter[1], ossStrlen( filter ) - 2 ) ;
   }
   else if ( filter[0] == '*' )
   {
      type = OSS_MATCH_RIGHT ;
      newFilter.assign( &filter[1], ossStrlen( filter ) - 1 ) ;
   }
   else
   {
      type = OSS_MATCH_LEFT ;
      newFilter.assign( filter, ossStrlen( filter ) -1 ) ;
   }

   return _ossEnumFiles( dirPath, mapFiles, newFilter.c_str(),
                         newFilter.length(), type, deep ) ;
}

INT32 ossEnumFiles2( const string &dirPath,
                     map<string, string> &mapFiles,
                     const CHAR *filter,
                     OSS_MATCH_TYPE type,
                     UINT32 deep )
{
   return _ossEnumFiles( dirPath, mapFiles, filter,
                         filter ? ossStrlen( filter ) : 0,
                         type, deep ) ;
}

static INT32 _ossEnumSubDirs( const string &dirPath,
                              const string &parentSubDir,
                              vector< string > &subDirs,
                              UINT32 deep )
{
   INT32 rc = SDB_OK ;

   fs::path dbDir ( dirPath ) ;
   fs::directory_iterator end_iter ;

   string subDir ;

   if ( 0 == deep )
   {
      goto done ;
   }

   if ( fs::exists ( dbDir ) && fs::is_directory ( dbDir ) )
   {
      for ( fs::directory_iterator dir_iter ( dbDir );
            dir_iter != end_iter; ++dir_iter )
      {
         if ( fs::is_directory( dir_iter->path() ) )
         {
            if ( parentSubDir.empty() )
            {
               subDir = dir_iter->path().leaf().string() ;
            }
            else
            {
               string subDir = parentSubDir ;
               subDir += OSS_FILE_SEP ;
               subDir += dir_iter->path().leaf().string() ;
            }
            subDirs.push_back( subDir ) ;

            if ( deep > 1 )
            {
               _ossEnumSubDirs( dir_iter->path().string(), subDir,
                                subDirs,deep - 1 ) ;
            }
         }
      }
   }
   else
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 ossEnumSubDirs( const string &dirPath, vector < string > &subDirs,
                      UINT32 deep )
{
   return _ossEnumSubDirs( dirPath, "", subDirs, deep ) ;
}

