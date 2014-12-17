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

   Source File Name = sdbDpsLogFilter.cpp

   Descriptive Name = N/A

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains code logic for
   data insert/update/delete. This file does NOT include index logic.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/19/2014  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#include "sdbDpsLogFilter.hpp"
#include "ossUtil.hpp"
#include "ossIO.hpp"
#include "dpsLogFile.hpp"
#include "dpsDef.hpp"
#include "toolsTrace.h"
#include "dpsLogRecord.hpp"
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

namespace fs = boost::filesystem ;

_dpsLogFilter::_dpsLogFilter( const dpsCmdData* data )
{
   SDB_ASSERT( NULL != data, "srcPath is NULL" ) ;

   _cmdData = data ;
}

_dpsLogFilter::~_dpsLogFilter()
{
   _filter = NULL ;
}

const CHAR* _dpsLogFilter::getSrcPath() const
{
   return _cmdData->srcPath ;
}

const CHAR* _dpsLogFilter::getDstPath() const
{
   return _cmdData->dstPath ;
}

const INT32 _dpsLogFilter::getFileCount( const CHAR *path )
{
   INT32 fileCount = 0 ;
   const CHAR *pFileName = "sequoiadbLog." ;
   fs::path fileDir( path ) ;
   fs::directory_iterator endIter ;
   for( fs::directory_iterator it( path ); it != endIter; ++it )
   {
      const CHAR *filepath = it->path().filename().string().c_str() ;
      const CHAR *dotPos = ossStrrchr( filepath, '.' ) ;
      if( !dotPos )
         continue ;
      const CHAR *namePos = ossStrstr( filepath, pFileName ) ;
      if( !namePos )
         continue ;
      if( ( dotPos - namePos + 1 ) != (UINT32)ossStrlen( pFileName ) )
         continue ;

      ++fileCount ;
   }

   return fileCount ;
}

BOOLEAN _dpsLogFilter::isFileExisted( const CHAR *path )
{
   BOOLEAN rc = TRUE ;
   OSSFILE file ;
   INT32 retValue = ossOpen( path, OSS_READONLY, OSS_RU, file ) ;
   if( retValue )
   {
      if( SDB_PERM == retValue )
      {
         printf( "File: %s has no permition", path ) ;
      }
      else
      {
         printf( "File: %s is not existed.\n", path ) ;
      }
      rc = FALSE ;
      goto done ;
   }
   ossClose( file ) ;

done:
   return rc ;
}

BOOLEAN _dpsLogFilter::isDir( const CHAR *path )
{
   BOOLEAN rc = FALSE ;
   SDB_OSS_FILETYPE fileType = SDB_OSS_UNK ;
   INT32 retValue = ossGetPathType( path, &fileType ) ;
   if( SDB_OSS_DIR == fileType && !retValue )
   {
      rc =  TRUE ;
      goto done ;
   }

done:
   return rc ;
}

INT32 _dpsLogFilter::doParse()
{
   INT32 rc     = SDB_OK ;
   BOOLEAN Open = FALSE ;
   CHAR dstFile[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ; 
   OSSFILE fileFrom, fileTo ;

   if( isDir( _cmdData->dstPath ) )
   {
      ossSnprintf( dstFile, OSS_MAX_PATHSIZE, "%s"OSS_FILE_SEP"%s",
                   _cmdData->dstPath, "tmpLog.log" ) ;   
   }
   else
   {
      ossSnprintf( dstFile, OSS_MAX_PATHSIZE, "%s",
                   _cmdData->dstPath ) ;
   }

   if( !_cmdData->output )
   {
      rc = ossOpen( dstFile, OSS_REPLACE | OSS_READWRITE, 
                    OSS_RU | OSS_WU | OSS_RG, fileTo ) ;
      if( rc )
      {
         printf( "Unable to open file: %s\n", dstFile ) ;
         goto error ;
      }
      Open = TRUE ;
   }

   if ( SDB_LOG_FILTER_META == _filter->getType() )
   {
      rc = _filter->doFilte( _cmdData, fileTo, NULL ) ;
      if( rc )
      {
         goto error ;
      }
      goto done ;
   }

   if( isDir( _cmdData->srcPath ) )
   {
      if( SDB_LOG_FILTER_LAST == _filter->getType() )
      {
         printf( "Error: Cannot specify a dir path when using --last/-e\n" );
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      INT32 const MAX_FILE_COUNT = getFileCount( _cmdData->srcPath ) ;
      if( 0 >= MAX_FILE_COUNT )
      {
         printf( "Cannot find any Log files\nPlease check"
                 " and input the correct log file path\n" ) ;
         rc = SDB_INVALIDPATH ;
         goto error ;
      }

      for( INT32 idx = 0 ; idx < MAX_FILE_COUNT ; ++idx )
      {
         fs::path fileDir( _cmdData->srcPath ) ;
         const CHAR *filepath = fileDir.string().c_str() ;
         CHAR filename[ OSS_MAX_PATHSIZE * 2 ] = { 0 } ;
         ossSnprintf( filename, OSS_MAX_PATHSIZE, "%s/sequoiadbLog.%d",
                      filepath, idx ) ;

         if( !isFileExisted( filename ) )
         {
            rc = SDB_INVALIDPATH ;
            goto error ;
         }

         rc = _filter->doFilte( _cmdData, fileTo, filename ) ;
         if( rc && idx != MAX_FILE_COUNT - 1 )
         {
            rc = SDB_OK ;
            continue ;
         }
      }
   }
   else
   {
      if( !isFileExisted( _cmdData->srcPath ) )
      {
         rc = SDB_INVALIDPATH ;
         goto error ;
      }

      rc = _filter->doFilte( _cmdData, fileTo, _cmdData->srcPath ) ;
      if( rc )
      {
         goto error ;
      }
   }

done:
   if( Open )
   {
      ossClose( fileTo ) ;
   }
   return rc ;

error:
   goto done ;
}

void _dpsLogFilter::setFilter( iFilter *filter )
{
   _filter = filter ;
}
