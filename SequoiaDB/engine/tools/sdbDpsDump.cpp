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

   Source File Name = dpsDump.cpp

   Descriptive Name = Data Protection Service Log Formatter

   When/how to use: this program may be used on binary and text-formatted
   versions of data protection component. This file contains code to format log
   files.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          12/05/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "dpsLogRecord.hpp"
#include "dpsLogFile.hpp"
#include "dpsDump.hpp"
#include "ossIO.hpp"
#include "ossUtil.hpp"
#include "ossMem.hpp"
#include "../bson/bson.h"
#include "pdTrace.hpp"
#include "toolsTrace.hpp"
#include <vector>

#define LOG_PREFIX "sequoiadbLog.%d"
#define LOG_FILE_NAME_BUFFER 128
#define LOG_BUFFER_FORMAT_MULTIPLIER 10
#define ARG_FROM "-f"
#define ARG_TO   "-t"
#define ARG_LOG  "-l"
using namespace engine ;
using namespace bson ;
using namespace std ;

OSSFILE outputFile ;
BOOLEAN writeToOutput = FALSE ;
BOOLEAN dumpHex       = FALSE ;
BOOLEAN dumpVerbos    = FALSE ;
vector<SINT32> logID ;
CHAR logFileName [128] ;

// PD_TRACE_DECLARE_FUNCTION ( SDB_FORMATLOG, "formatLog" )
INT32 formatLog ( SINT32 logID )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_FORMATLOG );
   ossMemset ( logFileName, 0, sizeof(logFileName) ) ;
   ossSnprintf ( logFileName, LOG_FILE_NAME_BUFFER,
                 LOG_PREFIX, logID ) ;
   printf ( "Formatting log file %s\n", logFileName ) ;
   INT64 fileSize = 0 ;
   INT64 fileRead = 0 ;
   OSSFILE file ;
   CHAR *pBuffer = NULL ;
   CHAR *pCur    = NULL ;
   CHAR *pOutputBuffer = NULL ;
   UINT32 outputBufferSz = 0 ;
   dpsLogHeader *logHeader = NULL ;
   BOOLEAN opened = FALSE ;

   INT64 restLen = 0 ;
   INT64 readPos = 0 ;

   rc = ossOpen ( logFileName, OSS_DEFAULT | OSS_READONLY,
                  OSS_RU | OSS_WU | OSS_RG, file ) ;
   if ( rc )
   {
      printf ( "Unable to open file: %s, rc = %d\n", logFileName, rc ) ;
      goto error ;
   }
   opened = TRUE ;
   rc = ossGetFileSize ( &file, &fileSize ) ;
   if ( rc )
   {
      printf ( "Failed to get file size: %s, rc = %d\n",
               logFileName, rc ) ;
      goto error ;
   }
   if ( fileSize < DPS_LOG_HEAD_LEN )
   {
      printf ( "Log file %s is %lld bytes, which is smaller than log file head",
               logFileName, fileSize ) ;
      rc = SDB_DPS_CORRUPTED_LOG ;
      goto error ;
   }

   if ( ( fileSize - DPS_LOG_HEAD_LEN ) % DPS_DEFAULT_PAGE_SIZE != 0 )
   {
      printf ( "Log file %s is %lld bytes, which is not aligned with page size",
               logFileName, fileSize ) ;
      rc = SDB_DPS_CORRUPTED_LOG ;
      goto error ;
   }

   pBuffer = (CHAR*)SDB_OSS_MALLOC ( fileSize ) ;
   if ( !pBuffer )
   {
      printf ( "Failed to allocate memory for %lld bytes\n", fileSize ) ;
      rc = SDB_OOM ;
      goto error ;
   }

   restLen = fileSize ;
   while ( restLen > 0 )
   {
      rc = ossRead ( &file, pBuffer + readPos, restLen, &fileRead ) ;
      if ( rc && SDB_INTERRUPT != rc )
      {
         printf ( "Failed to read from file, expect %lld bytes, "
                  "actual read %lld bytes, rc = %d\n", fileSize, fileRead, rc ) ;
         goto error ;
      }
      rc = SDB_OK ;
      restLen -= fileRead ;
      readPos += fileRead ;
   }
   pCur = pBuffer ;
   if ( DPS_LOG_HEAD_LEN * LOG_BUFFER_FORMAT_MULTIPLIER > outputBufferSz )
   {
      CHAR *pOrgBuff = pOutputBuffer ;
      pOutputBuffer = (CHAR*)SDB_OSS_REALLOC
            ( pOutputBuffer, DPS_LOG_HEAD_LEN * LOG_BUFFER_FORMAT_MULTIPLIER ) ;
      if ( !pOutputBuffer )
      {
         printf ( "Failed to allocate memory for %d bytes\n",
                  DPS_LOG_HEAD_LEN * LOG_BUFFER_FORMAT_MULTIPLIER ) ;
         pOutputBuffer = pOrgBuff ;
         rc = SDB_OOM ;
         goto error ;
      }
      outputBufferSz = DPS_LOG_HEAD_LEN * LOG_BUFFER_FORMAT_MULTIPLIER ;
   }

   dpsDump::dumpLogFileHead( pCur, DPS_LOG_HEAD_LEN, pOutputBuffer,
                             outputBufferSz,
                             DPS_DMP_OPT_HEX|DPS_DMP_OPT_HEX_WITH_ASCII|
                             DPS_DMP_OPT_FORMATTED ) ;
   printf ( "%s\n", pOutputBuffer ) ;
   logHeader = ( _dpsLogHeader*)pCur ;
   if ( DPS_INVALID_LOG_FILE_ID != logHeader->_logID )
   {
      UINT64 beginOffset = logHeader->_firstLSN.offset ;
      beginOffset = beginOffset % ( fileSize - DPS_LOG_HEAD_LEN ) ;
      pCur += beginOffset ;
   }
   pCur += DPS_LOG_HEAD_LEN ;
   while ( pCur < pBuffer + fileSize )
   {
      dpsLogRecord record ;
      rc = record.load( pCur ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to load log:%d",rc ) ;
         goto error ;
      }
      if ( record.head()._length * LOG_BUFFER_FORMAT_MULTIPLIER >
           outputBufferSz )
      {
         CHAR *pOrgBuff = pOutputBuffer ;
         pOutputBuffer = (CHAR*)SDB_OSS_REALLOC
               ( pOutputBuffer,
                 record.head()._length * LOG_BUFFER_FORMAT_MULTIPLIER ) ;
         if ( !pOutputBuffer )
         {
            printf ( "Failed to allocate memory for %d bytes\n",
                     ( record.head()._length *
                       LOG_BUFFER_FORMAT_MULTIPLIER ) ) ;
            pOutputBuffer = pOrgBuff ;
            rc = SDB_OOM ;
            goto error ;
         }
         outputBufferSz = record.head()._length *
                          LOG_BUFFER_FORMAT_MULTIPLIER ;
      }
      record.dump ( pOutputBuffer, outputBufferSz,
                    DPS_DMP_OPT_HEX|DPS_DMP_OPT_HEX_WITH_ASCII|
                    DPS_DMP_OPT_FORMATTED ) ;
      printf ( "%s\n", pOutputBuffer ) ;
      pCur += record.head()._length ;
   }
done :
   if ( pBuffer )
      SDB_OSS_FREE ( pBuffer ) ;
   if ( pOutputBuffer )
      SDB_OSS_FREE ( pOutputBuffer ) ;
   if ( opened )
      ossClose ( file ) ;
   PD_TRACE_EXITRC ( SDB_FORMATLOG, rc );
   return rc ;
error :
   goto done ;
}

void printSyntax ( CHAR *name )
{
   printf ( "Syntax: %s <["ARG_FROM" <from> "ARG_TO" <to>]|"
            "["ARG_LOG" <id>]>\n", name ) ;
}

#define ARG_EXPECT_UNKNOWN 0
#define ARG_EXPECT_RANGE   1
#define ARG_EXPECT_LOG     2
// PD_TRACE_DECLARE_FUNCTION ( SDB_PARSEARG, "parseArg" )
INT32 parseArg ( int argc, char **argv )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_PARSEARG );
   INT32 expect = ARG_EXPECT_UNKNOWN ;
   INT32 from = -1 ;
   INT32 to   = -1 ;
   for ( INT32 count = 0; count < argc; ++count )
   {
      if ( 0 == ossStrncmp ( argv[count], ARG_FROM, ossStrlen ( ARG_FROM ) ) )
      {
         if ( expect == ARG_EXPECT_LOG )
         {
            printf ( "Cannot specify both range and log\n" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         ++count ;
         if ( count >= argc )
         {
            printf ( "Log ID must follow "ARG_FROM"\n" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         from = atoi ( argv[count] ) ;
         expect = ARG_EXPECT_RANGE ;
      }
      else if ( 0 == ossStrncmp ( argv[count], ARG_TO,
                                  ossStrlen ( ARG_TO ) ) )
      {
         if ( expect == ARG_EXPECT_LOG )
         {
            printf ( "Cannot specify both range and log\n" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         ++count ;
         if ( count >= argc )
         {
            printf ( "Log ID must follow "ARG_TO"\n" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         to = atoi ( argv[count] ) ;
         expect = ARG_EXPECT_RANGE ;
      }
      else if ( 0 == ossStrncmp ( argv[count], ARG_LOG,
                                  ossStrlen ( ARG_LOG ) ) )
      {
         if ( expect == ARG_EXPECT_RANGE )
         {
            printf ( "Cannot specify both range and log\n" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         ++count ;
         if ( count >= argc )
         {
            printf ( "Log ID must follow "ARG_TO"\n" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         logID.push_back ( atoi ( argv[count] ) ) ;
         expect = ARG_EXPECT_LOG ;
      }
      else if ( count != 0 )
      {
         printf ( "Unknown option : %s\n", argv[count] ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   }
   if ( ARG_EXPECT_RANGE == expect )
   {
      if ( from < 0 || to < 0 )
      {
         printf ( "Both "ARG_FROM" and "ARG_TO" have to be specified and "
                  "greater or equal to 0\n" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( from > to )
      {
         printf ( "From must be less or equal to to: from %d, to %d\n",
                  from, to ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      for ( INT32 count = from; count <= to; ++count )
      {
         logID.push_back ( count ) ;
      }
   }
   if ( logID.size() == 0 )
   {
      printf ( "No log has been specified to format\n" ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   printf ( "Prepare to format log [ " ) ;
   for ( UINT32 count = 0; count < logID.size(); ++count )
   {
      printf ( "%d", logID[count] ) ;
      if ( count < logID.size()-1 )
         printf ( ", " ) ;
   }
   printf ( " ]\n" ) ;
done :
   PD_TRACE_EXITRC ( SDB_PARSEARG, rc );
   return rc ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DPSDUMP_MAIN, "main" )
int main ( int argc, char** argv )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_DPSDUMP_MAIN );
   rc = parseArg ( argc, argv ) ;
   if ( rc )
   {
      printSyntax ( argv[0] ) ;
   }
   for ( UINT32 i = 0; i < logID.size(); ++i )
   {
      rc = formatLog ( logID[i] ) ;
      if ( rc )
      {
         printf ( "Failed to format log %d\n", logID[i] ) ;
         PD_TRACE_EXIT ( SDB_DPSDUMP_MAIN );
         exit ( 0 ) ;
      }
   }
   PD_TRACE_EXIT ( SDB_DPSDUMP_MAIN );
   return 0 ;
}

