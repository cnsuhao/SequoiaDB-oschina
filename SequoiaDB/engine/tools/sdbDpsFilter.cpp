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

   Source File Name = sdbDpsFilter.cpp

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
#include "sdbDpsFilter.hpp"
#include "dpsLogRecordDef.hpp"
#include "dpsLogRecord.hpp"
#include "pdTrace.hpp"
#include "toolsTrace.h"
#include "dpsLogFile.hpp"
#include "ossIO.hpp"
#include "dpsDump.hpp"
#include "sdbDpsLogFilter.hpp"
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

using namespace engine ;
namespace fs = boost::filesystem ;

#define NONE_LSN_FILTER -1
#define BLOCK_SIZE 64 * 1024
#define DPS_LOG_FILE_INVALID 1

_dpsFilterFactory::_dpsFilterFactory()
{
}

_dpsFilterFactory::~_dpsFilterFactory()
{
   std::list< iFilter * >::iterator it = _filterList.begin() ;
   for ( ; it != _filterList.end() ; ++it )
   {
      release( *it ) ;
   }

   _filterList.clear() ;
}

_dpsFilterFactory* _dpsFilterFactory::getInstance()
{
   static _dpsFilterFactory factory ;
   return &factory ;
}

iFilter* _dpsFilterFactory::createFilter( int type )
{
   iFilter *filter = NULL ;

   switch( type )
   {
   case SDB_LOG_FILTER_TYPE :
      {
         filter = SDB_OSS_NEW dpsTypeFilter() ;
         break ;
      }
   case SDB_LOG_FILTER_NAME :
      {
         filter = SDB_OSS_NEW dpsNameFilter() ;
         break ;
      }
   case SDB_LOG_FILTER_LSN  :
      {
         filter = SDB_OSS_NEW dpsLsnFilter() ;
         break ;
      }
   case SDB_LOG_FILTER_META :
      {
         filter = SDB_OSS_NEW dpsMetaFilter() ;
         break ;
      }
   case SDB_LOG_FILTER_NONE :
      {
         filter = SDB_OSS_NEW dpsNoneFilter() ;
         break ;
      }
   case SDB_LOG_FILTER_LAST :
      {
         filter = SDB_OSS_NEW dpsLastFilter() ;
         break ;
      }

   default:
      printf("error filter type...\n") ;
   }

   if( NULL == filter )
   {
      printf("Unable to allocate filter.\n") ;
      goto error ;
   }
   _filterList.push_back( filter ) ;

done:
   return filter ;
error:
   goto done ;
}

void _dpsFilterFactory::release( iFilter *filter )
{
   if( NULL != filter )
   {
      SDB_OSS_DEL( filter ) ;
      filter = NULL ;
   }
}

namespace
{
   INT32 checkLogFile( OSSFILE& file, INT64& size, const CHAR *filename )
   {
      SDB_ASSERT( filename, "filename cannot null" ) ;

      INT32 rc = SDB_OK ;
      rc = ossGetFileSize( &file, &size ) ;
      if( rc )
      {
         printf( "Failed to get file size: %s, rc = %d\n", filename, rc ) ;
         goto error ;
      }
      if( size < DPS_LOG_HEAD_LEN )
      {
         printf( "Log file %s is %lld bytes, "
                 "which is smaller than log file head\n",
                 filename, size ) ;
         rc = SDB_DPS_CORRUPTED_LOG ;
         goto error ;
      }

      if(( size - DPS_LOG_HEAD_LEN ) % DPS_DEFAULT_PAGE_SIZE != 0 )
      {
         printf( "Log file %s is %lld bytes, "
                  "which is not aligned with page size\n",
                  filename, size ) ;
         rc = SDB_DPS_CORRUPTED_LOG ;
         goto error ;
      }

   done:
      return rc;
   error:
      goto done;
   }

   INT32 readLogHead( OSSFILE& in, INT64& offset, const INT64 fileSize,
                      CHAR *pOutBuffer, const INT64 bufferSize,
                      CHAR *pOutHeader, INT64& outLen )
   {
      SDB_ASSERT( fileSize > DPS_LOG_HEAD_LEN,
                  "fileSize must gt than DPS_LOG_HEAD_LEN" ) ;

      INT32 rc       = SDB_OK ;
      INT64 readPos  = 0 ;
      INT64 fileRead = 0 ;
      INT64 restLen  = DPS_LOG_HEAD_LEN ;
      CHAR  pBuffer[ DPS_LOG_HEAD_LEN + 1 ] = { 0 } ;
      dpsLogHeader *header = NULL ;
      INT64 len      = 0 ;

      while( restLen > 0 )
      {
         rc = ossRead( &in, pBuffer + readPos, restLen, &fileRead ) ;
         if( rc && SDB_INTERRUPT != rc)
         {
            printf( "Failed to read from file, expect %lld bytes,  "
                    "actual read %lld bytes, rc = %d\n",
                    fileSize, fileRead, rc ) ;
            goto error ;
         }
         rc = SDB_OK ;
         restLen -= fileRead ;
         readPos += fileRead ;
      }

      header =( _dpsLogHeader* )pBuffer ;
      if( DPS_INVALID_LOG_FILE_ID != header->_logID )
      {
         UINT64 beginOffset = header->_firstLSN.offset ;
         beginOffset = beginOffset %( fileSize - DPS_LOG_HEAD_LEN ) ;
         offset += beginOffset ;
      }
      else
      {
         rc = DPS_LOG_FILE_INVALID ;
      }
      offset += DPS_LOG_HEAD_LEN ;

      if( pOutBuffer )
      {
        len = dpsDump::dumpLogFileHead( pBuffer, DPS_LOG_HEAD_LEN, pOutBuffer,
                                   bufferSize,
                                   DPS_DMP_OPT_HEX |
                                   DPS_DMP_OPT_HEX_WITH_ASCII |
                                   DPS_DMP_OPT_FORMATTED ) ;
      }

      if ( pOutHeader )
      {
         ossMemcpy( pOutHeader, pBuffer, DPS_LOG_HEAD_LEN );
      }

   done:
      outLen = len ;
      return rc ;
   error:
      goto done ;
   }

   INT32 readRecordHead( OSSFILE& in, const INT64 offset, const INT64 fileSize,
                         CHAR *pRecordHeadBuffer )
   {
      SDB_ASSERT( offset < fileSize, "offset out of range " ) ;
      SDB_ASSERT( pRecordHeadBuffer, "OutBuffer cannot be NULL " ) ;

      INT32 rc       = SDB_OK ;
      INT64 readPos  = 0 ;
      INT64 restLen  = sizeof( dpsLogRecordHeader ) ;
      INT64 fileRead = 0 ;
      dpsLogRecordHeader *header = NULL ;

      while( restLen > 0 )
      {
         rc = ossSeekAndRead( &in, offset, pRecordHeadBuffer + readPos,
                              restLen, &fileRead ) ;
         if( rc && SDB_INTERRUPT != rc)
         {
            printf( "Failed to read from file, expect %lld bytes, "
                    "actual read %lld bytes, rc = %d\n",
                    restLen, fileRead, rc ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         rc = SDB_OK ;
         restLen -= fileRead ;
         readPos += fileRead ;
      }

      header = ( dpsLogRecordHeader * )pRecordHeadBuffer ;
      if ( header->_length < sizeof( dpsLogRecordHeader) ||
           header->_length > DPS_RECORD_MAX_LEN )
      {
         rc = SDB_DPS_CORRUPTED_LOG ;
         goto error ;
      }

      if ( LOG_TYPE_DUMMY == header->_type )
      {
         goto done ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 readRecord( OSSFILE& in, const INT64 offset, const INT64 fileSize,
                     const INT64 readLen, CHAR *pOutBuffer )
   {
      INT32 rc = SDB_OK ;

      SDB_ASSERT( offset < fileSize, "offset out of range " ) ;
      SDB_ASSERT( readLen > 0, "readLen lt 0!!" ) ;
      INT64 restLen  = 0 ;
      INT64 readPos  = 0 ;
      INT64 fileRead = 0;

      restLen = readLen ;
      while( restLen > 0 )
      {
         rc = ossSeekAndRead( &in, offset, pOutBuffer + readPos,
                              restLen, &fileRead ) ;
         if( rc && SDB_INTERRUPT != rc)
         {
            printf( "Failed to read from file, expect %lld bytes, "
                    "actual read %lld bytes, rc = %d\n",
                    restLen, fileRead, rc ) ;
            goto error ;
         }
         rc = SDB_OK ;
         restLen -= fileRead ;
         readPos += fileRead ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 writeToFile( OSSFILE& out, CHAR *pBuffer )
   {
      INT32 rc        = SDB_OK ;
      INT64 len       = ossStrlen( pBuffer ) ;
      CHAR  *pEnter   = OSS_NEWLINE ;
      INT64 restLen   = len ;
      INT64 writePos  = 0 ;
      INT64 writeSize = 0 ;
      while( restLen > 0 )
      {
         rc = ossWrite( &out, pBuffer + writePos, len, &writeSize ) ;
         if( rc && SDB_INTERRUPT != rc )
         {
            printf( "Failed to write data to file\n" ) ;
            goto error ;
         }
         rc = SDB_OK ;
         restLen -= writeSize ;
         writePos += writeSize ;
      }

      len = ossStrlen( OSS_NEWLINE ) ;
      restLen = len ;
      writePos = 0 ;
      while( restLen > 0 )
      {
         rc = ossWrite( &out, pEnter, ossStrlen( OSS_NEWLINE ), &writeSize ) ;
         if( rc && SDB_INTERRUPT != rc )
         {
            printf( "Failed to write data to file\n" ) ;
            goto error ;
         }
         rc = SDB_OK ;
         restLen -= writeSize ;
         writePos += writeSize ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 seekToLsnMatched( OSSFILE& in, INT64 &offset, const INT64 fileSize,
                           INT32 &prevCount )
   {
      SDB_ASSERT( offset >= DPS_LOG_HEAD_LEN, "offset lt DPS_LOG_HEAD_LEN" ) ;
      INT32 rc     = SDB_OK ;
      INT64 newOff = 0 ;
      INT32 count  = prevCount ;

      CHAR pRecordHead[ sizeof( dpsLogRecordHeader ) + 1 ] = { 0 } ;
      dpsLogRecordHeader *header = NULL ;

      if( offset > fileSize || offset < DPS_LOG_HEAD_LEN )
      {
         printf( "wrong LSN position\n" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if( count < 0 )
      {
         printf( "pre-count must be gt 0\n" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      prevCount = 0 ;
      while( offset < fileSize && prevCount < count )
      {
         rc = readRecordHead( in, offset, fileSize, pRecordHead ) ;
         header = ( dpsLogRecordHeader * )pRecordHead ;
         if ( header->_length < sizeof( dpsLogRecordHeader ) ||
              header->_length > DPS_RECORD_MAX_LEN )
         {
            rc = SDB_DPS_CORRUPTED_LOG ;
            goto error ;
         }

         if ( LOG_TYPE_DUMMY == header->_type )
         {
            printf( "Error: Lsn input is invalid\n" );
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         newOff = header->_lsn % ( fileSize - DPS_LOG_HEAD_LEN ) ;
         if( 0 == newOff || DPS_LOG_INVALID_LSN ==  header->_preLsn )
         {
            rc = DPS_LOG_REACH_HEAD;
            goto done ;
         }
         offset = DPS_LOG_HEAD_LEN +
                  header->_preLsn % ( fileSize - DPS_LOG_HEAD_LEN ) ;
         ++prevCount ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 seekToEnd( OSSFILE& in, INT64& offset, const INT64 fileSize )
   {
      SDB_ASSERT( offset >= DPS_LOG_HEAD_LEN, "offset lt DPS_LOG_HEAD_LEN" ) ;
      INT32 rc    = SDB_OK ;
      INT64 prevOffset = offset ;

      CHAR pRecordHead[ sizeof( dpsLogRecordHeader ) + 1 ] = { 0 } ;
      dpsLogRecordHeader *header = NULL;

      if( offset > fileSize || offset < DPS_LOG_HEAD_LEN )
      {
         printf( "wrong LSN position\n" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      while( offset < fileSize )
      {
         rc = readRecordHead( in, offset, fileSize, pRecordHead ) ;
         header = ( dpsLogRecordHeader * )pRecordHead ;
         if ( header->_length < sizeof( dpsLogRecordHeader ) ||
              header->_length > DPS_RECORD_MAX_LEN )
         {
            rc = SDB_DPS_CORRUPTED_LOG ;
            goto error ;
         }

         if ( LOG_TYPE_DUMMY == header->_type )
         {
            break ;
         }

         prevOffset = offset ;
         offset += header->_length ;
      }

   done:
      offset = prevOffset ;
      return rc ;
   error:
      goto done ;

   }

   INT32 filte( iFilter *filter, const dpsCmdData *data,
                    OSSFILE& out, const CHAR *filename )
   {
      SDB_ASSERT( filter, "filter is NULL" ) ;
      SDB_ASSERT( filename, "filename cannot be NULL" ) ;

      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_FORMATLOG ) ;
      OSSFILE in ;

      CHAR pLogHead[ sizeof( dpsLogHeader ) + 1 ] = { 0 } ;
      CHAR pRecordHead[ sizeof( dpsLogRecordHeader ) + 1 ] = { 0 } ;
      CHAR *pRecordBuffer = NULL ;
      INT64 recordLength  = 0 ;
      INT32 ahead         = data->lsnAhead ;
      INT32 back          = data->lsnBack ;
      INT32 totalCount    = NONE_LSN_FILTER ; //ahead + back ;

      BOOLEAN opened      = FALSE ;
      INT64 fileSize      = 0 ;
      INT64 offset        = 0 ;
      CHAR *pOutBuffer    = NULL ; ///< buffer for formatted log
      INT64 outBufferSize = 0 ;
      INT64 len           = 0 ;
      BOOLEAN printLogHead= FALSE ;

      CHAR parseBegin[ BLOCK_SIZE ] = { 0 } ;
      len  = ossSnprintf( parseBegin, BLOCK_SIZE, OSS_NEWLINE""OSS_NEWLINE ) ;
      len += ossSnprintf( parseBegin + len, BLOCK_SIZE - len,
                          "parse file : [%s]"OSS_NEWLINE, filename ) ;

      rc = ossOpen( filename, OSS_DEFAULT | OSS_READONLY,
                    OSS_RU | OSS_WU | OSS_RG, in ) ;
      if( rc )
      {
         printf( "Unable to open file: %s. rc = %d\n", filename, rc ) ;
         goto error ;
      }

      opened = TRUE ;

      rc = checkLogFile( in, fileSize, filename ) ;
      if( rc )
      {
         goto error ;
      }
      SDB_ASSERT( fileSize > 0, "fileSize must be gt 0" ) ;
      len = DPS_LOG_HEAD_LEN * LOG_BUFFER_FORMAT_MULTIPLIER ;

   retry_head:
      if( len > outBufferSize )
      {
         CHAR *pOrgBuff = pOutBuffer ;
         pOutBuffer =(CHAR*)SDB_OSS_REALLOC ( pOutBuffer, len + 1 ) ;
         if( !pOutBuffer )
         {
            printf( "Failed to allocate memory for %lld bytes\n",
                     len + 1 ) ;
            pOutBuffer = pOrgBuff ;
            rc = SDB_OOM ;
            goto error ;
         }
         outBufferSize = len ;
         ossMemset( pOutBuffer, 0, len + 1 ) ;
      }

      rc = readLogHead( in, offset, fileSize,  pOutBuffer, outBufferSize,
                        pLogHead, len ) ;
      if( rc && DPS_LOG_FILE_INVALID != rc )
      {
         goto error ;
      }
      if( len >= outBufferSize )
      {
         len += BLOCK_SIZE ;
         goto retry_head ;
      }

      if( SDB_LOG_FILTER_LSN == filter->getType() )
      {
         dpsLogHeader *logHeader = ( dpsLogHeader * )pLogHead ;
         if( ( logHeader->_firstLSN.offset > data->lsn ) ||
             ( data->lsn >= logHeader->_firstLSN.offset +
                            fileSize - DPS_LOG_HEAD_LEN ) )
         {
            goto done ;
         }

         offset = DPS_LOG_HEAD_LEN +
                  data->lsn % ( fileSize - DPS_LOG_HEAD_LEN ) ;
         rc = seekToLsnMatched( in, offset, fileSize, ahead ) ;
         if( rc && DPS_LOG_REACH_HEAD != rc )
         {
            printf( "the lsn offset: %lld in file : [%s] is invalid\n",
                     data->lsn, filename ) ;
            goto error ;
         }
         totalCount = ahead + back + 1 ;
         if( DPS_LOG_REACH_HEAD == rc )
         {
            offset = DPS_LOG_HEAD_LEN ;
         }
      }
      else if( SDB_LOG_FILTER_LAST == filter->getType() )
      {
         INT32 recordCount = data->lastCount - 1 ;
         offset = DPS_LOG_HEAD_LEN ;
         seekToEnd( in, offset, fileSize ) ;
         rc = seekToLsnMatched( in, offset, fileSize, recordCount ) ;
         if( rc && DPS_LOG_REACH_HEAD != rc )
         {
            goto error ;
         }
         totalCount = recordCount + 1 ;
         if( DPS_LOG_REACH_HEAD == rc )
         {
            offset = DPS_LOG_HEAD_LEN ;
         }
      }

      while( offset < fileSize &&
           ( NONE_LSN_FILTER  == totalCount || totalCount > 0 ) )
      {
         rc = readRecordHead( in, offset, fileSize, pRecordHead ) ;
         if( rc && SDB_DPS_CORRUPTED_LOG != rc )
         {
            goto error ;
         }

         dpsLogRecordHeader *header = ( dpsLogRecordHeader *)pRecordHead ;
         if( SDB_DPS_CORRUPTED_LOG == rc && header->_length == 0 )
         {
            goto error ;
         }

         if( header->_length > recordLength )
         {
            pRecordBuffer = ( CHAR * )SDB_OSS_REALLOC (
                              pRecordBuffer, header->_length + 1 );
            if( !pRecordBuffer )
            {
               rc = SDB_OOM;
               printf( "Failed to allocate %d bytes\n",
                       header->_length + 1 ) ;
               goto error ;
            }
            recordLength = header->_length ;
         }
         ossMemset( pRecordBuffer, 0, recordLength ) ;
         rc = readRecord( in, offset, fileSize, header->_length,
                          pRecordBuffer ) ;
         if( rc )
         {
            goto error;
         }

         if( !filter->match( data, pRecordBuffer ) )
         {
            offset += header->_length ;
            if( SDB_LOG_FILTER_LSN == filter->getType() )
            {
               --totalCount ;
            }
            continue ;
         }

         if( !printLogHead )
         {
            printLogHead = TRUE ;
            if( data->output )
            {
               printf( "%s\n", parseBegin ) ;
               printf( "%s\n", pOutBuffer ) ;
            }
            else
            {
               rc = writeToFile( out, parseBegin ) ;
               if( rc )
               {
                  goto error ;
               }
               rc = writeToFile ( out, pOutBuffer ) ;
               if( rc )
               {
                  goto error ;
               }
            }
         }

         dpsLogRecord record ;
         record.load( pRecordBuffer ) ;
         len = recordLength * LOG_BUFFER_FORMAT_MULTIPLIER ;

      retry_record:
         if( len > outBufferSize )
         {
            CHAR *pOrgBuff = pOutBuffer ;
            pOutBuffer =(CHAR*)SDB_OSS_REALLOC( pOutBuffer, len + 1 ) ;
            if( !pOutBuffer )
            {
               printf( "Failed to allocate memory for %lld bytes\n",
                       len + 1 ) ;
               pOutBuffer = pOrgBuff ;
               rc = SDB_OOM ;
               goto error ;
            }
            outBufferSize = len ;
            ossMemset( pOutBuffer, 0, len + 1 ) ;
         }

         len = record.dump( pOutBuffer, outBufferSize,
                      DPS_DMP_OPT_HEX | DPS_DMP_OPT_HEX_WITH_ASCII |
                      DPS_DMP_OPT_FORMATTED ) ;
         if( len >= outBufferSize )
         {
            len += BLOCK_SIZE ;
            goto retry_record ;
         }

         if( data->output )
         {
            printf( "%s\n", pOutBuffer ) ;
         }
         else
         {
            rc = writeToFile( out, pOutBuffer ) ;
            if( rc )
            {
               goto error ;
            }
         }

         offset += header->_length ;
         if( ( SDB_LOG_FILTER_LSN  == filter->getType() )
          || ( SDB_LOG_FILTER_LAST == filter->getType() ) )
         {
            --totalCount ;
         }
      }

   done:
      if( pRecordBuffer )
         SDB_OSS_FREE( pRecordBuffer ) ;
      if( pOutBuffer )
         SDB_OSS_FREE( pOutBuffer ) ;
      if( opened )
         ossClose( in ) ;
      PD_TRACE_EXITRC( SDB_FORMATLOG, rc ) ;
      return rc ;

   error:
      goto done ;
   }

   INT32 metaFilte( dpsMetaData& data,  OSSFILE& out,
                    const CHAR *filename, INT32 index )
   {
      SDB_ASSERT( filename, "filename cannot be NULL ") ;

      INT32 rc = SDB_OK  ;
      PD_TRACE_ENTRY( SDB_FORMATLOG ) ;
      OSSFILE in ;
      CHAR pRecordHead[ sizeof( dpsLogRecordHeader ) + 1 ] = { 0 } ;
      CHAR pLogHead[ DPS_LOG_HEAD_LEN + 1 ] = { 0 } ;
      BOOLEAN opened             = FALSE ;
      INT64 fileSize             = 0 ;
      INT64 offset               = 0 ;
      dpsLogHeader *logHeader    = NULL ;
      dpsLogRecordHeader *header = NULL ;
      INT64 len                  = 0 ;
      INT64 totalRecordSize      = 0 ;
      UINT64 preLsn              = 0 ;

      printf("Parse file:[ %s ] begin\n", filename ) ;
      rc = ossOpen( filename, OSS_DEFAULT | OSS_READONLY,
                    OSS_RU | OSS_WU | OSS_RG, in ) ;
      if( rc )
      {
         printf( "Unable to open file: %s. rc = %d\n", filename, rc ) ;
         goto error ;
      }
      opened = TRUE ;

      rc = checkLogFile( in, fileSize, filename );
      if( rc )
      {
         goto error ;
      }
      SDB_ASSERT( fileSize > 0, "fileSize must be gt 0" ) ;

      rc = readLogHead( in, offset, fileSize, NULL, 0, pLogHead, len ) ;
      if( rc && DPS_LOG_FILE_INVALID != rc )
      {
         goto error;
      }
      totalRecordSize = fileSize - DPS_LOG_HEAD_LEN ;
      logHeader = (dpsLogHeader*)pLogHead ;

      dpsFileMeta meta ;
      meta.index     = index ;
      meta.logID     = logHeader->_logID ;
      meta.firstLSN  = logHeader->_firstLSN.offset ;
      meta.lastLSN   = logHeader->_firstLSN.offset ;

      if( DPS_LOG_INVALID_LSN != logHeader->_firstLSN.offset )
      {
         while ( offset < fileSize )
         {
            rc = readRecordHead( in, offset, fileSize, pRecordHead ) ;
            if( rc && SDB_DPS_CORRUPTED_LOG != rc )
            {
               goto error ;
            }

            header = ( dpsLogRecordHeader * )pRecordHead ;
            if( SDB_DPS_CORRUPTED_LOG == rc )
            {
               printf( "Warning: Record was corruptrd with length %d\n",
                       header->_length ) ;
               rc = SDB_OK ;
               break ;
            }
            if( LOG_TYPE_DUMMY == header->_type )
            {
               meta.expectLSN = header->_lsn + header->_length ;
               meta.lastLSN = header->_lsn ;
               meta.validSize = header->_lsn % totalRecordSize ;
               meta.restSize = totalRecordSize -
                               ( meta.validSize + header->_length ) ;
               break;
            }

            if( preLsn > header->_lsn )
            {
               meta.expectLSN = logHeader->_firstLSN.offset +
                                offset - DPS_LOG_HEAD_LEN ;
               meta.lastLSN = preLsn ;
               meta.validSize = offset - DPS_LOG_HEAD_LEN ;
               meta.restSize = totalRecordSize - meta.validSize ;
               break ;
            }

            preLsn = header->_lsn ;
            offset += header->_length ;
         }
      }
      else
      {
         meta.validSize = 0 ;
         meta.restSize = totalRecordSize ;
      }

      data.metaList.push_back( meta ) ;

   done:
      if( opened )
         ossClose( in ) ;
      printf("Parse file:[ %s ] end\n\n", filename ) ;

      PD_TRACE_EXITRC( SDB_FORMATLOG, rc ) ;
      return rc ;

   error:
      goto done ;
   }

   UINT64 analysisMetaData( dpsMetaData& metaData,
                           CHAR *pOutBuffer, const UINT64 outBufferSize )
   {
      SDB_ASSERT( pOutBuffer, "pOutBuffer cannot be NULL " ) ;
      UINT64 len      = 0 ;
      UINT32 begin    = DPS_INVALID_LOG_FILE_ID ;
      UINT32 work     = 0 ;
      UINT32 idx      = 0 ;
      UINT32 beginIdx = 0 ;
      while( idx < metaData.metaList.size() )
      {
         const dpsFileMeta &meta = metaData.metaList[ idx ] ;
         if( DPS_INVALID_LOG_FILE_ID == meta.logID )
         {
            ++idx ;
            continue ;
         }

         if( DPS_INVALID_LOG_FILE_ID == begin
             || ( meta.logID < begin &&
                  begin - meta.logID < DPS_INVALID_LOG_FILE_ID / 2 )
             || ( meta.logID > begin &&
                  meta.logID - begin >  DPS_INVALID_LOG_FILE_ID / 2 ) )
         {
            metaData.fileBegin = meta.index ;
            begin = meta.logID;
            beginIdx = idx ;
         }
         ++idx ;
      }

      idx = 0 ;
      work = beginIdx ;
      while( 0 == metaData.metaList[ work ].restSize &&
             idx < metaData.metaList.size() )
      {
         metaData.fileWork = work ;
         ++work ;
         if( work > metaData.fileCount )
         {
            work = 0;
         }
         ++idx ;
      }
      if( DPS_INVALID_LOG_FILE_ID != metaData.metaList[ work ].logID )
      {
         metaData.fileWork = work ;
      }

      len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                         "======================================="OSS_NEWLINE
                         ) ;
      len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                         "    Log Files in total: %d"OSS_NEWLINE,
                         metaData.fileCount ) ;
      len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                         "    LogFile begin     : sequoiadbLog.%d"OSS_NEWLINE,
                         metaData.fileBegin ) ;
      len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                         "    LogFile work      : sequoiadbLog.%d"OSS_NEWLINE,
                         metaData.fileWork ) ;
      len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                         "        begin Lsn     : 0x%08lx"OSS_NEWLINE,
                         metaData.metaList[ beginIdx ].firstLSN ) ;
      len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                         "        current Lsn   : 0x%08lx"OSS_NEWLINE,
                         metaData.metaList[ work ].lastLSN ) ;
      len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                         "        expect Lsn    : 0x%08lx"OSS_NEWLINE,
                         ( metaData.metaList[ work ].expectLSN ) ) ;
      len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                         "======================================="OSS_NEWLINE
                         ) ;
      for( idx = 0; idx < metaData.metaList.size(); ++idx )
      {
         const dpsFileMeta& meta = metaData.metaList[ idx ] ;
         len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                OSS_NEWLINE"Log File Name: sequoiadbLog.%d"OSS_NEWLINE,
                meta.index ) ;
         len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                "Logic ID     : %d"OSS_NEWLINE, meta.logID ) ;
         len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                "First LSN    : 0x%08lx"OSS_NEWLINE, meta.firstLSN ) ;
         len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                "Last  LSN    : 0x%08lx"OSS_NEWLINE, meta.lastLSN ) ;
         len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                "Valid Size   : %lld bytes"OSS_NEWLINE, meta.validSize ) ;
         len += ossSnprintf( pOutBuffer + len, outBufferSize - len,
                "Rest Size    : %lld bytes"OSS_NEWLINE, meta.restSize ) ;
      }

      return len ;
   }
}

BOOLEAN _dpsTypeFilter::match( const dpsCmdData *data, CHAR *pRecord )
{
   BOOLEAN rc = FALSE ;
   dpsLogRecordHeader *pHeader =(dpsLogRecordHeader*)( pRecord ) ;
   if( pHeader->_type == data->type )
   {
      rc = iFilter::match( data, pRecord ) ;
      goto done ;
   }

done:
   return rc ;
}

INT32 _dpsTypeFilter::doFilte( const dpsCmdData *data, OSSFILE &out,
                               const CHAR *logFilePath )
{
   INT32 rc = SDB_OK ;

   rc = filte( this, data, out, logFilePath ) ;
   if( rc )
   {
      goto error ;
   }

done:
   return rc;

error:
   goto done;
}

BOOLEAN _dpsNameFilter::match( const dpsCmdData *data, CHAR *pRecord )
{
   BOOLEAN rc = FALSE ;
   dpsLogRecord record ;
   record.load( pRecord ) ;
   dpsLogRecord::iterator itr = record.find( DPS_LOG_PULIBC_FULLNAME ) ;

   if( 0 == ossStrncmp( data->inputName, "", sizeof( data->inputName ) ) )
   {
      rc = iFilter::match( data, pRecord ) ;
      goto done ;
   }

   if( itr.valid() )
   {
      if( NULL != ossStrstr( itr.value(), data->inputName ) )
      {
         rc = iFilter::match( data, pRecord ) ;
         goto done ;
      }
   }

done:
   return rc ;
}

INT32 _dpsNameFilter::doFilte( const dpsCmdData *data, OSSFILE &out,
                               const CHAR *logFilePath )
{
   INT32 rc = SDB_OK ;

   rc = filte( this, data, out, logFilePath ) ;
   if( rc )
   {
      goto error ;
   }

done:
   return rc;

error:
   goto done;
}

BOOLEAN _dpsMetaFilter::match( const dpsCmdData *data, CHAR *pRecord )
{
   return FALSE ;
}

INT32 _dpsMetaFilter::doFilte( const dpsCmdData *data, OSSFILE &out,
                               const CHAR *logFilePath )
{
   INT32 rc             = SDB_OK ;
   UINT64 len           = 0 ;
   UINT64 outBufferSize = BLOCK_SIZE ;
   CHAR *pOutBuffer     = NULL ;
   BOOLEAN start        = FALSE ;
   dpsMetaData metaData ;
   if( dpsLogFilter::isDir( data->srcPath ) )
   {
      INT32 const MAX_FILE_COUNT =
                  _dpsLogFilter::getFileCount( data->srcPath ) ;
      if( 0 == MAX_FILE_COUNT )
      {
         printf( "Cannot find any Log files\nPlease check"
                 " and input the correct log file path\n" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      printf("Analysis Log File Data begin...\n\n" ) ;
      start = TRUE ;
      for( INT32 idx = 0 ; idx < MAX_FILE_COUNT ; ++idx )
      {
         fs::path fileDir( data->srcPath ) ;
         const CHAR *filepath = fileDir.string().c_str() ;
         CHAR filename[ OSS_MAX_PATHSIZE * 2 ] = { 0 } ;
         ossSnprintf( filename, OSS_MAX_PATHSIZE, "%s/sequoiadbLog.%d",
                      filepath, idx ) ;

         if( !dpsLogFilter::isFileExisted( filename ) )
         {
            printf( "Warning: file:[%s] is missing\n", filename ) ;
            continue ;
         }

         rc = metaFilte( metaData, out, filename, idx ) ;
         if( rc )
         {
            printf( "!parse log file: [%s] error, rc = %d\n", filename, rc ) ;
            continue ;
         }
      }
      metaData.fileCount = metaData.metaList.size() ;

   retry:
      pOutBuffer = ( CHAR * )SDB_OSS_REALLOC( pOutBuffer , outBufferSize + 1 ) ;
      if( NULL == pOutBuffer )
      {
         printf( "Failed to allocate %lld bytes, LINE:%d, FILE:%s\n",
                 outBufferSize + 1, __LINE__, __FILE__ ) ;
         ossMemset( pOutBuffer, 0, outBufferSize + 1 ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      if( 0 < metaData.fileCount )
      {
         len = analysisMetaData( metaData, pOutBuffer, outBufferSize ) ;
         if( len >= outBufferSize )
         {
            outBufferSize += BLOCK_SIZE ;
            goto retry ;
         }

         if( data->output )
         {
            printf( "%s", pOutBuffer ) ;
         }
         else
         {
            rc = writeToFile( out, pOutBuffer ) ;
            if( rc )
            {
               goto error ;
            }
         }
      }
   }
   else
   {
      printf( "meta info need assigned a path of dir\n" ) ;
      rc = SDB_INVALIDPATH ;
      goto error ;
   }

done:
   if( start )
   {
      printf("\nAnalysis Log File Data end...\n" ) ;
   }
   return rc;
error:
   goto done;
}

BOOLEAN _dpsLsnFilter::match( const dpsCmdData *data, CHAR *pRecord )
{
   return iFilter::match( data, pRecord ) ;
}

INT32 _dpsLsnFilter::doFilte( const dpsCmdData *data, OSSFILE &out,
                              const CHAR *logFilePath )
{
   INT32 rc = SDB_OK ;

   rc = filte( this, data, out, logFilePath ) ;
   if( rc )
   {
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

BOOLEAN _dpsNoneFilter::match( const dpsCmdData *data, CHAR *pRecord )
{
   return iFilter::match( data, pRecord ) ;
}

INT32 _dpsNoneFilter::doFilte( const dpsCmdData *data, OSSFILE &out,
                               const CHAR *logFilePath )
{
   INT32 rc = SDB_OK ;

   rc = filte( this, data, out, logFilePath ) ;
   if( rc )
   {
      goto error ;
   }

done:
   return rc;

error:
   goto done;
}

BOOLEAN _dpsLastFilter::match( const dpsCmdData *data, CHAR *pRecord )
{
   return iFilter::match( data, pRecord ) ;
}

INT32 _dpsLastFilter::doFilte( const dpsCmdData *data, OSSFILE &out,
                               const CHAR *logFilePath )
{
   INT32 rc = SDB_OK ;

   rc = filte( this, data, out, logFilePath ) ;
   if( rc )
   {
      goto error ;
   }

done:
   return rc ;

error:
   goto done ;
}
