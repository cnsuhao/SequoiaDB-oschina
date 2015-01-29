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

   Source File Name = pd.cpp

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
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <string.h>
#if defined (_LINUX)
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#endif
#include "pd.hpp"
#include "oss.hpp"
#include "ossUtil.hpp"
#include "ossLatch.hpp"
#include "ossPrimitiveFileOp.hpp"
#include "utilStr.hpp"
#include "ossIO.hpp"
#include "ossPath.hpp"

#include "pdTrace.hpp"

PDLEVEL& getPDLevel()
{
   static PDLEVEL s_pdLevel = PDWARNING ;
   return s_pdLevel ;
}

PDLEVEL setPDLevel( PDLEVEL newLevel )
{
   PDLEVEL oldLevel = getPDLevel() ;
   getPDLevel() = newLevel ;
   return oldLevel ;
}

const CHAR* getPDLevelDesp ( PDLEVEL level )
{
   const static CHAR *s_PDLEVELSTRING[] =
   {
      "SEVERE",
      "ERROR",
      "EVENT",
      "WARNING",
      "INFO",
      "DEBUG"
   } ;
   if ( level >= 0 && level < (INT32)(sizeof(s_PDLEVELSTRING)/sizeof(CHAR*)) )
   {
      return s_PDLEVELSTRING[(UINT32)level] ;
   }
   return "UNKNOW" ;
}

/* private variables */
struct _pdLogFile : public SDBObject
{
   ossPrimitiveFileOp _logFile ;
   UINT64             _fileSize ;
   ossSpinXLatch _mutex ;
} ;
typedef struct _pdLogFile pdLogFile ;

enum _pdLogType
{
   PD_DIAGLOG = 0,
   PD_LOG_MAX
} ;
pdLogFile _pdLogFiles [ PD_LOG_MAX ] ;

/*
   _pdCfgInfo define
*/
typedef struct _pdCfgInfo
{
   CHAR        _pdLogFile[ OSS_MAX_PATHSIZE + 1 ] ;
   CHAR        _pdLogPath[ OSS_MAX_PATHSIZE + 1 ] ;
   INT32       _pdFileMaxNum ;
   UINT64      _pdFileMaxSize ;

   _pdCfgInfo()
   {
      _pdLogFile[0]  = 0 ;
      _pdLogPath[0]  = 0 ;
      _pdFileMaxNum  = 0 ;
      _pdFileMaxSize = 0 ;
   }

   BOOLEAN isEnabled() const
   {
      if ( _pdLogFile[0] != 0 && _pdFileMaxNum > 0 && _pdFileMaxSize > 0 )
      {
         return TRUE ;
      }
      return FALSE ;
   }
} pdCfgInfo ;

static pdCfgInfo& _getPDCfgInfo()
{
   static pdCfgInfo s_pdCfg ;
   return s_pdCfg ;
}

const CHAR* getDialogName ()
{
   return _getPDCfgInfo()._pdLogFile ;
}

const CHAR* getDialogPath ()
{
   return _getPDCfgInfo()._pdLogPath ;
}

void sdbEnablePD( const CHAR *pdPathOrFile, INT32 fileMaxNum,
                  UINT32 fileMaxSize )
{
   sdbDisablePD() ;

   pdCfgInfo &info = _getPDCfgInfo() ;
   const CHAR *shortName = PD_DFT_DIAGLOG ;

   if ( pdPathOrFile && 0 != pdPathOrFile[0] )
   {
      const CHAR *pDotStr = ossStrrchr( pdPathOrFile, '.' ) ;
      const CHAR *pSepStr1 = ossStrrchr( pdPathOrFile, '/' ) ;
      const CHAR *pSepStr2 = ossStrrchr( pdPathOrFile, '\\' ) ;
      const CHAR *pSepStr = pSepStr1 >= pSepStr2 ? pSepStr1 : pSepStr2 ;

      if ( pDotStr && pDotStr > pSepStr )
      {
         shortName = pSepStr ? pSepStr + 1 : pdPathOrFile ;
         ossStrncpy( info._pdLogPath, pdPathOrFile, shortName - pdPathOrFile ) ;
      }
      else
      {
         ossStrncpy( info._pdLogPath, pdPathOrFile, OSS_MAX_PATHSIZE ) ;
      }
   }

   if ( 0 == info._pdLogPath[0] )
   {
      ossStrcpy( info._pdLogPath, "." ) ;
   }

   if ( info._pdLogPath[0] != 0 )
   {
      engine::utilBuildFullPath( info._pdLogPath, shortName,
                                 OSS_MAX_PATHSIZE, info._pdLogFile ) ;
      info._pdLogFile[ OSS_MAX_PATHSIZE ] = 0 ;
   }
   info._pdFileMaxNum = fileMaxNum ;
   info._pdFileMaxSize = (UINT64)fileMaxSize * 1024 * 1024 ;
}

void sdbDisablePD()
{
   pdLogFile &logFile = _pdLogFiles[ PD_DIAGLOG ] ;
   if ( logFile._logFile.isValid() )
   {
      logFile._logFile.Close() ;
   }
   _getPDCfgInfo()._pdLogFile[0] = 0 ;
}

BOOLEAN sdbIsPDEnabled ()
{
   return _getPDCfgInfo().isEnabled() ;
}

/*
 * Log Header format
 * Arguments:
 * 1) Year (UINT32)
 * 2) Month (UINT32)
 * 3) Day (UINT32)
 * 4) Hour (UINT32)
 * 5) Minute (UINT32)
 * 6) Second (UINT32)
 * 7) Microsecond (UINT32)
 * 8) Level (string)
 * 9) Process ID (UINT64)
 * 10) Thread ID (UINT64)
 * 11) File Name (string)
 * 12) Function Name (string)
 * 13) Line number (UINT32)
 * 14) Message
 */
const static CHAR *PD_LOG_HEADER_FORMAT="%04d-%02d-%02d-%02d.%02d.%02d.%06d\
               \
Level:%s"OSS_NEWLINE"PID:%-37dTID:%d"OSS_NEWLINE"Function:%-32sLine:%d"\
OSS_NEWLINE"File:%s"OSS_NEWLINE"Message:"OSS_NEWLINE"%s"OSS_NEWLINE OSS_NEWLINE;
/* extern variables */

#ifndef SDB_CLIENT

static void _pdRemoveOutOfDataFiles( pdCfgInfo &info )
{
   map< string, string >  mapFiles ;
   const CHAR *p = ossStrrchr( info._pdLogFile, OSS_FILE_SEP_CHAR ) ;
   if ( p )
   {
      p = p + 1 ;
   }
   else
   {
      p = info._pdLogFile ;
   }
   CHAR filter[ OSS_MAX_PATHSIZE + 1 ] = {0} ;
   ossSnprintf( filter, OSS_MAX_PATHSIZE, "%s.*", p ) ;
   ossEnumFiles( info._pdLogPath, mapFiles, filter, 1 ) ;

   while ( mapFiles.size() > 0 &&
           mapFiles.size() >= (UINT32)info._pdFileMaxNum )
   {
      ossDelete( mapFiles.begin()->second.c_str() ) ;
      mapFiles.erase( mapFiles.begin() ) ;
   }
}

static INT32 _pdLogArchive( pdCfgInfo &info )
{
   INT32 rc = SDB_OK ;
   CHAR strTime[ 50 ] = {0} ;
   CHAR fileName[ OSS_MAX_PATHSIZE + 1 ] = {0} ;
   time_t tTime = time( NULL ) ;

   ossSnprintf( fileName, OSS_MAX_PATHSIZE, "%s.%s", info._pdLogFile,
                engine::utilAscTime( tTime, strTime, sizeof( strTime ) ) ) ;

   if ( SDB_OK == ossAccess( fileName ) )
   {
      rc = ossDelete( fileName ) ;
      if ( rc )
      {
         ossPrintf( "Delete file %s failed, rc: %d"OSS_NEWLINE,
                    fileName, rc ) ;
      }
   }
   rc = ossRenamePath( info._pdLogFile, fileName ) ;
   if ( rc )
   {
      ossPrintf( "Rename %s to %s failed, rc: %d"OSS_NEWLINE,
                 info._pdLogFile, fileName, rc ) ;
      ossDelete( info._pdLogFile ) ;
   }

   if ( info._pdFileMaxNum > 0 )
   {
      _pdRemoveOutOfDataFiles( info ) ;
   }

   return rc ;
}

#endif // SDB_CLIENT

// PD_TRACE_DECLARE_FUNCTION ( SDB_PDLOGFILEWRITE, "pdLogFileWrite" )
static INT32 pdLogFileWrite ( _pdLogType type, CHAR *pData )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_PDLOGFILEWRITE ) ;
   SDB_ASSERT ( type < PD_LOG_MAX, "type is out of range" ) ;
   SINT64 dataSize = ossStrlen ( pData ) ;
   pdLogFile &logFile = _pdLogFiles[type] ;
   pdCfgInfo &info = _getPDCfgInfo() ;

   logFile._mutex.get() ;


#ifndef SDB_CLIENT
open:
#endif // SDB_CLIENT

   if ( !logFile._logFile.isValid() )
   {
      rc = logFile._logFile.Open ( info._pdLogFile ) ;
      if ( rc )
      {
         ossPrintf ( "Failed to open log file, errno = %d"OSS_NEWLINE, rc ) ;
         goto error ;
      }
      else
      {
         ossPrimitiveFileOp::offsetType fileSize ;
         rc = logFile._logFile.getSize( &fileSize ) ;
         if ( rc )
         {
            ossPrintf( "Failed to get log file size, rc = %d"OSS_NEWLINE, rc ) ;
            logFile._fileSize = 0 ;
         }
         else
         {
            logFile._fileSize = (UINT64)fileSize.offset ;
         }
      }
      logFile._logFile.seekToEnd () ;
   }

#ifndef SDB_CLIENT
   if ( logFile._fileSize + dataSize > info._pdFileMaxSize )
   {
      logFile._logFile.Close() ;
      _pdLogArchive( info ) ;
      goto open ;
   }
#endif // SDB_CLIENT

   PD_TRACE1 ( SDB_PDLOGFILEWRITE, PD_PACK_RAW ( pData, dataSize ) ) ;
   rc = logFile._logFile.Write ( pData, dataSize ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to reopen log file, errno = %d"OSS_NEWLINE,
                  rc ) ;
      goto error ;
   } // if ( rc )
   logFile._fileSize += dataSize ;

done :
   logFile._logFile.Close() ;
   logFile._mutex.release() ;
   PD_TRACE_EXITRC ( SDB_PDLOGFILEWRITE, rc ) ;
   return rc ;
error :
   goto done ;
}

void pdLog( PDLEVEL level, const CHAR* func, const CHAR* file,
            UINT32 line, std::string message )
{
   pdLog(level, func, file, line, message.c_str() );
}

/*
 * Problem Detemination Log
 * Input:
 * Log level (PDSEVERE/PDERROR/PDWARNING/PDINFO/PDEVENT/PDDEBUG)
 * function name (char*)
 * file name (char*)
 * line number (integer)
 * output string (char*)
 * <followed by arguments>
 * Output:
 *    N/A
 */
// PD_TRACE_DECLARE_FUNCTION ( SDB_PDLOG, "pdLog" )
void pdLog( PDLEVEL level, const CHAR* func, const CHAR* file,
            UINT32 line, const CHAR* format, ...)
{
   INT32 rc = SDB_OK ;
   if ( getPDLevel() < level )
      return ;
   va_list ap;
   PD_TRACE_ENTRY ( SDB_PDLOG ) ;
   CHAR userInfo[ PD_LOG_STRINGMAX ];       // for user defined message
   CHAR sysInfo[ PD_LOG_STRINGMAX ];        // for log header
   struct tm otm ;
   struct timeval tv;
   struct timezone tz;
   time_t tt ;

   static OSS_THREAD_LOCAL BOOLEAN amIInPD = FALSE ;
   if ( amIInPD )
   {
      goto done ;
   }
   amIInPD = TRUE ;

   gettimeofday(&tv, &tz);
   tt = tv.tv_sec ;

#if defined (_WINDOWS)
   localtime_s( &otm, &tt ) ;
#else
   localtime_r( &tt, &otm ) ;
#endif

   va_start(ap, format);
   vsnprintf(userInfo, PD_LOG_STRINGMAX, format, ap);
   va_end(ap);

   ossSnprintf(sysInfo, PD_LOG_STRINGMAX, PD_LOG_HEADER_FORMAT,
               otm.tm_year+1900,            // 1) Year (UINT32)
               otm.tm_mon+1,                // 2) Month (UINT32)
               otm.tm_mday,                 // 3) Day (UINT32)
               otm.tm_hour,                 // 4) Hour (UINT32)
               otm.tm_min,                  // 5) Minute (UINT32)
               otm.tm_sec,                  // 6) Second (UINT32)
               tv.tv_usec,                  // 7) Microsecond (UINT32)
               getPDLevelDesp(level),       // 8) Level (string)
               ossGetCurrentProcessID(),    // 9) Process ID (UINT64)
               ossGetCurrentThreadID(),     // 10) Thread ID (UINT64)
               func,                        // 11) Function Name (string)
               line,                        // 12) Line number (UINT32)
               file,                        // 13) File Name (string)
               userInfo                     // 14) Message
   );

#if defined (_DEBUG) && defined (SDB_ENGINE)
   if ( 1 != ossGetParentProcessID() )
   {
      ossPrintf ( "%s"OSS_NEWLINE, sysInfo ) ;
   }
#else
   /* We write into log file if the string is not empty */
   if ( _getPDCfgInfo().isEnabled() )
#endif
   {
      rc = pdLogFileWrite ( PD_DIAGLOG, sysInfo ) ;
      if ( rc )
      {
         ossPrintf ( "Failed to write into log file, rc: %d"OSS_NEWLINE, rc ) ;
         ossPrintf ( "%s"OSS_NEWLINE, sysInfo ) ;
      }
   }

   amIInPD = FALSE ;

done :
   PD_TRACE_EXITRC ( SDB_PDLOG, rc ) ;
   return ;
}

#ifdef _DEBUG

void pdassert(const CHAR* string, const CHAR* func, const CHAR* file, UINT32 line)
{
   pdLog(PDSEVERE, func, file, line, string);
   ossPanic() ;
}

void pdcheck(const CHAR* string, const CHAR* func, const CHAR* file, UINT32 line)
{
   pdLog(PDSEVERE, func, file, line, string);
}

#endif // _DEBUG

