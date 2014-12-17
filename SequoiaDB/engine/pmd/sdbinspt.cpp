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

   Source File Name = sdbinspt.cpp

   Descriptive Name = SequoiaDB Inspect

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains code logic for
   data dump and integrity check.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/04/2013  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsInspect.hpp"
#include "dmsDump.hpp"
#include "ossUtil.hpp"
#include "ossIO.hpp"
#include "rtn.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "pmdEDU.hpp"
#include "ixmExtent.hpp"

#include <boost/program_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

using namespace std ;
using namespace engine ;
namespace po = boost::program_options;
namespace fs = boost::filesystem ;

#define BUFFERSIZE          256
#define OPTION_HELP         "help"
#define OPTION_DBPATH       "dbpath"
#define OPTION_INDEXPATH    "indexpath"
#define OPTION_OUTPUT       "output"
#define OPTION_VERBOSE      "verbose"
#define OPTION_CSNAME       "csname"
#define OPTION_CLNAME       "clname"
#define OPTION_ACTION       "action"
#define OPTION_DUMPDATA     "dumpdata"
#define OPTION_DUMPINDEX    "dumpindex"
#define OPTION_PAGESTART    "pagestart"
#define OPTION_NUMPAGE      "numpage"
#define OPTION_SHOW_CONTENT "record"
#define ADD_PARAM_OPTIONS_BEGIN( desc )\
        desc.add_options()

#define ADD_PARAM_OPTIONS_END ;

#define COMMANDS_STRING( a, b ) (string(a) +string( b)).c_str()
#define COMMANDS_OPTIONS \
       ( COMMANDS_STRING(OPTION_HELP, ",h"), "help" )\
       ( COMMANDS_STRING(OPTION_DBPATH, ",d"), boost::program_options::value<string>(), "database path" ) \
       ( COMMANDS_STRING(OPTION_INDEXPATH, ",x"), boost::program_options::value<string>(), "index path" ) \
       ( COMMANDS_STRING(OPTION_OUTPUT, ",o"), boost::program_options::value<string>(), "output file" ) \
       ( COMMANDS_STRING(OPTION_VERBOSE, ",v"), boost::program_options::value<string>(), "verbose (ture/false)" ) \
       ( COMMANDS_STRING(OPTION_CSNAME, ",c"), boost::program_options::value<string>(), "collection space name" ) \
       ( COMMANDS_STRING(OPTION_CLNAME, ",l"), boost::program_options::value<string>(), "collection name" ) \
       ( COMMANDS_STRING(OPTION_ACTION, ",a"), boost::program_options::value<string>(), "action (inspect/dump/all)" ) \
       ( COMMANDS_STRING(OPTION_DUMPDATA, ",t"), boost::program_options::value<string>(), "dump data (true/false)" ) \
       ( COMMANDS_STRING(OPTION_DUMPINDEX, ",i"), boost::program_options::value<string>(), "dump index (true/false)" ) \
       ( COMMANDS_STRING(OPTION_PAGESTART, ",s"), boost::program_options::value<SINT32>(), "starting page number" ) \
       ( COMMANDS_STRING(OPTION_NUMPAGE, ",n"), boost::program_options::value<SINT32>(), "number of pages" ) \
       ( COMMANDS_STRING(OPTION_SHOW_CONTENT, ",p"), boost::program_options::value<string>(), "display data/index content(true/false)" )

#define ACTION_INSPECT           0x01
#define ACTION_DUMP              0x02
#define ACTION_INSPECT_STRING    "inspect"
#define ACTION_DUMP_STRING       "dump"
#define ACTION_ALL_STRING        "all"

CHAR    gDatabasePath [ OSS_MAX_PATHSIZE + 1 ]       = {0} ;
CHAR    gIndexPath[ OSS_MAX_PATHSIZE + 1 ]           = {0} ;
CHAR    gOutputFile [ OSS_MAX_PATHSIZE + 1 ]         = {0} ;
BOOLEAN gVerbose                                     = TRUE ;
UINT32  gDumpType                                    = DMS_SU_DMP_OPT_FORMATTED;
CHAR    gCSName [ DMS_COLLECTION_SPACE_NAME_SZ + 1 ] = {0} ;
CHAR    gCLName [ DMS_COLLECTION_NAME_SZ + 1 ]       = {0} ;
CHAR    gAction                                      = ACTION_DUMP ;
BOOLEAN gDumpData                                    = FALSE ;
BOOLEAN gDumpIndex                                   = FALSE ;
SINT32  gStartingPage                                = -1 ;
SINT32  gNumPages                                    = 1 ;
SINT32  gCurFileIndex                                = 0 ;
OSSFILE gFile ;

#define DMS_DUMPFILE "dmsdump"

#define MAX_FILE_SIZE 500 * 1024 * 1024
#define BUFFER_INC_SIZE 67108864
#define BUFFER_INIT_SIZE 4194304
CHAR *  gBuffer                                      = NULL ;
UINT32  gBufferSize                                  = 0 ;

CHAR *  gExtentBuffer = NULL ;
UINT32  gExtentBufferSize = 0 ;

pmdEDUCB *cb             = NULL ;

enum SDB_INSPT_TYPE
{
   SDB_INSPT_DATA,
   SDB_INSPT_INDEX
};

UINT64  gSecretValue                                 = 0 ;
UINT32  gPageSize                                    = 0 ;
UINT32  gDataOffset                                  = 0 ;
INT32   gPageNum                                     = 0 ;
CHAR   *gMMEBuff                                     = NULL ;
BOOLEAN gInitMME                                     = FALSE ;
BOOLEAN gShowRecordContent                           = FALSE ;
BOOLEAN gReachEnd                                    = FALSE ;
BOOLEAN gHitCS                                       = FALSE ;
SDB_INSPT_TYPE gCurInsptType                         = SDB_INSPT_DATA ;

#define RETRY_COUNT 5
INT32 switchFile( OSSFILE& file, const INT32 size )
{
   INT32 rc = SDB_OK ;
   CHAR newFile[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
   INT64 fileSize = 0 ;
   INT32 retryCount = 0;

   rc = ossGetFileSize( &file, &fileSize ) ;
   if( rc )
   {
      printf( "Error: can not get fileSize. rc: %d\n", rc ) ;
      goto error ;
   }

   if( MAX_FILE_SIZE <= fileSize + size )
   {
      ossClose( file ) ;
   retry:
      
      ++gCurFileIndex ;
      ossSnprintf( newFile, OSS_MAX_PATHSIZE, "%s.%d",
                   gOutputFile, gCurFileIndex ) ;
      rc = ossOpen ( newFile, OSS_REPLACE | OSS_WRITEONLY,
                     OSS_RU|OSS_WU|OSS_RG, file ) ;
      if ( rc )
      {
         printf ( "Error: Failed to open output file: %s, rc = %d"
                  OSS_NEWLINE, newFile, rc ) ;
         ++retryCount ;
         if( RETRY_COUNT < retryCount )
         {
            printf( "retry times more than %d, return\n", RETRY_COUNT ) ;
            goto error ;
         }
         printf( "retry again. times : %d\n", retryCount ) ;
         ossMemset ( newFile, 0, OSS_MAX_PATHSIZE + 1 ) ;
         goto retry;
      }
   }

done:
   return rc ;
error:
   goto done ;
}

void dumpPrintf ( const CHAR *format, ... ) ;
#define dumpAndShowPrintf(x,...)                                               \
   do {                                                                        \
      ossPrintf ( x, ##__VA_ARGS__ ) ;                                         \
      if ( ossStrlen ( gOutputFile ) )                                         \
         dumpPrintf ( x, ##__VA_ARGS__ ) ;                                     \
   } while (0)

void init ( po::options_description &desc )
{
   ADD_PARAM_OPTIONS_BEGIN ( desc )
      COMMANDS_OPTIONS
   ADD_PARAM_OPTIONS_END
}

void displayArg ( po::options_description &desc )
{
   std::cout << desc << std::endl ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDBINSPT_RESVARG, "resolveArgument" )
INT32 resolveArgument ( po::options_description &desc, INT32 argc, CHAR **argv )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_SDBINSPT_RESVARG );
   CHAR actionString[BUFFERSIZE] = {0} ;
   CHAR outputFile[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
   po::variables_map vm ;
   try
   {
      po::store ( po::parse_command_line ( argc, argv, desc ), vm ) ;
      po::notify ( vm ) ;
   }
   catch ( po::unknown_option &e )
   {
      PD_LOG ( PDWARNING, ( ( std::string ) "Unknown argument: " +
                e.get_option_name ()).c_str () ) ;
      std::cerr <<  "Unknown argument: " << e.get_option_name () << std::endl ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   catch ( po::invalid_option_value &e )
   {
      PD_LOG ( PDWARNING, ( ( std::string ) "Invalid argument: " +
               e.get_option_name () ).c_str () ) ;
      std::cerr <<  "Invalid argument: "
                << e.get_option_name () << std::endl ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   catch( po::error &e )
   {
      std::cerr << e.what () << std::endl ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( vm.count ( OPTION_HELP ) )
   {
      displayArg ( desc ) ;
      rc = SDB_PMD_HELP_ONLY ;
      goto done ;
   }
   if ( vm.count ( OPTION_DBPATH ) )
   {
      const CHAR *dbpath = vm[OPTION_DBPATH].as<string>().c_str() ;
      if ( ossStrlen ( dbpath ) > OSS_MAX_PATHSIZE )
      {
         ossPrintf ( "Error: db path is too long: %s", dbpath ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossStrncpy ( gDatabasePath, dbpath, sizeof(gDatabasePath) ) ;
   }
   else
   {
      ossStrncpy ( gDatabasePath, ".", sizeof(gDatabasePath) ) ;
   }

   if ( vm.count( OPTION_INDEXPATH ) )
   {
      const CHAR *indexPath = vm[OPTION_INDEXPATH].as<string>().c_str() ;
      if ( ossStrlen ( indexPath ) > OSS_MAX_PATHSIZE )
      {
         ossPrintf ( "Error: index path is too long: %s", indexPath ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossMemcpy ( gIndexPath, indexPath, OSS_MAX_PATHSIZE ) ;
   }
   else
   {
      ossStrcpy( gIndexPath, gDatabasePath ) ;
   }

   if ( vm.count ( OPTION_OUTPUT ) )
   {
      const CHAR *output = vm[OPTION_OUTPUT].as<string>().c_str() ;
      INT32 rc = SDB_OK ;
      if ( ossStrlen ( output ) + 5 > OSS_MAX_PATHSIZE )
      {
         ossPrintf ( "Error: output is too long: %s", output ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossStrncpy ( gOutputFile, output, sizeof(gOutputFile) ) ;
      SDB_OSS_FILETYPE fileType = SDB_OSS_UNK ;
      INT32 retValue = ossGetPathType( gOutputFile, &fileType ) ;
      if( SDB_OSS_DIR == fileType && !retValue )
      {
         ossSnprintf( outputFile, OSS_MAX_PATHSIZE, "%s"DMS_DUMPFILE".%d",
                      gOutputFile, gCurFileIndex ) ;
      }
      else
      {
         ossSnprintf( outputFile, OSS_MAX_PATHSIZE, "%s.%d",
                      gOutputFile, gCurFileIndex ) ;
      }
      rc = ossOpen ( outputFile, OSS_REPLACE | OSS_WRITEONLY,
                     OSS_RU|OSS_WU|OSS_RG, gFile ) ;
      if ( rc )
      {
         ossPrintf ( "Error: Failed to open output file: %s, rc = %d"
                     OSS_NEWLINE,
                     outputFile, rc ) ;
         ossMemset ( gOutputFile, 0, sizeof(gOutputFile) ) ;
      }
   }

   if ( vm.count ( OPTION_VERBOSE ) )
   {
      ossStrToBoolean ( vm[OPTION_VERBOSE].as<string>().c_str(),
                        &gVerbose ) ;
      if ( !gVerbose )
         gDumpType = 0 ;
   }

   if ( vm.count ( OPTION_DUMPDATA ) )
   {
      ossStrToBoolean ( vm[OPTION_DUMPDATA].as<string>().c_str(),
                        &gDumpData ) ;
   }

   if ( vm.count ( OPTION_DUMPINDEX ) )
   {
      ossStrToBoolean ( vm[OPTION_DUMPINDEX].as<string>().c_str(),
                        &gDumpIndex ) ;
   }

   if ( vm.count ( OPTION_CSNAME ) )
   {
      const CHAR *csname = vm[OPTION_CSNAME].as<string>().c_str() ;
      if ( ossStrlen ( csname ) > DMS_COLLECTION_SPACE_NAME_SZ )
      {
         ossPrintf ( "Error: collection space name is too long: %s", csname ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossStrncpy ( gCSName, csname, sizeof(gCSName) ) ;
   }

   if ( vm.count ( OPTION_CLNAME ) )
   {
      const CHAR *clname = vm[OPTION_CLNAME].as<string>().c_str() ;
      if ( ossStrlen ( clname ) > DMS_COLLECTION_SPACE_NAME_SZ )
      {
         ossPrintf ( "Error: collection name is too long: %s", clname ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossStrncpy ( gCLName, clname, sizeof(gCLName) ) ;
   }

   if ( vm.count ( OPTION_PAGESTART ) )
   {
      gStartingPage = vm[OPTION_PAGESTART].as<SINT32>() ;
   }

   if ( vm.count ( OPTION_NUMPAGE ) )
   {
      gNumPages = vm[OPTION_NUMPAGE].as<SINT32>() ;
   }

   gAction = ACTION_DUMP ;
   ossStrncpy ( actionString, ACTION_DUMP_STRING,
                sizeof(actionString) ) ;
   if ( vm.count ( OPTION_ACTION ) )
   {
      const CHAR *action = vm[OPTION_ACTION].as<string>().c_str() ;
      if ( ossStrncasecmp ( action, ACTION_INSPECT_STRING,
           ossStrlen(action) ) == 0 )
      {
         ossStrncpy ( actionString, ACTION_INSPECT_STRING,
                      sizeof(actionString) ) ;
         gAction = ACTION_INSPECT ;
      }
      else if ( ossStrncasecmp ( action, ACTION_DUMP_STRING,
                ossStrlen(action) ) == 0 )
      {
         ossStrncpy ( actionString, ACTION_DUMP_STRING,
                      sizeof(actionString) ) ;
         gAction = ACTION_DUMP ;
      }
      else if ( ossStrncasecmp ( action, ACTION_ALL_STRING,
                ossStrlen(action) ) == 0 )
      {
         ossStrncpy ( actionString, ACTION_ALL_STRING,
                      sizeof(actionString) ) ;
         gAction = ACTION_INSPECT | ACTION_DUMP ;
      }
      else
      {
         dumpAndShowPrintf ( "Invalid Action Option: %s"OSS_NEWLINE,
                             action ) ;
         displayArg ( desc ) ;
         rc = SDB_PMD_HELP_ONLY ;
         goto done ;
      }
   }
   else
   {
      dumpAndShowPrintf ( "Action must be specified"OSS_NEWLINE ) ;
      displayArg ( desc ) ;
      rc = SDB_PMD_HELP_ONLY ;
      goto done ;
   }
   if( vm.count( OPTION_SHOW_CONTENT ) )
   {
      ossStrToBoolean( vm[OPTION_SHOW_CONTENT].as<string>().c_str(),
                       &gShowRecordContent ) ;
   }
   dumpAndShowPrintf ( "Run Options   :"OSS_NEWLINE ) ;
   dumpAndShowPrintf ( "Database Path : %s"OSS_NEWLINE,
                       gDatabasePath ) ;
   dumpAndShowPrintf ( "Index path    : %s"OSS_NEWLINE,
                       gIndexPath ) ;
   dumpAndShowPrintf ( "Output File   : %s"OSS_NEWLINE,
                       ossStrlen(gOutputFile)?gOutputFile:"{stdout}" ) ;
   dumpAndShowPrintf ( "Verbose       : %s"OSS_NEWLINE,
                       gVerbose?"True":"False" ) ;
   dumpAndShowPrintf ( "CS Name       : %s"OSS_NEWLINE,
                       ossStrlen(gCSName)?gCSName:"{all}" ) ;
   dumpAndShowPrintf ( "CL Name       : %s"OSS_NEWLINE,
                       ossStrlen(gCLName)?gCLName:"{all}" ) ;
   dumpAndShowPrintf ( "Action        : %s"OSS_NEWLINE,
                       actionString ) ;
   dumpAndShowPrintf ( "Dump Options  :"OSS_NEWLINE ) ;
   dumpAndShowPrintf ( "   Dump Data  : %s"OSS_NEWLINE,
                       gDumpData?"True":"False" ) ;
   dumpAndShowPrintf ( "   Dump Index : %s"OSS_NEWLINE,
                       gDumpIndex?"True":"False" ) ;
   dumpAndShowPrintf ( "   Start Page : %d"OSS_NEWLINE,
                       gStartingPage ) ;
   dumpAndShowPrintf ( "   Num Pages  : %d"OSS_NEWLINE,
                       gNumPages ) ;
   dumpAndShowPrintf ( "   Show record: %s"OSS_NEWLINE,
                       gShowRecordContent ? "True":"False") ;
   dumpAndShowPrintf ( OSS_NEWLINE ) ;
done :
   PD_TRACE_EXITRC ( SDB_SDBINSPT_RESVARG, rc );
   return rc ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_FLUSHOUTPUT, "flushOutput" )
void flushOutput ( const CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_FLUSHOUTPUT );
   SINT64 writeSize ;
   SINT64 writtenSize = 0 ;

   rc = switchFile( gFile, size ) ;
   if( rc )
   {
      goto error ;
   }

   if ( ossStrlen ( gOutputFile ) != 0 )
   {
      do
      {
         rc = ossWrite ( &gFile, &pBuffer[writtenSize], size-writtenSize,
                         &writeSize ) ;
         if ( rc && SDB_INTERRUPT != rc )
         {
            break ;
         }
         rc = SDB_OK ;
         writtenSize += writeSize ;
      } while ( writtenSize < size ) ;
      if ( rc )
      {
         ossPrintf ( "Error: Failed to write into file, rc = %d"OSS_NEWLINE,
                     rc ) ;
         goto error ;
      }
   }
   else
   {
      goto error ;
   }
done :
   PD_TRACE1 ( SDB_FLUSHOUTPUT, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_FLUSHOUTPUT );
   return ;
error :
   ossPrintf ( "%s", pBuffer ) ;
   goto done ;
}

#define DUMP_PRINTF_BUFFER_SZ 4095
// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPPRINTF, "dumpPrintf" )
void dumpPrintf ( const CHAR *format, ... )
{
   PD_TRACE_ENTRY ( SDB_DUMPPRINTF );
   INT32 len = 0 ;
   CHAR tempBuffer [ DUMP_PRINTF_BUFFER_SZ + 1 ] = {0} ;
   va_list ap ;
   va_start ( ap, format ) ;
   len = vsnprintf ( tempBuffer, DUMP_PRINTF_BUFFER_SZ, format, ap );
   va_end ( ap ) ;
   flushOutput ( tempBuffer, len ) ;
   PD_TRACE1 ( SDB_DUMPPRINTF, PD_PACK_INT(len) );
   PD_TRACE_EXIT ( SDB_DUMPPRINTF );
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_REALLOCBUFFER, "reallocBuffer" )
INT32 reallocBuffer ()
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_REALLOCBUFFER );
   if ( gBufferSize == 0 )
   {
      gBufferSize = BUFFER_INIT_SIZE ;
   }
   else
   {
      if ( gBufferSize > 0x7FFFFFFF )
      {
         dumpPrintf ( "Error: Cannot allocate more than 2GB"OSS_NEWLINE ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      gBufferSize += gBufferSize > BUFFER_INC_SIZE?
                     BUFFER_INC_SIZE : gBufferSize ;
   }

   if ( gBuffer )
   {
      SDB_OSS_FREE ( gBuffer ) ;
      gBuffer = NULL ;
   }
   gBuffer = (CHAR*)SDB_OSS_MALLOC ( gBufferSize ) ;
   if ( !gBuffer )
   {
      dumpPrintf ( "Error: Failed to allocate memory for %d bytes"OSS_NEWLINE,
                   gBufferSize ) ;
      rc = SDB_OOM ;
      gBufferSize = 0 ;
      goto error ;
   }

done :
   PD_TRACE_EXITRC ( SDB_REALLOCBUFFER, rc );
   return rc ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_GETEXTBUFFER, "getExtentBuffer" )
INT32 getExtentBuffer ( INT32 size )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_GETEXTBUFFER );
   if ( (UINT32)size > gExtentBufferSize )
   {
      if ( gExtentBuffer )
      {
         SDB_OSS_FREE ( gExtentBuffer ) ;
         gExtentBuffer = NULL ;
         gExtentBufferSize = 0 ;
      }
      gExtentBuffer = (CHAR*)SDB_OSS_MALLOC ( size ) ;
      if ( !gExtentBuffer )
      {
         dumpPrintf ( "Error: Failed to allocate extent buffer for %d bytes"
                      OSS_NEWLINE, size ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      gExtentBufferSize = size ;
   }

done :
   PD_TRACE_EXITRC ( SDB_GETEXTBUFFER, rc );
   return rc ;
error :
   goto done ;
}

void clearBuffer ()
{
   if ( gBuffer )
   {
      SDB_OSS_FREE ( gBuffer ) ;
      gBuffer = NULL ;
   }
   gBufferSize = 0 ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPECTHEADER, "inspectHeader" )
void inspectHeader ( OSSFILE &file, SINT32 &pageSize, SINT32 &err )
{
   INT32 rc       = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_INSPECTHEADER );
   INT32 localErr = 0 ;
   UINT32 len     = 0 ;
   CHAR headerBuffer [ DMS_HEADER_SZ ] = {0};
   SINT64 lenRead = 0 ;
   UINT64 secretValue = 0 ;

   rc = ossSeekAndRead ( &file, DMS_HEADER_OFFSET, headerBuffer,
                         DMS_HEADER_SZ, &lenRead ) ;
   if ( rc || lenRead != DMS_HEADER_SZ )
   {
      dumpPrintf ( "Error: Failed to read header, read %lld bytes, "
                   "rc = %d"OSS_NEWLINE, lenRead, rc ) ;
      ++err ;
      goto error ;
   }
retry :
   localErr = 0 ;
   len = dmsInspect::inspectHeader ( headerBuffer, DMS_HEADER_SZ,
                                     gBuffer, gBufferSize,
                                     pageSize, gPageNum,
                                     secretValue, localErr ) ;
   PD_TRACE1 ( SDB_INSPECTHEADER, PD_PACK_UINT(len) );
   if ( len >= gBufferSize - 1 )
   {
      if ( reallocBuffer () )
      {
         clearBuffer () ;
         goto error ;
      }
      goto retry ;
   }
   err += localErr ;
   flushOutput ( gBuffer, len ) ;

   if ( secretValue != gSecretValue )
   {
      dumpPrintf ( "Error: Secret value[%llu] is not expected[%llu]"OSS_NEWLINE,
                    secretValue, gSecretValue ) ;
      ++err ;
   }
   if ( (UINT32)pageSize != gPageSize )
   {
      dumpPrintf ( "Error: Page size[%d] is not expected[%d]"OSS_NEWLINE,
                    pageSize, gPageSize ) ;
      ++err ;
   }

done :
   PD_TRACE1 ( SDB_INSPECTHEADER, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_INSPECTHEADER );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPHEADER, "dumpHeader" )
void dumpHeader ( OSSFILE &file, SINT32 &pageSize )
{
   INT32 rc                            = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_DUMPHEADER );
   UINT32 len                          = 0 ;
   CHAR headerBuffer [ DMS_HEADER_SZ ] = {0};
   SINT64 lenRead                      = 0 ;
   rc = ossSeekAndRead ( &file, DMS_HEADER_OFFSET, headerBuffer,
                         DMS_HEADER_SZ, &lenRead ) ;
   if ( rc || lenRead != DMS_HEADER_SZ )
   {
      dumpPrintf ( "Error: Failed to read header, read %lld bytes, "
                   "rc = %d"OSS_NEWLINE, lenRead, rc ) ;
      goto error ;
   }
retry :
   len = dmsDump::dumpHeader ( headerBuffer, DMS_HEADER_SZ,
                               gBuffer, gBufferSize, NULL,
                               DMS_SU_DMP_OPT_HEX |
                               DMS_SU_DMP_OPT_HEX_WITH_ASCII |
                               DMS_SU_DMP_OPT_HEX_PREFIX_AS_ADDR |
                               gDumpType, pageSize, gPageNum ) ;
   PD_TRACE1 ( SDB_DUMPHEADER, PD_PACK_UINT(len) );
   if ( len >= gBufferSize - 1 )
   {
      if ( reallocBuffer () )
      {
         clearBuffer () ;
         goto error ;
      }
      goto retry ;
   }
   flushOutput ( gBuffer, len ) ;

done :
   PD_TRACE1 ( SDB_DUMPHEADER, PD_PACK_INT(rc) ) ;
   PD_TRACE_EXIT ( SDB_DUMPHEADER );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPECTSME, "inspectSME" )
void inspectSME ( OSSFILE &file, const CHAR *pExpBuf, SINT32 &hwm, SINT32 &err )
{
   INT32 rc        = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_INSPECTSME );
   UINT32 len      = 0 ;
   CHAR *smeBuffer = NULL ;
   SINT64 lenRead  = 0 ;
   SINT32 localErr = 0 ;
   smeBuffer = (CHAR*)SDB_OSS_MALLOC ( DMS_SME_SZ ) ;
   if ( !smeBuffer )
   {
      dumpPrintf ( "Error: Failed to allocate %d bytes for SME buffer"
                   OSS_NEWLINE, (INT32)DMS_SME_SZ ) ;
      ++err ;
      goto error ;
   }
   rc = ossSeekAndRead ( &file, DMS_SME_OFFSET, smeBuffer,
                         DMS_SME_SZ, &lenRead ) ;
   if ( rc || lenRead != DMS_SME_SZ )
   {
      dumpPrintf ( "Error: Failed to read sme, read %lld bytes, "
                   "rc = %d"OSS_NEWLINE, lenRead, rc ) ;
      ++err ;
      goto error ;
   }
retry :
   localErr = 0 ;
   len = dmsInspect::inspectSME ( smeBuffer, DMS_SME_SZ,
                                  gBuffer, gBufferSize,
                                  pExpBuf, gPageNum,
                                  hwm, localErr ) ;
   PD_TRACE1 ( SDB_INSPECTSME, PD_PACK_UINT(len) );
   if ( len >= gBufferSize - 1 )
   {
      if ( reallocBuffer () )
      {
         clearBuffer () ;
         goto error ;
      }
      goto retry ;
   }
   flushOutput ( gBuffer, len ) ;
   err += localErr ;
done :
   if ( smeBuffer )
   {
      SDB_OSS_FREE ( smeBuffer ) ;
   }
   PD_TRACE1 ( SDB_INSPECTSME, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_INSPECTSME );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPSME, "dumpSME" )
void dumpSME ( OSSFILE &file )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_DUMPSME );
   UINT32 len ;
   CHAR *smeBuffer = NULL ;
   SINT64 lenRead = 0 ;
   smeBuffer = (CHAR*)SDB_OSS_MALLOC ( DMS_SME_SZ ) ;
   if ( !smeBuffer )
   {
      dumpPrintf ( "Error: Failed to allocate %d bytes for SME buffer"
                   OSS_NEWLINE, (INT32)DMS_SME_SZ ) ;
      goto error ;
   }
   rc = ossSeekAndRead ( &file, DMS_SME_OFFSET, smeBuffer,
                         DMS_SME_SZ, &lenRead ) ;
   if ( rc || lenRead != DMS_SME_SZ )
   {
      dumpPrintf ( "Error: Failed to read sme, read %lld bytes, rc = %d"
                   OSS_NEWLINE, lenRead, rc ) ;
      goto error ;
   }
retry :
   len = dmsDump::dumpSME ( smeBuffer, DMS_SME_SZ,
                            gBuffer, gBufferSize, gPageNum ) ;
   PD_TRACE1 ( SDB_DUMPSME, PD_PACK_UINT(len) );
   if ( len >= gBufferSize - 1 )
   {
      if ( reallocBuffer () )
      {
         clearBuffer () ;
         goto error ;
      }
      goto retry ;
   }
   flushOutput ( gBuffer, len ) ;

done :
   if ( smeBuffer )
   {
      SDB_OSS_FREE ( smeBuffer ) ;
   }
   PD_TRACE1 ( SDB_DUMPSME, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_DUMPSME );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_GETEXTENTHEAD, "getExtentHead" )
INT32 getExtentHead ( OSSFILE &file, dmsExtentID extentID, SINT32 pageSize,
                      dmsExtent &extentHead )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_GETEXTENTHEAD );
   SINT64 lenRead = 0 ;
   rc = ossSeekAndRead ( &file, gDataOffset + (SINT64)pageSize * extentID,
                         (CHAR*)&extentHead, DMS_EXTENT_METADATA_SZ,
                         &lenRead ) ;
   if ( rc || lenRead != DMS_EXTENT_METADATA_SZ )
   {
      dumpPrintf ( "Error: Failed to read extent head, read %lld bytes, "
                   "rc = %d"OSS_NEWLINE, lenRead, rc ) ;
      if ( !rc )
         rc = SDB_IO ;
   }
   PD_TRACE_EXITRC ( SDB_GETEXTENTHEAD, rc );
   return rc ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_GETEXTENT, "getExtent" )
INT32 getExtent ( OSSFILE &file, dmsExtentID extentID, SINT32 pageSize,
                  SINT32 extentSize )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_GETEXTENT );
   SINT64 lenRead ;
   if ( gExtentBufferSize < (UINT32)(extentSize * pageSize ) )
   {
      rc = getExtentBuffer ( extentSize*pageSize ) ;
      if ( rc )
      {
         dumpPrintf ( "Error: Failed to allocate extent buffer, rc = %d"
                      OSS_NEWLINE, rc ) ;
         goto error ;
      }
   }
   rc = ossSeekAndRead ( &file, gDataOffset + (SINT64)pageSize * extentID,
                         gExtentBuffer, extentSize * pageSize,
                         &lenRead ) ;
   if ( rc || lenRead != extentSize * pageSize )
   {
      dumpPrintf ( "Error: Failed to read extent , read %lld bytes, "
                   "expect %d bytes, rc = %d"OSS_NEWLINE, lenRead,
                   extentSize * pageSize, rc ) ;
      gReachEnd = TRUE ;

      if ( !rc )
         rc = SDB_IO ;
   }

done :
   PD_TRACE_EXITRC ( SDB_GETEXTENT, rc );
   return rc ;
error :
   goto done ;
}

enum INSPECT_EXTENT_TYPE
{
   INSPECT_EXTENT_TYPE_DATA = 0,
   INSPECT_EXTENT_TYPE_INDEX,
   INSPECT_EXTENT_TYPE_INDEX_CB,
   INSPECT_EXTENT_TYPE_MBEX,
   INSPECT_EXTENT_TYPE_UNKNOWN
} ;
// PD_TRACE_DECLARE_FUNCTION ( SDB_EXTENTSANITYCHK, "extentSanityCheck" )
BOOLEAN extentSanityCheck ( dmsExtent &extentHead,
                            INSPECT_EXTENT_TYPE &type, // in-out
                            SINT32 pageSize,
                            UINT16 expID )
{
   PD_TRACE_ENTRY ( SDB_EXTENTSANITYCHK );
   BOOLEAN result = TRUE ;

retry :
   if ( INSPECT_EXTENT_TYPE_DATA == type )
   {
      if ( extentHead._eyeCatcher[0] != DMS_EXTENT_EYECATCHER0 ||
           extentHead._eyeCatcher[1] != DMS_EXTENT_EYECATCHER1 )
      {
         dumpPrintf ( "Error: Invalid eye catcher: %c%c"OSS_NEWLINE,
                      extentHead._eyeCatcher[0],
                      extentHead._eyeCatcher[1] ) ;
         result = FALSE ;
      }
      if ( extentHead._blockSize <= 0 ||
           extentHead._blockSize * pageSize > DMS_SEGMENT_SZ )
      {
         dumpPrintf ( "Error: Invalid block size: %d, pageSize: %d"
                      OSS_NEWLINE, extentHead._blockSize, pageSize ) ;
         result = FALSE ;
      }
      if ( extentHead._mbID != expID )
      {
         dumpPrintf ( "Error: Unexpected id: %d, expected %d"OSS_NEWLINE,
                      extentHead._mbID, expID ) ;
         result = FALSE ;
      }
      if ( extentHead._version > DMS_EXTENT_CURRENT_V )
      {
         dumpPrintf ( "Error: Invalid version: %d, current %d"OSS_NEWLINE,
                      extentHead._version, DMS_EXTENT_CURRENT_V ) ;
         result = FALSE ;
      }
      if ( ( extentHead._firstRecordOffset != DMS_INVALID_OFFSET &&
             extentHead._lastRecordOffset == DMS_INVALID_OFFSET ) ||
           ( extentHead._firstRecordOffset == DMS_INVALID_OFFSET &&
             extentHead._lastRecordOffset != DMS_INVALID_OFFSET ) )
      {
         dumpPrintf ( "Error: Bad first/last offset: %d:%d"OSS_NEWLINE,
                      extentHead._firstRecordOffset,
                      extentHead._lastRecordOffset ) ;
         result = FALSE ;
      }
      if ( extentHead._firstRecordOffset >=
           extentHead._blockSize * pageSize )
      {
         dumpPrintf ( "Error: Bad first record offset: %d"OSS_NEWLINE,
                      extentHead._firstRecordOffset ) ;
         result = FALSE ;
      }
      if ( extentHead._lastRecordOffset >=
           extentHead._blockSize * pageSize )
      {
         dumpPrintf ( "Error: Bad last record offset: %d"OSS_NEWLINE,
                      extentHead._lastRecordOffset ) ;
         result = FALSE ;
      }
      if ( (UINT32)extentHead._freeSpace >
           extentHead._blockSize * pageSize - DMS_EXTENT_METADATA_SZ )
      {
         dumpPrintf ( "Error: Invalid free space: %d, extentSize: %d"
                      OSS_NEWLINE, extentHead._freeSpace,
                      extentHead._blockSize * pageSize ) ;
         result = FALSE ;
      }
   }
   else if ( INSPECT_EXTENT_TYPE_INDEX == type )
   {
   }
   else if ( INSPECT_EXTENT_TYPE_INDEX_CB == type )
   {
   }
   else if ( INSPECT_EXTENT_TYPE_MBEX == type )
   {
      dmsMetaExtent *metaExt = ( dmsMetaExtent* )&extentHead ;
      if ( metaExt->_eyeCatcher[0] != DMS_META_EXTENT_EYECATCHER0 ||
           metaExt->_eyeCatcher[1] != DMS_META_EXTENT_EYECATCHER1 )
      {
         dumpPrintf ( "Error: Invalid eye catcher: %c%c"OSS_NEWLINE,
                      metaExt->_eyeCatcher[0],
                      metaExt->_eyeCatcher[1] ) ;
         result = FALSE ;
      }
      if ( metaExt->_blockSize <= 0 ||
           metaExt->_blockSize * pageSize > DMS_SEGMENT_SZ )
      {
         dumpPrintf ( "Error: Invalid block size: %d, pageSize: %d"
                      OSS_NEWLINE, metaExt->_blockSize, pageSize ) ;
         result = FALSE ;
      }
      if ( metaExt->_mbID != expID )
      {
         dumpPrintf ( "Error: Unexpected id: %d, expected %d"OSS_NEWLINE,
                      metaExt->_mbID, expID ) ;
         result = FALSE ;
      }
      if ( metaExt->_version > DMS_META_EXTENT_CURRENT_V )
      {
         dumpPrintf ( "Error: Invalid version: %d, current %d"OSS_NEWLINE,
                      metaExt->_version, DMS_META_EXTENT_CURRENT_V ) ;
         result = FALSE ;
      }
   }
   else if ( INSPECT_EXTENT_TYPE_UNKNOWN == type )
   {
      if ( extentHead._eyeCatcher[0] == DMS_EXTENT_EYECATCHER0 &&
           extentHead._eyeCatcher[1] == DMS_EXTENT_EYECATCHER1 )
      {
         type = INSPECT_EXTENT_TYPE_DATA ;
         goto retry ;
      }
      else if ( extentHead._eyeCatcher[0] == IXM_EXTENT_EYECATCHER0 &&
                extentHead._eyeCatcher[1] == IXM_EXTENT_EYECATCHER1 )
      {
         type = INSPECT_EXTENT_TYPE_INDEX ;
         goto retry ;
      }
      else if ( extentHead._eyeCatcher[0] == IXM_EXTENT_CB_EYECATCHER0 &&
                extentHead._eyeCatcher[1] == IXM_EXTENT_CB_EYECATCHER1 )
      {
         type = INSPECT_EXTENT_TYPE_INDEX_CB ;
         goto retry ;
      }
      else if ( extentHead._eyeCatcher[0] == DMS_META_EXTENT_EYECATCHER0 &&
                extentHead._eyeCatcher[1] == DMS_META_EXTENT_EYECATCHER1 )
      {
         type = INSPECT_EXTENT_TYPE_MBEX ;
         goto retry ;
      }
      else
      {
         dumpPrintf ( "Error: Unknown eye catcher: %c%c"OSS_NEWLINE,
                      extentHead._eyeCatcher[0],
                      extentHead._eyeCatcher[1] ) ;
         result = FALSE ;
      }
   }
   else
   {
      SDB_ASSERT ( FALSE, "should never hit here" ) ;
   }
   PD_TRACE1 ( SDB_EXTENTSANITYCHK, PD_PACK_INT(result) );
   PD_TRACE_EXIT ( SDB_EXTENTSANITYCHK );
   return result ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_LOADMB, "loadMB" )
INT32 loadMB ( UINT16 collectionID, dmsMB *&mb )
{
   INT32 rc = SDB_SYS ;
   PD_TRACE_ENTRY ( SDB_LOADMB ) ;
   mb = NULL ;

   if ( gInitMME && collectionID < DMS_MME_SLOTS )
   {
      dmsMetadataManagementExtent *pMME =
         (dmsMetadataManagementExtent*)gMMEBuff ;
      mb = &(pMME->_mbList[collectionID]) ;
      rc = SDB_OK ;
   }

   PD_TRACE_EXITRC ( SDB_LOADMB, rc );
   return rc ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_LOADEXTENT, "loadExtent" )
INT32 loadExtent ( OSSFILE &file, INSPECT_EXTENT_TYPE &type,
                   SINT32 pageSize, dmsExtentID extentID,
                   UINT16 collectionID )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_LOADEXTENT );
   dmsExtent extentHead ;
   rc = getExtentHead ( file, extentID, pageSize, extentHead ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to get extent head, rc = %d"OSS_NEWLINE,
                   rc ) ;
      goto error ;
   }
   if ( !extentSanityCheck ( extentHead, type, pageSize,
                             collectionID ) )
   {
      dumpPrintf ( "Error: Failed head sanity check, dump head:"
                   OSS_NEWLINE ) ;
      INT32 len = ossHexDumpBuffer ( (CHAR*)&extentHead,
                                     DMS_EXTENT_METADATA_SZ,
                                     gBuffer, gBufferSize, NULL,
                                     OSS_HEXDUMP_PREFIX_AS_ADDR ) ;
      flushOutput ( gBuffer, len ) ;
      rc = SDB_DMS_CORRUPTED_EXTENT ;
      goto error ;
   }

   rc = getExtent ( file, extentID, pageSize,
                    ( INSPECT_EXTENT_TYPE_DATA == type ||
                      INSPECT_EXTENT_TYPE_MBEX == type ) ?
                      extentHead._blockSize : 1 ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to get extent %d, rc = %d"
                   OSS_NEWLINE, extentID, rc ) ;
      goto error ;
   }

done :
   PD_TRACE_EXITRC ( SDB_LOADEXTENT, rc );
   return rc ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPOVFLWRECRDS, "inspectOverflowedRecords" )
void inspectOverflowedRecords ( OSSFILE &file, SINT32 pageSize,
                                UINT16 collectionID, dmsExtentID ovfFromExtent,
                                std::set<dmsRecordID> &overRIDList,
                                SINT32 &err )
{
   INT32 rc        = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_INSPOVFLWRECRDS );
   SINT32 oldErr   = err ;
   SINT32 localErr = 0 ;
   UINT32 len      = 0 ;
   INT32 count     = 0 ;
   std::set<dmsRecordID>::iterator it ;
   INSPECT_EXTENT_TYPE extentType = INSPECT_EXTENT_TYPE_DATA ;
   dmsExtentID currentExtentID = DMS_INVALID_EXTENT ;

   dumpPrintf ( " Inspect Overflow-Records for Collection [%d]'s extent [%d]"
                OSS_NEWLINE, collectionID, ovfFromExtent ) ;

   for ( it = overRIDList.begin() ; it != overRIDList.end() ; ++it )
   {
      dmsRecordID rid = *it ;
      dmsOffset offset = 0 ;
      if ( rid._extent > gPageNum )
      {
         dumpPrintf ( "Error: overflowed rid extent is out of range: "
                      "0x08lx (%d) 0x08lx (%d)"OSS_NEWLINE,
                      rid._extent, rid._extent,
                      rid._offset, rid._offset ) ;
         ++err ;
         continue ;
      }

      if ( currentExtentID != rid._extent )
      {
         rc = loadExtent ( file, extentType, pageSize, rid._extent,
                           collectionID ) ;
         if ( rc )
         {
            dumpPrintf ( "Error: Failed to load extent %d, rc = %d"OSS_NEWLINE,
                         rid._extent, rc ) ;
            ++err ;
            goto error ;
         }
         currentExtentID = rid._extent ;
      }

retry :
      offset = rid._offset ;
      localErr = 0 ;
      len = dmsInspect::inspectDataRecord ( cb, gExtentBuffer + offset,
              ((dmsExtent*)gExtentBuffer)->_blockSize * pageSize - offset,
              gBuffer, gBufferSize, count, offset, NULL, localErr ) ;
      if ( len >= gBufferSize-1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry ;
      }
      flushOutput ( gBuffer, len ) ;
      err += localErr ;
      ++count ;
   } // end for

done :
   if ( oldErr == err )
   {
      dumpPrintf ( " Inspect Overflow-Records for Collection [%d]' extent [%d] "
                   "done without Error"OSS_NEWLINE, collectionID,
                   ovfFromExtent ) ;
   }
   else
   {
      dumpPrintf ( " Inspect Overflow-Records for Collection [%d]' extent [%d] "
                   "done with error: %d"OSS_NEWLINE, collectionID,
                   ovfFromExtent, err-oldErr ) ;
   }
   PD_TRACE1 ( SDB_INSPOVFLWRECRDS, PD_PACK_INT(err) );
   PD_TRACE_EXIT ( SDB_INSPOVFLWRECRDS );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPOVFWRECRDS, "dumpOverflowedRecords" )
void dumpOverflowedRecords ( OSSFILE &file, SINT32 pageSize,
                             UINT16 collectionID, dmsExtentID ovfFromExtID,
                             std::set<dmsRecordID> &overRIDList )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_DUMPOVFWRECRDS );
   UINT32 len = 0 ;
   std::set<dmsRecordID>::iterator it ;
   INSPECT_EXTENT_TYPE extentType = INSPECT_EXTENT_TYPE_DATA ;
   dmsExtentID currentExtentID = DMS_INVALID_EXTENT ;
   dumpPrintf ( " Dump Overflow-Records for Collection [%d]'s extent [%d]"
                OSS_NEWLINE, collectionID, ovfFromExtID ) ;

   for ( it = overRIDList.begin() ; it != overRIDList.end() ; ++it )
   {
      dmsRecordID rid = *it ;
      dmsOffset offset = 0 ;
      if ( currentExtentID != rid._extent )
      {
         rc = loadExtent ( file, extentType, pageSize, rid._extent,
                           collectionID ) ;
         if ( rc )
         {
            dumpPrintf ( "Error: Failed to load extent %d, rc = %d"OSS_NEWLINE,
                         rid._extent, rc ) ;
            goto error ;
         }
         currentExtentID = rid._extent ;
      }

retry :
      offset = rid._offset ;
      dumpPrintf ( "    OvfRecord 0x%08x : 0x%08x:"OSS_NEWLINE,
                   rid._extent, rid._offset ) ;
      len = dmsDump::dumpDataRecord ( cb, gExtentBuffer + offset,
                 ((dmsExtent*)gExtentBuffer)->_blockSize * pageSize - offset,
                 gBuffer, gBufferSize, offset, NULL ) ;
      PD_TRACE1 ( SDB_DUMPOVFWRECRDS, PD_PACK_UINT(len) );
      if ( len >= gBufferSize-1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry ;
      }
      flushOutput ( gBuffer, len ) ;
      dumpPrintf ( OSS_NEWLINE ) ;
   }

done :
   PD_TRACE1 ( SDB_DUMPOVFWRECRDS, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_DUMPOVFWRECRDS );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPINXDEF, "inspectIndexDef" )
void inspectIndexDef ( OSSFILE &file, SINT32 pageSize, UINT16 collectionID,
                       dmsMB *mb, CHAR *pExpBuffer,
                       std::map<UINT16, dmsExtentID> &indexRoots,
                       SINT32 &err )
{
   UINT32 len = 0 ;
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_INSPINXDEF );
   INT32 localErr = 0 ;
   INT32 oldErr   = err ;
   INSPECT_EXTENT_TYPE extentType = INSPECT_EXTENT_TYPE_INDEX_CB ;

   if ( mb->_numIndexes > DMS_COLLECTION_MAX_INDEX )
   {
      dumpPrintf ( "Error: numIdx is out of range, max: %d"OSS_NEWLINE,
                   DMS_COLLECTION_MAX_INDEX ) ;
      ++err ;
      mb->_numIndexes = DMS_COLLECTION_MAX_INDEX ;
   }
   for ( UINT16 i = 0 ; i < mb->_numIndexes ; ++i )
   {
      dmsExtentID indexCBExtentID = mb->_indexExtent[i] ;
      dmsExtentID indexRoot = DMS_INVALID_EXTENT ;
      if ( indexCBExtentID == DMS_INVALID_EXTENT ||
           indexCBExtentID >= gPageNum )
      {
         dumpPrintf ( "Error: Index CB Extent ID is not valid: 0x%08lx (%d)"
                      OSS_NEWLINE, indexCBExtentID, indexCBExtentID ) ;
         ++err ;
         continue ;
      }
      if ( pExpBuffer )
      {
         dmsSpaceManagementExtent *pSME=( dmsSpaceManagementExtent*)pExpBuffer;
         if ( pSME->getBitMask( indexCBExtentID ) != DMS_SME_FREE )
         {
            dumpPrintf ( "Error: SME extent 0x%08lx (%d) is not free"
                         OSS_NEWLINE, indexCBExtentID, indexCBExtentID ) ;
            ++err ;
         }
         pSME->setBitMask( indexCBExtentID ) ;
      }
      rc = loadExtent ( file, extentType, pageSize, indexCBExtentID,
                        collectionID ) ;
      if ( rc )
      {
         dumpPrintf ( "Error: Failed to load extent, rc = %d"OSS_NEWLINE,
                      rc ) ;
         ++err ;
         continue ;
      }

retry :
      localErr = 0 ;
      len = dmsInspect::inspectIndexCBExtent ( gExtentBuffer, pageSize,
                                               gBuffer, gBufferSize,
                                               collectionID,
                                               indexRoot,
                                               localErr ) ;
      PD_TRACE1 ( SDB_INSPINXDEF, PD_PACK_UINT(len) );
      if ( len >= gBufferSize-1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry ;
      }
      flushOutput ( gBuffer, len ) ;
      err += localErr ;
      dumpPrintf ( OSS_NEWLINE ) ;
      indexRoots[i] = indexRoot ;
   }

done :
   if ( oldErr != err )
   {
      dumpPrintf ( " Inspect Index Def for Collection [%u] Done with (%d) Error"
                   OSS_NEWLINE, collectionID, err-oldErr ) ;
   }
   PD_TRACE1 ( SDB_INSPINXDEF, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_INSPINXDEF );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPINXDEF, "dumpIndexDef" )
void dumpIndexDef ( OSSFILE &file, SINT32 pageSize, UINT16 collectionID,
                    dmsMB *mb, std::map<UINT16, dmsExtentID> &indexRoots )
{
   UINT32 len = 0 ;
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_DUMPINXDEF );
   INSPECT_EXTENT_TYPE extentType = INSPECT_EXTENT_TYPE_INDEX_CB ;
   dumpPrintf ( " Dump Index Def for Collection [%u]"OSS_NEWLINE,
                collectionID ) ;
   if ( mb->_numIndexes > DMS_COLLECTION_MAX_INDEX )
   {
      dumpPrintf ( "Error: numIdx is out of range, max: %d"OSS_NEWLINE,
                   DMS_COLLECTION_MAX_INDEX ) ;
      mb->_numIndexes = DMS_COLLECTION_MAX_INDEX ;
   }
   for ( UINT16 i = 0 ; i < mb->_numIndexes; ++i )
   {
      dmsExtentID indexCBExtentID = mb->_indexExtent[i] ;
      dmsExtentID indexRoot = DMS_INVALID_EXTENT ;
      dumpPrintf ( "    Index [ %u ] : 0x%08lx"OSS_NEWLINE,
                   i, indexCBExtentID ) ;
      if ( indexCBExtentID == DMS_INVALID_EXTENT ||
           indexCBExtentID >= gPageNum )
      {
         dumpPrintf ( "Error: Index CB Extent ID is not valid: 0x%08lx (%d)"
                      OSS_NEWLINE, indexCBExtentID, indexCBExtentID ) ;
         continue ;
      }
      rc = loadExtent ( file, extentType, pageSize, indexCBExtentID,
                        collectionID ) ;
      if ( rc )
      {
         dumpPrintf ( "Error: Failed to load extent, rc = %d"OSS_NEWLINE,
                      rc ) ;
         continue ;
      }
retry :
      len = dmsDump::dumpIndexCBExtent ( gExtentBuffer, pageSize,
                                         gBuffer, gBufferSize,
                                         NULL,
                                         DMS_SU_DMP_OPT_HEX |
                                         DMS_SU_DMP_OPT_HEX_WITH_ASCII |
                                         DMS_SU_DMP_OPT_HEX_PREFIX_AS_ADDR |
                                         gDumpType,
                                         indexRoot ) ;
      PD_TRACE1 ( SDB_DUMPINXDEF, PD_PACK_UINT(len) );
      if ( len >= gBufferSize-1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry ;
      }
      flushOutput ( gBuffer, len ) ;
      dumpPrintf ( OSS_NEWLINE ) ;
      indexRoots[i] = indexRoot ;
   }

done :
   PD_TRACE1 ( SDB_DUMPINXDEF, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_DUMPINXDEF );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPINXEXTS, "inspectIndexExtents" )
void inspectIndexExtents ( OSSFILE &file, SINT32 pageSize,
                           dmsExtentID rootID, UINT16 collectionID,
                           CHAR *pExpBuffer, SINT32 &err )
{
   UINT32 len      = 0 ;
   INT32 rc        = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_INSPINXEXTS );
   SINT32 localErr = 0 ;
   std::deque<dmsExtentID> childExtents ;
   INSPECT_EXTENT_TYPE extentType = INSPECT_EXTENT_TYPE_INDEX ;
   childExtents.push_back ( rootID ) ;

   while ( !childExtents.empty() )
   {
      dmsExtentID childID = childExtents.front() ;
      childExtents.pop_front() ;

      if ( childID == DMS_INVALID_EXTENT || childID >= gPageNum )
      {
         dumpPrintf ( "Error: index extent ID is not valid: 0x%08lx (%d)"
                      OSS_NEWLINE, childID, childID ) ;
         ++err ;
         continue ;
      }
      if ( pExpBuffer )
      {
         dmsSpaceManagementExtent *pSME=(dmsSpaceManagementExtent*)pExpBuffer ;
         if ( pSME->getBitMask( childID ) != DMS_SME_FREE )
         {
            dumpPrintf ( "Error: SME extent 0x%08lx (%d) is not free"
                         OSS_NEWLINE, childID, childID ) ;
            ++err ;
         }
         pSME->setBitMask( childID ) ;
      }

      rc = loadExtent ( file, extentType, pageSize, childID, collectionID ) ;
      if ( rc )
      {
         dumpPrintf ( "Error: Failed to load extent %d, rc = %d"OSS_NEWLINE,
                      childID, rc ) ;
         ++err ;
         continue ;
      }

retry :
      localErr = 0 ;
      len = dmsInspect::inspectIndexExtent ( cb, gExtentBuffer, pageSize,
                                             gBuffer, gBufferSize,
                                             collectionID,
                                             childID, childExtents,
                                             localErr ) ;
      PD_TRACE1 ( SDB_INSPINXEXTS, PD_PACK_UINT(len) );
      if ( len >= gBufferSize-1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry ;
      }
      flushOutput ( gBuffer, len ) ;
      err += localErr ;
   }

done :
   PD_TRACE1 ( SDB_INSPINXEXTS, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_INSPINXEXTS );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPINXEXTS, "dumpIndexExtents" )
void dumpIndexExtents ( OSSFILE &file, SINT32 pageSize,
                        dmsExtentID rootID, UINT16 collectionID )
{
   UINT32 len = 0 ;
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_DUMPINXEXTS );
   std::deque<dmsExtentID> childExtents ;
   INSPECT_EXTENT_TYPE extentType = INSPECT_EXTENT_TYPE_INDEX ;
   childExtents.push_back ( rootID ) ;
   UINT32 count = 0 ;

   while ( !childExtents.empty() )
   {
      dmsExtentID childID = childExtents.front() ;
      childExtents.pop_front() ;
      ++count ;

      dumpPrintf ( "    Dump Index Page %d:"OSS_NEWLINE, childID ) ;
      if ( childID == DMS_INVALID_EXTENT || childID >= gPageNum )
      {
         dumpPrintf ( "Error: index extent ID is not valid: 0x%08lx (%d)"
                      OSS_NEWLINE, childID, childID ) ;
         continue ;
      }
      rc = loadExtent ( file, extentType,
                        pageSize, childID, collectionID ) ;
      if ( rc )
      {
         dumpPrintf ( "Error: Failed to load extent %d, rc = %d"OSS_NEWLINE,
                      childID, rc ) ;
         continue ;
      }

retry :
      len = dmsDump::dumpIndexExtent( gExtentBuffer, pageSize,
                                      gBuffer, gBufferSize, NULL,
                                      DMS_SU_DMP_OPT_HEX |
                                      DMS_SU_DMP_OPT_HEX_WITH_ASCII |
                                      DMS_SU_DMP_OPT_HEX_PREFIX_AS_ADDR |
                                      gDumpType,
                                      childExtents,
                                      gShowRecordContent ) ;
      PD_TRACE1 ( SDB_DUMPINXEXTS, PD_PACK_UINT(len) );
      if ( len >= gBufferSize-1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry ;
      }
      flushOutput ( gBuffer, len ) ;
      dumpPrintf ( OSS_NEWLINE ) ;
   }
   dumpPrintf ( "    Total: %u pages"OSS_NEWLINE, count ) ;
   dumpPrintf ( OSS_NEWLINE ) ;

done :
   PD_TRACE1 ( SDB_DUMPINXEXTS, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_DUMPINXEXTS );
   return ;
error :
   goto done ;
}

void inspectCollectionData( OSSFILE &file, SINT32 pageSize, UINT16 id,
                            SINT32 hwm, CHAR *pExpBuffer, SINT32 &err )
{
   INT32 rc        = SDB_OK ;
   INT32 len       = 0 ;
   SINT32 localErr = 0 ;
   std::set<dmsRecordID> extentRIDList ;
   INSPECT_EXTENT_TYPE extentType = INSPECT_EXTENT_TYPE_DATA ;
   dmsMB *mb = NULL ;
   dmsExtentID tempExtent = DMS_INVALID_EXTENT ;
   dmsExtentID firstExtent = DMS_INVALID_EXTENT ;

   rc = loadMB ( id, mb ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to load metadata block, rc = %d"OSS_NEWLINE,
                   rc ) ;
      ++err ;
      goto error ;
   }
   firstExtent = mb->_firstExtentID ;
   dumpPrintf ( " Inspect Data for collection [%d]"OSS_NEWLINE, id ) ;
   while ( DMS_INVALID_EXTENT != firstExtent )
   {
      if ( firstExtent >= gPageNum )
      {
         dumpPrintf ( "Error: data extent 0x%08lx (%d) is out of range"
                      OSS_NEWLINE, firstExtent, firstExtent ) ;
         ++err ;
         break ;
      }
      rc = loadExtent ( file, extentType, pageSize, firstExtent, id ) ;
      if ( rc )
      {
         dumpPrintf ( "Error: Failed to load extent %d, rc = %d"OSS_NEWLINE,
                      firstExtent, rc ) ;
         ++err ;
         goto error ;
      }

      if ( pExpBuffer )
      {
         dmsSpaceManagementExtent *pSME=(dmsSpaceManagementExtent*)pExpBuffer ;

         for ( INT32 i = 0 ; i < ((dmsExtent*)gExtentBuffer)->_blockSize ;
               ++i )
         {
            if ( pSME->getBitMask( firstExtent + i ) != DMS_SME_FREE )
            {
               dumpPrintf ( "Error: SME extent 0x%08lx (%d) is not free"
                            OSS_NEWLINE, firstExtent + i , firstExtent + i ) ;
               ++err ;
            }
            pSME->setBitMask( firstExtent + i ) ;
         }
      }

retry_data :
      extentRIDList.clear() ;
      tempExtent = firstExtent ;
      localErr = 0 ;
      len = dmsInspect::inspectDataExtent ( cb, gExtentBuffer,
                               ((dmsExtent*)gExtentBuffer)->_blockSize*pageSize,
                               gBuffer, gBufferSize, hwm, id, tempExtent,
                               &extentRIDList, localErr ) ;
      if ( (UINT32)len >= gBufferSize-1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry_data ;
      }

      flushOutput ( gBuffer, len ) ;

      if ( extentRIDList.size() != 0 )
      {
         inspectOverflowedRecords( file, pageSize, id, firstExtent,
                                   extentRIDList, err ) ;
      }

      firstExtent = tempExtent ;
      err += localErr ;
   } //end while

done :
   return ;
error :
   goto done ;
}

void inspectCollectionIndex( OSSFILE &file, SINT32 pageSize, UINT16 id,
                             SINT32 hwm, CHAR *pExpBuffer, SINT32 &err )
{
   INT32 rc        = SDB_OK ;
   dmsMB *mb       = NULL ;
   std::map<UINT16, dmsExtentID> indexRoots ;
   std::map<UINT16, dmsExtentID>::iterator it ;

   rc = loadMB ( id, mb ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to load metadata block, rc = %d"OSS_NEWLINE,
                   rc ) ;
      ++err ;
      goto error ;
   }

   inspectIndexDef ( file, pageSize, id, mb, pExpBuffer, indexRoots, err ) ;

   for ( it = indexRoots.begin() ; it != indexRoots.end() ; ++it )
   {
      UINT16 indexID = it->first ;
      dumpPrintf ( "    Index Inspection for Collection [%02u], Index [%02u]"
                   OSS_NEWLINE, id, indexID ) ;
      dmsExtentID rootID = it->second ;
      if ( DMS_INVALID_EXTENT != rootID )
      {
         inspectIndexExtents ( file, pageSize, rootID, id,
                               pExpBuffer, err ) ;
      }
   }

done :
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPCOLL, "inspectCollection" )
void inspectCollection ( OSSFILE &file, SINT32 pageSize, UINT16 id,
                         SINT32 hwm, CHAR *pExpBuffer, SINT32 &err )
{
   if ( SDB_INSPT_DATA == gCurInsptType )
   {
      inspectCollectionData( file, pageSize, id, hwm, pExpBuffer, err ) ;
   }
   else if ( SDB_INSPT_INDEX == gCurInsptType )
   {
      inspectCollectionIndex( file, pageSize, id, hwm, pExpBuffer, err ) ;
   }
}

void dumpCollectionData( OSSFILE &file, SINT32 pageSize, UINT16 id )
{
   INT32 rc = SDB_OK ;
   INT32 len = 0 ;
   std::set<dmsRecordID> extentRIDList ;
   INSPECT_EXTENT_TYPE extentType = INSPECT_EXTENT_TYPE_DATA ;
   dmsMB *mb = NULL ;
   dmsExtentID tempExtent = DMS_INVALID_EXTENT ;
   dmsExtentID firstExtent = DMS_INVALID_EXTENT ;

   rc = loadMB ( id, mb ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to load metadata block, rc = %d"OSS_NEWLINE,
                   rc ) ;
      goto error ;
   }

   if ( DMS_INVALID_EXTENT != mb->_mbExExtentID )
   {
      UINT32 size = 0 ;
      extentType = INSPECT_EXTENT_TYPE_MBEX ;

      dumpPrintf ( " Dump Meta Extent for Collection [%d]"OSS_NEWLINE, id ) ;

      rc = loadExtent( file, extentType, pageSize, mb->_mbExExtentID, id ) ;
      if ( rc )
      {
         dumpPrintf( "Error: Failed to load mb expand extent %d, rc = %d"
                     OSS_NEWLINE, mb->_mbExExtentID, rc ) ;
         goto error ;
      }
      size = ((dmsMetaExtent*)gExtentBuffer)->_blockSize*pageSize ;

   retry_mbEx:
      len = dmsDump::dumpMBEx( gExtentBuffer, size,
                               gBuffer, gBufferSize, NULL,
                               DMS_SU_DMP_OPT_HEX |
                               DMS_SU_DMP_OPT_HEX_WITH_ASCII |
                               DMS_SU_DMP_OPT_HEX_PREFIX_AS_ADDR |
                               gDumpType, mb->_mbExExtentID ) ;
      if ( (UINT32)len >= gBufferSize -1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry_mbEx ;
      }
      flushOutput ( gBuffer, len ) ;
   }

   extentType = INSPECT_EXTENT_TYPE_DATA ;
   firstExtent = mb->_firstExtentID ;
   dumpPrintf ( " Dump Data for Collection [%d]"OSS_NEWLINE, id ) ;
   while ( DMS_INVALID_EXTENT != firstExtent )
   {
      rc = loadExtent ( file, extentType, pageSize, firstExtent, id ) ;
      if ( rc )
      {
         dumpPrintf ( "Error: Failed to load extent %d, rc = %d"OSS_NEWLINE,
                      firstExtent, rc ) ;
         goto error ;
      }

retry_data :
      extentRIDList.clear() ;
      tempExtent = firstExtent ;
      len = dmsDump::dumpDataExtent ( cb, gExtentBuffer,
                               ((dmsExtent*)gExtentBuffer)->_blockSize*pageSize,
                               gBuffer, gBufferSize, NULL,
                               DMS_SU_DMP_OPT_HEX |
                               DMS_SU_DMP_OPT_HEX_WITH_ASCII |
                               DMS_SU_DMP_OPT_HEX_PREFIX_AS_ADDR |
                               gDumpType, tempExtent, &extentRIDList,
                               gShowRecordContent ) ;
      PD_TRACE1 ( SDB_DUMPCOLL, PD_PACK_INT(len) );
      if ( (UINT32)len >= gBufferSize-1 )
      {
         if ( reallocBuffer () )
         {
            clearBuffer () ;
            goto error ;
         }
         goto retry_data ;
      }
      flushOutput ( gBuffer, len ) ;

      if ( extentRIDList.size() != 0 && gShowRecordContent )
      {
         dumpOverflowedRecords ( file, pageSize, id, firstExtent,
                                 extentRIDList ) ;
      }

      firstExtent = tempExtent ;
   }

done :
   return ;
error :
   goto done ;
}

void dumpCollectionIndex( OSSFILE &file, SINT32 pageSize, UINT16 id )
{
   INT32 rc = SDB_OK ;
   dmsMB *mb = NULL ;
   std::map<UINT16, dmsExtentID> indexRoots ;
   std::map<UINT16, dmsExtentID>::iterator it ;

   rc = loadMB ( id, mb ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to load metadata block, rc = %d"OSS_NEWLINE,
                   rc ) ;
      goto error ;
   }

   dumpIndexDef ( file, pageSize, id, mb, indexRoots ) ;
   for ( it = indexRoots.begin() ; it != indexRoots.end() ; ++it )
   {
      UINT16 indexID = it->first ;
      dmsExtentID rootID = it->second ;
      dumpPrintf ( "    Index Dump for Collection [%02u], Index [%02u]"
                   OSS_NEWLINE, id, indexID ) ;
      if ( DMS_INVALID_EXTENT != rootID )
      {
         dumpIndexExtents ( file, pageSize, rootID, id ) ;
      }
      else
      {
         dumpPrintf ( "       Root extent does not exist"OSS_NEWLINE ) ;
      }
   }

done :
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPCOLL, "dumpCollection" )
void dumpCollection ( OSSFILE &file, SINT32 pageSize, UINT16 id )
{
   if ( SDB_INSPT_DATA == gCurInsptType )
   {
      dumpCollectionData( file, pageSize, id ) ;
   }
   else if ( SDB_INSPT_INDEX == gCurInsptType )
   {
      dumpCollectionIndex( file, pageSize, id ) ;
   }
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPCOLLS, "inspectCollections" )
void inspectCollections ( OSSFILE &file, SINT32 pageSize,
                          vector<UINT16> &collections,
                          SINT32 hwm, CHAR *pExpBuffer,
                          SINT32 &err )
{
   PD_TRACE_ENTRY ( SDB_INSPCOLLS );
   vector<UINT16>::iterator it ;
   for ( it = collections.begin(); it != collections.end(); ++it )
   {
      inspectCollection ( file, pageSize, *it, hwm, pExpBuffer, err ) ;
   }
   PD_TRACE_EXIT ( SDB_INSPCOLLS );
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPCOLLS, "dumpCollections" )
void dumpCollections ( OSSFILE &file, SINT32 pageSize,
                       vector<UINT16> &collections )
{
   PD_TRACE_ENTRY ( SDB_DUMPCOLLS );
   vector<UINT16>::iterator it ;
   for ( it = collections.begin() ; it != collections.end() ; ++it )
   {
      dumpCollection ( file, pageSize, *it ) ;
   }
   PD_TRACE_EXIT ( SDB_DUMPCOLLS );
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPCOLLECTIONS, "inspectCollections" )
void inspectCollections ( OSSFILE &file, SINT32 pageSize, SINT32 hwm,
                          CHAR *pExpBuffer, SINT32 &err )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_INSPCOLLECTIONS );
   UINT32 len ;
   SINT32 localErr = 0 ;
   vector<UINT16> collections ;

   if ( FALSE == gInitMME )
   {
      SINT64 lenRead = 0 ;
      rc = ossSeekAndRead ( &file, DMS_MME_OFFSET, gMMEBuff,
                            DMS_MME_SZ, &lenRead ) ;
      if ( rc || lenRead != DMS_MME_SZ )
      {
         dumpPrintf ( "Error: Failed to read sme, read %lld bytes, "
                      "rc = %d"OSS_NEWLINE, lenRead, rc ) ;
         ++err ;
         goto error ;
      }
      gInitMME = TRUE ;
   }

retry :
   collections.clear() ;
   localErr = 0 ;
   len = dmsInspect::inspectMME ( gMMEBuff, DMS_MME_SZ,
                                  gBuffer, gBufferSize,
                                  ossStrlen ( gCLName ) ? gCLName : NULL,
                                  ( SDB_INSPT_DATA==gCurInsptType ) ? hwm : -1,
                                  collections, localErr ) ;
   PD_TRACE1 ( SDB_INSPCOLLECTIONS, PD_PACK_UINT(len) );
   if ( len >= gBufferSize-1 )
   {
      if ( reallocBuffer () )
      {
         clearBuffer () ;
         goto error ;
      }
      goto retry ;
   }
   flushOutput ( gBuffer, len ) ;
   err += localErr ;

   if ( collections.size() != 0 )
   {
      inspectCollections ( file, pageSize, collections, hwm,
                           pExpBuffer, err ) ;
   }
   if ( pExpBuffer )
   {
      inspectSME ( file, pExpBuffer, hwm, err ) ;
   }

done :
   PD_TRACE1 ( SDB_INSPCOLLECTIONS, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_INSPCOLLECTIONS );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPRAWPAGE, "dumpRawPage" )
void dumpRawPage ( OSSFILE &file, SINT32 pageSize, SINT32 pageID )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_DUMPRAWPAGE );
   UINT32 len ;
   rc = getExtent ( file, pageID, pageSize, 1 ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to get extent %d, rc = %d"
                   OSS_NEWLINE, pageID, rc ) ;
      goto error ;
   }
retry :
   len = dmsDump::dumpRawPage ( gExtentBuffer, gExtentBufferSize,
                                gBuffer, gBufferSize ) ;
   PD_TRACE1 ( SDB_DUMPRAWPAGE, PD_PACK_UINT(len) );
   if ( len >= gBufferSize-1 )
   {
      if ( reallocBuffer () )
      {
         clearBuffer () ;
         goto error ;
      }
      goto retry ;
   }
   flushOutput ( gBuffer, len ) ;
   dumpPrintf ( OSS_NEWLINE ) ;

done :
   PD_TRACE1 ( SDB_DUMPRAWPAGE, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_DUMPRAWPAGE );
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPCOLLECTIONS, "dumpCollections" )
void dumpCollections ( OSSFILE &file, SINT32 pageSize )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_DUMPCOLLECTIONS ) ;
   UINT32 len ;
   vector<UINT16> collections ;

   if ( FALSE == gInitMME )
   {
      SINT64 lenRead = 0 ;
      rc = ossSeekAndRead ( &file, DMS_MME_OFFSET, gMMEBuff,
                            DMS_MME_SZ, &lenRead ) ;
      if ( rc || lenRead != DMS_MME_SZ )
      {
         dumpPrintf ( "Error: Failed to read sme, read %lld bytes, "
                      "rc = %d"OSS_NEWLINE, lenRead, rc ) ;
         goto error ;
      }
      gInitMME = TRUE ;
   }

retry :
   collections.clear() ;
   len = dmsDump::dumpMME ( gMMEBuff, DMS_MME_SZ,
                            gBuffer, gBufferSize, NULL,
                            DMS_SU_DMP_OPT_HEX |
                            DMS_SU_DMP_OPT_HEX_WITH_ASCII |
                            DMS_SU_DMP_OPT_HEX_PREFIX_AS_ADDR |
                            gDumpType,
                            ossStrlen ( gCLName ) ? gCLName : NULL,
                            collections ) ;
   PD_TRACE1 ( SDB_DUMPCOLLECTIONS, PD_PACK_UINT(len) );
   if ( len >= gBufferSize-1 )
   {
      if ( reallocBuffer () )
      {
         clearBuffer () ;
         goto error ;
      }
      goto retry ;
   }

   if ( SDB_INSPT_DATA == gCurInsptType )
   {
      flushOutput ( gBuffer, len ) ;
   }

   if ( collections.size() != 0 )
   {
      dumpCollections ( file, pageSize, collections ) ;
   }

done :
   PD_TRACE1 ( SDB_DUMPCOLLECTIONS, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_DUMPCOLLECTIONS );
   return ;
error :
   goto done ;
}

enum SDB_INSPT_ACTION
{
   SDB_INSPT_ACTION_DUMP = 0,
   SDB_INSPT_ACTION_INSPECT
} ;

// PD_TRACE_DECLARE_FUNCTION ( SDB_ACTIONCSATTEMPT, "actionCSAttempt" )
void actionCSAttempt ( const CHAR *pFile, const CHAR *expectEye,
                       BOOLEAN specific, SDB_INSPT_ACTION action )
{
   INT32    rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_ACTIONCSATTEMPT );
   OSSFILE  file ;
   BOOLEAN  isOpen = FALSE ;
   SINT32   csPageSize = 0 ;
   CHAR     eyeCatcher[DMS_HEADER_EYECATCHER_LEN+1] = {0} ;
   SINT64   readSize = 0 ;
   CHAR     *inspectSMEBuffer = NULL ;

   SINT64   restLen = 0 ;
   SINT64   readPos = 0 ;

   rc = ossOpen ( pFile, OSS_DEFAULT | OSS_READONLY | OSS_EXCLUSIVE,
                  OSS_RU | OSS_WU | OSS_RG, file ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to open %s, rc = %d"OSS_NEWLINE,
                   pFile, rc ) ;
      goto error ;
   }

   isOpen = TRUE ;

   restLen = DMS_HEADER_EYECATCHER_LEN ;
   while ( restLen > 0 )
   {
      rc = ossRead ( &file, eyeCatcher + readPos, restLen, &readSize ) ;
      if ( rc && SDB_INTERRUPT != rc )
      {
         dumpPrintf ( "Error: Failed to read %s, rc = %d"OSS_NEWLINE,
                      pFile, rc ) ;
         goto error ;
      }
      rc = SDB_OK ;
      restLen -= readSize ;
      readPos += readSize ;
   }

   if ( ossStrncmp ( eyeCatcher, expectEye, DMS_HEADER_EYECATCHER_LEN ) )
   {
      if ( specific )
      {
         dumpPrintf ( "Error: %s is not a valid storage unit"OSS_NEWLINE,
                      pFile ) ;
      }
      goto done ;
   }

   rc = ossSeek ( &file, 0, OSS_SEEK_SET ) ;
   if ( rc )
   {
      dumpPrintf ( "Error, Failed to seek to beginning of the file %s"
                   OSS_NEWLINE, pFile ) ;
      goto error ;
   }

   switch ( action )
   {
   case SDB_INSPT_ACTION_DUMP :
      if ( gStartingPage < 0 )
      {
         dumpPrintf ( " Dump collection space %s"OSS_NEWLINE, pFile ) ;
         dumpHeader ( file, csPageSize ) ;
         if ( csPageSize != DMS_PAGE_SIZE4K &&
              csPageSize != DMS_PAGE_SIZE8K &&
              csPageSize != DMS_PAGE_SIZE16K &&
              csPageSize != DMS_PAGE_SIZE32K &&
              csPageSize != DMS_PAGE_SIZE64K )
         {
            dumpPrintf ( "Error: %s page size is not valid: %d"OSS_NEWLINE,
                         pFile, csPageSize ) ;
            goto error ;
         }
         dumpSME ( file ) ;
         dumpCollections ( file, csPageSize ) ;
         dumpPrintf ( " Dump collection space is done"OSS_NEWLINE ) ;
      }
      else
      {
         dumpHeader ( file, csPageSize ) ;
         for ( SINT32 i = 0; i < gNumPages && !gReachEnd; ++i )
         {
            dumpPrintf ( " Dump page %d"OSS_NEWLINE, gStartingPage + i ) ;
            dumpRawPage ( file, csPageSize, gStartingPage + i ) ;
         }
      }
      break ;
   case SDB_INSPT_ACTION_INSPECT :
   {
      INT32 totalErr         = 0 ;
      SINT32 hwm             = 0 ;
      dumpPrintf ( " Inspect collection space %s"OSS_NEWLINE, pFile ) ;
      inspectHeader ( file, csPageSize, totalErr ) ;
      if ( csPageSize != DMS_PAGE_SIZE4K &&
           csPageSize != DMS_PAGE_SIZE8K &&
           csPageSize != DMS_PAGE_SIZE16K &&
           csPageSize != DMS_PAGE_SIZE32K &&
           csPageSize != DMS_PAGE_SIZE64K )
      {
         dumpPrintf ( "Error: %s page size is not valid: %d"OSS_NEWLINE,
                      pFile, csPageSize ) ;
         goto error ;
      }
      inspectSME ( file, NULL, hwm, totalErr ) ;

      if ( ossStrlen ( gCLName ) == 0 )
      {
         inspectSMEBuffer = (CHAR*)SDB_OSS_MALLOC ( DMS_SME_SZ ) ;
         dmsSpaceManagementExtent *pSME =
            ( dmsSpaceManagementExtent*)inspectSMEBuffer ;
         if ( !inspectSMEBuffer )
         {
            dumpPrintf ( "Error: Failed to allocate %d bytes for Exp SME buffer"
                         OSS_NEWLINE, (INT32)DMS_SME_SZ ) ;
            goto error ;
         }
         for ( UINT32 i = 0; i < DMS_SME_SZ ; ++i )
         {
            pSME->freeBitMask( i ) ;
         }
      }

      inspectCollections ( file, csPageSize, hwm, inspectSMEBuffer,
                           totalErr ) ;
      if ( 0 == totalErr )
      {
         dumpPrintf ( " Inspection collection space is Done without Error"
                      OSS_NEWLINE ) ;
      }
      else
      {
         dumpPrintf ( " Inspection collection space is Done with %d Error(s)"
                      OSS_NEWLINE, totalErr ) ;
      }
      break ;
   }
   default :
      dumpPrintf ( "Error: unexpected action"OSS_NEWLINE ) ;
      goto error ;
   }
   dumpPrintf ( OSS_NEWLINE ) ;

done :
   if ( isOpen )
   {
      ossClose ( file ) ;
   }
   if ( inspectSMEBuffer )
   {
      SDB_OSS_FREE ( inspectSMEBuffer ) ;
   }
   PD_TRACE1 ( SDB_ACTIONCSATTEMPT, PD_PACK_INT(rc) );
   PD_TRACE_EXIT ( SDB_ACTIONCSATTEMPT );
   return ;
error :
   goto done ;
}

INT32 prepareForDump( const CHAR *csName, UINT32 sequence )
{
   string csFileName = rtnMakeSUFileName( csName, sequence,
                                          DMS_DATA_SU_EXT_NAME ) ;
   string csFullName = rtnFullPathName( gDatabasePath, csFileName ) ;

   OSSFILE file ;
   BOOLEAN isOpen = FALSE ;
   INT32 rc = SDB_OK ;
   dmsStorageUnitHeader dataHeader ;
   SINT64 lenRead                      = 0 ;

   rc = ossOpen ( csFullName.c_str(), OSS_DEFAULT|OSS_READONLY|OSS_EXCLUSIVE,
                  OSS_RU | OSS_WU | OSS_RG, file ) ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to open %s, rc = %d"OSS_NEWLINE,
                   csFullName.c_str(), rc ) ;
      goto error ;
   }

   isOpen = TRUE ;

   rc = ossSeekAndRead ( &file, DMS_HEADER_OFFSET, (CHAR *)&dataHeader,
                         DMS_HEADER_SZ, &lenRead ) ;
   if ( rc || lenRead != DMS_HEADER_SZ )
   {
      dumpPrintf ( "Error: Failed to read header, read %lld bytes, "
                   "rc = %d"OSS_NEWLINE, lenRead, rc ) ;
      goto error ;
   }
   if ( 0 != ossStrncmp( dataHeader._eyeCatcher, DMS_DATASU_EYECATCHER,
                         DMS_HEADER_EYECATCHER_LEN ) )
   {
      dumpPrintf ( "Error: File[%s] is not dms storage unit data file"
                   OSS_NEWLINE, csFullName.c_str() ) ;
      goto error ;
   }

   gPageSize = dataHeader._pageSize ;
   gSecretValue = dataHeader._secretValue ;

   rc = ossSeekAndRead ( &file, DMS_MME_OFFSET, gMMEBuff,
                         DMS_MME_SZ, &lenRead ) ;
   if ( rc || lenRead != DMS_MME_SZ )
   {
      dumpPrintf ( "Error: Failed to read sme, read %lld bytes, "
                   "rc = %d"OSS_NEWLINE, lenRead, rc ) ;
      goto error ;
   }
   gInitMME = TRUE ;

done:
   if ( isOpen )
   {
      ossClose ( file ) ;
   }
   return rc ;
error:
   if ( SDB_OK == rc )
   {
      rc = SDB_SYS ;
   }
   goto done ;
}

void actionCSAttemptEntry( const CHAR *csName, UINT32 sequence,
                           BOOLEAN specific, SDB_INSPT_ACTION action )
{
   if ( !gHitCS )
   {
      gHitCS = TRUE ;
   }

   gReachEnd = FALSE ;

   string csFileName ;
   string csFullName ;

   gPageSize      = 0 ;
   gSecretValue   = 0 ;
   gInitMME       = FALSE ;
   ossMemset( gMMEBuff, 0, DMS_MME_SZ ) ;

   if ( SDB_OK != prepareForDump( csName, sequence ) )
   {
      return ;
   }

   if ( gDumpData )
   {
      csFileName = rtnMakeSUFileName( csName, sequence,
                                      DMS_DATA_SU_EXT_NAME ) ;
      csFullName = rtnFullPathName( gDatabasePath, csFileName ) ;
      gDataOffset = DMS_MME_OFFSET + DMS_MME_SZ ;
      gPageNum    = 0 ;
      gCurInsptType = SDB_INSPT_DATA ;

      actionCSAttempt( csFullName.c_str(), DMS_DATASU_EYECATCHER,
                       specific, action ) ;
   }
   if ( gDumpIndex )
   {
      csFileName = rtnMakeSUFileName( csName, sequence,
                                      DMS_INDEX_SU_EXT_NAME ) ;
      csFullName = rtnFullPathName( gDatabasePath, csFileName ) ;
      gDataOffset = DMS_SME_OFFSET + DMS_SME_SZ ;
      gPageNum    = 0 ;
      gCurInsptType = SDB_INSPT_INDEX ;

      actionCSAttempt( csFullName.c_str(), DMS_INDEXSU_EYECATCHER,
                       specific, action ) ;
   }
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPPAGES, "dumpPages" )
void dumpPages ()
{
   PD_TRACE_ENTRY ( SDB_DUMPPAGES ) ;
   CHAR csName [ DMS_COLLECTION_SPACE_NAME_SZ + 1 ] = {0} ;
   UINT32 sequence = 0 ;
   fs::path dbDir ( gDatabasePath ) ;
   fs::directory_iterator end_iter ;

   if ( ossStrlen ( gCSName ) == 0 )
   {
      dumpAndShowPrintf ( "Colletion Space Name must be specified for page dump"
                          OSS_NEWLINE ) ;
      goto error ;
   }

   if ( fs::exists ( dbDir ) && fs::is_directory ( dbDir ) )
   {
      for ( fs::directory_iterator dir_iter ( dbDir );
            dir_iter != end_iter; ++dir_iter )
      {
         if ( fs::is_regular_file ( dir_iter->status() ) )
         {
            const std::string fileName = dir_iter->path().filename().string() ;
            const CHAR *pFileName = fileName.c_str() ;
            if ( rtnVerifyCollectionSpaceFileName ( pFileName, csName,
                             DMS_COLLECTION_SPACE_NAME_SZ, sequence ) )
            {
               if ( ossStrncmp ( gCSName, csName,
                                 DMS_COLLECTION_SPACE_NAME_SZ ) == 0 )
               {
                  actionCSAttemptEntry ( csName, sequence, TRUE,
                                         SDB_INSPT_ACTION_DUMP ) ;
               }
            }
         }
      }
   }
   else
   {
      dumpPrintf ( "Error: dump path %s is not a valid directory"OSS_NEWLINE,
                   gDatabasePath ) ;
   }

done :
   PD_TRACE_EXIT ( SDB_DUMPPAGES ) ;
   return ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DUMPDB, "dumpDB" )
void dumpDB ()
{
   PD_TRACE_ENTRY ( SDB_DUMPDB ) ;
   CHAR csName [ DMS_COLLECTION_SPACE_NAME_SZ + 1 ] = {0} ;
   UINT32 sequence = 0 ;
   fs::path dbDir ( gDatabasePath ) ;
   fs::directory_iterator end_iter ;
   if ( fs::exists ( dbDir ) && fs::is_directory ( dbDir ) )
   {
      for ( fs::directory_iterator dir_iter ( dbDir );
            dir_iter != end_iter; ++dir_iter )
      {
         if ( fs::is_regular_file ( dir_iter->status() ) )
         {
            const std::string fileName = dir_iter->path().filename().string() ;
            const CHAR *pFileName = fileName.c_str() ;
            if ( rtnVerifyCollectionSpaceFileName ( pFileName, csName,
                               DMS_COLLECTION_SPACE_NAME_SZ, sequence ) )
            {
               if ( ossStrlen ( gCSName ) == 0 ||
                    ossStrncmp ( gCSName, csName,
                                 DMS_COLLECTION_SPACE_NAME_SZ ) == 0 )
               {
                  actionCSAttemptEntry ( csName, sequence,
                                         ossStrlen ( gCSName ) != 0,
                                         SDB_INSPT_ACTION_DUMP ) ;
               }
            }
         }
      }
   }
   else
   {
      dumpPrintf ( "Error: dump path %s is not a valid directory"OSS_NEWLINE,
                   gDatabasePath ) ;
   }
   PD_TRACE_EXIT ( SDB_DUMPDB );
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_INSPECTDB, "inspectDB" )
void inspectDB ()
{
   PD_TRACE_ENTRY ( SDB_INSPECTDB );
   CHAR csName [ DMS_COLLECTION_SPACE_NAME_SZ + 1 ] = {0} ;
   UINT32 sequence = 0 ;
   fs::path dbDir ( gDatabasePath ) ;
   fs::directory_iterator end_iter ;
   if ( fs::exists ( dbDir ) && fs::is_directory ( dbDir ) )
   {
      for ( fs::directory_iterator dir_iter ( dbDir );
            dir_iter != end_iter; ++dir_iter )
      {
         if ( fs::is_regular_file ( dir_iter->status() ) )
         {
            const std::string fileName = dir_iter->path().filename().string() ;
            const CHAR *pFileName = fileName.c_str() ;
            if ( rtnVerifyCollectionSpaceFileName ( pFileName, csName,
                                DMS_COLLECTION_SPACE_NAME_SZ, sequence ) )
            {
               if ( ossStrlen ( gCSName ) == 0 ||
                    ossStrncmp ( gCSName, csName,
                                 DMS_COLLECTION_SPACE_NAME_SZ ) == 0 )
               {
                  actionCSAttemptEntry ( csName, sequence,
                                         ossStrlen ( gCSName ) != 0,
                                         SDB_INSPT_ACTION_INSPECT ) ;
               }
            }
         }
      }
   }
   else
   {
      dumpPrintf ( "Error: inspect path %s is not a valid directory"OSS_NEWLINE,
                   gDatabasePath ) ;
   }
   PD_TRACE_EXIT ( SDB_INSPECTDB );
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDBINSPT_MAIN, "main" )
INT32 main ( INT32 argc, CHAR **argv )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_SDBINSPT_MAIN );
   po::options_description desc ( "Command options" ) ;
   init ( desc ) ;
   rc = resolveArgument ( desc, argc, argv ) ;
   if ( rc )
   {
      if ( SDB_PMD_HELP_ONLY != rc )
      {
         dumpPrintf ( "Error: Invalid arguments"OSS_NEWLINE ) ;
         displayArg ( desc ) ;
      }
      goto done ;
   }
   if ( OSS_BIT_TEST ( gAction, ACTION_DUMP ) &&
        FALSE == gDumpData && FALSE == gDumpIndex )
   {
      dumpPrintf( "Error: should specific dump data or index " ) ;
      goto done ;
   }
   if ( gDumpData == gDumpIndex && gStartingPage >= 0 )
   {
      dumpPrintf( "Error: dump from starting page only for the one of data or "
                  "index" ) ;
      goto done ;
   }

   gMMEBuff = (CHAR*)SDB_OSS_MALLOC( DMS_MME_SZ ) ;
   if ( !gMMEBuff )
   {
      dumpPrintf ( "Error: Failed to allocate mme buffer, exit"OSS_NEWLINE ) ;
      goto done ;
   }
   ossMemset( gMMEBuff, 0, DMS_MME_SZ ) ;

   rc = reallocBuffer () ;
   if ( rc )
   {
      dumpPrintf ( "Error: Failed to realloc buffer, exit"OSS_NEWLINE ) ;
      goto done ;
   }
   cb = SDB_OSS_NEW pmdEDUCB ( NULL, EDU_TYPE_AGENT ) ;
   if ( !cb )
   {
      dumpPrintf ( "Failed to allocate memory for educb"OSS_NEWLINE ) ;
      goto done ;
   }
   if ( OSS_BIT_TEST ( gAction, ACTION_INSPECT ) )
   {
      inspectDB () ;
   }

   if ( OSS_BIT_TEST ( gAction, ACTION_DUMP ) )
   {
      if ( gStartingPage >= 0 )
      {
         dumpPages () ;
      }
      else
      {
         dumpDB () ;
      }
   }

   if ( 0 != ossStrlen( gCSName ) && !gHitCS )
   {
      dumpPrintf( "Warning: Cannot find any collection space named %s",
                  gCSName ) ;
   }

done :
   if ( gBuffer )
   {
      SDB_OSS_FREE ( gBuffer ) ;
      gBuffer = NULL ;
   }
   if ( gExtentBuffer )
   {
      SDB_OSS_FREE ( gExtentBuffer ) ;
      gExtentBuffer = NULL ;
   }
   if ( gMMEBuff )
   {
      SDB_OSS_FREE ( gMMEBuff ) ;
      gMMEBuff = NULL ;
   }
   if ( ossStrlen ( gOutputFile ) != 0 )
   {
      ossClose ( gFile ) ;
   }
   if ( cb )
   {
      SDB_OSS_DEL ( cb ) ;
   }
   PD_TRACE_EXITRC ( SDB_SDBINSPT_MAIN, rc );
   return rc ;
}
