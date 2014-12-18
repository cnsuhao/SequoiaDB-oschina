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

   Source File Name = sdbCheckDumpMem.cpp

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

#include "core.hpp"
#include "ossIO.hpp"
#include "oss.hpp"
#include "ossMem.h"
#include "pmdOptions.h"
#include "pmdOptionsMgr.hpp"

#include <iostream>
#include <stdio.h>
#include <string>
#include <map>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

using namespace std ;

namespace po = boost::program_options ;
namespace fs = boost::filesystem ;

namespace engine
{
   struct _memHeader
   {
      UINT32               _eye1 ;
      UINT32               _freed ;
      UINT64               _size ;
      UINT32               _debug ;
      UINT32               _file ;
      UINT32               _line ;
      UINT32               _eye2 ;

      _memHeader ()
      {
         SDB_ASSERT( 32 == sizeof(_memHeader), "Size must be 32Byte" ) ;
      }
   } ;
   typedef _memHeader memHeader ;

   #define TEST_MEM_HEADSZ                sizeof(memHeader)
   #define TEST_BUFFSIZE                  (1024*1024*1024)

   string g_corefile ;
   string g_outputfile ;

   OSSFILE g_pCoreFile ;
   BOOLEAN g_openCoreFile  = FALSE ;
   UINT64  g_readPos       = 0 ;
   CHAR*   g_pBuff         = NULL ;
   INT64   g_dataLen       = 0 ;
   OSSFILE g_pOutFile ;
   BOOLEAN g_openOutFile   = FALSE ;
   CHAR    g_textBuff[ 1024 ] = {0} ;

   map< UINT64, UINT32 > g_memMap ;
   UINT64  g_totalMemSize  = 0 ;
   UINT64  g_totalMemNum   = 0 ;
   UINT32  g_errMemNum     = 0 ;

   #define TEST_CORE_FILE     "corefile"
   #define TEST_OUTPUT        "output"

   #define TEST_MEM_OPTIONS  \
      ( PMD_COMMANDS_STRING (PMD_OPTION_HELP, ",h"), "help" ) \
      ( PMD_COMMANDS_STRING (TEST_CORE_FILE, ",f"), boost::program_options::value<string>(), "core file" ) \
      ( PMD_COMMANDS_STRING (TEST_OUTPUT, ",o"), boost::program_options::value<string>(), "output file" ) \

   INT32 resolveArguments( INT32 argc, CHAR** argv )
   {
      INT32 rc = SDB_OK ;

      po::variables_map vm ;
      po::options_description desc( "Command options" ) ;

      PMD_ADD_PARAM_OPTIONS_BEGIN( desc )
         TEST_MEM_OPTIONS
      PMD_ADD_PARAM_OPTIONS_END

      try
      {
         po::store( po::parse_command_line ( argc, argv, desc ), vm ) ;
         po::notify ( vm ) ;
      }
      catch ( po::unknown_option &e )
      {
         std::cerr <<  "Unknown argument: "
                   << e.get_option_name () << std::endl ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      catch ( po::invalid_option_value &e )
      {
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

      if ( vm.count( PMD_OPTION_HELP ) )
      {
         std::cout << desc << std::endl ;
         rc = SDB_PMD_HELP_ONLY ;
         goto done ;
      }
      if ( vm.count( TEST_CORE_FILE ) )
      {
         g_corefile = vm[TEST_CORE_FILE].as<string>() ;
      }
      if ( vm.count( TEST_OUTPUT ) )
      {
         g_outputfile = vm[TEST_OUTPUT].as<string>() ;
      }

      if ( g_corefile.empty() )
      {
         std::cerr << "Core file is not configed" << std::endl ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 init()
   {
      INT32 rc = SDB_OK ;

      rc = ossOpen( g_corefile.c_str(), OSS_READONLY, OSS_RU, g_pCoreFile ) ;
      if ( rc )
      {
         std::cerr << "Open file[" << g_corefile << "] failed, rc: " << rc
                   << std::endl ;
         goto error ;
      }
      g_openCoreFile = TRUE ;

      g_pBuff = new ( std::nothrow ) CHAR[TEST_BUFFSIZE] ;
      if( !g_pBuff )
      {
         std::cerr << "Alloc memory failed" << std::endl ;
         rc = SDB_OOM ;
         goto error ;
      }

      if ( !g_outputfile.empty() )
      {
         rc = ossOpen( g_outputfile.c_str(), OSS_CREATE|OSS_READWRITE,
                       OSS_RWXU|OSS_RWXG, g_pOutFile ) ;
         if ( rc )
         {
            std::cerr << "Open file[" << g_outputfile << "] failed, rc: " << rc
                      << std::endl ;
            goto error ;
         }
         g_openOutFile = TRUE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 fini()
   {
      if ( g_openCoreFile )
      {
         ossClose( g_pCoreFile ) ;
         g_openCoreFile = FALSE ;
      }
      if ( g_openOutFile )
      {
         ossClose( g_pOutFile ) ;
         g_openOutFile = FALSE ;
      }
      if ( g_pBuff )
      {
         delete [] g_pBuff ;
         g_pBuff = NULL ;
      }
 
      return SDB_OK ;
   }

   INT32 getNextBlock()
   {
      INT32 rc = SDB_OK ;
      INT64 readLen = 0 ;
      INT64 needRead = TEST_BUFFSIZE ;

      if ( !g_openCoreFile )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      rc = ossSeek( &g_pCoreFile, (INT64)g_readPos, OSS_SEEK_SET ) ;
      if ( rc )
      {
         std::cerr << "seek to " << g_readPos << "failed" << std::endl ;
         goto error ;
      }
      g_dataLen = 0 ;

      while ( TRUE )
      {
         readLen = 0 ;
         rc = ossRead( &g_pCoreFile, g_pBuff+g_dataLen, needRead-g_dataLen,
                       &readLen ) ;
         g_dataLen += readLen ;
         if ( SDB_INTERRUPT == rc )
         {
            continue ;
         }
         else if ( SDB_OK == rc )
         {
            break ;
         }
         else if ( SDB_EOF == rc )
         {
            break ;
         }
         else
         {
            std::cerr << "read failed, rc: " << rc << std::endl ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void addMem( UINT64 key )
   {
      map< UINT64, UINT32 >::iterator it = g_memMap.find( key ) ;
      if ( it == g_memMap.end() )
      {
         g_memMap[ key ] = 1 ;
      }
      else
      {
         (it->second)++ ;
      }
   }

   void printMemInfo( CHAR *pointer, BOOLEAN isError )
   {
      memHeader *pHeader = (memHeader*)pointer ;
      if ( FALSE == isError )
      {
         ossSnprintf( g_textBuff, sizeof(g_textBuff)-1,
                      "%p    %10ld    %10u    %6u\n",
                      pointer, pHeader->_size, pHeader->_file,
                      pHeader->_line ) ;
      }
      else
      {
         ossSnprintf( g_textBuff, sizeof(g_textBuff)-1,
                      "%p    %10ld    %10u    %6u    ****(has error)\n",
                      pointer, pHeader->_size, pHeader->_file,
                      pHeader->_line ) ;
      }

      if ( g_openOutFile )
      {
         INT32 rc = SDB_OK ;
         INT64 writeLen = 0 ;
         INT64 toWrite = ossStrlen( g_textBuff ) ;
         const CHAR *writePtr = g_textBuff ;

         while ( TRUE )
         {
            writeLen = 0 ;
            rc = ossWrite( &g_pOutFile, writePtr, toWrite, &writeLen ) ;
            toWrite -= writeLen ;
            writePtr += writeLen ;
            if ( SDB_INTERRUPT == rc )
            {
               continue ;
            }
            break ;
         }
      }
      else
      {
         ossPrintf ( "%s", g_textBuff ) ;
      }
   }

   INT32 doMemCheck()
   {
      INT64 pos = 0 ;
      BOOLEAN hasError = FALSE ;

      while ( pos < g_dataLen &&
              pos < (INT64)(TEST_BUFFSIZE - TEST_MEM_HEADSZ) )
      {
         hasError = FALSE ;
         memHeader *pHeader = ( memHeader* )( g_pBuff + pos ) ;
         if ( pHeader->_eye1 == SDB_MEMHEAD_EYECATCHER1 &&
              pHeader->_eye2 == SDB_MEMHEAD_EYECATCHER2 &&
              pHeader->_freed == 0 )
         {
            ++g_totalMemNum ;
            UINT64 key = ossPack32To64( pHeader->_file, pHeader->_line ) ;
            addMem( key ) ;
            if ( pHeader->_size > 1024*1024*1024 )
            {
               hasError = TRUE ;
               pos += 1 ;
               ++g_errMemNum ;
            }
            else
            {
               pos += ( pHeader->_size + TEST_MEM_HEADSZ ) ;
               g_totalMemSize += pHeader->_size ;
            }

            printMemInfo( (CHAR*)pHeader, hasError ) ;
         }
         else
         {
            pos += 1 ;
         }
      }

      g_readPos += pos ;

      return SDB_OK ;
   }

   void printResult()
   {
      UINT32 file = 0 ;
      UINT32 line = 0 ;
      INT64 writeLen = 0 ;

      ossSnprintf( g_textBuff, sizeof(g_textBuff)-1, "\n\nStat:\n" ) ;
      if ( g_openOutFile )
      {
         ossWrite( &g_pOutFile, g_textBuff, ossStrlen(g_textBuff), &writeLen ) ;
      }
      else
      {
         ossPrintf ( "%s", g_textBuff ) ;
      }

      map< UINT64, UINT32 >::iterator it = g_memMap.begin() ;
      while ( it != g_memMap.end() )
      {
         ossUnpack32From64( it->first, file, line ) ;

         ossSnprintf( g_textBuff, sizeof(g_textBuff)-1, "%u : %u ---- %u\n",
                      file, line, it->second ) ;
         
         if ( g_openOutFile )
         {
            ossWrite( &g_pOutFile, g_textBuff, ossStrlen(g_textBuff), &writeLen ) ;
         }
         else
         {
            ossPrintf ( "%s", g_textBuff ) ;
         }

         ++it ;
      }

      ossSnprintf( g_textBuff, sizeof(g_textBuff)-1, "\n\nTotalNum: %llu\n"
                   "TotalSize: %llu\nErrorNum: %u\nStatNum: %u\n",
                   g_totalMemNum, g_totalMemSize, g_errMemNum,
                   g_memMap.size() ) ;
      if ( g_openOutFile )
      {
         ossWrite( &g_pOutFile, g_textBuff, ossStrlen(g_textBuff), &writeLen ) ;
      }
      else
      {
         ossPrintf ( "%s", g_textBuff ) ;
      }
   }

   INT32 mastMain( int argc, char **argv )
   {
      INT32 rc = SDB_OK ;

      rc = resolveArguments( argc, argv ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = init() ;
      if ( rc )
      {
         goto error ;
      }

      while ( SDB_OK == getNextBlock() )
      {
         rc = doMemCheck() ;
         if ( rc )
         {
            goto error ;
         }
      }

      printResult() ;

   done:
      fini() ;
      return rc ;
   error:
      goto done ;
   }

}

int main ( int argc, char **argv )
{
   return engine::mastMain( argc, argv ) ;
}

