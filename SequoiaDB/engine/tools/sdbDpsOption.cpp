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

   Source File Name = sdbDpsOption.cpp

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
#include "sdbDpsOption.hpp"
#include "ossUtil.hpp"
#include "utilParam.hpp"
#include "ossVer.h"

#define ASSIGNED_FILTER( filter, nextFilter ) \
        if( NULL == filter )                  \
           filter = nextFilter ;              \
        else                                  \
           filter->setNext( nextFilter ) ;

#define CHECK_FILTER( filter )                      \
        if ( NULL == filter )                       \
        {                                           \
           printf( "Failed to allocate filter\n" ); \
           goto error;                              \
        }

using namespace engine;

_dpsFilterOption::_dpsFilterOption()
{
   ossMemset( _cmdData.inputName, 0, OSS_MAX_PATHSIZE + 1 ) ;
   ossMemset( _cmdData.srcPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
   ossMemset( _cmdData.dstPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
}

_dpsFilterOption::~_dpsFilterOption()
{
}

void _dpsFilterOption::displayArgs( const po::options_description &desc )
{
   std::cout << desc << std::endl ;
}

BOOLEAN _dpsFilterOption::checkInput( const po::variables_map &vm )
{
   BOOLEAN valid = FALSE ;

   if( vm.count( DPS_LOG_FILTER_HELP )
    || vm.count( DPS_LOG_FILTER_VER )
    || vm.count( DPS_LOG_FILTER_TYPE )
    || vm.count( DPS_LOG_FILTER_NAME )
    || vm.count( DPS_LOG_FILTER_META )
    || vm.count( DPS_LOG_FILTER_LSN )
    || vm.count( DPS_LOG_FILTER_SOURCE )
    || vm.count( DPS_LOG_FILTER_OUTPUT )
    || vm.count( DPS_LOG_FILTER_LAST ) )
   {
      valid = TRUE ;
   }

   return valid ;
}

INT32 _dpsFilterOption::handle( const po::options_description &desc,
                                const po::variables_map &vm,
                                iFilter *&filter )
{
   INT32 rc            = SDB_OK ;
   iFilter *nextFilter = NULL ;

   if( vm.count( DPS_LOG_FILTER_HELP ) )
   {
      displayArgs( desc ) ;
      rc = SDB_DPS_DUMP_HELP ;
      goto done ;
   }

   if( vm.count( DPS_LOG_FILTER_VER ) )
   {
      ossPrintVersion( "SequoiaDB version" ) ;
      rc = SDB_DPS_DUMP_VER ;
      goto done ;
   }

   if( vm.count( DPS_LOG_FILTER_OUTPUT ) )
   {
      _cmdData.output = FALSE ;
   }
   else
   {
      _cmdData.output = TRUE ;
   }

   if( vm.count( DPS_LOG_FILTER_LSN ) && vm.count( DPS_LOG_FILTER_LAST ) )
   {
      printf( "--lsn cannot be used with --last!!\n" ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if( vm.count( DPS_LOG_FILTER_LSN ) )
   {
      filter = dpsFilterFactory::getInstance()
               ->createFilter( SDB_LOG_FILTER_LSN ) ;
      CHECK_FILTER( filter ) ;
      const CHAR *pLsn = vm[ DPS_LOG_FILTER_LSN ].as<std::string>().c_str() ;
      try
      {
         _cmdData.lsn = boost::lexical_cast< UINT64 >( pLsn ) ;
      }
      catch( boost::bad_lexical_cast& e )
      {
         printf( "Unable to cast lsn to UINT64\n" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   }
   else if( vm.count( DPS_LOG_FILTER_LAST ) )
   {
      filter = dpsFilterFactory::getInstance()
               ->createFilter( SDB_LOG_FILTER_LAST ) ;
      CHECK_FILTER( filter ) ;
   }

   if( vm.count( DPS_LOG_FILTER_TYPE ) )
   {
      nextFilter = dpsFilterFactory::getInstance()
               ->createFilter( SDB_LOG_FILTER_TYPE ) ;
      CHECK_FILTER( nextFilter ) ;
      ASSIGNED_FILTER( filter, nextFilter ) ;
   }

   if( vm.count( DPS_LOG_FILTER_NAME ) )
   {
      nextFilter = dpsFilterFactory::getInstance()
               ->createFilter( SDB_LOG_FILTER_NAME ) ;
      CHECK_FILTER( nextFilter ) ;
      ASSIGNED_FILTER( filter, nextFilter ) ;
   }

   if( vm.count( DPS_LOG_FILTER_META ) )
   {
      if( NULL != filter )
      {
         printf( "meta command must be used alone!\n" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      filter = dpsFilterFactory::getInstance()
               ->createFilter( SDB_LOG_FILTER_META ) ;
      CHECK_FILTER( filter ) ;
      goto done ;
   }

   if( NULL == filter )
   {
      filter = dpsFilterFactory::getInstance()
               ->createFilter( SDB_LOG_FILTER_NONE ) ;
      CHECK_FILTER( filter ) ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 _dpsFilterOption::init( INT32 argc, CHAR **argv,
                              po::options_description &desc,
                              po::variables_map &vm )
{
   INT32 rc            = SDB_OK ;

   DPS_FILTER_ADD_OPTIONS_BEGIN( desc )
      FILTER_OPTIONS
   DPS_FILTER_ADD_OPTIONS_END

   rc = utilReadCommandLine( argc, argv, desc, vm ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if( !checkInput( vm ) )
   {
      printf( "invalid arguments\n" ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = pmdCfgRecord::init( NULL, &vm ) ;
   if( rc )
   {
      printf( "invalid arguments\n" ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 _dpsFilterOption::postLoaded()
{
   return SDB_OK ;
}

INT32 _dpsFilterOption::preSaving()
{
   return SDB_OK ;
}

INT32 _dpsFilterOption::doDataExchange( engine::pmdCfgExchange *pEx )
{
   resetResult() ;

   rdxPath( pEx, DPS_LOG_FILTER_SOURCE,
            _cmdData.srcPath, OSS_MAX_PATHSIZE, FALSE, FALSE, "./" ) ;

   rdxPath( pEx, DPS_LOG_FILTER_OUTPUT,
            _cmdData.dstPath, OSS_MAX_PATHSIZE, FALSE, FALSE, "./" ) ;

   rdxString( pEx, DPS_LOG_FILTER_NAME,
            _cmdData.inputName, OSS_MAX_PATHSIZE, FALSE, FALSE, "" ) ;

   rdxUShort( pEx, DPS_LOG_FILTER_TYPE,
              _cmdData.type, FALSE, TRUE, (UINT16)PDWARNING ) ;

   rdxInt( pEx, DPS_LOG_LSN_AHEAD,
            _cmdData.lsnAhead, FALSE, TRUE, 20 ) ;

   rdxInt( pEx, DPS_LOG_LSN_BACK,
            _cmdData.lsnBack, FALSE, TRUE, 20 ) ;

   rdxInt( pEx, DPS_LOG_FILTER_LAST,
            _cmdData.lastCount, FALSE, TRUE, 0 ) ;

   return getResult() ;
}
