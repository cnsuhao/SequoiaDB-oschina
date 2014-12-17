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

   Source File Name = sdbDpsOption.hpp

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
#ifndef _SDB_DPS_FILTER_HPP_
#define _SDB_DPS_FILTER_HPP_

#include "pmdOptionsMgr.hpp"
#include "sdbDpsFilter.hpp"
#include <boost/program_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <iostream>

#define DPS_LOG_FILTER_HELP   "help"
#define DPS_LOG_FILTER_VER    "version"
#define DPS_LOG_FILTER_TYPE   "type"
#define DPS_LOG_FILTER_NAME   "name"
#define DPS_LOG_FILTER_META   "meta"
#define DPS_LOG_FILTER_LSN    "lsn"
#define DPS_LOG_FILTER_SOURCE "source"
#define DPS_LOG_FILTER_OUTPUT "output"
#define DPS_LOG_LSN_AHEAD     "ahead"
#define DPS_LOG_LSN_BACK      "back"
#define DPS_LOG_FILTER_LAST   "last"

#define DPS_FILTER_ADD_OPTIONS_BEGIN( desc ) desc.add_options()
#define DPS_FILTER_ADD_OPTIONS_END ;
#define DPS_FILTER_COMMANDS_STRING( a, b ) \
        ( std::string( a ) + std::string( b ) ).c_str()

#define FILTER_OPTIONS \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_HELP, ",h" ), "help" ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_VER, ",v" ), "show version" ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_META, ",m" ), "show meta info of logs" ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_TYPE, ",t" ), boost::program_options::value< INT32 >(), "specify the record type" ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_NAME, ",n" ), boost::program_options::value< std::string >(), "specify the name of collectionspace/collections" ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_LSN,  ",l" ), boost::program_options::value< std::string >(), "specify the lsn, -a/-b may help" ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_LAST, ",e" ), boost::program_options::value< INT32 >(), "specify the number of last records of file to display ")\
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_SOURCE, ",s" ), boost::program_options::value<std::string>(), "specify source log file path, or current path specified default" ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_FILTER_OUTPUT, ",o" ), boost::program_options::value< std::string >(), "specify output file path, or current path specified default " ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_LSN_AHEAD, ",a" ), boost::program_options::value< INT32 >(), "specify the number of records to display before the lsn specified by -l/--lsn" ) \
        ( DPS_FILTER_COMMANDS_STRING( DPS_LOG_LSN_BACK, ",b" ), boost::program_options::value< INT32 >(), "specify the number of records to display after the lsn specified by -l/--lsn" )

#define SDB_DPS_DUMP_HELP 1
#define SDB_DPS_DUMP_VER  2
class _dpsFilterOption : public engine::_pmdCfgRecord
{
public:
   _dpsFilterOption() ;
   ~_dpsFilterOption() ;

   const dpsCmdData *getCmdData() const
   {
      return &_cmdData ;
   }

   void displayArgs( const po::options_description &desc ) ;

public:
   virtual INT32 doDataExchange( engine::pmdCfgExchange *pEx ) ;
   virtual INT32 postLoaded() ;
   virtual INT32 preSaving() ;

public:
   INT32 init( INT32 argc, CHAR **argv, 
               po::options_description &desc,
               po::variables_map &vm ) ;

   BOOLEAN checkInput( const po::variables_map &vm ) ;

   INT32 handle( const po::options_description &desc,
                 const po::variables_map &vm,
                 iFilter *&filter ) ;

private:
   dpsCmdData _cmdData ;
} ;
typedef _dpsFilterOption dpsFilterOption ;
#endif
