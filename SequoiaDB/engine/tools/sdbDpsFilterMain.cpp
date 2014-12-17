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

   Source File Name = sdbDpsFilterMain.cpp

   Descriptive Name = N/A

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains code logic for
   data insert/update/delete. This file does NOT include index logic.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/19/2013  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#include "sdbDpsLogFilter.hpp"
#include "sdbDpsFilter.hpp"
#include "sdbDpsOption.hpp"
#include <iostream>

INT32 main(INT32 argc, CHAR** argv)
{
   INT32 rc                = SDB_OK ;

   dpsLogFilter *logFilter = NULL ;
   iFilter *filter         = NULL ;
   dpsFilterOption op ;

   po::options_description desc( "Command options" ) ;
   po::variables_map vm ;
   rc = op.init( argc, argv, desc, vm ) ;
   if( rc )
   {
      op.displayArgs( desc ) ;
      goto error ;
   }

   rc = op.handle( desc, vm, filter ) ;
   if( rc )
   {
      goto error ;
   }
   if( SDB_DPS_DUMP_HELP == rc || SDB_DPS_DUMP_VER == rc )
   {
      goto done ;
   }

   logFilter = SDB_OSS_NEW dpsLogFilter( op.getCmdData() ) ;
   if( NULL == logFilter )
   {
      printf( "Failed to allocate dpsLogFilter" ) ;
      rc = SDB_OOM ;
      goto error ;
   }

   logFilter->setFilter( filter ) ;
   rc = logFilter->doParse() ;
   if( rc )
   {
      goto error ;
   }

done:
   if ( logFilter )
   {
      SDB_OSS_DEL( logFilter ) ;
      logFilter = NULL ;
   }
   return rc  ;

error :
   goto done  ;
}
