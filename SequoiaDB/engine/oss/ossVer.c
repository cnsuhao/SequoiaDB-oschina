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

   Source File Name = ossVer.cpp

   Descriptive Name = Operating System Services Version

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains engine version information

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/16/2013  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "ossVer.h"
#include <iostream>

void ossGetVersion ( INT32 *version,
                     INT32 *subVersion,
                     INT32 *release,
                     const CHAR **ppBuild )
{
   if ( version )
      *version = SDB_ENGINE_VERISON_CURRENT ;
   if ( subVersion )
      *subVersion = SDB_ENGINE_SUBVERSION_CURRENT ;
   if ( release )
      *release = SDB_ENGINE_RELEASE_CURRENT ;
   if ( ppBuild )
      *ppBuild = SDB_ENGINE_BUILD_TIME ;
}

void ossPrintVersion( const CHAR *prompt )
{
   std::cout << prompt << ": " << SDB_ENGINE_VERISON_CURRENT << "."
             << SDB_ENGINE_SUBVERSION_CURRENT << std::endl ;
   std::cout << "Release: " << SDB_ENGINE_RELEASE_CURRENT << std::endl ;
   std::cout << SDB_ENGINE_BUILD_TIME << std::endl ;
}

