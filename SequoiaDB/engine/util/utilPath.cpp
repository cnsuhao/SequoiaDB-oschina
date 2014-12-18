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

   Source File Name = utilPath.cpp

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

#include "utilPath.hpp"
#include "pd.hpp"
#include "ossUtil.h"

CHAR progName[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;

const CHAR* getProgramName()
{
   return progName ;
}

INT32 setProgramName( const CHAR* name )
{
   INT32 rc = SDB_OK ;
   if ( !name || ossStrlen( name ) > OSS_MAX_PATHSIZE + 1 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   ossStrncpy ( progName, name, ossStrlen( name ) ) ;
done :
   return rc ;
error :
   goto done ;
}

INT32 getProgramPath( CHAR *pOutputPath )
{
   INT32 rc = SDB_OK ;
   CHAR *t = OSS_FILE_SEP ;
   const CHAR *p = NULL ;
   if ( !pOutputPath )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( progName[0] == '\0' )
   {
      pOutputPath[0] = '\0' ;
      goto done ;
   }
   p = ossStrrchr ( progName, t[0] ) ;
   if ( p )
   {
      INT32 pathLen = p - progName + 1 ;
      if ( pathLen > OSS_MAX_PATHSIZE + 1 )
      {
         pOutputPath[0] = '\0' ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossMemcpy ( pOutputPath, progName, pathLen ) ;
      pOutputPath[pathLen] = '\0' ;
   }
   else
   {
       pOutputPath[0] = '\0' ;
   }
done :
   return rc ;
error :
   goto done ;

}


/*
INT32 getProgramPath( const CHAR *pInputPath, const CHAR *pOutputPath )
{
   INT32 rc = SDB_OK ;
   CHAR *t = OSS_FILE_SEP_CHAR ;
   const CHAR *p = NULL ;
   if ( pInputPath || pOutputPath )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   p = ossStrrchr ( pInputPath, t[0] ) ;
   if ( p )
   {
      INT32 pathLen = p - pInputPath + 1 ;
      if ( pathLen > OSS_MAX_PATHSIZE + 1 )
      {
         pOutputPath[0] = 0 ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossMemcpy ( pOutputPath, pInputPath, pathLen ) ;
      pOutputPath[pathLen] = 0 ;
   }
   else
   {
       pOutputPath[0] = 0 ;
   }
done :
   return rc ;
error :
   goto done ;

}
*/

