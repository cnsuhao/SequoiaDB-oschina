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

   Source File Name = aggrGroup.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/27/2015  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#include "fapModuleWrapper.hpp"
#include "pd.hpp"

namespace engine {

_fapModuleWrapper::_fapModuleWrapper() : _loadModule( NULL )
{

}

_fapModuleWrapper::~_fapModuleWrapper()
{
   unload() ;
}

INT32 _fapModuleWrapper::load( const CHAR *module, const CHAR *path, UINT32 mode )
{
   INT32 rc = SDB_OK ;
   if ( NULL != _loadModule )
   {
      unload() ;
   }

   _loadModule = SDB_OSS_NEW ossModuleHandle( module, path, 0 ) ;
   if ( SDB_OK != rc )
   {
      PD_LOG( PDERROR, "Failed to alloc module" ) ;
      rc = SDB_OOM ;
      goto error ;
   }

   rc = _loadModule->init() ;
   if ( SDB_OK != rc )
   {
      PD_LOG( PDERROR, "Init module failed" ) ;
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

void _fapModuleWrapper::unload()
{
   fini() ;

   if ( NULL != _loadModule )
   {
      _loadModule->unload() ;
      SDB_OSS_DEL _loadModule ;
      _loadModule = NULL ;
   }
}

INT32 _fapModuleWrapper::getFunction( const CHAR *funcName, OSS_MODULE_PFUNCTION *func )
{
   SDB_ASSERT( NULL != funcName, "Function name cann't be NULL" ) ;

   INT32 rc = SDB_OK ;
   SDB_ASSERT( NULL != _loadModule, "module was not loaded" ) ;

   rc = _loadModule->resolveAddress( funcName, func ) ;
   if ( SDB_OK != rc )
   {
      PD_LOG( PDERROR, "Failed to get function address" ) ;
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 _fapModuleWrapper::create( IPmdAccessProtocol *&protocol )
{
   INT32 rc = SDB_OK ;
   SDB_ASSERT( NULL != _loadModule, "Module handle cann't be NULL" ) ;

   rc = _loadModule->resolveAddress( "createAccessProtocol", &_function ) ;
   if ( SDB_OK != rc )
   {
      PD_LOG( PDERROR, "Failed to get export function: " ) ;
      goto error ;
   }

   protocol = (OSS_FAP_CREATE(_function))() ;
   if ( NULL == protocol )
   {
      PD_LOG( PDERROR, "Failed to create protocol" ) ;
      rc = SDB_OOM ;
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 _fapModuleWrapper::release( IPmdAccessProtocol *protocol )
{
   INT32 rc = SDB_OK ;
   SDB_ASSERT( NULL != _loadModule, "Module handle cann't be NULL" ) ;

   rc = _loadModule->resolveAddress( "releaseAccessProtocol", &_function ) ;
   if ( SDB_OK != rc )
   {
      PD_LOG( PDERROR, "Failed to get export function: " ) ;
      goto error ;
   }

   (OSS_FAP_RELEASE(_function))( protocol ) ;

done:
   return rc ;
error:
   goto done ;
}

}
