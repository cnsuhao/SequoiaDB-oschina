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

   Source File Name = sptContainer.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptContainer.hpp"
#include "sptSPScope.hpp"
#include "spt.hpp"
#include "../spt/js_in_cpp.hpp"
#include "pd.hpp"
#include "sptUsrSsh.hpp"
#include "sptUsrCmd.hpp"
#include "sptUsrFile.hpp"
#include "sptUsrSystem.hpp"
#include "sptUsrOma.hpp"
#include "sptUsrHash.hpp"
#include "sptUsrSdbTool.hpp"

namespace engine
{
   #define SPT_SCOPE_CACHE_SIZE                 0 //(30)

   _sptContainer::_sptContainer( INT32 loadMask )
   {
      _loadMask = loadMask ;
   }

   _sptContainer::~_sptContainer()
   {
      fini() ;
   }

   INT32 _sptContainer::init()
   {
      return SDB_OK ;
   }

   INT32 _sptContainer::fini()
   {
      ossScopedLock lock( &_latch ) ;

      for ( UINT32 i = 0 ; i < _vecScopes.size() ; ++i )
      {
         _vecScopes[ i ]->shutdown() ;
         SDB_OSS_DEL _vecScopes[ i ] ;
      }
      _vecScopes.clear() ;
      return SDB_OK ;
   }

   _sptScope* _sptContainer::_getFromCache( SPT_SCOPE_TYPE type )
   {
      _sptScope *pScope = NULL ;
      VEC_SCOPE_IT it ;
      ossScopedLock lock( &_latch ) ;

      it = _vecScopes.begin() ;
      while ( it != _vecScopes.end() )
      {
         pScope = *it ;
         if ( type == pScope->getType() )
         {
            _vecScopes.erase( it ) ;
            break ;
         }
         ++it ;
      }

      return pScope ;
   }

   _sptScope* _sptContainer::_createScope( SPT_SCOPE_TYPE type )
   {
      INT32 rc = SDB_OK ;
      _sptScope *scope = NULL ;

      if ( SPT_SCOPE_TYPE_SP == type )
      {
         scope = SDB_OSS_NEW _sptSPScope() ;
      }

      if ( NULL == scope )
      {
         PD_LOG( PDERROR, "it is a unknown type of scope:%d", type ) ;
         goto error ;
      }

      rc = scope->start() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Failed to init scope, rc: %d", rc ) ;
         goto error ;
      }

      rc = _loadObj( scope ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to load objs, rc: %d", rc ) ;
         goto error ;
      }

   done:
      return scope ;
   error:
      SAFE_OSS_DELETE( scope ) ;
      goto done ;
   }

   _sptScope *_sptContainer::newScope( SPT_SCOPE_TYPE type )
   {
      _sptScope *scope = _getFromCache( type ) ;
      if ( scope )
      {
         goto done ;
      }

      scope = _createScope( type ) ;
      if ( !scope )
      {
         PD_LOG( PDERROR, "Failed to create scope[%d]", type ) ;
         goto error ;
      }

   done:
      return scope ;
   error:
      goto done ;
   }

   void _sptContainer::releaseScope( _sptScope * pScope )
   {
      if ( !pScope )
      {
         goto done ;
      }

      _latch.get() ;
      if ( _vecScopes.size() < SPT_SCOPE_CACHE_SIZE )
      {
         _vecScopes.push_back( pScope ) ;
         pScope = NULL ;
      }
      _latch.release() ;

      if ( pScope )
      {
         pScope->shutdown() ;
         SDB_OSS_DEL pScope ;
         pScope = NULL ;
      }

   done:
      return ;
   }

   INT32 _sptContainer::_loadObj( _sptScope * pScope )
   {
      INT32 rc = SDB_OK ;

      if ( _loadMask & SPT_OBJ_MASK_STANDARD )
      {
         if ( !InitDbClasses( ((sptSPScope *)pScope)->getContext(),
                              ((sptSPScope *)pScope)->getGlobalObj() ) )
         {
            PD_LOG( PDERROR, "Failed to init dbclass" ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

      if ( _loadMask & SPT_OBJ_MASK_USR )
      {
         rc = pScope->loadUsrDefObj<_sptUsrSsh>() ;
         PD_CHECK ( SDB_OK == rc, SDB_SYS, error, PDERROR,
                    "Failed to load class _sptUsrSsh, rc = %d", rc ) ;
         rc = pScope->loadUsrDefObj<_sptUsrCmd>() ;
         PD_CHECK ( SDB_OK == rc, SDB_SYS, error, PDERROR,
                    "Failed to load class _sptUsrCmd, rc = %d", rc ) ;
         rc = pScope->loadUsrDefObj<_sptUsrFile>() ;
         PD_CHECK ( SDB_OK == rc, SDB_SYS, error, PDERROR,
                    "Failed to load class _sptUsrFile, rc = %d", rc ) ;
         rc = pScope->loadUsrDefObj<_sptUsrSystem>() ;
         PD_CHECK ( SDB_OK == rc, SDB_SYS, error, PDERROR,
                    "Failed to load class _sptUsrSystem, rc = %d", rc ) ;
         rc = pScope->loadUsrDefObj<_sptUsrOma>() ;
         PD_CHECK ( SDB_OK == rc, SDB_SYS, error, PDERROR,
                    "Failed to load class _sptUsrOma, rc = %d", rc ) ;
         rc = pScope->loadUsrDefObj<_sptUsrHash>() ;
         PD_CHECK ( SDB_OK == rc, SDB_SYS, error, PDERROR,
                    "Failed to load class _sptUsrHash, rc = %d", rc ) ;
         rc = pScope->loadUsrDefObj<_sptUsrSdbTool>() ;
         PD_CHECK ( SDB_OK == rc, SDB_SYS, error, PDERROR,
                    "Failed to load class _sptUsrSdbTool, rc = %d", rc ) ;
      }

      if ( _loadMask & SPT_OBJ_MASK_INNER_JS )
      {
         rc = evalInitScripts2( pScope ) ;
         PD_CHECK ( SDB_OK == rc, SDB_SYS, error, PDERROR,
                    "Failed to init spt scope, rc = %d", rc ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

}

