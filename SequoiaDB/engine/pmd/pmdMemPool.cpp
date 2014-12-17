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

   Source File Name = pmdMemPool.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          27/11/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdMemPool.hpp"
#include "ossMem.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"

namespace engine
{
   /*
      Global define
   */
   const UINT32 PMD_MEM_ALIGMENT_SIZE  = 1024 ;

   /*
      _pmdMemPool implement
   */
   _pmdMemPool::_pmdMemPool()
   :_totalMemSize( 0 )
   {
   }

   _pmdMemPool::~_pmdMemPool()
   {
   }

   INT32 _pmdMemPool::initialize()
   {
      return SDB_OK ;
   }

   INT32 _pmdMemPool::final()
   {
      if ( 0 != totalSize() )
      {
         PD_LOG( PDERROR, "MemPool has memory leak: %llu", totalSize() ) ;
      }
      return SDB_OK ;
   }

   void _pmdMemPool::clear ()
   {
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDMEMPOL_ALLOC, "_pmdMemPool::alloc" )
   CHAR *_pmdMemPool::alloc( UINT32 size, UINT32 &assignSize )
   {
      PD_TRACE_ENTRY ( SDB__PMDMEMPOL_ALLOC );
      size = ossRoundUpToMultipleX( size, PMD_MEM_ALIGMENT_SIZE ) ;
      CHAR *pBuffer = (CHAR *)SDB_OSS_MALLOC ( size ) ;
      if ( pBuffer )
      {
         assignSize = size ;
         _totalMemSize.add( assignSize ) ;
      }
      else
      {
         assignSize = 0 ;
         PD_LOG ( PDERROR, "Failed to allocate memory" ) ;
      }

      PD_TRACE_EXIT ( SDB__PMDMEMPOL_ALLOC );
      return pBuffer ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDMEMPOL_RELEASE, "_pmdMemPool::release" )
   void _pmdMemPool::release( CHAR* pBuff, UINT32 size )
   {
      PD_TRACE_ENTRY ( SDB__PMDMEMPOL_RELEASE );
      if ( pBuff && size > 0 )
      {
         SDB_OSS_FREE ( pBuff ) ;
         _totalMemSize.sub( size ) ;
      }
      PD_TRACE_EXIT ( SDB__PMDMEMPOL_RELEASE );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDMEMPOL_REALLOC, "_pmdMemPool::realloc" )
   CHAR *_pmdMemPool::realloc ( CHAR* pBuff, UINT32 srcSize,
                                UINT32 needSize, UINT32 &assignSize )
   {
      PD_TRACE_ENTRY ( SDB__PMDMEMPOL_REALLOC );
      CHAR *p = NULL ;
      if ( srcSize >= needSize )
      {
         assignSize = srcSize ;
         p = pBuff ;
         goto done ;
      }

      release ( pBuff, srcSize );

      p = alloc ( needSize, assignSize ) ;
   done :
      PD_TRACE_EXIT ( SDB__PMDMEMPOL_REALLOC );
      return p ;
   }
}

