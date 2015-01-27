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

   Source File Name = mthNodePool.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef MTH_NODEPOOL_HPP_
#define MTH_NODEPOOL_HPP_

#include <list>

#define MTH_NODE_POOL_DEFAULT_SZ 32

namespace engine
{
   template<typename T>
   class _mthNodePool : public SDBObject
   {
   public:
      _mthNodePool()
      :_eleSize( 0 )
      {

      }

      ~_mthNodePool()
      {
         clear() ;
      }

   public:
      INT32 allocate( T *&tp )
      {
         INT32 rc = SDB_OK ;
         if ( _eleSize < MTH_NODE_POOL_DEFAULT_SZ )
         {
            tp = &(_static[_eleSize++]) ;
         }
         else
         {
            T *node = SDB_OSS_NEW T ;
            if ( NULL == node )
            {
               rc = SDB_OOM ;
               goto error ;
            }

            _dynamic.push_back( node ) ;
            tp = node ;
            ++_eleSize ;
         }
      done:
         return rc ;
      error:
         goto done ;
      }

      void clear()
      {
         typename std::list<T*>::iterator itr = _dynamic.begin() ;
         for ( ; itr != _dynamic.end(); ++itr )
         {
            SDB_OSS_DEL *itr ;
         }
         _dynamic.clear() ;
         _eleSize = 0 ;
         return ;
      }
   private:
      T _static[MTH_NODE_POOL_DEFAULT_SZ] ;
      std::list<T*> _dynamic ;
      UINT32 _eleSize ;
   } ;
}

#endif

