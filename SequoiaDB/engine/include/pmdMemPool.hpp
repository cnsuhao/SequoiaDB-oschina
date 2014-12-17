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

   Descriptive Name = Data Management Service Header

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          27/11/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef PMD_MEM_POOL_HPP_
#define PMD_MEM_POOL_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossAtomic.hpp"

namespace engine
{

   /*
      _pmdMemPool define
   */
   class _pmdMemPool : public SDBObject
   {
      public:
         _pmdMemPool() ;
         ~_pmdMemPool() ;

         INT32 initialize () ;
         INT32 final () ;
         void  clear() ;

         UINT64 totalSize() { return _totalMemSize.peek() ; }

      public:
         CHAR* alloc( UINT32 size, UINT32 &assignSize ) ;
         void  release ( CHAR* pBuff, UINT32 size ) ;
         CHAR* realloc ( CHAR* pBuff, UINT32 srcSize,
                         UINT32 needSize, UINT32 &assignSize ) ;

      private:
         ossAtomic64    _totalMemSize ;
   } ;
   typedef _pmdMemPool pmdMemPool ;

}

#endif //PMD_MEM_POOL_HPP_

