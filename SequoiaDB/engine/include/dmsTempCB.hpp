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

   Source File Name = dmsTempCB.hpp

   Descriptive Name = Data Management Service Temp Control Block Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS Temporary Table Control Block.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef DMSTEMPCB_HPP__
#define DMSTEMPCB_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "ossLatch.hpp"
#include "dms.hpp"
#include <queue>
#include <map>

using namespace std ;

namespace engine
{

   class _SDB_DMSCB ;
   class _dmsStorageUnit ;
   class _dmsMBContext ;

   /*
      _dmsTempCB define
   */
   class _dmsTempCB : public SDBObject
   {
   private :
   #ifdef DMSTEMPCB_XLOCK
   #undef DMSTEMPCB_XLOCK
   #endif
   #define DMSTEMPCB_XLOCK ossScopedLock _lock(&_mutex, EXCLUSIVE);
   #ifdef DMSTEMPCB_SLOCK
   #undef DMSTEMPCB_SLOCK
   #endif
   #define DMSTEMPCB_SLOCK ossScopedLock _lock(&_mutex, SHARED) ;
      ossSpinSLatch        _mutex ;

      _dmsStorageUnit      *_su ;
      queue<UINT16>        _freeCollections ;
      map<UINT16, UINT64>  _occupiedCollections ;
      _SDB_DMSCB           *_dmsCB ;

   public :
      _dmsTempCB ( _SDB_DMSCB *dmsCB ) ;
      INT32 init() ;

      _dmsStorageUnit *getTempSU ()
      {
         return _su ;
      }

      INT32 release ( _dmsMBContext *&context ) ;

      INT32 reserve ( _dmsMBContext **ppContext, UINT64 eduID ) ;

   private:
      INT32 _initTmpPath() ;

   } ;
   typedef class _dmsTempCB dmsTempCB ;

}

#endif //DMSTEMPCB_HPP__

