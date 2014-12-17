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

   Source File Name = dmsLobDirectBuffer.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DMS_LOBDIRECTBUFFER_HPP_
#define DMS_LOBDIRECTBUFFER_HPP_

#include "oss.hpp"
#include "core.hpp"

namespace engine
{
   class _pmdEDUCB ;

   class _dmsLobDirectBuffer : public SDBObject
   {
   public:
      _dmsLobDirectBuffer( _pmdEDUCB *cb ) ;
      virtual ~_dmsLobDirectBuffer() ;
   public:
       struct tuple
       {
         void *buf ;
         UINT32 size ;
         UINT32 offset ;
         tuple()
         :buf( NULL ),
          size( 0 ),
          offset( 0 )
         {

         }
       } ;
   public:
      virtual INT32 getAlignedTuple( tuple &t ) = 0 ;

   protected:
      INT32 _extendBuf( UINT32 size ) ;

   protected:
      _pmdEDUCB *_cb ;
      void *_buf ;
      UINT32 _bufSize ;
   } ;
   typedef class _dmsLobDirectBuffer dmsLobDirectBuffer ;
}

#endif

