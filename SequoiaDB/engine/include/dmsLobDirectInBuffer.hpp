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

   Source File Name = dmsLobDirectInBuffer.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DMS_LOBDIRECTINBUFFER_HPP_
#define DMS_LOBDIRECTINBUFFER_HPP_

#include "dmsLobDirectBuffer.hpp"

namespace engine
{
   class _dmsLobDirectInBuffer : public _dmsLobDirectBuffer
   {
   public:
      _dmsLobDirectInBuffer( void *usrBuf,
                             UINT32 size,
                             UINT32 offset,
                             _pmdEDUCB *cb ) ;
      virtual ~_dmsLobDirectInBuffer() ;

   public:
      virtual INT32 getAlignedTuple( tuple &t ) ;

      void copy2UsrBuf( const tuple &t ) ;

   private:
      void *_usrBuf ;
      UINT32 _usrSize ;
      UINT32 _usrOffset ;
   } ;
   typedef class _dmsLobDirectInBuffer dmsLobDirectInBuffer ;
}

#endif

