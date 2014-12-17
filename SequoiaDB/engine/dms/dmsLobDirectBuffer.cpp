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

   Source File Name = dmsLobDirectBuffer.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsLobDirectBuffer.hpp"
#include "pmdEDU.hpp"
#include "pd.hpp"
#include "dmsTrace.hpp"

namespace engine
{
   _dmsLobDirectBuffer::_dmsLobDirectBuffer( _pmdEDUCB *cb )
   :_cb( cb ),
    _buf( NULL ),
    _bufSize( 0 )
   {
      SDB_ASSERT( NULL != _cb, "can not be NULL" ) ;
   }

   _dmsLobDirectBuffer::~_dmsLobDirectBuffer()
   {

   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMS_LOBDIRECTBUF__EXTENDBUF, "_dmsLobDirectBuffer::_extendBuf" )
   INT32 _dmsLobDirectBuffer::_extendBuf( UINT32 size )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMS_LOBDIRECTBUF__EXTENDBUF ) ;

      _buf = _cb->getAlignedMemory( OSS_FILE_DIRECT_IO_ALIGNMENT, size ) ;
      if ( NULL == _buf )
      {
         PD_LOG( PDERROR, "failed to allocate mem" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__DMS_LOBDIRECTBUF__EXTENDBUF, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

