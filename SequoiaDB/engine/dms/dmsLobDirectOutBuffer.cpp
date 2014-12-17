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

   Source File Name = dmsLobDirectOutBuffer.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsLobDirectOutBuffer.hpp"
#include "pmdEDU.hpp"
#include "pd.hpp"
#include "dmsTrace.hpp"

namespace engine
{
   _dmsLobDirectOutBuffer::_dmsLobDirectOutBuffer( const void *buf,
                                                   UINT32 size,
                                                   _pmdEDUCB *cb )
   :_dmsLobDirectBuffer( cb ),
    _usrBuf( buf ),
    _size( size )
   {
      SDB_ASSERT( NULL != _usrBuf && 0 < size, "impossible" ) ;
   }

   _dmsLobDirectOutBuffer::~_dmsLobDirectOutBuffer()
   {

   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMS_LOBDIRECTOUTBUF_GETALIGNEDTUPLE, "_dmsLobDirectOutBuffer::getAlignedTuple" )
   INT32 _dmsLobDirectOutBuffer::getAlignedTuple( tuple &t )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMS_LOBDIRECTOUTBUF_GETALIGNEDTUPLE ) ;
      UINT32 aligned = ossRoundUpToMultipleX( _size,
                                              OSS_FILE_DIRECT_IO_ALIGNMENT ) ;
      if ( _bufSize < aligned )
      {
         rc = _extendBuf( aligned ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to extend buf:%d", rc ) ;
            goto error ;
         }
      }

      ossMemcpy( _buf, _usrBuf, _size ) ;
      if ( _size < aligned )
      {
         ossMemset( ( CHAR * )_buf + _size, '\0',
                    aligned - _size ) ;
      }

      t.buf = _buf ;
      t.size = aligned ;
   done:
      PD_TRACE_EXITRC( SDB__DMS_LOBDIRECTOUTBUF_GETALIGNEDTUPLE, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

