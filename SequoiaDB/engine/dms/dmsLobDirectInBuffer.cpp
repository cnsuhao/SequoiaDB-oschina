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

   Source File Name = dmsLobDirectInBuffer.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsLobDirectInBuffer.hpp"
#include "pmdEDU.hpp"
#include "dmsTrace.hpp"
#include "pd.hpp"

namespace engine
{
   _dmsLobDirectInBuffer::_dmsLobDirectInBuffer( void *usrBuf,
                                                 UINT32 size,
                                                 UINT32 offset,
                                                 _pmdEDUCB *cb )
   :_dmsLobDirectBuffer( cb ),
    _usrBuf( usrBuf ),
    _usrSize( size ),
    _usrOffset( offset )
   {
      SDB_ASSERT( NULL != _usrBuf && 0 < _usrSize, "impossible" ) ;
   }

   _dmsLobDirectInBuffer::~_dmsLobDirectInBuffer()
   {

   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMS_LOBDIRECTINBUF_GETALIGNEDTUPLE, "_dmsLobDirectInBuffer::getAlignedTuple" )
   INT32 _dmsLobDirectInBuffer::getAlignedTuple( tuple &t )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMS_LOBDIRECTINBUF_GETALIGNEDTUPLE ) ;
      UINT32 newSize = _usrSize ;
      UINT32 newOffset = ossRoundUpToMultipleX( _usrOffset,
                                                OSS_FILE_DIRECT_IO_ALIGNMENT ) ;
      if ( _usrOffset != newOffset )
      {
         newOffset -= OSS_FILE_DIRECT_IO_ALIGNMENT ;
      }

      SDB_ASSERT( newOffset <= _usrOffset, "impossible" ) ;
      newSize += _usrOffset - newOffset ;
      newSize = ossRoundUpToMultipleX( newSize,
                                       OSS_FILE_DIRECT_IO_ALIGNMENT ) ;

      if ( _bufSize < newSize )
      {
         rc = _extendBuf( newSize ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to extend buf:%d", rc ) ;
            goto error ;
         }
      }

      t.buf = _buf ;
      t.size = newSize ;
      t.offset = newOffset ;
   done:
      PD_TRACE_EXITRC( SDB__DMS_LOBDIRECTINBUF_GETALIGNEDTUPLE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMS_LOBDIRECTINBUF_CP2USRBUF, "_dmsLobDirectInBuffer::copy2UsrBuf" )
   void _dmsLobDirectInBuffer::copy2UsrBuf( const tuple &t )
   {
      PD_TRACE_ENTRY( SDB__DMS_LOBDIRECTINBUF_CP2USRBUF ) ;
      SDB_ASSERT( t.offset <= _usrOffset &&
                  _usrSize <= t.size - ( _usrOffset - t.offset ) &&
                  NULL != t.buf, "impossible" ) ;
      ossMemcpy( _usrBuf,
                 ( const CHAR * )_buf + ( _usrOffset - t.offset ),
                 _usrSize ) ;

      PD_TRACE_EXIT( SDB__DMS_LOBDIRECTINBUF_CP2USRBUF ) ;
      return ;
   }
}

