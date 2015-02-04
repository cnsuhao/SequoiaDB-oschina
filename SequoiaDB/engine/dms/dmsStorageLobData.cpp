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

   Source File Name = dmsStorageLobData.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          17/07/2014  YW Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsStorageLobData.hpp"
#include "ossUtil.hpp"
#include "utilStr.hpp"
#include "pmd.hpp"
#include "dmsLobDirectInBuffer.hpp"
#include "dmsLobDirectOutBuffer.hpp"
#include "dmsTrace.hpp"
#include "pdTrace.hpp"

namespace engine
{
   #define DMS_LOBD_EYECATCHER            "SDBLOBD"
   #define DMS_LOBD_EYECATCHER_LEN        8

   const UINT32 DMS_LOBD_EXTEND_LEN = 32 * 1024 * 1024 ;

   #define DMS_LOBD_FLAG_NULL 0x00000
   #define DMS_LOBD_FLAG_DIRECT 0x00001
   #define DMS_LOBD_FLAG_SPARSE 0x00002

   /*
      _dmsStorageLobData implement
   */
   _dmsStorageLobData::_dmsStorageLobData( const CHAR *fileName )
   :_fileSz( 0 ),
    _lastSz( 0 ),
    _pageSz( 0 ),
    _logarithmic( 0 ),
    _flags( DMS_LOBD_FLAG_NULL )
   {
      _fileName.assign( fileName ) ;
      _segmentPages = 0 ;
      _segmentPagesSquare = 0 ;
      ossMemset( _fullPath, 0, sizeof( _fullPath ) ) ;
      if ( pmdGetOptionCB()->useDirectIOInLob() )
      {
         _flags |= DMS_LOBD_FLAG_DIRECT ;
      }
      if ( pmdGetOptionCB()->sparseFile() )
      {
         _flags |= DMS_LOBD_FLAG_SPARSE ;
      }

   }

   _dmsStorageLobData::~_dmsStorageLobData()
   {
      close() ;   
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA_OPEN, "_dmsStorageLobData::open" )
   INT32 _dmsStorageLobData::open( const CHAR *path,
                                   BOOLEAN createNew,
                                   BOOLEAN delWhenExist,
                                   const dmsStorageInfo &info,
                                   _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA_OPEN ) ;
      UINT32 mode = OSS_READWRITE | OSS_SHAREREAD ; 
      SDB_ASSERT( path, "path can't be NULL" ) ;
      INT64 fileSize = 0 ;

      if ( createNew )
      {
         if ( delWhenExist )
         {
            mode |= OSS_REPLACE ;
         }
         else
         {
            mode |= OSS_CREATEONLY ;
         }
      }

      if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         mode |= OSS_DIRECTIO ;
      }

      rc = utilBuildFullPath( path, _fileName.c_str(), OSS_MAX_PATHSIZE,
                              _fullPath ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "path + name is too long: %s, %s",
                  path, _fileName.c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = ossOpen( _fullPath, mode, OSS_RU|OSS_WU|OSS_RG, _file ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open file:%s, rc:%d",
                 _fullPath, rc ) ;
         goto error ;
      }

      if ( createNew )
      {
         PD_LOG( PDEVENT, "create lobd file[%s] succeed, mode: %x",
                 _fullPath, mode ) ;
      }

      rc = ossGetFileSize( &_file, &fileSize ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get size of file:%s, rc:%d",
                 _fileName.c_str(), rc ) ;
         goto error ;
      }

      if ( 0 == fileSize )
      {
         if ( !createNew )
         {
            PD_LOG ( PDERROR, "lobd file is empty: %s", _fileName.c_str() ) ;
            rc = SDB_DMS_INVALID_SU ;
            goto error ;   
         }

         rc = _initFileHeader( info, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to init file header:%s, rc:%d",
                    _fileName.c_str(), rc ) ;
            goto error ;
         }
      }

      rc = _validateFile( info, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to validate file:%s, rc:%d",
                 _fullPath, rc ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA_OPEN, rc ) ;
      return rc ;
   error:
      close() ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA__REOPEN, "_dmsStorageLobData::_reopen" )
   INT32 _dmsStorageLobData::_reopen()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA__REOPEN ) ;
      INT64 tmpSz = _fileSz ;
      INT32 mode = OSS_READWRITE | OSS_SHAREREAD ;

      if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         mode |= OSS_DIRECTIO ;
      }

      rc = close() ;
      if( rc )
      {
         PD_LOG( PDERROR, "Close file[%s] failed, rc: %d",
                 _fileName.c_str(), rc ) ;
         goto error ;
      }

      rc = ossOpen( _fullPath, mode,
                    OSS_RU|OSS_WU|OSS_RG, _file ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to reopen file:%s, rc:%d",
                 _fullPath, rc ) ;
         goto error ;
      }
      _lastSz = tmpSz ;

   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA__REOPEN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN _dmsStorageLobData::isOpened()const
   {
      return _file.isOpened() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA_CLOSE, "_dmsStorageLobData::close" )
   INT32 _dmsStorageLobData::close()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA_CLOSE ) ;
      if ( _file.isOpened() )
      {
         rc = ossClose( _file ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to close file:%s, rc:%d",
                    _fullPath, rc ) ;
            goto error ;
         }
      }

   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA_CLOSE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA_WRITE, "_dmsStorageLobData::write" )
   INT32 _dmsStorageLobData::write( DMS_LOB_PAGEID page,
                                    const CHAR *data,
                                    UINT32 len,
                                    UINT32 offset,
                                    _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA_WRITE ) ;
      SDB_ASSERT( DMS_LOB_INVALID_PAGEID != page &&
                  NULL != data &&
                  0 == offset &&
                  len <= _pageSz, "invalid operation" ) ;

      _dmsLobDirectOutBuffer buffer( data, len, cb ) ;
      _dmsLobDirectBuffer::tuple t ;

      INT64 writeOffset = getSeek( page, offset ) ;
      if ( writeOffset + len > _fileSz )
      {
         PD_LOG( PDERROR, "Offset[%lld] grater than file size[%lld] in "
                 "file[%s]", writeOffset, _fileSz, _fileName.c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( writeOffset + len > _lastSz )
      {
         rc = _reopen() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to reopen file[%s], rc: %d",
                    _fileName.c_str(), rc ) ;
            goto error ;
         }
      }

      rc = ossSeek( &_file, writeOffset, OSS_SEEK_SET ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to seek file[%lld], rc: %d",
                 writeOffset, rc ) ;
         goto error ;
      }

      if ( !OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         t.buf = ( void * )data ;
         t.size = len ;
      }
      else
      {
         rc = buffer.getAlignedTuple( t ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to align the buffer:%d", rc ) ;
            goto error ;  
         }
      }

      rc = ossWriteN( &_file, ( const CHAR * )( t.buf ), t.size ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write data, page:%d, rc:%d",
                 page, rc ) ;
         goto error ; 
      }

   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA_WRITE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA_READ, "_dmsStorageLobData::read" )
   INT32 _dmsStorageLobData::read( DMS_LOB_PAGEID page,
                                   UINT32 len,
                                   UINT32 offset,
                                   _pmdEDUCB *cb,
                                   CHAR *buf,
                                   UINT32 &readLen )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA_READ ) ;
      SDB_ASSERT( DMS_LOB_INVALID_PAGEID != page &&
                  NULL != buf &&
                  len + offset <= _pageSz, "invalid operation" ) ;
      SINT64 readFromFile = 0 ;
      dmsLobDirectInBuffer buffer( buf, len, offset, cb ) ;
      dmsLobDirectBuffer::tuple t ;
      INT64 readOffset = 0 ;
      
      if ( !OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         t.buf = buf ;
         t.size = len ;
         t.offset = offset ;
      }
      else
      {
         rc = buffer.getAlignedTuple( t ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to align the buffer:%d", rc ) ;
            goto error ;
         }
      }

      readOffset = getSeek( page, t.offset ) ;

      if ( readOffset + t.size > _fileSz )
      {
         PD_LOG( PDERROR, "Offset[%lld] grater than file size[%lld] in "
                 "file[%s]", readOffset, _fileSz, _fileName.c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( readOffset + len > _lastSz )
      {
         rc = _reopen() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to reopen file[%s], rc: %d",
                    _fileName.c_str(), rc ) ;
            goto error ;
         }
      }

      rc = ossSeekAndReadN( &_file, readOffset,
                            t.size, ( CHAR * )( t.buf ),
                            readFromFile ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read page[%d], rc: %d",
                 page, rc ) ;
         goto error ;
      }

      readLen = len ;

      if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         buffer.copy2UsrBuf( t ) ;
      }
   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA_READ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA_READRAW, "_dmsStorageLobData::readRaw" )
   INT32 _dmsStorageLobData::readRaw( UINT64 offset, UINT32 len,
                                      CHAR * buf, UINT32 &readLen )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA_READRAW ) ;
      SDB_ASSERT( NULL != buf && ( SINT64 )offset <= _fileSz, "invalid operation" ) ;
      SINT64 readFromFile = 0 ;

      if ( ( SINT64 )(offset + len) > _fileSz )
      {
         PD_LOG( PDERROR, "Offset[%lld] grater than file size[%lld] in "
                 "file[%s]", offset, _fileSz, _fileName.c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( ( SINT64 )(offset + len) > _lastSz )
      {
         rc = _reopen() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to reopen file[%s], rc: %d",
                    _fileName.c_str(), rc ) ;
            goto error ;
         }
      }

      rc = ossSeekAndReadN( &_file, offset,
                            len, buf, readFromFile ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read data[offset: %lld, len: %d], rc: %d",
                 offset, len, rc ) ;
         goto error ;
      }

      readLen = readFromFile ;
   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA_READRAW, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA_EXTEND, "_dmsStorageLobData::extend" )
   INT32 _dmsStorageLobData::extend( INT64 len )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA_EXTEND ) ;
      do
      {
         rc = _extend( len ) ;
         if ( SDB_INVALIDARG == rc &&
              OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_SPARSE ) )
         {
            PD_LOG( PDWARNING, "this filesystem may not support sparse file"
                    ", we should try again" ) ;
            OSS_BIT_CLEAR( _flags, DMS_LOBD_FLAG_SPARSE ) ;
            rc = SDB_OK ;
            continue ;
         }
         else if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to extend file:%d", rc ) ;
            goto error ;
         }
         else
         {
            break ;
         }
     
      } while (  TRUE ) ; 
   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA_EXTEND, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA__EXTEND, "_dmsStorageLobData::_extend" )
   INT32 _dmsStorageLobData::_extend( INT64 len )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA__EXTEND ) ;
      SDB_ASSERT( 0 < len, "invalid extend size" ) ;
      SDB_ASSERT( 0 == len % OSS_FILE_DIRECT_IO_ALIGNMENT, "impossible" ) ;
      OSSFILE file ;
      UINT32 mode = OSS_READWRITE | OSS_SHAREREAD  ;
      CHAR *extendBuf = NULL ;
      UINT32 bufSize = DMS_LOBD_EXTEND_LEN ;

      if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         mode |= OSS_DIRECTIO ;
      }

      rc = ossOpen( _fullPath, mode, OSS_RU|OSS_WU|OSS_RG, file ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open file when extend:%d", rc ) ;
         goto error ;
      }

#ifdef _DEBUG
      {
         SINT64 sizeBeforeExtend = 0 ;
         rc = ossGetFileSize( &file, &sizeBeforeExtend ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get size of file:%s, rc:%d",
                    _fileName.c_str(), rc ) ;
            goto error ;
         }

         if ( 0 != _fileSz &&
              ( sizeBeforeExtend - sizeof( _dmsStorageUnitHeader ) ) %
              DMS_SEGMENT_SZ != 0 )
         {
            PD_LOG( PDERROR, "invalid file size:%lld, file:%s",
                    sizeBeforeExtend, _fileName.c_str() ) ;
            rc = SDB_SYS ;
            SDB_ASSERT( FALSE, "impossible" ) ;
            goto error ;
         }
      }
#endif // _DEBUG

      if ( !OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_SPARSE ) &&
           !OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         rc = ossExtendFile( &file, len ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to extend file:%d", rc ) ;
            goto error ;
         }
      }
      else
      {
         SINT64 extendSize = len ;

         rc = ossSeek( &file, 0, OSS_SEEK_END ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to seek to the end of file:%d", rc ) ;
            goto error ;
         }

         if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_SPARSE ) )
         {
            rc = ossSeek( &file,
                          len - OSS_FILE_DIRECT_IO_ALIGNMENT,
                          OSS_SEEK_CUR ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to seek offset[%lld](OSS_SEEK_CUR), rc:%d",
                       len - OSS_FILE_DIRECT_IO_ALIGNMENT, rc ) ;
               goto error ;
            }
            else
            {
               bufSize = OSS_FILE_DIRECT_IO_ALIGNMENT ;
               extendSize = OSS_FILE_DIRECT_IO_ALIGNMENT ;
            }
         }

         if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
         {
            extendBuf = ( CHAR * )ossAlignedAlloc(
                                          OSS_FILE_DIRECT_IO_ALIGNMENT,
                                          bufSize ) ;
         }
         else
         {
            extendBuf = (CHAR*) SDB_OSS_MALLOC ( bufSize ) ;
         }

         if ( NULL == extendBuf )
         {
            PD_LOG( PDERROR, "failed to allcate mem." ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         do
         {
            SINT64 writeSize = bufSize <= extendSize ?
                               bufSize : extendSize ;
            rc = ossWriteN( &file, extendBuf, writeSize ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to write file:%d", rc ) ;
               goto error ;
            }
            extendSize -= writeSize ;
         } while ( 0 < extendSize ) ;
      }
      
#ifdef _DEBUG
      {
         SINT64 sizeAfterExtend = 0 ;
         rc = ossGetFileSize( &file, &sizeAfterExtend ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get size of file:%s, rc:%d",
                    _fileName.c_str(), rc ) ;
            goto error ;
         }

         if ( ( sizeAfterExtend - sizeof( _dmsStorageUnitHeader ) ) %
              DMS_SEGMENT_SZ != 0 )
         {
            PD_LOG( PDERROR, "invalid file size:%lld, file:%s",
                    sizeAfterExtend, _fileName.c_str() ) ;
            rc = SDB_SYS ;
            SDB_ASSERT( FALSE, "impossible" ) ;
            goto error ;
         }
      }
#endif // _DEBUG
      _fileSz += len ;

   done:
      if ( file.isOpened() )
      {
         INT32 rcTmp = SDB_OK ;
         rcTmp = ossClose( file ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to close file after extend:%d", rcTmp ) ;
         }
      }

      if ( NULL != extendBuf )
      {
         if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
         {
            SDB_OSS_ORIGINAL_FREE( extendBuf ) ;
         }
         else
         {
            SDB_OSS_FREE( extendBuf ) ;
         }
      }
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA__EXTEND, rc ) ;
      return rc ;
   truncate:
      {
         INT32 rcTmp = SDB_OK ;
         rcTmp = ossTruncateFile( &file, _fileSz ) ;
         if ( SDB_OK != rcTmp )
         {
            PD_LOG( PDSEVERE, "Failed to revert the increase of segment, "
                     "rc = %d", rcTmp ) ;
            ossPanic() ;
         }
         goto done ;
      }
   error:
      {
         SINT64 nowSize = 0 ;
         INT32 rcTmp = ossGetFileSize( &file, &nowSize ) ;
         if ( SDB_OK != rcTmp )
         {
            PD_LOG( PDERROR, "failed to get file size:%d", rcTmp ) ;
            goto truncate ;
         }
         else if ( nowSize != _fileSz )
         {
            goto truncate ;
         }
         else
         {
            goto done ;
         }
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA_REMOVE, "_dmsStorageLobData::remove" )
   INT32 _dmsStorageLobData::remove()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA_REMOVE ) ;

      if ( _fullPath[ 0 ] == 0 )
      {
         goto done ;
      }

      rc = close() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to close file:%s, rc:%d",
                 _fileName.c_str(), rc ) ;
         goto error ;
      }

      rc = ossDelete( _fullPath ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to remove file:%s, rc:%d",
                 _fullPath, rc ) ;
         goto error ;
      }

      PD_LOG( PDEVENT, "remove file:%s", _fullPath ) ;
      _fullPath[ 0 ] = 0 ;

   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA_REMOVE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA__INITFILEHEADER, "_dmsStorageLobData::_initFileHeader" )
   INT32 _dmsStorageLobData::_initFileHeader( const dmsStorageInfo &info,
                                              _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA__INITFILEHEADER ) ;
      _dmsStorageUnitHeader header ;
      ossStrncpy( header._eyeCatcher, DMS_LOBD_EYECATCHER,
                  DMS_LOBD_EYECATCHER_LEN ) ;
      header._version = DMS_LOB_CUR_VERSION ;
      header._pageSize = 0 ;
      header._storageUnitSize = 0 ;
      ossStrncpy ( header._name, info._suName, DMS_SU_NAME_SZ ) ;
      header._sequence = info._sequence ;
      header._numMB    = 0 ;
      header._MBHWM    = 0 ;
      header._pageNum  = 0 ;
      header._secretValue = info._secretValue ;
      dmsLobDirectOutBuffer buffer( &header, sizeof( header ), cb ) ;
      dmsLobDirectBuffer::tuple t ;

      rc = extend( sizeof( header ) ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to extend header, rc: %d", rc ) ;
         goto error ;
      }

      rc = ossSeek ( &_file, 0, OSS_SEEK_SET ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to seek to beginning of the file, rc: %d",
                  rc ) ;
         goto error ;
      }

      if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         rc = buffer.getAlignedTuple( t ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to align the buffer:%d", rc ) ;
            goto error ;
         }
      }
      else
      {
         t.buf = &header ;
         t.size = sizeof( header ) ;   
      }

      rc = ossWriteN( &_file, ( const CHAR * )( t.buf ), t.size ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write file header:%s, rc:%d",
                 _fileName.c_str(), rc ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA__INITFILEHEADER, rc ) ;
      return rc ;
   error:
      goto done ;
   }

    // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA__VALIDATEFILE, "_dmsStorageLobData::_validateFile" )
   INT32 _dmsStorageLobData::_validateFile( const dmsStorageInfo &info,
                                            _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA__VALIDATEFILE ) ;
      _dmsStorageUnitHeader header ;
      rc = _getFileHeader( header, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get file header, rc:%d", rc ) ;
         goto error ;
      }

      if ( 0 != ossStrncmp( header._eyeCatcher, DMS_LOBD_EYECATCHER,
                            DMS_LOBD_EYECATCHER_LEN ) )
      {
         CHAR szTmp[ DMS_HEADER_EYECATCHER_LEN + 1 ] = {0} ;
         ossStrncpy( szTmp, header._eyeCatcher, DMS_HEADER_EYECATCHER_LEN ) ;
         PD_LOG( PDERROR, "invalid eye catcher:%s, file:%s",
                 szTmp, _fileName.c_str() ) ;
         rc = SDB_INVALID_FILE_TYPE ;
         goto error ;
      }

      if ( DMS_LOB_CUR_VERSION != header._version )
      {
         PD_LOG( PDERROR, "invalid version of header:%d, file:%s",
                 header._version, _fileName.c_str() ) ;
         rc = SDB_DMS_INCOMPATIBLE_VERSION ;
         goto error ;
      }

      if ( 0 != header._pageSize ||
           0 != header._storageUnitSize ||
           0 != header._numMB ||
           0 != header._MBHWM ||
           0 != header._pageNum )
      {
         PD_LOG( PDERROR, "invalid field value which not in used" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( 0 != ossStrncmp ( info._suName, header._name,
                             DMS_SU_NAME_SZ ) )
      {
         CHAR szTmp[ DMS_SU_NAME_SZ + 1 ] = {0} ;
         ossStrncpy( szTmp, header._name, DMS_SU_NAME_SZ ) ;
         PD_LOG( PDERROR, "invalid su name:%s in file:%s",
                 szTmp, _fileName.c_str() ) ;
         rc = SDB_SYS ;
      }

      if ( info._sequence != header._sequence )
      {
         PD_LOG( PDERROR, "invalid sequence:%d != %d in file:%s",
                 header._sequence, info._sequence, _fileName.c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( info._secretValue != header._secretValue )
      {
         PD_LOG( PDERROR, "invalid secret value: %lld, self: %lld, file:%s",
                 info._secretValue, header._secretValue, _fileName.c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = ossGetFileSize( &_file, &_fileSz ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get size of file:%s, rc:%d",
                 _fileName.c_str(), rc ) ;
         goto error ;
      }
      _lastSz = _fileSz ;

      if ( ( _fileSz - sizeof( header ) ) % DMS_SEGMENT_SZ != 0 )
      {
         PD_LOG( PDERROR, "invalid file size:%lld, file:%s",
                 _fileSz, _fileName.c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      _pageSz = info._lobdPageSize ;
      if ( !ossIsPowerOf2( _pageSz, &_logarithmic ) )
      {
         PD_LOG( PDERROR, "Page size[%d] is not power of 2", _pageSz ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      _segmentPages = getSegmentSize() >> _logarithmic ;
      if ( !ossIsPowerOf2( _segmentPages, &_segmentPagesSquare ) )
      {
         PD_LOG( PDERROR, "Segment pages[%u] must be the power of 2",
                 _segmentPages ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA__VALIDATEFILE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSSTORAGELOBDATA__FETFILEHEADER, "_dmsStorageLobData::_getFileHeader" )
   INT32 _dmsStorageLobData::_getFileHeader( _dmsStorageUnitHeader &header,
                                             _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_DMSSTORAGELOBDATA__FETFILEHEADER ) ;
      INT64 fileLen = 0 ;
      SINT64 readLen = 0 ;
      rc = ossGetFileSize( &_file, &fileLen ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to file len:%d", rc ) ;
         goto error ;
      }

      if ( fileLen < ( INT64 )sizeof( _dmsStorageUnitHeader ) )
      {
         PD_LOG( PDERROR, "invalid length of file:%lld", fileLen ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = ossSeek( &_file, 0, OSS_SEEK_SET ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to seek file:%d", rc ) ;
         goto error ;
      }

      {
      _dmsLobDirectInBuffer buffer( &header, sizeof( header ),
                                    0, cb ) ;
      _dmsLobDirectBuffer::tuple t ;

      if ( !OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         t.buf = &header ;
         t.size = sizeof( header ) ;
         t.offset = 0 ;
      }
      else
      {
         rc = buffer.getAlignedTuple( t ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to align the memory:%d", rc ) ;
            goto error ;
         }
      }

      rc = ossReadN( &_file, t.size,
                     ( CHAR * )( t.buf ), readLen ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read file:%d", rc ) ;
         goto error ;
      }

      if ( OSS_BIT_TEST( _flags, DMS_LOBD_FLAG_DIRECT ) )
      {
         buffer.copy2UsrBuf( t ) ;
      }
      }

      SDB_ASSERT( sizeof( _dmsStorageUnitHeader ) == readLen, "impossible" ) ;
   done:
      PD_TRACE_EXITRC( SDB_DMSSTORAGELOBDATA__FETFILEHEADER, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

