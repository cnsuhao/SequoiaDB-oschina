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

   Source File Name = dmsReorgUnit.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/
#include "dmsReorgUnit.hpp"
#include "dmsStorageUnit.hpp"
#include "dmsRecord.hpp"
#include "ossIO.hpp"
#include "pdTrace.hpp"
#include "dmsTrace.hpp"
#include "pmdEDU.hpp"
#include "dmsCompress.hpp"

namespace engine
{
#define DMS_REORG_UNIT_HEAD_SIZE_UNIT 1024
   _dmsReorgUnit::_dmsReorgUnit ( CHAR *pFileName, SINT32 pageSize )
   {
      _pCurrentExtent = NULL ;
      ossMemset ( _fileName, 0, sizeof(_fileName) ) ;
      ossStrncpy ( _fileName, pFileName, OSS_MAX_PATHSIZE ) ;
      _pageSize = pageSize ;
   }
   _dmsReorgUnit::~_dmsReorgUnit ()
   {
      close() ;
   }
   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT__INIT, "_dmsReorgUnit::_init" )
   INT32 _dmsReorgUnit::_init ( BOOLEAN createNew )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT__INIT );
      class _reorgUnitHead *unitHead = NULL ;
      INT32 bufSize = ossRoundUpToMultipleX (
                            sizeof ( class _reorgUnitHead ),
                            DMS_REORG_UNIT_HEAD_SIZE_UNIT ) ;
      INT32 restSize = bufSize ;
      _headSize = bufSize ;
      CHAR *pBuffer = (CHAR*)SDB_OSS_MALLOC (bufSize) ;
      if ( !pBuffer )
      {
         PD_LOG ( PDERROR, "Failed to allocate %d bytes of memory", bufSize ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      unitHead = (class _reorgUnitHead*)pBuffer ;
      ossMemset ( unitHead, 0, bufSize ) ;
      if ( createNew )
      {
         SINT64 writeSize = 0 ;
         _readOnly = FALSE ;
         ossMemcpy ( unitHead->_eyeCatcher, DMS_REORG_UNIT_EYECATCHER,
                     DMS_REORG_UNIT_EYECATCHER_LEN ) ;
         unitHead->_headerSize = bufSize ;
         ossMemcpy ( unitHead->_fileName, _fileName, OSS_MAX_PATHSIZE ) ;
         unitHead->_pageSize = _pageSize ;
         while ( restSize != 0 )
         {
            rc = ossWrite ( &_file, &pBuffer[bufSize-restSize], restSize,
                            &writeSize ) ;
            if ( rc && SDB_INTERRUPT != rc )
            {
               PD_LOG ( PDERROR, "Failed to write into file: %s, rc = %d",
                        _fileName, rc ) ;
               goto error ;
            }
            restSize -= writeSize ;
            rc = SDB_OK ;
         }
      }
      else
      {
         SINT64 readSize = 0 ;
         _readOnly = TRUE ;
         while ( restSize > 0 )
         {
            rc = ossRead ( &_file, &pBuffer[bufSize-restSize], restSize, &readSize ) ;
            if ( rc && SDB_INTERRUPT != rc )
            {
               PD_LOG ( PDERROR, "Failed to read from file: %s, rc = %d",
                        _fileName, rc ) ;
               goto error ;
            }
            restSize -= readSize ;
            rc = SDB_OK ;
         }
         if ( ossMemcmp ( unitHead->_eyeCatcher, DMS_REORG_UNIT_EYECATCHER,
                          DMS_REORG_UNIT_EYECATCHER_LEN ) ||
              unitHead->_headerSize != bufSize )
         {
            PD_LOG ( PDERROR, "Invalid reorg file is detected" ) ;
            rc = SDB_DMS_INVALID_REORG_FILE ;
            goto error ;
         }
      }
   done :
      if ( pBuffer )
      {
         SDB_OSS_FREE ( pBuffer ) ;
      }
      PD_TRACE_EXITRC ( SDB__DMSROUNIT__INIT, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_CLNUP, "_dmsReorgUnit::cleanup" )
   INT32 _dmsReorgUnit::cleanup ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_CLNUP );
      close() ;
      rc = ossDelete ( _fileName ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to delete reorg unit temp file, rc = %d",
                  rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_CLNUP, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_OPEN, "_dmsReorgUnit::open" )
   INT32 _dmsReorgUnit::open ( BOOLEAN createNew )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_OPEN );
      rc = ossOpen ( _fileName, OSS_READWRITE|
                     (createNew?OSS_CREATEONLY:OSS_DEFAULT),
                     OSS_RU|OSS_WU, _file ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to create file %s, rc = %d",
                  _fileName, rc ) ;
         goto error ;
      }
      rc = _init ( createNew ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to initialize file %s, rc = %d",
                  _fileName, rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_OPEN, rc );
      return rc ;
   error :
      goto done ;
   }

   void _dmsReorgUnit::close ()
   {
      ossClose ( _file ) ;
   }
   void _dmsReorgUnit::reset ()
   {
      ossSeek ( &_file, 0, OSS_SEEK_SET ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_IMPMME, "_dmsReorgUnit::importMME" )
   INT32 _dmsReorgUnit::importMME ( const CHAR *pMME )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_IMPMME );
      SDB_ASSERT ( pMME, "pMME can't be NULL" ) ;
      INT32 restSize = DMS_MB_SIZE ;
      INT64 writeSize = 0 ;
      INT32 bufSize = restSize ;
      if ( _readOnly )
      {
         PD_LOG ( PDERROR, "Modify is not allowed" ) ;
         rc = SDB_DMS_REORG_FILE_READONLY ;
         goto error ;
      }
      rc = ossSeek ( &_file, _headSize, OSS_SEEK_SET ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to seek to %d from file %s, rc = %d",
                  _headSize, _fileName, rc ) ;
         goto error ;
      }
      while ( restSize != 0 )
      {
         rc = ossWrite ( &_file, &pMME[bufSize-restSize], restSize, &writeSize ) ;
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG ( PDERROR, "Failed to write MME into file: %s, rc = %d",
                     _fileName, rc ) ;
            goto error ;
         }
         restSize -= (INT32)writeSize ;
         rc = SDB_OK ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_IMPMME, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_EXPMME, "_dmsReorgUnit::exportMME" )
   INT32 _dmsReorgUnit::exportMME ( CHAR *pMME )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_EXPMME );
      SDB_ASSERT ( pMME, "pMME can't be NULL" ) ;
      INT32 restSize = DMS_MB_SIZE ;
      INT64 readSize = 0 ;
      INT32 bufSize = restSize ;
      rc = ossSeek ( &_file, _headSize, OSS_SEEK_SET ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to seek to %d from file %s, rc = %d",
                  _headSize, _fileName, rc ) ;
         goto error ;
      }
      while ( restSize > 0 )
      {
         rc = ossRead ( &_file, &pMME[bufSize-restSize], restSize, &readSize ) ;
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG ( PDERROR, "Failed to write MME into file: %s, rc = %d",
                     _fileName, rc ) ;
            goto error ;
         }
         restSize -= (INT32)readSize ;
         rc = SDB_OK ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_EXPMME, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT__ALCEXT, "_dmsReorgUnit::_allocateExtent" )
   INT32 _dmsReorgUnit::_allocateExtent ( INT32 requestSize )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT__ALCEXT );
      SDB_ASSERT ( !_pCurrentExtent, "current extent must be NULL" ) ;
      if ( requestSize < DMS_MIN_EXTENT_SZ(_pageSize) )
         requestSize = DMS_MIN_EXTENT_SZ(_pageSize) ;
      else if ( requestSize > DMS_MAX_EXTENT_SZ )
         requestSize = DMS_MAX_EXTENT_SZ ;
      else
         requestSize = ossRoundUpToMultipleX ( requestSize, _pageSize ) ;
      _pCurrentExtent = (CHAR*)SDB_OSS_MALLOC ( requestSize ) ;
      if ( !_pCurrentExtent )
      {
         PD_LOG ( PDERROR, "Unable to allocate %d bytes memory", requestSize ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      _currentExtentSize = requestSize ;
      _initExtentHeader ( (dmsExtent*)_pCurrentExtent,
                          _currentExtentSize/_pageSize ) ;
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT__ALCEXT, rc );
      return rc ;
   error :
      goto done ;
   }
   void _dmsReorgUnit::_initExtentHeader ( dmsExtent *extAddr, UINT16 numPages )
   {
      SDB_ASSERT ( _pageSize * numPages == _currentExtentSize,
                   "extent size doesn't match" ) ;
      extAddr->_eyeCatcher[0]          = DMS_EXTENT_EYECATCHER0 ;
      extAddr->_eyeCatcher[1]          = DMS_EXTENT_EYECATCHER1 ;
      extAddr->_blockSize              = numPages ;
      extAddr->_mbID                   = 0 ;
      extAddr->_flag                   = DMS_EXTENT_FLAG_INUSE ;
      extAddr->_version                = DMS_EXTENT_CURRENT_V ;
      extAddr->_logicID                = DMS_INVALID_EXTENT ;
      extAddr->_prevExtent             = DMS_INVALID_EXTENT ;
      extAddr->_nextExtent             = DMS_INVALID_EXTENT ;
      extAddr->_recCount               = 0 ;
      extAddr->_firstRecordOffset      = DMS_INVALID_EXTENT ;
      extAddr->_lastRecordOffset       = DMS_INVALID_EXTENT ;
      extAddr->_freeSpace              = _pageSize * numPages -
                                         sizeof(dmsExtent) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT__FLSEXT, "_dmsReorgUnit::_flushExtent" )
   INT32 _dmsReorgUnit::_flushExtent ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT__FLSEXT );
      SDB_ASSERT ( _pCurrentExtent, "current extent can't be NULL" ) ;
      INT32 restSize = _currentExtentSize ;
      INT64 writeSize = 0 ;
      INT32 bufSize = restSize ;
      if ( _readOnly )
      {
         PD_LOG ( PDERROR, "Modify is not allowed" ) ;
         rc = SDB_DMS_REORG_FILE_READONLY ;
         goto error ;
      }
      while ( restSize != 0 )
      {
         rc = ossWrite ( &_file, &_pCurrentExtent[bufSize-restSize],
                         restSize, &writeSize ) ;
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG ( PDERROR, "Failed to flush extent into file: %s, rc = %d",
                     _fileName, rc ) ;
            goto error ;
         }
         restSize -= (INT32)writeSize ;
         rc = SDB_OK ;
      }
      SDB_OSS_FREE ( _pCurrentExtent ) ;
      _pCurrentExtent = NULL ;
      _currentExtentSize = 0 ;
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT__FLSEXT, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_FLUSH, "_dmsReorgUnit::flush" )
   INT32 _dmsReorgUnit::flush ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_FLUSH );
      if ( _pCurrentExtent )
      {
         rc = _flushExtent () ;
      }
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_FLUSH, rc );
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_INSRCD, "_dmsReorgUnit::insertRecord" )
   INT32 _dmsReorgUnit::insertRecord ( BSONObj &obj,
                                       _pmdEDUCB *cb, UINT32 attributes )
   {
      INT32 rc                     = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_INSRCD );
      UINT32 dmsrecordSize         = 0 ;
      ossValuePtr recordPtr        = 0 ;
      ossValuePtr prevPtr          = 0 ;
      dmsOffset offset             = DMS_INVALID_OFFSET ;
      dmsOffset recordOffset       = DMS_INVALID_OFFSET ;
      dmsExtent *currentExtent     = (dmsExtent*)_pCurrentExtent ;
      BOOLEAN isCompressed         = FALSE ;
      const CHAR *compressedData   = NULL ;
      INT32 compressedDataSize     = 0 ;

      if ( obj.objsize() + DMS_RECORD_METADATA_SZ >
           DMS_RECORD_MAX_SZ )
      {
         rc = SDB_CORRUPTED_RECORD ;
         goto error ;
      }

      if ( OSS_BIT_TEST ( attributes, DMS_MB_ATTR_COMPRESSED ) )
      {
         rc = dmsCompress ( cb, obj, NULL, 0, &compressedData,
                            &compressedDataSize ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to compress record, rc = %d: %s",
                       rc, obj.toString().c_str() ) ;
         dmsrecordSize = compressedDataSize + sizeof(INT32) ;
         if ( dmsrecordSize > (UINT32)(obj.objsize()) )
         {
            dmsrecordSize = obj.objsize() ;
         }
         else
         {
            isCompressed = TRUE ;
         }
      }
      else
      {
         dmsrecordSize = obj.objsize() ;
      }
      dmsrecordSize += DMS_RECORD_METADATA_SZ ;
      dmsrecordSize *= DMS_RECORD_OVERFLOW_RATIO ;
      dmsrecordSize = OSS_MIN(DMS_RECORD_MAX_SZ, ossAlignX(dmsrecordSize,4)) ;
   alloc:
      if ( !_pCurrentExtent )
      {
         rc = _allocateExtent ( dmsrecordSize <<
                                DMS_RECORDS_PER_EXTENT_SQUARE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to allocate new extent in reorg file, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         currentExtent = (dmsExtent*)_pCurrentExtent ;
      }
      if ( dmsrecordSize > (UINT32)currentExtent->_freeSpace )
      {
         rc = _flushExtent () ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to flush extent, rc = %d", rc ) ;
            goto error ;
         }
         goto alloc ;
      }
      recordOffset = _currentExtentSize - currentExtent->_freeSpace ;
      recordPtr = ((ossValuePtr)currentExtent) + recordOffset ;
      if ( currentExtent->_freeSpace - (INT32)dmsrecordSize <
           (INT32)DMS_MIN_RECORD_SZ &&
           currentExtent->_freeSpace <= (INT32)DMS_RECORD_MAX_SZ )
      {
         dmsrecordSize = (UINT32)currentExtent->_freeSpace ;
      }

      DMS_RECORD_SETSTATE ( recordPtr, DMS_RECORD_FLAG_NORMAL ) ;
      DMS_RECORD_RESETATTR ( recordPtr ) ;
      DMS_RECORD_SETMYOFFSET ( recordPtr, recordOffset ) ;
      DMS_RECORD_SETSIZE ( recordPtr, dmsrecordSize ) ;
      if ( isCompressed )
      {
         DMS_RECORD_SETATTR ( recordPtr, DMS_RECORD_FLAG_COMPRESSED ) ;
         DMS_RECORD_SETDATA ( recordPtr, compressedData, compressedDataSize ) ;
      }
      else
      {
         DMS_RECORD_SETDATA ( recordPtr, obj.objdata(), obj.objsize() ) ;
      }
      DMS_RECORD_SETNEXTOFFSET ( recordPtr, DMS_INVALID_OFFSET ) ;
      DMS_RECORD_SETPREVOFFSET ( recordPtr, DMS_INVALID_OFFSET ) ;
      currentExtent->_recCount ++ ;
      currentExtent->_freeSpace -= dmsrecordSize ;
      offset = currentExtent->_lastRecordOffset ;
      if ( DMS_INVALID_OFFSET != offset )
      {
         prevPtr = ((ossValuePtr)currentExtent) + offset ;
         DMS_RECORD_SETNEXTOFFSET ( prevPtr, recordOffset ) ;
         DMS_RECORD_SETPREVOFFSET ( recordPtr, offset ) ;
      }
      currentExtent->_lastRecordOffset = recordOffset ;
      offset = currentExtent->_firstRecordOffset ;
      if ( DMS_INVALID_OFFSET == offset )
      {
         currentExtent->_firstRecordOffset = recordOffset ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_INSRCD, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_GETNXTEXTSIZE, "_dmsReorgUnit::getNextExtentSize" )
   INT32 _dmsReorgUnit::getNextExtentSize ( SINT32 &size )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_GETNXTEXTSIZE );
      CHAR buffer [ sizeof(dmsExtent) ] ;
      dmsExtent *extent = (dmsExtent*)buffer ;
      ossMemset ( buffer, 0, sizeof(buffer) ) ;
      INT32 restSize = sizeof(buffer) ;
      INT64 readSize = 0 ;
      INT32 bufSize = restSize ;
      while ( restSize > 0 )
      {
         rc = ossRead ( &_file, &buffer[bufSize-restSize], restSize,
                        &readSize ) ;
         if ( rc && SDB_INTERRUPT != rc )
         {
            if ( SDB_EOF != rc )
            {
               PD_LOG ( PDERROR, "Failed to read header from file: %s, rc = %d",
                        _fileName, rc ) ;
            }
            goto error ;
         }
         restSize -= (INT32)readSize ;
         rc = SDB_OK ;
      }
      if ( DMS_EXTENT_EYECATCHER0 != extent->_eyeCatcher[0] ||
           DMS_EXTENT_EYECATCHER1 != extent->_eyeCatcher[1] )
      {
         PD_LOG ( PDERROR, "Invalid eye catcher" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      size = extent->_blockSize * _pageSize ;
      rc = ossSeek ( &_file, (INT64)(0-sizeof(dmsExtent)), OSS_SEEK_CUR ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to seek back %d bytes offset, rc = %d",
                  sizeof(dmsExtent) ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_GETNXTEXTSIZE, rc );
      return rc ;
   error :
      size = 0 ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_EXPHEAD, "_dmsReorgUnit::exportHead" )
   INT32 _dmsReorgUnit::exportHead ( CHAR *pBuffer )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_EXPHEAD );
      SDB_ASSERT ( pBuffer, "pBuffer can't be NULL" ) ;
      INT32 restSize = _headSize ;
      INT64 readSize = 0 ;
      INT32 bufSize = restSize ;
      rc = ossSeek ( &_file, 0, OSS_SEEK_SET ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to seek to %d from file %s, rc = %d",
                  0, _fileName, rc ) ;
         goto error ;
      }
      while ( restSize > 0 )
      {
         rc = ossRead ( &_file, &pBuffer[bufSize-restSize], restSize, &readSize ) ;
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG ( PDERROR, "Failed to read head from file: %s, rc = %d",
                     _fileName, rc ) ;
            goto error ;
         }
         restSize -= (INT32)readSize ;
         rc = SDB_OK ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_EXPHEAD, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_EXPEXT, "_dmsReorgUnit::exportExtent" )
   INT32 _dmsReorgUnit::exportExtent ( CHAR *pBuffer )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_EXPEXT );
      dmsExtent *extent = (dmsExtent*)pBuffer ;
      ossMemset ( pBuffer, 0, sizeof(dmsExtent) ) ;
      INT32 restSize = sizeof(dmsExtent) ;
      INT64 readSize = 0 ;
      INT32 bufSize = restSize ;
      while ( restSize > 0 )
      {
         rc = ossRead ( &_file, &pBuffer[bufSize-restSize], restSize, &readSize ) ;
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG ( PDERROR, "Failed to read header from file: %s, rc = %d",
                     _fileName, rc ) ;
            goto error ;
         }
         restSize -= (INT32)readSize ;
         rc = SDB_OK ;
      }
      if ( DMS_EXTENT_EYECATCHER0 != extent->_eyeCatcher[0] ||
           DMS_EXTENT_EYECATCHER1 != extent->_eyeCatcher[1] )
      {
         PD_LOG ( PDERROR, "Invalid eye catcher" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      restSize = extent->_blockSize * _pageSize - sizeof(dmsExtent) ;
      readSize = 0 ;
      bufSize = restSize ;
      while ( restSize > 0 )
      {
         rc = ossRead ( &_file, &pBuffer[sizeof(dmsExtent)+bufSize-restSize],
                        restSize, &readSize ) ;
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG ( PDERROR, "Failed to read header from file: %s, rc = %d",
                     _fileName, rc ) ;
            goto error ;
         }
         restSize -= (INT32)readSize ;
         rc = SDB_OK ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_EXPEXT, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSROUNIT_VLDHDBUFF, "_dmsReorgUnit::validateHeadBuffer" )
   INT32 _dmsReorgUnit::validateHeadBuffer ( CHAR *pBuffer )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSROUNIT_VLDHDBUFF );
      SDB_ASSERT ( pBuffer, "pBuffer can't be NULL" ) ;
      class _reorgUnitHead *unitHead = (class _reorgUnitHead *)pBuffer ;
      if ( ossMemcmp ( pBuffer, DMS_REORG_UNIT_EYECATCHER,
                       DMS_REORG_UNIT_EYECATCHER_LEN ) )
      {
         PD_LOG ( PDWARNING, "reorg file header is invalid" ) ;
         rc = SDB_DMS_INVALID_REORG_FILE ;
         goto error ;
      }
      if ( ossRoundUpToMultipleX ( unitHead->_headerSize,
                                   DMS_REORG_UNIT_HEAD_SIZE_UNIT ) !=
           unitHead->_headerSize )
      {
         PD_LOG ( PDWARNING, "reorg file header size is not valid: %d",
                  unitHead->_headerSize ) ;
         rc = SDB_DMS_INVALID_REORG_FILE ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSROUNIT_VLDHDBUFF, rc );
      return rc ;
   error :
      goto done ;
   }
}
