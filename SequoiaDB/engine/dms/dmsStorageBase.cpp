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

   Source File Name = dmsStorageBase.cpp

   Descriptive Name = Data Management Service Storage Unit Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          12/08/2013  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsStorageBase.hpp"
#include "dmsStorageData.hpp"
#include "dmsStorageJob.hpp"
#include "pdTrace.hpp"
#include "dmsTrace.hpp"
#include "pmdStartup.hpp"
#include "utilStr.hpp"

using namespace bson ;

namespace engine
{

   #define DMS_SME_FREE_STR             "Free"
   #define DMS_SME_ALLOCATED_STR        "Occupied"

   void smeMask2String( CHAR state, CHAR * pBuffer, INT32 buffSize )
   {
      SDB_ASSERT( DMS_SME_FREE == state || DMS_SME_ALLOCATED == state,
                  "SME Mask must be 1 or 0" ) ;
      SDB_ASSERT( pBuffer && buffSize > 0 , "Buffer can not be NULL" ) ;

      if ( DMS_SME_FREE == state )
      {
         ossStrncpy( pBuffer, DMS_SME_FREE_STR, buffSize - 1 ) ;
      }
      else
      {
         ossStrncpy( pBuffer, DMS_SME_ALLOCATED_STR, buffSize - 1 ) ;
      }
      pBuffer[ buffSize - 1 ] = 0 ;
   }


   #define DMS_EXTEND_THRESHOLD_SIZE      ( 33554432 )   // 32MB
   /*
      _dmsStorageBase : implement
   */
   _dmsStorageBase::_dmsStorageBase( const CHAR *pSuFileName,
                                     dmsStorageInfo *pInfo )
   {
      SDB_ASSERT( pSuFileName, "SU file name can't be NULL" ) ;

      _pStorageInfo       = pInfo ;
      _dmsHeader          = NULL ;
      _dmsSME             = NULL ;
      _dataSegID          = 0 ;
      _dirtyList          = NULL ;

      _pageNum            = 0 ;
      _maxSegID           = -1 ;
      _segmentPages       = 0 ;
      _segmentPagesSquare = 0 ;
      _pageSizeSquare     = 0 ;
      _isTempSU           = FALSE ;
      _pageSize           = 0 ;
      _lobPageSize        = 0 ;

      ossStrncpy( _suFileName, pSuFileName, DMS_SU_FILENAME_SZ ) ;
      _suFileName[ DMS_SU_FILENAME_SZ ] = 0 ;
      ossMemset( _fullPathName, 0, sizeof(_fullPathName) ) ;

      if ( 0 == ossStrcmp( pInfo->_suName, SDB_DMSTEMP_NAME ) )
      {
         _isTempSU = TRUE ;
      }
   }

   _dmsStorageBase::~_dmsStorageBase()
   {
      closeStorage() ;
      _pStorageInfo = NULL ;
      if ( _dirtyList )
      {
         SDB_OSS_FREE ( _dirtyList ) ;
      }
   }

   const CHAR* _dmsStorageBase::getSuFileName () const
   {
      return _suFileName ;
   }

   const CHAR* _dmsStorageBase::getSuName () const
   {
      if ( _pStorageInfo )
      {
         return _pStorageInfo->_suName ;
      }
      return "" ;
   }

   INT32 _dmsStorageBase::openStorage( const CHAR *pPath, BOOLEAN createNew,
                                       BOOLEAN delWhenExist )
   {
      INT32 rc               = SDB_OK ;
      UINT64 fileSize        = 0 ;
      UINT64 currentOffset   = 0 ;
      UINT32 mode = OSS_READWRITE|OSS_EXCLUSIVE ;

      SDB_ASSERT( pPath, "path can't be NULL" ) ;

      if ( NULL == _pStorageInfo )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

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

      rc = utilBuildFullPath( pPath, _suFileName, OSS_MAX_PATHSIZE,
                              _fullPathName ) ;

      if ( rc )
      {
         PD_LOG ( PDERROR, "Path+filename are too long: %s; %s", pPath,
                  _suFileName ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      PD_LOG ( PDDEBUG, "Open storage unit file %s", _fullPathName ) ;

      // open the file, create one if not exist
      rc = ossMmapFile::open ( _fullPathName, mode, OSS_RU|OSS_WU|OSS_RG ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to open %s, rc=%d", _fullPathName, rc ) ;
         goto error ;
      }
      if ( createNew )
      {
         PD_LOG( PDEVENT, "Create storage unit file[%s] succeed, mode: %x",
                 _fullPathName, mode ) ;
      }

      rc = ossMmapFile::size ( fileSize ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get file size: %s", _fullPathName ) ;
         goto error ;
      }

      // is it a brand new file
      if ( 0 == fileSize )
      {
         // if it's a brand new file but we don't ask for creating new storage
         // unit, then we exit with invalid su error
         if ( !createNew )
         {
            PD_LOG ( PDERROR, "storage unit file is empty: %s", _suFileName ) ;
            rc = SDB_DMS_INVALID_SU ;
            goto error ;
         }
         rc = _initializeStorageUnit () ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to initialize Storage Unit, rc=%d", rc ) ;
            goto error ;
         }
         // then we get the size again to make sure it's what we need
         rc = ossMmapFile::size ( fileSize ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get file size: %s", _suFileName ) ;
            goto error ;
         }
      }

      if ( fileSize < _dataOffset() )
      {
         PD_LOG ( PDERROR, "Invalid storage unit size: %s", _suFileName ) ;
         PD_LOG ( PDERROR, "Expected more than %d bytes, actually read %lld "
                  "bytes", _dataOffset(), fileSize ) ;
         rc = SDB_DMS_INVALID_SU ;
         goto error ;
      }

      // map metadata
      // header, 64K
      rc = map ( DMS_HEADER_OFFSET, DMS_HEADER_SZ, (void**)&_dmsHeader ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to map header: %s", _suFileName ) ;
         goto error ;
      }

      /// lobPageSize is 0 if it was created by db with older version.
      /// we reassign it with 256K -- yunwu
      if ( 0 == _dmsHeader->_lobdPageSize )
      {
         _dmsHeader->_lobdPageSize = DMS_DEFAULT_LOB_PAGE_SZ ;
      }

      // after we load SU, let's verify it's expected file
      rc = _validateHeader( _dmsHeader ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Storage Unit Header is invalid: %s, rc: %d",
                  _suFileName, rc ) ;
         goto error ;
      }

      // SME, 8MB
      rc = map ( DMS_SME_OFFSET, DMS_SME_SZ, (void**)&_dmsSME ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to map SME: %s", _suFileName ) ;
         goto error ;
      }

      // initialize SME Manager, which is used to do fast-lookup and release
      // for extents. Note _pageSize is initialized in _validateHeader, so
      // we are safe to use page size here
      rc = _smeMgr.init ( this, _dmsSME ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to initialize SME, rc = %d", rc ) ;
         goto error ;
      }

      rc = _onMapMeta( (UINT64)( DMS_SME_OFFSET + DMS_SME_SZ ) ) ;
      PD_RC_CHECK( rc, PDERROR, "map file[%s] meta failed, rc: %d",
                   _suFileName, rc ) ;

      // make sure the file size is multiple of segments
      if ( 0 != ( fileSize - _dataOffset() ) % _getSegmentSize() )
      {
         PD_LOG ( PDERROR, "Unexpected length[%d] of file: %s", fileSize,
                  _suFileName ) ;
         rc = SDB_DMS_INVALID_SU ;
         goto error ;
      }
      if ( fileSize != (UINT64)_dmsHeader->_storageUnitSize * pageSize() )
      {
         PD_LOG( PDWARNING, "File[%s] size[%llu] is not match with storage "
                 "unit pages[%u]", _suFileName, fileSize,
                 _dmsHeader->_storageUnitSize ) ;

         fileSize = (UINT64)_dmsHeader->_storageUnitSize * pageSize() ;
         rc = ossTruncateFile( &_file, fileSize ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Truncate file[%s] to size[%lld] failed, rc: %d",
                    _suFileName, fileSize, rc ) ;
            goto error ;
         }
         PD_LOG( PDEVENT, "Truncate file[%s] to size[%lld]", _suFileName,
                 fileSize ) ;
      }

      // loop and map each segment into separate mem range
      _dataSegID = ossMmapFile::segmentSize() ;
      currentOffset = _dataOffset() ;
      while ( currentOffset < fileSize )
      {
         rc = map ( currentOffset, _getSegmentSize(), NULL ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to map data segment at offset %lld",
                     currentOffset ) ;
            goto error ;
         }
         currentOffset += _getSegmentSize() ;
      }
      _maxSegID = (INT32)ossMmapFile::segmentSize() - 1 ;

      if ( isTempSU() && createNew )
      {
         rc = _extendSegments( 1 ) ;
         PD_RC_CHECK( rc, PDERROR, "Extent segments failed, rc: %d", rc ) ;
      }

      // create dirtyList to record dirty pages
      // note dirty list doesn't contain header and metadata segments, only for
      // data segments
      if ( _dirtyList )
      {
         SDB_OSS_FREE ( _dirtyList ) ;
      }
      // memory will be freed in destructor
      _dirtyList = (CHAR*)SDB_OSS_MALLOC ( maxSegmentNum() / 8 ) ;
      if ( !_dirtyList )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "Failed to allocate memory for dirty list for "
                  "%d bytes", maxSegmentNum() / 8 ) ;
         goto error ;
      }
      ossMemset ( _dirtyList, 0, maxSegmentNum() / 8 ) ;

   done:
      return rc ;
   error:
      ossMmapFile::close () ;
      goto done ;
   }

   void _dmsStorageBase::closeStorage ()
   {
      // be sure the extend job has quit
      ossLatch( &_segmentLatch, SHARED ) ;
      ossUnlatch( &_segmentLatch, SHARED );

      if ( ossMmapFile::_opened )
      {
         _dmsHeader     = NULL ;
         _dmsSME        = NULL ;
         // release header and attempt to get page cleaner latch
         // once page cleaner released the latch, the function is able to
         // proceed
         ossLatch ( &_pagecleanerLatch ) ;
         _onClosed() ;

         ossMmapFile::close() ;
         ossUnlatch ( &_pagecleanerLatch ) ;
      }
      _maxSegID = -1 ;
   }

   INT32 _dmsStorageBase::removeStorage()
   {
      INT32 rc = SDB_OK ;

      if ( _fullPathName[0] == 0 )
      {
         goto done ;
      }

      // close
      closeStorage() ;

      rc = ossDelete( _fullPathName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to remove storeage unit file: %s, "
                   "rc: %d", _fullPathName, rc ) ;

      PD_LOG( PDEVENT, "Remove storage unit file[%s] succeed", _fullPathName ) ;
      _fullPathName[ 0 ] = 0 ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _dmsStorageBase::_writeFile( OSSFILE *file, const CHAR * pData,
                                      INT64 dataLen )
   {
      INT32 rc = SDB_OK;
      SINT64 written = 0;
      SINT64 needWrite = dataLen;
      SINT64 bufOffset = 0;

      while ( 0 < needWrite )
      {
         rc = ossWrite( file, pData + bufOffset, needWrite, &written );
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG( PDWARNING, "Failed to write data, rc: %d", rc ) ;
            goto error ;
         }
         needWrite -= written ;
         bufOffset += written ;

         rc = SDB_OK ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _dmsStorageBase::_initializeStorageUnit ()
   {
      INT32   rc        = SDB_OK ;
      _dmsHeader        = NULL ;
      _dmsSME           = NULL ;

      // move to beginning of the file
      rc = ossSeek ( &_file, 0, OSS_SEEK_SET ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to seek to beginning of the file, rc: %d",
                  rc ) ;
         goto error ;
      }

      // allocate buffer for dmsHeader
      _dmsHeader = SDB_OSS_NEW dmsStorageUnitHeader ;
      if ( !_dmsHeader )
      {
         PD_LOG ( PDSEVERE, "Failed to allocate memory to for dmsHeader" ) ;
         PD_LOG ( PDSEVERE, "Requested memory: %d bytes", DMS_HEADER_SZ ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      // initialize a new header with empty size
      _initHeader ( _dmsHeader ) ;

      // write the buffer into file
      rc = _writeFile ( &_file, (const CHAR *)_dmsHeader, DMS_HEADER_SZ ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to write to file duirng SU init, rc: %d",
                  rc ) ;
         goto error ;
      }
      SDB_OSS_DEL _dmsHeader ;
      _dmsHeader = NULL ;

      // then SME
      _dmsSME = SDB_OSS_NEW dmsSpaceManagementExtent ;
      if ( !_dmsSME )
      {
         PD_LOG ( PDSEVERE, "Failed to allocate memory to for dmsSME" ) ;
         PD_LOG ( PDSEVERE, "Requested memory: %d bytes", DMS_SME_SZ ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = _writeFile ( &_file, (CHAR *)_dmsSME, DMS_SME_SZ ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to write to file duirng SU init, rc: %d",
                  rc ) ;
         goto error ;
      }
      SDB_OSS_DEL _dmsSME ;
      _dmsSME = NULL ;

      rc = _onCreate( &_file, (UINT64)( DMS_HEADER_SZ + DMS_SME_SZ )  ) ;
      PD_RC_CHECK( rc, PDERROR, "create storage unit failed, rc: %d", rc ) ;

   done :
      return rc ;
   error :
      if (_dmsHeader)
      {
         SDB_OSS_DEL _dmsHeader ;
         _dmsHeader = NULL ;
      }
      if (_dmsSME)
      {
         SDB_OSS_DEL _dmsSME ;
         _dmsSME = NULL ;
      }
      goto done ;
   }

   void _dmsStorageBase::_initHeaderPageSize( dmsStorageUnitHeader * pHeader,
                                              dmsStorageInfo * pInfo )
   {
      pHeader->_pageSize      = pInfo->_pageSize ;
      pHeader->_lobdPageSize  = pInfo->_lobdPageSize ;
   }

   void _dmsStorageBase::_initHeader( dmsStorageUnitHeader * pHeader )
   {
      ossStrncpy( pHeader->_eyeCatcher, _getEyeCatcher(),
                  DMS_HEADER_EYECATCHER_LEN ) ;
      pHeader->_version = _curVersion() ;
      _initHeaderPageSize( pHeader, _pStorageInfo ) ;
      pHeader->_storageUnitSize = _dataOffset() / pHeader->_pageSize ;
      ossStrncpy ( pHeader->_name, _pStorageInfo->_suName, DMS_SU_NAME_SZ ) ;
      pHeader->_sequence = _pStorageInfo->_sequence ;
      pHeader->_numMB    = 0 ;
      pHeader->_MBHWM    = 0 ;
      pHeader->_pageNum  = 0 ;
      pHeader->_secretValue = _pStorageInfo->_secretValue ;
      pHeader->_createLobs = 0 ;
   }

   INT32 _dmsStorageBase::_checkPageSize( dmsStorageUnitHeader * pHeader )
   {
      INT32 rc = SDB_OK ;

      // check page size
      if ( DMS_PAGE_SIZE4K  != pHeader->_pageSize &&
           DMS_PAGE_SIZE8K  != pHeader->_pageSize &&
           DMS_PAGE_SIZE16K != pHeader->_pageSize &&
           DMS_PAGE_SIZE32K != pHeader->_pageSize &&
           DMS_PAGE_SIZE64K != pHeader->_pageSize )
      {
         PD_LOG ( PDERROR, "Invalid page size: %u, page size must be one of "
                  "4K/8K/16K/32K/64K", pHeader->_pageSize ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else if ( DMS_DO_NOT_CREATE_LOB != pHeader->_lobdPageSize &&
                DMS_PAGE_SIZE4K != pHeader->_lobdPageSize &&
                DMS_PAGE_SIZE8K != pHeader->_lobdPageSize &&
                DMS_PAGE_SIZE16K != pHeader->_lobdPageSize &&
                DMS_PAGE_SIZE32K != pHeader->_lobdPageSize &&
                DMS_PAGE_SIZE64K != pHeader->_lobdPageSize &&
                DMS_PAGE_SIZE128K != pHeader->_lobdPageSize &&
                DMS_PAGE_SIZE256K != pHeader->_lobdPageSize &&
                DMS_PAGE_SIZE512K != pHeader->_lobdPageSize )
      {
         PD_LOG ( PDERROR, "Invalid lob page size: %d in file[%s], lob page "
                  "size must be one of 4K/8K/16K/32K/64K/128K/256K/512K",
                  pHeader->_lobdPageSize, getSuFileName() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      // set storage info page size, lob meta page size is 256B,
      // so can't be assign to storage info
      if ( (UINT32)_pStorageInfo->_pageSize != pHeader->_pageSize )
      {
         _pStorageInfo->_pageSize = pHeader->_pageSize ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _dmsStorageBase::_validateHeader( dmsStorageUnitHeader * pHeader )
   {
      INT32 rc = SDB_OK ;

      // check eye catcher
      if ( 0 != ossStrncmp ( pHeader->_eyeCatcher, _getEyeCatcher(),
                             DMS_HEADER_EYECATCHER_LEN ) )
      {
         CHAR szTmp[ DMS_HEADER_EYECATCHER_LEN + 1 ] = {0} ;
         ossStrncpy( szTmp, pHeader->_eyeCatcher, DMS_HEADER_EYECATCHER_LEN ) ;
         PD_LOG ( PDERROR, "Invalid eye catcher: %s", szTmp ) ;
         rc = SDB_INVALID_FILE_TYPE ;
         goto error ;
      }

      // check version
      rc = _checkVersion( pHeader ) ;
      if ( rc )
      {
         goto error ;
      }
      // check page size
      rc = _checkPageSize( pHeader ) ;
      if ( rc )
      {
         goto error ;
      }
      _pageSize = pHeader->_pageSize ;
      _lobPageSize = pHeader->_lobdPageSize ;

      if ( 0 != _dataOffset() % pHeader->_pageSize )
      {
         rc = SDB_SYS ;
         PD_LOG( PDSEVERE, "Dms storage meta size[%llu] is not a mutiple of "
                 "pagesize[%u]", _dataOffset(), pHeader->_pageSize ) ;
      }
      else if ( DMS_MAX_PG < pHeader->_pageNum )
      {
         PD_LOG ( PDERROR, "Invalid storage unit page number: %u",
                  pHeader->_pageNum ) ;
         rc = SDB_SYS ;
      }
      else if ( pHeader->_storageUnitSize - pHeader->_pageNum !=
                _dataOffset() / pHeader->_pageSize )
      {
         PD_LOG( PDERROR, "Invalid storage unit size: %u",
                 pHeader->_storageUnitSize ) ;
         rc = SDB_SYS ;
      }
      else if ( 0 != ossStrncmp ( _pStorageInfo->_suName, pHeader->_name,
                                  DMS_SU_NAME_SZ ) )
      {
         PD_LOG ( PDERROR, "Invalid storage unit name: %s", pHeader->_name ) ;
         rc = SDB_SYS ;
      }

      if ( rc )
      {
         goto error ;
      }

      if ( !ossIsPowerOf2( pHeader->_pageSize, &_pageSizeSquare ) )
      {
         PD_LOG( PDERROR, "Page size must be the power of 2" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( _pStorageInfo->_secretValue != pHeader->_secretValue )
      {
         _pStorageInfo->_secretValue = pHeader->_secretValue ;
      }
      if ( _pStorageInfo->_sequence != pHeader->_sequence )
      {
         _pStorageInfo->_sequence = pHeader->_sequence ;
      }
      if ( (UINT32)_pStorageInfo->_lobdPageSize != pHeader->_lobdPageSize )
      {
         _pStorageInfo->_lobdPageSize =  pHeader->_lobdPageSize ;   
      }
      _pageNum = pHeader->_pageNum ;
      _segmentPages = _getSegmentSize() >> _pageSizeSquare ;

      if ( !ossIsPowerOf2( _segmentPages, &_segmentPagesSquare ) )
      {
         PD_LOG( PDERROR, "Segment pages[%u] must be the power of 2",
                 _segmentPages ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      PD_LOG ( PDDEBUG, "Validated storage unit file %s\n"
               "page size: %d\ndata size: %d pages\nname: %s\nsequence: %d",
               getSuFileName(), pHeader->_pageSize, pHeader->_pageNum,
               pHeader->_name, pHeader->_sequence ) ;

   done :
      return rc ;
   error:
      goto done ;
   }

   INT32 _dmsStorageBase::_preExtendSegment ()
   {
      INT32 rc = _extendSegments( 1 ) ;
      // release lock
      ossUnlatch( &_segmentLatch, EXCLUSIVE ) ;

      if ( rc )
      {
         PD_LOG( PDERROR, "Pre-extend segment failed, rc: %d", rc ) ;
      }
      return rc ;
   }

   INT32 _dmsStorageBase::_extendSegments( UINT32 numSeg )
   {
      INT32 rc = SDB_OK ;
      INT64 fileSize = 0 ;

      // now other normal applications still able to access metadata in
      // read-only mode
      // Then we'll check if adding new segments will exceed the limit
      UINT32 beginExtentID = _dmsHeader->_pageNum ;
      UINT32 endExtentID   = beginExtentID + _segmentPages * numSeg ;

      if ( endExtentID > DMS_MAX_PG )
      {
         PD_LOG( PDERROR, "Extent page[%u] exceed max pages[%u] in su[%s]",
                 endExtentID, DMS_MAX_PG, _suFileName ) ;
         rc = SDB_DMS_NOSPC ;
         goto error ;
      }

      // We'll also verify the SME shows DMS_SME_FREE for all needed pages
      for ( UINT32 i = beginExtentID; i < endExtentID; i++ )
      {
         if ( DMS_SME_FREE != _dmsSME->getBitMask( i ) )
         {
            rc = SDB_DMS_CORRUPTED_SME ;
            goto error ;
         }
      }

      // get file size for map or rollback
      rc = ossGetFileSize ( &_file, &fileSize ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to get file size, rc = %d", rc ) ;

      // check wether the file length is match storage unit pages
      if ( fileSize != (INT64)_dmsHeader->_storageUnitSize * pageSize() )
      {
         PD_LOG( PDWARNING, "File[%s] size[%llu] is not match with storage "
                 "unit pages[%u]", _suFileName, fileSize,
                 _dmsHeader->_storageUnitSize ) ;

         fileSize = (UINT64)_dmsHeader->_storageUnitSize * pageSize() ;
         rc = ossTruncateFile( &_file, fileSize ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Truncate file[%s] to size[%lld] failed, rc: %d",
                    _suFileName, fileSize, rc ) ;
            goto error ;
         }
         PD_LOG( PDEVENT, "Truncate file[%s] to size[%lld]", _suFileName,
                 fileSize ) ;
      }

      // now we only hold extendsegment latch, no other sessions can extend
      // but other sessions can freely create new extents in existing segments
      // This should be safe because no one knows we are increasing the size of
      // file, so other sessions will not attempt to access the new space
      // then we need to increase the size of file first
      // MAKE SURE NOT HOLD ANY METADATA LATCH DURING SUCH EXPENSIVE DISK 
      // OPERATION extendSeg latch is held here so that it's not possible //
      // two sessions doing same extend
      rc = ossExtendFile( &_file, _getSegmentSize() * numSeg ) ;
      if ( rc )
      {
         INT32 rc1 = SDB_OK ;
         PD_LOG ( PDERROR, "Failed to extend storage unit for %lld bytes",
                  _getSegmentSize() * (UINT64)numSeg ) ;

         // truncate the file when it's failed to extend file
         rc1 = ossTruncateFile ( &_file, fileSize ) ;
         if ( rc1 )
         {
            PD_LOG ( PDSEVERE, "Failed to revert the increase of segment, "
                     "rc = %d", rc1 ) ;
            // if we increased the file size but got error, and we are not able
            // to decrease it, something BIG wrong, let's panic
            ossPanic () ;
         }
         // we need to manage how to truncate the file to original size here
         goto error ;
      }

      // map all new segments into memory
      for ( UINT32 i = 0; i < numSeg ; i++ )
      {
         rc = map ( fileSize, _getSegmentSize(), NULL ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to map storage unit from offset %lld",
                     _getSegmentSize() * i + _dmsHeader->_storageUnitSize ) ;
            goto error ;
         }
         _maxSegID += 1 ;

         // update SME Manager
         rc = _smeMgr.depositASegment( (dmsExtentID)beginExtentID ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to deposit new segment into SMEMgr, "
                     "rc = %d", rc ) ;
            ossPanic() ;
            goto error ;
         }
         beginExtentID += _segmentPages ;
         fileSize += _getSegmentSize() ;

         // update header
         _dmsHeader->_storageUnitSize += _segmentPages ;
         _dmsHeader->_pageNum += _segmentPages ;
         _pageNum = _dmsHeader->_pageNum ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

   UINT32 _dmsStorageBase::_extendThreshold () const
   {
      return (UINT32)( DMS_EXTEND_THRESHOLD_SIZE >> _pageSizeSquare ) ;
   }

   UINT32 _dmsStorageBase::_getSegmentSize() const
   {
      return DMS_SEGMENT_SZ ;
   }

   INT32 _dmsStorageBase::_findFreeSpace( UINT16 numPages, SINT32 & foundPage,
                                          dmsContext *context )
   {
      UINT32 segmentSize = 0 ;
      INT32 rc = SDB_OK ;
      INT32 rc1 = SDB_OK ;

      while ( TRUE )
      {
         rc = _smeMgr.reservePages( numPages, foundPage, &segmentSize ) ;
         if ( rc )
         {
            goto error ;
         }

         if ( DMS_INVALID_EXTENT != foundPage )
         {
            break ;
         }

         // if not able to find any, that means all pages are occupied
         // then we should call extendSegments
         if ( ossTestAndLatch( &_segmentLatch, EXCLUSIVE ) )
         {
            if ( segmentSize != _smeMgr.segmentNum() )
            {
               ossUnlatch( &_segmentLatch, EXCLUSIVE ) ;
               continue ;
            }

            // begin for extent
            rc = context ? context->pause() : SDB_OK ;
            if ( rc )
            {
               ossUnlatch( &_segmentLatch, EXCLUSIVE ) ;
               PD_LOG( PDERROR, "Failed to pause context[%s], rc: %d",
                       context->toString().c_str(), rc ) ;
               goto error ;
            }

            rc = _extendSegments( 1 ) ;

            // end to resume
            rc1 = context ? context->resume() : SDB_OK ;

            ossUnlatch( &_segmentLatch, EXCLUSIVE ) ;

            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to extend storage unit, rc=%d", rc );
               goto error ;
            }
            PD_RC_CHECK( rc1, PDERROR, "Failed to resume context[%s], rc: %d",
                         context->toString().c_str(), rc1 ) ;

            PD_LOG ( PDDEBUG, "Successfully extend storage unit for %d pages",
                     numPages ) ;
         }
         else
         {
            // begin for extent
            rc = context ? context->pause() : SDB_OK ;
            PD_RC_CHECK( rc, PDERROR, "Failed to pause context[%s], rc: %d",
                         context->toString().c_str(), rc ) ;
            ossLatch( &_segmentLatch, SHARED ) ;
            ossUnlatch( &_segmentLatch, SHARED );
            // end to resume
            rc = context ? context->resume() : SDB_OK ;
            PD_RC_CHECK( rc, PDERROR, "Failed to resum context[%s], rc: %d",
                         context->toString().c_str(), rc ) ;
         }
      }

      // start extend segment job
      if ( _extendThreshold() > 0 &&
           _smeMgr.totalFree() < _extendThreshold() &&
           ossTestAndLatch( &_segmentLatch, EXCLUSIVE ) )
      {
         if ( _smeMgr.totalFree() >= _extendThreshold() ||
              SDB_OK != startExtendSegmentJob( NULL, this ) )
         {
            ossUnlatch( &_segmentLatch, EXCLUSIVE ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _dmsStorageBase::_releaseSpace( SINT32 pageStart, UINT16 numPages )
   {
      return _smeMgr.releasePages( pageStart, numPages ) ;
   }

   UINT32 _dmsStorageBase::_totalFreeSpace ()
   {
      return _smeMgr.totalFree() ;
   }

   // flush all dirty segments to disk by traverse _dirtyList array
   // this function returns void since it doesn't affect frontend workload
   // regardless whether the flush success or not
   void _dmsStorageBase::flushDirtySegments ( UINT32 *pNum )
   {
      INT32 rc = SDB_OK ;
      INT32 maxSegmentID = 0 ;
      UINT32 numbers = 0 ;
      // once we latch the storage unit, we have to check if the header is null.
      // If the header is null, that means closeStorage is called and we should
      // get out of the function
      if ( !_dmsHeader )
         goto done ;
      SDB_ASSERT ( _dataSegID && _dirtyList,
                   "starting data segment can't be 0, and "
                   "dirty list can't be NULL" ) ;
      SDB_ASSERT ( (UINT32)_maxSegID <=
                   maxSegmentNum() + _dataSegID,
                   "current top segment id can't be greater than "
                   "maximum number of segment for storage unit" ) ;
      // calculate how many "segment groups" we should go through
      // note each group is consists of 8 segments
      maxSegmentID = ceil (( _maxSegID + 1 - _dataSegID ) / 8.0f ) ;
      // always flush header and metadata
      for ( UINT32 i = 0; i < _dataSegID; ++i )
      {
         // we should check the header before every phyical flush, to make sure
         // the storage unit is still open at the time
         if ( !_dmsHeader )
            goto done ;
         rc = flush ( i, TRUE ) ;
         if ( rc )
         {
            PD_LOG ( PDWARNING,
                     "Failed to flush segment %d to disk, rc = %d",
                     i, rc ) ;
         }
      }

      // then flush data with dirty pages
      for ( INT32 i = 0; i < maxSegmentID; ++i )
      {
         // we should check the header before every phyical flush,
         // to make sure the storage unit is still open at the time
         if ( !_dmsHeader )
            goto done ;
         // each byte represents 8 segments, let's check each byte first
         if ( _dirtyList[i] != 0 )
         {
            // if the byte is not 0, let's see which page need to be flushed
            for ( INT32 j = 0; j < 8; ++j )
            {
               // if the segment on j's bit is dirty, let's flush
               if ( OSS_BIT_TEST ( _dirtyList[i], ( 1 << j ) ) )
               {
                  rc = flush ( _dataSegID + (i<<3) + j, TRUE ) ;
                  if ( rc )
                  {
                     PD_LOG ( PDWARNING,
                              "Failed to flush segment %d to disk, rc = %d",
                              i + _dataSegID, rc ) ;
                  }
                  // now let's convert the bit back to 0
                  OSS_BIT_CLEAR ( _dirtyList[i], ( 1 << j ) ) ;
                  ++numbers ;
               } // if ( _dirtyList[i] & ( 1 << j ) )
            } // for ( INT32 j = 0; j < 8; ++j )
         } // if ( _dirtyList[i] != 0 )
      } // for ( INT32 i = 0; i < maxSegmentNum; ++i )
   done :
      if ( pNum )
      {
         *pNum = numbers ;
      }
      return ;
   }

   /*
      DMS TOOL FUNCTIONS:
   */
   BOOLEAN dmsAccessAndFlagCompatiblity ( UINT16 collectionFlag,
                                          DMS_ACCESS_TYPE accessType )
   {
      // if we are in crash recovery mode, only recovery thread is able to
      // perform query, in this case we always return TRUE
      if ( !pmdGetStartup().isOK() )
      {
         return TRUE ;
      }
      else if ( DMS_IS_MB_FREE(collectionFlag) ||
                DMS_IS_MB_DROPPED(collectionFlag) )
      {
         return FALSE ;
      }
      else if ( DMS_IS_MB_NORMAL(collectionFlag) )
      {
         return TRUE ;
      }
      else if ( DMS_IS_MB_OFFLINE_REORG(collectionFlag) )
      {
         if ( DMS_IS_MB_OFFLINE_REORG_TRUNCATE(collectionFlag) &&
            ( accessType == DMS_ACCESS_TYPE_TRUNCATE ) )
         {
            return TRUE ;
         }
         else if ( DMS_IS_MB_OFFLINE_REORG_SHADOW_COPY ( collectionFlag ) &&
                  ( ( accessType == DMS_ACCESS_TYPE_QUERY ) ||
                    ( accessType == DMS_ACCESS_TYPE_FETCH ) ) )
         {
            return TRUE ;
         }
         return FALSE ;
      }
      else if ( DMS_IS_MB_ONLINE_REORG(collectionFlag) )
      {
         return TRUE ;
      }
      else if ( DMS_IS_MB_LOAD ( collectionFlag ) &&
                DMS_ACCESS_TYPE_TRUNCATE != accessType )
      {
         return TRUE ;
      }
      return FALSE ;
   }

}


