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

   Source File Name = dmsStorageLob.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          17/07/2014  YW Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsStorageLob.hpp"
#include "dpsOp2Record.hpp"
#include "pmd.hpp"
#include "dpsTransCB.hpp"
#include "dmsTrace.hpp"
#include "pdTrace.hpp"

namespace engine
{
   #define DMS_LOB_META( extent )\
      ( ( _dmsLobDataMapBlk * )extent )

   #define DMS_LOBM_EYECATCHER               "SDBLOBM"
   #define DMS_LOBM_EYECATCHER_LEN           8

   #define DMS_LOB_EXTEND_THRESHOLD_SIZE     ( 65536 )   // 64K

   #define DMS_LOB_PAGE_IN_USED( page )\
           ( DMS_SME_ALLOCATED == getSME()->getBitMask( page ) )

   #define DMS_LOB_GET_HASH_FROM_BLK( blk, hash )\
           do\
           {\
              const BYTE *d1 = (blk)->_oid ;\
              const BYTE *d2 = ( const BYTE * )( &( (blk)->_sequence ) ) ;\
              (hash) = ossHash( d1, sizeof( (blk)->_oid ),\
                              d2, sizeof( (blk)->_sequence ) ) ;\
           } while( FALSE )

   /*
      _dmsStorageLob implement
   */
   _dmsStorageLob::_dmsStorageLob( const CHAR *lobmFileName,
                                   const CHAR *lobdFileName,
                                   dmsStorageInfo *info,
                                   dmsStorageData *pDataSu )
   :_dmsStorageBase( lobmFileName, info ),
    _dmsBME( NULL ),
    _segmentSize( 0 ),
    _dmsData( pDataSu ),
    _data( lobdFileName )
   {
      ossMemset( _path, 0, sizeof( _path ) ) ;
      _needDelayOpen = FALSE ;

      _dmsData->_attachLob( this ) ;
   }

   _dmsStorageLob::~_dmsStorageLob()
   {
      _dmsData->_detachLob() ;
      _dmsData = NULL ;
      _dmsBME = NULL ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_OPEN, "_dmsStorageLob::open" )
   INT32 _dmsStorageLob::open( const CHAR *path,
                               BOOLEAN createNew,
                               BOOLEAN rmWhenExist )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_OPEN ) ;

      ossStrncpy( _path, path, OSS_MAX_PATHSIZE ) ;

      if ( 0 == _dmsData->getHeader()->_createLobs )
      {
         _needDelayOpen = TRUE ;
      }
      else
      {
         rc = _openLob( path, createNew, rmWhenExist ) ;
      }

      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_OPEN, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__DELAYOPEN, "_dmsStorageLob::_delayOpen" )
   INT32 _dmsStorageLob::_delayOpen()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__DELAYOPEN ) ;

      _delayOpenLatch.get() ;

      if ( !_needDelayOpen )
      {
         goto done ;
      }

      if ( isOpened() )
      {
         goto done ;
      }

      rc = _openLob( _path, TRUE, TRUE ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Delay open[%s] failed, rc: %d",
                 getSuName(), rc ) ;
         goto error ;
      }

      _needDelayOpen = FALSE ;

      _dmsData->updateCreateLobs( 1 ) ;

   done:
      _delayOpenLatch.release() ;
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__DELAYOPEN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__OPENLOB, "_dmsStorageLob::_openLob" )
   INT32 _dmsStorageLob::_openLob( const CHAR *path,
                                   BOOLEAN createNew,
                                   BOOLEAN rmWhenExist )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__OPENLOB ) ;
      rc = openStorage( path, createNew, rmWhenExist ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lobm file:%s, rc:%d",
                 _suFileName, rc ) ;
         if ( createNew && SDB_FE != rc )
         {
            goto rmlobm ;
         }
         goto error ;
      }

      rc = _data.open( path, createNew, rmWhenExist,
                       *_pStorageInfo, pmdGetThreadEDUCB() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lobd file:%s, rc:%d",
                 _data.getFileName().c_str(), rc ) ;
         if ( createNew )
         {
            if ( SDB_FE != rc )
            {
               goto rmboth ;
            }
            goto rmlobm ;
         }
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__OPENLOB, rc ) ;
      return rc ;
   error:
      _data.close() ;
      close() ;
      goto done ;
   rmlobm:
      removeStorage() ;
      goto error ;
   rmboth:
      removeStorageFiles() ;
      goto error ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_REMOVESTORAGEFILES, "_dmsStorageLob::removeStorageFiles" )
   void _dmsStorageLob::removeStorageFiles()
   {
      INT32 rc = removeStorage() ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_REMOVESTORAGEFILES ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to remove file:%d", rc ) ;
      }

      rc = _data.remove() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to remove file:%d", rc ) ;
      }

      PD_TRACE_EXIT( SDB__DMSSTORAGELOB_REMOVESTORAGEFILES ) ;
      return ;
   }

   BOOLEAN _dmsStorageLob::isOpened() const
   {
      return _data.isOpened() ;
   }

    // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_GETLOBMETA, "_dmsStorageLob::getLobMeta" )
   INT32 _dmsStorageLob::getLobMeta( const bson::OID &oid,
                                     dmsMBContext *mbContext,
                                     pmdEDUCB *cb,
                                     _dmsLobMeta &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_GETLOBMETA ) ;
      UINT32 readSz = 0 ;
      dmsLobRecord piece ;
      piece.set( &oid, DMS_LOB_META_SEQUENCE, 0,
                 sizeof( meta ), NULL ) ;
      rc = read( piece, mbContext, cb, 
                 ( CHAR * )( &meta ), readSz ) ;
      if ( SDB_OK == rc )
      {
         if ( sizeof( meta ) != readSz )
         {
            PD_LOG( PDERROR, "read length is %d, big error!", readSz ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         goto done ;
      }
      else if ( SDB_LOB_SEQUENCE_NOT_EXIST == rc )
      {
         rc = SDB_FNE ;
         goto error ;
      }
      else
      {
         PD_LOG( PDERROR, "failed to read meta of lob, rc:%d",
                 rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_GETLOBMETA, rc ) ;
      return rc ;
   error:
      meta.clear() ;
      goto done ;
   }

    // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_WRITELOBMETA, "_dmsStorageLob::writeLobMeta" )
   INT32 _dmsStorageLob::writeLobMeta( const bson::OID &oid,
                                       dmsMBContext *mbContext,
                                       pmdEDUCB *cb,
                                       const _dmsLobMeta &meta,
                                       BOOLEAN isNew,
                                       SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_WRITELOBMETA ) ;
      dmsLobRecord piece ;
      piece.set( &oid, DMS_LOB_META_SEQUENCE, 0,
                 sizeof( meta ), ( const CHAR * )( &meta ) ) ;
      if ( isNew )
      {
         rc = write( piece, mbContext, cb, dpsCB ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
            goto error ;
         }
      }
      else
      {
         rc = update( piece, mbContext, cb, dpsCB ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to update lob:%d", rc ) ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_WRITELOBMETA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_WRITE, "_dmsStorageLob::write" )
   INT32 _dmsStorageLob::write( const dmsLobRecord &record,
                                dmsMBContext *mbContext,
                                pmdEDUCB *cb,
                                SDB_DPSCB *dpscb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_WRITE ) ;
      SDB_ASSERT( NULL != mbContext && NULL != cb, "can not be null" ) ;
      SDB_ASSERT( 0 == record._offset, "must be zero" ) ;
      DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
      dpsMergeInfo info ;
      dpsLogRecord &logRecord = info.getMergeBlock().record() ;
      dpsTransCB *transCB = pmdGetKRCB()->getTransCB() ;
      DPS_TRANS_ID transID = DPS_INVALID_TRANS_ID ;
      DPS_LSN_OFFSET preTransLsn = DPS_INVALID_LSN_OFFSET ;
      DPS_LSN_OFFSET relatedLsn = DPS_INVALID_LSN_OFFSET ;
      CHAR fullName[DMS_SU_FILENAME_SZ + DMS_COLLECTION_NAME_SZ + 2] ;
      BOOLEAN locked = mbContext->isMBLock() ;

      if ( _needDelayOpen )
      {
         rc = _delayOpen() ;
         PD_RC_CHECK( rc, PDERROR, "Delay open failed in write, rc: %d", rc ) ;
      }

      if ( NULL != dpscb )
      {
         UINT32 csNameLen = ossStrlen( getSuName() ) ;
         UINT32 clNameLen = ossStrlen( mbContext->mb()->_collectionName ) ;
         ossMemcpy( fullName, getSuName(), csNameLen ) ;
         fullName[csNameLen] = '.' ;
         ossMemcpy( fullName + csNameLen + 1,
                    mbContext->mb()->_collectionName,
                    clNameLen + 1 ) ;  /// +1 for '\0'
         
         rc = dpsLobW2Record( fullName,
                              record._oid,
                              record._sequence,
                              record._offset,
                              record._hash,
                              record._dataLen,
                              record._data,
                              page,
                              transID,
                              preTransLsn,
                              relatedLsn,
                              logRecord ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build dps log:%d", rc ) ;
            goto error ;
         }

         rc = dpscb->checkSyncControl( logRecord.head()._length, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "check sync control failed, rc: %d", rc ) ;
            goto error ;
         }

         rc = transCB->reservedLogSpace( logRecord.head()._length ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "failed to reserved log space(length=%u)",
                    logRecord.head()._length ) ;
            info.clear() ;
            goto error ;
         }
      }

      if ( !locked )
      {
         rc = mbContext->mbLock( EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;
      }

      if ( !isOpened() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "File[%s] is not open in write", getSuName() ) ;
         goto error ;
      }

      if ( !dmsAccessAndFlagCompatiblity ( mbContext->mb()->_flag,
                                           DMS_ACCESS_TYPE_INSERT ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  mbContext->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      rc = _allocatePage( record, mbContext, page ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to allocate page in cl:%s, rc:%d",
                 _suFileName, rc ) ;
         goto error ;
      }

      rc = _data.write( page, record._data, record._dataLen,
                        record._offset, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write data to cl:%s, rc:%d",
                 _suFileName, rc ) ;
         goto error ;
      }

      if ( NULL != dpscb )
      {
         SDB_ASSERT( NULL != _dmsData, "can not be null" ) ;
         info.setInfoEx( _dmsData->logicalID(), mbContext->clLID(), page, cb ) ;
         rc = dpscb->prepare( info ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to prepare dps log:%d", rc ) ;
            goto error ;
         }
      }

      rc = _fillPage( record, page, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to fill page:%d", rc ) ;
         goto error ;
      }

      if ( NULL != dpscb )
      {
         if ( !locked )
         {
            mbContext->mbUnlock() ;
            locked = TRUE ;
         }

         dpscb->writeData( info ) ;
      }
   done:
      if ( !locked )
      {
         mbContext->mbUnlock() ;
      }

      if ( 0 != logRecord.head()._length )
      {
         transCB->releaseLogSpace( logRecord.head()._length ) ;
      }

      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_WRITE, rc ) ;
      return rc ;
   error:
      if ( DMS_LOB_INVALID_PAGEID != page )
      {
         _releasePage( page ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_UPDATE, "_dmsStorageLob::update" )
   INT32 _dmsStorageLob::update( const dmsLobRecord &record,
                                 dmsMBContext *mbContext,
                                 pmdEDUCB *cb,
                                 SDB_DPSCB *dpscb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_UPDATE ) ;
      SDB_ASSERT( NULL != mbContext && NULL != cb, "can not be null" ) ;
      SDB_ASSERT( 0 == record._offset, "must be zero" ) ;
      SDB_ASSERT( _dmsHeader->_lobdPageSize <= DMS_PAGE_SIZE512K,
                  "can not over 512 KB" ) ;
      SDB_ASSERT( record._offset + record._dataLen <= _dmsHeader->_lobdPageSize,
                  "impossible" ) ;
      DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
      _dmsLobDataMapBlk *blk = NULL ;
      dpsMergeInfo info ;
      dpsLogRecord &logRecord = info.getMergeBlock().record() ;
      DPS_TRANS_ID transID = DPS_INVALID_TRANS_ID ;
      DPS_LSN_OFFSET preTransLsn = DPS_INVALID_LSN_OFFSET ;
      DPS_LSN_OFFSET relatedLsn = DPS_INVALID_LSN_OFFSET ;
      dpsTransCB *transCB = pmdGetKRCB()->getTransCB() ;
      CHAR oldData[DMS_PAGE_SIZE512K] ;
      UINT32 oldLen = 0 ;
      CHAR fullName[DMS_SU_FILENAME_SZ + DMS_COLLECTION_NAME_SZ + 2] ;
      BOOLEAN locked = mbContext->isMBLock() ;

      if ( _needDelayOpen )
      {
         rc = _delayOpen() ;
         PD_RC_CHECK( rc, PDERROR, "Delay open failed in update, rc: %d", rc ) ;
      }

      if ( !locked )
      {
         rc = mbContext->mbLock( EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;
      }

      if ( !isOpened() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "File[%s] is not open in update", getSuName() ) ;
         goto error ;
      }

      if ( !dmsAccessAndFlagCompatiblity ( mbContext->mb()->_flag,
                                           DMS_ACCESS_TYPE_INSERT ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  mbContext->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      rc = _find( record, mbContext->clLID(), page, blk ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to find piece:%d", rc ) ;
         goto error ;
      }

      if ( DMS_LOB_INVALID_PAGEID == page )
      {
         PD_LOG( PDERROR, "can not find piece[%s], sequence[%d]",
                 record._oid->str().c_str(), record._sequence ) ;
         rc = SDB_LOB_SEQUENCE_NOT_EXIST ;
         goto error ;
      }

      if ( NULL != dpscb )
      {
         UINT32 csNameLen = ossStrlen( getSuName() ) ;
         UINT32 clNameLen = ossStrlen( mbContext->mb()->_collectionName ) ;
         ossMemcpy( fullName, getSuName(), csNameLen ) ;
         fullName[csNameLen] = '.' ;
         ossMemcpy( fullName + csNameLen + 1,
                    mbContext->mb()->_collectionName,
                    clNameLen + 1 ) ; /// +1 for '\0'

         rc = _data.read( page, blk->_dataLen, 0, cb, oldData, oldLen ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to read data from file:%d", rc ) ;
            goto error ;
         }

         SDB_ASSERT( oldLen == blk->_dataLen, "impossible" ) ;

         rc = dpsLobU2Record( fullName,
                              record._oid,
                              record._sequence,
                              record._offset,
                              record._hash,
                              record._dataLen,
                              record._data,
                              oldLen,
                              oldData,
                              page,
                              transID,
                              preTransLsn,
                              relatedLsn,
                              logRecord ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build dps log:%d", rc ) ;
            goto error ;
         }

         rc = dpscb->checkSyncControl( logRecord.head()._length, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "check sync control failed, rc: %d", rc ) ;
            goto error ;
         }

         rc = transCB->reservedLogSpace( logRecord.head()._length ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "failed to reserved log space(length=%u)",
                    logRecord.head()._length ) ;
            info.clear() ;
            goto error ;
         }
      }

      rc = _data.write( page, record._data, record._dataLen,
                        record._offset, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write data to cl:%s, rc:%d",
                 _suFileName, rc ) ;
         goto error ;
      }

      blk->_dataLen = record._dataLen ;

      if ( NULL != dpscb )
      {
         SDB_ASSERT( NULL != _dmsData, "can not be null" ) ;
         info.setInfoEx( _dmsData->logicalID(), mbContext->clLID(), page, cb ) ;
         rc = dpscb->prepare( info ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to prepare dps log:%d", rc ) ;
            goto error ;
         }

         if ( !locked )
         {
            mbContext->mbUnlock() ;
            locked = TRUE ;
         }

         dpscb->writeData( info ) ;
      }
   done:
      if ( !locked )
      {
         mbContext->mbUnlock() ;
      }

      if ( 0 != logRecord.head()._length )
      {
         transCB->releaseLogSpace( logRecord.head()._length ) ;
      }
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_UPDATE, rc ) ;
      return rc ;
   error:
      if ( DMS_LOB_INVALID_PAGEID != page )
      {
         _releasePage( page ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_READ, "_dmsStorageLob::read" )
   INT32 _dmsStorageLob::read( const dmsLobRecord &record,
                               dmsMBContext *mbContext,
                               pmdEDUCB *cb,
                               CHAR *buf,
                               UINT32 &readLen )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_READ ) ;
      DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
      _dmsLobDataMapBlk *blk = NULL ;
      BOOLEAN locked = mbContext->isMBLock() ;

      if ( _needDelayOpen )
      {
         rc = _delayOpen() ;
         PD_RC_CHECK( rc, PDERROR, "Delay open failed in read, rc: %d", rc ) ;
      }

      if ( !locked )
      {
         rc = mbContext->mbLock( SHARED ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;
      }

      if ( !isOpened() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "File[%s] is not open in read", getSuName() ) ;
         goto error ;
      }

      rc = _find( record, mbContext->clLID(), page, blk ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to find page of oid:%s, sequence:%d, rc:%d",
                 record._oid->str().c_str(), record._sequence, rc ) ;
         goto error ;              
      }

      if ( DMS_LOB_INVALID_PAGEID == page )
      {
         rc = SDB_LOB_SEQUENCE_NOT_EXIST ;
         goto error ;
      }

      rc = _data.read( page, record._dataLen, record._offset,
                       cb, buf, readLen ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read data from file:%d", rc ) ;
         goto error ;
      }
   done:
      if ( !locked )
      {
         mbContext->mbUnlock() ;
      }
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_READ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__ALLOCATEPAGE, "_dmsStorageLob::_allocatePage" )
   INT32 _dmsStorageLob::_allocatePage( const dmsLobRecord &record,
                                        dmsMBContext *context,
                                        DMS_LOB_PAGEID &page )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__ALLOCATEPAGE ) ;
      SDB_ASSERT( NULL != record._oid && 0 <= record._sequence &&
                  record._dataLen <= getLobdPageSize() &&
                  0 == record._offset, "invalid lob record" ) ;

      rc = _findFreeSpace( 1, page, context ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to find free space:%d", rc ) ;
         goto error ;
      }
      context->mbStat()->_totalLobPages += 1 ;

   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__ALLOCATEPAGE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__FILLPAGE, "_dmsStorageLob::_fillPage" )
   INT32 _dmsStorageLob::_fillPage( const dmsLobRecord &record,
                                    DMS_LOB_PAGEID page,
                                    dmsMBContext *context )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__FILLPAGE ) ;
      _dmsLobDataMapBlk *blk = NULL ;

      ossValuePtr extent = extentAddr( page ) ;
      if ( !extent )
      {
         PD_LOG( PDERROR, "we got a NULL extent from extendAddr(), pageid:%d",
                 page ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      blk = DMS_LOB_META( extent ) ;
      ossMemcpy( blk->_oid, record._oid, DMS_LOB_OID_LEN ) ;
      blk->_sequence = record._sequence ;
      blk->_dataLen = record._dataLen ;
      blk->_clLogicalID = context->clLID() ;
      blk->_mbID = context->mbID() ;

      rc = _push2Bucket( _getBucket( record._hash ),
                         page, *blk ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to push page[%d] to bucket[%d]",
                 page, _getBucket( record._hash ) ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__FILLPAGE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_REMOVE, "_dmsStorageLob::remove" )
   INT32 _dmsStorageLob::remove( const dmsLobRecord &record,
                                 dmsMBContext *mbContext,
                                 pmdEDUCB *cb,
                                 SDB_DPSCB *dpscb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_REMOVE ) ;
      UINT32 bucketNumber = 0 ;
      _dmsLobDataMapBlk *blk = NULL ;
      DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
      dpsMergeInfo info ;
      dpsLogRecord &logRecord = info.getMergeBlock().record() ;
      DPS_TRANS_ID transID = DPS_INVALID_TRANS_ID ;
      DPS_LSN_OFFSET preTransLsn = DPS_INVALID_LSN_OFFSET ;
      DPS_LSN_OFFSET relatedLsn = DPS_INVALID_LSN_OFFSET ;
      dpsTransCB *transCB = pmdGetKRCB()->getTransCB() ;
      CHAR oldData[DMS_PAGE_SIZE512K] ;
      UINT32 oldLen = 0 ;
      CHAR fullName[DMS_SU_FILENAME_SZ + DMS_COLLECTION_NAME_SZ + 2] ;
      BOOLEAN locked = mbContext->isMBLock() ;

      if ( _needDelayOpen )
      {
         rc = _delayOpen() ;
         PD_RC_CHECK( rc, PDERROR, "Delay open failed in remove, rc: %d", rc ) ;
      }

      if ( !locked )
      {
         rc = mbContext->mbLock( EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;
      }

      if ( !isOpened() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "File[%s] is not open in remove", getSuName() ) ;
         goto error ;
      }

      if ( !dmsAccessAndFlagCompatiblity ( mbContext->mb()->_flag,
                                           DMS_ACCESS_TYPE_DELETE ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  mbContext->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      rc = _find( record, mbContext->clLID(), page, blk, &bucketNumber ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to find oid[%s] sequence[%d], rc:%d",
                 record._oid->str().c_str(), record._sequence, rc ) ;
         goto error ;
      }

      if ( DMS_LOB_INVALID_PAGEID == page )
      {
         goto done ;
      }

      if ( NULL != dpscb )
      {
         UINT32 csNameLen = ossStrlen( getSuName() ) ;
         UINT32 clNameLen = ossStrlen( mbContext->mb()->_collectionName ) ;
         ossMemcpy( fullName, getSuName(), csNameLen ) ;
         fullName[csNameLen] = '.' ;
         ossMemcpy( fullName + csNameLen + 1,
                    mbContext->mb()->_collectionName,
                    clNameLen + 1 ) ;

         rc = _data.read( page, blk->_dataLen, 0,
                          cb, oldData, oldLen ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to read data from file:%d", rc ) ;
            goto error ;
         }

         SDB_ASSERT( oldLen == blk->_dataLen, "impossible" ) ;

         rc = dpsLobRm2Record( fullName,
                               record._oid,
                               record._sequence,
                               0,
                               record._hash,
                               oldLen,
                               oldData,
                               page,
                               transID,
                               preTransLsn,
                               relatedLsn,
                               logRecord ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build dps log:%d", rc ) ;
            goto error ;
         }

         rc = dpscb->checkSyncControl( logRecord.head()._length, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "check sync control failed, rc: %d", rc ) ;
            goto error ;
         }

         rc = transCB->reservedLogSpace( logRecord.head()._length ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "failed to reserved log space(length=%u)",
                    logRecord.head()._length ) ;
            info.clear() ;
            goto error ;
         }
      }

      rc = _removePage( page, blk, &bucketNumber ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to remove page:%d, rc:%d", page, rc ) ;
         goto error ;
      }

      if ( NULL != dpscb )
      {
         SDB_ASSERT( NULL != _dmsData, "can not be null" ) ;
         info.setInfoEx( _dmsData->logicalID(), mbContext->clLID(), page, cb ) ;
         rc = dpscb->prepare( info ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to prepare dps log:%d", rc ) ;
            goto error ;
         }

         if ( !locked )
         {
            mbContext->mbUnlock() ;
            locked = TRUE ;
         }

         dpscb->writeData( info ) ;
      }
   done:
      if ( !locked )
      {
         mbContext->mbUnlock() ;
      }

      if ( 0 != logRecord.head()._length )
      {
         transCB->releaseLogSpace( logRecord.head()._length ) ;
      }
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_REMOVE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _dmsStorageLob::_releasePage( DMS_LOB_PAGEID page )
   {
      return _releaseSpace( page, 1 ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__FIND, "_dmsStorageLob::_find" )
   INT32 _dmsStorageLob::_find( const _dmsLobRecord &record,
                                UINT32 clID,
                                DMS_LOB_PAGEID &page,
                                _dmsLobDataMapBlk *&lobBlk,
                                UINT32 *bucket )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__FIND ) ;
      UINT32 bucketNumber = _getBucket( record._hash ) ;
      DMS_LOB_PAGEID pageInBucket = _dmsBME->_buckets[bucketNumber] ;
      while ( DMS_LOB_INVALID_PAGEID != pageInBucket )
      {
         _dmsLobDataMapBlk *blk = NULL ;
         ossValuePtr extent = extentAddr( pageInBucket ) ;
         if ( !extent )
         {
            PD_LOG( PDERROR, "we got a NULL extent from extendAddr(), "
                    "pageid:%d", pageInBucket ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         blk = DMS_LOB_META( extent ) ;
         if ( clID == blk->_clLogicalID &&
              blk->equals( record._oid->getData(), record._sequence ) )
         {
            page = pageInBucket ;
            lobBlk = blk ;
            break ;
         }
         else
         {
            pageInBucket = blk->_nextPageInBucket ;
            continue ; 
         }
      }

      if ( DMS_LOB_INVALID_PAGEID != pageInBucket && NULL != bucket )
      {
         *bucket = bucketNumber ;
      }
   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__FIND, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__PUSH2BUCKET, "_dmsStorageLob::_push2Bucket" )
   INT32 _dmsStorageLob::_push2Bucket( UINT32 bucket,
                                       DMS_LOB_PAGEID pageId,
                                       _dmsLobDataMapBlk &blk )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__PUSH2BUCKET ) ;
      DMS_LOB_PAGEID &pageInBucket = _dmsBME->_buckets[bucket] ;

      if ( DMS_LOB_INVALID_PAGEID == pageInBucket )
      {
         pageInBucket = pageId ;
         blk._prevPageInBucket = DMS_LOB_INVALID_PAGEID ;
         blk._nextPageInBucket = DMS_LOB_INVALID_PAGEID ;
      }
      else
      {
         DMS_LOB_PAGEID tmpPage = pageInBucket ;
         _dmsLobDataMapBlk *lastBlk = NULL ;
         ossValuePtr extent = 0 ;
         do
         {
            extent = extentAddr( tmpPage ) ;
            if ( !extent )
            {
               PD_LOG( PDERROR, "we got a NULL extent from extendAddr(), pageid:%d",
                       tmpPage ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            lastBlk = DMS_LOB_META( extent ) ;
            if ( DMS_LOB_INVALID_PAGEID == lastBlk->_nextPageInBucket )
            {
               lastBlk->_nextPageInBucket = pageId ;
               blk._prevPageInBucket = tmpPage ;
               blk._nextPageInBucket = DMS_LOB_INVALID_PAGEID ;
               break ;
            }
            else
            {
               tmpPage = lastBlk->_nextPageInBucket ;
               continue ;
            }
         } while ( TRUE ) ;
      }
   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__PUSH2BUCKET, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__ONCREATE, "_dmsStorageLob::_onCreate" )
   INT32 _dmsStorageLob::_onCreate( OSSFILE *file, UINT64 curOffSet )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__ONCREATE ) ;
      SDB_ASSERT( DMS_BME_OFFSET == curOffSet, "invalid offset" ) ;

      _dmsBucketsManagementExtent *bme =
                   SDB_OSS_NEW _dmsBucketsManagementExtent() ;
      if ( NULL == bme )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OK ;
         goto error ;
      }

      rc = _writeFile ( file, (const CHAR *)(bme),
                        sizeof( _dmsBucketsManagementExtent ) ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to write new bme to file rc: %d",
                  rc ) ;
         goto error ;
      }
   done:
      if ( NULL != bme )
      {
         SDB_OSS_DEL bme ;
         bme = NULL ;
      }
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__ONCREATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__ONMAPMETA, "_dmsStorageLob::_onMapMeta" )
   INT32 _dmsStorageLob::_onMapMeta( UINT64 curOffSet )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__ONMAPMETA ) ;
      rc = map ( DMS_BME_OFFSET, DMS_BME_SZ, (void**)&_dmsBME ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to map BME: %s", getSuFileName() ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__ONMAPMETA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   UINT32 _dmsStorageLob::_getSegmentSize() const
   {
      SDB_ASSERT( 0 != _segmentSize, "not initialized" ) ;
      return _segmentSize ;
   }

   UINT32 _dmsStorageLob::_extendThreshold() const
   {
      return (UINT32)( DMS_LOB_EXTEND_THRESHOLD_SIZE >> pageSizeSquareRoot() ) ;
   }

   UINT64 _dmsStorageLob::_dataOffset()
   {
      return DMS_BME_OFFSET + DMS_BME_SZ ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__EXTENDSEGMENTS, "_dmsStorageLob::_extendSegments" )
   INT32 _dmsStorageLob::_extendSegments( UINT32 numSeg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__EXTENDSEGMENTS ) ;
      INT64 extentLen = _data.getSegmentSize() ;
      rc = _data.extend( extentLen * numSeg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extend lobd file:%d", rc ) ;
         goto error ;
      }

      rc = this->_dmsStorageBase::_extendSegments( numSeg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extend lobm file:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__EXTENDSEGMENTS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   const CHAR* _dmsStorageLob::_getEyeCatcher() const
   {
      return DMS_LOBM_EYECATCHER ;
   }

   UINT32 _dmsStorageLob::_curVersion() const
   {
      return DMS_LOB_CUR_VERSION ;
   }

   INT32 _dmsStorageLob::_checkVersion( dmsStorageUnitHeader *pHeader )
   {
      INT32 rc = SDB_OK ;
      if ( pHeader->_version > _curVersion() )
      {
         PD_LOG( PDERROR, "Incompatible version: %u", pHeader->_version ) ;
         rc = SDB_DMS_INCOMPATIBLE_VERSION ;
      }
      else if ( pHeader->_secretValue != _pStorageInfo->_secretValue )
      {
         PD_LOG( PDERROR, "Secret value[%llu] not the same with data su[%llu]",
                 pHeader->_secretValue, _pStorageInfo->_secretValue ) ;
         rc = SDB_DMS_SECRETVALUE_NOT_SAME ;
      }
      return rc ;
   }

   void _dmsStorageLob::_onClosed()
   {
      _data.close() ;
      _dmsBME = NULL ;
      return ;
   }

   void _dmsStorageLob::_initHeaderPageSize( dmsStorageUnitHeader * pHeader,
                                             dmsStorageInfo * pInfo )
   {
      pHeader->_pageSize = DMS_PAGE_SIZE256B ;
      pHeader->_lobdPageSize = pInfo->_lobdPageSize ;
   }

   INT32 _dmsStorageLob::_checkPageSize( dmsStorageUnitHeader * pHeader )
   {
      INT32 rc = SDB_OK ;

      if ( pHeader->_pageSize != DMS_PAGE_SIZE256B )
      {
         PD_LOG( PDERROR, "Lob meta page size[%d] must be %d in file[%s]",
                 pHeader->_pageSize, DMS_PAGE_SIZE256B, getSuFileName() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      else if ( DMS_PAGE_SIZE4K != pHeader->_lobdPageSize &&
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

      _segmentSize = _data.getSegmentSize() / pHeader->_lobdPageSize *
                     DMS_LOB_DATA_MAP_BLK_LEN ;

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_READPAGE, "_dmsStorageLob::_readPage" )
   INT32 _dmsStorageLob::readPage( DMS_LOB_PAGEID &pos,
                                   BOOLEAN onlyMetaPage,
                                   _pmdEDUCB *cb,
                                   dmsMBContext *mbContext,
                                   dmsLobInfoOnPage &page )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_READPAGE ) ;
      DMS_LOB_PAGEID current = pos ;
      BOOLEAN locked = mbContext->isMBLock() ;

      if ( DMS_LOB_INVALID_PAGEID == current )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( _needDelayOpen )
      {
         rc = _delayOpen() ;
         PD_RC_CHECK( rc, PDERROR, "Delay open failed in read, rc: %d", rc ) ;
      }

      if ( !locked )
      {
         rc = mbContext->mbLock( SHARED ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get lock:%d", rc ) ;
            goto error ;
         }
      }

      if ( !isOpened() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "File[%s] is not open in read", getSuName() ) ;
         goto error ;
      }

      do
      {
         if ( pageNum() <= ( UINT32 )current )
         {
            rc = SDB_DMS_EOC ;
            goto error ;
         }
         else if ( DMS_LOB_PAGE_IN_USED( current ) )
         {
            _dmsLobDataMapBlk *blk = NULL ;
            ossValuePtr extent = extentAddr( current ) ;
            if ( !extent )
            {
               PD_LOG( PDERROR, "we got a NULL extent from extendAddr(), pageid:%d",
                       current ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            blk = DMS_LOB_META( extent ) ;
            if ( mbContext->clLID() != blk->_clLogicalID ||
                 ( onlyMetaPage && DMS_LOB_META_SEQUENCE != blk->_sequence ) )
            {
               ++current ;
               continue ;
            }

            ossMemcpy( &( page._oid ), blk->_oid, sizeof( page._oid ) ) ;
            page._sequence = blk->_sequence ;
            page._len = blk->_dataLen ;
            ++current ;
            break ;
         }
         else
         {
            ++current ;
         }
      } while ( TRUE ) ;

      pos = current ;
   done:
      if ( mbContext->isMBLock() )
      {
         mbContext->mbUnlock() ;
      }
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_READPAGE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB__REMOVEPAGE, "_dmsStorageLob::_removePage" )
   INT32 _dmsStorageLob::_removePage( DMS_LOB_PAGEID page,
                                      const _dmsLobDataMapBlk *blk,
                                      const UINT32 *bucket )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB__REMOVEPAGE ) ;
      UINT32 bucketNumber = 0 ;     

      if ( NULL != bucket )
      {
         bucketNumber = *bucket ;
      }
      else
      {
         UINT32 hash = 0 ;
         DMS_LOB_GET_HASH_FROM_BLK( blk, hash ) ;
         bucketNumber = _getBucket( hash ) ;
      }

      if ( DMS_LOB_INVALID_PAGEID == blk->_prevPageInBucket )
      {
         SDB_ASSERT( _dmsBME->_buckets[bucketNumber] == page,
                     "must be this page" ) ;
         _dmsBME->_buckets[bucketNumber] = blk->_nextPageInBucket ;
         if ( DMS_LOB_INVALID_PAGEID != blk->_nextPageInBucket )
         {
            _dmsLobDataMapBlk *nextBlk = NULL ;
            ossValuePtr nextExtent = extentAddr( blk->_nextPageInBucket ) ;
            if ( !nextExtent )
            {
               PD_LOG( PDERROR, "we got a NULL extent from extendAddr(), "
                       "pageid:%d", blk->_nextPageInBucket ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            nextBlk = DMS_LOB_META( nextExtent ) ;
            nextBlk->_prevPageInBucket = DMS_LOB_INVALID_PAGEID ;
         }
      }
      else
      {
         _dmsLobDataMapBlk *lastBlk = NULL ;
         ossValuePtr lastExtent = extentAddr( blk->_prevPageInBucket ) ;
         if ( !lastExtent )
         {
            PD_LOG( PDERROR, "we got a NULL extent from extendAddr(), pageid:%d",
                    blk->_prevPageInBucket ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         lastBlk = DMS_LOB_META( lastExtent ) ;
         lastBlk->_nextPageInBucket = blk->_nextPageInBucket ;

         if ( DMS_LOB_INVALID_PAGEID != blk->_nextPageInBucket )
         {
            _dmsLobDataMapBlk *nextBlk = NULL ;
            ossValuePtr nextExtent = extentAddr( blk->_nextPageInBucket ) ;
            if ( !nextExtent )
            {
               PD_LOG( PDERROR, "we got a NULL extent from extendAddr(), pageid:%d",
                       blk->_nextPageInBucket ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            nextBlk = DMS_LOB_META( nextExtent ) ;
            nextBlk->_prevPageInBucket = blk->_prevPageInBucket ;
         }
      }

      _dmsData->_mbStatInfo[ blk->_mbID ]._totalLobPages -= 1 ;
      _releasePage( page ) ;
   done:
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB__REMOVEPAGE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOB_TRUNCATE, "_dmsStorageLob::truncate" )
   INT32 _dmsStorageLob::truncate( dmsMBContext *mbContext,
                                   _pmdEDUCB *cb,
                                   SDB_DPSCB *dpscb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__DMSSTORAGELOB_TRUNCATE ) ;
      DMS_LOB_PAGEID current = -1 ;
      BOOLEAN locked = FALSE ;
      BOOLEAN needPanic = FALSE ;
      CHAR fullName[DMS_SU_FILENAME_SZ + DMS_COLLECTION_NAME_SZ + 2] ;
      dpsMergeInfo info ;
      dpsLogRecord &logRecord = info.getMergeBlock().record() ;
      dpsTransCB *transCB = pmdGetKRCB()->getTransCB() ;

      if ( !isOpened() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "File[%s] is not open in remove", getSuName() ) ;
         goto error ;
      }

      locked = mbContext->isMBLock() ;
      if ( !locked )
      {
         rc = mbContext->mbLock( EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;
      }

      if ( !dmsAccessAndFlagCompatiblity ( mbContext->mb()->_flag,
                                           DMS_ACCESS_TYPE_DELETE ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  mbContext->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      if ( NULL != dpscb )
      {
         UINT32 csNameLen = ossStrlen( getSuName() ) ;
         UINT32 clNameLen = ossStrlen( mbContext->mb()->_collectionName ) ;
         ossMemcpy( fullName, getSuName(), csNameLen ) ;
         fullName[csNameLen] = '.' ;
         ossMemcpy( fullName + csNameLen + 1,
                    mbContext->mb()->_collectionName,
                    clNameLen + 1 ) ;

         rc = dpsLobTruncate2Record( fullName,
                                     logRecord ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build dps log:%d", rc ) ;
            goto error ;
         }

         rc = dpscb->checkSyncControl( logRecord.head()._length, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "check sync control failed, rc: %d", rc ) ;
            goto error ;
         }

         rc = transCB->reservedLogSpace( logRecord.head()._length ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "failed to reserved log space(length=%u)",
                    logRecord.head()._length ) ;
            info.clear() ;
            goto error ;
         }
      }

      needPanic = TRUE ;
      while ( ( UINT32 )++current < pageNum() )
      {
         if ( !DMS_LOB_PAGE_IN_USED( current ) )
         {
            continue ;
         }

         _dmsLobDataMapBlk *blk = NULL ;
         ossValuePtr extent = extentAddr( current ) ;
         if ( !extent )
         {
            PD_LOG( PDERROR, "we got a NULL extent from extendAddr(), pageid:%d",
                    current ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         blk = DMS_LOB_META( extent ) ;
         if ( mbContext->clLID() != blk->_clLogicalID )
         {
            continue ; 
         }

         rc = _removePage( current, blk, NULL ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to remove page:%d, rc:%d", rc ) ;
            goto error ;
         }
      }

      mbContext->mbStat()->_totalLobPages = 0 ;

      if ( NULL != dpscb )
      {
         SDB_ASSERT( NULL != _dmsData, "can not be null" ) ;
         info.setInfoEx( _dmsData->logicalID(),
                         mbContext->clLID(),
                         DMS_INVALID_EXTENT, cb ) ;

         rc = dpscb->prepare( info ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to prepare dps log:%d", rc ) ;
            goto error ;
         }

         if ( !locked )
         {
            mbContext->mbUnlock() ;
            locked = TRUE ;
         }

         dpscb->writeData( info ) ;
      }

   done:
      if ( !locked )
      {
         mbContext->mbUnlock() ;
      }

      if ( 0 != logRecord.head()._length )
      {
         transCB->releaseLogSpace( logRecord.head()._length ) ;
      }
      PD_TRACE_EXITRC( SDB__DMSSTORAGELOB_TRUNCATE, rc ) ;
      return rc ;
   error:
      if ( needPanic )
      {
         PD_LOG( PDSEVERE, "we must panic db now, we got a lrreparable "
                 "error" ) ;
         ossPanic() ;
      }
      goto done ;
   }
}

