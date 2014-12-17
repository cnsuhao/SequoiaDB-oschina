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

   Source File Name = dmsStorageLoadExtent.cpp

   Descriptive Name =

   When/how to use: load extent to database

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/10/2013  JW  Initial Draft

   Last Changed =

******************************************************************************/

#include "dms.hpp"
#include "dmsExtent.hpp"
#include "dmsSMEMgr.hpp"
#include "dmsStorageLoadExtent.hpp"
#include "dmsCompress.hpp"
#include "pdTrace.hpp"
#include "dmsTrace.hpp"
#include "migLoad.hpp"

namespace engine
{
   void dmsStorageLoadOp::_initExtentHeader ( dmsExtent *extAddr,
                                              UINT16 numPages )
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

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOADEXT__ALLOCEXTENT, "dmsStorageLoadOp::_allocateExtent" )
   INT32 dmsStorageLoadOp::_allocateExtent ( INT32 requestSize )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGELOADEXT__ALLOCEXTENT );
      if ( requestSize < DMS_MIN_EXTENT_SZ(_pageSize) )
      {
         requestSize = DMS_MIN_EXTENT_SZ(_pageSize) ;
      }
      else if ( requestSize > DMS_MAX_EXTENT_SZ )
      {
         requestSize = DMS_MAX_EXTENT_SZ ;
      }
      else
      {
         requestSize = ossRoundUpToMultipleX ( requestSize, _pageSize ) ;
      }

      if ( !_pCurrentExtent )
      {
         _pCurrentExtent = (CHAR*)SDB_OSS_MALLOC ( requestSize ) ;
         if ( !_pCurrentExtent )
         {
            PD_LOG ( PDERROR, "Unable to allocate %d bytes memory",
                     requestSize ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         _currentExtentSize = requestSize ;
         _initExtentHeader ( (dmsExtent*)_pCurrentExtent,
                             _currentExtentSize/_pageSize ) ;
      }
      else
      {
         if ( requestSize > _currentExtentSize )
         {
            CHAR *pOldPtr = _pCurrentExtent ;
            _pCurrentExtent = (CHAR*)SDB_OSS_REALLOC ( _pCurrentExtent,
                                                       requestSize ) ;
            if ( !_pCurrentExtent )
            {
               PD_LOG ( PDERROR, "Unable to realloc %d bytes memory",
                        requestSize ) ;
               _pCurrentExtent = pOldPtr ;
               rc = SDB_OOM ;
               goto error ;
            }
            _currentExtentSize = requestSize ;
         }
         _initExtentHeader ( (dmsExtent*)_pCurrentExtent,
                             _currentExtentSize/_pageSize ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGELOADEXT__ALLOCEXTENT, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOADEXT__IMPRTBLOCK, "dmsStorageLoadOp::pushToTempDataBlock" )
   INT32 dmsStorageLoadOp::pushToTempDataBlock ( dmsMBContext *mbContext,
                                                 BSONObj &record,
                                                 BOOLEAN isLast,
                                                 BOOLEAN isAsynchr )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGELOADEXT__IMPRTBLOCK );
      UINT32      dmsrecordSize   = 0 ;
      ossValuePtr recordPtr       = 0 ;
      ossValuePtr prevPtr         = 0 ;
      dmsOffset   offset          = DMS_INVALID_OFFSET ;
      dmsOffset   recordOffset    = DMS_INVALID_OFFSET ;
      BSONElement ele ;
      _IDToInsert oid ;
      idToInsertEle oidEle((CHAR*)(&oid)) ;
      BOOLEAN addOID = FALSE ;
      INT32 oidLen = 0 ;

      SDB_ASSERT( mbContext, "mb context can't be NULL" ) ;

      /* (0) */
      ele = record.getField ( DMS_ID_KEY_NAME ) ;
      if ( ele.type() == Array )
      {
         PD_LOG ( PDERROR, "record id can't be array: %s",
                  record.toString().c_str()) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( ele.eoo() )
      {
         oid._oid.init() ;
         oidLen += oidEle.size() ;
         addOID = TRUE ;
      }

      if ( ( dmsrecordSize = 
             (record.objsize() + DMS_RECORD_METADATA_SZ + oidLen) )
             > DMS_RECORD_MAX_SZ )
      {
         rc = SDB_CORRUPTED_RECORD ;
         goto error ;
      }

      dmsrecordSize *= DMS_RECORD_OVERFLOW_RATIO ;
      dmsrecordSize = OSS_MIN(DMS_RECORD_MAX_SZ, ossAlignX(dmsrecordSize,4)) ;
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
         _currentExtent = (dmsExtent*)_pCurrentExtent ;
      }

      if ( dmsrecordSize > (UINT32)_currentExtent->_freeSpace || isLast )
      {
         rc = mbContext->mbLock( EXCLUSIVE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to lock collection, rc=%d", rc ) ;
            goto error ;
         }
         if ( !isAsynchr )
         {
            _currentExtent->_firstRecordOffset = DMS_INVALID_OFFSET ;
            _currentExtent->_lastRecordOffset = DMS_INVALID_OFFSET ;
         }

         rc = _su->loadExtentA( mbContext, _pCurrentExtent,
                                _currentExtentSize / _pageSize,
                                TRUE ) ;
         mbContext->mbUnlock() ;

         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to load extent, rc = %d", rc ) ;
            goto error ;
         }

         if ( isLast )
         {
            goto done ;
         }

         rc = _allocateExtent ( dmsrecordSize <<
                                DMS_RECORDS_PER_EXTENT_SQUARE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to allocate new extent in reorg file, "
                     "rc = %d", rc ) ;
            goto error ;
         }
      }
      recordOffset = _currentExtentSize - _currentExtent->_freeSpace ;
      recordPtr = ((ossValuePtr)_currentExtent) + recordOffset ;
      if ( _currentExtent->_freeSpace - (INT32)dmsrecordSize <
           (INT32)DMS_MIN_RECORD_SZ &&
           _currentExtent->_freeSpace <= (INT32)DMS_RECORD_MAX_SZ )
      {
         dmsrecordSize = _currentExtent->_freeSpace ;
      }

      DMS_RECORD_SETFLAG ( recordPtr, DMS_RECORD_FLAG_NORMAL ) ;
      DMS_RECORD_SETMYOFFSET ( recordPtr, recordOffset ) ;
      DMS_RECORD_SETSIZE ( recordPtr, dmsrecordSize ) ;
      if ( !addOID )
      {
         DMS_RECORD_SETDATA ( recordPtr, record.objdata(), record.objsize() ) ;
      }
      else
      {
         DMS_RECORD_SETDATA_OID ( recordPtr,
                                  record.objdata(),
                                  record.objsize(),
                                  oidEle ) ;
      }
      DMS_RECORD_SETNEXTOFFSET ( recordPtr, DMS_INVALID_OFFSET ) ;
      DMS_RECORD_SETPREVOFFSET ( recordPtr, DMS_INVALID_OFFSET ) ;
      if ( isAsynchr )
      {
         _currentExtent->_recCount++ ;
      }
      _currentExtent->_freeSpace -= dmsrecordSize ;
      offset = _currentExtent->_lastRecordOffset ;
      if ( DMS_INVALID_OFFSET != offset )
      {
         prevPtr = ((ossValuePtr)_currentExtent) + offset ;
         DMS_RECORD_SETNEXTOFFSET ( prevPtr, recordOffset ) ;
         DMS_RECORD_SETPREVOFFSET ( recordPtr, offset ) ;
      }

      _currentExtent->_lastRecordOffset = recordOffset ;

      offset = _currentExtent->_firstRecordOffset ;
      if ( DMS_INVALID_OFFSET == offset )
      {
         _currentExtent->_firstRecordOffset = recordOffset ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__DMSSTORAGELOADEXT__IMPRTBLOCK, rc );
      return rc ;
   error:
      goto done ;
   }


   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOADEXT__LDDATA, "dmsStorageLoadOp::loadBuildPhase" )
   INT32 dmsStorageLoadOp::loadBuildPhase ( dmsMBContext *mbContext,
                                            pmdEDUCB *cb,
                                            BOOLEAN isAsynchr,
                                            migMaster *pMaster,
                                            UINT32 *success,
                                            UINT32 *failure )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGELOADEXT__LDDATA ) ;

      dmsExtent     *extAddr        = NULL ;
      dmsRecordID    recordID ;
      ossValuePtr    recordPtr      = 0 ;
      ossValuePtr    recordDataPtr  = 0 ;
      ossValuePtr    extentPtr      = 0 ;
      dmsOffset      recordOffset   = DMS_INVALID_OFFSET ;
      dmsExtentID    tempExtentID   = 0 ;
      monAppCB * pMonAppCB          = cb ? cb->getMonAppCB() : NULL ;

      SDB_ASSERT ( _su, "_su can't be NULL" ) ;
      SDB_ASSERT ( mbContext, "dms mb context can't be NULL" ) ;
      SDB_ASSERT ( cb, "cb is NULL" ) ;

      rc = mbContext->mbLock( EXCLUSIVE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to lock collection, rc: %d", rc ) ;

      if ( DMS_INVALID_EXTENT == mbContext->mb()->_loadFirstExtentID &&
           DMS_INVALID_EXTENT == mbContext->mb()->_loadLastExtentID )
      {
         PD_LOG ( PDEVENT, "has not load extent" ) ;
         goto done ;
      }

      if ( ( DMS_INVALID_EXTENT == mbContext->mb()->_lastExtentID &&
             DMS_INVALID_EXTENT != mbContext->mb()->_firstExtentID ) ||
           ( DMS_INVALID_EXTENT != mbContext->mb()->_lastExtentID &&
             DMS_INVALID_EXTENT == mbContext->mb()->_firstExtentID ) )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR, "Invalid mb context[%s], first extent: %d, last "
                  "extent: %d", mbContext->toString().c_str(),
                  mbContext->mb()->_firstExtentID,
                  mbContext->mb()->_lastExtentID ) ;
         goto error ;
      }

      clearFlagLoadLoad ( mbContext->mb() ) ;
      setFlagLoadBuild ( mbContext->mb() ) ;

      while ( !cb->isForced() )
      {
         rc = mbContext->mbLock( EXCLUSIVE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "dms mb context lock failed, rc", rc ) ;
            goto error ;
         }

         tempExtentID = mbContext->mb()->_loadFirstExtentID ;
         if ( DMS_INVALID_EXTENT == tempExtentID )
         {
            mbContext->mb()->_loadFirstExtentID = DMS_INVALID_EXTENT ;
            mbContext->mb()->_loadLastExtentID = DMS_INVALID_EXTENT ;
            goto done ;
         }

         extentPtr = _su->data()->extentAddr( tempExtentID ) ;
         extAddr = (dmsExtent *)extentPtr ;
         PD_CHECK ( extAddr, SDB_SYS, error, PDERROR, "Invalid extent: %d",
                    tempExtentID ) ;
         SDB_ASSERT( extAddr->validate( mbContext->mbID() ),
                     "Invalid extent" ) ;

         rc = _su->data()->addExtent2Meta( tempExtentID, extAddr, mbContext ) ;
         PD_RC_CHECK( rc, PDERROR, "Add extent to meta failed, rc: %d", rc ) ;

         mbContext->mb()->_loadFirstExtentID = extAddr->_nextExtent ;

         extAddr->_firstRecordOffset = DMS_INVALID_OFFSET ;
         extAddr->_lastRecordOffset  = DMS_INVALID_OFFSET ;
         _su->addExtentRecordCount( mbContext->mb(), extAddr->_recCount ) ;
         extAddr->_recCount          = 0 ;
         _su->mapExtent2DelList( mbContext->mb(), extAddr, tempExtentID ) ;

         recordOffset = DMS_EXTENT_METADATA_SZ ;
         recordID._extent = tempExtentID ;

         while ( DMS_INVALID_OFFSET != recordOffset )
         {
            recordPtr = extentPtr + recordOffset ;
            recordID._offset = recordOffset ;
            DMS_RECORD_EXTRACTDATA(recordPtr, recordDataPtr) ;
            recordOffset = DMS_RECORD_GETNEXTOFFSET(recordPtr) ;
            ++( extAddr->_recCount ) ;

            try
            {
               BSONObj obj ( (const CHAR*)recordDataPtr ) ;
               DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_WRITE, 1 ) ;

               rc = _su->index()->indexesInsert( mbContext, tempExtentID, obj,
                                                 recordID, cb ) ;
               if ( rc )
               {
                  if ( SDB_IXM_DUP_KEY != rc )
                  {
                     PD_LOG ( PDERROR, "Failed to insert into index, rc=%d",
                              rc ) ;
                     goto rollback ;
                  }
                  if ( success )
                  {
                     --(*success) ;
                  }
                  if ( failure )
                  {
                     ++(*failure) ;
                  }
                  if ( pMaster )
                  {
                     rc = pMaster->sendMsgToClient( "Error: index insert, error"
                                                    "code %d, %s", rc,
                                                    obj.toString().c_str() ) ;
                     if ( rc )
                     {
                        PD_LOG ( PDERROR, "Failed to send msg, rc=%d", rc ) ;
                     }
                  }
                  rc = _su->data()->deleteRecord ( mbContext, recordID,
                                                   recordDataPtr,
                                                   cb, NULL ) ;
                  if ( rc )
                  {
                     PD_LOG ( PDERROR, "Failed to rollback, rc = %d", rc ) ;
                     goto rollback ;
                  }
               }
            }
            catch ( std::exception &e )
            {
               PD_LOG ( PDERROR, "Failed to create BSON object: %s",
                        e.what() ) ;
               rc = SDB_SYS ;
               goto rollback ;
            }

            if ( DMS_INVALID_OFFSET == extAddr->_firstRecordOffset )
            {
               extAddr->_firstRecordOffset = recordID._offset ;
            }
            extAddr->_lastRecordOffset = recordID._offset ;
         } //while ( DMS_INVALID_OFFSET != recordOffset )

         mbContext->mbUnlock() ;
      } // while

   done:
      PD_TRACE_EXITRC ( SDB__DMSSTORAGELOADEXT__LDDATA, rc );
      return rc ;
   error:
      goto done ;
   rollback:
      recordOffset = recordID._offset ;
      while ( DMS_INVALID_OFFSET != recordOffset )
      {
         recordPtr = extentPtr + recordOffset ;
         recordID._offset = recordOffset ;
         recordOffset = DMS_RECORD_GETNEXTOFFSET(recordPtr) ;

         _su->extentRemoveRecord( mbContext->mb(), recordID, 0, cb ) ;

         if ( DMS_INVALID_OFFSET != recordOffset )
         {
            ++( extAddr->_recCount ) ;
         }
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGELOADEXT__ROLLEXTENT, "dmsStorageLoadOp::loadRollbackPhase" )
   INT32 dmsStorageLoadOp::loadRollbackPhase( dmsMBContext *mbContext )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGELOADEXT__ROLLEXTENT ) ;

      SDB_ASSERT ( _su, "_su can't be NULL" ) ;
      SDB_ASSERT ( mbContext, "mb context can't be NULL" ) ;

      PD_LOG ( PDEVENT, "Start loadRollbackPhase" ) ;

      rc = _su->data()->truncateCollectionLoads( NULL, mbContext ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to truncate collection loads, rc: %d",
                   rc ) ;

   done:
      PD_LOG ( PDEVENT, "End loadRollbackPhase" ) ;
      PD_TRACE_EXITRC ( SDB__DMSSTORAGELOADEXT__ROLLEXTENT, rc );
      return rc ;
   error:
      goto done ;
   }

}

