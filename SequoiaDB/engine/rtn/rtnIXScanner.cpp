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

   Source File Name = rtnIXScanner.cpp

   Descriptive Name = Runtime Index Scanner

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains code for index traversal,
   including advance, pause, resume operations.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "rtnIXScanner.hpp"
#include "dmsStorageUnit.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace engine
{

   _rtnIXScanner::_rtnIXScanner ( ixmIndexCB *indexCB,
                                  rtnPredicateList *predList,
                                  dmsStorageUnit *su, pmdEDUCB *cb )
   : _predList(predList),
     _listIterator(*predList),
     _order(Ordering::make(indexCB->keyPattern())),
     _su(su),
     _pMonCtxCB(NULL),
     _cb(cb),
     _direction(predList->getDirection())
   {
      SDB_ASSERT ( indexCB && _predList && _su,
                   "indexCB, predList and su can't be NULL" ) ;

      _dedupBufferSize = RTN_IXSCANNER_DEDUPBUFSZ_DFT ;
      indexCB->getIndexID ( _indexOID ) ;
      _indexCBExtent = indexCB->getExtentID () ;
      _indexLID = indexCB->getLogicalID() ;
      _indexCB = SDB_OSS_NEW ixmIndexCB ( _indexCBExtent, su->index(),
                                          NULL ) ;
      reset() ;
   }

   _rtnIXScanner::~_rtnIXScanner ()
   {
      if ( _indexCB )
      {
         SDB_OSS_DEL _indexCB ;
      }
   }

   // change the scanner's current location to a given key and rid
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNIXSCAN_RELORID1, "_rtnIXScanner::relocateRID" )
   INT32 _rtnIXScanner::relocateRID ( const BSONObj &keyObj,
                                      const dmsRecordID &rid )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNIXSCAN_RELORID1 ) ;

      PD_CHECK ( _indexCB, SDB_OOM, error, PDERROR,
                 "Failed to allocate memory for indexCB" ) ;
      // sanity check, make sure we are on valid index
      PD_CHECK ( _indexCB->isInitialized(), SDB_RTN_INDEX_NOTEXIST, error,
                 PDERROR, "Index does not exist" ) ;
      PD_CHECK ( _indexCB->getFlag() == IXM_INDEX_FLAG_NORMAL,
                 SDB_IXM_UNEXPECTED_STATUS, error, PDERROR,
                 "Unexpected index status: %d", _indexCB->getFlag() ) ;
      {
         monAppCB * pMonAppCB   = _cb ? _cb->getMonAppCB() : NULL ;
         // get root
         dmsExtentID rootExtent = _indexCB->getRoot() ;
         ixmExtent root ( rootExtent, _su->index() ) ;
         BOOLEAN found          = FALSE ;
         // locate the new key, the returned RID is stored in _curIndexRID
         rc = root.locate ( keyObj, rid, _order, _curIndexRID, found,
                            _direction, _indexCB ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to locate from new keyobj and rid: %s, %d,%d",
                       keyObj.toString().c_str(), rid._extent,
                       rid._offset ) ;
         _savedObj = keyObj.copy() ;
         _savedRID = rid ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_INDEX_READ, 1 ) ;
         DMS_MON_CONTEXT_COUNT_INC ( _pMonCtxCB, MON_INDEX_READ, 1 ) ;
      }
      // mark _init to true so that advance won't call keyLocate again
      _init = TRUE ;

   done :
      PD_TRACE_EXITRC ( SDB__RTNIXSCAN_RELORID1, rc ) ;
      return rc ;
   error :
      goto done ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNIXSCAN_RELORID2, "_rtnIXScanner::relocateRID" )
   INT32 _rtnIXScanner::relocateRID ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNIXSCAN_RELORID2 ) ;
      monAppCB * pMonAppCB = _cb ? _cb->getMonAppCB() : NULL ;
      // either the key doesn't exist, or we got key/rid not match,
      // or the key is psuedo-deleted, we all get here
      dmsExtentID rootExtent = _indexCB->getRoot() ;
      ixmExtent root ( rootExtent, _su->index() ) ;
      BOOLEAN found ;
      rc = root.locate ( _savedObj, _savedRID, _order, _curIndexRID, found,
                         _direction, _indexCB ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to locate from saved obj and rid: %s, %d,%d",
                  _savedObj.toString().c_str(), _savedRID._extent,
                  _savedRID._offset ) ;
         goto error ;
      }
      DMS_MON_OP_COUNT_INC( pMonAppCB, MON_INDEX_READ, 1 ) ;
      DMS_MON_CONTEXT_COUNT_INC ( _pMonCtxCB, MON_INDEX_READ, 1 ) ;
   done :
      PD_TRACE_EXITRC ( SDB__RTNIXSCAN_RELORID2, rc ) ;
      return rc ;
   error :
      goto done ;
   }
   // return SDB_IXM_EOC for end of index
   // return SDB_IXM_DEDUP_BUF_MAX if hit max dedup buffer
   // otherwise rid is set to dmsRecordID
   // advance() must be called between resumeScan() and pauseScan()
   // caller must make sure the table is locked before calling resumeScan, and
   // must release the table lock right after pauseScan
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNIXSCAN_ADVANCE, "_rtnIXScanner::advance" )
   INT32 _rtnIXScanner::advance ( dmsRecordID &rid, BOOLEAN isReadOnly )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNIXSCAN_ADVANCE ) ;
      monAppCB * pMonAppCB = _cb ? _cb->getMonAppCB() : NULL ;

   begin:
      SDB_ASSERT ( _indexCB, "_indexCB can't be NULL, call resumeScan first" ) ;
      // first time run after reset, we need to locate the first key
      if ( !_init )
      {
         // when we get here, we should always hold lock on the collection, so
         // _indexCB should remain valid until pauseScan() and resumeScan(). So
         // in resumeScan() we should always validate if the index still the
         // same
         dmsExtentID rootExtent = _indexCB->getRoot() ;
         ixmExtent root ( rootExtent, _su->index() ) ;
         rc = root.keyLocate ( _curIndexRID, BSONObj(),0,FALSE,
                               _listIterator.cmp(), _listIterator.inc(),
                               _order, _direction, _cb ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "failed to locate first key, rc: %d", rc ) ;
            goto error ;
         }
         _init = TRUE ;
      }
      // otherwise _curIndexRID is pointing to the previous scanned location,
      // we should start from there
      else
      {
         // make sure the current _curIndexRID is not NULL
         if ( _curIndexRID.isNull() )
         {
            // if doing goto here, compile will get error for the following
            // indexExtent object declare
            return SDB_IXM_EOC ;
         }
         ixmExtent indexExtent ( _curIndexRID._extent, _su->index() ) ;
         // in readonly mode, _savedRID should always be NULL unless pauseScan()
         // + resumeScan() found the index structure is changed and we have
         // relocateRID(), in that case _savedRID may not be NULL for readonly
         // mode

         // in write mode, the possible NULL savedRID is that when the
         // previous read is a psuedo-delete, note resumeScan() shouldn't set
         // _savedRID to NULL in this case

         // In such scenario, we advance to next element
         if ( _savedRID.isNull() )
         {
            // changed during the time
            rc = indexExtent.advance ( _curIndexRID, _direction ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to advance to next key, rc: %d", rc ) ;
               goto error ;
            }
         }
         // need to specially deal with the situation that index tree structure
         // may changed during scan
         // in this case _savedRID can't be NULL
         // advance happened only when both _savedRID and _savedObj matches on
         // disk version
         else if ( !isReadOnly )
         {
            // if it's update or delete index scan, the index structure may get
            // changed so everytime we have to compare _curIndexRID and ondisk
            // rid, as well as the stored object
            dmsRecordID onDiskRID = indexExtent.getRID (
                  _curIndexRID._slot ) ;
            // is the index rid still in valid slot?
            if ( onDiskRID.isNull() )
            {
               // if _curIndexRID is no longer exist, for example if there's
               // page split so that the slot is not pointing to any valid
               // record, let's relocate rid
               rc = relocateRID () ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to relocate RID, rc: %d", rc ) ;
                  goto error ;
               }
            }
            // if it's still valid slot, let's see if it matches the last scan
            else if ( onDiskRID == _savedRID )
            {
               // if the RID it's pointing to looks the same, then we need to
               // compare the index key
               CHAR *dataBuffer = indexExtent.getKeyData ( _curIndexRID._slot );
               if ( dataBuffer )
               {
                  try
                  {
                     // get the key that the current RID is pointing to
                     _curKeyObj = ixmKey(dataBuffer).toBson() ;
                     DMS_MON_OP_COUNT_INC( pMonAppCB, MON_INDEX_READ, 1 ) ;
                     DMS_MON_CONTEXT_COUNT_INC ( _pMonCtxCB, MON_INDEX_READ, 1 ) ;
                     // if both on recordRID and key object are the same, let's
                     // say the index is not changed
                     if ( _curKeyObj.shallowEqual(_savedObj) )
                     {
                        // if both key and rid are the same, that means we
                        // should move on to the next
                        rc = indexExtent.advance ( _curIndexRID, _direction ) ;
                        if ( rc )
                        {
                           PD_LOG ( PDERROR, "Failed to advance, rc: %d", rc ) ;
                           goto error ;
                        }
                     }
                     // if the rid are the same but key object not, that means
                     // we got something changed in the index, let's relocate
                     // the RID
                     else
                     {
                        rc = relocateRID () ;
                        if ( rc )
                        {
                           PD_LOG ( PDERROR, "Failed to relocate RID, rc: :%d",
                                    rc ) ;
                           goto error ;
                        }
                     }
                  }
                  catch ( std::exception &e )
                  {
                     PD_LOG ( PDERROR, "Failed to convert buffer to bson from "
                              "current rid: %d,%d: %s", _curIndexRID._extent,
                              _curIndexRID._slot, e.what() ) ;
                     rc = SDB_SYS ;
                     goto error ;
                  }
               } // if ( dataBuffer )
               else
               {
                  PD_LOG ( PDERROR, "Unable to get buffer" ) ;
                  rc = SDB_SYS ;
                  goto error ;
               }
            } // if ( onDiskRID == _savedRID )
            // if the rid are not the same, we must have something changed in
            // the index, let's relocate RID
            else
            {
               rc = relocateRID () ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to relocate RID, rc: %d", rc ) ;
                  goto error ;
               }
            }
         } // if ( !isReadOnly )
         // readonly scan with _savedRID not NULL
         else
         {
            // for readonly scan, most of time _savedRID should be NULL, which
            // will not get into this codepath, the only time when we get here
            // is that pauseScan() + resumeScan() found the index structure is
            // changed so _savedRID is NOT reset, which means it called
            // relocateRID() and we should NOT advance to next
            _savedRID.reset() ;
         }
      }
      // after getting the first key location or advanced to next, let's
      // exame if this index key is what we want
      while ( TRUE )
      {
         // after getting _curIndexRID, we have to check if it's null
         if ( _curIndexRID.isNull() )
         {
            // if doing goto here, compile will get error for the following
            // indexExtent object declare
            return SDB_IXM_EOC ;
         }
         // now let's get the binary key and create BSONObj from it
         ixmExtent indexExtent ( _curIndexRID._extent, _su->index() ) ;
         CHAR *dataBuffer = indexExtent.getKeyData ( _curIndexRID._slot ) ;
         if ( !dataBuffer )
         {
            PD_LOG ( PDERROR, "Failed to get buffer from current rid: %d,%d",
                     _curIndexRID._extent, _curIndexRID._slot ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         try
         {
            // get the key from index rid
            try
            {
               _curKeyObj = ixmKey(dataBuffer).toBson() ;
               DMS_MON_OP_COUNT_INC( pMonAppCB, MON_INDEX_READ, 1 ) ;
               DMS_MON_CONTEXT_COUNT_INC ( _pMonCtxCB, MON_INDEX_READ, 1 ) ;
            }
            catch ( std::exception &e )
            {
               PD_RC_CHECK ( SDB_SYS, PDERROR,
                             "Failed to convert from buffer "
                             "to bson, rid: %d,%d: %s",
                             _curIndexRID._extent,
                             _curIndexRID._slot, e.what() ) ;
            }
            // compare the key in list iterator
            rc = _listIterator.advance ( _curKeyObj ) ;
            // if -2, that means we hit end of iterator, so all other keys in
            // index are not within our select range
            if ( -2 == rc )
            {
               rc = SDB_IXM_EOC ;
               goto done ;
            }
            // if >=0, that means the key is not selected and we want to
            // further advance the key in index
            else if ( rc >= 0 )
            {
               rc = indexExtent.keyAdvance ( _curIndexRID, _curKeyObj, rc,
                                             _listIterator.after(),
                                             _listIterator.cmp(),
                                             _listIterator.inc(),
                                             _order, _direction, _cb ) ;
               PD_RC_CHECK ( rc, PDERROR,
                             "Failed to advance, rc = %d", rc ) ;
               continue ;
            }
            // otherwise let's attempt to get dms rid
            else
            {
               _savedRID =
                     indexExtent.getRID ( _curIndexRID._slot ) ;
               // make sure the RID we read is not psuedo-deleted
               if ( _savedRID.isNull() ||
                    _dupBuffer.end() != _dupBuffer.find ( _savedRID ) )
               {
                  // usually this means a psuedo-deleted rid, we should jump
                  // back to beginning of the function and advance to next
                  // key

                  // if we are able to find the recordid in dupBuffer, that
                  // means we've already processed the record, so let's also
                  // jump back to begin
                  _savedRID.reset() ;
                  goto begin ;
               }
               // make sure we don't hit maximum size of dedup buffer
               /*if ( _dupBuffer.size() >= _dedupBufferSize )
               {
                  rc = SDB_IXM_DEDUP_BUF_MAX ;
                  goto error ;
               }*/
               _dupBuffer.insert ( _savedRID ) ;
               // ready to return to caller
               rid = _savedRID ;
               // if we are write mode, let's record the _savedObj as well
               if ( !isReadOnly )
               {
                  _savedObj = _curKeyObj.getOwned() ;
               }
               // otherwise if we are read mode, let's reset _savedRID
               else
               {
                  // in readonly scenario, _savedRID should always be null
                  // unless pauseScan() is called
                  _savedRID.reset() ;
               }
               rc = SDB_OK ;
               break ;
            }
         } // try
         catch ( std::exception &e )
         {
            PD_RC_CHECK ( SDB_SYS, PDERROR,
                          "exception during advance index tree: %s",
                          e.what() ) ;
         }
      } // while ( TRUE )
   done :
      PD_TRACE_EXITRC( SDB__RTNIXSCAN_ADVANCE, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // save the bson key + rid for the current index rid, before releasing
   // X latch on the collection
   // we have to call resumeScan after getting X latch again just in case
   // other sessions changed tree structure
   // this is used for query scan only
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNIXSCAN_PAUSESCAN, "_rtnIXScanner::pauseScan" )
   INT32 _rtnIXScanner::pauseScan( BOOLEAN isReadOnly )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNIXSCAN_PAUSESCAN ) ;
      if ( !_init || _curIndexRID.isNull() )
      {
         goto done ;
      }

      // for write mode, since we write _savedRID and _savedObj in advance, we
      // don't do it here, so let's jump to done directly
      if ( !isReadOnly )
      {
         goto done ;
      }

      {
         // for read mode, let's copy savedobj then
         ixmExtent indexExtent ( _curIndexRID._extent, _su->index() ) ;
         CHAR *dataBuffer = indexExtent.getKeyData ( _curIndexRID._slot ) ;
         if ( !dataBuffer )
         {
            PD_LOG ( PDERROR, "Failed to get buffer from current rid: %d,%d",
                     _curIndexRID._extent, _curIndexRID._slot ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         try
         {
            _savedObj = ixmKey(dataBuffer).toBson().copy() ;
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to convert buffer to bson from current "
                     "rid: %d,%d: %s", _curIndexRID._extent,
                     _curIndexRID._slot, e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         _savedRID = indexExtent.getRID ( _curIndexRID._slot ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__RTNIXSCAN_PAUSESCAN, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // restoring the bson key and rid for the current index rid. This is done by
   // comparing the current key/rid pointed by _curIndexRID still same as
   // previous saved one. If so it means there's no change in this key, and we
   // can move on the from the current index rid. Otherwise we have to locate
   // the new position for the saved key+rid
   // this is used in query scan only
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNIXSCAN_RESUMESCAN, "_rtnIXScanner::resumeScan" )
   INT32 _rtnIXScanner::resumeScan( BOOLEAN isReadOnly )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNIXSCAN_RESUMESCAN ) ;

      if ( !_indexCB )
      {
         PD_LOG ( PDERROR, "Failed to allocate memory for indexCB" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      if ( !_indexCB->isInitialized() )
      {
         rc = SDB_RTN_INDEX_NOTEXIST ;
         goto done ;
      }

      if ( _indexCB->getFlag() != IXM_INDEX_FLAG_NORMAL )
      {
         rc = SDB_IXM_UNEXPECTED_STATUS ;
         goto done ;
      }

      if ( !_init || _curIndexRID.isNull() || _savedObj.isEmpty() )
      {
         rc = SDB_OK ;
         goto done ;
      }

      // for write mode, since we write _savedRID and _savedObj in advance, we
      // don't do it here, so let's jump to done directly
      if ( !isReadOnly )
      {
         goto done ;
      }

      // compare the historical index OID with the current index oid, to make
      // sure the index is not changed during the time
      if ( !_indexCB->isStillValid ( _indexOID ) ||
           _indexLID != _indexCB->getLogicalID() )
      {
         rc = SDB_RTN_INDEX_NOTEXIST ;
         goto done ;
      }
      {
      // we assume _indexCB remains valid as long as the indexCBExtent still
      // pointing to the same index oid (which should be unique against each
      // index) because the storage unit should never be remapped in memory
      // during its run
      ixmExtent indexExtent ( _curIndexRID._extent, _su->index() ) ;
      CHAR *dataBuffer = NULL ;
      if ( indexExtent.isStillValid( _indexCB->getMBID() ) )
      {
         dataBuffer = indexExtent.getKeyData ( _curIndexRID._slot ) ;
      }

      if ( dataBuffer )
      {
         try
         {
            BSONObj curObj = ixmKey(dataBuffer).toBson() ;
            // make sure key is the same
            if ( curObj.shallowEqual(_savedObj) )
            {
               dmsRecordID onDiskRID = indexExtent.getRID (
                     _curIndexRID._slot ) ;
               // make sure rid is the same
               if ( onDiskRID == _savedRID )
               {
                  // this means the last scaned record is still here, so let's
                  // reset _savedRID so that we'll call advance()
                  _savedRID.reset() ;
                  // if both key and rid are not changed, let's continue
                  goto done ;
               }
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to convert buffer to bson from "
                     "current rid: %d,%d: %s", _curIndexRID._extent,
                     _curIndexRID._slot, e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
      // when we get here, it means something changed and we need to
      // relocateRID
      // note relocateRID may relocate to the index that already read. However
      // after advance() returning the RID we'll check if the index already has
      // been read, so we should be save to not reset _savedRID
      {
         rc = relocateRID () ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to relocate RID, rc: %d", rc ) ;
            goto error ;
         }
      }
      }

   done :
      PD_TRACE_EXITRC ( SDB__RTNIXSCAN_RESUMESCAN, rc ) ;
      return rc ;
   error :
      goto done ;
   }

}

