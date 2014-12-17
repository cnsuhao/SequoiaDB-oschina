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

   Source File Name = dmsTBScanner.hpp

   Descriptive Name = Data Management Service Storage Unit Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          22/08/2013  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsScanner.hpp"
#include "dmsStorageData.hpp"
#include "dmsStorageIndex.hpp"
#include "mthMatcher.hpp"
#include "rtnIXScanner.hpp"
#include "bpsPrefetch.hpp"
#include "dmsCompress.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"

using namespace bson ;

namespace engine
{

   /*
      _dmsScanner implement
   */
   _dmsScanner::_dmsScanner( dmsStorageData *su, dmsMBContext *context,
                             _mthMatcher *match, DMS_ACCESS_TYPE accessType )
   {
      SDB_ASSERT( su, "storage data can't be NULL" ) ;
      SDB_ASSERT( context, "context can't be NULL" ) ;
      _pSu = su ;
      _context = context ;
      _match = match ;
      _accessType = accessType ;
   }

   _dmsScanner::~_dmsScanner()
   {
      _context    = NULL ;
      _pSu        = NULL ;
   }

   /*
      _dmsExtScanner implement
   */
   _dmsExtScanner::_dmsExtScanner( dmsStorageData *su, dmsMBContext *context,
                                   mthMatcher *match, dmsExtentID curExtentID,
                                   DMS_ACCESS_TYPE accessType,
                                   INT64 maxRecords, INT64 skipNum )
   :_dmsScanner( su, context, match, accessType )
   {
      _maxRecords          = maxRecords ;
      _skipNum             = skipNum ;
      _curRecordPtr        = 0 ;
      _next                = DMS_INVALID_OFFSET ;
      _firstRun            = TRUE ;
      _pMonAppCB           = NULL ;
      _pTransCB            = NULL ;
      _curRID._extent      = curExtentID ;
      _recordXLock         = FALSE ;
      _needUnLock          = FALSE ;
      _cb                  = NULL ;

      if ( DMS_ACCESS_TYPE_UPDATE == _accessType ||
           DMS_ACCESS_TYPE_DELETE == _accessType ||
           DMS_ACCESS_TYPE_INSERT == _accessType )
      {
         _recordXLock = TRUE ;
      }
   }

   _dmsExtScanner::~_dmsExtScanner ()
   {
      _extent     = NULL ;
      _pMonAppCB  = NULL ;

      if ( FALSE == _firstRun && _recordXLock &&
           DMS_INVALID_OFFSET != _curRID._offset )
      {
         _pTransCB->transLockRelease( _cb, _pSu->logicalID(), _context->mbID(),
                                      &_curRID ) ;
      }
   }

   dmsExtentID _dmsExtScanner::nextExtentID() const
   {
      if ( _extent )
      {
         return _extent->_nextExtent ;
      }
      return DMS_INVALID_EXTENT ;
   }

   INT32 _dmsExtScanner::stepToNextExtent()
   {
      if ( 0 != _maxRecords &&
           DMS_INVALID_EXTENT != nextExtentID() )
      {
         _curRID._extent = nextExtentID() ;
         _firstRun = TRUE ;
         return SDB_OK ;
      }
      return SDB_DMS_EOC ;
   }

   INT32 _dmsExtScanner::_firstInit( pmdEDUCB *cb )
   {
      INT32 rc          = SDB_OK ;
      _pTransCB         = pmdGetKRCB()->getTransCB() ;
      SDB_BPSCB *pBPSCB = pmdGetKRCB()->getBPSCB () ;
      BOOLEAN   bPreLoadEnabled = pBPSCB->isPreLoadEnabled() ;
      _pMonAppCB        = cb ? cb->getMonAppCB() : NULL ;
      INT32 lockType    = _recordXLock ? EXCLUSIVE : SHARED ;

      if ( _recordXLock && DPS_INVALID_TRANS_ID == cb->getTransID() )
      {
         _needUnLock = TRUE ;
      }

      ossValuePtr extentPtr = _pSu->extentAddr( _curRID._extent ) ;
      SDB_ASSERT( extentPtr, "extent id is invalid" ) ;
      _extent = (dmsExtent*)extentPtr ;
      if ( NULL == _extent )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( cb->isInterrupted() )
      {
         rc = SDB_APP_INTERRUPT ;
         goto error ;
      }
      if ( !_context->isMBLock( lockType ) )
      {
         rc = _context->mbLock( lockType ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb lock failed, rc: %d", rc ) ;
      }
      if ( !dmsAccessAndFlagCompatiblity ( _context->mb()->_flag,
                                           _accessType ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  _context->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }
      if ( !_extent->validate( _context->mbID() ) )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      if ( bPreLoadEnabled && DMS_INVALID_EXTENT != _extent->_nextExtent )
      {
         pBPSCB->sendPreLoadRequest ( bpsPreLoadReq( _pSu->CSID(),
                                                     _pSu->logicalID(),
                                                     _extent->_nextExtent )) ;
      }

      _cb   = cb ;
      _next = _extent->_firstRecordOffset ;
      _firstRun = FALSE ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _dmsExtScanner::advance( dmsRecordID &recordID,
                                  ossValuePtr &recordDataPtr,
                                  pmdEDUCB *cb,
                                  vector<INT64> *dollarList )
   {
      INT32 rc                = SDB_OK ;
      BOOLEAN result          = TRUE ;

      if ( _firstRun )
      {
         rc = _firstInit( cb ) ;
         PD_RC_CHECK( rc, PDWARNING, "first init failed, rc: %d", rc ) ;
      }
      else if ( _needUnLock && DMS_INVALID_OFFSET != _curRID._offset )
      {
         _pTransCB->transLockRelease( cb, _pSu->logicalID(), _context->mbID(),
                                      &_curRID ) ;
      }

      if ( !_match && _skipNum > 0 && _skipNum >= _extent->_recCount )
      {
         _skipNum -= _extent->_recCount ;
         _next = DMS_INVALID_OFFSET ;
      }

      while ( DMS_INVALID_OFFSET != _next && 0 != _maxRecords )
      {
         _curRecordPtr = (ossValuePtr)_extent + _next ;
         _curRID._offset = _next ;
         _next = DMS_RECORD_GETNEXTOFFSET(_curRecordPtr) ;

         if ( _recordXLock )
         {
            rc = _pTransCB->tryOrAppendX( cb, _pSu->logicalID(),
                                          _context->mbID(), &_curRID ) ;
            if ( rc )
            {
               PD_CHECK( SDB_DPS_TRANS_APPEND_TO_WAIT == rc, rc, error, PDERROR,
                         "Failed to get record, append lock-wait-queue failed, "
                         "rc: %d", rc ) ;
               _context->pause() ;
               rc = _pTransCB->waitLock( cb, _pSu->logicalID(),
                                         _context->mbID(), &_curRID ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to wait record lock, rc: %d",
                            rc ) ;
               rc = _context->resume() ;
               if ( rc )
               {
                  PD_LOG( PDERROR, "Remove dms mb context failed, rc: %d", rc );
                  goto error_release ;
               }
               if ( !dmsAccessAndFlagCompatiblity ( _context->mb()->_flag,
                                                    _accessType ) )
               {
                  PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                           _context->mb()->_flag ) ;
                  rc = SDB_DMS_INCOMPATIBLE_MODE ;
                  goto error_release ;
               }
               _next = DMS_RECORD_GETNEXTOFFSET(_curRecordPtr) ;
            }
         }

         if ( OSS_BIT_TEST( DMS_RECORD_FLAG_DELETING,
                            DMS_RECORD_GETATTR(_curRecordPtr) ) )
         {
            if ( _recordXLock )
            {
               INT32 rc1 = _pSu->deleteRecord( _context, _curRID, 0, cb, NULL ) ;
               if ( rc1 )
               {
                  PD_LOG( PDWARNING, "Failed to delete the deleting record, "
                          "rc: %d", rc ) ;
               }
               _pTransCB->transLockRelease( cb, _pSu->logicalID(),
                                            _context->mbID(), &_curRID ) ;
            }
            continue ;
         }
         SDB_ASSERT( DMS_RECORD_FLAG_DELETED !=
                     DMS_RECORD_GETSTATE(_curRecordPtr),
                     "record can't be deleted" ) ;

         if ( !_match && _skipNum > 0 )
         {
            --_skipNum ;
         }
         else
         {
            recordID = _curRID ;
            if ( DMS_RECORD_FLAG_OVERFLOWF ==
                 DMS_RECORD_GETSTATE(_curRecordPtr) )
            {
               _ovfRID = DMS_RECORD_GETOVF(_curRecordPtr) ;
               _curRecordPtr = _pSu->extentAddr(_ovfRID._extent) +
                               _ovfRID._offset ;
               DMS_MON_OP_COUNT_INC( _pMonAppCB, MON_DATA_READ, 1 ) ;
            }
            DMS_RECORD_EXTRACTDATA(_curRecordPtr, recordDataPtr) ;
            DMS_MON_OP_COUNT_INC( _pMonAppCB, MON_DATA_READ, 1 ) ;
            DMS_MON_OP_COUNT_INC( _pMonAppCB, MON_READ, 1 ) ;

            if ( _match )
            {
               result = TRUE ;
               try
               {
                  BSONObj obj ( (CHAR*)recordDataPtr ) ;
                  rc = _match->matches( obj, result, dollarList ) ;
                  if ( rc )
                  {
                     PD_LOG( PDERROR, "Failed to match record, rc: %d", rc ) ;
                     goto error_release ;
                  }
                  if ( result )
                  {
                     if ( _skipNum > 0 )
                     {
                        --_skipNum ;
                     }
                     else
                     {
                        if ( _maxRecords > 0 )
                        {
                           --_maxRecords ;
                        }
                        goto done ; // find ok
                     }
                  }
               }
               catch( std::exception &e )
               {
                  PD_LOG ( PDERROR, "Failed to create BSON object: %s",
                           e.what() ) ;
                  rc = SDB_SYS ;
                  goto error_release ;
               }
            } // if ( _match )
            else
            {
               if ( _skipNum > 0 )
               {
                  --_skipNum ;
               }
               else
               {
                  if ( _maxRecords > 0 )
                  {
                     --_maxRecords ;
                  }
                  goto done ; // find ok
               }
            }
         }

         if ( _recordXLock )
         {
            _pTransCB->transLockRelease( cb, _pSu->logicalID(),
                                         _context->mbID(), &_curRID ) ;
         }
      } // while

      rc = SDB_DMS_EOC ;
      goto error ;

   done:
      return rc ;
   error:
      recordID.reset() ;
      recordDataPtr = 0 ;
      _curRID._offset = DMS_INVALID_OFFSET ;
      goto done ;
   error_release:
      _pTransCB->transLockRelease( cb, _pSu->logicalID(), _context->mbID(),
                                   &_curRID ) ;
      goto error ;
   }

   void _dmsExtScanner::stop()
   {
      if ( FALSE == _firstRun && _recordXLock &&
           DMS_INVALID_OFFSET != _curRID._offset )
      {
         _pTransCB->transLockRelease( _cb, _pSu->logicalID(), _context->mbID(),
                                      &_curRID ) ;
      }
      _next = DMS_INVALID_OFFSET ;
      _curRID._offset = DMS_INVALID_OFFSET ;
   }

   /*
      _dmsTBScanner implement
   */
   _dmsTBScanner::_dmsTBScanner( dmsStorageData *su, dmsMBContext *context,
                                 mthMatcher *match, DMS_ACCESS_TYPE accessType,
                                 INT64 maxRecords, INT64 skipNum )
   :_dmsScanner( su, context, match, accessType ),
    _extScanner( su, context, match, DMS_INVALID_EXTENT, accessType,
                 maxRecords, skipNum )
   {
      _curExtentID   = DMS_INVALID_EXTENT ;
      _firstRun      = TRUE ;
   }

   _dmsTBScanner::~_dmsTBScanner()
   {
      _curExtentID   = DMS_INVALID_EXTENT ;
   }

   INT32 _dmsTBScanner::_firstInit()
   {
      INT32 rc = SDB_OK ;
      INT32 lockType = _extScanner._recordXLock ? EXCLUSIVE : SHARED ;
      if ( !_context->isMBLock( lockType ) )
      {
         rc = _context->mbLock( lockType ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb lock failed, rc: %d", rc ) ;
      }
      _curExtentID = _context->mb()->_firstExtentID ;
      _resetExtScanner() ;
      _firstRun = FALSE ;

   done:
      return rc ;
   error:
      goto done ;
   }

   void _dmsTBScanner::_resetExtScanner()
   {
      _extScanner._firstRun = TRUE ;
      _extScanner._curRID._extent = _curExtentID ;
   }

   INT32 _dmsTBScanner::advance( dmsRecordID &recordID,
                                 ossValuePtr &recordDataPtr,
                                 pmdEDUCB *cb,
                                 vector<INT64> *dollarList )
   {
      INT32 rc = SDB_OK ;
      if ( _firstRun )
      {
         rc = _firstInit() ;
         PD_RC_CHECK( rc, PDERROR, "First init failed, rc: %d", rc ) ;
      }

      while ( DMS_INVALID_EXTENT != _curExtentID )
      {
         rc = _extScanner.advance( recordID, recordDataPtr, cb, dollarList ) ;
         if ( SDB_DMS_EOC == rc )
         {
            if ( 0 != _extScanner.getMaxRecords() )
            {
               _curExtentID = _extScanner.nextExtentID() ;
               _resetExtScanner() ;
               _context->pause() ;
               continue ;
            }
            else
            {
               _curExtentID = DMS_INVALID_EXTENT ;
               goto error ;
            }
         }
         else if ( rc )
         {
            PD_LOG( PDERROR, "Extent scanner failed, rc: %d", rc ) ;
            goto error ;
         }
         else
         {
            goto done ;
         }
      }
      rc = SDB_DMS_EOC ;
      goto error ;

   done:
      return rc ;
   error:
      goto done ;
   }

   void _dmsTBScanner::stop()
   {
      _extScanner.stop() ;
      _curExtentID = DMS_INVALID_EXTENT ;
   }

   /*
      _dmsIXSecScanner implement
   */
   _dmsIXSecScanner::_dmsIXSecScanner( dmsStorageData *su,
                                       dmsMBContext *context,
                                       mthMatcher *match,
                                       rtnIXScanner *scanner,
                                       DMS_ACCESS_TYPE accessType,
                                       INT64 maxRecords,
                                       INT64 skipNum )
   :_dmsScanner( su, context, match, accessType )
   {
      _maxRecords          = maxRecords ;
      _skipNum             = skipNum ;
      _curRecordPtr        = 0 ;
      _firstRun            = TRUE ;
      _pMonAppCB           = NULL ;
      _pTransCB            = NULL ;
      _recordXLock         = FALSE ;
      _needUnLock          = FALSE ;
      _cb                  = NULL ;
      _scanner             = scanner ;
      _onceRestNum         = 0 ;
      _eof                 = FALSE ;
      _indexBlockScan      = FALSE ;
      _judgeStartKey       = FALSE ;
      _includeStartKey     = FALSE ;
      _includeEndKey       = FALSE ;
      _blockScanDir        = 1 ;

      if ( DMS_ACCESS_TYPE_UPDATE == _accessType ||
           DMS_ACCESS_TYPE_DELETE == _accessType ||
           DMS_ACCESS_TYPE_INSERT == _accessType )
      {
         _recordXLock = TRUE ;
      }
   }

   _dmsIXSecScanner::~_dmsIXSecScanner ()
   {
      _pMonAppCB  = NULL ;

      if ( FALSE == _firstRun && _recordXLock &&
           DMS_INVALID_OFFSET != _curRID._offset )
      {
         _pTransCB->transLockRelease( _cb, _pSu->logicalID(), _context->mbID(),
                                      &_curRID ) ;
      }

      _scanner    = NULL ;
   }

   void  _dmsIXSecScanner::enableIndexBlockScan( const BSONObj &startKey,
                                                 const BSONObj &endKey,
                                                 const dmsRecordID &startRID,
                                                 const dmsRecordID &endRID,
                                                 INT32 direction )
   {
      SDB_ASSERT( _scanner, "Scanner can't be NULL" ) ;

      _blockScanDir     = direction ;
      _indexBlockScan   = TRUE ;
      _judgeStartKey    = FALSE ;
      _includeStartKey  = FALSE ;
      _includeEndKey    = FALSE ;

      _startKey = startKey.getOwned() ;
      _endKey = endKey.getOwned() ;
      _startRID = startRID ;
      _endRID = endRID ;

      BSONObj startObj = _scanner->getPredicateList()->startKey() ;
      BSONObj endObj = _scanner->getPredicateList()->endKey() ;

      if ( 0 == startObj.woCompare( *_getStartKey(), BSONObj(), false ) &&
           _getStartRID()->isNull() )
      {
         _includeStartKey = TRUE ;
      }
      if ( 0 == endObj.woCompare( *_getEndKey(), BSONObj(), false ) &&
           _getEndRID()->isNull() )
      {
         _includeEndKey = TRUE ;
      }

      if ( _getStartRID()->isNull() )
      {
         if ( 1 == _scanner->getDirection() )
         {
            _getStartRID()->resetMin () ;
         }
         else
         {
            _getStartRID()->resetMax () ;
         }
      }
      if ( _getEndRID()->isNull() )
      {
         if ( !_includeEndKey )
         {
            if ( 1 == _scanner->getDirection() )
            {
               _getEndRID()->resetMin () ;
            }
            else
            {
               _getEndRID()->resetMax () ;
            }
         }
         else
         {
            if ( 1 == _scanner->getDirection() )
            {
               _getEndRID()->resetMax () ;
            }
            else
            {
               _getEndRID()->resetMin () ;
            }
         }
      }
   }

   INT32 _dmsIXSecScanner::_firstInit( pmdEDUCB * cb )
   {
      INT32 rc          = SDB_OK ;
      _pTransCB         = pmdGetKRCB()->getTransCB() ;
      _pMonAppCB        = cb ? cb->getMonAppCB() : NULL ;
      INT32 lockType    = _recordXLock ? EXCLUSIVE : SHARED ;

      if ( _recordXLock && DPS_INVALID_TRANS_ID == cb->getTransID() )
      {
         _needUnLock = TRUE ;
      }
      if ( NULL == _scanner )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( cb && cb->isInterrupted() )
      {
         rc = SDB_APP_INTERRUPT ;
         goto error ;
      }
      if ( !_context->isMBLock( lockType ) )
      {
         rc = _context->mbLock( lockType ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb lock failed, rc: %d", rc ) ;
      }
      if ( !dmsAccessAndFlagCompatiblity ( _context->mb()->_flag,
                                           _accessType ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  _context->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      rc = _scanner->resumeScan( _recordXLock ? FALSE : TRUE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to resum ixscan, rc: %d", rc ) ;

      _cb   = cb ;
      _firstRun = FALSE ;
      _onceRestNum = (INT64)pmdGetKRCB()->getOptionCB()->indexScanStep() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   BSONObj* _dmsIXSecScanner::_getStartKey ()
   {
      return _scanner->getDirection() == _blockScanDir ? &_startKey : &_endKey ;
   }

   BSONObj* _dmsIXSecScanner::_getEndKey ()
   {
      return _scanner->getDirection() == _blockScanDir ? &_endKey : &_startKey ;
   }

   dmsRecordID* _dmsIXSecScanner::_getStartRID ()
   {
      return _scanner->getDirection() == _blockScanDir ? &_startRID : &_endRID ;
   }

   dmsRecordID* _dmsIXSecScanner::_getEndRID ()
   {
      return _scanner->getDirection() == _blockScanDir ? &_endRID : &_startRID ;
   }

   INT32 _dmsIXSecScanner::advance( dmsRecordID &recordID,
                                    ossValuePtr &recordDataPtr,
                                    pmdEDUCB * cb,
                                    vector<INT64> *dollarList )
   {
      INT32 rc                = SDB_OK ;
      BOOLEAN result          = TRUE ;

      if ( _firstRun )
      {
         rc = _firstInit( cb ) ;
         PD_RC_CHECK( rc, PDWARNING, "first init failed, rc: %d", rc ) ;
      }
      else if ( _needUnLock && DMS_INVALID_OFFSET != _curRID._offset )
      {
         _pTransCB->transLockRelease( cb, _pSu->logicalID(), _context->mbID(),
                                      &_curRID ) ;
      }

      while ( _onceRestNum-- > 0 && 0 != _maxRecords )
      {
         rc = _scanner->advance( _curRID, _recordXLock ? FALSE : TRUE ) ;
         if ( SDB_IXM_EOC == rc )
         {
            _eof = TRUE ;
            rc = SDB_DMS_EOC ;
            break ;
         }
         else if ( rc )
         {
            PD_LOG( PDERROR, "IXScanner advance failed, rc: %d", rc ) ;
            goto error ;
         }
         SDB_ASSERT( _curRID.isValid(), "rid msut valid" ) ;

         if ( _indexBlockScan )
         {
            INT32 result = 0 ;
            if ( !_judgeStartKey )
            {
               _judgeStartKey = TRUE ;

               result = _scanner->compareWithCurKeyObj( *_getStartKey() ) ;
               if ( 0 == result )
               {
                  result = _curRID.compare( *_getStartRID() ) *
                           _scanner->getDirection() ;
               }
               if ( result < 0 )
               {
                  rc = _scanner->relocateRID( *_getStartKey(),
                                              *_getStartRID() ) ;
                  PD_RC_CHECK( rc, PDERROR, "Failed to relocateRID, rc: %d",
                               rc ) ;
                  continue ;
               }
            }

            result = _scanner->compareWithCurKeyObj( *_getEndKey() ) ;
            if ( 0 == result )
            {
               result = _curRID.compare( *_getEndRID() ) *
                        _scanner->getDirection() ;
            }
            if ( result > 0 || ( !_includeEndKey && result == 0 ) )
            {
               _eof = TRUE ;
               rc = SDB_DMS_EOC ;
               break ;
            }
         }

         if ( !_match && _skipNum > 0 )
         {
            --_skipNum ;
            continue ;
         }

         _curRecordPtr = (ossValuePtr)_pSu->extentAddr( _curRID._extent ) +
                         _curRID._offset ;

         if ( _recordXLock )
         {
            rc = _pTransCB->tryOrAppendX( cb, _pSu->logicalID(),
                                          _context->mbID(), &_curRID ) ;
            if ( rc )
            {
               PD_CHECK( SDB_DPS_TRANS_APPEND_TO_WAIT == rc, rc, error, PDERROR,
                         "Failed to get record, append lock-wait-queue failed, "
                         "rc: %d", rc ) ;
               rc = _scanner->pauseScan( _recordXLock ? FALSE : TRUE ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to pause scan, rc: %d", rc ) ;

               _context->pause() ;
               rc = _pTransCB->waitLock( cb, _pSu->logicalID(),
                                         _context->mbID(), &_curRID ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to wait record lock, rc: %d",
                            rc ) ;
               rc = _context->resume() ;
               if ( rc )
               {
                  PD_LOG( PDERROR, "Remove dms mb context failed, rc: %d", rc );
                  goto error_release ;
               }
               if ( !dmsAccessAndFlagCompatiblity ( _context->mb()->_flag,
                                                    _accessType ) )
               {
                  PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                           _context->mb()->_flag ) ;
                  rc = SDB_DMS_INCOMPATIBLE_MODE ;
                  goto error_release ;
               }
               rc = _scanner->resumeScan( _recordXLock ? FALSE : TRUE ) ;
               if ( rc )
               {
                  PD_LOG( PDERROR, "Failed to resum ixscan, rc: %d", rc ) ;
                  goto error_release ;
               }
            }
         }
         /*
         if ( OSS_BIT_TEST( DMS_RECORD_FLAG_DELETING,
                            DMS_RECORD_GETATTR(_curRecordPtr) ) )
         {
            if ( _recordXLock )
            {
               INT32 rc1 = _pSu->deleteRecord( context, _curRID, 0, cb, NULL ) ;
               if ( rc1 )
               {
                  PD_LOG( PDWARNING, "Failed to delete the deleting record, "
                          "rc: %d", rc ) ;
               }
               _pTransCB->transLockRelease( cb, _pSu->logicalID(),
                                            _context->mbID(), &_curRID ) ;
            }
            continue ;
         }*/
         SDB_ASSERT( DMS_RECORD_FLAG_DELETED !=
                     DMS_RECORD_GETSTATE(_curRecordPtr),
                     "record can't be deleted" ) ;

         recordID = _curRID ;
         if ( DMS_RECORD_FLAG_OVERFLOWF ==
              DMS_RECORD_GETSTATE(_curRecordPtr) )
         {
            _ovfRID = DMS_RECORD_GETOVF(_curRecordPtr) ;
            _curRecordPtr = _pSu->extentAddr(_ovfRID._extent) +
                            _ovfRID._offset ;
            DMS_MON_OP_COUNT_INC( _pMonAppCB, MON_DATA_READ, 1 ) ;
         }
         DMS_RECORD_EXTRACTDATA(_curRecordPtr, recordDataPtr) ;
         DMS_MON_OP_COUNT_INC( _pMonAppCB, MON_DATA_READ, 1 ) ;
         DMS_MON_OP_COUNT_INC( _pMonAppCB, MON_READ, 1 ) ;

         if ( _match )
         {
            result = TRUE ;
            try
            {
               BSONObj obj ( (CHAR*)recordDataPtr ) ;
               rc = _match->matches( obj, result, dollarList ) ;
               if ( rc )
               {
                  PD_LOG( PDERROR, "Failed to match record, rc: %d", rc ) ;
                  goto error_release ;
               }
               if ( result )
               {
                  if ( _skipNum > 0 )
                  {
                     --_skipNum ;
                  }
                  else
                  {
                     if ( _maxRecords > 0 )
                     {
                        --_maxRecords ;
                     }
                     goto done ; // find ok
                  }
               }
            }
            catch( std::exception &e )
            {
               PD_LOG ( PDERROR, "Failed to create BSON object: %s",
                        e.what() ) ;
               rc = SDB_SYS ;
               goto error_release ;
            }
         } // if ( _match )
         else
         {
            if ( _skipNum > 0 )
            {
               --_skipNum ;
            }
            else
            {
               if ( _maxRecords > 0 )
               {
                  --_maxRecords ;
               }
               goto done ; // find ok
            }
         }

         if ( _recordXLock )
         {
            _pTransCB->transLockRelease( cb, _pSu->logicalID(),
                                         _context->mbID(), &_curRID ) ;
         }
      } // while

      rc = SDB_DMS_EOC ;
      {
         INT32 rcTmp = _scanner->pauseScan( _recordXLock ? FALSE : TRUE ) ;
         if ( rcTmp )
         {
            PD_LOG( PDERROR, "Pause scan failed, rc: %d", rcTmp ) ;
            rc = rcTmp ;
         }
      }
      goto error ;

   done:
      if ( SDB_OK == rc && ( _onceRestNum <= 0 || 0 == _maxRecords ) )
      {
         rc = _scanner->pauseScan( _recordXLock ? FALSE : TRUE ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Pause scan failed, rc: %d", rc ) ;
         }
      }
      return rc ;
   error:
      recordID.reset() ;
      recordDataPtr = 0 ;
      _curRID._offset = DMS_INVALID_OFFSET ;
      goto done ;
   error_release:
      _pTransCB->transLockRelease( cb, _pSu->logicalID(), _context->mbID(),
                                   &_curRID ) ;
      goto error ;
   }

   void _dmsIXSecScanner::stop ()
   {
      if ( FALSE == _firstRun && _recordXLock &&
           DMS_INVALID_OFFSET != _curRID._offset )
      {
         _pTransCB->transLockRelease( _cb, _pSu->logicalID(), _context->mbID(),
                                      &_curRID ) ;
      }
      if ( DMS_INVALID_OFFSET != _curRID._offset )
      {
         INT32 rc = _scanner->pauseScan( _recordXLock ? FALSE : TRUE ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Pause scan failed, rc: %d", rc ) ;
         }
      }
      _curRID._offset = DMS_INVALID_OFFSET ;
   }

   /*
      _dmsIXScanner implement
   */
   _dmsIXScanner::_dmsIXScanner( dmsStorageData *su, dmsMBContext *context,
                                 mthMatcher *match, rtnIXScanner *scanner,
                                 BOOLEAN ownedScanner,
                                 DMS_ACCESS_TYPE accessType,
                                 INT64 maxRecords,
                                 INT64 skipNum )
   :_dmsScanner( su, context, match, accessType ),
    _secScanner( su, context, match, scanner, accessType, maxRecords, skipNum )
   {
      _scanner       = scanner ;
      _eof           = FALSE ;
      _ownedScanner  = ownedScanner ;
   }

   _dmsIXScanner::~_dmsIXScanner()
   {
      if ( _scanner && _ownedScanner )
      {
         SDB_OSS_DEL _scanner ;
      }
      _scanner       = NULL ;
   }

   void _dmsIXScanner::_resetIXSecScanner ()
   {
      _secScanner._firstRun = TRUE ;
   }

   INT32 _dmsIXScanner::advance( dmsRecordID &recordID,
                                 ossValuePtr &recordDataPtr,
                                 pmdEDUCB * cb,
                                 vector<INT64> *dollarList )
   {
      INT32 rc = SDB_OK ;

      while ( !_eof )
      {
         rc = _secScanner.advance( recordID, recordDataPtr, cb, dollarList ) ;
         if ( SDB_DMS_EOC == rc )
         {
            if ( 0 != _secScanner.getMaxRecords() &&
                 !_secScanner.eof() )
            {
               _resetIXSecScanner() ;
               _context->pause() ;
               continue ;
            }
            else
            {
               _eof = TRUE ;
               goto error ;
            }
         }
         else if ( rc )
         {
            PD_LOG( PDERROR, "IX scanner failed, rc: %d", rc ) ;
            goto error ;
         }
         else
         {
            goto done ;
         }
      }
      rc = SDB_DMS_EOC ;
      goto error ;

   done:
      return rc ;
   error:
      goto done ;
   }

   void _dmsIXScanner::stop ()
   {
      _secScanner.stop() ;
      _eof = TRUE ;
   }

   /*
      _dmsExtentItr implement
   */
   _dmsExtentItr::_dmsExtentItr( dmsStorageData *su, dmsMBContext * context,
                                 DMS_ACCESS_TYPE accessType )
   {
      SDB_ASSERT( su, "storage data unit can't be NULL" ) ;
      SDB_ASSERT( context, "context can't be NULL" ) ;

      _pSu = su ;
      _context = context ;
      _curExtAddr = 0 ;
      _curExtent  = NULL ;
      _extentCount = 0 ;
      _accessType = accessType ;
   }

   _dmsExtentItr::~_dmsExtentItr ()
   {
      _pSu = NULL ;
      _context = NULL ;
      _curExtAddr = 0 ;
      _curExtent = NULL ;
   }

   #define DMS_EXTENT_ITR_EXT_PERCOUNT    20

   INT32 _dmsExtentItr::next( dmsExtent **ppExtent, pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;

      SDB_ASSERT( ppExtent, "Extent can't be NULL" ) ;
      *ppExtent = NULL ;

      if ( _extentCount >= DMS_EXTENT_ITR_EXT_PERCOUNT )
      {
         _context->pause() ;
         _extentCount = 0 ;

         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }
      }

      if ( !_context->isMBLock() )
      {
         if ( _context->canResume() )
         {
            rc = _context->resume() ;
         }
         else
         {
            rc = _context->mbLock( ( DMS_ACCESS_TYPE_UPDATE == _accessType ||
                                     DMS_ACCESS_TYPE_DELETE == _accessType ||
                                     DMS_ACCESS_TYPE_INSERT == _accessType ) ?
                                    EXCLUSIVE : SHARED ) ;
         }
         PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

         if ( !dmsAccessAndFlagCompatiblity ( _context->mb()->_flag,
                                              _accessType ) )
         {
            PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                     _context->mb()->_flag ) ;
            rc = SDB_DMS_INCOMPATIBLE_MODE ;
            goto error ;
         }
      }

      if ( NULL == _curExtent )
      {
         if ( DMS_INVALID_EXTENT == _context->mb()->_firstExtentID )
         {
            rc = SDB_DMS_EOC ;
            goto error ;
         }
         else
         {
            _curExtAddr = _pSu->extentAddr( _context->mb()->_firstExtentID ) ;
         }
      }
      else if ( DMS_INVALID_EXTENT == _curExtent->_nextExtent )
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }
      else
      {
         _curExtAddr = _pSu->extentAddr( _curExtent->_nextExtent ) ;
      }

      if ( 0 == _curExtAddr )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      _curExtent = (dmsExtent*)_curExtAddr ;
      if ( !_curExtent->validate( _context->mbID() ) )
      {
         PD_LOG( PDERROR, "Invalid extent[%d]", _pSu->extentID(_curExtAddr) ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      *ppExtent = _curExtent ;
      ++_extentCount ;

   done:
      return rc ;
   error:
      goto done ;
   }

}


