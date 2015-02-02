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

   Source File Name = rtnContext.cpp

   Descriptive Name = Runtime Context

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime Context helper
   functions.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "rtnContext.hpp"
#include "rtnIXScanner.hpp"
#include "optAccessPlan.hpp"
#include "coordCB.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "dmsScanner.hpp"
#include "dmsStorageUnit.hpp"
#include "msgMessage.hpp"
#include "rtnCoordCommon.hpp"
#include "spdSession.hpp"
#include "rtn.hpp"
#include "rtnContextSort.hpp"
#include "dpsOp2Record.hpp"
#include "rtnLob.hpp"

using namespace bson;
namespace engine
{
   extern void needResetSelector( const BSONObj &,
                                  const BSONObj &,
                                  BOOLEAN & ) ;
   /*
      Functions
   */
   const CHAR* getContextTypeDesp( RTN_CONTEXT_TYPE type )
   {
      switch ( type )
      {
         case RTN_CONTEXT_DATA :
            return "DATA" ;
         case RTN_CONTEXT_DUMP :
            return "DUMP" ;
         case RTN_CONTEXT_COORD :
            return "COORD" ;
         case RTN_CONTEXT_QGM :
            return "QGM" ;
         case RTN_CONTEXT_TEMP :
            return "TEMP" ;
         case RTN_CONTEXT_SP :
            return "SP" ;
         case RTN_CONTEXT_PARADATA :
            return "PARADATA" ;
         case RTN_CONTEXT_MAINCL :
            return "MAINCL" ;
         case RTN_CONTEXT_SORT :
            return "SORT" ;
         case RTN_CONTEXT_QGMSORT :
            return "QGMSORT" ;
         case RTN_CONTEXT_DELCS :
            return "DELCS" ;
         case RTN_CONTEXT_DELCL :
            return "DELCL" ;
         case RTN_CONTEXT_DELMAINCL :
            return "DELMAINCL" ;
         case RTN_CONTEXT_EXPLAIN :
            return "EXPLAIN" ;
         case RTN_CONTEXT_LOB :
            return "LOB" ;
         case RTN_CONTEXT_SHARD_OF_LOB :
            return "SHARD_OF_LOB" ;
         case RTN_CONTEXT_LIST_LOB :
            return "LIST_LOB" ;
         default :
            break ;
      }
      return "UNKNOW" ;
   }

   #define RTN_CONTEXT_GETNUM_ONCE              (100)

   /*
      _rtnContextBase implement
   */
   _rtnContextBase::_rtnContextBase( INT64 contextID, UINT64 eduID )
   :_waitPrefetchNum( 0 )
   {
      _contextID           = contextID ;
      _eduID               = eduID ;

      _pResultBuffer       = NULL ;
      _resultBufferSize    = 0 ;
      _bufferCurrentOffset = 0 ;
      _bufferEndOffset     = 0 ;
      _bufferNumRecords    = 0 ;
      _totalRecords        = 0 ;

      _hitEnd              = TRUE ;
      _isOpened            = FALSE ;

      _matcher             = NULL ;
      _ownedMatcher        = FALSE ;

      _prefetchID          = 0 ;
      _isInPrefetch        = FALSE ;
      _prefetchRet         = SDB_OK ;
      _pPrefWatcher        = NULL ;
      _pMonAppCB           = NULL ;
   }

   _rtnContextBase::~_rtnContextBase()
   {
      _close() ;

      if ( _pResultBuffer )
      {
         *RTN_GET_CONTEXT_FLAG( _pResultBuffer ) = 0 ;

         if ( *RTN_GET_REFERENCE( _pResultBuffer ) == 0 )
         {
            SDB_OSS_FREE( RTN_BUFF_TO_REAL_PTR( _pResultBuffer ) ) ;
            _pResultBuffer = NULL ;
         }
         else
         {
            _dataLock.release_r() ;
         }
      }

      _prefetchLock.lock_w() ;
      _prefetchLock.release_w() ;

      if ( _matcher && _ownedMatcher )
      {
         SDB_OSS_DEL _matcher ;
         _matcher = NULL ;
         _ownedMatcher = FALSE ;
      }
      _pPrefWatcher = NULL ;

      SDB_ASSERT( 0 == _waitPrefetchNum.peek(), "Has wait prefetch jobs" ) ;
      SDB_ASSERT( FALSE == _isInPrefetch, "Has prefetch job run" ) ;
   }

   void _rtnContextBase::waitForPrefetch()
   {
      _close() ;

      if ( _canPrefetch() )
      {
         _dataLock.lock_r() ;
         _dataLock.release_r() ;
      }
   }

   INT32 _rtnContextBase::getReference() const
   {
      if ( _pResultBuffer )
      {
         return *RTN_GET_REFERENCE( _pResultBuffer ) ;
      }
      return 0 ;
   }

   void _rtnContextBase::enablePrefetch( _pmdEDUCB * cb,
                                         rtnPrefWatcher *pWatcher )
   { 
      _prefetchID = 1 ;
      _pPrefWatcher = pWatcher ;
      _pMonAppCB = cb->getMonAppCB() ;
   }

   string _rtnContextBase::toString()
   {
      stringstream ss ;

      ss << "BufferSize:" << _resultBufferSize ;

      if ( _totalRecords > 0 )
      {
         ss << ",TotalRecordNum:" << _totalRecords ;
      }
      if ( _bufferNumRecords > 0 )
      {
         ss << ",BufferRecordNum:" << _bufferNumRecords ;
      }
      if ( _matcher && !_matcher->getMatchPattern().isEmpty() )
      {
         ss << ",Matcher:" << _matcher->getMatchPattern().toString() ;
      }

      _toString( ss ) ;

      return ss.str() ;
   }

   INT32 _rtnContextBase::newMatcher ()
   {
      if ( _matcher && _ownedMatcher )
      {
         SDB_OSS_DEL _matcher ;
         _matcher = NULL ;
         _ownedMatcher = FALSE ;
      }

      _matcher = SDB_OSS_NEW mthMatcher() ;
      if ( !_matcher )
      {
         return SDB_OOM ;
      }

      _ownedMatcher = TRUE ;
      return SDB_OK ;
   }

   INT32 _rtnContextBase::_reallocBuffer( SINT32 requiredSize )
   {
      INT32 rc = SDB_OK ;
      CHAR *originalPointer = _pResultBuffer ;
      SINT32 originalSize   = _resultBufferSize ;

      if ( 0 == originalSize )
      {
         _resultBufferSize = RTN_DFT_BUFFERSIZE ;
      }

      while ( requiredSize > _resultBufferSize )
      {
         if ( _resultBufferSize >= RTN_RESULTBUFFER_SIZE_MAX )
         {
            PD_LOG ( PDERROR, "Result buffer is greater than %d bytes",
                     RTN_RESULTBUFFER_SIZE_MAX ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         _resultBufferSize = _resultBufferSize << 1 ;
         if (_resultBufferSize > RTN_RESULTBUFFER_SIZE_MAX )
         {
            _resultBufferSize = RTN_RESULTBUFFER_SIZE_MAX ;
         }
      }

      if ( NULL == _pResultBuffer )
      {
         _pResultBuffer = ( CHAR* )SDB_OSS_MALLOC(
                                 RTN_BUFF_TO_PTR_SIZE( _resultBufferSize ) ) ;
      }
      else
      {
         _pResultBuffer = (CHAR*)SDB_OSS_REALLOC(
                                 RTN_BUFF_TO_REAL_PTR( _pResultBuffer ),
                                 RTN_BUFF_TO_PTR_SIZE( _resultBufferSize ) ) ;
      }
      if ( !_pResultBuffer )
      {
         PD_LOG ( PDERROR, "Unable to allocate buffer for %d bytes",
                  _resultBufferSize ) ;
         _pResultBuffer = originalPointer ;
         _resultBufferSize = originalSize ;
         rc = SDB_OOM ;
         goto error ;
      }
      else
      {
         _pResultBuffer = RTN_REAL_PTR_TO_BUFF( _pResultBuffer ) ;

         if ( !originalPointer )
         {
            *RTN_GET_REFERENCE( _pResultBuffer ) = 0 ;
            *RTN_GET_CONTEXT_FLAG( _pResultBuffer ) = 1 ;
         }
      }

   done :
      return rc ;
   error :
      goto done ;
   }

   INT32 _rtnContextBase::append( const BSONObj &result )
   {
      INT32 rc = SDB_OK ;

      if ( !_isOpened )
      {
         _isOpened = TRUE ;
      }

      _bufferEndOffset = ossAlign4( (UINT32)_bufferEndOffset ) ;
      if ( _bufferEndOffset + result.objsize () > _resultBufferSize )
      {
         rc = _reallocBuffer ( _bufferEndOffset + result.objsize() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to reallocate buffer for context, rc: "
                     "%d", rc ) ;
            goto error ;
         }
      }
      ossMemcpy ( &(_pResultBuffer[_bufferEndOffset]), result.objdata(),
                  result.objsize() ) ;
      ++_totalRecords ; // total num
      ++_bufferNumRecords ; // cur buff num
      _bufferEndOffset += result.objsize() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextBase::appendObjs( const CHAR * pObjBuff, INT32 len,
                                      INT32 num, BOOLEAN needAliened )
   {
      INT32 rc = SDB_OK ;

      if ( !_isOpened )
      {
         _isOpened = TRUE ;
      }

      if ( 0 < len )
      {
         if ( needAliened )
         {
            _bufferEndOffset = ossAlign4( (UINT32)_bufferEndOffset ) ;
         }

         if ( _bufferEndOffset + len > _resultBufferSize )
         {
            rc = _reallocBuffer ( _bufferEndOffset + len ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to reallocate buffer for context, "
                        "rc: %d", rc ) ;
               goto error ;
            }
         }

         ossMemcpy ( &(_pResultBuffer[_bufferEndOffset]), pObjBuff, len ) ;
      }

      _totalRecords += num ; // total num
      _bufferNumRecords += num ; // cur buff num
      _bufferEndOffset += len ;

   done:
      return rc ; 
   error:
      goto done ;
   }

   void _rtnContextBase::_onDataEmpty ()
   {
      if ( _canPrefetch() && 0 != _prefetchID )
      {
         SDB_BPSCB *bpsCB = pmdGetKRCB()->getBPSCB() ;
         if ( bpsCB->isPrefetchEnabled() )
         {
            _prefetchLock.lock_r() ;
            if ( SDB_OK == bpsCB->sendPrefechReq( bpsDataPref( _prefetchID,
                                                               this ) ) )
            {
               _waitPrefetchNum.inc() ;
            }
            else
            {
               _prefetchLock.release_r() ;
            }
         }
      }
   }

   INT32 _rtnContextBase::prefetch( pmdEDUCB * cb, UINT32 prefetchID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;
      BOOLEAN againTry = FALSE ;
      UINT32 timeout = 0 ;

      while ( timeout < OSS_ONE_SEC )
      {
         if ( prefetchID != _prefetchID )
         {
            goto done ;
         }
         rc = _dataLock.lock_w( 100 ) ;
         if ( SDB_OK == rc )
         {
            locked = TRUE ;
            _isInPrefetch = TRUE ;
            if ( _pPrefWatcher )
            {
               _pPrefWatcher->ntyBegin() ;
            }
            break ;
         }
         else if ( rc && SDB_TIMEOUT != rc )
         {
            goto error ;
         }
         timeout += 100 ;
      }

      if ( FALSE == locked )
      {
         goto error ;
      }

      if ( prefetchID != _prefetchID )
      {
         goto done ;
      }

      if ( !isOpened() || eof() || !isEmpty() )
      {
         goto done ;
      }

      if ( _pMonAppCB && cb->getID() != eduID() )
      {
         cb->getMonAppCB()->reset() ;
      }
      rc = _prepareData( cb ) ;
      _prefetchRet = rc ;
      if ( rc && SDB_DMS_EOC != rc )
      {
         PD_LOG( PDWARNING, "Prepare data failed, rc: %d", rc ) ;
      }

      if ( _pMonAppCB && cb->getID() != eduID() )
      {
         *_pMonAppCB += *cb->getMonAppCB() ;
         cb->getMonAppCB()->reset() ;
      }

      if ( SDB_OK == rc && isEmpty() && isOpened() && !eof() &&
           SDB_OK == pmdGetKRCB()->getBPSCB()->sendPrefechReq(
                     bpsDataPref( ++_prefetchID, this ), TRUE ) )
      {
         _waitPrefetchNum.inc() ;
         againTry = TRUE ;
      }

   done:
      pmdGetKRCB()->getBPSCB()->_idlePrefAgentNum.inc() ;
      if ( locked )
      {
         _isInPrefetch = FALSE ;
         if ( _pPrefWatcher )
         {
            _pPrefWatcher->ntyEnd() ;
         }
         _dataLock.release_w() ;
      }
      _waitPrefetchNum.dec() ;
      if ( FALSE == againTry )
      {
         _prefetchLock.release_r() ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextBase::getMore( INT32 maxNumToReturn,
                                   rtnContextBuf &buffObj,
                                   pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;

      buffObj.release() ;

      if ( !isOpened() )
      {
         rc = SDB_DMS_CONTEXT_IS_CLOSE ;
         goto error ;
      }
      else if ( eof() && isEmpty() )
      {
         rc = SDB_DMS_EOC ;
         _isOpened = FALSE ;
         goto error ;
      }

      while ( TRUE )
      {
         rc = _dataLock.lock_r( OSS_ONE_SEC ) ;
         if ( SDB_OK == rc )
         {
            locked = TRUE ;
            break ;
         }
         else if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }
      }

      if ( 0 != _prefetchID )
      {
         ++_prefetchID ;
      }
      if ( _prefetchRet && SDB_DMS_EOC != _prefetchRet )
      {
         rc = _prefetchRet ;
         PD_LOG( PDWARNING, "Occur error in prefetch, rc: %d", rc ) ;
         goto error ;
      }

      if ( isEmpty() && !eof() )
      {
         rc = _prepareData( cb ) ;
         if ( rc && SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "Prepare data failed, rc: %d", rc ) ;
            goto error ;
         }
      }

      if ( !isEmpty() )
      {
         _bufferCurrentOffset = ossAlign4( (UINT32)_bufferCurrentOffset ) ;
         buffObj._pOrgBuff = _pResultBuffer ;
         buffObj._pBuff = &_pResultBuffer[ _bufferCurrentOffset ] ;
         buffObj._startFrom = _totalRecords - _bufferNumRecords ;

         if ( maxNumToReturn < 0 )
         {
            buffObj._buffSize = _bufferEndOffset - _bufferCurrentOffset ;
            buffObj._recordNum = _bufferNumRecords ;
            _bufferCurrentOffset = _bufferEndOffset ;
            _bufferNumRecords = 0 ;
         }
         else
         {
            INT32 prevCurOffset = _bufferCurrentOffset ;
            while ( _bufferCurrentOffset < _bufferEndOffset &&
                    maxNumToReturn > 0 )
            {
               try
               {
                  BSONObj obj( &_pResultBuffer[_bufferCurrentOffset] ) ;
                  _bufferCurrentOffset += ossAlign4( (UINT32)obj.objsize() ) ;
               }
               catch ( std::exception &e )
               {
                  PD_LOG( PDERROR, "Can't convert into BSON object: %s",
                          e.what() ) ;
                  rc = SDB_SYS ;
                  goto error ;
               }

               ++buffObj._recordNum ;
               --_bufferNumRecords ;
               --maxNumToReturn ;
            } // end while

            if ( _bufferCurrentOffset > _bufferEndOffset )
            {
               _bufferCurrentOffset = _bufferEndOffset ;
               SDB_ASSERT( 0 == _bufferNumRecords, "buffer num records must "
                           " be zero" ) ;
            }
            buffObj._buffSize = _bufferCurrentOffset - prevCurOffset ;
         }

         buffObj._reference( RTN_GET_REFERENCE( _pResultBuffer ), &_dataLock ) ;
         locked = FALSE ;
         rc = SDB_OK ;

         if ( isEmpty() && !eof() )
         {
            _bufferCurrentOffset = 0 ;
            _bufferEndOffset     = 0 ;

            _onDataEmpty() ;
         }
      }
      else
      {
         rc = SDB_DMS_EOC ;
         _isOpened = FALSE ;
      }

   done:
      if ( locked )
      {
         _dataLock.release_r() ;
      }
      return rc ;
   error:
      goto done ;
   }

   /*
      _rtnContextData implement
   */
   _rtnContextData::_rtnContextData( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID )
   {
      _dmsCB            = NULL ;
      _su               = NULL ;
      _mbContext        = NULL ;
      _plan             = NULL ;
      _scanType         = UNKNOWNSCAN ;
      _numToReturn      = -1 ;
      _numToSkip        = 0 ;

      _extentID         = DMS_INVALID_EXTENT ;
      _lastExtLID       = DMS_INVALID_EXTENT ;
      _segmentScan      = FALSE ;
      _indexBlockScan   = FALSE ;
      _scanner          = NULL ;
      _direction        = 0 ;
   }

   _rtnContextData::~_rtnContextData ()
   {
      if ( _scanner )
      {
         SDB_OSS_DEL _scanner ;
         _scanner = NULL ;
      }
      if ( _plan && -1 != contextID() )
      {
         _plan->release() ;
      }
      if ( _mbContext && _su )
      {
         _su->data()->releaseMBContext( _mbContext ) ;
      }
      if ( _dmsCB && _su && -1 != contextID() )
      {
         _dmsCB->suUnlock ( _su->CSID() ) ;
      }
   }

   RTN_CONTEXT_TYPE _rtnContextData::getType() const
   {
      return RTN_CONTEXT_DATA ;
   }

   void _rtnContextData::_toString( stringstream & ss )
   {
      if ( _su && _plan )
      {
         ss << ",Collection:" << _su->CSName() << "." << _plan->getName() ;
      }
      ss << ",ScanType:" << ( ( TBSCAN == _scanType ) ? "TBSCAN" : "IXSCAN" ) ;
      if ( _numToReturn > 0 )
      {
         ss << ",NumToReturn:" << _numToReturn ;
      }
      if ( _numToSkip > 0 )
      {
         ss << ",NumToSkip:" << _numToSkip ;
      }
   }

   INT32 _rtnContextData::_openIXScan( dmsStorageUnit *su,
                                       dmsMBContext *mbContext,
                                       optAccessPlan *plan,
                                       pmdEDUCB *cb,
                                       const BSONObj *blockObj,
                                       INT32 direction )
   {
      INT32 rc = SDB_OK ;
      rtnPredicateList *predList = NULL ;

      ixmIndexCB indexCB ( plan->getIndexCBExtent(), su->index(), NULL ) ;
      if ( !indexCB.isInitialized() )
      {
         PD_LOG ( PDERROR, "unable to get proper index control block" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( indexCB.getLogicalID() != plan->getIndexLID() )
      {
         PD_LOG( PDERROR, "Index[extent id: %d] logical id[%d] is not "
                 "expected[%d]", plan->getIndexCBExtent(),
                 indexCB.getLogicalID(), plan->getIndexLID() ) ;
         rc = SDB_IXM_NOTEXIST ;
         goto error ;
      }
      predList = plan->getPredList() ;
      SDB_ASSERT ( predList, "predList can't be NULL" ) ;

      if ( _scanner )
      {
         SDB_OSS_DEL _scanner ;
      }
      _scanner = SDB_OSS_NEW rtnIXScanner ( &indexCB, predList,
                                            su, cb ) ;
      if ( !_scanner )
      {
         PD_LOG ( PDERROR, "Unable to allocate memory for scanner" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      _scanner->setMonCtxCB ( &_monCtxCB ) ;

      if ( blockObj )
      {
         SDB_ASSERT( direction == 1 || direction == -1,
                     "direction must be 1 or -1" ) ;

         _direction = direction ;
         rc = _parseIndexBlocks( *blockObj, _indexBlocks, _indexRIDs ) ;
         PD_RC_CHECK( rc, PDERROR, "Parse index blocks failed, rc: %d", rc ) ;
         _indexBlockScan = TRUE ;

         if ( _indexBlocks.size() < 2 )
         {
            _hitEnd = TRUE ;
         }
      }

   done :
      return rc ;
   error :
      goto done ;
   }

   INT32 _rtnContextData::_openTBScan( dmsStorageUnit *su,
                                       dmsMBContext *mbContext, 
                                       optAccessPlan *plan, 
                                       pmdEDUCB * cb,
                                       const BSONObj *blockObj )
   {
      INT32 rc = SDB_OK ;

      if ( blockObj )
      {
         rc = _parseSegments( *blockObj, _segments ) ;
         PD_RC_CHECK( rc, PDERROR, "Parse segments[%s] failed, rc: %d",
                      blockObj->toString().c_str(), rc ) ;

         _segmentScan = TRUE ;
         _extentID = _segments.size() > 0 ? *_segments.begin() :
                     DMS_INVALID_EXTENT ;
      }
      else
      {
         _extentID = mbContext->mb()->_firstExtentID ;
      }

      if ( DMS_INVALID_EXTENT == _extentID )
      {
         _hitEnd = TRUE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextData::open( dmsStorageUnit *su, dmsMBContext *mbContext,
                                optAccessPlan *plan, pmdEDUCB *cb,
                                const BSONObj &selector, INT64 numToReturn,
                                INT64 numToSkip,
                                const BSONObj *blockObj,
                                INT32 direction )
   {
      INT32 rc = SDB_OK ;

      SDB_ASSERT( su && mbContext && plan, "Invalid param" ) ;

      if ( _isOpened )
      {
         rc = SDB_DMS_CONTEXT_IS_OPEN ;
         goto error ;
      }

      rc = mbContext->mbLock( SHARED ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

      if ( !dmsAccessAndFlagCompatiblity ( mbContext->mb()->_flag,
                                           DMS_ACCESS_TYPE_QUERY ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  mbContext->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      _isOpened = TRUE ;
      _hitEnd = FALSE ;

      if ( TBSCAN == plan->getScanType() )
      {
         rc = _openTBScan( su, mbContext, plan, cb, blockObj ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to open tbscan, rc: %d", rc ) ;
      }
      else if ( IXSCAN == plan->getScanType() )
      {
         rc = _openIXScan( su, mbContext, plan, cb, blockObj, direction ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to open ixscan, rc: %d", rc ) ;
      }
      else
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Unknow scan type: %d", plan->getScanType() ) ;
         goto error ;
      }

      if ( !selector.isEmpty() )
      {
         try
         {
            rc = _selector.loadPattern ( selector ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Invalid pattern is detected for select: %s: %s",
                     selector.toString().c_str(), e.what() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         PD_RC_CHECK( rc, PDERROR, "Invalid pattern is detected for select: "
                      "%s, rc: %d", selector.toString().c_str(), rc ) ;
      }
      _matcher = &( plan->getMatcher() ) ;

      _dmsCB = pmdGetKRCB()->getDMSCB() ;
      _su = su ;
      _mbContext = mbContext ;
      _plan = plan ;
      _scanType = plan->getScanType() ;
      _numToReturn = numToReturn ;
      _numToSkip = numToSkip > 0 ? numToSkip : 0 ;

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

   done:
      mbContext->mbUnlock() ;
      return rc ;
   error:
      _isOpened = FALSE ;
      _hitEnd = TRUE ;
      goto done ;
   }

   INT32 _rtnContextData::openTraversal( dmsStorageUnit *su,
                                         dmsMBContext *mbContext,
                                         optAccessPlan *plan,
                                         rtnIXScanner *scanner,
                                         pmdEDUCB *cb,
                                         const BSONObj &selector,
                                         INT64 numToReturn,
                                         INT64 numToSkip )
   {
      INT32 rc = SDB_OK ;

      SDB_ASSERT( su && mbContext && plan && scanner, "Invalid param" ) ;

      if ( _isOpened )
      {
         rc = SDB_DMS_CONTEXT_IS_OPEN ;
         goto error ;
      }
      if ( IXSCAN != plan->getScanType() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "Open traversal must IXSCAN" ) ;
         goto error ;
      }

      rc = mbContext->mbLock( SHARED ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

      if ( !dmsAccessAndFlagCompatiblity ( mbContext->mb()->_flag,
                                           DMS_ACCESS_TYPE_QUERY ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  mbContext->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      if ( _scanner )
      {
         SDB_OSS_DEL _scanner ;
      }
      _scanner = scanner ;

      if ( !selector.isEmpty() )
      {
         try
         {
            rc = _selector.loadPattern ( selector ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Invalid pattern is detected for select: %s: %s",
                     selector.toString().c_str(), e.what() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         PD_RC_CHECK( rc, PDERROR, "Invalid pattern is detected for select: "
                      "%s, rc: %d", selector.toString().c_str(), rc ) ;
      }
      _matcher = &( plan->getMatcher() ) ;

      _dmsCB = pmdGetKRCB()->getDMSCB() ;
      _su = su ;
      _mbContext = mbContext ;
      _plan = plan ;
      _scanType = plan->getScanType() ;
      _numToReturn = numToReturn ;
      _numToSkip = numToSkip > 0 ? numToSkip : 0 ;

      _isOpened = TRUE ;
      _hitEnd = FALSE ;

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

   done:
      mbContext->mbUnlock() ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextData::_prepareData( pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;

      if ( TBSCAN == _scanType )
      {
         rc = _prepareByTBScan( cb ) ;
      }
      else if ( IXSCAN == _scanType )
      {
         rc = _prepareByIXScan( cb ) ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
      }

      return rc ;
   }

   INT32 _rtnContextData::_prepareByTBScan( pmdEDUCB * cb )
   {
      INT32 rc                = SDB_OK ;
      mthMatcher *matcher     = NULL ;
      mthSelector *selector   = NULL ;
      monAppCB * pMonAppCB    = cb ? cb->getMonAppCB() : NULL ;
      BOOLEAN hasLocked       = _mbContext->isMBLock() ;

      if ( _matcher && _matcher->isInitialized() && !_matcher->isMatchesAll() )
      {
         matcher = _matcher ;
      }
      if ( _selector.isInitialized() )
      {
         selector = &_selector ;
      }

      dmsRecordID recordID ;
      ossValuePtr recordDataPtr = 0 ;

      if ( DMS_INVALID_EXTENT == _extentID )
      {
         SDB_ASSERT( FALSE, "extentID can't be INVALID" ) ;
         _hitEnd = TRUE ;
         rc = SDB_DMS_EOC ;
         goto error ;
      }

      while ( isEmpty() )
      {
         if ( eduID() != cb->getID() && !isOpened() )
         {
            rc = SDB_DMS_CONTEXT_IS_CLOSE ;
            goto error ;
         }

         dmsExtScanner extScanner( _su->data(), _mbContext, matcher, _extentID,
                                   DMS_ACCESS_TYPE_FETCH, _numToReturn,
                                   _numToSkip ) ;

         while ( SDB_OK == ( rc = extScanner.advance( recordID,
                                                      recordDataPtr,
                                                      cb ) ) )
         {
            try
            {
               BSONObj selObj ;
               BSONObj obj( (const CHAR*)recordDataPtr ) ;
               if ( selector )
               {
                  rc = selector->select( obj, selObj ) ;
                  PD_RC_CHECK( rc, PDERROR, "Failed to build select record,"
                               "src obj: %s, rc: %d", obj.toString().c_str(),
                               rc ) ;
               }
               else
               {
                  selObj = obj ;
               }
               rc = append( selObj ) ;
               PD_RC_CHECK( rc, PDERROR, "Append obj[%s] failed, rc: %d",
                            selObj.toString().c_str(), rc ) ;
            }
            catch ( std::exception &e )
            {
               PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
               rc = SDB_SYS ;
               goto error ;
            }
            DMS_MON_OP_COUNT_INC( pMonAppCB, MON_SELECT, 1 ) ;
            if ( _numToReturn > 0 )
            {
               --_numToReturn ;
            }
         } // end while

         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "Extent scanner failed, rc: %d", rc ) ;
            goto error ;
         }

         _numToReturn = extScanner.getMaxRecords() ;
         _numToSkip   = extScanner.getSkipNum() ;

         if ( 0 == _numToReturn )
         {
            _hitEnd = TRUE ;
            break ;
         }

         if ( _segmentScan )
         {
            if ( DMS_INVALID_EXTENT == extScanner.nextExtentID() ||
                 _su->data()->extent2Segment( *_segments.begin() ) !=
                 _su->data()->extent2Segment( extScanner.nextExtentID() ) )
            {
               _segments.erase( _segments.begin() ) ;
               if ( _segments.size() > 0 )
               {
                  _extentID = *_segments.begin() ;
               }
               else
               {
                  _extentID = DMS_INVALID_EXTENT ;
               }
            }
            else
            {
               _extentID = extScanner.nextExtentID() ;
            }
         }
         else
         {
            _extentID = extScanner.nextExtentID() ;
         }
         _lastExtLID = extScanner.curExtent()->_logicID ;
         if ( DMS_INVALID_EXTENT == _extentID )
         {
            _hitEnd = TRUE ;
            break ;
         }
         if ( !hasLocked )
         {
            _mbContext->pause() ;
         }

      } // end while

      if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }

   done:
      if ( !hasLocked )
      {
         _mbContext->pause() ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextData::_prepareByIXScan( pmdEDUCB *cb )
   {
      INT32 rc                   = SDB_OK ;
      rtnIXScanner *scanner      = _scanner ;
      mthMatcher *matcher        = NULL ;
      mthSelector *selector      = NULL ;
      monAppCB * pMonAppCB       = cb ? cb->getMonAppCB() : NULL ;
      BOOLEAN hasLocked          = _mbContext->isMBLock() ;

      dmsRecordID rid ;
      BSONObj dataRecord ;

      if ( _matcher && _matcher->isInitialized() && !_matcher->isMatchesAll() )
      {
         matcher = _matcher ;
      }
      if ( _selector.isInitialized() )
      {
         selector = &_selector ;
      }

      dmsRecordID recordID ;
      ossValuePtr recordDataPtr = 0 ;

      while ( isEmpty() )
      {
         if ( eduID() != cb->getID() && !isOpened() )
         {
            rc = SDB_DMS_CONTEXT_IS_CLOSE ;
            goto error ;
         }

         dmsIXSecScanner secScanner( _su->data(), _mbContext, matcher, scanner,
                                     DMS_ACCESS_TYPE_FETCH, _numToReturn,
                                     _numToSkip ) ;
         if ( _indexBlockScan )
         {
            secScanner.enableIndexBlockScan( _indexBlocks[0],
                                             _indexBlocks[1],
                                             _indexRIDs[0],
                                             _indexRIDs[1],
                                             _direction ) ;
         }

         while ( SDB_OK == ( rc = secScanner.advance( recordID, recordDataPtr,
                                                      cb ) ) )
         {
            try
            {
               BSONObj selObj ;
               BSONObj obj( (const CHAR*)recordDataPtr ) ;
               if ( selector )
               {
                  rc = selector->select( obj, selObj ) ;
                  PD_RC_CHECK( rc, PDERROR, "Failed to build select record,"
                               "src obj: %s, rc: %d", obj.toString().c_str(),
                               rc ) ;
               }
               else
               {
                  selObj = obj ;
               }
               rc = append( selObj ) ;
               PD_RC_CHECK( rc, PDERROR, "Append obj[%s] failed, rc: %d",
                            selObj.toString().c_str(), rc ) ;

               if ( buffEndOffset() + DMS_RECORD_MAX_SZ >
                    RTN_RESULTBUFFER_SIZE_MAX )
               {
                  secScanner.stop () ;
                  break ;
               }
            }
            catch ( std::exception &e )
            {
               PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
               rc = SDB_SYS ;
               goto error ;
            }
            DMS_MON_OP_COUNT_INC( pMonAppCB, MON_SELECT, 1 ) ;
         }

         if ( rc && SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "Extent scanner failed, rc: %d", rc ) ;
            goto error ;
         }

         _numToReturn = secScanner.getMaxRecords() ;
         _numToSkip   = secScanner.getSkipNum() ;

         if ( 0 == _numToReturn )
         {
            _hitEnd = TRUE ;
            break ;
         }

         if ( secScanner.eof() )
         {
            if ( _indexBlockScan )
            {
               _indexBlocks.erase( _indexBlocks.begin() ) ;
               _indexBlocks.erase( _indexBlocks.begin() ) ;
               _indexRIDs.erase( _indexRIDs.begin() ) ;
               _indexRIDs.erase( _indexRIDs.begin() ) ;
               if ( _indexBlocks.size() < 2 )
               {
                  _hitEnd = TRUE ;
                  break ;
               }
            }
            else
            {
               _hitEnd = TRUE ;
               break ;
            }
         }

         if ( !hasLocked )
         {
            _mbContext->pause() ;
         }
      } // end while

      if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }

   done :
      if ( !hasLocked )
      {
         _mbContext->pause() ;
      }
      return rc ;
   error :
      goto done ;
   }

   INT32 _rtnContextData::_parseSegments( const BSONObj &obj,
                                          vector< dmsExtentID > &segments )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      segments.clear() ;

      BSONObjIterator it ( obj ) ;
      while ( it.more() )
      {
         ele = it.next() ;
         if ( NumberInt != ele.type() )
         {
            PD_LOG( PDWARNING, "Datablocks[%s] value type[%d] is not NumberInt",
                    obj.toString().c_str(), ele.type() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         segments.push_back( ele.numberInt() ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextData::_parseRID( const BSONElement & ele,
                                     dmsRecordID & rid )
   {
      INT32 rc = SDB_OK ;
      rid.reset() ;

      if ( ele.eoo() )
      {
         goto done ;
      }
      else if ( Array != ele.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDWARNING, "Field[%s] type is not Array",
                 ele.toString().c_str() ) ;
         goto error ;
      }
      else
      {
         UINT32 count = 0 ;
         BSONElement ridEle ;
         BSONObjIterator it( ele.embeddedObject() ) ;
         while ( it.more() )
         {
            ridEle = it.next() ;
            if ( NumberInt != ridEle.type() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG( PDERROR, "RID type is not NumberInt in field[%s]",
                       ele.toString().c_str() ) ;
               goto error ;
            }
            if ( 0 == count )
            {
               rid._extent = ridEle.numberInt() ;
            }
            else if ( 1 == count )
            {
               rid._offset = ridEle.numberInt() ;
            }

            ++count ;
         }

         if ( 2 != count )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "RID array size[%d] is not 2", count ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextData::_parseIndexBlocks( const BSONObj &obj,
                                             vector< BSONObj > &indexBlocks,
                                             vector< dmsRecordID > &indexRIDs )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      BSONObj indexObj ;
      BSONObj startKey, endKey ;
      dmsRecordID startRID, endRID ;

      indexBlocks.clear() ;
      indexRIDs.clear() ;

      BSONObjIterator it ( obj ) ;
      while ( it.more() )
      {
         ele = it.next() ;
         if ( Object != ele.type() )
         {
            PD_LOG( PDWARNING, "Indexblocks[%s] value type[%d] is not Object",
                    obj.toString().c_str(), ele.type() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         indexObj = ele.embeddedObject() ;
         rc = rtnGetObjElement( indexObj, FIELD_NAME_STARTKEY, startKey ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s] from obj[%s], "
                      "rc: %d", FIELD_NAME_STARTKEY,
                      indexObj.toString().c_str(), rc ) ;
         rc = rtnGetObjElement( indexObj, FIELD_NAME_ENDKEY, endKey ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s] from obj[%s], "
                      "rc: %d", FIELD_NAME_ENDKEY,
                      indexObj.toString().c_str(), rc ) ;
         rc = _parseRID( indexObj.getField( FIELD_NAME_STARTRID ), startRID ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to parse %s, rc: %d",
                      FIELD_NAME_STARTRID, rc ) ;

         rc = _parseRID( indexObj.getField( FIELD_NAME_ENDRID ), endRID ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to parse %s, rc: %d",
                      FIELD_NAME_ENDRID, rc ) ;

         indexBlocks.push_back( rtnNullKeyNameObj( startKey ).getOwned() ) ;
         indexBlocks.push_back( rtnNullKeyNameObj( endKey ).getOwned() ) ;

         indexRIDs.push_back( startRID ) ;
         indexRIDs.push_back( endRID ) ;
      }

      if ( indexBlocks.size() != indexRIDs.size() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "block array size is not the same with rid array" ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _rtnContextParaData implement
   */
   _rtnContextParaData::_rtnContextParaData( INT64 contextID, UINT64 eduID )
   :_rtnContextData( contextID, eduID )
   {
      _isParalled = FALSE ;
      _curIndex   = 0 ;
      _step       = 1 ;
   }

   _rtnContextParaData::~_rtnContextParaData()
   {
      vector< rtnContextData* >::iterator it = _vecContext.begin() ;
      while ( it != _vecContext.end() )
      {
         (*it)->_close () ;
         ++it ;
      }
      it = _vecContext.begin() ;
      while ( it != _vecContext.end() )
      {
         (*it)->waitForPrefetch() ;
         SDB_OSS_DEL (*it) ;
         ++it ;
      }
      _vecContext.clear () ;
   }

   RTN_CONTEXT_TYPE _rtnContextParaData::getType () const
   {
      return RTN_CONTEXT_PARADATA ;
   }

   INT32 _rtnContextParaData::open( dmsStorageUnit *su, dmsMBContext *mbContext,
                                    optAccessPlan *plan, pmdEDUCB *cb,
                                    const BSONObj &selector, INT64 numToReturn,
                                    INT64 numToSkip, const BSONObj *blockObj,
                                    INT32 direction )
   {
      INT32 rc = SDB_OK ;
      _step = pmdGetKRCB()->getOptionCB()->maxSubQuery() ;
      if ( 0 == _step )
      {
         _step = 1 ;
      }

      rc = _rtnContextData::open( su, mbContext, plan, cb, selector,
                                  numToReturn, numToSkip, blockObj,
                                  direction ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( eof() )
      {
         goto done ;
      }

      if ( TBSCAN == _scanType && FALSE == _segmentScan )
      {
         rc = _su->getSegExtents( NULL, _segments, mbContext ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get segment extent, rc: %d",
                      rc ) ;
         if ( _segments.size() <= 1 )
         {
            _segments.clear() ;
            goto done ;
         }
         _segmentScan = TRUE ;
      }
      else if ( IXSCAN == _scanType && FALSE == _indexBlockScan )
      {
         rc = rtnGetIndexSeps( plan, su, mbContext, cb, _indexBlocks,
                               _indexRIDs ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get index seperations, rc: %d",
                      rc ) ;
         if ( _indexBlocks.size() <= 2 )
         {
            _indexBlocks.clear() ;
            _indexRIDs.clear() ;
            goto done ;
         }
         _indexBlockScan = TRUE ;
         _direction = 1 ;
      }

      if ( ( _segmentScan && _segments.size() <= 1 ) ||
           ( _indexBlockScan && _indexBlocks.size() <= 2 ) )
      {
         goto done ;
      }

      _isParalled = TRUE ;
      mbContext->mbUnlock() ;

      if ( numToReturn > 0 && numToSkip > 0 )
      {
         numToReturn += numToSkip ;
      }

      while ( NULL != ( blockObj = _nextBlockObj() ) )
      {
         rc = _openSubContext( blockObj, selector, cb, numToReturn ) ;
         if ( rc )
         {
            goto error ;
         }
      }

      _checkAndPrefetch () ;

   done:
      mbContext->mbUnlock() ;
      return rc ;
   error:
      goto done ;
   }

   void _rtnContextParaData::_removeSubContext( rtnContextData *pContext )
   {
      vector< rtnContextData* >::iterator it = _vecContext.begin() ;
      while ( it != _vecContext.end() )
      {
         if ( *it == pContext )
         {
            pContext->waitForPrefetch() ;
            SDB_OSS_DEL pContext ;
            _vecContext.erase( it ) ;
            break ;
         }
         ++it ;
      }
   }

   INT32 _rtnContextParaData::_openSubContext( const BSONObj *blockObj,
                                               const BSONObj &selector,
                                               _pmdEDUCB *cb,
                                               INT64 numToReturn )
   {
      INT32 rc = SDB_OK ;

      dmsMBContext *mbContext = NULL ;
      rtnContextData *dataContext = NULL ;

      rc = _su->data()->getMBContext( &mbContext, _plan->getName(), -1 ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get dms mb context, rc: %d", rc ) ;

      dataContext = SDB_OSS_NEW rtnContextData( -1, eduID() ) ;
      if ( !dataContext )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "Alloc sub context outof memory" ) ;
         goto error ;
      }
      _vecContext.push_back( dataContext ) ;

      rc = dataContext->open( _su, mbContext, _plan, cb, selector,
                              numToReturn, 0, blockObj, _direction ) ;
      PD_RC_CHECK( rc, PDERROR, "Open sub context failed, blockObj: %s, "
                   "rc: %d", blockObj->toString().c_str(), rc ) ;

      mbContext = NULL ;

      dataContext->enablePrefetch ( cb, &_prefWather ) ;
      if ( cb->getMonConfigCB()->timestampON )
      {
         dataContext->getMonCB()->recordStartTimestamp() ;
      }
      dataContext->getSelector().setStringOutput(
         getSelector().getStringOutput() ) ;

   done :
      return rc ;
   error :
      if ( mbContext )
      {
         _su->data()->releaseMBContext( mbContext ) ;
      }
      goto done ;
   }

   INT32 _rtnContextParaData::_checkAndPrefetch ()
   {
      INT32 rc = SDB_OK ;
      rtnContextData *pContext = NULL ;
      vector< rtnContextData* >::iterator it = _vecContext.begin() ;
      while ( it != _vecContext.end() )
      {
         pContext = *it ;
         if ( pContext->eof() && pContext->isEmpty() )
         {
            pContext->waitForPrefetch() ;
            SDB_OSS_DEL pContext ;
            it = _vecContext.erase( it ) ;
            continue ;
         }
         else if ( !pContext->isEmpty() ||
                   pContext->_getWaitPrefetchNum() > 0 )
         {
            ++it ;
            continue ;
         }
         pContext->_onDataEmpty() ;
         ++it ;
      }

      if ( _vecContext.size() == 0 )
      {
         rc = SDB_DMS_EOC ;
         _hitEnd = TRUE ;
      }

      return rc ;
   }

   const BSONObj* _rtnContextParaData::_nextBlockObj ()
   {
      BSONArrayBuilder builder ;
      UINT32 curIndex = _curIndex ;

      if ( _curIndex >= _step ||
           ( TBSCAN == _scanType && _curIndex >= _segments.size() ) ||
           ( IXSCAN == _scanType && _curIndex + 1 >= _indexBlocks.size() ) )
      {
         return NULL ;
      }
      ++_curIndex ;

      if ( TBSCAN == _scanType )
      {
         while ( curIndex < _segments.size() )
         {
            builder.append( _segments[curIndex] ) ;
            curIndex += _step ;
         }
      }
      else if ( IXSCAN == _scanType )
      {
         while ( curIndex + 1 < _indexBlocks.size() )
         {
            builder.append( BSON( FIELD_NAME_STARTKEY <<
                                  _indexBlocks[curIndex] <<
                                  FIELD_NAME_ENDKEY <<
                                  _indexBlocks[curIndex+1] <<
                                  FIELD_NAME_STARTRID <<
                                  BSON_ARRAY( _indexRIDs[curIndex]._extent <<
                                              _indexRIDs[curIndex]._offset ) <<
                                  FIELD_NAME_ENDRID <<
                                  BSON_ARRAY( _indexRIDs[curIndex+1]._extent <<
                                              _indexRIDs[curIndex+1]._offset )
                                 )
                            ) ;
            curIndex += _step ;
         }
      }
      else
      {
         return NULL ;
      }

      _blockObj = builder.arr() ;
      return &_blockObj ;
   }

   INT32 _rtnContextParaData::_getSubCtxWithData( rtnContextData **ppContext,
                                                  _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      UINT32 index = 0 ;

      do
      {
         index = 0 ;
         _prefWather.reset() ;

         while ( index < _vecContext.size() )
         {
            rc = _vecContext[index]->prefetchResult () ;
            if ( rc && SDB_DMS_EOC != rc )
            {
               goto error ;
            }
            rc = SDB_OK ;

            if ( !_vecContext[index]->isEmpty() &&
                 !_vecContext[index]->_isInPrefetching () )
            {
               *ppContext = _vecContext[index] ;
               goto done ;
            }
            ++index ;
         }
      } while ( _prefWather.waitDone( OSS_ONE_SEC * 5 ) > 0 ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextParaData::_getSubContextData( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      rtnContextData *pContext = NULL ;
      INT64 maxReturnNum = -1 ;

      while ( isEmpty() && 0 != _numToReturn )
      {
         pContext = NULL ;
         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }

         if ( _numToSkip <= 0 )
         {
            rc = _getSubCtxWithData( &pContext, cb ) ;
            if ( rc )
            {
               goto error ;
            }
         }

         if ( !pContext && _vecContext.size() > 0 )
         {
            pContext = _vecContext[0] ;
         }

         if ( pContext )
         {
            rtnContextBuf buffObj ;
            if ( _numToSkip > 0 )
            {
               maxReturnNum = _numToSkip ;
            }
            else
            {
               maxReturnNum = -1 ;
            }

            rc = pContext->getMore( maxReturnNum, buffObj, cb ) ;
            if ( rc )
            {
               _removeSubContext( pContext ) ;
               if ( SDB_DMS_EOC != rc )
               {
                  PD_LOG( PDERROR, "Failed to get more from sub context, "
                          "rc: %d", rc ) ;
                  goto error ;
               }
               continue ;
            }

            if ( _numToSkip > 0 )
            {
               _numToSkip -= buffObj.recordNum() ;
               continue ;
            }

            if ( _numToReturn > 0 && buffObj.recordNum() > _numToReturn )
            {
               buffObj.truncate( _numToReturn ) ;
            }
            rc = appendObjs( buffObj.data(), buffObj.size(),
                             buffObj.recordNum() ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to add objs, rc: %d", rc ) ;
            if ( _numToReturn > 0 )
            {
               _numToReturn -= buffObj.recordNum() ;
            }
         } // end if ( pContext )

         if ( SDB_OK != _checkAndPrefetch() )
         {
            break ;
         }
      } // while ( isEmpty() && 0 != _numToReturn )

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

      if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextParaData::_prepareData( pmdEDUCB * cb )
   {
      if ( !_isParalled )
      {
         return _rtnContextData::_prepareData( cb ) ;
      }
      else
      {
         return _getSubContextData( cb ) ;
      }
   }

   /*
      _rtnContextTemp implement
   */
   _rtnContextTemp::_rtnContextTemp( INT64 contextID, UINT64 eduID )
   :_rtnContextData( contextID, eduID )
   {
   }

   _rtnContextTemp::~_rtnContextTemp ()
   {
      if ( _dmsCB && _mbContext )
      {
         _dmsCB->getTempCB()->release( _mbContext ) ;
      }
   }

   RTN_CONTEXT_TYPE _rtnContextTemp::getType () const
   {
      return RTN_CONTEXT_TEMP ;
   }

   /*
      _rtnContextQGM implement
   */
   _rtnContextQGM::_rtnContextQGM( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID )
   {
      _accPlan    = NULL ;
   }

   _rtnContextQGM::~_rtnContextQGM ()
   {
      if ( NULL != _accPlan )
      {
         SAFE_OSS_DELETE( _accPlan ) ;
         _accPlan = NULL ;
      }
   }

   RTN_CONTEXT_TYPE _rtnContextQGM::getType () const
   {
      return RTN_CONTEXT_QGM ;
   }

   INT32 _rtnContextQGM::open( qgmPlanContainer *accPlan )
   {
      INT32 rc = SDB_OK ;

      if ( _isOpened )
      {
         rc = SDB_DMS_CONTEXT_IS_OPEN ;
         goto error ;
      }
      if ( NULL == accPlan )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _accPlan = accPlan ;
      _isOpened = TRUE ;
      _hitEnd = FALSE ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextQGM::_prepareData( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj obj ;
      INT32 index = 0 ;
      monAppCB *pMonAppCB = cb ? cb->getMonAppCB() : NULL ;

      for ( ; index < RTN_CONTEXT_GETNUM_ONCE ; ++index )
      {
         try
         {
            rc = _accPlan->fetch( obj ) ;
         }
         catch( std::exception &e )
         {
            PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         if ( SDB_DMS_EOC == rc )
         {
            _hitEnd = TRUE ;
            break ;
         }
         else if ( rc )
         {
            PD_LOG( PDERROR, "Qgm fetch failed, rc: %d", rc ) ;
            goto error ;
         }

         rc = append( obj ) ;
         PD_RC_CHECK( rc, PDERROR, "Append obj[%s] failed, rc: %d",
                      obj.toString().c_str(), rc ) ;

         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_SELECT, 1 ) ;

         if ( buffEndOffset() + DMS_RECORD_MAX_SZ > RTN_RESULTBUFFER_SIZE_MAX )
         {
            break ;
         }
      }

      if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _rtnContextDump implement
   */
   _rtnContextDump::_rtnContextDump( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID )
   {
      _numToReturn = -1 ;
      _numToSkip   = 0 ;
   }

   _rtnContextDump::~_rtnContextDump()
   {
   }

   RTN_CONTEXT_TYPE _rtnContextDump::getType () const
   {
      return RTN_CONTEXT_DUMP ;
   }

   INT32 _rtnContextDump::open ( const BSONObj &selector,
                                 const BSONObj &matcher,
                                 INT64 numToReturn,
                                 INT64 numToSkip )
   {
      INT32 rc = SDB_OK ;

      if ( _isOpened )
      {
         rc = SDB_DMS_CONTEXT_IS_OPEN ;
         goto error ;
      }

      if ( !selector.isEmpty() )
      {
         try
         {
            rc = _selector.loadPattern ( selector ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed loading pattern for selector: %s: %s",
                     selector.toString().c_str(), e.what() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed loading selector: %s, rc = %d",
                     selector.toString().c_str(), rc ) ;
            goto error ;
         }
      }
      if ( !matcher.isEmpty() )
      {
         try
         {
            rc = newMatcher() ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to create new matcher, rc: %d", rc ) ;
               goto error ;
            }
            rc = _matcher->loadPattern ( matcher ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed loading pattern for matcher: %s: %s",
                     matcher.toString().c_str(), e.what() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed loading matcher: %s, rc = %d",
                     matcher.toString().c_str(), rc ) ;
            goto error ;
         }
      }

      _numToReturn = numToReturn ;
      _numToSkip   = numToSkip > 0 ? numToSkip : 0 ;

      _isOpened = TRUE ;
      _hitEnd = FALSE ;

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextDump::monAppend( const BSONObj & result )
   {
      INT32 rc = SDB_OK ;
      BSONObj tempObj ;
      BOOLEAN isMatch = TRUE ;

      if ( 0 == _numToReturn )
      {
         goto done ;
      }

      try
      {
         if ( _matcher && _matcher->isInitialized() )
         {
            rc = _matcher->matches ( result, isMatch ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to match record, rc: %d", rc ) ;
         }
         if ( isMatch )
         {
            if ( _numToSkip > 0 )
            {
               --_numToSkip ;
               goto done ;
            }

            if ( _selector.isInitialized() )
            {
               rc = _selector.select ( result, tempObj ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to build select record, "
                            "rc: %d", rc ) ;
            }
            else
            {
               tempObj = result ;
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to match or select from object: %s",
                  e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( isMatch )
      {
         rc = append ( tempObj ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to append to context, rc = %d",
                      rc ) ;

         if ( _numToReturn > 0 )
         {
            --_numToReturn ;
         }
      }

   done :
      return rc ;
   error :
      goto done ;
   }

   INT32 _rtnContextDump::_prepareData( pmdEDUCB * cb )
   {
      _hitEnd = TRUE ;
      return SDB_DMS_EOC ;
   }

   /*
      _rtnContextCoord implement
   */
   _rtnContextCoord::_rtnContextCoord( INT64 contextID, UINT64 eduID,
                                       BOOLEAN preRead )
   :_rtnContextBase( contextID, eduID )
   {
      _numToReturn      = -1 ;
      _numToSkip        = 0 ;
      _netAgent         = NULL ;
      _preRead          = preRead ;
      _keyGen           = NULL ;
   }

   _rtnContextCoord::~_rtnContextCoord ()
   {
      pmdKRCB *krcb = pmdGetKRCB() ;
      pmdEDUMgr *eduMgr = krcb->getEDUMgr() ;
      pmdEDUCB *cb = eduMgr->getEDUByID( eduID() ) ;


      killSubContexts( cb ) ;
      SAFE_OSS_DELETE( _keyGen ) ;
   }

   void _rtnContextCoord::killSubContexts( pmdEDUCB * cb )
   {
      UINT32 tid = 0 ;
      coordSubContext *pSubContext  = NULL ;

      if ( cb )
      {
         tid = cb->getTID() ;

         if ( _prepareNodeMap.size() > 0 )
         {
            _getPrepareNodesData( cb, TRUE ) ;
         }
         _prepareNodeMap.clear() ;
      }

      SUB_CONTEXT_MAP::iterator itSub = _subContextMap.begin() ;
      while ( _subContextMap.end() != itSub )
      {
         pSubContext = itSub->second ;
         _prepareContextMap.insert( EMPTY_CONTEXT_MAP::value_type(
                                    pSubContext->getRouteID().value,
                                    pSubContext ) ) ;
         ++itSub ;
      }
      _subContextMap.clear() ;

      EMPTY_CONTEXT_MAP::iterator it = _emptyContextMap.begin() ;
      while ( it != _emptyContextMap.end() )
      {
         _prepareContextMap.insert( EMPTY_CONTEXT_MAP::value_type(
                                    it->first,
                                    it->second ) ) ;
         ++it ;
      }
      _emptyContextMap.clear() ;

      if ( cb && !cb->isInterrupted() )
      {
         MsgOpKillContexts killMsg ;
         MsgRouteID routeID ;
         killMsg.header.messageLength = sizeof ( MsgOpKillContexts ) ;
         killMsg.header.opCode = MSG_BS_KILL_CONTEXT_REQ ;
         killMsg.header.TID = tid ;
         killMsg.header.routeID.value = 0;
         killMsg.ZERO = 0;
         killMsg.numContexts = 1 ;

         it = _prepareContextMap.begin() ;
         while ( it != _prepareContextMap.end() )
         {
            pSubContext = it->second ;
            routeID = pSubContext->getRouteID() ;
            killMsg.contextIDs[0] = pSubContext->getContextID() ;

            rtnCoordSendRequestToNode( (void*)&killMsg, routeID, _netAgent,
                                       cb, _prepareNodeMap ) ;

            ++it ;
         }

         if ( _prepareContextMap.size() > 0 )
         {
            REPLY_QUE replyQue ;
            rtnCoordGetReply( cb, _prepareNodeMap, replyQue,
                              MSG_BS_KILL_CONTEXT_RES ) ;
            while ( !replyQue.empty() )
            {
               SDB_OSS_FREE( replyQue.front() ) ;
               replyQue.pop() ;
            }
         }
      }

      it = _prepareContextMap.begin() ;
      while ( it != _prepareContextMap.end() )
      {
         pSubContext = it->second ;
         SDB_OSS_DEL pSubContext ;
         ++it ;
      }
      _prepareContextMap.clear() ;
   }

   RTN_CONTEXT_TYPE _rtnContextCoord::getType () const
   {
      return RTN_CONTEXT_COORD ;
   }

   INT32 _rtnContextCoord::open( const BSONObj &orderBy,
                                 const BSONObj &selector,
                                 INT64 numToReturn,
                                 INT64 numToSkip )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;

      if ( _isOpened )
      {
         rc = SDB_DMS_CONTEXT_IS_OPEN ;
         goto error ;
      }

      _netAgent = krcb->getCoordCB()->getRouteAgent() ;
      if ( NULL == _netAgent )
      {
         PD_LOG( PDERROR, "Net agent is null" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      _orderBy = orderBy.getOwned() ;
      _numToReturn = numToReturn ;
      _numToSkip = numToSkip > 0 ? numToSkip : 0 ;

      _keyGen = SDB_OSS_NEW _ixmIndexKeyGen( _orderBy ) ;
      PD_CHECK( _keyGen != NULL, SDB_OOM, error, PDERROR,
               "malloc failed!" ) ;
      if ( !selector.isEmpty() )
      {
         rc = _selector.loadPattern ( selector ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to load selector pattern:%d", rc ) ;
            goto error ;
         }
      }

      _isOpened = TRUE ;
      _hitEnd = FALSE ;

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextCoord::reopen ()
   {
      if ( _isOpened )
      {
         return SDB_DMS_CONTEXT_IS_OPEN ;
      }
      if ( !eof() || !isEmpty() )
      {
         return SDB_SYS ;
      }

      _isOpened = TRUE ;

      return SDB_OK ;
   }

   INT32 _rtnContextCoord::_send2EmptyNodes( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      MsgOpGetMore msgReq ;
      MsgRouteID routeID ;
      EMPTY_CONTEXT_MAP::iterator emptyIter ;

      msgFillGetMoreMsg( msgReq, cb->getTID(), -1, -1, 0 ) ;

      emptyIter = _emptyContextMap.begin() ;
      while( emptyIter != _emptyContextMap.end() )
      {
         if ( -1 == emptyIter->second->getContextID() )
         {
            SDB_OSS_DEL emptyIter->second ;
            _emptyContextMap.erase( emptyIter++ ) ;
            continue ;
         }

         routeID.value = emptyIter->first ;
         msgReq.contextID = emptyIter->second->getContextID() ;
         rc = rtnCoordSendRequestToNode( (void *)(&msgReq), routeID, _netAgent,
                                         cb, _prepareNodeMap ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to send getmore request to "
                     "node( groupID=%u, nodeID=%u, serviceID=%u, "
                     "contextID=%lld, rc=%d )",
                     routeID.columns.groupID,
                     routeID.columns.nodeID,
                     routeID.columns.serviceID,
                     msgReq.contextID, rc ) ;
            break ;
         }
         _prepareContextMap.insert( EMPTY_CONTEXT_MAP::value_type(
                                    emptyIter->first, emptyIter->second ) ) ;
         _emptyContextMap.erase( emptyIter++ ) ;
      }

      return rc;
   }

   INT32 _rtnContextCoord::_getPrepareNodesData( pmdEDUCB * cb,
                                                 BOOLEAN waitAll )
   {
      INT32 rc = SDB_OK ;
      REPLY_QUE replyQue ;

      if ( _prepareNodeMap.empty() )
      {
         goto done ;
      }
      else
      {
         CHAR *pData = NULL ;
         MsgOpReply *pReply = NULL ;

         rc = rtnCoordGetReply( cb, _prepareNodeMap, replyQue,
                                MSG_BS_GETMORE_RES, waitAll ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to get reply, rc: %d", rc ) ;
            goto error ;
         }

         while ( !replyQue.empty() )
         {
            pData = replyQue.front() ;
            pReply = ( MsgOpReply* )pData ;

            if ( pReply->header.messageLength < (INT32)sizeof( MsgOpReply ) )
            {
               _delPrepareContext( pReply->header.routeID ) ;
               rc = SDB_INVALIDARG ;
               PD_LOG ( PDERROR, "Get data failed, received invalid message "
                        "from node(groupID=%u, nodeID=%u, serviceID=%u, "
                        "messageLength=%d)",
                        pReply->header.routeID.columns.groupID,
                        pReply->header.routeID.columns.nodeID,
                        pReply->header.routeID.columns.serviceID,
                        pReply->header.messageLength ) ;
               break ;
            }
            else if ( SDB_OK != pReply->flags )
            {
               _delPrepareContext( pReply->header.routeID ) ;

               if ( SDB_DMS_EOC != pReply->flags )
               {
                  PD_LOG ( PDERROR, "Get data failed, failed to get data "
                           "from node (groupID=%u, nodeID=%u, serviceID=%u, "
                           "flag=%d)", pReply->header.routeID.columns.groupID,
                           pReply->header.routeID.columns.nodeID,
                           pReply->header.routeID.columns.serviceID,
                           pReply->flags ) ;
                  rc = pReply->flags ;
                  break ;
               }
               else
               {
                  SDB_OSS_FREE( pData ) ;
               }
            }
            else
            {
               rc = _appendSubData( pData ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to append the data, rc: %d", rc ) ;
                  break ;
               }
            }

            replyQue.pop() ;
         } // end while

         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      while ( !replyQue.empty() )
      {
         SDB_OSS_FREE( replyQue.front() ) ;
         replyQue.pop() ;
      }
      goto done ;
   }

   INT32 _rtnContextCoord::_prepareData( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;

      while ( TRUE )
      {
         rc = _getSubData() ;
         if ( SDB_OK == rc || SDB_DMS_EOC == rc )
         {
            goto done ;
         }
         else if ( SDB_RTN_COORD_CACHE_EMPTY != rc )
         {
            PD_LOG( PDERROR, "Failed to get sub data, rc: %d", rc ) ;
            goto error ;
         }

         if ( _emptyContextMap.size() == 0 &&
              _prepareContextMap.size() == 0 )
         {
            rc = SDB_SYS ;
            PD_LOG( PDERROR, "Empty context map can't be empty" ) ;
            goto error ;
         }

         rc = _send2EmptyNodes( cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Send request to empty nodes failed, rc: %d",
                      rc ) ;

         rc = _getPrepareNodesData( cb, requireOrder() ) ;
         PD_RC_CHECK( rc, PDERROR, "Get data from prepare nodes failed, rc: %d",
                      rc ) ;
      }

   done:
      if ( SDB_OK == rc && _numToReturn != 0 && _preRead )
      {
         _send2EmptyNodes( cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextCoord::_appendSubData( CHAR * pData )
   {
      INT32 rc = SDB_OK ;
      MsgOpReply *pReply = (MsgOpReply *)pData ;
      EMPTY_CONTEXT_MAP::iterator iter ;
      coordSubContext *pSubContext = NULL ;

      if ( (UINT32)pReply->header.opCode != MSG_BS_GETMORE_RES ||
           (UINT32)pReply->header.messageLength < sizeof( MsgOpReply ) )
      {
         rc = SDB_UNKNOWN_MESSAGE ;
         PD_LOG ( PDERROR, "Failed to append the data, invalid data"
                  "(opCode=%d, messageLength=%d)", pReply->header.opCode,
                  pReply->header.messageLength ) ;
         goto error ;
      }

      iter = _prepareContextMap.find( pReply->header.routeID.value ) ;
      if ( _prepareContextMap.end() == iter )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR, "Failed to append the data, no match context"
                  "(groupID=%u, nodeID=%u, serviceID=%u)",
                  pReply->header.routeID.columns.groupID,
                  pReply->header.routeID.columns.nodeID,
                  pReply->header.routeID.columns.serviceID ) ;
         goto error ;
      }

      pSubContext = iter->second ;
      SDB_ASSERT( pSubContext != NULL, "subContext can't be NULL" ) ;

      if ( pSubContext->getContextID() != pReply->contextID )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR, "Failed to append the data, no match context"
                  "(expectContextID=%lld, contextID=%lld)",
                  pSubContext->getContextID(), pReply->contextID ) ;
         goto error ;
      }

      pSubContext->appendData( pReply ) ;

      if ( !requireOrder() )
      {
         _prepareContextMap.erase( iter ) ;
         _subContextMap.insert( SUB_CONTEXT_MAP::value_type( _emptyKey,
                                pSubContext ) ) ;
      }
      else
      {
         coordOrderKey orderKey ;
         rc = pSubContext->getOrderKey( orderKey, _keyGen ) ;
         if ( rc != SDB_OK )
         {
            pSubContext->clearData() ;
            PD_LOG ( PDERROR, "Failed to get orderKey failed, rc: %d", rc ) ;
            goto error ;
         }
         _prepareContextMap.erase( iter ) ;
         _subContextMap.insert( SUB_CONTEXT_MAP::value_type( orderKey,
                                pSubContext ) ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextCoord::addSubContext( MsgRouteID routeID,
                                          SINT64 contextID )
   {
      INT32 rc = SDB_OK ;
      EMPTY_CONTEXT_MAP::iterator iter ;
      coordSubContext *pSubContext = NULL ;

      if ( !_isOpened || NULL == _netAgent )
      {
         rc = SDB_DMS_CONTEXT_IS_CLOSE ;
         goto error ;
      }

      iter = _emptyContextMap.find( routeID.value ) ;
      if ( iter != _emptyContextMap.end() )
      {
         rc = SDB_INVALIDARG;
         PD_LOG( PDERROR, "Repeat to add sub-context (groupID=%u, nodeID=%u, "
                 "serviceID=%u, oldContextID=%lld, newContextID=%lld)",
                 routeID.columns.groupID, routeID.columns.nodeID,
                 routeID.columns.serviceID, iter->second->getContextID(),
                 contextID ) ;
         goto error ;
      }

      pSubContext = SDB_OSS_NEW coordSubContext( routeID,
                                                 contextID,
                                                 _keyGen ) ;
      if ( NULL == pSubContext )
      {
         rc = SDB_OOM;
         PD_LOG ( PDERROR, "Failed to alloc memory" ) ;
         goto error ;
      }

      pSubContext->setOrderBy( _orderBy ) ;

      _emptyContextMap.insert( EMPTY_CONTEXT_MAP::value_type( routeID.value,
                               pSubContext ) ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextCoord::addSubContext( MsgOpReply * pReply,
                                          BOOLEAN & takeOver )
   {
      INT32 rc = SDB_OK ;
      takeOver = FALSE ;

      SDB_ASSERT ( NULL != pReply, "reply can't be NULL" ) ;

      rc = addSubContext( pReply->header.routeID, pReply->contextID ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( pReply->numReturned > 0 )
      {
         EMPTY_CONTEXT_MAP::iterator it ;
         it = _emptyContextMap.find( pReply->header.routeID.value ) ;
         SDB_ASSERT( it != _emptyContextMap.end(), "System error" ) ;

         _prepareContextMap.insert( EMPTY_CONTEXT_MAP::value_type(
                                    it->first, it->second ) ) ;
         _emptyContextMap.erase( it ) ;

         pReply->header.opCode = MSG_BS_GETMORE_RES ;
         rc = _appendSubData( (CHAR*)pReply ) ;
         if ( SDB_OK == rc )
         {
            takeOver = TRUE ;
         }
         else
         {
            PD_LOG( PDERROR, "Append sub data failed, rc: %d", rc ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void _rtnContextCoord::addSubDone( pmdEDUCB * cb )
   {
      _send2EmptyNodes( cb ) ;
   }

   INT32 _rtnContextCoord::_getSubData()
   {
      INT32 rc = SDB_OK ;

      if ( !requireOrder() )
      {
         rc = _getSubDataNormal() ;
      }
      else
      {
         rc = _getSubDataByOrder() ;
      }

      return rc ;
   }

   INT32 _rtnContextCoord::_getSubDataNormal ()
   {
      INT32 rc                      = SDB_OK ;
      SUB_CONTEXT_MAP::iterator iter ;
      coordSubContext *pSubContext  = NULL ;
      SINT32 recordNum              = 0 ;
      CHAR *pData                   = NULL ;

      while ( isEmpty() )
      {
         if ( 0 == _numToReturn )
         {
            _hitEnd = TRUE ;
            rc = SDB_DMS_EOC ;
            goto error ;
         }
         if ( _subContextMap.size() == 0 )
         {
            if ( _emptyContextMap.size() + _prepareContextMap.size() == 0 )
            {
               _hitEnd = TRUE ;
               rc = SDB_DMS_EOC ;
            }
            else
            {
               rc = SDB_RTN_COORD_CACHE_EMPTY ;
            }
            goto error ;
         }

         iter = _subContextMap.begin() ;
         pSubContext = iter->second ;
         recordNum = pSubContext->getRecordNum() ;

         if ( _numToSkip > 0 )
         {
            if ( _numToSkip >= recordNum )
            {
               rc = pSubContext->popAll() ;
               if ( rc != SDB_OK )
               {
                  PD_LOG ( PDERROR, "Failed to skip the data(rc=%d)", rc ) ;
                  goto error ;
               }
               _subContextMap.erase ( iter ) ;
               _emptyContextMap.insert( EMPTY_CONTEXT_MAP::value_type(
                                        pSubContext->getRouteID().value,
                                        pSubContext ) ) ;
               _numToSkip -= recordNum ;
               continue ;
            }
            else
            {
               rc = pSubContext->popN( _numToSkip ) ;
               if ( rc != SDB_OK )
               {
                  PD_LOG ( PDERROR, "Failed to skip the data(rc=%d)", rc ) ;
                  goto error ;
               }
               _numToSkip = 0 ;
            }
         }

         recordNum = pSubContext->getRecordNum() ;

         if ( ( _numToReturn < 0 || recordNum <= _numToReturn ) &&
              ( buffEndOffset() + pSubContext->getRemainLen() <=
                RTN_RESULTBUFFER_SIZE_MAX ) && !_selector.isInitialized() )
         {
            rc = appendObjs( pSubContext->front(),
                             (INT32)pSubContext->getRemainLen(), recordNum ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to append objs, rc: %d", rc ) ;

            rc = pSubContext->popAll() ;
            PD_RC_CHECK( rc, PDERROR, "Pop sub context all objs failed, rc: %d",
                         rc ) ;

            if ( _numToReturn > 0 )
            {
               _numToReturn -= recordNum ;
            }
         }
         else
         {
            while ( 0 != _numToReturn && recordNum > 0 )
            {
               pData = pSubContext->front() ;
               if ( NULL == pData )
               {
                  rc = SDB_SYS ;
                  PD_LOG ( PDERROR, "Failed to get the data, rc: %d", rc ) ;
                  goto error ;
               }

               try
               {
                  BSONObj boRecord( pData ) ;
                  BSONObj boSelected ;
                  BSONObj *boRealRecord = NULL ;
                  if ( !_selector.isInitialized() )
                  {
                     boRealRecord = &boRecord ;
                  }
                  else
                  {
                     rc = _selector.select( boRecord, boSelected ) ;
                     if ( SDB_OK != rc )
                     {
                        PD_LOG( PDERROR, "failed to select fields:%d", rc ) ;
                        goto error ;
                     }
                     boRealRecord = &boSelected ;
                  }

                  rc = append( *boRealRecord ) ;
                  PD_RC_CHECK( rc, PDERROR, "Append obj[%s] failed, rc: %d",
                               boRecord.toString().c_str(), rc ) ;
               }
               catch ( std::exception &e )
               {
                  rc = SDB_SYS ;
                  PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
                  goto error ;
               }

               rc = pSubContext->pop() ;
               PD_RC_CHECK( rc, PDERROR, "Failed to pop data, rc: %d", rc ) ;

               --recordNum ;
               if ( _numToReturn > 0 )
               {
                  --_numToReturn ;
               }

               if ( buffEndOffset() + DMS_RECORD_MAX_SZ >
                    RTN_RESULTBUFFER_SIZE_MAX )
               {
                  break ;
               }
            }
         }

         if ( pSubContext->getRecordNum() <= 0 )
         {
            _subContextMap.erase ( iter ) ;
            _emptyContextMap.insert( EMPTY_CONTEXT_MAP::value_type(
                                     pSubContext->getRouteID().value,
                                     pSubContext ) ) ;
         }
      } // end while

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

      if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_RTN_COORD_CACHE_EMPTY ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextCoord::_getSubDataByOrder ()
   {
      INT32 rc = SDB_OK ;
      SUB_CONTEXT_MAP::iterator iterFirst ;
      coordSubContext *pSubContext = NULL ;
      CHAR *pData = NULL ;

      while ( isEmpty() )
      {
         if ( _emptyContextMap.size() + _prepareContextMap.size() > 0 )
         {
            rc = SDB_RTN_COORD_CACHE_EMPTY ;
            goto error ;
         }
         else if ( _subContextMap.size() == 0 )
         {
            _hitEnd = TRUE ;
            rc = SDB_DMS_EOC ;
            goto error ;
         }
         if ( eof() )
         {
            break ;
         }

         for ( INT32 index = 0 ; index < RTN_CONTEXT_GETNUM_ONCE ; ++index )
         {
            if ( 0 == _numToReturn )
            {
               _hitEnd = TRUE ;
               break ;
            }

            iterFirst = _subContextMap.begin() ;
            pSubContext = iterFirst->second ;
            pData = pSubContext->front() ;
            if ( NULL == pData )
            {
               rc = SDB_SYS ;
               PD_LOG( PDERROR, "Failed to get the data, rc: %d", rc ) ;
               goto error ;
            }

            if ( _numToSkip > 0 )
            {
               --_numToSkip ;
            }
            else
            {
               try
               {
                  BSONObj obj( pData ) ;
                  BSONObj selected ;
                  const BSONObj *record = NULL ;

                  if ( !_selector.isInitialized() )
                  {
                     record = &obj ;
                  }
                  else
                  {
                     rc = _selector.select( obj, selected ) ;
                     if ( SDB_OK != rc )
                     {
                        PD_LOG( PDERROR, "failed to select fields from obj:%d", rc ) ;
                        goto error ;
                     }
                     record = &selected ;
                  }

                  rc = append( *record ) ;
                  PD_RC_CHECK( rc, PDERROR, "Append obj[%s] failed, rc: %d",
                               obj.toString().c_str(), rc ) ;
               }
               catch ( std::exception &e )
               {
                  rc = SDB_SYS ;
                  PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
                  goto error ;
               }

               if ( _numToReturn > 0 )
               {
                  --_numToReturn ;
               }
            }

            rc = pSubContext->pop() ;
            PD_RC_CHECK( rc, PDERROR, "Failed to get the data(rc=%d)", rc ) ;

            if ( pSubContext->getRecordNum() <= 0 )
            {
               _subContextMap.erase ( iterFirst ) ;
               _emptyContextMap.insert( EMPTY_CONTEXT_MAP::value_type(
                                        pSubContext->getRouteID().value,
                                        pSubContext ) ) ;
               break ;
            }
            else
            {
               coordOrderKey orderKey ;
               rc = pSubContext->getOrderKey( orderKey, _keyGen ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to get orderKey, rc:%d", rc ) ;

               _subContextMap.erase ( iterFirst ) ;
               _subContextMap.insert( SUB_CONTEXT_MAP::value_type( orderKey,
                                      pSubContext ) ) ;
            }

            if ( buffEndOffset() + DMS_RECORD_MAX_SZ >
                 RTN_RESULTBUFFER_SIZE_MAX )
            {
               break ;
            }
         }
      }

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

      if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_RTN_COORD_CACHE_EMPTY ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void _rtnContextCoord::_delPrepareContext( const MsgRouteID & routeID )
   {
      EMPTY_CONTEXT_MAP::iterator iter =
         _prepareContextMap.find( routeID.value ) ;

      if ( iter != _prepareContextMap.end() )
      {
         coordSubContext *pSubContext = iter->second ;

         if ( pSubContext != NULL )
         {
            SDB_OSS_DEL pSubContext ;
         }
         _prepareContextMap.erase ( iter ) ;
      }
   }

   /*
      _coordSubContext implement
   */
   _coordSubContext::_coordSubContext ()
   {
      _routeID.value = 0 ;
      _contextID = -1 ;
      _pData = NULL ;
      _curOffset = 0 ;
      _recordNum = 0 ;
      _isOrderKeyChange = FALSE ;
   }

   _coordSubContext::_coordSubContext ( const _coordSubContext &srcContext )
   {
   }

   _coordSubContext::_coordSubContext ( MsgRouteID routeID,
                                       SINT64 contextID,
                                       _ixmIndexKeyGen *keyGen )
   : _routeID( routeID ),
     _contextID( contextID ),
     _keyGen( keyGen )
   {
      _pData = NULL ;
      _curOffset = 0 ;
      _recordNum = 0 ;
      _isOrderKeyChange = FALSE ;
   }

   _coordSubContext::~_coordSubContext ()
   {
      if ( NULL != _pData )
      {
         SDB_OSS_FREE ( _pData ) ;
         _pData = NULL;
      }
      _keyGen = NULL ;
   }

   UINT32 _coordSubContext::getRemainLen()
   {
      if ( _pData->header.messageLength > _curOffset )
      {
         return _pData->header.messageLength - _curOffset ;
      }
      return 0 ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_COSUBCON_APPENDDATA, "coordSubContext::appendData" )
   void _coordSubContext::appendData( MsgOpReply * pReply )
   {
      PD_TRACE_ENTRY ( SDB_COSUBCON_APPENDDATA ) ;
      SDB_ASSERT( pReply != NULL, "pReply can't be NULL" ) ;

      if ( _pData != NULL )
      {
         SDB_ASSERT ( _recordNum <= 0, "the buffer must be empty" ) ;
         SDB_OSS_FREE( _pData ) ;
      }
      _routeID = pReply->header.routeID ;
      _pData = pReply ;
      _recordNum = pReply->numReturned ;
      _curOffset = ossAlign4( (UINT32)sizeof( MsgOpReply ) ) ;
      _isOrderKeyChange = TRUE ;
      PD_TRACE_EXIT ( SDB_COSUBCON_APPENDDATA ) ;
   }

   void _coordSubContext::clearData()
   {
      _pData = NULL; //don't delete it, the fun-caller will delete it
      _curOffset = 0;
      _recordNum = 0;
      _isOrderKeyChange = TRUE;
   }

   SINT64 _coordSubContext::getContextID()
   {
      return _contextID ;
   }

   MsgRouteID _coordSubContext::getRouteID()
   {
      return _routeID;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_COSUBCON_FRONT, "coordSubContext::front" )
   CHAR* _coordSubContext::front ()
   {
      PD_TRACE_ENTRY ( SDB_COSUBCON_FRONT ) ;
      if ( _recordNum > 0 && _pData->header.messageLength > _curOffset )
      {
         PD_TRACE_EXIT ( SDB_COSUBCON_FRONT ) ;
         return ( (CHAR *)_pData + _curOffset ) ;
      }
      else
      {
         PD_TRACE_EXIT ( SDB_COSUBCON_FRONT ) ;
         return NULL ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_COSUBCON_POP, "coordSubContext::pop" )
   INT32 _coordSubContext::pop()
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_COSUBCON_POP ) ;
      do
      {
         if ( _curOffset >= _pData->header.messageLength )
         {
            SDB_ASSERT( FALSE, "data-buffer is empty!" );
            rc = SDB_RTN_COORD_CACHE_EMPTY ;
            PD_LOG ( PDWARNING, "Failed to pop the data, reach the end of the "
                     "buffer" ) ;
            break;
         }
         try
         {
            BSONObj boRecord( (CHAR *)_pData + _curOffset ) ;
            _curOffset += boRecord.objsize() ;
            _curOffset = ossAlign4( (UINT32)_curOffset ) ;
            _isOrderKeyChange = TRUE ;
            --_recordNum ;
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "Failed to pop the data, occur unexpected "
                     "error(%s)", e.what() ) ;
         }
      }while ( FALSE ) ;

      PD_TRACE_EXITRC ( SDB_COSUBCON_POP, rc ) ;
      return rc;
   }

   INT32 _coordSubContext::popN( SINT32 num )
   {
      INT32 rc = SDB_OK ;
      while ( num > 0 )
      {
         rc = pop() ;
         if ( rc != SDB_OK )
         {
            break ;
         }
         --num ;
      }
      return rc;
   }

   INT32 _coordSubContext::popAll()
   {
      _recordNum = 0 ;
      _curOffset = _pData->header.messageLength ;
      _isOrderKeyChange = TRUE ;
      return SDB_OK ;
   }

   SINT32 _coordSubContext::getRecordNum()
   {
      return _recordNum ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_COSUBCON_GETORDERKEY, "coordSubContext::getOrderKey" )
   INT32 _coordSubContext::getOrderKey( coordOrderKey &orderKey,
                                       _ixmIndexKeyGen *keyGen )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_COSUBCON_GETORDERKEY ) ;
      do
      {
         if ( !_isOrderKeyChange )
         {
            break ;
         }
         if ( _recordNum <= 0 )
         {
            _orderKey.clear() ;
            break ;
         }
         try
         {
            BSONObj boRecord( (CHAR *)_pData + _curOffset ) ;
            rc = _orderKey.generateKey( boRecord, _keyGen ) ;
            if ( rc != SDB_OK )
            {
               PD_LOG ( PDERROR, "Failed to get order-key(rc=%d)", rc ) ;
               break ;
            }
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "Failed to get order-key, occur unexpected "
                     "error:%s", e.what() ) ;
            break ;
         }
      }while ( FALSE ) ;

      if ( SDB_OK == rc )
      {
         orderKey = _orderKey ;
      }

      PD_TRACE_EXITRC ( SDB_COSUBCON_GETORDERKEY, rc ) ;
      return rc;
   }

   void _coordSubContext::setOrderBy( const BSONObj &orderBy )
   {
      _orderBy = orderBy ;
      _orderKey.setOrderBy( orderBy ) ;
   }

   /*
      _coordOrderKey implement
   */
   _coordOrderKey::_coordOrderKey ( const _coordOrderKey &orderKey )
   {
      _orderBy = orderKey._orderBy ;
      _hash = orderKey._hash ;
      _keyObj = orderKey._keyObj ;
      _arrEle = orderKey._arrEle ;
   }

   _coordOrderKey::_coordOrderKey ()
   {
      _hash.hash = 0 ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_COORDERKEY_OPELT, "coordOrderKey::operator<" )
   BOOLEAN _coordOrderKey::operator<( const _coordOrderKey &rhs ) const
   {
      PD_TRACE_ENTRY ( SDB_COORDERKEY_OPELT ) ;
      BOOLEAN result = FALSE ;
      INT32 rsCmp = _keyObj.woCompare( rhs._keyObj, _orderBy, FALSE ) ;
      if ( rsCmp < 0
         || ( 0 == rsCmp && _hash.hash < rhs._hash.hash ))
      {
         result = TRUE ;
      }
      PD_TRACE1 ( SDB_COORDERKEY_OPELT, PD_PACK_INT(result) );
      PD_TRACE_EXIT ( SDB_COORDERKEY_OPELT ) ;
      return result;
   }

   void _coordOrderKey::clear()
   {
      _arrEle = BSONElement() ;
      _hash.columns.hash1 = 0 ;
      _hash.columns.hash2 = 0 ;
   }

   void _coordOrderKey::setOrderBy( const BSONObj &orderBy )
   {
      _orderBy = orderBy;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_COORDERKEY_GENKEY, "coordOrderKey::generateKey" )
   INT32 _coordOrderKey::generateKey( const BSONObj &record,
                                    _ixmIndexKeyGen *keyGen )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT( keyGen != NULL, "keyGen can't be null!" ) ;
      PD_TRACE_ENTRY ( SDB_COORDERKEY_GENKEY ) ;
      clear();
      BSONObjSet keySet( _orderBy ) ;

      rc = keyGen->getKeys( record, keySet, &_arrEle ) ;
      PD_RC_CHECK( rc, PDERROR,
                  "failed to generate order-key(rc=%d)",
                  rc ) ;
      SDB_ASSERT( !keySet.empty(), "empty key-set!" ) ;
      _keyObj = *(keySet.begin()) ;
      if ( _arrEle.eoo() )
      {
         _hash.hash = 0 ;
      }
      else
      {
         ixmMakeHashValue( _arrEle, _hash ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_COORDERKEY_GENKEY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   _rtnContextSP::_rtnContextSP( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID ),
    _sp(NULL)
   {

   }

   _rtnContextSP::~_rtnContextSP()
   {
      SAFE_OSS_DELETE( _sp ) ;
   }

   RTN_CONTEXT_TYPE _rtnContextSP::getType() const
   {
      return RTN_CONTEXT_SP ;
   }

   INT32 _rtnContextSP::open( _spdSession *sp )
   {
      INT32 rc = SDB_OK ;
      if ( _isOpened )
      {
         rc = SDB_DMS_CONTEXT_IS_OPEN ;
         goto error ;
      }
      if ( NULL == sp )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _sp = sp ;
      _isOpened = TRUE ;
      _hitEnd = FALSE ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32  _rtnContextSP::_prepareData( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj obj ;
      monAppCB *pMonAppCB = cb ? cb->getMonAppCB() : NULL ;

      for ( INT32 i = 0; i < RTN_CONTEXT_GETNUM_ONCE; i++ )
      {
         rc = _sp->next( obj ) ;
         if ( SDB_DMS_EOC == rc )
         {
            _hitEnd = TRUE ;
            break ;
         }
         else if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to fetch spdSession:%d", rc ) ;
            goto error ;
         }
         else
         {
            rc = append( obj ) ;
            PD_RC_CHECK( rc, PDERROR, "Append obj[%s] failed, rc: %d",
                      obj.toString().c_str(), rc ) ;
         }

         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_SELECT, 1 ) ;

         if ( buffEndOffset() + DMS_RECORD_MAX_SZ > RTN_RESULTBUFFER_SIZE_MAX )
         {
            break ;
         }
      }

      if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   _rtnSubCLBuf::_rtnSubCLBuf()
   {
      _isOrderKeyChange = TRUE;
      _remainNum = 0;
   }

   _rtnSubCLBuf::_rtnSubCLBuf( BSONObj &orderBy,
                               _ixmIndexKeyGen *keyGen )
   {
      _orderKey.setOrderBy( orderBy );
      _isOrderKeyChange = TRUE;
      _remainNum = 0;
      _keyGen = keyGen;
   }

   _rtnSubCLBuf::~_rtnSubCLBuf()
   {
      _keyGen = NULL;
   }

   const CHAR* _rtnSubCLBuf::front()
   {
      return _buffer.front();
   }

   INT32 _rtnSubCLBuf::pop()
   {
      INT32 rc = SDB_OK;
      BSONObj obj;
      _isOrderKeyChange = TRUE;
      _remainNum--;
      rc = _buffer.nextObj( obj );
      if ( _remainNum <= 0 )
      {
         rtnContextBuf emptyBuf;
         _buffer = emptyBuf;
      }
      return rc;
   }

   INT32 _rtnSubCLBuf::popN( SINT32 num )
   {
      INT32 rc = SDB_OK;
      _isOrderKeyChange = TRUE;
      if ( num >= recordNum() )
      {
         rc = popAll();
         goto done;
      }
      while ( num > 0 )
      {
         rc = pop();
         if ( rc )
         {
            goto error;
         }
         --num;
      }
   done:
      return rc;
   error:
      goto done;
   }
   INT32 _rtnSubCLBuf::popAll()
   {
      _isOrderKeyChange = TRUE;
      _remainNum = 0;
      rtnContextBuf emptyBuf;
      _buffer = emptyBuf;
      return SDB_OK;
   }

   INT32 _rtnSubCLBuf::recordNum()
   {
      return _remainNum;
   }

   INT32 _rtnSubCLBuf::getOrderKey( coordOrderKey &orderKey )
   {
      INT32 rc = SDB_OK;
      if ( _isOrderKeyChange )
      {
         if ( recordNum() <= 0 )
         {
            _orderKey.clear();
         }
         else
         {
            try
            {
               BSONObj boRecord( front() );
               rc = _orderKey.generateKey( boRecord, _keyGen );
               PD_RC_CHECK( rc, PDERROR, "Failed to get order-key(rc=%d)",
                            rc );
            }
            catch ( std::exception &e )
            {
               PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                            "Occur unexpected error:%s", e.what() ) ;
            }
         }
      }
      orderKey = _orderKey;
      _isOrderKeyChange = FALSE;
   done:
      return rc;
   error:
      goto done;
   }

   rtnContextBuf _rtnSubCLBuf::buffer()
   {
      return _buffer;
   }

   void _rtnSubCLBuf::setBuffer( rtnContextBuf &buffer )
   {
      _buffer = buffer;
      _isOrderKeyChange = TRUE;
      _remainNum = buffer.recordNum();
   }

   _rtnContextMainCL::_rtnContextMainCL( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID )
   {
      _keyGen = NULL;
   }
   _rtnContextMainCL::~_rtnContextMainCL()
   {
      pmdKRCB *pKrcb = pmdGetKRCB();
      SDB_RTNCB *pRtncb = pKrcb->getRTNCB();
      pmdEDUCB *cb = pKrcb->getEDUMgr()->getEDUByID( eduID() );
      SubCLBufList::iterator iterLst
                        = _subCLBufList.begin();
      while( iterLst != _subCLBufList.end() )
      {
         pRtncb->contextDelete( iterLst->first, cb );
         ++iterLst;
      }
      _subCLBufList.clear();
      SAFE_OSS_DELETE( _keyGen );
   }

   RTN_CONTEXT_TYPE _rtnContextMainCL::getType () const
   {
      return RTN_CONTEXT_MAINCL;
   }

   INT32 _rtnContextMainCL::open( const bson::BSONObj & orderBy,
                                  INT64 numToReturn,
                                  INT64 numToSkip,
                                  BOOLEAN includeShardingOrder )
   {
      INT32 rc = SDB_OK;
      _orderBy = orderBy.getOwned();
      _numToReturn = numToReturn;
      _numToSkip = numToSkip;
      _keyGen = SDB_OSS_NEW _ixmIndexKeyGen( _orderBy ) ;
      PD_CHECK( _keyGen != NULL, SDB_OOM, error, PDERROR,
                "malloc failed!" ) ;

      _isOpened = TRUE;
      _includeShardingOrder = includeShardingOrder;
      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE;
      }
      else
      {
         _hitEnd = FALSE;
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _rtnContextMainCL::addSubContext( SINT64 contextID )
   {
      rtnSubCLBuf emptyCTXBuf( _orderBy, _keyGen ) ;
      _subCLBufList[ contextID ] = emptyCTXBuf;
      return SDB_OK;
   }

   BOOLEAN _rtnContextMainCL::requireOrder () const
   {
      if ( _orderBy.isEmpty() || _subCLBufList.size() <= 1 )
      {
         return FALSE;
      }
      return TRUE;
   }

   INT32 _rtnContextMainCL::getMore( INT32 maxNumToReturn,
                                     rtnContextBuf &buffObj,
                                     _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      BOOLEAN hasData = FALSE;
      pmdKRCB *pKrcb = pmdGetKRCB();
      SDB_RTNCB *pRtncb = pKrcb->getRTNCB();

      buffObj.release() ;

      if ( !isOpened() )
      {
         rc = SDB_DMS_CONTEXT_IS_CLOSE ;
         goto error ;
      }
      else if ( eof() && isEmpty() )
      {
         rc = SDB_DMS_EOC ;
         _isOpened = FALSE ;
         goto error ;
      }

      if ( !isEmpty() || ( requireOrder() && !_includeShardingOrder ) )
      {
         rc = this->_rtnContextBase::getMore( maxNumToReturn, buffObj,
                                              cb );
         goto done;
      }

      while( !hasData )
      {
         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }

         while ( _numToSkip > 0 )
         {
            SubCLBufList::iterator iterSubCTXSkip = _subCLBufList.begin();
            if ( _subCLBufList.end() == iterSubCTXSkip ||
                 iterSubCTXSkip->second.recordNum() <= 0 )
            {
               break;
            }
            if ( _numToSkip >= iterSubCTXSkip->second.recordNum() )
            {
               _numToSkip -= iterSubCTXSkip->second.recordNum();
               iterSubCTXSkip->second.popAll();
            }
            else
            {
               iterSubCTXSkip->second.popN( _numToSkip );
               _numToSkip = 0;
            }
         }

         SubCLBufList::iterator iterSubCTX = _subCLBufList.begin();
         if ( _subCLBufList.end() == iterSubCTX )
         {
            _hitEnd = TRUE ;
            _isOpened = FALSE ;
            rc = SDB_DMS_EOC;
            goto error ;
         }

         if ( iterSubCTX->second.recordNum() <= 0 )
         {
            rc = _prepareSubCTXData( iterSubCTX, cb, maxNumToReturn ) ;
            if ( rc != SDB_OK )
            {
               pRtncb->contextDelete( iterSubCTX->first, cb );
               _subCLBufList.erase( iterSubCTX );
               if ( SDB_DMS_EOC != rc )
               {
                  goto error;
               }
            }
            continue ;
         }
         buffObj = iterSubCTX->second.buffer() ;
         iterSubCTX->second.popAll() ;
         hasData = TRUE ;

         if ( _numToReturn > 0 )
         {
            if ( buffObj.recordNum() > _numToReturn )
            {
               buffObj.truncate( _numToReturn ) ;
            }
            _numToReturn -= buffObj.recordNum() ;
         }
      }

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

   done:
      return rc;
   error:
      goto done;
   }

   INT32 _rtnContextMainCL::_prepareSubCTXData( SubCLBufList::iterator iterSubCTX,
                                                _pmdEDUCB * cb,
                                                INT32 maxNumToReturn )
   {
      INT32 rc = SDB_OK;
      _SDB_RTNCB *pRtnCB = pmdGetKRCB()->getRTNCB();
      rtnContext *pContext = NULL;
      rtnContextBuf contextBuf;
      SDB_ASSERT( _subCLBufList.end() != iterSubCTX, "invalid iterator!" );
      if ( iterSubCTX->second.recordNum() > 0 )
      {
         goto done;
      }

      pContext = pRtnCB->contextFind( iterSubCTX->first );
      PD_CHECK( pContext, SDB_RTN_CONTEXT_NOTEXIST, error, PDERROR,
                "Context %lld does not exist", iterSubCTX->first );
      rc = pContext->getMore( maxNumToReturn,
                              contextBuf,
                              cb );
      if ( rc )
      {
         if ( rc != SDB_DMS_EOC )
         {
            PD_LOG( PDERROR, "getmore failed(rc=%d)", rc );
         }
         goto error;
      }
      iterSubCTX->second.setBuffer( contextBuf );

   done:
      return rc;
   error:
      goto done;
   }

   INT32 _rtnContextMainCL::_prepareData( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT( requireOrder(), "here should be order!" );
      rc = _prepareDataByOrder( cb );
      return rc;
   }

   INT32 _rtnContextMainCL::_prepareDataByOrder( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      pmdKRCB *pKrcb = pmdGetKRCB();
      SDB_RTNCB *pRtncb = pKrcb->getRTNCB();

      while ( 0 != _numToReturn )
      {
         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }

         SubCLBufList::iterator iterSubCTXFirst = _subCLBufList.begin();
         if ( _subCLBufList.end() == iterSubCTXFirst )
         {
            _hitEnd = TRUE ;
            if ( isEmpty() )
            {
               rc = SDB_DMS_EOC;
            }
            break;
         }
         if ( iterSubCTXFirst->second.recordNum() <= 0 )
         {
            if ( !isEmpty() )
            {
               goto done;
            }
            rc = _prepareSubCTXData( iterSubCTXFirst, cb, -1 );
            if ( rc )
            {
               pRtncb->contextDelete( iterSubCTXFirst->first, cb );
               _subCLBufList.erase( iterSubCTXFirst );
               if ( rc != SDB_DMS_EOC )
               {
                  goto error;
               }
               continue;
            }
         }
         SubCLBufList::iterator iterSubCTXCur = iterSubCTXFirst;
         ++iterSubCTXCur;
         coordOrderKey firstOrderKey;
         coordOrderKey curOrderKey;
         rc = iterSubCTXFirst->second.getOrderKey( firstOrderKey );
         PD_RC_CHECK( rc, PDERROR,
                      "Failed to generate order-key(rc=%d)", rc );
         while( iterSubCTXCur != _subCLBufList.end() )
         {
            if ( iterSubCTXCur->second.recordNum() <= 0 )
            {
               if ( !isEmpty() )
               {
                  goto done;
               }
               rc = _prepareSubCTXData( iterSubCTXCur, cb, -1 );
               if ( rc )
               {
                  pRtncb->contextDelete( iterSubCTXCur->first, cb );
                  _subCLBufList.erase( iterSubCTXCur++ );
                  if ( rc != SDB_DMS_EOC )
                  {
                     goto error;
                  }
                  continue;
               }
            }
            rc = iterSubCTXCur->second.getOrderKey( curOrderKey );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to generate order-key(rc=%d)", rc );
            if ( curOrderKey < firstOrderKey )
            {
               iterSubCTXFirst = iterSubCTXCur;
               firstOrderKey = curOrderKey;
            }
            ++iterSubCTXCur;
         }

         if ( _numToSkip <= 0 )
         {
            try
            {
               BSONObj obj( iterSubCTXFirst->second.front() );
               rc = append( obj ) ;
               PD_RC_CHECK( rc, PDERROR,
                            "Failed to append data(rc=%d)", rc );
            }
            catch ( std::exception &e )
            {
               PD_LOG( PDERROR, "occur unexpected error:%s", e.what() );
               goto error;
            }

            if ( _numToReturn > 0 )
            {
               --_numToReturn ;
            }
         }
         else
         {
            --_numToSkip ;
         }
         rc = iterSubCTXFirst->second.pop();
         if ( rc )
         {
            goto error;
         }
      }

      if ( 0 == _numToReturn )
      {
         _hitEnd = TRUE ;
      }

   done:
      return rc;
   error:
      goto done;
   }

   _rtnContextSort::_rtnContextSort( INT64 contextID, UINT64 eduID )
   :_rtnContextData( contextID, eduID ),
    _skip( 0 ),
    _limit( -1 ),
    _planForExplain( NULL )
   {

   }

   _rtnContextSort::~_rtnContextSort()
   {
      _skip = 0 ;
      _limit = 0 ;
      _planForExplain = NULL ;
   }

   RTN_CONTEXT_TYPE _rtnContextSort::getType() const
   {
      return RTN_CONTEXT_SORT ;
   }

   INT32 _rtnContextSort::open( const BSONObj &orderby,
                                rtnContext *context,
                                pmdEDUCB *cb,
                                SINT64 numToSkip,
                                SINT64 numToReturn )
   {
      SDB_ASSERT( !orderby.isEmpty(), "impossible" ) ;
      SDB_ASSERT( NULL != cb, "possible" ) ;
      SDB_ASSERT( NULL != context, "impossible" ) ;
      INT32 rc = SDB_OK ;
      UINT64 sortBufSz = pmdGetOptionCB()->getSortBufSize() ;
      SINT64 limit = numToReturn ;

      if ( 0 < limit && 0 < numToSkip )
      {
         limit += numToSkip ;
      }

      rc = _sorting.init( sortBufSz, orderby, context,
                          contextID(), limit, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to init sort:%d", rc ) ;
         goto error ;
      }

      _isOpened = TRUE ;
      _hitEnd = FALSE ;
      _skip = numToSkip ;
      _limit = numToReturn ;

      rc = _rebuildSrcContext( orderby, context ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to rebuild src context:%d", rc ) ;
         goto error ;
      }

      if ( RTN_CONTEXT_DATA == context->getType() )
      {
         _planForExplain = ( ( _rtnContextData * )context )->getPlan() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextSort::_rebuildSrcContext( const BSONObj &orderBy,
                                              rtnContext *srcContext )
   {
      INT32 rc = SDB_OK ;
      const BSONObj &selector = srcContext->getSelector().getPattern() ;
      if ( selector.isEmpty() )
      {
         goto done ;
      }
      else
      {
         BOOLEAN needRebuild = FALSE ;
         needResetSelector( selector, orderBy, needRebuild ) ;
         if ( needRebuild )
         {
            rc = srcContext->getSelector().move( _selector ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to rebuild selector:%d", rc ) ;
               goto error ;      
            }
         }
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextSort::_prepareData( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;

      const INT32 maxNum = 1000000 ;
      const INT32 breakBufferSize = 2097152 ; /// 2MB
      const INT32 minRecordNum = 4 ;
      BSONObj obj ;
      monAppCB *pMonAppCB = cb ? cb->getMonAppCB() : NULL ;

      if ( 0 == _limit )
      {
         _hitEnd = TRUE ;
         rc = SDB_DMS_EOC ;
         goto error ;
      }

      for ( INT32 i = 0; i < maxNum; i++ )
      {
         rc = _sorting.fetch( obj, cb ) ;
         if ( SDB_DMS_EOC == rc )
         {
            _hitEnd = TRUE ;
            break ;
         }
         else if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to fetch from sorting:%d", rc ) ;
            goto error ;
         }
         else if ( 0 < _skip )
         {
            --_skip ;
            --i ;
            continue ;
         }
         else if ( 0 == _limit )
         {
            _hitEnd = TRUE ;
            break ;
         }
         else
         {
            const BSONObj *record = NULL ;
            BSONObj selected ;
            if ( _selector.isInitialized() )
            {
               rc = _selector.select( obj, selected ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "failed to select fields from obj:%d", rc ) ;
                  goto error ;
               }
               record = &selected ;
            }
            else
            {
               record = &obj ;
            }
   
            rc = append( *record ) ;
            PD_RC_CHECK( rc, PDERROR, "Append obj[%s] failed, rc: %d",
                      obj.toString().c_str(), rc ) ;

            if ( 0 < _limit )
            {
               --_limit ;
            }
         }

         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_SELECT, 1 ) ;

         if ( minRecordNum <= i && buffEndOffset() >= breakBufferSize )
         {
            break ;
         }

         if ( buffEndOffset() + DMS_RECORD_MAX_SZ > RTN_RESULTBUFFER_SIZE_MAX )
         {
            break ;
         }
      }

      if ( SDB_OK != rc )
      {
         goto error ;
      }
      else if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   _rtnContextQgmSort::_rtnContextQgmSort( INT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID ),
    _qp(NULL)
   {

   }

   _rtnContextQgmSort::~_rtnContextQgmSort()
   {
      _qp = NULL ;
   }

   RTN_CONTEXT_TYPE _rtnContextQgmSort::getType () const
   {
      return RTN_CONTEXT_QGMSORT ;
   }

   INT32 _rtnContextQgmSort::open( _qgmPlan *qp )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != qp, "impossible" ) ;
      if ( _isOpened )
      {
         rc = SDB_DMS_CONTEXT_IS_OPEN ;
         goto error ;
      }

      _qp = qp ;
      _isOpened = TRUE ;
      _hitEnd = FALSE ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextQgmSort::_prepareData( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != _qp, "impossible" ) ;
      qgmFetchOut next ;
      INT32 index = 0 ;
      monAppCB *pMonAppCB = cb ? cb->getMonAppCB() : NULL ;
      for ( ; index < RTN_CONTEXT_GETNUM_ONCE ; ++index )
      {
         try
         {
            rc = _qp->fetchNext( next ) ;
         }
         catch( std::exception &e )
         {
            PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         if ( SDB_DMS_EOC == rc )
         {
            _hitEnd = TRUE ;
            break ;
         }
         else if ( rc )
         {
            PD_LOG( PDERROR, "Qgm fetch failed, rc: %d", rc ) ;
            goto error ;
         }

         rc = append( next.obj ) ;
         PD_RC_CHECK( rc, PDERROR, "Append obj[%s] failed, rc: %d",
                      next.obj.toString().c_str(), rc ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_SELECT, 1 ) ;
         if ( buffEndOffset() + DMS_RECORD_MAX_SZ > RTN_RESULTBUFFER_SIZE_MAX )
         {
            break ;
         }
      }

      if ( !isEmpty() )
      {
         rc = SDB_OK ;
      }
      else
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   _rtnContextDelCS::_rtnContextDelCS( SINT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID )
   {
      _status = DELCSPHASE_0;
      _pDmsCB = pmdGetKRCB()->getDMSCB() ;
      _pDpsCB = pmdGetKRCB()->getDPSCB() ;
      _pCatAgent = pmdGetKRCB()->getClsCB ()->getCatAgent () ;
      _pTransCB = pmdGetKRCB()->getTransCB();
      _gotDmsCBWrite = FALSE;
      _gotLogSize = 0;
      _logicCSID = DMS_INVALID_LOGICCSID;
      ossMemset( _name, 0, DMS_COLLECTION_SPACE_NAME_SZ + 1 );
   }

   _rtnContextDelCS::~_rtnContextDelCS()
   {
      pmdEDUMgr *eduMgr = pmdGetKRCB()->getEDUMgr() ;
      pmdEDUCB *cb = eduMgr->getEDUByID( eduID() ) ;
      if ( DELCSPHASE_1 == _status )
      {
         INT32 rcTmp = SDB_OK;
         rcTmp = rtnDropCollectionSpaceP1Cancel( _name, cb, _pDmsCB, _pDpsCB );
         if ( rcTmp )
         {
            PD_LOG( PDERROR, "failed to cancel drop cs(name:%s, rc=%d)",
                    _name, rcTmp );
         }
         _status = DELCSPHASE_0;
      }
      _clean( cb );
   }

   RTN_CONTEXT_TYPE _rtnContextDelCS::getType () const
   {
      return RTN_CONTEXT_DELCS;
   }

   INT32 _rtnContextDelCS::open( const CHAR *pCollectionName,
                                 _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      dpsMergeInfo info ;
      dpsLogRecord &record = info.getMergeBlock().record();

      SDB_ASSERT( pCollectionName, "pCollectionName can't be null!" );
      PD_CHECK( pCollectionName, SDB_INVALIDARG, error, PDERROR,
                "pCollectionName is null!" );
      rc = dmsCheckCSName( pCollectionName );
      PD_RC_CHECK( rc, PDERROR, "Invalid cs name(name:%s)",
                   pCollectionName );

      if ( NULL != _pDpsCB )
      {
         UINT32 logRecSize = 0;
         rc = dpsCSDel2Record( pCollectionName, record ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "Failed to build record:%d",rc ) ;

         rc = _pDpsCB->checkSyncControl( record.alignedLen(), cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d", rc ) ;

         logRecSize = record.alignedLen() ;
         rc = _pTransCB->reservedLogSpace( logRecSize );
         PD_RC_CHECK( rc, PDERROR,
                      "Failed to reserved log space(length=%u)",
                      logRecSize );
         _gotLogSize = logRecSize ;
      }

      rc = _pDmsCB->writable ( cb ) ;
      PD_RC_CHECK( rc, PDERROR,
                   "dms is not writable, rc = %d", rc ) ;
      _gotDmsCBWrite = TRUE;

      rc = _tryLock( pCollectionName, cb );
      PD_RC_CHECK( rc, PDERROR, "Failed to lock, rc: %d", rc ) ;

      rc = rtnDropCollectionSpaceP1( _name, cb, _pDmsCB, _pDpsCB );
      PD_RC_CHECK( rc, PDERROR, "Failed to drop cs in phase1, rc: %d", rc );
      _status = DELCSPHASE_1 ;
      _isOpened = TRUE ;

   done:
      return rc;
   error:
      _clean( cb );
      goto done;
   }

   INT32 _rtnContextDelCS::getMore( INT32 maxNumToReturn,
                                    rtnContextBuf &buffObj,
                                    _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      if ( !isOpened() )
      {
         rc = SDB_DMS_CONTEXT_IS_CLOSE;
         goto error ;
      }
      _pCatAgent->lock_w () ;
      _pCatAgent->clearBySpaceName ( _name ) ;
      _pCatAgent->release_w () ;
      pmdGetKRCB()->getClsCB()->invalidateCata( _name ) ;

      PD_CHECK( pmdIsPrimary(), SDB_CLS_NOT_PRIMARY, error, PDERROR,
                "Failed to drop cs before phase2(%d)", rc ) ;

      _status = DELCSPHASE_2 ;
      rc = rtnDropCollectionSpaceP2( _name, cb, _pDmsCB, _pDpsCB ) ;
      PD_RC_CHECK( rc, PDERROR,
                  "Failed to drop cs in phase2(%d)", rc );
      _clean( cb );
      _status = DELCSPHASE_0 ;
      _isOpened = FALSE ;
      rc = SDB_DMS_EOC ;

   done:
      return rc;
   error:
      goto done;
   }

   INT32 _rtnContextDelCS::_tryLock( const CHAR *pCollectionName,
                                     _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      if ( _pDpsCB )
      {
         dmsStorageUnitID suID = DMS_INVALID_CS;
         UINT32 logicCSID = DMS_INVALID_LOGICCSID;
         dmsStorageUnit *su = NULL;
         _releaseLock( cb );
         UINT32 length = ossStrlen ( pCollectionName );
         PD_CHECK( (length > 0 && length <= DMS_SU_NAME_SZ), SDB_INVALIDARG,
                   error, PDERROR, "Invalid length of collectionspace name:%s",
                   pCollectionName );

         rc = _pDmsCB->nameToSUAndLock( pCollectionName, suID, &su );
         PD_RC_CHECK(rc, PDERROR, "lock collection space(%s) failed(rc=%d)",
                     pCollectionName, rc );
         logicCSID = su->LogicalCSID();
         _pDmsCB->suUnlock ( suID ) ;
         rc = _pTransCB->transLockTryX( cb, logicCSID ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "Get transaction-lock of CS(%s) failed(rc=%d)",
                      pCollectionName, rc ) ;
         ossStrcpy( _name, pCollectionName ) ;
         _logicCSID = logicCSID ;
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _rtnContextDelCS::_releaseLock( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      if ( cb && _pDpsCB && ( _logicCSID != DMS_INVALID_LOGICCSID ) )
      {
         _pTransCB->transLockRelease( cb, _logicCSID );
         ossMemset( _name, 0, DMS_COLLECTION_SPACE_NAME_SZ );
         _logicCSID = DMS_INVALID_LOGICCSID;
      }
      return rc;
   }

   void _rtnContextDelCS::_clean( _pmdEDUCB *cb )
   {
      INT32 rcTmp = SDB_OK;
      rcTmp = _releaseLock( cb );
      if ( rcTmp )
      {
         PD_LOG( PDERROR, "releas lock failed, rc: %d", rcTmp );
      }
      if ( _gotDmsCBWrite )
      {
         _pDmsCB->writeDown ( cb ) ;
         _gotDmsCBWrite = FALSE;
      }
      if ( _gotLogSize > 0 )
      {
         _pTransCB->releaseLogSpace( _gotLogSize );
         _gotLogSize = 0;
      }
   }

   _rtnContextDelCL::_rtnContextDelCL( SINT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID )
   {
      _pDmsCB        = pmdGetKRCB()->getDMSCB() ;
      _pDpsCB        = pmdGetKRCB()->getDPSCB() ;
      _pCatAgent     = pmdGetKRCB()->getClsCB ()->getCatAgent () ;
      _pTransCB      = pmdGetKRCB()->getTransCB();
      _gotDmsCBWrite = FALSE ;
      _hasLock       = FALSE ;
      _hasDropped    = FALSE ;
      _mbContext     = NULL ;
      _su            = NULL ;
   }

   _rtnContextDelCL::~_rtnContextDelCL()
   {
      pmdEDUMgr *eduMgr    = pmdGetKRCB()->getEDUMgr() ;
      pmdEDUCB *cb         = eduMgr->getEDUByID( eduID() ) ;
      _clean( cb ) ;
   }

   INT32 _rtnContextDelCL::_tryLock( const CHAR *pCollectionName,
                                     _pmdEDUCB *cb )
   {
      INT32 rc                = SDB_OK ;
      dmsStorageUnitID suID   = DMS_INVALID_CS ;
      const CHAR *pCollectionShortName = NULL;

      rc = rtnResolveCollectionNameAndLock ( pCollectionName, _pDmsCB,
                                             &_su, &pCollectionShortName,
                                             suID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to resolve collection name"
                   "(collection:%s, rc: %d)", pCollectionName, rc ) ;

      _collectionName = pCollectionName ;
      _clShortName    = pCollectionShortName ;

      if ( _pDpsCB && _pTransCB->isTransOn() )
      {
         rc = _su->data()->getMBContext( &_mbContext, pCollectionShortName,
                                         EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDERROR, "Get collection[%s] mb context failed, "
                      "rc: %d", pCollectionName, rc ) ;

         rc = _pTransCB->transLockTryX( cb, _su->LogicalCSID(),
                                        _mbContext->mbID() ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "Get transaction-lock of collection(%s) failed(rc=%d)",
                      pCollectionName, rc ) ;
         _hasLock = TRUE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextDelCL::_releaseLock( _pmdEDUCB *cb )
   {
      if ( cb && _hasLock )
      {
         _pTransCB->transLockRelease( cb, _su->LogicalCSID(),
                                      _mbContext->mbID() ) ;
         _hasLock = FALSE ;
      }
      return SDB_OK ;
   }

   RTN_CONTEXT_TYPE _rtnContextDelCL::getType () const
   {
      return RTN_CONTEXT_DELCL ;
   }

   INT32 _rtnContextDelCL::open( const CHAR *pCollectionName,
                                 _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( pCollectionName, "pCollectionName can't be null!" );
      PD_CHECK( pCollectionName, SDB_INVALIDARG, error, PDERROR,
               "pCollectionName is null!" );
      rc = dmsCheckFullCLName( pCollectionName );
      PD_RC_CHECK( rc, PDERROR, "Invalid collection name(name:%s)",
                   pCollectionName ) ;

      rc = _pDmsCB->writable ( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Database is not writable, rc = %d", rc ) ;
      _gotDmsCBWrite = TRUE ;

      rc = _tryLock( pCollectionName, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to lock(rc=%d)", rc ) ;
      _isOpened = TRUE ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnContextDelCL::getMore( INT32 maxNumToReturn,
                                    rtnContextBuf &buffObj,
                                    _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;

      if ( !isOpened() )
      {
         rc = SDB_DMS_CONTEXT_IS_CLOSE;
         goto error ;
      }
      _pCatAgent->lock_w () ;
      _pCatAgent->clear ( _collectionName.c_str() ) ;
      _pCatAgent->release_w () ;
      pmdGetKRCB()->getClsCB()->invalidateCata( _collectionName.c_str() ) ;

      PD_CHECK( pmdIsPrimary(), SDB_CLS_NOT_PRIMARY, error, PDERROR,
                "Failed to drop cs before phase2(%d)", rc );

      rc = _su->data()->dropCollection ( _clShortName.c_str(), cb, _pDpsCB,
                                         TRUE, _mbContext ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to drop collection %s, rc: %d",
                  _collectionName.c_str(), rc ) ;
         goto error ;
      }
      _su->getAPM()->invalidatePlans ( _clShortName.c_str() ) ;
      _hasDropped = TRUE ;

      _clean( cb ) ;
      _isOpened = FALSE ;
      rc = SDB_DMS_EOC ;

   done:
      return rc;
   error:
      goto done;
   }

   void _rtnContextDelCL::_clean( _pmdEDUCB *cb )
   {
      INT32 rcTmp = SDB_OK;
      rcTmp = _releaseLock( cb ) ;
      if ( rcTmp )
      {
         PD_LOG( PDERROR, "release lock failed, rc: %d", rcTmp ) ;
      }
      if ( _su && _mbContext )
      {
         _su->data()->releaseMBContext( _mbContext ) ;
      }
      if ( _pDmsCB && _su )
      {
         string csname = _su->CSName() ;
         _pDmsCB->suUnlock ( _su->CSID() ) ;
         _su = NULL ;

         if ( _hasDropped )
         {
            _pDmsCB->dropEmptyCollectionSpace( csname.c_str(), cb, _pDpsCB ) ;
         }
      }
      if ( _gotDmsCBWrite )
      {
         _pDmsCB->writeDown( cb ) ;
         _gotDmsCBWrite = FALSE ;
      }
      _isOpened = FALSE ;
   }

   _rtnContextDelMainCL::_rtnContextDelMainCL( SINT64 contextID, UINT64 eduID )
   :_rtnContextBase( contextID, eduID )
   {
      _pCatAgent     = pmdGetKRCB()->getClsCB()->getCatAgent() ;
      _pRtncb        = pmdGetKRCB()->getRTNCB();
      _version       = -1;
      ossMemset( _name, 0, DMS_COLLECTION_FULL_NAME_SZ + 1 );
   }

   _rtnContextDelMainCL::~_rtnContextDelMainCL()
   {
      pmdEDUMgr *eduMgr = pmdGetKRCB()->getEDUMgr() ;
      pmdEDUCB *cb = eduMgr->getEDUByID( eduID() ) ;
      _clean( cb );
   }

   void _rtnContextDelMainCL::_clean( _pmdEDUCB *cb )
   {
      SUBCL_CONTEXT_LIST::iterator iter = _subContextList.begin();
      while( iter != _subContextList.end() )
      {
         if ( iter->second != -1 )
         {
            _pRtncb->contextDelete( iter->second, cb );
         }
         _subContextList.erase( iter++ );
      }
   }

   RTN_CONTEXT_TYPE _rtnContextDelMainCL::getType () const
   {
      return RTN_CONTEXT_DELMAINCL;
   }

   INT32 _rtnContextDelMainCL::open( const CHAR *pCollectionName,
                                     _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      std::vector< std::string > strSubCLList ;
      std::vector< std::string > strSubCLListTmp ;
      std::vector< std::string >::iterator iter ;
      SDB_ASSERT( pCollectionName, "pCollectionName can't be null!" ) ;
      PD_CHECK( pCollectionName, SDB_INVALIDARG, error, PDERROR,
                "pCollectionName is null!" ) ;
      rc = dmsCheckFullCLName( pCollectionName ) ;
      PD_RC_CHECK( rc, PDERROR, "Invalid collection name[%s])",
                   pCollectionName ) ;

      {
      _clsCatalogSet *pCataSet = NULL;
      _pCatAgent->lock_r();
      pCataSet = _pCatAgent->collectionSet( pCollectionName );
      if ( NULL == pCataSet )
      {
         _pCatAgent->release_r();
         rc = SDB_DMS_NOTEXIST ;
         PD_LOG( PDERROR, "Can not find collection:%s", pCollectionName );
         goto error;
      }
      _version = pCataSet->getVersion();
      pCataSet->getSubCLList( strSubCLListTmp );
      iter = strSubCLListTmp.begin();
      while( iter != strSubCLListTmp.end() )
      {
         _clsCatalogSet *pSubSet = NULL;
         pSubSet = _pCatAgent->collectionSet( iter->c_str() );
         if ( NULL == pSubSet || 0 == pSubSet->groupCount() )
         {
            ++iter;
            continue;
         }
         strSubCLList.push_back( *iter );
         ++iter;
      }
      _pCatAgent->release_r();
      }

      {
         iter = strSubCLList.begin();
         while( iter != strSubCLList.end() )
         {
            rtnContextDelCL *delContext = NULL;
            SINT64 contextID;
            rc = _pRtncb->contextNew( RTN_CONTEXT_DELCL,
                                      (rtnContext **)&delContext,
                                      contextID, cb ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to create sub-context, "
                         "drop cl failed, rc: %d", rc ) ;
            rc = delContext->open( (*iter).c_str(), cb ) ;
            if ( rc != SDB_OK )
            {
               _pRtncb->contextDelete( contextID, cb ) ;
               if ( SDB_DMS_NOTEXIST == rc )
               {
                  ++iter;
                  continue;
               }
               PD_LOG( PDERROR, "Failed to open sub-context, drop "
                       "cl failed(rc=%d)", rc ) ;
               goto error;
            }
            _subContextList[ *iter ] = contextID ;
            ++iter;
         }
      }

      ossStrcpy( _name, pCollectionName );
      _isOpened = TRUE;
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _rtnContextDelMainCL::getMore( INT32 maxNumToReturn,
                                        rtnContextBuf &buffObj,
                                        _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      INT32 curVer = -1;
      std::vector< std::string > strSubCLList;
      if ( !isOpened() )
      {
         rc = SDB_DMS_CONTEXT_IS_CLOSE;
         goto error ;
      }

      PD_CHECK( pmdIsPrimary(), SDB_CLS_NOT_PRIMARY, error, PDERROR,
                "Failed to drop cs before phase2(%d)", rc );

      {
      _clsCatalogSet *pCataSet = NULL;
      _pCatAgent->lock_r();
      pCataSet = _pCatAgent->collectionSet( _name );
      if ( NULL == pCataSet )
      {
         _pCatAgent->release_r();
         rc = SDB_DMS_NOTEXIST;
         PD_LOG( PDERROR, "can not find collection:%s", _name );
         goto error;
      }
      pCataSet->getSubCLList( strSubCLList );
      curVer = pCataSet->getVersion();
      _pCatAgent->release_r();
      }

      {
         if ( _version != curVer )
         {
            SUBCL_CONTEXT_LIST::iterator iterCtx;
            std::vector< std::string >::iterator iterCl
                                    = strSubCLList.begin();
            while( iterCl != strSubCLList.end() )
            {
               iterCtx = _subContextList.find( *iterCl );
               if ( _subContextList.end() == iterCtx )
               {
                  PD_LOG( PDERROR, "The version is changed, "
                          "sub-collection(%s) have not been delete",
                          (*iterCl).c_str() );
               }
               else
               {
                  rtnContextBuf buffObj;
                  rc = rtnGetMore( iterCtx->second, -1, buffObj, cb, _pRtncb ) ;
                  PD_CHECK( SDB_DMS_EOC == rc || SDB_DMS_NOTEXIST == rc,
                            rc, error, PDERROR,
                            "Failed to del sub-collection, rc: %d",
                            rc ) ;
                  rc = SDB_OK;
                  _subContextList.erase( iterCtx );
               }
               ++iterCl;
            }
            iterCtx = _subContextList.begin();
            while( iterCtx != _subContextList.end() )
            {
               PD_LOG( PDERROR, "The version is changed, "
                       "sub-collection(%s) have not been delete",
                       (iterCtx->first).c_str() ) ;
               ++iterCtx ;
            }
         }
         else
         {
            SUBCL_CONTEXT_LIST::iterator iterCtx = _subContextList.begin();
            while( iterCtx != _subContextList.end() )
            {
               rtnContextBuf buffObj;
               rc = rtnGetMore( iterCtx->second, -1, buffObj, cb, _pRtncb );
               PD_CHECK( SDB_DMS_EOC == rc || SDB_DMS_NOTEXIST == rc,
                         rc, error, PDERROR,
                         "Failed to del sub-collection, rc: %d",
                         rc );
               rc = SDB_OK;
               _subContextList.erase( iterCtx++ );
            }
         }
      }

      _pCatAgent->lock_w () ;
      _pCatAgent->clear ( _name ) ;
      _pCatAgent->release_w () ;
      pmdGetKRCB()->getClsCB()->invalidateCata( _name ) ;
      _isOpened = FALSE;
      if ( _version != curVer &&
           ( !_subContextList.empty() || !strSubCLList.empty() ) )
      {
         std::vector< std::string >::iterator iterClTmp;
         iterClTmp = strSubCLList.begin();
         while( iterClTmp != strSubCLList.end() )
         {
            PD_LOG( PDERROR, "The version is changed, "
                    "sub-collection(%s) have not been delete",
                    (*iterClTmp).c_str() );
            ++iterClTmp;
         }
         rc = SDB_CLS_COORD_NODE_CAT_VER_OLD ;
      }
      else
      {
         rc = SDB_DMS_EOC ;
      }
   done:
      return rc;
   error:
      goto done;
   }

   _rtnContextExplain::_rtnContextExplain( INT64 contextID,
                                           UINT64 eduID )
   :_rtnContextBase( contextID, eduID ),
    _queryContextID( -1 ),
    _recordNum( 0 ),
    _cbOfQuery( NULL ),
    _explained( FALSE )
   {
      _needRun = FALSE ;
   }

   _rtnContextExplain::~_rtnContextExplain()
   {
      if ( -1 != _queryContextID )
      {
         sdbGetRTNCB()->contextDelete( _queryContextID,
                                       _cbOfQuery ) ;
         _queryContextID = -1 ;
         _cbOfQuery = NULL ;
      }
   }

   INT32 _rtnContextExplain::open( const rtnQueryOptions &options,
                                   const BSONObj &explainOptions )
   {
      INT32 rc = SDB_OK ;
      if ( _isOpened )
      {
         rc = SDB_DMS_CONTEXT_IS_OPEN ;
         goto error ;
      }

      _options = options ;
      rc = _options.getOwned() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "options failed to get owned:%d", rc ) ;
         goto error ;
      }

      try
      {
         BSONElement e = explainOptions.getField( FIELD_NAME_RUN ) ;
         if ( e.eoo() )
         {
            _needRun = FALSE ;
         }
         else if ( e.isNumber() )
         {
            _needRun = e.numberInt() == 0 ? FALSE : TRUE ;
         }
         else if ( e.isBoolean() )
         {
            _needRun = e.booleanSafe() ;
         }
         else
         {
            _needRun = FALSE ;
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Ocurr exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _isOpened = TRUE ;
      _hitEnd = FALSE ;
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCONTEXTEXPLAIN__PREPAREDATA, "_rtnContextExplain::_prepareData" )
   INT32 _rtnContextExplain::_prepareData( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCONTEXTEXPLAIN__PREPAREDATA ) ;

      if ( _explained )
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }

      rc = _prepareToExplain( cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare for explaining:%d", rc ) ;
         goto error ;
      }

      rc = _explainQuery( cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to explain query:%d", rc ) ;
         goto error ;
      }

      rc = _commitResult( cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to commit result:%d", rc ) ;
         goto error ;
      }

      _explained = TRUE ;
      _hitEnd    = TRUE ;

   done:
      PD_TRACE_EXITRC( SDB_RTNCONTEXTEXPLAIN__PREPAREDATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCONTEXTEXPLAIN__PREPARETOEXPLAIN, "_rtnContextExplain::_prepareToExplain" )
   INT32 _rtnContextExplain::_prepareToExplain( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCONTEXTEXPLAIN__PREPARETOEXPLAIN ) ;
      BSONObj dummy ;
      INT64 queryContextID = -1 ;
      rtnContextBuf ctxBuf ;
      _optAccessPlan *plan = NULL ;
      const CHAR* hostName = NULL ;
      stringstream ss ;
      _rtnContextBase *contextOfQuery = NULL ;

      rc = rtnQuery( _options._fullName, _options._selector,
                     _options._query, _options._orderBy,
                     _options._hint, _options._flag,
                     cb, _options._skip, _options._limit,
                     sdbGetDMSCB(), sdbGetRTNCB(),
                     queryContextID, &contextOfQuery ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to query data:%d", rc ) ;
         goto error ;
      }

      if ( NULL == contextOfQuery )
      {
         PD_LOG( PDERROR, "can not get the context of query" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      plan = contextOfQuery->getPlan() ;
      if ( NULL == plan )
      {
         PD_LOG( PDERROR, "plan should not be NULL" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      _builder.append( FIELD_NAME_NAME, _options._fullName ) ;
      _builder.append( FIELD_NAME_SCANTYPE, IXSCAN == plan->getScanType() ?
                       VALUE_NAME_IXSCAN : VALUE_NAME_TBSCAN ) ;
      _builder.append( FIELD_NAME_INDEXNAME,
                       plan->getIndexName() ) ; 
      _builder.appendBool( FIELD_NAME_USE_EXT_SORT, plan->sortRequired() ) ;
      hostName = pmdGetKRCB()->getHostName() ;
      ss << hostName << ":" << pmdGetOptionCB()->getServiceAddr() ;
      _builder.append( FIELD_NAME_NODE_NAME, ss.str() ) ;

      rc = _getMonInfo( cb, _beginMon ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get mon info before explain:%d", rc ) ;
         goto error ;
      }
      ossGetCurrentTime( _beginTime ) ;

      _queryContextID = queryContextID ;
      _cbOfQuery = cb ;
   done:
      PD_TRACE_EXITRC( SDB_RTNCONTEXTEXPLAIN__PREPARETOEXPLAIN, rc ) ;
      return rc ;
   error:
      if ( -1 != queryContextID )
      {
         sdbGetRTNCB()->contextDelete( queryContextID,
                                       cb ) ;
      }
      goto done ;
   }

   INT32 _rtnContextExplain::_getMonInfo( _pmdEDUCB*cb, BSONObj &info )
   {
      INT32 rc = SDB_OK ;
      BSONObj dummy ;
      INT64 snapshotContextID = -1 ;
      rtnContextBuf ctxBuf ;
      rc = rtnSnapCommandEntry( CMD_SNAPSHOT_SESSIONS_CURRENT,
                                dummy, dummy, dummy,
                                0, cb, 0, -1, sdbGetDMSCB(),
                                sdbGetRTNCB(), snapshotContextID,
                                TRUE ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get snapshot of current session:%d", rc ) ;
         goto error ;
      }

      rc = rtnGetMore( snapshotContextID, 1, ctxBuf, cb, sdbGetRTNCB() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get more from snapshot context:%d", rc ) ;
         goto error ;
      }

      rc = ctxBuf.nextObj( info ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get next obj from buf:%d", rc ) ;
         goto error ;
      }

      info = info.getOwned() ;

   done:
      if ( -1 != snapshotContextID )
      {
         sdbGetRTNCB()->contextDelete( snapshotContextID,
                                       cb ) ;
      }
      return rc ;
   error:
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_SYS ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCONTEXTEXPLAIN__EXPLAINQUERY, "_rtnContextExplain::_explainQuery" )
   INT32 _rtnContextExplain::_explainQuery( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCONTEXTEXPLAIN__EXPLAINQUERY ) ;
      rtnContextBuf ctxBuf ;
      BSONObj record ;

      while ( _needRun )
      {
         rc = rtnGetMore( _queryContextID, -1, ctxBuf, cb, sdbGetRTNCB() ) ;
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_OK ;
            _queryContextID = -1 ;  /// context has been freed in getmore.
            break ;
         }
         else if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get more from context[%lld]"
                    ", rc:%d", _queryContextID, rc ) ;
            _queryContextID = -1 ;
            goto error ;
         }
         else
         {
            while ( TRUE )
            {
               rc = ctxBuf.nextObj( record ) ;
               if ( SDB_DMS_EOC == rc )
               {
                  rc = SDB_OK ;
                  break ;
               }
               else if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "Failed to get more from buf of "
                          "context[%lld],rc:%d ", _queryContextID, rc ) ;
                  goto error ;
               }
               else
               {
                  ++_recordNum ;
               }
            }
         }
      }

      ossGetCurrentTime( _endTime ) ;
      rc = _getMonInfo( cb, _endMon ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get mon info before explain:%d", rc ) ;
         goto error ;
      }
   done:
      if ( -1 != _queryContextID )
      {
         sdbGetRTNCB()->contextDelete( _queryContextID,
                                       _cbOfQuery ) ;
         _queryContextID = -1 ;
      }
      PD_TRACE_EXITRC( SDB_RTNCONTEXTEXPLAIN__EXPLAINQUERY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCONTEXTEXPLAIN__COMMITRESULT, "_rtnContextExplain::_commitResult" )
   INT32 _rtnContextExplain::_commitResult( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCONTEXTEXPLAIN__COMMITRESULT ) ;

      _builder.appendNumber( FIELD_NAME_RETURN_NUM, _recordNum ) ;
      UINT64 beginTime = _beginTime.time * 1000000 + _beginTime.microtm  ;
      UINT64 endTime = _endTime.time * 1000000 + _endTime.microtm  ;
      _builder.append( FIELD_NAME_ELAPSED_TIME,
                       FLOAT64( ( endTime - beginTime ) / 1000000.0 ) ) ; 
      
      BSONElement begin = _beginMon.getField( FIELD_NAME_TOTALINDEXREAD ) ;
      BSONElement end = _endMon.getField( FIELD_NAME_TOTALINDEXREAD ) ;
      if ( begin.isNumber() && end.isNumber() )
      {
         _builder.appendNumber( FIELD_NAME_INDEXREAD,
                                end.Long() - begin.Long() ) ;
      }

      begin = _beginMon.getField( FIELD_NAME_TOTALDATAREAD ) ;
      end = _endMon.getField( FIELD_NAME_TOTALDATAREAD ) ;
      if ( begin.isNumber() && end.isNumber() )
      {
         _builder.appendNumber( FIELD_NAME_DATAREAD,
                                end.Long() - begin.Long() ) ;
      }

      begin = _beginMon.getField( FIELD_NAME_USERCPU ) ;
      end = _endMon.getField( FIELD_NAME_USERCPU ) ;
      if ( begin.isNumber() && end.isNumber() )
      {
         _builder.append( FIELD_NAME_USERCPU,
                          FLOAT64( end.Number() - begin.Number() ) ) ;
      }
      
      begin = _beginMon.getField( FIELD_NAME_SYSCPU ) ;
      end = _endMon.getField( FIELD_NAME_SYSCPU ) ;
      if ( begin.isNumber() && end.isNumber() )
      {
         _builder.append( FIELD_NAME_SYSCPU,
                          FLOAT64( end.Number() - begin.Number() ) ) ;
      }

      rc = append( _builder.obj() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed append obj to context[%lld]:%d",
                 contextID(), rc ) ;
         goto error ;
      }
       
   done:
      PD_TRACE_EXITRC( SDB_RTNCONTEXTEXPLAIN__COMMITRESULT, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

