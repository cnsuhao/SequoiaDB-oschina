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

   Source File Name = dmsCB.cpp

   Descriptive Name = Data Management Service Control Block

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains code logic for
   data management control block, which is the metatdata information for DMS
   component.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "dmsCB.hpp"
#include "dms.hpp"
#include "dmsStorageUnit.hpp"
#include "ossLatch.hpp"
#include "ossUtil.hpp"
#include "monDMS.hpp"
#include "dmsTempCB.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "pdTrace.hpp"
#include "dmsTrace.hpp"
#include "dpsOp2Record.hpp"
#include "rtn.hpp"
#include <list>
using namespace std;
namespace engine
{

   enum DMS_LOCK_LEVEL
   {
      DMS_LOCK_NONE     = 0,
      DMS_LOCK_WRITE    = 1,     // for writable
      DMS_LOCK_WHOLE    = 2      // for backup or reorg
   } ;

   /*
      _SDB_DMS_CSCB implement
   */
   _SDB_DMS_CSCB::~_SDB_DMS_CSCB()
   {
      if ( _su )
      {
         SDB_OSS_DEL _su ;
      }
   }

   #define DMS_CS_MUTEX_BUCKET_SIZE          ( 128 )

   /*
      _SDB_DMSCB implement
   */

   _SDB_DMSCB::_SDB_DMSCB()
   :_writeCounter(0),
    _dmsCBState(DMS_STATE_NORMAL),
    _logicalSUID(0),
    _tempCB(this)
   {
      for ( UINT32 i = 0 ; i< DMS_MAX_CS_NUM ; ++i )
      {
         _cscbVec.push_back ( NULL ) ;
         _delCscbVec.push_back ( NULL ) ;
         _latchVec.push_back ( new(std::nothrow) ossRWMutex() ) ;
         _freeList.push_back ( i ) ;
      }

      for ( UINT32 i = 0 ; i < DMS_CS_MUTEX_BUCKET_SIZE ; ++i )
      {
         _vecCSMutex.push_back( new( std::nothrow ) ossSpinXLatch() ) ;
      }

      _backEvent.signal() ;
   }

   _SDB_DMSCB::~_SDB_DMSCB()
   {
   }

   INT32 _SDB_DMSCB::init ()
   {
      INT32 rc = SDB_OK ;

      if ( SDB_ROLE_COORD != pmdGetDBRole() )
      {
         rc = rtnLoadCollectionSpaces ( pmdGetOptionCB()->getDbPath(),
                                        pmdGetOptionCB()->getIndexPath(),
                                        pmdGetOptionCB()->getLobPath(),
                                        this ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to load collectionspaces, rc: %d",
                      rc ) ;
      }

      rc = _tempCB.init() ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init temp cb, rc: %d", rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _SDB_DMSCB::active ()
   {
      return SDB_OK ;
   }

   INT32 _SDB_DMSCB::deactive ()
   {
      return SDB_OK ;
   }

   INT32 _SDB_DMSCB::fini ()
   {
      _CSCBNameMapCleanup () ;
      for ( UINT32 i = 0 ; i < DMS_MAX_CS_NUM ; ++i )
      {
         if ( _latchVec[i] )
         {
            SDB_OSS_DEL _latchVec[i] ;
            _latchVec[i] = NULL ;
         }
      }
      for ( UINT32 i = 0 ; i < DMS_CS_MUTEX_BUCKET_SIZE ; ++i )
      {
         SDB_OSS_DEL _vecCSMutex[ i ] ;
         _vecCSMutex[ i ] = NULL ;
      }
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB__LGCSCBNMMAP, "_SDB_DMSCB::_logCSCBNameMap" )
   void _SDB_DMSCB::_logCSCBNameMap ()
   {
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB__LGCSCBNMMAP );
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      for ( it = _cscbNameMap.begin(); it != _cscbNameMap.end() ;
            it ++ )
      {
         PD_LOG ( PDDEBUG, "%s\n", it->first ) ;
      }
      PD_TRACE_EXIT ( SDB__SDB_DMSCB__LGCSCBNMMAP );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB__CSCBNMINST, "_SDB_DMSCB::_CSCBNameInsert" )
   INT32 _SDB_DMSCB::_CSCBNameInsert ( const CHAR *pName, UINT32 topSequence,
                                       _dmsStorageUnit *su,
                                       dmsStorageUnitID &suID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB__CSCBNMINST );
      SDB_DMS_CSCB *cscb = NULL ;
      if ( 0 == _freeList.size() )
      {
         rc = SDB_DMS_SU_OUTRANGE ;
         goto error ;
      }
      cscb = SDB_OSS_NEW SDB_DMS_CSCB(pName, topSequence, su) ;
      if ( !cscb )
      {
         PD_LOG ( PDERROR, "Failed to allocate memory to insert cscb" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      suID = _freeList.back() ;
      su->_setCSID( suID ) ;
      su->_setLogicalCSID( _logicalSUID++ ) ;
      _freeList.pop_back() ;
      _cscbNameMap[cscb->_name] = suID ;
      _cscbVec[suID] = cscb ;

   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB__CSCBNMINST, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _SDB_DMSCB::_CSCBNameLookup ( const CHAR *pName,
                                       SDB_DMS_CSCB **cscb )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT( cscb, "cscb can't be null!" );
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      if ( _cscbNameMap.end() == (it = _cscbNameMap.find(pName)) )
      {
         rc = SDB_DMS_CS_NOTEXIST ;
         goto error;
      }
      *cscb = _cscbVec[(*it).second];
      if ( *cscb != NULL )
      {
         goto done;
      }
      if( _delCscbVec[(*it).second] )
      {
         rc = SDB_DMS_CS_DELETING;
      }
      else
      {
         SDB_ASSERT( FALSE, "faint, why the cs is in map?" );
         rc = SDB_DMS_CS_NOTEXIST;
      }
      goto error;
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _SDB_DMSCB::_CSCBNameLookupAndLock ( const CHAR *pName,
                                              dmsStorageUnitID &suID,
                                              SDB_DMS_CSCB **cscb,
                                              OSS_LATCH_MODE lockType,
                                              INT32 millisec )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT( cscb, "cscb can't be null!" );
      suID = DMS_INVALID_CS ;
      *cscb = NULL;
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      if ( _cscbNameMap.end() == (it = _cscbNameMap.find(pName)) )
      {
         return SDB_DMS_CS_NOTEXIST ;
      }
      dmsStorageUnitID temp_suID = (*it).second ;
      if ( NULL == _cscbVec[temp_suID] )
      {
         if ( NULL == _delCscbVec[temp_suID] )
         {
            return SDB_DMS_CS_NOTEXIST ;
         }
         return SDB_DMS_CS_DELETING ;
      }

      if ( EXCLUSIVE == lockType )
      {
         rc = _latchVec[temp_suID]->lock_w( millisec ) ;
      }
      else
      {
         rc = _latchVec[temp_suID]->lock_r( millisec ) ;
      }
      if ( SDB_OK == rc )
      {
         suID = temp_suID ;
         *cscb = _cscbVec[suID] ;
      }
      return rc;
   }

   void _SDB_DMSCB::_CSCBRelease ( dmsStorageUnitID suID,
                                   OSS_LATCH_MODE lockType )
   {
      if ( EXCLUSIVE == lockType )
      {
         _latchVec[suID]->release_w();
      }
      else
      {
         _latchVec[suID]->release_r() ;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB__CSCBNMREMV, "_SDB_DMSCB::_CSCBNameRemove" )
   INT32 _SDB_DMSCB::_CSCBNameRemove ( const CHAR *pName,
                                       _pmdEDUCB *cb,
                                       SDB_DPSCB *dpsCB,
                                       BOOLEAN onlyEmpty,
                                       SDB_DMS_CSCB *&pCSCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB__CSCBNMREMV );
      dmsStorageUnitID suID ;
      UINT32 csLID = ~0 ;
      dpsTransCB *pTransCB = pmdGetKRCB()->getTransCB();
      BOOLEAN isTransLocked = FALSE;
      BOOLEAN isReserved = FALSE;
      UINT32 logRecSize = 0;
      dpsMergeInfo info ;
      dpsLogRecord &record = info.getMergeBlock().record();
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      std::vector<SDB_DMS_CSCB*> *pVecCscb = NULL ;

      pCSCB = NULL ;
      _mutex.get_shared () ;
      if ( _cscbNameMap.end() == (it = _cscbNameMap.find( pName )))
      {
         _mutex.release_shared () ;
         rc = SDB_DMS_CS_NOTEXIST ;
         goto error ;
      }
      suID = (*it).second ;
      _mutex.release_shared () ;

      if ( NULL != dpsCB )
      {
         rc = dpsCSDel2Record( pName, record ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build record:%d",rc ) ;
            goto error ;
         }
         rc = dpsCB->checkSyncControl( record.alignedLen(), cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d", rc ) ;

         logRecSize = record.alignedLen() ;
         rc = pTransCB->reservedLogSpace( logRecSize );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to reserved log space(length=%u)",
                     logRecSize );
         isReserved = TRUE ;
      }

   retry :
      if ( SDB_OK != _latchVec[suID]->lock_w( OSS_ONE_SEC ) )
      {
         rc = SDB_LOCK_FAILED ;
         goto error ;
      }
      if ( !_mutex.try_get() )
      {
         _latchVec[suID]->release_w () ;
         goto retry ;
      }

      _latchVec[suID]->release_w () ;

      if ( _cscbNameMap.end() == (it = _cscbNameMap.find(pName)) ||
           suID != (*it).second )
      {
         _mutex.release () ;
         rc = SDB_DMS_CS_NOTEXIST ;
         goto error ;
      }

      if ( _cscbVec[suID] )
      {
         pVecCscb = &_cscbVec ;
      }
      else if ( _delCscbVec[suID] )
      {
        pVecCscb = &_delCscbVec ;
      }

      if ( pVecCscb )
      {
         pCSCB = (*pVecCscb)[ suID ] ;
         SDB_ASSERT ( pCSCB->_su, "su can't be null" ) ;

         if ( onlyEmpty && 0 != pCSCB->_su->data()->getCollectionNum() )
         {
            _mutex.release () ;
            rc = SDB_DMS_CS_NOT_EMPTY ;
            goto error ;
         }

         csLID = pCSCB->_su->LogicalCSID() ;
         if ( cb )
         {
            rc = pTransCB->transLockTryX( cb, csLID );
            if ( rc )
            {
               _mutex.release () ;
               PD_LOG ( PDERROR, "Failed to lock collection-space(rc=%d)", rc ) ;
               goto error ;
            }
            isTransLocked = TRUE ;
         }
         (*pVecCscb)[suID] = NULL ;
      }

      _cscbNameMap.erase(pName) ;
      _freeList.push_back ( suID ) ;

      if ( dpsCB )
      {
         info.setInfoEx( csLID, ~0, DMS_INVALID_EXTENT, cb ) ;
         rc = dpsCB->prepare ( info ) ;
         if ( rc )
         {
            _mutex.release () ;
            PD_LOG ( PDERROR, "Failed to insert cscrt into log, rc = %d", rc ) ;
            goto error ;
         }
         dpsCB->writeData( info ) ;
      }

      _mutex.release () ;

   done :
      if ( isTransLocked )
      {
         pTransCB->transLockRelease( cb, csLID );
      }
      if ( isReserved )
      {
         pTransCB->releaseLogSpace( logRecSize );
      }
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB__CSCBNMREMV, rc );
      return rc ;
   error :
      pCSCB = NULL ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB__CSCBNMREMVP1, "_SDB_DMSCB::_CSCBNameRemoveP1" )
   INT32 _SDB_DMSCB::_CSCBNameRemoveP1 ( const CHAR *pName,
                                         _pmdEDUCB *cb,
                                         SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB__CSCBNMREMVP1 );
      dmsStorageUnitID suID ;
      SDB_DMS_CSCB *pCSCB = NULL;
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::iterator it ;
#endif
      _mutex.get_shared () ;
      if ( _cscbNameMap.end() == (it = _cscbNameMap.find( pName )))
      {
         _mutex.release_shared () ;
         rc = SDB_DMS_CS_NOTEXIST ;
         goto error ;
      }
      suID = (*it).second ;
      _mutex.release_shared () ;

   retry :
      if ( SDB_OK != _latchVec[suID]->lock_w( OSS_ONE_SEC ) )
      {
         rc = SDB_LOCK_FAILED ;
         goto error ;
      }
      if ( !_mutex.try_get() )
      {
         _latchVec[suID]->release_w () ;
         goto retry ;
      }

      _latchVec[suID]->release_w () ;

      if ( _cscbNameMap.end() == (it = _cscbNameMap.find(pName)) ||
           suID != (*it).second )
      {
         _mutex.release () ;
         rc = SDB_DMS_CS_NOTEXIST ;
         goto error ;
      }

      if ( _cscbVec[suID] )
      {
         pCSCB = _cscbVec[suID] ;
         SDB_ASSERT ( pCSCB->_name, "cs-name can't be null" );
         _delCscbVec[ suID ] = pCSCB;
         _cscbVec[suID] = NULL;
      }
      else
      {
         if ( _delCscbVec[suID] )
         {
            rc = SDB_DMS_CS_DELETING;
         }
         else
         {
            SDB_ASSERT( FALSE, "faint, why cs is in map?" );
            PD_LOG( PDWARNING, "couldn't find the cscb(name:%s)", pName );
            _cscbNameMap.erase( it );
            rc = SDB_DMS_CS_NOTEXIST ;
         }
         _mutex.release () ;
         goto error ;
      }

      _mutex.release () ;

   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB__CSCBNMREMVP1, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB__CSCBNMREMVP1CANCEL, "_SDB_DMSCB::_CSCBNameRemoveP1Cancel" )
   INT32 _SDB_DMSCB::_CSCBNameRemoveP1Cancel ( const CHAR *pName,
                                             _pmdEDUCB *cb,
                                             SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB__CSCBNMREMVP1CANCEL );
      dmsStorageUnitID suID ;
      SDB_DMS_CSCB *pCSCB = NULL;
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif

      {
         ossScopedLock _lock(&_mutex, EXCLUSIVE) ;
         if ( _cscbNameMap.end() == (it = _cscbNameMap.find(pName)) )
         {
            SDB_ASSERT( FALSE, "faint, why the cs is not in the map?" );
            rc = SDB_DMS_CS_NOTEXIST;
            goto error;
         }
         suID = (*it).second;
         pCSCB = _delCscbVec[suID];
         if ( pCSCB )
         {
            SDB_ASSERT( pCSCB->_su, "su can't be null!" );
            _cscbVec[suID] = pCSCB;
            _delCscbVec[suID] = NULL;
         }
         else
         {
            SDB_ASSERT( FALSE, "faint, where is the CSCB?" );
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB__CSCBNMREMVP1CANCEL, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB__CSCBNMREMVP2, "_SDB_DMSCB::_CSCBNameRemoveP2" )
   INT32 _SDB_DMSCB::_CSCBNameRemoveP2 ( const CHAR *pName,
                                       _pmdEDUCB *cb,
                                       SDB_DPSCB *dpsCB,
                                       SDB_DMS_CSCB *&pCSCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB__CSCBNMREMVP2 );
      dmsStorageUnitID suID ;
      UINT32 csLID = ~0 ;
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::iterator it ;
#endif
      pCSCB = NULL ;
      {
         ossScopedLock _lock(&_mutex, EXCLUSIVE) ;
         if ( _cscbNameMap.end() == (it = _cscbNameMap.find(pName)) )
         {
            SDB_ASSERT( FALSE, "faint, why the cs is not in map?" );

            rc = SDB_DMS_CS_NOTEXIST ;
            goto error ;
         }
         suID = (*it).second ;
         if ( _delCscbVec[suID] )
         {
            pCSCB = _delCscbVec[suID] ;
            SDB_ASSERT ( pCSCB->_su, "su can't be null" ) ;
            csLID = pCSCB->_su->LogicalCSID () ;
            _delCscbVec[suID] = NULL ;
         }

         _cscbNameMap.erase(it) ;
         _freeList.push_back ( suID ) ;

         if ( dpsCB )
         {
            dpsMergeInfo info ;
            info.setInfoEx( csLID, ~0, DMS_INVALID_EXTENT, cb ) ;

            dpsLogRecord &record = info.getMergeBlock().record();

            rc = dpsCSDel2Record( pName, record ) ;
            PD_RC_CHECK( rc, PDERROR, "failed to build record:%d", rc );

            rc = dpsCB->prepare ( info ) ;
            if ( rc )
            {
               _mutex.release () ;
               PD_LOG ( PDERROR, "Failed to insert cscrt into log, rc = %d", rc ) ;
               goto error ;
            }
            dpsCB->writeData( info ) ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB__CSCBNMREMVP2, rc );
      return rc ;
   error :
      pCSCB = NULL ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB__CSCBNMMAPCLN, "_SDB_DMSCB::_CSCBNameMapCleanup" )
   void _SDB_DMSCB::_CSCBNameMapCleanup ()
   {
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB__CSCBNMMAPCLN );
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      for ( it = _cscbNameMap.begin(); it != _cscbNameMap.end(); it++ )
      {
         dmsStorageUnitID suID = (*it).second ;

         _freeList.push_back ( suID ) ;
         if ( _cscbVec[suID] )
         {
            SDB_OSS_DEL _cscbVec[suID] ;
            _cscbVec[suID] = NULL ;
         }
         if ( _delCscbVec[suID] )
         {
            SDB_OSS_DEL _delCscbVec[suID] ;
            _delCscbVec[suID] = NULL ;
         }
      }
      _cscbNameMap.clear() ;
      PD_TRACE_EXIT ( SDB__SDB_DMSCB__CSCBNMMAPCLN );
   }

   INT32 _SDB_DMSCB::writable( _pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN locked = FALSE ;

      if ( cb && cb->getDmsLockLevel() >= DMS_LOCK_WRITE )
      {
         ++_writeCounter ;
         goto done ;
      }

   retry:
      _stateMtx.get () ;
      locked = TRUE ;

      switch ( _dmsCBState )
      {
      case DMS_STATE_BACKUP :
         rc = SDB_RTN_IN_BACKUP ;
         break ;
      case DMS_STATE_REBUILD :
         rc = SDB_RTN_IN_REBUILD ;
         break ;
      default :
         break ;
      }
      if ( SDB_OK == rc )
      {
         ++_writeCounter ;
         if ( cb )
         {
            cb->setDmsLockLevel( DMS_LOCK_WRITE ) ;
         }
      }
      else if ( cb )
      {
         _stateMtx.release() ;
         locked = FALSE ;

         while ( TRUE )
         {
            if ( cb->isInterrupted() )
            {
               rc = SDB_APP_INTERRUPT ;
               break ;
            }
            rc = _backEvent.wait( OSS_ONE_SEC ) ;
            if ( SDB_OK == rc )
            {
               goto retry ;
            }
         }
      }

   done:
      if ( locked )
      {
         _stateMtx.release() ;
      }
      return rc;
   }

   void _SDB_DMSCB::writeDown( _pmdEDUCB * cb )
   {
      _stateMtx.get();
      --_writeCounter;
      SDB_ASSERT( 0 <= _writeCounter, "write counter should not < 0" ) ;
      _stateMtx.release();

      if ( cb && DMS_LOCK_WRITE == cb->getDmsLockLevel() )
      {
         cb->setDmsLockLevel( DMS_LOCK_NONE ) ;
      }
   }

   INT32 _SDB_DMSCB::registerBackup( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;

      _stateMtx.get();
      if ( DMS_STATE_NORMAL != _dmsCBState )
      {
         if ( DMS_STATE_BACKUP == _dmsCBState )
            rc = SDB_BACKUP_HAS_ALREADY_START ;
         else
            rc = SDB_DMS_STATE_NOT_COMPATIBLE ;
         _stateMtx.release () ;
         goto done;
      }
      _dmsCBState = DMS_STATE_BACKUP ;
      _stateMtx.release () ;

      while ( TRUE )
      {
         _stateMtx.get();
         if ( 0 == _writeCounter )
         {
            _backEvent.reset() ;
            _stateMtx.release();
            if ( cb )
            {
               cb->setDmsLockLevel( DMS_LOCK_WHOLE ) ;
            }
            goto done;
         }
         else
         {
            _stateMtx.release();
            ossSleepmillis( DMS_CHANGESTATE_WAIT_LOOP );
         }
      }

   done:
      return rc ;
   }

   void _SDB_DMSCB::backupDown( _pmdEDUCB *cb )
   {
      _stateMtx.get() ;
      _dmsCBState = DMS_STATE_NORMAL ;
      _backEvent.signalAll() ;
      _stateMtx.release() ;
      if ( cb )
      {
         cb->setDmsLockLevel( DMS_LOCK_NONE ) ;
      }
   }

   INT32 _SDB_DMSCB::registerRebuild( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;

      _stateMtx.get();
      if ( DMS_STATE_NORMAL != _dmsCBState )
      {
         if ( DMS_STATE_REBUILD == _dmsCBState )
            rc = SDB_REBUILD_HAS_ALREADY_START ;
         else
            rc = SDB_DMS_STATE_NOT_COMPATIBLE ;
         _stateMtx.release () ;
         goto done;
      }
      _dmsCBState = DMS_STATE_REBUILD ;
      _stateMtx.release () ;

      while ( TRUE )
      {
         _stateMtx.get();
         if ( 0 == _writeCounter )
         {
            _stateMtx.release();
            if ( cb )
            {
               cb->setDmsLockLevel( DMS_LOCK_WRITE ) ;
            }
            goto done;
         }
         else
         {
            _stateMtx.release();
            ossSleepmillis( DMS_CHANGESTATE_WAIT_LOOP );
         }
      }
   done :
      return rc ;
   }

   void _SDB_DMSCB::rebuildDown( _pmdEDUCB *cb )
   {
      _stateMtx.get();
      _dmsCBState = DMS_STATE_NORMAL;
      _stateMtx.release();

      if ( cb )
      {
         cb->setDmsLockLevel( DMS_LOCK_NONE ) ;
      }
   }

   INT32 _SDB_DMSCB::nameToSUAndLock ( const CHAR *pName,
                                       dmsStorageUnitID &suID,
                                       _dmsStorageUnit **su,
                                       OSS_LATCH_MODE lockType,
                                       INT32 millisec )
   {
      INT32 rc = SDB_OK;
      SDB_DMS_CSCB *cscb = NULL;
      SDB_ASSERT( su, "su can't be null!" );
      if ( !pName )
      {
         return SDB_INVALIDARG ;
      }
      ossScopedLock _lock(&_mutex, SHARED) ;
      rc = _CSCBNameLookupAndLock( pName, suID,
                                   &cscb, lockType,
                                   millisec ) ;
      if ( SDB_OK == rc )
      {
         *su = cscb->_su;
      }
      return rc ;
   }

   _dmsStorageUnit *_SDB_DMSCB::suLock ( dmsStorageUnitID suID )
   {
      ossScopedLock _lock(&_mutex, SHARED) ;
      if ( NULL == _cscbVec[suID] )
      {
         return NULL ;
      }
      _latchVec[suID]->lock_r() ;
      return _cscbVec[suID]->_su ;
   }

   void _SDB_DMSCB::suUnlock ( dmsStorageUnitID suID,
                              OSS_LATCH_MODE lockType )
   {
      _CSCBRelease( suID, lockType ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_ADDCS, "_SDB_DMSCB::addCollectionSpace" )
   INT32 _SDB_DMSCB::addCollectionSpace(const CHAR * pName,
                                        UINT32 topSequence,
                                        _dmsStorageUnit * su,
                                        _pmdEDUCB *cb,
                                        SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      dmsStorageUnitID suID ;
      SDB_DMS_CSCB *cscb = NULL ;
      BOOLEAN isReserved = FALSE;
      UINT32 logRecSize = 0;
      dpsMergeInfo info ;
      dpsLogRecord &record = info.getMergeBlock().record();
      INT32 pageSize = 0 ;
      INT32 lobPageSz = 0 ;
      dpsTransCB *pTransCB = pmdGetKRCB()->getTransCB();
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_ADDCS );

      ossScopedLock _lock(&_mutex, EXCLUSIVE) ;
      if ( !pName || !su )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = _CSCBNameLookup( pName, &cscb ) ;
      if ( SDB_OK == rc )
      {
         rc = SDB_DMS_CS_EXIST;
         goto error;
      }
      else if ( rc != SDB_DMS_CS_NOTEXIST )
      {
         goto error;
      }

      pageSize = su->getPageSize() ;
      lobPageSz = su->getLobPageSize() ;
      if ( NULL != dpsCB )
      {
         rc = dpsCSCrt2Record( pName, pageSize, lobPageSz, record ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build record:%d",rc ) ;
            goto error ;
         }
         rc = dpsCB->checkSyncControl( record.alignedLen(), cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d", rc ) ;

         logRecSize = record.alignedLen() ;
         rc = pTransCB->reservedLogSpace( logRecSize );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to reserved log space(length=%u)",
                     logRecSize );
         isReserved = TRUE ;
      }

      rc = _CSCBNameInsert ( pName, topSequence, su, suID ) ;
      if ( SDB_OK == rc )
      {
         INT32 tempRC = SDB_OK ;
         tempRC = _joinPageCleanSU ( suID ) ;
         if ( tempRC )
         {
            PD_LOG ( PDWARNING,
                     "Failed to join storage unit to page clean history, "
                     "rc = %d", tempRC ) ;
         }
      }
      if ( SDB_OK == rc && dpsCB )
      {
         UINT32 suLID = su->LogicalCSID() ;
         info.setInfoEx( suLID, ~0, DMS_INVALID_EXTENT, cb ) ;
         rc = dpsCB->prepare ( info ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to insert cscrt into log, rc = %d", rc ) ;
            goto error ;
         }
         dpsCB->writeData( info ) ;
      }
   done :
      if ( isReserved )
      {
         pTransCB->releaseLogSpace( logRecSize );
      }
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB_ADDCS, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DELCS, "_SDB_DMSCB::_delCollectionSpace" )
   INT32 _SDB_DMSCB::_delCollectionSpace( const CHAR * pName, _pmdEDUCB * cb,
                                          SDB_DPSCB * dpsCB,
                                          BOOLEAN removeFile,
                                          BOOLEAN onlyEmpty )
   {
      INT32 rc = SDB_OK ;
      SDB_DMS_CSCB *pCSCB = NULL ;

      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DELCS ) ;

      if ( !pName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      {
         _mutex.get_shared() ;
         SDB_DMS_CSCB *cscb = NULL;
         rc = _CSCBNameLookup( pName, &cscb ) ;
         if ( rc )
         {
            _mutex.release_shared() ;
            goto error ;
         }
         dmsStorageUnit *su = cscb->_su ;
         SDB_ASSERT ( su, "storage unit pointer can't be NULL" ) ;

         _mutex.release_shared() ;
         rc = _CSCBNameRemove ( pName, cb, dpsCB, onlyEmpty, pCSCB ) ;

         if ( SDB_OK == rc && pCSCB )
         {
            if ( removeFile )
            {
               rc = pCSCB->_su->remove() ;
            }
            else
            {
               pCSCB->_su->close() ;
            }
            SDB_OSS_DEL pCSCB ;

            if ( rc )
            {
               goto error ;
            }
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB_DELCS, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _SDB_DMSCB::dropCollectionSpace ( const CHAR *pName, _pmdEDUCB *cb,
                                           SDB_DPSCB *dpsCB )
   {
      aquireCSMutex( pName ) ;
      INT32 rc = _delCollectionSpace( pName, cb, dpsCB, TRUE, FALSE ) ;
      releaseCSMutex( pName ) ;
      return rc ;
   }

   INT32 _SDB_DMSCB::unloadCollectonSpace( const CHAR *pName, _pmdEDUCB *cb )
   {
      return _delCollectionSpace( pName, cb, NULL, FALSE, FALSE ) ;
   }

   INT32 _SDB_DMSCB::dropEmptyCollectionSpace( const CHAR *pName,
                                               _pmdEDUCB *cb,
                                               SDB_DPSCB *dpsCB )
   {
      aquireCSMutex( pName ) ;
      INT32 rc = _delCollectionSpace( pName, cb, dpsCB, TRUE, TRUE ) ;
      releaseCSMutex( pName ) ;
      if ( SDB_LOCK_FAILED == rc )
      {
         rc = SDB_DMS_CS_NOT_EMPTY ;
      }
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DROPCSP1, "_SDB_DMSCB::dropCollectionSpaceP1" )
   INT32 _SDB_DMSCB::dropCollectionSpaceP1 ( const CHAR *pName, _pmdEDUCB *cb,
                                             SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DROPCSP1 ) ;
      if ( !pName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      {
         _mutex.get_shared() ;
         SDB_DMS_CSCB *cscb = NULL;
         rc = _CSCBNameLookup( pName, &cscb ) ;
         if ( rc )
         {
            _mutex.release_shared() ;
            goto error ;
         }
         dmsStorageUnit *su = cscb->_su ;
         SDB_ASSERT ( su, "storage unit pointer can't be NULL" ) ;

         _mutex.release_shared() ;
         rc = _CSCBNameRemoveP1( pName, cb, dpsCB ) ;
         PD_RC_CHECK( rc, PDERROR,
                     "failed to drop cs(rc=%d)", rc );
      }
   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB_DROPCSP1, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DROPCSP1CANCEL, "_SDB_DMSCB::dropCollectionSpaceP1Cancel" )
   INT32 _SDB_DMSCB::dropCollectionSpaceP1Cancel ( const CHAR *pName, _pmdEDUCB *cb,
                                                   SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DROPCSP1CANCEL ) ;
      if ( !pName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = _CSCBNameRemoveP1Cancel( pName, cb, dpsCB );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to cancel remove cs(rc=%d)",
                  rc );
   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB_DROPCSP1CANCEL, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DROPCSP2, "_SDB_DMSCB::dropCollectionSpaceP2" )
   INT32 _SDB_DMSCB::dropCollectionSpaceP2 ( const CHAR *pName, _pmdEDUCB *cb,
                                             SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      SDB_DMS_CSCB *pCSCB = NULL ;

      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DROPCSP2 ) ;
      if ( !pName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      {
         rc = _CSCBNameRemoveP2( pName, cb, dpsCB, pCSCB ) ;

         if ( SDB_OK == rc && pCSCB )
         {
            rc = pCSCB->_su->remove() ;
            SDB_OSS_DEL pCSCB ;
            PD_RC_CHECK( rc, PDERROR,
                        "remove failed(rc=%d)", rc );
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB_DROPCSP2, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DUMPINFO, "_SDB_DMSCB::dumpInfo" )
   void _SDB_DMSCB::dumpInfo ( std::set<monCollection> &collectionList,
                               BOOLEAN sys )
   {
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DUMPINFO );
      dmsStorageUnit *su = NULL ;
      ossScopedLock _lock(&_mutex, SHARED) ;

#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      for ( it = _cscbNameMap.begin(); it != _cscbNameMap.end(); it++ )
      {
         su = NULL ;
         dmsStorageUnitID suID = (*it).second ;
         ossScopedRWLock lock ( _latchVec[suID], SHARED ) ;
         SDB_DMS_CSCB *cscb = _cscbVec[suID] ;
         if ( !cscb )
            continue ;
         su = cscb->_su ;
         SDB_ASSERT ( su, "storage unit pointer can't be NULL" ) ;

         if ( ( !sys && dmsIsSysCSName(su->CSName()) ) ||
              ( ossStrcmp ( su->CSName(), SDB_DMSTEMP_NAME ) == 0 ) )
         {
            continue ;
         }
         su->dumpInfo ( collectionList, sys ) ;
      } // for ( it = _cscbNameMap.begin(); it != _cscbNameMap.end(); it++ )
      PD_TRACE_EXIT ( SDB__SDB_DMSCB_DUMPINFO );
   }  // void dumpInfo

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DUMPINFO2, "_SDB_DMSCB::dumpInfo" )
   void _SDB_DMSCB::dumpInfo ( std::set<monCollectionSpace> &csList,
                               BOOLEAN sys )
   {
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DUMPINFO2 );
      dmsStorageUnit *su = NULL ;
      INT64 totalDataFreeSize    = 0 ;
      INT64 totalIndexFreeSize   = 0 ;
      INT64 totalLobFreeSize     = 0 ;

      ossScopedLock _lock(&_mutex, SHARED) ;
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      for ( it = _cscbNameMap.begin(); it != _cscbNameMap.end(); it++ )
      {
         dmsStorageUnitID suID = (*it).second ;
         ossScopedRWLock lock ( _latchVec[suID], SHARED ) ;
         SDB_DMS_CSCB *cscb = _cscbVec[suID] ;
         if ( !cscb )
         {
            continue ;
         }
         su = cscb->_su ;
         SDB_ASSERT ( su, "storage unit pointer can't be NULL" ) ;
         if ( !sys && dmsIsSysCSName(cscb->_name) )
         {
            continue ;
         }
         else if ( dmsIsSysCSName(cscb->_name) &&
                   0 == ossStrcmp(cscb->_name, SDB_DMSTEMP_NAME ) )
         {
            continue ;
         }
         monCollectionSpace cs ;
         dmsStorageUnitStat statInfo ;

         su->getStatInfo( statInfo ) ;
         totalDataFreeSize    = su->totalFreeSize( DMS_SU_DATA ) +
                                statInfo._totalDataFreeSpace ;
         totalIndexFreeSize   = su->totalFreeSize( DMS_SU_INDEX ) +
                                statInfo._totalIndexFreeSpace ;
         totalLobFreeSize     = su->totalFreeSize( DMS_SU_LOB ) ;

         ossMemset ( cs._name, 0, sizeof(cs._name) ) ;
         ossStrncpy ( cs._name, cscb->_name, DMS_COLLECTION_SPACE_NAME_SZ);
         cs._pageSize = su->getPageSize() ;
         cs._lobPageSize = su->getLobPageSize() ;
         cs._totalSize = su->totalSize() ;
         cs._clNum    = statInfo._clNum ;
         cs._totalRecordNum = statInfo._totalCount ;
         cs._freeSize = totalDataFreeSize + totalIndexFreeSize +
                        totalLobFreeSize ;
         cs._totalDataSize = su->totalSize( DMS_SU_DATA ) ;
         cs._freeDataSize  = totalDataFreeSize ;
         cs._totalIndexSize = su->totalSize( DMS_SU_INDEX ) ;
         cs._freeIndexSize = totalIndexFreeSize ;
         cs._totalLobSize = su->totalSize( DMS_SU_LOB ) ;
         cs._freeLobSize = totalLobFreeSize ;
         su->dumpInfo ( cs._collections, sys ) ;
         csList.insert ( cs ) ;
      }
      PD_TRACE_EXIT ( SDB__SDB_DMSCB_DUMPINFO2 );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DUMPINFO3, "_SDB_DMSCB::dumpInfo" )
   void _SDB_DMSCB::dumpInfo ( std::set<monStorageUnit> &storageUnitList,
                               BOOLEAN sys )
   {
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DUMPINFO3 );
      dmsStorageUnit *su = NULL ;
      ossScopedLock _lock(&_mutex, SHARED) ;

#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      for ( it = _cscbNameMap.begin(); it != _cscbNameMap.end(); it++ )
      {
         su = NULL ;
         dmsStorageUnitID suID = (*it).second ;
         ossScopedRWLock lock ( _latchVec[suID], SHARED ) ;
         SDB_DMS_CSCB *cscb = _cscbVec[suID] ;
         if ( !cscb )
         {
            continue ;
         }
         su = cscb->_su ;
         SDB_ASSERT ( su, "storage unit pointer can't be NULL" ) ;
         if ( !sys && dmsIsSysCSName(su->CSName()) )
         {
            continue ;
         }
         su->dumpInfo ( storageUnitList, sys ) ;
      } // for ( it = _cscbNameMap.begin(); it != _cscbNameMap.end(); it++ )
      PD_TRACE_EXIT ( SDB__SDB_DMSCB_DUMPINFO3 );
   }  // void dumpInfo

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DUMPINFO4, "_SDB_DMSCB::dumpInfo" )
   void _SDB_DMSCB::dumpInfo ( INT64 &totalFileSize )
   {
      totalFileSize = 0;
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DUMPINFO4 );
      dmsStorageUnit *su = NULL ;
      ossScopedLock _lock(&_mutex, SHARED) ;
#if defined (_WINDOWS)
      std::map<const CHAR*, dmsStorageUnitID, cmp_cscb>::const_iterator it ;
#elif defined (_LINUX)
      std::map<const CHAR*, dmsStorageUnitID>::const_iterator it ;
#endif
      for ( it = _cscbNameMap.begin(); it != _cscbNameMap.end(); it++ )
      {
         su = NULL ;
         dmsStorageUnitID suID = (*it).second ;
         ossScopedRWLock lock ( _latchVec[suID], SHARED ) ;
         SDB_DMS_CSCB *cscb = _cscbVec[suID] ;
         if ( !cscb )
         {
            continue ;
         }
         su = cscb->_su ;
         SDB_ASSERT ( su, "storage unit pointer can't be NULL" );
         totalFileSize += su->totalSize();
      }
      PD_TRACE_EXIT ( SDB__SDB_DMSCB_DUMPINFO4 );
   }

   dmsTempCB *_SDB_DMSCB::getTempCB ()
   {
      return &_tempCB ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_DISPATCHPAGECLEANSU, "_SDB_DMSCB::dispatchPageCleanSU" )
   _dmsStorageUnit *_SDB_DMSCB::dispatchPageCleanSU ( dmsStorageUnitID *suID )
   {
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_DISPATCHPAGECLEANSU ) ;
      *suID               = DMS_INVALID_SUID ;
      pmdOptionsCB *optCB = pmdGetOptionCB() ;
      _dmsStorageUnit *su = NULL ;
      SDB_ASSERT ( suID, "suID can't be NULL" ) ;
      _pageCleanHistory firstSU ;
      ossTickDelta deltaTime ;

      ossScopedLock _lock(&_mutex, EXCLUSIVE) ;

      if ( _pageCleanHistoryList.size() == 0 )
         goto done ;
      firstSU = _pageCleanHistoryList.front() ;
      deltaTime = pmdGetKRCB()->getCurTime () - firstSU.first ;
      if ( deltaTime.toUINT64() / 1000 >
           (UINT64)optCB->getPageCleanInterval() )
      {
         PD_TRACE1 ( SDB__SDB_DMSCB_DISPATCHPAGECLEANSU,
                     PD_PACK_ULONG ( firstSU.second ) ) ;
         if ( NULL != _cscbVec[firstSU.second] )
         {
            *suID = firstSU.second ;
            _latchVec[*suID]->lock_r() ;
            su = _cscbVec[*suID]->_su ;
         }
         _pageCleanHistorySet.erase ( firstSU.second ) ;
         _pageCleanHistoryList.pop_front () ;
      }
   done :
      PD_TRACE_EXIT ( SDB__SDB_DMSCB_DISPATCHPAGECLEANSU ) ;
      return su ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB_JOINPAGECLEANSU, "_SDB_DMSCB::joinPageCleanSU" )
   INT32 _SDB_DMSCB::joinPageCleanSU ( dmsStorageUnitID suID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB_JOINPAGECLEANSU ) ;

      ossScopedLock _lock(&_mutex, EXCLUSIVE) ;
      rc = _joinPageCleanSU ( suID ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR,
                  "Failed to join storage unit into history list, rc = %d",
                  rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB_JOINPAGECLEANSU, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_DMSCB__JOINPAGECLEANSU, "_SDB_DMSCB::_joinPageCleanSU" )
   INT32 _SDB_DMSCB::_joinPageCleanSU ( dmsStorageUnitID suID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__SDB_DMSCB__JOINPAGECLEANSU ) ;

      SDB_DMS_CSCB *cscb = _cscbVec [ suID ] ;
      if ( cscb &&
           0 == _pageCleanHistorySet.count ( suID ) )
      {
         _pageCleanHistoryList.push_back ( std::make_pair(
               pmdGetKRCB()->getCurTime (), suID ) ) ;
         _pageCleanHistorySet.insert ( suID ) ;
      }
      else if ( !cscb )
      {
         rc = SDB_DMS_CS_NOTEXIST ;
         goto error ;
      }
      PD_TRACE_EXITRC ( SDB__SDB_DMSCB__JOINPAGECLEANSU, rc ) ;
   done :
      return rc ;
   error :
      goto done ;
   }

   void _SDB_DMSCB::aquireCSMutex( const CHAR *pCSName )
   {
      UINT32 pos = ossHash( pCSName ) % DMS_CS_MUTEX_BUCKET_SIZE ;
      _vecCSMutex[ pos ]->get() ;
   }

   void _SDB_DMSCB::releaseCSMutex( const CHAR *pCSName )
   {
      UINT32 pos = ossHash( pCSName ) % DMS_CS_MUTEX_BUCKET_SIZE ;
      _vecCSMutex[ pos ]->release() ;
   }

   /*
      get global SDB_DMSCB
   */
   SDB_DMSCB* sdbGetDMSCB ()
   {
      static SDB_DMSCB s_dmsCB ;
      return &s_dmsCB ;
   }
}

