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

   Source File Name = dmsStorageData.cpp

   Descriptive Name = Data Management Service Storage Unit Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          14/08/2013  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsStorageData.hpp"
#include "dmsStorageIndex.hpp"
#include "dmsStorageLob.hpp"
#include "pmd.hpp"
#include "dpsTransCB.hpp"
#include "dpsOp2Record.hpp"
#include "mthModifier.hpp"
#include "dmsCompress.hpp"
#include "ixm.hpp"
#include "pdTrace.hpp"
#include "dmsTrace.hpp"

using namespace bson ;

namespace engine
{

   #define DMS_MB_FLAG_FREE_STR                       "Free"
   #define DMS_MB_FLAG_USED_STR                       "Used"
   #define DMS_MB_FLAG_DROPED_STR                     "Dropped"
   #define DMS_MB_FLAG_OFFLINE_REORG_STR              "Offline Reorg"
   #define DMS_MB_FLAG_ONLINE_REORG_STR               "Online Reorg"
   #define DMS_MB_FLAG_LOAD_STR                       "Load"
   #define DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY_STR  "Shadow Copy"
   #define DMS_MB_FLAG_OFFLINE_REORG_TRUNCATE_STR     "Truncate"
   #define DMS_MB_FLAG_OFFLINE_REORG_COPY_BACK_STR    "Copy Back"
   #define DMS_MB_FLAG_OFFLINE_REORG_REBUILD_STR      "Rebuild"
   #define DMS_MB_FLAG_LOAD_LOAD_STR                  "Load"
   #define DMS_MB_FLAG_LOAD_BUILD_STR                 "Build"
   #define DMS_MB_FLAG_UNKNOWN                        "Unknown"

   #define DMS_STATUS_SEPARATOR                       " | "

   static void appendFlagString( CHAR * pBuffer, INT32 bufSize,
                                 const CHAR *flagStr )
   {
      if ( 0 != *pBuffer )
      {
         ossStrncat( pBuffer, DMS_STATUS_SEPARATOR,
                     bufSize - ossStrlen( pBuffer ) ) ;
      }
      ossStrncat( pBuffer, flagStr, bufSize - ossStrlen( pBuffer ) ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MBFLAG2STRING, "mbFlag2String" )
   void mbFlag2String( UINT16 flag, CHAR * pBuffer, INT32 bufSize )
   {
      PD_TRACE_ENTRY ( SDB__MBFLAG2STRING ) ;
      SDB_ASSERT ( pBuffer, "pBuffer can't be NULL" ) ;
      ossMemset ( pBuffer, 0, bufSize ) ;
      if ( DMS_IS_MB_FREE ( flag ) )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_FREE_STR ) ;
         goto done ;
      }

      if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_USED ) )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_USED_STR ) ;
         OSS_BIT_CLEAR( flag, DMS_MB_FLAG_USED ) ;
      }
      if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_DROPED ) )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_DROPED_STR ) ;
         OSS_BIT_CLEAR( flag, DMS_MB_FLAG_DROPED ) ;
      }

      if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_OFFLINE_REORG ) )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_OFFLINE_REORG_STR ) ;
         OSS_BIT_CLEAR( flag, DMS_MB_FLAG_OFFLINE_REORG ) ;

         if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY ) )
         {
            appendFlagString( pBuffer, bufSize,
                              DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY_STR ) ;
            OSS_BIT_CLEAR( flag, DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY ) ;
         }
         if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_OFFLINE_REORG_TRUNCATE ) )
         {
            appendFlagString( pBuffer, bufSize,
                              DMS_MB_FLAG_OFFLINE_REORG_TRUNCATE_STR ) ;
            OSS_BIT_CLEAR( flag, DMS_MB_FLAG_OFFLINE_REORG_TRUNCATE ) ;
         }
         if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_OFFLINE_REORG_COPY_BACK ) )
         {
            appendFlagString( pBuffer, bufSize,
                              DMS_MB_FLAG_OFFLINE_REORG_COPY_BACK_STR ) ;
            OSS_BIT_CLEAR( flag, DMS_MB_FLAG_OFFLINE_REORG_COPY_BACK ) ;
         }
         if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_OFFLINE_REORG_REBUILD ) )
         {
            appendFlagString( pBuffer, bufSize,
                              DMS_MB_FLAG_OFFLINE_REORG_REBUILD_STR ) ;
            OSS_BIT_CLEAR( flag, DMS_MB_FLAG_OFFLINE_REORG_REBUILD ) ;
         }
      }
      if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_ONLINE_REORG ) )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_ONLINE_REORG_STR ) ;
         OSS_BIT_CLEAR( flag, DMS_MB_FLAG_ONLINE_REORG ) ;
      }
      if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_LOAD ) )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_LOAD_LOAD_STR ) ;
         OSS_BIT_CLEAR( flag, DMS_MB_FLAG_LOAD ) ;

         if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_LOAD_LOAD ) )
         {
            appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_LOAD_LOAD_STR ) ;
            OSS_BIT_CLEAR( flag, DMS_MB_FLAG_LOAD_LOAD ) ;
         }
         if ( OSS_BIT_TEST ( flag, DMS_MB_FLAG_LOAD_BUILD ) )
         {
            appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_LOAD_BUILD_STR ) ;
            OSS_BIT_CLEAR( flag, DMS_MB_FLAG_LOAD_BUILD ) ;
         }
      }

      if ( flag )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_UNKNOWN ) ;
      }
   done :
      PD_TRACE2 ( SDB__MBFLAG2STRING,
                  PD_PACK_USHORT ( flag ),
                  PD_PACK_STRING ( pBuffer ) ) ;
      PD_TRACE_EXIT ( SDB__MBFLAG2STRING ) ;
   }

   #define DMS_MB_ATTR_COMPRESSED_STR                        "Compressed"
   PD_TRACE_DECLARE_FUNCTION ( SDB__MBATTR2STRING, "mbAttr2String" )
   void mbAttr2String( UINT32 attributes, CHAR * pBuffer, INT32 bufSize )
   {
      PD_TRACE_ENTRY ( SDB__MBATTR2STRING ) ;
      SDB_ASSERT ( pBuffer, "pBuffer can't be NULL" ) ;
      ossMemset ( pBuffer, 0, bufSize ) ;

      if ( OSS_BIT_TEST ( attributes, DMS_MB_ATTR_COMPRESSED ) )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_ATTR_COMPRESSED_STR ) ;
         OSS_BIT_CLEAR( attributes, DMS_MB_ATTR_COMPRESSED ) ;
      }

      if ( attributes )
      {
         appendFlagString( pBuffer, bufSize, DMS_MB_FLAG_UNKNOWN ) ;
      }
      PD_TRACE2 ( SDB__MBATTR2STRING,
                  PD_PACK_UINT ( attributes ),
                  PD_PACK_STRING ( pBuffer ) ) ;
      PD_TRACE_EXIT ( SDB__MBATTR2STRING ) ;
   }

   /*
      _dmsMBContext implement
   */
   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSMBCONTEXT, "_dmsMBContext::_dmsMBContext" )
   _dmsMBContext::_dmsMBContext ()
   {
      PD_TRACE_ENTRY ( SDB__DMSMBCONTEXT ) ;
      _reset () ;
      PD_TRACE_EXIT ( SDB__DMSMBCONTEXT ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSMBCONTEXT_DESC, "_dmsMBContext::~_dmsMBContext" )
   _dmsMBContext::~_dmsMBContext ()
   {
      PD_TRACE_ENTRY ( SDB__DMSMBCONTEXT_DESC ) ;
      _reset () ;
      PD_TRACE_EXIT ( SDB__DMSMBCONTEXT_DESC ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSMBCONTEXT__RESET, "_dmsMBContext::_reset" )
   void _dmsMBContext::_reset ()
   {
      PD_TRACE_ENTRY ( SDB__DMSMBCONTEXT__RESET ) ;
      _mb            = NULL ;
      _mbStat        = NULL ;
      _latch         = NULL ;
      _clLID         = DMS_INVALID_CLID ;
      _mbID          = DMS_INVALID_MBID ;
      _mbLockType    = -1 ;
      _resumeType    = -1 ;
      PD_TRACE_EXIT ( SDB__DMSMBCONTEXT__RESET ) ;
   }

   string _dmsMBContext::toString() const
   {
      stringstream ss ;
      ss << "dms-mb-context[" ;
      if ( _mb )
      {
         ss << "Name: " ;
         ss << _mb->_collectionName ;
         ss << ", " ;
      }
      ss << "ID: " << _mbID ;
      ss << ", LID: " << _clLID ;
      ss << ", LockType: " << _mbLockType ;
      ss << ", ResumeType: " << _resumeType ;

      ss << " ]" ;

      return ss.str() ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSMBCONTEXT_PAUSE, "_dmsMBContext::pause" )
   INT32 _dmsMBContext::pause()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSMBCONTEXT_PAUSE ) ;
      if ( SHARED == _mbLockType || EXCLUSIVE == _mbLockType )
      {
         _resumeType = _mbLockType ;
         rc = mbUnlock() ;
      }
      PD_TRACE_EXITRC ( SDB__DMSMBCONTEXT_PAUSE, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSMBCONTEXT_RESUME, "_dmsMBContext::resume" )
   INT32 _dmsMBContext::resume()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSMBCONTEXT_RESUME ) ;
      if ( SHARED == _resumeType || EXCLUSIVE == _resumeType )
      {
         INT32 lockType = _resumeType ;
         _resumeType = -1 ;
         rc = mbLock( lockType ) ;
      }
      PD_TRACE_EXITRC ( SDB__DMSMBCONTEXT_RESUME, rc ) ;
      return rc ;
   }

   /*
      _dmsStorageData implement
   */
   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA, "_dmsStorageData::_dmsStorageData" )
   _dmsStorageData::_dmsStorageData ( const CHAR *pSuFileName,
                                      dmsStorageInfo *pInfo )
   :_dmsStorageBase( pSuFileName, pInfo )
   {
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA ) ;
      _pIdxSU           = NULL ;
      _pLobSU           = NULL ;
      _logicalCSID      = 0 ;
      _CSID             = DMS_INVALID_SUID ;
      PD_TRACE_EXIT ( SDB__DMSSTORAGEDATA ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_DESC, "_dmsStorageData::~_dmsStorageData" )
   _dmsStorageData::~_dmsStorageData ()
   {
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_DESC ) ;
      _collectionNameMapCleanup() ;

      vector<dmsMBContext*>::iterator it = _vecContext.begin() ;
      while ( it != _vecContext.end() )
      {
         SDB_OSS_DEL (*it) ;
         ++it ;
      }
      _vecContext.clear() ;

      _pIdxSU = NULL ;
      _pLobSU = NULL ;
      PD_TRACE_EXIT ( SDB__DMSSTORAGEDATA_DESC ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_SYNCMEMTOMMAP, "_dmsStorageData::syncMemToMmap" )
   void _dmsStorageData::syncMemToMmap ()
   {
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_SYNCMEMTOMMAP ) ;
      for ( UINT32 i = 0 ; i < DMS_MME_SLOTS ; ++i )
      {
         if ( DMS_IS_MB_INUSE ( _dmsMME->_mbList[i]._flag ) )
         {
            _dmsMME->_mbList[i]._totalRecords = _mbStatInfo[i]._totalRecords ;
            _dmsMME->_mbList[i]._totalDataPages =
               _mbStatInfo[i]._totalDataPages ;
            _dmsMME->_mbList[i]._totalIndexPages =
               _mbStatInfo[i]._totalIndexPages ;
            _dmsMME->_mbList[i]._totalDataFreeSpace =
               _mbStatInfo[i]._totalDataFreeSpace ;
            _dmsMME->_mbList[i]._totalIndexFreeSpace =
               _mbStatInfo[i]._totalIndexFreeSpace ;
            _dmsMME->_mbList[i]._totalLobPages =
               _mbStatInfo[i]._totalLobPages ;
         }
      }
      PD_TRACE_EXIT ( SDB__DMSSTORAGEDATA_SYNCMEMTOMMAP ) ;
   }

   void _dmsStorageData::_attach( _dmsStorageIndex * pIndexSu )
   {
      SDB_ASSERT( pIndexSu, "Index su can't be NULL" ) ;
      _pIdxSU = pIndexSu ;
   }

   void _dmsStorageData::_detach ()
   {
      _pIdxSU = NULL ;
   }

   void _dmsStorageData::_attachLob( _dmsStorageLob * pLobSu )
   {
      SDB_ASSERT( pLobSu, "Lob su can't be NULL" ) ;
      _pLobSU = pLobSu ;
   }

   void _dmsStorageData::_detachLob()
   {
      _pLobSU = NULL ;
   }

   UINT64 _dmsStorageData::_dataOffset ()
   {
      return ( DMS_MME_OFFSET + DMS_MME_SZ ) ;
   }

   const CHAR* _dmsStorageData::_getEyeCatcher () const
   {
      return DMS_DATASU_EYECATCHER ;
   }

   UINT32 _dmsStorageData::_curVersion () const
   {
      return DMS_DATASU_CUR_VERSION ;
   }

   INT32 _dmsStorageData::_checkVersion( dmsStorageUnitHeader * pHeader )
   {
      INT32 rc = SDB_OK ;

      if ( pHeader->_version > _curVersion() )
      {
         PD_LOG( PDERROR, "Incompatible version: %u", pHeader->_version ) ;
         rc = SDB_DMS_INCOMPATIBLE_VERSION ;
      }
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__ONCREATE, "_dmsStorageData::_onCreate" )
   INT32 _dmsStorageData::_onCreate( OSSFILE * file, UINT64 curOffSet )
   {
      INT32 rc          = SDB_OK ;
      _dmsMME           = NULL ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__ONCREATE ) ;
      SDB_ASSERT( DMS_MME_OFFSET == curOffSet, "Offset is not MME offset" ) ;

      _dmsMME = SDB_OSS_NEW dmsMetadataManagementExtent ; 
      if ( !_dmsMME )
      {
         PD_LOG ( PDSEVERE, "Failed to allocate memory to for dmsMME" ) ;
         PD_LOG ( PDSEVERE, "Requested memory: %d bytes", DMS_MME_SZ ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      _initializeMME () ;

      rc = _writeFile ( file, (CHAR *)_dmsMME, DMS_MME_SZ ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to write to file duirng SU init, rc: %d",
                  rc ) ;
         goto error ;
      }
      SDB_OSS_DEL _dmsMME ;
      _dmsMME = NULL ;

   done:
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__ONCREATE, rc ) ;
      return rc ;
   error:
      if ( _dmsMME )
      {
         SDB_OSS_DEL _dmsMME ;
         _dmsMME = NULL ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__ONMAPMETA, "_dmsStorageData::_onMapMeta" )
   INT32 _dmsStorageData::_onMapMeta( UINT64 curOffSet )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__ONMAPMETA ) ;
      rc = map ( DMS_MME_OFFSET, DMS_MME_SZ, (void**)&_dmsMME ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to map MME: %s", getSuFileName() ) ;
         goto error ;
      }

      for ( UINT16 i = 0 ; i < DMS_MME_SLOTS ; i++ )
      {
         if ( DMS_IS_MB_INUSE ( _dmsMME->_mbList[i]._flag ) )
         {
            _collectionNameInsert ( _dmsMME->_mbList[i]._collectionName, i ) ;

            _mbStatInfo[i]._totalRecords = _dmsMME->_mbList[i]._totalRecords ;
            _mbStatInfo[i]._totalDataPages =
               _dmsMME->_mbList[i]._totalDataPages ;
            _mbStatInfo[i]._totalIndexPages =
               _dmsMME->_mbList[i]._totalIndexPages ;
            _mbStatInfo[i]._totalDataFreeSpace =
               _dmsMME->_mbList[i]._totalDataFreeSpace ;
            _mbStatInfo[i]._totalIndexFreeSpace =
               _dmsMME->_mbList[i]._totalIndexFreeSpace ;
            _mbStatInfo[i]._totalLobPages =
               _dmsMME->_mbList[i]._totalLobPages ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__ONMAPMETA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__ONCLOSED, "_dmsStorageData::_onClosed" )
   void _dmsStorageData::_onClosed ()
   {
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__ONCLOSED ) ;
      syncMemToMmap () ;

      _dmsMME     = NULL ;
      PD_TRACE_EXIT ( SDB__DMSSTORAGEDATA__ONCLOSED ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__INITMME, "_dmsStorageData::_initializeMME" )
   void _dmsStorageData::_initializeMME ()
   {
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__INITMME ) ;
      SDB_ASSERT ( _dmsMME, "MME is NULL" ) ;

      for ( INT32 i = 0; i < DMS_MME_SLOTS ; i++ )
      {
         _dmsMME->_mbList[i].reset () ;
      }
      PD_TRACE_EXIT ( SDB__DMSSTORAGEDATA__INITMME ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__LOGDPS, "_dmsStorageData::_logDPS" )
   INT32 _dmsStorageData::_logDPS( SDB_DPSCB * dpsCB,
                                   dpsMergeInfo & info,
                                   pmdEDUCB * cb,
                                   ossSLatch * pLatch,
                                   OSS_LATCH_MODE mode,
                                   BOOLEAN & locked,
                                   UINT32 clLID,
                                   dmsExtentID extLID )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__LOGDPS ) ;
      info.setInfoEx( _logicalCSID, clLID, extLID, cb ) ;
      rc = dpsCB->prepare( info ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( pLatch && locked )
      {
         if ( SHARED == mode )
         {
            pLatch->release_shared() ;
         }
         else
         {
            pLatch->release() ;
         }
         locked = FALSE ;
      }
      dpsCB->writeData( info ) ;
   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__LOGDPS, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__LOGDPS1, "_dmsStorageData::_logDPS" )
   INT32 _dmsStorageData::_logDPS( SDB_DPSCB *dpsCB, dpsMergeInfo &info,
                                   pmdEDUCB *cb, dmsMBContext *context,
                                   dmsExtentID extLID,
                                   BOOLEAN needUnLock )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__LOGDPS1 ) ;
      info.setInfoEx( logicalID(), context->clLID(), extLID, cb ) ;
      rc = dpsCB->prepare( info ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( needUnLock )
      {
         context->mbUnlock() ;
      }

      dpsCB->writeData( info ) ;
   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__LOGDPS1, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__ALLOCATEEXTENT, "_dmsStorageData::_allocateExtent" )
   INT32 _dmsStorageData::_allocateExtent( dmsMBContext * context,
                                           UINT16 numPages,
                                           BOOLEAN map2DelList,
                                           BOOLEAN add2LoadList,
                                           dmsExtentID *allocExtID )
   {
      SDB_ASSERT( context, "dms mb context can't be NULL" ) ;
      INT32 rc                 = SDB_OK ;
      SINT32 firstFreeExtentID = DMS_INVALID_EXTENT ;
      dmsExtent *extAddr       = NULL ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__ALLOCATEEXTENT ) ;
      PD_TRACE3 ( SDB__DMSSTORAGEDATA__ALLOCATEEXTENT,
                  PD_PACK_USHORT ( numPages ),
                  PD_PACK_UINT ( map2DelList ),
                  PD_PACK_UINT ( add2LoadList ) ) ;
      if ( numPages > segmentPages() || numPages < 1 )
      {
         PD_LOG ( PDERROR, "Invalid number of pages: %d", numPages ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = context->mbLock( EXCLUSIVE ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb lock failed, rc: %d", rc ) ;

      rc = _findFreeSpace ( numPages, firstFreeExtentID, context ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Error find free space for %d pages, rc = %d",
                  numPages, rc ) ;
         goto error ;
      }

      extAddr = (dmsExtent*)extentAddr( firstFreeExtentID ) ;
      extAddr->init( numPages, context->mbID(),
                     (UINT32)numPages << pageSizeSquareRoot() ) ;

      if ( TRUE == add2LoadList )
      {
         extAddr->_prevExtent = context->mb()->_loadLastExtentID ;
         extAddr->_nextExtent = DMS_INVALID_EXTENT ;
         if ( DMS_INVALID_EXTENT == context->mb()->_loadFirstExtentID )
         {
            context->mb()->_loadFirstExtentID = firstFreeExtentID ;
         }

         if ( DMS_INVALID_EXTENT != extAddr->_prevExtent )
         {
            dmsExtent *prevExt = (dmsExtent*)extentAddr(extAddr->_prevExtent) ;
            prevExt->_nextExtent = firstFreeExtentID ;
         }

         context->mb()->_loadLastExtentID = firstFreeExtentID ;
      }
      else
      {
         rc = addExtent2Meta( firstFreeExtentID, extAddr, context ) ;
         PD_RC_CHECK( rc, PDERROR, "Add extent to meta failed, rc: %d", rc ) ;

         /*
         extAddr->_prevExtent = context->mb()->_lastExtentID ;

         if ( DMS_INVALID_EXTENT == context->mb()->_firstExtentID )
         {
            context->mb()->_firstExtentID = firstFreeExtentID ;
         }

         if ( DMS_INVALID_EXTENT != extAddr->_prevExtent )
         {
            dmsExtent *prevExt = (dmsExtent*)extentAddr(extAddr->_prevExtent) ;
            prevExt->_nextExtent = firstFreeExtentID ;
            extAddr->_logicID = prevExt->_logicID + 1 ;
         }
         else
         {
            extAddr->_logicID = DMS_INVALID_EXTENT + 1 ;
         }

         context->mb()->_lastExtentID = firstFreeExtentID ;
         */
      }

      if ( map2DelList )
      {
         _mapExtent2DelList( context->mb(), extAddr, firstFreeExtentID ) ;
      }

      if ( allocExtID )
      {
         *allocExtID = firstFreeExtentID ;
         PD_TRACE1 ( SDB__DMSSTORAGEDATA__ALLOCATEEXTENT,
                     PD_PACK_INT ( firstFreeExtentID ) ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__ALLOCATEEXTENT, rc ) ;
      return rc ;
   error :
      if ( DMS_INVALID_EXTENT != firstFreeExtentID )
      {
         _freeExtent( firstFreeExtentID ) ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__FREEEXTENT, "_dmsStorageData::_freeExtent" )
   INT32 _dmsStorageData::_freeExtent( dmsExtentID extentID )
   {
      INT32 rc = SDB_OK ;
      dmsExtent *extAddr = NULL ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__FREEEXTENT ) ;
      if ( DMS_INVALID_EXTENT == extentID )
      {
         PD_LOG( PDERROR, "Invalid extent id for free" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      PD_TRACE1 ( SDB__DMSSTORAGEDATA__FREEEXTENT,
                  PD_PACK_INT ( extentID ) ) ;
      extAddr = (dmsExtent*)extentAddr( extentID ) ;

      if ( !extAddr->validate() )
      {
         PD_LOG ( PDERROR, "Invalid eye catcher or flag for extent %d",
                  extentID ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      extAddr->_flag = DMS_EXTENT_FLAG_FREED ;
      extAddr->_logicID = DMS_INVALID_EXTENT ;

      rc = _releaseSpace( extentID, extAddr->_blockSize ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to release page, rc = %d", rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__FREEEXTENT, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__RESERVEFROMDELETELIST, "_dmsStorageData::_reserveFromDeleteList" )
   INT32 _dmsStorageData::_reserveFromDeleteList( dmsMBContext *context,
                                                  UINT32 requiredSize,
                                                  dmsRecordID &resultID,
                                                  pmdEDUCB * cb )
   {
      INT32 rc                      = SDB_OK ;
      UINT32 dmsRecordSizeTemp      = 0 ;
      UINT8  deleteRecordSlot       = 0 ;
      const static INT32 s_maxSearch = 3 ;

      INT32  j                      = 0 ;
      INT32  i                      = 0 ;
      dmsRecordID prevDeletedID ;
      dmsRecordID foundDeletedID  ;
      ossValuePtr prevExtentPtr     = 0 ;
      ossValuePtr extentPtr         = 0 ;
      ossValuePtr delRecordPtr      = 0 ;
      dpsTransCB *pTransCB          = pmdGetKRCB()->getTransCB() ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__RESERVEFROMDELETELIST ) ;
      PD_TRACE1 ( SDB__DMSSTORAGEDATA__RESERVEFROMDELETELIST,
                  PD_PACK_UINT ( requiredSize ) ) ;
      rc = context->mbLock( EXCLUSIVE ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

   retry:
      dmsRecordSizeTemp = ( requiredSize-1 ) >> 5 ;
      deleteRecordSlot  = 0 ;
      while ( dmsRecordSizeTemp != 0 )
      {
         deleteRecordSlot ++ ;
         dmsRecordSizeTemp = dmsRecordSizeTemp >> 1 ;
      }
      SDB_ASSERT( deleteRecordSlot < dmsMB::_max, "Invalid record size" ) ;

      if ( deleteRecordSlot >= dmsMB::_max )
      {
         PD_LOG( PDERROR, "Invalid record size: %u", requiredSize ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = SDB_DMS_NOSPC ;
      for ( j = deleteRecordSlot ; j < dmsMB::_max ; ++j )
      {
         prevDeletedID.reset() ;
         prevExtentPtr =  0 ;
         foundDeletedID = _dmsMME->_mbList[context->mbID()]._deleteList[j] ;
         for ( i = 0 ; i < s_maxSearch ; ++i )
         {
            if ( foundDeletedID.isNull() )
            {
               break ;
            }
            extentPtr = extentAddr( foundDeletedID._extent ) ;
            if ( 0 == extentPtr )
            {
               PD_LOG ( PDERROR, "Deleted record is incorrect: %d.%d",
                        foundDeletedID._extent, foundDeletedID._offset ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            delRecordPtr = extentPtr + foundDeletedID._offset ;
            if( DMS_RECORD_FLAG_DELETED ==
                DMS_DELETEDRECORD_GETFLAG( delRecordPtr ) &&
                DMS_DELETEDRECORD_GETSIZE( delRecordPtr ) >= requiredSize )
            {
               if ( SDB_OK == pTransCB->transLockTestX( cb, _logicalCSID,
                                                        context->mbID(),
                                                        &foundDeletedID ) )
               {
                  if ( 0 == prevExtentPtr )
                  {
                     context->mb()->_deleteList[j] =
                              DMS_DELETEDRECORD_GETNEXTRID( delRecordPtr ) ;
                  }
                  else
                  {
                     DMS_DELETEDRECORD_SETNEXTRID ( 
                              prevExtentPtr+prevDeletedID._offset,
                              DMS_DELETEDRECORD_GETNEXTRID ( delRecordPtr ) ) ;
                  }
                  resultID = foundDeletedID ;
                  ((dmsExtent*)extentPtr)->_freeSpace -=
                        DMS_DELETEDRECORD_GETSIZE( delRecordPtr ) ;
                  context->mbStat()->_totalDataFreeSpace -=
                        DMS_DELETEDRECORD_GETSIZE( delRecordPtr ) ;
                  rc = SDB_OK ;
                  goto done ;
               }
               else
               {
                  --i ;
               }
            }

            prevDeletedID  = foundDeletedID ;
            prevExtentPtr  = extentPtr ;
            foundDeletedID = DMS_DELETEDRECORD_GETNEXTRID( delRecordPtr ) ;
         }
      }

      {
         UINT32 reqPages = ( ( ( requiredSize + DMS_EXTENT_METADATA_SZ ) <<
                             DMS_RECORDS_PER_EXTENT_SQUARE ) + pageSize() -
                             1 ) >> pageSizeSquareRoot() ;
         if ( reqPages > segmentPages() )
         {
            reqPages = segmentPages() ;
         }
         if ( reqPages < 1 )
         {
            reqPages = 1 ;
         }

         rc = _allocateExtent( context, reqPages, TRUE, FALSE, NULL ) ;
         PD_RC_CHECK( rc, PDERROR, "Unable to allocate %d pages extent to the "
                      "collection, rc: %d", reqPages, rc ) ;
         goto retry ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__RESERVEFROMDELETELIST, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__TRUNCATECOLLECTION, "_dmsStorageData::_truncateCollection" )
   INT32 _dmsStorageData::_truncateCollection( dmsMBContext *context )
   {
      INT32 rc                     = SDB_OK ;
      dmsExtentID lastExt          = DMS_INVALID_EXTENT ;
      dmsExtentID prevExt          = DMS_INVALID_EXTENT ;
      dmsMetaExtent *metaExt       = NULL ;

      SDB_ASSERT( context, "dms mb context can't be NULL" ) ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__TRUNCATECOLLECTION ) ;
      rc = context->mbLock( EXCLUSIVE ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

      lastExt = context->mb()->_lastExtentID ;
      for ( UINT32 i = 0 ; i < dmsMB::_max ; i++ )
      {
         context->mb()->_deleteList[i].reset() ;
      }
      while ( DMS_INVALID_EXTENT != lastExt )
      {
         prevExt = ((dmsExtent*)extentAddr(lastExt))->_prevExtent ;
         rc = _freeExtent ( lastExt ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to free extent[%u], rc: %d", lastExt,
                     rc ) ;
            SDB_ASSERT( SDB_OK == rc, "Free extent can't be failure" ) ;
         }

         lastExt = prevExt ;
         context->mb()->_lastExtentID = lastExt ;
      }
      context->mb()->_firstExtentID = DMS_INVALID_EXTENT ;

      lastExt = context->mb()->_loadLastExtentID ;
      while ( DMS_INVALID_EXTENT != lastExt )
      {
         prevExt = ((dmsExtent*)extentAddr(lastExt))->_prevExtent ;
         rc = _freeExtent( lastExt ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to free load extent[%u], rc: %d",
                    lastExt, rc ) ;
            SDB_ASSERT( SDB_OK == rc, "Free extent can't be failure" ) ;
         }
         lastExt = prevExt ;
         context->mb()->_loadLastExtentID = lastExt ;
      }
      context->mb()->_loadFirstExtentID = DMS_INVALID_EXTENT ;
      metaExt = ( dmsMetaExtent* )extentAddr( context->mb()->_mbExExtentID ) ;
      if ( metaExt )
      {
         metaExt->reset() ;
      }
      context->mbStat()->_totalDataFreeSpace = 0 ;
      context->mbStat()->_totalDataPages = 0 ;
      context->mbStat()->_totalRecords = 0 ;

   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__TRUNCATECOLLECTION, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__TRUNCATECOLLECITONLOADS, "_dmsStorageData::_truncateCollectionLoads" )
   INT32 _dmsStorageData::_truncateCollectionLoads( dmsMBContext * context )
   {
      INT32 rc                     = SDB_OK ;
      dmsExtentID lastExt          = DMS_INVALID_EXTENT ;
      dmsExtentID prevExt          = DMS_INVALID_EXTENT ;

      SDB_ASSERT( context, "dms mb context can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__TRUNCATECOLLECITONLOADS ) ;
      rc = context->mbLock( EXCLUSIVE ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

      lastExt = context->mb()->_loadLastExtentID ;
      while ( DMS_INVALID_EXTENT != lastExt )
      {
         rc = context->mbLock( EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

         prevExt = ((dmsExtent*)extentAddr(lastExt))->_prevExtent ;
         rc = _freeExtent( lastExt ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to free load extent[%u], rc: %d",
                    lastExt, rc ) ;
            SDB_ASSERT( SDB_OK == rc, "Free extent can't be failure" ) ;
         }
         lastExt = prevExt ;
         context->mb()->_loadLastExtentID = lastExt ;

         if ( DMS_INVALID_EXTENT != lastExt )
         {
            context->mbUnlock() ;
         }
      }
      context->mb()->_loadFirstExtentID = DMS_INVALID_EXTENT ;

   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__TRUNCATECOLLECITONLOADS, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD, "_dmsStorageData::_saveDeletedRecord" )
   INT32 _dmsStorageData::_saveDeletedRecord( dmsMB *mb, dmsExtent * extAddr,
                                              dmsOffset offset,
                                              INT32 recordSize,
                                              INT32 extentID )
   {
      UINT8 deleteRecordSlot = 0 ;
      ossValuePtr recordPtr = ( (ossValuePtr)extAddr + offset ) ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD ) ;
      PD_TRACE6 ( SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD,
                  PD_PACK_STRING ( "offset" ),
                  PD_PACK_INT ( offset ),
                  PD_PACK_STRING ( "recordSize" ),
                  PD_PACK_INT ( recordSize ),
                  PD_PACK_STRING ( "extentID" ),
                  PD_PACK_INT ( extentID ) ) ;
      DMS_DELETEDRECORD_SETFLAG( recordPtr, DMS_RECORD_FLAG_DELETED ) ;
      DMS_DELETEDRECORD_SETSIZE( recordPtr, recordSize ) ;
      DMS_DELETEDRECORD_SETMYOFFSET( recordPtr, offset ) ;

      extAddr->_freeSpace += recordSize ;
      _mbStatInfo[mb->_blockID]._totalDataFreeSpace += recordSize ;

      recordSize = ( recordSize - 1 ) >> 5 ;

      while ( (recordSize) != 0 )
      {
         deleteRecordSlot ++ ;
         recordSize = ( recordSize >> 1 ) ;
      }

      SDB_ASSERT ( deleteRecordSlot < dmsMB::_max, "Invalid record size" ) ;

      DMS_DELETEDRECORD_SETNEXTRID ( recordPtr,
                                     mb->_deleteList [ deleteRecordSlot ] ) ;
      mb->_deleteList[ deleteRecordSlot ]._extent = extentID ;
      mb->_deleteList[ deleteRecordSlot ]._offset = offset ;
      PD_TRACE_EXIT ( SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD ) ;
      return SDB_OK ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD1, "_dmsStorageData::_saveDeletedRecord" )
   INT32 _dmsStorageData::_saveDeletedRecord( dmsMB * mb,
                                              const dmsRecordID &recordID,
                                              INT32 recordSize )
   {
      INT32 rc = SDB_OK ;
      ossValuePtr extentPtr = 0 ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD1 ) ;
      PD_TRACE2 ( SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD1,
                  PD_PACK_INT ( recordID._extent ),
                  PD_PACK_INT ( recordID._offset ) ) ;
      if ( recordID.isNull() )
      {
         rc = SDB_INVALIDARG ;
         goto done ;
      }
      extentPtr = extentAddr( recordID._extent ) ;

      if ( 0 == recordSize )
      {
         ossValuePtr recordPtr = extentPtr + recordID._offset ;
         recordSize = DMS_RECORD_GETSIZE(recordPtr) ;
      }

      rc = _saveDeletedRecord ( mb, (dmsExtent*)extentPtr, recordID._offset,
                                recordSize, recordID._extent ) ;
   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD1, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__MAPEXTENT2DELLIST, "_dmsStorageData::_mapExtent2DelList" )
   void _dmsStorageData::_mapExtent2DelList( dmsMB * mb, dmsExtent * extAddr,
                                             SINT32 extentID )
   {
      INT32 extentSize         = 0 ;
      INT32 extentUseableSpace = 0 ;
      INT32 deleteRecordSize   = 0 ;
      dmsOffset recordOffset   = 0 ;
      INT32 curUseableSpace    = 0 ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__MAPEXTENT2DELLIST ) ;
      if ( (UINT32)extAddr->_freeSpace < DMS_MIN_RECORD_SZ )
      {
         if ( extAddr->_freeSpace != 0 )
         {
            PD_LOG( PDINFO, "Collection[%s, mbID: %d]'s extent[%d] free "
                    "space[%d] is less than min record size[%d]",
                    mb->_collectionName, mb->_blockID, extentID,
                    extAddr->_freeSpace, DMS_MIN_RECORD_SZ ) ;
         }
         goto done ;
      }

      extentSize          = extAddr->_blockSize << pageSizeSquareRoot() ;
      extentUseableSpace  = extAddr->_freeSpace ;
      extAddr->_freeSpace = 0 ;

      deleteRecordSize    = OSS_MIN ( extentUseableSpace,
                                           DMS_RECORD_MAX_SZ ) ;
      recordOffset        = extentSize - extentUseableSpace ;
      curUseableSpace     = extentUseableSpace ;

      while ( curUseableSpace - deleteRecordSize >=
              (INT32)DMS_MIN_DELETEDRECORD_SZ )
      {
         _saveDeletedRecord( mb, extAddr, recordOffset, deleteRecordSize,
                             extentID ) ;
         curUseableSpace -= deleteRecordSize ;
         recordOffset += deleteRecordSize ;
      }
      if ( curUseableSpace > deleteRecordSize )
      {
         _saveDeletedRecord( mb, extAddr, recordOffset, DMS_PAGE_SIZE4K,
                             extentID ) ;
         curUseableSpace -= DMS_PAGE_SIZE4K ;
         recordOffset += DMS_PAGE_SIZE4K ;
      }

      if ( curUseableSpace > 0 )
      {
         _saveDeletedRecord( mb, extAddr, recordOffset, curUseableSpace,
                             extentID ) ;
      }

      SDB_ASSERT( extentUseableSpace == extAddr->_freeSpace,
                  "Extent[%d] free space invalid" ) ;
   done :
      PD_TRACE_EXIT ( SDB__DMSSTORAGEDATA__MAPEXTENT2DELLIST ) ;
      return ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_ADDEXTENT2META, "_dmsStorageData::addExtent2Meta" )
   INT32 _dmsStorageData::addExtent2Meta( dmsExtentID extID,
                                          dmsExtent *extent,
                                          dmsMBContext *context )
   {
      INT32 rc = SDB_OK ;
      dmsMBEx *mbEx = NULL ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_ADDEXTENT2META ) ;
      UINT32 segID = extent2Segment( extID ) - dataStartSegID() ;
      dmsExtentID lastExtID = DMS_INVALID_EXTENT ;
      dmsExtent *prevExt = NULL ;
      dmsExtent *nextExt = NULL ;

      if ( !context->isMBLock( EXCLUSIVE ) )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Caller must hold mb exclusive lock[%s]",
                 context->toString().c_str() ) ;
         goto error ;
      }

      if ( isTempSU() )
      {
         extent->_prevExtent = context->mb()->_lastExtentID ;

         if ( DMS_INVALID_EXTENT == context->mb()->_firstExtentID )
         {
            context->mb()->_firstExtentID = extID ;
         }

         if ( DMS_INVALID_EXTENT != extent->_prevExtent )
         {
            dmsExtent *prevExt = (dmsExtent*)extentAddr(extent->_prevExtent) ;
            prevExt->_nextExtent = extID ;
            extent->_logicID = prevExt->_logicID + 1 ;
         }
         else
         {
            extent->_logicID = DMS_INVALID_EXTENT + 1 ;
         }

         context->mb()->_lastExtentID = extID ;
      }
      else
      {
         mbEx = ( dmsMBEx* )extentAddr( context->mb()->_mbExExtentID ) ;
         if ( NULL == mbEx )
         {
            rc = SDB_SYS ;
            PD_LOG( PDERROR, "dms mb expand extent is invalid: %d",
                    context->mb()->_mbExExtentID ) ;
            goto error ;
         }

         if ( segID >= mbEx->_header._segNum )
         {
            rc = SDB_SYS ;
            PD_LOG( PDERROR, "Invalid segID: %d, max segNum: %d", segID,
                    mbEx->_header._segNum ) ;
            goto error ;
         }

         mbEx->getLastExtentID( segID, lastExtID ) ;

         if ( DMS_INVALID_EXTENT == lastExtID )
         {
            extent->_logicID = ( segID << _getFactor() ) ;
            mbEx->setFirstExtentID( segID, extID ) ;
            mbEx->setLastExtentID( segID, extID ) ;
            ++(mbEx->_header._usedSegNum) ;

            INT32 tmpSegID = segID ;
            dmsExtentID tmpExtID = DMS_INVALID_EXTENT ;
            while ( DMS_INVALID_EXTENT != context->mb()->_firstExtentID &&
                    --tmpSegID >= 0 )
            {
               mbEx->getLastExtentID( tmpSegID, tmpExtID ) ;
               if ( DMS_INVALID_EXTENT != tmpExtID )
               {
                  extent->_prevExtent = tmpExtID ;
                  prevExt = ( dmsExtent* )extentAddr( tmpExtID ) ;
                  break ;
               }
            }
         }
         else
         {
            mbEx->setLastExtentID( segID, extID ) ;
            extent->_prevExtent = lastExtID ;
            prevExt = ( dmsExtent* )extentAddr( lastExtID ) ;
            extent->_logicID = prevExt->_logicID + 1 ;
         }

         if ( prevExt )
         {
            if ( DMS_INVALID_EXTENT != prevExt->_nextExtent )
            {
               extent->_nextExtent = prevExt->_nextExtent ;
               nextExt = ( dmsExtent* )extentAddr( extent->_nextExtent ) ;
               nextExt->_prevExtent = extID ;
            }
            else
            {
               context->mb()->_lastExtentID = extID ;
            }
            prevExt->_nextExtent = extID ;
         }
         else
         {
            if ( DMS_INVALID_EXTENT != context->mb()->_firstExtentID )
            {
               extent->_nextExtent = context->mb()->_firstExtentID ;
               nextExt = ( dmsExtent* )extentAddr( extent->_nextExtent ) ;
               nextExt->_prevExtent = extID ;
            }
            context->mb()->_firstExtentID = extID ;

            if ( DMS_INVALID_EXTENT == context->mb()->_lastExtentID )
            {
               context->mb()->_lastExtentID = extID ;
            }
         }
      }

      context->mbStat()->_totalDataPages += extent->_blockSize ;

   done:
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_ADDEXTENT2META, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_ADDCOLLECTION, "_dmsStorageData::addCollection" )
   INT32 _dmsStorageData::addCollection( const CHAR * pName,
                                         UINT16 * collectionID,
                                         UINT32 attributes,
                                         pmdEDUCB * cb,
                                         SDB_DPSCB * dpscb,
                                         UINT16 initPages,
                                         BOOLEAN sysCollection,
                                         BOOLEAN noIDIndex )
   {
      INT32 rc                = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_ADDCOLLECTION ) ;
      static BSONObj s_idKeyObj = BSON( IXM_FIELD_NAME_KEY <<
                                        BSON( DMS_ID_KEY_NAME << 1 ) <<
                                        IXM_FIELD_NAME_NAME << IXM_ID_KEY_NAME
                                        << IXM_FIELD_NAME_UNIQUE <<
                                        true << IXM_FIELD_NAME_V << 0 <<
                                        IXM_FIELD_NAME_ENFORCED << true ) ;
      dpsMergeInfo info ;
      dpsLogRecord &record    = info.getMergeBlock().record() ;
      UINT32 logRecSize       = 0;
      dpsTransCB *pTransCB    = pmdGetKRCB()->getTransCB() ;
      CHAR fullName[DMS_COLLECTION_FULL_NAME_SZ + 1] = {0} ;
      UINT16 newCollectionID  = DMS_INVALID_MBID ;
      UINT32 logicalID        = DMS_INVALID_CLID ;
      BOOLEAN metalocked      = FALSE ;
      dmsMB *mb               = NULL ;
      SDB_DPSCB *dropDps      = NULL ;
      dmsMBContext *context   = NULL ;

      UINT32 segNum           = DMS_MAX_PG >> segmentPagesSquareRoot() ;
      UINT32 mbExSize         = (( segNum << 3 ) >> pageSizeSquareRoot()) + 1 ;
      dmsExtentID mbExExtent  = DMS_INVALID_EXTENT ;
      dmsMetaExtent *mbExtent = NULL ;

      SDB_ASSERT( pName, "Collection name cat't be NULL" ) ;

      rc = dmsCheckCLName ( pName, sysCollection ) ;
      PD_RC_CHECK( rc, PDERROR, "Invalid collection name %s, rc: %d",
                   pName, rc ) ;

      PD_CHECK( initPages <= segmentPages(), SDB_INVALIDARG, error, PDERROR,
                "Invalid pages: %u", initPages ) ;

      if ( dpscb )
      {
         rc = dpsCLCrt2Record( _clFullName(pName, fullName, sizeof(fullName)),
                               attributes, record ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build record, rc: %d", rc ) ;

         rc = dpscb->checkSyncControl( record.alignedLen(), cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d", rc ) ;

         logRecSize = record.alignedLen() ;
         rc = pTransCB->reservedLogSpace( logRecSize ) ;
         if( rc )
         {
            PD_LOG( PDERROR, "Failed to reserved log space(length=%u)",
                    logRecSize ) ;
            logRecSize = 0 ;
            goto error ;
         }
      }

      if ( !isTempSU () )
      {
         rc = _findFreeSpace( mbExSize, mbExExtent, NULL ) ;
         PD_RC_CHECK( rc, PDERROR, "Allocate metablock expand extent failed, "
                      "pageNum: %d, rc: %d", mbExSize, rc ) ;
      }

      ossLatch( &_metadataLatch, EXCLUSIVE ) ;
      metalocked = TRUE ;

      if ( DMS_INVALID_MBID != _collectionNameLookup ( pName ) )
      {
         rc = SDB_DMS_EXIST ;
         goto error ;
      }

      if ( DMS_MME_SLOTS <= _dmsHeader->_numMB )
      {
         PD_LOG ( PDERROR, "There is no free slot for extra collection" ) ;
         rc = SDB_DMS_NOSPC ;
         goto error ;
      }

      for ( UINT32 i = 0 ; i < DMS_MME_SLOTS ; ++i )
      {
         if ( DMS_IS_MB_FREE ( _dmsMME->_mbList[i]._flag ) ||
              DMS_IS_MB_DROPPED ( _dmsMME->_mbList[i]._flag ) )
         {
            newCollectionID = i ;
            break ;
         }
      }
      if ( DMS_INVALID_MBID == newCollectionID )
      {
         PD_LOG ( PDERROR, "Unable to find free collection id" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      logicalID = _dmsHeader->_MBHWM++ ;
      mb = &_dmsMME->_mbList[newCollectionID] ;
      mb->reset( pName, newCollectionID, logicalID, attributes ) ;
      _mbStatInfo[ newCollectionID ].reset() ;
      _dmsHeader->_numMB++ ;
      _collectionNameInsert( pName, newCollectionID ) ;

      if ( !isTempSU () )
      {
         mbExtent = ( dmsMetaExtent* )extentAddr( mbExExtent ) ;
         PD_CHECK( mbExtent, SDB_SYS, error, PDERROR, "Invalid meta extent[%d]",
                   mbExExtent ) ;
         mbExtent->init( mbExSize, newCollectionID, segNum ) ;
         mb->_mbExExtentID = mbExExtent ;
         mbExExtent = DMS_INVALID_EXTENT ;
      }

      if ( dpscb )
      {
         rc = _logDPS( dpscb, info, cb, &_metadataLatch, EXCLUSIVE,
                       metalocked, logicalID, DMS_INVALID_EXTENT ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert CLcrt record to log, "
                      "rc = %d", rc ) ;
      }

      if ( metalocked )
      {
         ossUnlatch( &_metadataLatch, EXCLUSIVE ) ;
         metalocked = FALSE ;
      }

      if ( collectionID )
      {
         *collectionID = newCollectionID ;
      }
      dropDps = dpscb ;

      rc = getMBContext( &context, newCollectionID, logicalID, EXCLUSIVE ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get mb[%u] context, rc: %d",
                 newCollectionID, rc ) ;
         if ( SDB_DMS_NOTEXIST == rc )
         {
            newCollectionID = DMS_INVALID_MBID ;
         }
         goto error ;
      }

      if ( 0 != initPages )
      {
         rc = _allocateExtent( context, initPages, TRUE, FALSE, NULL ) ;
         PD_RC_CHECK( rc, PDERROR, "Allocate new %u pages of collection[%s] "
                      "failed, rc: %d", pName, rc ) ;
      }

      if ( !noIDIndex &&
           !OSS_BIT_TEST( attributes, DMS_MB_ATTR_NOIDINDEX ) )
      {
         rc = _pIdxSU->createIndex( context, s_idKeyObj, cb, NULL, TRUE ) ;
         PD_RC_CHECK( rc, PDERROR, "Create $id index failed in collection[%s], "
                      "rc: %d", pName, rc ) ;
      }

   done:
      if ( context )
      {
         releaseMBContext( context ) ;
      }
      if ( 0 != logRecSize )
      {
         pTransCB->releaseLogSpace( logRecSize );
      }
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_ADDCOLLECTION, rc ) ;
      return rc ;
   error:
      if ( metalocked )
      {
         ossUnlatch( &_metadataLatch, EXCLUSIVE ) ;
      }
      if ( DMS_INVALID_EXTENT != mbExExtent )
      {
         _releaseSpace( mbExExtent, mbExSize ) ;
      }
      if ( DMS_INVALID_MBID != newCollectionID )
      {
         INT32 rc1 = dropCollection( pName, cb, dropDps, sysCollection,
                                     context ) ;
         if ( rc1 )
         {
            PD_LOG( PDSEVERE, "Failed to clean up bad collection creation[%s], "
                    "rc: %d", pName, rc ) ;
         }
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_DROPCOLLECTION, "_dmsStorageData::dropCollection" )
   INT32 _dmsStorageData::dropCollection( const CHAR * pName, pmdEDUCB * cb,
                                          SDB_DPSCB * dpscb,
                                          BOOLEAN sysCollection,
                                          dmsMBContext * context )
   {
      INT32 rc                = 0 ;
      CHAR fullName[DMS_COLLECTION_FULL_NAME_SZ + 1] = {0} ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_DROPCOLLECTION ) ;
      dpsMergeInfo info ;
      dpsLogRecord &record    = info.getMergeBlock().record() ;
      UINT32 logRecSize       = 0;
      dpsTransCB *pTransCB    = pmdGetKRCB()->getTransCB() ;
      BOOLEAN isTransLocked   = FALSE ;
      BOOLEAN getContext      = FALSE ;
      BOOLEAN metalocked      = FALSE ;
      dmsMetaExtent *metaExt  = NULL ;

      SDB_ASSERT( pName, "Collection name cat't be NULL" ) ;

      rc = dmsCheckCLName ( pName, sysCollection ) ;
      PD_RC_CHECK( rc, PDERROR, "Invalid collection name %s, rc: %d",
                   pName, rc ) ;

      if ( dpscb )
      {
         rc = dpsCLDel2Record( _clFullName(pName, fullName, sizeof(fullName)),
                               record ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build record, rc: %d", rc ) ;

         rc = dpscb->checkSyncControl( record.alignedLen(), cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d", rc ) ;

         logRecSize = record.alignedLen() ;
         rc = pTransCB->reservedLogSpace( logRecSize ) ;
         if( rc )
         {
            PD_LOG( PDERROR, "Failed to reserved log space(length=%u)",
                    logRecSize ) ;
            logRecSize = 0 ;
            goto error ;
         }
      }

      if ( NULL == context )
      {
         rc = getMBContext( &context, pName, EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to get mb[%s] context, rc: %d",
                      pName, rc ) ;
         getContext = TRUE ;
      }
      else
      {
         rc = context->mbLock( EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDWARNING, "Collection[%s] mblock failed, rc: %d",
                      pName, rc ) ;
      }

      if ( !dmsAccessAndFlagCompatiblity ( context->mb()->_flag,
                                           DMS_ACCESS_TYPE_TRUNCATE ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  context->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      if ( cb && dpscb )
      {
         rc = pTransCB->transLockTryX( cb, _logicalCSID, context->mbID() ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to lock the collection, rc: %d",
                      rc ) ;
         isTransLocked = TRUE ;
      }

      rc = _pIdxSU->dropAllIndexes( context, cb, NULL ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to drop index for collection[%s], "
                   "rc: %d", pName, rc ) ;

      rc = _truncateCollection( context ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to truncate the collection[%s], rc: %d",
                   pName, rc ) ;

      if ( _pLobSU->isOpened() )
      {
         rc = _pLobSU->truncate( context, cb, NULL ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to truncate the collection[%s] lob,"
                      "rc: %d", pName, rc ) ;
      }

      DMS_SET_MB_FREE( context->mb()->_flag ) ;
      context->mb()->_logicalID-- ;

      metaExt = ( dmsMetaExtent* )extentAddr( context->mb()->_mbExExtentID ) ;
      if ( metaExt )
      {
         _releaseSpace( context->mb()->_mbExExtentID, metaExt->_blockSize ) ;
         metaExt->_flag = DMS_EXTENT_FLAG_FREED ;
      }

      context->mbUnlock() ;

      ossLatch( &_metadataLatch, EXCLUSIVE ) ;
      metalocked = TRUE ;
      _collectionNameRemove( pName ) ;
      _dmsHeader->_numMB-- ;

      if ( dpscb )
      {
         rc = _logDPS( dpscb, info, cb, &_metadataLatch, EXCLUSIVE, metalocked,
                       context->clLID(), DMS_INVALID_EXTENT ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert CLDel record to log, rc: "
                      "%d", rc ) ;
      }

   done:
      if ( metalocked )
      {
         ossUnlatch( &_metadataLatch, EXCLUSIVE ) ;
         metalocked = FALSE ;
      }
      if ( isTransLocked )
      {
         pTransCB->transLockRelease( cb, _logicalCSID, context->mbID() );
      }
      if ( context && getContext )
      {
         releaseMBContext( context ) ;
      }
      if ( 0 != logRecSize )
      {
         pTransCB->releaseLogSpace( logRecSize );
      }
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_DROPCOLLECTION, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_TRUNCATECOLLECTION, "_dmsStorageData::truncateCollection" )
   INT32 _dmsStorageData::truncateCollection( const CHAR *pName,
                                              pmdEDUCB *cb,
                                              SDB_DPSCB *dpscb,
                                              BOOLEAN sysCollection,
                                              dmsMBContext *context,
                                              BOOLEAN needChangeCLID )
   {
      INT32 rc           = SDB_OK ;
      BOOLEAN getContext = FALSE ;
      UINT32 newCLID     = DMS_INVALID_CLID ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_TRUNCATECOLLECTION ) ;
      CHAR fullName[DMS_COLLECTION_FULL_NAME_SZ + 1] = {0} ;
      dpsMergeInfo info ;
      dpsLogRecord &record    = info.getMergeBlock().record() ;
      UINT32 logRecSize       = 0;
      dpsTransCB *pTransCB    = pmdGetKRCB()->getTransCB() ;
      BOOLEAN isTransLocked   = FALSE ;

      SDB_ASSERT( pName, "Collection name cat't be NULL" ) ;

      rc = dmsCheckCLName ( pName, sysCollection ) ;
      PD_RC_CHECK( rc, PDERROR, "Invalid collection name %s, rc: %d",
                   pName, rc ) ;

      if ( dpscb )
      {
         rc = dpsCLTrunc2Record( _clFullName(pName, fullName, sizeof(fullName)),
                                 record ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build record, rc: %d", rc ) ;

         rc = dpscb->checkSyncControl( record.alignedLen(), cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d", rc ) ;

         logRecSize = record.alignedLen() ;
         rc = pTransCB->reservedLogSpace( logRecSize ) ;
         if( rc )
         {
            PD_LOG( PDERROR, "Failed to reserved log space(length=%u)",
                    logRecSize ) ;
            logRecSize = 0 ;
            goto error ;
         }
      }

      if ( NULL == context )
      {
         rc = getMBContext( &context, pName, EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to get mb[%s] context, rc: %d",
                      pName, rc ) ;
         getContext = TRUE ;
      }
      else
      {
         rc = context->mbLock( EXCLUSIVE ) ;
         PD_RC_CHECK( rc, PDWARNING, "Collection[%s] mblock failed, rc: %d",
                      pName, rc ) ;
      }

      if ( !dmsAccessAndFlagCompatiblity ( context->mb()->_flag,
                                           DMS_ACCESS_TYPE_TRUNCATE ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  context->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      if ( cb && dpscb )
      {
         rc = pTransCB->transLockTryX( cb, _logicalCSID, context->mbID() ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to lock the collection, rc: %d",
                      rc ) ;
         isTransLocked = TRUE ;
      }

      context->pause() ;
      ossLatch( &_metadataLatch, EXCLUSIVE ) ;
      if ( needChangeCLID )
      {
         newCLID = _dmsHeader->_MBHWM++ ;
      }
      ossUnlatch( &_metadataLatch, EXCLUSIVE ) ;

      rc = context->resume() ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context resume falied, rc: %d", rc ) ;

      if ( needChangeCLID )
      {
         context->mb()->_logicalID = newCLID ;
         context->_clLID           = newCLID ;
      }

      rc = _pIdxSU->truncateIndexes( context ) ;
      PD_RC_CHECK( rc, PDERROR, "Truncate collection[%s] indexes failed, "
                   "rc: %d", pName, rc ) ;

      rc = _truncateCollection( context ) ;
      PD_RC_CHECK( rc, PDERROR, "Truncate collection[%s] data failed, rc: %d",
                   pName, rc ) ;

      if ( _pLobSU->isOpened() )
      {
         rc = _pLobSU->truncate( context, cb, NULL ) ;
         PD_RC_CHECK( rc, PDERROR, "Truncate collection[%s] lob failed, rc: %d",
                      pName, rc ) ;
      }

      if ( dpscb )
      {
         rc = _logDPS( dpscb, info, cb, context, DMS_INVALID_EXTENT, TRUE ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert CLTrunc record to log, "
                      "rc: %d", rc ) ;
      }

   done:
      if ( isTransLocked )
      {
         pTransCB->transLockRelease( cb, _logicalCSID, context->mbID() ) ;
      }
      if ( context && getContext )
      {
         releaseMBContext( context ) ;
      }
      if ( 0 != logRecSize )
      {
         pTransCB->releaseLogSpace( logRecSize ) ;
      }
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_TRUNCATECOLLECTION, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_TRUNCATECOLLECTIONLOADS, "_dmsStorageData::truncateCollectionLoads" )
   INT32 _dmsStorageData::truncateCollectionLoads( const CHAR *pName,
                                                   dmsMBContext * context )
   {
      INT32 rc           = SDB_OK ;
      BOOLEAN getContext = FALSE ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_TRUNCATECOLLECTIONLOADS ) ;
      if ( NULL == context )
      {
         SDB_ASSERT( pName, "Collection name cat't be NULL" ) ;

         rc = getMBContext( &context, pName, -1 ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to get mb[%s] context, rc: %d",
                      pName, rc ) ;
         getContext = TRUE ;
      }

      rc = _truncateCollectionLoads( context ) ;
      PD_RC_CHECK( rc, PDERROR, "Truncate collection[%s] loads failed, rc: %d",
                   pName, rc ) ;

   done:
      if ( context && getContext )
      {
         releaseMBContext( context ) ;
      }
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_TRUNCATECOLLECTIONLOADS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_RENAMECOLLECTION, "_dmsStorageData::renameCollecion" )
   INT32 _dmsStorageData::renameCollection( const CHAR * oldName,
                                            const CHAR * newName,
                                            pmdEDUCB * cb,
                                            SDB_DPSCB * dpscb,
                                            BOOLEAN sysCollection )
   {
      INT32 rc                = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_RENAMECOLLECTION ) ;
      dpsTransCB *pTransCB    = pmdGetKRCB()->getTransCB() ;
      UINT32 logRecSize       = 0 ;
      dpsMergeInfo info ;
      dpsLogRecord &record    = info.getMergeBlock().record() ;
      BOOLEAN metalocked      = FALSE ;
      UINT16  mbID            = DMS_INVALID_MBID ;
      UINT32  clLID           = DMS_INVALID_CLID ;

      PD_TRACE2 ( SDB__DMSSTORAGEDATA_RENAMECOLLECTION,
                  PD_PACK_STRING ( oldName ),
                  PD_PACK_STRING ( newName ) ) ;
      rc = dmsCheckCLName ( oldName, sysCollection ) ;
      PD_RC_CHECK( rc, PDERROR, "Invalid old collection name %s, rc: %d",
                   oldName, rc ) ;
      rc = dmsCheckCLName ( newName, sysCollection ) ;
      PD_RC_CHECK( rc, PDERROR, "Invalid new collection name %s, rc: %d",
                   newName, rc ) ;

      if ( dpscb )
      {
         rc = dpsCLRename2Record( getSuName(), oldName, newName, record ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build log record, rc: %d", rc ) ;

         rc = dpscb->checkSyncControl( record.alignedLen(), cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d", rc ) ;

         logRecSize = record.alignedLen() ;
         rc = pTransCB->reservedLogSpace( logRecSize );
         if( rc )
         {
            PD_LOG( PDERROR, "Failed to reserved log space(length=%u)",
                    logRecSize ) ;
            logRecSize = 0 ;
            goto error ;
         }
      }

      ossLatch ( &_metadataLatch, EXCLUSIVE ) ;
      metalocked = TRUE ;

      mbID = _collectionNameLookup( oldName ) ;

      if ( DMS_INVALID_MBID == mbID )
      {
         rc = SDB_DMS_NOTEXIST ;
         goto error ;
      }
      if ( DMS_INVALID_MBID != _collectionNameLookup ( newName ) )
      {
         rc = SDB_DMS_EXIST ;
         goto error ;
      }

      _collectionNameRemove ( oldName ) ;
      _collectionNameInsert ( newName, mbID ) ;
      ossMemset ( _dmsMME->_mbList[mbID]._collectionName, 0,
                  DMS_COLLECTION_NAME_SZ ) ;
      ossStrncpy ( _dmsMME->_mbList[mbID]._collectionName, newName,
                   DMS_COLLECTION_NAME_SZ ) ;
      clLID = _dmsMME->_mbList[mbID]._logicalID ;

      if ( dpscb )
      {
         rc = _logDPS( dpscb, info, cb, &_metadataLatch, EXCLUSIVE, metalocked,
                       clLID, DMS_INVALID_EXTENT ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert clrename to log, rc = %d",
                      rc ) ;
      }

   done :
      if ( metalocked )
      {
         ossUnlatch ( &_metadataLatch, EXCLUSIVE ) ;
         metalocked = FALSE ;
      }
      if ( 0 != logRecSize )
      {
         pTransCB->releaseLogSpace( logRecSize );
      }
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_RENAMECOLLECTION, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_FINDCOLLECTION, "_dmsStorageData::findCollection" )
   INT32 _dmsStorageData::findCollection( const CHAR * pName,
                                          UINT16 & collectionID )
   {
      INT32 rc            = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_FINDCOLLECTION ) ;
      PD_TRACE1 ( SDB__DMSSTORAGEDATA_FINDCOLLECTION,
                  PD_PACK_STRING ( pName ) ) ;
      ossLatch ( &_metadataLatch, SHARED ) ;
      collectionID = _collectionNameLookup ( pName ) ;
      PD_TRACE1 ( SDB__DMSSTORAGEDATA_FINDCOLLECTION,
                  PD_PACK_USHORT ( collectionID ) ) ;
      if ( DMS_INVALID_MBID == collectionID )
      {
         rc = SDB_DMS_NOTEXIST ;
         goto error ;
      }

   done :
      ossUnlatch ( &_metadataLatch, SHARED ) ;
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_FINDCOLLECTION, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_INSERTRECORD, "_dmsStorageData::insertRecord" )
   INT32 _dmsStorageData::insertRecord( dmsMBContext *context,
                                        const BSONObj &record, pmdEDUCB *cb,
                                        SDB_DPSCB *dpscb, BOOLEAN mustOID,
                                        BOOLEAN canUnLock )
   {
      INT32 rc                      = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_INSERTRECORD ) ;
      IDToInsert oid ;
      idToInsertEle oidEle((CHAR*)(&oid)) ;
      BOOLEAN addOID                = FALSE ;
      UINT32 oidLen                 = 0 ;
      UINT32 dmsRecordSize          = 0 ;
      CHAR fullName[DMS_COLLECTION_FULL_NAME_SZ + 1] = {0} ;
      dpsTransCB *pTransCB          = pmdGetKRCB()->getTransCB() ;
      UINT32 logRecSize             = 0 ;
      monAppCB * pMonAppCB          = cb ? cb->getMonAppCB() : NULL ;
      dpsMergeInfo info ;
      dpsLogRecord &logRecord       = info.getMergeBlock().record() ;
      SDB_DPSCB *dropDps            = NULL ;
      BOOLEAN isCompressed          = FALSE ;
      const CHAR *compressedData    = NULL ;
      INT32 compressedDataSize      = 0 ;
      DPS_TRANS_ID transID          = cb->getTransID() ;
      DPS_LSN_OFFSET preTransLsn    = cb->getCurTransLsn() ;
      DPS_LSN_OFFSET relatedLsn     = cb->getRelatedTransLSN() ;
      BOOLEAN  isTransLocked        = FALSE ;
      dmsRecordID foundDeletedID ;
      ossValuePtr extentPtr         = 0 ;
      ossValuePtr deletedRecordPtr  = 0 ;
      ossValuePtr insertedDataPtr   = 0 ;

      BSONElement ele = record.getField ( DMS_ID_KEY_NAME ) ;
      if ( ele.type() == Array )
      {
         PD_LOG ( PDERROR, "record id can't be array: %s",
                  record.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( mustOID && ele.eoo() )
      {
         oid._oid.init() ;
         oidLen = oidEle.size() ;
         addOID = TRUE ;
      }
      if ( record.objsize() + DMS_RECORD_METADATA_SZ + oidLen >
           DMS_RECORD_USER_MAX_SZ )
      {
         rc = SDB_DMS_RECORD_TOO_BIG ;
         goto error ;
      }
      if ( OSS_BIT_TEST ( context->mb()->_attributes,
                          DMS_MB_ATTR_COMPRESSED ) )
      {
         rc = dmsCompress( cb, record, ((CHAR*)(&oid)), oidLen,
                             &compressedData, &compressedDataSize ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to compress record[%s], rc: %d",
                       record.toString().c_str(), rc ) ;
         dmsRecordSize = compressedDataSize + sizeof(INT32) ;
         PD_TRACE2 ( SDB__DMSSTORAGEDATA_INSERTRECORD,
                     PD_PACK_STRING ( "size after compress" ),
                     PD_PACK_UINT ( dmsRecordSize ) ) ;
                     
         if ( dmsRecordSize > (UINT32)(record.objsize() + oidLen) )
         {
            dmsRecordSize = record.objsize() ;
         }
         else
         {
            addOID = FALSE ;
            oidLen = 0 ;
            isCompressed = TRUE ;
         }
      }
      else
      {
         dmsRecordSize = record.objsize() ;
      }

      dmsRecordSize += ( DMS_RECORD_METADATA_SZ + oidLen ) ;
      dmsRecordSize *= DMS_RECORD_OVERFLOW_RATIO ;
      dmsRecordSize = OSS_MIN( DMS_RECORD_MAX_SZ,
                               ossAlignX ( dmsRecordSize, 4 ) ) ;
      PD_TRACE2 ( SDB__DMSSTORAGEDATA_INSERTRECORD,
                  PD_PACK_STRING ( "size after align" ),
                  PD_PACK_UINT ( dmsRecordSize ) ) ;

      if ( dpscb )
      {
         _clFullName( context->mb()->_collectionName, fullName,
                      sizeof(fullName) ) ;
         rc = dpsInsert2Record( fullName, record, transID,
                                preTransLsn, relatedLsn, logRecord ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build record, rc: %d", rc ) ;

         logRecSize = ossAlign4( logRecord.alignedLen() + oidLen ) ;

         rc = dpscb->checkSyncControl( logRecSize, cb ) ;
         if ( SDB_OK != rc )
         {
            logRecSize = 0 ;
            PD_LOG( PDERROR, "Check sync control failed, rc: %d", rc ) ;
            goto error ;
         }

         logRecSize = ossAlign4( logRecord.alignedLen() + oidLen ) ;
         rc = pTransCB->reservedLogSpace( logRecSize ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to reserved log space(length=%u)",
                    logRecSize ) ;
            logRecSize = 0 ;
            goto error ;
         }
      }

      rc = context->mbLock( EXCLUSIVE ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

      if ( !dmsAccessAndFlagCompatiblity ( context->mb()->_flag,
                                           DMS_ACCESS_TYPE_INSERT ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  context->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      rc = _reserveFromDeleteList( context, dmsRecordSize,
                                   foundDeletedID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Reserve delete record failed, rc: %d", rc ) ;

      extentPtr = extentAddr( foundDeletedID._extent ) ;
      if ( !extentPtr || !((dmsExtent*)extentPtr)->validate(context->mbID()) )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      deletedRecordPtr = extentPtr + foundDeletedID._offset ;

      if ( dpscb )
      {
         rc = pTransCB->transLockTryX( cb, _logicalCSID, context->mbID(),
                                       &foundDeletedID ) ;
         SDB_ASSERT( SDB_OK == rc, "Failed to get record-X-LOCK" );
         PD_RC_CHECK( rc, PDERROR, "Failed to insert the record, get "
                     "transaction-X-lock of record failed, rc: %d", rc );
         isTransLocked = TRUE ;
      }
      rc = _extentInsertRecord( context, deletedRecordPtr, dmsRecordSize,
                                isCompressed ? (ossValuePtr)compressedData :
                                               (ossValuePtr)record.objdata(),
                                isCompressed ?  compressedDataSize :
                                                record.objsize(),
                                foundDeletedID._extent,
                                addOID ? &oidEle : NULL, cb, isCompressed,
                                TRUE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to append record, rc: %d", rc ) ;

      DMS_MON_OP_COUNT_INC( pMonAppCB, MON_INSERT, 1 ) ;

      _markDirty ( foundDeletedID._extent ) ;

      {
         DMS_RECORD_EXTRACTDATA ( deletedRecordPtr, insertedDataPtr ) ;
         BSONObj insertedObj ( (CHAR*)insertedDataPtr ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_READ, 1 ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_READ, 1 ) ;
         rc = _pIdxSU->indexesInsert(context, ((dmsExtent*)extentPtr)->_logicID,
                                     insertedObj, foundDeletedID, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert to index, rc: %d", rc ) ;
      }

      if ( dpscb )
      {
         dmsExtentID extLID = ((dmsExtent*)extentPtr)->_logicID ;
         info.clear() ;
         rc = dpsInsert2Record( fullName, BSONObj((CHAR*)insertedDataPtr),
                                transID, preTransLsn, relatedLsn, logRecord ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build insert record, rc: %d",
                      rc ) ;

         rc = _logDPS( dpscb, info, cb, context, extLID, canUnLock ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to insert record into log, rc: %d",
                       rc ) ;
         dropDps = dpscb ;
         if ( cb && transID != DPS_INVALID_TRANS_ID )
         {
            cb->setCurTransLsn( info.getMergeBlock().record().head()._lsn ) ;
            if ( pmdGetKRCB()->getTransCB()->isFirstOp( transID ))
            {
               pmdGetKRCB()->getTransCB()->clearFirstOpTag( transID );
               cb->setTransID( transID ) ;
            }
         }
      }

   done:
      if ( isTransLocked && ( transID == DPS_INVALID_TRANS_ID || rc ) )
      {
         pTransCB->transLockRelease( cb, _logicalCSID, context->mbID(),
                                     &foundDeletedID ) ;
      }
      if ( 0 != logRecSize )
      {
         pTransCB->releaseLogSpace( logRecSize ) ;
      }
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_INSERTRECORD, rc ) ;
      return rc ;
   error:
      if ( 0 != insertedDataPtr )
      {
         INT32 rc1 = deleteRecord( context, foundDeletedID, insertedDataPtr,
                                   cb, dropDps ) ;
         if ( rc1 )
         {
            PD_LOG( PDERROR, "Failed to rollback, rc: %d", rc1 ) ;
         }
      }
      else if ( foundDeletedID.isValid() )
      {
         _saveDeletedRecord( context->mb(), foundDeletedID, 0 ) ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__EXTENTINSERTRECORD, "_dmsStorageData::_extentInsertRecord" )
   INT32 _dmsStorageData::_extentInsertRecord( dmsMBContext *context,
                                               ossValuePtr deletedRecordPtr,
                                               UINT32 dmsRecordSize,
                                               ossValuePtr ptr,
                                               INT32 len, INT32 extentID,
                                               BSONElement *extraOID,
                                               pmdEDUCB *cb,
                                               BOOLEAN compressed,
                                               BOOLEAN addIntoList )
   {
      INT32 rc                         = SDB_OK ;
      dmsOffset deletedRecordOffset    = DMS_INVALID_OFFSET ;
      dmsExtent *extent                = NULL ;
      monAppCB * pMonAppCB             = cb ? cb->getMonAppCB() : NULL ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__EXTENTINSERTRECORD ) ;
      rc = context->mbLock( EXCLUSIVE ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

      if ( DMS_DELETEDRECORD_GETSIZE ( deletedRecordPtr ) < dmsRecordSize )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      deletedRecordOffset = DMS_DELETEDRECORD_GETMYOFFSET( deletedRecordPtr ) ;
      extent = (dmsExtent*)( deletedRecordPtr - deletedRecordOffset ) ;

      DMS_RECORD_SETSTATE ( deletedRecordPtr, DMS_RECORD_FLAG_NORMAL ) ;
      DMS_RECORD_RESETATTR ( deletedRecordPtr ) ;
      if ( compressed )
      {
         DMS_RECORD_SETATTR ( deletedRecordPtr, DMS_RECORD_FLAG_COMPRESSED ) ;
      }

      if ( (DMS_DELETEDRECORD_GETSIZE( deletedRecordPtr ) - dmsRecordSize) >
           DMS_MIN_RECORD_SZ )
      {
         dmsOffset newOffset = DMS_DELETEDRECORD_GETMYOFFSET(deletedRecordPtr) +
                               dmsRecordSize ;
         INT32 newSize = DMS_DELETEDRECORD_GETSIZE(deletedRecordPtr) -
                         dmsRecordSize ;
         rc = _saveDeletedRecord ( context->mb(), extent, newOffset, newSize,
                                   extentID ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to save deleted record, rc: %d", rc ) ;
            goto error ;
         }
         DMS_RECORD_SETSIZE ( deletedRecordPtr, dmsRecordSize ) ;
      }

      if ( NULL == extraOID )
      {
         DMS_RECORD_SETDATA ( deletedRecordPtr, ptr, len ) ;
      }
      else
      {
         DMS_RECORD_SETDATA_OID ( deletedRecordPtr, ptr, len, (*extraOID) ) ;
      }
      DMS_RECORD_SETNEXTOFFSET ( deletedRecordPtr, DMS_INVALID_OFFSET ) ;
      DMS_RECORD_SETPREVOFFSET ( deletedRecordPtr, DMS_INVALID_OFFSET ) ;
      DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_WRITE, 1 ) ;

      if ( addIntoList )
      {
         dmsOffset   offset      = extent->_lastRecordOffset ;
         extent->_recCount++ ;
         ++( _mbStatInfo[ context->mbID() ]._totalRecords ) ;
         if ( DMS_INVALID_OFFSET != offset )
         {
            ossValuePtr preRecord = ((ossValuePtr)extent + offset) ;
            DMS_RECORD_SETNEXTOFFSET ( preRecord, deletedRecordOffset ) ;
            DMS_RECORD_SETPREVOFFSET ( deletedRecordPtr, offset ) ;
         }
         extent->_lastRecordOffset = deletedRecordOffset ;
         if ( DMS_INVALID_OFFSET == extent->_firstRecordOffset )
         {
            extent->_firstRecordOffset = deletedRecordOffset ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__EXTENTINSERTRECORD, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_DELETERECORD, "_dmsStorageData::deleteRecord" )
   INT32 _dmsStorageData::deleteRecord( dmsMBContext *context,
                                        const dmsRecordID &recordID,
                                        ossValuePtr deletedDataPtr,
                                        pmdEDUCB *cb,
                                        SDB_DPSCB * dpscb )
   {
      INT32 rc                      = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_DELETERECORD ) ;
      ossValuePtr extentPtr         = extentAddr ( recordID._extent ) ;
      ossValuePtr recordPtr         = extentPtr + recordID._offset ;
      ossValuePtr realPtr           = recordPtr ;
      dpsTransCB *pTransCB          = pmdGetKRCB()->getTransCB() ;
      monAppCB * pMonAppCB          = cb ? cb->getMonAppCB() : NULL ;
      BOOLEAN isDeleting            = FALSE ;
      BOOLEAN isNeedToSetDeleting   = FALSE ;

      dmsRecordID ovfRID ;
      BSONObj delObject ;
      UINT32 logRecSize             = 0 ;
      dpsMergeInfo info ;
      dpsLogRecord &record          = info.getMergeBlock().record() ;
      CHAR fullName[DMS_COLLECTION_FULL_NAME_SZ + 1] = {0} ;
      DPS_TRANS_ID transID          = cb->getTransID() ;
      DPS_LSN_OFFSET preLsn         = cb->getCurTransLsn() ;
      DPS_LSN_OFFSET relatedLSN     = cb->getRelatedTransLSN() ;
      CHAR recordState              = 0 ;

      if ( !extentPtr )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( !context->isMBLock( EXCLUSIVE ) )
      {
         PD_LOG( PDERROR, "Caller must hold mb exlusive lock[%s]",
                 context->toString().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

#ifdef _DEBUG
      if ( !dmsAccessAndFlagCompatiblity ( context->mb()->_flag,
                                           DMS_ACCESS_TYPE_DELETE ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  context->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }
#endif //_DEBUG

      recordState = DMS_RECORD_GETSTATE(recordPtr) ;
      if ( DMS_RECORD_FLAG_OVERFLOWF == recordState )
      {
         ovfRID = DMS_RECORD_GETOVF(recordPtr) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_READ, 1 ) ;
         realPtr = extentAddr(ovfRID._extent) + ovfRID._offset ;
         SDB_ASSERT( DMS_RECORD_FLAG_OVERFLOWT == DMS_RECORD_GETSTATE(realPtr),
                     "ovf record must be over flow to" ) ;
      }
      else if ( DMS_RECORD_FLAG_OVERFLOWT == recordState )
      {
         _extentRemoveRecord( context->mb(), recordID, 0, cb ) ;
         goto done ;
      }
      else if ( DMS_RECORD_FLAG_DELETED == recordState )
      {
         rc = SDB_DMS_RECORD_NOTEXIST ;
         goto error ;
      }
      else if ( DMS_RECORD_FLAG_NORMAL != recordState )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      if ( pTransCB->hasWait( _logicalCSID, context->mbID(), &recordID ) )
      {
         if ( OSS_BIT_TEST ( DMS_RECORD_FLAG_DELETING,
                             DMS_RECORD_GETATTR(recordPtr)) )
         {
            rc = SDB_OK ;
            goto done ;
         }
         isNeedToSetDeleting = TRUE ;
      }
      else if ( OSS_BIT_TEST ( DMS_RECORD_FLAG_DELETING,
                               DMS_RECORD_GETATTR(recordPtr)) )
      {
         isDeleting = TRUE ;
      }

      if ( FALSE == isDeleting ) // first delete the record
      {
         if ( !deletedDataPtr )
         {
            DMS_RECORD_EXTRACTDATA ( realPtr, deletedDataPtr ) ;
            DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_READ, 1 ) ;
            DMS_MON_OP_COUNT_INC( pMonAppCB, MON_READ, 1 ) ;
         }
         try
         {
            delObject = BSONObj( (CHAR*)deletedDataPtr ) ;
            if ( NULL != dpscb )
            {
               _clFullName( context->mb()->_collectionName, fullName,
                            sizeof(fullName) ) ;
               rc = dpsDelete2Record( fullName, delObject, transID,
                                      preLsn, relatedLSN, record ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "Failed to build record: %d",rc ) ;
                  goto error ;
               }

               rc = dpscb->checkSyncControl( record.alignedLen(), cb ) ;
               PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d",
                            rc ) ;

               logRecSize = record.alignedLen() ;
               rc = pTransCB->reservedLogSpace( logRecSize ) ;
               if ( rc )
               {
                  PD_LOG( PDERROR, "Failed to reserved log space(length=%u)",
                          logRecSize ) ;
                  logRecSize = 0 ;
                  goto error ;
               }
            }
            rc = _pIdxSU->indexesDelete( context, recordID._extent,
                                         delObject, recordID, cb ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to delete indexes, rc: %d",rc ) ;
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Corrupted record: %d:%d: %s",
                     recordID._extent, recordID._offset, e.what() ) ;
            rc = SDB_CORRUPTED_RECORD ;
            goto error ;
         }
      }

      if ( FALSE == isNeedToSetDeleting )
      {
         rc = _extentRemoveRecord( context->mb(), recordID, 0, cb,
                                   !isDeleting ) ;
         PD_RC_CHECK( rc, PDERROR, "Extent remove record failed, rc: %d", rc ) ;
         if ( ovfRID.isValid() )
         {
            _extentRemoveRecord( context->mb(), ovfRID, 0, cb ) ;
         }
      }
      else
      {
         DMS_RECORD_SETATTR( recordPtr, DMS_RECORD_FLAG_DELETING ) ;
         --((dmsExtent*)extentPtr)->_recCount ;
         --( _mbStatInfo[ context->mbID() ]._totalRecords ) ;
      }

      if ( FALSE == isDeleting )
      {
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DELETE, 1 ) ;
      }

      if ( dpscb && FALSE == isDeleting )
      {
         dmsExtentID extLID = ((dmsExtent*)extentPtr)->_logicID ;
         rc = _logDPS( dpscb, info, cb, context, extLID, FALSE ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert record into log, rc: %d",
                      rc ) ;

         if ( cb && transID != DPS_INVALID_TRANS_ID )
         {
            cb->setCurTransLsn( info.getMergeBlock().record().head()._lsn );
            if ( pTransCB->isFirstOp( transID ))
            {
               pTransCB->clearFirstOpTag( transID ) ;
               cb->setTransID( transID ) ;
            }
         }
      }

   done :
      if ( 0 != logRecSize )
      {
         pTransCB->releaseLogSpace( logRecSize ) ;
      }
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_DELETERECORD, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__EXTENTREMOVERECORD, "_dmsStorageData::_extentRemoveRecord" )
   INT32 _dmsStorageData::_extentRemoveRecord( dmsMB *mb,
                                               const dmsRecordID &recordID,
                                               INT32 recordSize,
                                               pmdEDUCB * cb,
                                               BOOLEAN decCount )
   {
      INT32 rc              = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__EXTENTREMOVERECORD ) ;
      monAppCB * pMonAppCB  = cb ? cb->getMonAppCB() : NULL ;
      ossValuePtr extentPtr = extentAddr( recordID._extent ) ;
      ossValuePtr recordPtr = extentPtr + recordID._offset ;

      if ( DMS_RECORD_FLAG_OVERFLOWT != DMS_RECORD_GETSTATE(recordPtr) )
      {
         dmsExtent *extent = (dmsExtent*)extentPtr ;
         dmsOffset prevRecordOffset = DMS_RECORD_GETPREVOFFSET(recordPtr) ;
         dmsOffset nextRecordOffset = DMS_RECORD_GETNEXTOFFSET(recordPtr) ;

         if ( DMS_INVALID_OFFSET != prevRecordOffset )
         {
            DMS_RECORD_SETNEXTOFFSET ( extentPtr+prevRecordOffset,
                                       nextRecordOffset ) ;
         }
         if ( DMS_INVALID_OFFSET != nextRecordOffset )
         {
            DMS_RECORD_SETPREVOFFSET ( extentPtr+nextRecordOffset,
                                       prevRecordOffset ) ;
         }
         if ( extent->_firstRecordOffset == DMS_RECORD_GETMYOFFSET(recordPtr) )
         {
            extent->_firstRecordOffset = nextRecordOffset ;
         }
         if ( extent->_lastRecordOffset == DMS_RECORD_GETMYOFFSET(recordPtr) )
         {
            extent->_lastRecordOffset = prevRecordOffset ;
         }

         if ( decCount )
         {
            --(extent->_recCount) ;
            --( _mbStatInfo[ mb->_blockID ]._totalRecords ) ;
         }
      }
      DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_WRITE, 1 ) ;

      _markDirty ( recordID._extent ) ;

      rc = _saveDeletedRecord( mb, recordID, recordSize ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to save deleted record, rc = %d", rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__EXTENTREMOVERECORD, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_UPDATERECORD, "_dmsStorageData::updateRecord" )
   INT32 _dmsStorageData::updateRecord( dmsMBContext *context,
                                        const dmsRecordID &recordID,
                                        ossValuePtr updatedDataPtr,
                                        pmdEDUCB *cb, SDB_DPSCB *dpscb,
                                        mthModifier &modifier )
   {
      INT32 rc                      = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_UPDATERECORD ) ;
      ossValuePtr extentPtr         = 0 ;
      dmsExtent *extent             = NULL ;
      monAppCB * pMonAppCB          = cb ? cb->getMonAppCB() : NULL ;
      BSONObj oldMatch, oldChg ;
      BSONObj newMatch, newChg ;
      UINT32 logRecSize             = 0 ;
      dpsMergeInfo info ;
      dpsLogRecord &record = info.getMergeBlock().record() ;
      dpsTransCB *pTransCB = pmdGetKRCB()->getTransCB() ;
      CHAR fullName[DMS_COLLECTION_FULL_NAME_SZ + 1] = {0} ;
      DPS_TRANS_ID transID = cb->getTransID() ;
      DPS_LSN_OFFSET preTransLsn = cb->getCurTransLsn() ;
      DPS_LSN_OFFSET relatedLSN = cb->getRelatedTransLSN() ;

      extentPtr = extentAddr ( recordID._extent ) ;
      if ( !extentPtr )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      extent = ( dmsExtent* )extentPtr ;

      if ( !context->isMBLock( EXCLUSIVE ) )
      {
         PD_LOG( PDERROR, "Caller must hold mb exclusive lock[%s]",
                 context->toString().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

#ifdef _DEBUG
      if ( !dmsAccessAndFlagCompatiblity ( context->mb()->_flag,
                                           DMS_ACCESS_TYPE_UPDATE ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  context->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }
      if ( !extent->validate( context->mbID() ) )
      {
         rc = SDB_SYS ;
         goto error ;
      }
#endif //_DEBUG

      if ( 0 == updatedDataPtr )
      {
         ossValuePtr recordPtr = extentPtr+recordID._offset ;
         ossValuePtr recordRealPtr = recordPtr ;
         if ( DMS_RECORD_FLAG_OVERFLOWF == DMS_RECORD_GETSTATE(recordPtr) )
         {
            dmsRecordID ovfRID = DMS_RECORD_GETOVF(recordPtr) ;
            DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_READ, 1 ) ;
            ossValuePtr ovfExtentPtr = extentAddr ( ovfRID._extent ) ;
            recordRealPtr = ovfExtentPtr + ovfRID._offset ;
         }
         DMS_RECORD_EXTRACTDATA( recordRealPtr, updatedDataPtr ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_READ, 1 ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_READ, 1 ) ;
      }

      try
      {
         BSONObj obj ( (const CHAR*)updatedDataPtr ) ;
         BSONObj newobj ;
         try
         {
            if ( dpscb )
            {
               rc = modifier.modify ( obj, newobj, &oldMatch, &oldChg,
                                      &newMatch, &newChg ) ;
               if ( SDB_OK == rc && newChg.isEmpty() )
               {
                  SDB_ASSERT( oldChg.isEmpty(), "Old change must be empty" ) ;
                  goto done ;
               }
            }
            else
            {
               rc = modifier.modify ( obj, newobj ) ;
            }

            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to create modified record, rc: %d",
                        rc ) ;
               goto error ;
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Exception happened while trying to update "
                     "record: %s", e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         if ( NULL != dpscb )
         {
            _clFullName( context->mb()->_collectionName, fullName,
                         sizeof(fullName) ) ;
            rc = dpsUpdate2Record( fullName, oldMatch, oldChg, newMatch,
                                   newChg, transID, preTransLsn,
                                   relatedLSN, record ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to build record:%d", rc ) ;
               goto error ;
            }

            rc = dpscb->checkSyncControl( record.alignedLen(), cb ) ;
            PD_RC_CHECK( rc, PDERROR, "Check sync control failed, rc: %d",
                         rc ) ;

            logRecSize = record.alignedLen() ;
            rc = pTransCB->reservedLogSpace( logRecSize );
            if ( rc )
            {
               PD_LOG( PDERROR, "Failed to reserved log space(len:%u), rc: %d",
                       logRecSize, rc ) ;
               logRecSize = 0 ;
               goto error ;
            }
         }

         rc = _extentUpdatedRecord( context, recordID, updatedDataPtr,
                                    extent->_logicID,
                                    (ossValuePtr)newobj.objdata(),
                                    newobj.objsize(), cb) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to update record, rc: %d", rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_CORRUPTED_RECORD ;
         goto error ;
      }

      DMS_MON_OP_COUNT_INC( pMonAppCB, MON_UPDATE, 1 ) ;

      if ( dpscb )
      {
         PD_LOG ( PDDEBUG, "oldChange: %s,%s\nnewChange: %s,%s",
                  oldMatch.toString().c_str(),
                  oldChg.toString().c_str(),
                  newMatch.toString().c_str(),
                  newChg.toString().c_str() ) ;

         rc = _logDPS( dpscb, info, cb, context, extent->_logicID, FALSE ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert update record into log, "
                      "rc: %d", rc ) ;

         if ( cb && transID != DPS_INVALID_TRANS_ID )
         {
            cb->setCurTransLsn( info.getMergeBlock().record().head()._lsn ) ;
            if ( pTransCB->isFirstOp( transID ))
            {
               pTransCB->clearFirstOpTag( transID ) ;
               cb->setTransID( transID ) ;
            }
         }
      }

   done :
      if ( 0 != logRecSize )
      {
         pTransCB->releaseLogSpace( logRecSize );
      }
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_UPDATERECORD, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA__EXTENTUPDATERECORD, "_dmsStorageData::_extentUpdatedRecord" )
   INT32 _dmsStorageData::_extentUpdatedRecord( dmsMBContext *context,
                                                const dmsRecordID &recordID,
                                                ossValuePtr recordDataPtr,
                                                dmsExtentID extLID,
                                                ossValuePtr ptr,
                                                INT32 len,
                                                pmdEDUCB *cb )
   {
      INT32 rc                     = SDB_OK ;
      ossValuePtr recordPtr        = 0 ;
      ossValuePtr realRecordPtr    = 0 ;
      UINT32 dmsRecordSize         = 0 ;
      INT32 compressedDataSize     = 0 ;
      const CHAR *compressedData   = NULL ;
      BOOLEAN isCompressed         = FALSE ;
      dmsRecordID ovfRID ;
      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA__EXTENTUPDATERECORD ) ;
      monAppCB * pMonAppCB         = cb ? cb->getMonAppCB() : NULL ;

      SDB_ASSERT ( 0 != recordDataPtr, "recordDataPtr can't be NULL" ) ;

      if ( len + DMS_RECORD_METADATA_SZ > DMS_RECORD_USER_MAX_SZ )
      {
         PD_LOG ( PDERROR, "record is too big: %d", len ) ;
         rc = SDB_DMS_RECORD_TOO_BIG ;
         goto error ;
      }
      recordPtr = extentAddr(recordID._extent) + recordID._offset ;
      realRecordPtr = recordPtr ;

      if ( !context->isMBLock( EXCLUSIVE ) )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Caller must hold exclusive lock[%s]",
                 context->toString().c_str() ) ;
         goto error ;
      }

      if ( OSS_BIT_TEST(context->mb()->_attributes, DMS_MB_ATTR_COMPRESSED ) )
      {
         rc = dmsCompress( cb, (const CHAR*)ptr, len, &compressedData,
                           &compressedDataSize ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed, to compress record, rc: %d: %s",
                       rc, BSONObj((CHAR*)ptr).toString().c_str() ) ;

         dmsRecordSize = compressedDataSize + sizeof(INT32) ;
         if ( dmsRecordSize > (UINT32)len )
         {
            dmsRecordSize = (UINT32)len ;
         }
         else
         {
            isCompressed = TRUE ;
         }
      }
      else
      {
         dmsRecordSize = len ;
      }

      dmsRecordSize += DMS_RECORD_METADATA_SZ ;
      {
         BSONObj oriObj( (CHAR*)recordDataPtr ) ;
         BSONObj newObj( (CHAR*)ptr ) ;
         rc = _pIdxSU->indexesUpdate( context, extLID, oriObj, newObj,
                                      recordID, cb, FALSE ) ;
         if ( rc )
         {
            PD_LOG ( PDWARNING, "Failed to update index, rc: %d", rc ) ;
            goto error_rollbackindex ;
         }
      }

      if ( DMS_RECORD_FLAG_OVERFLOWF == DMS_RECORD_GETSTATE(recordPtr) )
      {
         ovfRID = DMS_RECORD_GETOVF( recordPtr ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_READ, 1 ) ;
         realRecordPtr = extentAddr(ovfRID._extent) + ovfRID._offset ;
      }

      if ( dmsRecordSize <= DMS_RECORD_GETSIZE(realRecordPtr) )
      {
         if ( isCompressed )
         {
            DMS_RECORD_SETATTR ( realRecordPtr, DMS_RECORD_FLAG_COMPRESSED ) ;
            DMS_RECORD_SETDATA ( realRecordPtr, compressedData,
                                 compressedDataSize ) ;
         }
         else
         {
            DMS_RECORD_UNSETATTR ( realRecordPtr, DMS_RECORD_FLAG_COMPRESSED ) ;
            DMS_RECORD_SETDATA ( realRecordPtr, ptr, len ) ;
         }
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_WRITE, 1 ) ;
         _markDirty ( recordID._extent ) ;
         goto done ;
      }
      else
      {
         dmsRecordID foundDeletedID ;
         ossValuePtr extentPtr        = 0 ;
         ossValuePtr deletedRecordPtr = 0 ;

         dmsRecordSize *= DMS_RECORD_OVERFLOW_RATIO ;
         dmsRecordSize = OSS_MIN( DMS_RECORD_MAX_SZ,
                                  ossAlignX ( dmsRecordSize, 4 ) ) ;

         rc = _reserveFromDeleteList ( context, dmsRecordSize,
                                       foundDeletedID, cb ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to reserve delete record, rc: %d", rc ) ;
            goto error_rollbackindex ;
         }
         extentPtr = extentAddr(foundDeletedID._extent) ;
         if ( 0 == extentPtr )
         {
            PD_LOG( PDERROR, "Found non-exist extent[%d:%d]",
                    foundDeletedID._extent, foundDeletedID._offset ) ;
            rc = SDB_SYS ;
            goto error_rollbackindex ;
         }
         if ( !((dmsExtent*)extentPtr)->validate(context->mbID()))
         {
            PD_LOG ( PDERROR, "Invalid extent[%d] is detected",
                     foundDeletedID._extent ) ;
            rc = SDB_SYS ;
            goto error_rollbackindex ;
         }
         deletedRecordPtr = extentPtr+foundDeletedID._offset ;
         rc = _extentInsertRecord ( context, deletedRecordPtr, dmsRecordSize,
                                    isCompressed?(ossValuePtr)compressedData:
                                                 (ossValuePtr)ptr,
                                    isCompressed?compressedDataSize:len,
                                    foundDeletedID._extent, NULL, cb,
                                    isCompressed, FALSE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to append record due to %d", rc ) ;
            goto error_rollbackindex ;
         }
         DMS_RECORD_SETSTATE ( deletedRecordPtr, DMS_RECORD_FLAG_OVERFLOWT ) ;
         DMS_RECORD_SETSTATE ( recordPtr, DMS_RECORD_FLAG_OVERFLOWF ) ;
         DMS_RECORD_SETOVF ( recordPtr, foundDeletedID ) ;
         if ( ovfRID.isValid() )
         {
            _extentRemoveRecord( context->mb(), ovfRID, 0, cb ) ;
         }
         _markDirty ( foundDeletedID._extent ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA__EXTENTUPDATERECORD, rc ) ;
      return rc ;
   error :
      goto done ;
   error_rollbackindex :
      BSONObj oriObj( (CHAR*)recordDataPtr ) ;
      BSONObj newObj( (CHAR*)ptr ) ;
      INT32 rc1 = _pIdxSU->indexesUpdate( context, extLID, newObj, oriObj,
                                          recordID, cb, TRUE ) ;
      if ( rc1 )
      {
         PD_LOG ( PDERROR, "Failed to rollback update due to rc %d", rc1 ) ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__DMSSTORAGEDATA_FETCH, "_dmsStorageData::fetch" )
   INT32 _dmsStorageData::fetch( dmsMBContext *context,
                                 const dmsRecordID &recordID,
                                 BSONObj &dataRecord,
                                 pmdEDUCB * cb,
                                 BOOLEAN dataOwned )
   {
      INT32 rc                     = SDB_OK ;
      ossValuePtr extentPtr        = 0 ;
      dmsExtent *extent            = NULL ;
      ossValuePtr recordPtr        = 0 ;
      CHAR flag                    = 0 ;
      monAppCB * pMonAppCB         = cb ? cb->getMonAppCB() : NULL ;

      PD_TRACE_ENTRY ( SDB__DMSSTORAGEDATA_FETCH ) ;

      extentPtr = extentAddr ( recordID._extent ) ;
      if ( ! extentPtr )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      extent = (dmsExtent*)extentPtr ;

      if ( !context->isMBLock() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Caller must hold mb lock[%s]",
                 context->toString().c_str() ) ;
         goto error ;
      }

#ifdef _DEBUG
      if ( !dmsAccessAndFlagCompatiblity ( context->mb()->_flag,
                                           DMS_ACCESS_TYPE_FETCH ) )
      {
         PD_LOG ( PDERROR, "Incompatible collection mode: %d",
                  context->mb()->_flag ) ;
         rc = SDB_DMS_INCOMPATIBLE_MODE ;
         goto error ;
      }

      if ( !extent->validate( context->mbID()) )
      {
         PD_LOG ( PDERROR, "Invalid extent[%d]", recordID._extent ) ;
         rc = SDB_SYS ;
         goto error ;
      }
#endif // _DEBUG

      recordPtr = extentPtr + recordID._offset ;

      flag = DMS_RECORD_GETSTATE( recordPtr ) ;
      if ( DMS_RECORD_FLAG_DELETED == flag )
      {
         rc = SDB_DMS_NOTEXIST ;
         goto error ;
      }
      else if ( OSS_BIT_TEST( DMS_RECORD_FLAG_DELETING,
                              DMS_RECORD_GETATTR(recordPtr) ) )
      {
         rc = SDB_DMS_DELETING ;
         goto error ;
      }

#ifdef _DEBUG
      if (  DMS_RECORD_FLAG_NORMAL != flag &&
            DMS_RECORD_FLAG_OVERFLOWF != flag )
      {
         PD_LOG ( PDERROR, "Record[%d:%d] flag[%d] error", recordID._extent,
                  recordID._offset, flag ) ;
         rc = SDB_SYS ;
         goto error ;
      }
#endif //_DEBUG

      if ( DMS_RECORD_FLAG_OVERFLOWF == flag )
      {
         dmsRecordID ovfRID = DMS_RECORD_GETOVF ( recordPtr ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_READ, 1 ) ;
         recordPtr = extentAddr( ovfRID._extent ) + ovfRID._offset ;
      }

      try
      {
         ossValuePtr fetchedRecord = 0 ;
         DMS_RECORD_EXTRACTDATA ( recordPtr, fetchedRecord ) ;
         BSONObj obj( (CHAR*)fetchedRecord ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_DATA_READ, 1 ) ;
         DMS_MON_OP_COUNT_INC( pMonAppCB, MON_READ, 1 ) ;
         if ( dataOwned )
         {
            dataRecord = obj.getOwned() ;
         }
         else
         {
            dataRecord = obj ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to create BSON object: %s",
                  e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__DMSSTORAGEDATA_FETCH, rc ) ;
      return rc ;
   error :
      goto done ;
   }

}


