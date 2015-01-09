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

   Source File Name = rtnReorg.cpp

   Descriptive Name = Runtime Reorg

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime code for collection
   reorgnization.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtn.hpp"
#include "dmsStorageUnit.hpp"
#include "rtnAPM.hpp"
#include "rtnIXScanner.hpp"
#include "dmsReorgUnit.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

using namespace bson ;

namespace engine
{

   #define RTN_REORG_SUFFIX ".REORG"

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREORGOCB, "rtnReorgOfflineCopyBack" )
   INT32 rtnReorgOfflineCopyBack ( dmsStorageUnit *su,
                                   dmsReorgUnit *ru,
                                   dmsMBContext *mbContext )
   {
      INT32 rc            = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNREORGOCB ) ;

      CHAR *headBuffer    = NULL ;
      CHAR *blockBuffer   = NULL ;
      INT32 blockBuffSize = 0 ;
      INT32 headSize      = 0 ;
      SINT32 blockSize    = 0 ;
      SDB_ASSERT ( su && ru, "SU and RU can't be NULL" ) ;

      ru->reset () ;
      headSize = ru->getHeadSize () ;
      headBuffer = ( CHAR* ) SDB_OSS_MALLOC ( headSize ) ;
      if ( !headBuffer )
      {
         PD_LOG ( PDERROR, "Failed to allocate memory for header" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = ru->exportHead ( headBuffer ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to export head, rc: %d", rc ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = ru->validateHeadBuffer ( headBuffer ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to validate head, rc: %d", rc ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      while ( TRUE )
      {
         rc = ru->getNextExtentSize ( blockSize ) ;
         if ( rc )
         {
            if ( SDB_EOF == rc )
            {
               rc = SDB_OK ;
               break ;
            }
            PD_LOG ( PDERROR, "Failed to get next extent size, rc = %d", rc ) ;
            goto error ;
         }

         if ( blockBuffSize < blockSize )
         {
            if ( blockBuffer )
            {
               SDB_OSS_FREE( blockBuffer ) ;
               blockBuffer = NULL ;
               blockBuffSize = 0 ;
            }
            blockBuffer = ( CHAR* )SDB_OSS_MALLOC ( blockSize ) ;
            if ( !blockBuffer )
            {
               PD_LOG ( PDERROR, "Failed to allocate memory for block" ) ;
               rc = SDB_OOM ;
               goto error ;
            }
            blockBuffSize = blockSize ;
         }

         rc = ru->exportExtent ( blockBuffer ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to export extent, rc = %d", rc ) ;
            goto error ;
         }
         rc = su->loadExtent ( mbContext, blockBuffer,
                               blockSize / su->getPageSize() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed load extent into DMS, rc = %d", rc ) ;
            goto error ;
         }
      }

   done :
      if ( headBuffer )
      {
         SDB_OSS_FREE ( headBuffer ) ;
         headBuffer = NULL ;
      }
      if ( blockBuffer )
      {
         SDB_OSS_FREE ( blockBuffer ) ;
         blockBuffer = NULL ;
         blockBuffSize = 0 ;
      }
      PD_TRACE_EXITRC ( SDB_RTNREORGOCB, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREORGRECOVER1, "rtnReorgRecover" )
   INT32 rtnReorgRecover ( const CHAR *pCollectionFullName,
                           const CHAR *pCollectionName,
                           dmsMBContext *mbContext,
                           dmsStorageUnit *su,
                           pmdEDUCB *cb,
                           SDB_RTNCB *rtnCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNREORGRECOVER1 ) ;

      SDB_ASSERT ( pCollectionFullName, "collection full name can't be null" ) ;
      SDB_ASSERT ( pCollectionName, "collection name can't be null" ) ;
      SDB_ASSERT ( su, "su can't be NULL" ) ;
      SDB_ASSERT ( cb, "cb can't be NULL" ) ;

      pmdKRCB *krcb = pmdGetKRCB () ;
      BOOLEAN ruExist = TRUE ;
      UINT16 flag = 0 ;
      CHAR fullFilePath [ OSS_MAX_PATHSIZE + 1 ] = {0} ;
      const CHAR *dbpath = krcb->getOptionCB()->getDbPath() ;

      if ( !mbContext->isMBLock( EXCLUSIVE ) )
      {
         PD_LOG( PDERROR, "Caller must hold collection EXCLUSIVE lock[%s]",
                 mbContext->toString().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( ossStrlen ( dbpath ) + 1 +
           ossStrlen ( pCollectionFullName ) +
           ossStrlen ( RTN_REORG_SUFFIX ) >
           OSS_MAX_PATHSIZE )
      {
         PD_LOG ( PDERROR, "Can't build reorg file path size it's too long: "
                  "%s%s%s%s", dbpath, OSS_FILE_SEP,
                  pCollectionFullName, RTN_REORG_SUFFIX ) ;
         goto error ;
      }

      utilBuildFullPath( dbpath, pCollectionFullName,
                         OSS_MAX_PATHSIZE, fullFilePath ) ;
      ossStrncat ( fullFilePath, RTN_REORG_SUFFIX,
                   ossStrlen (RTN_REORG_SUFFIX) ) ;

      flag = mbContext->mb()->_flag ;

      if ( (flag & DMS_MB_OPR_TYPE_MASK) == DMS_MB_FLAG_OFFLINE_REORG )
      {
         dmsReorgUnit ru ( fullFilePath, su->getPageSize() ) ;
         rc = ru.open ( FALSE ) ;
         if ( rc )
         {
            ruExist = FALSE ;
            if ( ( ( flag & DMS_MB_OPR_PHASE_MASK ) !=
                 DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY ) &&
                 ( ( flag & DMS_MB_OPR_PHASE_MASK ) !=
                 DMS_MB_FLAG_OFFLINE_REORG_REBUILD ) )
            {
               PD_LOG ( PDERROR, "Can't open temp reorg file, the collection "
                        "has to be rebuilt, rc = %d", rc ) ;
               goto error ;
            }
         }

         switch ( flag & DMS_MB_OPR_PHASE_MASK )
         {
         case DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY:
            PD_LOG ( PDEVENT, "Start reorg recovering from shadow copy phase" ) ;
            goto cleanup ;

         case DMS_MB_FLAG_OFFLINE_REORG_COPY_BACK:
            PD_LOG ( PDEVENT, "Start reorg recovering from copy_back phase" ) ;
            goto truncate ;

         case DMS_MB_FLAG_OFFLINE_REORG_TRUNCATE:
            PD_LOG ( PDEVENT, "Start reorg recovering from truncate phase" ) ;

   truncate:
            DMS_SET_MB_OFFLINE_REORG_TRUNCATE ( flag ) ;
            mbContext->mb()->_flag = flag ;

            rc = su->data()->truncateCollection ( pCollectionName, cb, NULL,
                                                  TRUE, mbContext,
                                                  FALSE ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to truncate collection, rc = %d",
                        rc ) ;
               goto error ;
            }

            DMS_SET_MB_OFFLINE_REORG_COPY_BACK ( flag ) ;
            mbContext->mb()->_flag = flag ;

            rc = rtnReorgOfflineCopyBack ( su, &ru, mbContext ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to perform copyback for offline "
                        "reorg, rc = %d", rc ) ;
               goto error ;
            }
            goto rebuild ;

         case DMS_MB_FLAG_OFFLINE_REORG_REBUILD:
            PD_LOG ( PDEVENT, "Start reorg recovering from rebuild phase" ) ;

   rebuild:
            DMS_SET_MB_OFFLINE_REORG_REBUILD ( flag ) ;
            mbContext->mb()->_flag = flag ;

            rc = su->index()->rebuildIndexes ( mbContext, cb ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to rebuild indexes, rc = %d", rc ) ;
               goto error ;
            }
            rc = mbContext->mbLock( EXCLUSIVE ) ;
            if ( rc )
            {
               PD_LOG( PDERROR, "dms mb context lock failed, rc: %d", rc ) ;
               goto error ;
            }

   cleanup:
            DMS_SET_MB_NORMAL( flag ) ;
            mbContext->mb()->_flag = flag ;

            if ( ruExist )
            {
               INT32 tempRC = SDB_OK ;
               tempRC = ru.cleanup () ;
               if ( tempRC )
               {
                  PD_LOG ( PDERROR, "Failed to clean up temp file, rc = %d, "
                           "manual clean up is required", rc ) ;
                  goto error ;
               }
            }
         break ;

         default:
            PD_LOG ( PDERROR, "Unexpected collection flag: %d", flag ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
      else if ( (flag & DMS_MB_OPR_TYPE_MASK) == DMS_MB_FLAG_ONLINE_REORG )
      {
         PD_LOG ( PDERROR, "Online reorg recover is not supported yet" ) ;
         rc = SDB_OPTION_NOT_SUPPORT ;
         goto error ;
      }
      else
      {
         PD_LOG ( PDWARNING, "Collection is not in reorg status" ) ;
         rc = SDB_DMS_NOT_IN_REORG ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNREORGRECOVER1, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREORGOFFLINE1, "rtnReorgOffline" )
   INT32 rtnReorgOffline ( const CHAR *pCollectionFullName,
                           dmsStorageUnit *su,
                           dmsMBContext *mbContext,
                           rtnContextData *context,
                           pmdEDUCB *cb,
                           SDB_RTNCB *rtnCB,
                           BOOLEAN ignoreError )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNREORGOFFLINE1 ) ;

      SDB_ASSERT ( pCollectionFullName, "collection name can't be null" ) ;
      SDB_ASSERT ( su, "su can't be NULL" ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;
      SDB_ASSERT ( cb, "cb can't be NULL" ) ;
      SDB_ASSERT ( mbContext, "mb context can't be NULL" ) ;

      BSONObj dummyObj ;
      pmdKRCB *krcb = pmdGetKRCB () ;
      UINT16 flag = 0 ;
      UINT32 attributes = 0 ;
      CHAR fullFilePath [ OSS_MAX_PATHSIZE + 1 ] = {0} ;
      const CHAR *dbpath = krcb->getOptionCB()->getDbPath() ;

      if ( ossStrlen ( dbpath ) + 1 +
           ossStrlen ( pCollectionFullName ) +
           ossStrlen ( RTN_REORG_SUFFIX ) >
           OSS_MAX_PATHSIZE )
      {
         PD_LOG ( PDERROR, "Can't build reorg file path size it's too long: "
                  "%s%s%s%s", dbpath, OSS_FILE_SEP, pCollectionFullName,
                  RTN_REORG_SUFFIX ) ;
         goto error ;
      }

      if ( !mbContext->isMBLock( EXCLUSIVE ) )
      {
         PD_LOG( PDERROR, "Caller must hold collection EXCLUSIVE lock[%s]",
                 mbContext->toString().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      utilBuildFullPath( dbpath, pCollectionFullName,
                         OSS_MAX_PATHSIZE, fullFilePath ) ;
      ossStrncat ( fullFilePath, RTN_REORG_SUFFIX,
                   ossStrlen (RTN_REORG_SUFFIX) ) ;

      flag = mbContext->mb()->_flag ;
      attributes = mbContext->mb()->_attributes ;

      if ( !DMS_IS_MB_NORMAL ( flag ) )
      {
         PD_LOG ( PDERROR, "Failed to start offline reorg: Collection status "
                  "is not normal: %d", flag ) ;
         rc = SDB_DMS_INVALID_COLLECTION_S ;
         goto error ;
      }

      PD_LOG ( PDEVENT, "Start offline reorg, use temp file %s",
               fullFilePath ) ;

      /*******************************************************************
       *           MAIN REORG BODY STARTS
       *******************************************************************/
      {
         rtnContextBuf buffObj ;
         dmsReorgUnit ru ( fullFilePath, su->getPageSize() ) ;
         rc = ru.open ( TRUE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to create and initialize new reorg temp "
                     "file, rc: %d", rc ) ;
            goto error ;
         }

         /******************************************************************
          *       SHADOW COPY PHASE STARTS
          ******************************************************************/
         DMS_SET_MB_OFFLINE_REORG_SHADOW_COPY ( flag ) ;
         PD_LOG ( PDEVENT, "Shadow copy phase starts" ) ;
         mbContext->mb()->_flag = flag ;

         while ( TRUE )
         {
            rc = context->getMore( 1, buffObj, cb ) ;
            if ( rc )
            {
               if ( SDB_DMS_EOC != rc )
               {
                  PD_LOG ( PDERROR, "Error detected during fetch, rc = %d",
                           rc ) ;
                  if ( !ignoreError )
                  {
                     goto error_shadow_copy ;
                  }
               }
               rc = SDB_OK ;
               break ;
            }
            try
            {
               BSONObj dataRecord ( buffObj.data() ) ;
               rc = ru.insertRecord ( dataRecord, cb, attributes ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to insert into temp file, rc = %d",
                           rc ) ;
                  goto error_shadow_copy ;
               }
            }
            catch ( std::exception &e )
            {
               PD_LOG ( PDERROR, "Failed to build bson record: %s",
                        e.what() ) ;
               goto error_shadow_copy ;
            }
         } // while ( TRUE )

         rc = ru.flush () ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to flush into reorg unit, rc = %d",
                     rc ) ;
            goto error_shadow_copy ;
         }

         /******************************************************************
          *       TRUNCATE PHASE STARTS
          ******************************************************************/
         DMS_SET_MB_OFFLINE_REORG_TRUNCATE ( flag ) ;
         PD_LOG ( PDEVENT, "Truncate phase starts" ) ;
         mbContext->mb()->_flag = flag ;

         rc = su->data()->truncateCollection ( mbContext->mb()->_collectionName,
                                               cb, NULL, TRUE, mbContext,
                                               FALSE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to truncate collection, rc = %d", rc ) ;
            goto error_truncate ;
         }

         /******************************************************************
          *       COPY_BACK PHASE STARTS
          ******************************************************************/
         DMS_SET_MB_OFFLINE_REORG_COPY_BACK ( flag ) ;
         PD_LOG ( PDEVENT, "Copy-back phase starts" ) ;
         mbContext->mb()->_flag = flag ;

         rc = rtnReorgOfflineCopyBack ( su, &ru, mbContext ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to perform copyback for offline reorg, "
                     "rc = %d", rc ) ;
            goto error_copy_back ;
         }

         /******************************************************************
          *       REBUILD PHASE STARTS
          ******************************************************************/
         DMS_SET_MB_OFFLINE_REORG_REBUILD ( flag ) ;
         PD_LOG ( PDEVENT, "Rebuild phase starts" ) ;
         mbContext->mb()->_flag = flag ;

         rc = su->index()->rebuildIndexes ( mbContext, cb ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rebuild indexes, rc = %d", rc ) ;
            goto error_rebuild ;
         }
         rc = mbContext->mbLock( EXCLUSIVE ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "dms mb context lock failed, rc: %d", rc ) ;
            goto error_rebuild ;
         }

      cleanup :
         DMS_SET_MB_NORMAL(flag) ;
         PD_LOG ( PDEVENT, "Clean-up phase starts" ) ;
         mbContext->mb()->_flag = flag ;

         {
            INT32 tempRC = SDB_OK ;
            tempRC = ru.cleanup () ;
            if ( tempRC )
            {
               PD_LOG ( PDERROR, "Failed to clean up temp file, rc = %d, "
                        "manual clean up is required", rc ) ;
               goto error ;
            }
         }
         goto done ;

      error_shadow_copy :
         goto cleanup ;
      error_truncate :
         goto error ;
      error_copy_back :
         goto error ;
      error_rebuild :
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNREORGOFFLINE1, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREORGRECOVER2, "rtnReorgRecover" )
   INT32 rtnReorgRecover ( const CHAR *pCollectionName,
                           pmdEDUCB *cb,
                           SDB_DMSCB *dmsCB,
                           SDB_RTNCB *rtnCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNREORGRECOVER2 ) ;

      SDB_ASSERT ( pCollectionName, "collection name can't be NULL" ) ;
      SDB_ASSERT ( cb, "educb can't be NULL" ) ;
      SDB_ASSERT ( rtnCB, "rtnCB can't be NULL" ) ;

      BSONObj dummyObj ;
      dmsStorageUnitID suID = DMS_INVALID_CS ;
      dmsStorageUnit *su = NULL ;
      const CHAR *pCollectionShortName = NULL ;
      dmsMBContext *mbContext = NULL ;

      if ( ossStrlen ( pCollectionName ) > DMS_COLLECTION_FULL_NAME_SZ )
      {
         PD_LOG ( PDERROR, "Collection name is too long: %s",
                  pCollectionName ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = rtnResolveCollectionNameAndLock ( pCollectionName, dmsCB, &su,
                                             &pCollectionShortName, suID ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to resolve collection name %s",
                  pCollectionName ) ;
         goto error ;
      }

      rc = su->data()->getMBContext( &mbContext, pCollectionShortName,
                                     EXCLUSIVE ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to lock collection %s, rc = %d",
                 pCollectionName, rc ) ;
         goto error ;
      }

      rc = rtnReorgRecover ( pCollectionName, pCollectionShortName,
                             mbContext, su, cb, rtnCB ) ;


   done :
      if ( su && mbContext )
      {
         su->data()->releaseMBContext( mbContext ) ;
      }
      if ( DMS_INVALID_CS != suID )
      {
         dmsCB->suUnlock ( suID ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNREORGRECOVER2, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREORGOFFLINE2, "rtnReorgOffline" )
   INT32 rtnReorgOffline ( const CHAR *pCollectionName,
                           const BSONObj &hint,
                           pmdEDUCB *cb,
                           SDB_DMSCB *dmsCB,
                           SDB_RTNCB *rtnCB,
                           BOOLEAN ignoreError )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNREORGOFFLINE2 ) ;

      BSONObj dummyObj ;
      SINT64 contextID           = -1 ;
      rtnContextData *context    = NULL ;

      SDB_ASSERT ( pCollectionName, "collection name can't be NULL" ) ;
      SDB_ASSERT ( cb, "educb can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( rtnCB, "rtnCB can't be NULL" ) ;

      if ( ossStrlen ( pCollectionName ) > DMS_COLLECTION_FULL_NAME_SZ )
      {
         PD_LOG ( PDERROR, "Collection name is too long: %s",
                  pCollectionName ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = rtnQuery ( pCollectionName, dummyObj, dummyObj, dummyObj,
                      hint, 0, cb, 0, -1, dmsCB, rtnCB, contextID,
                      (rtnContextBase**)&context ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            PD_LOG ( PDERROR, "Empty collection is detected, reorg is skipped" ) ;
            rc = SDB_OK ;
            goto done ;
         }
         PD_LOG ( PDERROR, "Failed to run query, rc = %d", rc ) ;
         goto error ;
      }

      rc = context->getMBContext()->mbLock( EXCLUSIVE ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to lock collection %s, rc = %d",
                  pCollectionName, rc ) ;
         goto error ;
      }

      rc = rtnReorgOffline ( pCollectionName, context->getSU(),
                             context->getMBContext(), context,
                             cb, rtnCB, ignoreError ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to do offline reorg, rc = %d", rc ) ;
         goto error ;
      }

   done :
      if ( -1 != contextID )
      {
         rtnCB->contextDelete( contextID, cb ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNREORGOFFLINE2, rc ) ;
      return rc ;
   error :
      goto done ;
   }

}

