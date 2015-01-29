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

   Source File Name = rtn.cpp

   Descriptive Name = Runtime

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime helper functions.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include "rtn.hpp"
#include "dms.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "dmsStorageUnit.hpp"
#include "dmsScanner.hpp"
#include "rtnIXScanner.hpp"
#include "boost/filesystem.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace fs = boost::filesystem ;
namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNGETINTELE, "rtnGetIntElement" )
   INT32 rtnGetIntElement ( const BSONObj &obj, const CHAR *fieldName,
                            INT32 &value )
   {
      SINT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNGETINTELE );
      SDB_ASSERT ( fieldName, "field name can't be NULL" ) ;
      BSONElement ele = obj.getField ( fieldName ) ;
      PD_CHECK ( !ele.eoo(), SDB_FIELD_NOT_EXIST, error, PDDEBUG,
                 "Can't locate field '%s': %s",
                 fieldName,
                 obj.toString().c_str() ) ;
      PD_CHECK ( ele.isNumber(), SDB_INVALIDARG, error, PDDEBUG,
                 "Unexpected field type : %s, supposed to be Integer",
                 obj.toString().c_str()) ;
      value = ele.numberInt() ;
   done :
      PD_TRACE_EXITRC ( SDB_RTNGETINTELE, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNGETSTRELE, "rtnGetStringElement" )
   INT32 rtnGetStringElement ( const BSONObj &obj, const CHAR *fieldName,
                               const CHAR **value )
   {
      SINT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNGETSTRELE );
      SDB_ASSERT ( fieldName && value, "field name and value can't be NULL" ) ;
      BSONElement ele = obj.getField ( fieldName ) ;
      PD_CHECK ( !ele.eoo(), SDB_FIELD_NOT_EXIST, error, PDDEBUG,
                 "Can't locate field '%s': %s",
                 fieldName,
                 obj.toString().c_str() ) ;
      PD_CHECK ( String == ele.type(), SDB_INVALIDARG, error, PDDEBUG,
                 "Unexpected field type : %s, supposed to be String",
                 obj.toString().c_str()) ;
      *value = ele.valuestr() ;
   done :
      PD_TRACE_EXITRC ( SDB_RTNGETSTRELE, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 rtnGetSTDStringElement ( const BSONObj &obj, const CHAR *fieldName,
                                  string &value )
   {
      SINT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNGETSTRELE ) ;
      SDB_ASSERT ( fieldName, "field name can't be NULL" ) ;
      BSONElement ele = obj.getField ( fieldName ) ;
      PD_CHECK ( !ele.eoo(), SDB_FIELD_NOT_EXIST, error, PDDEBUG,
                 "Can't locate field '%s': %s",
                 fieldName,
                 obj.toString().c_str() ) ;
      PD_CHECK ( String == ele.type(), SDB_INVALIDARG, error, PDDEBUG,
                 "Unexpected field type : %s, supposed to be String",
                 obj.toString().c_str()) ;
      value = ele.valuestr() ;
   done :
      PD_TRACE_EXITRC ( SDB_RTNGETSTRELE, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNGETOBJELE, "rtnGetObjElement" )
   INT32 rtnGetObjElement ( const BSONObj &obj, const CHAR *fieldName,
                            BSONObj &value )
   {
      SINT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNGETOBJELE );
      SDB_ASSERT ( fieldName , "field name can't be NULL" ) ;
      BSONElement ele = obj.getField ( fieldName ) ;
      PD_CHECK ( !ele.eoo(), SDB_FIELD_NOT_EXIST, error, PDDEBUG,
                 "Can't locate field '%s': %s",
                 fieldName,
                 obj.toString().c_str() ) ;
      PD_CHECK ( Object == ele.type(), SDB_INVALIDARG, error, PDDEBUG,
                 "Unexpected field type : %s, supposed to be Object",
                 obj.toString().c_str()) ;
      value = ele.embeddedObject() ;
   done :
      PD_TRACE_EXITRC ( SDB_RTNGETOBJELE, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNGETBOOLELE, "rtnGetBooleanElement" )
   INT32 rtnGetBooleanElement ( const BSONObj &obj, const CHAR *fieldName,
                                BOOLEAN &value )
   {
      SINT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNGETBOOLELE );
      SDB_ASSERT ( fieldName , "field name can't be NULL" ) ;
      BSONElement ele = obj.getField ( fieldName ) ;
      PD_CHECK ( !ele.eoo(), SDB_FIELD_NOT_EXIST, error, PDDEBUG,
                 "Can't locate field '%s': %s",
                 fieldName,
                 obj.toString().c_str() ) ;
      PD_CHECK ( Bool == ele.type(), SDB_INVALIDARG, error, PDDEBUG,
                 "Unexpected field type : %s, supposed to be Bool",
                 obj.toString().c_str()) ;
      value = ele.boolean() ;
   done :
      PD_TRACE_EXITRC ( SDB_RTNGETBOOLELE, rc );
      return rc ;
   error :
      goto done ;
   }

   BSONObj rtnUniqueKeyNameObj( const BSONObj & obj )
   {
      CHAR szTmp[ 5 ] = { 0 } ;
      UINT32 i = 0 ;
      BSONObjBuilder builder ;
      BSONObjIterator it ( obj ) ;
      while ( it.more() )
      {
         ossSnprintf( szTmp, 4, "%d", i++ ) ;
         builder.appendAs( it.next(), szTmp ) ;
      }
      return builder.obj() ;
   }

   BSONObj rtnNullKeyNameObj( const BSONObj &obj )
   {
      BSONObjBuilder builder ;
      BSONObjIterator it ( obj ) ;
      while ( it.more() )
      {
         builder.appendAs( it.next(), "" ) ;
      }
      return builder.obj() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREALLOCBUFF, "rtnReallocBuffer" )
   INT32 rtnReallocBuffer ( CHAR **ppBuffer, INT32 *bufferSize,
                            INT32 newLength, INT32 alignmentSize )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNREALLOCBUFF );
      if ( newLength > *bufferSize )
      {
         CHAR *pOrgBuff = *ppBuffer ;
         newLength = ossRoundUpToMultipleX ( newLength, alignmentSize ) ;
         PD_CHECK ( newLength >= 0, SDB_INVALIDARG, error, PDERROR,
                    "new buffer overflow" ) ;
         *ppBuffer = (CHAR*)SDB_OSS_REALLOC ( *ppBuffer,
                                              sizeof(CHAR)*(newLength) ) ;
         if ( !*ppBuffer )
         {
            PD_LOG( PDERROR, "Failed to realloc %d bytes memory", newLength ) ;
            *ppBuffer = pOrgBuff ;
            rc = SDB_OOM ;
            goto error ;
         }
         *bufferSize = newLength ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNREALLOCBUFF, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNVERIFYCSFN, "rtnVerifyCollectionSpaceFileName" )
   BOOLEAN rtnVerifyCollectionSpaceFileName ( const CHAR *pFileName,
                                              CHAR *pSUName,
                                              UINT32 bufferSize,
                                              UINT32 &sequence,
                                              const CHAR *extFilter )
   {
      PD_TRACE_ENTRY ( SDB_RTNVERIFYCSFN ) ;

      BOOLEAN ret = FALSE ;
      UINT32 size = 0 ;
      const CHAR *pDot = ossStrchr ( pFileName, '.' ) ;
      const CHAR *pDotr = ossStrrchr ( pFileName, '.' ) ;

      if ( !pDot || !pDotr || pDotr - pDot <= 1 || pDot == pFileName ||
           *(pDotr + 1) == 0 || ossStrchr( pDot + 1, '.' ) != pDotr )
      {
         goto done ;
      }

      if ( extFilter && 0 != ossStrcmp( pDotr + 1, extFilter ) )
      {
         goto done ;
      }

      {
         sequence = 0 ;
         const CHAR *pSeqPos = pDot + 1 ;
         while ( pSeqPos < pDotr )
         {
            if ( *pSeqPos >= '0' && *pSeqPos <= '9' )
            {
               sequence = 10 * sequence + ( *pSeqPos - '0' ) ;
            }
            else
            {
               goto done ;
            }
            ++pSeqPos ;
         }
      }

      size = pDot - pFileName ;
      if ( size > bufferSize )
      {
         goto done ;
      }

      if ( pSUName )
      {
         ossStrncpy ( pSUName, pFileName, size ) ;
         pSUName[size] = 0 ;
      }

      ret = TRUE ;

   done :
      PD_TRACE1 ( SDB_RTNVERIFYCSFN, PD_PACK_INT(ret) );
      PD_TRACE_EXIT ( SDB_RTNVERIFYCSFN );
      return ret ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOADCS, "rtnLoadCollectionSpace" )
   INT32 rtnLoadCollectionSpace ( const CHAR *pCSName,
                                  const CHAR *dataPath,
                                  const CHAR *indexPath,
                                  const CHAR *lobPath,
                                  SDB_DMSCB *dmsCB,
                                  BOOLEAN checkOnly )
   {
      SDB_ASSERT ( pCSName, "pCSName can't be NULL" ) ;
      SDB_ASSERT ( dataPath, "data path can't be NULL" ) ;
      SDB_ASSERT ( indexPath, "index path can't be NULL" ) ;
      SDB_ASSERT ( lobPath, "lob path can't be NULL" ) ;

      INT32 rc                                 = SDB_DMS_CS_NOTEXIST ;
      PD_TRACE_ENTRY ( SDB_RTNLOADCS );
      CHAR csName[ DMS_SU_NAME_SZ + 1 ]        = {0} ;
      UINT32 sequence                          = 0 ;
      dmsStorageUnit *storageUnit              = NULL ;

      try
      {
         fs::path dbDir ( dataPath ) ;
         fs::directory_iterator end_iter ;

         if ( fs::exists(dbDir) && fs::is_directory(dbDir) )
         {
            for ( fs::directory_iterator dir_iter(dbDir);
                  dir_iter != end_iter; ++dir_iter )
            {
               if ( fs::is_regular_file( dir_iter->status() ) )
               {
                  const std::string fileName =
                     dir_iter->path().filename().string() ;
                  const CHAR *pFileName = fileName.c_str() ;

                  if ( rtnVerifyCollectionSpaceFileName( pFileName, csName,
                                                         DMS_SU_NAME_SZ,
                                                         sequence ) &&
                       ossStrncmp ( pCSName, csName, DMS_SU_NAME_SZ) == 0 )
                  {
                     PD_LOG ( PDDEBUG, "Found Filename %s in path %s",
                              pFileName, dataPath) ;

                     if ( !checkOnly )
                     {
                        storageUnit = SDB_OSS_NEW dmsStorageUnit ( csName,
                                                                   sequence ) ;
                        if ( !storageUnit )
                        {
                           PD_LOG_MSG ( PDERROR, "Failed to allocate "
                                        "dmsStorageUnit for %s",
                                        dir_iter->path().string().c_str() ) ;
                           rc = SDB_OOM ;
                           goto error ;
                        }
                        rc = storageUnit->open ( dataPath, indexPath, lobPath, FALSE ) ;
                        if ( rc )
                        {
                           SDB_OSS_DEL storageUnit ;
                           storageUnit = NULL ;
                           PD_LOG ( PDWARNING, "Failed to open storage unit %s",
                                    dir_iter->path().string().c_str() ) ;
                           continue ;
                        }

                        rc = dmsCB->addCollectionSpace ( csName, sequence,
                                                         storageUnit, NULL,
                                                         NULL ) ;
                        if ( rc )
                        {
                           SDB_OSS_DEL storageUnit ;
                           storageUnit = NULL ;
                           if ( SDB_DMS_CS_EXIST == rc )
                           {
                              PD_LOG ( PDWARNING, "Failed to add collection "
                                       "space[%s] because it's already exist",
                                       csName ) ;
                           }
                           else
                           {
                              PD_LOG ( PDWARNING, "Failed to add collection "
                                       "space[%s], rc = %d", csName, rc ) ;
                           }
                           continue ;
                        }
                     }

                     rc = SDB_OK ;
                     goto done ;
                  } // if ( rtnVerifyCollectionSpaceFileName
               } //  if ( fs::is_regular_file(dir_iter->status()))
            } //for ( fs::directory_iterator dir_iter(dbDir)
         }
         else
         {
            PD_LOG_MSG ( PDERROR, "Given path %s is not a directory or not "
                         "exist", dataPath ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR, "Failed to iterate directory %s: %s",
                       dataPath, e.what() ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNLOADCS, rc );
      return rc ;
   error :
      if ( storageUnit )
      {
         SDB_OSS_DEL storageUnit ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOADCSS, "rtnLoadCollectionSpaces" )
   INT32 rtnLoadCollectionSpaces ( const CHAR *dataPath,
                                   const CHAR *indexPath,
                                   const CHAR *lobPath,
                                   SDB_DMSCB *dmsCB )
   {
      INT32 rc                                 = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNLOADCSS );
      CHAR csName [ DMS_SU_FILENAME_SZ + 1 ]   = {0} ;
      UINT32 sequence                          = 0 ;
      dmsStorageUnit *storageUnit              = NULL ;

      SDB_ASSERT ( dataPath, "data path can't be NULL" ) ;
      SDB_ASSERT ( indexPath, "index path can't be NULL" ) ;
      SDB_ASSERT ( lobPath, "lob path can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;

      try
      {
         fs::path dbDir ( dataPath ) ;
         fs::directory_iterator end_iter ;
         if ( fs::exists(dbDir) && fs::is_directory(dbDir) )
         {
            for ( fs::directory_iterator dir_iter(dbDir);
                  dir_iter != end_iter; ++dir_iter )
            {
               if ( fs::is_regular_file(dir_iter->status()))
               {
                  const std::string fileName =
                        dir_iter->path().filename().string() ;
                  const CHAR *pFileName = fileName.c_str() ;

                  if ( rtnVerifyCollectionSpaceFileName( pFileName, csName,
                                                         DMS_SU_FILENAME_SZ,
                                                         sequence ) )
                  {
                     PD_LOG ( PDDEBUG, "Candidate Filename %s", pFileName ) ;
                     storageUnit = SDB_OSS_NEW dmsStorageUnit ( csName,
                                                                sequence ) ;
                     PD_CHECK ( storageUnit, SDB_OOM, error, PDERROR,
                                "Failed to allocate dmsStorageUnit for %s",
                                dir_iter->path().string().c_str() ) ;

                     rc = storageUnit->open ( dataPath,
                                              indexPath,
                                              lobPath,
                                              FALSE ) ;
                     if ( rc )
                     {
                        SDB_OSS_DEL storageUnit ;
                        storageUnit = NULL ;
                        PD_LOG ( PDWARNING, "Failed to open storage unit %s",
                                 dir_iter->path().string().c_str() ) ;
                        rc = SDB_OK ;
                        continue ;
                     }

                     rc = dmsCB->addCollectionSpace ( csName, sequence,
                                                      storageUnit, NULL,
                                                      NULL ) ;
                     if ( rc )
                     {
                        SDB_OSS_DEL storageUnit ;
                        storageUnit = NULL ;
                        if ( SDB_DMS_CS_EXIST == rc )
                        {
                           PD_LOG ( PDWARNING, "Failed to add collection since "
                                    "it's already exist: %s", csName ) ;
                        }
                        else
                        {
                           PD_LOG ( PDWARNING, "Failed to add collection, rc: "
                                    "%d", rc ) ;
                        }
                        rc = SDB_OK ;
                        continue ;
                     }
                  } // if ( rtnVerifyCollectionSpaceFileName
               } //  if ( fs::is_regular_file(dir_iter->status()))
            } //for ( fs::directory_iterator dir_iter(dbDir)
         } // if ( fs::exists(dbDir) && fs::is_directory(dbDir) )
         else
         {
            PD_RC_CHECK ( SDB_INVALIDARG, PDERROR, "Given path %s is not a "
                          "directory or not exist", dataPath ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR, "Failed to iterate directory %s: %s",
                       dataPath, e.what() ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNLOADCSS, rc ) ;
      return rc ;
   error :
      if ( storageUnit )
      {
         SDB_OSS_DEL storageUnit ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNDELCSCOMMAND, "rtnDelCollectionSpaceCommand" )
   INT32 rtnDelCollectionSpaceCommand ( const CHAR *pCollectionSpace,
                                        _pmdEDUCB *cb,
                                        SDB_DMSCB *dmsCB,
                                        SDB_DPSCB *dpsCB,
                                        BOOLEAN sysCall,
                                        BOOLEAN dropFile )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNDELCSCOMMAND ) ;
      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
      SINT64 contextID = -1 ;
      BOOLEAN writable = FALSE ;
      SDB_ASSERT ( pCollectionSpace, "collection space can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dms control block can't be NULL" ) ;
      UINT32 length = ossStrlen ( pCollectionSpace ) ;
      if ( length <= 0 || length > DMS_SU_NAME_SZ )
      {
         PD_LOG ( PDERROR, "Invalid length for collectionspace: %s, rc: %d",
                  pCollectionSpace, rc ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = dmsCB->writable( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Database is not writable, rc = %d", rc ) ;
      writable = TRUE ;

      if ( NULL != cb )
      {
         std::set<SINT64> contextList ;
         cb->contextCopy( contextList ) ;

         std::set<SINT64>::iterator it = contextList.begin() ;
         while ( it != contextList.end() )
         {
            contextID = *it ;
            ++it ;

            rtnContext *ctx = rtnCB->contextFind ( contextID ) ;
            if ( !ctx || NULL == ctx->getSU() )
            {
               continue ;
            }
            if ( ossStrncmp ( ctx->getSU()->CSName(),
                              pCollectionSpace, DMS_SU_NAME_SZ ) == 0 )
            {
               rtnCB->contextDelete( contextID, cb ) ;
            }
         }
      }

      if ( dropFile )
      {
         rc = dmsCB->dropCollectionSpace ( pCollectionSpace, cb, dpsCB ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to drop collectionspace %s, "
                       "rc: %d", pCollectionSpace, rc ) ;
      }
      else
      {
         rc = dmsCB->unloadCollectonSpace( pCollectionSpace, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to unload collectionspace %s, "
                      "rc: %d", pCollectionSpace, rc ) ;
      }

   done :
      if ( writable )
      {
         dmsCB->writeDown( cb ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNDELCSCOMMAND, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   INT32 rtnUnloadCollectionSpace( const CHAR * pCollectionSpace,
                                   _pmdEDUCB * cb,
                                   SDB_DMSCB * dmsCB )
   {
      INT32 rc = rtnDelCollectionSpaceCommand( pCollectionSpace, cb,
                                               dmsCB, NULL, TRUE,
                                               FALSE ) ;
      if ( SDB_OK == rc )
      {
         PD_LOG( PDEVENT, "Unload collectionspace %s succeed.",
                 pCollectionSpace ) ;
      }
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNUNLOADALLCS, "rtnUnloadCollectionSpaces" )
   INT32 rtnUnloadCollectionSpaces( _pmdEDUCB * cb, SDB_DMSCB * dmsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNUNLOADALLCS ) ;

      std::set<_monCollectionSpace> csList ;

      dmsCB->dumpInfo( csList, TRUE ) ;
      std::set<_monCollectionSpace>::const_iterator it = csList.begin() ;
      while ( it != csList.end() )
      {
         const _monCollectionSpace &cs = *it ;
         rc = rtnUnloadCollectionSpace ( cs._name, cb, dmsCB ) ;
         if ( SDB_OK != rc && SDB_DMS_CS_NOTEXIST != rc )
         {
            PD_LOG ( PDERROR, "Unload collectionspace[%s] failed[rc:%d]",
                     cs._name, rc ) ;
            break ;
         }
         ++it ;
      }

      PD_TRACE_EXITRC ( SDB_RTNUNLOADALLCS, rc );
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNFINDCL, "rtnFindCollection" )
   INT32 rtnFindCollection ( const CHAR *pCollection,
                             SDB_DMSCB *dmsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNFINDCL );
      SDB_ASSERT ( pCollection, "collection can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dms control block can't be NULL" ) ;
      dmsStorageUnit *su = NULL ;
      dmsStorageUnitID suID = DMS_INVALID_CS ;
      const CHAR *pCollectionShortName = NULL ;
      rc = rtnResolveCollectionNameAndLock ( pCollection, dmsCB, &su,
                                             &pCollectionShortName, suID ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to resolve collection name %s",
                  pCollection ) ;
         goto error ;
      }
      UINT16 cID ;
      rc = su->data()->findCollection ( pCollectionShortName, cID ) ;

   done :
      if ( DMS_INVALID_CS != suID )
         dmsCB->suUnlock ( suID ) ;
      PD_TRACE_EXITRC ( SDB_RTNFINDCL, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCSLOCK, "rtnCollectionSpaceLock" )
   INT32 rtnCollectionSpaceLock ( const CHAR *pCollectionSpaceName,
                                  SDB_DMSCB *dmsCB,
                                  BOOLEAN loadFile,
                                  dmsStorageUnit **ppsu,
                                  dmsStorageUnitID &suID,
                                  OSS_LATCH_MODE lockType,
                                  INT32 millisec )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCSLOCK );
      SDB_ASSERT ( pCollectionSpaceName, "cs name can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( ppsu, "storage unit can't be NULL" ) ;

   retry:
      rc = dmsCB->nameToSUAndLock ( pCollectionSpaceName, suID,
                                       ppsu, lockType, millisec ) ;
      if ( SDB_OK == rc )
      {
         goto done ;
      }
      else if ( SDB_DMS_CS_NOTEXIST == rc && loadFile )
      {
         PD_LOG ( PDWARNING, "Collection Space %s does not exist in dms,"
                  " load from disk", pCollectionSpaceName ) ;

         rc = rtnLoadCollectionSpace ( pCollectionSpaceName,
                                       pmdGetOptionCB()->getDbPath(),
                                       pmdGetOptionCB()->getIndexPath(),
                                       pmdGetOptionCB()->getLobPath(),
                                       dmsCB, FALSE ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Unable to load collection %s from %s",
                         pCollectionSpaceName, pmdGetOptionCB()->getDbPath() ) ;
            goto error ;
         }
         goto retry ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNCSLOCK, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNRESOLVECLNAL, "rtnResolveCollectionNameAndLock" )
   INT32 rtnResolveCollectionNameAndLock ( const CHAR *pCollectionFullName,
                                           SDB_DMSCB *dmsCB,
                                           dmsStorageUnit **ppsu,
                                           const CHAR **ppCollectionName,
                                           dmsStorageUnitID &suID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNRESOLVECLNAL );
      SDB_ASSERT ( pCollectionFullName, "collection name can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( ppsu, "storage unit can't be NULL" ) ;
      CHAR *pDot = NULL ;
      CHAR *pDot1 = NULL ;
      CHAR strCollectionFullName [ DMS_COLLECTION_SPACE_NAME_SZ +
                                   DMS_COLLECTION_NAME_SZ + 2 ] = {0} ;
      if ( ossStrlen ( pCollectionFullName ) > DMS_COLLECTION_SPACE_NAME_SZ +
           DMS_COLLECTION_NAME_SZ + 1 )
      {
         PD_LOG_MSG ( PDERROR, "Collection name is too long: %s",
                      pCollectionFullName ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossStrncpy ( strCollectionFullName, pCollectionFullName,
                   sizeof(strCollectionFullName) ) ;
      pDot = (CHAR*)ossStrchr ( strCollectionFullName, '.' ) ;
      pDot1 = (CHAR*)ossStrrchr ( strCollectionFullName, '.' ) ;
      if ( !pDot || (pDot !=pDot1) || strCollectionFullName == pDot )
      {
         PD_LOG_MSG ( PDERROR, "Invalid format for collection name: %s,"
                      "Expected format: <collectionspace>.<collectionname>",
                      pCollectionFullName ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      *pDot = 0 ;
      if (ppCollectionName)
      {
         *ppCollectionName = pDot + 1 -&strCollectionFullName[0] +
                             pCollectionFullName ;
      }

      rc = rtnCollectionSpaceLock ( strCollectionFullName, dmsCB,
                                    FALSE, ppsu, suID ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to lock collection space %s, rc: %d",
                      strCollectionFullName, rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNRESOLVECLNAL, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNKILLCONTEXTS, "rtnKillContexts" )
   INT32 rtnKillContexts ( INT32 numContexts, INT64 *pContextIDs,
                           pmdEDUCB *cb, SDB_RTNCB *rtnCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNKILLCONTEXTS );
      for ( INT32 i = 0; i< numContexts ; i++ )
      {
         if ( rtnCB->contextFind ( pContextIDs[i] ) &&
              !cb->contextFind ( pContextIDs[i] ) )
         {
            PD_LOG ( PDWARNING, "Context %lld is not owned by current session",
                     pContextIDs[i] ) ;
            continue ;
         }
         rtnCB->contextDelete ( pContextIDs[i], cb ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNKILLCONTEXTS, rc );
      return rc ;
   }

   BOOLEAN rtnIsCommand ( const CHAR *name )
   {
      if ( name && name[0] == '$' )
      {
         return TRUE ;
      }
      return FALSE ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPARSERCOMMAND, "rtnParserCommand" )
   INT32 rtnParserCommand ( const CHAR *name, _rtnCommand **ppCommand )
   {
      INT32 rc = SDB_INVALIDARG ;
      PD_TRACE_ENTRY ( SDB_RTNPARSERCOMMAND );
      if ( ppCommand && rtnIsCommand ( name ) )
      {
         *ppCommand = getRtnCmdBuilder()->create ( &name[1] ) ;
         if ( *ppCommand )
         {
            rc = SDB_OK ;
         }
      }

      PD_TRACE_EXITRC ( SDB_RTNPARSERCOMMAND, rc );
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNRELEASECOMMAND, "rtnReleaseCommand" )
   INT32 rtnReleaseCommand ( _rtnCommand **ppCommand )
   {
      PD_TRACE_ENTRY ( SDB_RTNRELEASECOMMAND );
      if ( ppCommand && *ppCommand )
      {
         getRtnCmdBuilder()->release( *ppCommand ) ;
         *ppCommand = NULL ;
      }

      PD_TRACE_EXIT ( SDB_RTNRELEASECOMMAND );
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNINITCOMMAND, "rtnInitCommand" )
   INT32 rtnInitCommand ( _rtnCommand *pCommand ,INT32 flags, INT64 numToSkip,
                          INT64 numToReturn, const CHAR *pMatcherBuff,
                          const CHAR *pSelectBuff, const CHAR *pOrderByBuff,
                          const CHAR *pHintBuff )
   {
      INT32 rc = SDB_INVALIDARG ;
      PD_TRACE_ENTRY ( SDB_RTNINITCOMMAND );
      if ( pCommand )
      {
         try
         {
            rc = pCommand->init( flags, numToSkip, numToReturn, pMatcherBuff,
                                 pSelectBuff, pOrderByBuff, pHintBuff ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "init command[%s] exception[%s]", pCommand->name(),
                     e.what() ) ;
            rc = SDB_INVALIDARG ;
         }

         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR, "init command[%s] failed[rc=%d]",
                     pCommand->name(), rc ) ;
         }
      }

      PD_TRACE_EXITRC ( SDB_RTNINITCOMMAND, rc );
      return rc ;
   }

   INT32 dbRoleToSpaceNode ( SDB_ROLE role )
   {
      INT32 spaceNode = CMD_SPACE_NODE_NULL ;
      switch ( role )
      {
         case SDB_ROLE_DATA :
            spaceNode = CMD_SPACE_NODE_DATA ;
            break ;
         case SDB_ROLE_COORD :
            spaceNode = CMD_SPACE_NODE_COORD ;
            break ;
         case SDB_ROLE_CATALOG :
            spaceNode = CMD_SPACE_NODE_CATA ;
            break ;
         case SDB_ROLE_STANDALONE :
            spaceNode = CMD_SPACE_NODE_STANDALONE ;
            break ;
         case SDB_ROLE_OM :
            spaceNode = CMD_SPACE_NODE_OM ;
         default :
            break ;
      }
      return spaceNode ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNRUNCOMMAND, "rtnRunCommand" )
   INT32 rtnRunCommand ( _rtnCommand *pCommand, INT32 serviceType,
                         _pmdEDUCB *cb, SDB_DMSCB *dmsCB,
                         SDB_RTNCB *rtnCB, SDB_DPSCB *dpsCB,
                         INT16 w , INT64 *pContextID )
   {
      INT32 rc = SDB_INVALIDARG ;
      PD_TRACE_ENTRY ( SDB_RTNRUNCOMMAND );

      if ( pCommand )
      {
         SDB_ROLE role = pmdGetKRCB()->getDBRole() ;
         if ( !(dbRoleToSpaceNode ( role ) & pCommand->spaceNode()) )
         {
            rc = SDB_RTN_CMD_NO_NODE_AUTH ;
            goto error ;
         }

         if ( !(serviceType & pCommand->spaceService()) )
         {
            rc = SDB_RTN_CMD_NO_SERVICE_AUTH ;
            goto error ;
         }
         pCommand->setFromService( serviceType ) ;

         try
         {
            rc = pCommand->doit( cb, dmsCB, rtnCB, dpsCB, w, pContextID ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "run command[%s] exception[%s]", pCommand->name(),
                     e.what() ) ;
         }

         if ( SDB_OK != rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               PD_LOG ( PDDEBUG, "run command[%s] failed[end of the context]",
                        pCommand->name() ) ;
            }
            else
            {
               PD_LOG ( PDERROR, "run command[%s] failed[rc=%d]", 
                        pCommand->name(), rc ) ;
            }
         }

         if ( cb )
         {
            if ( SDB_OK == rc && pCommand->writable() && dpsCB )
            {
               rc = dpsCB->completeOpr( cb, w ) ;
            }
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNRUNCOMMAND, rc );
      return rc ;
   error:
      goto done ;
   }

   string rtnMakeSUFileName( const string &csName, UINT32 sequence,
                             const string &extName )
   {
      CHAR tmp[10] = {0} ;

      ossSnprintf( tmp, 9, "%d", sequence ) ;
      string suFile = csName ;
      suFile += "." ;
      suFile += tmp ;
      suFile += "." ;
      suFile += extName ;

      return suFile ;
   }

   string rtnFullPathName( const string &path, const string &name )
   {
      string fullPathName = path ;
      if ( path.length() > 0 &&
           0 != ossStrncmp(&(path.c_str())[path.length()-1], OSS_FILE_SEP, 1 ) )
      {
         fullPathName += OSS_FILE_SEP ;
      }
      fullPathName += name ;

      return fullPathName ;
   }

   INT32 rtnGetIXScanner ( const CHAR *pCollectionShortName,
                           optAccessPlan *plan,
                           dmsStorageUnit *su,
                           dmsMBContext *mbContext,
                           pmdEDUCB *cb,
                           dmsScanner **ppScanner,
                           DMS_ACCESS_TYPE accessType )
   {
      INT32 rc = SDB_OK ;

      SDB_ASSERT ( pCollectionShortName, "collection name can't be NULL" ) ;
      SDB_ASSERT ( su, "su can't be NULL" ) ;
      SDB_ASSERT ( cb, "cb can't be NULL" ) ;
      SDB_ASSERT ( plan, "plan can't be NULL" ) ;
      SDB_ASSERT ( mbContext, "mb context can't be NULL" ) ;
      SDB_ASSERT ( ppScanner, "Scanner can't be NULL" ) ;

      rtnPredicateList *predList = NULL ;
      mthMatcher *matcher        = NULL ;
      rtnIXScanner * scanner     = NULL ;

      rc = mbContext->mbLock( SHARED ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to lock collection[%s], rc = %d",
                  pCollectionShortName, rc ) ;
         goto error ;
      }

      {
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
         if ( plan->getMatcher().isInitialized() &&
              FALSE == plan->getMatcher().isMatchesAll() )
         {
            matcher = &plan->getMatcher() ;
         }
         scanner = SDB_OSS_NEW rtnIXScanner ( &indexCB, predList, su, cb ) ;
         if ( !scanner )
         {
            PD_LOG ( PDERROR, "Unable to allocate memory for scanner" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }
      mbContext->mbUnlock() ;

      *ppScanner = SDB_OSS_NEW dmsIXScanner( su->data(), mbContext, matcher,
                                             scanner, TRUE,
                                             accessType ) ;
      if ( !(*ppScanner) )
      {
         PD_LOG( PDERROR, "Unable to allocate memory for dms ixscanner" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      scanner = NULL ;

   done :
      return rc ;
   error :
      if ( scanner )
      {
         SDB_OSS_DEL scanner ;
      }
      mbContext->mbUnlock() ;
      goto done ;
   }

   INT32 rtnGetTBScanner ( const CHAR *pCollectionShortName,
                           mthMatcher &matcher,
                           dmsStorageUnit *su,
                           dmsMBContext *mbContext,
                           pmdEDUCB *cb,
                           dmsScanner **ppScanner,
                           DMS_ACCESS_TYPE accessType )
   {
      INT32 rc                 = SDB_OK ;
      mthMatcher *pMatcher     = matcher.isMatchesAll() ? NULL : &matcher ;

      SDB_ASSERT ( pCollectionShortName, "collection name can't be NULL" ) ;
      SDB_ASSERT ( su, "su can't be NULL" ) ;
      SDB_ASSERT ( mbContext, "mb context can't be NULL" ) ;
      SDB_ASSERT ( cb, "cb can't be NULL" ) ;
      SDB_ASSERT ( ppScanner, "scanner can't be NULL" ) ;

      *ppScanner = SDB_OSS_NEW dmsTBScanner( su->data(), mbContext, pMatcher,
                                             accessType ) ;
      if ( !(*ppScanner) )
      {
         PD_LOG( PDERROR, "Unable to allocate memory for dms tbscanner" ) ;
         rc = SDB_OOM ;
         goto error ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

}

