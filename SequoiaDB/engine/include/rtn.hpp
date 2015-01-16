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

   Source File Name = rtn.hpp

   Descriptive Name = RunTime Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains declare for runtime
   functions.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef RTN_HPP_
#define RTN_HPP_

#include "core.hpp"
#include "dms.hpp"
#include "dmsCB.hpp"
#include "rtnCB.hpp"
#include "../bson/bson.h"
#include "pmdEDU.hpp"
#include "rtnCommand.hpp"
#include "dpsLogWrapper.hpp"
#include "pmd.hpp"
#include "pd.hpp"

#define RTN_SORT_INDEX_NAME "sort"
using namespace bson;

namespace engine
{

   class _dmsScanner ;

   INT32 rtnReallocBuffer ( CHAR **ppBuffer, INT32 *bufferSize,
                            INT32 newLength, INT32 alignmentSize ) ;

   BSONObj rtnUniqueKeyNameObj( const BSONObj &obj ) ;

   BSONObj rtnNullKeyNameObj( const BSONObj &obj ) ;

   INT32 rtnGetIXScanner ( const CHAR *pCollectionShortName,
                           _optAccessPlan *plan,
                           _dmsStorageUnit *su,
                           _dmsMBContext *mbContext,
                           _pmdEDUCB *cb,
                           _dmsScanner **ppScanner,
                           DMS_ACCESS_TYPE accessType ) ;

   INT32 rtnGetTBScanner ( const CHAR *pCollectionShortName,
                           _mthMatcher &matcher,
                           _dmsStorageUnit *su,
                           _dmsMBContext *mbContext,
                           _pmdEDUCB *cb,
                           _dmsScanner **ppScanner,
                           DMS_ACCESS_TYPE accessType ) ;

   INT32 rtnGetIndexSeps( _optAccessPlan *plan,
                          _dmsStorageUnit *su,
                          _dmsMBContext *mbContext,
                          _pmdEDUCB *cb,
                          std::vector< BSONObj > &idxBlocks,
                          std::vector< dmsRecordID > &idxRIDs ) ;

   INT32 rtnInsert ( const CHAR *pCollectionName, BSONObj &objs, INT32 objNum,
                     INT32 flags, pmdEDUCB *cb ) ;

   INT32 rtnInsert ( const CHAR *pCollectionName, BSONObj &objs, INT32 objNum,
                     INT32 flags, pmdEDUCB *cb, SDB_DMSCB *dmsCB,
                     SDB_DPSCB *dpsCB, INT16 w = 1 ) ;

   INT32 rtnUpdate ( const CHAR *pCollectionName, const BSONObj &selector,
                     const BSONObj &updator, const BSONObj &hint, INT32 flags,
                     pmdEDUCB *cb, INT64 *pUpdateNum = NULL ) ;

   INT32 rtnUpdate ( const CHAR *pCollectionName, const BSONObj &selector,
                     const BSONObj &updator, const BSONObj &hint, INT32 flags,
                     pmdEDUCB *cb, SDB_DMSCB *dmsCB, SDB_DPSCB *dpsCB,
                     INT16 w = 1, INT64 *pUpdateNum = NULL ) ;

   INT32 rtnDelete ( const CHAR *pCollectionName, const BSONObj &deletor,
                     const BSONObj &hint, INT32 flags, pmdEDUCB *cb,
                     INT64 *pDelNum = NULL ) ;

   INT32 rtnDelete ( const CHAR *pCollectionName, const BSONObj &deletor,
                     const BSONObj &hint, INT32 flags, pmdEDUCB *cb,
                     SDB_DMSCB *dmsCB, SDB_DPSCB *dpsCB, INT16 w = 1,
                     INT64 *pDelNum = NULL ) ;

   INT32 rtnTraversalDelete ( const CHAR *pCollectionName,
                              const BSONObj &key,
                              const CHAR *pIndexName,
                              INT32 dir,
                              pmdEDUCB *cb,
                              SDB_DMSCB *dmsCB,
                              SDB_DPSCB *dpsCB,
                              INT16 w = 1 ) ;

   INT32 rtnMsg ( MsgOpMsg *pMsg ) ;

   INT32 rtnQuery ( const CHAR *pCollectionName,
                    const BSONObj &selector,
                    const BSONObj &matcher,
                    const BSONObj &orderBy,
                    const BSONObj &hint,
                    SINT32 flags,
                    pmdEDUCB *cb,
                    SINT64 numToSkip,
                    SINT64 numToReturn,
                    SDB_DMSCB *dmsCB,
                    SDB_RTNCB *rtnCB,
                    SINT64 &contextID,
                    rtnContextBase **ppContext = NULL,
                    BOOLEAN enablePrefetch = FALSE ) ;

   INT32 rtnSort ( rtnContext **ppContext,
                   const BSONObj &orderBy,
                   _pmdEDUCB *cb,
                   SINT64 numToSkip,
                   SINT64 numToReturn,
                   SINT64 &contextID ) ;

   INT32 rtnTraversalQuery ( const CHAR *pCollectionName,
                             const BSONObj &key,
                             const CHAR *pIndexName,
                             INT32 dir,
                             pmdEDUCB *cb,
                             SDB_DMSCB *dmsCB,
                             SDB_RTNCB *rtnCB,
                             SINT64 &contextID,
                             rtnContextData **ppContext = NULL,
                             BOOLEAN enablePrefetch = FALSE ) ;

   INT32 rtnCreateCollectionSpaceCommand ( const CHAR *pCollectionSpace,
                                           pmdEDUCB *cb,
                                           SDB_DMSCB *dmsCB, SDB_DPSCB *dpsCB,
                                           INT32 pageSize = DMS_PAGE_SIZE_DFT,
                                           INT32 lobPageSize = DMS_DEFAULT_LOB_PAGE_SZ,
                                           BOOLEAN sysCall = FALSE,
                                           BOOLEAN delWhenExist = FALSE ) ;

   INT32 rtnCreateCollectionCommand ( const CHAR *pCollection,
                                      UINT32 attributes,
                                      pmdEDUCB *cb,
                                      SDB_DMSCB *dmsCB,
                                      SDB_DPSCB *dpsCB,
                                      INT32 flags = 0,
                                      BOOLEAN sysCall = FALSE ) ;

   INT32 rtnCreateCollectionCommand ( const CHAR *pCollection,
                                      const BSONObj &shardingKey,
                                      UINT32 attributes,
                                      _pmdEDUCB * cb,
                                      SDB_DMSCB *dmsCB,
                                      SDB_DPSCB *dpsCB,
                                      INT32 flags = 0,
                                      BOOLEAN sysCall = FALSE ) ;

   INT32 rtnGetMore ( SINT64 contextID,            // input, context id
                      SINT32 maxNumToReturn,       // input, max record to read
                      rtnContextBuf &buffObj,      // output
                      pmdEDUCB *cb,                // input educb
                      SDB_RTNCB *rtnCB             // input runtimecb
                      ) ;

   INT32 rtnLoadCollectionSpace ( const CHAR *pCSName,
                                  const CHAR *dataPath,
                                  const CHAR *indexPath,
                                  const CHAR *lobPath,
                                  SDB_DMSCB *dmsCB,
                                  BOOLEAN checkOnly = FALSE ) ;

   INT32 rtnLoadCollectionSpaces ( const CHAR *dataPath,
                                   const CHAR *indexPath,
                                   const CHAR *lobPath,
                                   SDB_DMSCB *dmsCB ) ;

   INT32 rtnDelCollectionSpaceCommand ( const CHAR *pCollectionSpace,
                                        _pmdEDUCB *cb,
                                        SDB_DMSCB *dmsCB,
                                        SDB_DPSCB *dpsCB,
                                        BOOLEAN sysCall,
                                        BOOLEAN dropFile ) ;

   INT32 rtnUnloadCollectionSpace( const CHAR *pCollectionSpace,
                                   _pmdEDUCB *cb,
                                   SDB_DMSCB *dmsCB ) ;

   INT32 rtnUnloadCollectionSpaces( _pmdEDUCB * cb,
                                    SDB_DMSCB * dmsCB ) ;

   INT32 rtnCollectionSpaceLock ( const CHAR *pCollectionSpaceName,
                                  SDB_DMSCB *dmsCB,
                                  BOOLEAN loadFile,
                                  dmsStorageUnit **ppsu,
                                  dmsStorageUnitID &suID,
                                  OSS_LATCH_MODE lockType = SHARED,
                                  INT32 millisec = -1 ) ;

   INT32 rtnResolveCollectionNameAndLock ( const CHAR *pCollectionFullName,
                                           SDB_DMSCB *dmsCB,
                                           dmsStorageUnit **ppsu,
                                           const CHAR **ppCollectionName,
                                           dmsStorageUnitID &suID ) ;

   INT32 rtnFindCollection ( const CHAR *pCollection,
                             SDB_DMSCB *dmsCB ) ;

   INT32 rtnKillContexts ( SINT32 numContexts, SINT64 *pContextIDs,
                           pmdEDUCB *cb, SDB_RTNCB *rtnCB ) ;

   INT32 rtnBackup ( pmdEDUCB *cb, const CHAR *path, const CHAR *backupName,
                     BOOLEAN ensureInc, BOOLEAN rewrite, const CHAR *desp,
                     const BSONObj &option ) ;

   INT32 rtnRemoveBackup ( pmdEDUCB *cb, const CHAR *path,
                           const CHAR *backupName,
                           const BSONObj &option ) ;

   INT32 rtnDumpBackups ( const BSONObj &hint, rtnContextDump *context ) ;

   BOOLEAN rtnIsInBackup () ;

   INT32 rtnRebuildDB ( pmdEDUCB *cb ) ;

   INT32 rtnReorgOffline ( const CHAR *pCollectionName,
                           const BSONObj &hint,
                           pmdEDUCB *cb,
                           SDB_DMSCB *dmsCB,
                           SDB_RTNCB *rtnCB,
                           BOOLEAN ignoreError = FALSE ) ;

   INT32 rtnReorgRecover ( const CHAR *pCollectionName,
                           pmdEDUCB *cb,
                           SDB_DMSCB *dmsCB,
                           SDB_RTNCB *rtnCB ) ;


   INT32 rtnGetStringElement ( const BSONObj &obj, const CHAR *fieldName,
                               const CHAR **value ) ;

   INT32 rtnGetSTDStringElement ( const BSONObj &obj, const CHAR *fieldName,
                                  string &value ) ;

   INT32 rtnGetObjElement ( const BSONObj &obj, const CHAR *fieldName,
                            BSONObj &value ) ;

   INT32 rtnGetIntElement ( const BSONObj &obj, const CHAR *fieldName,
                            INT32 &value ) ;

   INT32 rtnGetBooleanElement ( const BSONObj &obj, const CHAR *fieldName,
                                BOOLEAN &value ) ;

   INT32 rtnCreateIndexCommand ( const CHAR *pCollection,
                                 const BSONObj &indexObj,
                                 _pmdEDUCB *cb,
                                 SDB_DMSCB *dmsCB,
                                 SDB_DPSCB *dpsCB,
                                 BOOLEAN isSys = FALSE ) ;

   INT32 rtnDropCollectionCommand ( const CHAR *pCollection,
                                    _pmdEDUCB *cb,
                                    SDB_DMSCB *dmsCB,
                                    SDB_DPSCB *dpsCB ) ;

   INT32 rtnTruncCollectionCommand( const CHAR *pCollection,
                                    _pmdEDUCB *cb,
                                    SDB_DMSCB *dmsCB,
                                    SDB_DPSCB *dpsCB ) ;

   INT32 rtnDropCollectionSpaceCommand ( const CHAR *pCollectionSpace,
                                         _pmdEDUCB *cb,
                                         SDB_DMSCB *dmsCB,
                                         SDB_DPSCB *dpsCB,
                                         BOOLEAN   sysCall = FALSE ) ;

   INT32 rtnDropCollectionSpaceP1 ( const CHAR *pCollectionSpace,
                                    _pmdEDUCB *cb,
                                    SDB_DMSCB *dmsCB,
                                    SDB_DPSCB *dpsCB,
                                    BOOLEAN   sysCall = FALSE );

   INT32 rtnDropCollectionSpaceP2 ( const CHAR *pCollectionSpace,
                                    _pmdEDUCB *cb,
                                    SDB_DMSCB *dmsCB,
                                    SDB_DPSCB *dpsCB,
                                    BOOLEAN   sysCall = FALSE );

   INT32 rtnDropCollectionSpaceP1Cancel ( const CHAR *pCollectionSpace,
                                    _pmdEDUCB *cb,
                                    SDB_DMSCB *dmsCB,
                                    SDB_DPSCB *dpsCB,
                                    BOOLEAN   sysCall = FALSE );

   INT32 rtnDropIndexCommand ( const CHAR *pCollection,
                               BSONElement &identifier,
                               pmdEDUCB *cb,
                               SDB_DMSCB *dmsCB,
                               SDB_DPSCB *dpsCB,
                               BOOLEAN sysCall = FALSE ) ;

   INT32 rtnGetCount ( const CHAR *pCollection,
                       const BSONObj &matcher,
                       const BSONObj &hint,
                       SDB_DMSCB *dmsCB,
                       _pmdEDUCB *cb,
                       SDB_RTNCB *rtnCB,
                       INT64 *count,
                       INT32 flags = 0 ) ;

   INT32 rtnGetCount ( const CHAR *pCollection,
                       const BSONObj &matcher,
                       const BSONObj &hint,
                       SDB_DMSCB *dmsCB,
                       _pmdEDUCB *cb,
                       SDB_RTNCB *rtnCB,
                       rtnContext *context,
                       INT32 flags = 0 ) ;

   INT32 rtnGetCommandEntry ( RTN_COMMAND_TYPE command,
                              const CHAR *pCollectionName,
                              const BSONObj &selector,
                              const BSONObj &matcher,
                              const BSONObj &orderBy,
                              const BSONObj &hint,
                              SINT32 flags,
                              pmdEDUCB *cb,
                              SINT64 numToSkip,
                              SINT64 numToReturn,
                              SDB_DMSCB *dmsCB,
                              SDB_RTNCB *rtnCB,
                              SINT64 &contextID ) ;

   INT32 rtnListCommandEntry ( RTN_COMMAND_TYPE command,
                               const BSONObj &selector,
                               const BSONObj &matcher,
                               const BSONObj &orderBy,
                               const BSONObj &hint,
                               SINT32 flags,
                               pmdEDUCB *cb,
                               SINT64 numToSkip,
                               SINT64 numToReturn,
                               SDB_DMSCB *dmsCB,
                               SDB_RTNCB *rtnCB,
                               SINT64 &contextID,
                               BOOLEAN addInfo ) ;

   INT32 rtnSnapCommandEntry ( RTN_COMMAND_TYPE command,
                               const BSONObj &selector,
                               const BSONObj &matcher,
                               const BSONObj &orderBy,
                               SINT32 flags,
                               pmdEDUCB *cb,
                               SINT64 numToSkip,
                               SINT64 numToReturn,
                               SDB_DMSCB *dmsCB,
                               SDB_RTNCB *rtnCB,
                               SINT64 &contextID,
                               BOOLEAN addInfo ) ;

   INT32 rtnGetQueryMeta( const CHAR *pCollectionName,
                          const BSONObj &match,
                          const BSONObj &orderby,
                          const BSONObj &hint,
                          SDB_DMSCB *dmsCB,
                          pmdEDUCB *cb,
                          rtnContextDump *context ) ;

   INT32 rtnTestCollectionCommand ( const CHAR *pCollection,
                                    SDB_DMSCB *dmsCB ) ;

   INT32 rtnTestCollectionSpaceCommand ( const CHAR *pCollectionSpace,
                                         SDB_DMSCB *dmsCB ) ;

   INT32 rtnTestIndex( const CHAR *pCollection,
                       const CHAR *pIndexName,
                       SDB_DMSCB *dmsCB,
                       const BSONObj *pIndexDef = NULL,
                       BOOLEAN *pIsSame = NULL ) ;

   BOOLEAN rtnIsCommand ( const CHAR *name ) ;
   INT32 rtnParserCommand ( const CHAR *name, _rtnCommand **ppCommand ) ;
   INT32 rtnReleaseCommand ( _rtnCommand **ppCommand ) ;
   INT32 rtnInitCommand ( _rtnCommand *pCommand ,INT32 flags, INT64 numToSkip, 
                          INT64 numToReturn, const CHAR *pMatcherBuff,
                          const CHAR *pSelectBuff, const CHAR *pOrderByBuff,
                          const CHAR *pHintBuff ) ;
   INT32 rtnRunCommand ( _rtnCommand *pCommand, INT32 serviceType,
                         _pmdEDUCB *cb, SDB_DMSCB *dmsCB,
                         SDB_RTNCB *rtnCB, SDB_DPSCB *dpsCB,
                         INT16 w = 1, INT64 *pContextID = NULL ) ;

   INT32 rtnTransBegin( _pmdEDUCB *cb );
   INT32 rtnTransCommit( _pmdEDUCB *cb, SDB_DPSCB *dpsCB );
   INT32 rtnTransRollback( _pmdEDUCB * cb, SDB_DPSCB *dpsCB );
   INT32 rtnTransRollbackAll( _pmdEDUCB * cb );
   INT32 rtnTransTryLockCL( const CHAR *pCollection, INT32 lockType,
                           _pmdEDUCB *cb,SDB_DMSCB *dmsCB,
                           SDB_DPSCB *dpsCB );
   INT32 rtnTransTryLockCS( const CHAR *pSpace, INT32 lockType,
                           _pmdEDUCB *cb,SDB_DMSCB *dmsCB,
                           SDB_DPSCB *dpsCB );
   INT32 rtnTransReleaseLock( const CHAR *pCollection,
                           _pmdEDUCB *cb,SDB_DMSCB *dmsCB,
                           SDB_DPSCB *dpsCB );

   BOOLEAN rtnVerifyCollectionSpaceFileName ( const CHAR *pFileName,
                                              CHAR *pSUName,
                                              UINT32 bufferSize,
                                              UINT32 &sequence,
                                              const CHAR *extFilter =
                                              DMS_DATA_SU_EXT_NAME ) ;

   string   rtnMakeSUFileName( const string &csName, UINT32 sequence,
                               const string &extName ) ;
   string   rtnFullPathName( const string &path, const string &name ) ;

   INT32    rtnAggregate( const CHAR *pCollectionName, bson::BSONObj &objs,
                          INT32 objNum, SINT32 flags, pmdEDUCB *cb,
                          SDB_DMSCB *dmsCB, SINT64 &contextID ) ;

   INT32 rtnExplain( const CHAR *pCollectionName,
                     const BSONObj &selector,
                     const BSONObj &matcher,
                     const BSONObj &orderBy,
                     const BSONObj &hint,
                     SINT32 flags,
                     SINT64 numToSkip,
                     SINT64 numToReturn,
                     pmdEDUCB *cb, SDB_DMSCB *dmsCB,
                     SDB_RTNCB *rtnCB, INT64 &contextID,
                     rtnContextBase **ppContext = NULL ) ;
}

#endif

