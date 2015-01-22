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

   Source File Name = rtnQuery.cpp

   Descriptive Name = Runtime Query

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime code for query
   and getmore request.

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
#include "mthMatcher.hpp"
#include "mthSelector.hpp"
#include "rtnAPM.hpp"
#include "rtnIXScanner.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "rtnContextSort.hpp"

using namespace bson ;

namespace engine
{

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNGETMORE, "rtnGetMore" )
   INT32 rtnGetMore ( SINT64 contextID,         // input, context id
                      SINT32 maxNumToReturn,    // input, max record to read
                      rtnContextBuf &buffObj,   // output
                      pmdEDUCB *cb,             // input educb
                      SDB_RTNCB *rtnCB          // input runtimecb
                      )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNGETMORE ) ;

      SDB_ASSERT ( cb, "educb can't be NULL" ) ;
      SDB_ASSERT ( rtnCB, "rtnCB can't be NULL" ) ;

      rtnContext *context = NULL ;

      context = rtnCB->contextFind ( contextID ) ;
      if ( !context )
      {
         PD_LOG ( PDERROR, "Context %lld does not exist", contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }
      if ( !cb->contextFind ( contextID ) )
      {
         PD_LOG ( PDERROR, "Context %lld does not owned by current session",
                  contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      rc = context->getMore( maxNumToReturn, buffObj, cb ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            PD_LOG( PDINFO, "Hit end of collection" ) ;
            goto error ;
         }
         PD_LOG( PDERROR, "Failed to get more from context[%lld], rc: %d",
                 context->contextID(), rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNGETMORE, rc ) ;
      return rc ;
   error :
      rtnCB->contextDelete ( contextID, cb ) ;
      goto done ;
   }

   static INT32 _rtnParseQueryMeta( const BSONObj &meta, const CHAR *&scanType,
                                    const CHAR *&indexName, INT32 &indexLID,
                                    INT32 &direction, BSONObj &blockObj )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;

      rc = rtnGetStringElement( meta, FIELD_NAME_SCANTYPE, &scanType ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_SCANTYPE, rc ) ;

      if ( 0 == ossStrcmp( scanType, VALUE_NAME_IXSCAN ) )
      {
         ele = meta.getField( FIELD_NAME_INDEXBLOCKS ) ;

         rc = rtnGetStringElement( meta, FIELD_NAME_INDEXNAME, &indexName ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                      FIELD_NAME_INDEXNAME, rc ) ;

         rc = rtnGetIntElement( meta, FIELD_NAME_INDEXLID, indexLID ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                      FIELD_NAME_INDEXLID, rc ) ;

         rc = rtnGetIntElement( meta, FIELD_NAME_DIRECTION, direction ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                      FIELD_NAME_DIRECTION, rc ) ;
      }
      else if ( 0 == ossStrcmp( scanType, VALUE_NAME_TBSCAN ) )
      {
         ele = meta.getField( FIELD_NAME_DATABLOCKS ) ;
      }
      else
      {
         PD_LOG( PDERROR, "Query meta[%s] scan type error",
                 meta.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( Array != ele.type() )
      {
         PD_LOG( PDERROR, "Block field[%s] type error",
                 ele.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      blockObj = ele.embeddedObject() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnSort ( rtnContext **ppContext,
                   const BSONObj &orderBy,
                   _pmdEDUCB *cb,
                   SINT64 numToSkip,
                   SINT64 numToReturn,
                   SINT64 &contextID )
   {
      INT32 rc = SDB_OK ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ; 
      rtnContext *context = NULL ;
      rtnContext *bkContext = NULL ;
      SINT64 old = contextID ;
      SINT64 sortContextID = -1 ;

      if ( NULL == ppContext ||
           NULL == *ppContext )
      {
         PD_LOG( PDERROR, "invalid src context" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      bkContext = *ppContext ;

      rc = rtnCB->contextNew ( RTN_CONTEXT_SORT,
                               &context,
                               sortContextID,
                               cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to crt context:%d", rc ) ;
         goto error ;
      }

      rc = ((_rtnContextSort *)context)->open( orderBy,
                                               *ppContext,
                                               cb,
                                               numToSkip,
                                               numToReturn ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open sort context:%d", rc ) ;
         goto error ;
      }

      contextID = sortContextID ;
      *ppContext = context ;
   done:
      return rc ;
   error:
      if ( -1 != sortContextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
         contextID = -1 ;
      }
      contextID = old ;
      if ( NULL != ppContext )
      {
         *ppContext = bkContext ;
      }
      goto done ;
   }

   void needResetSelector( const BSONObj &original,
                           const BSONObj &orderBy,
                           BOOLEAN &needReset )
   {
      needReset = FALSE ;
      if ( !original.isEmpty() &&
           !orderBy.isEmpty() )
      {
         BSONObjIterator itr( orderBy ) ;
         while ( itr.more() )
         {
            BSONElement ele = itr.next() ;
            const CHAR *fieldName = ele.fieldName() ;
            if ( !original.hasElement( fieldName ) )
            {
               CHAR *c = ( CHAR * )ossStrchr( fieldName, '.' ) ;
               if ( NULL == c )
               {
                  needReset = TRUE ;
                  break ;
               }

               *c = '\0' ;
               BOOLEAN has = original.hasElement( fieldName ) ;
               *c = '.' ;
               if ( !has )
               {
                  needReset = TRUE ;
                  break ;
               }
            }
         }
      }

      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNQUERY, "rtnQuery" )
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
                    rtnContextBase **ppContext,
                    BOOLEAN enablePrefetch )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNQUERY ) ;
      dmsStorageUnitID suID = DMS_INVALID_CS ;
      contextID             = -1 ;

      SDB_ASSERT ( pCollectionName, "collection name can't be NULL" ) ;
      SDB_ASSERT ( cb, "educb can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( rtnCB, "rtnCB can't be NULL" ) ;

      dmsStorageUnit *su = NULL ;
      dmsMBContext *mbContext = NULL ;
      rtnContextData *dataContext = NULL ;
      const CHAR *pCollectionShortName = NULL ;
      rtnAccessPlanManager *apm = NULL ;
      optAccessPlan *plan = NULL ;

      BSONObj hintTmp = hint ;
      BSONObj blockObj ;
      BSONObj *pBlockObj = NULL ;
      const CHAR *indexName = NULL ;
      const CHAR *scanType  = NULL ;
      INT32 indexLID = DMS_INVALID_EXTENT ;
      INT32 direction = 0 ;

      if ( FLG_QUERY_EXPLAIN & flags )
      {
         rc = rtnExplain( pCollectionName,
                          selector,
                          matcher,
                          orderBy,
                          hint,
                          flags, numToSkip,
                          numToReturn,
                          cb, dmsCB, rtnCB,
                          contextID,
                          ppContext ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to explain query:%d", rc ) ;
            goto error ;
         }
         else
         {
            goto done ;
         }
      }

      rc = rtnResolveCollectionNameAndLock ( pCollectionName, dmsCB, &su,
                                             &pCollectionShortName, suID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to resolve collection name %s",
                   pCollectionName ) ;

      rc = su->data()->getMBContext( &mbContext, pCollectionShortName, -1 ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get dms mb context, rc: %d", rc ) ;

      rc = rtnCB->contextNew ( ( flags & FLG_QUERY_PARALLED ) ?
                               RTN_CONTEXT_PARADATA : RTN_CONTEXT_DATA,
                               (rtnContext**)&dataContext,
                               contextID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to create new data context" ) ;

      if ( Object == hint.getField( FIELD_NAME_META ).type() )
      {
         BSONObjBuilder build ;
         rc = _rtnParseQueryMeta( hint.getField( FIELD_NAME_META ).embeddedObject(),
                                  scanType, indexName, indexLID, direction,
                                  blockObj ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to parase query meta[%s], rc: %d",
                      hint.toString().c_str(), rc ) ;

         pBlockObj = &blockObj ;

         if ( indexName )
         {
            build.append( "", indexName ) ;
         }
         else
         {
            build.appendNull( "" ) ;
         }
         hintTmp = build.obj () ;
      }

      apm = su->getAPM() ;
      SDB_ASSERT ( apm, "apm shouldn't be NULL" ) ;

      rc = apm->getPlan ( matcher,
                          orderBy, // orderBy
                          hintTmp, // hint
                          pCollectionShortName,
                          &plan ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get access plan for %s, context %lld, "
                  "rc: %d", pCollectionName, contextID, rc ) ;
         goto error ;
      }
      else if ( ( flags & FLG_QUERY_FORCE_HINT ) && !hintTmp.isEmpty() &&
                plan->isHintFailed() )
      {
         PD_LOG( PDERROR, "Query used force hint[%s] failed",
                 hintTmp.toString().c_str() ) ;
         rc = SDB_RTN_INVALID_HINT ;
         goto error ;
      }

      if ( pBlockObj )
      {
         if ( !indexName && TBSCAN != plan->getScanType() )
         {
            PD_LOG( PDERROR, "Scan type[%d] must be TBSCAN",
                    plan->getScanType() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         else if ( indexName && ( IXSCAN != plan->getScanType() ||
                   indexLID != plan->getIndexLID() ) )
         {
            PD_LOG( PDERROR, "Scan type[%d] error or indexLID[%d] is the "
                    "same with [%d]", plan->getScanType(),
                    plan->getIndexLID(), indexLID ) ;
            rc = SDB_IXM_NOTEXIST ;
            goto error ;
         }
      }

      if ( !plan->sortRequired() )
      {
         rc = dataContext->open( su, mbContext, plan, cb,
                                 selector,
                                 numToReturn,
                                 numToSkip,
                                 pBlockObj, direction ) ;
         PD_RC_CHECK( rc, PDERROR, "Open data context failed, rc: %d", rc ) ;
      }
      else
      {
         rc = dataContext->open( su, mbContext, plan, cb,
                                 selector,
                                 -1,
                                 0,
                                 pBlockObj, direction ) ;
         PD_RC_CHECK( rc, PDERROR, "Open data context failed, rc: %d", rc ) ;

         rc = rtnSort ( (rtnContext**)&dataContext,
                        orderBy,
                        cb, numToSkip,
                        numToReturn,
                        contextID ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to sort, rc: %d", rc ) ;
      }

      suID = DMS_INVALID_CS ;
      plan = NULL ;
      mbContext = NULL ;

      if ( cb->getMonConfigCB()->timestampON )
      {
         dataContext->getMonCB()->recordStartTimestamp() ;
      }

      if ( flags & FLG_QUERY_STRINGOUT )
      {
         dataContext->getSelector().setStringOutput( TRUE ) ;
      }
      
      if ( ppContext )
      {
         *ppContext = dataContext ;
      }
      if ( enablePrefetch )
      {
         dataContext->enablePrefetch ( cb ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNQUERY, rc ) ;
      return rc ;
   error :
      if ( su && mbContext )
      {
         su->data()->releaseMBContext( mbContext ) ;
      }
      if ( plan )
      {
         plan->release() ;
      }
      if ( DMS_INVALID_CS != suID )
      {
         dmsCB->suUnlock( suID ) ;
      }
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNTRAVERSALQUERY, "rtnTraversalQuery" )
   INT32 rtnTraversalQuery ( const CHAR *pCollectionName,
                             const BSONObj &key,
                             const CHAR *pIndexName,
                             INT32 dir,
                             pmdEDUCB *cb,
                             SDB_DMSCB *dmsCB,
                             SDB_RTNCB *rtnCB,
                             SINT64 &contextID,
                             rtnContextData **ppContext,
                             BOOLEAN enablePrefetch )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNTRAVERSALQUERY ) ;
      SDB_ASSERT ( pCollectionName, "collection name can't be NULL" ) ;
      SDB_ASSERT ( pIndexName, "index name can't be NULL" ) ;
      SDB_ASSERT ( cb, "cb can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( rtnCB, "rtnCB can't be NULL" ) ;
      SDB_ASSERT ( dir == 1 || dir == -1, "dir must be 1 or -1" ) ;

      dmsStorageUnitID      suID                 = DMS_INVALID_CS ;
      dmsStorageUnit       *su                   = NULL ;
      rtnContextData       *context              = NULL ;
      const CHAR           *pCollectionShortName = NULL ;
      optAccessPlan        *plan                 = NULL ;
      dmsMBContext         *mbContext            = NULL ;
      rtnPredicateList     *predList             = NULL ;
      rtnIXScanner         *scanner              = NULL ;

      BSONObj hint ;
      BSONObj dummy ;

      rc = rtnResolveCollectionNameAndLock ( pCollectionName, dmsCB, &su,
                                             &pCollectionShortName, suID ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to resolve collection name %s",
                    pCollectionName ) ;

      rc = su->data()->getMBContext( &mbContext, pCollectionShortName, -1 ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get dms mb context, rc: %d", rc ) ;

      rc = rtnCB->contextNew ( RTN_CONTEXT_DATA, (rtnContext**)&context,
                               contextID, cb ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to create new context, %d", rc ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;

      try
      {
         hint = BSON( "" << pIndexName ) ;
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR, "Failed to construct hint object: %s",
                       e.what() ) ;
      }

      plan = SDB_OSS_NEW optAccessPlan( su, pCollectionShortName, dummy,
                                        dummy, hint ) ;
      if ( !plan )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      rc = plan->optimize() ;
      PD_RC_CHECK( rc, PDERROR, "Plan optimize failed, rc: %d", rc ) ;
      PD_CHECK ( plan->getScanType() == IXSCAN && !plan->isAutoGen(),
                 SDB_INVALIDARG, error, PDERROR,
                 "Unable to generate access plan by index %s",
                 pIndexName ) ;

      rc = mbContext->mbLock( SHARED ) ;
      PD_RC_CHECK( rc, PDERROR, "dms mb context lock failed, rc: %d", rc ) ;

      {
         dmsRecordID rid ;
         if ( -1 == dir )
         {
            rid.resetMax() ;
         }
         else
         {
            rid.resetMin () ;
         }
         ixmIndexCB indexCB ( plan->getIndexCBExtent(), su->index(), NULL ) ;
         PD_CHECK ( indexCB.isInitialized(), SDB_SYS, error, PDERROR,
                    "unable to get proper index control block" ) ;
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
         predList->setDirection ( dir ) ;

         scanner = SDB_OSS_NEW rtnIXScanner ( &indexCB, predList, su, cb ) ;
         PD_CHECK ( scanner, SDB_OOM, error, PDERROR,
                    "Unable to allocate memory for scanner" ) ;

         rc = scanner->relocateRID ( key, rid ) ;
         PD_CHECK ( SDB_OK == rc, rc, error, PDERROR,
                    "Failed to relocate key to the specified location: %s, "
                    "rc = %d", key.toString().c_str(), rc ) ;
      }
      mbContext->mbUnlock() ;

      rc = context->openTraversal( su, mbContext, plan, scanner, cb,
                                   dummy, -1, 0 ) ;
      PD_RC_CHECK( rc, PDERROR, "Open context traversal faield, rc: %d", rc ) ;

      mbContext = NULL ;
      plan = NULL ;
      suID = DMS_INVALID_CS ;
      scanner = NULL ;
      su = NULL ;

      if ( cb->getMonConfigCB()->timestampON )
      {
         context->getMonCB()->recordStartTimestamp() ;
      }

      if ( ppContext )
      {
         *ppContext = context ;
      }
      if ( enablePrefetch )
      {
         context->enablePrefetch ( cb ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNTRAVERSALQUERY, rc ) ;
      return rc ;
   error :
      if ( su && mbContext )
      {
         su->data()->releaseMBContext( mbContext ) ;
      }
      if ( plan )
      {
         plan->release() ;
      }
      if ( scanner )
      {
         SDB_OSS_DEL scanner ;
      }
      if ( DMS_INVALID_CS != suID )
      {
         dmsCB->suUnlock( suID ) ;
      }
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNEXPLAIN, "rtnExplain" )
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
                     rtnContextBase **ppContext )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNEXPLAIN ) ;
      SDB_ASSERT ( cb, "educb can't be NULL" ) ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( rtnCB, "rtnCB can't be NULL" ) ;
      BSONObj explainOptions ;
      BSONObj realHint ;
      BSONElement ele = hint.getField( FIELD_NAME_OPTIONS ) ;
      if ( Object == ele.type() )
      {
         explainOptions = ele.embeddedObject() ;
      }

      ele = hint.getField( FIELD_NAME_HINT ) ;
      if ( Object == ele.type() )
      {
         realHint = ele.embeddedObject() ;
      }

      rtnQueryOptions options( matcher, selector,
                               orderBy, realHint,
                               pCollectionName,
                               numToSkip, numToReturn,
                               OSS_BIT_CLEAR( flags, FLG_QUERY_EXPLAIN),
                               FALSE ) ;

      rtnContextExplain *context = NULL ;
      rc = rtnCB->contextNew( RTN_CONTEXT_EXPLAIN,
                              ( rtnContext **)( &context ),
                              contextID, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to create explain context:%d", rc ) ;
         goto error ;
      }

      rc = context->open( options, explainOptions ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open explain context:%d", rc ) ;
         goto error ;
      }

      if ( ppContext )
      {
         *ppContext = context ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNEXPLAIN, rc ) ;
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

}

