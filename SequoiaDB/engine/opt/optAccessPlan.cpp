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

   Source File Name = optAccessPlan.cpp

   Descriptive Name = Optimizer Access Plan

   When/how to use: this program may be used on binary and text-formatted
   versions of Optimizer component. This file contains functions for optimizer
   access plan creation. It will calculate based on rules and try to estimate
   a lowest cost plan to access data.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "optAccessPlan.hpp"
#include "dmsStorageUnit.hpp"
#include "../bson/ordering.h"
#include "rtnAPM.hpp"
#include "pdTrace.hpp"
#include "optTrace.hpp"

using namespace bson;
namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB__OPTACCPLAN__OPTHINT, "_optAccessPlan::_optimizeHint" )
   INT32 _optAccessPlan::_optimizeHint ( dmsMBContext *mbContext,
                                         const CHAR *pIndexName,
                                         const rtnPredicateSet &predSet )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__OPTACCPLAN__OPTHINT );

      dmsExtentID indexCBExtent = DMS_INVALID_EXTENT ;
      INT64 costEstimation = 0 ;
      INT32 dir = 1 ;
      _estimateDetail detail ;

      rc = _su->index()->getIndexCBExtent( mbContext, pIndexName,
                                           indexCBExtent ) ;
      if ( rc )
      {
         goto error ;
      }

      // call estimate index to get estimation and most importantly the scan
      // direction
      rc = _estimateIndex ( indexCBExtent, costEstimation, dir, detail ) ;
      if ( rc )
      {
         if ( SDB_IXM_UNEXPECTED_STATUS == rc )
         {
            PD_LOG ( PDINFO, "Unable to use the specified index: %s, index is "
                     "not normal status.", pIndexName ) ;
         }
         goto error ;
      }

      rc = _useIndex ( indexCBExtent, dir, predSet, detail ) ;

   done :
      PD_TRACE_EXITRC ( SDB__OPTACCPLAN__OPTHINT, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__OPTACCPLAN__OPTHINT2, "_optAccessPlan::_optimizeHint" )
   INT32 _optAccessPlan::_optimizeHint ( dmsMBContext *mbContext,
                                         const OID &indexOID,
                                         const rtnPredicateSet &predSet )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__OPTACCPLAN__OPTHINT2 ) ;

      dmsExtentID indexCBExtent = DMS_INVALID_EXTENT ;
      INT64 costEstimation = 0 ;
      INT32 dir = 1 ;
      _estimateDetail detail ;

      rc = _su->index()->getIndexCBExtent( mbContext, indexOID,
                                           indexCBExtent ) ;
      if ( rc )
      {
         goto error ;
      }
      // call estimate index to get estimation and most importantly the scan
      // direction
      rc = _estimateIndex ( indexCBExtent, costEstimation, dir, detail ) ;
      if ( rc )
      {
         if ( SDB_IXM_UNEXPECTED_STATUS == rc )
         {
            PD_LOG ( PDINFO, "Unable to use the specified index, index is "
                     "not normal status." ) ;
         }
         goto error ;
      }
      rc = _useIndex ( indexCBExtent, dir, predSet, detail ) ;

   done :
      PD_TRACE_EXITRC ( SDB__OPTACCPLAN__OPTHINT2, rc );
      return rc ;
   error :
      goto done ;
   }

   // caller must hold S latch on the obj
   PD_TRACE_DECLARE_FUNCTION ( SDB__OPTACCPLAN__OPTHINT3, "_optAccessPlan::_optimizeHint" )
   INT32 _optAccessPlan::_optimizeHint( dmsMBContext *mbContext,
                                        const rtnPredicateSet &predSet )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__OPTACCPLAN__OPTHINT3 ) ;
      SDB_ASSERT ( !_hint.isEmpty(), "hint can't be empty" ) ;

      BSONObjIterator it ( _hint ) ;
      PD_LOG ( PDDEBUG, "Hint is provided: %s", _hint.toString().c_str() ) ;
      rc = SDB_RTN_INVALID_HINT ;
      // user can define more than one index name/oid in hint, it will pickup
      // the first valid one
      while ( it.more() )
      {
         BSONElement hint = it.next() ;
         if ( hint.type() == String )
         {
            PD_LOG ( PDDEBUG, "Try to use index: %s", hint.valuestr() ) ;
            // search based on index name
            rc = _optimizeHint ( mbContext, hint.valuestr(), predSet ) ;
         }
         else if ( hint.type() == jstOID )
         {
            PD_LOG ( PDDEBUG, "Try to use index: %s",
                     _hint.toString().c_str() ) ;
            // search based on OID
            rc = _optimizeHint ( mbContext, hint.__oid(), predSet ) ;
         }
         else if ( hint.type() == jstNULL )
         {
            PD_LOG ( PDDEBUG, "Use Collection Scan by Hint" ) ;
            // if we use null in the hint, we use tbscan
            _scanType = TBSCAN ;
            rc = SDB_OK ;
            // if hint shows tbscan, let's check whether manual sort is required
            if ( !_orderBy.isEmpty() )
            {
               _sortRequired = TRUE ;
            }
         }
         if ( SDB_OK == rc )
         {
            break ;
         }
      }

      // let's check return value
      if ( rc )
      {
         PD_LOG ( PDWARNING, "Hint is not valid: %s",
                  _hint.toString().c_str() ) ;
         _hintFailed = TRUE ;
         goto error ;
      }
      PD_LOG ( PDDEBUG, "Hint is successfully applied" ) ;

   done :
      PD_TRACE_EXITRC ( SDB__OPTACCPLAN__OPTHINT3, rc );
      return rc ;
   error :
      goto done ;
   }

   // we are not using real CBO since we don't have time to implement statistics
   // yet, so let's hardcode base line cost for now and we'll improve it further
   #define TEMP_COST_BASELINE 10000

   // output cost estimation, dir, and indexCBExtent
   PD_TRACE_DECLARE_FUNCTION ( SDB__OPTACCPLAN__ESTINX, "_optAccessPlan::_estimateIndex" )
   INT32 _optAccessPlan::_estimateIndex ( dmsExtentID indexCBExtent,
                                          INT64 &costEstimation,
                                          INT32 &dir,
                                          _estimateDetail &detail )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__OPTACCPLAN__ESTINX );
      ixmIndexCB indexCB ( indexCBExtent, _su->index(), NULL ) ;
      if ( !indexCB.isInitialized() )
      {
         PD_LOG ( PDWARNING, "Failed to use index at extent %d",
                  indexCBExtent ) ;
         rc = SDB_DMS_INIT_INDEX ;
         goto error ;
      }
      if ( indexCB.getFlag() != IXM_INDEX_FLAG_NORMAL )
      {
         PD_LOG ( PDDEBUG, "Index is not normal status, skip" ) ;
         rc = SDB_IXM_UNEXPECTED_STATUS ;
         goto error ;
      }
      {
         // compare with pure tablescan, each index scan need to advance +
         // compare key + fetch, which could be significantely more expensive
         // than tbscan. So let's increase baseline cost for full index
         // scan+fetch
         // note this is a very bad hack
         costEstimation = 10*TEMP_COST_BASELINE ;

         BSONObj idxPattern = indexCB.keyPattern() ;
         Ordering keyorder = Ordering::make(idxPattern) ;
         Ordering orderByorder = Ordering::make ( _orderBy ) ;
         INT32 nFields = _orderBy.nFields() ;
         INT32 nQueryFields = 0 ;
         INT32 matchedFields = 0 ;
         BSONObjIterator keyItr (idxPattern) ;
         BSONObjIterator orderItr ( _orderBy ) ;
         // first check order
         FLOAT32 orderFactor = 1.0f ;
         dir = 1 ;
         BOOLEAN start = TRUE ;
         rtnStartStopKey startStopKey;
         BSONElement startKey;
         BSONElement stopKey;
         while ( keyItr.more() && orderItr.more() )
         {
            BSONElement keyEle = keyItr.next() ;
            BSONElement orderEle = orderItr.next() ;
            if ( ossStrcmp ( keyEle.fieldName(), orderEle.fieldName() ) == 0 )
            {
               // if the first field match name, let's compare the order
               if ( start )
               {
                  // if key is forward and order by is forward, dir = forward
                  // if key is backward and order by is forward, dir = backward
                  // if key is backward and order by is backward, dir = forward
                  // if key is forward and order by is backward, dir = backward
                  dir = (keyorder.get(matchedFields) ==
                         orderByorder.get(matchedFields))?1:-1 ;
                  start = FALSE ;
               }
               // break if the order is different
               if ( keyorder.get(matchedFields)*dir !=
                    orderByorder.get(matchedFields) )
                  break ;
               ++matchedFields ;
            }
            else
               break ;
         }
         orderFactor = 1.0f - ((nFields == 0) ? (0) :
                                (((FLOAT32)matchedFields)/((FLOAT32)nFields)));
         orderFactor = OSS_MIN(1.0f, orderFactor) ;
         orderFactor = OSS_MAX(0.0f, orderFactor) ;

         // then we check query compare with index pattern
         FLOAT32 queryFactor = 1.0f ;
         keyItr = BSONObjIterator ( idxPattern ) ;
         nFields = idxPattern.nFields() ;
         matchedFields = 0 ;
         const map<string, rtnPredicate> &predicates
                     = _matcher.getPredicateSet().predicates();
         map<string, rtnPredicate>::const_iterator it;
         nQueryFields = predicates.size() ;
         while ( keyItr.more() )
         {
            BSONElement keyEle = keyItr.next() ;
            // for each element in the key, let's see if we used it in the
            // query. More keys used by query, we esimtate the index may more
            // satisfy our requirement
            if (( it = predicates.find( keyEle.fieldName() ))
                  != predicates.end() )
            {
               // some cases have predicates, though it's not 
               // that proper to use index, such as the case
               // with max and min boundanry,
               // so we need to lead such cases to table scan here
               startStopKey = it->second._startStopKeys[0] ;
               startKey = startStopKey._startKey._bound ;
               stopKey = startStopKey._stopKey._bound ;
               
               if(0 == startKey.woCompare( bson::minKey.firstElement() ) &&
                  0 == stopKey.woCompare( bson::maxKey.firstElement() ))
               {
                  continue;
               }
               
               ++matchedFields ;
            }
            else
            {
               break;
            }
         }
         if ( nFields == 0 || nQueryFields == 0 )
            queryFactor = 1.0f ;
         else
            queryFactor = 1.0f - ((FLOAT32)matchedFields)/
                  (OSS_MIN(((FLOAT32)nFields),((FLOAT32)nQueryFields))) ;
         queryFactor = OSS_MIN(1.0f, queryFactor) ;
         queryFactor = OSS_MAX(0.0f, queryFactor) ;

         costEstimation = costEstimation*queryFactor*orderFactor ;

         /// we try to set matchall only when all fields converted into predicates
         if ( _matcher.totallyConverted() )
         {
            detail.matchAll = ( 0 != matchedFields ) &&
                              ( matchedFields == nQueryFields ) &&
                               matchedFields <= idxPattern.nFields() ;
         }
      }
      PD_LOG ( PDDEBUG, "Index Scan Estimation: %s : %d",
               indexCB.getName (), costEstimation ) ;

   done :
      PD_TRACE_EXITRC ( SDB__OPTACCPLAN__ESTINX, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__OPTACCPLAN__ESTINX2, "_optAccessPlan::_estimateIndex" )
   INT32 _optAccessPlan::_estimateIndex ( dmsMBContext *mbContext,
                                          INT32 indexID,
                                          INT64 &costEstimation,
                                          INT32 &dir,
                                          dmsExtentID &indexCBExtent,
                                          _estimateDetail &detail )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__OPTACCPLAN__ESTINX2 ) ;

      rc = _su->index()->getIndexCBExtent( mbContext, indexID,
                                           indexCBExtent ) ;
      if ( rc )
      {
         goto done ;
      }
      rc = _estimateIndex ( indexCBExtent, costEstimation, dir, detail ) ;

   done :
      PD_TRACE_EXITRC ( SDB__OPTACCPLAN__ESTINX2, rc );
      return rc ;
   }

   void _optAccessPlan::_estimateTBScan ( INT64 &costEstimation )
   {
      costEstimation = TEMP_COST_BASELINE ;
      PD_LOG ( PDDEBUG, "Collection Scan estimation cost is %d",
               costEstimation ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__OPTACCPLAN__USEINX, "_optAccessPlan::_useIndex" )
   INT32 _optAccessPlan::_useIndex ( dmsExtentID indexCBExtent,
                                     INT32 dir,
                                     const rtnPredicateSet &predSet,
                                     const _estimateDetail &detail )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__OPTACCPLAN__USEINX );
      ixmIndexCB indexCB ( indexCBExtent, _su->index(), NULL ) ;
      if ( !indexCB.isInitialized() )
      {
         PD_LOG ( PDWARNING, "Failed to use index at extent %d",
                  indexCBExtent ) ;
         rc = SDB_DMS_INIT_INDEX ;
         goto error ;
      }
      {
         _direction = dir ;
         // if there's order by statement, let's see if the index we are using
         // is able to bypass sort phase
         if ( !_orderBy.isEmpty() )
         {
            BSONObj idxPattern = indexCB.keyPattern() ;
            Ordering keyorder = Ordering::make(idxPattern) ;
            Ordering orderByorder = Ordering::make ( _orderBy ) ;
            INT32 matchedFields = 0 ;
            BSONObjIterator keyItr (idxPattern) ;
            BSONObjIterator orderItr ( _orderBy ) ;
            while ( keyItr.more() && orderItr.more() )
            {
               BSONElement keyEle = keyItr.next() ;
               BSONElement orderEle = orderItr.next() ;
               if (ossStrcmp( keyEle.fieldName(), orderEle.fieldName() ) == 0)
               {
                  if ( keyorder.get(matchedFields)*dir !=
                       orderByorder.get(matchedFields) )
                     break ;
                  ++matchedFields ;
               }
               else
                  break ;
            }
            if ( matchedFields == _orderBy.nFields() )
               _sortRequired = FALSE ;
            else
               _sortRequired = TRUE ;
         }
         if ( _predList )
            SDB_OSS_DEL _predList ;
         // memory is freed in destructor
         _predList = SDB_OSS_NEW rtnPredicateList ( predSet, &indexCB,
                                                          _direction ) ;
         if ( !_predList )
         {
            PD_LOG ( PDERROR, "Out of memory" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         _scanType = IXSCAN ;
         _indexCBExtent = indexCBExtent ;
         _indexLID = indexCB.getLogicalID() ;
         indexCB.getIndexID(_indexOID) ;
         {
         const CHAR *idxName = indexCB.getName() ;
         ossMemcpy( _idxName, idxName, ossStrlen( idxName ) ) ;
         }
         _matcher.setMatchesAll( detail.matchAll ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__OPTACCPLAN__USEINX, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__OPTACCPLAN_OPT, "_optAccessPlan::optimize" )
   INT32 _optAccessPlan::optimize()
   {
      INT32 rc = SDB_OK ;
      dmsMBContext *mbContext = NULL ;

      PD_TRACE_ENTRY ( SDB__OPTACCPLAN_OPT ) ;

      // first let's build matcher
      rc = _matcher.loadPattern ( _query ) ;
      PD_RC_CHECK ( rc, (SDB_RTN_INVALID_PREDICATES==rc) ? PDINFO : PDERROR,
                    "Failed to load query, rc = %d", rc ) ;
      // currently the plan is set to tablescan with a valid matcher and Null
      // predList

      // then let's see if there's better index plan exist
      {
         // get the predicate sets from matcher
         const rtnPredicateSet &predSet = _matcher.getPredicateSet () ;
         // lock the collection before picking up the right index
         // this function will also get collection id
         rc = _su->data()->getMBContext( &mbContext, _collectionName, SHARED ) ;
         PD_RC_CHECK( rc, PDERROR, "Get dms mb context failed, rc: %d", rc ) ;

         if ( _hint.isEmpty() || SDB_OK != _optimizeHint( mbContext, predSet ) )
         {
            // if hint not defined, or if failed to optimize using hint, let's
            // set the plan is automatically picked
            _isAutoPlan = TRUE ;
            // if hint is not defined, or if failed to optimize using
            // hint, let's check each index
            dmsExtentID bestMatchedIndexCBExtent = DMS_INVALID_EXTENT ;
            INT64 bestCostEstimation = 0 ;
            INT32 bestMatchedIndexDirection = 1 ;
            _estimateDetail detail ;

            // use tbscan as baseline
            _estimateTBScan ( bestCostEstimation ) ;

            for ( INT32 i = 0 ; i<DMS_COLLECTION_MAX_INDEX; i++ )
            {
               _estimateDetail tmpDetail ;
               INT64 costEst ;
               dmsExtentID extID ;
               INT32 dir ;
               rc = _estimateIndex ( mbContext, i, costEst, dir, extID, tmpDetail ) ;
               if ( SDB_IXM_NOTEXIST == rc )
               {
                  break ;
               }
               if ( SDB_OK == rc )
               {
                  if ( costEst < bestCostEstimation )
                  {
                     bestMatchedIndexCBExtent = extID ;
                     bestMatchedIndexDirection = dir ;
                     bestCostEstimation = costEst ;
                     detail = tmpDetail ;
                     // we can't get to any lower than 0
                     if ( bestCostEstimation == 0 )
                     {
                        break ;
                     }
                  }
               }
               // otherwise we don't do anything, just skip
            }
            // if best matched index shows any index is better than tbscan, then
            // let's use the index
            if ( DMS_INVALID_EXTENT != bestMatchedIndexCBExtent )
            {
               PD_LOG ( PDDEBUG, "Use Index Scan" ) ;
               rc = _useIndex ( bestMatchedIndexCBExtent,
                                bestMatchedIndexDirection,
                                predSet,
                                detail ) ;
               if ( rc )
               {
                  PD_LOG ( PDWARNING, "Failed to use index %d",
                           bestMatchedIndexCBExtent ) ;
               }
            }
            else
            {
               PD_LOG ( PDDEBUG, "Use Collection Scan" ) ;
               // otherwise if there's no index is better than tbscan, let's
               // check if we need manually sort
               if ( !_orderBy.isEmpty() )
               {
                  _sortRequired = TRUE ;
               }
            }
         }

         // unlock collection and reset rc
         rc = SDB_OK ;
         mbContext->mbUnlock() ;
         _isInitialized = TRUE ;
         _isValid = TRUE ;
      }

   done :
      if ( mbContext )
      {
         _su->data()->releaseMBContext( mbContext ) ;
      }
      PD_TRACE_EXITRC ( SDB__OPTACCPLAN_OPT, rc );
      return rc ;
   error :
      goto done ;
   }

   BOOLEAN _optAccessPlan::Reusable ( const BSONObj &query,
                                      const BSONObj &orderBy,
                                      const BSONObj &hint ) const
   {
      if ( !_isValid )
         return FALSE ;
      // user query must be identical
      if ( !_query.shallowEqual ( query ) )
         return FALSE ;

      // order by must be identical
      if ( !_orderBy.shallowEqual ( orderBy ) )
         return FALSE ;

      // hint must be identical
      if ( !_hint.shallowEqual ( hint ) )
         return FALSE ;

      return TRUE ;
   }

   void _optAccessPlan::release()
   {
      if ( _apm )
      {
         _apm->releasePlan(this) ;
      }
      else
      {
         SDB_OSS_DEL this ;
      }
   }
}

