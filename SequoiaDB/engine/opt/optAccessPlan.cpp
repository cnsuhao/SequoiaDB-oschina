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
      while ( it.more() )
      {
         BSONElement hint = it.next() ;
         if ( hint.type() == String )
         {
            PD_LOG ( PDDEBUG, "Try to use index: %s", hint.valuestr() ) ;
            rc = _optimizeHint ( mbContext, hint.valuestr(), predSet ) ;
         }
         else if ( hint.type() == jstOID )
         {
            PD_LOG ( PDDEBUG, "Try to use index: %s",
                     _hint.toString().c_str() ) ;
            rc = _optimizeHint ( mbContext, hint.__oid(), predSet ) ;
         }
         else if ( hint.type() == jstNULL )
         {
            PD_LOG ( PDDEBUG, "Use Collection Scan by Hint" ) ;
            _scanType = TBSCAN ;
            rc = SDB_OK ;
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

   #define TEMP_COST_BASELINE 10000

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
         costEstimation = 10*TEMP_COST_BASELINE ;

         BSONObj idxPattern = indexCB.keyPattern() ;
         Ordering keyorder = Ordering::make(idxPattern) ;
         Ordering orderByorder = Ordering::make ( _orderBy ) ;
         INT32 nFields = _orderBy.nFields() ;
         INT32 nQueryFields = 0 ;
         INT32 matchedFields = 0 ;
         BSONObjIterator keyItr (idxPattern) ;
         BSONObjIterator orderItr ( _orderBy ) ;
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
               if ( start )
               {
                  dir = (keyorder.get(matchedFields) ==
                         orderByorder.get(matchedFields))?1:-1 ;
                  start = FALSE ;
               }
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
            if (( it = predicates.find( keyEle.fieldName() ))
                  != predicates.end() )
            {
               startStopKey = it->second._startStopKeys[0] ;
               startKey = startStopKey._startKey._bound ;
               stopKey = startStopKey._stopKey._bound ;
               
               if(0 == startKey.woCompare( bson::minKey.firstElement() ) &&
                  0 == stopKey.woCompare( bson::maxKey.firstElement() ))
               {
                  break;
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

   INT32 _optAccessPlan::_checkOrderBy()
   {
      INT32 rc = SDB_OK ;
      BSONObjIterator iter( _orderBy ) ;
      while ( iter.more() )
      {
         BSONElement ele = iter.next() ;
         INT32 value ;
         if ( ossStrcasecmp( ele.fieldName(), "" ) == 0 )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "orderBy's fieldName can't be empty:rc=%d", rc ) ;
            goto error ;
         }

         if ( !ele.isNumber() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "orderBy's value must be numberic:rc=%d", rc ) ;
            goto error ;
         }

         value = ele.numberInt() ;
         if ( value != 1 && value != -1 )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "orderBy's value must be 1 or -1:rc=%d", rc ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__OPTACCPLAN_OPT, "_optAccessPlan::optimize" )
   INT32 _optAccessPlan::optimize()
   {
      INT32 rc = SDB_OK ;
      dmsMBContext *mbContext = NULL ;

      PD_TRACE_ENTRY ( SDB__OPTACCPLAN_OPT ) ;

      rc = _checkOrderBy() ;
      PD_RC_CHECK( rc, PDERROR, "failed to check orderby", rc ) ;

      rc = _matcher.loadPattern ( _query ) ;
      PD_RC_CHECK ( rc, (SDB_RTN_INVALID_PREDICATES==rc) ? PDINFO : PDERROR,
                    "Failed to load query, rc = %d", rc ) ;

      {
         const rtnPredicateSet &predSet = _matcher.getPredicateSet () ;
         rc = _su->data()->getMBContext( &mbContext, _collectionName, SHARED ) ;
         PD_RC_CHECK( rc, PDERROR, "Get dms mb context failed, rc: %d", rc ) ;

         if ( _hint.isEmpty() || SDB_OK != _optimizeHint( mbContext, predSet ) )
         {
            _isAutoPlan = TRUE ;
            dmsExtentID bestMatchedIndexCBExtent = DMS_INVALID_EXTENT ;
            INT64 bestCostEstimation = 0 ;
            INT32 bestMatchedIndexDirection = 1 ;
            _estimateDetail detail ;

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
                     if ( bestCostEstimation == 0 )
                     {
                        break ;
                     }
                  }
               }
            }
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
               if ( !_orderBy.isEmpty() )
               {
                  _sortRequired = TRUE ;
               }
            }
         }

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
      if ( !_query.shallowEqual ( query ) )
         return FALSE ;

      if ( !_orderBy.shallowEqual ( orderBy ) )
         return FALSE ;

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

