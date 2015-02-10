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

   Source File Name = optAccessPlan.hpp

   Descriptive Name = Optimizer Access Plan Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Optimizer component. This file contains structure for access
   plan, which is indicating how to run a given query.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef OPTACCESSPLAN_HPP__
#define OPTACCESSPLAN_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "rtnPredicate.hpp"
#include "dms.hpp"
#include "mthMatcher.hpp"
#include "ixm.hpp"
#include "ossAtomic.hpp"
#include "../bson/oid.h"
#include "../bson/bson.h"
#include "ossUtil.hpp"

namespace engine
{
   class _dmsMBContext ;
   class _dmsStorageUnit ;
   class _rtnAccessPlanManager ;

   enum optScanType
   {
      TBSCAN = 0,
      IXSCAN,
      UNKNOWNSCAN
   } ;

   class _optAccessPlan : public SDBObject
   {
   private:
      dmsExtentID _indexCBExtent ;
      dmsExtentID _indexLID ;

      OID _indexOID ;         // the oid for the index, for validation
      mthMatcher _matcher ;   // matcher that should be used by the plan
      rtnPredicateList *_predList ; // predicate list that generated from
      _dmsStorageUnit *_su ;        // pointer for the storage unit
      _rtnAccessPlanManager *_apm ; // parent access plan manager

      CHAR _collectionName[ DMS_COLLECTION_NAME_SZ+1 ] ;
      CHAR _idxName[IXM_KEY_MAX_SIZE + 1] ;

      BSONObj _orderBy ;            // order by called by the user
      BSONObj _query ;              // query condition called by the user
      BSONObj _hint ;               // hint called by the user
      BOOLEAN _hintFailed ;

      INT32 _direction ;            // direction called by the user

      optScanType _scanType ;
      BOOLEAN     _isInitialized ;
      BOOLEAN     _isValid ;

      BOOLEAN _isAutoPlan ;         // auto plan, TRUE when the plan is not
      UINT32 _hashValue ;
      ossAtomicSigned32 _useCount ;
      BOOLEAN _sortRequired ; // whether we need to explicit sort the resultset
   private:
      struct _estimateDetail
      {
         BOOLEAN matchAll ;

         _estimateDetail()
         :matchAll( FALSE )
         {

         }
      } ;

   private:
      INT32 _optimizeHint ( _dmsMBContext *mbContext,
                            const CHAR *pIndexName,
                            const rtnPredicateSet &predSet ) ;

      INT32 _optimizeHint ( _dmsMBContext *mbContext,
                            const OID &indexOID,
                            const rtnPredicateSet &predSet ) ;

      INT32 _optimizeHint( _dmsMBContext *mbContext,
                           const rtnPredicateSet &predSet ) ;

      INT32 _estimateIndex ( dmsExtentID indexCBExtent, INT64 &costEstimation,
                             INT32 &dir, _estimateDetail &detail ) ;

      INT32 _estimateIndex ( _dmsMBContext *mbContext, INT32 indexID,
                             INT64 &costEstimation, INT32 &dir,
                             dmsExtentID &indexCBExtent,
                             _estimateDetail &detail ) ;

      void _estimateTBScan ( INT64 &costEstimation ) ;

      INT32 _useIndex ( dmsExtentID indexCBExtent,
                        INT32 dir,
                        const rtnPredicateSet &predSet,
                        const _estimateDetail &detail ) ;

      INT32 _checkOrderBy() ;

   public :
      _optAccessPlan ( _dmsStorageUnit *su, const CHAR *collectionName,
                       const BSONObj &query, const BSONObj &orderBy,
                       const BSONObj &hint )
      :_useCount(0)
      {
         ossMemset( _idxName, 0, sizeof( _idxName ) ) ;
         ossMemset ( _collectionName, 0, sizeof(_collectionName) ) ;
         ossStrncpy ( _collectionName, collectionName,
                      sizeof(_collectionName) ) ;

         _isInitialized = FALSE ;
         _scanType = TBSCAN ;
         _indexCBExtent = DMS_INVALID_EXTENT ;
         _indexLID = DMS_INVALID_EXTENT ;
         _su = su ;
         _query = query.copy() ;
         _orderBy = orderBy.copy() ;
         _hint = hint.copy() ;
         _hintFailed = FALSE ;
         _predList = NULL ;
         _hashValue = hash(query, orderBy, hint) ;
         _apm = NULL ;
         _sortRequired = FALSE ;
         _isAutoPlan = FALSE ;
      }

      ~_optAccessPlan()
      {
         if ( _useCount.peek() != 0 )
         {
            PD_LOG ( PDWARNING, "Plan is deleted when use count is not 0: %d",
                     _useCount.peek() ) ;
         }
         if ( _predList )
         {
            SDB_OSS_DEL _predList ;
         }
      }

      optScanType getScanType()
      {
         SDB_ASSERT ( _isInitialized,
                      "optAccessPlan must be optimized before start using" ) ;
         return _scanType ;
      }

      void incCount( INT32 inc = 1 )
      {
         SDB_ASSERT ( _isInitialized,
                      "optAccessPlan must be optimized before start using" ) ;
         _useCount.add ( inc ) ;
      }

      void decCount( INT32 dec = 1 )
      {
         SDB_ASSERT ( _isInitialized,
                      "optAccessPlan must be optimized before start using" ) ;
         _useCount.sub ( dec ) ;
      }

      INT32 getCount()
      {
         SDB_ASSERT ( _isInitialized,
                      "optAccessPlan must be optimized before start using" ) ;
         return _useCount.peek() ;
      }

      BOOLEAN isHintFailed() const { return _hintFailed ; }

      INT32 optimize() ;

      mthMatcher &getMatcher()
      {
         SDB_ASSERT ( _isInitialized,
                      "optAccessPlan must be optimized before start using" ) ;
         return _matcher ;
      }

      rtnPredicateList *getPredList ()
      {
         SDB_ASSERT ( _isInitialized,
                      "optAccessPlan must be optimized before start using" ) ;
         return _predList ;
      }

      INT32 getDirection ()
      {
         SDB_ASSERT ( _isInitialized,
                      "optAccessPlan must be optimized before start using" ) ;
         return _direction ;
      }

      dmsExtentID getIndexCBExtent () const
      {
         return _indexCBExtent ;
      }

      const CHAR *getIndexName() const
      {
         return _idxName ;
      }

      dmsExtentID getIndexLID () const
      {
         return _indexLID ;
      }

      BOOLEAN Reusable ( const BSONObj &query, const BSONObj &orderBy,
                         const BSONObj &hint ) const ;

      UINT32 hash ()
      {
         return _hashValue ;
      }

      static UINT32 hash ( const BSONObj &query, const BSONObj &orderBy,
                           const BSONObj &hint )
      {
         return ossHash ( query.objdata(), query.objsize() ) ^
                ossHash ( orderBy.objdata(), orderBy.objsize() ) ^
                ossHash ( hint.objdata(), hint.objsize() ) ;
      }

      const CHAR *getName()
      {
         return _collectionName ;
      }

      void setAPM ( _rtnAccessPlanManager *apm )
      {
         _apm = apm ;
      }

      _rtnAccessPlanManager *getAPM()
      {
         return _apm ;
      }

      void release() ;

      BOOLEAN sortRequired()
      {
         return _sortRequired ;
      }

      void setValid ( BOOLEAN valid )
      {
         _isValid = valid ;
      }

      BOOLEAN getValid ()
      {
         return _isValid ;
      }

      BOOLEAN isAutoGen ()
      {
         return _isAutoPlan ;
      }

   } ;
   typedef class _optAccessPlan optAccessPlan ;

}

#endif //OPTACCESSPLAN_HPP__

