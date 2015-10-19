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

   Source File Name = rtnAPM.cpp

   Descriptive Name = Runtime Access Plan Manager

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime Access Plan
   Manager, which is used to pool access plans that previously generated.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "rtnAPM.hpp"
#include "dmsStorageUnit.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "pmd.hpp"
namespace engine
{
   static INT32 createNewPlan( _dmsStorageUnit *su,
                               const CHAR *name,
                               const BSONObj &query,
                               const BSONObj &orderBy,
                               const BSONObj &hint,
                               optAccessPlan **out )
   {
      INT32 rc = SDB_OK ;
      *out = SDB_OSS_NEW optAccessPlan ( su, name, query,
                                         orderBy, hint ) ;
      if ( !(*out) )
      {
         PD_LOG ( PDERROR, "Not able to allocate memory for new plan" ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      (*out)->setAPM ( NULL ) ;
      rc = (*out)->optimize() ;
      PD_RC_CHECK ( rc, (SDB_RTN_INVALID_PREDICATES==rc)?PDINFO:PDERROR,
                    "Failed to optimize plan, query: %s\norder %s\nhint %s",
                    query.toString().c_str(),
                    orderBy.toString().c_str(),
                    hint.toString().c_str() ) ;
   done:
      return rc ;
   error:
      if ( NULL != *out )
      {
         SDB_OSS_DEL (*out) ;
         (*out) = NULL ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPL_INVALIDATE, "_rtnAccessPlanList::invalidate" )
   void _rtnAccessPlanList::invalidate ( UINT32 &cleanNum )
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPL_INVALIDATE );
      cleanNum = 0 ;
      vector<optAccessPlan *>::iterator it ;
      RTNAPL_XLOCK
      for ( it = _plans.begin(); it != _plans.end(); )
      {
         if ( (*it)->getCount() == 0 )
         {
            ++cleanNum ;
            optAccessPlan *tmp = (*it) ;
            SDB_OSS_DEL tmp ;
            it = _plans.erase(it) ;
         }
         else
         {
            (*it)->setValid ( FALSE ) ;
            ++it ;
         }
      }
      PD_TRACE_EXIT ( SDB__RTNACCESSPL_INVALIDATE );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPL_GETPLAN, "_rtnAccessPlanList::getPlan" )
   INT32 _rtnAccessPlanList::getPlan ( const BSONObj &query,
                                       const BSONObj &orderBy,
                                       const BSONObj &hint,
                                       optAccessPlan **out,
                                       SINT32 &incCount )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN hasDel = FALSE ;
      PD_TRACE_ENTRY ( SDB__RTNACCESSPL_GETPLAN );
      SDB_ASSERT ( out, "out can't be NULL" ) ;
      (*out) = NULL ;
      SINT32 inc = 0 ;
      vector<optAccessPlan *>::iterator it ;
      {
         RTNAPL_XLOCK
         for ( it = _plans.begin(); it != _plans.end(); ++it )
         {
            if ( (*it)->Reusable ( query, orderBy, hint )  )
            {
               *out = *it ;
               if ( it != _plans.begin() )
               {
                  _plans.erase(it) ;
                  _plans.insert ( _plans.begin(), *out ) ;
               }
               (*out)->incCount() ;
               goto done ;
            }
         }
      }

      rc = createNewPlan( _su, _collectionName,
                          query, orderBy, hint,
                          out ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to create new plan:%d", rc ) ;
         goto error ;
      }

      (*out)->setAPM ( _apm ) ;
      inc = 1 ;

      {
         RTNAPL_XLOCK
         if ( _plans.size() >= RTN_APL_SIZE )
         {
            vector<optAccessPlan *>::reverse_iterator rit ;
            for ( rit = _plans.rbegin() ; rit != _plans.rend(); )
            {
               if ( (*rit)->getCount() == 0 )
               {
                  inc = 0 ;
                  SDB_OSS_DEL (*rit) ;
                  _plans.erase( (++rit).base() ) ;
                  hasDel = TRUE ;
                  break ;
               }
               else
               {
                  ++rit ;
               }
            }
         }
         if ( !hasDel && _plans.size() >= RTN_APL_SIZE )
         {
            inc = 0 ;
            (*out)->setAPM ( NULL ) ;
            PD_LOG ( PDWARNING, "AccessPlanList is full" ) ;
         }
         else
         {
            _plans.insert ( _plans.begin(), (*out) ) ;
            (*out)->incCount() ;
         }
      }

   done :
      incCount = inc ;
      PD_TRACE_EXITRC ( SDB__RTNACCESSPL_GETPLAN, rc );
      return rc ;
   error :
      if ( (*out) )
      {
         SDB_OSS_DEL (*out) ;
         (*out) = NULL ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNACCESSPL_RELPL, "rtnAccessPlanList::releasePlan" )
   void _rtnAccessPlanList::releasePlan ( optAccessPlan *plan )
   {
      PD_TRACE_ENTRY ( SDB_RTNACCESSPL_RELPL );
      vector<optAccessPlan *>::iterator it ;
      RTNAPL_SLOCK
      for ( it = _plans.begin(); it != _plans.end(); ++it )
      {
         if ( *it == plan )
         {
            plan->decCount () ;
            goto done ;
         }
      }
      PD_LOG( PDERROR, "Access plan[%s] is not found in vector",
              plan->toString().c_str() ) ;
      SDB_OSS_DEL plan ;
   done :
      PD_TRACE_EXIT ( SDB_RTNACCESSPL_RELPL );
      return ;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPL_CLEAR, "_rtnAccessPlanList::clear" )
   void _rtnAccessPlanList::clear ( UINT32 &cleanNum )
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPL_CLEAR );
      cleanNum = 0 ;
      vector<optAccessPlan *>::iterator it ;
      RTNAPL_XLOCK
      for ( it = _plans.begin(); it != _plans.end(); )
      {
         if ( (*it)->getCount() == 0 )
         {
            ++cleanNum ;
            optAccessPlan *tmp = (*it) ;
            SDB_OSS_DEL tmp ;
            it = _plans.erase(it) ;
         }
         else
            ++it ;
      }
      PD_TRACE_EXIT ( SDB__RTNACCESSPL_CLEAR );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPS_INVALIDATE, "_rtnAccessPlanSet::invalidate" )
   void _rtnAccessPlanSet::invalidate ( UINT32 &cleanNum )
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPS_INVALIDATE );
      cleanNum = 0 ;
      map<UINT32, rtnAccessPlanList *>::iterator it ;
      RTNAPS_SLOCK
      for ( it = _planLists.begin(); it != _planLists.end(); )
      {
         UINT32 tempClean = 0 ;
         rtnAccessPlanList *list = (*it).second ;
         list->invalidate( tempClean ) ;
         ++it ;
         cleanNum += tempClean ;
         _totalNum.sub( tempClean ) ;
      }

      PD_TRACE_EXIT ( SDB__RTNACCESSPS_INVALIDATE );
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPS_GETPLAN, "_rtnAccessPlanSet::getPlan" )
   INT32 _rtnAccessPlanSet::getPlan ( const BSONObj &query,
                                      const BSONObj &orderBy,
                                      const BSONObj &hint,
                                      optAccessPlan **out,
                                      SINT32 &incCount )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNACCESSPS_GETPLAN );
      UINT32 hash = optAccessPlan::hash ( query, orderBy, hint ) ;
      SINT32 inc = 0 ;
      {
         RTNAPS_SLOCK
         map<UINT32, rtnAccessPlanList *>::iterator itr =
                                                 _planLists.find ( hash ) ;
         SINT32 tmp = 0 ;
         if ( _planLists.end() != itr )
         {
            rtnAccessPlanList *planList = itr->second ;
            rc = planList->getPlan ( query, orderBy, hint,
                                     out, tmp ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get plan, rc = %d", rc ) ;
               goto error ;
            }

            if ( 0 != tmp )
            {
               inc += tmp ;
               _totalNum.add( tmp ) ;
            }
            goto done ;
         }
      }
      {
         BOOLEAN newAlloc = FALSE ;
         SINT32 tmp = 0 ;
         RTNAPS_XLOCK
         if ( _planLists.find ( hash ) == _planLists.end() )
         {
            rtnAccessPlanList *list = SDB_OSS_NEW rtnAccessPlanList (
                  _su, _collectionName, _apm ) ;
            if ( !list )
            {
               PD_LOG ( PDERROR, "Failed to allocate memory for list" ) ;
               rc = SDB_OOM ;
               goto error ;
            }
            _planLists[hash] = list ;
            newAlloc = TRUE ;
         }
         rtnAccessPlanList *planlist = _planLists[hash] ;
         SDB_ASSERT( planlist, "not able to find the planlist" ) ;
         rc = planlist->getPlan ( query, orderBy, hint, out, tmp ) ;
         if ( rc )
         {
            if ( newAlloc )
            {
               SDB_OSS_DEL planlist ;
               _planLists.erase(hash) ;
            }
            PD_LOG ( PDERROR, "Failed to get plan, rc = %d", rc ) ;
            goto error ;
         }
         if ( 0 != tmp )
         {
            inc += tmp ;
            _totalNum.add( tmp ) ;
         }
         goto done ;
      }

   done :
      if ( SDB_OK == rc &&
           0 < inc &&
           _totalNum.peek() > RTN_APS_SIZE )
      {
         UINT32 cleanNum = 0 ;
         clear ( cleanNum, FALSE ) ;
         inc -= cleanNum ;
      }
      incCount = inc ;
      PD_TRACE_EXITRC ( SDB__RTNACCESSPS_GETPLAN, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPS_RELPL, "_rtnAccessPlanSet::releasePlan" )
   void _rtnAccessPlanSet::releasePlan ( optAccessPlan *plan )
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPS_RELPL );
      SDB_ASSERT ( plan, "plan can't be NULL" ) ;
      UINT32 hash = plan->hash () ;
      RTNAPS_SLOCK
      if ( _planLists.find ( hash ) == _planLists.end() )
      {
         PD_LOG( PDERROR, "Access plan[%s] is not found is lists",
                 plan->toString().c_str() ) ;
         SDB_OSS_DEL plan ;
         goto done ;
      }
      _planLists[hash]->releasePlan ( plan ) ;
   done :
      PD_TRACE_EXIT ( SDB__RTNACCESSPS_RELPL );
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPS_CLEAR, "_rtnAccessPlanSet::clear" )
   void _rtnAccessPlanSet::clear( UINT32 &cleanNum, BOOLEAN full )
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPS_CLEAR );
      cleanNum = 0 ;
      map<UINT32, rtnAccessPlanList *>::iterator it ;
      RTNAPS_XLOCK
      for ( it = _planLists.begin(); it != _planLists.end(); )
      {
         UINT32 tempClean = 0 ;
         rtnAccessPlanList *list = (*it).second ;
         list->clear( tempClean ) ;
         _totalNum.sub( tempClean ) ;
         cleanNum += tempClean ;
         if ( list->size() == 0 )
         {
            SDB_OSS_DEL list ;
            _planLists.erase(it++) ;
         }
         else
         {
            ++it ;
         }
         if ( !full && _totalNum.peek() < RTN_APS_DFT_OCCUPY )
         {
            break ;
         }
      }

      PD_TRACE_EXIT ( SDB__RTNACCESSPS_CLEAR );
      return ;
   }

   _rtnAccessPlanManager::_rtnAccessPlanManager( _dmsStorageUnit *su )
   :_totalNum( 0 ),
    _su( su ),
    _bucketsNum( 0 )
    
   {
      _bucketsNum = pmdGetOptionCB()->getPlanBuckets() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPLMAN_INVALIDATEPL, "_rtnAccessPlanManager::invalidatePlans" )
   void _rtnAccessPlanManager::invalidatePlans ( const CHAR *collectionName )
   {
      UINT32 cleanNum = 0 ;
#if defined (_WINDOWS)
      map<const CHAR*, rtnAccessPlanSet*, cmp_str>::iterator it ;
#elif defined (_LINUX)
      map<const CHAR*, rtnAccessPlanSet*>::iterator it ;
#endif
      PD_TRACE_ENTRY ( SDB__RTNACCESSPLMAN_INVALIDATEPL );
      RTNAPM_XLOCK
      if ( (it = _planSets.find(collectionName) ) != _planSets.end() )
      {
         rtnAccessPlanSet *planset = (*it).second ;
         planset->invalidate( cleanNum ) ;
         _totalNum.sub( cleanNum ) ;
         if ( planset->size() == 0 )
         {
            _planSets.erase(it++) ;
            SDB_OSS_DEL planset ;
         }
         else
         {
            ++it ;
         }
      }
      PD_TRACE_EXIT ( SDB__RTNACCESSPLMAN_INVALIDATEPL );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPLMAN, "_rtnAccessPlanManager::getPlan" )
   INT32 _rtnAccessPlanManager::getPlan ( const BSONObj &query,
                                          const BSONObj &orderBy,
                                          const BSONObj &hint,
                                          const CHAR *collectionName,
                                          optAccessPlan **out )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNACCESSPLMAN );
      SDB_ASSERT ( collectionName, "collection name can't be NULL" ) ;
      SDB_ASSERT ( out, "out can't be NULL" ) ;
      SINT32 incCount = 0 ;

      if ( 0 == _bucketsNum )
      {
         rc = createNewPlan( _su, collectionName,
                             query, orderBy,
                             hint, out ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to create new plan:%d", rc ) ;
            goto error ;
         }
         goto done ;
      }

      {
         RTNAPM_SLOCK
         PLAN_SETS_ITERATOR itr = _planSets.find ( collectionName ) ;
         if ( _planSets.end() != itr )
         {
            _rtnAccessPlanSet *plan = itr->second ;
            rc = plan->getPlan ( query, orderBy,
                                 hint, out, incCount ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get plan, rc = %d", rc ) ;
               goto error ;
            }
            if ( 0 != incCount )
            {
               _totalNum.add( incCount ) ;
            }
            goto done ;
         }
      } // S lock scope
      {
         BOOLEAN newAlloc = FALSE ;
         RTNAPM_XLOCK
         if ( _planSets.find ( collectionName ) == _planSets.end() )
         {
            CHAR *pCollectionName = NULL ;
            rtnAccessPlanSet *planset = SDB_OSS_NEW rtnAccessPlanSet (_su,
                  collectionName, this ) ;
            if ( !planset )
            {
               PD_LOG ( PDERROR, "Failed to allocate memory for set" ) ;
               rc = SDB_OOM ;
               goto error ;
            }
            pCollectionName = planset->getName() ;
            _planSets[pCollectionName] = planset ;
            newAlloc = TRUE ;
         }
         rtnAccessPlanSet *planset = _planSets[collectionName] ;
         SDB_ASSERT ( planset, "not able to find the planset" ) ;
         rc = planset->getPlan ( query, orderBy, hint, out, incCount ) ;
         if ( rc )
         {
            if ( newAlloc )
            {
                _planSets.erase(collectionName) ;
                SDB_OSS_DEL planset ;
            }
            PD_LOG ( PDERROR, "Failed to get plan, rc = %d", rc ) ;
            goto error ;
         }
         if ( 0 != incCount )
         {
            _totalNum.add( incCount ) ;
         }
         goto done ;
      } // X lock scope

   done :
      PD_TRACE_EXITRC ( SDB__RTNACCESSPLMAN, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPLMAN_RELPL, "_rtnAccessPlanManager::releasePlan" )
   void _rtnAccessPlanManager::releasePlan ( optAccessPlan *plan )
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPLMAN_RELPL );
      SDB_ASSERT ( plan, "plan can't be NULL" ) ;
      SDB_ASSERT ( plan->getAPM() == this,
                   "the owner of plan is not this APM" ) ;
#if defined (_WINDOWS)
      map<const CHAR*, rtnAccessPlanSet*, cmp_str>::iterator it ;
#elif defined (_LINUX)
      map<const CHAR*, rtnAccessPlanSet*>::iterator it ;
#endif
      const CHAR *pCollectionName = plan->getName() ;
      RTNAPM_SLOCK
      if ( (it = _planSets.find(pCollectionName) ) == _planSets.end() )
      {
         PD_LOG( PDERROR, "Access plan[%s] is not found in plan sets",
                 plan->toString().c_str() ) ;
         SDB_OSS_DEL plan ;
         goto done ;
      }
      (*it).second->releasePlan ( plan ) ;
   done :
      PD_TRACE_EXIT ( SDB__RTNACCESSPLMAN_RELPL );
      return ;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPLMAN_CLEAR, "_rtnAccessPlanManager::clear" )
   void _rtnAccessPlanManager::clear ( BOOLEAN full )
   {
      UINT32 cleanNum = 0 ;
      PD_TRACE_ENTRY ( SDB__RTNACCESSPLMAN_CLEAR );
#if defined (_WINDOWS)
      map<const CHAR*, rtnAccessPlanSet*, cmp_str>::iterator it ;
#elif defined (_LINUX)
      map<const CHAR*, rtnAccessPlanSet*>::iterator it ;
#endif
      RTNAPM_XLOCK
      for ( it = _planSets.begin(); it != _planSets.end(); )
      {
         rtnAccessPlanSet *planset = (*it).second ;
         planset->clear( cleanNum ) ;
         _totalNum.sub( cleanNum ) ;
         if ( planset->size() == 0 )
         {
            _planSets.erase(it++) ;
            SDB_OSS_DEL planset ;
         }
         else
         {
            ++it ;
         }
         if ( !full && _totalNum.peek() < RTN_APM_DFT_OCCUPY )
         {
            break ;
         }
      }

      PD_TRACE_EXIT ( SDB__RTNACCESSPLMAN_CLEAR );
      return ;
   }

}

