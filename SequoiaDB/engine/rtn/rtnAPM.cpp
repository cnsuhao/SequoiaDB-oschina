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
namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPL_INVALIDATE, "_rtnAccessPlanList::invalidate" )
   void _rtnAccessPlanList::invalidate ()
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPL_INVALIDATE );
      vector<optAccessPlan *>::iterator it ;
      RTNAPL_XLOCK
      // check if the plan is in the list
      for ( it = _plans.begin(); it != _plans.end(); )
      {
         // if so let's decrease the count
         if ( (*it)->getCount() == 0 )
         {
            optAccessPlan *tmp = (*it) ;
            SDB_OSS_DEL tmp ;
            // in vector, erase returns the next valid iterator, or
            // vector::end()
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

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPL_GETPLAN, "_rtnAccessPlanList::getPlan" )
   INT32 _rtnAccessPlanList::getPlan ( const BSONObj &query,
                                       const BSONObj &orderBy,
                                       const BSONObj &hint,
                                       optAccessPlan **out,
                                       BOOLEAN &incSize )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNACCESSPL_GETPLAN );
      SDB_ASSERT ( out, "out can't be NULL" ) ;
      (*out) = NULL ;
      vector<optAccessPlan *>::iterator it ;
      {
         RTNAPL_XLOCK
         // check if the plan is in the list
         for ( it = _plans.begin(); it != _plans.end(); ++it )
         {
            if ( (*it)->Reusable ( query, orderBy, hint )  )
            {
               // we found one plan match, then let's delete the plan we just
               // created and return the existing one
               incSize = FALSE ;
               *out = *it ;
               // if it's not the first one, let's remove it and re-add to begin
               // of the list
               if ( it != _plans.begin() )
               {
                  _plans.erase(it) ;
                  _plans.insert ( _plans.begin(), *out ) ;
               }
               // increase usage count
               (*out)->incCount() ;
               goto done ;
            }
         }
         // first create an uninitialized plan
         *out = SDB_OSS_NEW optAccessPlan ( _su, _collectionName, query,
                                            orderBy, hint ) ;
         if ( !(*out) )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "not able to allocate memory for new plan" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         // set the parent of plan apm to this apm
         (*out)->setAPM ( _apm ) ;
         // if the plan is not in the list, let's try to optimize it
         rc = (*out)->optimize() ;
         PD_RC_CHECK ( rc, (SDB_RTN_INVALID_PREDICATES==rc)?PDINFO:PDERROR,
                       "Failed to optimize plan, query: %s\norder %s\nhint %s",
                       query.toString().c_str(),
                       orderBy.toString().c_str(),
                       hint.toString().c_str() ) ;
         // now we have to insert it into the list, let's set incSize = TRUE for
         // now
         incSize = TRUE ;
         // now let's see how many plans we have, if so let's attempt to remove
         // any plan that not been used
         if ( _plans.size() >= RTN_APL_SIZE )
         {
            vector<optAccessPlan *>::reverse_iterator rit ;
            for ( rit = _plans.rbegin(); rit != _plans.rend(); rit++ )
            {
               if ( (*rit)->getCount() == 0 )
               {
                  // we can remove existing, let's reset incSize back to FALSE
                  incSize = FALSE ;
                  SDB_OSS_DEL (*rit) ;

                  vector<optAccessPlan *>::iterator tempIter = _plans.erase (
                     --rit.base() ) ;
                  rit = vector<optAccessPlan *>::reverse_iterator(tempIter) ;
               }
            }
         }
         // if the list still full, let's dump warning message
         // otherwise insert the new plan into plan list
         if ( _plans.size() >= RTN_APL_SIZE )
         {
            pdLog ( PDWARNING, __FUNC__, __FILE__, __LINE__,
                    "accessPlanList is full" ) ;
         }
         else
         {
            _plans.insert ( _plans.begin(), (*out) ) ;
         }
         (*out)->incCount() ;
      }

   done :
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

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNACCESSPL_RELPL, "rtnAccessPlanList::releasePlan" )
   void rtnAccessPlanList::releasePlan ( optAccessPlan *plan )
   {
      PD_TRACE_ENTRY ( SDB_RTNACCESSPL_RELPL );
      vector<optAccessPlan *>::iterator it ;
      RTNAPL_SLOCK
      // check if the plan is in the list
      for ( it = _plans.begin(); it != _plans.end(); ++it )
      {
         // if the plan is already in cache, let's return without any change
         if ( *it == plan )
         {
            // decrease plan count
            plan->decCount () ;
            goto done ;
         }
      }
      // otherwise it's not in the list, let's simply delete the plan
      SDB_OSS_DEL plan ;
   done :
      PD_TRACE_EXIT ( SDB_RTNACCESSPL_RELPL );
      return ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPL_CLEAR, "_rtnAccessPlanList::clear" )
   void _rtnAccessPlanList::clear ()
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPL_CLEAR );
      vector<optAccessPlan *>::iterator it ;
      RTNAPL_XLOCK
      // check if the plan is in the list
      for ( it = _plans.begin(); it != _plans.end(); )
      {
         // if so let's decrease the count
         if ( (*it)->getCount() == 0 )
         {
            optAccessPlan *tmp = (*it) ;
            SDB_OSS_DEL tmp ;
            // in vector, erase returns the next valid iterator, or
            // vector::end()
            it = _plans.erase(it) ;
         }
         else
            ++it ;
      }
      PD_TRACE_EXIT ( SDB__RTNACCESSPL_CLEAR );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPS_INVALIDATE, "_rtnAccessPlanSet::invalidate" )
   void _rtnAccessPlanSet::invalidate ()
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPS_INVALIDATE );
      map<UINT32, rtnAccessPlanList *>::iterator it ;
      RTNAPS_SLOCK
      for ( it = _planLists.begin(); it != _planLists.end(); )
      {
         rtnAccessPlanList *list = (*it).second ;
         list->invalidate() ;
         ++it ;
      }
      PD_TRACE_EXIT ( SDB__RTNACCESSPS_INVALIDATE );
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPS_GETPLAN, "_rtnAccessPlanSet::getPlan" )
   INT32 _rtnAccessPlanSet::getPlan ( const BSONObj &query,
                                      const BSONObj &orderBy,
                                      const BSONObj &hint,
                                      optAccessPlan **out,
                                      BOOLEAN &incSize )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNACCESSPS_GETPLAN );
      UINT32 hash = optAccessPlan::hash ( query, orderBy, hint ) ;
      // let's try to remove emptys
      if ( _totalNum > RTN_APS_SIZE )
      {
         clear ( FALSE ) ;
      }
      {
         RTNAPS_SLOCK
         if ( _planLists.find ( hash ) != _planLists.end() )
         {
            // if the hash already exist, then let's get in and see
            rc = _planLists[hash]->getPlan ( query, orderBy, hint,
                                             out, incSize ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get plan, rc = %d", rc ) ;
               goto error ;
            }

            if ( incSize )
               ++_totalNum ;
            goto done ;
         }
      }
      {
         BOOLEAN newAlloc = FALSE ;
         RTNAPS_XLOCK
         // check again just in case someone else added after we release S
         // lock
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
         rc = planlist->getPlan ( query, orderBy, hint, out, incSize ) ;
         if ( rc )
         {
            // let's delete the newly created list
            if ( newAlloc )
            {
               SDB_OSS_DEL planlist ;
               _planLists.erase(hash) ;
            }
            PD_LOG ( PDERROR, "Failed to get plan, rc = %d", rc ) ;
            goto error ;
         }
         if ( incSize )
            ++_totalNum ;
         goto done ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__RTNACCESSPS_GETPLAN, rc );
      return rc ;
   error :
      incSize = FALSE ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPS_RELPL, "_rtnAccessPlanSet::releasePlan" )
   void _rtnAccessPlanSet::releasePlan ( optAccessPlan *plan )
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPS_RELPL );
      SDB_ASSERT ( plan, "plan can't be NULL" ) ;
      UINT32 hash = plan->hash () ;
      RTNAPS_SLOCK
      if ( _planLists.find ( hash ) == _planLists.end() )
      {
         // if the plan is not in the plan list
         SDB_OSS_DEL plan ;
         goto done ;
      }
      _planLists[hash]->releasePlan ( plan ) ;
   done :
      PD_TRACE_EXIT ( SDB__RTNACCESSPS_RELPL );
      return ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPS_CLEAR, "_rtnAccessPlanSet::clear" )
   void _rtnAccessPlanSet::clear( BOOLEAN full )
   {
      PD_TRACE_ENTRY ( SDB__RTNACCESSPS_CLEAR );
      map<UINT32, rtnAccessPlanList *>::iterator it ;
      RTNAPS_XLOCK
      for ( it = _planLists.begin(); it != _planLists.end(); )
      {
         rtnAccessPlanList *list = (*it).second ;
         INT32 preSize = list->size() ;
         list->clear() ;
         INT32 afterSize = list->size() ;
         _totalNum -= preSize - afterSize ;
         // clear empty list
         if ( afterSize == 0 )
         {
            SDB_OSS_DEL list ;
            _planLists.erase(it++) ;
         }
         else
         {
            ++it ;
         }
         // don't run too aggresive, try to clear things to 75%
         if ( !full && _totalNum < RTN_APS_DFT_OCCUPY )
            goto done ;
      }
   done :
      PD_TRACE_EXIT ( SDB__RTNACCESSPS_CLEAR );
      return ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPLMAN_INVALIDATEPL, "_rtnAccessPlanManager::invalidatePlans" )
   void _rtnAccessPlanManager::invalidatePlans ( const CHAR *collectionName )
   {
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
         INT32 preSize = planset->size() ;
         planset->clear() ;
         INT32 afterSize = planset->size() ;
         _totalNum -= preSize - afterSize ;
         if ( afterSize == 0 )
         {
            _planSets.erase(it++) ;
            SDB_OSS_DEL planset ;
         }
         else
         {
            planset->invalidate () ;
            ++it ;
         }
      }
      PD_TRACE_EXIT ( SDB__RTNACCESSPLMAN_INVALIDATEPL );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPLMAN, "_rtnAccessPlanManager::getPlan" )
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
      if ( _totalNum > RTN_APM_SIZE )
      {
         clear ( FALSE ) ;
      }
      BOOLEAN incSize ;
      {
         RTNAPM_SLOCK
         if ( _planSets.find ( collectionName ) != _planSets.end() )
         {
            // if we are able to find the collection name
            rc = _planSets[collectionName]->getPlan ( query, orderBy,
                                                      hint, out, incSize ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get plan, rc = %d", rc ) ;
               goto error ;
            }
            if ( incSize )
               ++_totalNum ;
            goto done ;
         }
      } // S lock scope
      // if not able to find the collection
      {
         BOOLEAN newAlloc = FALSE ;
         RTNAPM_XLOCK
         // check again just in case someone else added after we release S
         // lock
         if ( _planSets.find ( collectionName ) == _planSets.end() )
         {
            CHAR *pCollectionName = NULL ;
            // memory will be freed in destructor or clear()
            rtnAccessPlanSet *planset = SDB_OSS_NEW rtnAccessPlanSet (_su,
                  collectionName, this ) ;
            if ( !planset )
            {
               pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                       "Failed to allocate memory for set" ) ;
               rc = SDB_OOM ;
               goto error ;
            }
            // get the _collectionName pointer from planset and save it in
            // _planSets.first
            pCollectionName = planset->getName() ;
            _planSets[pCollectionName] = planset ;
            newAlloc = TRUE ;
         }
         rtnAccessPlanSet *planset = _planSets[collectionName] ;
         SDB_ASSERT ( planset, "not able to find the planset" ) ;
         rc = planset->getPlan ( query, orderBy, hint, out, incSize ) ;
         if ( rc )
         {
            // clean up the currently allocated set
            if ( newAlloc )
            {
                // make sure to erase first because planset.first is pointing
                // to _collectionName in the rtnAccessPlanSet, so we need to
                // make sure during erase comparion, the object is still
                // avaliable
                _planSets.erase(collectionName) ;
                SDB_OSS_DEL planset ;
            }
            PD_LOG ( PDERROR, "Failed to get plan, rc = %d", rc ) ;
            goto error ;
         }
         if ( incSize )
            ++_totalNum ;
         goto done ;
      } // X lock scope
   done :
      PD_TRACE_EXITRC ( SDB__RTNACCESSPLMAN, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPLMAN_RELPL, "_rtnAccessPlanManager::releasePlan" )
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
      // if the collection no longer exist, we simply delete the plan
      if ( (it = _planSets.find(pCollectionName) ) == _planSets.end() )
      {
         SDB_OSS_DEL plan ;
         goto done ;
      }
      (*it).second->releasePlan ( plan ) ;
   done :
      PD_TRACE_EXIT ( SDB__RTNACCESSPLMAN_RELPL );
      return ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNACCESSPLMAN_CLEAR, "_rtnAccessPlanManager::clear" )
   void _rtnAccessPlanManager::clear ( BOOLEAN full )
   {
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
         INT32 preSize = planset->size() ;
         planset->clear() ;
         INT32 afterSize = planset->size() ;
         _totalNum -= preSize - afterSize ;
         // clear empty list
         if ( afterSize == 0 )
         {
            _planSets.erase(it++) ;
            SDB_OSS_DEL planset ;
         }
         else
         {
            ++it ;
         }
         if ( !full && _totalNum < RTN_APM_DFT_OCCUPY )
            goto done ;
      }
   done :
      PD_TRACE_EXIT ( SDB__RTNACCESSPLMAN_CLEAR );
      return ;
   }

}

