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

   Source File Name = rtnAPM.hpp

   Descriptive Name = RunTime Access Plan Manager Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains structure for Access
   Plan Manager, which is pooling access plans that has been used.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef RTNAPM_HPP__
#define RTNAPM_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "optAccessPlan.hpp"
#include "ossLatch.hpp"
#include <map>

using namespace std ;

namespace engine
{
   class _dmsStorageUnit ;
   class _rtnAccessPlanManager ;
#define RTN_APL_SIZE 5
   class _rtnAccessPlanList : public SDBObject
   {
   private :
   #ifdef RTNAPL_XLOCK
   #undef RTNAPL_XLOCK
   #endif
   #define RTNAPL_XLOCK ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
   #ifdef RTNAPL_SLOCK
   #undef RTNAPL_SLOCK
   #endif
   #define RTNAPL_SLOCK ossScopedLock _lock ( &_mutex, SHARED ) ;
      ossSpinSLatch _mutex ;
      vector<optAccessPlan *> _plans ;
      _dmsStorageUnit *_su ;
      CHAR *_collectionName ;
      _rtnAccessPlanManager *_apm ;
   public :
      explicit _rtnAccessPlanList ( _dmsStorageUnit *su, CHAR *collectionName,
                                    _rtnAccessPlanManager *apm )
      {
         _su = su ;
         _collectionName = collectionName ;
         _apm = apm ;
      }
      ~_rtnAccessPlanList()
      {
         vector<optAccessPlan *>::iterator it ;
         for ( it = _plans.begin(); it != _plans.end(); ++it )
         {
            SDB_OSS_DEL (*it) ;
         }
         _plans.clear() ;
      }

      void invalidate () ;
      INT32 getPlan ( const BSONObj &query, const BSONObj &orderBy,
                      const BSONObj &hint, optAccessPlan **out,
                      BOOLEAN &incSize ) ;

      void releasePlan ( optAccessPlan *plan ) ;

      INT32 size()
      {
         RTNAPL_SLOCK
         return _plans.size() ;
      }

      void clear () ;
   } ;
   typedef class _rtnAccessPlanList rtnAccessPlanList ;

#if defined (_DEBUG)
#define RTN_APS_SIZE 10
#else
#define RTN_APS_SIZE 50
#endif
#define RTN_APS_DFT_OCCUPY_PCT 0.75f
#define RTN_APS_DFT_OCCUPY (RTN_APS_SIZE*RTN_APS_DFT_OCCUPY_PCT)
   class _rtnAccessPlanSet : public SDBObject
   {
   private :
   #ifdef RTNAPS_XLOCK
   #undef RTNAPS_XLOCK
   #endif
   #define RTNAPS_XLOCK ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
   #ifdef RTNAPS_SLOCK
   #undef RTNAPS_SLOCK
   #endif
   #define RTNAPS_SLOCK ossScopedLock _lock ( &_mutex, SHARED ) ;
      ossSpinSLatch _mutex ;
      INT32 _totalNum ;
      map<UINT32, rtnAccessPlanList *> _planLists ;
      _dmsStorageUnit *_su ;
      CHAR _collectionName [DMS_COLLECTION_NAME_SZ+1] ;
      _rtnAccessPlanManager *_apm ;
   public :
      explicit _rtnAccessPlanSet( _dmsStorageUnit *su,
                                  const CHAR *collectionName,
                                  _rtnAccessPlanManager *apm )
      {
         _totalNum = 0 ;
         ossMemset ( _collectionName, 0, sizeof(_collectionName)) ;
         ossStrncpy ( _collectionName, collectionName,
                      sizeof(_collectionName) ) ;
         _su = su ;
         _apm = apm ;
      }
      ~_rtnAccessPlanSet()
      {
         map<UINT32, rtnAccessPlanList *>::iterator it ;
         for ( it = _planLists.begin(); it != _planLists.end(); ++it )
         {
            SDB_OSS_DEL (*it).second ;
         }
         _planLists.clear() ;
      }
      void invalidate () ;
      INT32 getPlan ( const BSONObj &query, const BSONObj &orderBy,
                      const BSONObj &hint, optAccessPlan **out,
                      BOOLEAN &incSize ) ;

      void releasePlan ( optAccessPlan *plan ) ;

      INT32 size()
      {
         return _totalNum ;
      }
      void clear ( BOOLEAN full = TRUE ) ;
      CHAR *getName()
      {
         return _collectionName ;
      }
   } ;
   typedef class _rtnAccessPlanSet rtnAccessPlanSet ;

#if defined (_DEBUG)
#define RTN_APM_SIZE 20
#else
#define RTN_APM_SIZE 500
#endif
#define RTN_APM_DFT_OCCUPY_PCT 0.75f
#define RTN_APM_DFT_OCCUPY (RTN_APM_SIZE*RTN_APM_DFT_OCCUPY_PCT)
   class _rtnAccessPlanSet ;
   class _rtnAccessPlanManager : public SDBObject
   {
   private :
   #ifdef RTNAPM_XLOCK
   #undef RTNAPM_XLOCK
   #endif
   #define RTNAPM_XLOCK ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
   #ifdef RTNAPM_SLOCK
   #undef RTNAPM_SLOCK
   #endif
   #define RTNAPM_SLOCK ossScopedLock _lock ( &_mutex, SHARED ) ;
      ossSpinSLatch _mutex ;
      INT32 _totalNum ;
      struct cmp_str
      {
         bool operator() (const char *a, const char *b)
         {
            return std::strcmp(a,b)<0 ;
         }
      } ;
      _dmsStorageUnit *_su ;
      map<const CHAR*, _rtnAccessPlanSet*, cmp_str> _planSets ;
   public :
      explicit _rtnAccessPlanManager( _dmsStorageUnit *su )
      {
         _totalNum = 0 ;
         _su = su ;
      }
      ~_rtnAccessPlanManager()
      {
#if defined (_WINDOWS)
         map<const CHAR*, rtnAccessPlanSet*, cmp_str>::iterator it ;
#elif defined (_LINUX)
         map<const CHAR*, rtnAccessPlanSet*>::iterator it ;
#endif
         for ( it = _planSets.begin(); it != _planSets.end(); ++it )
         {
            SDB_OSS_DEL (*it).second ;
         }
         _planSets.clear() ;
      }
      void invalidatePlans ( const CHAR *collectionName ) ;
      INT32 getPlan ( const BSONObj &query, const BSONObj &orderBy,
                      const BSONObj &hint, const CHAR *collectionName,
                      optAccessPlan **out ) ;
      void releasePlan ( optAccessPlan *plan ) ;
      INT32 size()
      {
         return _totalNum ;
      }
      void clear ( BOOLEAN full = TRUE ) ;
   } ;
   typedef class _rtnAccessPlanManager rtnAccessPlanManager ;
}

#endif //RTNAPM_HPP__

