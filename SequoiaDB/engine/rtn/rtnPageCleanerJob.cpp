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

   Source File Name = rtnPageCleaner.cpp

   Descriptive Name = Page cleaner implementation

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/04/2014  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "core.hpp"
#include "pmd.hpp"
#include "rtnPageCleanerJob.hpp"
#include "dms.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "dmsStorageUnit.hpp"

namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPAGECLEANERJOB_CONSTRUCTOR,"_rtnPageCleanerJob::_rtnPageCleanerJob" )
   _rtnPageCleanerJob::_rtnPageCleanerJob ( INT32 periodTime ):
   _periodTime ( periodTime )
   {
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPAGECLEANERJOB_DESTRUCTOR,"_rtnPageCleanerJob::~_rtnPageCleanerJob" )
   _rtnPageCleanerJob::~_rtnPageCleanerJob ()
   {
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPAGECLEANERJOB_TYPE, "_rtnPageCleanerJob::type" )
   RTN_JOB_TYPE _rtnPageCleanerJob::type () const
   {
      return RTN_JOB_PAGECLEANER ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPAGECLEANERJOB_NAME, "_rtnPageCleanerJob::name" )
   const CHAR* _rtnPageCleanerJob::name () const
   {
      return "Job[PageCleaner]" ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPAGECLEANERJOB_MUTEXON, "_rtnPageCleanerJob::muteXOn" )
   BOOLEAN _rtnPageCleanerJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPAGECLEANERJOB_DOIT, "_rtnPageCleanerJob::doit" )
   INT32 _rtnPageCleanerJob::doit ()
   {
      INT32 rc             = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNPAGECLEANERJOB_DOIT ) ;
      pmdKRCB *krcb        = pmdGetKRCB () ;
      pmdEDUMgr *eduMgr    = krcb->getEDUMgr () ;
      SDB_DMSCB *dmsCB     = krcb->getDMSCB () ;
      dmsStorageUnitID cleanSUID ;
      pmdEDUCB *cb         = eduCB() ;
      dmsStorageUnit *su   = NULL ;
      pmdEDUEvent event ;
      SDB_ASSERT ( krcb && eduMgr && dmsCB && cb,
                   "All control blocks can't be NULL" ) ;

      while ( !PMD_IS_DB_DOWN && !cb->isForced() )
      {
         // set EDU to wait status
         eduMgr->waitEDU ( cb->getID() ) ;
         cleanSUID = DMS_INVALID_SUID ;
         // dispatch the first storage unit in clean pending list
         // 1) suLock to lock the collection space, so that no one is able to
         // drop the cs during the time
         su = dmsCB->dispatchPageCleanSU ( &cleanSUID ) ;
         // if returns null, then sleep and wait
         if ( !su || DMS_INVALID_SUID == cleanSUID )
         {
            // wait any event may happen across this agent for _periodTime
            // milliseconds, if any events received ( such like termination ),
            // we just ignore the event and go back to loop
            cb->waitEvent ( event, _periodTime ) ;
            continue ;
         }
         eduMgr->activateEDU( cb->getID() ) ;
         // otherwise perform clean and reset the time
         // mark su's page cleaner is working, so that cs can't be destroyed
         // before page cleaner stop
         su->data()->lockPageCleaner() ;
         su->index()->lockPageCleaner() ;
         // unlock the cs
         dmsCB->suUnlock ( cleanSUID, SHARED ) ;
         // flush dirty pages
         su->data()->flushDirtySegments() ;
         // unlock page cleaner, so that the object can be destroyed
         su->data()->unlockPageCleaner () ;
         // flush dirty pages for index
         su->index()->flushDirtySegments() ;
         // unlock page cleaners
         su->index()->unlockPageCleaner () ;
         // if we can't lock the su, let's try to join the su back to list
         rc = dmsCB->joinPageCleanSU ( cleanSUID ) ;
         // joinPageCleanSU may fail with SDB_DMS_CS_NOTEXIST, and it's fully
         // expected
         if ( rc && SDB_DMS_CS_NOTEXIST != rc )
         {
            // we don't need to do anything if any other error happen, just log
            // it
            PD_LOG ( PDERROR,
                     "Failed to join su ( %d ) back to history list, rc = %d",
                     cleanSUID, rc ) ;
         }
         cb->incEventCount() ;
      }
      PD_TRACE_EXITRC ( SDB_RTNPAGECLEANERJOB_DOIT, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_STARTPAGECLEANERJOB, "startPageCleanerJob" )
   INT32 startPageCleanerJob ( EDUID *pEDUID, INT32 periodTime )
   {
      INT32 rc                = SDB_OK ;
      rtnPageCleanerJob *pJob = NULL ;
      PD_TRACE_ENTRY ( SDB_STARTPAGECLEANERJOB ) ;
      // object will be freed in job manager destructor
      pJob = SDB_OSS_NEW rtnPageCleanerJob ( periodTime ) ;
      if ( !pJob )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "Failed to allocate memory for page cleaners" ) ;
         goto error ;
      }
      // if startJob failed, it will free the memory in the function
      // so we don't need to free memory when startJob failed
      rc = rtnGetJobMgr()->startJob ( pJob, RTN_JOB_MUTEX_NONE, pEDUID ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start page cleaner job, rc = %d",
                  rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_STARTPAGECLEANERJOB, rc ) ;
      return rc ;
   error :
      goto done ;
   }
}
