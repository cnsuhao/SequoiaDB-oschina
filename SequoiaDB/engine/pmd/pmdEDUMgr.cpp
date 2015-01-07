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

   Source File Name = pmdEDUMgr.cpp

   Descriptive Name = Process MoDel Engine Dispatchable Unit Manager

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for thread pooling.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
/*
 * EDU Status Transition Table
 * C: CREATING
 * R: RUNNING
 * W: WAITING
 * I: IDLE
 * D: DESTROY
 * c: createNewEDU
 * a: activateEDU
 * d: destroyEDU
 * w: waitEDU
 * t: deactivateEDU
 *   C   R   W   I   D  <--- from
 * C c
 * R a   -   a   a   -  <--- Create/Idle/Wait status can move to Running status
 * W -   w   -   -   -  <--- Running status move to Waiting
 * I t   -   t   -   -  <--- Creating/Waiting status move to Idle
 * D d   -   d   d   -  <--- Creating / Waiting / Idle can be destroyed
 * ^ To
 */
#include "core.hpp"
#include "pd.hpp"
#include "pmd.hpp"
#include "pmdEDUMgr.hpp"
#include "oss.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
namespace engine
{
   _pmdEDUMgr::_pmdEDUMgr() :
   _EDUID(1),
   _isQuiesced(FALSE),
   _isDestroyed(FALSE)
   {
   }

   _pmdEDUMgr::~_pmdEDUMgr()
   {
      reset () ;
   }

   void _pmdEDUMgr::addIOService( io_service * service )
   {
      EDUMGR_XLOCK
      _ioserviceList.push_back ( service ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_DELIOSVC, "_pmdEDUMgr::deleteIOService" )
   void _pmdEDUMgr::deleteIOService( io_service * service )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_DELIOSVC );
      std::vector<io_service*>::iterator it ;
      {
         EDUMGR_XLOCK
         for ( it = _ioserviceList.begin() ;
               it != _ioserviceList.end() ;
               it++ )
         {
            if ( (*it) == service )
            {
               _ioserviceList.erase ( it ) ;
               break ;
            }
         }
      }
      PD_TRACE_EXIT ( SDB__PMDEDUMGR_DELIOSVC );
   }

   void _pmdEDUMgr::reset()
   {
      destroyAll () ;
   }

#if defined( SDB_ENGINE )
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_DUMPINFO, "_pmdEDUMgr::dumpInfo" )
   void _pmdEDUMgr::dumpInfo ( std::set<monEDUSimple> &info )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_DUMPINFO );
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      {
         EDUMGR_SLOCK
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; it ++ )
         {
            monEDUSimple simple ;
            (*it).second->dumpInfo ( simple ) ;
            info.insert(simple) ;
         }
         for ( it = _idleQueue.begin () ; it != _idleQueue.end () ; it ++ )
         {
            monEDUSimple simple ;
            (*it).second->dumpInfo ( simple ) ;
            info.insert(simple) ;
         }
      }
      PD_TRACE_EXIT ( SDB__PMDEDUMGR_DUMPINFO );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_DUMPINFO2, "_pmdEDUMgr::dumpInfo" )
   void _pmdEDUMgr::dumpInfo ( std::set<monEDUFull> &info )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_DUMPINFO2 );
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      {
         EDUMGR_SLOCK
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; it ++ )
         {
            monEDUFull full ;
            (*it).second->dumpInfo ( full ) ;
            info.insert(full) ;
         }
         for ( it = _idleQueue.begin () ; it != _idleQueue.end () ; it ++ )
         {
            monEDUFull full ;
            (*it).second->dumpInfo ( full ) ;
            info.insert(full) ;
         }
      }
      PD_TRACE_EXIT ( SDB__PMDEDUMGR_DUMPINFO2 );
   }
#endif // SDB_ENGINE

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_DESTROYALL, "_pmdEDUMgr::destroyAll" )
   INT32 _pmdEDUMgr::destroyAll ()
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_DESTROYALL );
      setDestroyed ( TRUE ) ;
      setQuiesced ( TRUE ) ;

      //stop all ioservice
      while ( _getIOServiceCount() > 0 )
      {
         _forceIOService () ;
         ossSleepmillis ( 200 ) ;
      }

      //stop all user edus
      UINT32 timeCounter = 0 ;
      UINT32 eduCount = _getEDUCount ( EDU_USER ) ;

      while ( eduCount != 0 )
      {
         if ( 0 == timeCounter % 50 )
         {
            _forceEDUs ( EDU_USER ) ;
         }
         ++timeCounter ;
         ossSleepmillis ( 100 ) ;
         eduCount = _getEDUCount ( EDU_USER ) ;
      }

      //stop all system edus
      timeCounter = 0 ;
      eduCount = _getEDUCount ( EDU_ALL ) ;
      while ( eduCount != 0 )
      {
         if ( 0 == timeCounter % 50 )
         {
            _forceEDUs ( EDU_ALL ) ;
         }

         ++timeCounter ;
         ossSleepmillis ( 100 ) ;
         eduCount = _getEDUCount ( EDU_ALL ) ;
      }

      PD_TRACE_EXIT ( SDB__PMDEDUMGR_DESTROYALL );
      return SDB_OK ;
   }

   // force a specific EDU
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_FORCEUSREDU, "_pmdEDUMgr::forceUserEDU" )
   INT32 _pmdEDUMgr::forceUserEDU ( EDUID eduID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_FORCEUSREDU );
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      if ( isSystemEDU ( eduID ) )
      {
         PD_LOG ( PDERROR, "System EDU %d can't be forced", eduID ) ;
         rc = SDB_PMD_FORCE_SYSTEM_EDU ;
         goto error ;
      }
      {
         // critical section start
         EDUMGR_XLOCK
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; ++it )
         {
            if ( (*it).second->getID () == eduID )
            {
               (*it).second->force () ;
               goto done ;
            }
         }
         for ( it = _idleQueue.begin () ; it != _idleQueue.end () ; ++it )
         {
            if ( (*it).second->getID () == eduID )
            {
               (*it).second->force () ;
               goto done ;
            }
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_FORCEUSREDU, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _pmdEDUMgr::interruptUserEDU( EDUID eduID )
   {
      INT32 rc = SDB_OK ;
      if ( isSystemEDU( eduID ) )
      {
         PD_LOG( PDERROR, "can not interrupt a system edu:%lld",
                 eduID ) ;
         rc = SDB_PMD_FORCE_SYSTEM_EDU ;
         goto error ;
      }

      {
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      EDUMGR_XLOCK
      for ( it = _runQueue.begin () ; it != _runQueue.end () ; ++it )
      {
         if ( (*it).second->getID () == eduID )
         {
            (*it).second->interrupt() ;
            break ;
         }
      }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR__FORCEIOSVC, "_pmdEDUMgr::_forceIOService" )
   INT32 _pmdEDUMgr::_forceIOService ()
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR__FORCEIOSVC );
      std::vector<io_service*>::iterator it_service ;

      /*******************CRITICAL SECTION ********************/
      {
         EDUMGR_XLOCK
         for ( it_service = _ioserviceList.begin();
               it_service != _ioserviceList.end(); it_service++ )
         {
            (*it_service)->stop() ;
         }
      }
      /******************END CRITICAL SECTION******************/
      PD_TRACE_EXIT ( SDB__PMDEDUMGR__FORCEIOSVC );
      return SDB_OK ;
   }

   UINT32 _pmdEDUMgr::_getIOServiceCount ()
   {
      EDUMGR_SLOCK
      return (UINT32)_ioserviceList.size() ;
   }

   // block all new request and attempt to terminate existing requests
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR__FORCEEDUS, "_pmdEDUMgr::_forceEDUs" )
   INT32 _pmdEDUMgr::_forceEDUs ( INT32 property )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR__FORCEEDUS );
      std::map<EDUID, pmdEDUCB*>::iterator it ;

      /*******************CRITICAL SECTION ********************/
      {
         EDUMGR_XLOCK
         // send terminate request to everyone
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; ++it )
         {
            if ( ((EDU_SYSTEM & property) && _isSystemEDU( it->first ))
               || ((EDU_USER & property) && !_isSystemEDU( it->first )) )
            {
               ( *it ).second->force () ;
               PD_LOG ( PDDEBUG, "force edu[ID:%lld]", it->first ) ;
            }
         }

         for ( it = _idleQueue.begin () ; it != _idleQueue.end () ; ++it )
         {
            if ( EDU_USER & property )
            {
               ( *it ).second->force () ;
            }
         }
      }
      /******************END CRITICAL SECTION******************/
      PD_TRACE_EXIT ( SDB__PMDEDUMGR__FORCEEDUS );
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR__GETEDUCNT, "_pmdEDUMgr::_getEDUCount" )
   UINT32 _pmdEDUMgr::_getEDUCount ( INT32 property )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR__GETEDUCNT );
      UINT32 eduCount = 0 ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;

      /*******************CRITICAL SECTION ********************/
      {
         EDUMGR_XLOCK
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; ++it )
         {
            if ( ((EDU_SYSTEM & property) && _isSystemEDU( it->first ))
               || ((EDU_USER & property) && !_isSystemEDU( it->first )) )
            {
               ++eduCount ;
            }
         }

         for ( it = _idleQueue.begin () ; it != _idleQueue.end () ; ++it )
         {
            if ( EDU_USER & property )
            {
               ++eduCount ;
            }
         }
      }
      /******************END CRITICAL SECTION******************/
      PD_TRACE1 ( SDB__PMDEDUMGR__GETEDUCNT, PD_PACK_UINT(eduCount) );
      PD_TRACE_EXIT ( SDB__PMDEDUMGR__GETEDUCNT );
      return eduCount ;
   }

   INT32 _pmdEDUMgr::_interruptWritingEDUs()
   {
      std::map<EDUID, pmdEDUCB*>::iterator it ;

      /*******************CRITICAL SECTION ********************/
      {
         EDUMGR_XLOCK
         // send terminate request to everyone
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; ++it )
         {
            if ( (*it).second->isWritingDB() )
            {
               ( *it ).second->interrupt() ;
               PD_LOG ( PDDEBUG, "Interrupt edu[ID:%lld]", it->first ) ;
            }
         }
      }
      /******************END CRITICAL SECTION******************/
      return SDB_OK ;
   }

   UINT32 _pmdEDUMgr::_getWritingEDUCount ( INT32 eduTypeFilter,
                                            UINT64 timeThreshold )
   {
      UINT32 eduCount = 0 ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;

      /*******************CRITICAL SECTION ********************/
      {
         EDUMGR_XLOCK
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; ++it )
         {
            if ( (*it).second->isWritingDB() )
            {
               if ( -1 != eduTypeFilter &&
                    eduTypeFilter != (*it).second->getType() )
               {
                  continue ;
               }
               else if ( 0 != timeThreshold &&
                         (*it).second->getWritingTime() > timeThreshold )
               {
                  continue ;
               }
               ++eduCount ;
            }
         }
      }
      /******************END CRITICAL SECTION******************/
      return eduCount ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_PSTEDUPST, "_pmdEDUMgr::postEDUPost" )
   INT32 _pmdEDUMgr::postEDUPost ( EDUID eduID, pmdEDUEventTypes type,
                                   pmdEDUMemTypes dataMemType , void *pData )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_PSTEDUPST );
      pmdEDUCB* eduCB = NULL ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      {
         // shared lock the block, since we don't change anything
         EDUMGR_SLOCK
         if ( _runQueue.end () == ( it = _runQueue.find ( eduID )) )
         {
            // if we cannot find it in runqueue, we search for idle queue
            // note that during the time, we already have EDUMgr locked,
            // so thread cannot change queue from idle to run
            // that means we are safe to exame both queues
            if ( _idleQueue.end () == ( it = _idleQueue.find ( eduID )) )
            {
               // we can't find edu id anywhere
               rc = SDB_SYS ;
               goto error ;
            }
         }
      }
      eduCB = ( *it ).second ;
      eduCB->postEvent( pmdEDUEvent ( type, dataMemType, pData ) ) ;
   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_PSTEDUPST, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_WAITEDUPST, "_pmdEDUMgr::waitEDUPost" )
   INT32 _pmdEDUMgr::waitEDUPost ( EDUID eduID, pmdEDUEvent& event,
                                  INT64 millsecond = -1 )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_WAITEDUPST );
      pmdEDUCB* eduCB = NULL ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      // shared lock the block, since we don't change anything
      {
         EDUMGR_SLOCK
         if ( _runQueue.end () == ( it = _runQueue.find ( eduID )) )
         {
            // if we cannot find it in runqueue, we search for idle queue
            // note that during the time, we already have EDUMgr locked,
            // so thread cannot change queue from idle to run
            // that means we are safe to exame both queues
            if ( _idleQueue.end () == ( it = _idleQueue.find ( eduID )) )
            {
               // we can't find edu id anywhere
               rc = SDB_SYS ;
               goto error ;
            }
         }
         eduCB = ( *it ).second ;
         // wait for event. when millsecond is 0, it should always return TRUE
         if ( !eduCB->waitEvent( event, millsecond ) )
         {
            rc = SDB_TIMEOUT ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_WAITEDUPST, rc );
      return rc ;
   error :
      goto done ;
   }

   // release control from a given EDU
   // EDUMgr should decide whether put the EDU to pool or destroy it
   // EDU Status must be in waiting or creating
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_RTNEDU, "_pmdEDUMgr::returnEDU" )
   INT32 _pmdEDUMgr::returnEDU ( EDUID eduID, BOOLEAN force, BOOLEAN* destroyed )
   {
      INT32 rc        = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_RTNEDU );
      EDU_TYPES type  = EDU_TYPE_UNKNOWN ;
      pmdEDUCB *educb = NULL ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      // shared critical section
      _mutex.get_shared () ;
      if ( _runQueue.end() == ( it = _runQueue.find ( eduID ) ) )
      {
         if ( _idleQueue.end() == ( it = _idleQueue.find ( eduID ) ) )
         {
            rc = SDB_SYS ;
            *destroyed = FALSE ;
            _mutex.release_shared () ;
            goto error ;
         }
      }
      educb = (*it).second ;
      // if we are trying to destry EDU manager, or enforce destroy, or
      // if the total number of threads are more than what we need
      // we need to destroy this EDU
      //
      // Currentl we only able to pool agent and coordagent
      if ( educb )
      {
         type = educb->getType() ;
         educb->resetDisconnect () ;
      }
      _mutex.release_shared () ;


      // if the EDU type can't be pooled, or if we forced, or if the EDU is
      // destroied, or we exceed max pooled edus, let's destroy it
      if ( !isPoolable(type) || force || isDestroyed () || size () >=
           pmdGetOptionCB()->getMaxPooledEDU () )
      {
         rc = destroyEDU ( eduID ) ;
         if ( destroyed )
         {
            // we consider the EDU is destroyed when destroyEDU returns
            // OK or SDB_SYS (edu can't be found), so that thread can terminate
            // itself
            if ( SDB_OK == rc || SDB_SYS == rc )
               *destroyed = TRUE ;
            else
               *destroyed = FALSE ;
         }
      }
      else
      {
         // in this case, we don't need to care whether the EDU is agent or not
         // as long as we treat SDB_SYS as "destroyed" signal, we should be
         // safe here
         rc = deactivateEDU ( eduID ) ;
         if ( destroyed )
         {
            // when we try to pool the EDU, destroyed set to true only when
            // the EDU can't be found in the list
            if ( SDB_SYS == rc )
               *destroyed = TRUE ;
            else
               *destroyed = FALSE ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_RTNEDU, rc );
      return rc ;
   error :
      goto done ;
   }

   // get an EDU from idle pool, if idle pool is empty, create new one
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_STARTEDU, "_pmdEDUMgr::startEDU" )
   INT32 _pmdEDUMgr::startEDU ( EDU_TYPES type, void* arg, EDUID *eduid )
   {
      INT32     rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_STARTEDU );
      EDUID     eduID = 0 ;
      pmdEDUCB* eduCB = NULL ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;

      if ( isQuiesced () )
      {
         rc = SDB_QUIESCED ;
         goto done ;
      }
      /****************** CRITICAL SECTION **********************/
      // get exclusive latch, we don't latch the entire function
      // in order to avoid creating new thread while holding latch
      _mutex.get () ;
      // if there's any pooled EDU?
      // or is the request type can be pooled ?
      if ( TRUE == _idleQueue.empty () || !isPoolable ( type ) )
      {
         // note that EDU types other than "agent" shouldn't be pooled at all
         // release latch before calling createNewEDU
         _mutex.release () ;
         rc = createNewEDU ( type, arg, eduid ) ;
         if ( SDB_OK == rc )
            goto done ;
         goto error ;
      }

      // if we can find something in idle queue, let's get the first of it
      for ( it = _idleQueue.begin () ;
            ( _idleQueue.end () != it ) &&
            ( PMD_EDU_IDLE != ( *it ).second->getStatus ()) ;
            it ++ ) ;

      // if everything in idleQueue are in DESTROY status, we still need to
      // create a new EDU
      if ( _idleQueue.end () == it )
      {
         // release latch before calling createNewEDU
         _mutex.release () ;
         rc = createNewEDU ( type, arg, eduid  ) ;
         if ( SDB_OK == rc )
            goto done ;
         goto error ;
      }

      // now "it" is pointing to an idle EDU
      // note that all EDUs in the idleQueue should be AGENT type
      eduID = ( *it ).first ;
      eduCB = ( *it ).second ;
      _idleQueue.erase ( eduID ) ;
      SDB_ASSERT ( isPoolable ( type ),
                   "must be agent/coordagent/subagent" ) ;
      // switch agent type for the EDU ( call different agent entry point )
      eduCB->setType ( type ) ;
      eduCB->setStatus ( PMD_EDU_WAITING ) ;
      _runQueue [ eduID ] = eduCB ;
      *eduid = eduID ;
      //The edu is start, need post a resum event
      eduCB->clear() ;
      eduCB->postEvent( pmdEDUEvent( PMD_EDU_EVENT_RESUME,
                                     PMD_EDU_MEM_NONE, arg ) ) ;
      _mutex.release () ;
      /*************** END CRITICAL SECTION **********************/

   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_STARTEDU, rc );
      return rc ;
   error :
      goto done ;
   }

   // whoever calling this function should NOT get latch
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_CRTNEWEDU, "_pmdEDUMgr::createNewEDU" )
   INT32 _pmdEDUMgr::createNewEDU ( EDU_TYPES type, void* arg, EDUID *eduid )
   {
      INT32 rc       = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_CRTNEWEDU );
      UINT32 probe   = 0 ;
      pmdEDUCB *cb   = NULL ;
      EDUID myEDUID  = 0 ;
      if ( isQuiesced () )
      {
         rc = SDB_QUIESCED ;
         goto done ;
      }

      if ( !getEntryFuncByType ( type ) )
      {
         PD_LOG ( PDERROR, "The edu[type:%d] not exist or function is null", type ) ;
         rc = SDB_INVALIDARG ;
         probe = 30 ;
         goto error ;
      }

      cb = SDB_OSS_NEW pmdEDUCB ( this, type ) ;
      SDB_VALIDATE_GOTOERROR ( cb, SDB_OOM,
               "Out of memory to create agent control block" ) ;
      // set to creating status
      cb->setStatus ( PMD_EDU_CREATING ) ;

      /***********CRITICAL SECTION*********************/
      _mutex.get () ;
      // if the EDU exist in runqueue
      if ( _runQueue.end() != _runQueue.find ( _EDUID )  )
      {
         _mutex.release () ;
         rc = SDB_SYS ;
         probe = 10 ;
         goto error ;
      }
      // if the EDU exist in idle queue
      if ( _idleQueue.end() != _idleQueue.find ( _EDUID )  )
      {
         _mutex.release () ;
         rc = SDB_SYS ;
         probe = 15 ;
         goto error ;
      }
      // assign EDU id and increment global EDUID
      cb->setID ( _EDUID ) ;
      if ( eduid )
         *eduid = _EDUID ;
      // place cb into runqueue
      _runQueue [ _EDUID ] = ( pmdEDUCB* ) cb ;
      myEDUID = _EDUID ;
      ++_EDUID ;
      // post RESUME event BEFORE agent starts!
      // we have to do this BEFORE starting the agent thread because we need to
      // make sure the agent is always picking up RESUME event at first
      cb ->postEvent( pmdEDUEvent( PMD_EDU_EVENT_RESUME, PMD_EDU_MEM_NONE,
                                   arg ) ) ;
      _mutex.release () ;
      /***********END CRITICAL SECTION****************/

      // create a new thread here, pass agent CB and other arguments
      try
      {
         boost::thread agentThread ( pmdEDUEntryPointWrapper,
                                     type, cb, arg ) ;
         // detach the agent so that he's all on his own
         // we only track based on CB
         agentThread.detach () ;
      }
      catch ( std::exception &e )
      {
         // if we failed to create thread, make sure to clean runqueue
         PD_LOG ( PDSEVERE, "Failed to create new agent: %s",
                  e.what() ) ;
         _runQueue.erase ( myEDUID ) ;
         rc = SDB_SYS ;
         probe = 30 ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_CRTNEWEDU, rc );
      return rc ;
   error :
      // clean out memory if it's allocated
      if ( cb )
      {
         SDB_OSS_DEL cb ;
      }
      PD_LOG ( PDERROR, "Failed to create new agent, probe = %d", probe ) ;
      goto done ;
   }

   // this function must be called against a thread that
   // in either SDB_EDU_WAITING or SDB_EDU_IDLE status
   // return: SDB_OK -- success
   //         SDB_EDU_INVAL_STATUS -- edu status is not destroy
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_DSTEDU, "_pmdEDUMgr::destroyEDU" )
   INT32 _pmdEDUMgr::destroyEDU ( EDUID eduID )
   {
      INT32 rc        = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_DSTEDU );
      pmdEDUCB* eduCB = NULL ;
      UINT32 eduStatus = PMD_EDU_CREATING ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      std::map<UINT32, EDUID>::iterator it1 ;
      {
         EDUMGR_XLOCK
         // try to find the edu id in runqueue
         // Since this is private function, no latch is needed
         if ( _runQueue.end () == ( it = _runQueue.find ( eduID )) )
         {
            // if we cannot find it in runqueue, we search for idle queue
            // note that during the time, we already have EDUMgr locked,
            // so thread cannot change queue from idle to run
            // that means we are safe to exame both queues
            if ( _idleQueue.end () == ( it = _idleQueue.find ( eduID )) )
            {
               // we can't find edu id anywhere
               rc = SDB_SYS ;
               goto error ;
            }
            eduCB = ( *it ).second ;
            // if we find in idle queue, we expect idle status
            if ( !PMD_IS_EDU_IDLE ( eduCB->getStatus ()) )
            {
               // if the status is not destroy
               rc = SDB_EDU_INVAL_STATUS ;
               goto error ;
            }
            // set the status to destroy
            eduCB->setStatus ( PMD_EDU_DESTROY ) ;
            _idleQueue.erase ( eduID ) ;
         }
         // if we find in run queue, we expect waiting status
         else
         {
            eduCB = ( *it ).second ;
            eduStatus = eduCB->getStatus () ;
            if ( !PMD_IS_EDU_WAITING ( eduStatus ) &&
                 !PMD_IS_EDU_CREATING ( eduStatus ) )
            {
               // if the status is not destroy
               // we should return error indicating bad status
               rc = SDB_EDU_INVAL_STATUS ;
               goto error ;
            }
            eduCB->setStatus ( PMD_EDU_DESTROY ) ;
            _runQueue.erase ( eduID ) ;
         }
         // clean up tid/eduid map
         for ( it1 = _tid_eduid_map.begin(); it1 != _tid_eduid_map.end();
               ++it1 )
         {
            if ( (*it1).second == eduID )
            {
               _tid_eduid_map.erase ( it1 ) ;
               break ;
            }
         }
         if ( eduCB )
         {
            SDB_OSS_DEL eduCB ;
            eduCB = NULL ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_DSTEDU, rc );
      return rc ;
   error :
      goto done ;
   }

   // change edu status from running to waiting
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_WAITEDU, "_pmdEDUMgr::waitEDU" )
   INT32 _pmdEDUMgr::waitEDU ( EDUID eduID )
   {
      INT32 rc        = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_WAITEDU );
      pmdEDUCB* eduCB = NULL ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;

      {
         /************** CRITICAL SECTION ***********/
         EDUMGR_SLOCK
         if ( _runQueue.end () == ( it = _runQueue.find ( eduID )) )
         {
            // we can't find EDU in run queue
            rc = SDB_SYS ;
            goto error ;
         }
         eduCB = ( *it ).second ;
         /************** CRITICAL SECTION ***********/
      }

      rc = waitEDU( eduCB ) ;

   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_WAITEDU, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_WAITEDU2, "_pmdEDUMgr::waitEDU" )
   INT32 _pmdEDUMgr::waitEDU( pmdEDUCB * cb )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_WAITEDU2 );
      if ( !cb )
      {
         return SDB_SYS ;
      }

      INT32 rc = SDB_OK ;
      UINT32 eduStatus = cb->getStatus() ;

      // if it's already waiting, let's do nothing
      if ( PMD_IS_EDU_WAITING ( eduStatus ) )
         goto done ;

      if ( !PMD_IS_EDU_RUNNING ( eduStatus ) )
      {
         // if it's not running status
         rc = SDB_EDU_INVAL_STATUS ;
         goto error ;
      }
      cb->setStatus ( PMD_EDU_WAITING ) ;
   done:
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_WAITEDU2, rc );
      return rc ;
   error:
      goto done ;
   }

   // creating/waiting status edu can be deactivated (pooled)
   // deactivateEDU supposed only happened to AGENT EDUs
   // any EDUs other than AGENT will be destroyed and SDB_SYS will be returned
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_DEATVEDU, "_pmdEDUMgr::deactivateEDU" )
   INT32 _pmdEDUMgr::deactivateEDU ( EDUID eduID )
   {
      INT32 rc         = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_DEATVEDU );
      UINT32 eduStatus = PMD_EDU_CREATING ;
      pmdEDUCB* eduCB  = NULL ;
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      // cross queue operation, need X lock
      {
         EDUMGR_XLOCK
         if ( _runQueue.end () == ( it = _runQueue.find ( eduID )) )
         {
            // if it's not in run queue, then is it in idle queue?
            // if it's already idle, we don't need to do anything
            if ( _idleQueue.end() != _idleQueue.find ( eduID )  )
            {
               goto done ;
            }
            // we can't find EDU in run queue
            rc = SDB_SYS ;
            goto error ;
         }
         eduCB = ( *it ).second ;

         eduStatus = eduCB->getStatus () ;

         // if it's already idle, let's get out of here
         if ( PMD_IS_EDU_IDLE ( eduStatus ) )
            goto done ;

         if ( !PMD_IS_EDU_WAITING ( eduStatus ) &&
              !PMD_IS_EDU_CREATING ( eduStatus ) )
         {
            rc = SDB_EDU_INVAL_STATUS ;
            goto error ;
         }

         // only Agent can be deactivated (pooled), other system
         // EDUs can only be destroyed
         SDB_ASSERT ( isPoolable ( eduCB->getType() ),
                      "Only agent, subagent and coordagent can be pooled" ) ;
         _runQueue.erase ( eduID ) ;
         eduCB->setStatus ( PMD_EDU_IDLE ) ;
         eduCB->writingDB ( FALSE ) ;
         _idleQueue [ eduID ] = eduCB ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_DEATVEDU, rc );
      return rc ;
   error :
      goto done ;
   }

   // make an idle EDU active (to RUNNING status)
   // runqueue: WAITING/CREATING status
   // idlequeue: IDLE status
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_ATVEDU, "_pmdEDUMgr::activateEDU" )
   INT32 _pmdEDUMgr::activateEDU ( EDUID eduID )
   {
      INT32   rc        = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_ATVEDU );
      UINT32  eduStatus = PMD_EDU_CREATING ;
      pmdEDUCB* eduCB   = NULL;
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      {
         /************** CRITICAL SECTION ***********/
         // we should lock the entire function in order to avoid
         // eduCB is deleted after we getting (*it).second
         EDUMGR_XLOCK
         if ( _idleQueue.end () == ( it = _idleQueue.find ( eduID )) )
         {
            if ( _runQueue.end () == ( it = _runQueue.find ( eduID )) )
            {
               // we can't find EDU in idle list nor runqueue
               rc = SDB_SYS ;
               goto error ;
            }
            eduCB = ( *it ).second ;
            // in runqueue we may have creating/waiting status
            eduStatus = eduCB->getStatus () ;

            if ( PMD_IS_EDU_RUNNING ( eduStatus ) )
               goto done ;
            if ( !PMD_IS_EDU_WAITING ( eduStatus ) &&
                 !PMD_IS_EDU_CREATING ( eduStatus ) )
            {
               rc = SDB_EDU_INVAL_STATUS ;
               goto error ;
            }
            eduCB->setStatus ( PMD_EDU_RUNNING ) ;
            goto done ;
         }
         eduCB = ( *it ).second ;
         eduStatus = eduCB->getStatus () ;
         if ( PMD_IS_EDU_RUNNING ( eduStatus ) )
            goto done ;
         // in idleQueue
         if ( !PMD_IS_EDU_IDLE ( eduStatus ) )
         {
            rc = SDB_EDU_INVAL_STATUS ;
            goto error ;
         }
         // now the EDU status is idle, let's bring it to RUNNING
         _idleQueue.erase ( eduID ) ;
         eduCB->setStatus ( PMD_EDU_RUNNING ) ;
         _runQueue [ eduID ] = eduCB ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_ATVEDU, rc );
      return rc ;
      /*********************END CRITICAL SECTION******************/
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_ATVEDU2, "_pmdEDUMgr::activateEDU" )
   INT32 _pmdEDUMgr::activateEDU( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_ATVEDU2 );
      if ( !cb )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      {
      UINT32  eduStatus = cb->getStatus() ;

      if ( PMD_IS_EDU_RUNNING ( eduStatus ) )
         goto done ;
      if ( !PMD_IS_EDU_WAITING ( eduStatus ) &&
           !PMD_IS_EDU_CREATING ( eduStatus ) )
      {
         rc = SDB_EDU_INVAL_STATUS ;
         goto error ;
      }
      cb->setStatus ( PMD_EDU_RUNNING ) ;
      }
   done:
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_ATVEDU2, rc );
      return rc ;
   error:
      goto done ;
   }

   // get pmdEDUCB for the given thread id
   pmdEDUCB *_pmdEDUMgr::getEDU ( UINT32 tid )
   {
      map<UINT32, EDUID>::iterator it ;
      map<EDUID, pmdEDUCB*>::iterator it1 ;
      EDUID eduid ;
      EDUMGR_SLOCK
      it = _tid_eduid_map.find ( tid ) ;
      if ( _tid_eduid_map.end() == it )
         return NULL ;
      eduid = (*it).second ;
      it1 = _runQueue.find ( eduid ) ;
      if ( _runQueue.end() != it1 )
         return (*it1).second ;
      it1 = _idleQueue.find ( eduid ) ;
      if ( _idleQueue.end() != it1 )
         return (*it1).second ;
      return NULL ;
   }

   void _pmdEDUMgr::setEDU ( UINT32 tid, EDUID eduid )
   {
      EDUMGR_XLOCK
      _tid_eduid_map [ tid ] = eduid ;
   }
   // get pmdEDUCB for the current thread
   pmdEDUCB *_pmdEDUMgr::getEDU ()
   {
      return getEDU ( ossGetCurrentThreadID() ) ;
   }

   pmdEDUCB *_pmdEDUMgr::getEDUByID ( EDUID eduID )
   {
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      // shared lock the block, since we don't change anything
      EDUMGR_SLOCK
      if ( _runQueue.end () == ( it = _runQueue.find ( eduID )) )
      {
         // if we cannot find it in runqueue, we search for idle queue
         // note that during the time, we already have EDUMgr locked,
         // so thread cannot change queue from idle to run
         // that means we are safe to exame both queues
         if ( _idleQueue.end () == ( it = _idleQueue.find ( eduID )) )
         {
            // we can't find edu id anywhere
            return NULL ;
         }
      }
      return it->second ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_WAITUTIL, "_pmdEDUMgr::waitUntil" )
   INT32 _pmdEDUMgr::waitUntil ( EDUID eduID, EDU_STATUS status,
                                 UINT32 waitPeriod, UINT32 waitRound )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_WAITUTIL );
      std::map<EDUID, pmdEDUCB*>::iterator it ;
      UINT32 round = 0 ;
      for ( round = 0; round < waitRound; ++round )
      {
         _mutex.get_shared() ;
         if ( _runQueue.end () == ( it = _runQueue.find ( eduID ) ) )
         {
            if ( _idleQueue.end () == ( it = _idleQueue.find ( eduID )) )
            {
               rc = SDB_INVALIDARG ;
               _mutex.release_shared () ;
               goto error ;
            }
         }
         if ( it->second->getStatus () == status )
         {
            _mutex.release_shared () ;
            break ;
         }
         _mutex.release_shared () ;
         ossSleepmillis ( waitPeriod ) ;
      }

      if ( round == waitRound )
      {
         rc = SDB_TIMEOUT ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_WAITUTIL, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_WAITUTIL2, "_pmdEDUMgr::waitUntil" )
   INT32 _pmdEDUMgr::waitUntil( EDU_TYPES type, EDU_STATUS status,
                                UINT32 waitPeriod, UINT32 waitRound )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_WAITUTIL2 );
      EDUID eduID = getSystemEDU( type ) ;

      while ( waitRound > 0 && PMD_INVALID_EDUID == eduID )
      {
         ossSleepmillis( waitPeriod ) ;
         eduID = getSystemEDU( type ) ;
         --waitRound ;

         if ( PMD_IS_DB_DOWN )
         {
            rc = SDB_APP_FORCED ;
            goto error ;
         }
      }

      if ( PMD_INVALID_EDUID == eduID )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = waitUntil( eduID, status, waitPeriod,
                      waitRound == 0 ? 1 : waitRound ) ;
   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUMGR_WAITUTIL2, rc );
      return rc ;
   error :
      goto done ;
   }
#if defined (_LINUX)
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUMGR_GETEDUTRDID, "_pmdEDUMgr::getEDUThreadID" )
   void _pmdEDUMgr::getEDUThreadID ( std::set<pthread_t> &tidList )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUMGR_GETEDUTRDID );
      try
      {
         std::map<EDUID, pmdEDUCB*>::iterator it ;
         EDUMGR_SLOCK
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; it ++ )
         {
            tidList.insert ( (*it).second->getThreadID () ) ;
         }
         for ( it = _idleQueue.begin () ; it != _idleQueue.end () ; it ++ )
         {
            tidList.insert ( (*it).second->getThreadID () ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR,
                  "Failed to insert tid into set: %s", e.what() ) ;
      }
      PD_TRACE_EXIT ( SDB__PMDEDUMGR_GETEDUTRDID );
   }
#endif
}

