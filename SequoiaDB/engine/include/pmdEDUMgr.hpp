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

   Source File Name = pmdEDUMgr.hpp

   Descriptive Name = Process MoDel Engine Dispatchable Unit Manager Header

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains structure for EDU Pool, which
   include operations like creating a new EDU, reuse existing EDU, destroy EDU
   and etc...

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef PMDEDUMGR_HPP__
#define PMDEDUMGR_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "pmdEDU.hpp"
#include "monEDU.hpp"
#include "ossLatch.hpp"
#include "ossUtil.hpp"
#include "ossSocket.hpp"
#include <boost/asio.hpp>
#include <map>
#include <set>
#include <string>

using namespace boost::asio ;
using namespace boost::asio::ip ;
namespace engine
{

#define PMD_EDU_WAIT_PERIOD     (200)
#define PMD_EDU_WAIT_ROUND      (600)

#define EDU_SYSTEM         0x0001
#define EDU_USER           0x0002
#define EDU_ALL            ( EDU_SYSTEM | EDU_USER )

   /* class for Engine Dispatchable Unit */
   class _pmdEDUMgr : public SDBObject
   {
   private :
      std::map<EDUID, pmdEDUCB*> _runQueue ;
      std::map<EDUID, pmdEDUCB*> _idleQueue ;
      std::map<UINT32, EDUID> _tid_eduid_map ;
      std::vector < io_service *> _ioserviceList ;

      ossSpinSLatch _mutex ;
      EDUID _EDUID ;

      std::map<UINT32, EDUID> _mapSystemEDUS;
      BOOLEAN _isQuiesced ;

      BOOLEAN _isDestroyed ;
   #ifdef EDUMGR_SLOCK
   #undef EDUMGR_SLOCK
   #endif
   #define EDUMGR_SLOCK ossScopedLock _lock ( &_mutex, SHARED ) ;

   #ifdef EDUMGR_XLOCK
   #undef EDUMGR_XLOCK
   #endif
   #define EDUMGR_XLOCK ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
   public :
      _pmdEDUMgr () ;
      ~_pmdEDUMgr () ;

   public:
      void addIOService( io_service *service ) ;
      void deleteIOService ( io_service *service ) ;

      void reset () ;

      UINT32 size ()
      {
         EDUMGR_SLOCK
         return ( UINT32 ) _runQueue.size () +  ( UINT32 ) _idleQueue.size () ;
      }
      UINT32 sizeRun ()
      {
         EDUMGR_SLOCK
         return ( UINT32 ) _runQueue.size () ;
      }
      UINT32 sizeIdle ()
      {
         EDUMGR_SLOCK
         return ( UINT32 ) _idleQueue.size () ;
      }
      UINT32 sizeSystem ()
      {
         EDUMGR_SLOCK
         return _mapSystemEDUS.size() ;
      }
      EDUID getSystemEDU ( EDU_TYPES edu )
      {
         EDUID eduID = PMD_INVALID_EDUID;
         EDUMGR_SLOCK
         std::map<UINT32, EDUID>::iterator it = _mapSystemEDUS.find( edu ) ;
         if ( it != _mapSystemEDUS.end() )
         {
            eduID = it->second  ;
         }
         return eduID ;
      }
      BOOLEAN isSystemEDU ( EDUID eduID )
      {
         EDUMGR_SLOCK
         return _isSystemEDU ( eduID ) ;
      }
      void regSystemEDU ( EDU_TYPES edu, EDUID eduid )
      {
         EDUMGR_XLOCK
         _mapSystemEDUS[ edu ] = eduid ;
      }
      BOOLEAN isQuiesced ()
      {
         EDUMGR_SLOCK
         return _isQuiesced ;
      }
      void setQuiesced ( BOOLEAN b )
      {
         EDUMGR_XLOCK
         _isQuiesced = b ;
      }
      BOOLEAN isDestroyed ()
      {
         EDUMGR_SLOCK
         return _isDestroyed ;
      }

#if defined( SDB_ENGINE )
      void dumpInfo ( std::set<monEDUSimple> &info ) ;
      void dumpInfo ( std::set<monEDUFull> &info ) ;

      void resetMon ()
      {
         std::map<EDUID, pmdEDUCB*>::iterator it ;
         EDUMGR_SLOCK
         for ( it = _runQueue.begin () ; it != _runQueue.end () ; it ++ )
         {
            (*it).second->resetMon () ;
         }
         for ( it = _idleQueue.begin () ; it != _idleQueue.end () ; it ++ )
         {
            (*it).second->resetMon () ;
         }
      }
#endif // SDB_ENGINE

      static BOOLEAN isPoolable ( EDU_TYPES type )
      {
         return ( type > EDU_TYPE_AGENT_BEGIN ) &&
                ( type < EDU_TYPE_AGENT_END ) ;
      }

#if defined (_LINUX)
      void getEDUThreadID ( std::set<pthread_t> &tidList ) ;
#endif

   private :
      INT32    createNewEDU ( EDU_TYPES type, void* arg, EDUID *eduid ) ;
      INT32    destroyAll () ;
      INT32    _forceEDUs ( INT32 property = EDU_ALL ) ;
      UINT32   _getEDUCount ( INT32 property = EDU_ALL ) ;
      INT32    _forceIOService() ;
      UINT32   _getIOServiceCount() ;

      INT32    _interruptWritingEDUs() ;
      UINT32   _getWritingEDUCount( INT32 eduTypeFilter = -1,
                                    UINT64 timeThreshold = 0 ) ;

      void setDestroyed ( BOOLEAN b )
      {
         EDUMGR_XLOCK
         _isDestroyed = b ;
      }
      BOOLEAN _isSystemEDU ( EDUID eduID )
      {
         std::map<UINT32, EDUID>::iterator it = _mapSystemEDUS.begin() ;
         while ( it != _mapSystemEDUS.end() )
         {
            if ( eduID == it->second )
            {
               return TRUE ;
            }
            ++it ;
         }
         return FALSE ;
      }

      /*
       * This function must be called against a thread that either in
       * PMD_EDU_WAITING or PMD_EDU_IDLE or PMD_EDU_CREATING status
       * This function set the status to PMD_EDU_DESTROY and remove
       * the control block from manager
       * Parameter:
       *   EDU ID (UINt64)
       * Return:
       *   SDB_OK (success)
       *   SDB_SYS (the given eduid can't be found)
       *   SDB_EDU_INVAL_STATUS (EDU is found but not with expected status)
       */
      INT32 destroyEDU ( EDUID eduID ) ;
      /*
       * This function must be called against a thread that either in creating
       * or waiting status, it will return without any change if the agent is
       * already in pool
       * This function will change the status to PMD_EDU_IDLE and put to
       * idlequeue, representing the EDU is pooled (no longer associate with
       * any user activities)
       * deactivateEDU supposed only happened to AGENT EDUs
       * Any EDUs other than AGENT will be forwarded to destroyEDU and return
       * SDB_SYS
       * Parameter:
       *   EDU ID (UINt64)
       * Return:
       *   SDB_OK (success)
       *   SDB_SYS (the given eduid can't be found, or it's not AGENT)
       *           (note that deactivate an non-AGENT EDU will cause the EDU
       *            destroyed and SDB_SYS return)
       *   SDB_EDU_INVAL_STATUS (EDU is found but not with expected status)
       */
      INT32 deactivateEDU ( EDUID eduID ) ;
   public :
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
       * R a   -   a   a   -  <--- Creating/Idle/Waiting status can move to Running status
       * W -   w   -   -   -  <--- Running status move to Waiting
       * I t   -   t   -   -  <--- Creating/Waiting status move to Idle
       * D d   -   d   d   -  <--- Creating / Waiting / Idle can be destroyed
       * ^ To
       */

      /*
       * This function must be called against a thread that either in
       * creating/idle or waiting status
       * Threads in creating/waiting status should be sit in runqueue
       * Threads in idle status should be sit in idlequeue
       * This function set the status to PMD_END_RUNNING and bring
       * the control block to runqueue if it was idle status
       * Parameter:
       *   EDU ID (UINt64)
       * Return:
       *   SDB_OK (success)
       *   SDB_SYS (the given eduid can't be found)
       *   SDB_EDU_INVAL_STATUS (EDU is found but not with expected status)
       */
      INT32    activateEDU ( EDUID eduID ) ;
      INT32    activateEDU ( pmdEDUCB *cb ) ;

      /*
       * This function must be called against a thread that in running
       * status
       * Threads in running status will be put in PMD_EDU_WAITING and
       * remain in runqueue
       * Parameter:
       *   EDU ID (UINt64)
       * Return:
       *   SDB_OK (success)
       *   SDB_SYS (the given eduid can't be found)
       *   SDB_EDU_INVAL_STATUS (EDU is found but not with expected status)
       */
      INT32    waitEDU ( EDUID eduID ) ;
      INT32    waitEDU ( pmdEDUCB *cb ) ;

      /*
       * This function is called to get an EDU run the given function
       * Depends on if there's any idle EDU, manager may choose an existing
       * idle thread or creating a new threads to serve the request
       * Parmaeter:
       *   pmdEntryPoint ( void (*entryfunc) (pmdEDUCB*, void*) )
       *   type (EDU type, PMD_TYPE_AGENT for example )
       *   arg ( void*, pass to pmdEntryPoint )
       * Output:
       *   eduid ( UINt64, the edu id for the assigned EDU )
       * Return:
       *   SDB_OK (success)
       *   SDB_SYS (internal error (edu id is reused) or creating thread fail)
       *   SDB_OOM (failed to allocate memory)
       *   SDB_INVALIDARG (the type is not valid )
       */
      INT32    startEDU ( EDU_TYPES type, void* arg, EDUID *eduid ) ;

      /*
       * This function should post a message to EDU
       * In each EDU control block there is a queue for message
       * Posting EDU means adding an element to the queue
       * If the EDU is doing some other activity at the moment, it may
       * not able to consume the event right away
       * There can be more than one event sitting in the queue
       * The order is first in first out
       * Parameter:
       *   EDU Id ( EDUID )
       *   enum pmdEDUEventTypes, in pmdDef.hpp
       *   pointer for data
       * Return:
       *   SDB_OK ( success )
       *   SDB_SYS ( given EDU ID can't be found )
       */
      INT32    postEDUPost ( EDUID eduID, pmdEDUEventTypes type,
                             pmdEDUMemTypes dataMemType = PMD_EDU_MEM_NONE,
                             void *pData = NULL ) ;


      /*
       * This function should wait an event for EDU
       * If there are more than one event sitting in the queue
       * waitEDU function should pick up the earliest event
       * This function will wait forever if the input is less than 0
       * Parameter:
       *    EDU ID ( EDUID )
       *    millisecond for the period of waiting ( -1 by default )
       * Output:
       *    Reference for event
       * Return:
       *   SDB_OK ( success )
       *   SDB_SYS ( given EDU ID can't be found )
       *   SDB_TIMEOUT ( timeout )
       */
      INT32    waitEDUPost ( EDUID eduID, pmdEDUEvent& event,
                             INT64 millsecond ) ;

      /*
       * This function should return an waiting/creating EDU to pool
       * (cannot be running)
       * Pool will decide whether to destroy it or pool the EDU
       * Any thread main function should detect the destroyed output
       * deactivateEDU supposed only happened to AGENT EDUs
       * Parameter:
       *   EDU ID ( EDUID )
       * Output:
       *   Pointer for whether the EDU is destroyed
       * Return:
       *   SDB_OK ( success )
       *   SDB_SYS ( given EDU ID can't be found )
       *   SDB_EDU_INVAL_STATUS (EDU is found but not with expected status)
       */
      INT32    returnEDU ( EDUID eduID, BOOLEAN force, BOOLEAN* destroyed ) ;

      INT32    forceUserEDU ( EDUID eduID ) ;

      INT32 interruptUserEDU( EDUID eduID ) ;

      INT32    interruptWritingEDUS() { return _interruptWritingEDUs() ; }
      UINT32   getWritingEDUCount( INT32 eduTypeFilter = -1,
                                   UINT64 timeThreshold = 0 )
      {
         return _getWritingEDUCount( eduTypeFilter, timeThreshold ) ;
      }

      pmdEDUCB *getEDU ( UINT32 tid ) ;
      pmdEDUCB *getEDU () ;
      pmdEDUCB *getEDUByID ( EDUID eduID ) ;
      void setEDU ( UINT32 tid, EDUID eduid ) ;
      INT32 waitUntil ( EDUID eduID, EDU_STATUS status,
                        UINT32 waitPeriod = PMD_EDU_WAIT_PERIOD,
                        UINT32 waitRound = PMD_EDU_WAIT_ROUND ) ;
      INT32 waitUntil ( EDU_TYPES type, EDU_STATUS status,
                        UINT32 waitPeriod = PMD_EDU_WAIT_PERIOD,
                        UINT32 waitRound = PMD_EDU_WAIT_ROUND ) ;

   };
   typedef class _pmdEDUMgr pmdEDUMgr ;
}

#endif // PMDEDUMGR_HPP__
