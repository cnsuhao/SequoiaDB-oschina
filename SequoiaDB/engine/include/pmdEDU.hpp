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

   Source File Name = pmdEDU.hpp

   Descriptive Name = Process MoDel Engine Dispatchable Unit Header

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains structure for EDU Control
   Block.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef PMDEDU_HPP__
#define PMDEDU_HPP__

#include "ossLatch.hpp"
#include "ossQueue.hpp"
#include "ossMem.hpp"
#include "ossSocket.hpp"
#include "oss.hpp"
#include "pmdDef.hpp"
#include "ossRWMutex.hpp"
#include "sdbInterface.hpp"
#include "pdTrace.hpp"

#if defined ( SDB_ENGINE )
#include "monEDU.hpp"
#include "monCB.hpp"
#include "dpsLogDef.hpp"
#include "dpsTransCB.hpp"
#include "dpsTransLockDef.hpp"
#endif // SDB_ENGINE

#include <set>
#include <string>

using namespace std ;

namespace engine
{
   /*
      CONST VALUE DEFINE
   */
   #define PMD_EDU_NAME_LENGTH         ( 512 )
   #define EDU_ERROR_BUFF_SIZE         ( 1024 )

   /*
      TOOL FUNCTIONS
   */
   INT32       registerEDUName ( EDU_TYPES type, const CHAR * name,
                                 BOOLEAN system ) ;
   const CHAR *getEDUStatusDesp ( EDU_STATUS status ) ;
   const CHAR *getEDUName ( EDU_TYPES type ) ;
   BOOLEAN     isSystemEDU ( EDU_TYPES type ) ;

   /*
      EDU CONTROL FLAG DEFINE
   */
   #define EDU_CTRL_INTERRUPTED        0x01
   #define EDU_CTRL_DISCONNECTED       0x02
   #define EDU_CTRL_FORCED             0x04

   /*
      EDU INFO TYPE DEFINE
   */
   enum EDU_INFO_TYPE
   {
      EDU_INFO_ERROR                   = 1   //Error
   } ;

   class _pmdEDUMgr ;
   class CoordSession ;

   /*
      _pmdEDUCB define
   */
   class _pmdEDUCB : public SDBObject
   {
   public:
      typedef std::multimap<INT32,CHAR*>     CATCH_MAP ;
      typedef CATCH_MAP::iterator            CATCH_MAP_IT ;

      typedef std::map<CHAR*,INT32>          ALLOC_MAP ;
      typedef ALLOC_MAP::iterator            ALLOC_MAP_IT ;

      friend class _pmdEDUMgr ;

   public:
      _pmdEDUCB( _pmdEDUMgr *mgr, EDU_TYPES type ) ;
      ~_pmdEDUCB () ;

      void clear () ;

      string toString() const ;

      EDUID getID() const { return _eduID ; }
      UINT32 getTID() const { return _tid ; }
      EDU_STATUS getStatus () const { return _status ; }
      EDU_TYPES getType () const { return _eduType ; }

      _pmdEDUMgr *getEDUMgr() { return _eduMgr ; }

      void setTID ( UINT32 tid ) { _tid = tid ; }
      void setType ( EDU_TYPES type ) ;

      void writingDB( BOOLEAN writing )
      {
         _writingDB = writing ;
         _writingTime = writing ? (UINT64)time( NULL ) : 0 ;
      }
      BOOLEAN isWritingDB() const { return _writingDB ; }
      UINT64  getWritingTime() const { return _writingTime ; }

   #if defined ( _LINUX )
      void        setThreadID ( pthread_t id ) { _threadID = id ; }
      pthread_t   getThreadID () const { return _threadID ; }
      void        setThreadHdl( OSSTID hdl ) { _threadHdl = hdl ; }
      OSSTID      getThreadHandle() const { return _threadHdl ; }
   #elif defined ( _WINDOWS )
      void        setThreadHdl( HANDLE hdl ) { _threadHdl = hdl ; }
      HANDLE      getThreadHandle() const { return _threadHdl ; }
   #endif

   public :
      void        attachSession( ISession *pSession ) ;
      void        detachSession() ;
      ISession*   getSession() { return _pSession ; }

      INT32       allocBuff( INT32 len, CHAR **ppBuff, INT32 &buffLen ) ;
      void        releaseBuff( CHAR *pBuff ) ;
      INT32       reallocBuff( INT32 len, CHAR **ppBuff, INT32 &buffLen ) ;
      void        restoreBuffs( CATCH_MAP &catchMap ) ;
      void        saveBuffs( CATCH_MAP &catchMap ) ;

      CHAR*       getCompressBuff( INT32 len ) ;
      INT32       getCompressBuffLen () const { return _compressBuffLen ; }
      CHAR*       getUncompressBuff( INT32 len ) ;
      INT32       getUncompressBuffLen() const { return _uncompressBuffLen ; }

      void        incEventCount( UINT32 step = 1 )
      {
         _processEventCount += step ;
      }

      void        interrupt () ;
      void        disconnect () ;
      void        force () ;
      BOOLEAN     isInterrupted ( BOOLEAN onlyFlag = FALSE ) ;
      BOOLEAN     isDisconnected () ;
      BOOLEAN     isForced () ;
      void        resetInterrupt () ;
      void        resetDisconnect () ;

      INT32 printInfo ( EDU_INFO_TYPE type, const CHAR *format, ... ) ;
      const CHAR *getInfo ( EDU_INFO_TYPE type ) ;
      void  resetInfo ( EDU_INFO_TYPE type ) ;

      void setClientInfo ( const CHAR *clientName, UINT16 clientPort ) ;
      void setUserInfo( const string &userName, const string &password ) ;
      void setName ( const CHAR *name ) ;
      void setClientSock ( ossSocket *pSock ) { _pClientSock = pSock ; }
      ossSocket * getClientSock () { return _pClientSock ; }
      BOOLEAN isFromLocal() const { return _pClientSock ? TRUE : FALSE ; }
      const CHAR *getName ()
      {
         ossScopedLock _lock ( &_mutex, SHARED ) ;
         return _Name ;
      }
      const CHAR *getUserName() const { return _userName.c_str() ; }
      const CHAR *getPassword() const { return _passWord.c_str() ; }

      void postEvent ( pmdEDUEvent const &data )
      {
         _queue.push ( data ) ;
      }

      BOOLEAN waitEvent ( pmdEDUEvent &data, INT64 millsec )
      {

         BOOLEAN waitMsg   = FALSE ;
         _writingDB        = FALSE ;
         if ( PMD_EDU_IDLE != _status )
         {
            _status = PMD_EDU_WAITING ;
         }

         if ( 0 > millsec )
         {
            _queue.wait_and_pop ( data ) ;
            waitMsg = TRUE ;
         }
         else
         {
            waitMsg = _queue.timed_wait_and_pop ( data, millsec ) ;
         }

         if ( waitMsg )
         {
            ++_processEventCount ;
            if ( data._eventType == PMD_EDU_EVENT_TERM )
            {
               _ctrlFlag |= ( EDU_CTRL_DISCONNECTED|EDU_CTRL_INTERRUPTED );
            }
            else
            {
               _status = PMD_EDU_RUNNING ;
            }
         }

         return waitMsg ;
      }

      BOOLEAN waitEvent( pmdEDUEventTypes type, pmdEDUEvent &data,
                         INT64 millsec )
      {
         BOOLEAN ret = FALSE ;
         INT64 waitTime = 0 ;
         ossQueue< pmdEDUEvent > tmpQue ;

         if ( millsec < 0 )
         {
            millsec = 0x7FFFFFFF ;
         }

         while ( !isInterrupted() )
         {
            waitTime = millsec < OSS_ONE_SEC ? millsec : OSS_ONE_SEC ;
            if ( !waitEvent( data, waitTime ) )
            {
               millsec -= waitTime ;
               if ( millsec <= 0 )
               {
                  break ;
               }
               continue ;
            }
            if ( type != data._eventType )
            {
               tmpQue.push( data ) ;
               --_processEventCount ;
               continue ;
            }
            ret = TRUE ;
            break ;
         }

         pmdEDUEvent tmpData ;
         while ( !tmpQue.empty() )
         {
            tmpQue.try_pop( tmpData ) ;
            _queue.push( tmpData ) ;
         }
         return ret ;
      }

   #if defined ( SDB_ENGINE )

      void  initMonAppCB()
      {
         _monApplCB.reset() ;
         if ( _monCfgCB.timestampON )
         {
            _monApplCB.recordConnectTimestamp() ;
         }
      }
      void resetMon () { _monApplCB.reset () ; }
      monConfigCB * getMonConfigCB() { return & _monCfgCB ; }
      monAppCB * getMonAppCB() { return & _monApplCB ; }

      ossEvent & getEvent () { return _event ; }

      void setCoordSession( CoordSession *pSession )
      {
         _pCoordSession = pSession;
      }
      CoordSession *getCoordSession() { return _pCoordSession ; }

      void contextInsert ( SINT64 contextID )
      {
         ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
         _contextList.insert ( contextID ) ;
      }
      void contextDelete ( SINT64 contextID )
      {
         ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
         _contextList.erase ( contextID ) ;
      }
      SINT64 contextPeek () ;
      BOOLEAN contextFind ( SINT64 contextID )
      {
         ossScopedLock _lock ( &_mutex, SHARED ) ;
         return _contextList.end() != _contextList.find( contextID ) ;
      }
      UINT32 contextNum ()
      {
         ossScopedLock _lock ( &_mutex, SHARED ) ;
         return _contextList.size() ;
      }
      void contextCopy ( std::set<SINT64> &contextList )
      {
         ossScopedLock _lock ( &_mutex, SHARED ) ;
         contextList = _contextList ;
      }

      UINT64 getBeginLsn () const { return _beginLsn ; }
      UINT64 getEndLsn () const { return _endLsn ; }
      UINT32 getLsnCount () const { return _lsnNumber ; }
      void   resetLsn ()
      {
         _beginLsn = ~0 ;
         _endLsn = ~0 ;
         _lsnNumber = 0 ;
      }
      void   insertLsn ( UINT64 lsn )
      {
         if ( _beginLsn == (UINT64)~0 )
         {
            _beginLsn = lsn ;
         }
         _endLsn = lsn ;
         _lsnNumber++ ;
      }
      UINT64 getCurRequestID() const { return _curRequestID ; }
      UINT64 incCurRequestID() { return ++_curRequestID ; }

      void  setTransID( DPS_TRANS_ID transID ) { _curTransID = transID ; }
      DPS_TRANS_ID getTransID() const { return _curTransID ; }
      void  setRelatedTransLSN( DPS_LSN_OFFSET relatedLSN )
      { _relatedTransLSN = relatedLSN ; }
      void  setCurTransLsn( DPS_LSN_OFFSET curLsn ) { _curTransLSN = curLsn ; }
      DPS_LSN_OFFSET getRelatedTransLSN() const { return _relatedTransLSN ; }
      DPS_LSN_OFFSET getCurTransLsn() const { return _curTransLSN ; }
      dpsTransCBLockInfo *getTransLock( const dpsTransLockId &lockId );
      void  addLockInfo( const dpsTransLockId &lockId,
                         DPS_TRANSLOCK_TYPE lockType ) ;
      void  delLockInfo( const dpsTransLockId &lockId ) ;
      DpsTransCBLockList *getLockList() ;
      INT32 createTransaction() ;
      void  delTransaction() ;
      void  addTransNode( MsgRouteID &routeID ) ;
      void  getTransNodeRouteID( UINT32 groupID, MsgRouteID &routeID ) ;
      DpsTransNodeMap *getTransNodeLst() ;
      BOOLEAN isTransaction() ;
      BOOLEAN isTransNode( MsgRouteID &routeID ) ;
      void  startRollback() { _isDoRollback = TRUE ; }
      void  stopRollback() { _isDoRollback = FALSE ; }
      void  setTransRC( INT32 rc ) { _transRC = rc ; }
      INT32 getTransRC() const { return _transRC ; }
      void  clearTransInfo() ;

      void dumpInfo ( monEDUSimple &simple ) ;
      void dumpInfo ( monEDUFull &full ) ;

      void *getAlignedMemory( UINT32 alignment, UINT32 size ) ;
      void releaseAlignedMemory() ;

      UINT32 getDmsLockLevel() const { return _dmsLockLevel ; }
      void   setDmsLockLevel( UINT32 lockLevel ) { _dmsLockLevel = lockLevel ; }

   #endif // SDB_ENGINE

   protected:
      void setStatus ( EDU_STATUS status ) { _status = status ; }
      void setID ( EDUID id ) { _eduID = id ; }

      CHAR*    _getBuffInfo ( EDU_INFO_TYPE type, UINT32 &size ) ;
      BOOLEAN  _allocFromCatch( INT32 len, CHAR **ppBuff, INT32 &buffLen ) ;

   private :
      ossRWMutex     _callInMutex ;
      EDUID          _eduID ;
      UINT32         _tid ;
      UINT64         _processEventCount ;
      _pmdEDUMgr     *_eduMgr ;
      ISession       *_pSession ;
      ossSpinSLatch  _mutex ;
      EDU_STATUS     _status ;
      ossQueue<pmdEDUEvent> _queue ;
      EDU_TYPES      _eduType ;

      INT32          _ctrlFlag ;
      BOOLEAN        _writingDB ;
      UINT64         _writingTime ;

      string         _userName ;
      string         _passWord ;

      CHAR           *_pCompressBuff ;
      INT32          _compressBuffLen ;
      CHAR           *_pUncompressBuff ;
      INT32          _uncompressBuffLen ;

      CATCH_MAP      _catchMap ;
      ALLOC_MAP      _allocMap ;
      INT64          _totalCatchSize ;
      INT64          _totalMemSize ;

      CHAR              *_pErrorBuff ;
   #if defined ( _WINDOWS )
      HANDLE            _threadHdl ;
   #elif defined ( _LINUX )
      OSSTID            _threadHdl ;
      pthread_t         _threadID ;
   #endif // _WINDOWS

      CHAR              _Name [ PMD_EDU_NAME_LENGTH + 1 ] ;
      ossSocket        *_pClientSock ;

      BOOLEAN                 _isDoRollback ;

   #if defined ( SDB_ENGINE )

      monAppCB                _monApplCB ;
      monConfigCB             _monCfgCB ;

      ossEvent                _event ;   // for cls replSet notify

      std::set<SINT64>        _contextList ;
      ossQueue<pmdEDUEvent>   _bpEventQueue ;

      UINT32                  _dmsLockLevel ; // for dms lock

      CoordSession            *_pCoordSession;

      UINT64                  _beginLsn ;
      UINT64                  _endLsn ;
      UINT32                  _lsnNumber ;

      UINT64                  _curRequestID ;

      DPS_LSN_OFFSET          _relatedTransLSN ;
      DPS_LSN_OFFSET          _curTransLSN ;
      DPS_TRANS_ID            _curTransID ;
      DpsTransCBLockList      _transLockLst ;
      DpsTransNodeMap         *_pTransNodeMap ;
      INT32                   _transRC ;

      void                    *_alignedMem ;
      UINT32                   _alignedMemSize ;
   #endif // SDB_ENGINE

   };
   typedef class _pmdEDUCB pmdEDUCB ;

   _pmdEDUCB *pmdGetThreadEDUCB () ;

   _pmdEDUCB *pmdCreateThreadEDUCB ( _pmdEDUMgr *mgr, EDU_TYPES type ) ;

   void pmdDeleteThreadEDUCB () ;

   _pmdEDUCB *pmdDeclareEDUCB ( _pmdEDUCB *p ) ;

   void pmdUndeclareEDUCB () ;

   /*
      ENTRY POINT
   */
   typedef INT32 (*pmdEntryPoint)( pmdEDUCB *, void * ) ;

   pmdEntryPoint getEntryFuncByType ( EDU_TYPES type ) ;

   INT32 pmdEDUEntryPoint ( EDU_TYPES type, pmdEDUCB *cb, void *arg ) ;
   INT32 pmdEDUEntryPointWrapper ( EDU_TYPES type, pmdEDUCB *cb, void *arg ) ;

   struct _eduEntryInfo
   {
      EDU_TYPES         type ;
      INT32             regResult ;
      pmdEntryPoint     entryFunc ;
   } ;

#define ON_EDUTYPE_TO_ENTRY1(type, system, entry, desp) \
   { type, registerEDUName(type, desp, system), entry }

#define ON_EDUTYPE_TO_ENTRY2(type, system, entry) \
   ON_EDUTYPE_TO_ENTRY1(type, system, entry, #type)

   /*
      TOOL FUNCTIONS
   */
   INT32 pmdRecv ( CHAR *pBuffer, INT32 recvSize,
                   ossSocket *sock, pmdEDUCB *cb ) ;
   INT32 pmdSend ( const CHAR *pBuffer, INT32 sendSize,
                   ossSocket *sock, pmdEDUCB *cb ) ;

   void  pmdEduEventRelase( pmdEDUEvent &event, pmdEDUCB *cb ) ;

}

#endif // PMDEDU_HPP__

