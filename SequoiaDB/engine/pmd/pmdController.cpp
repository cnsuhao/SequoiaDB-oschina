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

   Source File Name = pmdController.cpp

   Descriptive Name = Process MoDel Main

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for SequoiaDB,
   and all other process-initialization code.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          02/05/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdController.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "rtnPageCleanerJob.hpp"
#include "../bson/lib/md5.hpp"

namespace engine
{
   // Session Time out
   #define PMD_REST_SESSION_TIMEOUT             ( 10 * 60 * 1000 )
   #define PMD_FIX_BUFF_CATCH_NUMBER            ( 100 )

   // max rest body size
   #define PMD_REST_MAX_BODY_SIZE               ( 64 * 1024 * 1024 )

   #define PMD_FIX_PTR_SIZE(x)                  ( x + sizeof(INT32) )
   #define PMD_FIX_PTR_HEADER(ptr)              (*(INT32*)(ptr))
   #define PMD_FIX_BUFF_TO_PTR(buff)            ((CHAR*)(buff)-sizeof(INT32))
   #define PMD_FIX_PTR_TO_BUFF(ptr)             ((CHAR*)(ptr)+sizeof(INT32))
   #define PMD_FIX_BUFF_HEADER(buff)            (*(INT32*)((CHAR*)(buff)-sizeof(INT32)))

   _pmdController::_pmdController ()
   {
      _pTcpListener        = NULL ;
      _pHttpListener       = NULL ;
      _sequence            = 1 ;
      _timeCounter         = 0 ;
      _fixBufSize          = SDB_PAGE_SIZE ;
      _maxRestBodySize     = PMD_REST_MAX_BODY_SIZE ;
      _restTimeout         = REST_TIMEOUT ;
      _pRSManager          = NULL ;
   }

   _pmdController::~_pmdController ()
   {
      SDB_ASSERT( _vecFixBuf.size() == 0, "Fix buff catch must be empty" ) ;
      _pTcpListener        = NULL ;
      _pHttpListener       = NULL ;
   }

   SDB_CB_TYPE _pmdController::cbType () const
   {
      return SDB_CB_PMDCTRL ;
   }

   const CHAR* _pmdController::cbName () const
   {
      return "PMDCONTROLLER" ;
   }

   INT32 _pmdController::init ()
   {
      INT32 rc = SDB_OK ;
      pmdOptionsCB *pOptCB = pmdGetOptionCB() ;
      UINT16 port = 0 ;

      // 1. create tcp listerner
      port = pOptCB->getServicePort() ;
      _pTcpListener = SDB_OSS_NEW ossSocket( port ) ;
      if ( !_pTcpListener )
      {
         PD_LOG( PDERROR, "Failed to alloc socket" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = _pTcpListener->initSocket() ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init tcp listener socket[%d], "
                   "rc: %d", port, rc ) ;

      rc = _pTcpListener->bind_listen() ;
      PD_RC_CHECK( rc, PDERROR, "Failed to bind tcp listener socket[%d], "
                   "rc: %d", port, rc ) ;
      PD_LOG( PDEVENT, "Listerning on port[%d]", port ) ;

      // 2. create http listerner
      rc = ossGetPort( pOptCB->getRestService(), port ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get port by service name: %s, "
                   "rc: %d", pOptCB->getRestService(), rc ) ;
      _pHttpListener = SDB_OSS_NEW ossSocket( port ) ;
      if ( !_pHttpListener )
      {
         PD_LOG( PDERROR, "Failed to alloc socket" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = _pHttpListener->initSocket() ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init http listener socket[%d], "
                   "rc: %d", port, rc ) ;
      rc = _pHttpListener->bind_listen() ;
      PD_RC_CHECK( rc, PDERROR, "Failed to bind http listerner socket[%d], "
                   "rc: %d", port, rc ) ;
      PD_LOG( PDEVENT, "Http Listerning on port[%d]", port ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdController::active ()
   {
      INT32 rc = SDB_OK ;
      pmdEDUMgr *pEDUMgr = pmdGetKRCB()->getEDUMgr() ;
      EDUID eduID = PMD_INVALID_EDUID ;

      rc = _restAdptor.init( _fixBufSize, _maxRestBodySize, _restTimeout ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init rest adptor, rc: %d", rc ) ;

      // start time sync edu
      rc = pEDUMgr->startEDU( EDU_TYPE_SYNCCLOCK, NULL, &eduID ) ;
      pEDUMgr->regSystemEDU( EDU_TYPE_SYNCCLOCK, eduID ) ;

      // start tcp listern edu and http listerner edu
      rc = pEDUMgr->startEDU( EDU_TYPE_TCPLISTENER, (void*)_pTcpListener,
                              &eduID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to start tcp listerner, rc: %d",
                   rc ) ;
      pEDUMgr->regSystemEDU( EDU_TYPE_TCPLISTENER, eduID ) ;

      // wait until tcp listener starts
      rc = pEDUMgr->waitUntil ( eduID, PMD_EDU_RUNNING ) ;
      PD_RC_CHECK( rc, PDERROR, "Wait Tcp Listerner active failed, rc: %d",
                   rc ) ;

      rc = pEDUMgr->startEDU( EDU_TYPE_RESTLISTENER, (void*)_pHttpListener,
                              &eduID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to start rest listerner, rc: %d",
                   rc ) ;
      pEDUMgr->regSystemEDU( EDU_TYPE_RESTLISTENER, eduID ) ;

      // wait until http listener starts
      rc = pEDUMgr->waitUntil ( eduID, PMD_EDU_RUNNING ) ;
      PD_RC_CHECK( rc, PDERROR, "Wait rest Listener active failed, rc: %d",
                   rc ) ;

      if ( SDB_ROLE_COORD != pmdGetDBRole() )
      {
         UINT32 pageTaskNum = pmdGetOptionCB()->getPageCleanNum() ;
         UINT32 pageIntervel = pmdGetOptionCB()->getPageCleanInterval() ;
         // start page flush background task
         for ( UINT32 i = 0; i < pageTaskNum ; ++i )
         {
            startPageCleanerJob( NULL, (INT32)pageIntervel ) ;
         }
         // start load job
         rtnStartLoadJob() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdController::deactive ()
   {
      return SDB_OK ;
   }

   INT32 _pmdController::fini ()
   {
      if ( _pTcpListener )
      {
         SDB_OSS_DEL _pTcpListener ;
         _pTcpListener = NULL ;
      }
      if ( _pHttpListener )
      {
         SDB_OSS_DEL _pHttpListener ;
         _pHttpListener = NULL ;
      }

      // release fix buff catch
      _ctrlLatch.get() ;
      for ( UINT32 i = 0 ; i < _vecFixBuf.size() ; ++i )
      {
         SDB_OSS_FREE( PMD_FIX_BUFF_TO_PTR( _vecFixBuf[i] ) ) ;
      }
      _vecFixBuf.clear() ;
      _ctrlLatch.release() ;

      // release session info
      restSessionInfo *pSessionInfo = NULL ;
      map<string, restSessionInfo*>::iterator it = _mapSessions.begin() ;
      while( it != _mapSessions.end() )
      {
         pSessionInfo = it->second ;
         pSessionInfo->releaseMem() ;
         SDB_OSS_DEL pSessionInfo ;
         ++it ;
      }
      _mapSessions.clear() ;
      _mapUser2Sessions.clear() ;

      return SDB_OK ;
   }

   void _pmdController::registerCB( SDB_ROLE dbrole )
   {
      if ( SDB_ROLE_DATA == dbrole )
      {
         PMD_REGISTER_CB( sdbGetDPSCB() ) ;        // DPS
         PMD_REGISTER_CB( sdbGetTransCB() ) ;      // TRANS
         PMD_REGISTER_CB( sdbGetClsCB() ) ;        // CLS
         PMD_REGISTER_CB( sdbGetBPSCB() ) ;        // BPS
      }
      else if ( SDB_ROLE_COORD == dbrole )
      {
         PMD_REGISTER_CB( sdbGetTransCB() ) ;      // TRANS
         PMD_REGISTER_CB( sdbGetCoordCB() ) ;      // COORD
         PMD_REGISTER_CB( sdbGetFMPCB () ) ;       // FMP
      }
      else if ( SDB_ROLE_CATALOG == dbrole )
      {
         PMD_REGISTER_CB( sdbGetDPSCB() ) ;        // DPS
         PMD_REGISTER_CB( sdbGetTransCB() ) ;      // TRANS
         PMD_REGISTER_CB( sdbGetClsCB() ) ;        // CLS
         PMD_REGISTER_CB( sdbGetCatalogueCB() ) ;  // CATALOGUE
         PMD_REGISTER_CB( sdbGetBPSCB() ) ;        // BPS
         PMD_REGISTER_CB( sdbGetAuthCB() ) ;       // AUTH
      }
      else if ( SDB_ROLE_STANDALONE == dbrole )
      {
         PMD_REGISTER_CB( sdbGetDPSCB() ) ;        // DPS
         PMD_REGISTER_CB( sdbGetTransCB() ) ;      // TRANS
         PMD_REGISTER_CB( sdbGetBPSCB() ) ;        // BPS
      }
      else if ( SDB_ROLE_OM == dbrole )
      {
         PMD_REGISTER_CB( sdbGetDPSCB() ) ;        // DPS
         PMD_REGISTER_CB( sdbGetTransCB() ) ;      // TRANS
         PMD_REGISTER_CB( sdbGetBPSCB() ) ;        // BPS
         PMD_REGISTER_CB( sdbGetAuthCB() ) ;       // AUTH
         PMD_REGISTER_CB( sdbGetOMManager() ) ;    // OMSVC
      }

      PMD_REGISTER_CB( sdbGetDMSCB() ) ;           // DMS
      PMD_REGISTER_CB( sdbGetRTNCB() ) ;           // RTN
      PMD_REGISTER_CB( sdbGetSQLCB() ) ;           // SQL
      PMD_REGISTER_CB( sdbGetAggrCB() ) ;          // AGGR
      PMD_REGISTER_CB( sdbGetPMDController() ) ;   // CONTROLLER
   }

   void _pmdController::detachSessionInfo( restSessionInfo * pSessionInfo )
   {
      SDB_ASSERT( pSessionInfo, "Session can't be NULL" ) ;

      if ( pSessionInfo->isLock() )
      {
         pSessionInfo->unlock() ;
         pSessionInfo->_inNum.dec() ;
      }
   }

   restSessionInfo* _pmdController::attachSessionInfo( const string & id )
   {
      restSessionInfo *pSessionInfo = NULL ;

      _ctrlLatch.get_shared() ;
      map<string, restSessionInfo*>::iterator it = _mapSessions.find( id ) ;
      if ( it != _mapSessions.end() )
      {
         pSessionInfo = it->second ;
         if ( pSessionInfo->isValid() )
         {
            pSessionInfo->_inNum.inc() ;
         }
         else
         {
            pSessionInfo = NULL ;
         }
      }
      _ctrlLatch.release_shared() ;

      if ( pSessionInfo )
      {
         pSessionInfo->lock() ;
      }

      return pSessionInfo ;
   }

   restSessionInfo* _pmdController::newSessionInfo( const string & userName,
                                                    UINT32 localIP )
   {
      restSessionInfo *newSession = SDB_OSS_NEW restSessionInfo ;
      if( !newSession )
      {
         PD_LOG( PDERROR, "Alloc rest session info failed" ) ;
         goto error ;
      }

      // get lock
      _ctrlLatch.get() ;
      newSession->_attr._sessionID = ossPack32To64( localIP, _sequence++ ) ;
      ossStrncpy( newSession->_attr._userName, userName.c_str(),
                  SESSION_USER_NAME_LEN ) ;
      // add to session map
      _mapSessions[ _makeID( newSession ) ] = newSession ;
      // add to user session map
      _add2UserMap( userName, newSession ) ;
      // attach session
      newSession->_inNum.inc() ;
      // release lock
      _ctrlLatch.release() ;

      if ( newSession )
      {
         newSession->lock() ;
      }

   done:
      return newSession ;
   error:
      goto done ;
   }

   void _pmdController::releaseSessionInfo( const string & sessionID )
   {
      restSessionInfo *pInfo = NULL ;
      map<string, restSessionInfo*>::iterator it ;

      _ctrlLatch.get() ;
      it = _mapSessions.find( sessionID ) ;
      if ( it != _mapSessions.end() )
      {
         pInfo = it->second ;
         _delFromUserMap( pInfo->_attr._userName, pInfo ) ;

         if ( pInfo->isLock() )
         {
            detachSessionInfo( pInfo ) ;
         }

         // no use
         if ( !pInfo->isIn() )
         {
            SDB_OSS_DEL pInfo ;
            _mapSessions.erase( it ) ;
         }
         else
         {
            _invalidSessionInfo( pInfo ) ;
         }
      }
      _ctrlLatch.release() ;
   }

   string _pmdController::_makeID( restSessionInfo * pSessionInfo )
   {
      UINT32 ip = 0 ;
      UINT32 seq = 0 ;
      ossUnpack32From64( pSessionInfo->_attr._sessionID, ip, seq ) ;
      CHAR tmp[9] = {0} ;
      ossSnprintf( tmp, sizeof(tmp)-1, "%08x", seq ) ;
      string strValue = md5::md5simpledigest( (const void*)pSessionInfo,
                                              pSessionInfo->getAttrSize() ) ;
      UINT32 size = strValue.size() ;
      strValue = strValue.substr( 0, size - ossStrlen( tmp ) ) ;
      strValue += tmp ;

      // set id
      pSessionInfo->_id = strValue ;
      return strValue ;
   }

   void _pmdController::_add2UserMap( const string & user,
                                      restSessionInfo * pSessionInfo )
   {
      map<string, vector<restSessionInfo*> >::iterator it ;
      it = _mapUser2Sessions.find( user ) ;
      // the user first session
      if ( it == _mapUser2Sessions.end() )
      {
         vector<restSessionInfo*> vecSession ;
         vecSession.push_back( pSessionInfo ) ;
         _mapUser2Sessions.insert( make_pair( user, vecSession ) ) ;
      }
      // the user already exist
      else
      {
         it->second.push_back( pSessionInfo ) ;
      }
   }

   void _pmdController::_delFromUserMap( const string & user,
                                         restSessionInfo * pSessionInfo )
   {
      map<string, vector<restSessionInfo*> >::iterator it ;
      it = _mapUser2Sessions.find( user ) ;
      if ( it != _mapUser2Sessions.end() )
      {
         vector<restSessionInfo*> &vecSessions = it->second ;
         vector<restSessionInfo*>::iterator itVec = vecSessions.begin() ;
         while ( itVec != vecSessions.end() )
         {
            if ( *itVec == pSessionInfo )
            {
               vecSessions.erase( itVec ) ;
               break ;
            }
            ++itVec ;
         }

         if ( vecSessions.size() == 0 )
         {
            _mapUser2Sessions.erase( it ) ;
         }
      }
   }

   void _pmdController::_invalidSessionInfo( restSessionInfo * pSessionInfo )
   {
      SDB_ASSERT( pSessionInfo, "Session can't be NULL" ) ;
      pSessionInfo->invalidate() ;
   }

   void _pmdController::_checkSession( UINT32 interval )
   {
      map<string, restSessionInfo*>::iterator it  ;
      restSessionInfo *pInfo = NULL ;

      _ctrlLatch.get() ;
      it = _mapSessions.begin() ;
      while ( it != _mapSessions.end() )
      {
         pInfo = it->second ;
         if ( pInfo->isIn() )
         {
            ++it ;
            continue ;
         }

         if ( pInfo->isValid()  )
         {
            pInfo->onTimer( interval ) ;
            if ( pInfo->isTimeout( PMD_REST_SESSION_TIMEOUT ) )
            {
               pInfo->invalidate() ;
            }
         }

         if ( !pInfo->isValid() )
         {
            _delFromUserMap( pInfo->_attr._userName, pInfo ) ;
            SDB_OSS_DEL pInfo ;
            _mapSessions.erase( it++ ) ;
            continue ;
         }
         ++it ;
      }
      _ctrlLatch.release() ;
   }

   void _pmdController::onTimer( UINT32 interval )
   {
      _timeCounter += interval ;

      if ( _timeCounter > 10 * OSS_ONE_SEC )
      {
         _checkSession( interval ) ;
         _timeCounter = 0 ;
      }
   }

   void _pmdController::releaseFixBuf( CHAR * pBuff )
   {
      SDB_ASSERT( pBuff, "Buff can't be NULL" ) ;
      SDB_ASSERT( PMD_FIX_BUFF_HEADER( pBuff ) == _fixBufSize,
                  "Buff is not alloc by fix buff" ) ;

      // if fix buff catch is not full, push to catch
      _ctrlLatch.get() ;
      if ( _vecFixBuf.size() < PMD_FIX_BUFF_CATCH_NUMBER )
      {
         _vecFixBuf.push_back( pBuff ) ;
         pBuff = NULL ;
      }
      _ctrlLatch.release() ;

      if ( pBuff )
      {
         SDB_OSS_FREE( PMD_FIX_BUFF_TO_PTR( pBuff ) ) ;
      }
   }

   CHAR* _pmdController::allocFixBuf()
   {
      CHAR *pBuff = NULL ;

      // if fix buff catch is not empty, get from catch
      _ctrlLatch.get() ;
      if ( _vecFixBuf.size() > 0 )
      {
         pBuff = _vecFixBuf.back() ;
         _vecFixBuf.pop_back() ;
      }
      _ctrlLatch.release() ;

      if ( pBuff )
      {
         goto done ;
      }

      // alloc
      pBuff = ( CHAR* )SDB_OSS_MALLOC( PMD_FIX_PTR_SIZE( _fixBufSize ) ) ;
      if ( !pBuff )
      {
         PD_LOG( PDERROR, "Alloc fix buff failed, size: %d",
                 PMD_FIX_PTR_SIZE( _fixBufSize ) ) ;
         goto error ;
      }
      PMD_FIX_PTR_HEADER( pBuff ) = _fixBufSize ;
      pBuff = PMD_FIX_PTR_TO_BUFF( pBuff ) ;

   done:
      return pBuff ;
   error:
      goto done ;
   }

   void _pmdController::setRSManager( _pmdRemoteSessionMgr * pRSManager )
   {
      _pRSManager = pRSManager ;
   }

   /*
      get global pointer
   */
   pmdController* sdbGetPMDController()
   {
      static pmdController s_pmdctrl ;
      return &s_pmdctrl ;
   }

}


