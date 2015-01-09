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

   Source File Name = netMultiRouteAgent.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "netMultiRouteAgent.hpp"
#include "pmdDef.hpp"
#include "coordSession.hpp"
#include "pdTrace.hpp"
#include "netTrace.hpp"

namespace engine
{
   netMultiRouteAgent::netMultiRouteAgent()
   {
      _pNetWork = NULL;
      _pReqID = SDB_OSS_NEW ossAtomic64(0);
   }

   netMultiRouteAgent::~netMultiRouteAgent()
   {
      SDB_OSS_DEL _pReqID;
   }

   void netMultiRouteAgent::setNetWork( _netRouteAgent *pNetWork)
   {
      _pNetWork = pNetWork;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_SYNCSENDWITHOUTSESSION, "netMultiRouteAgent::syncSendWithoutSession" )
   INT32 netMultiRouteAgent::syncSendWithoutCheck( const MsgRouteID &id, void *header,
                                                   UINT64 &reqID, pmdEDUCB *pEduCB,
                                                   void *body, UINT32 bodyLen )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_SYNCSENDWITHOUTSESSION );
      MsgHeader *pHeader = (MsgHeader *)header ;

      CoordSession *pSession = NULL;
      if ( pEduCB )
      {
         pHeader->TID = pEduCB->getTID();
         pSession = pEduCB->getCoordSession();
         SDB_ASSERT( pSession, "pSession can't be NULL!" ) ;
         pSession->addSubSessionWithoutCheck( id );
      }
      reqID = _pReqID->inc();
      pHeader->requestID = reqID;
      pHeader->routeID.value = MSG_INVALID_ROUTEID ;

      PD_LOG ( PDDEBUG, "Send request to node(opCode=%d, requestID=%llu, "
               "TID=%u, groupID=%u, nodeID=%u, serviceID=%u)",
               pHeader->opCode, pHeader->requestID, pHeader->TID,
               id.columns.groupID, id.columns.nodeID, id.columns.serviceID );
      if ( !body )
      {
         rc = _pNetWork->syncSend( id, header );
      }
      else
      {
         rc = _pNetWork->syncSend( id, pHeader, body, bodyLen ) ;
      }
      if ( SDB_OK == rc && pSession )
      {
         pSession->addRequest( reqID, id );
      }
      return rc;
      PD_TRACE_EXITRC ( SDB_NETMLTRTAGT_SYNCSENDWITHOUTSESSION, rc );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_SYNCSEND, "netMultiRouteAgent::syncSend" )
   INT32 netMultiRouteAgent::syncSend( const MsgRouteID &id, void *header, UINT64 &reqID,
                                       pmdEDUCB *pEduCB, void *body, UINT32 bodyLen )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_SYNCSEND );
      MsgHeader *pHeader = (MsgHeader *)header ;

      SDB_ASSERT( _pNetWork && header, "_pNetWork && header can't be NULL") ;

      CoordSession *pSession = NULL;
      if ( pEduCB )
      {
         pHeader->TID = pEduCB->getTID();
         pSession = pEduCB->getCoordSession();
         SDB_ASSERT( pSession, "pSession can't be NULL!" ) ;
         rc = pSession->addSubSession( id );
      }
      PD_RC_CHECK( rc, PDERROR,
                  "failed to create session(rc=%d)",
                  rc );

      reqID = _pReqID->inc();
      pHeader->requestID = reqID;
      pHeader->routeID.value = MSG_INVALID_ROUTEID ;

      PD_LOG ( PDDEBUG, "Send request to node(opCode=%d, requestID=%llu, "
               "TID=%u, groupID=%u, nodeID=%u, serviceID=%u)",
               pHeader->opCode, pHeader->requestID, pHeader->TID,
               id.columns.groupID, id.columns.nodeID, id.columns.serviceID );

      if ( !body )
      {
         rc = _pNetWork->syncSend( id, header );
      }
      else
      {
         rc = _pNetWork->syncSend( id, pHeader, body, bodyLen ) ;
      }
      if ( SDB_OK == rc && pSession )
      {
         pSession->addRequest( reqID, id );
      }

   done:
      PD_TRACE_EXITRC ( SDB_NETMLTRTAGT_SYNCSEND, rc );
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_SYNCSEND2, "netMultiRouteAgent::syncSend" )
   INT32 netMultiRouteAgent::syncSend( const MsgRouteID &id, MsgHeader *header,
                                       const netIOVec &iov, UINT64 &reqID,
                                       pmdEDUCB *pEduCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_SYNCSEND2 );
      MsgHeader *pHeader = (MsgHeader *)header ;

      SDB_ASSERT( _pNetWork && header, "_pNetWork && header can't be NULL") ;

      CoordSession *pSession = NULL;
      if ( pEduCB )
      {
         pHeader->TID = pEduCB->getTID();
         pSession = pEduCB->getCoordSession();
         SDB_ASSERT( pSession, "pSession can't be NULL!" ) ;
         rc = pSession->addSubSession( id );
      }
      PD_RC_CHECK( rc, PDERROR,
                  "failed to create session(rc=%d)",
                  rc );

      reqID = _pReqID->inc();
      pHeader->requestID = reqID;
      pHeader->routeID.value = MSG_INVALID_ROUTEID ;

      PD_LOG ( PDDEBUG, "Send request to node(opCode=%d, requestID=%llu, "
               "TID=%u, groupID=%u, nodeID=%u, serviceID=%u)",
               pHeader->opCode, pHeader->requestID, pHeader->TID,
               id.columns.groupID, id.columns.nodeID, id.columns.serviceID );

      rc = _pNetWork->syncSendv( id, header, iov );
      if ( SDB_OK == rc && pSession )
      {
         pSession->addRequest( reqID, id );
      }
   done:
      PD_TRACE_EXITRC ( SDB_NETMLTRTAGT_SYNCSEND2, rc );
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_MULTSYNCSND, "netMultiRouteAgent::multiSyncSend" )
   void netMultiRouteAgent::multiSyncSend( const ROUTE_SET &routeSet,
                                           void *header )
   {
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_MULTSYNCSND );
      MsgHeader *pHeader = (MsgHeader *)header ;

      SDB_ASSERT( _pNetWork && header, "_pNetWork && header can't be NULL") ;

      pHeader->routeID.value = MSG_INVALID_ROUTEID ;

      ROUTE_SET::const_iterator iterSet = routeSet.begin();
      while ( iterSet != routeSet.end() )
      {
         UINT64 reqId = _pReqID->inc();
         pHeader->requestID = reqId;
         MsgRouteID routeID;
         routeID.value = *iterSet;
         _pNetWork->syncSend( routeID, header );
         ++iterSet;
      }
      PD_TRACE_EXIT ( SDB_NETMLTRTAGT_MULTSYNCSND );
   }

   INT32 netMultiRouteAgent::updateRoute( const _MsgRouteID &id,
                                          const CHAR *pHost,
                                          const CHAR *pService)
   {
      SDB_ASSERT( _pNetWork, "_pNetWork can't be NULL") ;
      return _pNetWork->updateRoute( id, pHost, pService );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_HNDCLS, "netMultiRouteAgent::handleClose" )
   void netMultiRouteAgent::handleClose( const NET_HANDLE &handle,
                                         MsgRouteID id )
   {
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_HNDCLS );
      ossScopedLock _lock(&_mutex, SHARED) ;
      COORD_SESSION_MAP::iterator it = _sessionMap.begin();
      while ( it != _sessionMap.end() )
      {
         it->second->disConnect( id );
         ++it;
      }
      PD_TRACE_EXIT ( SDB_NETMLTRTAGT_HNDCLS );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_HNDMSG, "netMultiRouteAgent::handleMsg" )
   INT32 netMultiRouteAgent::handleMsg( const NET_HANDLE &handle,
                                        const _MsgHeader *header,
                                        const CHAR *msg )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_HNDMSG );

      CHAR *pMsgRsp = (CHAR *)SDB_OSS_MALLOC( header->messageLength );
      PD_CHECK( pMsgRsp, SDB_OOM, error, PDERROR,
                "Memory malloc failed(size = %d)", header->messageLength ) ;

      ossMemcpy( pMsgRsp, msg, header->messageLength );

      {
         ossScopedLock _lock( &_mutex, SHARED ) ;
         COORD_SESSION_MAP::iterator it = _sessionMap.find( header->TID );
         if ( _sessionMap.end() != it )
         {
            it->second->postEvent( pmdEDUEvent( PMD_EDU_EVENT_MSG,
                                                PMD_EDU_MEM_ALLOC,
                                                pMsgRsp ) ) ;
         }
         else
         {
            PD_LOG( PDWARNING, "Recieve expired msg[opCode:[%d]%d, TID:%d,"
                    "ReqID:%lld] from node[%d:%d:%d]",
                    IS_REPLY_TYPE( header->opCode ),
                    GET_REQUEST_TYPE( header->opCode ), header->TID,
                    header->requestID, header->routeID.columns.groupID,
                    header->routeID.columns.nodeID,
                    header->routeID.columns.serviceID ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB_NETMLTRTAGT_HNDMSG, rc );
      return rc ;
   error:
      if ( pMsgRsp )
      {
         SDB_OSS_FREE( pMsgRsp ) ;
      }
      goto done ;
   }

   UINT64 netMultiRouteAgent::reqIDNew()
   {
      return _pReqID->inc();
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_ADDSESS, "netMultiRouteAgent::addSession" )
   INT32 netMultiRouteAgent::addSession( pmdEDUCB *pEduCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_ADDSESS );
      CoordSession *pSession = NULL;
      pSession = SDB_OSS_NEW CoordSession( pEduCB );
      if ( NULL == pSession )
      {
         rc = SDB_OOM;
         goto done ;
      }
      pEduCB->setCoordSession( pSession );
      {
         ossScopedLock _lock(&_mutex, EXCLUSIVE) ;
         _sessionMap.insert( COORD_SESSION_MAP::value_type(pEduCB->getTID(),
                                                pSession ));
      }
   done :
      PD_TRACE_EXITRC ( SDB_NETMLTRTAGT_ADDSESS, rc );
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_GETSESS, "netMultiRouteAgent::getSession" )
   CoordSession *netMultiRouteAgent::getSession( UINT32 tID )
   {
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_GETSESS );
      CoordSession *pSession = NULL;
      COORD_SESSION_MAP::iterator it;
      ossScopedLock _lock(&_mutex, SHARED) ;
      it = _sessionMap.find( tID );
      if ( it != _sessionMap.end() )
      {
         pSession = it->second;
      }
      PD_TRACE_EXIT ( SDB_NETMLTRTAGT_GETSESS );
      return pSession;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_DELSESS, "netMultiRouteAgent::delSession" )
   void netMultiRouteAgent::delSession( UINT32 tID )
   {
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_DELSESS );
      CoordSession *pSession = NULL;
      {
         COORD_SESSION_MAP::iterator it;
         ossScopedLock _lock(&_mutex, EXCLUSIVE) ;
         it = _sessionMap.find( tID );
         if ( it != _sessionMap.end() )
         {
            pSession = it->second;
            _sessionMap.erase( it );
         }
      }
      if ( pSession )
      {
         ROUTE_SET routeSet;
         pSession->getAllSessionRoute( routeSet );
         MsgOpDisconnect msgDisconnect;
         msgDisconnect.header.messageLength = sizeof( MsgOpDisconnect );
         msgDisconnect.header.opCode = MSG_BS_DISCONNECT;
         msgDisconnect.header.requestID = 0;
         msgDisconnect.header.routeID.value = 0;
         msgDisconnect.header.TID = tID;
         multiSyncSend( routeSet, (void *)&msgDisconnect );
         SDB_OSS_DEL pSession;
         pSession = NULL;
      }
      PD_TRACE_EXIT ( SDB_NETMLTRTAGT_DELSESS );
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB_NETMLTRTAGT_ISSUBSESSCONN, "netMultiRouteAgent::isSubSessionConnected" )
   BOOLEAN netMultiRouteAgent::isSubSessionConnected( UINT32 tID,
                                                      const MsgRouteID &ID )
   {
      PD_TRACE_ENTRY ( SDB_NETMLTRTAGT_ISSUBSESSCONN );
      BOOLEAN hasConnected = FALSE;
      CoordSession *pSession = NULL;

      {
         COORD_SESSION_MAP::iterator it;
         ossScopedLock _lock(&_mutex, SHARED) ;
         it = _sessionMap.find( tID );
         if ( it != _sessionMap.end() )
         {
            pSession = it->second ;
         }
      }
      if ( pSession != NULL )
      {
         hasConnected = pSession->isSubsessionConnected( ID );
      }
      PD_TRACE1 ( SDB_NETMLTRTAGT_ISSUBSESSCONN, PD_PACK_INT(hasConnected) );
      PD_TRACE_EXIT ( SDB_NETMLTRTAGT_ISSUBSESSCONN );
      return hasConnected;
   }
}
