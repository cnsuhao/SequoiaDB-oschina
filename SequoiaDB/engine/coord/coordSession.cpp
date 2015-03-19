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

   Source File Name = coordSession.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/
#include "coordSession.hpp"
#include "pmdDef.hpp"
#include "pdTrace.hpp"
#include "coordTrace.hpp"
#include "pmd.hpp"
#include "rtnCoordCommon.hpp"

namespace engine
{
   CoordSession::CoordSession( pmdEDUCB *pEduCB )
   {
      _pEduCB = pEduCB;
      _preferReplType = pmdGetKRCB()->getOptionCB()->preferedReplica() ;
   }

   INT32 CoordSession::getPreferReplType()
   {
      return _preferReplType;
   }

   void CoordSession::setPreferReplType( INT32 type )
   {
      if ( PREFER_REPL_TYPE_MIN < type &&
           type < PREFER_REPL_TYPE_MAX &&
           type != _preferReplType )
      {
         _preferReplType = type;
         _lastNodeMap.clear() ;
      }
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_COORDSN_DISCONN, "CoordSession::disConnect" )
   INT32 CoordSession::disConnect( const MsgRouteID &routeID )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_COORDSN_DISCONN );
      BOOLEAN hasSession = FALSE;
      {
         ossScopedLock _lock( &_mutex ) ;
         COORD_SUBSESSION_MAP::iterator it;
         it = _subSessionMap.find( routeID.value );
         if ( it != _subSessionMap.end() )
         {
            hasSession = TRUE;
            it->second.isConnected = FALSE;
         }
      }

      if ( hasSession )
      {
         MsgOpReply *pMsg = (MsgOpReply *)SDB_OSS_MALLOC( sizeof(MsgOpReply) );
         if ( NULL != pMsg )
         {
            pMsg->header.messageLength = sizeof(MsgOpReply);
            pMsg->header.opCode = MSG_COOR_REMOTE_DISC;
            pMsg->header.routeID = routeID;
            pMsg->header.requestID = pmdGetKRCB()->getCoordCB(
               )->getRouteAgent()->reqIDNew();
            pMsg->flags = SDB_COORD_REMOTE_DISC;
            pMsg->numReturned = 0;
            _pEduCB->postEvent ( pmdEDUEvent( PMD_EDU_EVENT_MSG,
                                              PMD_EDU_MEM_ALLOC,
                                              pMsg ) ) ;
         }
         else
         {
            rc = SDB_OOM;
            PD_LOG( PDERROR, "malloc failed(size=%d)", sizeof(MsgHeader) );
         }
      }

      PD_TRACE_EXITRC ( SDB_COORDSN_DISCONN, rc );
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_COORDSN_CHECKREMOTEROUTEID, "CoordSession::checkRemoteRouteID" )
   INT32 CoordSession::checkRemoteRouteID( const MsgRouteID & routeID )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_COORDSN_CHECKREMOTEROUTEID );
      netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->getCoordCB()->getRouteAgent();
      SDB_ASSERT( _pEduCB, "_pEduCB can't be NULL!" );
      MsgCoordCheckRouteID msgReq;
      msgReq.header.requestID = 0;
      msgReq.header.messageLength = sizeof( MsgCoordCheckRouteID );
      msgReq.header.opCode = MSG_COOR_CHECK_ROUTEID_REQ;
      msgReq.header.routeID.value = 0;
      msgReq.header.TID = _pEduCB->getTID();
      msgReq.dstRouteID = routeID;
      REQUESTID_MAP requestIdMap;
      REPLY_QUE replyQue;

      rc = rtnCoordSendRequestToNodeWithoutCheck( (void *)(&msgReq), routeID,
                                                pRouteAgent, _pEduCB,
                                                requestIdMap );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to send the message to the node"
                  "(groupID=%u, nodeID=%u, serviceID=%u)",
                  routeID.columns.groupID,
                  routeID.columns.nodeID,
                  routeID.columns.serviceID );
      rc = rtnCoordGetReply( _pEduCB, requestIdMap, replyQue,
                             MSG_COOR_CHECK_ROUTEID_RSP );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get reply from node"
                  "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                  routeID.columns.groupID,
                  routeID.columns.nodeID,
                  routeID.columns.serviceID,
                  rc );
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = (MsgOpReply *)(replyQue.front());
         SDB_ASSERT( pReply, "pReply can't be NULL!" );
         replyQue.pop();
         rc = rc ? rc : pReply->flags;
         SDB_OSS_FREE( pReply );
      }
   done:
      PD_TRACE_EXIT ( SDB_COORDSN_CHECKREMOTEROUTEID );
      return rc;
   error:
      rtnCoordClearRequest( _pEduCB, requestIdMap );
      goto done;
   }

   void CoordSession::addSubSessionWithoutCheck( const MsgRouteID &routeID )
   {
      subSessionInfo subSession;
      subSession.routeID = routeID;
      subSession.isConnected = TRUE;
      ossScopedLock _lock( &_mutex ) ;
      _subSessionMap[routeID.value] = subSession;
   }

   INT32 CoordSession::addSubSession( const MsgRouteID &routeID )
   {
      INT32 rc = SDB_OK;
      COORD_SUBSESSION_MAP::iterator iterMap = _subSessionMap.find( routeID.value );
      if ( iterMap != _subSessionMap.end()
         && TRUE == iterMap->second.isConnected )
      {
         goto done;
      }
      {
         subSessionInfo subSession;
         subSession.routeID = routeID;
         subSession.isConnected = TRUE;
         ossScopedLock _lock( &_mutex ) ;
         _subSessionMap[routeID.value] = subSession;
      }
      rc = checkRemoteRouteID( routeID );
      if ( rc )
      {
         {
         ossScopedLock _lock( &_mutex ) ;
         iterMap = _subSessionMap.find( routeID.value );
         if ( iterMap != _subSessionMap.end() )
         {
            iterMap->second.isConnected = FALSE;
         }
         }
         PD_LOG( PDERROR,
               "check remote routeID faled(rc=%d)", rc );
         goto error;
      }
   done:
      return rc;
   error:
      goto done;
   }

   BOOLEAN CoordSession::delSubSession( const MsgRouteID & routeID )
   {
      UINT32 num = 0;
      {
         ossScopedLock _lock( &_mutex ) ;
         num = _subSessionMap.erase( routeID.value );
      }
      if ( 0 == num )
      {
         return FALSE;
      }
      else
      {
         return TRUE;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_COORDSN_GETALLSR, "CoordSession::getAllSessionRoute" )
   void CoordSession::getAllSessionRoute( ROUTE_SET & routeSet )
   {
      PD_TRACE_ENTRY ( SDB_COORDSN_GETALLSR );
      ossScopedLock _lock( &_mutex ) ;
      COORD_SUBSESSION_MAP::iterator it = _subSessionMap.begin();
      while ( it != _subSessionMap.end() )
      {
         if ( it->second.isConnected )
         {
            routeSet.insert( it->second.routeID.value );
         }
         ++it;
      }
      PD_TRACE_EXIT ( SDB_COORDSN_GETALLSR );
   }

   void CoordSession::postEvent( pmdEDUEvent const & data )
   {
      _pEduCB->postEvent( data );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_COORDSN_ISSUBSNCONN, "CoordSession::isSubsessionConnected" )
   BOOLEAN CoordSession::isSubsessionConnected( const MsgRouteID &routeID )
   {
      PD_TRACE_ENTRY ( SDB_COORDSN_ISSUBSNCONN );
      BOOLEAN hasConnected = FALSE;
      ossScopedLock _lock( &_mutex ) ;
      COORD_SUBSESSION_MAP::iterator it;
      it = _subSessionMap.find( routeID.value );
      if ( it != _subSessionMap.end() )
      {
         hasConnected = it->second.isConnected;
      }
      PD_TRACE_EXIT ( SDB_COORDSN_ISSUBSNCONN );
      return hasConnected;
   }

   void CoordSession::addLastNode( const MsgRouteID &routeID )
   {
      _lastNodeMap[routeID.columns.groupID] = routeID;
   }

   void CoordSession::removeLastNode( UINT32 groupID )
   {
      _lastNodeMap.erase(groupID);
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_COORDSN_GETLASTND, "CoordSession::getLastNode" )
   MsgRouteID CoordSession::getLastNode( UINT32 groupID )
   {
      PD_TRACE_ENTRY ( SDB_COORDSN_GETLASTND );
      MsgRouteID routeID;
      COORD_LASTNODE_MAP::iterator it = _lastNodeMap.find( groupID );
      if ( it != _lastNodeMap.end() )
      {
         routeID = it->second ;
      }
      else
      {
         routeID.value = 0 ;
      }
      PD_TRACE_EXIT ( SDB_COORDSN_GETLASTND );
      return routeID;
   }

   void CoordSession::addRequest( const UINT64 reqID, const MsgRouteID &routeID )
   {
      _requestMap[ reqID ] = routeID;
   }

   void CoordSession::delRequest( const UINT64 reqID )
   {
      _requestMap.erase( reqID );
   }

   /*void CoordSession::delRequest( const MsgRouteID &routeID )
   {
      REQUESTID_MAP::iterator iterMap = _requestMap.begin();
      while( iterMap != _requestMap.end() )
      {
         if ( iterMap->second.value == routeID.value )
         {
            _requestMap.erase( iterMap++ );
            continue;
         }
         ++iterMap;
      }
   }*/

   void CoordSession::clearRequest()
   {
      _requestMap.clear();
   }

   BOOLEAN CoordSession::isValidResponse( const UINT64 reqID )
   {
      if ( _requestMap.find( reqID ) != _requestMap.end() )
      {
         return TRUE;
      }
      return FALSE;
   }

   BOOLEAN  CoordSession::isValidResponse( const MsgRouteID &routeID,
                                           const UINT64 reqID )
   {
      REQUESTID_MAP::iterator iterMap = _requestMap.begin();
      while( iterMap != _requestMap.end() )
      {
         if ( iterMap->second.value == routeID.value
            && iterMap->first <= reqID )
         {
            return TRUE;
         }
         ++iterMap;
      }
      return FALSE;
   }
}

