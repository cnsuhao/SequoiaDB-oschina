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

   Source File Name = rtnCoordLobDispatcher.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/10/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnCoordLobDispatcher.hpp"
#include "rtnCoordCommon.hpp"
#include "rtnTrace.hpp"
#include "pmdEDU.hpp"
#include "pmd.hpp"
#include "coordSession.hpp"

using namespace std ;

#define RTN_COORD_LOB_RETRY_TIMES 2

namespace engine
{
   _rtnCoordLobDispatcher::_rtnCoordLobDispatcher()
   :_retryTimes( 0 )
   {
      ossMemset( &_header, 0, sizeof( _header ) ) ;
      _header.contextID = -1 ;
      _header.header.opCode = MSG_NULL ;
      UINT32 *tmp = ( UINT32 * )_alignBuf ;
      *tmp = 0 ;
   }

   _rtnCoordLobDispatcher::~_rtnCoordLobDispatcher()
   {
      clear() ;
   }

   void _rtnCoordLobDispatcher::clear( BOOLEAN releaseReply )
   {
      if ( releaseReply )
      {
         vector<MsgOpReply *>::iterator itr = _replyBuf.begin() ;
         for ( ; itr != _replyBuf.end(); ++itr )
         {
            SAFE_OSS_FREE( *itr ) ;
         }
      }
      _replyBuf.clear() ;
      _sendMap.clear() ;
      _tuples.clear() ;
      _options.clear() ;
      _retryTimes = 0 ;
      _header.header.messageLength = 0 ;
      _header.header.opCode = MSG_NULL ;
      _header.contextID = -1 ;
      _header.bsonLen = 0 ;
      _msgObj = BSONObj () ;
   }

   void _rtnCoordLobDispatcher::setContextID( MsgRouteID id,
                                              SINT64 contextID )
   {
      MSG_TUPLES::iterator itr = _tuples.find( id ) ;
      if ( _tuples.end() != itr )
      {
         itr->second.contextID = contextID ;
      }
      return ;
   }

   //PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOORDLOBDISPATCHER_CREATEMSG, "_rtnCoordLobDispatcher::createMsg" )
   INT32 _rtnCoordLobDispatcher::createMsg( INT32 opCode,
                                            INT32 version,
                                            const msgOptions &options,
                                            const bson::BSONObj &obj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOORDLOBDISPATCHER_CREATEMSG ) ;
      clear() ;
      _header.header.opCode = opCode ;
      _header.version = version ;
      _options = options ;
      _msgObj = obj ;
      if ( !_msgObj.isEmpty() )
      {
         _header.bsonLen = ossRoundUpToMultipleX( _msgObj.objsize(), 4 ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNCOORDLOBDISPATCHER_CREATEMSG, rc ) ;
      return rc ;
   }

   _rtnCoordLobDispatcher &
         _rtnCoordLobDispatcher::add( UINT32 groupID,
                                      const void *data,
                                      UINT32 len )
   {
      SDB_ASSERT( NULL != data && 0 != len, "can not be null" ) ;
      MsgRouteID id ;
      id.value = MSG_INVALID_ROUTEID ;
      id.columns.groupID = groupID ;
      return add( id, data, len ) ;
   }

   _rtnCoordLobDispatcher &
         _rtnCoordLobDispatcher::add( MsgRouteID id,
                                      const void *data,
                                      UINT32 len )
   {
      SDB_ASSERT( MSG_NULL != _header.header.opCode, "please create msg first" ) ;
      MSG_TUPLES::iterator itr =
                      _tuples.insert(
                       std::make_pair( id, msgTuple() ) ).first ;

      if ( itr->second.bodies.empty() )
      {
         const CHAR *body = ( const CHAR * )( &_header ) + sizeof( MsgHeader );
         itr->second.bodies.push_back(
                    netIOV( body,
                            sizeof( MsgOpLob ) - sizeof( MsgHeader ) ) ) ;
         if ( !_msgObj.isEmpty() )
         {
            itr->second.bodies.push_back( netIOV(
                                             _msgObj.objdata(),
                                             _msgObj.objsize() ) ) ;
            if ( ( UINT32 )_msgObj.objsize() < _header.bsonLen )
            {
               itr->second.bodies.push_back( netIOV( _alignBuf,
                                                     _header.bsonLen - _msgObj.objsize() ) ) ;
            }
         }
      }

      if ( NULL != data )
      {
         itr->second.bodies.push_back( netIOV( data, len ) ) ;
      }
      return *this ;
   }

   _rtnCoordLobDispatcher &
         _rtnCoordLobDispatcher::add( UINT32 groupID )
   {
      MsgRouteID id ;
      id.value = MSG_INVALID_ROUTEID ;
      id.columns.groupID = groupID ;
      return add( id, NULL, 0  ) ;
   }

   _rtnCoordLobDispatcher &
         _rtnCoordLobDispatcher::add( const MsgRouteID &id )
   {
      return add( id, NULL, 0  ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCOORDLOBDISPATCHER_ADDDONE, "_rtnCoordLobDispatcher::addDone" )
   void _rtnCoordLobDispatcher::addDone()
   {
      PD_TRACE_ENTRY( SDB__RTNCOORDLOBDISPATCHER_ADDDONE ) ;
      MSG_TUPLES::iterator itr = _tuples.begin() ;
      for ( ; itr != _tuples.end(); ++itr )
      {
         itr->second.totalLen = sizeof( MsgHeader ) ;

         for ( netIOVec::const_iterator bitr = itr->second.bodies.begin() ;
               bitr != itr->second.bodies.end() ;
               ++bitr )
         {
            itr->second.totalLen += bitr->iovLen ;
         }
      }
      PD_TRACE_EXIT( SDB__RTNCOORDLOBDISPATCHER_ADDDONE ) ;
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCOORDLOBDISPATCHER_WAIT4REPLY, "_rtnCoordLobDispatcher::wait4Reply" )
   INT32 _rtnCoordLobDispatcher::wait4Reply( pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCOORDLOBDISPATCHER_WAIT4REPLY ) ;
      REPLY_QUE replyQueue ;

      rc = _sendMsg( cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to send msg:%d", rc ) ;
         goto error ;
      }

      if ( _options.dispatchedByGroupID )
      {
         rc = _getReplyWithRetry( cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to dispatch msg:%d, rc:%d",
                    _header.header.opCode, rc ) ;
            goto error ;
         }
      }
      else
      {
         rc = _getReply( cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to dispatch msg:%d, rc:%d",
                    _header.header.opCode, rc ) ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCOORDLOBDISPATCHER_WAIT4REPLY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCOORDLOBDISPATCHER__GETREPLY, "_rtnCoordLobDispatcher::_getReply" )
   INT32 _rtnCoordLobDispatcher::_getReply( pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCOORDLOBDISPATCHER__GETREPLY ) ;
      REPLY_QUE replyQueue ;
      INT32 replyType = MAKE_REPLY_TYPE( _header.header.opCode ) ;
      rc = rtnCoordGetReply( cb, _sendMap, replyQueue, replyType,
                             TRUE, TRUE ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get reply msg:%d", rc ) ;
         goto error ;
      }

      while ( !replyQueue.empty() )
      {
         MsgOpReply *replyHeader = ( MsgOpReply * )( replyQueue.front() ) ;
         replyQueue.pop() ;
         MsgRouteID id = replyHeader->header.routeID ;
         SDB_ASSERT( 1 == _tuples.count( id ), "impossible" ) ;
         _replyBuf.push_back( replyHeader ) ;
         _tuples.erase( id ) ;
      }
   done:
      while ( !replyQueue.empty() )
      {
         CHAR *p = replyQueue.front();
         replyQueue.pop();
         SAFE_OSS_FREE( p ) ;
      }

      PD_TRACE_EXITRC( SDB__RTNCOORDLOBDISPATCHER__GETREPLY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCOORDLOBDISPATCHER__GETREPLYWITHRETRY, "_rtnCoordLobDispatcher::_getReplyWithRetry" )
   INT32 _rtnCoordLobDispatcher::_getReplyWithRetry( pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCOORDLOBDISPATCHER__GETREPLY ) ;
      REPLY_QUE replyQueue ;
      INT32 lastErrRc = SDB_OK ;

      INT32 replyType = MAKE_REPLY_TYPE( _header.header.opCode ) ;
      rc = rtnCoordGetReply( cb, _sendMap, replyQueue, replyType,
                             TRUE, TRUE ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get reply msg:%d", rc ) ;
         goto error ;
      }

      while ( !replyQueue.empty() )
      {
         MsgOpReply *replyHeader = ( MsgOpReply * )( replyQueue.front() ) ;
         replyQueue.pop() ;
         INT32 flag = replyHeader->flags ;
         if ( SDB_OK != flag )
         {
            lastErrRc = flag ;
         }
         MsgRouteID id ;
         id.value = MSG_INVALID_ROUTEID ;
         id.columns.groupID = replyHeader->header.routeID.columns.groupID ;
         SDB_ASSERT( 1 == _tuples.count( id ), "impossible" ) ;

         if ( SDB_OK == flag )
         {
            _replyBuf.push_back( replyHeader ) ;
            _tuples.erase( id ) ;
            continue ;
         }
         else if ( SDB_CLS_FULL_SYNC == flag ) 
         {
            if ( _options.dispatchedByGroupID )
            {
               cb->getCoordSession()->removeLastNode( id.columns.groupID ) ;
               rtnCoordUpdateNodeStatByRC( replyHeader->header.routeID,
                                           flag );
            }
            
            continue ;
         }
         else if ( SDB_CLS_NOT_PRIMARY == flag )
         {
            if ( _options.dispatchedByGroupID )
            {
               cb->getCoordSession()->removeLastNode( id.columns.groupID ) ;
               CoordGroupInfoPtr groupInfoTmp ;
               INT32 rcTmp = rtnCoordGetGroupInfo( cb, id.columns.groupID, TRUE,
                                                   groupInfoTmp );
               if ( SDB_OK != rcTmp )
               {
                  PD_LOG( PDERROR, "failed to refresh group info:%d", rc ) ;
               }
            }
         }
         else if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == flag )
         {
            _replyBuf.push_back( replyHeader ) ;
            _tuples.erase( id ) ; 
            continue ;
         }
         else
         {
            _replyBuf.push_back( replyHeader ) ;
            _tuples.erase( id ) ;
            continue ;
         }

         SDB_OSS_FREE( replyHeader ) ;
      }

      if ( _tuples.empty() )
      {
         rc = SDB_OK ;
         goto done ;
      }
      else if ( _retryTimes++ < RTN_COORD_LOB_RETRY_TIMES )
      {
         std::stringstream ss ;
         for ( MSG_TUPLES::const_iterator itr = _tuples.begin();
               itr != _tuples.end();
               ++itr )
         {
            MsgRouteID dst ;
            dst = itr->first ;
            ss << dst.columns.groupID << ":" << dst.columns.nodeID << " " ;
         }
         PD_LOG( PDEVENT, "the %dth time we retry to send msg:%s",
                 _retryTimes, ss.str().c_str() ) ;

         rc = wait4Reply( cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "we retry to get reply, but also failed:%d", rc ) ;
            goto error ;
         }
      }
      else
      {
         rc = lastErrRc ;
         PD_LOG( PDERROR, "sth wrong happened, failed to get reply:%d", rc ) ;
         goto error ;
      }
      
   done:
      while ( !replyQueue.empty() )
      {
         CHAR *p = replyQueue.front();
         replyQueue.pop();
         SAFE_OSS_FREE( p ) ;
      }
      PD_TRACE_EXITRC( SDB__RTNCOORDLOBDISPATCHER__GETREPLY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNCOORDLOBDISPATCHER__SENDMSG, "_rtnCoordLobDispatcher::_sendMsg" )
   INT32 _rtnCoordLobDispatcher::_sendMsg( pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNCOORDLOBDISPATCHER__SENDMSG ) ;
      netMultiRouteAgent *routeAgent = pmdGetKRCB()->getCoordCB()->
                                       getRouteAgent() ;
      MSG_TUPLES::const_iterator itr = _tuples.begin() ;
      for ( ; itr != _tuples.end(); ++itr )
      {
         _header.header.messageLength = itr->second.totalLen ;
         _header.contextID = itr->second.contextID ;
         if ( !_options.dispatchedByGroupID )
         {
            rc = rtnCoordSendRequestToNode( &( _header.header ),
                                            itr->first,
                                            routeAgent,
                                            cb,
                                            itr->second.bodies,
                                             _sendMap ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to send msg to node[%d:%hd], rc:%d",
                    itr->first.columns.groupID, itr->first.columns.nodeID, rc ) ;
               goto error ;
            }
         }
         else
         {
            rc = rtnCoordSendRequestToNodeGroup( &( _header.header ),
                                                 itr->first.columns.groupID,
                                                 _options.onlyPrimary,
                                                 routeAgent,
                                                 cb, itr->second.bodies,
                                                 _sendMap ) ;
         }

         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to send msg to group:%d, rc:%d",
                    itr->first.columns.groupID, rc ) ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNCOORDLOBDISPATCHER__SENDMSG, rc ) ;
      return rc ;
   error:
      if ( !_sendMap.empty() )
      {
         rtnCoordClearRequest( cb, _sendMap ) ;
         _sendMap.clear() ;
      }
      goto done ;
   }   
}

