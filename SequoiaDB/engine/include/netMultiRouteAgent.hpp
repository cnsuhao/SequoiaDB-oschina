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

   Source File Name = netMultiRouteAgent.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#ifndef NETMULTIROUTEAGENT_HPP__
#define NETMULTIROUTEAGENT_HPP__

#include "core.hpp"
#include "pd.hpp"
#include "netRouteAgent.hpp"
#include "ossAtomic.hpp"
#include "netMsgHandler.hpp"
#include "msg.h"
#include "pmdEDU.hpp"
#include "coordDef.hpp"

namespace engine
{
   typedef  std::map<UINT64, MsgRouteID>           REQUESTID_MAP ;
   typedef  std::set<UINT64>                       ROUTE_SET;
   class    CoordSession ;

   class netMultiRouteAgent : public _netMsgHandler
   {
   typedef std::map<SINT64, CoordSession*>         COORD_SESSION_MAP;

   public:
      netMultiRouteAgent() ;
      ~netMultiRouteAgent() ;

      void  setNetWork( _netRouteAgent *pNetWork)   ;

   public:
      virtual INT32 handleMsg( const NET_HANDLE &handle,
                               const _MsgHeader *header,
                               const CHAR *msg ) ;

      virtual void  handleClose( const NET_HANDLE &handle, _MsgRouteID id ) ;

      INT32 updateRoute( const _MsgRouteID &id,
                         const CHAR *pHost,
                         const CHAR *pService ) ;

      INT32 addSession( pmdEDUCB *pEduCB );

      void  delSession( UINT32 tID );

      BOOLEAN isSubSessionConnected( UINT32 tID, const MsgRouteID &ID );

      CoordSession *getSession( UINT32 tID );

      UINT64 reqIDNew() ;

   protected:
      INT32 syncSend( const MsgRouteID &id, void *header, UINT64 &reqID,
                      pmdEDUCB *pEduCB, void *body = NULL, UINT32 bodyLen = 0 ) ;

      INT32 syncSend( const MsgRouteID &id, MsgHeader *header,
                      const netIOVec &iov, UINT64 &reqID,
                      pmdEDUCB *pEduCB ) ;

      INT32 syncSendWithoutCheck( const MsgRouteID &id, void *header,
                                 UINT64 &reqID, pmdEDUCB *pEduCB,
                                 void *body = NULL, UINT32 bodyLen = 0 );

      void  multiSyncSend( const ROUTE_SET &routeSet, void *header ) ;

   public:
      friend INT32 rtnCoordGetRemoteCataGroupInfoByAddr ( pmdEDUCB *cb,
                                                CoordGroupInfoPtr &groupInfo );

      friend void rtnCoordSendRequestToNodesWithOutReply( void *pBuffer,
                                                ROUTE_SET &nodes,
                                                netMultiRouteAgent *pRouteAgent );

      friend INT32 rtnCoordSendRequestToNodeWithoutCheck( void *pBuffer,
                                                const MsgRouteID &routeID,
                                                netMultiRouteAgent *pRouteAgent,
                                                pmdEDUCB *cb,
                                                REQUESTID_MAP &sendNodes );

      friend INT32 rtnCoordSendRequestToNodeWithoutReply( void *pBuffer,
                                                MsgRouteID &routeID,
                                                netMultiRouteAgent *pRouteAgent );

      friend INT32 rtnCoordSendRequestToNode( void *pBuffer,
                                    MsgRouteID routeID,
                                    netMultiRouteAgent *pRouteAgent,
                                    pmdEDUCB *cb,
                                    REQUESTID_MAP &sendNodes );

      friend INT32 rtnCoordSendRequestToNodeGroup( CHAR *pBuffer,
                                          UINT32 groupID,
                                          BOOLEAN isSendPrimary,
                                          netMultiRouteAgent *pRouteAgent,
                                          pmdEDUCB *cb,
                                          REQUESTID_MAP &sendNodes,
                                          MSG_ROUTE_SERVICE_TYPE type );

      friend INT32 rtnCoordSendRequestToNodeGroup( MsgHeader *pBuffer,
                                          UINT32 groupID,
                                          BOOLEAN isSendPrimary,
                                          netMultiRouteAgent *pRouteAgent,
                                          pmdEDUCB *cb,
                                          const netIOVec &iov,
                                          REQUESTID_MAP &sendNodes,
                                          MSG_ROUTE_SERVICE_TYPE type );

      friend INT32 rtnCoordSendRequestToPrimary( CHAR *pBuffer,
                                       const CoordGroupInfoPtr &groupInfo,
                                       REQUESTID_MAP &sendNodes,
                                       netMultiRouteAgent *pRouteAgent,
                                       MSG_ROUTE_SERVICE_TYPE type,
                                       pmdEDUCB *cb );

      friend INT32 rtnCoordSendRequestToPrimary( MsgHeader *pBuffer,
                                       const CoordGroupInfoPtr &groupInfo,
                                       REQUESTID_MAP &sendNodes,
                                       netMultiRouteAgent *pRouteAgent,
                                       const netIOVec &iov,
                                       MSG_ROUTE_SERVICE_TYPE type,
                                       pmdEDUCB *cb );

      friend INT32 rtnCoordSendRequestToOne( CHAR *pBuffer,
                                 CoordGroupInfoPtr &groupInfo,
                                 REQUESTID_MAP &sendNodes,
                                 netMultiRouteAgent *pRouteAgent,
                                 MSG_ROUTE_SERVICE_TYPE type,
                                 pmdEDUCB *cb );

      friend INT32 rtnCoordSendRequestToOne( MsgHeader *pBuffer,
                                 CoordGroupInfoPtr &groupInfo,
                                 REQUESTID_MAP &sendNodes,
                                 netMultiRouteAgent *pRouteAgent,
                                 const netIOVec &iov,
                                 MSG_ROUTE_SERVICE_TYPE type,
                                 pmdEDUCB *cb );

      friend INT32 rtnCoordSendRequestToLast( CHAR * pBuffer,
                                    const CoordGroupInfoPtr & groupInfo,
                                    REQUESTID_MAP & sendNodes,
                                    netMultiRouteAgent * pRouteAgent,
                                    pmdEDUCB *cb );

      friend INT32 rtnCoordSendRequestToNode( void *pBuffer,
                                    MsgRouteID routeID,
                                    netMultiRouteAgent *pRouteAgent,
                                    pmdEDUCB *cb,
                                    const netIOVec &iov,
                                    REQUESTID_MAP &sendNodes ) ;

   private:
      _netRouteAgent       *_pNetWork;
      ossSpinSLatch        _mutex ;
      ossAtomic64          *_pReqID;
      COORD_SESSION_MAP    _sessionMap;
   };
}

#endif
