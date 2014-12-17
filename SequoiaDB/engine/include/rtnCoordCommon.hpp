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

   Source File Name = rtnCoordCommon.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#ifndef RTNCOORDCOMMON_HPP__
#define RTNCOORDCOMMON_HPP__

#include "coordCB.hpp"
#include "rtnContext.hpp"
#include "../bson/bson.h"

using namespace bson ;

namespace engine
{
   typedef std::queue<CHAR *>  REPLY_QUE;
   typedef std::vector< CoordGroupInfoPtr > GROUP_VEC;
   typedef  std::map< UINT64, INT32 >     ROUTE_RC_MAP;

   INT32 rtnCoordGetReply ( pmdEDUCB *cb, REQUESTID_MAP &requestIdMap,
                           REPLY_QUE &replyQue, const SINT32 opCode,
                           BOOLEAN isWaitAll = TRUE,
                           BOOLEAN clearReplyIfFailed = TRUE );

   INT32 rtnCoordCataQuery ( const CHAR *pCollectionName,
                             const bson::BSONObj &selector,
                             const bson::BSONObj &matcher,
                             const bson::BSONObj &orderBy,
                             const bson::BSONObj &hint,
                             INT32 flag,
                             pmdEDUCB *cb,
                             SINT64 numToSkip,
                             SINT64 numToReturn,
                             SINT64 &contextID );

   INT32 getServiceName ( bson::BSONElement &beService,
                          INT32 serviceType,
                          std::string &strServiceName );
   INT32 rtnCoordNodeQuery ( const CHAR *pCollectionName,
                             const bson::BSONObj &condition,
                             const bson::BSONObj &selector,
                             const bson::BSONObj &orderBy,
                             const bson::BSONObj &hint,
                             INT64 numToSkip, INT64 numToReturn,
                             CoordGroupList &groupLst,
                             pmdEDUCB *cb,
                             rtnContext **ppContext,
                             const CHAR *realCLName = NULL,
                             INT32 flag = 0 ) ;

   INT32 rtnCoordGetCataInfo( pmdEDUCB *cb,
                      const CHAR *pCollectionName,
                      BOOLEAN isNeedRefreshCata,
                      CoordCataInfoPtr &cataInfo );

   INT32 rtnCoordGetGroupInfo ( pmdEDUCB *cb,
                        UINT32 groupID,
                        BOOLEAN isNeedRefresh,
                        CoordGroupInfoPtr &groupInfo ) ;

   INT32 rtnCoordGetGroupInfo ( pmdEDUCB *cb,
                        const CHAR *groupName,
                        BOOLEAN isNeedRefresh,
                        CoordGroupInfoPtr &groupInfo ) ;


   INT32 rtnCoordGetCatGroupInfo ( pmdEDUCB *cb,
                           BOOLEAN isNeedRefresh,
                           CoordGroupInfoPtr &groupInfo );

   INT32 rtnCoordGetGroupsByCataInfo( const CoordCataInfoPtr &cataInfo,
                              CoordGroupList &sendGroupLst,
                              CoordGroupList &groupLst );

   void  rtnCoordRemoveGroup( UINT32 group ) ;

   void rtnCoordSendRequestToNodesWithOutReply( void *pBuffer,
                                                ROUTE_SET &nodes,
                                                netMultiRouteAgent *pRouteAgent );

   INT32 rtnCoordSendRequestToNodes( void *pBuffer,
                                    ROUTE_SET &nodes,
                                    netMultiRouteAgent *pRouteAgent,
                                    pmdEDUCB *cb,
                                    REQUESTID_MAP &sendNodes,
                                    ROUTE_RC_MAP &failedNodes );

   INT32 rtnCoordSendRequestToNode( void *pBuffer,
                                    MsgRouteID routeID,
                                    netMultiRouteAgent *pRouteAgent,
                                    pmdEDUCB *cb,
                                    REQUESTID_MAP &sendNodes );

   INT32 rtnCoordSendRequestToNode( void *pBuffer,
                                    MsgRouteID routeID,
                                    netMultiRouteAgent *pRouteAgent,
                                    pmdEDUCB *cb,
                                    const netIOVec &iov,
                                    REQUESTID_MAP &sendNodes );

   INT32 rtnCoordSendRequestToNodeWithoutReply( void *pBuffer,
                                                MsgRouteID &routeID,
                                                netMultiRouteAgent *pRouteAgent );

   INT32 rtnCoordSendRequestToNodeWithoutCheck( void *pBuffer,
                                                const MsgRouteID &routeID,
                                                netMultiRouteAgent *pRouteAgent,
                                                pmdEDUCB *cb,
                                                REQUESTID_MAP &sendNodes );

   INT32 rtnCoordSendRequestToNodeGroup( CHAR *pBuffer,
                                         UINT32 groupID,
                                         BOOLEAN isSendPrimary,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb,
                                         REQUESTID_MAP &sendNodes,
                                         MSG_ROUTE_SERVICE_TYPE type ) ;

   INT32 rtnCoordSendRequestToNodeGroups( CHAR *pBuffer,
                                  CoordGroupList &groupLst,
                                  BOOLEAN isSendPrimary,
                                  netMultiRouteAgent *pRouteAgent,
                                  pmdEDUCB *cb,
                                  REQUESTID_MAP &sendNodes,
                                  MSG_ROUTE_SERVICE_TYPE type =
                                  MSG_ROUTE_SHARD_SERVCIE );

   INT32 rtnCoordSendRequestToNodeGroup( MsgHeader *pBuffer,
                                  UINT32 groupID,
                                  BOOLEAN isSendPrimary,
                                  netMultiRouteAgent *pRouteAgent,
                                  pmdEDUCB *cb,
                                  const netIOVec &iov,
                                  REQUESTID_MAP &sendNodes,
                                  MSG_ROUTE_SERVICE_TYPE type =
                                  MSG_ROUTE_SHARD_SERVCIE );

   INT32 rtnCoordSendRequestToNodeGroups( MsgHeader *pBuffer,
                                  CoordGroupList &groupLst,
                                  BOOLEAN isSendPrimary,
                                  netMultiRouteAgent *pRouteAgent,
                                  pmdEDUCB *cb,
                                  const netIOVec &iov,
                                  REQUESTID_MAP &sendNodes,
                                  MSG_ROUTE_SERVICE_TYPE type =
                                  MSG_ROUTE_SHARD_SERVCIE );


   INT32 rtnCoordSendRequestToPrimary( CHAR *pBuffer,
                               const CoordGroupInfoPtr &groupInfo,
                               REQUESTID_MAP &sendNodes,
                               netMultiRouteAgent *pRouteAgent,
                               MSG_ROUTE_SERVICE_TYPE type,
                               pmdEDUCB *cb );

   INT32 rtnCoordSendRequestToPrimary( MsgHeader *pBuffer,
                               const CoordGroupInfoPtr &groupInfo,
                               REQUESTID_MAP &sendNodes,
                               netMultiRouteAgent *pRouteAgent,
                               const netIOVec &iov,
                               MSG_ROUTE_SERVICE_TYPE type,
                               pmdEDUCB *cb );

   INT32 rtnCoordSendRequestToOne( CHAR *pBuffer,
                           CoordGroupInfoPtr &groupInfo,
                           REQUESTID_MAP &sendNodes,
                           netMultiRouteAgent *pRouteAgent,
                           MSG_ROUTE_SERVICE_TYPE type,
                           pmdEDUCB *cb ) ;

   INT32 rtnCoordSendRequestToOne( MsgHeader *pBuffer,
                           CoordGroupInfoPtr &groupInfo,
                           REQUESTID_MAP &sendNodes,
                           netMultiRouteAgent *pRouteAgent,
                           const netIOVec &iov,
                           MSG_ROUTE_SERVICE_TYPE type,
                           pmdEDUCB *cb ) ;

   INT32 rtnCoordSendRequestToLast ( CHAR *pBuffer,
                             const CoordGroupInfoPtr &groupInfo,
                             REQUESTID_MAP &sendNodes,
                             netMultiRouteAgent *pRouteAgent,
                             pmdEDUCB *cb ) ;


   INT32 rtnCoordGetLocalGroupInfo ( UINT32 groupID,
                             CoordGroupInfoPtr &groupInfo ) ;

   INT32 rtnCoordGetLocalGroupInfo ( const CHAR *groupName,
                             CoordGroupInfoPtr &groupInfo ) ;

   INT32 rtnCoordGetRemoteCataGroupInfoByAddr ( pmdEDUCB *cb,
                                        CoordGroupInfoPtr &groupInfo );

   INT32 rtnCoordGetRemoteGroupInfo ( pmdEDUCB *cb,
                              UINT32 groupID,
                              const CHAR *groupName,
                              CoordGroupInfoPtr &groupInfo,
                              BOOLEAN addToLocal=TRUE ) ;

   INT32 rtnCoordGetLocalCatGroupInfo ( CoordGroupInfoPtr &groupInfo );

   INT32 rtnCoordGetRemoteCatGroupInfo ( pmdEDUCB *cb,
                                 CoordGroupInfoPtr &groupInfo );

   INT32 rtnCoordGetLocalCata( const CHAR *pCollectionName,
                       CoordCataInfoPtr &cataInfo ) ;

   INT32 rtnCoordGetRemoteCata( pmdEDUCB *cb,
                        const CHAR *pCollectionName,
                        CoordCataInfoPtr &cataInfo );

   INT32 rtnCoordProcessQueryCatReply ( MsgCatQueryCatRsp *pReply,
                                CoordCataInfoPtr &cataInfo );

   INT32 rtnCoordProcessGetGroupReply ( MsgCatGroupRes *pReply,
                                CoordGroupInfoPtr &groupInfo );

   INT32 rtnCoordUpdateRoute ( CoordGroupInfoPtr &groupInfo,
                       netMultiRouteAgent *pRouteAgent,
                       MSG_ROUTE_SERVICE_TYPE type ) ;

   INT32 rtnCoordGetAllGroupList( pmdEDUCB * cb, GROUP_VEC &groupLst,
                                  const BSONObj *query = NULL,
                                  BOOLEAN exceptCata = FALSE,
                                  BOOLEAN exceptCoord = TRUE ) ;

   INT32 rtnCoordReadALine( const CHAR *&pInput, CHAR *pOutput );

   void rtnCoordClearRequest( pmdEDUCB *cb, REQUESTID_MAP &sendNodes );

   INT32 rtnCoordGetSubCLsByGroups( const CoordSubCLlist &subCLList,
                                    const CoordGroupList &sendGroupList,
                                    pmdEDUCB *cb,
                                    CoordGroupSubCLMap &groupSubCLMap );

   INT32 rtnCoordParseGroupList( pmdEDUCB *cb, const BSONObj &obj,
                                 CoordGroupList &groupList ) ;

   enum FILTER_BSON_ID
   {
      FILTER_ID_MATCHER    = 1,
      FILTER_ID_SELECTOR,
      FILTER_ID_ORDERBY,
      FILTER_ID_HINT
   } ;
   INT32 rtnCoordParseGroupList( pmdEDUCB *cb, MsgOpQuery *pMsg,
                                 FILTER_BSON_ID filterObjID,
                                 CoordGroupList &groupList ) ;

   INT32 rtnCoordGetAllGroupList( pmdEDUCB * cb, CoordGroupList &groupList,
                                  const BSONObj *query = NULL,
                                  BOOLEAN exceptCata = FALSE,
                                  BOOLEAN exceptCoord = TRUE ) ;

   INT32 rtnGroupList2GroupPtr( pmdEDUCB *cb, CoordGroupList &groupList,
                                GROUP_VEC &groupPtrs ) ;
   INT32 rtnGroupPtr2GroupList( pmdEDUCB *cb, GROUP_VEC &groupPtrs,
                                CoordGroupList &groupList ) ;

   enum NODE_SEL_STY
   {
      NODE_SEL_ALL         = 1,
      NODE_SEL_PRIMARY
   } ;
   INT32 rtnCoordGetGroupNodes( pmdEDUCB *cb, const BSONObj &filterObj,
                                NODE_SEL_STY emptyFilterSel,
                                CoordGroupList &groupList, ROUTE_SET &nodes ) ;

   INT32 rtnCoordGetNodePos( pmdEDUCB *cb,
                           const CoordGroupInfoPtr &groupInfo,
                           UINT32 random,
                           UINT32 &pos );

   void rtnCoordUpdateNodeStatByRC( MsgRouteID &routeID,
                                    INT32 retCode );

   BOOLEAN rtnCoordWriteRetryRC( INT32 retCode );

   INT32 rtnCataChangeNtyToAllNodes( pmdEDUCB *cb ) ;

}

#endif //RTNCOORDCOMMON_HPP__

