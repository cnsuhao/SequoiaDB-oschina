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

   Source File Name = rtnCoordCommon.cpp

   Descriptive Name = Runtime Coord Common

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

#include "rtnCoord.hpp"
#include "rtnCoordCommon.hpp"
#include "msg.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "rtnCoordQuery.hpp"
#include "msgMessage.hpp"
#include "coordCB.hpp"
#include "rtnContext.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "coordSession.hpp"
#include "rtnCoordCommands.hpp"
#include "rtn.hpp"

using namespace bson;

namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCATAQUERY, "rtnCoordCataQuery" )
   INT32 rtnCoordCataQuery ( const CHAR *pCollectionName,
                             const BSONObj &selector,
                             const BSONObj &matcher,
                             const BSONObj &orderBy,
                             const BSONObj &hint,
                             INT32 flag,
                             pmdEDUCB *cb,
                             SINT64 numToSkip,
                             SINT64 numToReturn,
                             SINT64 &contextID )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCATAQUERY ) ;
      pmdKRCB *pKrcb    = pmdGetKRCB();
      CoordCB *pCoordcb = pKrcb->getCoordCB();
      SDB_RTNCB *pRtncb = pKrcb->getRTNCB();
      netMultiRouteAgent *pRouteAgent = pCoordcb->getRouteAgent();
      CoordGroupInfoPtr cataGroupInfo;
      CHAR *pBuf = NULL;
      MsgOpQuery *pQueryMsg = NULL;
      INT32 bufferSize = 0;
      rtnContextCoord *pContext = NULL ;
      CoordGroupList groupLst;
      REPLY_QUE replyQue;
      REQUESTID_MAP sendNodes;
      BOOLEAN hasRetry = FALSE;
      MsgOpReply *pReply = NULL ;
      BOOLEAN takeOver = FALSE ;

      groupLst[CATALOG_GROUPID] = CATALOG_GROUPID;
      contextID = -1;

      rc = msgBuildQueryMsg( &pBuf, &bufferSize, pCollectionName, flag, 0,
                             numToSkip, numToReturn, &matcher, &selector,
                             &orderBy, &hint ) ;
      PD_RC_CHECK( rc, PDERROR, "failed to build the query-msg(rc=%d)", rc );

      pQueryMsg = (MsgOpQuery *)pBuf;
      pQueryMsg->header.TID = cb->getTID();

      rc = pRtncb->contextNew( RTN_CONTEXT_COORD, (rtnContext**)&pContext,
                               contextID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "failed to allocate context(rc=%d)", rc );
      rc = pContext->open( BSONObj(), BSONObj(), -1, 0 ) ;
      PD_RC_CHECK( rc, PDERROR, "Open context failed, rc: %d", rc ) ;

   retry:
      rc = rtnCoordSendRequestToNodeGroups( pBuf, groupLst, TRUE, pRouteAgent,
                                            cb, sendNodes,
                                            MSG_ROUTE_CAT_SERVICE ) ;
      PD_RC_CHECK( rc, PDERROR, "failed to send the message to catalog "
                   "node(rc=%d)", rc ) ;

      rc = rtnCoordGetReply( cb, sendNodes, replyQue, MSG_BS_QUERY_RES );
      PD_RC_CHECK( rc, PDERROR, "failed to get the reply(rc=%d)", rc );

      SDB_ASSERT( replyQue.size() == 1, "The replyQue size must be 1" );
      while ( !replyQue.empty() )
      {
         if ( pReply )
         {
            SDB_OSS_FREE( pReply ) ;
         }
         pReply = (MsgOpReply *)(replyQue.front()) ;
         replyQue.pop() ;
         rc = pReply->flags ;
      }
      if ( rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            goto done;
         }
         else if ( SDB_CLS_NOT_PRIMARY == rc && !hasRetry )
         {
            rc = rtnCoordGetRemoteCatGroupInfo( cb, cataGroupInfo );
            PD_RC_CHECK( rc, PDERROR, "failed to get catalog group info(rc=%d)",
                         rc );
            hasRetry = TRUE;
            goto retry;
         }
         PD_LOG ( PDERROR, "failed to query on catalog(rc=%d)", rc );
         goto error;
      }

      rc = pContext->addSubContext( pReply, takeOver );
      PD_RC_CHECK( rc, PDERROR, "failed to add sub-context(rc=%d)", rc ) ;
      pContext->addSubDone( cb ) ;

   done:
      if ( pBuf )
      {
         SDB_OSS_FREE( pBuf );
      }
      if ( pReply && !takeOver )
      {
         SDB_OSS_FREE( pReply ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOCATAQUERY, rc ) ;
      return rc;
   error:
      rtnCoordClearRequest( cb, sendNodes );
      if ( contextID >= 0 )
      {
         pRtncb->contextDelete( contextID, cb );
         contextID = -1 ;
      }
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCONODEQUERY, "rtnCoordNodeQuery" )
   INT32 rtnCoordNodeQuery ( const CHAR *pCollectionName,
                             const BSONObj &condition,
                             const BSONObj &selector,
                             const BSONObj &orderBy,
                             const BSONObj &hint,
                             INT64 numToSkip, INT64 numToReturn,
                             CoordGroupList &groupLst,
                             pmdEDUCB *cb,
                             rtnContext **ppContext,
                             const CHAR *realCLName,
                             INT32 flag )
   {
      INT32 rc                        = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCONODEQUERY ) ;
      CHAR *pBuffer                   = NULL ;
      MsgOpQuery *pQueryMsg           = NULL ;
      INT32 bufferSize                = 0 ;
      pmdKRCB *pKRCB                  = pmdGetKRCB () ;
      SDB_RTNCB *pRtncb               = pKRCB->getRTNCB() ;
      CoordCB *pCoordcb               = pKRCB->getCoordCB () ;
      netMultiRouteAgent *pRouteAgent = pCoordcb->getRouteAgent () ;
      SINT64 contextID                = -1 ;
      SDB_ASSERT ( ppContext, "ppContext can't be NULL" ) ;
      SDB_ASSERT ( cb, "cb can't be NULL" ) ;
      rtnContextCoord *pContext       = NULL ;
      BOOLEAN isNeedRefreshCata       = FALSE ;
      rtnCoordQuery newQuery ;
      CoordCataInfoPtr cataInfo ;
      CoordGroupList sendGroupLstTmp ;
      BOOLEAN onlyOneNode = groupLst.size() == 1 ? TRUE : FALSE ;
      INT64 tmpSkip = onlyOneNode ? numToSkip : 0 ;
      INT64 tmpReturn = onlyOneNode ? numToReturn : numToReturn + numToSkip ;

      const CHAR *realCLFullName = realCLName ? realCLName : pCollectionName ;

      if ( groupLst.size() == 0 )
      {
         PD_LOG( PDERROR, "Send group list is empty" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg ( &pBuffer, &bufferSize, pCollectionName,
                              flag, 0, tmpSkip, tmpReturn,
                              condition.isEmpty()?NULL:&condition,
                              selector.isEmpty()?NULL:&selector,
                              orderBy.isEmpty()?NULL:&orderBy,
                              hint.isEmpty()?NULL:&hint ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to build query message, rc = %d",
                  rc ) ;
         goto error ;
      }
      pQueryMsg = (MsgOpQuery*)pBuffer ;
   retry :
      rc = pRtncb->contextNew ( RTN_CONTEXT_COORD, (rtnContext**)&pContext,
                                contextID, cb ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to allocate new context, rc = %d",
                  rc ) ;
         goto error ;
      }
      rc = pContext->open( orderBy,
                           selector,
                           numToReturn, onlyOneNode ? 0 : numToSkip ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Open context failed, rc: %d", rc ) ;
         goto error ;
      }

      rc = rtnCoordGetCataInfo ( cb, realCLFullName, isNeedRefreshCata, cataInfo ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to get catalog info for %s, rc = %d",
                  realCLFullName, rc ) ;
         goto error ;
      }
      pQueryMsg->version = cataInfo->getVersion () ;
      pQueryMsg->header.routeID.value = 0 ;
      pQueryMsg->header.TID = cb->getTID () ;

      rc = newQuery.queryToDataNodeGroup ( pBuffer, groupLst, sendGroupLstTmp,
                                           pRouteAgent, cb, pContext, TRUE ) ;
      if ( rc )
      {
         if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == rc &&
              !isNeedRefreshCata )
         {
            isNeedRefreshCata = TRUE ;
            SDB_ASSERT ( contextID >= 0, "contextID must be positive" ) ;
            pRtncb->contextDelete ( contextID, cb ) ;
            contextID = -1 ;
            goto retry ;
         }
         PD_LOG ( PDERROR, "Query from data node failed, rc = %d", rc ) ;
         goto error ;
      }

   done :
      if ( pBuffer )
      {
         SDB_OSS_FREE ( pBuffer ) ;
      }
      if ( pContext )
      {
         *ppContext = pContext ;
      }
      PD_TRACE_EXITRC ( SDB_RTNCONODEQUERY, rc ) ;
      return rc ;
   error :
      if ( contextID >= 0 )
      {
         pRtncb->contextDelete ( contextID, cb ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETREPLY, "rtnCoordGetReply" )
   INT32 rtnCoordGetReply ( pmdEDUCB *cb,  REQUESTID_MAP &requestIdMap,
                            REPLY_QUE &replyQue, const SINT32 opCode,
                            BOOLEAN isWaitAll, BOOLEAN clearReplyIfFailed )
   {
      INT32 rc = SDB_OK;
      ossQueue<pmdEDUEvent> tmpQue;
      PD_TRACE_ENTRY ( SDB_RTNCOGETREPLY ) ;
      REQUESTID_MAP::iterator iterMap;
      INT64 waitTime = RTN_COORD_RSP_WAIT_TIME;
      while ( requestIdMap.size() > 0 )
      {
         pmdEDUEvent pmdEvent;
         BOOLEAN isGotMsg = cb->waitEvent( pmdEvent, waitTime ) ;
         PD_CHECK( !cb->isInterrupted() && !cb->isForced(),
                   SDB_APP_INTERRUPT, error, PDERROR,
                   "Interrupt! stop receiving reply!" ) ;

         if ( FALSE == isGotMsg )
         {
            if ( !isWaitAll && !replyQue.empty() )
            {
               break;
            }
            else
            {
               continue;
            }
         }

         if ( pmdEvent._eventType != PMD_EDU_EVENT_MSG )
         {
            PD_LOG ( PDWARNING, "received unknown event(eventType:%d)",
                     pmdEvent._eventType ) ;
            pmdEduEventRelase( pmdEvent, cb ) ;
            pmdEvent.reset () ;
            continue;
         }
         MsgHeader *pMsg = (MsgHeader *)(pmdEvent._Data);
         if ( NULL == pMsg )
         {
            PD_LOG ( PDWARNING, "received invalid event(data is null)" );
            continue;
         }

         if ( MSG_COOR_REMOTE_DISC == pMsg->opCode )
         {
            MsgRouteID routeID;
            routeID.value = pMsg->routeID.value;
            if ( cb->isTransNode( routeID ))
            {
               cb->setTransRC( SDB_COORD_REMOTE_DISC );
               PD_LOG ( PDERROR,
                        "transaction operation interrupt, "
                        "remote-node disconnected:"
                        "groupID=%u, nodeID=%u, serviceID=%u",
                        routeID.columns.groupID,
                        routeID.columns.nodeID,
                        routeID.columns.serviceID );
            }

            iterMap = requestIdMap.begin();
            while ( iterMap != requestIdMap.end() )
            {
               if ( iterMap->second.value == routeID.value )
               {
                  break;
               }
               ++iterMap;
            }
            if ( iterMap != requestIdMap.end()
               && iterMap->first <= pMsg->requestID )
            {

               PD_LOG ( PDERROR,
                     "get reply failed, remote-node disconnected:"
                     "groupID=%u, nodeID=%u, serviceID=%u",
                     iterMap->second.columns.groupID,
                     iterMap->second.columns.nodeID,
                     iterMap->second.columns.serviceID );
               MsgOpReply *pDiscSrc = ( MsgOpReply *)(pmdEvent._Data);
               MsgOpReply *pDiscMsg = NULL;
               pDiscMsg = (MsgOpReply *)SDB_OSS_MALLOC( pDiscSrc->header.messageLength );
               if ( NULL == pDiscMsg )
               {
                  rc = rc ? rc : SDB_OOM;
                  PD_LOG( PDERROR, "malloc failed(size=%d)", sizeof(MsgOpReply) );
               }
               else
               {
                  ossMemcpy( (CHAR *)pDiscMsg, (CHAR *)pDiscSrc,
                              pDiscSrc->header.messageLength );
                  replyQue.push( (CHAR *)pDiscMsg );
               }
               cb->getCoordSession()->delRequest( iterMap->first );
               requestIdMap.erase( iterMap );
            }
            if ( cb->getCoordSession()->isValidResponse( routeID, pMsg->requestID ))
            {
               tmpQue.push( pmdEvent );
            }
            else
            {
               SDB_OSS_FREE ( pmdEvent._Data );
               pmdEvent.reset () ;
            }
            continue;
         }
         MsgHeader *pReply = ( MsgHeader *)(pmdEvent._Data);

         if ( opCode != pReply->opCode
            || ( iterMap=requestIdMap.find( pReply->requestID ))
                           == requestIdMap.end() )
         {
            if ( cb->getCoordSession()->isValidResponse( pReply->requestID ))
            {
               tmpQue.push( pmdEvent );
            }
            else
            {
               PD_LOG ( PDWARNING, 
                        "received unexpected msg(opCode=%[%d]%d,"
                        "expectOpCode=[%d]%d,"
                        "groupID=%u, nodeID=%u, serviceID=%u)",
                        IS_REPLY_TYPE( pReply->opCode ),
                        GET_REQUEST_TYPE( pReply->opCode ),
                        IS_REPLY_TYPE( opCode ),
                        GET_REQUEST_TYPE( opCode ),
                        pReply->routeID.columns.groupID,
                        pReply->routeID.columns.nodeID,
                        pReply->routeID.columns.serviceID );
               SDB_OSS_FREE( pReply );
            }
         }
         else
         {
            PD_LOG ( PDDEBUG, "received the reply("
                     " opCode=[%d]%d, requestID=%llu, TID=%u, "
                     "groupID=%u, nodeID=%u, serviceID=%u )",
                     IS_REPLY_TYPE( pReply->opCode ),
                     GET_REQUEST_TYPE( pReply->opCode ),
                     pReply->requestID, pReply->TID,
                     pReply->routeID.columns.groupID,
                     pReply->routeID.columns.nodeID,
                     pReply->routeID.columns.serviceID );

            requestIdMap.erase(iterMap);
            cb->getCoordSession()->delRequest( pReply->requestID );
            replyQue.push( (CHAR *)( pmdEvent._Data ));
            pmdEvent.reset () ;
            if ( !isWaitAll )
            {
               waitTime = RTN_COORD_RSP_WAIT_TIME_QUICK;
            }
         }
      }
      if ( rc )
      {
         goto error;
      }
   done:
      while( !tmpQue.empty() )
      {
         pmdEDUEvent otherEvent;
         tmpQue.wait_and_pop( otherEvent );
         cb->postEvent( otherEvent );
      }
      PD_TRACE_EXITRC ( SDB_RTNCOGETREPLY, rc ) ;
      return rc;
   error:
      if ( clearReplyIfFailed )
      {
         while ( !replyQue.empty() )
         {
            CHAR *pData = replyQue.front();
            replyQue.pop();
            SDB_OSS_FREE( pData );
         }
      }
      rtnCoordClearRequest( cb, requestIdMap );
      goto done;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_GETSERVNAME, "getServiceName" )
   INT32 getServiceName ( BSONElement &beService,
                                 INT32 serviceType,
                                 std::string &strServiceName )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_GETSERVNAME ) ;
      strServiceName = "";
      do
      {
         if ( beService.type() != Array )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR,
                     "failed to get the service-name(type=%d),\
                     the service field is invalid", serviceType );
            break;
         }
         try
         {
            BSONObjIterator i( beService.embeddedObject() );
            while ( i.more() )
            {
               BSONElement beTmp = i.next();
               BSONObj boTmp = beTmp.embeddedObject();
               BSONElement beServiceType = boTmp.getField(CAT_SERVICE_TYPE_FIELD_NAME);
               if ( beServiceType.eoo() || !beServiceType.isNumber() )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDERROR,
                        "failed to get the service-name(type=%d),\
                        get the field(%s) failed", serviceType,
                        CAT_SERVICE_TYPE_FIELD_NAME );
                  break;
               }
               if ( beServiceType.numberInt() == serviceType )
               {
                  BSONElement beServiceName = boTmp.getField(CAT_SERVICE_NAME_FIELD_NAME);
                  if ( beServiceType.eoo() || beServiceName.type() != String )
                  {
                     rc = SDB_INVALIDARG;
                     PD_LOG ( PDERROR,
                           "failed to get the service-name(type=%d),\
                           get the field(%s) failed", serviceType,
                           CAT_SERVICE_NAME_FIELD_NAME );
                     break;
                  }
                  strServiceName = beServiceName.String();
                  break;
               }
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR,
                    "unexpected exception: %s", e.what() ) ;
            rc = SDB_INVALIDARG ;
         }
      }while ( FALSE );
      PD_TRACE_EXITRC ( SDB_GETSERVNAME, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETCATAINFO, "rtnCoordGetCataInfo" )
   INT32 rtnCoordGetCataInfo( pmdEDUCB *cb,
                              const CHAR *pCollectionName,
                              BOOLEAN isNeedRefreshCata,
                              CoordCataInfoPtr &cataInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETCATAINFO ) ;
      while( TRUE )
      {
         if ( isNeedRefreshCata )
         {
            rc = rtnCoordGetRemoteCata( cb, pCollectionName, cataInfo );
         }
         else
         {
            rc = rtnCoordGetLocalCata ( pCollectionName, cataInfo );
            if ( SDB_CAT_NO_MATCH_CATALOG == rc )
            {
               isNeedRefreshCata = TRUE;
               continue;
            }
         }
         break;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOGETCATAINFO, rc ) ;
      return rc;
   }

   void rtnCoordRemoveGroup( UINT32 group )
   {
      pmdGetKRCB()->getCoordCB()->removeGroupInfo( group ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETLOCALCATA, "rtnCoordGetLocalCata" )
   INT32 rtnCoordGetLocalCata( const CHAR *pCollectionName,
                                         CoordCataInfoPtr &cataInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETLOCALCATA ) ;
      pmdKRCB *pKrcb          = pmdGetKRCB();
      CoordCB *pCoordcb       = pKrcb->getCoordCB();
      rc = pCoordcb->getCataInfo( pCollectionName, cataInfo );
      PD_TRACE_EXITRC ( SDB_RTNCOGETLOCALCATA, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETREMOTECATA, "rtnCoordGetRemoteCata" )
   INT32 rtnCoordGetRemoteCata( pmdEDUCB *cb,
                              const CHAR *pCollectionName,
                              CoordCataInfoPtr &cataInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETREMOTECATA ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      BOOLEAN isNeedRefresh   = FALSE;
      do
      {
         CoordGroupInfoPtr cataGroupInfo;
         rc = rtnCoordGetCatGroupInfo( cb, isNeedRefresh, cataGroupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to get the catalogue-node-group "
                     "info(rc=%d)", rc);
            break;
         }

         BSONObj boQuery;
         BSONObj boFieldSelector;
         BSONObj boOrderBy;
         BSONObj boHint;
         try
         {
            boQuery = BSON( CAT_CATALOGNAME_NAME<<pCollectionName );
         }
         catch ( std::exception &e )
         {
            rc = SDB_SYS;
            PD_LOG ( PDERROR, "Get reomte catalogue failed, while "
                     "build query-obj received unexception error:%s",
                     e.what() );
            break;
         }
         CHAR *pBuffer = NULL;
         INT32 bufferSize = 0;
         rc = msgBuildQueryCatalogReqMsg ( &pBuffer, &bufferSize,
                                           0, 0, 0, 1, cb->getTID(),
                                           &boQuery, &boFieldSelector,
                                           &boOrderBy, &boHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to build query-catalog-message(rc=%d)",
                     rc );
            break;
         }

         REQUESTID_MAP sendNodes;
         rc = rtnCoordSendRequestToPrimary( pBuffer, cataGroupInfo, sendNodes,
                                    pRouteAgent, MSG_ROUTE_CAT_SERVICE, cb );
         if ( pBuffer != NULL )
         {
            SDB_OSS_FREE( pBuffer );
         }
         if ( rc != SDB_OK )
         {
            rtnCoordClearRequest( cb, sendNodes );
            if ( FALSE == isNeedRefresh )
            {
               isNeedRefresh = TRUE;
               continue;
            }

            PD_LOG ( PDERROR,
                     "Failed to send the request to catalogue-group(rc=%d)",
                     rc );
            break;
         }
         REPLY_QUE replyQue;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                        MSG_CAT_QUERY_CATALOG_RSP );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,"Failed to get reply from catalogue-node(rc=%d)",
                     rc );
            break;
         }

         BOOLEAN isGetExpectReply = FALSE;
         while ( !replyQue.empty() )
         {
            MsgCatQueryCatRsp *pReply = NULL;
            pReply = (MsgCatQueryCatRsp *)(replyQue.front());
            replyQue.pop();
            if ( FALSE == isGetExpectReply )
            {
               rc = rtnCoordProcessQueryCatReply( pReply, cataInfo );
               if ( SDB_CLS_NOT_PRIMARY == rc )
               {
                  cataGroupInfo->setSlave( pReply->header.routeID );
               }
               else
               {
                  isGetExpectReply = TRUE;
                  if ( SDB_OK == rc )
                  {
                     pCoordcb->updateCataInfo( pCollectionName, cataInfo );
                  }
               }
            }
            if ( NULL != pReply )
            {
               SDB_OSS_FREE( pReply );
            }
         }

         if ( SDB_CLS_NOT_PRIMARY == rc && FALSE == isNeedRefresh )
         {
            isNeedRefresh = TRUE;
            continue;
         }
         break;
      }while ( TRUE );
      if ( SDB_OK == rc )
      {
         if ( cataInfo->isMainCL() )
         {
            std::vector<std::string> subCLLst;
            rc = cataInfo->getSubCLList( subCLLst );
            if ( SDB_OK == rc )
            {
               std::vector<std::string>::iterator iterLst
                                 = subCLLst.begin();
               while ( SDB_OK == rc && iterLst != subCLLst.end() )
               {
                  CoordCataInfoPtr subCataInfoTmp;
                  rc = rtnCoordGetRemoteCata( cb, iterLst->c_str(), subCataInfoTmp );
                  ++iterLst;
               }
            }
         }
      }
      PD_TRACE_EXITRC ( SDB_RTNCOGETREMOTECATA, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETGROUPINFO, "rtnCoordGetGroupInfo" )
   INT32 rtnCoordGetGroupInfo ( pmdEDUCB *cb,
                                UINT32 groupID,
                                BOOLEAN isNeedRefresh,
                                CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETGROUPINFO ) ;

      while( TRUE )
      {
         if ( isNeedRefresh )
         {
            rc = rtnCoordGetRemoteGroupInfo( cb, groupID, NULL, groupInfo ) ;
         }
         else
         {
            rc = rtnCoordGetLocalGroupInfo ( groupID, groupInfo );
            if ( SDB_COOR_NO_NODEGROUP_INFO == rc )
            {
               isNeedRefresh = TRUE;
               continue;
            }
         }
         break;
      }

      PD_TRACE_EXITRC ( SDB_RTNCOGETGROUPINFO, rc ) ;

      return rc;
   }

   INT32 rtnCoordGetGroupInfo( pmdEDUCB * cb,
                               const CHAR * groupName,
                               BOOLEAN isNeedRefresh,
                               CoordGroupInfoPtr & groupInfo )
   {
      INT32 rc = SDB_OK;

      while( TRUE )
      {
         if ( isNeedRefresh )
         {
            rc = rtnCoordGetRemoteGroupInfo( cb, 0, groupName, groupInfo ) ;
         }
         else
         {
            rc = rtnCoordGetLocalGroupInfo ( groupName, groupInfo ) ;
            if ( SDB_COOR_NO_NODEGROUP_INFO == rc )
            {
               isNeedRefresh = TRUE ;
               continue ;
            }
         }
         break ;
      }

      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETLOCALGROUPINFO, "rtnCoordGetLocalGroupInfo" )
   INT32 rtnCoordGetLocalGroupInfo ( UINT32 groupID,
                                     CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETLOCALGROUPINFO ) ;
      pmdKRCB *pKrcb          = pmdGetKRCB();
      CoordCB *pCoordcb       = pKrcb->getCoordCB();

      if ( CATALOG_GROUPID == groupID )
      {
         groupInfo = pCoordcb->getCatGroupInfo() ;
      }
      else
      {
         rc = pCoordcb->getGroupInfo( groupID, groupInfo ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOGETLOCALGROUPINFO, rc ) ;
      return rc ;
   }

   INT32 rtnCoordGetLocalGroupInfo ( const CHAR *groupName,
                                     CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *pKrcb          = pmdGetKRCB() ;
      CoordCB *pCoordcb       = pKrcb->getCoordCB() ;
      if ( 0 == ossStrcmp( CATALOG_GROUPNAME, groupName ) )
      {
         groupInfo = pCoordcb->getCatGroupInfo() ;
      }
      else
      {
         rc = pCoordcb->getGroupInfo( groupName, groupInfo ) ;
      }
      return rc ;
   }


   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETREMOTEGROUPINFO, "rtnCoordGetRemoteGroupInfo" )
   INT32 rtnCoordGetRemoteGroupInfo ( pmdEDUCB *cb,
                                      UINT32 groupID,
                                      const CHAR *groupName,
                                      CoordGroupInfoPtr &groupInfo,
                                      BOOLEAN addToLocal )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETREMOTEGROUPINFO ) ;
      pmdKRCB *pKrcb          = pmdGetKRCB();
      CoordCB *pCoordcb       = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      BOOLEAN isNeedRefresh = FALSE;
      CHAR *buf = NULL ;
      MsgCatGroupReq *msg = NULL ;

      do
      {
         if ( CATALOG_GROUPID == groupID ||
              ( groupName && 0 == ossStrcmp( groupName, CATALOG_GROUPNAME ) ) )
         {
            rc = rtnCoordGetRemoteCatGroupInfo( cb, groupInfo ) ;
            break ;
         }

         CoordGroupInfoPtr cataGroupInfo;
         rc = rtnCoordGetCatGroupInfo( cb, isNeedRefresh, cataGroupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to get cata-group-info,"
                     "no catalogue-node-group info" );
            break;
         }

         if ( NULL == groupName )
         {
            MsgCatGroupReq msgGroupReq;
            msgGroupReq.id.columns.groupID = groupID;
            msgGroupReq.id.columns.nodeID = 0;
            msgGroupReq.id.columns.serviceID = 0;
            msgGroupReq.header.messageLength = sizeof( MsgCatGroupReq );
            msgGroupReq.header.opCode = MSG_CAT_GRP_REQ;
            msgGroupReq.header.routeID.value = 0;
            msgGroupReq.header.TID = cb->getTID();
            msg = &msgGroupReq ;
         }
         else
         {
            UINT32 nameLen = ossStrlen( groupName ) + 1 ;
            UINT32 msgLen = nameLen +  sizeof(MsgCatGroupReq) ;
            buf = ( CHAR * )SDB_OSS_MALLOC( msgLen ) ;
            if ( NULL == buf )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }
            msg = ( MsgCatGroupReq * )buf ;
            msg->id.value = 0 ;
            msg->header.messageLength = msgLen ;
            msg->header.opCode = MSG_CAT_GRP_REQ;
            msg->header.routeID.value = 0;
            msg->header.TID = cb->getTID();
            ossMemcpy( buf + sizeof(MsgCatGroupReq),
                       groupName, nameLen ) ;
         }

         REQUESTID_MAP sendNodes;
         rc = rtnCoordSendRequestToPrimary( (CHAR *)(msg),
                                            cataGroupInfo, sendNodes,
                                            pRouteAgent,
                                            MSG_ROUTE_CAT_SERVICE,
                                            cb );
         if ( rc != SDB_OK )
         {
            rtnCoordClearRequest( cb, sendNodes );
            if ( SDB_RTN_NO_PRIMARY_FOUND == rc && !isNeedRefresh )
            {
               isNeedRefresh = TRUE;
               continue;
            }
            else
            {
               PD_LOG ( PDERROR, "Failed to get group-info from catalogue-node,"
                        "send group-info-request failed(rc=%d)", rc );
               break;
            }
         }
         REPLY_QUE replyQue;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue, MSG_CAT_GRP_RES );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDWARNING, "Failed to get group-info from catalogue-node,"
                     "get reply failed(rc=%d)", rc );
            break ;
         }
         while ( !replyQue.empty() )
         {
            MsgCatGroupRes *pReply = NULL;
            pReply = (MsgCatGroupRes *)(replyQue.front());
            replyQue.pop() ;
            if ( SDB_OK == rc )
            {
               rc = rtnCoordProcessGetGroupReply( pReply, groupInfo );
               if ( SDB_OK == rc )
               {
                  if ( addToLocal )
                  {
                     pCoordcb->addGroupInfo( groupInfo );
                     rc = rtnCoordUpdateRoute( groupInfo, pRouteAgent,
                                               MSG_ROUTE_SHARD_SERVCIE ) ;
                  }
               }
            }
            if ( NULL != pReply )
            {
               SDB_OSS_FREE( pReply );
            }
         }
         if ( rc != SDB_OK )
         {
            if ( SDB_CLS_NOT_PRIMARY == rc && !isNeedRefresh )
            {
               isNeedRefresh = TRUE;
               continue;
            }
            else
            {
               PD_LOG ( PDERROR, "Failed to get group-info from catalogue-node,"
                        "reply error(flag=%d)", rc ) ;
            }
         }
         break;
      }while ( TRUE );

   done:
      if ( NULL != buf )
      {
         SDB_OSS_FREE( buf ) ;
         buf = NULL ;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOGETREMOTEGROUPINFO, rc ) ;
      return rc;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETCATGROUPINFO, "rtnCoordGetCatGroupInfo" )
   INT32 rtnCoordGetCatGroupInfo ( pmdEDUCB *cb,
                                   BOOLEAN isNeedRefresh,
                                   CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETCATGROUPINFO ) ;
      UINT32 retryCount = 0 ;

      while( TRUE )
      {
         if ( isNeedRefresh )
         {
            rc = rtnCoordGetRemoteCatGroupInfo( cb, groupInfo ) ;
            if ( SDB_CLS_GRP_NOT_EXIST == rc && retryCount < 30 )
            {
               ++retryCount ;
               ossSleep( 100 ) ;
               continue ;
            }
         }
         else
         {
            rc = rtnCoordGetLocalCatGroupInfo ( groupInfo ) ;
            if ( ( SDB_OK == rc && groupInfo->getGroupSize() == 0 ) ||
                 SDB_COOR_NO_NODEGROUP_INFO == rc )
            {
               isNeedRefresh = TRUE;
               continue ;
            }
         }
         break;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOGETCATGROUPINFO, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETLOCALCATGROUPINFO, "rtnCoordGetLocalCatGroupInfo" )
   INT32 rtnCoordGetLocalCatGroupInfo ( CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETLOCALCATGROUPINFO ) ;
      pmdKRCB *pKrcb          = pmdGetKRCB();
      CoordCB *pCoordcb       = pKrcb->getCoordCB();
      groupInfo = pCoordcb->getCatGroupInfo();
      PD_TRACE_EXITRC ( SDB_RTNCOGETLOCALCATGROUPINFO, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETREMOTECATAGROUPINFOBYADDR, "rtnCoordGetRemoteCataGroupInfoByAddr" )
   INT32 rtnCoordGetRemoteCataGroupInfoByAddr ( pmdEDUCB *cb,
                                                CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETREMOTECATAGROUPINFOBYADDR ) ;
      UINT64 reqID;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      MsgCatCatGroupRes *pReply = NULL;
      ossQueue<pmdEDUEvent> tmpQue;

      do
      {
         CoordVecNodeInfo cataNodeAddrList;
         pCoordcb->getCatNodeAddrList( cataNodeAddrList );
         if ( cataNodeAddrList.size() == 0 )
         {
            rc = SDB_CAT_NO_ADDR_LIST ;
            PD_LOG ( PDERROR, "no catalog node info" );
            break;
         }
         MsgCatCatGroupReq msgGroupReq;
         msgGroupReq.id.columns.groupID = CAT_CATALOG_GROUPID;
         msgGroupReq.id.columns.nodeID = 0;
         msgGroupReq.id.columns.serviceID = 0;
         msgGroupReq.header.messageLength = sizeof( MsgCatGroupReq );
         msgGroupReq.header.opCode = MSG_CAT_GRP_REQ;
         msgGroupReq.header.routeID.value = 0;
         msgGroupReq.header.TID = cb->getTID();
         UINT32 i = 0;
         for ( ; i < cataNodeAddrList.size(); i++ )
         {
            rc = pRouteAgent->syncSendWithoutCheck( cataNodeAddrList[i]._id,
                                        (MsgHeader*)&msgGroupReq, reqID, cb );
            if ( SDB_OK == rc )
            {
               break;
            }
         }
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to send the request to catalog-node"
                     "(rc=%d)", rc );
            break;
         }
         while ( TRUE )
         {
            pmdEDUEvent pmdEvent;
            BOOLEAN isGotMsg = cb->waitEvent( pmdEvent, RTN_COORD_RSP_WAIT_TIME );
            if ( cb->isForced()
               || ( cb->isInterrupted() && !( cb->isDisconnected() )))
            {
               rc = SDB_APP_INTERRUPT;
               break;
            }
            if ( FALSE == isGotMsg )
            {
               continue;
            }
            if ( PMD_EDU_EVENT_MSG != pmdEvent._eventType )
            {
               PD_LOG ( PDWARNING, "received unknown event(eventType=%d)",
                        pmdEvent._eventType );
               continue;
            }
            MsgHeader *pMsg = (MsgHeader *)(pmdEvent._Data);
            if ( NULL == pMsg )
            {
               PD_LOG ( PDWARNING, "received invalid msg-event(data is null)" );
               continue;
            }
            if ( (UINT32)pMsg->opCode != MSG_CAT_GRP_RES
               || pMsg->requestID != reqID )
            {
               if ( cb->getCoordSession()->isValidResponse( reqID ) )
               {
                  tmpQue.push( pmdEvent );
               }
               else
               {
                  PD_LOG( PDWARNING, "received unexpected message"
                        "(opCode=[%d]%d, requestID=%llu, TID=%u, "
                        "groupID=%u, nodeID=%u, serviceID=%u )",
                        pMsg->opCode>>31&0x01, pMsg->opCode&0x7fffffff,
                        pMsg->requestID, pMsg->TID,
                        pMsg->routeID.columns.groupID,
                        pMsg->routeID.columns.nodeID,
                        pMsg->routeID.columns.serviceID );
                  SDB_OSS_FREE( pMsg );
               }
               continue;
            }
            cb->getCoordSession()->delRequest( reqID );
            pReply = (MsgCatCatGroupRes *)pMsg;
            break;
         }

         if ( rc != SDB_OK || NULL == pReply )
         {
            PD_LOG ( PDERROR, "failed to get reply(rc=%d)", rc );
            break;
         }
         rc = rtnCoordProcessGetGroupReply( pReply, groupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "Failed to get catalog-group info, reply error(rc=%d)",
                     rc );
            break;
         }
         rc = rtnCoordUpdateRoute( groupInfo, pRouteAgent, MSG_ROUTE_CAT_SERVICE );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to get catalog-group info,"
                     "update route failed(rc=%d)", rc );
            break;
         }
         pCoordcb->updateCatGroupInfo( groupInfo );
      }while( FALSE );
      if ( NULL != pReply )
      {
         SDB_OSS_FREE( pReply );
      }
      while ( !tmpQue.empty() )
      {
         pmdEDUEvent otherEvent;
         tmpQue.wait_and_pop( otherEvent );
         cb->postEvent( otherEvent );
      }
      PD_TRACE_EXITRC ( SDB_RTNCOGETREMOTECATAGROUPINFOBYADDR, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETREMOTECATGROUPINFO, "rtnCoordGetRemoteCatGroupInfo" )
   INT32 rtnCoordGetRemoteCatGroupInfo ( pmdEDUCB *cb,
                                         CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETREMOTECATGROUPINFO ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      do
      {
         CoordGroupInfoPtr cataGroupInfo;
         rc = rtnCoordGetLocalCatGroupInfo( cataGroupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to get cata-group-info from local(rc=%d)",
                     rc );
            break;
         }
         if ( cataGroupInfo->getGroupSize() == 0 )
         {
            rc = rtnCoordGetRemoteCataGroupInfoByAddr( cb, groupInfo );
            if ( rc != SDB_OK )
            {
               PD_LOG ( PDERROR, "Failed to get cata-group-info(rc=%d)", rc );
            }
            break;
         }
         MsgCatCatGroupReq msgGroupReq;
         msgGroupReq.id.columns.groupID = CAT_CATALOG_GROUPID;
         msgGroupReq.id.columns.nodeID = 0;
         msgGroupReq.id.columns.serviceID = 0;
         msgGroupReq.header.messageLength = sizeof( MsgCatGroupReq );
         msgGroupReq.header.opCode = MSG_CAT_GRP_REQ;
         msgGroupReq.header.routeID.value = 0;
         msgGroupReq.header.TID = cb->getTID();
         REQUESTID_MAP sendNodes;
         rc = rtnCoordSendRequestToOne( (CHAR *)(&msgGroupReq),
                                        cataGroupInfo, sendNodes,
                                        pRouteAgent, MSG_ROUTE_CAT_SERVICE,
                                        cb ) ;
         if ( rc != SDB_OK )
         {
            rtnCoordClearRequest( cb, sendNodes );
            PD_LOG ( PDERROR, "Failed to get cata-group-info from "
                     "catalogue-node,send group-info-request failed(rc=%d)",
                     rc );
            break;
         }
         REPLY_QUE replyQue;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                                MSG_CAT_GRP_RES ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDWARNING,
                     "Failed to get cata-group-info from catalogue-node,"
                     "get reply failed(rc=%d)", rc );
         }
         while ( !replyQue.empty() )
         {
            MsgCatCatGroupRes *pReply = NULL;
            pReply = (MsgCatGroupRes *)(replyQue.front());
            replyQue.pop();
            if ( SDB_OK == rc )
            {
               rc = rtnCoordProcessGetGroupReply( pReply, groupInfo );
               if ( SDB_OK == rc )
               {
                  pCoordcb->updateCatGroupInfo( groupInfo );
                  rc = rtnCoordUpdateRoute( groupInfo,  pRouteAgent,
                                    MSG_ROUTE_CAT_SERVICE );
                  if ( rc != SDB_OK )
                  {
                     PD_LOG ( PDERROR, "Failed to get cata-group-info"
                              "update route failed(rc=%d)", rc ) ;
                  }
               }
            }
            if ( NULL != pReply )
            {
               SDB_OSS_FREE( pReply );
            }
         }
      }while ( FALSE );
      PD_TRACE_EXITRC ( SDB_RTNCOGETREMOTECATGROUPINFO, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOUPDATEROUTE, "rtnCoordUpdateRoute" )
   INT32 rtnCoordUpdateRoute ( CoordGroupInfoPtr &groupInfo,
                               netMultiRouteAgent *pRouteAgent,
                               MSG_ROUTE_SERVICE_TYPE type )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOUPDATEROUTE ) ;

      string host ;
      string service ;
      MsgRouteID routeID ;
      routeID.value = MSG_INVALID_ROUTEID ;

      UINT32 index = 0 ;
      clsGroupItem *groupItem = groupInfo->getGroupItem() ;
      while ( SDB_OK == groupItem->getNodeInfo( index++, routeID, host,
                                                service, type ) )
      {
         rc = pRouteAgent->updateRoute( routeID, host.c_str(),
                                        service.c_str() ) ;
         if ( rc != SDB_OK )
         {
            if ( SDB_NET_UPDATE_EXISTING_NODE == rc )
            {
               rc = SDB_OK;
            }
            else
            {
               PD_LOG ( PDERROR, "update route failed (rc=%d)", rc ) ;
               break;
            }
         }
      }
      PD_TRACE_EXITRC ( SDB_RTNCOUPDATEROUTE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETGROUPSBYCATAINFO, "rtnCoordGetGroupsByCataInfo" )
   INT32 rtnCoordGetGroupsByCataInfo( const CoordCataInfoPtr &cataInfo,
                                      CoordGroupList &sendGroupLst,
                                      CoordGroupList &groupLst )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETGROUPSBYCATAINFO ) ;
      if ( !cataInfo->isMainCL() )
      {
         cataInfo->getGroupLst( groupLst );
         if ( groupLst.size() <= 0 )
         {
            rc = SDB_CAT_NO_MATCH_CATALOG;
         }
         else
         {
            CoordGroupList::iterator iter = sendGroupLst.begin();
            while( iter != sendGroupLst.end() )
            {
               groupLst.erase( iter->first );
               ++iter;
            }
         }
      }
      else
      {
         std::vector< std::string > subCLLst;
         rc = cataInfo->getSubCLList( subCLLst );
         PD_RC_CHECK( rc, PDERROR, "Failed to get sub-collection list(rc=%d)",
                      rc ) ;
         std::vector< std::string >::iterator iterCL
                           = subCLLst.begin();
         while( iterCL != subCLLst.end() )
         {
            CoordCataInfoPtr subCataInfo;
            CoordGroupList groupLstTmp;
            CoordGroupList::iterator iterGrp;
            rc = rtnCoordGetLocalCata( (*iterCL).c_str(), subCataInfo );
            PD_RC_CHECK( rc, PDWARNING,
                         "Failed to get sub-collection catalog-info(rc=%d)",
                         rc ) ;
            rc = rtnCoordGetGroupsByCataInfo( subCataInfo,
                                              sendGroupLst,
                                              groupLstTmp ) ;
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to get sub-collection group-info(rc=%d)",
                         rc ) ;
            iterGrp = groupLstTmp.begin();
            while ( iterGrp != groupLstTmp.end() )
            {
               groupLst[ iterGrp->first ] = iterGrp->second;
               ++iterGrp;
            }
            ++iterCL;
         }
      }
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOGETGROUPSBYCATAINFO, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODESWITHOUTREPLY, "rtnCoordSendRequestToNodesWithOutReply" )
   void rtnCoordSendRequestToNodesWithOutReply( void *pBuffer,
                                                ROUTE_SET &nodes,
                                                netMultiRouteAgent *pRouteAgent )
   {
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTONODESWITHOUTREPLY ) ;
      pRouteAgent->multiSyncSend( nodes, pBuffer );
      PD_TRACE_EXIT ( SDB_RTNCOSENDREQUESTTONODESWITHOUTREPLY ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODEWITHOUTCHECK, "rtnCoordSendRequestToNodeWithoutCheck" )
   INT32 rtnCoordSendRequestToNodeWithoutCheck( void *pBuffer,
                                                const MsgRouteID &routeID,
                                                netMultiRouteAgent *pRouteAgent,
                                                pmdEDUCB *cb,
                                                REQUESTID_MAP &sendNodes )
   {
      INT32 rc = SDB_OK;
      UINT64 reqID = 0;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTONODEWITHOUTCHECK ) ;
      rc = pRouteAgent->syncSendWithoutCheck( routeID, pBuffer, reqID, cb );
      PD_RC_CHECK ( rc, PDERROR,
                  "failed to send the request to node"
                  "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                  routeID.columns.groupID,
                  routeID.columns.nodeID,
                  routeID.columns.serviceID,
                  rc );
      sendNodes[ reqID ] = routeID;
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTONODEWITHOUTCHECK, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODEWITHOUTREPLY, "rtnCoordSendRequestToNodeWithoutReply" )
   INT32 rtnCoordSendRequestToNodeWithoutReply( void *pBuffer,
                                                MsgRouteID &routeID,
                                                netMultiRouteAgent *pRouteAgent )
   {
      INT32 rc = SDB_OK;
      UINT64 reqID = 0;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTONODEWITHOUTREPLY ) ;
      rc = pRouteAgent->syncSend( routeID, pBuffer, reqID, NULL );
      PD_RC_CHECK ( rc, PDERROR,
                  "failed to send the request to node"
                  "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                  routeID.columns.groupID,
                  routeID.columns.nodeID,
                  routeID.columns.serviceID,
                  rc );
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTONODEWITHOUTREPLY, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODE, "rtnCoordSendRequestToNode" )
   INT32 rtnCoordSendRequestToNode( void *pBuffer,
                                    MsgRouteID routeID,
                                    netMultiRouteAgent *pRouteAgent,
                                    pmdEDUCB *cb,
                                    REQUESTID_MAP &sendNodes )
   {
      INT32 rc = SDB_OK;
      UINT64 reqID = 0;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTONODE ) ;
      rc = pRouteAgent->syncSend( routeID, pBuffer, reqID, cb );
      PD_RC_CHECK ( rc, PDERROR,
                  "failed to send the request to node"
                  "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                  routeID.columns.groupID,
                  routeID.columns.nodeID,
                  routeID.columns.serviceID,
                  rc );
      sendNodes[ reqID ] = routeID;
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTONODE, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODE2, "rtnCoordSendRequestToNode" )
   INT32 rtnCoordSendRequestToNode( void *pBuffer,
                                    MsgRouteID routeID,
                                    netMultiRouteAgent *pRouteAgent,
                                    pmdEDUCB *cb,
                                    const netIOVec &iov,
                                    REQUESTID_MAP &sendNodes )
   {
      INT32 rc = SDB_OK;
      UINT64 reqID = 0;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTONODE2 ) ;
      rc = pRouteAgent->syncSend( routeID, ( MsgHeader *)pBuffer, iov, reqID, cb );
      PD_RC_CHECK ( rc, PDERROR,
                  "failed to send the request to node"
                  "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                  routeID.columns.groupID,
                  routeID.columns.nodeID,
                  routeID.columns.serviceID,
                  rc );
      sendNodes[ reqID ] = routeID;
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTONODE2, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODEGROUPS, "rtnCoordSendRequestToNodeGroups" )
   INT32 rtnCoordSendRequestToNodeGroups( CHAR *pBuffer,
                                          CoordGroupList &groupLst,
                                          BOOLEAN isSendPrimary,
                                          netMultiRouteAgent *pRouteAgent,
                                          pmdEDUCB *cb,
                                          REQUESTID_MAP &sendNodes,
                                          MSG_ROUTE_SERVICE_TYPE type )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTONODEGROUPS ) ;
      CoordGroupList::iterator iter = groupLst.begin();
      while ( iter != groupLst.end() )
      {
         rc = rtnCoordSendRequestToNodeGroup( pBuffer, iter->first,
                                              isSendPrimary, pRouteAgent,
                                              cb, sendNodes, type ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR, "Failed to send the request to the group(%u), "
                     "rc=%d", iter->first, rc ) ;
            break ;
         }
         ++iter;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTONODEGROUPS, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODEGROUP, "rtnCoordSendRequestToNodeGroup" )
   INT32 rtnCoordSendRequestToNodeGroup( CHAR *pBuffer,
                                         UINT32 groupID,
                                         BOOLEAN isSendPrimary,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb,
                                         REQUESTID_MAP &sendNodes,
                                         MSG_ROUTE_SERVICE_TYPE type )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOSENDREQUESTTONODEGROUP ) ;
      BOOLEAN isNeedRefresh = FALSE;
      do
      {
         MsgRouteID routeID;
         cb->getTransNodeRouteID( groupID, routeID );
         if ( routeID.value != 0 )
         {
            UINT64 reqID = 0;
            rc = pRouteAgent->syncSend( routeID, ( void * )pBuffer, reqID, cb );
            if ( SDB_OK == rc )
            {
               sendNodes[ reqID ] = routeID ;
            }
            break;
         }

         CoordGroupInfoPtr groupInfo;
         rc = rtnCoordGetGroupInfo( cb, groupID, isNeedRefresh, groupInfo );
         if ( rc != SDB_OK )
         {
            break;
         }
         if ( isSendPrimary )
         {
            rc = rtnCoordSendRequestToPrimary( pBuffer, groupInfo, sendNodes,
                                               pRouteAgent, type, cb );
         }
         else
         {
            rc = rtnCoordSendRequestToOne( pBuffer, groupInfo, sendNodes,
                                           pRouteAgent, type, cb );
         }
         if ( SDB_OK == rc || isNeedRefresh )
         {
            break;
         }
         isNeedRefresh = TRUE ;
         }while( TRUE );

         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR,
                     "Failed to send the request to the group(%u), rc=%d",
                     groupID, rc );
         }
      PD_TRACE_EXITRC( SDB_RTNCOSENDREQUESTTONODEGROUP, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODEGROUPS2, "rtnCoordSendRequestToNodeGroups" )
   INT32 rtnCoordSendRequestToNodeGroups( MsgHeader *pBuffer,
                                  CoordGroupList &groupLst,
                                  BOOLEAN isSendPrimary,
                                  netMultiRouteAgent *pRouteAgent,
                                  pmdEDUCB *cb,
                                  const netIOVec &iov,
                                  REQUESTID_MAP &sendNodes,
                                  MSG_ROUTE_SERVICE_TYPE type )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOSENDREQUESTTONODEGROUPS2 ) ;
      CoordGroupList::iterator iter = groupLst.begin() ;
      while ( iter != groupLst.end() )
      {
         rc = rtnCoordSendRequestToNodeGroup( pBuffer, iter->first,
                                              isSendPrimary, pRouteAgent,
                                              cb, iov, sendNodes,
                                              type ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to send msg to group:%d, rc:%d",
                    iter->first, rc ) ;
            goto error ;
         }
         ++iter ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNCOSENDREQUESTTONODEGROUPS2, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTONODEGROUP2, "rtnCoordSendRequestToNodeGroup" )
   INT32 rtnCoordSendRequestToNodeGroup( MsgHeader *pBuffer,
                                         UINT32 groupID,
                                         BOOLEAN isSendPrimary,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb,
                                         const netIOVec &iov,
                                          REQUESTID_MAP &sendNodes,
                                          MSG_ROUTE_SERVICE_TYPE type )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTONODEGROUP2 ) ;
      BOOLEAN isNeedRefresh = FALSE;
      do
      {
         MsgRouteID routeID;
         cb->getTransNodeRouteID( groupID, routeID );
         if ( routeID.value != 0 )
         {
            UINT64 reqID = 0;
            rc = pRouteAgent->syncSend( routeID, pBuffer, iov, reqID, cb );
            if ( SDB_OK == rc )
            {
               sendNodes[ reqID ] = routeID ;
            }
            break;
         }
         CoordGroupInfoPtr groupInfo;
         rc = rtnCoordGetGroupInfo( cb, groupID, isNeedRefresh,
                                    groupInfo );
         if ( rc != SDB_OK )
         {
            break;
         }
         if ( isSendPrimary )
         {
            rc = rtnCoordSendRequestToPrimary( pBuffer, groupInfo, sendNodes,
                                               pRouteAgent, iov, type, cb );
         }
         else
         {
            rc = rtnCoordSendRequestToOne( pBuffer, groupInfo, sendNodes,
                                           pRouteAgent, iov, type, cb );
         }
         if ( SDB_OK == rc || isNeedRefresh )
         {
            break;
         }
         isNeedRefresh = TRUE ;
      }while( TRUE );
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR,
                  "Failed to send the request to the group(%u), rc=%d",
                  groupID, rc );
      }
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTONODEGROUP2, rc ) ;
      return rc;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTOPRIMARY, "rtnCoordSendRequestToPrimary" )
   INT32 rtnCoordSendRequestToPrimary( CHAR *pBuffer,
                                       const CoordGroupInfoPtr &groupInfo,
                                       REQUESTID_MAP &sendNodes,
                                       netMultiRouteAgent *pRouteAgent,
                                       MSG_ROUTE_SERVICE_TYPE type,
                                       pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTOPRIMARY ) ;
      MsgRouteID primaryRouteID = groupInfo->getPrimary( type );
      if ( primaryRouteID.value != 0 )
      {
         UINT64 reqID = 0;
         rc = pRouteAgent->syncSend( primaryRouteID, pBuffer, reqID, cb );
         if ( rc != SDB_OK )
         {
            groupInfo->setSlave( primaryRouteID );
            PD_LOG ( PDWARNING, "Failed to send the request to primary(rc=%d)",
                     rc );
         }
         else
         {
            sendNodes[reqID] = primaryRouteID ;
         }
      }
      else
      {
         rc = SDB_RTN_NO_PRIMARY_FOUND;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTOPRIMARY, rc ) ;
      return rc;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTOPRIMARY2, "rtnCoordSendRequestToPrimary" )
   INT32 rtnCoordSendRequestToPrimary( MsgHeader *pBuffer,
                                       const CoordGroupInfoPtr &groupInfo,
                                       REQUESTID_MAP &sendNodes,
                                       netMultiRouteAgent *pRouteAgent,
                                       const netIOVec &iov,
                                       MSG_ROUTE_SERVICE_TYPE type,
                                       pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTOPRIMARY2 ) ;
      MsgRouteID primaryRouteID = groupInfo->getPrimary( type );
      if ( primaryRouteID.value != 0 )
      {
         UINT64 reqID = 0;
         rc = pRouteAgent->syncSend( primaryRouteID, pBuffer, iov, reqID, cb );
         if ( rc != SDB_OK )
         {
            groupInfo->setSlave( primaryRouteID );
            PD_LOG ( PDWARNING, "Failed to send the request to primary(rc=%d)",
                     rc );
         }
         else
         {
            sendNodes[reqID] = primaryRouteID ;
         }
      }
      else
      {
         rc = SDB_RTN_NO_PRIMARY_FOUND;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTOPRIMARY2, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOGETNODEPOS, "rtnCoordGetNodePos" )
   INT32 rtnCoordGetNodePos( pmdEDUCB *cb,
                           const CoordGroupInfoPtr &groupInfo,
                           UINT32 random,
                           UINT32 &pos )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOGETNODEPOS ) ;
      INT32 preferReplicaType = PREFER_REPL_ANYONE;
      CoordSession *pSession = cb->getCoordSession();
      UINT32 posTmp = 0;
      clsGroupItem *groupItem = groupInfo->getGroupItem();
      if ( NULL != pSession )
      {
         preferReplicaType = pSession->getPreferReplType();
      }
      switch( preferReplicaType )
      {
         case PREFER_REPL_NODE_1:
         case PREFER_REPL_NODE_2:
         case PREFER_REPL_NODE_3:
         case PREFER_REPL_NODE_4:
         case PREFER_REPL_NODE_5:
         case PREFER_REPL_NODE_6:
         case PREFER_REPL_NODE_7:
            {
               posTmp = preferReplicaType - 1;
               break;
            }
         case PREFER_REPL_MASTER:
            {
               posTmp = groupItem->getPrimaryPos();
               if ( CLS_RG_NODE_POS_INVALID != posTmp )
               {
                  break;
               }
            }
         case PREFER_REPL_ANYONE:
         case PREFER_REPL_SLAVE:
         default:
            {
               posTmp = random;
               break;
            }
      }
      UINT32 nodeNum = groupInfo->getGroupSize();
      PD_CHECK( nodeNum > 0, SDB_SYS, error, PDERROR,
               "the group is empty!" );
      pos = posTmp % nodeNum;
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOGETNODEPOS, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTOONE, "rtnCoordSendRequestToOne" )
   INT32 rtnCoordSendRequestToOne( CHAR *pBuffer,
                                   CoordGroupInfoPtr &groupInfo,
                                   REQUESTID_MAP &sendNodes,
                                   netMultiRouteAgent *pRouteAgent,
                                   MSG_ROUTE_SERVICE_TYPE type,
                                   pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      BOOLEAN hasRetry = FALSE;
      BOOLEAN isNeedRetry = FALSE;
      UINT64 reqID = 0;
      INT32 preferReplicaType = PREFER_REPL_ANYONE;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTOONE ) ;
      do
      {
         hasRetry = isNeedRetry;
         isNeedRetry = FALSE;
         /*******************************
         ********************************/
         MsgHeader *pHeader = (MsgHeader *)pBuffer ;
         CoordSession *pSession = NULL;
         pSession = cb->getCoordSession();
         if ( NULL != pSession )
         {
            preferReplicaType = pSession->getPreferReplType();
            MsgRouteID routeID = pSession->getLastNode(groupInfo->getGroupID());
            if ( routeID.value != 0 )
            {
               rc = pRouteAgent->syncSend( routeID, pBuffer, reqID, cb ) ;
               if ( SDB_OK == rc )
               {
                  sendNodes[reqID] = routeID ;
                  break;
               }
               else
               {
                  pSession->removeLastNode(groupInfo->getGroupID());
               }
            }
         }

         /*******************************
         ********************************/
         clsGroupItem *groupItem = groupInfo->getGroupItem() ;
         UINT32 nodeNum = groupInfo->getGroupSize() ;
         if ( nodeNum <= 0 )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "couldn't send the request to empty group" );
            break;
         }

         MsgRouteID routeID ;
         routeID.value = MSG_INVALID_ROUTEID ;

         UINT32 beginPos;
         rc = rtnCoordGetNodePos( cb, groupInfo, pHeader->TID, beginPos );
         if ( rc )
         {
            PD_LOG( PDERROR,
                  "failed to send the request(rc=%d)",
                  rc );
            break;
         }

         UINT32 i = 0;
         while ( i != nodeNum )
         {
            if ( i + 1 == nodeNum
               && PREFER_REPL_ANYONE != preferReplicaType
               && PREFER_REPL_MASTER != preferReplicaType )
            {
               routeID = groupItem->primary( type );
               if ( MSG_INVALID_ROUTEID == routeID.value )
               {
                  rc = groupItem->getNodeID( beginPos, routeID, type ) ;
                  if ( rc != SDB_OK )
                  {
                     break;
                  }
               }
               else
               {
                  beginPos = groupItem->getPrimaryPos();
               }
            }
            else
            {
               if ( PREFER_REPL_ANYONE != preferReplicaType
                  && PREFER_REPL_MASTER != preferReplicaType
                  && beginPos == groupItem->getPrimaryPos() )
               {
                  beginPos = ( beginPos + 1 ) % nodeNum ;
                  continue;
               }
               rc = groupItem->getNodeID( beginPos, routeID, type ) ;
               if ( rc != SDB_OK )
               {
                  break;
               }
            }
            SINT32 status = NET_NODE_STAT_NORMAL ;
            if ( SDB_OK == groupItem->getNodeInfo( beginPos, status )
               && NET_NODE_STAT_NORMAL == status )
            {
               rc = pRouteAgent->syncSend( routeID, pBuffer, reqID, cb ) ;
            }
            else
            {
               rc = SDB_CLS_NODE_BSFAULT ;
            }
            ++i;

            if ( SDB_OK == rc )
            {
               sendNodes[reqID] = routeID ;

               if ( pSession )
               {
                  pSession->addLastNode( routeID );
               }
               break;
            }
            beginPos = ( beginPos + 1 ) % nodeNum ;
         }
         if ( rc != SDB_OK && !hasRetry &&
              CATALOG_GROUPID != groupInfo->getGroupID() )
         {
            rc = rtnCoordGetGroupInfo( cb, groupInfo->getGroupID(),
                                       TRUE, groupInfo );
            if ( SDB_OK == rc )
            {
               isNeedRetry = TRUE;
            }
         }
      }while ( isNeedRetry );
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTOONE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTOONE2, "rtnCoordSendRequestToOne" )
   INT32 rtnCoordSendRequestToOne( MsgHeader *pBuffer,
                                   CoordGroupInfoPtr &groupInfo,
                                   REQUESTID_MAP &sendNodes,
                                   netMultiRouteAgent *pRouteAgent,
                                   const netIOVec &iov,
                                   MSG_ROUTE_SERVICE_TYPE type,
                                   pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      BOOLEAN hasRetry = FALSE;
      BOOLEAN isNeedRetry = FALSE;
      UINT64 reqID = 0;
      INT32 preferReplicaType = PREFER_REPL_ANYONE;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTOONE2 ) ;
      do
      {
         hasRetry = isNeedRetry;
         isNeedRetry = FALSE;
         /*******************************
         ********************************/
         MsgHeader *pHeader = (MsgHeader *)pBuffer ;
         CoordSession *pSession = cb->getCoordSession();
         if ( NULL != pSession )
         {
            preferReplicaType = pSession->getPreferReplType();
            MsgRouteID routeID = pSession->getLastNode(groupInfo->getGroupID());
            if ( routeID.value != 0 )
            {
               rc = pRouteAgent->syncSend( routeID, pBuffer, iov, reqID, cb ) ;
               if ( SDB_OK == rc )
               {
                  sendNodes[reqID] = routeID ;
                  break;
               }
               else
               {
                  pSession->removeLastNode( groupInfo->getGroupID() );
               }
            }
         }

         /*******************************
         ********************************/
         clsGroupItem *groupItem = groupInfo->getGroupItem() ;
         UINT32 nodeNum = groupInfo->getGroupSize() ;
         if ( nodeNum <= 0 )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "couldn't send the request to empty group" );
            break;
         }

         MsgRouteID routeID ;
         routeID.value = MSG_INVALID_ROUTEID ;

         UINT32 beginPos;
         rc = rtnCoordGetNodePos( cb, groupInfo, pHeader->TID, beginPos );
         if ( rc )
         {
            PD_LOG( PDERROR,
                  "failed to send the request(rc=%d)",
                  rc );
            break;
         }

         UINT32 i = 0;
         while ( i != nodeNum )
         {
            if ( i + 1 == nodeNum
               && PREFER_REPL_ANYONE != preferReplicaType
               && PREFER_REPL_MASTER != preferReplicaType )
            {
               routeID = groupItem->primary( type );
               if ( MSG_INVALID_ROUTEID == routeID.value )
               {
                  rc = groupItem->getNodeID( beginPos, routeID, type ) ;
                  if ( rc != SDB_OK )
                  {
                     break;
                  }
               }
               else
               {
                  beginPos = groupItem->getPrimaryPos();
               }
            }
            else
            {
               if ( PREFER_REPL_ANYONE != preferReplicaType
                  && PREFER_REPL_MASTER != preferReplicaType
                  && beginPos == groupItem->getPrimaryPos() )
               {
                  beginPos = ( beginPos + 1 ) % nodeNum ;
                  continue;
               }
               rc = groupItem->getNodeID( beginPos, routeID, type ) ;
               if ( rc != SDB_OK )
               {
                  break;
               }
            }
            SINT32 status = NET_NODE_STAT_NORMAL ;
            if ( SDB_OK == groupItem->getNodeInfo( beginPos, status )
               && NET_NODE_STAT_NORMAL == status )
            {
               rc = pRouteAgent->syncSend( routeID, pBuffer, iov, reqID, cb ) ;
            }
            else
            {
               rc = SDB_CLS_NODE_BSFAULT ;
            }
            ++i;

            if ( SDB_OK == rc )
            {
               sendNodes[reqID] = routeID ;

               if ( pSession )
               {
                  pSession->addLastNode( routeID );
               }
               break;
            }
            beginPos = ( beginPos + 1 ) % nodeNum ;
         }
         if ( rc != SDB_OK && !hasRetry &&
              CATALOG_GROUPID != groupInfo->getGroupID() )
         {
            rc = rtnCoordGetGroupInfo( cb, groupInfo->getGroupID(),
                                       TRUE, groupInfo );
            if ( SDB_OK == rc )
            {
               isNeedRetry = TRUE;
            }
         }
      }while ( isNeedRetry );
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTOONE2, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOSENDREQUESTTOLAST, "rtnCoordSendRequestToLast" )
   INT32 rtnCoordSendRequestToLast( CHAR * pBuffer,
                                    const CoordGroupInfoPtr & groupInfo,
                                    REQUESTID_MAP & sendNodes,
                                    netMultiRouteAgent * pRouteAgent,
                                    pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      UINT64 reqID = 0;
      PD_TRACE_ENTRY ( SDB_RTNCOSENDREQUESTTOLAST ) ;
      MsgHeader *pHeader = (MsgHeader *)pBuffer ;
      MsgRouteID routeID ;
      routeID.value = MSG_INVALID_ROUTEID ;
      CoordSession *pSession = cb->getCoordSession();


      PD_CHECK( pSession, SDB_SYS, error, PDERROR,
                "Not found the session[%d]", pHeader->TID ) ;

      routeID = pSession->getLastNode( groupInfo->getGroupID() ) ;
      PD_CHECK( routeID.value != MSG_INVALID_ROUTEID, SDB_SYS, error, PDERROR,
                "The session[%d] not found last send node", pHeader->TID ) ;

      rc = pRouteAgent->syncSend( routeID, pBuffer, reqID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Send msg to node[%d:%d:%d] failed, rc: %d",
                   routeID.columns.groupID, routeID.columns.nodeID,
                   routeID.columns.serviceID, rc ) ;

      sendNodes[reqID] = routeID ;

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOSENDREQUESTTOLAST, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROCESSGETGROUPREPLY, "rtnCoordProcessGetGroupReply" )
   INT32 rtnCoordProcessGetGroupReply ( MsgCatGroupRes *pReply,
                                       CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOPROCESSGETGROUPREPLY ) ;
      do
      {
         if ( SDB_OK == pReply->header.res &&
              pReply->header.header.messageLength >=
              (INT32)sizeof(MsgCatGroupRes)+5 )
         {
            try
            {
               BSONObj boGroupInfo( (CHAR *)pReply
                                    + sizeof(MsgCatGroupRes) );
               BSONElement beGroupID
                              = boGroupInfo.getField( CAT_GROUPID_NAME );
               if ( beGroupID.eoo() || !beGroupID.isNumber() )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDERROR,
                           "Process get-group-info-reply failed,"
                           "failed to get the field(%s)", CAT_GROUPID_NAME );
                  break;
               }
               CoordGroupInfo *pGroupInfo
                           = SDB_OSS_NEW CoordGroupInfo( beGroupID.number() );
               if ( NULL == pGroupInfo )
               {
                  rc = SDB_OOM;
                  PD_LOG ( PDERROR, "Process get-group-info-reply failed,"
                           "new failed ");
                  break;
               }
               CoordGroupInfoPtr groupInfoTmp( pGroupInfo );
               rc = groupInfoTmp->fromBSONObj( boGroupInfo );
               if ( rc != SDB_OK )
               {
                  PD_LOG ( PDERROR, "Process get-group-info-reply failed,"
                           "failed to parse the groupInfo(rc=%d)", rc );
                  break;
               }
               groupInfo = groupInfoTmp;
            }
            catch ( std::exception &e )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Process get-group-info-reply failed,"
                        "received unexpected error:%s", e.what() );
               break;
            }
         }
         else
         {
            rc = pReply->header.res;
         }
      }while ( FALSE );
      PD_TRACE_EXITRC ( SDB_RTNCOPROCESSGETGROUPREPLY, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROCESSQUERYCATREPLY, "rtnCoordProcessQueryCatReply" )
   INT32 rtnCoordProcessQueryCatReply ( MsgCatQueryCatRsp *pReply,
                                       CoordCataInfoPtr &cataInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOPROCESSQUERYCATREPLY ) ;
      do
      {
         if ( 0 == pReply->flags )
         {
            try
            {
               BSONObj boCataInfo( (CHAR *)pReply + sizeof(MsgCatQueryCatRsp) );
               BSONElement beName = boCataInfo.getField( CAT_CATALOGNAME_NAME );
               if ( beName.eoo() || beName.type() != String )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDERROR, "Failed to parse query-catalogue-reply,"
                           "failed to get the field(%s)",
                           CAT_CATALOGNAME_NAME );
                  break;
               }
               BSONElement beVersion = boCataInfo.getField( CAT_CATALOGVERSION_NAME );
               if ( beVersion.eoo() || !beVersion.isNumber() )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDERROR, "Failed to parse query-catalogue-reply, "
                           "failed to get the field(%s)",
                           CAT_CATALOGVERSION_NAME );
                  break;
               }

               CoordCataInfo *pCataInfoTmp = NULL;
               pCataInfoTmp = SDB_OSS_NEW CoordCataInfo( beVersion.number(),
                                                      beName.str().c_str() );
               if ( NULL == pCataInfoTmp )
               {
                  rc = SDB_OOM;
                  PD_LOG ( PDERROR,
                           "Failed to parse query-catalogue-reply, new failed");
                  break;
               }
               CoordCataInfoPtr cataInfoPtr( pCataInfoTmp );
               rc = cataInfoPtr->fromBSONObj( boCataInfo );
               if ( rc != SDB_OK )
               {
                  PD_LOG ( PDERROR, "Failed to parse query-catalogue-reply, "
                           "parse catalogue info from bson-obj failed(rc=%d)",
                           rc );
                  break;
               }
               PD_LOG ( PDDEBUG, "new catalog info:%s",
                        boCataInfo.toString( false, false ).c_str() );
               cataInfo = cataInfoPtr;
            }
            catch ( std::exception &e )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to parse query-catalogue-reply,"
                        "received unexcepted error:%s", e.what() );
               break;
            }
         }
         else
         {
            PD_LOG ( PDWARNING,
                     "received unexcepted reply while query catalogue(flag=%d)",
                     pReply->flags );
         }
         rc= pReply->flags;
      }while ( FALSE );
      PD_TRACE_EXITRC ( SDB_RTNCOPROCESSQUERYCATREPLY, rc ) ;
      return rc;
   }

   INT32 rtnCoordGetAllGroupList( pmdEDUCB * cb, CoordGroupList &groupList,
                                  const BSONObj *query, BOOLEAN exceptCata,
                                  BOOLEAN exceptCoord )
   {
      INT32 rc = SDB_OK ;
      GROUP_VEC vecGrpPtr ;

      rc = rtnCoordGetAllGroupList( cb, vecGrpPtr, query, exceptCata,
                                    exceptCoord ) ;
      if ( rc )
      {
         goto error ;
      }
      else
      {
         GROUP_VEC::iterator it = vecGrpPtr.begin() ;
         while ( it != vecGrpPtr.end() )
         {
            CoordGroupInfoPtr &ptr = *it ;
            groupList[ ptr->getGroupID() ] = ptr->getGroupID() ;
            ++it ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnCoordGetAllGroupList( pmdEDUCB * cb, GROUP_VEC &groupLst,
                                  const BSONObj *query, BOOLEAN exceptCata,
                                  BOOLEAN exceptCoord )
   {
      INT32 rc = SDB_OK;
      pmdKRCB *pKrcb = pmdGetKRCB();
      SDB_RTNCB *pRtncb = pKrcb->getRTNCB();
      CoordCB *pCoordcb = pKrcb->getCoordCB();
      INT32 bufferSize = 0;
      CHAR *pResultBuffer;
      MsgOpReply replyHeader;
      rtnCoordCommand *pCmdProcesser = NULL;
      CHAR *pListReq = NULL;
      SINT64 contextID = -1;

      BSONObj *err = NULL ;

      rtnCoordProcesserFactory *pProcesserFactory
               = pCoordcb->getProcesserFactory();
      pCmdProcesser = pProcesserFactory->getCommandProcesser(
         COORD_CMD_LISTGROUPS ) ;
      SDB_ASSERT( pCmdProcesser, "pCmdProcesser can't be NULL!" );
      rc = msgBuildQueryMsg( &pListReq, &bufferSize, COORD_CMD_LISTGROUPS,
                             0, 0, 0, -1, query ) ;
      PD_RC_CHECK( rc, PDERROR, "failed to build list groups request(rc=%d)",
                   rc );
      rc = pCmdProcesser->execute( pListReq, 0, &pResultBuffer, cb,
                                   replyHeader, &err ) ;
      SDB_ASSERT( NULL == err, "impossible" ) ;
      PD_RC_CHECK( rc, PDERROR, "failed to list groups(rc=%d)", rc ) ;

      contextID = replyHeader.contextID ;

      while ( TRUE )
      {
         rtnContextBuf buffObj ;

         rc = rtnGetMore( contextID, 1, buffObj, cb, pRtncb ) ;
         if ( rc )
         {
            if ( rc != SDB_DMS_EOC )
            {
               PD_RC_CHECK( rc, PDERROR, "failed to execute getmore(rc=%d)",
                            rc );
            }
            contextID = -1 ;
            rc = SDB_OK ;
            break ;
         }

         try
         {
            CoordGroupInfo *pGroupInfo = NULL ;
            CoordGroupInfoPtr groupInfoTmp ;
            BSONObj boGroupInfo( buffObj.data() ) ;
            BSONElement beGroupID = boGroupInfo.getField( CAT_GROUPID_NAME );
            PD_CHECK( beGroupID.isNumber(), SDB_INVALIDARG, error, PDERROR,
                      "failed to process group info, failed to get the field"
                      "(%s)", CAT_GROUPID_NAME ) ;
            pGroupInfo = SDB_OSS_NEW CoordGroupInfo( beGroupID.number() );
            PD_CHECK( pGroupInfo != NULL, SDB_OOM, error, PDERROR,
                      "malloc failed!" );
            groupInfoTmp = CoordGroupInfoPtr( pGroupInfo );
            rc = groupInfoTmp->fromBSONObj( boGroupInfo );
            PD_RC_CHECK( rc, PDERROR, "failed to parse the group info(rc=%d)",
                         rc ) ;

            if ( groupInfoTmp->getGroupID() == CATALOG_GROUPID )
            {
               if ( !exceptCata )
               {
                  groupLst.push_back( groupInfoTmp ) ;
               }
            }
            else if ( groupInfoTmp->getGroupID() == COORD_GROUPID )
            {
               if ( !exceptCoord )
               {
                  groupLst.push_back( groupInfoTmp ) ;
               }
            }
            else
            {
               groupLst.push_back( groupInfoTmp );
            }
            pCoordcb->addGroupInfo( groupInfoTmp );
            rc = rtnCoordUpdateRoute( groupInfoTmp, pCoordcb->getRouteAgent(),
                                      MSG_ROUTE_SHARD_SERVCIE ) ;
         }
         catch ( std::exception &e )
         {
            rc = SDB_SYS ;
            PD_RC_CHECK( rc, PDERROR, "Failed to process group info, received "
                         "unexpected error:%s", e.what() ) ;
         }
      }

   done:
      if ( -1 != contextID )
      {
         pRtncb->contextDelete( contextID, cb );
      }
      SAFE_OSS_FREE( pListReq ) ;
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordSendRequestToNodes( void *pBuffer,
                                    ROUTE_SET &nodes,
                                    netMultiRouteAgent *pRouteAgent,
                                    pmdEDUCB *cb,
                                    REQUESTID_MAP &sendNodes,
                                    ROUTE_RC_MAP &failedNodes )
   {
      INT32 rc = SDB_OK;
      ROUTE_SET::iterator iter = nodes.begin();
      while( iter != nodes.end() )
      {
         MsgRouteID routeID;
         routeID.value = *iter;
         rc = rtnCoordSendRequestToNode( pBuffer, routeID,
                                         pRouteAgent, cb,
                                         sendNodes );
         if ( rc != SDB_OK )
         {
            failedNodes[*iter] = rc ;
         }
         ++iter;
      }
      return SDB_OK;
   }

   INT32 rtnCoordReadALine( const CHAR *&pInput,
                           CHAR *pOutput )
   {
      INT32 rc = SDB_OK;
      while( *pInput != 0x0D && *pInput != 0x0A
            && *pInput != '\0' )
      {
         *pOutput = *pInput;
         ++pInput;
         ++pOutput;
      }
      *pOutput = '\0';
      return rc;
   }

   void rtnCoordClearRequest( pmdEDUCB *cb, REQUESTID_MAP &sendNodes )
   {
      REQUESTID_MAP::iterator iterMap = sendNodes.begin();
      while( iterMap != sendNodes.end() )
      {
         cb->getCoordSession()->delRequest( iterMap->first );
         sendNodes.erase( iterMap++ );
      }
   }


   INT32 rtnCoordGetSubCLsByGroups( const CoordSubCLlist &subCLList,
                                    const CoordGroupList &sendGroupList,
                                    pmdEDUCB *cb,
                                    CoordGroupSubCLMap &groupSubCLMap )
   {
      INT32 rc = SDB_OK;
      CoordGroupList::const_iterator iterSend;
      CoordSubCLlist::const_iterator iterCL
                                    = subCLList.begin();
      while( iterCL != subCLList.end() )
      {
         CoordCataInfoPtr cataInfo;
         CoordGroupList groupList;
         CoordGroupList::iterator iterGroup;
         rc = rtnCoordGetCataInfo( cb, (*iterCL).c_str(),
                                 FALSE, cataInfo );
         PD_RC_CHECK( rc, PDWARNING,
                     "failed to get catalog info of sub-collection(%s)",
                     (*iterCL).c_str() );
         cataInfo->getGroupLst( groupList );
         SDB_ASSERT( groupList.size() > 0, "group list can't be empty!" );
         iterGroup = groupList.begin();
         while( iterGroup != groupList.end() )
         {
            groupSubCLMap[ iterGroup->first ].push_back( (*iterCL) );
            ++iterGroup;
         }
         ++iterCL;
      }

      iterSend = sendGroupList.begin();
      while( iterSend != sendGroupList.end() )
      {
         groupSubCLMap.erase( iterSend->first );
         ++iterSend;
      }

   done:
      return rc;
   error:
      goto done;
   }


   INT32 rtnCoordParseGroupList( pmdEDUCB *cb,
                                 const BSONObj &obj,
                                 CoordGroupList &groupList )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      CoordGroupInfoPtr grpPtr ;

      ele = obj.getField( CAT_GROUPID_NAME ) ;
      if ( ele.type() == NumberInt )
      {
         groupList[ (UINT32)ele.numberInt() ] = (UINT32)ele.numberInt() ;
      }
      else if ( ele.type() == Array )
      {
         BSONObjIterator itr( ele.embeddedObject() ) ;
         while ( itr.more() )
         {
            ele = itr.next () ;
            if ( ele.type() != NumberInt )
            {
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            groupList[ (UINT32)ele.numberInt() ] = (UINT32)ele.numberInt() ;
         }
      }
      else if ( !ele.eoo() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      ele = obj.getField( FIELD_NAME_GROUPNAME ) ;
      if ( ele.type() == String )
      {
         rc = rtnCoordGetGroupInfo( cb, ele.str().c_str(), FALSE, grpPtr ) ;
         PD_RC_CHECK( rc, PDERROR, "Get group[%s] failed, rc: %d",
                      ele.str().c_str(), rc ) ;
         groupList[ grpPtr->getGroupID() ] = grpPtr->getGroupID() ;
      }
      else if ( ele.type() == Array )
      {
         BSONObjIterator itr( ele.embeddedObject() ) ;
         while ( itr.more() )
         {
            ele = itr.next() ;
            if ( ele.type() != String )
            {
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            rc = rtnCoordGetGroupInfo( cb, ele.str().c_str(), FALSE, grpPtr ) ;
            PD_RC_CHECK( rc, PDERROR, "Get group[%s] failed, rc: %d",
                         ele.str().c_str(), rc ) ;
            groupList[ grpPtr->getGroupID() ] = grpPtr->getGroupID() ;
         }
      }

   done:
      return rc ;
   error:
      PD_LOG( PDERROR, "Failed to parse group list from bson[%s], rc: %d",
              obj.toString().c_str(), rc ) ;
      goto done ;
   }

   INT32 rtnCoordParseGroupList( pmdEDUCB *cb, MsgOpQuery *pMsg,
                                 FILTER_BSON_ID filterObjID,
                                 CoordGroupList &groupList )
   {
      INT32 rc = SDB_OK ;
      INT32 flag = 0 ;
      CHAR *pCollectionName = NULL ;
      INT64 numToSkip = 0 ;
      INT64 numToReturn = 0 ;
      CHAR *pQuery = NULL ;
      CHAR *pSelector = NULL ;
      CHAR *pOrderBy = NULL ;
      CHAR *pHint = NULL ;
      CHAR *pParseData = NULL ;

      rc = msgExtractQuery( (CHAR *)pMsg, &flag, &pCollectionName, &numToSkip,
                            &numToReturn, &pQuery, &pSelector, &pOrderBy,
                            &pHint ) ;
      PD_RC_CHECK( rc, PDERROR, "Extract query msg failed, rc: %d", rc ) ;

      switch ( filterObjID )
      {
         case FILTER_ID_SELECTOR:
            pParseData = pSelector ;
            break ;
         case FILTER_ID_ORDERBY:
            pParseData = pOrderBy ;
            break ;
         case FILTER_ID_HINT:
            pParseData = pHint ;
            break ;
         default:
            pParseData = pQuery ;
            break ;
      }

      try
      {
         BSONObj obj( pParseData ) ;
         if ( !obj.isEmpty() )
         {
            rc = rtnCoordParseGroupList( cb, obj, groupList ) ;
            if ( rc )
            {
               goto error ;
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnGroupList2GroupPtr( pmdEDUCB * cb, CoordGroupList & groupList,
                                GROUP_VEC & groupPtrs )
   {
      INT32 rc = SDB_OK ;
      groupPtrs.clear() ;
      CoordGroupInfoPtr ptr ;

      CoordGroupList::iterator it = groupList.begin() ;
      while ( it != groupList.end() )
      {
         rc = rtnCoordGetGroupInfo( cb, it->second, FALSE, ptr ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get group[%d] info, rc: %d",
                      it->second, rc ) ;
         groupPtrs.push_back( ptr ) ;
         ++it ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnGroupPtr2GroupList( pmdEDUCB * cb, GROUP_VEC & groupPtrs,
                                CoordGroupList & groupList )
   {
      groupList.clear() ;

      GROUP_VEC::iterator it = groupPtrs.begin() ;
      while ( it != groupPtrs.end() )
      {
         CoordGroupInfoPtr &ptr = *it ;
         groupList[ ptr->getGroupID() ] = ptr->getGroupID() ;
         ++it ;
      }

      return SDB_OK ;
   }

   INT32 rtnCoordGetGroupNodes( pmdEDUCB *cb, const BSONObj &filterObj,
                                NODE_SEL_STY emptyFilterSel,
                                CoordGroupList &groupList, ROUTE_SET &nodes )
   {
      INT32 rc = SDB_OK ;
      CoordGroupInfoPtr ptr ;
      MsgRouteID routeID ;
      GROUP_VEC groupPtrs ;
      GROUP_VEC::iterator it ;

      UINT16 nodeID = 0 ;
      string hostName ;
      string svcName ;
      BOOLEAN emptyFilter = TRUE ;
      BSONElement ele ;

      nodes.clear() ;

      rc = rtnGroupList2GroupPtr( cb, groupList, groupPtrs ) ;
      PD_RC_CHECK( rc, PDERROR, "Group ids to group info failed, rc: %d", rc ) ;

      ele = filterObj.getField( CAT_NODEID_NAME ) ;
      if ( ele.type() == NumberInt )
      {
         nodeID = ele.numberInt() ;
         emptyFilter = FALSE ;
      }
      else if ( !ele.eoo() )
      {
         PD_LOG( PDWARNING, "Field[%s] type[%d] error", CAT_NODEID_NAME,
                 ele.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      ele = filterObj.getField( FIELD_NAME_HOST ) ;
      if ( ele.type() == String )
      {
         hostName = ele.str() ;
         emptyFilter = FALSE ;
      }
      else if ( !ele.eoo() )
      {
         PD_LOG( PDWARNING, "Field[%s] type[%d] error", FIELD_NAME_HOST,
                 ele.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      ele = filterObj.getField( PMD_OPTION_SVCNAME ) ;
      if ( ele.type() == String )
      {
         svcName = ele.str() ;
         emptyFilter = FALSE ;
      }
      else if ( !ele.eoo() )
      {
         PD_LOG( PDWARNING, "Field[%s] type[%d] error", PMD_OPTION_SVCNAME,
                 ele.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      it = groupPtrs.begin() ;
      while ( it != groupPtrs.end() )
      {
         ptr = *it ;

         routeID.value = MSG_INVALID_ROUTEID ;
         clsGroupItem *grp = ptr->getGroupItem() ;
         routeID.columns.groupID = grp->groupID() ;
         const VEC_NODE_INFO *nodesInfo = grp->getNodes() ;
         for ( VEC_NODE_INFO::const_iterator itrn = nodesInfo->begin() ;
               itrn != nodesInfo->end();
               ++itrn )
         {
            if ( FALSE == emptyFilter )
            {
               if ( 0 != nodeID && nodeID != itrn->_id.columns.nodeID )
               {
                  continue ;
               }
               else if ( !hostName.empty() &&
                         0 != hostName.compare( itrn->_host ) )
               {
                  continue ;
               }
               else if ( !svcName.empty() &&
                         0 != svcName.compare(
                         itrn->_service[MSG_ROUTE_LOCAL_SERVICE] ) )
               {
                  continue ;
               }
            }
            else
            {
               if ( NODE_SEL_PRIMARY == emptyFilterSel &&
                    itrn->_id.columns.nodeID !=
                    grp->primary().columns.nodeID )
               {
                  continue ;
               }
            }
            routeID.columns.nodeID = itrn->_id.columns.nodeID ;
            routeID.columns.serviceID = MSG_ROUTE_SHARD_SERVCIE ;
            nodes.insert( routeID.value ) ;
         }
         ++it ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOUPNODESTATBYRC, "rtnCoordUpdateNodeStatByRC" )
   void rtnCoordUpdateNodeStatByRC( MsgRouteID &routeID,
                                    INT32 retCode )
   {
      INT32 rc = SDB_OK;
      CoordGroupInfoPtr groupInfo;
      clsGroupItem *groupItem;
      NET_NODE_STATUS status = NET_NODE_STAT_NORMAL;
      if ( MSG_INVALID_ROUTEID == routeID.value )
      {
         goto done;
      }
      switch ( retCode )
      {
         case SDB_CLS_FULL_SYNC:
            {
               status = NET_NODE_STAT_FULLSYNC ;
               break ;
            }
         case SDB_NETWORK:
            {
               status = NET_NODE_STAT_OFFLINE ;
               break ;
            }
         default:
            {
               status = NET_NODE_STAT_NORMAL ;
            }
      }
      rc = rtnCoordGetLocalGroupInfo( routeID.columns.groupID,
                                    groupInfo );
      if ( SDB_OK != rc )
      {
         goto done;
      }
      groupItem = groupInfo->getGroupItem();
      groupItem->updateNodeStat( routeID.columns.nodeID,
                                 status );
   done:
      return ;
   }

   BOOLEAN rtnCoordWriteRetryRC( INT32 retCode )
   {
      if ( SDB_CLS_NOT_PRIMARY == retCode ||
           SDB_CLS_COORD_NODE_CAT_VER_OLD == retCode ||
           SDB_CLS_NODE_NOT_ENOUGH == retCode ||
           SDB_CLS_NO_CATALOG_INFO == retCode ||
           SDB_NETWORK == retCode ||
           SDB_NET_CANNOT_CONNECT == retCode ||
           SDB_CLS_GRP_NOT_EXIST == retCode ||
           SDB_CLS_NODE_NOT_EXIST == retCode ||
           SDB_CAT_NO_MATCH_CATALOG == retCode )
      {
         return TRUE ;
      }
      return FALSE ;
   }

   INT32 rtnCataChangeNtyToAllNodes( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      netMultiRouteAgent *pRouteAgent = sdbGetCoordCB()->getRouteAgent() ;
      MsgHeader ntyMsg ;
      ntyMsg.messageLength = sizeof( MsgHeader ) ;
      ntyMsg.opCode = MSG_CAT_GRP_CHANGE_NTY ;
      ntyMsg.requestID = 0 ;
      ntyMsg.routeID.value = 0 ;
      ntyMsg.TID = cb->getTID() ;

      CoordGroupList groupLst ;
      ROUTE_SET sendNodes ;
      REQUESTID_MAP successNodes ;
      ROUTE_RC_MAP failedNodes ;

      rc = rtnCoordGetAllGroupList( cb, groupLst, NULL, FALSE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get all group list, rc: %d", rc ) ;

      rc = rtnCoordGetGroupNodes( cb, BSONObj(), NODE_SEL_ALL,
                                  groupLst, sendNodes ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get nodes, rc: %d", rc ) ;
      if ( sendNodes.size() == 0 )
      {
         PD_LOG( PDWARNING, "Not found any node" ) ;
         rc = SDB_CLS_NODE_NOT_EXIST ;
         goto error ;
      }

      rtnCoordSendRequestToNodes( (void*)&ntyMsg, sendNodes, 
                                  pRouteAgent, cb, successNodes,
                                  failedNodes ) ;
      if ( failedNodes.size() != 0 )
      {
         rc = failedNodes.begin()->second ;
      }
      rtnCoordClearRequest( cb, successNodes ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

}

