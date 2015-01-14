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

   Source File Name = rtnCoordQuery.cpp

   Descriptive Name = Runtime Coord Query

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   query on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "coordSession.hpp"
#include "rtnCoord.hpp"
#include "rtnContext.hpp"
#include "rtnCB.hpp"
#include "msg.h"
#include "coordCB.hpp"
#include "rtnCoordCommon.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "msgMessage.hpp"
#include "rtnCoordQuery.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "rtn.hpp"
#include "rtnCoordCommands.hpp"
#include "pmdEDU.hpp"

using namespace bson;

namespace engine
{
   extern void needResetSelector( const BSONObj &,
                                  const BSONObj &,
                                  BOOLEAN & ) ;
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOQUERY_QUERYTODNGROUP, "rtnCoordQuery::queryToDataNodeGroup" )
   INT32 rtnCoordQuery::queryToDataNodeGroup( CHAR *pBuffer,
                                              CoordGroupList &groupLst,
                                              CoordGroupList &sendGroupLst,
                                              netMultiRouteAgent *pRouteAgent,
                                              pmdEDUCB *cb,
                                              rtnContextCoord *pContext,
                                              BOOLEAN sendToPrimary,
                                              std::set<INT32> *ignoreRCList )
   {
      INT32 rc = SDB_OK;
      INT32 rcTmp = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOQUERY_QUERYTODNGROUP ) ;
      BOOLEAN isNeedRetry = FALSE;
      BOOLEAN hasRetry = FALSE;
      MsgHeader *pHead = (MsgHeader *)pBuffer;
      SINT32 resCode = MAKE_REPLY_TYPE(pHead->opCode);
      BOOLEAN takeOver = FALSE ;

      do
      {
         hasRetry = isNeedRetry;
         isNeedRetry = FALSE;
         REQUESTID_MAP sendNodes;
         rcTmp= rtnCoordSendRequestToNodeGroups( pBuffer, groupLst,
                                                 sendToPrimary, pRouteAgent,
                                                 cb, sendNodes );
         if ( rcTmp != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to query on data-node, send request "
                     "failed(rc=%d)", rcTmp ) ;
            if ( SDB_OK == rc )
            {
               rc = rcTmp;
            }
         }

         REPLY_QUE replyQue;
         rcTmp = rtnCoordGetReply( cb, sendNodes, replyQue, resCode ) ;
         if ( rcTmp != SDB_OK )
         {
            PD_LOG ( PDWARNING, "Failed to query on data-node, get reply "
                     "failed(rc=%d)", rcTmp );
            if ( SDB_APP_INTERRUPT == rcTmp || SDB_OK == rc )
            {
               rc = rcTmp;
            }
         }

         while ( !replyQue.empty() )
         {
            MsgOpReply *pReply   = NULL;
            takeOver             = FALSE ;
            pReply               = (MsgOpReply *)(replyQue.front());
            rcTmp                = pReply->flags;
            replyQue.pop();
            UINT32 groupID = pReply->header.routeID.columns.groupID;

            if ( rcTmp != SDB_OK )
            {
               if ( SDB_DMS_EOC == rcTmp ||
                    ( ignoreRCList != NULL &&
                      ignoreRCList->find( rcTmp ) != ignoreRCList->end() ) )
               {
                  sendGroupLst[ groupID ] = groupID;
                  groupLst.erase( groupID );
                  rcTmp = SDB_OK;
               }
               else
               {
                  cb->getCoordSession()->removeLastNode( groupID );
                  if ( SDB_CLS_FULL_SYNC == rcTmp )
                  {
                     rtnCoordUpdateNodeStatByRC( pReply->header.routeID,
                                                 rcTmp );
                  }
                  if ( !hasRetry )
                  {
                     if ( SDB_CLS_FULL_SYNC == rcTmp )
                     {
                        isNeedRetry = TRUE;
                     }
                     else if ( SDB_CLS_NOT_PRIMARY == rcTmp && sendToPrimary )
                     {
                        CoordGroupInfoPtr groupInfoTmp ;
                        rcTmp = rtnCoordGetGroupInfo( cb, groupID, TRUE,
                                                      groupInfoTmp );
                        if ( rcTmp )
                        {
                           PD_LOG ( PDERROR, "Failed to update group info"
                                    "(groupID=%u, rc=%d)",
                                    pReply->header.routeID.columns.groupID,
                                    rcTmp );
                           if ( SDB_OK == rc )
                           {
                              rc = rcTmp;
                           }
                        }
                        isNeedRetry = TRUE ;
                     }
                  }
               }
            }
            else
            {
               rcTmp = pContext->addSubContext( pReply, takeOver );
               if ( SDB_OK == rcTmp )
               {
                  sendGroupLst[ groupID ] = groupID;
                  groupLst.erase( groupID );
               }
            }

            if ( rcTmp != SDB_OK )
            {
               if ( SDB_OK == rc || SDB_CLS_COORD_NODE_CAT_VER_OLD == rc )
               {
                  rc = rcTmp;
               }
               PD_LOG( PDERROR, "Failed to query on data node"
                       "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                       pReply->header.routeID.columns.groupID,
                       pReply->header.routeID.columns.nodeID,
                       pReply->header.routeID.columns.serviceID,
                       rcTmp );
            }
            if ( !takeOver )
            {
               SDB_OSS_FREE( pReply ) ;
            }
         }

         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to query on data-node(rc=%d)", rc ) ;
            break;
         }
      }while ( isNeedRetry ) ;

      PD_TRACE_EXITRC ( SDB_RTNCOQUERY_QUERYTODNGROUP, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOQUERY_GETNODEGROUPS, "rtnCoordQuery::getNodeGroups" )
   INT32 rtnCoordQuery::getNodeGroups( const CoordCataInfoPtr &cataInfo,
                                       const BSONObj &queryObj,
                                       const CoordGroupList &sendGroupLst,
                                       CoordGroupList &groupLst )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOQUERY_GETNODEGROUPS ) ;

      cataInfo->getGroupByMatcher( queryObj, groupLst ) ;
      PD_CHECK( groupLst.size() > 0, SDB_CAT_NO_MATCH_CATALOG, error, PDERROR,
               "failed to get match groups" );
      {
      CoordGroupList::const_iterator iterList
                              = sendGroupLst.begin();
      while( iterList != sendGroupLst.end() )
      {
         groupLst.erase( iterList->first );
         ++iterList;
      }
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOQUERY_GETNODEGROUPS, rc ) ;
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordQuery::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                 CHAR **ppResultBuffer, pmdEDUCB *cb,
                                 MsgOpReply &replyHeader,
                                 BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      rtnContextCoord *pContext        = NULL ;
      BSONObj boQuery;
      BSONObj boOrderBy;
      BSONObj boSelector ;
      CoordGroupList                   sendGroupList ;

      MsgHeader*pHeader                = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      SINT64 numToSkip = 0;
      SINT64 numToReturn = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      BSONObj *err = NULL ;
      MsgOpQuery *pSrc = (MsgOpQuery *)pReceiveBuffer ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector, &pOrderBy, &pHint );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to parse query request(rc=%d)", rc );

      if ( pCollectionName != NULL && '$' == pCollectionName[0] )
      {
         rtnCoordCommand *pCmdProcesser = NULL;
         rtnCoordProcesserFactory *pProcesserFactory
                  = pCoordcb->getProcesserFactory();
         pCmdProcesser = pProcesserFactory->getCommandProcesser(
            pCollectionName );
         PD_CHECK( pCmdProcesser != NULL, SDB_INVALIDARG, error, PDERROR,
                  "unknown command:%s", pCollectionName );
         rc = pCmdProcesser->execute( pReceiveBuffer, packSize,
                                      ppResultBuffer, cb, replyHeader,
                                      &err );
         SDB_ASSERT( NULL == err, "impossible" );
         PD_RC_CHECK( rc, PDERROR, "failed to execute the command(command:%s, "
                    "rc=%d)", pCollectionName, rc );
         goto done;
      }

      try
      {
         boQuery = BSONObj( pQuery );
         boOrderBy = BSONObj( pOrderBy );
         boSelector = BSONObj( pFieldSelector ) ;
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                     "occur unexpected error:%s",
                     e.what() );
      }

      rc = executeQuery( (CHAR *)pSrc, boQuery,
                          boSelector,
                          boOrderBy, pCollectionName,
                          pRouteAgent, cb, pContext ) ;
      PD_RC_CHECK( rc, PDERROR, "query failed(rc=%d)", rc );

      replyHeader.contextID = pContext->contextID() ;
      pContext->addSubDone( cb );
   done:
      return rc;
   error:
      replyHeader.flags = rc;
      goto done;
   }

   INT32 rtnCoordQuery::queryOnMainCL( CoordGroupSubCLMap &groupSubCLMap,
                                       MsgOpQuery *pSrc,
                                       pmdEDUCB *cb,
                                       netMultiRouteAgent *pRouteAgent,
                                       CoordGroupList &sendGroupList,
                                       rtnContextCoord *pContext )
   {
      INT32 rc = SDB_OK;
      INT32 rcTmp = SDB_OK;
      CHAR *pBuffer = NULL;
      INT32 bufferSize = 0;
      REPLY_QUE replyQue;
      BOOLEAN takeOver = FALSE ;

      SDB_ASSERT( pContext, "pContext can't be NULL!" );

      try
      {
         CoordGroupSubCLMap::iterator iterGroup;
         REQUESTID_MAP sendNodes;

         INT32 flag;
         CHAR *pCollectionName;
         SINT64 numToSkip;
         SINT64 numToReturn;
         CHAR *pQuery;
         CHAR *pFieldSelector;
         CHAR *pOrderBy;
         CHAR *pHint;
         BSONObj boQuery;
         BSONObj boFieldSelector;
         BSONObj boOrderBy;
         BSONObj boHint;
         rc = msgExtractQuery( (CHAR *)pSrc, &flag, &pCollectionName,
                               &numToSkip, &numToReturn, &pQuery,
                               &pFieldSelector, &pOrderBy, &pHint );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to parse query message(rc=%d)", rc );
         boQuery           = BSONObj( pQuery );
         boFieldSelector   = BSONObj( pFieldSelector );
         boOrderBy         = BSONObj( pOrderBy );
         boHint            = BSONObj( pHint );

         iterGroup = groupSubCLMap.begin();
         while( iterGroup != groupSubCLMap.end() )
         {
            BSONArrayBuilder babSubCL;
            CoordGroupInfoPtr groupInfo;
            CoordSubCLlist::iterator iterSubCL
                                    = iterGroup->second.begin();
            while( iterSubCL != iterGroup->second.end() )
            {
               babSubCL.append( *iterSubCL );
               ++iterSubCL;
            }
            BSONObjBuilder bobNewQuery;
            bobNewQuery.appendElements( boQuery );
            bobNewQuery.appendArray( CAT_SUBCL_NAME, babSubCL.arr() );
            BSONObj boNewQuery = bobNewQuery.obj();
            rc = msgBuildQueryMsg( &pBuffer, &bufferSize, pCollectionName,
                                   flag, 0, numToSkip, numToReturn,
                                   &boNewQuery, &boFieldSelector,
                                   &boOrderBy, &boHint );
            PD_CHECK( SDB_OK == rc, rc, RECV_MSG, PDERROR,
                      "Failed to build query message(rc=%d)", rc );
            {
               MsgOpQuery *pReqMsg = (MsgOpQuery *)pBuffer;
               pReqMsg->version = pSrc->version;
               pReqMsg->w = pSrc->w;
            }
            rc = rtnCoordGetGroupInfo( cb, iterGroup->first, FALSE,
                                       groupInfo ) ;
            PD_CHECK( SDB_OK == rc, rc, RECV_MSG, PDERROR,
                      "Failed to get group info(groupId=%u, rc=%d)",
                      iterGroup->first, rc ) ;
            rc = rtnCoordSendRequestToOne( pBuffer, groupInfo, sendNodes,
                                           pRouteAgent,
                                           MSG_ROUTE_SHARD_SERVCIE, cb ) ;
            PD_CHECK( SDB_OK == rc, rc, RECV_MSG, PDERROR,
                      "Failed to send request(rc=%d)", rc ) ;
            ++iterGroup;
         }

      RECV_MSG:
         rcTmp = rtnCoordGetReply( cb, sendNodes, replyQue, MSG_BS_QUERY_RES ) ;
         if ( SDB_OK != rcTmp )
         {
            PD_LOG( PDWARNING, "failed to get reply(rcTmp=%d)", rcTmp );
            if ( SDB_APP_INTERRUPT == rcTmp || SDB_OK == rc )
            {
               rc = rcTmp;
            }
         }
         while( !replyQue.empty() )
         {
            takeOver             = FALSE ;
            MsgOpReply *pReply   = NULL;
            pReply               = (MsgOpReply *)(replyQue.front());
            rcTmp                = pReply->flags ;
            replyQue.pop();
            UINT32 groupID = pReply->header.routeID.columns.groupID;
            if ( rcTmp != SDB_OK )
            {
               if ( SDB_OK == rc )
               {
                  rc = rcTmp;
               }
               if ( SDB_DMS_EOC == rcTmp )
               {
                  sendGroupList[ groupID ] = groupID;
                  groupSubCLMap.erase( groupID );
                  rcTmp = SDB_OK;
               }
               else
               {
                  CoordGroupInfoPtr groupInfoTmp;
                  cb->getCoordSession()->removeLastNode( groupID );
                  if ( SDB_CLS_FULL_SYNC == rcTmp )
                  {
                     rtnCoordUpdateNodeStatByRC( pReply->header.routeID,
                                                rcTmp );
                  }
                  else
                  {
                     rcTmp = rtnCoordGetGroupInfo( cb, groupID, TRUE,
                                                   groupInfoTmp );
                     if ( rcTmp )
                     {
                        PD_LOG ( PDERROR, "Failed to update group info"
                                 "(groupID=%u, rc=%d)",
                                 pReply->header.routeID.columns.groupID,
                                 rcTmp );
                        if ( SDB_OK == rc )
                        {
                           rc = rcTmp;
                        }
                     }
                  }
               }
            }
            else
            {
               rcTmp = pContext->addSubContext( pReply, takeOver );
               if ( SDB_OK == rcTmp )
               {
                  sendGroupList[ groupID ] = groupID;
                  groupSubCLMap.erase( groupID );
               }
            }
            if ( rcTmp != SDB_OK && SDB_OK == rc )
            {
               rc = rcTmp;
            }

            if ( !takeOver )
            {
               SDB_OSS_FREE( pReply );
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR, "Occur unexpected error:%s",
                      e.what() ) ;
      }
   done:
      if ( pBuffer != NULL )
      {
         SDB_OSS_FREE( pBuffer );
         pBuffer = NULL;
      }
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordQuery::executeQuery( CHAR *pSrcMsg ,
                                      const BSONObj &boQuery ,
                                      const BSONObj &boSelector,
                                      const BSONObj &boOrderBy ,
                                      const CHAR * pCollectionName ,
                                      netMultiRouteAgent *pRouteAgent ,
                                      pmdEDUCB *cb ,
                                      rtnContextCoord *&pContext )
   {
      SDB_ASSERT( pSrcMsg, "pSrcMsg can't be null!" ) ;
      SDB_ASSERT( pCollectionName, "pCollectionName can't be null!" ) ;
      INT32 rc = SDB_OK ;
      SINT64 contextID = -1 ;
      BOOLEAN isNeedRefresh = FALSE ;
      BOOLEAN hasRetry = FALSE ;
      CoordCataInfoPtr cataInfo ;
      CoordGroupList sendGroupList ;
      MsgOpQuery *pQuery = (MsgOpQuery *)pSrcMsg ;
      SDB_RTNCB *pRtncb = pmdGetKRCB()->getRTNCB() ;
      CHAR *newMsg = NULL ;
      CHAR *msg = pSrcMsg ;
      BOOLEAN needReset = FALSE ;
      
      rc = pRtncb->contextNew( RTN_CONTEXT_COORD,
                               (rtnContext **)&pContext,
                               contextID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to allocate context(rc=%d)", rc ) ;

      needResetSelector( boSelector, boOrderBy, needReset ) ;
      if ( needReset )
      {
         rc = _buildNewMsg( pSrcMsg, BSONObj(), newMsg ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build new msg:%d", rc ) ;
            goto error ;
         }
         pQuery = (MsgOpQuery *)newMsg ;
         msg = newMsg ;
      }

      if ( FLG_QUERY_EXPLAIN & pQuery->flags )
      {
         rc = pContext->open( BSONObj(), BSONObj(), -1, 0 ) ;
      }
      else
      {
         rc = pContext->open( boOrderBy,
                              needReset ? boSelector : BSONObj(),
                              pQuery->numToReturn,
                              pQuery->numToSkip ) ;
      }
      PD_RC_CHECK( rc, PDERROR, "Open context failed(rc=%d)", rc ) ;
      pQuery->header.routeID.value = 0;
      pQuery->header.TID = cb->getTID();
      if ( pQuery->numToReturn > 0 && pQuery->numToSkip > 0 )
      {
         pQuery->numToReturn += pQuery->numToSkip ;
      }
      pQuery->numToSkip = 0 ;

   retry:
      rc = rtnCoordGetCataInfo( cb, pCollectionName,
                                isNeedRefresh, cataInfo ) ;
      PD_RC_CHECK( rc, PDERROR,
                   "Failed to get the catalog info(collection:%s)",
                   pCollectionName ) ;
      pQuery->version = cataInfo->getVersion() ;
      if ( cataInfo->isMainCL() )
      {
         CoordSubCLlist subCLList ;
         CoordGroupSubCLMap groupSubCLMap ;
         rc = cataInfo->getMatchSubCLs( boQuery, subCLList ) ;
         PD_CHECK( SDB_OK == rc, rc, retry_check, PDWARNING,
                   "Failed to get match sub collection(rc=%d)",
                   rc ) ;
         rc = rtnCoordGetSubCLsByGroups( subCLList, sendGroupList,
                                         cb, groupSubCLMap ) ;
         PD_CHECK( SDB_OK == rc, rc, retry_check, PDWARNING,
                   "Failed to get sub-collection info(rc=%d)",
                   rc );
         rc = queryOnMainCL( groupSubCLMap, pQuery, cb, pRouteAgent,
                             sendGroupList, pContext ) ;
         PD_CHECK( SDB_OK == rc, rc, retry_check, PDWARNING,
                   "Query on main collection failed(rc=%d)",
                   rc );
      }
      else
      {
         CoordGroupList groupList ;
         rc = getNodeGroups( cataInfo, boQuery,
                             sendGroupList, groupList ) ;
         PD_CHECK( SDB_OK == rc, rc, retry_check, PDWARNING,
                   "Failed to get match sharding(rc=%d)",
                   rc ) ;
         rc = queryToDataNodeGroup( msg, groupList, sendGroupList,
                                    pRouteAgent, cb, pContext ) ;
         PD_CHECK( SDB_OK == rc, rc, retry_check, PDWARNING,
                   "Query on data node failed(rc=%d)",
                   rc ) ;
      }

   retry_check:
      if ( rc != SDB_OK )
      {
         if ( rc != SDB_APP_INTERRUPT && !hasRetry )
         {
            hasRetry = TRUE ;
            isNeedRefresh = TRUE ;
            goto retry ;
         }
         goto error ;
      }
   done:
      SAFE_OSS_FREE( newMsg ) ;
      return rc ;
   error:
      if ( SDB_CAT_NO_MATCH_CATALOG == rc )
      {
         rc = SDB_OK ;
         goto done ;
      }
      if ( contextID >= 0 )
      {
         pRtncb->contextDelete( contextID, cb ) ;
         contextID = -1 ;
         pContext = NULL ;
      }
      goto done ;
   }

   INT32 rtnCoordQuery::_buildNewMsg( const CHAR *msg,
                                      const bson::BSONObj &newSelector,
                                      CHAR *&newMsg )
   {
      INT32 rc = SDB_OK ;
      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      SINT64 numToSkip = 0;
      SINT64 numToReturn = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      BSONObj query ;
      BSONObj selector ;
      BSONObj orderBy ;
      BSONObj hint ;
      INT32 bufSize = 0 ;
      MsgOpQuery *pSrc = (MsgOpQuery *)msg;

      rc = msgExtractQuery( ( CHAR * )msg, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector, &pOrderBy, &pHint );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to parse query request(rc=%d)", rc );

      try
      {
         query = BSONObj( pQuery ) ;
         selector = BSONObj( pFieldSelector ) ;
         orderBy = BSONObj( pOrderBy ) ;
         hint = BSONObj( pHint ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected error happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &newMsg, &bufSize,
                             pCollectionName,
                             flag, pSrc->header.requestID,
                             numToSkip, numToReturn,
                             &query, &newSelector,
                             &orderBy, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to build new msg:%d", rc ) ;
         goto error ;
      } 
   done:
      return rc ;
   error:
      SAFE_OSS_FREE( newMsg ) ;
      goto done ;
   }

}
