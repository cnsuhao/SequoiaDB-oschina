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

   Source File Name = rtnCoordDelete.cpp

   Descriptive Name = Runtime Coord Delete

   When/how to use: this program may be used on binary and text-formatted
   version of runtime component. This file contains code logic for
   data delete request from coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoordDelete.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "msgMessage.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

using namespace bson;

namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCODEL_GETNODEGROUPS, "rtnCoordDelete::getNodeGroups" )
   INT32 rtnCoordDelete::getNodeGroups( const CoordCataInfoPtr &cataInfo,
                           bson::BSONObj &deleteObj,
                           CoordGroupList &sendGroupLst,
                           CoordGroupList &groupLst )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCODEL_GETNODEGROUPS ) ;
      cataInfo->getGroupByMatcher( deleteObj, groupLst );
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
      PD_TRACE_EXITRC ( SDB_RTNCODEL_GETNODEGROUPS, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCODEL_DELTODNGROUP, "rtnCoordDelete::deleteToDataNodeGroup" )
   INT32 rtnCoordDelete::deleteToDataNodeGroup( CHAR *pBuffer,
                                                CoordGroupList &groupLst,
                                                CoordGroupList &sendGroupLst,
                                                netMultiRouteAgent *pRouteAgent,
                                                pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCODEL_DELTODNGROUP ) ;
      BOOLEAN isNeedRetry = FALSE;
      BOOLEAN hasRetry = FALSE;
      MsgOpDelete *pDelMsg = (MsgOpDelete *)pBuffer;
      do
      {
         hasRetry = isNeedRetry;
         isNeedRetry = FALSE;
         REQUESTID_MAP sendNodes;
         if ( cb->isTransaction() )
         {
            pDelMsg->header.opCode = MSG_BS_TRANS_DELETE_REQ;
         }
         rc = rtnCoordSendRequestToNodeGroups( pBuffer, groupLst, TRUE,
                                               pRouteAgent, cb, sendNodes );
         if ( rc != SDB_OK )
         {
            rtnCoordClearRequest( cb, sendNodes );
            PD_LOG ( PDERROR, "failed to delete on data-node,"
                     "send request failed(rc=%d)");
            break;
         }
         REPLY_QUE replyQue;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                        MAKE_REPLY_TYPE(pDelMsg->header.opCode) );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDWARNING, "failed to delete on data-node,"
                     "get reply failed(rc=%d)", rc );
            break;
         }
         while ( !replyQue.empty() )
         {
            MsgOpReply *pReply = NULL;
            pReply = (MsgOpReply *)(replyQue.front());
            replyQue.pop();
            INT32 rcTmp = pReply->flags;
            if ( SDB_OK == rc || SDB_CLS_COORD_NODE_CAT_VER_OLD == rc )
            {
               if ( SDB_OK != rcTmp )
               {
                  if ( SDB_CLS_NOT_PRIMARY == rcTmp
                     && !hasRetry )
                  {
                     CoordGroupInfoPtr groupInfoTmp;
                     rcTmp = rtnCoordGetGroupInfo( cb,
                        pReply->header.routeID.columns.groupID,
                        TRUE, groupInfoTmp );
                     if ( SDB_OK == rcTmp )
                     {
                        isNeedRetry = TRUE;
                     }
                  }
                  if ( rcTmp )
                  {
                     rc = rcTmp ;
                     PD_LOG ( PDERROR, "failed to delete on data node"
                              "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                              pReply->header.routeID.columns.groupID,
                              pReply->header.routeID.columns.nodeID,
                              pReply->header.routeID.columns.serviceID,
                              rc );
                  }
               }
               else
               {
                  UINT32 groupID = pReply->header.routeID.columns.groupID ;
                  sendGroupLst[groupID] = groupID;
                  groupLst.erase( groupID );
               }
            }
            if ( NULL != pReply )
            {
               SDB_OSS_FREE( pReply );
            }
         }
      }while ( isNeedRetry );
      PD_TRACE_EXITRC ( SDB_RTNCODEL_DELTODNGROUP, rc ) ;
      return rc;
   }
   
   //PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCODEL_EXECUTE, "rtnCoordDelete::execute" )
   INT32 rtnCoordDelete::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                              CHAR **ppResultBuffer, pmdEDUCB *cb,
                              MsgOpReply &replyHeader,
                              BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      rtnCoordOperator *pRollbackOperator = NULL;
      BOOLEAN isNeedRefresh = FALSE;
      BOOLEAN hasRefresh = FALSE;
      CoordGroupList sendGroupLst;
      BSONObj boDeletor;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_DELETE_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      CHAR *pDeletor = NULL;
      CHAR *pHint = NULL;
      rc = msgExtractDelete( pReceiveBuffer, &flag, &pCollectionName,
                           &pDeletor, &pHint );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to parse delete request(rc=%d)", rc );

      try
      {
         boDeletor = BSONObj( pDeletor );
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                     "delete failed, received unexpected error:%s",
                     e.what() );
      }

      do
      {
         hasRefresh = isNeedRefresh;
         CoordCataInfoPtr cataInfo;
         rc = rtnCoordGetCataInfo(cb, pCollectionName, isNeedRefresh, cataInfo );
         PD_RC_CHECK( rc, PDERROR,
                     "delete failed, "
                     "failed to get the catalogue info(collection name:%s)",
                     pCollectionName );
         if ( cataInfo->isMainCL() )
         {
            std::set< INT32 > emptyRCList;
            CoordSubCLlist subCLList;
            rc = cataInfo->getMatchSubCLs( boDeletor, subCLList );
            PD_RC_CHECK( rc, PDERROR,
                        "failed to get match sub-collection(rc=%d)",
                        rc );
            rc = modifyOpOnMainCL( cataInfo, subCLList, (MsgHeader *)pReceiveBuffer,
                                 pRouteAgent, cb, isNeedRefresh, emptyRCList, sendGroupLst );
         }
         else
         {
            rc = deleteNormalCL( cataInfo, boDeletor, (MsgOpDelete *)pReceiveBuffer,
                                 pRouteAgent, cb, sendGroupLst );
         }
         if ( !hasRefresh && rtnCoordWriteRetryRC( rc ) )
         {
            isNeedRefresh = TRUE;
         }
         else
         {
            isNeedRefresh = FALSE;
         }
      }while( isNeedRefresh );
      if ( cb->isTransaction() )
      {
         rc = rc ? rc : cb->getTransRC();
      }
      PD_RC_CHECK( rc, PDERROR, "delete failed(rc=%d)", rc ) ;
   done:
      return rc;
   error:
      if ( cb->isTransaction() )
      {
         pRollbackOperator
               = pCoordcb->getProcesserFactory()->getOperator( MSG_BS_TRANS_ROLLBACK_REQ );
         if ( pRollbackOperator )
         {
            pRollbackOperator->execute( pReceiveBuffer, packSize, ppResultBuffer,
                                       cb, replyHeader, ppErrorObj );
            SDB_ASSERT( NULL == *ppErrorObj, "impossible" ) ;
         }
      }
      replyHeader.flags = rc;
      goto done;
   }

   INT32 rtnCoordDelete::deleteNormalCL( CoordCataInfoPtr cataInfo,
                                         bson::BSONObj &boDelete,
                                         MsgOpDelete *pDelMsg,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb,
                                         CoordGroupList &sendGroupLst )
   {
      INT32 rc = SDB_OK;
      CoordGroupList groupLst;
      rc = getNodeGroups( cataInfo, boDelete, sendGroupLst, groupLst );
      PD_RC_CHECK( rc, PDERROR,
                  "delete failed, couldn't get the match sharding(rc=%d)",
                  rc );

      rc = buildTransSession( groupLst, pRouteAgent, cb );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to build transaction session(rc=%d)",
                  rc );

      pDelMsg->version = cataInfo->getVersion();
      pDelMsg->header.routeID.value = 0;
      pDelMsg->header.TID = cb->getTID();
      rc = deleteToDataNodeGroup( (CHAR *)pDelMsg, groupLst,
                                 sendGroupLst, pRouteAgent, cb );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to delete on data node(rc=%d)", rc );
   done:
      return rc;
   error:
      adjustTransSession( sendGroupLst, pRouteAgent, cb );
      goto done;
   }

   INT32 rtnCoordDelete::buildOpMsg( const CoordCataInfoPtr &cataInfo,
                                    const CoordSubCLlist &subCLList,
                                    CHAR *pSrcMsg, CHAR *&pDstMsg,
                                    INT32 &bufferSize )
   {
      INT32 rc = SDB_OK;
      INT32 flag;
      CHAR *pCollectionName = NULL;
      CHAR *pDeletor = NULL;
      CHAR *pHint = NULL;
      BSONObj boDeletor;
      BSONObj boHint;
      rc = msgExtractDelete( pSrcMsg, &flag, &pCollectionName,
                           &pDeletor, &pHint );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to parse delete request(rc=%d)",
                  rc );
      try
      {
         boDeletor = BSONObj( pDeletor );
         boHint = BSONObj( pHint );
         BSONArrayBuilder babSubCL;
         CoordSubCLlist::const_iterator iterCL = subCLList.begin();
         while( iterCL != subCLList.end() )
         {
            babSubCL.append( *iterCL );
            ++iterCL;
         }
         BSONObjBuilder bobNewDeletor;
         bobNewDeletor.appendElements( boDeletor );
         bobNewDeletor.appendArray( CAT_SUBCL_NAME, babSubCL.arr() );
         BSONObj boNewDeletor = bobNewDeletor.obj();
         rc = msgBuildDeleteMsg( &pDstMsg, &bufferSize, pCollectionName,
                                 flag, 0, &boNewDeletor, &boHint );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to build delete request(rc=%d)",
                     rc );
         {
         MsgOpDelete *pReqMsg = (MsgOpDelete *)pDstMsg;
         MsgOpDelete *pSrcReq = (MsgOpDelete *)pSrcMsg;
         pReqMsg->version = cataInfo->getVersion();
         pReqMsg->w = pSrcReq->w;
         }
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                     "occur unexpected error:%s",
                     e.what() );
      }
   done:
      return rc;
   error:
      goto done;
   }
}
