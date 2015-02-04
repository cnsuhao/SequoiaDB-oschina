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

   Source File Name = rtnCoordCommands.cpp

   Descriptive Name = Runtime Coord Commands

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   user command processing on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoord.hpp"
#include "ossTypes.h"
#include "ossErr.h"
#include "msgMessage.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "rtnContext.hpp"
#include "netMultiRouteAgent.hpp"
#include "msgCatalog.hpp"
#include "rtnCoordCommands.hpp"
#include "rtnCoordCommon.hpp"
#include "msgCatalog.hpp"
#include "catCommon.hpp"
#include "../bson/bson.h"
#include "pmdOptions.hpp"
#include "rtnRemoteExec.hpp"
#include "dms.hpp"
#include "omagentDef.hpp"
#include "rtn.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "rtnCommand.hpp"
#include "dpsTransLockDef.hpp"
#include "coordSession.hpp"
#include "mthModifier.hpp"
#include "rtnCoordDef.hpp"
#include "aggrDef.hpp"
#include "spdSession.hpp"
#include "spdCoordDownloader.hpp"
#include "rtnDataSet.hpp"

using namespace bson;
namespace engine
{
   RTN_COORD_CMD_BEGIN
   RTN_COORD_CMD_ADD( COORD_CMD_BACKUP_OFFLINE, rtnCoordBackupOffline )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_BACKUPS, rtnCoordListBackup )
   RTN_COORD_CMD_ADD( COORD_CMD_REMOVE_BACKUP, rtnCoordRemoveBackup )
   RTN_COORD_CMD_ADD( COORD_CMD_LISTGROUPS,  rtnCoordCMDListGroups )
   RTN_COORD_CMD_ADD( COORD_CMD_LISTCOLLECTIONSPACES, rtnCoordCMDListCollectionSpace )
   RTN_COORD_CMD_ADD( COORD_CMD_LISTCOLLECTIONS, rtnCoordCMDListCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATECOLLECTIONSPACE, rtnCoordCMDCreateCollectionSpace )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATECOLLECTION, rtnCoordCMDCreateCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_ALTERCOLLECTION, rtnCoordCMDAlterCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_DROPCOLLECTION, rtnCoordCMDDropCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_DROPCOLLECTIONSPACE, rtnCoordCMDDropCollectionSpace )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTDATABASE, rtnCoordCMDSnapshotDataBase )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSYSTEM, rtnCoordCMDSnapshotSystem )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSESSIONS, rtnCoordCMDSnapshotSessions )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSESSIONSCUR, rtnCoordCMDSnapshotSessionsCur )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCONTEXTS, rtnCoordCMDSnapshotContexts )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCONTEXTSCUR, rtnCoordCMDSnapshotContextsCur )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTRESET, rtnCoordCMDSnapshotReset )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCOLLECTIONS, rtnCoordCMDSnapshotCollections )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCOLLECTIONSPACES, rtnCoordCMDSnapshotSpaces )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCATALOG, rtnCoordCMDSnapshotCata )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTDBINTR, rtnCoordCMDSnapshotDBIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSYSINTR, rtnCoordCMDSnapshotSysIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCLINTR, rtnCoordCMDSnapshotClIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCSINTR, rtnCoordCMDSnapshotCsIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCTXINTR, rtnCoordCMDSnapshotCtxIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCTXCURINTR, rtnCoordCMDSnapshotCtxCurIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSESSINTR, rtnCoordCMDSnapshotSessionIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSESSCURINTR, rtnCoordCMDSnapshotSessionCurIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_TESTCOLLECTIONSPACE, rtnCoordCMDTestCollectionSpace )
   RTN_COORD_CMD_ADD( COORD_CMD_TESTCOLLECTION, rtnCoordCMDTestCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATEGROUP, rtnCoordCMDCreateGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_REMOVEGROUP, rtnCoordCMDRemoveGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_ACTIVEGROUP, rtnCoordCMDActiveGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATENODE, rtnCoordCMDCreateNode )
   RTN_COORD_CMD_ADD( COORD_CMD_REMOVENODE, rtnCoordCMDRemoveNode )
   RTN_COORD_CMD_ADD( COORD_CMD_UPDATENODE, rtnCoordCMDUpdateNode )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATEINDEX, rtnCoordCMDCreateIndex )
   RTN_COORD_CMD_ADD( COORD_CMD_DROPINDEX, rtnCoordCMDDropIndex )
   RTN_COORD_CMD_ADD( COORD_CMD_STARTUPNODE, rtnCoordCMDStartupNode )
   RTN_COORD_CMD_ADD( COORD_CMD_SHUTDOWNNODE, rtnCoordCMDShutdownNode )
   RTN_COORD_CMD_ADD( COORD_CMD_SHUTDOWNGROUP, rtnCoordCMDShutdownGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_GETCOUNT, rtnCoordCMDGetCount )
   RTN_COORD_CMD_ADD( COORD_CMD_GETDATABLOCKS, rtnCoordCMDGetDatablocks )
   RTN_COORD_CMD_ADD( COORD_CMD_GETQUERYMETA, rtnCoordCMDGetQueryMeta )
   RTN_COORD_CMD_ADD( COORD_CMD_SPLIT, rtnCoordCMDSplit )
   RTN_COORD_CMD_ADD( COORD_CMD_WAITTASK, rtnCoordCmdWaitTask )
   RTN_COORD_CMD_ADD( COORD_CMD_GETINDEXES, rtnCoordCMDGetIndexes )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATECATAGROUP, rtnCoordCMDCreateCataGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_TRACESTART, rtnCoordCMDTraceStart )
   RTN_COORD_CMD_ADD( COORD_CMD_TRACESTOP, rtnCoordCMDTraceStop )
   RTN_COORD_CMD_ADD( COORD_CMD_TRACERESUME, rtnCoordCMDTraceResume )
   RTN_COORD_CMD_ADD( COORD_CMD_TRACESTATUS, rtnCoordCMDTraceStatus )
   RTN_COORD_CMD_ADD( COORD_CMD_EXPCONFIG, rtnCoordCMDExpConfig )
   RTN_COORD_CMD_ADD( COORD_CMD_CRT_PROCEDURE, rtnCoordCMDCrtProcedure )
   RTN_COORD_CMD_ADD( COORD_CMD_EVAL, rtnCoordCMDEval )
   RTN_COORD_CMD_ADD( COORD_CMD_RM_PROCEDURE, rtnCoordCMDRmProcedure )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_PROCEDURES, rtnCoordCMDListProcedures )
   RTN_COORD_CMD_ADD( COORD_CMD_DEFAULT, rtnCoordDefaultCommand )
   RTN_COORD_CMD_ADD( COORD_CMD_LINK, rtnCoordCMDLinkCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_UNLINK, rtnCoordCMDUnlinkCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_TASKS, rtnCoordCmdListTask )
   RTN_COORD_CMD_ADD( COORD_CMD_CANCEL_TASK, rtnCoordCmdCancelTask )
   RTN_COORD_CMD_ADD( COORD_CMD_SET_SESS_ATTR, rtnCoordCMDSetSessionAttr )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_DOMAINS, rtnCoordCMDListDomains )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATE_DOMAIN, rtnCoordCMDCreateDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_DROP_DOMAIN, rtnCoordCMDDropDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_ALTER_DOMAIN, rtnCoordCMDAlterDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_CS_IN_DOMAIN, rtnCoordCMDListCSInDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_CL_IN_DOMAIN, rtnCoordCMDListCLInDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_INVALIDATE_CACHE, rtnCoordCMDInvalidateCache )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_LOBS, rtnCoordCMDListLobs )
   RTN_COORD_CMD_END

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCOM_PROCCATREPLY, "rtnCoordCommand::processCatReply" )
   INT32 rtnCoordCommand::processCatReply( MsgOpReply *pReply,
                                           CoordGroupList &groupLst )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCOM_PROCCATREPLY ) ;
      do
      {
         if ( SDB_OK == pReply->flags )
         {
            try
            {
               SINT32 curOffset = sizeof( MsgOpReply );
               BSONObj boReplyInfo( (CHAR *)pReply + curOffset );
               BSONElement beGroupArr = boReplyInfo.getField( CAT_GROUP_NAME );
               if ( beGroupArr.eoo() || beGroupArr.type() != Array )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDERROR, "Failed to get the field(%s),parse "
                           "catalogue-node's reply failed", CAT_GROUP_NAME ) ;
                  break;
               }
               BSONObjIterator i( beGroupArr.embeddedObject() );
               while ( i.more() )
               {
                  BSONElement beTmp = i.next();
                  BSONObj boGroupInfo = beTmp.embeddedObject();
                  BSONElement beGrpId = boGroupInfo.getField( CAT_GROUPID_NAME );
                  if ( beGrpId.eoo() || !beGrpId.isNumber() )
                  {
                     rc = SDB_INVALIDARG;
                     PD_LOG ( PDERROR, "Failed to get the field(%s), parse "
                              "catalogue-node's reply failed",
                              CAT_GROUPID_NAME );
                     break;
                  }
                  groupLst.insert(
                           CoordGroupList::value_type( beGrpId.number(),
                                                       beGrpId.number() ));
               }
            }
            catch ( std::exception &e )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to process catalogue-node's reply,"
                        "received unexpected error:%s", e.what() );
               break;
            }
         }
         else
         {
            rc = pReply->flags;
         }
      }while ( FALSE );
      PD_TRACE_EXITRC ( SDB_RTNCOCOM_PROCCATREPLY, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCOM_EXEONDATAGR, "rtnCoordCommand::executeOnDataGroup" )
   INT32 rtnCoordCommand::executeOnDataGroup ( MsgHeader *pMsg,
                                               CoordGroupList &groupLst,
                                               CoordGroupList &sendGroupLst,
                                               netMultiRouteAgent *pRouteAgent,
                                               pmdEDUCB *cb,
                                               BOOLEAN onPrimary,
                                               std::set<INT32> *ignoreRCList,
                                               std::map<UINT64, SINT64> *contexts )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCOM_EXEONDATAGR ) ;
      BOOLEAN isNeedRetry = FALSE;
      BOOLEAN hasRetry = FALSE;

      do
      {
         hasRetry = isNeedRetry;
         isNeedRetry = FALSE;
         REQUESTID_MAP sendNodes;
         rc = rtnCoordSendRequestToNodeGroups( (CHAR *)pMsg, groupLst,
                                               onPrimary, pRouteAgent, cb,
                                               sendNodes ) ;
         if ( rc != SDB_OK )
         {
            rtnCoordClearRequest( cb, sendNodes );
            PD_LOG ( PDERROR, "Failed to execute on data-node,send request "
                     "failed(rc=%d)", rc );
            break;
         }
         REPLY_QUE replyQue;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                                MAKE_REPLY_TYPE(pMsg->opCode) );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to execute on data-node, get reply "
                     "failed(rc=%d)", rc ) ;
            break;
         }
         while ( !replyQue.empty() )
         {
            MsgOpReply *pReply = NULL;
            pReply = (MsgOpReply *)(replyQue.front());
            replyQue.pop();
            INT32 rcTmp = pReply->flags;
            if ( SDB_OK == rcTmp ||
                 ( ignoreRCList &&
                   ignoreRCList->end() != ignoreRCList->find( rcTmp ) ) )
            {
               UINT32 groupID = pReply->header.routeID.columns.groupID;
               sendGroupLst[groupID] = groupID;
               groupLst.erase ( groupID );
               rcTmp = SDB_OK;
               if ( NULL != contexts && -1 != pReply->contextID )
               {
                  contexts->insert( std::make_pair(
                                       pReply->header.routeID.value,
                                       pReply->contextID ) ) ;
               }
            }
            if ( SDB_OK == rc || SDB_CLS_COORD_NODE_CAT_VER_OLD == rc )
            {
               if ( SDB_CLS_NOT_PRIMARY == rcTmp && !hasRetry )
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
               rc = rcTmp ? rcTmp : rc;
               if ( rc && rc != SDB_CLS_COORD_NODE_CAT_VER_OLD )
               {
                  PD_LOG ( PDERROR, "Failed to execute on data node"
                           "(groupID=%u, rc=%d)",
                           pReply->header.routeID.columns.groupID, rc ) ;
               }
            }
            SDB_OSS_FREE( pReply );
         }
      }while ( isNeedRetry );
      PD_TRACE_EXITRC ( SDB_RTNCOCOM_EXEONDATAGR, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCOM_QUERYONCATALOG, "rtnCoordCommand::queryOnCatalog" )
   INT32 rtnCoordCommand::queryOnCatalog ( CHAR *pReceiveBuffer, SINT32 packSize,
                                           CHAR **ppResultBuffer, pmdEDUCB *cb,
                                           MsgOpReply &replyHeader,
                                           INT32 requestType )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCOM_QUERYONCATALOG ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      SDB_RTNCB *pRtncb                = pKrcb->getRTNCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      SINT64 contextID                 = -1;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof(MsgOpReply);
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32       flag                 = 0;
      CHAR       *pCollectionName      = NULL;
      SINT64      numToSkip            = 0;
      SINT64      numToReturn          = 0;
      CHAR       *pQuery               = NULL;
      CHAR       *pFieldSelector       = NULL;
      CHAR       *pOrderBy             = NULL;
      CHAR       *pHint                = NULL;
      MsgOpQuery *pSrc                 = (MsgOpQuery *)pReceiveBuffer;
      rtnContextCoord *pContext        = NULL;
      MsgOpQuery *pListReq             = NULL;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector, &pOrderBy, &pHint );
      if ( rc )
      {
         PD_LOG ( PDERROR,
                  "failed to parse query request(rc=%d)",
                  rc );
         goto error ;
      }
      rc = pRtncb->contextNew( RTN_CONTEXT_COORD, (rtnContext**)&pContext,
                               contextID, cb );
      if ( rc )
      {
         PD_LOG ( PDERROR, "list groups failed, failed to allocate "
                  "context(rc=%d)", rc ) ;
         goto error ;
      }
      rc = pContext->open( BSONObj(), BSONObj(),
                           pSrc->numToReturn, pSrc->numToSkip ) ;
      PD_RC_CHECK( rc, PDERROR, "Open context failed, rc: %d", rc ) ;

      pListReq = pSrc;
      pListReq->header.opCode = requestType;
      pListReq->header.routeID.value = 0;
      pListReq->header.TID = cb->getTID();

      rc = executeOnCataGroup ( (CHAR*)pListReq, pRouteAgent,
                                cb, pContext ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "list groups failed, rc = %d", rc ) ;
         goto error ;
      }
   done :
      replyHeader.flags = rc;
      replyHeader.contextID = contextID ;
      return rc;
   error :
      if ( contextID >= 0 )
      {
         pRtncb->contextDelete( contextID, cb );
         contextID = -1 ;
      }
      PD_TRACE_EXIT ( SDB_RTNCOCOM_QUERYONCATALOG ) ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCOM_QUERYONCATAANDPUSHTOVEC, "rtnCoordCommand::queryOnCataAndPushToContext" )
   INT32 rtnCoordCommand::queryOnCataAndPushToVec( const rtnQueryOptions &options,
                                                   pmdEDUCB *cb,
                                                   std::vector<BSONObj> &objs )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOCOM_QUERYONCATAANDPUSHTOVEC ) ;
      SINT64 contextID = -1 ;
      rtnContextBuf bufObj ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      rc = queryOnCatalog( options, cb, contextID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to query on catalog:%d", rc ) ;
         goto error ;
      }

      do
      {
         rc = rtnGetMore( contextID, -1, bufObj, cb, rtnCB ) ;
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_OK ;
            contextID = -1 ;
            break ;
         }
         else if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to getmore from context:%d", rc ) ;
            goto error ;
         }
         else
         {
            while ( !bufObj.eof() )
            {
               BSONObj obj ;
               rc = bufObj.nextObj( obj ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "failed to get obj from obj buf:%d", rc ) ;
                  goto error ;
               }

               objs.push_back( obj.getOwned() ) ;
            }
            continue ;
         }
      } while ( TRUE ) ;
   done:

      if ( -1 != contextID )
      {
         rtnCB->contextDelete( contextID, cb ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNCOCOM_QUERYONCATAANDPUSHTOVEC, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCOM_QUERYONCATAANDPUSHTOCONTEXT, "rtnCoordCommand::queryOnCataAndPushToContext" )
   INT32 rtnCoordCommand::queryOnCatalog( const rtnQueryOptions &options,
                                          pmdEDUCB *cb,
                                          SINT64 &contextID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOCOM_QUERYONCATAANDPUSHTOCONTEXT ) ;
      BOOLEAN needToRetry = FALSE ;
      CoordGroupInfoPtr catGroupInfo ;
      REQUESTID_MAP sendNodes;
      REPLY_QUE replyQueue;
      CHAR *msgBuf = NULL ;
      INT32 msgBufLen = 0 ;
      netMultiRouteAgent *routeAgent = pmdGetKRCB()->getCoordCB(
         )->getRouteAgent() ;
      rtnContext *context = NULL ;

      rc = msgBuildQueryMsg( &msgBuf, &msgBufLen, options._fullName,
                             options._flag, 0, options._skip,
                             options._limit, &( options._query ),
                             &( options._selector ),
                             &( options._orderBy ),
                             &( options._hint ) ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to build query msg:%d", rc ) ;
         goto error ;
      }

      {
      MsgOpQuery *queryHeader = ( MsgOpQuery * )msgBuf ;
      queryHeader->header.routeID.value = MSG_INVALID_ROUTEID ;
      queryHeader->header.TID = cb->getTID();
      queryHeader->header.opCode = MSG_BS_QUERY_REQ ;
      queryHeader->header.messageLength = msgBufLen ;
      }

      rc = sdbGetRTNCB()->contextNew( RTN_CONTEXT_COORD,
                                      &context,
                                      contextID,
                                      cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to create coord context:%d", rc ) ;
         goto error ;
      }

      rc = ( ( rtnContextCoord * )context )->open( options._orderBy,
                                                   options._selector,
                                                   options._limit,
                                                   options._skip ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open context:%d", rc ) ;
         goto error ;
      }

      do
      {
         rc = rtnCoordGetCatGroupInfo( cb, needToRetry,  catGroupInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get cata group:%d, will retry ? :%d",
                    rc, !needToRetry ) ;
            if ( needToRetry )
            {
               goto error ;
            }
            else if ( SDB_RTN_NO_PRIMARY_FOUND == rc ||
                      SDB_CLS_NOT_PRIMARY == rc )
            {
               rc = SDB_OK ;
               needToRetry = TRUE ;
               continue ;
            }
            else
            {
               goto error ;
            }
         }

         rc = rtnCoordSendRequestToPrimary( msgBuf, catGroupInfo, sendNodes,
                                            routeAgent, MSG_ROUTE_CAT_SERVICE, cb );
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get cata group:%d, will retry ? :%d",
                    rc, !needToRetry ) ;
            if ( needToRetry )
            {
               goto error ;
            }
            else if ( SDB_RTN_NO_PRIMARY_FOUND == rc ||
                      SDB_CLS_NOT_PRIMARY == rc )
            {
               rc = SDB_OK ;
               needToRetry = TRUE ;
               continue ;
            }
            else
            {
               goto error ;
            }
         }
      } while ( needToRetry ) ;

      rc = rtnCoordGetReply( cb, sendNodes, replyQueue,
                             MAKE_REPLY_TYPE( ((MsgHeader*)msgBuf)->opCode ));
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get reply msg from catalog:%d", rc ) ;
         goto error ;
      }

      rc = _getReplyObjsFromQueue( replyQueue, cb, ( rtnContextCoord * )context ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get reply from queue:%d", rc ) ;
         goto error ;
      }
   done:
      if ( NULL != msgBuf )
      {
         SDB_OSS_FREE( msgBuf ) ;
         msgBuf = NULL ;
      }
      PD_TRACE_EXITRC( SDB_RTNCOCOM_QUERYONCATAANDPUSHTOCONTEXT, rc ) ;
      return rc ;
   error:
      if ( -1 != contextID )
      {
         sdbGetRTNCB()->contextDelete( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCOM__GETREPLYOBJSFROMQUEUE, "rtnCoordCommand::_getReplyObjsFromQueue" )
   INT32 rtnCoordCommand::_getReplyObjsFromQueue( REPLY_QUE &replyQueue,
                                                  pmdEDUCB *cb,
                                                  rtnContextCoord *context )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOCOM__GETREPLYOBJSFROMQUEUE ) ;

      while ( !replyQueue.empty() )
      {
         MsgOpReply *replyHeader = (MsgOpReply *)(replyQueue.front() );

         rc = replyHeader->flags ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "catalog returned a err:%d", rc ) ;
            goto error ;
         }

         if ( -1 != replyHeader->contextID )
         {
            rc = context->addSubContext( replyHeader->header.routeID,
                                         replyHeader->contextID ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to add sub context:%d", rc ) ;
               goto error ;
            }
         }


         if ( 0 < replyHeader->numReturned )
         {
            try
            {
               const CHAR *objPos = (const CHAR*)replyHeader + sizeof( MsgOpReply ) ;
               INT32 len = ( INT32 )
                           ( replyHeader->header.messageLength - sizeof( MsgOpReply )) ;
               _rtnObjBuff objBuf( objPos, len, replyHeader->numReturned ) ;
               while ( !objBuf.eof() )
               {
                  BSONObj obj ;
                  rc = objBuf.nextObj( obj ) ;
                  if ( SDB_OK != rc )
                  {
                     PD_LOG( PDERROR, "failed to get next obj from obj buf:%d", rc ) ;
                     goto error ;
                  }

                  rc = context->append( obj ) ;
                  if ( SDB_OK != rc )
                  {
                     PD_LOG( PDERROR, "failed to append object to context:%d", rc ) ;
                     goto error ;
                  }
               }
            }
            catch( std::exception &e )
            {
               PD_LOG( PDERROR, "unexpected err happeded: %s", e.what() ) ;
               rc = SDB_SYS ;
            }
         }

         replyQueue.pop();
         SDB_OSS_FREE ( replyHeader );
         break ;
      }
   done:
      while ( !replyQueue.empty() )
      {
         MsgOpReply *replyHeader = (MsgOpReply *)(replyQueue.front() );
         replyQueue.pop() ;
         SDB_OSS_FREE ( replyHeader );   
      }
      PD_TRACE_EXITRC( SDB_RTNCOCOM__GETREPLYOBJSFROMQUEUE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCOM_EXEONCATAGR, "rtnCoordCommand::executeOnCataGroup" )
   INT32 rtnCoordCommand::executeOnCataGroup ( CHAR *pBuffer,
                                               netMultiRouteAgent *pRouteAgent,
                                               pmdEDUCB *cb,
                                               rtnContextCoord *pContext,
                                               CoordGroupList *pGroupList,
                                               std::vector<BSONObj> *pReplyObjs )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCOM_EXEONCATAGR ) ;
      BOOLEAN isNeedRefresh = FALSE;
      CoordGroupInfoPtr catGroupInfo;
      REQUESTID_MAP sendNodes;
      REPLY_QUE replyQue;
      INT32 probe = 0 ;
   retry :
      rc = rtnCoordGetCatGroupInfo( cb, isNeedRefresh, catGroupInfo );
      if ( rc )
      {
         probe = 100 ;
         goto error ;
         PD_LOG ( PDERROR, "Execute on catalogue node failed, failed to get "
                  "catalogue group info(rc=%d)", rc );
      }
      rc = rtnCoordSendRequestToPrimary( pBuffer, catGroupInfo, sendNodes,
                                         pRouteAgent, MSG_ROUTE_CAT_SERVICE,
                                         cb );
      if ( rc )
      {
         probe = 200 ;
         goto error ;
      }
      rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                             MAKE_REPLY_TYPE(((MsgHeader*)pBuffer)->opCode) ) ;
      if ( rc )
      {
         probe = 300 ;
         goto error ;
      }
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = (MsgOpReply *)(replyQue.front());
         replyQue.pop();
         if ( SDB_OK == rc )
         {
            rc = pReply->flags ;
            if ( pContext && SDB_OK == rc && -1 != pReply->contextID )
            {
               rc = pContext->addSubContext ( pReply->header.routeID,
                                              pReply->contextID ) ;
            }
            if ( pGroupList && SDB_OK == rc && pReply->numReturned > 0 )
            {
               rc = processCatReply( pReply, *pGroupList) ;
            }
            if ( pReplyObjs && SDB_OK == rc && pReply->numReturned > 0 )
            {
               try
               {
                  const CHAR *objPos = (const CHAR*)pReply + sizeof( MsgOpReply ) ;
                  INT32 len = ( INT32 )
                              ( pReply->header.messageLength - sizeof( MsgOpReply )) ;
                  _rtnObjBuff objBuf( objPos, len, pReply->numReturned ) ;
                  while ( !objBuf.eof() )
                  {
                     BSONObj obj ;
                     rc = objBuf.nextObj( obj ) ;
                     if ( SDB_OK != rc )
                     {
                        PD_LOG( PDERROR, "failed to get next obj from obj buf:%d", rc ) ;
                        goto error ;
                     }

                     pReplyObjs->push_back( obj.getOwned() ) ;
                  }
               }
               catch( std::exception &e )
               {
                  PD_LOG( PDERROR, "Occur expcetion: %s", e.what() ) ;
                  rc = SDB_SYS ;
               }
            }
         }
         SDB_OSS_FREE ( pReply );
      }
      if ( rc )
      {
         probe = 400 ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_RTNCOCOM_EXEONCATAGR, rc ) ;
      return rc;
   error :
      rtnCoordClearRequest( cb, sendNodes );
      if ( ( SDB_RTN_NO_PRIMARY_FOUND == rc || SDB_CLS_NOT_PRIMARY == rc ) &&
           !isNeedRefresh )
      {
         isNeedRefresh = TRUE ;
         goto retry ;
      }
      switch ( probe )
      {
      case 100 :
         PD_LOG ( PDERROR,
                  "execute on catalogue node failed, "
                  "failed to get catalogue group info(rc=%d)",
                  rc );
         break ;
      case 200 :
         PD_LOG ( PDERROR,
                  "execute on catalogue node failed, "
                  "failed to send request to catalogue-node(rc=%d)",
                  rc );
         break ;
      case 300 :
         PD_LOG ( PDERROR,
                  "execute on catalogue node failed, "
                  "get reply failed(rc=%d)", rc );
         break ;
      case 400 :
         PD_LOG ( PDERROR,
                  "execute on catalogue node failed, "
                  "get error from reply queue(rc=%d)", rc ) ;
         break ;
      default :
         PD_LOG ( PDERROR,
                  "execute on catalogue node failed, rc = %d", rc ) ;
         break ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCOM__PRINTDEBUG, "rtnCoordCommand::_printDebug" )
   void rtnCoordCommand::_printDebug ( CHAR *pReceiveBuffer,
                                       const CHAR *pFuncName )
   {
   #if defined (_DEBUG)
      INT32 rc         = SDB_OK ;
      INT32 flag       = 0 ;
      CHAR *collection = NULL ;
      SINT64 skip      =  0;
      SINT64 limit     = -1 ;
      CHAR *query      = NULL ;
      CHAR *selector   = NULL ;
      CHAR *orderby    = NULL ;
      CHAR *hint       = NULL ;
      rc = msgExtractQuery( pReceiveBuffer, &flag, &collection,
                            &skip, &limit, &query, &selector,
                            &orderby, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract query msg:%d", rc ) ;
         goto error ;
      }

      try
      {
         BSONObj func( query ) ;
         PD_LOG( PDDEBUG, "%s: %s", pFuncName,
                 func.toString().c_str() ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done :
      return ;
   error :
      goto done ;
   #endif // _DEBUG
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCODEFCOM_EXE, "rtnCoordDefaultCommand::execute" )
   INT32 rtnCoordDefaultCommand::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                 CHAR **ppResultBuffer, pmdEDUCB *cb,
                                 MsgOpReply &replyHeader,
                                 BSONObj **ppErrorObj )
   {
      PD_TRACE_ENTRY ( SDB_RTNCODEFCOM_EXE ) ;
      MsgOpQuery *pQueryReq            = (MsgOpQuery *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof(MsgOpReply);
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pQueryReq->header.requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pQueryReq->header.TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_COORD_UNKNOWN_OP_REQ ;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;
      PD_TRACE_EXIT ( SDB_RTNCODEFCOM_EXE ) ;
      return SDB_COORD_UNKNOWN_OP_REQ ;
   }

   INT32 rtnCoordBackupBase::_getFilterFromMsg( CHAR *pReceiveBuffer,
                                                SINT32 packSize,
                                                BSONObj &filterObj,
                                                BSONObj *pOrderByObj,
                                                INT64 *pNumToReturn,
                                                INT64 *pNumToSkip )
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
      MsgOpQuery *pQueryMsg = (MsgOpQuery*)pReceiveBuffer ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName, &numToSkip,
                            &numToReturn, &pQuery, &pSelector, &pOrderBy,
                            &pHint ) ;
      PD_RC_CHECK( rc, PDERROR, "Extract query msg failed, rc: %d", rc ) ;

      if ( pNumToReturn )
      {
         *pNumToReturn = numToReturn ;
      }
      if ( pNumToSkip )
      {
         *pNumToSkip = numToSkip ;
      }

      if ( numToReturn > 0 && numToSkip > 0 )
      {
         pQueryMsg->numToReturn = numToReturn + numToSkip ;
      }
      pQueryMsg->numToSkip = 0 ;

      switch ( _getGroupMatherIndex() )
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
         filterObj = obj ;
         if ( pOrderByObj )
         {
            BSONObj orderby( pOrderBy ) ;
            *pOrderByObj = orderby ;
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

   INT32 rtnCoordBackupBase::_processReply( pmdEDUCB *cb, REPLY_QUE &replyQue,
                                            ROUTE_RC_MAP &failedNodes,
                                            rtnContextCoord *pContext )
   {
      INT32 rc = SDB_OK ;
      if ( _useContext () )
      {
         SDB_ASSERT( pContext != NULL, "pContext can't be NULL!" ) ;
      }

      while( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = ( MsgOpReply *)( replyQue.front() ) ;
         replyQue.pop() ;
         rc = pReply->flags ;

         if ( SDB_OK == rc )
         {
            if ( _useContext () )
            {
               if ( pReply->contextID != -1 )
               {
                  rc = pContext->addSubContext( pReply->header.routeID,
                                                pReply->contextID ) ;
                  if ( rc != SDB_OK )
                  {
                     PD_LOG ( PDERROR, "Failed to add sub-context, rc: %d",
                              rc ) ;
                  }
               }
               else
               {
                  rc = SDB_SYS ;
                  PD_LOG( PDERROR, "node return invalid contextID(%lld)",
                          pReply->contextID ) ;
               }
            }
         }
         else if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR, "Failed to process reply(groupID=%u, nodeID=%u, "
                    "serviceID=%u, rc:%d )",
                    pReply->header.routeID.columns.groupID,
                    pReply->header.routeID.columns.nodeID,
                    pReply->header.routeID.columns.serviceID, rc ) ;
            failedNodes[ pReply->header.routeID.value ] = rc ;
         }
         SDB_OSS_FREE( pReply ) ;
      }

      return SDB_OK ;
   }

   INT32 rtnCoordBackupBase::_buildFailedNodeReply( ROUTE_RC_MAP &failedNodes,
                                                    rtnContextCoord *pContext )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( pContext != NULL, "pContext can't be NULL!" ) ;

      CoordCB *pCoordcb = pmdGetKRCB()->getCoordCB() ;
      ROUTE_RC_MAP::iterator iter ;
      CoordGroupInfoPtr groupInfo ;
      std::string strHostName ;
      std::string strServiceName ;
      std::string strNodeName ;
      MsgRouteID routeID ;
      BSONObj errObj ;
      BSONObjBuilder builder ;
      BSONArrayBuilder arrayBD( builder.subarrayStart(
                                FIELD_NAME_ERROR_NODES ) ) ;

      if ( 0 == failedNodes.size() )
      {
         goto done ;
      }

      iter = failedNodes.begin() ;
      while ( iter != failedNodes.end() )
      {
         routeID.value = iter->first ;
         rc = pCoordcb->getGroupInfo( routeID.columns.groupID, groupInfo ) ;
         if ( rc )
         {
            PD_LOG( PDWARNING, "Failed to get group[%d] info, rc: %d",
                    routeID.columns.groupID, rc ) ;
            errObj = BSON( FIELD_NAME_NODEID <<
                           (INT32)routeID.columns.nodeID <<
                           FIELD_NAME_RCFLAG << iter->second ) ;
         }
         else
         {
            routeID.columns.serviceID = MSG_ROUTE_LOCAL_SERVICE ;
            rc = groupInfo->getGroupItem()->getNodeInfo( routeID, strHostName,
                                                         strServiceName ) ;
            if ( rc )
            {
               PD_LOG( PDWARNING, "Failed to get node[%d] info failed, rc: %d",
                       routeID.columns.nodeID, rc ) ;
               errObj = BSON( FIELD_NAME_NODEID <<
                              (INT32)routeID.columns.nodeID <<
                              FIELD_NAME_RCFLAG << iter->second ) ;
            }
            else
            {
               strNodeName = strHostName + ":" + strServiceName ;
               errObj = BSON( FIELD_NAME_NODE_NAME << strNodeName <<
                              FIELD_NAME_RCFLAG << iter->second ) ;
            }
         }

         arrayBD.append( errObj ) ;
         ++iter ;
      }

      arrayBD.done() ;
      rc = pContext->append( builder.obj() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to append obj, rc: %d", rc ) ;
      rc = SDB_OK ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnCoordBackupBase::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                      CHAR **ppResultBuffer, pmdEDUCB *cb,
                                      MsgOpReply &replyHeader,
                                      BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *pKrcb = pmdGetKRCB() ;
      CoordCB *pCoordcb = pKrcb->getCoordCB() ;
      netMultiRouteAgent *pRouteAgent = pCoordcb->getRouteAgent() ;
      SDB_RTNCB *pRtncb = pKrcb->getRTNCB() ;

      INT32 rcTmp = SDB_OK ;
      REPLY_QUE replyQue ;
      SINT64 contextID = -1 ;
      rtnContextCoord *pContext = NULL ;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode        = MSG_BS_QUERY_RES ;
      replyHeader.header.requestID     = pHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID           = pHeader->TID ;
      replyHeader.contextID            = -1 ;
      replyHeader.flags                = SDB_OK ;
      replyHeader.numReturned          = 0 ;
      replyHeader.startFrom            = 0 ;
      pHeader->TID = cb->getTID() ;

      BSONObj filterObj ;
      BSONObj orderBy ;
      INT64 numToReturn = -1 ;
      INT64 numToSkip = 0 ;

      CoordGroupList allGroupLst ;
      CoordGroupList groupLst ;

      ROUTE_SET sendNodes ;
      REQUESTID_MAP successNodes ;
      ROUTE_RC_MAP failedNodes ;

      rc = _getFilterFromMsg( pReceiveBuffer, packSize, filterObj, &orderBy,
                              &numToReturn, &numToSkip ) ;
      PD_RC_CHECK( rc, PDWARNING, "Failed to get filter obj, rc: %d", rc ) ;

      rc = rtnCoordGetAllGroupList( cb, allGroupLst ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get all group list, rc: %d", rc ) ;
      if ( !filterObj.isEmpty() )
      {
         rc = rtnCoordParseGroupList( cb, filterObj, groupLst ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to parse groups, rc: %d", rc  ) ;
      }
      if ( groupLst.size() == 0 )
      {
         groupLst = allGroupLst ;
      }
      rc = rtnCoordGetGroupNodes( cb, filterObj, _nodeSelWhenNoFilter(),
                                  groupLst, sendNodes ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get nodes, rc: %d", rc ) ;
      if ( sendNodes.size() == 0 )
      {
         PD_LOG( PDWARNING, "Node specfic nodes[%s]",
                 filterObj.toString().c_str() ) ;
         rc = SDB_CLS_NODE_NOT_EXIST ;
         goto error ;
      }
      if ( _useContext() )
      {
         rc = pRtncb->contextNew( RTN_CONTEXT_COORD, (rtnContext**)&pContext,
                                  contextID, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to create context, rc: %d", rc ) ;
         rc = pContext->open( orderBy, BSONObj(), numToReturn, numToSkip ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to open context, rc: %d", rc ) ;
      }
      rtnCoordSendRequestToNodes( pReceiveBuffer, sendNodes, 
                                  pRouteAgent, cb, successNodes,
                                  failedNodes ) ;
      rcTmp = rtnCoordGetReply( cb, successNodes, replyQue, MSG_BS_QUERY_RES,
                                TRUE, FALSE ) ;
      if ( rcTmp != SDB_OK )
      {
         PD_LOG( PDERROR, "Failed to get the reply, rc", rcTmp ) ;
      }
      _processReply( cb, replyQue, failedNodes, pContext ) ;

      if ( failedNodes.size() != 0 )
      {
         if ( FALSE == _allowFailed () || !_useContext() )
         {
            rc = rcTmp ? rcTmp : failedNodes.begin()->second ;
            goto error ;
         }
         else
         {
            rc = _buildFailedNodeReply( failedNodes, pContext ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to build failed node reply, "
                         "rc: %d", rc ) ;
         }
      }

      replyHeader.contextID = contextID ;

   done:
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL ;
         pReply = ( MsgOpReply *)( replyQue.front() ) ;
         replyQue.pop() ;
         SDB_OSS_FREE( pReply ) ;
      }
      return rc ;
   error:
      rtnCoordClearRequest( cb, successNodes ) ;
      replyHeader.flags = rc ;
      if ( contextID >= 0 )
      {
         pRtncb->contextDelete( contextID, cb ) ;
      }
      goto done ;
   }

   FILTER_BSON_ID rtnCoordListBackup::_getGroupMatherIndex ()
   {
      return FILTER_ID_HINT ;
   }

   NODE_SEL_STY rtnCoordListBackup::_nodeSelWhenNoFilter ()
   {
      return NODE_SEL_ALL ;
   }

   BOOLEAN rtnCoordListBackup::_allowFailed ()
   {
      return TRUE ;
   }

   BOOLEAN rtnCoordListBackup::_useContext ()
   {
      return TRUE ;
   }

   FILTER_BSON_ID rtnCoordRemoveBackup::_getGroupMatherIndex ()
   {
      return FILTER_ID_MATCHER ;
   }

   NODE_SEL_STY rtnCoordRemoveBackup::_nodeSelWhenNoFilter ()
   {
      return NODE_SEL_ALL ;
   }

   BOOLEAN rtnCoordRemoveBackup::_allowFailed ()
   {
      return TRUE ;
   }

   BOOLEAN rtnCoordRemoveBackup::_useContext ()
   {
      return FALSE ;
   }

   FILTER_BSON_ID rtnCoordBackupOffline::_getGroupMatherIndex ()
   {
      return FILTER_ID_MATCHER ;
   }

   NODE_SEL_STY rtnCoordBackupOffline::_nodeSelWhenNoFilter ()
   {
      return NODE_SEL_PRIMARY ;
   }

   BOOLEAN rtnCoordBackupOffline::_allowFailed ()
   {
      return FALSE ;
   }

   BOOLEAN rtnCoordBackupOffline::_useContext ()
   {
      return FALSE ;
   }

  /* INT32 rtnCoordBackupOffline::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                         CHAR **ppResultBuffer, pmdEDUCB *cb,
                                         MsgOpReply &replyHeader,
                                         BSONObj **ppErrorObj )
   {
      INT32 rc          = SDB_OK ;
      pmdKRCB *pKrcb                   = pmdGetKRCB() ;
      CoordCB *pCoordcb                = pKrcb->getCoordCB() ;
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent() ;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      CoordGroupList allGroupLst ;
      CoordGroupList groupLst ;
      CoordGroupList sendGroupLst ;

      pHeader->TID = cb->getTID() ;

      rc = rtnCoordGetAllGroupList( cb, allGroupLst ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get all group list, rc: %d", rc ) ;

      rc = rtnCoordParseGroupList( cb, (MsgOpQuery*)pReceiveBuffer,
                                   FILTER_ID_MATCHER, groupLst ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to parse groups, rc: %d", rc ) ;

      if ( groupLst.size() == 0 )
      {
         groupLst = allGroupLst ;
      }

      rc = executeOnDataGroup( pHeader, groupLst, sendGroupLst, pRouteAgent,
                               cb, NULL ) ;
      PD_RC_CHECK( rc, PDWARNING, "Backup in some group failed, rc: %d", rc ) ;

   done:
      replyHeader.flags = rc ;
      return rc ;
   error:
      goto done ;
   }*/

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDLISTGRS_EXE, "rtnCoordCMDListGroups::execute" )
   INT32 rtnCoordCMDListGroups::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                         CHAR **ppResultBuffer, pmdEDUCB *cb,
                                         MsgOpReply &replyHeader,
                                         BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDLISTGRS_EXE ) ;
      rc = queryOnCatalog ( pReceiveBuffer, packSize, ppResultBuffer, cb,
                            replyHeader, MSG_CAT_QUERY_DATA_GRP_REQ ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDLISTGRS_EXE, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCRCS_EXE, "rtnCoordCMDCreateCollectionSpace::execute" )
   INT32 rtnCoordCMDCreateCollectionSpace::execute( CHAR *pReceiveBuffer,
                                                    SINT32 packSize,
                                                    CHAR **ppResultBuffer,
                                                    pmdEDUCB *cb,
                                                    MsgOpReply &replyHeader,
                                                    BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCRCS_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *pCreateReq           = (MsgOpQuery *)pReceiveBuffer;
      pCreateReq->header.routeID.value = 0;
      pCreateReq->header.TID           = cb->getTID();
      pCreateReq->header.opCode        = MSG_CAT_CREATE_COLLECTION_SPACE_REQ;

      rc = executeOnCataGroup ( (CHAR*)pCreateReq, pRouteAgent,
                                cb, NULL, NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "create collectionspace failed, rc = %d", rc ) ;
         goto error ;
      }

   done :
      replyHeader.flags = rc ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCRCS_EXE, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDALCL_EXE, "rtnCoordCMDAlterCollection::execute" )
   INT32 rtnCoordCMDAlterCollection::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                    CHAR **ppResultBuffer, pmdEDUCB *cb,
                                    MsgOpReply &replyHeader, BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDALCL_EXE ) ;
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      MsgOpQuery *pAlterReq            = (MsgOpQuery *)pReceiveBuffer;
      pAlterReq->header.routeID.value  = 0;
      pAlterReq->header.TID            = cb->getTID();
      pAlterReq->header.opCode         = MSG_CAT_ALTER_COLLECTION_REQ;
      CoordGroupList groupList ;
      CoordGroupList sendList ;
      const CHAR *fullName             = NULL ;
      CHAR *queryBuf             = NULL ;
      CoordCataInfoPtr cataInfo ;
      CHAR **dummy                      = NULL ;
      INT32 *flags                     = NULL ;
      INT64 *numDummy                  = NULL ;

      rc = msgExtractQuery( pReceiveBuffer, flags, dummy,
                            numDummy, numDummy, &queryBuf,
                            dummy, dummy, dummy ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract query msg:%d", rc ) ;
         goto error ;
      }

      try
      {
         BSONObj query( queryBuf ) ;
         BSONElement ele = query.getField( FIELD_NAME_NAME ) ;
         if ( String != ele.type() )
         {
            PD_LOG( PDERROR, "invalid query object:%s",
                    query.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         fullName = ele.valuestr() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = executeOnCataGroup ( (CHAR*)pAlterReq, pRouteAgent, cb,
                                NULL, &groupList ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "alter collection failed on catalog, rc = %d",
                  rc ) ;
         goto error ;
      }

      rc = rtnCoordGetCataInfo( cb, fullName, TRUE, cataInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get cata info of cl[%s], rc:%d",
                 fullName, rc ) ;
         rc = SDB_BUT_FAILED_ON_DATA ;
         goto error ;
      }

      if ( !groupList.empty() )
      {
         pAlterReq->header.opCode = MSG_BS_QUERY_REQ ;
         pAlterReq->version = cataInfo->getVersion() ;

         rc = executeOnDataGroup( (MsgHeader *)pAlterReq, groupList,
                                   sendList, pRouteAgent, cb,
                                   TRUE ) ;
         if ( SDB_MAIN_CL_OP_ERR == rc ||
              SDB_CLS_COORD_NODE_CAT_VER_OLD == rc )
         {
            rc = SDB_OK ;
         }
         else if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to alter collection on data group:%d",
                    rc ) ;
            rc = SDB_BUT_FAILED_ON_DATA ;
            goto error ;
         }
         else
         {
         }
      }
   done :
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDALCL_EXE, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCRCL_EXE, "rtnCoordCMDCreateCollection::execute" )
   INT32 rtnCoordCMDCreateCollection::execute( CHAR *pReceiveBuffer,
                                               SINT32 packSize,
                                               CHAR **ppResultBuffer,
                                               pmdEDUCB *cb,
                                               MsgOpReply &replyHeader,
                                               BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCRCL_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      vector<BSONObj> replyFromCata ;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *pCreateReq           = (MsgOpQuery *)pReceiveBuffer;
      pCreateReq->header.routeID.value = 0;
      pCreateReq->header.TID           = cb->getTID();
      pCreateReq->header.opCode        = MSG_CAT_CREATE_COLLECTION_REQ;
      CoordGroupList groupLst ;
      CoordGroupList sendGroupLst ;
      BOOLEAN isMainCL                 = FALSE ;
      const CHAR *pCollectionName      = NULL ;

      try
      {
         CHAR *pCommandName = NULL ;
         INT32 flag = 0;
         SINT64 numToSkip = 0 ;
         SINT64 numToReturn = -1 ;
         CHAR *pQuery = NULL ;
         CHAR *pFieldSelector = NULL ;
         CHAR *pOrderBy = NULL ;
         CHAR *pHint = NULL ;
         BSONObj boQuery;
         BSONElement beIsMainCL ;
         BSONElement beShardingType ;
         BSONElement beShardingKey ;
         BSONElement eleName ;

         rc = msgExtractQuery( pReceiveBuffer, &flag, &pCommandName,
                               &numToSkip, &numToReturn, &pQuery,
                               &pFieldSelector, &pOrderBy, &pHint ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to parse the "
                      "create-collection-message(rc=%d)", rc ) ;
         boQuery = BSONObj( pQuery );
         beIsMainCL = boQuery.getField( FIELD_NAME_ISMAINCL );
         isMainCL = beIsMainCL.booleanSafe();
         if ( isMainCL )
         {
            beShardingKey = boQuery.getField( FIELD_NAME_SHARDINGKEY );
            PD_CHECK( beShardingKey.type() == Object, SDB_NO_SHARDINGKEY,
                      error, PDERROR, "There is no valid sharding-key field" ) ;
            beShardingType = boQuery.getField( FIELD_NAME_SHARDTYPE );
            PD_CHECK( 0 != beShardingType.str().compare(
                      FIELD_NAME_SHARDTYPE_HASH ),
                      SDB_INVALID_MAIN_CL_TYPE, error, PDERROR,
                      "The sharding-type of main-collection must be range" ) ;
         }
         eleName = boQuery.getField( FIELD_NAME_NAME ) ;
         if ( eleName.type() != String )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] is error",
                    FIELD_NAME_NAME, eleName.type() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         pCollectionName = eleName.valuestr() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR, "Failed to create collection, received "
                  "unexpected error: %s", e.what() ) ;
         goto error ;
      }

      if ( !isMainCL )
      {
         rc = executeOnCataGroup ( (CHAR*)pCreateReq, pRouteAgent,
                                   cb, NULL, &groupLst, &replyFromCata ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "create collection failed on catalog, rc = %d",
                     rc ) ;
            goto error ;
         }

         UINT32 retryTime = 0 ;
         while ( TRUE )
         {
            CoordCataInfoPtr cataInfo ;
            rc = rtnCoordGetCataInfo( cb, pCollectionName, TRUE, cataInfo ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to get catalog info of "
                         "collection[%s], rc: %d", pCollectionName, rc ) ;

            pCreateReq->header.opCode = MSG_BS_QUERY_REQ;
            pCreateReq->version = cataInfo->getVersion() ;
            rc = executeOnDataGroup( (MsgHeader *)pCreateReq, groupLst,
                                     sendGroupLst, pRouteAgent, cb, TRUE ) ;
            if ( rc != SDB_OK )
            {
               if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == rc && retryTime < 3 )
               {
                  ++retryTime ;
                  continue ;
               }
               PD_LOG ( PDWARNING, "Create collection failed on data "
                        "node(rc = %d)", rc ) ;
               rc = SDB_OK ;
            }
            break ;
         }
      }
      else
      {
         rc = executeOnCataGroup ( (CHAR*)pCreateReq, pRouteAgent,
                                   cb, NULL, NULL ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "create collection failed on catalog, rc = %d",
                     rc ) ;
            goto error ;
         }
      }

      if ( !isMainCL && !replyFromCata.empty() )
      {
      BSONElement task = replyFromCata.at(0).getField( CAT_TASKID_NAME ) ;
      if ( Array == task.type() )
      {
         rc = _notifyDataGroupsToStartTask( task, pRouteAgent, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to notify data groups to start task:%d", rc ) ;
            rc = SDB_BUT_FAILED_ON_DATA ;
            goto error ;
         }
      }
      }

   done :
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCRCL_EXE, rc ) ;
      return rc;
   /*error_rollback :
      pCreateReq->header.opCode = MSG_CAT_DROP_COLLECTION_REQ ;
      rcTmp = executeOnCataGroup( (CHAR *)pCreateReq, requestID,
                                  pRouteAgent, cb ) ;
      if ( rcTmp != SDB_OK )
      {
         PD_LOG ( PDERROR,
                  "failed to rollback creating collection(), "
                  "drop on catalogue-node failed(rc=%d)", rcTmp ) ;
         goto error ;
      }*/
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDSSONNODE__NOTIFYDATAGROUPS, "rtnCoordCMDCreateCollection::_notifyDataGroupsToStartTask" )
   INT32 rtnCoordCMDCreateCollection::_notifyDataGroupsToStartTask( const BSONElement &task,
                                                                    netMultiRouteAgent *agent,
                                                                    pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOCMDSSONNODE__NOTIFYDATAGROUPS ) ;
      CHAR *buffer = NULL ;
      INT32 bufferLen = 0 ;
      std::vector<BSONObj> reply ;
      BSONObjBuilder builder ;
      builder.appendAs( task, "$in" ) ;
      BSONObj condition = BSON( FIELD_NAME_TASKID << builder.obj() );
      MsgOpQuery *msgHeader = NULL ;
      INT32 everRc = SDB_OK ;
      CHAR *waitTaskResBuf = NULL ;
      BSONObj *waitTaskErrObj = NULL ;

      rc = msgBuildQueryMsg( &buffer, &bufferLen, CAT_TASK_INFO_COLLECTION,
                             0, 0, 0, -1, &condition, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to build query msg:%d", rc ) ;
         goto error ;
      }

      msgHeader = ( MsgOpQuery * )buffer ;
      msgHeader->header.routeID.value = MSG_INVALID_ROUTEID ;
      msgHeader->header.TID = cb->getTID();
      msgHeader->header.opCode = MSG_CAT_QUERY_TASK_REQ ;
      msgHeader->header.messageLength = bufferLen ;
      
      rc = executeOnCataGroup( buffer, agent,
                               cb, NULL, NULL, &reply ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get task info from catalog, rc:%d",rc ) ;
         goto error ;
      }

      {
      BSONElement group ;
      CoordGroupInfoPtr gpInfo ;
      CoordGroupList groupList ;
      CoordGroupList dummy ;
      std::set<INT32> ignore ;
      CoordCB *coordCb = pmdGetKRCB()->getCoordCB () ;
      rtnCoordCommand *cmd = coordCb->getProcesserFactory(
         )->getCommandProcesser( COORD_CMD_WAITTASK ) ;
      vector<BSONObj>::const_iterator itr = reply.begin() ;
      for ( ; itr != reply.end(); itr++ )
      {
         groupList.clear() ;
         dummy.clear() ;
         ignore.clear() ;

         group = itr->getField( FIELD_NAME_TARGETID ) ;
         if ( NumberInt != group.type() )
         {
            PD_LOG( PDERROR, "target id is not a numberint.[%s]",
                    itr->toString().c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         rc = rtnCoordGetGroupInfo( cb, group.Int(),
                                    TRUE, gpInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get group info:%d", rc ) ;
            goto error ;
         }

         groupList.insert( std::make_pair( gpInfo->getGroupID(),
                                           gpInfo->getGroupID()) ) ;

         rc = msgBuildQueryMsg( &buffer, &bufferLen,
                                CMD_ADMIN_PREFIX CMD_NAME_SPLIT,
                                0, 0, 0, -1, &(*itr), NULL, NULL, NULL ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build split msg:%d", rc ) ;
            goto error ;
         }

         msgHeader = ( MsgOpQuery * )buffer ;
         msgHeader->header.routeID.value = MSG_INVALID_ROUTEID ;
         msgHeader->header.TID = cb->getTID();
         msgHeader->header.opCode = MSG_BS_QUERY_REQ ;
         msgHeader->header.messageLength = bufferLen ;

         rc = executeOnDataGroup( (MsgHeader *)buffer,
                                   groupList, dummy,
                                   agent, cb, TRUE, &ignore ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to notify group[%s] to split",
                    gpInfo->groupName().c_str() ) ;
            everRc = ( SDB_OK == everRc ) ? rc : everRc ;
            rc = SDB_OK ;
            continue ;
         }
      }

      rc = ( SDB_OK == everRc ) ? SDB_OK : everRc ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "some spliting are fail, check the catalog info." ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &buffer, &bufferLen, "CAT",
                                0, 0, 0, -1, &condition, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to build split msg:%d", rc ) ;
         goto error ;
      }

      {
      MsgOpReply replyHeader ;
      rc = cmd->execute( buffer, bufferLen,
                         &waitTaskResBuf, cb,
                         replyHeader, &waitTaskErrObj ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to wait task done:%d", rc ) ;
         rc = SDB_OK ;
         ossSleep( 5000 ) ;
      }
      }
      }
   done:
      if ( NULL != buffer )
      {
         SDB_OSS_FREE( buffer ) ;
      }
      if ( NULL != waitTaskResBuf )
      {
         SDB_OSS_FREE( waitTaskResBuf ) ;
      }
      if ( NULL != waitTaskErrObj )
      {
         SDB_OSS_DEL( waitTaskErrObj ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNCOCMDSSONNODE__NOTIFYDATAGROUPS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSONNODE_EXE, "rtnCoordCMDSnapshotOnNode::execute" )
   INT32 rtnCoordCMDSnapshotOnNode::execute( CHAR *pReceiveBuffer,
                                             SINT32 packSize,
                                             CHAR **ppResultBuffer,
                                             pmdEDUCB *cb,
                                             MsgOpReply &replyHeader,
                                             BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSONNODE_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      SDB_RTNCB *pRtncb                = pKrcb->getRTNCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      SINT64 contextID                 = -1;

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
      do
      {
         INT32 flag = 0;
         CHAR *pCollectionName = NULL;
         SINT64 numToSkip = 0;
         SINT64 numToReturn = 0;
         CHAR *pQuery = NULL;
         CHAR *pFieldSelector = NULL;
         CHAR *pOrderBy = NULL;
         CHAR *pHint = NULL;
         MsgOpQuery *pSrc = (MsgOpQuery *)pReceiveBuffer;

         rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName,
                               &numToSkip, &numToReturn, &pQuery,
                               &pFieldSelector, &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "snapshot failed, failed to parse query request(rc=%d)",
                     rc );
            break;
         }

         UINT32 groupID;
         UINT16 nodeID;
         BSONObj boQuery;
         BSONObj boOrderBy;
         BSONObj boFieldSelector;
         BSONObj boHint;
         BSONObj boDummy ;
         try
         {
            boQuery = BSONObj ( pQuery ) ;
            boOrderBy = BSONObj( pOrderBy );
            boFieldSelector = BSONObj ( pFieldSelector );
            boHint = BSONObj ( pHint );
            BSONElement beGroupID = boQuery.getField( CAT_GROUPID_NAME );
            if ( beGroupID.eoo() || !beGroupID.isNumber() )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "snapshot failed, failed to get the field(%s)",
                        CAT_GROUPID_NAME );
               break;
            }
            groupID = beGroupID.number();
            BSONElement beNodeID = boQuery.getField( CAT_NODEID_NAME );
            if ( beNodeID.eoo() || !beNodeID.isNumber() )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "snapshot failed, failed to get the field(%s)",
                        CAT_NODEID_NAME );
               break;
            }
            nodeID = beNodeID.number();
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR,
                     "snapshot failed, received unexpected error:%s",
                     e.what() );
            break;
         }
         rtnContextCoord *pContext = NULL ;
         rc = pRtncb->contextNew( RTN_CONTEXT_COORD, (rtnContext**)&pContext,
                                  contextID, cb );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "snapshot failed, failed to allocate "
                     "context(rc=%d)", rc ) ;
            break;
         }

         rc = pContext->open( boOrderBy, BSONObj(), pSrc->numToReturn, pSrc->numToSkip ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR, "Open context failed, rc: %d", rc ) ;
            break ;
         }

         MsgOpQuery *pSnapshotReq = NULL;
         INT32 msgSize = 0;
         rc = BuildRequestMsg( ((CHAR **)&pSnapshotReq), &msgSize, flag,
                               numToSkip, numToReturn, &boDummy,
                               &boFieldSelector, &boOrderBy, &boHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "snapshot failed, failed to build the "
                     "request message(rc=%d)",
                     rc );
            break;
         }

         pSnapshotReq->header.routeID.value = 0;
         pSnapshotReq->header.TID = cb->getTID();
         BOOLEAN isNeedRefresh = FALSE;
         while ( TRUE )
         {
            CoordGroupInfoPtr groupInfo ;
            rc = rtnCoordGetGroupInfo( cb, groupID, isNeedRefresh, groupInfo );
            if ( rc != SDB_OK )
            {
               PD_LOG ( PDERROR, "snapshot failed, failed to get the group "
                        "info(groupID=%u, rc=%d)",
                        groupID, rc );
               break;
            }
            clsGroupItem *groupItem = groupInfo->getGroupItem() ;
            MsgRouteID routeID ;
            routeID.value = MSG_INVALID_ROUTEID ;
            INT32 nodePos = groupItem->nodePos( nodeID ) ;

            if ( nodePos >= 0 )
            {
               groupItem->getNodeID( nodePos, routeID,
                                     MSG_ROUTE_SHARD_SERVCIE ) ;
            }

            if ( MSG_INVALID_ROUTEID == routeID.value )
            {
               if ( !isNeedRefresh )
               {
                  isNeedRefresh = TRUE;
                  continue;
               }
               else
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDERROR,
                           "snapshot failed, failed to get the node info(groupID=%u, nodeID=%u)",
                           groupID, nodeID );
                  break;
               }
            }
            REQUESTID_MAP requestIdMap;
            rc = rtnCoordSendRequestToNode( (void *)pSnapshotReq, routeID,
                                            pRouteAgent,
                                            cb, requestIdMap );
            if ( rc != SDB_OK )
            {
               rtnCoordClearRequest( cb, requestIdMap );
               if ( !isNeedRefresh )
               {
                  isNeedRefresh = TRUE;
                  continue;
               }
               else
               {
                  PD_LOG ( PDERROR, "Snapshot failed, failed to send the "
                           "request to node(groupID=%u, nodeID=%u, rc=%d)",
                           groupID, nodeID, rc );
                  break;
               }
            }
            REPLY_QUE replyQue;
            rc = rtnCoordGetReply ( cb, requestIdMap, replyQue, MSG_BS_QUERY_RES );
            if ( rc != SDB_OK )
            {
               PD_LOG ( PDERROR,
                        "snapshot failed, failed to get the reply from node(rc=%d)",
                        rc );
               break;
            }
            while ( !replyQue.empty())
            {
               MsgOpReply *pReply = NULL;
               pReply = (MsgOpReply *)(replyQue.front());
               replyQue.pop();
               if ( SDB_OK == rc )
               {
                  if ( SDB_OK == pReply->flags &&
                       pReply->contextID != -1 )
                  {
                     rc = pContext->addSubContext( routeID,
                                       pReply->contextID );
                  }
                  else
                  {
                     rc = pReply->flags;
                  }
               }
               SDB_OSS_FREE( pReply );
            }
            if ( rc != SDB_OK )
            {
               if ( !isNeedRefresh )
               {
                  isNeedRefresh = TRUE;
                  continue;
               }
               else
               {
                  PD_LOG ( PDERROR, "Snapshot failed, error occured while "
                           "process node-reply(rc=%d)", rc );
               }
            }
            break;
         }
         if ( NULL != pSnapshotReq )
         {
            SDB_OSS_FREE( pSnapshotReq );
         }
      }while ( FALSE );
      replyHeader.flags = rc;
      if ( SDB_OK == rc  )
      {
         replyHeader.contextID = contextID;
      }
      else if ( contextID >= 0 )
      {
         pRtncb->contextDelete( contextID, cb );
      }
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSONNODE_EXE, rc ) ;
      return rc;
   }

   INT32 rtnCoordCMDSnapshotIntrBase::execute( CHAR *pReceiveBuffer,
                                               SINT32 packSize,
                                               CHAR **ppResultBuffer,
                                               pmdEDUCB *cb,
                                               MsgOpReply &replyHeader,
                                               BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      INT32 rcTmp = SDB_OK ;
      pmdKRCB *pKrcb = pmdGetKRCB() ;
      SDB_RTNCB *pRtncb = pKrcb->getRTNCB() ;
      CoordCB *pCoordcb = pKrcb->getCoordCB() ;
      netMultiRouteAgent *pRouteAgent = pCoordcb->getRouteAgent() ;
      ROUTE_SET sendNodes;
      REQUESTID_MAP successNodes;
      ROUTE_RC_MAP failedNodes;
      CHAR *pSnapshotReq = NULL;
      INT32 bufferSize = 0;
      REPLY_QUE replyQue;
      SINT64 contextID = -1;

      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      SINT64 numToSkip = 0;
      SINT64 numToReturn = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      BSONObj boQuery;
      BSONObj boOrderBy;
      BSONObj boFieldSelector;
      BSONObj boHint;
      BSONObj boDummy;
      BSONObj newQuery;
      rtnContextCoord *pContext = NULL ;

      MsgHeader *pHeader = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode = MSG_BS_QUERY_RES;
      replyHeader.header.requestID = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID = pHeader->TID;
      replyHeader.contextID = -1;
      replyHeader.flags = SDB_OK;
      replyHeader.numReturned = 0;
      replyHeader.startFrom = 0;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector, &pOrderBy, &pHint );
      PD_RC_CHECK( rc, PDERROR, "Snapshot failed, failed to parse query "
                   "request(rc=%d)", rc ) ;
      try
      {
         boQuery = BSONObj( pQuery );
         boOrderBy = BSONObj( pOrderBy );
         boFieldSelector = BSONObj( pFieldSelector );
         boHint = BSONObj( pHint );
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR, "Snapshot failed, received "
                      "unexpected error:%s", e.what() ) ;
      }
      rc = getNodes( cb, boQuery, sendNodes, newQuery );
      PD_RC_CHECK( rc, PDERROR, "Snapshot failed, failed to get node "
                   "list(rc=%d)", rc );
      rc = BuildRequestMsg(&pSnapshotReq, &bufferSize, flag, numToSkip,
                           numToReturn, &newQuery, &boFieldSelector,
                           &boOrderBy, &boHint );
      PD_RC_CHECK( rc, PDERROR, "Snapshot failed, failed to build "
                   "requst(rc=%d)", rc );
      rtnCoordSendRequestToNodes( pSnapshotReq, sendNodes, 
                                  pRouteAgent, cb, successNodes,
                                  failedNodes );

      rc = rtnCoordGetReply( cb, successNodes, replyQue, MSG_BS_QUERY_RES,
                           TRUE, FALSE );
      if ( rc != SDB_OK )
      {
         PD_LOG( PDERROR, "Failed to get the reply(rc=%d)", rc );
      }

      rcTmp = pmdGetKRCB()->getRTNCB()->contextNew( RTN_CONTEXT_COORD,
                                                    (rtnContext**)&pContext,
                                                    contextID, cb ) ;
      if ( rcTmp )
      {
         if ( SDB_OK == rc )
         {
            rc = rcTmp;
         }
         PD_LOG( PDERROR, "failed to create context(rc=%d)", rcTmp );
         goto error;
      }

      rcTmp = pContext->open( boOrderBy, BSONObj(), numToReturn, numToSkip ) ;
      if ( rcTmp )
      {
         if ( SDB_OK == rc )
         {
            rc = rcTmp;
         }
         PD_LOG( PDERROR, "failed to open context(rc=%d)", rcTmp );
         goto error;
      }

      processReply( cb, replyQue, failedNodes, pContext );

      rcTmp = buildFailedNodeReply( failedNodes, pContext );
      if ( rcTmp )
      {
         if ( SDB_OK == rc )
         {
            rc = rcTmp;
         }
         PD_LOG( PDERROR, "failed to build error reply(rc=%d)", rcTmp );
         goto error;
      }
      replyHeader.contextID = contextID ;

   done:
      SAFE_OSS_FREE( pSnapshotReq );
      while ( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = ( MsgOpReply *)( replyQue.front() );
         replyQue.pop();
         SDB_OSS_FREE( pReply );
      }
      return rc;
   error:
      rtnCoordClearRequest( cb, successNodes );
      replyHeader.flags = rc;
      if ( contextID >= 0 )
      {
         pRtncb->contextDelete( contextID, cb );
      }
      goto done;
   }

   INT32 rtnCoordCMDSnapshotIntrBase::getNodes( pmdEDUCB *cb, BSONObj &query,
                                                ROUTE_SET &nodes, BSONObj &newQuery )
   {
      INT32 rc = SDB_OK;
      UINT16 nodeID = 0;
      BSONObj groupCond;
      std::string strHost;
      std::string strSvcName;
      GROUP_VEC groupLst;
      UINT32 i = 0;

      try
      {
         BSONObjBuilder queryBuilder;
         BSONObjBuilder groupCondBuilder;
         BSONObjBuilder nodesCondBuilder;
         BSONObjIterator iter( query );
         while( iter.more() )
         {
            BSONElement beField = iter.next();
            if ( 0 == ossStrcmp( beField.fieldName(),
                                 CAT_GROUPID_NAME )
               || 0 == ossStrcmp( beField.fieldName(),
                                 FIELD_NAME_GROUPNAME ))
            {
               groupCondBuilder.append( beField );
               continue;
            }
            else if ( 0 == ossStrcmp( beField.fieldName(),
                                 CAT_NODEID_NAME )
               && beField.isNumber() )
            {
               nodesCondBuilder.append( beField );
               nodeID = beField.number();
               continue;
            }
            else if ( 0 == ossStrcmp( beField.fieldName(),
                                 FIELD_NAME_HOST )
               && beField.type() == String )
            {
               nodesCondBuilder.append( beField );
               strHost = beField.str();
               continue;
            }
            else if ( 0 == ossStrcmp( beField.fieldName(),
                                 PMD_OPTION_SVCNAME )
               && beField.type() == String )
            {
               nodesCondBuilder.appendAs( beField,
                              FIELD_NAME_SERVICE"."FIELD_NAME_NAME );
               strSvcName = beField.str();
               continue;
            }
            queryBuilder.append( beField );
         }
         BSONObj nodesObj = nodesCondBuilder.obj();
         if ( !nodesObj.isEmpty() )
         {
            groupCondBuilder.append( FIELD_NAME_GROUP,
                              BSON( "$elemMatch" << nodesObj ));
         }
         newQuery = queryBuilder.obj();
         groupCond = groupCondBuilder.obj();
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                     "received unexpected error:%s",
                     e.what() );
      }
      rc = rtnCoordGetAllGroupList( cb, groupLst, &groupCond );
      PD_RC_CHECK( rc, PDERROR, "failed to get group list(rc=%d)", rc );
      for ( i = 0; i < groupLst.size(); i++ )
      {
         if ( groupCond.isEmpty() && CATALOG_GROUPID == groupLst[i]->getGroupID() )
         {
            continue;
         }
         const VEC_NODE_INFO *pNodeLst
               = groupLst[i]->getGroupItem()->getNodes();
         SDB_ASSERT( pNodeLst != NULL, "pNodeLst can't be NULL!" );
         UINT32 j = 0;
         for ( j = 0; j < pNodeLst->size(); j++ )
         {
            if ( !strHost.empty()
                  && 0 != strHost.compare((*pNodeLst)[j]._host))
            {
               continue;
            }
            if ( !strSvcName.empty()
                  && 0 != strSvcName.compare(
                           (*pNodeLst)[j]._service[MSG_ROUTE_LOCAL_SERVICE]))
            {
               continue;
            }
            if ( nodeID != 0
                  && nodeID != (*pNodeLst)[j]._id.columns.nodeID )
            {
               continue;
            }
            MsgRouteID routeID = (*pNodeLst)[j]._id;
            routeID.columns.serviceID = MSG_ROUTE_SHARD_SERVCIE;
            nodes.insert( routeID.value );
         }
      }
      
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get nodes list(rc=%d)",
                  rc );
   done:
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordCMDSnapshotIntrBase::processReply( _pmdEDUCB * pEDUCB,
                                                   REPLY_QUE &replyQue,
                                                   ROUTE_RC_MAP &failedNodes,
                                                   rtnContextCoord *pContext )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT( pContext != NULL, "pContext can't be NULL!" ) ;

      while( !replyQue.empty() )
      {
         MsgOpReply *pReply = NULL;
         pReply = ( MsgOpReply *)( replyQue.front() );
         replyQue.pop();
         rc = pReply->flags;
         if ( SDB_OK == rc )
         {
            if ( pReply->contextID != -1 )
            {
               rc = pContext->addSubContext( pReply->header.routeID,
                                             pReply->contextID );
               if ( rc != SDB_OK )
               {
                  PD_LOG ( PDERROR, "Failed to add sub-context(rc=%d)",
                           rc );
               }
            }
            else
            {
               rc = SDB_SYS;
               PD_LOG( PDERROR, "node return invalid contextID(%lld)",
                       pReply->contextID ) ;
            }
         }
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR,
                  "failed to process reply"
                  "(groupID=%u, nodeID=%u, serviceID=%u, rc=%d)",
                  pReply->header.routeID.columns.groupID,
                  pReply->header.routeID.columns.nodeID,
                  pReply->header.routeID.columns.serviceID,
                  rc );
            failedNodes[ pReply->header.routeID.value ] = rc ;
         }
         SDB_OSS_FREE( pReply );
      }

      return SDB_OK;
   }

   INT32 rtnCoordCMDSnapshotIntrBase::buildFailedNodeReply( ROUTE_RC_MAP &failedNodes,
                                                            rtnContext *pContext )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT( pContext != NULL, "pContext can't be NULL!" ) ;

      CoordCB *pCoordcb = pmdGetKRCB()->getCoordCB();
      ROUTE_RC_MAP::iterator iter = failedNodes.begin();
      while ( iter != failedNodes.end() )
      {
         try
         {
            MsgRouteID routeID;
            routeID.value = iter->first;
            CoordGroupInfoPtr groupInfo;
            std::string strHostName;
            std::string strServiceName;
            std::string strNodeName;
            BSONObj errObj;
            rc = pCoordcb->getGroupInfo( routeID.columns.groupID,
                                       groupInfo );
            PD_CHECK( SDB_OK == rc, rc, save, PDERROR,
                     "failed to get group info(rc=%d)",
                        rc );
            routeID.columns.serviceID = MSG_ROUTE_LOCAL_SERVICE;
            rc = groupInfo->getGroupItem()->getNodeInfo( routeID, strHostName,
                                                         strServiceName );
            if ( rc != SDB_OK )
            {
               rc = SDB_CAT_NODE_NOT_FOUND;
            }
            PD_CHECK( SDB_OK == rc, rc, save, PDERROR,
                     "failed to get node info(rc=%d)",
                     rc );
   save:
            strNodeName = strHostName + ":" + strServiceName;
            errObj = BSON( FIELD_NAME_ERROR_NODES
                           << BSON( FIELD_NAME_NODE_NAME << strNodeName
                                    << FIELD_NAME_RCFLAG << iter->second ));
            rc = pContext->append( errObj );
            PD_RC_CHECK( rc, PDERROR,
                        "failed to append the error info" );
         }
         catch( std::exception &e )
         {
            PD_RC_CHECK( SDB_SYS, PDERROR,
                        "recieved unexpected error:%s",
                        e.what() );
         }
         ++iter;
      }
   done:
      return SDB_OK;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSDBINTR_BUILDREQMSG, "rtnCoordCMDSnapshotDBIntr::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotDBIntr::BuildRequestMsg  ( CHAR **ppBuffer,
                                                       INT32 *bufferSize,
                                                       SINT32 flag,
                                                       SINT64 numToSkip,
                                                       SINT64 numToReturn,
                                                       bson::BSONObj *query,
                                                       bson::BSONObj *fieldSelector,
                                                       bson::BSONObj *orderBy,
                                                       bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSDBINTR_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTDATABASE,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSDBINTR_BUILDREQMSG, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSSYSINTR_BUILDREQMSG, "rtnCoordCMDSnapshotSysIntr::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotSysIntr::BuildRequestMsg  ( CHAR **ppBuffer,
                                                        INT32 *bufferSize,
                                                        SINT32 flag,
                                                        SINT64 numToSkip,
                                                        SINT64 numToReturn,
                                                        bson::BSONObj *query,
                                                        bson::BSONObj *fieldSelector,
                                                        bson::BSONObj *orderBy,
                                                        bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSSYSINTR_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTSYSTEM,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSSYSINTR_BUILDREQMSG, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSCLINTR_BUILDREQMSG, "rtnCoordCMDSnapshotClIntr::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotClIntr::BuildRequestMsg  ( CHAR **ppBuffer,
                                                       INT32 *bufferSize,
                                                       SINT32 flag,
                                                       SINT64 numToSkip,
                                                       SINT64 numToReturn,
                                                       bson::BSONObj *query,
                                                       bson::BSONObj *fieldSelector,
                                                       bson::BSONObj *orderBy,
                                                       bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSCLINTR_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTCOLLECTIONS,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSCLINTR_BUILDREQMSG, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSCSINTR_BUILDREQMSG, "rtnCoordCMDSnapshotCsIntr::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotCsIntr::BuildRequestMsg  ( CHAR **ppBuffer,
                                                       INT32 *bufferSize,
                                                       SINT32 flag,
                                                       SINT64 numToSkip,
                                                       SINT64 numToReturn,
                                                       bson::BSONObj *query,
                                                       bson::BSONObj *fieldSelector,
                                                       bson::BSONObj *orderBy,
                                                       bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSCSINTR_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTCOLLECTIONSPACES,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSCSINTR_BUILDREQMSG, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSCTXINTR_BUILDREQMSG, "rtnCoordCMDSnapshotCtxIntr::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotCtxIntr::BuildRequestMsg  ( CHAR **ppBuffer,
                                                        INT32 *bufferSize,
                                                        SINT32 flag,
                                                        SINT64 numToSkip,
                                                        SINT64 numToReturn,
                                                        bson::BSONObj *query,
                                                        bson::BSONObj *fieldSelector,
                                                        bson::BSONObj *orderBy,
                                                        bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSCTXINTR_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTCONTEXTS,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSCTXINTR_BUILDREQMSG, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSCTXCURINTR_BUILDREQMSG, "rtnCoordCMDSnapshotCtxCurIntr::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotCtxCurIntr::BuildRequestMsg  ( CHAR **ppBuffer,
                                                           INT32 *bufferSize,
                                                           SINT32 flag,
                                                           SINT64 numToSkip,
                                                           SINT64 numToReturn,
                                                           bson::BSONObj *query,
                                                           bson::BSONObj *fieldSelector,
                                                           bson::BSONObj *orderBy,
                                                           bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSCTXCURINTR_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTCONTEXTSCUR,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSCTXCURINTR_BUILDREQMSG, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSSESSIONINTR_BUILDREQMSG, "rtnCoordCMDSnapshotSessionIntr::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotSessionIntr::BuildRequestMsg  ( CHAR **ppBuffer,
                                                            INT32 *bufferSize,
                                                            SINT32 flag,
                                                            SINT64 numToSkip,
                                                            SINT64 numToReturn,
                                                            bson::BSONObj *query,
                                                            bson::BSONObj *fieldSelector,
                                                            bson::BSONObj *orderBy,
                                                            bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSSESSIONINTR_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTSESSIONS,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSSESSIONINTR_BUILDREQMSG, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSSESSIONCURINTR_BUILDREQMSG, "rtnCoordCMDSnapshotSessionCurIntr::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotSessionCurIntr::BuildRequestMsg  ( CHAR **ppBuffer,
                                                               INT32 *bufferSize,
                                                               SINT32 flag,
                                                               SINT64 numToSkip,
                                                               SINT64 numToReturn,
                                                               bson::BSONObj *query,
                                                               bson::BSONObj *fieldSelector,
                                                               bson::BSONObj *orderBy,
                                                               bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSSESSIONCURINTR_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTSESSIONSCUR,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint );
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSSESSIONCURINTR_BUILDREQMSG, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSRESET_BUILDREQMSG, "rtnCoordCMDSnapshotReset::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotReset::BuildRequestMsg  ( CHAR **ppBuffer,
                                                      INT32 *bufferSize,
                                                      SINT32 flag,
                                                      SINT64 numToSkip,
                                                      SINT64 numToReturn,
                                                      bson::BSONObj *query,
                                                      bson::BSONObj *fieldSelector,
                                                      bson::BSONObj *orderBy,
                                                      bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSRESET_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTRESET,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint );
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSRESET_BUILDREQMSG, rc ) ;
      return rc ;
   }
   INT32 rtnCoordCMDSnapshotDataBaseTmp::BuildRequestMsg  ( CHAR **ppBuffer,
                                                            INT32 *bufferSize,
                                                            SINT32 flag,
                                                            SINT64 numToSkip,
                                                            SINT64 numToReturn,
                                                            bson::BSONObj *query,
                                                            bson::BSONObj *fieldSelector,
                                                            bson::BSONObj *orderBy,
                                                            bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTDATABASE,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      return rc ;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSSYS_BUILDREQMSG, "rtnCoordCMDSnapshotSystemTmp::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotSystemTmp::BuildRequestMsg  ( CHAR **ppBuffer,
                                                          INT32 *bufferSize,
                                                          SINT32 flag,
                                                          SINT64 numToSkip,
                                                          SINT64 numToReturn,
                                                          bson::BSONObj *query,
                                                          bson::BSONObj *fieldSelector,
                                                          bson::BSONObj *orderBy,
                                                          bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSSYS_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTSYSTEM,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSSYS_BUILDREQMSG, rc ) ;
      return rc ;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSSE_BUILDREQMSG, "rtnCoordCMDSnapshotSessions::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotSessionsTmp::BuildRequestMsg  ( CHAR **ppBuffer,
                                                            INT32 *bufferSize,
                                                            SINT32 flag,
                                                            SINT64 numToSkip,
                                                            SINT64 numToReturn,
                                                            bson::BSONObj *query,
                                                            bson::BSONObj *fieldSelector,
                                                            bson::BSONObj *orderBy,
                                                            bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSSE_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTSESSIONS,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSSE_BUILDREQMSG, rc ) ;
      return rc ;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSCONT_BUILDREQMSG, "rtnCoordCMDSnapshotContexts::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotContextsTmp::BuildRequestMsg  ( CHAR **ppBuffer,
                                                            INT32 *bufferSize,
                                                            SINT32 flag,
                                                            SINT64 numToSkip,
                                                            SINT64 numToReturn,
                                                            bson::BSONObj *query,
                                                            bson::BSONObj *fieldSelector,
                                                            bson::BSONObj *orderBy,
                                                            bson::BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSCONT_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTCONTEXTS,
                             flag, 0, numToSkip, numToReturn, query,
                             fieldSelector, orderBy, hint );
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSCONT_BUILDREQMSG, rc ) ;
      return rc ;
   }
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSCLS_EXE, "rtnCoordCMDSnapshotCollections::execute" )
   INT32 rtnCoordCMDSnapshotCollectionsTmp::execute( CHAR *pReceiveBuffer,
                                                     SINT32 packSize,
                                                     CHAR **ppResultBuffer,
                                                     pmdEDUCB *cb,
                                                     MsgOpReply &replyHeader,
                                                     BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSCLS_EXE ) ;
      rc = queryOnCatalog ( pReceiveBuffer, packSize, ppResultBuffer, cb,
                            replyHeader, MSG_CAT_QUERY_COLLECTIONS_REQ ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSCLS_EXE, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSSCSS_EXE, "rtnCoordCMDSnapshotCollectionSpaces::execute" )
   INT32 rtnCoordCMDSnapshotCollectionSpacesTmp::execute( CHAR *pReceiveBuffer,
                                                          SINT32 packSize,
                                                          CHAR **ppResultBuffer,
                                                          pmdEDUCB *cb,
                                                          MsgOpReply &replyHeader,
                                                          BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSCSS_EXE ) ;
      rc = queryOnCatalog ( pReceiveBuffer, packSize, ppResultBuffer, cb,
                            replyHeader, MSG_CAT_QUERY_COLLECTIONSPACES_REQ ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSCSS_EXE, rc ) ;
      return rc ;
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCOCMDSSRESETTMP_BUILDREQMSG, "rtnCoordCMDSnapshotResetTmp::BuildRequestMsg" )
   INT32 rtnCoordCMDSnapshotResetTmp::BuildRequestMsg  ( CHAR **ppBuffer,
                                                         INT32 *bufferSize,
                                                         SINT32 flag,
                                                         SINT64 numToSkip,
                                                         SINT64 numToReturn,
                                                         BSONObj *query,
                                                         BSONObj *fieldSelector,
                                                         BSONObj *orderBy,
                                                         BSONObj *hint )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSSRESETTMP_BUILDREQMSG ) ;
      rc = msgBuildQueryMsg( ppBuffer, bufferSize, COORD_CMD_SNAPSHOTRESET,
                             flag, 0, numToSkip, numToReturn, query, fieldSelector,
                             orderBy, hint );
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSSRESETTMP_BUILDREQMSG, rc ) ;
      return rc ;
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCOCMD2PC_EXE, "rtnCoordCMD2PhaseCommit::execute" )
   INT32 rtnCoordCMD2PhaseCommit::execute( CHAR *pReceiveBuffer,
                                           SINT32 packSize,
                                           CHAR **ppResultBuffer,
                                           pmdEDUCB * cb,
                                           MsgOpReply &replyHeader,
                                           BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMD2PC_EXE ) ;
      SINT64 contextID = -1;
      pmdKRCB *pKrcb = pmdGetKRCB();
      _SDB_RTNCB *pRtncb = pKrcb->getRTNCB();
      std::set<INT32> ignoreRCList;
      BOOLEAN hasRetry = FALSE;

      getIgnoreRCList( ignoreRCList );

   retry:
      rc = doP1OnDataGroup( pReceiveBuffer, cb, contextID, ignoreRCList,
                           hasRetry );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to execute phase1 on data group(rc=%d)",
                  rc );

      rc = doOnCataGroup( pReceiveBuffer, cb );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to execute on cata group(rc=%d)",
                  rc );

      rc = doP2OnDataGroup( pReceiveBuffer, cb, contextID );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to execute phase2 on data group(rc=%d)",
                  rc );

      rc = complete( pReceiveBuffer, cb );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to complete the operation(rc=%d)",
                  rc );
   done:
      if ( -1 != contextID )
      {
         pRtncb->contextDelete ( contextID, cb ) ;
         contextID = -1;
      }
      fillReply( (MsgHeader *)pReceiveBuffer, rc, ppErrorObj,
                  replyHeader );
      PD_TRACE_EXITRC ( SDB_RTNCOCMD2PC_EXE, rc ) ;
      return rc;
   error:
      if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == rc && !hasRetry )
      {
         if ( -1 != contextID )
         {
            pRtncb->contextDelete ( contextID, cb ) ;
            contextID = -1;
         }
         hasRetry = TRUE;
         goto retry;
      }
      goto done;
   }

   void rtnCoordCMD2PhaseCommit::getIgnoreRCList( std::set<INT32> &ignoreRCList )
   {
   }

   INT32 rtnCoordCMD2PhaseCommit::complete( CHAR *pReceiveBuffer,
                                          pmdEDUCB * cb )
   {
      return SDB_OK;
   }

   void rtnCoordCMD2PhaseCommit::fillReply( MsgHeader *pSrcMsg,
                                            INT32 rc, BSONObj **ppErrorObj,
                                            MsgOpReply &replyHeader )
   {
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pSrcMsg->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pSrcMsg->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = rc;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCOCMD2PC_DOP1, "rtnCoordCMD2PhaseCommit::doP1OnDataGroup" )
   INT32 rtnCoordCMD2PhaseCommit::doP1OnDataGroup( CHAR *pReceiveBuffer,
                                                   pmdEDUCB * cb,
                                                   SINT64 &contextID,
                                                   std::set<INT32> &ignoreRCList,
                                                   BOOLEAN isNeedRefresh )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMD2PC_DOP1 ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      _SDB_RTNCB *pRtncb               = pKrcb->getRTNCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      CoordGroupList sendGroupLst;
      BOOLEAN hasRefresh = FALSE;
      contextID = -1;
      BSONObj boEmpty;
      rtnContextCoord *pContext = NULL;
      rtnCoordQuery queryHandler;
      rc = pRtncb->contextNew( RTN_CONTEXT_COORD, (rtnContext **)&pContext,
                               contextID, cb );
      PD_RC_CHECK( rc, PDERROR, "failed to  create context(rc=%d)", rc );
      rc = pContext->open( boEmpty, boEmpty, -1, 0 );
      PD_RC_CHECK( rc, PDERROR, "open context failed(rc=%d)", rc ) ;
      do
      {
         CoordGroupList groupLst;
         hasRefresh = isNeedRefresh;
         rc = getGroupList( pReceiveBuffer, groupLst, sendGroupLst,
                            cb, isNeedRefresh );
         PD_RC_CHECK( rc, PDERROR, "Failed to get group-list(rc=%d)", rc );
         rc = queryHandler.queryToDataNodeGroup( pReceiveBuffer, groupLst,
                                                 sendGroupLst, pRouteAgent,
                                                 cb, pContext, TRUE,
                                                 &ignoreRCList );
         if ( rc != SDB_OK )
         {
            if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == rc
               && !hasRefresh )
            {
               rc = SDB_OK;
               isNeedRefresh = TRUE;
               continue;
            }
         }
         isNeedRefresh = FALSE;
      }while( isNeedRefresh );
      PD_RC_CHECK( rc, PDERROR,
                   "Failed to execute phase-1 on data node(rc=%d)",
                   rc );
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOCMD2PC_DOP1, rc ) ;
      return rc;
   error:
      if ( contextID >= 0 )
      {
         pRtncb->contextDelete( contextID, cb );
         contextID = -1;
         pContext = NULL;
      }
      goto done;
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCOCMD2PC_DOP2, "rtnCoordCMD2PhaseCommit::doP2OnDataGroup" )
   INT32 rtnCoordCMD2PhaseCommit::doP2OnDataGroup( CHAR *pReceiveBuffer,
                                                   pmdEDUCB * cb,
                                                   SINT64 &contextID )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMD2PC_DOP2 ) ;
      pmdKRCB *pKrcb = pmdGetKRCB();
      _SDB_RTNCB *pRtncb = pKrcb->getRTNCB();
      rtnContextBuf buffObj;
      rc = rtnGetMore( contextID, -1, buffObj, cb, pRtncb ) ;
      if ( SDB_DMS_EOC == rc )
      {
         contextID = -1;
         rc = SDB_OK;
      }
      PD_RC_CHECK( rc, PDERROR,
                   "Failed to execute phase-2 on data node(rc=%d)",
                   rc );
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOCMD2PC_DOP2, rc ) ;
      return rc;
   error:
      if ( -1 != contextID )
      {
         pRtncb->contextDelete ( contextID, cb ) ;
         contextID = -1;
      }
      goto done;
   }

   void rtnCoordCMDDropCollection::getIgnoreRCList( std::set<INT32> &ignoreRCList )
   {
      ignoreRCList.insert( SDB_DMS_NOTEXIST );
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCODROPCL_GETCLNAME, "rtnCoordCMDDropCollection::getCLName" )
   INT32 rtnCoordCMDDropCollection::getCLName( CHAR *pReceiveBuffer,
                                             std::string &strCLName )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCODROPCL_GETCLNAME ) ;
      INT32 flag                       = 0;
      CHAR *pCommandName               = NULL;
      SINT64 numToSkip                 = 0;
      SINT64 numToReturn               = 0;
      CHAR *pQuery                     = NULL;
      CHAR *pFieldSelector             = NULL;
      CHAR *pOrderBy                   = NULL;
      CHAR *pHint                      = NULL;
      BSONObj boQuery;
      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCommandName,
                            &numToSkip, &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to parse the request(rc=%d)",
                  rc );
      try
      {
         boQuery = BSONObj( pQuery );
         BSONElement beCLName = boQuery.getField( CAT_COLLECTION_NAME );
         PD_CHECK( beCLName.type() == String, SDB_INVALIDARG, error, PDERROR,
                  "failed to get collection name" );
         strCLName = beCLName.str();
      }
      catch( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR,
                  "failed to drop collection, occured unexpected error:%s",
                  e.what() );
         goto error;
      }
   done:
      PD_TRACE_EXITRC ( SDB_RTNCODROPCL_GETCLNAME, rc ) ;
      return rc;
   error:
      goto done;
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCODROPCL_GETGPLST, "rtnCoordCMDDropCollection::getGroupList" )
   INT32 rtnCoordCMDDropCollection::getGroupList( CHAR *pReceiveBuffer,
                                                CoordGroupList &groupLst,
                                                CoordGroupList &sendGroupLst,
                                                pmdEDUCB * cb,
                                                BOOLEAN isNeedRefresh )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCODROPCL_GETGPLST ) ;
      std::string strCLName;
      CoordCataInfoPtr cataInfo;
      BOOLEAN hasRetry = FALSE;
      MsgOpQuery *pDropReq = (MsgOpQuery *)pReceiveBuffer ;

      rc = getCLName( pReceiveBuffer, strCLName );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get collection name(rc=%d)", rc );
   retry:
      hasRetry = isNeedRefresh;
      rc = rtnCoordGetCataInfo( cb, strCLName.c_str(), isNeedRefresh, cataInfo );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get catalog(name:%s, rc=%d)",
                  strCLName.c_str(), rc );

      rc = rtnCoordGetGroupsByCataInfo( cataInfo, sendGroupLst, groupLst );
      if ( rc != SDB_OK )
      {
         if ( !hasRetry && SDB_CAT_NO_MATCH_CATALOG == rc )
         {
            rc = SDB_OK;
            isNeedRefresh = TRUE;
            goto retry;
         }
      }
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get group list(rc=%d)",
                  rc );
      pDropReq->version = cataInfo->getVersion();
   done:
      PD_TRACE_EXITRC ( SDB_RTNCODROPCL_GETGPLST, rc ) ;
      return rc;
   error:
      goto done;
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCODROPCL_CMPL, "rtnCoordCMDDropCollection::complete" )
   INT32 rtnCoordCMDDropCollection::complete( CHAR *pReceiveBuffer,
                                             pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCODROPCL_CMPL ) ;
      pmdKRCB *pKrcb = pmdGetKRCB();
      CoordCB *pCoordcb = pKrcb->getCoordCB();
      std::string strCLName;
      std::string strMainCLName;
      CoordCataInfoPtr cataInfo;
      rc = getCLName( pReceiveBuffer, strCLName );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get collection name(rc=%d)", rc );
      rc = rtnCoordGetCataInfo( cb, strCLName.c_str(), FALSE, cataInfo );
      PD_RC_CHECK( rc, PDWARNING,
                  "failed to get catalog, complete drop-CL failed(rc=%d)",
                  rc );
      strMainCLName = cataInfo->getCatalogSet()->getMainCLName();
      pCoordcb->delCataInfo( strCLName );
      if ( !strMainCLName.empty() )
      {
         pCoordcb->delCataInfo( strMainCLName );
      }
   done:
      PD_TRACE_EXITRC ( SDB_RTNCODROPCL_CMPL, rc ) ;
      return SDB_OK;
   error:
      goto done;
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCODROPCL_DOONCATA, "rtnCoordCMDDropCollection::doOnCataGroup" )
   INT32 rtnCoordCMDDropCollection::doOnCataGroup( CHAR *pReceiveBuffer,
                                                   pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCODROPCL_DOONCATA ) ;
      SINT32 opCode;
      UINT32 TID;
      MsgOpQuery *pDropReq             = (MsgOpQuery *)pReceiveBuffer ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      opCode = pDropReq->header.opCode;
      TID = pDropReq->header.TID;

      pDropReq->header.opCode = MSG_CAT_DROP_COLLECTION_REQ;
      pDropReq->header.routeID.value = 0;
      pDropReq->header.TID = cb->getTID();

      rc = executeOnCataGroup( pReceiveBuffer, pRouteAgent, cb );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to drop the catalog of cl(rc=%d)",
                  rc );

   done:
      pDropReq->header.opCode = opCode;
      pDropReq->header.TID = TID;
      PD_TRACE_EXITRC ( SDB_RTNCODROPCL_DOONCATA, rc ) ;
      return rc;
   error:
      goto done;
   }

   void rtnCoordCMDDropCollectionSpace::getIgnoreRCList( std::set<INT32> &ignoreRCList )
   {
      ignoreRCList.insert( SDB_DMS_CS_NOTEXIST );
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCODROPCS_GETGPLST, "rtnCoordCMDDropCollectionSpace::getGroupList" )
   INT32 rtnCoordCMDDropCollectionSpace::getGroupList( CHAR *pReceiveBuffer,
                                                       CoordGroupList &groupLst,
                                                       CoordGroupList &sendGroupLst,
                                                       pmdEDUCB * cb,
                                                       BOOLEAN isNeedRefresh )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCODROPCS_GETGPLST ) ;

      INT32 flag                       = 0;
      CHAR *pCommandName               = NULL;
      SINT64 numToSkip                 = 0;
      SINT64 numToReturn               = 0;
      CHAR *pQuery                     = NULL;
      CHAR *pFieldSelector             = NULL;
      CHAR *pOrderBy                   = NULL;
      CHAR *pHint                      = NULL;
      BSONObj boQuery;
      BSONObj boEmpty;
      CHAR *pBuffer                    = NULL;
      INT32 bufferSize                 = 0;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      CoordGroupList::const_iterator iter;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCommandName,
                            &numToSkip, &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint );
      PD_RC_CHECK( rc, PDERROR, "Failed to parse the request(rc=%d)",
                   rc ) ;

      try
      {
         boQuery = BSONObj( pQuery );
         BSONElement beCSName
            = boQuery.getField( CAT_COLLECTION_SPACE_NAME );
         PD_CHECK( beCSName.type() == String, SDB_INVALIDARG,
                   error, PDERROR, "failed to get cs name" );
      }
      catch( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG( PDERROR, "Failed to drop cs, received unexpected error:%s",
                 e.what() );
         goto error ;
      }

      rc = msgBuildQuerySpaceReqMsg( &pBuffer, &bufferSize, 0, 0, 0, -1,
                                     cb->getTID(), &boQuery, &boEmpty,
                                     &boEmpty, &boEmpty );
      PD_RC_CHECK( rc, PDERROR, "Failed to build query request(rc=%d)",
                   rc );
      rc = executeOnCataGroup( pBuffer, pRouteAgent, cb, NULL, &groupLst );
      PD_RC_CHECK( rc, PDERROR, "Failed to get cs info from catalog(rc=%d)",
                   rc );

      iter = sendGroupLst.begin();
      while( iter != sendGroupLst.end() )
      {
         groupLst.erase( iter->first );
         ++iter;
      }
   done:
      SAFE_OSS_FREE( pBuffer );
      PD_TRACE_EXITRC ( SDB_RTNCODROPCS_GETGPLST, rc ) ;
      return rc;
   error:
      goto done;
   }

   //PD_TRACE_DECLARE_FUNCTION (SDB_RTNCODROPCS_DOONCATA, "rtnCoordCMDDropCollectionSpace::doOnCataGroup" )
   INT32 rtnCoordCMDDropCollectionSpace::doOnCataGroup( CHAR *pReceiveBuffer,
                                                        pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCODROPCS_DOONCATA ) ;
      SINT32 opCode;
      UINT32 TID;
      MsgOpQuery *pDropReq             = (MsgOpQuery *)pReceiveBuffer ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      opCode = pDropReq->header.opCode;
      TID = pDropReq->header.TID;

      pDropReq->header.opCode = MSG_CAT_DROP_SPACE_REQ;
      pDropReq->header.routeID.value = 0;
      pDropReq->header.TID = cb->getTID();

      rc = executeOnCataGroup( pReceiveBuffer, pRouteAgent, cb );
      PD_RC_CHECK( rc, PDERROR, "Failed to drop the catalog of cs, rc: %d",
                   rc ) ;

   done:
      pDropReq->header.opCode = opCode ;
      pDropReq->header.TID = TID ;
      PD_TRACE_EXITRC ( SDB_RTNCODROPCS_DOONCATA, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDQUBASE_QUTOCANOGR, "rtnCoordCMDQueryBase::queryToCataNodeGroup" )
   INT32 rtnCoordCMDQueryBase::queryToCataNodeGroup( CHAR *pBuffer,
                                               netMultiRouteAgent *pRouteAgent,
                                               pmdEDUCB *cb,
                                               rtnContextCoord *pContext )
   {
      INT32 rc = SDB_OK;
      BOOLEAN isNeedRefresh = FALSE;
      BOOLEAN takeOver = FALSE ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDQUBASE_QUTOCANOGR );

      do
      {
         CoordGroupInfoPtr catGroupInfo;
         rc = rtnCoordGetCatGroupInfo( cb, isNeedRefresh, catGroupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to get catalogue-group info(rc=%d)",
                     rc );
            break;
         }
         REQUESTID_MAP sendNodes;
         rc = rtnCoordSendRequestToPrimary( pBuffer, catGroupInfo,
                                            sendNodes, pRouteAgent,
                                            MSG_ROUTE_CAT_SERVICE,
                                            cb );
         if ( rc != SDB_OK )
         {
            rtnCoordClearRequest( cb, sendNodes );
            if ( SDB_RTN_NO_PRIMARY_FOUND == rc && FALSE == isNeedRefresh )
            {
               isNeedRefresh = TRUE;
               continue;
            }
            PD_LOG( PDERROR, "Failed to send the query request to "
                    "catalogue-group(rc=%d)", rc );
            break;
         }

         REPLY_QUE replyQue;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                                MSG_BS_QUERY_RES );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to get reply from catalogue-node(rc=%d)",
                     rc );
            break;
         }
         while ( !replyQue.empty() )
         {
            takeOver             = FALSE ;
            MsgOpReply *pReply   = NULL;
            pReply = (MsgOpReply *)(replyQue.front());
            replyQue.pop();
            if ( SDB_OK == rc )
            {
               if ( SDB_OK == pReply->flags )
               {
                  rc = pContext->addSubContext( pReply, takeOver );
               }
               else
               {
                  rc = pReply->flags ;
               }
            }
            if ( !takeOver )
            {
               SDB_OSS_FREE( pReply ) ;
            }
         }
         if ( rc != SDB_OK && rc != SDB_DMS_EOC )
         {
            PD_LOG ( PDERROR, "Error occured while process catalogue-node "
                     "reply(rc=%d)", rc );
            if ( SDB_CLS_NOT_PRIMARY == rc && FALSE == isNeedRefresh )
            {
               isNeedRefresh = TRUE;
               continue;
            }
         }
         else
         {
            pContext->addSubDone( cb ) ;
         }
         break;
      }while ( TRUE );

      PD_TRACE_EXITRC ( SDB_RTNCOCMDQUBASE_QUTOCANOGR, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDQUBASE_EXE, "rtnCoordCMDQueryBase::execute" )
   INT32 rtnCoordCMDQueryBase::execute( CHAR *pReceiveBuffer,
                                        SINT32 packSize,
                                        CHAR **ppResultBuffer,
                                        pmdEDUCB *cb,
                                        MsgOpReply &replyHeader,
                                        BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDQUBASE_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      SDB_RTNCB *pRtncb                = pKrcb->getRTNCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      SINT64 contextID                 = -1;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      do
      {
         MsgOpQuery *pSrc = (MsgOpQuery *)pReceiveBuffer;
         rtnContextCoord *pContext = NULL ;
         rc = pRtncb->contextNew( RTN_CONTEXT_COORD, (rtnContext**)&pContext,
                                  contextID, cb );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "failed to allocate context(rc=%d)", rc );
            break;
         }
         rc = pContext->open( BSONObj(), BSONObj(), pSrc->numToReturn, pSrc->numToSkip ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR, "Open context failed, rc: %d", rc ) ;
            break ;
         }

         CHAR *pListReq = NULL;
         rc = buildQueryRequest( pReceiveBuffer, cb, &pListReq );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "build query request failed(rc=%d)",
                     rc );
            break;
         }
         rc = queryToCataNodeGroup( pListReq, pRouteAgent,
                                    cb, pContext );
         if ( NULL != pListReq )
         {
            SDB_OSS_FREE( pListReq );
         }
         if ( rc != SDB_OK && rc != SDB_DMS_EOC )
         {
            PD_LOG ( PDERROR, "Failed to query on catalogue-node(rc=%d)",
                     rc );
            break;
         }
      }while ( FALSE );

      replyHeader.flags = rc;
      if ( rc != SDB_OK )
      {
         if ( contextID >= 0 )
         {
            pRtncb->contextDelete( contextID, cb );
         }
      }
      else
      {
         replyHeader.contextID = contextID;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOCMDQUBASE_EXE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDLISTCS_BQREQ, "rtnCoordCMDListCollectionSpace::buildQueryRequest" )
   INT32 rtnCoordCMDListCollectionSpace::buildQueryRequest( CHAR *pIntput,
                                                            pmdEDUCB *cb,
                                                            CHAR **ppOutput )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDLISTCS_BQREQ ) ;
      do
      {
         INT32 flag = 0;
         CHAR *pCollectionName = NULL;
         SINT64 numToSkip = 0;
         SINT64 numToReturn = 0;
         CHAR *pQuery = NULL;
         CHAR *pFieldSelector = NULL;
         CHAR *pOrderBy = NULL;
         CHAR *pHint = NULL;
         rc = msgExtractQuery( pIntput, &flag, &pCollectionName,
                               &numToSkip, &numToReturn, &pQuery, 
                               &pFieldSelector, &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "failed to parse query request(rc=%d)",
                     rc );
            break;
         }
         INT32 bufferSize = 0;
         BSONObj query;
         BSONObj fieldSelector;
         BSONObj orderBy;
         BSONObj hint;
         try
         {
            query = BSONObj ( pQuery );
            orderBy = BSONObj ( pOrderBy );
            hint = BSONObj ( pHint );
            BSONObjBuilder bobFieldSelector;
            bobFieldSelector.appendNull( CAT_COLLECTION_SPACE_NAME );
            fieldSelector = bobFieldSelector.obj();
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "occured unexpected error:%s",
                     e.what() );
            break;
         }
         rc = msgBuildQueryMsg( ppOutput, &bufferSize,
                                CAT_COLLECTION_SPACE_COLLECTION,
                                flag, 0, numToSkip, numToReturn,
                                &query, &fieldSelector,
                                &orderBy, &hint ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to build the query message(rc=%d)",
                     rc );
            break;
         }
         MsgOpQuery *pQueryMsg = (MsgOpQuery *)(*ppOutput);
         pQueryMsg->header.routeID.value = 0;
         pQueryMsg->header.TID = cb->getTID();
      }while( FALSE );
      PD_TRACE_EXITRC ( SDB_RTNCOCMDLISTCS_BQREQ, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDLISTCL_BQREQ, "rtnCoordCMDListCollection::buildQueryRequest" )
   INT32 rtnCoordCMDListCollection::buildQueryRequest( CHAR *pIntput,
                                                       pmdEDUCB *cb,
                                                       CHAR **ppOutput )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDLISTCL_BQREQ ) ;
      do
      {
         INT32 flag = 0;
         CHAR *pCollectionName = NULL;
         SINT64 numToSkip = 0;
         SINT64 numToReturn = 0;
         CHAR *pQuery = NULL;
         CHAR *pFieldSelector = NULL;
         CHAR *pOrderBy = NULL;
         CHAR *pHint = NULL;
         rc = msgExtractQuery( pIntput, &flag, &pCollectionName,
                               &numToSkip, &numToReturn, &pQuery, 
                               &pFieldSelector, &pOrderBy, &pHint ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to parse query request(rc=%d)",
                     rc );
            break;
         }
         INT32 bufferSize = 0;
         BSONObj query;
         BSONObj fieldSelector;
         BSONObj orderBy;
         BSONObj hint;
         try
         {
            query = BSONObj ( pQuery );
            orderBy = BSONObj ( pOrderBy );
            hint = BSONObj ( pHint );
            BSONObjBuilder bobFieldSelector;
            bobFieldSelector.appendNull( CAT_COLLECTION_NAME );
            fieldSelector = bobFieldSelector.obj();
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR,
                     "occured unexpected error:%s",
                     e.what() );
            break;
         }
         rc = msgBuildQueryMsg( ppOutput, &bufferSize,
                                CAT_COLLECTION_INFO_COLLECTION,
                                flag, 0, numToSkip, numToReturn,
                                &query, &fieldSelector,
                                &orderBy, &hint ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to build the query message(rc=%d)",
                     rc ) ;
            break;
         }
         MsgOpQuery *pQueryMsg = (MsgOpQuery *)(*ppOutput);
         pQueryMsg->header.routeID.value = 0;
         pQueryMsg->header.TID = cb->getTID();
      }while( FALSE );
      PD_TRACE_EXITRC ( SDB_RTNCOCMDLISTCL_BQREQ, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDTESTCS_EXE, "rtnCoordCMDTestCollectionSpace::execute" )
   INT32 rtnCoordCMDTestCollectionSpace::execute( CHAR *pReceiveBuffer,
                                                  SINT32 packSize,
                                                  CHAR **ppResultBuffer,
                                                  pmdEDUCB *cb,
                                                  MsgOpReply &replyHeader,
                                                  BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDTESTCS_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      SDB_RTNCB *pRtncb                = pKrcb->getRTNCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      replyHeader.contextID = -1 ;

      do
      {
         rtnCoordProcesserFactory *pProcesserFactory
               = pCoordcb->getProcesserFactory();
         rtnCoordOperator *pCmdProcesser = NULL ;
         pCmdProcesser = pProcesserFactory->getCommandProcesser(
            COORD_CMD_LISTCOLLECTIONSPACES ) ;
         SDB_ASSERT( pCmdProcesser , "pCmdProcesser can't be NULL" ) ;
         rc = pCmdProcesser->execute( pReceiveBuffer, packSize, ppResultBuffer,
                                      cb, replyHeader, ppErrorObj ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to list collectionspaces(rc=%d)", rc ) ;
            break;
         }

         rtnContextBuf buffObj ;
         rc = rtnGetMore( replyHeader.contextID, -1, buffObj, cb, pRtncb ) ;

         if ( rc )
         {
            replyHeader.contextID = -1 ;
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_DMS_CS_NOTEXIST ;
            }
            else
            {
               PD_LOG ( PDERROR, "getmore failed(rc=%d)", rc ) ;
            }
         }
      }while ( FALSE ) ;

      if ( replyHeader.contextID >= 0 )
      {
         pRtncb->contextDelete( replyHeader.contextID, cb ) ;
         replyHeader.contextID = -1 ;
      }
      replyHeader.flags = rc ;
      replyHeader.numReturned = 0 ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDTESTCS_EXE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDTESTCL_EXE, "rtnCoordCMDTestCollection::execute" )
   INT32 rtnCoordCMDTestCollection::execute( CHAR *pReceiveBuffer,
                                             SINT32 packSize,
                                             CHAR **ppResultBuffer,
                                             pmdEDUCB *cb,
                                             MsgOpReply &replyHeader,
                                             BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDTESTCL_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB() ;
      SDB_RTNCB *pRtncb                = pKrcb->getRTNCB() ;
      CoordCB *pCoordcb                = pKrcb->getCoordCB() ;

      do
      {
         rtnCoordProcesserFactory *pProcesserFactory
               = pCoordcb->getProcesserFactory() ;
         rtnCoordOperator *pCmdProcesser = NULL ;
         pCmdProcesser = pProcesserFactory->getCommandProcesser(
            COORD_CMD_LISTCOLLECTIONS ) ;
         SDB_ASSERT( pCmdProcesser , "pCmdProcesser can't be NULL" ) ;
         rc = pCmdProcesser->execute( pReceiveBuffer, packSize, ppResultBuffer,
                                      cb, replyHeader, ppErrorObj ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to list collections(rc=%d)", rc ) ;
            break;
         }

         rtnContextBuf buffObj ;
         rc = rtnGetMore( replyHeader.contextID, -1, buffObj, cb, pRtncb ) ;
         if ( rc )
         {
            replyHeader.contextID = -1 ;
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_DMS_NOTEXIST;
            }
            else
            {
               PD_LOG ( PDERROR, "Getmore failed(rc=%d)", rc ) ;
            }
         }
      }while ( FALSE );

      if ( replyHeader.contextID >= 0 )
      {
         pRtncb->contextDelete( replyHeader.contextID, cb ) ;
         replyHeader.contextID = -1 ;
      }
      replyHeader.flags = rc ;
      replyHeader.numReturned = 0 ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDTESTCL_EXE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCTGR, "rtnCoordCMDCreateGroup::execute" )
   INT32 rtnCoordCMDCreateGroup::execute( CHAR *pReceiveBuffer,
                                                SINT32 packSize,
                                                CHAR **ppResultBuffer,
                                                pmdEDUCB *cb,
                                                MsgOpReply &replyHeader,
                                                BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCTGR ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *pCreateReq = (MsgOpQuery *)pReceiveBuffer;
      pCreateReq->header.routeID.value = 0;
      pCreateReq->header.TID = cb->getTID();
      pCreateReq->header.opCode = MSG_CAT_CREATE_GROUP_REQ;
      REPLY_QUE replyQue;

      rc = executeOnCataGroup ( (CHAR*)pCreateReq, pRouteAgent, cb ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to execute on catalog, rc = %d", rc ) ;
         goto error ;
      }
   done :
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCTGR, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDRMR, "rtnCoordCMDRemoveGroup::execute" )
   INT32 rtnCoordCMDRemoveGroup::execute( CHAR *pReceiveBuffer,
                                          SINT32 packSize,
                                          CHAR **ppResultBuffer,
                                          pmdEDUCB *cb,
                                          MsgOpReply &replyHeader,
                                          BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDRMR ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      const CHAR *groupName = NULL ;
      INT32 flag = -1 ;
      CHAR *pCommandName  = NULL;
      SINT64 numToSkip  = 0;
      SINT64 numToReturn  = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector  = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *forward = (MsgOpQuery *)pReceiveBuffer;
      forward->header.routeID.value = 0;
      forward->header.TID = cb->getTID();
      forward->header.opCode = MSG_CAT_RM_GROUP_REQ;
      CoordGroupInfoPtr group;
      REPLY_QUE replyQue;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCommandName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector,
                            &pOrderBy, &pHint );
      try
      {
         BSONObj obj( pQuery ) ;
         BSONElement ele = obj.getField( FIELD_NAME_GROUPNAME ) ;
         if ( ele.eoo() || String != ele.type() )
         {
            PD_LOG( PDERROR, "failed to get groupname from msg[%s]",
                    obj.toString().c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         groupName = ele.valuestr() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s",
                 e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = rtnCoordGetGroupInfo( cb, groupName, TRUE, group ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get group info by name:%d", rc ) ;
         goto error ;
      }

      rc = executeOnCataGroup ( (CHAR*)forward, pRouteAgent, cb ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to execute on catalog, rc = %d", rc ) ;
         goto error ;
      }

      if ( 0 == ossStrcmp( groupName, CATALOG_GROUPNAME ) )
      {
         sdbGetCoordCB()->getLock( EXCLUSIVE ) ;
         sdbGetCoordCB()->clearCatNodeAddrList() ;
         sdbGetCoordCB()->releaseLock( EXCLUSIVE ) ;

         CoordGroupInfo *pEmptyGroupInfo = NULL ;
         pEmptyGroupInfo = SDB_OSS_NEW CoordGroupInfo( CAT_CATALOG_GROUPID ) ;
         if ( NULL != pEmptyGroupInfo )
         {
            CoordGroupInfoPtr groupInfo( pEmptyGroupInfo ) ;
            sdbGetCoordCB()->updateCatGroupInfo( groupInfo ) ;
         }
      }

      {
      SINT32 ret = SDB_OK ;
      INT32 rrc = SDB_OK ;
      BSONObj execObj ;
      clsGroupItem *groupItem = group->getGroupItem() ;

      MsgRouteID routeID ;
      string hostName ;
      string serviceName ;
      UINT32 index = 0 ;

      while ( SDB_OK == groupItem->getNodeInfo( index++, routeID, hostName,
                                                serviceName,
                                                MSG_ROUTE_LOCAL_SERVICE ) )
      {
         execObj = BSON( FIELD_NAME_HOST << hostName
                         << PMD_OPTION_SVCNAME << serviceName ) ;
         rc = rtnRemoteExec ( SDBSTOP, hostName.c_str() ,
                              &ret, &execObj ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR,
                    "remote node execute(configure) failed(rc=%d) node(%s)",
                    rc, execObj.toString().c_str());
            rrc = SDB_CATA_FAILED_TO_CLEANUP ;
         }

         rc = rtnRemoteExec ( SDBRM, hostName.c_str(),
                              &ret, &execObj ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR,
                    "remote node execute(configure) failed(rc=%d) node(%s)",
                    rc, execObj.toString().c_str());
            rrc = SDB_CATA_FAILED_TO_CLEANUP ;
         }
      }

      rtnCoordRemoveGroup ( group->getGroupID() ) ;

      {
         CoordSession *session = cb->getCoordSession();
         if ( NULL != session )
         {
            session->removeLastNode( group->getGroupID()) ;
         }
      }

      rc = rrc ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      }
   done:
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDRMR, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCONFN_GETNCONF, "rtnCoordCMDConfigNode::getNodeConf" )
   INT32 rtnCoordCMDConfigNode::getNodeConf( char *pQuery,
                                             BSONObj &nodeConf,
                                             CoordGroupInfoPtr &catGroupInfo )
   {
      INT32 rc             = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCONFN_GETNCONF ) ;
      const CHAR *roleStr  = NULL ;
      SDB_ROLE role        = SDB_ROLE_DATA ;

      try
      {
         BSONObj boInput( pQuery ) ;
         BSONObjBuilder bobNodeConf ;
         BSONObjIterator iter( boInput ) ;
         BOOLEAN hasCatalogAddrKey = FALSE ;

         while ( iter.more() )
         {
            BSONElement beField = iter.next();
            std::string strFieldName(beField.fieldName());
            if ( strFieldName == FIELD_NAME_HOST ||
                 strFieldName == PMD_OPTION_ROLE )
            {
               continue;
            }
            if ( strFieldName == FIELD_NAME_GROUPNAME )
            {
               if ( 0 == ossStrcmp( CATALOG_GROUPNAME, beField.valuestr() ) )
               {
                  role = SDB_ROLE_CATALOG ;
               }
               else if ( 0 == ossStrcmp( COORD_GROUPNAME, beField.valuestr() ) )
               {
                  role = SDB_ROLE_COORD ;
               }
               continue ;
            }

            bobNodeConf.append( beField );

            if ( PMD_OPTION_CATALOG_ADDR == strFieldName )
            {
               hasCatalogAddrKey = TRUE ;
            }
         }
         roleStr = utilDBRoleStr( role ) ;
         if ( *roleStr == 0 )
         {
            goto error ;
         }
         bobNodeConf.append ( PMD_OPTION_ROLE, roleStr ) ;

         if ( !hasCatalogAddrKey )
         {
            MsgRouteID routeID ;
            clsGroupItem *groupItem = catGroupInfo->getGroupItem() ;
            std::string cataNodeLst = "";
            UINT32 i = 0;
            if ( catGroupInfo->getGroupSize() == 0 )
            {
               rc = SDB_CLS_EMPTY_GROUP ;
               PD_LOG ( PDERROR, "Get catalog group info failed(rc=%d)", rc ) ;
               goto error ;
            }

            routeID.value = MSG_INVALID_ROUTEID ;
            string host ;
            string service ;

            while ( SDB_OK == groupItem->getNodeInfo( i, routeID, host,
                                                      service,
                                                      MSG_ROUTE_CAT_SERVICE ) )
            {
               if ( i > 0 )
               {
                  cataNodeLst += "," ;
               }
               cataNodeLst += host + ":" + service ;
               ++i ;
            }
            bobNodeConf.append( PMD_OPTION_CATALOG_ADDR, cataNodeLst ) ;
         }
         nodeConf = bobNodeConf.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR, "Occured unexpected error:%s", e.what() );
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCONFN_GETNCONF, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCONFN_GETNINFO, "rtnCoordCMDConfigNode::getNodeInfo" )
   INT32 rtnCoordCMDConfigNode::getNodeInfo( char *pQuery,
                                             BSONObj &NodeInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCONFN_GETNINFO ) ;
      do
      {
         try
         {
            BSONObj boConfig( pQuery );
            BSONObjBuilder bobNodeInfo;
            BSONElement beGroupName = boConfig.getField( FIELD_NAME_GROUPNAME );
            if ( beGroupName.eoo() || beGroupName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to get the field(%s)",
                        FIELD_NAME_GROUPNAME );
               break;
            }
            bobNodeInfo.append( beGroupName );

            BSONElement beHostName = boConfig.getField( FIELD_NAME_HOST );
            if ( beHostName.eoo() || beGroupName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to get the field(%s)",
                        FIELD_NAME_HOST );
               break;
            }
            bobNodeInfo.append( beHostName );

            BSONElement beLocalSvc = boConfig.getField( PMD_OPTION_SVCNAME );
            if ( beLocalSvc.eoo() || beLocalSvc.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to get the field(%s)",
                        PMD_OPTION_SVCNAME );
               break;
            }
            bobNodeInfo.append( beLocalSvc );

            BSONElement beReplSvc = boConfig.getField( PMD_OPTION_REPLNAME );
            if ( !beReplSvc.eoo() && beReplSvc.type()==String )
            {
               bobNodeInfo.append( beReplSvc );
            }

            BSONElement beShardSvc = boConfig.getField( PMD_OPTION_SHARDNAME );
            if ( !beShardSvc.eoo() && beShardSvc.type()==String )
            {
               bobNodeInfo.append( beShardSvc );
            }
            BSONElement beCataSvc = boConfig.getField( PMD_OPTION_CATANAME );
            if ( !beCataSvc.eoo() && beCataSvc.type()==String )
            {
               bobNodeInfo.append( beCataSvc );
            }

            BSONElement beDBPath = boConfig.getField( PMD_OPTION_DBPATH );
            if ( beDBPath.eoo() || beDBPath.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to get the field(%s)",
                        PMD_OPTION_DBPATH );
               break;
            }
            bobNodeInfo.append( beDBPath );
            NodeInfo = bobNodeInfo.obj();
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "Occured unexpected error:%s", e.what() ) ;
            break;
         }
      }while ( FALSE );
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCONFN_GETNINFO, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCTN_EXE, "rtnCoordCMDCreateNode::execute" )
   INT32 rtnCoordCMDCreateNode::execute( CHAR *pReceiveBuffer,
                                         SINT32 packSize,
                                         CHAR **ppResultBuffer,
                                         pmdEDUCB *cb,
                                         MsgOpReply &replyHeader,
                                         BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCTN_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      do
      {
         INT32 flag = 0 ;
         CHAR *pCMDName = NULL ;
         SINT64 numToSkip = 0 ;
         SINT64 numToReturn = 0 ;
         CHAR *pQuery = NULL ;
         CHAR *pFieldSelector = NULL ;
         CHAR *pOrderBy = NULL ;
         CHAR *pHint = NULL ;
         rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip,
                               &numToReturn, &pQuery, &pFieldSelector,
                               &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "failed to parse create node request(rc=%d)",
                     rc );
            break;
         }
         BSONObj boNodeInfo;
         rc = getNodeInfo( pQuery, boNodeInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Create node failed, failed to get "
                     "node info(rc=%d)", rc ) ;
            break;
         }
         CoordGroupInfoPtr catGroupInfo ;
         rc = rtnCoordGetCatGroupInfo( cb, TRUE, catGroupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Create node failed, failed to get "
                     "cata-group-info(rc=%d)", rc ) ;
            break ;
         }
         CHAR *pBuffer = NULL;
         INT32 bufferSize = 0;
         BSONObj fieldSelector;
         BSONObj orderBy;
         BSONObj hint;
         BSONObjBuilder builder ;
         BSONObj newNodeInfo ;
         try
         {
            BSONObjIterator itrObj( boNodeInfo ) ;
            while ( itrObj.more() )
            {
               BSONElement nextEle = itrObj.next() ;
               if ( 0 == ossStrcmp( nextEle.fieldName(),
                                    CMD_NAME_ENFORCED ) )
               {
                  continue ;
               }
               builder.append( nextEle ) ;
            }

            builder.appendBool( CMD_NAME_ENFORCED, TRUE ) ;
            newNodeInfo = builder.obj() ;
         }
         catch ( std::exception &e )
         {
            PD_LOG( PDERROR, "unexcepted err happened:%s", e.what() ) ;
            rc = SDB_SYS ;
            break ;
         }

         MsgOpQuery *pCatReq = (MsgOpQuery *)pReceiveBuffer;
         pCatReq->header.routeID.value = 0;
         pCatReq->header.TID = cb->getTID();
         pCatReq->header.opCode = MSG_CAT_CREATE_NODE_REQ;
         rc = executeOnCataGroup( pReceiveBuffer, pRouteAgent, cb ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to create node, execute on catalog-node "
                     "failed(rc=%d)", rc ) ;
            break;
         }
         std::string strHostName ;
         BSONObj boNodeConfig ;
         try
         {
            BSONElement beHostName = boNodeInfo.getField( FIELD_NAME_HOST );
            if ( beHostName.eoo() || beHostName.type() != String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to get the field(%s)",
                        FIELD_NAME_HOST ) ;
               break;
            }
            strHostName = beHostName.str() ;
            rc = getNodeConf( pQuery, boNodeConfig, catGroupInfo );
            if ( rc != SDB_OK )
            {
               PD_LOG( PDERROR, "Failed to get node config(rc=%d)",
                       rc ) ;
               break ;
            }
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "Occured unexpected error:%s", e.what() ) ;
         }
         SINT32 retCode;
         rc = rtnRemoteExec ( SDBADD, strHostName.c_str(),
                              &retCode, &boNodeConfig ) ;
         rc = rc ? rc : retCode;
         if ( SDB_OK == rc )
         {
            if ( 0 == ossStrcmp( newNodeInfo.getField(
                                 FIELD_NAME_GROUPNAME ).valuestr(),
                                 CATALOG_GROUPNAME ) )
            {
               rtnCoordGetCatGroupInfo( cb, TRUE, catGroupInfo ) ;
               rtnCataChangeNtyToAllNodes( cb ) ;
            }
            break ;
         }

         PD_LOG( PDERROR, "Remote node execute(configure) failed(rc=%d)",
                 rc ) ;
         INT32 rcDel = SDB_OK ;
         rcDel = msgBuildQueryMsg( &pBuffer, &bufferSize, COORD_CMD_REMOVENODE,
                                   flag, 0, numToSkip, numToReturn,
                                   &newNodeInfo, &fieldSelector, &orderBy,
                                   &hint ) ;
         if ( rcDel != SDB_OK )
         {
            PD_LOG ( PDERROR, "failed to build the request for "
                     "catalog-node(rc=%d)", rcDel ) ;
            break ;
         }
         pCatReq = (MsgOpQuery *)pBuffer;
         pCatReq->header.routeID.value = 0;
         pCatReq->header.TID = cb->getTID();
         pCatReq->header.opCode = MSG_CAT_DEL_NODE_REQ;
         rcDel = executeOnCataGroup( pBuffer, pRouteAgent, cb ) ;
         SDB_OSS_FREE(pBuffer);
         if ( rcDel!= SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to delete node, execute on catalog-node "
                     "failed(rc=%d)", rcDel ) ;
            break ;
         }
         break ;
      }while ( FALSE ) ;

      replyHeader.flags = rc ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCTN_EXE, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDRMN_EXE, "rtnCoordCMDRemoveNode::execute" )
   INT32 rtnCoordCMDRemoveNode::execute( CHAR *pReceiveBuffer,
                                         SINT32 packSize,
                                         CHAR **ppResultBuffer,
                                         pmdEDUCB *cb,
                                         MsgOpReply &replyHeader,
                                         BSONObj **ppErrorObj )
   {
      PD_TRACE_ENTRY ( SDB_RTNCOCMDRMN_EXE ) ;
      INT32 rc = SDB_OK ;
      pmdKRCB *pKrcb = pmdGetKRCB() ;
      CoordCB *pCoordcb = pKrcb->getCoordCB() ;
      netMultiRouteAgent *pRouteAgent = pCoordcb->getRouteAgent() ;
      MsgHeader *rHeader = (MsgHeader *)pReceiveBuffer ;
      MsgOpQuery *forward = NULL ;
      std::string groupName ;
      std::string host ;
      std::string srv ;
      INT32 flag = -1 ;
      CHAR *pCommandName  = NULL;
      SINT64 numToSkip  = 0;
      SINT64 numToReturn  = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector  = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      CoordGroupList groupLst ;
      CoordGroupInfoPtr group ;
      BSONObj rInfo ;

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode = MSG_BS_QUERY_RES ;
      replyHeader.header.requestID = rHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID = rHeader->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      forward = (MsgOpQuery *)pReceiveBuffer ;
      forward->header.routeID.value = 0 ;
      forward->header.TID = cb->getTID() ;
      forward->header.opCode = MSG_CAT_DEL_NODE_REQ ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCommandName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector,
                            &pOrderBy, &pHint );
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to parse msg:%s",rc ) ;
         goto error ;
      }

      try
      {
         rInfo = BSONObj( pQuery ) ;
         BSONElement ele = rInfo.getField( FIELD_NAME_GROUPNAME ) ;
         if ( ele.eoo() || String != ele.type() )
         {
            PD_LOG( PDERROR, "failed to get groupname from msg[%s]",
                    rInfo.toString().c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         groupName = ele.String() ;

         if ( 0 == groupName.compare( CATALOG_GROUPNAME ) )
         {
            PD_LOG( PDERROR, "can not remove catalog node." ) ;
            rc = SDB_CATA_RM_CATA_FORBIDDEN ;
            goto error ;
         }

         ele = rInfo.getField( FIELD_NAME_HOST ) ;
         if ( ele.eoo() || String != ele.type() )
         {
            PD_LOG( PDERROR, "failed to get host from msg[%s]",
                    rInfo.toString().c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         host = ele.String() ;

         ele = rInfo.getField( PMD_OPTION_SVCNAME ) ;
         if ( ele.eoo() || String != ele.type() )
         {
            PD_LOG( PDERROR, "failed to get srv from msg[%s]",
                    rInfo.toString().c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         srv = ele.String() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happended:%s",
                 e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = executeOnCataGroup ( (CHAR*)forward, pRouteAgent,
                                cb, NULL, &groupLst ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to remove node, rc = %d", rc ) ;
         goto error ;
      }

      rc = rtnCoordGetGroupInfo( cb, groupName.c_str(), TRUE, group ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get groupinfo from cata:%d", rc ) ;
         goto error ;
      }

      if ( COORD_GROUPID != group->getGroupID() )
      {
         _MsgClsGInfoUpdated updated ;
         updated.groupID = group->getGroupID() ;
         clsGroupItem *groupItem = group->getGroupItem() ;
         MsgRouteID routeID ;
         UINT32 index = 0 ;

         while ( SDB_OK == groupItem->getNodeID( index++, routeID,
                                                 MSG_ROUTE_SHARD_SERVCIE ) )
         {
            rtnCoordSendRequestToNodeWithoutReply( (void *)(&updated),
                                                    routeID, pRouteAgent );
         }
      }

      {
         SINT32 retCode;
         INT32 rrc = SDB_OK ;
         rrc = rtnRemoteExec ( SDBSTOP, host.c_str(),
                               &retCode, &rInfo ) ;
         if ( SDB_OK != rrc )
         {
            PD_LOG( PDERROR,
                    "remote node execute(configure) failed(rc=%d)",
                    rrc );
            rc = SDB_CATA_FAILED_TO_CLEANUP ;
         }

         rrc = rtnRemoteExec ( SDBRM, host.c_str(),
                               &retCode, &rInfo ) ;
         if ( SDB_OK != rrc )
         {
            PD_LOG( PDERROR,
                    "remote node execute(configure) failed(rc=%d)",
                    rrc );
            rc = SDB_CATA_FAILED_TO_CLEANUP ;
         }
      }

      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      replyHeader.flags = rc ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDRMN_EXE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDUPN_EXE, "rtnCoordCMDUpdateNode::execute" )
   INT32 rtnCoordCMDUpdateNode::execute( CHAR *pReceiveBuffer,
                                         SINT32 packSize,
                                         CHAR **ppResultBuffer,
                                         pmdEDUCB *cb,
                                         MsgOpReply &replyHeader,
                                         BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDUPN_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      BOOLEAN isNeedRefresh = FALSE;
      do
      {
         INT32 flag;
         CHAR *pCMDName;
         SINT64 numToSkip;
         SINT64 numToReturn;
         CHAR *pQuery;
         CHAR *pFieldSelector;
         CHAR *pOrderBy;
         CHAR *pHint;
         rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip,
                           &numToReturn, &pQuery, &pFieldSelector,
                           &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "failed to parse update node request(rc=%d)",
                     rc );
            break;
         }
         BSONObj boNodeInfoOld;
         rc = getNodeInfo( pQuery, boNodeInfoOld );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "update node failed, failed to get old node info(rc=%d)",
                     rc );
            break;
         }
         BSONObj boNodeInfoNew;
         rc = getNodeInfo( pFieldSelector, boNodeInfoNew );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "update node failed, failed to get new node info(rc=%d)",
                     rc );
            break;
         }
         CoordGroupInfoPtr catGroupInfo;
         rc = rtnCoordGetCatGroupInfo( cb, isNeedRefresh, catGroupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "update node failed, failed to get cata-group-info(rc=%d)",
                     rc );
            break;
         }
         CHAR *pBuffer = NULL;
         INT32 bufferSize = 0;
         BSONObj orderBy;
         BSONObj hint;
         rc = msgBuildQueryMsg( &pBuffer, &bufferSize, "", flag, 0,
                              numToSkip, numToReturn, &boNodeInfoOld, &boNodeInfoNew,
                              &orderBy, &hint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "failed to build the request for catalog-node(rc=%d)",
                     rc );
            break;
         }
         MsgOpQuery *pCatReq = (MsgOpQuery *)pBuffer;
         pCatReq->header.routeID.value = 0;
         pCatReq->header.TID = cb->getTID();
         pCatReq->header.opCode = MSG_CAT_CREATE_NODE_REQ;
         rc = executeOnCataGroup( pBuffer, pRouteAgent, cb ) ;
         SDB_OSS_FREE(pBuffer);
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "failed to update node, execute on catalog-node failed(rc=%d)",
                     rc );
            break;
         }
         std::string strHostName;
         BSONObj boNodeConfig;
         try
         {
            BSONElement beHostName = boNodeInfoNew.getField( FIELD_NAME_HOST );
            if ( beHostName.eoo() || beHostName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "failed to get the field(%s)",
                        FIELD_NAME_HOST );
               break;
            }
            strHostName = beHostName.str();
            rc = getNodeConf( pFieldSelector, boNodeConfig, catGroupInfo );
            if ( rc != SDB_OK )
            {
               PD_LOG( PDERROR,
                     "failed to get node config(rc=%d)",
                     rc );
               break;
            }
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR,
                     "occured unexpected error:%s",
                     e.what() );
         }
         SINT32 retCode;
         rc = rtnRemoteExec ( SDBADD, strHostName.c_str(),
                              &retCode, &boNodeConfig ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR,
                  "add the node failed(rc=%d)",
                  rc );
            break;
         }
         if ( retCode != SDB_OK )
         {
            rc = retCode;
            PD_LOG ( PDERROR,
                  "remote node execute(Add) failed(rc=%d)",
                  rc );
            break;
         }
         rc = rtnRemoteExec ( SDBSTOP, strHostName.c_str(),
                              &retCode, &boNodeConfig ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR,
                  "stop the node failed(rc=%d)",
                  rc );
            break;
         }
         if ( retCode != SDB_OK )
         {
            rc = retCode;
            PD_LOG ( PDERROR,
                  "remote node execute(stop) failed(rc=%d)",
                  rc );
            break;
         }
         rc = rtnRemoteExec ( SDBSTART, strHostName.c_str(),
                              &retCode, &boNodeConfig ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR,
                  "start the node failed(rc=%d)",
                  rc );
            break;
         }
         if ( retCode != SDB_OK )
         {
            rc = retCode;
            PD_LOG ( PDERROR,
                  "remote node execute(start) failed(rc=%d)",
                  rc );
            break;
         }
         break;
      }while ( FALSE );
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDUPN_EXE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDATGR_STNS, "rtnCoordCMDActiveGroup::startNodes" )
   INT32 rtnCoordCMDActiveGroup::startNodes( bson::BSONObj &boGroupInfo,
                                             vector<BSONObj> &objList )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDATGR_STNS ) ;
      do
      {
         try
         {
            BSONElement beGroup = boGroupInfo.getField( FIELD_NAME_GROUP );
            if ( beGroup.eoo() || beGroup.type()!=Array )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "failed to get the field(%s)",
                        FIELD_NAME_GROUP );
               break;
            }
            BSONObjIterator i( beGroup.embeddedObject() );
            while ( i.more() )
            {
               BSONElement beTmp = i.next();
               BSONObj boTmp = beTmp.embeddedObject();
               BSONElement beHostName = boTmp.getField( FIELD_NAME_HOST );
               if ( beHostName.eoo() || beHostName.type()!=String )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDERROR, "Failed to get the HostName" );
                  break;
               }
               std::string strHostName = beHostName.str();
               BSONElement beService = boTmp.getField( FIELD_NAME_SERVICE );
               if ( beService.eoo() || beService.type()!=Array )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDWARNING, "Failed to get the field(%s)",
                           FIELD_NAME_SERVICE );
                  break;
               }
               std::string strServiceName;
               rc = getServiceName( beService, MSG_ROUTE_LOCAL_SERVICE,
                                    strServiceName );
               if ( rc != SDB_OK )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDWARNING, "Failed to get local-service-name" ) ;
                  break ;
               }
               SINT32 retCode ;
               BSONObjBuilder bobLocalService ;
               bobLocalService.append( PMD_OPTION_SVCNAME, strServiceName );
               BSONObj boLocalService = bobLocalService.obj();
               rc = rtnRemoteExec ( SDBSTART, strHostName.c_str(),
                                    &retCode, &boLocalService ) ;
               if ( SDB_OK == rc && SDB_OK == retCode )
               {
                  continue ;
               }
               if ( rc != SDB_OK )
               {
                  PD_LOG( PDERROR, "start the node failed (HostName=%s, "
                          "LocalService=%s, rc=%d)", strHostName.c_str(),
                          strServiceName.c_str(), rc ) ;
               }
               else if ( retCode != SDB_OK )
               {
                  rc = retCode;
                  PD_LOG( PDERROR, "remote node execute(start) failed "
                          "(HostName=%s, LocalService=%s, rc=%d)", 
                          strHostName.c_str(), strServiceName.c_str(), rc ) ;
               }
               BSONObjBuilder bobReply;
               bobReply.append( FIELD_NAME_HOST, strHostName );
               bobReply.append( PMD_OPTION_SVCNAME, strServiceName );
               bobReply.append( FIELD_NAME_ERROR_NO, retCode );
               objList.push_back( bobReply.obj() );
            }
            break ;
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "occured unexpected error:%s", e.what() ) ;
            break;
         }
      }while ( FALSE );

      if ( objList.size() != 0 )
      {
         rc = SDB_CM_RUN_NODE_FAILED ;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOCMDATGR_STNS, rc ) ;
      return rc ;
   }

   INT32 rtnCoordCMDActiveGroup::startNodes( clsGroupItem *pItem,
                                             vector<bson::BSONObj> &objList )
   {
      INT32 rc = SDB_OK ;
      MsgRouteID id ;
      string hostName ;
      string svcName ;
      UINT32 pos = 0 ;
      SINT32 retCode = SDB_OK ;

      while ( SDB_OK == pItem->getNodeInfo( pos, id, hostName, svcName,
                                            MSG_ROUTE_LOCAL_SERVICE ) )
      {
         ++pos ;

         retCode = SDB_OK ;
         BSONObjBuilder bobLocalService ;
         bobLocalService.append( PMD_OPTION_SVCNAME, svcName ) ;
         BSONObj boLocalService = bobLocalService.obj() ;

         rc = rtnRemoteExec ( SDBSTART, hostName.c_str(),
                              &retCode, &boLocalService ) ;
         if ( SDB_OK == rc && SDB_OK == retCode )
         {
            continue ;
         }
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR, "start the node failed (HostName=%s, "
                    "LocalService=%s, rc=%d)", hostName.c_str(),
                    svcName.c_str(), rc ) ;
         }
         else if ( retCode != SDB_OK )
         {
            rc = retCode ;
            PD_LOG( PDERROR, "remote node execute(start) failed "
                    "(HostName=%s, LocalService=%s, rc=%d)", 
                    hostName.c_str(), svcName.c_str(), rc ) ;
         }
         BSONObjBuilder bobReply ;
         bobReply.append( FIELD_NAME_HOST, hostName ) ;
         bobReply.append( PMD_OPTION_SVCNAME, svcName ) ;
         bobReply.append( FIELD_NAME_ERROR_NO, retCode ) ;
         objList.push_back( bobReply.obj() ) ;
      }

      if ( objList.size() != 0 )
      {
         rc = SDB_CM_RUN_NODE_FAILED ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDATGR_EXEONCATAGR, "rtnCoordCMDActiveGroup::executeOnCataGroup" )
   INT32 rtnCoordCMDActiveGroup::executeOnCataGroup ( CHAR *pBuffer,
                                                      netMultiRouteAgent *pRouteAgent,
                                                      pmdEDUCB *cb,
                                                      BSONObj &boGroupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDATGR_EXEONCATAGR ) ;
      BOOLEAN isNeedRetry = FALSE;
      BOOLEAN hasRetry = FALSE;
      BOOLEAN isNeedRefresh = FALSE;
      do
      {
         hasRetry = isNeedRetry ;
         isNeedRetry = FALSE;
         CoordGroupInfoPtr catGroupInfo;
         rc = rtnCoordGetCatGroupInfo( cb, isNeedRefresh, catGroupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Execute on catalogue node failed, failed to "
                     "get catalogue group info(rc=%d)", rc );
            break;
         }
         REQUESTID_MAP sendNodes ;
         rc = rtnCoordSendRequestToPrimary( pBuffer, catGroupInfo, sendNodes,
                                            pRouteAgent, MSG_ROUTE_CAT_SERVICE,
                                            cb ) ;
         if ( rc != SDB_OK )
         {
            rtnCoordClearRequest( cb, sendNodes );
            if ( !hasRetry )
            {
               isNeedRetry = TRUE;
               isNeedRefresh = TRUE;
               continue;
            }
            else
            {
               PD_LOG ( PDERROR, "Execute on catalogue node failed, failed "
                        "to send request to catalogue-node(rc=%d)", rc );
               break;
            }
         }
         REPLY_QUE replyQue ;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue,
                                MAKE_REPLY_TYPE(
                                ((MsgHeader*)pBuffer)->opCode ) ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Execute on catalogue node failed, get reply"
                     "failed(rc=%d)", rc );
            break;
         }
         while ( !replyQue.empty() )
         {
            MsgOpReply *pReply = NULL;
            pReply = (MsgOpReply *)(replyQue.front());
            replyQue.pop();
            if ( SDB_OK == rc )
            {
               if ( SDB_OK != pReply->flags )
               {
                  if ( SDB_CLS_NOT_PRIMARY == pReply->flags
                     && !hasRetry )
                  {
                     CoordGroupInfoPtr groupInfoTmp;
                     rc = rtnCoordGetCatGroupInfo( cb, TRUE, groupInfoTmp );
                     if ( SDB_OK == rc )
                     {
                        isNeedRetry = TRUE;
                     }
                  }
                  else
                  {
                     rc = pReply->flags;
                  }
               }
               else
               {
                  try
                  {
                     if ( pReply->numReturned > 0 )
                     {
                        CHAR *pInfo = (CHAR *)pReply + sizeof(MsgOpReply);
                        BSONObj boTmp( pInfo ) ;
                        BSONObjBuilder bobGroupInfo;
                        bobGroupInfo.appendElements( boTmp ) ;
                        boGroupInfo = bobGroupInfo.obj() ;
                     }
                     else
                     {
                        rc = SDB_INVALIDARG;
                        PD_LOG ( PDERROR, "get invalid reply(numReturned=%d)",
                                 pReply->numReturned );
                     }
                  }
                  catch ( std::exception &e )
                  {
                     rc = SDB_INVALIDARG;
                     PD_LOG ( PDERROR, "occured unexpected error:%s",
                              e.what() ) ;
                  }
               }
            }
            SDB_OSS_FREE ( pReply );
         }
      }while ( isNeedRetry );
      PD_TRACE_EXITRC ( SDB_RTNCOCMDATGR_EXEONCATAGR, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDATGR_EXE, "rtnCoordCMDActiveGroup::execute" )
   INT32 rtnCoordCMDActiveGroup::execute( CHAR *pReceiveBuffer,
                                          SINT32 packSize,
                                          CHAR **ppResultBuffer,
                                          pmdEDUCB *cb,
                                          MsgOpReply &replyHeader,
                                          BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDATGR_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *pReq = (MsgOpQuery *)pReceiveBuffer ;
      pReq->header.routeID.value = 0 ;
      pReq->header.TID = cb->getTID() ;
      pReq->header.opCode = MSG_CAT_ACTIVE_GROUP_REQ ;

      const CHAR *pGroupName = NULL ;
      vector<BSONObj> objList ;
      CoordGroupInfoPtr catGroupInfo ;

      do
      {
         CHAR *pQuery = NULL ;
         rc = msgExtractQuery( pReceiveBuffer, NULL, NULL, NULL, NULL,
                               &pQuery, NULL, NULL, NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to extract msg, rc: %d", rc ) ;
            break ;
         }
         try
         {
            BSONObj boQuery( pQuery ) ;
            BSONElement ele = boQuery.getField( CAT_GROUPNAME_NAME ) ;
            if ( ele.type() != String )
            {
               PD_LOG( PDERROR, "Get field[%s] type[%d] is not String",
                       CAT_GROUPNAME_NAME, ele.type() ) ;
               rc = SDB_INVALIDARG ;
               break ;
            }
            pGroupName = ele.valuestr() ;
         }
         catch( std::exception &e )
         {
            PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
            rc = SDB_INVALIDARG ;
            break ;
         }

         BSONObj boGroupInfo;
         rc = executeOnCataGroup( pReceiveBuffer, pRouteAgent,
                                  cb, boGroupInfo ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to active group, execute on "
                     "catalog-node failed(rc=%d)", rc ) ;

            if ( 0 != ossStrcmp( CATALOG_GROUPNAME, pGroupName ) ||
                 SDB_OK != rtnCoordGetLocalCatGroupInfo( catGroupInfo ) ||
                 NULL == catGroupInfo.get() ||
                 catGroupInfo->getGroupSize() == 0 )
            {
               break ;
            }
         }

         if ( catGroupInfo.get() &&
              catGroupInfo->getGroupSize() > 0 )
         {
            rc = startNodes( catGroupInfo->getGroupItem(), objList ) ;
         }
         else
         {
            rc = startNodes( boGroupInfo, objList ) ;
         }
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Start node failed(rc=%d)", rc ) ;
            UINT32 i = 0;
            std::string strNodeList;
            for ( ; i < objList.size(); i++ )
            {
               strNodeList += objList[i].toString( false, false );
            }
            PD_LOG_MSG( PDERROR, "Strart failed nodes: %s",
                        strNodeList.c_str() ) ;
            break;
         }
      }while ( FALSE ) ;

      replyHeader.flags = rc ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDATGR_EXE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCTIND_EXE, "rtnCoordCMDCreateIndex::execute" )
   INT32 rtnCoordCMDCreateIndex::execute( CHAR *pReceiveBuffer,
                                          SINT32 packSize,
                                          CHAR **ppResultBuffer,
                                          pmdEDUCB *cb,
                                          MsgOpReply &replyHeader,
                                          BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCTIND_EXE ) ;
      INT32 tempRC                     = SDB_OK ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag                       = 0 ;
      CHAR *pCMDName                   = NULL ;
      SINT64 numToSkip                 = 0 ;
      SINT64 numToReturn               = -1 ;
      CHAR *pQuery                     = NULL ;
      CHAR *pFieldSelector             = NULL ;
      CHAR *pOrderBy                   = NULL ;
      CHAR *pHint                      = NULL ;

      const CHAR *strCollectionName    = NULL ;
      const CHAR *strIndexName         = NULL ;
      BOOLEAN isNeedRefresh            = TRUE ;
      MsgOpQuery *pCreateReq           = NULL ;

      CoordCataInfoPtr cataInfo;
      CoordGroupList dataNodeGroupLst;
      CoordGroupList sendGroupLst;
      CoordGroupList hasRollBackGroups;

      CHAR *pDropMsg                   = NULL;
      INT32 bufferSize                 = 0;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector, &pOrderBy, &pHint );
      PD_RC_CHECK ( rc, PDERROR,
                    "failed to parse create index request(rc=%d)",
                    rc ) ;
      try
      {
         BSONObj boQuery(pQuery);
         BSONElement beCollectionName
                     = boQuery.getField( FIELD_NAME_COLLECTION );
         BSONElement beIndex = boQuery.getField( FIELD_NAME_INDEX );
         BSONElement beIndexName ;
         PD_CHECK ( beCollectionName.type() == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "create index failed, failed to get the field(%s)",
                    FIELD_NAME_COLLECTION ) ;
         strCollectionName = beCollectionName.valuestr() ;

         PD_CHECK ( beIndex.type() == Object,
                    SDB_INVALIDARG, error, PDERROR,
                    "create index failed, failed to get the field(%s)",
                    FIELD_NAME_INDEX ) ;
         beIndexName = beIndex.embeddedObject().getField( IXM_FIELD_NAME_NAME );
         PD_CHECK ( beIndexName.type() == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "create index failed, failed to get the field(%s)",
                    IXM_FIELD_NAME_NAME ) ;
         strIndexName=beIndexName.valuestr();
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( rc, PDERROR,
                       "create index failed, occured unexpected error:%s",
                       e.what() );
      }

   retry :
      rc = rtnCoordGetCataInfo( cb, strCollectionName,
                                isNeedRefresh, cataInfo ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "failed to create index(%s), "
                    "get catalogue failed(rc=%d)",
                    strCollectionName, rc ) ;
      if ( cataInfo->isSharded() )
      {
         try
         {
            BSONObj arg ( pQuery ) ;
            BSONObj indexObj ;
            BSONObj indexKey ;
            BSONElement indexUnique ;
            BOOLEAN isUnique = TRUE ;
            rc = rtnGetObjElement ( arg, FIELD_NAME_INDEX, indexObj ) ;
            PD_RC_CHECK ( rc, PDERROR,
                          "Failed to get object index, rc = %d", rc ) ;
            rc = rtnGetObjElement ( indexObj, IXM_KEY_FIELD, indexKey ) ;
            PD_RC_CHECK ( rc, PDERROR,
                          "Failed to get key for index: %s, rc = %d",
                          indexObj.toString().c_str(), rc ) ;
            indexUnique = indexObj.getField ( IXM_UNIQUE_FIELD ) ;
            if ( indexUnique.type() != Bool )
            {
               isUnique = FALSE ;
            }
            else
            {
               isUnique = indexUnique.boolean () ;
            }
            if ( isUnique )
            {
               BSONObj shardingKey ;
               cataInfo->getShardingKey ( shardingKey ) ;
               BSONObjIterator shardingItr ( shardingKey ) ;
               while ( shardingItr.more () )
               {
                  BSONElement sk = shardingItr.next() ;
                  PD_CHECK ( !indexKey.getField ( sk.fieldName () ).eoo(),
                              SDB_SHARD_KEY_NOT_IN_UNIQUE_KEY, error,
                              PDWARNING,
                              "All fields in sharding key must be included "
                              "in unique index, missing field: %s; "
                              "shardingKey: %s, indexKey: %s",
                              sk.fieldName(), shardingKey.toString().c_str(),
                              indexKey.toString().c_str() ) ;
               }
            }
         } // try
         catch ( std::exception &e )
         {
            PD_RC_CHECK ( SDB_SYS, PDERROR,
                          "Exception during extracting unique key: %s",
                          e.what() ) ;
         }
      } // if ( beCollectionName.type()!=String )

      rc = rtnCoordGetGroupsByCataInfo( cataInfo, sendGroupLst,
                                        dataNodeGroupLst );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to get group list(rc=%d)", rc ) ;
         goto error;
      }
      pCreateReq                       = ( MsgOpQuery *)pReceiveBuffer;
      pCreateReq->version              = cataInfo->getVersion();
      pCreateReq->header.routeID.value = 0;
      pCreateReq->header.TID           = cb->getTID();
      pCreateReq->header.opCode        = MSG_BS_QUERY_REQ;
      rc = executeOnDataGroup( (MsgHeader *)pCreateReq,
                               dataNodeGroupLst, sendGroupLst,
                               pRouteAgent, cb, TRUE );
      if ( rc != SDB_OK )
      {
         if ( !isNeedRefresh && rtnCoordWriteRetryRC( rc ) )
         {
            isNeedRefresh = TRUE;
            goto retry ;
         }
         PD_CHECK ( SDB_OK == rc, rc, rollback, PDERROR,
                    "Failed to create index on data group, rc = %d", rc ) ;
      }
   done :
      replyHeader.flags = rc;
      if ( pDropMsg )
      {
         SDB_OSS_FREE ( pDropMsg ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCTIND_EXE, rc ) ;
      return rc;
   rollback :
      tempRC = msgBuildDropIndexMsg( &pDropMsg, &bufferSize,
                                     strCollectionName,
                                     strIndexName, 0 );
      PD_RC_CHECK ( tempRC, PDERROR,
                    "Failed to build drop index message, rc = %d",
                    tempRC ) ;
      isNeedRefresh = FALSE ;
   retry_rollback :
      tempRC = rtnCoordGetCataInfo( cb, strCollectionName,
                                    isNeedRefresh, cataInfo );
      PD_RC_CHECK ( tempRC, PDERROR,
                    "failed to rollback create index(%s), "
                    "get catalogue failed(rc=%d)",
                    strCollectionName, tempRC ) ;

      if ( cataInfo->isMainCL() )
      {
         PD_LOG( PDWARNING,
                 "main-collection create index failed and will not rollback" );
         goto error;
      }

      ((MsgOpQuery *)pDropMsg)->version              = cataInfo->getVersion();
      ((MsgOpQuery *)pDropMsg)->header.routeID.value = 0;
      ((MsgOpQuery *)pDropMsg)->header.TID           = cb->getTID();
      tempRC = executeOnDataGroup( (MsgHeader *)pDropMsg,
                                    sendGroupLst,
                                    hasRollBackGroups,
                                    pRouteAgent, cb, TRUE );
      if ( tempRC != SDB_OK )
      {
         if ( !isNeedRefresh && rtnCoordWriteRetryRC( tempRC ) )
         {
            isNeedRefresh = TRUE;
            goto retry_rollback ;
         }
         PD_RC_CHECK ( tempRC, PDERROR,
                       "Failed to rollback create index, rc = %d", tempRC ) ;
      }
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDDPIN_EXE, "rtnCoordCMDDropIndex::execute" )
   INT32 rtnCoordCMDDropIndex::execute( CHAR *pReceiveBuffer,
                                        SINT32 packSize,
                                        CHAR **ppResultBuffer,
                                        pmdEDUCB *cb,
                                        MsgOpReply &replyHeader,
                                        BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDDPIN_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      do
      {
         INT32 flag;
         CHAR *pCMDName;
         SINT64 numToSkip;
         SINT64 numToReturn;
         CHAR *pQuery;
         CHAR *pFieldSelector;
         CHAR *pOrderBy;
         CHAR *pHint;
         rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName,
                           &numToSkip, &numToReturn, &pQuery,
                           &pFieldSelector, &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "failed to parse drop index request(rc=%d)",
                     rc );
            break;
         }
         std::string strCollectionName;
         try
         {
            BSONObj boQuery(pQuery);
            BSONElement beCollectionName
                        = boQuery.getField( FIELD_NAME_COLLECTION );
            if ( beCollectionName.eoo() || beCollectionName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG( PDERROR,
                     "drop index failed, failed to get the field(%s)",
                     FIELD_NAME_COLLECTION );
               break;
            }
            strCollectionName = beCollectionName.str();
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR,
                     "drop index failed, occured unexpected error:%s",
                     e.what() );
         }
         BOOLEAN isNeedRefresh = FALSE;
         CoordGroupList sendGroupLst;
         while ( TRUE )
         {
            CoordCataInfoPtr cataInfo;
            rc = rtnCoordGetCataInfo( cb, strCollectionName.c_str(),
                                      isNeedRefresh, cataInfo );
            if ( rc != SDB_OK )
            {
               PD_LOG ( PDERROR,
                        "failed to drop index(%s), get catalogue failed(rc=%d)",
                        strCollectionName.c_str(), rc );
               break;
            }
            CoordGroupList dataNodeGroupLst;
            rc = rtnCoordGetGroupsByCataInfo( cataInfo, sendGroupLst,
                                              dataNodeGroupLst );
            if ( rc != SDB_OK )
            {
               PD_LOG ( PDERROR,
                        "failed to get group list(rc=%d)",
                        rc );
               break;
            }
            MsgOpQuery *pDropReq = ( MsgOpQuery *)pReceiveBuffer;
            pDropReq->version = cataInfo->getVersion();
            pDropReq->header.routeID.value = 0;
            pDropReq->header.TID = cb->getTID();
            pDropReq->header.opCode = MSG_BS_QUERY_REQ;
            rc = executeOnDataGroup( (MsgHeader *)pDropReq,
                                    dataNodeGroupLst, sendGroupLst,
                                    pRouteAgent, cb, TRUE );
            if ( rc != SDB_OK )
            {
               if ( !isNeedRefresh && rtnCoordWriteRetryRC( rc ) )
               {
                  isNeedRefresh = TRUE;
                  continue;
               }
               PD_LOG ( PDERROR, "Failed to drop index(%s), drop on data-node "
                        "failed(rc=%d)", strCollectionName.c_str(), rc );
            }
            break;
         }
      }while ( FALSE );
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDDPIN_EXE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDOPONNODE_EXE, "rtnCoordCMDOperateOnNode::execute" )
   INT32 rtnCoordCMDOperateOnNode::execute( CHAR *pReceiveBuffer,
                                            SINT32 packSize,
                                            CHAR **ppResultBuffer,
                                            pmdEDUCB *cb,
                                            MsgOpReply &replyHeader,
                                            BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDOPONNODE_EXE ) ;
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;
      do
      {
         INT32 flag;
         CHAR *pCMDName;
         SINT64 numToSkip;
         SINT64 numToReturn;
         CHAR *pQuery;
         CHAR *pFieldSelector;
         CHAR *pOrderBy;
         CHAR *pHint;
         rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip,
                               &numToReturn, &pQuery, &pFieldSelector,
                               &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "failed to parse the request(rc=%d)", rc );
            break;
         }
         const CHAR *strHostName = NULL ;
         BSONObj boNodeConf;
         try
         {
            BSONObj boQuery( pQuery );
            BSONElement beHostName = boQuery.getField( FIELD_NAME_HOST );
            if ( beHostName.eoo() || beHostName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "failed to get the field(%s)",
                        FIELD_NAME_HOST );
               break;
            }
            strHostName = beHostName.valuestrsafe () ;
            BSONElement beSvcName = boQuery.getField( PMD_OPTION_SVCNAME );
            if ( beSvcName.eoo() || beSvcName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "failed to get the field(%s)",
                        PMD_OPTION_SVCNAME );
               break;
            }
            BSONObjBuilder bobNodeConf;
            bobNodeConf.append( beSvcName );
            boNodeConf = bobNodeConf.obj();
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "occured unexpected error:%s",
                     e.what() );
            break;
         }
         SINT32 opType = getOpType();
         SINT32 retCode;
         rc = rtnRemoteExec ( opType, strHostName,
                              &retCode, &boNodeConf ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR, "operate failed(rc=%d)", rc );
            break;
         }
         if ( retCode != SDB_OK )
         {
            rc = retCode;
            PD_LOG ( PDERROR, "remote node execute failed(rc=%d)", rc ) ;
         }
         break ;
      }while ( FALSE );
      replyHeader.flags = rc ;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDOPONNODE_EXE, rc ) ;
      return rc;
   }

   SINT32 rtnCoordCMDStartupNode::getOpType()
   {
      return SDBSTART;
   }

   SINT32 rtnCoordCMDShutdownNode::getOpType()
   {
      return SDBSTOP;
   }

   // PD_TRACE_DECLARE_FUNCTION (SDB_RTNCOCMDOPONGR_EXE, "rtnCoordCMDOperateOnGroup::execute" )
   INT32 rtnCoordCMDOperateOnGroup::execute( CHAR *pReceiveBuffer,
                                             SINT32 packSize,
                                             CHAR **ppResultBuffer,
                                             pmdEDUCB *cb,
                                             MsgOpReply &replyHeader,
                                             BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDOPONGR_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      SDB_RTNCB *pRtncb                = pKrcb->getRTNCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      SINT64 contextID = -1 ;

      do
      {
         INT32 flag;
         CHAR *pCMDName;
         SINT64 numToSkip;
         SINT64 numToReturn;
         CHAR *pQuery;
         CHAR *pFieldSelector;
         CHAR *pOrderBy;
         CHAR *pHint;
         rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName,
                               &numToSkip, &numToReturn, &pQuery,
                               &pFieldSelector, &pOrderBy, &pHint ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to parse the request(rc=%d)", rc ) ;
            break;
         }
         BSONObj boQuery;
         BSONObj boFieldSelector;
         BSONObj boOrderBy;
         BSONObj boHint;
         try
         {
            BSONObjBuilder bobQuery;
            BSONObj boReq(pQuery);
            BSONElement beGroupName = boReq.getField( FIELD_NAME_GROUPNAME );
            if ( beGroupName.eoo() || beGroupName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to get the field(%s)",
                        FIELD_NAME_GROUPNAME ) ;
               break ;
            }
            bobQuery.append( beGroupName );
            boQuery = bobQuery.obj();
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "Occured unexpected error:%s", e.what() ) ;
            break ;
         }

         rtnCoordProcesserFactory *pProcesserFactory
               = pCoordcb->getProcesserFactory() ;
         rtnCoordOperator *pCmdProcesser = NULL ;
         pCmdProcesser = pProcesserFactory->getCommandProcesser(
            COORD_CMD_LISTGROUPS ) ;
         SDB_ASSERT( pCmdProcesser , "pCmdProcesser can't be NULL" ) ;
         char *pListReq = NULL ;
         INT32 listReqSize = 0 ;
         rc = msgBuildQueryMsg( &pListReq, &listReqSize, "", 0, 0, 0, 1,
                                &boQuery, &boFieldSelector, &boOrderBy,
                                &boHint ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to build list request(rc=%d)", rc ) ;
            break;
         }
         rc = pCmdProcesser->execute( pReceiveBuffer, listReqSize,
                                      ppResultBuffer, cb, replyHeader,
                                      ppErrorObj ) ;
         if ( pListReq )
         {
            SDB_OSS_FREE( pListReq ) ;
            pListReq = NULL;
         }
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to list groups(rc=%d)", rc ) ;
            break;
         }

         rtnContextBuf buffObj ;
         rc = rtnGetMore( replyHeader.contextID, -1, buffObj, cb, pRtncb ) ;
         if ( rc != SDB_OK )
         {
            replyHeader.contextID = -1 ;
            if ( rc == SDB_DMS_EOC || NULL == buffObj.data() )
            {
               rc = SDB_CLS_GRP_NOT_EXIST;
            }
            PD_LOG ( PDERROR, "Failed to get group info(rc=%d)", rc ) ;
            break;
         }
         contextID = replyHeader.contextID ;
         BSONObj boGroupInfo ;
         try
         {
            boGroupInfo = BSONObj( buffObj.data() ) ;
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "occured unexpected error:%s", e.what() ) ;
            break;
         }
         rc = opOnGroup( boGroupInfo ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Operate failed(rc=%d)", rc ) ;
            break;
         }
      }while ( FALSE ) ;

      replyHeader.flags = rc ;
      replyHeader.contextID = -1 ;
      if ( contextID >= 0 )
      {
         pRtncb->contextDelete( contextID, cb ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNCOCMDOPONGR_EXE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTN_COCOMDOPONGR_OPONGR, "rtnCoordCMDOperateOnGroup::opOnGroup" )
   INT32 rtnCoordCMDOperateOnGroup::opOnGroup( bson::BSONObj &boGroupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTN_COCOMDOPONGR_OPONGR ) ;
      SINT32 opType = getOpType();
      vector<BSONObj> objList;
      do
      {
         try
         {
            BSONElement beGroup = boGroupInfo.getField( FIELD_NAME_GROUP );
            if ( beGroup.eoo() || beGroup.type()!=Array )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "failed to get the field(%s)",
                        FIELD_NAME_GROUP );
               break;
            }
            BSONObjIterator i( beGroup.embeddedObject() );
            while ( i.more() )
            {
               BSONElement beTmp = i.next();
               BSONObj boTmp = beTmp.embeddedObject();
               BSONElement beHostName = boTmp.getField( FIELD_NAME_HOST );
               if ( beHostName.eoo() || beHostName.type()!=String )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDERROR, "failed to get the HostName");
                  break;
               }
               std::string strHostName = beHostName.str();
               BSONElement beService = boTmp.getField( FIELD_NAME_SERVICE );
               if ( beService.eoo() || beService.type()!=Array )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDWARNING, "failed to get the field(%s)",
                           FIELD_NAME_SERVICE );
                  break;
               }
               std::string strServiceName;
               rc = getServiceName( beService, MSG_ROUTE_LOCAL_SERVICE,
                                    strServiceName );
               if ( rc != SDB_OK )
               {
                  rc = SDB_INVALIDARG;
                  PD_LOG ( PDWARNING, "failed to get local-service-name" );
                  break;
               }
               SINT32 retCode;
               BSONObjBuilder bobLocalService;
               bobLocalService.append( PMD_OPTION_SVCNAME, strServiceName );
               BSONObj boLocalService = bobLocalService.obj();
               rc = rtnRemoteExec ( opType, strHostName.c_str(),
                                    &retCode, &boLocalService ) ;
               if ( SDB_OK == rc && SDB_OK == retCode )
               {
                  continue;
               }
               if ( rc != SDB_OK )
               {
                  PD_LOG( PDERROR, "Operate failed (HostName=%s, "
                          "LocalService=%s, rc=%d)", strHostName.c_str(),
                          strServiceName.c_str(), rc ) ;
               }
               else if ( retCode != SDB_OK )
               {
                  rc = retCode;
                  PD_LOG( PDERROR, "Remote node execute(opType=%d) failed "
                          "(HostName=%s, LocalService=%s, rc=%d)",
                          opType, strHostName.c_str(), strServiceName.c_str(),
                          rc ) ;
               }
               BSONObjBuilder bobReply;
               bobReply.append( FIELD_NAME_HOST, strHostName );
               bobReply.append( PMD_OPTION_SVCNAME, strServiceName );
               bobReply.append( FIELD_NAME_ERROR_NO, retCode );
               objList.push_back( bobReply.obj() );
            }
            break;
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "occured unexpected error:%s", e.what() ) ;
            break;
         }
      }while ( FALSE );
      if ( objList.size() != 0 )
      {
         rc = SDB_CM_OP_NODE_FAILED;
      }
      PD_TRACE_EXITRC ( SDB_RTN_COCOMDOPONGR_OPONGR, rc ) ;
      return rc ;
   }
   SINT32 rtnCoordCMDShutdownGroup::getOpType()
   {
      return SDBSTOP;
   }

   INT32 rtnCoordCMDSplit::getCLCount( const CHAR * clFullName,
                                       CoordGroupList & groupList,
                                       pmdEDUCB *cb,
                                       UINT64 & count )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *pKRCB                   = pmdGetKRCB () ;
      SDB_RTNCB *pRtncb                = pKRCB->getRTNCB() ;
      rtnContext *pContext             = NULL ;
      count                            = 0 ;
      CoordGroupList tmpGroupList      = groupList ;

      BSONObj collectionObj ;
      BSONObj dummy ;
      rtnContextBuf buffObj ;

      collectionObj = BSON( FIELD_NAME_COLLECTION << clFullName ) ;

      rc = rtnCoordNodeQuery( CMD_ADMIN_PREFIX CMD_NAME_GET_COUNT,
                              dummy, dummy, dummy, collectionObj,
                              0, 1, tmpGroupList, cb, &pContext,
                              clFullName ) ;

      PD_RC_CHECK ( rc, PDERROR, "Failed to getcount from source node, rc = %d",
                    rc ) ;

      rc = pContext->getMore( -1, buffObj, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Get count getmore failed, rc: %d", rc ) ;
         goto error ;
      }
      else
      {
         BSONObj countObj ( buffObj.data() ) ;
         BSONElement beTotal = countObj.getField( FIELD_NAME_TOTAL );
         PD_CHECK( beTotal.isNumber(), SDB_INVALIDARG, error,
                   PDERROR, "count failed, failed to get the field(%s)",
                   FIELD_NAME_TOTAL ) ;
         count = beTotal.numberLong() ;
      }

   done:
      if ( pContext )
      {
         SINT64 contextID = pContext->contextID() ;
         pRtncb->contextDelete ( contextID, cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSP_EXE, "rtnCoordCMDSplit::execute" )
   INT32 rtnCoordCMDSplit::execute( CHAR *pReceiveBuffer,
                                    SINT32 packSize,
                                    CHAR **ppResultBuffer,
                                    pmdEDUCB *cb,
                                    MsgOpReply &replyHeader,
                                    BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSP_EXE ) ;
      pmdKRCB *pKRCB                   = pmdGetKRCB () ;
      SDB_RTNCB *pRtncb                = pKRCB->getRTNCB() ;
      CoordCB *pCoordcb                = pKRCB->getCoordCB () ;
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent () ;
      INT64 contextID                  = -1 ;

      CHAR *pCollectionName            = NULL ;
      CHAR *pQuery                     = NULL ;
      CHAR *pSelector                  = NULL ;
      CHAR *pOrderBy                   = NULL ;
      CHAR *pHint                      = NULL ;
      INT64 numToSkip                  = 0 ;
      INT64 numToReturn                = -1 ;
      INT32 flag                       = 0 ;

      CHAR szSource [ OSS_MAX_GROUPNAME_SIZE + 1 ] = {0} ;
      CHAR szTarget [ OSS_MAX_GROUPNAME_SIZE + 1 ] = {0} ;
      const CHAR *strName              = NULL ;
      CHAR *splitReadyBuffer           = NULL ;
      INT32 splitReadyBufferSz         = 0 ;
      CHAR *splitQueryBuffer           = NULL ;
      INT32 splitQueryBufferSz         = 0 ;
      MsgOpQuery *pSplitQuery          = NULL ;
      UINT64 taskID                    = 0 ;
      BOOLEAN async                    = FALSE ;

      BSONObj boShardingKey ;
      CoordCataInfoPtr cataInfo ;
      BSONObj boKeyStart ;
      BSONObj boKeyEnd ;
      BSONObj boDummy ;
      BSONObj boHint ;
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      BSONObj boRecord ;
      FLOAT64 percent = 0.0 ;

      MsgOpQuery *pSplitReq            = (MsgOpQuery *)pReceiveBuffer ;
      pSplitReq->header.routeID.value  = 0 ;
      pSplitReq->header.TID            = cb->getTID () ;
      pSplitReq->header.opCode         = MSG_CAT_SPLIT_PREPARE_REQ ;
      CoordGroupList groupLst ;
      CoordGroupList groupList1 ;
      CoordGroupList groupLstTmp;
      BOOLEAN isNeedRefresh            = FALSE ;

      /******************************************************************
       *              PREPARE PHASE                                     *
       ******************************************************************/
      rc = executeOnCataGroup ( (CHAR*)pSplitReq, pRouteAgent,
                                cb, NULL, &groupLst ) ;
      PD_RC_CHECK ( rc, PDERROR, "Split failed on catalog, rc = %d", rc ) ;

      rc = msgExtractQuery ( (CHAR*)pSplitReq, &flag, &pCollectionName,
                             &numToSkip, &numToReturn, &pQuery,
                             &pSelector, &pOrderBy, &pHint ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to extract query, rc = %d", rc ) ;
      try
      {
         /***************************************************************
          *             DO SOME VALIDATION HERE                         *
          ***************************************************************/
         BSONObj boQuery ( pQuery ) ;

         BSONElement beName = boQuery.getField ( CAT_COLLECTION_NAME ) ;
         BSONElement beSplitQuery =
               boQuery.getField ( CAT_SPLITQUERY_NAME ) ;
         BSONElement beSplitEndQuery ;
         BSONElement beSource = boQuery.getField ( CAT_SOURCE_NAME ) ;
         BSONElement beTarget = boQuery.getField ( CAT_TARGET_NAME ) ;
         BSONElement beAsync  = boQuery.getField ( FIELD_NAME_ASYNC ) ;
         percent = boQuery.getField( CAT_SPLITPERCENT_NAME ).numberDouble() ;
         PD_CHECK ( !beName.eoo() && beName.type () == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "Failed to process split prepare, unable to find "
                    "collection name field" ) ;
         strName = beName.valuestr() ;
         PD_CHECK ( !beSource.eoo() && beSource.type() == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "Unable to find source field" ) ;
         rc = catGroupNameValidate ( beSource.valuestr() ) ;
         PD_CHECK ( SDB_OK == rc, SDB_INVALIDARG, error, PDERROR,
                    "Source name is not valid: %s",
                    beSource.valuestr() ) ;
         ossStrncpy ( szSource, beSource.valuestr(), sizeof(szSource) ) ;

         PD_CHECK ( !beTarget.eoo() && beTarget.type() == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "Unable to find target field" ) ;
         rc = catGroupNameValidate ( beTarget.valuestr() ) ;
         PD_CHECK ( SDB_OK == rc, SDB_INVALIDARG, error, PDERROR,
                    "Target name is not valid: %s",
                    beTarget.valuestr() ) ;
         ossStrncpy ( szTarget, beTarget.valuestr(), sizeof(szTarget) ) ;

         if ( Bool == beAsync.type() )
         {
            async = beAsync.Bool() ? TRUE : FALSE ;
         }
         else if ( !beAsync.eoo() )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] error", FIELD_NAME_ASYNC,
                    beAsync.type() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         if ( !beSplitQuery.eoo() )
         {
            PD_CHECK ( beSplitQuery.type() == Object,
                       SDB_INVALIDARG, error, PDERROR,
                       "Split is not defined or not valid" ) ;
            beSplitEndQuery = boQuery.getField ( CAT_SPLITENDQUERY_NAME ) ;
            if ( !beSplitEndQuery.eoo() )
            {
               PD_CHECK ( beSplitEndQuery.type() == Object,
                          SDB_INVALIDARG, error, PDERROR,
                          "Split is not defined or not valid" ) ;
            }
         }
         else
         {
            PD_CHECK( percent > 0.0 && percent <= 100.0,
                      SDB_INVALIDARG, error, PDERROR,
                      "Split percent value is error" ) ;
         }

         rc = rtnCoordGetCataInfo ( cb, strName, TRUE, cataInfo ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to get cata info for collection %s, rc = %d",
                       strName, rc ) ;
         cataInfo->getShardingKey ( boShardingKey ) ;
         PD_CHECK ( !boShardingKey.isEmpty(), SDB_COLLECTION_NOTSHARD, error,
                    PDWARNING, "Collection must be sharded: %s", strName ) ;

         /*********************************************************************
          *           GET THE SHARDING KEY VALUE FROM SOURCE                  *
          *********************************************************************/
         if ( cataInfo->getCatalogSet()->isHashSharding() )
         {
            if ( !beSplitQuery.eoo() )
            {
               boKeyStart = beSplitQuery.embeddedObject() ;
               boKeyEnd = beSplitEndQuery.eoo() ?
                          BSONObj() : beSplitEndQuery.embeddedObject() ;
            }
         }
         else
         {
            if ( beSplitQuery.eoo())
            {
               rc = _getBoundByPercent( strName, percent, cataInfo,
                                        groupLst, cb, boKeyStart, boKeyEnd ) ;
            }
            else
            {
               rc = _getBoundByCondition( strName,
                                          beSplitQuery.embeddedObject(),
                                          beSplitEndQuery.eoo() ?
                                          BSONObj():
                                          beSplitEndQuery.embeddedObject(),
                                          groupLst,
                                          cb,
                                          cataInfo,
                                          boKeyStart, boKeyEnd ) ;
            }

            PD_RC_CHECK( rc, PDERROR,
                         "failed to get bound, rc: %d",
                         rc ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR,
                       "Exception when query from remote node: %s",
                       e.what() ) ;
      }

      /************************************************************************
       *         SHARDING READY REQUEST                                       *
       ************************************************************************/
      try
      {
         BSONObj boSend ;
         vector<BSONObj> boRecv ;
         boSend = BSON ( CAT_COLLECTION_NAME << strName <<
                         CAT_SOURCE_NAME << szSource <<
                         CAT_TARGET_NAME << szTarget <<
                         CAT_SPLITPERCENT_NAME << percent <<
                         CAT_SPLITVALUE_NAME << boKeyStart <<
                         CAT_SPLITENDVALUE_NAME << boKeyEnd ) ;
         rc = msgBuildQueryMsg ( &splitReadyBuffer, &splitReadyBufferSz,
                                 CMD_ADMIN_PREFIX CMD_NAME_SPLIT, 0,
                                 0, 0, -1, &boSend, NULL,
                                 NULL, NULL ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to build query message, rc: %d",
                       rc ) ;
         pSplitReq                        = (MsgOpQuery *)splitReadyBuffer ;
         pSplitReq->header.routeID.value  = 0 ;
         pSplitReq->header.TID            = cb->getTID () ;
         pSplitReq->header.opCode         = MSG_CAT_SPLIT_READY_REQ ;
         pSplitReq->version               = cataInfo->getVersion();

         rc = executeOnCataGroup ( (CHAR*)pSplitReq, pRouteAgent,
                                   cb, NULL, &groupList1, &boRecv ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to execute split ready on catalog, "
                       "rc = %d", rc ) ;
         if ( boRecv.empty() )
         {
            PD_LOG( PDERROR, "Failed to get task id from result msg" ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         taskID = (UINT64)(boRecv.at(0).getField( CAT_TASKID_NAME ).numberLong()) ;

         boSend = BSON( CAT_TASKID_NAME << (long long)taskID ) ;
         rc = msgBuildQueryMsg( &splitQueryBuffer, &splitQueryBufferSz,
                                CMD_ADMIN_PREFIX CMD_NAME_SPLIT, 0,
                                0, 0, -1, &boSend, NULL,
                                NULL, NULL ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build query message, rc: %d",
                      rc ) ;
         pSplitQuery                      = (MsgOpQuery *)splitQueryBuffer ;
         pSplitQuery->header.routeID.value= 0 ;
         pSplitQuery->header.TID          = cb->getTID () ;
         pSplitQuery->version             = cataInfo->getVersion() ;
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR,
                       "Exception when building split ready message: %s",
                       e.what() ) ;
      }
      /************************************************************************
       *           SHARDING START REQUEST                                     *
       ************************************************************************/
   retry :
      groupLst = groupList1 ;
      pSplitReq->header.opCode = MSG_BS_QUERY_REQ ;
      rc = executeOnDataGroup( (MsgHeader *)splitReadyBuffer,
                               groupLst, groupLstTmp, pRouteAgent,
                               cb, TRUE );
      if ( rc )
      {
         if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == rc &&
              !isNeedRefresh )
         {
            isNeedRefresh = TRUE;
            rc = rtnCoordGetCataInfo ( cb, strName, TRUE, cataInfo ) ;
            PD_RC_CHECK ( rc, PDERROR,
                          "Failed to get cata info for collection %s, rc = %d",
                          strName, rc ) ;
            pSplitReq->version = cataInfo->getVersion();
            goto retry ;
         }
         PD_LOG ( PDERROR, "Failed to execute split on data node, rc = %d",
                  rc ) ;
         goto cancel ;
      }

      if ( !async )
      {
         rtnCoordProcesserFactory *pFactory = pCoordcb->getProcesserFactory() ;
         rtnCoordCommand *pCmd = pFactory->getCommandProcesser(
                                 COORD_CMD_WAITTASK ) ;
         SDB_ASSERT( pCmd, "wait task command not found" ) ;
         rc = pCmd->execute( splitQueryBuffer, splitQueryBufferSz,
                             ppResultBuffer, cb, replyHeader, ppErrorObj ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else // return taskid to client
      {
         rtnContextDump *pContext = NULL ;
         rc = pRtncb->contextNew( RTN_CONTEXT_DUMP, (rtnContext**)&pContext,
                                  contextID, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to create context, rc: %d", rc ) ;
         rc = pContext->open( BSONObj(), BSONObj(), 1, 0 ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to open context, rc: %d", rc ) ;
         pContext->append( BSON( CAT_TASKID_NAME << (long long)taskID ) ) ;
         replyHeader.contextID = contextID ;
      }

   done :
      if ( splitReadyBuffer )
      {
         SDB_OSS_FREE ( splitReadyBuffer ) ;
      }
      if ( splitQueryBuffer )
      {
         SDB_OSS_FREE ( splitQueryBuffer ) ;
      }
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSP_EXE, rc ) ;
      return rc ;
   cancel :
      pSplitQuery->header.opCode       = MSG_CAT_SPLIT_CANCEL_REQ ;
      pSplitQuery->version             = cataInfo->getVersion();
      {
         INT32 rctmp = executeOnCataGroup ( (CHAR*)pSplitQuery,
                                            pRouteAgent, cb ) ;
         if ( rctmp )
         {
            PD_LOG( PDWARNING, "Failed to execute split cancel on catalog, "
                    "rc = %d", rctmp ) ;
            goto error ;
         }
      }

   error :
      if ( SDB_RTN_INVALID_HINT == rc )
      {
         rc = SDB_COORD_SPLIT_NO_SHDIDX ;
      }
      if ( -1 != contextID )
      {
         pRtncb->contextDelete( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSP_GETBOUNDRONDATA, "rtnCoordCMDSplit::_getBoundRecordOnData" )
   INT32 rtnCoordCMDSplit::_getBoundRecordOnData( const CHAR *cl,
                                                  const BSONObj &condition,
                                                  const BSONObj &hint,
                                                  INT32 flag,
                                                  INT64 skip,
                                                  CoordGroupList &groupList,
                                                  pmdEDUCB *cb,
                                                  BSONObj &shardingKey,
                                                  BSONObj &record )
   {
      PD_TRACE_ENTRY( SDB_RTNCOCMDSP_GETBOUNDRONDATA ) ;
      INT32 rc = SDB_OK ;
      BSONObj empty ;
      rtnContext *context = NULL ;
      rtnContextBuf buffObj ;
      BSONObj obj ;

      if ( !condition.okForStorage() )
      {
         PD_LOG( PDERROR, "Condition[%s] has invalid field name",
                 condition.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( condition.isEmpty() )
      {
         rc = rtnCoordNodeQuery( cl, condition, empty, empty,
                                 hint, skip, 1, groupList,
                                 cb, &context, NULL, flag ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to query from data group, rc = %d",
                       rc ) ;
         rc = context->getMore( -1, buffObj, cb ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         else
         {
            obj = BSONObj( buffObj.data() ) ;
         }
      }
      else
      {
         obj = condition ;
      }

      {
         PD_LOG ( PDINFO, "Split found record %s", obj.toString().c_str() ) ;
         ixmIndexKeyGen keyGen ( shardingKey ) ;
         BSONObjSet keys ;
         BSONObjSet::iterator keyIter ;
         rc = keyGen.getKeys ( obj, keys ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to extract keys\nkeyDef = %s\n"
                       "record = %s\nrc = %d", shardingKey.toString().c_str(),
                       obj.toString().c_str(), rc ) ;
         PD_CHECK ( keys.size() == 1, SDB_INVALID_SHARDINGKEY, error,
                    PDWARNING, "There must be a single key generate for "
                    "sharding\nkeyDef = %s\nrecord = %s\n",
                    shardingKey.toString().c_str(),
                    obj.toString().c_str() ) ;

         keyIter = keys.begin () ;
         record = (*keyIter).copy() ;

         /*{
            BSONObjIterator iter ( record ) ;
            while ( iter.more () )
            {
               BSONElement e = iter.next () ;
               PD_CHECK ( e.type() != Undefined, SDB_CLS_BAD_SPLIT_KEY,
                          error, PDERROR, "The split record does not contains "
                          "a valid key\nRecord: %s\nShardingKey: %s\n"
                          "SplitKey: %s", obj.toString().c_str(),
                          shardingKey.toString().c_str(),
                          record.toString().c_str() ) ;
            }
         }*/

        PD_LOG ( PDINFO, "Split found key %s", record.toString().c_str() ) ;
     }

   done:
      if ( NULL != context )
      {
         SINT64 contextID = context->contextID() ;
         pmdGetKRCB()->getRTNCB()->contextDelete( contextID, cb ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNCOCMDSP_GETBOUNDRONDATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSP__GETBOUNDBYC, "rtnCoordCMDSplit::_getBoundByCondition" )
   INT32 rtnCoordCMDSplit::_getBoundByCondition( const CHAR *cl,
                                                 const BSONObj &begin,
                                                 const BSONObj &end,
                                                 CoordGroupList &groupList,
                                                 pmdEDUCB *cb,
                                                 CoordCataInfoPtr &cataInfo,
                                                 BSONObj &lowBound,
                                                 BSONObj &upBound )
   {
      PD_TRACE_ENTRY( SDB_RTNCOCMDSP__GETBOUNDBYC ) ;
      INT32 rc = SDB_OK ;
      CoordGroupList grpTmp = groupList ;
      BSONObj shardingKey ;
      cataInfo->getShardingKey ( shardingKey ) ;
      PD_CHECK ( !shardingKey.isEmpty(), SDB_COLLECTION_NOTSHARD, error,
                  PDWARNING, "Collection must be sharded: %s", cl ) ;

      rc = _getBoundRecordOnData( cl, begin, BSONObj(),
                                  0, 0, grpTmp, cb,
                                  shardingKey, lowBound ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get begin bound:%d",rc ) ;
         goto error ;
      }

      if ( !end.isEmpty() )
      {
         grpTmp = groupList ;
         rc = _getBoundRecordOnData( cl, end, BSONObj(),
                                     0, 0, grpTmp, cb,
                                     shardingKey, upBound ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get end bound:%d",rc ) ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNCOCMDSP__GETBOUNDBYC, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSP__GETBOUNDBYP, "rtnCoordCMDSplit::_getBoundByPercent" )
   INT32 rtnCoordCMDSplit::_getBoundByPercent( const CHAR *cl,
                                               FLOAT64 percent,
                                               CoordCataInfoPtr &cataInfo,
                                               CoordGroupList &groupList,
                                               pmdEDUCB *cb,
                                               BSONObj &lowBound,
                                               BSONObj &upBound )
   {
      PD_TRACE_ENTRY( SDB_RTNCOCMDSP__GETBOUNDBYP ) ;
      INT32 rc = SDB_OK ;
      BSONObj shardingKey ;
      cataInfo->getShardingKey ( shardingKey ) ;
      CoordGroupList grpTmp = groupList ;
      PD_CHECK ( !shardingKey.isEmpty(), SDB_COLLECTION_NOTSHARD, error,
                 PDWARNING, "Collection must be sharded: %s", cl ) ;

      if ( 100.0 - percent < OSS_EPSILON )
      {
         rc = cataInfo->getGroupLowBound( grpTmp.begin()->second,
                                          lowBound ) ;
         PD_RC_CHECK( rc, PDERROR, "Get group[%d] low bound failed, rc: %d",
                      grpTmp.begin()->second, rc ) ;
      }
      else
      {
         UINT64 totalCount = 0 ;
         INT64 skipCount = 0 ;
         INT32 flag = 0 ;
         BSONObj hint ;
         while ( TRUE )
         {
            rc = getCLCount( cl, grpTmp, cb, totalCount ) ;
            PD_RC_CHECK( rc, PDERROR,
                         "Get collection count failed, rc: %d",
                         rc ) ;
            if ( 0 == totalCount )
            {
               rc = SDB_DMS_EOC ;
               PD_LOG( PDDEBUG, "collection[%s] is empty", cl ) ;
               break ;
            }

            skipCount = (INT64)(totalCount * ( 1 - percent/100 )) ;
            hint = BSON( "" << IXM_SHARD_KEY_NAME ) ;
            flag = FLG_QUERY_FORCE_HINT ;

            rc = _getBoundRecordOnData( cl, BSONObj(), hint,
                                        flag, skipCount, grpTmp,
                                        cb, shardingKey, lowBound ) ;
            if ( SDB_DMS_EOC == rc )
            {
               continue ;
            }
            else if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get bound from data:%d",rc ) ;
               goto error ;
            }
            else
            {
               break ;
            }
         }
      }

      upBound = BSONObj() ;
   done:
      PD_TRACE_EXITRC( SDB_RTNCOCMDSP__GETBOUNDBYP, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnCoordCmdWaitTask::execute( CHAR * pReceiveBuffer,
                                       SINT32 packSize,
                                       CHAR **ppResultBuffer,
                                       pmdEDUCB *cb,
                                       MsgOpReply &replyHeader,
                                       BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *pKRCB                   = pmdGetKRCB () ;
      CoordCB *pCoordcb                = pKRCB->getCoordCB () ;
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent () ;

      MsgHeader *pSrc   = ( MsgHeader* )pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode        = MSG_BS_QUERY_RES ;
      replyHeader.header.requestID     = pSrc->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID           = pSrc->TID ;
      replyHeader.contextID            = -1 ;
      replyHeader.flags                = SDB_OK ;
      replyHeader.numReturned          = 0 ;
      replyHeader.startFrom            = 0 ;

      pSrc->opCode                     = MSG_CAT_QUERY_TASK_REQ ;
      pSrc->TID                        = cb->getTID() ;

      while ( TRUE )
      {
         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }

         BOOLEAN isNeedRefresh = FALSE ;
         CoordGroupInfoPtr catGroupInfo ;
         REQUESTID_MAP sendNodes ;
         REPLY_QUE replyQue ;

      retry:
         rc = rtnCoordGetCatGroupInfo( cb, isNeedRefresh, catGroupInfo ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get cat group info, rc: %d",
                      rc ) ;
         rc = rtnCoordSendRequestToPrimary( pReceiveBuffer, catGroupInfo,
                                            sendNodes, pRouteAgent,
                                            MSG_ROUTE_CAT_SERVICE, cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to send msg to primary node, rc: %d",
                      rc ) ;
         rc = rtnCoordGetReply( cb, sendNodes, replyQue, MSG_CAT_QUERY_TASK_RSP,
                                TRUE, TRUE ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get reply, rc: %d", rc ) ;

         while ( !replyQue.empty() )
         {
            MsgOpReply *pReply = (MsgOpReply *)( replyQue.front() ) ;
            replyQue.pop() ;
            if ( SDB_OK == rc )
            {
               rc = pReply->flags ;
            }
            SDB_OSS_FREE( pReply ) ;
         }

         if ( ( SDB_RTN_NO_PRIMARY_FOUND == rc || SDB_CLS_NOT_PRIMARY == rc ) &&
              !isNeedRefresh )
         {
            isNeedRefresh = TRUE ;
            goto retry ;
         }
         else if ( SDB_DMS_EOC == rc || SDB_CAT_TASK_NOTFOUND == rc )
         {
            rc = SDB_OK ;
            break ;
         }
         ossSleep( OSS_ONE_SEC ) ;
      }

   done:
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   INT32 rtnCoordCmdListTask::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                       CHAR **ppResultBuffer, pmdEDUCB *cb,
                                       MsgOpReply &replyHeader,
                                       BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;

      MsgHeader *pSrc   = ( MsgHeader* )pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode        = MSG_BS_QUERY_RES ;
      replyHeader.header.requestID     = pSrc->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID           = pSrc->TID ;
      replyHeader.contextID            = -1 ;
      replyHeader.flags                = SDB_OK ;
      replyHeader.numReturned          = 0 ;
      replyHeader.startFrom            = 0 ;

      INT32 flag = 0 ;
      CHAR *pCollectionName = NULL ;
      INT64 numToSkip = 0 ;
      INT64 numToReturn = -1 ;
      CHAR *pQueryBuf = NULL ;
      CHAR *pSelectorBuf = NULL ;
      CHAR *pOrderbyBuf = NULL ;
      CHAR *pHintBuf = NULL ;
      INT64 contextID = -1 ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName, &numToSkip,
                            &numToReturn, &pQueryBuf, &pSelectorBuf,
                            &pOrderbyBuf, &pHintBuf ) ;
      PD_RC_CHECK( rc, PDERROR, "Extract query msg failed, rc: %d", rc ) ;

      try
      {
         BSONObj matcher( pQueryBuf ) ;
         BSONObj selector( pSelectorBuf ) ;
         BSONObj orderby( pOrderbyBuf ) ;
         BSONObj hint( pHintBuf ) ;

         rc = rtnCoordCataQuery( CAT_TASK_INFO_COLLECTION, selector, matcher,
                                 orderby, hint, flag, cb, numToSkip,
                                 numToReturn, contextID ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to query to catalog, rc: %d", rc ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      replyHeader.contextID = contextID ;

   done:
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   INT32 rtnCoordCmdCancelTask::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                         CHAR **ppResultBuffer, pmdEDUCB *cb,
                                         MsgOpReply &replyHeader,
                                         BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *pKRCB                   = pmdGetKRCB () ;
      CoordCB *pCoordcb                = pKRCB->getCoordCB () ;
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent () ;
      rtnCoordProcesserFactory *pFactory = pCoordcb->getProcesserFactory() ;
      BOOLEAN async                    = FALSE ;

      MsgHeader *pSrc   = ( MsgHeader* )pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode        = MSG_BS_QUERY_RES ;
      replyHeader.header.requestID     = pSrc->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID           = pSrc->TID ;
      replyHeader.contextID            = -1 ;
      replyHeader.flags                = SDB_OK ;
      replyHeader.numReturned          = 0 ;
      replyHeader.startFrom            = 0 ;

      CoordGroupList groupLst ;
      CoordGroupList groupLstSend ;
      INT32 rcTmp = SDB_OK ;

      CHAR *pQueryBuf = NULL ;
      rc = msgExtractQuery( pReceiveBuffer, NULL, NULL, NULL, NULL, &pQueryBuf,
                            NULL, NULL, NULL ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to extract query msg, rc: %d", rc ) ;

      try
      {
         BSONObj matcher( pQueryBuf ) ;
         rc = rtnGetBooleanElement( matcher, FIELD_NAME_ASYNC, async ) ;
         if ( SDB_FIELD_NOT_EXIST == rc )
         {
            rc = SDB_OK ;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                      FIELD_NAME_ASYNC, rc ) ;
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      pSrc->opCode                     = MSG_CAT_SPLIT_CANCEL_REQ ;
      pSrc->TID                        = cb->getTID() ;

      rc = executeOnCataGroup( pReceiveBuffer, pRouteAgent, cb, NULL,
                               &groupLst ) ;
      PD_RC_CHECK( rc, PDERROR, "Excute on catalog failed, rc: %d", rc ) ;

      pSrc->opCode                     = MSG_BS_QUERY_REQ ;
      rcTmp = executeOnDataGroup( (MsgHeader*)pReceiveBuffer, groupLst,
                                  groupLstSend, pRouteAgent, cb, TRUE, NULL ) ;
      if ( rcTmp )
      {
         PD_LOG( PDWARNING, "Failed to notify to data node, rc: %d", rcTmp ) ;
      }

      if ( !async )
      {
         rtnCoordCommand *pCmd = pFactory->getCommandProcesser(
                                           COORD_CMD_WAITTASK ) ;
         if ( !pCmd )
         {
            rc = SDB_SYS ;
            PD_LOG( PDERROR, "Command[%s] is null", COORD_CMD_WAITTASK ) ;
            goto error ;
         }
         rc = pCmd->execute( pReceiveBuffer, packSize, ppResultBuffer, cb,
                             replyHeader, ppErrorObj ) ;
         if ( rc )
         {
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDSTB_EXE, "rtnCoordCMDStatisticsBase::execute" )
   INT32 rtnCoordCMDStatisticsBase::execute( CHAR *pReceiveBuffer,
                                             SINT32 packSize,
                                             CHAR **ppResultBuffer,
                                             pmdEDUCB *cb,
                                             MsgOpReply &replyHeader,
                                             BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSTB_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      SDB_RTNCB *pRtncb                = pKrcb->getRTNCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

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

      CoordCataInfoPtr cataInfo;
      CoordGroupList dataNodeGroupLst;
      CoordGroupList sendGroupLst;
      rtnContextCoord *pContext = NULL ;
      std::string strCollectionName;
      MsgOpQuery *pQueryReq = ( MsgOpQuery *)pReceiveBuffer;
      pQueryReq->header.routeID.value = 0;
      pQueryReq->header.TID = cb->getTID();

      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      SINT64 numToSkip = 0;
      SINT64 numToReturn = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      BSONObj boOrderBy;
      BSONObj boQuery;
      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName,
                        &numToSkip, &numToReturn, &pQuery, &pFieldSelector,
                        &pOrderBy, &pHint );
      PD_RC_CHECK ( rc, PDERROR, "Execute failed, failed to parse query "
                    "request(rc=%d)", rc ) ;

      try
      {
         BSONObj boHint( pHint );
         boOrderBy = BSONObj( pOrderBy );
         boQuery = BSONObj( pQuery );

         BSONElement beCollectionName
                     = boHint.getField( FIELD_NAME_COLLECTION );
         PD_CHECK ( beCollectionName.type() == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "Execute failed, failed to get the field(%s)",
                    FIELD_NAME_COLLECTION ) ;
         strCollectionName = beCollectionName.str() ;
      }
      catch( std::exception &e )
      {
         PD_RC_CHECK ( rc, PDERROR, "Execute failed, occured unexpected "
                       "error:%s", e.what() ) ;
      }

      rc = executeQuery( pReceiveBuffer, boQuery,
                         BSONObj(),
                         boOrderBy,
                         strCollectionName.c_str(),
                         pRouteAgent,
                         cb, pContext ) ;
      PD_RC_CHECK( rc, PDERROR,
                  "query failed(rc=%d)", rc ) ;

      rc = generateResult( pContext, pRouteAgent, cb );
      PD_RC_CHECK( rc, PDERROR, "Failed to execute statistics(rc=%d)", rc ) ;

      replyHeader.contextID = pContext->contextID() ;
      pContext->reopen() ;

   done:
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSTB_EXE, rc ) ;
      return rc;
   error:
      if ( pContext )
      {
         pRtncb->contextDelete( pContext->contextID(), cb );
      }
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDGETIXS_GENRT, "rtnCoordCMDGetIndexes::generateResult" )
   INT32 rtnCoordCMDGetIndexes::generateResult( rtnContext *pContext,
                                 netMultiRouteAgent *pRouteAgent,
                                 pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDGETIXS_GENRT ) ;
      CoordIndexMap indexMap ;
      rtnContextBuf buffObj ;

      do
      {
         rc = pContext->getMore( 1, buffObj, cb ) ;
         if ( rc != SDB_OK )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK;
            }
            else
            {
               PD_LOG ( PDERROR, "Failed to get index data(rc=%d)", rc );
            }
            break;
         }

         try
         {
            BSONObj boTmp( buffObj.data() ) ;
            BSONElement beIndexDef = boTmp.getField(
                                       IXM_FIELD_NAME_INDEX_DEF );
            PD_CHECK ( beIndexDef.type() == Object, SDB_INVALIDARG, error,
                       PDERROR, "Get index failed, failed to get the field(%s)",
                       IXM_FIELD_NAME_INDEX_DEF ) ;

            BSONObj boIndexDef = beIndexDef.embeddedObject() ;
            BSONElement beIndexName = boIndexDef.getField( IXM_NAME_FIELD ) ;
            PD_CHECK ( beIndexName.type() == String, SDB_INVALIDARG, error,
                       PDERROR, "Get index failed, failed to get the field(%s)",
                       IXM_NAME_FIELD ) ;

            std::string strIndexName = beIndexName.valuestr() ;
            CoordIndexMap::iterator iter = indexMap.find( strIndexName ) ;
            if ( indexMap.end() == iter )
            {
               indexMap[ strIndexName ] = boTmp.copy() ;
            }
            else
            {
               BSONObjIterator newIter( boIndexDef );
               BSONObj boOldDef;
               BSONElement beOldDef =
                  iter->second.getField( IXM_FIELD_NAME_INDEX_DEF );
               PD_CHECK ( beOldDef.type() == Object, SDB_INVALIDARG, error,
                       PDERROR, "Get index failed, failed to get the field(%s)",
                       IXM_FIELD_NAME_INDEX_DEF ) ;
               boOldDef = beOldDef.embeddedObject();
               while( newIter.more() )
               {
                  BSONElement beTmp1 = newIter.next();
                  if ( 0 == ossStrcmp( beTmp1.fieldName(), "_id") )
                  {
                     continue;
                  }
                  BSONElement beTmp2 = boOldDef.getField( beTmp1.fieldName() );
                  if ( 0 != beTmp1.woCompare( beTmp2 ) )
                  {
                     PD_RC_CHECK( SDB_SYS, PDERROR,
                                 "Corrupted index(name:%s, "
                                 "define1:%s, define2:%s)",
                                 strIndexName.c_str(),
                                 beTmp1.toString().c_str(),
                                 beTmp2.toString().c_str() );
                  }
               }

               if ( boIndexDef.woCompare( iter->second ) != 0 )
               {
                  PD_LOG ( PDWARNING, "Corrupted index(%s)",
                           strIndexName.c_str() ) ;
               }
            }
         }
         catch ( std::exception &e )
         {
            PD_RC_CHECK( rc, PDERROR, "Failed to get index, occured unexpected"
                         "error:%s", e.what() ) ;
         }
      }while( SDB_OK == rc ) ;

      if ( rc != SDB_OK )
      {
         goto error;
      }

      {
         CoordIndexMap::iterator iterMap = indexMap.begin();
         while( iterMap != indexMap.end() )
         {
            rc = pContext->append( iterMap->second );
            PD_RC_CHECK( rc, PDERROR, "Failed to get index, append the data "
                         "failed(rc=%d)", rc ) ;
            ++iterMap;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOCMDGETIXS_GENRT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDGETCT_GENRT, "rtnCoordCMDGetCount::generateResult" )
   INT32 rtnCoordCMDGetCount::generateResult( rtnContext *pContext,
                                 netMultiRouteAgent *pRouteAgent,
                                 pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDGETCT_GENRT ) ;
      SINT64 totalCount = 0 ;
      rtnContextBuf buffObj ;

      do
      {
         rc = pContext->getMore( 1, buffObj, cb ) ;
         if ( rc != SDB_OK )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK;
            }
            else
            {
               PD_LOG ( PDERROR, "Failed to generate count result"
                        "get data failed(rc=%d)", rc );
            }
            break;
         }

         try
         {
            BSONObj boTmp( buffObj.data() );
            BSONElement beTotal = boTmp.getField( FIELD_NAME_TOTAL );
            PD_CHECK( beTotal.isNumber(), SDB_INVALIDARG, error,
                  PDERROR, "count failed, failed to get the field(%s)",
                  FIELD_NAME_TOTAL );
            totalCount += beTotal.number() ;
         }
         catch ( std::exception &e )
         {
            PD_RC_CHECK( rc, PDERROR,
                        "failed to generate count result,"
                        "occured unexpected error:%s",
                        e.what() );
         }
      }while( SDB_OK == rc ) ;

      if ( rc != SDB_OK )
      {
         goto error;
      }
      try
      {
         BSONObjBuilder bobResult ;
         bobResult.append( FIELD_NAME_TOTAL, totalCount ) ;
         BSONObj boResult = bobResult.obj() ;
         rc = pContext->append( boResult ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to generate count result,"
                      "append the data failed(rc=%d)", rc ) ;
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( rc, PDERROR, "Failed to generate count result,"
                      "occured unexpected error:%s", e.what() ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOCMDGETCT_GENRT, rc ) ;
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordCMDGetDatablocks::generateResult( rtnContext * pContext,
                                                   netMultiRouteAgent * pRouteAgent,
                                                   pmdEDUCB * cb )
   {
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCTCAGP_EXE, "rtnCoordCMDCreateCataGroup::execute" )
   INT32 rtnCoordCMDCreateCataGroup::execute( CHAR *pReceiveBuffer,
                                              SINT32 packSize,
                                              CHAR **ppResultBuffer,
                                              pmdEDUCB *cb,
                                              MsgOpReply &replyHeader,
                                              BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCTCAGP_EXE ) ;
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode        = MSG_BS_QUERY_RES ;
      replyHeader.header.requestID     = pHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID           = pHeader->TID ;
      replyHeader.contextID            = -1 ;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0 ;
      replyHeader.startFrom            = 0 ;

      INT32 flag = 0 ;
      CHAR *pCMDName = NULL ;
      SINT64 numToSkip = 0 ;
      SINT64 numToReturn = 0 ;
      CHAR *pQuery = NULL ;
      CHAR *pFieldSelector = NULL ;
      CHAR *pOrderBy = NULL ;
      CHAR *pHint = NULL ;
      BSONObj boNodeConfig;
      BSONObj boNodeInfo;
      const CHAR *pHostName = NULL;
      SINT32 retCode = 0 ;
      BSONObj boLocalSvc ;
      BSONObj boBackup = BSON( "Backup" << true ) ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip,
                            &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to parse create catalog-group "
                   "request(rc=%d)", rc ) ;

      rc = getNodeConf( pQuery, boNodeConfig ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get configure info(rc=%d)", rc ) ;

      rc = getNodeInfo( pQuery, boNodeInfo ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get node info(rc=%d)", rc ) ;

      try
      {
         BSONElement beHostName = boNodeInfo.getField( FIELD_NAME_HOST );
         PD_CHECK( beHostName.type()==String, SDB_INVALIDARG, error, PDERROR,
                   "Failed to get the field(%s)", FIELD_NAME_HOST );
         pHostName = beHostName.valuestr() ;

         BSONElement beLocalSvc = boNodeInfo.getField( PMD_OPTION_SVCNAME );
         PD_CHECK( beLocalSvc.type()==String, SDB_INVALIDARG, error, PDERROR,
                   "Failed to get the field(%s)", PMD_OPTION_SVCNAME );
         BSONObjBuilder bobLocalSvc ;
         bobLocalSvc.append( beLocalSvc ) ;
         boLocalSvc = bobLocalSvc.obj() ;
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                   "Failed to create catalog group, occured unexpected "
                   "error:%s", e.what() );
      }

      rc = rtnRemoteExec( SDBADD, pHostName, &retCode, &boNodeConfig,
                          &boNodeInfo ) ;
      rc = rc ? rc : retCode ;
      PD_RC_CHECK( rc, PDERROR, "remote node execute(configure) "
                   "failed(rc=%d)", rc ) ;

      rc = rtnRemoteExec( SDBSTART, pHostName, &retCode, &boLocalSvc ) ;
      rc = rc ? rc : retCode ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Remote node execute(start) failed(rc=%d)", rc ) ;
         rtnRemoteExec( SDBRM, pHostName, &retCode, &boNodeConfig, &boBackup ) ;
         goto error ;
      }

      {
         CoordVecNodeInfo cataList ;
         sdbGetCoordCB()->getLock( EXCLUSIVE ) ;
         sdbGetCoordCB()->getCatNodeAddrList( cataList ) ;
         pmdOptionsCB *optCB = pmdGetKRCB()->getOptionCB() ;

         for ( CoordVecNodeInfo::const_iterator itr = cataList.begin() ;
               itr != cataList.end() ;
               itr++ )
         {
            optCB->setCatAddr( itr->_host, itr->_service[
               MSG_ROUTE_CAT_SERVICE].c_str() ) ;
         }
         optCB->reflush2File() ;
         sdbGetCoordCB()->releaseLock( EXCLUSIVE ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCTCAGP_EXE, rc ) ;
      return rc;
   error:
      if ( rc != SDB_COORD_RECREATE_CATALOG )
      {
         sdbGetCoordCB()->getLock( EXCLUSIVE ) ;
         sdbGetCoordCB()->clearCatNodeAddrList() ;
         sdbGetCoordCB()->releaseLock( EXCLUSIVE ) ;

         CoordGroupInfo *pEmptyGroupInfo = NULL ;
         pEmptyGroupInfo = SDB_OSS_NEW CoordGroupInfo( CAT_CATALOG_GROUPID ) ;
         if ( NULL != pEmptyGroupInfo )
         {
            CoordGroupInfoPtr groupInfo( pEmptyGroupInfo ) ;
            sdbGetCoordCB()->updateCatGroupInfo( groupInfo ) ;
         }
      }
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCTCAGP_GETNDCF, "rtnCoordCMDCreateCataGroup::getNodeConf" )
   INT32 rtnCoordCMDCreateCataGroup::getNodeConf( CHAR *pQuery,
                                                  BSONObj &boNodeConfig )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCTCAGP_GETNDCF ) ;

      try
      {
         std::string strCataHostName ;
         std::string strSvcName ;
         std::string strCataSvc ;
         CoordVecNodeInfo cataNodeLst ;

         BSONObj boInput( pQuery ) ;
         BSONObjBuilder bobNodeConf ;
         BSONObjIterator iter( boInput ) ;

         while ( iter.more() )
         {
            BSONElement beField = iter.next() ;
            std::string strFieldName( beField.fieldName() ) ;

            if ( strFieldName == FIELD_NAME_HOST )
            {
               strCataHostName = beField.str();
               continue;
            }
            else if ( strFieldName == FIELD_NAME_GROUPNAME ||
                      strFieldName == PMD_OPTION_ROLE ||
                      strFieldName == PMD_OPTION_CATALOG_ADDR )
            {
               continue;
            }
            else if ( strFieldName == PMD_OPTION_CATANAME )
            {
               strCataSvc = beField.str() ;
            }
            else if ( strFieldName == PMD_OPTION_SVCNAME )
            {
               strSvcName = beField.str() ;
            }

            bobNodeConf.append( beField ) ;
         }

         if ( strSvcName.empty() )
         {
            PD_LOG( PDERROR, "Service name can't be empty" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         if ( strCataSvc.empty() )
         {
            UINT16 svcPort = 0 ;
            ossGetPort( strSvcName.c_str(), svcPort ) ;
            CHAR szPort[ 10 ] = { 0 } ;
            ossItoa( svcPort + MSG_ROUTE_CAT_SERVICE , szPort, 10 ) ;
            strCataSvc = szPort ;
         }

         bobNodeConf.append ( PMD_OPTION_ROLE, SDB_ROLE_CATALOG_STR ) ;

         sdbGetCoordCB()->getLock( EXCLUSIVE ) ;
         sdbGetCoordCB()->getCatNodeAddrList( cataNodeLst ) ;

         if ( cataNodeLst.size() > 0 )
         {
            rc = SDB_COORD_RECREATE_CATALOG ;
            PD_LOG( PDERROR, "Repeat to create catalog-group" ) ;
            sdbGetCoordCB()->releaseLock( EXCLUSIVE ) ;
            goto error ;
         }
         else
         {
            std::string strCataNodeLst = strCataHostName + ":" + strCataSvc ;
            MsgRouteID routeID ;
            routeID.columns.groupID = CATALOG_GROUPID ;
            routeID.columns.nodeID = CATA_NODE_ID_BEGIN ;
            routeID.columns.serviceID = MSG_ROUTE_CAT_SERVICE ;
            sdbGetCoordCB()->addCatNodeAddr( routeID, strCataHostName.c_str(),
                                             strCataSvc.c_str() ) ;
            sdbGetCoordCB()->releaseLock( EXCLUSIVE ) ;

            bobNodeConf.append( PMD_OPTION_CATALOG_ADDR, strCataNodeLst ) ;
         }

         boNodeConfig = bobNodeConf.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Occured unexpected error:%s", e.what() ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCTCAGP_GETNDCF, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDCTCAGP_GETNDINFO, "rtnCoordCMDCreateCataGroup::getNodeInfo" )
   INT32 rtnCoordCMDCreateCataGroup::getNodeInfo( CHAR *pQuery, BSONObj &boNodeInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCTCAGP_GETNDINFO ) ;

      try
      {
         BSONObj boConf( pQuery ) ;
         BSONObjBuilder bobNodeInfo ;
         BSONElement beHostName = boConf.getField( FIELD_NAME_HOST ) ;
         PD_CHECK( beHostName.type()==String, SDB_INVALIDARG, error, PDERROR,
                  "Failed to get the field(%s)", FIELD_NAME_HOST ) ;
         bobNodeInfo.append( beHostName ) ;

         BSONElement beDBPath = boConf.getField( PMD_OPTION_DBPATH ) ;
         PD_CHECK( beDBPath.type()==String, SDB_INVALIDARG, error, PDERROR,
                   "Failed to get the field(%s)", PMD_OPTION_DBPATH );
         bobNodeInfo.append( beDBPath ) ;

         BSONElement beLocalSvc = boConf.getField( PMD_OPTION_SVCNAME ) ;
         PD_CHECK( beLocalSvc.type()==String, SDB_INVALIDARG, error, PDERROR,
                   "Failed to get the field(%s)", PMD_OPTION_SVCNAME ) ;
         bobNodeInfo.append( beLocalSvc ) ;

         BSONElement beReplSvc = boConf.getField( PMD_OPTION_REPLNAME );
         if ( beReplSvc.type() == String )
         {
            bobNodeInfo.append( beReplSvc ) ;
         }

         BSONElement beShardSvc = boConf.getField( PMD_OPTION_SHARDNAME ) ;
         if ( beShardSvc.type() == String )
         {
            bobNodeInfo.append( beShardSvc ) ;
         }

         BSONElement beCataSvc = boConf.getField( PMD_OPTION_CATANAME ) ;
         if ( beCataSvc.type() == String )
         {
            bobNodeInfo.append( beCataSvc ) ;
         }
         boNodeInfo = bobNodeInfo.obj() ;
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                   "Occured unexpected error:%s", e.what() ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCTCAGP_GETNDINFO, rc ) ;
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordCMDTraceStart::execute( CHAR *pReceiveBuffer,
                                         SINT32 packSize,
                                         CHAR **ppResultBuffer,
                                         pmdEDUCB *cb,
                                         MsgOpReply &replyHeader,
                                         BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag;
      CHAR *pCMDName;
      SINT64 numToSkip;
      SINT64 numToReturn;
      CHAR *pQuery;
      CHAR *pFieldSelector;
      CHAR *pOrderBy;
      CHAR *pHint;
      _rtnTraceStart tracestart ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip,
                            &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint );
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to extract query, rc = %d", rc ) ;
      rc = tracestart.init ( flag, numToSkip, numToReturn, pQuery,
                             pFieldSelector, pOrderBy, pHint ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to init tracestart, rc = %d", rc ) ;
      rc = tracestart.doit ( cb, NULL, NULL, NULL, 0, NULL ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to run tracestart, rc = %d", rc ) ;
   done:
      return rc;
   error:
      replyHeader.flags = rc;
      goto done;
   }

   INT32 rtnCoordCMDTraceResume::execute( CHAR *pReceiveBuffer,
                                         SINT32 packSize,
                                         CHAR **ppResultBuffer,
                                         pmdEDUCB *cb,
                                         MsgOpReply &replyHeader,
                                         BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag;
      CHAR *pCMDName;
      SINT64 numToSkip;
      SINT64 numToReturn;
      CHAR *pQuery;
      CHAR *pFieldSelector;
      CHAR *pOrderBy;
      CHAR *pHint;
      _rtnTraceResume traceResume ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip,
                            &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint );
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to extract query, rc = %d", rc ) ;
      rc = traceResume.init ( flag, numToSkip, numToReturn, pQuery,
                             pFieldSelector, pOrderBy, pHint ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to init tracestart, rc = %d", rc ) ;
      rc = traceResume.doit ( cb, NULL, NULL, NULL, 0, NULL ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to run tracestart, rc = %d", rc ) ;
   done:
      return rc;
   error:
      replyHeader.flags = rc;
      goto done;
   }


   INT32 rtnCoordCMDTraceStop::execute( CHAR *pReceiveBuffer,
                                        SINT32 packSize,
                                        CHAR **ppResultBuffer,
                                        pmdEDUCB *cb,
                                        MsgOpReply &replyHeader,
                                        BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag;
      CHAR *pCMDName;
      SINT64 numToSkip;
      SINT64 numToReturn;
      CHAR *pQuery;
      CHAR *pFieldSelector;
      CHAR *pOrderBy;
      CHAR *pHint;
      _rtnTraceStop tracestop ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip,
                            &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint );
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to extract query, rc = %d", rc ) ;
      rc = tracestop.init ( flag, numToSkip, numToReturn, pQuery,
                            pFieldSelector, pOrderBy, pHint ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to init tracestop, rc = %d", rc ) ;
      rc = tracestop.doit ( cb, NULL, NULL, NULL, 0, NULL ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to run tracestop, rc = %d", rc ) ;
   done:
      return rc;
   error:
      replyHeader.flags = rc;
      goto done;
   }

   INT32 rtnCoordCMDTraceStatus::execute( CHAR *pReceiveBuffer,
                                          SINT32 packSize,
                                          CHAR **ppResultBuffer,
                                          pmdEDUCB *cb,
                                          MsgOpReply &replyHeader,
                                          BSONObj **ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      SDB_RTNCB *pRtncb                = pKrcb->getRTNCB();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag;
      CHAR *pCMDName;
      SINT64 numToSkip;
      SINT64 numToReturn;
      CHAR *pQuery;
      CHAR *pFieldSelector;
      CHAR *pOrderBy;
      CHAR *pHint;
      _rtnTraceStatus tracestatus ;
      INT64 contextID ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip,
                            &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint );
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to extract query, rc = %d", rc ) ;
      rc = tracestatus.init ( flag, numToSkip, numToReturn, pQuery,
                            pFieldSelector, pOrderBy, pHint ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to init tracestop, rc = %d", rc ) ;
      rc = tracestatus.doit ( cb, NULL, pRtncb, NULL, 0, &contextID ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to run tracestop, rc = %d", rc ) ;
   done:
      replyHeader.flags = rc;
      replyHeader.contextID = contextID ;
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordCMDExpConfig::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                        CHAR **ppResultBuffer,
                                        pmdEDUCB *cb, MsgOpReply &replyHeader,
                                        BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *pKrcb =  pmdGetKRCB();
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      CHAR *query = NULL ;
      CHAR *selector = NULL ;
      CHAR *orderby = NULL ;
      CHAR *hint = NULL ;
      INT32 flag = 0 ;
      CHAR *collectionName = NULL;
      SINT64 numToSkip = 0;
      SINT64 numToReturn = 0;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &collectionName,
                            &numToSkip, &numToReturn, &query,
                            &selector, &orderby, &hint );
      PD_RC_CHECK ( rc, PDERROR,
                  "execute failed, failed to parse query request(rc=%d)",
                  rc );

      try
      {
         BSONObj param( query ) ;
         BSONElement ele ;
         ele = param.getField( FIELD_NAME_EXPORTCONF_GLOBAL ) ;
         if ( ele.eoo() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "invalid arg:%s",param.toString().c_str() ) ;
            goto error ;
         }
         else if ( Bool == ele.type() )
         {
            if ( ele.Bool() )
            {
               ROUTE_SET nodeSet ;
               rc = _getNodesSet( cb, nodeSet ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "failed to get nodes:%d",rc ) ;
                  rc = SDB_SYS ;
                  goto error ;
               }

               {
               REQUESTID_MAP completed ;
               ROUTE_RC_MAP uncompleted ;
               CoordCB *coordcb = pmdGetKRCB()->getCoordCB();
               netMultiRouteAgent *routeAgent = coordcb->getRouteAgent();
               REPLY_QUE replyQueue ;
               pHeader->TID = cb->getTID() ;
               rtnCoordSendRequestToNodes( pReceiveBuffer,
                                           nodeSet,
                                           routeAgent,
                                           cb,
                                           completed,
                                           uncompleted ) ;
               if ( !uncompleted.empty() )
               {
                  PD_LOG( PDERROR, "Not all requests are successful,"
                          "failed num:%d", uncompleted.size() ) ;
                  rc = SDB_RTN_EXPORTCONF_NOT_COMPLETE ;
               }

               if ( !completed.empty() )
               {
                  rc = rtnCoordGetReply(cb, completed, replyQueue,
                                        MSG_BS_QUERY_RES ) ;
                  CHAR *queueItr = NULL ;
                  while ( !replyQueue.empty() )
                  {
                     queueItr = replyQueue.front() ;
                     MsgOpReply *reply = ( MsgOpReply * )queueItr ;
                     if ( SDB_OK  != reply->flags )
                     {
                        rc = SDB_RTN_EXPORTCONF_NOT_COMPLETE ;
                     }
                     SAFE_OSS_FREE( queueItr ) ;
                     replyQueue.pop() ;
                  }
                  if ( SDB_OK != rc )
                  {
                     PD_LOG( PDERROR, "failed to get reply:%d",rc ) ;
                     goto error ;
                  }
               }
               }
            }
            else
            {
               rc = pKrcb->getOptionCB()->reflush2File() ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "failed to export configration:%d",rc ) ;
                  goto error ;
               }
            }
         }
         else
         {
            PD_LOG( PDERROR, "unrecognized param:%s",
                    param.toString().c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      catch (std::exception &e )
      {
         PD_RC_CHECK ( rc, PDERROR,
                     "unexpected err happened:%s",
                     e.what() );
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      replyHeader.flags = rc ;
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnCoordCMDExpConfig::_getNodesSet( pmdEDUCB *cb, ROUTE_SET &nSet )
   {
      INT32 rc = SDB_OK ;
      GROUP_VEC grpVec ;
      rc = rtnCoordGetAllGroupList( cb, grpVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get group list:%d", rc ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      for ( GROUP_VEC::const_iterator itr = grpVec.begin();
            itr != grpVec.end();
            itr++ )
      {
      MsgRouteID routeID ;
      routeID.value = MSG_INVALID_ROUTEID ;
      clsGroupItem *grp = (*itr)->getGroupItem() ;
      routeID.columns.groupID = grp->groupID() ;
      const VEC_NODE_INFO *nodesInfo = grp->getNodes() ;
      for ( VEC_NODE_INFO::const_iterator itrn = nodesInfo->begin();
            itrn != nodesInfo->end();
            itrn++ )
      {
         routeID.columns.nodeID = itrn->_id.columns.nodeID ;
         routeID.columns.serviceID = MSG_ROUTE_SHARD_SERVCIE ;
         nSet.insert( routeID.value ) ;
      }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnCoordCMDSnapShotBase::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                          CHAR **ppResultBuffer, pmdEDUCB *cb,
                                          MsgOpReply &replyHeader,
                                          BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      INT32 objNum = 0;
      BSONObj objs;
      CHAR *pCMDName = NULL;
      CHAR *pObjsBuffer = NULL;
      INT64 contextID = -1;
      MsgHeader *pHeader = (MsgHeader *)pReceiveBuffer;
      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
      BSONObj selector ;

      replyHeader.contextID = -1;
      replyHeader.flags = SDB_OK;
      replyHeader.numReturned = 0;
      replyHeader.startFrom = 0;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode = MSG_BS_QUERY_RES;
      replyHeader.header.requestID = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID = pHeader->TID;

      rc = generateAggrObjs( pReceiveBuffer, pObjsBuffer,
                             objNum, pCMDName, selector );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to generate aggregation object(rc=%d)",
                   rc );
      SDB_ASSERT( pObjsBuffer != NULL, "pObjsBuffer can't be NULL!" );

      try
      {
         objs = BSONObj( pObjsBuffer );
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( rc, PDERROR,
                      "failed to execute snapshot, received unexpecte error:%s",
                      e.what() );
      }

      rc = openContext( objs, objNum, selector, cb, contextID ) ;
      PD_RC_CHECK( rc, PDERROR,
                   "failed to build snapshot context(rc=%d)",
                   rc );
      replyHeader.contextID = contextID;
   done:
      SAFE_OSS_FREE( pObjsBuffer );
      return rc;
   error:
      replyHeader.flags = rc;
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done;
   }

   INT32 rtnCoordCMDSnapShotBase::parseMatcher( BSONObj &query,
                                                BSONObj &nodesMatcher,
                                                BSONObj &newMatcher )
   {
      INT32 rc = SDB_OK;
      try
      {
         BSONObjBuilder matcherBuilder;
         BSONObjBuilder nodesCondBuilder;
         BSONObjIterator iter( query );
         while( iter.more() )
         {
            BSONElement beField = iter.next();
            if ( 0 == ossStrcmp( beField.fieldName(),
                                 CAT_GROUPID_NAME )
               || 0 == ossStrcmp( beField.fieldName(),
                                 FIELD_NAME_GROUPNAME ))
            {
               nodesCondBuilder.append( beField );
            }
            else if ( 0 == ossStrcmp( beField.fieldName(),
                                 CAT_NODEID_NAME )
               && beField.isNumber() )
            {
               nodesCondBuilder.append( beField );
            }
            else if ( 0 == ossStrcmp( beField.fieldName(),
                                 FIELD_NAME_HOST )
               && beField.type() == String )
            {
               nodesCondBuilder.append( beField );
            }
            else if ( 0 == ossStrcmp( beField.fieldName(),
                                 PMD_OPTION_SVCNAME )
               && beField.type() == String )
            {
               nodesCondBuilder.append( beField );
            }
            else
            {
               matcherBuilder.append( beField );
            }
         }
         newMatcher = matcherBuilder.obj();
         nodesMatcher = nodesCondBuilder.obj();
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                      "received unexpected error:%s",
                      e.what() );
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordCMDSnapShotBase::generateAggrObjs( CHAR *pInputBuffer,
                                                    CHAR *&pOutputBuffer,
                                                    INT32 &objNum,
                                                    CHAR *&pCLName,
                                                    BSONObj &selector   )
   {
      INT32 rc = SDB_OK;

      BSONObj nodesMatcher;
      BSONObj newMatcher;
      BSONObj orderBy;

      INT32 flag = 0;
      SINT64 numToSkip = 0;
      SINT64 numToReturn = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      INT32 addObjNum = 0;
      INT32 bufUsed = 0;
      INT32 bufSize = 0;

      rc = msgExtractQuery( pInputBuffer, &flag, &pCLName, &numToSkip,
                           &numToReturn, &pQuery, &pFieldSelector,
                           &pOrderBy, &pHint );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to parse request message(rc=%d)",
                   rc );

      pOutputBuffer = (CHAR *)SDB_OSS_MALLOC( RTNCOORD_ALLO_UNIT_SIZE );
      PD_CHECK( pOutputBuffer != NULL, SDB_OOM, error, PDERROR,
                "malloc failed(size=%d)", RTNCOORD_ALLO_UNIT_SIZE );
      bufSize = RTNCOORD_ALLO_UNIT_SIZE;

      try
      {
         BSONObj query( pQuery );
         if ( !query.isEmpty() )
         {
            BSONObj boNodes;
            BSONObj boMatcher;
            rc = parseMatcher( query, boNodes, boMatcher );
            PD_RC_CHECK( rc, PDERROR,
                        "failed to parse the matcher(rc=%d)", rc );
            if ( !boNodes.isEmpty() )
            {
               nodesMatcher = BSON( AGGR_MATCH_PARSER_NAME << boNodes );
            }
            if ( !boMatcher.isEmpty() )
            {
               newMatcher = BSON( AGGR_MATCH_PARSER_NAME << boMatcher );
            }
         }

         selector = BSONObj( pFieldSelector );

         BSONObj boOrderBy( pOrderBy );
         if ( !boOrderBy.isEmpty() )
         {
            orderBy = BSON( AGGR_SORT_PARSER_NAME << boOrderBy );
         }
      }
      catch( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                     "received unexpected error:%s",
                     e.what() );
      }
      if ( !nodesMatcher.isEmpty() )
      {
         rc = appendObj( nodesMatcher, pOutputBuffer, bufSize, bufUsed );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to append nodes-matcher objs(rc=%d)",
                        rc );
         ++addObjNum;
      }

      rc = appendAggrObjs( pOutputBuffer, bufSize, addObjNum, bufUsed );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to append aggregation operation objs(rc=%d)",
                   rc );

      if ( !newMatcher.isEmpty() )
      {
         rc = appendObj( newMatcher, pOutputBuffer, bufSize, bufUsed );
         PD_RC_CHECK( rc, PDERROR,
                      "failed to append matcher objs(rc=%d)",
                      rc );
         ++addObjNum;
      }

      if ( !orderBy.isEmpty() )
      {
         rc = appendObj( orderBy, pOutputBuffer, bufSize, bufUsed );
         PD_RC_CHECK( rc, PDERROR,
                      "failed to append orderBy objs(rc=%d)",
                      rc );
         ++addObjNum;
      }

/*
      if ( !selector.isEmpty() )
      {
         rc = appendObj( selector, pOutputBuffer, bufSize, bufUsed );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to append selector objs(rc=%d)",
                        rc );
         ++addObjNum;
      }
*/
      objNum = addObjNum;

   done:
      return rc;
   error:
      selector = BSONObj() ;
      SAFE_OSS_FREE( pOutputBuffer );
      goto done;
   }

   INT32 rtnCoordCMDSnapShotBase::appendObjs( const CHAR *pInputBuffer,
                                             CHAR *&pOutputBuffer,
                                             INT32 &bufferSize,
                                             INT32 &addObjNum,
                                             INT32 &bufUsed )
   {
   #define RTNCOORD_OBJS_BUF_SIZE      (4096+1)
      INT32 rc = SDB_OK;
      SDB_ASSERT( strlen( pInputBuffer ) < RTNCOORD_OBJS_BUF_SIZE,
                  "invalid input length" );
      CHAR szBuffer[ RTNCOORD_OBJS_BUF_SIZE ] = {0};
      while ( *pInputBuffer != '\0' )
      {
         rc = rtnCoordReadALine( pInputBuffer, szBuffer );
         PD_RC_CHECK( rc, PDERROR, "read failed(rc=%d)", rc );
         if ( strlen(szBuffer) != 0 )
         {
            try
            {
               BSONObj obj;
               rc = fromjson( szBuffer, obj );
               PD_RC_CHECK( rc, PDERROR,
                           "failed to parsed the json(rc=%d)",
                           rc );
               rc = appendObj( obj, pOutputBuffer, bufferSize, bufUsed );
               PD_RC_CHECK( rc, PDERROR,
                            "failed to append the obj(rc=%d)",
                            rc );
               ++addObjNum;
            }
            catch ( std::exception &e )
            {
               PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                            "received unexpected error:%s",
                            e.what() );
            }
         }

         if ( '\0' == *pInputBuffer )
         {
            break;
         }
         ++pInputBuffer;
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordCMDSnapShotBase::appendObj( bson::BSONObj &obj,
                                             CHAR *&pOutputBuffer,
                                             INT32 &bufferSize,
                                             INT32 &bufUsed )
   {
      INT32 rc = SDB_OK;
      INT32 bufEnd = bufUsed;
      INT32 curUsedSize = 0;
      try
      {
         bufEnd = ossRoundUpToMultipleX( bufEnd, 4 );
         curUsedSize = bufEnd + obj.objsize();
         if ( curUsedSize > bufferSize )
         {
            INT32 newBufSize = bufferSize;
            while ( newBufSize < curUsedSize )
            {
               newBufSize += RTNCOORD_ALLO_UNIT_SIZE;
            }
            CHAR *pOrgBuff = pOutputBuffer ;
            pOutputBuffer = (CHAR *)SDB_OSS_REALLOC( pOutputBuffer,
                                                     newBufSize ) ;
            if ( !pOutputBuffer )
            {
               PD_LOG( PDERROR, "Failed to realloc %d bytes memory",
                       newBufSize ) ;
               pOutputBuffer = pOrgBuff ;
               rc = SDB_OOM ;
               goto error ;
            }
            bufferSize = newBufSize;
         }
         ossMemcpy( pOutputBuffer + bufEnd, obj.objdata(),
                     obj.objsize() );
         bufUsed = curUsedSize;
      }
      catch( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                      "received unexpected error:%s",
                      e.what() );
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 rtnCoordCMDSnapShotBase::openContext( BSONObj &objs,
                                               INT32 objNum,
                                               const BSONObj &selector,
                                               pmdEDUCB *cb,
                                               SINT64 &contextID )
   {
      INT32 rc = SDB_OK ;
      rtnContextDump *context = NULL ;
      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
      SINT64 aggrContextID = -1 ;
      BSONObj obj ;
      rc = pmdGetKRCB()->getAggrCB()->build( objs,
                                             objNum,
                                             getIntrCMDName(),
                                             cb, aggrContextID );
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to build context:%d", rc ) ;
         goto error ;
      }

      rc = rtnCB->contextNew ( RTN_CONTEXT_DUMP, (rtnContext**)&context,
                               contextID, cb ) ;
      if ( SDB_OK != rc )
      {
         context = NULL ;
         PD_LOG ( PDERROR, "Failed to create new context" ) ;
         goto error ;
      }

      rc = context->open( selector, BSONObj() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open dump context:%d", rc ) ;
         goto error ;
      }

      {
      rtnDataSet ds( aggrContextID, cb ) ;
      do
      {
         rc = ds.next( obj ) ;
         if ( SDB_OK == rc )
         {
            rc = context->monAppend( obj ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to append obj to context:%d", rc ) ;
               goto error ;
            }
         }
         else if ( SDB_DMS_EOC == rc )
         {
            aggrContextID = -1 ;
            rc = SDB_OK ;
            break ;
         }
         else
         {
            PD_LOG( PDERROR, "failed to get next obj:%d", rc ) ;
            goto error ;
         }
      } while ( TRUE ) ;
      }
   done:
      if ( -1 != aggrContextID )
      {
         rtnCB->contextDelete ( aggrContextID, cb ) ;
         aggrContextID = -1 ;
      }
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }


   INT32 rtnCoordCMDSnapshotDataBase::appendAggrObjs( CHAR *&pOutputBuffer,
                                                      INT32 &bufferSize,
                                                      INT32 &addObjNum,
                                                      INT32 &bufUsed )
   {
      return appendObjs( RTNCOORD_SNAPSHOTDB_INPUT, pOutputBuffer,
                        bufferSize, addObjNum, bufUsed );
   }

   const CHAR * rtnCoordCMDSnapshotDataBase::getIntrCMDName()
   {
      return COORD_CMD_SNAPSHOTDBINTR;
   }

   INT32 rtnCoordCMDSnapshotSystem::appendAggrObjs( CHAR *&pOutputBuffer,
                                                      INT32 &bufferSize,
                                                      INT32 &addObjNum,
                                                      INT32 &bufUsed )
   {
      return appendObjs( RTNCOORD_SNAPSHOTSYS_INPUT, pOutputBuffer,
                        bufferSize, addObjNum, bufUsed );
   }

   const CHAR * rtnCoordCMDSnapshotSystem::getIntrCMDName()
   {
      return COORD_CMD_SNAPSHOTSYSINTR;
   }

   INT32 rtnCoordCMDSnapshotCollections::appendAggrObjs( CHAR *&pOutputBuffer,
                                                      INT32 &bufferSize,
                                                      INT32 &addObjNum,
                                                      INT32 &bufUsed )
   {
      return appendObjs( RTNCOORD_SNAPSHOTCL_INPUT, pOutputBuffer,
                         bufferSize, addObjNum, bufUsed ) ;
   }

   const CHAR * rtnCoordCMDSnapshotCollections::getIntrCMDName()
   {
      return COORD_CMD_SNAPSHOTCLINTR;
   }

   INT32 rtnCoordCMDSnapshotSpaces::appendAggrObjs( CHAR *&pOutputBuffer,
                                                      INT32 &bufferSize,
                                                      INT32 &addObjNum,
                                                      INT32 &bufUsed )
   {
      return appendObjs( RTNCOORD_SNAPSHOTCS_INPUT, pOutputBuffer,
                        bufferSize, addObjNum, bufUsed );
   }

   const CHAR * rtnCoordCMDSnapshotSpaces::getIntrCMDName()
   {
      return COORD_CMD_SNAPSHOTCSINTR;
   }

   INT32 rtnCoordCMDSnapshotContexts::appendAggrObjs( CHAR *&pOutputBuffer,
                                                      INT32 &bufferSize,
                                                      INT32 &addObjNum,
                                                      INT32 &bufUsed )
   {
      return appendObjs( RTNCOORD_SNAPSHOTCONTEXTS_INPUT, pOutputBuffer,
                        bufferSize, addObjNum, bufUsed );
   }

   const CHAR * rtnCoordCMDSnapshotContexts::getIntrCMDName()
   {
      return COORD_CMD_SNAPSHOTCTXINTR;
   }

   INT32 rtnCoordCMDSnapshotContextsCur::appendAggrObjs( CHAR *&pOutputBuffer,
                                                      INT32 &bufferSize,
                                                      INT32 &addObjNum,
                                                      INT32 &bufUsed )
   {
      return appendObjs( RTNCOORD_SNAPSHOTCONTEXTSCUR_INPUT, pOutputBuffer,
                        bufferSize, addObjNum, bufUsed );
   }

   const CHAR * rtnCoordCMDSnapshotContextsCur::getIntrCMDName()
   {
      return COORD_CMD_SNAPSHOTCTXCURINTR;
   }

   INT32 rtnCoordCMDSnapshotSessions::appendAggrObjs( CHAR *&pOutputBuffer,
                                                      INT32 &bufferSize,
                                                      INT32 &addObjNum,
                                                      INT32 &bufUsed )
   {
      return appendObjs( RTNCOORD_SNAPSHOTSESS_INPUT, pOutputBuffer,
                        bufferSize, addObjNum, bufUsed );
   }

   const CHAR * rtnCoordCMDSnapshotSessions::getIntrCMDName()
   {
      return COORD_CMD_SNAPSHOTSESSINTR;
   }

   INT32 rtnCoordCMDSnapshotSessionsCur::appendAggrObjs( CHAR *&pOutputBuffer,
                                                      INT32 &bufferSize,
                                                      INT32 &addObjNum,
                                                      INT32 &bufUsed )
   {
      return appendObjs( RTNCOORD_SNAPSHOTSESSCUR_INPUT, pOutputBuffer,
                        bufferSize, addObjNum, bufUsed );
   }

   const CHAR * rtnCoordCMDSnapshotSessionsCur::getIntrCMDName()
   {
      return COORD_CMD_SNAPSHOTSESSCURINTR;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOORDCMDSSCATA_BUILDREQ, "rtnCoordCMDSnapshotCata::buildQueryRequest" )
   INT32 rtnCoordCMDSnapshotCata::buildQueryRequest( CHAR *pIntput,
                                                     pmdEDUCB *cb,
                                                     CHAR **ppOutput )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOORDCMDSSCATA_BUILDREQ ) ;
      do
      {
         INT32 flag = 0;
         CHAR *pCollectionName = NULL;
         SINT64 numToSkip = 0;
         SINT64 numToReturn = 0;
         CHAR *pQuery = NULL;
         CHAR *pFieldSelector = NULL;
         CHAR *pOrderBy = NULL;
         CHAR *pHint = NULL;
         rc = msgExtractQuery( pIntput, &flag, &pCollectionName,
                              &numToSkip, &numToReturn, &pQuery, 
                              &pFieldSelector, &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "failed to parse query request(rc=%d)",
                     rc );
            break;
         }
         INT32 bufferSize = 0;
         BSONObj query;
         BSONObj fieldSelector;
         BSONObj orderBy;
         BSONObj hint;
         try
         {
            query = BSONObj ( pQuery );
            orderBy = BSONObj ( pOrderBy );
            hint = BSONObj ( pHint );
            fieldSelector = BSONObj( pFieldSelector );
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR,
                     "occured unexpected error:%s",
                     e.what() );
            break;
         }
         rc = msgBuildQueryMsg( ppOutput, &bufferSize,
                                CAT_COLLECTION_INFO_COLLECTION,
                                flag, 0, numToSkip, numToReturn,
                                &query, &fieldSelector,
                                &orderBy, &hint ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to build the query message(rc=%d)",
                     rc );
            break;
         }
         MsgOpQuery *pQueryMsg = (MsgOpQuery *)(*ppOutput);
         pQueryMsg->header.routeID.value = 0;
         pQueryMsg->header.TID = cb->getTID();
      }while( FALSE );
      PD_TRACE_EXITRC ( SDB_RTNCOORDCMDSSCATA_BUILDREQ, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOORDCMDCRTPROCEDURE_EXE, "rtnCoordCMDCrtProcedure::execute" )
   INT32 rtnCoordCMDCrtProcedure::execute( CHAR *pReceiveBuffer,
                                            SINT32 packSize,
                                            CHAR **ppResultBuffer,
                                            pmdEDUCB *cb,
                                            MsgOpReply &replyHeader,
                                            BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY(SDB_RTNCOORDCMDCRTPROCEDURE_EXE) ;
      netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->
                                        getCoordCB()->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *forward  = (MsgOpQuery *)pReceiveBuffer;
      forward->header.routeID.value = 0;
      forward->header.TID = cb->getTID();
      forward->header.opCode = MSG_CAT_CRT_PROCEDURES_REQ;
      CoordGroupList groupLst ;

      _printDebug ( pReceiveBuffer, "rtnCoordCMDCrtProcedure" ) ;

      rc = executeOnCataGroup ( (CHAR*)forward, pRouteAgent,
                                cb, NULL, &groupLst ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to crt procedures, rc = %d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC(SDB_RTNCOORDCMDCRTPROCEDURE_EXE, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   INT32 rtnCoordCMDCrtProcedure::processCatReply( MsgOpReply *pReply,
                                                    CoordGroupList &groupLst )
   {
      INT32 rc = SDB_OK ;
      rc = pReply->flags ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get err from catalog when create store "
                 "procedure:%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOCMDEVAL_EXE, "rtnCoordCMDEval::execute" )
   INT32 rtnCoordCMDEval::execute( CHAR *pReceiveBuffer,
                                   SINT32 packSize,
                                   CHAR **ppResultBuffer,
                                   pmdEDUCB *cb,
                                   MsgOpReply &replyHeader,
                                   BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOCMDEVAL_EXE ) ;
      spdSession *session = NULL ;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
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
      BSONObj procedures ;
      spcCoordDownloader downloader( this, cb ) ;
      SINT64 contextID = -1 ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName,
                           &numToSkip, &numToReturn, &pQuery, &pFieldSelector,
                           &pOrderBy, &pHint );
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract eval msg:%d", rc) ;
         goto error ;
      }

      try
      {
         procedures = BSONObj( pQuery ) ;
         PD_LOG( PDDEBUG, "eval:%s", procedures.toString().c_str() ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }


      session = SDB_OSS_NEW _spdSession() ;
      if ( NULL == session )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = session->eval( procedures, &downloader, cb ) ;
      if ( SDB_OK != rc )
      {
         const BSONObj &errmsg = session->getErrMsg() ;
         if ( !errmsg.isEmpty() )
         {
            *ppErrorObj = SDB_OSS_NEW BSONObj() ;
            if ( NULL == *ppErrorObj )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
            }
            else
            {
               **ppErrorObj = errmsg.getOwned() ;
            }
         }
         PD_LOG( PDERROR, "failed to eval store procedures:%d", rc ) ;
         goto error ;
      }

      if ( FMP_RES_TYPE_VOID != session->resType() )
      {
         rc = _buildContext( session, cb, contextID ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to prepare reply msg:%d", rc ) ;
            goto error ;
         }
      }

      replyHeader.numReturned = session->resType() ;
      replyHeader.contextID = contextID ;

   done:
      if ( -1 == contextID )
      {
         SAFE_OSS_DELETE( session ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNCOCMDEVAL_EXE, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      if ( NULL != *ppErrorObj )
      {
         replyHeader.header.messageLength = sizeof( MsgOpReply ) +
                                            (*ppErrorObj)->objsize() ;
      }
      goto done ;
   }

   INT32 rtnCoordCMDEval::_buildContext( _spdSession *session,
                                         pmdEDUCB *cb,
                                         SINT64 &contextID )
   {
      INT32 rc = SDB_OK ;
      const BSONObj &evalRes = session->getRetMsg() ;
      SDB_ASSERT( !evalRes.isEmpty(), "impossible" ) ;

      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
      rtnContextSP *context = NULL ;
      rc = rtnCB->contextNew ( RTN_CONTEXT_SP, (rtnContext**)&context,
                               contextID, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to create new context, rc: %d", rc ) ;

      rc = context->open( session ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to open context[%lld], rc: %d",
                   context->contextID(), rc ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOORDCMDRMPROCEDURE_EXE, "rtnCoordCMDRmProcedure::execute" )
   INT32 rtnCoordCMDRmProcedure::execute( CHAR *pReceiveBuffer,
                                           SINT32 packSize,
                                           CHAR **ppResultBuffer,
                                           pmdEDUCB *cb,
                                           MsgOpReply &replyHeader,
                                           BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY(SDB_RTNCOORDCMDRMPROCEDURE_EXE) ;
      netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->
                                        getCoordCB()->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *forward  = (MsgOpQuery *)pReceiveBuffer;
      forward->header.routeID.value = 0;
      forward->header.TID = cb->getTID();
      forward->header.opCode = MSG_CAT_RM_PROCEDURES_REQ;
      CoordGroupList groupLst ;

      rc = executeOnCataGroup ( (CHAR*)forward, pRouteAgent,
                                cb, NULL, &groupLst ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to rm procedures, rc = %d",
                  rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC(SDB_RTNCOORDCMDRMPROCEDURE_EXE, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   INT32 rtnCoordCMDRmProcedure::processCatReply( MsgOpReply *pReply,
                                                   CoordGroupList &groupLst )
   {
      INT32 rc = SDB_OK ;
      rc = pReply->flags ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get err from catalog when remove store "
                 "procedure:%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDLISTPROCEDURES_BUILD, "rtnCoordCMDListProcedures::buildQueryRequest" )
   INT32 rtnCoordCMDListProcedures::buildQueryRequest( CHAR *pIntput,
                                                       pmdEDUCB *cb,
                                                       CHAR **ppOutput )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOCMDLISTPROCEDURES_BUILD ) ;
      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      SINT64 numToSkip = 0;
      SINT64 numToReturn = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      INT32 bufferSize = 0;
      BSONObj query;
      BSONObj fieldSelector;
      BSONObj orderBy;
      BSONObj hint;

      rc = msgExtractQuery( pIntput, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery, 
                            &pFieldSelector, &pOrderBy, &pHint );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "failed to parse query request(rc=%d)", rc ) ;
         goto error ;
      }

      try
      {
         query = BSONObj ( pQuery );
         orderBy = BSONObj ( pOrderBy );
         hint = BSONObj ( pHint );
         fieldSelector = BSONObj( pFieldSelector ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( ppOutput, &bufferSize, CAT_PROCEDURES_COLLECTION,
                             flag, 0, numToSkip, numToReturn, &query,
                             &fieldSelector, &orderBy, &hint );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to build the query message(rc=%d)", rc );
         goto error;
      }

      {
         MsgOpQuery *pQueryMsg = (MsgOpQuery *)(*ppOutput);
         pQueryMsg->header.routeID.value = 0;
         pQueryMsg->header.TID = cb->getTID();
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNCOCMDLISTPROCEDURES_BUILD, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDLINKCL_EXE, "rtnCoordCMDLinkCollection::execute" )
   INT32 rtnCoordCMDLinkCollection::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                             CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                             MsgOpReply & replyHeader,
                                             BSONObj * * ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDLINKCL_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      CoordCataInfoPtr cataInfo;
      std::string strMainCLName;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *pLinkReq           = (MsgOpQuery *)pReceiveBuffer;
      pLinkReq->header.routeID.value = 0;
      pLinkReq->header.TID           = cb->getTID();
      CoordGroupList groupLst ;
      CoordGroupList sendGroupLst ;

      try
      {
         INT32 flag                       = 0;
         CHAR *pCommandName               = NULL;
         SINT64 numToSkip                 = 0;
         SINT64 numToReturn               = 0;
         CHAR *pQuery                     = NULL;
         CHAR *pFieldSelector             = NULL;
         CHAR *pOrderBy                   = NULL;
         CHAR *pHint                      = NULL;

         rc = msgExtractQuery( pReceiveBuffer, &flag, &pCommandName,
                               &numToSkip, &numToReturn, &pQuery, &pFieldSelector,
                               &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to parse link collection request(rc=%d)",
                     rc );
            goto error ;
         }
         BSONObj boQuery( pQuery );
         {
         BSONElement beMainCLName = boQuery.getField( CAT_COLLECTION_NAME );
         PD_CHECK( beMainCLName.type() == String, SDB_INVALIDARG, error, PDERROR,
                  "failed to get the field(%s)", CAT_COLLECTION_NAME );
         strMainCLName = beMainCLName.str();
         PD_CHECK( !strMainCLName.empty(), SDB_INVALIDARG, error, PDERROR,
                  "collection name can't be empty!" );
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG;
         goto error ;
      }

      pLinkReq->header.opCode        = MSG_CAT_LINK_CL_REQ;
      rc = executeOnCataGroup ( (CHAR*)pLinkReq, pRouteAgent,
                                cb, NULL, &groupLst ) ;
      PD_RC_CHECK( rc, PDERROR,
                   "failed to execute on catalog(rc=%d)",
                   rc );

      rc = rtnCoordGetCataInfo( cb, strMainCLName.c_str(), TRUE, cataInfo );
      PD_CHECK( SDB_OK == rc, rc, error_rollback, PDERROR,
                "failed to get catalog info(rc=%d)", rc );
      pLinkReq->version = cataInfo->getVersion();

      pLinkReq->header.opCode        = MSG_BS_QUERY_REQ;
      pLinkReq->header.routeID.value = 0;
      rc = executeOnDataGroup( pHeader, groupLst, sendGroupLst,
                               pRouteAgent, cb, TRUE ) ;
      PD_CHECK( SDB_OK == rc, rc, error_rollback, PDERROR,
                "Failed to execute on data-node(rc=%d)", rc);

   done :
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDLINKCL_EXE, rc ) ;
      return rc;
   error_rollback:
      {
      INT32 rcRBk = SDB_OK;
      pHeader->opCode = MSG_CAT_UNLINK_CL_REQ ;
      rcRBk = executeOnCataGroup ( (CHAR*)pLinkReq, pRouteAgent,
                                   cb, NULL, &groupLst ) ;
      PD_RC_CHECK( rcRBk, PDERROR, "Failed to execute on catalog(rc=%d), "
                   "rollback failed!", rcRBk ) ;
      }
   error :
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDUNLINKCL_EXE, "rtnCoordCMDUnlinkCollection::execute" )
   INT32 rtnCoordCMDUnlinkCollection::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                               CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                               MsgOpReply & replyHeader,
                                               BSONObj * * ppErrorObj )
   {
      INT32 rc                         = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDUNLINKCL_EXE ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *pReqMsg              = (MsgOpQuery *)pReceiveBuffer;
      pReqMsg->header.routeID.value    = 0;
      pReqMsg->header.TID              = cb->getTID();
      pReqMsg->header.opCode           = MSG_CAT_UNLINK_CL_REQ;
      CoordGroupList groupLst ;
      CoordGroupList sendGroupLst ;

      INT32 flag                       = 0;
      CHAR *pCMDName                   = NULL;
      SINT64 numToSkip                 = 0;
      SINT64 numToReturn               = 0;
      CHAR *pQuery                     = NULL;
      CHAR *pFieldSelector             = NULL;
      CHAR *pOrderBy                   = NULL;
      CHAR *pHint                      = NULL;
      std::string strSubClName;
      std::string strMainClName;
      CoordCataInfoPtr mainCataInfo;
      CoordCataInfoPtr subCataInfo;
      BOOLEAN isNeedRefresh            = FALSE;
      BOOLEAN hasRefresh               = FALSE;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip, &numToReturn,
                           &pQuery, &pFieldSelector, &pOrderBy, &pHint );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to parse unlink collection request(rc=%d)",
                   rc );

      try
      {
         BSONObj boQuery = BSONObj( pQuery );
         BSONElement beClNameTmp = boQuery.getField( CAT_SUBCL_NAME );
         PD_CHECK( beClNameTmp.type() == String, SDB_INVALIDARG, error, PDERROR,
                   "failed to unlink collection, failed to get sub-collection name" );
         strSubClName = beClNameTmp.str();

         beClNameTmp = boQuery.getField( CAT_COLLECTION_NAME );
         PD_CHECK( beClNameTmp.type() == String, SDB_INVALIDARG, error, PDERROR,
                   "failed to unlink collection, failed to get sub-collection name" );
         strMainClName = beClNameTmp.str();
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG( PDERROR,
                 "failed to unlink collection, received unexpected error:%s",
                 e.what() );
         goto error;
      }

   retry:
      hasRefresh = isNeedRefresh;
      rc = rtnCoordGetCataInfo( cb, strMainClName.c_str(), isNeedRefresh, mainCataInfo );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to unlink collection(MainCL:%s, subCL:%s), "
                   "get main-collection catalog failed(rc=%d)",
                   strMainClName.c_str(), strSubClName.c_str(), rc );

      if ( !mainCataInfo->isMainCL() )
      {
         PD_CHECK( !hasRefresh, SDB_INVALIDARG, error, PDERROR,
                   "collection(%s) is not main-collection",
                   strMainClName.c_str() );
         isNeedRefresh = TRUE;
         goto retry;
      }

      if ( !mainCataInfo->isContainSubCL( strSubClName ) )
      {
         PD_CHECK( !hasRefresh, SDB_INVALIDARG, error, PDERROR,
                   "collection(%s) is not contain sub-collection(%s)",
                   strMainClName.c_str(), strSubClName.c_str() );
         isNeedRefresh = TRUE;
         goto retry;
      }

      rc = rtnCoordGetCataInfo( cb, strSubClName.c_str(), isNeedRefresh, subCataInfo );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to unlink collection(MainCL:%s, subCL:%s), "
                   "get sub-collection catalog failed(rc=%d)",
                   strMainClName.c_str(), strSubClName.c_str(), rc );

      rc = rtnCoordGetGroupsByCataInfo( subCataInfo, sendGroupLst, groupLst );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to get group list(rc=%d)",
                   rc );

      pReqMsg->header.opCode = MSG_CAT_UNLINK_CL_REQ;
      rc = executeOnCataGroup ( (CHAR*)pReqMsg, pRouteAgent,
                                cb, NULL, &groupLst ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "create collection failed on catalog, rc = %d",
                  rc ) ;
         goto error ;
      }

      rc = rtnCoordGetCataInfo( cb, strMainClName.c_str(), TRUE, mainCataInfo );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to unlink collection(MainCL:%s, subCL:%s), "
                   "get main-collection catalog failed(rc=%d)",
                   strMainClName.c_str(), strSubClName.c_str(), rc );
      pReqMsg->version = mainCataInfo->getVersion();
      pReqMsg->header.routeID.value = 0;
      pReqMsg->header.TID = cb->getTID();
      pReqMsg->header.opCode = MSG_BS_QUERY_REQ;
      rc = executeOnDataGroup( pHeader, groupLst, sendGroupLst,
                               pRouteAgent, cb, TRUE );
      if ( rc )
      {
         PD_CHECK( !hasRefresh && SDB_CLS_COORD_NODE_CAT_VER_OLD == rc,
                   rc, error, PDERROR,
                   "failed to unlink collection(MainCL:%s, subCL:%s), "
                   "execute on data-node failed(rc=%d)",
                   strMainClName.c_str(), strSubClName.c_str(), rc );
         isNeedRefresh = TRUE;
         goto retry;
      }

   done :
      replyHeader.flags = rc;
      PD_TRACE_EXITRC ( SDB_RTNCOCMDUNLINKCL_EXE, rc ) ;
      return rc;
   error :
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDSETSESSATTR_EXE, "rtnCoordCMDSetSessionAttr::execute" )
   INT32 rtnCoordCMDSetSessionAttr::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                             CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                             MsgOpReply & replyHeader,
                                             BSONObj * * ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDSETSESSATTR_EXE ) ;
      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      INT32 flag                       = 0;
      CHAR *pCMDName                   = NULL;
      SINT64 numToSkip                 = 0;
      SINT64 numToReturn               = 0;
      CHAR *pQuery                     = NULL;
      CHAR *pFieldSelector             = NULL;
      CHAR *pOrderBy                   = NULL;
      CHAR *pHint                      = NULL;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCMDName, &numToSkip, &numToReturn,
                           &pQuery, &pFieldSelector, &pOrderBy, &pHint );
      PD_RC_CHECK( rc, PDERROR,
                   "Failed to parse unlink collection request(rc=%d)",
                   rc );

      try
      {
         CoordSession *pSession = NULL;
         BSONObj boQuery;
         BSONElement bePreferRepl;
         INT32 sessReplType = PREFER_REPL_TYPE_MIN;
         GROUP_VEC groupLstTmp;

         pSession = cb->getCoordSession();
         PD_CHECK( pSession != NULL, SDB_SYS, error, PDERROR,
                   "Failed to get session!" );
         boQuery = BSONObj( pQuery );
         bePreferRepl = boQuery.getField( FIELD_NAME_PREFERED_INSTANCE );
         PD_CHECK( bePreferRepl.type() == NumberInt, SDB_INVALIDARG, error,
                   PDERROR, "Failed to set session attribute, failed to get "
                   "the field(%s)", FIELD_NAME_PREFERED_INSTANCE );
         sessReplType = bePreferRepl.Int();
         PD_CHECK( sessReplType > PREFER_REPL_TYPE_MIN &&
                   sessReplType < PREFER_REPL_TYPE_MAX,
                   SDB_INVALIDARG, error, PDERROR,
                   "Failed to set prefer-replica-type, invalid value!"
                   "(range:%d~%d)", PREFER_REPL_TYPE_MIN,
                   PREFER_REPL_TYPE_MAX ) ;
         pSession->setPreferReplType( sessReplType );

         rc = rtnCoordGetAllGroupList( cb, groupLstTmp );
         PD_RC_CHECK( rc, PDERROR, "Failed to update all group info!(rc=%d)",
                      rc ) ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG( PDERROR, "Failed to unlink collection, received unexpected "
                 "error:%s", e.what() ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_RTNCOCMDSETSESSATTR_EXE, rc ) ;
      return rc;
   error:
      replyHeader.flags = rc;
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDCREATEDOMAIN_EXE, "rtnCoordCMDCreateDomain::execute" )
   INT32 rtnCoordCMDCreateDomain::execute ( CHAR *pReceiveBuffer,
                                            SINT32 packSize,
                                            CHAR **ppResultBuffer,
                                            pmdEDUCB *cb,
                                            MsgOpReply &replyHeader,
                                            BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDCREATEDOMAIN_EXE ) ;
      netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->
                                        getCoordCB()->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *forward  = (MsgOpQuery *)pReceiveBuffer;
      forward->header.routeID.value = 0;
      forward->header.TID = cb->getTID();
      forward->header.opCode = MSG_CAT_CREATE_DOMAIN_REQ;

      _printDebug ( pReceiveBuffer, "rtnCoordCMDCreateDomain" ) ;

      rc = executeOnCataGroup ( (CHAR*)forward, pRouteAgent, cb ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to create domain, rc = %d", rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_RTNCOCMDCREATEDOMAIN_EXE, rc ) ;
      return rc ;
   error :
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDDROPDOMAIN_EXE, "rtnCoordCMDDropDomain::execute" )
   INT32 rtnCoordCMDDropDomain::execute ( CHAR *pReceiveBuffer,
                                          SINT32 packSize,
                                          CHAR **ppResultBuffer,
                                          pmdEDUCB *cb,
                                          MsgOpReply &replyHeader,
                                          BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDDROPDOMAIN_EXE ) ;
      netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->
                                        getCoordCB()->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *forward  = (MsgOpQuery *)pReceiveBuffer;
      forward->header.routeID.value = 0;
      forward->header.TID = cb->getTID();
      forward->header.opCode = MSG_CAT_DROP_DOMAIN_REQ;

      _printDebug ( pReceiveBuffer, "rtnCoordCMDDropDomain" ) ;

      rc = executeOnCataGroup ( (CHAR*)forward, pRouteAgent, cb ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to drop domain, rc = %d", rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_RTNCOCMDDROPDOMAIN_EXE, rc ) ;
      return rc ;
   error :
      replyHeader.flags = rc ;
      goto done ;

   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDALTERDOMAIN_EXE, "rtnCoordCMDAlterDomain::execute" )
   INT32 rtnCoordCMDAlterDomain::execute ( CHAR *pReceiveBuffer,
                                           SINT32 packSize,
                                           CHAR **ppResultBuffer,
                                           pmdEDUCB *cb,
                                           MsgOpReply &replyHeader,
                                           BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOCMDALTERDOMAIN_EXE ) ;
      netMultiRouteAgent *pRouteAgent = pmdGetKRCB()->getCoordCB(
         )->getRouteAgent();

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *forward  = (MsgOpQuery *)pReceiveBuffer;
      forward->header.routeID.value = 0;
      forward->header.TID = cb->getTID();
      forward->header.opCode = MSG_CAT_ALTER_DOMAIN_REQ;

      _printDebug ( pReceiveBuffer, "rtnCoordCMDAlterDomain" ) ;

      rc = executeOnCataGroup ( (CHAR*)forward, pRouteAgent, cb ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to alter domain, rc = %d", rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_RTNCOCMDALTERDOMAIN_EXE, rc ) ;
      return rc ;
   error :
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDADDDOMAINGROUP_EXE, "rtnCoordCMDAddDomainGroup::execute" )
   INT32 rtnCoordCMDAddDomainGroup::execute ( CHAR *pReceiveBuffer,
                                              SINT32 packSize,
                                              CHAR **ppResultBuffer,
                                              pmdEDUCB *cb,
                                              MsgOpReply &replyHeader,
                                              BSONObj **ppErrorObj )
   {
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDREMOVEDOMAINGROUP_EXE, "rtnCoordCMDRemoveDomainGroup::execute" )
   INT32 rtnCoordCMDRemoveDomainGroup::execute ( CHAR *pReceiveBuffer,
                                                 SINT32 packSize,
                                                 CHAR **ppResultBuffer,
                                                 pmdEDUCB *cb,
                                                 MsgOpReply &replyHeader,
                                                 BSONObj **ppErrorObj )
   {
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB_RTNCOCMDLISTDOMAINS_BUILD, "rtnCoordCMDListDomains::buildQueryRequest" )
   INT32 rtnCoordCMDListDomains::buildQueryRequest( CHAR *pInput,
                                                    pmdEDUCB *cb,
                                                    CHAR **ppOutput )
   {
      INT32 rc              = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOCMDLISTDOMAINS_BUILD ) ;
      INT32 flag            = 0;
      CHAR *pCollectionName = NULL;
      SINT64 numToSkip      = 0;
      SINT64 numToReturn    = 0;
      CHAR *pQuery          = NULL;
      CHAR *pFieldSelector  = NULL;
      CHAR *pOrderBy        = NULL;
      CHAR *pHint           = NULL;
      INT32 bufferSize      = 0;
      BSONObj query;
      BSONObj fieldSelector;
      BSONObj orderBy;
      BSONObj hint;

      rc = msgExtractQuery( pInput, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector, &pOrderBy, &pHint );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "failed to parse query request(rc=%d)", rc ) ;
         goto error ;
      }

      try
      {
         query = BSONObj ( pQuery );
         orderBy = BSONObj ( pOrderBy );
         hint = BSONObj ( pHint );
         fieldSelector = BSONObj( pFieldSelector ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( ppOutput, &bufferSize, CAT_DOMAIN_COLLECTION,
                             flag, 0, numToSkip, numToReturn, &query, &fieldSelector,
                             &orderBy, &hint );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to build the query message(rc=%d)", rc );
         goto error;
      }

      {
         MsgOpQuery *pQueryMsg = (MsgOpQuery *)(*ppOutput);
         pQueryMsg->header.routeID.value = 0;
         pQueryMsg->header.TID = cb->getTID();
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNCOCMDLISTPROCEDURES_BUILD, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( CMD_RTNCOCMDLISTCSINDOMAIN_BUILD, "rtnCoordCMDListCSInDomain::buildQueryRequest" )
   INT32 rtnCoordCMDListCSInDomain::buildQueryRequest( CHAR *pInput,
                                                       pmdEDUCB *cb,
                                                       CHAR **ppOutput )
   {
      INT32 rc              = SDB_OK ;
      PD_TRACE_ENTRY( CMD_RTNCOCMDLISTCSINDOMAIN_BUILD ) ;
      INT32 flag            = 0;
      CHAR *pCollectionName = NULL;
      SINT64 numToSkip      = 0;
      SINT64 numToReturn    = 0;
      CHAR *pQuery          = NULL;
      CHAR *pFieldSelector  = NULL;
      CHAR *pOrderBy        = NULL;
      CHAR *pHint           = NULL;
      INT32 bufferSize      = 0;
      BSONObj query;
      BSONObj fieldSelector;
      BSONObj orderBy;
      BSONObj hint;

      rc = msgExtractQuery( pInput, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector, &pOrderBy, &pHint );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "failed to parse query request(rc=%d)", rc ) ;
         goto error ;
      }

      try
      {
         query = BSONObj ( pQuery );
         orderBy = BSONObj ( pOrderBy );
         hint = BSONObj ( pHint );
         fieldSelector = BSONObj( pFieldSelector ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( ppOutput, &bufferSize, CAT_COLLECTION_SPACE_COLLECTION,
                             flag, 0, numToSkip, numToReturn, &query, &fieldSelector,
                             &orderBy, &hint );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to build the query message(rc=%d)", rc );
         goto error;
      }
      {
      MsgOpQuery *pQueryMsg = (MsgOpQuery *)(*ppOutput);
      pQueryMsg->header.routeID.value = 0;
      pQueryMsg->header.TID = cb->getTID();
      }
   done:
      PD_TRACE_EXITRC( CMD_RTNCOCMDLISTCSINDOMAIN_BUILD, rc ) ;
      return rc ;
   error:
      goto done ;
   }


   // PD_TRACE_DECLARE_FUNCTION( CMD_RTNCOCMDLISTCLINDOMAIN_EXECUTE, "rtnCoordCMDListCLInDomain::execute" )
   INT32 rtnCoordCMDListCLInDomain::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                             CHAR **ppResultBuffer,
                                             pmdEDUCB *cb, MsgOpReply &replyHeader,
                                             BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( CMD_RTNCOCMDLISTCLINDOMAIN_EXECUTE ) ;
      BSONObj conObj ;
      BSONObj selObj ;
      BSONObj dummy ;
      CHAR *query = NULL ;
      CHAR *selector = NULL ;
      CHAR *orderby = NULL ;
      CHAR *hint = NULL ;
      INT32 flag = 0 ;
      CHAR *collectionName = NULL ;
      SINT64 skip = 0 ;
      SINT64 limit = -1 ;
      BSONElement domain ;
      CHAR *msgBuf = NULL ;
      rtnQueryOptions queryOptions ;

      std::vector<BSONObj> replyFromCata ;

      MsgHeader *reqHeader = (MsgHeader *)pReceiveBuffer;

      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES;
      replyHeader.header.requestID     = reqHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = reqHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &collectionName,
                            &skip, &limit, &query,
                            &selector, &orderby, &hint );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "failed to parse query request(rc=%d)", rc ) ;
         goto error ;
      }

      try
      {
         conObj = BSONObj( query ) ;
         selObj = BSONObj( selector ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      domain = conObj.getField( FIELD_NAME_DOMAIN ) ;
      if ( String != domain.type() )
      {
         PD_LOG( PDERROR, "invalid domain field in object:%s",
                  conObj.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      queryOptions._query = BSON( CAT_DOMAIN_NAME << domain.valuestr() ) ;
      queryOptions._fullName = CAT_COLLECTION_SPACE_COLLECTION ;

      rc = queryOnCataAndPushToVec( queryOptions, cb, replyFromCata ) ; 
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to execute query on catalog:%d", rc ) ;
         goto error ;
      }

      {
      SINT64 contextID = -1 ;
      rc = _rebuildListResult( replyFromCata, cb, contextID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to rebuild list result:%d", rc ) ;
         goto error ;
      }
      replyHeader.flags = SDB_OK ;
      replyHeader.contextID = contextID ;
      }
   done:
      if ( NULL != msgBuf )
      {
         SDB_OSS_FREE( msgBuf ) ;
         msgBuf = NULL ;
      }
      PD_TRACE_EXITRC( CMD_RTNCOCMDLISTCLINDOMAIN_EXECUTE, rc )  ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( CMD_RTNCOCMDLISTCLINDOMAIN__REBUILDRESULT, "rtnCoordCMDListCLInDomain::_rebuildListResult" )
   INT32 rtnCoordCMDListCLInDomain::_rebuildListResult(
                                const std::vector<BSONObj> &infoFromCata,
                                pmdEDUCB *cb,                       
                                SINT64 &contextID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( CMD_RTNCOCMDLISTCLINDOMAIN__REBUILDRESULT ) ;
      rtnContext *context = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;

      rc = rtnCB->contextNew( RTN_CONTEXT_DUMP,
                              &context,
                              contextID,
                              cb ) ;
      if  ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to create new context:%d", rc ) ;
         goto error ;
      }

      rc = (( rtnContextDump * )context)->open( BSONObj(), BSONObj() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open context:%d", rc ) ;
         goto error ;
      }

      for ( vector<BSONObj>::const_iterator itr = infoFromCata.begin();
            itr != infoFromCata.end();
            itr++ )
      {
         BSONElement cl ;
         BSONElement cs = itr->getField( FIELD_NAME_NAME ) ;
         if ( String != cs.type() )
         {
            PD_LOG( PDERROR, "invalid collection space info:%s",
                    itr->toString().c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         cl = itr->getField( FIELD_NAME_COLLECTION ) ;
         if ( Array != cl.type() )
         {
            PD_LOG( PDERROR, "invalid collection space info:%s",
                    itr->toString().c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
              
         }

         {
         BSONObjIterator clItr( cl.embeddedObject() ) ;
         while ( clItr.more() )
         {
            stringstream ss ;
            BSONElement clName ;
            BSONElement oneCl = clItr.next() ;
            if ( Object != oneCl.type() )
            {
               PD_LOG( PDERROR, "invalid collection space info:%s",
                    itr->toString().c_str() ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            clName = oneCl.embeddedObject().getField( FIELD_NAME_NAME ) ;
            if ( String != clName.type() )
            {
               PD_LOG( PDERROR, "invalid collection space info:%s",
                    itr->toString().c_str() ) ;
               rc = SDB_SYS ;
               goto error ;
            }

            ss << cs.valuestr() << "." << clName.valuestr() ;
            context->append( BSON( FIELD_NAME_NAME << ss.str() ) ) ;
         }
         }
      }
   done:
      PD_TRACE_EXITRC( CMD_RTNCOCMDLISTCLINDOMAIN__REBUILDRESULT, rc ) ;
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   INT32 rtnCoordCMDOnMultiNodes::_extractExecRange( const BSONObj &condition,
                                                     pmdEDUCB *cb,
                                                     ROUTE_SET &nodes )
   {
      INT32 rc = SDB_OK ;
      BSONElement groups = condition.getField( FIELD_NAME_GROUPS ) ;
      if ( groups.eoo() )
      {
         GROUP_VEC gpLst ;
         rc = rtnCoordGetAllGroupList( cb, gpLst ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get group list:%d", rc ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         for ( GROUP_VEC::const_iterator itr = gpLst.begin();
               itr != gpLst.end();
               itr++ )
         {
            MsgRouteID routeID ;
            routeID.value = MSG_INVALID_ROUTEID ;
            clsGroupItem *grp = (*itr)->getGroupItem() ;
            routeID.columns.groupID = grp->groupID() ;
            const VEC_NODE_INFO *nodesInfo = grp->getNodes() ;
            for ( VEC_NODE_INFO::const_iterator itrn = nodesInfo->begin();
                  itrn != nodesInfo->end();
                itrn++ )
            {
               routeID.columns.nodeID = itrn->_id.columns.nodeID ;
               routeID.columns.serviceID = MSG_ROUTE_SHARD_SERVCIE ;
               nodes.insert( routeID.value ) ;
            }
         }
      }
      else if ( groups.isNull() )
      {
      }
      else if ( String == groups.type() )
      {
         CoordGroupInfoPtr gpInfo ;
         rc = rtnCoordGetGroupInfo( cb, groups.valuestr(),
                                    FALSE, gpInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get group info which is specified[%s],"
                    "rc:%d", groups.valuestr(), rc ) ;
            goto error ;
         }

         {
         MsgRouteID routeID ;
         routeID.value = MSG_INVALID_ROUTEID ;
         clsGroupItem *grp = gpInfo->getGroupItem() ;
         routeID.columns.groupID = grp->groupID() ;
         const VEC_NODE_INFO *nodesInfo = grp->getNodes() ;
         for ( VEC_NODE_INFO::const_iterator itrn = nodesInfo->begin();
                  itrn != nodesInfo->end();
                itrn++ )
         {
            routeID.columns.nodeID = itrn->_id.columns.nodeID ;
            routeID.columns.serviceID = MSG_ROUTE_SHARD_SERVCIE ;
            nodes.insert( routeID.value ) ;
         }
         }
      }
      else if ( Array == groups.type() )
      {
         BSONObjIterator itr( groups.embeddedObject() ) ;
         while ( itr.more() )
         {
            BSONElement ele = itr.next() ;
            if ( String != ele.type() )
            {
               PD_LOG( PDERROR, "invalid condition object:%s",
                       condition.toString( FALSE, TRUE ).c_str() ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }

            {
            CoordGroupInfoPtr gpInfo ;
            rc = rtnCoordGetGroupInfo( cb, ele.valuestr(),
                                       FALSE, gpInfo ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get group info which is specified[%s],"
                       "rc:%d", groups.valuestr(), rc ) ;
               goto error ;
            }

            {
            MsgRouteID routeID ;
            routeID.value = MSG_INVALID_ROUTEID ;
            clsGroupItem *grp = gpInfo->getGroupItem() ;
            routeID.columns.groupID = grp->groupID() ;
            const VEC_NODE_INFO *nodesInfo = grp->getNodes() ;
            for ( VEC_NODE_INFO::const_iterator itrn = nodesInfo->begin();
                     itrn != nodesInfo->end();
                   itrn++ )
            {
               routeID.columns.nodeID = itrn->_id.columns.nodeID ;
               routeID.columns.serviceID = MSG_ROUTE_SHARD_SERVCIE ;
               nodes.insert( routeID.value ) ;
            }
            }
            }
         } 
      }
      else
      {
         PD_LOG( PDERROR, "invalid condition object:%s",
                 condition.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnCoordCMDOnMultiNodes::_executeOnMultiNodes( CHAR *msg,
                                                        pmdEDUCB *cb,
                                                        ROUTE_SET &nodes,
                                                        ROUTE_RC_MAP &uncompleted )
   {
      INT32 rc = SDB_OK ;
      REQUESTID_MAP completed ;
      CoordCB *coordcb = pmdGetKRCB()->getCoordCB();
      netMultiRouteAgent *routeAgent = coordcb->getRouteAgent();
      REPLY_QUE replyQueue ;

      rc = rtnCoordSendRequestToNodes( msg,
                                       nodes,
                                       routeAgent,
                                       cb,
                                       completed,
                                       uncompleted ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to send request to multi nodes:%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( CMD_RTNCOCMDINVALIDATECACHE_EXEC, "rtnCoordCMDInvalidateCache::execute" )
   INT32 rtnCoordCMDInvalidateCache::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                           CHAR **ppResultBuffer,
                                           pmdEDUCB *cb, MsgOpReply &replyHeader,
                                           BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( CMD_RTNCOCMDINVALIDATECACHE_EXEC ) ;

      CHAR *query = NULL ;
      CHAR *selector = NULL ;
      CHAR *orderby = NULL ;
      CHAR *hint = NULL ;
      INT32 flag = 0 ;
      CHAR *collectionName = NULL ;
      SINT64 skip = 0 ;
      SINT64 limit = -1 ;
      BSONObj condition ;
      BSONElement execRange ;
      ROUTE_SET nodes ;
      ROUTE_RC_MAP uncompleted ;

      MsgHeader *reqHeader = (MsgHeader *)pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode = MSG_BS_QUERY_RES ;
      replyHeader.header.requestID = reqHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID = reqHeader->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &collectionName,
                            &skip, &limit, &query,
                            &selector, &orderby, &hint );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "failed to parse query request(rc=%d)", rc ) ;
         goto error ;
      }

      try
      {
         condition = BSONObj( query ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = _extractExecRange( condition, cb, nodes ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract range from object:%d", rc ) ;
         goto error ;
      }

      sdbGetCoordCB()->invalidateCataInfo() ;
      sdbGetCoordCB()->invalidateGroupInfo() ;

      reqHeader->TID = cb->getTID() ;
      rc = _executeOnMultiNodes( pReceiveBuffer,
                                 cb, nodes, uncompleted ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to execute on multi nodes:%d", rc ) ;
         goto error ;
      }

      if ( !uncompleted.empty() )
      {
         std::stringstream ss ;
         MsgRouteID routeID ;
         routeID.value = MSG_INVALID_ROUTEID ;
         for ( ROUTE_RC_MAP::const_iterator itr = uncompleted.begin() ;
               itr != uncompleted.end() ;
               itr++ )
         {
            routeID.value = itr->first ;
            ss << " [" << routeID.columns.groupID << "," << routeID.columns.nodeID
               << "]:" << itr->second ;
         }
         PD_LOG( PDERROR, "not all nodes returned ok:%s",
                 ss.str().c_str() ) ;
         rc = SDB_COORD_NOT_ALL_DONE ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( CMD_RTNCOCMDINVALIDATECACHE_EXEC, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( CMD_RTNCOCMDLISTLOBS_EXEC, "rtnCoordListLobs::execute" )   
   INT32 rtnCoordCMDListLobs::execute( CHAR *pReceiveBuffer,
                                       SINT32 packSize,
                                       CHAR **ppResultBuffer,
                                       pmdEDUCB *cb,
                                       MsgOpReply &replyHeader,
                                       BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( CMD_RTNCOCMDLISTLOBS_EXEC ) ;
      INT32 flag = 0;
      CHAR *pCollectionName = NULL;
      SINT64 numToSkip = 0;
      SINT64 numToReturn = 0;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      BSONObj query ;
      const CHAR *fullName = NULL ;
      CoordCataInfoPtr cataInfo ;
      CoordGroupList sendGroupLst ;
      CoordGroupList groupLst ;
      SDB_RTNCB *pRtncb = pmdGetKRCB()->getRTNCB() ;
      CoordCB *pCoordcb = pmdGetKRCB()->getCoordCB() ;
      netMultiRouteAgent *pRouteAgent = pCoordcb->getRouteAgent() ;
      ROUTE_SET sendNodes;
      map<UINT64, SINT64> contexts ;
      rtnContextCoord *context = NULL ;
      SINT64 contextID = -1 ;
      BOOLEAN refresh = FALSE ;

      MsgHeader *pHeader = (MsgHeader *)pReceiveBuffer;
      MsgOpQuery *queryHeader = ( MsgOpQuery * )pReceiveBuffer ;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode = MSG_BS_QUERY_RES;
      replyHeader.header.requestID = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID = pHeader->TID;
      replyHeader.contextID = -1;
      replyHeader.flags = SDB_OK;
      replyHeader.numReturned = 0;
      replyHeader.startFrom = 0;

      rc = msgExtractQuery( pReceiveBuffer, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery,
                            &pFieldSelector, &pOrderBy, &pHint );

      PD_RC_CHECK( rc, PDERROR, "Snapshot failed, failed to parse query "
                   "request(rc=%d)", rc ) ;

      try
      {
         query = BSONObj( pQuery ) ;
         BSONElement ele = query.getField( FIELD_NAME_COLLECTION ) ;
         if ( String != ele.type() )
         {
            PD_LOG( PDERROR, "invalid obj of list lob:%s",
                    query.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         fullName = ele.valuestr() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

retry:
      rc = rtnCoordGetCataInfo( cb, fullName, refresh, cataInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get catainfo of:%s, rc:%d",
                 fullName, rc ) ;
         goto error ;
      }

      queryHeader->version = cataInfo->getVersion() ;

      rc = rtnCoordGetGroupsByCataInfo( cataInfo,
                                        sendGroupLst,
                                        groupLst ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get group list of:%s, rc:%d",
                 fullName, rc ) ;
         goto error ;
      }

      rc = pRtncb->contextNew( RTN_CONTEXT_COORD,
                               (rtnContext**)&context,
                                contextID, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to create new context:%d", rc ) ;
         goto error ;
      }

      rc = context->open( BSONObj(), BSONObj() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open context:%d", rc ) ;
         goto error ;
      }

      rc = executeOnDataGroup( pHeader, groupLst, sendGroupLst,
                               pRouteAgent, cb, FALSE, NULL, &contexts ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to execute command on data groups:%d", rc ) ;
      }

      for ( map<UINT64, SINT64>::const_iterator itr = contexts.begin();
            itr != contexts.end();
            ++itr )
      {
         MsgRouteID id ;
         id.value = itr->first ;
         INT32 rcTmp = context->addSubContext( id, itr->second ) ;
         if ( SDB_OK != rcTmp )
         {
            PD_LOG( PDERROR, "failed to add su context:%d", rc ) ;
         }
      }

      if ( SDB_OK != rc )
      {
         if ( SDB_CLS_COORD_NODE_CAT_VER_OLD == rc )
         {
            pRtncb->contextDelete( contextID, cb ) ;
            contextID = -1 ;
            context = NULL ;
            refresh = TRUE ;
            goto retry ;
         }
         goto error ;
      }

      replyHeader.contextID = contextID ;
   done:
      PD_TRACE_EXITRC( CMD_RTNCOCMDLISTLOBS_EXEC, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc;
      if ( -1 != contextID )
      {
         pRtncb->contextDelete( contextID, cb ) ;
      }
      goto done ;
   }
}

