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

   Source File Name = catNodeManager.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =     XJH Opt

*******************************************************************************/
#include "catCommon.hpp"
#include "../util/fromjson.hpp"
#include "msgCatalog.hpp"
#include "pmdCB.hpp"
#include "rtn.hpp"
#include "catNodeManager.hpp"
#include "msgMessage.hpp"
#include "pdTrace.hpp"
#include "catTrace.hpp"

using namespace bson;

#define CAT_PORT_STR_SZ 10

namespace engine
{

   /*
      catNodeManager implement
   */
   catNodeManager::catNodeManager()
   {
      _status = SDB_CAT_MODULE_DEACTIVE ;
      _pDmsCB = NULL ;
      _pDpsCB = NULL ;
      _pRtnCB = NULL ;
      _pCatCB = NULL ;
      _pEduCB = NULL ;
   }

   catNodeManager::~catNodeManager()
   {
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_INIT, "catNodeManager::init" )
   INT32 catNodeManager::init()
   {
      INT32 rc = SDB_OK;
      pmdKRCB *krcb     = pmdGetKRCB() ;
      _pDmsCB           = krcb->getDMSCB();
      _pDpsCB           = krcb->getDPSCB();
      _pRtnCB           = krcb->getRTNCB();
      _pCatCB           = krcb->getCATLOGUECB();
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_INIT ) ;
      rc = readCataConf() ;
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_INIT, rc ) ;
      return rc ;
   }

   void catNodeManager::attachCB( pmdEDUCB * cb )
   {
      _pEduCB = cb ;
   }

   void catNodeManager::detachCB( pmdEDUCB * cb )
   {
      _pEduCB = NULL ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_ACTIVE, "catNodeManager::active" )
   INT32 catNodeManager::active()
   {
      INT32 rc            = SDB_OK;

      rtnContextBuf buffObj ;

      PD_TRACE_ENTRY ( SDB_CATNODEMGR_ACTIVE ) ;
      BSONObj boEmpty;
      SINT64 sContextID   = -1;
      CHAR szBuf[ OP_MAXNAMELENGTH+1 ] = {0} ;
      ossStrncpy( szBuf, CAT_NODE_INFO_COLLECTION, OP_MAXNAMELENGTH );
      rc = rtnQuery ( szBuf, boEmpty, boEmpty,
                      boEmpty, boEmpty, 0, _pEduCB, 0, -1, _pDmsCB,
                      _pRtnCB, sContextID ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to query %s collection, rc = %d",
                  CAT_NODE_INFO_COLLECTION, rc ) ;
         goto error ;
      }
      while ( TRUE )
      {
         rc = rtnGetMore( sContextID, 1, buffObj, _pEduCB, _pRtnCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK;
               sContextID = -1 ;
               break;
            }
            PD_LOG ( PDERROR, "Failed to fetch from %s collection, rc = %d",
                     CAT_NODE_INFO_COLLECTION, rc );
            goto error ;
         }

         if ( buffObj.data() != NULL )
         {
            try
            {
               BSONObj bsGrpInfo( buffObj.data() );
               PD_TRACE1 ( SDB_CATNODEMGR_ACTIVE,
                           PD_PACK_STRING ( bsGrpInfo.toString().c_str() ) ) ;
               rc = parseIDInfo( bsGrpInfo );
               if ( rc )
               {
                  if ( SDB_INVALIDARG == rc )
                  {
                     rc = SDB_CAT_CORRUPTION ;
                  }
                  PD_LOG( PDERROR, "Failed to parse node info: %s",
                          bsGrpInfo.toString().c_str());
                  goto error ;
               }
            }
            catch (std::exception &e)
            {
               PD_LOG ( PDERROR, "Invalid data is read from context buffer: %s",
                        e.what() ) ;
               rc = SDB_CAT_CORRUPTION ;
               goto error ;
            }
         } // if ( pBuffer != NULL )
      } // while ( TRUE );
      _status = SDB_CAT_MODULE_ACTIVE ;

   done :
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_ACTIVE, rc ) ;
      return rc;
   error :
      if ( -1 != sContextID )
      {
         _pRtnCB->contextDelete( sContextID, _pEduCB );
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_DEACTIVE, "catNodeManager::deactive" )
   INT32 catNodeManager::deactive()
   {
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_DEACTIVE ) ;
      _status = SDB_CAT_MODULE_DEACTIVE;
      PD_TRACE_EXIT ( SDB_CATNODEMGR_DEACTIVE ) ;
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_PROCESSMSG, "catNodeManager::processMsg" )
   INT32 catNodeManager::processMsg( const NET_HANDLE &handle,
                                     MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_PROCESSMSG ) ;
      PD_TRACE1 ( SDB_CATNODEMGR_PROCESSMSG, PD_PACK_INT ( pMsg->opCode ) ) ;

      switch ( pMsg->opCode )
      {
      case MSG_CAT_REG_REQ:
            rc = processRegReq( handle, pMsg ) ;
            break;

      case MSG_CAT_CATGRP_REQ:
      case MSG_CAT_NODEGRP_REQ:
      case MSG_CAT_GRP_REQ:
            rc = processGrpReq( handle, pMsg ) ;
            break;

      case MSG_CAT_PAIMARY_CHANGE:
            rc = processPrimaryChange( handle, pMsg ) ;
            break;

      case MSG_CAT_CREATE_GROUP_REQ :
      case MSG_CAT_CREATE_DOMAIN_REQ :
      case MSG_CAT_CREATE_NODE_REQ :
      case MSG_CAT_UPDATE_NODE_REQ :
      case MSG_CAT_DEL_NODE_REQ :
            rc = processCommandMsg( handle, pMsg, TRUE ) ;
            break;
      case MSG_CAT_RM_GROUP_REQ:
            rc = processRemoveGrp( handle, pMsg ) ;
            break ;
      case MSG_CAT_ACTIVE_GROUP_REQ:
            rc = processActiveGrp( handle, pMsg ) ;
            break;
      default:
            rc = SDB_UNKNOWN_MESSAGE;
            PD_LOG( PDWARNING, "Received unknown message (opCode: [%d]%u )",
                    IS_REPLY_TYPE(pMsg->opCode),
                    GET_REQUEST_TYPE(pMsg->opCode) ) ;
            break;
      }
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_PROCESSMSG, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_PRIMARYCHANGE, "catNodeManager::processPrimaryChange" )
   INT32 catNodeManager::processPrimaryChange( const NET_HANDLE &handle,
                                               MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_PRIMARYCHANGE ) ;
      UINT32 w = _majoritySize() ;
      MsgCatPrimaryChange *pRequest = (MsgCatPrimaryChange *)pMsg ;
      UINT32 groupID = pRequest->newPrimary.columns.groupID;
      UINT16 nodeID = pRequest->newPrimary.columns.nodeID;
      PD_TRACE2 ( SDB_CATNODEMGR_PRIMARYCHANGE,
                  PD_PACK_UINT ( groupID ),
                  PD_PACK_USHORT ( nodeID ) ) ;
      MsgCatPrimaryChangeRes msgReply;
      msgReply.header.header.messageLength = sizeof(MsgCatPrimaryChange);
      msgReply.header.header.opCode = MSG_CAT_PAIMARY_CHANGE_RES;
      msgReply.header.header.requestID = pRequest->header.requestID;
      msgReply.header.header.routeID.value= 0;
      msgReply.header.header.TID = pRequest->header.TID;

      if ( !pmdIsPrimary() || SDB_CAT_MODULE_ACTIVE != _status )
      {
         rc = SDB_CLS_NOT_PRIMARY ;
         PD_LOG ( PDWARNING, "service deactive but received primary-change "
                  "request" );
         goto done;
      }

      try
      {
         BSONObj boUpdater;
         BSONObj boMatcher;
         if ( INVALID_GROUPID == groupID || INVALID_NODEID == nodeID )
         {
            boMatcher = BSON( CAT_GROUPID_NAME <<
                              pRequest->oldPrimary.columns.groupID <<
                              CAT_PRIMARY_NAME <<
                              pRequest->oldPrimary.columns.nodeID ) ;
            boUpdater = BSON( "$unset" << BSON( CAT_PRIMARY_NAME << nodeID ) ) ;
         }
         else
         {
            if ( pRequest->header.routeID.columns.nodeID == nodeID )
            {
               boMatcher = BSON( CAT_GROUPID_NAME << groupID ) ;
            }
            else
            {
               boMatcher = BSON( CAT_GROUPID_NAME << groupID <<
                                 CAT_PRIMARY_NAME <<
                                 pRequest->header.routeID.columns.nodeID ) ;
            }
            BSONObj boComment = BSON( CAT_PRIMARY_NAME << nodeID ) ;
            BSONObjBuilder bobUpdater ;
            bobUpdater.append( "$set", boComment ) ;
            boUpdater = bobUpdater.obj() ;
         }

         BSONObj hint ;
         rc = rtnUpdate ( CAT_NODE_INFO_COLLECTION, boMatcher, boUpdater,
                          hint, 0, _pEduCB, _pDmsCB, _pDpsCB, w ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to set primary-node(rc=%d)",
                      rc ) ;
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_SYS, SDB_SYS, error, PDERROR,
                   "Failed to set primary-node, received unexpected error:%s",
                   e.what() ) ;
      }

   done:
      msgReply.header.res = rc ;
      PD_TRACE1 ( SDB_CATNODEMGR_PRIMARYCHANGE, PD_PACK_INT ( rc ) ) ;
      rc = _pCatCB->netWork()->syncSend( handle, &msgReply ) ;
      if ( rc )
      {
	      PD_LOG( PDERROR, "failed to send response(primary-change)(rc=%d)",
                 rc ) ;
      }
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_PRIMARYCHANGE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_GRPREQ, "catNodeManager::processGrpReq" )
   INT32 catNodeManager::processGrpReq( const NET_HANDLE &handle,
                                        MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_GRPREQ ) ;
      MsgCatGroupRes *pMsgRsp = NULL ;
      SINT32 msgLen = 0 ;

      MsgCatGroupReq *pGrpReq = (MsgCatGroupReq *)pMsg ;
      UINT32 groupID = pGrpReq->id.columns.groupID ;
      PD_TRACE1 ( SDB_CATNODEMGR_GRPREQ, PD_PACK_UINT ( groupID ) ) ;

      BSONObj boGroupInfo ;
      PD_CHECK( ( ( pmdIsPrimary() && SDB_CAT_MODULE_ACTIVE == _status ) ||
                  CATALOG_GROUPID == groupID ), SDB_CLS_NOT_PRIMARY, error,
                  PDWARNING, "Service deactive but received group-info-request"
                  "(groupID=%u)", groupID ) ;

      if ( 0 != groupID )
      {
         rc = catGetGroupObj( groupID, boGroupInfo, _pEduCB ) ;
      }
      else if ( pGrpReq->header.messageLength > (SINT32)sizeof(MsgCatGroupReq) )
      {
         const CHAR *name = (CHAR *)(&(pGrpReq->header)) +
                            sizeof(MsgCatGroupReq) ;
         rc = catGetGroupObj( name, FALSE, boGroupInfo, _pEduCB ) ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "Groupid and groupName are all not assigned" ) ;
      }

      PD_RC_CHECK( rc, PDERROR, "Failed to get group info, rc: %d", rc ) ;

      msgLen = sizeof( MsgCatGroupRes ) + boGroupInfo.objsize() ;
      pMsgRsp = (MsgCatGroupRes *)SDB_OSS_MALLOC( msgLen ) ;
      PD_CHECK( pMsgRsp, SDB_OOM, error, PDERROR,
                "Failed to alloc msg group-info request(size=%d)", msgLen ) ;
      pMsgRsp->header.res = SDB_OK ;
      pMsgRsp->header.header.messageLength = msgLen ;
      _fillRspHeader( &(pMsgRsp->header.header), &(pGrpReq->header) ) ;
      ossMemcpy( (CHAR *)pMsgRsp+sizeof(MsgCatGroupRes), boGroupInfo.objdata(),
                 boGroupInfo.objsize() );
      PD_TRACE1 ( SDB_CATNODEMGR_GRPREQ,
                  PD_PACK_INT ( rc ) ) ;
      rc = _pCatCB->netWork()->syncSend( handle, pMsgRsp ) ;
      PD_LOG( PDDEBUG, "Response the group request(succeed)" ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_GRPREQ, rc ) ;
      if ( pMsgRsp )
      {
         SDB_OSS_FREE( pMsgRsp ) ;
      }
      return rc ;
   error:
      PD_TRACE1 ( SDB_CATNODEMGR_GRPREQ, PD_PACK_INT ( rc ) ) ;
      rc = _sendFailedRsp( handle, rc, &(pGrpReq->header) ) ;
      PD_LOG( PDDEBUG, "Response the group request(failed)" ) ;
      goto done;
   }

   INT32 catNodeManager::_sendFailedRsp( NET_HANDLE handle, INT32 res,
                                         MsgHeader *reqMsg )
   {
      MsgInternalReplyHeader reply ;
      reply.res = res ;
      reply.header.messageLength = sizeof( MsgInternalReplyHeader ) ;
      _fillRspHeader( &(reply.header), reqMsg ) ;
      return _pCatCB->netWork()->syncSend( handle, (void*)&reply ) ;
   }

   void catNodeManager::_fillRspHeader( MsgHeader * rspMsg,
                                        const MsgHeader * reqMsg )
   {
      rspMsg->opCode = MAKE_REPLY_TYPE( reqMsg->opCode ) ;
      rspMsg->requestID = reqMsg->requestID ;
      rspMsg->routeID.value = 0 ;
      rspMsg->TID = reqMsg->TID ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_REGREQ, "catNodeManager::processRegReq" )
   INT32 catNodeManager::processRegReq( const NET_HANDLE &handle,
                                        MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      INT32 nodeRole = SDB_ROLE_DATA ;
      BSONObj boReq ;
      MsgCatRegisterRsp *pMsgRsp = NULL ;
      SINT32 msgLen = 0;
      MsgCatRegisterReq *pRegReq = (MsgCatRegisterReq *)pMsg ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_REGREQ ) ;
      BSONObj boNodeInfo;
      try
      {
         boReq = BSONObj( ( CHAR *)pRegReq + sizeof(MsgCatRegisterReq));
         PD_LOG( PDEVENT, "get register request:%s", boReq.toString().c_str());
         BSONElement beRole = boReq.getField( FIELD_NAME_ROLE );
         PD_CHECK( beRole.isNumber(), SDB_INVALIDARG, error,
                   PDERROR, "failed to get the field(%s)",
                   FIELD_NAME_ROLE ) ;
         nodeRole = beRole.number();
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_SYS, SDB_SYS, error, PDERROR,
                   "Failed to process register-request, received "
                   "unexpected error:%s", e.what() );
      }
      PD_CHECK( ( ( pmdIsPrimary() && SDB_CAT_MODULE_ACTIVE == _status )
                || SDB_ROLE_CATALOG == nodeRole ), SDB_CLS_NOT_PRIMARY,
                error, PDWARNING, "service deactive but received "
                "register-request:%s", boReq.toString().c_str() );
      PD_TRACE1 ( SDB_CATNODEMGR_REGREQ,
                  PD_PACK_STRING ( boReq.toString().c_str() ) ) ;
      rc = getNodeInfo( boReq, boNodeInfo );
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get node-info:%s (rc=%d)",
                  boReq.toString().c_str(), rc );
         rc = SDB_CAT_AUTH_FAILED;
         goto error;
      }

      msgLen = sizeof( MsgCatRegisterRsp ) + boNodeInfo.objsize();
      pMsgRsp = (MsgCatRegisterRsp *)SDB_OSS_MALLOC( msgLen );
      PD_CHECK( pMsgRsp!=NULL, SDB_OOM, error, PDERROR,
                "failed to build response(group-info request), malloc "
                "failed(size=%d)", msgLen );
      pMsgRsp->header.res = SDB_OK;
      pMsgRsp->header.header.messageLength = msgLen;
      pMsgRsp->header.header.opCode = MAKE_REPLY_TYPE(pRegReq->header.opCode);
      pMsgRsp->header.header.requestID = pRegReq->header.requestID;
      pMsgRsp->header.header.routeID.value = 0;
      pMsgRsp->header.header.TID = pRegReq->header.TID;
      ossMemcpy((CHAR *)pMsgRsp+sizeof(MsgCatRegisterRsp), boNodeInfo.objdata(),
                boNodeInfo.objsize() );
      PD_TRACE1 ( SDB_CATNODEMGR_REGREQ, PD_PACK_INT ( rc ) ) ;
      rc = _pCatCB->netWork()->syncSend( handle, pMsgRsp );
      SDB_OSS_FREE( pMsgRsp );
   done:
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_REGREQ, rc ) ;
      return rc;
   error:
      MsgCatRegisterRsp msgRsp;
      msgRsp.header.res = rc;
      msgRsp.header.header.messageLength = sizeof( MsgCatGroupRes );
      msgRsp.header.header.opCode = MAKE_REPLY_TYPE(pRegReq->header.opCode);
      msgRsp.header.header.requestID = pRegReq->header.requestID;
      msgRsp.header.header.routeID.value = 0;
      msgRsp.header.header.TID = pRegReq->header.TID;
      PD_TRACE1 ( SDB_CATNODEMGR_REGREQ, PD_PACK_INT ( rc ) ) ;
      rc = _pCatCB->netWork()->syncSend( handle, &msgRsp );
      goto done;
   }

   INT32 catNodeManager::processCommandMsg( const NET_HANDLE &handle,
                                            MsgHeader *pMsg,
                                            BOOLEAN writable )
   {
      INT32 rc = SDB_OK ;
      MsgOpQuery *pQueryReq = (MsgOpQuery *)pMsg ;

      MsgOpReply replyHeader ;
      CHAR       *replyData = NULL ;
      UINT32     replyDataLen = 0 ;
      INT32      returnNum    = 0 ;

      INT32 flag = 0 ;
      CHAR *pCMDName = NULL ;
      INT64 numToSkip = 0 ;
      INT64 numToReturn = 0 ;
      CHAR *pQuery = NULL ;
      CHAR *pFieldSelector = NULL ;
      CHAR *pOrderBy = NULL ;
      CHAR *pHint = NULL ;

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;
      _fillRspHeader( &(replyHeader.header), &(pQueryReq->header) ) ;

      rc = msgExtractQuery( (CHAR*)pMsg, &flag, &pCMDName, &numToSkip,
                            &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to extract query msg, rc: %d", rc ) ;

      if ( writable && SDB_CAT_MODULE_DEACTIVE == _status )
      {
         rc = SDB_CLS_NOT_PRIMARY ;
         PD_LOG ( PDWARNING, "Service deactive but received command: %s"
                  "opCode: %d", pCMDName, pQueryReq->header.opCode ) ;
         goto error ;
      }

      switch ( pQueryReq->header.opCode )
      {
         case MSG_CAT_CREATE_GROUP_REQ :
            rc = processCmdCreateGrp( pQuery ) ;
            break ;
         case MSG_CAT_CREATE_NODE_REQ :
            rc = processCmdCreateNode( handle, pQuery ) ;
            break ;
         case MSG_CAT_UPDATE_NODE_REQ :
            rc = processCmdUpdateNode( handle, pQuery, pFieldSelector ) ;
            break ;
         case MSG_CAT_DEL_NODE_REQ :
            rc = processCmdDelNode( handle, pQuery ) ;
            break ;
         default :
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "Recieved unknow command: %s, opCode: %d",
                    pCMDName, pQueryReq->header.opCode ) ;
            break ;
      }

      PD_RC_CHECK( rc, PDERROR, "Process command[%s] failed, opCode: %d, "
                   "rc: %d", pCMDName, pQueryReq->header.opCode, rc ) ;

   done:
      if ( !_pCatCB->isDelayed() )
      {
         if ( 0 == replyDataLen )
         {
            rc = _pCatCB->netWork()->syncSend( handle, (void*)&replyHeader ) ;
         }
         else
         {
            replyHeader.header.messageLength += replyDataLen ;
            replyHeader.numReturned = returnNum ;
            rc = _pCatCB->netWork()->syncSend( handle,
                                               &(replyHeader.header),
                                               (void*)replyData,
                                               replyDataLen ) ;
         }
      }
      if ( replyData )
      {
         SDB_OSS_FREE( replyData ) ;
      }
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_PCREATEGRP, "catNodeManager::processCmdCreateGrp" )
   INT32 catNodeManager::processCmdCreateGrp( const CHAR *pQuery )
   {
      INT32 rc = SDB_OK ;
      const CHAR *groupName = NULL ;

      PD_TRACE_ENTRY ( SDB_CATNODEMGR_PCREATEGRP ) ;

      try
      {
         BSONObj boQuery( pQuery ) ;
         rc = rtnGetStringElement( boQuery, CAT_GROUPNAME_NAME, &groupName ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get the field: %s, rc: %d",
                      CAT_GROUPNAME_NAME, rc ) ;

         PD_TRACE1 ( SDB_CATNODEMGR_PCREATEGRP,
                     PD_PACK_STRING ( groupName ) ) ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR, "occured unexpected error:%s", e.what() );
         goto error;
      }

      rc = _createGrp( groupName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to process create group request(rc=%d)",
                   rc ) ;

   done:
      PD_TRACE1 ( SDB_CATNODEMGR_PCREATEGRP, PD_PACK_INT ( rc ) ) ;
      PD_TRACE_EXITRC( SDB_CATNODEMGR_PCREATEGRP, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_CREATENODE, "catNodeManager::processCreateNode" )
   INT32 catNodeManager::processCmdCreateNode( const NET_HANDLE &handle,
                                               const CHAR *pQuery )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_CREATENODE ) ;
      BOOLEAN isLocalConnection = FALSE ;

      NET_EH eh = _pCatCB->netWork()->getFrame()->getEventHandle( handle ) ;
      if ( eh.get() )
      {
         isLocalConnection = eh->isLocalConnection() ;
      }
      else
      {
         rc = SDB_NETWORK_CLOSE ;
         goto error ;
      }

      rc = _createNode( pQuery, isLocalConnection ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to create node, rc: %d", rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_CREATENODE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_UPDATENODE, "catNodeManager::processCmdUpdateNode" )
   INT32 catNodeManager::processCmdUpdateNode( const NET_HANDLE &handle,
                                               const CHAR *pQuery,
                                               const CHAR *pSelector )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_UPDATENODE ) ;
      BOOLEAN isLocalConnection = FALSE ;

      NET_EH eh = _pCatCB->netWork()->getFrame()->getEventHandle( handle ) ;
      if ( eh.get() )
      {
         isLocalConnection = eh->isLocalConnection() ;
      }
      else
      {
         rc = SDB_NETWORK_CLOSE ;
         goto error ;
      }

      try
      {
         BSONObj query( pQuery ) ;
         BSONObj seletor( pSelector ) ;
         BSONObj groupInfo ;
         INT32 nodeID = CAT_INVALID_NODEID ;

         rc = rtnGetIntElement( query, FIELD_NAME_NODEID, nodeID ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                      FIELD_NAME_NODEID, rc ) ;

         rc = catGetGroupObj( (UINT16)nodeID, groupInfo, _pEduCB ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get group info by nodeid[%d], "
                      "rc: %d", nodeID, rc ) ;

         rc = _updateNodeToGrp( groupInfo, seletor, (UINT16)nodeID,
                                isLocalConnection ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to update node to group, rc: %d",
                      rc ) ;
      }
      catch( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Occured exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_UPDATENODE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_DELNODE, "catNodeManager::processCmdDelNode" )
   INT32 catNodeManager::processCmdDelNode( const NET_HANDLE &handle,
                                            const CHAR *pQuery )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_DELNODE ) ;

      BOOLEAN isLocalConnection = FALSE ;

      NET_EH eh = _pCatCB->netWork()->getFrame()->getEventHandle( handle ) ;
      if ( eh.get() )
      {
         isLocalConnection = eh->isLocalConnection() ;
      }
      else
      {
         rc = SDB_NETWORK_CLOSE ;
         goto error ;
      }

      try
      {
         BSONObj query( pQuery ) ;
         rc = _delNode( query, isLocalConnection ) ;
         PD_RC_CHECK( rc, PDERROR, "Delete node[%s] failed, rc: %d",
                      query.toString().c_str(), rc ) ;
      }
      catch( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Ouccured exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      PD_TRACE1 ( SDB_CATNODEMGR_DELNODE, PD_PACK_INT ( rc ) ) ;
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_DELNODE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_PREMOVEGRP, "catNodeManager::processRemoveGrp" )
   INT32 catNodeManager::processRemoveGrp( const NET_HANDLE &handle,
                                           MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      MsgOpQuery *pQueryReq = (MsgOpQuery *)pMsg ;
      MsgOpReply replyHeader;
      const CHAR *strGroupName = NULL ;
      INT32 flag;
      CHAR *pCMDName = NULL;
      SINT64 numToSkip;
      SINT64 numToReturn;
      CHAR *pQuery = NULL;
      CHAR *pFieldSelector = NULL;
      CHAR *pOrderBy = NULL;
      CHAR *pHint = NULL;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_PREMOVEGRP ) ;
      replyHeader.header.messageLength = sizeof(MsgOpReply);
      replyHeader.header.opCode = MSG_CAT_RM_GROUP_RES;
      replyHeader.header.requestID = pQueryReq->header.requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID = pQueryReq->header.TID;
      replyHeader.contextID = -1;
      replyHeader.flags = SDB_OK;
      replyHeader.numReturned = 0;
      replyHeader.startFrom = 0;

      if ( SDB_CAT_MODULE_DEACTIVE == _status )
      {
         rc = SDB_CLS_NOT_PRIMARY;
         PD_LOG ( PDWARNING, "Service deactive but received remove-group "
                  "request" );
         goto error;
      }
      rc = msgExtractQuery( (CHAR*)pMsg, &flag, &pCMDName, &numToSkip,
                            &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint ) ;
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to parse remove group request(rc=%d)",
                  rc );
         goto error;
      }

      try
      {
         BSONObj boQuery( pQuery );
         BSONElement beGroupName = boQuery.getField(CAT_GROUPNAME_NAME);
         if ( beGroupName.eoo() || beGroupName.type()!=String )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "failed to get the field:%s",
                     CAT_GROUPNAME_NAME );
            goto error;
         }
         strGroupName = beGroupName.valuestr();
         PD_TRACE1 ( SDB_CATNODEMGR_PCREATEGRP,
                     PD_PACK_STRING ( strGroupName ) ) ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG;
         PD_LOG ( PDERROR, "occured unexpected error:%s", e.what() );
         goto error;
      }

      rc = removeGrp( strGroupName ) ;
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "failed to process remove group request(rc=%d)",
                  rc );
         goto error;
      }
   done:
      replyHeader.flags = rc;
      PD_TRACE1 ( SDB_CATNODEMGR_PREMOVEGRP,
                  PD_PACK_INT ( rc ) ) ;
      rc = _pCatCB->netWork()->syncSend( handle, &replyHeader );
      PD_TRACE_EXITRC( SDB_CATNODEMGR_PCREATEGRP, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_ACTIVEGRP, "catNodeManager::processActiveGrp" )
   INT32 catNodeManager::processActiveGrp( const NET_HANDLE &handle,
                                           MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      MsgOpQuery *pQueryMsg = (MsgOpQuery *)pMsg ;
      MsgOpReply replyHeader ;
      CHAR *pBuffer = NULL ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_ACTIVEGRP ) ;

      do
      {
         if ( SDB_CAT_MODULE_DEACTIVE == _status )
         {
            rc = SDB_CLS_NOT_PRIMARY ;
            PD_LOG ( PDWARNING, "Service deactive but received active-node "
                     "request" ) ;
            break ;
         }
         INT32 flag ;
         CHAR *pCMDName = NULL ;
         SINT64 numToSkip = 0 ;
         SINT64 numToReturn = 0 ;
         CHAR *pQuery = NULL ;
         CHAR *pFieldSelector = NULL ;
         CHAR *pOrderBy = NULL ;
         CHAR *pHint = NULL ;
         rc = msgExtractQuery( (CHAR*)pMsg, &flag, &pCMDName, &numToSkip,
                               &numToReturn, &pQuery, &pFieldSelector,
                               &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to parse add node request(rc=%d)",
                     rc );
            break;
         }

         const CHAR *strGroupName = NULL ;
         try
         {
            BSONObj boQuery( pQuery );
            BSONElement beGroupName = boQuery.getField(CAT_GROUPNAME_NAME);
            if ( beGroupName.eoo() || beGroupName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to get the field:%s",
                        CAT_GROUPNAME_NAME ) ;
               break;
            }
            strGroupName = beGroupName.valuestr();
            PD_TRACE1 ( SDB_CATNODEMGR_ACTIVEGRP,
                        PD_PACK_STRING ( strGroupName ) ) ;
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR, "occured unexpected error:%s", e.what() );
            break;
         }
         BSONObj boGroupInfo;
         rc = activeGrp( strGroupName, 0, boGroupInfo );
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR, "Failed to active group(rc=%d)", rc ) ;
            break ;
         }
         INT32 bufferSize = 0;
         rc = msgBuildReplyMsg( &pBuffer, &bufferSize,
                                MSG_CAT_ACTIVE_GROUP_RSP, 0, -1, 0, 1,
                                pQueryMsg->header.requestID, &boGroupInfo ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR, "Failed to build reply(rc=%d)", rc );
            break;
         }
         MsgOpReply *pReply = (MsgOpReply *)pBuffer;
         pReply->header.routeID.value = 0;
         pReply->header.TID = pQueryMsg->header.TID;
      }while ( FALSE ) ;

      if ( rc != SDB_OK || NULL == pBuffer )
      {
         PD_LOG ( PDERROR, "Failed to process add node request(rc=%d)", rc ) ;
         replyHeader.header.messageLength = sizeof(MsgOpReply);
         replyHeader.header.opCode = MSG_CAT_ACTIVE_GROUP_RSP;
         replyHeader.header.requestID = pQueryMsg->header.requestID;
         replyHeader.header.routeID.value = 0;
         replyHeader.header.TID = pQueryMsg->header.TID;
         replyHeader.contextID = -1;
         replyHeader.flags = rc;
         replyHeader.numReturned = 0;
         replyHeader.startFrom = 0;
         PD_TRACE1 ( SDB_CATNODEMGR_ACTIVEGRP,
                     PD_PACK_INT ( rc ) ) ;
         rc = _pCatCB->netWork()->syncSend( handle, &replyHeader );
      }
      else
      {
         PD_TRACE1 ( SDB_CATNODEMGR_ACTIVEGRP,
                     PD_PACK_INT ( rc ) ) ;
         rc = _pCatCB->netWork()->syncSend( handle, pBuffer );
      }
      if ( NULL != pBuffer )
      {
         SDB_OSS_FREE(pBuffer);
      }
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_ACTIVEGRP, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_READCATACONF, "catNodeManager::readCataConf" )
   INT32 catNodeManager::readCataConf()
   {
      INT32 rc = SDB_OK;
      BOOLEAN isFileOpened = FALSE;
      SINT32 sBufferBegin = 0;
      INT32 iReadReturn = SDB_OK;

      const CHAR *szCatFilePath = pmdGetOptionCB()->getCatFile() ;
      CHAR szBuffer[READ_BUFFER_SIZE] = {0};
      OSSFILE catFile;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_READCATACONF ) ;
      PD_LOG ( PDINFO, "read catalogue-nodes info from configure file" );
      rc = ossOpen( szCatFilePath, OSS_READONLY, 0, catFile );
      if ( rc )
      {
         PD_LOG( PDINFO, "Failed to open the catalog-configure-file(rc=%d)",
                 rc ) ;
         rc = SDB_OK ;
         goto done ;
      }
      isFileOpened = TRUE;

      while( iReadReturn != SDB_EOF )
      {
         SINT64 sReadSize = 0;
         iReadReturn = ossRead ( &catFile, szBuffer + sBufferBegin,
                                 READ_BUFFER_SIZE-sBufferBegin-1,
                                 &sReadSize ) ;
         if ( iReadReturn != SDB_OK && iReadReturn != SDB_EOF &&
              iReadReturn != SDB_INTERRUPT )
         {
            rc = iReadReturn;
            PD_LOG( PDERROR, "read catalogue-configure-file error: %s, rc: %d",
                    szCatFilePath, rc ) ;
            goto error;
         }
         SINT64 sContentLen = sBufferBegin + sReadSize;
         szBuffer[sContentLen] = '\0';

         SINT64 sParseBytes = 0;
         PD_TRACE1 ( SDB_CATNODEMGR_READCATACONF,
                     PD_PACK_RAW ( szBuffer, sContentLen ) ) ;
         rc = parseCatalogConf( szBuffer, sContentLen + 1, sParseBytes);
         if ( rc != SDB_OK )
         {
            PD_LOG( PDERROR, "parse catalogue-configure-file error: %s, rc: %d",
                    szCatFilePath, rc );
            goto error;
         }

         if ( SDB_EOF == iReadReturn )
         {
            rc = SDB_OK;
            break;
         }

         if ( sContentLen > sParseBytes )
         {
            ossMemcpy( szBuffer, szBuffer + sParseBytes,
                       sContentLen - sParseBytes );
            sBufferBegin = sContentLen - sParseBytes;
         }
         else
         {
            sBufferBegin = 0;
         }
      }//end of "while( iReadReturn != SDB_EOF)"
      if ( isFileOpened )
      {
         ossClose( catFile );
         isFileOpened = FALSE;
      }
      ossDelete( szCatFilePath );

   done:
      if ( isFileOpened )
      {
         ossClose( catFile );
         isFileOpened = FALSE;
      }
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_READCATACONF, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_PARSECATCONF, "catNodeManager::parseCatalogConf" )
   INT32 catNodeManager::parseCatalogConf( CHAR *pData,
                                           const SINT64 sDataSize,
                                           SINT64 &sParseBytes )
   {
      SINT64 sEnd = 0;
      SINT64 sBegin = 0;
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_PARSECATCONF ) ;
      while ( sEnd < sDataSize )
      {
         if ( 0x0D != *(pData + sEnd) && 0x0A != *(pData + sEnd) &&
              '\0' != *(pData + sEnd) )
         {
            ++sEnd;
            continue;
         }
         *( pData + sEnd ) = '\0';

         BSONObj obj;
         rc = parseLine( pData+sBegin, obj );
         if ( rc != SDB_OK )
         {
            break;
         }
         BSONObj boGroupInfo;
         rc = generateGroupInfo( obj, boGroupInfo );
         if ( rc != SDB_OK )
         {
            break;
         }
         PD_TRACE1 ( SDB_CATNODEMGR_PARSECATCONF,
                     PD_PACK_STRING ( boGroupInfo.toString().c_str() ) ) ;
         if ( 0 != boGroupInfo.nFields() )
         {
            rc = saveGroupInfo( boGroupInfo, 1 );
         }
         if ( rc != SDB_OK )
         {
            break;
         }
         sParseBytes = sEnd;

         sBegin = sEnd + 1;
         ++sEnd;
      }//end of while ( sEnd < sDataSize )
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_PARSECATCONF, rc ) ;
      return rc;
   }//end of parseCatalogConf()

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_SAVEGRPINFO, "catNodeManager::saveGroupInfo" )
   INT32 catNodeManager::saveGroupInfo ( BSONObj &boGroupInfo,
                                         INT16 w )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_SAVEGRPINFO ) ;
      BSONElement beGrpId = boGroupInfo.getField( FIELD_NAME_GROUPID );
      BSONObjBuilder bobMatcher;
      BSONObjBuilder bobUpdater;
      BSONObj boMatcher;
      BSONObj boUpdater;
      BSONObj boHint ;
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;

      PD_CHECK( beGrpId.isNumber(), SDB_INVALIDARG, error, PDERROR,
                "Failed to get the field(%s), save group-info failed",
                FIELD_NAME_GROUPID ) ;
      bobMatcher.append( beGrpId ) ;
      boMatcher = bobMatcher.obj() ;

      bobUpdater.append("$set", boGroupInfo );
      boUpdater = bobUpdater.obj();
      rc = rtnUpdate( CAT_NODE_INFO_COLLECTION, boMatcher, boUpdater,
                      boHint, FLG_UPDATE_UPSERT, cb, _pDmsCB,
                      _pDpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "failed to update the group(%d) info",
                   beGrpId.number() ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_SAVEGRPINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_GENGROUPINFO, "catNodeManager::generateGroupInfo" )
   INT32 catNodeManager::generateGroupInfo( BSONObj &boConf,
                                            BSONObj &boGroupInfo )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_GENGROUPINFO ) ;
      try
      {
         if ( 0 == boConf.nFields() )
         {
            goto done;
         }
         BSONObjBuilder bobGroupInfo;
         bobGroupInfo.append( FIELD_NAME_GROUPNAME, CATALOG_GROUPNAME );
         bobGroupInfo.append( FIELD_NAME_GROUPID, CATALOG_GROUPID );
         bobGroupInfo.append( FIELD_NAME_ROLE, SDB_ROLE_CATALOG );
         bobGroupInfo.append( FIELD_NAME_VERSION, CAT_VERSION_BEGIN );
         bobGroupInfo.append( FIELD_NAME_GROUP_STATUS, SDB_CAT_GRP_ACTIVE );

         BSONObjBuilder bobNodeInfo;
         rc = _getNodeInfoByConf( boConf, bobNodeInfo );
         PD_RC_CHECK(rc, PDERROR,
                     "failed to get catalog-node info(rc=%d)", rc );
         UINT16 nodeID = _pCatCB->AllocCataNodeID();
         PD_TRACE1 ( SDB_CATNODEMGR_GENGROUPINFO,
                     PD_PACK_USHORT ( nodeID ) ) ;
         PD_CHECK( nodeID!=CAT_INVALID_NODEID, SDB_SYS, error,
                  PDERROR, "failed to allocate nodeId" );
         bobNodeInfo.append( FIELD_NAME_NODEID, nodeID );
         BSONObj boNodeInfo = bobNodeInfo.obj();
         BSONArrayBuilder babGroup;
         babGroup.append( boNodeInfo );
         bobGroupInfo.appendArray( FIELD_NAME_GROUP, babGroup.arr() );
         boGroupInfo = bobGroupInfo.obj();
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Unexpected exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_GENGROUPINFO, rc ) ;
      return rc;
   error:
      goto done;
   }//end of parseBsonObj()

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_GETNODEINFOBYCONF, "catNodeManager::_getNodeInfoByConf" )
   INT32 catNodeManager::_getNodeInfoByConf( BSONObj &boConf,
                                             BSONObjBuilder &bobNodeInfo )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_GETNODEINFOBYCONF ) ;
      PD_TRACE1 ( SDB_CATNODEMGR_GETNODEINFOBYCONF,
                  PD_PACK_STRING ( boConf.toString().c_str() ) ) ;
      do
      {
         try
         {
            BSONArrayBuilder babSvcArray;
            BSONElement beLocalSvc = boConf.getField( PMD_OPTION_SVCNAME );
            if ( beLocalSvc.eoo() || beLocalSvc.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "failed to get the field(%s)",
                        PMD_OPTION_SVCNAME );
               break;
            }
            BSONObjBuilder bobLocalSvc;
            bobLocalSvc.append( FIELD_NAME_SERVICE_TYPE, MSG_ROUTE_LOCAL_SERVICE );
            bobLocalSvc.appendAs( beLocalSvc, FIELD_NAME_NAME );
            babSvcArray.append( bobLocalSvc.obj() );

            UINT16 svcPort = 0;
            std::string strSvc = beLocalSvc.str();
            ossSocket::getPort ( strSvc.c_str(), svcPort ) ;

            BSONObjBuilder bobReplSvc;
            bobReplSvc.append( FIELD_NAME_SERVICE_TYPE, MSG_ROUTE_REPL_SERVICE );
            BSONElement beReplSvc = boConf.getField( PMD_OPTION_REPLNAME );
            if ( !beReplSvc.eoo() && beReplSvc.type()==String )
            {
               bobReplSvc.appendAs( beReplSvc, FIELD_NAME_NAME );
            }
            else if ( svcPort != 0 )
            {
               CHAR szPort[CAT_PORT_STR_SZ] = {0};
               UINT16 port = svcPort + MSG_ROUTE_REPL_SERVICE;
               ossItoa( port, &szPort[0], CAT_PORT_STR_SZ );
               bobReplSvc.append( FIELD_NAME_NAME, szPort );
            }
            else
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "failed to get the field(%s)",
                        PMD_OPTION_REPLNAME );
               break;
            }
            babSvcArray.append( bobReplSvc.obj() );

            BSONObjBuilder bobShardSvc;
            bobShardSvc.append( FIELD_NAME_SERVICE_TYPE, MSG_ROUTE_SHARD_SERVCIE );
            BSONElement beShardSvc = boConf.getField( PMD_OPTION_SHARDNAME );
            if ( !beShardSvc.eoo() && beShardSvc.type()==String )
            {
               bobShardSvc.appendAs( beShardSvc, FIELD_NAME_NAME );
            }
            else if ( svcPort != 0 )
            {
               CHAR szPort[CAT_PORT_STR_SZ] = {0};
               UINT16 port = svcPort + MSG_ROUTE_SHARD_SERVCIE;
               ossItoa( port, &szPort[0], CAT_PORT_STR_SZ );
               bobShardSvc.append( FIELD_NAME_NAME, szPort );
            }
            else
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "failed to get the field(%s)",
                        PMD_OPTION_SHARDNAME );
               break;
            }
            babSvcArray.append( bobShardSvc.obj() );

            BSONObjBuilder bobCataSvc;
            bobCataSvc.append( FIELD_NAME_SERVICE_TYPE, MSG_ROUTE_CAT_SERVICE );
            BSONElement beCataSvc = boConf.getField( PMD_OPTION_CATANAME );
            if ( !beCataSvc.eoo() && beCataSvc.type()==String )
            {
               bobCataSvc.appendAs( beCataSvc, FIELD_NAME_NAME );
            }
            else if ( svcPort != 0 )
            {
               CHAR szPort[CAT_PORT_STR_SZ] = {0};
               UINT16 port = svcPort + MSG_ROUTE_CAT_SERVICE;
               ossItoa( port, &szPort[0], CAT_PORT_STR_SZ );
               bobCataSvc.append( FIELD_NAME_NAME, szPort );
            }
            else
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "failed to get the field(%s)",
                        PMD_OPTION_CATANAME );
               break;
            }
            babSvcArray.append( bobCataSvc.obj() );

            BSONElement beHostName = boConf.getField( FIELD_NAME_HOST );
            if ( beHostName.eoo() || beHostName.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "failed to get the field(%s)",
                        FIELD_NAME_HOST );
               break;
            }
            BSONElement beDBPath = boConf.getField( PMD_OPTION_DBPATH );
            if ( beDBPath.eoo() || beDBPath.type()!=String )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR,
                        "failed to get the field(%s)",
                        PMD_OPTION_DBPATH );
               break;
            }
            bobNodeInfo.append( beDBPath );
            bobNodeInfo.append( beHostName );
            bobNodeInfo.appendArray( FIELD_NAME_SERVICE, babSvcArray.arr() );
         }
         catch( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG( PDERROR,
                  "occured unexpected error:%s",
                  e.what() );
            break;
         }
      }while ( FALSE );
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_GETNODEINFOBYCONF, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_PARSELINE, "catNodeManager::parseLine" )
   INT32 catNodeManager::parseLine( const CHAR *pLine, BSONObj &obj )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT ( pLine, "line can't be NULL" ) ;
      const CHAR *pBegin = pLine;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_PARSELINE ) ;
      while ( '\0' != *pBegin )
      {
         if ( *pBegin != '\t' && *pBegin !=' ' )
         {
            break;
         }
         ++pBegin;
      }
      if ( '#' != *pBegin )
      {
         rc = fromjson ( pBegin, obj ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to parse line into BSONObj: %s",
                     pBegin ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_PARSELINE, rc ) ;
      return rc;
   error :
      goto done ;
   }//end of parseLine()

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_PARSEIDINFO, "catNodeManager::parseIDInfo" )
   INT32 catNodeManager::parseIDInfo( BSONObj &obj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_PARSEIDINFO ) ;
      PD_TRACE1 ( SDB_CATNODEMGR_PARSEIDINFO,
                  PD_PACK_STRING ( obj.toString().c_str() ) ) ;
      try
      {
         MsgRouteID routeID;
         BOOLEAN isGrpActive = FALSE;
         BSONElement beGrpID = obj.getField ( CAT_GROUPID_NAME );
         if ( beGrpID.eoo() || ! beGrpID.isNumber() )
         {
            PD_LOG( PDWARNING, "Failed to get the field(%s)",
                    CAT_GROUPID_NAME );
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         {
            BSONElement beGrpStatus = obj.getField ( CAT_GROUP_STATUS );
            if ( !beGrpStatus.isNumber() ||
                 SDB_CAT_GRP_ACTIVE == beGrpStatus.numberInt() )
            {
               isGrpActive = TRUE ;
            }
            _pCatCB->insertGroupID( beGrpID.numberInt(), isGrpActive );
            routeID.columns.groupID = beGrpID.numberInt();

            BSONElement beNodes = obj.getField ( CAT_GROUP_NAME );
            if ( beNodes.eoo() || beNodes.type()!= Array )
            {
               PD_LOG( PDINFO, "Failed to get the field(%s), usually it "
                       "means one or more replica groups are empty",
                       CAT_GROUP_NAME );
               rc = SDB_OK ;
               goto done ;
            }
            BSONObjIterator i( beNodes.embeddedObject() );
            while ( i.more() )
            {
               BSONElement beTmp = i.next();

               BSONObj boTmp = beTmp.embeddedObject();
               BSONElement beNodeID = boTmp.getField( CAT_NODEID_NAME );
               BSONElement beHost = boTmp.getField( CAT_HOST_FIELD_NAME );
               BSONElement beService = boTmp.getField( CAT_SERVICE_FIELD_NAME );
               if ( beNodeID.eoo() || ! beNodeID.isNumber() )
               {
                  PD_LOG( PDWARNING, "Failed to get the field(%s)",
                          CAT_NODEID_NAME );
                  rc = SDB_INVALIDARG;
                  goto error ;
               }
               _pCatCB->insertNodeID( beNodeID.numberInt() );
               routeID.columns.nodeID = beNodeID.numberInt();

               if ( beHost.eoo() || beHost.type()!=String )
               {
                  PD_LOG( PDWARNING, "Failed to get the field(%s)",
                          CAT_HOST_FIELD_NAME );
                  rc = SDB_INVALIDARG;
                  goto error ;
               }

               if ( beService.eoo() || beService.type()!=Array )
               {
                  PD_LOG( PDWARNING, "Failed to get the field(%s)",
                          CAT_SERVICE_FIELD_NAME );
                  rc = SDB_INVALIDARG;
                  goto error ;
               }
               {
                  BSONObjIterator j( beService.embeddedObject() );
                  while ( j.more() )
                  {
                     BSONElement beServiceTmp = j.next();
                     BSONObj boServiceTmp = beServiceTmp.embeddedObject();
                     BSONElement beServiceType = boServiceTmp.getField(
                           CAT_SERVICE_TYPE_FIELD_NAME );
                     BSONElement beServiceName = boServiceTmp.getField(
                           CAT_SERVICE_NAME_FIELD_NAME );
                     if ( beServiceType.eoo() || !beServiceType.isNumber() )
                     {
                        PD_LOG( PDWARNING, "Failed to get the field(%s)",
                                CAT_NODEID_NAME );
                        rc = SDB_INVALIDARG;
                        goto error ;
                     }
                     routeID.columns.serviceID = beServiceType.numberInt();

                     if ( beServiceName.eoo() || beServiceName.type()!=String )
                     {
                        PD_LOG( PDWARNING, "Failed to get the field(%s)",
                                CAT_NODEID_NAME );
                        rc = SDB_INVALIDARG;
                        goto error ;
                     }
                     rc = _pCatCB->netWork()->updateRoute( routeID,
                           beHost.String().c_str(),
                           beServiceName.String().c_str() ) ;
                     if ( rc && SDB_NET_UPDATE_EXISTING_NODE != rc )
                     {
                        PD_LOG( PDWARNING, "Failed to update route(rc = %d)",
                                rc );
                        goto error ;
                     }
                     rc = SDB_OK;
                  }//end of while ( j.moreWithEOO() )
               }
            }//end of while ( i.moreWithEOO() )
         }
      } // try
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "unexpected exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_PARSEIDINFO, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_GETNODEINFO, "catNodeManager::getNodeInfo" )
   INT32 catNodeManager::getNodeInfo( const BSONObj &boReq, BSONObj &boNodeInfo )
   {
      INT32 rc                         = SDB_OK;
      SINT64 sContextID                = -1;
      BSONObj boMatcher ;
      BSONObj boOrderBy;
      BSONObj boHint;
      CHAR szBuf[ OP_MAXNAMELENGTH+1 ] = {0};

      rtnContextBuf buffObj ;
      BOOLEAN found                    = FALSE ;
      const CHAR *strShardServiceName  = NULL ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_GETNODEINFO ) ;
      PD_TRACE1 ( SDB_CATNODEMGR_GETNODEINFO,
                  PD_PACK_STRING ( boReq.toString().c_str() ) ) ;
      try
      {
         BSONObj boSelector;

         BSONElement beHostName = boReq.getField( CAT_HOST_FIELD_NAME);
         if ( beHostName.type()!=String )
         {
            PD_LOG ( PDERROR,
                     "failed to get the field: %s", CAT_HOST_FIELD_NAME );
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         BSONElement beServiceName = boReq.getField( CAT_SERVICE_FIELD_NAME );
         if ( beServiceName.type()!=Array )
         {
            PD_LOG( PDERROR,
                    "failed to get the field: %s", CAT_SERVICE_FIELD_NAME );
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         strShardServiceName = getShardServiceName ( beServiceName ) ;
         if ( !strShardServiceName || strShardServiceName[0] == '\0' )
         {
            PD_LOG( PDERROR,
                    "failed to get the shard service name" );
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         BSONElement beHostIP = boReq.getField( CAT_IP_FIELD_NAME );
         if ( beHostIP.type() == Array )
         {
            BSONArrayBuilder hostNameArrayBuilder;

            hostNameArrayBuilder.append( BSON(CAT_MATCHER_HOST_NAME << beHostName.valuestr()));

            BSONObjIterator iter( beHostIP.embeddedObject() );
            while ( iter.more() )
            {
               BSONElement ip = iter.next();
               if (ip.type() != String)
               {
                  continue;
               }

               hostNameArrayBuilder.append( BSON(CAT_MATCHER_HOST_NAME << ip.valuestr()));
            }

            boMatcher = BSON( FIELD_NAME_GROUP
                              << BSON("$elemMatch"
                                    << BSON( "$or" << hostNameArrayBuilder.arr()
                                          << CAT_MATCHER_SERVICE_NAME
                                             << strShardServiceName )));
         }
         else
         {
            boMatcher = BSON( FIELD_NAME_GROUP
                              << BSON("$elemMatch"
                                    << BSON(CAT_MATCHER_HOST_NAME
                                             << beHostName.valuestr()
                                          << CAT_MATCHER_SERVICE_NAME
                                             << strShardServiceName )));
         }

         ossStrncpy( szBuf, CAT_NODE_INFO_COLLECTION, OP_MAXNAMELENGTH);
         rc = rtnQuery ( szBuf, boSelector, boMatcher,
                         boOrderBy, boHint, 0, _pEduCB, 0, -1, _pDmsCB,
                         _pRtnCB, sContextID);
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to query from collection %s, rc = %d",
                     CAT_NODE_INFO_COLLECTION, rc ) ;
            goto error ;
         }
         rc = rtnGetMore( sContextID, 1, buffObj, _pEduCB,
                          _pRtnCB );
         if ( rc )
         {
            if ( SDB_DMS_EOC != rc )
            {
               PD_LOG ( PDERROR, "Failed to get more from colleciton %s, "
                        "rc = %d", CAT_NODE_INFO_COLLECTION, rc ) ;
            }
            else
            {
               rc = SDB_CLS_NODE_NOT_EXIST ;
            }
            goto error ;
         }

         {
            BSONObj boGrpInfo ( buffObj.data() );
            PD_TRACE1 ( SDB_CATNODEMGR_GETNODEINFO,
                        PD_PACK_STRING ( boGrpInfo.toString().c_str() ) ) ;
            BSONElement beGroup = boGrpInfo.getField( CAT_GROUP_NAME );
            if ( !beGroup.eoo() && beGroup.type()==Array )
            {
               BSONObjIterator i(beGroup.embeddedObject());
               BSONObj boTmp;
               while( i.more() )
               {
                  const CHAR *strShardServiceNameTmp = NULL ;
                  BSONElement beTmp = i.next();
                  if ( beTmp.type() != Object )
                  {
                     rc = SDB_CAT_CORRUPTION ;
                     PD_LOG ( PDERROR,
                              "Field is not Object: %s",
                              beTmp.toString().c_str() ) ;
                     goto error ;
                  }
                  {
                     boTmp = beTmp.embeddedObject();
                     BSONElement beHostNameTmp =
                           boTmp.getField( CAT_HOST_FIELD_NAME );
                     BSONElement beServiceTmp =
                           boTmp.getField( CAT_SERVICE_FIELD_NAME );
                     if ( beHostNameTmp.eoo() || beHostNameTmp.type()!= String )
                     {
                        rc = SDB_CAT_CORRUPTION ;
                        PD_LOG( PDERROR,
                                "failed to get the field: %s",
                                CAT_HOST_FIELD_NAME );
                        goto error ;
                     }
                     if ( ossStrcmp ( beHostNameTmp.valuestr(),
                                      beHostName.valuestr() ) != 0 )
                     {
                        if ( beHostIP.type() != Array )
                        {
                           continue;
                        }

                        BOOLEAN isIP = FALSE;
                        BSONObjIterator iter( beHostIP.embeddedObject() );
                        while ( iter.more() )
                        {
                           BSONElement ip = iter.next();
                           if (ip.type() != String)
                           {
                              continue ;
                           }

                           if (ossStrcmp ( beHostNameTmp.valuestr(),
                                      ip.valuestr() ) == 0 )
                           {
                              isIP = TRUE;
                              break;
                           }
                        }

                        if ( !isIP )
                        {
                           continue ;
                        }
                     }
                     if ( beServiceTmp.eoo() || beServiceTmp.type()!=Array )
                     {
                        rc = SDB_CAT_CORRUPTION ;
                        PD_LOG( PDERROR,
                                "failed to get the field: %s",
                                CAT_HOST_FIELD_NAME );
                        goto error ;
                     }
                     strShardServiceNameTmp = getShardServiceName
                           ( beServiceTmp ) ;
                     if ( !strShardServiceNameTmp ||
                           strShardServiceNameTmp[0] == '\0' )
                     {
                        rc = SDB_CAT_CORRUPTION ;
                        PD_LOG( PDERROR,
                                "failed to get the shard service name" );
                        goto error ;
                     }
                     if ( ossStrcmp ( strShardServiceName,
                                      strShardServiceNameTmp ) == 0 )
                     {
                        found = TRUE ;
                        break ;
                     }
                  }
               } // while ( i.more() )

               if ( found )
               {
                  BSONObjBuilder bobNodeInfo;
                  bobNodeInfo.append( boGrpInfo.getField(CAT_TYPE_FIELD_NAME) );
                  bobNodeInfo.append( boGrpInfo.getField(CAT_GROUPID_NAME) );
                  bobNodeInfo.append( boTmp.getField(CAT_HOST_FIELD_NAME) );
                  bobNodeInfo.append( boTmp.getField(CAT_NODEID_NAME) );
                  bobNodeInfo.append( boTmp.getField(CAT_SERVICE_FIELD_NAME) );
                  boNodeInfo = bobNodeInfo.obj();
               }
            } // if ( !beGroup.eoo() && beGroup.type()==Array )
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR,
                  "unexpected exception: %s", e.what() ) ;
         rc = SDB_CAT_CORRUPTION ;
      }

   done :
      if ( sContextID != -1 )
      {
         _pRtnCB->contextDelete ( sContextID, _pEduCB ) ;
      }
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_GETNODEINFO, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_CREATEGRP, "catNodeManager::_createGrp" )
   INT32 catNodeManager::_createGrp( const CHAR *groupName )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_CREATEGRP ) ;

      BOOLEAN bExist = FALSE ;
      UINT32  newGroupID = CAT_INVALID_GROUPID ;
      INT32   role = SDB_ROLE_DATA ;
      INT32   status = SDB_CAT_GRP_DEACTIVE ;

      if ( 0 == ossStrcmp( groupName, COORD_GROUPNAME ) )
      {
         newGroupID = COORD_GROUPID ;
         role = SDB_ROLE_COORD ;
         status = SDB_CAT_GRP_ACTIVE ;
      }
      else
      {
         rc = catGroupNameValidate( groupName, FALSE ) ;
         PD_RC_CHECK( rc, PDERROR, "Group name[%s] is invalid", groupName ) ;
      }

      rc = catGroupCheck( groupName, bExist, _pEduCB ) ;
      PD_RC_CHECK( rc, PDERROR, "Check group name[%s] exist failed, rc: %d",
                   groupName, rc ) ;
      PD_CHECK( FALSE == bExist, SDB_CAT_GRP_EXIST, error, PDERROR,
                "Create group failed, the group[%s] existed", groupName ) ;

      {
         catGroupLock groupLock( groupName ) ;
         if ( !groupLock.tryLock( EXCLUSIVE ) )
         {
            if ( !_pCatCB->delayCurOperation() )
            {
               rc = SDB_LOCK_FAILED ;
            }
            goto error ;
         }
      }

      if ( CAT_INVALID_GROUPID == newGroupID )
      {
         newGroupID = _pCatCB->AllocGroupID() ;
         PD_CHECK( CAT_INVALID_GROUPID != newGroupID, SDB_SYS, error, PDERROR,
                   "Failed to assign group id, maybe group if full" ) ;
      }

      PD_TRACE1 ( SDB_CATNODEMGR_CREATEGRP, PD_PACK_UINT ( newGroupID ) ) ;

      try
      {
         BSONObjBuilder bobGroupInfo ;
         bobGroupInfo.append( CAT_GROUPNAME_NAME, groupName ) ;
         bobGroupInfo.append( CAT_GROUPID_NAME, newGroupID ) ;
         bobGroupInfo.append( CAT_ROLE_NAME, role ) ;
         bobGroupInfo.append( CAT_VERSION_NAME, CAT_VERSION_BEGIN ) ;
         bobGroupInfo.append( CAT_GROUP_STATUS, status ) ;
         BSONObjBuilder sub( bobGroupInfo.subarrayStart( CAT_GROUP_NAME ) ) ;
         sub.done() ;
         BSONObj boGroupInfo = bobGroupInfo.obj() ;

         rc = rtnInsert( CAT_NODE_INFO_COLLECTION, boGroupInfo, 1, 0,
                         _pEduCB, _pDmsCB, _pDpsCB, _majoritySize() ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert group info[%s] to "
                      "collection, rc: %d", boGroupInfo.toString().c_str(),
                      rc ) ;
         _pCatCB->insertGroupID( newGroupID, FALSE ) ;
      }
      catch( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR, "Occured exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATNODEMGR_CREATEGRP, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATNODEMGR_REMOVEGRP, "catNodeManager::removeGrp" )
   INT32 catNodeManager::removeGrp( const CHAR *groupName )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATNODEMGR_REMOVEGRP ) ;
      BSONObj groupInfo ;
      BSONObj matcher ;
      UINT32 groupID = 0 ;
      BOOLEAN isDeleted = FALSE ;

      if ( 0 != ossStrcmp( groupName, COORD_GROUPNAME ) &&
           0 != ossStrcmp( groupName, CATALOG_GROUPNAME ) )
      {
         rc = catGroupNameValidate( groupName, FALSE ) ;
         PD_RC_CHECK( rc, PDERROR, "Group name[%s] is invalid", groupName ) ;
      }

      rc = catGetGroupObj( groupName, FALSE, groupInfo, _pEduCB ) ;
      if ( SDB_CLS_GRP_NOT_EXIST == rc )
      {
         rc = SDB_CAT_GRP_NOT_EXIST;
         PD_LOG ( PDERROR, "the group(%s) is not exist",
                  groupName );
         goto error ;
      }
      else if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "failed to check if group exist(rc=%d)",
                  rc );
         goto error ;
      }

      try
      {
         BSONElement ele = groupInfo.getField( FIELD_NAME_GROUPID ) ;
         if ( ele.eoo() || !ele.isNumber() )
         {
            PD_LOG( PDERROR, "unexpected err with group info[%s]",
                    groupInfo.toString().c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         groupID = ele.Number() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( CATALOG_GROUPID == groupID )
      {
         INT64 count = 0 ;
         rc = catGroupCount( count, _pEduCB ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to count groups, rc: %d", rc ) ;

         if ( count > 1 )
         {
            rc = SDB_CATA_RM_CATA_FORBIDDEN ;
            PD_LOG( PDERROR, "Cant not remove catalog group when has other "
                    "group exist(%lld)", count ) ;
            goto error ;
         }
      }
      else
      {
         UINT64 count = 0 ;
         matcher = BSON( FIELD_NAME_CATALOGINFO".GroupID" << groupID ) ;

         rc = _count( CAT_COLLECTION_INFO_COLLECTION, matcher, count ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to count collection: %s, match: "
                      "%s, rc: %d", CAT_COLLECTION_INFO_COLLECTION,
                      matcher.toString().c_str(), rc ) ;

         if ( 0 != count )
         {
            PD_LOG( PDERROR, "can not remove a group with data in it" ) ;
            rc = SDB_CAT_RM_GRP_FORBIDDEN ;
            goto error ;
         }

         matcher = BSON( FIELD_NAME_TARGETID << groupID ) ;
         rc = _count( CAT_TASK_INFO_COLLECTION, matcher, count ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to count collection: %s, match: "
                      "%s, rc: %d", CAT_TASK_INFO_COLLECTION,
                      matcher.toString().c_str(), rc ) ;
         if ( 0 != count )
         {
            PD_LOG( PDERROR, "can not remove a group with task in it" ) ;
            rc = SDB_CAT_RM_GRP_FORBIDDEN ;
            goto error ;
         }

         pmdGetKRCB()->getCATLOGUECB()->removeGroupID( groupID ) ;
         isDeleted = TRUE ;

         rc = catDelGroupFromDomain( NULL, groupName, groupID, _pEduCB,
                                     _pDmsCB, _pDpsCB, 1 ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Remove group[%s] from domain failed, rc: %d",
                    groupName, rc ) ;
            goto error ;
         }

         matcher = BSON( FIELD_NAME_GROUPNAME << groupName ) ;
         rc = rtnDelete( CAT_NODE_INFO_COLLECTION, matcher,
                         BSONObj(), 0, _pEduCB, _pDmsCB, _pDpsCB,
                         _majoritySize() ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to delete record:%s", rc ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      if ( isDeleted )
      {
         pmdGetKRCB()->getCATLOGUECB()->insertGroupID ( groupID ) ;
      }
      goto done ;
   }

   INT32 catNodeManager::_count( const CHAR *collection,
                                 const BSONObj &matcher,
                                 UINT64 &count )
   {
      INT32 rc = SDB_OK ;
      INT64 totalCount;

      rc = rtnGetCount( collection, matcher, BSONObj(), _pDmsCB, _pEduCB,
                        _pRtnCB, &totalCount ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to rtn count:%d",rc ) ;
         goto error ;
      }
      SDB_ASSERT( totalCount >= 0, "totalCount must be greater than or equal 0") ;

      count = static_cast<UINT64>( totalCount ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catNodeManager::_delNode( BSONObj &boDelNodeInfo, BOOLEAN isLoalConn )
   {
      INT32 rc = SDB_OK ;

      const CHAR *groupName = NULL ;
      const CHAR *hostName  = NULL ;
      const CHAR *svcName   = NULL ;
      BOOLEAN forced        = FALSE ;

      BSONObj groupInfo ;
      BSONObj groupsObj ;

      BSONArrayBuilder newGroupsBuild ;
      INT32   removeNode = CAT_INVALID_NODEID ;

      rc = rtnGetStringElement( boDelNodeInfo, FIELD_NAME_GROUPNAME,
                                &groupName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_GROUPNAME, rc ) ;

      rc = rtnGetStringElement( boDelNodeInfo, FIELD_NAME_HOST, &hostName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_HOST, rc ) ;
      rc = rtnGetStringElement( boDelNodeInfo, PMD_OPTION_SVCNAME, &svcName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   PMD_OPTION_SVCNAME, rc ) ;
      rc = rtnGetBooleanElement( boDelNodeInfo, CMD_NAME_ENFORCED, forced ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
         forced = FALSE ;
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CMD_NAME_ENFORCED, rc ) ;

      if ( 0 == ossStrcmp( hostName, OSS_LOCALHOST ) ||
           0 == ossStrcmp( hostName, OSS_LOOPBACK_IP ) )
      {
         if ( !isLoalConn )
         {
            rc = SDB_CAT_NOT_LOCALCONN ;
            goto error ;
         }
      }

      if ( !forced &&
           0 == ossStrcmp( groupName, CATALOG_GROUPNAME ) )
      {
         rc = SDB_CATA_RM_CATA_FORBIDDEN ;
         PD_LOG( PDERROR, "Can not remove %s nodes", CATALOG_GROUPNAME ) ;
         goto error ;
      }

      rc = catGetGroupObj( groupName, FALSE, groupInfo, _pEduCB ) ;
      PD_RC_CHECK( rc, PDERROR, "Get group info by name[%s] failed, rc: %d",
                   groupName, rc ) ;

      {
         BSONElement beGroup = groupInfo.getField( FIELD_NAME_GROUP ) ;
         if ( beGroup.eoo() )
         {
            rc = SDB_CLS_NODE_NOT_EXIST ;
         }
         else if ( Array != beGroup.type() )
         {
            rc = SDB_SYS ;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                      FIELD_NAME_GROUP, rc ) ;
         groupsObj = beGroup.embeddedObject() ;
      }

      rc = _getRemovedGroupsObj( groupsObj, hostName, svcName, newGroupsBuild,
                                 &removeNode ) ;
      PD_RC_CHECK( rc, PDERROR, "Remove node[%s:%s] from group[%s] failed, "
                   "rc: %d", hostName, svcName, groupInfo.toString().c_str(),
                   rc ) ;

      if ( 1 == groupsObj.nFields() && !forced )
      {
         rc = SDB_CATA_RM_NODE_FORBIDDEN ;
         PD_LOG( PDERROR, "Can not remove node when group[%s] is only one node",
                 groupInfo.toString().c_str() ) ;
         goto error ;
      }

      {
         BSONObjBuilder updateBuilder ;
         BSONObj matcher, updator ;
         BSONObj dummyObj ;

         updateBuilder.append("$inc", BSON( FIELD_NAME_VERSION << 1 ) ) ;
         updateBuilder.append("$set", BSON( FIELD_NAME_GROUP <<
                               newGroupsBuild.arr() ) ) ;

         updator = updateBuilder.obj() ;
         matcher = BSON( FIELD_NAME_GROUPNAME << groupName ) ;

         rc = rtnUpdate( CAT_NODE_INFO_COLLECTION, matcher, updator, dummyObj,
                         0, _pEduCB, _pDmsCB, _pDpsCB, _majoritySize() ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to update group info[%s], matcher: "
                      "%s, updator: %s, rc: %d", groupInfo.toString().c_str(),
                      matcher.toString().c_str(), updator.toString().c_str(),
                      rc ) ;
      }

      _pCatCB->releaseNodeID( removeNode ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catNodeManager::_createNode( const CHAR *pQuery, BOOLEAN isLoalConn )
   {
      INT32 rc = SDB_OK ;
      UINT16 nodeID = CAT_INVALID_NODEID ;

      try
      {
         const CHAR *groupName = NULL ;
         const CHAR *hostName = NULL ;
         BSONObj boGroupInfo ;

         BSONObj boNodeInfo( pQuery ) ;
         rc = rtnGetStringElement( boNodeInfo, FIELD_NAME_GROUPNAME,
                                   &groupName ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get the field[%s], rc: %d",
                      FIELD_NAME_GROUPNAME, rc ) ;
         rc = rtnGetStringElement( boNodeInfo, FIELD_NAME_HOST,
                                   &hostName ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get the field[%s], rc: %d",
                      FIELD_NAME_HOST, rc ) ;

         rc = catGetGroupObj( groupName, FALSE, boGroupInfo, _pEduCB ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get group[%s] info, rc: %d",
                      groupName, rc ) ;

         {
         BSONElement beGroupId = boGroupInfo.getField( FIELD_NAME_GROUPID );
         PD_CHECK( beGroupId.type() == NumberInt, SDB_SYS, error, PDERROR,
                  "failed to get the field(%s)", FIELD_NAME_GROUPID );
         CoordGroupInfo groupInfo( beGroupId.numberInt() );
         rc = groupInfo.fromBSONObj( boGroupInfo );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to parse group info(rc=%d)",
                     rc );
         if ( groupInfo.getGroupID() != COORD_GROUPID )
         {
            PD_CHECK( groupInfo.getGroupSize() < CLS_REPLSET_MAX_NODE_SIZE,
                      SDB_DMS_REACHED_MAX_NODES, error, PDERROR,
                      "reached the maximum number of nodes!" ) ;
         }
         }

         {
         BOOLEAN isLocalHost = FALSE;
         if ( 0 == ossStrcmp( hostName, OSS_LOCALHOST ) ||
              0 == ossStrcmp( hostName, OSS_LOOPBACK_IP ) )
         {
            if ( !isLoalConn )
            {
               rc = SDB_CAT_NOT_LOCALCONN ;
               goto error ;
            }
            isLocalHost = TRUE ;
         }

         BOOLEAN isValid = FALSE;
         rc = _checkLocalHost( isValid ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "Failed to get localhost existing info, rc: %d", rc ) ;

         PD_CHECK( !(isLocalHost ^ isValid),
                   SDB_CAT_LOCALHOST_CONFLICT, error, PDERROR,
                   "'localhost' and '127.0.0.1' cannot be used mixed with "
                   "other hostname and IP address" );
         }

         if ( 0 == ossStrcmp( groupName, CATALOG_GROUPNAME ) ||
              0 == ossStrcmp( groupName, COORD_GROUPNAME ) )
         {
            nodeID = _pCatCB->AllocCataNodeID() ;
         }
         else
         {
            nodeID = _pCatCB->AllocNodeID();
         }

         PD_CHECK( CAT_INVALID_NODEID != nodeID, SDB_SYS, error, PDERROR,
                   "Failed to allocate node id, maybe node is full" ) ;

         rc = _addNodeToGrp( boGroupInfo, boNodeInfo, nodeID ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to add node to group, rc: %d", rc ) ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR, "Occured unexpected error: %s", e.what() );
         goto error ;
      }

   done:
      return rc ;
   error:
      if ( CAT_INVALID_NODEID != nodeID )
      {
         _pCatCB->releaseNodeID( nodeID ) ;
      }
      goto done ;
   }

   INT32 catNodeManager::_addNodeToGrp ( BSONObj &boGroupInfo,
                                         BSONObj &boNodeInfo,
                                         UINT16 nodeID )
   {
      INT32 rc = SDB_OK ;
      INT32  nodeRole = SDB_ROLE_DATA ;
      const  CHAR *groupName = NULL ;
      BSONObjBuilder newObjBuilder ;
      BSONObj newInfoObj ;
      BSONObjBuilder updateBuilder ;
      BSONObj updator, matcher ;
      BSONObj dummyObj ;

      rc = rtnGetIntElement( boGroupInfo, CAT_ROLE_NAME, nodeRole ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_ROLE_NAME, rc ) ;

      rc = _checkNodeInfo( boNodeInfo, nodeRole, &newObjBuilder ) ;
      PD_RC_CHECK( rc, PDERROR, "Check node info failed, rc: %d", rc ) ;
      newObjBuilder.append( FIELD_NAME_NODEID, nodeID ) ;
      newInfoObj = newObjBuilder.obj() ;

      rc = rtnGetStringElement( boGroupInfo, FIELD_NAME_GROUPNAME,
                                &groupName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_GROUPNAME, rc ) ;

      updateBuilder.append("$inc", BSON( FIELD_NAME_VERSION << 1 ) ) ;
      updateBuilder.append("$push", BSON( FIELD_NAME_GROUP << newInfoObj ) ) ;

      updator = updateBuilder.obj() ;
      matcher = BSON( FIELD_NAME_GROUPNAME << groupName ) ;

      rc = rtnUpdate( CAT_NODE_INFO_COLLECTION, matcher, updator, dummyObj,
                      0, _pEduCB, _pDmsCB, _pDpsCB, _majoritySize() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to update node info[%s] to group[%s],"
                   "matcher: %s, updator: %s, rc: %d",
                   newInfoObj.toString().c_str(), groupName,
                   matcher.toString().c_str(), updator.toString().c_str(),
                   rc ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catNodeManager::_updateNodeToGrp ( BSONObj &boGroupInfo,
                                            BSONObj &boNodeInfoNew,
                                            UINT16 nodeID,
                                            BOOLEAN isLoalConn )
   {
      INT32 rc = SDB_OK ;
      INT32 nodeRole = SDB_ROLE_DATA ;
      const CHAR *groupName = NULL ;
      const CHAR *hostName = NULL ;
      BSONObj oldInfoObj ;
      BSONObj newInfoObj ;
      BSONArrayBuilder newGroupsBuild ;

      BSONObjBuilder updateBuilder ;
      BSONObj updator, matcher ;
      BSONObj dummyObj ;

      rc = rtnGetIntElement( boGroupInfo, CAT_ROLE_NAME, nodeRole ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_ROLE_NAME, rc ) ;

      rc = rtnGetStringElement( boGroupInfo, FIELD_NAME_GROUPNAME, &groupName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_GROUPNAME, rc ) ;

      {
         BSONElement beGroups = boGroupInfo.getField( FIELD_NAME_GROUP ) ;
         if ( beGroups.eoo() || beGroups.type() != Array )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "Field[%s] type error, groupInfo: %s",
                    FIELD_NAME_GROUP, boGroupInfo.toString().c_str() ) ;
            goto error ;
         }
         rc = _getRemovedGroupsObj( beGroups.embeddedObject(), nodeID,
                                    oldInfoObj, newGroupsBuild ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove the node[%d] from group info "
                      "failed, rc: %d, groupInfo: %s", nodeID, rc,
                      boGroupInfo.toString().c_str() ) ;
      }

      rc = rtnGetStringElement( oldInfoObj, FIELD_NAME_HOST, &hostName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_HOST, rc ) ;

      if ( 0 == ossStrcmp( hostName, OSS_LOCALHOST ) ||
           0 == ossStrcmp( hostName, OSS_LOOPBACK_IP ) )
      {
         if ( !isLoalConn )
         {
            rc = SDB_CAT_NOT_LOCALCONN ;
            goto error ;
         }
      }

      {
         BSONObjBuilder mergeBuild ;
         mergeBuild.append( FIELD_NAME_HOST, hostName ) ;

         BSONObjIterator itr( oldInfoObj ) ;
         while ( itr.more() )
         {
            BSONElement e = itr.next() ;
            if ( 0 == ossStrcmp( e.fieldName(), FIELD_NAME_HOST ) ||
                 0 == ossStrcmp( e.fieldName(), FIELD_NAME_NODEID ) )
            {
               continue ;
            }
            BSONElement newEle = boNodeInfoNew.getField( e.fieldName() ) ;
            if ( !newEle.eoo() )
            {
               mergeBuild.append( newEle ) ;
            }
            else
            {
               mergeBuild.append( e ) ;
            }
         }
         mergeBuild.append( FIELD_NAME_NODEID, nodeID ) ;
         newInfoObj = mergeBuild.obj() ;
      }

      rc = _checkNodeInfo( newInfoObj, nodeRole, NULL ) ;
      PD_RC_CHECK( rc, PDERROR, "Check node info failed, rc: %d", rc ) ;

      newGroupsBuild.append( newInfoObj ) ;

      updateBuilder.append("$inc", BSON( FIELD_NAME_VERSION << 1 ) ) ;
      updateBuilder.append("$set", BSON( FIELD_NAME_GROUP <<
                            newGroupsBuild.arr() ) ) ;

      updator = updateBuilder.obj() ;
      matcher = BSON( FIELD_NAME_GROUPNAME << groupName ) ;

      rc = rtnUpdate( CAT_NODE_INFO_COLLECTION, matcher, updator, dummyObj,
                      0, _pEduCB, _pDmsCB, _pDpsCB, _majoritySize() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to update group info[%s], matcher: %s, "
                   "updator: %s, rc: %d", boGroupInfo.toString().c_str(),
                   matcher.toString().c_str(), updator.toString().c_str(),
                   rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catNodeManager::_getRemovedGroupsObj( const BSONObj & srcGroupsObj,
                                               UINT16 removeNode,
                                               BSONObj &removedObj,
                                               BSONArrayBuilder &newObjBuilder )
   {
      INT32 rc = SDB_OK ;
      INT32 nodeID = CAT_INVALID_NODEID ;
      BOOLEAN exist = FALSE ;

      BSONObjIterator i( srcGroupsObj ) ;
      while ( i.more() )
      {
         BSONElement beNode = i.next() ;
         BSONObj boNode = beNode.embeddedObject() ;

         rc = rtnGetIntElement( boNode, FIELD_NAME_NODEID, nodeID ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s] from [%s], rc: %d",
                      FIELD_NAME_NODEID, boNode.toString().c_str(), rc ) ;

         if ( removeNode != (UINT16)nodeID )
         {
            newObjBuilder.append( boNode ) ;
         }
         else
         {
            exist = TRUE ;
            removedObj = boNode.getOwned() ;
         }
      }

      PD_CHECK( exist, SDB_CLS_NODE_NOT_EXIST, error, PDERROR,
                "Remove node[%d] is not exist in group[%s]", removeNode,
                srcGroupsObj.toString().c_str() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catNodeManager::_getRemovedGroupsObj( const BSONObj & srcGroupsObj,
                                               const CHAR * hostName,
                                               const CHAR * serviceName,
                                               BSONArrayBuilder &newObjBuilder,
                                               INT32 *pRemoveNodeID )
   {
      INT32 rc = SDB_OK ;
      const CHAR *tmpHostName = NULL ;
      BOOLEAN exist = FALSE ;
      string strSvcName ;

      BSONObjIterator i( srcGroupsObj ) ;
      while ( i.more() )
      {
         BSONElement beNode = i.next() ;
         BSONObj boNode = beNode.embeddedObject() ;

         rc = rtnGetStringElement( boNode, FIELD_NAME_HOST, &tmpHostName ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s] from [%s], rc: %d",
                      FIELD_NAME_HOST, boNode.toString().c_str(), rc ) ;

         if ( 0 != ossStrcmp( hostName, tmpHostName ) )
         {
            newObjBuilder.append( boNode ) ;
         }
         else
         {
            strSvcName = "" ;
            BSONElement beService = boNode.getField( FIELD_NAME_SERVICE ) ;
            if ( beService.eoo() || Array != beService.type() )
            {
               rc = SDB_SYS ;
               PD_LOG( PDERROR, "Failed to get field[%s], field type: %d",
                       FIELD_NAME_SERVICE, beService.type() ) ;
               goto error ;
            }
            strSvcName = getServiceName( beService, MSG_ROUTE_LOCAL_SERVICE ) ;

            if ( 0 == ossStrcmp( serviceName, strSvcName.c_str() ) )
            {
               exist = TRUE ;
               if ( pRemoveNodeID )
               {
                  rc = rtnGetIntElement( boNode, FIELD_NAME_NODEID,
                                         *pRemoveNodeID ) ;
                  PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                               FIELD_NAME_NODEID, rc ) ;
               }
            }
            else
            {
               newObjBuilder.append( boNode ) ;
            }
         }
      }

      PD_CHECK( exist, SDB_CLS_NODE_NOT_EXIST, error, PDERROR,
                "Remove node[%s:%s] is not exist in group[%s]", hostName,
                serviceName, srcGroupsObj.toString().c_str() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   string catNodeManager::_getServiceName( UINT16 localPort,
                                           MSG_ROUTE_SERVICE_TYPE type )
   {
      CHAR szPort[ CAT_PORT_STR_SZ+1 ] = {0};
      UINT16 port = localPort + type ;
      ossItoa( port, szPort, CAT_PORT_STR_SZ ) ;
      return string( szPort ) ;
   }

   INT32 catNodeManager::_checkNodeInfo( BSONObj &boNodeInfo, INT32 nodeRole,
                                         BSONObjBuilder *newObjBuilder )
   {
      INT32 rc = SDB_OK ;

      const CHAR *hostName = NULL ;
      const CHAR *dbPath   = NULL ;
      const CHAR *localSvc = NULL ;
      const CHAR *replSvc  = NULL ;
      const CHAR *shardSvc = NULL ;
      const CHAR *cataSvc  = NULL ;
      BOOLEAN exist        = FALSE ;
      UINT16 svcPort       = 0 ;
      string strReplSvc ;
      string strShardSvc ;
      string strCataSvc ;

      rc = rtnGetStringElement( boNodeInfo, FIELD_NAME_HOST, &hostName ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get the field[%s], rc: %d",
                   FIELD_NAME_HOST, rc ) ;

      rc = rtnGetStringElement( boNodeInfo, PMD_OPTION_DBPATH, &dbPath ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get the field[%s], rc: %d",
                   PMD_OPTION_DBPATH, rc ) ;

      rc = rtnGetStringElement( boNodeInfo, PMD_OPTION_SVCNAME, &localSvc ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get the field[%s], rc: %d",
                   PMD_OPTION_SVCNAME, rc ) ;

      rc = catServiceCheck( hostName, localSvc, exist, _pEduCB ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to check local service[%s], "
                   "rc: %d", localSvc, rc ) ;
      PD_CHECK( !exist, SDB_CM_CONFIG_CONFLICTS, error, PDERROR,
                "Local service[%s] conflict", localSvc ) ;

      ossSocket::getPort( localSvc, svcPort ) ;
      PD_CHECK( 0 != svcPort, SDB_INVALIDARG, error, PDERROR,
                "Local service[%s] is invalid, translate to port 0",
                localSvc ) ;

      rc = rtnGetStringElement( boNodeInfo, PMD_OPTION_REPLNAME,
                                &replSvc ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         strReplSvc = _getServiceName( svcPort, MSG_ROUTE_REPL_SERVICE ) ;
         replSvc = strReplSvc.c_str() ;
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get the field[%s], rc: %d",
                   PMD_OPTION_REPLNAME, rc ) ;

      rc = catServiceCheck( hostName, replSvc, exist, _pEduCB ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to check repl service[%s], "
                   "rc: %d", replSvc, rc ) ;
      PD_CHECK( !exist, SDB_CM_CONFIG_CONFLICTS, error, PDERROR,
                "Repl service[%s] conflict", replSvc ) ;

      rc = rtnGetStringElement( boNodeInfo, PMD_OPTION_SHARDNAME,
                                &shardSvc ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         strShardSvc = _getServiceName( svcPort, MSG_ROUTE_SHARD_SERVCIE ) ;
         shardSvc = strShardSvc.c_str() ;
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get the field[%s], rc: %d",
                   PMD_OPTION_SHARDNAME, rc ) ;

      rc = catServiceCheck( hostName, shardSvc, exist, _pEduCB ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to check shard service[%s], "
                   "rc: %d", shardSvc, rc ) ;
      PD_CHECK( !exist, SDB_CM_CONFIG_CONFLICTS, error, PDERROR,
                "Shard service[%s] conflict", shardSvc ) ;

      if ( SDB_ROLE_CATALOG == nodeRole )
      {
         rc = rtnGetStringElement( boNodeInfo, PMD_OPTION_CATANAME,
                                   &cataSvc ) ;
         if ( SDB_FIELD_NOT_EXIST == rc )
         {
            strCataSvc = _getServiceName( svcPort, MSG_ROUTE_CAT_SERVICE ) ;
            cataSvc = strCataSvc.c_str() ;
            rc = SDB_OK ;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to get the field[%s], rc: %d",
                      PMD_OPTION_CATANAME, rc ) ;

         rc = catServiceCheck( hostName, cataSvc, exist, _pEduCB ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to check cata service[%s], "
                      "rc: %d", cataSvc, rc ) ;
         PD_CHECK( !exist, SDB_CM_CONFIG_CONFLICTS, error, PDERROR,
                   "Cata service[%s] conflict", cataSvc ) ;
      }

      if ( newObjBuilder )
      {
         newObjBuilder->append( FIELD_NAME_HOST, hostName ) ;
         newObjBuilder->append( PMD_OPTION_DBPATH, dbPath ) ;
         BSONObjBuilder sub( newObjBuilder->subarrayStart(
                             FIELD_NAME_SERVICE ) ) ;
         BSONObjBuilder sub1( sub.subobjStart("0") ) ;
         sub1.append( FIELD_NAME_SERVICE_TYPE, MSG_ROUTE_LOCAL_SERVICE ) ;
         sub1.append( FIELD_NAME_NAME, localSvc ) ;
         sub1.done() ;

         BSONObjBuilder sub2( sub.subobjStart("1") ) ;
         sub2.append( FIELD_NAME_SERVICE_TYPE, MSG_ROUTE_REPL_SERVICE ) ;
         sub2.append( FIELD_NAME_NAME, replSvc ) ;
         sub2.done() ;
         BSONObjBuilder sub3( sub.subobjStart("2") ) ;
         sub3.append( FIELD_NAME_SERVICE_TYPE, MSG_ROUTE_SHARD_SERVCIE ) ;
         sub3.append( FIELD_NAME_NAME, shardSvc ) ;
         sub3.done() ;
         if ( SDB_ROLE_CATALOG == nodeRole )
         {
            BSONObjBuilder sub4( sub.subobjStart("3") ) ;
            sub4.append( FIELD_NAME_SERVICE_TYPE, MSG_ROUTE_CAT_SERVICE ) ;
            sub4.append( FIELD_NAME_NAME, cataSvc ) ;
            sub4.done() ;
         }

         sub.done() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catNodeManager::activeGrp( const std::string &strGroupName,
                                    UINT32 groupID,
                                    BSONObj &boGroupInfo )
   {
      INT32 rc = SDB_OK;
      do
      {
         try
         {
            if ( 0 != groupID )
            {
               rc = catGetGroupObj( groupID, boGroupInfo, _pEduCB ) ;
            }
            else
            {
               rc = catGetGroupObj( strGroupName.c_str(),
                                    FALSE, boGroupInfo, _pEduCB ) ;
            }
            if ( rc )
            {
               PD_LOG( PDERROR, "Failed to get group(%s/%d) info, rc: %d",
                       strGroupName.c_str(), groupID, rc ) ;
               break ;
            }

            BSONElement beGroup = boGroupInfo.getField( CAT_GROUP_NAME );
            if ( beGroup.eoo() || beGroup.type()!=Array )
            {
               rc = SDB_CLS_EMPTY_GROUP ;
               PD_LOG( PDERROR, "Active group failed, can't active "
                       "empty-group" );
               break ;
            }
            BSONObj boGroup = beGroup.embeddedObject() ;
            if ( boGroup.isEmpty() )
            {
               rc = SDB_CLS_EMPTY_GROUP ;
               PD_LOG( PDERROR, "Active group failed, can't active "
                       "empty-group" );
               break;
            }
            BSONElement beGroupID = boGroupInfo.getField(CAT_GROUPID_NAME);
            if ( beGroupID.eoo() || !beGroupID.isNumber() )
            {
               rc = SDB_INVALIDARG;
               PD_LOG ( PDERROR, "Failed to get the field(%s)",
                        CAT_GROUPID_NAME );
               break;
            }
            UINT32 groupID = beGroupID.number() ;

            if ( CATALOG_GROUPID == groupID ||
                 SDB_CAT_GRP_ACTIVE ==
                 boGroupInfo.getField( CAT_GROUP_STATUS ).numberInt() )
            {
               break ;
            }

            BSONObjBuilder bobStatus ;
            bobStatus.append( CAT_GROUP_STATUS, SDB_CAT_GRP_ACTIVE );
            BSONObj boStatus = bobStatus.obj() ;
            BSONObjBuilder bobUpdater;
            bobUpdater.append("$set",  boStatus );
            BSONObj boUpdater = bobUpdater.obj();
            BSONObj boMatcher = BSON( CAT_GROUPNAME_NAME <<
                                      strGroupName.c_str() );
            BSONObj boHint;
            rc = rtnUpdate( CAT_NODE_INFO_COLLECTION, boMatcher,
                            boUpdater, boHint,
                            FLG_UPDATE_UPSERT, _pEduCB, _pDmsCB,
                            _pDpsCB, _majoritySize() );
            if ( rc != SDB_OK )
            {
               PD_LOG ( PDERROR, "Failed to active, failed to update "
                        "catalogue-group info(rc=%d)", rc ) ;
               break;
            }
            _pCatCB->activeGroup( groupID ) ;
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "occured unexpected error:%s", e.what() ) ;
         }
      }while ( FALSE ) ;
      return rc ;
   }

   INT16 catNodeManager::_majoritySize()
   {
      return _pCatCB->majoritySize() ;
   }

   INT32 catNodeManager::_checkLocalHost( BOOLEAN &isValid )
   {
      BSONObj matcher;
      UINT64 count = 0;
      INT32 rc = SDB_OK;

      rc = _count( CAT_NODE_INFO_COLLECTION, matcher, count) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      if ( 0 == count )
      {
         isValid = TRUE;
         goto done;
      }

      matcher =
         BSON( CAT_GROUP_NAME"."CAT_HOST_FIELD_NAME <<
            BSON ( "$in" <<
               BSON_ARRAY( OSS_LOCALHOST << OSS_LOOPBACK_IP ) ) ) ;
      rc = _count( CAT_NODE_INFO_COLLECTION, matcher, count) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      isValid = ( 0 == count) ? FALSE : TRUE;

      done:
         return rc ;
      error:
         goto done ;
   }
}
