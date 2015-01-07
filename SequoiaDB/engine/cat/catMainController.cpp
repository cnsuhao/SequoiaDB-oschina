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

   Source File Name = catMainController.cpp

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
#include "core.hpp"
#include "catMainController.hpp"
#include "catalogueCB.hpp"
#include "pmdCB.hpp"
#include "pd.hpp"
#include "catDef.hpp"
#include "rtn.hpp"
#include "dpsLogWrapper.hpp"
#include "rtnCoord.hpp"
#include "msgMessage.hpp"
#include "msgAuth.hpp"
#include "../util/fromjson.hpp"
#include "pmdDef.hpp"
#include "pdTrace.hpp"
#include "catTrace.hpp"
#include "catCommon.hpp"

using namespace bson;
namespace engine
{
   BEGIN_OBJ_MSG_MAP( catMainController, _pmdObjBase )
      ON_EVENT( PMD_EDU_EVENT_ACTIVE, _onActiveEvent )
      ON_EVENT( PMD_EDU_EVENT_DEACTIVE, _onDeactiveEvent )
   END_OBJ_MSG_MAP()

   /*
      catMainController implement
   */
   catMainController::catMainController ()
   {
      _nodeManagerEDUID    = PMD_INVALID_EDUID ;
      _catalogManagerEDUID = PMD_INVALID_EDUID ;
      _pEduMgr             = NULL ;
      _pCatCB              = NULL ;
      _pDmsCB              = NULL ;
      _pRtnCB              = NULL ;
      _pAuthCB             = NULL ;
      _pNodeMgrCB          = NULL ;
      _pCataMgrCB          = NULL ;
      _pEDUCB              = NULL ;

      _isActived           = FALSE ;
      _changeEvent.signal() ;
   }

   catMainController::~catMainController()
   {
   }

   void catMainController::attachCB( pmdEDUCB * cb )
   {
      if ( EDU_TYPE_CATMAINCONTROLLER == cb->getType() )
      {
         _pEDUCB = cb ;
      }
      else if ( EDU_TYPE_CATCATALOGUEMANAGER == cb->getType() )
      {
         _pCataMgrCB = cb ;
         _catalogManagerEDUID = cb->getID() ;
      }
      else if ( EDU_TYPE_CATNODEMANAGER == cb->getType() )
      {
         _pNodeMgrCB = cb ;
         _nodeManagerEDUID = cb->getID() ;
      }
      _attachEvent.signalAll() ;
   }

   void catMainController::detachCB( pmdEDUCB * cb )
   {
      if ( EDU_TYPE_CATMAINCONTROLLER == cb->getType() )
      {
         _pEDUCB = NULL ;
         _changeEvent.signal() ;
      }
      else if ( EDU_TYPE_CATCATALOGUEMANAGER == cb->getType() )
      {
         _pCataMgrCB = NULL ;
      }
      else if ( EDU_TYPE_CATNODEMANAGER == cb->getType() )
      {
         _pNodeMgrCB = NULL ;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_HANDLEMSG, "catMainController::handleMsg" )
   INT32 catMainController::handleMsg( const NET_HANDLE &handle,
                                       const _MsgHeader *header,
                                       const CHAR *msg )
   {
      SDB_ASSERT ( _pEduMgr && _pCatCB && _pDmsCB,
                   "all of the members must be initialized before init "
                   "netfram" ) ;
      SDB_ASSERT ( NULL != header, "message-header should not be NULL" ) ;
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATMAINCT_HANDLEMSG ) ;
      PD_TRACE1 ( SDB_CATMAINCT_HANDLEMSG,
                  PD_PACK_INT ( header->opCode ) ) ;

      rc = _postMsg( handle, header ) ;

      PD_TRACE_EXITRC ( SDB_CATMAINCT_HANDLEMSG, rc ) ;
      return rc ;
   }

   void catMainController::handleClose( const NET_HANDLE & handle,
                                        _MsgRouteID id )
   {
      _delContextByHandle( handle );
   }

   INT32 catMainController::_processInterruptMsg( const NET_HANDLE & handle,
                                                  MsgHeader * header )
   {
      PD_LOG( PDEVENT, "Recieve interrupt msg[handle: %u, tid: %u]",
              handle, header->TID ) ;
      // release the ' handle + tid ' all context
      _delContext( handle, header->TID ) ;
      // not reply
      return SDB_OK ;
   }

   INT32 catMainController::_processDisconnectMsg( const NET_HANDLE & handle,
                                                   MsgHeader * header )
   {
      PD_LOG( PDEVENT, "Recieve disconnect msg[handle: %u, tid: %u]",
              handle, header->TID ) ;
      // release the ' handle + tid ' all context
      _delContext( handle, header->TID ) ;
      // not reply
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_POSTMSG, "catMainController::_postMsg" )
   INT32 catMainController::_postMsg( const NET_HANDLE &handle,
                                      const MsgHeader *header )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATMAINCT_POSTMSG ) ;
      PD_TRACE1 ( SDB_CATMAINCT_POSTMSG,
                  PD_PACK_INT ( header->opCode ) ) ;

      pmdEDUEvent event ;

      if ( MSG_CAT_CATALOGUE_BEGIN < (UINT32)header->opCode &&
           (UINT32)header->opCode < MSG_CAT_CATALOGUE_END )
      {
         if ( NULL == _pCataMgrCB )
         {
            rc = SDB_SYS ;
            goto error ;
         }
         rc = _catBuildMsgEvent ( handle, header, event ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build the event, rc: %d", rc );
         _pCataMgrCB->postEvent( event ) ;
      }
      else if  ( MSG_CAT_NODE_BEGIN < (UINT32)header->opCode &&
                 (UINT32)header->opCode < MSG_CAT_NODE_END )
      {
         if ( NULL == _pNodeMgrCB )
         {
            rc = SDB_SYS ;
            goto error ;
         }
         rc = _catBuildMsgEvent ( handle, header, event ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build the event, rc: %d", rc );
         _pNodeMgrCB->postEvent( event ) ;
      }
      else
      {
         if ( NULL == _pEDUCB )
         {
            rc = SDB_SYS ;
            goto error ;
         }
         rc = _catBuildMsgEvent( handle, header, event ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to build the event, rc: %d", rc ) ;
         _pEDUCB->postEvent( event ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATMAINCT_POSTMSG, rc ) ;
      return rc;
   error:
      PD_LOG ( PDERROR, "Failed to process message(MessageType = %d, rc=%d)",
               header->opCode, rc ) ;
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_INIT, "catMainController::init" )
   INT32 catMainController::init()
   {
      INT32 rc             = SDB_OK ;
      pmdKRCB *pKrcb       = pmdGetKRCB() ;
      _pEduMgr             = pKrcb->getEDUMgr () ;
      _pDmsCB              = pKrcb->getDMSCB() ;
      _pRtnCB              = pKrcb->getRTNCB() ;
      _pAuthCB             = pKrcb->getAuthCB() ;
      _pCatCB              = pKrcb->getCATLOGUECB() ;

      PD_TRACE_ENTRY ( SDB_CATMAINCT_INIT ) ;

      // after initializing, let's attempt to create collectionspace and
      // collections
      rc = _ensureMetadata () ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to create metadata "
                    "collections/indexes, rc = %d", rc ) ;

   done :
      PD_TRACE_EXITRC ( SDB_CATMAINCT_INIT, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT__CREATESYSIDX, "catMainController::_createSysIndex" )
   INT32 catMainController::_createSysIndex ( const CHAR *pCollection,
                                              const CHAR *pIndex,
                                              pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj indexDef ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT__CREATESYSIDX ) ;
      PD_TRACE2 ( SDB_CATMAINCT__CREATESYSIDX,
                  PD_PACK_STRING ( pCollection ),
                  PD_PACK_STRING ( pIndex ) ) ;

      rc = fromjson ( pIndex, indexDef ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to build index object, rc = %d",
                    rc ) ;

      rc = catTestAndCreateIndex( pCollection, indexDef, cb, _pDmsCB,
                                  NULL, TRUE ) ;
      if ( rc )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATMAINCT__CREATESYSIDX, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT__CREATESYSCOL, "catMainController::_createSysCollection" )
   INT32 catMainController::_createSysCollection ( const CHAR *pCollection,
                                                   pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT__CREATESYSCOL ) ;
      PD_TRACE1 ( SDB_CATMAINCT__CREATESYSCOL,
                  PD_PACK_STRING ( pCollection ) ) ;

      rc = catTestAndCreateCL( pCollection, cb, _pDmsCB, NULL, TRUE ) ;
      if ( rc )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATMAINCT__CREATESYSCOL, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // NO NEED TO SYNC LOG FOR ANY CREATION
   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT__ENSUREMETADATA, "catMainController::_ensureMetadata" )
   INT32 catMainController::_ensureMetadata()
   {
      INT32 rc = SDB_OK ;
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT__ENSUREMETADATA ) ;

      // create SYSCAT.SYSNODES
      rc = _createSysCollection( CAT_NODE_INFO_COLLECTION, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createSysIndex ( CAT_NODE_INFO_COLLECTION,
                             CAT_NODEINFO_GROUPNAMEIDX, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createSysIndex ( CAT_NODE_INFO_COLLECTION,
                             CAT_NODEINFO_GROUPIDIDX, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      // create SYSCAT.SYSCOLLECTIONSPACES
      rc = _createSysCollection ( CAT_COLLECTION_SPACE_COLLECTION, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createSysIndex ( CAT_COLLECTION_SPACE_COLLECTION,
                             CAT_COLLECTION_SPACE_NAMEIDX, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      // create SYSCAT.SYSCOLLECTIONS
      rc = _createSysCollection ( CAT_COLLECTION_INFO_COLLECTION, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createSysIndex ( CAT_COLLECTION_INFO_COLLECTION,
                             CAT_COLLECTION_NAMEIDX, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      // create SYSCAT.SYSTASKS
      rc = _createSysCollection ( CAT_TASK_INFO_COLLECTION, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createSysIndex ( CAT_TASK_INFO_COLLECTION,
                             CAT_TASK_INFO_CLOBJIDX, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      // create SYSCAT.SYSDOMAINS
      rc = _createSysCollection ( CAT_DOMAIN_COLLECTION, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createSysIndex ( CAT_DOMAIN_COLLECTION,
                             CAT_DOMAIN_NAMEIDX, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      // create SYSCAT.SYSHISTORY
      rc = _createSysCollection( CAT_HISTORY_COLLECTION, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createSysIndex( CAT_HISTORY_COLLECTION, CAT_HISTORY_BUCKETID_IDX,
                            cb ) ;
      if ( rc )
      {
         goto error ;
      }

      /// procedures
      rc = _createSysCollection ( CAT_PROCEDURES_COLLECTION, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createSysIndex ( CAT_PROCEDURES_COLLECTION,
                             CAT_PROCEDURES_COLLECTION_INDEX, cb ) ;
      if ( rc )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATMAINCT__ENSUREMETADATA, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // when we activate the main controller, we should always assume there's
   // metadata collections exist
   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_ACTIVE, "catMainController::_onActiveEvent" )
   INT32 catMainController::_onActiveEvent( pmdEDUEvent *event )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT_ACTIVE ) ;

      rc = _pAuthCB->checkNeedAuth( _pEDUCB, TRUE ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to check need auth, rc: %d", rc ) ;
         goto error ;
      }
      _isActived = TRUE ;

      _pCatCB->getCatNodeMgr()->getChangeEvent()->reset() ;
      rc = _pEduMgr->postEDUPost( _nodeManagerEDUID, PMD_EDU_EVENT_ACTIVE ) ;
      if ( rc )
      {
         _pCatCB->getCatNodeMgr()->getChangeEvent()->signal() ;
         PD_LOG( PDERROR, "Failed to post active event to node manager, "
                 "rc: %d", rc ) ;
         goto error ;
      }
      _pCatCB->getCatlogueMgr()->getChangeEvent()->reset() ;
      rc = _pEduMgr->postEDUPost( _catalogManagerEDUID, PMD_EDU_EVENT_ACTIVE ) ;
      if ( rc )
      {
         _pCatCB->getCatlogueMgr()->getChangeEvent()->signal() ;
         PD_LOG( PDERROR, "Failed to post active event to catalog manager, "
                 "rc: %d", rc ) ;
         goto error ;
      }

      _pCatCB->getCatlogueMgr()->getChangeEvent()->wait( OSS_ONE_SEC * 120 ) ;
      _pCatCB->getCatNodeMgr()->getChangeEvent()->wait( OSS_ONE_SEC * 120 ) ;

   done:
      _changeEvent.signal() ;
      PD_TRACE_EXITRC ( SDB_CATMAINCT_ACTIVE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_DEACTIVE, "catMainController::_onDeactiveEvent" )
   INT32 catMainController::_onDeactiveEvent( pmdEDUEvent *event )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT_DEACTIVE ) ;

      _pCatCB->getCatNodeMgr()->getChangeEvent()->reset() ;
      rc = _pEduMgr->postEDUPost( _nodeManagerEDUID, PMD_EDU_EVENT_DEACTIVE ) ;
      if ( rc )
      {
         _pCatCB->getCatNodeMgr()->getChangeEvent()->signal() ;
         PD_LOG( PDERROR, "Post deactive event to node manager failed, "
                 "rc: %d", rc ) ;
         // run continue
      }
      _pCatCB->getCatlogueMgr()->getChangeEvent()->reset() ;
      rc = _pEduMgr->postEDUPost( _catalogManagerEDUID,
                                  PMD_EDU_EVENT_DEACTIVE ) ;
      if ( rc )
      {
         _pCatCB->getCatlogueMgr()->getChangeEvent()->signal() ;
         PD_LOG( PDERROR, "Post deactive event to catalog manager failed, "
                 "rc: %d", rc ) ;
         // run continue
      }

      _pCatCB->getCatlogueMgr()->getChangeEvent()->wait( OSS_ONE_SEC * 120 ) ;
      _pCatCB->getCatNodeMgr()->getChangeEvent()->wait( OSS_ONE_SEC * 120 ) ;

      _isActived = FALSE ;
      _changeEvent.signal() ;

      PD_TRACE_EXITRC ( SDB_CATMAINCT_DEACTIVE, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_BUILDMSGEVENT, "catMainController::_catBuildMsgEvent" )
   INT32 catMainController::_catBuildMsgEvent ( const NET_HANDLE &handle,
                                                const MsgHeader *pMsg,
                                                pmdEDUEvent &event )
   {
      INT32 rc = SDB_OK ;
      CHAR *pEventData = NULL ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT_BUILDMSGEVENT ) ;

      pEventData = (CHAR *)SDB_OSS_MALLOC( pMsg->messageLength ) ;
      if ( NULL == pEventData )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "malloc failed(size = %d)", pMsg->messageLength ) ;
         goto error ;
      }
      ossMemcpy( (void *)pEventData, pMsg, pMsg->messageLength ) ;

      event._Data = pEventData ;
      event._dataMemType = PMD_EDU_MEM_ALLOC ;
      event._eventType = PMD_EDU_EVENT_MSG ;
      event._userData = (UINT64)handle ;

   done :
      PD_TRACE_EXITRC ( SDB_CATMAINCT_BUILDMSGEVENT, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_GETMOREMSG, "catMainController::_processGetMoreMsg" )
   INT32 catMainController::_processGetMoreMsg ( const NET_HANDLE &handle,
                                                 MsgHeader *pMsg )
   {
      INT32 rc               = SDB_OK ;
      MsgOpGetMore *pGetMore = (MsgOpGetMore*)pMsg ;

      rtnContextBuf buffObj ;
      SINT32 msgLen          = 0 ;
      MsgOpReply *pReply     = NULL ;

      PD_TRACE_ENTRY ( SDB_CATMAINCT_GETMOREMSG ) ;
      // send the reply whether successful or not
      rc = rtnGetMore( pGetMore->contextID, pGetMore->numToReturn,
                       buffObj, _pEDUCB, _pRtnCB ) ;
      if ( rc )
      {
         _delContextByID( pGetMore->contextID, FALSE );
      }
      msgLen =  sizeof(MsgOpReply) + buffObj.size() ;
      // free by end of function
      pReply = (MsgOpReply *)SDB_OSS_MALLOC( msgLen );
      if ( NULL == pReply )
      {
         PD_LOG ( PDERROR, "Malloc error ( size = %d )", msgLen ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      pReply->header.messageLength = msgLen ;
      pReply->header.opCode        = MSG_BS_GETMORE_RES ;
      pReply->header.TID           = pGetMore->header.TID ;
      pReply->header.routeID.value = 0 ;
      pReply->header.requestID     = pGetMore->header.requestID ;
      pReply->contextID            = pGetMore->contextID ;
      pReply->startFrom            = (INT32)buffObj.getStartFrom() ;
      pReply->numReturned          = buffObj.recordNum() ;
      pReply->flags                = rc ;
      PD_TRACE1 ( SDB_CATMAINCT_GETMOREMSG,
                  PD_PACK_INT ( rc ) ) ;
      if ( SDB_OK != rc && SDB_DMS_EOC != rc )
      {
         PD_LOG ( PDERROR, "Failed to get more, rc = %d", rc ) ;
      }
      ossMemcpy( (CHAR *)pReply + sizeof(MsgOpReply), buffObj.data(),
                 buffObj.size() ) ;
      rc = _pCatCB->netWork()->syncSend(handle, pReply);
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to syncSend, rc = %d", rc ) ;
         goto error ;
      }
   done :
      if ( pReply )
      {
         SDB_OSS_FREE( pReply ) ;
      }
      PD_TRACE_EXITRC ( SDB_CATMAINCT_GETMOREMSG, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_KILLCONTEXT, "catMainController::_processKillContext" )
   INT32 catMainController::_processKillContext( const NET_HANDLE &handle,
                                                 MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      INT32 contextNum = 0 ;
      INT64 *pContextIDs = NULL ;
      MsgOpReply msgReply;
      MsgOpKillContexts *pReq = (MsgOpKillContexts *)pMsg;
      msgReply.contextID = -1;
      msgReply.flags = SDB_OK;
      msgReply.numReturned = 0;
      msgReply.startFrom = 0;
      msgReply.header.messageLength = sizeof(MsgOpReply);
      msgReply.header.opCode = MSG_BS_KILL_CONTEXT_RES;
      msgReply.header.requestID = pReq->header.requestID;
      msgReply.header.routeID.value = 0;
      msgReply.header.TID = pReq->header.TID;

      PD_TRACE_ENTRY ( SDB_CATMAINCT_KILLCONTEXT ) ;
      do
      {
         rc = msgExtractKillContexts ( (CHAR *)pMsg,
                                       &contextNum, &pContextIDs ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR, "Failed to parse the killcontexts request, "
                     "rc: %d", rc ) ;
            break;
         }

         if ( contextNum > 0 )
         {
            PD_LOG ( PDDEBUG, "KillContext: contextNum:%d\ncontextID: %lld",
                     contextNum, pContextIDs[0] ) ;
         }

         for ( INT32 i = 0 ; i < contextNum ; ++i )
         {
            _delContextByID( pContextIDs[ i ], TRUE ) ;
         }
      }while ( FALSE ) ;
      msgReply.flags = rc;
      PD_TRACE1 ( SDB_CATMAINCT_KILLCONTEXT,
                  PD_PACK_INT ( rc ) ) ;
      rc = _pCatCB->netWork()->syncSend( handle, &msgReply );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to send the message "
                  "( groupID=%d, nodeID=%d, serviceID=%d )",
                  pReq->header.routeID.columns.groupID,
                  pReq->header.routeID.columns.nodeID,
                  pReq->header.routeID.columns.serviceID );
      }
      PD_TRACE_EXITRC ( SDB_CATMAINCT_KILLCONTEXT, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_QUERYMSG, "catMainController::_processQueryMsg" )
   INT32 catMainController::_processQueryMsg( const NET_HANDLE &handle,
                                              MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK;
      MsgOpReply msgReply;
      MsgOpQuery *pReq = (MsgOpQuery *)pMsg;
      msgReply.contextID = -1;
      msgReply.flags = SDB_OK;
      msgReply.numReturned = 0;
      msgReply.startFrom = 0;
      msgReply.header.messageLength = sizeof(MsgOpReply);
      msgReply.header.opCode = MSG_BS_QUERY_RES;
      msgReply.header.requestID = pReq->header.requestID;
      msgReply.header.routeID.value = 0;
      msgReply.header.TID = pReq->header.TID;

      PD_TRACE_ENTRY ( SDB_CATMAINCT_QUERYMSG ) ;
      do
      {
         SINT64 contextID      = 0 ;
         INT32 flags           = 0 ;
         SINT64 numToSkip      = -1 ;
         SINT64 numToReturn    = -1 ;
         CHAR *pCollectionName = NULL ;
         CHAR *pQuery          = NULL ;
         CHAR *pFieldSelector  = NULL ;
         CHAR *pOrderBy        = NULL ;
         CHAR *pHint           = NULL ;
         rc = msgExtractQuery( (CHAR *)pMsg, &flags, &pCollectionName,
                               &numToSkip, &numToReturn, &pQuery,
                               &pFieldSelector, &pOrderBy, &pHint );
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR,
                     "failed to parse query request(rc=%d)",
                     rc );
            break;
         }
         BSONObj selector;
         BSONObj matcher;
         BSONObj orderBy;
         BSONObj hint;
         try
         {
            selector = BSONObj ( pFieldSelector );
            matcher = BSONObj ( pQuery );
            orderBy = BSONObj ( pOrderBy );
            hint = BSONObj ( pHint );
         }
         catch ( std::exception &e )
         {
            rc = SDB_INVALIDARG;
            PD_LOG ( PDERROR,
                     "occured unexpected error:%s",
                     e.what() );
            break;
         }
         rc = rtnQuery( pCollectionName, selector, matcher, orderBy,
                        hint, flags, _pEDUCB, numToSkip, numToReturn,
                        _pDmsCB, _pRtnCB, contextID ) ;
         if ( rc != SDB_OK )
         {
            if ( rc != SDB_DMS_EOC )
            {
               PD_LOG ( PDERROR, "Failed to query the collection:%s(rc=%d)",
                        pCollectionName, rc );
            }
            break;
         }
         _addContext( handle, pReq->header.TID, contextID ) ;
         msgReply.contextID = contextID ;
      }while ( FALSE ) ;

      msgReply.flags = rc;
      PD_TRACE1 ( SDB_CATMAINCT_QUERYMSG,
                  PD_PACK_INT ( rc ) ) ;
      rc = _pCatCB->netWork()->syncSend ( handle, &msgReply );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to send the message "
                  "( groupID=%d, nodeID=%d, serviceID=%d )",
                  pReq->header.routeID.columns.groupID,
                  pReq->header.routeID.columns.nodeID,
                  pReq->header.routeID.columns.serviceID );
      }
      PD_TRACE_EXITRC ( SDB_CATMAINCT_QUERYMSG, rc ) ;
      return rc ;
   }

   INT32 catMainController::_processQueryCollections ( const NET_HANDLE &handle,
                                                       MsgHeader *pMsg )
   {
      return _processQueryRequest ( handle, pMsg,
                                    CAT_COLLECTION_INFO_COLLECTION ) ;
   }

   INT32 catMainController::_processQueryCollectionSpaces( const NET_HANDLE &handle,
                                                           MsgHeader *pMsg )
   {
      return _processQueryRequest ( handle, pMsg,
                                    CAT_COLLECTION_SPACE_COLLECTION ) ;
   }

   INT32 catMainController::_processQueryDataGrp( const NET_HANDLE &handle,
                                                  MsgHeader *pMsg )
   {
      return _processQueryRequest ( handle, pMsg, CAT_NODE_INFO_COLLECTION ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_QUERYREQUEST, "catMainController::_processQueryRequest" )
   INT32 catMainController::_processQueryRequest ( const NET_HANDLE &handle,
                                                   MsgHeader *pMsg,
                                                   const CHAR *pCollectionName )
   {
      INT32 rc              = SDB_OK ;
      MsgOpReply msgReply ;
      MsgHeader *pMsgHeader = (MsgHeader *)( pMsg ) ;
      SINT64 contextID      = 0 ;
      SINT32 flags          = 0 ;
      SINT64 numToSkip      = -1 ;
      SINT64 numToReturn    = -1 ;
      CHAR *pCN             = NULL ;
      CHAR *pQuery          = NULL ;
      CHAR *pFieldSelector  = NULL ;
      CHAR *pOrderByBuffer  = NULL ;
      CHAR *pHintBuffer     = NULL ;
      INT32 iNameLen        = 0 ;

      PD_TRACE_ENTRY ( SDB_CATMAINCT_QUERYREQUEST ) ;

      PD_CHECK( pmdIsPrimary(), SDB_CLS_NOT_PRIMARY, reply, PDWARNING,
                "it is not primary node but received query request!" );

      rc = msgExtractQuery ( (CHAR *)pMsgHeader, &flags, &pCN,
                             &numToSkip, &numToReturn, &pQuery,
                             &pFieldSelector, &pOrderByBuffer,
                             &pHintBuffer ) ;
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to read query packet, rc = %d", rc ) ;
         rc = SDB_INVALIDARG ;
         goto reply ;
      }
      iNameLen = ossStrlen(pCN) ;
      if ( iNameLen <= 0 || pCN[0]!='$')
      {
         PD_LOG ( PDERROR, "Invalid command-begin" ) ;
         rc = SDB_INVALIDARG ;
         goto reply ;
      }
      try
      {
         BSONObj matcher ( pQuery ) ;
         BSONObj selector ( pFieldSelector ) ;
         BSONObj orderBy ( pOrderByBuffer ) ;
         BSONObj hint ( pHintBuffer ) ;
         rc = rtnQuery( pCollectionName, selector, matcher, orderBy,
                        hint, flags, _pEDUCB, numToSkip, numToReturn,
                        _pDmsCB, _pRtnCB, contextID ) ;
         if ( rc != SDB_OK )
         {
            if ( rc != SDB_DMS_EOC )
            {
               PD_LOG ( PDERROR, "Failed to list data-node-groups (rc=%d)",
                        rc  ) ;
            }
            goto reply ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to create arg1 and arg2 for command: %s",
                  e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto reply ;
      }
   reply :
      msgReply.header.messageLength = sizeof(MsgOpReply);
      msgReply.header.opCode = MAKE_REPLY_TYPE( pMsgHeader->opCode );
      msgReply.header.TID = pMsgHeader->TID;
      msgReply.header.routeID.value = 0;
      msgReply.header.requestID = pMsgHeader->requestID;
      msgReply.contextID = contextID;
      msgReply.startFrom = 0;
      msgReply.numReturned = 0;

      if ( rc != SDB_OK )
      {
         msgReply.flags = rc;
         if ( SDB_PERM == rc)
         {
            msgReply.flags = SDB_CLS_NOT_PRIMARY ;
         }
      }
      else
      {
         _addContext( handle, pMsgHeader->TID, contextID );
         msgReply.flags = 0;
      }
      PD_TRACE1 ( SDB_CATMAINCT_QUERYREQUEST,
                  PD_PACK_INT ( msgReply.flags ) ) ;
      rc = _pCatCB->netWork()->syncSend ( handle, &msgReply );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "failed to send the message(routeID=%lld)",
                  pMsgHeader->routeID.value);   //print the routeID, don't print handle,
                                                //because we can't get any
                                                //useful info from handle
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CATMAINCT_QUERYREQUEST, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   INT32 catMainController::_defaultMsgFunc( NET_HANDLE handle,
                                             MsgHeader * msg )
   {
      return _processMsg( handle, msg ) ;
   }

   INT32 catMainController::_processMsg( const NET_HANDLE &handle,
                                         MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;

      switch ( pMsg->opCode )
      {
      case MSG_BS_QUERY_REQ:
         {
            rc = _processQueryMsg( handle, pMsg );
            break;
         }
      case MSG_BS_GETMORE_REQ :
         {
            rc = _processGetMoreMsg( handle, pMsg ) ;
            break ;
         }
      case MSG_BS_KILL_CONTEXT_REQ:
         {
            rc = _processKillContext( handle, pMsg ) ;
            break;
         }
      case MSG_BS_INTERRUPTE :
         {
            rc = _processInterruptMsg( handle, pMsg ) ;
            break ;
         }
      case MSG_BS_DISCONNECT :
         {
            rc = _processDisconnectMsg( handle, pMsg ) ;
            break ;
         }
      case MSG_CAT_QUERY_DATA_GRP_REQ :
         {
            rc = _processQueryDataGrp( handle, pMsg ) ;
            break ;
         }
      case MSG_CAT_QUERY_COLLECTIONS_REQ :
         {
            rc = _processQueryCollections( handle, pMsg ) ;
            break ;
         }
      case MSG_CAT_QUERY_COLLECTIONSPACES_REQ :
         {
            rc = _processQueryCollectionSpaces ( handle, pMsg ) ;
            break ;
         }
      case MSG_AUTH_VERIFY_REQ :
         {
            rc = _processAuthenticate( handle, pMsg ) ;
            break ;
         }
      case MSG_AUTH_CRTUSR_REQ :
         {
            rc = _processAuthCrt( handle, pMsg ) ;
            break ;
         }
      case MSG_AUTH_DELUSR_REQ :
         {
            rc = _processAuthDel( handle, pMsg ) ;
            break ;
         }
      case MSG_COOR_CHECK_ROUTEID_REQ :
         {
            rc = _processCheckRouteID( handle, pMsg ) ;
            break;
         }
      default :
         {
            PD_LOG( PDERROR, "Recieve unknow msg[opCode:(%d)%d, len: %d, "
                    "tid: %d, reqID: %lld, nodeID: %u.%u.%u]",
                    IS_REPLY_TYPE(pMsg->opCode), GET_REQUEST_TYPE(pMsg->opCode),
                    pMsg->messageLength, pMsg->TID, pMsg->requestID,
                    pMsg->routeID.columns.groupID, pMsg->routeID.columns.nodeID,
                    pMsg->routeID.columns.serviceID ) ;
            rc = SDB_UNKNOWN_MESSAGE ;

            BSONObj err = utilGetErrorBson( rc, _pEDUCB->getInfo(
                                            EDU_INFO_ERROR ) ) ;
            MsgOpReply reply ;
            reply.header.opCode = MAKE_REPLY_TYPE( pMsg->opCode ) ;
            reply.header.messageLength = sizeof( MsgOpReply ) + err.objsize() ;
            reply.header.requestID = pMsg->requestID ;
            reply.header.routeID.value = 0 ;
            reply.header.TID = pMsg->TID ;
            reply.flags = rc ;
            reply.contextID = -1 ;
            reply.numReturned = 1 ;
            reply.startFrom = 0 ;

            _pCatCB->netWork()->syncSend( handle, (MsgHeader*)&reply,
                                          (void*)err.objdata(),
                                          err.objsize() ) ;
            break ;
         }
      }

      if ( rc && SDB_UNKNOWN_MESSAGE != rc )
      {
         PD_LOG( PDWARNING, "Process msg[opCode:(%d)%d, len: %d, tid: %d, "
                 "reqID: %lld, nodeID: %u.%u.%u] failed, rc: %d",
                 IS_REPLY_TYPE(pMsg->opCode), GET_REQUEST_TYPE(pMsg->opCode),
                 pMsg->messageLength, pMsg->TID, pMsg->requestID,
                 pMsg->routeID.columns.groupID, pMsg->routeID.columns.nodeID,
                 pMsg->routeID.columns.serviceID, rc ) ;
      }

      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_AUTHCRT, "catMainController::_processAuthCrt" )
   INT32 catMainController::_processAuthCrt( const NET_HANDLE &handle,
                                             MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT_AUTHCRT ) ;
      MsgAuthCrtUsr *msg = ( MsgAuthCrtUsr * )pMsg ;
      BSONObj obj ;
      MsgAuthCrtReply reply ;

      if ( !pmdIsPrimary() || !_isActived )
      {
         rc = SDB_CLS_NOT_PRIMARY ;
         goto error ;
      }

      rc = extractAuthMsg( &(msg->header), obj ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _pAuthCB->createUsr( obj, _pEDUCB, _pCatCB->majoritySize() ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      reply.header.res = rc ;
      PD_TRACE1 ( SDB_CATMAINCT_AUTHCRT,
                  PD_PACK_INT ( rc ) ) ;
      reply.header.header.TID = msg->header.TID ;
      reply.header.header.requestID = msg->header.requestID ;
      _pCatCB->netWork()->syncSend( handle, &reply );
      PD_TRACE_EXITRC ( SDB_CATMAINCT_AUTHCRT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_AUTHENTICATE, "catMainController::_processAuthenticate" )
   INT32 catMainController::_processAuthenticate( const NET_HANDLE &handle,
                                                  MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT_AUTHENTICATE ) ;
      MsgAuthentication *msg = ( MsgAuthentication * )pMsg ;
      BSONObj obj ;
      MsgAuthReply reply ;

      if ( !pmdIsPrimary() && !_isActived )
      {
         rc = SDB_CLS_NOT_PRIMARY ;
         goto error ;
      }

      if ( !_pAuthCB->needAuthenticate() )
      {
         goto done ;
      }
      rc = extractAuthMsg( &(msg->header), obj ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _pAuthCB->authenticate( obj, _pEDUCB ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      reply.header.res = rc ;
      PD_TRACE1 ( SDB_CATMAINCT_AUTHENTICATE,
                  PD_PACK_INT ( rc ) ) ;
      reply.header.header.TID = msg->header.TID ;
      reply.header.header.requestID = msg->header.requestID ;
      _pCatCB->netWork()->syncSend( handle, &reply );
      PD_TRACE_EXITRC ( SDB_CATMAINCT_AUTHENTICATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_AUTHDEL, "catMainController::_processAuthDel" )
   INT32 catMainController::_processAuthDel( const NET_HANDLE &handle,
                                             MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATMAINCT_AUTHDEL ) ;
      MsgAuthDelUsr *msg = ( MsgAuthDelUsr * )pMsg ;
      BSONObj obj ;
      MsgAuthDelReply reply ;

      if ( !pmdIsPrimary() || !_isActived )
      {
         rc = SDB_CLS_NOT_PRIMARY ;
         goto error ;
      }

      rc = extractAuthMsg( &(msg->header), obj ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _pAuthCB->removeUsr( obj, _pEDUCB, _pCatCB->majoritySize() ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      reply.header.res = rc ;
      PD_TRACE1 ( SDB_CATMAINCT_AUTHDEL,
                  PD_PACK_INT ( rc ) ) ;
      reply.header.header.TID = msg->header.TID ;
      reply.header.header.requestID = msg->header.requestID ;
      _pCatCB->netWork()->syncSend( handle, &reply );
      PD_TRACE_EXITRC ( SDB_CATMAINCT_AUTHDEL, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB_CATMAINCT_CHECKROUTEID, "catMainController::_processCheckRouteID" )
   INT32 catMainController::_processCheckRouteID( const NET_HANDLE &handle,
                                                  MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_CATMAINCT_CHECKROUTEID ) ;
      MsgCoordCheckRouteID *pMsgReq = (MsgCoordCheckRouteID *)pMsg;
      MsgOpReply reply;
      reply.contextID = -1;
      reply.numReturned = 0;
      reply.startFrom = 0;
      reply.header.messageLength = sizeof( MsgOpReply );
      reply.header.opCode = MSG_COOR_CHECK_ROUTEID_RSP;
      reply.header.requestID = pMsgReq->header.requestID;
      reply.header.routeID.value = 0;
      reply.header.TID = pMsgReq->header.TID;
      MsgRouteID localRouteID = _pCatCB->netWork()->localID();
      if ( pMsgReq->dstRouteID.columns.nodeID != localRouteID.columns.nodeID
         || pMsgReq->dstRouteID.columns.groupID != localRouteID.columns.groupID
         || pMsgReq->dstRouteID.columns.serviceID != localRouteID.columns.serviceID )
      {
         rc = SDB_INVALID_ROUTEID;
         PD_LOG ( PDERROR, "routeID is different from the local"
                  "RemoteRouteID(groupID=%u, nodeID=%u, serviceID=%u)"
                  "LocalRouteID(groupID=%u, nodeID=%u, serviceID=%u)",
                  pMsgReq->dstRouteID.columns.groupID,
                  pMsgReq->dstRouteID.columns.nodeID,
                  pMsgReq->dstRouteID.columns.serviceID,
                  localRouteID.columns.groupID,
                  localRouteID.columns.nodeID,
                  localRouteID.columns.serviceID );
      }
      reply.flags = rc;
      _pCatCB->netWork()->syncSend( handle, (void *)&reply );
      PD_TRACE_EXITRC ( SDB_CATMAINCT_CHECKROUTEID, rc ) ;
      return rc;
   }

   void catMainController::_addContext( const UINT32 &handle, UINT32 tid,
                                        INT64 contextID )
   {
      PD_LOG( PDDEBUG, "add context( handle=%u, contextID=%lld )",
              handle, contextID );
      _contextLst[ contextID ] = ossPack32To64( handle, tid ) ;
   }

   void catMainController::_delContextByHandle( const UINT32 &handle )
   {
      PD_LOG ( PDDEBUG, "delete context( handle=%u )",
               handle ) ;
      UINT32 saveTid = 0 ;
      UINT32 saveHandle = 0 ;

      CONTEXT_LIST::iterator iterMap = _contextLst.begin() ;
      while ( iterMap != _contextLst.end() )
      {
         ossUnpack32From64( iterMap->second, saveHandle, saveTid ) ;
         if ( handle != saveHandle )
         {
            ++iterMap ;
            continue ;
         }
         // rtn delete
         _pRtnCB->contextDelete( iterMap->first, _pEDUCB );
         _contextLst.erase( iterMap++ ) ;
      }
   }

   void catMainController::_delContext( const UINT32 &handle,
                                        UINT32 tid )
   {
      PD_LOG ( PDDEBUG, "delete context( handle=%u, tid=%u )",
               handle, tid ) ;
      UINT32 saveTid = 0 ;
      UINT32 saveHandle = 0 ;

      CONTEXT_LIST::iterator iterMap = _contextLst.begin() ;
      while ( iterMap != _contextLst.end() )
      {
         ossUnpack32From64( iterMap->second, saveHandle, saveTid ) ;
         if ( handle != saveHandle || tid != saveTid )
         {
            ++iterMap ;
            continue ;
         }
         // rtn delete
         _pRtnCB->contextDelete( iterMap->first, _pEDUCB ) ;
         _contextLst.erase( iterMap++ ) ;
      }
   }

   void catMainController::_delContextByID( INT64 contextID, BOOLEAN rtnDel )
   {
      PD_LOG ( PDDEBUG, "delete context( contextID=%lld )", contextID ) ;

      CONTEXT_LIST::iterator iterMap = _contextLst.find( contextID ) ;
      if ( iterMap != _contextLst.end() )
      {
         if ( rtnDel )
         {
            _pRtnCB->contextDelete( iterMap->first, _pEDUCB ) ;
         }
         _contextLst.erase( iterMap ) ;
      }
   }

}

