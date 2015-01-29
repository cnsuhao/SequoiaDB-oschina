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

   Source File Name = clsShardSession.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/12/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "clsShardSession.hpp"
#include "pmd.hpp"
#include "clsMgr.hpp"
#include "msgMessage.hpp"
#include "pdTrace.hpp"
#include "clsTrace.hpp"
#include "rtnDataSet.hpp"
#include "rtnContextShdOfLob.hpp"

using namespace bson ;

namespace engine
{

#define SHD_SESSION_TIMEOUT         (60)
#define SHD_INTERRUPT_CHECKPOINT    (10)

   BEGIN_OBJ_MSG_MAP( _clsShdSession, _pmdAsyncSession )
      ON_MSG ( MSG_BS_UPDATE_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_INSERT_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_DELETE_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_QUERY_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_GETMORE_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_KILL_CONTEXT_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_MSG_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_INTERRUPTE, _onOPMsg )
      ON_MSG ( MSG_BS_TRANS_BEGIN_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_TRANS_COMMIT_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_TRANS_ROLLBACK_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_TRANS_COMMITPRE_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_TRANS_UPDATE_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_TRANS_DELETE_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_TRANS_INSERT_REQ, _onOPMsg )
      ON_MSG ( MSG_COOR_CHECK_ROUTEID_REQ, _onOPMsg )
#if defined (_DEBUG)
      ON_MSG ( MSG_AUTH_VERIFY_REQ, _onOPMsg )
      ON_MSG ( MSG_AUTH_CRTUSR_REQ, _onOPMsg )
      ON_MSG ( MSG_AUTH_DELUSR_REQ, _onOPMsg )
#endif
      ON_MSG ( MSG_BS_LOB_OPEN_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_LOB_WRITE_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_LOB_READ_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_LOB_CLOSE_REQ, _onOPMsg )
      ON_MSG ( MSG_BS_LOB_REMOVE_REQ, _onOPMsg )
      ON_MSG ( MSG_CAT_GRP_CHANGE_NTY, _onCatalogChangeNtyMsg )

      ON_EVENT( PMD_EDU_EVENT_TRANS_STOP, _onTransStopEvnt )
   END_OBJ_MSG_MAP()

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSDSESS__CLSSHDSESS, "_clsShdSession::_clsShdSession" )
   _clsShdSession::_clsShdSession ( UINT64 sessionID )
      :_pmdAsyncSession ( sessionID )
   {
      PD_TRACE_ENTRY ( SDB__CLSSDSESS__CLSSHDSESS ) ;
      _pCollectionName  = NULL ;
      pmdKRCB *pKRCB = pmdGetKRCB () ;
      _pReplSet  = sdbGetReplCB () ;
      _pShdMgr   = sdbGetShardCB () ;
      _pCatAgent = pKRCB->getClsCB ()->getCatAgent () ;
      _pDmsCB    = pKRCB->getDMSCB () ;
      _pDpsCB    = pKRCB->getDPSCB () ;
      _pRtnCB    = pKRCB->getRTNCB () ;
      PD_TRACE_EXIT ( SDB__CLSSDSESS__CLSSHDSESS ) ;
   }

   _clsShdSession::~_clsShdSession ()
   {
      _pReplSet  = NULL ;
      _pShdMgr   = NULL ;
      _pCatAgent = NULL ;
      _pDmsCB    = NULL ;
      _pRtnCB    = NULL ;
      _pDpsCB    = NULL ;
      _pCollectionName = NULL ;
   }

   SDB_SESSION_TYPE _clsShdSession::sessionType() const
   {
      return SDB_SESSION_SHARD ;
   }

   EDU_TYPES _clsShdSession::eduType () const
   {
      return EDU_TYPE_SHARDAGENT ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS_ONRV, "_clsShdSession::onRecieve" )
   void _clsShdSession::onRecieve ( const NET_HANDLE netHandle,
                                    MsgHeader * msg )
   {
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS_ONRV ) ;
      ossGetCurrentTime( _lastRecvTime ) ;
      PD_TRACE_EXIT ( SDB__CLSSHDSESS_ONRV ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS_TMOUT, "_clsShdSession::timeout" )
   BOOLEAN _clsShdSession::timeout ( UINT32 interval )
   {
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS_TMOUT ) ;
      BOOLEAN ret = FALSE ;
      ossTimestamp curTime ;
      ossGetCurrentTime ( curTime ) ;

      if ( curTime.time - _lastRecvTime.time > SHD_SESSION_TIMEOUT &&
           _pEDUCB->contextNum() == 0 &&
           ( _pEDUCB->getTransID() == DPS_INVALID_TRANS_ID ||
           !(sdbGetReplCB()->primaryIsMe())))
      {
         ret = TRUE ;
         goto done ;
      }
   done :
      PD_TRACE_EXIT ( SDB__CLSSHDSESS_TMOUT ) ;
      return ret ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONDETACH, "_clsShdSession::_onDetach" )
   void _clsShdSession::_onDetach ()
   {
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONDETACH ) ;
      if ( _pEDUCB )
      {
         INT64 contextID = -1 ;
         while ( -1 != ( contextID = _pEDUCB->contextPeek() ) )
         {
            _pRtnCB->contextDelete ( contextID, NULL ) ;
         }

         INT32 rcTmp = rtnTransRollback( _pEDUCB, _pDpsCB ) ;
         if ( rcTmp)
         {
            PD_LOG ( PDERROR, "Failed to rollback(rc=%d)", rcTmp ) ;
         }
      }

      _pmdAsyncSession::_onDetach () ;
      PD_TRACE_EXIT ( SDB__CLSSHDSESS__ONDETACH ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__DFMSGFUNC, "_clsShdSession::_defaultMsgFunc" )
   INT32 _clsShdSession::_defaultMsgFunc ( NET_HANDLE handle, MsgHeader * msg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__DFMSGFUNC ) ;
      rc = _onOPMsg( handle, msg ) ;
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__DFMSGFUNC, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__CK, "_clsShdSession::_check" )
   INT32 _clsShdSession::_check ( INT16 &w )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__CK ) ;

      if ( w < 0 )
      {
         PD_LOG( PDWARNING, "we get a w<0 here." ) ;
         w = 1 ;
      }
      else if ( w > 1 )
      {
         INT16 nodes = (INT16)_pReplSet->groupSize() ;
         if ( w > nodes )
         {
            w = nodes ;
         }
      }
      else
      {
      }

      if ( !_pReplSet->primaryIsMe () )
      {
         rc = SDB_CLS_NOT_PRIMARY ;
      }
      else if ( w > 1 && (INT16)(_pReplSet->ailves()) < w )
      {
         rc = SDB_CLS_NODE_NOT_ENOUGH ;
      }
      else if ( pmdGetKRCB()->getTransCB()->isDoRollback() )
      {
         rc = SDB_DPS_TRANS_DOING_ROLLBACK ;
      }

      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__CK, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__CKCATA, "_clsShdSession::_checkCata" )
   INT32 _clsShdSession::_checkCata ( INT32 version, const CHAR * name,
                                      INT16 &w, BOOLEAN &isMainCL,
                                      BOOLEAN exceptVer )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__CKCATA ) ;
      INT32 curVer = -1 ;
      INT16 curW = 0 ;
      UINT32 groupCount = 0 ;
      _clsCatalogSet *set = NULL ;

      if ( _pReplSet->isFullSync() )
      {
         rc = SDB_CLS_FULL_SYNC ;
         goto error ;
      }

      if ( exceptVer )
      {
         goto done ;
      }

      _pCatAgent->lock_r () ;
      set = _pCatAgent->collectionSet( name ) ;
      if ( set )
      {
         curW = set->getW() ;
         curVer = set->getVersion() ;
         groupCount = set->groupCount() ;
         isMainCL = set->isMainCL();
      }
      _pCatAgent->release_r () ;

      if ( curVer < 0 || curVer < version )
      {
         rc = curVer < 0 ? SDB_CLS_NO_CATALOG_INFO :
                           SDB_CLS_DATA_NODE_CAT_VER_OLD ;
      }
      else if ( curVer > version
               || ( 0 == groupCount && !isMainCL ) )
      {
         if ( 0 == groupCount )
         {
            _pCatAgent->lock_w() ;
            _pCatAgent->clear( name ) ;
            _pCatAgent->release_w() ;
         }

         PD_LOG ( PDINFO, "Collecton[%s]: self verions:%d, coord version:%d, "
                  "groupCount:%d", name, curVer, version, groupCount ) ;
         rc = SDB_CLS_COORD_NODE_CAT_VER_OLD ;
      }
      else if ( 0 == w )
      {
         if ( curW > 1 )
         {
            INT16 nodes = (INT16)_pReplSet->groupSize() ;
            if ( curW > nodes )
            {
               curW = nodes ;
            }
         }

         if ( curW > 1 && _pReplSet->ailves() < (UINT32)curW )
         {
            rc = SDB_CLS_NODE_NOT_ENOUGH ;
         }
         else
         {
            w = curW ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__CKCATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__REPLY, "_clsShdSession::_reply" )
   INT32 _clsShdSession::_reply ( MsgOpReply * header, const CHAR * buff,
                                  UINT32 size )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__REPLY ) ;

      if ( (UINT32)(header->header.messageLength) !=
           sizeof (MsgOpReply) + size )
      {
         PD_LOG ( PDERROR, "Session[%s] reply message length error[%u != %u]",
                  sessionName() ,header->header.messageLength,
                  sizeof ( MsgOpReply ) + size ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( size > 0 )
      {
         rc = routeAgent()->syncSend ( _netHandle, (MsgHeader *)header, 
                                       (void*)buff, size ) ;
      }
      else
      {
         rc = routeAgent()->syncSend ( _netHandle, (void *)header ) ;
      }

      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Session[%s] send reply message failed[rc:%d]",
            sessionName(), rc ) ;
      }
   done:
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__REPLY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONOPMSG, "_clsShdSession::_onOPMsg" )
   INT32 _clsShdSession::_onOPMsg ( NET_HANDLE handle, MsgHeader * msg )
   {
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONOPMSG ) ;
      BOOLEAN loop = TRUE ;
      INT32 loopTime = 0 ;
      INT32 rc = SDB_OK ;

      INT32 flags = 0 ;
      SINT64 contextID = -1 ;
      INT32 startFrom = 0 ;
      INT32 numReturn = 0 ;
      const CHAR  *pReponseBuff = NULL ;
      INT32  buffLen = 0 ;
      rtnContextBuf buffObj ;
      _pCollectionName = NULL ;
      BOOLEAN isNeedRollback = FALSE;

      MON_START_OP( _pEDUCB->getMonAppCB() ) ;
      _pEDUCB->resetLsn() ;

      while ( loop )
      {
         switch ( msg->opCode )
         {
            case MSG_BS_UPDATE_REQ :
               isNeedRollback = TRUE ;
               rc = _onUpdateReqMsg ( handle, msg, contextID ) ;
               break ;
            case MSG_BS_INSERT_REQ :
               isNeedRollback = TRUE ;
               rc = _onInsertReqMsg ( handle, msg ) ;
               break ;
            case MSG_BS_DELETE_REQ :
               isNeedRollback = TRUE ;
               rc = _onDeleteReqMsg ( handle, msg, contextID ) ;
               break ;
            case MSG_BS_QUERY_REQ :
               rc = _onQueryReqMsg ( handle, msg, buffObj, startFrom, contextID ) ;
               break ;
            case MSG_BS_GETMORE_REQ :
               rc = _onGetMoreReqMsg ( msg, buffObj, startFrom, contextID ) ;
               break ;
            case MSG_BS_TRANS_UPDATE_REQ :
               isNeedRollback = TRUE ;
               rc = _onTransUpdateReqMsg ( handle, msg, contextID ) ;
               break ;
            case MSG_BS_TRANS_INSERT_REQ :
               isNeedRollback = TRUE ;
               rc = _onTransInsertReqMsg ( handle, msg ) ;
               break ;
            case MSG_BS_TRANS_DELETE_REQ :
               isNeedRollback = TRUE ;
               rc = _onTransDeleteReqMsg ( handle, msg, contextID ) ;
               break ;
            case MSG_BS_KILL_CONTEXT_REQ :
               rc = _onKillContextsReqMsg ( handle, msg ) ;
               break ;
            case MSG_BS_MSG_REQ :
               rc = _onMsgReq ( handle, msg ) ;
               break ;
            case MSG_BS_INTERRUPTE :
               rc = _onInterruptMsg( handle, msg ) ;
               break ;
#if defined (_DEBUG)
            case MSG_AUTH_VERIFY_REQ :
            case MSG_AUTH_CRTUSR_REQ :
            case MSG_AUTH_DELUSR_REQ :
               rc = SDB_OK ;
               break ;
#endif
            case MSG_BS_TRANS_BEGIN_REQ :
               rc = _onTransBeginMsg();
               break;

            case MSG_BS_TRANS_COMMIT_REQ :
               isNeedRollback = TRUE ;
               rc = _onTransCommitMsg();
               break;

            case MSG_BS_TRANS_ROLLBACK_REQ:
               rc = _onTransRollbackMsg();
               break;

            case MSG_BS_TRANS_COMMITPRE_REQ:
               rc = _onTransCommitPreMsg( msg );
               break;

            case MSG_COOR_CHECK_ROUTEID_REQ:
               rc = _onCheckRouteIDReqMsg( msg );
               break;

            case MSG_BS_LOB_OPEN_REQ:
               rc = _onOpenLobReq( msg, contextID,
                                   buffObj ) ;
               break ;
            case MSG_BS_LOB_WRITE_REQ:
               rc = _onWriteLobReq( msg ) ;
               break ;
            case MSG_BS_LOB_READ_REQ:
               rc = _onReadLobReq( msg, buffObj ) ;
               break ;
            case MSG_BS_LOB_CLOSE_REQ:
               rc = _onCloseLobReq( msg ) ;
               break ;
            case MSG_BS_LOB_REMOVE_REQ:
               rc = _onRemoveLobReq( msg ) ;
               break ;
            case MSG_BS_LOB_UPDATE_REQ:
               rc = _onUpdateLobReq( msg ) ;
               break ;
            default:
               rc = SDB_CLS_UNKNOW_MSG ;
               break ;
         }

         if ( ( SDB_CLS_NO_CATALOG_INFO == rc ||
                SDB_CLS_DATA_NODE_CAT_VER_OLD == rc ||
                SDB_CLS_COORD_NODE_CAT_VER_OLD == rc ) && loopTime < 1 )
         {
            loopTime++ ;
            PD_LOG ( PDWARNING, "Catalog is empty or older[rc:%d] in "
                     "session[%s]", rc, sessionName() ) ;
            rc = _pShdMgr->syncUpdateCatalog( _pCollectionName ) ;
            if ( SDB_OK == rc )
            {
               continue ;
            }
         }
         else if ( (SDB_DMS_CS_NOTEXIST == rc || SDB_DMS_NOTEXIST == rc)
                   && _pCollectionName && _pReplSet->primaryIsMe() )
         {
            if ( SDB_DMS_CS_NOTEXIST == rc )
            {
               rc = _createCSByCatalog( _pCollectionName ) ;
            }
            else if ( SDB_DMS_NOTEXIST == rc )
            {
               rc = _createCLByCatalog( _pCollectionName ) ;
            }

            if ( SDB_OK == rc )
            {
               PD_LOG ( PDEVENT, "Session[%s]: Create CS/CL[%s] by catalog",
                        sessionName(), _pCollectionName ) ;
               continue ;
            }
         }

         if ( SDB_CLS_DATA_NODE_CAT_VER_OLD == rc )
         {
            rc = SDB_CLS_COORD_NODE_CAT_VER_OLD ;
         }

         loop = FALSE ;
      }

      if ( MSG_BS_INTERRUPTE == msg->opCode )
      {
         goto done ;
      }

      if ( rc < -SDB_MAX_ERROR || rc > SDB_MAX_WARNING )
      {
         PD_LOG ( PDERROR, "Session[%s] OP[type:%u] return code error[rc:%d]",
                  sessionName(), msg->opCode, rc ) ;
         rc = SDB_SYS ;
      }

      _replyHeader.header.opCode = MAKE_REPLY_TYPE( msg->opCode ) ;
      _replyHeader.header.messageLength = sizeof ( MsgOpReply ) ;
      _replyHeader.header.requestID = msg->requestID ;
      _replyHeader.header.TID = msg->TID ;
      _replyHeader.header.routeID.value = 0 ;

      if ( SDB_OK != rc )
      {
         numReturn = 1 ;
         flags = rc ;

         if ( isNeedRollback && _pReplSet->primaryIsMe () )
         {
            INT32 rcTmp = rtnTransRollback( _pEDUCB, _pDpsCB ) ;
            if ( rcTmp )
            {
               PD_LOG ( PDERROR, "Failed to rollback(rc=%d)", rcTmp ) ;
            }
         }

         _errorInfo = utilGetErrorBson( rc, _pEDUCB->getInfo(
                                        EDU_INFO_ERROR ) ) ;
         buffLen = _errorInfo.objsize() ;
         pReponseBuff = _errorInfo.objdata() ;

         if ( rc != SDB_DMS_EOC )
         {
            PD_LOG ( PDERROR, "Session[%s] process OP[type:%u] failed[rc:%d]",
                     sessionName(), msg->opCode, rc ) ;
         }
      }
      else
      {
         pReponseBuff = buffObj.data() ;
         buffLen = buffObj.size() ;
         numReturn = buffObj.recordNum() ;
      }

      _replyHeader.header.messageLength += buffLen ;
      _replyHeader.flags = flags ;
      _replyHeader.contextID = contextID ;
      _replyHeader.numReturned = numReturn ;
      _replyHeader.startFrom = startFrom ;

      rc = _reply ( &_replyHeader, pReponseBuff, buffLen ) ;

   done:
      eduCB()->writingDB( FALSE ) ;
      MON_END_OP( _pEDUCB->getMonAppCB() ) ;
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__ONOPMSG, rc ) ;
      return rc ;
   }

   INT32 _clsShdSession::_createCSByCatalog( const CHAR * clFullName )
   {
      INT32 rc = SDB_OK ;
      CHAR csName[ DMS_COLLECTION_SPACE_NAME_SZ + 1 ] = { 0 } ;
      INT32 index = 0 ;
      while ( clFullName[ index ] && index < DMS_COLLECTION_SPACE_NAME_SZ )
      {
         if ( '.' == clFullName[ index ] )
         {
            break ;
         }
         csName[ index ] = clFullName[ index ] ;
         ++index ;
      }

      UINT32 pageSize = DMS_PAGE_SIZE_DFT ;
      UINT32 lobPageSize = DMS_DEFAULT_LOB_PAGE_SZ ;
      rc = _pShdMgr->rGetCSPageSize( csName, pageSize, lobPageSize ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDWARNING, "Get collection space[%s] page size from catalog "
                 "failed, rc: %d, use default pagesize", csName, rc ) ;
      }
      rc = rtnCreateCollectionSpaceCommand( csName, _pEDUCB, _pDmsCB, _pDpsCB,
                                            pageSize, lobPageSize, FALSE ) ;
      if ( SDB_DMS_CS_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      else if ( SDB_OK != rc )
      {
         PD_LOG( PDWARNING, "Create collection space[%s] by catalog failed, rc:"
                 " %d", csName, rc ) ;
      }
      return rc ;
   }

   INT32 _clsShdSession::_createCLByCatalog( const CHAR * clFullName )
   {
      INT32 rc = SDB_OK ;
      BSONObj shardingKey ;
      UINT32 attribute = 0 ;
      BOOLEAN isMainCL = FALSE;
      UINT32 groupCount = 0 ;

      _pCatAgent->lock_r() ;
      clsCatalogSet *set = _pCatAgent->collectionSet( clFullName ) ;
      if ( NULL == set )
      {
         _pCatAgent->release_r() ;
         rc = SDB_DMS_NOTEXIST ;
         PD_LOG( PDERROR, "can not find collection:%s", clFullName ) ;
         goto error ;
      }

      if ( set->isSharding() && set->ensureShardingIndex() )
      {
         shardingKey = set->getShardingKey().getOwned() ;
      }

      attribute = set->getAttribute() ;
      isMainCL = set->isMainCL() ;
      groupCount = set->groupCount() ;
      _pCatAgent->release_r() ;

      if ( isMainCL )
      {
         BSONObj boMatcher;
         BSONObj boNewMatcher;
         std::vector< std::string > subCLList;
         std::vector< std::string >::iterator iter;
         rc = _getSubCLList( boMatcher, clFullName, boNewMatcher, subCLList );
         PD_RC_CHECK( rc, PDERROR, "failed to get sub collection(rc=%d)", rc );
         iter = subCLList.begin();
         while ( iter != subCLList.end() )
         {
            BSONObj subShardingKey;
            UINT32 subAttribute = 0 ;
            _pCatAgent->lock_r() ;
            clsCatalogSet *subSet = _pCatAgent->collectionSet( iter->c_str() );
            if ( NULL == subSet )
            {
               _pCatAgent->release_r();
               PD_RC_CHECK( SDB_DMS_NOTEXIST, PDERROR,
                           "can not find sub-collection:%s", iter->c_str() );
            }
            if ( subSet->isSharding() && subSet->ensureShardingIndex() )
            {
               subShardingKey = subSet->getShardingKey().getOwned() ;
            }
            subAttribute = subSet->getAttribute() ;
            _pCatAgent->release_r();
            rc = rtnCreateCollectionCommand( iter->c_str(), subShardingKey,
                                             subAttribute, _pEDUCB, _pDmsCB,
                                             _pDpsCB, 0, FALSE );
            if ( SDB_OK != rc && SDB_DMS_EXIST != rc )
            {
               PD_RC_CHECK( rc, PDERROR,
                            "create sub-collection(%s) failed(rc=%d)",
                            iter->c_str(), rc );
            }
            rc = SDB_OK;
            ++iter;
         }
      }
      else
      {
         if( 0 == groupCount )
         {
            _pCatAgent->lock_w() ;
            _pCatAgent->clear( clFullName ) ;
            _pCatAgent->release_w() ;
            rc = SDB_CLS_COORD_NODE_CAT_VER_OLD ;
            PD_LOG( PDERROR, "can not find collection:%s", clFullName ) ;
            goto error ;
         }
         rc = rtnCreateCollectionCommand( clFullName, shardingKey, attribute,
                                          _pEDUCB, _pDmsCB, _pDpsCB, 0,
                                          FALSE ) ;
         if ( SDB_DMS_EXIST == rc )
         {
            rc = SDB_OK ;
         }
      }
      if ( SDB_OK != rc )
      {
         PD_LOG( PDWARNING, "Create collection[%s] by catalog failed, rc: %d",
                 clFullName, rc ) ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONUPREQMSG, "_clsShdSession::_onUpdateReqMsg" )
   INT32 _clsShdSession::_onUpdateReqMsg( NET_HANDLE handle, MsgHeader * msg,
                                          INT64 &updateNum )
   {
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONUPREQMSG ) ;
      PD_LOG ( PDDEBUG, "session[%s] _onUpdateReqMsg", sessionName() ) ;

      INT32 rc = SDB_OK ;
      MsgOpUpdate *pUpdate = (MsgOpUpdate*)msg ;
      INT32 flags = 0 ;
      CHAR *pCollectionName = NULL ;
      CHAR *pSelectorBuffer = NULL ;
      CHAR *pUpdatorBuffer = NULL ;
      CHAR *pHintBuffer = NULL ;
      BOOLEAN isMainCL = FALSE ;
      INT16 w = pUpdate->w ;

      rc = _check ( w ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = msgExtractUpdate( (CHAR*)msg, &flags, &pCollectionName,
                             &pSelectorBuffer, &pUpdatorBuffer, &pHintBuffer );
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Extract update message failed[rc:%d] in "
                  "session[%s]", rc, sessionName() ) ;
         goto error ;
      }
      _pCollectionName = pCollectionName ;
      _pEDUCB->writingDB( TRUE ) ; // it call must before _checkCata

      rc = _checkCata ( pUpdate->version, pCollectionName, w, isMainCL ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      try
      {
         BSONObj selector( pSelectorBuffer );
         BSONObj updator( pUpdatorBuffer );
         BSONObj hint( pHintBuffer );
         MON_SAVE_OP_DETAIL( _pEDUCB->getMonAppCB(), MSG_BS_UPDATE_REQ,
                           "CL:%s, Match:%s, Updator:%s, Hint:%s",
                           pCollectionName,
                           selector.toString( false, false ).c_str(),
                           updator.toString(false, false ).c_str(),
                           hint.toString(false, false ).c_str() ) ;

         PD_LOG ( PDDEBUG, "Session[%s] Update: selctor: %s\nupdator: %s\n"
                  "hint: %s", sessionName(), selector.toString().c_str(),
                  updator.toString().c_str(), hint.toString().c_str() ) ;
         if ( isMainCL )
         {
            rc = _updateToMainCL( pCollectionName, selector, updator, hint,
                                  flags, _pEDUCB, _pDmsCB, _pDpsCB, w,
                                  ( pUpdate->flags & FLG_UPDATE_RETURNNUM ) ?
                                  &updateNum : NULL );
         }
         else
         {
            rc = rtnUpdate( pCollectionName, selector, updator, hint, 
                            flags, _pEDUCB, _pDmsCB, _pDpsCB, w,
                            ( pUpdate->flags & FLG_UPDATE_RETURNNUM ) ?
                            &updateNum : NULL ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Session[%s] Failed to create selector and updator "
                  "for update: %s", sessionName(), e.what () ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__ONUPREQMSG, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONINSTREQMSG, "_clsShdSession::_onInsertReqMsg" )
   INT32 _clsShdSession::_onInsertReqMsg ( NET_HANDLE handle, MsgHeader * msg )
   {
      PD_LOG ( PDDEBUG, "session[%s] _onInsertReqMsg", sessionName() ) ;

      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONINSTREQMSG ) ;
      INT32 flags = 0 ;
      CHAR *pCollectionName = NULL ;
      CHAR *pInsertorBuffer = NULL ;
      INT32 recordNum = 0 ;
      BOOLEAN isMainCL = FALSE;
      MsgOpInsert *pInsert = (MsgOpInsert*)msg ;
      INT16 w = pInsert->w ;
      rc = _check ( w ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = msgExtractInsert ( (CHAR*)msg,  &flags, &pCollectionName,
                              &pInsertorBuffer, recordNum ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Session[%s] extract insert msg failed[rc:%d]",
                  sessionName(), rc ) ;
         goto error ;
      }
      _pCollectionName = pCollectionName ;
      _pEDUCB->writingDB( TRUE ) ;  // it call must before _checkCata

      rc = _checkCata ( pInsert->version, pCollectionName, w, isMainCL ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      try
      {
         BSONObj insertor ( pInsertorBuffer ) ;
         MON_SAVE_OP_DETAIL( _pEDUCB->getMonAppCB(), MSG_BS_INSERT_REQ,
                           "CL:%s, Insertor:%s",
                           pCollectionName,
                           insertor.toString( false, false ).c_str() ) ;

         PD_LOG ( PDDEBUG, "Session[%s] Insert: %s\nCollection: %s",
                  sessionName(), insertor.toString().c_str(),
                  pCollectionName ) ;

         if ( isMainCL )
         {
            rc = _InsertToMainCL( insertor, recordNum, flags, w );
         }
         else
         {

            rc = rtnInsert ( pCollectionName, insertor, recordNum, flags, 
                             _pEDUCB, _pDmsCB, _pDpsCB, w ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Session[%s] Failed to create insertor for "
                  "insert: %s", sessionName(), e.what () ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__ONINSTREQMSG, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONDELREQMSG, "_clsShdSession::_onDeleteReqMsg" )
   INT32 _clsShdSession::_onDeleteReqMsg ( NET_HANDLE handle, MsgHeader * msg,
                                           INT64 &delNum )
   {
      PD_LOG ( PDDEBUG, "session[%s] _onDeleteReqMsg", sessionName() ) ;

      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONDELREQMSG ) ;
      INT32 flags = 0 ;
      CHAR *pCollectionName = NULL ;
      CHAR *pDeletorBuffer = NULL ;
      CHAR *pHintBuffer = NULL ;
      MsgOpDelete * pDelete = (MsgOpDelete*)msg ;
      INT16 w = pDelete->w ;
      BOOLEAN isMainCL = FALSE ;

      rc = _check ( w ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = msgExtractDelete ( (CHAR *)msg , &flags, &pCollectionName, 
                              &pDeletorBuffer, &pHintBuffer ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Session[%s] extract delete msg failed[rc:%d]",
            sessionName(), rc ) ;
         goto error ;
      }
      _pCollectionName = pCollectionName ;
      _pEDUCB->writingDB( TRUE ) ;  // it call must before _checkCata

      rc = _checkCata ( pDelete->version, pCollectionName, w, isMainCL ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      try
      {
         BSONObj deletor ( pDeletorBuffer ) ;
         BSONObj hint ( pHintBuffer ) ;
         MON_SAVE_OP_DETAIL( _pEDUCB->getMonAppCB(), MSG_BS_DELETE_REQ,
                           "CL:%s, Deletor:%s, Hint:%s",
                           pCollectionName,
                           deletor.toString( false, false ).c_str(),
                           hint.toString( false, false ).c_str() );

         PD_LOG ( PDDEBUG, "Session[%s] Delete: deletor: %s\nhint: %s",
                  sessionName(), deletor.toString().c_str(), 
                  hint.toString().c_str() ) ;

         if ( isMainCL )
         {
            rc = _deleteToMainCL( pCollectionName, deletor, hint, flags,
                                  _pEDUCB, _pDmsCB, _pDpsCB, w,
                                  ( pDelete->flags & FLG_DELETE_RETURNNUM ) ?
                                  &delNum : NULL );
         }
         else
         {

            rc = rtnDelete( pCollectionName, deletor, hint, flags, _pEDUCB, 
                            _pDmsCB, _pDpsCB, w,
                            ( pDelete->flags & FLG_DELETE_RETURNNUM ) ?
                            &delNum : NULL ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Session[%s] Failed to create deletor for "
                  "DELETE: %s", sessionName(), e.what () ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__ONDELREQMSG, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONQYREQMSG, "_clsShdSession::_onQueryReqMsg" )
   INT32 _clsShdSession::_onQueryReqMsg ( NET_HANDLE handle, MsgHeader * msg,
                                          rtnContextBuf &buffObj,
                                          INT32 &startingPos,
                                          INT64 &contextID )
   {
      PD_LOG ( PDDEBUG, "session[%s] _onQueryReqMsg", sessionName() ) ;

      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONQYREQMSG ) ;
      INT32 flags = 0 ;
      CHAR *pCollectionName = NULL ;
      CHAR *pQueryBuff = NULL ;
      CHAR *pFieldSelector = NULL ;
      CHAR *pOrderByBuffer = NULL ;
      CHAR *pHintBuffer = NULL ;
      INT64 numToSkip = -1 ;
      INT64 numToReturn = -1 ;
      MsgOpQuery *pQuery = (MsgOpQuery*)msg ;
      INT16 w = pQuery->w ;
      _rtnCommand *pCommand = NULL ;
      BOOLEAN isMainCL = FALSE;

      rc = msgExtractQuery ( (CHAR *)msg, &flags, &pCollectionName,
                             &numToSkip, &numToReturn, &pQueryBuff,
                             &pFieldSelector, &pOrderByBuffer, &pHintBuffer ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Session[%s] extract query msg failed[rc:%d]",
                  sessionName(), rc ) ;
         goto error ;
      }

      if ( !rtnIsCommand ( pCollectionName ) )
      {
         rtnContextBase *pContext = NULL ;
         _pCollectionName = pCollectionName ;
         w = 1 ;
         rc = _checkCata ( pQuery->version, pCollectionName, w, isMainCL ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         try
         {
            BSONObj matcher ( pQueryBuff ) ;
            BSONObj selector ( pFieldSelector ) ;
            BSONObj orderBy ( pOrderByBuffer ) ;
            BSONObj hint ( pHintBuffer ) ;
            MON_SAVE_OP_DETAIL( _pEDUCB->getMonAppCB(), MSG_BS_QUERY_REQ,
                              "CL:%s, Match:%s, Selector:%s, OrderBy:%s, Hint:%s",
                              pCollectionName,
                              matcher.toString().c_str(),
                              selector.toString().c_str(),
                              orderBy.toString().c_str(),
                              hint.toString().c_str() ) ;

            PD_LOG ( PDDEBUG, "Session[%s] Query: matcher: %s\nselector: "
                     "%s\norderBy: %s\nhint:%s", sessionName(),
                     matcher.toString().c_str(), selector.toString().c_str(),
                     orderBy.toString().c_str(), hint.toString().c_str() ) ;

            if ( !isMainCL )
            {
               rc = rtnQuery( pCollectionName, selector, matcher, orderBy,
                              hint, flags, _pEDUCB, numToSkip, numToReturn,
                              _pDmsCB, _pRtnCB, contextID, &pContext, TRUE ) ;
            }
            else
            {
               rc = _queryToMainCL( pCollectionName, selector, matcher,
                                    orderBy, hint, flags, _pEDUCB, numToSkip,
                                    numToReturn, contextID, &pContext ) ;
            }

            if ( rc )
            {
               goto error ;
            }

            if ( ( flags & FLG_QUERY_WITH_RETURNDATA ) && NULL != pContext )
            {
               rc = pContext->getMore( -1, buffObj, _pEDUCB ) ;
               if ( rc || pContext->eof() )
               {
                  _pRtnCB->contextDelete( contextID, _pEDUCB ) ;
                  contextID = -1 ;
               }
               startingPos = ( INT32 )buffObj.getStartFrom() ;

               if ( SDB_DMS_EOC == rc )
               {
                  rc = SDB_OK ;
               }
               else if ( rc )
               {
                  PD_LOG( PDERROR, "Session[%s] failed to query with return "
                          "data, rc: %d", sessionName(), rc ) ;
                  goto error ;
               }
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Session[%s] Failed to create matcher and "
                     "selector for QUERY: %s", sessionName(), e.what () ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      else
      {
         rc = rtnParserCommand( pCollectionName, &pCommand ) ;

         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR, "Parse command[%s] failed[rc:%d]",
                     pCollectionName, rc ) ;
            goto error ;
         }

         rc = rtnInitCommand( pCommand , flags, numToSkip, numToReturn,
                              pQueryBuff, pFieldSelector, pOrderByBuffer,
                              pHintBuffer ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         _pCollectionName = pCommand->collectionFullName () ;

         if ( pCommand->writable () )
         {
            rc = _check ( w ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            _pEDUCB->writingDB( TRUE ) ;  // it call must before _checkCata
         }
         else
         {
            w = 1 ;
         }

         if ( pCommand->collectionFullName() &&
              SDB_OK != ( rc = _checkCata( pQuery->version,
                          pCommand->collectionFullName(),
                          w, isMainCL ) ) )
         {
            goto error ;
         }
         else if ( ( CMD_CREATE_COLLECTIONSPACE == pCommand->type() ||
                     CMD_DROP_COLLECTIONSPACE == pCommand->type() ) &&
                    SDB_OK != ( rc = _checkCata( 0, "", w, isMainCL, TRUE ) ) )
         {
            goto error ;
         }

         PD_LOG ( PDDEBUG, "Command: %s", pCommand->name () ) ;

         if ( w < 1 )
         {
            w = 1 ;
         }

         if ( isMainCL )
         {
            rc = _runOnMainCL( pCollectionName, pCommand, flags, numToSkip,
                               numToReturn, pQueryBuff, pFieldSelector,
                               pOrderByBuffer, pHintBuffer, w, contextID );
         }
         else
         {
            rc = rtnRunCommand( pCommand, getServiceType(),
                                _pEDUCB, _pDmsCB, _pRtnCB,
                                _pDpsCB, w, &contextID ) ;
         }
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         if ( CMD_RENAME_COLLECTION == pCommand->type() )
         {
            _pCatAgent->lock_w () ;
            _pCatAgent->clear ( pCommand->collectionFullName() ) ;
            _pCatAgent->release_w () ;

            sdbGetClsCB()->invalidateCata( pCommand->collectionFullName() ) ;
         }
         else if ( CMD_DROP_COLLECTIONSPACE == pCommand->type () )
         {
            _rtnDropCollectionspace *pDropCSCommand =
                           ( _rtnDropCollectionspace *)pCommand ;
            _pCatAgent->lock_w () ;
            _pCatAgent->clearBySpaceName ( pDropCSCommand->spaceName() ) ;
            _pCatAgent->release_w () ;

            sdbGetClsCB()->invalidateCata( pDropCSCommand->spaceName() ) ;
         }
      }

   done:
      if ( pCommand )
      {
         rtnReleaseCommand( &pCommand ) ;
      }
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__ONQYREQMSG, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONGETMOREREQMSG, "_clsShdSession::_onGetMoreReqMsg" )
   INT32 _clsShdSession::_onGetMoreReqMsg( MsgHeader * msg,
                                           rtnContextBuf &buffObj,
                                           INT32 & startingPos,
                                           INT64 &contextID )
   {
      PD_LOG ( PDDEBUG, "session[%s] _onGetMoreReqMsg", sessionName() ) ;

      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONGETMOREREQMSG ) ;
      INT32 numToRead = 0 ;

      rc = msgExtractGetMore ( (CHAR*)msg, &numToRead, &contextID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Session[%s] extract GETMORE msg failed[rc:%d]",
                  sessionName(), rc ) ;
         goto error ;
      }

      MON_SAVE_OP_DETAIL( _pEDUCB->getMonAppCB(), MSG_BS_GETMORE_REQ,
                          "ContextID:%lld, NumToRead:%d",
                          contextID, numToRead ) ;

      PD_LOG ( PDDEBUG, "GetMore: contextID:%lld\nnumToRead: %d", contextID,
               numToRead ) ;

      rc = rtnGetMore ( contextID, numToRead, buffObj, _pEDUCB, _pRtnCB ) ;

      startingPos = ( INT32 )buffObj.getStartFrom() ;

   done:
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__ONGETMOREREQMSG, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONKILLCTXREQMSG, "_clsShdSession::_onKillContextsReqMsg" )
   INT32 _clsShdSession::_onKillContextsReqMsg ( NET_HANDLE handle,
                                                 MsgHeader * msg )
   {
      PD_LOG ( PDDEBUG, "session[%s] _onKillContextsReqMsg", sessionName() ) ;

      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONKILLCTXREQMSG ) ;
      INT32 contextNum = 0 ;
      INT64 *pContextIDs = NULL ;

      rc = msgExtractKillContexts ( (CHAR*)msg, &contextNum, &pContextIDs ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Session[%s] extract KILLCONTEXT msg failed[rc:%d]",
                  sessionName(), rc ) ;
         goto error ;
      }

      if ( contextNum > 0 )
      {
         PD_LOG ( PDDEBUG, "KillContext: contextNum:%d\ncontextID: %lld",
                  contextNum, pContextIDs[0] ) ;
      }

      rc = rtnKillContexts ( contextNum, pContextIDs, _pEDUCB, _pRtnCB ) ;

   done:
      PD_TRACE_EXITRC ( SDB__CLSSHDSESS__ONKILLCTXREQMSG, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _clsShdSession::_onMsgReq ( NET_HANDLE handle, MsgHeader * msg )
   {
      return rtnMsg( (MsgOpMsg*)msg ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSHDSESS__ONINRPTMSG, "_clsShdSession::_onInterruptMsg" )
   INT32 _clsShdSession::_onInterruptMsg ( NET_HANDLE handle, MsgHeader * msg )
   {
      PD_TRACE_ENTRY ( SDB__CLSSHDSESS__ONINRPTMSG ) ;
      if ( _pEDUCB )
      {
         INT64 contextID = -1 ;
         while ( -1 != ( contextID = _pEDUCB->contextPeek() ) )
         {
            _pRtnCB->contextDelete ( contextID, NULL ) ;
         }

         INT32 rcTmp = rtnTransRollback( _pEDUCB, _pDpsCB );
         if ( rcTmp )
         {
            PD_LOG ( PDERROR, "Failed to rollback(rc=%d)", rcTmp ) ;
         }
      }

      PD_TRACE_EXIT ( SDB__CLSSHDSESS__ONINRPTMSG ) ;
      return SDB_OK ;
   }
   INT32 _clsShdSession::_onTransBeginMsg ()
   {
      if ( !(_pReplSet->primaryIsMe ()) )
      {
         return SDB_CLS_NOT_PRIMARY;
      }
      return rtnTransBegin( _pEDUCB ) ;
   }

   INT32 _clsShdSession::_onTransCommitMsg ()
   {
      if ( !(_pReplSet->primaryIsMe ()) )
      {
         return SDB_CLS_NOT_PRIMARY;
      }
      return rtnTransCommit( _pEDUCB, _pDpsCB );
   }

   INT32 _clsShdSession::_onTransRollbackMsg ()
   {
      if ( !(_pReplSet->primaryIsMe ()) )
      {
         return SDB_CLS_NOT_PRIMARY;
      }
      return rtnTransRollback( _pEDUCB, _pDpsCB ) ;
   }

   INT32 _clsShdSession::_onTransCommitPreMsg( MsgHeader *msg )
   {
      if ( _pEDUCB->getTransID() == DPS_INVALID_TRANS_ID )
      {
         return SDB_DPS_TRANS_NO_TRANS;
      }
      return SDB_OK;
   }

   INT32 _clsShdSession::_onTransUpdateReqMsg ( NET_HANDLE handle,
                                                MsgHeader *msg,
                                                INT64 &updateNum )
   {
      if ( _pEDUCB->getTransID() == DPS_INVALID_TRANS_ID )
      {
         return SDB_DPS_TRANS_NO_TRANS;
      }
      return _onUpdateReqMsg( handle, msg, updateNum );
   }

   INT32 _clsShdSession::_onTransInsertReqMsg ( NET_HANDLE handle,
                                                MsgHeader *msg )
   {
      if ( _pEDUCB->getTransID() == DPS_INVALID_TRANS_ID )
      {
         return SDB_DPS_TRANS_NO_TRANS;
      }
      return _onInsertReqMsg( handle, msg );
   }

   INT32 _clsShdSession::_onTransDeleteReqMsg ( NET_HANDLE handle,
                                                MsgHeader *msg,
                                                INT64 &delNum )
   {
      if ( _pEDUCB->getTransID() == DPS_INVALID_TRANS_ID )
      {
         return SDB_DPS_TRANS_NO_TRANS;
      }
      return _onDeleteReqMsg( handle, msg, delNum );
   }

   INT32 _clsShdSession::_onCheckRouteIDReqMsg ( MsgHeader *msg )
   {
      INT32 rc = SDB_OK;
      MsgCoordCheckRouteID *pMsgReq = (MsgCoordCheckRouteID *)msg;
      MsgRouteID localRouteID = routeAgent()->localID();
      if ( pMsgReq->dstRouteID.columns.nodeID !=
           localRouteID.columns.nodeID ||
           pMsgReq->dstRouteID.columns.groupID !=
           localRouteID.columns.groupID ||
           pMsgReq->dstRouteID.columns.serviceID !=
           localRouteID.columns.serviceID )
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
      return rc ;
   }

   INT32 _clsShdSession::_onTransStopEvnt( pmdEDUEvent *event )
   {
      INT32 rcTmp = rtnTransRollback( _pEDUCB, _pDpsCB ) ;
      if ( rcTmp )
      {
         PD_LOG ( PDERROR, "Failed to rollback(rc=%d)", rcTmp ) ;
      }
      return SDB_OK ;
   }

   INT32 _clsShdSession::_InsertToMainCL( BSONObj &objs, INT32 objNum,
                                          INT32 flags, INT16 w )
   {
      INT32 rc = SDB_OK;
      ossValuePtr pCurPos = 0;
      INT32 totalObjsNum = 0;
      try
      {
         PD_CHECK( !objs.isEmpty(), SDB_INVALIDARG, error, PDERROR,
                  "Insert record can't be empty" );
         pCurPos = (ossValuePtr)objs.objdata();
         while ( totalObjsNum < objNum )
         {
            BSONObj subObjsInfo( (const CHAR *)pCurPos );
            INT32 subObjsNum = 0;
            UINT32 subObjsSize = 0;
            const CHAR *pSubCLName = NULL;
            BSONElement beSubObjsNum;
            BSONElement beSubObjsSize;
            BSONElement beSubCLName;
            BSONObj insertor;
            beSubObjsNum = subObjsInfo.getField( FIELD_NAME_SUBOBJSNUM );
            PD_CHECK( beSubObjsNum.isNumber(), SDB_INVALIDARG, error, PDERROR,
                      "Failed to get the field(%s)", FIELD_NAME_SUBOBJSNUM );
            subObjsNum = beSubObjsNum.numberInt();

            beSubObjsSize = subObjsInfo.getField( FIELD_NAME_SUBOBJSSIZE );
            PD_CHECK( beSubObjsSize.isNumber(), SDB_INVALIDARG, error, PDERROR,
                      "Failed to get the field(%s)", FIELD_NAME_SUBOBJSSIZE );
            subObjsSize = beSubObjsSize.numberInt();

            beSubCLName = subObjsInfo.getField( FIELD_NAME_SUBCLNAME );
            PD_CHECK( beSubCLName.type() == String, SDB_INVALIDARG, error,
                      PDERROR, "Failed to get the field(%s)",
                      FIELD_NAME_SUBCLNAME );
            pSubCLName = beSubCLName.valuestr();

            pCurPos += ossAlignX( (ossValuePtr)subObjsInfo.objsize(), 4 );
            ++totalObjsNum;
            insertor = BSONObj( (CHAR *)pCurPos );
            rc = rtnInsert ( pSubCLName, insertor, subObjsNum, flags, 
                             _pEDUCB, _pDmsCB, _pDpsCB, w ) ;
            if ( SDB_DMS_NOTEXIST == rc )
            {
               rc = _pShdMgr->syncUpdateCatalog( pSubCLName ) ;
               PD_RC_CHECK( rc, PDERROR,
                           "failed to insert on sub-collection catalog(%s)",
                           pSubCLName ) ;

               rc = _createCLByCatalog( pSubCLName ) ;
               PD_RC_CHECK( rc, PDERROR,
                           "failed to create sub-collection(%s)",
                           pSubCLName ) ;
               continue ;
            }
            PD_RC_CHECK( rc, PDERROR, "Failed to insert to sub-collection(%s), "
                         "rc: %d", pSubCLName, rc ) ;
            pCurPos += subObjsSize;
            totalObjsNum += subObjsNum ;
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG;
         goto error ;
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_includeShardingOrder( const CHAR *pCollectionName,
                                                const BSONObj &orderBy,
                                                BOOLEAN &result )
   {
      INT32 rc = SDB_OK;
      BSONObj shardingKey;
      _clsCatalogSet *pCataSet = NULL;
      BOOLEAN catLocked = FALSE;
      result = FALSE;
      BOOLEAN isRange = FALSE;
      try
      {
         if ( orderBy.isEmpty() )
         {
            goto done;
         }
         _pCatAgent->lock_r () ;
         catLocked = TRUE;
         pCataSet = _pCatAgent->collectionSet( pCollectionName );
         if ( NULL == pCataSet )
         {
            _pCatAgent->release_r () ;
            catLocked = FALSE;
            rc = SDB_DMS_NOTEXIST;
            PD_LOG( PDERROR, "can not find collection:%s", pCollectionName );
            goto error;
         }
         isRange = pCataSet->isRangeSharding();
         shardingKey = pCataSet->getShardingKey().copy();
         _pCatAgent->release_r () ;
         catLocked = FALSE;
         if ( !isRange )
         {
            goto done;
         }
         if ( !shardingKey.isEmpty() )
         {
            result = TRUE;
            BSONObjIterator iterOrder( orderBy );
            BSONObjIterator iterSharding( shardingKey );
            while( iterOrder.more() && iterSharding.more() )
            {
               BSONElement beOrder = iterOrder.next();
               BSONElement beSharding = iterSharding.next();
               if ( 0 != beOrder.woCompare( beSharding ) )
               {
                  result = FALSE;
                  break;
               }
            }
         }
      }
      catch( std::exception &e )
      {
         PD_RC_CHECK( SDB_SYS, PDERROR,
                     "occur unexpected error:%s",
                     e.what() );
      }
   done:
      if ( catLocked )
      {
         _pCatAgent->release_r () ;
      }
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_queryToMainCL( const CHAR *pCollectionName,
                                         const BSONObj &selector,
                                         const BSONObj &matcher,
                                         const BSONObj &orderBy,
                                         const BSONObj &hint,
                                         SINT32 flags,
                                         pmdEDUCB *cb,
                                         SINT64 numToSkip,
                                         SINT64 numToReturn,
                                         SINT64 &contextID,
                                         _rtnContextBase **ppContext )
   {
      INT32 rc = SDB_OK;
      std::vector< std::string > strSubCLList;
      BSONObj boNewMatcher;
      rtnContextMainCL *pContextMainCL = NULL;
      BOOLEAN includeShardingOrder = FALSE;
      SINT64 tmpContextID = -1 ;
      INT64 subNumToReturn = numToReturn ;

      SDB_ASSERT( pCollectionName, "collection name can't be NULL!" ) ;
      SDB_ASSERT( cb, "educb can't be NULL!" );

      if ( numToReturn > 0 && numToSkip > 0 )
      {
         subNumToReturn = numToReturn + numToSkip ;
      }

      rc = _includeShardingOrder( pCollectionName, orderBy,
                                  includeShardingOrder );
      PD_RC_CHECK( rc, PDERROR,
                   "Failed to check order-key(rc=%d)", rc );

      rc = _getSubCLList( matcher, pCollectionName,
                          boNewMatcher, strSubCLList );
      if ( rc != SDB_OK )
      {
         goto error;
      }

      rc = _pRtnCB->contextNew( RTN_CONTEXT_MAINCL,
                                (rtnContext **)&pContextMainCL,
                                tmpContextID, cb ) ;
      PD_RC_CHECK( rc, PDERROR,
                   "Failed to create new main-collection context(rc=%d)",
                   rc );

      rc = pContextMainCL->open( orderBy, numToReturn, numToSkip,
                                 includeShardingOrder );
      PD_RC_CHECK( rc, PDERROR,
                   "Open main-collection context failed(rc=%d)",
                   rc );

      {
      std::vector< std::string >::iterator iterSubCLSet = strSubCLList.begin();
      while( iterSubCLSet != strSubCLList.end() )
      {
         SINT64 subContextID = -1;
         rc = rtnQuery( (*iterSubCLSet).c_str(), selector, boNewMatcher,
                        orderBy, hint, flags, cb, 0, subNumToReturn, _pDmsCB,
                        _pRtnCB, subContextID ) ;
         PD_RC_CHECK( rc, PDERROR,
                      "Query sub-collection(%s) failed!(rc=%d)",
                      iterSubCLSet->c_str(), rc );
         pContextMainCL->addSubContext( subContextID );
         ++iterSubCLSet;
      }
      }

      if ( FLG_QUERY_EXPLAIN & flags )
      {
         rc = _aggregateMainCLExplaining( pCollectionName, cb,
                                          tmpContextID,
                                          contextID ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to aggregate sub cl info:%d", rc ) ;
            goto error ;
         }
      }
      else
      {
         contextID = tmpContextID ;
         tmpContextID = -1 ;

         if ( ppContext )
         {
            *ppContext = pContextMainCL ;
         }
      }
   done:
      return rc;
   error:
      if ( -1 != contextID )
      {
         _pRtnCB->contextDelete( contextID, cb );
         contextID = -1;
      }
      if ( -1 != tmpContextID )
      {
         _pRtnCB->contextDelete( tmpContextID, cb );
         tmpContextID = -1;
      }
      goto done;
   }

   INT32 _clsShdSession::_getSubCLList( const BSONObj &matcher,
                                        const CHAR *pCollectionName,
                                        BSONObj &boNewMatcher,
                                        std::vector< std::string > &strSubCLList )
   {
      INT32 rc = SDB_OK;

      try
      {
         BSONObjBuilder bobNewMatcher;
         BSONObjIterator iter( matcher );
         while( iter.more() )
         {
            BSONElement beTmp = iter.next();
            if ( beTmp.type() == Array &&
                 0 == ossStrcmp(beTmp.fieldName(), CAT_SUBCL_NAME ) )
            {
               BSONObj boSubCLList = beTmp.embeddedObject();
               BSONObjIterator iterSubCL( boSubCLList );
               while( iterSubCL.more() )
               {
                  BSONElement beSubCL = iterSubCL.next();
                  std::string strSubCLName = beSubCL.str();
                  if ( !strSubCLName.empty() )
                  {
                     strSubCLList.push_back( strSubCLName );
                  }
               }
            }
            else
            {
               bobNewMatcher.append( beTmp );
            }
         }
         boNewMatcher = bobNewMatcher.obj();
      }
      catch( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                      "occur unexpected error:%s",
                      e.what() );
      }
      if ( strSubCLList.empty() )
      {
         std::vector< std::string > strSubCLListTmp;
         _clsCatalogSet *pCataSet = NULL;
         std::vector< std::string >::iterator iter;
         _pCatAgent->lock_r () ;
         pCataSet = _pCatAgent->collectionSet( pCollectionName );
         if ( NULL == pCataSet )
         {
            _pCatAgent->release_r () ;
            rc = SDB_DMS_NOTEXIST;
            PD_LOG( PDERROR, "can not find collection:%s", pCollectionName );
            goto error;
         }
         pCataSet->getSubCLList( strSubCLListTmp );
         iter = strSubCLListTmp.begin();
         while( iter != strSubCLListTmp.end() )
         {
            _clsCatalogSet *pSubSet = NULL;
            pSubSet = _pCatAgent->collectionSet( iter->c_str() );
            if ( NULL == pSubSet || 0 == pSubSet->groupCount() )
            {
               ++iter;
               continue;
            }
            strSubCLList.push_back( *iter );
            ++iter;
         }
         _pCatAgent->release_r () ;
      }

      PD_CHECK( !strSubCLList.empty(), SDB_INVALID_MAIN_CL, error, PDERROR,
                "main-collection has no sub-collection!" );
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_updateToMainCL( const CHAR *pCollectionName,
                                          const BSONObj &selector,
                                          const BSONObj &updator,
                                          const BSONObj &hint,
                                          SINT32 flags,
                                          pmdEDUCB *cb,
                                          SDB_DMSCB *pDmsCB,
                                          SDB_DPSCB *pDpsCB,
                                          INT16 w,
                                          INT64 *pUpdateNum )
   {
      INT32 rc = SDB_OK;
      BSONObj boNewSelector;
      std::vector< std::string > strSubCLList;
      INT64 updateNum = 0;
      rc = _getSubCLList( selector, pCollectionName,
                        boNewSelector, strSubCLList );
      if ( rc != SDB_OK )
      {
         goto error;
      }
      {
      std::vector< std::string >::iterator iterSubCLSet
                                       = strSubCLList.begin();
      while( iterSubCLSet != strSubCLList.end() )
      {
         INT64 numTmp = 0;
         rc = rtnUpdate( (*iterSubCLSet).c_str(), boNewSelector, updator,
                        hint, flags, cb, pDmsCB, pDpsCB, w, &numTmp );
         if ( SDB_DMS_NOTEXIST == rc )
         {
            rc = _pShdMgr->syncUpdateCatalog( (*iterSubCLSet).c_str() ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to update on sub-collection catalog(%s)",
                        (*iterSubCLSet).c_str() ) ;
            rc = _createCLByCatalog( (*iterSubCLSet).c_str() ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to create sub-collection(%s)",
                        (*iterSubCLSet).c_str() ) ;
            continue ;
         }
         PD_RC_CHECK( rc, PDERROR,
                     "update on sub-collection(%s) failed(rc=%d)",
                     (*iterSubCLSet).c_str(), rc );
         updateNum += numTmp;
         ++iterSubCLSet;
      }
      }
   done:
      if ( pUpdateNum )
      {
         *pUpdateNum = updateNum;
      }
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_deleteToMainCL ( const CHAR *pCollectionName,
                                           const BSONObj &deletor,
                                           const BSONObj &hint, INT32 flags,
                                           pmdEDUCB *cb,
                                           SDB_DMSCB *dmsCB, SDB_DPSCB *dpsCB,
                                           INT16 w, INT64 *pDelNum )
   {
      INT32 rc = SDB_OK;
      BSONObj boNewDeletor;
      std::vector< std::string > strSubCLList;
      INT64 delNum = 0;
      rc = _getSubCLList( deletor, pCollectionName,
                        boNewDeletor, strSubCLList );
      if ( rc != SDB_OK )
      {
         goto error;
      }
      {
      std::vector< std::string >::iterator iterSubCLSet
                                       = strSubCLList.begin();
      while( iterSubCLSet != strSubCLList.end() )
      {
         INT64 numTmp = 0;
         rc = rtnDelete( (*iterSubCLSet).c_str(), boNewDeletor, hint,
                        flags, cb, dmsCB, dpsCB, w, &numTmp );
         if ( SDB_DMS_NOTEXIST == rc )
         {
            rc = _pShdMgr->syncUpdateCatalog( (*iterSubCLSet).c_str() ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to delete on sub-collection catalog(%s)",
                        (*iterSubCLSet).c_str() ) ;
            rc = _createCLByCatalog( (*iterSubCLSet).c_str() ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to create sub-collection(%s)",
                        (*iterSubCLSet).c_str() ) ;
            continue ;
         }
         PD_RC_CHECK( rc, PDERROR,
                     "delete on sub-collection(%s) failed(rc=%d)",
                     (*iterSubCLSet).c_str(), rc );
         delNum += numTmp;
         ++iterSubCLSet;
      }
      }
   done:
      if ( pDelNum )
      {
         *pDelNum = delNum;
      }
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_runOnMainCL( const CHAR *pCommandName,
                                       _rtnCommand *pCommand,
                                       INT32 flags,
                                       INT64 numToSkip,
                                       INT64 numToReturn,
                                       const CHAR *pQuery,
                                       const CHAR *pField,
                                       const CHAR *pOrderBy,
                                       const CHAR *pHint,
                                       INT16 w,
                                       SINT64 &contextID )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT( pCommandName && pCommand, "pCommand can't be null!" );
      switch( pCommand->type() )
      {
      case CMD_GET_COUNT:
      case CMD_GET_INDEXES:
         rc = _getOnMainCL( pCommandName, pCommand->collectionFullName(),
                            flags, numToSkip, numToReturn, pQuery, pField,
                            pOrderBy, pHint, w, contextID );
         break;

      case CMD_CREATE_INDEX:
         rc = _createIndexOnMainCL( pCommandName,
                                    pCommand->collectionFullName(),
                                    pQuery, w, contextID );
         break;

      case CMD_DROP_INDEX:
         rc = _dropIndexOnMainCL( pCommandName, pCommand->collectionFullName(),
                                  pQuery, w, contextID );
         break;

      case CMD_LINK_COLLECTION:
      case CMD_UNLINK_COLLECTION:
         rc = rtnRunCommand( pCommand, CMD_SPACE_SERVICE_SHARD,
                             _pEDUCB, _pDmsCB, _pRtnCB,
                             _pDpsCB, w, &contextID ) ;
         break;

      case CMD_DROP_COLLECTION:
         rc = _dropMainCL( pCommand->collectionFullName(), w, contextID );
         break;

      default:
         rc = SDB_MAIN_CL_OP_ERR;
         break;
      }
      PD_RC_CHECK( rc, PDERROR,
                   "failed to run command on main-collection(rc=%d)",
                   rc );
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_getOnMainCL( const CHAR *pCommand,
                                       const CHAR *pCollection,
                                       INT32 flags,
                                       INT64 numToSkip,
                                       INT64 numToReturn,
                                       const CHAR *pQuery,
                                       const CHAR *pField,
                                       const CHAR *pOrderBy,
                                       const CHAR *pHint,
                                       INT16 w,
                                       SINT64 &contextID )
   {
      INT32 rc = SDB_OK;
      std::vector< std::string > strSubCLList;
      BSONObj boNewMatcher;
      rtnContextMainCL *pContextMainCL = NULL;
      BSONObj boMatcher;
      BSONObj boEmpty;
      BSONObj boHint;
      _rtnCommand *pCommandTmp = NULL;
      INT64 subNumToReturn = -1;
      SDB_ASSERT( pCommand, "pCommand can't be null!" );
      SDB_ASSERT( pCollection,
                  "collection name can't be null!"  );
      if ( numToSkip > 0 && numToReturn > 0 )
      {
         subNumToReturn = numToReturn + numToSkip;
      }
      else
      {
         subNumToReturn = numToReturn;
      }
      try
      {
         boMatcher = BSONObj( pQuery );
         BSONObj boHintTmp = BSONObj( pHint );
         BSONObjBuilder bobHint;
         BSONObjIterator iter( boHintTmp );
         while( iter.more() )
         {
            BSONElement beTmp = iter.next();
            if ( 0 != ossStrcmp( beTmp.fieldName(), FIELD_NAME_COLLECTION ))
            {
               bobHint.append( beTmp );
            }
         }
         boHint = bobHint.obj();
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Session[%s] Failed to create matcher: %s",
            sessionName(), e.what () ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = _getSubCLList( boMatcher, pCollection, boNewMatcher,
                        strSubCLList );
      PD_RC_CHECK( rc, PDERROR, "failed to get sub-collection list(rc=%d)",
                   rc );
      rc = _pRtnCB->contextNew( RTN_CONTEXT_MAINCL,
                                (rtnContext **)&pContextMainCL,
                                contextID, _pEDUCB );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to create new main-collection context(rc=%d)",
                  rc );
      rc = pContextMainCL->open( boEmpty, numToReturn, numToSkip );
      PD_RC_CHECK( rc, PDERROR, "open main-collection context failed(rc=%d)",
                   rc );
      {
      std::vector< std::string >::iterator iterSubCLSet
                                             = strSubCLList.begin();
      while( iterSubCLSet != strSubCLList.end() )
      {
         SINT64 subContextID = -1;
         BSONObj boSubHint;
         try
         {
            BSONObjBuilder bobSubHint;
            bobSubHint.appendElements( boHint );
            bobSubHint.append( FIELD_NAME_COLLECTION, *iterSubCLSet );
            boSubHint = bobSubHint.obj();
         }
         catch( std::exception &e )
         {
            PD_LOG ( PDERROR, "Session[%s] Failed to create hint: %s",
                     sessionName(), e.what () ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         rc = rtnParserCommand( pCommand, &pCommandTmp );
         PD_RC_CHECK( rc, PDERROR,
                      "parse command[%s] failed[rc=%d]",
                      pCommand, rc );
         rc = rtnInitCommand( pCommandTmp, flags, 0, subNumToReturn,
                              boNewMatcher.objdata(), pField, pOrderBy,
                              boSubHint.objdata() );
         PD_RC_CHECK( rc, PDERROR,
                      "failed to init command(rc=%d)", rc );

         rc = rtnRunCommand( pCommandTmp, CMD_SPACE_SERVICE_SHARD, _pEDUCB,
                             _pDmsCB, _pRtnCB, _pDpsCB, w, &subContextID );

         PD_RC_CHECK( rc, PDERROR,
                      "run on sub-collection(%s) failed(rc=%d)",
                      iterSubCLSet->c_str(), rc );
         pContextMainCL->addSubContext( subContextID );
         if ( pCommandTmp )
         {
            rtnReleaseCommand( &pCommandTmp );
            pCommandTmp = NULL;
         }
         ++iterSubCLSet;
      }
      }
   done:
      if ( pCommandTmp )
      {
         rtnReleaseCommand( &pCommandTmp );
      }
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_createIndexOnMainCL( const CHAR *pCommand,
                                               const CHAR *pCollection,
                                               const CHAR *pQuery,
                                               INT16 w,
                                               SINT64 &contextID )
   {
      INT32 rc = SDB_OK;
      BSONObj boMatcher;
      BSONObj boNewMatcher;
      BSONObj boIndex;
      std::vector< std::string > strSubCLList;
      std::vector< std::string >::iterator iter;
      try
      {
         boMatcher = BSONObj( pQuery );
         rc = rtnGetObjElement( boMatcher, FIELD_NAME_INDEX,
                              boIndex );
         PD_RC_CHECK( rc, PDERROR,
                      "failed to get object index(rc=%d)", rc );
      }
      catch( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                      "occur unexpected error(%s)",
                      e.what() );
      }
      rc = _getSubCLList( boMatcher, pCollection, boNewMatcher,
                          strSubCLList );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to get sub-collection list(rc=%d)", rc );
      iter = strSubCLList.begin();
      while( iter != strSubCLList.end() )
      {
         INT32 rcTmp = SDB_OK;
         rcTmp = rtnCreateIndexCommand( iter->c_str(), boIndex, _pEDUCB,
                                     _pDmsCB, _pDpsCB );
         if ( SDB_DMS_NOTEXIST == rc )
         {
            rc = _pShdMgr->syncUpdateCatalog( (*iter).c_str() ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to create index on sub-collection catalog(%s)",
                        (*iter).c_str() ) ;
            rc = _createCLByCatalog( (*iter).c_str() ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to create sub-collection(%s)",
                        (*iter).c_str() ) ;
            continue ;
         }
         if ( SDB_OK != rcTmp && SDB_IXM_REDEF != rcTmp )
         {
            PD_LOG( PDERROR,
                   "create index for sub-collection(%s) failed(rc=%d)",
                   iter->c_str(), rcTmp );
            if ( SDB_OK == rc )
            {
               rc = rcTmp;
            }
         }
         ++iter;
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_dropIndexOnMainCL( const CHAR *pCommand,
                                             const CHAR *pCollection,
                                             const CHAR *pQuery,
                                             INT16 w,
                                             SINT64 &contextID )
   {
      INT32 rc = SDB_OK;
      BSONObj boMatcher;
      BSONObj boNewMatcher;
      BSONObj boIndex;
      std::vector< std::string > strSubCLList;
      std::vector< std::string >::iterator iter;
      BSONElement ele;
      BOOLEAN isExist = FALSE;
      try
      {
         boMatcher = BSONObj( pQuery );
         rc = rtnGetObjElement( boMatcher, FIELD_NAME_INDEX,
                              boIndex );
         PD_RC_CHECK( rc, PDERROR,
                      "failed to get object index(rc=%d)", rc );
         ele = boIndex.firstElement();
      }
      catch( std::exception &e )
      {
         PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                      "occur unexpected error(%s)",
                      e.what() );
      }
      rc = _getSubCLList( boMatcher, pCollection, boNewMatcher,
                        strSubCLList );
      PD_RC_CHECK( rc, PDERROR,
                   "failed to get sub-collection list(rc=%d)", rc );
      iter = strSubCLList.begin();
      while( iter != strSubCLList.end() )
      {
         INT32 rcTmp = SDB_OK;
         rcTmp = rtnDropIndexCommand( iter->c_str(), ele, _pEDUCB,
                                    _pDmsCB, _pDpsCB );
         if ( SDB_DMS_NOTEXIST == rc )
         {
            rc = _pShdMgr->syncUpdateCatalog( (*iter).c_str() ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to drop index sub-collection catalog(%s)",
                        (*iter).c_str() ) ;
            rc = _createCLByCatalog( (*iter).c_str() ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to create sub-collection(%s)",
                        (*iter).c_str() ) ;
            continue ;
         }
         if ( SDB_OK == rcTmp )
         {
            isExist = TRUE;
         }
         else
         {
            if ( SDB_OK == rc || SDB_IXM_NOTEXIST == rc )
            {
               rc = rcTmp;
            }
            PD_LOG( PDERROR,
                   "drop index for sub-collection(%s) failed(rc=%d)",
                   iter->c_str(), rcTmp );
         }
         ++iter;
      }
      if ( SDB_IXM_NOTEXIST == rc && isExist )
      {
         rc = SDB_OK;
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_dropMainCL( const CHAR *pCollection,
                                      INT16 w,
                                      SINT64 &contextID )
   {
      INT32 rc = SDB_OK;
      contextID = -1;
      rtnContextDelMainCL *delContext = NULL;
      rc = _pRtnCB->contextNew( RTN_CONTEXT_DELMAINCL,
                                (rtnContext **)&delContext,
                                contextID, _pEDUCB );
      PD_RC_CHECK( rc, PDERROR, "Failed to create context, drop "
                   "main collection[%s] failed, rc: %d", pCollection,
                   rc ) ;
      rc = delContext->open( pCollection, _pEDUCB );
      PD_RC_CHECK( rc, PDERROR, "Failed to open context, drop "
                   "main collection[%s] failed, rc: %d", pCollection,
                   rc );
   done:
      return rc;
   error:
      goto done;
   }

   INT32 _clsShdSession::_onCatalogChangeNtyMsg( MsgHeader * msg )
   {
      _pShdMgr->updateCatGroup( FALSE ) ;
      return SDB_OK ;
   }

   INT32 _clsShdSession::_aggregateMainCLExplaining( const CHAR *fullName,
                                                     pmdEDUCB *cb,
                                                     SINT64 &mainCLContextID,
                                                     SINT64 &contextID )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      BSONArrayBuilder arrBuilder ;
      BSONObj obj ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      _rtnContextDump *context = NULL ;
      BOOLEAN extractNode = FALSE ;

      rc = rtnCB->contextNew ( RTN_CONTEXT_DUMP,
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

      builder.append( FIELD_NAME_NAME, fullName ) ;
      {
      rtnDataSet dataSet( mainCLContextID, cb ) ;
      while ( TRUE )
      {
         BSONObjBuilder tmp ;
         BSONElement ele ;
         rc = dataSet.next( obj ) ;
         if ( SDB_OK != rc )
         {
            break ;
         }

         if ( !extractNode )
         {
            ele = obj.getField( FIELD_NAME_NODE_NAME ) ;
            if ( String != ele.type() )
            {
               PD_LOG( PDERROR, "invalid result of explaining:%s",
                       obj.toString( FALSE, TRUE ).c_str() ) ;
               rc = SDB_SYS ;
               goto error ;
            }
            builder.append( ele ) ;
         }

         ele = obj.getField( FIELD_NAME_NAME ) ;
         if ( String != ele.type() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         ele = obj.getField( FIELD_NAME_USE_EXT_SORT ) ;
         if ( Bool != ele.type() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;
         
         ele = obj.getField( FIELD_NAME_SCANTYPE ) ;
         if ( String != ele.type() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         ele = obj.getField( FIELD_NAME_INDEXNAME ) ;
         if ( String != ele.type() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         ele = obj.getField( FIELD_NAME_RETURN_NUM ) ;
         if ( !ele.isNumber() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         ele = obj.getField( FIELD_NAME_ELAPSED_TIME ) ;
         if ( !ele.isNumber() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         ele = obj.getField( FIELD_NAME_INDEXREAD ) ;
         if ( !ele.isNumber() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         ele = obj.getField( FIELD_NAME_DATAREAD ) ;
         if ( !ele.isNumber() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         ele = obj.getField( FIELD_NAME_USERCPU ) ;
         if ( !ele.isNumber() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         ele = obj.getField( FIELD_NAME_SYSCPU ) ;
         if ( !ele.isNumber() )
         {
            PD_LOG( PDERROR, "invalid result of explaining:%s",
                    obj.toString( FALSE, TRUE ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         tmp.append( ele ) ;

         arrBuilder << tmp.obj() ;
      }

      if ( SDB_DMS_EOC != rc )
      {
         PD_LOG( PDERROR, "failed to get the next obj:%d", rc ) ;
         goto error ;
      }
      mainCLContextID = -1 ;

      builder.append( FIELD_NAME_SUB_COLLECTIONS, arrBuilder.arr() ) ;
      }

      rc = context->monAppend( builder.obj() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to append obj to context:%d", rc ) ;
         goto error ;
      }

      
   done:
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   INT32 _clsShdSession::_onOpenLobReq( MsgHeader *msg,
                                        SINT64 &contextID,
                                        rtnContextBuf &buffObj )
   {
      INT32 rc = SDB_OK ;
      const MsgOpLob *header = NULL ;
      BSONObj lob ;
      BSONObj meta ;
      BSONElement fullName ;
      BSONElement mode ;
      INT16 w = 0 ;
      _rtnContextShdOfLob *context = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      BOOLEAN isMainCL = FALSE ; 

      rc = msgExtractOpenLobRequest( ( const CHAR * )msg, &header, lob ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract open msg:%d", rc ) ;
         goto error ;
      }
      fullName = lob.getField( FIELD_NAME_COLLECTION ) ;
      if ( String != fullName.type() )
      {
         PD_LOG( PDERROR, "invalid lob obj:%s",
                 lob.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _pCollectionName = fullName.valuestr() ;

      mode = lob.getField( FIELD_NAME_LOB_OPEN_MODE ) ;
      if ( NumberInt != mode.type() )
      {
         PD_LOG( PDERROR, "invalid lob obj:%s",
                 lob.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      w = header->w ;

      if ( SDB_LOB_MODE_R != mode.Int() )
      {
         rc = _check( w ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }

      rc = _checkCata( header->version, fullName.valuestr(),
                       w, isMainCL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to check catainfo:%d", rc ) ;
         goto error ;
      }

      rc = rtnCB->contextNew( RTN_CONTEXT_SHARD_OF_LOB,
                              (rtnContext**)(&context),
                              contextID, _pEDUCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open context:%d", rc ) ;
         goto error ;
      }

      rc = context->open( lob, header->version, w,
                          _pDpsCB, _pEDUCB, meta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob context:%d", rc ) ;
         goto error ;
      }

      if ( !meta.isEmpty() )
      {
         buffObj = rtnContextBuf( meta.objdata(),
                                  meta.objsize(),
                                  1 ) ;
      }
   done:
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete( contextID, _pEDUCB ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   INT32 _clsShdSession::_onWriteLobReq( MsgHeader *msg )
   {
      INT32 rc = SDB_OK ;
      const MsgOpLob *header = NULL ;
      BSONObj obj ;
      const MsgLobTuple *tuple = NULL ;
      UINT32 tSize = 0 ;
      const MsgLobTuple *curTuple = NULL ;
      UINT32 tupleNum = 0 ;
      const CHAR *data = NULL ;
      rtnContext *context = NULL ;
      rtnContextShdOfLob *lobContext = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      INT16 w = 0 ;
      BOOLEAN isMainCl = FALSE ;

      rc = msgExtractLobRequest( ( const CHAR * )msg,
                                 &header, obj,
                                 &tuple, &tSize ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract write msg:%d", rc ) ;
         goto error ;
      }

      context = (rtnCB->contextFind ( header->contextID )) ;
      if ( NULL == context )
      {
         PD_LOG ( PDERROR, "context %lld does not exist", header->contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( RTN_CONTEXT_SHARD_OF_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "invalid type of context:%d", context->getType() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      lobContext = ( rtnContextShdOfLob * )context ;
      _pCollectionName = lobContext->getFullName() ;
      w = lobContext->getW() ;
      rc = _check( w ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      _pEDUCB->writingDB( TRUE ) ;  // it call must before _checkCata

      rc = _checkCata( header->version, lobContext->getFullName(),
                       w, isMainCl, FALSE ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to check catainfo:%d", rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         BOOLEAN got = FALSE ;
         rc = msgExtractTuplesAndData( &tuple, &tSize,
                                       &curTuple, &data,
                                       &got ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to extract next tuple:%d", rc ) ;
            goto error ;
         }

         if ( !got )
         {
            break ;
         }

         rc = lobContext->write( curTuple->columns.sequence,
                                 curTuple->columns.offset,
                                 curTuple->columns.len,
                                 data, _pEDUCB ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
            goto error ;
         }

         ++tupleNum ;
         if ( 0 == tupleNum % SHD_INTERRUPT_CHECKPOINT &&
              _pEDUCB->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }
      }

      PD_LOG( PDDEBUG, "%d pieces of lob[%s] write done",
              tupleNum, lobContext->getOID().str().c_str() ) ;
   done:
      return rc ;
   error:
      if ( NULL != context &&
           SDB_CLS_COORD_NODE_CAT_VER_OLD != rc &&
           SDB_CLS_DATA_NODE_CAT_VER_OLD != rc )
      {
         rtnCB->contextDelete( context->contextID(), _pEDUCB ) ;
      }
      goto done ;
   }

   INT32 _clsShdSession::_onCloseLobReq( MsgHeader *msg )
   {
      INT32 rc = SDB_OK ;
      const MsgOpLob *header = NULL ;
      rtnContextShdOfLob *lobContext = NULL ;
      rtnContext *context = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;

      rc = msgExtractCloseLobRequest( ( const CHAR * )msg, &header ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract close msg:%d", rc ) ;
         goto error ;
      }

      context = rtnCB->contextFind ( header->contextID ) ;
      if ( NULL == context )
      {
         PD_LOG ( PDERROR, "context %lld does not exist",
                  header->contextID ) ;
         goto done ;
      }

      if ( RTN_CONTEXT_SHARD_OF_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "invalid context type:%d", context->getType() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      lobContext = ( rtnContextShdOfLob * )context ;
      rc = lobContext->close( _pEDUCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to close lob:%d", rc ) ;
         goto error ;
      }

   done:
      if ( NULL != context ) 
      {
         rtnCB->contextDelete ( context->contextID(), _pEDUCB ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _clsShdSession::_onReadLobReq( MsgHeader *msg,
                                        rtnContextBuf &buffObj )
   {
      INT32 rc = SDB_OK ;
      const MsgOpLob *header = NULL ;
      rtnContextShdOfLob *lobContext = NULL ;
      rtnContext *context = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      const MsgLobTuple *tuple = NULL ;
      UINT32 tuplesSize = 0 ;
      bson::BSONObj meta ;
      INT16 w = 0 ;
      BOOLEAN isMainCl = FALSE ;
      const CHAR *data = NULL ;
      UINT32 read = 0 ;

      rc = msgExtractLobRequest( ( const CHAR * )msg,
                                 &header, meta, &tuple, &tuplesSize ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract read msg:%d", rc ) ;
         goto error ;
      }

      context = rtnCB->contextFind ( header->contextID ) ;
      if ( NULL == context )
      {
         PD_LOG ( PDERROR, "context %lld does not exist",
                  header->contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( RTN_CONTEXT_SHARD_OF_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "invalid context type:%d", context->getType() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      lobContext = ( rtnContextShdOfLob * )context ;
      _pCollectionName = lobContext->getFullName() ;
      w = lobContext->getW() ;
      rc = _checkCata( header->version, lobContext->getFullName(),
                       w, isMainCl, FALSE ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to check catainfo:%d", rc ) ;
         goto error ;
      }

      rc = lobContext->readv( tuple, tuplesSize / sizeof( MsgLobTuple ),
                              _pEDUCB, &data, read ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read lob:%d", rc ) ;
         goto error ;
      }

      buffObj = rtnContextBuf( data, read, 0 ) ;
   done:
      return rc ;
   error:
      if ( NULL != context &&
           SDB_CLS_COORD_NODE_CAT_VER_OLD != rc &&
           SDB_CLS_DATA_NODE_CAT_VER_OLD != rc  )
      {
         rtnCB->contextDelete ( context->contextID(), _pEDUCB ) ;
      }
      goto done ;
   }

   INT32 _clsShdSession::_onRemoveLobReq( MsgHeader *msg )
   {
      INT32 rc = SDB_OK ;
      const MsgOpLob *header = NULL ;
      rtnContextShdOfLob *lobContext = NULL ;
      rtnContext *context = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      const MsgLobTuple *begin = NULL ;
      UINT32 tuplesSize = 0 ;
      BSONObj obj ;
      BOOLEAN isMainCl = FALSE ;
      INT16 w = 0 ;
      UINT32 tupleNum = 0 ;

      rc = msgExtractLobRequest( ( const CHAR * )msg, &header,
                                 obj, &begin, &tuplesSize ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract close msg:%d", rc ) ;
         goto error ;
      }

      context = rtnCB->contextFind ( header->contextID ) ;
      if ( NULL == context )
      {
         PD_LOG ( PDERROR, "context %lld does not exist",
                  header->contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( RTN_CONTEXT_SHARD_OF_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "invalid context type:%d", context->getType() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      lobContext = ( rtnContextShdOfLob * )context ;
      _pCollectionName = lobContext->getFullName() ;
      w = lobContext->getW() ;
      rc = _check( w ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      _pEDUCB->writingDB( TRUE ) ;  // it call must before _checkCata
      rc = _checkCata( header->version, lobContext->getFullName(),
                       w, isMainCl, FALSE ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to check catainfo:%d", rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         BOOLEAN got = FALSE ;
         const MsgLobTuple *curTuple = NULL ;
         rc = msgExtractTuples( &begin, &tuplesSize,
                                &curTuple, &got ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to extract next tuple:%d", rc ) ;
            goto error ;
         }

         if ( !got )
         {
            break ;
         }

         rc = lobContext->remove( curTuple->columns.sequence,
                                  _pEDUCB ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to remove lob:%d", rc ) ;
            goto error ;
         }

         if ( 0 == ++tupleNum % SHD_INTERRUPT_CHECKPOINT &&
              _pEDUCB->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }
      }

      PD_LOG( PDDEBUG, "%d pieces of lob[%s] remove done",
              tupleNum, lobContext->getOID().str().c_str() ) ;
   done:
      return rc ;
   error:
      if ( NULL != context &&
           SDB_CLS_COORD_NODE_CAT_VER_OLD != rc &&
           SDB_CLS_DATA_NODE_CAT_VER_OLD != rc  )
      {
         rtnCB->contextDelete ( context->contextID(), _pEDUCB ) ;
      }
      goto done ;
   }

   INT32 _clsShdSession::_onUpdateLobReq( MsgHeader *msg )
   {
      INT32 rc = SDB_OK ;
      const MsgOpLob *header = NULL ;
      BSONObj obj ;
      const MsgLobTuple *tuple = NULL ;
      UINT32 tSize = 0 ;
      const MsgLobTuple *curTuple = NULL ;
      UINT32 tupleNum = 0 ;
      const CHAR *data = NULL ;
      rtnContext *context = NULL ;
      rtnContextShdOfLob *lobContext = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      INT16 w = 0 ;
      BOOLEAN isMainCl = FALSE ;

      rc = msgExtractLobRequest( ( const CHAR * )msg,
                                 &header, obj,
                                 &tuple, &tSize ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract write msg:%d", rc ) ;
         goto error ;
      }

      context = (rtnCB->contextFind ( header->contextID )) ;
      if ( NULL == context )
      {
         PD_LOG ( PDERROR, "context %lld does not exist", header->contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( RTN_CONTEXT_SHARD_OF_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "invalid type of context:%d", context->getType() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      _pEDUCB->writingDB( TRUE ) ;  // it call must before _checkCata

      lobContext = ( rtnContextShdOfLob * )context ;
      _pCollectionName = lobContext->getFullName() ;
      w = lobContext->getW() ;
      rc = _checkCata( header->version, lobContext->getFullName(),
                       w, isMainCl, FALSE ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to check catainfo:%d", rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         BOOLEAN got = FALSE ;
         rc = msgExtractTuplesAndData( &tuple, &tSize,
                                       &curTuple, &data,
                                       &got ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to extract next tuple:%d", rc ) ;
            goto error ;
         }

         if ( !got )
         {
            break ;
         }

         rc = lobContext->update( curTuple->columns.sequence,
                                  curTuple->columns.offset,
                                  curTuple->columns.len,
                                  data, _pEDUCB ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to update lob:%d", rc ) ;
            goto error ;
         }

         if ( 0 == ++tupleNum % SHD_INTERRUPT_CHECKPOINT &&
              _pEDUCB->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }
      }

      PD_LOG( PDDEBUG, "%d pieces of lob[%s] update done",
              tupleNum, lobContext->getOID().str().c_str() ) ;      
   done:
      return rc ;
   error:
      if ( NULL != context &&
           SDB_CLS_COORD_NODE_CAT_VER_OLD != rc &&
           SDB_CLS_DATA_NODE_CAT_VER_OLD != rc  )
      {
         rtnCB->contextDelete( context->contextID(), _pEDUCB ) ;
      }
      goto done ;
   }
}

