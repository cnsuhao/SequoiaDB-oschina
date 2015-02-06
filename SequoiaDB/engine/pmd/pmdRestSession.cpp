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

   Source File Name = pmdRestSession.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/14/2014  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdRestSession.hpp"
#include "pmdController.hpp"
#include "omManager.hpp"
#include "pmdEDUMgr.hpp"
#include "msgDef.h"
#include "utilCommon.hpp"
#include "ossMem.hpp"
#include "rtnCommand.hpp"
#include "../omsvc/omGetFileCommand.hpp"
#include "rtn.hpp"
#include "msgAuth.hpp"

#include "../bson/bson.h"

using namespace bson ;

namespace engine
{
   static void _sendOpError2Web ( INT32 rc, restAdaptor *pAdptor,
                                  pmdRestSession *pRestSession,
                                  pmdEDUCB* pEduCB ) ;

   void _sendOpError2Web ( INT32 rc, restAdaptor *pAdptor, 
                           pmdRestSession *pRestSession,
                           pmdEDUCB* pEduCB )
   {
      BSONObj _errorInfo = utilGetErrorBson( rc, pEduCB->getInfo( 
                                             EDU_INFO_ERROR ) ) ;
      pAdptor->setOPResult( pRestSession, rc, _errorInfo ) ;
      pAdptor->sendResponse( pRestSession, HTTP_OK ) ;
   }

   #define PMD_REST_SESSION_SNIFF_TIMEOUT    ( 10 * OSS_ONE_SEC )

   /*
      _restSessionInfo implement
   */
   void _restSessionInfo::releaseMem()
   {
      pmdEDUCB::CATCH_MAP_IT it = _catchMap.begin() ;
      while ( it != _catchMap.end() )
      {
         SDB_OSS_FREE( it->second ) ;
         ++it ;
      }
      _catchMap.clear() ;
   }

   void _restSessionInfo::pushMemToMap( _pmdEDUCB::CATCH_MAP &catchMap )
   {
      _pmdEDUCB::CATCH_MAP_IT it = _catchMap.begin() ;
      while ( it != _catchMap.end() )
      {
         catchMap.insert( std::make_pair( it->first, it->second ) ) ;
         ++it ;
      }
      _catchMap.clear() ;
   }

   void _restSessionInfo::makeMemFromMap( _pmdEDUCB::CATCH_MAP &catchMap )
   {
      _pmdEDUCB::CATCH_MAP_IT it = catchMap.begin() ;
      while ( it != catchMap.end() )
      {
         _catchMap.insert( std::make_pair( it->first, it->second ) ) ;
         ++it ;
      }
      catchMap.clear() ;
   }

   /*
      _pmdRestSession implement
   */
   _pmdRestSession::_pmdRestSession( SOCKET fd )
   :_pmdSession( fd )
   {
      _pFixBuff         = NULL ;
      _pSessionInfo     = NULL ;
      _pRTNCB           = NULL ;

      _wwwRootPath      = pmdGetOptionCB()->getWWWPath() ;
      _pRestTransfer    = SDB_OSS_NEW RestToMSGTransfer( this ) ;
   }

   _pmdRestSession::~_pmdRestSession()
   {
      if ( _pFixBuff )
      {
         sdbGetPMDController()->releaseFixBuf( _pFixBuff ) ;
         _pFixBuff = NULL ;
      }

      if ( NULL != _pRestTransfer )
      {
         SDB_OSS_DEL _pRestTransfer ;
         _pRestTransfer = NULL ;
      }
   }

   UINT64 _pmdRestSession::identifyID()
   {
      if ( _pSessionInfo )
      {
         return _pSessionInfo->_attr._sessionID ;
      }
      return 0 ;
   }

   INT32 _pmdRestSession::getServiceType() const
   {
      return CMD_SPACE_SERVICE_LOCAL ;
   }

   SDB_SESSION_TYPE _pmdRestSession::sessionType() const
   {
      return SDB_SESSION_REST ;
   }

   INT32 _pmdRestSession::run()
   {
      INT32 rc                         = SDB_OK ;
      restAdaptor *pAdptor             = sdbGetPMDController()->getRestAdptor() ;
      pmdEDUMgr *pEDUMgr               = NULL ;
      const CHAR *pSessionID           = NULL ;
      HTTP_PARSE_COMMON httpCommon     = COM_GETFILE ;
      CHAR *pFilePath                  = NULL ;
      INT32 bodySize                   = 0 ;

      if ( !_pEDUCB )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      pEDUMgr = _pEDUCB->getEDUMgr() ;

      while ( !_pEDUCB->isDisconnected() && !_socket.isClosed() )
      {
         rc = sniffData( _pSessionInfo ? OSS_ONE_SEC :
                         PMD_REST_SESSION_SNIFF_TIMEOUT ) ;
         if ( SDB_TIMEOUT == rc )
         {
            if ( _pSessionInfo )
            {
               saveSession() ;
               sdbGetPMDController()->detachSessionInfo( _pSessionInfo ) ;
               _pSessionInfo = NULL ;
               continue ;
            }
            else
            {
               break ;
            }
         }
         else if ( rc < 0 )
         {
            break ;
         }

         if ( _pEDUCB->isInterrupted( TRUE ) )
         {
            INT64 contextID = -1 ;
            while ( -1 != ( contextID = _pEDUCB->contextPeek() ) )
            {
               _pRTNCB->contextDelete( contextID, NULL ) ;
            }
         }

         _pEDUCB->resetInterrupt() ;
         _pEDUCB->resetInfo( EDU_INFO_ERROR ) ;
         _pEDUCB->resetLsn() ;

         rc = pAdptor->recvRequestHeader( this ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Session[%s] failed to recv rest header, "
                    "rc: %d", sessionName(), rc ) ;
            if ( SDB_REST_EHS == rc )
            {
               pAdptor->sendResponse( this, HTTP_BADREQ ) ;
            }
            else if ( SDB_APP_FORCED != rc )
            {
               _sendOpError2Web( rc, pAdptor, this, _pEDUCB ) ;
            }
            break ;
         }
         if ( !_pSessionInfo )
         {
            pAdptor->getHttpHeader( this, OM_REST_HEAD_SESSIONID, 
                                    &pSessionID ) ;
            if ( pSessionID )
            {
               PD_LOG( PDINFO, "Rest session: %s", pSessionID ) ;
               _pSessionInfo = sdbGetPMDController()->attachSessionInfo(
                                  pSessionID ) ;
            }

            if ( _pSessionInfo )
            {
               _client.setAuthed( TRUE ) ;
               restoreSession() ;
            }
         }
         rc = pAdptor->recvRequestBody( this, httpCommon, &pFilePath, 
                                        bodySize ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Session[%s] failed to recv rest body, "
                    "rc: %d", sessionName(), rc ) ;
            if ( SDB_REST_EHS == rc )
            {
               pAdptor->sendResponse( this, HTTP_BADREQ ) ;
            }
            else if ( SDB_APP_FORCED != rc )
            {
               _sendOpError2Web( rc, pAdptor, this, _pEDUCB ) ;
            }
            break ;
         }

         if ( _pSessionInfo )
         {
            _pSessionInfo->active() ;
         }

         _pEDUCB->incEventCount() ;

         if ( SDB_OK != ( rc = pEDUMgr->activateEDU( _pEDUCB ) ) )
         {
            PD_LOG( PDERROR, "Session[%s] activate edu failed, rc: %d",
                    sessionName(), rc ) ;
            break ;
         }

         rc = _processMsg( httpCommon, pFilePath ) ;
         if ( rc )
         {
            break ;
         }

         if ( SDB_OK != ( rc = pEDUMgr->waitEDU( _pEDUCB ) ) )
         {
            PD_LOG( PDERROR, "Session[%s] wait edu failed, rc: %d",
                    sessionName(), rc ) ;
            break ;
         }

         if ( pFilePath )
         {
            releaseBuff( pFilePath, bodySize ) ;
            pFilePath = NULL ;
         }
         rc = SDB_OK ;
      } // end while

   done:
      if ( pFilePath )
      {
         releaseBuff( pFilePath, bodySize ) ;
      }
      disconnect() ;
      return rc ;
   error:
      goto done ;
   }


   INT32 _pmdRestSession::_translateMSG( restAdaptor *pAdaptor, 
                                         HTTP_PARSE_COMMON command, 
                                         const CHAR *pFilePath,
                                         MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      if ( NULL == msg )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "msg can't be null" ) ;
         goto error ;
      }

      rc = _pRestTransfer->trans( pAdaptor, command, pFilePath, msg ) ;

   done:
      return rc ;
   error:
      goto done ;
   }


   INT32 _pmdRestSession::_fetchOneContext( SINT64 &contextID, 
                                            rtnContextBuf &contextBuff )
   {
      INT32 rc = SDB_OK ;
      rtnContext *pContext = _pRTNCB->contextFind( contextID ) ;
      if ( NULL == pContext )
      {
         contextID = -1 ;
      }
      else
      {
         rc = pContext->getMore( -1, contextBuff, _pEDUCB ) ;
         if ( rc || pContext->eof() )
         {
            _pRTNCB->contextDelete( contextID, _pEDUCB ) ;
            contextID = -1 ;
            if ( SDB_DMS_EOC != rc )
            {
               PD_LOG( PDERROR, "getmore failed:rc=%d,contextID=%u", rc, 
                       contextID ) ;
            }
         }
      }

      rc = SDB_OK ;
      return rc ;
   }

   INT32 _pmdRestSession::_processMsg( HTTP_PARSE_COMMON command, 
                                       const CHAR *pFilePath )
   {
      INT32 rc = SDB_OK ;
      restAdaptor *pAdaptor = sdbGetPMDController()->getRestAdptor() ;
      const CHAR *pSubCommand = NULL ;
      pAdaptor->getQuery( this, OM_REST_FIELD_COMMAND, &pSubCommand ) ;
      if ( NULL != pSubCommand )
      {
         if ( ossStrcasecmp( pSubCommand, OM_LOGOUT_REQ ) == 0 )
         {
            doLogout() ;
            pAdaptor->sendResponse( this, HTTP_OK ) ;
            goto done ;
         }
      }

      rc = _processBusinessMsg( pAdaptor, command, pFilePath ) ;
      if ( SDB_UNKNOWN_MESSAGE == rc )
      {
         if ( pmdGetKRCB()->isCBValue( SDB_CB_OMSVC ) )
         {
            rc = _processOMRestMsg( command, pFilePath ) ;
         }
         else
         {
            PD_LOG( PDERROR, "translate message failed:rc=%d", rc ) ;
            _sendOpError2Web( rc, pAdaptor, this, _pEDUCB ) ;
         }
      }

   done:
      return rc ;
   }
   INT32 _pmdRestSession::_processBusinessMsg( restAdaptor *pAdaptor, 
                                               HTTP_PARSE_COMMON command, 
                                               const CHAR *pFilePath ) 
   {
      INT32 rc        = SDB_OK ;
      INT64 contextID = -1 ;
      rtnContextBuf contextBuff ;
      BOOLEAN needReplay = FALSE ;
      MsgHeader *msg = NULL ;
      rc = _translateMSG( pAdaptor, command, pFilePath, &msg ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_UNKNOWN_MESSAGE != rc )
         {
            PD_LOG( PDERROR, "translate message failed:rc=%d", rc ) ;
            _sendOpError2Web( rc, pAdaptor, this, _pEDUCB ) ;
         }

         goto error ;
      }

      rc = getProcessor()->processMsg( msg, contextBuff, contextID, 
                                       needReplay ) ;
      if ( SDB_OK != rc )
      {
         BSONObjBuilder builder ;
         if ( contextBuff.recordNum() != 0 )
         {
            BSONObj errorInfo( contextBuff.data() ) ;
            if ( !errorInfo.hasField( OM_REST_RES_RETCODE ) )
            {
               builder.append( OM_REST_RES_RETCODE, rc ) ;
            }
            builder.appendElements( errorInfo ) ;
         }
         else
         {
            BSONObj errorInfo = utilGetErrorBson( rc, 
                                          _pEDUCB->getInfo( EDU_INFO_ERROR ) ) ;
            builder.appendElements( errorInfo ) ;
         }

         BSONObj tmp = builder.obj() ;
         pAdaptor->setOPResult( this, rc, tmp ) ;
      }
      else 
      {
         rtnContextBuf fetchOneBuff ;
         if ( -1 != contextID )
         {
            _fetchOneContext( contextID, fetchOneBuff ) ;
            if ( -1 != contextID )
            {
               pAdaptor->setChunkModal( this ) ;
            }
         }

         BSONObj tmp = BSON( OM_REST_RES_RETCODE << rc ) ;
         pAdaptor->setOPResult( this, rc, tmp ) ;
         if ( 0 != contextBuff.recordNum() )
         {
            pAdaptor->appendHttpBody( this, contextBuff.data(), 
                                      contextBuff.size(), 
                                      contextBuff.recordNum() ) ;
         }

         if ( 0 != fetchOneBuff.recordNum() )
         {
            pAdaptor->appendHttpBody( this, fetchOneBuff.data(), 
                                      fetchOneBuff.size(), 
                                      fetchOneBuff.recordNum() ) ;
         }

         if ( -1 != contextID )
         {
            rtnContext *pContext = _pRTNCB->contextFind( contextID ) ;
            while ( NULL != pContext )
            {
               rtnContextBuf tmpContextBuff ;
               rc = pContext->getMore( -1, tmpContextBuff, _pEDUCB ) ;
               if ( SDB_OK == rc )
               {
                  rc = pAdaptor->appendHttpBody( this, tmpContextBuff.data(), 
                                              tmpContextBuff.size(), 
                                              tmpContextBuff.recordNum() ) ;
                  if ( SDB_OK != rc )
                  {
                     PD_LOG( PDERROR, "append http body failed:rc=%d", rc ) ;
                     goto error ;
                  }
               }
               else 
               {
                  _pRTNCB->contextDelete( contextID, _pEDUCB ) ;
                  contextID = -1 ;
                  if ( SDB_DMS_EOC != rc )
                  {
                     PD_LOG( PDERROR, "getmore failed:rc=%d", rc ) ;
                  }

                  rc = SDB_OK ;
                  break ;
               }
            }
         }
      }

      _dealWithLoginReq( rc ) ;
      pAdaptor->sendResponse( this, HTTP_OK ) ;

      rc = SDB_OK ;

   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete( contextID, _pEDUCB ) ;
         contextID = -1 ;
      }
      if ( NULL != msg )
      {
         SDB_OSS_FREE( msg ) ;
         msg = NULL ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdRestSession::_dealWithLoginReq( INT32 result )
   {
      INT32 rc = SDB_OK ;
      const CHAR *pSubCommand = NULL ;
      restAdaptor *pAdaptor   = sdbGetPMDController()->getRestAdptor() ;

      if ( SDB_OK != result )
      {
         goto done ;
      }

      pAdaptor->getQuery( this, OM_REST_FIELD_COMMAND, &pSubCommand ) ;
      if ( NULL != pSubCommand )
      {
         if ( ossStrcasecmp( pSubCommand, OM_LOGIN_REQ ) == 0 )
         {
            const CHAR* pUser = NULL ;
            pAdaptor->getQuery( this, OM_REST_FIELD_LOGIN_NAME , &pUser ) ;
            SDB_ASSERT( ( NULL != pUser ), "" ) ;
            rc = doLogin( pUser, socket()->getLocalIP() ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "login failed:rc=%d", rc ) ;
            }
            pAdaptor->appendHttpHeader( this, OM_REST_HEAD_SESSIONID, 
                                        getSessionID() ) ;

         }
      }

   done:
      return rc ;
   }
   INT32 _pmdRestSession::_processOMRestMsg( HTTP_PARSE_COMMON command, 
                                             const CHAR *pFilePath )
   {
      restAdaptor *pAdptor          = NULL ;
      omRestCommandBase *pOmCommand = NULL ;
      pAdptor = sdbGetPMDController()->getRestAdptor() ;
      pOmCommand = _createCommand( command, pFilePath ) ;
      if ( NULL == pOmCommand )
      {
         goto error ;
      }

      pOmCommand->init( _pEDUCB ) ;
      pOmCommand->doCommand() ;

   done:
      if ( NULL != pOmCommand )
      {
         SDB_OSS_DEL pOmCommand ;
      }
      return SDB_OK ;

   error:
      goto done ;
   }

   omRestCommandBase *_pmdRestSession::_createCommand( 
                                             HTTP_PARSE_COMMON command, 
                                             const CHAR *pFilePath )
   {
      omRestCommandBase *commandIf = NULL ;
      restAdaptor *pAdptor         = NULL ;
      const CHAR* hostName = pmdGetKRCB()->getHostName();
      pAdptor = sdbGetPMDController()->getRestAdptor() ;
      string localAgentHost = hostName ;
      string localAgentPort = sdbGetOMManager()->getLocalAgentPort() ;

      if ( COM_GETFILE == command )
      {
         PD_LOG( PDEVENT, "OM: getfile command:file=%s", pFilePath ) ;
         commandIf = SDB_OSS_NEW omGetFileCommand( pAdptor, this,
                                                   _wwwRootPath.c_str(),
                                                   pFilePath ) ;
      }
      else 
      {
         const CHAR *pSubCommand = NULL ;
         pAdptor->getQuery( this, OM_REST_FIELD_COMMAND, &pSubCommand ) ;
         if ( NULL == pSubCommand )
         {
            BSONObjBuilder builder ;
            builder.append( OM_REST_RES_RETCODE, SDB_INVALIDARG ) ;
            builder.append( OM_REST_RES_DESP, getErrDesp( SDB_INVALIDARG ) ) ;
            builder.append( OM_REST_RES_DETAIL, "command is null" ) ;
            pAdptor->setOPResult( this, SDB_INVALIDARG, builder.obj() ) ;
            pAdptor->sendResponse( this, HTTP_OK ) ;
            goto error ;
         }

         PD_LOG( PDDEBUG, "OM: command:command=%s", pSubCommand ) ;
         if ( ossStrcmp( pSubCommand, OM_LOGIN_REQ ) != 0
              && ossStrcmp( pSubCommand, OM_CHECK_SESSION_REQ ) != 0
              && !isAuthOK() )
         {
            BSONObjBuilder builder ;
            builder.append( OM_REST_RES_RETCODE, 
                            SDB_PMD_SESSION_NOT_EXIST ) ;
            builder.append( OM_REST_RES_DESP, 
                            getErrDesp( SDB_PMD_SESSION_NOT_EXIST ) ) ;
            builder.append( OM_REST_RES_DETAIL, "" ) ;
            pAdptor->setOPResult( this, SDB_PMD_SESSION_NOT_EXIST, 
                                  builder.obj() ) ;
            pAdptor->sendResponse( this, HTTP_OK ) ;
            PD_LOG( PDEVENT, "OM: redirect to:%s", OM_REST_LOGIN_HTML ) ;
            goto error ;
         }

         else if ( ossStrcasecmp( pSubCommand, OM_CHANGE_PASSWD_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omChangePasswdCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_CHECK_SESSION_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omCheckSessionCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_CREATE_CLUSTER_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omCreateClusterCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_QUERY_CLUSTER_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omQueryClusterCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_SCAN_HOST_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omScanHostCommand( pAdptor, this, 
                                       localAgentHost, localAgentPort ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_CHECK_HOST_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omCheckHostCommand( pAdptor, this, 
                                       localAgentHost, localAgentPort ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_ADD_HOST_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omAddHostCommand( pAdptor, this, 
                                       localAgentHost, localAgentPort ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_LIST_HOST_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omListHostCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_QUERY_HOST_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omQueryHostCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_LIST_BUSINESS_TYPE_REQ ) 
                                                                          == 0 )
         {
            commandIf = SDB_OSS_NEW omListBusinessTypeCommand( pAdptor, this, 
                                       _wwwRootPath.c_str(), pFilePath ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, 
                                  OM_GET_BUSINESS_TEMPLATE_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omGetBusinessTemplateCommand( pAdptor, 
                                       this, _wwwRootPath.c_str(), pFilePath ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_CONFIG_BUSINESS_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omConfigBusinessCommand( pAdptor, this, 
                                       _wwwRootPath.c_str(), pFilePath ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_INSTALL_BUSINESS_REQ) == 0 )
         {
            commandIf = SDB_OSS_NEW omInstallBusinessReq( pAdptor, this, 
                                       _wwwRootPath.c_str(), pFilePath, 
                                       localAgentHost, localAgentPort ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_LIST_NODE_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omListNodeCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_GET_NODE_CONF_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omQueryNodeConfCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_LIST_BUSINESS_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omListBusinessCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_QUERY_BUSINESS_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omQueryBusinessCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_REMOVE_CLUSTER_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omRemoveClusterCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_REMOVE_HOST_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omRemoveHostCommand( pAdptor, this,
                                       localAgentHost, localAgentPort ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_REMOVE_BUSINESS_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omRemoveBusinessCommand( pAdptor, this,
                                       localAgentHost, localAgentPort ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_QUERY_HOST_STATUS_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omQueryHostStatusCommand( pAdptor, this,
                                       localAgentHost, localAgentPort ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_PREDICT_CAPACITY_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omPredictCapacity( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_LIST_TASK_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omListTaskCommand( pAdptor, this ) ;
         }
         else if ( ossStrcasecmp( pSubCommand, OM_QUERY_TASK_REQ ) == 0 )
         {
            commandIf = SDB_OSS_NEW omQueryTaskCommand( pAdptor, this ) ;
         }
         else
         {
            BSONObjBuilder builder ;
            string errorInfo ;
            errorInfo = string("command is unreconigzed:") + pSubCommand ;
            builder.append( OM_REST_RES_RETCODE, SDB_INVALIDARG ) ;
            builder.append( OM_REST_RES_DESP, getErrDesp( SDB_INVALIDARG ) ) ;
            builder.append( OM_REST_RES_DETAIL, errorInfo.c_str() ) ;
            pAdptor->setOPResult( this, SDB_INVALIDARG, builder.obj() ) ;
            pAdptor->sendResponse( this, HTTP_OK ) ;
            goto error ;
         }
      }

   done:
      return commandIf ;
   error:
      goto done ;
   }

   void _pmdRestSession::_onAttach()
   {
      pmdKRCB *krcb = pmdGetKRCB() ;
      _pRTNCB = krcb->getRTNCB() ;

      if ( NULL != sdbGetPMDController()->getRSManager() )
      {
         sdbGetPMDController()->getRSManager()->registerEDU( eduCB() ) ;
      }
   }

   void _pmdRestSession::_onDetach()
   {
      if ( _pSessionInfo )
      {
         saveSession() ;
         sdbGetPMDController()->detachSessionInfo( _pSessionInfo ) ;
         _pSessionInfo = NULL ;
      }

      if ( NULL != sdbGetPMDController()->getRSManager() )
      {
         sdbGetPMDController()->getRSManager()->unregEUD( eduCB() ) ;
      }
   }

   INT32 _pmdRestSession::getFixBuffSize() const
   {
      return sdbGetPMDController()->getFixBufSize() ;
   }

   CHAR* _pmdRestSession::getFixBuff ()
   {
      if ( !_pFixBuff )
      {
         _pFixBuff = sdbGetPMDController()->allocFixBuf() ;
      }
      return _pFixBuff ;
   }

   void _pmdRestSession::restoreSession()
   {
      pmdEDUCB::CATCH_MAP catchMap ;
      _pSessionInfo->pushMemToMap( catchMap ) ;
      eduCB()->restoreBuffs( catchMap ) ;
   }

   void _pmdRestSession::saveSession()
   {
      pmdEDUCB::CATCH_MAP catchMap ;
      eduCB()->saveBuffs( catchMap ) ;
      _pSessionInfo->makeMemFromMap( catchMap ) ;
   }

   BOOLEAN _pmdRestSession::isAuthOK()
   {
      if ( NULL != _pSessionInfo )
      {
         if ( _pSessionInfo->_authOK )
         {
            return TRUE ;
         }
      }

      return FALSE ;
   }

   string _pmdRestSession::getLoginUserName()
   {
      if ( isAuthOK() )
      {
         return _pSessionInfo->_attr._userName ;
      }

      return "" ;
   }

   const CHAR* _pmdRestSession::getSessionID()
   {
      if ( NULL != _pSessionInfo )
      {
         if ( _pSessionInfo->_authOK )
         {
            return _pSessionInfo->_id.c_str();
         }
      }

      return "" ;
   }

   INT32 _pmdRestSession::doLogin( const string & username,
                                   UINT32 localIP )
   {
      INT32 rc = SDB_OK ;

      doLogout() ;

      _pSessionInfo = sdbGetPMDController()->newSessionInfo( username,
                                                             localIP ) ;
      if ( !_pSessionInfo )
      {
         PD_LOG ( PDERROR, "Failed to allocate session" ) ;
         rc = SDB_OOM ;
      }
      else
      {
         _pSessionInfo->_authOK = TRUE ;
      }
      return rc ;
   }

   void _pmdRestSession::doLogout()
   {
      if ( _pSessionInfo )
      {
         sdbGetPMDController()->releaseSessionInfo( _pSessionInfo->_id ) ;
         _pSessionInfo = NULL ;
      }
   }

   RestToMSGTransfer::RestToMSGTransfer( pmdRestSession *session )
                     :_restSession( session )
   {
   }

   RestToMSGTransfer::~RestToMSGTransfer()
   {
   }

   INT32 RestToMSGTransfer::trans( restAdaptor *pAdaptor, 
                                   HTTP_PARSE_COMMON command, 
                                   const CHAR *pFilePath, MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      const CHAR *pSubCommand = NULL ;
      if ( COM_GETFILE == command )
      {
         rc = SDB_UNKNOWN_MESSAGE ;
         PD_LOG_MSG( PDERROR, "unsupported command:command=%d", command ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, OM_REST_FIELD_COMMAND, &pSubCommand ) ;
      if ( NULL == pSubCommand )
      {
         rc = SDB_UNKNOWN_MESSAGE ;
         PD_LOG_MSG( PDERROR, "can't resolve field:field=%s", 
                     OM_REST_FIELD_COMMAND ) ;
         goto error ;
      }

      if ( ossStrcasecmp( pSubCommand, REST_CMD_NAME_QUERY ) == 0 )
      {
         rc = _convertQuery( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, REST_CMD_NAME_INSERT ) == 0 )
      {
         rc = _convertInsert( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, REST_CMD_NAME_DELETE ) == 0 )
      {
         rc = _convertDelete( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, REST_CMD_NAME_UPDATE ) == 0 )
      {
         rc = _convertUpdate( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, CMD_NAME_CREATE_COLLECTIONSPACE ) == 0 )
      {
         rc = _convertCreateCS( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, CMD_NAME_CREATE_COLLECTION ) == 0 )
      {
         rc = _convertCreateCL( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, CMD_NAME_DROP_COLLECTIONSPACE ) == 0 )
      {
         rc = _convertDropCS( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, CMD_NAME_DROP_COLLECTION ) == 0 )
      {
         rc = _convertDropCL( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, CMD_NAME_SPLIT ) == 0 )
      {
         rc = _convertSplit( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, CMD_NAME_LIST_GROUPS ) == 0 )
      {
         rc = _convertListGroups( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, CMD_NAME_ALTER_COLLECTION ) == 0 )
      {
         rc = _convertAlterCollection( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, CMD_NAME_GET_COUNT ) == 0 )
      {
         rc = _convertGetCount( pAdaptor, msg ) ;
      }
      else if ( ossStrcasecmp( pSubCommand, OM_LOGIN_REQ ) == 0 )
      {
         rc = _convertLogin( pAdaptor, msg ) ;
      }
      else
      {
         PD_LOG_MSG( PDERROR, "unsupported command:command=%s", pSubCommand ) ;
         rc = SDB_UNKNOWN_MESSAGE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertCreateCS( restAdaptor *pAdaptor, 
                                              MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTIONSPACE ;
      const CHAR *pOption   = NULL ;
      const CHAR *pCollectionSpace = NULL ;
      BSONObj option ;
      BSONObj query ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, 
                          &pCollectionSpace ) ;
      if ( NULL == pCollectionSpace )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collectionspace's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_OPTIONS, &pOption ) ;
      if ( NULL != pOption )
      {
         rc = fromjson( pOption, option ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s", 
                        FIELD_NAME_OPTIONS, pOption ) ;
            goto error ;
         }
      }

      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_NAME, pCollectionSpace ) ;
         {
            BSONObjIterator it ( option ) ;
            while ( it.more() )
            {
               builder.append( it.next() ) ;
            }
         }

         query = builder.obj() ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query, 
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d", 
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertCreateCL( restAdaptor *pAdaptor,
                                              MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTION ;
      const CHAR *pOption   = NULL ;
      const CHAR *pCollection = NULL ;
      BSONObj option ;
      BSONObj query ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, &pCollection ) ;
      if ( NULL == pCollection )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_OPTIONS, &pOption ) ;
      if ( NULL != pOption )
      {
         rc = fromjson( pOption, option ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s", 
                        FIELD_NAME_OPTIONS, pOption ) ;
            goto error ;
         }
      }

      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_NAME, pCollection ) ;
         {
            BSONObjIterator it ( option ) ;
            while ( it.more() )
            {
               builder.append( it.next() ) ;
            }
         }

         query = builder.obj() ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query, 
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d", 
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDropCS( restAdaptor *pAdaptor, 
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTIONSPACE ;
      const CHAR *pCollectionSpace = NULL ;
      BSONObj query ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, 
                          &pCollectionSpace ) ;
      if ( NULL == pCollectionSpace )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collectionspace's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_NAME, pCollectionSpace ) ;
         query = builder.obj() ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query, 
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d", 
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDropCL( restAdaptor *pAdaptor,
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTION ;
      const CHAR *pCollection = NULL ;
      BSONObj query ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, &pCollection ) ;
      if ( NULL == pCollection )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_NAME, pCollection ) ;
         query = builder.obj() ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query, 
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d", 
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertQuery( restAdaptor *pAdaptor, 
                                           MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pTable    = NULL ;
      const CHAR *pOrder    = NULL ;
      const CHAR *pHint     = NULL ;
      const CHAR *pMatch    = NULL ;
      const CHAR *pSelector = NULL ;
      const CHAR *pFlag     = NULL ;
      const CHAR *pSkip     = NULL ;
      const CHAR *pReturnRow = NULL ;
      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, &pTable ) ;
      if ( NULL == pTable )
      {
         PD_LOG_MSG( PDERROR, "get field failed:field=%s", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_SORT, &pOrder ) ;
      pAdaptor->getQuery( _restSession, FIELD_NAME_HINT, &pHint ) ;
      pAdaptor->getQuery( _restSession, FIELD_NAME_FILTER, &pMatch ) ;
      pAdaptor->getQuery( _restSession, FIELD_NAME_SELECTOR, &pSelector ) ;
      pAdaptor->getQuery( _restSession, REST_KEY_NAME_FLAG, &pFlag ) ;
      pAdaptor->getQuery( _restSession, FIELD_NAME_SKIP, &pSkip ) ;
      pAdaptor->getQuery( _restSession, FIELD_NAME_RETURN_NUM, 
                          &pReturnRow ) ;
      {
         BSONObj order ;
         BSONObj hint ;
         BSONObj match ;
         BSONObj selector ;
         INT32 flag = FLG_QUERY_WITH_RETURNDATA ;
         INT32 skip = 0 ;
         INT32 returnRow = -1 ;
         if ( NULL != pOrder )
         {
            rc = fromjson( pOrder, order ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s", 
                           FIELD_NAME_SORT, pOrder ) ;
               goto error ;
            }
         }

         if ( NULL != pHint )
         {
            rc = fromjson( pHint, hint ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s", 
                           FIELD_NAME_HINT, pHint ) ;
               goto error ;
            }
         }

         if ( NULL != pMatch )
         {
            rc = fromjson( pMatch, match ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s", 
                           FIELD_NAME_FILTER, pMatch ) ;
               goto error ;
            }
         }

         if ( NULL != pSelector )
         {
            rc = fromjson( pSelector, selector ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s", 
                           FIELD_NAME_SELECTOR, pSelector ) ;
               goto error ;
            }
         }

         if ( NULL != pFlag )
         {
            flag = ossAtoi( pFlag ) ;
            flag = flag | FLG_QUERY_WITH_RETURNDATA ;
         }

         if ( NULL != pSkip )
         {
            skip = ossAtoi( pSkip ) ;
         }

         if ( NULL != pReturnRow )
         {
            returnRow = ossAtoi( pReturnRow ) ;
         }


         rc = msgBuildQueryMsg( &pBuff, &buffSize, pTable, flag, 0, skip, 
                                returnRow, &match, &selector, &order, &hint ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "build queryMSG failed:rc=%d", rc ) ;
            goto error ;
         }
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertInsert( restAdaptor *pAdaptor, 
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pFlag     = NULL ;
      SINT32 flag           = 0 ;
      const CHAR *pCollection = NULL ;
      const CHAR *pInsertor   = NULL ;
      BSONObj insertor ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, &pCollection ) ;
      if ( NULL == pCollection )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, REST_KEY_NAME_FLAG, &pFlag ) ;
      if ( NULL != pFlag )
      {
         flag = ossAtoi( pFlag ) ;
      }

      pAdaptor->getQuery( _restSession, REST_KEY_NAME_INSERTOR, &pInsertor ) ;
      if ( NULL == pInsertor )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     REST_KEY_NAME_INSERTOR ) ;
         goto error ;
      }

      rc = fromjson( pInsertor, insertor ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                     REST_KEY_NAME_INSERTOR, pInsertor ) ;
         goto error ;
      }

      rc = msgBuildInsertMsg( &pBuff, &buffSize, pCollection, flag, 0, 
                              &insertor );
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build insertMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertUpdate( restAdaptor *pAdaptor,
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pFlag     = NULL ;
      SINT32 flag           = 0 ;
      const CHAR *pCollection = NULL ;
      const CHAR *pUpdator    = NULL ;
      const CHAR *pMatcher    = NULL ;
      const CHAR *pHint       = NULL ;
      BSONObj updator ;
      BSONObj matcher ;
      BSONObj hint ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, &pCollection ) ;
      if ( NULL == pCollection )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, REST_KEY_NAME_FLAG, &pFlag ) ;
      if ( NULL != pFlag )
      {
         flag = ossAtoi( pFlag ) ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_FILTER, &pMatcher ) ;
      if ( NULL != pMatcher )
      {
         rc = fromjson( pMatcher, matcher ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                        FIELD_NAME_FILTER, pMatcher ) ;
            goto error ;
         }
      }

      pAdaptor->getQuery( _restSession, REST_KEY_NAME_UPDATOR, &pUpdator ) ;
      if ( NULL == pUpdator )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     REST_KEY_NAME_UPDATOR ) ;
         goto error ;
      }

      rc = fromjson( pUpdator, updator ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                     REST_KEY_NAME_UPDATOR, pUpdator ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_HINT, &pHint ) ;
      if ( NULL != pHint )
      {
         rc = fromjson( pHint, hint ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                        FIELD_NAME_HINT, pHint ) ;
            goto error ;
         }
      }

      rc = msgBuildUpdateMsg( &pBuff, &buffSize, pCollection, flag, 0, 
                              &matcher, &updator, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build updateMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDelete( restAdaptor *pAdaptor,
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pFlag     = NULL ;
      SINT32 flag           = 0 ;
      const CHAR *pCollection = NULL ;
      const CHAR *pDeletor    = NULL ;
      const CHAR *pHint       = NULL ;
      BSONObj deletor ;
      BSONObj hint ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, &pCollection ) ;
      if ( NULL == pCollection )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, REST_KEY_NAME_FLAG, &pFlag ) ;
      if ( NULL != pFlag )
      {
         flag = ossAtoi( pFlag ) ;
      }

      pAdaptor->getQuery( _restSession, REST_KEY_NAME_DELETOR, &pDeletor ) ;
      if ( NULL != pDeletor )
      {
         rc = fromjson( pDeletor, deletor ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                        REST_KEY_NAME_DELETOR, pDeletor ) ;
            goto error ;
         }
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_HINT, &pHint ) ;
      if ( NULL != pHint )
      {
         rc = fromjson( pHint, hint ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                        FIELD_NAME_HINT, pHint ) ;
            goto error ;
         }
      }

      rc = msgBuildDeleteMsg( &pBuff, &buffSize, pCollection, flag, 0, 
                              &deletor, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build deleteMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertAlterCollection( restAdaptor *pAdaptor,
                                                     MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_ALTER_COLLECTION ;
      const CHAR *pOption   = NULL ;
      const CHAR *pCollection = NULL ;
      BSONObj option ;
      BSONObj query ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, &pCollection ) ;
      if ( NULL == pCollection )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_OPTIONS, &pOption ) ;
      if ( NULL == pOption )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get alter collection's %s failed", 
                     FIELD_NAME_OPTIONS ) ;
         goto error ;
      }

      rc = fromjson( pOption, option ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s", 
                     FIELD_NAME_OPTIONS, pOption ) ;
         goto error ;
      }

      query = BSON( FIELD_NAME_NAME << pCollection 
                    << FIELD_NAME_OPTIONS << option ) ;
      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query, 
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d", 
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSplit( restAdaptor *pAdaptor,
                                           MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SPLIT ;
      const CHAR *pSource   = NULL ;
      const CHAR *pTarget   = NULL ;
      const CHAR *pAsync    = NULL ;
      const CHAR *pCollection    = NULL ;
      const CHAR *pSplitQuery    = NULL ;
      const CHAR *pSplitEndQuery = NULL ;
      const CHAR *pPercent = NULL ;
      BOOLEAN isUsePercent = FALSE ;
      INT32 percent        = 0 ;
      bool bAsync   = false ;
      BSONObj splitQuery ;
      BSONObj splitEndQuery ;
      BSONObj query ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, 
                          &pCollection ) ;
      if ( NULL == pCollection )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", 
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_SOURCE, &pSource ) ;
      if ( NULL == pSource )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get split's %s failed", FIELD_NAME_SOURCE ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_TARGET, &pTarget ) ;
      if ( NULL == pTarget )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get split's %s failed", FIELD_NAME_TARGET ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_SPLITPERCENT, &pPercent ) ;
      if ( NULL == pPercent )
      {
         pAdaptor->getQuery( _restSession, FIELD_NAME_SPLITQUERY, &pSplitQuery ) ;
         if ( NULL == pSplitQuery )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get split's %s failed", FIELD_NAME_SPLITQUERY ) ;
            goto error ;
         }

         rc = fromjson( pSplitQuery, splitQuery ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                        FIELD_NAME_SPLITQUERY, pSplitQuery ) ;
            goto error ;
         }

         pAdaptor->getQuery( _restSession, FIELD_NAME_SPLITENDQUERY, 
                             &pSplitEndQuery ) ;
         if ( NULL != pSplitEndQuery )
         {
            rc = fromjson( pSplitEndQuery, splitEndQuery ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                           FIELD_NAME_SPLITENDQUERY, pSplitEndQuery ) ;
               goto error ;
            }
         }
      }
      else 
      {
         isUsePercent = TRUE ;
         percent      = ossAtoi( pPercent ) ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_ASYNC, &pAsync ) ;
      if ( NULL != pAsync )
      {
         if ( ossStrcasecmp( pAsync, "TRUE" ) == 0 )
         {
            bAsync = true ;
         }
      }

      if ( isUsePercent )
      {
         query = BSON( FIELD_NAME_NAME << pCollection << FIELD_NAME_SOURCE
                       << pSource << FIELD_NAME_TARGET << pTarget 
                       << FIELD_NAME_SPLITPERCENT << ( FLOAT64 )percent
                       << FIELD_NAME_ASYNC << bAsync ) ;
      }
      else
      {
         query = BSON( FIELD_NAME_NAME << pCollection << FIELD_NAME_SOURCE
                       << pSource << FIELD_NAME_TARGET << pTarget 
                       << FIELD_NAME_SPLITQUERY << splitQuery 
                       << FIELD_NAME_SPLITENDQUERY << splitEndQuery
                       << FIELD_NAME_ASYNC << bAsync ) ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query, 
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d", 
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListGroups( restAdaptor *pAdaptor,
                                                MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_GROUPS ;

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, NULL, 
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d", 
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertGetCount( restAdaptor *pAdaptor,
                                              MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_GET_COUNT ;
      const CHAR *pCollection = NULL ;
      const CHAR *pMatcher    = NULL ;
      const CHAR *pHint       = NULL ;
      BSONObj matcher ;
      BSONObj hint ;

      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, 
                          &pCollection ) ;
      if ( NULL == pCollection )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed", FIELD_NAME_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_FILTER, &pMatcher ) ;
      if ( NULL != pMatcher )
      {
         rc = fromjson( pMatcher, matcher ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                        FIELD_NAME_FILTER, pMatcher ) ;
            goto error ;
         }
      }

      pAdaptor->getQuery( _restSession, FIELD_NAME_HINT, &pHint ) ;
      if ( NULL != pHint )
      {
         rc = fromjson( pHint, hint ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s", 
                        FIELD_NAME_HINT, pHint ) ;
            goto error ;
         }
      }

      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_COLLECTION, pCollection ) ;
         if ( !hint.isEmpty() )
         {
            builder.append( FIELD_NAME_HINT, hint ) ;
         }

         hint = builder.obj() ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &matcher, 
                             NULL, NULL, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d", 
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertLogin( restAdaptor *pAdaptor,
                                           MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pUser     = NULL ;
      const CHAR *pPasswd   = NULL ;

      pAdaptor->getQuery( _restSession, OM_REST_FIELD_LOGIN_NAME, 
                          &pUser ) ;
      if ( NULL == pUser )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get login name failed:field=%s", 
                     OM_REST_FIELD_LOGIN_NAME ) ;
         goto error ;
      }

      pAdaptor->getQuery( _restSession, OM_REST_FIELD_LOGIN_PASSWD, 
                          &pPasswd ) ;
      if ( NULL == pPasswd )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get passwd failed:field=%s", 
                     OM_REST_FIELD_LOGIN_PASSWD ) ;
         goto error ;
      }

      rc = msgBuildAuthMsg( &pBuff, &buffSize, pUser, pPasswd, 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "buildAuthMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }
}

