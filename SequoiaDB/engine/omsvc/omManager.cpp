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

   Source File Name = omManager.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/15/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "omManager.hpp"
#include "../bson/lib/md5.hpp"
#include "authCB.hpp"
#include "pmdEDU.hpp"
#include "pmd.hpp"
#include "../bson/bsonobj.h"
#include "../util/fromjson.hpp"
#include "catCommon.hpp"
#include "ossProc.hpp"
#include "rtn.hpp"
#include "rtnBackgroundJob.hpp"
#include "pmdController.hpp"
#include "omManagerJob.hpp"
#include "../omsvc/omGetFileCommand.hpp"

using namespace bson ;

namespace engine
{

   #define OM_WAIT_CB_ATTACH_TIMEOUT               ( 300 * OSS_ONE_SEC )

   /*
      Message Map
   */
   BEGIN_OBJ_MSG_MAP( _omManager, _pmdObjBase )
      ON_MSG( MSG_BS_QUERY_REQ, _onAgentQueryTaskReq )
      ON_MSG( MSG_OM_UPDATE_TASK_REQ, _onAgentUpdateTaskReq )
   END_OBJ_MSG_MAP()

   /*
      implement om manager
   */
   _omManager::_omManager()
   :_rsManager(),
    _msgHandler( &_rsManager ),
    _netAgent( &_msgHandler )
   {
      _hwRouteID.value             = MSG_INVALID_ROUTEID ;
      _hwRouteID.columns.groupID   = 2 ;
      _hwRouteID.columns.nodeID    = 0 ;
      _hwRouteID.columns.serviceID = MSG_ROUTE_LOCAL_SERVICE ;

      _pKrcb               = NULL ;
      _pDmsCB              = NULL ;
      _hostVersion         = SDB_OSS_NEW omHostVersion() ;
      _taskManager         = SDB_OSS_NEW omTaskManager() ;
   }

   _omManager::~_omManager()
   {
      if ( NULL != _hostVersion )
      {
         SDB_OSS_DEL _hostVersion ;
         _hostVersion = NULL ;
      }

      if ( NULL != _taskManager )
      {
         SDB_OSS_DEL _taskManager ;
         _taskManager = NULL ;
      }
   }

   INT32 _omManager::init ()
   {
      INT32 rc           = SDB_OK ;

      _pKrcb  = pmdGetKRCB() ;
      _pDmsCB = _pKrcb->getDMSCB() ;
      _pRtnCB = _pKrcb->getRTNCB() ;

      _pmdOptionsMgr *pOptMgr = _pKrcb->getOptionCB() ;

      _wwwRootPath = pmdGetOptionCB()->getWWWPath() ;

      sdbGetPMDController()->setRSManager( &_rsManager ) ;

      rc = _rsManager.init( getRouteAgent() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init remote session manager, rc: %d",
                   rc ) ;

      rc = _initOmTables();
      PD_RC_CHECK ( rc, PDERROR, "Failed to initial the om tables rc = %d", 
                    rc ) ;

      rc = _createJobs() ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to create jobs:rc=%d", 
                    rc ) ;

      rc = refreshVersions() ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to update cluster version:rc=%d", 
                    rc ) ;

      _readAgentPort() ;

      _myNodeID.value             = MSG_INVALID_ROUTEID ;
      _myNodeID.columns.serviceID = MSG_ROUTE_LOCAL_SERVICE ;
      _netAgent.updateRoute( _myNodeID, _pKrcb->getHostName(), 
                             pOptMgr->getOMService() ) ;
      rc = _netAgent.listen( _myNodeID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Create listen failed:host=%s,port=%s", 
                  _pKrcb->getHostName(), pOptMgr->getOMService() ) ;
         goto error ;
      }

      PD_LOG ( PDEVENT, "Create listen success:host=%s,port=%s",
               _pKrcb->getHostName(), pOptMgr->getOMService() ) ;

   done:
      return rc;
   error:
      goto done;

   }

   INT32 _omManager::_createJobs()
   {
      INT32 rc                = SDB_OK ;
      BOOLEAN returnResult    = FALSE ;
      _rtnBaseJob *pJob       = NULL ;
      EDUID jobID             = PMD_INVALID_EDUID ;
      pJob = SDB_OSS_NEW omHostNotifierJob( this, _hostVersion ) ;
      if ( !pJob )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "failed to create omHostNotifierJob:rc=%d", rc ) ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, &jobID,
                                     returnResult ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "create omHostNotifierJob failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omManager::refreshVersions()
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObjBuilder builder ;
      SINT64 contextID = -1 ;

      BSONObjBuilder resultBuilder ;
      BSONObj result ;
      pmdKRCB *pKrcb     = pmdGetKRCB() ;
      _SDB_DMSCB *pDMSCB = pKrcb->getDMSCB() ;
      _SDB_RTNCB *pRTNCB = pKrcb->getRTNCB() ;
      _pmdEDUCB *pEDUCB  = pmdGetThreadEDUCB() ;

      selector = BSON( OM_CLUSTER_FIELD_NAME << "" ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CLUSTER, selector, matcher, order, hint, 0, 
                     pEDUCB, 0, -1, pDMSCB, pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG( PDERROR, "fail to query table:%s,rc=%d", 
                 OM_CS_DEPLOY_CL_CLUSTER, rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore( contextID, 1, buffObj, pEDUCB, pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            PD_LOG( PDERROR, "failed to get record from table:%s,rc=%d", 
                    OM_CS_DEPLOY_CL_CLUSTER, rc ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         string clusterName = record.getStringField( OM_CLUSTER_FIELD_NAME ) ;
         _hostVersion->incVersion( clusterName ) ;
      }
   done:
      return rc ;
   error:
      if ( -1 != contextID )
      {
         pRTNCB->contextDelete( contextID, pEDUCB ) ;
      }
      goto done ;
   }

   void _omManager::updateClusterVersion( string cluster )
   {
      _hostVersion->incVersion( cluster ) ;
   }

   void _omManager::removeClusterVersion( string cluster )
   {
      _hostVersion->removeVersion( cluster ) ;
   }

   omTaskManager *_omManager::getTaskManager()
   {
      return _taskManager ;
   }

   INT32 _omManager::_initOmTables() 
   {
      _pmdEDUCB *cb       = NULL ;
      INT32 rc            = SDB_OK ;
      BSONObjBuilder bsonBuilder ;
      SDB_AUTHCB *pAuthCB = NULL ;

      cb = pmdGetThreadEDUCB() ;

      rc = _createCollection ( OM_CS_DEPLOY_CL_CLUSTER, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createCollectionIndex ( OM_CS_DEPLOY_CL_CLUSTER,
                                    OM_CS_DEPLOY_CL_CLUSTERIDX1, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      rc = _createCollection ( OM_CS_DEPLOY_CL_HOST, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createCollectionIndex ( OM_CS_DEPLOY_CL_HOST,
                                    OM_CS_DEPLOY_CL_HOSTIDX1, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createCollectionIndex ( OM_CS_DEPLOY_CL_HOST,
                                    OM_CS_DEPLOY_CL_HOSTIDX2, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      rc = _createCollection ( OM_CS_DEPLOY_CL_BUSINESS, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createCollectionIndex ( OM_CS_DEPLOY_CL_BUSINESS,
                             OM_CS_DEPLOY_CL_BUSINESSIDX1, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      rc = _createCollection ( OM_CS_DEPLOY_CL_CONFIGURE, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      rc = _createCollection ( OM_CS_DEPLOY_CL_TASKINFO, cb ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _createCollectionIndex ( OM_CS_DEPLOY_CL_TASKINFO,
                             OM_CS_DEPLOY_CL_TASKINFOIDX1, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      pAuthCB = pmdGetKRCB()->getAuthCB() ;
      pAuthCB->checkNeedAuth( cb, TRUE ) ;
      if ( !pAuthCB->needAuthenticate() )
      {
         md5::md5digest digest ;
         BSONObj obj ;
         bsonBuilder.append( SDB_AUTH_USER, OM_DEFAULT_LOGIN_USER ) ;
         md5::md5( ( const void * )OM_DEFAULT_LOGIN_PASSWD, 
                   ossStrlen( OM_DEFAULT_LOGIN_PASSWD ), digest) ;
         bsonBuilder.append( SDB_AUTH_PASSWD, md5::digestToString( digest ) ) ;
         obj = bsonBuilder.obj() ;
         rc = pAuthCB->createUsr( obj, cb ) ;
         if ( SDB_IXM_DUP_KEY == rc )
         {
            rc = SDB_OK ;
         }
         PD_RC_CHECK ( rc, PDERROR, "Failed to create default user:rc = %d",
                       rc ) ;
      }
      pAuthCB->checkNeedAuth( cb, TRUE ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omManager::_createCollectionIndex ( const CHAR *pCollection,
                                              const CHAR *pIndex,
                                              pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj indexDef ;

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
      return rc ;
   error :
      goto done ;
   }

   INT32 _omManager::_createCollection ( const CHAR *pCollection, pmdEDUCB *cb )
   {
      return catTestAndCreateCL( pCollection, cb, _pDmsCB, NULL, TRUE ) ;
   }

   INT32 _omManager::active ()
   {
      INT32 rc = SDB_OK ;
      pmdEDUMgr *pEDUMgr = pmdGetKRCB()->getEDUMgr() ;
      EDUID eduID = PMD_INVALID_EDUID ;

      pmdSetPrimary( TRUE ) ;

      rc = pEDUMgr->startEDU( EDU_TYPE_OMMGR, (_pmdObjBase*)this, &eduID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to start OM Manager edu, rc: %d", rc ) ;
      pEDUMgr->regSystemEDU( EDU_TYPE_OMMGR, eduID ) ;
      rc = _attachEvent.wait( OM_WAIT_CB_ATTACH_TIMEOUT ) ;
      PD_RC_CHECK( rc, PDERROR, "Wait OM Manager edu attach failed, rc: %d",
                   rc ) ;

      rc = pEDUMgr->startEDU( EDU_TYPE_OMNET, (netRouteAgent*)&_netAgent,
                              &eduID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to start om net, rc: %d", rc ) ;
      pEDUMgr->regSystemEDU( EDU_TYPE_OMNET, eduID ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omManager::deactive ()
   {
      _netAgent.closeListen() ;
      _netAgent.stop() ;

      return SDB_OK ;
   }

   INT32 _omManager::fini ()
   {
      _rsManager.fini() ;

      _mapID2Host.clear() ;
      _mapHost2ID.clear() ;

      return SDB_OK ;
   }

   void _omManager::attachCB( _pmdEDUCB *cb )
   {
      _rsManager.registerEDU( cb ) ;
      _msgHandler.attach( cb ) ;
      _timerHandler.attach( cb ) ;
      _attachEvent.signalAll() ;
   }

   void _omManager::detachCB( _pmdEDUCB *cb )
   {
      _msgHandler.detach() ;
      _timerHandler.detach() ;
      _rsManager.unregEUD( cb ) ;
   }

   UINT32 _omManager::setTimer( UINT32 milliSec )
   {
      UINT32 timeID = NET_INVALID_TIMER_ID ;
      _netAgent.addTimer( milliSec, &_timerHandler, timeID ) ;
      return timeID ;
   }

   void _omManager::killTimer( UINT32 timerID )
   {
      _netAgent.removeTimer( timerID ) ;
   }

   void _omManager::onTimer( UINT64 timerID, UINT32 interval )
   {
   }

   INT32 _omManager::authenticate( BSONObj &obj, pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      SDB_AUTHCB *pAuthCB = pmdGetKRCB()->getAuthCB() ;

      if ( !pAuthCB || !pAuthCB->needAuthenticate() )
      {
         goto done ;
      }

      rc = pAuthCB->authenticate( obj, cb ) ;
      if ( rc )
      {
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omManager::authUpdatePasswd( string user, string oldPasswd,
                                       string newPasswd, pmdEDUCB *cb )
   {
      INT32 rc            = SDB_OK ;
      SDB_AUTHCB *pAuthCB = pmdGetKRCB()->getAuthCB() ;
      if ( NULL == pAuthCB )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = pAuthCB->updatePasswd( user, oldPasswd, newPasswd, cb ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   netRouteAgent* _omManager::getRouteAgent()
   {
      return &_netAgent ;
   }

   MsgRouteID _omManager::_incNodeID()
   {
      ++_hwRouteID.columns.nodeID ;
      if ( 0 == _hwRouteID.columns.nodeID )
      {
         _hwRouteID.columns.nodeID = 1 ;
         ++_hwRouteID.columns.groupID ;
      }
      return _hwRouteID ;
   }

   MsgRouteID _omManager::updateAgentInfo( const string &host,
                                           const string &service )
   {
      MsgRouteID nodeID ;
      ossScopedLock lock( &_omLatch, EXCLUSIVE ) ;
      MAP_HOST2ID_IT it = _mapHost2ID.find( host ) ;
      if ( it != _mapHost2ID.end() )
      {
         omAgentInfo &info = it->second ;
         nodeID.value = info._id ;
         _netAgent.updateRoute( nodeID, host.c_str(), service.c_str() ) ;
         info._host = host ;
         info._service = service ;
      }
      else
      {
         nodeID = _incNodeID() ;
         omAgentInfo &info = _mapHost2ID[ host ] ;
         info._id = nodeID.value ;
         info._host = host ;
         info._service = service ;
         _mapID2Host[ info._id ] = &info ;
         _netAgent.updateRoute( nodeID, host.c_str(), service.c_str() ) ;
      }

      return nodeID ;
   }

   MsgRouteID _omManager::getAgentIDByHost( const string &host )
   {
      MsgRouteID nodeID ;
      nodeID.value = MSG_INVALID_ROUTEID ;
      ossScopedLock lock( &_omLatch, SHARED ) ;
      MAP_HOST2ID_IT it = _mapHost2ID.find( host ) ;
      if ( it != _mapHost2ID.end() )
      {
         nodeID.value = it->second._id ;
      }
      return nodeID ;
   }

   INT32 _omManager::getHostInfoByID( MsgRouteID routeID, string &host,
                                      string &service )
   {
      INT32 rc = SDB_OK ;
      omAgentInfo *pInfo = NULL ;

      ossScopedLock lock( &_omLatch, SHARED ) ;
      MAP_ID2HOSTPTR_IT it = _mapID2Host.find( routeID.value ) ;
      if ( it != _mapID2Host.end() )
      {
         pInfo = it->second ;
         host = pInfo->_host ;
         service = pInfo->_service ;
      }
      else
      {
         rc = SDB_CLS_NODE_NOT_EXIST ;
      }

      return rc ;
   }

   void _omManager::delAgent( const string &host )
   {
      ossScopedLock lock( &_omLatch, EXCLUSIVE ) ;
      MAP_HOST2ID_IT it = _mapHost2ID.find( host ) ;
      if ( it != _mapHost2ID.end() )
      {
         MsgRouteID nodeID ;
         nodeID.value = it->second._id ;
         _mapID2Host.erase( it->second._id ) ;
         _netAgent.delRoute( nodeID ) ;
         _mapHost2ID.erase( it ) ;
      }
   }

   void _omManager::delAgent( MsgRouteID routeID )
   {
      ossScopedLock lock( &_omLatch, EXCLUSIVE ) ;
      MAP_ID2HOSTPTR_IT it = _mapID2Host.find( routeID.value ) ;
      if ( it != _mapID2Host.end() )
      {
         MsgRouteID nodeID ;
         nodeID.value = it->first ;
         string host = it->second->_host ;
         _netAgent.delRoute( nodeID ) ;
         _mapID2Host.erase( it ) ;
         _mapHost2ID.erase( host ) ;
      }
   }

   string _omManager::getLocalAgentPort()
   {
      return _localAgentPort ;
   }

   void _omManager::_readAgentPort()
   {
      INT32 rc = SDB_OK ;
      CHAR conf[OSS_MAX_PATHSIZE + 1] = { 0 } ;
      po::options_description desc ( "Config options" ) ;
      po::variables_map vm ;
      CHAR hostport[OSS_MAX_HOSTNAME + 6] = { 0 } ;
      _localAgentPort = boost::lexical_cast<string>( SDBCM_DFT_PORT ) ;
      rc = ossGetHostName( hostport, OSS_MAX_HOSTNAME ) ;
      if ( rc != SDB_OK )
      {
         PD_LOG( PDERROR, "get host name failed:rc=%d", rc ) ;
         goto error ;
      }

      ossStrncat ( hostport, SDBCM_CONF_PORT, ossStrlen(SDBCM_CONF_PORT) ) ;

      desc.add_options()
         (SDBCM_CONF_DFTPORT, po::value<string>(), "sdbcm default "
         "listening port")
         (hostport, po::value<string>(), "sdbcm specified listening port")
      ;

      rc = ossGetEWD ( conf, OSS_MAX_PATHSIZE ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get excutable file's working "
                  "directory" ) ;
         goto error ;
      }

      if ( ( ossStrlen ( conf ) + ossStrlen ( SDBCM_CONF_PATH_FILE ) + 2 ) >
           OSS_MAX_PATHSIZE )
      {
         PD_LOG ( PDERROR, "Working directory too long" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      ossStrncat( conf, OSS_FILE_SEP, 1 );
      ossStrncat( conf, SDBCM_CONF_PATH_FILE,
                  ossStrlen( SDBCM_CONF_PATH_FILE ) );
      rc = utilReadConfigureFile ( conf, desc, vm ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to read configure file, rc = %d", rc ) ;
         goto error ;
      }
      else if ( vm.count( hostport ) )
      {
         _localAgentPort = vm[hostport].as<string>() ;
      }
      else if ( vm.count( SDBCM_CONF_DFTPORT ) )
      {
         _localAgentPort = vm[SDBCM_CONF_DFTPORT].as<string>() ;
      }
      else
      {
         _localAgentPort = boost::lexical_cast<string>( SDBCM_DFT_PORT ) ;
      }

   done:
      return ;
   error:
      goto done ;
   }

   BOOLEAN _omManager::_isCommand( const CHAR *pCheckName )
   {
      if ( pCheckName && '$' == pCheckName[0] )
      {
         return TRUE ;
      }

      return FALSE ;
   }

   void _omManager::_sendRes2Agent( NET_HANDLE handle, MsgHeader *pSrcMsg, 
                                    INT32 flag, rtnContextBuf &buffObj )
   {

      MsgOpReply reply ;
      INT32 rc                   = SDB_OK ;
      const CHAR *pBody          = buffObj.data() ;
      INT32 bodyLen              = buffObj.size() ;
      reply.header.messageLength = sizeof( MsgOpReply ) + bodyLen ;
      reply.header.opCode        = MAKE_REPLY_TYPE( pSrcMsg->opCode ) ;
      reply.header.TID           = pSrcMsg->TID ;
      reply.header.routeID.value = 0 ;
      reply.header.requestID     = pSrcMsg->requestID ;
      reply.contextID            = -1 ;
      reply.flags                = flag ;
      reply.startFrom            = 0 ;
      reply.numReturned          = buffObj.recordNum() ;

      if ( bodyLen > 0 )
      {
         rc = _netAgent.syncSend ( handle, (MsgHeader *)( &reply ),
                                   (void*)pBody, bodyLen ) ;
      }
      else
      {
         rc = _netAgent.syncSend ( handle, (void *)( &reply ) ) ;
      }

      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "send response to agent failed:rc=%d", rc ) ;
      }
   }

   void _omManager::_sendRes2Agent( NET_HANDLE handle, MsgHeader *pSrcMsg, 
                                    INT32 flag, BSONObj &obj )
   {

      MsgOpReply reply ;
      INT32 rc                   = SDB_OK ;
      const CHAR *pBody          = obj.objdata() ;
      INT32 bodyLen              = obj.objsize() ;
      reply.header.messageLength = sizeof( MsgOpReply ) + bodyLen ;
      reply.header.opCode        = MAKE_REPLY_TYPE( pSrcMsg->opCode ) ;
      reply.header.TID           = pSrcMsg->TID ;
      reply.header.routeID.value = 0 ;
      reply.header.requestID     = pSrcMsg->requestID ;
      reply.contextID            = -1 ;
      reply.flags                = flag ;
      reply.startFrom            = 0 ;
      reply.numReturned          = 1 ;

      rc = _netAgent.syncSend ( handle, (MsgHeader *)( &reply ),
                                (void*)pBody, bodyLen ) ;
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "send response to agent failed:rc=%d", rc ) ;
      }
   }

   INT32 _omManager::_onAgentUpdateTaskReq( NET_HANDLE handle, MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      INT32 flags               = 0 ;
      CHAR *pCollectionName     = NULL ;
      CHAR *pQuery              = NULL ;
      CHAR *pFieldSelector      = NULL ;
      CHAR *pOrderByBuffer      = NULL ;
      CHAR *pHintBuffer         = NULL ;
      SINT64 numToSkip          = -1 ;
      SINT64 numToReturn        = -1 ;
      BSONObj response ;

      rc = msgExtractQuery ( (CHAR *)pMsg, &flags, &pCollectionName,
                             &numToSkip, &numToReturn, &pQuery,
                             &pFieldSelector, &pOrderByBuffer, &pHintBuffer ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "extract omAgent's command msg failed:rc=%d", 
                     rc ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      PD_LOG( PDEVENT, "receive agent's command:%s", pCollectionName ) ;
      if ( _isCommand( pCollectionName ) )
      {
         if ( ossStrcasecmp( OM_AGENT_UPDATE_TASK, 
                                            ( pCollectionName + 1 ) ) == 0 )
         {
            BSONObj updateReq( pQuery ) ;
            BSONObj taskUpdateInfo ;
            INT64 taskID ;

            BSONElement ele = updateReq.getField( OM_TASKINFO_FIELD_TASKID ) ;
            taskID = ele.numberLong() ;

            BSONObj filter = BSON( OM_TASKINFO_FIELD_TASKID << 1 ) ;
            taskUpdateInfo = updateReq.filterFieldsUndotted( filter, false ) ;
            rc = _taskManager->updateTask( taskID, taskUpdateInfo) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "update task failed:rc=%d", rc ) ;
               goto error ;
            }
         }
         else
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "unreconigzed agent request:command=%s", 
                        pCollectionName ) ;
            goto error ;
         }
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "unreconigzed agent request:command=%s", 
                     pCollectionName ) ;
         goto error ;
      }

   done:

      if ( SDB_OK == rc )
      {
         _sendRes2Agent( handle, pMsg, rc, response ) ;
      }
      else
      {
         string errorInfo = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         response = BSON( OP_ERR_DETAIL << errorInfo ) ;
         _sendRes2Agent( handle, pMsg, rc, response ) ;
      }

      return rc ;
   error:
      goto done ;
   }

   INT32 _omManager::_onAgentQueryTaskReq( NET_HANDLE handle, MsgHeader *pMsg )
   {
      INT32 rc = SDB_OK ;
      INT32 flags               = 0 ;
      CHAR *pCollectionName     = NULL ;
      CHAR *pQuery              = NULL ;
      CHAR *pFieldSelector      = NULL ;
      CHAR *pOrderByBuffer      = NULL ;
      CHAR *pHintBuffer         = NULL ;
      SINT64 numToSkip          = -1 ;
      SINT64 numToReturn        = -1 ;
      BSONObj response ;
      rc = msgExtractQuery ( (CHAR *)pMsg, &flags, &pCollectionName,
                             &numToSkip, &numToReturn, &pQuery,
                             &pFieldSelector, &pOrderByBuffer, &pHintBuffer ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "extract omAgent's command msg failed:rc=%d", 
                     rc ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      PD_LOG( PDEVENT, "receive agent's command:%s", pCollectionName ) ;
      try
      {
         BSONObj matcher( pQuery ) ;
         BSONObj selector( pFieldSelector ) ;
         BSONObj orderBy( pOrderByBuffer ) ;
         BSONObj hint( pHintBuffer ) ;
         PD_LOG ( PDDEBUG, "Query: matcher: %s\nselector: "
                  "%s\norderBy: %s\nhint:%s",
                  matcher.toString().c_str(), selector.toString().c_str(),
                  orderBy.toString().c_str(), hint.toString().c_str() ) ;

         rc = _taskManager->queryOneTask( selector, matcher, orderBy, hint, 
                                          response ) ;
         if( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "query task failed:rc=%d", rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create matcher and "
                     "selector for QUERY: %s", e.what () ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:

      if ( SDB_OK == rc )
      {
         _sendRes2Agent( handle, pMsg, rc, response ) ;
      }
      else
      {
         string errorInfo = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         response = BSON( OP_ERR_DETAIL << errorInfo ) ;
         _sendRes2Agent( handle, pMsg, rc, response ) ;
      }

      return rc ;
   error:
      goto done ;
   }

   /*
      get the global om manager object point
   */
   omManager* sdbGetOMManager()
   {
      static omManager s_omManager ;
      return &s_omManager ;
   }

}


