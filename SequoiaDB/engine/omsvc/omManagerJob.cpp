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

   Source File Name = omManagerJob.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/21/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omManagerJob.hpp"
#include "omDef.hpp"
#include "rtn.hpp"
#include "ossProc.hpp"
#include "msgMessage.hpp"
#include "../bson/bson.h"
#include <string>
#include <vector>

using namespace bson ;

namespace engine
{
   omHostVersion::omHostVersion()
   {
   }

   omHostVersion::~omHostVersion()
   {
   }

   void omHostVersion::incVersion( string clusterName )
   {
      _lock.get() ;

      _MAP_CV_ITER iter = _mapClusterVersion.find( clusterName ) ;
      if ( iter == _mapClusterVersion.end() )
      {
         _mapClusterVersion.insert( _MAP_CV_VALUETYPE( clusterName, 1 ) ) ;
      }
      else
      {
         iter->second = iter->second + 1 ;
      }

      _lock.release() ;
   }

   void omHostVersion::removeVersion( string clusterName )
   {
      _lock.get() ;
      _mapClusterVersion.erase( clusterName ) ;
      _lock.release() ;
   }

   void omHostVersion::getVersionMap( map<string, UINT32> &mapClusterVersion )
   {
      mapClusterVersion.clear() ;
      _lock.get() ;

      _MAP_CV_ITER iter = _mapClusterVersion.begin() ;
      if ( iter != _mapClusterVersion.end() )
      {
         mapClusterVersion.insert( _MAP_CV_VALUETYPE ( iter->first, 
                                                       iter->second ) ) ;
         iter++ ;
      }

      _lock.release() ;
   }

   UINT32 omHostVersion::getVersion( string clusterName )
   {
      UINT32 tmp = 0 ;

      _lock.get() ;
      _MAP_CV_ITER iter = _mapClusterVersion.find( clusterName ) ;
      if ( iter != _mapClusterVersion.end() )
      {
         tmp = iter->second ;
      }

      _lock.release() ;

      return tmp ;
   }

   omClusterNotifier::omClusterNotifier( pmdEDUCB *cb, omManager *om, 
                                         string clusterName )
                     :_cb( cb ), _om( om ), _clusterName( clusterName ),
                      _version( 0 )
   {
   }

   omClusterNotifier::~omClusterNotifier()
   {
   }

   INT32 omClusterNotifier::notify( UINT32 newVersion )
   {
      INT32 rc = SDB_OK ;
      if ( _version != newVersion )
      {
         rc = _updateNotifier() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "update notifier failed:rc=%d", rc ) ;
            goto error ;
         }

         _version = newVersion ;       
      }

      _notifyAgent() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   void omClusterNotifier::_getAgentService( string &serviceName )
   {
      INT32 rc = SDB_OK ;
      CHAR conf[OSS_MAX_PATHSIZE + 1] = { 0 } ;
      po::options_description desc ( "Config options" ) ;
      po::variables_map vm ;
      CHAR hostport[OSS_MAX_HOSTNAME + 6] = { 0 } ;
      serviceName = boost::lexical_cast<string>( SDBCM_DFT_PORT ) ;
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
         serviceName = vm[hostport].as<string>() ;
      }
      else if ( vm.count( SDBCM_CONF_DFTPORT ) )
      {
         serviceName = vm[SDBCM_CONF_DFTPORT].as<string>() ;
      }
      else
      {
         serviceName = boost::lexical_cast<string>( SDBCM_DFT_PORT ) ;
      }

   done:
      return ;
   error:
      goto done ;
   }

   INT32 omClusterNotifier::_updateNotifier()
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

      matcher  = BSON( OM_HOST_FIELD_CLUSTERNAME << _clusterName ) ;
      selector = BSON( OM_HOST_FIELD_NAME << ""
                       << OM_HOST_FIELD_IP << "" 
                       << OM_HOST_FIELD_USER << ""
                       << OM_HOST_FIELD_PASSWORD << ""
                       << OM_HOST_FIELD_AGENT_PORT << "" ) ;

      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 0, 
                     _cb, 0, -1, pDMSCB, pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG( PDERROR, "fail to query table:%s,rc=%d", OM_CS_DEPLOY_CL_HOST, 
                 rc ) ;
         goto error ;
      }

      _mapTargetAgents.clear() ;
      _vHostTable.clear() ;
      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            PD_LOG( PDERROR, "failed to get record from table:%s,rc=%d", 
                    OM_CS_DEPLOY_CL_HOST, rc ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         omHostContent tmp ;
         tmp.hostName    = record.getStringField( OM_HOST_FIELD_NAME ) ;
         tmp.ip          = record.getStringField( OM_HOST_FIELD_IP ) ;
         tmp.serviceName = record.getStringField( OM_HOST_FIELD_AGENT_PORT ) ;
         tmp.user        = record.getStringField( OM_HOST_FIELD_USER ) ;
         tmp.passwd      = record.getStringField( OM_HOST_FIELD_PASSWORD ) ;

         _vHostTable.push_back( tmp ) ;
         _mapTargetAgents.insert( _MAPAGENT_VALUE( tmp.hostName, tmp ) ) ;
      }

      if ( _mapTargetAgents.size() > 0 )
      {
         CHAR localHost[ OSS_MAX_HOSTNAME + 1 ] ;
         ossGetHostName( localHost, OSS_MAX_HOSTNAME ) ;
         _MAPAGENT_ITER iter = _mapTargetAgents.find( localHost ) ;
         if ( iter == _mapTargetAgents.end() )
         {
            omHostContent content ;
            content.hostName = localHost ;
            content.ip       = "127.0.0.1" ;
            string serviceName ;
            _getAgentService( serviceName ) ;
            content.serviceName = serviceName ;
            content.user        = "" ;
            content.passwd      = "" ;

            _mapTargetAgents.insert( 
                                _MAPAGENT_VALUE( content.hostName, content ) ) ;
         }
      }
   done:
      return rc ;
   error:
      if ( -1 != contextID )
      {
         pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      goto done ;
   }

   INT32 omClusterNotifier::_notifyAgent()
   {
      INT32 rc       = SDB_OK ;
      INT32 sucNum   = 0 ;
      INT32 totalNum = 0 ;
      pmdRemoteSession *remoteSession = NULL ;

      VEC_SUB_SESSIONPTR subSessionVec ;

      if ( _mapTargetAgents.size() == 0 )
      {
         goto done ;
      }

      remoteSession = _om->getRSManager()->addSession( _cb, 
                                                   OM_WAIT_UPDATE_HOST_INTERVAL,
                                                   NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "create remote session failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = _addUpdateHostReq( remoteSession ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_addUpdateHostReq failed:rc=%d", rc ) ;
         goto error ;
      }

      remoteSession->sendMsg( &sucNum, &totalNum ) ;
      rc = remoteSession->waitReply( TRUE, &subSessionVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "wait replay failed:rc=%d", rc ) ;
         goto error ;
      }

      for ( UINT32 i = 0 ; i < subSessionVec.size() ; i++ )
      {
         vector<BSONObj> objVec ;
         INT32 innerRC             = SDB_OK ;
         SINT32 flag               = SDB_OK ;
         SINT64 contextID          = -1 ;
         SINT32 startFrom          = 0 ;
         SINT32 numReturned        = 0 ;
         MsgHeader* pRspMsg        = NULL ;
         BSONObj result ;
         pmdSubSession *subSession = subSessionVec[i] ;
         if ( subSession->isDisconnect() )
         {
            PD_LOG(PDERROR, "session disconnected:id=%s", 
                   routeID2String(subSession->getNodeID()).c_str() ) ;
            continue ;
         }

         pRspMsg = subSession->getRspMsg() ;
         if ( NULL == pRspMsg )
         {
            PD_LOG(PDERROR, "unexpected result" ) ;
            continue ;
         }

         innerRC = msgExtractReply( (CHAR *)pRspMsg, &flag, &contextID, 
                                    &startFrom, &numReturned, objVec ) ;
         if ( SDB_OK != innerRC )
         {
            PD_LOG( PDERROR, "extract reply failed" ) ;
            continue ;
         }

         if ( SDB_OK != flag )
         {
            string detail ;
            if ( objVec.size() > 0 )
            {
               detail = objVec[0].getStringField( OP_ERR_DETAIL ) ;
            }
            PD_LOG( PDERROR, "agent process failed:detail=%s", 
                    detail.c_str() ) ;
            continue ;
         }

         if ( 1 != objVec.size() )
         {
            PD_LOG( PDERROR, "unexpected response size:size=%d", 
                    objVec.size() ) ;
            continue ;
         }

         {
            string host ;
            string service ;
            _om->getHostInfoByID( subSession->getNodeID(), host, service ) ;
            _mapTargetAgents.erase( host ) ;
         }
      }

   done:
      _clearSession( remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omClusterNotifier::_addUpdateHostReq( pmdRemoteSession *remoteSession )
   {
      INT32 rc    = SDB_OK ;
      BSONArrayBuilder arrayBuilder ;
      BSONArray hosttable ;
      BSONObj request ;
      UINT32 i     = 0 ;
      for ( ; i < _vHostTable.size(); i++ )
      {
         omHostContent &agentInfo = _vHostTable[i] ;
         BSONObj tmp ;
         if ( agentInfo.serviceName 
                              == boost::lexical_cast<string>( SDBCM_DFT_PORT ) )
         {
            tmp = BSON ( OM_BSON_FIELD_HOST_NAME << agentInfo.hostName
                         << OM_BSON_FIELD_HOST_IP << agentInfo.ip ) ;
         }
         else
         {
            tmp = BSON ( OM_BSON_FIELD_HOST_NAME << agentInfo.hostName
                        << OM_BSON_FIELD_HOST_IP << agentInfo.ip
                        << OM_BSON_FIELD_AGENT_PORT << agentInfo.serviceName ) ;
         }

         arrayBuilder.append( tmp ) ;
      }

      hosttable = arrayBuilder.arr() ;

      _MAPAGENT_ITER iter = _mapTargetAgents.begin() ;
      while ( iter != _mapTargetAgents.end() )
      {
         MsgRouteID routeID ;
         pmdSubSession *subSession = NULL ;
         CHAR *pContent            = NULL ;
         INT32 contentSize         = 0 ;
         omHostContent &agentInfo  = iter->second ;
         routeID   = _om->updateAgentInfo( agentInfo.hostName, 
                                           agentInfo.serviceName ) ;
         subSession = remoteSession->addSubSession( routeID.value ) ;
         if ( NULL == subSession )
         {
            rc = SDB_OOM ;
            PD_LOG( PDERROR, "addSubSessin failed" ) ;
            goto error ;
         }

         request = BSON( OM_BSON_FIELD_HOST_NAME << agentInfo.hostName
                         << OM_BSON_FIELD_HOST_IP << agentInfo.ip
                         << OM_BSON_FIELD_HOST_USER << agentInfo.user
                         << OM_BSON_FIELD_HOST_PASSWD << agentInfo.passwd
                         << OM_BSON_FIELD_HOST_INFO << hosttable ) ;
         rc = msgBuildQueryMsg( &pContent, &contentSize, 
                                CMD_ADMIN_PREFIX OM_UPDATE_HOSTNAME_REQ,
                                0, 0, 0, -1, &request, NULL, NULL, NULL ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "msgBuildQueryMsg failed:rc=%d", rc ) ;
            goto error ;
         }

         subSession->setReqMsg( (MsgHeader *)pContent ) ;
         iter++ ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   void omClusterNotifier::_clearSession( pmdRemoteSession *remoteSession )
   {
      if ( NULL != remoteSession )
      {
         pmdSubSession *pSubSession = NULL ;
         pmdSubSessionItr itr       = remoteSession->getSubSessionItr() ;
         while ( itr.more() )
         {
            pSubSession = itr.next() ;
            MsgHeader *pMsg = pSubSession->getReqMsg() ;
            if ( NULL != pMsg )
            {
               SDB_OSS_FREE( pMsg ) ;
            }
         }

         remoteSession->clearSubSession() ;
         _om->getRSManager()->removeSession( remoteSession ) ;
      }
   }

   omHostNotifierJob::omHostNotifierJob( omManager *om, omHostVersion *version )
                     :_om( om ), _shareVersion( version )
   {
   }

   omHostNotifierJob::~omHostNotifierJob()
   {
   }

   RTN_JOB_TYPE omHostNotifierJob::type() const
   {
      return RTN_JOB_MAX ;
   }

   const CHAR* omHostNotifierJob::name() const
   {
      return "omHostNotifierJob" ;
   }

   BOOLEAN omHostNotifierJob::muteXOn( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   void omHostNotifierJob::_checkUpdateCluster(
                                        map<string, UINT32> &mapClusterVersion )
   {
      map<string, UINT32>::iterator iter = mapClusterVersion.begin() ;
      while ( iter != mapClusterVersion.end() )
      {
         string clusterName = iter->first ;
         UINT32 version     = iter->second ;
         _MAP_CLUSTER_ITER clusterIter = _mapClusters.find( clusterName ) ;
         if ( clusterIter == _mapClusters.end() )
         {
            omClusterNotifier *notifier = NULL ;
            notifier = SDB_OSS_NEW omClusterNotifier( eduCB(), _om, 
                                                      clusterName ) ;
            _mapClusters.insert( _MAP_CLUSTER_VALUE( clusterName, 
                                                     notifier ) ) ;
            clusterIter = _mapClusters.find( clusterName ) ;
         }

         omClusterNotifier *pNotifier = clusterIter->second ;
         pNotifier->notify( version ) ;

         iter++ ;
      }
   }

   void omHostNotifierJob::_checkDeleteCluster(
                                        map<string, UINT32> &mapClusterVersion )
   {
      _MAP_CLUSTER_ITER clusterIter = _mapClusters.begin() ;
      while ( clusterIter != _mapClusters.end() )
      {
         string clusterName            = clusterIter->first ;
         omClusterNotifier *notifier   = clusterIter->second ;

         map<string, UINT32>::iterator iter ;
         iter = mapClusterVersion.find( clusterName ) ;
         if ( iter == mapClusterVersion.end() )
         {
            _mapClusters.erase( clusterIter++ ) ;
            SDB_OSS_DEL notifier ;
            continue ;
         }

         clusterIter++ ;
      }
   }

   INT32 omHostNotifierJob::doit()
   {
      INT32 rc     = SDB_OK ;
      UINT64 count = 0 ;
      _om->getRSManager()->registerEDU( eduCB() ) ;
      while ( TRUE )
      {
         count++ ;
         if ( eduCB()->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }

         if ( count % 10 == 0 )
         {
            map< string, UINT32 > mapClusterVersion ;
            _shareVersion->getVersionMap( mapClusterVersion );

            _checkUpdateCluster( mapClusterVersion ) ;

            _checkDeleteCluster( mapClusterVersion ) ;
         }

         ossSleep( OSS_ONE_SEC ) ;
      }

   done:
      _om->getRSManager()->unregEUD( eduCB() );
      return rc ;
   error:
      goto done ;
   }
}

