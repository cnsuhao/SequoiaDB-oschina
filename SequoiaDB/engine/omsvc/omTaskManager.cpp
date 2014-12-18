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

   Source File Name = omTaskManager.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/12/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omTaskManager.hpp"
#include "omDef.hpp"
#include "rtn.hpp"
#include "msgMessage.hpp"
#include "ossVer.hpp"

using namespace bson ;

namespace engine
{
   #define OM_TASK_INVALID_ID     (-1)

   void taskDeleter( omTaskBase *pTask )
   { 
      SDB_OSS_DEL pTask ;
   }

   omTaskBase::omTaskBase( omManager *om )
              :_om( om )
   {
   }

   omTaskBase::~omTaskBase()
   {
   }

   void omTaskBase::setDetail( string detail )
   {
      _omTaskInfo.detail = detail ;
   }

   string omTaskBase::getDetail()
   {
      return _omTaskInfo.detail ;
   }

   INT32 omTaskBase::_saveFinishTask()
   {
      INT32 rc         = SDB_OK ;
      pmdEDUCB *cb     = pmdGetThreadEDUCB() ;
      BSONObj selector ;
      selector = BSON( OM_TASKINFO_FIELD_TASKID 
                       << ( long long )_omTaskInfo.taskID ) ;

      BSONObj tmp ;
      BSONObj updator ;
      tmp     = BSON( OM_TASKINFO_FIELD_PROGRESS << _omTaskInfo.progress
                      << OM_TASKINFO_FIELD_STATUS << _omTaskInfo.taskStatus
                      << OM_TASKINFO_FIELD_DETAIL << _omTaskInfo.detail
                      << OM_TASKINFO_FIELD_ISFINISH << true ) ;
      updator = BSON( "$set" << tmp ) ;

      BSONObj hint ;
      rc = rtnUpdate( OM_CS_DEPLOY_CL_TASKINFO, selector, updator, hint,
                         0, cb ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to delete taskinfo from table:%s,"
                     "taskID="OSS_LL_PRINT_FORMAT",rc=%d", 
                     OM_CS_DEPLOY_CL_TASKINFO, _omTaskInfo.taskID, rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskBase::_getProgressFromAgent( INT32 &flag, BSONObj &response )
   {
      BSONObj result ;
      INT32 rc          = SDB_OK ;
      _pmdEDUCB *cb     = pmdGetThreadEDUCB() ;
      MsgHeader *pMsg   = NULL ;
      CHAR* pContent    = NULL ;
      INT32 contentSize = 0 ;
      pmdRemoteSession *remoteSession = NULL ;
      BSONObjBuilder builder ;
      BSONObj request ;
      builder.append( OM_BSON_TASKID, ( long long )_omTaskInfo.taskID ) ;
      request = builder.obj() ;
      rc = msgBuildQueryMsg( &pContent, &contentSize, 
                             CMD_ADMIN_PREFIX OM_QUERY_PROGRESS, 
                             0, 0, 0, -1, &request, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "build query msg failed:rc=%d", rc ) ;
         goto error ;
      }

      remoteSession = _om->getRSManager()->addSession( cb, 
                                                  OM_WAIT_PROGRESS_RES_INTERVAL,
                                                  NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "addSession failed" ) ;
         SDB_OSS_FREE( pContent ) ;
         goto error ;
      }

      pMsg = (MsgHeader *)pContent ;
      rc   = _sendMsgToAgent( _omTaskInfo.agentHost, _omTaskInfo.agentService,  
                              remoteSession, pMsg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "send message to agent failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         remoteSession->clearSubSession() ;
         goto error ;
      }

      rc = _receiveFromAgent( remoteSession, flag, result ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "receive from agent failed:rc=%d", rc ) ;
         goto error ;
      }

      PD_LOG( PDEVENT, "receive from agent:%s", result.toString().c_str() ) ;

      response = result.copy() ;
   done:
      _clearSession( cb, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskBase::_sendMsgToAgent( const string &host, const string &port,
                                      pmdRemoteSession *remoteSession, 
                                      MsgHeader *pMsg )
   {
      MsgRouteID localAgentID ;
      INT32 rc = SDB_OK ;

      localAgentID = _om->updateAgentInfo( host, port ) ;
      if ( NULL == remoteSession->addSubSession( localAgentID.value ) )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "addSubSession failed:id=%ld", localAgentID.value ) ;
         goto error ;
      }

      rc = remoteSession->sendMsg( pMsg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "send msg to localhost's agent failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskBase::_receiveFromAgent( pmdRemoteSession *remoteSession,
                                        SINT32 &flag, BSONObj &result )
   {
      VEC_SUB_SESSIONPTR subSessionVec ;
      INT32 rc           = SDB_OK ;
      MsgHeader *pRspMsg = NULL ;
      SINT64 contextID   = -1 ;
      SINT32 startFrom   = 0 ;
      SINT32 numReturned = 0 ;
      vector<BSONObj> objVec ;

      rc = remoteSession->waitReply( TRUE, &subSessionVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "wait reply failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( 1 != subSessionVec.size() )
      {
         rc = SDB_UNEXPECTED_RESULT ;
         PD_LOG( PDERROR, "unexpected session size:size=%d", 
                 subSessionVec.size() ) ;
         goto error ;
      }

      if ( subSessionVec[0]->isDisconnect() )
      {
         rc = SDB_UNEXPECTED_RESULT ;
         PD_LOG(PDERROR, "session disconnected:id=%s,rc=%d", 
                routeID2String(subSessionVec[0]->getNodeID()).c_str(), rc ) ;
         goto error ;
      }

      pRspMsg = subSessionVec[0]->getRspMsg() ;
      if ( NULL == pRspMsg )
      {
         rc = SDB_UNEXPECTED_RESULT ;
         PD_LOG( PDERROR, "receive null response:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgExtractReply( (CHAR *)pRspMsg, &flag, &contextID, &startFrom, 
                            &numReturned, objVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "extract reply failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( objVec.size() > 0 )
      {
         result = objVec[0] ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omTaskBase::_clearSession( _pmdEDUCB *cb, 
                                   pmdRemoteSession *remoteSession )
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

   omInstallTask::omInstallTask( omManager *om )
                 :omTaskBase( om )
   {
      _omTaskInfo.isEnable   = false ;
      _omTaskInfo.isFinished = false ;
      _omTaskInfo.taskID     = OM_TASK_INVALID_ID ;
      _omTaskInfo.taskType   = OM_INSTALL_BUSINESS_REQ ;
      _omTaskInfo.taskStatus = OM_TASK_STATUS_INSTALL ;
   }

   omInstallTask::~omInstallTask()
   {
   }

   INT32 omInstallTask::restore( BSONObj &record )
   {
      INT32 rc = SDB_OK ;
      BSONElement element ;
      element       = record.getField( OM_TASKINFO_FIELD_TASKID ) ;
      _omTaskInfo.taskID       = element.Long() ;

      _omTaskInfo.agentHost    = record.getStringField( 
                                             OM_TASKINFO_FIELD_AGENTHOST ) ;
      _omTaskInfo.agentService = record.getStringField( 
                                             OM_TASKINFO_FIELD_AGENTPORT ) ;
      _omTaskInfo.taskInfo     = record.getObjectField( 
                                             OM_TASKINFO_FIELD_INFO ).copy() ;
      _omTaskInfo.isFinished   = record.getBoolField( 
                                             OM_TASKINFO_FIELD_ISFINISH ) ;
      _omTaskInfo.isEnable     = record.getBoolField( 
                                             OM_TASKINFO_FIELD_ISENABLE ) ;
      _omTaskInfo.taskStatus   = record.getStringField( 
                                             OM_TASKINFO_FIELD_STATUS ) ;
      _omTaskInfo.taskType     = record.getStringField( 
                                             OM_TASKINFO_FIELD_TYPE ) ;
      _omTaskInfo.detail       = record.getStringField( 
                                             OM_TASKINFO_FIELD_DETAIL ) ;
      _omTaskInfo.progress = record.getObjectField(
                                          OM_TASKINFO_FIELD_PROGRESS ).copy() ;

      if ( !_omTaskInfo.taskInfo.hasField( OM_BSON_BUSINESS_NAME )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_DEPLOY_MOD )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_BUSINESS_TYPE )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_FIELD_CLUSTER_NAME ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "taskInfo error:taskInfo=%s", 
                     _omTaskInfo.taskInfo.toString().c_str() ) ;
         goto error ;
      }

      PD_LOG( PDEVENT, "restore install:%s", record.toString().c_str() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::_insertTask()
   {
      BSONObj tmp ;
      INT32 rc      = SDB_OK ;
      pmdEDUCB *cb  = pmdGetThreadEDUCB() ;
      tmp = BSON( OM_TASKINFO_FIELD_TASKID << ( long long )_omTaskInfo.taskID 
                  << OM_TASKINFO_FIELD_TYPE << _omTaskInfo.taskType
                  << OM_TASKINFO_FIELD_AGENTHOST << _omTaskInfo.agentHost 
                  << OM_TASKINFO_FIELD_AGENTPORT << _omTaskInfo.agentService
                  << OM_TASKINFO_FIELD_INFO << _omTaskInfo.taskInfo 
                  << OM_TASKINFO_FIELD_PROGRESS << _omTaskInfo.progress
                  << OM_TASKINFO_FIELD_STATUS << _omTaskInfo.taskStatus
                  << OM_TASKINFO_FIELD_ISFINISH << _omTaskInfo.isFinished
                  << OM_TASKINFO_FIELD_ISENABLE << _omTaskInfo.isEnable
                  << OM_TASKINFO_FIELD_DETAIL << _omTaskInfo.detail ) ;

      rc = rtnInsert( OM_CS_DEPLOY_CL_TASKINFO, tmp, 1, 0, cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to store taskinfo into table:%s,rc=%d", 
                 OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::init( const string &agentHost, 
                              const string &agentService, const BSONObj &conf, 
                              UINT64 taskID )
   {
      INT32 rc      = SDB_OK ;
      _omTaskInfo.taskID       = taskID ;
      _omTaskInfo.agentHost    = agentHost ;
      _omTaskInfo.agentService = agentService ;
      _omTaskInfo.taskInfo     = conf ;
      _omTaskInfo.isEnable     = false ;
      _omTaskInfo.isFinished   = false ;
      _omTaskInfo.taskType     = OM_INSTALL_BUSINESS_REQ ;
      _omTaskInfo.taskStatus   = OM_TASK_STATUS_INSTALL ;
      PD_LOG( PDDEBUG, "_omTaskInfo.taskInfo:%s", 
              _omTaskInfo.taskInfo.toString().c_str() ) ;

      if ( !_omTaskInfo.taskInfo.hasField( OM_BSON_BUSINESS_NAME )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_DEPLOY_MOD )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_BUSINESS_TYPE )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_FIELD_CLUSTER_NAME ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "install task configure error:conf=%s", 
                     conf.toString().c_str() ) ;
         goto error ;
      }

      rc = _insertTask() ;
      if ( rc )
      {
         PD_LOG( PDERROR, "failed to insert taskinfo rc=%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::cancel()
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;

      BSONObj selector ;
      selector = BSON( OM_TASKINFO_FIELD_TASKID 
                       << (long long)_omTaskInfo.taskID ) ;

      BSONObj tmp ;
      BSONObj updator ;
      tmp     = BSON( OM_TASKINFO_FIELD_ISENABLE << false
                      << OM_TASKINFO_FIELD_DETAIL << _omTaskInfo.detail ) ;
      updator = BSON( "$set" << tmp ) ;

      BSONObj hint ;
      rc = rtnUpdate( OM_CS_DEPLOY_CL_TASKINFO, selector, updator, hint, 0, 
                      cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "failed to update table[%s]:selector=%s,"
                     "updator=%s,rc=%d", OM_CS_DEPLOY_CL_TASKINFO, 
                     selector.toString().c_str(), 
                     updator.toString().c_str(), rc ) ;
         goto error ;
      }

      _omTaskInfo.isEnable = false ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::enable()
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;

      BSONObj selector ;
      selector = BSON( OM_TASKINFO_FIELD_TASKID 
                       << ( long long )_omTaskInfo.taskID ) ;

      BSONObj tmp ;
      BSONObj updator ;
      tmp     = BSON( OM_TASKINFO_FIELD_ISENABLE << true ) ;
      updator = BSON( "$set" << tmp ) ;

      BSONObj hint ;

      rc = rtnUpdate( OM_CS_DEPLOY_CL_TASKINFO, selector, updator, hint, 0, 
                      cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "failed to update table[%s]:selector=%s,"
                     "updator=%s,rc=%d", OM_CS_DEPLOY_CL_TASKINFO, 
                     selector.toString().c_str(), 
                     updator.toString().c_str(), rc ) ;
         goto error ;
      }

      _omTaskInfo.isEnable = true ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::finish()
   {
      SDB_ASSERT( false, "install task should not finish from outside" ) ;
      return SDB_OK ;
   }

   INT32 omInstallTask::getProgress( bool &isEnable, bool &isFinish, 
                                     string &status, BSONObj &progress,
                                     string &detail )
   {
      isEnable = _omTaskInfo.isEnable ;
      isFinish = _omTaskInfo.isFinished ;
      status   = _omTaskInfo.taskStatus ;
      progress = _omTaskInfo.progress ;
      detail   = _omTaskInfo.detail ;
      return SDB_OK ;
   }

   string omInstallTask::getType()
   {
      return _omTaskInfo.taskType ;
   }

   UINT64 omInstallTask::getTaskID()
   {
      return _omTaskInfo.taskID ;
   }

   string omInstallTask::getStatus()
   {
      return _omTaskInfo.taskStatus ;
   }

   INT32 omInstallTask::_storeBusinessInfo()
   {
      INT32 rc = SDB_OK ;
      string businessName ;
      string deployMod ;
      string businessType ;
      string clusterName ;
      BSONObj obj ;
      BSONObj configs ;
      pmdEDUCB *cb  = pmdGetThreadEDUCB() ;

      businessName  = _omTaskInfo.taskInfo.getStringField( 
                                                      OM_BSON_BUSINESS_NAME );
      deployMod     = _omTaskInfo.taskInfo.getStringField( 
                                                      OM_BSON_DEPLOY_MOD ) ;
      businessType  = _omTaskInfo.taskInfo.getStringField( 
                                                      OM_BSON_BUSINESS_TYPE );
      clusterName   = _omTaskInfo.taskInfo.getStringField( 
                                                  OM_BSON_FIELD_CLUSTER_NAME ) ;

      obj = BSON( OM_BUSINESS_FIELD_NAME << businessName 
                  << OM_BSON_BUSINESS_TYPE << businessType 
                  << OM_BSON_DEPLOY_MOD << deployMod
                  << OM_BSON_FIELD_CLUSTER_NAME << clusterName ) ;
      rc = rtnInsert( OM_CS_DEPLOY_CL_BUSINESS, obj, 1, 0, cb );
      if ( rc )
      {
         if ( SDB_IXM_DUP_KEY != rc )
         {
            PD_LOG_MSG( PDERROR, "failed to store business into table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_BUSINESS, rc ) ;
            goto error ;
         }

         rc = SDB_OK ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN omInstallTask::_isHostConfExist( const string &hostName, 
                                            const string &businessName )
   {
      INT32 rc         = SDB_OK ;
      pmdEDUCB *cb     = pmdGetThreadEDUCB() ;
      BOOLEAN flag     = FALSE ;
      SINT64 contextID = -1 ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      pmdKRCB *pKRCB = pmdGetKRCB() ;

      matcher = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName 
                      << OM_CONFIGURE_FIELD_HOSTNAME << hostName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CONFIGURE, selector, matcher, orderBy, 
                     hint, 0, cb, 0, -1, pKRCB->getDMSCB(), pKRCB->getRTNCB(), 
                     contextID ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_CONFIGURE, rc ) ;
         goto done ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, cb, pKRCB->getRTNCB() ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               goto done ;
            }

            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
            goto done ;
         }

         flag = TRUE ;
         goto done ;
      }

   done:
      if ( -1 != contextID )
      {
         pKRCB->getRTNCB()->contextDelete ( contextID, cb ) ;
      }
      return flag ;
   }

   INT32 omInstallTask::_appendConfigure( const string &hostName,
                                          const string &businessName,
                                          BSONObj &oneNode )
   {
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      INT32 rc     = SDB_OK ;
      BSONArrayBuilder arrayBuilder ;
      BSONObj filter  = BSON( OM_BSON_FIELD_HOST_NAME << "" 
                              << OM_BSON_FIELD_HOST_USER << "" 
                              << OM_BSON_FIELD_HOST_PASSWD << "" ) ;
      BSONObj oneConf = oneNode.filterFieldsUndotted( filter, false ) ;
      arrayBuilder.append( oneConf ) ;

      BSONObj selector = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName 
                               << OM_CONFIGURE_FIELD_HOSTNAME << hostName );
      BSONObj tmp      = BSON( OM_CONFIGURE_FIELD_CONFIG 
                               << arrayBuilder.arr() ) ;
      BSONObj updator  = BSON( "$addtoset" << tmp ) ;
      {
         BSONObj hint ;
         rc = rtnUpdate( OM_CS_DEPLOY_CL_CONFIGURE, selector, updator, hint,
                         0, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to update config for %s in %s:rc=%d", 
                    hostName.c_str(), OM_CS_DEPLOY_CL_CONFIGURE, rc ) ;
            goto error ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::_insertConfigure( const string &hostName,
                                          const string &businessName ,
                                          BSONObj &oneNode )
   {
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      INT32 rc     = SDB_OK ;
      BSONArrayBuilder arrayBuilder ;
      BSONObj filter  = BSON( OM_BSON_FIELD_HOST_NAME << "" 
                              << OM_BSON_FIELD_HOST_USER << "" 
                              << OM_BSON_FIELD_HOST_PASSWD << "" ) ;
      BSONObj oneConf = oneNode.filterFieldsUndotted( filter, false ) ;
      arrayBuilder.append( oneConf ) ;
      BSONObj obj = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName 
                          << OM_CONFIGURE_FIELD_HOSTNAME << hostName 
                          << OM_CONFIGURE_FIELD_CONFIG << arrayBuilder.arr() ) ;
      rc = rtnInsert( OM_CS_DEPLOY_CL_CONFIGURE, obj, 1, 0, cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to store config into table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_CONFIGURE, rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   void omInstallTask::_updateHostOMVersion( const string &hostName )
   {
      pmdEDUCB *cb     = pmdGetThreadEDUCB() ;
      INT32 rc         = SDB_OK ;
      INT32 version    = 0 ;
      INT32 subVersion = 0 ;
      INT32 release    = 0 ;
      ossGetVersion( &version, &subVersion, &release, NULL ) ;

      CHAR ver[ OM_INT32_LENGTH+1 ]    = "" ;
      CHAR subVer[ OM_INT32_LENGTH+1 ] = "" ;
      ossItoa( version, ver, OM_INT32_LENGTH ) ;
      ossItoa( subVersion, subVer, OM_INT32_LENGTH ) ;
      string fullVer = string( ver ) + "." + subVer ;

      BSONObj selector = BSON( OM_HOST_FIELD_NAME << hostName );
      BSONObj tmp = BSON( OM_HOST_FIELD_OM_HASINSTALL << true 
                          << OM_HOST_FIELD_OM_VERSION << fullVer
                          << OM_HOST_FIELD_OM_RELEASE << release ) ;

      BSONObj om      = BSON( OM_HOST_FIELD_OM << tmp ) ;
      BSONObj updator = BSON( "$set" << om ) ;
      {
         BSONObj hint ;
         rc = rtnUpdate( OM_CS_DEPLOY_CL_HOST, selector, updator, hint,
                         0, cb ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to update config for %s in %s:rc=%d", 
                    hostName.c_str(), OM_CS_DEPLOY_CL_HOST, rc ) ;
            goto error ;
         }
      }
   done:
      return ;
   error:
      goto done ;
   }

   INT32 omInstallTask::_storeConfigInfo()
   {
      string businessName ;
      BSONObj configs ;
      INT32 rc      = SDB_OK ;
      businessName  = _omTaskInfo.taskInfo.getStringField( 
                                                       OM_BSON_BUSINESS_NAME ) ;
      configs       = _omTaskInfo.taskInfo.getObjectField( 
                                                       OM_BSON_FIELD_CONFIG ) ;
      {
         BSONObjIterator iter( configs ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            BSONObj oneNode = ele.embeddedObject() ;
            string hostName = oneNode.getStringField( 
                                                     OM_BSON_FIELD_HOST_NAME ) ;
            if ( _isHostConfExist( hostName, businessName ) )
            {
               rc = _appendConfigure( hostName, businessName, oneNode ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "append configure failed:host=%s,"
                                   "business=%s, node=%s, rc=%d", 
                                   hostName.c_str(), businessName.c_str(), 
                                   oneNode.toString().c_str(), 
                                   rc ) ;
                  goto error ;
               }
            }
            else
            {
               rc = _insertConfigure( hostName, businessName, oneNode ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "insert configure failed:host=%s,"
                                   "business=%s, node=%s, rc=%d", 
                          hostName.c_str(), businessName.c_str(), 
                          oneNode.toString().c_str(), rc ) ;
                  goto error ;
               }
            }

            _updateHostOMVersion( hostName ) ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::_finishTask()
   {
      INT32 rc = SDB_OK ;
      if ( _omTaskInfo.taskStatus == OM_TASK_STATUS_INSTALL )
      {
         rc = _storeBusinessInfo() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "store business info failed:rc=%d", rc ) ;
            goto error ;
         }

         rc = _storeConfigInfo() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "store config info failed:rc=%d", rc ) ;
            goto error ;
         }
      }

      rc = _saveFinishTask() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "store config info failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::_checkTaskResponse( BSONObj &response )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele = response.getField( OM_BSON_TASK_STATUS ) ;
      if ( ele.type() != String )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "agent's response field format error:field=%s,"
                 "type=%d", OM_BSON_TASK_STATUS, ele.type() ) ;
         goto error ;
      }

      ele = response.getField( OM_BSON_TASK_PROGRESS ) ;
      if ( ele.type() != Array )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "agent's response field format error:field=%s,"
                 "type=%d", OM_BSON_TASK_PROGRESS, ele.type() ) ;
         goto error ;
      }

      ele = response.getField( OM_BSON_TASK_ISFINISHED ) ;
      if ( ele.type() != Bool )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "agent's response field format error:field=%s,"
                 "type=%d", OM_BSON_TASK_ISFINISHED, ele.type() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallTask::updateProgress()
   {
      INT32 rc   = SDB_OK ;
      INT32 flag = SDB_OK ;
      BSONObj response ;
      bool tmpFinished = false ;

      if ( !_omTaskInfo.isEnable )
      {
         PD_LOG( PDERROR, "should not happend here" ) ;
         goto done ;
      }

      rc = _getProgressFromAgent( flag, response ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getProgressFromAgent failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         string errorDetail = response.getStringField( OM_REST_RES_DETAIL ) ;
         PD_LOG( PDERROR, "agent process %s failed, cancel task:taskID="
                 OSS_LL_PRINT_FORMAT",detail=%s,rc=%d", OM_INSTALL_BUSINESS_REQ, 
                 _omTaskInfo.taskID, errorDetail.c_str(), rc ) ;
         _om->getTaskManager()->cancelTask( _omTaskInfo.taskID, errorDetail ) ;
         goto error ;
      }

      rc = _checkTaskResponse( response ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "agent's response format error:res=%s,rc=%d",
                 response.toString().c_str(), rc ) ;
         goto error ;
      }

      _omTaskInfo.taskStatus = response.getStringField( OM_BSON_TASK_STATUS ) ;
      _omTaskInfo.progress   = response.getObjectField( 
                                             OM_BSON_TASK_PROGRESS ).copy() ;
      _omTaskInfo.detail     = response.getStringField( OM_BSON_TASK_DETAIL ) ;
      tmpFinished = response.getBoolField( OM_BSON_TASK_ISFINISHED ) ;
      if ( tmpFinished )
      {
         rc = _finishTask() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "finish task failed:rc=%d", rc ) ;
            goto error ;
         }

         _omTaskInfo.isFinished = tmpFinished ;
      }

   done:
      return rc  ;
   error:
      goto done ;
   }

   BOOLEAN omInstallTask::isEnable()
   {
      return _omTaskInfo.isEnable ;
   }

   BOOLEAN omInstallTask::isFinish()
   {
      return _omTaskInfo.isFinished ;
   }

   void omInstallTask::getAllTaskInfo( omTaskInfo &taskInfo )
   {
      taskInfo = _omTaskInfo ;
   }

   omUninstallTask::omUninstallTask( omManager *om )
                   :omInstallTask( om )
   {
      _omTaskInfo.taskType   = OM_REMOVE_BUSINESS_REQ ;
      _omTaskInfo.taskStatus = OM_TASK_STATUS_UNINSTALL ;
   }

   omUninstallTask::~omUninstallTask()
   {
   }

   INT32 omUninstallTask::init( const string &agentHost, 
                                const string &agentService, const BSONObj &conf, 
                                UINT64 taskID )
   {
      BSONObj tmp ;
      INT32 rc      = SDB_OK ;
      _omTaskInfo.taskID       = taskID ;
      _omTaskInfo.agentHost    = agentHost ;
      _omTaskInfo.agentService = agentService ;
      _omTaskInfo.taskInfo     = conf ;
      _omTaskInfo.isEnable     = false ;
      _omTaskInfo.isFinished   = false ;
      _omTaskInfo.taskType     = OM_REMOVE_BUSINESS_REQ ;
      _omTaskInfo.taskStatus   = OM_TASK_STATUS_UNINSTALL ;
      _omTaskInfo.progress     = BSONObj() ;
      PD_LOG( PDDEBUG, "_omTaskInfo.taskInfo:%s", 
              _omTaskInfo.taskInfo.toString().c_str() ) ;

      if ( !_omTaskInfo.taskInfo.hasField( OM_BSON_BUSINESS_NAME )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_DEPLOY_MOD )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_BUSINESS_TYPE )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_FIELD_CLUSTER_NAME ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "uninstall task configure error:conf=%s", 
                     conf.toString().c_str() ) ;
         goto error ;
      }

      rc = _insertTask() ;
      if ( rc )
      {
         PD_LOG( PDERROR, "failed to insert taskinfo:rc=%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omUninstallTask::_removeConfigInfo()
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = NULL ;
      string businessName ;
      BSONObj condition ;
      BSONObj hint ;
      businessName = _omTaskInfo.taskInfo.getStringField( 
                                                      OM_BSON_BUSINESS_NAME ) ;

      cb           = pmdGetThreadEDUCB() ;
      condition    = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName ) ;

      rc = rtnDelete( OM_CS_DEPLOY_CL_CONFIGURE, condition, hint, 0, cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to delete configure from table:%s,"
                     "business=%s,rc=%d", OM_CS_DEPLOY_CL_CONFIGURE, 
                     businessName.c_str(), rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omUninstallTask::_removeBusinessInfo()
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = NULL ;
      string businessName ;
      BSONObj condition ;
      BSONObj hint ;
      businessName = _omTaskInfo.taskInfo.getStringField( 
                                                      OM_BSON_BUSINESS_NAME ) ;

      cb           = pmdGetThreadEDUCB() ;
      condition    = BSON( OM_BUSINESS_FIELD_NAME << businessName ) ;

      rc = rtnDelete( OM_CS_DEPLOY_CL_BUSINESS, condition, hint, 0, cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to delete business from table:%s,"
                     "business=%s,rc=%d", OM_CS_DEPLOY_CL_BUSINESS, 
                     businessName.c_str(), rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omUninstallTask::_finishUninstallTask()
   {
      INT32 rc = SDB_OK ;

      if ( _omTaskInfo.taskStatus == OM_TASK_STATUS_ADDHOST )
      {
         rc = _removeConfigInfo() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "remove config info failed:rc=%d", rc ) ;
            goto error ;
         }

         rc = _removeBusinessInfo() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "remove business info failed:rc=%d", rc ) ;
            goto error ;
         }
      }

      rc = _saveFinishTask() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "update task progress failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omUninstallTask::finish()
   {
      return _finishUninstallTask() ;
   }

   INT32 omUninstallTask::updateProgress()
   {
      INT32 rc   = SDB_OK ;
      INT32 flag = SDB_OK ;
      BSONObj response ;
      bool tmpFinished = false ;

      if ( !_omTaskInfo.isEnable )
      {
         PD_LOG( PDERROR, "should not happend here" ) ;
         goto done ;
      }

      rc = _getProgressFromAgent( flag, response ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getProgressFromAgent failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         string errorDetail = response.getStringField( OM_REST_RES_DETAIL ) ;
         PD_LOG( PDERROR, "agent process %s failed, cancel task:taskID="
                 OSS_LL_PRINT_FORMAT",detail=%s,rc=%d", OM_REMOVE_BUSINESS_REQ, 
                 _omTaskInfo.taskID, errorDetail.c_str(), rc ) ;
         _om->getTaskManager()->cancelTask( _omTaskInfo.taskID, errorDetail ) ;
         goto error ;
      }

      if ( !response.hasField( OM_BSON_TASK_ISFINISHED ) 
           || !response.hasField( OM_BSON_TASK_STATUS ) 
           || !response.hasField( OM_BSON_TASK_PROGRESS ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "agent's response format error:res=%s,rc=%d",
                 response.toString().c_str(), rc ) ;
         goto error ;
      }

      _omTaskInfo.taskStatus = response.getStringField( OM_BSON_TASK_STATUS ) ;
      _omTaskInfo.progress   = response.getObjectField( 
                                              OM_BSON_TASK_PROGRESS ).copy() ;
      tmpFinished = response.getBoolField( OM_BSON_TASK_ISFINISHED ) ;
      if ( tmpFinished )
      {
         rc = _finishUninstallTask() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "finish task failed:rc=%d", rc ) ;
            goto error ;
         }

         _omTaskInfo.isFinished = tmpFinished ;
      }

   done:
      return rc  ;
   error:
      goto done ;
   }

   omAddHostTask::omAddHostTask( omManager *om )
                 :omInstallTask( om )
   {
      _omTaskInfo.taskType   = OM_ADD_HOST_REQ ;
      _omTaskInfo.taskStatus = OM_TASK_STATUS_ADDHOST ;
   }

   omAddHostTask::~omAddHostTask()
   {
   }

   INT32 omAddHostTask::init( const string &agentHost, 
                              const string &agentService, const BSONObj &conf, 
                              UINT64 taskID )
   {
      BSONObj tmp ;
      INT32 rc      = SDB_OK ;
      _omTaskInfo.taskID       = taskID ;
      _omTaskInfo.agentHost    = agentHost ;
      _omTaskInfo.agentService = agentService ;
      _omTaskInfo.taskInfo     = conf ;
      _omTaskInfo.isEnable     = false ;
      _omTaskInfo.isFinished   = false ;
      _omTaskInfo.taskType     = OM_ADD_HOST_REQ ;
      _omTaskInfo.taskStatus   = OM_TASK_STATUS_ADDHOST ;
      _omTaskInfo.progress     = BSONObj() ;
      PD_LOG( PDDEBUG, "_omTaskInfo.taskInfo:%s", 
              _omTaskInfo.taskInfo.toString().c_str() ) ;

      if ( !_omTaskInfo.taskInfo.hasField( OM_BSON_FIELD_CLUSTER_NAME )
            || !_omTaskInfo.taskInfo.hasField( OM_BSON_FIELD_HOST_INFO ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "add host task configure error:conf=%s", 
                     conf.toString().c_str() ) ;
         goto error ;
      }

      _clusterName = _omTaskInfo.taskInfo.getStringField( 
                                                  OM_BSON_FIELD_CLUSTER_NAME ) ;

      rc = _insertTask() ;
      if ( rc )
      {
         PD_LOG( PDERROR, "failed to insert taskinfo:rc=%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostTask::_storeHostInfo()
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;

      BSONObj hosts ;
      hosts = _omTaskInfo.taskInfo.getObjectField( OM_BSON_FIELD_HOST_INFO ) ;
      {
         BSONObjIterator iter( hosts ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            BSONObj oneHost = ele.embeddedObject() ;
            rc = rtnInsert( OM_CS_DEPLOY_CL_HOST, oneHost, 1, 0, cb ) ;
            if ( rc )
            {
               if ( SDB_IXM_DUP_KEY != rc )
               {
                  PD_LOG( PDERROR, "insert into table failed:%s,rc=%d", 
                          OM_CS_DEPLOY_CL_HOST, rc ) ;
                  goto error ;
               }
            }
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostTask::_finishAddHostTask()
   {
      INT32 rc = SDB_OK ;
      if ( _omTaskInfo.taskStatus == OM_TASK_STATUS_ADDHOST )
      {
         rc = _storeHostInfo() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "store host info failed:rc=%d", rc ) ;
            goto error ;
         }
      }

      rc = _saveFinishTask() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "update task progress failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostTask::updateProgress()
   {
      INT32 rc   = SDB_OK ;
      INT32 flag = SDB_OK ;
      BSONObj response ;
      bool tmpFinished = false ;

      if ( !_omTaskInfo.isEnable )
      {
         PD_LOG( PDERROR, "should not happend here" ) ;
         goto done ;
      }

      rc = _getProgressFromAgent( flag, response ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getProgressFromAgent failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         string errorDetail = response.getStringField( OM_REST_RES_DETAIL ) ;
         PD_LOG( PDERROR, "agent process %s failed, cancel task:taskID="
                 OSS_LL_PRINT_FORMAT",detail=%s,rc=%d", OM_ADD_HOST_REQ, 
                 _omTaskInfo.taskID, errorDetail.c_str(), rc ) ;
         _om->getTaskManager()->cancelTask( _omTaskInfo.taskID, errorDetail ) ;
         goto error ;
      }

      if ( !response.hasField( OM_BSON_TASK_ISFINISHED ) 
           || !response.hasField( OM_BSON_TASK_STATUS ) 
           || !response.hasField( OM_BSON_TASK_PROGRESS ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "agent's response format error:res=%s,rc=%d",
                 response.toString().c_str(), rc ) ;
         goto error ;
      }

      _omTaskInfo.taskStatus = response.getStringField( OM_BSON_TASK_STATUS ) ;
      _omTaskInfo.progress   = response.getObjectField( 
                                               OM_BSON_TASK_PROGRESS ).copy() ;
      tmpFinished = response.getBoolField( OM_BSON_TASK_ISFINISHED ) ;
      if ( tmpFinished )
      {
         rc = _finishAddHostTask() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "finish task failed:rc=%d", rc ) ;
            goto error ;
         }

         _omTaskInfo.isFinished = tmpFinished ;
         sdbGetOMManager()->updateClusterVersion( _clusterName ) ;
      }

   done:
      return rc  ;
   error:
      goto done ;
   }

   omTaskManager::omTaskManager( omManager *om )
                 :_om( om ), _maxTaskID( 0 )
   {
   }

   omTaskManager::~omTaskManager()
   {
      _mapTasks.clear() ;
   }

   INT32 omTaskManager::_restoreInstallTask( BSONObj &record )
   {
      INT32 rc            = SDB_OK ;
      omInstallTask *task = NULL ;
      UINT64 taskID ;

      if ( _isTaskTypeExist( OM_INSTALL_BUSINESS_REQ, taskID) )
      {
         rc = SDB_IXM_DUP_KEY ;
         PD_LOG_MSG( PDERROR, "task exist:taskType=%s,taskID="
                     OSS_LL_PRINT_FORMAT, OM_INSTALL_BUSINESS_REQ, taskID ) ;
         goto error ;
      }

      task = SDB_OSS_NEW omInstallTask( _om ) ;
      rc = task->restore( record ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "restore omInstallTask failed:rc=%d", rc ) ;
         goto error ;
      }

      _addTaskToMap( task ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::_restoreUninstallTask( BSONObj &record ) 
   {
      INT32 rc              = SDB_OK ;
      omUninstallTask *task = NULL ;
      UINT64 taskID ;

      if ( _isTaskTypeExist( OM_REMOVE_BUSINESS_REQ, taskID) )
      {
         rc = SDB_IXM_DUP_KEY ;
         PD_LOG( PDERROR, "task exist:taskType=%s,taskID="OSS_LL_PRINT_FORMAT,
                 OM_REMOVE_BUSINESS_REQ, taskID ) ;
         goto error ;
      }

      task = SDB_OSS_NEW omUninstallTask( _om ) ;
      rc = task->restore( record ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "restore omUninstallTask failed:rc=%d", rc ) ;
         goto error ;
      }

      _addTaskToMap( task ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::_getTaskRecord( UINT64 taskID, BSONObj &result )
   {
      INT32 rc           = SDB_OK ;
      pmdEDUCB *cb       = pmdGetThreadEDUCB() ;
      pmdKRCB *pKRCB     = pmdGetKRCB() ;
      _SDB_DMSCB *pdmsCB = pKRCB->getDMSCB() ;
      _SDB_RTNCB *pRtnCB = pKRCB->getRTNCB() ;
      SINT64 contextID = -1 ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;

      matcher = BSON( OM_TASKINFO_FIELD_TASKID << (long long)taskID ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_TASKINFO, selector, matcher, orderBy, hint, 
                     0, cb, 0, -1, pdmsCB, pRtnCB, contextID ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, cb, pRtnCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               PD_LOG_MSG( PDERROR, "task is not exit in table:%s,taskID="
                           OSS_LL_PRINT_FORMAT",rc=%d", 
                           OM_CS_DEPLOY_CL_TASKINFO, taskID, rc ) ;
               goto error ;
            }

            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         result = record.copy() ;
         goto done ;
      }

   done:
      if ( -1 != contextID )
      {
         pRtnCB->contextDelete ( contextID, cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::restoreTask()
   {
      INT32 rc           = SDB_OK ;
      pmdEDUCB *cb       = pmdGetThreadEDUCB() ;
      pmdKRCB *pKRCB     = pmdGetKRCB() ;
      _SDB_DMSCB *pdmsCB = pKRCB->getDMSCB() ;
      _SDB_RTNCB *pRtnCB = pKRCB->getRTNCB() ;
      SINT64 contextID = -1 ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      _maxTaskID = 0;

      rc = rtnQuery( OM_CS_DEPLOY_CL_TASKINFO, selector, matcher, orderBy, hint, 
                     0, cb, 0, -1, pdmsCB, pRtnCB, contextID ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, cb, pRtnCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               goto done ;
            }

            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         BSONElement element = record.getField( OM_TASKINFO_FIELD_TASKID ) ;
         UINT64 taskID       = element.Long() ;
         if ( _maxTaskID < taskID )
         {
            _maxTaskID = taskID ;
         }

         bool isEnable   = record.getBoolField( OM_TASKINFO_FIELD_ISENABLE ) ;
         bool isFinish   = record.getBoolField( OM_TASKINFO_FIELD_ISFINISH ) ;
         if ( !isEnable || isFinish )
         {
            continue ;
         }

         string taskType = record.getStringField( OM_TASKINFO_FIELD_TYPE ) ;
         if ( taskType == OM_INSTALL_BUSINESS_REQ )
         {
            rc = _restoreInstallTask( record ) ;
         }
         else if ( taskType == OM_REMOVE_BUSINESS_REQ )
         {
            rc = _restoreUninstallTask( record ) ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
         }

         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "restore task failed:type=%s,taskID="
                    OSS_LL_PRINT_FORMAT, taskType.c_str(), taskID ) ;
            goto error ;
         }
      }

   done:
      if ( -1 != contextID )
      {
         pRtnCB->contextDelete ( contextID, cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   UINT64 omTaskManager::_generateTaskID()
   {
      return ++_maxTaskID ;
   }

   BOOLEAN omTaskManager::_isTaskTypeExist( string taskType, UINT64 &taskID )
   {
      BOOLEAN isTaskTypeExist = FALSE ;

      MAP_TASK_INTER iter = _mapTasks.begin() ;
      while ( iter != _mapTasks.end() )
      {
         boost::shared_ptr< omTaskBase > sharedTask = iter->second ;
         if ( sharedTask->getType() == taskType )
         {
            isTaskTypeExist = TRUE ;
            taskID          = sharedTask->getTaskID() ;
            break ;
         }

         iter++ ;
      }

      return isTaskTypeExist ;
   }

   void omTaskManager::_addTaskToMap( omTaskBase *task )
   {
      _mapTasks[task->getTaskID()] =
                          boost::shared_ptr< omTaskBase >( task, taskDeleter ) ;
   }

   INT32 omTaskManager::createInstallTask( const string &agentHost, 
                                           const string &agentService, 
                                           const BSONObj &confValue, 
                                           UINT64 &taskID )
   {
      INT32 rc            = SDB_OK ;
      omInstallTask *task = NULL ;

      _lock.get() ;
      if ( _isTaskTypeExist( OM_INSTALL_BUSINESS_REQ, taskID ) )
      {
         rc = SDB_IXM_DUP_KEY ;
         PD_LOG_MSG( PDERROR, "task exist:taskType=%s,taskID="
                     OSS_LL_PRINT_FORMAT, OM_INSTALL_BUSINESS_REQ, taskID ) ;
         goto error ;
      }

      taskID = _generateTaskID() ;

      task = SDB_OSS_NEW omInstallTask( _om ) ;
      rc = task->init( agentHost, agentService, confValue, taskID ) ;
      if ( SDB_OK != rc )
      {
         SDB_OSS_DEL task ;
         PD_LOG_MSG( PDERROR, "init omInstallTask failed:rc=%d", rc ) ;
         goto error ;
      }

      _addTaskToMap( task ) ;
      PD_LOG( PDEVENT, "create task success:type=%s,taskID="OSS_LL_PRINT_FORMAT,
              task->getType().c_str(), task->getTaskID() ) ;
   done:
      _lock.release() ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::createUninstallTask( const string &agentHost, 
                                             const string &agentService, 
                                             const BSONObj &confValue,
                                             UINT64 &taskID )
   {
      INT32 rc              = SDB_OK ;
      omUninstallTask *task = NULL ;

      _lock.get() ;
      if ( _isTaskTypeExist( OM_REMOVE_BUSINESS_REQ, taskID ) )
      {
         rc = SDB_IXM_DUP_KEY ;
         PD_LOG_MSG( PDERROR, "task exist:taskType=%s,taskID="
                     OSS_LL_PRINT_FORMAT, OM_REMOVE_BUSINESS_REQ, taskID ) ;
         goto error ;
      }

      taskID = _generateTaskID() ;
      task = SDB_OSS_NEW omUninstallTask( _om ) ;
      rc   = task->init( agentHost, agentService, confValue, taskID ) ;
      if ( SDB_OK != rc )
      {
         SDB_OSS_DEL task ;
         PD_LOG_MSG( PDERROR, "init omUninstallTask failed:rc=%d", rc ) ;
         goto error ;
      }

      _addTaskToMap( task ) ;
   done:
      _lock.release() ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::createAddHostTask( const string &agentHost, 
                                           const string &agentService, 
                                           const BSONObj &confValue, 
                                           UINT64 &taskID )
   {
      INT32 rc            = SDB_OK ;
      omAddHostTask *task = NULL ;

      _lock.get() ;
      if ( _isTaskTypeExist( OM_ADD_HOST_REQ, taskID ) )
      {
         rc = SDB_IXM_DUP_KEY ;
         PD_LOG_MSG( PDERROR, "task exist:taskType=%s,taskID="
                     OSS_LL_PRINT_FORMAT, OM_ADD_HOST_REQ, taskID ) ;
         goto error ;
      }

      taskID = _generateTaskID() ;
      task = SDB_OSS_NEW omAddHostTask( _om ) ;
      rc   = task->init( agentHost, agentService, confValue, taskID ) ;
      if ( SDB_OK != rc )
      {
         SDB_OSS_DEL task ;
         PD_LOG_MSG( PDERROR, "init omAddHostTask failed:rc=%d", rc ) ;
         goto error ;
      }

      _addTaskToMap( task ) ;
   done:
      _lock.release() ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::cancelTask( UINT64 taskID, const string &detail )
   {
      INT32 rc          = SDB_OK ;
      boost::shared_ptr< omTaskBase > shareTask ;
      _lock.get() ;
      MAP_TASK_INTER iter = _mapTasks.find( taskID ) ;
      if ( iter == _mapTasks.end() )
      {
         _lock.release() ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "task is not exist:taskID="OSS_LL_PRINT_FORMAT, 
                 taskID ) ;
         goto error ;
      }
      shareTask = iter->second ;
      _lock.release() ;

      shareTask->setDetail( detail ) ;
      rc = shareTask->cancel() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "cancel task failed:taskID="OSS_LL_PRINT_FORMAT
                 ",rc=%d", taskID, rc ) ;
         goto error ;
      }

      _lock.get() ;
      _mapTasks.erase( taskID ) ;
      _lock.release() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::finishTask( UINT64 taskID )
   {
      INT32 rc          = SDB_OK ;
      boost::shared_ptr< omTaskBase > shareTask ;
      _lock.get() ;
      MAP_TASK_INTER iter = _mapTasks.find( taskID ) ;
      if ( iter == _mapTasks.end() )
      {
         _lock.release() ;
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "task is not exist:taskID="OSS_LL_PRINT_FORMAT, 
                     taskID ) ;
         goto error ;
      }
      shareTask = iter->second ;
      _lock.release() ;

      rc    = shareTask->finish() ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "finish task failed:taskID="OSS_LL_PRINT_FORMAT
                     ",rc=%d", taskID, rc ) ;
         goto error ;
      }

      _lock.get() ;
      _mapTasks.erase( taskID ) ;
      _lock.release() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::enableTask( UINT64 taskID )
   {
      INT32 rc          = SDB_OK ;
      boost::shared_ptr< omTaskBase > shareTask ;
      _lock.get() ;
      MAP_TASK_INTER iter = _mapTasks.find( taskID ) ;
      if ( iter == _mapTasks.end() )
      {
         _lock.release() ;
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "task is not exist:taskID="OSS_LL_PRINT_FORMAT, 
                     taskID ) ;
         goto error ;
      }
      shareTask = iter->second ;
      _lock.release() ;

      rc    = shareTask->enable() ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "enable task failed:taskID="OSS_LL_PRINT_FORMAT
                     ",rc=%d", taskID, rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omTaskManager::getProgress( UINT64 taskID, string &taskType, 
                                     bool &isEnable, bool &isFinish, 
                                     string &status, BSONObj &progress,
                                     string &detail )
   {
      INT32 rc = SDB_OK ;
      boost::shared_ptr< omTaskBase > shareTask ;
      _lock.get() ;
      MAP_TASK_INTER iter = _mapTasks.find( taskID ) ;
      if ( iter == _mapTasks.end() )
      {
         _lock.release() ;
         BSONObj result ;
         rc = _getTaskRecord( taskID, result ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "get task's record failed:taskID="
                    OSS_LL_PRINT_FORMAT",rc=%d", taskID, rc ) ;
            goto error ;
         }

         taskType = result.getStringField( OM_TASKINFO_FIELD_TYPE ) ;
         isEnable = result.getBoolField( OM_TASKINFO_FIELD_ISENABLE ) ;
         isFinish = result.getBoolField( OM_TASKINFO_FIELD_ISFINISH ) ;
         status   = result.getStringField( OM_TASKINFO_FIELD_STATUS ) ;
         progress = result.getObjectField( OM_TASKINFO_FIELD_PROGRESS ) ;
         detail   = result.getStringField( OM_TASKINFO_FIELD_DETAIL ) ;
      }
      else
      {
         shareTask = iter->second ;
         _lock.release() ;

         taskType = shareTask->getType() ;
         rc = shareTask->getProgress( isEnable, isFinish, status, progress, 
                                      detail ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "get task's progress failed:taskID="
                        OSS_LL_PRINT_FORMAT",rc=%d", taskID, rc ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omTaskManager::getTaskInfo( const string &agentHost, 
                                    const string &agentService, 
                                    list<omTaskInfo> &taskList )
   {

      _lock.get() ;
      MAP_TASK_INTER iter = _mapTasks.begin() ;
      while ( iter != _mapTasks.end() )
      {
         omTaskInfo taskInfo ;
         boost::shared_ptr< omTaskBase > shareTask = iter->second ;
         shareTask->getAllTaskInfo( taskInfo ) ;
         if ( ( taskInfo.agentHost == agentHost )
              && ( taskInfo.agentService == agentService ) )
         {
            taskList.push_back( taskInfo ) ;
         }
         iter++ ;
      }
   }

   INT32 omTaskManager::run()
   {
      INT32 rc = SDB_OK ;
      list< boost::shared_ptr< omTaskBase > > shareTaskList ;

      _lock.get() ;
      MAP_TASK_INTER iter = _mapTasks.begin() ;
      while ( iter != _mapTasks.end() )
      {
         boost::shared_ptr< omTaskBase > shareTask = iter->second ;
         if ( shareTask->isFinish() )
         {
            _mapTasks.erase( iter++ ) ;
            continue ;
         }

         if ( shareTask->isEnable() )
         {
            shareTaskList.push_back( iter->second ) ;
         }

         iter++ ;
      }
      _lock.release() ;

      list< boost::shared_ptr< omTaskBase > >::iterator iterList ;
      iterList = shareTaskList.begin() ;
      while ( iterList != shareTaskList.end() )
      {
         rc = ( *iterList )->updateProgress() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "update progress failed:taskID="OSS_LL_PRINT_FORMAT
                    ",rc=%d", ( *iterList )->getTaskID(), rc ) ;
         }
         iterList++ ;
      }

      shareTaskList.clear() ;

      return rc ;
   }
}

