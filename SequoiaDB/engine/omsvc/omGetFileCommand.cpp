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

   Source File Name = omGetFileCommand.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/12/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omGetFileCommand.hpp"
#include "omDef.hpp"
#include "rtn.hpp"
#include "omManager.hpp"
#include "omConfigGenerator.hpp"
#include "omTaskManager.hpp"
#include "../bson/lib/md5.hpp"
#include "ossPath.hpp"
#include "ossProc.hpp"
#include <set>

using namespace bson;
using namespace boost::property_tree;


namespace engine
{
   static INT32 getMaxTaskID( INT64 &taskID ) ;
   static INT32 createTask( INT32 taskType, INT64 taskID, 
                            const string &taskName, const string &agentHost, 
                            const string &agentService, const BSONObj &taskInfo, 
                            const BSONArray &resultInfo ) ;

   static INT32 removeTask( INT64 taskID ) ;

   INT32 getMaxTaskID( INT64 &taskID )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj orderBy ;
      BSONObj hint ;
      SINT64 contextID   = -1 ;
      pmdEDUCB *cb       = pmdGetThreadEDUCB() ;
      pmdKRCB *pKRCB     = pmdGetKRCB() ;
      _SDB_DMSCB *pdmsCB = pKRCB->getDMSCB() ;
      _SDB_RTNCB *pRtnCB = pKRCB->getRTNCB() ;

      taskID = 0 ;

      selector    = BSON( OM_TASKINFO_FIELD_TASKID << 1 ) ;
      orderBy     = BSON( OM_TASKINFO_FIELD_TASKID << -1 ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_TASKINFO, selector, matcher, orderBy, 
                     hint, 0, cb, 0, 1, pdmsCB, pRtnCB, contextID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get taskid failed:rc=%d", rc ) ;
         goto error ;
      }

      {
         BSONElement ele ;
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, cb, pRtnCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               goto done ;
            }

            PD_LOG( PDERROR, "failed to get record from table:%s,rc=%d", 
                    OM_CS_DEPLOY_CL_TASKINFO, rc ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         ele    = result.getField( OM_TASKINFO_FIELD_TASKID ) ;
         taskID = ele.numberLong() ;
      }

   done:
      if ( -1 != contextID )
      {
         pRtnCB->contextDelete( contextID, cb ) ;
         contextID = -1 ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 createTask( INT32 taskType, INT64 taskID, const string &taskName,
                     const string &agentHost, const string &agentService,
                     const BSONObj &taskInfo, const BSONArray &resultInfo )
   {
      INT32 rc     = SDB_OK ;
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      BSONObj record ;

      BSONObjBuilder builder ;
      time_t now = time( NULL ) ;
      builder.append( OM_TASKINFO_FIELD_TASKID, taskID ) ;
      builder.append( OM_TASKINFO_FIELD_TYPE, taskType ) ;
      builder.append( OM_TASKINFO_FIELD_TYPE_DESC, 
                      getTaskTypeStr( taskType ) ) ;
      builder.append( OM_TASKINFO_FIELD_NAME, taskName ) ;
      builder.appendTimestamp( OM_TASKINFO_FIELD_CREATE_TIME, 
                               (unsigned long long)now * 1000, 0 ) ;
      builder.appendTimestamp( OM_TASKINFO_FIELD_END_TIME, 
                               (unsigned long long)now * 1000, 0 ) ;
      builder.append( OM_TASKINFO_FIELD_STATUS, OM_TASK_STATUS_INIT ) ;
      builder.append( OM_TASKINFO_FIELD_STATUS_DESC, 
                      getTaskStatusStr( OM_TASK_STATUS_INIT ) ) ;
      builder.append( OM_TASKINFO_FIELD_AGENTHOST, agentHost ) ;
      builder.append( OM_TASKINFO_FIELD_AGENTPORT, agentService ) ;
      builder.append( OM_TASKINFO_FIELD_INFO, taskInfo ) ;
      builder.append( OM_TASKINFO_FIELD_ERRNO, SDB_OK ) ;
      builder.append( OM_TASKINFO_FIELD_DETAIL, "" ) ;
      builder.append( OM_TASKINFO_FIELD_PROGRESS, 0 ) ;
      builder.append( OM_TASKINFO_FIELD_RESULTINFO, resultInfo ) ;

      record = builder.obj() ;
      rc = rtnInsert( OM_CS_DEPLOY_CL_TASKINFO, record, 1, 0, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "insert task failed:rc=%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 removeTask( INT64 taskID )
   {
      INT32 rc = SDB_OK ;
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      BSONObj deletor ;
      BSONObj hint ;
      deletor = BSON( OM_TASKINFO_FIELD_TASKID << taskID ) ;
      rc = rtnDelete( OM_CS_DEPLOY_CL_TASKINFO, deletor, hint, 0, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "rtnDelete task failed:taskID="OSS_LL_PRINT_FORMAT
                 ",rc=%d", taskID, rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   const CHAR *getTaskStatusStr( INT32 status )
   {
      static CHAR *s_taskStatusStr[ OM_TASK_STATUS_END ] = 
                                                    { 
                                                       "INIT", 
                                                       "RUNNING", 
                                                       "ROLLBACK", 
                                                       "CANCEL", 
                                                       "FINISH" 
                                                    } ;
      if ( status < OM_TASK_STATUS_END )
      {
         return s_taskStatusStr[status] ;
      }
      else
      {
         return "unknown" ;
      }
   }

   const CHAR *getTaskTypeStr( INT32 type )
   {
      static CHAR *s_taskTypeStr[ OM_TASK_TYPE_END ] = 
                                                   { 
                                                       "ADD_HOST", 
                                                       "ADD_BUSINESS",
                                                       "REMOVE_BUSINESS"
                                                    } ;
      if ( type < OM_TASK_TYPE_END )
      {
         return s_taskTypeStr[type] ;
      }
      else
      {
         return "unknown" ;
      }
   }

   omAuthCommand::omAuthCommand( restAdaptor *pRestAdaptor, 
                                 pmdRestSession *pRestSession )
   {
      _restAdaptor = pRestAdaptor ;
      _restSession = pRestSession ;
   }

   omAuthCommand::~omAuthCommand()
   {
   }

   void omAuthCommand::_setOPResult( INT32 rc, const CHAR* detail )
   {
      BSONObj res = BSON( OM_REST_RES_RETCODE << rc 
                          << OM_REST_RES_DESP << getErrDesp( rc )
                          << OM_REST_RES_DETAIL << detail ) ;

      _restAdaptor->setOPResult( _restSession, rc, res ) ;
   }

   string omAuthCommand::_getLanguage()
   {
      const CHAR *pLanguage = NULL ;
      _restAdaptor->getHttpHeader( _restSession, OM_REST_HEAD_LANGUAGE, 
                                   &pLanguage ) ;
      if ( NULL != pLanguage 
           && ossStrcasecmp( pLanguage, OM_REST_LANGUAGE_EN ) == 0 )
      {
         return OM_REST_LANGUAGE_EN ;
      }
      else
      {
         return OM_REST_LANGUAGE_ZH_CN ;
      }
   }

   void omAuthCommand::_setFileLanguageSep()
   {
      string language = _getLanguage() ;
      _languageFileSep = "_" + language ;
   }

   INT32 omAuthCommand::_getQueryPara( BSONObj &selector,  BSONObj &matcher,
                                       BSONObj &order, BSONObj &hint )
   {
      const CHAR *pSelector = NULL ;
      const CHAR *pMatcher  = NULL ;
      const CHAR *pOrder    = NULL ;
      const CHAR *pHint     = NULL ;
      INT32 rc = SDB_OK ;
      _restAdaptor->getQuery(_restSession, FIELD_NAME_SELECTOR, &pSelector ) ;
      if ( NULL != pSelector )
      {
         rc = fromjson( pSelector, selector ) ;
         if ( rc )
         {
            PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                        "rc=%d", pSelector, rc ) ;
            goto error ;
         }
      }

      _restAdaptor->getQuery(_restSession, FIELD_NAME_FILTER, &pMatcher ) ;
      if ( NULL != pMatcher )
      {
         rc = fromjson( pMatcher, matcher ) ;
         if ( rc )
         {
            PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                        "rc=%d", pMatcher, rc ) ;
            goto error ;
         }
      }

      _restAdaptor->getQuery(_restSession, FIELD_NAME_SORT, &pOrder ) ;
      if ( NULL != pOrder )
      {
         rc = fromjson( pOrder, order ) ;
         if ( rc )
         {
            PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                        "rc=%d", pOrder, rc ) ;
            goto error ;
         }
      }

      _restAdaptor->getQuery(_restSession, FIELD_NAME_HINT, &pHint ) ;
      if ( NULL != pHint )
      {
         rc = fromjson( pHint, hint ) ;
         if ( rc )
         {
            PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                        "rc=%d", pHint, rc ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAuthCommand::_queryTable( const string &tableName, 
                                    const BSONObj &selector, 
                                    const BSONObj &matcher,
                                    const BSONObj &order, 
                                    const BSONObj &hint, SINT32 flag,
                                    SINT64 numSkip, SINT64 numReturn, 
                                    list<BSONObj> &records )
   {
      INT32 rc         = SDB_OK ;
      SINT64 contextID = -1 ;
      rc = rtnQuery( tableName.c_str(), selector, matcher, order, hint, flag, 
                     _cb, numSkip, numReturn, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:name=%s,rc=%d", 
                     tableName.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            _pRTNCB->contextDelete( contextID, _cb ) ;
            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:name=%s,"
                        "rc=%d", tableName.c_str(), rc ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         records.push_back( result.copy() );
      }

   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   void omAuthCommand::_sendOKRes2Web()
   {
      _sendErrorRes2Web( SDB_OK, "" ) ;
   }

   void omAuthCommand::_sendErrorRes2Web( INT32 rc, const CHAR* detail )
   {
      _setOPResult( rc, detail ) ;
      _restAdaptor->sendResponse( _restSession, HTTP_OK ) ;
   }

   void omAuthCommand::_sendErrorRes2Web( INT32 rc, const string &detail )
   {
      _sendErrorRes2Web( rc, detail.c_str() ) ;
   }

   void omAuthCommand::_decryptPasswd( const string &encryptPasswd, 
                                       const string &time,
                                       string &decryptPasswd )
   {
      decryptPasswd = encryptPasswd ;
   }

   INT32 omAuthCommand::_getSdbUsrInfo( const string &clusterName, string &sdbUser, 
                                        string &sdbPasswd, 
                                        string &sdbUserGroup )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder resultBuilder ;
      BSONObj result ;

      BSONObjBuilder builder ;
      builder.append( OM_CLUSTER_FIELD_NAME, clusterName ) ;
      BSONObj matcher ;
      matcher = builder.obj() ;

      BSONObj selector ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID             = -1 ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CLUSTER, selector, matcher, order, hint, 0, 
                     _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to query table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_CLUSTER, rc ) ;
         goto error ;
      }

      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               PD_LOG_MSG( PDERROR, "cluster is not exist:cluster=%s,rc=%d", 
                           clusterName.c_str(), rc ) ;
               goto error ;
            }

            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get recrod from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_CLUSTER, rc ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         sdbUser      = record.getStringField( OM_CLUSTER_FIELD_SDBUSER ) ;
         sdbPasswd    = record.getStringField( OM_CLUSTER_FIELD_SDBPASSWD ) ;
         sdbUserGroup = record.getStringField( OM_CLUSTER_FIELD_SDBUSERGROUP ) ;
      }

   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omAuthCommand::doCommand()
   {
      const CHAR *pUserName        = NULL ;
      const CHAR *pPasswd          = NULL ;
      const CHAR *pTime            = NULL ;
      INT32 rc                     = SDB_OK ;
      ossSocket *socket            = NULL ;
      BSONObjBuilder authBuilder ;
      BSONObjBuilder resBuilder ;
      BSONObj bsonRes ;
      BSONObj bsonAuth ;

      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_LOGIN_NAME, 
                              &pUserName ) ;
      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_LOGIN_PASSWD, 
                              &pPasswd ) ;
      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_TIMESTAMP, &pTime ) ;
      if ( ( NULL == pUserName ) || ( NULL == pPasswd ) || ( NULL == pTime ) )
      {
         rc = SDB_INVALIDARG ;
         _errorDetail = string( OM_REST_FIELD_LOGIN_NAME ) + " or " 
                        + OM_REST_FIELD_LOGIN_PASSWD + " or " 
                        + OM_REST_FIELD_TIMESTAMP + " is null" ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      authBuilder.append( SDB_AUTH_USER, pUserName ) ;
      authBuilder.append( SDB_AUTH_PASSWD, pPasswd ) ;
      bsonAuth = authBuilder.obj() ;
      rc = sdbGetOMManager()->authenticate( bsonAuth, _cb ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_AUTH_AUTHORITY_FORBIDDEN == rc )
         {
            _sendErrorRes2Web( rc, "username or passwd is wrong" ) ;
         }
         else
         {
            _sendErrorRes2Web( rc, "system error" ) ;
         }

         goto error ;
      }

      socket = _restSession->socket() ;
      if ( NULL == socket )
      {
         PD_LOG( PDERROR, "socket is null, impossible" ) ;
         _sendErrorRes2Web( SDB_SYS, "system error" ) ;
         goto error ;
      }

      rc = _restSession->doLogin( pUserName, socket->getLocalIP() ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "do login failed:user=%s, ip=%u", pUserName,
                 socket->getLocalIP() ) ;
         _sendErrorRes2Web( SDB_SYS, "system error" ) ;
         goto error ;
      }

      _restAdaptor->appendHttpHeader( _restSession, OM_REST_HEAD_SESSIONID, 
                                      _restSession->getSessionID() ) ;
      _sendOKRes2Web() ;
   done:
      return rc ;
   error:
      goto done ;
   }

   omLogoutCommand::omLogoutCommand( restAdaptor *pRestAdaptor, 
                                     pmdRestSession *pRestSession )
                   :omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omLogoutCommand::~omLogoutCommand()
   {
   }

   INT32 omLogoutCommand::doCommand()
   {
      _restSession->doLogout() ;
      _sendOKRes2Web() ;
      return SDB_OK ;
   }

   omChangePasswdCommand::omChangePasswdCommand( restAdaptor *pRestAdaptor, 
                                                 pmdRestSession *pRestSession )
                         :omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omChangePasswdCommand::~omChangePasswdCommand()
   {
   }

   INT32 omChangePasswdCommand::_getRestDetail( string &user, string &oldPasswd, 
                                                string &newPasswd, string &time )
   {
      INT32 rc               = SDB_OK ;
      const CHAR *pUser      = NULL ;
      const CHAR *pOldPasswd = NULL ;
      const CHAR *pNewPasswd = NULL ;
      const CHAR *pTime      = NULL ;
      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_LOGIN_NAME, &pUser ) ;
      if ( NULL == pUser )
      {
         rc           = SDB_INVALIDARG ;
         _errorDetail = string( OM_REST_FIELD_LOGIN_NAME ) + " is null" ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_LOGIN_PASSWD, 
                              &pOldPasswd ) ;
      if ( NULL == pOldPasswd )
      {
         rc           = SDB_INVALIDARG ;
         _errorDetail = string( OM_REST_FIELD_LOGIN_PASSWD ) + " is null" ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_NEW_PASSWD, 
                              &pNewPasswd ) ;
      if ( NULL == pNewPasswd )
      {
         rc           = SDB_INVALIDARG ;
         _errorDetail = string( OM_REST_FIELD_NEW_PASSWD ) + " is null" ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_TIMESTAMP, &pTime ) ;
      if ( NULL == pTime )
      {
         rc           = SDB_INVALIDARG ;
         _errorDetail = string( OM_REST_FIELD_TIMESTAMP ) + " is null" ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      user      = pUser ;
      newPasswd = pNewPasswd ;
      oldPasswd = pOldPasswd ;
      time      = pTime ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omChangePasswdCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      string user ;
      string oldPasswd ;
      string newPasswd ;
      string time ;

      BSONObj bsonAuth ;

      rc = _getRestDetail( user, oldPasswd, newPasswd, time ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get rest info failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      if ( user != _restSession->getLoginUserName() )
      {
         rc           = SDB_INVALIDARG ;
         _errorDetail = string( "can't not change other usr's passwd:" )
                        + "myusr=" + _restSession->getLoginUserName()
                        + ",usr=" + user ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      bsonAuth = BSON( SDB_AUTH_USER << user 
                       << SDB_AUTH_PASSWD << oldPasswd ) ;
      rc = sdbGetOMManager()->authenticate( bsonAuth, _cb ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_AUTH_AUTHORITY_FORBIDDEN == rc )
         {
            PD_LOG( PDERROR, "username or passwd is wrong:rc=%d", rc ) ;
            _sendErrorRes2Web( rc, "username or passwd is wrong" ) ;
         }
         else
         {
            PD_LOG( PDERROR, "system error:rc=%d", rc ) ;
            _sendErrorRes2Web( rc, "system error" ) ;
         }

         goto error ;
      }

      rc = sdbGetOMManager()->authUpdatePasswd( user, oldPasswd, newPasswd,
                                                _cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "change passwd failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, "change passwd failed" ) ;
         goto error ;
      }

      _sendOKRes2Web() ;
   done:
      return rc ;
   error:
      goto done ;
   }

   omCheckSessionCommand::omCheckSessionCommand( restAdaptor *pRestAdaptor, 
                                                 pmdRestSession *pRestSession )
                         :omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omCheckSessionCommand::~omCheckSessionCommand()
   {
   }

   INT32 omCheckSessionCommand::doCommand()
   {
      BSONObjBuilder bsonBuilder ;
      const CHAR* sessionID = NULL ;
      _restAdaptor->getHttpHeader( _restSession, OM_REST_HEAD_SESSIONID, 
                                   &sessionID ) ;
      if ( NULL != sessionID && _restSession->isAuthOK() )
      {
         _sendOKRes2Web() ;
      }
      else
      {
         _sendErrorRes2Web( SDB_AUTH_AUTHORITY_FORBIDDEN, "" ) ;
      }

      return SDB_OK ;
   }

   omCreateClusterCommand::omCreateClusterCommand( restAdaptor *pRestAdaptor, 
                                                 pmdRestSession *pRestSession )
                          :omCheckSessionCommand( pRestAdaptor, 
                                                  pRestSession )
   {
   }

   omCreateClusterCommand::~omCreateClusterCommand()
   {
   }

   INT32 omCreateClusterCommand::_getParaOfCreateCluster( string &clusterName, 
                                                          string &desc,
                                                          string &sdbUsr, 
                                                          string &sdbPasswd,
                                                          string &sdbUsrGroup,
                                                          string &installPath )
   {
      const CHAR *pClusterInfo = NULL ;
      BSONObj clusterInfo ;
      INT32 rc                 = SDB_OK ;
      _restAdaptor->getQuery(_restSession, OM_REST_CLUSTER_INFO, 
                             &pClusterInfo ) ;
      if ( NULL == pClusterInfo )
      {
         _errorDetail = string("rest field ") + OM_REST_CLUSTER_INFO
                        + " is null" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      rc = fromjson( pClusterInfo, clusterInfo ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                     "rc=%d", pClusterInfo, rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      clusterName = clusterInfo.getStringField( OM_BSON_FIELD_CLUSTER_NAME ) ;
      desc        = clusterInfo.getStringField( OM_BSON_FIELD_CLUSTER_DESC ) ;
      sdbUsr      = clusterInfo.getStringField( OM_BSON_FIELD_SDB_USER ) ;
      sdbPasswd   = clusterInfo.getStringField( OM_BSON_FIELD_SDB_PASSWD ) ;
      sdbUsrGroup = clusterInfo.getStringField( OM_BSON_FIELD_SDB_USERGROUP ) ;
      installPath = clusterInfo.getStringField( OM_BSON_FIELD_INSTALL_PATH ) ;
      if ( 0 == clusterName.length() || 0 == sdbUsr.length()
           || 0 == sdbPasswd.length() || 0 == sdbUsrGroup.length() )
      {
         _errorDetail = string( OM_BSON_FIELD_CLUSTER_NAME ) + " is null" 
                        + " or " + OM_BSON_FIELD_SDB_USER + " is null"
                        + " or " + OM_BSON_FIELD_SDB_PASSWD + " is null"
                        + " or " + OM_BSON_FIELD_SDB_USERGROUP + " is null" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      if ( 0 == installPath.length() )
      {
         installPath = OM_DEFAULT_INSTALL_PATH ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omCreateClusterCommand::doCommand()
   {
      string clusterName ;
      string desc ;
      string sdbUser ;
      string sdbPasswd ;
      string sdbUserGroup ;
      string sdbinstallPath ;
      BSONObjBuilder resBuilder ;
      BSONObj bsonCluster ;
      INT32 rc                 = SDB_OK ;

      rc = _getParaOfCreateCluster( clusterName, desc, sdbUser, sdbPasswd, 
                                    sdbUserGroup, sdbinstallPath ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get cluster info failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      bsonCluster = BSON( OM_CLUSTER_FIELD_NAME << clusterName
                          << OM_CLUSTER_FIELD_DESC << desc
                          << OM_CLUSTER_FIELD_SDBUSER << sdbUser
                          << OM_CLUSTER_FIELD_SDBPASSWD << sdbPasswd
                          << OM_CLUSTER_FIELD_SDBUSERGROUP << sdbUserGroup
                          << OM_CLUSTER_FIELD_INSTALLPATH << sdbinstallPath ) ;
      rc = rtnInsert( OM_CS_DEPLOY_CL_CLUSTER, bsonCluster, 1, 0, _cb );
      if ( rc )
      {
         if ( SDB_IXM_DUP_KEY == rc )
         {
            PD_LOG_MSG( PDERROR, "%s is already exist:rc=%d", 
                        clusterName.c_str(), rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
         }
         else
         {
            PD_LOG_MSG( PDERROR, "failed to insert cluster:name=%s,rc=%d", 
                        clusterName.c_str(), rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
         }

         goto error ;
      }

      _sendOKRes2Web() ;
   done:
      return rc ;
   error:
      goto done ;
   }

   omQueryClusterCommand::omQueryClusterCommand( restAdaptor *pRestAdaptor, 
                                                 pmdRestSession *pRestSession )
                         : omCreateClusterCommand( pRestAdaptor, pRestSession )
   {
   }

   omQueryClusterCommand::~omQueryClusterCommand()
   {
   }

   INT32 omQueryClusterCommand::doCommand()
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      INT32 rc         = SDB_OK ;
      list<BSONObj> records ;
      list<BSONObj>::iterator iter ;

      rc = _getQueryPara( selector, matcher, order, hint ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _queryTable( OM_CS_DEPLOY_CL_CLUSTER, selector, matcher, order, 
                        hint, 0, 0, -1, records ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         PD_LOG( PDERROR, "query table failed:rc=%d", rc ) ;
      }

      iter = records.begin() ;
      while ( iter != records.end() )
      {
         rc = _restAdaptor->appendHttpBody( _restSession, iter->objdata(), 
                                            iter->objsize(), 1 ) ;
         if ( rc )
         {
            PD_LOG_MSG( PDERROR, "falied to append http body:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
            goto error ;
         }

         iter++ ;
      }

      _sendOKRes2Web() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omScanHostCommand::omScanHostCommand( restAdaptor *pRestAdaptor, 
                                         pmdRestSession *pRestSession, 
                                         const string &localAgentHost, 
                                         const string &localAgentService )
                     : omCreateClusterCommand( pRestAdaptor, pRestSession ),
                       _localAgentHost( localAgentHost ), 
                       _localAgentService( localAgentService )
   {
   }

   omScanHostCommand::~omScanHostCommand()
   {
   }

   void omScanHostCommand::_generateHostList( 
                                             list<omScanHostInfo> &hostInfoList, 
                                             BSONObj &bsonRequest )
   {
      BSONObjBuilder builder ;
      BSONArrayBuilder arrayBuilder ;

      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         omScanHostInfo *pInfo = &(*iter) ;
         BSONObjBuilder builder ;
         if ( pInfo->ip != "" )
         {
            builder.append( OM_BSON_FIELD_HOST_IP, pInfo->ip ) ;
         }

         if ( pInfo->hostName != "" )
         {
            builder.append( OM_BSON_FIELD_HOST_NAME, pInfo->hostName ) ;
         }

         builder.append( OM_BSON_FIELD_HOST_USER, pInfo->user ) ;
         builder.append( OM_BSON_FIELD_HOST_PASSWD, pInfo->passwd ) ;
         builder.append( OM_BSON_FIELD_HOST_SSHPORT, pInfo->sshPort ) ;
         BSONObj oneHost = builder.obj() ;
         arrayBuilder.append( oneHost.copy() ) ;
         iter++ ;
      }

      builder.appendArray( OM_REST_FIELD_HOST_INFO, arrayBuilder.arr() );
      bsonRequest = builder.obj() ;

      return ;
   }

   void omScanHostCommand::_generateArray( list<BSONObj> &hostInfoList, 
                                           const string &arrayKeyName, 
                                           BSONObj &result )
   {
      BSONObjBuilder builder ;
      BSONArrayBuilder arrayBuilder ;

      list<BSONObj>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         arrayBuilder.append( *iter ) ;
         iter++ ;
      }

      builder.appendArray( arrayKeyName.c_str(), arrayBuilder.arr() );
      result = builder.obj() ;

      return ;
   }

   void omScanHostCommand::_sendResult2Web( list<BSONObj> &hostResult )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder bsonBuilder ;

      list<BSONObj>::iterator iter = hostResult.begin() ;
      while ( iter != hostResult.end() )
      {
         rc = _restAdaptor->appendHttpBody( _restSession, iter->objdata(), 
                                       iter->objsize(), 1 ) ;
         if ( rc )
         {
            PD_LOG(PDERROR, "Failed to appendHttpBody:rc=%d", rc ) ;
            _sendErrorRes2Web( rc, "Failed to appendHttpBody" ) ;
            goto error ;
         }

         iter++ ;
      }

      _sendOKRes2Web() ;

   done:
      return ;
   error:
      goto done ;
   }

   void omScanHostCommand::_filterExistHost( list<omScanHostInfo> &hostInfoList, 
                                             list<BSONObj> &hostResult )
   {
      set<string> hostNameSet ;
      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         if ( _isHostExist( *iter ) )
         {
            BSONObj tmp = BSON( OM_REST_RES_RETCODE << SDB_IXM_DUP_KEY
                                << OM_REST_RES_DETAIL << "host is exist"
                                << OM_BSON_FIELD_HOST_IP << iter->ip
                                << OM_BSON_FIELD_SCAN_STATUS 
                                << OM_SCAN_HOST_STATUS_FINISH
                                << OM_BSON_FIELD_HOST_NAME << iter->hostName ) ;
                               
            hostResult.push_back( tmp ) ;
            hostInfoList.erase( iter++ ) ;
            continue ;
         }
         else
         {
            if ( hostNameSet.find( iter->hostName ) != hostNameSet.end()
                 || hostNameSet.find( iter->ip ) != hostNameSet.end() )
            {
               PD_LOG( PDWARNING, "host is duplicate in request, ignored:"
                       "host=%s,ip=%s", iter->hostName.c_str(), 
                       iter->ip.c_str() ) ;
               hostInfoList.erase( iter++ ) ;
               continue ;
            }
            else
            {
               if ( !iter->hostName.empty() )
               {
                  hostNameSet.insert( iter->hostName ) ;
               }
               else if ( !iter->ip.empty() )
               {
                  hostNameSet.insert( iter->ip ) ;
               }
            }
         }

         iter++ ;
      }
   }

   bool omScanHostCommand::_isHostIPExist( const string &ip )
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID   = -1 ;
      INT32 rc           = SDB_OK ;
      rtnContextBuf buffObj ;

      matcher = BSON( OM_HOST_FIELD_IP << ip ) ; ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 0, 
                     _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to query host:rc=%d,host=%s", rc, 
                 matcher.toString().c_str() ) ;
         return false ;
      }

      rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "Failed to retreive record, rc = %d", rc ) ;
         }

         return false ;
      }

      _pRTNCB->contextDelete( contextID, _cb ) ;

      return true ;
   }

   bool omScanHostCommand::_isHostNameExist( const string &hostName )
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID   = -1 ;
      INT32 rc           = SDB_OK ;
      rtnContextBuf buffObj ;

      matcher = BSON( OM_HOST_FIELD_NAME << hostName ) ; ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 0, 
                     _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to query host:rc=%d,host=%s", rc, 
                 matcher.toString().c_str() ) ;
         return false ;
      }

      rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "Failed to retreive record, rc = %d", rc ) ;
         }

         return false ;
      }

      _pRTNCB->contextDelete( contextID, _cb ) ;

      return true ;
   }

   bool omScanHostCommand::_isHostExist( const omScanHostInfo &host )
   {
      if ( host.hostName != "" )
      {
         return _isHostNameExist( host.hostName ) ;
      }

      if ( host.ip != "" )
      {
         return _isHostIPExist( host.ip );
      }

      return false ;
   }

   INT32 omScanHostCommand::_checkRestHostInfo( BSONObj &hostInfo )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele = hostInfo.getField( OM_BSON_FIELD_HOST_USER ) ;
      if ( ele.type() != String )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "hostinfo field is not string:field=%s,type=%d",
                     OM_BSON_FIELD_HOST_USER, String ) ;
         goto error ;
      }

      ele = hostInfo.getField( OM_BSON_FIELD_HOST_PASSWD ) ;
      if ( ele.type() != String )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "hostinfo field is not string:field=%s,type=%d",
                     OM_BSON_FIELD_HOST_PASSWD, String ) ;
         goto error ;
      }

      ele = hostInfo.getField( OM_BSON_FIELD_HOST_SSHPORT ) ;
      if ( ele.type() != String )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "hostinfo field is not string:field=%s,type=%d",
                     OM_BSON_FIELD_HOST_SSHPORT, String ) ;
         goto error ;
      }

      ele = hostInfo.getField( OM_BSON_FIELD_CLUSTER_NAME ) ;
      if ( ele.type() != String )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "hostinfo field is not string:field=%s,type=%d",
                     OM_BSON_FIELD_CLUSTER_NAME, String ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omScanHostCommand::_getScanHostList( string &clusterName, 
                                              list<omScanHostInfo> &hostInfo )
   {
      INT32 rc                     = SDB_OK ;
      const CHAR* pGlobalUser      = NULL ;
      const CHAR* pGlobalPasswd    = NULL ;
      const CHAR* pGlobalSshPort   = NULL ;
      const CHAR* pGlobalAgentPort = NULL ;
      const CHAR* pHostInfo        = NULL ;
      BSONObj bsonHostInfo ;
      BSONElement element ;

      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_HOST_INFO, 
                              &pHostInfo ) ;
      if ( NULL == pHostInfo )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "rest field is null:field=%s", 
                     OM_REST_FIELD_HOST_INFO ) ;
         goto error ;
      }

      rc = fromjson( pHostInfo, bsonHostInfo ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                     "rc=%d", pHostInfo, rc ) ;
         goto error ;
      }

      rc = _checkRestHostInfo( bsonHostInfo ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "check rest hostinfo failed:rc=%d", rc ) ;
         goto error ;
      }

      pGlobalUser    = bsonHostInfo.getStringField( OM_BSON_FIELD_HOST_USER ) ;
      pGlobalPasswd  = bsonHostInfo.getStringField( 
                                                OM_BSON_FIELD_HOST_PASSWD ) ;
      pGlobalSshPort = bsonHostInfo.getStringField( 
                                                OM_BSON_FIELD_HOST_SSHPORT ) ;
      pGlobalAgentPort = _localAgentService.c_str() ;
      clusterName      = bsonHostInfo.getStringField( 
                                                OM_BSON_FIELD_CLUSTER_NAME ) ;
      if ( 0 == ossStrlen( pGlobalUser ) || 0 == ossStrlen( pGlobalPasswd )
           || 0 == ossStrlen( pGlobalSshPort ) || 0 == clusterName.length()
           || 0 == ossStrlen( pGlobalAgentPort ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "rest field have not been set:field="
                     "%s or %s or %s or %s or %s", OM_BSON_FIELD_HOST_USER,
                     OM_BSON_FIELD_HOST_PASSWD, OM_BSON_FIELD_HOST_SSHPORT,
                     OM_BSON_FIELD_CLUSTER_NAME ) ;
         goto error ;
      }

      element = bsonHostInfo.getField( OM_BSON_FIELD_HOST_INFO ) ;
      if ( element.isNull() || Array != element.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is not array type:type=%d", 
                     OM_BSON_FIELD_HOST_INFO, element.type() ) ;
         goto error ;
      }

      {
         BSONObjIterator i( element.embeddedObject() ) ;
         while ( i.more() )
         {
            omScanHostInfo host ;
            BSONObjBuilder builder ;
            BSONObj tmp ;
            BSONElement ele = i.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "host info is invalid, element of %s "
                           "is not Object type:type=%d", 
                           OM_BSON_FIELD_HOST_INFO, ele.type() ) ;
               goto error ;
            }
            BSONObj oneHost = ele.embeddedObject() ;

            host.ip        = oneHost.getStringField( OM_BSON_FIELD_HOST_IP ) ;
            host.hostName  = oneHost.getStringField( OM_BSON_FIELD_HOST_NAME ) ;
            host.user      = pGlobalUser ;
            host.passwd    = pGlobalPasswd ;
            host.sshPort   = pGlobalSshPort ;
            host.agentPort = pGlobalAgentPort ;

            if ( oneHost.hasField( OM_BSON_FIELD_HOST_USER ) )
            {
               host.user = oneHost.getStringField( OM_BSON_FIELD_HOST_USER ) ;
            }
            if ( oneHost.hasField( OM_BSON_FIELD_HOST_PASSWD ) )
            {
               host.passwd = oneHost.getStringField( 
                                                  OM_BSON_FIELD_HOST_PASSWD ) ;
            }
            if ( oneHost.hasField( OM_BSON_FIELD_HOST_SSHPORT ) )
            {
               host.sshPort = oneHost.getStringField( 
                                                  OM_BSON_FIELD_HOST_SSHPORT ) ;
            }
            if ( oneHost.hasField( OM_BSON_FIELD_AGENT_PORT ) )
            {
               host.agentPort = oneHost.getStringField( 
                                                  OM_BSON_FIELD_AGENT_PORT ) ;
            }

            if ( host.ip=="" && host.hostName=="" )
            {
               _errorDetail = string( "rest field miss:" ) 
                              + OM_BSON_FIELD_HOST_IP + " or " 
                              + OM_BSON_FIELD_HOST_NAME ;
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "rest field miss:field=%s or %s", 
                           OM_BSON_FIELD_HOST_IP, OM_BSON_FIELD_HOST_NAME ) ;
               goto error ;
            }

            hostInfo.push_back( host ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omScanHostCommand::_clearSession( omManager *om, 
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
         om->getRSManager()->removeSession( remoteSession ) ;
      }
   }

   INT32 omScanHostCommand::_notifyAgentTask( INT64 taskID )
   {
      INT32 rc          = SDB_OK ;
      SINT32 flag       = SDB_OK ;
      CHAR *pContent    = NULL ;
      INT32 contentSize = 0 ;
      MsgHeader *pMsg   = NULL ;
      omManager *om     = NULL ;
      pmdRemoteSession *remoteSession = NULL ;
      BSONObj bsonRequest ;
      BSONObj result ;

      bsonRequest = BSON( OM_TASKINFO_FIELD_TASKID << taskID ) ;
      rc = msgBuildQueryMsg( &pContent, &contentSize, 
                             CMD_ADMIN_PREFIX OM_NOTIFY_TASK, 
                             0, 0, 0, -1, &bsonRequest, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build msg failed:cmd=%s,rc=%d", OM_NOTIFY_TASK,
                     rc ) ;
         goto error ;
      }

      om            = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                      OM_WAIT_SCAN_RES_INTERVAL,
                                                      NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "create remote session failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         goto error ;
      }

      pMsg = (MsgHeader *)pContent ;
      rc   = _sendMsgToLocalAgent( om, remoteSession, pMsg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "send message to agent failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         remoteSession->clearSubSession() ;
         goto error ;
      }

      rc = _receiveFromAgent( remoteSession, flag, result ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "receive from agent failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         string tmpError = result.getStringField( OM_REST_RES_DETAIL ) ;
         PD_LOG_MSG( PDERROR, "agent process failed:detail=(%s),rc=%d", 
                     tmpError.c_str(), rc ) ;
         goto error ;
      }
   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omScanHostCommand::_sendMsgToLocalAgent( omManager *om,
                                             pmdRemoteSession *remoteSession, 
                                             MsgHeader *pMsg )
   {
      MsgRouteID localAgentID ;
      INT32 rc = SDB_OK ;

      localAgentID = om->updateAgentInfo( _localAgentHost, 
                                          _localAgentService ) ;
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

   INT32 omScanHostCommand::_parseResonpse( VEC_SUB_SESSIONPTR &subSessionVec, 
                                            BSONObj &response,
                                            list<BSONObj> &bsonResult )
   {
      INT32 rc           = SDB_OK ;
      BSONElement hostElement ;

      hostElement = response.getField( OM_BSON_FIELD_HOST_INFO ) ;
      if ( hostElement.isNull() || Array != hostElement.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "agent's response is unrecognized, %s is not "
                     "array type:type=%d", OM_BSON_FIELD_HOST_INFO, 
                     hostElement.type()) ;
         goto error ;
      }
      {
         BSONObjIterator i( hostElement.embeddedObject() ) ;
         while ( i.more() )
         {
            BSONObjBuilder builder ;
            BSONObj tmp ;
            BSONElement ele = i.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "rest info is invalid, element of %s "
                           "is not Object type:type=%d", 
                           OM_BSON_FIELD_HOST_INFO, ele.type() ) ;
               goto error ;
            }

            BSONObj oneHost = ele.embeddedObject() ;
            bsonResult.push_back( oneHost ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omScanHostCommand::doCommand()
   {
      string clusterName ;
      list<omScanHostInfo> hostInfoList ;
      INT32 rc                        = SDB_OK ;
      CHAR* pContent                  = NULL ;
      INT32 contentSize               = 0 ;
      SINT32 flag                     = SDB_OK ;
      MsgHeader *pMsg                 = NULL ;
      pmdRemoteSession *remoteSession = NULL ;
      BSONObj bsonRequest ;
      BSONObj bsonResponse ;
      list<BSONObj> bsonResult ;
      pmdEDUEvent eventData ;
      omManager *om                   = NULL ;
      VEC_SUB_SESSIONPTR subSessionVec ;

      rc = _getScanHostList( clusterName, hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get host list failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _filterExistHost( hostInfoList, bsonResult ) ;
      if ( hostInfoList.size() == 0 )
      {
         _sendResult2Web( bsonResult ) ;
         goto done ;
      }

      _generateHostList( hostInfoList, bsonRequest ) ;
      rc = msgBuildQueryMsg( &pContent, &contentSize, 
                             CMD_ADMIN_PREFIX OM_SCAN_HOST_REQ, 
                             0, 0, 0, -1, &bsonRequest, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = string( "build message failed:cmd=" ) 
                        + OM_SCAN_HOST_REQ ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      om   = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                      OM_WAIT_SCAN_RES_INTERVAL,
                                                      NULL ) ;
      if ( NULL == remoteSession )
      {
         SDB_OSS_FREE( pContent ) ;

         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "create remove session failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      pMsg = (MsgHeader *)pContent ;
      rc   = _sendMsgToLocalAgent( om, remoteSession, pMsg ) ;
      if ( SDB_OK != rc )
      {
         SDB_OSS_FREE( pContent ) ;
         remoteSession->clearSubSession() ;

         PD_LOG_MSG( PDERROR, "send message to agent failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _receiveFromAgent( remoteSession, flag, bsonResponse ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "receive from agent failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         _errorDetail = bsonResponse.getStringField( OM_REST_RES_DETAIL ) ;
         PD_LOG( PDERROR, "agent process failed:detail=%s,rc=%d", 
                 _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _parseResonpse( subSessionVec, bsonResponse, bsonResult ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_parseResonpse failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendResult2Web( bsonResult ) ;

   done:
      _clearSession( om, remoteSession ) ;
      return rc; 
   error:
      goto done ;
   }

   omCheckHostCommand::omCheckHostCommand( restAdaptor *pRestAdaptor, 
                                           pmdRestSession *pRestSession,
                                           const string &localAgentHost, 
                                           const string &localAgentService )
                      : omScanHostCommand( pRestAdaptor, pRestSession, 
                                           localAgentHost, localAgentService )
   {
   }

   omCheckHostCommand::~omCheckHostCommand()
   {
   }

   void omCheckHostCommand::_eraseFromListByIP( 
                                            list<omScanHostInfo> &hostInfoList, 
                                            const string &ip )
   {
      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         if ( iter->ip == ip )
         {
            hostInfoList.erase( iter ) ;
            return ;
         }
         iter++ ;
      }
   }

   void omCheckHostCommand::_updateAgentService( 
                                            list<omScanHostInfo> &hostInfoList, 
                                            const string &ip, 
                                            const string &port )
   {
      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         if ( iter->ip == ip )
         {
            iter->agentPort = port ;
            return ;
         }
         iter++ ;
      }
   }

   void omCheckHostCommand::_eraseFromListByHost( 
                                             list<omScanHostInfo> &hostInfoList, 
                                             const string &hostName )
   {
      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         if ( iter->hostName == hostName )
         {
            hostInfoList.erase( iter ) ;
            return ;
         }
         iter++ ;
      }
   }

   void omCheckHostCommand::_eraseFromList( list<omScanHostInfo> &hostInfoList, 
                                            BSONObj &oneHost )
   {
      string hostName = oneHost.getStringField( OM_BSON_FIELD_HOST_NAME ) ;
      _eraseFromListByHost( hostInfoList, hostName ) ;
   }

   void omCheckHostCommand::_updateUninstallFlag( 
                                            list<omScanHostInfo> &hostInfoList, 
                                            const string &ip, 
                                            const string &agentPort,
                                            bool isNeedUninstall )
   {
      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         if ( iter->ip == ip )
         {
            if ( agentPort != "" )
            {
               iter->agentPort       = agentPort ;
            }

            iter->isNeedUninstall = isNeedUninstall ;
            return ;
         }
         iter++ ;
      }
   }

   INT32 omCheckHostCommand::_installAgent( list<omScanHostInfo> &hostInfoList, 
                                            list<BSONObj> &hostResult )
   {
      INT32 rc          = SDB_OK ;
      SINT32 flag       = SDB_OK ;
      CHAR *pContent    = NULL ;
      INT32 contentSize = 0 ;
      MsgHeader *pMsg   = NULL ;
      omManager *om     = NULL ;
      pmdRemoteSession *remoteSession = NULL ;
      BSONObj bsonRequest ;
      BSONObj result ;
      BSONObj hostResults ;
      BSONElement rcElement ;

      _generateHostList( hostInfoList, bsonRequest ) ;
      rc = msgBuildQueryMsg( &pContent, &contentSize, 
                             CMD_ADMIN_PREFIX OM_PRE_CHECK_HOST, 
                             0, 0, 0, -1, &bsonRequest, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build query msg failed:cmd=%s,rc=%d", 
                     OM_PRE_CHECK_HOST, rc ) ;
         goto error ;
      }

      om            = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                      OM_INSTALL_AGET_INTERVAL,
                                                      NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "addSession failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         goto error ;
      }

      pMsg = (MsgHeader *)pContent ;
      rc   = _sendMsgToLocalAgent( om, remoteSession, pMsg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "send message to local agent failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         remoteSession->clearSubSession() ;
         goto error ;
      }

      rc = _receiveFromAgent( remoteSession, flag, result ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "receive from agent failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         PD_LOG_MSG( PDERROR, "agent process failed:detail=%s,rc=%d", 
                     result.getStringField( OM_REST_RES_DETAIL ), rc ) ;
         goto error ;
      }

      hostResults = result.getObjectField( OM_BSON_FIELD_HOST_INFO ) ;
      {
         BSONObjIterator iter( hostResults ) ;
         while ( iter.more() )
         {
            BSONElement ele   = iter.next() ;
            BSONObj oneResult = ele.embeddedObject() ;
            INT32 rc    = oneResult.getIntField( OM_REST_RES_RETCODE ) ;
            string ip   = oneResult.getStringField( OM_BSON_FIELD_HOST_IP ) ;
            string port = oneResult.getStringField( OM_BSON_FIELD_AGENT_PORT ) ;
            if ( SDB_OK != rc )
            {
               hostResult.push_back( oneResult ) ;
               _eraseFromListByIP( hostInfoList, ip ) ;
            }
            else
            {
               _updateAgentService( hostInfoList, ip, port ) ;
            }
         }
      }

   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omCheckHostCommand::_addCheckHostReq( omManager *om,
                                           pmdRemoteSession *remoteSession,
                                           list<omScanHostInfo> &hostInfoList ) 
   {
      INT32 rc = SDB_OK ;
      _id2Host.clear() ;
      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         MsgRouteID routeID ;
         BSONObj bsonRequest ;
         pmdSubSession *subSession = NULL ;
         CHAR *pContent            = NULL ;
         INT32 contentSize         = 0 ;

         routeID   = om->updateAgentInfo( iter->ip, iter->agentPort ) ;
         subSession = remoteSession->addSubSession( routeID.value ) ;
         if ( NULL == subSession )
         {
            rc = SDB_OOM ;
            PD_LOG( PDERROR, "addSubSessin failed" ) ;
            goto error ;
         }

         bsonRequest = BSON( OM_BSON_FIELD_HOST_IP << iter->ip 
                             << OM_BSON_FIELD_HOST_NAME << iter->hostName 
                             << OM_BSON_FIELD_HOST_USER << iter->user
                             << OM_BSON_FIELD_HOST_PASSWD << iter->passwd ) ;
         rc = msgBuildQueryMsg( &pContent, &contentSize, 
                                CMD_ADMIN_PREFIX OM_CHECK_HOST_REQ,
                                0, 0, 0, -1, &bsonRequest, NULL, NULL, NULL ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "msgBuildQueryMsg failed:rc=%d", rc ) ;
            goto error ;
         }

         _id2Host[routeID.value] = *iter ;

         subSession->setReqMsg( (MsgHeader *)pContent ) ;
         iter++;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omCheckHostCommand::_updateDiskInfo( BSONObj &onehost )
   {
      BSONObj filter = BSON( OM_BSON_FIELD_DISK << "" ) ;
      BSONObj others ;
      BSONObj disk ;
      others = onehost.filterFieldsUndotted( filter, false ) ;
      disk   = onehost.filterFieldsUndotted( filter, true ) ;

      BSONArrayBuilder arrayBuilder ;

      BSONObj disks = disk.getObjectField( OM_BSON_FIELD_DISK ) ;
      BSONObjIterator iter( disks ) ;
      while ( iter.more() )
      {
         BSONElement ele ;
         BSONObj oneDisk ;
         ele     = iter.next() ;
         oneDisk = ele.embeddedObject() ;

         BSONObjBuilder builder ;
         builder.appendElements( oneDisk ) ;

         BSONElement sizeEle ;
         sizeEle = oneDisk.getField( OM_BSON_FIELD_DISK_FREE_SIZE ) ;
         INT64 freeSize = sizeEle.numberLong();
         if ( freeSize < OM_MIN_DISK_FREE_SIZE )
         {
            builder.append( OM_BSON_FIELD_DISK_CANUSE, false ) ;
         }
         else
         {
            builder.append( OM_BSON_FIELD_DISK_CANUSE, true ) ;
         }

         BSONObj tmp = builder.obj() ;
         arrayBuilder.append( tmp ) ;
      }

      disk = BSON( OM_BSON_FIELD_DISK << arrayBuilder.arr() ) ;

      BSONObjBuilder builder ;
      builder.appendElements( others ) ;
      builder.appendElements( disk ) ;
      onehost = builder.obj() ;
   }

   INT32 omCheckHostCommand::_checkResFormat( BSONObj &oneHost )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;

      ele = oneHost.getField( OM_REST_RES_RETCODE ) ;
      if ( ele.isNumber() )
      {
         INT32 innerRC = ele.Int() ;
         if ( innerRC != SDB_OK )
         {
            goto done ;
         }
      }

      rc = _checkHostBasicContent( oneHost ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_checkHostBasicContent failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omCheckHostCommand::_errorCheckHostEnv( 
                                          list<omScanHostInfo> &hostInfoList,
                                          list<BSONObj> &hostResult, 
                                          MsgRouteID id, int flag,
                                          const string &error )
   {
      string hostName = "" ;
      string ip       = "" ;
      map<UINT64, omScanHostInfo>::iterator iter = _id2Host.find( id.value ) ;
      SDB_ASSERT( iter != _id2Host.end() , "" ) ;
      if ( iter != _id2Host.end() )
      {
         omScanHostInfo info = iter->second ;
         hostName = info.hostName ;
         ip       = info.ip ;
      }

      BSONObj result = BSON( OM_BSON_FIELD_HOST_IP << ip
                             << OM_BSON_FIELD_HOST_NAME << hostName
                             << OM_REST_RES_RETCODE << flag 
                             << OM_REST_RES_DETAIL << error ) ;
      hostResult.push_back( result ) ;
      _eraseFromListByIP( hostInfoList, ip ) ;

      PD_LOG( PDERROR, "check host error:%s,agent=%s,rc=%d", error.c_str(), 
              hostName.c_str(), flag ) ;
   }

   INT32 omCheckHostCommand::_checkHostEnv( list<omScanHostInfo> &hostInfoList, 
                                            list<BSONObj> &hostResult )
   {
      INT32 rc          = SDB_OK ;
      omManager *om     = NULL ;
      pmdRemoteSession *remoteSession = NULL ;
      VEC_SUB_SESSIONPTR subSessionVec ;
      INT32 sucNum   = 0 ; 
      INT32 totalNum = 0 ;

      om            = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                      OM_CHECK_HOST_INTERVAL,
                                                      NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "addSession failed:rc=%d", rc ) ;
         goto error ;
      }
      rc = _addCheckHostReq( om, remoteSession, hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "generate check host request failed:rc=%d", rc ) ;
         goto done ;
      }

      remoteSession->sendMsg( &sucNum, &totalNum ) ;
      rc = _getAllReplay( remoteSession, &subSessionVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "wait agent's replay failed:rc=%d", rc ) ;
         goto error ;
      }

      for ( UINT32 i = 0 ; i < subSessionVec.size() ; i++ )
      {
         rc = SDB_OK ;
         vector<BSONObj> objVec ;
         SINT32 flag               = SDB_OK ;
         SINT64 contextID          = -1 ;
         SINT32 startFrom          = 0 ;
         SINT32 numReturned        = 0 ;
         MsgHeader* pRspMsg        = NULL ;
         pmdSubSession *subSession = subSessionVec[i] ;
         string errorMsg ;
         if ( subSession->isDisconnect() )
         {
            PD_LOG( PDERROR, "session disconnected:id=%s,rc=%d", 
                    routeID2String(subSession->getNodeID()).c_str(), rc ) ;
            continue ;
         }

         pRspMsg = subSession->getRspMsg() ;
         if ( NULL == pRspMsg )
         {
            errorMsg = "receive null response from agent" ;
            PD_LOG( PDERROR, "%s", errorMsg.c_str() ) ;
            _errorCheckHostEnv( hostInfoList, hostResult, 
                                subSession->getNodeID(), SDB_UNEXPECTED_RESULT,
                                errorMsg ) ;
            continue ;
         }

         rc = msgExtractReply( (CHAR *)pRspMsg, &flag, &contextID, &startFrom, 
                               &numReturned, objVec ) ;
         if ( SDB_OK != rc )
         {
            errorMsg = "extract reply failed" ;
            PD_LOG( PDERROR, "%s:rc=%d", errorMsg.c_str(), rc ) ;
            _errorCheckHostEnv( hostInfoList, hostResult, 
                                subSession->getNodeID(), rc, errorMsg ) ;
            continue ;
         }

         if ( SDB_OK != flag )
         {
            if ( objVec.size() > 0 )
            {
               errorMsg = objVec[0].getStringField( OM_REST_RES_DETAIL ) ;
            }
            _errorCheckHostEnv( hostInfoList, hostResult, 
                                subSession->getNodeID(), flag, errorMsg ) ;
            continue ;
         }

         if ( 1 != objVec.size() )
         {
            errorMsg = "unexpected response size" ;
            _errorCheckHostEnv( hostInfoList, hostResult, 
                                subSession->getNodeID(), SDB_UNEXPECTED_RESULT, 
                                errorMsg ) ;
            continue ;
         }

         BSONObj result = objVec[0] ;
         rc = _checkResFormat( result ) ;
         if ( SDB_OK != rc )
         {
            errorMsg = "agent's response format error" ;
            _errorCheckHostEnv( hostInfoList, hostResult, 
                                subSession->getNodeID(), rc, errorMsg ) ;
            continue ;
         }

         _updateDiskInfo( result ) ;
         hostResult.push_back( result ) ;
         _eraseFromList( hostInfoList, result ) ;
      }

      rc = SDB_OK ;

      {
         list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
         while ( iter != hostInfoList.end() )
         {
            BSONObj tmp = BSON( OM_BSON_FIELD_HOST_IP << iter->ip 
                                << OM_BSON_FIELD_HOST_NAME << iter->hostName 
                                << OM_REST_RES_RETCODE << SDB_NETWORK 
                                << OM_REST_RES_DETAIL << "network error" ) ;
            hostResult.push_back( tmp ) ;
            hostInfoList.erase( iter++ ) ;
         }
      }

   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omCheckHostCommand::_addAgentExitReq( omManager *om,
                                            pmdRemoteSession *remoteSession,
                                            list<omScanHostInfo> &hostInfoList ) 
   {
      INT32 rc = SDB_OK ;
      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         if ( iter->isNeedUninstall )
         {
            MsgRouteID routeID ;
            pmdSubSession *subSession = NULL ;
            CHAR *pContent            = NULL ;
            INT32 contentSize         = 0 ;
            routeID   = om->updateAgentInfo( iter->ip, iter->agentPort ) ;
            subSession = remoteSession->addSubSession( routeID.value ) ;
            if ( NULL == subSession )
            {
               rc = SDB_OOM ;
               PD_LOG( PDERROR, "addSubSessin failed" ) ;
               goto error ;
            }

            rc = msgBuildQueryMsg( &pContent, &contentSize, 
                                   CMD_ADMIN_PREFIX CMD_NAME_SHUTDOWN,
                                   0, 0, 0, -1, NULL, NULL, NULL, NULL ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "msgBuildQueryMsg failed:rc=%d", rc ) ;
               goto error ;
            }

            subSession->setReqMsg( (MsgHeader *)pContent ) ;
         }

         iter++;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omCheckHostCommand::_notifyAgentExit( 
                                            list<omScanHostInfo> &hostInfoList )
   {
      INT32 rc                        = SDB_OK ;
      omManager *om                   = NULL ;
      pmdRemoteSession *remoteSession = NULL ;
      INT32 sucNum                    = 0 ; 
      INT32 totalNum                  = 0 ;
      VEC_SUB_SESSIONPTR subSessionVec ;

      om            = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                OM_WAIT_AGENT_EXIT_RES_INTERVAL,
                                                NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "addSession failed" ) ;
         goto error ;
      }
      rc = _addAgentExitReq( om, remoteSession, hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_addAgentExitReq failed:rc=%d", rc ) ;
         goto done ;
      }

      remoteSession->sendMsg( &sucNum, &totalNum ) ;
      _getAllReplay( remoteSession, &subSessionVec ) ;
   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   void omCheckHostCommand::_generateUninstallReq( 
                                             list<omScanHostInfo> &hostInfoList, 
                                             BSONObj &bsonRequest )
   {
      BSONObjBuilder builder ;
      BSONArrayBuilder arrayBuilder ;

      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         omScanHostInfo *pInfo = &(*iter) ;
         if ( pInfo->isNeedUninstall )
         {
            BSONObjBuilder builder ;
            if ( pInfo->ip != "" )
            {
               builder.append( OM_BSON_FIELD_HOST_IP, pInfo->ip ) ;
            }

            if ( pInfo->hostName != "" )
            {
               builder.append( OM_BSON_FIELD_HOST_NAME, pInfo->hostName ) ;
            }

            builder.append( OM_BSON_FIELD_HOST_USER, pInfo->user ) ;
            builder.append( OM_BSON_FIELD_HOST_PASSWD, pInfo->passwd ) ;
            builder.append( OM_BSON_FIELD_HOST_SSHPORT, pInfo->sshPort ) ;
            BSONObj oneHost = builder.obj() ;
            arrayBuilder.append( oneHost.copy() ) ;
         }

         iter++ ;
      }

      builder.appendArray( OM_REST_FIELD_HOST_INFO, arrayBuilder.arr() );
      bsonRequest = builder.obj() ;

      return ;
   }

   bool omCheckHostCommand::_isNeedUnistall( 
                                            list<omScanHostInfo> &hostInfoList )
   {
      bool isNeedUninsall = false ;
      list<omScanHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         if ( iter->isNeedUninstall )
         {
            isNeedUninsall = true ;
            break ;
         }
         iter++ ;
      }

      return isNeedUninsall ;
   }

   INT32 omCheckHostCommand::_uninstallAgent( 
                                            list<omScanHostInfo> &hostInfoList )
   {
      INT32 rc          = SDB_OK ;
      CHAR *pContent    = NULL ;
      INT32 contentSize = 0 ;
      MsgHeader *pMsg   = NULL ;
      omManager *om     = NULL ;
      pmdRemoteSession *remoteSession = NULL ;
      BSONObj bsonRequest ;
      VEC_SUB_SESSIONPTR subSessionVec ;

      if ( !_isNeedUnistall( hostInfoList ) )
      {
         goto done ;
      }

      _notifyAgentExit( hostInfoList ) ;

      _generateUninstallReq( hostInfoList, bsonRequest ) ;
      rc = msgBuildQueryMsg( &pContent, &contentSize, 
                             CMD_ADMIN_PREFIX OM_POST_CHECK_HOST, 
                             0, 0, 0, -1, &bsonRequest, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG(PDERROR, "build query msg failed:cmd=%s,rc=%d", 
                OM_POST_CHECK_HOST, rc ) ;
         goto error ;
      }

      om            = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                OM_WAIT_AGENT_UNISTALL_INTERVAL,
                                                NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "addSession failed" ) ;
         SDB_OSS_FREE( pContent ) ;
         goto error ;
      }

      pMsg = (MsgHeader *)pContent ;
      rc   = _sendMsgToLocalAgent( om, remoteSession, pMsg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "send message to agent failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         remoteSession->clearSubSession() ;
         goto error ;
      }

      remoteSession->waitReply( TRUE, &subSessionVec ) ;

   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omCheckHostCommand::_doCheck( list<omScanHostInfo> &hostInfoList, 
                                       list<BSONObj> &hostResult )
   {
      INT32 rc = SDB_OK ;
      list<omScanHostInfo> agentInstalledInfo ;
      PD_LOG( PDEVENT, "start to _installAgent" ) ;
      rc = _installAgent( hostInfoList, hostResult ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG(PDERROR, "install agent failed:rc=%d", rc ) ;
         goto error ;
      }

      agentInstalledInfo.assign( hostInfoList.begin(), hostInfoList.end() ) ;
      PD_LOG( PDEVENT, "start to _checkHostEnv" ) ;
      rc = _checkHostEnv( hostInfoList, hostResult ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG(PDERROR, "check host env failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      PD_LOG( PDEVENT, "start to _uninstallAgent" ) ;
      _uninstallAgent( agentInstalledInfo ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omCheckHostCommand::_getCheckHostList( string &clusterName, 
                                                list<omScanHostInfo> &hostInfo )
   {
      INT32 rc                     = SDB_OK ;
      const CHAR* pGlobalUser      = NULL ;
      const CHAR* pGlobalPasswd    = NULL ;
      const CHAR* pGlobalSshPort   = NULL ;
      const CHAR* pGlobalAgentPort = NULL ;
      const CHAR* pHostInfo        = NULL ;
      BSONObj bsonHostInfo ;
      BSONElement element ;


      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_HOST_INFO, 
                              &pHostInfo ) ;
      if ( NULL == pHostInfo )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is null:rc=%d", OM_REST_FIELD_HOST_INFO, 
                     rc ) ;
         goto error ;
      }

      rc = fromjson( pHostInfo, bsonHostInfo ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                     "rc=%d", pHostInfo, rc ) ;
         goto error ;
      }

      pGlobalUser    = bsonHostInfo.getStringField( OM_BSON_FIELD_HOST_USER ) ;
      pGlobalPasswd  = bsonHostInfo.getStringField( 
                                                OM_BSON_FIELD_HOST_PASSWD ) ;
      pGlobalSshPort = bsonHostInfo.getStringField( 
                                                OM_BSON_FIELD_HOST_SSHPORT ) ;
      pGlobalAgentPort = _localAgentService.c_str() ;
      clusterName      = bsonHostInfo.getStringField( 
                                                OM_BSON_FIELD_CLUSTER_NAME ) ;
      if ( 0 == ossStrlen( pGlobalUser ) || 0 == ossStrlen( pGlobalPasswd )
           || 0 == ossStrlen( pGlobalSshPort ) || 0 == clusterName.length() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "parameter is empty:%s=%s,%s=%d,%s=%s,%s=%s", 
                     OM_BSON_FIELD_HOST_USER, pGlobalUser, 
                     OM_BSON_FIELD_HOST_PASSWD, ossStrlen( pGlobalPasswd ),
                     OM_BSON_FIELD_HOST_SSHPORT, pGlobalSshPort,
                     OM_BSON_FIELD_CLUSTER_NAME, clusterName.c_str() ) ;
         goto error ;
      }

      element = bsonHostInfo.getField( OM_BSON_FIELD_HOST_INFO ) ;
      if ( Array != element.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is not array type:type=%d", 
                     OM_BSON_FIELD_HOST_INFO, element.type() ) ;
         goto error ;
      }

      {
         BSONObjIterator i( element.embeddedObject() ) ;
         while ( i.more() )
         {
            omScanHostInfo host;
            BSONObjBuilder builder ;
            BSONObj tmp ;
            BSONElement ele = i.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "rest info is invalid, element of %s "
                           "is not Object type:type=%d", 
                           OM_BSON_FIELD_HOST_INFO, ele.type() ) ;
               goto error ;
            }

            BSONObj oneHost = ele.embeddedObject() ;

            host.ip        = oneHost.getStringField( OM_BSON_FIELD_HOST_IP ) ;
            host.hostName  = oneHost.getStringField( OM_BSON_FIELD_HOST_NAME ) ;
            host.user      = pGlobalUser ;
            host.passwd    = pGlobalPasswd ;
            host.sshPort   = pGlobalSshPort ;
            host.agentPort = pGlobalAgentPort ;
            host.isNeedUninstall = true ;

            if ( oneHost.hasField( OM_BSON_FIELD_HOST_USER ) )
            {
               host.user = oneHost.getStringField( OM_BSON_FIELD_HOST_USER ) ;
            }
            if ( oneHost.hasField( OM_BSON_FIELD_HOST_PASSWD ) )
            {
               host.passwd = oneHost.getStringField( 
                                                  OM_BSON_FIELD_HOST_PASSWD ) ;
            }
            if ( oneHost.hasField( OM_BSON_FIELD_HOST_SSHPORT ) )
            {
               host.sshPort = oneHost.getStringField( 
                                                  OM_BSON_FIELD_HOST_SSHPORT ) ;
            }
            if ( oneHost.hasField( OM_BSON_FIELD_AGENT_PORT ) )
            {
               host.agentPort = oneHost.getStringField( 
                                                  OM_BSON_FIELD_AGENT_PORT ) ;
            }

            if ( host.ip=="" || host.hostName=="" )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "rest field miss:%s or %s", 
                           OM_BSON_FIELD_HOST_IP, OM_BSON_FIELD_HOST_NAME ) ;
               goto error ;
            }

            hostInfo.push_back( host ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omCheckHostCommand::doCommand()
   {
      list<omScanHostInfo> hostInfoList ;
      list<BSONObj> hostResult ;
      BSONObj bsonRequest ;
      string clusterName = "" ;
      INT32 rc           = SDB_OK ;

      rc = _getCheckHostList( clusterName, hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "fail to get host list:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _filterExistHost( hostInfoList, hostResult ) ;
      if ( hostInfoList.size() == 0 )
      {
         _sendResult2Web( hostResult ) ;
         goto done ;
      }

      rc = _doCheck( hostInfoList, hostResult ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "do check failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendResult2Web( hostResult ) ;

   done:
      return rc; 
   error:
      goto done ;
   }

   omAddHostCommand::omAddHostCommand( restAdaptor *pRestAdaptor, 
                                       pmdRestSession *pRestSession,
                                       const string &localAgentHost, 
                                       const string &localAgentService )
                    : omScanHostCommand( pRestAdaptor, pRestSession, 
                                         localAgentHost, localAgentService )
   {
   }

   omAddHostCommand::~omAddHostCommand()
   {
   }

   INT32 omAddHostCommand::_getRestHostList( string &clusterName, 
                                             list<BSONObj> &hostInfo )
   {
      INT32 rc                     = SDB_OK ;
      const CHAR* pGlobalUser      = NULL ;
      const CHAR* pGlobalPasswd    = NULL ;
      const CHAR* pGlobalSshPort   = NULL ;
      const CHAR* pGlobalAgentPort = NULL ;
      const CHAR* pHostInfo        = NULL ;
      string installPath ;
      BSONObj bsonHostInfo ;
      BSONElement element ;

      _restAdaptor->getQuery(_restSession, OM_REST_FIELD_HOST_INFO, 
                             &pHostInfo ) ;
      if ( NULL == pHostInfo )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "rest field miss:field=%s", 
                     OM_REST_FIELD_HOST_INFO ) ;
         goto error ;
      }

      rc = fromjson( pHostInfo, bsonHostInfo ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                     "rc=%d", pHostInfo, rc ) ;
         goto error ;
      }

      pGlobalUser      = bsonHostInfo.getStringField( OM_BSON_FIELD_HOST_USER ) ;
      pGlobalPasswd    = bsonHostInfo.getStringField( OM_BSON_FIELD_HOST_PASSWD ) ;
      pGlobalSshPort   = bsonHostInfo.getStringField( 
                                       OM_BSON_FIELD_HOST_SSHPORT ) ;
      pGlobalAgentPort = bsonHostInfo.getStringField( 
                                       OM_BSON_FIELD_AGENT_PORT ) ;
      clusterName    = bsonHostInfo.getStringField( 
                                       OM_BSON_FIELD_CLUSTER_NAME ) ;
      if ( 0 == ossStrlen( pGlobalUser ) || 0 == ossStrlen( pGlobalPasswd )
           || 0 == ossStrlen( pGlobalSshPort ) || 0 == clusterName.length()
           || 0 == ossStrlen( pGlobalAgentPort ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "rest field miss:field=[%s or %s or %s or %s "
                     "or %s]", OM_BSON_FIELD_HOST_USER, 
                     OM_BSON_FIELD_HOST_PASSWD, OM_BSON_FIELD_HOST_SSHPORT,
                     OM_BSON_FIELD_AGENT_PORT, OM_BSON_FIELD_CLUSTER_NAME ) ;
         goto error ;
      }

      rc = _getClusterInstallPath( clusterName, installPath ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get cluster's install path failed:rc=%d", rc ) ;
         goto error ;
      }

      element = bsonHostInfo.getField( OM_BSON_FIELD_HOST_INFO ) ;
      {
         BSONObjIterator i( element.embeddedObject() ) ;
         while ( i.more() )
         {
            string hostName ;
            BSONObjBuilder builder ;
            BSONObj tmp ;
            BSONElement ele = i.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "element of %s is not Object type"
                           ":type=%d", OM_BSON_FIELD_HOST_INFO, ele.type() ) ;
               goto error ;
            }
            BSONObj oneHost = ele.embeddedObject() ;
            rc = _checkHostBasicContent( oneHost ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "_checkHostBasicContent failed:rc=%d", rc ) ;
               goto error ;
            }

            builder.appendElements( oneHost ) ;
            if ( !oneHost.hasField( OM_BSON_FIELD_HOST_USER ) )
            {
               builder.append( OM_BSON_FIELD_HOST_USER, pGlobalUser ) ;
            }

            if ( !oneHost.hasField( OM_BSON_FIELD_HOST_PASSWD ) )
            {
               builder.append( OM_BSON_FIELD_HOST_PASSWD, pGlobalPasswd) ;
            }

            if ( !oneHost.hasField( OM_BSON_FIELD_HOST_SSHPORT ) )
            {
               builder.append( OM_BSON_FIELD_HOST_SSHPORT, pGlobalSshPort) ;
            }

            if ( !oneHost.hasField( OM_BSON_FIELD_AGENT_PORT ) )
            {
               builder.append( OM_BSON_FIELD_AGENT_PORT, pGlobalAgentPort) ;
            }

            if ( !oneHost.hasField( OM_BSON_FIELD_INSTALL_PATH ) )
            {
               builder.append( OM_BSON_FIELD_INSTALL_PATH, installPath) ;
            }

            tmp = builder.obj() ;
            hostInfo.push_back( tmp ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostCommand::_getClusterInstallPath( const string &clusterName, 
                                                   string &installPath )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder resultBuilder ;
      BSONObj result ;

      BSONObjBuilder builder ;
      builder.append( OM_CLUSTER_FIELD_NAME, clusterName ) ;
      BSONObj matcher ;
      matcher = builder.obj() ;

      BSONObj selector ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID             = -1 ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CLUSTER, selector, matcher, order, hint, 0, 
                     _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to query table:table=%s,rc=%d", 
                     OM_CS_DEPLOY_CL_CLUSTER, rc ) ;
         goto error ;
      }

      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               PD_LOG_MSG( PDERROR, "cluster is not exist:cluster=%s,rc=%d", 
                           clusterName.c_str(), rc ) ;
               goto error ;
            }

            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_CLUSTER, rc ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         installPath = record.getStringField( OM_CLUSTER_FIELD_INSTALLPATH ) ;
      }

   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostCommand::_getPacketFullPath( char *path )
   {
      INT32 rc = SDB_OK ;
      CHAR tmpPath[ OSS_MAX_PATHSIZE + 1 ] = "" ;
      ossGetEWD( tmpPath, OSS_MAX_PATHSIZE ) ;
      utilCatPath( tmpPath, OSS_MAX_PATHSIZE, ".." ) ;
      utilCatPath( tmpPath, OSS_MAX_PATHSIZE, OM_PACKET_SUBPATH ) ;

      map< string, string> mapFiles ;      
      rc = ossEnumFiles( tmpPath, mapFiles ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "path is invalid:path=%s,rc=%d", tmpPath, rc ) ;
         goto error ;
      }

      if ( mapFiles.size() != 1 )
      {
         rc = SDB_FNE ;
         PD_LOG_MSG( PDERROR, "path is invalid:path=%s,fileCount=%d", tmpPath, 
                     mapFiles.size() ) ;
         goto error ;
      }

      ossSnprintf( path, OSS_MAX_PATHSIZE, "%s", 
                   mapFiles.begin()->second.c_str() ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   void omAddHostCommand::_generateTableField( BSONObjBuilder &builder, 
                                               const string &newFieldName,
                                               BSONObj &bsonOld,
                                               const string &oldFiledName ) 
   {  
      BSONElement element = bsonOld.getField( oldFiledName ) ;
      if ( !element.eoo() )
      {
         builder.appendAs( element, newFieldName ) ;
      }
   }

   BOOLEAN omAddHostCommand::_isHostExistInTask( const string &hostName )
   {
      BSONObjBuilder bsonBuilder ;
      BOOLEAN isExist = TRUE ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;
      rtnContextBuf buffObj ;

      BSONObj hostNameObj = BSON( OM_HOST_FIELD_NAME << hostName ) ;
      BSONObj elemMatch   = BSON( "$elemMatch" << hostNameObj ) ;

      BSONArrayBuilder arrBuilder ;
      arrBuilder.append( OM_TASK_STATUS_FINISH ) ;
      arrBuilder.append( OM_TASK_STATUS_CANCEL ) ;
      BSONObj status = BSON( "$nin" << arrBuilder.arr() ) ;

      matcher = BSON( OM_TASKINFO_FIELD_RESULTINFO << elemMatch
                      << OM_TASKINFO_FIELD_STATUS << status ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_TASKINFO, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to query host:rc=%d,host=%s", rc, 
                 matcher.toString().c_str() ) ;
         goto done ;
      }

      rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "Failed to retreive record, rc = %d", rc ) ;
            goto done ;
         }

         isExist = FALSE ;
         goto done ;
      }
      {
         BSONObj result( buffObj.data() ) ;
         BSONElement eleID = result.getField( OM_TASKINFO_FIELD_TASKID ) ;
         PD_LOG( PDERROR, "host[%s] is exist in task["OSS_LL_PRINT_FORMAT"]",
                 hostName.c_str(), eleID.numberLong() ) ;
      }

      _pRTNCB->contextDelete( contextID, _cb ) ;

   done:
      return isExist;
   }
   
   INT32 omAddHostCommand::_checkTaskExistence( list<BSONObj> &hostInfoList )
   {
      INT32 rc = SDB_OK ;
      list<BSONObj>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         string hostName = iter->getStringField( OM_BSON_FIELD_HOST_NAME ) ;
         if ( _isHostExistInTask( hostName ) )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "host is exist in task:host=%s", 
                        hostName.c_str() ) ;
            goto error ;
         }
         iter++ ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostCommand::_checkHostExistence( list<BSONObj> &hostInfoList )
   {
      INT32 rc = SDB_OK ;
      set<string> hostNameSet ;
      list<BSONObj>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         string host = iter->getStringField( OM_BSON_FIELD_HOST_NAME ) ;
         if ( _isHostNameExist( host ) )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "host is exist:host=%s", host.c_str() ) ;
            goto error ;
         }
         iter++ ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostCommand::_generateTaskInfo( const string &clusterName, 
                                              list<BSONObj> &hostInfoList, 
                                              BSONObj &taskInfo,
                                              BSONArray &resultInfo )
   {
      INT32 rc = SDB_OK ;
      BSONArrayBuilder resultArrBuilder ;
      BSONArrayBuilder arrayBuilder ;
      list<BSONObj>::iterator iter ;
      string sdbUser ;
      string sdbPasswd ;
      string sdbUserGroup ;
      CHAR packetPath[ OSS_MAX_PATHSIZE ] = "" ;
      rc = _getPacketFullPath( packetPath ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get packet path failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = _getSdbUsrInfo( clusterName, sdbUser, sdbPasswd, sdbUserGroup ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getSdbUsrInfo failed:rc=%d", rc ) ;
         goto error ;
      }

      iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         BSONObjBuilder innerBuilder ;
         BSONObj tmp ;

         _generateTableField( innerBuilder, OM_HOST_FIELD_NAME, *iter, 
                              OM_BSON_FIELD_HOST_NAME ) ;
         innerBuilder.append( OM_HOST_FIELD_CLUSTERNAME, clusterName ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_IP, *iter, 
                              OM_BSON_FIELD_HOST_IP ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_USER, *iter, 
                              OM_BSON_FIELD_HOST_USER ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_PASSWORD, *iter, 
                              OM_BSON_FIELD_HOST_PASSWD ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_OS, *iter, 
                              OM_BSON_FIELD_OS ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_OMA, *iter, 
                              OM_BSON_FIELD_OMA ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_MEMORY, *iter, 
                              OM_BSON_FIELD_MEMORY ) ;   
         _generateTableField( innerBuilder, OM_HOST_FIELD_DISK, *iter, 
                              OM_BSON_FIELD_DISK ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_CPU, *iter, 
                              OM_BSON_FIELD_CPU ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_NET, *iter, 
                              OM_BSON_FIELD_NET ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_PORT, *iter, 
                              OM_BSON_FIELD_PORT ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_SERVICE, *iter, 
                              OM_BSON_FIELD_SERVICE ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_SAFETY, *iter, 
                              OM_BSON_FIELD_SAFETY ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_INSTALLPATH, *iter, 
                              OM_BSON_FIELD_INSTALL_PATH ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_AGENT_PORT, *iter, 
                              OM_BSON_FIELD_AGENT_PORT ) ;
         _generateTableField( innerBuilder, OM_HOST_FIELD_SSHPORT, *iter, 
                              OM_BSON_FIELD_HOST_SSHPORT ) ;

         tmp = innerBuilder.obj() ;
         arrayBuilder.append( tmp ) ;

         BSONObjBuilder itemBuilder ;
         itemBuilder.append( OM_HOST_FIELD_IP, 
                             iter->getStringField( OM_HOST_FIELD_IP ) ) ;
         itemBuilder.append( OM_HOST_FIELD_NAME, 
                             iter->getStringField( OM_HOST_FIELD_NAME ) ) ;
         itemBuilder.append( OM_TASKINFO_FIELD_STATUS, OM_TASK_STATUS_INIT ) ;
         itemBuilder.append( OM_TASKINFO_FIELD_STATUS_DESC, 
                             getTaskStatusStr( OM_TASK_STATUS_INIT ) ) ;
         itemBuilder.append( OM_REST_RES_RETCODE, SDB_OK ) ;
         itemBuilder.append( OM_REST_RES_DETAIL, "" ) ;
         {
            BSONArrayBuilder tmpEmptyBuilder ;
            itemBuilder.append( OM_TASKINFO_FIELD_FLOW, 
                                tmpEmptyBuilder.arr() ) ;
         }
         

         BSONObj resultItem = itemBuilder.obj() ;
         resultArrBuilder.append( resultItem ) ;

         iter++ ;
      }

      taskInfo = BSON( OM_BSON_FIELD_CLUSTER_NAME << clusterName 
                       << OM_BSON_FIELD_SDB_USER << sdbUser 
                       << OM_BSON_FIELD_SDB_PASSWD << sdbPasswd
                       << OM_BSON_FIELD_SDB_USERGROUP << sdbUserGroup
                       << OM_BSON_FIELD_PATCKET_PATH << packetPath
                       << OM_BSON_FIELD_HOST_INFO << arrayBuilder.arr() ) ;

      resultInfo = resultArrBuilder.arr() ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT64 omAddHostCommand::_generateTaskID()
   {
      INT64 taskID = 0 ;
      getMaxTaskID( taskID ) ;

      return ++taskID ;
   }

   INT32 omAddHostCommand::_saveTask( INT64 taskID, const BSONObj &taskInfo, 
                                      const BSONArray &resultInfo )
   {
      INT32 rc = SDB_OK ;

      rc = createTask( OM_TASK_TYPE_ADD_HOST, taskID, 
                       getTaskTypeStr( OM_TASK_TYPE_ADD_HOST ), 
                       _localAgentHost, _localAgentService, taskInfo, 
                       resultInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "create task failed:rc=%d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omAddHostCommand::_removeTask( INT64 taskID )
   {
      return removeTask( taskID ) ;
   }

   INT32 omAddHostCommand::doCommand()
   {
      INT64 taskID ;
      list<BSONObj> hostInfoList ;
      string clusterName ;
      INT32 rc = SDB_OK ;
      BSONObjBuilder bsonBuilder ;

      rc = _getRestHostList( clusterName, hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "fail to get host list:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _checkHostExistence( hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "fail to _checkHostExistence:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _checkTaskExistence( hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "fail to _checkTaskExistence:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      {
         BSONObj taskInfo ;
         BSONArray resultInfo ;
         rc = _generateTaskInfo( clusterName, hostInfoList, taskInfo, 
                                 resultInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "generate task info failed:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
            goto error ;
         }

         taskID = _generateTaskID() ;

         rc = _saveTask( taskID, taskInfo, resultInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "fail to save task:taskID="OSS_LL_PRINT_FORMAT
                    ",rc=%d", taskID, rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
            goto error ;
         }


      }

      rc = _notifyAgentTask( taskID ) ;
      if ( SDB_OK != rc )
      {
         _removeTask( taskID ) ;
         PD_LOG( PDERROR, "fail to notify task:taskID="OSS_LL_PRINT_FORMAT
                 ",rc=%d", taskID, rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      {
         BSONObj result = BSON( OM_BSON_TASKID << (long long)taskID ) ;
         _restAdaptor->appendHttpBody( _restSession, result.objdata(), 
                                       result.objsize(), 1 ) ;
         _sendOKRes2Web() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   omListHostCommand::omListHostCommand( restAdaptor *pRestAdaptor, 
                                         pmdRestSession *pRestSession )
                     :omCreateClusterCommand( pRestAdaptor, pRestSession )
   {
   }

   omListHostCommand::~omListHostCommand()
   {
   }

   void omListHostCommand::_sendHostInfo2Web( list<BSONObj> &hosts )
   {
      BSONObj filter = BSON( OM_HOST_FIELD_PASSWORD << "" ) ;
      list<BSONObj>::iterator iter = hosts.begin() ;
      while ( iter != hosts.end() )
      {
         BSONObj tmp = iter->filterFieldsUndotted( filter, false ) ;
         _restAdaptor->appendHttpBody( _restSession, tmp.objdata(), 
                                       tmp.objsize(), 1 ) ;
         iter++ ;
      }

      _sendOKRes2Web() ;

      return ;
   }

   INT32 omListHostCommand::doCommand()
   {
      INT32 rc                  = SDB_OK ;
      const CHAR *pClusterName  = NULL ;
      const CHAR *pBusiness     = NULL ;
      list<BSONObj> records ;

      _restAdaptor->getQuery( _restSession, OM_REST_CLUSTER_NAME, 
                              &pClusterName ) ;
      _restAdaptor->getQuery( _restSession, OM_REST_BUSINESS_NAME, 
                              &pBusiness ) ;
      if( NULL != pClusterName )
      {
         BSONObj selector = BSON( OM_HOST_FIELD_NAME << 1 
                                  << OM_HOST_FIELD_IP << 1) ;
         BSONObj matcher  = BSON( OM_HOST_FIELD_CLUSTERNAME << pClusterName ) ;
         BSONObj order ;
         BSONObj hint ;
         rc = _queryTable( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 
                           0, 0, -1, records ) ;
      }
      else if ( NULL != pBusiness )
      {
         list<BSONObj> tmpRecords ;
         BSONObj selector = BSON( OM_CONFIGURE_FIELD_HOSTNAME << 1 ) ;
         BSONObj matcher  = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME 
                                  << pBusiness ) ;
         BSONObj order ;
         BSONObj hint ;
         rc = _queryTable( OM_CS_DEPLOY_CL_CONFIGURE, selector, matcher, order, 
                           hint, 0, 0, -1, tmpRecords ) ;
         if ( SDB_OK == rc && tmpRecords.size() > 0 )
         {
            selector = BSON( OM_HOST_FIELD_NAME << 1 << OM_HOST_FIELD_IP << 1) ;
            BSONArrayBuilder arrBuilder ;
            list<BSONObj>::iterator iter = tmpRecords.begin() ;
            while ( iter != tmpRecords.end() )
            {
               arrBuilder.append( *iter ) ;
               iter++ ;
            }

            matcher  = BSON( "$or" << arrBuilder.arr() ) ;
            rc = _queryTable( OM_CS_DEPLOY_CL_HOST, selector, matcher, 
                              order, hint, 0, 0, -1, records ) ;
         }
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "rest field miss:%s or %s", OM_REST_CLUSTER_NAME, 
                     OM_REST_BUSINESS_NAME ) ;
      }

      if ( SDB_OK != rc )
      {
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendHostInfo2Web( records ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   omQueryHostCommand::omQueryHostCommand( restAdaptor *pRestAdaptor, 
                                           pmdRestSession *pRestSession )
                      :omListHostCommand( pRestAdaptor, pRestSession )
   {
   }

   omQueryHostCommand::~omQueryHostCommand()
   {  
   }

   INT32 omQueryHostCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      list<BSONObj> hosts ;

      rc = _getQueryPara( selector, matcher, order, hint ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _queryTable( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 
                        0, 0, -1, hosts ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendHostInfo2Web( hosts ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omListBusinessTypeCommand::omListBusinessTypeCommand( 
                                                restAdaptor *pRestAdaptor, 
                                                pmdRestSession *pRestSession, 
                                                const CHAR *pRootPath, 
                                                const CHAR *pSubPath )
                             :omCreateClusterCommand( pRestAdaptor, 
                                                      pRestSession ),
                              _rootPath( pRootPath ), _subPath( pSubPath )
   {
   }

   omListBusinessTypeCommand::~omListBusinessTypeCommand()
   {
   }

   BOOLEAN omListBusinessTypeCommand::_isArray( ptree &pt )
   {
      BOOLEAN isArr = FALSE ;
      string type ;
      try
      {
         type = pt.get<string>( OM_XMLATTR_TYPE ) ;
      }
      catch( std::exception &e )
      {
         isArr = FALSE ;
         goto done ;
      }

      if ( ossStrcasecmp( type.c_str(), OM_XMLATTR_TYPE_ARRAY ) == 0 )
      {
         isArr = TRUE ;
         goto done ;
      }

   done:
      return isArr ;
   }

   BOOLEAN omListBusinessTypeCommand::_isStringValue( ptree &pt )
   {
      BOOLEAN isStringV = FALSE ;
      if ( _isArray( pt ) )
      {
         isStringV = FALSE ;
         goto done ;
      }

      if ( pt.size() == 0 )
      {
         isStringV = TRUE ;
         goto done ;
      }

      if ( pt.size() > 1 )
      {
         isStringV = FALSE ;
         goto done ;
      }

      {
         ptree::iterator ite = pt.begin() ;
         string key          = ite->first ;
         if ( ossStrcasecmp( key.c_str(), OM_XMLATTR_KEY ) == 0 )
         {
            isStringV = TRUE ;
            goto done ;
         }
      }

   done:
      return isStringV ;
   }

   void omListBusinessTypeCommand::_parseArray( ptree &pt, 
                                            BSONArrayBuilder &arrayBuilder )
   {
      ptree::iterator ite = pt.begin() ;
      for( ; ite != pt.end() ; ite++ )
      {
         string key    = ite->first ;
         if ( ossStrcasecmp( key.c_str(), OM_XMLATTR_KEY ) == 0 )
         {
            continue ;
         }

         BSONObj obj ;
         ptree child = ite->second ;
         _recurseParseObj( child, obj ) ;
         arrayBuilder.append( obj ) ;
      }
   }

   void omListBusinessTypeCommand::_recurseParseObj( ptree &pt, BSONObj &out )
   {
      BSONObjBuilder builder ;
      ptree::iterator ite = pt.begin() ;
      for( ; ite != pt.end() ; ite++ )
      {
         string key = ite->first ;
         if ( ossStrcasecmp( key.c_str(), OM_XMLATTR_KEY ) == 0 )
         {
            continue ;
         }

         ptree child = ite->second ;
         if ( _isArray( child ) )
         {
            BSONArrayBuilder arrayBuilder ;
            _parseArray( child, arrayBuilder ) ;
            builder.append( key, arrayBuilder.arr() ) ;
         }
         else if ( _isStringValue( child ) )
         {
            string value = ite->second.data() ;
            builder.append( key, value ) ;
         }
         else
         {
            BSONObj obj ;
            _recurseParseObj( child, obj ) ;
            builder.append(key, obj ) ;
         }
      }

      out = builder.obj() ;
   }

   INT32 omListBusinessTypeCommand::_readConfigFile( const string &file, 
                                                     BSONObj &obj )
   {
      INT32 rc = SDB_OK ;
      try
      {
         ptree pt ;
         read_xml( file.c_str(), pt ) ;
         _recurseParseObj( pt, obj ) ;
      }
      catch( std::exception &e )
      {
         rc = SDB_INVALIDPATH ;
         PD_LOG_MSG( PDERROR, "%s", e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omListBusinessTypeCommand::_getBusinessList( 
                                               list<BSONObj> &businessList )
   {
      INT32 rc = SDB_OK ;
      string businessFile ;
      BSONObj fileContent ;

      businessFile = _rootPath + OSS_FILE_SEP + OM_BUSINESS_CONFIG_SUBDIR
                     + OSS_FILE_SEP + OM_BUSINESS_FILE_NAME
                     + _languageFileSep + OM_CONFIG_FILE_TYPE ;

      rc = _readConfigFile( businessFile, fileContent ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "read business file failed:file=%s", 
                 businessFile.c_str() ) ;
         goto error ;
      }

      {
         BSONObj businessArray = fileContent.getObjectField(
                                                       OM_BSON_BUSINESS_LIST ) ;
         BSONObjIterator iter( businessArray ) ;
         while( iter.more() )
         {
            BSONElement ele      = iter.next() ;
            if ( Object != ele.type() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "field is not Object:field=%s,type=%d",
                           OM_BSON_BUSINESS_LIST, ele.type() ) ;
               goto error ;
            }

            BSONObj oneBusiness = ele.embeddedObject() ;
            businessList.push_back( oneBusiness.copy() ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omListBusinessTypeCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      list<BSONObj> businessList ;

      _setFileLanguageSep() ;

      rc = _getBusinessList( businessList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getBusinessList failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      {
         list<BSONObj>::iterator iter = businessList.begin() ;
         while ( iter != businessList.end() )
         {

            _restAdaptor->appendHttpBody( _restSession, iter->objdata(), 
                                          iter->objsize(), 1 ) ;
            iter++ ;
         }
      }
      _sendOKRes2Web() ;
   done:
      return rc ;
   error:
      goto done ;
   }

   omGetBusinessTemplateCommand::omGetBusinessTemplateCommand( 
                                                   restAdaptor *pRestAdaptor, 
                                                   pmdRestSession *pRestSession, 
                                                   const CHAR *pRootPath, 
                                                   const CHAR *pSubPath )
                                :omListBusinessTypeCommand( pRestAdaptor, 
                                                            pRestSession, 
                                                            pRootPath,
                                                            pSubPath )
   {
   }

   omGetBusinessTemplateCommand::~omGetBusinessTemplateCommand()
   {
   }

   INT32 omGetBusinessTemplateCommand::_readConfTemplate( 
                                                const string &businessType, 
                                                const string &file, 
                                                list<BSONObj> &deployModList ) 
   {
      INT32 rc = SDB_OK ;
      BSONObj deployModArray ;
      BSONObj deployMods ;
      rc = _readConfigFile( file, deployModArray ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "read file failed:file=%s", file.c_str() ) ;
         goto error ;
      }

      deployMods = deployModArray.getObjectField( OM_BSON_DEPLOY_MOD_LIST ) ;
      {
         BSONObjIterator iter( deployMods ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "field's element is not Object:field=%s"
                           ",type=%d", OM_BSON_DEPLOY_MOD_LIST, ele.type() ) ;
               goto error ;
            }
            BSONObj oneDeployMod = ele.embeddedObject() ;
            deployModList.push_back( oneDeployMod.copy()) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omGetBusinessTemplateCommand::_readConfDetail( const string &file, 
                                                        BSONObj &bsonConfDetail )
   {
      INT32 rc = SDB_OK ;
      rc = _readConfigFile( file, bsonConfDetail ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "read file failed:file=%s", file.c_str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omGetBusinessTemplateCommand::doCommand()
   {
      INT32 rc                  = SDB_OK ;
      const CHAR* pBusinessType = NULL ;
      string templateFile       = "" ;
      list<BSONObj> deployModList ;
      list<BSONObj>::iterator iter ;

      _restAdaptor->getQuery(_restSession, OM_REST_BUSINESS_TYPE, 
                             &pBusinessType ) ;
      if ( NULL == pBusinessType )
      {
         _errorDetail = string( "rest field:" ) + OM_REST_BUSINESS_TYPE
                        + " is null" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _setFileLanguageSep() ;
      templateFile = _rootPath + OSS_FILE_SEP + OM_BUSINESS_CONFIG_SUBDIR 
                     + OSS_FILE_SEP + pBusinessType + OM_TEMPLATE_FILE_NAME
                     + _languageFileSep + OM_CONFIG_FILE_TYPE ;
      rc = _readConfTemplate( pBusinessType, templateFile, deployModList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "read template file failed:file=%s:rc=%d", 
                 templateFile.c_str(), rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      iter = deployModList.begin() ;
      while ( iter != deployModList.end() )
      {
         _restAdaptor->appendHttpBody( _restSession, iter->objdata(),
                                       iter->objsize(), 1 ) ;
         iter++ ;
      }

      _sendOKRes2Web() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omConfigBusinessCommand::omConfigBusinessCommand(  
                                                  restAdaptor *pRestAdaptor, 
                                                  pmdRestSession *pRestSession, 
                                                  const CHAR *pRootPath, 
                                                  const CHAR *pSubPath )
                           :omGetBusinessTemplateCommand( pRestAdaptor, 
                                                          pRestSession, 
                                                          pRootPath,
                                                          pSubPath)
   {
   }

   omConfigBusinessCommand::~omConfigBusinessCommand()
   {
   }

   INT32 omConfigBusinessCommand::_getPropertyNameValue( BSONObj &bsonTemplate, 
                                                         string propertyName, 
                                                         string &value )
   {
      INT32 rc = SDB_OK ;
      BSONObj properties ;
      properties = bsonTemplate.getObjectField( OM_BSON_PROPERTY_ARRAY ) ;
      {
         BSONObjIterator iter( properties ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "template format error, element of %s"
                           " shoule be Object type:type=%d", 
                           OM_BSON_PROPERTY_ARRAY, ele.type() ) ;
               goto error ;
            }
            /*{ 
                 "Name": "replica_num", "Value": "2" 
              }*/
            BSONObj oneProperty = ele.embeddedObject() ;
            string name = oneProperty.getStringField( OM_BSON_PROPERTY_NAME ) ;
            if ( propertyName.compare( name ) == 0 )
            {
               if ( !oneProperty.hasField( OM_BSON_PROPERTY_VALUE ) )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG_MSG( PDERROR, "propery have not value:name=%s", 
                              name.c_str() ) ;
                  goto error ;
               }

               value = oneProperty.getStringField( OM_BSON_PROPERTY_VALUE ) ;
               goto done ;
            }
         }
      }

      rc = SDB_INVALIDARG ;
      PD_LOG_MSG( PDERROR, "propery is not found:name=%s", 
                  propertyName.c_str() ) ;
      goto error ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omConfigBusinessCommand::_fillTemplateInfo( BSONObj &bsonTemplate )
   {
      INT32 rc = SDB_OK ;
      string clusterName ;
      string businessType ;
      string deployMod ;
      string businessName ;
      string file ;
      list<BSONObj> deployModList ;
      list<BSONObj>::iterator iterList ;
      BSONObj oneDeployMod ;
      BSONObjBuilder builder ;
      BSONArrayBuilder arrayBuilder ;
      BSONObj properties ;

      clusterName  = bsonTemplate.getStringField( OM_BSON_FIELD_CLUSTER_NAME ) ;
      businessType = bsonTemplate.getStringField( OM_BSON_BUSINESS_TYPE ) ;
      deployMod    = bsonTemplate.getStringField( OM_BSON_DEPLOY_MOD ) ;
      businessName = bsonTemplate.getStringField( OM_BSON_BUSINESS_NAME ) ;

      file = _rootPath + OSS_FILE_SEP + OM_BUSINESS_CONFIG_SUBDIR 
             + OSS_FILE_SEP + businessType + OM_TEMPLATE_FILE_NAME
             + _languageFileSep + OM_CONFIG_FILE_TYPE ;
      rc = _readConfTemplate( businessType, file, deployModList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "read template file failed:file=%s:rc=%d", 
                 file.c_str(), rc ) ;
         goto error ;
      }

      iterList = deployModList.begin() ;
      while ( iterList != deployModList.end() )
      {
         string tmpClusterType = iterList->getStringField(
                                                          OM_BSON_DEPLOY_MOD ) ;
         if ( tmpClusterType.compare( deployMod ) == 0 )
         {
            oneDeployMod = *iterList ;
            break ;
         }
         iterList++ ;
      }

      if ( iterList == deployModList.end() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is not existed:mode=%s", OM_BSON_DEPLOY_MOD, 
                     deployMod.c_str() ) ;
         goto error ;
      }

      builder.append( OM_BSON_FIELD_CLUSTER_NAME, businessName ) ;
      builder.append( OM_BSON_BUSINESS_TYPE, businessType ) ;
      builder.append( OM_BSON_DEPLOY_MOD, deployMod ) ;
      builder.append( OM_BSON_BUSINESS_NAME, businessName ) ;
      /*{
           "Property": [ { "Name": "replica_num", "Type": "int", "Default": "1", 
                           "Valid": "1", "Display": "edit box", "Edit": "false", 
                           "Desc": "", "WebName": "" 
                         }, ...
                       ]
        }*/
      properties = oneDeployMod.getObjectField( OM_BSON_PROPERTY_ARRAY ) ;
      {
         BSONObjIterator iter( properties ) ;
         while ( iter.more() )
         {
            BSONObjBuilder propertyBuilder ;
            BSONObj tmp ;
            BSONElement ele = iter.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "template file format error,"
                           "elment of %s should be Object type:file=%s,type=%d", 
                           OM_BSON_PROPERTY_ARRAY, file.c_str(), ele.type() ) ;
               goto error ;
            }
            BSONObj oneProperty = ele.embeddedObject() ;
            string propertyName ;
            string value ;
            /*{ 
                 "Name": "replica_num", "Type": "int", "Default": "1", 
                 "Valid": "1", "Display": "edit box", "Edit": "false", 
                 "Desc": "", "WebName": "" 
              }*/
            propertyBuilder.appendElements( oneProperty ) ;
            propertyName = oneProperty.getStringField( OM_BSON_PROPERTY_NAME) ;
            rc = _getPropertyNameValue( bsonTemplate, propertyName, value ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "template miss property:%s", 
                       propertyName.c_str() ) ;
               goto error ;
            }
            propertyBuilder.append( OM_BSON_PROPERTY_VALUE, value ) ;
            tmp = propertyBuilder.obj() ;
            arrayBuilder.append( tmp ) ;
         }
      }

      builder.append( OM_BSON_PROPERTY_ARRAY, arrayBuilder.arr() ) ;
      bsonTemplate = builder.obj() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omConfigBusinessCommand::_getHostConfig( string hostName, 
                                                  string businessName,
                                                  BSONObj &config )
   {
      BSONObjBuilder confBuilder ;
      BSONArrayBuilder arrayBuilder ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID        = -1 ;
      INT32 rc                = SDB_OK ;

      BSONObjBuilder condBuilder ;
      condBuilder.append( OM_CONFIGURE_FIELD_HOSTNAME, hostName ) ;
      BSONObj condition = condBuilder.obj() ;

      rc = rtnQuery( OM_CS_DEPLOY_CL_CONFIGURE, selector, condition, order, 
                     hint, 0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d",
                     OM_CS_DEPLOY_CL_CONFIGURE, rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         BSONObj tmpConf ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_CONFIGURE, rc ) ;
            goto error ;
         }

         /*
            {
               "HostName":"h1", "BusinessName":"b1", 
               "Config":
               [{"dbpath":"","svcname":"11810", ...}
                  , ...
               ]
            }
         */
         BSONObj result( buffObj.data() ) ;
         businessName = result.getStringField( 
                                             OM_CONFIGURE_FIELD_BUSINESSNAME ) ;
         tmpConf = result.getObjectField( OM_CONFIGURE_FIELD_CONFIG ) ;
         {
            BSONObjIterator iter( tmpConf ) ;
            while ( iter.more() )
            {  
               BSONObjBuilder innerBuilder ;
               BSONElement ele     = iter.next() ;

               innerBuilder.appendElements( ele.embeddedObject() ) ;
               innerBuilder.append( OM_BSON_BUSINESS_NAME, businessName ) ;
               arrayBuilder.append( innerBuilder.obj() ) ;
            }
         }
      }

      /*
         {
            "Config":
            [{"BusinessName":"b1","dbpath":"","svcname":"11810", ...}
               , ...
            ]
         }
      */
      confBuilder.append( OM_BSON_FIELD_CONFIG, arrayBuilder.arr() ) ;
      config = confBuilder.obj() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omConfigBusinessCommand::_checkBusiness( string businessName, 
                                                  const string &businessType,
                                                  const string &deployMod,
                                                  const string &clusterName )
   {
      INT32 rc = SDB_OK ;
      string existType ;
      string existDeployMod ;
      string existClusterName ;

      rc = _getExistBusiness( businessName, existType, existDeployMod, 
                              existClusterName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get exist business failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( existType != "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "business conflict with same name:"
                     "exist=[%s,%s,%s,%s] new=[%s,%s,%s,%s]", 
                     businessName.c_str(), existType.c_str(),
                     existDeployMod.c_str(), existClusterName.c_str(), 
                     businessName.c_str(), businessType.c_str(), 
                     deployMod.c_str(), clusterName.c_str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }
   INT32 omConfigBusinessCommand::_getExistBusiness( const string &businessName, 
                                                     string &businessType,
                                                     string &deployMod,
                                                     string &clusterName )
   {
      BSONObj selector ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID  = -1 ;
      INT32 rc          = SDB_OK ;
      BSONObj condition = BSON( OM_BUSINESS_FIELD_NAME << businessName )  ;

      businessType = "" ;
      deployMod    = "" ;
      clusterName  = "" ;

      rc = rtnQuery( OM_CS_DEPLOY_CL_BUSINESS, selector, condition, order, 
                     hint, 0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_BUSINESS, rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         BSONObj tmpConf ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_BUSINESS, rc ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         businessType = result.getStringField( OM_BUSINESS_FIELD_TYPE );
         deployMod    = result.getStringField( OM_BUSINESS_FIELD_DEPLOYMOD );
         clusterName  = result.getStringField( OM_BUSINESS_FIELD_CLUSTERNAME );
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*input parameter: 
     bsonHostInfo
     {
        "HostInfo":[{"HostName":"host1"}, ...]
     }

     output parameter:
     bsonHostInfo
     {
        "HostInfo":[{"HostName":"host1", "ClusterName":"c1", 
                     "Disk":[{"Name":"dev", "Mount":"/mnt", ... }, ...],
                     "Config":[{"dbpath":"","svcname":"11810", ...}, ...]
                    , ...
                   ]
     }

     */
   INT32 omConfigBusinessCommand::_fillHostInfo( string clusterName, 
                                                 string businessName,
                                                 BSONObj &bsonHostInfo )
   {
      BSONObj condition ;
      BSONObj result ;
      BSONArrayBuilder ArrBuilder ;
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj hint ;
      BOOLEAN isRecordFetched = false ;
      SINT64 contextID        = -1 ;
      INT32 rc                = SDB_OK ;

      BSONObjBuilder condBuilder ;
      condBuilder.append( OM_HOST_FIELD_CLUSTERNAME, clusterName ) ;
      if ( !bsonHostInfo.isEmpty() )
      {
         BSONArrayBuilder innArrBuilder ;
         BSONObj hosts ;
         hosts = bsonHostInfo.getObjectField( OM_BSON_FIELD_HOST_INFO ) ;
         {
            BSONObjIterator iter( hosts ) ;
            while ( iter.more() )
            {
               string hostName ;
               BSONObjBuilder builder ;
               BSONObj oneHost ;
               BSONObj tmp ;
               BSONElement ele = iter.next() ;
               if ( ele.type() != Object )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG_MSG( PDERROR, "element of %s should be Object type"
                              ":type=%d", OM_BSON_FIELD_HOST_INFO, 
                              ele.type() ) ;
                  goto error ;
               }

               oneHost  = ele.embeddedObject() ;
               hostName = oneHost.getStringField( OM_BSON_FIELD_HOST_NAME ) ;
               builder.append( OM_HOST_FIELD_NAME, hostName ) ;
               tmp      = builder.obj() ;
               innArrBuilder.append( tmp ) ;
             }
          }

          condBuilder.append( "$or", innArrBuilder.arr() ) ;
      }

      condition = condBuilder.obj() ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, condition, order, hint, 0, 
                     _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d", 
                     OM_CS_DEPLOY_CL_HOST, rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         BSONObjBuilder resultBuilder ;
         BSONObj config ;
         string hostName ;
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_HOST, rc ) ;
            goto error ;
         }

         /*
            result= {"HostName":"host1", "ClusterName":"c1", 
                     "Disk":[{"Name":"dev", "Mount":"/mnt", ... }, ...]
                     , ...  }
         */
         BSONObj result( buffObj.data() ) ;
         resultBuilder.appendElements( result ) ;
         hostName = result.getStringField( OM_HOST_FIELD_NAME ) ;
         rc       = _getHostConfig( hostName, businessName, config ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "Failed to get host config, rc=%d", rc ) ;
            goto error ;
         }

         resultBuilder.appendElements( config ) ;

         /*
            result= {"HostName":"host1", "ClusterName":"c1", 
                     "Disk":[{"Name":"dev", "Mount":"/mnt", ... }, ...],
                     "Config":[{"BusinessName":"b1","dbpath":"",
                                "svcname":"11810", ... }, ...]
                     , ...  }
         */
         result = resultBuilder.obj() ;
         ArrBuilder.append( result ) ;
         isRecordFetched = TRUE ;
      }

      if ( !isRecordFetched )
      {
         rc = SDB_DMS_RECORD_NOTEXIST ;
         PD_LOG_MSG( PDERROR, "host is not exist:host=%s", 
                     condition.toString().c_str() ) ;
         goto error ;
      }

      bsonBuilder.append( OM_BSON_FIELD_HOST_INFO, ArrBuilder.arr() ) ;
      bsonHostInfo = bsonBuilder.obj() ;

   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   /*
     bsonTemplate(out)
       {
           "ClusterName":"c1","BusinessType":"sequoiadb", "BusinessName":"b1",
           "DeployMod": "standalone", 
           "Property": [ { "Name": "replica_num", "Type": "int", "Default": "1", 
                           "Valid": "1", "Display": "edit box", "Edit": "false", 
                           "Desc": "", "WebName": "" }
                           , ...
                       ] 
        }

     bsonHostInfo(out):    
        {
           "HostInfo":[{"HostName":"host1", "ClusterName":"c1", 
                        "Disk":[{"Name":"dev", "Mount":"/mnt", ... }, ...],
                        "Config":[{"BusinessName":"b1","dbpath":"","svcname":"11810", ...}, ...]
                       }
                       , ...
                      ]
        }*/
   INT32 omConfigBusinessCommand::_getTemplateInfo( BSONObj &bsonTemplate, 
                                                    BSONObj &bsonHostInfo )
   {
      INT32 rc          = SDB_OK ;
      const CHAR *pInfo = NULL ;
      BSONObj bsonInfo ;
      BSONObjBuilder templateBuilder ;
      BSONObj filter ;
      BSONObjBuilder hostInfoBuilder ;
      _restAdaptor->getQuery( _restSession, OM_REST_TEMPLATE_INFO, 
                              &pInfo ) ;
      if ( NULL == pInfo )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "rest field is null:field=%s", 
                     OM_REST_TEMPLATE_INFO ) ;
         goto error ;
      }

      rc = fromjson( pInfo, bsonInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                     "rc=%d", pInfo, rc ) ;
         goto error ;
      }

      filter = BSON( OM_BSON_FIELD_HOST_INFO << "" ) ;
      /*{
           "ClusterName":"c1","BusinessType":"sequoiadb", "BusinessName":"b1",
           "ClusterType": "standalone", 
           "Property": [ { "Name": "replica_num", "Value":"" }, ...] 
        } */
      bsonTemplate = bsonInfo.filterFieldsUndotted( filter, false ) ;

      _clusterName = bsonTemplate.getStringField( OM_BSON_FIELD_CLUSTER_NAME ) ;
      if ( _clusterName == "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is empty:rc=%d", OM_BSON_FIELD_CLUSTER_NAME, 
                     rc ) ;
         goto error ;
      }

      _businessType = bsonTemplate.getStringField( OM_BSON_BUSINESS_TYPE ) ;
      if ( _businessType == "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is empty:rc=%d", OM_BSON_BUSINESS_TYPE , 
                     rc ) ;
         goto error ;
      }

      _businessName = bsonTemplate.getStringField( OM_BSON_BUSINESS_NAME ) ;
      if ( _businessName == "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is empty:rc=%d", OM_BSON_BUSINESS_NAME, 
                     rc ) ;
         goto error ;
      }

      _deployMod = bsonTemplate.getStringField( OM_BSON_DEPLOY_MOD ) ;
      if ( _deployMod == "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is empty:rc=%d", OM_BSON_DEPLOY_MOD, rc ) ;
         goto error ;
      }

      rc = _fillTemplateInfo( bsonTemplate ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_fillTemplateInfo failed:rc=%d", rc ) ;
         goto error ;
      }
      /*{
           "ClusterName":"c1","BusinessType":"sequoiadb", "BusinessName":"b1",
           "ClusterType": "standalone", 
           "Property": [ { "Name": "replica_num", "Type": "int", "Default": "1", 
                           "Valid": "1", "Display": "edit box", "Edit": "false", 
                           "Desc": "", "WebName": "" }
                           , ...
                       ] 
        } */

      /*{
           "HostInfo":[{"HostName":"host1"}, ...]}
        }*/
      bsonHostInfo = bsonInfo.filterFieldsUndotted( filter, true ) ;
      /*{
           "HostInfo":[{"HostName":"host1", "ClusterName":"c1", 
                        "Disk":[{"Name":"dev", "Mount":"/mnt", ... }, ...],
                        "Config":[{"BusinessName":"b1","dbpath":"","svcname":"11810", ...}, ...]
                       }
                       , ...
                      ]
        }*/
      rc = _fillHostInfo( _clusterName, _businessName, bsonHostInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_fillHostInfo failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
   bsonConfDetail:
   {
      "Property":[{"Name":"dbpath", "Type":"path", "Default":"/opt/sequoiadb", 
                      "Valid":"1", "Display":"edit box", "Edit":"false", 
                      "Desc":"", "WebName":"" }
                      , ...
                 ] 
   }
   */
   INT32 omConfigBusinessCommand::_getConfigDetail( const BSONObj &bsonTemplate, 
                                                  BSONObj &bsonConfDetail )
   {
      INT32 rc = SDB_OK ;
      string confDetailFile = "" ;
      string businessType ;

      businessType = bsonTemplate.getStringField( OM_REST_BUSINESS_TYPE ) ;
      if ( businessType.length() == 0 )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is null", OM_REST_BUSINESS_TYPE ) ;
         goto error ;
      }
      confDetailFile = _rootPath + OSS_FILE_SEP + OM_BUSINESS_CONFIG_SUBDIR 
                       + OSS_FILE_SEP + businessType + OM_CONFIG_ITEM_FILE_NAME
                       + _languageFileSep + OM_CONFIG_FILE_TYPE ;

      rc = _readConfDetail(confDetailFile, bsonConfDetail ) ;
      if( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "read configure failed:file=%s", 
                 confDetailFile.c_str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omConfigBusinessCommand::_addProperties( BSONObjBuilder &builder, 
                                                 const BSONObj &bsonTemplate, 
                                                 const BSONObj &bsonConfDetail )
   {
      BSONObjBuilder valueCondBuilder ;
      BSONObj valueCondition ;
      valueCondBuilder.append( OM_BSON_PROPERTY_VALUE, "" ) ;
      valueCondition = valueCondBuilder.obj() ;

      BSONArrayBuilder arrayBuilder ;
      BSONObj tmp ;
      tmp = bsonConfDetail.getObjectField( OM_BSON_PROPERTY_ARRAY ) ;
      BSONObjIterator iter2( tmp ) ;
      while ( iter2.more() )
      {
         BSONElement ele ;
         BSONObj oneProperty ;
         string propertyName ;

         ele          = iter2.next() ;
         oneProperty  = ele.embeddedObject() ;
         arrayBuilder.append( oneProperty ) ;
      }

      builder.append( OM_BSON_PROPERTY_ARRAY, arrayBuilder.arr() ) ;
   }

   INT32 omConfigBusinessCommand::_generateConfig( 
                                                  const BSONObj &bsonTemplate, 
                                                  const BSONObj &bsonHostInfo, 
                                                  const BSONObj &bsonConfigItem, 
                                                  BSONObj &bsonConfig )
   {
      INT32 rc = SDB_OK ;
      omConfigGenerator confGenerator ;
      BSONObjBuilder builder ;
      BSONObj tmpConf ;
      /* tmpConf:
         {
            "Config":
            [
              {"HostName":"host1","DataGroupName":"",
               "DBPath": "/home/db2/coord/11830", "SvcName": "11830", ... 
              }, ...
            ]
         }
      */
      rc = confGenerator.generateSDBConfig( bsonTemplate, bsonConfigItem, 
                                            bsonHostInfo, tmpConf ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "%s", confGenerator.getErrorDetail().c_str() ) ;
         goto error ;
      }

      builder.appendElements( tmpConf ) ;
      builder.append( OM_BSON_FIELD_CLUSTER_NAME, _clusterName ) ;
      _addProperties( builder, bsonTemplate, bsonConfigItem ) ;
      bsonConfig = builder.obj() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omConfigBusinessCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      BSONObj bsonTemplate ;
      BSONObj bsonHostInfo ;
      BSONObj bsonConfigDetail ;
      BSONObj bsonConfig ;
      BSONObjBuilder opBuilder ;

      _setFileLanguageSep() ;

      rc = _getTemplateInfo( bsonTemplate, bsonHostInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get config info failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _checkBusiness( _businessName, _businessType, _deployMod, 
                           _clusterName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_checkBusiness failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _getConfigDetail( bsonTemplate, bsonConfigDetail ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get config item failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _generateConfig( bsonTemplate, bsonHostInfo, bsonConfigDetail, 
                            bsonConfig ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "generate config failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _restAdaptor->appendHttpBody( _restSession, bsonConfig.objdata(),
                                    bsonConfig.objsize(), 1 ) ;

      _sendOKRes2Web() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omInstallBusinessReq::omInstallBusinessReq( restAdaptor *pRestAdaptor, 
                                               pmdRestSession *pRestSession, 
                                               const CHAR *pRootPath, 
                                               const CHAR *pSubPath,
                                               string localAgentHost, 
                                               string localAgentService)
                        :omConfigBusinessCommand( pRestAdaptor, 
                                                  pRestSession, pRootPath,
                                                  pSubPath ), 
                         _localAgentHost( localAgentHost ),
                         _localAgentService( localAgentService )
   {
   }

   omInstallBusinessReq::~omInstallBusinessReq()
   {
   }

   /*
   bsonConfValue:
   {
      "BusinessType":"sequoiadb", "BusinessName":"b1", "DeployMod":"xxx", 
      "ClusterName":"c1", 
      "Config":
      [
         {"HostName": "host1", "datagroupname": "", 
          "dbpath": "/home/db2/standalone/11830", "svcname": "11830", ...}
         ,...
      ]
   }
   bsonHostInfo:
   {
     "HostInfo":[
                   {
                      "HostName":"host1", "ClusterName":"c1", 
                      "Disk":{"Name":"/dev/sdb", Size:"", Mount:"", Used:""},
                      "Config":[{"BusinessName":"b2","dbpath":"", svcname:"", 
                                 "role":"", ... }, ...]
                   }
                    , ... 
                ]
   }
   */
   INT32 omInstallBusinessReq::_extractHostInfo( BSONObj &bsonConfValue, 
                                                 BSONObj &bsonHostInfo )
   {
      INT32 rc = SDB_OK ;
      BSONObj condition ;
      BSONObjBuilder builder ;
      BSONArrayBuilder arrayBuilder ;
      BSONObj config ;
      set<string> tmpHost ;
      set<string>::iterator iterSet ;

      condition = BSON( OM_BSON_FIELD_HOST_NAME << "" ) ;
      config    = bsonConfValue.getObjectField( OM_BSON_FIELD_CONFIG ) ;
      {
         BSONObjIterator iter( config ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "field's element is not Object:field=%s"
                           ",type=%d", OM_BSON_FIELD_CONFIG, ele.type() ) ;
               goto error ;
            }
            BSONObj oneNode = ele.embeddedObject() ;
            BSONObj host    = oneNode.filterFieldsUndotted(condition, true );
            tmpHost.insert( host.getStringField( OM_BSON_FIELD_HOST_NAME ) ) ;
         }
      }

      iterSet = tmpHost.begin() ;
      while( iterSet != tmpHost.end() )
      {
         BSONObj host = BSON( OM_BSON_FIELD_HOST_NAME << *iterSet ) ;
         arrayBuilder.append( host ) ;
         iterSet++ ;
      }

      builder.append( OM_BSON_FIELD_HOST_INFO, arrayBuilder.arr() ) ;
      bsonHostInfo = builder.obj() ;

      rc = _fillHostInfo( _clusterName, _businessName, bsonHostInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_fillHostInfo failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallBusinessReq::_combineConfDetail( string businessType, 
                                                   string deployMod, 
                                                   BSONObj &bsonAllConf )
   {
      INT32 rc = SDB_OK ;
      string templateFile ;
      list <BSONObj> deployModList ;
      list <BSONObj>::iterator iterList ;
      BSONObj bsonDeployMod ;
      BSONObj bsonDetail ;
      string confDetailFile ;

      templateFile = _rootPath + OSS_FILE_SEP + OM_BUSINESS_CONFIG_SUBDIR 
                     + OSS_FILE_SEP + businessType + OM_TEMPLATE_FILE_NAME
                     + _languageFileSep + OM_CONFIG_FILE_TYPE ;
      rc = _readConfTemplate( businessType, templateFile, deployModList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "read template file failed:file=%s,rc=%d", 
                 templateFile.c_str(), rc ) ;
         goto error ;
      }

      iterList = deployModList.begin() ;
      while ( iterList != deployModList.end() )
      {
         string tmpDeployMod = iterList->getStringField( OM_BSON_DEPLOY_MOD ) ;
         if ( deployMod == tmpDeployMod )
         {
            bsonDeployMod = *iterList ;
            break ;
         }
         iterList++ ;
      }

      if ( iterList == deployModList.end() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is not exsit:", OM_BSON_DEPLOY_MOD, 
                     deployMod.c_str() ) ;
         goto error ;
      }

      confDetailFile = _rootPath + OSS_FILE_SEP + OM_BUSINESS_CONFIG_SUBDIR 
                       + OSS_FILE_SEP + businessType 
                       + OM_CONFIG_ITEM_FILE_NAME + _languageFileSep 
                       + OM_CONFIG_FILE_TYPE ;
      rc = _readConfDetail( confDetailFile, bsonDetail ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "read config file failed:file=%s,rc=%d", 
                 confDetailFile.c_str(), rc ) ;
         goto error ;
      }

      {
         BSONArrayBuilder arrayBuilder ;
         BSONObj properties ;
         properties = bsonDeployMod.getObjectField( OM_BSON_PROPERTY_ARRAY ) ;
         BSONObjIterator iter1( properties ) ;
         while ( iter1.more() )
         {
            BSONElement ele  = iter1.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "config info is invalid, element of %s "
                           "is not Object type:type=%d", 
                           OM_BSON_PROPERTY_ARRAY, ele.type() ) ;
               goto error ;
            }
            BSONObj tmp      = ele.embeddedObject() ;
            arrayBuilder.append( tmp ) ;
         }

         properties = bsonDetail.getObjectField( OM_BSON_PROPERTY_ARRAY ) ;
         BSONObjIterator iter2( properties ) ;
         while ( iter2.more() )
         {
            BSONElement ele  = iter2.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "config info is invalid, element of %s "
                           "is not Object type:type=%d", 
                           OM_BSON_PROPERTY_ARRAY, ele.type() ) ;
               goto error ;
            }
            BSONObj tmp      = ele.embeddedObject() ;
            arrayBuilder.append( tmp ) ;
         }

         BSONObjBuilder builder ;
         builder.append( OM_BSON_PROPERTY_ARRAY, arrayBuilder.arr() ) ;
         bsonAllConf = builder.obj() ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallBusinessReq::_notifyAgentTask( INT64 taskID )
   {
      INT32 rc          = SDB_OK ;
      SINT32 flag       = SDB_OK ;
      CHAR *pContent    = NULL ;
      INT32 contentSize = 0 ;
      MsgHeader *pMsg   = NULL ;
      omManager *om     = NULL ;
      pmdRemoteSession *remoteSession = NULL ;
      BSONObj bsonRequest ;
      BSONObj result ;

      bsonRequest = BSON( OM_TASKINFO_FIELD_TASKID << taskID ) ;
      rc = msgBuildQueryMsg( &pContent, &contentSize, 
                             CMD_ADMIN_PREFIX OM_NOTIFY_TASK, 
                             0, 0, 0, -1, &bsonRequest, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build msg failed:cmd=%s,rc=%d", OM_NOTIFY_TASK,
                     rc ) ;
         goto error ;
      }

      om            = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                      OM_WAIT_SCAN_RES_INTERVAL,
                                                      NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "create remote session failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         goto error ;
      }

      pMsg = (MsgHeader *)pContent ;
      rc   = _sendMsgToLocalAgent( om, remoteSession, pMsg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "send message to agent failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         remoteSession->clearSubSession() ;
         goto error ;
      }

      rc = _receiveFromAgent( remoteSession, flag, result ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "receive from agent failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         string tmpError = result.getStringField( OM_REST_RES_DETAIL ) ;
         PD_LOG_MSG( PDERROR, "agent process failed:detail=(%s),rc=%d", 
                     tmpError.c_str(), rc ) ;
         goto error ;
      }
   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallBusinessReq::_sendMsgToLocalAgent( omManager *om,
                                                pmdRemoteSession *remoteSession, 
                                                MsgHeader *pMsg )
   {
      MsgRouteID localAgentID ;
      INT32 rc = SDB_OK ;

      localAgentID = om->updateAgentInfo( _localAgentHost, 
                                          _localAgentService ) ;
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

   void omInstallBusinessReq::_clearSession( omManager *om, 
                                             pmdRemoteSession *remoteSession) 
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
         om->getRSManager()->removeSession( remoteSession ) ;
      }
   }

   INT32 omInstallBusinessReq::_applyInstallRequest( 
                                                  const BSONObj &bsonConfValue,
                                                  UINT64 taskID )
   {
      INT32 rc          = SDB_OK ;
      SINT32 flag       = SDB_OK ;
      BSONObj result ;
      CHAR* pContent    = NULL ;
      INT32 contentSize = 0 ;
      omManager *om     = sdbGetOMManager() ;
      MsgHeader *pMsg   = NULL ;
      pmdRemoteSession *remoteSession = NULL ;
      BSONObj request ;
      BSONObjBuilder builder ;
      builder.appendElements( bsonConfValue ) ;
      builder.append( OM_BSON_TASKID, (long long)taskID ) ;
      request = builder.obj() ;
      PD_LOG( PDDEBUG, "install req:%s", 
              request.toString( false, true ).c_str() ) ;

      rc = msgBuildQueryMsg( &pContent, &contentSize, 
                             CMD_ADMIN_PREFIX OM_INSTALL_BUSINESS_REQ, 
                             0, 0, 0, -1, &request, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build msg failed:cmd=%s,rc=%d", 
                     OM_INSTALL_BUSINESS_REQ, rc ) ;
         goto error ;
      }

      remoteSession = om->getRSManager()->addSession( _cb, 
                                                      OM_WAIT_SCAN_RES_INTERVAL,
                                                      NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "create remote session failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         goto error ;
      }

      pMsg = (MsgHeader *)pContent ;
      rc   = _sendMsgToLocalAgent( om, remoteSession, pMsg ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "send message to agent failed:rc=%d", rc ) ;
         SDB_OSS_FREE( pContent ) ;
         remoteSession->clearSubSession() ;
         goto error ;
      }

      rc = _receiveFromAgent( remoteSession, flag, result ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "receive from agent failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         _errorDetail = result.getStringField( OM_REST_RES_DETAIL ) ;
         PD_LOG_MSG( PDERROR, "agent process failed:detail=%s,rc=%d", 
                     _errorDetail.c_str(), rc ) ;
         goto error ;
      }
   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   /*
   bsonConfValue:
   {
      "BusinessType":"sequoiadb", "BusinessName":"b1", "deployMod":"xxx", 
      "ClusterName":"c1", 
      "Config":
      [
         {"HostName": "host1", "datagroupname": "", 
          "dbpath": "/home/db2/standalone/11830", "svcname": "11830", ...}
         ,...
      ]
   }
   bsonHostInfo:
   {
     "HostInfo":[
                   {
                      "HostName":"host1", "ClusterName":"c1", 
                      "Disk":{"Name":"/dev/sdb", Size:"", Mount:"", Used:""},
                      "Config":[{"BusinessName":"b2","dbpath":"", svcname:"", 
                                 "role":"", ... }, ...]
                   }
                    , ... 
                ]
   }
   */
   void omInstallBusinessReq::_compeleteConfValue( const BSONObj &bsonHostInfo, 
                                                   BSONObj &bsonConfValue )
   {
      map< string, simpleHostInfo > hostMap ;
      map< string, simpleHostInfo >::iterator iterMap ;
      BSONObj hostInfos ;
      hostInfos = bsonHostInfo.getObjectField( OM_BSON_FIELD_HOST_INFO ) ;
      BSONObjIterator iter( hostInfos ) ;
      while ( iter.more() )
      {
         simpleHostInfo host ;
         BSONElement ele  = iter.next() ;
         BSONObj oneHost  = ele.embeddedObject() ;

         host.hostName    = oneHost.getStringField( OM_HOST_FIELD_NAME ) ;

         host.user        = oneHost.getStringField( OM_HOST_FIELD_USER ) ;
         host.passwd      = oneHost.getStringField( 
                                                OM_HOST_FIELD_PASSWORD ) ;
         host.clusterName = oneHost.getStringField( 
                                                OM_HOST_FIELD_CLUSTERNAME ) ;
         host.sshPort     = oneHost.getStringField( 
                                                OM_HOST_FIELD_SSHPORT ) ;
         hostMap[ host.hostName ] = host ;
      }

      BSONObj condition = BSON( OM_BSON_FIELD_CONFIG << "" ) ;

      BSONArrayBuilder arrayBuilder ;
      BSONObj configs = bsonConfValue.filterFieldsUndotted( condition, true ) ;
      BSONObj commons = bsonConfValue.filterFieldsUndotted( condition, false ) ;
      BSONObj nodes ;
      nodes           = configs.getObjectField( OM_BSON_FIELD_CONFIG ) ;
      {
         BSONObjIterator iterBson( nodes ) ;
         while ( iterBson.more() )
         {
            BSONObjBuilder tmpBuilder ;
            BSONElement ele = iterBson.next() ;
            BSONObj oneNode = ele.embeddedObject() ;
            string hostName = oneNode.getStringField( 
                                                     OM_BSON_FIELD_HOST_NAME ) ;
            iterMap = hostMap.find( hostName ) ;
            SDB_ASSERT( iterMap != hostMap.end(), "" ) ;

            tmpBuilder.appendElements( oneNode ) ;
            tmpBuilder.append( OM_BSON_FIELD_HOST_USER, iterMap->second.user ) ;
            tmpBuilder.append( OM_BSON_FIELD_HOST_PASSWD, 
                               iterMap->second.passwd ) ;
            tmpBuilder.append( OM_BSON_FIELD_HOST_SSHPORT, 
                               iterMap->second.sshPort ) ;
            BSONObj tmp = tmpBuilder.obj() ;
            arrayBuilder.append( tmp ) ;
         }
      }

      INT32 rc = SDB_OK ;
      string clusterName ;
      string sdbUser ;
      string sdbUserGroup ;
      string sdbPasswd ;
      clusterName = bsonConfValue.getStringField( OM_BSON_FIELD_CLUSTER_NAME ) ;
      rc = _getSdbUsrInfo( clusterName, sdbUser, sdbPasswd, sdbUserGroup ) ;

      BSONObjBuilder valueBuilder ;
      if ( SDB_OK == rc )
      {
         valueBuilder.append( OM_BSON_FIELD_SDB_USER, sdbUser ) ;
         valueBuilder.append( OM_BSON_FIELD_SDB_PASSWD, sdbPasswd ) ;
         valueBuilder.append( OM_BSON_FIELD_SDB_USERGROUP, sdbUserGroup ) ;
      }

      valueBuilder.append( OM_BSON_FIELD_CONFIG, arrayBuilder.arr() ) ;
      valueBuilder.appendElements( commons ) ;

      bsonConfValue = valueBuilder.obj() ;
   }

   INT32 omInstallBusinessReq::_getRestInfo( BSONObj &bsonConfValue )
   {
      INT32 rc          = SDB_OK ;
      const CHAR *pInfo = NULL ;

      _restAdaptor->getQuery( _restSession, OM_REST_CONFIG_INFO, &pInfo ) ;
      if ( NULL == pInfo )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "rest field is null:field=%s", OM_REST_CONFIG_INFO ) ;
         goto error ;
      }

      rc = fromjson( pInfo, bsonConfValue ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                     "rc=%d", pInfo, rc ) ;
         goto error ;
      }

      _clusterName = bsonConfValue.getStringField( 
                                                  OM_BSON_FIELD_CLUSTER_NAME ) ;
      if ( _clusterName == "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is empty", OM_BSON_FIELD_CLUSTER_NAME ) ;
         goto error ;
      }

      _businessType = bsonConfValue.getStringField( OM_BSON_BUSINESS_TYPE ) ;
      if ( _businessType == "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is empty", OM_BSON_BUSINESS_TYPE ) ;
         goto error ;
      }

      _businessName = bsonConfValue.getStringField( OM_BSON_BUSINESS_NAME ) ;
      if ( _businessName == "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is empty", OM_BSON_BUSINESS_NAME ) ;
         goto error ;
      }

      _deployMod = bsonConfValue.getStringField( OM_BSON_DEPLOY_MOD ) ;
      if ( _deployMod == "" )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s is empty", OM_BSON_DEPLOY_MOD ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omInstallBusinessReq::_generateTaskInfo( const BSONObj &bsonConfValue, 
                                                  BSONObj &taskInfo, 
                                                  BSONArray &resultInfo )
   {
      INT32 rc = SDB_OK ;
      taskInfo = bsonConfValue.copy() ;
      BSONArrayBuilder arrayBuilder ;
      BSONObj condition = BSON( OM_BSON_FIELD_HOST_NAME << "" 
                                << OM_CONF_DETAIL_SVCNAME << "" 
                                << OM_CONF_DETAIL_ROLE << "" 
                                << OM_CONF_DETAIL_DATAGROUPNAME << "" ) ;

      BSONObj nodes = bsonConfValue.getObjectField( OM_BSON_FIELD_CONFIG ) ;
      BSONObjIterator iter( nodes ) ;
      while ( iter.more() )
      {
         BSONElement ele = iter.next() ;
         BSONObj oneNode = ele.embeddedObject() ;

         BSONObj tmp = oneNode.filterFieldsUndotted( condition, true ) ;

         BSONObjBuilder builder ;
         builder.appendElements( tmp ) ;
         builder.append( OM_TASKINFO_FIELD_STATUS, OM_TASK_STATUS_INIT ) ;
         builder.append( OM_TASKINFO_FIELD_STATUS_DESC,
                         getTaskStatusStr( OM_TASK_STATUS_INIT ) ) ;
         builder.append( OM_REST_RES_RETCODE, SDB_OK ) ;
         builder.append( OM_REST_RES_DETAIL, "" ) ;
         {
            BSONArrayBuilder tmpEmptyBuilder ;
            builder.append( OM_TASKINFO_FIELD_FLOW, tmpEmptyBuilder.arr() ) ;
         }

         BSONObj result = builder.obj() ;
         arrayBuilder.append( result ) ;
      }

      resultInfo = arrayBuilder.arr() ;

      return rc ;
   }

   INT32 omInstallBusinessReq::doCommand()
   {
      INT32 rc = SDB_OK ;
      omConfigGenerator confGenerator ;
      BSONObjBuilder opBuilder ;
      BSONObj bsonConfValue ;
      BSONObj bsonHostInfo ;
      BSONObj bsonAllConf ;
      INT64 taskID ;

      _setFileLanguageSep() ;

      rc = _getRestInfo( bsonConfValue ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getRestInfo failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _checkBusiness( _businessName, _businessType, _deployMod, 
                           _clusterName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_checkBusiness failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _combineConfDetail( _businessType, _deployMod, bsonAllConf ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_combineConfDetail failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _extractHostInfo( bsonConfValue, bsonHostInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get host info failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = confGenerator.checkSDBConfig( bsonConfValue, bsonAllConf, 
                                         bsonHostInfo ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = confGenerator.getErrorDetail() ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _compeleteConfValue( bsonHostInfo, bsonConfValue ) ;

      {
         BSONObj taskInfo ;
         BSONArray resultInfo ;
         rc = _generateTaskInfo( bsonConfValue, taskInfo, resultInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "generate task info failed:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
            goto error ;
         }

         getMaxTaskID( taskID ) ;
         taskID++ ;

         rc = createTask( OM_TASK_TYPE_ADD_BUSINESS, taskID, 
                          getTaskTypeStr( OM_TASK_TYPE_ADD_BUSINESS ), 
                          _localAgentHost, _localAgentService,
                          taskInfo, resultInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "fail to _saveTask:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
            goto error ;
         }
      }

      rc = _notifyAgentTask( taskID ) ;
      if ( SDB_OK != rc )
      {
         removeTask( taskID ) ;
         PD_LOG( PDERROR, "fail to _notifyAgentTask:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      {
         BSONObj result = BSON( OM_BSON_TASKID << (long long)taskID ) ;
         _restAdaptor->appendHttpBody( _restSession, result.objdata(), 
                                       result.objsize(), 1 ) ;
         _sendOKRes2Web() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   omListTaskCommand::omListTaskCommand( restAdaptor *pRestAdaptor, 
                                         pmdRestSession *pRestSession )
                     :omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omListTaskCommand::~omListTaskCommand()
   {
   }

   INT32 omListTaskCommand::_getTaskList( list<BSONObj> &taskList )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID = -1 ;

      selector = BSON( OM_TASKINFO_FIELD_TASKID << 1 
                       << OM_TASKINFO_FIELD_TYPE << 1
                       << OM_TASKINFO_FIELD_TYPE_DESC << 1
                       << OM_TASKINFO_FIELD_NAME << 1 ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_TASKINFO, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_TASKINFO ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_TASKINFO ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         taskList.push_back( record.copy() ) ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   void omListTaskCommand::_sendTaskList2Web( list<BSONObj> &taskList )
   {
      BSONObjBuilder opBuilder ;
      list<BSONObj>::iterator iter = taskList.begin() ;
      while ( iter != taskList.end() )
      {
         _restAdaptor->appendHttpBody( _restSession, iter->objdata(), 
                                       iter->objsize(), 1 ) ;
         iter++ ;
      }

      _sendOKRes2Web() ;

      return ;
   }

   INT32 omListTaskCommand::doCommand() 
   {
      INT32 rc = SDB_OK ;
      list<BSONObj> taskList ;
      rc = _getTaskList( taskList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get task list failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendTaskList2Web( taskList ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omQueryTaskCommand::omQueryTaskCommand( restAdaptor *pRestAdaptor, 
                                           pmdRestSession *pRestSession )
                      :omAuthCommand( pRestAdaptor, pRestSession ) 
   {
   }

   omQueryTaskCommand::~omQueryTaskCommand()
   {
   }

   void omQueryTaskCommand::_sendTaskInfo2Web( list<BSONObj> &tasks )
   {
      string clusterName ;
      string businessName ;
      BSONObj filter = BSON( OM_TASKINFO_FIELD_INFO << "" ) ;

      list<BSONObj>::iterator iter = tasks.begin() ;
      while ( iter != tasks.end() )
      {
         BSONElement clusterEle ;
         clusterEle = iter->getFieldDotted( 
                         OM_TASKINFO_FIELD_INFO"."OM_BSON_FIELD_CLUSTER_NAME ) ;
         clusterName  = clusterEle.String() ;

         BSONElement businessEle ;
         businessEle = iter->getFieldDotted( 
                         OM_TASKINFO_FIELD_INFO"."OM_BSON_BUSINESS_NAME ) ;
         businessName = businessEle.String() ;

         BSONObj info = BSON( OM_BSON_FIELD_CLUSTER_NAME << clusterName 
                              << OM_BSON_BUSINESS_NAME << businessName ) ;

         BSONObj tmp = iter->filterFieldsUndotted( filter, false ) ;
         BSONObjBuilder resultBuilder ;
         resultBuilder.appendElements( tmp ) ;
         resultBuilder.append( OM_TASKINFO_FIELD_INFO, info ) ;

         BSONObj result = resultBuilder.obj() ;
         _restAdaptor->appendHttpBody( _restSession, result.objdata(), 
                                       result.objsize(), 1 ) ;
         iter++ ;
      }

      _sendOKRes2Web() ;

      return ;
   }

   INT32 omQueryTaskCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      list<BSONObj> tasks ;

      rc = _getQueryPara( selector, matcher, order, hint ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _queryTable( OM_CS_DEPLOY_CL_TASKINFO, selector, matcher, order, 
                        hint, 0, 0, -1, tasks ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendTaskInfo2Web( tasks ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omListNodeCommand::omListNodeCommand( restAdaptor *pRestAdaptor,
                                         pmdRestSession *pRestSession )
                     :omAuthCommand( pRestAdaptor, pRestSession ) 
   {
   }

   omListNodeCommand::~omListNodeCommand()
   {
   }

   INT32 omListNodeCommand::_getNodeList( string businessName,
                                          list<simpleNodeInfo> &nodeList )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID = -1 ;

      matcher  = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CONFIGURE, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_CONFIGURE ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_CONFIGURE ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         BSONObj confs ;
         confs    = record.getObjectField( OM_CONFIGURE_FIELD_CONFIG ) ;
         {
            BSONObjIterator iterBson( confs ) ;
            while ( iterBson.more() )
            {
               BSONObjBuilder builder ;
               BSONElement ele = iterBson.next() ;
               BSONObj oneNode = ele.embeddedObject() ;
               simpleNodeInfo nodeInfo ;
               nodeInfo.hostName = record.getStringField( 
                                                 OM_CONFIGURE_FIELD_HOSTNAME ) ;
               nodeInfo.role     = oneNode.getStringField( 
                                                 OM_CONF_DETAIL_ROLE ) ;
               nodeInfo.svcName  = oneNode.getStringField( 
                                                 OM_CONF_DETAIL_SVCNAME ) ;
               nodeList.push_back( nodeInfo ) ;
            }
         }
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   void omListNodeCommand::_sendNodeList2Web( list<simpleNodeInfo> &nodeList )
   {
      BSONObjBuilder opBuilder ;
      list<simpleNodeInfo>::iterator iter = nodeList.begin() ;
      while ( iter != nodeList.end() )
      {
         BSONObj node ;
         node = BSON( OM_BSON_FIELD_HOST_NAME << (*iter).hostName 
                      << OM_BSON_FIELD_ROLE << (*iter).role
                      << OM_BSON_FIELD_SVCNAME << (*iter).svcName ) ;
         _restAdaptor->appendHttpBody( _restSession, node.objdata(), 
                                       node.objsize(), 1 ) ;
         iter++ ;
      }

      _sendOKRes2Web() ;

      return ;
   }

   INT32 omListNodeCommand::doCommand()
   {
      INT32 rc                  = SDB_OK ;
      const CHAR *businessName  = NULL ;
      list<simpleNodeInfo> nodeList ;

      _restAdaptor->getQuery( _restSession, OM_REST_BUSINESS_NAME, 
                              &businessName ) ;
      if ( NULL == businessName )
      {
         _errorDetail = "rest field miss:" + string( OM_REST_BUSINESS_NAME ) ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _getNodeList( businessName, nodeList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getNodeList failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendNodeList2Web( nodeList ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   omQueryNodeConfCommand::omQueryNodeConfCommand( restAdaptor *pRestAdaptor, 
                                                  pmdRestSession *pRestSession )
                          :omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omQueryNodeConfCommand::~omQueryNodeConfCommand()
   {
   }

   void omQueryNodeConfCommand::_expandNodeInfo( BSONObj &oneConfig, 
                                                 const string &svcName,
                                                 BSONObj &nodeinfo )
   {
      string hostName ;
      string business ;
      BSONObj confs ;
      hostName = oneConfig.getStringField( OM_CONFIGURE_FIELD_HOSTNAME ) ;
      business = oneConfig.getStringField( OM_CONFIGURE_FIELD_BUSINESSNAME ) ;
      confs    = oneConfig.getObjectField( OM_CONFIGURE_FIELD_CONFIG ) ;
      {
         BSONObjIterator iterBson( confs ) ;
         while ( iterBson.more() )
         {
            BSONObjBuilder builder ;
            BSONElement ele = iterBson.next() ;
            BSONObj oneNode = ele.embeddedObject() ;

            if ( oneNode.getStringField( OM_CONF_DETAIL_SVCNAME ) == svcName )
            {
               builder.append( OM_BSON_BUSINESS_NAME, business ) ;
               builder.append( OM_BSON_FIELD_HOST_NAME, hostName ) ;
               builder.appendElements( oneNode ) ;
               nodeinfo = builder.obj() ;
               break ;
            }
         }
      }
   }

   INT32 omQueryNodeConfCommand::_getNodeInfo( const string &hostName, 
                                               const string &svcName, 
                                               BSONObj &nodeinfo )
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;

      BSONObj tmpSvcName = BSON( OM_CONF_DETAIL_SVCNAME << svcName ) ;
      BSONObj elemMatch  = BSON( "$elemMatch" << tmpSvcName ) ;
      matcher = BSON( OM_CONFIGURE_FIELD_HOSTNAME << hostName 
                      << OM_CONFIGURE_FIELD_CONFIG << elemMatch ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CONFIGURE, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_CONFIGURE ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_CONFIGURE ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         _expandNodeInfo( result, svcName, nodeinfo ) ;
         break ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   void omQueryNodeConfCommand::_sendNodeInfo2Web( BSONObj &nodeInfo )
   {
      _restAdaptor->appendHttpBody( _restSession, nodeInfo.objdata(), 
                                    nodeInfo.objsize(), 1 ) ;
      _sendOKRes2Web() ;
      return ;
   }

   INT32 omQueryNodeConfCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      const CHAR* pHostName = NULL ;
      const CHAR* pSvcName  = NULL ;
      BSONObj nodeInfo ;

      _restAdaptor->getQuery( _restSession, OM_REST_HOST_NAME, &pHostName ) ;
      _restAdaptor->getQuery( _restSession, OM_REST_SVCNAME, &pSvcName ) ;
      if ( NULL == pHostName || NULL == pSvcName )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "rest field miss:field=%s or field=%s", 
                     OM_REST_HOST_NAME, OM_REST_SVCNAME ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _getNodeInfo( pHostName, pSvcName, nodeInfo ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         PD_LOG( PDERROR, "get node info failed:rc=", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendNodeInfo2Web( nodeInfo ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   omListBusinessCommand::omListBusinessCommand( restAdaptor *pRestAdaptor, 
                                                 pmdRestSession *pRestSession )
                         :omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omListBusinessCommand::~omListBusinessCommand()
   {
   }

   INT32 omListBusinessCommand::doCommand()
   {
      INT32 rc                  = SDB_OK ;
      const CHAR *pClusterName  = NULL ;
      const CHAR *pHostName     = NULL ;
      list<BSONObj> businessList ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj selector ;
      BSONObj matcher ;

      _restAdaptor->getQuery( _restSession, OM_REST_CLUSTER_NAME, 
                              &pClusterName ) ;
      _restAdaptor->getQuery( _restSession, OM_REST_HOST_NAME, &pHostName ) ;
      if ( NULL != pClusterName )
      {
         selector = BSON( OM_BUSINESS_FIELD_NAME << 1 
                          << OM_BUSINESS_FIELD_TYPE << 1 ) ;
         matcher = BSON( OM_BUSINESS_FIELD_CLUSTERNAME << pClusterName ) ;
         rc = _queryTable( OM_CS_DEPLOY_CL_BUSINESS, selector, matcher, order,
                           hint, 0, 0, -1, businessList ) ;
      }
      else if ( NULL != pHostName )
      {
         list<BSONObj> tmpRecords ;
         selector = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << 1 ) ;
         matcher  = BSON( OM_CONFIGURE_FIELD_HOSTNAME << pHostName ) ;
         rc = _queryTable( OM_CS_DEPLOY_CL_CONFIGURE, selector, matcher, order,
                           hint, 0, 0, -1, tmpRecords ) ;
         if ( SDB_OK == rc && tmpRecords.size() > 0 )
         {
            BSONArrayBuilder arrBuilder ;
            list<BSONObj>::iterator iter = tmpRecords.begin() ;
            while ( iter != tmpRecords.end() )
            {
               arrBuilder.append( *iter ) ;
               iter++ ;
            }

            selector = BSON( OM_BUSINESS_FIELD_NAME << 1 
                             << OM_BUSINESS_FIELD_TYPE << 1 ) ;
            matcher  = BSON( "$or" << arrBuilder.arr() ) ;
            rc = _queryTable( OM_CS_DEPLOY_CL_BUSINESS, selector, matcher, 
                              order, hint, 0, 0, -1, businessList ) ;
         }
      }
      else
      {
         selector = BSON( OM_BUSINESS_FIELD_NAME << 1 
                          << OM_BUSINESS_FIELD_TYPE << 1) ;
         rc = _queryTable( OM_CS_DEPLOY_CL_BUSINESS, selector, matcher, order,
                           hint, 0, 0, -1, businessList ) ;
      }

      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "list business failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendBusinessList2Web( businessList ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   void omListBusinessCommand::_sendBusinessList2Web( 
                                                   list<BSONObj> &businessList )
   {
      BSONObjBuilder opBuilder ;
      list<BSONObj>::iterator iter = businessList.begin() ;
      while ( iter != businessList.end() )
      {
         _restAdaptor->appendHttpBody( _restSession, iter->objdata(), 
                                       iter->objsize(), 1 ) ;
         iter++ ;
      }

      _sendOKRes2Web() ;

      return ;
   }

   omQueryBusinessCommand::omQueryBusinessCommand( restAdaptor *pRestAdaptor, 
                                                  pmdRestSession *pRestSession )
                          :omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omQueryBusinessCommand::~omQueryBusinessCommand()
   {
   }

   void omQueryBusinessCommand::_sendBusinessInfo2Web( 
                                                  list<BSONObj> &businessInfo )
   {
      list<BSONObj>::iterator iter = businessInfo.begin() ;
      while ( iter != businessInfo.end() )
      {
         _restAdaptor->appendHttpBody( _restSession, iter->objdata(), 
                                       iter->objsize(), 1 ) ;
         iter++ ;
      }

      _sendOKRes2Web() ;

      return ;
   }

   INT32 omQueryBusinessCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ; 
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      list<BSONObj> businessInfo ;

      rc = _getQueryPara( selector, matcher, order, hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get query parameter failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _queryTable( OM_CS_DEPLOY_CL_BUSINESS, selector, matcher, order, 
                        hint, 0, 0, -1, businessInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "query table failed:table=%s,rc=%d", 
                 OM_CS_DEPLOY_CL_BUSINESS, rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendBusinessInfo2Web( businessInfo ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   omStartBusinessCommand::omStartBusinessCommand( restAdaptor *pRestAdaptor, 
                                                   pmdRestSession *pRestSession,
                                                   string localAgentHost, 
                                                   string localAgentService )
                          :omScanHostCommand( pRestAdaptor, pRestSession,
                                             localAgentHost, localAgentService ) 
   {
   }

   omStartBusinessCommand::~omStartBusinessCommand()
   {
   }

   INT32 omStartBusinessCommand::_getNodeInfo( const string &businessName, 
                                               BSONObj &nodeInfos,
                                               BOOLEAN &isExistFlag )
   {
      BSONArrayBuilder arrayBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;

      isExistFlag      = FALSE ;

      matcher = BSON( OM_CONFIGURE_FIELD_BUSINESSNAME << businessName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CONFIGURE, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_CONFIGURE ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         string hostName ;
         BSONObjBuilder builder ;
         rc = rtnGetMore( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc          = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_CONFIGURE ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         isExistFlag = TRUE ;
         BSONObj result( buffObj.data() ) ;
         rc = _expandNodeInfoToBuilder( result, arrayBuilder ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "_expandNodeInfoToBuilder failed:rc=%d", rc ) ;
            goto error ;
         }
      }

      nodeInfos = BSON( OM_BSON_FIELD_CONFIG << arrayBuilder.arr() ) ;
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   /*
     record:
     {
       BusinessName:"b1", Hostname:"h1"
       Config:
       [
         {
           dbpath:"", role:"", logfilesz:"", ...
         }, ...
       ]
     }

     this function transfer record to a BSON like below, and add to arrayBuilder

     {
       HostName:"", User:"", Passwd:"", dbpath:"", role:"", logfilesz:"", ...
     }
   */
   INT32 omStartBusinessCommand::_expandNodeInfoToBuilder( 
                                                const BSONObj &record, 
                                                BSONArrayBuilder &arrayBuilder )
   {
      INT32 rc = SDB_OK ;
      string hostName ;
      simpleHostInfo hostInfo ;
      BOOLEAN isHostExist = FALSE ;
      BSONObj confs ;
      hostName = record.getStringField( OM_CONFIGURE_FIELD_HOSTNAME ) ;
      rc = _getHostInfo( hostName, hostInfo, isHostExist ) ;
      if ( SDB_OK != rc || !isHostExist )
      {
         _errorDetail = string( "failed to get record from table:" )
                        + OM_CS_DEPLOY_CL_HOST + ",host=" + hostName ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      confs = record.getObjectField( OM_CONFIGURE_FIELD_CONFIG ) ;
      {
         CHAR catName[ OM_INT32_LENGTH + 1 ] = "" ;
         BSONObjIterator iter( confs ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "config info is invalid, element of %s "
                           "is not Object type:type=%d", 
                           OM_CONFIGURE_FIELD_CONFIG, ele.type() ) ;
               _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               goto error ;
            }
            BSONObj tmp = ele.embeddedObject() ;
            BSONObjBuilder builder ;
            builder.appendElements( tmp ) ;
            string svcName = tmp.getStringField( OM_CONF_DETAIL_SVCNAME ) ;
            INT32 iSvcName = ossAtoi( svcName.c_str() ) ;
            INT32 iCatName = iSvcName + MSG_ROUTE_CAT_SERVICE ;
            ossItoa( iCatName, catName, OM_INT32_LENGTH ) ;
            builder.append( OM_CONF_DETAIL_CATANAME, catName ) ;
            builder.append( OM_BSON_FIELD_HOST_NAME, hostInfo.hostName ) ;
            builder.append( OM_BSON_FIELD_HOST_USER, hostInfo.user ) ;
            builder.append( OM_BSON_FIELD_HOST_PASSWD, hostInfo.passwd ) ;

            BSONObj oneNode = builder.obj() ;
            arrayBuilder.append( oneNode ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omStartBusinessCommand::_getHostInfo( const string &hostName,
                                               simpleHostInfo &hostInfo,
                                               BOOLEAN &isExistFlag )
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;

      matcher = BSON( OM_HOST_FIELD_NAME << hostName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_HOST ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc          = SDB_OK ;
               isExistFlag = FALSE ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_HOST ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         hostInfo.hostName    = result.getStringField( OM_HOST_FIELD_NAME ) ;
         hostInfo.clusterName = result.getStringField( 
                                                   OM_HOST_FIELD_CLUSTERNAME ) ;
         hostInfo.ip          = result.getStringField( OM_HOST_FIELD_IP ) ;
         hostInfo.user        = result.getStringField( OM_HOST_FIELD_USER ) ;
         hostInfo.passwd      = result.getStringField( 
                                                   OM_HOST_FIELD_PASSWORD ) ;
         hostInfo.installPath = result.getStringField( 
                                                   OM_HOST_FIELD_INSTALLPATH ) ;
         hostInfo.agentPort   = result.getStringField( 
                                                   OM_HOST_FIELD_AGENT_PORT ) ;
         hostInfo.sshPort     = result.getStringField( 
                                                   OM_HOST_FIELD_SSHPORT ) ;
         isExistFlag = TRUE ;
         break ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omStartBusinessCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      return rc ;
   }

   omStopBusinessCommand::omStopBusinessCommand( restAdaptor *pRestAdaptor, 
                                                 pmdRestSession *pRestSession,
                                                 string localAgentHost, 
                                                 string localAgentService )
                         :omScanHostCommand( pRestAdaptor, pRestSession,
                                             localAgentHost, localAgentService ) 
   {
   }

   omStopBusinessCommand::~omStopBusinessCommand()
   {
   }

   INT32 omStopBusinessCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      return rc ;
   }

   omRemoveClusterCommand::omRemoveClusterCommand( restAdaptor *pRestAdaptor, 
                                                  pmdRestSession *pRestSession )
                          :omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omRemoveClusterCommand::~omRemoveClusterCommand()
   {
   }

   INT32 omRemoveClusterCommand::_getClusterExistFlag( 
                                                      const string &clusterName, 
                                                      BOOLEAN &flag )
   {
      INT32 rc = SDB_OK ;
      BSONObj clusterInfo ;
      rc = _getClusterInfo( clusterName, clusterInfo ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            rc   = SDB_OK ;
            flag = false ;
            goto done ;
         }

         PD_LOG( PDERROR, "get clusterInfo failed:cluster=%s,rc=%d", 
                 clusterName.c_str(), rc ) ;
         goto error ;
      }

      flag = TRUE ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveClusterCommand::_getClusterExistHostFlag( 
                                                      const string &clusterName, 
                                                      BOOLEAN &flag )
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;

      matcher = BSON( OM_HOST_FIELD_CLUSTERNAME << clusterName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_HOST ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               flag = FALSE ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_HOST ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         flag = TRUE ;
         break ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveClusterCommand::_removeCluster( const string &clusterName )
   {
      INT32 rc          = SDB_OK ;
      BSONObj condition = BSON( OM_CLUSTER_FIELD_NAME << clusterName ) ;
      BSONObj hint ;

      rc = rtnDelete( OM_CS_DEPLOY_CL_CLUSTER, condition, hint, 0, _cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to delete taskinfo from table:%s,"
                     "%s=%s,rc=%d", OM_CS_DEPLOY_CL_CLUSTER, 
                     OM_CLUSTER_FIELD_NAME, clusterName.c_str(), rc ) ;
         _errorDetail = _cb->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveClusterCommand::doCommand()
   {
      INT32 rc                   = SDB_OK ;
      const CHAR *clusterName    = NULL ;
      BOOLEAN isClusterExist     = FALSE ;
      BSONObj result ;
      BOOLEAN isClusterExistHost = FALSE ;

      _restAdaptor->getQuery( _restSession, OM_REST_CLUSTER_NAME, 
                              &clusterName ) ;
      if ( NULL == clusterName )
      {
         _errorDetail = "rest field:" + string( OM_REST_CLUSTER_NAME )
                        + " is null" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _getClusterExistFlag( clusterName, isClusterExist ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      if ( !isClusterExist )
      {
         rc = SDB_DMS_RECORD_NOTEXIST ;
         _errorDetail = string( clusterName ) + " is not exist" ;
         PD_LOG( PDERROR, "%s:rc=%d", _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto done ;
      }

      rc = _getClusterExistHostFlag( clusterName, isClusterExistHost ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      if ( isClusterExistHost )
      {
         rc = SDB_INVALIDARG ;
         _errorDetail = string( "host exist in cluster, host should be "
                                "removed first:cluster=" ) + clusterName ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _removeCluster( clusterName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _sendOKRes2Web() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omRemoveHostCommand::omRemoveHostCommand( restAdaptor *pRestAdaptor, 
                                             pmdRestSession *pRestSession,
                                             string localAgentHost, 
                                             string localAgentService )
                       :omStartBusinessCommand( pRestAdaptor, pRestSession, 
                                                localAgentHost, 
                                                localAgentService )
   {
   }

   omRemoveHostCommand::~omRemoveHostCommand()
   {
   }

   INT32 omRemoveHostCommand::_getHostName( string &hostName, 
                                            BOOLEAN &isForced )
   {
      INT32 rc              = SDB_OK ;
      const CHAR *pHostName = NULL ;
      const CHAR *pForce    = NULL ;
      _restAdaptor->getQuery( _restSession, OM_REST_HOST_NAME, 
                              &pHostName ) ;
      if ( NULL == pHostName )
      {
         _errorDetail = "rest field:" + string( OM_REST_HOST_NAME )
                        + " is null" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      hostName = pHostName ;
      isForced = FALSE ;
      _restAdaptor->getQuery( _restSession, OM_REST_ISFORCE, 
                              &pForce ) ;
      if ( ( NULL != pForce ) && ( ossStrcasecmp( pForce, "1" ) == 0 ) )
      {
         isForced = TRUE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveHostCommand::_getHostExistBusinessFlag( const string &hostName, 
                                                         BOOLEAN &flag )
   {
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;

      matcher = BSON( OM_CONFIGURE_FIELD_HOSTNAME << hostName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CONFIGURE, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_CONFIGURE ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               flag = FALSE ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_CONFIGURE ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         flag = TRUE ;
         break ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveHostCommand::_removeHostByAgent( 
                                                const simpleHostInfo &hostInfo )
   {
      INT32 rc          = SDB_OK ;
      SINT32 flag       = SDB_OK ;
      CHAR *pContent    = NULL ;
      INT32 contentSize = 0 ;
      omManager *om     = NULL ;
      MsgHeader *pMsg   = NULL ;

      pmdRemoteSession *remoteSession = NULL ;
      BSONObj bsonResponse ;
      BSONObj bsonRequest ;
      bsonRequest = BSON( OM_BSON_FIELD_HOST_NAME << hostInfo.hostName 
                       << OM_BSON_FIELD_HOST_IP << hostInfo.ip 
                       << OM_BSON_FIELD_HOST_USER << hostInfo.user 
                       << OM_BSON_FIELD_HOST_PASSWD << hostInfo.passwd 
                       << OM_BSON_FIELD_INSTALLPATH << hostInfo.installPath
                       << OM_BSON_FIELD_HOST_SSHPORT << hostInfo.sshPort ) ;

      PD_LOG( PDDEBUG, "remove host req:%s", 
              bsonRequest.toString( false, true ).c_str() ) ;
      rc = msgBuildQueryMsg( &pContent, &contentSize, 
                             CMD_ADMIN_PREFIX OM_REMOVE_HOST_REQ, 
                             0, 0, 0, -1, &bsonRequest, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = string( "build message failed:cmd=" ) 
                        + OM_REMOVE_HOST_REQ ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      om   = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                      OM_MSG_TIMEOUT_TWO_HOUR,
                                                      NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         _errorDetail = string( "create remote session failed" ) ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         SDB_OSS_FREE( pContent ) ;
         goto error ;
      }

      pMsg = (MsgHeader *)pContent ;
      rc   = _sendMsgToLocalAgent( om, remoteSession, pMsg ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = string( "send message to agent failed" ) ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         SDB_OSS_FREE( pContent ) ;
         remoteSession->clearSubSession() ;
         goto error ;
      }

      rc = _receiveFromAgent( remoteSession, flag, bsonResponse ) ;
      if ( SDB_OK != rc )
      {
         _errorDetail = string( "receive from agent failed" ) ;
         PD_LOG( PDERROR, "%s:rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      if ( SDB_OK != flag )
      {
         rc = flag ;
         _errorDetail = bsonResponse.getStringField( OM_REST_RES_DETAIL ) ;
         PD_LOG( PDERROR, "agent process failed:detail=%s,rc=%d", 
                 _errorDetail.c_str(), rc ) ;
         goto error ;
      }

   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveHostCommand::_removeHost( const simpleHostInfo &hostInfo, 
                                           BOOLEAN isForced )
   {
      INT32 rc = SDB_OK ;
      rc = _removeHostByAgent( hostInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "agent remove host failed:rc=%d", rc ) ;
         if ( !isForced )
         {
            goto error ;
         }
      }

      _errorDetail = "" ;
      rc = _deleteHost( hostInfo.hostName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "delete host's record failed:host=%s,rc=%d", 
                 hostInfo.hostName.c_str(), rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveHostCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      string hostName ;
      BSONObj result ;
      simpleHostInfo hostInfo ;
      BOOLEAN isForced            = FALSE ;
      BOOLEAN isHostExist         = FALSE ;
      BOOLEAN isHostExistBusiness = FALSE ;

      rc = _getHostName( hostName, isForced ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _getHostInfo( hostName, hostInfo, isHostExist ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      if ( !isHostExist )
      {
         rc = SDB_DMS_RECORD_NOTEXIST ;
         _errorDetail = hostName + " is not exist" ;
         PD_LOG( PDERROR, "%s:rc=%d", _errorDetail.c_str(), rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto done ;
      }

      rc = _getHostExistBusinessFlag( hostName, isHostExistBusiness ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      if ( isHostExistBusiness )
      {
         rc = SDB_INVALIDARG ;
         _errorDetail = string( "business exist in host, business should be "
                                "removed first:host=" ) + hostName.c_str() ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _removeHost( hostInfo, isForced ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      sdbGetOMManager()->updateClusterVersion( hostInfo.clusterName ) ;

      _sendOKRes2Web() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omRemoveBusinessCommand::omRemoveBusinessCommand( restAdaptor *pRestAdaptor, 
                                                   pmdRestSession *pRestSession,
                                                   string localAgentHost, 
                                                   string localAgentService )
                           :omStartBusinessCommand( pRestAdaptor, pRestSession, 
                                                    localAgentHost, 
                                                    localAgentService )
   {
   }

   omRemoveBusinessCommand::~omRemoveBusinessCommand()
   {
   }

   INT32 omRemoveBusinessCommand::_getBusinessExistFlag( 
                                     const string &businessName, BOOLEAN &flag )
   {
      INT32 rc = SDB_OK ;
      BSONObj businessInfo ;
      rc = _getBusinessInfo( businessName, businessInfo ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            rc   = SDB_OK ;
            flag = false ;
            goto done ;
         }

         PD_LOG( PDERROR, "get businessInfo failed:business=%s,rc=%d", 
                 businessName.c_str(), rc ) ;
         goto error ;
      }

      flag = TRUE ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveBusinessCommand::_generateTaskInfo( string businessName,
                                                     BSONObj &nodeInfos, 
                                                     BSONObj &taskInfo,
                                                     BSONArray &resultInfo )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder taskInfoBuilder ;
      taskInfoBuilder.appendElements( nodeInfos ) ;
      taskInfoBuilder.append( OM_SDB_AUTH_USER, "" ) ;
      taskInfoBuilder.append( OM_SDB_AUTH_PASSWD, "" ) ;

      BSONObj businessInfo ;
      string clusterName ;
      string type ;
      string deployMod ;
      rc = _getBusinessInfo( businessName, businessInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get business info failed:business=%s", 
                 businessName.c_str() ) ;
         goto error ;
      }

      clusterName = businessInfo.getStringField( 
                                               OM_BUSINESS_FIELD_CLUSTERNAME ) ;
      type        = businessInfo.getStringField( OM_BUSINESS_FIELD_TYPE ) ;
      deployMod   = businessInfo.getStringField( OM_BUSINESS_FIELD_DEPLOYMOD ) ;

      taskInfoBuilder.append( OM_BSON_FIELD_CLUSTER_NAME, clusterName ) ;
      taskInfoBuilder.append( OM_BSON_BUSINESS_TYPE, type ) ;
      taskInfoBuilder.append( OM_BSON_BUSINESS_NAME, businessName ) ;
      taskInfoBuilder.append( OM_BSON_DEPLOY_MOD, deployMod ) ;
      taskInfo = taskInfoBuilder.obj() ;

      {
         BSONArrayBuilder arrayBuilder ;
         BSONObj filter = BSON( OM_BSON_FIELD_HOST_NAME << ""
                                << OM_CONF_DETAIL_SVCNAME << "" 
                                << OM_CONF_DETAIL_ROLE << "" 
                                << OM_CONF_DETAIL_DATAGROUPNAME << "" ) ;
         BSONObj nodes = nodeInfos.getObjectField( OM_BSON_FIELD_CONFIG ) ;
         BSONObjIterator iter( nodes ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            BSONObj oneNode = ele.embeddedObject() ;

            BSONObj tmp = oneNode.filterFieldsUndotted( filter, true ) ;
            BSONObjBuilder builder ;
            builder.appendElements( tmp ) ;
            builder.append( OM_TASKINFO_FIELD_STATUS, OM_TASK_STATUS_INIT ) ;
            builder.append( OM_TASKINFO_FIELD_STATUS_DESC,
                            getTaskStatusStr( OM_TASK_STATUS_INIT ) ) ;
            builder.append( OM_REST_RES_RETCODE, SDB_OK ) ;
            builder.append( OM_REST_RES_DETAIL, "" ) ;
            {
               BSONArrayBuilder tmpEmptyBuilder ;
               builder.append( OM_TASKINFO_FIELD_FLOW, tmpEmptyBuilder.arr() ) ;
            }

            BSONObj result = builder.obj() ;
            arrayBuilder.append( result ) ;
         }

         resultInfo = arrayBuilder.arr() ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRemoveBusinessCommand::doCommand()
   {
      BSONObj nodeInfos ;
      INT64 taskID ;
      BOOLEAN isBusinessExist     = FALSE ;
      BOOLEAN isBusinessExistNode = FALSE ;
      INT32 rc                    = SDB_OK ;
      const CHAR *pBusinessName   = NULL ;
      _restAdaptor->getQuery( _restSession, OM_REST_BUSINESS_NAME, 
                              &pBusinessName ) ;
      if ( NULL == pBusinessName )
      {
         _errorDetail = "rest field:" + string( OM_REST_BUSINESS_NAME )
                        + " is null" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _getBusinessExistFlag( pBusinessName, isBusinessExist ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      if ( !isBusinessExist )
      {
         string detail = string( pBusinessName ) + " is not exist" ;
         _sendErrorRes2Web( SDB_DMS_RECORD_NOTEXIST, detail ) ;
         goto done ;
      }

      rc = _getNodeInfo( pBusinessName, nodeInfos, isBusinessExistNode ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "get node info failed:business=%s,rc=%d", 
                 pBusinessName, rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      {
         BSONObj taskInfo ;
         BSONArray resultInfo ;
         rc = _generateTaskInfo( pBusinessName, nodeInfos, taskInfo,
                                 resultInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "generate task info failed:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
            goto error ;
         }

         getMaxTaskID( taskID ) ;
         taskID++ ;

         rc = createTask( OM_TASK_TYPE_ADD_BUSINESS, taskID, 
                          getTaskTypeStr( OM_TASK_TYPE_ADD_BUSINESS ), 
                          _localAgentHost, _localAgentService,
                          taskInfo, resultInfo ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "fail to _saveTask:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
            goto error ;
         }
      }

      rc = _notifyAgentTask( taskID ) ;
      if ( SDB_OK != rc )
      {
         removeTask( taskID ) ;
         PD_LOG( PDERROR, "fail to _notifyAgentTask:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      {
         BSONObj result = BSON( OM_BSON_TASKID << (long long)taskID ) ;
         _restAdaptor->appendHttpBody( _restSession, result.objdata(), 
                                       result.objsize(), 1 ) ;
         _sendOKRes2Web() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   omQueryHostStatusCommand::omQueryHostStatusCommand( 
                                                   restAdaptor *pRestAdaptor, 
                                                   pmdRestSession *pRestSession,
                                                   string localAgentHost, 
                                                   string localAgentService )
                            :omStartBusinessCommand( pRestAdaptor, pRestSession, 
                                                    localAgentHost, 
                                                    localAgentService )
   {
   }

   omQueryHostStatusCommand::~omQueryHostStatusCommand()
   {
   }

   INT32 omQueryHostStatusCommand::_getRestHostList( 
                                                    list<string> &hostNameList )
   {
      INT32 rc = SDB_OK ;
      const CHAR *pHostInfo = NULL ;
      BSONObj hostInfo ;
      BSONObj hostInfoArray ;
      _restAdaptor->getQuery( _restSession, OM_REST_FIELD_HOST_INFO, 
                              &pHostInfo ) ;
      if ( NULL == pHostInfo )
      {
         _errorDetail = "rest field:" + string( OM_REST_FIELD_HOST_INFO )
                        + " is null" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      rc = fromjson( pHostInfo, hostInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                     "rc=%d", pHostInfo, rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      hostInfoArray = hostInfo.getObjectField( OM_BSON_FIELD_HOST_INFO ) ;
      {
         BSONObjIterator iter( hostInfoArray ) ;
         while ( iter.more() )
         {
            BSONElement ele = iter.next() ;
            if ( ele.type() != Object )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "host info is invalid, element of %s "
                           "is not Object type:type=%d", 
                           OM_BSON_FIELD_HOST_INFO, ele.type() ) ;
               _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               goto error ;
            }
            BSONObj oneHost = ele.embeddedObject() ;
            hostNameList.push_back( 
                           oneHost.getStringField( OM_BSON_FIELD_HOST_NAME ) ) ;
         }
      }

      if ( hostNameList.size() == 0 )
      {
         _errorDetail = "rest field:" + string( OM_REST_FIELD_HOST_INFO )
                        + " is invalid" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s:rest=%s", _errorDetail.c_str(), pHostInfo ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omQueryHostStatusCommand::_verifyHostInfo( 
                                           list<string> &hostNameList, 
                                           list<fullHostInfo> &hostInfoList )
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;

      BSONArrayBuilder arrayBuilder ;
      list<string>::iterator iterList = hostNameList.begin() ;
      while ( iterList != hostNameList.end() )
      {
         BSONObj tmp = BSON( OM_HOST_FIELD_NAME << *iterList ) ;
         arrayBuilder.append( tmp ) ;
         iterList++ ;
      }
      matcher = BSON( "$or" << arrayBuilder.arr() ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_HOST ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_HOST ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         BSONObj diskArray ;
         fullHostInfo hostInfo ;
         hostInfo.hostName  = result.getStringField( OM_HOST_FIELD_NAME ) ;
         hostInfo.user      = result.getStringField( OM_HOST_FIELD_USER ) ;
         hostInfo.passwd    = result.getStringField( OM_HOST_FIELD_PASSWORD ) ;
         hostInfo.agentPort = result.getStringField( OM_HOST_FIELD_AGENT_PORT ) ;
         diskArray = result.getObjectField( OM_HOST_FIELD_DISK ) ;
         {
            BSONObjIterator iter( diskArray ) ;
            while ( iter.more() )
            {
               BSONElement ele = iter.next() ;
               if ( ele.type() != Object )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG_MSG( PDERROR, "disk info is invalid, element of %s "
                              "is not Object type:type=%d", 
                              OM_HOST_FIELD_DISK, ele.type() ) ;
                  _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
                  goto error ;
               }
               BSONObj oneDisk = ele.embeddedObject() ;
               simpleDiskInfo diskInfo ;
               diskInfo.diskName  = oneDisk.getStringField( 
                                                    OM_HOST_FIELD_DISK_NAME ) ;
               diskInfo.mountPath = oneDisk.getStringField( 
                                                    OM_HOST_FIELD_DISK_MOUNT ) ;
               hostInfo.diskInfo.push_back( diskInfo ) ;
            }
         }

         BSONObj netArray ;
         netArray = result.getObjectField( OM_HOST_FIELD_NET ) ;
         {
            BSONObjIterator iter( netArray ) ;
            while ( iter.more() )
            {
               BSONElement ele = iter.next() ;
               if ( ele.type() != Object )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG_MSG( PDERROR, "net info is invalid, element of %s "
                              "is not Object type:type=%d", 
                              OM_HOST_FIELD_NET, ele.type() ) ;
                  _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
                  goto error ;
               }
               BSONObj oneNet = ele.embeddedObject() ;
               simpleNetInfo netInfo ;
               netInfo.netName = oneNet.getStringField( 
                                                    OM_HOST_FIELD_NET_NAME ) ;
               netInfo.ip = oneNet.getStringField( 
                                                    OM_HOST_FIELD_NET_IP ) ;
               hostInfo.netInfo.push_back( netInfo ) ;
            }
         }

         hostInfoList.push_back( hostInfo ) ;
      }

      if ( hostInfoList.size() == 0 )
      {
         rc = SDB_INVALIDARG ;
         _errorDetail = "host is not found" ;
         PD_LOG( PDERROR, "%s:rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omQueryHostStatusCommand::_addQueryHostStatusReq( omManager *om,
                                            pmdRemoteSession *remoteSession,
                                            list<fullHostInfo> &hostInfoList ) 
   {
      INT32 rc = SDB_OK ;
      list<fullHostInfo>::iterator iter = hostInfoList.begin() ;
      while ( iter != hostInfoList.end() )
      {
         MsgRouteID routeID ;
         BSONArrayBuilder arrayBuilder ;
         BSONArrayBuilder arrayNetBuilder ;
         BSONObj bsonRequest ;
         pmdSubSession *subSession   = NULL ;
         CHAR *pContent              = NULL ;
         INT32 contentSize           = 0 ;
         fullHostInfo *pHostInfo     = &(*iter) ;
         routeID   = om->updateAgentInfo( pHostInfo->hostName, 
                                          pHostInfo->agentPort ) ;
         subSession = remoteSession->addSubSession( routeID.value ) ;
         if ( NULL == subSession )
         {
            rc = SDB_OOM ;
            PD_LOG( PDERROR, "addSubSessin failed" ) ;
            goto error ;
         }

         list<simpleDiskInfo>::iterator diskIter = pHostInfo->diskInfo.begin() ;
         while ( diskIter != pHostInfo->diskInfo.end() ) 
         {
            BSONObj tmp = BSON( OM_BSON_FIELD_DISK_NAME << diskIter->diskName
                                << OM_BSON_FIELD_DISK_MOUNT 
                                << diskIter->mountPath ) ;
            arrayBuilder.append( tmp ) ;
            diskIter++ ;
         }

         list<simpleNetInfo>::iterator netIter = pHostInfo->netInfo.begin() ;
         while ( netIter != pHostInfo->netInfo.end() ) 
         {
            BSONObj tmp = BSON( OM_HOST_FIELD_NET_NAME << netIter->netName
                                << OM_HOST_FIELD_NET_IP 
                                << netIter->ip ) ;
            arrayNetBuilder.append( tmp ) ;
            netIter++ ;
         }

         bsonRequest = BSON( OM_BSON_FIELD_HOST_NAME << pHostInfo->hostName 
                             << OM_BSON_FIELD_HOST_USER << pHostInfo->user 
                             << OM_BSON_FIELD_HOST_PASSWD 
                             << pHostInfo->passwd 
                             << OM_BSON_FIELD_DISK << arrayBuilder.arr()
                             << OM_BSON_FIELD_NET << arrayNetBuilder.arr() ) ;
         rc = msgBuildQueryMsg( &pContent, &contentSize, 
                                CMD_ADMIN_PREFIX OM_QUERY_HOST_STATUS_REQ,
                                0, 0, 0, -1, &bsonRequest, NULL, NULL, NULL ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "msgBuildQueryMsg failed:rc=%d", rc ) ;
            goto error ;
         }

         subSession->setReqMsg( (MsgHeader *)pContent ) ;
         iter++;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omQueryHostStatusCommand::_appendErrorResult( 
                                                BSONArrayBuilder &arrayBuilder, 
                                                const string &host, INT32 err,
                                                const string &detail )
   {
      BSONObj obj = BSON( OM_BSON_FIELD_HOST_NAME << host 
                          << OM_REST_RES_RETCODE << err 
                          << OM_REST_RES_DETAIL << detail ) ;
      arrayBuilder.append( obj ) ;
   }

   INT32 omQueryHostStatusCommand::_getHostStatus( 
                                             list<fullHostInfo> &hostInfoList, 
                                             BSONObj &bsonStatus )
   {
      INT32 rc      = SDB_OK ;
      omManager *om = NULL ;
      BSONArrayBuilder arrayBuilder ;
      pmdRemoteSession *remoteSession = NULL ;
      VEC_SUB_SESSIONPTR subSessionVec ;
      INT32 sucNum   = 0 ; 
      INT32 totalNum = 0 ;

      om            = sdbGetOMManager() ;
      remoteSession = om->getRSManager()->addSession( _cb, 
                                                      OM_CHECK_HOST_INTERVAL,
                                                      NULL ) ;
      if ( NULL == remoteSession )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "addSession failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }
      rc = _addQueryHostStatusReq( om, remoteSession, hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "_addQueryHostStatusReq failed:rc=%d", rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      remoteSession->sendMsg( &sucNum, &totalNum ) ;
      rc = _getAllReplay( remoteSession, &subSessionVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "wait replay failed:rc=%d", rc ) ;
         goto error ;
      }

      for ( UINT32 i = 0 ; i < subSessionVec.size() ; i++ )
      {
         vector<BSONObj> objVec ;
         SINT32 flag               = SDB_OK ;
         SINT64 contextID          = -1 ;
         SINT32 startFrom          = 0 ;
         SINT32 numReturned        = 0 ;
         MsgHeader* pRspMsg        = NULL ;
         pmdSubSession *subSession = subSessionVec[i] ;

         string tmpHost = "" ;
         string tmpService ;
         om->getHostInfoByID( subSession->getNodeID(), tmpHost, tmpService ) ;
         if ( subSession->isDisconnect() )
         {
            rc = SDB_NETWORK ;
            PD_LOG_MSG( PDERROR, "session disconnected:id=%s,rc=%d", 
                        routeID2String(subSession->getNodeID()).c_str(), rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _appendErrorResult( arrayBuilder, tmpHost, rc, _errorDetail ) ;
            continue ;
         }

         pRspMsg = subSession->getRspMsg() ;
         if ( NULL == pRspMsg )
         {
            rc = SDB_UNEXPECTED_RESULT ;
            PD_LOG_MSG( PDERROR, "unexpected result:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _appendErrorResult( arrayBuilder, tmpHost, rc, _errorDetail ) ;
            continue ;
         }

         rc = msgExtractReply( (CHAR *)pRspMsg, &flag, &contextID, &startFrom, 
                               &numReturned, objVec ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "extract reply failed:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _appendErrorResult( arrayBuilder, tmpHost, rc, _errorDetail ) ;
            continue ;
         }

         if ( SDB_OK != flag )
         {
            rc = flag ;
            if ( objVec.size() > 0 )
            {
               _errorDetail = objVec[0].getStringField( OM_REST_RES_DETAIL ) ;
            }
            _appendErrorResult( arrayBuilder, tmpHost, rc, _errorDetail ) ;
            PD_LOG( PDERROR, "agent process failed:detail=%s,rc=%d", 
                    _errorDetail.c_str(), rc ) ;
            continue ;
         }

         if ( 1 != objVec.size() )
         {
            rc = SDB_UNEXPECTED_RESULT ;
            PD_LOG_MSG( PDERROR, "unexpected response size:rc=%d", rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            _appendErrorResult( arrayBuilder, tmpHost, rc, _errorDetail ) ;
            continue ;
         }

         BSONObj result = objVec[0] ;
         _formatHostStatus( result ) ;
         arrayBuilder.append( result ) ;
      }

      rc = SDB_OK ;
      bsonStatus = BSON( OM_BSON_FIELD_HOST_INFO << arrayBuilder.arr() ) ;
   done:
      _clearSession( om, remoteSession ) ;
      return rc ;
   error:
      goto done ;
   }

   /*
      {
         "Name":"eth1","RXBytes":15686963,"RXPackets":192255,"RXErrors":0,
         "RXDrops":0,"TXBytes":4064080,"TXPackets":42780,"TXErrors":0,
         "TXDrops":0
      }
   */
   void omQueryHostStatusCommand::_formatHostStatusOneNet( BSONObj &oneNet )
   {
      BSONElement rxbyteEle  = oneNet.getField( OM_BSON_FIELD_NET_RXBYTES ) ;
      BSONElement rxpackEle  = oneNet.getField( OM_BSON_FIELD_NET_RXPACKETS ) ;
      BSONElement rxerrorEle = oneNet.getField( OM_BSON_FIELD_NET_RXERRORS ) ;
      BSONElement rxdropEle  = oneNet.getField( OM_BSON_FIELD_NET_RXDROPS ) ;
      if ( !rxbyteEle.isNumber() || !rxpackEle.isNumber() 
           || !rxerrorEle.isNumber() || !rxdropEle.isNumber() )
      {
         return ;
      }

      BSONObj newRxByte ;
      _seperateMegaBitValue( newRxByte, rxbyteEle.numberLong() ) ;

      BSONObj newRxPack ;
      _seperateMegaBitValue( newRxPack, rxpackEle.numberLong() ) ;

      BSONObj newRxErr ;
      _seperateMegaBitValue( newRxErr, rxerrorEle.numberLong() ) ;

      BSONObj newRxDrop ;
      _seperateMegaBitValue( newRxDrop, rxdropEle.numberLong() ) ;

      BSONElement txbyteEle  = oneNet.getField( OM_BSON_FIELD_NET_TXBYTES ) ;
      BSONElement txpackEle  = oneNet.getField( OM_BSON_FIELD_NET_TXPACKETS ) ;
      BSONElement txerrorEle = oneNet.getField( OM_BSON_FIELD_NET_TXERRORS ) ;
      BSONElement txdropEle  = oneNet.getField( OM_BSON_FIELD_NET_TXDROPS ) ;
      if ( !txbyteEle.isNumber() || !txpackEle.isNumber() 
           || !txerrorEle.isNumber() || !txdropEle.isNumber() )
      {
         return ;
      }

      BSONObj newTxByte ;
      _seperateMegaBitValue( newTxByte, txbyteEle.numberLong() ) ;

      BSONObj newTxPack ;
      _seperateMegaBitValue( newTxPack, txpackEle.numberLong() ) ;

      BSONObj newTxErr ;
      _seperateMegaBitValue( newTxErr, txerrorEle.numberLong() ) ;

      BSONObj newTxDrop ;
      _seperateMegaBitValue( newTxDrop, txdropEle.numberLong() ) ;

      oneNet = BSON( OM_BSON_FIELD_NET_NAME 
                     << oneNet.getStringField( OM_BSON_FIELD_NET_NAME )
                     << OM_BSON_FIELD_NET_RXBYTES << newRxByte
                     << OM_BSON_FIELD_NET_RXPACKETS << newRxPack 
                     << OM_BSON_FIELD_NET_RXERRORS << newRxErr 
                     << OM_BSON_FIELD_NET_RXDROPS << newRxDrop
                     << OM_BSON_FIELD_NET_TXBYTES << newTxByte
                     << OM_BSON_FIELD_NET_TXPACKETS << newTxPack 
                     << OM_BSON_FIELD_NET_TXERRORS << newTxErr 
                     << OM_BSON_FIELD_NET_TXDROPS << newTxDrop ) ;
   }

   /*
      {
         "Net":[
            {
               "Name":"eth1","RXBytes":15686963,"RXPackets":192255,"RXErrors":0,
               "RXDrops":0,"TXBytes":4064080,"TXPackets":42780,"TXErrors":0,
               "TXDrops":0
            }
         ]
      }
   */
   void omQueryHostStatusCommand::_formatHostStatusNet( BSONObj &net )
   {
      BSONElement netEle = net.getField( OM_BSON_FIELD_NET ) ;
      if ( Array != netEle.type() )
      {
         return ;
      }

      BSONArrayBuilder arrayBuilder ;

      BSONObj nets = net.getObjectField( OM_BSON_FIELD_NET ) ;
      BSONObjIterator iter( nets ) ;
      while ( iter.more() )
      {
         BSONElement ele = iter.next() ;
         if ( ele.type() != Object )
         {
            return ;
         }

         BSONObj oneNet = ele.embeddedObject() ;
         _formatHostStatusOneNet( oneNet ) ;

         arrayBuilder.append( oneNet ) ;
      }

      net = BSON( OM_BSON_FIELD_NET << arrayBuilder.arr() ) ;
   }

   void omQueryHostStatusCommand::_seperateMegaBitValue( BSONObj &obj, 
                                                         long value )
   {
      INT32 megabit = ( INT32 ) ( value/OM_SIZE_MEGABIT ) ;
      INT32 unit    = ( INT32 ) ( value%OM_SIZE_MEGABIT ) ;
      obj = BSON( OM_BSON_FIELD_CPU_MEGABIT << megabit 
                  << OM_BSON_FIELD_CPU_UNIT  << unit ) ;
   }

   /*
      {
         CPU: { "Sys":349550,"Idle":404327420,"Other":149190,"User":402630 }
      }
   */
   void omQueryHostStatusCommand::_formatHostStatusCPU( BSONObj &cpu )
   {
      BSONElement cpuEle = cpu.getField( OM_BSON_FIELD_CPU ) ;
      if ( Object != cpuEle.type() )
      {
         return ;
      }

      BSONObj cpuValue     = cpuEle.embeddedObject() ;

      BSONElement sysEle   = cpuValue.getField( OM_BSON_FIELD_CPU_SYS ) ;
      BSONElement idleEle  = cpuValue.getField( OM_BSON_FIELD_CPU_IDLE ) ;
      BSONElement otherEle = cpuValue.getField( OM_BSON_FIELD_CPU_OTHER ) ;
      BSONElement userEle  = cpuValue.getField( OM_BSON_FIELD_CPU_USER ) ;
      if ( !sysEle.isNumber() || !idleEle.isNumber() || !otherEle.isNumber()
           || !userEle.isNumber() )
      {
         return ;
      }

      BSONObj newSys ;
      _seperateMegaBitValue( newSys, sysEle.numberLong() ) ;

      BSONObj newIdle ;
      _seperateMegaBitValue( newIdle, idleEle.numberLong() ) ;

      BSONObj newOther ;
      _seperateMegaBitValue( newOther, otherEle.numberLong() ) ;

      BSONObj newUser ;
      _seperateMegaBitValue( newUser, userEle.numberLong() ) ;

      BSONObj cpuNewValue = BSON( OM_BSON_FIELD_CPU_SYS << newSys 
                                  << OM_BSON_FIELD_CPU_IDLE << newIdle
                                  << OM_BSON_FIELD_CPU_OTHER << newOther
                                  << OM_BSON_FIELD_CPU_USER << newUser ) ;

      cpu = BSON( OM_BSON_FIELD_CPU << cpuNewValue ) ;
   }

   void omQueryHostStatusCommand::_formatHostStatus( BSONObj &status )
   {
      BSONObj filterCPU = BSON( OM_BSON_FIELD_CPU << "" ) ;
      BSONObj cpu       = status.filterFieldsUndotted( filterCPU, true ) ;
      BSONObj others    = status.filterFieldsUndotted( filterCPU, false ) ;

      _formatHostStatusCPU( cpu ) ;

      BSONObj filterNet = BSON( OM_BSON_FIELD_NET << "" ) ;
      BSONObj net  = others.filterFieldsUndotted( filterNet, true ) ;
      BSONObj left = others.filterFieldsUndotted( filterNet, false ) ;

      /*
         {
           "Net": { 
                     "CalendarTime": 1413277808, 
                     "Net": [ 
                     { 
                        "Name": "lo", "RXBytes": 5125811, "RXPackets": 95615, 
                        "RXErrors": 0, "RXDrops": 0, "TXBytes": 5125811, 
                        "TXPackets": 95615, "TXErrors": 0, "TXDrops": 0 
                     } 
                     ]
                  }
         }
      */
      BSONElement netEle = net.getField( OM_BSON_FIELD_NET ) ;
      if ( Object != netEle.type() )
      {
         return ;
      }

      BSONObj netValue = netEle.embeddedObject() ;

      BSONObj innerNetOther = netValue.filterFieldsUndotted( filterNet, false ) ;
      BSONObj innerNet = netValue.filterFieldsUndotted( filterNet, true ) ;
      _formatHostStatusNet( innerNet ) ;

      BSONObjBuilder innerBuilder ;
      innerBuilder.appendElements( innerNetOther ) ;
      innerBuilder.appendElements( innerNet ) ;
      BSONObj newNet = BSON( OM_BSON_FIELD_NET << innerBuilder.obj() ) ;

      BSONObjBuilder builder ;
      builder.appendElements( cpu ) ;
      builder.appendElements( newNet ) ;
      builder.appendElements( left ) ;

      status = builder.obj() ;
   }

   INT32 omQueryHostStatusCommand::doCommand()
   {
      INT32 rc = SDB_OK ;
      list<string> hostNameList ;
      list<fullHostInfo> hostInfoList ;
      BSONObj bsonStatus ;
      BSONObj result ;
      rc = _getRestHostList( hostNameList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getRestHostList failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _verifyHostInfo( hostNameList, hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_verifyHostInfo failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _getHostStatus( hostInfoList, bsonStatus ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getHostStatus failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      _restAdaptor->appendHttpBody( _restSession, bsonStatus.objdata(), 
                                    bsonStatus.objsize(), 1 ) ;
      _sendOKRes2Web() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omPredictCapacity::omPredictCapacity( restAdaptor *pRestAdaptor, 
                                         pmdRestSession *pRestSession )
                     : omAuthCommand( pRestAdaptor, pRestSession )
   {
   }

   omPredictCapacity::~omPredictCapacity()
   {
   }

   INT32 omPredictCapacity::_getHostList( BSONObj &hostInfos, 
                                          list<string> &hostNameList )
   {
      INT32 rc = SDB_OK ;
      BSONObjIterator iter( hostInfos ) ;
      while ( iter.more() )
      {
         string hostName ;
         BSONElement oneEle  = iter.next() ;
         if ( oneEle.type() != Object )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "host info is invalid, element of host "
                        "is not Object type:type=%d", oneEle.type() ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }
         BSONObj oneHostName = oneEle.embeddedObject() ;
         BSONElement nameEle = oneHostName.getField( 
                                                OM_BSON_FIELD_HOST_NAME ) ;
         if ( nameEle.type() != String )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "bson field is not String:field=%s,type=%d",
                        OM_BSON_FIELD_HOST_NAME, nameEle.type() ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }
         hostNameList.push_back( nameEle.String() ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omPredictCapacity::_getTemplateValue( BSONObj &properties,  
                                               INT32 &replicaNum, 
                                               INT32 &groupNum )
   {
      INT32 rc              = SDB_OK ;
      BOOLEAN isRepSet      = FALSE ;
      BOOLEAN isGroupNumSet = FALSE ;
      BSONObjIterator iter( properties ) ;
      while ( iter.more() )
      {
         string name ;
         string value ;
         BSONElement oneEle  = iter.next() ;
         if ( oneEle.type() != Object )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "property info is invalid, element of property "
                        "is not Object type:type=%d", oneEle.type() ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }

         BSONObj oneProperty = oneEle.embeddedObject() ;
         BSONElement nameEle = oneProperty.getField( 
                                                OM_BSON_PROPERTY_NAME ) ;
         if ( nameEle.type() != String )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "bson field is not String:field=%s,type=%d",
                        OM_BSON_PROPERTY_NAME, nameEle.type() ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }
         name = nameEle.String() ;
         BSONElement valueEle = oneProperty.getField( 
                                                OM_BSON_PROPERTY_VALUE ) ;
         if ( valueEle.type() != String )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "bson field is not String:field=%s,type=%d",
                        OM_BSON_PROPERTY_VALUE, valueEle.type() ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }
         value = valueEle.String() ;

         if ( name == OM_TEMPLATE_REPLICA_NUM )
         {
            replicaNum = ossAtoi( value.c_str() ) ;
            isRepSet   = TRUE ;
         }
         else if ( name == OM_TEMPLATE_DATAGROUP_NUM )
         {
            groupNum      = ossAtoi( value.c_str() ) ;
            isGroupNumSet = TRUE ;
         }
      }

      if ( !isRepSet )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "bson miss template field:field=%s",
                     OM_TEMPLATE_REPLICA_NUM ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      if ( !isRepSet )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "bson miss template field:field=%s",
                     OM_TEMPLATE_DATAGROUP_NUM ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      if ( replicaNum <= 0 || groupNum <= 0 )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "replicaNum or groupNum is invalid:replicaNum=%d,"
                     "groupNum=%d", replicaNum, groupNum ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omPredictCapacity::_getRestInfo( list<string> &hostNameList, 
                                          string &clusterName, 
                                          INT32 &replicaNum, INT32 &groupNum )
   {
      INT32 rc = SDB_OK ;
      const CHAR *pTemplate = NULL ;
      BSONObj bsonTemplate ;
      _restAdaptor->getQuery( _restSession, OM_REST_TEMPLATE_INFO, 
                              &pTemplate ) ;
      if ( NULL == pTemplate )
      {
         _errorDetail = "rest field:" + string( OM_REST_TEMPLATE_INFO )
                        + " is null" ;
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "%s", _errorDetail.c_str() ) ;
         goto error ;
      }

      rc = fromjson( pTemplate, bsonTemplate ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "change rest field to BSONObj failed:src=%s,"
                     "rc=%d", pTemplate, rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      {
         BSONObj properties ;
         BSONElement ele = bsonTemplate.getField( OM_BSON_PROPERTY_ARRAY ) ;
         if ( ele.type() != Array )
         {  
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "bson field is not array:field=%s,type=%d",
                        OM_BSON_PROPERTY_ARRAY, ele.type() ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }

         properties = ele.embeddedObject() ;
         rc = _getTemplateValue( properties, replicaNum, groupNum ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "_getTemplateValue failed:rc=%d", rc ) ;
            goto error ;
         }
      }

      if ( bsonTemplate.hasField( OM_BSON_FIELD_HOST_INFO ) )
      {
         BSONObj hostInfos ;
         BSONElement ele = bsonTemplate.getField( OM_BSON_FIELD_HOST_INFO ) ;
         if ( ele.type() != Array )
         {  
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "bson field is not array:field=%s,type=%d",
                        OM_BSON_FIELD_HOST_INFO, ele.type() ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }
         hostInfos = ele.embeddedObject() ;
         rc = _getHostList( hostInfos, hostNameList ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "_getHostList failed:rc=%d", rc ) ;
            goto error ;
         }
      }

      {
         BSONElement ele ;
         ele = bsonTemplate.getField( OM_BSON_FIELD_CLUSTER_NAME ) ;
         if ( ele.type() != String )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "bson field is not String:field=%s,type=%d",
                        OM_BSON_FIELD_CLUSTER_NAME, ele.type() ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }

         clusterName = ele.String() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omPredictCapacity::_predictCapacity( 
                                           list<simpleHostDisk> &hostInfoList, 
                                           INT32 replicaNum, INT32 groupNum, 
                                           UINT64 &totalSize, UINT64 &validSize, 
                                           UINT32 &redundancyRate )
   {
      INT32 rc = SDB_OK ;
      redundancyRate = replicaNum - 1 ;

      UINT32 nodeCount         = replicaNum * groupNum ;
      UINT32 diskCount         = 0 ;
      UINT64 totalDiskFreeSize = 0 ;
      list<simpleHostDisk>::iterator iterHost = hostInfoList.begin() ;
      while ( iterHost != hostInfoList.end() )
      {
         simpleHostDisk *pHost = &( *iterHost ) ;
         list<simpleDiskInfo>::iterator iterDisk = pHost->diskInfo.begin() ;
         while ( iterDisk != pHost->diskInfo.end() )
         {
            simpleDiskInfo *pDisk = &( *iterDisk ) ;
            totalDiskFreeSize += pDisk->freeSize ;
            diskCount++ ;
            iterDisk++ ;
         }
         iterHost++ ;
      }

      if ( 0 == diskCount || 0 == totalDiskFreeSize )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "disk info error:diskCount=%u,diskFreeSize="
                     OSS_LL_PRINT_FORMAT, diskCount, totalDiskFreeSize ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      if ( nodeCount < diskCount )
      {
         totalSize = totalDiskFreeSize / diskCount * nodeCount ;
      }
      else
      {
         totalSize = totalDiskFreeSize ;
      }

      validSize = totalSize / replicaNum ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omPredictCapacity::doCommand()
   {
      INT32 rc = SDB_OK ;
      list<string> hostNameList ;
      string clusterName ;
      INT32 replicaNum ;
      INT32 groupNum ;
      list<simpleHostDisk> hostInfoList ;

      rc = _getRestInfo( hostNameList, clusterName, replicaNum, groupNum ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getRestInfo failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      rc = _fetchHostDiskInfo( clusterName, hostNameList, hostInfoList ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_getSpecificHostInfo failed:rc=%d", rc ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      if ( hostInfoList.size() == 0 )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "there is not host in the cluster:clusterName=%s",
                     clusterName.c_str() ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         _sendErrorRes2Web( rc, _errorDetail ) ;
         goto error ;
      }

      {
         BSONObj predict ;
         BSONObj result ;
         UINT64 totalSize      = 0 ;
         UINT64 validSize      = 0 ;
         UINT32 redundancyRate = 0 ;
         rc = _predictCapacity( hostInfoList, replicaNum, groupNum, totalSize,
                                validSize, redundancyRate ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "_predictCapacity failed:rc=%d", rc ) ;
            _sendErrorRes2Web( rc, _errorDetail ) ;
            goto error ;
         }

         predict = BSON( OM_BSON_FIELD_TOTAL_SIZE << (long long)totalSize
                         << OM_BSON_FIELD_VALID_SIZE << (long long)validSize
                         << OM_BSON_FIELD_REDUNDANCY_RATE << redundancyRate ) ;
         _restAdaptor->appendHttpBody( _restSession, predict.objdata(), 
                                       predict.objsize(), 1) ;
         _sendOKRes2Web() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   omGetFileCommand::omGetFileCommand( restAdaptor *pRestAdaptor, 
                                       pmdRestSession *pRestSession, 
                                       const CHAR *pRootPath,
                                       const CHAR *pSubPath )
   {
      _restAdaptor = pRestAdaptor ;
      _restSession = pRestSession ;
      _rootPath    = pRootPath ;
      _subPath     = pSubPath ;
   }

   omGetFileCommand::~omGetFileCommand()
   {
   }

   INT32 omGetFileCommand::doCommand() 
   {
      INT32 rc                      = SDB_OK ;
      CHAR *pContent                = NULL ;
      INT32 contentLength           = 0 ;
      restFileController* transfer = NULL ;
      string realSubPath            = _subPath ;

      transfer = restFileController::getTransferInstance() ;
      transfer->getTransferedPath( _subPath.c_str(), realSubPath ) ;

      rc = _getFileContent( _rootPath + realSubPath, &pContent, 
                            contentLength ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_FNE == rc )
         {
            PD_LOG( PDEVENT, "OM: file no found:%s, rc=%d", 
                    realSubPath.c_str(), rc ) ;
            _restAdaptor->sendResponse( _restSession, HTTP_NOTFOUND ) ;
         }
         else
         {
            PD_LOG( PDEVENT, "OM: open file failed:%s, rc=%d", 
                    realSubPath.c_str(), rc ) ;
            _restAdaptor->sendResponse( _restSession, HTTP_SERVICUNAVA ) ;
         }

         goto error ;
      }

      _restAdaptor->appendHttpBody( _restSession, pContent, contentLength ) ;
      _restAdaptor->sendResponse( _restSession, HTTP_OK ) ;

   done:
      if ( NULL != pContent )
      {
         _restSession->releaseBuff( pContent, contentLength ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omGetFileCommand::undoCommand()
   {
      return SDB_OK ;
   }

   INT32 omGetFileCommand::_getFileContent( string filePath, 
                                            CHAR **pFileContent, 
                                            INT32 &fileContentLen )
   {
      OSSFILE file ;
      INT32 rc               = SDB_OK ;
      INT64 fileSize         = 0 ;
      SINT64 realFileSize    = 0 ;
      INT32 buffSize         = 0 ;
      bool isFileOpened      = false ;
      bool isAllocBuff       = false ;

      rc = ossOpen( filePath.c_str(), OSS_READONLY, OSS_RWXU, file ) ;
      PD_RC_CHECK ( rc, PDERROR, 
                    "Failed to open file:file=%s, rc = %d", 
                    filePath.c_str(), rc ) ;

      isFileOpened = true ;
      rc           = ossGetFileSize( &file, &fileSize ) ;
      PD_RC_CHECK ( rc, PDERROR, 
                    "Failed to get file size:file=%s, rc = %d", 
                    filePath.c_str(), rc ) ;

      rc = _restSession->allocBuff( fileSize, pFileContent, buffSize ) ;
      PD_RC_CHECK ( rc, PDERROR, 
                    "Failed to alloc buff:buff_size=%I64d, rc = %d", 
                    fileSize, rc ) ;

      isAllocBuff = true ;
      rc          = ossRead( &file, *pFileContent, fileSize, &realFileSize ) ;
      PD_RC_CHECK ( rc, PDERROR, 
                    "Failed to read file content:file=%s, rc = %d", 
                    filePath.c_str(), rc ) ;

      fileContentLen = realFileSize ;

   done:
      if ( isFileOpened )
      {
         ossClose( file ) ;
      }

      return rc ;
   error:
      if ( isAllocBuff )
      {
         _restSession->releaseBuff( *pFileContent, buffSize ) ;
      }
      goto done ;
   }


   restFileController::restFileController()
   {
      INT32 pairCount   = 0 ;
      INT32 i           = 0 ;
      INT32 publicCount = 0 ;

      static char* filePathTable[][2] = {
         { OSS_FILE_SEP,    OSS_FILE_SEP OM_REST_INDEX_HTML }
      } ;

      static char *fileAuthorityPublic[] = {
         OSS_FILE_SEP OM_REST_LOGIN_HTML ,
      } ;

      pairCount = sizeof( filePathTable ) / ( 2 * sizeof ( char * ) ) ;

      for ( i = 0 ; i < pairCount ; i++ )
      {
         _transfer.insert( mapValueType( filePathTable[i][0], 
                                                    filePathTable[i][1])) ; 
      }

      publicCount = sizeof( fileAuthorityPublic ) / ( sizeof ( char * ) ) ;
      for ( i = 0 ; i < publicCount ; i++ )
      {
         _publicAccessFiles.insert( mapValueType( fileAuthorityPublic[i], 
                                                  fileAuthorityPublic[i])) ; 
      }
   }

   restFileController* restFileController::getTransferInstance()
   {
      static restFileController instance ;
      return &instance ;
   }

   INT32 restFileController::getTransferedPath( const char *src, 
                                                string &transfered )
   {
      INT32 rc = SDB_OK ;
      mapIteratorType iter = _transfer.find( src ) ;
      if ( iter == _transfer.end() )
      {
         goto error ;
      }

      transfered = iter->second ;

   done:
      return rc ; 
   error:
      rc = -1 ;
      goto done ;
   }

   bool restFileController::isFileAuthorPublic( const char *file ) 
   {
      mapIteratorType iter = _publicAccessFiles.find( file ) ;
      if ( iter == _publicAccessFiles.end() )
      {
         return false ;
      }

      return true ;
   }
}

