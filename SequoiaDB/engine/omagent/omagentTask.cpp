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

   Source File Name = omagentTask.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#include "ossTypes.h"
#include "omagentUtil.hpp"
#include "omagentTask.hpp"
#include "omagentJob.hpp"
#include "pmdDef.hpp"
#include "pmdEDU.hpp"
#include "omagentAsyncCmd.hpp"
#include "omagentMgr.hpp"

namespace engine
{

   /*
      LOCAL DEFINE
   */
   #define OMA_WAIT_OMSVC_RES_TIMEOUT       ( 1 * OSS_ONE_SEC )
   #define OMA_WAIT_SUB_TASK_NOTIFY_TIMEOUT ( 3 * OSS_ONE_SEC )
   #define ADD_HOST_MAX_THREAD_NUM          10

   
   /*
      add host task
   */
   _omaAddHostTask::_omaAddHostTask( INT64 taskID )
   : _omaTask( taskID )
   {
      _taskType = OMA_TASK_ADD_HOST ;
      _taskName = OMA_TASK_NAME_ADD_HOST ;
      _eventID  = 0 ;
      _progress = 0 ;
      _errno    = SDB_OK ;
      ossMemset( _detail, 0, OMA_BUFF_SIZE + 1 ) ;
   }

   _omaAddHostTask::~_omaAddHostTask()
   {
   }

   INT32 _omaAddHostTask::init( const BSONObj &info, void *ptr )
   {
      INT32 rc = SDB_OK ;

      _addHostRawInfo = info.copy() ;
      
      PD_LOG ( PDDEBUG, "Add host passes argument: %s",
               _addHostRawInfo.toString( FALSE, TRUE ).c_str() ) ;

      rc = _initAddHostInfo( _addHostRawInfo ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to init to get add host's info" ) ;
         goto error ;
      }
      _initAddHostResult() ;

      done:
         return rc ;
      error:
         goto done ;
   }

   INT32 _omaAddHostTask::doit()
   {
      INT32 rc = SDB_OK ;

      setTaskStatus( OMA_TASK_STATUS_RUNNING ) ;

      rc = _checkHostInfo() ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to check add host's informations, "
                      "rc = %d", rc ) ;
         goto error ;
      }
      rc = _addHost() ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to add host, rc = %d", rc ) ;
         goto error ;
      }
      
      rc = _waitAndUpdateProgress() ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to wait and update add host progress, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      
   done:
      setTaskStatus( OMA_TASK_STATUS_FINISH ) ;
      
      rc = _updateProgressToOM() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Failed to update add host progress"
                 "to omsvc, rc = %d", rc ) ;
      }
      sdbGetOMAgentMgr()->submitTaskInfo( _taskID ) ;
      
      PD_LOG( PDEVENT, "Omagent finish running add host task" ) ;
      
      return SDB_OK ;
   error:
      _setRetErr( rc ) ;
      goto done ;
   }

   BOOLEAN _omaAddHostTask::regSubTask( string subTaskName )
   {
      ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
      if ( OMA_TASK_STATUS_FAIL == _taskStatus )
      {
         return FALSE ;
      }
      setSubTaskStatus( subTaskName, OMA_TASK_STATUS_RUNNING ) ;
      return TRUE ;
   }

   AddHostInfo* _omaAddHostTask::getAddHostItem()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      vector<AddHostInfo>::iterator it = _addHostInfo.begin() ;
      for( ; it != _addHostInfo.end(); it++ )
      {
         if ( FALSE == it->_flag )
         {
            it->_flag = TRUE ;
            return &(*it) ;
         }
      }
      return NULL ;
   }

   INT32 _omaAddHostTask::updateProgressToTask( INT32 serialNum,
                                                AddHostResultInfo &resultInfo )
   {
      INT32 rc            = SDB_OK ;
      INT32 totalNum      = 0 ;
      INT32 finishNum     = 0 ;
      
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
 
      map<INT32, AddHostResultInfo>::iterator it ;
      it = _addHostResult.find( serialNum ) ;
      if ( it != _addHostResult.end() )
      {
         PD_LOG( PDDEBUG, "No.%d add host sub task update progress to local "
                 "add host task. ip[%s], hostName[%s], status[%d], "
                 "statusDesc[%s], errno[%d], detail[%s], flow num[%d]",
                 serialNum, resultInfo._ip.c_str(),
                 resultInfo._hostName.c_str(),
                 resultInfo._status,
                 resultInfo._statusDesc.c_str(),
                 resultInfo._errno,
                 resultInfo._detail.c_str(),
                 resultInfo._flow.size() ) ;
         it->second = resultInfo ;
      }
      
      totalNum = _addHostResult.size() ;
      if ( 0 == totalNum )
      {
         rc = SDB_SYS ;
         PD_LOG_MSG( PDERROR, "Add host result is empty" ) ;
         goto error ;
      }
      it = _addHostResult.begin() ;
      for( ; it != _addHostResult.end(); it++ )
      {
         if ( OMA_TASK_STATUS_FINISH == it->second._status )
            finishNum++ ;
      }
      _progress = ( finishNum * 100 ) / totalNum ;

      _eventID++ ;
      _taskEvent.signal() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   void _omaAddHostTask::notifyUpdateProgress()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _eventID++ ;
      _taskEvent.signal() ;
   }

   INT32 _omaAddHostTask::_initAddHostInfo( BSONObj &info )
   {
      INT32 rc                   = SDB_OK ;
      const CHAR *pSdbUser       = NULL ;
      const CHAR *pSdbPasswd     = NULL ;
      const CHAR *pSdbUserGroup  = NULL ;
      const CHAR *pInstallPacket = NULL ;
      const CHAR *pStr           = NULL ;
      BSONObj hostInfoObj ;
      BSONElement ele ;

      ele = info.getField( OMA_FIELD_TASKID ) ;
      if ( NumberInt != ele.type() && NumberLong != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive invalid task id from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
       }
      _taskID = ele.numberLong() ;

      rc = omaGetObjElement( info, OMA_FIELD_INFO, hostInfoObj ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_INFO, rc ) ;
      rc = omaGetStringElement( hostInfoObj, OMA_FIELD_SDBUSER, &pSdbUser ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_SDBUSER, rc ) ;
      rc = omaGetStringElement( hostInfoObj, OMA_FIELD_SDBPASSWD,
                                &pSdbPasswd ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_SDBPASSWD, rc ) ;
      rc = omaGetStringElement( hostInfoObj, OMA_FIELD_SDBUSERGROUP,
                                &pSdbUserGroup ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_SDBUSERGROUP, rc ) ;
      rc = omaGetStringElement( hostInfoObj, OMA_FIELD_INSTALLPACKET,
                                &pInstallPacket ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_INSTALLPACKET, rc ) ;
      ele = hostInfoObj.getField( OMA_FIELD_HOSTINFO ) ;
      if ( Array != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive wrong format add hosts"
                      "info from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
      {
         BSONObjIterator itr( ele.embeddedObject() ) ;
         INT32 serialNum = 0 ;
         while( itr.more() )
         {
            AddHostInfo hostInfo ;
            BSONObj item ;
            
            hostInfo._serialNum = serialNum++ ;
            hostInfo._flag      = FALSE ;
            hostInfo._taskID    = getTaskID() ;
            hostInfo._common._sdbUser = pSdbUser ;
            hostInfo._common._sdbPasswd = pSdbPasswd ;
            hostInfo._common._userGroup = pSdbUserGroup ;
            hostInfo._common._installPacket = pInstallPacket ;

            ele = itr.next() ;
            if ( Object != ele.type() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG ( PDERROR, "Receive wrong format bson from omsvc" ) ;
               goto error ;
            }
            item = ele.embeddedObject() ;
            rc = omaGetStringElement( item, OMA_FIELD_IP, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_IP, rc ) ;
            hostInfo._item._ip = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_HOSTNAME, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_HOSTNAME, rc ) ;
            hostInfo._item._hostName = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_USER, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_USER, rc ) ;
            hostInfo._item._user = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_PASSWD, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_PASSWD, rc ) ;
            hostInfo._item._passwd = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_SSHPORT, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_SSHPORT, rc ) ;
            hostInfo._item._sshPort = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_AGENTSERVICE, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_AGENTSERVICE, rc ) ;
            hostInfo._item._agentService = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_INSTALLPATH, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_INSTALLPATH, rc ) ;
            hostInfo._item._installPath = pStr ;

            _addHostInfo.push_back( hostInfo ) ;
         }
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }

   void _omaAddHostTask::_initAddHostResult()
   {
      vector<AddHostInfo>::iterator itr = _addHostInfo.begin() ;

      for( ; itr != _addHostInfo.end(); itr++ )
      {
         AddHostResultInfo result ;
         result._ip         = itr->_item._ip ;
         result._hostName   = itr->_item._hostName ;
         result._status     = OMA_TASK_STATUS_INIT ;
         result._statusDesc = "" ;
         result._errno      = SDB_OK ;
         result._detail     = "" ;
         
         _addHostResult.insert( std::pair< INT32, AddHostResultInfo >( 
            itr->_serialNum, result ) ) ;
      }
   }

   INT32 _omaAddHostTask::_checkHostInfo()
   {
      INT32 rc = SDB_OK ;
      INT32 errNum = SDB_OK ;
      const CHAR *pErrMsg = NULL ;
      BSONObj retObj ;
      _omaRunCheckAddHostInfo checkInfo ;

      rc = checkInfo.init( _addHostRawInfo.objdata() ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to check add host's raw information "
                  " rc = %d", rc ) ;
         goto error ;
      }
      rc = checkInfo.doit( retObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to do check add host's raw information "
                  " rc = %d", rc ) ;
         goto error ;
      }
      
      rc = omaGetIntElement ( retObj, OMA_FIELD_ERRNO, errNum ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                  "rc = %d", OMA_FIELD_ERRNO, rc ) ;
         goto error ;
      }
      if ( SDB_OK  != errNum )
      {
         rc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                     "rc = %d", OMA_FIELD_ERRNO, rc ) ;
            goto error ;
         }
         ossSnprintf( _detail, OMA_BUFF_SIZE, "%s", pErrMsg ) ;
         _errno = errNum ;
         rc = errNum ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHostTask::_addHost()
   {
      INT32 rc = SDB_OK ;
      INT32 threadNum = 0 ;
      INT32 hostNum = _addHostInfo.size() ;
      
      if ( 0 == hostNum )
      {
         PD_LOG_MSG ( PDERROR, "No add host's information" ) ;
         goto error ;
      }
      threadNum = hostNum < ADD_HOST_MAX_THREAD_NUM ? hostNum :
         ADD_HOST_MAX_THREAD_NUM ;
      for( INT32 i = 0; i < threadNum; i++ )
      { 
         ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
         if ( OMA_TASK_STATUS_RUNNING == _taskStatus )
         {
            rc = startOmagentJob( OMA_TASK_ADD_HOST_SUB, _taskID,
                                  BSONObj(), (void *)this ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to run add host sub task with the "
                        "type[%d], rc = %d", OMA_TASK_ADD_HOST_SUB, rc ) ;
               goto error ;
            }
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHostTask::_waitAndUpdateProgress()
   {
      INT32 rc = SDB_OK ;
      BOOLEAN flag = FALSE ;
      UINT64 subTaskEventID = 0 ;
      _pmdEDUCB *cb = pmdGetThreadEDUCB () ;

      while ( !cb->isInterrupted() )
      {
         if ( SDB_OK != _taskEvent.wait ( OMA_WAIT_SUB_TASK_NOTIFY_TIMEOUT ) )
         {
            continue ;
         }
         else
         {
            while( TRUE )
            {
               _taskLatch.get() ;
               _taskEvent.reset() ;
               flag = ( subTaskEventID < _eventID ) ? TRUE : FALSE ;
               subTaskEventID = _eventID ;
               _taskLatch.release() ;
               if ( TRUE == flag )
               {
                  rc = _updateProgressToOM() ;
                  if ( SDB_APP_INTERRUPT == rc )
                  {
                     PD_LOG( PDERROR, "Failed to update add host progress"
                             " to omsvc, rc = %d", rc ) ;
                     goto error ;
                  }
                  else if ( SDB_OK != rc )
                  {
                     PD_LOG( PDERROR, "Failed to update add host progress"
                             " to omsvc, rc = %d", rc ) ;
                  }
               }
               else
               {
                  break ;
               }
            }
            if ( _isTaskFinish() )
            {
               PD_LOG( PDEVENT, "All the add host sub tasks had finished" ) ;
               goto done ;
            }
            
         }
      }

      PD_LOG( PDERROR, "Receive interrupt when running add host task" ) ;
      rc = SDB_APP_INTERRUPT ;
    
   done:
      return rc ;
   error:
      goto done ; 
   }

   void _omaAddHostTask::_buildUpdateTaskObj( BSONObj &retObj )
   {
      
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      map<INT32, AddHostResultInfo>::iterator it = _addHostResult.begin() ;
      for ( ; it != _addHostResult.end(); it++ )
      {
         BSONObjBuilder builder ;
         BSONArrayBuilder arrBuilder ;
         BSONObj obj ;

         vector<string>::iterator itr = it->second._flow.begin() ;
         for ( ; itr != it->second._flow.end(); itr++ )
            arrBuilder.append( *itr ) ;
         
         builder.append( OMA_FIELD_IP, it->second._ip ) ;
         builder.append( OMA_FIELD_HOSTNAME, it->second._hostName ) ;
         builder.append( OMA_FIELD_STATUS, it->second._status ) ;
         builder.append( OMA_FIELD_STATUSDESC, it->second._statusDesc ) ;
         builder.append( OMA_FIELD_ERRNO, it->second._errno ) ;
         builder.append( OMA_FIELD_DETAIL, it->second._detail ) ;
         builder.append( OMA_FIELD_FLOW, arrBuilder.arr() ) ;
         obj = builder.obj() ;
         bab.append( obj ) ;
      }

      bob.appendNumber( OMA_FIELD_TASKID, _taskID ) ;
      bob.appendNumber( OMA_FIELD_ERRNO, _errno ) ;
      bob.append( OMA_FIELD_DETAIL, _detail ) ;
      bob.appendNumber( OMA_FIELD_STATUS, _taskStatus ) ;
      bob.append( OMA_FIELD_STATUSDESC, getTaskStatusDesc( _taskStatus ) ) ;
      bob.appendNumber( OMA_FIELD_PROGRESS, _progress ) ;
      bob.appendArray( OMA_FIELD_RESULTINFO, bab.arr() ) ;

      retObj = bob.obj() ;
   }

   INT32 _omaAddHostTask::_updateProgressToOM()
   {
      INT32 rc            = SDB_OK ;
      INT32 retRc         = SDB_OK ;
      UINT64 reqID        = 0 ;
      omAgentMgr *pOmaMgr = sdbGetOMAgentMgr() ;
      _pmdEDUCB *cb       = pmdGetThreadEDUCB () ;
      ossAutoEvent updateEvent ;
      BSONObj obj ;
      
      _buildUpdateTaskObj( obj ) ;

      reqID = pOmaMgr->getRequestID() ;
      pOmaMgr->registerTaskEvent( reqID, &updateEvent ) ;
      
      while( !cb->isInterrupted() )
      {
         pOmaMgr->sendUpdateTaskReq( reqID, &obj ) ;
         while ( !cb->isInterrupted() )
         {
            if ( SDB_OK != updateEvent.wait( OMA_WAIT_OMSVC_RES_TIMEOUT, &retRc ) )
            {
               continue ;
            }
            else
            {
               if ( SDB_OM_TASK_NOT_EXIST == retRc )
               {
                  PD_LOG( PDERROR, "Failed to update task[%s]'s progress "
                          "with requestID[%lld], rc = %d",
                          _taskName.c_str(), reqID, retRc ) ;
                  pOmaMgr->unregisterTaskEvent( reqID ) ;
                  rc = retRc ;
                  goto error ;
               }
               else if ( SDB_OK != retRc )
               {
                  PD_LOG( PDWARNING, "Retry to update task[%s]'s progress "
                          "with requestID[%lld], rc = %d",
                          _taskName.c_str(), reqID, retRc ) ;
                  break ;
               }
               else
               {
                  PD_LOG( PDDEBUG, "Success to update task[%s]'s progress "
                          "with requestID[%lld]", _taskName.c_str(), reqID ) ;
                  pOmaMgr->unregisterTaskEvent( reqID ) ;
                  goto done ;
               }
            }
         }
      }

      PD_LOG( PDERROR, "Receive interrupt when update add host task "
              "progress to omsvc" ) ;
      rc = SDB_APP_INTERRUPT ;
      
   done:
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN _omaAddHostTask::_isTaskFinish()
   {
      INT32 runNum    = 0 ;
      INT32 finishNum = 0 ;
      INT32 failNum   = 0 ;
      INT32 otherNum  = 0 ;
      BOOLEAN flag    = TRUE ;
      ossScopedLock lock( &_latch, EXCLUSIVE ) ;
      
      map< string, OMA_TASK_STATUS >::iterator it = _subTaskStatus.begin() ;
      for ( ; it != _subTaskStatus.end(); it++ )
      {
         switch ( it->second )
         {
         case OMA_TASK_STATUS_FINISH :
            finishNum++ ;
            break ;
         case OMA_TASK_STATUS_FAIL :            
            failNum++ ;
            break ;
         case OMA_TASK_STATUS_RUNNING :
            runNum++ ;
            flag = FALSE ;
            break ;
         default :
            otherNum++ ;
            flag = FALSE ;
            break ;
         }
      }
      PD_LOG( PDDEBUG, "In add host task, the amount of sub tasks is [%d]: "
              "[%d]running, [%d]finish, [%d]in the other status",
              _subTaskStatus.size(), runNum, finishNum, otherNum ) ;

      return flag ;
   }

   void _omaAddHostTask::_setRetErr( INT32 errNum )
   {
      const CHAR *pDetail = NULL ;

      if ( SDB_OK != _errno && '\0' != _detail[0] )
      {
         return ;
      }
      else
      {
         _errno = errNum ;
         pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         if ( NULL != pDetail && 0 != *pDetail )
         {
            ossMemcpy( _detail, pDetail, OMA_BUFF_SIZE ) ;
         }
         else
         {
            pDetail = getErrDesp( errNum ) ;
            if ( NULL != pDetail )
               ossMemcpy( _detail, pDetail, OMA_BUFF_SIZE ) ;
            else
               PD_LOG( PDERROR, "Failed to get error message" ) ;
         }
      }
   }


   /*
      install db business task
   */
   _omaInstDBBusTask::_omaInstDBBusTask( INT64 taskID )
   : _omaTask( taskID )
   {
      _taskType      = OMA_TASK_INSTALL_DB ;
      _taskName      = OMA_TASK_NAME_INSTALL_DB_BUSINESS ;
      _isStandalone  = FALSE ;
      _nodeSerialNum = 0 ;
      _eventID       = 0 ;
      _progress      = 0 ;
      _errno         = SDB_OK ;
      ossMemset( _detail, 0, OMA_BUFF_SIZE + 1 ) ;
   }

   _omaInstDBBusTask::~_omaInstDBBusTask()
   {
   }

   INT32 _omaInstDBBusTask::init( const BSONObj &info, void *ptr )
   {
      INT32 rc = SDB_OK ;

      _instDBBusRawInfo = info.copy() ;
      PD_LOG ( PDDEBUG, "Install db business passes argument: %s",
               _instDBBusRawInfo.toString( FALSE, TRUE ).c_str() ) ;

      rc = _initInstInfo( _instDBBusRawInfo ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to init to get install db business info "
                 "rc = %d", rc ) ;
         goto error ;
      }
      rc = _restoreResultInfo() ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to restore install db business result info "
                 "rc = %d", rc ) ;
         goto error ;
      }

      done:
         return rc ;
      error:
         goto done ;
   }

   INT32 _omaInstDBBusTask::doit()
   {
      INT32 rc                    = SDB_OK ;

      setTaskStatus( OMA_TASK_STATUS_RUNNING ) ;

      if ( _isStandalone )
      {
         rc = _installStandalone() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to install standalone, rc = %d", rc ) ;
            goto error ;
         }
      }
      else // in case of cluster
      {
         rc = _installTmpCoord() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to install temporary coord, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to create catalog, "
                    "rc = %d", rc ) ;
            goto error ;
         }
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to create coord, "
                    "rc = %d", rc ) ;
            goto error ;
         }
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to create data nodes, "
                    "rc = %d", rc ) ;
            goto error ;
         }

         rc = _waitAndUpdateProgress() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to wait and update install db "
                     "business progress, rc = %d", rc ) ;
            goto error ;
         }

      }
      
   done:
      setTaskStatus( OMA_TASK_STATUS_FINISH ) ;
      
      rc = _updateProgressToOM() ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Failed to update install db business progress"
                 "to omsvc, rc = %d", rc ) ;
      }
      sdbGetOMAgentMgr()->submitTaskInfo( _taskID ) ;
      
      PD_LOG( PDEVENT, "Omagent finish running install db business task" ) ;
      
      return SDB_OK ;
   error:
      setTaskStatus( OMA_TASK_STATUS_ROLLBACK ) ;
      _rollback() ;
      goto done ;
   }

   INT32 _omaInstDBBusTask::updateProgressToTask( INT32 serialNum,
                                                  InstDBResult &instResult,
                                                  BOOLEAN needToNotify )
   {
      INT32 rc            = SDB_OK ;
      INT32 totalNum      = 0 ;
      INT32 finishNum     = 0 ;
      vector<InstDBBusInfo>::iterator it ;
      map< string, vector<InstDBBusInfo> >::iterator it2 ;
      
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;

      PD_LOG( PDDEBUG, "Install db business update progress to local "
              "task: serialNum[%d], hostName[%s], svcName[%s], role[%s], "
              "groupName[%s], status[%d], statusDesc[%s], errno[%d], "
              "detail[%s], flow num[%d]",
              serialNum, instResult._hostName.c_str(),
              instResult._svcName.c_str(), instResult._role.c_str(),
              instResult._groupName.c_str(), instResult._status,
              instResult._statusDesc.c_str(), instResult._errno,
              instResult._detail.c_str(), instResult._flow.size() ) ;
 
      if ( TRUE == _isStandalone )
      {
         it = _standalone.begin() ;
         for ( ; it != _standalone.end(); it++ )
         {
            if ( serialNum == it->_nodeSerialNum )
            {
               it->_instResult = instResult ;
               break ;
            }
         }
      }
      else
      {
         if ( string(ROLE_DATA) == instResult._role )
         {
            it2 = _mapGroups.find( instResult._groupName ) ;
            if ( it2 != _mapGroups.end() )
            {
               it = it2->second.begin() ;
               for ( ; it != it2->second.end(); it++ )
               {
                  it->_instResult = instResult ;
                  break ;
               }
            }
         }
         else if ( string(ROLE_COORD) == instResult._role )
         {
            it = _coord.begin() ;
            for ( ; it != _coord.end(); it++ )
            {
               if ( serialNum == it->_nodeSerialNum )
               {
                  it->_instResult = instResult ;
                  break ;
               }
            }
         }
         else if ( string(ROLE_CATA) == instResult._role )
         {
            it = _catalog.begin() ;
            for ( ; it != _catalog.end(); it++ )
            {
               if ( serialNum == it->_nodeSerialNum )
               {
                  it->_instResult = instResult ;
                  break ;
               }
            }
         }
         else
         {
            rc = SDB_SYS ;
            PD_LOG( PDWARNING, "Unknown role for updating progress when "
                    "installing node[%s:%s]",
                    instResult._hostName.c_str(),
                    instResult._svcName.c_str() ) ;
            goto error ;
         }
      }
      
      if ( TRUE == _isStandalone )
      {
         totalNum = _standalone.size() ;
         if ( 0 == totalNum )
         {
            rc = SDB_SYS ;
            PD_LOG_MSG( PDERROR, "Install standalone's info is empty" ) ;
            goto error ;
         }
         it = _standalone.begin() ;
         for( ; it != _standalone.end(); it++ )
         {
            if ( OMA_TASK_STATUS_FINISH == it->_instResult._status )
               finishNum++ ;
         }
         _progress = ( finishNum * 100 ) / totalNum ;
      }
      else
      {
         totalNum = _catalog.size() + _coord.size() ;
         it2 = _mapGroups.begin() ;
         for ( ; it2 != _mapGroups.end(); it2++ )
            totalNum += it2->second.size() ;
         it = _catalog.begin() ;
         for( ; it != _catalog.end(); it++ )
         {
            if ( OMA_TASK_STATUS_FINISH == it->_instResult._status )
               finishNum++ ;
         }
         it = _coord.begin() ;
         for( ; it != _coord.end(); it++ )
         {
            if ( OMA_TASK_STATUS_FINISH == it->_instResult._status )
               finishNum++ ;
         }
         it2 = _mapGroups.begin() ;
         for ( ; it2 != _mapGroups.end(); it2++ )
         {
            it = it2->second.begin() ;
            for( ; it != it2->second.end(); it++ )
            {
               if ( OMA_TASK_STATUS_FINISH == it->_instResult._status )
                  finishNum++ ;
            }
         }
         _progress = ( finishNum * 100 ) / totalNum ;         
      }

      if ( TRUE == needToNotify )
      {
         _eventID++ ;
         _taskEvent.signal() ;
      }
      else
      {
         rc = _updateProgressToOM() ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDWARNING, "Failed to update install db business progress"
                    "to omsvc, rc = %d", rc ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_initInstInfo( BSONObj &info )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      BSONObj hostInfoObj ;
      BSONObj filter ;
      BSONObj commonFileds ;
      BSONObjBuilder builder ;
      string deplayMod ;
      const CHAR *pStr = NULL ;

      ele = info.getField( OMA_FIELD_TASKID ) ;
      if ( NumberInt != ele.type() && NumberLong != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive invalid task id from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _taskID = ele.numberLong() ;
      ele = info.getField( OMA_FIELD_STATUS ) ;
      if ( NumberInt != ele.type() && NumberLong != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive invalid task status from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _taskStatus = (OMA_TASK_STATUS)ele.numberInt() ;

      rc = omaGetObjElement( info, OMA_FIELD_INFO, hostInfoObj ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_INFO, rc ) ;
      
      ele = hostInfoObj.getField( OMA_FIELD_DEPLOYMOD ) ;
      if ( String != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive invalid content from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      deplayMod = ele.String() ;
      if ( deplayMod == string(DEPLAY_SA) )
      {
         _isStandalone = TRUE ;
      }
      else if ( deplayMod == string(DEPLAY_DB) )
      {
         _isStandalone = FALSE ;
      }
      else
      {
         PD_LOG_MSG ( PDERROR, "Receive invalid deplay mode from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      
      rc = omaGetStringElement ( hostInfoObj, OMA_FIELD_SDBUSER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_SDBUSER, rc ) ;
      builder.append( OMA_FIELD_SDBUSER, pStr ) ;
      rc = omaGetStringElement ( hostInfoObj, OMA_FIELD_SDBPASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_SDBPASSWD, rc ) ;
      builder.append( OMA_FIELD_SDBPASSWD, pStr ) ;
      rc = omaGetStringElement ( hostInfoObj, OMA_FIELD_SDBUSERGROUP, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_SDBUSERGROUP, rc ) ;
      builder.append( OMA_FIELD_SDBUSERGROUP, pStr ) ;
      commonFileds = builder.obj() ;
      
      ele = hostInfoObj.getField ( OMA_FIELD_CONFIG ) ;
      if ( Array != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive wrong format install "
                      "db business info from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
      {
         BSONObjIterator itr( ele.embeddedObject() ) ;
         while ( itr.more() )
         {
            InstDBBusInfo instDBBusInfo ;
            BSONObjBuilder bob ;
            BSONObj hostInfo ;
            BSONObj temp ;
            const CHAR *pRole = NULL ;
            ele = itr.next() ;
            if ( Object != ele.type() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG ( PDERROR, "Receive wrong format bson from omsvc" ) ;
               goto error ;
            }
            temp = ele.embeddedObject() ;
            bob.appendElements( temp ) ;
            bob.appendElements( commonFileds ) ;
            hostInfo = bob.obj() ;
            rc = omaGetStringElement ( temp, OMA_OPTION_ROLE, &pRole ) ;
            if ( rc )
            {
               PD_LOG_MSG ( PDERROR, "Get field[%s] failed, rc = %d",
                            OMA_OPTION_ROLE, rc ) ;
               goto error ;
            }
            if ( 0 == ossStrncmp( pRole, ROLE_DATA,
                                  ossStrlen( ROLE_DATA ) ) )
            {
               string groupName = "" ;
               rc = omaGetStringElement( temp, OMA_FIELD_DATAGROUPNAME, &pStr ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                         "Get field[%s] failed, rc: %d",
                         OMA_FIELD_DATAGROUPNAME, rc ) ;
               groupName = string( pStr ) ;
               rc = _initInstAndResultInfo( hostInfo, instDBBusInfo ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                         "Failed to init install db business info and result, "
                         "rc: %d", rc ) ;
               _mapGroups[groupName].push_back( instDBBusInfo ) ;
            }
            else if ( 0 == ossStrncmp( pRole, ROLE_COORD,
                                       ossStrlen( ROLE_COORD ) ) )
            {
               rc = _initInstAndResultInfo( hostInfo, instDBBusInfo ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                         "Failed to init install db business info and result, "
                         "rc: %d", rc ) ;
               _coord.push_back( instDBBusInfo ) ;
            }
            else if ( 0 == ossStrncmp( pRole, ROLE_CATA,
                                       ossStrlen( ROLE_CATA ) ) )
            {
               rc = _initInstAndResultInfo( hostInfo, instDBBusInfo ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                         "Failed to init install db business info and result, "
                         "rc: %d", rc ) ;
               _catalog.push_back( instDBBusInfo ) ;
            }
            else if ( 0 == ossStrncmp( pRole, ROLE_STANDALONE,
                                       ossStrlen( ROLE_STANDALONE ) ) )
            {
               rc = _initInstAndResultInfo( hostInfo, instDBBusInfo ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                         "Failed to init install db business info and result, "
                         "rc: %d", rc ) ;
               _standalone.push_back( instDBBusInfo ) ;
            }
            else
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "Unknown role for install db business" ) ;
               goto error ;
            }
         }
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   INT32 _omaInstDBBusTask::_initInstAndResultInfo( BSONObj& hostInfo,
                                                    InstDBBusInfo &info )
   { 
      INT32 rc               = SDB_OK ; 
      const CHAR *pHostName  = NULL ;
      const CHAR *pSvcName   = NULL ;
      const CHAR *pGroupName = NULL ;
      const CHAR *pStr       = NULL ;
      BSONObj conf ;
      BSONObj pattern ;

      info._nodeSerialNum = _nodeSerialNum++ ;
      
      rc = omaGetStringElement( hostInfo, OMA_FIELD_HOSTNAME, &pHostName ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_HOSTNAME, rc ) ;
      info._instInfo._hostName = pHostName ;
      rc = omaGetStringElement( hostInfo, OMA_OPTION_SVCNAME, &pSvcName ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_OPTION_SVCNAME, rc ) ;
      info._instInfo._svcName = pSvcName ;
      rc = omaGetStringElement( hostInfo, OMA_OPTION_DBPATH, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_OPTION_DBPATH, rc ) ;
      info._instInfo._dbPath = pStr ;
      info._instInfo._confPath = "" ;
      rc = omaGetStringElement( hostInfo, OMA_OPTION_DATAGROUPNAME,
                                &pGroupName ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_OPTION_DATAGROUPNAME, rc ) ;
      info._instInfo._dataGroupName = pGroupName ;
      rc = omaGetStringElement( hostInfo, OMA_FIELD_SDBUSER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBUSER, rc ) ;
      info._instInfo._sdbUser = pStr ;
      rc = omaGetStringElement( hostInfo, OMA_FIELD_SDBPASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBPASSWD, rc ) ;
      info._instInfo._sdbPasswd = pStr ;
      rc = omaGetStringElement( hostInfo, OMA_FIELD_SDBUSERGROUP, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBUSERGROUP, rc ) ;
      info._instInfo._sdbUserGroup = pStr ;
      rc = omaGetStringElement( hostInfo, OMA_FIELD_USER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_USER, rc ) ;
      info._instInfo._user = pStr ;
      rc = omaGetStringElement( hostInfo, OMA_FIELD_PASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_PASSWD, rc ) ;
      info._instInfo._passwd = pStr ;
      rc = omaGetStringElement( hostInfo, OMA_FIELD_SSHPORT, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SSHPORT, rc ) ;
      info._instInfo._sshPort = pStr ;
      pattern = BSON( OMA_FIELD_HOSTNAME       << 1 <<
                      OMA_OPTION_SVCNAME       << 1 <<
                      OMA_OPTION_DBPATH        << 1 <<
                      OMA_OPTION_DATAGROUPNAME << 1 <<
                      OMA_FIELD_SDBUSER        << 1 <<
                      OMA_FIELD_SDBPASSWD      << 1 << 
                      OMA_FIELD_SDBUSERGROUP   << 1 <<
                      OMA_FIELD_USER           << 1 <<
                      OMA_FIELD_PASSWD         << 1 <<
                      OMA_FIELD_SSHPORT        << 1 ) ;
      conf = hostInfo.filterFieldsUndotted( pattern, false ) ;
      info._instInfo._conf = conf.copy() ;

      rc = omaGetStringElement( hostInfo, OMA_OPTION_ROLE, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_OPTION_ROLE, rc ) ;
      info._instResult._errno      = SDB_OK ;
      info._instResult._detail     = "" ;
      info._instResult._hostName   = pHostName ;
      info._instResult._svcName    = pSvcName ;
      info._instResult._role       = pStr ;
      info._instResult._groupName  = pGroupName ;
      info._instResult._status     = OMA_TASK_STATUS_INIT ;
      info._instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_INIT ) ;

   done:
      return rc ;
   error:
      goto done ;

   }

   INT32 _omaInstDBBusTask::_restoreResultInfo()
   {
      INT32 rc = SDB_OK ;
      vector<InstDBBusInfo>::iterator it ;
      map< string, vector<InstDBBusInfo> >::iterator it2 ;
      BSONElement ele ;
      BSONElement ele2 ;

      ele = _instDBBusRawInfo.getField ( OMA_FIELD_RESULTINFO ) ;
      if ( Array != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive wrong format install "
                      "db business info from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
      {
         BSONObjIterator itr( ele.embeddedObject() ) ;
         const CHAR *pStr       = NULL ;
         const CHAR *pRole      = NULL ;
         InstDBResult tempResult ;
         
         while ( itr.more() )
         {
            BSONObj resultInfo ;
            INT32 num = 0 ;
            ele = itr.next() ;
            if ( Object != ele.type() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG ( PDERROR, "Receive wrong format bson from omsvc" ) ;
               goto error ;
            }
            resultInfo = ele.embeddedObject() ;
            rc = omaGetIntElement( resultInfo, OMA_FIELD_ERRNO, num ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d", OMA_FIELD_ERRNO, rc ) ;
            tempResult._errno = num ;
            rc = omaGetStringElement( resultInfo, OMA_FIELD_DETAIL, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d", OMA_FIELD_DETAIL, rc ) ;
            tempResult._detail = pStr ;
            rc = omaGetStringElement( resultInfo, OMA_FIELD_HOSTNAME, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d", OMA_FIELD_HOSTNAME, rc ) ;
            tempResult._hostName = pStr ;
            rc = omaGetStringElement( resultInfo, OMA_FIELD_SVCNAME, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d", OMA_FIELD_SVCNAME, rc ) ;
            tempResult._svcName = pStr ;
            rc = omaGetStringElement( resultInfo, OMA_FIELD_ROLE, &pRole ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d", OMA_FIELD_ROLE, rc ) ;
            tempResult._role = pRole ;
            rc = omaGetStringElement( resultInfo, OMA_FIELD_DATAGROUPNAME, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d", OMA_FIELD_DATAGROUPNAME, rc ) ;
            tempResult._groupName = pStr ;
            rc = omaGetIntElement( resultInfo, OMA_FIELD_STATUS, num ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d", OMA_FIELD_STATUS, rc ) ;
            tempResult._status = num ;
            rc = omaGetStringElement( resultInfo, OMA_FIELD_STATUSDESC, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d", OMA_FIELD_STATUSDESC, rc ) ;
            tempResult._statusDesc = pStr ;
            ele2 = resultInfo.getField ( OMA_FIELD_FLOW ) ;
            if ( Array == ele2.type() )
            {
               BSONObjIterator itr( ele2.embeddedObject() ) ;
               while ( itr.more() )
               {
                  ele2 = itr.next() ;
                  string str = ele2.str() ;
                  tempResult._flow.push_back( str ) ;
               }
            }

            if ( 0 == ossStrncmp( pRole, ROLE_DATA, ossStrlen( ROLE_DATA ) ) )
            {
               it2 = _mapGroups.find( tempResult._groupName ) ;
               if ( it2 != _mapGroups.end() )
               {
                  it = it2->second.begin() ;
                  for ( ; it != it2->second.end(); it++ )
                  {
                     if ( ( it->_instInfo._hostName == tempResult._hostName ) &&
                          ( it->_instInfo._svcName == tempResult._svcName ) )
                     {
                        it->_instResult = tempResult ;
                     }
                  }
               }
            }
            else if ( 0 == ossStrncmp( pRole, ROLE_COORD,
                                       ossStrlen( ROLE_COORD ) ) )
            {
               it = _coord.begin() ;
               for ( ; it != _coord.end(); it++ )
               {
                  if ( ( it->_instInfo._hostName == tempResult._hostName ) &&
                       ( it->_instInfo._svcName == tempResult._svcName ) )
                  {
                     it->_instResult = tempResult ;
                  }
               }
            }
            else if ( 0 == ossStrncmp( pRole, ROLE_CATA,
                                       ossStrlen( ROLE_CATA ) ) )
            {
               it = _catalog.begin() ;
               for ( ; it != _catalog.end(); it++ )
               {
                  if ( ( it->_instInfo._hostName == tempResult._hostName ) &&
                       ( it->_instInfo._svcName == tempResult._svcName ) )
                  {
                     it->_instResult = tempResult ;
                  }
               }
            }
            else if ( 0 == ossStrncmp( pRole, ROLE_STANDALONE,
                                       ossStrlen( ROLE_STANDALONE ) ) )
            {
               it = _standalone.begin() ;
               for ( ; it != _standalone.end(); it++ )
               {
                  if ( ( it->_instInfo._hostName == tempResult._hostName ) &&
                       ( it->_instInfo._svcName == tempResult._svcName ) )
                  {
                     it->_instResult = tempResult ;
                  }
               }
            }
            else
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "Unknown role for install db business" ) ;
               goto error ;
            }
            
         }
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_saveTmpCoordInfo( BSONObj &info )
   {
      INT32 rc         = SDB_OK ;
      const CHAR *pStr = NULL ;
      rc = omaGetStringElement( info, OMA_FIELD_TMPCOORDSVCNAME, &pStr ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get filed[%s], rc = %s",
                  OMA_FIELD_TMPCOORDSVCNAME, rc ) ;
         goto error ;
      }
      _tmpCoordSvcName = pStr ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_installTmpCoord()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      const CHAR *pErrMsg = NULL ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      BSONObj retObj ;
      _omaCreateTmpCoord tmpCoord ;
      
      rc = tmpCoord.createTmpCoord( retObj ) ;
      if ( rc )
      {
         tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( tmpRc )
         {
            pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pErrMsg || 0 == *pErrMsg )
            {
               pErrMsg = "Not execute js file yet" ;
            }
         }
         ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to create temporary "
                      "coord: %s", pErrMsg ) ;
         PD_LOG_MSG( PDERROR, desc ) ;
         goto error ;
      }
      rc = _saveTmpCoordInfo( retObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to save installed temporary coord's info, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_installStandalone()
   {
      INT32 rc                     = SDB_OK ;
      INT32 tmpRc                  = SDB_OK ;
      CHAR flow[OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pDetail          = "" ;
      INT32 errNum                 = 0 ;
      vector<InstDBBusInfo>::iterator itr = _standalone.begin() ;

      for ( ; itr != _standalone.end(); itr++ )
      {
         BSONObj retObj ;
         InstDBResult instResult = itr->_instResult ;
         _omaInstallStandalone runCmd( _taskID, itr->_instInfo ) ;
         const CHAR *pHostName = itr->_instInfo._hostName.c_str() ;
         const CHAR *pSvcName  = itr->_instInfo._svcName.c_str() ;
/*
         instResult._hostName  = pHostName ;
         instResult._svcName   = pSvcName ;
*/

         ossSnprintf( flow, OMA_BUFF_SIZE, "Installing standalone[%s:%s]",
                      pHostName, pSvcName ) ;
         instResult._status = OMA_TASK_STATUS_RUNNING ;
         instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_RUNNING ) ;
         instResult._flow.push_back( flow ) ;
         updateProgressToTask( itr->_nodeSerialNum, instResult, FALSE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to update progress before install "
                     "standalone, rc = %d", rc ) ;
            goto error ;
         }
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to init to install standalone[%s:%s], "
                    "rc = %d", pHostName, pSvcName, rc ) ;
            
            pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pDetail || 0 == *pDetail )
               pDetail = "Failed to init to install standalone" ;
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to install "
                         "standalone[%s:%s], going to rollback",
                         pHostName, pSvcName ) ;
            instResult._status     = OMA_TASK_STATUS_ROLLBACK ;
            instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_ROLLBACK ) ;
            instResult._errno      = rc ;
            instResult._detail     = pDetail ;
            instResult._flow.push_back( flow ) ;
            tmpRc = updateProgressToTask( itr->_nodeSerialNum, instResult, FALSE ) ;
            if ( SDB_OK != tmpRc )
            {
               PD_LOG( PDWARNING, "Failed to update install standalone[%s:%s]'s "
                       "progress, rc = %d", pHostName, pSvcName, tmpRc ) ;
            }            
            goto error ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to install standalone[%s:%s], rc = %d",
                    pHostName, pSvcName, rc ) ;
            tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pDetail ) ;
            if ( tmpRc )
            {
               pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               if ( NULL == pDetail || 0 == *pDetail )
                  pDetail = "Not exeute js file yet" ;
            }
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to install "
                         "standalone[%s:%s], going to rollback",
                         pHostName, pSvcName ) ;
            instResult._status     = OMA_TASK_STATUS_ROLLBACK ;
            instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_ROLLBACK ) ;
            instResult._errno      = rc ;
            instResult._detail     = pDetail ;
            instResult._flow.push_back( flow ) ;
            tmpRc = updateProgressToTask( itr->_nodeSerialNum, instResult, FALSE ) ;
            if ( tmpRc )
            {
               PD_LOG( PDWARNING, "Failed to update install standalone[%s:%s]'s "
                       "progress, rc = %d", pHostName, pSvcName, rc ) ;
            }
            goto error ;
         }
         rc = omaGetIntElement ( retObj, OMA_FIELD_ERRNO, errNum ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to get errno from js after "
                    "installing standalone[%s:%s], rc = %d",
                    pHostName, pSvcName, rc ) ;
            pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pDetail || 0 == *pDetail )
               pDetail = "Failed to get errno from js after installing standalone" ;
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to install "
                         "standalone[%s:%s], going to rollback",
                         pHostName, pSvcName ) ;
            instResult._status     = OMA_TASK_STATUS_ROLLBACK ;
            instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_ROLLBACK ) ;
            instResult._errno      = rc ;
            instResult._detail     = pDetail ;
            instResult._flow.push_back( flow ) ;
            tmpRc = updateProgressToTask( itr->_nodeSerialNum, instResult, FALSE ) ;
            if ( tmpRc )
            {
               PD_LOG( PDWARNING, "Failed to uupdate install standalone[%s:%s]'s "
                       "progress, rc = %d", pHostName, pSvcName, tmpRc ) ;
            }
            goto error ;
         }
         if ( SDB_OK != errNum )
         {
            rc = errNum ;
            tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pDetail ) ;
            if ( SDB_OK != tmpRc )
            {
               PD_LOG( PDERROR, "Failed to get error detail from js after "
                       "installing standalone[%s:%s], rc = %d",
                       pHostName, pSvcName, tmpRc ) ;
               pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               if ( NULL == pDetail || 0 == *pDetail )
                  pDetail = "Failed to get error detail from js after "
                            "installing standalone" ;
            }
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to install "
                         "standalone[%s:%s], going to rollback",
                         pHostName, pSvcName ) ;
            instResult._status     = OMA_TASK_STATUS_ROLLBACK ;
            instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_ROLLBACK ) ;
            instResult._errno      = errNum ;
            instResult._detail     = pDetail ;
            instResult._flow.push_back( flow ) ;
            tmpRc = updateProgressToTask( itr->_nodeSerialNum, instResult, FALSE ) ;
            if ( tmpRc )
            {
               PD_LOG( PDWARNING, "Failed to uupdate install standalone[%s:%s]'s "
                       "progress, rc = %d", pHostName, pSvcName, tmpRc ) ;
            }
            goto error ;
         }
         else
         {

            ossSnprintf( flow, OMA_BUFF_SIZE,
                         "Finish installing standalone[%s:%s]",
                         pHostName, pSvcName ) ;
            PD_LOG ( PDEVENT, "Success to install standalone[%s:%s]",
                     pHostName, pSvcName ) ;
            instResult._status     = OMA_TASK_STATUS_FINISH ;
            instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
            instResult._flow.push_back( flow ) ;
            tmpRc = updateProgressToTask( itr->_nodeSerialNum, instResult, FALSE ) ;
            if ( tmpRc )
            {
               PD_LOG( PDWARNING, "Failed to uupdate install standalone[%s:%s]'s "
                       "progress, rc = %d", pHostName, pSvcName, tmpRc ) ;
            }
         }
      }
      
   done:
      return rc ;
   error:

      goto done ;
   }

   INT32 _omaInstDBBusTask::_rollback()
   {
      INT32 rc = SDB_OK ;

      if ( TRUE == _isStandalone )
      {
         rc = _rollbackStandalone() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rollback standalone, rc = %d", rc ) ;
            goto error ;
         }
      }
      else
      {
/*
         rc = _rollbackDataNode () ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rollback data nodes, rc = %d", rc ) ;
            goto error ;
         }
         rc = _rollbackCoord () ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rollback coord nodes, rc = %d", rc ) ;
            goto error ;
         }
         rc = _rollbackCatalog () ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rollback catalog nodes, rc = %d", rc ) ;
            goto error ;
         }
*/
      }
      

   done:
      return rc ;
   error:
      goto done ;

   }

   INT32 _omaInstDBBusTask::_rollbackStandalone()
   {
      INT32 rc                     = SDB_OK ;
      INT32 tmpRc                  = SDB_OK ;
      INT32 errNum                 = 0 ;
      BOOLEAN needToRollback       = FALSE ;
      CHAR flow[OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pDetail          = NULL ;
      const CHAR *pHostName        = NULL ;
      const CHAR *pSvcName         = NULL ;
      InstDBResult instResult ;
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      BSONObj bus ;
      BSONObj sys ;
      BSONObj retObj ;
      vector<InstDBBusInfo>::iterator it = _standalone.begin() ;
      
      for ( ; it != _standalone.end(); it++ )
      {
         if ( OMA_TASK_STATUS_INIT != it->_instResult._status )
         {
            bus = BSON( OMA_FIELD_UNINSTALLHOSTNAME << it->_instInfo._hostName <<
                        OMA_FIELD_UNINSTALLSVCNAME << it->_instInfo._svcName ) ;
            sys = BSON( OMA_FIELD_TASKID << _taskID ) ;
            instResult = it->_instResult ;
            needToRollback = TRUE ;
            break ;
         }
      }
      _omaRollbackStandalone runCmd ( bus, sys, _taskID ) ;
      if ( FALSE == needToRollback )
      {
         PD_LOG ( PDEVENT, "No standalone need to rollback" ) ;
         goto done ;
      } 
      pHostName = it->_instInfo._hostName.c_str() ;
      pSvcName  = it->_instInfo._svcName.c_str() ;

      ossSnprintf( flow, OMA_BUFF_SIZE, "Rollbacking standalone[%s:%s]",
                   pHostName, pSvcName ) ;
      instResult._flow.push_back( flow ) ;
      updateProgressToTask( it->_nodeSerialNum, instResult, FALSE ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to update progress before install "
                  "standalone, rc = %d", rc ) ;
         goto error ;
      }
      
      rc = runCmd.init( NULL ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to init to rollback standalone[%s:%s], "
                 "rc = %d", pHostName, pSvcName, rc ) ;
         
         pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         if ( NULL == pDetail || 0 == *pDetail )
            pDetail = "Failed to init to rollback standalone" ;
         goto error ;
      }
      rc = runCmd.doit( retObj ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to rollback standalone[%s:%s], rc = %d",
                 pHostName, pSvcName, rc ) ;
         tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pDetail ) ;
         if ( tmpRc )
         {
            pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pDetail || 0 == *pDetail )
               pDetail = "Not exeute js file yet" ;
         }
         goto error ;
      }
      rc = omaGetIntElement ( retObj, OMA_FIELD_ERRNO, errNum ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get errno from js after "
                 "rollback standalone[%s:%s], rc = %d",
                 pHostName, pSvcName, rc ) ;
         pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         if ( NULL == pDetail || 0 == *pDetail )
            pDetail = "Failed to get errno from js after rollback standalone" ;
         goto error ;
      }
      if ( SDB_OK != errNum )
      {
         rc = errNum ;
         tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pDetail ) ;
         if ( SDB_OK != tmpRc )
         {
            PD_LOG( PDERROR, "Failed to get error detail from js after "
                    "rollback standalone[%s:%s], rc = %d",
                    pHostName, pSvcName, tmpRc ) ;
            pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pDetail || 0 == *pDetail )
               pDetail = "Failed to get error detail from js after "
                         "rollback standalone" ;
         }
         goto error ;
      }
      else
      {

         ossSnprintf( flow, OMA_BUFF_SIZE,
                      "Finish rollback standalone[%s:%s]",
                      pHostName, pSvcName ) ;
         PD_LOG ( PDEVENT, "Success to rollback standalone[%s:%s]",
                  pHostName, pSvcName ) ;
         instResult._status     = OMA_TASK_STATUS_FINISH ;
         instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
         instResult._flow.push_back( flow ) ;
         tmpRc = updateProgressToTask( it->_nodeSerialNum, instResult, FALSE ) ;
         if ( tmpRc )
         {
            PD_LOG( PDWARNING, "Failed to update rollback standalone[%s:%s]'s "
                    "progress, rc = %d", pHostName, pSvcName, tmpRc ) ;
         }
      }

   done:
      return rc ;
   error:
      ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to rollback "
                   "standalone[%s:%s]", pHostName, pSvcName ) ;
      instResult._status     = OMA_TASK_STATUS_FINISH ;
      instResult._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
      instResult._errno      = rc ;
      instResult._detail     = pDetail ;
      instResult._flow.push_back( flow ) ;
      tmpRc = updateProgressToTask( it->_nodeSerialNum, instResult, FALSE ) ;
      if ( tmpRc )
      {
         PD_LOG( PDWARNING, "Failed to update rollback standalone[%s:%s]'s "
                 "progress, rc = %d", pHostName, pSvcName, tmpRc ) ;
      }
      
      goto done ; 
   }
/*
   INT32 _omaInstDBBusTask::_rollbackCoord ()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = NULL ;
      BSONObj retObj ;
      _omaRunRollbackCoordJob rollbackCoord ( vCoordSvcName, info ) ;
      map< string, vector< InstalledNode> >::iterator it = info.begin() ;
      
      if ( ( 1 != info.size() ) && ( string( ROLE_COORD ) != it->first ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Invalid coord's rollback info" ) ;
         goto error ;
      }
      rc = rollbackCoord.init( NULL ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to init to do rollback create coord "
                      "rc = %d", rc ) ;
         goto error ;
      }
      rc = rollbackCoord.doit( retObj ) ;
      if ( rc )
      {
         tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( tmpRc )
         {
            pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pErrMsg )
            {
               pErrMsg = "" ;
            }
         }
         ossSnprintf( desc, OMA_BUFF_SIZE,
                      "Failed to rollback coord" ) ;
         PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;   
         goto error ;
      }
      PD_LOG ( PDEVENT, "The rollback coord's result is: %s",
               retObj.toString().c_str() ) ;

   done:
      return rc ;
   error:
      goto done ; 
   }

   INT32 _omaInstDBBusTask::_rollbackCatalog ()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = NULL ;
      BSONObj retObj ;
      _omaRunRollbackCatalogJob rollbackCatalog ( vCoordSvcName, info ) ;
      map< string, vector< InstalledNode> >::iterator it = info.begin() ;

      if ( ( 1 != info.size() ) && ( string( ROLE_CATA ) != it->first ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Invalid catalog's rollback info" ) ;
         goto error ;
      }
      rc = rollbackCatalog.init( NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to do rollback create catalog "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = rollbackCatalog.doit( retObj ) ;
      if ( rc )
      {
         tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( tmpRc )
         {
            pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pErrMsg )
            {
               pErrMsg = "" ;
            }
         }
         ossSnprintf( desc, OMA_BUFF_SIZE,
                      "Failed to rollback catalog" ) ;
         PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;     
         goto error ;
      }
      PD_LOG ( PDEVENT, "The rollback catalog's result is: %s",
               retObj.toString().c_str() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_rollbackDataNode ()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = NULL ;
      BSONObj retObj ;
      _omaRunRollbackDataNodeJob rollbackDataNode ( vCoordSvcName, info ) ;
      map< string, vector<InstalledNode> >::iterator it = info.begin() ;

      rc = rollbackDataNode.init( NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to do rollback create data "
                  "node job, rc = %d", rc ) ;
         goto error ;
      }
      rc = rollbackDataNode.doit( retObj ) ;
      if ( rc )
      {
         tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( tmpRc )
         {
            pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pErrMsg )
            {
               pErrMsg = "" ;
            }
         }
         ossSnprintf( desc, OMA_BUFF_SIZE,
                      "Failed to rollback data groups" ) ;
         PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;   
         goto error ;
      }
      PD_LOG ( PDEVENT, "The rollback data groups' result is: %s",
               retObj.toString().c_str() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }
*/

   void _omaInstDBBusTask::_buildResultInfo( vector<InstDBBusInfo> &info,
                                             BSONArrayBuilder &bab )
   {
      vector<InstDBBusInfo>::iterator it = info.begin() ;

      for ( ; it != _standalone.end(); it++ )
      {
         BSONObjBuilder builder ;
         BSONArrayBuilder arrBuilder ;
         BSONObj obj ;

         vector<string>::iterator itr = it->_instResult._flow.begin() ;
         for ( ; itr != it->_instResult._flow.end(); itr++ )
            arrBuilder.append( *itr ) ;
         
         builder.append( OMA_FIELD_ERRNO, it->_instResult._errno ) ;
         builder.append( OMA_FIELD_DETAIL, it->_instResult._detail ) ;         
         builder.append( OMA_FIELD_HOSTNAME, it->_instResult._hostName ) ;
         builder.append( OMA_FIELD_SVCNAME, it->_instResult._svcName ) ;
         builder.append( OMA_FIELD_ROLE, it->_instResult._role ) ;
         builder.append( OMA_OPTION_DATAGROUPNAME, it->_instResult._groupName ) ;
         builder.append( OMA_FIELD_STATUS, it->_instResult._status ) ;
         builder.append( OMA_FIELD_STATUSDESC, it->_instResult._statusDesc ) ;
         builder.append( OMA_FIELD_FLOW, arrBuilder.arr() ) ;
         
         obj = builder.obj() ;
         bab.append( obj ) ;
      }
   }
   
   void _omaInstDBBusTask::_buildUpdateTaskObj( BSONObj &retObj )
   {
      
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      map< string, vector<InstDBBusInfo> >::iterator it ;

      if ( TRUE == _isStandalone )
      {
         _buildResultInfo( _standalone, bab ) ;
      }
      else
      {
         _buildResultInfo( _catalog, bab ) ;
         _buildResultInfo( _coord, bab ) ;
         it = _mapGroups.begin() ;
         for ( ; it != _mapGroups.end(); it++ )
            _buildResultInfo( it->second, bab ) ;
      }

      bob.appendNumber( OMA_FIELD_TASKID, _taskID ) ;
      bob.appendNumber( OMA_FIELD_ERRNO, _errno ) ;
      bob.append( OMA_FIELD_DETAIL, _detail ) ;
      bob.appendNumber( OMA_FIELD_STATUS, _taskStatus ) ;
      bob.append( OMA_FIELD_STATUSDESC, getTaskStatusDesc( _taskStatus ) ) ;
      bob.appendNumber( OMA_FIELD_PROGRESS, _progress ) ;
      bob.appendArray( OMA_FIELD_RESULTINFO, bab.arr() ) ;

      retObj = bob.obj() ;
   }

   INT32 _omaInstDBBusTask::_updateProgressToOM()
   {
      INT32 rc            = SDB_OK ;
      INT32 retRc         = SDB_OK ;
      UINT64 reqID        = 0 ;
      omAgentMgr *pOmaMgr = sdbGetOMAgentMgr() ;
      _pmdEDUCB *cb       = pmdGetThreadEDUCB () ;
      ossAutoEvent updateEvent ;
      BSONObj obj ;
      
      _buildUpdateTaskObj( obj ) ;

      reqID = pOmaMgr->getRequestID() ;
      pOmaMgr->registerTaskEvent( reqID, &updateEvent ) ;
      
      while( !cb->isInterrupted() )
      {
         pOmaMgr->sendUpdateTaskReq( reqID, &obj ) ;
         while ( !cb->isInterrupted() )
         {
            if ( SDB_OK != updateEvent.wait( OMA_WAIT_OMSVC_RES_TIMEOUT, &retRc ) )
            {
               continue ;
            }
            else
            {
               if ( SDB_OM_TASK_NOT_EXIST == retRc )
               {
                  PD_LOG( PDERROR, "Failed to update task[%s]'s progress "
                          "with requestID[%lld], rc = %d",
                          _taskName.c_str(), reqID, retRc ) ;
                  pOmaMgr->unregisterTaskEvent( reqID ) ;
                  rc = retRc ;
                  goto error ;
               }
               else if ( SDB_OK != retRc )
               {
                  PD_LOG( PDWARNING, "Retry to update task[%s]'s progress "
                          "with requestID[%lld], rc = %d",
                          _taskName.c_str(), reqID, retRc ) ;
                  break ;
               }
               else
               {
                  PD_LOG( PDDEBUG, "Success to update task[%s]'s progress "
                          "with requestID[%lld]", _taskName.c_str(), reqID ) ;
                  pOmaMgr->unregisterTaskEvent( reqID ) ;
                  goto done ;
               }
            }
         }
      }

      PD_LOG( PDERROR, "Receive interrupt when update install db business task "
              "progress to omsvc" ) ;
      rc = SDB_APP_INTERRUPT ;
      
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_waitAndUpdateProgress()
   {
      INT32 rc = SDB_OK ;
      BOOLEAN flag = FALSE ;
      UINT64 subTaskEventID = 0 ;
      _pmdEDUCB *cb = pmdGetThreadEDUCB () ;

      while ( !cb->isInterrupted() )
      {
         if ( SDB_OK != _taskEvent.wait ( OMA_WAIT_SUB_TASK_NOTIFY_TIMEOUT ) )
         {
            continue ;
         }
         else
         {
            while( TRUE )
            {
               _taskLatch.get() ;
               _taskEvent.reset() ;
               flag = ( subTaskEventID < _eventID ) ? TRUE : FALSE ;
               subTaskEventID = _eventID ;
               _taskLatch.release() ;
               if ( TRUE == flag )
               {
                  rc = _updateProgressToOM() ;
                  if ( SDB_APP_INTERRUPT == rc )
                  {
                     PD_LOG( PDERROR, "Failed to update add host progress"
                             " to omsvc, rc = %d", rc ) ;
                     goto error ;
                  }
                  else if ( SDB_OK != rc )
                  {
                     PD_LOG( PDERROR, "Failed to update add host progress"
                             " to omsvc, rc = %d", rc ) ;
                  }
               }
               else
               {
                  break ;
               }
            }
            if ( _isTaskFinish() )
            {
               PD_LOG( PDEVENT, "All the add host sub tasks had finished" ) ;
               goto done ;
            }
            
         }
      }

      PD_LOG( PDERROR, "Receive interrupt when running add host task" ) ;
      rc = SDB_APP_INTERRUPT ;
    
   done:
      return rc ;
   error:
      goto done ; 
   }

   BOOLEAN _omaInstDBBusTask::_isTaskFinish()
   {
      INT32 runNum    = 0 ;
      INT32 rbNum     = 0 ;
      INT32 finishNum = 0 ;
      INT32 otherNum  = 0 ;
      BOOLEAN flag    = TRUE ;
      ossScopedLock lock( &_latch, EXCLUSIVE ) ;
      
      map< string, OMA_TASK_STATUS >::iterator it = _subTaskStatus.begin() ;
      for ( ; it != _subTaskStatus.end(); it++ )
      {
         switch ( it->second )
         {
         case OMA_TASK_STATUS_FINISH :
            finishNum++ ;
            break ;
         case OMA_TASK_STATUS_RUNNING :
            runNum++ ;
            flag = FALSE ;
            break ;
         case OMA_TASK_STATUS_ROLLBACK :
            rbNum++ ;
            flag = FALSE ;
         default :
            otherNum++ ;
            flag = FALSE ;
            break ;
         }
      }
      PD_LOG( PDDEBUG, "In task[%s], there are [%d] sub task(s): "
              "[%d]running, [%d]rollback,[%d]finish, [%d]in the other status",
              _taskName.c_str(), _subTaskStatus.size(),
              runNum, rbNum, finishNum, otherNum ) ;

      return flag ;
   }

/*
   INT32 _omaInstDBBusTask::_installCatalog()
   {
      INT32 rc = SDB_OK ;
      EDUID installCatalogJobID = PMD_INVALID_EDUID ;
      rc = startCreateCatalogJob( this, &installCatalogJobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start create catalog job, rc = %d", rc ) ;
         goto error ;
      }
      while ( rtnGetJobMgr()->findJob ( installCatalogJobID ) )
      {
         ossSleep ( OSS_ONE_SEC ) ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_installCoord()
   {
      INT32 rc = SDB_OK ;
      EDUID installCoordJobID = PMD_INVALID_EDUID ;
      if ( getIsInstallFail() )
      {
         PD_LOG ( PDWARNING, "Install had failed, no need to install coord" ) ;
         goto done ;
      }
      if ( getIsInstallFinish() )
      {
         goto done ;
      }
      rc = startCreateCoordJob( this, &installCoordJobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start create coord job, rc = %d", rc ) ;
         goto error ;
      }
      while ( rtnGetJobMgr()->findJob ( installCoordJobID ) )
      {
         ossSleep ( OSS_ONE_SEC ) ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_installData()
   {
      INT32 rc = SDB_OK ;
      map< string, vector<BSONObj> >::iterator it ;
      it = _mapGroups.begin() ;
      while( it != _mapGroups.end() )
      {
         string groupname = it->first ;
         EDUID installDataJobID = PMD_INVALID_EDUID ;
         if ( getIsInstallFail() )
         {
            PD_LOG ( PDWARNING, "Install had failed, no need to install "
                     "data group[%s]", groupname.c_str() ) ;
            goto done ;
         }
         if ( getIsInstallFinish() )
         {
            goto done ;
         }
         rc = startCreateDataJob( groupname.c_str(), this,
                                  &installDataJobID ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to start create data node job, rc = %d", rc ) ;
            goto error ;
         }
         it++ ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInstDBBusTask::_getInstallInfo( BSONObj &obj,
                                                   InstallInfo &info )
   {
      INT32 rc = SDB_OK ; 
      const CHAR *pStr = NULL ;
      BSONObj conf ;
      BSONObj pattern ;

      rc = omaGetStringElement( obj, OMA_OPTION_DATAGROUPNAME,
                                &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_OPTION_DATAGROUPNAME, rc ) ;
      info._dataGroupName = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_HOSTNAME, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_HOSTNAME, rc ) ;
      info._hostName = pStr ;
      rc = omaGetStringElement( obj, OMA_OPTION_SVCNAME, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_OPTION_SVCNAME, rc ) ;
      info._svcName = pStr ;
      rc = omaGetStringElement( obj, OMA_OPTION_DBPATH, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_OPTION_DBPATH, rc ) ;
      info._dbPath = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_SDBUSER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBUSER, rc ) ;
      info._sdbUser = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_SDBPASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBPASSWD, rc ) ;
      info._sdbPasswd = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_SDBUSERGROUP, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBUSERGROUP, rc ) ;
      info._sdbUserGroup = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_USER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_USER, rc ) ;
      info._user = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_PASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_PASSWD, rc ) ;
      info._passwd = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_SSHPORT, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SSHPORT, rc ) ;
      info._sshPort = pStr ;
      pattern = BSON( OMA_FIELD_HOSTNAME       << 1 <<
                      OMA_OPTION_DATAGROUPNAME << 1 <<
                      OMA_OPTION_SVCNAME       << 1 <<
                      OMA_OPTION_DBPATH        << 1 <<
                      OMA_FIELD_SDBUSER        << 1 <<
                      OMA_FIELD_SDBPASSWD      << 1 << 
                      OMA_FIELD_SDBUSERGROUP   << 1 <<
                      OMA_FIELD_USER           << 1 <<
                      OMA_FIELD_PASSWD         << 1 << 
                      OMA_FIELD_SSHPORT << 1 ) ;
      conf = obj.filterFieldsUndotted( pattern, false ) ;
      info._conf = conf.getOwned() ;

   done:
      return rc ;
   error:
      goto done ;
   }
*/


} // namespace engine
