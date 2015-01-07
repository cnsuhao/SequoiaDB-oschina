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


#define ADD_HOST_MAX_THREAD_NUM 3

namespace engine
{

   /*
      omagent task
   */
   OMA_TASK_STATUS _omaTask::status ()
   {
      ossScopedLock lock ( &_latch, EXCLUSIVE ) ; 
      return _status ;
   }
 
   void _omaTask::setStatus( OMA_TASK_STATUS status )
   {
      ossScopedLock lock ( &_latch, EXCLUSIVE ) ;   
      _status = status ;
   }

   INT32 _omaTask::setJobStatus( string &name, OMA_JOB_STATUS status )
   {
      INT32 rc = SDB_OK ;
      ossScopedLock lock ( &_latch, EXCLUSIVE ) ;

      if ( name.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Invalid job name" ) ;
         goto error ;
      }
      PD_LOG ( PDDEBUG, "Job[%s] set status[%d]",
               name.c_str(), status ) ;
      _jobStatus[name] = status ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaTask::getJobStatus( string &name, OMA_JOB_STATUS &status )
   {
      INT32 rc =SDB_OK ;
      map< string, OMA_JOB_STATUS >::iterator it ;

      it = _jobStatus.find( name ) ;
      if ( _jobStatus.end() != it )
      {
         status = it->second ;
      }
      else
      {
         PD_LOG ( PDERROR, "Failed to get job status, no such job named %s",
                  name.c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      omagent manager
   */
   _omaTaskMgr::_omaTaskMgr ( UINT64 taskID )
   {
      _taskID = taskID ;
   }

   _omaTaskMgr::~_omaTaskMgr ()
   {
      std::map<UINT64, _omaTask*>::iterator it = _taskMap.begin() ;
      while ( it != _taskMap.end() )
      {
         SDB_OSS_DEL it->second ;
         ++it ;
      }
      _taskMap.clear() ;
   }

   UINT64 _omaTaskMgr::getTaskID ()
   {
      UINT64 id = OMA_INVALID_TASKID ;
      std::map<UINT64, _omaTask*>::iterator it ;

      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      while ( TRUE )
      {
         id = ++_taskID ;
         it = _taskMap.find( id ) ;
         if ( it == _taskMap.end() )
         {
            break ;
         }
      }
      
      return id ;
   }

   INT32 _omaTaskMgr::addTask ( _omaTask * pTask, UINT64 taskID )
   {
      INT32 rc = SDB_OK ;
      _omaTask *indexTask = NULL ;

      if ( OMA_INVALID_TASKID == taskID )
      {
         taskID = pTask->taskID() ;
      }

      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;

      std::map<UINT64, _omaTask*>::iterator it ;
      it = _taskMap.find( taskID ) ;
      if ( it != _taskMap.end() )
      {
           indexTask = it->second ;
           PD_LOG ( PDWARNING, "Exist task[%lld,%s] mutex with new task[%lld,%s]",
                    indexTask->taskID(), indexTask->taskName(),
                    pTask->taskID(), pTask->taskName() ) ;
           rc = SDB_CLS_MUTEX_TASK_EXIST ;
           goto error ;
      }
      // add to map
      _taskMap[ taskID ] = pTask ;
   done:
      return rc ;
   error:
      SDB_OSS_DEL pTask ;
      goto done ;
   }

   INT32 _omaTaskMgr::removeTask ( UINT64 taskID )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      std::map<UINT64, _omaTask*>::iterator it = _taskMap.find ( taskID ) ;
      if ( it != _taskMap.end() )
      {
         SDB_OSS_DEL it->second ;
         _taskMap.erase ( it ) ;
      }
      return SDB_OK ;
   }

   INT32 _omaTaskMgr::removeTask ( _omaTask * pTask )
   {
      INT32 rc = SDB_OK ;
      rc = removeTask ( pTask->taskID () ) ;
      return rc ;
   }

   INT32 _omaTaskMgr::removeTask ( const CHAR *pTaskName )
   {
      INT32 rc = SDB_OK ;
      std::map<UINT64, _omaTask*>::iterator it = _taskMap.begin() ;
      PD_LOG( PDDEBUG, "There are [%d] task kept in task manager, "
              "the removing task is[%s]", _taskMap.size(), pTaskName ) ;
      for ( ; it != _taskMap.end(); it++ )
      {
         _omaTask *pTask = it->second ;
         const CHAR *name = pTask->taskName() ;
         PD_LOG ( PDDEBUG, "The task is [%s]", name ) ;
         if ( 0 == ossStrncmp( name, pTaskName, ossStrlen(pTaskName) ) )
         {
            rc = removeTask( pTask ) ;
            // when remove old task, must stop iterate
            break ;
         }
      }
      return rc ;
   }

   _omaTask* _omaTaskMgr::findTask ( UINT64 taskID )
   {
      ossScopedLock lock ( &_taskLatch, SHARED ) ;
      std::map<UINT64, _omaTask*>::iterator it = _taskMap.find ( taskID ) ;
      if ( it != _taskMap.end() )
      {
         return it->second ;
      }
      return NULL ;
   }

   // get omagent task manager
   _omaTaskMgr* getTaskMgr()
   {
      static _omaTaskMgr taskMgr ;
      return &taskMgr ;
   }


   /*
      add host task
   */
   _omaAddHostTask::_omaAddHostTask( UINT64 taskID )
   : _omaTask( taskID )
   {
      _taskType             = OMA_TASK_ADD_HOST;
      _taskName             = OMA_TASK_NAME_ADD_HOST;
      _stage                = OMA_OPT_INSTALL ;
      _isTaskFinish         = FALSE ;
      _isTaskFail           = FALSE ;
      _isAddHostFail        = FALSE ;
      ossMemset( _detail, 0, OMA_BUFF_SIZE + 1 ) ;
   }

   _omaAddHostTask::~_omaAddHostTask()
   {
   }

   INT32 _omaAddHostTask::init( BSONObj &addHostRawInfo,
                                vector<AddHostInfo> addHostInfo )
   {
      _addHostRawInfo = addHostRawInfo.getOwned() ;
      _addHostInfo = addHostInfo ;
      return SDB_OK ;
   }

   void _omaAddHostTask::setTaskStage( OMA_OPT_STAGE stage )
   {
      ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
      _stage = stage ;
   }         

   void _omaAddHostTask::setIsTaskFail( BOOLEAN isFail )
   {
      ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
      _isTaskFail = isFail ;
   }

   BOOLEAN _omaAddHostTask::getIsTaskFail()
   {
      ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
      return _isTaskFail ;
   }

   void _omaAddHostTask::setIsAddHostFail( BOOLEAN isFail )
   {
      ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
      _isAddHostFail = isFail ;
   }

   BOOLEAN _omaAddHostTask::getIsAddHostFail()
   {
      ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
      return _isAddHostFail ;
   }

   INT32 _omaAddHostTask::doit()
   {
      INT32 rc = SDB_OK ;

      // check host info before add hosts
      rc = _checkHostInfo () ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to add host, for add host's informations "
                  "are conflicting, rc = %d", rc ) ;
         goto error ;
      }
      // begin to add hosts
      rc = _addHost() ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to add host, rc = %d", rc ) ;
         goto error ;
      }
      
   done:
      return rc ;
   error:
      goto done ;
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

   AddHostInfo* _omaAddHostTask::getRbHostItem()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      vector<AddHostInfo>::iterator it = _rollbackInfo.begin() ;
      for( ; it != _rollbackInfo.end(); it++ )
      {
         if ( FALSE == it->_flag )
         {
            it->_flag = TRUE ;
            return &(*it) ;
         }
      }
      return NULL ;
   }

   BOOLEAN _omaAddHostTask::registerJob( string jobName )
   {
      ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
      if ( OMA_OPT_INSTALL == _stage )
      {
         if ( _isAddHostFail )
            return FALSE ;
      }
      setJobStatus( jobName, OMA_JOB_STATUS_RUNNING ) ;
      return TRUE ;
   }

   INT32 _omaAddHostTask::updateJobStatus( string jobName,
                                           OMA_JOB_STATUS status )
   {
      INT32 rc = SDB_OK ;
      map< string, OMA_JOB_STATUS >::iterator it ;

      ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
      // set job status
      rc = setJobStatus( jobName, status ) ;
      if ( rc )
      {
         PD_LOG ( PDWARNING, "Failed to set job[%s] status, rc = %d",
                  jobName.c_str(), rc ) ;
      }
      if ( OMA_OPT_INSTALL == _stage )
      {
         if ( OMA_JOB_STATUS_FAIL == status )
         {
            _isAddHostFail = TRUE ;
         }
         // when add host fail, check whether it's the time to rollback or not
         if ( TRUE == _isAddHostFail )
         {
            PD_LOG ( PDDEBUG, "Add host had failed, going to check whether it's "
                     "time to rollback add host or not" ) ;
            for ( it = _jobStatus.begin(); it != _jobStatus.end(); it++ )
            {
               if( OMA_JOB_STATUS_RUNNING == it->second )
               {
                  // some job is still running, can't rollback
                  PD_LOG ( PDDEBUG, "Some jobs are still running in task[%s], "
                           "not the time to rollback", _taskName.c_str() ) ;
                  goto done ;
               }
            }
            // begin to rollback
            PD_LOG ( PDWARNING, "Start to rollback add host.." ) ;
            // start a async job to rollback add host task
            rc = _rollback() ;
            if ( rc )
            {
               PD_LOG( PDERROR, "Failed to rollback in add host task, "
                       "rc = %d", rc ) ;
               goto error ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHostTask::updateProgressStatus ( INT32 serialNum, AddHostPS ps,
                                                 BOOLEAN isFinish )
   {
      INT32 rc = SDB_OK ;
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
 
      if ( OMA_OPT_INSTALL == _stage )
      {
         // update the add host result
         vector<AddHostInfo>::iterator it = _addHostInfo.begin() ;
         for ( ; it != _addHostInfo.end(); it++ )
         {
            if ( serialNum == it->_serialNum )
            {
               it->_isFinish = isFinish ;
               it->_ps = ps ;
               break ;
            }
         }
      }
      else if ( OMA_OPT_ROLLBACK == _stage )
      {
         // update the remove host result
         vector<AddHostInfo>::iterator it = _rollbackInfo.begin() ;
         for ( ; it != _rollbackInfo.end(); it++ )
         {
            if ( serialNum == it->_serialNum )
            {
               it->_isFinish = isFinish ;
               it->_ps = ps ;
               break ;
            }
         }
      }
      else
      {
         rc = SDB_SYS ;
         PD_LOG_MSG ( PDERROR, "Invalid add host stage" ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHostTask::queryProgress ( BSONObj &progress )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      const CHAR *pStage = NULL ;

      // collect prgress info
      _collectProgressInfo() ;
      
      // while task has failed
      if ( getIsTaskFail() )
      {
         if ( '\0' == _detail[0] )
         {
            PD_LOG_MSG ( PDERROR,"Task[%s] had failed, please check "
                         "the dialog for more detail", taskName() ) ;
         }
         else
         {
            PD_LOG_MSG ( PDERROR, _detail ) ;
         }
         rc = SDB_OMA_TASK_FAIL ;
         goto done ;
      }
      
      // test which stage task is in
      if ( OMA_OPT_INSTALL == _stage )
      {
         pStage = STAGE_INSTALL ;
      }
      else if ( OMA_OPT_ROLLBACK == _stage )
      {
         pStage = STAGE_ROLLBACK ;
      }
      else
      {
         PD_LOG ( PDERROR, "Invalid task's stage" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      try
      {
         // taskID
         bob.append( OMA_FIELD_TASKID, (SINT64)_taskID ) ;
         // isFinish
         bob.appendBool( OMA_FIELD_ISFINISH, _isTaskFinish ) ;
         // status
         bob.append( OMA_FIELD_STATUS, pStage ) ;
         
         // set add host status
         vector<AddHostInfo>::iterator it ;
         if ( OMA_OPT_INSTALL== _stage )
         {
            it = _addHostInfo.begin() ;
            while ( it != _addHostInfo.end() )
            {
               string ip = it->_item._ip ;
               string desc = it->_ps._desc ;
               BSONObjBuilder bob ;
               BSONObj result ;
               bob.append( OMA_FIELD_IP, ip.c_str() ) ;
               bob.appendBool( OMA_FIELD_HASFINISH,
                               ( (TRUE == it->_isFinish) ? 1 : 0 ) ) ;
               if ( SDB_OK != it->_ps._errno )
               {
                  bob.appendBool( OMA_FIELD_HASERROR, 1 ) ;
                  bob.append( OMA_FIELD_DESC, it->_ps._errMsg.c_str() ) ;
               }
               else
               {
                  bob.appendBool( OMA_FIELD_HASERROR, 0 ) ;
                  bob.append( OMA_FIELD_DESC, desc.c_str() ) ;
               }
               result = bob.obj() ;
               bab.append ( result ) ;
               it++ ;
            } 
         }
         else
         {
            it = _rollbackInfo.begin() ;
            while ( it != _rollbackInfo.end() )
            {
               string ip = it->_item._ip ;
               string desc = it->_ps._desc ;
               BSONObjBuilder bob ;
               BSONObj result ;
               bob.append( OMA_FIELD_IP, ip.c_str() ) ;
               bob.appendBool( OMA_FIELD_HASFINISH,
                               ( (TRUE == it->_isFinish) ? 1 : 0 ) ) ;
               if ( SDB_OK != it->_ps._errno )
               {
                  bob.appendBool( OMA_FIELD_HASERROR, 1 ) ;
                  bob.append( OMA_FIELD_DESC, it->_ps._errMsg.c_str() ) ;
               }
               else
               {
                  bob.appendBool( OMA_FIELD_HASERROR, 0 ) ;
                  bob.append( OMA_FIELD_DESC, desc.c_str() ) ;
               }
               result = bob.obj() ;
               bab.append ( result ) ;
               it++ ;
            } 
         }
      
         // try to set ErrMsg
         // TODO:
         if ( !(bob.hasField( OMA_FIELD_ERRMSG ) ) )
         {
            bob.append( OMA_FIELD_ERRMSG, "" ) ;
         }
         // set return result
         bob.appendArray( OMA_FIELD_PROGRESS, bab.arr() ) ;
         progress = bob.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR, "Failed to get add host progress: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
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
      
      // extract "errno"
      rc = omaGetIntElement ( retObj, OMA_FIELD_ERRNO, errNum ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                  "rc = %d", OMA_FIELD_ERRNO, rc ) ;
         goto error ;
      }
      if ( SDB_OK  != errNum )
      {
         // extract "detail"
         rc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                     "rc = %d", OMA_FIELD_ERRNO, rc ) ;
            goto error ;
         }
         // set task to be failing
         ossSnprintf( _detail, OMA_BUFF_SIZE, "%s", pErrMsg ) ;
         _isTaskFail = TRUE ;
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
      INT32 hostNum = _addHostInfo.size() ;
      INT32 threadNum = 0 ;
      if ( 0 == hostNum )
      {
         PD_LOG_MSG ( PDERROR, "No information for adding host" ) ;
         goto error ;
      }
      threadNum = hostNum < ADD_HOST_MAX_THREAD_NUM ? hostNum :
                                                      ADD_HOST_MAX_THREAD_NUM ;
      for( INT32 i = 0; i < threadNum; i++ )
      { 
         CHAR jobName[OMA_BUFF_SIZE + 1] = { 0 };
         EDUID jobID = PMD_INVALID_EDUID ;
         ossSnprintf(jobName, OMA_BUFF_SIZE, "%s %d", OMA_JOB_ADDHOST, i ) ;
         ossScopedLock lock( &_taskLatch, EXCLUSIVE ) ;
         if ( !_isAddHostFail )
         {
            // start add host jobs
            rc = startAddHostJob( jobName, this, &jobID ) ;
            if ( rc )
            {
               // can't use setIsAddHostFail(), for it also has _taskLatch
               _isAddHostFail = TRUE ;
               PD_LOG ( PDERROR, "Failed to start add host job[%s], "
                        "rc = %d", jobName, rc ) ;
               goto error ;
            }
         }
         else
         {
            PD_LOG( PDEVENT, "Add host had failed, no need to "
                    "start the rest jobs" ) ;
            goto done ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHostTask::_rollback()
   {
      INT32 rc = SDB_OK ;
      INT32 hostNum = 0 ;
      INT32 threadNum = 0 ;
      
      //get rollback host info
      _getRollbackInfo() ;
      // clear _jobStatus for rollback host jobs
      _jobStatus.clear() ;
      // set task stage to be rollback
      _stage = OMA_OPT_ROLLBACK ;

      hostNum = _rollbackInfo.size() ;
      if ( 0 == hostNum )
      {
         PD_LOG ( PDDEBUG, "No host needs to rollback" ) ;
         goto done ;
      }
      threadNum = hostNum < ADD_HOST_MAX_THREAD_NUM ? hostNum :
                                                      ADD_HOST_MAX_THREAD_NUM ;
      for( INT32 i = 0; i < threadNum; i++ )
      { 
         CHAR jobName[OMA_BUFF_SIZE + 1] = { 0 };
         EDUID jobID = PMD_INVALID_EDUID ;
         ossSnprintf(jobName, OMA_BUFF_SIZE, "%s %d", OMA_JOB_ROLLBACKHOST, i ) ;
         // start add host jobs
         rc = startRbHostJob( jobName, this, &jobID ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to start add host job[%s], "
                     "rc = %d", jobName, rc ) ;
            goto error ;
         }
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }
   
   void _omaAddHostTask::_getRollbackInfo()
   {
      vector<AddHostInfo>::iterator it = _addHostInfo.begin() ;

      for ( ; it != _addHostInfo.end(); it++ )
      {
         if ( (TRUE == it->_flag) && (TRUE == it->_isFinish) &&
              (TRUE == it->_ps._hasInstall) )
         {
            it->_flag = FALSE ;
            it->_isFinish = FALSE ;
            _rollbackInfo.push_back( *it ) ;
         }
      }
   }

   BOOLEAN _omaAddHostTask::_hasUninstallHost()
   {
      BOOLEAN flag = FALSE ;
      vector<AddHostInfo>::iterator it = _rollbackInfo.begin() ;
      for ( ; it != _rollbackInfo.end(); it++ )
      {
         if ( TRUE == it->_ps._hasInstall )
         {
            flag = TRUE ;
            break ;
         }
      }
      return flag ;
      
   }

   void _omaAddHostTask::_buildErrMsg()
   {
      string result ;
      string str ;
      string str2 ;
      BSONArrayBuilder bab ;
      BSONArray arr ;
      vector<AddHostInfo>::iterator it = _rollbackInfo.begin() ;
      for ( ; it != _rollbackInfo.end(); it++ )
      {
         if ( TRUE == it->_ps._hasInstall )
         {
            bab.append ( it->_item._ip.c_str() ) ;
         }
         if ( SDB_OK != it->_ps._errno )
         {
            if ( 0 == str.length() )
               str += "in host[" + it->_item._ip + "], " + it->_ps._errMsg ;
            else
               str += "; in host [" + it->_item._ip + "], " + it->_ps._errMsg ;
         }
      }
      arr = bab.arr() ;
      // build the return result
      if ( 0 != str.length() )
      {
         result = "Rollback is failing for these reasons: " ;
         result += str ;
      }
      if ( !arr.isEmpty() )
      {
         str2 = "Need to uninstall db packet in these hosts manually: " ;
         str2 += arr.toString( TRUE, FALSE ).c_str() ;
      }
      if ( 0 != str2.length() )
      {
         if ( 0 == result.length() )
            result = str2 ;
         else
            result += ". " + str2 ;
      }
      if ( 0 != result.length() )
      {
         ossSnprintf( _detail, OMA_BUFF_SIZE, "%s", result.c_str() ) ;
      }
   }

   void _omaAddHostTask::_collectProgressInfo()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      vector<AddHostInfo>::iterator it ;
      
      if ( OMA_OPT_INSTALL == _stage )
      {
         // check wether add host finish or not
         _isTaskFinish = TRUE ;
         it = _addHostInfo.begin() ;
         for ( ; it != _addHostInfo.end(); it++ )
         {
            if ( (FALSE == it->_flag) ||
                 (FALSE == it->_isFinish) ||
                 (SDB_OK != it->_ps._errno) )
            {
               _isTaskFinish = FALSE ;
               break ;
            }
         }
      }
      else
      {
         BOOLEAN hasJobFail = FALSE ;

         // check wether remove host finish or not
         it = _rollbackInfo.begin() ;
         // to see wether all the hosts had been handled or not
         for ( ; it != _rollbackInfo.end(); it++ )
         {
            if ( FALSE == it->_flag )
            {
               // some hosts have not been handled yet
               return ;
            }
         }
         // if all the hosts had been handled,
         // check task is suceessful or not
         map<string, OMA_JOB_STATUS>::iterator it = _jobStatus.begin() ;
         for ( ; it != _jobStatus.end(); it++ )
         {
            if ( OMA_JOB_STATUS_RUNNING == it->second )
            {
               // some jobs are still running,
               // task not finish yet
               return ;
            }
            else if ( OMA_JOB_STATUS_FAIL == it->second )
            {
               hasJobFail = TRUE ;
            }
         }
         // if no job is running, but some jobs had failed
         // or some hosts had not been uninstall, task fail
         if ( hasJobFail || _hasUninstallHost() )
         {
            _isTaskFail = TRUE ;
            _buildErrMsg() ;
         }
         else
         {
            _isTaskFinish = TRUE ;
         }
      }
   }

   /*
      install database business
   */
   _omaInsDBBusTask::_omaInsDBBusTask( UINT64 taskID )
   : _omaTask( taskID )
   {
      _taskType             = OMA_TASK_INSTALL_DB ;
      _taskName             = OMA_TASK_NAME_INSTALL_DB_BUSINESS ;
      _stage                = OMA_OPT_INSTALL ;
      _isInstallFinish      = FALSE ;
      _isRollbackFinish     = FALSE ;
      _isRemoveVCoordFinish = FALSE ;
      _isTaskFinish         = FALSE ;
      _isInstallFail        = FALSE ;
      _isRollbackFail       = FALSE ;
      _isRemoveVCoordFail   = FALSE ;
      _isTaskFail           = FALSE ;
      _vCoordSvcName        = "" ;
      ossMemset( _detail, 0, OMA_BUFF_SIZE + 1 ) ;
   }

   _omaInsDBBusTask::~_omaInsDBBusTask()
   {
   }

   INT32 _omaInsDBBusTask::init( BOOLEAN isStandalone,
                                 vector<BSONObj> standalone,
                                 vector<BSONObj> coord,
                                 vector<BSONObj> catalog,
                                 vector<BSONObj> data,
                                 BSONObj &other )
   {
      INT32 rc = SDB_OK ;
      vector<BSONObj>::iterator it ;
      map<string, vector<BSONObj> >::iterator iter ;
      _isStandalone = isStandalone ;
      // in case of standalone
      if ( isStandalone )
      {
         // init _standalone and _standaloneResult
         _standalone = standalone ;
         _standaloneResult._rc = SDB_OK ;
         _standaloneResult._totalNum = _standalone.size() ;
         _standaloneResult._finishNum = 0 ;
      }
      else // in case of cluster
      {
         // init _coord and _coordResult
         _coord = coord ;
         _coordResult._rc = SDB_OK ;
         _coordResult._totalNum = _coord.size() ;
         _coordResult._finishNum = 0 ;
         // init _catalog and _catalogResult
         _catalog = catalog ;
         _catalogResult._rc = SDB_OK ;
         _catalogResult._totalNum = _catalog.size() ;
         _catalogResult._finishNum = 0 ;
         // init _mapGroups and _mapGroupsResult
         it = data.begin() ;
         // let data node sort by group name
         while( it != data.end() )
         {
            const CHAR *name = NULL ;
            string key = "" ;
            rc = omaGetStringElement ( *it, OMA_OPTION_DATAGROUPNAME, &name ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_OPTION_DATAGROUPNAME, rc ) ;
            key = string( name ) ;
            _mapGroups[key].push_back( *it ) ;
            it++ ;
         }
         // init data node result
         iter = _mapGroups.begin() ;
         while ( iter != _mapGroups.end() )
         {
            string groupname = iter->first ;
            InstallResult result ;
            result._rc = 0 ;
            result._totalNum = (iter->second).size() ;
            result._finishNum = 0 ;
            _mapGroupsResult.insert( std::pair<string,
                                     InstallResult>( groupname, result ) ) ;
            iter++ ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::doit()
   {
      INT32 rc = SDB_OK ;
      // in case of standalone
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
         // create temporary catalog
         rc = _installVirtualCoord() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to create temporary coord, rc = %d", rc ) ;
            goto error ;
         }
         // create catalog job
         rc = _installCatalog() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to start create catalog job, rc = %d", rc ) ;
            goto error ;
         }
         // create coord job
         rc = _installCoord() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to start create coord job, rc = %d", rc ) ;
            goto error ;
         }
         // create data node job
         rc = _installData() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to start create data node job, rc = %d", rc ) ;
            goto error ;
         }
      }
   done:
      return rc ;
   error:
      setIsTaskFail( TRUE ) ;
      goto done ;
   }

   void _omaInsDBBusTask::setTaskStage( OMA_OPT_STAGE stage )
   {
      _stage = stage ;
   }

   void _omaInsDBBusTask::setIsInstallFinish( BOOLEAN isFinish )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _isInstallFinish = isFinish ;
   }   

   void _omaInsDBBusTask::setIsRollbackFinish( BOOLEAN isFinish )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _isRollbackFinish = isFinish ;
   } 

   void _omaInsDBBusTask::setIsRemoveVCoordFinish( BOOLEAN isFinish )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _isRemoveVCoordFinish = isFinish ;
   }

   void _omaInsDBBusTask::setIsTaskFinish( BOOLEAN isFinish )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _isTaskFinish = isFinish ;
   }

   void _omaInsDBBusTask::setIsInstallFail( BOOLEAN isFail )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _isInstallFail = isFail ;
   }

   void _omaInsDBBusTask::setIsRollbackFail( BOOLEAN isFail )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _isRollbackFail = isFail ;
   }   

   void _omaInsDBBusTask::setIsRemoveVCoordFail( BOOLEAN isFail )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _isRemoveVCoordFail = isFail ;
   }   

   void _omaInsDBBusTask::setIsTaskFail( BOOLEAN isFail )
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      _isTaskFail = isFail ;
   }   

   BOOLEAN _omaInsDBBusTask::getIsInstallFinish()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      return _isInstallFinish ;
   }

   BOOLEAN _omaInsDBBusTask::getIsRollbackFinish()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      return _isRollbackFinish ;
   }

   BOOLEAN _omaInsDBBusTask::getIsRemoveVCoordFinish()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      return _isRemoveVCoordFinish ;
   }

   BOOLEAN _omaInsDBBusTask::getIsTaskFinish()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      return _isTaskFinish ;
   }

   BOOLEAN _omaInsDBBusTask::getIsInstallFail()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      return _isInstallFail ;
   }

   BOOLEAN _omaInsDBBusTask::getIsRollbackFail()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      return _isRollbackFail ;
   }

   BOOLEAN _omaInsDBBusTask::getIsRemoveVCoordFail()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      return _isRemoveVCoordFail ;
   }
 
   BOOLEAN _omaInsDBBusTask::getIsTaskFail()
   {
      ossScopedLock lock ( &_taskLatch, EXCLUSIVE ) ;
      return _isTaskFail ;
   }

   void _omaInsDBBusTask::setErrDetail( const CHAR *pErrDetail )
   {
      ossSnprintf( _detail, OMA_BUFF_SIZE, pErrDetail ) ;
   }

   vector<BSONObj>& _omaInsDBBusTask::getInstallStandaloneInfo()
   {
      return _standalone;
   }

   vector<BSONObj>& _omaInsDBBusTask::getInstallCatalogInfo()
   {
      return _catalog ;
   }

   vector<BSONObj>& _omaInsDBBusTask::getInstallCoordInfo()
   {
      return _coord ;
   }

   INT32 _omaInsDBBusTask::getInstallDataGroupInfo( string &name,
                                        vector<BSONObj> &dataGroupInstallInfo )
   {
      INT32 rc  = SDB_OK ;
      map< string, vector<BSONObj> >::iterator it ;

      it = _mapGroups.find( name ) ;
      if ( it != _mapGroups.end() )
      {
         dataGroupInstallInfo = it->second ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "No group[%s] install info", name.c_str() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::updateInstallStatus( BOOLEAN isFinish,
                                                INT32 retRc,
                                                const CHAR *pRole,
                                                const CHAR *pErrMsg,
                                                const CHAR *pDesc,
                                                const CHAR *pGroupName,
                                                InstalledNode *pNode )
   {
      INT32 rc = SDB_OK ;
      ossScopedLock lock ( &_jobLatch, EXCLUSIVE ) ;

      // check argument
      if ( NULL == pRole )
      {
         PD_LOG ( PDERROR,
                  "Not speciefy role for updating install result" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( ( 0 == ossStrncmp( pRole, ROLE_DATA, ossStrlen( ROLE_DATA ) ) ) &&
           ( NULL == pGroupName ) )
      {
         PD_LOG ( PDERROR,
                  "Not speciefy data group for updating install result" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( ( TRUE == isFinish ) && ( NULL == pNode ) )
      {
         PD_LOG ( PDERROR, "The info of finish installed node "
                  "is empty for register" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( NULL == pErrMsg ) pErrMsg = "" ;
      if ( NULL == pDesc ) pDesc = "" ;
      if ( NULL == pGroupName ) pGroupName = "" ;

      // update the install result
      if ( 0 == ossStrncmp( pRole, ROLE_DATA, ossStrlen( ROLE_DATA ) ) )
      {
         map<string, InstallResult>::iterator it ; 
         string groupname = pGroupName ;
         it = _mapGroupsResult.find( groupname ) ;
         if ( it != _mapGroupsResult.end() )
         {
            InstallResult &result = it->second ;
            result._desc = pDesc ;
            if ( retRc )
            {
               result._rc = retRc ;
               result._errMsg = pErrMsg ;
               goto done ;
            }
            if ( isFinish )
            {
               result._finishNum++ ;
               result._installedNodes.push_back( *pNode ) ;
            }
         }
      }
      else if ( 0 == ossStrncmp( pRole, ROLE_COORD,
                                 ossStrlen( ROLE_COORD ) ) )
      {
         _coordResult._desc = pDesc ;
         if ( retRc )
         {
            _coordResult._rc = retRc ;
            _coordResult._errMsg = pErrMsg ;
            goto done ;
         }
         if ( isFinish )
         {
            _coordResult._finishNum++ ;
            _coordResult._installedNodes.push_back( *pNode ) ;
         }
      }
      else if ( 0 == ossStrncmp( pRole, ROLE_CATA,
                                 ossStrlen( ROLE_CATA ) ) )
      {
         _catalogResult._desc = pDesc ;
         if ( retRc )
         {
            _catalogResult._rc = retRc ;
            _catalogResult._errMsg = pErrMsg ;
            goto done ;
         }
         if ( isFinish )
         {
            _catalogResult._finishNum++ ;
            _catalogResult._installedNodes.push_back( *pNode ) ;
         }
      }
      else if ( 0 == ossStrncmp( pRole, ROLE_STANDALONE,
                                 ossStrlen( ROLE_STANDALONE) ) )
      {
         _standaloneResult._desc = pDesc ;
         if ( retRc )
         {
            _standaloneResult._rc = retRc ;
            _standaloneResult._errMsg = pErrMsg ;
            // though this node failed to be created, it's info
            // had been registed in remote sdbcm, we need to remove
            // those info, so we need to keep this node's install info
            // if it's offered
            if ( NULL != pNode )
            {
               _standaloneResult._installedNodes.push_back( *pNode ) ;
            }
            goto done ;
         }
         if ( isFinish )
         {
            _standaloneResult._finishNum++ ;
         }
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR,
                  "Failed to update install result, rc = %d", rc ) ;
         goto error ;
      }
      // check whether it's time to set task's status or 
      // remove virtual coord
      if ( isInstallFinish() )
      {
         setIsInstallFinish( TRUE ) ;
         if ( _isStandalone )
         {
            setIsTaskFinish( TRUE ) ;
            goto done ;
         }
         else
         {
            // start an async job to remove virtual coord
            rc = removeVirtualCoord() ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed remove virtual coord, rc = %d", rc ) ;
               goto error ;
            }
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::getInstalledNodeResult ( const CHAR *pRole,
                                   map< string, vector<InstalledNode> >& info )
   {
      INT32 rc = SDB_OK ;
      
      if ( 0 == ossStrncmp( ROLE_DATA, pRole, ossStrlen(ROLE_DATA) ) )
      {
         map< string, InstallResult >::iterator it = _mapGroupsResult.begin() ;
         for( ; it != _mapGroupsResult.end(); it++ )
         {
            vector< InstalledNode > &nodes = (it->second)._installedNodes ;
            info.insert (
               pair< string, vector<InstalledNode> >( string(it->first), nodes )
            ) ;
         }
      }
      else if ( 0 == ossStrncmp( ROLE_CATA, pRole, ossStrlen(ROLE_CATA) ) )
      {
         vector< InstalledNode > &nodes = _catalogResult._installedNodes ;
         info.insert (
            pair< string, vector<InstalledNode> >( string(ROLE_CATA), nodes )
         ) ;
      }
      else if ( 0 == ossStrncmp( ROLE_COORD, pRole, ossStrlen(ROLE_COORD) ) )
      {
         vector< InstalledNode > &nodes = _coordResult._installedNodes ;
         info.insert (
            pair< string, vector<InstalledNode> >( string(ROLE_COORD), nodes )
         ) ;
      }
      else if ( 0 == ossStrncmp( ROLE_STANDALONE, pRole,
                                 ossStrlen(ROLE_STANDALONE) ) )
      {
         vector< InstalledNode > &nodes = _standaloneResult._installedNodes ;
         info.insert (
         pair< string, vector<InstalledNode> >( string(ROLE_STANDALONE), nodes )
         ) ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Invalid role for get installed node result" ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN _omaInsDBBusTask::isInstallFinish ()
   {
      // TODO: need to add a lock different with update install statue?

      // in case of standalone
      if ( _isStandalone )
      {
         if ( _standaloneResult._totalNum == _standaloneResult._finishNum )
         {
            return TRUE ;
         }
         else
         {
            return FALSE ;
         }
      }
      else // in case of cluster
      {
         map<string, InstallResult>::iterator it ;
         if ( _catalogResult._totalNum > _catalogResult._finishNum )
         {
            return FALSE ;
         }
         if ( _coordResult._totalNum > _coordResult._finishNum )
         {
            return FALSE ;
         }
         it = _mapGroupsResult.begin() ;
         while( it != _mapGroupsResult.end() )
         {
            InstallResult &result = it->second ;
            if ( result._totalNum > result._finishNum )
            {
               return FALSE ;
            }
            it++ ;
         }
         return TRUE ;
      }
   }

   INT32 _omaInsDBBusTask::queryProgress ( BSONObj &progress )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      BSONObj standaloneResult ;
      BSONObj coordResult ;
      BSONObj catalogResult ;
      const CHAR *pStage = NULL ;
      
      // while task has failed
      if ( getIsTaskFail() )
      {
         if ( '\0' == _detail[0] )
         {
            PD_LOG_MSG ( PDERROR,"Task[%s] had failed, please check "
                         "the dialog for more detail", taskName() ) ;
         }
         else
         {
            PD_LOG_MSG ( PDERROR, _detail ) ;
         }
         rc = SDB_OMA_TASK_FAIL ;
         goto done ;
      }
      // test which stage task is in
      if ( OMA_OPT_INSTALL == _stage )
      {
         pStage = STAGE_INSTALL ;
      }
      else if ( OMA_OPT_ROLLBACK == _stage )
      {
         pStage = STAGE_ROLLBACK ;
      }
      else
      {
         PD_LOG ( PDERROR, "Invalid task's stage" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      try
      {
         // taskID
         bob.append( OMA_FIELD_TASKID, (SINT64)_taskID ) ;
         // isFinish
         bob.appendBool( OMA_FIELD_ISFINISH, _isTaskFinish ) ;
         // status
         bob.append( OMA_FIELD_STATUS, pStage ) ;
         // in case of standalone
         if ( _isStandalone )
         {
            // get standalone status
            if ( _standaloneResult._rc )
            {
               bob.append( OMA_FIELD_ERRMSG, _standaloneResult._errMsg ) ;
            }
            standaloneResult = BSON ( OMA_FIELD_NAME
                                      << OMA_FIELD_STANDALONE
                                      << OMA_FIELD_TOTALCOUNT
                                      << _standaloneResult._totalNum
                                      << OMA_FIELD_INSTALLEDCOUNT
                                      << _standaloneResult._finishNum
                                      << OMA_FIELD_DESC
                                      << _standaloneResult._desc.c_str() ) ;
            bab.append ( standaloneResult ) ;
         }
         else // in case of cluster
         {
            // get catalog status
            if ( _catalogResult._rc )
            {
               bob.append( OMA_FIELD_ERRMSG, _catalogResult._errMsg ) ;
            }
            catalogResult = BSON ( OMA_FIELD_NAME
                                   << OMA_FIELD_CATALOG
                                   << OMA_FIELD_TOTALCOUNT
                                   << _catalogResult._totalNum
                                   << OMA_FIELD_INSTALLEDCOUNT
                                   << _catalogResult._finishNum
                                   << OMA_FIELD_DESC
                                   << _catalogResult._desc.c_str() ) ;
            bab.append ( catalogResult ) ;
            // get coord status
            if ( ( _coordResult._rc ) && ( !bob.hasField(OMA_FIELD_ERRMSG) ) )
            {
               bob.append( OMA_FIELD_ERRMSG, _coordResult._errMsg ) ;
            }
            coordResult = BSON ( OMA_FIELD_NAME
                                 << OMA_FIELD_COORD
                                 << OMA_FIELD_TOTALCOUNT
                                 << _coordResult._totalNum
                                 << OMA_FIELD_INSTALLEDCOUNT
                                 << _coordResult._finishNum
                                 << OMA_FIELD_DESC
                                 << _coordResult._desc.c_str() ) ;
            bab.append ( coordResult ) ;
            // get data group status
            std::map< string, InstallResult >::iterator it ;
            it = _mapGroupsResult.begin() ;
            while ( it != _mapGroupsResult.end() )
            {
               string groupname = it->first ;
               InstallResult &result = it->second ;
               BSONObj groupResult ;
               if ( ( result._rc ) && ( !bob.hasField(OMA_FIELD_ERRMSG) ) )
               {
                  bob.append( OMA_FIELD_ERRMSG, result._errMsg ) ;
               }
               groupResult = BSON ( OMA_FIELD_NAME
                                    << groupname.c_str()
                                    << OMA_FIELD_TOTALCOUNT
                                    << result._totalNum
                                    << OMA_FIELD_INSTALLEDCOUNT
                                    << result._finishNum
                                    << OMA_FIELD_DESC
                                    << result._desc.c_str() ) ;
               bab.append ( groupResult ) ;
               it++ ;
            }
         }
         // try to set ErrMsg
         if ( !(bob.hasField( OMA_FIELD_ERRMSG ) ) )
         {
            bob.append( OMA_FIELD_ERRMSG, "" );
         }
         // set return result
         bob.appendArray( OMA_FIELD_PROGRESS, bab.arr() ) ;
         progress = bob.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR,
                  "Failed to get install db business progress: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::updateInstallJobStatus( string &name,
                                                   OMA_JOB_STATUS status )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN needRollback = FALSE ;
      map< string, OMA_JOB_STATUS >::iterator it ;
      ossScopedLock lock ( &_jobLatch, EXCLUSIVE ) ;
      // set job status
      rc = setJobStatus( name, status ) ;
      if ( rc )
      {
         PD_LOG ( PDWARNING, "Failed to set job[%s] status, rc = %d",
                  name.c_str(), rc ) ;
      }
      if ( OMA_JOB_STATUS_FAIL == status )
      {
         setIsInstallFail( TRUE ) ;
      }
      // check whether is there any job failed or not,
      // and whether it's the time to rollback
      for ( it = _jobStatus.begin(); it != _jobStatus.end(); it++ )
      {
         PD_LOG ( PDDEBUG, "Job[%s]'s status is : %d",
                  it->first.c_str(), it->second ) ;
         if( OMA_JOB_STATUS_RUNNING == it->second )
         {
            // some job is still running, can't rollback
            PD_LOG ( PDDEBUG, "Some jobs are still running "
                     "in task[%s]", _taskName.c_str() ) ;
            goto done ;
         }
         else if ( OMA_JOB_STATUS_FAIL == it->second )
         {
            PD_LOG ( PDWARNING, "Some jobs are failing, need to rollback" ) ;
            needRollback = TRUE ;
         }
      }
      if ( TRUE == needRollback )
      {
         PD_LOG ( PDWARNING, "Start to rollback.." ) ;
         // start a async job to rollback task
         rc = rollbackInternal() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to rollback in add db business task, "
                    "rc = %d", rc ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::rollbackInternal()
   {
      INT32 rc = SDB_OK ;
      EDUID jobID = PMD_INVALID_EDUID ;
      // set task stage
      setTaskStage ( OMA_OPT_ROLLBACK ) ;
      rc = startInsDBBusTaskRbJob ( _isStandalone, _vCoordSvcName,
                                    this, &jobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start to roolback in add db business task "
                 "rc = %d", rc ) ;
         goto error ;
      }
      // wait until rollback is finish
      while ( rtnGetJobMgr()->findJob( jobID ) )
      {
         ossSleep ( OSS_ONE_SEC ) ;
      }
   done:
      return rc ;
   error:
      setIsRollbackFail( TRUE ) ;
      setErrDetail( "Failed to rollback in add "
                    "db business task, please do it manually" ) ;
      goto done ;
   }

   INT32 _omaInsDBBusTask::_saveVCoordInfo( BSONObj &info )
   {
      INT32 rc                    = SDB_OK ;
      const CHAR *pVCoordSvcName  = NULL ;
      rc = omaGetStringElement( info, OMA_FIELD_VCOORDSVCNAME, &pVCoordSvcName ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get filed[%s], rc = %s",
                  OMA_FIELD_VCOORDSVCNAME, rc ) ;
         goto error ;
      }
      _vCoordSvcName = pVCoordSvcName ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::_installVirtualCoord()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      const CHAR *pErrMsg = NULL ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      BSONObj vCoordRet ;
      _omaCreateVirtualCoord createVCoord ;
      
      // create virtual coord and save it's info for future
      rc = createVCoord.createVirtualCoord( vCoordRet ) ;
      if ( rc )
      {
         // if we can't get field "detail", it means we failed in CPP,
         // we had not executed js file yet
         tmpRc = omaGetStringElement ( vCoordRet, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( tmpRc )
         {
            pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pErrMsg )
            {
               pErrMsg = "" ;
            }
         }
         ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to create temporary "
                      "coord: %s", pErrMsg ) ;
         PD_LOG_MSG( PDERROR, desc ) ;
         // set task to be failing
         setIsTaskFail( TRUE ) ;
         // set error detail
         setErrDetail( desc ) ;
         goto error ;
      }
      rc = _saveVCoordInfo( vCoordRet ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to save virtual coord install result, "
                  "rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::_installStandalone()
   {
      INT32 rc = SDB_OK ;
      EDUID createStandaloneJobID = PMD_INVALID_EDUID ;
      // start create standalone job
      rc = startCreateStandaloneJob( this, &createStandaloneJobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start create standalone job, rc = %d", rc ) ;
         goto error ;
      }
      while ( rtnGetJobMgr()->findJob ( createStandaloneJobID ) )
      {
         ossSleep ( OSS_ONE_SEC ) ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::_installCatalog()
   {
      INT32 rc = SDB_OK ;
      EDUID installCatalogJobID = PMD_INVALID_EDUID ;
      // start create catalog job
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

   INT32 _omaInsDBBusTask::_installCoord()
   {
      INT32 rc = SDB_OK ;
      EDUID installCoordJobID = PMD_INVALID_EDUID ;
      // test install status, and decide go on or stop task
      if ( getIsInstallFail() )
      {
         PD_LOG ( PDWARNING, "Install had failed, no need to install coord" ) ;
         goto done ;
      }
      if ( getIsInstallFinish() )
      {
         goto done ;
      }
      // start coord job
      rc = startCreateCoordJob( this, &installCoordJobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start create coord job, rc = %d", rc ) ;
         goto error ;
      }
      // wait until this job finish
      while ( rtnGetJobMgr()->findJob ( installCoordJobID ) )
      {
         ossSleep ( OSS_ONE_SEC ) ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBusTask::_installData()
   {
      INT32 rc = SDB_OK ;
      map< string, vector<BSONObj> >::iterator it ;
      it = _mapGroups.begin() ;
      while( it != _mapGroups.end() )
      {
         string groupname = it->first ;
         EDUID installDataJobID = PMD_INVALID_EDUID ;
         // test install status, and decide go on or stop task
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
         // start data job
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

   INT32 _omaInsDBBusTask::removeVirtualCoord()
   {
      INT32 rc = SDB_OK ;
      EDUID jobID = PMD_INVALID_EDUID ;
      // start remove virtual coord job
      rc = startRemoveVirtualCoordJob( _vCoordSvcName.c_str(), this, &jobID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start remove temperary coord job, "
                 "rc = %d", rc ) ;
         goto error ;
      }
      // wait until job is finish
      while ( rtnGetJobMgr()->findJob ( jobID ) )
      {
         ossSleep ( OSS_ONE_SEC ) ;
      } 
      // set task finish or fail
      if ( _isRemoveVCoordFinish && !_isRollbackFail)
      {
         setIsTaskFinish( TRUE ) ;
      }
      else if ( _isRemoveVCoordFail || _isRollbackFail )
      {
         setIsTaskFail( TRUE ) ;
      }
      else
      {
         PD_LOG ( PDERROR, "Task[%s] in a unknown status", taskName() ) ;
#if defined (_DEBUG)
         ossPanic() ;
#endif
         rc = SDB_OMA_TASK_FAIL ;
         goto error ;
      }
   done:
      return rc ;
   error:
      // set remove virtual coord fail detail
      setIsRemoveVCoordFail( TRUE ) ;
      setIsTaskFail( TRUE ) ;
      setErrDetail( "Failed to remove temporary coord, please do it manually" ) ;
      goto done ;
   }

   /*
      remove database business
   */
   _omaRmDBBusTask::_omaRmDBBusTask( UINT64 taskID )
   : _omaTask( taskID )
   {
      _taskType             = OMA_TASK_REMOVE_DB ;
      _taskName             = OMA_TASK_NAME_REMOVE_DB_BUSINESS ;
      _vCoordSvcName        = "" ;
      _isStandalone         = FALSE ;
      _isTaskFinish         = FALSE ;
      _isUninstallFinish    = FALSE ;
      _isRemoveVCoordFinish = FALSE ;
      _isTaskFail           = FALSE ;
      _isUninstallFail      = FALSE ;
      _isRemoveVCoordFail   = FALSE ;
      ossMemset( _detail, 0, OMA_BUFF_SIZE + 1 ) ;
   }

   _omaRmDBBusTask::~_omaRmDBBusTask()
   {
   }

   INT32 _omaRmDBBusTask::init( BOOLEAN isStandalone,
                                map<string, BSONObj> standalone,
                                map<string, BSONObj> coord,
                                map<string, BSONObj> catalog,
                                map<string, BSONObj> data,
                                BSONObj &other )
   {
      INT32 rc = SDB_OK ;
      map<string, BSONObj>::iterator it ;
      _isStandalone = isStandalone ;
      _cataAddrInfo = other.getOwned() ;
      // in case of standalone
      if ( isStandalone )
      {
         // init _standalone and _standaloneResult
         _standalone = standalone ;
         _standaloneResult._rc = SDB_OK ;
         _standaloneResult._totalNum = 1 ;
         _standaloneResult._finishNum = 0 ;
      }
      else // in case of cluster
      {
         // init _coord and _coordResult
         _coord = coord ;
         _coordResult._rc = SDB_OK ;
         _coordResult._totalNum = 1 ;
         _coordResult._finishNum = 0 ;
         // init _catalog and _catalogResult
         _catalog = catalog ;
         _catalogResult._rc = SDB_OK ;
         _catalogResult._totalNum = 1 ;
         _catalogResult._finishNum = 0 ;
         // init _data and _mapGroupsResult
         _data = data ;
         it = _data.begin() ;
         while ( it != _data.end() )
         {
            string groupname = it->first ;
            UninstallResult result ;
            result._rc = 0 ;
            result._totalNum = 1 ;
            result._finishNum = 0 ;
            _mapDataResult.insert( 
               pair<string, UninstallResult>( groupname, result ) ) ;
            it++ ;
         }
      }
      return rc ;
   }

   INT32 _omaRmDBBusTask::doit()
   {
      INT32 rc = SDB_OK ;
      BOOLEAN hasVCoordCreated = FALSE ;
      // in case of standalone
      if ( _isStandalone )
      {
         rc = _uninstallStandalone() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to remove standalone, rc = %d", rc ) ;
            goto error ;
         }
      }
      else // in case of cluster
      { 
         // create virtual coord
         rc = _installVirtualCoord() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to create virtual coord, rc = %d", rc ) ;
            goto error ;
         }
         hasVCoordCreated = TRUE ;
         // remove data group 
         rc = _uninstallData() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to remove data groups, rc = %d", rc ) ;
            goto error ;
         }
         // remove coord
         rc = _uninstallCoord() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to remove coord group, rc = %d", rc ) ;
            goto error ;
         }
         // remove catalog
         rc = _uninstallCatalog() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to remove catalog group, rc = %d", rc ) ;
            goto error ;
         }
      }
      setIsUninstallFinish( TRUE ) ;
   done:
      if ( _isStandalone )
      {
         if ( getIsUninstallFinish() )
         {
            setIsTaskFinish( TRUE ) ;
         }
      }
      else
      {
         // remove virtual coord
         if ( hasVCoordCreated )
         {
            rc = _removeVirtualCoord() ;
            if ( rc )
            {
               PD_LOG( PDERROR, "Failed to remove virtual coord, "
                       "rc = %d", rc ) ;
            }
         }
      }
      return rc ;
   error:
      setIsUninstallFail( TRUE ) ;
      setIsTaskFail( TRUE ) ;
      if ( '\0' == _detail[0] )
      {
         const CHAR *pErrMsg = NULL ;
         pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         if ( NULL == pErrMsg )
         {
            pErrMsg = "" ;
         }
         setErrDetail( pErrMsg ) ;
      }
      goto done ;
   }

   void _omaRmDBBusTask::setIsTaskFail( BOOLEAN isFail )
   {
      _isTaskFail = isFail ;
   }

   void _omaRmDBBusTask::setIsUninstallFail( BOOLEAN isFail )
   {
      _isUninstallFail = isFail ;
   }
   
   void _omaRmDBBusTask::setIsRemoveVCoordFail( BOOLEAN isFail )
   {
      _isRemoveVCoordFail = isFail ;
   }

   void _omaRmDBBusTask::setIsTaskFinish( BOOLEAN isFinish )
   {
      _isTaskFinish = isFinish ;
   }

   void _omaRmDBBusTask::setIsUninstallFinish( BOOLEAN isFinish )
   {
      _isUninstallFinish = isFinish ;
   }
   
   void _omaRmDBBusTask::setIsRemoveVCoordFinish( BOOLEAN isFinish )
   {
      _isRemoveVCoordFinish = isFinish;
   }

   BOOLEAN _omaRmDBBusTask::getIsTaskFail()
   {
      return _isTaskFail ;
   }

   BOOLEAN _omaRmDBBusTask::getIsUninstallFail()
   {
      return _isUninstallFail ;
   }

   BOOLEAN _omaRmDBBusTask::getIsRemoveVCoordFail()
   {
      return _isRemoveVCoordFail ;
   }

   BOOLEAN _omaRmDBBusTask::getIsTaskFinish()
   {
      return _isTaskFinish ;
   }

   BOOLEAN _omaRmDBBusTask::getIsUninstallFinish()
   {
      return _isUninstallFinish ;
   }

   BOOLEAN _omaRmDBBusTask::getIsRemoveVCoordFinish()
   {
      return _isRemoveVCoordFinish ;
   }

   void _omaRmDBBusTask::setErrDetail( const CHAR *pErrDetail )
   {
      ossSnprintf( _detail, OMA_BUFF_SIZE, pErrDetail ) ;
   }

   INT32 _omaRmDBBusTask::_updateUninstallStatus( BOOLEAN isFinish,
                                                  INT32 retRc,
                                                  const CHAR *pRole,
                                                  const CHAR *pErrMsg,
                                                  const CHAR *pDesc,
                                                  const CHAR *pGroupName )
   {
      INT32 rc = SDB_OK ;
      // check argument
      if ( NULL == pRole )
      {
         PD_LOG_MSG ( PDERROR, "Not speciefy role for "
                      "updating uninstall status" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( ( 0 == ossStrncmp( pRole, ROLE_DATA, ossStrlen( ROLE_DATA ) ) ) &&
           ( NULL == pGroupName ) )
      {
         PD_LOG_MSG ( PDERROR, "Not speciefy data group "
                      "for updating uninstall status" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( NULL == pErrMsg ) pErrMsg = "" ;
      if ( NULL == pDesc ) pDesc = "" ;
      if ( NULL == pGroupName ) pGroupName = "" ;

      // update the remove result
      if ( 0 == ossStrncmp( pRole, ROLE_DATA, ossStrlen( ROLE_DATA ) ) )
      {
         map<string, UninstallResult>::iterator it ; 
         string groupname = pGroupName ;
         it = _mapDataResult.find( groupname ) ;
         if ( it != _mapDataResult.end() )
         {
            UninstallResult &result = it->second ;
            result._desc = pDesc ;
            if ( retRc )
            {
               result._rc = retRc ;
               result._errMsg = pErrMsg ;
               goto done ;
            }
            if ( isFinish )
            {
               result._finishNum++ ;
            }
         }
      }
      else if ( 0 == ossStrncmp( pRole, ROLE_COORD,
                                 ossStrlen( ROLE_COORD ) ) )
      {
         _coordResult._desc = pDesc ;
         if ( retRc )
         {
            _coordResult._rc = retRc ;
            _coordResult._errMsg = pErrMsg ;
            goto done ;
         }
         if ( isFinish )
         {
            _coordResult._finishNum++ ;
         }
      }
      else if ( 0 == ossStrncmp( pRole, ROLE_CATA,
                                 ossStrlen( ROLE_CATA ) ) )
      {
         _catalogResult._desc = pDesc ;
         if ( retRc )
         {
            _catalogResult._rc = retRc ;
            _catalogResult._errMsg = pErrMsg ;
            goto done ;
         }
         if ( isFinish )
         {
            _catalogResult._finishNum++ ;
         }
      }
      else if ( 0 == ossStrncmp( pRole, ROLE_STANDALONE,
                                 ossStrlen( ROLE_STANDALONE) ) )
      {
         _standaloneResult._desc = pDesc ;
         if ( retRc )
         {
            _standaloneResult._rc = retRc ;
            _standaloneResult._errMsg = pErrMsg ;
            goto done ;
         }
         if ( isFinish )
         {
            _standaloneResult._finishNum++ ;
         }
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Invalid role for updating uninstall status" ) ;
         goto error ;
      }
      // check whether uninstall is finish or not
      if ( _isRemoveFinish() )
      {
         setIsTaskFinish( TRUE ) ;
         goto done ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN _omaRmDBBusTask::_isRemoveFinish ()
   {
      // in case of standalone
      if ( _isStandalone )
      {
         if ( _standaloneResult._totalNum == _standaloneResult._finishNum )
         {
            return TRUE ;
         }
         else
         {
            return FALSE ;
         }
      }
      else // in case of cluster
      {
         map<string, UninstallResult>::iterator it ;
         if ( _catalogResult._totalNum > _catalogResult._finishNum )
         {
            return FALSE ;
         }
         if ( _coordResult._totalNum > _coordResult._finishNum )
         {
            return FALSE ;
         }
         it = _mapDataResult.begin() ;
         while( it != _mapDataResult.end() )
         {
            UninstallResult &result = it->second ;
            if ( result._totalNum > result._finishNum )
            {
               return FALSE ;
            }
            it++ ;
         }
         return TRUE ;
      }
   }

   INT32 _omaRmDBBusTask::queryProgress ( BSONObj &progress )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      BSONObj standaloneResult ;
      BSONObj coordResult ;
      BSONObj catalogResult ;
      const CHAR *pStage = STAGE_UNINSTALL ;
      
      // while task has failed
      if ( getIsTaskFail() )
      {
         if ( '\0' == _detail[0] )
         {
            PD_LOG_MSG ( PDERROR,"Task[%s] had failed, please check "
                         "the dialog for more detail", taskName() ) ;
         }
         else
         {
            PD_LOG_MSG ( PDERROR, _detail ) ;
         }
         rc = SDB_OMA_TASK_FAIL ;
         goto done ;
      }
      try
      {
         // taskID
         bob.append( OMA_FIELD_TASKID, (SINT64)_taskID ) ;
         // isFinish
         bob.appendBool( OMA_FIELD_ISFINISH, _isTaskFinish ) ;
         // status
         bob.append( OMA_FIELD_STATUS, pStage ) ;

         bob.append( OMA_FIELD_ERRMSG, _detail ) ;
         // in case of standalone
         if ( _isStandalone )
         {
            // get standalone status
            if ( _standaloneResult._rc )
            {
               bob.append( OMA_FIELD_ERRMSG, _standaloneResult._errMsg ) ;
            }
            standaloneResult = BSON ( OMA_FIELD_NAME
                                      << OMA_FIELD_STANDALONE
                                      << OMA_FIELD_TOTALCOUNT
                                      << _standaloneResult._totalNum
                                      << OMA_FIELD_UNINSTALLEDCOUNT
                                      << _standaloneResult._finishNum
                                      << OMA_FIELD_DESC
                                      << _standaloneResult._desc.c_str() ) ;
            bab.append ( standaloneResult ) ;
         }
         else // in case of cluster
         {
            // get catalog status
            if ( _catalogResult._rc )
            {
               bob.append( OMA_FIELD_ERRMSG, _catalogResult._errMsg ) ;
            }
            catalogResult = BSON ( OMA_FIELD_NAME
                                   << OMA_FIELD_CATALOG
                                   << OMA_FIELD_TOTALCOUNT
                                   << _catalogResult._totalNum
                                   << OMA_FIELD_UNINSTALLEDCOUNT
                                   << _catalogResult._finishNum
                                   << OMA_FIELD_DESC
                                   << _catalogResult._desc.c_str() ) ;
            bab.append ( catalogResult ) ;
            // get coord status
            if ( ( _coordResult._rc ) && ( !bob.hasField(OMA_FIELD_ERRMSG) ) )
            {
               bob.append( OMA_FIELD_ERRMSG, _coordResult._errMsg ) ;
            }
            coordResult = BSON ( OMA_FIELD_NAME
                                 << OMA_FIELD_COORD
                                 << OMA_FIELD_TOTALCOUNT
                                 << _coordResult._totalNum
                                 << OMA_FIELD_UNINSTALLEDCOUNT
                                 << _coordResult._finishNum
                                 << OMA_FIELD_DESC
                                 << _coordResult._desc.c_str() ) ;
            bab.append ( coordResult ) ;
            // get data group status
            std::map< string, UninstallResult >::iterator it ;
            it = _mapDataResult.begin() ;
            while ( it != _mapDataResult.end() )
            {
               string groupname = it->first ;
               UninstallResult &result = it->second ;
               BSONObj groupResult ;
               if ( ( result._rc ) && ( !bob.hasField(OMA_FIELD_ERRMSG) ) )
               {
                  bob.append( OMA_FIELD_ERRMSG, result._errMsg ) ;
               }
               groupResult = BSON ( OMA_FIELD_NAME
                                    << groupname.c_str()
                                    << OMA_FIELD_TOTALCOUNT
                                    << result._totalNum
                                    << OMA_FIELD_UNINSTALLEDCOUNT
                                    << result._finishNum
                                    << OMA_FIELD_DESC
                                    << result._desc.c_str() ) ;
               bab.append ( groupResult ) ;
               it++ ;
            }
         }
         // try to set ErrMsg
         if ( !(bob.hasField( OMA_FIELD_ERRMSG ) ) )
         {
            bob.append( OMA_FIELD_ERRMSG, "" );
         }
         // set return result
         bob.appendArray( OMA_FIELD_PROGRESS, bab.arr() ) ;
         progress = bob.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR,
                  "Failed to get remove db business progress: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaRmDBBusTask::_saveVCoordInfo( BSONObj &info )
   {
      INT32 rc                    = SDB_OK ;
      const CHAR *pVCoordSvcName  = NULL ;
      rc = omaGetStringElement( info, OMA_FIELD_VCOORDSVCNAME, &pVCoordSvcName ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get filed[%s], rc = %s",
                  OMA_FIELD_VCOORDSVCNAME, rc ) ;
         goto error ;
      }
      _vCoordSvcName = pVCoordSvcName ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaRmDBBusTask::_installVirtualCoord()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      const CHAR *pErrMsg = NULL ;
      CHAR detail[OMA_BUFF_SIZE + 1] = { 0 } ;
      BSONObj vCoordRet ;
      _omaCreateVirtualCoord vCoord ;
      
      // create virtual coord and save it's info for future
      rc = vCoord.init( _cataAddrInfo.objdata() ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init for creating "
                  "temporary coord, rc = %d", rc ) ;
         goto error ;
      }
      rc = vCoord.doit( vCoordRet ) ;
      if ( rc )
      {
         // if we can't get field "detail", it means we failed in CPP,
         // we had not executed js file yet
         tmpRc = omaGetStringElement ( vCoordRet, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( tmpRc )
         {
            pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pErrMsg )
            {
               pErrMsg = "" ;
            }
         }
         ossSnprintf( detail, OMA_BUFF_SIZE, "Failed to create temporary "
                      "coord: %s", pErrMsg ) ;
         PD_LOG_MSG( PDERROR, detail ) ;
         goto error ;
      }
      rc = _saveVCoordInfo( vCoordRet ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to save temporary coord install result, "
                      "rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaRmDBBusTask::_uninstallStandalone()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      BSONObj retObj ;
      const CHAR *pInfo = NULL ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = "" ;
      _omaRmStandalone rmSa ;
      map<string, BSONObj>::iterator it = _standalone.begin() ;
      if ( it != _standalone.end() )
      {
         pInfo = it->second.objdata() ;
      }
      else
      {
         PD_LOG_MSG( PDERROR, "No standalone's info for removing" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }      
      // update status for web before remove catalog
      ossSnprintf( desc, OMA_BUFF_SIZE, "Removing standalone" ) ;
      rc = _updateUninstallStatus( FALSE, SDB_OK, ROLE_STANDALONE,
                                   NULL, desc, NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to update status before remove standalone, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      // remove standalone
      rc = rmSa.init( pInfo ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to remove standalone "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = rmSa.doit( retObj ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to remove standalone, rc = %d", rc ) ;
         // if we can't get field "detail", it means we failed in CPP,
         // we had not executed js file yet
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
                      "Failed to remove standalone" ) ;
         _updateUninstallStatus( FALSE, rc, ROLE_STANDALONE,
                                 pErrMsg, desc, NULL ) ;
         PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
         _updateUninstallStatus( FALSE, rc, ROLE_STANDALONE,
                                 pErrMsg, desc, NULL ) ;
         goto error ;
      }
      else
      {
         PD_LOG ( PDEVENT, "The remove standalone's result is: %s",
                  retObj.toString(FALSE, TRUE).c_str() ) ;
         ossSnprintf( desc, OMA_BUFF_SIZE,
                      "Finish removing standalone" ) ;
         PD_LOG ( PDEVENT, "Succeed to remove standalone" ) ;
         _updateUninstallStatus( TRUE, SDB_OK, ROLE_STANDALONE,
                                 NULL, desc, NULL ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaRmDBBusTask::_uninstallCatalog()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      BSONObj retObj ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = "" ;
      const CHAR *pInfo = NULL ;
      _omaRmCataRG rmCata( _vCoordSvcName ) ;
      map<string, BSONObj>::iterator it = _catalog.begin() ;
      if ( it != _catalog.end() )
      {
         pInfo = it->second.objdata() ;
      }
      else
      {
         PD_LOG_MSG( PDERROR, "No catalog's info for removing" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      // update status for web before remove catalog
      ossSnprintf( desc, OMA_BUFF_SIZE, "Removing catalog group" ) ;
      rc = _updateUninstallStatus( FALSE, SDB_OK, ROLE_CATA,
                                   NULL, desc, NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to update status before remove catalog "
                  "group, rc = %d", rc ) ;
         goto error ;
      }
      // remove catalog
      rc = rmCata.init( pInfo ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to remove catalog "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = rmCata.doit( retObj ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to remove catalog group, rc = %d", rc ) ;
         // if we can't get field "detail", it means we failed in CPP,
         // we had not executed js file yet
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
                      "Failed to remove catalog group" ) ;
         PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
         _updateUninstallStatus( FALSE, rc, ROLE_CATA,
                                 pErrMsg, desc, NULL ) ;
         goto error ;
      }
      else
      {
         PD_LOG ( PDEVENT, "The remove catalog's result is: %s",
                  retObj.toString(FALSE, TRUE).c_str() ) ;
         ossSnprintf( desc, OMA_BUFF_SIZE,
                      "Finish removing catalog group" ) ;
         PD_LOG ( PDEVENT, "Succeed to install catalog group" ) ;
         _updateUninstallStatus( TRUE, SDB_OK, ROLE_CATA,
                                 pErrMsg, desc, NULL ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaRmDBBusTask::_uninstallCoord()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      BSONObj retObj ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = "" ;
      const CHAR *pInfo = NULL ;
      _omaRmCoordRG rmCoord( _vCoordSvcName ) ;
      map<string, BSONObj>::iterator it = _coord.begin() ;
      if ( it != _coord.end() )
      {
         pInfo = it->second.objdata() ;
      }
      else
      {
         PD_LOG_MSG( PDWARNING, "No coord's info for removing" ) ;
         goto done ;
      }
      // update status for web before remove coord
      ossSnprintf( desc, OMA_BUFF_SIZE, "Removing coord group" ) ;
      rc = _updateUninstallStatus( FALSE, SDB_OK, ROLE_COORD,
                                   NULL, desc, NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to update status before remove coord "
                  "group, rc = %d", rc ) ;
         goto error ;
      }
      // remove coord
      rc = rmCoord.init( pInfo ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to init to remove coord group"
                      "rc = %d", rc ) ;
         goto error ;
      }
      rc = rmCoord.doit( retObj ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to remove coord group, rc = %d", rc ) ;
         // if we can't get field "detail", it means we failed in CPP,
         // we had not executed js file yet
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
                      "Failed to remove coord group" ) ;
         PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
         _updateUninstallStatus( FALSE, rc, ROLE_COORD, pErrMsg, desc, NULL ) ;
         goto error ;
      }
      else
      {
         PD_LOG ( PDEVENT, "The remove coord's result is: %s",
                  retObj.toString(FALSE, TRUE).c_str() ) ;
         ossSnprintf( desc, OMA_BUFF_SIZE,
                      "Finish removing coord group" ) ;
         PD_LOG ( PDEVENT, "Succeed to install coord group" ) ;
         _updateUninstallStatus( TRUE, SDB_OK, ROLE_COORD, NULL, desc, NULL ) ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaRmDBBusTask::_uninstallData()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;   
      BSONObj retObj ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = "" ;
      const CHAR *pInfo = NULL ;
      map<string, BSONObj>::iterator it = _data.begin() ;
      if ( it == _data.end() )
      {
         PD_LOG_MSG( PDWARNING, "No data group's info for removing" ) ;
         goto done ;
      }
      for( ; it != _data.end(); it++ )
      {
         _omaRmDataRG rmData( _vCoordSvcName ) ;
         pInfo = it->second.objdata() ;
         // update status for web before remove data group
         ossSnprintf( desc, OMA_BUFF_SIZE, "Removing data group[%s]",
                      it->first.c_str() ) ;
         rc = _updateUninstallStatus( FALSE, SDB_OK, ROLE_DATA,
                                      NULL, desc, it->first.c_str() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to update status before remove data "
                     "group[%s], rc = %d", it->first.c_str(), rc ) ;
            goto error ;
         }
         // remove data rg
         rc = rmData.init( pInfo ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to init to remove data group[%s] "
                         "rc = %d", it->first.c_str(), rc ) ;
            goto error ;
         }
         rc = rmData.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to remove data group[%s], rc = %d",
                    it->first.c_str(), rc ) ;
            // if we can't get field "detail", it means we failed in CPP,
            // we had not executed js file yet
            tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
            if ( tmpRc )
            {
               pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               if ( NULL == pErrMsg )
               {
                  pErrMsg = "" ;
               }
            }
            ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to remove data"
                         "group[%s]", it->first.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            _updateUninstallStatus( FALSE, rc, ROLE_DATA,
                                    pErrMsg, desc, it->first.c_str() ) ;
            goto error ;
         }
         else
         {
            PD_LOG ( PDEVENT, "The remove data group[%s]'s result is: %s",
                     it->first.c_str(), retObj.toString(FALSE, TRUE).c_str() ) ;
            ossSnprintf( desc, OMA_BUFF_SIZE,
                         "Finish removing data group[%s]", it->first.c_str() ) ;
            PD_LOG ( PDEVENT, "Succeed to remove data group[%s]",
                     it->first.c_str() ) ;
            _updateUninstallStatus( TRUE, SDB_OK, ROLE_DATA,
                                    NULL, desc, it->first.c_str() ) ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaRmDBBusTask::_removeVirtualCoord()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      CHAR detail[OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = NULL ;
      BSONObj removeRet ;
      _omaRemoveVirtualCoord removeVCoord( _vCoordSvcName.c_str() ) ;
      rc = removeVCoord.removeVirtualCoord ( removeRet ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to remove temporary coord in remove "
                  "db business task, rc = %d", rc ) ;
         // if we can't get field "detail", it means we failed in CPP,
         // we had not executed js file yet
         tmpRc = omaGetStringElement( removeRet, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( tmpRc )
         {
            pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pErrMsg )
            {
               pErrMsg = "" ;
            }
         }
         ossSnprintf( detail, OMA_BUFF_SIZE, "Failed to remove temporary "
                      "coord: %s", pErrMsg ) ;
         // set remove temp coord to be failing
         setIsRemoveVCoordFail( TRUE ) ;
         // set error detail
         setErrDetail( detail ) ;
         goto error ;
      }
      else
      {
         PD_LOG ( PDEVENT, "Succeed to remove temporary coord" ) ;
         setIsRemoveVCoordFinish( TRUE ) ;
      } 
      
      // set task finish or fail
      if ( _isRemoveVCoordFinish && !_isUninstallFail )
      {
         setIsTaskFinish( TRUE ) ;
      }
      else if ( _isRemoveVCoordFail || _isUninstallFail )
      {
         setIsTaskFail( TRUE ) ;
      }
      else
      {
         PD_LOG ( PDERROR, "Task[%s] in a unknown status", taskName() ) ;
#if defined (_DEBUG)
         ossPanic() ;
#endif
         rc = SDB_OMA_TASK_FAIL ;
         goto error ;
      }
   done:
      return rc ;
   error:
      // set remove virtual coord fail detail
      setIsRemoveVCoordFail( TRUE ) ;
      setIsTaskFail( TRUE ) ;
      goto done ;
   }

}
