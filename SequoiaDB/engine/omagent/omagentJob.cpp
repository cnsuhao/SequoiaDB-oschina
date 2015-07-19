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

   Source File Name = omagentJob.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/
#include "omagentUtil.hpp"
#include "omagentJob.hpp"
#include "omagentBackgroundCmd.hpp"
#include "pmdEDU.hpp"

namespace engine
{
   /*
      omagent job
   */
   _omagentJob::_omagentJob ( _omaTask *pTask, const BSONObj &info, void *ptr )
   {
      _pTask   = pTask ;
      _info    = info.copy() ;
      _pointer = ptr ;
      if ( _pTask )
         _jobName = _jobName + "Omagent job for task[" +
                    _pTask->getTaskName() + "]" ;
   }

   _omagentJob::~_omagentJob()
   {
      SAFE_OSS_FREE( _pTask ) ;
   }

   RTN_JOB_TYPE _omagentJob::type () const
   {
      return RTN_JOB_OMAGENT ;
   }

   const CHAR* _omagentJob::name () const
   {
      return _jobName.c_str() ;
   }

   BOOLEAN _omagentJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omagentJob::doit()
   {
      INT32 rc = SDB_OK ;

      if ( NULL == _pTask )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "Invalid task poiter" ) ;
         goto error ;
      }
      rc = _pTask->init( _info, _pointer ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to init in job[%s] for running task[%s], "
                 "rc = %d", _jobName.c_str(), _pTask->getTaskName(), rc ) ;
         goto error ;
      }
      rc = _pTask->doit() ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to do it in job[%s] for running task[%s], "
                 "rc = %d", _jobName.c_str(), _pTask->getTaskName(), rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }





   INT32 startOmagentJob ( OMA_TASK_TYPE taskType, INT64 taskID,
                           const BSONObj &info, void *ptr )
   {
      INT32 rc               = SDB_OK ;
      EDUID eduID            = PMD_INVALID_EDUID ;
      BOOLEAN returnResult   = FALSE ;
      _omagentJob *pJob      = NULL ;
      _omaTask *pTask        = NULL ;

      pTask = getTaskByType( taskType, taskID ) ;
      if ( NULL == pTask )
      {
         PD_LOG( PDERROR, "Unkown task type" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      pJob = SDB_OSS_NEW _omagentJob( pTask, info, ptr ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for running task "
                  "with the type[%d]", taskType ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, &eduID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start task with the type[%d], rc = %d",
                  taskType, rc ) ;
         goto done ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   _omaTask* getTaskByType( OMA_TASK_TYPE taskType, INT64 taskID )
   {
      _omaTask *pTask = NULL ;
      
      switch ( taskType )
      {
         case OMA_TASK_ADD_HOST :
            pTask = SDB_OSS_NEW _omaAddHostTask( taskID ) ;
            break ;
         case OMA_TASK_ADD_HOST_SUB :
            pTask = SDB_OSS_NEW _omaAddHostSubTask( taskID ) ;
            break ;
         case OMA_TASK_REMOVE_HOST :
            pTask = SDB_OSS_NEW _omaRemoveHostTask( taskID ) ;
            break ;
         case OMA_TASK_INSTALL_DB :
            pTask = SDB_OSS_NEW _omaInstDBBusTask( taskID ) ;
            break ;
         case OMA_TASK_INSTALL_DB_SUB :
            pTask = SDB_OSS_NEW _omaInstDBBusSubTask( taskID ) ;
            break ;
         case OMA_TASK_REMOVE_DB :
            pTask= SDB_OSS_NEW _omaRemoveDBBusTask( taskID ) ;
            break ;
         default :
            PD_LOG_MSG( PDERROR, "Unknow task type[%d]", taskType ) ;
            break ;
      }
      if ( NULL == pTask )
      {
         PD_LOG_MSG( PDERROR, "Failed to malloc for task with the type[%d]",
                     taskType ) ;
      }
      return pTask ;
   }

   

}
