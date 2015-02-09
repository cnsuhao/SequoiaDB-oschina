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

   Source File Name = omagentSubTask.cpp

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
#include "pmdDef.hpp"
#include "pmdEDU.hpp"
#include "omagentSubTask.hpp"
#include "omagentAsyncCmd.hpp"


namespace engine
{
   

   /*
      add host sub task
   */
   _omaAddHostSubTask::_omaAddHostSubTask( INT64 taskID )
   : _omaTask( taskID )
   {
      _taskType = OMA_TASK_ADD_HOST_SUB ;
      _taskName = OMA_TASK_NAME_ADD_HOST_SUB ;
   }

   _omaAddHostSubTask::~_omaAddHostSubTask()
   {
   }


   INT32 _omaAddHostSubTask::init( const BSONObj &info, void *ptr )
   {
      INT32 rc = SDB_OK ;
      stringstream ss ;
      _pTask = (_omaAddHostTask *)ptr ;
      if ( NULL == _pTask )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "No add host task's info for "
                     "add host sub task" ) ;
         goto error ;
      }
      ss << _taskName << "[" << _pTask->getSubTaskSerialNum() << "]" ;
      _taskName = ss.str() ;
      
      done:
         return rc ;
      error:
         goto done ;
   }

   INT32 _omaAddHostSubTask::doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;

      _pTask->setSubTaskStatus( _taskName, OMA_TASK_STATUS_RUNNING ) ;
      
      while( TRUE )
      {
         AddHostInfo *pInfo           = NULL ;
         AddHostResultInfo resultInfo = { "", "", OMA_TASK_STATUS_INIT,
                                          OMA_TASK_STATUS_DESC_INIT,
                                          SDB_OK, "" } ;
         CHAR flow[OMA_BUFF_SIZE + 1] = { 0 } ;
         const CHAR *pDetail          = NULL ;
         const CHAR *pIP              = NULL ;
         const CHAR *pHostName        = NULL ;
         INT32 errNum                 = 0 ;
         BSONObj retObj ;

         pInfo = _pTask->getAddHostItem() ;
         if ( NULL == pInfo )
         {
            PD_LOG( PDEVENT, "No hosts need to add now, sub task[%s] exits",
                    _taskName.c_str() ) ;
            goto done ;
         }

         pIP                  = pInfo->_item._ip.c_str() ;
         pHostName            = pInfo->_item._hostName.c_str() ;
         resultInfo._ip       = pIP ;
         resultInfo._hostName = pHostName ;

         ossSnprintf( flow, OMA_BUFF_SIZE, "Adding host[%s]", pIP ) ;
         resultInfo._status     = OMA_TASK_STATUS_RUNNING ;
         resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_RUNNING ) ;
         resultInfo._errno      = SDB_OK ;
         resultInfo._detail     = "" ;
         resultInfo._flow.push_back( flow ) ;
         rc = _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
         if ( rc )
         {
            PD_LOG( PDWARNING, "Failed to update add host[%s]'s progress, "
                    "rc = %d", pIP, rc ) ;
         }

         _omaRunAddHost runCmd( *pInfo ) ;
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to init for adding "
                    "host[%s], rc = %d", pIP, rc ) ;
            pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pDetail || 0 == *pDetail )
               pDetail = "Failed to init for adding host " ;
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to add host[%s]", pIP ) ;
            resultInfo._status     = OMA_TASK_STATUS_FINISH ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
            resultInfo._errno      = rc ;
            resultInfo._detail     = pDetail ;
            resultInfo._flow.push_back( flow ) ;
            rc = _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
            if ( rc )
            {
               PD_LOG( PDWARNING, "Failed to update add host[%s]'s progress, "
                       "rc = %d", pIP, rc ) ;
            }
            continue ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to do adding host[%s], rc = %d", pIP, rc ) ;
            tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pDetail ) ;
            if ( SDB_OK != tmpRc )
            {
               pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               if ( NULL == pDetail || 0 == *pDetail )
                  pDetail = "Not exeute js file yet" ;
            }
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to add host[%s]", pIP ) ;
            resultInfo._status     = OMA_TASK_STATUS_FINISH ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
            resultInfo._errno      = rc ;
            resultInfo._detail     = pDetail ;
            resultInfo._flow.push_back( flow ) ;
            _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
            if ( rc )
            {
               PD_LOG( PDWARNING, "Failed to update add host[%s]'s progress, "
                       "rc = %d", pIP, rc ) ;
            }
            continue ;
         }
         rc = omaGetIntElement ( retObj, OMA_FIELD_ERRNO, errNum ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to get errno from js after "
                    "adding host[%s], rc = %d", pIP, rc ) ;
            pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pDetail || 0 == *pDetail )
               pDetail = "Failed to get errno from js after adding host" ;
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to add host[%s]", pIP ) ;
            resultInfo._status     = OMA_TASK_STATUS_FINISH ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
            resultInfo._errno      = rc ;
            resultInfo._detail     = pDetail ;
            resultInfo._flow.push_back( flow ) ;
            rc =_pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
            if ( rc )
            {
               PD_LOG( PDWARNING, "Failed to update add host[%s]'s progress, "
                       "rc = %d", pIP, rc ) ;
            }
            continue ;
         }
         if ( SDB_OK != errNum )
         {
            rc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pDetail ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "Failed to get error detail from js after "
                       "adding host[%s], rc = %d", pIP, rc ) ;
               pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               if ( NULL == pDetail || 0 == *pDetail )
                  pDetail = "Failed to get error detail from js after adding host" ;
            }
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to add host[%s]", pIP ) ;
            resultInfo._status     = OMA_TASK_STATUS_FINISH ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
            resultInfo._errno      = errNum ;
            resultInfo._detail     = pDetail ;
            resultInfo._flow.push_back( flow ) ;
            rc = _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
            if ( rc )
            {
               PD_LOG( PDWARNING, "Failed to update add host[%s]'s progress, "
                       "rc = %d", pIP, rc ) ;
            }
            continue ;
         }
         else
         {
            ossSnprintf( flow, OMA_BUFF_SIZE, "Finish adding host[%s]", pIP ) ;
            PD_LOG ( PDEVENT, "Success to add host[%s]", pIP ) ;
            resultInfo._status     = OMA_TASK_STATUS_FINISH ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
            resultInfo._flow.push_back( flow ) ;
            rc = _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
            if ( rc )
            {
               PD_LOG( PDWARNING, "Failed to update add host[%s]'s progress, "
                       "rc = %d", pIP, rc ) ;
            }
         }
      }

   done:
      _pTask->setSubTaskStatus( _taskName, OMA_TASK_STATUS_FINISH ) ;
      _pTask->notifyUpdateProgress() ;
      return SDB_OK ;
   }

/*
   INT32 doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;

      BOOLEAN flag = _pTask->regSubTask( _taskName ) ;
      if ( !flag )
      {
         PD_LOG ( PDWARNING, "No need to run sub task[%s], some other sub task "
                  "had failed", _taskName.c_str() ) ;
         goto done ;
      }
      
      while( TRUE )
      {
         AddHostInfo *pInfo           = NULL ;
         AddHostResultInfo resultInfo = { "", "", OMA_TASK_STATUS_INIT,
                                          OMA_TASK_STATUS_DESC_INIT,
                                          SDB_OK, "" } ;
         CHAR flow[OMA_BUFF_SIZE + 1] = { 0 } ;
         const CHAR *pDetail          = NULL ;
         INT32 errNum                 = 0 ;
         BSONObj retObj ;

         pInfo = _pTask->getAddHostItem() ;
         if ( NULL == pInfo )
         {
            _pTask->setSubTaskStatus( _taskName, OMA_TASK_STATUS_FINISH ) ;
            goto done ;
         }

         if ( OMA_TASK_STATUS_FAIL == _pTask->getTaskStatus() )
         {
            rc = SDB_OMA_TASK_FAIL ;
            PD_LOG ( PDEVENT, "Stop running sub task[%s], for some other "
                     "sub task had failed", _taskName.c_str() ) ;
            goto error ;
         }
         ossSnprintf( flow, OMA_BUFF_SIZE, "Adding host[%s]",
                      pInfo->_item._ip.c_str() ) ;
         resultInfo._ip         = pInfo->_item._ip ;
         resultInfo._hostName   = pInfo->_item._hostName ;
         resultInfo._status     = OMA_TASK_STATUS_RUNNING ;
         resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_RUNNING ) ;
         resultInfo._flow.push_back( flow ) ;
         rc = _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to update add host[%s] progress, "
                    "rc = %d", pInfo->_item._ip.c_str(), rc ) ;
            goto error ;
         }

         _omaRunAddHost runCmd( *pInfo ) ;
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Sub task[%s] failed to init for adding host[%s], "
                    "rc = %d", _taskName.c_str(), pInfo->_item._ip.c_str(), rc ) ;
            goto error ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to add host[%s], "
                    "rc = %d", pInfo->_item._ip.c_str(), rc ) ;
            tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pDetail ) ;
            if ( SDB_OK != tmpRc )
            {
               pDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               if ( NULL == pDetail )
               {
                  pDetail = "Not exeute js file yet" ;
               }
            }
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to add host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", flow, pDetail ) ;
            resultInfo._status     = OMA_TASK_STATUS_FAIL ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FAIL ) ;
            resultInfo._errno      = rc ;
            resultInfo._detail     = pDetail ;
            resultInfo._flow.push_back( flow ) ;
            _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
            goto error ;
         }
         rc = omaGetIntElement ( retObj, OMA_FIELD_ERRNO, errNum ) ;
         if ( rc )
         {
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to add host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            pDetail = "Failed to get return error number" ;
            PD_LOG_MSG ( PDERROR, "%s: %s", flow, pDetail ) ;
            resultInfo._status     = OMA_TASK_STATUS_FAIL ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FAIL ) ;
            resultInfo._errno      = rc ;
            resultInfo._detail     = pDetail ;
            resultInfo._flow.push_back( flow ) ;
            _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
            goto error ;
         }
         if ( SDB_OK != errNum )
         {
            rc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pDetail ) ;
            if ( SDB_OK != rc )
            {
               pDetail = "Js not return error detail" ;
            }
            ossSnprintf( flow, OMA_BUFF_SIZE, "Failed to add host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", flow, pDetail ) ;
            resultInfo._status     = OMA_TASK_STATUS_FAIL ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FAIL ) ;
            resultInfo._errno      = errNum ;
            resultInfo._detail     = pDetail ;
            resultInfo._flow.push_back( flow ) ;
            _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
            goto error ;
         }
         else
         {
            ossSnprintf( flow, OMA_BUFF_SIZE, "Finish adding host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG ( PDEVENT, "Success to add host[%s]",
                     pInfo->_item._ip.c_str() ) ;
            resultInfo._status     = OMA_TASK_STATUS_FINISH ;
            resultInfo._statusDesc = getTaskStatusDesc( OMA_TASK_STATUS_FINISH ) ;
            resultInfo._flow.push_back( flow ) ;
            _pTask->updateProgressToTask( pInfo->_serialNum, resultInfo ) ;
         }
      }

   done:
      return rc ;
   error:
      _pTask->setSubTaskStatus( _taskName, OMA_TASK_STATUS_FAIL ) ;
      _pTask->setTaskStatus( OMA_TASK_STATUS_FAIL ) ;
      goto done ;
   }
*/

}  // namespace engine
