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
#include "omagentAsyncCmd.hpp"
#include "pmdEDU.hpp"

namespace engine
{

/*


   _omaAddHostJob::_omaAddHostJob ( string jobName, _omaAddHostTask *pTask )
   {
      _jobName = jobName ;
      _pTask = pTask ;
   }

   _omaAddHostJob::~_omaAddHostJob()
   {
   }

   RTN_JOB_TYPE _omaAddHostJob::type () const
   {
      return RTN_JOB_ADDHOST ;
   }

   const CHAR* _omaAddHostJob::name () const
   {
      return _jobName.c_str() ;
   }

   BOOLEAN _omaAddHostJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaAddHostJob::doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;

      BOOLEAN flag = _pTask->registerJob( _jobName ) ;
      if ( !flag )
      {
         PD_LOG ( PDWARNING, "No need to run job[%s], some other jobs had "
                  "failed", _jobName.c_str() ) ;
         goto done ;
      }
      
      while( TRUE )
      {
         CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
         const CHAR *pErrMsg = NULL ;
         AddHostInfo *pInfo = NULL ;
         AddHostPS ps = { 0, "", "", FALSE } ;
         INT32 errNum = 0 ;
         BSONObj retObj ;

         pInfo = _pTask->getAddHostItem() ;
         if ( NULL == pInfo )
         {
            _pTask->updateJobStatus( _jobName, OMA_JOB_STATUS_FINISH ) ;
            goto done ;
         }

         if ( _pTask->getIsAddHostFail() )
         {
            rc = SDB_SYS ;
            PD_LOG ( PDEVENT, "Stop running job[%s], for some other jobs"
                     "had failed", _jobName.c_str() ) ;
            goto error ;
         }
         ossSnprintf( desc, OMA_BUFF_SIZE, "Adding host[%s]",
                      pInfo->_item._ip.c_str() ) ;
         ps._desc = desc ;
         rc = _pTask->updateProgressStatus( pInfo->_serialNum, ps ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to update add host[%s] progress status, "
                    "rc = %d", pInfo->_item._ip.c_str(), rc ) ;
            goto error ;
         }

         _omaRunAddHost runCmd( *pInfo ) ;
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to init for adding host[%s], "
                    "rc = %d", pInfo->_item._ip.c_str(), rc ) ;
            goto error ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to add host[%s], "
                    "rc = %d", pInfo->_item._ip.c_str(), rc ) ;
            tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
            if ( tmpRc )
            {
               pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               if ( NULL == pErrMsg )
               {
                  pErrMsg = "Not exeute js file yet" ;
               }
            }
            ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to add host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            ps._errno = rc ;
            ps._errMsg = pErrMsg ;
            ps._desc = desc ;
            ps._hasInstall = FALSE ;
            _pTask->updateProgressStatus( pInfo->_serialNum, ps, TRUE ) ;
            goto error ;
         }
         rc = omaGetIntElement ( retObj, OMA_FIELD_ERRNO, errNum ) ;
         if ( rc )
         {
            ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to add host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            pErrMsg = "Failed to get return error number" ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            ps._errno = rc ;
            ps._errMsg = pErrMsg ;
            ps._desc = desc ;
            ps._hasInstall = FALSE ;
            _pTask->updateProgressStatus( pInfo->_serialNum, ps, TRUE ) ;
            goto error ;
         }
         if ( SDB_OK != errNum )
         {
            rc = omaGetBooleanElement ( retObj, OMA_FIELD_HASINSTALL,
                                        ps._hasInstall ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG ( PDWARNING, "Failed to add host[%s], but js file didn't"
                        "tell wether db packet had been installed or not, "
                        "assume it had not been installed, rc = %d",
                        pInfo->_item._ip.c_str(), tmpRc ) ;
               ps._hasInstall = FALSE ;
            }
            rc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
            if ( SDB_OK != rc )
            {
               pErrMsg = "Js not return error detail" ;
            }
            ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to add host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            ps._errno = errNum ;
            ps._errMsg = pErrMsg ;
            ps._desc = desc ;
            _pTask->updateProgressStatus( pInfo->_serialNum, ps, TRUE ) ;
            goto error ;
         }
         else
         {
            ossSnprintf( desc, OMA_BUFF_SIZE, "Finish adding host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG ( PDEVENT, "Sucessed to add host[%s]",
                     pInfo->_item._ip.c_str() ) ;
            ps._desc = desc ;
            rc = omaGetBooleanElement ( retObj, OMA_FIELD_HASINSTALL,
                                           ps._hasInstall ) ;
            if ( tmpRc )
            {
               PD_LOG ( PDWARNING, "Succeed to add host[%s], but js file didn't"
                        "tell wether db packet had been installed or not, "
                        "assume it had not been installed, rc = %d",
                        pInfo->_item._ip.c_str(), tmpRc ) ;
               ps._hasInstall = FALSE ;
            }
            _pTask->updateProgressStatus( pInfo->_serialNum, ps, TRUE ) ;
         }
      }
      _pTask->updateJobStatus( _jobName, OMA_JOB_STATUS_FINISH ) ;

   done:
      return rc ;
   error:
      _pTask->setIsAddHostFail( TRUE ) ;
      _pTask->updateJobStatus( _jobName, OMA_JOB_STATUS_FAIL ) ;
      goto done ;
   }



   _omaRbHostJob::_omaRbHostJob ( string jobName, _omaAddHostTask *pTask )
   {
      _jobName = jobName ;
      _pTask = pTask ;
   }

   _omaRbHostJob::~_omaRbHostJob()
   {
   }

   RTN_JOB_TYPE _omaRbHostJob::type () const
   {
      return RTN_JOB_RMHOST ;
   }

   const CHAR* _omaRbHostJob::name () const
   {
      return _jobName.c_str() ;
   }

   BOOLEAN _omaRbHostJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaRbHostJob::doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;

      _pTask->registerJob( _jobName ) ;
      
      while( TRUE )
      {
         BOOLEAN hasUninstall = FALSE ;
         CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
         const CHAR *pErrMsg = NULL ;
         AddHostInfo *pInfo = NULL ;
         AddHostPS ps = { 0, "", "", FALSE } ;
         INT32 errNum = 0 ;
         BSONObj retObj ;

         pInfo = _pTask->getRbHostItem() ;
         if ( NULL == pInfo )
         {
            _pTask->updateJobStatus( _jobName, OMA_JOB_STATUS_FINISH ) ;
            goto done ;
         }
         ossSnprintf( desc, OMA_BUFF_SIZE, "Removing host[%s]",
                      pInfo->_item._ip.c_str() ) ;
         ps._desc = desc ;
         rc = _pTask->updateProgressStatus( pInfo->_serialNum, ps ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to update remove host[%s] progress status, "
                    "rc = %d", pInfo->_item._ip.c_str(), rc ) ;
            goto error ;
         }

         _omaRunRmHost runCmd( *pInfo ) ;
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to init for removing host[%s], "
                    "rc = %d", pInfo->_item._ip.c_str(), rc ) ;
            goto error ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to remove host[%s], "
                    "rc = %d", pInfo->_item._ip.c_str(), rc ) ;
            tmpRc= omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
            if ( tmpRc )
            {
               pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
               if ( NULL == pErrMsg )
               {
                  pErrMsg = "no exeute js file yet" ;
               }
            }
            ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to remove host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            ps._errno = rc ;
            ps._errMsg = pErrMsg ;
            ps._desc = desc ;
            ps._hasInstall = TRUE ;

            _pTask->updateProgressStatus( pInfo->_serialNum, ps, TRUE ) ;
            goto error ;
         }
         rc = omaGetIntElement ( retObj, OMA_FIELD_ERRNO, errNum ) ;
         if ( rc )
         {
            ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to remove host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            pErrMsg = "Failed to get return error number" ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            ps._errno = rc ;
            ps._errMsg = pErrMsg ;
            ps._desc = desc ;
            ps._hasInstall = TRUE ;
            _pTask->updateProgressStatus( pInfo->_serialNum, ps, TRUE ) ;
            goto error ;
         }
         if ( SDB_OK != errNum )
         {
            rc = omaGetBooleanElement ( retObj, OMA_FIELD_HASUNINSTALL,
                                        hasUninstall ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG ( PDWARNING, "Failed to add host[%s], but js file didn't"
                        "tell wether db packet had been uninstalled or not, "
                        "assume it had not been uninstalled, rc = %d",
                        pInfo->_item._ip.c_str(), rc ) ;
               hasUninstall = FALSE ;
            }
            rc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
            if ( SDB_OK != rc )
            {
               pErrMsg = "Js not return error detail" ;
            }
            ossSnprintf( desc, OMA_BUFF_SIZE, "Failed to remove host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            ps._errno = errNum ;
            ps._errMsg = pErrMsg ;
            ps._desc = desc ;
            ps._hasInstall = !hasUninstall ;
            _pTask->updateProgressStatus( pInfo->_serialNum, ps, TRUE ) ;
            goto error ;
         }
         else
         {
            ossSnprintf( desc, OMA_BUFF_SIZE, "Finish removing host[%s]",
                         pInfo->_item._ip.c_str() ) ;
            PD_LOG ( PDEVENT, "Sucessed to remove host[%s]",
                     pInfo->_item._ip.c_str() ) ;
            ps._desc = desc ;
            rc = omaGetBooleanElement ( retObj, OMA_FIELD_HASUNINSTALL,
                                        hasUninstall ) ;
            if ( rc )
            {
               PD_LOG ( PDWARNING, "Succeed to remove host[%s], but js file didn't"
                        "tell wether db packet had been uninstalled or not, "
                        "assume it has not been uninstalled, rc = %d",
                        pInfo->_item._ip.c_str(), rc ) ;
               hasUninstall = FALSE ;
            }
            ps._hasInstall = !hasUninstall ;
            _pTask->updateProgressStatus( pInfo->_serialNum, ps, TRUE ) ;
         }
      }
      _pTask->updateJobStatus( _jobName, OMA_JOB_STATUS_FINISH ) ;

   done:
      return rc ;
   error:
      _pTask->updateJobStatus( _jobName, OMA_JOB_STATUS_FAIL ) ;
      goto done ;
   }



   _omaStartAddHostTaskJob::_omaStartAddHostTaskJob ( BSONObj &addHostInfo )
   {
      _addHostInfoObj = addHostInfo.getOwned() ;
      _jobName = OMA_JOB_START_ADD_HOST_TASK ;
      _pTask = NULL ;
   }

   _omaStartAddHostTaskJob::~_omaStartAddHostTaskJob()
   {
   }

   RTN_JOB_TYPE _omaStartAddHostTaskJob::type () const
   {
      return RTN_JOB_STARTADDHOSTTASK ;
   }

   const CHAR* _omaStartAddHostTaskJob::name () const
   {
      return _jobName.c_str() ;
   }

   BOOLEAN _omaStartAddHostTaskJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaStartAddHostTaskJob::init()
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;

      PD_LOG ( PDDEBUG, "Add host passes argument: %s",
               _addHostInfoObj.toString( FALSE, TRUE ).c_str() ) ;
      ele = _addHostInfoObj.getField( OMA_FIELD_TASKID ) ;
      if ( NumberInt != ele.type() && NumberLong != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive invalid task id from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _taskID = ele.numberLong() ;
      rc = _initAddHostsInfo( _addHostInfoObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get add host info, rc = %d", rc ) ;
         goto error ;
      }
      
   done:
      return rc ;
   error :
      goto done ;
   }

   INT32 _omaStartAddHostTaskJob::init2()
   {
      INT32 rc                         = SDB_OK ;
      _omaTaskMgr *pTaskMgr            = getTaskMgr() ;
      
      rc = pTaskMgr->removeTask ( OMA_TASK_NAME_ADD_HOST ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "Failed to remove task[%s], "
                     "rc = %d", OMA_TASK_NAME_ADD_HOST, rc ) ;
         goto error ;
      }
      _pTask = SDB_OSS_NEW _omaAddHostTask( _taskID ) ;
      if ( !_pTask )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "Failed to create add host task, "
                     "rc = %d", rc ) ;
         goto error ;
      }
      pTaskMgr->addTask( _pTask, _taskID ) ;
      rc = _pTask->init( _addHostInfoObj, _addHostInfo ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "Failed to init in add host task, "
                     "rc = %d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaStartAddHostTaskJob::doit()
   {
      INT32 rc = SDB_OK ;
      rc = _pTask->doit() ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "Failed to do add host task, "
                     "rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaStartAddHostTaskJob::_initAddHostsInfo( BSONObj &info )
   {
      INT32 rc                   = SDB_OK ;
      const CHAR *pSdbUser       = NULL ;
      const CHAR *pSdbPasswd     = NULL ;
      const CHAR *pSdbUserGroup  = NULL ;
      const CHAR *pInstallPacket = NULL ;
      const CHAR *pStr           = NULL ;
      BSONElement ele ;
      
      rc = omaGetStringElement( info, OMA_FIELD_SDBUSER, &pSdbUser ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_SDBUSER, rc ) ;
      rc = omaGetStringElement( info, OMA_FIELD_SDBPASSWD, &pSdbPasswd ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_SDBPASSWD, rc ) ;
      rc = omaGetStringElement( info, OMA_FIELD_SDBUSERGROUP, &pSdbUserGroup ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_SDBUSERGROUP, rc ) ;
      rc = omaGetStringElement( info, OMA_FIELD_INSTALLPACKET, &pInstallPacket ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_FIELD_INSTALLPACKET, rc ) ;
      ele = info.getField( OMA_FIELD_HOSTINFO ) ;
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
            AddHostItem hostItem ;
            hostInfo._serialNum = serialNum++ ;
            hostInfo._flag = FALSE ;
            hostInfo._isFinish = FALSE ; 
            hostInfo._ps._errno = 0 ;
            hostInfo._ps._errMsg = "" ;
            hostInfo._ps._desc = "" ;
            hostInfo._ps._hasInstall = FALSE ;
            hostInfo._common._sdbUser = pSdbUser ;
            hostInfo._common._sdbPasswd = pSdbPasswd ;
            hostInfo._common._userGroup = pSdbUserGroup ;
            hostInfo._common._installPacket = pInstallPacket ;
            BSONObj item ;
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
            hostItem._ip = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_HOSTNAME, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_HOSTNAME, rc ) ;
            hostItem._hostName = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_USER, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_USER, rc ) ;
            hostItem._user = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_PASSWD, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_PASSWD, rc ) ;
            hostItem._passwd = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_SSHPORT, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_SSHPORT, rc ) ;
            hostItem._sshPort = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_AGENTPORT, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_AGENTPORT, rc ) ;
            hostItem._agentPort = pStr ;
            rc = omaGetStringElement( item, OMA_FIELD_INSTALLPATH, &pStr ) ;
            PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                      "Get field[%s] failed, rc: %d",
                      OMA_FIELD_INSTALLPATH, rc ) ;
            hostItem._installPath = pStr ;

            hostInfo._item = hostItem ;
            _addHostInfo.push_back( hostInfo ) ;
         }
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }




   _omaCreateStandaloneJob::_omaCreateStandaloneJob (
                                             _omaInsDBBusTask *pTask )
   {
      _name = OMA_JOB_CREATE_STANDALONE;
      _status = OMA_JOB_STATUS_INIT ;
      _pTask = pTask ;
   }

   _omaCreateStandaloneJob::~_omaCreateStandaloneJob()
   {
   }

   RTN_JOB_TYPE _omaCreateStandaloneJob::type () const
   {
      return RTN_JOB_CREATESTANDALONE ;
   }

   const CHAR* _omaCreateStandaloneJob::name () const
   {
      return _name.c_str() ;
   }

   BOOLEAN _omaCreateStandaloneJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaCreateStandaloneJob::doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      INT32 num = 0 ;
      vector<BSONObj> &standaloneInsInfo = _pTask->getInstallStandaloneInfo() ;
      vector<BSONObj>::iterator itr ;

      setJobStatus( OMA_JOB_STATUS_RUNNING ) ;
      num = standaloneInsInfo.size() ;
      if ( 0 == num )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "No install standalone info, rc = %d", rc ) ;
         goto error ;
      }
      itr = standaloneInsInfo.begin() ;
      for ( ; itr != standaloneInsInfo.end(); itr++ )
      {
         BSONObj retObj ;
         InstallInfo installInfo ;
         InstalledNode node ;
         CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
         const CHAR *pErrMsg = "" ;
         rc = _getInstallInfo( *itr, installInfo ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get install standalone info, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         node._role = ROLE_STANDALONE ;
         node._dataGroupName = "" ;
         node._hostName = installInfo._hostName.c_str() ;
         node._svcName = installInfo._svcName.c_str() ;
         ossSnprintf( desc, OMA_BUFF_SIZE, "Installing standalone[%s:%s]",
                      installInfo._hostName.c_str(),
                      installInfo._svcName.c_str() ) ;
         rc = _updateInstallStatus( FALSE, SDB_OK, NULL, desc, NULL ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to update status before install "
                     "standalone, rc = %d", rc ) ;
            goto error ;
         }
         _omaRunCreateStandaloneJob runCmd( _pTask->getVCoordSvcName(),
                                            installInfo ) ;
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to init for creating standalone, "
                    "rc = %d", rc ) ;
            goto error ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to create standalone, rc = %d", rc ) ;
            tmpRc = omaGetStringElement ( retObj, OMA_FIELD_DETAIL, &pErrMsg ) ;
            if ( tmpRc )
            {
               PD_LOG ( PDWARNING, "Get field[%s] failed, rc = %d",
                        OMA_FIELD_DETAIL, tmpRc ) ;
            }
            ossSnprintf( desc, OMA_BUFF_SIZE,
                         "Failed to install standalone[%s:%s]: %s",
                         installInfo._hostName.c_str(),
                         installInfo._svcName.c_str(), pErrMsg ) ;
            PD_LOG_MSG ( PDERROR, desc ) ;
            _updateInstallStatus( FALSE, rc, pErrMsg, desc, &node ) ;
            goto error ;
         }
         else
         {
            ossSnprintf( desc, OMA_BUFF_SIZE,
                         "Finish installing standalone[%s:%s]",
                         installInfo._hostName.c_str(),
                         installInfo._svcName.c_str() ) ;
            PD_LOG ( PDDEBUG, "Sucessed to install standalone[%s:%s]",
                     installInfo._hostName.c_str(),
                     installInfo._svcName.c_str() ) ;
            _updateInstallStatus( TRUE, SDB_OK, pErrMsg, desc, &node ) ;
         }
      }
      setJobStatus( OMA_JOB_STATUS_FINISH ) ;
   done:
      return rc ;
   error:
      setJobStatus( OMA_JOB_STATUS_FAIL ) ;
      goto done ;
   }

   INT32 _omaCreateStandaloneJob::_getInstallInfo( BSONObj &obj,
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

   INT32 _omaCreateStandaloneJob::_updateInstallStatus( BOOLEAN isFinish,
                                                        INT32 retRc,
                                                        const CHAR *pErrMsg,
                                                        const CHAR *pDesc,
                                                        InstalledNode *pNode )
   {
      INT32 rc = SDB_OK ;

      rc = _pTask->updateInstallStatus( isFinish, retRc, ROLE_STANDALONE,
                                        pErrMsg, pDesc, NULL, pNode ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to update install catalog information, "
                  "rc = %d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }
   


   _omaCreateCatalogJob::_omaCreateCatalogJob (
                                             _omaInsDBBusTask *pTask )
   {
      _name = OMA_JOB_CREATE_CATALOG ;
      _status = OMA_JOB_STATUS_INIT ;
      _pTask = pTask ;
   }

   _omaCreateCatalogJob::~_omaCreateCatalogJob()
   {
   }

   RTN_JOB_TYPE _omaCreateCatalogJob::type () const
   {
      return RTN_JOB_CREATECATALOG ;
   }

   const CHAR* _omaCreateCatalogJob::name () const
   {
      return _name.c_str() ;
   }

   BOOLEAN _omaCreateCatalogJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaCreateCatalogJob::doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      INT32 num = 0 ;
      vector<BSONObj> &catalogInstallInfo = _pTask->getInstallCatalogInfo() ;
      vector<BSONObj>::iterator itr ;

      setJobStatus( OMA_JOB_STATUS_RUNNING ) ;
      num = catalogInstallInfo.size() ;
      if ( 0 == num )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "No install catalog info, rc = %d", rc ) ;
         goto error ;
      }
      itr = catalogInstallInfo.begin() ;
      for ( ; itr != catalogInstallInfo.end(); itr++ )
      {
         BSONObj retObj ;
         InstallInfo installInfo ;
         CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
         const CHAR *pErrMsg = "" ;
         rc = _getInstallInfo( *itr, installInfo ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to get install catalog info, "
                         "rc = %d", rc ) ;
            goto error ;
         }
         ossSnprintf( desc, OMA_BUFF_SIZE, "Installing catalog[%s:%s]",
                      installInfo._hostName.c_str(),
                      installInfo._svcName.c_str() ) ;
         rc = _updateInstallStatus( FALSE, SDB_OK, NULL, desc, NULL ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to update status before install catalog "
                     "node, rc = %d", rc ) ;
            goto error ;
         }
         _omaRunCreateCatalogJob runCmd( _pTask->getVCoordSvcName(),
                                          installInfo ) ;
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to init for creating catalog, "
                    "rc = %d", rc ) ;
            goto error ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to create catalog, rc = %d", rc ) ;
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
                         "Failed to install catalog[%s:%s]",
                         installInfo._hostName.c_str(),
                         installInfo._svcName.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            _updateInstallStatus( FALSE, rc, pErrMsg, desc, NULL ) ;
            goto error ;
         }
         else
         {
            InstalledNode node ;
            node._role = ROLE_CATA ;
            node._dataGroupName = "" ;
            node._hostName = installInfo._hostName.c_str() ;
            node._svcName = installInfo._svcName.c_str() ;
            ossSnprintf( desc, OMA_BUFF_SIZE,
                         "Finish installing catalog[%s:%s]",
                         installInfo._hostName.c_str(),
                         installInfo._svcName.c_str() ) ;
            PD_LOG ( PDEVENT, "Sucessed to install catalog[%s:%s]",
                     installInfo._hostName.c_str(),
                     installInfo._svcName.c_str() ) ;
            _updateInstallStatus( TRUE, SDB_OK, pErrMsg, desc, &node ) ;
         }
      }
      setJobStatus( OMA_JOB_STATUS_FINISH ) ;
   done:
      return rc ;
   error:
      setJobStatus( OMA_JOB_STATUS_FAIL ) ;
      goto done ;
   }

   INT32 _omaCreateCatalogJob::_getInstallInfo( BSONObj &obj,
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
                      OMA_FIELD_SSHPORT        << 1 ) ;
      conf = obj.filterFieldsUndotted( pattern, false ) ;
      info._conf = conf.getOwned() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaCreateCatalogJob::_updateInstallStatus( BOOLEAN isFinish,
                                                     INT32 retRc,
                                                     const CHAR *pErrMsg,
                                                     const CHAR *pDesc,
                                                     InstalledNode *pNode )
   {
      INT32 rc = SDB_OK ;

      rc = _pTask->updateInstallStatus( isFinish, retRc, ROLE_CATA,
                                        pErrMsg, pDesc, NULL, pNode ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to update install catalog information, "
                      "rc = %d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }



   _omaCreateCoordJob::_omaCreateCoordJob ( _omaInsDBBusTask *pTask )
   {
      _name =  OMA_JOB_CREATE_COORD;
      _status = OMA_JOB_STATUS_INIT ;
      _pTask = pTask ;
   }

   _omaCreateCoordJob::~_omaCreateCoordJob()
   {
   }

   RTN_JOB_TYPE _omaCreateCoordJob::type () const
   {
      return RTN_JOB_CREATECOORD ;
   }

   const CHAR* _omaCreateCoordJob::name () const
   {
      return _name.c_str() ;
   }

   BOOLEAN _omaCreateCoordJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaCreateCoordJob::doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      INT32 num = 0 ;
      vector<BSONObj> &coordInstallInfo = _pTask->getInstallCoordInfo() ;
      vector<BSONObj>::iterator itr ;

      setJobStatus( OMA_JOB_STATUS_RUNNING ) ;
      num = coordInstallInfo.size() ;
      if ( 0 == num )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "No install catalog info, rc = %d", rc ) ;
         goto error ;
      }
      itr = coordInstallInfo.begin() ;
      for ( ; itr != coordInstallInfo.end(); itr++ )
      {
         BSONObj retObj ;
         InstallInfo installInfo ;
         CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
         const CHAR *pErrMsg = "" ;

         if ( _pTask->getIsInstallFail() )
         {
            rc = SDB_SYS ;
            PD_LOG ( PDWARNING, "Stop installing coord, install has failed" ) ;
            goto error ;
         }
         rc = _getInstallInfo( *itr, installInfo ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get install coord info, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         ossSnprintf( desc, OMA_BUFF_SIZE, "Installing coord[%s:%s]",
                      installInfo._hostName.c_str(),
                      installInfo._svcName.c_str() ) ;
         rc = _updateInstallStatus( FALSE, SDB_OK, NULL, desc, NULL ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to update status before install coord, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         _omaRunCreateCoordJob runCmd( _pTask->getVCoordSvcName(),
                                        installInfo ) ;
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to init for creating coord, "
                    "rc = %d", rc ) ;
            goto error ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to create coord, rc = %d", rc ) ;
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
                         "Failed to install coord[%s:%s]",
                         installInfo._hostName.c_str(),
                         installInfo._svcName.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            _updateInstallStatus( FALSE, rc, pErrMsg, desc, NULL ) ;
            goto error ;
         }
         else
         {
            InstalledNode node ;
            node._role = ROLE_COORD ;
            node._dataGroupName = "" ;
            node._hostName = installInfo._hostName.c_str() ;
            node._svcName = installInfo._svcName.c_str() ;
            ossSnprintf( desc, OMA_BUFF_SIZE,
                         "Finish installing coord[%s:%s]",
                         installInfo._hostName.c_str(),
                         installInfo._svcName.c_str() ) ;
            PD_LOG ( PDEVENT, "Sucessed to install coord[%s:%s]",
                     installInfo._hostName.c_str(),
                     installInfo._svcName.c_str() ) ;
            _updateInstallStatus( TRUE, SDB_OK, pErrMsg, desc, &node ) ;
         }
      }
      setJobStatus( OMA_JOB_STATUS_FINISH ) ;
   done:
      return rc ;
   error:
      setJobStatus( OMA_JOB_STATUS_FAIL ) ;
      goto done ;
   }

   INT32 _omaCreateCoordJob::_getInstallInfo( BSONObj &obj,
                                              InstallInfo &info )
   {
      INT32 rc = SDB_OK ; 
      const CHAR *pStr = NULL ;
      BSONObj conf ;
      BSONObj pattern ;

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
                      OMA_FIELD_SSHPORT        << 1 ) ;
      conf = obj.filterFieldsUndotted( pattern, false ) ;
      info._conf = conf.getOwned() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaCreateCoordJob::_updateInstallStatus( BOOLEAN isFinish,
                                                   INT32 retRc,
                                                   const CHAR *pErrMsg,
                                                   const CHAR *pDesc,
                                                   InstalledNode *pNode )
   {
      INT32 rc = SDB_OK ;
      rc = _pTask->updateInstallStatus( isFinish, retRc, ROLE_COORD,
                                        pErrMsg, pDesc, NULL, pNode ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to update install coord information, "
                  "rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }



   _omaCreateDataJob::_omaCreateDataJob ( const CHAR *pGroupName,
                                          _omaInsDBBusTask *pTask )
   {
      _groupname = pGroupName ;
      _name = _name + "create data node in " + pGroupName  ;
      _status = OMA_JOB_STATUS_INIT ;
      _pTask = pTask ;
   }

   _omaCreateDataJob::~_omaCreateDataJob()
   {
   }

   RTN_JOB_TYPE _omaCreateDataJob::type () const
   {
      return RTN_JOB_CREATEDATA ;
   }

   const CHAR* _omaCreateDataJob::name () const
   {
      return _name.c_str() ;
   }

   BOOLEAN _omaCreateDataJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaCreateDataJob::doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      INT32 num = 0 ;
      vector<BSONObj>::iterator itr ;
      vector<BSONObj> dataNodeInstallInfo ;

      setJobStatus( OMA_JOB_STATUS_RUNNING ) ;
      rc = _pTask->getInstallDataGroupInfo( _groupname,
                                            dataNodeInstallInfo ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to get install data node information "
                      "in group %s, rc = %d", _groupname.c_str(), rc ) ;
         goto error ;
      }
      num = dataNodeInstallInfo.size() ;
      if ( 0 == num )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "No install data node info in group %s, rc = %d",
                      _groupname.c_str(), rc ) ;
         goto error ;
      }
      itr = dataNodeInstallInfo.begin() ;
      for ( ; itr != dataNodeInstallInfo.end(); itr++ )
      {
         BSONObj retObj ;
         InstallInfo installInfo ;
         CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
         const CHAR *pErrMsg = "" ;

         if ( _pTask->getIsInstallFail() )
         {
            rc = SDB_SYS ;
            PD_LOG ( PDWARNING, "Stop installing data node in group [%s], "
                     "install had failed", _groupname.c_str() ) ;
            goto error ;
         }
         rc = _getInstallInfo( *itr, installInfo ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to get install data node info, "
                         "rc = %d", rc ) ;
            goto error ;
         }
         ossSnprintf( desc, OMA_BUFF_SIZE, "Installing data node[%s:%s]",
                      installInfo._hostName.c_str(),
                      installInfo._svcName.c_str() ) ;
         rc = _updateInstallStatus( FALSE, SDB_OK, NULL, desc, NULL ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to update status before "
                     "install data node, rc = %d", rc ) ;
            goto error ;
         }
         _omaRunCreateDataNodeJob runCmd( _pTask->getVCoordSvcName(),
                                           installInfo ) ;
         rc = runCmd.init( NULL ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to init for installing data node, "
                    "rc = %d", rc ) ;
            goto error ;
         }
         rc = runCmd.doit( retObj ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Job failed to create data node, rc = %d", rc ) ;
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
                         "Failed to install data node[%s:%s]",
                         installInfo._hostName.c_str(),
                         installInfo._svcName.c_str() ) ;
            PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;
            _updateInstallStatus( FALSE, rc, pErrMsg, desc, NULL ) ;
            goto error ;
         }
         else
         {
            InstalledNode node ;
            node._role = ROLE_DATA ;
            node._dataGroupName = _groupname ;
            node._hostName = installInfo._hostName.c_str() ;
            node._svcName = installInfo._svcName.c_str() ;
            ossSnprintf( desc, OMA_BUFF_SIZE,
                         "Finish installing data node[%s:%s]",
                         installInfo._hostName.c_str(),
                         installInfo._svcName.c_str() ) ;
            PD_LOG ( PDEVENT, "Sucessed to install data node[%s:%s]",
                     installInfo._hostName.c_str(),
                     installInfo._svcName.c_str() ) ;
            _updateInstallStatus( TRUE, SDB_OK, pErrMsg, desc, &node ) ;
         }
      }
      setJobStatus( OMA_JOB_STATUS_FINISH ) ;
   done:
      return rc ;
   error:
      setJobStatus( OMA_JOB_STATUS_FAIL ) ;
      goto done ;
   }

   INT32 _omaCreateDataJob::_getInstallInfo ( BSONObj &obj,
                                              InstallInfo &installInfo )
   {
      INT32 rc = SDB_OK ;
      const CHAR* pStr = "" ;
      BSONObj conf ;
      BSONObj pattern ;

      rc = omaGetStringElement( obj, OMA_OPTION_DATAGROUPNAME, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d",
                OMA_OPTION_DATAGROUPNAME, rc ) ;
      installInfo._dataGroupName = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_HOSTNAME, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_HOSTNAME, rc ) ;
      installInfo._hostName = pStr ;
      rc = omaGetStringElement( obj, OMA_OPTION_SVCNAME, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_OPTION_SVCNAME, rc ) ;
      installInfo._svcName = pStr ;
      rc = omaGetStringElement( obj, OMA_OPTION_DBPATH, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_OPTION_DBPATH, rc ) ;
      installInfo._dbPath = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_SDBUSER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBUSER, rc ) ;
      installInfo._sdbUser = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_SDBPASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBPASSWD, rc ) ;
      installInfo._sdbPasswd = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_SDBUSERGROUP, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SDBUSERGROUP, rc ) ;
      installInfo._sdbUserGroup = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_USER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_USER, rc ) ;
      installInfo._user = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_PASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_PASSWD, rc ) ;
      installInfo._passwd = pStr ;
      rc = omaGetStringElement( obj, OMA_FIELD_SSHPORT, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR,
                "Get field[%s] failed, rc: %d", OMA_FIELD_SSHPORT, rc ) ;
      installInfo._sshPort = pStr ;
      pattern = BSON( OMA_FIELD_HOSTNAME       << 1 <<
                      OMA_OPTION_DATAGROUPNAME << 1 <<
                      OMA_OPTION_SVCNAME       << 1 <<
                      OMA_OPTION_DBPATH        << 1 <<
                      OMA_FIELD_SDBUSER        << 1 <<
                      OMA_FIELD_SDBPASSWD      << 1 <<
                      OMA_FIELD_SDBUSERGROUP   << 1 <<
                      OMA_FIELD_USER           << 1 <<
                      OMA_FIELD_PASSWD         << 1 <<
                      OMA_FIELD_SSHPORT        << 1 ) ;
      conf = obj.filterFieldsUndotted( pattern, false ) ;
      installInfo._conf = conf.getOwned() ; 

   done:
     return rc ;
   error:
      goto done ;
   }

   INT32 _omaCreateDataJob::_updateInstallStatus( BOOLEAN isFinish,
                                                  INT32 retRc,
                                                  const CHAR *pErrMsg,
                                                  const CHAR *pDesc,
                                                  InstalledNode *pNode )
   {
      INT32 rc = SDB_OK ;

      rc = _pTask->updateInstallStatus( isFinish, retRc,
                                        ROLE_DATA, pErrMsg, pDesc,
                                        _groupname.c_str(), pNode ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to update install data node information, "
                  "rc = %d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }



   _omaStartInsDBBusTaskJob::_omaStartInsDBBusTaskJob ( BSONObj &installInfo )
   {
      _isStandalone = FALSE ;
      _installInfoObj = installInfo.getOwned() ;
      _name = OMA_JOB_START_INSTALL_DB_BUS_TASK ;
      _pTask = NULL ;
   }

   _omaStartInsDBBusTaskJob::~_omaStartInsDBBusTaskJob()
   {
   }

   RTN_JOB_TYPE _omaStartInsDBBusTaskJob::type () const
   {
      return RTN_JOB_STARTINSDBBUSTASK ;
   }

   const CHAR* _omaStartInsDBBusTaskJob::name () const
   {
      return _name.c_str() ;
   }

   BOOLEAN _omaStartInsDBBusTaskJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaStartInsDBBusTaskJob::init()
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      BSONObj filter ;
      BSONObj commonFileds ;
      BSONObjBuilder builder ;
      string deplayMod ;
      const CHAR *pStr = NULL ;

      PD_LOG ( PDDEBUG, "Add db business passes argument: %s",
               _installInfoObj.toString( FALSE, TRUE ).c_str() ) ;
      ele = _installInfoObj.getField( OMA_FIELD_TASKID ) ;
      if ( NumberInt != ele.type() && NumberLong != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive invalid task id from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _taskID = ele.numberLong() ;
      ele = _installInfoObj.getField( OMA_FIELD_DEPLOYMOD ) ;
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
      rc = omaGetStringElement ( _installInfoObj, OMA_FIELD_SDBUSER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_SDBUSER, rc ) ;
      builder.append( OMA_FIELD_SDBUSER, pStr ) ;
      rc = omaGetStringElement ( _installInfoObj, OMA_FIELD_SDBPASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_SDBPASSWD, rc ) ;
      builder.append( OMA_FIELD_SDBPASSWD, pStr ) ;
      rc = omaGetStringElement ( _installInfoObj, OMA_FIELD_SDBUSERGROUP, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_SDBUSERGROUP, rc ) ;
      builder.append( OMA_FIELD_SDBUSERGROUP, pStr ) ;
      commonFileds = builder.obj() ;
      ele = _installInfoObj.getField ( OMA_FIELD_CONFIG ) ;
      if ( Array != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive wrong format install "
                      "info from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
      {
         BSONObjIterator itr( ele.embeddedObject() ) ;
         while ( itr.more() )
         {
            BSONObjBuilder bob ;
            BSONObj info ;
            BSONObj temp ;
            const CHAR *value = NULL ;
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
            info = bob.obj() ;
            rc = omaGetStringElement ( temp, OMA_OPTION_ROLE, &value ) ;
            if ( rc )
            {
               PD_LOG_MSG ( PDERROR, "Get field[%s] failed, rc = %d",
                            OMA_OPTION_ROLE, rc ) ;
               goto error ;
            }
            if ( 0 == ossStrncmp( value, ROLE_DATA,
                                  ossStrlen( ROLE_DATA ) ) )
            {
               _data.push_back( info ) ;
            }
            else if ( 0 == ossStrncmp( value, ROLE_COORD,
                                       ossStrlen( ROLE_COORD ) ) )
            {
               _coord.push_back( info ) ;
            }
            else if ( 0 == ossStrncmp( value, ROLE_CATA,
                                       ossStrlen( ROLE_CATA ) ) )
            {
               _catalog.push_back( info ) ;
            }
            else if ( 0 == ossStrncmp( value, ROLE_STANDALONE,
                                       ossStrlen( ROLE_STANDALONE ) ) )
            {
               _standalone.push_back( info ) ;
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

   INT32 _omaStartInsDBBusTaskJob::init2()
   {
      INT32 rc                         = SDB_OK ;
      _omaTaskMgr *pTaskMgr            = getTaskMgr() ;
      BSONObj otherInfo ;
      
      rc = pTaskMgr->removeTask ( OMA_TASK_NAME_INSTALL_DB_BUSINESS ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "Failed to remove task[%s], "
                     "rc = %d", OMA_TASK_NAME_INSTALL_DB_BUSINESS, rc ) ;
         goto error ;
      }
      _pTask = SDB_OSS_NEW _omaInsDBBusTask( _taskID ) ;
      if ( !_pTask )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "Failed to create install db business task, "
                     "rc = %d", rc ) ;
         goto error ;
      }
      pTaskMgr->addTask( _pTask, _taskID ) ;
      rc = _pTask->init( _isStandalone, _standalone, _coord,
                         _catalog, _data, otherInfo ) ;
      if ( rc  )
      {
         PD_LOG_MSG( PDERROR, "Failed to init install db busniness task, "
                     "rc = %d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaStartInsDBBusTaskJob::doit()
   {
      INT32 rc = SDB_OK ;
      rc = _pTask->doit() ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "Failed to do install db busniness task, "
                     "rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }



   _omaStartRmDBBusTaskJob::_omaStartRmDBBusTaskJob ( BSONObj &uninstallInfo )
   {
      _isStandalone = FALSE ;
      _uninstallInfoObj = uninstallInfo.getOwned() ;
      _name = OMA_JOB_START_REMOVE_DB_BUS_TASK ;
      _pTask = NULL ;
   }

   _omaStartRmDBBusTaskJob::~_omaStartRmDBBusTaskJob()
   {
   }

   RTN_JOB_TYPE _omaStartRmDBBusTaskJob::type () const
   {
      return RTN_JOB_STARTRMDBBUSTASK ;
   }

   const CHAR* _omaStartRmDBBusTaskJob::name () const
   {
      return _name.c_str() ;
   }

   BOOLEAN _omaStartRmDBBusTaskJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaStartRmDBBusTaskJob::init()
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      BSONObj filter ;
      BSONObj commonFileds ;
      BSONObjBuilder builder ;
      string deplayMod ;
      const CHAR *pStr = NULL ;

      PD_LOG ( PDDEBUG, "Remove db business passes argument: %s",
               _uninstallInfoObj.toString( FALSE, TRUE ).c_str() ) ;
      ele = _uninstallInfoObj.getField( OMA_FIELD_TASKID ) ;
      if ( NumberInt != ele.type() && NumberLong != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive invalid task id from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _taskID = ele.numberLong() ;
      ele = _uninstallInfoObj.getField( OMA_FIELD_DEPLOYMOD ) ;
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
      rc = omaGetStringElement ( _uninstallInfoObj, OMA_FIELD_AUTHUSER, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_AUTHUSER, rc ) ;
      builder.append( OMA_FIELD_AUTHUSER, pStr ) ;
      rc = omaGetStringElement ( _uninstallInfoObj, OMA_FIELD_AUTHPASSWD, &pStr ) ;
      PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_AUTHPASSWD, rc ) ;
      builder.append( OMA_FIELD_AUTHPASSWD, pStr ) ;
      commonFileds = builder.obj() ;
      ele = _uninstallInfoObj.getField ( OMA_FIELD_CONFIG ) ;
      if ( Array != ele.type() )
      {
         PD_LOG_MSG ( PDERROR, "Receive wrong format install "
                      "info from omsvc" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
      {
         BSONObjBuilder cataInfoBuilder ;
         BSONArrayBuilder bab ;
         BSONObjIterator itr( ele.embeddedObject() ) ;
         while ( itr.more() )
         {
            BSONObj temp ;
            const CHAR *value = NULL ;
            ele = itr.next() ;
            if ( Object != ele.type() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG ( PDERROR, "Receive wrong format bson from omsvc" ) ;
               goto error ;
            }
            temp = ele.embeddedObject() ;
            rc = omaGetStringElement ( temp, OMA_OPTION_ROLE, &value ) ;
            if ( rc )
            {
               PD_LOG_MSG ( PDERROR, "Get field[%s] failed, rc = %d",
                            OMA_OPTION_ROLE, rc ) ;
               goto error ;
            }
            if ( 0 == ossStrncmp( value, ROLE_DATA,
                                  ossStrlen( ROLE_DATA ) ) )
            {
               BSONObjBuilder bob ;
               BSONObj obj ;
               bob.appendElements( commonFileds ) ;
               rc = omaGetStringElement ( temp, OMA_OPTION_DATAGROUPNAME,
                                          &value ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_OPTION_DATAGROUPNAME, rc ) ;
               bob.append( OMA_FIELD_UNINSTALLGROUPNAME, value ) ;
               obj = bob.obj() ;
               _data.insert( pair<string, BSONObj>( string(value), obj ) ) ;
            }
            else if ( 0 == ossStrncmp( value, ROLE_COORD,
                                       ossStrlen( ROLE_COORD ) ) )
            {
               _coord.insert( pair<string, BSONObj>( string(ROLE_COORD),
                                                     commonFileds ) ) ;
            }
            else if ( 0 == ossStrncmp( value, ROLE_CATA,
                                       ossStrlen( ROLE_CATA ) ) )
            {
               BSONObjBuilder bob ;
               rc = omaGetStringElement ( temp, OMA_FIELD_HOSTNAME, &pStr ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_HOSTNAME, rc ) ;
               bob.append( OMA_FIELD_HOSTNAME, pStr ) ;
               rc = omaGetStringElement ( temp, OMA_OPTION_CATANAME, &pStr ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_OPTION_CATANAME, rc ) ;
               bob.append( OMA_FIELD_CATASVCNAME, pStr ) ;
               bab.append( bob.obj() ) ;
               _catalog.insert( pair<string, BSONObj>( string(ROLE_CATA),
                                                       commonFileds ) ) ;
            }
            else if ( 0 == ossStrncmp( value, ROLE_STANDALONE,
                                       ossStrlen( ROLE_STANDALONE ) ) )
            {
               BSONObjBuilder bob ;
               rc = omaGetStringElement ( temp, OMA_FIELD_HOSTNAME, &pStr ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_HOSTNAME, rc ) ;
               bob.append( OMA_FIELD_HOSTNAME, pStr ) ;
               rc = omaGetStringElement ( temp, OMA_FIELD_SVCNAME, &pStr ) ;
               PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                "rc: %d", OMA_FIELD_SVCNAME, rc ) ;
               bob.append( OMA_FIELD_SVCNAME, pStr ) ;
               _standalone.insert( 
                  pair<string, BSONObj>( string(ROLE_STANDALONE),
                                         bob.obj() ) ) ;
            }
            else
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "Unknown role for remove db business" ) ;
               goto error ;
            }
         }
         cataInfoBuilder.appendArray( OMA_FIELD_CATAADDR, bab.arr() ) ;
         _cataAddrInfo = cataInfoBuilder.obj() ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   INT32 _omaStartRmDBBusTaskJob::init2()
   {
      INT32 rc                         = SDB_OK ;
      _omaTaskMgr *pTaskMgr            = getTaskMgr() ;
      
      rc = pTaskMgr->removeTask ( OMA_TASK_NAME_REMOVE_DB_BUSINESS ) ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "Failed to remove task[%s], "
                     "rc = %d", OMA_TASK_NAME_REMOVE_DB_BUSINESS, rc ) ;
         goto error ;
      }
      _pTask = SDB_OSS_NEW _omaRmDBBusTask( _taskID ) ;
      if ( !_pTask )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "Failed to create remove db business task, "
                     "rc = %d", rc ) ;
         goto error ;
      }
      pTaskMgr->addTask( _pTask, _taskID ) ;
      rc = _pTask->init( _isStandalone, _standalone, _coord,
                         _catalog, _data, _cataAddrInfo ) ;
      if ( rc  )
      {
         PD_LOG_MSG( PDERROR, "Failed to init remove db busniness task, "
                     "rc = %d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaStartRmDBBusTaskJob::doit()
   {
      INT32 rc = SDB_OK ;
      rc = _pTask->doit() ;
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "Failed to do remove db busniness task, "
                     "rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }



   _omaInsDBBusTaskRbJob::_omaInsDBBusTaskRbJob (
                                              BOOLEAN isStandalone,
                                              string &vCoordSvcName,
                                              _omaInsDBBusTask *pTask )
   {
      _name = OMA_JOB_ROLLBACK_INSTALL_DB_BUSINESS ;
      _isStandalone = isStandalone ;
      _vCoordSvcName = vCoordSvcName ;
      _pTask = pTask ;
   }

   _omaInsDBBusTaskRbJob::~_omaInsDBBusTaskRbJob()
   {
   }

   RTN_JOB_TYPE _omaInsDBBusTaskRbJob::type () const
   {
      return RTN_JOB_INSDBBUSTASKRB ;
   }

   const CHAR* _omaInsDBBusTaskRbJob::name () const
   {
      return _name.c_str() ;
   }

   BOOLEAN _omaInsDBBusTaskRbJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaInsDBBusTaskRbJob::doit()
   {
      INT32 rc = SDB_OK ;
      const CHAR *pErrMsg = NULL ;
      RollbackInfo info ;
      BSONObj retObj ;

      _pTask->setTaskStage( OMA_OPT_ROLLBACK ) ;
      rc = _getRollbackInfo ( info ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to get rollback info, rc = %d", rc ) ;
         goto error ;
      }
      if ( _isStandalone )
      {
         rc = _rollbackStandalone( _vCoordSvcName,
                                   info._standaloneRollbackInfo ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rollback standalone, rc = %d", rc ) ;
            goto error ;
         }
      }
      else
      {
         rc = _rollbackDataNode ( _vCoordSvcName,
                                  info._dataGroupRollbackInfo ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rollback data nodes, rc = %d", rc ) ;
            goto error ;
         }
         rc = _rollbackCoord ( _vCoordSvcName,
                               info._coordRollbackInfo ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rollback coord nodes, rc = %d", rc ) ;
            goto error ;
         }
         rc = _rollbackCatalog ( _vCoordSvcName,
                                 info._catalogRollbackInfo ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to rollback catalog nodes, rc = %d", rc ) ;
            goto error ;
         }
      }
      _pTask->setIsRollbackFinish( TRUE ) ;
 
   done:
      if ( _isStandalone )
      {
         if ( _pTask->getIsRollbackFinish() )
         {
            _pTask->setIsTaskFinish( TRUE ) ;
         }
         else if ( _pTask->getIsRollbackFail() )
         {
            _pTask->setIsTaskFail( TRUE ) ;
         }
         else
         {
            PD_LOG ( PDERROR, "Task[%s] in a unknown status",
                     _pTask->taskName() ) ;
#if defined (_DEBUG)
            ossPanic() ;
#endif
            rc = SDB_OMA_TASK_FAIL ;
            goto error ;
         }
      }
      else
      {
         if ( _pTask->getIsRollbackFinish() )
         {
            _pTask->removeVirtualCoord() ;
         }
         else if ( _pTask->getIsRollbackFail() )
         {
            _pTask->setIsTaskFail( TRUE ) ;
         }
      }
      return rc ;
   error:
      _pTask->setIsRollbackFail( TRUE ) ;
      pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
      if ( NULL == pErrMsg )
      {
         _pTask->setErrDetail( "Failed to rollback in add db business task, "
                               "please do it manually" ) ;
      }
      else
      {
         _pTask->setErrDetail ( pErrMsg ) ;
      }
      goto done ;
   }

   INT32 _omaInsDBBusTaskRbJob::_getRollbackInfo( RollbackInfo &info )
   {
      INT32 rc = SDB_OK ;
      rc = _pTask->getInstalledNodeResult ( ROLE_STANDALONE,
                                            info._standaloneRollbackInfo ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get standalone rollback info, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = _pTask->getInstalledNodeResult ( ROLE_COORD,
                                            info._coordRollbackInfo ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get coord rollback info, rc = %d", rc ) ;
         goto error ;
      }
      rc = _pTask->getInstalledNodeResult ( ROLE_CATA,
                                            info._catalogRollbackInfo ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get catalog rollback info, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = _pTask->getInstalledNodeResult ( ROLE_DATA,
                                            info._dataGroupRollbackInfo ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get datagroup rollback info, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      
   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaInsDBBusTaskRbJob::_rollbackStandalone (
                                  string &vCoordSvcName,
                                  map< string, vector< InstalledNode> > &info )
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      BSONObj retObj ;
      CHAR desc [OMA_BUFF_SIZE + 1] = { 0 } ;
      const CHAR *pErrMsg = NULL ;
      _omaRunRollbackStandaloneJob rollbackStandalone ( vCoordSvcName, info ) ;
      map< string, vector< InstalledNode> >::iterator it = info.begin() ;
      
      if ( ( 1 != info.size() ) && ( string( ROLE_STANDALONE) != it->first ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Invalid standalone's rollback info" ) ;
         goto error ;
      }
      rc = rollbackStandalone.init( NULL ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to init to rollback create standalone "
                      "rc = %d", rc ) ;
         goto error ;
      }
      rc = rollbackStandalone.doit( retObj ) ;
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
                      "Failed to rollback standalone" ) ;
         PD_LOG_MSG ( PDERROR, "%s: %s", desc, pErrMsg ) ;         
         goto error ;
      }
      PD_LOG ( PDEVENT, "The rollback standalone's result is: %s",
               retObj.toString(FALSE, TRUE).c_str() ) ;

   done:
      return rc ;
   error:
      goto done ; 
   }

   INT32 _omaInsDBBusTaskRbJob::_rollbackCoord (
                                  string &vCoordSvcName,
                                  map< string, vector< InstalledNode> > &info )
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

   INT32 _omaInsDBBusTaskRbJob::_rollbackCatalog (
                                  string &vCoordSvcName,
                                  map< string, vector< InstalledNode> > &info )
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

   INT32 _omaInsDBBusTaskRbJob::_rollbackDataNode (
                                  string &vCoordSvcName,
                                  map< string, vector< InstalledNode> > &info )
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



   _omaRemoveVirtualCoordJob::_omaRemoveVirtualCoordJob (
                                                    const CHAR *vCoordSvcName,
                                              _omaInsDBBusTask *pTask )
   {
      _vCoordSvcName  = vCoordSvcName ;
      _pTask = pTask ;
   }

   _omaRemoveVirtualCoordJob::~_omaRemoveVirtualCoordJob()
   {
   }

   RTN_JOB_TYPE _omaRemoveVirtualCoordJob::type () const
   {
      return RTN_JOB_REMOVEVIRTUALCOORD ;
   }

   const CHAR* _omaRemoveVirtualCoordJob::name () const
   {
      return OMA_JOB_REMOVE_VIRTUAL_COORD ;
   }

   BOOLEAN _omaRemoveVirtualCoordJob::muteXOn ( const _rtnBaseJob *pOther )
   {
      return FALSE ;
   }

   INT32 _omaRemoveVirtualCoordJob::doit()
   {
      INT32 rc = SDB_OK ;
      INT32 tmpRc = SDB_OK ;
      BSONObj removeRet ;
      const CHAR *pErrMsg = NULL ;
      string str ;
      _omaRemoveVirtualCoord removeVCoord( _vCoordSvcName.c_str() ) ;
      rc = removeVCoord.removeVirtualCoord ( removeRet ) ;
      if ( rc )
      {
         tmpRc = omaGetStringElement( removeRet, OMA_FIELD_DETAIL, &pErrMsg ) ;
         if ( rc )
         {
            pErrMsg = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            if ( NULL == pErrMsg )
            {
               pErrMsg = "" ;
            }
         }
         _pTask->setIsRemoveVCoordFail( TRUE ) ;
         str += "Failed to remove temporary coord: " ;
         str += pErrMsg ;
         _pTask->setErrDetail( str.c_str() ) ;
         PD_LOG_MSG ( PDERROR, "%s", str.c_str() ) ;
         goto error ;
      }
      else
      {
         PD_LOG ( PDEVENT, "Succeed to remove temporary coord" ) ;
         _pTask->setIsRemoveVCoordFinish( TRUE ) ;
      } 
   done:
      return rc ;
   error:
      goto done ;
   }


   INT32 startAddHostJob( string jobName, _omaAddHostTask *pTask, EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaAddHostJob *pJob = NULL ;

      pJob = SDB_OSS_NEW _omaAddHostJob( jobName, pTask ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for "
                  "adding host job" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start add host job, rc = %d", rc ) ;
         goto done ;
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startRbHostJob( string jobName, _omaAddHostTask *pTask, EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaRbHostJob *pJob = NULL ;

      pJob = SDB_OSS_NEW _omaRbHostJob( jobName, pTask ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for "
                  "adding host job" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start remove host job, rc = %d", rc ) ;
         goto done ;
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startAddHostTaskJob ( const CHAR *pAddHostInfo, EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaStartAddHostTaskJob *pJob = NULL ;

      try
      {
         BSONObj info( pAddHostInfo ) ;
         pJob = SDB_OSS_NEW _omaStartAddHostTaskJob( info ) ;
         if ( !pJob )
         {
            PD_LOG ( PDERROR, "Failed to alloc memory for starting "
                     "add host job" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         rc = pJob->init() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failded to init in add host task, "
                     "rc = %d", rc) ;
            goto error ;
         }
         rc = pJob->init2() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failded to init in add host task, "
                     "rc = %d", rc) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR, "Occur error, exception is: %s", e.what() ) ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start add host job, rc = %d", rc ) ;
         goto done ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startCreateStandaloneJob ( _omaInsDBBusTask *pTask,
                                    EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaCreateStandaloneJob *pJob = NULL ;
      pJob = SDB_OSS_NEW _omaCreateStandaloneJob( pTask ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for creating "
                  "standalone job" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start job, rc = %d", rc ) ;
         goto done ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startCreateCatalogJob ( _omaInsDBBusTask *pTask,
                                 EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaCreateCatalogJob *pJob = NULL ;
      pJob = SDB_OSS_NEW _omaCreateCatalogJob( pTask ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for creating catalog job" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start job, rc = %d", rc ) ;
         goto done ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startCreateCoordJob ( _omaInsDBBusTask *pTask,
                               EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaCreateCoordJob *pJob = NULL ;
      pJob = SDB_OSS_NEW _omaCreateCoordJob( pTask ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for creating coord job" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start job, rc = %d", rc ) ;
         goto done ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startCreateDataJob ( const CHAR *pGroupName,
                              _omaInsDBBusTask *pTask,
                              EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaCreateDataJob *pJob = NULL ;
      pJob = SDB_OSS_NEW _omaCreateDataJob( pGroupName, pTask ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for "
                  "creating data node job" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start job, rc = %d", rc ) ;
         goto done ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startInsDBBusTaskJob ( const CHAR *pInstallInfo, EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaStartInsDBBusTaskJob *pJob = NULL ;

      try
      {
         BSONObj info( pInstallInfo ) ;
         pJob = SDB_OSS_NEW _omaStartInsDBBusTaskJob( info ) ;
         if ( !pJob )
         {
            PD_LOG ( PDERROR, "Failed to alloc memory for starting "
                     "install db business job" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         rc = pJob->init() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failded to init install db business task, "
                     "rc = %d", rc) ;
            goto error ;
         }
         rc = pJob->init2() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failded to init install db business task, "
                     "rc = %d", rc) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR, "Occur error, exception is: %s", e.what() ) ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start install db business job,"
                  "rc = %d", rc ) ;
         goto done ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startRmDBBusTaskJob ( const CHAR *pUninstallInfo, EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaStartRmDBBusTaskJob *pJob = NULL ;

      try
      {
         BSONObj info( pUninstallInfo ) ;
         pJob = SDB_OSS_NEW _omaStartRmDBBusTaskJob( info ) ;
         if ( !pJob )
         {
            PD_LOG ( PDERROR, "Failed to alloc memory for starting "
                     "install db business job" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         rc = pJob->init() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failded to init install db business task, "
                     "rc = %d", rc) ;
            goto error ;
         }
         rc = pJob->init2() ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failded to init install db business task, "
                     "rc = %d", rc) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG ( PDERROR, "Occur error, exception is: %s", e.what() ) ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start job, rc = %d", rc ) ;
         goto done ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startInsDBBusTaskRbJob ( BOOLEAN isStandalone,
                                  string &vCoordSvcName,
                                  _omaInsDBBusTask *pTask,
                                  EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaInsDBBusTaskRbJob *pJob = NULL ;
      pJob = SDB_OSS_NEW _omaInsDBBusTaskRbJob( isStandalone, 
                                                vCoordSvcName, pTask ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for "
                  "installing db business rollback job" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start job, rc = %d", rc ) ;
         goto done ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 startRemoveVirtualCoordJob ( const CHAR *vCoordSvcName,
                                      _omaInsDBBusTask *pTask,
                                      EDUID *pEDUID )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN returnResult = FALSE ;
      _omaRemoveVirtualCoordJob *pJob = NULL ;
      pJob = SDB_OSS_NEW _omaRemoveVirtualCoordJob( vCoordSvcName,
                                                    pTask ) ;
      if ( !pJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for "
                  "removing temporary coord job" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID,
                                     returnResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start job, rc = %d", rc ) ;
         goto done ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }
*/

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
         case OMA_TASK_INSTALL_DB :
            pTask = SDB_OSS_NEW _omaInstDBBusTask( taskID ) ;
            break ;
         case OMA_TASK_REMOVE_DB :
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
