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

   Source File Name = omagentTask.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENTTASK_HPP_
#define OMAGENTTASK_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossUtil.hpp"
#include "ossLatch.hpp"
#include "../bson/bson.h"
#include "omagent.hpp"
#include <map>
#include <vector>
#include <string>

using namespace std ;
using namespace bson ;

#define OMA_INVALID_TASKID     (0)

#define OMA_TASK_NAME_ADD_HOST                "add host task"
#define OMA_TASK_NAME_INSTALL_DB_BUSINESS     "install db business task"
#define OMA_TASK_NAME_REMOVE_DB_BUSINESS      "remove db business task"


namespace engine
{
   enum OMA_TASK_TYPE
   {
      OMA_TASK_ADD_HOST           = 0, // add host
      OMA_TASK_INSTALL_DB         = 1, // install db business
      OMA_TASK_REMOVE_DB          = 2, // remove db business

      OMA_TASK_UNKNOW             = 255
   } ;

   enum OMA_TASK_STATUS
   {
      OMA_TASK_STATUS_READY       = 0, // when initially created
      OMA_TASK_STATUS_RUN         = 1, // when starts running
      OMA_TASK_STATUS_FINISH      = 2, // when finish doing something
      OMA_TASK_STATUS_FAIL        = 3, // when error happen

      OMA_TASK_STATUS_END         = 10 // nothing should have this status
   } ;

   /*
      omagent task
   */
   class _omaTask : public SDBObject
   {
      public:
         _omaTask ( UINT64 taskID ) : _taskID ( taskID )
         {
            _status = OMA_TASK_STATUS_READY ;
         }
         virtual ~_omaTask () {}

         UINT64          taskID () const { return _taskID ; }

         OMA_TASK_STATUS status () ;

         void setStatus( OMA_TASK_STATUS status ) ;

         INT32 setJobStatus( string &name, OMA_JOB_STATUS status ) ;

         INT32 getJobStatus( string &name, OMA_JOB_STATUS &status ) ;

         virtual INT32 queryProgress( BSONObj &progress ) { return SDB_OK ; }

      public:
         virtual OMA_TASK_TYPE taskType () const = 0 ;

         virtual const CHAR*   taskName () const = 0 ;

      private:
         ossSpinSLatch                        _latch ;

      protected:
         UINT64                               _taskID ;
         OMA_TASK_STATUS                      _status ;
//         ossSpinSLatch                        _taskLatch ;
//         ossSpinSLatch                        _jobLatch ;
         map< string, OMA_JOB_STATUS >        _jobStatus ;
   } ;
   typedef _omaTask omaTask ;

   /*
      task manager
   */
   class _omaTaskMgr : public SDBObject
   {
      public:
         _omaTaskMgr ( UINT64 taskID = OMA_INVALID_TASKID ) ;
         ~_omaTaskMgr () ;

         UINT64     getTaskID () ;
         void       setTaskID ( UINT64 taskID ) ;

      public:
         INT32      addTask ( _omaTask *pTask,
                              UINT64 taskID = OMA_INVALID_TASKID ) ;
         INT32      removeTask ( _omaTask *pTask ) ;
         INT32      removeTask ( UINT64 taskID ) ;
         INT32      removeTask ( const CHAR *pTaskName ) ;
         _omaTask*  findTask ( UINT64 taskID ) ;

      private:
         std::map<UINT64, _omaTask*>         _taskMap ;
         ossSpinSLatch                       _taskLatch ;
         UINT64                              _taskID ;
   } ;
   typedef _omaTaskMgr omaTaskMgr ;

   /*
      get task manager
   */
   _omaTaskMgr* getTaskMgr() ;


   /*
      add host task
   */
   class _omaAddHostTask : public _omaTask
   {
      public:
         _omaAddHostTask ( UINT64 taskID ) ;
         virtual ~_omaAddHostTask () ;

      public:
         virtual OMA_TASK_TYPE taskType () const { return _taskType ; }
         virtual const CHAR*   taskName () const { return _taskName.c_str() ; }

      public:
         INT32 init( BSONObj &addHostRawInfo,
                     vector<AddHostInfo> addHostInfo ) ;
         // start job
         INT32 doit() ;
         
      public:
         // respond query of add host progress
         virtual INT32 queryProgress( BSONObj &progress ) ;

      public:
         void setTaskStage( OMA_OPT_STAGE stage ) ;
         void setIsTaskFail( BOOLEAN isFail ) ;
         BOOLEAN getIsTaskFail() ;
         void setIsAddHostFail( BOOLEAN isFail ) ;
         BOOLEAN getIsAddHostFail() ;
         void setIsTaskFinish( BOOLEAN isFinish ) ;

      public:
         AddHostInfo* getAddHostItem() ;
         AddHostInfo* getRbHostItem() ;
         BOOLEAN registerJob( string jobName ) ;
         INT32 updateJobStatus( string jobName, OMA_JOB_STATUS status ) ;
         INT32 updateProgressStatus( INT32 serialNum, AddHostPS ps,
                                     BOOLEAN isFinish = FALSE ) ;

      private:
         INT32 _checkHostInfo() ;
         INT32 _addHost() ;
         INT32 _rollback() ;
         void _getRollbackInfo() ;
         void _buildErrMsg() ;
         BOOLEAN _hasUninstallHost() ;
         void _collectProgressInfo() ;

      private:
         // add host raw info
         BSONObj                      _addHostRawInfo ;
         // add host info
         vector<AddHostInfo>          _addHostInfo ;
         // rollback host info
         vector<AddHostInfo>          _rollbackInfo ;

         ossSpinSLatch                _taskLatch ;

         string                       _taskName ;
         OMA_TASK_TYPE                _taskType ;
         OMA_OPT_STAGE                _stage ;
         BOOLEAN                      _isTaskFinish ;
         BOOLEAN                      _isAddHostFail ;
         BOOLEAN                      _isTaskFail ;
         CHAR                         _detail[OMA_BUFF_SIZE + 1] ; 
   } ;
   typedef _omaAddHostTask omaAddHostTask ;
   

   /*
      install database business
   */
   class _omaInsDBBusTask : public _omaTask
   {
      public:
         _omaInsDBBusTask ( UINT64 taskID ) ;
         virtual ~_omaInsDBBusTask () ;

      public:
         virtual OMA_TASK_TYPE taskType () const
         {
            return _taskType ;
         }
         virtual const CHAR*   taskName () const
         {
            return _taskName.c_str() ;
         }

      public:
         INT32 init( BOOLEAN isStandalone,
                     vector<BSONObj> standalone,
                     vector<BSONObj> coord,
                     vector<BSONObj> catalog,
                     vector<BSONObj> data,
                     BSONObj &otherInfo ) ;
         // start job
         INT32 doit() ;
         // respond query of install status
         virtual INT32 queryProgress( BSONObj &progress ) ;

      public:
         void setTaskStage( OMA_OPT_STAGE stage ) ;
         void setIsInstallFinish( BOOLEAN isFinish ) ;
         void setIsRollbackFinish( BOOLEAN isFinish ) ;
         void setIsRemoveVCoordFinish( BOOLEAN isFinish ) ;
         void setIsTaskFinish( BOOLEAN isFinish ) ;
         void setIsRollbackFail( BOOLEAN isFail ) ;
         void setIsInstallFail( BOOLEAN isFail ) ;
         void setIsRemoveVCoordFail( BOOLEAN isFail ) ;
         void setIsTaskFail( BOOLEAN isFail ) ;
         BOOLEAN getIsInstallFinish() ;
         BOOLEAN getIsRollbackFinish() ;
         BOOLEAN getIsRemoveVCoordFinish() ;
         BOOLEAN getIsTaskFinish() ;
         BOOLEAN getIsInstallFail() ;
         BOOLEAN getIsRollbackFail() ;
         BOOLEAN getIsRemoveVCoordFail() ;
         BOOLEAN getIsTaskFail() ;
         void setErrDetail( const CHAR *pErrDetail ) ;
         string& getVCoordSvcName() { return _vCoordSvcName ; }
         vector<BSONObj>& getInstallStandaloneInfo() ;
         vector<BSONObj>& getInstallCatalogInfo() ;
         vector<BSONObj>& getInstallCoordInfo() ;
         INT32 getInstallDataGroupInfo( string &name,
                                        vector<BSONObj> &dataGroupInstallInfo ) ;
         INT32 getInstalledNodeResult( const CHAR *pRole,
                                      map< string, vector<InstalledNode> >& info ) ;
         INT32 updateInstallStatus( BOOLEAN isFinish,
                                    INT32 retRc,
                                    const CHAR *pRole,
                                    const CHAR *pErrMsg,
                                    const CHAR *pDesc,
                                    const CHAR *pGroupName,
                                    InstalledNode *pNode ) ;
         INT32 updateInstallJobStatus( string &name, OMA_JOB_STATUS status ) ;
         INT32 rollbackInternal() ;      
         INT32 removeVirtualCoord() ;
         BOOLEAN isInstallFinish() ;
          
      private:
         INT32 _saveVCoordInfo( BSONObj &info ) ;
         INT32 _installVirtualCoord() ;
         INT32 _installStandalone() ;
         INT32 _installCatalog() ;
         INT32 _installCoord() ;
         INT32 _installData() ;

         // install info
         vector<BSONObj>                      _standalone ;
         vector<BSONObj>                      _catalog ;
         vector<BSONObj>                      _coord ;
         map< string, vector<BSONObj> >       _mapGroups ;
         // install result
         InstallResult                        _standaloneResult ;
         InstallResult                        _catalogResult ;
         InstallResult                        _coordResult ;
         map< string, InstallResult >         _mapGroupsResult ;
         // virtual coord info
         string                               _vCoordSvcName ;

         ossSpinSLatch                        _taskLatch ;
         ossSpinSLatch                        _taskLatch2 ;
         ossSpinSLatch                        _jobLatch ;
         OMA_TASK_TYPE                        _taskType ;
         string                               _taskName ;
         OMA_OPT_STAGE                        _stage ;
         BOOLEAN                              _isStandalone ;
         BOOLEAN                              _isInstallFinish ;
         BOOLEAN                              _isRollbackFinish ;
         BOOLEAN                              _isRemoveVCoordFinish ;
         BOOLEAN                              _isTaskFinish ;
         BOOLEAN                              _isInstallFail ;
         BOOLEAN                              _isRollbackFail ;
         BOOLEAN                              _isRemoveVCoordFail ;
         BOOLEAN                              _isTaskFail ;
         CHAR                                 _detail[OMA_BUFF_SIZE + 1] ; 
   } ;
   typedef _omaInsDBBusTask omaInsDBBusTask ;

   /*
      remove database business
   */
   class _omaRmDBBusTask : public _omaTask
   {
      public:
         _omaRmDBBusTask ( UINT64 taskID ) ;
         virtual ~_omaRmDBBusTask () ;

      public:
         virtual OMA_TASK_TYPE taskType () const
         {
            return _taskType ;
         }
         virtual const CHAR*   taskName () const
         {
            return _taskName.c_str() ;
         }

      public:
         INT32 init( BOOLEAN isStandalone,
                     map<string, BSONObj> standalone,
                     map<string, BSONObj> coord,
                     map<string, BSONObj> catalog,
                     map<string, BSONObj> data,
                     BSONObj &otherInfo ) ;
         // start job
         INT32 doit() ;
         // respond query of remove task status
         virtual INT32 queryProgress( BSONObj &progress ) ;

      public:
         void setIsUninstallFinish( BOOLEAN isFinish ) ;
         void setIsRemoveVCoordFinish( BOOLEAN isFinish ) ;
         void setIsTaskFinish( BOOLEAN isFinish ) ;
         void setIsUninstallFail( BOOLEAN isFail ) ;
         void setIsRemoveVCoordFail( BOOLEAN isFail ) ;
         void setIsTaskFail( BOOLEAN isFail ) ;
         BOOLEAN getIsUninstallFinish() ;
         BOOLEAN getIsRemoveVCoordFinish() ;
         BOOLEAN getIsTaskFinish() ;
         BOOLEAN getIsUninstallFail() ;
         BOOLEAN getIsRemoveVCoordFail() ;
         BOOLEAN getIsTaskFail() ;
         void setErrDetail( const CHAR *pErrDetail ) ;
          
      private:
         BOOLEAN _isRemoveFinish() ;
         INT32 _updateUninstallStatus( BOOLEAN isFinish,
                                       INT32 retRc,
                                       const CHAR *pRole,
                                       const CHAR *pErrMsg,
                                       const CHAR *pDesc,
                                       const CHAR *pGroupName ) ;
         INT32 _saveVCoordInfo( BSONObj &info ) ;
         INT32 _installVirtualCoord() ;
         INT32 _removeVirtualCoord() ;
         INT32 _uninstallStandalone() ;
         INT32 _uninstallCatalog() ;
         INT32 _uninstallCoord() ;
         INT32 _uninstallData() ;

         // uninstall info
         map<string, BSONObj>                 _standalone ;
         map<string, BSONObj>                 _catalog ;
         map<string, BSONObj>                 _coord ;
         map<string, BSONObj>                 _data ;
         // uninstall result
         UninstallResult                      _standaloneResult ;
         UninstallResult                      _catalogResult ;
         UninstallResult                      _coordResult ;
         map< string, UninstallResult >       _mapDataResult ;
         // virtual coord info
         BSONObj                              _cataAddrInfo ;
         string                               _vCoordSvcName ;
         string                               _taskName ;
         OMA_TASK_TYPE                        _taskType ;
         BOOLEAN                              _isStandalone ;
         BOOLEAN                              _isTaskFinish ;
         BOOLEAN                              _isUninstallFinish ;
         BOOLEAN                              _isRemoveVCoordFinish ;
         BOOLEAN                              _isTaskFail ;
         BOOLEAN                              _isUninstallFail ;
         BOOLEAN                              _isRemoveVCoordFail ;
         CHAR                                 _detail[OMA_BUFF_SIZE + 1] ;
   } ;
   typedef _omaRmDBBusTask omaRmDBBusTask ;

}




#endif
