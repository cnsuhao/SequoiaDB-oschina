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
#include "ossEvent.hpp"
#include "omagentTaskBase.hpp"
#include "../bson/bson.h"
#include "omagent.hpp"
#include <map>
#include <vector>
#include <string>

using namespace std ;
using namespace bson ;

#define OMA_TASK_NAME_ADD_HOST                "add host task"
#define OMA_TASK_NAME_INSTALL_DB_BUSINESS     "install db business task"
#define OMA_TASK_NAME_REMOVE_DB_BUSINESS      "remove db business task"


namespace engine
{
   /*
      add host task
   */
   class _omaAddHostTask : public _omaTask
   {
      public:
         _omaAddHostTask ( INT64 taskID ) ;
         virtual ~_omaAddHostTask () ;

      public:
         INT32 init( const BSONObj &info, void *ptr = NULL ) ;
         INT32 doit() ;

      public:
         BOOLEAN regSubTask( string subTaskName ) ;
         AddHostInfo* getAddHostItem() ;
         INT32 updateProgressToTask( INT32 serialNum,
                                     AddHostResultInfo &resultInfo ) ;
         void notifyUpdateProgress() ;
         
      private:
         INT32 _initAddHostInfo( BSONObj &info ) ;
         void _initAddHostResult() ;
         INT32 _checkHostInfo() ;
         INT32 _addHost() ;
         INT32 _waitAndUpdateProgress() ;
         void _buildUpdateTaskObj( BSONObj &retObj ) ; 
         INT32 _updateProgressToOM() ;
         BOOLEAN _isTaskFinish() ;
         void _setRetErr( INT32 errNum ) ;

      private:
         BSONObj                           _addHostRawInfo ;
         vector<AddHostInfo>               _addHostInfo ;
         map< INT32, AddHostResultInfo >   _addHostResult ;

         ossSpinSLatch                     _taskLatch ;
         ossEvent                          _taskEvent ;
         UINT64                            _eventID ; 

         INT32                             _progress ;
         INT32                             _errno ;
         CHAR                              _detail[OMA_BUFF_SIZE + 1] ;
   } ;
   typedef _omaAddHostTask omaAddHostTask ;


   /*
      install db business task
   */
   class _omaInstDBBusTask: public _omaTask
   {
      public:
         _omaInstDBBusTask( INT64 taskID ) ;
         virtual ~_omaInstDBBusTask () ;

      public:
         INT32 init( const BSONObj &info, void *ptr = NULL ) ;
         INT32 doit() ;

      public:
         INT32 updateProgressToTask( INT32 serialNum, InstDBResult &instResult,
                                     BOOLEAN needToNotify = FALSE ) ;

      private:
         INT32 _initInstInfo( BSONObj &info ) ;
         INT32 _initInstAndResultInfo( BSONObj &hostInfo,
                                       InstDBBusInfo &info ) ;
         INT32 _restoreResultInfo() ;
         INT32 _waitAndUpdateProgress() ;
         void  _buildResultInfo( vector<InstDBBusInfo> &info,
                                 BSONArrayBuilder &bab ) ;
         void  _buildUpdateTaskObj( BSONObj &retObj ) ;
         INT32 _updateProgressToOM() ;
         BOOLEAN _isTaskFinish() ;
         

      private:
         INT32 _saveTmpCoordInfo( BSONObj &info ) ;
         INT32 _installTmpCoord() ;
         INT32 _installStandalone() ;
         INT32 _rollback() ;
         INT32 _rollbackStandalone() ;
/*
         INT32 _installCatalog() ;
         INT32 _installCoord() ;
         INT32 _installData() ;
*/

      private:
         BSONObj                                _instDBBusRawInfo ;
         vector<InstDBBusInfo>                  _standalone ;
         vector<InstDBBusInfo>                  _catalog ;
         vector<InstDBBusInfo>                  _coord ;
         map< string, vector<InstDBBusInfo> >   _mapGroups ;
         
         string                            _tmpCoordSvcName ;

         BOOLEAN                           _isStandalone ;
         
         INT32                             _nodeSerialNum ;
         ossSpinSLatch                     _taskLatch ;
         ossEvent                          _taskEvent ;
         UINT64                            _eventID ; 

         INT32                             _progress ;
         INT32                             _errno ;
         CHAR                              _detail[OMA_BUFF_SIZE + 1] ;
         
   } ;
   typedef _omaInstDBBusTask omaInstDBBusTask ;
   
/*
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
         INT32 doit() ;
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

         vector<BSONObj>                      _standalone ;
         vector<BSONObj>                      _catalog ;
         vector<BSONObj>                      _coord ;
         map< string, vector<BSONObj> >       _mapGroups ;
         InstallResult                        _standaloneResult ;
         InstallResult                        _catalogResult ;
         InstallResult                        _coordResult ;
         map< string, InstallResult >         _mapGroupsResult ;
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
         INT32 doit() ;
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

         map<string, BSONObj>                 _standalone ;
         map<string, BSONObj>                 _catalog ;
         map<string, BSONObj>                 _coord ;
         map<string, BSONObj>                 _data ;
         UninstallResult                      _standaloneResult ;
         UninstallResult                      _catalogResult ;
         UninstallResult                      _coordResult ;
         map< string, UninstallResult >       _mapDataResult ;
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
*/
}




#endif
