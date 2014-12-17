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

   Source File Name = omTaskManager.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/12/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OM_TASKMANAGER_HPP_
#define OM_TASKMANAGER_HPP_

#include "rtnCB.hpp"
#include "pmd.hpp"
#include "dmsCB.hpp"
#include "omManager.hpp"
#include <map>
#include <string>

#include <vector>
#include <string>
#include <map>

using namespace std ;
using namespace bson ;

namespace engine
{
   //class omTaskManager ;

   struct omTaskInfo
   {
      bool               isEnable ;
      bool               isFinished ;
      UINT64             taskID ;
      string             taskType ;
      string             taskStatus ;
      string             agentHost ;
      string             agentService ;
      BSONObj            taskInfo ;
      BSONObj            progress ;
      string             detail ;
   } ;

   class omTaskBase : public SDBObject
   {
      public:
         omTaskBase( omManager *om ) ;
         virtual ~omTaskBase() ;

      public:
         virtual INT32     cancel() = 0 ;

         virtual INT32     enable() = 0 ;

         virtual INT32     finish() = 0 ;

         virtual INT32     getProgress( bool &isEnable, bool &isFinish, 
                                       string &status, BSONObj &progress,
                                       string &detail ) = 0 ;

         virtual string    getType() = 0 ;

         virtual UINT64    getTaskID() = 0 ;

         virtual string    getStatus() = 0 ;

         virtual INT32     updateProgress() = 0 ;

         virtual BOOLEAN   isEnable() = 0 ;

         virtual BOOLEAN   isFinish() = 0 ;

         virtual void      getAllTaskInfo( omTaskInfo &taskInfo ) = 0 ;

         void              setDetail( string detail ) ;
         string            getDetail() ;

      protected:
         INT32             _saveFinishTask() ;
         INT32             _getProgressFromAgent( INT32 &flag, 
                                                  BSONObj &response ) ;
         INT32             _receiveFromAgent( pmdRemoteSession *remoteSession,
                                              SINT32 &flag, BSONObj &result ) ;
         INT32             _sendMsgToAgent( const string &host,
                                            const string &port,
                                            pmdRemoteSession *remoteSession, 
                                            MsgHeader *pMsg ) ;
         void              _clearSession( _pmdEDUCB *cb, 
                                          pmdRemoteSession *remoteSession ) ;

      protected:
         omManager         *_om ;
         omTaskInfo        _omTaskInfo ;
   };

   class omInstallTask : public omTaskBase
   {
      public:
         omInstallTask( omManager *om ) ;
         virtual ~omInstallTask() ;

      public:
         // create a new task, insert into table OM_CS_DEPLOY_CL_TASKINFO
         INT32             init( const string &agentHost, 
                                 const string &agentService,
                                 const BSONObj &conf, UINT64 taskID ) ;

         INT32             restore( BSONObj &record ) ;

      public:
         virtual INT32     cancel() ;

         virtual INT32     enable() ;

         virtual INT32     finish() ;

         virtual INT32     getProgress( bool &isEnable, bool &isFinish, 
                                        string &status, BSONObj &progress,
                                        string &detail ) ;

         virtual string    getType() ;

         virtual UINT64    getTaskID() ;

         virtual string    getStatus() ;

         virtual INT32     updateProgress() ;

         virtual BOOLEAN   isEnable() ;

         virtual BOOLEAN   isFinish() ;

         virtual void      getAllTaskInfo( omTaskInfo &taskInfo ) ;

      protected:
         INT32             _insertTask() ;
         
      private:
         INT32             _storeBusinessInfo() ;
         BOOLEAN           _isHostConfExist( const string &hostName, 
                                             const string &businessName ) ;
         INT32             _appendConfigure( const string &hostName, 
                                             const string &businessName,
                                             BSONObj &oneNode ) ;
         INT32             _insertConfigure( const string &hostName, 
                                             const string &businessName ,
                                             BSONObj &oneNode ) ;
         INT32             _storeConfigInfo() ;

         INT32             _finishTask() ;

         INT32             _checkTaskResponse( BSONObj &response ) ;

         void              _updateHostOMVersion( const string &hostName ) ;

      protected:
         //TODO: to protect the progress ;
         ossSpinSLatch     _lock ;
      /*
         _omTaskInfo.taskInfo:
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
      */
   } ;

   class omUninstallTask : public omInstallTask
   {
      public:
         omUninstallTask( omManager *om ) ;
         virtual ~omUninstallTask() ;

      public:
         INT32             init( const string &agentHost, 
                                 const string &agentService, 
                                 const BSONObj &conf, UINT64 taskID ) ;
      public:
         virtual INT32     updateProgress() ;

         virtual INT32     finish() ;

      

      private:
         INT32             _finishUninstallTask() ;
         INT32             _removeBusinessInfo() ;
         INT32             _removeConfigInfo() ;

      /*
         _omTaskInfo.taskInfo:
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
      */
   } ;

   class omAddHostTask : public omInstallTask
   {
      public:
         omAddHostTask( omManager *om ) ;
         virtual ~omAddHostTask() ;

      public:
         INT32             init( const string &agentHost, 
                                 const string &agentService, 
                                 const BSONObj &conf, UINT64 taskID ) ;
      public:
         virtual INT32     updateProgress() ;

      private:
         INT32             _finishAddHostTask() ;
         INT32             _storeHostInfo() ;

      private:
         string            _clusterName ;

      /*
         _omTaskInfo.taskInfo:
         {
            "HostInfo":
            [
               {
                  "HostName":"host1", "ClusterName":"c1", "IP":"", "OS":"",
                  "CPU":{}, "NET":{}...
               }
               ,...
            ]
         }
      */
   } ;

   class omTaskManager : public SDBObject
   {
      public:
         omTaskManager( omManager *om ) ;
         ~omTaskManager() ;

      public:
         INT32             createInstallTask( const string &agentHost, 
                                              const string &agentService, 
                                              const BSONObj &confValue, 
                                              UINT64 &taskID ) ;
         INT32             createUninstallTask( const string &agentHost, 
                                                const string &agentService, 
                                                const BSONObj &confValue,
                                                UINT64 &taskID ) ;
         INT32             createAddHostTask( const string &agentHost, 
                                              const string &agentService, 
                                              const BSONObj &confValue, 
                                              UINT64 &taskID ) ;
         INT32             restoreTask() ;

         INT32             cancelTask( UINT64 taskID, const string &detail ) ;

         INT32             enableTask( UINT64 taskID ) ;

         INT32             finishTask( UINT64 taskID ) ;

         INT32             getProgress( UINT64 taskID, string &taskType,
                                        bool &isEnable, bool &isFinish, 
                                        string &status, BSONObj &progress,
                                        string &detail ) ;

         void              getTaskInfo( const string &agentHost, 
                                        const string &agentService, 
                                        list<omTaskInfo> &taskList ) ;

         INT32             run() ;

      private:
         BOOLEAN           _isTaskTypeExist( string taskType, UINT64 &taskID ) ;
         UINT64            _generateTaskID() ;
         INT32             _restoreInstallTask( BSONObj &record ) ;
         INT32             _restoreUninstallTask( BSONObj &record ) ;
         void              _addTaskToMap( omTaskBase *task ) ;
         INT32             _getTaskRecord( UINT64 taskID, BSONObj &result ) ;

      private:
         omManager                *_om ;
         ossSpinSLatch            _lock ;
         map< UINT64, boost::shared_ptr<omTaskBase> > _mapTasks ;
         typedef map< UINT64, boost::shared_ptr<omTaskBase> >::iterator 
                                                            MAP_TASK_INTER ;
         typedef map< UINT64, boost::shared_ptr<omTaskBase> >::value_type 
                                                            MAP_TASK_VALUETYPE ;
         UINT64                   _maxTaskID ;
   } ;
}

#endif /* OM_TASKMANAGER_HPP_ */



