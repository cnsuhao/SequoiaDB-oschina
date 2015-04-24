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
#include <set>
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
         string getTmpCoordSvcName() ;
         void notifyUpdateProgress() ;
         string getDataRGToInst() ;
         InstDBBusInfo* getDataNodeInfo( string &groupName ) ;

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
         void  _setRetErr( INT32 errNum ) ;

      private:
         INT32 _saveTmpCoordInfo( BSONObj &info ) ;
         INT32 _installTmpCoord() ;
         INT32 _removeTmpCoord() ;
         INT32 _installStandalone() ;
         INT32 _rollback() ;
         INT32 _rollbackStandalone() ;
         INT32 _rollbackCatalog() ;
         INT32 _rollbackCoord() ;
         INT32 _rollbackDataRG () ;
         INT32 _installCatalog() ;
         INT32 _installCoord() ;
         INT32 _installDataRG() ;

      private:
         BSONObj                                _instDBBusRawInfo ;
         vector<InstDBBusInfo>                  _standalone ;
         vector<InstDBBusInfo>                  _catalog ;
         vector<InstDBBusInfo>                  _coord ;
         map< string, vector<InstDBBusInfo> >   _mapGroups ;
         
         string                            _tmpCoordSvcName ;
         BSONObj                           _tmpCoordCfgObj ;

         BOOLEAN                           _isStandalone ;
         
         INT32                             _nodeSerialNum ;
         ossSpinSLatch                     _taskLatch ;
         ossEvent                          _taskEvent ;
         UINT64                            _eventID ;

         INT32                             _progress ;
         INT32                             _errno ;
         CHAR                              _detail[OMA_BUFF_SIZE + 1] ;

         set<string>                       _existGroups ;
         
   } ;
   typedef _omaInstDBBusTask omaInstDBBusTask ;
   
   /*
      remove db business task
   */
   class _omaRemoveDBBusTask : public _omaTask
   {
      public:
         _omaRemoveDBBusTask ( INT64 taskID ) ;
         virtual ~_omaRemoveDBBusTask () ;

      public:
         INT32 init( const BSONObj &info, void *ptr = NULL ) ;
         INT32 doit() ;

      public:
         INT32 updateProgressToTask( INT32 serialNum, InstDBResult &instResult,
                                     BOOLEAN needToNotify = FALSE ) ;
         string getTmpCoordSvcName() ;
         void notifyUpdateProgress() ;
/*
         string getDataRGToInst() ;
         InstDBBusInfo* getDataNodeInfo( string &groupName ) ;
*/

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
         void  _setRetErr( INT32 errNum ) ;

      private:
         INT32 _saveTmpCoordInfo( BSONObj &info ) ;
         INT32 _installTmpCoord() ;
         INT32 _removeTmpCoord() ;
         INT32 _removeStandalone() ;
         INT32 _removeCatalog() ;
         INT32 _removeCoord() ;
         INT32 _removeDataRG() ;

      private:
         BSONObj                                _removeDBBusRawInfo ;
         vector<InstDBBusInfo>                  _standalone ;
         vector<InstDBBusInfo>                  _catalog ;
         vector<InstDBBusInfo>                  _coord ;
         map< string, vector<InstDBBusInfo> >   _mapGroups ;

      private:
         string                            _tmpCoordSvcName ;
         BSONObj                           _tmpCoordCfgObj ;
         BOOLEAN                           _isStandalone ;
         INT32                             _nodeSerialNum ;
         ossSpinSLatch                     _taskLatch ;
         ossEvent                          _taskEvent ;
         UINT64                            _eventID ;
         
      private:
         INT32                             _progress ;
         INT32                             _errno ;
         CHAR                              _detail[OMA_BUFF_SIZE + 1] ;

   } ;
   typedef _omaRemoveDBBusTask omaRemoveDBBusTask ;


}




#endif
