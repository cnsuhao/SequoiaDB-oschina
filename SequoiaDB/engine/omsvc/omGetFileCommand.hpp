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

   Source File Name = omGetFileCommand.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/12/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OM_GETFILECOMMAND_HPP__
#define OM_GETFILECOMMAND_HPP__

#include "omCommandInterface.hpp"
#include "restAdaptor.hpp"
#include "pmdRestSession.hpp"
#include "pmdRemoteSession.hpp"
#include "rtnCB.hpp"
#include "pmd.hpp"
#include "dmsCB.hpp"
#include "omManager.hpp"
#include "omTaskManager.hpp"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/exception/all.hpp>
#include <map>
#include <string>

using namespace bson;
using namespace boost::property_tree;

namespace engine
{
   struct simpleHostInfo : public SDBObject
   {
      string hostName ;
      string clusterName ;
      string ip ;
      string user ;
      string passwd ;
      string installPath ;
      string agentPort ;
      string sshPort ;
   } ;

   struct simpleNodeInfo : public SDBObject
   {
      string hostName ;
      string svcName ;
      string role ;
   } ;

   class omAuthCommand : public omRestCommandBase
   {
      public:
         omAuthCommand( restAdaptor *pRestAdaptor, 
                        pmdRestSession *pRestSession ) ;

         ~omAuthCommand() ;

      public:
         virtual INT32   doCommand() ;

      protected:
         void            _sendOKRes2Web() ;
         void            _setOPResult( INT32 rc, const CHAR* detail ) ;
         void            _sendErrorRes2Web( INT32 rc, const CHAR* detail ) ;
         void            _sendErrorRes2Web( INT32 rc, const string &detail ) ;
         void            _decryptPasswd( const string &encryptPasswd, 
                                         const string &time,
                                         string &decryptPasswd) ;
         INT32           _getSdbUsrInfo( const string &clusterName, 
                                         string &sdbUser, 
                                         string &sdbPasswd, 
                                         string &sdbUserGroup ) ;

         INT32           _getQueryPara( BSONObj &selector, BSONObj &matcher,
                                        BSONObj &order, BSONObj &hint) ;
         INT32           _queryTable( const string &tableName, 
                                      const BSONObj &selector, 
                                      const BSONObj &matcher,
                                      const BSONObj &order, 
                                      const BSONObj &hint, SINT32 flag,
                                      SINT64 numSkip, SINT64 numReturn, 
                                      list<BSONObj> &records ) ;
         string          _getLanguage() ;
         void            _setFileLanguageSep() ;

      protected:
         restAdaptor*    _restAdaptor ;
         pmdRestSession* _restSession ;
         string          _languageFileSep ;
   };

   class omLogoutCommand : public omAuthCommand
   {
      public:
         omLogoutCommand( restAdaptor *pRestAdaptor, 
                          pmdRestSession *pRestSession ) ;
         ~omLogoutCommand() ;

      public:
         virtual INT32   doCommand() ;
   };

   class omChangePasswdCommand : public omAuthCommand
   {
      public:
         omChangePasswdCommand( restAdaptor *pRestAdaptor, 
                                pmdRestSession *pRestSession ) ;
         ~omChangePasswdCommand() ;

      public:
         virtual INT32   doCommand() ;

      private:
         INT32           _getRestDetail( string &user, string &oldPasswd, 
                                         string &newPasswd, string &time ) ;
   };

   class omCheckSessionCommand : public omAuthCommand
   {
      public:
         omCheckSessionCommand( restAdaptor *pRestAdaptor, 
                                pmdRestSession *pRestSession ) ;

         ~omCheckSessionCommand() ;

      public:
         virtual INT32   doCommand() ;

      protected:
   };

   class omCreateClusterCommand : public omCheckSessionCommand
   {
      public:
         omCreateClusterCommand( restAdaptor *pRestAdaptor, 
                                 pmdRestSession *pRestSession ) ;

         virtual ~omCreateClusterCommand() ;

      public:
         virtual INT32   doCommand() ;

      protected:

      private:
         INT32           _getParaOfCreateCluster( string &clusterName, 
                                                  string &desc,
                                                  string &sdbUsr, 
                                                  string &sdbPasswd,
                                                  string &sdbUsrGroup,
                                                  string &installPath ) ;
   };

   class omQueryClusterCommand : public omCreateClusterCommand 
   {
      public:
         omQueryClusterCommand( restAdaptor *pRestAdaptor, 
                                pmdRestSession *pRestSession ) ;

         ~omQueryClusterCommand() ;

      public:
         virtual INT32   doCommand() ;
   };

   struct omScanHostInfo
   {
      string hostName ;
      string ip ;
      string user ;
      string passwd ;
      string sshPort ;
      string agentPort ;
      bool isNeedUninstall ;

      omScanHostInfo()
      {
         hostName        = "" ;
         ip              = "" ;
         user            = "" ;
         passwd          = "" ;
         sshPort         = "" ;
         agentPort       = "" ;
         isNeedUninstall = false ;
      }

      omScanHostInfo( const omScanHostInfo &right )
      {
         hostName        = right.hostName ;
         ip              = right.ip ;
         user            = right.user ;
         passwd          = right.passwd ;
         sshPort         = right.sshPort ;
         agentPort       = right.agentPort ;
         isNeedUninstall = right.isNeedUninstall ;
      }
   } ;
   class omScanHostCommand : public omCreateClusterCommand
   {
      public:
         omScanHostCommand( restAdaptor *pRestAdaptor, 
                            pmdRestSession *pRestSession, 
                            const string &localAgentHost, 
                            const string &localAgentService ) ;

         ~omScanHostCommand() ;

      public:
         virtual INT32   doCommand() ;

      protected:
         bool            _isHostNameExist( const string &hostName ) ;
         bool            _isHostIPExist( const string &hostName ) ;
         bool            _isHostExist( const omScanHostInfo &host ) ;
         void            _filterExistHost( list<omScanHostInfo> &hostInfoList, 
                                           list<BSONObj> &hostResult ) ;
         void            _generateArray( list<BSONObj> &hostInfoList, 
                                         const string &arrayKeyName, 
                                         BSONObj &result ) ;
         void            _sendResult2Web( list<BSONObj> &hostResult ) ;
         INT32           _notifyAgentTask( INT64 taskID ) ;
         INT32           _sendMsgToLocalAgent( omManager *om,
                                               pmdRemoteSession *remoteSession, 
                                               MsgHeader *pMsg ) ;
         INT32           _getScanHostList( string &clusterName, 
                                           list<omScanHostInfo> &hostInfo ) ;
         void            _clearSession( omManager *om, 
                                        pmdRemoteSession *remoteSession) ;
         void            _generateHostList( list<omScanHostInfo> &hostInfoList, 
                                            BSONObj &bsonRequest ) ;

      private:
         INT32           _parseResonpse( VEC_SUB_SESSIONPTR &subSessionVec, 
                                         BSONObj &response, 
                                         list<BSONObj> &bsonResult ) ;
         INT32           _checkRestHostInfo( BSONObj &hostInfo ) ;

      protected:
         string          _localAgentHost ;
         string          _localAgentService ;

      private:


   };

   class omCheckHostCommand : public omScanHostCommand
   {
      public:
         omCheckHostCommand( restAdaptor *pRestAdaptor, 
                             pmdRestSession *pRestSession,
                             const string &localAgentHost, 
                             const string &localAgentService ) ;

         ~omCheckHostCommand() ;

      public:
         virtual INT32   doCommand() ;

      private:
         INT32           _getCheckHostList( string &clusterName, 
                                          list<omScanHostInfo> &hostInfoList ) ;
         INT32           _doCheck( list<omScanHostInfo> &hostInfoList, 
                                        list<BSONObj> &hostResult ) ;

         void            _updateUninstallFlag( 
                                            list<omScanHostInfo> &hostInfoList, 
                                            const string &ip, 
                                            const string &agentPort,
                                            bool isNeedUninstall ) ;
         INT32           _installAgent( list<omScanHostInfo> &hostInfoList, 
                                        list<BSONObj> &hostResult ) ;
         INT32           _addCheckHostReq( omManager *om,
                                          pmdRemoteSession *remoteSession,
                                          list<omScanHostInfo> &hostInfoList ) ;
         void            _updateDiskInfo( BSONObj &onehost ) ;

         INT32           _checkResFormat( BSONObj &result ) ;
         void            _errorCheckHostEnv( list<omScanHostInfo> &hostInfoList,
                                             list<BSONObj> &hostResult, 
                                             MsgRouteID id, int flag,
                                             const string &error ) ;
         INT32           _checkHostEnv( list<omScanHostInfo> &hostInfoList, 
                                        list<BSONObj> &hostResult ) ;

         bool            _isNeedUnistall( list<omScanHostInfo> &hostInfoList ) ;
         void            _generateUninstallReq( 
                                             list<omScanHostInfo> &hostInfoList, 
                                             BSONObj &bsonRequest ) ;
         INT32           _uninstallAgent( list<omScanHostInfo> &hostInfoList ) ;

         void            _updateAgentService( 
                                            list<omScanHostInfo> &hostInfoList, 
                                            const string &ip, 
                                            const string &port ) ;

         void            _eraseFromList( list<omScanHostInfo> &hostInfoList, 
                                         BSONObj &oneHost ) ;
         void            _eraseFromListByIP( list<omScanHostInfo> &hostInfoList, 
                                             const string &ip ) ;
         void            _eraseFromListByHost( 
                                             list<omScanHostInfo> &hostInfoList, 
                                             const string &hostName ) ;

         INT32           _notifyAgentExit( 
                                          list<omScanHostInfo> &hostInfoList ) ;
         INT32           _addAgentExitReq( omManager *om,
                                          pmdRemoteSession *remoteSession,
                                          list<omScanHostInfo> &hostInfoList ) ;

      private:
         map<UINT64, omScanHostInfo> _id2Host ;
   };

   class omAddHostCommand : public omScanHostCommand
   {
      public:
         omAddHostCommand( restAdaptor *pRestAdaptor, 
                           pmdRestSession *pRestSession,
                           const string &localAgentHost, 
                           const string &localAgentService ) ;

         ~omAddHostCommand() ;

      public:
         virtual INT32   doCommand() ;

      protected:
         INT32           _getRestHostList( string &clusterName, 
                                           list<BSONObj> &hostInfo ) ;

      private:
         void            _generateTableField( BSONObjBuilder &builder, 
                                              const string &newFieldName,
                                              BSONObj &bsonOld,
                                              const string &oldFiledName ) ;
         INT32           _getClusterInstallPath( const string &clusterName, 
                                                 string &installPath ) ;
         INT32           _getPacketFullPath( char *path ) ;
         INT32           _checkHostExistence( list<BSONObj> &hostInfoList ) ;

         INT32           _generateTaskInfo( const string &clusterName, 
                                            list<BSONObj> &hostInfoList, 
                                            BSONObj &taskInfo,
                                            BSONArray &resultInfo ) ;
         INT64           _generateTaskID() ;

         INT32           _saveTask( INT64 taskID, const BSONObj &taskInfo, 
                                    const BSONArray &resultInfo ) ;

         INT32           _removeTask( INT64 taskID ) ;

         INT32           _checkTaskExistence( list<BSONObj> &hostInfoList ) ;

         BOOLEAN         _isHostExistInTask( const string &hostName ) ;
   };

   class omListHostCommand : public omCreateClusterCommand
   {
      public:
         omListHostCommand( restAdaptor *pRestAdaptor, 
                            pmdRestSession *pRestSession ) ;
         ~omListHostCommand() ;

      public:
         virtual INT32   doCommand() ;

      protected:
         void            _sendHostInfo2Web( list<BSONObj> &hosts ) ;

      private:
   } ;

   class omQueryHostCommand : public omListHostCommand
   {
      public:
         omQueryHostCommand( restAdaptor *pRestAdaptor, 
                             pmdRestSession *pRestSession ) ;

         ~omQueryHostCommand() ;

      public:
         virtual INT32   doCommand() ;

      private:
   } ;

   class omListBusinessTypeCommand : public omCreateClusterCommand
   {
      public:
         omListBusinessTypeCommand( restAdaptor *pRestAdaptor, 
                                    pmdRestSession *pRestSession, 
                                    const CHAR *pRootPath, 
                                    const CHAR *pSubPath ) ;
         virtual ~omListBusinessTypeCommand() ;

      public:
         virtual INT32  doCommand() ;

      protected:
         INT32          _readConfigFile( const string &file, BSONObj &obj ) ;
         void           _recurseParseObj( ptree &pt, BSONObj &out ) ;
         void           _parseArray( ptree &pt, 
                                     BSONArrayBuilder &arrayBuilder ) ;
         BOOLEAN        _isStringValue( ptree &pt ) ;
         BOOLEAN        _isArray( ptree &pt ) ;

         INT32          _getBusinessList( list<BSONObj> &businessList ) ;

      protected:
         string          _rootPath ;
         string          _subPath ;

   } ;

   class omGetBusinessTemplateCommand : public omListBusinessTypeCommand
   {
      public:
         omGetBusinessTemplateCommand( restAdaptor *pRestAdaptor, 
                                         pmdRestSession *pRestSession, 
                                         const CHAR *pRootPath, 
                                         const CHAR *pSubPath ) ;
         virtual ~omGetBusinessTemplateCommand() ;

      public:
         virtual INT32  doCommand() ;

      protected:
         INT32          _readConfTemplate( const string &businessType, 
                                           const string &file, 
                                           list<BSONObj> &clusterTypeList ) ;
         INT32          _readConfDetail( const string &file, 
                                         BSONObj &bsonConfDetail ) ;

      protected:

   } ;

   class omConfigBusinessCommand : public omGetBusinessTemplateCommand
   {
      public:
         omConfigBusinessCommand( restAdaptor *pRestAdaptor, 
                                  pmdRestSession *pRestSession, 
                                  const CHAR *pRootPath, 
                                  const CHAR *pSubPath ) ;
         virtual ~omConfigBusinessCommand() ;

      public:
         virtual INT32  doCommand() ;

      protected:
         INT32          _fillHostInfo( string clusterName, string businessName,
                                       BSONObj &bsonHostInfo ) ;

         INT32          _checkBusiness( string businessName, 
                                        const string &businessType,
                                        const string &deployMod,
                                        const string &clusterName ) ;

      private:
         INT32          _generateConfig( const BSONObj &bsonTemplate, 
                                         const BSONObj &bsonHostInfo, 
                                         const BSONObj &bsonConfigItem, 
                                         BSONObj &bsonConfig ) ;
         void           _addProperties( BSONObjBuilder &builder, 
                                        const BSONObj &bsonTemplate, 
                                        const BSONObj &bsonConfDetail ) ;
         INT32          _getConfigDetail( const BSONObj &bsonTemplate, 
                                        BSONObj &bsonConfDetail ) ;
         INT32          _getTemplateInfo( BSONObj &bsonTemplate, 
                                          BSONObj &bsonHostInfo ) ;
         INT32          _fillTemplateInfo( BSONObj &bsonTemplate ) ;
         INT32          _getPropertyNameValue( BSONObj &bsonTemplate, 
                                               string propertyName, 
                                               string &value ) ;
         INT32          _getHostConfig( string hostName, string businessName,
                                        BSONObj &config ) ;

         INT32          _getExistBusiness( const string &businessName, 
                                           string &businessType,
                                           string &deployMod,
                                           string &clusterName ) ;
      protected:
         string         _clusterName ;
         string         _deployMod ;
         string         _businessType ;
         string         _businessName ;

   } ;

   class omInstallBusinessReq : public omConfigBusinessCommand
   {
      public:
         omInstallBusinessReq( restAdaptor *pRestAdaptor, 
                               pmdRestSession *pRestSession, 
                               const CHAR *pRootPath, 
                               const CHAR *pSubPath,
                               string localAgentHost, 
                               string localAgentService ) ;
         virtual ~omInstallBusinessReq() ;

      public:
         virtual INT32  doCommand() ;

      private:
         INT32          _combineConfDetail( string businessType, 
                                            string clusterType, 
                                            BSONObj &bsonConfDetail ) ;
         INT32          _extractHostInfo( BSONObj &bsonConfValue, 
                                          BSONObj &bsonHostInfo ) ;

         INT32          _applyInstallRequest( const BSONObj &bsonConfValue, 
                                              UINT64 taskID ) ;

         INT32          _sendMsgToLocalAgent( omManager *om,
                                              pmdRemoteSession *remoteSession, 
                                              MsgHeader *pMsg ) ;
         void           _compeleteConfValue( const BSONObj &bsonHostInfo, 
                                             BSONObj &bsonConfValue ) ;
         void           _clearSession( omManager *om, 
                                       pmdRemoteSession *remoteSession) ;
         INT32          _getRestInfo( BSONObj &bsonConfValue ) ;

         INT32          _generateTaskInfo( const BSONObj &bsonConfValue, 
                                           BSONObj &taskInfo, 
                                           BSONArray &resultInfo ) ;

         INT32          _notifyAgentTask( INT64 taskID ) ;
      private:
         string         _localAgentHost ;
         string         _localAgentService ;
   } ;

   class omListTaskCommand : public omAuthCommand
   {
      public:
         omListTaskCommand( restAdaptor *pRestAdaptor, 
                            pmdRestSession *pRestSession ) ;
         virtual ~omListTaskCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         INT32          _getTaskList( list<BSONObj> &taskList ) ;
         void           _sendTaskList2Web( list<BSONObj> &taskList ) ;
   } ;

   class omQueryTaskCommand : public omAuthCommand
   {
      public:
         omQueryTaskCommand( restAdaptor *pRestAdaptor, 
                            pmdRestSession *pRestSession ) ;
         virtual ~omQueryTaskCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         void           _sendTaskInfo2Web( list<BSONObj> &tasks ) ;
   } ;

   class omListNodeCommand : public omAuthCommand
   {
      public:
         omListNodeCommand( restAdaptor *pRestAdaptor,
                            pmdRestSession *pRestSession ) ;
         virtual ~omListNodeCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         INT32          _getNodeList( string businessName,
                                      list<simpleNodeInfo> &nodeList ) ;
         void           _sendNodeList2Web( list<simpleNodeInfo> &nodeList ) ;
   } ;

   class omQueryNodeConfCommand : public omAuthCommand
   {
      public:
         omQueryNodeConfCommand( restAdaptor *pRestAdaptor, 
                                 pmdRestSession *pRestSession ) ;
         virtual ~omQueryNodeConfCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         INT32          _getNodeInfo( const string &hostName, 
                                      const string &svcName, 
                                      BSONObj &nodeinfo ) ;
         void           _expandNodeInfo( BSONObj &oneConfig, 
                                         const string &svcName,
                                         BSONObj &nodeinfo ) ;
         void           _sendNodeInfo2Web( BSONObj &nodeList ) ;
   } ;

   class omQueryBusinessCommand : public omAuthCommand
   {
      public:
         omQueryBusinessCommand( restAdaptor *pRestAdaptor, 
                                 pmdRestSession *pRestSession ) ;
         virtual ~omQueryBusinessCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         void           _sendBusinessInfo2Web( list<BSONObj> &businessInfo ) ;
   } ;

   class omListBusinessCommand : public omAuthCommand
   {
      public:
         omListBusinessCommand( restAdaptor *pRestAdaptor, 
                                pmdRestSession *pRestSession ) ;
         virtual ~omListBusinessCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         void           _sendBusinessList2Web( list<BSONObj> &businessList ) ;
   };

   class omStartBusinessCommand : public omScanHostCommand
   {
      public:
         omStartBusinessCommand( restAdaptor *pRestAdaptor, 
                                 pmdRestSession *pRestSession,
                                 string localAgentHost, 
                                 string localAgentService ) ;
         virtual ~omStartBusinessCommand() ;

      public:
         virtual INT32  doCommand() ;

      protected:
         INT32          _getNodeInfo( const string &businessName, 
                                      BSONObj &nodeInfos,
                                      BOOLEAN &isExistFlag ) ;
         INT32          _getHostInfo( const string &hostName,
                                      simpleHostInfo &hostInfo,
                                      BOOLEAN &isExistFlag ) ;

      private:
         INT32          _expandNodeInfoToBuilder( const BSONObj &record, 
                                             BSONArrayBuilder &arrayBuilder ) ;
   } ;

   class omStopBusinessCommand : public omScanHostCommand
   {
      public:
         omStopBusinessCommand( restAdaptor *pRestAdaptor, 
                                pmdRestSession *pRestSession,
                                string localAgentHost, 
                                string localAgentService ) ;
         virtual ~omStopBusinessCommand() ;

      public:
         virtual INT32  doCommand() ;
   } ;

   class omRemoveClusterCommand : public omAuthCommand
   {
      public:
         omRemoveClusterCommand( restAdaptor *pRestAdaptor, 
                                pmdRestSession *pRestSession ) ;
         virtual ~omRemoveClusterCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         INT32          _getClusterExistHostFlag( const string &clusterName, 
                                                  BOOLEAN &flag ) ;
         INT32          _getClusterExistFlag( const string &clusterName, 
                                              BOOLEAN &flag ) ;
         INT32          _removeCluster( const string &clusterName ) ;
   } ;

   class omRemoveHostCommand : public omStartBusinessCommand
   {
      public:
         omRemoveHostCommand( restAdaptor *pRestAdaptor, 
                              pmdRestSession *pRestSession,
                              string localAgentHost, 
                              string localAgentService ) ;
         virtual ~omRemoveHostCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         INT32          _getHostExistBusinessFlag( const string &hostName, 
                                                   BOOLEAN &flag ) ;
         INT32          _removeHost( const simpleHostInfo &hostInfo, 
                                     BOOLEAN isForced ) ;
         INT32          _removeHostByAgent( const simpleHostInfo &hostInfo ) ;
         INT32          _getHostName( string &hostName, BOOLEAN &isForced ) ;
   } ;

   class omRemoveBusinessCommand : public omStartBusinessCommand
   {
      public:
         omRemoveBusinessCommand( restAdaptor *pRestAdaptor, 
                                  pmdRestSession *pRestSession,
                                  string localAgentHost, 
                                  string localAgentService ) ;
         virtual ~omRemoveBusinessCommand() ;

      public:
         virtual INT32  doCommand() ;

      private:
         INT32          _getBusinessExistFlag( const string &businessName, 
                                               BOOLEAN &flag ) ;

         INT32          _getHostNameInfo( const string &businessName,
                                       map<string, simpleHostInfo> &mapHosts) ;
         INT32          _generateRequest( string businessName,
                                          BSONObj &nodeInfos, 
                                          BSONObj &request ) ;

         INT32          _generateTaskInfo( string businessName,
                                           BSONObj &nodeInfos, 
                                           BSONObj &taskInfo,
                                           BSONArray &resultInfo ) ;
   } ;

   class omQueryHostStatusCommand : public omStartBusinessCommand
   {
      public:
         omQueryHostStatusCommand( restAdaptor *pRestAdaptor, 
                                   pmdRestSession *pRestSession,
                                   string localAgentHost, 
                                   string localAgentService ) ;

         ~omQueryHostStatusCommand() ;

      public:
         virtual INT32   doCommand() ;

      private:
         INT32           _getRestHostList( list<string> &hostNameList ) ;
         INT32           _verifyHostInfo( list<string> &hostNameList, 
                                          list<fullHostInfo> &hostInfoList ) ;
         INT32           _addQueryHostStatusReq( omManager *om,
                                          pmdRemoteSession *remoteSession,
                                          list<fullHostInfo> &hostInfoList ) ;
         INT32           _getHostStatus( list<fullHostInfo> &hostInfoList, 
                                         BSONObj &bsonStatus ) ;
         void            _appendErrorResult( BSONArrayBuilder &arrayBuilder, 
                                             const string &host, INT32 err,
                                             const string &detail ) ;
         void            _formatHostStatusOneNet( BSONObj &oneNet ) ;
         void            _formatHostStatusNet( BSONObj &net ) ;
         void            _formatHostStatusCPU( BSONObj &cpu ) ;
         void            _formatHostStatus( BSONObj &status ) ;

         void            _seperateMegaBitValue( BSONObj &obj, long value ) ;
   } ;

   class omPredictCapacity : public omAuthCommand
   {
      public:
         omPredictCapacity( restAdaptor *pRestAdaptor, 
                            pmdRestSession *pRestSession ) ;

         ~omPredictCapacity() ;

      public:
         virtual INT32   doCommand() ;

      private:
         INT32           _getHostList( BSONObj &hostInfos, 
                                       list<string> &hostNameList ) ;

         INT32           _getTemplateValue( BSONObj &properties,  
                                            INT32 &replicaNum, 
                                            INT32 &groupNum ) ;

         INT32           _getRestInfo( list<string> &hostNameList, 
                                       string &clusterName, 
                                       INT32 &replicaNum, INT32 &groupNum ) ;

         INT32           _predictCapacity( list<simpleHostDisk> &hostInfoList, 
                                           INT32 replicaNum, INT32 groupNum, 
                                           UINT64 &totalSize, UINT64 &validSize, 
                                           UINT32 &redundancyRate ) ;
   } ;

   class omGetFileCommand : public omRestCommandBase
   {
      public:
         omGetFileCommand( restAdaptor *pRestAdaptor, 
                           pmdRestSession *pRestSession, 
                           const CHAR *pRootPath, const CHAR *pSubPath ) ;
         virtual ~omGetFileCommand() ;

      public:
         virtual INT32   doCommand() ;
         virtual INT32   undoCommand() ;

      private:
         INT32           _getFileContent( string filePath, CHAR **pFileContent, 
                                          INT32 &fileContentLen ) ;

      private:
         restAdaptor*    _restAdaptor ;
         pmdRestSession* _restSession ;
         string          _rootPath ;
         string          _subPath ;
   };

   class restFileController : public SDBObject
   {
      public:
         static restFileController* getTransferInstance() ;

         INT32 getTransferedPath( const char *src_file, string &transfered ) ;

         bool isFileAuthorPublic( const char *file ) ;

      private:
         restFileController() ;
         restFileController(const restFileController &) ;
         restFileController& operator = ( const restFileController & ) ;

      private:
         typedef map < string, string >::iterator mapIteratorType ; 
         typedef map < string, string >::value_type mapValueType ;
         map < string, string > _transfer ;

         map < string, string > _publicAccessFiles ;
   };
}

#endif /* OM_GETFILECOMMAND_HPP__ */

