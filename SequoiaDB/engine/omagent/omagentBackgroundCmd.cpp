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

   Source File Name = omagentBackgroundCmd.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omagentUtil.hpp"
#include "omagentBackgroundCmd.hpp"
#include "utilStr.hpp"
#include "omagentMgr.hpp"

using namespace bson ;

namespace engine
{

   /*
      _omaAddHost
   */
   _omaAddHost::_omaAddHost ( AddHostInfo &info )
   {
      _addHostInfo = info ;
   }

   _omaAddHost::~_omaAddHost ()
   {
   }

   INT32 _omaAddHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus ;
         BSONObj sys ;
         stringstream ss ;
         rc = _getAddHostInfo( bus, sys ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get add host info for js file, "
                     "rc = %d", rc ) ;
            goto error ;
         }

         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Add host passes argument: %s", _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_ADD_HOST, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_ADD_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   INT32 _omaAddHost::_getAddHostInfo( BSONObj &retObj1, BSONObj &retObj2 )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      BSONObjBuilder bob ;
      BSONObj subObj ;


      try
      {
         bob.append( OMA_FIELD_IP, _addHostInfo._item._ip.c_str() ) ;
         bob.append( OMA_FIELD_HOSTNAME, _addHostInfo._item._hostName.c_str() ) ;
         bob.append( OMA_FIELD_USER, _addHostInfo._item._user.c_str() ) ;
         bob.append( OMA_FIELD_PASSWD, _addHostInfo._item._passwd.c_str() ) ;
         bob.append( OMA_FIELD_SSHPORT, _addHostInfo._item._sshPort.c_str() ) ;
         bob.append( OMA_FIELD_AGENTSERVICE, _addHostInfo._item._agentService.c_str() ) ;
         bob.append( OMA_FIELD_INSTALLPATH, _addHostInfo._item._installPath.c_str() ) ;
         subObj = bob.obj() ;

         builder.append( OMA_FIELD_SDBUSER,
                         _addHostInfo._common._sdbUser.c_str() ) ;
         builder.append( OMA_FIELD_SDBPASSWD,
                         _addHostInfo._common._sdbPasswd.c_str() ) ;
         builder.append( OMA_FIELD_SDBUSERGROUP,
                         _addHostInfo._common._userGroup.c_str() ) ;
         builder.append( OMA_FIELD_INSTALLPACKET,
                         _addHostInfo._common._installPacket.c_str() ) ;
         builder.append( OMA_FIELD_HOSTINFO, subObj ) ;
         retObj1 = builder.obj() ;
         retObj2 = BSON( OMA_FIELD_TASKID << _addHostInfo._taskID ) ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson for add host, "
                      "exception is: %s", e.what() ) ;
         goto error ;
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _omaCheckAddHostInfo
   */
   _omaCheckAddHostInfo::_omaCheckAddHostInfo()
   {
   }

   _omaCheckAddHostInfo::~_omaCheckAddHostInfo()
   {
   }

   INT32 _omaCheckAddHostInfo::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;
         stringstream ss ;

         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Check add host information passes argument: %s",
                  _jsFileArgs.c_str() ) ;
         rc = addJsFile( FIEL_ADD_HOST_CHECK_INFO, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FIEL_ADD_HOST_CHECK_INFO, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      _omaRemoveHost
   */
   _omaRemoveHost::_omaRemoveHost ( RemoveHostInfo &info )
   {
      _removeHostInfo = info ;
   }

   _omaRemoveHost::~_omaRemoveHost ()
   {
   }

   INT32 _omaRemoveHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus ;
         BSONObj sys ;
         stringstream ss ;
         rc = _getRemoveHostInfo( bus, sys ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get remove host info for js file, "
                     "rc = %d", rc ) ;
            goto error ;
         }

         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Remove host passes argument: %s",
                  _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_REMOVE_HOST, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_REMOVE_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   INT32 _omaRemoveHost::_getRemoveHostInfo( BSONObj &retObj1,
                                             BSONObj &retObj2 )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder bob ;

      try
      {
         bob.append( OMA_FIELD_IP, _removeHostInfo._item._ip.c_str() ) ;
         bob.append( OMA_FIELD_HOSTNAME, _removeHostInfo._item._hostName.c_str() ) ;
         bob.append( OMA_FIELD_USER, _removeHostInfo._item._user.c_str() ) ;
         bob.append( OMA_FIELD_PASSWD, _removeHostInfo._item._passwd.c_str() ) ;
         bob.append( OMA_FIELD_SSHPORT, _removeHostInfo._item._sshPort.c_str() ) ;
         bob.append( OMA_FIELD_CLUSTERNAME, _removeHostInfo._item._clusterName.c_str() ) ;
         bob.append( OMA_FIELD_INSTALLPATH, _removeHostInfo._item._installPath.c_str() ) ;
         retObj1 = bob.obj() ;
         retObj2 = BSON( OMA_FIELD_TASKID << _removeHostInfo._taskID ) ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson for add host, "
                      "exception is: %s", e.what() ) ;
         goto error ;
      }
      
   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _omaCreateTmpCoord
   */
   _omaCreateTmpCoord::_omaCreateTmpCoord( INT64 taskID )
   {
      _taskID = taskID ;
   }

   _omaCreateTmpCoord::~_omaCreateTmpCoord()
   {
   }

   INT32 _omaCreateTmpCoord::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj bus = BSONObj(pInstallInfo).copy() ;
         BSONObj sys = BSON( OMA_FIELD_TASKID << _taskID ) ;
         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Install temporary coord passes argument: %s",
                  _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_INSTALL_TMP_COORD, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_INSTALL_TMP_COORD, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaCreateTmpCoord::createTmpCoord( BSONObj &cfgObj, BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      rc = init( cfgObj.objdata() ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to create "
                  "temporary coord, rc = %d", rc ) ;
         goto error ;
      }
      rc = doit( retObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to create temporary coord, rc = %d", rc ) ;
         goto error ;
      }     
   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _omaRemoveTmpCoord
   */
   _omaRemoveTmpCoord::_omaRemoveTmpCoord( INT64 taskID,
                                           string &tmpCoordSvcName )
   {
      _taskID          = taskID ;
      _tmpCoordSvcName = tmpCoordSvcName ;
   }

   _omaRemoveTmpCoord::~_omaRemoveTmpCoord ()
   {
   }

   INT32 _omaRemoveTmpCoord::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj bus = BSON( OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName ) ;
         BSONObj sys = BSON( OMA_FIELD_TASKID << _taskID ) ;

         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Remove temporary coord passes argument: %s",
                  _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_REMOVE_TMP_COORD, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_REMOVE_TMP_COORD, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaRemoveTmpCoord::removeTmpCoord( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      rc = init( NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to remove temporary coord, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = doit( retObj ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to remove temporary coord, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _omaInstallStandalone
   */
   _omaInstallStandalone::_omaInstallStandalone( INT64 taskID,
                                                 InstDBInfo &info )
   {
      _taskID              = taskID ;
      _info._hostName      = info._hostName;
      _info._svcName       = info._svcName ;
      _info._dbPath        = info._dbPath ;
      _info._confPath      = info._confPath ;
      _info._dataGroupName = info._dataGroupName ;
      _info._sdbUser       = info._sdbUser ;
      _info._sdbPasswd     = info._sdbPasswd ;
      _info._sdbUserGroup  = info._sdbUserGroup ;
      _info._user          = info._user ;
      _info._passwd        = info._passwd ;
      _info._sshPort       = info._sshPort ;
      _info._conf          = info._conf.copy() ;
   }

   _omaInstallStandalone::~_omaInstallStandalone()
   {
   }

   INT32 _omaInstallStandalone::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj bus = BSON (
                 OMA_FIELD_SDBUSER         << _info._sdbUser.c_str() <<
                 OMA_FIELD_SDBPASSWD       << _info._sdbPasswd.c_str() <<
                 OMA_FIELD_SDBUSERGROUP    << _info._sdbUserGroup.c_str() <<
                 OMA_FIELD_USER            << _info._user.c_str() <<
                 OMA_FIELD_PASSWD          << _info._passwd.c_str() <<
                 OMA_FIELD_SSHPORT         << _info._sshPort.c_str() <<
                 OMA_FIELD_INSTALLHOSTNAME << _info._hostName.c_str() <<
                 OMA_FIELD_INSTALLSVCNAME  << _info._svcName.c_str() <<
                 OMA_FIELD_INSTALLPATH2    << _info._dbPath.c_str() <<
                 OMA_FIELD_INSTALLCONFIG   << _info._conf ) ;
         BSONObj sys = BSON ( OMA_FIELD_TASKID << _taskID ) ;
         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Install standalone passes argument: %s",
                  _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_INSTALL_STANDALONE, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_INSTALL_STANDALONE, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      _omaInstallCatalog
   */
   _omaInstallCatalog::_omaInstallCatalog( INT64 taskID,
                                           string &tmpCoordSvcName,
                                           InstDBInfo &info )
   {
      _taskID              = taskID ;
      _tmpCoordSvcName     = tmpCoordSvcName ;
      _info._hostName      = info._hostName;
      _info._svcName       = info._svcName ;
      _info._dbPath        = info._dbPath ;
      _info._confPath      = info._confPath ;
      _info._dataGroupName = info._dataGroupName ;
      _info._sdbUser       = info._sdbUser ;
      _info._sdbPasswd     = info._sdbPasswd ;
      _info._sdbUserGroup  = info._sdbUserGroup ;
      _info._user          = info._user ;
      _info._passwd        = info._passwd ;
      _info._sshPort       = info._sshPort ;
      _info._conf          = info._conf.copy() ;
   }

   _omaInstallCatalog::~_omaInstallCatalog()
   {
   }

   INT32 _omaInstallCatalog::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj bus = BSON (
                 OMA_FIELD_SDBUSER         << _info._sdbUser.c_str() <<
                 OMA_FIELD_SDBPASSWD       << _info._sdbPasswd.c_str() <<
                 OMA_FIELD_SDBUSERGROUP    << _info._sdbUserGroup.c_str() <<
                 OMA_FIELD_USER            << _info._user.c_str() <<
                 OMA_FIELD_PASSWD          << _info._passwd.c_str() <<
                 OMA_FIELD_SSHPORT         << _info._sshPort.c_str() <<
                 OMA_FIELD_INSTALLHOSTNAME << _info._hostName.c_str() <<
                 OMA_FIELD_INSTALLSVCNAME  << _info._svcName.c_str() <<
                 OMA_FIELD_INSTALLPATH2    << _info._dbPath.c_str() <<
                 OMA_FIELD_INSTALLCONFIG   << _info._conf ) ;
         BSONObj sys = BSON (
                 OMA_FIELD_TASKID << _taskID <<
                 OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;
         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Install catalog passes argument: %s",
                  _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_INSTALL_CATALOG, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_INSTALL_CATALOG, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      _omaInstallCoord
   */
   _omaInstallCoord::_omaInstallCoord( INT64 taskID,
                                       string &tmpCoordSvcName,
                                       InstDBInfo &info )
   {
      _taskID              = taskID ;
      _tmpCoordSvcName     = tmpCoordSvcName ;
      _info._hostName      = info._hostName;
      _info._svcName       = info._svcName ;
      _info._dbPath        = info._dbPath ;
      _info._confPath      = info._confPath ;
      _info._dataGroupName = info._dataGroupName ;
      _info._sdbUser       = info._sdbUser ;
      _info._sdbPasswd     = info._sdbPasswd ;
      _info._sdbUserGroup  = info._sdbUserGroup ;
      _info._user          = info._user ;
      _info._passwd        = info._passwd ;
      _info._sshPort       = info._sshPort ;
      _info._conf          = info._conf.copy() ;
   }

   _omaInstallCoord::~_omaInstallCoord()
   {
   }

   INT32 _omaInstallCoord::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj bus = BSON (
                 OMA_FIELD_SDBUSER         << _info._sdbUser.c_str() <<
                 OMA_FIELD_SDBPASSWD       << _info._sdbPasswd.c_str() <<
                 OMA_FIELD_SDBUSERGROUP    << _info._sdbUserGroup.c_str() <<
                 OMA_FIELD_USER            << _info._user.c_str() <<
                 OMA_FIELD_PASSWD          << _info._passwd.c_str() <<
                 OMA_FIELD_SSHPORT         << _info._sshPort.c_str() <<
                 OMA_FIELD_INSTALLHOSTNAME << _info._hostName.c_str() <<
                 OMA_FIELD_INSTALLSVCNAME  << _info._svcName.c_str() <<
                 OMA_FIELD_INSTALLPATH2    << _info._dbPath.c_str() <<
                 OMA_FIELD_INSTALLCONFIG   << _info._conf ) ;
         BSONObj sys = BSON (
                 OMA_FIELD_TASKID << _taskID <<
                 OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;
         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Install coord passes argument: %s",
                  _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_INSTALL_COORD, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_INSTALL_COORD, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      _omaInstallDataNode
   */
   _omaInstallDataNode::_omaInstallDataNode( INT64 taskID,
                                             string tmpCoordSvcName,
                                             InstDBInfo &info )
   {
      _taskID              = taskID ;
      _tmpCoordSvcName     = tmpCoordSvcName ;
      _info._hostName      = info._hostName;
      _info._svcName       = info._svcName ;
      _info._dbPath        = info._dbPath ;
      _info._confPath      = info._confPath ;
      _info._dataGroupName = info._dataGroupName ;
      _info._sdbUser       = info._sdbUser ;
      _info._sdbPasswd     = info._sdbPasswd ;
      _info._sdbUserGroup  = info._sdbUserGroup ;
      _info._user          = info._user ;
      _info._passwd        = info._passwd ;
      _info._sshPort       = info._sshPort ;
      _info._conf          = info._conf.copy() ;
   }

   _omaInstallDataNode::~_omaInstallDataNode()
   {
   }

   INT32 _omaInstallDataNode::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj bus = BSON (
                 OMA_FIELD_SDBUSER          << _info._sdbUser.c_str() <<
                 OMA_FIELD_SDBPASSWD        << _info._sdbPasswd.c_str() << 
                 OMA_FIELD_SDBUSERGROUP     << _info._sdbUserGroup.c_str() <<
                 OMA_FIELD_USER             << _info._user.c_str() <<
                 OMA_FIELD_PASSWD           << _info._passwd.c_str() <<
                 OMA_FIELD_SSHPORT          << _info._sshPort.c_str() <<
                 OMA_FIELD_INSTALLGROUPNAME << _info._dataGroupName.c_str() <<
                 OMA_FIELD_INSTALLHOSTNAME  << _info._hostName.c_str() <<
                 OMA_FIELD_INSTALLSVCNAME   << _info._svcName.c_str() <<
                 OMA_FIELD_INSTALLPATH2     << _info._dbPath.c_str() <<
                 OMA_FIELD_INSTALLCONFIG    << _info._conf ) ;
         BSONObj sys = BSON (
                 OMA_FIELD_TASKID << _taskID <<
                 OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;
         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Install data node passes "
                  "argument: %s", _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_INSTALL_DATANODE, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_INSTALL_DATANODE, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      rollback standalone
   */
   _omaRollbackStandalone::_omaRollbackStandalone ( BSONObj &bus, BSONObj &sys )
   {
      _bus    = bus.copy() ;
      _sys    = sys.copy() ;
   }

   _omaRollbackStandalone::~_omaRollbackStandalone ()
   {
   }
   
   INT32 _omaRollbackStandalone::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      stringstream ss ;
      
      ss << "var " << JS_ARG_BUS << " = " 
         << _bus.toString(FALSE, TRUE).c_str() << " ; "
         << "var " << JS_ARG_SYS << " = "
         << _sys.toString(FALSE, TRUE).c_str() << " ; " ;
      _jsFileArgs = ss.str() ;
      PD_LOG ( PDDEBUG, "Rollback standalone passes "
               "argument: %s", _jsFileArgs.c_str() ) ;
      rc = addJsFile( FILE_ROLLBACK_STANDALONE, _jsFileArgs.c_str() ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                  FILE_ROLLBACK_STANDALONE, rc ) ;
         goto error ;
      }
      
   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      rollback catalog
   */
   _omaRollbackCatalog::_omaRollbackCatalog (
                                   INT64 taskID,
                                   string &tmpCoordSvcName )
   {
      _taskID          = taskID ;
      _tmpCoordSvcName = tmpCoordSvcName ;
   }

   _omaRollbackCatalog::~_omaRollbackCatalog ()
   {
   }
   
   INT32 _omaRollbackCatalog::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj sys = BSON (
                 OMA_FIELD_TASKID << _taskID <<
                 OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;

         ss << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Rollback catalog passes "
                  "argument: %s", _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_ROLLBACK_CATALOG, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_ROLLBACK_CATALOG, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }
   
   /*
      rollback coord
   */

   _omaRollbackCoord::_omaRollbackCoord ( INT64 taskID,
                                          string &tmpCoordSvcName )
   {
      _taskID          = taskID ;
      _tmpCoordSvcName = tmpCoordSvcName ;
   }

   _omaRollbackCoord::~_omaRollbackCoord ()
   {
   }
   
   INT32 _omaRollbackCoord::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj sys = BSON (
                 OMA_FIELD_TASKID << _taskID <<
                 OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;
         ss << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Rollback coord passes "
                  "argument: %s", _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_ROLLBACK_COORD, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_ROLLBACK_COORD, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }


   /*
      rollback data groups
   */

   _omaRollbackDataRG::_omaRollbackDataRG ( INT64 taskID,
                                            string &tmpCoordSvcName,
                                            set<string> &info )
   : _info( info )
   {
      _taskID          = taskID ;
      _tmpCoordSvcName = tmpCoordSvcName ;
   }

   _omaRollbackDataRG::~_omaRollbackDataRG ()
   {
   }
   
   INT32 _omaRollbackDataRG::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         stringstream ss ;
         BSONObj bus ;
         BSONObj sys ;
         _getInstalledDataGroupInfo( bus ) ;
         sys = BSON( OMA_FIELD_TASKID << _taskID <<
                     OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;
         ss << "var " << JS_ARG_BUS << " = " 
            << bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
         PD_LOG ( PDDEBUG, "Rollback data groups passes "
                  "argument: %s", _jsFileArgs.c_str() ) ;
         rc = addJsFile( FILE_ROLLBACK_DATA_RG, _jsFileArgs.c_str() ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_ROLLBACK_DATA_RG, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   void _omaRollbackDataRG::_getInstalledDataGroupInfo( BSONObj &obj )
   {
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      set<string>::iterator it = _info.begin() ;

      for( ; it != _info.end(); it++ )
      {
         string groupname = *it ;
         bab.append( groupname.c_str() ) ;
      }
      bob.appendArray( OMA_FIELD_UNINSTALLGROUPNAMES, bab.arr() ) ;
      obj = bob.obj() ;
   }

   /*
      remove standalone
   */
   _omaRmStandalone::_omaRmStandalone( BSONObj &bus, BSONObj &sys )
   {
      _bus = bus.copy() ;
      _sys = sys.copy() ;
   }

   _omaRmStandalone::~_omaRmStandalone()
   {
   }

   INT32 _omaRmStandalone::init ( const CHAR *pInfo )
   {
      INT32 rc = SDB_OK ;
      stringstream ss ;
      
         ss << "var " << JS_ARG_BUS << " = " 
            << _bus.toString(FALSE, TRUE).c_str() << " ; "
            << "var " << JS_ARG_SYS << " = "
            << _sys.toString(FALSE, TRUE).c_str() << " ; " ;
         _jsFileArgs = ss.str() ;
      PD_LOG ( PDDEBUG, "Remove standalone passes argument: %s",
               _jsFileArgs.c_str() ) ;
      rc = addJsFile( FILE_REMOVE_STANDALONE, _jsFileArgs.c_str() ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                      FILE_REMOVE_STANDALONE, rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      remove catalog group
   */
   _omaRmCataRG::_omaRmCataRG ( INT64 taskID, string &tmpCoordSvcName,
                                BSONObj &info )
   {
      _taskID = taskID ;
      _tmpCoordSvcName = tmpCoordSvcName ;
      _info = info.copy() ;
   }

   _omaRmCataRG::~_omaRmCataRG ()
   {
   }
   
   INT32 _omaRmCataRG::init ( const CHAR *pInfo )
   {
      INT32 rc = SDB_OK ;
      stringstream ss ;

      BSONObj bus = _info.copy() ;
      BSONObj sys = BSON( OMA_FIELD_TASKID << _taskID <<
                          OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;
      ss << "var " << JS_ARG_BUS << " = " 
         << bus.toString(FALSE, TRUE).c_str() << " ; "
         << "var " << JS_ARG_SYS << " = "
         << sys.toString(FALSE, TRUE).c_str() << " ; " ;
      _jsFileArgs = ss.str() ;
      PD_LOG ( PDDEBUG, "Remove catalog group passes "
               "argument: %s", _jsFileArgs.c_str() ) ;
      rc = addJsFile( FILE_REMOVE_CATALOG_RG, _jsFileArgs.c_str() ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                  FILE_REMOVE_CATALOG_RG, rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      remove coord group
   */

   _omaRmCoordRG::_omaRmCoordRG ( INT64 taskID, string &tmpCoordSvcName,
                                  BSONObj &info )
   {
      _taskID          = taskID ;
      _tmpCoordSvcName = tmpCoordSvcName ;
      _info            = info.copy() ;
   }

   _omaRmCoordRG::~_omaRmCoordRG ()
   {
   }
   
   INT32 _omaRmCoordRG::init ( const CHAR *pInfo )
   {
      INT32 rc = SDB_OK ;
      stringstream ss ;
      BSONObj bus = _info.copy() ;
      BSONObj sys = BSON( OMA_FIELD_TASKID << _taskID <<
                          OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;

      ss << "var " << JS_ARG_BUS << " = " 
         << bus.toString(FALSE, TRUE).c_str() << " ; "
         << "var " << JS_ARG_SYS << " = "
         << sys.toString(FALSE, TRUE).c_str() << " ; " ;
      _jsFileArgs = ss.str() ;
      PD_LOG ( PDDEBUG, "Remove coord group passes "
               "argument: %s", _jsFileArgs.c_str() ) ;
      rc = addJsFile( FILE_REMOVE_COORD_RG, _jsFileArgs.c_str() ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                      FILE_REMOVE_COORD_RG, rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      remove data rg
   */
   _omaRmDataRG::_omaRmDataRG ( INT64 taskID, string &tmpCoordSvcName,
                                BSONObj &info )
   {
      _taskID = taskID ;
      _tmpCoordSvcName = tmpCoordSvcName ;
      _info = info.copy() ;
   }

   _omaRmDataRG::~_omaRmDataRG ()
   {
   }
   
   INT32 _omaRmDataRG::init ( const CHAR *pInfo )
   {
      INT32 rc = SDB_OK ;
      stringstream ss ;
      BSONObj bus = _info.copy() ;
      BSONObj sys = BSON( OMA_FIELD_TASKID << _taskID <<
                          OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName.c_str() ) ;
      ss << "var " << JS_ARG_BUS << " = " 
         << bus.toString(FALSE, TRUE).c_str() << " ; "
         << "var " << JS_ARG_SYS << " = "
         << sys.toString(FALSE, TRUE).c_str() << " ; " ;
      _jsFileArgs = ss.str() ;
      PD_LOG ( PDDEBUG, "Remove data group passes "
               "argument: %s", _jsFileArgs.c_str() ) ;
      rc = addJsFile( FILE_REMOVE_DATA_RG, _jsFileArgs.c_str() ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                      FILE_REMOVE_DATA_RG, rc ) ;
         goto error ;
      }
         
   done:
      return rc ;
   error:
     goto done ;
   }

   /*
      init for executing js
   */
   _omaInitEnv::_omaInitEnv ( INT64 taskID, BSONObj &info )
   {
      _taskID = taskID ;
      _info = info.copy() ;
   }

   _omaInitEnv::~_omaInitEnv ()
   {
   }
   
   INT32 _omaInitEnv::init ( const CHAR *pInfo )
   {
      INT32 rc = SDB_OK ;
      stringstream ss ;
      BSONObj bus = _info.copy() ;
      BSONObj sys = BSON( OMA_FIELD_TASKID << _taskID ) ;
      ss << "var " << JS_ARG_BUS << " = " 
         << bus.toString(FALSE, TRUE).c_str() << " ; "
         << "var " << JS_ARG_SYS << " = "
         << sys.toString(FALSE, TRUE).c_str() << " ; " ;
      _jsFileArgs = ss.str() ;
      PD_LOG ( PDDEBUG, "Init for executing js passes "
               "argument: %s", _jsFileArgs.c_str() ) ;
      rc = addJsFile( FILE_INIT_ENV, _jsFileArgs.c_str() ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                      FILE_INIT_ENV, rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
     goto done ;
   }

}

