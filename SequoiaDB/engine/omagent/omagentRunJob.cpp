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

   Source File Name = omagentRunJob.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omagentUtil.hpp"
#include "omagentAsyncCmd.hpp"
#include "utilStr.hpp"
#include "omagentMgr.hpp"

using namespace bson ;

namespace engine
{

   /*
      _omaRunAddHost
   */
   _omaRunAddHost::_omaRunAddHost ( AddHostInfo &info )
   {
      _addHostInfo = info ;
   }

   _omaRunAddHost::~_omaRunAddHost ()
   {
   }

   INT32 _omaRunAddHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus ;
         BSONObj sys ;
         rc = _getAddHostInfo( bus, sys ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get add host info for js file, "
                     "rc = %d", rc ) ;
            goto error ;
         }

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; var %s = %s;",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Add hosts passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_ADD_HOST, _jsFileArgs ) ;
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

   INT32 _omaRunAddHost::_getAddHostInfo( BSONObj &retObj1, BSONObj &retObj2 )
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
       _omaRunRmHost
   */
   _omaRunRmHost::_omaRunRmHost( AddHostInfo &info )
   {
      _RmHostInfo = info ;
   }

   _omaRunRmHost::~_omaRunRmHost()
   {
   }

   INT32 _omaRunRmHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus ;
         rc = _getRmHostInfo( bus ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get remove host info for js file, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove host passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_ADDHOST_ROLLBACK2, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_ADDHOST_ROLLBACK2, rc ) ;
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

   INT32 _omaRunRmHost::_getRmHostInfo( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      BSONObjBuilder bob ;
      BSONObj subObj ;

      
      try
      {
         bob.append( OMA_FIELD_IP, _RmHostInfo._item._ip.c_str() ) ;
         bob.append( OMA_FIELD_HOSTNAME, _RmHostInfo._item._hostName.c_str() ) ;
         bob.append( OMA_FIELD_USER, _RmHostInfo._item._user.c_str() ) ;
         bob.append( OMA_FIELD_PASSWD, _RmHostInfo._item._passwd.c_str() ) ;
         bob.append( OMA_FIELD_SSHPORT, _RmHostInfo._item._sshPort.c_str() ) ;
         bob.append( OMA_FIELD_AGENTSERVICE, _RmHostInfo._item._agentService.c_str() ) ;
         bob.append( OMA_FIELD_INSTALLPATH, _RmHostInfo._item._installPath.c_str() ) ;
         subObj = bob.obj() ;

         builder.append( OMA_FIELD_HOSTINFO, subObj ) ;
         retObj = builder.obj() ;
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
      _omaRunCheckAddHostInfo
   */
   _omaRunCheckAddHostInfo::_omaRunCheckAddHostInfo()
   {
   }

   _omaRunCheckAddHostInfo::~_omaRunCheckAddHostInfo()
   {
   }

   INT32 _omaRunCheckAddHostInfo::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj obj( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, obj.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Check add host information passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FIEL_CHECK_ADD_HOST_INFO, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FIEL_CHECK_ADD_HOST_INFO, rc ) ;
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
      _omaRunCreateStandaloneJob
   */
   _omaRunCreateStandaloneJob::_omaRunCreateStandaloneJob( string &vCoordSvcName,
                                                           InstallInfo &info )
   {
      _info._hostName      = info._hostName ;
      _info._svcName       = info._svcName ;
      _info._dbPath        = info._dbPath ;
      _info._conf          = info._conf.getOwned() ;
      _info._sdbUser       = info._sdbUser ;
      _info._sdbPasswd     = info._sdbPasswd ;
      _info._sdbUserGroup  = info._sdbUserGroup ;
      _info._user          = info._user ;
      _info._passwd        = info._passwd ;
      _info._sshPort       = info._sshPort ;
      _vCoordSvcName       = vCoordSvcName ;
   }

   _omaRunCreateStandaloneJob::~_omaRunCreateStandaloneJob()
   {
   }

   INT32 _omaRunCreateStandaloneJob::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus = BSON (
                 OMA_FIELD_INSTALLHOSTNAME << _info._hostName.c_str() <<
                 OMA_FIELD_INSTALLSVCNAME  << _info._svcName.c_str() <<
                 OMA_FIELD_INSTALLPATH2    << _info._dbPath.c_str() <<
                 OMA_FIELD_INSTALLCONFIG   << _info._conf ) ;
         BSONObj sys = BSON (
                 OMA_FIELD_SDBUSER       << _info._sdbUser.c_str() <<
                 OMA_FIELD_SDBPASSWD     << _info._sdbPasswd.c_str() <<
                 OMA_FIELD_SDBUSERGROUP  << _info._sdbUserGroup.c_str() <<
                 OMA_FIELD_USER          << _info._user.c_str() <<
                 OMA_FIELD_PASSWD        << _info._passwd.c_str() <<
                 OMA_FIELD_SSHPORT       << _info._sshPort.c_str() ) ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Create standalone passes argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_CREATE_STANDALONE, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_CREATE_STANDALONE, rc ) ;
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
      _omaRunCreateCatalogJob
   */
   _omaRunCreateCatalogJob::_omaRunCreateCatalogJob( string &vCoordSvcName,
                                                     InstallInfo &info )
   {
      _info._hostName      = info._hostName ;
      _info._svcName       = info._svcName ;
      _info._dbPath        = info._dbPath ;
      _info._conf          = info._conf.getOwned() ;
      _info._sdbUser       = info._sdbUser ;
      _info._sdbPasswd     = info._sdbPasswd ;
      _info._sdbUserGroup  = info._sdbUserGroup ;
      _info._user          = info._user ;
      _info._passwd        = info._passwd ;
      _info._sshPort       = info._sshPort ;
      _vCoordSvcName       = vCoordSvcName ;
   }

   _omaRunCreateCatalogJob::~_omaRunCreateCatalogJob()
   {
   }

   INT32 _omaRunCreateCatalogJob::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus = BSON (
                 OMA_FIELD_INSTALLHOSTNAME << _info._hostName.c_str() <<
                 OMA_FIELD_INSTALLSVCNAME  << _info._svcName.c_str() <<
                 OMA_FIELD_INSTALLPATH2    << _info._dbPath.c_str() <<
                 OMA_FIELD_INSTALLCONFIG   << _info._conf ) ;
         BSONObj sys = BSON (
                 OMA_FIELD_VCOORDSVCNAME << _vCoordSvcName.c_str() <<
                 OMA_FIELD_SDBUSER       << _info._sdbUser.c_str() <<
                 OMA_FIELD_SDBPASSWD     << _info._sdbPasswd.c_str() <<
                 OMA_FIELD_SDBUSERGROUP  << _info._sdbUserGroup.c_str() <<
                 OMA_FIELD_USER          << _info._user.c_str() <<
                 OMA_FIELD_PASSWD        << _info._passwd.c_str() <<
                 OMA_FIELD_SSHPORT       << _info._sshPort.c_str() ) ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Create catalog passes argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_CREATE_CATALOG, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_CREATE_CATALOG, rc ) ;
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
      _omaRunCreateCoordJob
   */
   _omaRunCreateCoordJob::_omaRunCreateCoordJob( string &vCoordSvcName,
                                                 InstallInfo &info )
   {
      _info._hostName      = info._hostName ;
      _info._svcName       = info._svcName ;
      _info._dbPath        = info._dbPath ;
      _info._conf          = info._conf.getOwned() ;
      _info._sdbUser       = info._sdbUser ;
      _info._sdbPasswd     = info._sdbPasswd ;
      _info._sdbUserGroup  = info._sdbUserGroup ;
      _info._user          = info._user ;
      _info._passwd        = info._passwd ;
      _info._sshPort       = info._sshPort ;
      _vCoordSvcName       = vCoordSvcName ;
   }

   _omaRunCreateCoordJob::~_omaRunCreateCoordJob()
   {
   }

   INT32 _omaRunCreateCoordJob::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus = BSON (
                 OMA_FIELD_INSTALLHOSTNAME << _info._hostName.c_str() <<
                 OMA_FIELD_INSTALLSVCNAME  << _info._svcName.c_str() <<
                 OMA_FIELD_INSTALLPATH2    << _info._dbPath.c_str() <<
                 OMA_FIELD_INSTALLCONFIG   << _info._conf ) ;
         BSONObj sys = BSON (
                 OMA_FIELD_VCOORDSVCNAME << _vCoordSvcName.c_str() <<
                 OMA_FIELD_SDBUSER       << _info._sdbUser.c_str() <<
                 OMA_FIELD_SDBPASSWD     << _info._sdbPasswd.c_str() <<
                 OMA_FIELD_SDBUSERGROUP  << _info._sdbUserGroup.c_str() <<
                 OMA_FIELD_USER          << _info._user.c_str() <<
                 OMA_FIELD_PASSWD        << _info._passwd.c_str() <<
                 OMA_FIELD_SSHPORT       << _info._sshPort.c_str() ) ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Create coord passes argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_CREATE_COORD, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_CREATE_COORD, rc ) ;
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
      _omaRunCreateDataNodeJob
   */
   _omaRunCreateDataNodeJob::_omaRunCreateDataNodeJob( string &vCoordSvcName,
                                                       InstallInfo &info )
   {
      _info._hostName      = info._hostName ;
      _info._svcName       = info._svcName ;
      _info._dbPath        = info._dbPath ;
      _info._dataGroupName = info._dataGroupName ;
      _info._conf          = info._conf.getOwned() ;
      _info._sdbUser       = info._sdbUser ;
      _info._sdbPasswd     = info._sdbPasswd ;
      _info._sdbUserGroup  = info._sdbUserGroup ;
      _info._user          = info._user ;
      _info._passwd        = info._passwd ;
      _info._sshPort       = info._sshPort ;
      _vCoordSvcName       = vCoordSvcName ;
   }

   _omaRunCreateDataNodeJob::~_omaRunCreateDataNodeJob()
   {
   }

   INT32 _omaRunCreateDataNodeJob::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus = BSON (
                 OMA_FIELD_INSTALLGROUPNAME << _info._dataGroupName.c_str() <<
                 OMA_FIELD_INSTALLHOSTNAME  << _info._hostName.c_str() <<
                 OMA_FIELD_INSTALLSVCNAME   << _info._svcName.c_str() <<
                 OMA_FIELD_INSTALLPATH2     << _info._dbPath.c_str() <<
                 OMA_FIELD_INSTALLCONFIG    << _info._conf ) ;
         BSONObj sys = BSON (
                 OMA_FIELD_VCOORDSVCNAME << _vCoordSvcName.c_str() <<
                 OMA_FIELD_SDBUSER       << _info._sdbUser.c_str() <<
                 OMA_FIELD_SDBPASSWD     << _info._sdbPasswd.c_str() << 
                 OMA_FIELD_SDBUSERGROUP  << _info._sdbUserGroup.c_str() <<
                 OMA_FIELD_USER          << _info._user.c_str() <<
                 OMA_FIELD_PASSWD        << _info._passwd.c_str() <<
                 OMA_FIELD_SSHPORT       << _info._sshPort.c_str() ) ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Create data node passes "
                  "argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_CREATE_DATANODE, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_CREATE_DATANODE, rc ) ;
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
      install db business task run rollback standalone job
   */
   _omaRunRollbackStandaloneJob::_omaRunRollbackStandaloneJob (
                                   string &vCoordSvcName,
                                   map< string, vector<InstalledNode> > &info )
   :_info( info )
   {
      _vCoordSvcName = vCoordSvcName ;
   }

   _omaRunRollbackStandaloneJob::~_omaRunRollbackStandaloneJob ()
   {
   }
   
   INT32 _omaRunRollbackStandaloneJob::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObjBuilder bob ;
         BSONObj bus ;
         _getInstallStandaloneInfo( bus ) ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Rollback standalone passes "
                  "argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_STANDALONE, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_REMOVE_STANDALONE, rc ) ;
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

   void _omaRunRollbackStandaloneJob::_getInstallStandaloneInfo( BSONObj &obj )
   {
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      map< string, vector< InstalledNode > >::iterator it = _info.begin() ;
      for ( ; it != _info.end(); it++ )
      {
         vector< InstalledNode > &vec = it->second ;
         vector< InstalledNode >::iterator iter = vec.begin() ;
         for( ; iter != vec.end(); iter++ )
         {
            BSONObj temp = BSON ( OMA_FIELD_UNINSTALLHOSTNAME << iter->_hostName <<
                                  OMA_FIELD_UNINSTALLSVCNAME << iter->_svcName ) ;
            bab.append( temp ) ;
         }
         break ;
      }
      bob.append( OMA_FIELD_HOSTINFO, bab.arr() ) ;
      obj = bob.obj() ;
   }
   
   /*
      install db business task run rollback coord job
   */

   _omaRunRollbackCoordJob::_omaRunRollbackCoordJob (
                                   string &vCoordSvcName,
                                   map< string, vector<InstalledNode> > &info )
   :_info( info )
   {
      _vCoordSvcName = vCoordSvcName ;
   }

   _omaRunRollbackCoordJob::~_omaRunRollbackCoordJob ()
   {
   }
   
   INT32 _omaRunRollbackCoordJob::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj sys = BSON (
                 OMA_FIELD_VCOORDSVCNAME << _vCoordSvcName.c_str() ) ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Rollback coord passes "
                  "argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_ROLLBACK_COORD, _jsFileArgs ) ;
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
      install db business task run rollback catalog job
   */
   _omaRunRollbackCatalogJob::_omaRunRollbackCatalogJob (
                                   string &vCoordSvcName,
                                   map< string, vector<InstalledNode> > &info )
   : _info( info )
   {
      _vCoordSvcName = vCoordSvcName ;
   }

   _omaRunRollbackCatalogJob::~_omaRunRollbackCatalogJob ()
   {
   }
   
   INT32 _omaRunRollbackCatalogJob::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj sys = BSON (
                 OMA_FIELD_VCOORDSVCNAME << _vCoordSvcName.c_str() ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Rollback catalog passes "
                  "argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_ROLLBACK_CATALOG, _jsFileArgs ) ;
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
      install db business task run rollback data node job
   */

   _omaRunRollbackDataNodeJob::_omaRunRollbackDataNodeJob (
                                   string &vCoordSvcName,
                                   map< string, vector<InstalledNode> > &info )
   : _info( info )
   {
      _vCoordSvcName = vCoordSvcName ;
   }

   _omaRunRollbackDataNodeJob::~_omaRunRollbackDataNodeJob ()
   {
   }
   
   INT32 _omaRunRollbackDataNodeJob::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObjBuilder builder ;
         BSONObj dataGroupInfo ;
         BSONObj sys ;
         _getInstalledDataGroupInfo( dataGroupInfo ) ;
         builder.append( OMA_FIELD_VCOORDSVCNAME, _vCoordSvcName.c_str() ) ;
         builder.appendElements( dataGroupInfo ) ;
         sys = builder.obj() ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Rollback data group passes "
                  "argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_ROLLBACK_DATANODE, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_ROLLBACK_DATANODE, rc ) ;
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

   void _omaRunRollbackDataNodeJob::_getInstalledDataGroupInfo( BSONObj &obj )
   {
      BSONObjBuilder bob ;
      BSONArrayBuilder bab ;
      map< string, vector< InstalledNode > >::iterator it = _info.begin() ;

      for( ; it != _info.end(); it++ )
      {
         string groupname = it->first ;
         bab.append( groupname.c_str() ) ;
      }
      bob.appendArray( OMA_FIELD_UNINSTALLGROUPNAMES, bab.arr() ) ;
      obj = bob.obj() ;
   }

   /*
      remove standalone
   */
   _omaRmStandalone::_omaRmStandalone()
   {
   }

   _omaRmStandalone::~_omaRmStandalone()
   {
   }

   INT32 _omaRmStandalone::init ( const CHAR *pUninstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONArrayBuilder bab ;
         BSONObjBuilder builder ;
         BSONObjBuilder bob ;
         BSONObj bus ;
         BSONObj info = BSONObj( pUninstallInfo ).getOwned() ;
         const CHAR *pStr = NULL ;
         PD_LOG ( PDDEBUG, "Remove standalone info is: %s",
                  info.toString(FALSE, TRUE).c_str() ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_HOSTNAME, &pStr ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Get field[%s] failed, rc: %d",
                         OMA_FIELD_HOSTNAME, rc ) ;
            goto error ;
         }
         bob.append( OMA_FIELD_UNINSTALLHOSTNAME, pStr ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_SVCNAME, &pStr ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Get field[%s] failed, rc: %d",
                         OMA_FIELD_SVCNAME, rc ) ;
            goto error ;
         }
         bob.append( OMA_FIELD_UNINSTALLSVCNAME, pStr ) ;
         bab.append( bob.obj() ) ;
         builder.appendArray( OMA_FIELD_HOSTINFO, bab.arr() ) ;
         bus = builder.obj() ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove standalone passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_STANDALONE, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_REMOVE_STANDALONE, rc ) ;
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
      remove catalog rg
   */

   _omaRmCataRG::_omaRmCataRG ( string &vCoordSvcName )
   {
      _vCoordSvcName = vCoordSvcName ;
   }

   _omaRmCataRG::~_omaRmCataRG ()
   {
   }
   
   INT32 _omaRmCataRG::init ( const CHAR *pUninstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONArrayBuilder bab ;
         BSONObjBuilder bob ;
         BSONObj bus ;
         BSONObj sys = BSON ( OMA_FIELD_VCOORDSVCNAME <<
                              _vCoordSvcName.c_str() ) ;
         BSONObj info = BSONObj( pUninstallInfo ).getOwned() ;
         const CHAR *pStr = NULL ;
         PD_LOG ( PDDEBUG, "Remove catalog group info is: %s",
                  info.toString(FALSE, TRUE).c_str() ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_AUTHUSER, &pStr ) ;
         PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                   "rc: %d", OMA_FIELD_AUTHUSER, rc ) ;
         bob.append( OMA_FIELD_AUTHUSER, pStr ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_AUTHPASSWD, &pStr ) ;
         PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                   "rc: %d", OMA_FIELD_AUTHPASSWD, rc ) ;
         bob.append( OMA_FIELD_AUTHPASSWD, pStr ) ;
         bus = bob.obj() ;
         
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove catalog group passes "
                  "argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_CATALOG_RG, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_REMOVE_CATALOG_RG, rc ) ;
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
      remove coord rg
   */

   _omaRmCoordRG::_omaRmCoordRG ( string &vCoordSvcName )
   {
      _vCoordSvcName = vCoordSvcName ;
   }

   _omaRmCoordRG::~_omaRmCoordRG ()
   {
   }
   
   INT32 _omaRmCoordRG::init ( const CHAR *pUninstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONArrayBuilder bab ;
         BSONObjBuilder bob ;
         BSONObj bus ;
         BSONObj sys = BSON ( OMA_FIELD_VCOORDSVCNAME <<
                              _vCoordSvcName.c_str() ) ;
         BSONObj info = BSONObj( pUninstallInfo ).getOwned() ;
         const CHAR *pStr = NULL ;
         PD_LOG ( PDDEBUG, "Remove coord group info is: %s",
                  info.toString(FALSE, TRUE).c_str() ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_AUTHUSER, &pStr ) ;
         PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                   "rc: %d", OMA_FIELD_AUTHUSER, rc ) ;
         bob.append( OMA_FIELD_AUTHUSER, pStr ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_AUTHPASSWD, &pStr ) ;
         PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                   "rc: %d", OMA_FIELD_AUTHPASSWD, rc ) ;
         bob.append( OMA_FIELD_AUTHPASSWD, pStr ) ;
         bus = bob.obj() ;
         
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove coord group passes "
                  "argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_COORD_RG, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_REMOVE_COORD_RG, rc ) ;
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
      remove data rg
   */

   _omaRmDataRG::_omaRmDataRG ( string &vCoordSvcName )
   {
      _vCoordSvcName = vCoordSvcName ;
   }

   _omaRmDataRG::~_omaRmDataRG ()
   {
   }
   
   INT32 _omaRmDataRG::init ( const CHAR *pUninstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONArrayBuilder bab ;
         BSONObjBuilder bob ;
         BSONObj bus ;
         BSONObj sys = BSON ( OMA_FIELD_VCOORDSVCNAME <<
                              _vCoordSvcName.c_str() ) ;
         BSONObj info = BSONObj( pUninstallInfo ).getOwned() ;
         const CHAR *pStr = NULL ;
         PD_LOG ( PDDEBUG, "Remove data group info is: %s",
                  info.toString(FALSE, TRUE).c_str() ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_AUTHUSER, &pStr ) ;
         PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                   "rc: %d", OMA_FIELD_AUTHUSER, rc ) ;
         bob.append( OMA_FIELD_AUTHUSER, pStr ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_AUTHPASSWD, &pStr ) ;
         PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                   "rc: %d", OMA_FIELD_AUTHPASSWD, rc ) ;
         bob.append( OMA_FIELD_AUTHPASSWD, pStr ) ;
         rc = omaGetStringElement ( info, OMA_FIELD_UNINSTALLGROUPNAME, &pStr ) ;
         PD_CHECK( SDB_OK == rc, rc, error, PDERROR, "Get field[%s] failed, "
                   "rc: %d", OMA_FIELD_UNINSTALLGROUPNAME, rc ) ;
         bob.append( OMA_FIELD_UNINSTALLGROUPNAME, pStr ) ;
         bus = bob.obj() ;
         
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove data group passes "
                  "argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_DATA_RG, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_REMOVE_DATA_RG, rc ) ;
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

}

