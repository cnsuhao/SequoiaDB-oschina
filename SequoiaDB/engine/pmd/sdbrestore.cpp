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

   Source File Name = sdbrestore.cpp

   Descriptive Name = Process MoDel Main

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for SequoiaDB,
   and all other process-initialization code.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/07/2013  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmd.hpp"
#include "msgMessage.hpp"
#include "ossStackDump.hpp"
#include "ossEDU.hpp"
#include "utilCommon.hpp"
#include "rtn.hpp"
#include "pmdCB.hpp"
#include "barRestoreJob.hpp"
#include "ossVer.h"
#include "pmdStartup.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "pmdController.hpp"

#include <iostream>
#include <string>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

using namespace std;
using namespace bson;
namespace po = boost::program_options ;
namespace fs = boost::filesystem ;

namespace engine
{

   /*
      restore logger define
   */
   barRSOfflineLogger   g_restoreLogger ;

   #define PMD_SDBRESTORE_DIAGLOG_NAME          "sdbrestore.txt"

   /*
      configure define
   */
   #define RS_BK_PATH            "bkpath"
   #define RS_BK_NAME            "bkname"
   #define RS_INC_ID             "increaseid"
   #define RS_BK_ACTION          "action"
   #define RS_BK_RESTORE         "restore"
   #define RS_BK_LIST            "list"
   #define RS_BK_IS_SELF         "isSelf"

   #define PMD_RS_OPTIONS  \
      ( PMD_COMMANDS_STRING (PMD_OPTION_HELP, ",h"), "help" ) \
      ( PMD_OPTION_VERSION, "show version" ) \
      ( PMD_COMMANDS_STRING (RS_BK_PATH, ",p"), boost::program_options::value<string>(), "backup path" ) \
      ( PMD_COMMANDS_STRING (RS_INC_ID, ",i"), boost::program_options::value<int>(), "increase id, default is -1" ) \
      ( PMD_COMMANDS_STRING (RS_BK_NAME, ",n"), boost::program_options::value<string>(), "backup name" ) \
      ( PMD_COMMANDS_STRING (RS_BK_ACTION, ",a"), boost::program_options::value<string>(), "action(restore/list), defalut is restore" ) \
      ( PMD_COMMANDS_STRING (PMD_OPTION_DIAGLEVEL, ",v"), boost::program_options::value<int>(), "diag level,default:3,value range:[0-5]" ) \
      ( RS_BK_IS_SELF, boost::program_options::value<string>(),          "whether restore self node(true/false),default is true" ) \
      ( PMD_OPTION_DBPATH, boost::program_options::value<string>(),      "override database path" )                    \
      ( PMD_OPTION_IDXPATH, boost::program_options::value<string>(),     "override index path" )                       \
      ( PMD_OPTION_LOGPATH, boost::program_options::value<string>(),     "override log file path" )                    \
      ( PMD_OPTION_CONFPATH, boost::program_options::value<string>(),    "override configure file path" )              \
      ( PMD_OPTION_DIAGLOGPATH, boost::program_options::value<string>(), "override diagnostic log file path" )         \
      ( PMD_OPTION_BKUPPATH, boost::program_options::value<string>(),    "override backup path" )                      \
      ( PMD_OPTION_SVCNAME, boost::program_options::value<string>(),     "override local service name or port" )       \
      ( PMD_OPTION_REPLNAME, boost::program_options::value<string>(),    "override replication service name or port" ) \
      ( PMD_OPTION_SHARDNAME, boost::program_options::value<string>(),   "override sharding service name or port" )    \
      ( PMD_OPTION_CATANAME, boost::program_options::value<string>(),    "override catalog service name or port" )     \
      ( PMD_OPTION_RESTNAME, boost::program_options::value<string>(),    "override REST service name or port" )        \

   #define RS_BK_ACTION_NAME_LEN          (20)


   BSONObj rsMakeNoneSelfCfg( const BSONObj &obj )
   {
      BSONObjBuilder builder ;
      BSONObjIterator it( obj ) ;
      while ( it.more() )
      {
         BSONElement ele = it.next () ;

         if ( 0 == ossStrcmp( ele.fieldName(), PMD_OPTION_DBPATH ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_IDXPATH ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_LOGPATH ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_CONFPATH ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_DIAGLOGPATH ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_BKUPPATH ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_SVCNAME ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_REPLNAME ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_SHARDNAME ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_CATANAME ) ||
              0 == ossStrcmp( ele.fieldName(), PMD_OPTION_RESTNAME ) )
         {
            continue ;
         }
         builder.append( ele ) ;
      }
      return builder.obj() ;
   }

   /*
      Tool functions :
   */
   INT32 sdbCleanDirFiles( const CHAR *pPath )
   {
      INT32 rc = SDB_OK ;

      fs::path dbDir ( pPath ) ;
      fs::directory_iterator end_iter ;

      if ( fs::exists ( dbDir ) && fs::is_directory ( dbDir ) )
      {
         for ( fs::directory_iterator dir_iter ( dbDir );
               dir_iter != end_iter; ++dir_iter )
         {
            if ( fs::is_regular_file ( dir_iter->status() ) )
            {
               const std::string fileName = dir_iter->path().string() ;
               rc = ossDelete( fileName.c_str() ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to remove %s, rc: %d",
                            fileName.c_str(), rc ) ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 sdbCleanDirSUFiles( const CHAR *pPath )
   {
      INT32 rc = SDB_OK ;
      CHAR csName [ DMS_COLLECTION_SPACE_NAME_SZ + 1 ] = {0} ;
      UINT32 sequence = 0 ;

      fs::path dbDir ( pPath ) ;
      fs::directory_iterator end_iter ;

      if ( fs::exists ( dbDir ) && fs::is_directory ( dbDir ) )
      {
         for ( fs::directory_iterator dir_iter ( dbDir );
               dir_iter != end_iter; ++dir_iter )
         {
            if ( fs::is_regular_file ( dir_iter->status() ) )
            {
               const std::string fileName =
                  dir_iter->path().filename().string() ;
               if ( rtnVerifyCollectionSpaceFileName( fileName.c_str(), csName,
                    DMS_COLLECTION_SPACE_NAME_SZ, sequence,
                    DMS_DATA_SU_EXT_NAME ) ||
                    rtnVerifyCollectionSpaceFileName( fileName.c_str(), csName,
                    DMS_COLLECTION_SPACE_NAME_SZ, sequence,
                    DMS_INDEX_SU_EXT_NAME ) )
               {
                  const std::string pathName = dir_iter->path().string() ;
                  rc = ossDelete( pathName.c_str() ) ;
                  PD_RC_CHECK( rc, PDERROR, "Failed to remove %s, rc: %d",
                               pathName.c_str(), rc ) ;
               }
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _rsOptionMgr define and implement
   */
   class _rsOptionMgr : public _pmdCfgRecord
   {
      public:
         _rsOptionMgr ()
         {
            ossMemset( _bkPath, 0, sizeof( _bkPath ) ) ;
            ossMemset( _bkName, 0, sizeof( _bkName ) ) ;
            ossMemset( _action, 0, sizeof( _action ) ) ;
            ossMemset( _dialogPath, 0, sizeof( _dialogPath ) ) ;
            ossMemset( _dbPath, 0, sizeof( _dbPath ) ) ;
            ossMemset( _cfgPath, 0, sizeof( _cfgPath ) ) ;
            ossMemset( _svcName, 0, sizeof( _svcName ) ) ;
            _incID = -1 ;
            _isSelf = TRUE ;
            _diagLevel = (UINT16)PDWARNING ;

            ossStrcpy( _dialogPath, PMD_OPTION_DIAG_PATH ) ;
         }

      protected:
         virtual INT32 doDataExchange( pmdCfgExchange *pEX )
         {
            resetResult() ;

            rdxString( pEX, RS_BK_PATH, _bkPath, sizeof( _bkPath ), FALSE,
                       FALSE, PMD_CURRENT_PATH ) ;
            rdxString( pEX, RS_BK_NAME, _bkName, sizeof( _bkName ), FALSE,
                       FALSE, "" ) ;
            rdxString( pEX, RS_BK_ACTION, _action, sizeof( _action ), FALSE,
                       FALSE, RS_BK_RESTORE ) ;
            rdxString( pEX, PMD_OPTION_DBPATH, _dbPath, sizeof( _dbPath ),
                       FALSE, FALSE, "" ) ;
            rdxString( pEX, PMD_OPTION_CONFPATH, _cfgPath, sizeof( _cfgPath ),
                       FALSE, FALSE, "" ) ;
            rdxString( pEX, PMD_OPTION_SVCNAME, _svcName, sizeof( _svcName ),
                       FALSE, FALSE, "" ) ;
            rdxBooleanS( pEX, RS_BK_IS_SELF, _isSelf, FALSE, FALSE, TRUE ) ;
            rdxInt( pEX, RS_INC_ID, _incID, FALSE, FALSE, -1 ) ;
            rdxUShort( pEX, PMD_OPTION_DIAGLEVEL, _diagLevel, FALSE, TRUE,
                       (UINT16)PDWARNING ) ;
            rdvMinMax( pEX, _diagLevel, PDSEVERE, PDDEBUG, TRUE ) ;

            return getResult() ;
         }
         virtual INT32 postLoaded()
         {
            if ( 0 != ossStrcmp( _action, RS_BK_RESTORE ) &&
                 0 != ossStrcmp( _action, RS_BK_LIST ) )
            {
               std::cerr << "action[ " << _action << " ] not invalid"
                         << std::endl ;
               return SDB_INVALIDARG ;
            }
            if ( 0 == ossStrcmp( _action, RS_BK_RESTORE ) &&
                 0 == ossStrlen( _bkName ) )
            {
               std::cerr << "In restore action, bkname can't be empty"
                         << std::endl ;
               return SDB_INVALIDARG ;
            }

            if ( !_isSelf && ( 0 == _dbPath[0] || 0 == _cfgPath[0] ||
                 0 == _svcName[0] ) )
            {
               std::cerr << "Restore not self node, must config "
                         << PMD_OPTION_DBPATH << ", " << PMD_OPTION_CONFPATH
                         << ", " << PMD_OPTION_SVCNAME << std::endl ;
               return SDB_INVALIDARG ;
            }

            ossMkdir( _dialogPath, OSS_CREATE|OSS_READWRITE ) ;

            return SDB_OK ;
         }

      public:
         CHAR              _bkPath[ OSS_MAX_PATHSIZE + 1 ] ;
         CHAR              _bkName[ OSS_MAX_PATHSIZE + 1 ] ;
         CHAR              _action[ RS_BK_ACTION_NAME_LEN + 1 ] ;
         CHAR              _dialogPath[ OSS_MAX_PATHSIZE + 1 ] ;
         INT32             _incID ;

         BOOLEAN           _isSelf ;
         CHAR              _dbPath[ OSS_MAX_PATHSIZE + 1 ] ;
         CHAR              _svcName[ OSS_MAX_SERVICENAME + 1 ] ;
         CHAR              _cfgPath[ OSS_MAX_PATHSIZE + 1 ] ;

         UINT16            _diagLevel ;

         po::variables_map _vm ;
   } ;
   typedef _rsOptionMgr rsOptionMgr ;

   INT32 resolveArguments( INT32 argc, CHAR** argv, rsOptionMgr &rsOptMgr )
   {
      INT32 rc = SDB_OK ;

      po::variables_map vm ;
      po::options_description desc( "Command options" ) ;

      PMD_ADD_PARAM_OPTIONS_BEGIN( desc )
         PMD_RS_OPTIONS
      PMD_ADD_PARAM_OPTIONS_END

      rc = utilReadCommandLine( argc, argv,  desc, vm ) ;
      if ( rc )
      {
         std::cerr << "read command line failed: " << rc << std::endl ;
         goto error ;
      }

      rsOptMgr._vm = vm ;
      if ( vm.count( PMD_OPTION_HELP ) )
      {
         std::cout << desc << std::endl ;
         rc = SDB_PMD_HELP_ONLY ;
         goto done ;
      }
      if ( vm.count( PMD_OPTION_VERSION ) )
      {
         ossPrintVersion( "Sdb Restore Version" ) ;
         rc = SDB_PMD_VERSION_ONLY ;
         goto done ;
      }

      rc = rsOptMgr.init( NULL, &vm ) ;
      if ( rc )
      {
         std::cerr << "Init restore optionMgr failed: " << rc << std::endl ;
         goto error ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

   void registerCB()
   {
      PMD_REGISTER_CB( sdbGetDPSCB() ) ;
      PMD_REGISTER_CB( sdbGetTransCB() ) ;
      PMD_REGISTER_CB( sdbGetDMSCB() ) ;
      PMD_REGISTER_CB( sdbGetRTNCB() ) ;
   }

   INT32 restoreSysInit ()
   {
      INT32 rc = SDB_OK ;

      rc = pmdGetStartup().init( pmdGetOptionCB()->getDbPath() ) ;
      if ( rc )
      {
         std::cout << "Check sequoiadb("
                   << pmdGetOptionCB()->getServiceAddr()
                   << ") is not running...FAILED" << std::endl ;
         goto error ;
      }
      std::cout << "Check sequoiadb("
                << pmdGetOptionCB()->getServiceAddr()
                << ") is not running...OK" << std::endl ;

      std::cout << "Begin to clean dps logs..." << std::endl ;
      rc = sdbCleanDirFiles( pmdGetOptionCB()->getReplLogPath() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to clean dps logs[%s], rc: %d",
                   pmdGetOptionCB()->getReplLogPath(), rc ) ;

      std::cout << "Begin to clean dms storages..." << std::endl ;
      rc = sdbCleanDirSUFiles( pmdGetOptionCB()->getDbPath() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to clean data[%s] su, rc: %d",
                   pmdGetOptionCB()->getDbPath(), rc ) ;
      if ( 0 != ossStrcmp( pmdGetOptionCB()->getDbPath(),
                           pmdGetOptionCB()->getIndexPath() ) )
      {
         rc = sdbCleanDirSUFiles( pmdGetOptionCB()->getIndexPath() ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to clean index[%s], rc: %d",
                      pmdGetOptionCB()->getIndexPath(), rc ) ;
      }

      pmdGetStartup().ok( TRUE ) ;
      pmdGetStartup().final() ;

      std::cout << "Begin to init dps logs..." << std::endl ;

   done :
      return rc ;
   error :
      goto done ;
   }

   INT32 listBackups ( rsOptionMgr &optMgr )
   {
      barBackupMgr bkMgr ;
      INT32 rc = bkMgr.init( optMgr._bkPath, optMgr._bkName, NULL ) ;
      if ( rc )
      {
         std::cerr << "Init backup manager failed: " << rc << std::endl ;
         return rc ;
      }
      vector < BSONObj > backups ;
      rc = bkMgr.list( backups, TRUE ) ;
      if ( rc )
      {
         std::cerr << "List backups failed: " << rc << std::endl ;
         return rc ;
      }
      std::cout << "backup list: " << std::endl ;
      vector < BSONObj >::iterator it = backups.begin() ;
      while ( it != backups.end() )
      {
         std::cout << "    " << (*it).toString().c_str() << std::endl ;
         ++it ;
      }
      std::cout << "total: " << backups.size() << std::endl ;

      return SDB_OK ;
   }

   void pmdOnQuit()
   {
      PMD_SHUTDOWN_DB( SDB_INTERRUPT ) ;
   }

   INT32 pmdRestoreThreadMain ( INT32 argc, CHAR** argv )
   {
      INT32      rc       = SDB_OK ;
      pmdKRCB   *krcb     = pmdGetKRCB () ;
      EDUID      agentEDU = PMD_INVALID_EDUID ;
      CHAR diaglog[ OSS_MAX_PATHSIZE + 1 ] = {0} ;
      rsOptionMgr optMgr ;

      rc = resolveArguments ( argc, argv, optMgr ) ;
      if ( SDB_PMD_HELP_ONLY == rc || SDB_PMD_VERSION_ONLY == rc )
      {
         PMD_SHUTDOWN_DB( SDB_OK ) ;
         rc = SDB_OK ;
         return rc ;
      }
      else if ( rc )
      {
         return rc ;
      }

      utilBuildFullPath( optMgr._dialogPath, PMD_SDBRESTORE_DIAGLOG_NAME,
                         OSS_MAX_PATHSIZE, diaglog ) ;
      sdbEnablePD( diaglog ) ;
      setPDLevel( (PDLEVEL)optMgr._diagLevel ) ;

      rc = pmdEnableSignalEvent( optMgr._dialogPath,
                                 (PMD_ON_QUIT_FUNC)pmdOnQuit ) ;
      if ( rc )
      {
         std::cerr << "Failed to setup signal handler, rc: " << rc
                   << std::endl ;
         return rc ;
      }

      registerCB() ;

      if ( 0 == ossStrcmp( optMgr._action, RS_BK_LIST ) )
      {
         rc = listBackups( optMgr ) ;
         return rc ;
      }

      PD_LOG ( ( getPDLevel() > PDEVENT ? PDEVENT : getPDLevel() ) ,
               "Start sdbrestore [Ver: %d.%d, Release: %d, Build: %s]...",
               SDB_ENGINE_VERISON_CURRENT, SDB_ENGINE_SUBVERSION_CURRENT,
               SDB_ENGINE_RELEASE_CURRENT, SDB_ENGINE_BUILD_TIME ) ;

      rc = g_restoreLogger.init( optMgr._bkPath, optMgr._bkName, NULL,
                                 optMgr._incID ) ;
      if ( rc )
      {
         std::cerr << "Init restore failed: " << rc << std::endl ;
         goto error ;
      }

      if ( optMgr._isSelf )
      {
         rc = krcb->getOptionCB()->restore ( g_restoreLogger.getConf(),
                                             &(optMgr._vm) ) ;
      }
      else
      {
         BSONObj newCfgObj = rsMakeNoneSelfCfg( g_restoreLogger.getConf() ) ;
         rc = krcb->getOptionCB()->restore ( newCfgObj, &(optMgr._vm) ) ;
      }
      if ( rc )
      {
         std::cerr << "Init option cb failed: " << rc << std::endl ;
         goto error ;
      }

      rc = restoreSysInit () ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to initialize, rc: %d", rc ) ;

      rc = krcb->init() ;
      if ( rc )
      {
         std::cerr << "init krcb failed, " << rc << std::endl ;
         return rc ;
      }

      std::cout << "Begin to restore... " << std::endl ;
      rc = startRestoreJob( &agentEDU, &g_restoreLogger ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start restore task, rc: %d", rc ) ;
         std::cerr << "Start restore task failed: " << rc << std::endl ;
         goto error ;
      }

      while ( PMD_IS_DB_UP )
      {
         ossSleepsecs ( 1 ) ;
      }
      rc = krcb->getExitCode() ;

   done :
      PMD_SHUTDOWN_DB( rc ) ;
      pmdSetQuit() ;
      krcb->destroy () ;
      PD_LOG ( PDEVENT, "Stop sdbrestore, exist code: %d",
               krcb->getExitCode() ) ;

      std::cout << "*****************************************************"
                << std::endl ;
      if ( SDB_OK != krcb->getExitCode() )
      {
         std::cout << "Restore failed: " << krcb->getExitCode() << std::endl ;
      }
      else
      {
         std::cout << "Restore succeed!" << std::endl ;
      }
      std::cout << "*****************************************************"
                << std::endl ;
      return rc ;
   error :
      goto done ;
   }

}

/**************************************/
/*   SDB RESTORE MAIN FUNCTION        */
/**************************************/
INT32 main ( INT32 argc, CHAR** argv )
{
   INT32 rc = SDB_OK ;
   rc = engine::pmdRestoreThreadMain ( argc, argv ) ;
   return rc ;
}

