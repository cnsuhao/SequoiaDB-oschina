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

   Source File Name = pmdMain.cpp

   Descriptive Name = Process MoDel Main

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for SequoiaDB,
   and all other process-initialization code.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "ossVer.h"
#include "pmd.hpp"
#include "rtn.hpp"
#include "ossProc.hpp"
#include "utilCommon.hpp"
#include "pmdStartup.hpp"
#include "optQgmStrategy.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "pmdController.hpp"

using namespace std;
using namespace bson;

namespace engine
{
   /*
    * This function resolve all input arguments from command line
    * It first construct options_description to register all
    * possible arguments we may have
    * And then it will to load from config file
    * Then it will parse command line input again to override config file
    * Basically we want to make sure all parameters that
    * specified in config file
    * can be simply overrided from commandline
    */
   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDRESVARGS, "pmdResolveArguments" )
   INT32 pmdResolveArguments( INT32 argc, CHAR** argv )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDRESVARGS ) ;
      CHAR exePath[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;

      rc = ossGetEWD( exePath, OSS_MAX_PATHSIZE ) ;
      if ( rc )
      {
         std::cerr << "Get module path failed: " << rc << std::endl ;
         goto error ;
      }

      rc = pmdGetOptionCB()->init( argc, argv, exePath ) ;
      if ( SDB_PMD_HELP_ONLY == rc || SDB_PMD_VERSION_ONLY == rc )
      {
         PMD_SHUTDOWN_DB( SDB_OK ) ;
         rc = SDB_OK;
         goto done;
      }
      else if ( rc )
      {
         goto error;
      }

   done :
      PD_TRACE_EXITRC ( SDB_PMDRESVARGS, rc );
      return rc ;
   error :
      goto done ;
   }

   void pmdOnQuit()
   {
      PMD_SHUTDOWN_DB( SDB_INTERRUPT ) ;
   }

   static INT32 _pmdSystemInit()
   {
      INT32 rc = SDB_OK ;

      rc = pmdGetStartup().init( pmdGetOptionCB()->getDbPath() ) ;
      PD_RC_CHECK( rc, PDERROR, "Start up check failed[rc:%d]", rc ) ;

      rc = getQgmStrategyTable()->init() ;
      PD_RC_CHECK( rc, PDERROR, "Init qgm strategy table failed, rc: %d",
                   rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   static INT32 _pmdPostInit()
   {
      INT32 rc = SDB_OK ;

      if ( SDB_ROLE_STANDALONE == pmdGetDBRole() )
      {
         pmdSetPrimary( TRUE ) ;

         if ( !pmdGetStartup().isOK() )
         {
            pmdEDUCB *cb = pmdGetThreadEDUCB() ;
            rc = rtnRebuildDB( cb ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to rebuild database, rc: %d",
                         rc ) ;

            rc = sdbGetDPSCB()->move( 0, 0 ) ;
            if ( rc )
            {
               PD_LOG( PDERROR, "Move dps to begin failed, rc: %d", rc ) ;
               goto error ;
            }
            PD_LOG( PDEVENT, "Clean dps logs succeed." ) ;
            PD_LOG( PDEVENT, "Rebuild database succeed." ) ;
            pmdGetStartup().ok( TRUE ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   #define PMD_START_WAIT_TIME         ( 60000 )

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDMSTTHRDMAIN, "pmdMasterThreadMain" )
   INT32 pmdMasterThreadMain ( INT32 argc, CHAR** argv )
   {
      INT32      rc       = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDMSTTHRDMAIN );
      pmdKRCB   *krcb     = pmdGetKRCB () ;
      UINT32     startTimerCount = 0 ;

      rc = pmdResolveArguments ( argc, argv ) ;
      if ( rc )
      {
         ossPrintf( "Failed resolving arguments(error=%d), exit"OSS_NEWLINE,
                    rc ) ;
         goto error ;
      }
      if ( PMD_IS_DB_DOWN )
      {
         return rc ;
      }

      sdbEnablePD( pmdGetOptionCB()->getDiagLogPath(),
                   pmdGetOptionCB()->diagFileNum() ) ;
      setPDLevel( (PDLEVEL)( pmdGetOptionCB()->getDiagLevel() ) ) ;

      PD_LOG ( ( getPDLevel() > PDEVENT ? PDEVENT : getPDLevel() ) ,
               "Start sequoiadb(%s) [Ver: %d.%d, Release: %d, Build: %s]...",
               pmdGetOptionCB()->krcbRole(), SDB_ENGINE_VERISON_CURRENT,
               SDB_ENGINE_SUBVERSION_CURRENT, SDB_ENGINE_RELEASE_CURRENT,
               SDB_ENGINE_BUILD_TIME ) ;

      {
         BSONObj confObj ;
         krcb->getOptionCB()->toBSON( confObj ) ;
         PD_LOG( PDEVENT, "All configs: %s", confObj.toString().c_str() ) ;
      }

      rc = pmdEnableSignalEvent( pmdGetOptionCB()->getDiagLogPath(),
                                 (PMD_ON_QUIT_FUNC)pmdOnQuit ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to enable trap, rc: %d", rc ) ;

      sdbGetPMDController()->registerCB( pmdGetDBRole() ) ;

      rc = _pmdSystemInit() ;
      if ( rc )
      {
         goto error ;
      }

      rc = krcb->init() ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to init krcb, rc: %d", rc ) ;
         goto error ;
      }

      rc = _pmdPostInit() ;
      if ( rc )
      {
         goto error ;
      }

      while ( PMD_IS_DB_UP && startTimerCount < PMD_START_WAIT_TIME &&
              !krcb->isBusinessOK() )
      {
         ossSleepmillis( 100 ) ;
         startTimerCount += 100 ;
      }

      if ( PMD_IS_DB_DOWN )
      {
         rc = krcb->getExitCode() ;
         PD_LOG( PDERROR, "Start failed, rc: %d", rc ) ;
         goto error ;
      }
      else if ( startTimerCount >= PMD_START_WAIT_TIME )
      {
         PD_LOG( PDWARNING, "Start warning (timeout)" ) ;
      }

#if defined (_LINUX)
      {
         CHAR pmdProcessName [ OSS_RENAME_PROCESS_BUFFER_LEN + 1 ] = {0} ;
         ossSnprintf ( pmdProcessName, OSS_RENAME_PROCESS_BUFFER_LEN,
                       "%s(%s) %s", utilDBTypeStr( pmdGetDBType() ),
                       pmdGetOptionCB()->getServiceAddr(),
                       utilDBRoleShortStr( pmdGetDBRole() ) ) ;
         ossEnableNameChanges ( argc, argv ) ;
         ossRenameProcess ( pmdProcessName ) ;
      }
#endif // _LINUX
      {
         EDUID agentEDU = PMD_INVALID_EDUID ;
         pmdEDUMgr *eduMgr = pmdGetKRCB()->getEDUMgr() ;
         eduMgr->startEDU ( EDU_TYPE_PIPESLISTENER,
                            (void*)pmdGetOptionCB()->getServiceAddr(),
                            &agentEDU ) ;
         eduMgr->regSystemEDU ( EDU_TYPE_PIPESLISTENER, agentEDU ) ;
      }

      while ( PMD_IS_DB_UP )
      {
         ossSleepsecs ( 1 ) ;
         sdbGetPMDController()->onTimer( OSS_ONE_SEC ) ;
      }
      rc = krcb->getExitCode() ;

   done :
      PMD_SHUTDOWN_DB( rc ) ;
      pmdSetQuit() ;
      krcb->destroy () ;
      pmdGetStartup().final() ;
      PD_LOG ( PDEVENT, "Stop sequoiadb, exit code: %d",
               krcb->getExitCode() ) ;
      PD_TRACE_EXITRC ( SDB_PMDMSTTHRDMAIN, rc );
      return utilRC2ShellRC( rc ) ;
   error :
      goto done ;
   }

}

/**************************************/
/*   DATABASE MAIN FUNCTION           */
/**************************************/
//PD_TRACE_DECLARE_FUNCTION ( SDB_PMDMAIN, "main" )
INT32 main ( INT32 argc, CHAR** argv )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB_PMDMAIN );
   rc = engine::pmdMasterThreadMain ( argc, argv ) ;
   PD_TRACE_EXITRC ( SDB_PMDMAIN, rc );
   return rc ;
}

