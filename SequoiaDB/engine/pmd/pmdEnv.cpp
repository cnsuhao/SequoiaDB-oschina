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

   Source File Name = pmdEnv.cpp

   Descriptive Name = Process MoDel Main

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for SequoiaDB,
   and all other process-initialization code.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          22/04/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdEnv.hpp"
#include "ossEDU.hpp"
#include "pmdSignalHandler.hpp"
#include "pmd.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"

using namespace bson ;

namespace engine
{
   pmdSysInfo* pmdGetSysInfo()
   {
      static pmdSysInfo s_sysInfo ;
      return &s_sysInfo ;
   }
   SDB_ROLE pmdGetDBRole()
   {
      return pmdGetSysInfo()->_dbrole ;
   }
   void  pmdSetDBRole( SDB_ROLE role )
   {
      pmdGetSysInfo()->_dbrole = role ;
      pmdSetDBType( utilRoleToType( role ) ) ;
   }
   SDB_TYPE pmdGetDBType()
   {
      return pmdGetSysInfo()->_dbType ;
   }
   void pmdSetDBType( SDB_TYPE type )
   {
      pmdGetSysInfo()->_dbType = type ;
   }
   MsgRouteID pmdGetNodeID()
   {
      return pmdGetSysInfo()->_nodeID ;
   }
   void pmdSetNodeID( MsgRouteID id )
   {
      pmdGetSysInfo()->_nodeID = id ;
   }
   BOOLEAN pmdIsPrimary ()
   {
      return pmdGetSysInfo()->_isPrimary.peek() ;
   }
   void pmdSetPrimary( BOOLEAN primary )
   {
      pmdGetSysInfo()->_isPrimary.init( primary ) ;
   }

   BOOLEAN pmdIsQuitApp()
   {
      return pmdGetSysInfo()->_quitFlag ;
   }

   void pmdSetQuit()
   {
      pmdGetSysInfo()->_quitFlag = TRUE ;
   }

   INT32& pmdGetSigNum()
   {
      static INT32 s_sigNum = -1 ;
      return s_sigNum ;
   }

#if defined (_LINUX)

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDSIGHND, "pmdSignalHandler" )
   void pmdSignalHandler ( INT32 sigNum )
   {
      PD_TRACE_ENTRY ( SDB_PMDSIGHND ) ;

      PMD_SIGNUM = sigNum ;

      if ( sigNum > 0 && sigNum <= OSS_MAX_SIGAL )
      {
         PD_LOG ( PDEVENT, "Recieve signal[%d:%s, %s]",
                  sigNum, pmdGetSignalInfo( sigNum )._name,
                  pmdGetSignalInfo( sigNum )._handle ? "QUIT" : "IGNORE" ) ;
         if ( pmdGetSignalInfo( sigNum )._handle ) // quit
         {
            pmdGetSysInfo()->_quitFlag = TRUE ;
            if ( pmdGetSysInfo()->_pQuitFunc )
            {
               (*pmdGetSysInfo()->_pQuitFunc)() ;
            }
         }
      }
      PD_TRACE_EXIT ( SDB_PMDSIGHND ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDEDUUSERTRAPHNDL, "pmdEDUUserTrapHandler" )
   void pmdEDUUserTrapHandler( OSS_HANDPARMS )
   {
#if defined( SDB_ENGINE )
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDEDUUSERTRAPHNDL );
      oss_edu_data * pEduData = NULL ;
      const CHAR *dumpPath = ossGetTrapExceptionPath () ;
      if ( !dumpPath )
      {
         goto done ;
      }

      pEduData = ossGetThreadEDUData() ;

      if ( NULL == pEduData )
      {
         goto done ;
      }

      if ( OSS_AM_I_INSIDE_SIGNAL_HANDLER( pEduData ) )
      {
         goto done ;
      }
      OSS_ENTER_SIGNAL_HANDLER( pEduData ) ;

      if ( signum == OSS_STACK_DUMP_SIGNAL )
      {
         PD_LOG ( PDEVENT, "Signal %d is received, "
                  "prepare to dump stack for all threads", signum ) ;
         std::set<pthread_t>::iterator it ;
         std::set<pthread_t> tidList ;
         pmdGetKRCB()->getEDUMgr()->getEDUThreadID ( tidList ) ;
         for ( it = tidList.begin(); it != tidList.end(); ++it )
         {
            if ( 0 == (*it) )
            {
               continue ;
            }
            rc = ossPThreadKill ( (*it), OSS_STACK_DUMP_SIGNAL_INTERNAL ) ;
            if ( rc )
            {
               PD_LOG ( PDWARNING, "Failed to send signal %d to thread %llu, "
                        "errno = %d", OSS_STACK_DUMP_SIGNAL_INTERNAL,
                        (*it), ossGetLastError() ) ;
            }
         }
         ossMemTrace ( dumpPath ) ;
      }
      else if ( signum == OSS_STACK_DUMP_SIGNAL_INTERNAL )
      {
         PD_LOG ( PDEVENT, "Signal %d is received, "
                  "prepare to dump stack for %u:%u", signum,
                  ossGetCurrentProcessID(),
                  ossGetCurrentThreadID() ) ;
         ossStackTrace( OSS_HANDARGS, dumpPath ) ;
      }
      else
      {
         PD_LOG ( PDWARNING, "Unexpected signal is received: %d",
                  signum ) ;
      }
      OSS_LEAVE_SIGNAL_HANDLER( pEduData ) ;
   done :
      PD_TRACE1 ( SDB_PMDEDUUSERTRAPHNDL, PD_PACK_INT(rc) );
      PD_TRACE_EXIT ( SDB_PMDEDUUSERTRAPHNDL ) ;
#endif // SDB_ENGINE
      return ;
   }

   INT32 pmdEnableSignalEvent( const CHAR *filepath, PMD_ON_QUIT_FUNC pFunc,
                               INT32 *pDelSig )
   {
      INT32 rc = SDB_OK ;
      ossSigSet sigSet ;
      struct sigaction newact ;
      ossMemset ( &newact, 0, sizeof(newact)) ;
      sigemptyset ( &newact.sa_mask ) ;

      if ( filepath )
      {
         ossSetTrapExceptionPath ( filepath ) ;
      }
      pmdGetSysInfo()->_pQuitFunc = pFunc ;

      newact.sa_sigaction = ( OSS_SIGFUNCPTR ) ossEDUCodeTrapHandler ;
      newact.sa_flags |= SA_SIGINFO ;
      newact.sa_flags |= SA_ONSTACK ;
      if ( sigaction ( SIGSEGV, &newact, NULL ) )
      {
         PD_LOG ( PDERROR, "Failed to setup signal handler for SIGSEGV" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( sigaction ( SIGBUS, &newact, NULL ) )
      {
         PD_LOG ( PDERROR, "Failed to setup signal handler for SIGBUS" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      newact.sa_sigaction = ( OSS_SIGFUNCPTR ) pmdEDUUserTrapHandler ;
      newact.sa_flags |= SA_SIGINFO ;
      newact.sa_flags |= SA_ONSTACK ;
      if ( sigaction ( OSS_STACK_DUMP_SIGNAL, &newact, NULL ) )
      {
         PD_LOG ( PDERROR, "Failed to setup signal handler for dump signal" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( sigaction ( OSS_STACK_DUMP_SIGNAL_INTERNAL, &newact, NULL ) )
      {
         PD_LOG ( PDERROR, "Failed to setup signal handler for dump signal" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      sigSet.fillSet () ;
      sigSet.sigDel ( SIGSEGV ) ;
      sigSet.sigDel ( SIGBUS ) ;
      sigSet.sigDel ( SIGALRM ) ;
      sigSet.sigDel ( SIGPROF ) ;
      sigSet.sigDel ( OSS_STACK_DUMP_SIGNAL ) ;
      sigSet.sigDel ( OSS_STACK_DUMP_SIGNAL_INTERNAL ) ;

      if ( pDelSig )
      {
         UINT32 i = 0 ;
         while ( 0 != pDelSig[ i ] )
         {
            sigSet.sigDel( pDelSig[ i ] ) ;
            ++i ;
         }
      }

      rc = ossRegisterSignalHandle( sigSet, (SIG_HANDLE)pmdSignalHandler ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDWARNING, "Failed to register signals, rc = %d", rc ) ;
         rc = SDB_OK ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

#else

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDCTRLHND, "pmdCtrlHandler" )
   BOOL pmdCtrlHandler( DWORD fdwCtrlType )
   {
      BOOLEAN ret = FALSE ;
      PD_TRACE_ENTRY ( SDB_PMDCTRLHND );
      switch( fdwCtrlType )
      {
      case CTRL_C_EVENT:
         printf( "Ctrl-C event\n\n" ) ;
         pmdGetSysInfo()->_quitFlag = TRUE ;
         if ( pmdGetSysInfo()->_pQuitFunc )
         {
            (*pmdGetSysInfo()->_pQuitFunc)() ;
         }
         Beep( 750, 300 );
         ret = TRUE ;
         goto done ;

      case CTRL_CLOSE_EVENT:
         Beep( 600, 200 );
         printf( "Ctrl-Close event\n\n" ) ;
         ret = TRUE ;
         goto done ;

      case CTRL_BREAK_EVENT:
         Beep( 900, 200 );
         printf( "Ctrl-Break event\n\n" ) ;
         ret = FALSE ;
         goto done ;

      case CTRL_LOGOFF_EVENT:
         Beep( 1000, 200 );
         printf( "Ctrl-Logoff event\n\n" ) ;
         ret = FALSE ;
         goto done ;

      case CTRL_SHUTDOWN_EVENT:
         Beep( 750, 500 );
         printf( "Ctrl-Shutdown event\n\n" ) ;
         ret = FALSE ;
         goto done ;

      default:
         ret = FALSE ;
         goto done ;
      }
   done :
      PD_TRACE1 ( SDB_PMDCTRLHND, PD_PACK_INT(ret) ) ;
      PD_TRACE_EXIT ( SDB_PMDCTRLHND ) ;
      return ret ;
   }

   INT32 pmdEnableSignalEvent( const CHAR * filepath, PMD_ON_QUIT_FUNC pFunc,
                               INT32 *pDelSig )
   {
      if ( filepath )
      {
         ossSetTrapExceptionPath ( filepath ) ;
      }
      pmdGetSysInfo()->_pQuitFunc = pFunc ;

      SetConsoleCtrlHandler( (PHANDLER_ROUTINE)pmdCtrlHandler, TRUE ) ;

      return SDB_OK ;
   }

#endif // _LINUX

}


