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

   Source File Name = omagentCommand.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omagentSyncCmd.hpp"
#include "omagentUtil.hpp"
#include "omagentHelper.hpp"
#include "ossProc.hpp"
#include "utilPath.hpp"
#include "ossPath.h"
#include "omagentJob.hpp"
#include "omagentMgr.hpp"

using namespace bson ;

namespace engine
{
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaScanHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaPreCheckHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaCheckHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaPostCheckHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaUpdateHostsInfo )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaQueryHostStatus )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaHandleTaskNotify )


   /******************************* scan host *********************************/
   /*
      _omaScanHost
   */
   _omaScanHost::_omaScanHost()
   {
   }

   _omaScanHost::~_omaScanHost()
   {
   }

   INT32 _omaScanHost::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj obj( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; "
                      "var %s = %s; var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, obj.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, "{}",
                      JS_ARG_ENV, "{}",
                      JS_ARG_OTHER, "{}" ) ;
         PD_LOG ( PDDEBUG, "Scan host passes argument: %s", _jsFileArgs ) ;
         rc = addJsFile( FILE_SCAN_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_SCAN_HOST, rc ) ;
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

   /******************************* pre-check host ****************************/
   /*
      _omaPreCheckHost
   */
   _omaPreCheckHost::_omaPreCheckHost ()
   {
   }

   _omaPreCheckHost::~_omaPreCheckHost ()
   {
   }

   INT32 _omaPreCheckHost::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      CHAR prog_path[ OSS_MAX_PATHSIZE + 1 ] = { 0 };

      try
      {
         BSONObj bus( pInstallInfo ) ;
         BSONObj sys ;
         rc = _getProgPath ( prog_path, OSS_MAX_PATHSIZE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get sdbcm program path, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         sys = BSON( OMA_FIELD_PROG_PATH << prog_path ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; "
                      "var %s = %s; var %s = %s; var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str(),
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str(),
                      JS_ARG_ENV, "{}",
                      JS_ARG_OTHER, "{}" ) ;
         PD_LOG ( PDDEBUG, "Pre-check host passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_PRE_CHECK_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_PRE_CHECK_HOST, rc ) ;
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

   INT32 _omaPreCheckHost::_getProgPath ( CHAR *path, INT32 len )
   {
      INT32 rc = SDB_OK ;
      std::string str ;
      std::string key ;
      UINT32 found = 0 ;
      CHAR tmp[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;

      rc = ossGetEWD ( tmp, OSS_MAX_PATHSIZE ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR,
                      "Failed to get program's work directory, rc = %d", rc ) ;
         goto error ;
      }
      rc = utilCatPath ( tmp, OSS_MAX_PATHSIZE, SDBSDBCMPROG ) ;
      if ( rc )
      {
         PD_LOG_MSG ( PDERROR,
                      "Failed to build program's full directory, rc = %d",
                      rc ) ;
         goto error ;
      }
      str = tmp ;
      key = SDBSDBCMPROG ;
      found = str.rfind( key ) ;
      if ( found != std::string::npos )
      {
         str.replace( found, key.length(), "\0" ) ;
         ossStrncpy( path, str.c_str(), len ) ;
      }
      else
      {
         rc = SDB_SYS ;
         PD_LOG_MSG ( PDERROR, "Failed to set program's path" ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /******************************* check host ********************************/
   /*
      _omaCheckHost
   */
   _omaCheckHost::_omaCheckHost ()
   {
   }

   _omaCheckHost::~_omaCheckHost ()
   {
   }

   INT32 _omaCheckHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;

      try
      {
         BSONObj bus( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Check host info passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_CHECK_HOST_ITEM ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_CHECK_HOST_ITEM, rc ) ;
            goto error ;
         }
         rc = addJsFile( FILE_CHECK_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_CHECK_HOST, rc ) ;
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

   /******************************* post-check host ***************************/
   /*
      _omaPostCheckHost
   */
   _omaPostCheckHost::_omaPostCheckHost ()
   {
   }

   _omaPostCheckHost::~_omaPostCheckHost ()
   {
   }

   INT32 _omaPostCheckHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Post-check host passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_POST_CHECK_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_POST_CHECK_HOST, rc ) ;
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

   
   /******************************* query progress status *********************/
   /*
      _omaQueryTaskProgress
   */
/*
   _omaQueryTaskProgress::_omaQueryTaskProgress ()
   {
      _taskID = OMA_INVALID_TASKID ;
   }

   _omaQueryTaskProgress::~_omaQueryTaskProgress ()
   {
   }

   INT32 _omaQueryTaskProgress::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      BSONObj arg( pInstallInfo ) ;
      try
      {
         ele = arg.getField ( OMA_FIELD_TASKID ) ;
         if ( NumberInt != ele.type() && NumberLong != ele.type() )
         {
            rc = SDB_UNEXPECTED_RESULT ;
            PD_LOG_MSG ( PDERROR, "Failed to get taskID, rc = %d", rc ) ;
            goto error ;
         }
         _taskID = ele.numberLong () ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG_MSG ( PDERROR,  "Failed to get taskID, "
                      "received exception: %s", e.what() ) ;
         goto error ;
      }
      _taskMgr = getTaskMgr() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaQueryTaskProgress::doit ( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      _omaTask *pTask  = NULL ;
      pTask = _taskMgr->findTask( _taskID ) ;
      if ( NULL == pTask )
      {
         rc = SDB_CAT_TASK_NOTFOUND ;
         PD_LOG_MSG ( PDERROR, "No such task with id[%ld], "
                      "failed to query task's progress", (INT64)_taskID ) ;
         goto error ;
      }
      rc = pTask->queryProgress( retObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to query task's progress, "
                  "rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }
*/
   /*************************** update hosts table info **********************/
   /*
      _omaUpdateHostsInfo
   */
   _omaUpdateHostsInfo::_omaUpdateHostsInfo ()
   {
   }

   _omaUpdateHostsInfo::~_omaUpdateHostsInfo ()
   {
   }

   INT32 _omaUpdateHostsInfo::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      BSONObj bus( pInstallInfo ) ;
      ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                   JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
      PD_LOG ( PDDEBUG, "Update hosts info passes argument: %s",
               _jsFileArgs ) ;
      rc = addJsFile ( FILE_UPDATE_HOSTS_INFO, _jsFileArgs ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to add js file[%s]", FILE_UPDATE_HOSTS_INFO ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*************************** query host status **************************/
   /*
      _omaQueryHostStatus
   */
   _omaQueryHostStatus::_omaQueryHostStatus()
   {
   }

   _omaQueryHostStatus::~_omaQueryHostStatus()
   {
   }

   INT32 _omaQueryHostStatus::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "_omaQueryHostStatus passes argument: %s",
                  _jsFileArgs ) ;

         rc = addJsFile( FILE_QUERY_HOSTSTATUS_ITEM ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_QUERY_HOSTSTATUS_ITEM, rc ) ;
            goto error ;
         }

         rc = addJsFile( FILE_QUERY_HOSTSTATUS, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_QUERY_HOSTSTATUS, rc ) ;
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
   error :
      goto done ;
   }


   /*************************** handle task notify ****************************/
   /*
      _omaHandleTaskNotify
   */
   _omaHandleTaskNotify::_omaHandleTaskNotify()
   {
   }

   _omaHandleTaskNotify::~_omaHandleTaskNotify()
   {
   }

   INT32 _omaHandleTaskNotify::init ( const CHAR *pInstallInfo )
   {
      INT32 rc      = SDB_OK ;
      UINT64 taskID = 0 ;
      BSONObj obj ;
      BSONElement ele ;
      try
      {
         obj = BSONObj( pInstallInfo ).copy() ;
         ele = obj.getField( OMA_FIELD_TASKID ) ;
         if ( NumberInt != ele.type() && NumberLong != ele.type() )
         {
            PD_LOG_MSG ( PDERROR, "Receive invalid task id from omsvc" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         taskID = ele.numberLong() ;
         _taskIDObj = BSON( OMA_FIELD_TASKID << (INT64)taskID ) ;
         
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

   INT32 _omaHandleTaskNotify::doit ( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      
      rc = sdbGetOMAgentMgr()->startTaskCheck( _taskIDObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start task check, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

} // namespace engine

