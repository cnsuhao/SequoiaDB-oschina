/*******************************************************************************

   Copyright (C) 2012-2014 SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*******************************************************************************/
/*
@description: rollback standalone
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "UninstallHostName": "rhel64-test8", "UninstallSvcName": "11820" }
   SYS_JSON: the format is: { "TaskID": 1 }
@return
   RET_JSON: the format is: { "errno":0, "detail":"" }
*/

// println
//var BUS_JSON = { "UninstallHostName": "susetzb", "UninstallSvcName": "20000" } ;
//var SYS_JSON = { "TaskID": 3 } ;

var RET_JSON = new rollbackNodeResult() ;
var rc       = SDB_OK ;
var errMsg   = "" ;

var task_id   = "" ;
var host_ip   = "" ;
var host_name = "" ;
var host_svc  = "" ;
var FILE_NAME_ROLLBACKSTANDALONE = "rollbackStandalone.js" ;

/* *****************************************************************************
@discretion: init
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _init()
{
   // 1. get task id
   task_id = getTaskID( SYS_JSON ) ;

   // 2. specify the log file name
   try
   {
      host_name = BUS_JSON[UninstallHostName] ;
      host_svc = BUS_JSON[UninstallSvcName] ;
   }
   catch ( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = "Js receive invalid argument" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_ROLLBACKSTANDALONE,
              sprintf( errMsg + ", rc: ?, detail: ?", GETLASTERROR(), GETLASTERRMSG() ) ) ;
      exception_handle( SDB_SYS, errMsg ) ;
   }
   setTaskLogFileName( task_id, host_name ) ;
   
   PD_LOG2( task_id, arguments, PDEVENT, FILE_NAME_ROLLBACKSTANDALONE,
            sprintf( "Begin to rollback standalone[?:?]", host_name, host_svc ) ) ;
}

/* *****************************************************************************
@discretion: final
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _final()
{
   PD_LOG2( task_id, arguments, PDEVENT, FILE_NAME_ROLLBACKSTANDALONE,
            sprintf( "Finish rollbacking standalone[?:?]", host_name, host_svc ) ) ;
}

/* *****************************************************************************
@discretion remove standalone
@parameter
   hostName[string]: remove host name
   svcName[string]: remove service name
   agentPort[string]: the port of sdbcm in target standalone host
@return void
***************************************************************************** */
function _removeStandalone( hostName, svcName, agentPort )
{
   var oma = null ;
   try
   {
      oma = new Oma( hostName, agentPort ) ;
   }
   catch ( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = sprintf( "Failed to connect to OM Agent in host[?:?]", hostName, agentPort );
      PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_ROLLBACKSTANDALONE,
               sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
      exception_handle( rc, errMsg ) ;
   }
   // remove standalone
   try
   {
      oma.removeData( svcName ) ;
      oma.close() ;
   }
   catch ( e ) 
   {
      if ( null != oma && "undefined" != typeof(oma) )
      {
         try
         {
            oma.close() ;
         }
         catch ( e2 )
         {
         }
      }
      rc = GETLASTERROR() ;
      errMsg = sprintf( "Failed to remove standalone[?:?]", hostName, svcName ) ;
      PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_ROLLBACKSTANDALONE,
               sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
      exception_handle( rc, errMsg ) ;
   }
}

function main()
{
   var hostName  = null ;
   var svcName   = null ;
   var agentPort = null ;

   _init() ;
   try
   {
      hostName  = BUS_JSON[UninstallHostName] ;
      svcName   = BUS_JSON[UninstallSvcName] ;
      agentPort = getOMASvcFromCfgFile( hostName ) ;
      // uninstall standalone
      _removeStandalone( hostName, svcName, agentPort ) ;
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = GETLASTERRMSG() ;
      PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_ROLLBACKSTANDALONE,
               sprintf( "Failed to rollback standalone[?:?], rc: ?, detail: ?",
                       hostName, svcName, rc, errMsg ) ) ;
      RET_JSON[Errno] = rc ;
      RET_JSON[Detail] = errMsg ;
   }
   
   _final() ;
println("RET_JSON is: " + JSON.stringify(RET_JSON) ) ;
   return RET_JSON ;
}

// execute
   main() ;

