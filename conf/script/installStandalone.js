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
@description: create standalone
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "InstallHostName": "rhel64-test8", "InstallSvcName": "11820", "InstallPath": "/opt/sequoiadb/database/standalone", "InstallConfig": { "diaglevel": 3, "role": "standalone", "logfilesz": 64, "logfilenum": 20, "transactionon": "false", "preferedinstance": "A", "numpagecleaners": 1, "pagecleaninterval": 10000, "hjbuf": 128, "logbuffsize": 1024, "maxprefpool": 200, "maxreplsync": 10, "numpreload": 0, "sortbuf": 512, "syncstrategy": "none" } }
   SYS_JSON: the format is: { "SdbUser": "sdbadmin", "SdbPasswd": "sdbadmin", "SdbUserGroup": "sdbadmin_group", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" } 
   ENV_JSON:
@return
   RET_JSON: the format is: { "errno":0, "detail":"" }
*/

//var BUS_JSON = { "SdbUser": "sdbadmin", "SdbPasswd": "sdbadmin", "SdbUserGroup": "sdbadmin_group", "User": "root", "Passwd": "sequoiadb", "SshPort": "22", "InstallHostName": "susetzb", "InstallSvcName": "20000", "InstallPath": "/opt/sequoiadb/database/standalone/20000", "InstallConfig": { "diaglevel": "5", "role": "standalone", "logfilesz": "64", "logfilenum": "10", "transactionon": "false", "preferedinstance": "2", "numpagecleaners": "10", "pagecleaninterval": "1000", "hjbuf": "128", "logbuffsize": "1024", "maxprefpool": "200", "maxreplsync": "10", "numpreload": "0", "sortbuf": "512", "syncstrategy": "none" } };

//var SYS_JSON = { "TaskID": 3 };

var RET_JSON = new installNodeResult() ;
var rc       = SDB_OK ;
var errMsg   = "" ;

var task_id  = "" ;
var host_ip  = "" ;
var host_name = "" ;
var host_svc  = "" ;
var FILE_NAME_INSTALL_STANDALONE = "installStandalone.js" ;


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
      host_name = BUS_JSON[InstallHostName] ;
      host_svc  = BUS_JSON[InstallSvcName] ;
   }
   catch ( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = "Js receive invalid argument" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_INSTALL_STANDALONE,
              sprintf( errMsg + ", rc: ?, detail: ?", GETLASTERROR(), GETLASTERRMSG() ) ) ;
      exception_handle( SDB_SYS, errMsg ) ;
   }
   setTaskLogFileName( task_id, host_name ) ;
   
   PD_LOG2( task_id, arguments, PDEVENT, FILE_NAME_INSTALL_STANDALONE,
            sprintf( "Begin to install standalone[?:?]", host_name, host_svc ) ) ;
}

/* *****************************************************************************
@discretion: final
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _final()
{
   PD_LOG2( task_id, arguments, PDEVENT, FILE_NAME_INSTALL_STANDALONE,
            sprintf( "Finish installing standalone[?:?]", host_name, host_svc ) ) ;
}

/* *****************************************************************************
@discretion: create standalone
@parameter
   hostName[string]: install host name
   svcName[string]: install svc name
   installPath[string]: install path
   config[json]: config info 
   agentPort[string]: the port of sdbcm in install host
@return void
***************************************************************************** */
function _createStandalone( hostName, svcName, installPath, config, agentPort )
{
   var oma = null ;
   try
   {
      oma = new Oma( hostName, agentPort ) ;
      oma.createData( svcName, installPath, config ) ;
      oma.startNode( svcName ) ;
      oma.close() ;
      oma = null ;
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
      SYSEXPHANDLE( e ) ;
      errMsg = sprintf( "Failed to install standalone[?:?]", hostName, svcName ) ;
      rc = GETLASTERROR() ;
      PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_INSTALL_STANDALONE,
               sprintf( errMsg + ", rc:?, detail:? ", rc, GETLASTERRMSG() ) ) ;
      exception_handle( rc, errMsg ) ;
   }
}

function main()
{
   var sdbUser         = null ;
   var sdbUserGroup    = null ;
   var user            = null ;
   var passwd          = null ;
   var sshport         = null ;
   var installHostName = null ;
   var installSvcName  = null ;
   var installPath     = null ;
   var installConfig   = null ;
   var ssh             = null ;
   var agentPort       = null ;
   
   _init() ;
   
   try
   {
      // 1. get arguments
      try
      {
         sdbUser         = BUS_JSON[SdbUser] ;
         sdbUserGroup    = BUS_JSON[SdbUserGroup] ;
         user            = BUS_JSON[User] ;
         passwd          = BUS_JSON[Passwd] ;    
         sshport         = parseInt(BUS_JSON[SshPort]) ;
         installHostName = BUS_JSON[InstallHostName] ;
         installSvcName  = BUS_JSON[InstallSvcName] ;
         installPath     = BUS_JSON[InstallPath] ;
         installConfig   = BUS_JSON[InstallConfig] ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = "Js receive invalid argument" ;
         rc = GETLASTERROR() ;
         // record error message in log
         PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_INSTALL_STANDALONE,
                  errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
         // tell to user error happen
         exception_handle( SDB_INVALIDARG, errMsg ) ;
      }
      // 2. ssh to target host
      try
      {
         ssh = new Ssh( installHostName, user, passwd, sshport ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = sprintf( "Failed to ssh to host[?]", installHostName ) ; 
         rc = GETLASTERROR() ;
         PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_INSTALL_STANDALONE,
                  errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
         exception_handle( rc, errMsg ) ;
      }
      // 3. get OM Agent's service in target host from local sdbcm config file
      agentPort = getOMASvcFromCfgFile( installHostName ) ;
      // change install path owner
      changeDirOwner( ssh, installPath, sdbUser, sdbUserGroup ) ;
      // create standalone
      _createStandalone( installHostName, installSvcName,
                         installPath, installConfig, agentPort ) ;
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = GETLASTERRMSG() ; 
      rc = GETLASTERROR() ;
      PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_INSTALL_STANDALONE,
               sprintf( "Failed to install standalone[?:?], rc:?, detail:?",
               host_name, host_svc, rc, errMsg ) ) ;             
/*
      _final() ;
      exception_handle( rc, errMsg ) ;
*/
      RET_JSON[Errno] = rc ;
      RET_JSON[Detail] = errMsg ;
   }
   
   _final() ;
   return RET_JSON ;
}

// execute
   main() ;
