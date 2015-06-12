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
@description: remove target host from cluster( uninstall the db packet and
              stop sdbcm)
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostName": "rhel64-test8", "IP": "192.168.20.165", "User": "root", "Passwd": "sequoiadb", "InstallPath": "/opt/sequoiadb", "SshPort": "22" }
   SYS_JSON: {}
   ENV_JSON: {}
   OTHER_JSON: {}
@return
   RET_JSON: the format is: { "errno": 0, "detail": "" }
*/

//println
//var BUS_JSON = { "HostName": "rhel64-test8", "IP": "192.168.20.165", "User": "root", "Passwd": "sequoiadb","InstallPath": "/opt/sequoiadb", "SshPort": "22" } ;

var FILE_NAME_REMOVE_HOST = "removeHost.js" ;
var RET_JSON       = new removeHostResult() ;
var rc             = SDB_OK ;
var errMsg         = "" ;

var host_name      = "" ;

/* *****************************************************************************
@discretion: init
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _init()
{
   try
   {
      host_name = BUS_JSON[HostName] ;
   }
   catch ( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = "Js receive invalid argument" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_REMOVE_HOST,
              sprintf( errMsg + ", rc: ?, detail: ?", GETLASTERROR(), GETLASTERRMSG() ) ) ;
      exception_handle( SDB_INVALIDARG, errMsg ) ;
   }
   
   PD_LOG( arguments, PDEVENT, FILE_NAME_REMOVE_HOST,
           sprintf( "Begin to remove host[?]", host_name ) ) ;
}

/* *****************************************************************************
@discretion: final
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _final()
{
   PD_LOG( arguments, PDEVENT, FILE_NAME_REMOVE_HOST,
           sprintf( "Finish removing host[?]", host_name ) ) ;
}

/* *****************************************************************************
@discretion: uninstall sequoiadb packet and stop sdbcm in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   path[string]: the path where the sequoiadb install in
@return void
***************************************************************************** */
function _uninstallPacketInRemote( ssh, path )
{
   var installpath = adaptPath( path ) ;
   var uninstallprog = null ;
   if ( SYS_LINUX == SYS_TYPE )
   {
      uninstallprog = installpath + OMA_PROG_UNINSTALL_L + " --mode " + " unattended " ;
   }
   else
   {
      // TODO: windows
   }
   try
   {
      ssh.exec( uninstallprog ) ;
   }
   catch ( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = "Failed to uninstall sequoiadb" ;
      rc = GETLASTERROR() ;
      PD_LOG( arguments, PDERROR, FILE_NAME_REMOVE_HOST,
              sprintf( errMsg + ", rc: ?, detail: ?", GETLASTERRMSG() ) ) ;
      exception_handle( rc, errMsg ) ;
   }
}

function main()
{
   var ip          = null ;
   var user        = null ;
   var passwd      = null ;
   var sshport     = null ;
   var installPath = null ;
   var ssh         = null ;
   
   _init() ;
   
   try
   {
      // 1. remove arguments
      try
      {
         ip           = BUS_JSON[IP] ;
         RET_JSON[IP] = ip ;
         user         = BUS_JSON[User] ;
         passwd       = BUS_JSON[Passwd] ;
         sshport      = parseInt(BUS_JSON[SshPort]) ;
         installPath  = BUS_JSON[InstallPath] ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = "Js receive invalid argument" ;
         rc = GETLASTERROR() ;
         // record error message in log
         PD_LOG( arguments, PDERROR, FILE_NAME_REMOVE_HOST,
                 errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
         // tell to user error happen
         exception_handle( SDB_INVALIDARG, errMsg ) ;
      }
      // 2. ssh to target host
      try
      {
         ssh = new Ssh( ip, user, passwd, sshport ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = sprintf( "Failed to ssh to host[?]", ip ) ; 
         rc = GETLASTERROR() ;
         PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_REMOVE_HOST,
                  errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
         exception_handle( rc, errMsg ) ;
      }
      // 3. judge whether it's in local host, if so, no need to uninstall
      if ( true == isInLocalHost( ssh ) )
      {
         PD_LOG( arguments, PDEVENT, FILE_NAME_REMOVE_HOST,
                 sprintf( "It's local host[?], not going uninstall", ip ) ) ;
         _final() ;
         return RET_JSON ;
      }
      // 4. uninstall db packet and stop sdbcm in remote host
      _uninstallPacketInRemote( ssh, installPath ) ;
   
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = GETLASTERRMSG() ; 
      rc = GETLASTERROR() ;
      PD_LOG( arguments, PDERROR, FILE_NAME_REMOVE_HOST,
              sprintf( "Failed to remove host[?], rc:?, detail:?",
                       host_name, rc, errMsg ) ) ;
      RET_JSON[Errno] = rc ;
      RET_JSON[Detail] = errMsg ;
   }
   
   _final() ;
println("RET_JSON is: " + JSON.stringify(RET_JSON) ) ;
   // return the result
   return RET_JSON ;
}

// execute
   main() ;

