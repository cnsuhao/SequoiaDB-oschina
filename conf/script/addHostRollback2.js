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
@description: remove the installed db packet
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostInfo": { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sequoiadb", "SshPort": "22", "AgentPort": "11790", "InstallPath": "/opt/sequoiadb" } }
   SYS_JSON: {}
   ENV_JSON: {}
   OTHER_JSON: {}
@return
   RET_JSON: the format is: {"errno":0,"detail":"","IP":"192.168.20.165","HasUninstall":true}
*/

var RET_JSON       = new addHostRollbackResult() ;
var errMsg         = "" ;

/* *****************************************************************************
@discretion: uninstall db packet in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
   path[string]: the path db installed in
@return void
***************************************************************************** */
function uninstallDBPacket ( ssh, osInfo, path )
{
   var cmd = "" ;
   var path = adaptPath( osInfo, path ) ;
   if ( OMA_LINUX == osInfo )
   {
      // try to stop sdbcm
      try
      {
         cmd = path + OMA_PROG_BIN_SDBCMTOP_L ;
         ssh.exec( cmd ) ;
      }
      catch ( e )
      {
      }
      // remove db packet
      try
      {
         cmd = path + OMA_PROG_UNINSTALL_L ;
         ssh.exec( "chmod a+x " + cmd ) ;
         ssh.exec( cmd + " --mode unattended " ) ;
      }
      catch ( e )
      {
         errMsg = "Failed to uninstall db packet in host[" + ssh.getPeerIP() + "]" ;
         exception_handle( e, errMsg ) ;
      }
   }
   else
   {
      // DOTO: tanzhaobo
      // windows
   }
}

function main()
{
   var osInfo       = null ;
   var ssh          = null ;
   var obj          = null ;
   var ip           = null ;
   var user         = null ;
   var passwd       = null ;
   var sshport      = null ;
   var installPath  = null ;

   try
   {
      // get os information
      try
      {
         osInfo = System.type() ;
      }
      catch ( e )
      {
         errMsg = "Failed to get os info" ;
         exception_handle( e, errMsg ) ;
      }
      // get remove info
      try
      {
         obj           = BUS_JSON[HostInfo] ;
         ip            = obj[IP] ;
         user          = obj[User] ;
         passwd        = obj[Passwd] ;
         sshport       = parseInt(obj[SshPort]) ;
         installPath   = obj[InstallPath] ; 
         RET_JSON[IP]  = ip ;
      }
      catch ( e )
      {
         errMsg = "Failed to get info to rollback added host" ;
         exception_handle( e, errMsg ) ;
      }
      // ssh
      try
      {
         ssh = new Ssh( ip, user, passwd, sshport ) ;
      }
      catch ( e )
      {
         errMsg = "Failed to ssh to host[" + ip + "]" ;
         exception_handle( e, errMsg ) ;
      }
      // judge whether it's in local host, if so, no need to uninstall
      var isLocal = isInLocalHost( ssh ) ;
      if ( isLocal )
      {
         RET_JSON[HasUninstall] = true ;
         return RET_JSON ; 
      }
      // uninstall business packet from remote host
      uninstallDBPacket( ssh, osInfo, installPath ) ;
      RET_JSON[HasUninstall] = true ;
   }
   catch ( e )
   {
      RET_JSON[Errno]  = getLastError() ;
      RET_JSON[Detail] = getLastErrMsg() ;
   }

   // return the result
   return RET_JSON ;
}

// execute
   main() ;

