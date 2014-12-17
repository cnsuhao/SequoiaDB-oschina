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
   BUS_JSON: the format is: { "HostInfo": [ { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sequoiadb", "SshPort": "22", "AgentPort": "11790", "InstallPath": "/opt/sequoiadb" } ] }
   SYS_JSON: {}
   ENV_JSON: {}
   OTHER_JSON: {}
@return
   RET_JSON: the format is: {"HostInfo":[{"errno":0,"detail":"","IP":"192.168.20.165","HasUninstall":true}]}
*/

var RET_JSON       = new Object() ;
RET_JSON[HostInfo] = [] ;
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
      cmd = path + OMA_PROG_UNINSTALL_L ;
      try
      {
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
   var infoArr = BUS_JSON[HostInfo] ;
   var arrLen = infoArr.length ;
   if ( arrLen == 0 )
   {
      return RET_JSON ;
   }
   // get os information
   var osInfo = System.type() ;
   for ( var i = 0; i < arrLen; i++ )
   {
      var ssh          = null ;
      var obj          = infoArr[i]
      var ip           = obj[IP] ;
      var user         = obj[User] ;
      var passwd       = obj[Passwd] ;
      var sshport      = parseInt(obj[SshPort]) ;
      var installPath  = obj[InstallPath] ; 
      var retObj       = new addHostRollbackResult() ;
      retObj[IP]       = ip ;
      try
      {
         // ssh
         var ssh = new Ssh( ip, user, passwd, sshport ) ;
         // judge whether it's in local host, if so, no need to uninstall
         var isLocal = isInLocalHost( ssh ) ;
         if ( isLocal )
         {
            retObj[HasUninstall] = true ;
            RET_JSON[HostInfo].push( retObj ) ;
            continue ;
         }
         // uninstall business packet from remote host
         uninstallDBPacket( ssh, osInfo, installPath ) ;
         retObj[HasUninstall] = true ;
      }
      catch ( e )
      {
         retObj[Errno]  = GETLASTERROR( e, false ) ;
         retObj[Detail] = GETLASTERRMSG() ;
      }
      RET_JSON[HostInfo].push( retObj ) ;
   }
   // return the result
   return RET_JSON ;
}

// execute
   main() ;

