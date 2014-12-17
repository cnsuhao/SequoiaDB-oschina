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
@description: remove remote host from cluster( remove the db packet and
              stop sdbcm)
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostName": "rhel64-test8", "IP": "192.168.20.165", "User": "root", "Passwd": "sequoiadb", "InstallPath": "/opt/sequoiadb", "SshPort": "22" }
   SYS_JSON: {}
   ENV_JSON: {}
   OTHER_JSON: {}
@return
   RET_JSON: the format is: {}
*/

var RET_JSON       = new Object() ;
var errMsg         = "" ;
/* *****************************************************************************
@discretion: uninstall sequoiadb packet and stop sdbcm in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
   path[string]: the path where the sequoiadb install in
@return void
***************************************************************************** */
function uninstallPacketInRemote( ssh, osInfo, path )
{
   var installpath = adaptPath( osInfo, path ) ;
   var uninstallprog = null ;
   if ( OMA_LINUX == osInfo )
   {
      uninstallprog = installpath + OMA_PROG_UNINSTALL_L + " --mode " + " unattended " ;
   }
   else
   {
      // TODO:
   }
   try
   {
      ssh.exec( uninstallprog ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to uninstall sequoiadb packet" ;
      exception_handle( e, errMsg ) ;
   }
}

function main()
{
   var osInfo      = null ;
   var ssh         = null ;
   var ip          = BUS_JSON[IP] ;
   var user        = BUS_JSON[User] ;
   var passwd      = BUS_JSON[Passwd] ;
   var sshport     = parseInt(BUS_JSON[SshPort]) ;
   var installPath = BUS_JSON[InstallPath] ;
   // get os infomation
   var osInfo = System.type() ;
   // ssh
   var ssh = new Ssh( ip, user, passwd, sshport ) ;
   // judge whether it's in local mathine, if so, no need to uninstall
   var isLocal = isInLocalHost( ssh ) ;
   if ( isLocal )
   {
      return RET_JSON ;
   }
   // uninstall db packet and stop sdbcm in remote host
   uninstallPacketInRemote( ssh, osInfo, installPath ) ;
   // return the result
   return RET_JSON ;
}

// execute
   main() ;

