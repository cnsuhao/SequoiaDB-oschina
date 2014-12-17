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
@description: add host to cluster( install db patcket and start sdbcm )
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: {"SdbUser":"sdbadmin","SdbPasswd":"sdbadmin","SdbUserGroup":"sdbadmin_group","InstallPacket":"/home/users/tanzhaobo/sequoiadb/bin/../packet/sequoiadb-1.10-linux_x86_64-installer.run","HostInfo":{"IP":"192.168.20.166","HostName":"rhel64-test9","User":"root","Passwd":"sequoiadb","SshPort":"22","AgentPort":"11790","InstallPath":"/opt/sequoiadb"}}
   SYS_JSON: {}
   ENV_JSON: {}
   OTHER_JSON: {}
@return
   RET_JSON: the format is: {"errno":0,"detail":"","IP":"192.168.20.166","HasInstall":true}
*/

var RET_JSON       = new addHostResult() ;
var errMsg         = "" ;

/* *****************************************************************************
@discretion: get the name of install packet
@author: Tanzhaobo
@parameter
   osInfo[string]: os type
   packet[string]: the full name of the packet,
                   e.g. /tmp/packet/sequoiadb-1.8-linux_x86_64-installer.run
@return
   packetname[string]: the name of the install packet
***************************************************************************** */
function getInstallPacketName( osInfo, packet )
{
   var s = "" ;
   var i = 1 ;
   var packetname = "" ;
   if ( OMA_LINUX == osInfo )
   {
      s = "/" ;
      i = packet.lastIndexOf( s ) ;
      if ( -1 != i )
      {
         packetname = packet.substring( i+1 ) ;
      }
      else
      {
         packetname = packet ;
      }
   }
   else
   {
      // TODO:
   }
   return packetname ;
}

/* *****************************************************************************
@discretion: create tmp diretory in /tmp
@author: Tanzhaobo
@parameter
   ssh[object]: Ssh object
   osInfo[string]: os type
@return void
***************************************************************************** */
function createTmpDir( ssh, osInfo )
{
   var cmd = "" ;
   if ( OMA_LINUX == osInfo )
   {
      try
      {
         // mkdir /tmp/omatmp
         cmd = "mkdir -p " + OMA_PATH_TEMP_OMA_DIR_L ;
         ssh.exec( cmd ) ;
         // mkdir  /tmp/omatmp/packet
         cmd = "mkdir -p " + OMA_PATH_TEMP_PACKET_DIR_L ; 
         ssh.exec( cmd ) ;
         // mkdir  /tmp/omatmp/data/vCoord
         cmd = "mkdir -p " + OMA_PATH_VCOORD_PATH_L ;
         ssh.exec( cmd ) ;
         // chmod
         // TODO: 
         cmd = "chmod -R 777 " + OMA_PATH_TEMP_OMA_DIR_L ;
         ssh.exec( cmd ) ;
      }
      catch( e )
      {
         errMsg = "Failed to create tmp director in add host[" + ssh.getPeerIP() + "]"  ;
         exception_handle( e, errMsg ) ;
      }
   }
   else
   {
      // DOTO: tanzhaobo
      // windows
   }
}

/* *****************************************************************************
@discretion: push install packet to remote host
@author: Tanzhaobo
@parameter
   ssh[object]: Ssh object
   osInfo[string]: os type
   packet[string]: the full name of the packet,
                   e.g. /tmp/packet/sequoiadb-1.8-linux_x86_64-installer.run
@return void
***************************************************************************** */
function pushInstallPacket( ssh, osInfo, packet )
{
   var src = "" ;
   var dest = "" ;
   var packetName = getInstallPacketName( osInfo, packet ) ;
   createTmpDir( ssh, osInfo ) ;
   if ( OMA_LINUX == osInfo )
   {
      try
      {
         // installer.run
         src = packet;
         dest = OMA_PATH_TEMP_PACKET_DIR_L + packetName ;
         ssh.push( src, dest ) ;
         var cmd = "chmod a+x " + OMA_PATH_TEMP_PACKET_DIR_L + packetName ;
         ssh.exec( cmd ) ;
      }
      catch ( e )
      {
         errMsg = "Failed to push db packet to host [" + ssh.getPeerIP() + "]" ;
         exception_handle( e, errMsg ) ;
      }
   }
   else
   {
      // TODO: tanzhaobo
      // push packet in windows
   }
}

/* *****************************************************************************
@discretion: uninstall db packet in remote host when install failed
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
      }
   }
   else
   {
      // DOTO: tanzhaobo
      // windows
   }
}


/* *****************************************************************************
@discretion: push install packet to remote host
@author: Tanzhaobo
@parameter
   ssh[object]: Ssh object
   osInfo[string]: os type
   sdbuser[string]: the user to be add for running sequoiadb program
   sdbpasswd[string]: the password of sdbuser
   packet[string]: the full name of the packet,
                   e.g. /tmp/packet/sequoiadb-1.8-linux_x86_64-installer.run
   path[string]: the path where the install packet is in local host, we need 
                 to push this packet to remote host
@return void
***************************************************************************** */
function installDBPacket( ssh, osInfo, sdbuser, sdbpasswd, packet, path )
{
   var cmd = "" ;
   var option = "" ;
   option += " --mode unattended " + " --prefix " + path ;
   option += " --username " + sdbuser + " --userpasswd " + sdbpasswd ;
   var packetName = getInstallPacketName( osInfo, packet ) ; 
   if ( OMA_LINUX == osInfo )
   {
      cmd = OMA_PATH_TEMP_PACKET_DIR_L + packetName + option ;
      try
      {
         ssh.exec( cmd ) ; 
      }
      catch ( e )
      {
         errMsg = "Failed to insall db packet in host [" + ssh.getPeerIP() + "]" ;
         exception_handle( e, errMsg ) ;
      }
   }
   else
   {
      // TODO: tanzhaobo
      // execute in windows
   }
}

function main()
{
   var sdbUser         = null ;
   var sdbPasswd       = null ;
   var sdbUserGroup    = null ;
   var installPacket   = null ;
   var hostInfo        = null ;

   var ip              = null ;
   var user            = null ;
   var passwd          = null ;
   var sshPort         = null ;
   var agentPort       = null ;
   var installPath     = null ;

   var ssh             = null ;
   var osInfo          = null ;

   try
   {
      sdbUser          = BUS_JSON[SdbUser] ;
      sdbPasswd        = BUS_JSON[SdbPasswd] ;
      sdbUserGroup     = BUS_JSON[SdbUserGroup] ;
      installPacket    = BUS_JSON[InstallPacket] ;
      hostInfo         = BUS_JSON[HostInfo] ;
  
      ip               = hostInfo[IP] ;
      RET_JSON[IP]     = ip ;
      user             = hostInfo[User] ;
      passwd           = hostInfo[Passwd] ;
      sshPort          = parseInt(hostInfo[SshPort]) ;
      agentPort        = hostInfo[AgentPort] ;
      installPath      = hostInfo[InstallPath] ;
   }
   catch ( e )
   {
      RET_JSON[Errno] = SDB_INVALIDARG ;
      RET_JSON[Detail] = "Failed to get field: " + e ;
      return RET_JSON ;
   }
   // get os information
   try
   {
      osInfo = System.type() ;
   }
   catch ( e )
   {
      RET_JSON[Errno] = SDB_INVALIDARG ;
      RET_JSON[Detail] = "Failed to get os info: " + e ;
      return RET_JSON ;
   }
   // add host
   try
   {
      // ssh
      var ssh = new Ssh( ip, user, passwd, sshPort ) ;
      // judge whether it's in local host, if so, no need to install
      var isLocal = isInLocalHost( ssh ) ;
      if ( isLocal )
      {
         RET_JSON[HasInstall] = true ;
         return RET_JSON ;
      }
      // push packet to remote host
      pushInstallPacket( ssh, osInfo, installPacket ) ;
      // install db packet
      try
      {
         installDBPacket( ssh, osInfo, sdbUser, sdbPasswd, installPacket, installPath ) ;
      }
      catch ( e )
      {
         // save the real error
         errMsg = getLastErrMsg() ;
         var errno = null ;
         if ( "number" == typeof(e) && e < 0 )
            errno = e ;
         else
            errno = SDB_SYS ;   
         // when install failed try to remove the packet
         uninstallDBPacket( ssh, osInfo, installPath ) ;
         // recover real error
         e = errno ;
         setLastError( e ) ;
         setLastErrMsg( errMsg ) ;
         throw e ;
      }
      RET_JSON[HasInstall] = true ;
   }
   catch ( e )
   {
      if ( "number" == typeof(e) && e < 0 )
      {
         RET_JSON[Errno] = e ;
         RET_JSON[Detail] = getLastErrMsg() ;
      }
      else
      {
         RET_JSON[Errno] = SDB_SYS ;
         RET_JSON[Detail] = "Failed to add host[" + ip + "]: " + e ;
      }
   }

   // return the result
   return RET_JSON ;
}

// execute
   main() ;

