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
   BUS_JSON: the format is: {"SdbUser":"sdbadmin","SdbPasswd":"sdbadmin","SdbUserGroup":"sdbadmin_group","InstallPacket":"/home/users/tanzhaobo/sequoiadb/bin/../packet/sequoiadb-1.8-linux_x86_64-installer.run","HostInfo":[{"IP":"192.168.20.42","HostName":"susetzb","User":"root","Passwd":"sequoiadb","SshPort":"22","AgentPort":"11790","InstallPath":"/opt/sequoiadb"},{"IP":"192.168.20.165","HostName":"rhel64-test8","User":"root","Passwd":"sequoiadb","SshPort":"22","AgentPort":"11790","InstallPath":"/opt/sequoiadb"},{"IP":"192.168.20.166","HostName":"rhel64-test9","User":"root","Passwd":"sequoiadb","SshPort":"22","AgentPort":"11790","InstallPath":"/opt/sequoiadb"}]}
   SYS_JSON: {}
   ENV_JSON: {}
   OTHER_JSON: {}
@return
   RET_JSON: the format is: {"errno":0,"detail":"","HostInfo":[{"errno":0,"detail":"","IP":"192.168.20.42","HasInstall":true},{"errno":0,"detail":"","IP":"192.168.20.165","HasInstall":true}]}
*/

var RET_JSON       = new Object() ;
RET_JSON[Errno]    = SDB_OK ;
RET_JSON[Detail]   = "" ;
RET_JSON[HostInfo] = [] ;
var errMsg         = "" ;

/* *****************************************************************************
@discretion: check when install infos include installing in local, whether these
             infos match local installed db's infos or not
@author: Tanzhaobo
@parameter
   osInfo[string]: os type
@return
   [bool]: true or false
***************************************************************************** */
function isMatchLocalInfo( osInfo )
{
   var infoArr          = null ;
   var arrLen           = null ;
   var localInstallInfo = null ;
   var adminUser        = null ;
   var installPath      = null ;
   var localAgentPort   = null ;
   var localIP          = getLocalIP() ;

   // get local install info
   try
   {
      localInstallInfo = eval( '(' + Oma.getOmaInstallInfo() + ')' ) ;
   }
   catch ( e )
   {
      // when no install info in /etc/default/sequoiadb, think it to be false
      errMsg = "Failed to get localhost[" + localIP + "] db install info" ;
      exception_handle( e, errMsg ) ;
   }
   if ( null == localInstallInfo || "undefined" == typeof( localInstallInfo ) )
   {
      errMsg = "Failed to get localhost[" + localIP + "] db install info" ;
      exception_handle( e, errMsg ) ;
   }
   adminUser      = localInstallInfo[SDBADMIN_USER] ;
   installPath    = localInstallInfo[INSTALL_DIR] ;
   // check wether BUS_JSON include info installing in local host
   // if so, get them for compare
   infoArr       = BUS_JSON[HostInfo] ;
   arrLen        = infoArr.length ;
   var sdbUser   = BUS_JSON[SdbUser] ;

   for( var i = 0; i < arrLen; i++ )
   {
      var obj = infoArr[i] ;
      var ip = obj[IP] ;

      if ( localIP == ip )
      {
         var ssh     = null ;
         var user    = obj[User] ;
         var passwd  = obj[Passwd] ; 
         var path    = obj[InstallPath] ;
         var port    = obj[AgentPort] ;
         var sshport = parseInt(obj[SshPort]) ;
         // get local agent port
         try
         {
            ssh = new Ssh( ip, user, passwd, sshport ) ;
         }
         catch ( e )
         {
            errMsg = "Failed to ssh to localhost[" + ip + "]" ;
            exception_handle( e, errMsg ) ;
         }
         // firstly, check sdb user
         if ( adminUser != sdbUser )
         {
            errMsg = "When install db packet in localhost, sdb admin user[" + sdbUser  + "] does not match current one[" + adminUser + "]" ;
            setLastErrMsg( errMsg ) ;
            return false ;
         }
         // secondly, check agent port
         localAgentPort = getSdbcmPort( ssh, osInfo ) ;
         if ( localAgentPort != port )
         {
            errMsg = "When install db packet in localhost, agent port[" + port  + "] does not match current one[" + localAgentPort  + "]" ;
            setLastErrMsg( errMsg ) ;
            return false ;
         }
         // thirdly, check install path
         var path1 = adaptPath(osInfo, installPath) ;
         var path2 = adaptPath(osInfo, path) ;
         if ( path1 != path2 )
         {
            errMsg = "When install db packet in localhost, install path[" + path  + "] does not match current one[" + installPath  + "]" ;
            setLastErrMsg( errMsg ) ;
            return false ;
         }
      }
   }
 
   return true ;
}

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
         cmd = "mkdir -p " + OMA_PATH_TMP_COORD_PATH ;
         ssh.exec( cmd ) ;
         // chmod
         // TODO: 
         cmd = "chmod -R 777 " + OMA_PATH_TEMP_OMA_DIR_L ;
         ssh.exec( cmd ) ;
      }
      catch( e )
      {
         errMsg = "Failed to create tmp director in check host" ;
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
   var infoArr         = null ;
   var arrLen          = null ;
   var ssh             = null ;
   var osInfo          = null ;

   try
   {
      sdbUser          = BUS_JSON[SdbUser] ;
      sdbPasswd        = BUS_JSON[SdbPasswd] ;
      sdbUserGroup     = BUS_JSON[SdbUserGroup] ;
      installPacket    = BUS_JSON[InstallPacket] ;
      infoArr          = BUS_JSON[HostInfo] ;
      arrLen           = infoArr.length ;
   }
   catch ( e )
   {
      RET_JSON[Errno] = SDB_INVALIDARG ;
      RET_JSON[Detail] = "Failed to get field: " + e ;
      return RET_JSON ;

   }
   // check
   if ( arrLen == 0 )
   {
      RET_JSON[Errno] = SDB_INVALIDARG ;
      RET_JSON[Detail] = "Not specified any hosts to add" ;
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
   // check install info
   var isMatch = null ;
   try
   {
      isMatch = isMatchLocalInfo( osInfo ) ;
      if ( !isMatch )
      {
         RET_JSON[Errno] = SDB_INVALIDARG ;
         RET_JSON[Detail] = getLastErrMsg() ;
         return RET_JSON ;
      }
   }
   catch ( e )
   {
      RET_JSON[Errno] = getLastError() ;
      RET_JSON[Detail] = getLastErrMsg() ;
      return RET_JSON ;
   }

   // add host
   for ( var i = 0; i < arrLen; i++ )
   {
      var ssh          = null ;
      var obj          = null ;
      var ip           = null ;
      var user         = null ;
      var passwd       = null ;
      var sshPort      = null ;
      var agentPort    = null ;
      var installPath  = null ;
      var retObj       = null ;

      try
      {
         retObj        = new addHostResult() ;
         obj           = infoArr[i] ;
         ip            = obj[IP] ;
         retObj[IP]    = obj[IP] ;
         user          = obj[User] ;
         passwd        = obj[Passwd] ;
         sshPort       = parseInt(obj[SshPort]) ;
         agentPort     = obj[AgentPort] ;
         installPath   = obj[InstallPath] ;

         // ssh
         var ssh = new Ssh( ip, user, passwd, sshPort ) ;
         // judge whether it's in local host, if so, no need to install
         var isLocal = isInLocalHost( ssh ) ;
         if ( isLocal )
         {
            retObj[HasInstall] = true ;
            RET_JSON[HostInfo].push( retObj ) ;
            continue ;
         }
         // push packet to remote host
         pushInstallPacket( ssh, osInfo, installPacket ) ;
         installDBPacket( ssh, osInfo, sdbUser, sdbPasswd, installPacket, installPath ) ;
         retObj[HasInstall] = true ;
      }
      catch ( e )
      {
         if ( "number" == typeof(e) && e < 0 )
         {
            retObj[Errno] = e ;
            retObj[Detail] = GETLASTERRMSG() ;
            RET_JSON[HostInfo].push( retObj ) ;
            RET_JSON[Errno] = e ;
            RET_JSON[Detail] = "Failed to add host[" + ip + "]: " + retObj[Detail] ;
            break ;
         }
         else
         {
            retObj[Errno] = SDB_SYS ;
            retObj[Detail] = GETLASTERRMSG() ;
            RET_JSON[HostInfo].push( retObj ) ;
            RET_JSON[Errno] = SDB_SYS ;
            RET_JSON[Detail] = "Failed to add host[" + ip + "]: " + e ;
            break ;
         }
      }
      RET_JSON[HostInfo].push( retObj ) ;
   }
   // return the result
   return RET_JSON ;
}

// execute
   main() ;

