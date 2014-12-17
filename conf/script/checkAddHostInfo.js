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
   RET_JSON: the format is: {"errno":0,"detail":""}
*/

var RET_JSON       = new checkAddHostInfoResult() ;
var errMsg         = "" ;

/* *****************************************************************************
@discretion: check when install informations include installing db packet in
             local, whether these informations match local installed db's
             informations or not
@author: Tanzhaobo
@parameter
   osInfo[string]: os type
@return
   [bool]: true or false
***************************************************************************** */
function checkAddHostInfo( osInfo )
{
   var infoArr          = null ;
   var arrLen           = null ;
   var localInstallInfo = null ;
   var adminUser        = null ;
   var installPath      = null ;
   var localAgentPort   = null ;
   var localIP          = getLocalIPAddr() ;

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

   if ( arrLen == 0 )
   {
      errMsg = "Not specified any hosts to add" ;
      setLastErrMsg( errMsg ) ;
      setLastError( SDB_INVALIDARG ) ;
      return false ;
   }
   
   for( var i = 0; i < arrLen; i++ )
   {
      var obj = infoArr[i] ;
      var ip = obj[IP] ;
      if ( localIP == ip )
      {
         var ssh       = null ;
         var sdbUser   = BUS_JSON[SdbUser] ;
         var sdbPasswd = BUS_JSON[SdbPasswd] ;
         var user      = obj[User] ;
         var passwd    = obj[Passwd] ; 
         var path      = obj[InstallPath] ;
         var port      = obj[AgentPort] ;
         var sshport   = parseInt(obj[SshPort]) ;

         // ssh to local root user
         try
         {
            ssh = new Ssh( ip, user, passwd, sshport ) ;
         }
         catch ( e )
         {
            errMsg = "Failed to ssh to localhost[" + ip + "]" ;
            exception_handle( e, errMsg ) ;
         }
         // firstly, check sdb user and password
         if ( adminUser != sdbUser )
         {
            errMsg = "When installing db packet in localhost[" + localIP + "], sdb admin user[" + sdbUser  + "] needs to match current one[" + adminUser + "]" ;
            setLastErrMsg( errMsg ) ;
            return false ;
         }
         else
         {
            try
            {
               // ssh to local sdb user
               var ssh2 = new Ssh( ip, sdbUser, sdbPasswd, sshport ) ;
            }
            catch ( e )
            {
               errMsg = "When installing db packet in localhost[" + localIP + "], sdb admin password needs to match current one" ;
               setLastErrMsg( errMsg ) ;
               return false ;
            }
         }
         // secondly, check agent port
         localAgentPort = getSdbcmPort( ssh, osInfo ) ;
         if ( localAgentPort != port )
         {
            errMsg = "When installing db packet in localhost[" + localIP + "], agent port[" + port  + "] needs to match current one[" + localAgentPort  + "]" ;
            setLastErrMsg( errMsg ) ;
            return false ;
         }
         // thirdly, check install path
         var path1 = adaptPath(osInfo, installPath) ;
         var path2 = adaptPath(osInfo, path) ;
         if ( path1 != path2 )
         {
            errMsg = "When installing db packet in localhost[" + localIP + "], install path[" + path  + "] needs to match current one[" + installPath  + "]" ;
            setLastErrMsg( errMsg ) ;
            return false ;
         }
      }
   }
 
   return true ;
}

function main()
{
   // check install info
   try
   {
      var osInfo = System.type() ;
      var flag = checkAddHostInfo( osInfo ) ;
      if ( !flag )
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
   }
   return RET_JSON ;
}

// execute
   main() ;

