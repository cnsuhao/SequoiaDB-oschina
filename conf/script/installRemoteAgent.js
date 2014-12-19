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
@description: install SequoiaDB Cluster Manager(sdbcm) in remote host
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostInfo": [ { "IP": "192.168.20.42", "HostName": "susetzb", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }, { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }, { "IP": "192.168.20.166", "HostName": "rhel64-test9", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" } ] } ;
   SYS_JSON: the system info: { "ProgPath": "/opt/sequoiadb/bin/" } ;
   ENV_JSON:
@return
   RET_JSON: the install result: { "HostInfo": [ { "errno": 0, "detail": "", "IsNeedUninstall": true, "AgentPort": "10001", "IP": "192.168.20.165" }, { "errno": 0, "detail": "", "IsNeedUninstall": true, "AgentPort": "10001", "IP": "192.168.20.166" } ] }
*/

var RET_JSON       = new Object() ;
RET_JSON[HostInfo] = [] ;
var errMsg         = "" ;

var LOCAL_PROG_PATH = SYS_JSON[ProgPath] ;
var LOCAL_CONF_PATH = Oma.getOmaConfigFile() ;
var LOCAL_SPT_PATH  = getScriptPath( LOCAL_CONF_PATH ) ;

/* *****************************************************************************
@discretion: create temp directory in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
@return void
***************************************************************************** */
function createTmpDir( ssh, osInfo )
{
   var cmd = "" ;
   if ( "LINUX" == osInfo )
   {
     // rm /tmp/omatmp
     cmd = "rm " + OMA_PATH_TEMP_OMA_DIR_L + " -rf " ;
     ssh.exec( cmd ) ;
     // mkdir /tmp/omatmp
     cmd = "mkdir " + OMA_PATH_TEMP_OMA_DIR_L ;
     ssh.exec( cmd ) ;
     // mkdir /tmp/omatmp/bin
     cmd = "mkdir " + OMA_PATH_TEMP_BIN_DIR_L ;
     ssh.exec( cmd ) ;
     // mkdir /tmp/omatmp/conf
     cmd = "mkdir " + OMA_PATH_TEMP_CONF_DIR_L ;
     ssh.exec( cmd ) ;
     // mkdir /tmp/omatmp/conf/log
     cmd = "mkdir " + OMA_PATH_TEMP_LOG_DIR_L ;
     ssh.exec( cmd ) ;
     // mkdir /tmp/omatmp/conf/script
     cmd = "mkdir " + OMA_PATH_TEMP_SPT_DIR_L ;
     ssh.exec( cmd ) ;
     // mkdir /tmp/omatmp/temp
     cmd = "mkdir " + OMA_PATH_TEMP_TEMP_DIR_L ;
     ssh.exec( cmd ) ;
     //  mkdir /tmp/omatmp/data/vCoord
     cmd = "mkdir -p " + OMA_PATH_VCOORD_PATH_L ;
     ssh.exec( cmd ) ;
     // change mode
     cmd = "chmod -R 777 " + OMA_PATH_TEMP_OMA_DIR_L ;
     ssh.exec( cmd ) ;
   }
   else
   {
      // TODO: tanzhaobo
      // create dir in windows
   }
}

/* *****************************************************************************
@discretion: push sdblist pachet and some js script to remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
@return void
***************************************************************************** */
function pushPacket1( ssh, osInfo )
{
   var src = "" ;
   var dest = "" ;
   if ( OMA_LINUX == osInfo )
   {
      // sdblist
      src = LOCAL_PROG_PATH + OMA_PROG_SDBLIST_L ;
      dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBLIST_L ;
      ssh.push( src, dest ) ;
      // sdbcmtop
      src = LOCAL_PROG_PATH + OMA_PROG_SDBCMTOP_L ;
      dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCMTOP_L ;
      ssh.push( src, dest ) ;
      // script error.js
      src = LOCAL_SPT_PATH + OMA_FILE_ERROR ;
      dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_ERROR ;
      ssh.push( src, dest ) ;
      // script common.js
      src = LOCAL_SPT_PATH + OMA_FILE_COMMON ;
      dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_COMMON ;
      ssh.push( src, dest ) ;
      // script define.js
      src = LOCAL_SPT_PATH + OMA_FILE_DEFINE ;
      dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_DEFINE ;
      ssh.push( src, dest ) ;
      // script func.js
      src = LOCAL_SPT_PATH + OMA_FILE_FUNC ;
      dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_FUNC ;
      ssh.push( src, dest ) ;
      // script checkHostItem.js
      src = LOCAL_SPT_PATH + OMA_FILE_CHECK_HOST_ITEM ;
      dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_CHECK_HOST_ITEM ;
      ssh.push( src, dest ) ;
      // script checkHost.js
      src = LOCAL_SPT_PATH + OMA_FILE_CHECK_HOST ;
      dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_CHECK_HOST ;
      ssh.push( src, dest ) ;
   }
   else
   {
      // TODO:
   }
}

/* *****************************************************************************
@discretion: push other pachet to remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
@return void
***************************************************************************** */
function pushPacket2( ssh, osInfo )
{
   var src = "" ;
   var dest = "" ;
   if ( OMA_LINUX == osInfo )
   {
      // sdbcm
      src = LOCAL_PROG_PATH + OMA_PROG_SDBCM_L;
      dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCM_L ;
      ssh.push( src, dest ) ;
      // sdbcmd
      src = LOCAL_PROG_PATH + OMA_PROG_SDBCMD_L;
      dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCMD_L ;
      ssh.push( src, dest ) ;
      // sdbcmart
      src = LOCAL_PROG_PATH + OMA_PROG_SDBCMART_L ;
      dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCMART_L ;
      ssh.push( src, dest ) ;
   }
   else
   {
      // TODO: tanzhaobo
      // push program in windows
   }
}

/* *****************************************************************************
@discretion: start sdbcm program in remote or local host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
   port[string]: port for remote sdbcm
@return void
***************************************************************************** */
function startRemoteSdbcm( ssh, osInfo, port )
{
   var cmd = "" ;
   if ( OMA_LINUX == osInfo )
   {
      cmd += OMA_PATH_TEMP_BIN_DIR_L ;
      cmd += OMA_PROG_SDBCMART_L ;
      cmd += " " + OMA_OPTION_SDBCMART_I ;
      cmd += " " + OMA_OPTION_SDBCMART_PORT ;
      cmd += " " + port ;
      try
      {
         ssh.exec( cmd ) ;
      }
      catch ( e )
      {
         errMsg = "Failed to start sdbcm in host[" + ssh.getPeerIP() + "]" ;
         exception_handle( e, errMsg ) ;
      }
      // wait util sdbcm start in target host
      var times = 0 ;
      for ( ; times < OMA_TRY_TIMES; times++ )
      {
         var isRunning = isSdbcmRunning ( ssh, osInfo ) ;
         if ( isRunning )
         {
            break ;
         }
         else
         {
            sleep( OMA_SLEEP_TIME ) ;
         }
      }
      if ( OMA_TRY_TIMES <= times )
      {
         setLastErrMsg( "Time out, sdbcm does not start successfully in host[" + ssh.getPeerIP() + "]" ) ;
         setLastError( SDB_SYS ) ;
         throw SDB_SYS ;
      }
   }
   else
   {
      // TODO:
   }
}

/* *****************************************************************************
@discretion: intall sdbcm program in remote(local) host
@author: Tanzhaobo
@parameter
   ssh[object]: the ssh object
   osInfo[string]: the system type
   ip[string]: the ip address
@note
   either ip or hostname must be specified
@return
   retObj[string]: the result of install
***************************************************************************** */
function installRemoteAgent( ssh, osInfo, ip )
{
   var retObj                 = new Object() ;
   retObj[Errno]              = SDB_OK ;
   retObj[Detail]             = "" ;
   retObj[IsNeedUninstall]    = false ;
   retObj[AgentPort]          = OMA_PORT_INVALID + "";
   retObj[IP]                 = ip ;

   // test whether sdbcm has been installed in local,
   // if so, no need to push packet 
   var flag = isInLocalHost( ssh ) ;
   if ( flag )
   {
      // if sdbcm running in local, get the agent port by sdblist
      flag = isSdbcmRunning( ssh, osInfo ) ;
      if ( flag )
      {
         retObj[AgentPort] = "" + getSdbcmPort( ssh, osInfo ) ;
      }
      else // else, set error
      {
         setLastErrMsg( "sdbcm is not running in localhost[" + ssh.getPeerIP() + "]" ) ;
         setLastError( SDB_SYS ) ;
         throw SDB_SYS ;
      }
      return retObj ;
   }

   // build directory in remote mechine
   createTmpDir( ssh, osInfo ) ;
   // push sdblist packet to remote
   pushPacket1( ssh, osInfo ) ;
   // push other packet to remote
   pushPacket2( ssh, osInfo ) ; 
   // check wether sdbcm is running in target host
   flag = isSdbcmRunning( ssh, osInfo ) ;
   if ( flag )
   {
      retObj[AgentPort] = "" + getSdbcmPort( ssh, osInfo ) ;
      removeTmpDir( ssh, osInfo ) ;
      return retObj ;
   }
   // get a port in remote machine for installing sdbcm in target host
   var port = getAUsablePortFromRemote( ssh, osInfo ) ;
   if ( OMA_PORT_INVALID == port )
   {
      setLastErrMsg( "Failed to get a usable port in host[" + ssh.getPeerIP() + "]" ) ;
      setLastError( SDB_SYS ) ;
      throw SDB_SYS ;
   }
   // start the sdbcm program in remote
   startRemoteSdbcm( ssh, osInfo, port ) ;

   retObj[AgentPort] = port + "" ;
   retObj[IsNeedUninstall] = true ;
   return retObj ;
}

function main()
{
   var infoArr = BUS_JSON[HostInfo] ;
   var arrLen = infoArr.length ;
   if ( arrLen == 0 )
   {
      setLastErrMsg( "Not specified any host to install" ) ;
      setLastError( SDB_INVALIDARG ) ;
      throw SDB_INVALIDARG ;
   }
   // get os information
   var osInfo = System.type() ;
   for( var i = 0; i < arrLen; i++ )
   {
      var ssh      = null ;
      var obj      = infoArr[i] ;
      var user     = obj[User] ;
      var passwd   = obj[Passwd] ;
      var ip       = obj[IP] ;
      var sshport  = parseInt(obj[SshPort]) ;
      var ret      = new installTmpCMResult() ;
      ret[IP]      = ip ;
      try
      {
         ssh = new Ssh( ip, user, passwd, sshport ) ;
         // install
         ret = installRemoteAgent( ssh, osInfo, ip ) ;
      }
      catch ( e )
      {
         ret[Errno]     = GETLASTERROR( e, false ) ;
         ret[Detail]    = GETLASTERRMSG() ;
      }
      // set return result
      RET_JSON[HostInfo].push( ret ) ;
   }

   return RET_JSON ;
}

// execute
main() ;

