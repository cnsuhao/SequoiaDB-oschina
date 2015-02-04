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
@description: functions for check host info
@modify list:
   2014-7-26 Zhaobo Tan  Init
*/
var errMsg = "" ;
// directories make in target host /tmp
var dirs = [ OMA_PATH_TEMP_OMA_DIR_L,
             OMA_PATH_TEMP_BIN_DIR_L,
             OMA_PATH_TEMP_CONF_DIR_L,
             OMA_PATH_TEMP_LOG_DIR_L,
             OMA_PATH_TEMP_SPT_DIR_L,
             OMA_PATH_TEMP_TEMP_DIR_L,
             OMA_PATH_VCOORD_PATH_L ] ;
             
// tool programs used to start remote sdbcm in remote
var programs = [ "sdblist", "sdbcmd", "sdbcm", "sdbcmart", "sdbcmtop" ] ;

// js files used to check remote host's info
var js_files = [ "error.js", "common.js", "define.js",
                 "func.js", "checkHostItem.js", "checkHost.js" ] ;

/* *****************************************************************************
@discretion: create temp directory in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
@return void
***************************************************************************** */
function _createTmpDir( ssh, osInfo )
{
   var str = "" ;
    
   if ( "LINUX" == osInfo )
   {
     // rm /tmp/omatmp
     str = "rm " + OMA_PATH_TEMP_OMA_DIR_L + " -rf " ;
     ssh.exec( str ) ;
     // mkdir dirs
     for ( var i = 0; i < dirs.length; i++ )
     {
        str = "mkdir -p " + dirs[i] ;
        ssh.exec( str ) ;
     }
   }
   else
   {
      // TODO: tanzhaobo
      // create dirs in windows
   }
}

/* *****************************************************************************
@discretion: push tool programs and js scripts to remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
   prog_path[string]: tool program path in local host
@return void
***************************************************************************** */
function _pushPacket( ssh, osInfo, prog_path )
{
   var src             = "" ;
   var dest            = "" ;
   var local_conf_file = Oma.getOmaConfigFile() ;
   var spt_path        = getScriptPath( local_conf_file ) ;
   if ( OMA_LINUX == osInfo )
   {
      // push tool programs
      for ( var i = 0; i < programs.length; i++ )
      {
         src = prog_path + programs[i] ;
         dest = OMA_PATH_TEMP_BIN_DIR_L + programs[i] ;
         ssh.push( src, dest ) ;
      }
      // push js files
      for ( var i = 0; i < js_files.length; i++ )
      {
         src = spt_path + js_files[i] ;
         dest = OMA_PATH_TEMP_SPT_DIR_L + js_files[i] ;
         ssh.push( src, dest ) ;
      }
     // change mode in /tmp/omatmp/bin
     str = "chmod a+x " + OMA_PATH_TEMP_BIN_DIR_L + "*";
     ssh.exec( str ) ;
   }
   else
   {
      // TODO:
   }
}

/* *****************************************************************************
@discretion: start sdbcm program in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
   port[string]: port for remote sdbcm
@return void
***************************************************************************** */
function _startRemoteTmpAgent( ssh, osInfo, port )
{
   var str = "" ;
   if ( OMA_LINUX == osInfo )
   {
      str += OMA_PATH_TEMP_BIN_DIR_L ;
      str += OMA_PROG_SDBCMART_L ;
      str += " " + OMA_OPTION_SDBCMART_I ;
      str += " " + OMA_OPTION_SDBCMART_PORT ;
      str += " " + port ;
      try
      {
         ssh.exec( str ) ;
      }
      catch ( e )
      {
         errMsg = "Failed to start sdbcm in host[" + ssh.getPeerIP() + "]" ;
         exception_handle( e, errMsg ) ;
      }
      // wait until sdbcm start in remote host
      var times = 0 ;
      for ( times = 0; times < OMA_TRY_TIMES; times++ )
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
@discretion: stop sdbcm program in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   osInfo[string]: os type
@return void
***************************************************************************** */
function _stopRemoteTmpAgent( ssh, osInfo )
{
   var str = "" ;
   if ( OMA_LINUX == osInfo )
   {
      str += OMA_PATH_TEMP_BIN_DIR_L ;
      str += OMA_PROG_SDBCMTOP_L ;
      str += " --I" ;
      try
      {
         ssh.exec( str ) ;
      }
      catch ( e )
      {
         getLastErrMsg() ;
         getLastError() ;
      }
   }
   else
   {
      // TODO: windows
   }
}

/* *****************************************************************************
@discretion: check whether host can be "ping" and "ssh" or not
@author: Tanzhaobo
@parameter
   user[string]: the user name
   passwd[string]: the password
   ip[string]: the ip address
   sshport[int]: ssh port
@return
   retObj[object]: whether can ping and ssh, and the error message
***************************************************************************** */
function basicCheckHost( user, passwd, ip, sshport )
{
   var retObj = new basicCheckRet() ;
   var ping = null ;
   var ssh = null ;
   retObj[IP] = ip ;

   // ping
   try
   {
      ping = eval( "(" + System.ping( ip ) + ")" ) ;
      if ( true != ping[Reachable] )
      {
         retObj[CanPing] = false ;
         retObj[Errno] = SDB_INVALIDARG ;
         retObj[Description] = getErr( SDB_INVALIDARG ) ;
         retObj[Detail] = "Failed to ping to ip[" + ip + "]" ;
         return retObj ;
      }
      retObj[CanPing] = true ;
   }
   catch ( e )
   {
      retObj[CanPing] = false ;
      if ( "number" == typeof(e) && e < 0 )
      {
         retObj[Errno] = getLastError() ;
         retObj[Description] = getErr( retObj[Errno] ) ;
         retObj[Detail] = "Failed to ping to ip[" + ip + "]" ;
      }
      else
      {
         retObj[Errno] = SDB_SYS ;
         retObj[Description] = getErr( SDB_SYS ) ;
         retObj[Detail] = exception_msg( e ) ;
      }
      return retObj ;
   }
   
   // ssh
   try
   {
      ssh = new Ssh( ip, user, passwd, sshport ) ;
      retObj[CanSsh] = true ;
   }
   catch ( e )
   {
      retObj[CanSsh] = false ;
      if ( "number" == typeof(e) && e < 0 )
      {
         retObj[Errno] = getLastError() ;
         retObj[Description] = getErr( retObj[Errno] ) ; 
         retObj[Detail] = getLastErrMsg() ;
      }
      else
      {
         retObj[Errno] = SDB_SYS ;
         retObj[Description] = getErr( SDB_SYS ) ;
         retObj[Detail] = exception_msg( e ) ;
      }
      return retObj ;
   }

println("In basic check, retObj is: " + JSON.stringify(retObj) ) ;
   return retObj ;
}

/* *****************************************************************************
@discretion: install sdbcm program in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: the ssh object
   osInfo[string]: the system type
   ip[string]: the ip address
@return
   retObj[object]: the result of install
***************************************************************************** */
function installRemoteAgent( ssh, osInfo, ip, prog_path )
{
   var retObj                 = new installSdbcmRet() ;
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
         retObj[IsNeedUninstall] = false ;
         retObj[AgentPort] = "" + getSdbcmPort( ssh, osInfo ) ;
      }
      else // else, set error
      {
         retObj[Errno] = SDB_SYS ;
         retObj[Description] = getErr( SDB_SYS ) ;
         retObj[Detail] = "sdbcm is not running in localhost[" + ssh.getPeerIP() + "]" ;
      }
      return retObj ;
   }

   // create dir and push js files
   try
   {
      _createTmpDir( ssh, osInfo ) ;
      _pushPacket( ssh, osInfo, prog_path ) ;
   }
   catch ( e )
   {
      retObj[Errno] = getLastError() ;
      retObj[Description] = getErr( retObj[Errno] ) ;
      retObj[Detail] = getLastErrMsg() ;
      return retObj ;
   }
   // check whether sdbcm is running in remote host
   flag = isSdbcmRunning( ssh, osInfo ) ;
   if ( flag )
   {
      retObj[AgentPort] = "" + getSdbcmPort( ssh, osInfo ) ;
      removeTmpDir( ssh, osInfo ) ;
      return retObj ;
   }
   // get a port in remote host for installing sdbcm in target host
   var port = getAUsablePortFromRemote( ssh, osInfo ) ;
   if ( OMA_PORT_INVALID == port )
   {
      retObj[Errno] = SDB_SYS ;
      retObj[Description] = getErr( retObj[Errno] ) ;
      retObj[Detail] = "Failed to get a usable port from host[" + ssh.getPeerIP() + "]" ;
      return retObj ;
   }
   // start the sdbcm program in remote
   try
   {
      _startRemoteTmpAgent( ssh, osInfo, port ) ;
   }
   catch ( e )
   {
      retObj[Errno] = getLastError() ;
      retObj[Description] = getErr( retObj[Errno] ) ;
      retObj[Detail] = getLastErrMsg() ;
      _stopRemoteTmpAgent( ssh, osInfo ) ;
      return retObj ;
   }
   
   retObj[AgentPort] = port + "" ;
   retObj[IsNeedUninstall] = true ;
println("In install remote sdbcm, retObj is: " + JSON.stringify(retObj) ) ;
   return retObj ;
}

/* *****************************************************************************
@discretion: uninstall sdbcm program in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: the ssh object
   osInfo[string]: the system type
   ip[string]: the ip address
@return
   retObj[object]: the result of uninstall
***************************************************************************** */
function uninstallRemoteAgent( ssh, osInfo, ip )
{
   var retObj     = new uninstallSdbcmRet() ;
   retObj[IP]     = ip ;
   try
   {
      // check whether it is in localhost,
      // we would not stop local sdbcm
      var flag = isInLocalHost( ssh ) ;
      if ( flag )
      {
         return retObj ;
      }
      // stop sdbcm in remote host
      _stopRemoteTmpAgent( ssh, osInfo ) ;
      // remove the temporary directory in remote host
      removeTmpDir( ssh, osInfo ) ;
   }
   catch ( e )
   {
      retObj[Errno] = getLastError() ;
      retObj[Description] = getErr( retObj[Errno] ) ;
      retObj[Detail] = getLastErrMsg() ;
      _stopRemoteTmpAgent( ssh, osInfo ) ;
      return retObj ;
   }
println("In uninstall remote sdbcm, retObj is: " + JSON.stringify(retObj) ) ;
   return retObj ;
}
