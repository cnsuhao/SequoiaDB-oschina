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
@description: common function for all the js files in current document
@modify list:
   2014-7-26 Zhaobo Tan  Init
*/

var FILE_NAME_FUNC = "func.js" ;
var errMsg         = "" ;
var rc             = SDB_OK ;

/* *****************************************************************************
@discretion: handle system exception
@author: Tanzhaobo
@parameter
   exp[object]: all kinds of exceptions
@return void
***************************************************************************** */
function SYSEXPHANDLE( exp )
{
   if ( "number" != typeof(exp) )
   {
      if ( "object" == typeof(exp) )
         setLastErrMsg( exp.message ) ;
      else
         setLastErrMsg( exp + "" ) ;
      setLastError( SDB_SYS ) ;
   }
}

/* *****************************************************************************
@discretion: get last error number
@author: Tanzhaobo
@parameter void
@return
   errno[int] the error number
***************************************************************************** */
function GETLASTERROR ()
{
   var errno = getLastError() ;
   if ( undefined == errno )
      errno = SDB_SYS ;
   return errno ;
}

/* *****************************************************************************
@discretion: get last error message
@author: Tanzhaobo
@parameter void
@return
   errmsg[string]: the error message
***************************************************************************** */
function GETLASTERRMSG ()
{
   var errmsg = getLastErrMsg() ;
   if ( undefined == errmsg )
      errmsg = "" ;
   return errmsg ;
}

/* *****************************************************************************
@discretion: handle exception
@author: Tanzhaobo
@parameter
   exp[object]: the exception
   msg[string]: error message 
@return void
***************************************************************************** */
function exception_handle( exp, msg )
{
   setLastErrMsg( msg ) ;
   if ( "number" == typeof( exp ) && exp < 0 )
   {
      setLastError( exp ) ;
      throw exp ;
   }
   else
   {
      setLastError( SDB_SYS ) ;
      throw SDB_SYS ;
   }
}

function exception_msg( exp )
{
   return ((null != exp.message) && undefined != exp.message) ? exp.message : exp ;
}

/* *****************************************************************************
@discretion: remove the "\n" or "\n\r" in the end of string
@author: Tanzhaobo
@parameter
   str[string]: the string to deal with
@return
   retStr[string]: the return string without "\n" or "\n\r"
***************************************************************************** */
function removeLineBreak ( str )
{
   var retStr = str ;

   if ( SYS_LINUX == SYS_TYPE )
   {
      var i = str.indexOf( "\n" ) ;
      if ( -1 != i )
      {
         var substr = str.substring(0, i);
         retStr = substr ;
      }
      else
      {
         retStr = str ;
      }
   }
   else
   {
      // TODO:
   }
   return retStr ;
}

/* *****************************************************************************
@discretion: gen a string with the format "YYYY-MM-DD-HH.mm.ss.ffffff"
@author: Tanzhaobo
@parameter
@return
   retStr[string]: a string with the format "YYYY-MM-DD-HH.mm.ss.ffffff"
                   to express current timestamp
***************************************************************************** */
function genTimeStamp()
{
   var retStr = null ;
   var dateVar = new Date() ;
   var dateStr = dateVar.toLocaleDateString() ;
   var timeStr = dateVar.toLocaleTimeString() ;
   var strs = dateStr.split( '/' ) ;
   retStr = strs[2] + "-" + strs[0] + "-" + strs[1] + "-" + timeStr ;

   return retStr ;
}

/* *****************************************************************************
@discretion: check whether it's in local host environment
@author: Tanzhaobo
@parameter
   ssh[object]: Ssh object
@return
   [bool]: whether it's in local host environment
***************************************************************************** */
function isInLocalHost( ssh )
{
   var ip1 = ssh.getLocalIP() ;
   var ip2 = ssh.getPeerIP() ;
   if( ip1 == ip2 )
   {
      return true ;
   }
   else
   {
      return false ;
   }
}

/* *****************************************************************************
@discretion: adapt path with "\"(linux) or "//"(window) in the end
@author: Tanzhaobo
@parameter
   path[string]: a path
@return
   [string]: a path with "\" or "//" in the end
***************************************************************************** */
function adaptPath( path )
{
   var s = "" ;
   var i = -1 ;
   if ( SYS_LINUX == SYS_TYPE )
   {
      s = "/" ;
   }
   else
   {
      s = "\\" ;
   }
   i = path.lastIndexOf( s ) ;
   if ( i != ( path.length -1 ) )
   {
      path += s ;
   }
   return path ;
}

/* *****************************************************************************
@discretion: judge whethe a value is array or not
@author: Tanzhaobo
@parameter
   value[]: input value
@return
   [bool]: return whethe the value is array
***************************************************************************** */
function isArray( value )
{
   return Object.prototype.toString.call(value) === '[object Array]' ;
}

/* *****************************************************************************
@discretion: judge whethe a value is array or not
@author: Tanzhaobo
@parameter
   port[int]: input port
@return
   ret[bool]: return whether the port is a reserved port
***************************************************************************** */
function isReservedPort( port )
{
   var ret = false ;
   var len = OMA_RESERVED_PORT.length ;
   for ( var i = 0; i < len; i++ )
   {
      var val = OMA_RESERVED_PORT[i] ;
      var flag = isArray( val ) ;
      if ( flag )
      {
         var port1 = val[0] ;
         var port2 = val[1] ;
         if ( port1 <= port && port <= port2 )
         {

            ret = true ;
            break ;
         } 
      }
      else
      {
         if ( port === val )
         {
            ret = true ;
            break ;
         }
      }
   }
   return ret ;
}

/* *****************************************************************************
@discretion: create temporary directory in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return void
***************************************************************************** */
function createTmpDir( ssh )
{
   var str = "" ;
   // directories make in target host /tmp
   var dirs = [ OMA_PATH_TEMP_OMA_DIR_L,
                OMA_PATH_TEMP_BIN_DIR_L,
                OMA_PATH_TEMP_PACKET_DIR_L,
                OMA_PATH_TEMP_CONF_DIR_L,
                OMA_PATH_TEMP_LOG_DIR_L,
                OMA_PATH_TEMP_SPT_DIR_L,
                OMA_PATH_TEMP_TEMP_DIR_L,
                OMA_PATH_VCOORD_PATH_L ] ;
   try
   {
      if ( SYS_LINUX == SYS_TYPE )
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
      }
/*
      if ( SYS_LINUX == SYS_TYPE )
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
        // mkdir /tmp/omatmp/packet
        cmd = "mkdir -p " + OMA_PATH_TEMP_PACKET_DIR_L ; 
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
        // mkdir /tmp/omatmp/data/vCoord
        cmd = "mkdir -p " + OMA_PATH_VCOORD_PATH_L ;
        ssh.exec( cmd ) ;
      }
      else
      {
         // TODO: tanzhaobo
      }
*/
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = "Failed to create temporary directory in host[" + ssh.getPeerIP() + "]" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_FUNC,
              errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
      exception_handle( rc, errMsg ) ;
   }
}

/* *****************************************************************************
@discretion: remove the temporary directory and files in temporary directory
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return void
***************************************************************************** */
function removeTmpDir( ssh )
{
   var str = "" ;
   
   try
   {   
      if ( SYS_LINUX == SYS_TYPE )
      {
         str = "rm -rf " + OMA_PATH_TEMP_OMA_DIR_L2 ;
         ssh.exec( str ) ;
      }
      else
      {
         // TODO:
      }
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = "Failed to remove temporary directory in host [" + ssh.getPeerIP() + "]" ;
      PD_LOG( arguments, PDWARNING, FILE_NAME_FUNC,
              errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
   }
}

/* *****************************************************************************
@discretion: remove the temporary directory but leave log files
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return void
***************************************************************************** */
function removeTmpDir2( ssh )
{
   var str = "" ;
   // directories need to be removed in target host
   var dirs = [ OMA_PATH_TEMP_BIN_DIR_L,
                OMA_PATH_TEMP_PACKET_DIR_L,
                OMA_PATH_TEMP_SPT_DIR_L,
                OMA_PATH_TEMP_LOCAL_DIR_L,
                OMA_PATH_TEMP_DATA_DIR ] ;
   try
   {
      if ( SYS_LINUX == SYS_TYPE )
      {
        // remove dirs
        for ( var i = 0; i < dirs.length; i++ )
        {
           str = "rm -rf " + dirs[i] ;
           ssh.exec( str ) ;
        }
      }
      else
      {
         // TODO: tanzhaobo
      }
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = "Failed to remove temporary directory but leave log file in host[" + ssh.getPeerIP() + "]" ;
      PD_LOG( arguments, PDWARNING, FILE_NAME_FUNC,
              errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
   }
}

/* *****************************************************************************
@discretion: get the script's path
@author: Tanzhaobo
@parameter
   path[string]: the path of current program working
                 e.g. /opt/sequoiadb/bin
@return
   retStr[string]: the path of script files
***************************************************************************** */
function getSptPath( path )
{
   var retStr = "" ;
   var str = "" ;
   var pos = -1 ;

   if ( SYS_LINUX == SYS_TYPE )
   {
      retStr = adaptPath( path ) ;
      str = "/" ;
      pos = path.lastIndexOf( str, retStr.length - 2 ) ;
      if ( -1 == pos )
      {
         setLastErrMsg( "Invalid sdbcm running path: " + path ) ;
         setLastError( SDB_INVALIDARG ) ;
         throw SDB_INVALIDARG ;
      }
      retStr = path.substring( 0, pos + 1 ) + "conf/script/" ;
   }
   else
   {
      // TODO:
   }
   return retStr ;
}

/* *****************************************************************************
@discretion: get the total number of program about sdbcm in remote host
             according the result of sdblist
@author: Tanzhaobo
@parameter
   str[string]: the result of sdblist
@return
   retNum[number]: the total nunber or -1
***************************************************************************** */
function extractTotalNumber( str )
{
   var retNum = -1 ;
   var symbol = "Total:" ;

   var pos = str.lastIndexOf( symbol ) ;
   if ( -1 != pos )
   {
      var subStr = str.substring( pos + symbol.length, str.length ) ;
      retNum = parseInt( subStr ) ;
   }
   return retNum ;
}

/* *****************************************************************************
@discretion: get remote sdbcm port according the result of sdblist
@author: Tanzhaobo
@parameter
   str[string]: the result of sdblist
@return
   retPort[number]: the port or -1
***************************************************************************** */
function extractPort( str )
{
   var retPort = -1 ;
   var symbol = "(" ;

   var pos = str.indexOf( symbol ) ;
   if ( -1 != pos )
   {
      var subStr = str.substring( pos + symbol.length, str.length ) ;
      retNum = parseInt( subStr ) ;
   }
   return retNum ;
}

/* *****************************************************************************
@discretion: get sdbcm port in target host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@exception
@return
   retPort[number]: the port of target sdbcm
***************************************************************************** */
function getSdbcmPort( ssh )
{
   var retPort = -1 ;
   var str = "" ;
   var errMsg = "" ;

   if ( SYS_LINUX == SYS_TYPE )
   {
      var installInfoObj = null ;
      var installPath = null ;
      var prog = null ;
      var cmd = null ;
      var ret = SDB_OK ;
      var isLocal = isInLocalHost( ssh ) ;
      if ( isLocal )
      {
         try
         {
            installInfoObj = eval( '(' + Oma.getOmaInstallInfo() + ')' ) ;
            installPath = adaptPath( installInfoObj[INSTALL_DIR] ) ;
            prog = installPath  + OMA_PROG_BIN_SDBLIST_L ;
         }
         catch( e )
         {
            errMsg = "Failed to get sdbcm's port in host[" + ssh.getPeerIP() + "]" ;
            PD_LOG( arguments, PDERROR, FILE_NAME_FUNC,
                    errMsg + ", rc: " + getLastError() + ", detail: " + getLastErrMsg() ) ;
            exception_handle( SDB_SYS, errMsg ) ;
         }
      }
      else
      {
         prog = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBLIST_L ;
      }
      cmd = prog + " -t cm " ;
      try
      {
         str = ssh.exec( cmd ) ;
      }
      catch ( e )
      {
         SYSEXPHANDLE( e ) ;
         ret = ssh.getLastRet() ;
         if ( ret < 0 )
         {
            errMsg = "Failed to get sdbcm's port in host[" + ssh.getPeerIP() + "]" ;
            PD_LOG( arguments, PDERROR, FILE_NAME_FUNC,
                    errMsg + ", rc: " + getLastError() + ", detail: " + getLastErrMsg() ) ;
            exception_handle( SDB_SYS, errMsg ) ;
         }
         else if ( ret > 0 )
         {
            errMsg = "sdbcm is not running in host[" + ssh.getPeerIP() + "]" ;
            PD_LOG( arguments, PDERROR, FILE_NAME_FUNC,
                    errMsg + ", rc: " + getLastError() + "detail: " + getLastErrMsg() ) ;
            exception_handle( SDB_SYS, errMsg ) ;
         }
      }
      retPort = extractPort ( str ) ;
      if ( -1 == retPort )
      {
         errMsg = ( "Failed to get sdbcm's port in host[" + ssh.getPeerIP() + "]" ) ;
         PD_LOG( arguments, PDERROR, FILE_NAME_FUNC,
                 errMsg + ", rc: " + getLastError() + "detail: " + getLastErrMsg() ) ;
         exception_handle( SDB_SYS, errMsg ) ;
      }
   }
   else
   {
      //TODO:
   }
   return retPort ;
}

/* *****************************************************************************
@discretion: check whether sdbcm is running in target host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@exception
@return
   isRunning[bool]: whether sdbcm is running
***************************************************************************** */
function isSdbcmRunning( ssh, host )
{
   var isRunning = false ;
   var str = null ;
   var ret = SDB_OK ;
   if ( SYS_LINUX == SYS_TYPE )
   {
      var installInfoObj = null ;
      var installPath = null ;
      var prog = null ;
      var cmd = null ;
      var isLocal = isInLocalHost( ssh ) ;
      if ( isLocal )
      {
         installInfoObj = eval( '(' + Oma.getOmaInstallInfo() + ')' ) ;
         installPath = adaptPath( installInfoObj[INSTALL_DIR] ) ;
         prog = installPath  + OMA_PROG_BIN_SDBLIST_L ;
      }
      else
      {
         prog = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBLIST_L ;   
      }
      cmd = prog + " -t cm " ;
      try
      {
         str = ssh.exec( cmd ) ;
      }
      catch ( e )
      {
         SYSEXPHANDLE( e ) ;
         ret = ssh.getLastRet() ;
         if ( ret > 0 )
         {
            isRunning = false ;
            return isRunning ;
         }
         else if ( ret < 0 )
         {
            setLastErrMsg( "Unkown sdbcm's status in[" + ssh.getPeerIP() + "]" ) ;
            setLastError( SDB_SYS ) ;
            throw SDB_SYS ;
         }
      }
      // when sdbcm is running
      var num = extractTotalNumber ( str ) ;
      if ( -1 == num )
      {
         setLastErrMsg( "Unkown sdbcm's status in[" + ssh.getPeerIP() + "]" ) ;
         setLastError( SDB_SYS ) ;
         throw SDB_SYS ;
      }
      else if ( 0 == num )
      {
         isRunning = false ;
      }
      else
      {
         isRunning = true ;
      }
   }
   else
   {
      // TODO:
   }
   return isRunning ;
}

/* *****************************************************************************
@discretion: get a usable port from local host
@author: Tanzhaobo
@parameter
   osInfo[string]: os type
@return
   retPort[nunber]: return a usable port or OMA_PORT_INVALID
***************************************************************************** */
function getAUsablePortFromLocal( osInfo )
{
   var retPort = OMA_PORT_INVALID ;
   var flag = false ;   

   if ( OMA_LINUX == osInfo )
   {
      for ( var port = OMA_PORT_TEMP_AGENT_PORT ;
            port <= OMA_PORT_MAX; port++ )
      {
         flag = isReservedPort( port ) ;
         if ( flag )
         {
            continue ;
         } 
         var ret =  eval( '(' + System.sniffPort( port ) + ')' ) ;
         flag = ret[Usable];
         if ( flag )
         {
            retPort = port ;
            break ;
         }
      }
   }
   else
   {
      // TODO:
   }
   return retPort ;
}

/* *****************************************************************************
@discretion: get a usable port from remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return
   retPort[nunber]: return a usable port or OMA_PORT_INVALID
***************************************************************************** */
function getAUsablePortFromRemote( ssh )
{
   var retPort = OMA_PORT_INVALID ;
   var str = "" ;
   var port = OMA_PORT_TEMP_AGENT_PORT ;
   var flag = false ;

   if ( SYS_LINUX == SYS_TYPE )
   {
      for ( var port = OMA_PORT_TEMP_AGENT_PORT ;
            port <= OMA_PORT_MAX; port++ )
      {
         flag = isReservedPort( port ) ;
         if ( flag )
         {
            continue ;
         }
         str = "netstat -nap | grep " + port + " | grep -v grep" ;
         try
         {
            ssh.exec( str ) ;
         }
         catch ( e )
         {
            SYSEXPHANDLE( e ) ;
            var ret = ssh.getLastRet() ;
            if ( 1 == ret )
            {
               retPort = port ;
            }
            else
            {
               retPort = OMA_PORT_INVALID ;
            }
            break ;
         }
      }
   }
   else
   {
      // TODO:
   }
   return retPort ;
}

/* *****************************************************************************
@discretion: get the right place to change the owner of a directory
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return
   retPort[nunber]: return a dirctory path
***************************************************************************** */
function getThePlaceToChangeOwner( path )
{
   var retStr = path ;
   var pos = -1 ;
   if ( SYS_LINUX == SYS_TYPE )
   {
      var arr = path.split( '/' ) ;
      var num = arr.length ;
      // in case: "/" or "/opt"
      if ( num <= 2 )
      {
         return path ;
      }
      pos = path.lastIndexOf( '/' ) ;
      // in case: "/opt/"
      if ( pos == path.length -1 && num == 3 )
      {
         return path ;
      }
      // otherwise
      var len = arr[num - 1].length ; ;
      // in case: "/opt/.../123/345"
      if ( len )
      {
         pos = path.length - 1 - len ;
         retStr = path.substring( 0, pos ) ;
      }
      // in case: "/opt/.../123/345/"
      else
      {
         len = arr[num - 2].length ;
         pos = path.length - 1 - len - 1 ;
         retStr = path.substring( 0, pos ) ;
      }
   }
   else
   {
     // TODO:
   }

   return retStr ;
}

/* *****************************************************************************
@discretion: change the owner of the directory
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   path[string]: the path of the directory
   user[string]: the user to change to
   userGroup[sring]: the user group to change to
@return void
***************************************************************************** */
function changeDirOwner( ssh, path, user, userGroup )
{
   var ret = null ;
   var str = null ;
   var cmd = null ;
   if ( SYS_LINUX == SYS_TYPE )
   {
      cmd = " mkdir -p " + path ;
      try
      {
         ssh.exec( cmd ) ;
      }
      catch ( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = "Failed to create path [" + path + "] in " + "[" + ssh.getPeerIP() + "]" ;
         exception_handle( e, errMsg ) ;
      }
      path = getThePlaceToChangeOwner( path ) ;
      str = user + ":" + userGroup ;
      cmd = " chown -R " + str + " " + path ;
      try
      {
         ssh.exec( cmd ) ;
      }
      catch ( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = "Failed to change path [" + path + "]'s owner in " + "[" + ssh.getPeerIP() + "]" ;
         exception_handle( e, errMsg ) ;
      }
   }
   else
   {
      // TODO: windows
   }
}

/* *****************************************************************************
@discretion: change the mode of file or directory
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   path[string]: file or directory
   mode[string]: the mode to change to, e.g. "755"
@return void
***************************************************************************** */
function changeFileOrDirMode( ssh, path, mode )
{
   var str = "" ;
   if ( SYS_LINUX == SYS_TYPE )
   {
      try
      {
         str = "chmod -R " + mode + " " + path ;
         ssh.exec( str ) ;
      }
      catch( e )
      {
         errMsg = "Failed to change file or directory[" + path + "]'s mode to mode[" + mode + "]" ;
         rc = GETLASTERROR() ;
         PD_LOG( arguments, PDERROR, FILE_NAME_FUNC,
                 errMsg + "rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
         exception_handle( rc, errMsg ) ;
      }
   }
   else
   {
      // TODO:
   }
}

/* *****************************************************************************
@discretion: check wether catalog is running or not
@author: Tanzhaobo
@parameter
   db[object]: Sdb object
@return 
   [bool]
***************************************************************************** */
function isCatalogRunning( db )
{
   var rg = null ;
   try
   {
      rg = db.getCatalogRG() ;
   }
   catch ( e )
   {
      return false ;
   }
   return true ;
}

/* *****************************************************************************
@discretion: get remote or local sdbcm port from local sdbcm config file
@parameter
   hostName[string]: install host name
@return
   retPort[string]: the sdbcm port in remote or in local
***************************************************************************** */
function getAgentPort( hostname )
{
   var retPort = null ;
   var localhostname = System.getHostName() ;
   if ( hostname == localhostname )
   {
      retPort = Oma.getAOmaSvcName( hostname ) ;
   }
   else
   {
      var key = hostname + OMA_MISC_CONFIG_PORT ;
      var obj =  eval ( '(' + Oma.getOmaConfigs() + ')' ) ;
      var retPort = obj[key]
      if ( "undefined" == typeof(retPort) )
         retPort = OMA_PORT_DEFAULT_SDBCM_PORT + "" ;
   }
   return retPort + "" ;
}

/* *****************************************************************************
@discretion: get local host name or ip address from hosts table
@parameter void
@return
   retStr[string]: local host name or ip address
***************************************************************************** */
function _getLocalHostNameOrIP( type )
{
   var retStr = null ;
   var cmd = new Cmd() ;
   var osInfo = System.type() ;
   try
   {
      if ( SYS_LINUX == SYS_TYPE )
      {
         var str = null ;
         if ( "hostname" == type )
            str = cmd.run( "hostname" ) ;
         else if ( "ip" == type )
            str = cmd.run("hostname -i") ;
         else
           throw SDB_INVALIDARG ;
         str = removeLineBreak( str ) ;
         var arr = str.split(" ") ;
         retStr = arr[0] ;
      }
      else
      {
         // TODO:
      }
   }
   catch ( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = "Failed to get localhost host name or ip address" ;
      exception_handle( e, errMsg ) ;
   }
   return retStr ;
}

/* *****************************************************************************
@discretion: get local host name from hosts table
@parameter void
@return
   [string]: local host name
***************************************************************************** */
function getLocalHostName()
{
   return _getLocalHostNameOrIP( "hostname" ) ;
}

/* *****************************************************************************
@discretion: get local ip from hosts table
@parameter void
@return
   [string]: local ip address
***************************************************************************** */
function getLocalIP()
{
   return _getLocalHostNameOrIP( "ip" ) ;
}

/* *****************************************************************************
@discretion: stop the temporary sdbcm installed in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return void
***************************************************************************** */
function stopRemoteSdbcmProgram( ssh )
{
   var cmd = "" ;

   if ( SYS_LINUX == SYS_TYPE )
   {
      cmd += OMA_PATH_TEMP_BIN_DIR_L ;
      cmd += OMA_PROG_SDBCMTOP_L ;
      cmd += " " + OMA_OPTION_SDBCMART_I ;
      try
      {
         ssh.exec( cmd ) ;
      }
      catch ( e )
      {
         setLastErrMsg( "Failed to stop sdbcm in host[" + ssh.getPeerIP() + "]" ) ;
         setLastError( SDB_SYS ) ;
         throw SDB_SYS ;
      }
      // check wether sdb is stop in target host
      var times = 0 ;
      for ( ; times < OMA_TRY_TIMES; times++ )
      {
         var isRunning = isSdbcmRunning ( ssh ) ;
         if ( isRunning )
         {
            sleep( OMA_SLEEP_TIME ) ;
         }
         else
         {
            break ;
         }
      }
      if ( OMA_TRY_TIMES <= times )
      {
         setLastErrMsg( "Time out, failed to stop sdbcm in host[" + ssh.getPeerIP() + "]" ) ;
         throw e ;
      }
   }
   else
   {
      // TODO: tanzhaobo
   }
}

/* *****************************************************************************
@discretion: mimic "sprintf" in "C" simply 
@author: Tanzhaobo
@parameter
   format[string]: e.g. "a = ?, b = ?, is it right/? "
@return
   newStr[string]: e.g. "a = 1, b = 2, is it right? "
@usage
   var str = sprintf( "a = ?, b = ?, is it right/? ", 1, 2, '?' ) ;
***************************************************************************** */
function sprintf( format )
{
   var len = arguments.length ;
   var strLen = format.length ;
   var newStr = '' ;
   for ( var i = 0, k = 1; i < strLen; i++ )
   {
      var char = format.charAt( i ) ;
      if ( char == '\\' && (i + 1 < strLen) && format.charAt(i + 1) == '?' )
      {
         newStr += '?' ;
         i++ ;
      }
      else if ( char == '?' && k < len )
      {
         newStr += ( '' + arguments[k] ) ;
         ++k ;
      }
      else
      {
         newStr += char ;
      }
   }
   return newStr ;
}

