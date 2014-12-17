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
@description: update host info in local host table
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostName": "rhel64-test8", "IP": "192.168.20.42", "User": "root", "Passwd": "sequoiadb", "HostInfo": [ { "HostName": "rhel64-test8", "IP": "192.168.20.165" }, { "HostName": "rhel64-test9", "IP": "192.168.20.166", "AgentPort":"11790" } ] }
   SYS_JSON: the format is:
   ENV_JSON:
@return
   RET_JSON: the format is: {}
*/

var RET_JSON          = new Object() ;
var errMsg            = "" ;
/* *****************************************************************************
@discretion: get local db business install path
@author: Tanzhaobo
@parameter
   localhostip[string]: ip of localhost
   osInfo[string]: os information, "LINUX" or "WINDOWS"
@return
   installpath[string]: the db business install director
***************************************************************************** */
function getLocalDBInstallPath( localhostip, osInfo )
{
   var omaInstallInfo = null ;
   var installpath = null ;
   try
   {
      omaInstallInfo = eval( '(' + Oma.getOmaInstallInfo() + ')' ) ;
   }
   catch( e )
   {
      errMsg = "Failed to get db install path in host[" + localhostip + "]" ;
      exception_handle( e, errMsg ) ;
   }
   installpath = adaptPath ( osInfo, omaInstallInfo[INSTALL_DIR] ) ;
   return installpath ;
}

/* *****************************************************************************
@discretion: update host table
@author: Tanzhaobo
@parameter
   localhostip[string]: ip of localhost
   osInfo[string]: os information, "LINUX" or "WINDOWS"
   arr[Array]: update info
@return void
***************************************************************************** */
function updateHostInfo( localhostip, osInfo, arr )
{
   var installpath = getLocalDBInstallPath( localhostip, osInfo ) ;
   var cmd = new Cmd() ;
   if ( OMA_LINUX == osInfo )
   {
      var omtoolpath = installpath + OMA_PROG_BIN_SDBOMTOOL_L ;
      for ( var i = 0; i < arr.length; i++ )
      {
         var str      = null ;
         var obj      = arr[i] ;
         var hostname = obj[HostName] ;
         var ip       = obj[IP] ;
         str = omtoolpath + ' -m addhost --hostname ' + hostname + ' --ip ' + ip ;
         try
         {
            cmd.run( str ) ;
         }
         catch ( e )
         {
            errMsg = "Failed to set info [" + ip + " " + hostname + "] to hosts table in host[" + localhostip + "]" ;
            exception_handle( e, errMsg ) ;
         }
      }
   }
   else
   {
      // TODO: windows
   }
}

/* *****************************************************************************
@discretion: update sdbcm config file
@author: Tanzhaobo
@parameter
   localhostip[string]: ip of localhost
   osInfo[string]: os information, "LINUX" or "WINDOWS"
   arr[Array]: update info
@return void
***************************************************************************** */
function updateSdbcmCfgFile( lcoalhostip, osInfo, arr )
{
   var agentport = null ;
   var hostname  = null ;
   var configobj = null ;
   try
   {
      configobj = eval ( '(' + Oma.getOmaConfigs() + ')' ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to get oma config info in host [" + lcoalhostip + "]" ;
      exception_handle( e, errMsg ) ;
   }
   for ( var i = 0; i < arr.length; i++ )
   {
      try
      {
         agentport = arr[i][AgentPort] ;
         hostname  = arr[i][HostName] ;
      }
      catch ( e )
      {
         continue ;
      }
      var str = hostname + OMA_MISC_CONFIG_PORT ;
      configobj[str] = agentport ; 
   }
   try
   {
      Oma.setOmaConfigs( configobj ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to set oma config info in host [" + lcoalhostip + "]" ;
      exception_handle( e, errMsg ) ;
   }
}

function main()
{
   var ip               = BUS_JSON[IP] ;
   var user             = BUS_JSON[User] ;
   var passwd           = BUS_JSON[Passwd] ;
   var updateInfoArr    = BUS_JSON[HostInfo] ;
   var osInfo           = null ;

   // get os info
   osInfo = System.type() ;
   // update host table
   updateHostInfo( ip, osInfo, updateInfoArr ) ;     
   // update sdbcm comfig file
   updateSdbcmCfgFile( ip, osInfo, updateInfoArr ) ;

   return RET_JSON ;
}

// execute
main() ;

