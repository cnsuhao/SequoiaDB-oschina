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
@description: check whether localhost can ping and ssh to remote host
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostInfo": [ { "IP": "192.168.20.42", "HostName": "susetzb", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }, { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }, { "IP": "192.168.20.166", "HostName": "rhel64-test9", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" } ] } ;
   SYS_JSON: the format is: { "ProgPath": "/opt/sequoiadb/bin/" } ;
   ENV_JSON:
@return
   RET_JSON:
*/

var BUS_JSON = { "HostInfo": [ { "IP": "192.168.20.42", "HostName": "susetzb", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }, { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }, { "IP": "192.168.20.166", "HostName": "rhel64-test9", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" } ] } ;
var SYS_JSON = { "ProgPath": "/opt/sequoiadb/bin/" } ;

var RET_JSON = new checkHostResult() ;

/* *****************************************************************************
@discretion: check host
@author: Tanzhaobo
@parameter
   user[string]: the user name
   passwd[string]: the password
   ip[string]: the target host's ip address
   hostname[string]: the target host's hostname
   sshport[int]: ssh port
@return
   retObj[object]: the remote host's status, include OS, OM, CPU...
***************************************************************************** */
function checkHost( user, passwd, ip, hostname, sshport )
{
   var retObj       = new checkHostRet() ;
   var prog_path    = SYS_JSON[ProgPath] ;
   var bcRet        = null ;
   var installRet   = null ;
   var uninstallRet = null ;
   var ssh          = null ;
   var osInfo       = null ;
   var prog_path    = SYS_JSON[ProgPath] ;
   retObj[IP]       = ip ;
   retObj[HostName] = hostname ;

println("Basic check host, 111111111111111111111111111111111") ;
   // 1: basic check
   bcRet = basicCheckHost( user, passwd, ip, sshport ) ;
   if ( true != bcRet[CanPing] || true != bcRet[CanSsh] )
   {
      retObj[Errno] = bcRet[Errno] ;
      retObj[Description] = bcRet[Description] ;
      retObj[Detail] = bcRet[Detail] ;
      return retObj ;
   }
   
   // get os info and ssh object
   ssh = new Ssh( ip, user, passwd, sshport ) ;
   osInfo = System.type() ;
println("Install sdbcm to remote, 22222222222222222222222222222222") ;
   // 2: install sdbcm to remote
   installRet = installRemoteAgent( ssh, osInfo, ip, prog_path ) ;
   if ( SDB_OK != installRet[Errno] && "" != installRet[Detail] )
   {
      retObj[Errno] = installRet[Errno] ;
      retObj[Description] = installRet[Description] ;
      retObj[Detail] = installRet[Detail] ;
      return retObj ;
   }
   
   // 3: check host info
	// TODO: tanzhaobo
   // TODO: need some apis offered by xu dao
   
   // 4: stop remote sdbcm and remote it
   if ( true == installRet[IsNeedUninstall] )
   {
println("uninstall sdbcm to remote, 333333333333333333333333333333") ;
      uninstallRet = uninstallRemoteAgent( ssh, osInfo, ip ) ;
      if ( SDB_OK != installRet[Errno] && "" != installRet[Detail] )
      {
         retObj[Errno] = installRet[Errno] ;
         retObj[Description] = installRet[Description] ;
         retObj[Detail] = installRet[Detail] ;
         return retObj ;
      }
	}
   return retObj ;
}

function main()
{
   // check input argument
   var infoArr = BUS_JSON[HostInfo] ;
   var arrLen = infoArr.length ;
   if ( arrLen == 0 )
   {
      RET_JSON[Errno] = SDB_INVALIDARG ;
      RET_JSON[Description] = getErr( SDB_INVALIDARG ) ;
      RET_JSON[Detail] = "Not specified any host to check" ;
      return RET_JSON ;
   }
// { "IP": "192.168.20.42", "HostName": "susetzb", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }
   for( var i = 0; i < arrLen; i++ )
   {
      var obj      = null ;
      var ip       = null ;
      var hostname = null ;
      var user     = null ;
      var passwd   = null ;
      var sshport  = null ;
      var oneRes   = new checkHostRet() ;
      try
      {
         obj       = infoArr[i] ;
         ip        = obj[IP] ;
         hostname  = obj[HostName] ;
         user      = obj[User] ;
         passwd    = obj[Passwd] ;
         sshport   = parseInt(obj[SshPort]) ;
         // check host
         oneRes = checkHost( user, passwd, ip, hostname, sshport ) ;
      }
      catch( e )
      {
         if ( "number" == typeof(e) && e < 0 )
         {
            oneRes[Errno] = getLastError() ;
            oneRes[Description] = getErr( oneRes[Errno] ) ;
            oneRes[Detail] = getLastErrMsg() ;
         }
         else
         {
            oneRes[Errno] = SDB_SYS ;
            oneRes[Description] = getErr( oneRes[Errno] ) ;
            oneRes[Detail] = exception_msg( e ) ;
         }
      }
      RET_JSON[HostInfo].push( oneRes ) ;
   }

println("RET_JSON is: " + JSON.stringify(RET_JSON) ) ;
   return RET_JSON ;
}

// execute
main() ;

