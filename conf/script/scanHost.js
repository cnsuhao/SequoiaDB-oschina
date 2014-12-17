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
@description: scan host
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostInfo": [ { "IP": "192.168.20.42", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }, { "IP": "192.168.20.165", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" }, { "IP": "192.168.20.166", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" } ] } ;
   SYS_JSON:
   ENV_JSON:
@return
   RET_JSON the scan result, the format is as: { "HostInfo": [ { "errno": 0, "detail": "", "Ping": true, "Ssh": false, "HostName": "rhel64-test8", "IP": "" }, { "errno": 0, "detail": "", "Ping": true, "Ssh": true, "HostName": "rhel64-test9", "IP": "192.168.20.166" } ] }
*/

var RET_JSON       = new Object() ;
RET_JSON[HostInfo] = [] ;
var errMsg         = "" ;
/* *****************************************************************************
@discretion: scan a remote host, to check wether it can been "ping" and "ssh"
             or not, and try to get it's hostname if hostname is not specified
@author: Tanzhaobo
@parameter
   user[string]: the user name
   passwd[string]: the password
   hostname[string]: the hostname
   sshport[int]: the ssh port
   ip[string]: the ip address
@note
   either ip or hostname must be specified
@return
   retStr[string]: the hostname after adapting
***************************************************************************** */
function scanHost( user, passwd, hostname, sshport, ip )
{
   var ssh             = null ;
   var ping            = null ;
   var retObj          = new Object() ;
   retObj[Errno]       = SDB_OK ;
   retObj[Detail]      = "" ;
   retObj[CanPing]     = false ;
   retObj[CanSsh]      = false ;
   retObj[HostName]    = "" ;
   retObj[IP]          = "" ;

   // in case hostname is specified
   if ( null != hostname && undefined != hostname )
   {
      // hostname
      retObj.HostName = hostname ;
      // ping
      var ret = System.ping( hostname ) ;
      ping = eval( "(" + ret + ")" ) ;
      if ( true != ping[Reachable] )
      {
         return retObj ;
      }
      retObj[CanPing] = true ;
      // ssh
      try
      {
         ssh = new Ssh( hostname, user, passwd, sshport ) ;
         retObj[CanSsh] = true ;
      }
      catch ( e )
      {
         retObj[Errno] = getLastError() ;
         retObj[Detail] = getLastErrMsg() ;
         return retObj ;
      }
      // ip
      var ipTmp = "" ;
      try
      {
         ipTmp = ssh.getPeerIP() ;
      }
      catch ( e )
      {
         retObj[Errno] = getLastError() ;
         retObj[Detail] = getLastErrMsg() ;
         return retObj ;
      }
      // if no error, extract the ip
      if ( "string" == typeof(ipTmp) )
      {
         retObj[IP] = removeLineBreak( ipTmp ) ;
      }
   }
   else if ( null != ip && undefined != ip )
   {
      // ip
      retObj[IP] = ip ;
      // ping
      var ret = System.ping( ip, 3 ) ;
      ping = eval( "(" + ret + ")" ) ;
      if ( true != ping[Reachable] )
         return retObj ;
      retObj[CanPing] = true ;
      // ssh
      try
      {
         ssh = new Ssh( ip, user, passwd, sshport ) ;
         retObj[CanSsh] = true ;
      }
      catch ( e )
      {
         retObj[Errno] = getLastError() ;
         retObj[Detail] = getLastErrMsg() ;
         return retObj ;
      }
      // hostName
      var name = "" ;
      try
      {
         name = ssh.exec("hostname") ;
      }
      catch ( e )
      {
         retObj[Errno] = getLastError() ;
         retObj[Detail] = getLastErrMsg() ;
         return retObj ;
      }
      if ( "string" == typeof(name) )
      {
         retObj[HostName] = removeLineBreak( name ) ;
      }
   }
   return retObj ;
}

function main()
{
   var infoArr = BUS_JSON[HostInfo] ;
   var arrLen = infoArr.length ;
   if ( arrLen == 0 )
   {
      setLastErrMsg( "Not specified any host to scan" ) ;
      setLastError( SDB_INVALIDARG ) ;
      throw SDB_INVALIDARG ;
   }
   for( var i = 0; i < arrLen; i++ )
   {
      var obj      = null ;
      var user     = null ;
      var passwd   = null ;
      var hostname = null ;
      var sshport  = null ;
      var ip       = null ;
      var ret      = null ;
      try
      {
         obj       = infoArr[i] ;
         user      = obj[User] ;
         passwd    = obj[Passwd] ;
         hostname  = obj[HostName] ;
         sshport   = parseInt(obj[SshPort]) ;
         ip        = obj[IP] ;
         if ( undefined != hostname )
         { 
            if ( "localhost" == hostname )
               hostname = getLocalHostName() ;
            ret = scanHost( user, passwd, hostname, sshport, null ) ;
         }
         else if ( undefined != ip )
         {
            if ( "127.0.0.1" == ip )
               ip = getLocalIP() ;
            ret = scanHost( user, passwd, null, sshport, ip ) ;
         }
         else
         {
            setLastErrMsg( "Not specified hostname or ip" ) ;
            setLastError( SDB_INVALIDARG ) ;
            throw SDB_INVALIDARG ;
         }
      }
      catch ( e )
      {
         if ( undefined != hostname )
         {
            errMsg = "Failed to scan host[" + hostname + "]" ;
         }
         else if ( undefined != ip )
         {
            errMsg = "Failed to scan host [" + ip + "]" ;
         }
         else
         {
            errMsg = "Failed to scan host" ;
         }
         exception_handle( e, errMsg ) ;
      }
      RET_JSON[HostInfo].push( ret ) ;
   }

   return RET_JSON ;
}

// execute
main() ;

