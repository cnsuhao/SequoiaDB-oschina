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
@description: uninstall remote sdbcm packet
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the info for unindtall remote host: { "HostInfo": [ { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sequoiadb", "InstallPath": "/opt/sequoiadb", "SshPort": "22", "AgentPort": "11790" }, { "IP": "192.168.20.166", "HostName": "rhel64-test9", "User": "root", "Passwd": "sequoiadb", "InstallPath": "/opt/sequoiadb", "SshPort": "22", "AgentPort": "11790" } ] }
   SYS_JSON:
   ENV_JSON:
   OTHER_JSON:
@return
   RET_JSON: the uninstall result:  { "HostInfo": [ { "IP": "192.168.20.165", "errno": 0, "detail": "", "HasUninstall": true }, { "IP": "192.168.20.166", "errno": 0, "detail": "", "HasUninstall": true } ] }
*/

var RET_JSON = new Object() ;
RET_JSON[HostInfo] = [] ;

function main()
{
   var infoArr = BUS_JSON[HostInfo] ;
   var arrLen = infoArr.length ;
   if ( arrLen == 0 )
   {
      setLastErrMsg( "Not specified any hosts to uninstall it's sdbcm" ) ;
      setLastError( SDB_INVALIDARG ) ;
      throw SDB_INVALIDARG ;
   }
   // get os information
   var osInfo = System.type() ;
   for ( var i = 0; i < arrLen; i++ )
   {
      var ssh        = null ;
      var obj        = infoArr[i]
      var ip         = obj[IP] ;
      var user       = obj[User] ;
      var passwd     = obj[Passwd] ;
      var sshport    = parseInt(obj[SshPort]) ;
      var retObj     = new uninstallTmpCMResult() ;
      retObj[IP]     = ip ;
      try
      {
         // ssh
         var ssh = new Ssh( ip, user, passwd, sshport ) ;
         // check wether it is in localhost,
         // we would not stop local sdbcm
         var flag = isInLocalHost( ssh ) ;
         if ( flag )
         {
            RET_JSON[HostInfo].push( retObj ) ;
            continue ;
         }

         // remove the packet in remote machine
         removeTmpDir( ssh, osInfo ) ;
      }
      catch ( e )
      {
         retObj[Errno]  = GETLASTERROR( e, true ) ;
         retObj[Detail] = GETLASTERRMSG() ;
      }
      RET_JSON[HostInfo].push( retObj ) ;
   }

   // return the result
   return RET_JSON ;
}

// execute
   main() ;

