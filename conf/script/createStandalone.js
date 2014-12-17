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
@description: create standalone
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "InstallHostName": "rhel64-test8", "InstallSvcName": "11820", "InstallPath": "/opt/sequoiadb/database/standalone", "InstallConfig": { "diaglevel": 3, "role": "standalone", "logfilesz": 64, "logfilenum": 20, "transactionon": "false", "preferedinstance": "A", "numpagecleaners": 1, "pagecleaninterval": 10000, "hjbuf": 128, "logbuffsize": 1024, "maxprefpool": 200, "maxreplsync": 10, "numpreload": 0, "sortbuf": 512, "syncstrategy": "none" } }
   SYS_JSON: the format is: { "SdbUser": "sdbadmin", "SdbPasswd": "sdbadmin", "SdbUserGroup": "sdbadmin_group", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" } 
   ENV_JSON:
@return
   RET_JSON: the format is: {}
*/

var RET_JSON        = new Object() ;
var errMsg          = "" ;

/* *****************************************************************************
@discretion: create standalone
@parameter
   hostName[string]: install host name
   svcName[string]: install svc name
   installPath[string]: install path
   config[json]: config info 
   agentPort[string]: the port of sdbcm in install host
@return void
***************************************************************************** */
function createStandalone( hostName, svcName, installPath, config, agentPort )
{
   var oma = null ;
   try
   {
      oma = new Oma( hostName, agentPort ) ;
      oma.createData( svcName, installPath, config ) ;
      oma.startNode( svcName ) ;
      oma.close() ;
      oma = null ;
   }
   catch ( e )
   {
      if ( null != oma && "undefined" != typeof(oma) )
      {
         try
         {
            oma.close() ;
         }
         catch ( e2 )
         {
         }
      }
      errMsg = "Failed to create standalone [" + hostName + ":" + svcName + "]" ;
      exception_handle( e, errMsg ) ;
   }
}

function main()
{
   var sdbUser         = SYS_JSON[SdbUser] ;
   var sdbUserGroup    = SYS_JSON[SdbUserGroup] ;
   var user            = SYS_JSON[User] ;
   var passwd          = SYS_JSON[Passwd] ;    
   var sshport         = parseInt(SYS_JSON[SshPort]) ;
   var installHostName = BUS_JSON[InstallHostName] ;
   var installSvcName  = BUS_JSON[InstallSvcName] ;
   var installPath     = BUS_JSON[InstallPath] ;
   var installConfig   = BUS_JSON[InstallConfig] ;

   var ssh             = new Ssh( installHostName, user, passwd, sshport ) ;
   var osInfo          = System.type() ;

   // get remote or local sdbcm port
   var agentPort       = getAgentPort( installHostName ) ;
   // change install path owner
   changeDirOwner( ssh, osInfo, installPath, sdbUser, sdbUserGroup ) ;
   // create standalone
   createStandalone( installHostName, installSvcName,
                     installPath, installConfig, agentPort ) ;
   return RET_JSON ;
}

// execute
   main() ;
