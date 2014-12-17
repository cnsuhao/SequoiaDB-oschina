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
@description: create coord
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "InstallHostName": "rhel64-test8", "InstallSvcName": "11810", "InstallPath": "/opt/sequoiadb/database/coord", "InstallConfig": { "diaglevel": 3, "role": "coord", "logfilesz": 64, "logfilenum": 20, "transactionon": "false", "preferedinstance": "A", "numpagecleaners": 1, "pagecleaninterval": 10000, "hjbuf": 128, "logbuffsize": 1024, "maxprefpool": 200, "maxreplsync": 10, "numpreload": 0, "sortbuf": 512, "syncstrategy": "none" } } ;
   SYS_JSON: the format is:  { "VCoordSvcName": "10000", "SdbUser": "sdbadmin", "SdbPasswd": "sdbadmin", "SdbUserGroup": "sdbadmin_group", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" } ;
@return
   RET_JSON: the format is: {}
*/

var RET_JSON     = new Object() ;
var errMsg       = "" ;
/* *****************************************************************************
@discretion: create coord
@parameter
   db[object]: Sdb object
   hostName[string]: install host name
   svcName[string]: install svc name
   installPath[string]: install path
   config[json]: config info
@return void
***************************************************************************** */
function createCoordNode( db, hostName, svcName, installPath, config )
{
   var rg = null ;
   var node = null ;
   try
   {
      rg = db.getRG( OMA_SYS_COORD_RG ) ;
   }
   catch ( e )
   {
      if ( SDB_CLS_GRP_NOT_EXIST == e )
      {
         try
         {
            // create coord replica group
            rg = db.createCoordRG() ;
         }
         catch ( e )
         {
            errMsg = "Failed to create coord group" ;
            exception_handle( e, errMsg ) ;
         }
      }
      else
      {
         errMsg = "Failed to get coord group" ;
         exception_handle( e, errMsg ) ;
      }
   }
   // create coord node
   try
   {
      node = rg.createNode( hostName, svcName, installPath, config ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to create coord node [" + hostName + ":" + svcName + "]" ;
      exception_handle( e, errMsg ) ;
   }
   // start coord node
   try
   {
      node.start() ;
   }
   catch ( e )
   {
      errMsg = "Failed to start coord node [" + hostName + ":" + svcName + "]" ;
      exception_handle( e, errMsg ) ;
   }
}

function main()
{
   var vCoordHostName  = System.getHostName() ;
   var vCoordSvcName   = SYS_JSON[VCoordSvcName] ;
   var sdbUser         = SYS_JSON[SdbUser] ;
   var sdbUserGroup    = SYS_JSON[SdbUserGroup] ;
   var user            = SYS_JSON[User] ;
   var passwd          = SYS_JSON[Passwd] ;
   var sshport         = parseInt(SYS_JSON[SshPort]) ;
   var installHostName = BUS_JSON[InstallHostName] ;
   var installSvcName  = BUS_JSON[InstallSvcName] ;
   var installPath     = BUS_JSON[InstallPath] ;
   var installConfig   = BUS_JSON[InstallConfig] ;
   var db              = null ;
   var ssh             = new Ssh( installHostName, user, passwd, sshport ) ;
   var osInfo          = System.type() ;
   // change install path owner
   changeDirOwner( ssh, osInfo, installPath, sdbUser, sdbUserGroup ) ;
   // connect to virtual coord
   try
   {
      db = new Sdb( vCoordHostName, vCoordSvcName, "", "" ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to connect to temporary coord [" + vCoordHostName + ":" + vCoordSvcName  + "]" ;
      exception_handle( e, errMsg ) ;
   }
   // create coord node
   createCoordNode( db, installHostName, installSvcName,
                    installPath, installConfig ) ;
   return RET_JSON ;
}

// execute
   main() ;

