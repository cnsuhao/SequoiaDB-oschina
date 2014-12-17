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
@description: create data node
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "InstallGroupName": "group1", "InstallHostName": "rhel64-test8", "InstallSvcName": "51000", "InstallPath": "/opt/sequoiadb/database/data/51000", "InstallConfig": { "diaglevel": 3, "role": "data", "logfilesz": 64, "logfilenum": 20, "transactionon": "false", "preferedinstance": "A", "numpagecleaners": 1, "pagecleaninterval": 10000, "hjbuf": 128, "logbuffsize": 1024, "maxprefpool": 200, "maxreplsync": 10, "numpreload": 0, "sortbuf": 512, "syncstrategy": "none" } } ;
   SYS_JSON: the format is: { "VCoordSvcName": "10000", "SdbUser": "sdbadmin", "SdbPasswd": "sdbadmin", "SdbUserGroup": "sdbadmin_group", "User": "root", "Passwd": "sequoiadb", "SshPort": "22" } ;
@return
   RET_JSON: the format is: {}
*/

var RET_JSON     = new Object() ;
var errMsg       = "" ;
/* *****************************************************************************
@discretion: create data node
@parameter
   db[object]: Sdb object
   hostName[string]: install host name
   svcName[string]: install svc name
   installGroup[string]: install group name
   installPath[string]: install path
   config[json]: config info
@return void
***************************************************************************** */
function createDataNode( db, hostName, svcName,
                         installGroup, installPath, config )
{
   var rg     = null ;
   var node   = null ;
   try
   {
      rg = db.getRG( installGroup ) ;
   }
   catch ( e )
   {
      if ( SDB_CLS_GRP_NOT_EXIST == e )
      {
         try
         {
            // create coord replica group
            rg = db.createRG( installGroup ) ;
         }
         catch ( e )
         {
            errMsg = "Failed to create data group [" + installGroup + "]" ;
            exception_handle( e, errMsg ) ;
         }
      }
      else
      {
         errMsg = "Failed to get data group [" + installGroup + "]" ;
         exception_handle( e, errMsg ) ;
      }
   }
   // create data node
   try
   {
      node = rg.createNode( hostName, svcName, installPath, config ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to create data node [" + hostName + ":" + svcName + "] in group [" + installGroup + "]" ;
      exception_handle( e, errMsg ) ;
   }
   // start data node
   try
   {
      node.start() ;
   }
   catch ( e )
   {
      errMsg = "Failed to start data node [" + hostName + ":" + svcName + "] in group [" + installGroup + "]" ;
      exception_handle( e, errMsg ) ;
   }
   // try to start data group
   try
   {
      var obj = eval ( '(' + rg.getDetail().next() + ')' ) ;
      var s = obj[Status] ;
      if ( "number" == typeof( s ) )
      {
         if ( s > 0 )
         {
            return ;
         }
         else
         {
            rg.start() ;
         }
      }
      else
      {
         // start the data group anyway
         rg.start() ;
      }
   }
   catch ( e )
   {
      errMsg = "Failed to start data group [" + installGroup + "]" ;
      exception_handle( e, errMsg ) ;
   }
}

function main()
{
   var vCoordHostName   = System.getHostName() ;
   var vCoordSvcName    = SYS_JSON[VCoordSvcName] ;
   var sdbUser          = SYS_JSON[SdbUser] ;
   var sdbUserGroup     = SYS_JSON[SdbUserGroup] ;
   var user             = SYS_JSON[User] ;
   var passwd           = SYS_JSON[Passwd] ;
   var sshport          = parseInt(SYS_JSON[SshPort]) ;
   var installHostName  = BUS_JSON[InstallHostName] ;
   var installSvcName   = BUS_JSON[InstallSvcName] ;
   var installGroupName = BUS_JSON[InstallGroupName] ;
   var installPath      = BUS_JSON[InstallPath] ;
   var installConfig    = BUS_JSON[InstallConfig] ;

   var ssh              = new Ssh( installHostName, user, passwd, sshport ) ;
   var osInfo           = System.type() ;
   var db               = null ;
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
   // create data node
   createDataNode( db, installHostName, installSvcName,
                   installGroupName, installPath, installConfig ) ;
   return RET_JSON ;
}

// execute
   main() ;

