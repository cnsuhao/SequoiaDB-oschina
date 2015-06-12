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
@description: install temporary coord
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is:
      { "InstallConfig":{ "clustername":"c2", "businessname":"b2", "usertag":"tmpCoord" }, "CataAddr":[] }
      or
      { "InstallConfig":{ "clustername":"c1", "businessname":"b1", "usertag":"tmpCoord" }, "CataAddr":[ { "HostName":"suse", "SvcName":"11803" }, { "HostName":"rhel64-test8", "SvcName":"11803" }, { "HostName":"rhel64-test9", "SvcName":"11803" } ] } ;
   SYS_JSON: the format is: { "TaskID" : 5 }
@return
   RET_JSON: the format is: { "Port", "10000" }
*/

// println
// var BUS_JSON = { "clustername": "c1", "businessname": "b1", "usertag": "tmpCoord", "CataAddr": [] };

// var SYS_JSON = { "TaskID": 17 };

//var BUS_JSON = { "clustername": "c1", "businessname": "b1", "usertag": "tmpCoord", "CataAddr": [ { "HostName": "rhel64-test8", "SvcName": "60003" }, { "HostName": "rhel64-test9", "SvcName": "61003" } ] };
//var SYS_JSON = { "TaskID": 2 }


var FILE_NAME_INSTALL_TEMPORARY_COORD = "installTmpCoord.js" ;
var RET_JSON = new installTmpCoordResult() ;
var rc       = SDB_OK ;
var errMsg   = "" ;

var task_id  = "" ;
var tmp_coord_install_path = "" ;

/* *****************************************************************************
@discretion: init
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _init()
{
   // get task id
   task_id = getTaskID( SYS_JSON ) ;

   PD_LOG( arguments, PDEVENT, FILE_NAME_INSTALL_TEMPORARY_COORD,
           sprintf( "Begin to install temporary coord for task[?]", task_id ) ) ;
}

/* *****************************************************************************
@discretion: final
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _final()
{
   PD_LOG( arguments, PDEVENT, FILE_NAME_INSTALL_TEMPORARY_COORD,
           sprintf( "Finish installing temporary coord for task[?]", task_id ) ) ;
}

/* *****************************************************************************
@discretion: get catalog address for installing temporary coord
@parameter
   cfgInfo[json]: catalog cfg info object
@return
   retObj[json]: the return catalog address, e.g.
                 { "clustername": "c1", "businessname": "b1", "usertag":     "tmpCoord","catalogaddr" : "rhel64-test8:11803,rhel64-test9:11803" }
***************************************************************************** */
function _getCatalogCfg( cfgInfo )
{
   var retObj           = new Object() ;
   var addrArr          = [] ;
   var len              = 0 ;
   var addr             = "" ;
   var str              = "" ;
   
   // get configure information
   retObj[ClusterName]  = cfgInfo[ClusterName] ;
   retObj[BusinessName] = cfgInfo[BusinessName] ;
   retObj[UserTag]      = cfgInfo[UserTag] ;
   retObj[CatalogAddr]  = "" ;
   addrArr              = cfgInfo[CataAddr] ;
   len                  = addrArr.length ;
   
   // check info
   PD_LOG( arguments, PDDEBUG, FILE_NAME_INSTALL_TEMPORARY_COORD,
           sprintf( "clustername[?], businessname[?], usertag[?]",
                    retObj[ClusterName], retObj[BusinessName], retObj[UserTag] ) ) ;
                    
   if ( "undefined" == typeof(retObj[ClusterName]) ||
        "undefined" == typeof(retObj[BusinessName]) ||
        "undefined" == typeof(retObj[UserTag]) )
   {
      errMsg = "Invalid configure information for installing temporary coord" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_INSTALL_TEMPORARY_COORD,
              sprintf( errMsg + " : clustername[?], businessname[?], usertag[?]",
                       retObj[ClusterName], retObj[BusinessName], retObj[UserTag] ) ) ;
      exception_handle( SDB_INVALIDARG, errMsg ) ;
   }
   if ( 0 == len )
   {
      return retObj ;
   }
   // get catalog address
   for( var i = 0; i < len; i++ )
   {
      var obj = addrArr[i] ;
      var hostname = obj[HostName] ;
      var svcname = obj[SvcName] ;
      if ( 0 == i )
      {
         addr = hostname + ":" + svcname ;
      }
      else
      {
         addr += "," + hostname + ":" + svcname ;
      }
   }
   retObj[CatalogAddr] = addr ;
   return retObj ;
}

function main()
{
   var oma                    = null ;
   var omaHostName            = null ;
   var omaSvcName             = null ;
   var tmpCoordSvcName        = null ;
   var installInfoObj         = null ;
   var dbInstallPath          = null ;
   var cfgObj                 = null ;

   _init() ;
   
   try
   {
      // 1. get install temporary coord arguments
      try
      {
         omaHostName     = System.getHostName() ;
         omaSvcName      = Oma.getAOmaSvcName( "localhost" ) ;
         tmpCoordSvcName = getAUsablePortFromLocal() + "" ;
         installInfoObj  = eval( '(' + Oma.getOmaInstallInfo() + ')' ) ;
         dbInstallPath   = adaptPath( installInfoObj[INSTALL_DIR] ) ;
         tmp_coord_install_path = dbInstallPath + "database/tmpCoord/" + tmpCoordSvcName ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         rc = GETLASTERROR() ;
         errMsg = "Failed to get arguments for installing temporary coord" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_INSTALL_TEMPORARY_COORD,
                 sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
         exception_handle( rc, errMsg ) ;
      }
      
      // 2. get catalog address
      try
      {
         cfgObj = _getCatalogCfg( BUS_JSON ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         rc = GETLASTERROR() ;
         errMsg = "Failed to get catalog's address" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_INSTALL_TEMPORARY_COORD,
                 sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
         exception_handle( rc, errMsg ) ;
      }
      
      // 3. connet to OM Agent in local host
      try
      {
         oma = new Oma( omaHostName, omaSvcName ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         rc = GETLASTERROR() ;
         errMsg = "Failed to connect to OM Agent in local host" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_INSTALL_TEMPORARY_COORD,
                 sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
         exception_handle( rc, errMsg ) ;
      }

      // 4. create temporary coord
      try
      {
         PD_LOG( arguments, PDDEBUG, FILE_NAME_INSTALL_TEMPORARY_COORD,
                 sprintf( "Create temporary coord passes arguments: svc[?], path[?], cfgObj[?]",
                          tmpCoordSvcName, tmp_coord_install_path, JSON.stringify(cfgObj) ) ) ;
         oma.createCoord( tmpCoordSvcName, tmp_coord_install_path, cfgObj ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         rc = GETLASTERROR() ;
         errMsg = "Failed to create temporary coord" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_INSTALL_TEMPORARY_COORD,
                 sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
         exception_handle( rc, errMsg ) ;
      }
      // 5. start temporary coord
      try
      {
         oma.startNode( tmpCoordSvcName ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         rc = GETLASTERROR() ;
         errMsg = "Failed to start temporary coord" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_INSTALL_TEMPORARY_COORD,
                 sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
         exception_handle( rc, errMsg ) ;
      }
      // 6. close connection and return the port of temporary coord
      oma.close() ;
      oma = null ;
      RET_JSON[TmpCoordSvcName] = tmpCoordSvcName ;
   }
   catch ( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = "Failed to install temporary coord in local host" ;
      rc = GETLASTERROR() ;
      PD_LOG( arguments, PDERROR, FILE_NAME_INSTALL_TEMPORARY_COORD,
              sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
      if ( null != oma && "undefined" != typeof(oma) )
      {
         try
         {
            oma.removeCoord( tmpCoordSvcName ) ;
         }
         catch ( e1 )
         {
         }
         try
         {
            oma.close() ;
            oma = null ;
         }
         catch ( e2 )
         {
         }
      }
      RET_JSON[Errno]  = rc ;
      RET_JSON[Detail] = errMsg ;
   }

   _final() ;
println( "RET_JSON is: " + JSON.stringify(RET_JSON) ) ;
   return RET_JSON ;
}

// execute
   main() ;

