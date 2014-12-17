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
@description: remove the newly created data group
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is:  
   SYS_JSON: the format is: { "VCoordSvcName": "10000", "UninstallGroupNames": [ "group1", "group2" ] }
   ENV_JSON:
@return
   RET_JSON: the format is: {}
*/

var RET_JSON     = new Object() ;
var errMsg       = "" ;
/* *****************************************************************************
@discretion: remove data group
@parameter
   db[object]: Sdb object
   name[string]: data group name
@return void
***************************************************************************** */
function removeGroup( db, name )
{
   var rg = null ;
   // get rg
   try
   {
      rg = db.getRG( name ) ;
   }
   catch ( e )
   {
      if ( SDB_CLS_GRP_NOT_EXIST == e )
      {
         return ;
      }
      else
      {
         errMsg = "Failed to get data group [" + name + "]" ;
         exception_handle( e, errMsg ) ;
      }
   }
   // stop all the data node in this group
   try
   {
      rg.stop() ;
   }
   catch ( e )
   {
      errMsg = "Failed to stop data group [" + name + "]" ;
      exception_handle( e, errMsg ) ;
   } 
   // remove data group
   try
   {
      db.removeRG( name ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to remove data group [" + name + "]" ;
      exception_handle( e, errMsg ) ;
   }
}

function removeDataGroup( db, groups )
{
   for ( var i = 0; i < groups.length; i++ )
   {
      removeGroup( db, groups[i] ) ;
   }
}

function main()
{
   var vCoordHostName   = System.getHostName() ;
   var vCoordSvcName    = SYS_JSON[VCoordSvcName] ;
   var groups           = SYS_JSON[UninstallGroupNames] ;
   var db               = null ;
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
   // test whether catalog is running or not
   // if catalog is not running, no need to rollback data group
   var flag = isCatalogRunning( db ) ;
   if ( !flag )
   {
      return RET_JSON ;
   }
   // remove data group
   removeDataGroup( db, groups ) ;

   return RET_JSON ;

}

// execute
   main() ;

