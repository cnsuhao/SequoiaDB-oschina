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
@description: remove data group
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "AuthUser": "", "AuthPasswd": "", "UninstallGroupNames": ["group1", "group2"] }
   SYS_JSON: the format is: { "TaskID": 1, "TmpCoordSvcName": "10000" }
@return
   RET_JSON: the format is: { "errrno": 0, "detail": "" }
*/

// println
//var BUS_JSON = { "AuthUser": "", "AuthPasswd": "", "UninstallGroupNames": ["group1", "group2"] } ;
//var SYS_JSON = { "TaskID": 1, "TmpCoordSvcName": "10000" } ;


var FILE_NAME_REMOVE_DATA_RG = "removeDataRG.js" ;
var RET_JSON = new removeRGResult() ;
var rc       = SDB_OK ;
var errMsg   = "" ;

var task_id = "" ;
// println
var rg_name = "datagroup" ;

/* *****************************************************************************
@discretion: init
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _init()
{           
   // 1. get task id
   task_id = getTaskID( SYS_JSON ) ;

   setTaskLogFileName( task_id, rg_name ) ;
   
   PD_LOG2( task_id, arguments, PDEVENT, FILE_NAME_REMOVE_DATA_RG,
            sprintf( "Begin to remove data group in task[?]", task_id ) ) ;
}

/* *****************************************************************************
@discretion: final
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _final()
{
   PD_LOG2( task_id, arguments, PDEVENT, FILE_NAME_REMOVE_DATA_RG,
            sprintf( "Finish removing data group in task[?]", task_id ) ) ;
}

function main()
{
   var tmpCoordHostName = null ;
   var tmpCoordSvcName  = null ;
   var authUser         = null ;
   var authPasswd       = null ;
   var groupName        = null ;
   var db               = null ;
   var i                = 0 ;
   
   _init() ;
   
   try
   {
      // 1. get arguments
      try
      {
         tmpCoordHostName = System.getHostName() ;
         tmpCoordSvcName  = SYS_JSON[TmpCoordSvcName] ;
         authUser         = BUS_JSON[AuthUser] ;
         authPasswd       = BUS_JSON[AuthPasswd] ;
         groupNames       = BUS_JSON[UninstallGroupNames] ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = "Js receive invalid argument" ;
         rc = GETLASTERROR() ;
         // record error message in log
         PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_REMOVE_DATA_RG,
                  errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
         // tell to user error happen
         exception_handle( SDB_INVALIDARG, errMsg ) ;
      }
      // 2. connect to temporary coord
      try
      {
         db = new Sdb ( tmpCoordHostName, tmpCoordSvcName, authUser, authPasswd ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = sprintf( "Failed to connect to temporary coord[?:?]",
                           tmpCoordHostName, tmpCoordSvcName ) ;
         rc = GETLASTERROR() ;
         PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_REMOVE_DATA_RG,
                  errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
         exception_handle( rc, errMsg ) ;
      }
      // 3. remove data group
      for ( i = 0; i < groupNames.length; i++ )
      {
         try
         {
            db.removeRG( groupNames[i] ) ;
         }
         catch( e )
         {
            if ( SDB_CLS_GRP_NOT_EXIST == e )
            {
               continue ;
            }
            else
            {
               SYSEXPHANDLE( e ) ;
               errMsg = sprintf( "Failed to remove data group[?]", groupNames[i] ) ;
               rc = GETLASTERROR() ;
               PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_REMOVE_DATA_RG,
                        errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
               exception_handle( rc, errMsg ) ;
            }
         }
      }

   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = GETLASTERRMSG() ; 
      rc = GETLASTERROR() ;
      PD_LOG2( task_id, arguments, PDERROR, FILE_NAME_REMOVE_DATA_RG,
               sprintf( "Failed to remove all the data group, rc:?, detail:?",
                        rc, errMsg ) ) ;          
      RET_JSON[Errno] = rc ;
      RET_JSON[Detail] = errMsg ;
   }

   _final() ;
println("RET_JSON is: " + JSON.stringify(RET_JSON) ) ;
   return RET_JSON ;
}

// execute
   main() ;

