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
@description: remove temporary coord
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   SYS_JSON: the format is: { "VCoordSvcName": "10000" }
   ENV_JSON:
@return
   RET_JSON: the format is: {}
*/

var RET_JSON     = new Object() ;
var errMsg       = "" ;

/* *****************************************************************************
@discretion: backup temporary coord's diaglog
@parameter
   svcName[string]: install svc name
@return void
***************************************************************************** */

function backupVCoordDiaglog ( svcName )
{
   var src = OMA_PATH_TMP_COORD_PATH + svcName + "/diaglog/sdbdiag.log" ;
   var dst = OMA_PATH_TMP_COORD_BACKUP_DIR + "/diaglog/sdbdiag.log" + "." + genTimeStamp() ;
   // mkdir director
   File.mkdir( OMA_PATH_TMP_COORD_BACKUP_DIR + "diaglog/" ) ;
   // backup sdbdiag.log
   File.copy( src, dst ) ;
}

function main()
{
   var omaHostName    = System.getHostName() ;
   var omaSvcName     = Oma.getAOmaSvcName( "localhost" ) ;
   var vCoordHostName = omaHostName ;
   var vCoordSvcName  = SYS_JSON[VCoordSvcName] ;
   var oma = null ;
   try
   {
      // new oma object
      var oma = new Oma( omaHostName, omaSvcName ) ;
      
      // backup virtual coord dialog
      backupVCoordDiaglog( vCoordSvcName ) ;

      // stop virtual coord
      oma.stopNode( vCoordSvcName ) ;

      // remomve virtual coord
      oma.removeCoord( vCoordSvcName ) ;
   
      // close connection
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
            oma = null ;
         }
         catch ( e )
         {
         }
      }
      errMsg = "Failed to remove temporary coord [" + vCoordHostName + ":" + vCoordSvcName  + "] in local host" ;
      exception_handle( e, errMsg ) ;
   }
   return RET_JSON ;
}

// execute
   main() ;

