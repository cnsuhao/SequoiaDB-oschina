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
@description: create temporary coord
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "CataAddr":[] } or  { "CataAddr":[ { "HostName":"rhel64-test8", "CataSvcName":"11803" }, { "HostName":"rhel64-test9", "CataSvcName":"11803" }, { "HostName":"rhel64-test9", "CataSvcName":"11903" } ] }
@return
   RET_JSON: the format is: { "Port", "10000" }
*/

var RET_JSON             = new Object() ;
var errMsg               = "" ;
RET_JSON[VCoordSvcName]  = "" ;

/* *****************************************************************************
@discretion: get catalog address for creating virtual coord
@parameter
   addrInfo[json]: address info object
@return
   retObj[json]: the return catalog address, e.g.
                 { "catalogaddr" : "rhel64-test8:11803,rhel64-test9:11803" }
***************************************************************************** */
function getCatalogAddress( addrInfo )
{
   var retObj = new Object() ;
   var infoArr = addrInfo[CataAddr] ;
   var len = infoArr.length ;
   var addr = "" ;
   if ( 0 == len )
   {
      return retObj ;
   }
   for( var i = 0; i < len; i++ )
   {
      var obj = infoArr[i] ;
      var hostname = obj[HostName] ;
      var svcname = obj[CataSvcName] ;
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
   var oma           = null ;
   var osInfo        = null ;
   var omaHostName   = null ;
   var omaSvcName    = null ;
   var vCoordSvcName = null ;
   var cataAddr      = null ;
   var dataPath      = null ;

   try
   {
      osInfo        = System.type() ;
      omaHostName   = System.getHostName() ;
      omaSvcName    = Oma.getAOmaSvcName( "localhost" ) ;
      vCoordSvcName = getAUsablePortFromLocal( osInfo ) + "" ;
      cataAddr      = getCatalogAddress( BUS_JSON ) ;
      dataPath      = OMA_PATH_VCOORD_PATH_L + vCoordSvcName ;
      oma           = new Oma( omaHostName, omaSvcName ) ;
      // create virtual coord
      oma.createCoord( vCoordSvcName, dataPath, cataAddr ) ;
      // start virtual coord
      oma.startNode( vCoordSvcName ) ;
      // close connection
      oma.close() ;
      oma = null ;
      RET_JSON[VCoordSvcName] = vCoordSvcName ;
   }
   catch ( e )
   {
      if ( null != oma && "undefined" != typeof(oma) )
      {
         try
         {
            oma.removeCoord( vCoordSvcName ) ;
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
      errMsg = "Failed to create or start temporary coord in local host" ;
      exception_handle( e, errMsg ) ;
   }
   return RET_JSON ;
}

// execute
   main() ;

