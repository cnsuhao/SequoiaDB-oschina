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
@description: get host info
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the info for check host: { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sequoiadb" } ;
   SYS_JSON:
   ENV_JSON:
@return
   RET_JSON: the check host result: {"IP":"192.168.20.165","HostName":"rhel64-test8","OS":{"Distributor":"RedHatEnterpriseServer","Release":"6.4","Bit":64},"OM":{"HasInstalled":true,"Version":"1.8","Path":"/opt/sequoiadb/bin/","Port":"11790","Release":15348},"CPU":[{"ID":"","Model":"","Core":2,"Freq":"2.00GHz"}],"Memory":{"Model":"","Size":2887,"Free":174},"Disk":[{"Name":"/dev/mapper/vg_rhel64test8-lv_root","Mount":"/","Size":43659,"Free":35065,"IsLocal":false},{"Name":"/dev/sda1","Mount":"/boot","Size":460,"Free":423,"IsLocal":true},{"Name":"//192.168.20.10/files","Mount":"/mnt","Size":47836,"Free":29332,"IsLocal":false}],"Net":[{"Name":"lo","Model":"","Bandwidth":"","IP":"127.0.0.1"},{"Name":"eth0","Model":"","Bandwidth":"","IP":"192.168.20.165"}],"Port":[{"Port":"","CanUse":false}],"Service":[{"Name":"","IsRunning":false,"Version":""}],"Safety":{"Name":"","Context":"","IsRunning":false}} 
*/

var RET_JSON = new Object() ;

RET_JSON[IP]         = "" ;
RET_JSON[HostName]   = "" ; 
RET_JSON[OS]         = "" ;
RET_JSON[OM]         = "" ;
RET_JSON[CPU]        = "" ;
RET_JSON[Memory]     = "" ;
RET_JSON[Disk]       = "" ;
RET_JSON[Net]        = "" ;
RET_JSON[Port]       = "" ;
RET_JSON[Service]    = "" ;
RET_JSON[Safety]     = "" ;

var errMsg           = "" ;

// os info
function getOSInfo()
{
   var obj             =  eval( '(' + System.getReleaseInfo() + ')' ) ;
   var osInfo          = new OSInfo() ;
   osInfo[Distributor] = obj[Distributor] ;
   osInfo[Release]     = obj[Release] ;
   osInfo[Bit]         = obj[Bit] ;
   RET_JSON[OS]        = osInfo ;
}

// om status and version
function getOMInfo()
{
   var omInfo = new OMInfo() ;
   try
   {
      var obj = Oma.getOmaInstallInfo() ;
      // when has installed
      omInfo[HasInstalled] = true ;
      // get other info
      var info = extractOMInfo( eval( '(' + obj + ')' ) ) ;
      omInfo[Version] = info[Version] ;
      omInfo[Path]    = info[Path] ;
      omInfo[Port]    = info[Port] ;
      omInfo[Release] = info[Release] ;
      RET_JSON[OM]    = omInfo ;
   }
   catch ( e )
   {
      // when has not installed
      if ( SDB_FNE == e )
      {
         omInfo[HasInstalled] = false ;
         RET_JSON[OM] = omInfo ;
      }
      else if ( SDB_TIMEOUT == e )
      {
         errMsg = "Incompatible versions" ;
         exception_handle( e, errMsg ) ;
      }
      else
      {
         throw e ;
      }
   }
}

// memory
function getMemInfo()
{
   var obj          = eval( '(' + System.getMemInfo() + ')' ) ;
   var memInfo      = new MemoryInfo() ;
   // TODO: model is not offer
   memInfo[Model]   = "" ;
   memInfo[Size]    = obj[Size] ;
   memInfo[Free]    = obj[Free] ;
   RET_JSON[Memory] = memInfo ;
}

// disk
function getDiskInfo()
{
   var objs      = eval( '(' + System.getDiskInfo() + ')' ) ;
   var arr       = objs[Disks] ;
   var diskInfos = [] ;
   for ( var i = 0; i < arr.length; i++ )
   {
      var obj           = arr[i] ;
      var diskInfo      = new DiskInfo() ;
      diskInfo[Name]    = obj[Filesystem] ;
      diskInfo[Mount]   = obj[Mount] ;
      diskInfo[Size]    = obj[Size] ;
      diskInfo[Free]    = obj[Size] - obj[Used] ;
      diskInfo[IsLocal] = obj[IsLocal] ;
      diskInfos.push( diskInfo ) ;
   }
   RET_JSON[Disk] = diskInfos ;
}

// cpu
function getCPUInfo()
{
   var objs     = eval( '(' + System.getCpuInfo() + ')' ) ;
   var arr      = objs[Cpus] ;
   var cpuInfos = [] ;
   for ( var i = 0; i < arr.length; i++ )
   {
      var obj        = arr[i] ;
      var cpuInfo    = new CPUInfo() ;
      // TODO: not offer ID and Model
      cpuInfo[ID]    = "" ;
      cpuInfo[Model] = obj[Info] ;
      cpuInfo[Core]  = obj[Core] ;
      cpuInfo[Freq]  = obj[Freq] ;
      cpuInfos.push( cpuInfo ) ;
   }
   RET_JSON[CPU] = cpuInfos ;
}

// net card
function getNetCardInfo()
{
   var objs         = eval( '(' + System.getNetcardInfo() + ')' ) ;
   var arr          = objs[Netcards] ;
   var netcardInfos = [] ;
   for ( var i = 0; i < arr.length; i++ )
   {
      var obj                = arr[i] ;
      var netcardInfo        = new NetInfo() ;
      netcardInfo[Name]      = obj[Name] ;
      // TODO: not offer Model and bandwidth 
      netcardInfo[Model]     = "" ;
      netcardInfo[Bandwidth] = "" ;
      netcardInfo[IP]        = obj[Ip] ;
      netcardInfos.push( netcardInfo ) ;
   }
   RET_JSON[Net] = netcardInfos ;
}

// port status 
function getPortInfo()
{
   // TODO: no any plan yet
   var portInfos = [] ;
   portInfos.push( new PortInfo() ) ;
   RET_JSON[Port] = portInfos ;
}

// service
function getServiceInfo()
{
   // TODO: no any plan yet
   var svcInfos = [] ;
   svcInfos.push( new ServiceInfo() ) ;
   RET_JSON[Service] = svcInfos ;
}

// safety
function getSafetyInfo()
{
   var obj =  eval( '(' + System.getIpTablesInfo() + ')' ) ;
   var safetyInfo = new SafetyInfo() ;
   // TODO: System.getIpTablesInfo does not offer any useful info
   RET_JSON[Safety] = safetyInfo ;
}

// extract OM version, release, path and port, when it has been installed
function extractOMInfo ( obj )
{
   var retObj = new OMInfo() ;
   var osInfo = System.type() ;
   if ( OMA_LINUX == osInfo )
   {
      // get install path
      var installpath = obj[INSTALL_DIR] ;
      retObj[Path] = adaptPath( osInfo, installpath ) + OMA_PATH_BIN_L ;
      // get sdbcm port
      var configfile = adaptPath( osInfo, installpath ) + OMA_FILE_SDBCM_CONF2_L ;
      try
      {
         retObj[Port] = "" + Oma.getAOmaSvcName("localhost", configfile ) ;
      }
      catch ( e )
      {
         errMsg = "Failed to extract OM's info" ;
         exception_handle( e, errMsg ) ;
      }
      // get version
      var sdbcmprog = adaptPath( osInfo, installpath ) + OMA_PROG_BIN_SDBCM_L ;
      var cmd = new Cmd() ;
      var str = null ;
      try
      {
         str = cmd.run( sdbcmprog + " --version ", "", OMA_GTE_VERSION_TIME ) ;
      }
      catch ( e )
      {
         // version 1.8 sp1 need this error msg
         if ( (1 == e) && (null == str) )
         {
            errMsg = "Incompatible versions" ;
            exception_handle( e, errMsg ) ;
         }
         else
         {
            errMsg = "Failed to extract OM's info" ;
            exception_handle( e, errMsg ) ;
         }
      }
      var beg = str.indexOf( OMA_MISC_OM_VERSION ) ;
      var end = str.indexOf( '\n' ) ;
      var len = OMA_MISC_OM_VERSION.length ;
      retObj[Version] = str.substring( beg + len, end ) ;
      // get release
      beg = str.indexOf( OMA_MISC_OM_RELEASE ) ;
      len = OMA_MISC_OM_RELEASE.length ;
      var subStr = str.substring( beg + len, str.length ) ;
      retObj[Release] = parseInt( subStr ) ;
   }
   else
   {
      // TODO:
      // windows 
   }
   return retObj ;
}

function main()
{
   RET_JSON[IP] = BUS_JSON[IP] ;
   RET_JSON[HostName] = BUS_JSON[HostName] ; 
   // get local host info
   getOMInfo() ;
   getOSInfo() ;
   getCPUInfo() ;
   getMemInfo() ;
   getDiskInfo() ;
   getNetCardInfo() ;
   getPortInfo() ;
   getServiceInfo() ;
   getSafetyInfo() ;

   return RET_JSON ;
}

// execute
main() ;

