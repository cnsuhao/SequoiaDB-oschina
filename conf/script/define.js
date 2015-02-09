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
@description: global value define
@modify list:
   2014-7-26 Zhaobo Tan  Init
*/

// global
var SYS_LINUX = "LINUX" ;
var SYS_WIN   = "WINDOWS" ;
var SYS_TYPE  = System.type() ;

if ( "LINUX" != SYS_TYPE && "WINDOWS" != SYS_TYPE )
{
   throw new Error("Failed to get system type") ;
}

// fields
var AgentPort                    = "AgentService" ;
var AgentService                 = "AgentService" ;
var AuthUser                     = "AuthUser" ;
var AuthPasswd                   = "AuthPasswd" ;
var Bandwidth                    = "Bandwidth" ;
var Bit                          = "Bit" ;
var CataAddr                     = "CataAddr" ;
var CataSvcName                  = "CataSvcName" ;
var CatalogAddr                  = "catalogaddr" ;
var CanPing                      = "Ping" ;
var CanSsh                       = "Ssh" ;
var Core                         = "Core" ;
var Context                      = "Context" ;
var CPU                          = "CPU" ;
var Cpus                         = "Cpus" ;
var CanUse                       = "CanUse" ;
var Detail                       = "detail" ;
var DefaultPort                  = "defaultPort" ;
var Description                  = "description" ;
var Disk                         = "Disk" ;
var Disks                        = "Disks" ;
var Distributor                  = "Distributor" ;
var Free                         = "Free" ;
var Freq                         = "Freq" ;
var Filesystem                   = "Filesystem" ;
var HostName                     = "HostName" ;
var HasInstall                   = "HasInstall" ;
var HasInstalled                 = "HasInstalled" ;
var HasUninstall                 = "HasUninstall" ;
var HostInfo                     = "HostInfo" ;
var Hosts                        = "Hosts" ;
var MD5                          = "MD5" ;
var Memory                       = "Memory" ;
var Model                        = "Model" ;
var Mount                        = "Mount" ;
var Name                         = "Name" ;
var Net                          = "Net" ;
var Netcards                     = "Netcards" ;
var ID                           = "ID" ;
var IP                           = "IP" ;
var Ip                           = "Ip" ;
var Info                         = "Info" ;
var IsLocal                      = "IsLocal" ;
var IsNeedUninstall              = "IsNeedUninstall" ;
var LocalHost                    = "localhost" ;
var IsRunning                    = "IsRunning" ;
var IsOMStop                     = "IsOMStop" ;
var ISPROGRAMEXIST               = "ISPROGRAMEXIST" ;
var INSTALL_DIR                  = "INSTALL_DIR" ;
var InstallConfig                = "InstallConfig" ;
var InstallGroupName             = "InstallGroupName" ;
var InstallGroupNames            = "InstallGroupNames" ;
var InstallHostName              = "InstallHostName" ;
var InstallPacket                = "InstallPacket" ;
var InstallPath                  = "InstallPath" ;
var InstallSvcName               = "InstallSvcName" ;
var OS                           = "OS" ;
var OM                           = "OM" ;
var OMA                          = "OMA" ;
var OmaHostName                  = "OmaHostName" ;
var OmaSvcName                   = "OmaSvcName" ;
var Path                         = "Path" ;
var Passwd                       = "Passwd" ;
var ProgPath                     = "ProgPath" ;
var Port                         = "Port" ;
var PrimaryNode                  = "PrimaryNode" ;
var Rc                           = "Rc" ;
var Errno                        = "errno" ;
var Reachable                    = "Reachable" ;
var Result                       = "Result" ;
var Release                      = "Release" ;
var Safety                       = "Safety" ;
var SdbUserGroup                 = "SdbUserGroup" ;
var SdbPasswd                    = "SdbPasswd" ;
var SdbUser                      = "SdbUser" ;
var Service                      = "Service" ;
var Size                         = "Size" ;
var SshPort                      = "SshPort" ;
var Status                       = "Status" ;
var Task                         = "task" ;
var TaskID                       = "TaskID" ;
var TaskLog                      = "TaskLog" ;
var TmpCoordHostName             = "TmpCoordHostName" ;
var TmpCoordSvcName              = "TmpCoordSvcName" ;
var Time                         = "Time" ;
var VCoordHostName               = "TmpCoordHostName" ;
var VCoordSvcName                = "TmpCoordSvcName" ;
var Usable                       = "Usable" ;
var Used                         = "Used" ;
var User                         = "User" ;
var UninstallGroupName           = "UninstallGroupName" ;
var UninstallGroupNames          = "UninstallGroupNames" ;
var UninstallHostName            = "UninstallHostName" ;
var UninstallSvcName             = "UninstallSvcName" ;
var Version                      = "Version" ;
var SDBADMIN_USER                = "SDBADMIN_USER" ;
var SvcName                      = "SvcName" ;
var SvcName2                     = "svcname" ;
var Sys                          = "Sys" ;
var Idle                         = "Idle" ;
var Other                        = "Other" ;
var CalendarTime                 = "CalendarTime" ;
var NetCards                     = "NetCards" ;


var OMA_PATH_TEMP_OMA_DIR_L           = "/tmp/omatmp/" ;
var OMA_PATH_TEMP_OMA_DIR_L2          = "/tmp/omatmp" ;
var OMA_PATH_TEMP_BIN_DIR_L           = "/tmp/omatmp/bin/" ;
var OMA_PATH_TEMP_PACKET_DIR_L        = "/tmp/omatmp/packet/" ;
var OMA_PATH_TEMP_CONF_DIR_L          = "/tmp/omatmp/conf/" ;
var OMA_PATH_TEMP_DATA_DIR            = "/tmp/omatmp/data/" ;
var OMA_PATH_TMP_WEB_DIR              = "/tmp/omatmp/web/" ;
var OMA_PATH_TEMP_TEMP_DIR_L          = "/tmp/omatmp/tmp/" ;
var OMA_PATH_TEMP_LOG_DIR_L           = "/tmp/omatmp/conf/log/" ;
var OMA_PATH_TEMP_LOCAL_DIR_L         = "/tmp/omatmp/conf/local/" ;
var OMA_PATH_TEMP_SPT_DIR_L           = "/tmp/omatmp/conf/script/" ;
var OMA_PATH_TMP_COORD_PATH           = "/tmp/omatmp/data/tmpCoord/" ;
var OMA_PATH_TMP_COORD_BACKUP_DIR     = "/tmp/omatmp/data/tmpCoord/backup/" ;
var OMA_PATH_TMP_WEB_LOG_DIR          = "/tmp/omatmp/web/log/" ;
var OMA_PATH_SCRIPT_L                 = "script/" ;
var OMA_PATH_BIN_L                    = "bin/";

// file in linux
var OMA_FILE_TEMP_HOSTS_TABLE_L      = OMA_PATH_TEMP_TEMP_DIR_L + "hosts" ;
var OMA_FILE_TEMP_ADD_HOST_CHECK     = OMA_PATH_TEMP_TEMP_DIR_L + "addHostPreCheckResult" ;
var OMA_FILE_HOSTS_TABLE_L           = "/etc/hosts" ;
var OMA_FILE_UPDATE_HOSTS_L          = "conf/script/updateHosts.js" ;
var OMA_FILE_SDBCM_CONF2_L           = "conf/sdbcm.conf" ;
var OMA_FILE_SDBCM_CONF              = "sdbcm.conf" ;
var OMA_FILE_ERROR                   = "error.js" ;
var OMA_FILE_LOG                     = "log.js" ;
var OMA_FILE_COMMON                  = "common.js" ;
var OMA_FILE_DEFINE                  = "define.js" ;
var OMA_FILE_FUNC                    = "func.js" ;
var OMA_FILE_CHECK_HOST_ITEM         = "checkHostItem.js" ;
var OMA_FILE_CHECK_HOST              = "checkHost.js" ;
var OMA_FILE_ADD_HOST_PRE_CHECK      = "addHostPreCheck.js" ;
var OMA_FILE_INSTALL_INFO            = "/etc/default/sequoiadb" ;



// program in linux
var OMA_PROG_SDBCM_L                  = "sdbcm" ;
var OMA_PROG_BIN_SDB_L                = "bin/sdb" ;  
var OMA_PROG_BIN_SDBCM_L              = "bin/sdbcm" ;
var OMA_PROG_BIN_SDBLIST_L            = "bin/sdblist" ;
var OMA_PROG_BIN_SDBOMTOOL_L          = "bin/sdbomtool" ;
var OMA_PROG_SDB                      = "sdb" ;
var OMA_PROG_SDBCMD_L                 = "sdbcmd" ;
var OMA_PROG_SDBCMART_L               = "sdbcmart" ;
var OMA_PROG_SDBCMTOP_L               = "sdbcmtop" ;
var OMA_PROG_SDBLIST_L                = "sdblist" ;
var OMA_PROG_UNINSTALL_L              = "uninstall" ;

// misc
var OMA_MISC_HOSTS_TABLE_PROMPT1      = "#############" ;
var OMA_MISC_HOSTS_TABLE_PROMPT2      = " add by sdbcm " ;
var OMA_MISC_CONFIG_PORT              = "_Port" ;
var OMA_MISC_OM_VERSION               = "version: " ;
var OMA_MISC_OM_RELEASE               = "Release: " ;

// port
var OMA_PORT_DEFAULT_SDBCM_PORT       = 11790 ;
var OMA_PORT_MAX                      = 65535 ;
var OMA_PORT_INVALID                  = -1 ;
var OMA_PORT_TEMP_AGENT_PORT          = 10000 ;
var OMA_RESERVED_PORT = [ 11790, [11800, 11804], [11810, 11814], [11820, 11824], 30000, 50000, 60000 ] ;
// option
var OMA_OPTION_SDBCMART_I             = "--I" ;
var OMA_OPTION_SDBCMART_PORT          = "--port" ;
var OMA_OPTION_SDBCMART_STANDALONE    = "--standalone" ;
var OMA_OPTION_SDBCMART_ALIVETIME     = "--alivetime" ;

// other
var OMA_NEW_LINE                      = "\n" ;
var OMA_NEW_LINE_L                    = "\n" ;
var OMA_SYS_CATALOG_RG                = "SYSCatalogGroup" ;
var OMA_SYS_COORD_RG                  = "SYSCoord" ;
var OMA_LINUX                         = "LINUX" ;
var OMA_WINDOWS                       = "WINDOWS" ;
var OMA_TMP_SDBCM_ALIVE_TIME          = 300 // sec
var OMA_SLEEP_TIME                    = 500 ; // ms
var OMA_TRY_TIMES                     = 6 ;
var OMA_WAIT_CATA_RG_TRY_TIMES        = 600 ;
var OMA_GTE_VERSION_TIME              = 10000 // ms

