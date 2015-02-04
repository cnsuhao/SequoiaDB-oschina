/*******************************************************************************


   Copyright (C) 2011-2014 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

   Source File Name = omDef.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/15/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OM_DEF_HPP__
#define OM_DEF_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "omagentDef.hpp"

namespace engine
{
   #define OM_TEMPLATE_REPLICA_NUM     "replicanum"
   #define OM_TEMPLATE_DATAGROUP_NUM   "datagroupnum"
   #define OM_TEMPLATE_CATALOG_NUM     "catalognum"
   #define OM_TEMPLATE_COORD_NUM       "coordnum"
   #define OM_TEMPLATE_TRANSACTION     PMD_OPTION_TRANSACTIONON

   #define OM_DBPATH_PREFIX_DATABASE   "database"

   #define OM_CONF_DETAIL_EX_DG_NAME   "datagroupname"

   #define OM_SIZE_MEGABIT                   ( 1024 * 1024 )

   #define OM_INT32_LENGTH                   (20)
   #define OM_INT64_LENGTH                   (20)

   #define  OM_DEFAULT_LOGIN_USER            "admin"
   #define  OM_DEFAULT_LOGIN_PASSWD          "admin"


   #define OM_CONF_DETAIL_DBPATH             PMD_OPTION_DBPATH
   #define OM_CONF_DETAIL_SVCNAME            PMD_OPTION_SVCNAME
   #define OM_CONF_DETAIL_CATANAME           PMD_OPTION_CATANAME
   #define OM_CONF_DETAIL_DIAGLEVEL          PMD_OPTION_DIAGLEVEL
   #define OM_CONF_DETAIL_ROLE               PMD_OPTION_ROLE
   #define OM_CONF_DETAIL_LOGFSIZE           PMD_OPTION_LOGFILESZ
   #define OM_CONF_DETAIL_LOGFNUM            PMD_OPTION_LOGFILENUM
   #define OM_CONF_DETAIL_TRANSACTION        PMD_OPTION_TRANSACTIONON
   #define OM_CONF_DETAIL_PREINSTANCE        PMD_OPTION_PREFINST
   #define OM_CONF_DETAIL_PCNUM              PMD_OPTION_NUMPAGECLEANERS
   #define OM_CONF_DETAIL_PCINTERVAL         PMD_OPTION_PAGECLEANINTERVAL

   #define OM_CONF_DETAIL_DATAGROUPNAME      "datagroupname"

   /*
      OM Field Define
   */
   #define OM_CS_DEPLOY                      "SYSDEPLOY"

   #define OM_CS_DEPLOY_CL_CLUSTER           OM_CS_DEPLOY".SYSCLUSTER"

   #define OM_CLUSTER_FIELD_NAME             "ClusterName"
   #define OM_CLUSTER_FIELD_DESC             "Desc"
   #define OM_CLUSTER_FIELD_SDBUSER          "SdbUser"
   #define OM_CLUSTER_FIELD_SDBPASSWD        "SdbPasswd"
   #define OM_CLUSTER_FIELD_SDBUSERGROUP     "SdbUserGroup"
   #define OM_CLUSTER_FIELD_INSTALLPATH      "InstallPath"

   #define OM_CS_DEPLOY_CL_CLUSTERIDX1       "{name:\"SYSDEPLOY_CLUSTER_IDX1\",key: {"\
                                             OM_CLUSTER_FIELD_NAME":1}, unique: true, enforced: true } "

   #define OM_CS_DEPLOY_CL_HOST              OM_CS_DEPLOY".SYSHOST"

   #define OM_HOST_FIELD_NAME                "HostName"
   #define OM_HOST_FIELD_CLUSTERNAME         OM_CLUSTER_FIELD_NAME
   #define OM_HOST_FIELD_IP                  "IP"
   #define OM_HOST_FIELD_USER                "User"
   #define OM_HOST_FIELD_PASSWORD            "Passwd"
   #define OM_HOST_FIELD_TIME                "Time"
   #define OM_HOST_FIELD_OS                  "OS"

   #define OM_HOST_FIELD_OMA                 "OMA"
   #define OM_HOST_FIELD_OM_HASINSTALL       "HasInstalled"
   #define OM_HOST_FIELD_OM_VERSION          "Version"
   #define OM_HOST_FIELD_OM_PATH             "Path"
   #define OM_HOST_FIELD_OM_PORT             "Port"
   #define OM_HOST_FIELD_OM_RELEASE          "Release"
   
   #define OM_HOST_FIELD_MEMORY              "Memory"

   #define OM_HOST_FIELD_DISK                "Disk"
   #define OM_HOST_FIELD_DISK_NAME           "Name"
   #define OM_HOST_FIELD_DISK_SIZE           "Size"
   #define OM_HOST_FIELD_DISK_MOUNT          "Mount"
   #define OM_HOST_FIELD_DISK_FREE_SIZE      "Free"
   #define OM_HOST_FIELD_DISK_USED           "Used"

   #define OM_HOST_FIELD_CPU                 "CPU"

   #define OM_HOST_FIELD_NET                 "Net"
   #define OM_HOST_FIELD_NET_NAME            "Name"
   #define OM_HOST_FIELD_NET_IP              OM_HOST_FIELD_IP

   #define OM_HOST_FIELD_PORT                "Port"
   #define OM_HOST_FIELD_SERVICE             "Service"
   #define OM_HOST_FIELD_SAFETY              "Safety"
   #define OM_HOST_FIELD_INSTALLPATH         OM_CLUSTER_FIELD_INSTALLPATH
   #define OM_HOST_FIELD_AGENT_PORT          "AgentService"
   #define OM_HOST_FIELD_SSHPORT             "SshPort"

   #define OM_CS_DEPLOY_CL_HOSTIDX1          "{name:\"SYSDEPLOY_HOST_IDX1\",key: {"\
                                             OM_HOST_FIELD_NAME":1}, unique: true, enforced: true } "

   #define OM_CS_DEPLOY_CL_HOSTIDX2          "{name:\"SYSDEPLOY_HOST_IDX2\",key: {"\
                                             OM_HOST_FIELD_IP":1}, unique: true, enforced: true } "


   #define OM_CS_DEPLOY_CL_BUSINESS          OM_CS_DEPLOY".SYSBUSINESS"

   #define OM_BUSINESS_FIELD_NAME            "BusinessName"
   #define OM_BUSINESS_FIELD_TYPE            "BusinessType"
   #define OM_BUSINESS_FIELD_DEPLOYMOD       "DeployMod"
   #define OM_BUSINESS_FIELD_CLUSTERNAME     OM_HOST_FIELD_CLUSTERNAME
   #define OM_BUSINESS_FIELD_TIME            "Time"
   #define OM_BUSINESS_FIELD_LOCATION        "Location"

   #define OM_CS_DEPLOY_CL_BUSINESSIDX1      "{name:\"SYSDEPLOY_BUSINESS_IDX1\",key: {"\
                                             OM_BUSINESS_FIELD_NAME":1}, unique: true, enforced: true } "

   #define OM_CS_DEPLOY_CL_CONFIGURE         OM_CS_DEPLOY".SYSCONFIGURE"

   #define OM_CONFIGURE_FIELD_HOSTNAME       OM_HOST_FIELD_NAME
   #define OM_CONFIGURE_FIELD_BUSINESSNAME   OM_BUSINESS_FIELD_NAME
   #define OM_CONFIGURE_FIELD_CONFIG         "Config"

   #define OM_CS_DEPLOY_CL_TASKINFO          OM_CS_DEPLOY".SYSTASKINFO"

   #define OM_TASKINFO_FIELD_TASKID          FIELD_NAME_TASKID
   #define OM_TASKINFO_FIELD_TYPE            "Type"
   #define OM_TASKINFO_FIELD_TYPE_DESC       "TypeDesc"
   #define OM_TASKINFO_FIELD_NAME            "TaskName"
   #define OM_TASKINFO_FIELD_CREATE_TIME     "CreateTime"
   #define OM_TASKINFO_FIELD_END_TIME        "EndTime"
   #define OM_TASKINFO_FIELD_STATUS          "Status"
   #define OM_TASKINFO_FIELD_STATUS_DESC     "StatusDesc"
   #define OM_TASKINFO_FIELD_AGENTHOST       "AgentHost"
   #define OM_TASKINFO_FIELD_AGENTPORT       OM_HOST_FIELD_AGENT_PORT
   #define OM_TASKINFO_FIELD_INFO            "Info"
   #define OM_TASKINFO_FIELD_ERRNO           OP_ERRNOFIELD
   #define OM_TASKINFO_FIELD_DETAIL          OP_ERR_DETAIL
   #define OM_TASKINFO_FIELD_PROGRESS        "Progress"
   #define OM_TASKINFO_FIELD_RESULTINFO      "ResultInfo"

   /*
      addHost's ResultInfo:
      {
        IP,HostName,Status,StatusDesc,errno,detail,Flow
      }
   */
   #define OM_TASKINFO_FIELD_FLOW            "Flow"

   #define OM_CS_DEPLOY_CL_TASKINFOIDX1      "{name:\"SYSDEPLOY_TASKINFO_IDX1\",key: {"\
                                             OM_TASKINFO_FIELD_TASKID":1}, unique: true, enforced: true } "

   enum omTaskType
   {
      OM_TASK_TYPE_ADD_HOST        = 0,
      OM_TASK_TYPE_ADD_BUSINESS    = 1,
      OM_TASK_TYPE_REMOVE_BUSINESS = 2,

      OM_TASK_TYPE_END
   } ;

   #define OM_TASK_TYPE_ADD_HOST_STR          "ADD_HOST"
   #define OM_TASK_TYPE_ADD_BUSINESS_STR      "ADD_BUSINESS"
   #define OM_TASK_TYPE_REMOVE_BUSINESS_STR   "REMOVE_BUSINESS"

   const CHAR *getTaskTypeStr( INT32 taskType ) ;

   enum omTaskStatus
   {
      OM_TASK_STATUS_INIT      = 0,
      OM_TASK_STATUS_RUNNING   = 1,
      OM_TASK_STATUS_ROLLBACK  = 2,
      OM_TASK_STATUS_CANCEL    = 3,
      OM_TASK_STATUS_FINISH    = 4,

      OM_TASK_STATUS_END
   } ;

   #define OM_TASK_STATUS_INIT_STR       "INIT"
   #define OM_TASK_STATUS_RUNNING_STR    "RUNNING"
   #define OM_TASK_STATUS_ROLLBACK_STR   "ROLLBACK"
   #define OM_TASK_STATUS_CANCEL_STR     "CANCEL"
   #define OM_TASK_STATUS_FINISH_STR     "FINISH"

   const CHAR *getTaskStatusStr( INT32 taskStatus ) ;

   #define  OM_REST_LOGIN_HTML               "login.html"
   #define  OM_REST_INDEX_HTML               "index.html"
   #define  OM_REST_FAVICON_ICO              "favicon.ico"

   #define  OM_REST_REDIRECT_LOGIN           "<!DOCTYPE html><html><head>"\
                                             "<meta http-equiv=\"refresh\" content=\"0;url="\
                                             OM_REST_LOGIN_HTML"\"></head></html>"

   #define  OM_REST_REDIRECT_INDEX           "<!DOCTYPE html><html><head>"\
                                             "<meta http-equiv=\"refresh\" content=\"0;url="\
                                             OM_REST_INDEX_HTML"\"></head></html>"

   #define  OM_REST_RES_RETCODE              OP_ERRNOFIELD
   #define  OM_REST_RES_DESP                 OP_ERRDESP_FIELD
   #define  OM_REST_RES_DETAIL               OP_ERR_DETAIL
   #define  OM_REST_RES_LOCAL                "local"

   #define  OM_REST_FIELD_COMMAND            "cmd"

   #define  OM_CREATE_CLUSTER_REQ            "create cluster"

   #define  OM_REST_CLUSTER_INFO             "ClusterInfo"

   #define  OM_BSON_FIELD_CLUSTER_NAME       OM_HOST_FIELD_CLUSTERNAME
   #define  OM_BSON_FIELD_CLUSTER_DESC       OM_CLUSTER_FIELD_DESC
   #define  OM_BSON_FIELD_SDB_USER           OM_CLUSTER_FIELD_SDBUSER
   #define  OM_BSON_FIELD_SDB_PASSWD         OM_CLUSTER_FIELD_SDBPASSWD
   #define  OM_BSON_FIELD_SDB_USERGROUP      OM_CLUSTER_FIELD_SDBUSERGROUP

   #define  OM_DEFAULT_SDB_USER              "sdbadmin"
   #define  OM_DEFAULT_SDB_PASSWD            "sdbadmin"
   #define  OM_DEFAULT_SDB_USERGROUP         "sdbadmin_group"

#if defined _WINDOWS
   #define  OM_DEFAULT_INSTALL_PATH          "C:\\Program Files\\sequoiadb\\"
#else 
   #define  OM_DEFAULT_INSTALL_PATH          "/opt/sequoiadb/"
#endif


   #define  OM_QUERY_CLUSTER_REQ             "query cluster"

   #define  OM_BSON_FIELD_INSTALLPATH        OM_CLUSTER_FIELD_INSTALLPATH

   #define  OM_LOGIN_REQ                     "login"

   #define  OM_REST_HEAD_SESSIONID           "SdbSessionID"
   #define  OM_REST_HEAD_LANGUAGE            "SdbLanguage"

   #define  OM_REST_LANGUAGE_EN              "en"
   #define  OM_REST_LANGUAGE_ZH_CN           "zh-CN"

   #define  OM_REST_FIELD_LOGIN_NAME         "User"
   #define  OM_REST_FIELD_LOGIN_PASSWD       "Passwd"
   #define  OM_REST_FIELD_TIMESTAMP          "Timestamp"

   #define  OM_LOGOUT_REQ                    "logout"



   #define  OM_CHANGE_PASSWD_REQ             "change passwd"

   #define  OM_REST_FIELD_NEW_PASSWD         "NewPasswd"

   #define  OM_CHECK_SESSION_REQ             "check session"

   #define  OM_SCAN_HOST_REQ                 "scan host"

   #define  OM_REST_FIELD_HOST_INFO          "HostInfo"

   #define  OM_BSON_FIELD_HOST_INFO          OM_REST_FIELD_HOST_INFO
   #define  OM_BSON_FIELD_HOST_IP            OM_HOST_FIELD_IP
   #define  OM_BSON_FIELD_HOST_NAME          OM_HOST_FIELD_NAME
   #define  OM_BSON_FIELD_HOST_USER          OM_HOST_FIELD_USER
   #define  OM_BSON_FIELD_HOST_PASSWD        OM_HOST_FIELD_PASSWORD
   #define  OM_BSON_FIELD_HOST_SSHPORT       OM_HOST_FIELD_SSHPORT

   #define  OM_BSON_FIELD_AGENT_PORT         OM_HOST_FIELD_AGENT_PORT

   #define  OM_BSON_FIELD_SCAN_STATUS        "Status"

   #define  OM_SCAN_HOST_STATUS_FINISH       "finish"

   #define  OM_MSG_TIMEOUT_TWO_HOUR          (2 * 3600 * 1000)
   #define  OM_BASICCHECK_INTERVAL           OM_MSG_TIMEOUT_TWO_HOUR
   #define  OM_INSTALL_AGET_INTERVAL         OM_MSG_TIMEOUT_TWO_HOUR
   #define  OM_CHECK_HOST_INTERVAL           OM_MSG_TIMEOUT_TWO_HOUR
   #define  OM_WAIT_AGENT_UNISTALL_INTERVAL  OM_MSG_TIMEOUT_TWO_HOUR
   #define  OM_WAIT_SCAN_RES_INTERVAL        OM_MSG_TIMEOUT_TWO_HOUR

   #define  OM_WAIT_PROGRESS_RES_INTERVAL    (3000)
   #define  OM_WAIT_AGENT_EXIT_RES_INTERVAL  (1000)
   #define  OM_WAIT_UPDATE_HOST_INTERVAL     (OM_MSG_TIMEOUT_TWO_HOUR)

   #define  OM_CHECK_HOST_REQ                "check host"

   #define  OM_PRE_CHECK_HOST                "pre-check host"
   #define  OM_POST_CHECK_HOST               "post-check host"

   #define  OM_BSON_FIELD_OS                 OM_HOST_FIELD_OS
   #define  OM_BSON_FIELD_OMA                OM_HOST_FIELD_OMA
   #define  OM_BSON_FIELD_MEMORY             "Memory"
   #define  OM_BSON_FIELD_DISK               OM_HOST_FIELD_DISK
   #define  OM_BSON_FIELD_DISK_NAME          OM_HOST_FIELD_DISK_NAME
   #define  OM_BSON_FIELD_DISK_SIZE          OM_HOST_FIELD_DISK_SIZE
   #define  OM_BSON_FIELD_DISK_MOUNT         OM_HOST_FIELD_DISK_MOUNT
   #define  OM_BSON_FIELD_DISK_FREE_SIZE     OM_HOST_FIELD_DISK_FREE_SIZE
   #define  OM_BSON_FIELD_DISK_USED          OM_HOST_FIELD_DISK_USED

   #define  OM_BSON_FIELD_DISK_CANUSE        "CanUse"

   #define  OM_MIN_DISK_FREE_SIZE            (600)

   #define  OM_BSON_FIELD_CPU                OM_HOST_FIELD_CPU
   #define  OM_BSON_FIELD_NET                OM_HOST_FIELD_NET
   #define  OM_BSON_FIELD_PORT               OM_HOST_FIELD_PORT
   #define  OM_BSON_FIELD_SERVICE            OM_HOST_FIELD_SERVICE
   #define  OM_BSON_FIELD_SAFETY             OM_HOST_FIELD_SAFETY
   #define  OM_BSON_FIELD_CONFIG             OM_CONFIGURE_FIELD_CONFIG

   #define  OM_BSON_FIELD_NEEDUNINSTALL      "IsNeedUninstall"


   #define  OM_ADD_HOST_REQ                  "add host"

   #define  OM_BSON_FIELD_INSTALL_PATH       OM_HOST_FIELD_INSTALLPATH

   #define  OM_BSON_FIELD_PATCKET_PATH       "InstallPacket"

   #define  OM_PACKET_SUBPATH                "packet"

   #define  OM_NOTIFY_TASK                   "notify task"

   #define  OM_LIST_HOST_REQ                 "list host"

   #define  OM_QUERY_HOST_REQ                "query host"

   #define  OM_QUERY_HOST_STATUS_REQ         "query host status"

   #define  OM_BSON_FIELD_CPU_SYS            "Sys"
   #define  OM_BSON_FIELD_CPU_IDLE           "Idle"
   #define  OM_BSON_FIELD_CPU_OTHER          "Other"
   #define  OM_BSON_FIELD_CPU_USER           "User"

   #define  OM_BSON_FIELD_CPU_MEGABIT        "Megabit"
   #define  OM_BSON_FIELD_CPU_UNIT           "Unit"

   #define  OM_BSON_FIELD_NET_MEGABIT        OM_BSON_FIELD_CPU_MEGABIT
   #define  OM_BSON_FIELD_NET_UNIT           OM_BSON_FIELD_CPU_UNIT

   #define  OM_BSON_FIELD_NET_NAME           "Name"
   #define  OM_BSON_FIELD_NET_RXBYTES        "RXBytes"
   #define  OM_BSON_FIELD_NET_RXPACKETS      "RXPackets"
   #define  OM_BSON_FIELD_NET_RXERRORS       "RXErrors"
   #define  OM_BSON_FIELD_NET_RXDROPS        "RXDrops"
   #define  OM_BSON_FIELD_NET_TXBYTES        "TXBytes"
   #define  OM_BSON_FIELD_NET_TXPACKETS      "TXPackets"
   #define  OM_BSON_FIELD_NET_TXERRORS       "TXErrors"
   #define  OM_BSON_FIELD_NET_TXDROPS        "TXDrops"


   #define  OM_LIST_BUSINESS_TYPE_REQ        "list business type"

   #define  OM_BUSINESS_CONFIG_SUBDIR        "config"
   #define  OM_BUSINESS_FILE_NAME            "business"
   #define  OM_CONFIG_FILE_TYPE              ".xml"

   #define  OM_XMLATTR_KEY                   "<xmlattr>"
   #define  OM_XMLATTR_TYPE                  "<xmlattr>.type"
   #define  OM_XMLATTR_TYPE_ARRAY            "array"

   #define  OM_BSON_BUSINESS_LIST            "BusinessList"
   #define  OM_BSON_BUSINESS_TYPE            OM_BUSINESS_FIELD_TYPE
   #define  OM_BSON_BUSINESS_NAME            OM_BUSINESS_FIELD_NAME

   #define  OM_GET_BUSINESS_TEMPLATE_REQ     "get business template"

   #define  OM_TEMPLATE_FILE_NAME            "_template"

   #define  OM_REST_BUSINESS_TYPE            OM_BSON_BUSINESS_TYPE

   #define  OM_BSON_DEPLOY_MOD_LIST          "DeployModList"

   #define  OM_BSON_DEPLOY_MOD               "DeployMod"
   #define  OM_BSON_PROPERTY_ARRAY           "Property"
   #define  OM_BSON_PROPERTY_NAME            "Name"
   #define  OM_BSON_PROPERTY_TYPE            "Type"
   #define  OM_BSON_PROPERTY_DEFAULT         "Default"
   #define  OM_BSON_PROPERTY_VALID           "Valid"
   #define  OM_BSON_PROPERTY_DISPLAY         "Display"
   #define  OM_BSON_PROPERTY_EDIT            "Edit"
   #define  OM_BSON_PROPERTY_DESC            "Desc"
   #define  OM_BSON_PROPERTY_LEVEL           "Level"
   #define  OM_BSON_PROPERTY_WEBNAME         "WebName"


   #define  OM_CONFIG_BUSINESS_REQ           "get business config"

   #define  OM_CONFIG_ITEM_FILE_NAME         "_config"
   #define  OM_XML_CONFIG                    "config"

   #define  OM_REST_TEMPLATE_INFO            "TemplateInfo"

   #define  OM_BSON_PROPERTY_VALUE           "Value"


   #define  OM_INSTALL_BUSINESS_REQ          "add business"

   #define  OM_REST_CONFIG_INFO              "ConfigInfo"

   #define  OM_BSON_TASKID                   "TaskID"
   #define  OM_BSON_TASKTYPE                 "TaskType"


   #define  OM_LIST_NODE_REQ                 "list node"


   #define  OM_BSON_FIELD_SVCNAME            FIELD_NAME_SERVICE_NAME
   #define  OM_BSON_FIELD_ROLE               FIELD_NAME_ROLE

   #define  OM_GET_NODE_CONF_REQ             "get node configure"

   #define  OM_REST_BUSINESS_NAME            OM_BSON_BUSINESS_NAME
   #define  OM_REST_SVCNAME                  FIELD_NAME_SERVICE_NAME


   #define  OM_LIST_BUSINESS_REQ             "list business"

   #define  OM_QUERY_BUSINESS_REQ            "query business"

   #define  OM_REST_CLUSTER_NAME             OM_BSON_FIELD_CLUSTER_NAME
   #define  OM_BSON_BUSINESS_INFO            "BusinessInfo"



   #define  OM_REMOVE_CLUSTER_REQ            "remove cluster"


   #define  OM_REMOVE_HOST_REQ               "remove host"

   #define  OM_REST_HOST_NAME                OM_BSON_FIELD_HOST_NAME
   #define  OM_REST_ISFORCE                  "IsForce"


   #define  OM_REMOVE_BUSINESS_REQ           "remove business"

   #define  OM_SDB_AUTH_USER                 "AuthUser"
   #define  OM_SDB_AUTH_PASSWD               "AuthPasswd"


   #define  OM_UPDATE_HOSTNAME_REQ           "update hostname"

   #define  OM_PREDICT_CAPACITY_REQ          "predict capacity"

   #define  OM_BSON_FIELD_VALID_SIZE         "ValidSize"
   #define  OM_BSON_FIELD_TOTAL_SIZE         "TotalSize"
   #define  OM_BSON_FIELD_REDUNDANCY_RATE    "RedundancyRate"

   #define  OM_LIST_TASK_REQ                 "list task"

   #define  OM_QUERY_TASK_REQ                "query task"


   #define  OM_AGENT_UPDATE_TASK             "update task"


   #define  OM_DEFAULT_LOCAL_HOST            "localhost"
   #define  OM_AGENT_DEFAULT_PORT            SDBCM_DFT_PORT


}

#endif // OM_DEF_HPP__

