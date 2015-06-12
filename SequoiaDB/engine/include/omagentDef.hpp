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

   Source File Name = omagentDef.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/29/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_DEF_HPP__
#define OMAGENT_DEF_HPP__

#include "core.hpp"
#include "oss.hpp"

namespace engine
{

   /*
      [ sdbcm.conf ] Config Param Define
   */
   #define SDBCM_CONF_DFTPORT          "defaultPort"
   #define SDBCM_CONF_PORT             "_Port"
   #define SDBCM_RESTART_COUNT         "RestartCount"
   #define SDBCM_RESTART_INTERVAL      "RestartInterval"       // minute
   #define SDBCM_AUTO_START            "AutoStart"
   #define SDBCM_DIALOG_LEVEL          "DiagLevel"
   #define SDBCM_CONF_OMADDR           "OMAddress"
   #define SDBCM_CONF_ISGENERAL        "IsGeneral"

   #define SDBCM_DFT_PORT              11790
   #define SDBCM_OPTION_PREFIX         "--"

   /*
      CM REMOTE CODE DEFINE
   */
   enum CM_REMOTE_OP_CODE
   {
      SDBSTART       = 1,
      SDBSTOP        = 2,
      SDBADD         = 3,
      SDBMODIFY      = 4,
      SDBRM          = 5,
      SDBSTARTALL    = 6,
      SDBSTOPALL     = 7
   } ;

   /*
      SDB STOP RETURN RC
   */
   #define STOPFAIL              1
   #define STOPPART              3

   /*
      cm define
   */
   #define SDBCM_CONF_DIR_NAME         "conf"
   #define SDBCM_LOCAL_DIR_NAME        "local"
   #define SDBCM_LOG_DIR_NAME          "log"
   #define SDBOMA_SCRIPT_DIR_NAME      "script"

   #define SDBCM_EXE_FILE_NAME         "sdbcm"
   #define SDBCM_CFG_FILE_NAME         SDBCM_EXE_FILE_NAME".conf"
   #define SDBCM_DIALOG_FILE_NAME      SDBCM_EXE_FILE_NAME".log"

   #define SDB_CM_ROOT_PATH            ".." OSS_FILE_SEP SDBCM_CONF_DIR_NAME OSS_FILE_SEP
   #define SDBCM_CONF_PATH_FILE        SDB_CM_ROOT_PATH SDBCM_CFG_FILE_NAME
   #define SDBCM_LOCAL_PATH            SDB_CM_ROOT_PATH SDBCM_LOCAL_DIR_NAME
   #define SDBCM_LOG_PATH              SDB_CM_ROOT_PATH SDBCM_LOG_DIR_NAME
   #define SDBOMA_SCRIPT_PATH          SDB_CM_ROOT_PATH SDBOMA_SCRIPT_DIR_NAME

#if defined (_LINUX)
      #define SDBSTARTPROG             "sdbstart"
      #define SDBSTOPPROG              "sdbstop"
      #define SDBSDBCMPROG             SDBCM_EXE_FILE_NAME
#elif defined (_WINDOWS)
      #define SDBSTARTPROG             "sdbstart.exe"
      #define SDBSTOPPROG              "sdbstop.exe"
      #define SDBSDBCMPROG             SDBCM_EXE_FILE_NAME".exe"
#endif

   #define SDB_OMA_USER                "OMA_ADMIN"
   #define SDB_OMA_USERPASSWD          "OMA_ADMIN_PASSWD"

   /*
      oma command define
   */
   #define OMA_CMD_SCAN_HOST                          OM_SCAN_HOST_REQ
   #define OMA_CMD_PRE_CHECK_HOST                     OM_PRE_CHECK_HOST
   #define OMA_CMD_CHECK_HOST                         OM_CHECK_HOST_REQ
   #define OMA_CMD_POST_CHECK_HOST                    OM_POST_CHECK_HOST

   #define OMA_CMD_HANDLE_TASK_NOTIFY                 OM_NOTIFY_TASK

   #define OMA_CMD_ADD_HOST                           OM_ADD_HOST_REQ
   #define OMA_CMD_REMOVE_HOST                        OM_REMOVE_HOST_REQ
   #define OMA_CMD_INSTALL_DB_BUSINESS                OM_INSTALL_BUSINESS_REQ
   #define OMA_CMD_UNINSTALL_DB_BUSINESS              OM_REMOVE_BUSINESS_REQ
   #define OMA_CMD_START_DB_BUSINESS                  ""
   #define OMA_CMD_STOP_DB_BUSINESS                   ""
   #define OMA_CMD_QUERY_PROGRESS                     "" //OM_QUERY_PROGRESS
   #define OMA_CMD_UPDATE_HOSTS                       OM_UPDATE_HOSTNAME_REQ

   #define OMA_CMD_QUERY_HOST_STATUS                  OM_QUERY_HOST_STATUS_REQ



   /*
      oma internal command
   */
   #define OMA_CMD_CHECK_ADD_HOST_INFO                "check add host info"
   
   #define OMA_CMD_CRRATE_TMP_COORD                   "create temporary coord"
   #define OMA_CMD_REMOVE_TMP_COORD                   "remove temporary coord"
   
   #define OMA_CMD_ROLLBACK_ADD_HOSTS                 "rollback add hosts"
   #define OMA_CMD_RM_HOST                            "remove host"
   #define OMA_CMD_INSTALL_STANDALONE                 "install standalone"
   #define OMA_CMD_INSTALL_COORD                      "install coord"
   #define OMA_CMD_INSTALL_CATALOG                    "install catalog"
   #define OMA_CMD_INSTALL_DATA_NODE                  "install data node"
   #define OMA_CMD_RM_STANDALONE                      "remove standalone"
   #define OMA_CMD_RM_CATA_RG                         "remove cataloggroup"
   #define OMA_CMD_RM_COORD_RG                        "remove coordgroup"
   #define OMA_CMD_RM_DATA_RG                         "remove datagroup"

   #define OMA_CMD_UPDATE_TASK                        "update task"
   
   /*
      oma job
   */
   #define OMA_JOB_ADDHOST                            "add host job"
   #define OMA_JOB_ROLLBACKHOST                       "rollback host job"
   #define OMA_JOB_START_ADD_HOST_TASK                "start add host task job"
   #define OMA_JOB_CREATE_STANDALONE                  "create standalone job"
   #define OMA_JOB_CREATE_CATALOG                     "create catalog job"
   #define OMA_JOB_CREATE_COORD                       "create coord job"
   #define OMA_JOB_START_INSTALL_DB_BUS_TASK          "start install db business task job"
   #define OMA_JOB_START_REMOVE_DB_BUS_TASK           "start remove db business task job"
   #define OMA_ROLLBACK_INSTALLED_DB_BUSINESS         "rollback installed db business"
   #define OMA_ROLLBACK_STANDALONE                    "rollback installed standalone"
   #define OMA_ROLLBACK_CATALOG                       "rollback installed catalog"
   #define OMA_ROLLBACK_COORD                         "rollback installed coord"
   #define OMA_JOB_REMOVE_VIRTUAL_COORD               "remove virtual coord job"

   #define OMA_JOB                                    "omagent job"

   /*
      oma js file
   */
   #define FILE_DEFINE                      "define.js"
   #define FILE_ERROR                       "error.js"
   #define FILE_COMMON                      "common.js"
   #define FILE_LOG                         "log.js"
   #define FILE_FUNC                        "func.js"
   
   #define FILE_SCAN_HOST                   "scanHost.js"
   #define FILE_PRE_CHECK_HOST              "preCheckHost.js"
   #define FILE_CHECK_HOST                  "checkHost.js"
   #define FILE_CHECK_HOST_ITEM             "checkHostItem.js"
   #define FILE_POST_CHECK_HOST             "postCheckHost.js"
   
   #define FILE_ADD_HOST                    "addHostNew.js"
   #define FILE_ADD_HOST2                   "addHost2.js"
   #define FIEL_CHECK_ADD_HOST_INFO         "checkAddHostInfo.js"
   #define FILE_REMOVE_HOST                 "removeHost.js"
   #define FILE_CREATE_TMP_COORD            "createTmpCoord.js"
   #define FILE_REMOVE_TMP_COORD            "removeTmpCoord.js"
   #define FILE_ADDHOST_ROLLBACK            "addHostRollback.js"
   #define FILE_ADDHOST_ROLLBACK2            "addHostRollback2.js"
   #define FILE_UPDATE_HOSTS_INFO           "updateHostsInfo.js"
   #define FILE_UPDATE_HOSTS                "updateHosts.js"

   #define FILE_INSTALL_STANDALONE          "installStandalone.js"
   #define FILE_INSTALL_CATALOG             "installCatalog.js"
   #define FILE_INSTALL_COORD               "installCoord.js"
   #define FILE_INSTALL_DATANODE            "installDataNode.js"

   #define FILE_REMOVE_STANDALONE           "removeStandalone.js"
   #define FILE_REMOVE_CATALOG_RG           "removeCatalogRG.js"
   #define FILE_REMOVE_COORD_RG             "removeCoordRG.js"
   #define FILE_REMOVE_DATA_RG              "removeDataRG.js"

   #define FILE_ROLLBACK_STANDALONE         "rollbackStandalone.js"
   #define FILE_ROLLBACK_CATALOG            "rollbackCatalog.js"
   #define FILE_ROLLBACK_COORD              "rollbackCoord.js"
   #define FILE_ROLLBACK_DATANODE           "rollbackDataNode.js"


   #define FILE_QUERY_HOSTSTATUS            "queryHostStatus.js"
   #define FILE_QUERY_HOSTSTATUS_ITEM       "queryHostStatusItem.js"

   /*
      oma js argument type
   */
   #define JS_ARG_BUS                       "BUS_JSON"
   #define JS_ARG_SYS                       "SYS_JSON"
   #define JS_ARG_ENV                       "ENV_JSON"
   #define JS_ARG_OTHER                     "OTHER_JSON"

   /*
      oma create role
   */
   #define ROLE_STANDALONE                  "standalone"
   #define ROLE_COORD                       "coord"
   #define ROLE_CATA                        "catalog"
   #define ROLE_DATA                        "data"

   /*
      oma deplay mode
   */
   #define DEPLAY_SA                        "standalone"
   #define DEPLAY_DB                        "distribution"

   /*
      oma misc
   */
   #define OMA_BUFF_SIZE                    (1024)
   #define JS_FILE_NAME_LEN                 (512)
   #define JS_ARG_LEN                       (4096)
   #define WAITING_TIME                     (3000)


}

#endif // OMAGENT_DEF_HPP__

