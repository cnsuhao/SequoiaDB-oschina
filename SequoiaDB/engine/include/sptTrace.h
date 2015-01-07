/*    Copyright 2012 SequoiaDB Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
/*    Copyright (C) 2011-2014 SequoiaDB Ltd.
 *    This program is free software: you can redistribute it and/or modify
 *    it under the term of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warrenty of
 *    MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program. If not, see <http://www.gnu.org/license/>.
 */

/* This list file is automatically generated,you shoud NOT modify this file anyway! test comment*/
#ifndef sptTRACE_H__
#define sptTRACE_H__
// Component: spt
#define SDB_OBJ2BSON                                       0x80000000000000L
#define SDB_BSON_DESTRUCTOR                                0x80000000000001L
#define SDB_BSON_CONSTRUCTOR                               0x80000000000002L
#define SDB_BSON_TO_JSON                                   0x80000000000003L
#define SDB_GLOBAL_PRINT                                   0x80000000000004L
#define SDB_TRACE_FMT                                      0x80000000000005L
#define SDB_GLOBAL_HELP                                    0x80000000000006L
#define SDB_CURSOR_DESTRUCTOR                              0x80000000000007L
#define SDB_CURSOR_RESV                                    0x80000000000008L
#define SDB_CURSOR_CONSTRUCTOR                             0x80000000000009L
#define SDB_CURSOR_NEXT                                    0x8000000000000aL
#define SDB_CURSOR_CURRENT                                 0x8000000000000bL
#define SDB_CURSOR_UP_CURRENT                              0x8000000000000cL
#define SDB_CURSOR_DEL_CURR                                0x8000000000000dL
#define SDB_CURSOR_CLOSE                                   0x8000000000000eL
#define SDB_COUNT_DESTRUCTOR                               0x8000000000000fL
#define SDB_COUNT_RESV                                     0x80000000000010L
#define SDB_COUNT_CONSTRUCTOR                              0x80000000000011L
#define SDB_COLL_DESTRUCTOR                                0x80000000000012L
#define SDB_COLL_CONSTRUCTOR                               0x80000000000013L
#define SDB_COLL_RAW_FND                                   0x80000000000014L
#define SDB_COLL_INSERT                                    0x80000000000015L
#define SDB_COLL_UPDATE                                    0x80000000000016L
#define SDB_COLL_UPSERT                                    0x80000000000017L
#define SDB_COLL_REMOVE                                    0x80000000000018L
#define SDB_COLL_DELETE_LOB                                0x80000000000019L
#define SDB_COLL_LIST_LOBS                                 0x8000000000001aL
#define SDB_COLL_LIST_LOBPIECES                            0x8000000000001bL
#define SDB_COLL_GET_LOB                                   0x8000000000001cL
#define SDB_COLL_PUT_LOB                                   0x8000000000001dL
#define SDB_COLL_EXPLAIN                                   0x8000000000001eL
#define SDB_COLL_COUNT                                     0x8000000000001fL
#define SDB_COLL_SPLIT                                     0x80000000000020L
#define SDB_COLL_SPLIT_ASYNC                               0x80000000000021L
#define SDB_COLL_CRT_INX                                   0x80000000000022L
#define SDB_COLL_GET_INX                                   0x80000000000023L
#define SDB_COLL_DROP_INX                                  0x80000000000024L
#define SDB_COLL_BULK_INSERT                               0x80000000000025L
#define SDB_COLL_RENM                                      0x80000000000026L
#define SDB_COLL_AGGR                                      0x80000000000027L
#define SDB_COLL_ATTACHCOLLECTION                          0x80000000000028L
#define SDB_COLL_DETACHCOLLECTION                          0x80000000000029L
#define SDB_QUERY_RESV                                     0x8000000000002aL
#define SDB_QUERY_CONSTRUCTOR                              0x8000000000002bL
#define SDB_RN_DESTRUCTOR                                  0x8000000000002cL
#define SB_RG_DESTRUCTOR                                   0x8000000000002dL
#define SDB_RG_CONSTRUCTOR                                 0x8000000000002eL
#define SDB_RG_GET_MST                                     0x8000000000002fL
#define SDB_RG_GET_SLAVE                                   0x80000000000030L
#define SDB_RG_START                                       0x80000000000031L
#define SDB_RG_STOP                                        0x80000000000032L
#define SDB_RG_CRT_NODE                                    0x80000000000033L
#define SDB_RG_RM_NODE                                     0x80000000000034L
#define SDB_GET_NODE_AND_SETPROPERTY                       0x80000000000035L
#define SDB_RG_GET_NODE                                    0x80000000000036L
#define SDB_CS_DESTRUCTOR                                  0x80000000000037L
#define SDB_ISSPECCOLLNM                                   0x80000000000038L
#define SDB_CS_RESV                                        0x80000000000039L
#define GET_CL_AND_SETPROPERTY                             0x8000000000003aL
#define SDB_CS_GET_CL                                      0x8000000000003bL
#define SB_CS_CRT_CL                                       0x8000000000003cL
#define SDB_CS_DROP_CL                                     0x8000000000003dL
#define SDB_DOMAIN_DESTRUCTOR                              0x8000000000003eL
#define SDB_DOMAIN_ALTER                                   0x8000000000003fL
#define SDB_DOMAIN_LIST_GROUP                              0x80000000000040L
#define SDB_DOMAIN_LIST_CS                                 0x80000000000041L
#define SDB_DOMAIN_LIST_CL                                 0x80000000000042L
#define SDB_DESTRUCTOR                                     0x80000000000043L
#define SDB_ISSPECSNM                                      0x80000000000044L
#define SDB_SDB_RESV                                       0x80000000000045L
#define SDB_SDB_CONSTRUCTOR                                0x80000000000046L
#define SDB_RN_CONNECT                                     0x80000000000047L
#define SDB_RN_START                                       0x80000000000048L
#define SDB_RN_STOP                                        0x80000000000049L
#define SDB_SDB_CRT_RG                                     0x8000000000004aL
#define SDB_SDB_CREATE_DOMAIN                              0x8000000000004bL
#define SDB_SDB_DROP_DOMAIN                                0x8000000000004cL
#define SDB_SDB_GET_DOMAIN                                 0x8000000000004dL
#define SDB_SDB_LIST_DOMAINS                               0x8000000000004eL
#define SDB_SDB_CRT_PROCEDURE                              0x8000000000004fL
#define SDB_SDB_RM_PROCEDURE                               0x80000000000050L
#define SDB_SDB_LIST_PROCEDURES                            0x80000000000051L
#define SDB_SDB_EVAL                                       0x80000000000052L
#define SDB_SDB_FLUSH_CONF                                 0x80000000000053L
#define SDB_SDB_RM_RG                                      0x80000000000054L
#define SDB_SDB_CRT_CATA_RG                                0x80000000000055L
#define SDB_SDB_CRT_CS                                     0x80000000000056L
#define SDB_GET_RG_AND_SETPROPERTY                         0x80000000000057L
#define SDB_SDB_GET_RG                                     0x80000000000058L
#define GET_CS_AND_SETPROPERTY                             0x80000000000059L
#define SDB_GET_CS                                         0x8000000000005aL
#define SDB_SDB_DROP_CS                                    0x8000000000005bL
#define SDB_SDB_SNAPSHOT                                   0x8000000000005cL
#define SDB_SDB_RESET_SNAP                                 0x8000000000005dL
#define SDB_SDB_LIST                                       0x8000000000005eL
#define SDB_SDB_START_RG                                   0x8000000000005fL
#define SDB_SDB_CRT_USER                                   0x80000000000060L
#define SDB_SDB_DROP_USER                                  0x80000000000061L
#define SDB_SDB_EXEC                                       0x80000000000062L
#define SDB_SDB_EXECUP                                     0x80000000000063L
#define SDB_SDB_TRACE_ON                                   0x80000000000064L
#define SDB_SDB_TRACE_RESUME                               0x80000000000065L
#define SDB_SDB_TRACE_OFF                                  0x80000000000066L
#define SDB_SDB_TRANS_BEGIN                                0x80000000000067L
#define SDB_SDB_TRANS_COMMIT                               0x80000000000068L
#define SDB_SDB_TRANS_ROLLBACK                             0x80000000000069L
#define SDB_SDB_CLOSE                                      0x8000000000006aL
#define SDB_SDB_BACKUP_OFFLINE                             0x8000000000006bL
#define SDB_SDB_LIST_BACKUP                                0x8000000000006cL
#define SDB_SDB_REMOVE_BACKUP                              0x8000000000006dL
#define SDB_SDB_LIST_TASKS                                 0x8000000000006eL
#define SDB_SDB_WAIT_TASKS                                 0x8000000000006fL
#define SDB_SDB_CANCEL_TASK                                0x80000000000070L
#define SDB_SDB_SET_SESSION_ATTR                           0x80000000000071L
#define SDB_SDB_MSG                                        0x80000000000072L
#define SDB_SDB_INVALIDATE_CACHE                           0x80000000000073L
#define SDB_SDB_FORCE_SESSION                              0x80000000000074L
#define SDB_SE_GLBSE                                       0x80000000000075L
#define SDB_SE_NEWSCOPE                                    0x80000000000076L
#define SDB_SCOPE_INIT                                     0x80000000000077L
#define SDB_SCOPE_EVALUATE                                 0x80000000000078L
#define SDB_SCOPE_EVALUATE2                                0x80000000000079L
#endif
