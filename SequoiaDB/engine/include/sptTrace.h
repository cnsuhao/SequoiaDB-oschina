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

/* This list file is automatically generated,you shoud NOT modify this file anyway! test comment*/
#ifndef sptTRACE_H__
#define sptTRACE_H__
#define SDB_SE_GLBSE                                       0x800000000000f7L
#define SDB_SE_NEWSCOPE                                    0x800000000000f8L
#define SDB_SCOPE_INIT                                     0x800000000000f9L
#define SDB_SCOPE_EVALUATE                                 0x800000000000faL
#define SDB_SCOPE_EVALUATE2                                0x800000000000fbL
#define SDB_OBJ2BSON                                       0x800000000000fcL
#define SDB_BSON_DESTRUCTOR                                0x800000000000fdL
#define SDB_BSON_CONSTRUCTOR                               0x800000000000feL
#define SDB_BSON_TO_JSON                                   0x800000000000ffL
#define SDB_GLOBAL_PRINT                                   0x80000000000100L
#define SDB_TRACE_FMT                                      0x80000000000101L
#define SDB_GLOBAL_HELP                                    0x80000000000102L
#define SDB_CURSOR_DESTRUCTOR                              0x80000000000103L
#define SDB_CURSOR_RESV                                    0x80000000000104L
#define SDB_CURSOR_CONSTRUCTOR                             0x80000000000105L
#define SDB_CURSOR_NEXT                                    0x80000000000106L
#define SDB_CURSOR_CURRENT                                 0x80000000000107L
#define SDB_CURSOR_UP_CURRENT                              0x80000000000108L
#define SDB_CURSOR_DEL_CURR                                0x80000000000109L
#define SDB_CURSOR_CLOSE                                   0x8000000000010aL
#define SDB_COUNT_DESTRUCTOR                               0x8000000000010bL
#define SDB_COUNT_RESV                                     0x8000000000010cL
#define SDB_COUNT_CONSTRUCTOR                              0x8000000000010dL
#define SDB_COLL_DESTRUCTOR                                0x8000000000010eL
#define SDB_COLL_CONSTRUCTOR                               0x8000000000010fL
#define SDB_COLL_RAW_FND                                   0x80000000000110L
#define SDB_COLL_INSERT                                    0x80000000000111L
#define SDB_COLL_UPDATE                                    0x80000000000112L
#define SDB_COLL_UPSERT                                    0x80000000000113L
#define SDB_COLL_REMOVE                                    0x80000000000114L
#define SDB_COLL_DELETE_LOB                                0x80000000000115L
#define SDB_COLL_LIST_LOBS                                 0x80000000000116L
#define SDB_COLL_LIST_LOBPIECES                            0x80000000000117L
#define SDB_COLL_GET_LOB                                   0x80000000000118L
#define SDB_COLL_PUT_LOB                                   0x80000000000119L
#define SDB_COLL_EXPLAIN                                   0x8000000000011aL
#define SDB_COLL_COUNT                                     0x8000000000011bL
#define SDB_COLL_SPLIT                                     0x8000000000011cL
#define SDB_COLL_SPLIT_ASYNC                               0x8000000000011dL
#define SDB_COLL_CRT_INX                                   0x8000000000011eL
#define SDB_COLL_GET_INX                                   0x8000000000011fL
#define SDB_COLL_DROP_INX                                  0x80000000000120L
#define SDB_COLL_BULK_INSERT                               0x80000000000121L
#define SDB_COLL_RENM                                      0x80000000000122L
#define SDB_COLL_AGGR                                      0x80000000000123L
#define SDB_COLL_ATTACHCOLLECTION                          0x80000000000124L
#define SDB_COLL_DETACHCOLLECTION                          0x80000000000125L
#define SDB_QUERY_RESV                                     0x80000000000126L
#define SDB_QUERY_CONSTRUCTOR                              0x80000000000127L
#define SDB_RN_DESTRUCTOR                                  0x80000000000128L
#define SB_RG_DESTRUCTOR                                   0x80000000000129L
#define SDB_RG_CONSTRUCTOR                                 0x8000000000012aL
#define SDB_RG_GET_MST                                     0x8000000000012bL
#define SDB_RG_GET_SLAVE                                   0x8000000000012cL
#define SDB_RG_START                                       0x8000000000012dL
#define SDB_RG_STOP                                        0x8000000000012eL
#define SDB_RG_CRT_NODE                                    0x8000000000012fL
#define SDB_RG_RM_NODE                                     0x80000000000130L
#define SDB_GET_NODE_AND_SETPROPERTY                       0x80000000000131L
#define SDB_RG_GET_NODE                                    0x80000000000132L
#define SDB_RG_REELECT                                     0x80000000000133L
#define SDB_CS_DESTRUCTOR                                  0x80000000000134L
#define SDB_ISSPECCOLLNM                                   0x80000000000135L
#define SDB_CS_RESV                                        0x80000000000136L
#define GET_CL_AND_SETPROPERTY                             0x80000000000137L
#define SDB_CS_GET_CL                                      0x80000000000138L
#define SB_CS_CRT_CL                                       0x80000000000139L
#define SDB_CS_DROP_CL                                     0x8000000000013aL
#define SDB_DOMAIN_DESTRUCTOR                              0x8000000000013bL
#define SDB_DOMAIN_ALTER                                   0x8000000000013cL
#define SDB_DOMAIN_LIST_GROUP                              0x8000000000013dL
#define SDB_DOMAIN_LIST_CS                                 0x8000000000013eL
#define SDB_DOMAIN_LIST_CL                                 0x8000000000013fL
#define SDB_DESTRUCTOR                                     0x80000000000140L
#define SDB_ISSPECSNM                                      0x80000000000141L
#define SDB_SDB_RESV                                       0x80000000000142L
#define SDB_SDB_CONSTRUCTOR                                0x80000000000143L
#define SDB_RN_CONNECT                                     0x80000000000144L
#define SDB_RN_START                                       0x80000000000145L
#define SDB_RN_STOP                                        0x80000000000146L
#define SDB_SDB_CRT_RG                                     0x80000000000147L
#define SDB_SDB_CREATE_DOMAIN                              0x80000000000148L
#define SDB_SDB_DROP_DOMAIN                                0x80000000000149L
#define SDB_SDB_GET_DOMAIN                                 0x8000000000014aL
#define SDB_SDB_LIST_DOMAINS                               0x8000000000014bL
#define SDB_SDB_CRT_PROCEDURE                              0x8000000000014cL
#define SDB_SDB_RM_PROCEDURE                               0x8000000000014dL
#define SDB_SDB_LIST_PROCEDURES                            0x8000000000014eL
#define SDB_SDB_EVAL                                       0x8000000000014fL
#define SDB_SDB_FLUSH_CONF                                 0x80000000000150L
#define SDB_SDB_RM_RG                                      0x80000000000151L
#define SDB_SDB_CRT_CATA_RG                                0x80000000000152L
#define SDB_SDB_CRT_CS                                     0x80000000000153L
#define SDB_GET_RG_AND_SETPROPERTY                         0x80000000000154L
#define SDB_SDB_GET_RG                                     0x80000000000155L
#define GET_CS_AND_SETPROPERTY                             0x80000000000156L
#define SDB_GET_CS                                         0x80000000000157L
#define SDB_SDB_DROP_CS                                    0x80000000000158L
#define SDB_SDB_SNAPSHOT                                   0x80000000000159L
#define SDB_SDB_RESET_SNAP                                 0x8000000000015aL
#define SDB_SDB_LIST                                       0x8000000000015bL
#define SDB_SDB_START_RG                                   0x8000000000015cL
#define SDB_SDB_CRT_USER                                   0x8000000000015dL
#define SDB_SDB_DROP_USER                                  0x8000000000015eL
#define SDB_SDB_EXEC                                       0x8000000000015fL
#define SDB_SDB_EXECUP                                     0x80000000000160L
#define SDB_SDB_TRACE_ON                                   0x80000000000161L
#define SDB_SDB_TRACE_RESUME                               0x80000000000162L
#define SDB_SDB_TRACE_OFF                                  0x80000000000163L
#define SDB_SDB_TRANS_BEGIN                                0x80000000000164L
#define SDB_SDB_TRANS_COMMIT                               0x80000000000165L
#define SDB_SDB_TRANS_ROLLBACK                             0x80000000000166L
#define SDB_SDB_CLOSE                                      0x80000000000167L
#define SDB_SDB_BACKUP_OFFLINE                             0x80000000000168L
#define SDB_SDB_LIST_BACKUP                                0x80000000000169L
#define SDB_SDB_REMOVE_BACKUP                              0x8000000000016aL
#define SDB_SDB_LIST_TASKS                                 0x8000000000016bL
#define SDB_SDB_WAIT_TASKS                                 0x8000000000016cL
#define SDB_SDB_CANCEL_TASK                                0x8000000000016dL
#define SDB_SDB_SET_SESSION_ATTR                           0x8000000000016eL
#define SDB_SDB_MSG                                        0x8000000000016fL
#define SDB_SDB_INVALIDATE_CACHE                           0x80000000000170L
#define SDB_SDB_FORCE_SESSION                              0x80000000000171L
#define SDB_SDB_FORCE_STEP_UP                              0x80000000000172L
#define SDB_OBJECTID_DESTRUCTOR                            0x80000000000173L
#define SDB_OBJECTID_CONSTRUCTOR                           0x80000000000174L
#endif
