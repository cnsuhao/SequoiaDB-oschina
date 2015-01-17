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
#define SDB_SE_GLBSE                                       0x800000000001d4L
#define SDB_SE_NEWSCOPE                                    0x800000000001d5L
#define SDB_SCOPE_INIT                                     0x800000000001d6L
#define SDB_SCOPE_EVALUATE                                 0x800000000001d7L
#define SDB_SCOPE_EVALUATE2                                0x800000000001d8L
#define SDB_OBJ2BSON                                       0x800000000001d9L
#define SDB_BSON_DESTRUCTOR                                0x800000000001daL
#define SDB_BSON_CONSTRUCTOR                               0x800000000001dbL
#define SDB_BSON_TO_JSON                                   0x800000000001dcL
#define SDB_GLOBAL_PRINT                                   0x800000000001ddL
#define SDB_TRACE_FMT                                      0x800000000001deL
#define SDB_GLOBAL_HELP                                    0x800000000001dfL
#define SDB_CURSOR_DESTRUCTOR                              0x800000000001e0L
#define SDB_CURSOR_RESV                                    0x800000000001e1L
#define SDB_CURSOR_CONSTRUCTOR                             0x800000000001e2L
#define SDB_CURSOR_NEXT                                    0x800000000001e3L
#define SDB_CURSOR_CURRENT                                 0x800000000001e4L
#define SDB_CURSOR_UP_CURRENT                              0x800000000001e5L
#define SDB_CURSOR_DEL_CURR                                0x800000000001e6L
#define SDB_CURSOR_CLOSE                                   0x800000000001e7L
#define SDB_COUNT_DESTRUCTOR                               0x800000000001e8L
#define SDB_COUNT_RESV                                     0x800000000001e9L
#define SDB_COUNT_CONSTRUCTOR                              0x800000000001eaL
#define SDB_COLL_DESTRUCTOR                                0x800000000001ebL
#define SDB_COLL_CONSTRUCTOR                               0x800000000001ecL
#define SDB_COLL_RAW_FND                                   0x800000000001edL
#define SDB_COLL_INSERT                                    0x800000000001eeL
#define SDB_COLL_UPDATE                                    0x800000000001efL
#define SDB_COLL_UPSERT                                    0x800000000001f0L
#define SDB_COLL_REMOVE                                    0x800000000001f1L
#define SDB_COLL_DELETE_LOB                                0x800000000001f2L
#define SDB_COLL_LIST_LOBS                                 0x800000000001f3L
#define SDB_COLL_LIST_LOBPIECES                            0x800000000001f4L
#define SDB_COLL_GET_LOB                                   0x800000000001f5L
#define SDB_COLL_PUT_LOB                                   0x800000000001f6L
#define SDB_COLL_EXPLAIN                                   0x800000000001f7L
#define SDB_COLL_COUNT                                     0x800000000001f8L
#define SDB_COLL_SPLIT                                     0x800000000001f9L
#define SDB_COLL_SPLIT_ASYNC                               0x800000000001faL
#define SDB_COLL_CRT_INX                                   0x800000000001fbL
#define SDB_COLL_GET_INX                                   0x800000000001fcL
#define SDB_COLL_DROP_INX                                  0x800000000001fdL
#define SDB_COLL_BULK_INSERT                               0x800000000001feL
#define SDB_COLL_RENM                                      0x800000000001ffL
#define SDB_COLL_AGGR                                      0x80000000000200L
#define SDB_COLL_ATTACHCOLLECTION                          0x80000000000201L
#define SDB_COLL_DETACHCOLLECTION                          0x80000000000202L
#define SDB_QUERY_RESV                                     0x80000000000203L
#define SDB_QUERY_CONSTRUCTOR                              0x80000000000204L
#define SDB_RN_DESTRUCTOR                                  0x80000000000205L
#define SB_RG_DESTRUCTOR                                   0x80000000000206L
#define SDB_RG_CONSTRUCTOR                                 0x80000000000207L
#define SDB_RG_GET_MST                                     0x80000000000208L
#define SDB_RG_GET_SLAVE                                   0x80000000000209L
#define SDB_RG_START                                       0x8000000000020aL
#define SDB_RG_STOP                                        0x8000000000020bL
#define SDB_RG_CRT_NODE                                    0x8000000000020cL
#define SDB_RG_RM_NODE                                     0x8000000000020dL
#define SDB_GET_NODE_AND_SETPROPERTY                       0x8000000000020eL
#define SDB_RG_GET_NODE                                    0x8000000000020fL
#define SDB_CS_DESTRUCTOR                                  0x80000000000210L
#define SDB_ISSPECCOLLNM                                   0x80000000000211L
#define SDB_CS_RESV                                        0x80000000000212L
#define GET_CL_AND_SETPROPERTY                             0x80000000000213L
#define SDB_CS_GET_CL                                      0x80000000000214L
#define SB_CS_CRT_CL                                       0x80000000000215L
#define SDB_CS_DROP_CL                                     0x80000000000216L
#define SDB_DOMAIN_DESTRUCTOR                              0x80000000000217L
#define SDB_DOMAIN_ALTER                                   0x80000000000218L
#define SDB_DOMAIN_LIST_GROUP                              0x80000000000219L
#define SDB_DOMAIN_LIST_CS                                 0x8000000000021aL
#define SDB_DOMAIN_LIST_CL                                 0x8000000000021bL
#define SDB_DESTRUCTOR                                     0x8000000000021cL
#define SDB_ISSPECSNM                                      0x8000000000021dL
#define SDB_SDB_RESV                                       0x8000000000021eL
#define SDB_SDB_CONSTRUCTOR                                0x8000000000021fL
#define SDB_RN_CONNECT                                     0x80000000000220L
#define SDB_RN_START                                       0x80000000000221L
#define SDB_RN_STOP                                        0x80000000000222L
#define SDB_SDB_CRT_RG                                     0x80000000000223L
#define SDB_SDB_CREATE_DOMAIN                              0x80000000000224L
#define SDB_SDB_DROP_DOMAIN                                0x80000000000225L
#define SDB_SDB_GET_DOMAIN                                 0x80000000000226L
#define SDB_SDB_LIST_DOMAINS                               0x80000000000227L
#define SDB_SDB_CRT_PROCEDURE                              0x80000000000228L
#define SDB_SDB_RM_PROCEDURE                               0x80000000000229L
#define SDB_SDB_LIST_PROCEDURES                            0x8000000000022aL
#define SDB_SDB_EVAL                                       0x8000000000022bL
#define SDB_SDB_FLUSH_CONF                                 0x8000000000022cL
#define SDB_SDB_RM_RG                                      0x8000000000022dL
#define SDB_SDB_CRT_CATA_RG                                0x8000000000022eL
#define SDB_SDB_CRT_CS                                     0x8000000000022fL
#define SDB_GET_RG_AND_SETPROPERTY                         0x80000000000230L
#define SDB_SDB_GET_RG                                     0x80000000000231L
#define GET_CS_AND_SETPROPERTY                             0x80000000000232L
#define SDB_GET_CS                                         0x80000000000233L
#define SDB_SDB_DROP_CS                                    0x80000000000234L
#define SDB_SDB_SNAPSHOT                                   0x80000000000235L
#define SDB_SDB_RESET_SNAP                                 0x80000000000236L
#define SDB_SDB_LIST                                       0x80000000000237L
#define SDB_SDB_START_RG                                   0x80000000000238L
#define SDB_SDB_CRT_USER                                   0x80000000000239L
#define SDB_SDB_DROP_USER                                  0x8000000000023aL
#define SDB_SDB_EXEC                                       0x8000000000023bL
#define SDB_SDB_EXECUP                                     0x8000000000023cL
#define SDB_SDB_TRACE_ON                                   0x8000000000023dL
#define SDB_SDB_TRACE_RESUME                               0x8000000000023eL
#define SDB_SDB_TRACE_OFF                                  0x8000000000023fL
#define SDB_SDB_TRANS_BEGIN                                0x80000000000240L
#define SDB_SDB_TRANS_COMMIT                               0x80000000000241L
#define SDB_SDB_TRANS_ROLLBACK                             0x80000000000242L
#define SDB_SDB_CLOSE                                      0x80000000000243L
#define SDB_SDB_BACKUP_OFFLINE                             0x80000000000244L
#define SDB_SDB_LIST_BACKUP                                0x80000000000245L
#define SDB_SDB_REMOVE_BACKUP                              0x80000000000246L
#define SDB_SDB_LIST_TASKS                                 0x80000000000247L
#define SDB_SDB_WAIT_TASKS                                 0x80000000000248L
#define SDB_SDB_CANCEL_TASK                                0x80000000000249L
#define SDB_SDB_SET_SESSION_ATTR                           0x8000000000024aL
#define SDB_SDB_MSG                                        0x8000000000024bL
#define SDB_SDB_INVALIDATE_CACHE                           0x8000000000024cL
#define SDB_SDB_FORCE_SESSION                              0x8000000000024dL
#endif
