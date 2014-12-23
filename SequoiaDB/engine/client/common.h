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

#ifndef COMMON_H__
#define COMMON_H__

#include "msg.h"
#ifndef __cplusplus
#include "bson/bson.h"
#endif
SDB_EXTERN_C_START
#define CLIENT_RECORD_ID_FIELD "_id"
#define CLIENT_RECORD_ID_INDEX "$id"
#define CLIENT_RECORD_ID_FIELD_STRLEN 3

#if defined (_WINDOWS)
#define SOCKET_INVALIDSOCKET  INVALID_SOCKET
#else
#define SOCKET_INVALIDSOCKET  -1
#endif

#define SDB_MD5_DIGEST_LENGTH 16

INT32 clientCheckRetMsgHeader( const CHAR *pSendBuf, const CHAR *pRecvBuf ) ;

#ifdef __cplusplus
INT32 clientBuildUpdateMsgCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, SINT32 flag,
                                UINT64 reqID,
                                const CHAR*selector, const CHAR*updator,
                                const CHAR*hint, BOOLEAN endianConvert ) ;
INT32 clientAppendInsertMsgCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                 const CHAR *insertor, BOOLEAN endianConvert ) ;
INT32 clientBuildInsertMsgCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, SINT32 flag,
                                UINT64 reqID,
                                const CHAR *insertor,
                                BOOLEAN endianConvert ) ;
INT32 clientBuildQueryMsgCpp  ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, SINT32 flag,
                                UINT64 reqID,
                                SINT64 numToSkip, SINT64 numToReturn,
                                const CHAR *query, const CHAR *fieldSelector,
                                const CHAR *orderBy, const CHAR *hint,
                                BOOLEAN endianConvert ) ;
INT32 clientBuildDeleteMsgCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, SINT32 flag,
                                UINT64 reqID,
                                const CHAR *deletor, const CHAR *hint,
                                BOOLEAN endianConvert ) ;
INT32 clientBuildAggrRequestCpp( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, const CHAR *obj,
                                BOOLEAN endianConvert ) ;
INT32 clientAppendAggrRequestCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *obj, BOOLEAN endianConvert ) ;
INT32 clientBuildOpenLobMsgCpp( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *pMeta, SINT32 flags, SINT16 w,
                                UINT64 reqID,
                                BOOLEAN endianConvert ) ;
INT32 clientBuildLobMsgCpp( CHAR **ppBuffer, INT32 *bufferSize,
                            INT32 msgType, const CHAR *pMeta,
                            SINT32 flags, SINT16 w, SINT64 contextID,
                            UINT64 reqID, const SINT64 *lobOffset,
                            const UINT32 *len, const CHAR *data,
                            BOOLEAN endianConvert ) ;
INT32 clientBuildRemoveLobMsgCpp( CHAR **ppBuffer, INT32 *bufferSize,
                                  const CHAR *pMeta,
                                  SINT32 flags, SINT16 w,
                                  UINT64 reqID,
                                  BOOLEAN endianConvert ) ;
#else
INT32 clientBuildUpdateMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, SINT32 flag,
                             UINT64 reqID,
                             bson *selector, bson *updator,
                             bson *hint, BOOLEAN endianConvert ) ;

INT32 clientAppendInsertMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                              bson *insertor, BOOLEAN endianConvert ) ;

INT32 clientBuildInsertMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, SINT32 flag,
                             UINT64 reqID,
                             bson *insertor, BOOLEAN endianConvert ) ;

INT32 clientBuildQueryMsg  ( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, SINT32 flag,
                             UINT64 reqID,
                             SINT64 numToSkip, SINT64 numToReturn,
                             const bson *query, const bson *fieldSelector,
                             const bson *orderBy, const bson *hint,
                             BOOLEAN endianConvert ) ;
INT32 clientBuildDeleteMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, SINT32 flag,
                             UINT64 reqID,
                             bson *deletor, bson *hint, BOOLEAN endianConvert ) ;
INT32 clientAppendOID ( bson *obj, bson_iterator *ret ) ;

INT32 clientBuildAggrRequest1( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, bson **objs,
                             SINT32 num, BOOLEAN endianConvert );
INT32 clientBuildAggrRequest( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, bson *obj,
                             BOOLEAN endianConvert ) ;
INT32 clientAppendAggrRequest ( CHAR **ppBuffer, INT32 *bufferSize,
                              bson *obj, BOOLEAN endianConvert ) ;

INT32 clientBuildLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                         INT32 msgType, const bson *meta,
                         SINT32 flags, SINT16 w, SINT64 contextID,
                         UINT64 reqID, const SINT64 *lobOffset,
                         const UINT32 *len, const CHAR *data,
                         BOOLEAN endianConvert ) ;

INT32 clientBuildOpenLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             const bson *meta, SINT32 flags, SINT16 w,
                             UINT64 reqID,
                             BOOLEAN endianConvert ) ;

INT32 clientBuildRemoveLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                               const bson *meta,
                               SINT32 flags, SINT16 w,
                               UINT64 reqID,
                               BOOLEAN endianConvert ) ;

#endif
INT32 clientBuildGetMoreMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                              SINT32 numToReturn,
                              SINT64 contextID, UINT64 reqID,
                              BOOLEAN endianConvert ) ;

INT32 clientBuildKillContextsMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                                  UINT64 reqID,
                                  SINT32 numContexts, const SINT64 *pContextIDs,
                                  BOOLEAN endianConvert ) ;

INT32 clientBuildKillAllContextsMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                                      UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientExtractReply ( CHAR *pBuffer, SINT32 *flag, SINT64 *contextID,
                           SINT32 *startFrom, SINT32 *numReturned,
                           BOOLEAN endianConvert ) ;

INT32 clientBuildDisconnectMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                                 UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientReplicaGroupExtractNode ( const CHAR *data,
                                      CHAR *pHostName,
                                      INT32 hostNameSize,
                                      CHAR *pServiceName,
                                      INT32 serviceNameSize,
                                      INT32 *pNodeID ) ;

INT32 clientBuildSqlMsg( CHAR **ppBuffer, INT32 *bufferSize,
                         const CHAR *sql, UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientBuildAuthMsg( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *pUsrName,
                          const CHAR *pPasswd,
                          UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientBuildAuthCrtMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *pUsrName,
                             const CHAR *pPasswd,
                             UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientBuildAuthDelMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *pUsrName,
                             const CHAR *pPasswd,
                             UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientBuildTransactionBegMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientBuildTransactionCommitMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientBuildTransactionRollbackMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             UINT64 reqID, BOOLEAN endianConvert ) ;


INT32 md5Encrypt( const CHAR *src,
                  CHAR *code,
                  UINT32 size ) ;

INT32 clientBuildSysInfoRequest ( CHAR **ppBuffer, INT32 *pBufferSize ) ;

INT32 clientExtractSysInfoReply ( CHAR *pBuffer, BOOLEAN *endianConvert,
                                  INT32 *osType ) ;

INT32 clientValidateSql( const CHAR *sql, BOOLEAN isExec ) ;

INT32 clientBuildFlushConfMsg( CHAR **ppBuffer, INT32 *bufferSize,
                               UINT64 reqID, BOOLEAN endianConvert) ;

INT32 clientBuildTestMsg( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *msg, UINT64 reqID, BOOLEAN endianConvert ) ;

INT32 clientBuildReadLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             UINT32 len, SINT64 offset,
                             SINT32 flags, SINT64 contextID,
                             UINT64 reqID,
                             BOOLEAN endianConvert ) ;
INT32 clientBuildWriteLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                              const CHAR *buf, UINT32 len,
                              SINT64 offset, SINT32 flags, SINT16 w,
                              SINT64 contextID, UINT64 reqID,
                              BOOLEAN endianConvert ) ;
INT32 clientBuildCloseLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                              SINT32 flags, SINT16 w,
                              SINT64 contextID, UINT64 reqID,
                              BOOLEAN endianConvert ) ;

SDB_EXTERN_C_END
#endif
