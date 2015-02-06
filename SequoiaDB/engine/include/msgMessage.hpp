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

   Source File Name = msgMessage.hpp

   Descriptive Name = Message Client Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Messaging component. This file contains message structure for
   client-server communication.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef MSGMESSAGE_HPP_
#define MSGMESSAGE_HPP_
#include "msg.hpp"
#include "netDef.hpp"
#include "../bson/bson.h"
#include "../bson/oid.h"
#include <vector>
#include <string>
using namespace bson;
using namespace std;

INT32 msgCheckBuffer ( CHAR **ppBuffer, INT32 *bufferSize,
                       INT32 packetLength ) ;
INT32 extractRC ( BSONObj &obj ) ;
string routeID2String( MsgRouteID routeID ) ;
string routeID2String( UINT64 nodeID ) ;

/*
 * Create Update Message in ppBuffer
 * in/out ppBuffer
 * in/out bufferSize
 * in CollectionName
 * in flag
 * in selector
 * in updator
 * return SDB_OK for success
 */
INT32 msgBuildUpdateMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *CollectionName, SINT32 flag, UINT64 reqID,
                          const BSONObj *selector = NULL,
                          const BSONObj *updator = NULL,
                          const BSONObj *hint = NULL ) ;
/*
 * Extract Update Message from pBuffer
 * in pBuffer
 * out pflag
 * out ppCollectionName
 * out ppSelector
 * out ppUpdator
 */
INT32 msgExtractUpdate ( CHAR *pBuffer, INT32 *pflag, CHAR **ppCollectionName,
                         CHAR **ppSelector, CHAR **ppUpdator, CHAR **ppHint ) ;

/*
 * Create Insert Message in ppBuffer
 * in/out ppBuffer
 * in/out bufferSize
 * in CollectionName
 * in flag
 * in insertor
 * return SDB_OK for success
 */
INT32 msgBuildInsertMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *CollectionName, SINT32 flag, UINT64 reqID,
                          const BSONObj *insertor ) ;

INT32 msgBuildInsertMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *CollectionName, SINT32 flag, UINT64 reqID,
                          void *pFiller, std::vector< CHAR * > &ObjQueue,
                          engine::netIOVec &ioVec );

INT32 msgAppendInsertMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                           const BSONObj *insertor ) ;

/*
 * Extract Insert Message from pBuffer
 * in pBuffer
 * out pflag
 * out ppCollectionName
 * out ppInsertor
 */
INT32 msgExtractInsert ( CHAR *pBuffer, INT32 *pflag, CHAR **ppCollectionName,
                         CHAR **ppInsertor, INT32 &count ) ;

INT32 msgBuildQueryMsg  ( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *CollectionName, SINT32 flag, UINT64 reqID,
                          SINT64 numToSkip, SINT64 numToReturn,
                          const BSONObj *query = NULL,
                          const BSONObj *fieldSelector = NULL,
                          const BSONObj *orderBy = NULL,
                          const BSONObj *hint = NULL ) ;

INT32 msgExtractQuery  ( CHAR *pBuffer, INT32 *pflag, CHAR **ppCollectionName,
                         SINT64 *numToSkip, SINT64 *numToReturn,
                         CHAR **ppQuery, CHAR **ppFieldSelector,
                         CHAR **ppOrderBy, CHAR **ppHint ) ;

INT32 msgExtractQueryCommand ( CHAR *pBuffer, SINT32 packetSize, CHAR **ppCommand,
                              SINT32 &len );

INT32 msgBuildGetMoreMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                           SINT32 numToReturn,
                           SINT64 contextID, UINT64 reqID ) ;

INT32 msgExtractGetMore  ( CHAR *pBuffer,
                           SINT32 *numToReturn, SINT64 *contextID ) ;

void msgFillGetMoreMsg ( MsgOpGetMore &getMoreMsg, const UINT32 tid,
                        const SINT64 contextID, const SINT32 numToReturn,
                        const UINT64 reqID );

INT32 msgBuildDeleteMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *CollectionName, SINT32 flag, UINT64 reqID,
                          const BSONObj *deletor = NULL,
                          const BSONObj *hint = NULL ) ;

INT32 msgExtractDelete ( CHAR *pBuffer, INT32 *pflag, CHAR **ppCollectionName,
                         CHAR **ppDeletor, CHAR **ppHint ) ;

INT32 msgBuildKillContextsMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                               UINT64 reqID,
                               SINT32 numContexts, SINT64 *pContextIDs ) ;

INT32 msgExtractKillContexts ( CHAR *pBuffer,
                              SINT32 *numContexts, SINT64 **ppContextIDs ) ;

INT32 msgBuildMsgMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                       UINT64 reqID, CHAR *pMsgStr ) ;

INT32 msgExtractMsg ( CHAR *pBuffer,
                      CHAR **ppMsgStr ) ;

INT32 msgBuildReplyMsg ( CHAR **ppBuffer, INT32 *bufferSize, INT32 opCode,
                         SINT32 flag, SINT64 contextID, SINT32 startFrom,
                         SINT32 numReturned, UINT64 reqID,
                         vector<BSONObj> *objList ) ;

INT32 msgBuildReplyMsg ( CHAR **ppBuffer, INT32 *bufferSize, INT32 opCode,
                         SINT32 flag, SINT64 contextID, SINT32 startFrom,
                         SINT32 numReturned, UINT64 reqID,
                         const BSONObj *bsonobj ) ;

INT32 msgExtractReply ( CHAR *pBuffer, SINT32 *flag, SINT64 *contextID,
                        SINT32 *startFrom, SINT32 *numReturned,
                        vector<BSONObj> &objList ) ;

void msgBuildReplyMsgHeader ( MsgOpReply &replyHeader, SINT32 packetLength,
                              INT32 opCode, SINT32 flag, SINT64 contextID, 
                              SINT32 startFrom, SINT32 numReturned, 
                              MsgRouteID &routeID, UINT64 reqID ) ;

INT32 msgBuildDisconnectMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                              UINT64 reqID ) ;

void msgBuildDisconnectMsg ( MsgOpDisconnect &disconnectHeader,
                             MsgRouteID &nodeID, UINT64 reqID );

INT32 msgBuildQueryCatalogReqMsg ( CHAR **ppBuffer, INT32 *pBufferSize,
                              SINT32 flag, UINT64 reqID, SINT64 numToSkip,
                              SINT64 numToReturn, UINT32 TID,
                              const BSONObj *query,
                              const BSONObj *fieldSelector,
                              const BSONObj *orderBy,
                              const BSONObj *hint );

INT32 msgBuildQuerySpaceReqMsg ( CHAR **ppBuffer, INT32 *pBufferSize,
                                 SINT32 flag, UINT64 reqID, SINT64 numToSkip,
                                 SINT64 numToReturn, UINT32 TID,
                                 const BSONObj *query,
                                 const BSONObj *fieldSelector,
                                 const BSONObj *orderBy,
                                 const BSONObj *hint );


INT32 msgExtractSql( CHAR *pBuffer, CHAR **sql ) ;

INT32 msgBuildCMRequest ( CHAR **ppBuffer, INT32 *pBufferSize, SINT32 remoCode,
                          const BSONObj *arg1 = NULL,
                          const BSONObj *arg2 = NULL,
                          const BSONObj *arg3 = NULL,
                          const BSONObj *arg4 = NULL ) ;
INT32 msgExtractCMRequest ( CHAR *pBuffer, SINT32 *remoCode,
                            CHAR **arg1, CHAR **arg2,
                            CHAR **arg3, CHAR **arg4 ) ;

INT32 msgBuildDropIndexMsg  ( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *CollectionName, const CHAR *IndexName,
                          UINT64 reqID ) ;

INT32 msgBuildSysInfoRequest ( CHAR **ppBuffer, INT32 *pBufferSize ) ;

INT32 msgExtractSysInfoRequest ( CHAR *pBuffer, BOOLEAN &endianConvert ) ;

INT32 msgBuildSysInfoReply ( CHAR **ppBuffer, INT32 *pBufferSize ) ;

INT32 msgExtractSysInfoReply ( CHAR *pBuffer, BOOLEAN &endianConvert,
                               INT32 *osType ) ;

INT32 msgBuildTransCommitPreMsg ( CHAR **ppBuffer, INT32 *bufferSize );

INT32 msgBuildTransCommitMsg ( CHAR **ppBuffer, INT32 *bufferSize );

INT32 msgBuildTransRollbackMsg ( CHAR **ppBuffer, INT32 *bufferSize );

INT32 msgBuildSysInfoRequest ( CHAR **ppBuffer, INT32 *pBufferSize ) ;

INT32 msgExtractSysInfoRequest ( CHAR *pBuffer, BOOLEAN &endianConvert ) ;

INT32 msgBuildSysInfoReply ( CHAR **ppBuffer, INT32 *pBufferSize ) ;

INT32 msgExtractSysInfoReply ( CHAR *pBuffer, BOOLEAN &endianConvert,
                               INT32 *osType ) ;

INT32 msgExtractAggrRequest ( CHAR *pBuffer, CHAR **ppCollectionName,
                              CHAR **ppObjs, INT32 &count,
                              INT32 *pFlags = NULL );

INT32 msgExtractLobRequest( const CHAR *pBuffer, const MsgOpLob **header,
                            bson::BSONObj &meta, const MsgLobTuple **tuples,
                            UINT32 *tuplesSize ) ;

INT32 msgExtractTuples( const MsgLobTuple **begin,
                        UINT32 *tuplesSize,
                        const MsgLobTuple **tuple,
                        BOOLEAN *got ) ;

INT32 msgExtractTuplesAndData( const MsgLobTuple **begin, UINT32 *tuplesSize,
                               const MsgLobTuple **tuple, const CHAR **data,
                               BOOLEAN *got ) ;

INT32 msgExtractOpenLobRequest( const CHAR *pBuffer, const MsgOpLob **header,
                                bson::BSONObj &meta ) ;

INT32 msgExtractWriteLobRequest( const CHAR *pBuffer, const MsgOpLob **header,
                                 UINT32 *len, SINT64 *offset, const CHAR **data ) ;

INT32 msgExtractReadLobRequest( const CHAR *pBuffer, const MsgOpLob **header,
                                 UINT32 *len, SINT64 *offset ) ;

INT32 msgExtractCloseLobRequest( const CHAR *pBuffer, const MsgOpLob **header ) ;

INT32 msgExtractRemoveLobRequest( const CHAR *pBuffer, const MsgOpLob **header,
                                  BSONObj &obj ) ;

INT32 msgBuildGetLobMetaRequest( CHAR **ppBuffer, INT32 *pBufferSize ) ;

INT32 msgExtractReadResult( const MsgOpReply *header,
                            const MsgLobTuple **begin,
                            UINT32 *tupleSz ) ;


#endif // MSGMESSAGE_HPP_

