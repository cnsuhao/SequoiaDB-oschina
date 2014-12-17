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

#ifndef FUNCTION_SDB_HPP__
#define FUNCTION_SDB_HPP__

#include "client.hpp"
#include "ossUtil.h"
#include "ossMem.h"
using namespace sdbclient ;

#define CHAR_ERR_SIZE 1024
#define INIT_BUF_SIZE 1024
#define INC_BUF_SIZE 4096
#define BUF_BSON_ID "_id"

/*********** sequoiadb *****************/
INT32 connect ( sdb *connection, const CHAR * hostName ) ;

INT32 connect ( sdb *connection,
                const CHAR *hostName,
                const CHAR *userName,
                const CHAR *password ) ;

void close ( sdb *connection ) ;

INT32 execSQL ( sdb *connection, CHAR *sql ) ;

INT32 execSQL ( sdb *connection, CHAR *sql, sdbCursor **query ) ;

INT32 getSnapshot ( sdb *connection,
                    sdbCursor **query,
                    INT32 listType,
                    CHAR *condition,
                    CHAR *selector,
                    CHAR *orderBy ) ;

INT32 getList ( sdb *connection,
                sdbCursor **query,
                INT32 listType,
                CHAR *condition,
                CHAR *selector,
                CHAR *orderBy ) ;

INT32 resetSnapshot ( sdb *connection, CHAR *condition ) ;

INT32 selectCollectionSpace ( sdb *pConnection,
                              sdbCollectionSpace **cs,
                              const CHAR *csName,
                              INT32 pageSize ) ;

INT32 selectCollectionSpace2 ( sdb *pConnection,
                               sdbCollectionSpace **cs,
                               const CHAR *csName,
                               CHAR *pOptions ) ;

INT32 listCollectionSpaces ( sdb *connection, sdbCursor **query ) ;

INT32 listCollections ( sdb *connection, sdbCursor **query ) ;

INT32 selectGroup ( sdb *connection,
                    sdbReplicaGroup **group,
                    const CHAR *groupName ) ;

INT32 createCataGroup ( sdb *connection,
                        const CHAR *pHostName,
                        const CHAR *pServiceName,
                        const CHAR *pDatabasePath,
                        const CHAR *configure ) ;

INT32 dropCollectionSpace ( sdb *connection,
                            const CHAR *pCsName ) ;

INT32 createDomain( sdb *connection, const CHAR *pDomainName,
                    CHAR *pOptions, sdbDomain **dm );

INT32 dropDomain( sdb *connection, const CHAR *pDomainName ) ;

INT32 getDomain( sdb *connection, const CHAR *pDomainName, sdbDomain **dm ) ;

INT32 listDomains( sdb *connection, sdbCursor **query,
                   const CHAR *condition, const CHAR *selector,
                   const CHAR *orderBy, const CHAR *hint ) ;

/*
INT32 activateGroup ( sdb *connection,
                      const CHAR *groupName ) ;
*/
/************ sequoiadb_CS *********************/

INT32 selectCollection ( sdbCollectionSpace *cs,
                         sdbCollection **collection,
                         const CHAR *cName,
                         CHAR *shardingKey ) ;

INT32 collectionSpaceDrop  ( sdbCollectionSpace *cs ) ;

const CHAR *getCSName ( sdbCollectionSpace *cs ) ;

INT32 dropCollection ( sdbCollectionSpace *cs,
                       const CHAR *pClName ) ;

/*********** sequoiadb_collection ******************/

INT32 insertData ( sdbCollection *collection, CHAR **json_str ) ;

INT32 updateData ( sdbCollection *collection,
                   const CHAR *rule      ,
                   const CHAR *condition ,
                   const CHAR *hint ) ;

INT32 deleteData ( sdbCollection *collection,
                   const CHAR *condition ,
                   const CHAR *hint ) ;

INT32 queryData ( sdbCollection *collection, sdbCursor **query ,
                  const CHAR *condition , const CHAR *selected ,
                  const CHAR *orderBy   , const CHAR *hint     ,
                  INT64 numToSkip       , INT64 numToReturn ) ;

INT32 aggregate ( sdbCollection *collection, sdbCursor **query ,
                  std::vector<CHAR *> &json ) ;

INT32 splitData ( sdbCollection *collection,
                  CHAR *sourceGroupName,
                  CHAR *destGroupName,
                  CHAR *splitQueryCond,
                  CHAR *splitEndCond ) ;

INT32 splitData2 ( sdbCollection *collection,
                  CHAR *sourceGroupName,
                  CHAR *destGroupName,
                  FLOAT64 percent ) ;

INT32 collectionRename ( sdbCollection *collection, CHAR *pName ) ;

INT32 createIndex ( sdbCollection *collection,
                    const CHAR *indexDef,
                    const CHAR *pName,
                    BOOLEAN isUnique,
                    BOOLEAN isEnforced ) ;

INT32 getIndex ( sdbCollection *collection,
                 sdbCursor **query,
                 const CHAR *pName ) ;

INT32 dropIndex ( sdbCollection *collection,
                  const CHAR *pName ) ;

const CHAR *getCSName ( sdbCollection *collection ) ;

const CHAR *getClName ( sdbCollection *collection ) ;

const CHAR *getFullName ( sdbCollection *collection ) ;

INT32 collectionDrop ( sdbCollection *cs ) ;

INT32 getCount ( sdbCollection *collection,
                 const CHAR *condition ,
                 INT64 &count ) ;

/*************** cursor ******************************/

INT32 getNext ( sdbCursor *query, CHAR **pBuf, INT32 *bufSize ) ;

INT32 current ( sdbCursor *query, CHAR **pBuf, INT32 *bufSize ) ;

INT32 updateCurrent ( sdbCursor *query, const CHAR *rule ) ;

INT32 delCurrent ( sdbCursor *query ) ;

/*************** sdbDomain ******************************/

INT32 alterDomain ( sdbDomain *dm, CHAR *pOptions ) ;

INT32 listCollectionSpacesInDomain( sdbDomain *dm, sdbCursor **query ) ;

INT32 listCollectionsInDomain( sdbDomain *dm, sdbCursor **query ) ;

/*************** sdbReplicaGroup ******************************/

INT32 getNodeNum ( sdbReplicaGroup *gr, INT32 nudeType, INT32 *nodeNum ) ;

INT32 getDetail ( sdbReplicaGroup *gr, CHAR **pBuf, INT32 *bufSize ) ;

INT32 getMaster ( sdbReplicaGroup *gr, sdbNode **node ) ;

INT32 getSlave ( sdbReplicaGroup *gr, sdbNode **node ) ;

INT32 getNode ( sdbReplicaGroup *gr,
                sdbNode **node,
                const CHAR *nodeName ) ;

INT32 createNode ( sdbReplicaGroup *gr,
                   const CHAR *pHostName,
                   const CHAR *pServiceName,
                   const CHAR *pDatabasePath,
                   std::map<std::string,std::string> &config ) ;


INT32 groupStop ( sdbReplicaGroup *gr ) ;

INT32 groupStart ( sdbReplicaGroup *gr ) ;

BOOLEAN isCatalog ( sdbReplicaGroup *gr ) ;

/**************** *******************/

INT32 nodeConnect ( sdbNode *node,
                    sdb **connection ) ;

INT32 getStatus ( sdbNode *node ) ;

const CHAR *getHostName ( sdbNode *node ) ;

const CHAR *getServiceName ( sdbNode *node ) ;

const CHAR *getNodeName ( sdbNode *node ) ;

INT32 nodeStop ( sdbNode *node ) ;

INT32 nodeStart ( sdbNode *node ) ;

#endif
