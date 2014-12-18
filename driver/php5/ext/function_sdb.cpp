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

#include <string>
#include <vector>
#include "function_sdb.h"

#if defined (_LINUX)
#define SQDB_MAX_HOSTNAME 255
#elif defined (_WINDOWS)
#define SQDB_MAX_HOSTNAME 1025
#endif
#define SQDB_MAX_SERVICENAME           65
#define SQDB_COLLECTION_SPACE_NAME_SZ  128
#define SQDB_COLLECTION_NAME_SZ        128
using namespace bson ;
BOOLEAN strSplit( const CHAR *strtemp,
                  CHAR *left,
                  CHAR *right,
                  INT32 left_Size,
                  INT32 right_size,
                  CHAR str ) ;
BOOLEAN _reallocateBuffer ( CHAR **pBuf, INT32 *bufSize, INT32 reqSize ) ;

INT32 connect ( sdb *connection, const CHAR * hostName )
{
   BOOLEAN rc = TRUE ;
   CHAR  host [ SQDB_MAX_HOSTNAME ] = {0};
   CHAR  portName [ SQDB_MAX_SERVICENAME ] = {0} ;
   rc = strSplit ( hostName,
                   host,
                   portName,
                   SQDB_MAX_HOSTNAME,
                   SQDB_MAX_SERVICENAME,
                   ':' ) ;
   if ( !rc )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = connection->connect ( host, portName ) ;
done :
   return rc ;
error :
   goto done ;
}


INT32 connect ( sdb *connection,
                const CHAR *hostName,
                const CHAR *userName,
                const CHAR *password )
{
   BOOLEAN rc = TRUE ;
   CHAR  host [ SQDB_MAX_HOSTNAME ] = {0};
   CHAR  portName [ SQDB_MAX_SERVICENAME ] = {0} ;
   rc = strSplit ( hostName,
                   host,
                   portName,
                   SQDB_MAX_HOSTNAME,
                   SQDB_MAX_SERVICENAME,
                   ':' ) ;
   if ( !rc )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = connection->connect ( host, portName, userName, password ) ;
done :
   return rc ;
error :
   goto done ;
}

void close ( sdb *connection )
{
   connection->disconnect () ;
}

INT32 execSQL ( sdb *connection, CHAR *sql )
{
   INT32 rc = SDB_OK ;
   rc = connection->execUpdate ( sql ) ;
   return rc ;
}

INT32 execSQL ( sdb *connection, CHAR *sql, sdbCursor **query )
{
   INT32 rc = SDB_OK ;
   rc = connection->exec ( sql, **query ) ;
   return rc ;
}

INT32 selectGroup ( sdb *connection,
                    sdbReplicaGroup **group,
                    const CHAR *groupName )
{
   INT32 rc = SDB_OK ;
   rc = connection->getReplicaGroup ( groupName, **group ) ;
   if ( SDB_CLS_GRP_NOT_EXIST == rc )
   {
      rc = connection->createReplicaGroup ( groupName, **group ) ;
   }
   return rc ;
}

INT32 createCataGroup ( sdb *connection,
                        const CHAR *pHostName,
                        const CHAR *pServiceName,
                        const CHAR *pDatabasePath,
                        const CHAR *configure )
{
   INT32 rc = SDB_OK ;
   BSONObj configureBson ;
   if ( configure && *configure )
   {
      rc = fromjson ( configure, configureBson ) ;
      if ( rc )
      {
         return rc ;
      }
   }
   rc = connection->createReplicaCataGroup ( pHostName,
                                             pServiceName,
                                             pDatabasePath,
                                             configureBson ) ;
   return rc ;
}

INT32 dropCollectionSpace ( sdb *connection,
                            const CHAR *pCsName )
{
   INT32 rc = SDB_OK ;
   rc = connection->dropCollectionSpace ( pCsName ) ;
   return rc ;
}
/*
INT32 activateGroup ( sdb *connection,
                      const CHAR *groupName )
{
   INT32 rc = SDB_OK ;
   sdbReplicaGroup group ;
   rc = connection->getReplicaGroup ( groupName, group ) ;
   if ( !rc )
   {
      rc = connection->activateReplicaGroup ( groupName, group ) ;
   }
   return rc ;
}*/


INT32 getSnapshot ( sdb *connection,
                    sdbCursor **query,
                    INT32 listType,
                    CHAR *condition,
                    CHAR *selector,
                    CHAR *orderBy )
{
   INT32 rc = SDB_OK ;

   BSONObj condition_bson ;
   BSONObj selector_bson ;
   BSONObj orderBy_bson ;

   if ( condition && *condition )
   {
      rc = fromjson ( condition, condition_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( selector && *selector )
   {
      rc = fromjson ( selector, selector_bson ) ;
      if ( rc )
         return rc ;
    }

   if ( orderBy && *orderBy )
   {
      rc = fromjson ( orderBy, orderBy_bson ) ;
      if ( rc )
         return rc ;
   }

   rc = connection->getSnapshot ( **query,
                                  listType,
                                  condition_bson,
                                  selector_bson,
                                  orderBy_bson ) ;

   return rc ;
}

INT32 getList ( sdb *connection,
                sdbCursor **query,
                INT32 listType,
                CHAR *condition,
                CHAR *selector,
                CHAR *orderBy )
{
   INT32 rc = SDB_OK ;

   BSONObj condition_bson ;
   BSONObj selector_bson ;
   BSONObj orderBy_bson ;

   if ( condition && *condition )
   {
      rc = fromjson ( condition, condition_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( selector && *selector )
   {
      rc = fromjson ( selector, selector_bson ) ;
      if ( rc )
         return rc ;
    }

   if ( orderBy && *orderBy )
   {
      rc = fromjson ( orderBy, orderBy_bson ) ;
      if ( rc )
         return rc ;
   }

   rc = connection->getList ( **query,
                              listType,
                              condition_bson,
                              selector_bson,
                              orderBy_bson ) ;

   return rc ;
}

INT32 resetSnapshot ( sdb *connection, CHAR *condition )
{
   INT32 rc = SDB_OK ;
   BSONObj condition_bson ;

   if ( condition && *condition )
   {
      rc = fromjson ( condition, condition_bson ) ;
      if ( rc )
         return rc ;
   }

   rc = connection->resetSnapshot ( condition_bson ) ;
   return rc ;
}

INT32 selectCollectionSpace ( sdb *connection,
                              sdbCollectionSpace **cs,
                              const CHAR *csName,
                              INT32 pageSize )
{
   INT32 rc = SDB_OK ;
   rc = connection->getCollectionSpace ( csName, **cs ) ;
   if ( SDB_DMS_CS_NOTEXIST == rc )
   {
      rc = connection->createCollectionSpace ( csName, pageSize, **cs ) ;
   }
   return rc ;
}

INT32 selectCollectionSpace2 ( sdb *pConnection,
                               sdbCollectionSpace **cs,
                               const CHAR *csName,
                               CHAR *pOptions )
{
   INT32 rc = SDB_OK ;
   BSONObj options ;
   rc = pConnection->getCollectionSpace ( csName, **cs ) ;
   if ( SDB_DMS_CS_NOTEXIST == rc )
   {
      if ( pOptions && *pOptions )
      {
         rc = fromjson ( pOptions, options ) ;
         if ( rc )
         {
            return rc ;
         }
      }
      rc = pConnection->createCollectionSpace ( csName, options, **cs ) ;
   }
   return rc ;
}

INT32 listCollectionSpaces ( sdb *connection, sdbCursor **query )
{
   INT32 rc = SDB_OK ;
   rc = connection->listCollectionSpaces( **query ) ;
   return rc;
}

INT32 listCollections ( sdb *connection, sdbCursor **query )
{
   INT32 rc = SDB_OK ;
   rc = connection->listCollections ( **query ) ;
   return rc;
}

INT32 createDomain( sdb *connection, const CHAR *pDomainName,
                    CHAR *pOptions, sdbDomain **dm )
{
   INT32 rc = SDB_OK ;
   BSONObj options ;
   if ( pOptions && *pOptions )
   {
      rc = fromjson ( pOptions, options ) ;
      if ( rc )
      {
         return rc ;
      }
   }
   rc = connection->createDomain( pDomainName, options, **dm ) ;
   return rc ;
}

INT32 dropDomain( sdb *connection, const CHAR *pDomainName )
{
   return connection->dropDomain( pDomainName ) ;
}

INT32 getDomain( sdb *connection, const CHAR *pDomainName, sdbDomain **dm )
{
   return connection->getDomain( pDomainName, **dm ) ;
}

INT32 listDomains( sdb *connection, sdbCursor **query,
                   const CHAR *condition, const CHAR *selector,
                   const CHAR *orderBy, const CHAR *hint )
{
   INT32 rc = SDB_OK ;
   BSONObj condition_bson ;
   BSONObj selector_bson ;
   BSONObj orderBy_bson ;
   BSONObj hint_bson ;

   if ( condition && *condition )
   {
      rc = fromjson ( condition, condition_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( selector && *selector )
   {
      rc = fromjson ( selector, selector_bson ) ;
      if ( rc )
         return rc ;
    }

   if ( orderBy && *orderBy )
   {
      rc = fromjson ( orderBy, orderBy_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( hint && *hint )
   {
      rc = fromjson ( hint, hint_bson ) ;
      if ( rc )
         return rc ;
   }
   return connection->listDomains( **query, condition_bson, selector_bson,
                                   orderBy_bson, hint_bson ) ;
}

INT32 collectionSpaceDrop( sdbCollectionSpace *cs )
{
   INT32 rc = SDB_OK ;
   rc = cs->drop () ;
   return rc ;
}

const CHAR *getCSName ( sdbCollectionSpace *cs )
{
   const CHAR *csName = NULL ;
   csName = cs->getCSName () ;
   return csName ;
}

INT32 dropCollection ( sdbCollectionSpace *cs,
                       const CHAR *pClName )
{
   INT32 rc = SDB_OK ;
   rc = cs->dropCollection( pClName ) ;
   return rc ;
}

INT32 selectCollection ( sdbCollectionSpace *cs,
                         sdbCollection **collection,
                         const CHAR *cName,
                         CHAR *shardingKey )
{
   INT32 rc = SDB_OK ;
   rc = cs->getCollection ( cName, **collection ) ;
   if ( SDB_DMS_NOTEXIST == rc )
   {
      if ( shardingKey && *shardingKey )
      {
         BSONObj bs ;
         rc = fromjson ( shardingKey, bs ) ;
         if ( rc )
            return rc ;
         rc = cs->createCollection ( cName, bs, **collection ) ;
      }
      else
      {
         rc = cs->createCollection ( cName, **collection ) ;
      }
   }
   return rc ;
}

INT32 collectionDrop ( sdbCollection *collection )
{
   INT32 rc = SDB_OK ;
   rc = collection->drop () ;
   return rc ;
}

INT32 insertData ( sdbCollection *collection, CHAR **json_str )
{
   INT32 rc = SDB_OK ;
   OID oid ;
   INT32 len = 0 ;
   CHAR *temp = NULL ;
   BSONObj bs ;
   rc = fromjson ( *json_str, bs ) ;
   if ( rc )
   {
      return rc ;
   }

   rc = collection->insert ( bs, &oid ) ;
   if ( rc )
   {
      return rc ;
   }
   std::string str = oid.toString () ;
   const CHAR *p = str.c_str () ;
   INT32 eleSize = strlen(p) ;
   if ( eleSize > 0 )
   {
      CHAR *pTemp = (CHAR*)malloc(eleSize+1) ;
      if ( !pTemp )
         return SDB_OOM ;
      memcpy ( pTemp, p, eleSize ) ;
      pTemp[eleSize] = '\0' ;
      *json_str = pTemp ;
   }

   return rc ;
}

INT32 updateData ( sdbCollection *collection,
                   const CHAR *rule      ,
                   const CHAR *condition ,
                   const CHAR *hint )
{
   INT32 rc = SDB_OK ;
   BSONObj rule_bson ;
   BSONObj condition_bson ;
   BSONObj hint_bson ;

   if ( !rule )
   {
      return SDB_INVALIDARG ;
   }
   else
   {
      rc = fromjson ( rule, rule_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( condition && *condition )
   {
      rc = fromjson ( condition, condition_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( hint && *hint )
   {
      rc = fromjson ( hint, hint_bson ) ;
      if ( rc )
         return rc ;
   }

   rc = collection->update ( rule_bson, condition_bson, hint_bson ) ;

   return rc ;
}

INT32 deleteData ( sdbCollection *collection,
                   const CHAR *condition ,
                   const CHAR *hint )
{
   INT32 rc = SDB_OK ;
   BSONObj condition_bson ;
   BSONObj hint_bson ;

   if ( condition && *condition )
   {
      rc = fromjson ( condition, condition_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( hint && *hint )
   {
      rc = fromjson ( hint, hint_bson ) ;
      if ( rc )
         return rc ;
   }
   rc = collection->del ( condition_bson, hint_bson ) ;
   return rc ;
}

INT32 queryData ( sdbCollection *collection, sdbCursor **query ,
                  const CHAR *condition , const CHAR *selected ,
                  const CHAR *orderBy   , const CHAR *hint     ,
                  INT64 numToSkip       , INT64 numToReturn )
{
   INT32 rc = SDB_OK ;
   BSONObj condition_bson ;
   BSONObj selected_bson ;
   BSONObj orderBy_bson ;
   BSONObj hint_bson ;

   if ( condition && *condition )
   {
      rc = fromjson ( condition, condition_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( selected && *selected )
   {
      rc = fromjson ( selected, selected_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( orderBy && *orderBy )
   {
      rc = fromjson ( orderBy, orderBy_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( hint && *hint )
   {
      rc = fromjson ( hint, hint_bson ) ;
      if ( rc )
         return rc ;
   }


   rc = collection->query ( **query        ,
                            condition_bson ,
                            selected_bson  ,
                            orderBy_bson   ,
                            hint_bson      ,
                            numToSkip      ,
                            numToReturn ) ;

   return rc ;
}

INT32 aggregate ( sdbCollection *collection, sdbCursor **query ,
                  std::vector<CHAR *> &json )
{
   INT32 rc = SDB_OK ;
   std::vector<CHAR *>::iterator iter ;
   std::vector<BSONObj> bsonObj ;

   for ( iter = json.begin(); iter != json.end(); ++iter )
   {
      BSONObj bsonRecord ;
      rc = fromjson ( *iter, bsonRecord ) ;
      if ( rc )
      {
         return rc ;
      }
      bsonObj.push_back ( bsonRecord ) ;
   }

   rc = collection->aggregate ( **query, bsonObj ) ;

   return rc ;
}

INT32 splitData ( sdbCollection *collection,
                  CHAR *sourceGroupName,
                  CHAR *destGroupName,
                  CHAR *splitQueryCond,
                  CHAR *splitEndCond )
{
   INT32 rc = SDB_OK ;
   BSONObj condition_bson ;
   BSONObj endCondition_bson ;

   if ( splitQueryCond && *splitQueryCond )
   {
      rc = fromjson ( splitQueryCond, condition_bson ) ;
      if ( rc )
         return rc ;
   }

   if ( splitEndCond && *splitEndCond )
   {
      rc = fromjson ( splitEndCond, endCondition_bson ) ;
      if ( rc )
         return rc ;
   }

   rc = collection->split ( sourceGroupName,
                            destGroupName,
                            condition_bson,
                            endCondition_bson ) ;

   return rc ;
}

INT32 splitData2 ( sdbCollection *collection,
                   CHAR *sourceGroupName,
                   CHAR *destGroupName,
                   FLOAT64 percent )
{
   INT32 rc = SDB_OK ;
   BSONObj condition_bson ;
   BSONObj endCondition_bson ;

   rc = collection->split ( sourceGroupName,
                            destGroupName,
                            percent ) ;

   return rc ;
}


/*INT32 collectionRename ( sdbCollection *collection, CHAR *pName )
{
   INT32 rc = SDB_OK ;
   rc = collection->rename ( pName ) ;
   return rc;
}*/

INT32 createIndex ( sdbCollection *collection,
                    const CHAR *indexDef,
                    const CHAR *pName,
                    BOOLEAN isUnique,
                    BOOLEAN isEnforced )
{
   INT32 rc = SDB_OK ;
   BSONObj indexDef_bson ;

   if ( indexDef )
   {
      rc = fromjson ( indexDef, indexDef_bson ) ;
      if ( rc )
         return rc ;
   }
   else
   {
      return SDB_INVALIDARG;
   }
   rc = collection->createIndex ( indexDef_bson,
                                  pName,
                                  isUnique,
                                  isEnforced ) ;

   return rc ;
}

INT32 getIndex ( sdbCollection *collection,
                 sdbCursor **query,
                 const CHAR *pName )
{
   INT32 rc = SDB_OK ;
   rc = collection->getIndexes ( **query, pName ) ;
   return rc ;
}

INT32 dropIndex ( sdbCollection *collection,
                  const CHAR *pName )
{
   INT32 rc = SDB_OK ;
   rc = collection->dropIndex ( pName ) ;
   return rc ;
}


const CHAR *getCSName ( sdbCollection *collection )
{
   const CHAR *csName = NULL ;
   csName = collection->getCSName () ;
   return csName ;
}


const CHAR *getClName ( sdbCollection *collection )
{
   const CHAR *clName = NULL ;
   clName = collection->getCollectionName () ;
   return clName ;
}


const CHAR *getFullName ( sdbCollection *collection )
{
   const CHAR *fullName = NULL ;
   fullName = collection->getFullName () ;
   return fullName ;
}

INT32 getCount ( sdbCollection *collection,
                 const CHAR *condition ,
                 INT64 &count )
{
   INT32 rc = SDB_OK ;
   BSONObj condition_bson ;
   if ( condition && *condition )
   {
      rc = fromjson ( condition, condition_bson ) ;
      if ( rc )
         return rc ;
   }
   rc = collection->getCount ( count, condition_bson ) ;
   return rc ;
}

INT32 getNext ( sdbCursor *query, CHAR **pBuf, INT32 *bufSize )
{
   INT32 rc = SDB_OK ;
   BSONObj obj ;

   rc = query->next ( obj ) ;
   if ( rc )
   {
      return rc ;
   }
   std::string str = obj.toString(FALSE,TRUE) ;
   INT32 strLen = strlen ( str.c_str() ) ;
   if ( !_reallocateBuffer ( pBuf, bufSize, *bufSize + strLen + 1 ) )
   {
      rc = SDB_PHP_DRIVER_INTERNAL_ERROR ;
      return rc ;
   }
   memcpy ( *pBuf, str.c_str(), strLen ) ;
   *(*pBuf+strLen) = '\0' ;
   return rc ;
}

INT32 current ( sdbCursor *query, CHAR **pBuf, INT32 *bufSize )
{
   INT32 rc = SDB_OK ;
   BSONObj obj ;

   rc = query->current ( obj ) ;
   if ( rc )
   {
      return rc ;
   }
   std::string str = obj.toString(FALSE,TRUE) ;
   INT32 strLen = strlen ( str.c_str() ) ;
   if ( !_reallocateBuffer ( pBuf, bufSize, *bufSize + strLen ) )
   {
      rc = SDB_PHP_DRIVER_INTERNAL_ERROR ;
      return rc ;
   }
   memcpy ( *pBuf, str.c_str(), strLen ) ;
   *(*pBuf+strLen) = '\0' ;
   return rc ;
}

/*
INT32 updateCurrent ( sdbCursor *query, const CHAR *rule )
{
   INT32 rc = SDB_OK ;
   bson *rule_bson = NULL ;
   if ( rule )
   {
      rule_bson = bson_create () ;
      if( !jsonToBson ( rule_bson, rule) )
      {
          bson_finish  ( rule_bson ) ;
          bson_dispose ( rule_bson ) ;
          return SDB_INVALIDARG ;
      }
      bson_finish ( rule_bson ) ;
   }
   else
   {
      return SDB_INVALIDARG;
   }
   rc = query->updateCurrent ( *rule_bson ) ;
   bson_dispose ( rule_bson ) ;
   return rc ;
}

INT32 delCurrent ( sdbCursor *query )
{
   INT32 rc = SDB_OK ;
   rc = query->delCurrent () ;
   return rc ;
}*/

/*************** sdbDomain ******************************/

const CHAR *getDomainName( sdbDomain *dm )
{
   return dm->getName() ;
}

INT32 alterDomain ( sdbDomain *dm, CHAR *pOptions )
{
   INT32 rc = SDB_OK ;
   BSONObj bs ;
   rc = fromjson ( pOptions, bs ) ;
   if ( rc )
   {
      return rc ;
   }
   return dm->alterDomain( bs ) ;
}

INT32 listCollectionSpacesInDomain( sdbDomain *dm, sdbCursor **query )
{
   return dm->listCollectionSpacesInDomain( **query ) ;
}

INT32 listCollectionsInDomain( sdbDomain *dm, sdbCursor **query )
{
   return dm->listCollectionsInDomain( **query ) ;
}

/*************** sdbReplicaGroup ******************************/

INT32 getNodeNum ( sdbReplicaGroup *gr, INT32 nudeType, INT32 *nodeNum )
{
   INT32 rc = SDB_OK ;
   sdbNodeStatus status = (sdbNodeStatus)nudeType ;
   rc = gr->getNodeNum ( status, nodeNum ) ;
   return rc ;
}

INT32 getDetail ( sdbReplicaGroup *gr, CHAR **pBuf, INT32 *bufSize )
{
   INT32 rc = SDB_OK ;
   BSONObj obj ;
   rc = gr->getDetail ( obj ) ;
   if ( rc )
   {
      return rc ;
   }
   std::string str = obj.toString(FALSE,TRUE) ;
   INT32 strLen = strlen ( str.c_str() ) ;
   if ( !_reallocateBuffer ( pBuf, bufSize, *bufSize + strLen ) )
   {
      rc = SDB_PHP_DRIVER_INTERNAL_ERROR ;
      return rc ;
   }
   memcpy ( pBuf, str.c_str(), strLen ) ;
   pBuf[strLen] = '\0' ;
   return rc ;
}

INT32 getMaster ( sdbReplicaGroup *gr, sdbNode **node )
{
   INT32 rc = SDB_OK ;
   rc = gr->getMaster ( **node ) ;
   return rc ;
}

INT32 getSlave ( sdbReplicaGroup *gr, sdbNode **node )
{
   INT32 rc = SDB_OK ;
   rc = gr->getSlave ( **node ) ;
   return rc ;
}

INT32 getNode ( sdbReplicaGroup *gr,
                sdbNode **node,
                const CHAR *nodeName )
{
   INT32 rc = SDB_OK ;
   rc = gr->getNode ( nodeName, **node ) ;
   return rc ;
}

INT32 createNode ( sdbReplicaGroup *gr,
                   const CHAR *pHostName,
                   const CHAR *pServiceName,
                   const CHAR *pDatabasePath,
                   std::map<std::string,std::string> &config )
{
   INT32 rc = SDB_OK ;
   rc = gr->createNode ( pHostName,
                         pServiceName,
                         pDatabasePath,
                         config ) ;
   return rc ;  
}

/*
INT32 activate ( sdbReplicaGroup *gr )
{
   INT32 rc = SDB_OK ;
   rc = gr->activate() ;
   return rc ;
}
*/

INT32 groupStop ( sdbReplicaGroup *gr )
{
   INT32 rc = SDB_OK ;
   rc = gr->stop() ;
   return rc ;
}

INT32 groupStart ( sdbReplicaGroup *gr )
{
   INT32 rc = SDB_OK ;
   rc = gr->start() ;
   return rc ;
}

BOOLEAN isCatalog ( sdbReplicaGroup *gr )
{
   return gr->isCatalog() ;
}
/*************** sdbNode ******************************/

INT32 nodeConnect ( sdbNode *node,
                    sdb **connection )
{
   INT32 rc = SDB_OK ;
   rc = node->connect ( **connection ) ;
   return rc ;
}

INT32 getStatus ( sdbNode *node )
{
   INT32 status = node->getStatus() ;
   return status ;
}

const CHAR *getHostName ( sdbNode *node )
{
   const CHAR *hostName = NULL ;
   hostName = node->getHostName () ;
   return hostName ;
}

const CHAR *getServiceName ( sdbNode *node )
{
   const CHAR *serviceName = NULL ;
   serviceName = node->getServiceName () ;
   return serviceName ;
}

const CHAR *getNodeName ( sdbNode *node )
{
   const CHAR *nodeName = NULL ;
   nodeName = node->getNodeName () ;
   return nodeName ;
}

INT32 nodeStop ( sdbNode *node )
{
   INT32 rc = SDB_OK ;
   rc = node->stop() ;
   return rc ;
}

INT32 nodeStart ( sdbNode *node )
{
   INT32 rc = SDB_OK ;
   rc = node->start() ;
   return rc ;
}


BOOLEAN _reallocateBuffer ( CHAR **pBuf, INT32 *bufSize, INT32 reqSize )
{
   INT32 currentSize = *bufSize ;
   CHAR *pOldBuf     = *pBuf ;
   if ( reqSize > *bufSize )
   {
      CHAR *pOldBuf = *pBuf ;
      reqSize = ossRoundUpToMultipleX ( reqSize, SDB_PAGE_SIZE ) ;
      *pBuf = (CHAR*)realloc(*pBuf, sizeof(CHAR)*reqSize ) ;
      if ( !pBuf )
      {
         *pBuf = pOldBuf ;
         return FALSE ;
      }
      *bufSize = reqSize ;
   }
   return TRUE ;
}

BOOLEAN strSplit( const CHAR *strtemp, CHAR *left, CHAR *right, INT32 left_Size, INT32 right_size, CHAR str )
{
   CHAR *pBuffer = new( std::nothrow ) CHAR [ left_Size + right_size + 1 ] ;
   if ( !pBuffer )
       return FALSE ;
   memset  ( pBuffer,   0, left_Size + right_size + 1 ) ;
   strncpy ( pBuffer, strtemp, left_Size + right_size ) ;
   CHAR *host = pBuffer ;
   CHAR *pos  = strchr  ( host, str ) ;
   CHAR *pos1 = strrchr ( host, str ) ;
   if ( !pos || !pos1 )
   {
      if( ':' == str )
      {
         strncpy ( left, host, left_Size ) ;
         strncpy ( right, "50000", right_size ) ;
         delete[] pBuffer ;
         pBuffer = NULL ;
         return TRUE ;
      }
      delete[] pBuffer ;
      pBuffer = NULL ;
      return FALSE ;
    }
    if ( pos != pos1 )
    {
       delete[] pBuffer ;
       pBuffer = NULL ;
       return FALSE ;
    }
    *pos = '\0' ;
    pos1 = pos + 1 ;
    strncpy ( left, host, left_Size ) ;
    strncpy ( right, pos1, right_size ) ;
    delete[] pBuffer ;
    pBuffer = NULL ;
    return TRUE ;
}
