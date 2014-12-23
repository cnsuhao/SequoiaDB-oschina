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


/** \file client.hpp
    \brief C++ Client Driver
*/


#ifndef CLIENT_HPP__
#define CLIENT_HPP__
#include "core.hpp"
#if defined (SDB_ENGINE) || defined (SDB_CLIENT)
#include "../bson/bson.h"
#include "../util/fromjson.hpp"
#else
#include "bson/bson.hpp"
#include "fromjson.hpp"
#endif
#include "spd.h"
#include <map>
#include <string>
#include <vector>
/*
#if defined (_WINDOWS)
   #if defined (SDB_DLL_BUILD)
      #define DLLEXPORT __declspec(dllexport)
   #else
      #define DLLEXPORT __declspec(dllimport)
   #endif
#else
   #define DLLEXPORT
#endif
*/

#define DLLEXPORT SDB_EXPORT

#define SDB_PAGESIZE_4K           4096
#define SDB_PAGESIZE_8K           8192
#define SDB_PAGESIZE_16K          16384
#define SDB_PAGESIZE_32K          32768
#define SDB_PAGESIZE_64K          65536
/** 0 means using database's default pagesize, it 64k now */
#define SDB_PAGESIZE_DEFAULT      0

#define SDB_SNAP_CONTEXTS         0
#define SDB_SNAP_CONTEXTS_CURRENT 1
#define SDB_SNAP_SESSIONS         2
#define SDB_SNAP_SESSIONS_CURRENT 3
#define SDB_SNAP_COLLECTIONS      4
#define SDB_SNAP_COLLECTIONSPACES 5
#define SDB_SNAP_DATABASE         6
#define SDB_SNAP_SYSTEM           7
#define SDB_SNAP_CATALOG          8

#define SDB_LIST_CONTEXTS         0
#define SDB_LIST_CONTEXTS_CURRENT 1
#define SDB_LIST_SESSIONS         2
#define SDB_LIST_SESSIONS_CURRENT 3
#define SDB_LIST_COLLECTIONS      4
#define SDB_LIST_COLLECTIONSPACES 5
#define SDB_LIST_STORAGEUNITS     6
#define SDB_LIST_GROUPS           7
#define SDB_LIST_STOREPROCEDURES  8
#define SDB_LIST_DOMAINS          9
#define SDB_LIST_TASKS            10
#define SDB_LIST_CS_IN_DOMAIN     11
#define SDB_LIST_CL_IN_DOMAIN     12

/** The flags represent whether bulk insert continue when hitting index key duplicate error */
#define FLG_INSERT_CONTONDUP  0x00000001

#define SDB_CLIENT_SOCKET_TIMEOUT_DFT 10000

/** class name 'sdbReplicaNode' will be deprecated in version 2.x, use 'sdbNode' instead of it. */
#define sdbReplicaNode         sdbNode

#define activateReplicaGroup   activateReplicaGroup

enum _SDB_LOB_OPEN_MODE
{
   SDB_LOB_CREATEONLY = 0x00000001, /**< Open a new lob only */
   SDB_LOB_READ = 0x00000004        /**< Open an existing lob to read */
} ;
typedef enum _SDB_LOB_OPEN_MODE SDB_LOB_OPEN_MODE ;

enum _SDB_LOB_SEEK
{
   SDB_LOB_SEEK_SET = 0, /**< Seek from the beginning of file */
   SDB_LOB_SEEK_CUR,     /**< Seek from the current place */
   SDB_LOB_SEEK_END      /**< Seek from the end of file  */
} ;
typedef enum _SDB_LOB_SEEK SDB_LOB_SEEK ;

/** \namespace sdbclient
    \brief SequoiaDB Driver for C++
*/
namespace sdbclient
{
   const static bson::BSONObj _sdbStaticObject ;
   const static bson::OID _sdbStaticOid ;
   class _sdbCursor ;
   class _sdbCollection ;
   class sdb ;
   class _sdb ;
   class _ossSocket ;
   class _sdbLob ;
   class sdbLob ;

   class DLLEXPORT _sdbCursor
   {
   private :
      _sdbCursor ( const _sdbCursor& other ) ;
      _sdbCursor& operator=( const _sdbCursor& ) ;
   public :
      _sdbCursor () {}
      virtual ~_sdbCursor () {}
      virtual INT32 next          ( bson::BSONObj &obj ) = 0 ;
      virtual INT32 current       ( bson::BSONObj &obj ) = 0 ;
      virtual INT32 close () = 0 ;
   } ;

/** \class  sdbCursor
      \brief Database operation interfaces of cursor.
*/
   class DLLEXPORT sdbCursor
   {
   private :
      sdbCursor ( const sdbCursor& other ) ;
      sdbCursor& operator=( const sdbCursor& ) ;
   public :
/** \var pCursor
      \breif A pointer of virtual base class _sdbCursor

      Class sdbCursor is a shell for _sdbCursor. We use pCursor to
      call the methods in class _sdbCursor.
*/
      _sdbCursor *pCursor ;

/** \fn sdbCursor ()
      \brief default constructor
*/
      sdbCursor ()
      {
         pCursor = NULL ;
      }

/** \fn ~sdbCursor ()
      \brief destructor
*/
      ~sdbCursor ()
      {
         if ( pCursor )
            delete pCursor ;
      }

/** \fn  INT32 next ( bson::BSONObj &obj )
      \brief Return the next document of current cursor, and move forward
      \param [out] obj The return bson object
      \retval SDB_OK Operation Success
      \retval Others Operation Fail
*/
      INT32 next ( bson::BSONObj &obj )
      {
         if ( !pCursor )
            return SDB_NOT_CONNECTED ;
         return pCursor->next ( obj ) ;
      }

/** \fn INT32 current ( bson::BSONObj &obj )
      \brief Return the current document of cursor, and don't move
      \param [out] obj The return bson object
      \retval SDB_OK Operation Success
      \retval Others Operation Fail
*/
      INT32 current ( bson::BSONObj &obj )
      {
         if ( !pCursor )
            return SDB_NOT_CONNECTED ;
         return pCursor->current ( obj ) ;
      }

/** \fn INT32 close ()
      \brief Close the cursor's connection to database.
      \retval SDB_OK Operation Success
      \retval Others Operation Fail
*/
      INT32 close ()
      {
         if ( !pCursor )
            return SDB_OK ;
         return pCursor->close () ;
      }

/*
* \fn INT32 updateCurrent ( bson::BSONObj &rule )
    \brief Update the current document of cursor
    \param [in] rule The updating rule, cannot be null
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
      INT32 updateCurrent ( bson::BSONObj &rule )
      {
         if ( !pCursor )
            return SDB_NOT_CONNECTED ;
         return pCursor->updateCurrent ( rule ) ;
      }

* \fn INT32 delCurrent ()
      \brief Delete the current document of cursor
      \retval SDB_OK Operation Success
      \retval Others Operation Fail

      INT32 delCurrent ()
      {
         if ( !pCursor )
            return SDB_NOT_CONNECTED ;
         return pCursor->delCurrent () ;
      }*/
   } ;

   class DLLEXPORT _sdbCollection
   {
   private :
      _sdbCollection ( const _sdbCollection& other ) ;
      _sdbCollection& operator=( const _sdbCollection& ) ;
   public :
      _sdbCollection () {}
      virtual ~_sdbCollection () {}
      virtual INT32 getCount ( SINT64 &count,
                               const bson::BSONObj &condition = _sdbStaticObject ) = 0 ;

      virtual INT32 insert ( const bson::BSONObj &obj, bson::OID *id = NULL ) = 0 ;

      virtual INT32 bulkInsert ( SINT32 flags,
                                 std::vector<bson::BSONObj> &obj
                               ) = 0 ;
      virtual INT32 update ( const bson::BSONObj &rule,
                             const bson::BSONObj &condition = _sdbStaticObject,
                             const bson::BSONObj &hint      = _sdbStaticObject
                           ) = 0 ;

      virtual INT32 upsert ( const bson::BSONObj &rule,
                             const bson::BSONObj &condition = _sdbStaticObject,
                             const bson::BSONObj &hint      = _sdbStaticObject
                           ) = 0 ;

      virtual INT32 del ( const bson::BSONObj &condition = _sdbStaticObject,
                          const bson::BSONObj &hint      = _sdbStaticObject
                        ) = 0 ;

      virtual INT32 query  ( _sdbCursor **cursor,
                             const bson::BSONObj &condition = _sdbStaticObject,
                             const bson::BSONObj &selected  = _sdbStaticObject,
                             const bson::BSONObj &orderBy   = _sdbStaticObject,
                             const bson::BSONObj &hint      = _sdbStaticObject,
                             INT64 numToSkip    = 0,
                             INT64 numToReturn  = -1,
                             INT32 flag         = 0
                           ) = 0 ;

      virtual INT32 query  ( sdbCursor &cursor,
                             const bson::BSONObj &condition = _sdbStaticObject,
                             const bson::BSONObj &selected  = _sdbStaticObject,
                             const bson::BSONObj &orderBy   = _sdbStaticObject,
                             const bson::BSONObj &hint      = _sdbStaticObject,
                             INT64 numToSkip    = 0,
                             INT64 numToReturn  = -1,
                             INT32 flag         = 0
                           ) = 0 ;
      virtual INT32 createIndex ( const bson::BSONObj &indexDef,
                                  const CHAR *pName,
                                  BOOLEAN isUnique,
                                  BOOLEAN isEnforced
                                ) = 0 ;
      virtual INT32 getIndexes ( _sdbCursor **cursor,
                                 const CHAR *pName ) = 0 ;
      virtual INT32 getIndexes ( sdbCursor &cursor,
                                 const CHAR *pName ) = 0 ;
      virtual INT32 dropIndex ( const CHAR *pName ) = 0 ;
      virtual INT32 create () = 0 ;
      virtual INT32 drop () = 0 ;
      virtual const CHAR *getCollectionName () = 0 ;
      virtual const CHAR *getCSName () = 0 ;
      virtual const CHAR *getFullName () = 0 ;
      virtual INT32 split ( const CHAR *pSourceGroupName,
                            const CHAR *pTargetGroupName,
                            const bson::BSONObj &splitConditon,
                            const bson::BSONObj &splitEndCondition = _sdbStaticObject) = 0 ;
      virtual INT32 split ( const CHAR *pSourceGroupName,
                            const CHAR *pTargetGroupName,
                            FLOAT64 percent ) = 0 ;
      virtual INT32 splitAsync ( SINT64 &taskID,
                            const CHAR *pSourceGroupName,
                            const CHAR *pTargetGroupName,
                            const bson::BSONObj &splitCondition,
                            const bson::BSONObj &splitEndCondition = _sdbStaticObject) = 0 ;
      virtual INT32 splitAsync ( const CHAR *pSourceGroupName,
                            const CHAR *pTargetGroupName,
                            FLOAT64 percent,
                            SINT64 &taskID ) = 0 ;
      virtual  INT32 aggregate ( _sdbCursor **cursor,
                                std::vector<bson::BSONObj> &obj
                               )  = 0 ;
      virtual  INT32 aggregate ( sdbCursor &cursor,
                                std::vector<bson::BSONObj> &obj
                               )  = 0 ;
      virtual INT32 getQueryMeta  ( _sdbCursor **cursor,
                             const bson::BSONObj &condition = _sdbStaticObject,
                             const bson::BSONObj &orderBy   = _sdbStaticObject,
                             const bson::BSONObj &hint  = _sdbStaticObject,
                             INT64 numToSkip    = 0,
                             INT64 numToReturn  = -1
                           ) = 0 ;
      virtual INT32 getQueryMeta  ( sdbCursor &cursor,
                             const bson::BSONObj &condition = _sdbStaticObject,
                             const bson::BSONObj &orderBy   = _sdbStaticObject,
                             const bson::BSONObj &hint  = _sdbStaticObject,
                             INT64 numToSkip    = 0,
                             INT64 numToReturn  = -1
                           ) = 0 ;
      virtual INT32 attachCollection ( const CHAR *subClFullName,
                                      const bson::BSONObj &options) = 0 ;
      virtual INT32 detachCollection ( const CHAR *subClFullName) = 0 ;

      virtual INT32 alterCollection ( const bson::BSONObj &options ) = 0 ;
      virtual INT32 explain ( sdbCursor &cursor,
                              const bson::BSONObj &condition = _sdbStaticObject,
                              const bson::BSONObj &select    = _sdbStaticObject,
                              const bson::BSONObj &orderBy   = _sdbStaticObject,
                              const bson::BSONObj &hint      = _sdbStaticObject,
                              INT64 numToSkip                = 0,
                              INT64 numToReturn              = -1,
                              INT32 flag                     = 0,
                              const bson::BSONObj &options   = _sdbStaticObject ) = 0 ;
      virtual INT32 createLob( sdbLob &lob, const bson::OID *oid = NULL ) = 0 ;

      virtual INT32 removeLob( const bson::OID &oid ) = 0 ;

      virtual INT32 openLob( sdbLob &lob, const bson::OID &oid ) = 0 ;
      
      virtual INT32 listLobs( sdbCursor &cursor ) = 0 ;
      
   } ;

/** \class sdbCollection
      \brief Database operation interfaces of collection.
*/
   class DLLEXPORT sdbCollection
   {
   private :
/** \fn sdbCollection ( const sdbCollection& other ) ;
      \brief Copy constructor
      \param[in] A const object reference of class sdbCollection.
*/
      sdbCollection ( const sdbCollection& other ) ;

/** \fn sdbCollection& operator=( const sdbCollection& )
      \brief Assignment constructor
      \param[in] a const reference of class sdbCollection.
      \retval A const object reference of class sdbCollection.
*/
      sdbCollection& operator=( const sdbCollection& ) ;
   public :
/** \var pCollection
      \breif A pointer of virtual base class _sdbCollection

      Class sdbCollection is a shell for _sdbCollection. We use pCollection to
      call the methods in class _sdbCollection.
*/
      _sdbCollection *pCollection ;

/** \fn sdbCollection ()
    \brief Default constructor
*/
      sdbCollection ()
      {
         pCollection = NULL ;
      }

/** \fn ~sdbCollection ()
    \brief Destructor.
*/
      ~sdbCollection ()
      {
         if ( pCollection )
            delete pCollection ;
      }

/** \fn INT32 getCount ( SINT64 &count,
                         const bson::BSONObj &condition )
    \brief Get the count of matching documents in current collection.
    \param [in] condition The matching rule, return the count of all documents if this parameter is empty
    \param [out] count The count of matching documents, matches all records if not provided.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getCount ( SINT64 &count,
                       const bson::BSONObj &condition = _sdbStaticObject )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->getCount ( count, condition ) ;
      }

/** \fn INT32 split ( const CHAR *pSourceGroupName,
                      const CHAR *pTargetGroupName,
                      const bson::BSONObj &splitCondition,
                      const bson::BSONObj &splitEndCondition)
    \brief Split the specified collection from source replica group
           to target replica group by range.
    \param [in] pSourceGroupName The source replica group name
    \param [in] pTargetGroupName The target replica group name
    \param [in] splitCondition The split condition
    \param [in] splitEndCondition The split end condition or null
              eg:If we create a collection with the option {ShardingKey:{"age":1},ShardingType:"Hash",Partition:2^10},
             we can fill {age:30} as the splitCondition, and fill {age:60} as the splitEndCondition. when split,
             the target replica group will get the records whose age's hash value are in [30,60). If splitEndCondition is null,
             they are in [30,max).
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 split ( const CHAR *pSourceGroupName,
                    const CHAR *pTargetGroupName,
                    const bson::BSONObj &splitCondition,
                    const bson::BSONObj &splitEndCondition = _sdbStaticObject)
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->split ( pSourceGroupName,
                                     pTargetGroupName,
                                     splitCondition,
                                     splitEndCondition) ;
      }

/** \fn INT32 split ( const CHAR *pSourceGroupName,
                      const CHAR *pTargetGroupName,
                      FLOAT64 percent )
    \brief Split the specified collection from source replica group to target
           replica group by percent.
    \param [in] pSourceGroupName The source replica group name
    \param [in] pTargetGroupName The target replica group name
    \param [in] percent The split percent, Range:(0,100]
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 split ( const CHAR *pSourceGroupName,
                    const CHAR *pTargetGroupName,
                    FLOAT64 percent )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->split ( pSourceGroupName,
                                     pTargetGroupName,
                                     percent ) ;
      }

/** \fn INT32 splitAsync ( SINT64 &taskID,
                                   const CHAR *pSourceGroupName,
                                   const CHAR *pTargetGroupName,
                                   const bson::BSONObj &splitCondition,
                                   const bson::BSONObj &splitEndCondition )
    \brief Split the specified collection from source replica group to target
           replica group by range
    \param [out] taskID The id of current split task
    \param [in] pSourceGroupName The source replica group name
    \param [in] pTargetGroupName The target replica group name
    \param [in] splitCondition The split condition
    \param [in] splitEndCondition The split end condition or null
              eg:If we create a collection with the option {ShardingKey:{"age":1},ShardingType:"Hash",Partition:2^10},
              we can fill {age:30} as the splitCondition, and fill {age:60} as the splitEndCondition. when split,
              the target replica group will get the records whose age's hash value are in [30,60). If splitEndCondition is null,
              they are in [30,max).
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 splitAsync ( SINT64 &taskID,
                         const CHAR *pSourceGroupName,
                         const CHAR *pTargetGroupName,
                         const bson::BSONObj &splitCondition,
                         const bson::BSONObj &splitEndCondition = _sdbStaticObject )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->splitAsync ( taskID,
                                          pSourceGroupName,
                                          pTargetGroupName,
                                          splitCondition,
                                          splitEndCondition ) ;
      }


/** \fn INT32 INT32 splitAsync ( const CHAR *pSourceGroup,
                                 const CHAR *pTargetGroup,
                                 FLOAT64 percent,
                                 SINT64 &taskID )
    \brief Split the specified collection from source replica group to target
           replica group by percent
    \param [in] pSourceGroupName The source replica group name
    \param [in] pTargetGroupName The target replica group name
    \param [in] percent The split percent, Range:(0.0, 100.0]
    \param [out] taskID The id of current split task
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 splitAsync ( const CHAR *pSourceGroupName,
                         const CHAR *pTargetGroupName,
                         FLOAT64 percent,
                         SINT64 &taskID )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->splitAsync ( pSourceGroupName,
                                          pTargetGroupName,
                                          percent,
                                          taskID ) ;
      }


/** \fn INT32 alterCollection ( const bson::BSONObj &options )
    \brief Alter the current collection
    \param [in] options The modified options as following:

        ReplSize     : Assign how many replica nodes need to be synchronized when a write request(insert, update, etc) is executed
        ShardingKey  : Assign the sharding key
        ShardingType : Assign the sharding type
        Partition    : When the ShardingType is "hash", need to assign Partition, it's the bucket number for hash, the range is [2^3,2^20]
                       e.g. {RepliSize:0, ShardingKey:{a:1}, ShardingType:"hash", Partition:1024}
    \note Can't alter attributes about split in partition collection; After altering a collection to
          be a partition collection, need to split this collection manually
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 alterCollection ( const bson::BSONObj &options )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->alterCollection ( options ) ;
      }

/** \fn  INT32 bulkInsert ( SINT32 flags,
                         std::vector<bson::BSONObj> &obj
                       )
    \brief Insert a bulk of bson objects into current collection
    \param [in] flags FLG_INSERT_CONTONDUP or 0. While FLG_INSERT_CONTONDUP
                is set, if some records hit index key duplicate error,
                database will skip them and go on inserting. However, while 0 
                is set, database will stop inserting in that case, and return
                errno code.
    \param [in] obj The array of inserted bson objects
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 bulkInsert ( SINT32 flags,
                         std::vector<bson::BSONObj> &obj
                       )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->bulkInsert ( flags, obj ) ;
      }

/** \fn INT32 insert ( bson::BSONObj &obj, BSONElement *id = NULL )
    \brief Insert a bson object into current collection
    \param [in] obj The inserted bson object
    \param [out] id The object id of inserted bson object in current collection, the memory of id will be invalidated when next insert/bulkInsert is performed or the obj is destroyed
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 insert ( const bson::BSONObj &obj, bson::OID *id = NULL )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->insert ( obj, id ) ;
      }

/** \fn  INT32 update ( const bson::BSONObj &rule,
                     const bson::BSONObj &condition,
                     const bson::BSONObj &hint
                   )
    \brief Update the matching documents in current collection
    \param [in] rule The updating rule
    \param [in] condition The matching rule, update all the documents if not provided
    \param [in] hint The hint, automatically match the optimal hint if not provided
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \note It won't work to update the "ShardingKey" field, but the other fields take effect
*/
      INT32 update ( const bson::BSONObj &rule,
                     const bson::BSONObj &condition = _sdbStaticObject,
                     const bson::BSONObj &hint      = _sdbStaticObject
                   )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->update ( rule, condition, hint ) ;
      }

/** \fn INT32 upsert ( const bson::BSONObj &rule,
                     const bson::BSONObj &condition = _sdbStaticObject,
                     const bson::BSONObj &hint      = _sdbStaticObject
                   )
    \brief Update the matching documents in current collection, insert if no matching
    \param [in] rule The updating rule
    \param [in] condition The matching rule, update all the documents if not provided
    \param [in] hint The hint, automatically match the optimal hint if not provided
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \note It won't work to upsert the "ShardingKey" field, but the other fields take effect
*/
      INT32 upsert ( const bson::BSONObj &rule,
                     const bson::BSONObj &condition = _sdbStaticObject,
                     const bson::BSONObj &hint      = _sdbStaticObject
                   )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->upsert ( rule, condition, hint ) ;
      }

/** \fn   INT32 del ( const bson::BSONObj &condition,
                  const bson::BSONObj &hint
                )
    \brief Delete the matching documents in current collection
    \param [in] condition The matching rule, delete all the documents if not provided
    \param [in] hint The hint, automatically match the optimal hint if not provided
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 del ( const bson::BSONObj &condition = _sdbStaticObject,
                  const bson::BSONObj &hint      = _sdbStaticObject
                )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->del ( condition, hint ) ;
      }

/* \fn INT32 query  ( _sdbCursor **cursor,
                     const bson::BSONObj &condition,
                     const bson::BSONObj &selected,
                     const bson::BSONObj &orderBy,
                     const bson::BSONObj &hint,
                     INT64 numToSkip,
                     INT64 numToReturn,
                     INT32 flag
                    )
    \brief Get the matching documents in current collection
    \param [in] condition The matching rule, return all the documents if not provided
    \param [in] selected The selective rule, return the whole document if not provided
    \param [in] orderBy The ordered rule, result set is unordered if not provided
    \param [in] hint The hint, automatically match the optimal hint if not provided
    \param [in] numToSkip Skip the first numToSkip documents, default is 0
    \param [in] numToReturn Only return numToReturn documents, default is -1 for returning all results
    \param [in] flag The query flag, default to be 0

        FLG_QUERY_FORCE_HINT(0x00000080)      : Force to use specified hint to query, if database have no index assigned by the hint, fail to query
        FLG_QUERY_PARALLED(0x00000100)        : Enable paralled sub query
        FLG_QUERY_WITH_RETURNDATA(0x00000200) : In general, query won't return data until cursor get from database,
                                                when add this flag, return data in query response, it will be more high-performance
        
    \param [out] cursor The cursor of current query
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 query  ( _sdbCursor **cursor,
                     const bson::BSONObj &condition = _sdbStaticObject,
                     const bson::BSONObj &selected  = _sdbStaticObject,
                     const bson::BSONObj &orderBy   = _sdbStaticObject,
                     const bson::BSONObj &hint      = _sdbStaticObject,
                     INT64 numToSkip          = 0,
                     INT64 numToReturn        = -1,
                     INT32 flag               = 0
                   )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->query ( cursor, condition, selected, orderBy,
                                     hint, numToSkip, numToReturn, flag ) ;
      }

/** \fn INT32 query  ( sdbCursor &cursor,
                     const bson::BSONObj &condition,
                     const bson::BSONObj &selected,
                     const bson::BSONObj &orderBy,
                     const bson::BSONObj &hint,
                     INT64 numToSkip,
                     INT64 numToReturn,
                     INT32 flag
                   )
    \brief Get the matching documents in current collection
    \param [in] condition The matching rule, return all the documents if not provided
    \param [in] selected The selective rule, return the whole document if not provided
    \param [in] orderBy The ordered rule, result set is unordered if not provided
    \param [in] hint The hint, automatically match the optimal hint if not provided
    \param [in] numToSkip Skip the first numToSkip documents, default is 0
    \param [in] numToReturn Only return numToReturn documents, default is -1 for returning all results
    \param [in] flag The query flag, defalt to be 0

        FLG_QUERY_FORCE_HINT(0x00000080)      : Force to use specified hint to query, if database have no index assigned by the hint, fail to query
        FLG_QUERY_PARALLED(0x00000100)        : Enable paralled sub query
        FLG_QUERY_WITH_RETURNDATA(0x00000200) : In general, query won't return data until cursor get from database,
                                                when add this flag, return data in query response, it will be more high-performance
        
    \param [out] cursor The cursor of current query
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 query  ( sdbCursor &cursor,
                     const bson::BSONObj &condition = _sdbStaticObject,
                     const bson::BSONObj &selected  = _sdbStaticObject,
                     const bson::BSONObj &orderBy   = _sdbStaticObject,
                     const bson::BSONObj &hint      = _sdbStaticObject,
                     INT64 numToSkip          = 0,
                     INT64 numToReturn        = -1,
                     INT32 flag               = 0
                   )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->query ( cursor, condition, selected, orderBy,
                                     hint, numToSkip, numToReturn, flag ) ;
      }

/* \fn INT32 rename ( const CHAR *pNewName )
    \brief Rename the specified collection
    \param [in] pNewName The new collection name
    \retval SDB_OK Operation Success
    \retval Others Operation Fail

      INT32 rename ( const CHAR *pNewName )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->rename ( pNewName ) ;
      }*/

/** \fn INT32 createIndex ( const bson::BSONObj &indexDef,
                          const CHAR *pName,
                          BOOLEAN isUnique
                        )
    \brief Create the index in current collection
    \param [in] indexDef The bson structure of index element, e.g. {name:1, age:-1}
    \param [in] pIndexName The index name
    \param [in] isUnique Whether the index elements are unique or not
    \param [in] isEnforced Whether the index is enforced unique
                           This element is meaningful when isUnique is set to true
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createIndex ( const bson::BSONObj &indexDef,
                          const CHAR *pName,
                          BOOLEAN isUnique,
                          BOOLEAN isEnforced
                        )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->createIndex ( indexDef, pName, isUnique,
                                           isEnforced ) ;
      }

/* \fn INT32 getIndexes ( _sdbCursor **cursor,
                         const CHAR *pName )
    \brief Get all of or one of the indexes in current collection
    \param [in] pName  The index name, returns all of the indexes if this parameter is null
    \param [out] cursor The cursor of all the result for current query
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getIndexes ( _sdbCursor **cursor,
                         const CHAR *pName )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->getIndexes ( cursor, pName ) ;
      }

/** \fn INT32 getIndexes ( sdbCursor &cursor,
                         const CHAR *pName )
    \brief Get all of or one of the indexes in current collection
    \param [in] pName  The index name, returns all of the indexes if this parameter is null
    \param [out] cursor The cursor of all the result for current query
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getIndexes ( sdbCursor &cursor,
                         const CHAR *pName )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->getIndexes ( cursor, pName ) ;
      }

/** \fn INT32 dropIndex ( const CHAR *pName )
    \brief Drop the index in current collection
    \param [in] pName The index name
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 dropIndex ( const CHAR *pName )
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->dropIndex ( pName ) ;
      }

/** \fn INT32 create ()
    \brief create the specified collection of current collection space
    \deprecated This function will be deprecated in SequoiaDB1.6, use sdbCollectionSpace::createCollection instead of it.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 create ()
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->create () ;
      }

/** \fn INT32 drop ()
    \brief Drop the specified collection of current collection space
    \deprecated This function will be deprecated in SequoiaDB1.6, use sdbCollectionSpace::dropCollection instead of it.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 drop ()
      {
         if ( !pCollection )
            return SDB_NOT_CONNECTED ;
         return pCollection->drop () ;
      }


/** \fn const CHAR *getCollectionName ()
    \brief Get the name of specified collection in current collection space
    \return The name of specified collection.
*/
      const CHAR *getCollectionName ()
      {
         if ( !pCollection )
            return NULL ;
         return pCollection->getCollectionName () ;
      }

/** \fn const CHAR *getCSName ()
    \brief Get the name of current collection space
    \return The name of current collection space.
*/
      const CHAR *getCSName ()
      {
         if ( !pCollection )
            return NULL ;
         return pCollection->getCSName () ;
      }

/** \fn const CHAR *getFullName ()
    \brief Get the full name of specified collection in current collection space
    \return The full name of specified collection.
*/
      const CHAR *getFullName ()
      {
         if ( !pCollection )
            return NULL ;
         return pCollection->getFullName () ;
      }

/* \fn INT32 aggregate ( _sdbCursor **cursor,
                         std::vector<bson::BSONObj> &obj
                       )
    \brief Execute aggregate operation in specified collection
    \param [in] obj The array of bson objects
    \param [out] cursor The cursor handle of result
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
 INT32 aggregate ( _sdbCursor **cursor,
                   std::vector<bson::BSONObj> &obj
                 )
{
   if ( !pCollection )
      return SDB_NOT_CONNECTED ;
   return pCollection->aggregate ( cursor, obj ) ;
}

/** \fn INT32 aggregate ( sdbCursor &cursor,
                          std::vector<bson::BSONObj> &obj
                        )
    \brief Execute aggregate operation in specified collection
    \param [in] obj The array of bson objects
    \param [out] cursor The cursor object of result
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
 INT32 aggregate ( sdbCursor &cursor,
                   std::vector<bson::BSONObj> &obj
                 )
{
   if ( !pCollection )
      return SDB_NOT_CONNECTED ;
   return pCollection->aggregate ( cursor, obj ) ;
}

/* \fn  INT32 getQueryMeta ( _sdbCursor **cursor,
                             const bson::BSONObj &condition,
                             const bson::BSONObj &selected,
                             const bson::BSONObj &orderBy,
                             INT64 numToSkip,
                             INT64 numToReturn ) ;
    \brief Get the index blocks' or data blocks' infomation for concurrent query
    \param [in] condition The matching rule, return all the documents if not provided
    \param [in] orderBy The ordered rule, result set is unordered if not provided
    \param [in] hint One of the indexs of current collection, using default index to query if not provided
                    eg:{"":"ageIndex"}
    \param [in] numToSkip Skip the first numToSkip documents, default is 0
    \param [in] numToReturn Only return numToReturn documents, default is -1 for returning all results
    \param [out] cursor The cursor of current query
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
   INT32 getQueryMeta ( _sdbCursor **cursor,
                             const bson::BSONObj &condition,
                             const bson::BSONObj &orderBy,
                             const bson::BSONObj &hint,
                             INT64 numToSkip,
                             INT64 numToReturn ) ;

/** \fn  INT32 getQueryMeta ( sdbCursor &cursor,
                         const bson::BSONObj &condition,
                         const bson::BSONObj &selected,
                         const bson::BSONObj &orderBy,
                         INT64 numToSkip,
                         INT64 numToReturn )
    \brief Get the index blocks' or data blocks' infomations for concurrent query
    \param [in] condition The matching rule, return the whole range of index blocks if not provided
                    eg:{"age":{"$gt":25},"age":{"$lt":75}}
    \param [in] orderBy The ordered rule, result set is unordered if not provided
    \param [in] hint One of the indexs in current collection, using default index to query if not provided
                    eg:{"":"ageIndex"}
    \param [in] numToSkip Skip the first numToSkip documents, default is 0
    \param [in] numToReturn Only return numToReturn documents, default is -1 for returning all results
    \param [out] cursor The result of query
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
    INT32 getQueryMeta ( sdbCursor &cursor,
                         const bson::BSONObj &condition,
                         const bson::BSONObj &orderBy,
                         const bson::BSONObj &hint,
                         INT64 numToSkip,
                         INT64 numToReturn )
    {
       if ( !pCollection )
          return SDB_NOT_CONNECTED ;
       return pCollection->getQueryMeta ( cursor, condition, orderBy,
                                     hint, numToSkip, numToReturn ) ;
    }

/** \fn INT32 attachCollection ( const CHAR *subClFullName,
                                      const bson::BSONObj &options)
    \brief Attach the specified collection.
    \param [in] subClFullName The name of the subcollection
    \param [in] options The low boudary and up boudary
                eg: {"LowBound":{a:1},"UpBound":{a:100}}
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
    INT32 attachCollection ( const CHAR *subClFullName,
                                      const bson::BSONObj &options)
    {
       if ( !pCollection )
          return SDB_NOT_CONNECTED ;
       return pCollection->attachCollection ( subClFullName, options ) ;
    }

/** \fn INT32 detachCollection ( const CHAR *subClFullName)
    \brief Dettach the specified collection.
    \param [in] subClFullName The name of the subcollection
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
    INT32 detachCollection ( const CHAR *subClFullName)
    {
       if ( !pCollection )
          return SDB_NOT_CONNECTED ;
       return pCollection->detachCollection ( subClFullName ) ;
    }

/** \fn INT32 explain ( sdbCursor &cursor,
                    const bson::BSONObj &condition,
                    const bson::BSONObj &select,
                    const bson::BSONObj &orderBy,
                    const bson::BSONObj &hint,
                    INT64 numToSkip,
                    INT64 numToReturn,
                    INT32 flag,
                    const bson::BSONObj &options )
    \brief Get access plan of query.
    \param [in] condition The matching rule, return all the documents if null
    \param [in] select The selective rule, return the whole document if null
    \param [in] orderBy The ordered rule, never sort if null
    \param [in] hint The hint, automatically match the optimal hint if null
    \param [in] numToSkip Skip the first numToSkip documents, never skip if this parameter is 0
    \param [in] numToReturn Only return numToReturn documents, return all if this parameter is -1
    \param [in] options the rules of explain, the options are as below:

        Run     : Whether execute query explain or not, true for excuting query explain then get
                  the data and time information; false for not excuting query explain but get the
                  query explain information only. e.g. {Run:true}
    \param [in] flags The flags of query
    \param [out] cursor The cursor of current query
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
    INT32 explain ( sdbCursor &cursor,
                    const bson::BSONObj &condition = _sdbStaticObject,
                    const bson::BSONObj &select    = _sdbStaticObject,
                    const bson::BSONObj &orderBy   = _sdbStaticObject,
                    const bson::BSONObj &hint      = _sdbStaticObject,
                    INT64 numToSkip                = 0,
                    INT64 numToReturn              = -1,
                    INT32 flag                     = 0,
                    const bson::BSONObj &options   = _sdbStaticObject )
    {
       if ( !pCollection )
         return SDB_NOT_CONNECTED ;
       return pCollection->explain( cursor, condition, select, orderBy, hint,
                                    numToSkip, numToReturn, flag, options ) ;
    }

/** \fn INT32 createLob( sdbLob &lob, const bson::OID *oid = NULL )
    \brief Create large object.
    \param [in] oid The id of the large object
    \param [out] lob The newly create large object
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \note When oid is offered, use it to create a lob for writing, otherwise, API will generate one. After finish writing the newly created lob, need to close it to release resource.
*/
    INT32 createLob( sdbLob &lob, const bson::OID *oid = NULL )
    {
       if ( !pCollection )
         return SDB_NOT_CONNECTED ;
       return pCollection->createLob( lob, oid ) ;
    }

/** \fn INT32 removeLob( const bson::OID &oid )
    \brief Remove large object.
    \param [in] oid The id of the large object
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
    INT32 removeLob( const bson::OID &oid )
    {
       if ( !pCollection )
         return SDB_NOT_CONNECTED ;
       return pCollection->removeLob( oid ) ;
    }

/** \fn INT32 openLob( sdbLob &lob, const bson::OID &oid )
    \brief Open an existing large object for reading.
    \param [in] oid The id of the large object
    \param [out] lob The large object to get
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \note Need to close lob to release resource, after opening a lob.
*/
    INT32 openLob( sdbLob &lob, const bson::OID &oid )
    {
       if ( !pCollection )
         return SDB_NOT_CONNECTED ;
       return pCollection->openLob( lob, oid ) ;
    }

/** \fn INT32 listLobs( sdbCursor &cursor )
    \brief List all the lobs' meta data in current collection.
    \param [out] cursor The curosr reference of the result
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
    INT32 listLobs( sdbCursor &cursor )
    {
       if ( !pCollection )
         return SDB_NOT_CONNECTED ;
       return pCollection->listLobs( cursor ) ;
    }

   } ;

/** \enum sdbNodeStatus
    \breif The status of the node.
*/
   enum sdbNodeStatus
   {
      SDB_NODE_ALL = 0,
      SDB_NODE_ACTIVE,
      SDB_NODE_INACTIVE,
      SDB_NODE_UNKNOWN
   } ;

/** \typedef enum sdbNodeStatus sdbNodeStatus
    \breif The status of the node.
*/
   typedef enum sdbNodeStatus sdbNodeStatus ;

   class DLLEXPORT _sdbNode
   {
   private :
      _sdbNode ( const _sdbNode& other ) ;
      _sdbNode& operator=( const _sdbNode& ) ;
   public :
      _sdbNode () {}
      virtual ~_sdbNode () {}
      virtual INT32 connect ( _sdb **dbConn ) = 0 ;
      virtual INT32 connect ( sdb &dbConn ) = 0 ;

      virtual sdbNodeStatus getStatus () = 0 ;

      virtual const CHAR *getHostName () = 0 ;

      virtual const CHAR *getServiceName () = 0 ;

      virtual const CHAR *getNodeName () = 0 ;

      virtual INT32 stop () = 0 ;

      virtual INT32 start () = 0 ;

/*      virtual INT32 modifyConfig ( std::map<std::string,std::string>
                                   &config ) = 0 ; */
   } ;

/** \class sdbNode
    \brief Database operation interfaces of node. This class takes the place of class "replicaNode".
    \note We use concept "node" instead of "replica node",
            and change the class name "ReplicaNode" to "Node".
            class "ReplicaNode" will be deprecated in version 2.x.
*/
   class DLLEXPORT sdbNode
   {
   private :
/** \fn sdbNode ( const sdbNode& other )
    \brief Copy Constructor
    \param[in] A const object reference  of class sdbNode.
*/
      sdbNode ( const sdbNode& other ) ;

/** \fn sdbNode& operator=( const sdbNode& )
    \brief Assignment constructor
    \param[in] A const reference  of class sdbNode.
    \retval A object const reference  of class sdbNode.
*/
      sdbNode& operator=( const sdbNode& ) ;
   public :
/** \var pNode
    \breif A pointer of virtual base class _sdbNode

    Class sdbNode is a shell for _sdbNode. We use pNode to
    call the methods in class _sdbNode.
*/
      _sdbNode *pNode ;

/** \fn sdbNode ()
    \brief Default constructor.
*/
      sdbNode ()
      {
         pNode = NULL ;
      }

/** \fn ~sdbNode ()
    \brief Destructor.
*/
      ~sdbNode ()
      {
         if ( pNode )
            delete pNode ;
      }
/* \fn connect ( _sdb **dbConn )
    \brief Connect to the current node.
    \param [out] dbConn The database obj of current connection
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 connect ( _sdb **dbConn )
      {
         if ( !pNode )
            return SDB_NOT_CONNECTED ;
         return pNode->connect ( dbConn ) ;
      }

/** \fn connect ( sdb &dbConn )
    \brief Connect to the current node.
    \param [out] dbConn The database obj of current connection
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 connect ( sdb &dbConn )
      {
         if ( !pNode )
            return SDB_NOT_CONNECTED ;
         return pNode->connect ( dbConn ) ;
      }

/** \fn sdbNodeStatus getStatus ()
    \brief Get status of the current node.
    \return  The status of current node.
*/
      sdbNodeStatus getStatus ()
      {
         if ( !pNode )
            return SDB_NODE_UNKNOWN ;
         return pNode->getStatus () ;
      }

/** \fn const CHAR *getHostName ()
    \brief Get host name of the current node.
    \return The host name.
*/
      const CHAR *getHostName ()
      {
         if ( !pNode )
            return NULL ;
         return pNode->getHostName () ;
      }

/** \fn CHAR *getServiceName ()
    \brief Get service name of the current node.
    \return The service name.
*/
      const CHAR *getServiceName ()
      {
         if ( !pNode )
            return NULL ;
         return pNode->getServiceName () ;
      }

/** \fn const CHAR *getNodeName ()
    \brief Get node name of the current node.
    \return The node name.
*/
      const CHAR *getNodeName ()
      {
         if ( !pNode )
            return NULL ;
         return pNode->getNodeName () ;
      }

/** \fn INT32  stop ()
    \brief Stop the node.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32  stop ()
      {
         if ( !pNode )
            return SDB_NOT_CONNECTED ;
         return pNode->stop () ;
      }

/** \fn INT32 start ()
    \brief Start the node.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 start ()
      {
         if ( !pNode )
            return SDB_NOT_CONNECTED ;
         return pNode->start () ;
      }
/*      INT32 modifyConfig ( std::map<std::string,std::string> &config )
      {
         if ( !pNode )
            return NULL ;
         return pNode->modifyConfig ( config ) ;
      }*/
   } ;

   class DLLEXPORT _sdbReplicaGroup
   {
   private :
      _sdbReplicaGroup ( const _sdbReplicaGroup& other ) ;
      _sdbReplicaGroup& operator=( const _sdbReplicaGroup& ) ;
   public :
      _sdbReplicaGroup () {}
      virtual ~_sdbReplicaGroup () {}
      virtual INT32 getNodeNum ( sdbNodeStatus status, INT32 *num ) = 0 ;

      virtual INT32 getDetail ( bson::BSONObj &result ) = 0 ;

      virtual INT32 getMaster ( _sdbNode **node ) = 0 ;
      virtual INT32 getMaster ( sdbNode &node ) = 0 ;

      virtual INT32 getSlave ( _sdbNode **node ) = 0 ;
      virtual INT32 getSlave ( sdbNode &node ) = 0 ;

      virtual INT32 getNode ( const CHAR *pNodeName,
                              _sdbNode **node ) = 0 ;
      virtual INT32 getNode ( const CHAR *pNodeName,
                              sdbNode &node ) = 0 ;

      virtual INT32 getNode ( const CHAR *pHostName,
                              const CHAR *pServiceName,
                              _sdbNode **node ) = 0 ;
      virtual INT32 getNode ( const CHAR *pHostName,
                              const CHAR *pServiceName,
                              sdbNode &node ) = 0 ;

      virtual INT32 createNode ( const CHAR *pHostName,
                                 const CHAR *pServiceName,
                                 const CHAR *pDatabasePath,
                                 std::map<std::string,std::string> &config )= 0;
      virtual INT32 removeNode ( const CHAR *pHostName,
                                 const CHAR *pServiceName,
                                 const bson::BSONObj &configure = _sdbStaticObject ) = 0 ;
      virtual INT32 stop () = 0 ;

      virtual INT32 start () = 0 ;

      virtual const CHAR *getName () = 0 ;

      virtual BOOLEAN isCatalog () = 0 ;
   } ;

/** \class sdbReplicaGroup
    \brief Database operation interfaces of replica group.
*/
   class DLLEXPORT sdbReplicaGroup
   {
   private :
      sdbReplicaGroup ( const sdbReplicaGroup& other ) ;
      sdbReplicaGroup& operator=( const sdbReplicaGroup& ) ;
   public :
/** \var pReplicaGroup
    \breif A pointer of virtual base class _sdbReplicaGroup

     Class sdbReplicaGroup is a shell for _sdbReplicaGroup. We use pCursor to
     call the methods in class _sdbReplicaGroup.
*/
      _sdbReplicaGroup *pReplicaGroup ;

/** \fn sdbReplicaGroup ()
    \brief Default constructor
*/
      sdbReplicaGroup ()
      {
         pReplicaGroup = NULL ;
      }

/** \fn ~sdbReplicaGroup ()
    \brief Destructor
*/
      ~sdbReplicaGroup ()
      {
         if ( pReplicaGroup )
            delete pReplicaGroup ;
      }

/** \fn INT32 getNodeNum ( sdbNodeStatus status, INT32 *num )
    \brief Get the count of node with given status in current replica group.
    \param [in] status The specified status as below

        SDB_NODE_ALL
        SDB_NODE_ACTIVE
        SDB_NODE_INACTIVE
        SDB_NODE_UNKNOWN
    \param [out] num The count of node.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getNodeNum ( sdbNodeStatus status, INT32 *num )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getNodeNum ( status, num ) ;
      }

/** \fn INT32 getDetail ( bson::BSONObj &result )
    \brief Get the detail of the replica group.
    \param [out] result Return the all the info of current replica group.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getDetail ( bson::BSONObj &result )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getDetail ( result ) ;
      }

/* \fn INT32 getMaster ( _sdbNode **node )
    \brief Get the master node of the current replica group.
    \param [out] node The master node.If not exit,return null.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getMaster ( _sdbNode **node )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getMaster ( node ) ;
      }

/** \fn INT32 getMaster ( sdbNode &node )
    \brief Get the master node of the current replica group.
    \param [out] node The master node.If not exit,return null.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getMaster ( sdbNode &node )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getMaster ( node ) ;
      }

/* \fn INT32 getSlave ( _sdbNode **node )
    \brief Get one of slave node of the current replica group,
           if no slave exists then get master
    \param [out] node The slave node
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getSlave ( _sdbNode **node )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getSlave ( node ) ;
      }

/** \fn  INT32 getSlave ( sdbNode &node )
    \brief Get one of slave node of the current replica group,
           if no slave exists then get master
    \param [out] node The slave node
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getSlave ( sdbNode &node )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getSlave ( node ) ;
      }

/* \fn INT32 getNode ( const CHAR *pNodeName,
                      _sdbNode **node )
    \brief Get specified node from current replica group.
    \param [in] pHostName The host name of the node
    \param [out] node  The specified node
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getNode ( const CHAR *pNodeName,
                      _sdbNode **node )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getNode ( pNodeName, node ) ;
      }

/** \fn INT32 getNode ( const CHAR *pNodeName,
                      sdbNode &node )
    \brief Get specified node from current replica group.
    \param [in] pHostName The host name of the node.
    \param [out] node  The specified node.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getNode ( const CHAR *pNodeName,
                      sdbNode &node )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getNode ( pNodeName, node ) ;
      }

/* \fn INT32 getNode ( const CHAR *pHostName,
                      const CHAR *pServiceName,
                      _sdbNode **node )
    \brief Get specified node from current replica group.
    \param [in] pHostName The host name of the node.
    \param [in] pServiceName The service name of the node.
    \param [out] node The specified node.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getNode ( const CHAR *pHostName,
                      const CHAR *pServiceName,
                      _sdbNode **node )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getNode ( pHostName, pServiceName, node ) ;
      }

/** \fn INT32 getNode ( const CHAR *pHostName,
                      const CHAR *pServiceName,
                      sdbNode &node )
    \brief Get specified node from current replica group.
    \param [in] pHostName The host name of the node.
    \param [in] pServiceName The service name of the node.
    \param [out] node The specified node.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getNode ( const CHAR *pHostName,
                      const CHAR *pServiceName,
                      sdbNode &node )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->getNode ( pHostName, pServiceName, node ) ;
      }

/** \fn INT32 createNode ( const CHAR *pHostName,
                         const CHAR *pServiceName,
                         const CHAR *pDatabasePath,
                         std::map<std::string,std::string> &config )
    \brief Create node in a given replica group.
    \param [in] pHostName The hostname for the node
    \param [in] pServiceName The servicename for the node
    \param [in] pDatabasePath The database path for the node
    \param [in] configure The configurations for the node
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createNode ( const CHAR *pHostName,
                         const CHAR *pServiceName,
                         const CHAR *pDatabasePath,
                         std::map<std::string,std::string> &config )
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->createNode ( pHostName, pServiceName,
                                            pDatabasePath, config ) ;
      }
/** \fn INT32 removeNode ( const CHAR *pHostName,
                                        const CHAR *pServiceName,
                                        const BSONObj &configure = _sdbStaticObject  )
    \brief remove node in a given replica group.
    \param [in] pHostName The hostname for the node
    \param [in] pServiceName The servicename for the node
    \param [in] configure The configurations for the node
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 removeNode ( const CHAR *pHostName,
                                          const CHAR *pServiceName,
                                          const bson::BSONObj &configure = _sdbStaticObject )
         {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->removeNode ( pHostName, pServiceName,
                                           configure ) ;
   }
/** \fn INT32 stop ()
    \brief Stop current replica group.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 stop ()
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->stop () ;
      }

/** \fn INT32 INT32 start ()
    \brief Start up current replica group.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 start ()
      {
         if ( !pReplicaGroup )
            return SDB_NOT_CONNECTED ;
         return pReplicaGroup->start () ;
      }

/** \fn const CHAR *getName () ;
    \brief Get the name of current replica group.
    \retval The name of current replica group or null if fail
*/
      const CHAR *getName ()
      {
         if ( !pReplicaGroup )
            return NULL ;
         return pReplicaGroup->getName() ;
      }

/** \fn BOOLEAN isCatalog ()
    \brief Test whether current replica group is catalog replica group.
    \retval TRUE The replica group is catalog
    \retval FALSE The replica group is not catalog
*/
      BOOLEAN isCatalog ()
      {
         if ( !pReplicaGroup )
            return FALSE ;
         return pReplicaGroup->isCatalog() ;
      }
   } ;

   class DLLEXPORT _sdbCollectionSpace
   {
   private :
      _sdbCollectionSpace ( const _sdbCollectionSpace& other ) ;
      _sdbCollectionSpace& operator=( const _sdbCollectionSpace& ) ;
   public :
      _sdbCollectionSpace () {}
      virtual ~_sdbCollectionSpace () {}
      virtual INT32 getCollection ( const CHAR *pCollectionName,
                                    _sdbCollection **collection ) = 0 ;

      virtual INT32 getCollection ( const CHAR *pCollectionName,
                                    sdbCollection &collection ) = 0 ;

      virtual INT32 createCollection ( const CHAR *pCollection,
                                       const bson::BSONObj &options,
                                       _sdbCollection **collection ) = 0 ;

      virtual INT32 createCollection ( const CHAR *pCollection,
                                       const bson::BSONObj &options,
                                       sdbCollection &collection ) = 0 ;

      virtual INT32 createCollection ( const CHAR *pCollection,
                                       _sdbCollection **collection ) = 0 ;

      virtual INT32 createCollection ( const CHAR *pCollection,
                                       sdbCollection &collection ) = 0 ;

      virtual INT32 dropCollection ( const CHAR *pCollection ) = 0 ;

      virtual INT32 create () = 0 ;
      virtual INT32 drop () = 0 ;

      virtual const CHAR *getCSName () = 0 ;

   } ;
/** \class sdbCollectionSpace
    \brief Database operation interfaces of collection space
*/
   class DLLEXPORT sdbCollectionSpace
   {
   private :
/** \fn sdbCollectionSpace ( const sdbCollectionSpace& other )
    \brief Copy constructor.
    \param[in] A const object reference of class sdbCollectionSpace .
*/
      sdbCollectionSpace ( const sdbCollectionSpace& other ) ;

/** \fn sdbCollectionSpace& operator=( const sdbCollectionSpace& )
    \brief Assignment constructor.
    \param[in] A const object reference of class sdb.
    \retval A const object reference  of class sdb.
*/
      sdbCollectionSpace& operator=( const sdbCollectionSpace& ) ;
   public :
/** \var pCollectionSpace
    \breif A pointer of virtual base class _sdbCollectionSpace

     Class sdbCollectionSpace is a shell for _sdbCollectionSpace. We use
     pCollectionSpace to call the methods in class _sdbCollectionSpace.
*/
      _sdbCollectionSpace *pCollectionSpace ;

/** \fn sdbCollectionSpace ()
    \brief Default constructor.
*/
      sdbCollectionSpace ()
      {
         pCollectionSpace = NULL ;
      }

/** \fn ~sdbCollectionSpace ()
    \brief Destructor.
*/
      ~sdbCollectionSpace ()
      {
         if ( pCollectionSpace )
            delete pCollectionSpace ;
      }
/* \fn INT32 getCollection ( const CHAR *pCollectionName,
                            _sdbCollection **collection )
    \brief Get the named collection.
    \param [in] pCollectionName The full name of the collection.
    \param [out] collection The return collection handle.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getCollection ( const CHAR *pCollectionName,
                            _sdbCollection **collection )
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->getCollection ( pCollectionName,
                                                  collection ) ;
      }

/** \fn INT32 getCollection ( const CHAR *pCollectionName,
                            sdbCollection &collection )
    \brief Get the named collection.
    \param [in] pCollectionName The full name of the collection.
    \param [out] collection The return collection object.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getCollection ( const CHAR *pCollectionName,
                            sdbCollection &collection )
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->getCollection ( pCollectionName,
                                                  collection ) ;
      }

/* \fn INT32 createCollection ( const CHAR *pCollection,
 *                             const bson::BSONObj &options,
                               _sdbCollection **collection )
    \brief Create the specified collection in current collection space with options
    \param [in] pCollection The collection name
    \param [in] options The options for creating collection,
                including "ShardingKey", "ReplSize", "IsMainCL" and "Compressed" informations,
                no options, if null
    \param [out] collection The return collection handle.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createCollection ( const CHAR *pCollection,
                               const bson::BSONObj &options,
                               _sdbCollection **collection )
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->createCollection ( pCollection,
                                                     options,
                                                     collection ) ;
      }

/** \fn INT32 createCollection ( const CHAR *pCollection,
 *                             const bson::BSONObj &options,
                               sdbCollection &collection )
    \brief Create the specified collection in current collection space with options
    \param [in] pCollection The collection name
    \param [in] options The options for creating collection,
                including "ShardingKey", "ReplSize", "IsMainCL" and "Compressed" informations,
                no options, if null
    \param [out] collection The return collection object .
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createCollection ( const CHAR *pCollection,
                               const bson::BSONObj &options,
                               sdbCollection &collection )
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->createCollection ( pCollection,
                                                     options,
                                                     collection ) ;
      }

/* \fn INT32 createCollection ( const CHAR *pCollection,
                               _sdbCollection **collection )
    \brief Create the specified collection in current collection space without
           sharding key and default ReplSize
    \param [in] pCollection The collection name
    \param [out] collection The return collection handle.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createCollection ( const CHAR *pCollection,
                               _sdbCollection **collection )
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->createCollection ( pCollection,
                                                     collection ) ;
      }

/** \fn INT32 createCollection ( const CHAR *pCollection,
                               sdbCollection &collection )
    \brief Create the specified collection in current collection space without
           sharding key and default ReplSize.
    \param [in] pCollection The collection name.
    \param [out] collection The return collection object.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createCollection ( const CHAR *pCollection,
                               sdbCollection &collection )
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->createCollection ( pCollection,
                                                     collection ) ;
      }

/** \fn INT32 dropCollection ( const CHAR *pCollection )
    \brief Drop the specified collection in current collection space.
    \param [in] pCollection  The collection name.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 dropCollection ( const CHAR *pCollection )
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->dropCollection ( pCollection ) ;
      }

/** \fn INT32 create ()
    \brief Create a new collection space.
    \deprecated This function will be deprecated in SequoiaDB2.x, use sdb::createCollectionSpace instead of it.
    \retval SDB_OK Operation Success.
    \retval Others Operation Fail
*/
      INT32 create ()
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->create () ;
      }

/** \fn INT32 drop ()
    \brief Drop current collection space.
    \deprecated This function will be deprecated in SequoiaDB2.x, use sdb::dropCollectionSpace instead of it.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 drop ()
      {
         if ( !pCollectionSpace )
            return SDB_NOT_CONNECTED ;
         return pCollectionSpace->drop () ;
      }


/** \fn const CHAR *getCSName ()
    \brief Get the current collection space name.
    \return The name of current collection space.
*/
      const CHAR *getCSName ()
      {
         if ( !pCollectionSpace )
            return NULL ;
         return pCollectionSpace->getCSName () ;
      }
   } ;

   class DLLEXPORT _sdbDomain
   {
   private :
      _sdbDomain ( const _sdbDomain& other ) ; // non construction-copyable
      _sdbDomain& operator= ( const _sdbDomain& ) ; // non copyable
   public :
      _sdbDomain () {}
      virtual ~_sdbDomain () {}

      virtual const CHAR* getName () = 0 ;

      virtual INT32 alterDomain ( const bson::BSONObj &options = _sdbStaticObject ) = 0 ;

      virtual INT32 listCollectionSpacesInDomain ( _sdbCursor **cursor ) = 0 ;

      virtual INT32 listCollectionSpacesInDomain ( sdbCursor &cursor ) = 0 ;

      virtual INT32 listCollectionsInDomain ( _sdbCursor **cursor ) = 0 ;

      virtual INT32 listCollectionsInDomain ( sdbCursor &cursor ) = 0 ;

   } ;

   /** \class  sdbDomain
       \brief Database operation interfaces of domain.
   */
   class DLLEXPORT sdbDomain
   {
   private :
      sdbDomain ( const sdbDomain& ) ; // non construction-copyable
      sdbDomain& operator= ( const sdbDomain& ) ; // non copyable
   public :

/** \var pDomain
    \breif A pointer of virtual base class _sdbDomain

     Class sdbDomain is a shell for _sdbDomain. We use pDomain to
     call the methods in class _sdbDomain.
*/
      _sdbDomain *pDomain ;

/** \fn sdbCollectionSpace ()
    \brief Default constructor.
*/
      sdbDomain() { pDomain = NULL ; }

/** \fn ~sdbCollectionSpace ()
    \brief Destructor.
*/
      ~sdbDomain()
      {
         if ( pDomain )
            delete pDomain ;
      }

/** \fn const CHAR *getName () ;
    \brief Get the name of current domain.
    \retval The name of current domain or null if fail
*/
      const CHAR *getName ()
      {
         if ( !pDomain )
            return NULL ;
         return pDomain->getName() ;
      }

/** \fn INT32 alterDomain( const bson::BSONObj &options ) ;
    \brief Alter the current domain.
    \param [in] options The options user wants to alter

        Groups:    The list of replica groups' names which the domain is going to contain.
                   eg: { "Groups": [ "group1", "group2", "group3" ] }, it means that domain
                   changes to contain "group1" "group2" or "group3".
                   We can add or remove groups in current domain. However, if a group has data
                   in it, remove it out of domain will be failing.
        AutoSplit: Alter current domain to have the ability of automatically split or not. 
                   If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
                   the data of this collection will be split(hash split) into all the groups in this domain automatically.
                   However, it won't automatically split data into those groups which were add into this domain later.
                   eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 alterDomain ( const bson::BSONObj &options )
      {
         if ( !pDomain )
            return SDB_SYS ;
         return pDomain->alterDomain ( options ) ;
      }

/** \fn INT32 listCollectionSpacesInDomain ( sdbCursor &cursor ) ;
    \brief List all the collection spaces in current domain.
    \param [in] cHandle The domain handle
    \param [out] cursor The sdbCursor object of result
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listCollectionSpacesInDomain ( sdbCursor &cursor )
      {
         if ( !pDomain )
            return SDB_SYS ;
         return pDomain->listCollectionSpacesInDomain ( cursor ) ;
      }

/** \fn INT32 listCollectionsInDomain ( sdbCursor &cursor ) ;
    \brief List all the collections in current domain.
    \param [in] cHandle The domain handle
    \param [out] cursor The sdbCursor object of result
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listCollectionsInDomain ( sdbCursor &cursor )
      {
         if ( !pDomain )
            return SDB_SYS ;
         return pDomain->listCollectionsInDomain ( cursor ) ;
      }

   };

   class DLLEXPORT _sdbLob
   {
   private :
      _sdbLob ( const _sdbLob& other ) ; // non construction-copyable
      _sdbLob& operator= ( const _sdbLob& ) ; // non copyable
    
   public :
      _sdbLob () {}

      virtual ~_sdbLob () {}

      virtual INT32 close () = 0 ;

      virtual INT32 read ( UINT32 len, CHAR *buf, UINT32 *read ) = 0 ;

      virtual INT32 write ( const CHAR *buf, UINT32 len ) = 0 ;

      virtual INT32 seek ( SINT64 size, SDB_LOB_SEEK whence ) = 0 ;

      virtual INT32 isClosed( BOOLEAN &flag ) = 0 ;

      virtual INT32 getOid( bson::OID &oid ) = 0 ;

      virtual INT32 getSize( SINT64 *size ) = 0 ;
         
      virtual INT32 getCreateTime ( UINT64 *millis ) = 0 ;

      virtual BOOLEAN isClosed() = 0 ;

      virtual bson::OID getOid() = 0 ;

      virtual SINT64 getSize() = 0 ;
         
      virtual UINT64 getCreateTime () = 0 ;

   } ;

   /** \class  sdbLob
       \brief Database operation interfaces of large object.
   */
   class DLLEXPORT sdbLob
   {
   private :
      sdbLob ( const sdbLob& ) ; // non construction-copyable
      sdbLob& operator= ( const sdbLob& ) ; // non copyable

   public :

/** \var pLob
    \breif A pointer of virtual base class _sdbLob

    Class sdbLob is a shell for _sdbLob. We use pLob to
    call the methods in class _sdbLob.
*/
      _sdbLob *pLob ;
/** \fn sdbLob()
    \brief Default constructor.
*/
      sdbLob() { pLob = NULL ; }

/** \fn ~sdb()
    \brief Destructor.
*/
      ~sdbLob()
      {
         if ( pLob )
            delete pLob ;
      }

/** \fn INT32 close ()
    \brief Close lob.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 close ()
      {
         if ( !pLob )
            return SDB_OK ;
         return pLob->close() ;
      }

/** \fn INT32 read ( UINT32 len, CHAR *buf, UINT32 *read )
    \brief Read lob.
    \param [in] len The length want to read
    \param [out] buf Put the data into buf
    \param [out] read The length of read
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 read ( UINT32 len, CHAR *buf, UINT32 *read )
      {
         if ( !pLob )
            return SDB_SYS ;
         return pLob->read( len, buf, read ) ;
      }

/** \fn INT32 write ( const CHAR *buf, UINT32 len )
    \brief Write lob.
    \param [in] buf The buf of write
    \param [in] len The length of write
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 write ( const CHAR *buf, UINT32 len )
      {
         if ( !pLob )
            return SDB_SYS ;
         return pLob->write( buf, len ) ;
      }

/** \fn INT32 seek ( SINT64 size, SDB_LOB_SEEK whence )
    \brief Seek the place to read.
    \param [in] size The size of seek
    \param [in] whence The whence of seek
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 seek ( SINT64 size, SDB_LOB_SEEK whence )
      {
         if ( !pLob )
            return SDB_SYS ;
         return pLob->seek( size, whence ) ;
      }

/** \fn INT32 isClosed( BOOLEAN &flag )
    \brief Test whether lob has been closed or not.
    \param [out] flag TRUE for lob has been closed, FALSE for not.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \deprecated Deprecated in version 2.x. Use "BOOLEAN isClosed()" instead
*/
    INT32 isClosed( BOOLEAN &flag )
    {
       if ( !pLob )
         return SDB_SYS ;
       return pLob->isClosed ( flag ) ;
    }

/** \fn BOOLEAN isClosed()
    \brief Test whether lob has been closed or not.
    \retval TRUE for lob has been closed, FALSE for not.
*/
    BOOLEAN isClosed()
    {
       if ( !pLob )
         return TRUE ;
       return pLob->isClosed () ;
    }

/** \fn INT32 getOid ( bson::OID &oid )
    \brief Get the lob's oid.
    \param [out] oid The oid of the lob
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \deprecated Deprecated in version 2.x. Use "bson::OID getOid ()" instead
*/
      INT32 getOid ( bson::OID &oid )
      {
         if ( !pLob )
            return SDB_SYS ;
         return pLob->getOid( oid ) ;
      }

/** \fn bson::OID getOid ()
    \brief Get the lob's oid.
    \retval The oid of the lob or a empty Oid bson::OID() when the lob is not be opened or has been closed
*/
      bson::OID getOid ()
      {
         if ( !pLob )
            return bson::OID();
         return pLob->getOid() ;
      }

/** \fn INT32 getSize ( SINT64 *size )
    \brief Get the lob's size.
    \param [out] size The size of lob
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \deprecated Deprecated in version 2.x. Use "SINT64 getSize ()" instead
*/
      INT32 getSize ( SINT64 *size )
      {
         if ( !pLob )
            return SDB_SYS ;
         return pLob->getSize( size ) ;
      }

/** \fn SINT64 getSize ()
    \brief Get the lob's size.
    \reval The size of lob, or -1 when the lob is not be opened or has been closed
*/
      SINT64 getSize ()
      {
         if ( !pLob )
            return -1 ;
         return pLob->getSize();
      }

/** \fn INT32 getCreateTime ( UINT64 *millis )
    \brief Get lob's create time.
    \param [out] millis The create time in milliseconds of lob
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \deprecated Deprecated in version 2.x. Use "UINT64 getCreateTime ()" instead
*/
      INT32 getCreateTime ( UINT64 *millis )
      {
         if ( !pLob )
            return SDB_SYS ;
         return pLob->getCreateTime( millis ) ;
      }

/** \fn UINT64 getCreateTime ()
    \brief Get lob's create time.
    \retval The create time in milliseconds of lob or -1 when the lob does not be opened or has been closed
*/
      UINT64 getCreateTime ()
      {
         if ( !pLob )
            return -1 ;
         return pLob->getCreateTime() ;
      }
      
   } ;

   class DLLEXPORT _sdb
   {
   private :
      _sdb ( const _sdb& other ) ; // non construction-copyable
      _sdb& operator=( const _sdb& ) ; // non copyable
   public :
      _sdb () {}
      virtual ~_sdb () {}
      virtual INT32 connect ( const CHAR *pHostName,
                              UINT16 port
                            ) = 0 ;
      virtual INT32 connect ( const CHAR *pHostName,
                              UINT16 port,
                              const CHAR *pUsrName,
                              const CHAR *pPasswd ) = 0 ;
      virtual INT32 connect ( const CHAR *pHostName,
                              const CHAR *pServiceName ) = 0 ;
      virtual INT32 connect ( const CHAR *pHostName,
                              const CHAR *pServiceName,
                              const CHAR *pUsrName,
                              const CHAR *pPasswd ) = 0 ;
      virtual INT32 connect ( const CHAR **pConnAddrs,
                              INT32 arrSize,
                              const CHAR *pUsrName,
                              const CHAR *pPasswd ) = 0 ;

      virtual void disconnect () = 0 ;

      virtual INT32 createUsr( const CHAR *pUsrName,
                               const CHAR *pPasswd ) = 0 ;

      virtual INT32 removeUsr( const CHAR *pUsrName,
                               const CHAR *pPasswd ) = 0 ;

      virtual INT32 getSnapshot ( _sdbCursor **cursor,
                                  INT32 snapType,
                                  const bson::BSONObj &condition = _sdbStaticObject,
                                  const bson::BSONObj &selector  = _sdbStaticObject,
                                  const bson::BSONObj &orderBy   = _sdbStaticObject
                                ) = 0 ;

      virtual INT32 getSnapshot ( sdbCursor &cursor,
                                  INT32 snapType,
                                  const bson::BSONObj &condition = _sdbStaticObject,
                                  const bson::BSONObj &selector  = _sdbStaticObject,
                                  const bson::BSONObj &orderBy   = _sdbStaticObject
                                ) = 0 ;

      virtual INT32 resetSnapshot ( const bson::BSONObj &condition = _sdbStaticObject ) = 0 ;

      virtual INT32 getList ( _sdbCursor **cursor,
                              INT32 listType,
                              const bson::BSONObj &condition = _sdbStaticObject,
                              const bson::BSONObj &selector  = _sdbStaticObject,
                              const bson::BSONObj &orderBy   = _sdbStaticObject
                            ) = 0 ;
      virtual INT32 getList ( sdbCursor &cursor,
                              INT32 listType,
                              const bson::BSONObj &condition = _sdbStaticObject,
                              const bson::BSONObj &selector  = _sdbStaticObject,
                              const bson::BSONObj &orderBy   = _sdbStaticObject
                            ) = 0 ;

      virtual INT32 getCollection ( const CHAR *pCollectionFullName,
                                    _sdbCollection **collection
                                  ) = 0 ;

      virtual INT32 getCollection ( const CHAR *pCollectionFullName,
                                    sdbCollection &collection
                                  ) = 0 ;

      virtual INT32 getCollectionSpace ( const CHAR *pCollectionSpaceName,
                                         _sdbCollectionSpace **cs
                                       ) = 0 ;

      virtual INT32 getCollectionSpace ( const CHAR *pCollectionSpaceName,
                                         sdbCollectionSpace &cs
                                       ) = 0 ;

      virtual INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                            INT32 iPageSize,
                                            _sdbCollectionSpace **cs
                                          ) = 0 ;

      virtual INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                            INT32 iPageSize,
                                            sdbCollectionSpace &cs
                                          ) = 0 ;

      virtual INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                            const bson::BSONObj &options,
                                            _sdbCollectionSpace **cs
                                          ) = 0 ;

      virtual INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                            const bson::BSONObj &options,
                                            sdbCollectionSpace &cs
                                          ) = 0 ;

      virtual INT32 dropCollectionSpace ( const CHAR *pCollectionSpaceName )
            = 0 ;

      virtual INT32 listCollectionSpaces ( _sdbCursor **result ) = 0 ;

      virtual INT32 listCollectionSpaces ( sdbCursor &result ) = 0 ;

      virtual INT32 listCollections ( _sdbCursor **result ) = 0 ;

      virtual INT32 listCollections ( sdbCursor &result ) = 0 ;

      virtual INT32 listReplicaGroups ( _sdbCursor **result ) = 0 ;

      virtual INT32 listReplicaGroups ( sdbCursor &result ) = 0 ;

      virtual INT32 getReplicaGroup ( const CHAR *pName,
                               _sdbReplicaGroup **result ) = 0 ;

      virtual INT32 getReplicaGroup ( const CHAR *pName,
                               sdbReplicaGroup &result ) = 0 ;

      virtual INT32 getReplicaGroup ( INT32 id,
                                _sdbReplicaGroup **result ) = 0 ;

      virtual INT32 getReplicaGroup ( INT32 id,
                               sdbReplicaGroup &result ) = 0 ;

      virtual INT32 createReplicaGroup ( const CHAR *pName,
                                  _sdbReplicaGroup **replicaGroup ) = 0 ;

      virtual INT32 createReplicaGroup ( const CHAR *pName,
                                  sdbReplicaGroup &replicaGroup ) = 0 ;

      virtual INT32 removeReplicaGroup ( const CHAR *pName ) = 0 ;

      virtual INT32 createReplicaCataGroup (  const CHAR *pHostName,
                                       const CHAR *pServiceName,
                                       const CHAR *pDatabasePath,
                                       const bson::BSONObj &configure ) =0 ;

      virtual INT32 activateReplicaGroup ( const CHAR *pName,
                                    _sdbReplicaGroup **replicaGroup ) = 0 ;
      virtual INT32 activateReplicaGroup ( const CHAR *pName,
                                    sdbReplicaGroup &replicaGroup ) = 0 ;

      virtual INT32 execUpdate( const CHAR *sql ) = 0 ;

      virtual INT32 exec( const CHAR *sql,
                          _sdbCursor **result ) = 0 ;

      virtual INT32 exec( const CHAR *sql,
                          sdbCursor &result ) = 0 ;

      virtual INT32 transactionBegin() = 0 ;

      virtual INT32 transactionCommit() = 0 ;

      virtual INT32 transactionRollback() = 0 ;

      virtual INT32 flushConfigure( const bson::BSONObj &options ) = 0 ;
      virtual INT32 crtJSProcedure ( const CHAR *code ) = 0 ;
      virtual INT32 rmProcedure( const CHAR *spName ) = 0 ;
      virtual INT32 listProcedures( _sdbCursor **cursor, const bson::BSONObj &condition ) = 0 ;
      virtual INT32 listProcedures( sdbCursor &cursor, const bson::BSONObj &condition ) = 0 ;
      virtual INT32 evalJS( _sdbCursor **cursor,
                             const CHAR *code,
                             SDB_SPD_RES_TYPE *type,
                             const bson::BSONObj &errmsg ) = 0 ;
      virtual INT32 evalJS( sdbCursor &cursor,
                             const CHAR *code,
                             SDB_SPD_RES_TYPE *type,
                             const bson::BSONObj &errmsg ) = 0 ;

      virtual INT32 backupOffline ( const bson::BSONObj &options) = 0 ;
      virtual INT32 listBackup ( _sdbCursor **cursor,
                              const bson::BSONObj &options,
                              const bson::BSONObj &condition = _sdbStaticObject,
                              const bson::BSONObj &selector = _sdbStaticObject,
                              const bson::BSONObj &orderBy = _sdbStaticObject) = 0 ;
      virtual INT32 listBackup ( sdbCursor &cursor,
                              const bson::BSONObj &options,
                              const bson::BSONObj &condition = _sdbStaticObject,
                              const bson::BSONObj &selector = _sdbStaticObject,
                              const bson::BSONObj &orderBy = _sdbStaticObject)  = 0 ;
      virtual INT32 removeBackup ( const bson::BSONObj &options ) = 0 ;

      virtual INT32 listTasks ( _sdbCursor **cursor,
                        const bson::BSONObj &condition = _sdbStaticObject,
                        const bson::BSONObj &selector = _sdbStaticObject,
                        const bson::BSONObj &orderBy = _sdbStaticObject,
                        const bson::BSONObj &hint = _sdbStaticObject) = 0 ;


      virtual INT32 listTasks ( sdbCursor &cursor,
                        const bson::BSONObj &condition = _sdbStaticObject,
                        const bson::BSONObj &selector = _sdbStaticObject,
                        const bson::BSONObj &orderBy = _sdbStaticObject,
                        const bson::BSONObj &hint = _sdbStaticObject) = 0 ;

      virtual INT32 waitTasks ( const SINT64 *taskIDs,
                        SINT32 num ) = 0 ;

      virtual INT32 cancelTask ( SINT64 taskID,
                        BOOLEAN isAsync ) = 0 ;
      virtual INT32 setSessionAttr ( const bson::BSONObj &options =
                                     _sdbStaticObject) = 0 ;
      virtual INT32 closeAllCursors () = 0 ;

      virtual INT32 isValid( BOOLEAN *result ) = 0 ;
      virtual BOOLEAN isValid() = 0 ;
      
      virtual INT32 createDomain ( const CHAR *pDomainName,
                                   const bson::BSONObj &options,
                                   _sdbDomain **domain ) = 0 ;

      virtual INT32 createDomain ( const CHAR *pDomainName,
                                   const bson::BSONObj &options,
                                   sdbDomain &domain ) = 0 ;

      virtual INT32 dropDomain ( const CHAR *pDomainName ) = 0 ;

      virtual INT32 getDomain ( const CHAR *pDomainName,
                                _sdbDomain **domain ) = 0 ;

      virtual INT32 getDomain ( const CHAR *pDomainName,
                                sdbDomain &domain ) = 0 ;

      virtual INT32 listDomains ( _sdbCursor **cursor,
                                  const bson::BSONObj &condition = _sdbStaticObject,
                                  const bson::BSONObj &selector = _sdbStaticObject,
                                  const bson::BSONObj &orderBy = _sdbStaticObject,
                                  const bson::BSONObj &hint = _sdbStaticObject
                                ) = 0 ;

      virtual INT32 listDomains ( sdbCursor &cursor,
                                  const bson::BSONObj &condition = _sdbStaticObject,
                                  const bson::BSONObj &selector = _sdbStaticObject,
                                  const bson::BSONObj &orderBy = _sdbStaticObject,
                                  const bson::BSONObj &hint = _sdbStaticObject
                                ) = 0 ;


/*      virtual INT32 modifyConfig ( INT32 nodeID,
                       std::map<std::string,std::string> &config ) = 0 ;

      virtual INT32 getConfig ( INT32 nodeID,
                       std::map<std::string,std::string> &config ) = 0 ;

      virtual INT32 modifyConfig (
                       std::map<std::string,std::string> &config ) = 0 ;

      virtual INT32 getConfig (
                       std::map<std::string,std::string> &config ) = 0 ;
*/
      static _sdb *getObj () ;
   } ;
/** \typedef class _sdb _sdb
*/
   typedef class _sdb _sdb ;
/** \class sdb
    \brief Database operation interfaces of admin.
*/
   class DLLEXPORT sdb
   {
   private:
      sdb ( const sdb& other ) ;
      sdb& operator=( const sdb& ) ;
   public :
/** \var pSDB
    \breif A pointer of virtual base class _sdb

    Class sdb is a shell for _sdb. We use pSDB to
    call the methods in class _sdb.
*/
      _sdb *pSDB ;

/** \fn sdb()
    \brief Default constructor.
*/
      sdb () :
      pSDB ( _sdb::getObj() )
      {
      }

/** \fn ~sdb()
    \brief Destructor.
*/
      ~sdb ()
      {
         if ( pSDB )
            delete pSDB ;
      }

/** \fn INT32 connect ( const CHAR *pHostName,
                      UINT16 port
                    )
    \brief Connect to remote Database Server.
    \param [in] pHostName The Host Name or IP Address of Database Server.
    \param [in] port The Port of Database Server.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 connect ( const CHAR *pHostName,
                      UINT16 port
                    )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->connect ( pHostName, port ) ;
      }

/** \fn INT32 connect ( const CHAR *pHostName,
                     UINT16 port,
                     const CHAR *pUsrName,
                     const CHAR *pPasswd
                     )
    \brief Connect to remote Database Server.
    \param [in] pHostName The Host Name or IP Address of Database Server.
    \param [in] port The Port of Database Server.
    \param [in] pUsrName The connection user name.
    \param [in] pPasswd The connection password.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
     INT32 connect ( const CHAR *pHostName,
                     UINT16 port,
                     const CHAR *pUsrName,
                     const CHAR *pPasswd
                     )
     {
        if ( !pSDB )
           return SDB_SYS ;
        return pSDB->connect ( pHostName, port,
                               pUsrName, pPasswd ) ;
     }

/** \fn INT32 connect ( const CHAR *pHostName,
                      const CHAR *pServiceName
                    )
    \brief Connect to remote Database Server.
    \param [in] pHostName The Host Name or IP Address of Database Server.
    \param [in] pServiceName The Service Name of Database Server.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 connect ( const CHAR *pHostName,
                      const CHAR *pServiceName
                    )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->connect ( pHostName, pServiceName ) ;
      }

/** \fn INT32 connect ( const CHAR *pHostName,
                     const CHAR *pServiceName,
                     const CHAR *pUsrName,
                     const CHAR *pPasswd
                     )
    \brief Connect to remote Database Server.
    \param [in] pHostName The Host Name or IP Address of Database Server.
    \param [in] pServiceName The Service Name of Database Server.
    \param [in] pUsrName The connection user name.
    \param [in] pPasswd The connection password.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 connect ( const CHAR *pHostName,
                      const CHAR *pServiceName,
                      const CHAR *pUsrName,
                      const CHAR *pPasswd )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->connect ( pHostName, pServiceName,
                                 pUsrName, pPasswd ) ;
      }

/** \fn INT32 connect ( const CHAR **pConnAddrs,
                        INT32 arrSize,
                        const CHAR *pUsrName,
                        const CHAR *pPasswd
                      )
    \brief Connect to database used  a random  valid address in the array.
    \param [in] pConnAddrs The array of the coord's address
    \param [in] arrSize The size of the array
    \param [in] pUsrName The connection user name.
    \param [in] pPasswd The connection password.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 connect ( const CHAR **pConnAddrs,
                      INT32 arrSize,
                      const CHAR *pUsrName,
                      const CHAR *pPasswd )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->connect ( pConnAddrs, arrSize,
                                 pUsrName, pPasswd ) ;
      }

/** \fn INT32 createUsr( const CHAR *pUsrName,
                         const CHAR *pPasswd )
    \brief Add an user in current database.
    \param [in] pUsrName The connection user name.
    \param [in] pPasswd The connection password.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createUsr( const CHAR *pUsrName,
                       const CHAR *pPasswd )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->createUsr( pUsrName, pPasswd ) ;
      }

/** \fn INT32 removeUsr( const CHAR *pUsrName,
                           const CHAR *pPasswd )
    \brief Remove the spacified user from current database.
    \param [in] pUsrName The connection user name.
    \param [in] pPasswd The connection password.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 removeUsr( const CHAR *pUsrName,
                       const CHAR *pPasswd )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->removeUsr( pUsrName, pPasswd ) ;
      }

/** \fn void disconnect ()
    \brief Disconnect the remote Database Server.
*/
      void disconnect ()
      {
         if ( !pSDB )
            return ;
         pSDB->disconnect () ;
      }

/** \fn  INT32 getSnapshot ( sdbCursor &cursor,
                          INT32 snapType,
                          const bson::BSONObj &condition,
                          const bson::BSONObj &selector,
                          const bson::BSONObj &orderBy
                        )
    \brief Get the snapshots of specified type.
    \param [in] snapType The snapshot type as below

        SDB_SNAP_CONTEXTS         : Get all contexts' snapshot
        SDB_SNAP_CONTEXTS_CURRENT : Get the current context's snapshot
        SDB_SNAP_SESSIONS         : Get all sessions' snapshot
        SDB_SNAP_SESSIONS_CURRENT : Get the current session's snapshot
        SDB_SNAP_COLLECTIONS        : Get the collections' snapshot
        SDB_SNAP_COLLECTIONSPACES        : Get the collection spaces' snapshot
        SDB_SNAP_DATABASE         : Get database's snapshot
        SDB_SNAP_SYSTEM           : Get system's snapshot
        SDB_SNAP_CATA           : Get catalog's snapshot
       \param [in] condition The matching rule, match all the documents if not provided.
       \param [in] select The selective rule, return the whole document if not provided.
       \param [in] orderBy The ordered rule, result set is unordered if not provided.
       \param [out] cursor The return cursor object of query.
       \retval SDB_OK Operation Success
       \retval Others Operation Fail
*/
      INT32 getSnapshot ( sdbCursor &cursor,
                          INT32 snapType,
                          const bson::BSONObj &condition = _sdbStaticObject,
                          const bson::BSONObj &selector  = _sdbStaticObject,
                          const bson::BSONObj &orderBy   = _sdbStaticObject
                        )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getSnapshot ( cursor, snapType, condition,
                                    selector, orderBy ) ;
      }


/* \fn  INT32 getSnapshot (_sdbCursor **cursor,
                          INT32 snapType,
                          const bson::BSONObj &condition,
                          const bson::BSONObj &selector,
                          const bson::BSONObj &orderBy
                        )
    \brief Get the snapshots of specified type.
    \param [in] snapType The snapshot type as below

        SDB_SNAP_CONTEXTS         : Get all contexts' snapshot
        SDB_SNAP_CONTEXTS_CURRENT : Get the current context's snapshot
        SDB_SNAP_SESSIONS         : Get all sessions' snapshot
        SDB_SNAP_SESSIONS_CURRENT : Get the current session's snapshot
        SDB_SNAP_COLLECTIONS        : Get the collections' snapshot
        SDB_SNAP_COLLECTIONSPACES        : Get the collection spaces' snapshot
        SDB_SNAP_DATABASE         : Get database snapshot
        SDB_SNAP_SYSTEM           : Get system snapshot
        SDB_SNAP_CATALOG           : Get catalog snapshot
     \param [in] condition The matching rule, match all the documents if not provided.
     \param [in] select The selective rule, return the whole document if not provided.
     \param [in] orderBy The ordered rule, result set is unordered if not provided.
     \param [out] cursor The return cursor handle of query.
     \retval SDB_OK Operation Success
     \retval Others Operation Fail
*/
      INT32 getSnapshot ( _sdbCursor **cursor,
                          INT32 snapType,
                          const bson::BSONObj &condition = _sdbStaticObject,
                          const bson::BSONObj &selector = _sdbStaticObject,
                          const bson::BSONObj &orderBy = _sdbStaticObject
                        )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getSnapshot ( cursor, snapType, condition,
                                    selector, orderBy ) ;
      }

/** \fn INT32 resetSnapshot ( const bson::BSONObj &condition )
    \brief Reset the snapshot.
    \param [in] condition The matching rule, usually specifies the node in sharding environment,
                   in standalone mode, this option is ignored.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 resetSnapshot ( const bson::BSONObj &condition = _sdbStaticObject )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->resetSnapshot ( condition ) ;
      }

/* \fn INT32 getList ( _sdbCursor **cursor,
                      INT32 listType,
                      const bson::BSONObj &condition,
                      const bson::BSONObj &selector,
                      const bson::BSONObj &orderBy
                    )
    \brief Get the informations of specified type.
    \param [in] listType The list type as below

        SDB_LIST_CONTEXTS         : Get all contexts list
        SDB_LIST_CONTEXTS_CURRENT : Get contexts list for the current session
        SDB_LIST_SESSIONS         : Get all sessions list
        SDB_LIST_SESSIONS_CURRENT : Get the current session
        SDB_LIST_COLLECTIONS      : Get all collections list
        SDB_LIST_COLLECTIONSPACES : Get all collecion spaces' list
        SDB_LIST_STORAGEUNITS     : Get storage units list
        SDB_LIST_GROUPS           : Get replica groups list ( only applicable in sharding env )
        SDB_LIST_STOREPROCEDURES           : Get stored procedure list ( only applicable in sharding env )
   \param [in] condition The matching rule, match all the documents if null.
   \param [in] select The selective rule, return the whole document if null.
   \param [in] orderBy The ordered rule, never sort if null.
   \param [out] cursor The return cursor handle of query.
   \retval SDB_OK Operation Success
   \retval Others Operation Fail
*/
    INT32 getList ( _sdbCursor **cursor,
                    INT32 listType,
                    const bson::BSONObj &condition = _sdbStaticObject,
                    const bson::BSONObj &selector  = _sdbStaticObject,
                    const bson::BSONObj &orderBy   = _sdbStaticObject
                  )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getList ( cursor,
                                listType,
                                condition,
                                selector,
                                orderBy ) ;
      }


/** \fn INT32 getList ( sdbCursor &cursor,
                      INT32 listType,
                      const bson::BSONObj &condition,
                      const bson::BSONObj &selector,
                      const bson::BSONObj &orderBy
                    )
    \brief Get the informations of specified type.
    \param [in] listType The list type as below

        SDB_LIST_CONTEXTS         : Get all contexts list
        SDB_LIST_CONTEXTS_CURRENT : Get contexts list for the current session
        SDB_LIST_SESSIONS         : Get all sessions list
        SDB_LIST_SESSIONS_CURRENT : Get the current session
        SDB_LIST_COLLECTIONS      : Get all collections list
        SDB_LIST_COLLECTIONSPACES : Get all collecion spaces' list
        SDB_LIST_STORAGEUNITS     : Get storage units list
        SDB_LIST_GROUPS           : Get replicaGroup list ( only applicable in sharding env )
   \param [in] condition The matching rule, match all the documents if null.
   \param [in] select The selective rule, return the whole document if null.
   \param [in] orderBy The ordered rule, never sort if null.
   \param [out] cursor The return cursor object of query.
   \retval SDB_OK Operation Success
   \retval Others Operation Fail
*/

      INT32 getList ( sdbCursor &cursor,
                      INT32 listType,
                      const bson::BSONObj &condition = _sdbStaticObject,
                      const bson::BSONObj &selector  = _sdbStaticObject,
                      const bson::BSONObj &orderBy   = _sdbStaticObject
                    )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getList ( cursor,
                                listType,
                                condition,
                                selector,
                                orderBy ) ;
      }

/* \fn INT32 getCollection ( const CHAR *pCollectionFullName,
                            _sdbCollection **collection
                          )
    \biref Get the specified collection.
    \param [in] pCollectionFullName The full name of collection.
    \param [out] collection The return collection handle of query.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getCollection ( const CHAR *pCollectionFullName,
                            _sdbCollection **collection
                          )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getCollection ( pCollectionFullName,
                                      collection ) ;
      }

/** \fn INT32 getCollection ( const CHAR *pCollectionFullName,
                            sdbCollection &collection
                          )
    \biref Get the specified collection.
    \param [in] pCollectionFullName The full name of collection.
    \param [out] collection The return collection object of query.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getCollection ( const CHAR *pCollectionFullName,
                            sdbCollection &collection
                          )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getCollection ( pCollectionFullName,
                                      collection ) ;
      }

/* \fn INT32 getCollectionSpace ( const CHAR *pCollectionSpaceName,
                                 _sdbCollectionSpace **cs)
     \brief Get the specified collection space.
     \param [in] pCollectionSpaceName The name of collection space.
    \param [out] cs The return collection space handle of query.
     \retval SDB_OK Operation Success
     \retval Others Operation Fail
*/
      INT32 getCollectionSpace ( const CHAR *pCollectionSpaceName,
                                 _sdbCollectionSpace **cs
                               )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getCollectionSpace ( pCollectionSpaceName,
                                           cs ) ;
      }


/** \fn INT32 getCollectionSpace ( const CHAR *pCollectionSpaceName,
                                  sdbCollectionSpace &cs)
     \brief Get the specified collection space.
     \param [in] pCollectionSpaceName The name of collection space.
     \param [out] cs The return collection space object of query.
     \retval SDB_OK Operation Success
     \retval Others Operation Fail
*/
      INT32 getCollectionSpace ( const CHAR *pCollectionSpaceName,
                                 sdbCollectionSpace &cs
                               )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getCollectionSpace ( pCollectionSpaceName,
                                           cs ) ;
      }

/* \fn INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                    INT32 iPageSize,
                                    _sdbCollectionSpace **cs
                                  )
    \brief Create collection space with specified pagesize.
    \param [in] pCollectionSpaceName The name of collection space.
    \param [in] iPageSize The Page Size as below

        SDB_PAGESIZE_4K
        SDB_PAGESIZE_8K
        SDB_PAGESIZE_16K
        SDB_PAGESIZE_32K
        SDB_PAGESIZE_64K
        SDB_PAGESIZE_DEFAULT
    \param [out] cs The return collection space handle of creation.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                    INT32 iPageSize,
                                    _sdbCollectionSpace **cs
                                  )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->createCollectionSpace ( pCollectionSpaceName,
                                              iPageSize,
                                              cs ) ;
      }


/** \fn INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                      INT32 iPageSize,
                                      sdbCollectionSpace &cs
                                     )
    \brief Create collection space with specified pagesize.
    \param [in] pCollectionSpaceName The name of collection space.
    \param [in] iPageSize The Page Size as below

        SDB_PAGESIZE_4K
        SDB_PAGESIZE_8K
        SDB_PAGESIZE_16K
        SDB_PAGESIZE_32K
        SDB_PAGESIZE_64K
        SDB_PAGESIZE_DEFAULT
    \param [out] cs The return collection space object of creation.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                    INT32 iPageSize,
                                    sdbCollectionSpace &cs
                                  )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->createCollectionSpace ( pCollectionSpaceName,
                                              iPageSize, cs ) ;
      }


/** \fn INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                      const bson::BSONObj &options,
                                      sdbCollectionSpace &cs
                                     )
    \brief Create collection space with specified pagesize.
    \param [in] pCollectionSpaceName The name of collection space.
    \param [in] options The options specified by user, e.g. {"PageSize": 4096, "Domain": "mydomain"}.

        PageSize   : Assign the pagesize of the collection space
        Domain     : Assign which domain does current collection space belong to
    \param [out] cs The return collection space object of creation.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createCollectionSpace ( const CHAR *pCollectionSpaceName,
                                    const bson::BSONObj &options,
                                    sdbCollectionSpace &cs
                                  )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->createCollectionSpace ( pCollectionSpaceName,
                                              options, cs ) ;
      }

/** \fn INT32 dropCollectionSpace ( const CHAR *pCollectionSpaceName )
    \brief Remove the specified collection space.
    \param [in] pCollectionSpaceName The name of collection space.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 dropCollectionSpace ( const CHAR *pCollectionSpaceName )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->dropCollectionSpace ( pCollectionSpaceName ) ;
      }

/* \fn INT32 listCollectionSpaces  ( _sdbCursor **result )
    \brief List all collection space of current database(include temporary collection space).
    \param [out] result The return cursor handle of query.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listCollectionSpaces ( _sdbCursor **result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listCollectionSpaces ( result ) ;
      }

/** \fn INT32 listCollectionSpaces  ( sdbCursor &result )
    \brief List all collection space of current database(include temporary collection space).
    \param [out] result The return cursor object of query.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listCollectionSpaces ( sdbCursor &result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listCollectionSpaces ( result ) ;
      }

/* \fn INT32 listCollections ( _sdbCursor **result )
    \brief list all collections in current database.
    \param [out] result The return cursor handle of query.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listCollections ( _sdbCursor **result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listCollections ( result ) ;
      }

/** \fn  INT32 listCollections ( sdbCursor &result )
    \brief list all collections in current database.
    \param [out] result The return cursor object of query.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listCollections ( sdbCursor &result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listCollections ( result ) ;
      }

/* \fn INT32 listReplicaGroups ( _sdbCursor **result )
    \brief List all replica groups of current database.
    \param [out] result The return cursor handle of query.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listReplicaGroups ( _sdbCursor **result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listReplicaGroups ( result ) ;
      }


/** \fn INT32 listReplicaGroups ( sdbCursor &result )
    \brief List all replica groups of current database.
    \param [out] result The return cursor object of query.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listReplicaGroups ( sdbCursor &result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listReplicaGroups ( result ) ;
      }

/* \fn INT32 getReplicaGroup ( const CHAR *pName, _sdbReplicaGroup **result )
    \brief Get the specified replica group.
    \param [in] pName The name of replica group.
    \param [out] result The sdbReplicaGroup object.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getReplicaGroup ( const CHAR *pName, _sdbReplicaGroup **result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getReplicaGroup ( pName, result ) ;
      }


/** \fn INT32 getReplicaGroup ( const CHAR *pName, sdbReplicaGroup &result )
    \brief Get the specified replica group.
    \param [in] pName The name of replica group.
    \param [out] result The sdbReplicaGroup object.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getReplicaGroup ( const CHAR *pName, sdbReplicaGroup &result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getReplicaGroup ( pName, result ) ;
      }

/* \fn INT32 getReplicaGroup ( INT32 id, _sdbReplicaGroup **result )
    \brief Get the specified replica group.
    \param [in] id The id of replica group.
    \param [out] result The _sdbReplicaGroup object.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getReplicaGroup ( INT32 id, _sdbReplicaGroup **result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getReplicaGroup ( id, result ) ;
      }

/** \fn INT32 getReplicaGroup ( INT32 id, sdbReplicaGroup &result )
    \brief Get the specified replica group.
    \param [in] id The id of replica group.
    \param [out] result The sdbReplicaGroup object.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getReplicaGroup ( INT32 id, sdbReplicaGroup &result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getReplicaGroup ( id, result ) ;
      }

/* \fn INT32 createReplicaGroup ( const CHAR *pName, _sdbReplicaGroup **replicaGroup )
    \brief Create the specified replica group.
    \param [in] pName The name of the replica group.
    \param [out] replicaGroup The return _sdbReplicaGroup object.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createReplicaGroup ( const CHAR *pName, _sdbReplicaGroup **replicaGroup )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->createReplicaGroup ( pName, replicaGroup ) ;
      }

/** \fn INT32 createReplicaGroup ( const CHAR *pName, sdbReplicaGroup &replicaGroup )
    \brief Create the specified replica group.
    \param [in] pName The name of the replica group.
    \param [out] replicaGroup The return sdbReplicaGroup object.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createReplicaGroup ( const CHAR *pName, sdbReplicaGroup &replicaGroup )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->createReplicaGroup ( pName, replicaGroup ) ;
      }

/** \fn INT32 removeReplicaGroup ( const CHAR *pName )
    \brief Remove the specified replica group.
    \param [in] pName The name of the replica group
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 removeReplicaGroup ( const CHAR *pName )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->removeReplicaGroup ( pName ) ;
      }

/** \fn INT32 createReplicaCataGroup (  const CHAR *pHostName,
                                        const CHAR *pServiceName,
                                        const CHAR *pDatabasePath,
                                        const bson::BSONObj &configure )
    \brief Create a catalog replica group.
    \param [in] pHostName The hostname for the catalog replica group
    \param [in] pServiceName The servicename for the catalog replica group
    \param [in] pDatabasePath The path for the catalog replica group
    \param [in] configure The configurations for the catalog replica group
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createReplicaCataGroup (  const CHAR *pHostName,
                               const CHAR *pServiceName,
                               const CHAR *pDatabasePath,
                               const bson::BSONObj &configure )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->createReplicaCataGroup ( pHostName, pServiceName,
                                        pDatabasePath, configure ) ;
      }

/* \fn INT32 activateReplicaGroup ( const CHAR *pName, _sdbReplicaGroup **replicaGroup )
    \brief Activate the specified replica group.
    \param [in] pName The name of the replica group
    \param [out] replicaGroup The return _sdbReplicaGroup object
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 activateReplicaGroup ( const CHAR *pName, _sdbReplicaGroup **replicaGroup )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->activateReplicaGroup ( pName, replicaGroup ) ;
      }

/** \fn INT32 activateReplicaGroup ( const CHAR *pName, sdbReplicaGroup &replicaGroup )
    \brief Activate the specified replica group
    \param [in] pName The name of the replica group
    \param [out] replicaGroup The return sdbReplicaGroup object
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 activateReplicaGroup ( const CHAR *pName, sdbReplicaGroup &replicaGroup )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->activateReplicaGroup ( pName, replicaGroup ) ;
      }

/** \fn INT32 execUpdate( const CHAR *sql )
    \brief Executing SQL command for updating.
    \param [in] sql The SQL command.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 execUpdate( const CHAR *sql )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->execUpdate( sql ) ;
      }

/* \fn INT32 exec( const CHAR *sql,
                  _sdbCursor **result )
    \brief Executing SQL command.
    \param [in] sql The SQL command.
    \param [out] result The return cursor handle of matching documents.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 exec( const CHAR *sql,
                  _sdbCursor **result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->exec( sql, result ) ;
      }

/** \fn INT32 exec( const CHAR *sql,
                 sdbCursor &result )
    \brief Executing SQL command.
    \param [in] sql The SQL command.
    \param [out] result The return cursor object of matching documents.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 exec( const CHAR *sql,
                  sdbCursor &result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->exec( sql, result ) ;
      }

/** \fn INT32 transactionBegin()
    \brief Transaction commit.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 transactionBegin()
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->transactionBegin() ;
      }

/** \fn INT32 transactionCommit()
    \brief Transaction commit.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 transactionCommit()
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->transactionCommit() ;
      }

/** \fn INT32 transactionRollback()
    \brief Transaction rollback.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 transactionRollback()
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->transactionRollback() ;
      }
/** \fn INT32 flushConfigure( BSONObj &options )
    \brief flush the options to configure file.
    \param [in] options The configure infomation, pass {"Global":true} or {"Global":false}
                    In cluster environment, passing {"Global":true} will flush data's and catalog's configuration file,
                    while passing {"Global":false} will flush coord's configuration file.
                    In stand-alone environment, both them have the same behaviour.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 flushConfigure( const bson::BSONObj &options )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->flushConfigure( options ) ;
      }

/** \fn INT32 crtJSProcedure ( const CHAR *code )
 *  \brief create a store procedures.
 *  \param [in] code The code of store procedures
 *  \retval SDB_OK Operation Success
 *  \retval Others  Operation Fail
*/
      INT32 crtJSProcedure ( const CHAR *code )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->crtJSProcedure( code ) ;
      }

/** \fn INT32 rmProcedure( const CHAR *spName )
 *  \brief remove a store procedure.
 *  \param [in] spName The name of store procedure
 *  \retval SDB_OK Operation Success
 *  \retval Others  Operation Fail
 */
      INT32 rmProcedure( const CHAR *spName )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->rmProcedure( spName ) ;
      }

      INT32 listProcedures( _sdbCursor **cursor, const bson::BSONObj &condition )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listProcedures( cursor, condition ) ;
      }

/** \fn INT32 listProcedures( sdbCursor &cursor, const bson::BSONObj &condition )
 *  \brief list store procedures.
 *  \param [in] condition The condition of list
 *  \param [out] cursor The cursor of the result
 *  \retval SDB_OK Operation Success
 *  \retval Others  Operation Fail
 */
      INT32 listProcedures( sdbCursor &cursor, const bson::BSONObj &condition )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listProcedures( cursor, condition ) ;
      }

     INT32 evalJS( _sdbCursor **cursor,
                             const CHAR *code,
                             SDB_SPD_RES_TYPE *type,
                             const bson::BSONObj &errmsg )
     {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->evalJS( cursor, code, type, errmsg ) ;
     }

/** \fn INT32 evalJS( sdbCursor &cursor,
                             const CHAR *code,
                             SDB_SPD_RES_TYPE *type,
                             const bson::BSONObj &errmsg )
 * \brief eval a func.
 * \      type is declared in spd.h. see SDB_FMP_RES_TYPE.
 * \param [in] code The code to eval
 * \param [out] type The type of value
 * \param [out] cursor The cursor handle of current eval
 * \param [out] errmsg The errmsg from eval
 * \retval SDB_OK Operation Success
 * \retval Others  Operation Fail
 */
     INT32 evalJS( sdbCursor &cursor,
                             const CHAR *code,
                             SDB_SPD_RES_TYPE *type,
                             const bson::BSONObj &errmsg )
     {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->evalJS( cursor, code, type, errmsg ) ;
     }

/** \fn INT32 backupOffline ( const bson::BSONObj &options)
    \brief Backup the whole database or specifed replica group.
    \param [in] options Contains a series of backup configuration infomations. Backup the whole cluster if null. The "options" contains 5 options as below. All the elements in options are optional. eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName", "Description":description, "EnsureInc":true, "OverWrite":true}

        GroupID     : The id(s) of replica group(s) which to be backuped
        GroupName   : The replica groups which to be backuped
        Path        : The backup path, if not assign, use the backup path assigned in the configuration file,
                      the path support to use wildcard(%g/%G:group name, %h/%H:host name, %s/%S:service name). e.g.  {Path:"/opt/sequoiadb/backup/%g"}
        isSubDir    : Whether the path specified by paramer "Path" is a subdirectory of the path specified in the configuration file, default to be false
        Name        : The name for the backup
        Prefix      : The prefix of name for the backup, default to be null. e.g. {Prefix:"%g_bk_"}
        EnableDateDir : Whether turn on the feature which will create subdirectory named to current date like "YYYY-MM-DD" automatically, default to be false
        Description : The description for the backup
        EnsureInc   : Whether excute increment synchronization, default to be false
        OverWrite   : Whether overwrite the old backup file, default to be false
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 backupOffline ( const bson::BSONObj &options)
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->backupOffline( options ) ;
      }

      INT32 listBackup ( _sdbCursor **cursor,
                              const bson::BSONObj &options,
                              const bson::BSONObj &condition = _sdbStaticObject,
                              const bson::BSONObj &selector = _sdbStaticObject,
                              const bson::BSONObj &orderBy = _sdbStaticObject)
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listBackup( cursor, options, condition, selector, orderBy ) ;
      }

/** \fn INT32 listBackup ( sdbCursor &cursor,
                              const bson::BSONObj &options,
                              const bson::BSONObj &condition = _sdbStaticObject,
                              const bson::BSONObj &selector = _sdbStaticObject,
                              const bson::BSONObj &orderBy = _sdbStaticObject);
    \brief List the backups.
    \param [in] options Contains configuration infomations for remove backups, list all the backups in the default backup path if null. The "options" contains 3 options as below. All the elements in options are optional. eg: {"GroupName":["rgame1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}

        GroupName   : Assign the backups of specifed replica groups to be list
        Path        : Assign the backups in specifed path to be list, if not assign, use the backup path asigned in the configuration file
        Name        : Assign the backups with specifed name to be list
    \param [in] condition The matching rule, return all the documents if not provided
    \param [in] selector The selective rule, return the whole document if null
    \param [in] orderBy The ordered rule, never sort if null
    \param [out] cursor The cusor handle of result
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listBackup ( sdbCursor &cursor,
                              const bson::BSONObj &options,
                              const bson::BSONObj &condition = _sdbStaticObject,
                              const bson::BSONObj &selector = _sdbStaticObject,
                              const bson::BSONObj &orderBy = _sdbStaticObject)
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listBackup( cursor, options, condition, selector, orderBy ) ;
      }

/** \fn INT32 removeBackup ( const bson::BSONObj &options);
    \brief Remove the backups.
    \param [in] options Contains configuration infomations for remove backups, remove all the backups in the default backup path if null. The "options" contains 3 options as below. All the elements in options are optional. eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}

        GroupName   : Assign the backups of specifed replica groups to be remove
        Path        : Assign the backups in specifed path to be remove, if not assign, use the backup path asigned in the configuration file
        Name        : Assign the backups with specifed name to be remove
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 removeBackup ( const bson::BSONObj &options)
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->removeBackup( options ) ;
      }

      INT32 listTasks ( _sdbCursor **cursor,
                        const bson::BSONObj &condition = _sdbStaticObject,
                        const bson::BSONObj &selector = _sdbStaticObject,
                        const bson::BSONObj &orderBy = _sdbStaticObject,
                        const bson::BSONObj &hint = _sdbStaticObject)
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listTasks ( cursor,
                                  condition,
                                  selector,
                                  orderBy,
                                  hint ) ;
      }
/** \fn INT32 listTasks ( sdbCursor &cursor,
                          const bson::BSONObj &condition = _sdbStaticObject,
                          const bson::BSONObj &selector = _sdbStaticObject,
                          const bson::BSONObj &orderBy = _sdbStaticObject,
                          const bson::BSONObj &hint = _sdbStaticObject) ;
    \brief List the tasks.
    \param [in] condition The matching rule, return all the documents if null
    \param [in] selector The selective rule, return the whole document if null
    \param [in] orderBy The ordered rule, never sort if null
    \param [in] hint The hint, automatically match the optimal hint if null
    \param [out] cursor The connection handle
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listTasks ( sdbCursor &cursor,
                        const bson::BSONObj &condition = _sdbStaticObject,
                        const bson::BSONObj &selector = _sdbStaticObject,
                        const bson::BSONObj &orderBy = _sdbStaticObject,
                        const bson::BSONObj &hint = _sdbStaticObject)
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listTasks ( cursor,
                                  condition,
                                  selector,
                                  orderBy,
                                  hint ) ;
      }

/** \fn INT32 waitTasks ( const SINT64 *taskIDs,
                             SINT32 num ) ;
    \brief Wait the tasks to finish.
    \param [in] taskIDs The array of task id
    \param [in] num The number of task id
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 waitTasks ( const SINT64 *taskIDs,
                        SINT32 num )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->waitTasks ( taskIDs,
                                  num ) ;
      }

/** \fn INT32 cancelTask ( SINT64 taskID,
                           BOOLEAN isAsync ) ;
    \brief Cancel the specified task.
    \param [in] taskID The task id
    \param [in] isAsync The operation "cancel task" is async or not,
                "true" for async, "false" for sync. Default sync
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 cancelTask ( SINT64 taskID,
                         BOOLEAN isAsync )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->cancelTask ( taskID,
                                   isAsync ) ;
      }

/** \fn INT32 setSessionAttr ( const bson::BSONObj &options ) ;
    \brief Set the attributes of the session.
    \param [in] options The configuration options for session.The options are as below:

        PreferedInstance : indicate which instance to respond read request in current session.
                          eg:{"PreferedInstance":"m"/"M"/"s"/"S"/"a"/"A"/1-7},
                          prefer to choose "read and write instance"/"read only instance"/"anyone instance"/instance1-insatance7,
                          default to be {"PreferedInstance":"A"}, means would like to choose anyone instance to respond read request such as query. 
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 setSessionAttr ( const bson::BSONObj &options = _sdbStaticObject )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->setSessionAttr ( options ) ;
      }

/** \fn INT32 closeAllCursors () ;
    \brief Close all the cursors in current thread, we can't use those cursors
           to get data again.
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 closeAllCursors ()
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->closeAllCursors () ;
      }

/** \fn INT32 isValid ( BOOLEAN *result ) ;
    \brief Judge whether the connection is valid.
    \param [out] result the output result
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
    \deprecated Deprecated in version 2.x. Use "BOOLEAN isValid ()" instead.
*/
      INT32 isValid ( BOOLEAN *result )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->isValid ( result ) ;
      }


/** \fn BOOLEAN isValid () ;
    \brief Judge whether the connection is valid.
    \retval TRUE for the connection is valid while FALSE for not
*/
      BOOLEAN isValid ()
      {
         if ( !pSDB )
            return FALSE ;
         return pSDB->isValid () ;
      }

/** \fn INT32 createDomain ( const CHAR *pDomainName,
                             const bson::BSONObj &options,
                             sdbDomain &domain ) ;
    \brief Create a domain.
    \param [in] pDomainName The name of the domain
    \param [in] options The options for the domain. The options are as below:

        Groups:    The list of replica groups' names which the domain is going to contain.
                   eg: { "Groups": [ "group1", "group2", "group3" ] }
                   If this argument is not included, the domain will contain all replica groups in the cluster.
        AutoSplit: If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
                   the data of this collection will be split(hash split) into all the groups in this domain automatically.
                   However, it won't automatically split data into those groups which were add into this domain later.
                   eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
    \param [out] domain The created sdbDomain object
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 createDomain ( const CHAR *pDomainName,
                           const bson::BSONObj &options,
                           sdbDomain &domain )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->createDomain ( pDomainName, options, domain ) ;
      }

/** \fn INT32 dropDomain ( const CHAR *pDomainName ) ;
    \brief Drop a domain.
    \param [in] pDomainName The name of the domain
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/    INT32 dropDomain ( const CHAR *pDomainName )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->dropDomain ( pDomainName ) ;
      }

/** \fn INT32 getDomain ( const CHAR *pDomainName,
                          sdbDomain &domain ) ;
    \brief Get a domain.
    \param [in] pDomainName The name of the domain
    \param [out] domain The sdbDomain object to get
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 getDomain ( const CHAR *pDomainName,
                        sdbDomain &domain )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getDomain ( pDomainName, domain ) ;
      }

/** \fn INT32 listDomains ( sdbCursor &cursor,
                          const bson::BSONObj &condition,
                          const bson::BSONObj &selector,
                          const bson::BSONObj &orderBy,
                          const bson::BSONObj &hint ) ;
    \brief List the domains.
    \param [in] condition The matching rule, return all the documents if null
    \param [in] selector The selective rule, return the whole document if null
    \param [in] orderBy The ordered rule, never sort if null
    \param [in] hint The hint, automatically match the optimal hint if null
    \param [out] cursor The sdbCursor object of result
    \retval SDB_OK Operation Success
    \retval Others Operation Fail
*/
      INT32 listDomains ( sdbCursor &cursor,
                          const bson::BSONObj &condition,
                          const bson::BSONObj &selector,
                          const bson::BSONObj &orderBy,
                          const bson::BSONObj &hint )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->listDomains ( cursor, condition, selector, orderBy, hint ) ;
      }



/*      INT32 modifyConfig ( INT32 nodeID,
                           std::map<std::string,std::string> &config )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->modifyConfig ( nodeID, config ) ;
      }

      INT32 getConfig ( INT32 nodeID,
                        std::map<std::string,std::string> &config )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getConfig ( nodeID, config ) ;
      }

      INT32 modifyConfig ( std::map<std::string,std::string> &config )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->modifyConfig ( CURRENT_NODEID, config ) ;
      }

      INT32 getConfig ( std::map<std::string,std::string> &config )
      {
         if ( !pSDB )
            return SDB_SYS ;
         return pSDB->getConfig ( CURRENT_NODEID, config ) ;
      }*/

   } ;
/** \typedef class sdb sdb
      \brief Class sdb definition for sdb.
*/
   typedef class sdb sdb ;
}

#endif
