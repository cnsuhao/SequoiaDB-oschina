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

/** \file sequoiadb.h
    \brief php driver
 */

/** \class SequoiaDB
 */
class SequoiaDB
{
public:
   /** \fn __construct ( [string $hostName],
                         [string $userName],
                         [string $password] )
       \brief creates a new database connection object
       \param [in] $hostName Optional parameter,The Host Name or IP Address and The Service Name or Port of Database Server
       \param [in] $userName if registered user, need to enter user name and password
       \param [in] $password if registered user, need to enter user name and password
       \retval a new database connection object
   */
   __construct ( string $hostName = NULL,
                 string $userName = NULL,
                 string $password = NULL ) ;

   /** \fn void install ( array|string $install )
       \brief set function, "install" default is true, output array, set false, output string
       \param [in] $install The Array or json, key is "install"
   */
   void install ( array|string $install ) ;

   /** \fn array|string getError ( void )
       \brief When function return type is "array|string", the return content contains the error code,
       \brief but a small part of function does not return an error code, So you can call getError() to retrieve the error code,
   */
   array|string getError ( void ) ;

   /** \fn array|string connect ( string $hostName,
                                  [string $userName],
                                  [string $password] )
       \brief Connect to database
       \param [in] $hostName Optional parameter,The Host Name or IP Address and the Service Name or Port of Database Server
       \param [in] $userName if registered user, need to enter user name and password
       \param [in] $password if registered user, need to enter user name and password
       \retval Returns the database response
   */
   array|string connect ( string $hostName,
                          string $userName = NULL,
                          string $password = NULL ) ;
   
   /** \fn void close ( void )
       \brief Disconnect to database
   */
   void close ( void ) ;

   /** \fn SequoiaCursor execSQL ( string $sql )
       \brief execute of SQL code
       \param [in] $sql sql code
       \retval a new cursor object
   */
   SequoiaCursor execSQL ( string $sql ) ;
   
   /** \fn array|string execUpdateSQL ( string $sql )
       \brief execute of SQL code
       \param [in] $sql sql code
       \retval execute result
   */
   array|string execUpdateSQL ( string $sql ) ;

   /** \fn SequoiaCursor getSnapshot ( int $snapType,
                                       [array|string $condition],
                                       [array|string $select],
                                       [array|string $orderBy] )
       \brief Get the snapshot
       \param [in] $snapType The snapshot type as below
   
           SDB_SNAP_CONTEXTS: Get all the contexts' snapshot
           SDB_SNAP_CONTEXTS_CURRENT: Get the current context's snapshot
           SDB_SNAP_SESSIONS: Get all the sessions' snapshot
           SDB_SNAP_SESSIONS_CURRENT: Get the current session's snapshot
           SDB_SNAP_COLLECTION:Get all collection's snapshot
           SDB_SNAP_COLLECTIONSPACE:Get all collection space snapshot
           SDB_SNAP_DATABASE: Get the database's snapshot
           SDB_SNAP_SYSTEM:Get the system's snapshot
           SDB_SNAP_CATALOG:Get the group's snapshot
       \param [in] $condition The matching rule, match all the documents if null
       \param [in] $select The selective rule, return the whole document if null
       \param [in] $orderBy The ordered rule, never sort if null
       \retval a new cursor object
   */
   SequoiaCursor getSnapshot ( int $snapType,
                               array|string $condition = NULL,
                               array|string $select = NULL,
                               array|string $orderBy = NULL ) ;
   
   /** \fn SequoiaCursor getList ( int $listType,
                                   [array|string $condition],
                                   [array|string $select],
                                   [array|string $orderBy] )
       \brief Get the specified list
       \param [in] $listType The list type as below
   
           SDB_LIST_CONTEXTS: Get all the contexts' list
           SDB_LIST_CONTEXTS_CURRENT: Get the current context's list
           SDB_LIST_SESSIONS: Get all the sessions' list
           SDB_LIST_SESSIONS_CURRENT: Get the current session's list
           SDB_LIST_COLLECTIONS: Get all the collections' list
           SDB_LIST_COLLECTIONSPACES: Get all the collecion spaces' list
           SDB_LIST_STORAGEUNITS: Get all the storage units' list
           SDB_LIST_GROUPS:Get all the group list
       \param [in] $condition The matching rule, match all the documents if null
       \param [in] $select The selective rule, return the whole document if null
       \param [in] $orderBy The ordered rule, never sort if null
       \retval a new cursor object
   */
   SequoiaCursor getList ( int $listType,
                           array|string $condition = NULL,
                           array|string $select = NULL,
                           array|string $orderBy = NULL ) ;

   /** \fn SequoiaGroup selectGroup ( string $name )
       \brief Gets group object
       \param [in] $name The group name
       \retval a new group object
   */
   SequoiaGroup selectGroup ( string $name ) ;

   /** \fn array|string resetSnapshot ( void )
       \brief Reset the snapshot
       \retval Returns the database response
   */
   array|string resetSnapshot ( void ) ;
   
   /** \fn SequoiadbCS selectCS ( string $name, [int $pageSize] )
       \brief Gets a collection Space, if is not exist,will auto create
       \param [in] $name The collection Space name
       \param [in] $pageSize create collection space, set page size,input 4096,8192,16384,32768,65536
       \retval a new collection space object
   */
   SequoiadbCS selectCS ( string $name, int $pageSize = 4096 ) ;

   /** \fn SequoiaCursor listCSs ( void )
       \brief Lists all of the collection space
       \retval a new cursor object
   */
   SequoiaCursor listCSs ( void ) ;

   /** \fn SequoiaCursor listCollections ( void )
       \brief Lists all of the collection
       \retval a new cursor object
   */
   SequoiaCursor listCollections ( void ) ;

   /** \fn array|string createCataGroup ( string $hostname,
                                          string $serviceName,
                                          string $databasePath,
                                          array|string $config )
       \brief create cata group
       \param [in] $hostname cata group host name
       \param [in] $serviceName cata group service name
       \param [in] $databasePath cata group path
       \param [in] $config cata group create config
       \retval Returns the database response
   */
   array|string createCataGroup ( string $hostname,
                                  string $serviceName,
                                  string $databasePath,
                                  array|string $config ) ;

   /** \fn array|string dropCollectionSpace ( string $name )
       \brief drop a collection Space
       \param [in] $name The collection Space name
       \retval Returns the database response
   */
   array|string dropCollectionSpace ( string $name ) ;

} ;

/** \class SecureSdb
    \brief secure SequoiaDB, use SSL connection
 */
class SecureSdb: public SequoiaDB
{
} ;

/** \class SequoiaCS
 */
class SequoiaCS
{
public:

   /** \fn SequoiaCL selectCollection ( string $name, [array|string $options] )
       \brief Gets a collection, if is not exist, will auto create
       \param [in] $name The collection name
       \param [in] $options create collection,set collection options
       \retval a new collection object
   */
   SequoiaCL selectCollection ( string $name, array|string $options ) ;
   
   /** \fn array|string drop ( void )
       \brief Drops this collection space
       \retval Returns the database response
   */
   array|string drop ( void ) ;

   /** \fn array|string dropCollection ( string $name )
       \brief drop this collection
       \retval Returns the database response
   */
   array|string dropCollection ( string $name ) ;

   /** \fn string getName ( void )
       \brief Returns this collection space's name
       \retval Returns the name of this collection space
   */
   string getName ( void ) ;
} ;

/** \class SequoiaCL
 */
class SequoiaCL
{
public:
   
   /** \fn array|string insert ( array|string $record )
       \brief Inserts an array into the collection
       \param [in] $record a record
       \retval Returns an array or string of the object id
   */
   array|string insert ( array|string $record ) ;
   
   /** \fn array|string update ( array|string $rule,
                                 [array|string $condition],
                                 [array|string $hint] )
       \brief Update the matching documents in current collection
       \param [in] $rule The updating rule, cannot be null
       \param [in] $condition The matching rule, update all the documents if this parameter is null
       \param [in] $hint The hint, automatically match the optimal hint if null
       \note It won't work to update the "ShardingKey" field, but the other fields take effect
       \retval Returns the database response
   */
   array|string update ( array|string $rule,
                         array|string $condition = NULL,
                         array|string $hint = NULL ) ;
   
   /** \fn array|string remove ( [array|string $condition],
                                 [array|string $hint] )
       \brief Remove records from this collection
       \param [in] $condition condition The matching rule, delete all the documents if null
       \param [in] $hint The hint, automatically match the optimal hint if null
       \retval Returns the database response
   */
   array|string remove ( array|string $condition = NULL,
                         array|string $hint = NULL ) ;

   /** \fn SequoiaCursor find ( [array|string $condition],
                               [array|string $select],
                               [array|string $orderBy],
                               [array|string $hint],
                               [int $numToSkip],
                               [int $numToReturn] )
       \brief Querys this collection, returning a SequoiaCursor for the result set
       \param [in] $condition The matching rule, return all the documents if null
       \param [in] $select The selective rule, return the whole document if null
       \param [in] $orderBy The ordered rule, never sort if null
       \param [in] $hint The hint, automatically match the optimal hint if null
       \param [in] $numToSkip Skip the first numToSkip documents, never skip if this parameter is 0
       \param [in] $numToReturn Only return numToReturn documents, return all if this parameter is -1
       \retval Returns a SequoiaCursor for the result set
   */
   SequoiaCursor find ( array|string $condition = NULL,
                        array|string $select = NULL,
                        array|string $orderBy = NULL,
                        array|string $hint = NULL,
                        int $numToSkip = 0,
                        int $numToReturn = -1 ) ;

   /** \fn array|string split ( string $sourceName, string $destName, array|string $condition, array|string $endCondition )
       \brief data split,source collection split to dest collection
       \param [in] $sourceName source collection
       \param [in] $destName dest collection
       \param [in] $condition The matching rule
       \param [in] $endCondition The end of matching rule
       \retval Returns the database response
   */
   array|string split ( string $sourceName, string $destName, array|string $condition, array|string $endCondition = NULL ) ;

   /** \fn array|string split ( string $sourceName, string $destName, double $percent )
       \brief data split,source collection split to dest collection
       \param [in] $sourceName source collection
       \param [in] $destName dest collection
       \param [in] $percent  percentage of split
       \retval Returns the database response
   */
   array|string split ( string $sourceName, string $destName, double $percent ) ;

   /** \fn array|string drop ( void )
       \brief Drops this collection
       \retval Returns the database response
   */
   array|string drop ( void ) ;

   /** \fn SequoiaCursor aggregate ( array|string $aggrObj )
       \brief aggregate this collection, returning a SequoiaCursor for the result set
       \param [in] $aggrObj Aggregation parameter, if the input string or an associative array, you can enter only one parameter, such as the string, "{ $project: {field:1}}", an associative array, array( '$project' => array ( 'field' => 1 ) ), if the input array, you can enter multiple parameters, such as array ( "{$project:{field1:1,field2:2}}", "{$project:{field1:1}}" ) or array ( array ( '$project' => array ( 'field1' => 1, "field2' => 2 ) ), array ( '$project' => array ( 'field1' => 1 ) ) )
       \retval Returns a SequoiaCursor for the result set
   */
   SequoiaCursor aggregate ( array|string $aggrObj ) ;

   /** \fn array|string createIndex ( array|string $indexDef,
                                      string $pIndexName,
                                      [bool   $isUnique] )
       \brief Create the index in current collection
       \param [in] $indexDef The bson structure of index element, e.g. {name:1, age:-1}
       \param [in] $pIndexName The index name
       \param [in] $isUnique Whether the index elements are unique or not,default is FALSE
       \retval Returns the database response
   */
   array|string createIndex ( array|string $indexDef,
                              string $pIndexName,
                              bool   $isUnique = FALSE) ;

   /** \fn array|string deleteIndex ( string $pIndexName )
       \brief delete the index in current collection
       \param [in] $pIndexName The index name
       \retval Returns the database response
   */
   array|string deleteIndex ( string $pIndexName ) ;

   /** \fn SequoiaCursor getIndex ( string $pIndexName )
       \brief Get all of or one of the indexes in current collection
       \param [in] $pIndexName The index name
       \retval Returns a new SequoiaCursor object
   */
   SequoiaCursor getIndex ( string $pIndexName = "" ) ;
   
   /** \fn string getCSName ( void )
       \brief Returns this collection space's name
       \retval Returns the name of this collection space
   */
   string getCSName ( void ) ;
   
   /** \fn string getCollectionName ( void )
       \brief Returns this collection's name
       \retval Returns the name of this collection
   */
   string getCollectionName ( void ) ;
   
   /** \fn string getFullName ( void )
       \brief Returns this collection full name
       \retval Returns the name of this collection space
   */
   string getFullName ( void ) ;
   
   /** \fn int count ( array|string $condition )
       \brief Counts the number of documents in this collection
       \param [in] $condition The matching rule, return the count of all documents if this parameter is null
       \retval Returns the number of documents matching the query
   */
   int count ( array|string $condition = NULL ) ;
} ;

/** \class SequoiaCursor
 */
class SequoiaCursor
{
public:
   
   /** \fn array|string getNext ( void )
       \brief Return the next object to which this cursor points, and advance the cursor
       \retval Returns the next object
   */
   array|string getNext ( void ) ;
   
   /** \fn array|string current ( void )
       \brief Return the current element
       \retval The current result as an associative array or a string
   */
   array|string current ( void ) ;
   
   /** \fn array|string updateCurrent ( array|string $rule )
       \brief Update the current document of cursor
       \param [in] $rule The updating rule, cannot be null
       \retval Returns the database response
   */
   /*array|string updateCurrent ( array|string $rule ) ;*/
   
   /** \fn array|string deleteCurrent ( void )
       \brief Delete the current document of cursor
       \retval Returns the database response
   */
   /*array|string deleteCurrent ( void ) ;*/
} ;

/** \class SequoiaGroup
 */
class SequoiaGroup
{
public:

   /** \fn int getNodeNum ( int $status )
       \brief get the status node number
       \param [in] $status The node status, cannot be null
       \retval Returns the node number
   */
   int getNodeNum ( int $status ) ;

   /** \fn array|string getDetail ( void )
       \brief get the group detail
       \retval Returns the group detail
   */
   array|string getDetail ( void ) ;

   /** \fn SequoiaNode getMaster ( void )
       \brief get the master node
       \retval Returns the master node
   */
   SequoiaNode getMaster ( void ) ;

   /** \fn SequoiaNode getSlave ( void )
       \brief get the slave node
       \retval Returns the slave node
   */
   SequoiaNode getSlave ( void ) ;

   /** \fn SequoiaNode getNode ( string $nodeName )
       \brief get the node
       \param [in] $nodeName The node name, cannot be null
       \retval Returns the node
   */
   SequoiaNode getNode ( string $nodeName ) ;

   /** \fn array|string createNode ( string $hostName, string $serviceName, string $databasePath, [array|string $config] )
       \brief create node
       \param [in] $hostName The node host name, cannot be null
       \param [in] $serviceName The node service name, cannot be null
       \param [in] $databasePath The node database path, cannot be null
       \param [in] $config The node name
       \retval Returns the database response
   */
   array|string createNode ( string $hostName, string $serviceName, string $databasePath, array|string $config ) ;

   /** \fn array|string start ( void )
       \brief start group
       \retval Returns the database response
   */
   array|string start ( void ) ;

   /** \fn array|string stop ( void )
       \brief stop group
       \retval Returns the database response
   */
   array|string stop ( void ) ;

   /** \fn bool isCatalog ( void )
       \brief judge group is catalog
       \retval Returns true or false
   */
   bool isCatalog ( void ) ;
};

/** \class SequoiaNode
 */
class SequoiaNode
{
public:

   /** \fn array|string stop ( void )
       \brief stop node
       \retval Returns the database response
   */
   array|string stop ( void ) ;

   /** \fn array|string start ( void )
       \brief start node
       \retval Returns the database response
   */
   array|string start ( void ) ;

   /** \fn string getNodeName ( void )
       \brief get node name
       \retval Returns the node name
   */
   string getNodeName ( void ) ;

   /** \fn string getServiceName ( void )
       \brief get node service name
       \retval Returns the node service name
   */
   string getServiceName ( void ) ;

   /** \fn string getHostName ( void )
       \brief get node host name
       \retval Returns the node host name
   */
   string getHostName ( void ) ;

   /** \fn int getStatus ( void )
       \brief get node status
       \retval Returns status
   */
   int getStatus ( void ) ;

   /** \fn SequoiaDB connect ( void )
       \brief connect the node
       \retval Returns the SequoiaDB object
   */
   SequoiaDB connect ( void ) ;
};


/** \class SequoiaID
 */
class SequoiaID
{
public:
   /** \fn __construct ( string $oid )
       \brief Creates a new id
       \param [in] $oid a string to uses as the id
       \retval Returns a new id
    */
   __construct ( string $oid ) ;

   /** \fn string __toString ( void )
       \brief Returns this objectID
       \retval Returns this objectID
   */
   string __toString ( void ) ;
} ;


/** \class SequoiaDate
 */
class SequoiaDate
{
public:
   /** \fn __construct ( string $date )
       \brief Creates a new date
       \param [in] $date a string to uses as the date
       \retval Returns this new date
   */
   __construct ( string $date ) ;
   
   /** \fn string __toString ( void )
       \brief Returns this date
       \retval Returns this date
   */
   string __toString ( void ) ;
} ;

/** \class SequoiaTimestamp
 */
class SequoiaTimestamp
{
public:
   /** \fn __construct ( string $timestamp )
       \brief Creates a new timestamp
       \param [in] $timestamp a string to uses as the timestamp
       \retval Returns this new timestamp
   */
   __construct ( string $timestamp ) ;
   
   /** \fn string __toString ( void )
       \brief Returns this timestamp
       \retval Returns this timestamp
   */
   string __toString ( void ) ;
} ;

/** \class SequoiaRegex
 */
class SequoiaRegex
{
public:
   /** \fn __construct ( string $regex )
       \brief Creates a new regex
       \param [in] $regex a string to uses as the regex
       \retval Returns this new regex
   */
   __construct ( string $regex ) ;
   
   /** \fn string __toString ( void )
       \brief Returns this regex
       \retval Returns this regex
   */
   string __toString ( void ) ;
} ;
