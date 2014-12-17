// Global Constants
var SDB_PAGESIZE_4K              = 4096 ;
var SDB_PAGESIZE_8K              = 8192 ;
var SDB_PAGESIZE_16K             = 16384 ;
var SDB_PAGESIZE_32K             = 32768 ;
var SDB_PAGESIZE_64K             = 65536 ;
var SDB_PAGESIZE_DEFAULT         = SDB_PAGESIZE_64K ;

var SDB_SNAP_CONTEXTS            = 0 ;
var SDB_SNAP_CONTEXTS_CURRENT    = 1 ;
var SDB_SNAP_SESSIONS            = 2 ;
var SDB_SNAP_SESSIONS_CURRENT    = 3 ;
var SDB_SNAP_COLLECTIONS         = 4 ;
var SDB_SNAP_COLLECTIONSPACES    = 5 ;
var SDB_SNAP_DATABASE            = 6 ;
var SDB_SNAP_SYSTEM              = 7 ;
var SDB_SNAP_CATALOG             = 8 ;

var SDB_LIST_CONTEXTS            = 0 ;
var SDB_LIST_CONTEXTS_CURRENT    = 1 ;
var SDB_LIST_SESSIONS            = 2 ;
var SDB_LIST_SESSIONS_CURRENT    = 3 ;
var SDB_LIST_COLLECTIONS         = 4 ;
var SDB_LIST_COLLECTIONSPACES    = 5 ;
var SDB_LIST_STORAGEUNITS        = 6 ;
var SDB_LIST_GROUPS              = 7 ;
var SDB_LIST_STOREPROCEDURES     = 8 ;
var SDB_LIST_DOMAINS             = 9 ;
var SDB_LIST_TASKS               = 10 ;

var SDB_INSERT_CONTONDUP         = 1 ;
var SDB_INSERT_RETURN_ID         = 2 ; // only available when inserting only one document

var SDB_TRACE_FLW                = 0 ;
var SDB_TRACE_FMT                = 1 ;

var SDB_COORD_GROUP_NAME         = "SYSCoord" ;
var SDB_CATALOG_GROUP_NAME       = "SYSCatalogGroup" ;

// end Global Constants

// Global functions
function println ( val ) {
   if ( arguments.length > 0 )
      print ( val ) ;
   print ( '\n' ) ;
}
// return a double number between 0 and 1
function rand () {
   return Math.random() ;
}

// end Global functions

// Bson
Bson.prototype.toObj = function() {
   return JSON.parse( this.toJson() );
}

Bson.prototype.toString = function() {
   try
   {
      var obj = this.toObj();
      var str = JSON.stringify ( obj, undefined, 2 ) ;
      return str ;
   }
   catch ( e )
   {
      return this.toJson() ;
   }
}
// end Bson

// SdbCursor
SdbCursor.prototype.toArray = function() {
   if ( this._arr )
      return this._arr;

   var a = [];
   while ( true ) {
      var bs = this.next();
      if ( ! bs ) break ;
      var json = bs.toJson () ;
      try
      {
         var stf = JSON.stringify(JSON.parse(json), undefined, 2) ;
         a.push ( stf ) ;
      }
      catch ( e )
      {
         a.push ( json ) ;
      }
   }
   this._arr = a ;
   return this._arr ;
}

SdbCursor.prototype.arrayAccess = function( idx ) {
   return this.toArray()[idx] ;
}

SdbCursor.prototype.size = function() {
   //return this.toArray().length ;
   var cursor = this ;
   var size = 0 ;
   var record = undefined ;
   while( ( record = cursor.next() ) != undefined )
   {
      size++ ;
   }
   return size ;
}

SdbCursor.prototype.toString = function() {
   //return this.toArray().join('\n') ;
   var csr = this ;
   var record = undefined ;
   var returnRecordNum = 0 ;
   while ( ( record = csr.next() ) != undefined )
   {
      returnRecordNum++ ;
      try
      {
         println ( record ) ;
      }
      catch ( e )
      {
         var json = record.toJson () ;
         println ( json ) ;
      }
   }
   println("Return "+returnRecordNum+" row(s).") ;
   return "" ;
}
// end SdbCursor


// CLCount
CLCount.prototype.toString = function() {
   this._exec() ;
   return this._count ;
}
CLCount.prototype.hint = function( hint ) {
   this._hint = hint ;
   return this ;
}
CLCount.prototype._exec = function() {
   this._count = this._collection._count ( this._condition,
                                           this._hint ) ;
}
// end CLCount

// SdbCollection
SdbCollection.prototype.count = function( condition ) {
   return new CLCount( this, condition ) ;
}

SdbCollection.prototype.find = function( query, select ) {
   return new SdbQuery( this , query, select );
}

SdbCollection.prototype.findOne = function( query, select ) {
   return new SdbQuery( this , query, select ).limit( 1 ) ;
}

SdbCollection.prototype.getIndex = function( name ) {
   if ( ! name )
      throw "SdbCollection.getIndex(): 1st parameter should be valid string" ;
   return this._getIndexes( name ).next() ;
}

SdbCollection.prototype.listIndexes = function() {
   return this._getIndexes() ;
}

SdbCollection.prototype.toString = function() {
   return this._cs.toString() + "." + this._name;
}

SdbCollection.prototype.insert = function ( data , flags )
{
   if ( (typeof data) != "object" )
      throw "SdbCollection.insert(): the 1st param should be obj or array of objs";
   var newFlags = 0 ;
   if ( flags != undefined )
   {
      if ( (typeof flags) != "number" ||
            ( flags != 0 &&
              flags != SDB_INSERT_RETURN_ID &&
              flags != SDB_INSERT_CONTONDUP ) )
         throw "SdbCollection.insert(): the 2nd param if existed should be 0 or SDB_INSERT_RETURN_ID or SDB_INSERT_CONTONDUP only";
      newFlags = flags ;
   }

   if ( data instanceof Array )
   {
      if ( 0 == data.length ) return ;
      if ( newFlags != 0 && newFlags != SDB_INSERT_CONTONDUP )
         throw "SdbCollection.insert(): when insert more than 1 records, the 2nd param if existed should be 0 or SDB_INSERT_CONTONDUP only";
      return this._bulkInsert ( data , newFlags ) ;
   }
   else
   {
      if ( newFlags != 0 && newFlags != SDB_INSERT_RETURN_ID )
         throw "SdbCollection.insert(): when insert 1 record, the 2nd param if existed should be 0 or SDB_INSERT_RETURN_ID only";
      return this._insert ( data , SDB_INSERT_RETURN_ID == flags ) ;
   }
}

/*
SdbCollection.prototype.rename = function ( newName ) {
   this._rename ( newName ) ;
   this._name = newName ;
}
*/
// end SdbCollection

// SdbQuery
SdbQuery.prototype._checkExecuted = function() {
   if ( this._cursor )
      throw "query already executed";
}

SdbQuery.prototype._exec = function() {
   if ( ! this._cursor ) {
      this._cursor = this._collection.rawFind( this._query,
                                               this._select,
                                               this._sort,
                                               this._hint,
                                               this._skip,
                                               this._limit,
                                               this._flags );
   }
   return this._cursor;
}

SdbQuery.prototype.sort = function( sort ) {
   this._checkExecuted();
   this._sort = sort;
   return this;
}

SdbQuery.prototype.hint = function( hint ) {
   this._checkExecuted();
   this._hint = hint;
   return this;
}

SdbQuery.prototype.skip = function( skip ) {
   this._checkExecuted();
   this._skip = skip;
   return this;
}

SdbQuery.prototype.limit = function( limit ) {
   this._checkExecuted();
   this._limit = limit;
   return this;
}

SdbQuery.prototype.flags = function( flags ) {
   this._checkExecuted();
   this._flags = flags;
   return this;
}

SdbQuery.prototype.next = function() {
   this._exec();
   return this._cursor.next();
}

SdbQuery.prototype.current = function() {
   this._exec();
   return this._cursor.current();
}

SdbQuery.prototype.close = function() {
   this._exec();
   return this._cursor.close();
}

/*
SdbQuery.prototype.updateCurrent = function ( rule ) {
   this._exec();
   return this._cursor.updateCurrent( rule ) ;
}

SdbQuery.prototype.deleteCurrent = function () {
   this._exec();
   return this._cursor.deleteCurrent();
}
*/
SdbQuery.prototype.toArray = function() {
   this._exec();
   return this._cursor.toArray();
}

SdbQuery.prototype.arrayAccess = function( idx ) {
   return this.toArray()[idx];
}

SdbQuery.prototype.count = function() {
   return this._collection.count( this._query ) ;
}

SdbQuery.prototype.explain = function( options ) {
   return this._collection.explain( this._query,
                                    this._select,
                                    this._sort,
                                    this._hint,
                                    this._skip,
                                    this._limit,
                                    options ) ;
}

SdbQuery.prototype.size = function() {
//   return this.toArray().length;
   this._exec();
   return this._cursor.size() ;
}

SdbQuery.prototype.toString = function() {
   this._exec();
   var csr = this._cursor ;
   var record = undefined ;
   var returnRecordNum = 0 ;
   while ( ( record = csr.next() ) != undefined )
   {
      returnRecordNum++ ;
      try
      {
         println ( record ) ;
      }
      catch ( e )
      {
         var json = record.toJson () ;
         println ( json ) ;
      }
   }
   println("Return "+returnRecordNum+" row(s).") ;
   return "" ;
   //return this._cursor.toString();
}
// end SdbQuery

// SdbNode
SdbNode.prototype.toString = function() {
   return this._hostname + ":" +
          this._servicename ;
}

SdbNode.prototype.getHostName = function() {
   return this._hostname ;
}

SdbNode.prototype.getServiceName = function() {
   return this._servicename ;
}

SdbNode.prototype.getNodeDetail = function() {
   return this._nodeid + ":" + this._hostname + ":" +
          this._servicename + "(" +
          this._rg.toString() + ")" ;
}
// end SdbNode

// SdbReplicaGroup
SdbReplicaGroup.prototype.toString = function() {
   return this._name;
}

SdbReplicaGroup.prototype.getDetail = function() {
   return this._conn.list( SDB_LIST_GROUPS,
                           {GroupName: this._name } ) ;
}
// end SdbReplicaGroup

// SdbCS
SdbCS.prototype.toString = function() {
   return this._conn.toString() + "." + this._name;
}

SdbCS.prototype._resolveCL = function(clName) {
   this.getCL(clName) ;
}
// end SdbCS


// SdbDomain
SdbDomain.prototype.toString = function() {
   return this._domainname ;
}
// end SdbDomain

// Sdb
Sdb.prototype.toString = function() {
   return this._host + ":" + this._port;
}

Sdb.prototype.listCollectionSpaces = function() {
   return this.list( SDB_LIST_COLLECTIONSPACES ) ;
}

Sdb.prototype.listCollections = function() {
   return this.list( SDB_LIST_COLLECTIONS ) ;
}

Sdb.prototype.listReplicaGroups = function() {
   return this.list( SDB_LIST_GROUPS ) ;
}

Sdb.prototype._resolveCS = function(csName) {
   this.getCS( csName ) ;
}

Sdb.prototype.getCatalogRG = function() {
   return this.getRG( SDB_CATALOG_GROUP_NAME ) ;
}

Sdb.prototype.removeCatalogRG = function() {
   return this.removeRG( SDB_CATALOG_GROUP_NAME ) ;
}

Sdb.prototype.createCoordRG = function() {
   return this.createRG( SDB_COORD_GROUP_NAME ) ;
}

Sdb.prototype.removeCoordRG = function() {
   return this.removeRG( SDB_COORD_GROUP_NAME ) ;
}

Sdb.prototype.getCoordRG = function() {
   return this.getRG( SDB_COORD_GROUP_NAME ) ;
}

// end Sdb

function printCallStack()
{
   try
   {
      throw new Error( "print ErrStack" ) ;
   }
   catch ( e )
   {
      print( e.stack ) ;
   }
}

function assert( condition )
{
   if ( !condition )
   {
      printCallStack() ;
   }
}

// end Sdb

