/******************************************************************************
 *
 * Name: query.cpp
 * Description: This program demostrates how to connect to query from database.
 *
 * Parameters:
 *              HostName: The hostname for database server
 *              ServiceName: The service name or port number for the database
 *                           service
 *              Username: The user name for database server
 *              Password: The password  for user
 * Auto Compile:
 * Linux: ./buildApp.sh query
 * Win: buildApp.bat query
 * Manual Compile:
 *    Dynamic Linking:
 *    Linux:
 *       g++ query.cpp common.cpp -o query -I../../include \
 *       -L../../lib -lsdbcpp
 *    Win:
 *       cl /Foquery.obj /c query.cpp /I..\..\include /wd4047
 *       cl /Focommon.obj /c common.cpp /I..\..\include /wd4047
 *       link /OUT:query.exe /LIBPATH:..\..\lib sdbcpp.lib query.obj common.obj
 *       copy ..\..\lib\sdbcpp.dll .
 *    Static Linking:
 *    Linux: g++ query.cpp common.cpp -o query.static -I../../include -O0
 *           -ggdb -Wno-deprecated ../../lib/libstaticsdbcpp.a -lm -lpthread
 * Run:
 *    Linux: LD_LIBRARY_PATH=<path for libsdbcpp.so> ./query <hostname> \
 *           <servicename> <username> <password>
 *    Win: query.exe <hostname> <servicename> <username> <password>
 *
 ******************************************************************************/
#include <iostream>
#include "common.hpp"

using namespace std ;
using namespace sdbclient ;

#define NUM_RECORD 5

#define COLLECTION_SPACE_NAME "foo"
#define COLLECTION_NAME       "bar"

// Display Syntax Error
void displaySyntax ( CHAR *pCommand ) ;
// create record list
void createRecordList ( vector<BSONObj> &objlist, INT32 listSize ) ;

INT32 main ( INT32 argc, CHAR **argv )
{
   // verify syntax
   if ( 5 != argc )
   {
      displaySyntax ( (CHAR*)argv[0] ) ;
      exit ( 0 ) ;
   }
   // initialize local variables
   CHAR *pHostName    = (CHAR*)argv[1] ;
   CHAR *Port         = (CHAR*)argv[2] ;
   CHAR *pUsr         = (CHAR*)argv[3] ;
   CHAR *pPasswd      = (CHAR*)argv[4] ;

   // define a sdb object
   // use to connect to database
   sdb connection ;
   // define a sdbCollectionSpace object
   sdbCollectionSpace collectionspace ;
   // define a sdbCollection object
   sdbCollection collection ;
   // define a sdbCursor object for query
   sdbCursor cursor ;

   // define local variables
   // initialize them before use
   BSONObj obj ;
   BSONObj rule ;
   vector<BSONObj> objList ;
   int count = 0 ;
   INT32 rc = SDB_OK ;

   // connect to database
   rc = connection.connect ( pHostName, Port, pUsr, pPasswd ) ;
   if( rc!=SDB_OK )
   {
      cout<<"Failed to connet to database, rc = "<<rc<<endl ;
      goto error ;
   }

   // create collection space
   rc = connection.createCollectionSpace ( COLLECTION_SPACE_NAME,
                                   SDB_PAGESIZE_4K, collectionspace ) ;
   if( rc!=SDB_OK )
   {
      cout<<"Failed to create collection space, rc = "<<rc<<endl ;
      goto error ;
   }

   // create collection in a specified colletion space.
   // Here,we build it up in the new collection.
   rc = collectionspace.createCollection ( COLLECTION_NAME, collection ) ;
   if( rc!=SDB_OK )
   {
      cout<<"Failed to create collection, rc = "<<rc<<endl ;
      goto error ;
   }

   // create record list using objList
   createRecordList ( objList, NUM_RECORD ) ;
   // insert obj and free memory that allocated by createRecordList
   for ( count = 0; count < NUM_RECORD; count++ )
   {
      // all the contents of inserted record are the same except _id
      rc = collection.insert ( objList[count] ) ;
      if ( rc )
      {
         cout<<"Failed to insert record, rc = "<<rc<<endl ;
      }
   }

   // query all the record in this collection
   // and return the result by the cursor
   rc = collection.query ( cursor ) ;
   if( rc!=SDB_OK )
   {
      cout<<"Failed to query, rc = "<<rc<<endl ;
      goto error ;
   }
   // get all the qureied records
   while( !( rc=cursor.next( obj ) ) )
   {
      cout << obj.toString() << endl ;
   }
   if( rc==SDB_DMS_EOC )
   {
      cout<<"All the record had been list."<<endl ;
   }
   else if( rc!=SDB_OK )
   {
      cout<<"Failed to get the current record, rc = "<<rc<<endl ;
      goto error ;
   }

   // drop the specified collection
   rc = collectionspace.dropCollection( COLLECTION_NAME ) ;
   if( rc!=SDB_OK )
   {
      cout<<"Failed to drop the specified collection,rc = "<<rc<<endl ;
      goto error ;
   }

   // drop the specified collection space
   rc = connection.dropCollectionSpace( COLLECTION_SPACE_NAME ) ;
   if( rc!=SDB_OK )
   {
      cout<<"Failed to drop the specified collection space,rc = "<<rc<<endl ;
      goto error ;
   }

done:
   // disconnect from database
   connection.disconnect () ;
   return 0;
error:
   goto done ;
}

// Display Syntax Error
void displaySyntax ( CHAR *pCommand )
{
   cout<<"Syntax:"<<pCommand<<" <hostname> <servicename>\
 <username> <password>"<<endl ;
}
