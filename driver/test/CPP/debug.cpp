#include <stdio.h>
#include <gtest/gtest.h>
#include "client.hpp"
#include "testcommon.hpp"
#include <string>
#include <iostream>

using namespace std ;
using namespace sdbclient ;

#define USERNAME               "SequoiaDB"
#define PASSWORD               "SequoiaDB"

#define NUM_RECORD             5

TEST ( debug, test )
{
   ASSERT_TRUE( 1 == 1 ) ;
}

/*
TEST ( debug, date )
{
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;

   sdb db ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;

   INT32 rc = SDB_OK ;
   BSONObj obj ;
   BSONObjBuilder ob ;
   time_t s = 1379404680 ;
   unsigned long long millis = s*1000 ;
   Date_t date ( millis ) ;
   cout<<"date is: "<<date.toString ()<<endl ;
   ob.appendDate ( "date", date ) ;
   obj = ob.obj () ;
   cout<<obj.toString ()<<endl ;
   ASSERT_TRUE ( obj.toString()=="{ \"date\": {\"$date\": \"2013-09-17\"} }" ) ;
   rc = db.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollectionSpace( db, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = cl.insert( obj, NULL ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   db.disconnect() ;

}


TEST ( debug, time_stamp )
{
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;

   sdb db ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;

   INT32 rc = SDB_OK ;

   BSONObj obj ;
   BSONObjBuilder ob ;
   BSONObjBuilder ob1 ;
   ob.appendTimestamp ( "timestamp", 1379323441000, 0 ) ;
   obj = ob.obj () ;
   cout<<"timestamp1 is: "<<obj.toString()<<endl ;
   ASSERT_TRUE ( obj.toString() == "{ \"timestamp\": {\"$timestamp\": \
\"2013-09-16-17.24.01.000000\"} }" ) ;

   ob1.appendTimestamp ( "timestamp", 1379323441000, 1 ) ;
   obj = ob1.obj () ;
   cout<<"timestamp2 is: "<<obj.toString()<<endl ;
   ASSERT_TRUE ( obj.toString() == "{ \"timestamp\": {\"$timestamp\": \
\"2013-09-16-17.24.01.000001\"} }" ) ;

   rc = db.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollectionSpace( db, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = cl.insert( obj, NULL ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   db.disconnect() ;

}

*/



/*
TEST( debug, SdbIsValid )
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   sdbCursor cursor ;
   sdbCursor cursor1 ;
   sdbCursor cursor2 ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   BOOLEAN result = FALSE ;
   rc = initEnv() ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   rc = connection.isValid( &result ) ;
   CHECK_MSG( "%s%d\n", "rc = ", rc ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   std::cout<< "before close connection, result is " << result << endl ;
   ASSERT_TRUE( result == TRUE ) ;
   result = TRUE ;
   rc = connection.isValid( &result ) ;
   CHECK_MSG( "%s%d\n", "rc = ", rc ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   std::cout<< "after close connection manually, result is " << result << endl ;
   ASSERT_TRUE( result == TRUE ) ;
   connection.disconnect() ;
   result = FALSE ;
   rc = connection.isValid( &result ) ;
   CHECK_MSG( "%s%d\n", "rc = ", rc ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   std::cout<< "after close connection, result is " << result << endl ;
   ASSERT_TRUE( result == FALSE ) ;
}


TEST( debug, connect )
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCursor cursor ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   const CHAR* connArr[10] = {"192.168.20.35:12340",
                              "192.168.20.36:12340",
                              "123:123",
                              "",
                              ":12340",
                              "192.168.20.40",
                              "localhost:50000",
                              "192.168.20.40:12340",
                              "localhost:12340",
                              "localhost:11810"} ;
   rc = connection.connect( connArr, 10, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = connection.getList( cursor, 5);
   ASSERT_TRUE( rc==SDB_OK ) ;
   connection.disconnect() ;
}
*/
/*
TEST( debug, cl_alter )
{
   sdb db ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   sdbCursor cursor ;
   const CHAR* pCS = "cs_cl_alter" ;
   const CHAR* pCL = "cl_cl_alter" ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;

   rc = db.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   BSONObj cs_opt ;
   rc = db.createCollectionSpace( pCS, cs_opt, cs ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   BSONObj cl_opt ;
   rc = cs.createCollection( pCL, cl_opt, cl ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   BSONObj cl_opt2 = BSON( "ShardingKey" << BSON( "b" << 1 ) << "ShardingType" <<
                           "hash" << "ReplSize" << 1 << "Partition" << 1024 ) ;
   rc = cl.alterCollection( cl_opt2 ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;

   rc = db.dropCollectionSpace( pCS ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   db.disconnect() ;

}

TEST(debug,connect_with_serval_addr)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCursor cursor ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   const CHAR* connArr[10] = {"192.168.20.35:12340",
                              "192.168.20.36:12340",
                              "123:123",
                              "",
                              ":12340",
                              "192.168.20.40",
                              "localhost:50000",
                              "192.168.20.40:12340",
                              "localhost:12340",
                              "localhost:11810"} ;
   rc = connection.connect( connArr, 10, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = connection.getList( cursor, 5);
   ASSERT_TRUE( rc==SDB_OK ) ;
   connection.disconnect() ;
}
*/


/*
TEST(debug,next_current)
{
   sdb connection ;
   sdbCollection cl ;
   sdbCollectionSpace cs ;
   sdbCursor cursor ;
   sdbCursor cursor2 ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   BSONObj obj ;

   rc = initEnv() ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = insertRecords ( cl, 1 ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = cl.query( cursor ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = connection.getSnapshot( cursor2, 5 ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;

   rc = cursor.next( obj ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   cout<<"The next record is:"<<endl;
   cout<< obj.toString() << endl ;
   rc = cursor.current( obj ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   cout<<"The current record is:"<<endl;
   cout<< obj.toString() << endl ;

   rc = cursor2.next( obj ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   cout<<"The next record is:"<<endl;
   cout<< obj.toString() << endl ;
   rc = cursor2.current( obj ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   cout<<"The current record is:"<<endl;
   cout<< obj.toString() << endl ;
   connection.disconnect() ;
}
*/
/*
TEST(debug, aggregate)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   sdbCursor cursor ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   BSONObj obj ;
   vector<BSONObj> ob ;
   int iNUM = 5 ;
   int rNUM = 5 ;
   int i = 0 ;
   const char* command[iNUM] ;
   const char* record[rNUM] ;
   command[0] = "{\"$match\":{\"interest\":{\"$exists\":1}}}" ;
   command[1] = "{\"$group\":{\"_id\":\"$major\",\"avg_age\":{\"$avg\":\"$info.age\"},\"major\":{\"$first\":\"$major\"}}}" ;
   command[2] = "{\"$sort\":{\"avg_age\":-1,\"major\":1}}" ;
   command[3] = "{\"$skip\":0}" ;
   command[4] = "{\"$limit\":5}" ;

   record[0] = "{\"no\":1000,\"score\":80,\"interest\":[\"basketball\",\"football\"],\"major\":\"computer th\",\"dep\":\"computer\",\"info\":{\"name\":\"tom\",\"age\":25,\"gender\":\"man\"}}" ;
   record[1] = "{\"no\":1001,\"score\":90,\"interest\":[\"basketball\",\"football\"],\"major\":\"computer sc\",\"dep\":\"computer\",\"info\":{\"name\":\"mike\",\"age\":24,\"gender\":\"lady\"}}" ;
   record[2] = "{\"no\":1002,\"score\":85,\"interest\":[\"basketball\",\"football\"],\"major\":\"computer en\",\"dep\":\"computer\",\"info\":{\"name\":\"kkk\",\"age\":25,\"gender\":\"man\"}}" ;
   record[3] = "{\"no\":1003,\"score\":92,\"interest\":[\"basketball\",\"football\"],\"major\":\"computer en\",\"dep\":\"computer\",\"info\":{\"name\":\"mmm\",\"age\":25,\"gender\":\"man\"}}" ;
   record[4] = "{\"no\":1004,\"score\":88,\"interest\":[\"basketball\",\"football\"],\"major\":\"computer sc\",\"dep\":\"computer\",\"info\":{\"name\":\"ttt\",\"age\":25,\"gender\":\"man\"}}" ;
   const char* m = "{$match:{status:\"A\"}}" ;
   const char* g = "{$group:{_id:\"$cust_id\",total:{$sum:\"$amount\"}}}" ;

   rc = initEnv() ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;

   for( i=0; i<rNUM; i++ )
   {
      rc = fromjson( record[i], obj ) ;
      ASSERT_TRUE( rc == SDB_OK ) ;
      cout<<obj.toString()<<endl ;
      rc = cl.insert( obj ) ;
      ASSERT_TRUE( rc == SDB_OK ) ;
   }
   for ( i=0; i<iNUM; i++ )
   {
      rc = fromjson( command[i], obj ) ;
      ASSERT_TRUE( rc == SDB_OK ) ;
      cout<<obj.toString()<<endl ;
      ob.push_back( obj ) ;
   }
   rc = cl.aggregate( cursor, ob ) ;
   cout<<"rc is "<<rc<<endl ;
   displayRecord( cursor ) ;
   connection.disconnect() ;
}
*/

/*
TEST(debug, connect)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   const CHAR* host                = "localhost" ;
   const CHAR* port                = "11810" ;
   const CHAR* username            = "" ;
   const CHAR* password            = "" ;
   const CHAR* user                = "" ;
   const CHAR* passwd              = "" ;
   INT32 rc                        = SDB_OK ;

   rc = connection.connect( host, port, username, password ) ;
   ASSERT_TRUE( rc==SDB_OK ) << "Failed to connect to sdb, rc = " << rc ;
*/
/*
   rc = connection.createUsr( user, passwd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;

   rc = connection.removeUsr( user, passwd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
*/
/*
   connection.disconnect() ;
}

*/

/*
TEST(debug, explain)
{
   sdb db ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR* host                = "192.168.20.181" ;
   const CHAR* port                = "11810" ;
   const CHAR* username            = "" ;
   const CHAR* password            = "" ;
   const CHAR* user                = "" ;
   const CHAR* passwd              = "" ;
   INT32 rc                        = SDB_OK ;

   rc = db.connect( host, port, username, password ) ;
   ASSERT_TRUE( rc==SDB_OK ) << "Failed to connect to sdb, rc = " << rc ;

   rc = getCollectionSpace( db, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = cl.del() ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   for ( INT32 i = 0; i < 100; i++ )
   {
      BSONObj obj = BSON ( "firstName" << "John" <<
                   "lastName" << "Smith" <<
                   "age" << i ) ; 
      rc = cl.insert( obj ) ;
      ASSERT_TRUE( rc==SDB_OK ) ;
   }
   const CHAR *pIndex = "explain_index" ;
   BSONObj index = BSON( "age" << 1 ) ;
   rc = cl.createIndex( index, pIndex, FALSE, FALSE ) ;
   BSONObj condition = BSON( "age" << BSON( "$gt" << 50 ) ) ;
   BSONObj select    = BSON( "age" << "" ) ;
   BSONObj orderBy   = BSON( "age" << -1 ) ;
   BSONObj hint      = BSON( "" << pIndex ) ;

   BSONObjBuilder bob ;
   bob.appendBool( "Run", TRUE ) ;
   BSONObj option = bob.obj() ;  
   sdbCursor cursor ;
   rc = cl.explain( cursor, condition, select, orderBy, hint, 47, 3, 0, option ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   displayRecord( cursor ) ;  

   
   db.disconnect() ;
}
*/

TEST(debug, queryone)
{

   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc = SDB_OK ;
   const CHAR *pIndex = "queryone_index" ;

   rc = initEnv() ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   sdbclient::sdb db ;
   rc = db.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   sdbclient::sdbCollectionSpace cs ;
   rc = getCollectionSpace( db, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   sdbclient::sdbCollection cl ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   rc = cl.del() ;
   BSONObj index = BSON( "age" << 1 ) ;
   rc = cl.createIndex( index, pIndex, FALSE, FALSE ) ;
   for ( INT32 i = 0; i < 200; i++ )
   {
      BSONObj obj = BSON ( "firstName" << "John" <<
                   "lastName" << "Smith" <<
                   "age" << i ) ;
      rc = cl.insert( obj ) ;
      ASSERT_TRUE( rc==SDB_OK ) ;
   }
   sdbclient::sdbCursor cursor ;
   BSONObj condition = BSON( "age" << BSON( "$gt" << 50 ) ) ;
   BSONObj select    = BSON( "age" << "" ) ;
   BSONObj orderBy   = BSON( "age" << -1 ) ;
   BSONObj hint      = BSON( "" << pIndex ) ;
   BSONObj record ;
   INT32 count = 0 ;
   rc = cl.query( cursor, condition, select, orderBy, hint, 46, 3, 0 ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   cout << "case 1: general dispaly: " << endl ;
   count = 0 ;
   while ( 0 == (rc = cursor.next(record)) )
   {
      count++ ;
   }
   cout << "count is: " << count << endl ;
   ASSERT_TRUE( 3 == count ) ;
   
   rc = cl.query( cursor, condition, select, orderBy, hint, 46, 3, 0x00000200 ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   cout << "case 2: use flag dispaly: " << endl ;
   count = 0 ;
   while ( 0 == (rc = cursor.next(record)) )
   {
      count++ ;
   }
   cout << "count is: " << count << endl ;
   ASSERT_TRUE( 3 == count ) ;
   

   rc = cl.query( cursor, condition, select, orderBy, hint, 46, 1 ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   cout << "case 3: return 1 display: " << endl ;
   count = 0 ;
   while ( 0 == (rc = cursor.next(record)) )
   {
      count++ ;
   }
   cout << "count is: " << count << endl ;
   ASSERT_TRUE( 1 == count ) ;

   rc = cl.query( cursor, condition, select, orderBy, hint, 0, -1, 0x00000200 ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_TRUE( rc==SDB_OK ) ;
   cout << "case 4: return all, user flag display: " << endl ;
   count = 0 ;
   while ( 0 == (rc = cursor.next(record)) )
   {
      count++ ;
   }
   cout << "count is: " << count << endl ;
   ASSERT_TRUE( 149 == count ) ;


   db.disconnect() ;
}

