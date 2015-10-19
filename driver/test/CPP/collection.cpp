#include <stdio.h>
#include <gtest/gtest.h>
#include "client.hpp"
#include "testcommon.hpp"
#include <string>
#include <iostream>

using namespace std ;
using namespace sdbclient ;

#define INDEXNAMEDEF         "indexNameDef"

TEST(collection,getCount_without_condition)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   SINT64 count                             = 0 ;
   SINT64 NUM                               = 1000 ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = insertRecords ( cl, NUM ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cl.getCount( count ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   ASSERT_EQ( NUM, count ) ;
   cout<<"The total number of records is "<<count<<endl ;
   connection.disconnect() ;
}

TEST(collection,getCount_with_condition)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;

   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   SINT64 count                             = 0 ;
   BSONObj condition1 ;
   BSONObj condition2 ;
   BSONObj obj ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   condition1 = BSON ( "age" << 50 ) ;
   rc = cl.getCount( count, condition1 ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   cout<<"Condition1 is"<<condition1<<endl ;
   cout<<"The number of matched records is "<<count<<endl ;
   condition2 = BSON ( "age" << BSON ( "$gt" << 50 ) ) ;
   rc = cl.getCount( count, condition2 ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   cout<<"Condition2 is"<<condition2<<endl ;
   cout<<"The number of records is "<<count<<endl ;

   connection.disconnect() ;
}

TEST(collection,bulkInsert)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   SINT64 NUM                               = 10000 ;
   SINT64 totalNum                          = 0 ;
   int count                                = 0 ;
   vector<BSONObj> objList ;
   BSONObj obj ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cl.getCount (  totalNum ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   printf("Before bulk insert, the total number \
of records is %lld\n",totalNum ) ;
   printf( "Bulk insert records." OSS_NEWLINE ) ;
   for ( count = 0; count < NUM; count++ )
   {
      obj = BSON ( "firstName" << "John" <<
                   "lastName" << "Smith" <<
                   "age" << 50 ) ;
      objList.push_back ( obj ) ;
   }
   rc = cl.bulkInsert( 0, objList ) ;
   CHECK_MSG("%s%d\n", "rc = ", rc) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cl.getCount ( totalNum ) ;
   CHECK_MSG("%s%lld%s%lld%s%d\n", " NUM = ", NUM,
             " totalNum = ", totalNum, " rc = ", rc) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   ASSERT_EQ( NUM, totalNum ) ;
   printf("After bulk insert,the total number \
of records is %lld\n",totalNum ) ;

   connection.disconnect() ;
}

TEST(collection,bulkInsert_empty)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   vector<BSONObj> objList ;
   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cl.bulkInsert( 0, objList ) ;
   CHECK_MSG("%s%d\n", "rc = ", rc) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,insert_without_iterator)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   BSONObj obj ;

   rc = initEnv() ;
   CHECK_MSG( "%s%d\n", "rc = ", rc ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace ( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   createEnglishRecord( obj ) ;
   cout<<"the insert record is(notice the id):"<<endl;
   cout<<toJson(obj)<<endl;
   rc = cl.insert( obj ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,insert_Chinese_record)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;

   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   BSONObj obj ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace ( connection, COLLECTION_SPACE_NAME, cs ) ;
   rc = getCollection ( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   createChineseRecord( obj ) ;
   cout<<"the insert Chinese record is:"<<endl;
   cout << obj.toString () << endl ;
   rc = cl.insert( obj ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,update_condition_is_true)
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
   BSONElement ele ;
   BSONObj rule ;
   BSONObj obj ;
   BSONObjBuilder ob ;
   BSONObj updateCondition ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = insertRecords( cl, 10 ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   cl.query(cursor) ;
   rc = cursor.current( obj ) ;
   CHECK_MSG("%s%d","rc = ",rc) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   ele = obj.getField ( "_id" ) ;
   ASSERT_TRUE( !ele.eoo() ) ;
   ob.append ( ele ) ;
   updateCondition = ob.obj () ;
   rule = BSON ( "$set" << BSON ( "age" << 19 ) ) ;
   cout<<"The update rule is:"<<endl;
   cout << rule.toString() << endl ;
   rc = cl.update( rule, updateCondition ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,update_condition_is_false)
{

   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc = SDB_OK ;
   BSONElement ele ;
   BSONObj rule ;
   BSONObj updateCondition ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rule = BSON ( "$set" << BSON ( "age" << 19 ) ) ;
   cout<<"the update rule is:"<<endl;
   cout << rule.toString() << endl ;
   updateCondition = BSON ( "condition" << "the condition doesn't exist in any record" ) ;
   rc = cl.update( rule, updateCondition ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,upsert_condition_is_true)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc = SDB_OK ;
   BSONElement ele ;
   BSONObj rule ;
   BSONObj obj ;
   BSONObj updateCondition ;
   BSONObjBuilder ob ;
   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;

   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = insertRecords( cl, 10 ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   sdbCursor cursor ;
   cl.query(cursor) ;
   rc = cursor.current(obj) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   ele = obj.getField ( "_id" ) ;
   ASSERT_TRUE( !ele.eoo() ) ;
   ob.append ( ele ) ;
   updateCondition = ob.obj () ;
   rule = BSON ( "$set" << BSON ( "age" << 100 ) ) ;
   cout<<"the update rule is:"<<endl;
   cout << rule.toString() << endl ;
   rc = cl.upsert( rule, updateCondition ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,upsert_condition_is_false)
{

   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc = SDB_OK ;
   BSONElement ele ;
   BSONObj rule ;
   BSONObj obj ;
   BSONObj updateCondition ;

   rc = initEnv() ;

   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rule = BSON ( "$set" << BSON ( "age" << 19 ) ) ;
   cout<<"the update rule is:"<<endl;
   cout << rule.toString() << endl ;
   updateCondition = BSON ( "condition" << "the condition doesn't exist in any record" ) ;
   rc = cl.upsert( rule, updateCondition ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,del_without_condition)
{

   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc = SDB_OK ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;

   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cl.del() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,del_with_condition)
{

   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc = SDB_OK ;
   BSONObj condition ;
   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   condition  = BSON ( "age" << 50 ) ;
   rc = cl.del( condition ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,query)
{

   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc = SDB_OK ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   BSONObj condition ;
   sdbclient::sdb connection ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   sdbclient::sdbCollectionSpace cs ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   sdbclient::sdbCollection cl ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   sdbclient::sdbCursor cursor ;
   condition = BSON ( "age" << 50 ) ;
   rc = cl.query( cursor, condition  ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   connection.disconnect() ;
}

TEST(collection,createIndex)
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

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs );
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   obj = BSON ( "name" << 1 << "age" << -1 ) ;
   rc = cl.createIndex( obj, INDEXNAMEDEF, FALSE, FALSE ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   rc = cl.getIndexes( cursor, INDEX_NAME ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cursor.current( obj ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   printf( "After creating index ,the current index is:\n" ) ;
   cout << obj.toString() << endl ;

   connection.disconnect() ;
}

TEST(collection,getIndexes)
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

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs );
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   rc = cl.getIndexes( cursor, "$id" ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cursor.current( obj ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   printf( "The current index we get is:\n" ) ;
   cout << obj.toString() << endl ;

   connection.disconnect() ;
}

TEST(collection,dropIndex)
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

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs );
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   obj = BSON ( "name" << 1 << "age" << -1 ) ;
   rc = cl.createIndex( obj, INDEXNAMEDEF, FALSE, FALSE ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cl.dropIndex( INDEX_NAME ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cl.getIndexes( cursor, INDEX_NAME ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cursor.current( obj ) ;
   ASSERT_EQ( SDB_DMS_EOC, rc ) ;

   connection.disconnect() ;
}

TEST(collection,getCollectionName)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   const CHAR *clName                       = NULL ;
   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace ( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   clName = cl.getCollectionName() ;
   cout<<"The cl name is ："<<clName<<endl ;
   connection.disconnect() ;
}

TEST(collection,getCSName)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   const CHAR *csName                       = NULL ;
   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   csName = cl.getCSName() ;
   cout<<"The cs name is ："<<csName<<endl ;
   connection.disconnect() ;
}

TEST(collection,getFullName)
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   const CHAR *fullName                       = NULL ;
   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   fullName = cl.getFullName() ;
   cout<<"The full name is ："<<fullName<<endl ;
   connection.disconnect() ;
}

TEST(collection,aggregate)
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
   int iNUM = 2 ;
   int rNUM = 4 ;
   int i = 0 ;
   const char* command[iNUM] ;
   const char* record[rNUM] ;
   command[0] = "{$match:{status:\"A\"}}" ;
   command[1] = "{$group:{_id:\"$cust_id\",total:{$sum:\"$amount\"}}}" ;
   record[0] = "{cust_id:\"A123\",amount:500,status:\"A\"}" ;
   record[1] = "{cust_id:\"A123\",amount:250,status:\"A\"}" ;
   record[2] = "{cust_id:\"B212\",amount:200,status:\"A\"}" ;
   record[3] = "{cust_id:\"A123\",amount:300,status:\"D\"}" ;
   const char* m = "{$match:{status:\"A\"}}" ;
   const char* g = "{$group:{_id:\"$cust_id\",total:{$sum:\"$amount\"}}}" ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   for( i=0; i<rNUM; i++ )
   {
      rc = fromjson( record[i], obj ) ;
      ASSERT_EQ( SDB_OK, rc ) ;
      cout<<obj.toString()<<endl ;
      rc = cl.insert( obj ) ;
      ASSERT_EQ( SDB_OK, rc ) ;
   }
   for ( i=0; i<iNUM; i++ )
   {
      rc = fromjson( command[i], obj ) ;
      ASSERT_EQ( SDB_OK, rc ) ;
      cout<<obj.toString()<<endl ;
      ob.push_back( obj ) ;
   }
   rc = cl.aggregate( cursor, ob ) ;
   cout<<"rc is "<<rc<<endl ;
   displayRecord( cursor ) ;
   connection.disconnect() ;
}

TEST(collection, aggregate_2)
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

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   for( i=0; i<rNUM; i++ )
   {
      rc = fromjson( record[i], obj ) ;
      ASSERT_EQ( SDB_OK, rc ) ;
      cout<<obj.toString()<<endl ;
      rc = cl.insert( obj ) ;
      ASSERT_EQ( SDB_OK, rc ) ;
   }
   for ( i=0; i<iNUM; i++ )
   {
      rc = fromjson( command[i], obj ) ;
      ASSERT_EQ( SDB_OK, rc ) ;
      cout<<obj.toString()<<endl ;
      ob.push_back( obj ) ;
   }
   rc = cl.aggregate( cursor, ob ) ;
   cout<<"rc is "<<rc<<endl ;
   displayRecord( cursor ) ;
   connection.disconnect() ;
}


TEST( collection, getQueryMeta )
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   sdbCursor cursor ;
   sdbCursor datacursor ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   BSONObj obj ;
   BSONObj temp ;
   BSONObj empty ;
   BSONObj condition ;
   BSONObj orderBy ;
   BSONObj hint ;
   string st ;
   const char* st1 = "tbscan" ;
   const char* st2 = "ixscan" ;
   long i = 0 ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   condition = BSON( "age"<<BSON("$gt" << 0)<<"age"<<BSON( "$lt" << 100 ) ) ;
   cout<<"condition is: "<<condition.toString()<<endl ;
   int flag = 0 ;
   if( 0 == flag )
   {
      hint = BSON( "" << "ageIndex" ) ;
   }
   else
   {
      BSONObjBuilder ob1 ;
      ob1.appendNull ( "" ) ;
      hint = ob1.obj() ;
   }
   orderBy = BSON( "Indexblocks"<<1 ) ;
   rc = cl.getQueryMeta( cursor, condition, empty, hint, 0, -1 ) ;
   ASSERT_EQ( SDB_OK, rc ) ;

   while ( !( rc = cursor.next( obj ) ) )
   {
      cout<<obj.toString()<<endl ;
      BSONObjBuilder bo ;
      obj.getField( "ScanType" ).Val( st ) ;
      cout<<"ScanType is: "<<st.c_str()<<endl ;
      if ( !st.compare( st1 ) )
      {
         bo.appendAs( obj.getField( "Datablocks" ), "Datablocks" ) ;
      }
      else if ( !st.compare( st2 ) )
      {
         bo.appendAs( obj.getField( "Indexblocks" ), "Indexblocks" ) ;
      }
      else
      {
         cout<<"the \"ScanType\" is "<<st.c_str()
             <<",not \"tbscan\" or \"ixscan\"."<<endl ;
         ASSERT_EQ( 1, 0 ) ;
      }
      BSONObj hint = bo.obj() ;
      cout<<"hint is: "<<hint.toString()<<endl ;
      rc = cl.query( datacursor, empty, empty, empty, hint, 0, -1 ) ;
      ASSERT_EQ( SDB_OK, rc ) ;
      while ( !( rc = datacursor.next( temp ) ) )
      {
         i++ ;
      }
   }
   cout<<"the record number is: "<<i<<endl ;
   ASSERT_EQ( 1, 1 ) ;
   connection.disconnect() ;

}

TEST( collection, getQueryMeta_select_is_null )
{
   sdb connection ;
   sdbCollectionSpace cs ;
   sdbCollection cl ;
   sdbCursor cursor ;
   sdbCursor datacursor ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   BSONObj obj ;
   BSONObj temp ;
   BSONObj empty ;
   BSONObj condition ;
   BSONObj orderBy ;
   BSONObj hint ;
   BSONObjBuilder ob1 ;
   string st ;
   const char* st1 = "tbscan" ;
   const char* st2 = "ixscan" ;
   long i = 0 ;

   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( cs, COLLECTION_NAME, cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   condition = BSON( "age"<<BSON("$gt" << 50) ) ;
   cout<<"condition is: "<<condition.toString()<<endl ;
   int flag = 1 ;
   if( 0 == flag )
   {
      hint = BSON( "" << "ageIndex" ) ;
   }
   else
   {
      ob1.appendNull ( "" ) ;
      hint = ob1.obj() ;
   }
   orderBy = BSON( "Datablocks"<<1 ) ;
   rc = cl.getQueryMeta( cursor, condition, empty, hint, 0, -1 ) ;
   CHECK_MSG( "%s%d\n", "rc = ", rc ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   while ( !( rc = cursor.next( obj ) ) )
   {
      cout<<obj.toString()<<endl ;
      BSONObjBuilder bo ;
      obj.getField( "ScanType" ).Val( st ) ;
      cout<<"ScanType is: "<<st.c_str()<<endl ;
      if ( !st.compare( st1 ) )
      {
         bo.appendAs( obj.getField( "Datablocks" ), "Datablocks" ) ;
      }
      else if ( !st.compare( st2 ) )
      {
         bo.appendAs( obj.getField( "Indexblocks" ), "Indexblocks" ) ;
      }
      else
      {
         cout<<"the \"ScanType\" is "<<st.c_str()
             <<",not \"tbscan\" or \"ixscan\"."<<endl ;
         ASSERT_TRUE( 1 == 0 ) ;
      }
      BSONObj hint = bo.obj() ;
      cout<<"hint is: "<<hint.toString()<<endl ;
      rc = cl.query( datacursor, empty, empty, empty, hint, 0, -1 ) ;
      ASSERT_EQ( SDB_OK, rc ) ;
      while ( !( rc = datacursor.next( temp ) ) )
      {
         i++ ;
      }
   }
   cout<<"the record number is: "<<i<<endl ;
   ASSERT_TRUE( 1==1 ) ;
   connection.disconnect() ;

}

TEST(collection, attachCollection)
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
   INT32 NUM                                = 100 ;
   INT32 i                                  = 0 ;
   SINT64 count                             = 0 ;
   BSONObj obj ;
   rc = initEnv() ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = connection.getList( cursor, SDB_LIST_GROUPS ) ;
   if ( rc == SDB_RTN_COORD_ONLY )
   {
      cout << "attachCollection use \
in the cluser environment only" << endl ;
      ASSERT_EQ( SDB_RTN_COORD_ONLY, rc );
      return ;
   }
   rc = getCollectionSpace( connection, COLLECTION_SPACE_NAME, cs ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cs.createCollection( "main",
                             BSON("IsMainCL"<<true<<"ShardingKey"<<BSON("id"<<1)),
                             cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   obj =  BSON( "LowBound" << BSON("id"<<0) << "UpBound" << BSON("id"<<100) ) ;
   rc = cl.attachCollection ( COLLECTION_FULL_NAME, obj ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   for ( i=0; i < NUM; i++ )
   {
      rc = cl.insert(BSON("id"<<i)) ;
      ASSERT_EQ( SDB_OK, rc ) ;
   }
   INT32 cnt = 0 ;
   do
   {
      rc = cl.getCount ( count ) ;
      ++cnt ;
   }while( 100 != count && 100 > cnt ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   cout << "NUM is: " << NUM << "count is: " << count << endl ;
   ASSERT_EQ( NUM, count ) ;
   rc = cl.detachCollection ( COLLECTION_FULL_NAME ) ;
   CHECK_MSG("%s%d\n","rc = ",rc) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = cl.insert(BSON("id"<<101)) ;
   cout << "rc is: " << rc << endl ;
   ASSERT_EQ( SDB_CAT_NO_MATCH_CATALOG, rc ) ;
   connection.disconnect() ;
}



/* some doubtful test */
/*
TEST(collection,insert_with_iterator)
{
   sdb connection ;
   sdbCollection collection ;
   const CHAR *pHostName                    = HOST ;
   const CHAR *pPort                        = SERVER ;
   const CHAR *pUsr                         = USER ;
   const CHAR *pPasswd                      = PASSWD ;
   INT32 rc                                 = SDB_OK ;
   BSONElement ele ;
   BSONObj obj ;
   const char *key                          = NULL ;
   const char *val                          = NULL ;
   int value                                = 5 ;

   initEnv() ;
   rc = connection.connect( pHostName, pPort, pUsr, pPasswd ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection( connection, COLLECTION_FULL_NAME, collection ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   obj = BSON ( "a" << 1 ) ;
   cout<<"The insert record is :"<<endl;
   cout << obj.toString() << endl ;
   rc = collection.insert( obj, &ele ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   cout<<"Read the record by the iterator:"<<endl;

   connection.disconnect() ;
}

TEST(collection,create)
{
   ASSERT_TRUE( 0==0) ;
}

TEST(collection,drop)
{
   ASSERT_TRUE( 0==0) ;
}

*/

/*******************************************************************************
*@Description : query one testcase.[db.foo.bar.findOne()]
*@Modify List :
*               2014-10-24   xiaojun Hu   Init
*******************************************************************************/
TEST( collection, sdbCppQueryOne )
{
/*
   INT32 rc = SDB_OK ;

   sdbclient::sdb db ;
   rc = db.connect( HOST, SERVER, USER, PASSWD ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
*/
}
