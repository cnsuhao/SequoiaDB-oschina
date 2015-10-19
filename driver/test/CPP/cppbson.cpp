#include <stdio.h>
#include <gtest/gtest.h>
#include "client.hpp"
#include "testcommon.hpp"
#include <string>
#include <iostream>

using namespace std ;
using namespace bson ;

TEST ( cpp_bson_base_type, int )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   BSONObj obj1 ;
   obj = BSON( "a"<<123 ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"a\": 123 }" ) ;

   ob.append ( "b",456 ) ;
   obj1 = ob.obj () ;
   cout<<obj1.toString()<<endl ;
   ASSERT_TRUE( obj1.toString() == "{ \"b\": 456 }" ) ;
}

TEST ( cpp_bson_base_type, long )
{
   BSONObjBuilder ob1 ;
   BSONObjBuilder ob2 ;
   BSONObj obj ;
   int num1 = 2147483647 ;
   int num2 = -2147483648 ;

   obj = BSON( "positive_long"<<2147483647) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"positive_long\": 2147483647 }" ) ;
   obj = BSON( "negative_long"<<(int)-2147483648 ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"negative_long\": -2147483648 }" ) ;

   ob1.append ( "positive_long", num1 ) ;
   obj = ob1.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"positive_long\": 2147483647 }" ) ;
   ob2.append ( "negative_long", num2 ) ;
   obj = ob2.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"negative_long\": -2147483648 }" ) ;
}

TEST ( cpp_bson_base_type, long_long )
{
   BSONObjBuilder ob ;
   BSONObjBuilder ob1 ;
   BSONObj obj ;
#if defined (_WIN32)
   long long num1 = -9223372036854775808LL ;
   long long num2 = 9223372036854775807LL ;
#else
   long long num1 = -9223372036854775808ll ;
   long long num2 = 9223372036854775807ll ;
#endif

   obj = BSON( "negative_long_long"<<(long long )-9223372036854775808ll ) ;
   ASSERT_TRUE( obj.toString() == "{ \"negative_long_long\": -9223372036854775808 }" );
   obj = BSON( "positive_long_long"<<9223372036854775807ll ) ;
   cout<<"num2 is: "<<num2<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"positive_long_long\": 9223372036854775807 }" );

   ob.append ( "negative_long_long", num1 ) ;
   obj = ob.obj() ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"negative_long_long\": -9223372036854775808 }") ;

   ob1.append ( "positive_long_long", num2 ) ;
   obj = ob1.obj() ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"positive_long_long\": 9223372036854775807 }" );
}

TEST ( cpp_bson_base_type, float )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   obj = BSON( "b"<<3.14159265359 ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"b\": 3.14159265359 }" ) ;

   ob.append ( "pi", 3.14159265359 ) ;
   obj = ob.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"pi\": 3.14159265359 }" ) ;
}

TEST ( cpp_bson_base_type, string )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   obj = BSON( "foo"<<"bar" ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"foo\": \"bar\" }" ) ;

   ob.append ( "abc", "def" ) ;
   obj = ob.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"abc\": \"def\" }" ) ;
}

TEST ( cpp_bson_base_type, OID )
{
   BSONObjBuilder ob ;
   BSONObjBuilder ob1 ;
   BSONObjBuilder ob2 ;
   BSONObj obj ;
   obj = BSON( GENOID ) ;
   cout<<obj.toString()<<endl ;

   OID oid = OID::gen() ;
   ob.appendOID ( "OID", &oid ) ;
   obj = ob.obj() ;
   cout<<obj.toString()<<endl ;

   OID oid1 = OID::gen() ;
   ob1.appendOID ( "OID", &oid1 ) ;
   obj = ob1.obj() ;
   cout<<obj.toString()<<endl ;

   OID oid2 ;
   memset(&oid2, 0xFF, sizeof(oid2));
   ob2.appendOID( "OID", &oid2, true );
   obj = ob2.obj() ;
   cout<<obj.toString()<<endl ;
}

TEST ( cpp_bson_base_type, bool )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   obj = BSON( "flag"<<true<<"ret"<<false ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"flag\": true, \"ret\": false }" ) ;

   BSONObjBuilder ob1 ;
   ob1.appendBool ( "bool", 1 ) ;
   obj = ob1.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"bool\": true }" ) ;

   BSONObjBuilder ob2 ;
   ob2.appendBool ( "bool", 0 ) ;
   obj = ob2.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"bool\": false }" ) ;
}

TEST ( cpp_bson_base_type, date )
{
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

}

TEST ( cpp_bson_base_type, timestamp )
{
   BSONObj obj ;
   BSONObjBuilder ob ;
   BSONObjBuilder ob1 ;
   ob.appendTimestamp ( "timestamp", 1379323441000, 0 ) ;
   obj = ob.obj () ;
   cout<<"timestamp1 is: "<<obj.toString()<<endl ;
   cout<<"obj is: "<<obj.getOwned()<<endl ;

   ASSERT_TRUE ( obj.toString() == "{ \"timestamp\": {\"$timestamp\": \"2013-09-16-17.24.01.000000\"} }" ) ;

   ob1.appendTimestamp ( "timestamp", 1379323441000, 1 ) ;
   obj = ob1.obj () ;
   cout<<"timestamp2 is: "<<obj.toString()<<endl ;
   ASSERT_TRUE ( obj.toString() == "{ \"timestamp\": {\"$timestamp\": \"2013-09-16-17.24.01.000001\"} }" ) ;
}

TEST ( cpp_bson_base_type, binary )
{
   int rc = 0 ;
   BSONObj obj ;
   BSONObj temp ;
   BSONObjBuilder ob1 ;
   BSONObjBuilder ob2 ;
   BSONObjBuilder ob3 ;
   BSONObjBuilder ob4 ;
   BSONObjBuilder ob5 ;
   BSONObjBuilder ob6 ;

   const char *str = "hello world" ;
   const char *str2 = "{ \"key\": { \"$binary\" : \"aGVsbG8gd29ybGQ=\", \"$type\": \"1\" } }" ;

   rc = fromjson( str2, temp ) ;
   ASSERT_TRUE ( rc == SDB_OK ) ;
   cout<<temp.toString(false,true)<<endl ;
   ASSERT_TRUE ( temp.toString(false,true) == "{ \"key\": { \"$binary\": \"aGVsbG8gd29ybGQ=\", \"$type\": \"1\" } }" ) ;

   ob6.appendBinData ( "binaryData", strlen(str),
                       BinDataGeneral, str ) ;
   obj = ob6.obj () ;
   cout<<obj.toString(false,true)<<endl ;
   ASSERT_TRUE ( obj.toString(false,true) == "{ \"binaryData\": { \"$binary\": \"aGVsbG8gd29ybGQ=\", \"$type\": \"0\" } }" ) ;
   ob1.appendBinData ( "binaryData", strlen(str),
                       Function, str ) ;
   obj = ob1.obj () ;
   cout<<obj.toString(false,true)<<endl ;
   ASSERT_TRUE ( obj.toString(false,true) == "{ \"binaryData\": { \"$binary\": \"aGVsbG8gd29ybGQ=\", \"$type\": \"1\" } }" ) ;


   ob3.appendBinData ( "binaryData", strlen(str),
                       bdtUUID, str ) ;
   obj = ob3.obj () ;
   cout<<obj.toString(false,true)<<endl ;
   ASSERT_TRUE ( obj.toString(false,true) == "{ \"binaryData\": { \"$binary\": \"aGVsbG8gd29ybGQ=\", \"$type\": \"3\" } }" ) ;
   ob4.appendBinData ( "binaryData", strlen(str),
                       MD5Type, str ) ;
   obj = ob4.obj () ;
   cout<<obj.toString(false,true)<<endl ;
   ASSERT_TRUE ( obj.toString(false,true) == "{ \"binaryData\": { \"$binary\": \"aGVsbG8gd29ybGQ=\", \"$type\": \"5\" } }" ) ;
   ob5.appendBinData ( "binaryData", strlen(str),
                       bdtCustom, str ) ;
   obj = ob5.obj () ;
   cout<<obj.toString(false,true)<<endl ;
   ASSERT_TRUE ( obj.toString(false,true) == "{ \"binaryData\": { \"$binary\": \"aGVsbG8gd29ybGQ=\", \"$type\": \"128\" } }" ) ;
}

TEST ( cpp_bson_base_type, regex )
{
   BSONObj obj1 ;
   BSONObjBuilder ob1 ;
   BSONObj obj2 ;
   BSONObjBuilder ob2 ;
   BSONObj obj3 ;
   BSONObjBuilder ob3 ;
   BSONObj obj4 ;
   BSONObjBuilder ob4 ;

   const char *c1 = "^张" ;
   const char *c2 = "\\ba\\w*\\b" ;
   const char *c3 = "0\\d{2}-\\d{8}|0\\d{3}-\\d{7}" ;
   const char *c4 = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?" ;

   ob1.appendRegex ( "regex", c1, "i" ) ;
   obj1 = ob1.obj () ;
   cout<<"regex1 is: "<<obj1.toString()<<endl ;

   ob2.appendRegex ( "regex", c2, "m" ) ;
   obj2 = ob2.obj () ;
   cout<<"regex2 is: "<<obj2.toString()<<endl ;

   ob3.appendRegex ( "regex", c3, "x" ) ;
   obj3 = ob3.obj () ;
   cout<<"regex3 is: "<<obj3.toString()<<endl ;

   ob4.appendRegex ( "regex", c4, "s" ) ;
   obj4 = ob4.obj () ;
   cout<<"regex4 is: "<<obj4.toString()<<endl ;
}

TEST ( cpp_bson_base_type, object )
{
   BSONObjBuilder ob ;
   BSONObjBuilder ob1 ;
   BSONObjBuilder ob2 ;
   BSONObj obj ;
   BSONObj obj1 ;
   BSONObj obj2 ;

   obj = BSON( "d"<<BSON("e"<<"hi!") ) ;
   ASSERT_TRUE( obj.toString() == "{ \"d\": { \"e\": \"hi!\" } }" ) ;

   ob1.append ( "name", "sam" ) ;
   ob1.append ( "age", 19 ) ;
   obj1 = ob1.obj() ;
   ob.appendObject ( "info", obj1.objdata() ) ;
   ob.append ( "home","guangzhou" ) ;
   obj = ob.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE ( obj.toString() == "{ \"info\": { \"name\": \"sam\", \"age\": 19 }, \"home\": \"guangzhou\" }" ) ;
}

TEST ( cpp_bson_base_type, array )
{
   BSONObjBuilder ob ;
   BSONObjBuilder ob1 ;
   BSONObj obj ;
   BSONObj obj1 ;

   BSONArray arr = BSON_ARRAY( 34.0 << "day" << BSON( "foo" << BSON_ARRAY( "bar"<< "baz" << "qux" ) ) );
   cout<<arr.toString()<<endl ;
   ASSERT_TRUE ( arr.toString() == "{ \"0\": 34.0, \"1\": \"day\", \"2\": { \"foo\": [ \"bar\", \"baz\", \"qux\" ] } }" ) ;

   obj = BSON( "phone" << BSON_ARRAY( "13800138123" << "13800138124" ) ) ;
   ASSERT_TRUE( obj.toString() == "{ \"phone\": [ \"13800138123\", \"13800138124\" ] }" ) ;

   ob.append ( "0", 1 ) ;
   ob.append ( "1", "hi!" ) ;
   ob.append ( "2", BSON("c"<<true) ) ;
   ob1.appendArray ( "array", ob.obj() ) ;
   obj = ob1.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE ( obj.toString() == "{ \"array\": [ 1, \"hi!\", { \"c\": true } ] }" ) ;
}

TEST ( cpp_bson_base_type, null )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   ob.appendNull ( "key" ) ;
   obj = ob.obj() ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"key\": null }" ) ;
}

TEST ( cpp_bson_base_type, object_nest )
{
   BSONObj obj ;
   BSONObjBuilder b ;
   BSONObjBuilder sub ( b.subobjStart("subobj") ) ;
   sub.append ( "a", 1 ) ;
   sub.append ( "b", "hi!" ) ;
   sub.done () ;
   b.append ( "other", BSON("c"<<true) ) ;
   obj = b.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE ( obj.toString() == "{ \"subobj\": { \"a\": 1, \"b\": \"hi!\" }, \"other\": { \"c\": true } }" ) ;
}

TEST ( cpp_bson_base_type, array_nest )
{
   BSONObj obj ;
   BSONObjBuilder ob ;
   BSONObjBuilder sub ( ob.subarrayStart("subarray") ) ;
   sub.append ( "0", 1 ) ;
   sub.append ( "1", "hi!" ) ;
   sub.append ( "2", false ) ;
   sub.done () ;
   ob.appendArray ( "other", BSON_ARRAY( "0"<<2<<"1"<<"hello!" ) ) ;
   obj = ob.obj () ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE ( obj.toString() == "{ \"subarray\": [ 1, \"hi!\", false ], \"other\": [ \"0\", 2, \"1\", \"hello!\" ] }" ) ;
}

TEST ( cpp_bson_base_type, GT )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   obj = BSON( "g"<<GT<<99 ) ; // { "g": { "$gt": 99 } }
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"g\": { \"$gt\": 99 } }" ) ;
}

TEST ( cpp_bson_base_type, GTE )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   obj = BSON( "g"<<GTE<<99 ) ; // { "g": { "$gte": 99 } }
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"g\": { \"$gte\": 99 } }" ) ;
}

TEST ( cpp_bson_base_type, LT )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   obj = BSON( "g"<<LT<<99 ) ; // { "g": { "$lt": 99 } }
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"g\": { \"$lt\": 99 } }" ) ;
}

TEST ( cpp_bson_base_type, LTE )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   obj = BSON( "g"<<LTE<<99 ) ; // { "g": { "$lte": 99 } }
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"g\": { \"$lte\": 99 } }" ) ;
}

TEST ( cpp_bson_base_type, NE )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   obj = BSON( "g"<<NE<<99 ) ; // { "g": { "$ne": 99 } }
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"g\": { \"$ne\": 99 } }" ) ;
}

TEST ( cpp_bson_base_type, OR )
{
   BSONObjBuilder ob ;
   BSONObj obj ;
   BSONObj obj1 ;
   BSONObj obj2 ;
   BSONObj obj3 ;
   BSONObj obj4 ;
   BSONObj obj5 ;
   obj1 = BSON( "a"<<GT<<99 ) ;
   obj2 = BSON( "b"<<GTE<<99 ) ;
   obj3 = BSON( "c"<<LT<<99 ) ;
   obj4 = BSON( "d"<<LTE<<99 ) ;
   obj5 = BSON( "e"<<NE<<99 ) ;

   obj = OR( obj1, obj2 ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"$or\": [ { \"a\": { \"$gt\": 99 } }, { \"b\": { \"$gte\": 99 } } ] }" ) ;

   obj = OR( obj1, obj2, obj3 ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"$or\": [ { \"a\": { \"$gt\": 99 } }, { \
\"b\": { \"$gte\": 99 } }, { \"c\": { \"$lt\": 99 } } ] }" ) ;

   obj = OR( obj1, obj2, obj3, obj4 ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"$or\": [ { \"a\": { \"$gt\": 99 } }, { \
\"b\": { \"$gte\": 99 } }, { \"c\": { \"$lt\": 99 } }, { \"d\": { \"$lte\": 99 } } ] }" ) ;

   obj = OR( obj1, obj2, obj3, obj4, obj5 ) ;
   cout<<obj.toString()<<endl ;
   ASSERT_TRUE( obj.toString() == "{ \"$or\": [ { \"a\": { \"$gt\": 99 } }, { \
\"b\": { \"$gte\": 99 } }, { \"c\": { \"$lt\": 99 } }, { \"d\": { \"$lte\": 99 } }, \
{ \"e\": { \"$ne\": 99 } } ] }" ) ;
}

