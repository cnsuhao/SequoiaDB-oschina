#include "mthSelector.hpp"
#include <gtest/gtest.h>
#include <iostream>

using namespace std ;
using namespace bson ;
using namespace engine ;

TEST( selector, simple_include_test_1 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "b" << BSON( "$include" << 1 ) << "a" << BSON( "$include" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 1 << "b" << 2 << "c" << 3 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << 1 << "b" << 2 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_include_test_2 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "b" << BSON( "$include" << 1 ) << "a" << BSON( "$include" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "c" << 1 << "b" << 2 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "b" << 2 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_include_test_3 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "b" << BSON( "$include" << 1 ) << "a" << BSON( "$include" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "c" << 1 << "d" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSONObj() ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_include_test_4 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON( "c" << 1 << "b" << 1 ) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON( "b" << 1 ) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_include_test_5 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSONObj() ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_include_test_6 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( BSON( "c" << 1) << BSON("d" << 1) << BSON("b"<< 1) << BSON("b" << 2)) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY(BSONObj() << BSONObj() <<BSON("b" << 1) << BSON("b" << 2))) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_include_test_7 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( BSON( "c" << 1) ) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( BSONObj() ) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_include_test_8 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( 1 << 2 ) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSONArrayBuilder().arr() ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_exclude_test_1 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "b" << BSON( "$include" << 0 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 1 << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_exclude_test_2 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 0 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 1 << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << 1 << "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_exclude_test_3 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 0 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON( "b" << 1 << "c" << 1) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON("c" << 1) << "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_exclude_test_4 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 0 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( 1 << BSON("b" << 1 << "c" <<1 ) << BSON("b" << 1 ) << BSON("d" << 1))) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( 1 << BSON("c" <<1 ) << BSONObj() << BSON("d" << 1)) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_exclude_test_5 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$include" << 0 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_exclude_test_6 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$include" << 0 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_exclude_test_7 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$include" << 1 ) << "b" << BSON("$include" << 0 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_INVALIDARG , rc ) ;
}

TEST( selector, simple_default_test_1 )
{
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$default" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }

   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << 1 ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
}


TEST( selector, simple_default_test_2 )
{
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$default" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 2 << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << 2 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }

   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << 1 ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 2 << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << 2 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
}

TEST( selector, simple_default_test_3 )
{
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$default" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON( "c" << 1 << "d" << 1 ) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON( "b" << 1) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << 1 ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON( "c" << 1 << "d" << 1 ) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON( "b" << 1) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
}

TEST( selector, simple_default_test_4 )
{
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$default" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "b" << 1) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON( "b" << 1 ) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << 1 ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "b" << 1) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON( "b" << 1 ) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
}

TEST( selector, simple_default_test_5 )
{
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$default" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( BSON("c" << 1) << 1 << BSON("b" << 1) )) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( BSON("b" << 1) << BSON( "b" << 1 )) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << 1 ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( BSON("c" << 1) << 1 << BSON("b" << 1 )) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( BSON("b" << 1) << BSON("b" << 1 )) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
}

TEST( selector, simple_default_test_6 )
{
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$default" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 1 << "b" << 1) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSONObj() ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
   {
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << 1 ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 1 << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSONObj() ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   }
}

TEST( selector, simple_slice_test_1 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$slice" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( 1 << 2 << 3 ) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( 1 ) << "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_slice_test_2 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$slice" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON( "a" << 1 << "b" << BSON_ARRAY(1 << 2 << 3)) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON( "a" << 1 << "b" << BSON_ARRAY( 1 ))<< "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_slice_test_3 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$slice" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON( "a" << 1 << "b" << 1 ) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON( "a" << 1 << "b" << 1 )<< "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_slice_test_4 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$slice" << -2 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( 1 << 2 << 3 ) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( 2 << 3 )<< "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_slice_test_5 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$slice" << -2 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( 1 ) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( 1 )<< "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_slice_test_6 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$slice" << BSON_ARRAY( 0 << 2 ) ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( 1 << 2 << 3 ) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( 1 << 2 )<< "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_slice_test_7 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$slice" << BSON_ARRAY( 2 << 2 ) ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( 1 << 2 << 3 ) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( 3 )<< "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_slice_test_8 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a.b" << BSON( "$slice" << 1 ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( BSON("b" << 1 ) << BSON( "b" << BSON_ARRAY( 1 << 2 << 3)) << BSON( "b" << BSON_ARRAY(4 << 5 << 6))<< BSON("b" << 1) ) << "b" << 1 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( BSON( "b" << 1) << BSON("b" << BSON_ARRAY( 1)) << BSON("b" << BSON_ARRAY(4)) << BSON("b" << 1) )<< "b" << 1 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_elemmatch_test_1 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$elemMatch" << BSON( "b" << 1 ) ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( BSON( "b" << 1 ) << BSON( "b" << 2 ) << BSON( "b" << 1 ) ) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( BSON("b" << 1 ) << BSON( "b" << 1 )) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_elemmatch_test_2 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$elemMatch" << BSON( "c" << 1 ) ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( BSON( "b" << 1 ) << BSON( "b" << 2 ) << BSON( "b" << 1 ) ) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSONArrayBuilder().arr() ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_elemmatch_test_3 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$elemMatch" << BSON( "b" << 1 ) ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << 1 << "b" << 2 ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "b" << 2 ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

TEST( selector, simple_elemmatchone_test_1 )
{
   INT32 rc = SDB_OK ;
   mthSelector selector ;
   BSONObj rule = BSON( "a" << BSON( "$elemMatchOne" << BSON( "b" << 1 ) ) ) ;
   rc = selector.loadPattern( rule ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   BSONObj record = BSON( "a" << BSON_ARRAY( BSON( "b" << 1 ) << BSON( "b" << 2 ) << BSON( "b" << 1 ) ) ) ;
   BSONObj result ;
   rc = selector.select( record, result ) ;
   ASSERT_EQ( SDB_OK , rc ) ;
   cout << result.toString( FALSE, TRUE ) << endl ;
   BSONObj expect = BSON( "a" << BSON_ARRAY( BSON("b" << 1 ) ) ) ;
   rc = expect.woCompare( result ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
}

