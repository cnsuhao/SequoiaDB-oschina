#include <stdio.h>
#include <gtest/gtest.h>
#include "testcommon.h"
#include "client.h"

TEST(lob,sdbNext)
{
   INT32 rc = SDB_OK ;
   rc = initEnv( HOST, SERVER, USER, PASSWD ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   sdbConnectionHandle db            = 0 ;
   sdbCollectionHandle cl            = 0 ;
   sdbCursorHandle cur               = 0 ;
   sdbLobHandle lob                  = 0 ;
   INT32 NUM                         = 10 ;
   SINT64 count                      = 0 ;
   bson_oid_t oid ;
   bson obj ;
   INT32 bufSize = 1000 ;
   SINT64 lobSize = -1 ;
   UINT64 createTime = -1 ;
   CHAR buf[bufSize] ;
   for ( INT32 i = 0; i < bufSize; i++ )
   {
      buf[i] = 'a' ;
   }
   rc = sdbConnect ( HOST, SERVER, USER, PASSWD, &db ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = getCollection ( db,
                        COLLECTION_FULL_NAME,
                        &cl ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   bson_oid_gen( &oid ) ; 
   rc = sdbOpenLob( cl, &oid, SDB_LOB_CREATEONLY, &lob ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = sdbGetLobSize( lob, &lobSize ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   ASSERT_EQ( 0, lobSize ) ;
   rc = sdbGetLobCreateTime( lob, &createTime ) ;
   ASSERT_EQ( 0, createTime ) ;
   ASSERT_EQ( 0, createTime ) ;
   rc = sdbWriteLob( lob, buf, bufSize ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = sdbGetLobSize( lob, &lobSize ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   ASSERT_EQ( bufSize, lobSize ) ;
   rc = sdbWriteLob( lob, buf, bufSize ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = sdbGetLobSize( lob, &lobSize ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   ASSERT_EQ( 2 * bufSize, lobSize ) ;
   rc = sdbGetLobCreateTime( lob, &createTime ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   ASSERT_EQ( 0, createTime ) ;
   rc = sdbCloseLob ( &lob ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
   rc = sdbRemoveLob( cl, &oid ) ;
   ASSERT_EQ( SDB_OK, rc ) ;
    
   sdbDisconnect ( db ) ;
   sdbReleaseCursor ( cur ) ;
   sdbReleaseCollection ( cl ) ;
   sdbReleaseConnection ( db ) ;
}

