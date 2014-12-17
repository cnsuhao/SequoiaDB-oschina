#include "jstobs.h"
#include "client.hpp"
#include <iostream>
using namespace sdbclient ;
using namespace std ;

#define CHAR_ERR_SIZE 1024
#define INIT_BUF_SIZE 1024
#define INC_BUF_SIZE 4096
#define BUF_BSON_ID "_id"


sdbCollectionSpace cs ;
sdbCollection collection ;
sdb connection ;
sdbCursor query ;
bson obj ;
CHAR *pBuf ;
INT32 bufSize ;

int main()
{
   pBuf = NULL ;
   bufSize = 0 ;
   
   INT32 rc = SDB_OK ;
   CHAR condition[3][23] = 
   {
      "{ hello : 123456 }",
      "{ hello : \"world\" }",
      "{ hello : true }"
   } ;
   rc = connection.connect ( "127.0.0.1", "50000" ) ;
   if ( rc )
   {
      cout<<rc<<endl;
      return rc;
   }
   bson *condition_bson [ 3 ] = { NULL } ;
   for ( int i = 0; i < 3; ++i )
   {
      condition_bson[i] = bson_create () ;
      if( !jsonToBson ( condition_bson[i], condition[i] ) )
      {
         bson_finish  ( condition_bson[i] ) ;
         bson_dispose ( condition_bson[i] ) ;
         cout<<0<<endl;
         return 0 ;
      }
      bson_finish ( condition_bson[i] ) ;
   }
   rc = connection.getCollectionSpace ( "foo", cs ) ;
   if ( rc )
   {
      cout<<rc<<endl;
      return rc ;
   }
   rc = cs.getCollection ( "big", collection ) ;
   if ( rc )
   {
      cout<<rc<<endl;
      return rc ;
   }

   rc = collection.bulkInsert ( FLG_INSERT_CONTONDUP, &condition_bson[0], 3 );
   cout<<rc<<endl;
   if ( pBuf )
   {
      free ( pBuf ) ;
   }
   cout<<"end"<<endl;
   bson_dispose ( condition_bson[0] ) ;
   bson_dispose ( condition_bson[1] ) ;
   bson_dispose ( condition_bson[2] ) ;
   int ak;
   cin>>ak;
   return 0;
}



BOOLEAN _reallocateBuffer ()
{
   INT32 currentSize = bufSize ;
   CHAR *pOldBuf     = pBuf ;
   if ( 0 == currentSize )
      currentSize = INIT_BUF_SIZE ;
   else
      currentSize += currentSize>INC_BUF_SIZE?INC_BUF_SIZE:currentSize ;

   pBuf = (CHAR*)malloc(sizeof(CHAR) * currentSize) ;
   if ( !pBuf )
   {
      pBuf = pOldBuf ;
      return FALSE ;
   }
   free ( pOldBuf ) ;
   bufSize = currentSize ;
   return TRUE ;
}

INT32 connect (const CHAR * hostName,const CHAR * port)
{
   INT32 rc = SDB_OK ;
   rc = connection.connect ( hostName, port ) ;
   return rc ;
}

void close ()
{
   connection.disconnect () ;
}


INT32 createCollectionSpace ( const CHAR *csName )
{
   INT32 rc = SDB_OK ;
   rc = connection.createCollectionSpace ( csName, cs ) ;
   return rc ;
}

INT32 dropCollectionSpace  ( const CHAR *csName )
{
   INT32 rc = SDB_OK ;
   rc = connection.dropCollectionSpace ( csName ) ;
   return rc ;
}

INT32 listCollectionSpaces ()
{
   INT32 rc = SDB_OK ;
   rc = connection.listCollectionSpaces( query ) ;
   return rc;
}

INT32 createCollection ( const CHAR *csName, const CHAR *cName )
{
   INT32 rc = SDB_OK ;
   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      return rc ;
   }

   rc = cs.createCollection ( cName, collection ) ;
   return rc ;
}
INT32 dropCollection ( const CHAR *csName, const CHAR *cName )
{
   INT32 rc = SDB_OK ;
   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      return rc ;
   }
   rc = cs.dropCollection ( cName ) ;
   return rc ;
}

INT32 listCollections ()
{
   INT32 rc = SDB_OK ;
   rc = connection.listCollections ( query ) ;;
   return rc;
}

INT32 getSnapshot ( INT32 listType,
                               CHAR *condition,
                               CHAR *selector,
                               CHAR *orderBy )
{
   INT32 rc = SDB_OK ;

   bson *condition_bson = NULL, *selector_bson = NULL,
        *orderBy_bson = NULL ;

   if ( condition && *condition )
   {
      condition_bson = bson_create () ;
      if( !jsonToBson ( condition_bson, condition ) )
      {
         bson_finish  ( condition_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( condition_bson ) ;
   }

   if ( selector && *selector )
   {
      selector_bson = bson_create () ;
      if ( !jsonToBson ( selector_bson, selector ) )
      {
         bson_finish  ( selector_bson ) ;
         bson_dispose ( selector_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( selector_bson ) ;
    }

   if ( orderBy && *orderBy )
   {
      orderBy_bson = bson_create () ;
      if ( !jsonToBson ( orderBy_bson, orderBy ) )
      {
         bson_finish  ( orderBy_bson ) ;
         bson_dispose ( orderBy_bson ) ;
         bson_dispose ( selector_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( orderBy_bson ) ;
   }

   rc = connection.getSnapshot ( query,listType,
                                 condition_bson,
                                 selector_bson,
                                 orderBy_bson ) ;

   bson_dispose ( condition_bson ) ;
   bson_dispose ( selector_bson  ) ;
   bson_dispose ( orderBy_bson   ) ;
   return rc ;
}

INT32 getList ( INT32 listType,
                           CHAR *condition,
                           CHAR *selector,
                           CHAR *orderBy )
{
   INT32 rc = SDB_OK ;

   bson *condition_bson = NULL, *selector_bson = NULL,
        *orderBy_bson = NULL ;

   if ( condition && *condition )
   {
      condition_bson = bson_create () ;
      if( !jsonToBson ( condition_bson, condition ) )
      {
         bson_finish  ( condition_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( condition_bson ) ;
   }

   if ( selector && *selector )
   {
      selector_bson = bson_create () ;
      if ( !jsonToBson ( selector_bson, selector ) )
      {
         bson_finish  ( selector_bson ) ;
         bson_dispose ( selector_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( selector_bson ) ;
    }

   if ( orderBy && *orderBy )
   {
      orderBy_bson = bson_create () ;
      if ( !jsonToBson ( orderBy_bson, orderBy ) )
      {
         bson_finish  ( orderBy_bson ) ;
         bson_dispose ( orderBy_bson ) ;
         bson_dispose ( selector_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( orderBy_bson ) ;
   }

   rc = connection.getList ( query,listType,
                             condition_bson,
                             selector_bson,
                             orderBy_bson ) ;

   bson_dispose ( condition_bson ) ;
   bson_dispose ( selector_bson  ) ;
   bson_dispose ( orderBy_bson   ) ;
   return rc ;
}

INT32 resetSnapshot ()
{
   INT32 rc = SDB_OK ;
   rc = connection.resetSnapshot () ;
   return rc ;
}


INT32 queryData ( const CHAR *csName    , const CHAR *cName    ,
                             const CHAR *condition , const CHAR *selected ,
                             const CHAR *orderBy   , const CHAR *hint     ,
                             INT64 numToSkip       , INT64 numToReturn )
{
   INT32 rc = SDB_OK ;
   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      return rc ;
   }
   rc = cs.getCollection ( cName, collection ) ;
   if ( rc )
   {
      return rc ;
   }

   bson *condition_bson = NULL, *selected_bson = NULL,
        *orderBy_bson = NULL, *hint_bson = NULL ;

   if ( condition && *condition )
   {
      condition_bson = bson_create () ;
      if( !jsonToBson ( condition_bson, condition ) )
      {
         bson_finish  ( condition_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( condition_bson ) ;
   }

   if ( selected && *selected )
   {
      selected_bson = bson_create () ;
      if ( !jsonToBson ( selected_bson, selected ) )
      {
         bson_finish  ( selected_bson ) ;
         bson_dispose ( selected_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( selected_bson ) ;
    }

   if ( orderBy && *orderBy )
   {
      orderBy_bson = bson_create () ;
      if ( !jsonToBson ( orderBy_bson, orderBy ) )
      {
         bson_finish  ( orderBy_bson ) ;
         bson_dispose ( orderBy_bson ) ;
         bson_dispose ( selected_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( orderBy_bson ) ;
   }

   if ( hint && *hint )
   {
      hint_bson = bson_create () ;
      if ( !jsonToBson ( hint_bson, hint ) )
      {
         bson_finish  ( hint_bson ) ;
         bson_dispose ( hint_bson ) ;
         bson_dispose ( orderBy_bson ) ;
         bson_dispose ( selected_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( hint_bson ) ;
   }


   rc = collection.query ( query          ,
                           condition_bson ,
                           selected_bson  ,
                           orderBy_bson   ,
                           hint_bson      ,
                           numToSkip      ,
                           numToReturn ) ;

   bson_dispose ( condition_bson ) ;
   bson_dispose ( selected_bson  ) ;
   bson_dispose ( orderBy_bson   ) ;
   bson_dispose ( hint_bson      ) ;
   return rc ;
}

INT32 insertData ( const CHAR *csName ,
                              const CHAR *cName  ,
                              const CHAR *json_str )
{
   INT32 rc = SDB_OK ;

   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      return rc ;
   }
   rc = cs.getCollection ( cName, collection ) ;
   if ( rc )
   {
      return rc ;
   }

   bson *bs = bson_create () ;

   if ( !jsonToBson ( bs, json_str ) )
   {
      bson_finish  ( bs ) ;
      bson_dispose ( bs ) ;
      return SDB_INVALIDARG ;
   }

   bson_finish ( bs ) ;

   rc = collection.insert ( bs ) ;

   bson_dispose ( bs ) ;

   return rc ;
}


INT32 updateData ( const CHAR *csName    ,
                              const CHAR *cName     ,
                              const CHAR *rule      ,
                              const CHAR *condition ,
                              const CHAR *hint )
{
   INT32 rc = SDB_OK ;
   bson *rule_bson      = NULL ;
   bson *condition_bson = NULL ;
   bson *hint_bson      = NULL ;

   if ( !rule )
   {
      return SDB_INVALIDARG ;
   }
   else
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

   if ( condition && *condition )
   {
      condition_bson = bson_create () ;
      if( !jsonToBson ( condition_bson, condition) )
      {
          bson_finish  ( condition_bson ) ;
          bson_dispose ( condition_bson ) ;
          bson_dispose ( rule_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( condition_bson ) ;
   }

   if ( hint && *hint )
   {
      hint_bson = bson_create () ;
      if ( !jsonToBson ( hint_bson, hint) )
      {
          bson_finish  ( hint_bson ) ;
          bson_dispose ( hint_bson ) ;
          bson_dispose ( condition_bson ) ;
          bson_dispose ( rule_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( hint_bson ) ;
   }

   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      bson_dispose ( rule_bson      ) ;
      bson_dispose ( condition_bson ) ;
      bson_dispose ( hint_bson      ) ;
      return rc ;
   }
   rc = cs.getCollection ( cName, collection ) ;
   if ( rc )
   {
      bson_dispose ( rule_bson      ) ;
      bson_dispose ( condition_bson ) ;
      bson_dispose ( hint_bson      ) ;
      return rc ;
   }

   rc = collection.update ( rule_bson, condition_bson, hint_bson ) ;

   bson_dispose ( rule_bson      ) ;
   bson_dispose ( condition_bson ) ;
   bson_dispose ( hint_bson      ) ;
   return rc ;
}

INT32 deleteData ( const CHAR *csName    ,
                              const CHAR *cName     ,
                              const CHAR *condition ,
                              const CHAR *hint )
{
   INT32 rc = SDB_OK ;

   bson *condition_bson = NULL, *hint_bson = NULL ;

   if ( condition && *condition )
   {
      condition_bson = bson_create () ;
      if( !jsonToBson ( condition_bson, condition) )
      {
         bson_finish  ( condition_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( condition_bson ) ;
   }

   if ( hint && *hint )
   {
      hint_bson = bson_create () ;
      if ( !jsonToBson ( hint_bson, hint) )
      {
         bson_finish  ( hint_bson ) ;
         bson_dispose ( hint_bson ) ;
         bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( hint_bson ) ;
   }

   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      bson_dispose ( condition_bson ) ;
      bson_dispose ( hint_bson      ) ;
      return rc ;
   }
   rc = cs.getCollection ( cName, collection ) ;
   if ( rc )
   {
      bson_dispose ( condition_bson ) ;
      bson_dispose ( hint_bson      ) ;
      return rc ;
   }
   rc = collection.del ( condition_bson, hint_bson ) ;
   bson_dispose ( condition_bson ) ;
   bson_dispose ( hint_bson      ) ;
   return rc ;
}

INT32 getCount ( const CHAR *csName    ,
                            const CHAR *cName     ,
                            const CHAR *condition ,
                            INT64 &count )
{
   INT32 rc = SDB_OK ;
   bson *condition_bson = NULL ;
   if ( condition )
   {
      condition_bson = bson_create () ;
      if( !jsonToBson ( condition_bson, condition) )
      {
          bson_finish  ( condition_bson ) ;
          bson_dispose ( condition_bson ) ;
         return SDB_INVALIDARG ;
      }
      bson_finish ( condition_bson ) ;
   }
   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      bson_dispose ( condition_bson ) ;
      return rc ;
   }
   rc = cs.getCollection ( cName, collection ) ;
   if ( rc )
   {
      bson_dispose ( condition_bson ) ;
      return rc ;
   }
   rc = collection.getCount ( condition_bson, count ) ;
   bson_dispose ( condition_bson ) ;

   return rc ;
}

INT32 createIndex ( const CHAR *csName   ,
                               const CHAR *cName    ,
                               const CHAR *indexDef ,
                               const CHAR *pName    ,
                               BOOLEAN isUnique )
{
   INT32 rc = SDB_OK ;

   bson *indexDef_bson ;

   if ( indexDef )
   {
      indexDef_bson = bson_create () ;
      if( !jsonToBson ( indexDef_bson, indexDef) )
      {
          bson_finish  ( indexDef_bson ) ;
          bson_dispose ( indexDef_bson ) ;
          return SDB_INVALIDARG ;
      }
      bson_finish ( indexDef_bson ) ;
   }
   else
   {
      return SDB_INVALIDARG;
   }

   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      bson_dispose ( indexDef_bson ) ;
      return rc ;
   }
   rc = cs.getCollection ( cName, collection ) ;
   if ( rc )
   {
      bson_dispose ( indexDef_bson ) ;
      return rc ;
   }

   rc = collection.createIndex ( indexDef_bson, pName, isUnique ) ;
   bson_dispose ( indexDef_bson ) ;

   return rc ;
}

INT32 dropIndex ( const CHAR *csName ,
                             const CHAR *cName  ,
                             const CHAR *pName )
{
   INT32 rc = SDB_OK ;

   rc = connection.getCollectionSpace ( csName, cs ) ;
   if ( rc )
   {
      return rc ;
   }
   rc = cs.getCollection ( cName, collection ) ;
   if ( rc )
   {
      return rc ;
   }
   rc = collection.dropIndex ( pName ) ;

   return rc ;
}

const CHAR * queryNext ()
{
   INT32 rc = SDB_OK ;
   rc = query.next ( obj ) ;
   if ( !rc )
   {
      if ( !pBuf && !_reallocateBuffer() )
         return NULL ;
      do
      {
         rc = bsonToJson ( pBuf, bufSize, &obj );
         if ( !rc && !_reallocateBuffer ())
            return NULL ;
      } while ( !rc ) ;
      return pBuf ;
   }
   return NULL ;
}

const CHAR * queryCurrent ()
{
   INT32 rc = SDB_OK ;
   rc = query.current ( obj ) ;
   if ( !rc )
   {
      if ( !pBuf && !_reallocateBuffer() )
         return NULL ;
      do
      {
         rc = bsonToJson ( pBuf, bufSize, &obj );
         if ( !rc && !_reallocateBuffer ())
            return NULL ;
      } while ( !rc ) ;
      return pBuf ;
   }
   return NULL ;
}

INT32 updateCurrent ( const CHAR *rule )
{
   INT32 rc = SDB_OK ;
   bson *rule_bson ;
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
   rc = query.updateCurrent ( *rule_bson ) ;
   bson_dispose ( rule_bson ) ;
   return rc ;
}

INT32 delCurrent ()
{
   INT32 rc = SDB_OK ;
   rc = query.delCurrent () ;
   return rc ;
}


