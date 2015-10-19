#include "postgres.h"
#include "sdb_fdw.h"
#include "msgDef.h"

#include "time.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#define SDB_COLLECTIONSPACE_NAME_LEN 128
#define SDB_COLLECTION_NAME_LEN      128
#define SDB_COLUMNA_ID_NAME          "_id"
#define SDB_SHARDINGKEY_NAME         "ShardingKey"

#define SDB_MSECS_TO_USECS           ( 1000L )
#define SDB_SECONDS_TO_USECS         ( 1000L * 1000L )
#define SDB_MSECS_PER_HOUR           ( USECS_PER_HOUR/1000 )


#define SDB_FIELD_COMMA              ","
#define SDB_FIELD_SEMICOLON          ":"
#define SDB_FIELD_SEMICOLON_CHR      ':'

#define SDB_FIELD_VALUE_LEN          (128)

static bool SdbIsListMode( CHAR *hostName ) ;

static int SdbSetNodeAddressInfo( SdbInputOptions *options, CHAR *hostName, 
                                  CHAR *service ) ;

static void SdbGetForeignRelSize( PlannerInfo *root,
                                   RelOptInfo *baserel,
                                   Oid foreignTableId ) ;

static void SdbGetForeignPaths( PlannerInfo *root,
                                 RelOptInfo *baserel,
                                 Oid foreignTableId ) ;

static ForeignScan *SdbGetForeignPlan( PlannerInfo *root,
                                        RelOptInfo *baserel,
                                        Oid foreignTableId,
                                        ForeignPath *bestPath,
                                        List *targetList,
                                        List *restrictionClauses ) ;

static void SdbBeginForeignScan( ForeignScanState *scanState,
                                  INT32 executorFlags ) ;

static TupleTableSlot *SdbIterateForeignScan( ForeignScanState *scanState ) ;

static void SdbRescanForeignScan( ForeignScanState *scanState ) ;

static void SdbEndForeignScan( ForeignScanState *scanState ) ;

static void SdbExplainForeignScan( ForeignScanState *scanState,
                                    ExplainState *explainState ) ;

static bool SdbAnalyzeForeignTable( Relation relation,
                                     AcquireSampleRowsFunc *acquireSampleRowsFunc,
                                     BlockNumber *totalPageCount ) ;

static INT32 SdbIsForeignRelUpdatable( Relation rel ) ;

static void SdbAddForeignUpdateTargets( Query *parsetree,
                                         RangeTblEntry *target_rte,
                                         Relation target_relation ) ;

static List *SdbPlanForeignModify ( PlannerInfo *root,
                                    ModifyTable *plan,
                                    Index resultRelation,
                                    INT32 subplan_index ) ;

static void sdb_slot_deform_tuple( TupleTableSlot *slot, int natts ) ;

/* module initialization */
void _PG_init (  ) ;

/* transaction management */
static void SdbFdwXactCallback( XactEvent event, void *arg ) ;

#if PG_VERSION_NUM>90300
static void SdbBeginForeignModify( ModifyTableState *mtstate,
      ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags ) ;

static TupleTableSlot *SdbExecForeignInsert( EState *estate, ResultRelInfo *rinfo,
      TupleTableSlot *slot, TupleTableSlot *planSlot ) ;

static TupleTableSlot *SdbExecForeignDelete( EState *estate, ResultRelInfo *rinfo,
      TupleTableSlot *slot, TupleTableSlot *planSlot ) ;

static TupleTableSlot *SdbExecForeignUpdate( EState *estate, ResultRelInfo *rinfo,\
      TupleTableSlot *slot, TupleTableSlot *planSlot ) ;

static void SdbEndForeignModify( EState *estate, ResultRelInfo *rinfo ) ;

static void SdbExplainForeignModify( ModifyTableState *mtstate, ResultRelInfo *rinfo,
      List *fdw_private, int subplan_index, struct ExplainState *es ) ;

#endif

/* register handler and validator */
PG_MODULE_MAGIC ;
PG_FUNCTION_INFO_V1( sdb_fdw_handler ) ;
PG_FUNCTION_INFO_V1( sdb_fdw_validator ) ;

/* declare for sdb_fdw_handler */
Datum sdb_fdw_handler( PG_FUNCTION_ARGS )
{
   FdwRoutine *fdwRoutine              = makeNode( FdwRoutine ) ;
   /* Functions for scanning foreign tables */
   fdwRoutine->GetForeignRelSize       = SdbGetForeignRelSize ;
   fdwRoutine->GetForeignPaths         = SdbGetForeignPaths ;
   fdwRoutine->GetForeignPlan          = SdbGetForeignPlan ;
   fdwRoutine->BeginForeignScan        = SdbBeginForeignScan ;
   fdwRoutine->IterateForeignScan      = SdbIterateForeignScan ;
   fdwRoutine->ReScanForeignScan       = SdbRescanForeignScan ;
   fdwRoutine->EndForeignScan          = SdbEndForeignScan ;
#if PG_VERSION_NUM>90300
   /* Remaining functions are optional */
   fdwRoutine->AddForeignUpdateTargets = SdbAddForeignUpdateTargets ;
   fdwRoutine->PlanForeignModify       = SdbPlanForeignModify ;
   fdwRoutine->BeginForeignModify      = SdbBeginForeignModify ;
   fdwRoutine->ExecForeignInsert       = SdbExecForeignInsert ;
   fdwRoutine->ExecForeignUpdate       = SdbExecForeignUpdate ;
   fdwRoutine->ExecForeignDelete       = SdbExecForeignDelete ;
   fdwRoutine->EndForeignModify        = SdbEndForeignModify ;
   fdwRoutine->IsForeignRelUpdatable   = SdbIsForeignRelUpdatable ;
#endif
   /* Support functions for EXPLAIN */
   fdwRoutine->ExplainForeignScan      = SdbExplainForeignScan ;
#if PG_VERSION_NUM>90300
   fdwRoutine->ExplainForeignModify    = SdbExplainForeignModify ;
#endif
   fdwRoutine->AnalyzeForeignTable     = SdbAnalyzeForeignTable ;
   PG_RETURN_POINTER( fdwRoutine ) ;
}

static void sdbGetOptions( Oid foreignTableId, SdbInputOptions *options ) ;
static SdbConnectionPool *sdbGetConnectionPool(  ) ;


static int sdbSetConnectionPreference(  ) ;
static int sdbGetSdbServerOptions( Oid foreignTableId, SdbExecState *sdbExecState ) ;

static void sdbReleaseConnectionFromPool( int index ) ;
static sdbConnectionHandle sdbGetConnectionHandle( const char **serverList, 
                                             int serverNum, 
                                             const char *usr, 
                                             const char *passwd, 
                                             const char *preference_instance ) ;
static sdbCollectionHandle sdbGetSdbCollection( sdbConnectionHandle connectionHandle, 
      const char *sdbcs, const char *sdbcl ) ;

static PgTableDesc *sdbGetPgTableDesc( Oid foreignTableId ) ;
static void initSdbExecState( SdbExecState *sdbExecState ) ;
static void sdbFreeScanState( SdbExecState *executionState, bool deleteShared ) ;

static Const *sdbSerializeDocument( sdbbson *document ) ;
static void sdbDeserializeDocument( Const *constant, sdbbson *document ) ;

static void sdbPrintBson( sdbbson *bson, int log_level ) ;

#define serializeInt( x )makeConst( INT4OID, -1, InvalidOid, 4, Int32GetDatum( ( int32 )( x ) ), 0, 1 )
#define serializeOid( x )makeConst( OIDOID, -1, InvalidOid, 4, ObjectIdGetDatum( x ), 0, 1 )
static Const *serializeString( const char *s ) ;
static Const *serializeUint64( UINT64 value ) ;
static List *serializeSdbExecState( SdbExecState *fdwState ) ;

static char *deserializeString( Const *constant ) ;
static UINT64 deserializeUint64( Const *constant ) ;
static SdbExecState *deserializeSdbExecState( List *sdbExecStateList ) ;

static int sdbSetBsonValue( sdbbson *bsonObj, const char *name, Datum valueDatum, 
      Oid columnType, INT32 columnTypeMod ) ;

static void sdbAppendConstantValue( sdbbson *bsonObj, const char *keyName, 
      Const *constant ) ;


static UINT64 sdbCreateBsonRecordAddr(  ) ;
static sdbbson* sdbGetRecordPointer( UINT64 record_addr ) ;

static void sdbGetColumnKeyInfo( SdbExecState *fdw_state ) ;

static bool sdbIsShardingKeyChanged( SdbExecState *fdw_state, sdbbson *oldBson, 
      sdbbson *newBson ) ;

static UINT64 sdbbson_iterator_getusecs( sdbbson_iterator *ite ) ;

static bool isAllTwoArgumentVarType( List *arguments ) ;
static INT32 sdbRecurOperExprTwoVar( OpExpr *opr_two_argument, SdbExprTreeState *expr_state, 
                              sdbbson *condition ) ;
static INT32 sdbRecurOperExpr( OpExpr *opr, SdbExprTreeState *expr_state, 
                        sdbbson *condition ) ;

static INT32 sdbRecurScalarArrayOpExpr( ScalarArrayOpExpr *scalaExpr, 
                                 SdbExprTreeState *expr_state, 
                                 sdbbson *condition ) ;
static INT32 sdbRecurNullTestExpr( NullTest *ntest, SdbExprTreeState *expr_state, 
                              sdbbson *condition ) ;
static INT32 sdbRecurBoolExpr( BoolExpr *boolexpr, SdbExprTreeState *expr_state, 
                                 sdbbson *condition ) ;
static INT32 sdbRecurExprTree( Node *node, SdbExprTreeState *expr_state, 
                                sdbbson *condition ) ;

static INT32 sdbGenerateFilterCondition( Oid foreign_id, RelOptInfo *baserel, sdbbson *condition ) ;

static const CHAR *sdbOperatorName( const CHAR *operatorName ) ;

static Expr *sdbFindArgumentOfType( List *argumentList, NodeTag argumentType ) ;


bool SdbIsListMode( CHAR *hostName )
{
   INT32 i      = 0 ;
   INT32 len    = strlen( hostName ) ;
   for( i = 0 ; i < len ; i++ )
   {
      if ( hostName[i] == SDB_FIELD_SEMICOLON_CHR )
      {
         return true ;
      }
   }

   return false ;
}

int SdbSetNodeAddressInfo( SdbInputOptions *options, CHAR *hostName, 
                           CHAR *service )
{
   CHAR tmpName[ SDB_MAX_SERVICE_LENGTH + 1 ] ;
   snprintf( tmpName, SDB_MAX_SERVICE_LENGTH, "%s", hostName ) ;
   ereport( DEBUG1, ( errcode( ERRCODE_FDW_ERROR ), 
            errmsg( "hostName=%s,service=%s", tmpName, service ) ) ) ;
   if( !SdbIsListMode( tmpName ) )
   {
      StringInfo result = makeStringInfo() ;
      appendStringInfo( result, "%s%s%s", tmpName, SDB_FIELD_SEMICOLON,
                        service ) ;
      options->serviceNum     = 1 ;
      options->serviceList[0] = pstrdup( result->data ) ;
   }
   else
   {
      CHAR *tmpSrc    = tmpName ;
      CHAR *tmpPtr    = NULL ;
      CHAR *tmpResult = NULL ;
      INT32 i = 0 ;
      while( ( tmpResult = strtok_r( tmpSrc, SDB_FIELD_COMMA, &tmpPtr ) )
             != NULL )
      {
         tmpSrc = NULL ;
         if ( i >= INITIAL_ARRAY_CAPACITY )
         {
            break ;
         }

         options->serviceList[i] = pstrdup( tmpResult ) ;
         i++ ;         
      }

      options->serviceNum = i ;
   }

   return SDB_OK ;
}

int sdbSetConnectionPreference( sdbConnectionHandle hConnection, 
   const CHAR *preference_instance )
{
   int intPreferenece_instance = 0 ;
   int rc = 0 ;
   if ( NULL != preference_instance ){
      sdbbson recordObj ;
      sdbbson_init( &recordObj ) ;
      intPreferenece_instance = atoi( preference_instance ) ;
      if ( 0 == intPreferenece_instance )
      {
         sdbbson_append_string( &recordObj, FIELD_NAME_PREFERED_INSTANCE, preference_instance ) ;
      }
      else
      {
         sdbbson_append_int( &recordObj, FIELD_NAME_PREFERED_INSTANCE, intPreferenece_instance ) ;
      }

      rc = sdbbson_finish( &recordObj ) ;
      if ( rc != SDB_OK )
      {
         ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
               errmsg( "finish bson failed:rc = %d", rc ), 
               errhint( "Make sure the data is all right" ) ) ) ;

         sdbbson_destroy( &recordObj ) ;
         return rc ;
      }

      rc = sdbSetSessionAttr( hConnection, &recordObj ) ;
      if ( rc != SDB_OK )
      {
         ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
               errmsg( "set session attribute failed:rc = %d", rc ), 
               errhint( "Make sure the session is all right" ) ) ) ;

         sdbbson_destroy( &recordObj ) ;
         return rc ;
      }

      sdbbson_destroy( &recordObj ) ;
   }

   return rc ;
}

int sdbGetSdbServerOptions( Oid foreignTableId, SdbExecState *sdbExecState )
{
   INT32 i = 0 ;
   SdbInputOptions options ;
   sdbGetOptions( foreignTableId, &options ) ;
   for ( i = 0 ; i < options.serviceNum; i++ )
   {
      sdbExecState->sdbServerList[i] = options.serviceList[i] ;
   }
   sdbExecState->sdbServerNum  = options.serviceNum;
   sdbExecState->sdbcs         = options.collectionspace ;
   sdbExecState->sdbcl         = options.collection ;
   sdbExecState->usr           = options.user ;
   sdbExecState->passwd        = options.password ;
   sdbExecState->preferenceInstance = options.preference_instance ;

   return 0 ;
}

void sdbReleaseConnectionFromPool( int index )
{
   INT32 i = index ;
   SdbConnection *connection = NULL ;
   INT32 j = i + 1 ;

   SdbConnectionPool *pool = sdbGetConnectionPool() ;
   if ( i >= pool->numConnections )
   {
      return ;
   }

   connection = &pool->connList[i] ;
   if( connection->connName )
   {
      free( connection->connName ) ;
   }
   sdbDisconnect( connection->hConnection ) ;
   sdbReleaseConnection( connection->hConnection ) ;

   for ( ; j < pool->numConnections ; ++i, ++j )
   {
      pool->connList[i] = pool->connList[j] ;
   }

   pool->numConnections-- ;
}

sdbConnectionHandle sdbGetConnectionHandle( const char **serverList, 
                                            int serverNum, 
                                            const char *usr, 
                                            const char *passwd, 
                                            const char *preference_instance )
{
   sdbConnectionHandle hConnection = SDB_INVALID_HANDLE ;
   SdbConnectionPool *pool         = NULL ;
   INT32 count                     = 0 ;
   INT32 rc                        = SDB_OK ;
   INT32 i                         = 0 ;
   SdbConnection *connect          = NULL ;

   /* connection string is address + service + user + password */
   StringInfo connName = makeStringInfo() ;
   i = 0 ;
   while ( i < serverNum )
   {
      appendStringInfo( connName, "%s:", serverList[i] ) ;
      i++ ;
   }
   appendStringInfo( connName, "%s:%s", usr, passwd ) ;

   /* iterate all connections in pool */
   pool = sdbGetConnectionPool() ;
   for ( count = 0 ; count < pool->numConnections ; ++count )
   {
      SdbConnection *tmpConnection = &pool->connList[count] ;
      if ( strcmp( tmpConnection->connName, connName->data ) == 0 )
      {
         BOOLEAN result = FALSE ;
         sdbIsValid( tmpConnection->hConnection, &result ) ;
         if ( !result )
         {
            sdbReleaseConnectionFromPool( count ) ;
            break ;
         }

         if ( tmpConnection->transLevel <= 0 )
         {
            tmpConnection->transLevel = 1 ;
         }

         return tmpConnection->hConnection;
      }
   }

   /* when we get here, we don't have the connection so let's create one */
   ereport( DEBUG1, (errcode( ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION ), 
            errmsg( "connecting :list=%s,num=%d", connName->data, serverNum ) )) ;
   rc = sdbConnect1( serverList, serverNum, usr, passwd, &hConnection ) ;
   if ( rc )
   {
      StringInfo tmpInfo = makeStringInfo() ;
      i = 0 ;
      while ( i < serverNum )
      {
         appendStringInfo( tmpInfo, "%s:", serverList[i] ) ;
         i++ ;
      }
      ereport( ERROR, ( errcode( ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION ),
                        errmsg( "unable to establish connection to \"%s\""
                                ", rc = %d", tmpInfo->data, rc ),
                        errhint( "Make sure remote service is running "
                                 "and username/password are valid" ) ) ) ;
      return SDB_INVALID_HANDLE ;
   }

   rc = sdbSetConnectionPreference( hConnection, preference_instance ) ;
   if ( rc )
   {
      ereport( WARNING, ( errcode( ERRCODE_WITH_CHECK_OPTION_VIOLATION ), 
                          errmsg( "set connection's preference instance failed"
                                  ":rc=%d,preference=%s", rc, 
                                  preference_instance ),
                          errhint( "Make sure the OPTION_NAME_PREFEREDINSTANCE " 
                                   "are valid" ) ) ) ;
   }

   /* add connection into pool */
   if ( pool->poolSize <= pool->numConnections )
   {
      /* allocate new slots */
      SdbConnection *pNewMem = pool->connList ;
      INT32 poolSize = pool->poolSize ;
      poolSize = poolSize << 1 ;
      pNewMem  = ( SdbConnection* )realloc( pNewMem, sizeof( SdbConnection )* poolSize ) ;
      if ( !pNewMem )
      {
         sdbDisconnect( hConnection ) ;
         ereport( ERROR, ( errcode( ERRCODE_FDW_OUT_OF_MEMORY ),
                           errmsg( "Unable to allocate connection pool" ),
                           errhint( "Make sure the memory pool or ulimit is "
                                    "properly configured" ) ) ) ;
         return SDB_INVALID_HANDLE ;
      }

      pool->connList = pNewMem ;
      pool->poolSize = poolSize ;
   }

   connect = &pool->connList[pool->numConnections] ;
   connect->connName      = strdup( connName->data ) ;
   connect->hConnection   = hConnection ;
   connect->transLevel    = 1 ;
   pool->numConnections++ ;


   return hConnection ;
}

sdbCollectionHandle sdbGetSdbCollection( sdbConnectionHandle connectionHandle, 
      const char *sdbcs, const char *sdbcl )

{
   /* get collection */
   int rc = SDB_OK ;
   sdbCollectionHandle hCollection = SDB_INVALID_HANDLE ;
   StringInfo fullCollectionName   = makeStringInfo(  ) ;
   appendStringInfoString( fullCollectionName, sdbcs ) ;
   appendStringInfoString( fullCollectionName, "." ) ;
   appendStringInfoString( fullCollectionName, sdbcl ) ;
   rc = sdbGetCollection( connectionHandle, fullCollectionName->data, &hCollection ) ;
   if ( rc )
   {
      ereport( ERROR, ( errcode( ERRCODE_FDW_ERROR ),
                         errmsg( "Unable to get collection \"%s\", rc = %d",
                                  fullCollectionName->data, rc ),
                         errhint( "Make sure the collectionspace and "
                                   "collection exist on the remote database" ) ) ) ;
   }

   return hCollection ;
}

PgTableDesc *sdbGetPgTableDesc( Oid foreignTableId )
{
   int i = 0 ;
   Relation rel      = heap_open( foreignTableId, NoLock ) ;
   TupleDesc tupdesc = rel->rd_att ;

   PgTableDesc *tableDesc = palloc( sizeof( PgTableDesc ) ) ;
   tableDesc->ncols = tupdesc->natts ;
   tableDesc->name  = get_rel_name( foreignTableId ) ;
   tableDesc->cols  = palloc( tableDesc->ncols * sizeof( PgColumnsDesc ) ) ;

   for ( i = 0 ; i < tupdesc->natts ; i++ )
   {
      Form_pg_attribute att_tuple = tupdesc->attrs[i] ;
      tableDesc->cols[i].isDropped = false ;
      tableDesc->cols[i].pgattnum  = att_tuple->attnum ;
      tableDesc->cols[i].pgtype    = att_tuple->atttypid ;
      tableDesc->cols[i].pgtypmod  = att_tuple->atttypmod ;
      tableDesc->cols[i].pgname    = pstrdup( NameStr( att_tuple->attname ) ) ;

      if ( att_tuple->attisdropped )
      {
         tableDesc->cols[i].isDropped = true ;
      }
   }

   heap_close( rel, NoLock ) ;
   return tableDesc ;
}

void initSdbExecState( SdbExecState *sdbExecState )
{
   memset( sdbExecState, 0, sizeof( SdbExecState ) ) ;
   sdbExecState->hCursor           = SDB_INVALID_HANDLE ;
   sdbExecState->hConnection       = SDB_INVALID_HANDLE ;
   sdbExecState->hCollection       = SDB_INVALID_HANDLE ;

   sdbbson_init( &sdbExecState->queryDocument ) ;
   sdbbson_finish( &sdbExecState->queryDocument ) ;
}

Const *serializeString( const char *s )
{
   if ( s == NULL )
      return makeNullConst( TEXTOID, -1, InvalidOid ) ;
   else
      return makeConst( TEXTOID, -1, InvalidOid, -1, PointerGetDatum( cstring_to_text( s ) ), 0, 0 ) ;
}

Const *serializeUint64( UINT64 value )
{
   return makeConst( INT4OID, -1, InvalidOid, 8, Int64GetDatum( ( int64 )value ), 
      #ifdef USE_FLOAT8_BYVAL
          1,
      #else
          0,
      #endif  /* USE_FLOAT8_BYVAL */
          0 ) ;
}


List *serializeSdbExecState( SdbExecState *fdwState )
{
   List *result = NIL ;
   int i        = 0 ;

   /* sdbServerNum */
   result = lappend( result, serializeInt( fdwState->sdbServerNum ) ) ;

   /* sdbServerList */
   for ( i = 0 ; i < fdwState->sdbServerNum ; i++ )
   {
      result = lappend( result, serializeString( fdwState->sdbServerList[i]) ) ;
   }

   /* usr */
   result = lappend( result, serializeString( fdwState->usr ) ) ;
   /* passwd */
   result = lappend( result, serializeString( fdwState->passwd ) ) ;
   /* preferenceInstance */
   result = lappend( result, serializeString( fdwState->preferenceInstance ) ) ;

   /* sdbcs */
   result = lappend( result, serializeString( fdwState->sdbcs ) ) ;
   /* sdbcl */
   result = lappend( result, serializeString( fdwState->sdbcl ) ) ;

   /* table name */
   result = lappend( result, serializeString( fdwState->pgTableDesc->name ) ) ;
   /* number of columns in the table */
   result = lappend( result, serializeInt( fdwState->pgTableDesc->ncols ) ) ;

   /* column data */
   for ( i = 0 ; i<fdwState->pgTableDesc->ncols ; ++i )
   {
      result = lappend( result, serializeString( fdwState->pgTableDesc->cols[i].pgname ) ) ;
      result = lappend( result, serializeInt( fdwState->pgTableDesc->cols[i].pgattnum ) ) ;
      result = lappend( result, serializeOid( fdwState->pgTableDesc->cols[i].pgtype ) ) ;
      result = lappend( result, serializeInt( fdwState->pgTableDesc->cols[i].pgtypmod ) ) ;
   }

   /* queryDocument */
   result = lappend( result, sdbSerializeDocument( &fdwState->queryDocument ) ) ;

   /* _id_addr */
   result = lappend( result, serializeUint64( fdwState->bson_record_addr ) ) ;

   /* key_num */
   result = lappend( result, serializeInt( fdwState->key_num ) ) ;
   for ( i = 0 ; i < fdwState->key_num ; i++ )
   {
      result = lappend( result, serializeString( fdwState->key_name[i] ) ) ;
   }

   return result ;
}

SdbExecState *deserializeSdbExecState( List *sdbExecStateList )
{
   ListCell *cell = NULL ;
   int i = 0 ;
   SdbExecState *fdwState = ( SdbExecState* )palloc( sizeof( SdbExecState ) ) ;
   initSdbExecState( fdwState ) ;

   cell = list_head( sdbExecStateList ) ;

   /* sdbServerNum */
   fdwState->sdbServerNum = ( int )DatumGetInt32( ( ( Const * )lfirst( cell ) )->constvalue ) ;
   cell = lnext( cell ) ;

   /* sdbServerList */
   for ( i = 0 ; i < fdwState->sdbServerNum ; ++i )
   {
      fdwState->sdbServerList[i] = deserializeString( lfirst( cell ) ) ;
      cell = lnext( cell ) ;
   }

   /* usr */
   fdwState->usr = deserializeString( lfirst( cell ) ) ;
   cell = lnext( cell ) ;

   /* passwd */
   fdwState->passwd = deserializeString( lfirst( cell ) ) ;
   cell = lnext( cell ) ;

   /* preferenceInstance */
   fdwState->preferenceInstance = deserializeString( lfirst( cell ) ) ;
   cell = lnext( cell ) ;

   /* sdbcs */
   fdwState->sdbcs = deserializeString( lfirst( cell ) ) ;
   cell = lnext( cell ) ;
   /* sdbcl */
   fdwState->sdbcl = deserializeString( lfirst( cell ) ) ;
   cell = lnext( cell ) ;

   /* table name */
   fdwState->pgTableDesc = palloc( sizeof( PgTableDesc ) ) ;
   fdwState->pgTableDesc->name = deserializeString( lfirst( cell ) ) ;
   cell = lnext( cell ) ;

   /* number of columns in the table */
   fdwState->pgTableDesc->ncols= ( int )DatumGetInt32( ( ( Const * )lfirst( cell ) )->constvalue ) ;
   cell = lnext( cell ) ;

   fdwState->pgTableDesc->cols = palloc( fdwState->pgTableDesc->ncols * sizeof( PgColumnsDesc ) ) ;
   /* column data */
   for ( i = 0 ; i < fdwState->pgTableDesc->ncols ; ++i )
   {
      fdwState->pgTableDesc->cols[i].pgname = deserializeString( lfirst( cell ) ) ;
      cell = lnext( cell ) ;
      fdwState->pgTableDesc->cols[i].pgattnum = ( int )DatumGetInt32( ( ( Const * )lfirst( cell ) )->constvalue ) ;
      cell = lnext( cell ) ;
      fdwState->pgTableDesc->cols[i].pgtype = DatumGetObjectId( ( ( Const * )lfirst( cell ) )->constvalue ) ;
      cell = lnext( cell ) ;
      fdwState->pgTableDesc->cols[i].pgtypmod = ( int )DatumGetInt32( ( ( Const * )lfirst( cell ) )->constvalue ) ;
      cell = lnext( cell ) ;
   }

   sdbbson_init( &fdwState->queryDocument ) ;
   sdbDeserializeDocument( ( Const * )lfirst( cell ), &fdwState->queryDocument ) ;
   cell = lnext( cell ) ;

   /* _id_addr */
   fdwState->bson_record_addr = deserializeUint64( lfirst( cell ) ) ;
   cell = lnext( cell ) ;

   /* key_num */
   fdwState->key_num = ( int )DatumGetInt32( ( ( Const * )lfirst( cell ) )->constvalue ) ;
   cell = lnext( cell ) ;

   for ( i = 0 ; i < fdwState->key_num ; i++ )
   {
      strncpy( fdwState->key_name[i], deserializeString( lfirst( cell ) ), 
            SDB_MAX_KEY_COLUMN_LENGTH - 1 ) ;
      cell = lnext( cell ) ;
   }

   return fdwState ;
}

char *deserializeString( Const *constant )
{
   if ( constant->constisnull )
      return NULL ;
   else
      return text_to_cstring( DatumGetTextP( constant->constvalue ) ) ;
}

UINT64 deserializeUint64( Const *constant )
{
   return ( UINT64 )DatumGetInt64( constant->constvalue ) ;
}


/*long deserializeLong( Const *constant )
{
   if ( sizeof( long )<= 4 )
      return ( long )DatumGetInt32( constant->constvalue ) ;
   else
      return ( long )DatumGetInt64( constant->constvalue ) ;
}*/

int sdbSetBsonValue( sdbbson *bsonObj, const char *name, Datum valueDatum, 
         Oid columnType, INT32 columnTypeMod )
{
   INT32 rc = SDB_OK ;
   switch( columnType )
   {
      case INT2OID :
      {
         INT16 value = DatumGetInt16( valueDatum ) ;
         sdbbson_append_int( bsonObj, name, ( INT32 )value ) ;
         break ;
      }

      case INT4OID :
      {
         INT32 value = DatumGetInt32( valueDatum ) ;
         sdbbson_append_int( bsonObj, name, value ) ;
         break ;
      }

      case INT8OID :
      {
         INT64 value = DatumGetInt64( valueDatum ) ;
         sdbbson_append_long( bsonObj, name, value ) ;
         break ;
      }

      case FLOAT4OID :
      {
         FLOAT32 value = DatumGetFloat4( valueDatum ) ;
         sdbbson_append_double( bsonObj, name, ( FLOAT64 )value ) ;
         break ;
      }

      case FLOAT8OID :
      {
         FLOAT64 value = DatumGetFloat8( valueDatum ) ;
         sdbbson_append_double( bsonObj, name, value ) ;
         break ;
      }

      case NUMERICOID :
      {
         Datum valueDatum_tmp = DirectFunctionCall1( numeric_float8, valueDatum ) ;
         FLOAT64 value        = DatumGetFloat8( valueDatum_tmp ) ;
         sdbbson_append_double( bsonObj, name, value ) ;
         break ;
      }

      case BOOLOID :
      {
         BOOLEAN value = DatumGetBool( valueDatum ) ;
         sdbbson_append_bool( bsonObj, name, value ) ;
         break ;
      }

      case BPCHAROID :
      case VARCHAROID :
      case TEXTOID :
      {
         CHAR *outputString    = NULL ;
         Oid outputFunctionId  = InvalidOid ;
         bool typeVarLength    = false ;
         getTypeOutputInfo( columnType, &outputFunctionId, &typeVarLength ) ;
         outputString = OidOutputFunctionCall( outputFunctionId, valueDatum ) ;
         sdbbson_append_string( bsonObj, name, outputString ) ;
         break ;
      }

      case NAMEOID :
      {
         sdbbson_oid_t sdbbsonObjectId ;
         CHAR *outputString    = NULL ;
         Oid outputFunctionId  = InvalidOid ;
         bool typeVarLength    = false ;         
         getTypeOutputInfo( columnType, &outputFunctionId, &typeVarLength ) ;
         outputString = OidOutputFunctionCall( outputFunctionId, valueDatum ) ;

         memset( sdbbsonObjectId.bytes, 0, sizeof( sdbbsonObjectId.bytes ) ) ;
         sdbbson_oid_from_string( &sdbbsonObjectId, outputString ) ;
         sdbbson_append_oid( bsonObj, name, &sdbbsonObjectId ) ;
         break ;
      }

      case DATEOID :
      {
         Datum valueDatum_tmp = DirectFunctionCall1( date_timestamptz, valueDatum ) ;
         Timestamp valueTimestamp = DatumGetTimestamp( valueDatum_tmp ) ;
         INT64 valueUsecs         = valueTimestamp + POSTGRES_TO_UNIX_EPOCH_USECS ;
         INT64 valueMilliSecs     = valueUsecs / SDB_MSECS_TO_USECS ;

         /* here store the UTC time */
         sdbbson_append_date( bsonObj, name, valueMilliSecs ) ;
         break ;
      }

      case TIMESTAMPOID :
      case TIMESTAMPTZOID :
      {
         Datum valueDatum_tmp = DirectFunctionCall1( timestamp_timestamptz, valueDatum ) ;
         Timestamp valueTimestamp = DatumGetTimestamp( valueDatum_tmp ) ;
         INT64 valueUsecs         = valueTimestamp + POSTGRES_TO_UNIX_EPOCH_USECS ;
         sdbbson_timestamp_t bson_time ;
         bson_time.t = valueUsecs/SDB_SECONDS_TO_USECS ;
         bson_time.i = valueUsecs%SDB_SECONDS_TO_USECS ;

         sdbbson_append_timestamp( bsonObj, name, &bson_time ) ;
         break ;
      }
      case BYTEAOID :
      {
			CHAR *buff = VARDATA( ( bytea * )DatumGetPointer( valueDatum ) );
			INT32 len  = VARSIZE( ( bytea * )DatumGetPointer( valueDatum ) ) 
			             - VARHDRSZ;
         sdbbson_append_binary( bsonObj, name, BSON_BIN_BINARY, buff, len ) ;
         break ;
      }

      case TEXTARRAYOID:
      case INT4ARRAYOID:
      case FLOAT4ARRAYOID:
      case 1022:
      /* FLOAT8ARRAY is not support */
      case INT2ARRAYOID:
      /* this type do not have type name, so we must use the value(see more types in pg_type.h) */ 
      case 1115:
      case 1182:
      case 1014 :
      {
         Datum datumTmp ;
         bool isNull            = false ;
         ArrayType *arr         = DatumGetArrayTypeP( valueDatum ) ;
         ArrayIterator iterator = array_create_iterator( arr , 0 ) ;
         Oid element_type       = ARR_ELEMTYPE( arr ) ;

         sdbbson_append_start_array( bsonObj, name ) ;
         while ( array_iterate( iterator, &datumTmp, &isNull ) )
         {
            rc = sdbSetBsonValue( bsonObj, "", datumTmp, element_type, 0 ) ;
            if ( SDB_OK != rc )
            {
               break ;
            }
         }
         sdbbson_append_finish_array( bsonObj ) ;

         array_free_iterator(iterator);

         break ;
      }

      default :
      {
         /* we do not support other data types */
         ereport ( WARNING, ( errcode( ERRCODE_FDW_INVALID_DATA_TYPE ),
            errmsg( "Cannot convert constant value to BSON" ), 
            errhint( "Constant value data type: %u", columnType ) ) ) ;

         return -1 ;
      }
   }

   return rc ;
}

UINT64 sdbCreateBsonRecordAddr(  )
{
   sdbbson *p = malloc( sizeof( sdbbson ) ) ;
   sdbbson_init( p ) ;

   return ( UINT64 )p ;
}

sdbbson *sdbGetRecordPointer( UINT64 record_addr )
{
   sdbbson *p = ( sdbbson * )record_addr ;

   return p ;
}

void sdbGetColumnKeyInfo( SdbExecState *fdw_state )
{
   int rc = SDB_OK ;
   sdbConnectionHandle connection ;
   sdbCursorHandle cursor ;
   sdbbson condition ;
   sdbbson selector ;
   sdbbson ShardingKey ;

   sdbbson_iterator ite ;
   sdbbson_type type ;

   StringInfo fullCollectionName = makeStringInfo(  ) ;
   appendStringInfoString( fullCollectionName, fdw_state->sdbcs ) ;
   appendStringInfoString( fullCollectionName, "." ) ;
   appendStringInfoString( fullCollectionName, fdw_state->sdbcl ) ;

   /* first add the _id to the key_name */
   fdw_state->key_num     = 1 ;
   strncpy( fdw_state->key_name[0], SDB_COLUMNA_ID_NAME, 
         SDB_MAX_KEY_COLUMN_LENGTH - 1 ) ;

   /* get the cs.cl's shardingkey */
   connection = sdbGetConnectionHandle( (const char **)fdw_state->sdbServerList, 
                                        fdw_state->sdbServerNum, 
                                        fdw_state->usr, fdw_state->passwd, 
                                        fdw_state->preferenceInstance ) ;

   sdbbson_init( &condition ) ;
   sdbbson_append_string( &condition, "Name", fullCollectionName->data ) ;
   rc = sdbbson_finish( &condition ) ;
   if ( SDB_OK != rc )
   {
      sdbbson_destroy( &condition ) ;
      ereport( ERROR, ( errcode( ERRCODE_FDW_ERROR ), 
               errmsg( "sdbbson_finish failed:rc=%d", rc ) ) ) ;
      return ;
   }

   sdbbson_init( &selector ) ;
   sdbbson_append_string( &selector, SDB_SHARDINGKEY_NAME, "" ) ;
   rc = sdbbson_finish( &selector ) ;
   if ( SDB_OK != rc )
   {
      sdbbson_destroy( &condition ) ;
      sdbbson_destroy( &selector ) ;
      ereport( ERROR, ( errcode( ERRCODE_FDW_ERROR ), 
            errmsg( "sdbbson_finish failed:rc=%d", rc ) ) ) ;
      return ;
   }

   rc = sdbGetSnapshot( connection, SDB_SNAP_CATALOG, &condition, &selector, 
         NULL, &cursor ) ;
   if ( SDB_OK != rc )
   {
      if ( rc != SDB_RTN_COORD_ONLY )
      {
         sdbPrintBson( &condition, WARNING ) ;
         sdbPrintBson( &selector, WARNING ) ;
         ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
                  errmsg( "sdbGetSnapshot failed:rc=%d", rc ) ) ) ;
      }

      sdbbson_destroy( &condition ) ;
      sdbbson_destroy( &selector ) ;
      return ;
   }

   sdbbson_init( &ShardingKey ) ;
   rc = sdbNext( cursor, &ShardingKey ) ;
   if( rc )
   {
      sdbbson_destroy( &condition ) ;
      sdbbson_destroy( &selector ) ;
      sdbbson_destroy( &ShardingKey ) ;
      return ;
   }

   type = sdbbson_find( &ite, &ShardingKey, SDB_SHARDINGKEY_NAME ) ;
   if ( BSON_OBJECT == type )
   {
      sdbbson tmpValue ;
      sdbbson_init( &tmpValue ) ;
      sdbbson_iterator_subobject( &ite, &tmpValue ) ;
      sdbbson_iterator_init( &ite, &tmpValue ) ;
      while ( sdbbson_iterator_next( &ite ) )
      {
         const CHAR *sdbbsonKey = sdbbson_iterator_key( &ite ) ;

         strncpy( fdw_state->key_name[fdw_state->key_num], 
               sdbbsonKey, SDB_MAX_KEY_COLUMN_LENGTH - 1 ) ;
         fdw_state->key_num++ ;
      }
      sdbbson_destroy( &tmpValue ) ;
   }

   sdbbson_destroy( &condition ) ;
   sdbbson_destroy( &selector ) ;
   sdbbson_destroy( &ShardingKey ) ;

   sdbCloseCursor( cursor ) ;
}

bool sdbIsShardingKeyChanged( SdbExecState *fdw_state, sdbbson *oldBson, 
      sdbbson *newBson )
{
   int i = 1 ;
   sdbbson_iterator oldIte ;
   sdbbson oldSubBson ;
   sdbbson_type oldType ;
   int oldSize ;

   sdbbson_iterator newIte ;
   sdbbson newSubBson ;
   sdbbson_type newType ;
   int newSize ;

   /* shardingKey start from 1( 0 is for the "_id" )*/
   for ( i = 1 ; i < fdw_state->key_num ; i++ )
   {
      sdbbson_init( &newSubBson ) ;
      newType = sdbbson_find( &newIte, newBson, fdw_state->key_name[i] ) ;
      if ( BSON_EOO == newType )
      {
         /* not in the newBson, so no change */
         continue ;
      }
      sdbbson_append_element( &newSubBson, NULL, &newIte ) ;
      sdbbson_finish( &newSubBson ) ;

      sdbbson_init( &oldSubBson ) ;
      oldType = sdbbson_find( &oldIte, oldBson, fdw_state->key_name[i] ) ;
      if ( BSON_EOO == oldType )
      {
         /* not in the oldBson, but appear in the newBson, 
            this means the shardingKey is changed */
         ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
               errmsg( "the shardingKey is changed, shardingKey:" ) ) ) ;
         sdbPrintBson( &newSubBson, WARNING ) ;
         sdbbson_destroy( &newSubBson ) ;
         sdbbson_destroy( &oldSubBson ) ;
         return true ;
      }
      sdbbson_append_element( &oldSubBson, NULL, &oldIte ) ;
      sdbbson_finish( &oldSubBson ) ;

      oldSize = sdbbson_size( &oldSubBson ) ;
      newSize = sdbbson_size( &newSubBson ) ;
      if ( ( oldSize != newSize )|| 
            ( 0 != memcmp( newSubBson.data, oldSubBson.data, oldSize ) ) )
      {
         ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
               errmsg( "the shardingKey is changed, old shardingKey:" ) ) ) ;
         sdbPrintBson( &oldSubBson, WARNING ) ;
         sdbbson_destroy( &oldSubBson ) ;

         ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
               errmsg( "the shardingKey is changed, new shardingKey:" ) ) ) ;
         sdbPrintBson( &newSubBson, WARNING ) ;
         sdbbson_destroy( &newSubBson ) ;

         return true ;
      }

      sdbbson_destroy( &newSubBson ) ;
      sdbbson_destroy( &oldSubBson ) ;
   }

   return false ;
}

UINT64 sdbbson_iterator_getusecs( sdbbson_iterator *ite )
{
   sdbbson_type type = sdbbson_iterator_type( ite ) ;
   if ( BSON_DATE == type )
   {
      return ( sdbbson_iterator_date( ite )* SDB_MSECS_TO_USECS ) ;
   }
   else
   {
      sdbbson_timestamp_t timestamp = sdbbson_iterator_timestamp( ite ) ;
      return ( ( timestamp.t * SDB_SECONDS_TO_USECS )+ timestamp.i ) ;
   }
}

bool isAllTwoArgumentVarType( List *arguments )
{
   INT32 varCount         = 0 ;
   ListCell *argumentCell = NULL ;

   foreach( argumentCell, arguments )
   {
      Expr *argument = ( Expr * )lfirst( argumentCell ) ;
      if( nodeTag( argument )!= T_Var )
      {
         return false ;
      }

      varCount++ ;
   }

   if ( varCount > 2 )
   {
      return false ;
   }

   return true ;
}

INT32 sdbRecurOperExprTwoVar( OpExpr *opr_two_argument, SdbExprTreeState *expr_state, 
                              sdbbson *condition )
{
   INT32 rc               = SDB_OK ;
   char *pgOpName         = NULL ;
   const CHAR *sdbOpName  = NULL ;
   char *columnName1      = NULL ;
   char *columnName2      = NULL ;
   ListCell *argumentCell = NULL ;
   Var *argument1         = NULL ;
   Var *argument2         = NULL ;
   INT32 count            = 0 ;

   sdbbson temp1 ;
   sdbbson temp2 ;

   foreach( argumentCell, opr_two_argument->args )
   {
      if ( count == 0 )
      {
         argument1 = ( Var * )lfirst( argumentCell ) ;
         if ( ( argument1->varno != expr_state->foreign_table_index )
          || ( argument1->varlevelsup != 0 ) )
         {
            elog( DEBUG1, "column is not reconigzed:table_index=%d, varno=%d, "
                  "valevelsup=%d", expr_state->foreign_table_index, 
                  argument1->varno, argument1->varlevelsup ) ;
            goto error ;
         }
      }
      else
      {
         argument2 = ( Var * )lfirst( argumentCell ) ;
         if ( ( argument2->varno != expr_state->foreign_table_index )
                   || ( argument2->varlevelsup != 0 ) )
         {
            elog( DEBUG1, "column is not reconigzed:table_index=%d, varno=%d, "
                  "valevelsup=%d", expr_state->foreign_table_index, 
                  argument2->varno, argument2->varlevelsup ) ;
            goto error ;
         }
      }

      count++ ;
   }
   /* the caller make sure the argument have two var! */

   pgOpName    = get_opname( opr_two_argument->opno ) ;
   sdbOpName   = sdbOperatorName( pgOpName ) ;
   if( !sdbOpName )
   {
      elog( DEBUG1, "operator is not supported:op=%s", pgOpName ) ;
      goto error ;
   }

   columnName1 = get_relid_attribute_name( expr_state->foreign_table_id, 
                                                argument1->varattno ) ;
   columnName2 = get_relid_attribute_name( expr_state->foreign_table_id, 
                                                argument2->varattno ) ;

   sdbbson_init( &temp1 ) ;
   sdbbson_append_string( &temp1, "$field", columnName2 ) ;
   sdbbson_finish( &temp1 ) ;

   sdbbson_init( &temp2 ) ;
   sdbbson_append_sdbbson( &temp2, sdbOpName, &temp1 ) ;
   sdbbson_finish( &temp2 ) ;
   sdbbson_destroy( &temp1 ) ;

   sdbbson_append_sdbbson( condition, columnName1, &temp2 ) ;
   sdbbson_destroy( &temp2 ) ;

done:
   return rc ;
error:
   rc = -1 ;
   goto done ;

}

INT32 sdbRecurOperExpr( OpExpr *opr, SdbExprTreeState *expr_state, 
                        sdbbson *condition )
{
   INT32 rc              = SDB_OK ;
   char *pgOpName        = NULL ;
   const CHAR *sdbOpName = NULL ;
   char *columnName      = NULL ;
   Var *var              = NULL ;
   Const *const_val      = NULL ;
   bool need_not_condition = false ;

   if ( isAllTwoArgumentVarType( opr->args ) )
   {
      rc = sdbRecurOperExprTwoVar( opr, expr_state, condition ) ;
      if ( rc != SDB_OK )
      {
         goto error ;
      }
      else
      {
         goto done ;
      }
   }

   pgOpName  = get_opname( opr->opno ) ;
   /* we only support > >= < <= <> = ~~ */
   sdbOpName = sdbOperatorName( pgOpName ) ;
   if( !sdbOpName )
   {
      elog( DEBUG1, "operator is not supported:op=%s", pgOpName ) ;
      goto error ;
   }

   /* first find the key( column )*/
   var = ( Var * )sdbFindArgumentOfType( opr->args, T_Var ) ;
   if ( NULL == var )
   {
      /* var must exist */
      elog( DEBUG1, " Var is null " ) ;
      goto error ;
   }
   elog( DEBUG1, "var info:table_index=%d, varno=%d, valevelsup=%d", 
                 expr_state->foreign_table_index, var->varno, var->varlevelsup ) ;
   if ( ( var->varno != expr_state->foreign_table_index )
          || ( var->varlevelsup != 0 ) )
   {
      elog( DEBUG1, "column is not reconigzed:table_index=%d, varno=%d, valevelsup=%d", 
            expr_state->foreign_table_index, var->varno, var->varlevelsup ) ;
      goto error ;
   }

   columnName = get_relid_attribute_name( expr_state->foreign_table_id, 
                                                var->varattno ) ;

   const_val = ( Const * )sdbFindArgumentOfType( opr->args, T_Const ) ;
   if ( NULL == const_val || ( InvalidOid != get_element_type( const_val->consttype ) ) )
   {
      /* const must exist and const is not array */
      /* get_element_type(  )!= InvalidOid indicate const_val is array */
      elog( DEBUG1, " const_val is null " ) ;
      goto error ;
   }

   sdbbson_append_start_object( condition, columnName ) ;
   sdbAppendConstantValue( condition, sdbOpName, const_val ) ;
   sdbbson_append_finish_object( condition ) ;

   if ( need_not_condition )
   {
      sdbbson temp ;
      sdbbson_init( &temp ) ;
      sdbbson_finish( condition ) ;
      sdbbson_copy( &temp, condition ) ;

      sdbbson_destroy( condition ) ;
      sdbbson_init( condition ) ;

      sdbbson_append_start_array( condition, "$not" ) ;
      sdbbson_append_sdbbson( condition, "", &temp ) ;
      sdbbson_append_finish_array( condition ) ;
      sdbbson_destroy( &temp ) ;
   }

done:
   return rc ;
error:
   rc = -1 ;
   goto done ;
}

INT32 sdbRecurScalarArrayOpExpr( ScalarArrayOpExpr *scalaExpr, 
                                 SdbExprTreeState *expr_state, 
                                 sdbbson *condition )
{
   INT32 rc         = SDB_OK ;
   char *pgOpName   = NULL ;
   char *keyName    = NULL ;
   Var *var         = NULL ;
   char *columnName = NULL ;
   ListCell *cell   = NULL ;
   int varCount     = 0 ;
   int constCount   = 0 ;

   pgOpName = get_opname( scalaExpr->opno ) ;
   if ( strcmp( pgOpName, "=" )== 0 )
   {
      keyName = "$in" ;
   }
   else if ( strcmp( pgOpName, "<>" )== 0 )
   {
      keyName = "$nin" ;
   }
   else
   {
      elog( DEBUG1, "operator is not supported:op=%d,str=%s", 
            scalaExpr->opno, pgOpName ) ;
      goto error ;
   }

   /* first find the key( column )*/
   var = ( Var * )sdbFindArgumentOfType( scalaExpr->args, T_Var ) ;
   if ( NULL == var )
   {
      /* var must exist */
      elog( DEBUG1, " Var is null " ) ;
      goto error ;
   }

   if ( ( var->varno != expr_state->foreign_table_index )
          || ( var->varlevelsup != 0 ) )
   {
      elog( DEBUG1, "column is not reconigzed:table_index=%d, varno=%d, valevelsup=%d", 
            expr_state->foreign_table_index, var->varno, var->varlevelsup ) ;
      goto error ;
   }

   columnName = get_relid_attribute_name( expr_state->foreign_table_id, 
                                             var->varattno ) ;
   foreach( cell, scalaExpr->args )
   {
      Node *oprarg = lfirst( cell ) ;
      if ( T_Const == oprarg->type )
      {
         sdbbson temp ;
         Const *const_val = ( Const * )oprarg ;
         sdbbson_init( &temp ) ;
         rc = sdbSetBsonValue( &temp, keyName, const_val->constvalue, 
                               const_val->consttype, const_val->consttypmod ) ;
         if ( SDB_OK != rc )
         {
            sdbbson_destroy( &temp ) ;
            ereport ( WARNING, ( errcode( ERRCODE_FDW_INVALID_DATA_TYPE ),
                      errmsg( "convert value failed:key=%s", keyName ) ) ) ;
            goto error ;
         }
         sdbbson_finish( &temp ) ;
         sdbbson_append_sdbbson( condition, columnName, &temp ) ;
         sdbbson_destroy( &temp ) ;

         constCount++ ;
      }
      else if ( T_Var == oprarg->type )
      {
         varCount++ ;
      }
      else
      {
         goto error ;
      }
   }

   if ( ( varCount != 1 )|| ( constCount != 1 ) )
   {
      elog( DEBUG1, "parameter count error:varCount=%d, constCount=%d", 
            varCount, constCount ) ;
      goto error ;
   }

done:
   return rc ;
error:
   rc = -1 ;
   goto done ;
}

INT32 sdbRecurNullTestExpr( NullTest *ntest, SdbExprTreeState *expr_state, 
                              sdbbson *condition )
{
   INT32 rc         = SDB_OK ;
   char *columnName = NULL ;
   Var *var         = NULL ;
   sdbbson isNullcondition ;
   AttrNumber columnId ;

   if ( ntest->argisrow )
   {
      elog( DEBUG1, "argisrow is true" ) ;
      goto error ;
   }

   if ( T_Var != ( ( Expr * )( ntest->arg ) )->type )
   {
      elog( DEBUG1, "argument is not var:type=%d", ( ( Expr * )( ntest->arg ) )->type ) ;
      goto error ;
   }

   var = ( Var * )ntest->arg ;
   if ( ( var->varno != expr_state->foreign_table_index )
          || ( var->varlevelsup != 0 ) )
   {
      elog( DEBUG1, "column is not reconigzed:table_index=%d, varno=%d, valevelsup=%d", 
            expr_state->foreign_table_index, var->varno, var->varlevelsup ) ;
      goto error ;
   }

   columnId = var->varattno ;
   columnName    = get_relid_attribute_name( expr_state->foreign_table_id,
                                                 columnId ) ;
   sdbbson_init( &isNullcondition ) ;
   switch ( ntest->nulltesttype )
   {
      case IS_NULL:
         sdbbson_append_int( &isNullcondition, "$isnull", 1 ) ;
         break ;
      case IS_NOT_NULL:
         sdbbson_append_int( &isNullcondition, "$isnull", 0 ) ;

         break ;
      default:
         sdbbson_destroy( &isNullcondition ) ;
         elog( DEBUG1, "nulltesttype error:type=%d", ntest->nulltesttype ) ;
         goto error ;
   }

   sdbbson_finish( &isNullcondition ) ;
   sdbbson_append_sdbbson( condition, columnName, &isNullcondition ) ;
   sdbbson_destroy( &isNullcondition ) ;

done:
   return rc ;
error:
   rc = -1 ;
   goto done ;
}

INT32 sdbRecurBoolExpr( BoolExpr *boolexpr, SdbExprTreeState *expr_state, sdbbson *condition )
{
   INT32 rc = SDB_OK ;
   ListCell *cell ;
   char *key = NULL ;

   switch ( boolexpr->boolop )
   {
      case AND_EXPR:
         key = "$and" ;
         break ;

      case OR_EXPR:
         key = "$or" ;
         break ;

      case NOT_EXPR:
         key = "$not" ;
         break ;
      default:
         elog( DEBUG1, "unsupported boolean expression type:type=%d", 
            boolexpr->boolop ) ;
         goto error ;
   }

   sdbbson_append_start_array( condition, key ) ;
   foreach( cell, boolexpr->args )
   {
      Node *bool_arg = ( Node * )lfirst ( cell ) ;
      sdbbson sub_condition ;
      sdbbson_init( &sub_condition ) ;
      sdbRecurExprTree( bool_arg, expr_state, &sub_condition ) ;
      sdbbson_finish( &sub_condition ) ;

      sdbbson_append_sdbbson( condition, "", &sub_condition ) ;
      sdbbson_destroy( &sub_condition ) ;
   }

   sdbbson_append_finish_array( condition ) ;

done:
   return rc ;

error:
   rc = -1 ;
   goto done ;
}

/* This is a recurse function to walk over the tree */
INT32 sdbRecurExprTree( Node *node, SdbExprTreeState *expr_state, sdbbson *condition )
{
   INT32 rc = SDB_OK ;
   if( NULL == node )
   {
      goto done ;
   }

   if( IsA( node, BoolExpr ) )
   {
      rc = sdbRecurBoolExpr( ( BoolExpr * )node, expr_state, condition ) ;
      if ( rc != SDB_OK )
      {
         elog( DEBUG1, "sdbRecurBoolExpr" ) ;
         goto error ;
      }
   }
   else if ( IsA( node, NullTest ) )
   {
      rc = sdbRecurNullTestExpr( ( NullTest * )node, expr_state, condition ) ;
      if ( rc != SDB_OK )
      {
         elog( DEBUG1, "sdbRecurNullTestExpr" ) ;
         goto error ;
      }
   }
   /*ScalarArrayOpExpr*/
   else if ( IsA( node, ScalarArrayOpExpr ) )
   {
      rc = sdbRecurScalarArrayOpExpr( ( ScalarArrayOpExpr * )node, 
                                       expr_state, condition ) ;
      if ( rc != SDB_OK )
      {
         elog( DEBUG1, "sdbRecurScalarArrayOpExpr" ) ;
         goto error ;
      }
   }
   else if ( IsA( node, OpExpr ) )
   {
      rc = sdbRecurOperExpr( ( OpExpr * )node, expr_state, condition ) ;
      if ( rc != SDB_OK )
      {
         elog( DEBUG1, "sdbRecurOperExpr" ) ;
         goto error ;
      }
   }
   else
   {
      elog( DEBUG1, "node type is not supported:type=%d", nodeTag( node ) ) ;
      goto error ;
   }

done:
   return SDB_OK ;

error:
   expr_state->unsupport_count++ ;
   goto done ;

}

INT32 sdbGenerateFilterCondition ( Oid foreign_id, RelOptInfo *baserel, sdbbson *condition )
{
   ListCell *cell = NULL ;
   INT32 rc       = SDB_OK ;
   SdbExprTreeState expr_state ;

   memset( &expr_state, 0, sizeof( SdbExprTreeState ) ) ;
   expr_state.foreign_table_index = baserel->relid ;
   expr_state.foreign_table_id    = foreign_id ;

   sdbbson_destroy( condition ) ;
   sdbbson_init( condition ) ;
   foreach( cell, baserel->baserestrictinfo )
   {
      RestrictInfo *info =( RestrictInfo * )lfirst( cell ) ;
      sdbRecurExprTree( ( Node * )info->clause, &expr_state, condition ) ;
      if( expr_state.unsupport_count > 0 )
      {
         sdbbson_destroy( condition ) ;
         sdbbson_init( condition ) ;
         break ;
      }
   }

   rc = sdbbson_finish( condition ) ;
   if ( SDB_OK != rc )
   {
      sdbbson_destroy( condition ) ;
   }

   sdbPrintBson( condition, DEBUG1 ) ;

   return SDB_OK ;
}


/* connection pool */
SdbConnectionPool *sdbGetConnectionPool (  )
{
   static SdbConnectionPool connPool ;
   return &connPool ;
}

static void sdbInitConnectionPool (  )
{
   SdbConnection *pNewMem  = NULL ;
   SdbConnectionPool *pool = sdbGetConnectionPool(  ) ;
   memset( pool, 0, sizeof( SdbConnectionPool ) ) ;
   pNewMem = ( SdbConnection* )malloc( sizeof( SdbConnection )*
                                      INITIAL_ARRAY_CAPACITY ) ;
   if( !pNewMem )
   {
      ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ),
                         errmsg( "Unable to allocate connection pool" ),
                         errhint( "Make sure the memory pool or ulimit is "
                                   "properly configured" ) ) ) ;
      goto error ;
   }
   pool->connList = pNewMem ;
   pool->poolSize = INITIAL_ARRAY_CAPACITY ;
error :
   return ;
}

static void sdbUninitConnectionPool (  )
{
   INT32 count             = 0 ;
   SdbConnectionPool *pool = sdbGetConnectionPool(  ) ;
   for( count = 0 ; count < pool->numConnections ; ++count )
   {
      SdbConnection *conn = &pool->connList[count] ;
      if( conn->connName )
      {
         free( conn->connName ) ;
      }
      sdbDisconnect( conn->hConnection ) ;
      sdbReleaseConnection( conn->hConnection ) ;
   }
   free( pool->connList ) ;
}

/* sdbSerializeDocument serializes the sdbbson document to a constant
 * Note this function just copies the pointer of documents' data,
 * therefore the caller should NOT destroy the object
 */
Const *sdbSerializeDocument( sdbbson *document )
{
   Const *serializedDocument = NULL ;
   Datum documentDatum       = 0 ;
   const CHAR *documentData  = sdbbson_data( document ) ;
   INT32 documentSize        = sdbbson_buffer_size( document ) ;
   documentDatum             = CStringGetDatum( documentData ) ;
   serializedDocument        = makeConst( CSTRINGOID, -1,
                                           InvalidOid, documentSize,
                                           documentDatum, FALSE, FALSE ) ;
   return serializedDocument ;
}

/* sdbDeserializeDocument deserializes a constant into sdbbson document
 */
void sdbDeserializeDocument( Const *constant,
                                     sdbbson *document )
{
   Datum documentDatum = constant->constvalue ;
   CHAR *documentData = DatumGetCString( documentDatum ) ;
   sdbbson_init_size( document, 0 ) ;
   sdbbson_init_finished_data( document, documentData ) ;
   return ;
}

void sdbPrintBson( sdbbson *bson, int log_level )
{
   int bufferSize = 0 ;
   char *p        = NULL ;

   bufferSize = sdbbson_sprint_length( bson ) ;
   p = ( char* )malloc( bufferSize ) ;
   sdbbson_sprint( p, bufferSize, bson ) ;

   ereport( log_level, ( errcode( ERRCODE_FDW_ERROR ), 
           errmsg( "bson value=%s", p ) ) ) ;

   free( p ) ;
}

/* sdbOperatorName converts PG comparison operator to Sdb
 */
const CHAR *sdbOperatorName( const CHAR *operatorName )
{
   const CHAR *pResult                  = NULL ;
   const INT32 totalNames               = 6 ;
   static const CHAR *nameMappings[][2] = {
      { "<",   "$lt"    },
      { "<=",  "$lte"   },
      { ">",   "$gt"    },
      { ">=",  "$gte"   },
      { "=",   "$et"    },
      { "<>",  "$ne"    }
   } ;

   INT32 i = 0 ;
   for( i = 0 ; i < totalNames ; ++i )
   {
      if( strncmp( nameMappings[i][0],
                     operatorName, NAMEDATALEN )== 0 )
      {
         pResult = nameMappings[i][1] ;
         break ;
      }
   }
   return pResult ;
}

/* sdbFindArgumentOfType iterate the given argument list, looks for an argument
 * with the given type and returns the argument if found
 */
Expr *sdbFindArgumentOfType( List *argumentList, NodeTag argumentType )
{
   Expr *foundArgument = NULL ;
   ListCell *argumentCell = NULL ;
   foreach( argumentCell, argumentList )
   {
      Expr *argument = ( Expr * )lfirst( argumentCell ) ;
      if( nodeTag( argument )== argumentType )
      {
         foundArgument = argument ;
         break ;
      }
      else if ( nodeTag( argument ) == T_RelabelType )
      {
         RelabelType *relabel = (RelabelType *)argument ;
         if ( nodeTag( relabel->arg ) == argumentType )
         {
            foundArgument = relabel->arg ;
            break ;
         }
      }
   }
   return foundArgument ;
}

/* sdbAppendConstantValue appends to query document with key and value */
void sdbAppendConstantValue ( sdbbson *bsonObj, const char *keyName, 
      Const *constant )
{
   int rc = SDB_OK ;

   if ( constant->constisnull )
   {
      /* this matches null and not exists for both table and index scan */
      sdbbson_append_int( bsonObj, "$isnull", 1 ) ;
      return ;
   }

   rc = sdbSetBsonValue( bsonObj, keyName, constant->constvalue, 
         constant->consttype, constant->consttypmod ) ;
   if ( SDB_OK != rc )
   {
      ereport ( WARNING, ( errcode( ERRCODE_FDW_INVALID_DATA_TYPE ),
                errmsg( "convert value failed:key=%s", keyName ) ) ) ;
   }

}

/* sdbApplicableOpExpressionList iterate all operators and push the predicates
 * that's able to handled by sdb into sdb
 */
static List *sdbApplicableOpExpressionList( RelOptInfo *baserel )
{
   List *opExpressionList     = NIL ;
   List *restrictInfoList     = baserel->baserestrictinfo ;
   ListCell *restrictInfoCell = NULL ;

   foreach( restrictInfoCell, restrictInfoList )
   {
      RestrictInfo *restrictInfo = ( RestrictInfo * )lfirst( restrictInfoCell ) ;
      Expr *expression           = restrictInfo->clause ;
      NodeTag expressionType     = 0 ;
      OpExpr *opExpression       = NULL ;
      CHAR *operatorName         = NULL ;
      const CHAR *sdbOpName      = NULL ;
      List *argumentList         = NIL ;
      Var *column                = NULL ;
      Const *constant            = NULL ;
      BOOLEAN constantIsArray    = FALSE ;

      /* we only support operator expressions */
      expressionType = nodeTag( expression ) ;
      if( T_OpExpr != expressionType )
      {
         continue ;
      }
      opExpression = ( OpExpr* )expression ;
      operatorName = get_opname( opExpression->opno ) ;
      /* we only support > >= < <= <> = */
      sdbOpName    = sdbOperatorName( operatorName ) ;
      if( !sdbOpName )
      {
         continue ;
      }
      /* we only support simple binary operators */
      argumentList = opExpression->args ;
      column = ( Var* )sdbFindArgumentOfType( argumentList, T_Var ) ;
      constant = ( Const* )sdbFindArgumentOfType( argumentList, T_Const ) ;
      /* we skip array */
      if( NULL != constant )
      {
         Oid constantArrayTypeId = get_element_type( constant->consttype ) ;
         if( constantArrayTypeId != InvalidOid )
         {
            constantIsArray = TRUE ;
         }
      }
      if( NULL != column && NULL != constant && !constantIsArray )
      {
         opExpressionList = lappend( opExpressionList, opExpression ) ;
      }
   }
   return opExpressionList ;
}

/* sdbColumnList takes the planner's information and extract all columns that
 * may be used in projections, joins, and filter clauses, de-duplicate those
 * columns and returns them in a new list
 */
static List *sdbColumnList( RelOptInfo *baserel )
{
   List *columnList           = NIL ;
   List *neededColumnList     = NIL ;
   AttrNumber columnIndex     = 1 ;
   AttrNumber columnCount     = baserel->max_attr ;
   List *targetColumnList     = baserel->reltargetlist ;
   List *restrictInfoList     = baserel->baserestrictinfo ;
   ListCell *restrictInfoCell = NULL ;
   /* first add the columns used in joins and projections */
   neededColumnList = list_copy( targetColumnList ) ;
   /* then walk over all restriction clauses, and pull up any used columns */
   foreach( restrictInfoCell, restrictInfoList )
   {
      RestrictInfo *restrictInfo = ( RestrictInfo * )lfirst( restrictInfoCell ) ;
      Node *restrictClause       = ( Node * )restrictInfo->clause ;
      List *clauseColumnList     = NIL ;
      /* recursively pull up any columns used in the restriction clauses */
      clauseColumnList = pull_var_clause( restrictClause ,
                                           PVC_RECURSE_AGGREGATES,
                                           PVC_RECURSE_PLACEHOLDERS ) ;
      neededColumnList = list_union( neededColumnList, clauseColumnList ) ;
   }
   /* walk over all column definitions and deduplicate column list */
   for ( columnIndex = 1 ; columnIndex <= columnCount ; columnIndex++ )
   {
      ListCell *neededColumnCell = NULL ;
      Var *column = NULL ;
      foreach( neededColumnCell, neededColumnList )
      {
         Var *neededColumn = ( Var * )lfirst( neededColumnCell ) ;
         if( neededColumn->varattno == columnIndex )
         {
            column = neededColumn ;
            break ;
         }
      }
      if( NULL != column )
      {
         columnList = lappend( columnList, column ) ;
      }
   }
   return columnList ;
}

/* Iterate SdbInputOptionList and return all possible option name for
 * the given context id
 */
static StringInfo sdbListOptions( const Oid currentContextID )
{
   StringInfo resultString = makeStringInfo (  ) ;
   INT32 firstPrinted      = 0 ;
   INT32 i                 = 0 ;
   for( i = 0 ; i < sizeof( SdbInputOptionList ) ; ++i )
   {
      const SdbInputOption *inputOption = &( SdbInputOptionList[i] ) ;
      /* if currentContextID matches the context, then let's append */
      if( currentContextID == inputOption->optionContextID )
      {
         /* append ", " if it's not the first element */
         if( firstPrinted )
         {
            appendStringInfoString( resultString, ", " ) ;
         }
         appendStringInfoString( resultString, inputOption->optionName ) ;
         firstPrinted = 1 ;
      }
   }
   return resultString ;
}

/* sdbGetOptionValue retrieve options info from PG catalog */
static CHAR *sdbGetOptionValue( Oid foreignTableId, const CHAR *optionName )
{
   ForeignTable *ft     = NULL ;
   ForeignServer *fs    = NULL ;
   List *optionList     = NIL ;
   ListCell *optionCell = NULL ;
   CHAR *optionValue    = NULL ;
   /* retreive foreign table and server in order to get options */
   ft  = GetForeignTable( foreignTableId ) ;
   fs = GetForeignServer( ft->serverid ) ;
   /* add server and table options into list */
   optionList    = list_concat( optionList, ft->options ) ;
   optionList    = list_concat( optionList, fs->options ) ;
   /* iterate each option and match the option name */
   foreach( optionCell, optionList )
   {
      DefElem *optionDef = ( DefElem * )lfirst( optionCell ) ;
      if( strncmp( optionDef->defname, optionName, NAMEDATALEN )== 0 )
      {
         optionValue = defGetString( optionDef ) ;
         goto done ;
      }
   }
done :
   return optionValue ;
}


/* getOptions retreive connection options from Oid */
void sdbGetOptions( Oid foreignTableId, SdbInputOptions *options )
{
   CHAR *addressName         = NULL ;
   CHAR *serviceName         = NULL ;
   CHAR *userName            = NULL ;
   CHAR *passwordName        = NULL ;
   CHAR *collectionspaceName = NULL ;
   CHAR *collectionName      = NULL ;
   CHAR *preferedInstance    = NULL ;
   if( NULL == options )
      goto done ;
   /* address name */
   addressName = sdbGetOptionValue( foreignTableId,
                                     OPTION_NAME_ADDRESS ) ;
   if( NULL == addressName )
   {
      addressName = pstrdup( DEFAULT_HOSTNAME ) ;
   }

   /* service name */
   serviceName = sdbGetOptionValue( foreignTableId,
                                     OPTION_NAME_SERVICE ) ;
   if( NULL == serviceName )
   {
      serviceName = pstrdup( DEFAULT_SERVICENAME ) ;
   }

   /* user name */
   userName = sdbGetOptionValue( foreignTableId,
                                 OPTION_NAME_USER ) ;
   if( NULL == userName )
   {
      userName = pstrdup( DEFAULT_USERNAME ) ;
   }

   /* password name */
   passwordName = sdbGetOptionValue( foreignTableId,
                                     OPTION_NAME_PASSWORD ) ;
   if( NULL == passwordName )
   {
      passwordName = pstrdup( DEFAULT_PASSWORDNAME ) ;
   }

   /* collectionspace name */
   collectionspaceName = sdbGetOptionValue( foreignTableId,
                                            OPTION_NAME_COLLECTIONSPACE ) ;
   if( NULL == collectionspaceName )
   {
      /* if collectionspace name is not provided, let's use the schema name
       * of the table
       */
      collectionspaceName = get_namespace_name ( 
            get_rel_namespace( foreignTableId ) ) ;
   }

   /* collection name */
   collectionName = sdbGetOptionValue( foreignTableId,
                                       OPTION_NAME_COLLECTION ) ;
   if( NULL == collectionName )
   {
      /* if collection name is not provided, let's use the table name */
      collectionName = get_rel_name( foreignTableId ) ;
   }

   /* OPTION_NAME_PREFEREDINSTANCE */
   preferedInstance = sdbGetOptionValue( foreignTableId, 
                                         OPTION_NAME_PREFEREDINSTANCE ) ;
   if ( NULL == preferedInstance )
   {
      preferedInstance = pstrdup( DEFAULT_PREFEREDINSTANCE ) ;
   }

   /* fill up the result structure */
   SdbSetNodeAddressInfo( options, addressName, serviceName ) ;

   options->user                = userName ;
   options->password            = passwordName ;
   options->collectionspace     = collectionspaceName ;
   options->collection          = collectionName ;
   options->preference_instance = preferedInstance ;

done :
   return ;
}

/* validator validates for:
 * foreign data wrapper
 * server
 * user mapping
 * foreign table
 */
Datum sdb_fdw_validator( PG_FUNCTION_ARGS )
{
   Datum optionArray    = PG_GETARG_DATUM( 0 ) ;
   Oid optionContextId  = PG_GETARG_OID( 1 ) ;
   List *optionList     = untransformRelOptions( optionArray ) ;
   ListCell *optionCell = NULL ;

   /* iterate each element in option */
   foreach( optionCell, optionList )
   {
      DefElem *option  =( DefElem * )lfirst( optionCell ) ;
      CHAR *optionName = option->defname ;
      INT32 i = 0 ;
      /* find out which option it is */
      for( i = 0 ;
            i < sizeof( SdbInputOptionList )/ sizeof( SdbInputOption ) ;
            ++i )
      {
         const SdbInputOption *inputOption = &( SdbInputOptionList[i] ) ;
         /* compare type and name */
         if( optionContextId == inputOption->optionContextID &&
              strncmp( optionName, inputOption->optionName,
                        NAMEDATALEN )== 0 )
         {
            /* if we find the option, let's jump out */
            break ;
         }
      }
      /* if we don't find any match */
      if( sizeof( SdbInputOptionList )/ sizeof( SdbInputOption )== i )
      {
         StringInfo optionNames = sdbListOptions( optionContextId ) ;
         ereport( ERROR,( errcode( ERRCODE_FDW_INVALID_OPTION_NAME ),
                            errmsg( "invalid option \"%s\"", optionName ),
                            errhint( "Valid options in this context are: %s",
                                      optionNames->data ) ) ) ;
      }
      /* make sure the port is integer */
      if( strncmp( optionName, OPTION_NAME_SERVICE, NAMEDATALEN )== 0 )
      {
         CHAR *optionValue = defGetString( option ) ;
         INT32 portNumber  = pg_atoi( optionValue, sizeof( INT32 ), 0 ) ;
         ( void )portNumber ;
      }
   }
   PG_RETURN_VOID (  ) ;
}

/* sdbColumnMappingHash creates a hash table to map sdbbson fields into PG column
 * index
 */
static HTAB *sdbColumnMappingHash( Oid foreignTableId,
                                    List *columnList )
{
   ListCell *columnCell     = NULL ;
   const long hashTableSize = 2048 ;
   HTAB *columnMappingHash  = NULL ;
   /* create hash table */
   HASHCTL hashInfo ;
   memset( &hashInfo, 0, sizeof( hashInfo ) ) ;
   hashInfo.keysize = NAMEDATALEN ;
   hashInfo.entrysize = sizeof( SdbColumnMapping ) ;
   hashInfo.hash = string_hash ;
   hashInfo.hcxt = CurrentMemoryContext ;

   /* create hash table */
   columnMappingHash = hash_create( "Column Mapping Hash",
                                     hashTableSize,
                                     &hashInfo,
                                     ( HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT )
                                  ) ;
   if( !columnMappingHash )
   {
      goto error ;
   }
   /* iterate each column */
   foreach( columnCell, columnList )
   {
      Var *column                     = ( Var* )lfirst( columnCell ) ;
      AttrNumber columnId             = column->varattno ;
      SdbColumnMapping *columnMapping = NULL ;
      CHAR *columnName                = NULL ;
      bool handleFound                = false ;
      void *hashKey                   = NULL ;

      columnName = get_relid_attribute_name( foreignTableId, columnId ) ;
      hashKey = ( void * )columnName ;

      /* validate each column only appears once, HASH_ENTER means we want to
       * enter the entry into hash table if not found
       */
      columnMapping = ( SdbColumnMapping* )hash_search( columnMappingHash,
                                                       hashKey,
                                                       HASH_ENTER,
                                                       &handleFound ) ;
      if( !columnMapping )
      {
         goto error ;
      }
      columnMapping->columnIndex       = columnId - 1 ;
      columnMapping->columnTypeId      = column->vartype ;
      columnMapping->columnTypeMod     = column->vartypmod ;
      columnMapping->columnArrayTypeId = get_element_type( column->vartype ) ;
   }
done :
   return columnMappingHash ;
error :
   goto done ;
}

/* sdbColumnTypesCompatible checks if a sdbbson type is compatible with PG type */
static BOOLEAN sdbColumnTypesCompatible( sdbbson_type sdbbsonType, Oid columnTypeId )
{
   BOOLEAN compatibleType = FALSE ;
   switch( columnTypeId )
   {
   case BOOLOID :
      if( BSON_BOOL == sdbbsonType )
      {
         compatibleType = TRUE ;
      }
      /* do not break here since we also want to compatible with int/long/double
       * for boolean type
       */
   case INT2OID :
   case INT4OID :
   case INT8OID :
   case FLOAT4OID :
   case FLOAT8OID :
   case NUMERICOID :
   {
      if( BSON_INT == sdbbsonType || BSON_LONG == sdbbsonType ||
           BSON_DOUBLE == sdbbsonType )
      {
         compatibleType = TRUE ;
      }
      break ;
   }
   case BPCHAROID :
   case VARCHAROID :
   case TEXTOID :
   {
      if( BSON_STRING == sdbbsonType )
         compatibleType = TRUE ;
      break ;
   }
   case NAMEOID :
   {
      if( BSON_OID == sdbbsonType )
         compatibleType = TRUE ;
      break ;
   }
   case DATEOID :
   case TIMESTAMPOID :
   case TIMESTAMPTZOID :
   {
      if ( ( BSON_TIMESTAMP == sdbbsonType )|| ( BSON_DATE == sdbbsonType ) )
         compatibleType = TRUE ;
      break ;
   }
   case BYTEAOID :
   {
      if ( ( BSON_BINDATA == sdbbsonType ) )
      {
         compatibleType = TRUE ;
      }
      break ;
   }
   default :
   {
      ereport( ERROR,( errcode( ERRCODE_FDW_INVALID_DATA_TYPE ),
                         errmsg( "cannot convert sdbbson type to column type" ),
                         errhint( "Column type: %u",
                                   ( UINT32 )columnTypeId ) ) ) ;
      break ;
   }
   }
   return compatibleType ;
}

/* sdbColumnValue converts sdbbson value into PG datum
 */
static Datum sdbColumnValue( sdbbson_iterator *sdbbsonIterator, Oid columnTypeId,
                              INT32 columnTypeMod )
{
   Datum columnValue = 0 ;
   switch( columnTypeId )
   {
   case INT2OID :
   {
      INT16 value = ( INT16 )sdbbson_iterator_int( sdbbsonIterator ) ;
      columnValue = Int16GetDatum( value ) ;
      break ;
   }
   case INT4OID :
   {
      INT32 value = sdbbson_iterator_int( sdbbsonIterator ) ;
      columnValue = Int32GetDatum( value ) ;
      break ;
   }
   case INT8OID :
   {
      INT64 value = sdbbson_iterator_long( sdbbsonIterator ) ;
      columnValue = Int64GetDatum( value ) ;
      break ;
   }
   case FLOAT4OID :
   {
      FLOAT32 value = ( FLOAT32 )sdbbson_iterator_double( sdbbsonIterator ) ;
      columnValue = Float4GetDatum( value ) ;
      break ;
   }
   case FLOAT8OID :
   {
      FLOAT64 value = sdbbson_iterator_double( sdbbsonIterator ) ;
      columnValue = Float8GetDatum( value ) ;
      break ;
   }
   case NUMERICOID :
   {
      FLOAT64 value = sdbbson_iterator_double( sdbbsonIterator ) ;
      Datum valueDatum = Float8GetDatum( value ) ;
      columnValue = DirectFunctionCall1( float8_numeric, valueDatum ) ;
      break ;
   }
   case BOOLOID :
   {
      BOOLEAN value = sdbbson_iterator_bool( sdbbsonIterator ) ;
      columnValue = BoolGetDatum( value ) ;
      break ;
   }
   case BPCHAROID :
   {
      const CHAR *value = sdbbson_iterator_string( sdbbsonIterator ) ;
      Datum valueDatum = CStringGetDatum( value ) ;
      columnValue = DirectFunctionCall3( bpcharin, valueDatum,
                                          ObjectIdGetDatum( InvalidOid ),
                                          Int32GetDatum( columnTypeMod ) ) ;
      break ;
   }
   case VARCHAROID :
   {
      const CHAR *value = sdbbson_iterator_string( sdbbsonIterator ) ;
      Datum valueDatum = CStringGetDatum( value ) ;
      columnValue = DirectFunctionCall3( varcharin, valueDatum,
                                          ObjectIdGetDatum( InvalidOid ),
                                          Int32GetDatum( columnTypeMod ) ) ;
      break ;
   }
   case TEXTOID :
   {
      const CHAR *value = sdbbson_iterator_string( sdbbsonIterator ) ;
      columnValue       = CStringGetTextDatum( value ) ;
      break ;
   }
   case NAMEOID :
   {
      CHAR value [ NAMEDATALEN ] = {0} ;
      Datum valueDatum = 0 ;
      sdbbson_oid_t *sdbbsonObjectId = sdbbson_iterator_oid( sdbbsonIterator ) ;
      sdbbson_oid_to_string( sdbbsonObjectId, value ) ;
      valueDatum               = CStringGetDatum( value ) ;
      columnValue = DirectFunctionCall3( namein, valueDatum,
                                          ObjectIdGetDatum( InvalidOid ),
                                          Int32GetDatum( columnTypeMod ) ) ;
      break ;
   }
   case DATEOID :
   {  
      INT64 utcUsecs       = sdbbson_iterator_getusecs( sdbbsonIterator ) ;   
      INT64 timestamp      = utcUsecs - POSTGRES_TO_UNIX_EPOCH_USECS ;
      Datum timestampDatum = TimestampGetDatum( timestamp ) ;
      columnValue = DirectFunctionCall1( timestamptz_date, timestampDatum ) ;
      break ;
   }
   case TIMESTAMPOID :
   case TIMESTAMPTZOID :
   {
      INT64 utcUsecs       = sdbbson_iterator_getusecs( sdbbsonIterator ) ;
      INT64 timestamp      = utcUsecs - POSTGRES_TO_UNIX_EPOCH_USECS ;
      Datum timestampDatum = TimestampGetDatum( timestamp ) ;
      columnValue = DirectFunctionCall1( timestamptz_timestamp, timestampDatum ) ;
      break ;
   }
   case BYTEAOID :
   {
      const CHAR *buff = sdbbson_iterator_bin_data( sdbbsonIterator ) ;
      INT32 len        = sdbbson_iterator_bin_len( sdbbsonIterator ) ;

      bytea *result = (bytea *)palloc( len + VARHDRSZ ) ;
      memcpy( VARDATA(result), buff, len ) ;
      SET_VARSIZE(result, len + VARHDRSZ) ;

      columnValue = PointerGetDatum(result) ;

      break ;
   }
   default :
   {
      ereport( ERROR,( errcode( ERRCODE_FDW_INVALID_DATA_TYPE ),
                         errmsg( "cannot convert sdbbson type to column type" ),
                         errhint( "Column type: %u",
                                   ( UINT32 )columnTypeId ) ) ) ;
      break ;
   }
   }
   return columnValue ;
}

/* sdbFreeScanState closes the cursor, connection and collection to SequoiaDB
 */
void sdbFreeScanState( SdbExecState *executionState, bool deleteShared )
{
   sdbbson *original = NULL ;
   if( !executionState )
      goto done ;

   if( SDB_INVALID_HANDLE != executionState->hCursor )
      sdbReleaseCursor( executionState->hCursor ) ;
   if( SDB_INVALID_HANDLE != executionState->hCollection )
      sdbReleaseCollection( executionState->hCollection ) ;

   if ( deleteShared )
   {
      if ( 0 != executionState->bson_record_addr )
      {
         original = sdbGetRecordPointer( executionState->bson_record_addr ) ;
         sdbbson_destroy( original ) ;
         free( original ) ;
         executionState->bson_record_addr = 0 ;
      }
   }
   /* do not free connection since it's in pool */
done :
   return ;
}

/* sdbColumnValueArray build array
 */
static Datum sdbColumnValueArray( sdbbson_iterator *sdbbsonIterator,
                                   Oid valueTypeId )
{
   UINT32 arrayCapacity          = INITIAL_ARRAY_CAPACITY ;
   UINT32 arrayGrowthFactor      = 2 ;
   UINT32 arrayIndex             = 0 ;
   ArrayType *columnValueObject  = NULL ;
   Datum columnValueDatum        =  0 ;
   bool typeByValue              = false ;
   CHAR typeAlignment            = 0 ;
   INT16 typeLength              = 0 ;
   sdbbson_iterator sdbbsonSubIterator = { NULL, 0 } ;
   Datum *columnValueArray       = NULL ;
   sdbbson_iterator_subiterator( sdbbsonIterator, &sdbbsonSubIterator ) ;
   columnValueArray = ( Datum* )palloc0( INITIAL_ARRAY_CAPACITY *
                                        sizeof( Datum ) ) ;
   if( !columnValueArray )
   {
      ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ),
                         errmsg( "Unable to allocate array memory" ),
                         errhint( "Make sure the memory pool or ulimit is "
                                   "properly configured" ) ) ) ;
      goto error ;
   }
   /* go through each element in array */
   while( sdbbson_iterator_next( &sdbbsonSubIterator ) )
   {
      sdbbson_type sdbbsonType = sdbbson_iterator_type( &sdbbsonSubIterator ) ;
      BOOLEAN compatibleTypes = FALSE ;
      compatibleTypes = sdbColumnTypesCompatible( sdbbsonType, valueTypeId ) ;
      /* skip the element if it's not compatible */
      if( BSON_NULL == sdbbsonType || !compatibleTypes )
      {
         continue ;
      }
      if( arrayIndex >= arrayCapacity )
      {
         arrayCapacity *= arrayGrowthFactor ;
         columnValueArray = repalloc( columnValueArray,
                                       arrayCapacity * sizeof( Datum ) ) ;
      }
      /* use default type modifier to convert column value */
      columnValueArray[arrayIndex] = sdbColumnValue( &sdbbsonSubIterator,
                                                      valueTypeId, 0 ) ;
      ++arrayIndex ;
   }
done :
   get_typlenbyvalalign( valueTypeId, &typeLength, &typeByValue,
                          &typeAlignment ) ;
   columnValueObject = construct_array( columnValueArray, arrayIndex,
                                         valueTypeId, typeLength,
                                         typeByValue, typeAlignment ) ;
   columnValueDatum = PointerGetDatum( columnValueObject ) ;
   return columnValueDatum ;
error :
   goto done ;
}

/* sdbFillTupleSlot go through sdbbson document and the hash table, try to build
 * the tuple for PG
 * documentKey: if we want to find things in nested sdbbson object
 */
static void sdbFillTupleSlot( const sdbbson *sdbbsonDocument,
                               const CHAR *documentKey,
                               HTAB *columnMappingHash,
                               Datum *columnValues,
                               bool *columnNulls )
{
   sdbbson_iterator sdbbsonIterator = { NULL, 0 } ;
   sdbbson_iterator_init( &sdbbsonIterator, sdbbsonDocument ) ;

   /* for each element in sdbbson object */
   while( sdbbson_iterator_next( &sdbbsonIterator ) )
   {
      const CHAR *sdbbsonKey   = sdbbson_iterator_key( &sdbbsonIterator ) ;
      sdbbson_type sdbbsonType = sdbbson_iterator_type( &sdbbsonIterator ) ;
      SdbColumnMapping *columnMapping = NULL ;
      Oid columnTypeId                = InvalidOid ;
      Oid columnArrayTypeId           = InvalidOid ;
      BOOLEAN compatibleTypes         = FALSE ;
      bool handleFound                = false ;
      const CHAR *sdbbsonFullKey         = NULL ;
      void *hashKey                   = NULL ;

      /*
       * if we have object
       * {
       *    a: {
       *       A: 1,
       *       B: 2
       *    },
       *    b: "hello"
       * }
       * first round we have documentKey=NULL, then sdbbsonKey=a
       * next we have nested object, then recursively we call sdbFillTupleSlot
       * so we have
       * documentKey=a, sdbbsonKey=A, then we have sdbbsonFullKey=a.A
       */
      if( documentKey )
      {
         /* if we want to find entries in nested object, we should use this one
          */
         StringInfo sdbbsonFullKeyString = makeStringInfo (  ) ;
         appendStringInfo( sdbbsonFullKeyString, "%s.%s", documentKey,
                            sdbbsonKey ) ;
         sdbbsonFullKey = sdbbsonFullKeyString->data ;
      }
      else
      {
         sdbbsonFullKey = sdbbsonKey ;
      }
      /* recurse into nested objects */
      if( BSON_OBJECT == sdbbsonType )
      {
         sdbbson subObject ;
         sdbbson_init( &subObject ) ;
         sdbbson_iterator_subobject( &sdbbsonIterator, &subObject ) ;
         sdbFillTupleSlot( &subObject, sdbbsonFullKey,
                           columnMappingHash, columnValues, columnNulls ) ;
         sdbbson_destroy( &subObject ) ;
         continue ;
      }
      /* match columns for sdbbson key */
      hashKey = ( void* )sdbbsonFullKey ;
      columnMapping = ( SdbColumnMapping* )hash_search( columnMappingHash,
                                                       hashKey,
                                                       HASH_FIND,
                                                       &handleFound ) ;
      /* if we cannot find the column, or if the sdbbson type is null, let's just
       * leave it as null
       */
      if( NULL == columnMapping || BSON_NULL == sdbbsonType )
      {
         continue ;
      }

      /* check if columns have compatible types */
      columnTypeId = columnMapping->columnTypeId ;
      columnArrayTypeId = columnMapping->columnArrayTypeId ;
      if( OidIsValid( columnArrayTypeId )&& sdbbsonType == BSON_ARRAY )
      {
         compatibleTypes = TRUE ;
      }
      else
      {
         compatibleTypes = sdbColumnTypesCompatible( sdbbsonType, columnTypeId ) ;
      }

      /* if types are incompatible, leave this column null */
      if( !compatibleTypes )
      {
         continue ;
      }
      /* fill in corresponding column values and null flag */
      if( OidIsValid( columnArrayTypeId ) )
      {
         INT32 columnIndex = columnMapping->columnIndex ;
         columnValues[columnIndex] = sdbColumnValueArray( &sdbbsonIterator,
                                                           columnArrayTypeId ) ;
         columnNulls[columnIndex] = false ;
      }
      else
      {
         INT32 columnIndex = columnMapping->columnIndex ;
         Oid columnTypeMod = columnMapping->columnTypeMod ;
         columnValues[columnIndex] = sdbColumnValue( &sdbbsonIterator,
                                                      columnTypeId,
                                                      columnTypeMod ) ;
         columnNulls[columnIndex] = false ;
      }
   }
}

/* sdbRowsCount get count of records in the given collection */
static INT32 sdbRowsCount( Oid foreignTableId, SINT64 *count )
{
   INT32 rc                        = SDB_OK ;
   sdbCollectionHandle hCollection = SDB_INVALID_HANDLE ;
   sdbConnectionHandle hConnection = SDB_INVALID_HANDLE ;
   SdbInputOptions options ;
   StringInfo fullCollectionName = makeStringInfo (  ) ;

   sdbGetOptions( foreignTableId, &options ) ;
   /* attempt to connect to remote database */
   hConnection = sdbGetConnectionHandle( (const char **)options.serviceList, 
                                         options.serviceNum, options.user, 
                                         options.password, 
                                         options.preference_instance ) ;
   if ( SDB_INVALID_HANDLE == hConnection )
   {
      goto error ;
   }

   /* get collection */
   hCollection = sdbGetSdbCollection( hConnection, options.collectionspace, 
            options.collection ) ;
   if ( SDB_INVALID_HANDLE == hCollection )
   {
      ereport( ERROR, ( errcode( ERRCODE_FDW_ERROR ), 
            errmsg( "Unable to get collection \"%s.%s\", rc = %d", 
            options.collectionspace, options.collection, rc ),
            errhint( "Make sure the collectionspace and " 
               "collection exist on the remote database" ) ) ) ;

       goto error ;
   }

   /* get count */
   rc = sdbGetCount( hCollection, NULL, count ) ;
   if( rc )
   {
      ereport( ERROR,( errcode( ERRCODE_FDW_ERROR ),
                         errmsg( "Unable to get count for \"%s\", rc = %d",
                                  fullCollectionName->data, rc ),
                         errhint( "Make sure the collectionspace and "
                                   "collection exist on the remote database" )
                       ) ) ;
      goto error ;
   }
done :
   if( SDB_INVALID_HANDLE != hCollection )
      sdbReleaseCollection( hCollection ) ;
   return rc ;
error :
   goto done ;
}

/* Get the estimation size for Sdb foreign table
 * This function is also responsible to establish connection to remote table
 */
static void SdbGetForeignRelSize( PlannerInfo *root,
                                   RelOptInfo *baserel,
                                   Oid foreignTableId )
{
   INT32 rc                = SDB_OK ;
   SdbExecState *fdw_state = NULL ;
   List *rowClauseList     = NULL ;
   FLOAT64 rowSelectivity  = 0.0 ;
   FLOAT64 outputRowCount  = 0.0 ;

   fdw_state = palloc0( sizeof( SdbExecState ) ) ;
   initSdbExecState( fdw_state ) ;
   sdbGetSdbServerOptions( foreignTableId, fdw_state ) ;

   fdw_state->pgTableDesc = sdbGetPgTableDesc( foreignTableId ) ;
   if ( NULL == fdw_state->pgTableDesc )
   {
      ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ), 
            errmsg( "Unable to allocate pgTableDesc" ),
            errhint( "Make sure the memory pool or ulimit is properly configured" ) ) ) ;
      return ;
   }

   fdw_state->bson_record_addr = sdbCreateBsonRecordAddr(  ) ;

   sdbRowsCount( foreignTableId, &fdw_state->row_count ) ;
   if ( rc )
   {
      ereport( DEBUG1, ( errmsg( "Unable to retrieve the row count for collection" ),
            errhint( "Falling back to default estimates in planning" ) ) ) ;
      return ;
   }

   rowClauseList  = baserel->baserestrictinfo ;
   rowSelectivity = clauselist_selectivity( root, rowClauseList, 0, JOIN_INNER, NULL ) ;
   outputRowCount = clamp_row_est( fdw_state->row_count * rowSelectivity ) ;
   baserel->rows  = outputRowCount ;

   baserel->fdw_private = ( void * )fdw_state ;
}

/* SdbGetForeignPaths creates the scan path to execute query.
 * Sdb may decide to use underlying index, but the caller procedures do not
 * know whether it will happen or not.
 * Therefore, we simply create a table scan path
 * Estimation includes:
 * 1 )row count
 * 2 )disk cost
 * 3 )cpu cost
 * 4 )startup cost
 */
static void SdbGetForeignPaths( PlannerInfo *root,
                                 RelOptInfo *baserel,
                                 Oid foreignTableId )
{
   BlockNumber pageCount    = 0 ;
   INT32 rowWidth           = 0 ;
   FLOAT64 selectivity      = 0.0 ;
   FLOAT64 inputRowCount    = 0.0 ;
   FLOAT64 foreignTableSize = 0.0 ;
   List *opExpressionList   = NIL ;
   Path *foreignPath        = NULL ;

   /* cost estimation */
   FLOAT64 totalDiskCost    = 0.0 ;
   FLOAT64 totalCPUCost     = 0.0 ;
   FLOAT64 totalStartupCost = 0.0 ;
   FLOAT64 totalCost        = 0.0 ;

   SdbExecState *fdw_state  = NULL ;

   fdw_state = ( SdbExecState * )baserel->fdw_private ;
   /* rows estimation after applying predicates */
   opExpressionList = sdbApplicableOpExpressionList( baserel ) ;
   selectivity      = clauselist_selectivity( root, opExpressionList,
                                               0, JOIN_INNER, NULL ) ;
   inputRowCount    = clamp_row_est( fdw_state->row_count * selectivity ) ;

   /* disk cost estimation */
   rowWidth         = get_relation_data_width( foreignTableId,
                                                baserel->attr_widths ) ;
   foreignTableSize = rowWidth * fdw_state->row_count ;
   pageCount =( BlockNumber )rint( foreignTableSize / BLCKSZ ) ;
   totalDiskCost = seq_page_cost * pageCount ;

   /* cpu cost estimation, it includes the record process time + return time
    * cpu_tuple_cost: time to process each row
    * SDB_TUPLE_COST_MULTIPLIER * cpu_tuple_cost: time to return a row
    * baserel->baserestrictcost.per_tuple: time to process a row in PG
    */
   totalCPUCost = cpu_tuple_cost * fdw_state->row_count +
                 ( SDB_TUPLE_COST_MULTIPLIER * cpu_tuple_cost +
                    baserel->baserestrictcost.per_tuple )* inputRowCount ;

   /* startup cost estimation, which includes query execution startup time
    */
   totalStartupCost = baserel->baserestrictcost.startup ;

   /* total cost includes totalDiskCost + totalCPUCost + totalStartupCost
    */
   totalCost = totalStartupCost + totalCPUCost + totalDiskCost ;

   /* create foreign path node */
   foreignPath = ( Path* )create_foreignscan_path( root, baserel, baserel->rows, 
         totalStartupCost, totalCost, NIL, /* no pathkeys */
         NULL, /* no outer rel */ 
         NIL ) ; /* no fdw_private */

   /* add foreign path into path */
   add_path( baserel, foreignPath ) ;
   return ;
}

/* SdbGetForeignPlan creates a foreign scan plan node for Sdb collection */
static ForeignScan *SdbGetForeignPlan( PlannerInfo *root,
                                        RelOptInfo *baserel,
                                        Oid foreignTableId,
                                        ForeignPath *bestPath,
                                        List *targetList,
                                        List *restrictionClauses )
{
   Index scanRangeTableIndex = baserel->relid ;
   ForeignScan *foreignScan  = NULL ;
   List *foreignPrivateList  = NIL ;
   List *columnList          = NIL ;

   SdbExecState *fdw_state   = ( SdbExecState * )baserel->fdw_private ;
   sdbGetColumnKeyInfo( fdw_state ) ;

   /* We keep all restriction clauses at PG ide to re-check */
   restrictionClauses = extract_actual_clauses( restrictionClauses, FALSE ) ;
   /* construct the query sdbbson document */

   sdbGenerateFilterCondition( foreignTableId, baserel, &fdw_state->queryDocument ) ;
   foreignPrivateList = serializeSdbExecState( fdw_state ) ;

   /* copy document list */
   columnList = sdbColumnList( baserel ) ;
   /* construct foreign plan with query predicates and column list */
   foreignPrivateList = list_make2( foreignPrivateList, columnList ) ;

   /* create the foreign scan node */
   foreignScan =  make_foreignscan( targetList, restrictionClauses,
                                     scanRangeTableIndex,
                                     NIL, /* no expressions to evaluate */
                                     foreignPrivateList ) ;
   /* object should NOT be destroyed since serializeSdbExecState does not make
    * memory copy
    * So sdbbson_destroy is only called when rc != SDB_OK( that means no
    * serializeSdbExecState is called )
    */
   return foreignScan ;
}

/* SdbExplainForeignScan produces output for EXPLAIN command
 */
static void SdbExplainForeignScan( ForeignScanState *scanState,
                                    ExplainState *explainState )
{
   SdbInputOptions options ;
   StringInfo namespaceName = makeStringInfo (  ) ;
   Oid foreignTableId = RelationGetRelid( scanState->ss.ss_currentRelation ) ;
   sdbGetOptions( foreignTableId, &options ) ;
   appendStringInfo( namespaceName, "%s.%s",
                      options.collectionspace, options.collection ) ;
   ExplainPropertyText( "Foreign Namespace", namespaceName->data,
                         explainState ) ;
}

/* SdbBeginForeignScan connects to sdb server and open a cursor to perform scan
 */
static void SdbBeginForeignScan( ForeignScanState *scanState,
                                  INT32 executorFlags )
{
   INT32 rc                  = SDB_OK ;
   Oid foreignTableId        = InvalidOid ;
   ForeignScan *foreignScan  = NULL ;
   List *foreignPrivateList  = NIL ;
   List *columnList          = NIL ;
   HTAB *columnMappingHash   = NULL ;
   List *fdw_state_list      = NIL ;
   SdbExecState *fdw_state   = NULL ;

   /* do not begin real scan if it's explain only */
   if( executorFlags & EXEC_FLAG_EXPLAIN_ONLY )
   {
      return ;
   }

   foreignTableId = RelationGetRelid( scanState->ss.ss_currentRelation ) ;

   /* deserialize fdw*/
   foreignScan        = ( ForeignScan * )scanState->ss.ps.plan ;
   foreignPrivateList = foreignScan->fdw_private ;
   fdw_state_list     = ( List * )linitial( foreignPrivateList ) ;
   fdw_state          = deserializeSdbExecState( fdw_state_list ) ;

   /* build hash map */
   columnList = ( List * )lsecond( foreignPrivateList ) ;
   columnMappingHash = sdbColumnMappingHash( foreignTableId, columnList ) ;

   fdw_state->planType          = SDB_PLAN_SCAN ;
   fdw_state->columnMappingHash = columnMappingHash ;

   /* retreive target information */
   fdw_state->hConnection = sdbGetConnectionHandle( 
                                        (const char **)fdw_state->sdbServerList,
                                        fdw_state->sdbServerNum, 
                                        fdw_state->usr, 
                                        fdw_state->passwd, 
                                        fdw_state->preferenceInstance ) ;

   fdw_state->hCollection = sdbGetSdbCollection( fdw_state->hConnection, 
         fdw_state->sdbcs, fdw_state->sdbcl ) ;

   rc = sdbQuery( fdw_state->hCollection, &fdw_state->queryDocument, NULL, NULL, 
         NULL, 0, -1, &fdw_state->hCursor ) ;
   if ( rc )
   {
      sdbPrintBson( &fdw_state->queryDocument, WARNING ) ;
      ereport( ERROR, ( errcode ( ERRCODE_FDW_ERROR ),
            errmsg( "query collection failed:cs=%s,cl=%s,rc=%d", 
               fdw_state->sdbcs, fdw_state->sdbcl, rc ),
            errhint( "Make sure collection exists on remote SequoiaDB database" ) ) ) ;

      sdbbson_dispose( &fdw_state->queryDocument ) ;
      return ;
   }

   scanState->fdw_state = ( void* )fdw_state ;
}

/* SdbIterateForeignScan reads the next record from SequoiaDB and converts into
 * PostgreSQL tuple
 */
static TupleTableSlot * SdbIterateForeignScan( ForeignScanState *scanState )
{
   sdbbson *recordObj           = NULL ;
   INT32 rc                     = SDB_OK ;
   SdbExecState *executionState = ( SdbExecState* )scanState->fdw_state ;
   TupleTableSlot *tupleSlot    = scanState->ss.ss_ScanTupleSlot ;
   TupleDesc tupleDescriptor    = tupleSlot->tts_tupleDescriptor ;
   Datum *columnValues          = tupleSlot->tts_values ;
   bool *columnNulls            = tupleSlot->tts_isnull ;
   INT32 columnCount            = tupleDescriptor->natts ;
   const CHAR *sdbbsonDocumentKey  = NULL ;

   recordObj = sdbGetRecordPointer( executionState->bson_record_addr ) ;
   sdbbson_destroy( recordObj ) ;
   sdbbson_init( recordObj ) ;
   /* if there's nothing more to fetch, we return empty slot to represent
    * there's no more data to read
    */
   ExecClearTuple( tupleSlot ) ;
   /* initialize all values */
   memset( columnValues, 0, columnCount * sizeof( Datum ) ) ;
   memset( columnNulls, TRUE, columnCount * sizeof( bool ) ) ;

   /* cursor read next */
   rc = sdbNext( executionState->hCursor, recordObj ) ;
   if( rc )
   {
      if( SDB_DMS_EOC != rc )
      {
         sdbFreeScanState( executionState, true ) ;
         /* if other error happened, let's report them */
         ereport( ERROR,( errcode( ERRCODE_FDW_ERROR ),
                            errmsg( "unable to fetch next record"
                                     ", rc = %d", rc ),
                            errhint( "Make sure collection exists on remote "
                                      "SequoiaDB database, and the server "
                                      "is still up and running" ) ) ) ;
         goto error ;
      }
      /* if we get EOC, let's just goto done to return empty tupleSlot */
      goto done ;
   }

   sdbFillTupleSlot( recordObj, sdbbsonDocumentKey,
                      executionState->columnMappingHash,
                      columnValues, columnNulls ) ;

   ExecStoreVirtualTuple( tupleSlot ) ;
done :

   return tupleSlot ;
error :
   goto done ;
}

/* SdbRescanForeignScan rescans the foreign table
 */
static void SdbRescanForeignScan( ForeignScanState *scanState )
{
   INT32 rc                     = SDB_OK ;
   SdbExecState *executionState =( SdbExecState * )scanState->fdw_state ;
   if( !executionState )
      goto error ;
   /* kill the cursor and start up a new one */
   sdbReleaseCursor( executionState->hCursor ) ;
   rc = sdbQuery( executionState->hCollection, &executionState->queryDocument,
                   NULL, NULL, NULL, 0, -1,
                   &executionState->hCursor ) ;
   if( rc )
   {
      ereport( ERROR,( errcode( ERRCODE_FDW_ERROR ),
                         errmsg( "unable to rescan collection"
                                  ", rc = %d", rc ),
                         errhint( "Make sure collection exists on remote "
                                   "SequoiaDB database" ) ) ) ;
      goto error ;
   }
done :
   return ;
error :
   goto done ;
}

/* SdbEndForeignScan finish scanning the foreign table
 */
static void SdbEndForeignScan( ForeignScanState *scanState )
{
   SdbExecState *executionState =( SdbExecState * )scanState->fdw_state ;
   if( executionState )
   {
      sdbFreeScanState( executionState, true ) ;
   }
}

/* sdbAcquireSampleRows acquires a random sample of rows from the foreign table.
 */
static INT32 sdbAcquireSampleRows( Relation relation, INT32 errorLevel,
                                    HeapTuple *sampleRows, INT32 targetRowCount,
                                    FLOAT64 *totalRowCount,
                                    FLOAT64 *totalDeadRowCount )
{
   INT32 rc                          = SDB_OK ;
   INT32 sampleRowCount              = 0 ;
   INT64 rowCount                    = 0 ;
   INT64 rowCountToSkip              = -1 ;
   FLOAT64 randomState               = 0 ;
   Datum *columnValues               = NULL ;
   bool *columnNulls                 = NULL ;
   Oid foreignTableId                = InvalidOid ;
   TupleDesc tupleDescriptor         = NULL ;
   Form_pg_attribute *attributesPtr  = NULL ;
   AttrNumber columnCount            = 0 ;
   AttrNumber columnId               = 0 ;
   HTAB *columnMappingHash           = NULL ;
   sdbCursorHandle hCursor           = SDB_INVALID_HANDLE ;
   List *columnList                  = NIL ;
   ForeignScanState *scanState       = NULL ;
   List *foreignPrivateList          = NIL ;
   ForeignScan *foreignScan          = NULL ;
   CHAR *relationName                = NULL ;
   INT32 executorFlags               = 0 ;
   MemoryContext oldContext          = CurrentMemoryContext ;
   MemoryContext tupleContext        = NULL ;
   sdbbson queryDocument ;
   SdbExecState *fdw_state            = NULL ;

   sdbbson_init( &queryDocument ) ;

   /* create columns in the relation */
   tupleDescriptor = RelationGetDescr( relation ) ;
   columnCount     = tupleDescriptor->natts ;
   attributesPtr   = tupleDescriptor->attrs ;

   for( columnId = 1 ; columnId <= columnCount ; ++columnId )
   {
      Var *column       = ( Var* )palloc0( sizeof( Var ) ) ;
      if( !column )
      {
         ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ),
                            errmsg( "Unable to allocate Var memory" ),
                            errhint( "Make sure the memory pool or ulimit is "
                                      "properly configured" ) ) ) ;
         goto error ;
      }
      column->varattno  = columnId ;
      column->vartype   = attributesPtr[columnId-1]->atttypid ;
      column->vartypmod = attributesPtr[columnId-1]->atttypmod ;
      columnList        = lappend( columnList, column ) ;
   }

   foreignTableId = RelationGetRelid( relation ) ;
   fdw_state = palloc0( sizeof( SdbExecState ) ) ;
   initSdbExecState( fdw_state ) ;
   sdbGetSdbServerOptions( foreignTableId, fdw_state ) ;

   fdw_state->pgTableDesc = sdbGetPgTableDesc( foreignTableId ) ;
   if ( NULL == fdw_state->pgTableDesc )
   {
      sdbFreeScanState( fdw_state, true ) ;
      ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ), 
            errmsg( "Unable to allocate pgTableDesc" ),
            errhint( "Make sure the memory pool or ulimit is properly configured" ) ) ) ;
      goto error ;
   }

   /* create state structure */
   scanState = makeNode( ForeignScanState ) ;
   scanState->ss.ss_currentRelation = relation ;

   foreignPrivateList = serializeSdbExecState( fdw_state ) ;
   foreignPrivateList = list_make2( foreignPrivateList, columnList ) ;

   foreignScan              = makeNode( ForeignScan ) ;
   foreignScan->fdw_private = foreignPrivateList ;
   scanState->ss.ps.plan = ( Plan * )foreignScan ;
   SdbBeginForeignScan( scanState, executorFlags ) ;

   fdw_state = ( SdbExecState* )scanState->fdw_state ;
   hCursor = fdw_state->hCursor ;
   columnMappingHash = fdw_state->columnMappingHash ;

   tupleContext = AllocSetContextCreate( CurrentMemoryContext,
                                          "sdb_fdw temp context",
                                          ALLOCSET_DEFAULT_MINSIZE,
                                          ALLOCSET_DEFAULT_INITSIZE,
                                          ALLOCSET_DEFAULT_MAXSIZE ) ;
   /* prepare for sampling rows */
   randomState  = anl_init_selection_state( targetRowCount ) ;
   columnValues = ( Datum * )palloc0( columnCount * sizeof( Datum ) ) ;
   columnNulls  = ( bool* )palloc0( columnCount * sizeof( bool ) ) ;
   if( !columnValues || !columnNulls )
   {
      sdbFreeScanState( fdw_state, true ) ;
      ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ),
                         errmsg( "Unable to allocate Var memory" ),
                         errhint( "Make sure the memory pool or ulimit is "
                                   "properly configured" ) ) ) ;
      goto error ;
   }
   while( TRUE )
   {
      sdbbson recordObj ;
      sdbbson_init( &recordObj ) ;
      /* check for any break or terminate events */
      vacuum_delay_point(  ) ;
      /* init all values for this row to null */
      memset( columnValues, 0, columnCount * sizeof( Datum ) ) ;
      memset( columnNulls, true, columnCount * sizeof( bool ) ) ;
      rc = sdbNext( hCursor, &recordObj ) ;
      if( rc )
      {
         sdbbson_destroy( &recordObj ) ;
         if( SDB_DMS_EOC != rc )
         {
            sdbFreeScanState( fdw_state, true ) ;
            /* if other error happened, let's report them */
            ereport( ERROR,( errcode( ERRCODE_FDW_ERROR ),
                               errmsg( "unable to fetch next record"
                                        ", rc = %d", rc ),
                               errhint( "Make sure collection exists on remote"
                                         " SequoiaDB database, and the server "
                                         "is still up and running" ) ) ) ;
            goto error ;
         }
         break ;
      }
      /* if we can read the next record */
      MemoryContextReset( tupleContext ) ;
      MemoryContextSwitchTo( tupleContext ) ;
      sdbFillTupleSlot( &recordObj, NULL, columnMappingHash,
                         columnValues, columnNulls ) ;
      MemoryContextSwitchTo( oldContext ) ;

      if( sampleRowCount < targetRowCount )
      {
         sampleRows[sampleRowCount++] = heap_form_tuple( tupleDescriptor,
                                                          columnValues,
                                                          columnNulls ) ;
      }
      else
      {
         if( rowCountToSkip < 0 )
         {
            rowCountToSkip = anl_get_next_S( rowCount, targetRowCount,
                                              &randomState ) ;
         }
         if( rowCountToSkip <= 0 )
         {
            /* process the row if we skipped enough records */
            INT32 rowIndex = ( INT32 )( targetRowCount * anl_random_fract(  ) ) ;
            heap_freetuple( sampleRows[rowIndex] ) ;
            sampleRows[rowIndex] = heap_form_tuple( tupleDescriptor,
                                                     columnValues,
                                                     columnNulls ) ;
         }
         rowCountToSkip -= 1 ;
      }
      rowCount += 1 ;
      sdbbson_destroy( &recordObj ) ;
   }

   sdbFreeScanState( fdw_state, true ) ;
done :
   MemoryContextDelete( tupleContext ) ;
   pfree( columnValues ) ;
   pfree( columnNulls ) ;
   sdbbson_destroy( &queryDocument ) ;

   /* get some result */
   relationName = RelationGetRelationName( relation ) ;
   ereport( errorLevel, ( errmsg( "\"%s\": collection contains %lld rows; "
                                   "%d rows in sample",
                                   relationName, rowCount, sampleRowCount ) ) ) ;
   ( *totalRowCount )= rowCount ;
   ( *totalDeadRowCount )= 0 ;
   return sampleRowCount ;
error :
   goto done ;
}

/* SdbAnalyzeForeignTable collects statistics for the given foreign table
 */
static bool SdbAnalyzeForeignTable ( 
      Relation relation,
      AcquireSampleRowsFunc *acquireSampleRowsFunc,
      BlockNumber *totalPageCount )
{
   INT32 rc                 = SDB_OK ;
   BlockNumber pageCount    = 0 ;
   INT32 attributeCount     = 0 ;
   INT32 *attributeWidths   = NULL ;
   Oid foreignTableId       = InvalidOid ;
   INT32 documentWidth      = 0 ;
   INT64 rowsCount          = 0 ;
   FLOAT64 foreignTableSize = 0 ;

   foreignTableId = RelationGetRelid( relation ) ;
   rc = sdbRowsCount( foreignTableId, &rowsCount ) ;
   if( rc )
   {
      goto error ;
   }
   if( rowsCount >= 0 )
   {
      attributeCount  = RelationGetNumberOfAttributes( relation ) ;
      attributeWidths = ( INT32* )palloc0( ( attributeCount + 1 )*sizeof( INT32 ) ) ;
      if( !attributeWidths )
      {
         ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ),
                            errmsg( "Unable to allocate attr array memory" ),
                            errhint( "Make sure the memory pool or ulimit is "
                                      "properly configured" ) ) ) ;
         goto error ;
      }
      documentWidth = get_relation_data_width( foreignTableId,
                                                attributeWidths ) ;
      foreignTableSize = rowsCount * documentWidth ;
      pageCount = ( BlockNumber )rint( foreignTableSize / BLCKSZ ) ;
   }
   else
   {
      ereport( ERROR,( errmsg( "Unable to retreive rows count" ),
                         errhint( "Unable to collect stats about foreign "
                                   "table" ) ) ) ;
   }
done :
   ( *totalPageCount )= pageCount ;
   ( *acquireSampleRowsFunc )= sdbAcquireSampleRows ;
   return TRUE ;
error :
   goto done ;
}

#if PG_VERSION_NUM>90300

static INT32 SdbIsForeignRelUpdatable( Relation rel )
{
   return( 1 << CMD_INSERT )|( 1 << CMD_UPDATE )|
         ( 1 << CMD_DELETE ) ;
}

static void SdbAddForeignUpdateTargets( Query *parsetree, RangeTblEntry *target_rte,
      Relation target_relation )
{




}

 static List *SdbPlanForeignModify( PlannerInfo *root, ModifyTable *plan, 
      Index resultRelation, INT32 subplan_index )
{
   RangeTblEntry *rte           = NULL ;
   Oid foreignTableId ;
   SdbExecState *fdw_state      = NULL ;

   if ( resultRelation < root->simple_rel_array_size
         && root->simple_rel_array[resultRelation] != NULL )
   {
      fdw_state = ( SdbExecState * )( root->simple_rel_array[resultRelation]->fdw_private ) ;
   }
   else
   {
      /* allocate new execution state */
      fdw_state = ( SdbExecState* )palloc( sizeof( SdbExecState ) ) ;
      if ( !fdw_state )
      {
         ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ), 
               errmsg( "Unable to allocate execution state memory" ), 
               errhint( "Make sure the memory pool or ulimit is properly configured" ) ) ) ;

         return NULL ;
      }

      initSdbExecState( fdw_state ) ;
      rte = planner_rt_fetch( resultRelation, root ) ;
      foreignTableId = rte->relid ;
      sdbGetSdbServerOptions( foreignTableId, fdw_state ) ;

      fdw_state->pgTableDesc = sdbGetPgTableDesc( foreignTableId ) ;
      if ( NULL == fdw_state->pgTableDesc )
      {
         ereport( ERROR,( errcode( ERRCODE_FDW_OUT_OF_MEMORY ), 
               errmsg( "Unable to allocate pgTableDesc" ),
               errhint( "Make sure the memory pool or ulimit is properly configured" ) ) ) ;

         return NULL ;
      }
   }

   return serializeSdbExecState( fdw_state ) ;
}

void SdbBeginForeignModify( ModifyTableState *mtstate,
      ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags )
{

   SdbExecState *fdw_state = deserializeSdbExecState( fdw_private ) ;
   rinfo->ri_FdwState = fdw_state ;

   fdw_state->hConnection = sdbGetConnectionHandle( 
                                       (const char **)fdw_state->sdbServerList, 
                                       fdw_state->sdbServerNum, 
                                       fdw_state->usr, 
                                       fdw_state->passwd, 
                                       fdw_state->preferenceInstance ) ;
   fdw_state->hCollection = sdbGetSdbCollection( fdw_state->hConnection, 
      fdw_state->sdbcs, fdw_state->sdbcl ) ;


}

void sdb_slot_deform_tuple( TupleTableSlot *slot, int natts )
{
   HeapTuple tuple     = slot->tts_tuple;
   TupleDesc tupleDesc = slot->tts_tupleDescriptor;
   Datum *values       = slot->tts_values;
   bool *isnull        = slot->tts_isnull;
   HeapTupleHeader tup = tuple->t_data;
   bool hasnulls       = HeapTupleHasNulls(tuple);
   Form_pg_attribute *att = tupleDesc->attrs;
   int attnum;
   char *tp;           /* ptr to tuple data */
   long off;        /* offset in tuple data */
   bits8 *bp = tup->t_bits;      /* ptr to null bitmap in tuple */
   bool slow;       /* can we use/set attcacheoff? */

   /*
    * Check whether the first call for this tuple, and initialize or restore
    * loop state.
    */
   attnum = slot->tts_nvalid;
   if (attnum == 0)
   {
      /* Start from the first attribute */
      off = 0;
      slow = false;
   }
   else
   {
      /* Restore state from previous execution */
      off = slot->tts_off;
      slow = slot->tts_slow;
   }

   tp = (char *) tup + tup->t_hoff;

   for (; attnum < natts; attnum++)
   {
      Form_pg_attribute thisatt = att[attnum];

      if (hasnulls && att_isnull(attnum, bp))
      {
         values[attnum] = (Datum) 0;
         isnull[attnum] = true;
         slow = true;      /* can't use attcacheoff anymore */
         continue;
      }

      isnull[attnum] = false;

      if (!slow && thisatt->attcacheoff >= 0)
         off = thisatt->attcacheoff;
      else if (thisatt->attlen == -1)
      {
         /*
          * We can only cache the offset for a varlena attribute if the
          * offset is already suitably aligned, so that there would be no
          * pad bytes in any case: then the offset will be valid for either
          * an aligned or unaligned value.
          */
         if (!slow &&
            off == att_align_nominal(off, thisatt->attalign))
            thisatt->attcacheoff = off;
         else
         {
            off = att_align_pointer(off, thisatt->attalign, -1,
                              tp + off);
            slow = true;
         }
      }
      else
      {
         /* not varlena, so safe to use att_align_nominal */
         off = att_align_nominal(off, thisatt->attalign);

         if (!slow)
            thisatt->attcacheoff = off;
      }

      values[attnum] = fetchatt(thisatt, tp + off);

      off = att_addlength_pointer(off, thisatt->attlen, tp + off);

      if (thisatt->attlen <= 0)
         slow = true;      /* can't use attcacheoff anymore */
   }

   /*
    * Save state for next execution
    */
   slot->tts_nvalid = attnum;
   slot->tts_off = off;
   slot->tts_slow = slow;
}

TupleTableSlot *SdbExecForeignInsert( EState *estate, ResultRelInfo *rinfo,
      TupleTableSlot *slot, TupleTableSlot *planSlot )
{
   SdbExecState *fdw_state = ( SdbExecState * )rinfo->ri_FdwState ;
   int attnum = 0 ;
   PgTableDesc *tableDesc = NULL ;
   int rc = SDB_OK ;

   sdbbson insert ;
   sdbbson_init( &insert ) ;
   tableDesc = fdw_state->pgTableDesc ;
   if ( slot->tts_nvalid == 0 )
   {
      sdb_slot_deform_tuple( slot, tableDesc->ncols ) ;
   }

   for (  ; attnum < tableDesc->ncols ; attnum++ )
   {
      if ( slot->tts_isnull[attnum] )
      {
         continue ;
      }

      rc = sdbSetBsonValue( &insert, tableDesc->cols[attnum].pgname, 
                            slot->tts_values[attnum], 
                            tableDesc->cols[attnum].pgtype, 
                            tableDesc->cols[attnum].pgtypmod ) ;
      if ( SDB_OK != rc )
      {
         ereport ( WARNING, ( errcode( ERRCODE_FDW_INVALID_DATA_TYPE ),
                   errmsg( "convert value failed:key=%s", 
                   tableDesc->cols[attnum].pgname ) ) ) ;
         sdbbson_destroy( &insert ) ;
         return NULL ;
      }
   }

   rc = sdbbson_finish( &insert ) ;
   if ( rc != SDB_OK )
   {
      ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
            errmsg( "finish bson failed:rc = %d", rc ), 
            errhint( "Make sure the data type is all right" ) ) ) ;

      sdbbson_destroy( &insert ) ;
      return NULL ;
   }

   rc = sdbInsert( fdw_state->hCollection, &insert ) ;
   if ( rc != SDB_OK )
   {
      ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
            errmsg( "sdbInsert failed:rc = %d", rc ), 
            errhint( "Make sure the data type is all right" ) ) ) ;

      sdbbson_destroy( &insert ) ;
      return NULL ;
   }

   sdbbson_destroy( &insert ) ;

   /* store the virtual tuple */
   return slot ;
}

TupleTableSlot *SdbExecForeignDelete( EState *estate, ResultRelInfo *rinfo,
      TupleTableSlot *slot, TupleTableSlot *planSlot )
{
   int i   = 0 ;
   sdbbson sdbbsonCondition ;
   sdbbson *original ;
   int rc = SDB_OK ;
   SdbExecState *fdw_state = ( SdbExecState * )rinfo->ri_FdwState ;

   sdbbson_init( &sdbbsonCondition ) ;
   original = sdbGetRecordPointer( fdw_state->bson_record_addr ) ;
   for ( i = 0 ; i < fdw_state->key_num ; i++ )
   {
      sdbbson_iterator ite ;
      sdbbson_find( &ite, original, fdw_state->key_name[i] ) ;
      sdbbson_append_element( &sdbbsonCondition, NULL, &ite ) ;
   }
   sdbbson_finish( &sdbbsonCondition ) ;

   rc = sdbDelete( fdw_state->hCollection, &sdbbsonCondition, NULL ) ;
   if ( rc != SDB_OK )
   {
      sdbbson_destroy( &sdbbsonCondition ) ;
      ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
            errmsg( "sdbDelete failed:rc = %d", rc ), 
            errhint( "Make sure the data type is all right" ) ) ) ;
      return NULL ;
   }

   sdbbson_destroy( &sdbbsonCondition ) ;

   ExecClearTuple( slot ) ;
   ExecStoreVirtualTuple( slot ) ;

   return slot ;
}

TupleTableSlot *SdbExecForeignUpdate( EState *estate, ResultRelInfo *rinfo,
      TupleTableSlot *slot, TupleTableSlot *planSlot )
{
   int i   = 0 ;
   sdbbson sdbbsonCondition ;
   sdbbson sdbbsonValues ;
   sdbbson sdbbsonTempValue ;
   sdbbson *original = NULL ;
   Datum datum ;
   int rc = SDB_OK ;
   bool isnull ;
   SdbExecState *fdw_state = ( SdbExecState * )rinfo->ri_FdwState ;

   sdbbson_init( &sdbbsonTempValue ) ;
   for ( i = 0 ; i < fdw_state->pgTableDesc->ncols ; i++ )
   {
      datum = slot_getattr( slot, fdw_state->pgTableDesc->cols[i].pgattnum, &isnull ) ;
      if ( !isnull )
      {
         rc = sdbSetBsonValue( &sdbbsonTempValue, 
                               fdw_state->pgTableDesc->cols[i].pgname, 
                               datum, fdw_state->pgTableDesc->cols[i].pgtype, 
                               fdw_state->pgTableDesc->cols[i].pgtypmod ) ;
         if ( SDB_OK != rc )
         {
            ereport ( WARNING, ( errcode( ERRCODE_FDW_INVALID_DATA_TYPE ),
                      errmsg( "convert value failed:key=%s", 
                      fdw_state->pgTableDesc->cols[i].pgname ) ) ) ;
            sdbbson_destroy( &sdbbsonTempValue ) ;
            return NULL ;
         }
      }
   }
   sdbbson_finish( &sdbbsonTempValue ) ;

   sdbbson_init( &sdbbsonCondition ) ;
   original = sdbGetRecordPointer( fdw_state->bson_record_addr ) ;
   for ( i = 0 ; i < fdw_state->key_num ; i++ )
   {
      sdbbson_iterator ite ;
      sdbbson_find( &ite, original, fdw_state->key_name[i] ) ;
      sdbbson_append_element( &sdbbsonCondition, NULL, &ite ) ;
   }
   sdbbson_finish( &sdbbsonCondition ) ;

   if ( sdbIsShardingKeyChanged( fdw_state, original, &sdbbsonTempValue ) )
   {
      sdbbson_destroy( &sdbbsonCondition ) ;
      sdbbson_destroy( &sdbbsonTempValue ) ;

      ereport( ERROR, ( errcode( ERRCODE_FDW_ERROR ), 
            errmsg( "update failed due to shardingkey is changed" ) ) ) ;

      return NULL ;
   }

   sdbbson_init( &sdbbsonValues ) ;
   sdbbson_append_sdbbson( &sdbbsonValues, "$set", &sdbbsonTempValue ) ;
   sdbbson_finish( &sdbbsonValues ) ;

   rc = sdbUpdate( fdw_state->hCollection, &sdbbsonValues, &sdbbsonCondition, NULL ) ;
   if ( rc != SDB_OK )
   {
      sdbbson_destroy( &sdbbsonCondition ) ;
      sdbbson_destroy( &sdbbsonTempValue ) ;
      sdbbson_destroy( &sdbbsonValues ) ;
      ereport( WARNING, ( errcode( ERRCODE_FDW_ERROR ), 
            errmsg( "sdbUpdate failed:rc = %d", rc ), 
            errhint( "Make sure the data type is all right" ) ) ) ;
      return NULL ;
   }

   sdbbson_destroy( &sdbbsonCondition ) ;
   sdbbson_destroy( &sdbbsonTempValue ) ;
   sdbbson_destroy( &sdbbsonValues ) ;


   return slot ;
}


void SdbEndForeignModify( EState *estate, ResultRelInfo *rinfo )
{
   SdbExecState *fdw_state = ( SdbExecState * )rinfo->ri_FdwState ;
   if ( fdw_state )
   {
      sdbFreeScanState( fdw_state, false ) ;
   }
}

void SdbExplainForeignModify( ModifyTableState *mtstate, ResultRelInfo *rinfo,
      List *fdw_private, int subplan_index, struct ExplainState *es )
{
}

#endif

/* Transaction related */

static void SdbFdwXactCallback( XactEvent event, void *arg )
{
   INT32 count             = 0 ;
   SdbConnectionPool *pool = sdbGetConnectionPool(  ) ;
   for( count = 0 ; count < pool->numConnections ; ++count )
   {
      /* if there's no transaction started on this connection, let's continue */
      if( 0 == pool->connList[count].transLevel )
         continue ;
      /* attempt to commit/abort the transaction, ignore return code */
      switch( event )
      {
      case XACT_EVENT_COMMIT :
         pool->connList[count].transLevel = 0 ;
         break ;
      case XACT_EVENT_ABORT :
         pool->connList[count].transLevel = 0 ;
         break ;
      default :
         break ;
      }
   }
}

void _PG_init (  )
{
   sdbInitConnectionPool (  ) ;
   RegisterXactCallback( SdbFdwXactCallback, NULL ) ;
}

void _PG_fini (  )
{
   sdbUninitConnectionPool (  ) ;
}
