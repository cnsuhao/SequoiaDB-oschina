#ifndef SDB_FDW_H__
#define SDB_FDW_H__

#include "client.h"
#include "fmgr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"

/* Connection related options */
#define OPTION_NAME_ADDRESS          "address"
#define OPTION_NAME_SERVICE          "service"
#define OPTION_NAME_USER             "user"
#define OPTION_NAME_PASSWORD         "password"
/******************************/

/* Table related options */
#define OPTION_NAME_COLLECTIONSPACE  "collectionspace"
#define OPTION_NAME_COLLECTION       "collection"
#define OPTION_NAME_PREFEREDINSTANCE "preferedinstance"

#define DEFAULT_PREFEREDINSTANCE     "A"
/*************************/

#define DEFAULT_HOSTNAME            "localhost"
#define DEFAULT_SERVICENAME         "30010"
#define DEFAULT_USERNAME            ""
#define DEFAULT_PASSWORDNAME        ""

#define INITIAL_ARRAY_CAPACITY         8
#define SDB_TUPLE_COST_MULTIPLIER      5

#define SDB_MAX_KEY_COLUMN_COUNT       (10)
#define SDB_MAX_KEY_COLUMN_LENGTH      (20)

#define POSTGRES_TO_UNIX_EPOCH_DAYS (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define POSTGRES_TO_UNIX_EPOCH_USECS (POSTGRES_TO_UNIX_EPOCH_DAYS * USECS_PER_DAY)
struct SdbInputOption
{
   const CHAR *optionName ;
   Oid   optionContextID ;
} ;
typedef struct SdbInputOption SdbInputOption ;

static const SdbInputOption SdbInputOptionList[] =
{
   { OPTION_NAME_ADDRESS,          ForeignServerRelationId },
   { OPTION_NAME_SERVICE,          ForeignServerRelationId },
   { OPTION_NAME_USER,             ForeignServerRelationId },
   { OPTION_NAME_PASSWORD,         ForeignServerRelationId },
   { OPTION_NAME_PREFEREDINSTANCE, ForeignServerRelationId },

   { OPTION_NAME_COLLECTIONSPACE,  ForeignTableRelationId },
   { OPTION_NAME_COLLECTION,       ForeignTableRelationId }
} ;

struct SdbInputOptions
{
   CHAR *address ;
   CHAR *service ;
   CHAR *user ;
   CHAR *password ;
   CHAR *collectionspace ;
   CHAR *collection ;
   CHAR *preference_instance;
} ;
typedef struct SdbInputOptions SdbInputOptions ;

/* SdbColumnMapping is for mapping between sdbbson's field name and PG's column
 * name
 */
struct SdbColumnMapping
{
   CHAR columnName [ NAMEDATALEN ] ;
   UINT32 columnIndex ;
   Oid columnTypeId ;
   INT32 columnTypeMod ;
   Oid columnArrayTypeId ;
} ;
typedef struct SdbColumnMapping SdbColumnMapping ;

enum SDB_PLAN_TYPE
{
   SDB_PLAN_SCAN = 0,
   SDB_PLAN_INSERT,
   SDB_PLAN_UPDATE,
   SDB_PLAN_DELETE,

   SDB_PLAN_UNKNOWN = 100
} ;
typedef enum SDB_PLAN_TYPE SDB_PLAN_TYPE ;

typedef struct PgColumnDesc_s
{
   char *pgname; /* PostgreSQL column name */
   int pgattnum; /* PostgreSQL attribute number */
   int attnum_in_target;
   Oid pgtype;   /* PostgreSQL data type */
   int pgtypmod; /* PostgreSQL type modifier */

   bool isDropped;
   bool isInReturningList;
}PgColumnsDesc;

typedef struct SdbExprTreeState_s
{
   INT32 unsupport_count;
   Index foreign_table_index;   /* range table index of foreign table */
   Oid foreign_table_id;      
}SdbExprTreeState;

typedef struct PgTableDesc_s
{
   char *name;    /* table name */
   int ncols;     /* number of columns */
   PgColumnsDesc *cols;
}PgTableDesc;

/* SdbExecState represents the runtime state for sdb cursor
 */
struct SdbExecState
{
   SDB_PLAN_TYPE planType ;
   struct HTAB *columnMappingHash ; /* map sdbbson fields to columns */
   sdbCursorHandle hCursor ;
   sdbConnectionHandle hConnection ;
   sdbCollectionHandle hCollection ;
   sdbbson queryDocument ; /* query request */

   /* sdb server options */
   char *sdbServerHost;
   char *sdbServerPort;
   char *usr;
   char *passwd;
   char *preferenceInstance;
   
   char *sdbcs;
   char *sdbcl;

   /* pg table column's description */
   PgTableDesc *pgTableDesc;

   /* foreign table's rowcount */
   SINT64 row_count;

   /* record the fetch column's bson address */
   UINT64 bson_record_addr;

   /* save the key column name to identify the specify column(0 for _id, others for the sharding key) */
   int key_num;
   char key_name[SDB_MAX_KEY_COLUMN_COUNT][SDB_MAX_KEY_COLUMN_LENGTH];
} ;
typedef struct SdbExecState SdbExecState ;


/* Sdb represents connection handle for each host:service */
struct SdbConnection
{
   /* transaction name include address:service */
   CHAR *connName ;
   sdbConnectionHandle hConnection ;
   /* by default 0 means there's no transaction started on this connection */
   INT32 transLevel ;
} ;
typedef struct SdbConnection SdbConnection ;

/* SdbTransList represents list of active transactions */
struct SdbConnectionPool
{
   INT32 numConnections ;
   INT32 poolSize ;
   SdbConnection *connList ;
} ;
typedef struct SdbConnectionPool  SdbConnectionPool ;


extern Datum sdb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum sdb_fdw_validator(PG_FUNCTION_ARGS);
#endif

