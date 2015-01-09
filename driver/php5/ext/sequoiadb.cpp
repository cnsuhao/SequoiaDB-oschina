/*******************************************************************************
   Copyright (C) 2012-2014 SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*******************************************************************************/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "function_sdb.h"
#include "php.h"
#include "php_ini.h"
#include "standard/info.h"
#include "php_sequoiadb.h"
#include "cJSON.h"
#include <zend_exceptions.h>

#ifndef PHP_FE_END
#define PHP_FE_END {NULL,NULL,NULL}
#endif

#define PRINTFERROR(in,out) \
{ \
   CHAR *temp = (CHAR *)emalloc ( 64 ) ; \
   if ( temp ) \
   { \
      ossMemset ( temp, 0, 64 ) ; \
      ossSnprintf ( temp, 64, "{\"errno\":%d}", (in) ) ; \
      out = temp ; \
   } \
   else \
   { \
      out = NULL ; \
   } \
}

/*#define PRINTFNAME(in,out) \
{ \
   INT32 len = ossStrlen ( (in) ) + 64 ; \
   CHAR *temp = (CHAR *)emalloc ( len ) ; \
   ossSnprintf ( temp, len, "{\"Name\":%s}", (in) ) ; \
   out = temp ; \
}*/

#define GETCLASSFROMZVAL(obj,name,classname,out) \
{ \
    zval *val = zend_read_property ( Z_OBJCE_P(obj),\
(obj), ZEND_STRL(name), 0 TSRMLS_CC ) ; \
  CHAR *temp = Z_STRVAL_P ( val ) ; \
  ossValuePtr num = (ossValuePtr)ossAtoll(temp) ; \
  out = *(classname**)&num ; \
}
/* double maybe will make bug in linux, now we use string, not double
#define SETCLASSFROMZVAL(obj,name,classname,in) \
{ \
  double temp = 0 ; \
  memset ( &temp, 0, sizeof(temp) ) ; \
  memcpy ( &temp, in, sizeof(classname*) ) ; \
  zend_update_property_double ( Z_OBJCE_P(obj), (obj), \
                                ZEND_STRL(name), *(double*)&temp TSRMLS_CC ) ; \
}
*/
#define SETZVAL(obj,name,in) \
{ \
  zend_update_property ( Z_OBJCE_P(obj), (obj), \
                         ZEND_STRL(name), in TSRMLS_CC ) ; \
}

#define SETSTRING(obj,name,in) \
{ \
  zend_update_property_string ( Z_OBJCE_P(obj), (obj), \
                                ZEND_STRL(name), in TSRMLS_CC ) ; \
}

#define GETZVAL(obj,name,out) \
{ \
   out = zend_read_property ( Z_OBJCE_P(obj), (obj),\
   ZEND_STRL(name), 0 TSRMLS_CC ) ; \
}

#define CREATECLASS(obj,name,classname,out) \
{ \
  classname *fsdb = new(std::nothrow) classname () ; \
  if ( !fsdb ) { out = NULL ; } \
  else { \
     CHAR temp[22] = {0} ; \
     ossSnprintf ( temp,22, "%lld", (UINT64)fsdb ) ;\
     zend_update_property_string ( Z_OBJCE_P(obj), (obj), \
ZEND_STRL(name), temp TSRMLS_CC ) ; \
     out = fsdb ; \
  } \
}

#define RETURN_ARRAY(in) \
{ \
   php_json_decode ( in, &return_value TSRMLS_CC ) ; \
   return ; \
}

#define RETURN_EMPTY_ARRAY() \
{ \
   array_init ( return_value ) ; \
   return ; \
}

#define HASH_P(a) (Z_TYPE_P(a) == IS_ARRAY ? Z_ARRVAL_P(a) : Z_OBJPROP_P(a))

#define RETURN_ARRAY_STRING(obj,in,a) \
{ \
   zval *sdb_obj = NULL ; \
   MAKE_STD_ZVAL ( sdb_obj ) ; \
   GETZVAL ( obj, "_connection", sdb_obj ) ; \
   zval *val = zend_read_property ( Z_OBJCE_P(sdb_obj), (sdb_obj),\
   ZEND_STRL("_return_model"), 0 TSRMLS_CC ) ; \
   INT32 return_model = Z_LVAL_P ( val ) ; \
   if ( 1 == return_model || \
        PHP_GET_VALUE_NOTFIND == return_model ) \
   { \
      RETURN_ARRAY ( in ) ; \
   } \
   else if ( 0 == return_model ) \
   { \
      RETURN_STRING ( in, a ) ; \
   } \
   else \
   { \
      return ; \
   } \
}

#define RETURN_ARRAY_STRING2(obj,in,a) \
{ \
   zval *sdb_obj = obj ; \
   zval *val = zend_read_property ( Z_OBJCE_P(sdb_obj), (sdb_obj),\
   ZEND_STRL("_return_model"), 0 TSRMLS_CC ) ; \
   INT32 return_model = Z_LVAL_P ( val ) ; \
   if ( 1 == return_model || \
        PHP_GET_VALUE_NOTFIND == return_model ) \
   { \
      RETURN_ARRAY ( in ) ; \
   } \
   else if ( 0 == return_model ) \
   { \
      RETURN_STRING ( in, a ) ; \
   } \
   else \
   { \
      return ; \
   } \
}

#define RETURN_EMPTY_ARRAY_STRING(obj) \
{ \
   zval *sdb_obj = NULL ; \
   MAKE_STD_ZVAL ( sdb_obj ) ; \
   GETZVAL ( obj, "_connection", sdb_obj ) ; \
   zval *val = zend_read_property ( Z_OBJCE_P(sdb_obj), (sdb_obj),\
ZEND_STRL("_return_model"), 0 TSRMLS_CC ) ; \
   INT32 return_model = Z_LVAL_P ( val ) ; \
   if ( 1 == return_model || \
        PHP_GET_VALUE_NOTFIND == return_model ) \
   { \
      RETURN_EMPTY_ARRAY () ; \
   } \
   else if ( 0 == return_model ) \
   { \
      RETURN_EMPTY_STRING () ; \
   } \
   else \
   { \
      return ; \
   } \
}

#define RETURN_EMPTY_ARRAY_STRING2(obj) \
{ \
   zval *sdb_obj = obj ; \
   zval *val = zend_read_property ( Z_OBJCE_P(sdb_obj), (sdb_obj),\
ZEND_STRL("_return_model"), 0 TSRMLS_CC ) ; \
   INT32 return_model = Z_LVAL_P ( val ) ; \
   if ( 1 == return_model || \
        PHP_GET_VALUE_NOTFIND == return_model ) \
   { \
      RETURN_EMPTY_ARRAY () ; \
   } \
   else if ( 0 == return_model ) \
   { \
      RETURN_EMPTY_STRING () ; \
   } \
   else \
   { \
      return ; \
   } \
}

#define SETERROR(obj,in) \
{ \
   zval *sdb_obj = NULL ; \
   MAKE_STD_ZVAL ( sdb_obj ) ; \
   GETZVAL ( obj, "_connection", sdb_obj ) ; \
   zend_update_property_long ( Z_OBJCE_P(sdb_obj), (sdb_obj), \
                               ZEND_STRL("_error"), in TSRMLS_CC ) ; \
}

#define SETERROR2(obj,in) \
{ \
   zend_update_property_long ( Z_OBJCE_P(obj), (obj), \
                               ZEND_STRL("_error"), in TSRMLS_CC ) ; \
}


static int le_sequoiadb;
static zend_class_entry *pSequoiadbSdb ;
static zend_class_entry *pSequoiadbCollectionSpace ;
static zend_class_entry *pSequoiadbCollection ;
static zend_class_entry *pSequoiadbCursor ;
static zend_class_entry *pSequoiadbId ;
static zend_class_entry *pSequoiadbData ;
static zend_class_entry *pSequoiadbTimeStamp ;
static zend_class_entry *pSequoiadbRegex ;
static zend_class_entry *pSequoiadbInt64 ;
static zend_class_entry *pSequoiadbGroup ;
static zend_class_entry *pSequoiadbReplicaNode ;
static zend_class_entry *pSequoiadbDomain ;

const zend_function_entry sequoiadb_sdb_functions[] = {
   PHP_ME ( SequoiaDB, __construct     , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, __destruct      , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, install         , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, getError        , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, connect         , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, close           , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, getSnapshot     , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, getList         , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, resetSnapshot   , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, selectCS        , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, listCSs         , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, listCollections , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, selectGroup     , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, execSQL         , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, execUpdateSQL   , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, createCataGroup , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, dropCollectionSpace , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, createDomain    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, dropDomain      , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, getDomain       , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDB, listDomains     , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoiadb_cs_functions[] = {
   PHP_ME ( SequoiaCS, __construct        , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCS, __destruct         , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCS, drop               , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCS, getName            , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCS, selectCollection   , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCS, dropCollection     , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoiadb_collection_functions[] = {

   PHP_ME ( SequoiaCL, __construct      , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, insert           , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, update           , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, remove           , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, drop             , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, find             , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, aggregate        , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, createIndex      , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, getIndex         , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, deleteIndex      , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, getCSName        , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, getCollectionName, NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, getFullName      , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, count            , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, __destruct       , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCL, split            , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoiadb_cursor_functions[] = {
   PHP_ME ( SequoiaCursor, __construct    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCursor, __destruct     , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCursor, getNext        , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaCursor, current        , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoiadb_domain_functions[] = {
   PHP_ME ( SequoiaDomain, alterDomain    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDomain, listCSInDomain , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDomain, listCLInDomain , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoia_id_functions[] = {
   PHP_ME ( SequoiaID, __construct    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaID, __toString     , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoia_date_functions[] = {
   PHP_ME ( SequoiaDate, __construct    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaDate, __toString     , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoia_timestamp_functions[] = {
   PHP_ME ( SequoiaTimestamp, __construct    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaTimestamp, __toString     , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoia_regex_functions[] = {
   PHP_ME ( SequoiaRegex, __construct    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaRegex, __toString     , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoia_int64_functions[] = {
   PHP_ME ( SequoiaINT64, __construct    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( SequoiaINT64, __toString     , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoia_group_functions[] = {
   PHP_ME ( sequoiaGroup, getNodeNum , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaGroup, getDetail  , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaGroup, getMaster  , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaGroup, getSlave   , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaGroup, getNode    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaGroup, createNode , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaGroup, start      , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaGroup, stop       , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaGroup, isCatalog  , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

const zend_function_entry sequoia_node_functions[] = {
   PHP_ME ( sequoiaNode, stop           , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaNode, start          , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaNode, getNodeName    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaNode, getServiceName , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaNode, getHostName    , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaNode, getStatus      , NULL, ZEND_ACC_PUBLIC )
   PHP_ME ( sequoiaNode, connect        , NULL, ZEND_ACC_PUBLIC )
   PHP_FE_END
};

zend_module_entry sequoiadb_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
   STANDARD_MODULE_HEADER,
#endif
   "Sequoiadb",
   sequoiadb_sdb_functions,
   PHP_MINIT(sequoiadb),
   PHP_MSHUTDOWN(sequoiadb),
   PHP_RINIT(sequoiadb),
   PHP_RSHUTDOWN(sequoiadb),
   PHP_MINFO(sequoiadb),
#if ZEND_MODULE_API_NO >= 20010901
   "0.1",
#endif
   STANDARD_MODULE_PROPERTIES
};


#ifdef COMPILE_DL_SEQUOIADB
ZEND_GET_MODULE(sequoiadb)
#endif

PHP_MINIT_FUNCTION(sequoiadb)
{
   zend_class_entry sequoiadbSdb ;
   zend_class_entry sequoiadbCollectionSpace ;
   zend_class_entry sequoiadbCollection ;
   zend_class_entry sequoiadbCursor ;
   zend_class_entry sequoiaDomain ;
   zend_class_entry sequoiadbID ;
   zend_class_entry sequoiadbDate ;
   zend_class_entry sequoiadbTimeStamp ;
   zend_class_entry sequoiaRegex ;
   zend_class_entry sequoiaINT64 ;
   zend_class_entry sequoiaGroup ;
   zend_class_entry sequoiaNode ;

   INIT_CLASS_ENTRY ( sequoiadbSdb, "SequoiaDB", sequoiadb_sdb_functions ) ;
   INIT_CLASS_ENTRY ( sequoiadbCollectionSpace,
                      "SequoiaCS",sequoiadb_cs_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiadbCollection,
                      "SequoiaCL",
                      sequoiadb_collection_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiadbCursor,
                      "SequoiaCursor", sequoiadb_cursor_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiaDomain,
                      "SequoiaDomain", sequoiadb_domain_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiadbID, "SequoiaID", sequoia_id_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiadbDate, "SequoiaDate", sequoia_date_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiadbTimeStamp,
                      "SequoiaTimestamp", sequoia_timestamp_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiaRegex,
                      "SequoiaRegex", sequoia_regex_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiaINT64,
                      "SequoiaINT64", sequoia_int64_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiaGroup,
                      "sequoiaGroup", sequoia_group_functions  ) ;
   INIT_CLASS_ENTRY ( sequoiaNode,
                      "sequoiaNode", sequoia_node_functions  ) ;

   pSequoiadbSdb             =
zend_register_internal_class( &sequoiadbSdb TSRMLS_CC ) ;
   pSequoiadbCollectionSpace =
zend_register_internal_class( &sequoiadbCollectionSpace TSRMLS_CC ) ;
   pSequoiadbCollection      =
zend_register_internal_class( &sequoiadbCollection TSRMLS_CC ) ;
   pSequoiadbCursor          =
zend_register_internal_class( &sequoiadbCursor TSRMLS_CC ) ;
   pSequoiadbDomain          =
zend_register_internal_class( &sequoiaDomain TSRMLS_CC ) ;
   pSequoiadbId              =
zend_register_internal_class( &sequoiadbID TSRMLS_CC ) ;
   pSequoiadbData            =
zend_register_internal_class( &sequoiadbDate TSRMLS_CC ) ;
   pSequoiadbTimeStamp       =
zend_register_internal_class( &sequoiadbTimeStamp TSRMLS_CC ) ;
   pSequoiadbRegex           =
zend_register_internal_class( &sequoiaRegex TSRMLS_CC ) ;
   pSequoiadbInt64           =
zend_register_internal_class( &sequoiaINT64 TSRMLS_CC ) ;
   pSequoiadbGroup    =
zend_register_internal_class( &sequoiaGroup TSRMLS_CC ) ;
   pSequoiadbReplicaNode     =
zend_register_internal_class( &sequoiaNode TSRMLS_CC ) ;


   zend_declare_property_null( pSequoiadbSdb,
                               ZEND_STRL("_connection"),
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_long( pSequoiadbSdb,
                               ZEND_STRL("_return_model"),
                               1,
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_long( pSequoiadbSdb,
                               ZEND_STRL("_error"),
                               0,
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbCollectionSpace,
                               ZEND_STRL("_collectionSpace"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbCollectionSpace,
                               ZEND_STRL("_connection"),
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbCollection,
                               ZEND_STRL("_collection"),
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbCollection,
                               ZEND_STRL("_connection"),
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbDomain,
                               ZEND_STRL("_domain"),
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbDomain,
                               ZEND_STRL("_connection"),
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbCursor,
                               ZEND_STRL("_cursor"),
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbCursor,
                               ZEND_STRL("_connection"),
                               ZEND_ACC_PRIVATE TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbId,
                               ZEND_STRL("$oid"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbData,
                               ZEND_STRL("$date"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbTimeStamp,
                               ZEND_STRL("$timestamp"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbRegex,
                               ZEND_STRL("$regex"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbRegex,
                               ZEND_STRL("$options"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;

   zend_declare_property_null( pSequoiadbInt64,
                               ZEND_STRL("INT64"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;

   zend_declare_property_null( pSequoiadbGroup,
                               ZEND_STRL("_sdbReplicaGroup"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbGroup,
                               ZEND_STRL("_connection"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;

   zend_declare_property_null( pSequoiadbReplicaNode,
                               ZEND_STRL("_sdbNode"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;
   zend_declare_property_null( pSequoiadbReplicaNode,
                               ZEND_STRL("_connection"),
                               ZEND_ACC_PUBLIC TSRMLS_CC ) ;

   REGISTER_LONG_CONSTANT ( "SDB_SNAP_CONTEXTS",
                            0, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_SNAP_CONTEXTS_CURRENT",
                            1, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_SNAP_SESSIONS",
                            2, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_SNAP_SESSIONS_CURRENT",
                            3, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_SNAP_COLLECTION",
                            4, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_SNAP_COLLECTIONSPACE",
                            5, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_SNAP_DATABASE",
                            6, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_SNAP_SYSTEM",
                            7, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_SNAP_CATALOG",
                            8, CONST_CS | CONST_PERSISTENT ) ;

   REGISTER_LONG_CONSTANT ( "SDB_LIST_CONTEXTS",
                            0, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_LIST_CONTEXTS_CURRENT",
                            1, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_LIST_SESSIONS",
                            2, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_LIST_SESSIONS_CURRENT",
                            3, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_LIST_COLLECTIONS",
                            4, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_LIST_COLLECTIONSPACES",
                            5, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_LIST_STORAGEUNITS",
                            6, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_LIST_GROUPS",
                            7, CONST_CS | CONST_PERSISTENT ) ; //waste
   REGISTER_LONG_CONSTANT ( "SDB_LIST_SHARDS",
                            7, CONST_CS | CONST_PERSISTENT ) ;

   REGISTER_LONG_CONSTANT ( "SDB_NODE_ALL",
                            0, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_NODE_ACTIVE",
                            1, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_NODE_INACTIVE",
                            2, CONST_CS | CONST_PERSISTENT ) ;
   REGISTER_LONG_CONSTANT ( "SDB_NODE_UNKNOWN",
                            3, CONST_CS | CONST_PERSISTENT ) ;
   return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(sequoiadb)
{
   return SUCCESS;
}

PHP_RINIT_FUNCTION(sequoiadb)
{
   return SUCCESS;
}

PHP_RSHUTDOWN_FUNCTION(sequoiadb)
{
   return SUCCESS;
}

PHP_MINFO_FUNCTION(sequoiadb)
{
   php_info_print_table_start();
   php_info_print_table_header(2, "sequoiadb support", "enabled");
   php_info_print_table_end();
}


/* Remove the following function when you have succesfully modified config.m4
   so that your module can be compiled into PHP, it exists only for testing
   purposes. */

/* Every user-visible function in PHP should document itself in the source */
/* {{{ proto string confirm_sequoiadb_compiled(string arg)
   Return a string to confirm that the module is compiled in */

/* *****************  Sequoiadb class  ****************/

PHP_METHOD ( SequoiaDB, __construct )
{
   INT32 rc = SDB_OK ;
   CHAR *hostName     = NULL ;
   INT32 hostName_len = 0    ;
   sdb *connection    = NULL ;
   CHAR *userName     = NULL ;
   INT32 userName_len = 0    ;
   CHAR *password     = NULL ;
   INT32 password_len = 0    ;

   CREATECLASS ( getThis(), "_connection", sdb, connection ) ;
   if ( connection )
   {
      if ( !(zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                     "|sss",
                                     &hostName,
                                     &hostName_len,
                                     &userName,
                                     &userName_len,
                                     &password,
                                     &password_len ) == FAILURE ) )
      {
         if ( hostName_len > 0 )
         {
            if ( userName_len > 0 && password_len > 0 )
            {
               rc = connect ( connection, hostName, userName, password ) ;
            }
            else
            {
               rc = connect ( connection, hostName ) ;
            }
            SETERROR2 ( getThis(), rc ) ;
         }
      }
   }
}

PHP_METHOD ( SequoiaDB, connect )
{
   INT32 rc = SDB_OK ;
   CHAR *error        = NULL ;
   sdb *connection    = NULL ;
   CHAR *hostName     = NULL ;
   INT32 hostName_len = 0    ;
   CHAR *userName     = NULL ;
   INT32 userName_len = 0    ;
   CHAR *password     = NULL ;
   INT32 password_len = 0    ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s|ss",
                                &hostName,
                                &hostName_len,
                                &userName,
                                &userName_len,
                                &password,
                                &password_len ) == FAILURE )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }
   if ( userName_len > 0 && password_len > 0 )
   {
      rc = connect ( connection, hostName, userName, password ) ;
   }
   else
   {
      rc = connect ( connection, hostName ) ;
   }
   SETERROR2 ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaDB, install )
{
   zval *install      = NULL ;
   INT32 return_model = 1 ;
   INT32 auto_disconnect = 1 ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z",
                                &install ) == FAILURE )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      return ;
   }
   return_model = key_get_value ( install, "install" TSRMLS_CC ) ;
   auto_disconnect = key_get_value ( install, "autoDisconnect" TSRMLS_CC ) ;
   if ( return_model != -1 )
   {
      zend_update_property_long ( Z_OBJCE_P(getThis()), getThis(),
                                  ZEND_STRL ("_return_model"),
                                  return_model TSRMLS_CC ) ;
   }
   /*if ( auto_disconnect != -1 )
   {
      zend_update_property_long ( Z_OBJCE_P(getThis()), getThis(),
                                  ZEND_STRL ("_auto_disconnect"),
                                  auto_disconnect TSRMLS_CC ) ;
   }*/
}

PHP_METHOD ( SequoiaDB, getError )
{
   zval *obj = getThis() ;
   zval *val = zend_read_property ( Z_OBJCE_P(obj),
                                    obj, ZEND_STRL("_error"),
                                    0 TSRMLS_CC ) ;
   INT32 errorNum = Z_LVAL_P ( val ) ;
   CHAR *buf = NULL ;
   PRINTFERROR ( errorNum, buf ) ;
   SETERROR2 ( obj, 0 ) ;
   if ( buf )
   {
      RETURN_ARRAY_STRING2 ( obj, buf, 0 ) ;
   }
   RETURN_EMPTY_ARRAY_STRING2 ( getThis() ) ;
}

PHP_METHOD ( SequoiaDB, close )
{
   INT32 rc = SDB_OK ;
   CHAR *error     = NULL ;
   sdb *connection = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( connection )
   {
      close ( connection ) ;
   }
}

PHP_METHOD ( SequoiaDB, getSnapshot )
{
   INT32 rc = SDB_OK ;
   sdbCursor *query = NULL ;
   zval* cursor_obj = NULL ;
   sdb *connection  = NULL ;

   zval *pListType   = NULL ;
   zval *pCondition  = NULL ;
   zval *pSelected   = NULL ;
   zval *pOrderBy    = NULL ;
   CHAR *condition   = NULL ;
   CHAR *selected    = NULL ;
   CHAR *orderBy     = NULL ;
   INT32 listType    = 0 ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS() TSRMLS_CC,
                                "|zzzz",
                                &pListType,
                                &pCondition,
                                &pSelected,
                                &pOrderBy ) == FAILURE )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   if ( pListType )
   {
      if ( IS_LONG == Z_TYPE_P ( pListType ) )
      {
         listType = Z_LVAL_P ( pListType ) ;
      }
      else if ( IS_STRING == Z_TYPE_P ( pListType ) )
      {
         CHAR *num = Z_STRVAL_P ( pListType ) ;
         listType = ossAtoi ( num ) ;
      }
      else
      {
         SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
         RETURN_NULL() ;
      }
   }
   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   if ( !php_toJson ( &condition, pCondition TSRMLS_CC ) ||
        !php_toJson ( &selected , pSelected  TSRMLS_CC ) ||
        !php_toJson ( &orderBy  , pOrderBy   TSRMLS_CC ) )
   {
      SETERROR2 ( getThis(), SDB_INVALIDARG ) ;
      RETURN_NULL() ;
   }

   rc = getSnapshot ( connection,
                      &query, listType, condition, selected, orderBy ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( cursor_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, getList )
{
   INT32 rc = SDB_OK ;
   sdbCursor *query = NULL ;
   zval* cursor_obj = NULL ;
   sdb *connection  = NULL ;

   zval *pListType   = NULL ;
   zval *pCondition  = NULL ;
   zval *pSelected   = NULL ;
   zval *pOrderBy    = NULL ;
   CHAR *condition   = NULL ;
   CHAR *selected    = NULL ;
   CHAR *orderBy     = NULL ;
   INT32 listType    = 0 ;
   if ( zend_parse_parameters ( ZEND_NUM_ARGS() TSRMLS_CC,
                                "|zzzz",
                                &pListType,
                                &pCondition,
                                &pSelected,
                                &pOrderBy ) == FAILURE )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   if ( pListType )
   {
      if ( IS_LONG == Z_TYPE_P ( pListType ) )
      {
         listType = Z_LVAL_P ( pListType ) ;
      }
      else if ( IS_STRING == Z_TYPE_P ( pListType ) )
      {
         CHAR *num = Z_STRVAL_P ( pListType ) ;
         listType = ossAtoi ( num ) ;
      }
      else
      {
         SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
         RETURN_NULL() ;
      }
   }
   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   if ( !php_toJson ( &condition, pCondition TSRMLS_CC ) ||
        !php_toJson ( &selected , pSelected  TSRMLS_CC ) ||
        !php_toJson ( &orderBy  , pOrderBy   TSRMLS_CC ) )
   {
      SETERROR2 ( getThis(), SDB_INVALIDARG ) ;
      RETURN_NULL() ;
   }

   rc = getList ( connection, &query, listType, condition, selected, orderBy ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( cursor_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, resetSnapshot )
{
   INT32 rc = SDB_OK ;
   sdb *connection  = NULL ;
   CHAR *error      = NULL ;
   zval *pCondition = NULL ;
   CHAR *condition  = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "|z",
                                &pCondition ) == FAILURE )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   if ( !php_toJson ( &condition, pCondition TSRMLS_CC ) )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   rc = resetSnapshot ( connection, condition ) ;
   SETERROR2 ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaDB, selectCS )
{
   INT32 rc = SDB_OK ;
   CHAR *csName           = NULL ;
   INT32 csName_len       = 0    ;
   sdb *connection        = NULL ;
   sdbCollectionSpace *cs = NULL ;
   zval* cs_obj           = NULL ;
   CHAR *pError           = NULL ;
   zval *pPageSize        = NULL ;
   CHAR *options          = NULL ;
   INT32 pageSize         = 4096 ;
   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s|z",
                                &csName,
                                &csName_len,
                                &pPageSize ) == FAILURE )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   MAKE_STD_ZVAL ( cs_obj ) ;
   object_init_ex ( cs_obj, pSequoiadbCollectionSpace ) ;
   CREATECLASS ( cs_obj, "_collectionSpace", sdbCollectionSpace, cs ) ;
   if ( !cs )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   if ( pPageSize )
   {
      if ( IS_LONG == Z_TYPE_P ( pPageSize ) )
      {
         pageSize = Z_LVAL_P ( pPageSize ) ;
         rc = selectCollectionSpace ( connection, &cs, csName, pageSize ) ;
      }
      else if ( IS_STRING == Z_TYPE_P ( pPageSize ) )
      {
         if ( !php_toJson ( &options, pPageSize TSRMLS_CC ) )
         {
            SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
            RETURN_NULL() ;
         }
         rc = selectCollectionSpace2 ( connection, &cs, csName, options ) ;
      }
      else
      {
         SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
         RETURN_NULL() ;
      }
   }
   else
   {
      rc = selectCollectionSpace ( connection, &cs, csName, pageSize ) ;
   }
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( cs_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( cs_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, selectGroup )
{
   INT32 rc = SDB_OK ;
   CHAR *grName           = NULL ;
   INT32 grName_len       = 0    ;
   sdb *connection        = NULL ;
   sdbReplicaGroup *gr    = NULL ;
   zval* gr_obj           = NULL ;
   CHAR *pError           = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &grName,
                                &grName_len ) == FAILURE )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   MAKE_STD_ZVAL ( gr_obj ) ;
   object_init_ex ( gr_obj, pSequoiadbGroup ) ;
   CREATECLASS ( gr_obj, "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = selectGroup ( connection, &gr, grName ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( gr_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( gr_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, execSQL )
{
   INT32 rc = SDB_OK ;
   sdb *connection  = NULL ;
   sdbCursor *query = NULL ;
   zval* cursor_obj = NULL ;
   CHAR *sql        = NULL ;
   INT32 sqlSize    = 0 ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &sql,
                                &sqlSize ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = execSQL ( connection, sql, &query ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( cursor_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, execUpdateSQL )
{
   INT32 rc = SDB_OK ;
   sdb *connection  = NULL ;
   CHAR *sql        = NULL ;
   INT32 sqlSize    = 0 ;
   CHAR *error      = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &sql,
                                &sqlSize ) == FAILURE )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   rc = execSQL ( connection, sql ) ;
   PRINTFERROR ( rc, error ) ;
   SETERROR2 ( getThis(), rc ) ;
   RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaDB, listCSs )
{
   INT32 rc = SDB_OK ;
   sdb *connection  = NULL ;
   sdbCursor *query = NULL ;
   zval* cursor_obj = NULL ;

   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = listCollectionSpaces ( connection, &query ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( cursor_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, listCollections )
{
   INT32 rc = SDB_OK ;
   sdb *connection  = NULL ;
   sdbCursor *query = NULL ;
   zval* cursor_obj = NULL ;

   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = listCollections ( connection, &query ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( cursor_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, createCataGroup )
{
   INT32 rc = SDB_OK ;
   sdb *connection  = NULL ;
   CHAR *error      = NULL ;
   CHAR *hostName           = NULL ;
   INT32 hostName_len       = 0    ;
   CHAR *serviceName        = NULL ;
   INT32 serviceName_len    = 0    ;
   CHAR *databasePath       = NULL ;
   INT32 databasePath_len   = 0    ;
   zval *pConfig            = NULL ;
   CHAR *config             = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "sss|z",
                                &hostName,
                                &hostName_len,
                                &serviceName,
                                &serviceName_len,
                                &databasePath,
                                &databasePath_len,
                                &pConfig ) == FAILURE )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   if ( !php_toJson ( &config, pConfig TSRMLS_CC ) )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }

   rc = createCataGroup ( connection,
                          hostName,
                          serviceName,
                          databasePath,
                          config ) ;
   PRINTFERROR ( rc, error ) ;
   SETERROR2 ( getThis(), rc ) ;
   RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaDB, dropCollectionSpace )
{
   INT32 rc = SDB_OK ;
   CHAR *error      = NULL ;
   sdb *connection  = NULL ;
   CHAR *pCsName    = NULL ;
   INT32 csNamelen  = 0    ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &pCsName,
                                &csNamelen ) == FAILURE )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }
   rc = dropCollectionSpace( connection, pCsName ) ;
   PRINTFERROR ( rc, error ) ;
   SETERROR2 ( getThis(), rc ) ;
   RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaDB, createDomain )
{
   INT32 rc = SDB_OK ;
   CHAR *name           = NULL ;
   INT32 name_len       = 0    ;
   zval *pOptions       = NULL ;
   CHAR *options        = NULL ;
   zval* domain_obj     = NULL ;
   sdb *connection      = NULL ;
   sdbDomain *domain    = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "sz",
                                &name,
                                &name_len,
                                &pOptions ) == FAILURE )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   MAKE_STD_ZVAL ( domain_obj ) ;
   object_init_ex ( domain_obj, pSequoiadbDomain ) ;
   CREATECLASS ( domain_obj, "_domain", sdbDomain, domain ) ;
   if ( !domain )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   if ( !php_toJson ( &options, pOptions TSRMLS_CC ) )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = createDomain( connection, name, options, &domain ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( domain_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( domain_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, dropDomain )
{
   INT32 rc = SDB_OK ;
   CHAR *error     = NULL ;
   sdb *connection = NULL ;
   CHAR *pName     = NULL ;
   INT32 namelen   = 0    ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &pName,
                                &namelen ) == FAILURE )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
   }
   rc = dropDomain( connection, pName ) ;
   PRINTFERROR ( rc, error ) ;
   SETERROR2 ( getThis(), rc ) ;
   RETURN_ARRAY_STRING2 ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaDB, getDomain )
{
   INT32 rc = SDB_OK ;
   sdb *connection   = NULL ;
   sdbDomain *domain = NULL ;
   zval* domain_obj  = NULL ;
   CHAR *pName       = NULL ;
   INT32 namelen     = 0    ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &pName,
                                &namelen ) == FAILURE )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   MAKE_STD_ZVAL ( domain_obj ) ;
   object_init_ex ( domain_obj, pSequoiadbDomain ) ;
   CREATECLASS ( domain_obj, "_domain", sdbDomain, domain ) ;
   if ( !domain )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = getDomain ( connection, pName, &domain ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( domain_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( domain_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, listDomains )
{
   INT32 rc = SDB_OK ;
   sdbCursor *query = NULL ;
   zval* cursor_obj = NULL ;
   sdb *connection  = NULL ;

   zval *pCondition  = NULL ;
   zval *pSelected   = NULL ;
   zval *pOrderBy    = NULL ;
   zval *pHint       = NULL ;
   CHAR *condition   = NULL ;
   CHAR *selected    = NULL ;
   CHAR *orderBy     = NULL ;
   CHAR *hint        = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS() TSRMLS_CC,
                                "|zzzz",
                                &pCondition,
                                &pSelected,
                                &pOrderBy,
                                &pHint ) == FAILURE )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   if ( !php_toJson ( &condition, pCondition TSRMLS_CC ) ||
        !php_toJson ( &selected , pSelected  TSRMLS_CC ) ||
        !php_toJson ( &orderBy  , pOrderBy   TSRMLS_CC ) ||
        !php_toJson ( &hint     , pHint      TSRMLS_CC ) )
   {
      SETERROR2 ( getThis(), SDB_INVALIDARG ) ;
      RETURN_NULL() ;
   }

   rc = listDomains ( connection, &query, condition, selected, orderBy, hint ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   SETZVAL ( cursor_obj, "_connection", getThis() ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDB, __destruct )
{
   sdb *connection = NULL ;
   zval *num_val = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_connection", sdb, connection ) ;
   if ( connection )
   {
      close ( connection ) ;
      delete connection ;
   }
}

/* **************  SequoiaCS class  ****************/

PHP_METHOD ( SequoiaCS, __construct )
{

}
/*
PHP_METHOD ( SequoiaCS, install )
{
   zval *install     = NULL ;
   INT32 return_model = 0 ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z",
                                &install ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      return ;
   }
   return_model = key_get_value ( install, "install" TSRMLS_CC ) ;
   zend_update_property_long ( Z_OBJCE_P(getThis()), getThis(),
                               ZEND_STRL ("_return_model"),
                               return_model TSRMLS_CC ) ;
}
*/

PHP_METHOD ( SequoiaCS, selectCollection )
{
   INT32 rc = SDB_OK ;
   CHAR *clName              = NULL ;
   INT32 clName_len          = 0    ;
   zval* collection_obj      = NULL ;
   sdbCollectionSpace *cs    = NULL ;
   sdbCollection *collection = NULL ;
   zval *pShardingKey        = NULL ;
   CHAR *shardingKey         = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s|z",
                                &clName,
                                &clName_len,
                                &pShardingKey ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collectionSpace", sdbCollectionSpace, cs ) ;
   if ( !cs )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   if ( !php_toJson ( &shardingKey, pShardingKey TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      RETURN_NULL() ;
   }
   MAKE_STD_ZVAL ( collection_obj ) ;
   object_init_ex ( collection_obj, pSequoiadbCollection ) ;
   CREATECLASS ( collection_obj, "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = selectCollection ( cs, &collection, clName, shardingKey ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( collection_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( collection_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaCS, drop )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   sdbCollectionSpace *cs = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_collectionSpace", sdbCollectionSpace, cs ) ;
   if ( !cs )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = collectionSpaceDrop ( cs ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaCS, dropCollection )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   CHAR *clName              = NULL ;
   INT32 clName_len          = 0    ;
   sdbCollectionSpace *cs    = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &clName,
                                &clName_len ) == FAILURE )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collectionSpace", sdbCollectionSpace, cs ) ;
   if ( !cs )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = dropCollection( cs, clName ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaCS, getName )
{
   const CHAR *name = NULL ;
   sdbCollectionSpace *cs = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_collectionSpace", sdbCollectionSpace, cs ) ;
   if ( !cs )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_STRING ( ) ;
   }
   name = getCSName ( cs ) ;
   RETURN_STRING ( name, 1 ) ;
}

PHP_METHOD ( SequoiaCS, __destruct )
{
   sdbCollectionSpace *cs = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_collectionSpace", sdbCollectionSpace, cs ) ;
   if ( cs )
   {
      delete cs ;
   }
}
/* **************  SequoiaCL class  ****************/

PHP_METHOD ( SequoiaCL, __construct )
{

}
/*
PHP_METHOD ( SequoiaCL, install )
{
   zval *install      = NULL ;
   INT32 return_model = 0 ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z",
                                &install ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      return ;
   }
   return_model = key_get_value ( install, "install" TSRMLS_CC ) ;
   zend_update_property_long ( Z_OBJCE_P(getThis()), getThis(),
                               ZEND_STRL ("_return_model"),
                               return_model TSRMLS_CC ) ;
}
*/



PHP_METHOD ( SequoiaCL, drop )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   sdbCollection *collection = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = collectionDrop ( collection ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaCL, insert )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   sdbCollection *collection = NULL ;

   zval *pRecord  = NULL ;
   CHAR *record   = NULL ;
   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z",
                                &pRecord ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   if ( !php_toJson ( &record, pRecord TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      PRINTFERROR ( SDB_INVALIDARG, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   rc = insertData ( collection, &record ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      PRINTFERROR ( rc, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   INT32 record_strlen = ossStrlen ( record ) + 32 ;
   CHAR *temp = (CHAR *)emalloc ( record_strlen ) ;
   ossSnprintf ( temp, record_strlen, "{\"errno\":0,\"_id\":%s}", record ) ;
   free ( record ) ;
   RETURN_ARRAY_STRING ( getThis(), temp, 0 ) ;
}

PHP_METHOD ( SequoiaCL, update )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   sdbCollection *collection = NULL ;

   zval *pRule       = NULL ;
   zval *pCondition  = NULL ;
   zval *pHint       = NULL ;
   CHAR *rule        = NULL ;
   CHAR *condition   = NULL ;
   CHAR *hint        = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z|zz",
                                &pRule,
                                &pCondition,
                                &pHint ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   if ( !php_toJson ( &rule,       pRule       TSRMLS_CC ) ||
        !php_toJson ( &condition , pCondition  TSRMLS_CC ) ||
        !php_toJson ( &hint  ,     pHint       TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      PRINTFERROR ( SDB_INVALIDARG, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   rc = updateData ( collection, rule, condition, hint ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaCL, remove )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   sdbCollection *collection = NULL ;

   zval *pCondition  = NULL ;
   zval *pHint       = NULL ;
   CHAR *condition   = NULL ;
   CHAR *hint        = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "|zz",
                                &pCondition,
                                &pHint ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   if ( !php_toJson ( &condition, pCondition TSRMLS_CC ) ||
        !php_toJson ( &hint     , pHint      TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      PRINTFERROR ( SDB_INVALIDARG, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   rc = deleteData ( collection, condition, hint ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

/*
PHP_METHOD ( SequoiaCL, rename )
{
   INT32 rc = SDB_OK ;
   CHAR *error     = NULL ;
   CHAR *pName     = NULL ;
   INT32 pName_len = 0    ;
   sdbCollection *collection = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &pName,
                                &pName_len ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = collectionRename ( collection, pName ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}*/

PHP_METHOD ( SequoiaCL, split )
{
   INT32 rc = SDB_OK ;
   CHAR *error     = NULL ;
   CHAR *pSourceName         = NULL ;
   INT32 pSourceName_len     = 0    ;
   CHAR *pDestName           = NULL ;
   INT32 pDestName_len       = 0    ;
   sdbCollection *collection = NULL ;
   zval *pCondition          = NULL ;
   CHAR *condition           = NULL ;
   zval *pEndCondition       = NULL ;
   CHAR *endCondition        = NULL ;
   FLOAT64 percent           = 0 ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "ssz|z",
                                &pSourceName,
                                &pSourceName_len,
                                &pDestName,
                                &pDestName_len,
                                &pCondition,
                                &pEndCondition ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   if ( IS_DOUBLE == Z_TYPE_P ( pCondition ) )
   {
      percent = Z_DVAL_P ( pCondition ) ;
      rc = splitData2 ( collection, pSourceName, pDestName, percent ) ;
   }
   else
   {
      if ( !php_toJson ( &condition, pCondition TSRMLS_CC ) )
      {
         SETERROR ( getThis(), SDB_INVALIDARG ) ;
         PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
         RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
      }
      if ( !php_toJson ( &endCondition, pEndCondition TSRMLS_CC ) )
      {
         SETERROR ( getThis(), SDB_INVALIDARG ) ;
         PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
         RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
      }
      rc = splitData ( collection,
                       pSourceName,
                       pDestName,
                       condition,
                       endCondition ) ;
   }
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaCL, find )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   sdbCollection *collection = NULL ;
   sdbCursor *query = NULL ;
   zval *cursor_obj = NULL ;

   zval *pCondition    = NULL ;
   zval *pSelected     = NULL ;
   zval *pOrderBy      = NULL ;
   zval *pHint         = NULL ;
   CHAR *condition     = NULL ;
   CHAR *selected      = NULL ;
   CHAR *orderBy       = NULL ;
   CHAR *hint          = NULL ;
   zval *pNumToSkip    = NULL ;
   zval *pNumToReturn  = NULL ;
   INT64 numToSkip64     = 0  ;
   INT64 numToReturn64   = -1 ;
   
   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "|zzzzzz",
                                &pCondition,
                                &pSelected,
                                &pOrderBy,
                                &pHint,
                                &pNumToSkip,
                                &pNumToReturn ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL () ;
   }

   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL () ;
   }

   if ( !php_toJson ( &condition, pCondition TSRMLS_CC ) ||
        !php_toJson ( &selected , pSelected  TSRMLS_CC ) ||
        !php_toJson ( &orderBy  , pOrderBy   TSRMLS_CC ) ||
        !php_toJson ( &hint     , pHint      TSRMLS_CC ) ||
        !php_toNum64 ( numToSkip64, pNumToSkip TSRMLS_CC ) ||
        !php_toNum64 ( numToReturn64, pNumToReturn TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      RETURN_NULL() ;
   }

   rc = queryData ( collection, &query,
                    condition,
                    selected,
                    orderBy,
                    hint,
                    numToSkip64,
                    numToReturn64 ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL () ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( cursor_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaCL, aggregate )
{
   INT32 rc = SDB_OK ;
   std::vector<CHAR *> vector ;
   CHAR *error = NULL ;
   sdbCollection *collection = NULL ;
   sdbCursor *query = NULL ;
   zval *cursor_obj = NULL ;
   zval *pbson_obj  = NULL ;
   
   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z",
                                &pbson_obj ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL () ;
   }

   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL () ;
   }

   if ( !php_jsonArr2Vector ( &vector, pbson_obj TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      RETURN_NULL() ;
   }

   rc = aggregate ( collection, &query,
                    vector ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL () ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( cursor_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaCL, createIndex )
{
   INT32 rc = SDB_OK ;
   sdbCollection *collection = NULL ;
   CHAR *error = NULL ;

   zval *pIndexDef    = NULL ;
   CHAR *indexDef     = NULL ;
   CHAR *pName        = NULL ;
   INT32 pName_len    = 0    ;
   BOOLEAN isUnique   = FALSE ;
   BOOLEAN isEnforced = FALSE ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "zs|bb",
                                &pIndexDef,
                                &pName,
                                &pName_len,
                                &isUnique,
                                &isEnforced ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   if ( !php_toJson ( &indexDef, pIndexDef TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      PRINTFERROR ( SDB_INVALIDARG, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   rc = createIndex ( collection, indexDef, pName, isUnique, isEnforced ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaCL, getIndex )
{
   INT32 rc = SDB_OK ;
   sdbCollection *collection = NULL ;
   sdbCursor *query = NULL ;
   CHAR *pName      = NULL ;
   INT32 pName_len  = 0    ;
   zval *cursor_obj = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "|s",
                                &pName,
                                &pName_len ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL () ;
   }
   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL () ;
   }

   rc = getIndex ( collection, &query, pName_len > 0 ? pName : NULL ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL () ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( cursor_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaCL, deleteIndex )
{
   INT32 rc = SDB_OK ;
   sdbCollection *collection = NULL ;
   CHAR *error     = NULL ;
   CHAR *pName     = NULL ;
   INT32 pName_len = 0    ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &pName,
                                &pName_len ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = dropIndex ( collection, pName ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaCL, getCSName )
{
   const CHAR *name = NULL ;
   sdbCollection *collection = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_STRING ( ) ;
   }
   name = getCSName ( collection ) ;
   RETURN_STRING ( name, 1 ) ;
}

PHP_METHOD ( SequoiaCL, getCollectionName )
{
   const CHAR *name = NULL ;
   sdbCollection *collection = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_STRING ( ) ;
   }
   name = getClName ( collection ) ;
   RETURN_STRING ( name, 1 ) ;
}

PHP_METHOD ( SequoiaCL, getFullName )
{
   const CHAR *name = NULL ;
   sdbCollection *collection = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_STRING ( ) ;
   }
   name = getFullName ( collection ) ;
   RETURN_STRING ( name, 1 ) ;
}

PHP_METHOD ( SequoiaCL, count )
{
   INT32 rc = SDB_OK ;
   sdbCollection *collection = NULL ;
   INT64 count = 0 ;

   zval *pCondition = NULL ;
   CHAR *condition = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "|z",
                                &pCondition ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_LONG ( -1 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( !collection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_LONG ( -1 ) ;
   }

   if ( !php_toJson ( &condition, pCondition TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      RETURN_LONG ( -1 ) ;
   }

   rc = getCount ( collection, condition, count ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_LONG ( -1 ) ;
   }
   RETURN_LONG ( count ) ;
}

PHP_METHOD ( SequoiaCL, __destruct )
{
   sdbCollection *collection = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_collection", sdbCollection, collection ) ;
   if ( collection )
   {
      delete collection ;
   }
}

/* **************  Sequoiadb_cursor class  ****************/

PHP_METHOD ( SequoiaCursor, __construct )
{

}
/*
PHP_METHOD ( SequoiaCursor, install )
{
   zval *install = NULL ;
   INT32 return_model = 0 ;
   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z",
                                &install ) == FAILURE )
   {
         return ;
   }
   return_model = key_get_value ( install, "install" TSRMLS_CC ) ;
   zend_update_property_long ( Z_OBJCE_P(getThis()), getThis(),
                               ZEND_STRL ("_return_model"),
                               return_model TSRMLS_CC ) ;
}
*/


PHP_METHOD ( SequoiaCursor, getNext )
{
   INT32 rc = SDB_OK ;
   CHAR *out = NULL ;
   INT32 outSize = 0 ;
   sdbCursor *query = NULL ;
   CHAR *pBuf = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_ARRAY_STRING ( getThis() ) ;
   }

   rc = getNext ( query, &out, &outSize ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      if ( out )
      {
         free ( out ) ;
      }
      RETURN_EMPTY_ARRAY_STRING ( getThis() ) ;
   }
   pBuf = (CHAR *)emalloc ( outSize ) ;
   if ( !pBuf )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_ARRAY_STRING ( getThis() ) ;
   }
   memcpy ( pBuf, out, outSize ) ;
   free ( out ) ;
   RETURN_ARRAY_STRING ( getThis(), pBuf, 0 ) ;
}

PHP_METHOD ( SequoiaCursor, current )
{
   INT32 rc = SDB_OK ;
   CHAR *out        = NULL ;
   INT32 outSize    = 0 ;
   sdbCursor *query = NULL ;
   CHAR *pBuf       = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_ARRAY_STRING ( getThis() ) ;
   }

   rc = current ( query, &out, &outSize ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      if ( out )
      {
         free ( out ) ;
      }
      RETURN_EMPTY_ARRAY_STRING ( getThis() ) ;
   }
   pBuf = (CHAR *)emalloc ( outSize ) ;
   memcpy ( pBuf, out, outSize ) ;
   free ( out ) ;
   RETURN_ARRAY_STRING ( getThis(), pBuf, 0 ) ;
}
/*
PHP_METHOD ( SequoiaCursor, updateCurrent )
{
   INT32 rc = SDB_OK ;
   CHAR *error     = NULL ;
   sdbCursor *query = NULL ;

   zval *pRule  = NULL ;
   CHAR *rule   = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z",
                                &pRule ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   if ( !php_toJson ( &rule, pRule TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      PRINTFERROR ( SDB_INVALIDARG, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }

   rc = updateCurrent ( query, rule ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaCursor, deleteCurrent )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   sdbCursor *query = NULL ;

   GETCLASSFROMZVAL ( getThis(), "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = delCurrent ( query ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}*/

PHP_METHOD ( SequoiaCursor, __destruct )
{
   sdbCursor *query = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_cursor", sdbCursor, query ) ;
   if ( query )
   {
      delete query ;
   }
}

/***************  domain class  ****************/

PHP_METHOD ( SequoiaDomain, alterDomain )
{
   INT32 rc = SDB_OK ;
   sdbDomain *domain = NULL ;
   CHAR *error       = NULL ;
   zval *pOptions    = NULL ;
   CHAR *options     = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "z",
                                &pOptions ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_domain", sdbDomain, domain ) ;
   if ( !domain )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   if ( !php_toJson ( &options, pOptions TSRMLS_CC ) )
   {
      SETERROR2 ( getThis(), SDB_INVALIDARG ) ;
      PRINTFERROR ( SDB_INVALIDARG, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = alterDomain ( domain, options ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
}

PHP_METHOD ( SequoiaDomain, listCSInDomain )
{
   INT32 rc = SDB_OK ;
   sdbDomain *domain = NULL ;
   sdbCursor *query = NULL ;
   zval* cursor_obj = NULL ;

   GETCLASSFROMZVAL ( getThis(), "_domain", sdbDomain, domain ) ;
   if ( !domain )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = listCollectionSpacesInDomain ( domain, &query ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( cursor_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

PHP_METHOD ( SequoiaDomain, listCLInDomain )
{
   INT32 rc = SDB_OK ;
   sdbDomain *domain = NULL ;
   sdbCursor *query = NULL ;
   zval* cursor_obj = NULL ;

   GETCLASSFROMZVAL ( getThis(), "_domain", sdbDomain, domain ) ;
   if ( !domain )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   MAKE_STD_ZVAL ( cursor_obj ) ;
   object_init_ex ( cursor_obj, pSequoiadbCursor ) ;
   CREATECLASS ( cursor_obj, "_cursor", sdbCursor, query ) ;
   if ( !query )
   {
      SETERROR2 ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = listCollectionsInDomain ( domain, &query ) ;
   SETERROR2 ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( cursor_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( cursor_obj, 1, 0 ) ;
}

/* **************  Sequoiadb_id class  ****************/
PHP_METHOD ( SequoiaID, __construct )
{
   INT32 rc = SDB_OK ;
   zval *objectId     = NULL ;

   if ( !(zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                  "z",
                                  &objectId ) == FAILURE) )
   {
      if ( objectId )
      {
         if ( IS_STRING == Z_TYPE_P ( objectId ) )
         {
            zend_update_property( Z_OBJCE_P( getThis() ),
                                  getThis(),
                                  ZEND_STRL("$oid"),
                                  objectId TSRMLS_CC ) ;
         }
      }
   }
}

PHP_METHOD ( SequoiaID, __toString )
{
   CHAR *temp = NULL ;
   zval *val = NULL ;
   val = zend_read_property ( Z_OBJCE_P( getThis() ),
                              getThis(), ZEND_STRL("$oid"), 0 TSRMLS_CC ) ;
   if ( val )
   {
      temp = Z_STRVAL_P ( val ) ;
      if ( temp )
      {
         RETURN_STRING ( temp, 1 ) ;
      }
   }
   RETURN_EMPTY_STRING ( ) ;
}
/*
PHP_METHOD ( SequoiaID, __set_state)
{
	zval temp, *state, **id;

	if ( zend_parse_parameters ( ZEND_NUM_ARGS() TSRMLS_CC, "a", &state ) == FAILURE )
	{
		return ;
	}

	if ( zend_hash_find ( HASH_P(state), "$oid", ossStrlen("$oid")+1, (void**) &id ) == FAILURE )
	{
		return ;
	}
	object_init_ex ( return_value, pSequoiadbId ) ;
}
*/
PHP_METHOD ( SequoiaDate, __construct )
{
   INT32 rc = SDB_OK ;
   zval *date     = NULL ;

   if ( !(zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                  "z",
                                  &date ) == FAILURE) )
   {
      if ( date )
      {
         if ( IS_STRING == Z_TYPE_P ( date ) )
         {
            zend_update_property( Z_OBJCE_P( getThis() ),
                                  getThis(),
                                  ZEND_STRL("$date"),
                                  date TSRMLS_CC ) ;
         }
      }
   }
}

PHP_METHOD ( SequoiaDate, __toString )
{
   CHAR *temp = NULL ;
   zval *val = NULL ;
   val = zend_read_property ( Z_OBJCE_P( getThis() ),
                              getThis(), ZEND_STRL("$date"), 0 TSRMLS_CC ) ;
   if ( val )
   {
      temp = Z_STRVAL_P ( val ) ;
      if ( temp )
      {
         RETURN_STRING ( temp, 1 ) ;
      }
   }
   RETURN_EMPTY_STRING () ;
}

PHP_METHOD ( SequoiaTimestamp, __construct )
{
   INT32 rc = SDB_OK ;
   zval *timestamp = NULL ;

   if ( !(zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                  "z",
                                  &timestamp ) == FAILURE) )
   {
      if ( timestamp )
      {
         if ( IS_STRING == Z_TYPE_P ( timestamp ) )
         {
            zend_update_property( Z_OBJCE_P( getThis() ),
                                  getThis(),
                                  ZEND_STRL("$timestamp"),
                                  timestamp TSRMLS_CC ) ;
         }
      }
   }
}

PHP_METHOD ( SequoiaTimestamp, __toString )
{
   CHAR *temp = NULL ;
   zval *val = NULL ;
   val = zend_read_property ( Z_OBJCE_P( getThis() ),
                              getThis(), ZEND_STRL("$timestamp"), 0 TSRMLS_CC ) ;
   if ( val )
   {
      temp = Z_STRVAL_P ( val ) ;
      if ( temp )
      {
         RETURN_STRING ( temp, 1 ) ;
      }
   }
   RETURN_EMPTY_STRING () ;
}

PHP_METHOD ( SequoiaRegex, __construct )
{
   INT32 rc = SDB_OK ;
   zval *timestamp = NULL ;
   CHAR *regex = NULL ;
   CHAR *options = NULL ;

   if ( !(zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                  "z",
                                  &timestamp ) == FAILURE) )
   {
      if ( timestamp )
      {
         if ( IS_STRING == Z_TYPE_P ( timestamp ) )
         {
            regex = Z_STRVAL_P ( timestamp ) ;
            INT32 len = ossStrlen ( regex ) - 1 ;
            options = regex + len ;
            regex[len-1] = 0 ;
            ++regex ;
            zend_update_property_string( Z_OBJCE_P( getThis() ),
                                         getThis(),
                                         ZEND_STRL("$regex"),
                                         regex TSRMLS_CC ) ;
            zend_update_property_string( Z_OBJCE_P( getThis() ),
                                         getThis(),
                                         ZEND_STRL("$options"),
                                         options TSRMLS_CC ) ;
         }
      }
   }
}

PHP_METHOD ( SequoiaRegex, __toString )
{
   CHAR *regex = NULL ;
   CHAR *options = NULL ;
   zval *val1 = NULL ;
   zval *val2 = NULL ;
   val1 = zend_read_property ( Z_OBJCE_P( getThis() ),
                              getThis(), ZEND_STRL("$regex"), 0 TSRMLS_CC ) ;
   val2 = zend_read_property ( Z_OBJCE_P( getThis() ),
                              getThis(), ZEND_STRL("$options"), 0 TSRMLS_CC ) ;
   if ( val1 && val2 )
   {
      regex = Z_STRVAL_P ( val1 ) ;
      options = Z_STRVAL_P ( val2 ) ;
      if ( regex && options )
      {
         INT32 len = ossStrlen ( regex ) + ossStrlen ( options ) + 3 ;
         CHAR *newRegex = (CHAR *)emalloc ( len ) ;
         ossSnprintf ( newRegex, len, "/%s/%s", regex,options ) ;
         RETURN_STRING ( newRegex, 0 ) ;
      }
   }
   RETURN_EMPTY_STRING () ;
}

PHP_METHOD ( SequoiaINT64, __construct )
{
   INT32 rc = SDB_OK ;
   zval *timestamp = NULL ;
   CHAR *numInt64 = NULL ;

   if ( !(zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                  "z",
                                  &timestamp ) == FAILURE) )
   {
      if ( timestamp )
      {
         if ( IS_STRING == Z_TYPE_P ( timestamp ) )
         {
            numInt64 = Z_STRVAL_P ( timestamp ) ;
            zend_update_property_string( Z_OBJCE_P( getThis() ),
                                         getThis(),
                                         ZEND_STRL("INT64"),
                                         numInt64 TSRMLS_CC ) ;
         }
      }
   }
}

PHP_METHOD ( SequoiaINT64, __toString )
{
   CHAR *numInt64 = NULL ;
   zval *val = NULL ;
   val = zend_read_property ( Z_OBJCE_P( getThis() ),
                              getThis(), ZEND_STRL("INT64"), 0 TSRMLS_CC ) ;
   if ( val )
   {
      numInt64 = Z_STRVAL_P ( val ) ;
      if ( numInt64 )
      {
         RETURN_STRING ( numInt64, 1 ) ;
      }
   }
   RETURN_EMPTY_STRING () ;
}

PHP_METHOD ( sequoiaGroup, getNodeNum )
{
   INT32 rc = SDB_OK ;
   CHAR *error = NULL ;
   sdbReplicaGroup *gr = NULL ;
   INT32 nodeNum = 0 ;
   zval *pNodeType = NULL ;
   INT32 nodeType = 0 ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS() TSRMLS_CC,
                                "|z",
                                &pNodeType ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_LONG ( -1 ) ;
   }
   if ( pNodeType )
   {
      if ( IS_LONG == Z_TYPE_P ( pNodeType ) )
      {
         nodeType = Z_LVAL_P ( pNodeType ) ;
      }
      else if ( IS_STRING == Z_TYPE_P ( pNodeType ) )
      {
         CHAR *num = Z_STRVAL_P ( pNodeType ) ;
         nodeType = ossAtoi ( num ) ;
      }
      else
      {
         SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
         RETURN_LONG ( -1 ) ;
      }
   }
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_LONG ( -1 ) ;
   }
   rc = getNodeNum ( gr, nodeType, &nodeNum ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   if ( rc )
   {
      RETURN_LONG ( -1 ) ;
   }
   else
   {
      RETURN_LONG ( nodeNum ) ;
   }
}

PHP_METHOD ( sequoiaGroup, getDetail )
{
   INT32 rc = SDB_OK ;
   CHAR *out = NULL ;
   INT32 outSize = 0 ;
   sdbReplicaGroup *gr = NULL ;
   CHAR *pBuf = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_ARRAY_STRING ( getThis() ) ;
   }

   rc = getDetail ( gr, &out, &outSize ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      if ( out )
      {
         free ( out ) ;
      }
      RETURN_EMPTY_ARRAY_STRING ( getThis() ) ;
   }
   pBuf = (CHAR *)emalloc ( outSize ) ;
   memcpy ( pBuf, out, outSize ) ;
   free ( out ) ;
   RETURN_ARRAY_STRING ( getThis(), pBuf, 0 ) ;
}

PHP_METHOD ( sequoiaGroup, getMaster )
{
   INT32 rc = SDB_OK ;
   sdbReplicaGroup *gr   = NULL ;
   sdbNode  *node = NULL ;
   zval* node_obj        = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   
   MAKE_STD_ZVAL ( node_obj ) ;
   object_init_ex ( node_obj, pSequoiadbReplicaNode ) ;
   CREATECLASS ( node_obj, "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = getMaster ( gr, &node ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( node_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( node_obj, 1, 0 ) ;
}

PHP_METHOD ( sequoiaGroup, getSlave )
{
   INT32 rc = SDB_OK ;
   sdbReplicaGroup *gr   = NULL ;
   sdbNode  *node = NULL ;
   zval* node_obj        = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   
   MAKE_STD_ZVAL ( node_obj ) ;
   object_init_ex ( node_obj, pSequoiadbReplicaNode ) ;
   CREATECLASS ( node_obj, "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = getSlave ( gr, &node ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( node_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( node_obj, 1, 0 ) ;
}

PHP_METHOD ( sequoiaGroup, getNode )
{
   INT32 rc = SDB_OK ;
   CHAR *nodeName        = NULL ;
   INT32 nodeName_len    = 0    ;
   sdbReplicaGroup *gr   = NULL ;
   sdbNode  *node = NULL ;
   zval* node_obj        = NULL ;

   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "s",
                                &nodeName,
                                &nodeName_len ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }

   MAKE_STD_ZVAL ( node_obj ) ;
   object_init_ex ( node_obj, pSequoiadbReplicaNode ) ;
   CREATECLASS ( node_obj, "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = getNode ( gr, &node, nodeName ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   zval *sdbobj = NULL ;
   MAKE_STD_ZVAL ( sdbobj ) ;
   GETZVAL ( getThis(), "_connection", sdbobj ) ;
   SETZVAL ( node_obj, "_connection", sdbobj ) ;
   RETURN_ZVAL( node_obj, 1, 0 ) ;
}

PHP_METHOD ( sequoiaGroup, createNode )
{
   INT32 rc = SDB_OK ;
   CHAR *hostName           = NULL ;
   INT32 hostName_len       = 0    ;
   CHAR *serviceName        = NULL ;
   INT32 serviceName_len    = 0    ;
   CHAR *databasePath       = NULL ;
   INT32 databasePath_len   = 0    ;
   zval *pConfig            = NULL ;
   sdbReplicaGroup *gr             = NULL ;
   CHAR *error              = NULL ;
   std::map<std::string,std::string> config ;


   if ( zend_parse_parameters ( ZEND_NUM_ARGS () TSRMLS_CC,
                                "sss|z",
                                &hostName,
                                &hostName_len,
                                &serviceName,
                                &serviceName_len,
                                &databasePath,
                                &databasePath_len,
                                &pConfig ) == FAILURE )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   if ( !php_to_map ( pConfig, &config TSRMLS_CC ) )
   {
      SETERROR ( getThis(), SDB_INVALIDARG ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = createNode ( gr,
                     hostName,
                     serviceName,
                     databasePath,
                     config ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;                
}
/*
PHP_METHOD ( sequoiaGroup, activate )
{
   INT32 rc = SDB_OK ;
   CHAR *error           = NULL ;
   sdbReplicaGroup *gr   = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = activate ( gr ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ; 
}
*/
PHP_METHOD ( sequoiaGroup, start )
{
   INT32 rc = SDB_OK ;
   CHAR *error  = NULL ;
   sdbReplicaGroup *gr = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = groupStart ( gr ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ; 
}

PHP_METHOD ( sequoiaGroup, stop )
{
   INT32 rc = SDB_OK ;
   CHAR *error           = NULL ;
   sdbReplicaGroup *gr   = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = groupStop ( gr ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ; 
}

PHP_METHOD ( sequoiaGroup, isCatalog )
{
   CHAR *error    = NULL ;
   sdbReplicaGroup *gr   = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbReplicaGroup", sdbReplicaGroup, gr ) ;
   if ( !gr )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   RETURN_BOOL ( isCatalog ( gr ) ) ;
}



PHP_METHOD ( sequoiaNode, connect )
{
   INT32 rc = SDB_OK ;
   CHAR *error          = NULL ;
   sdb *connection      = NULL ;
   sdbNode *node = NULL ;
   zval* sdb_obj        = NULL ;

   GETCLASSFROMZVAL ( getThis(), "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   MAKE_STD_ZVAL ( sdb_obj ) ;
   object_init_ex ( sdb_obj, pSequoiadbSdb ) ;
   CREATECLASS ( sdb_obj, "_connection", sdb, connection ) ;
   if ( !connection )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_NULL() ;
   }
   rc = nodeConnect ( node, &connection ) ;
   SETERROR ( getThis(), rc ) ;
   if ( rc )
   {
      RETURN_NULL() ;
   }
   RETURN_ZVAL( sdb_obj, 1, 0 ) ;
}

PHP_METHOD ( sequoiaNode, getStatus )
{
   INT32 num = 0 ;
   CHAR *error          = NULL ;
   sdbNode *node = NULL ;

   GETCLASSFROMZVAL ( getThis(), "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      RETURN_LONG ( -1 ) ;
   }
   num = getStatus ( node ) ;
   RETURN_LONG ( num ) ;
}

PHP_METHOD ( sequoiaNode, getHostName )
{
   const CHAR *name = NULL ;
   sdbNode *node = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_STRING ( ) ;
   }
   name = getHostName ( node ) ;
   RETURN_STRING ( name, 1 ) ;
}

PHP_METHOD ( sequoiaNode, getServiceName )
{
   const CHAR *name = NULL ;
   sdbNode *node = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_STRING ( ) ;
   }
   name = getServiceName ( node ) ;
   RETURN_STRING ( name, 1 ) ;
}

PHP_METHOD ( sequoiaNode, getNodeName )
{
   const CHAR *name = NULL ;
   sdbNode *node = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      RETURN_EMPTY_STRING ( ) ;
   }
   name = getNodeName ( node ) ;
   RETURN_STRING ( name, 1 ) ;
}

PHP_METHOD ( sequoiaNode, start )
{
   INT32 rc = SDB_OK ;
   CHAR *error           = NULL ;
   sdbNode *node = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = nodeStart ( node ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ; 
}

PHP_METHOD ( sequoiaNode, stop )
{
   INT32 rc = SDB_OK ;
   CHAR *error           = NULL ;
   sdbNode *node = NULL ;
   GETCLASSFROMZVAL ( getThis(), "_sdbNode", sdbNode, node ) ;
   if ( !node )
   {
      SETERROR ( getThis(), SDB_PHP_DRIVER_INTERNAL_ERROR ) ;
      PRINTFERROR ( SDB_PHP_DRIVER_INTERNAL_ERROR, error ) ;
      RETURN_ARRAY_STRING ( getThis(), error, 0 ) ;
   }
   rc = nodeStop ( node ) ;
   SETERROR ( getThis(), rc ) ;
   PRINTFERROR ( rc, error ) ;
   RETURN_ARRAY_STRING ( getThis(), error, 0 ) ; 
}

PHP_SEQUOIADB_API INT32 key_get_value ( zval *val, const CHAR *key TSRMLS_DC )
{
   CHAR *buf = NULL ;
   INT32 len = ossStrlen ( key ) ;
   php_toJson ( &buf, val TSRMLS_CC ) ;
   if ( buf )
   {
      cJSON *c = cJSON_Parse( buf ) ;
      c = c->child ;
      if ( c )
      {
         cJSON *next;
         while (c)
         {
            next=c->next;
            if ( !strncmp ( c->string, key, len ) )
            {
               switch ( c->type )
               {
               case cJSON_String :
                  break ;
               case cJSON_True :
                  return 1 ;
               case cJSON_False :
                  return 0 ;
               case cJSON_NULL :
               case cJSON_Number :
               case cJSON_Array :
               case cJSON_Object :
               case cJSON_Timestamp :
               case cJSON_Date :
               case cJSON_Regex :
               case cJSON_Oid :
               case cJSON_Binary :
               default:
                  break ;
               }
            }
            c=next;
         }
         return PHP_GET_VALUE_NOTFIND ;
      }
   }
   return PHP_GET_VALUE_ERROR ;
}

BOOLEAN append ( CHAR **buf, INT32 &bufSize, INT32 &leftLen,
                 const CHAR *temp, INT32 size TSRMLS_DC )
{
   if ( (bufSize - leftLen) < ( size + 1 ) )
   {
      bufSize = leftLen + size + 1 ;
      (*buf) = (CHAR *)erealloc ( (*buf), bufSize ) ;
      if ( !(*buf) )
      {
         return FALSE ;
      }
      (*buf)[bufSize-1] = 0 ;
   }
   memcpy ( (*buf) + leftLen, temp, size ) ;
   leftLen += size ;
   (*buf)[leftLen] = 0 ;
   return TRUE ;

}

BOOLEAN appendl ( CHAR **buf, INT32 &bufSize, INT32 &leftLen,
                  INT32 num TSRMLS_DC )
{
   INT32 len = 0 ;
   CHAR number[64] = {0} ;
   len = ossSnprintf ( number, 64, "%d", num ) ;
   return append ( buf, bufSize, leftLen, number, len TSRMLS_CC ) ;
}

BOOLEAN appendd ( CHAR **buf, INT32 &bufSize, INT32 &leftLen,
                  double num TSRMLS_DC )
{
   INT32 len = 0 ;
   CHAR number[256] = {0} ;
   len = ossSnprintf ( number, 256, "%g", num ) ;
   return append ( buf, bufSize, leftLen, number, len TSRMLS_CC ) ;
}

static void json_encode_serializable_object ( CHAR **buf,
                                              INT32 &bufSize,
                                              INT32 &leftLen, 
                                              zval *val TSRMLS_DC )
{
   zend_class_entry *ce = Z_OBJCE_P(val) ;
   zval *retval = NULL, fname ;
   HashTable* myht ;

   if ( Z_TYPE_P(val) == IS_ARRAY )
   {
      myht = HASH_OF(val) ;
   }
   else
   {
      myht = Z_OBJPROP_P(val) ;
   }

   if ( myht && myht->nApplyCount > 1 )
   {
      append ( buf, bufSize, leftLen, "null", 4 TSRMLS_CC ) ;
      return ;
   }

   ZVAL_STRING ( &fname, "jsonSerialize", 0 ) ;

   if ( FAILURE == call_user_function_ex( EG(function_table),
                                          &val,
                                          &fname,
                                          &retval,
                                          0,
                                          NULL,
                                          1,
                                          NULL TSRMLS_CC) ||
        !retval )
   {
      append ( buf, bufSize, leftLen, "null", 4 TSRMLS_CC ) ;
      return ;
   }

   if ( EG(exception) )
   {
      zval_ptr_dtor ( &retval ) ;
      append ( buf, bufSize, leftLen, "null", 4 TSRMLS_CC ) ;
      return ;
   }

   if ( ( Z_TYPE_P(retval) == IS_OBJECT ) &&
        ( Z_OBJ_HANDLE_P(retval) == Z_OBJ_HANDLE_P(val) ) )
   {
      json_encode_array ( buf, bufSize, leftLen, &retval TSRMLS_CC ) ;
   }
   else
   {
      php_json_encode ( buf, bufSize, leftLen, retval TSRMLS_CC ) ;
   }

   zval_ptr_dtor ( &retval ) ;
}

PHP_SEQUOIADB_API void php_json_encode ( CHAR **buf,
                                         INT32 &bufSize,
                                         INT32 &leftLen,
                                         zval *val TSRMLS_DC )
{
   switch ( Z_TYPE_P(val) )
   {
      case IS_NULL:
         append ( buf, bufSize, leftLen, "null", 4 TSRMLS_CC ) ;
         break ;
      case IS_BOOL:
         if ( Z_BVAL_P(val) )
         {
            append ( buf, bufSize, leftLen, "true", 4 TSRMLS_CC ) ;
         }
         else
         {
            append ( buf, bufSize, leftLen, "false", 5 TSRMLS_CC ) ;
         }
         break ;
      case IS_LONG:
         appendl ( buf, bufSize, leftLen, Z_LVAL_P(val) TSRMLS_CC ) ;
         break ;
      case IS_DOUBLE:
         appendd ( buf, bufSize, leftLen, Z_DVAL_P(val) TSRMLS_CC ) ;
         break ;
      case IS_STRING:
      {
         append ( buf, bufSize, leftLen, "\"", 1 TSRMLS_CC ) ;
         INT32 valLen = Z_STRLEN_P(val) ;
         INT32 len = valLen * 2 ;
         if ( len > 0 )
         {
            CHAR *data = Z_STRVAL_P(val) ;
            /*CHAR *temp = (CHAR *)emalloc ( len ) ;
            ossMemset ( temp, 0, len ) ;
            for ( INT32 i = 0, k = 0; i < len && k < valLen; ++i, ++k )
            {
               switch ( *data )
               {
               case '\'':
               {
                 temp[i] = '\\' ;
                 ++i ;
                 temp[i] = '\'' ;
                 break ;
               }
               case '\"':
               {
                 temp[i] = '\\' ;
                 ++i ;
                 temp[i] = '\"' ;
                 break ;
               }
               case '\\':
               {
                 temp[i] = '\\' ;
                 ++i ;
                 temp[i] = '\\' ;
                 break ;
               }
               default :
               {
                  temp[i] = *data ;
                  break ;
               }
               }
               ++data ;
            }*/
            append ( buf, bufSize, leftLen,
                     data, valLen  TSRMLS_CC ) ;
         }
         append ( buf, bufSize, leftLen, "\"", 1 TSRMLS_CC ) ;
         break ;
      }
      case IS_OBJECT:
         if ( instanceof_function( Z_OBJCE_P(val), pSequoiadbSdb TSRMLS_CC ) )
         {
            append ( buf, bufSize, leftLen, "{", 1 TSRMLS_CC ) ;
            json_encode_serializable_object ( buf, bufSize,
                                              leftLen, val TSRMLS_CC ) ;
            append ( buf, bufSize, leftLen, "}", 1 TSRMLS_CC ) ;
            break ;
         }
         /* fallthrough -- Non-serializable object */
      case IS_ARRAY:
         json_encode_array ( buf, bufSize, leftLen, &val TSRMLS_CC ) ;
         break ;
      default:
         append ( buf, bufSize, leftLen, "null", 4 TSRMLS_CC ) ;
         break ;
   }
   return ;
}

PHP_SEQUOIADB_API BOOLEAN php_toJson ( CHAR **buf, zval *val TSRMLS_DC )
{
   if ( val )
   {
      if ( IS_ARRAY == Z_TYPE_P ( val ) )
      {
         INT32 leftLen = 0 ;
         INT32 bufSize = 0 ;
         php_json_encode ( buf, bufSize, leftLen, val TSRMLS_CC ) ;
      }
      else if ( IS_STRING == Z_TYPE_P ( val ) )
      {
         *buf = Z_STRVAL_P ( val ) ;
      }
      else if ( IS_NULL == Z_TYPE_P ( val ) )
      {
         return TRUE ;
      }
      else
      {
         return FALSE ;
      }
   }
   return TRUE ;
}

static INT32 json_determine_array_type(zval **val TSRMLS_DC)
{
   INT32 i ;
   HashTable *myht = HASH_OF ( *val ) ;

   i = myht ? zend_hash_num_elements(myht) : 0 ;
   if ( i > 0 )
   {
      CHAR *key = NULL ;
      ulong index, idx ;
      uint key_len ;
      HashPosition pos ;

      zend_hash_internal_pointer_reset_ex ( myht, &pos ) ;
      idx = 0 ;
      for ( ; ; zend_hash_move_forward_ex ( myht, &pos ) )
      {
         i = zend_hash_get_current_key_ex ( myht,
                                            &key,
                                            &key_len,
                                            &index,
                                            0,
                                            &pos ) ;
         if ( i == HASH_KEY_NON_EXISTANT )
         {
            break ;
         }

         if ( i == HASH_KEY_IS_STRING )
         {
            return PHP_JSON_OUTPUT_OBJECT ;
         }
         else
         {
            if ( index != idx )
            {
               return PHP_JSON_OUTPUT_OBJECT ;
            }
         }
         ++idx ;
      }
   }

   return PHP_JSON_OUTPUT_ARRAY ;
}

PHP_SEQUOIADB_API BOOLEAN json_encode_vector ( std::vector<CHAR *> *pVector,
                                               zval **val TSRMLS_DC )
{
   INT32 i = 0, r = 0 ;
   BOOLEAN rc = TRUE ;
   zval **z_item ;
   HashTable *myht;

   if ( Z_TYPE_PP(val) == IS_ARRAY )
   {
      myht = HASH_OF( *val ) ;
      r = json_determine_array_type ( val TSRMLS_CC ) ;
   }
   else
   {
      myht = Z_OBJPROP_PP( val ) ;
      r = PHP_JSON_OUTPUT_OBJECT ;
   }

   if ( myht && myht->nApplyCount > 1 )
   {
      return rc ;
   }

   i = myht ? zend_hash_num_elements ( myht ) : 0;

   if ( i > 0 )
   {
      CHAR* key = NULL ;
      ulong idx = 0 ;

      zend_hash_internal_pointer_reset( myht ) ;
      for ( ; ; zend_hash_move_forward ( myht ) )
      {
         CHAR *buffer = NULL ;
         i = zend_hash_get_current_key_ex( myht, &key, NULL, &idx, 0,NULL ) ;
         if ( HASH_KEY_NON_EXISTANT == i )
         {
            break ;
         }

         if ( SUCCESS == zend_hash_get_current_data ( myht, (void**) &z_item ) )
         {
            if ( i == HASH_KEY_IS_STRING && key && *key )
            {
               CHAR *buffer = NULL ;
               rc = php_toJson ( &buffer, *val TSRMLS_CC ) ;
               pVector->push_back ( buffer ) ;
               return rc ;
            }
            else
            {
               rc = php_toJson ( &buffer, *z_item TSRMLS_CC ) ;
               if ( !rc )
               {
                  return rc ;
               }
               pVector->push_back ( buffer ) ;
            }
         }
      }
   }
   return TRUE ;
}

PHP_SEQUOIADB_API BOOLEAN php_jsonArr2Vector ( std::vector<CHAR *> *pVector, zval *val TSRMLS_DC )
{
   if ( val )
   {
      if ( IS_ARRAY == Z_TYPE_P ( val ) )
      {
         return json_encode_vector ( pVector, &val TSRMLS_CC ) ;
      }
      else if ( IS_STRING == Z_TYPE_P ( val ) )
      {
         pVector->push_back ( Z_STRVAL_P ( val ) ) ;
         return TRUE ;
      }
      else if ( IS_NULL == Z_TYPE_P ( val ) )
      {
         return FALSE ;
      }
      else
      {
         return FALSE ;
      }
   }
   return FALSE ;
}

PHP_SEQUOIADB_API void php_map_encode(std::map<std::string,std::string> *pMap,
                                      zval *val TSRMLS_DC )
{
   switch ( Z_TYPE_P(val) )
   {
      case IS_NULL:
      case IS_BOOL:
      case IS_LONG:
      case IS_DOUBLE:
      case IS_STRING:
      case IS_OBJECT:
         break ;
      case IS_ARRAY:
         map_encode_array ( pMap, &val TSRMLS_CC ) ;
         break ;
      default:
         break ;
   }
   return ;
}

PHP_SEQUOIADB_API BOOLEAN php_to_map ( zval *val,
                                       std::map<std::string,std::string> *pMap
                                       TSRMLS_DC )
{
   if ( val )
   {
      if ( IS_ARRAY == Z_TYPE_P ( val ) )
      {
         php_map_encode ( pMap, val TSRMLS_CC ) ;
      }
      else if ( IS_STRING == Z_TYPE_P ( val ) )
      {
         const CHAR *buf = Z_STRVAL_P ( val ) ;
         zval *temp_val = NULL ;
         MAKE_STD_ZVAL ( temp_val ) ;
         php_json_decode ( buf, &temp_val TSRMLS_CC ) ;
         php_map_encode ( pMap, temp_val TSRMLS_CC ) ;
      }
   }
   return TRUE ;
}

PHP_SEQUOIADB_API BOOLEAN php_toNum64 ( INT64 &num64, zval *val TSRMLS_DC )
{
   if ( val )
   {
      if ( IS_LONG == Z_TYPE_P ( val ) )
      {
         num64 = Z_LVAL_P ( val ) ;
      }
      else if ( IS_OBJECT == Z_TYPE_P ( val ) )
      {
         zval **z_item ;
         HashTable *myht = HASH_OF( val ) ;
         if ( SUCCESS == zend_hash_get_current_data ( myht, (void**) &z_item ) )
         {
            CHAR *buf = Z_STRVAL_P ( *z_item ) ;
            num64 = ossAtoll ( buf ) ;
         }
      }
      else if ( IS_STRING == Z_TYPE_P ( val ) )
      {
         CHAR *temp = Z_STRVAL_P ( val ) ;
         num64 = ossAtoi ( temp ) ;
      }
      else if ( IS_NULL == Z_TYPE_P ( val ) )
      {
         return TRUE ;
      }
      else
      {
         return FALSE ;
      }
   }
   return TRUE ;
}

PHP_SEQUOIADB_API void map_encode_array(std::map<std::string,std::string> *pMap,
                                        zval **val TSRMLS_DC )
{
   INT32 i = 0, r = 0 ;
   zval **z_item ;
   HashTable *myht;

   if ( Z_TYPE_PP(val) == IS_ARRAY )
   {
      myht = HASH_OF( *val ) ;
      r = json_determine_array_type ( val TSRMLS_CC ) ;
   }
   else
   {
      myht = Z_OBJPROP_PP( val ) ;
      r = PHP_JSON_OUTPUT_OBJECT ;
   }

   if ( myht && myht->nApplyCount > 1 )
   {
      return ;
   }

   i = myht ? zend_hash_num_elements ( myht ) : 0;

   if ( i > 0 )
   {
      CHAR* key = NULL ;
      ulong idx = 0 ;
      INT32 needComma = 0 ;

      zend_hash_internal_pointer_reset( myht ) ;
      for ( ; ; zend_hash_move_forward ( myht ) )
      {
         i = zend_hash_get_current_key_ex( myht, &key, NULL, &idx, 0,NULL ) ;
         if ( HASH_KEY_NON_EXISTANT == i )
         {
            break ;
         }

         if ( SUCCESS == zend_hash_get_current_data ( myht, (void**) &z_item ) )
         {
            if ( i == HASH_KEY_IS_STRING && key && *key )
            {
               if ( IS_STRING != Z_TYPE_PP( z_item ) )
               {
                  return ;
               }
               CHAR *temp = NULL ;
               INT32 valLen = Z_STRLEN_PP(z_item) ;
               INT32 len = valLen * 2 ;
               if ( len > 0 )
               {
                  CHAR *data = Z_STRVAL_PP(z_item) ;
                  temp = (CHAR *)emalloc ( len ) ;
                  ossMemset ( temp, 0, len ) ;
                  for ( INT32 i = 0, k = 0; i < len && k < valLen; ++i, ++k )
                  {
                     switch ( *data )
                     {
                     case '\'':
                     {
                       temp[i] = '\\' ;
                       ++i ;
                       temp[i] = '\'' ;
                       break ;
                     }
                     case '\"':
                     {
                       temp[i] = '\\' ;
                       ++i ;
                       temp[i] = '\"' ;
                       break ;
                     }
                     case '\\':
                     {
                       temp[i] = '\\' ;
                       ++i ;
                       temp[i] = '\\' ;
                       break ;
                     }
                     default :
                     {
                        temp[i] = *data ;
                        break ;
                     }
                     }
                     ++data ;
                  }
               }
               std::string strTemp1( key ) ;
               std::string strTemp2( temp ) ;
               pMap->insert (std::make_pair(strTemp1,strTemp2) ) ;
            }
         }
      }
   }
}

PHP_SEQUOIADB_API void json_encode_array ( CHAR **buf,
                                           INT32 &bufSize,
                                           INT32 &leftLen,
                                           zval **val TSRMLS_DC )
{
   INT32 i = 0, r = 0 ;
   zval **z_item ;
   HashTable *myht;

   if ( Z_TYPE_PP(val) == IS_ARRAY )
   {
      myht = HASH_OF( *val ) ;
      r = json_determine_array_type ( val TSRMLS_CC ) ;
   }
   else
   {
      myht = Z_OBJPROP_PP( val ) ;
      r = PHP_JSON_OUTPUT_OBJECT ;
   }

   if ( myht && myht->nApplyCount > 1 )
   {
      append ( buf, bufSize, leftLen, "null", 4 TSRMLS_CC ) ;
      return ;
   }

   if ( PHP_JSON_OUTPUT_OBJECT == r )
      append ( buf, bufSize, leftLen, "{", 1 TSRMLS_CC ) ;
   else
      append ( buf, bufSize, leftLen, "[", 1 TSRMLS_CC ) ;

   i = myht ? zend_hash_num_elements ( myht ) : 0;

   if ( i > 0 )
   {
      CHAR* key = NULL ;
      ulong idx = 0 ;
      INT32 needComma = 0 ;

      zend_hash_internal_pointer_reset( myht ) ;
      for ( ; ; zend_hash_move_forward ( myht ) )
      {
         i = zend_hash_get_current_key_ex( myht, &key, NULL, &idx, 0,NULL ) ;
         if ( HASH_KEY_NON_EXISTANT == i )
         {
            break ;
         }

         if ( SUCCESS == zend_hash_get_current_data ( myht, (void**) &z_item ) )
         {
            if ( needComma )
            {
               append ( buf, bufSize, leftLen, ",", 1 TSRMLS_CC ) ;
            }
            else
            {
               needComma = 1;
            }
            if ( i == HASH_KEY_IS_STRING && key && *key )
            {
               append ( buf, bufSize, leftLen, "\"", 1 TSRMLS_CC ) ;
               append ( buf, bufSize, leftLen, key, ossStrlen(key) TSRMLS_CC ) ;
               append ( buf, bufSize, leftLen, "\"", 1 TSRMLS_CC ) ;
               append ( buf, bufSize, leftLen, ":", 1 TSRMLS_CC ) ;
               php_json_encode ( buf, bufSize, leftLen, *z_item TSRMLS_CC ) ;
            }
            else
            {
               php_json_encode ( buf, bufSize, leftLen, *z_item TSRMLS_CC ) ;
            }
         }
      }
   }
   if ( PHP_JSON_OUTPUT_OBJECT == r )
      append ( buf, bufSize, leftLen, "}", 1 TSRMLS_CC ) ;
   else
      append ( buf, bufSize, leftLen, "]", 1 TSRMLS_CC ) ;
}

static BOOLEAN jsonConvertArray ( cJSON *cj,
                                  zval **ppVal,
                                  BOOLEAN isObj TSRMLS_DC )
{
   zval *val = *ppVal ;
   INT32 i = 0 ;
   while ( cj )
   {
      switch ( cj->type )
      {
      case cJSON_Number:
      {
         if ( cJSON_DOUBLE == cj->numType )
         {
            /* for 64 bit float */
            if ( isObj && cj->string )
            {
               add_assoc_double ( val, cj->string, cj->valuedouble ) ;
            }
            else
            {
               add_next_index_double ( val, cj->valuedouble ) ;
            }
         }
         else if ( cJSON_INT32 == cj->numType )
         {
            /* for 32 bit int */
            if ( isObj && cj->string )
            {
               add_assoc_long ( val, cj->string, cj->valueint ) ;
            }
            else
            {
               add_next_index_long ( val, cj->valueint ) ;
            }
         }
         else if ( cJSON_INT64 == cj->numType )
         {
            /* for 64 bit int */
            zval *Int64 = NULL ;
            MAKE_STD_ZVAL ( Int64 ) ;
            
            if ( object_init_ex ( Int64, pSequoiadbInt64 ) == SUCCESS )
            {
               CHAR tempBuf[512] = {0} ;
               ossSnprintf ( tempBuf, 512, "%lld", cj->valuelongint ) ;
               SETSTRING ( Int64, "INT64", tempBuf ) ;
            }
            if ( isObj && cj->string )
               add_assoc_zval ( val, cj->string, Int64 ) ;
            else
               add_next_index_zval ( val, Int64 ) ;
            break ;
         }
         break ;
      }
      case cJSON_NULL:
      {
         /* for null type */
         if ( isObj && cj->string )
            add_assoc_null ( val, cj->string ) ;
         else
            add_next_index_null ( val ) ;
         break ;
      }
      case cJSON_True:
      case cJSON_False:
      {
         /* for boolean */
         if ( isObj && cj->string )
            add_assoc_bool ( val, cj->string, cj->type ) ;
         else
         {
            add_next_index_bool ( val, cj->type ) ;
         }
         break ;
      }
      case cJSON_MinKey:
      {
         if ( isObj && cj->string )
            add_assoc_string ( val, cj->string, "minKey", 1 ) ;
         else
         {
            add_next_index_string ( val, "minKey", 1 ) ;
         }
         break ;
      }
      case cJSON_MaxKey:
      {
         if ( isObj && cj->string )
            add_assoc_string ( val, cj->string, "maxKey", 1 ) ;
         else
         {
            add_next_index_string ( val, "maxKey", 1 ) ;
         }
         break ;
      }
      case cJSON_String:
      {
         /* string type */
         if ( isObj && cj->string )
         {
         
            add_assoc_string ( val, cj->string, cj->valuestring, 1 ) ;
         }
         else
         {
            add_next_index_string ( val, cj->valuestring, 1 ) ;
         }
         break ;
      }
      case cJSON_Timestamp:
      {
         zval *timestamp = NULL ;
         MAKE_STD_ZVAL ( timestamp ) ;
         if ( object_init_ex ( timestamp, pSequoiadbTimeStamp ) == SUCCESS )
         {
            add_property_string ( timestamp, "$timestamp", cj->valuestring, 1 ) ;
         }
         if ( isObj && cj->string )
            add_assoc_zval ( val, cj->string, timestamp ) ;
         else
            add_next_index_zval ( val, timestamp ) ;
         break ;
      }
      case cJSON_Date:
      {
         zval *date = NULL ;
         MAKE_STD_ZVAL ( date ) ;
         if ( object_init_ex ( date, pSequoiadbData ) == SUCCESS )
         {
            add_property_string ( date, "$date", cj->valuestring, 1 ) ;
         }
         if ( isObj && cj->string )
            add_assoc_zval ( val, cj->string, date ) ;
         else
            add_next_index_zval ( val, date ) ;
         break ;
      }
      case cJSON_Regex:
      {
         zval *regex = NULL ;
         MAKE_STD_ZVAL ( regex ) ;
         if ( object_init_ex ( regex, pSequoiadbRegex ) == SUCCESS )
         {
            add_property_string ( regex, "$regex", cj->valuestring, 1 ) ;
            add_property_string ( regex, "$options", cj->valuestring2, 1 ) ;
         }
         if ( isObj && cj->string )
            add_assoc_zval ( val, cj->string, regex ) ;
         else
            add_next_index_zval ( val, regex ) ;
         break ;
      }
      case cJSON_Oid:
      {
         zval *objId = NULL ;
         MAKE_STD_ZVAL ( objId ) ;
         if ( object_init_ex ( objId, pSequoiadbId ) == SUCCESS )
         {
            add_property_string ( objId, "$oid", cj->valuestring, 1 ) ;
         }
         if ( isObj && cj->string )
            add_assoc_zval ( val, cj->string, objId ) ;
         else
            add_next_index_zval ( val, objId ) ;
         break ;
      }
      case cJSON_Binary:
      {
         break ;
      }
      case cJSON_Object:
      {
         zval *childVal = NULL ;
         ALLOC_INIT_ZVAL ( childVal ) ;
         array_init ( childVal ) ;
         jsonConvertArray ( cj->child, &childVal, TRUE TSRMLS_CC ) ;
         if ( isObj && cj->string )
            add_assoc_zval ( val, cj->string, childVal ) ;
         else
            add_next_index_zval ( val, childVal ) ;
         break ;
      }
      case cJSON_Array:
      {
         zval *childVal = NULL ;
         ALLOC_INIT_ZVAL ( childVal ) ;
         array_init ( childVal ) ;
         jsonConvertArray ( cj->child, &childVal, FALSE TSRMLS_CC ) ;
         if ( isObj && cj->string )
            add_assoc_zval ( val, cj->string, childVal ) ;
         else
            add_next_index_zval ( val, childVal ) ;
         break ;
      }
      default :
         break ;
      }
      cj = cj->next ;
      ++i ;
   }
   return TRUE ;
}

PHP_SEQUOIADB_API BOOLEAN php_json_decode ( const CHAR *buf,
                                            zval **val TSRMLS_DC )
{
   cJSON *cj = cJSON_Parse( buf ) ;
   if ( !cj || !(*val) )
   {
      cJSON_Delete ( cj ) ;
      return FALSE ;
   }
   array_init ( *val ) ;
   jsonConvertArray ( cj->child, val, TRUE TSRMLS_CC ) ;
   cJSON_Delete ( cj ) ;
   return TRUE ;
}
/* }}} */
/* The previous line is meant for vim and emacs, so it can correctly fold and 
   unfold functions in source code. See the corresponding marks just before 
   function definition, where the functions purpose is also documented. Please 
   follow this convention for the convenience of others editing your code.
*/

/* __function_stubs_here__ */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
