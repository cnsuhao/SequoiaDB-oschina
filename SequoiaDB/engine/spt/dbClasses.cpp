/*******************************************************************************

   Copyright (C) 2011-2014 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

   Source File Name = dbClasses.cpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          12/1/2014  ly  Initial Draft

   Last Changed =

*******************************************************************************/

#include "core.hpp"
#include "jsapi.h"
#include "../client/jstobs.h"
#include "../client/bson/bson.h"
#include "../mdocml/parseMandocCpp.hpp"
#include "sptParseTroff.hpp"
#include "ossUtil.h"
#include "pd.hpp"
#include "ossErr.h"
#include "msgDef.h"
#include "pdTrace.hpp"
#include "sptTrace.hpp"
#include "sptConvertor.hpp"
#include "fmpDef.hpp"
#include "oss.h"
#include "ossSocket.hpp"
#include "string"
#include "climits"
#include "sptCommon.hpp"
#include "utilPath.hpp"
#include "ossIO.hpp"
#include "sptSPDef.hpp"
#include "sptConvertorHelper.hpp"

#define SAFE_BSON_DISPOSE( p ) \
   do { if ( p ) { bson_dispose( p ) ; ( p ) = NULL ; } } while ( 0 )

#define REPORT(cond, fmt, ...)                           \
   do {                                                  \
      if ( ! ( cond ) ) {                                \
         ret = JS_FALSE ;                                \
         JS_ReportError ( cx , (fmt) , ##__VA_ARGS__ ) ; \
         goto error ;                                    \
      }                                                  \
   } while ( 0 )

#define REPORT_RC(cond, funcName, rc)                       \
   do {                                                     \
      engine::sdbSetErrMsg( NULL ) ;\
      engine::sdbSetErrno( SDB_OK ) ;                           \
      if ( ! (cond) ) {                                     \
         ret = JS_FALSE ;                                   \
         engine::sdbSetErrMsg( rc ? getErrDesp( rc ) : NULL ) ;\
         engine::sdbSetErrno( rc ) ;                         \
         JS_SetPendingException ( cx , INT_TO_JSVAL( rc ) ) ;   \
         goto error ;                                       \
      }                                                     \
   } while ( 0 )

#define REPORT_RC_MSG(cond, funcName, rc, msg )             \
   do {                                                     \
      engine::sdbSetErrMsg( NULL ) ;                         \
      engine::sdbSetErrno( SDB_OK ) ;                           \
      if ( ! (cond) ) {                                     \
         ret = JS_FALSE ;                                   \
         engine::sdbSetErrMsg( rc ? getErrDesp( rc ) : NULL ) ;\
         engine::sdbSetErrno( rc ) ;                         \
         JS_SetPendingException ( cx , INT_TO_JSVAL( rc ) ) ;   \
         goto error ;                                       \
      }                                                     \
   } while ( 0 )

#define TRY_REPORT( cx, msg ) \
   if ( ! JS_IsExceptionPending( cx ) ) JS_ReportError( ( cx ), ( msg ) )

#define VERIFY(cond)                                        \
   do {                                                     \
      if ( !( cond ) ) {                                    \
         ret = JS_FALSE ;                                   \
         goto error ;                                       \
      }                                                     \
   } while ( 0 )

#define SAFE_RELEASE_CONNECTION(x)                    \
   do {                                               \
      if ( (x) && *(x) != SDB_INVALID_HANDLE ) {      \
         sdbReleaseConnection ( *(x) ) ;              \
         *(x) = SDB_INVALID_HANDLE ;                  \
      }                                               \
   } while (0)

#define SAFE_RELEASE_RN(x)                            \
   do {                                               \
      if ( (x) && *(x) != SDB_INVALID_HANDLE ) {      \
         sdbReleaseNode ( *(x) ) ;                    \
         *(x) = SDB_INVALID_HANDLE ;                  \
      }                                               \
   } while ( 0 )

#define SAFE_RELEASE_RG(x)                            \
   do {                                               \
      if ( (x) && *(x) != SDB_INVALID_HANDLE ) {      \
         sdbReleaseReplicaGroup ( *(x) ) ;                   \
         *(x) = SDB_INVALID_HANDLE ;                  \
      }                                               \
   } while ( 0 )

#define SAFE_RELEASE_CS(x)                            \
   do {                                               \
      if ( (x) && *(x) != SDB_INVALID_HANDLE ) {      \
         sdbReleaseCS ( *(x) ) ;                      \
         *(x) = SDB_INVALID_HANDLE ;                  \
      }                                               \
   } while (0)

#define SAFE_RELEASE_COLLECTION(x)                    \
   do {                                               \
      if ( (x) && *(x) != SDB_INVALID_HANDLE ) {      \
         sdbReleaseCollection ( *(x) ) ;              \
         *(x) = SDB_INVALID_HANDLE ;                  \
      }                                               \
   } while (0)

#define SAFE_RELEASE_CURSOR(x)                        \
   do {                                               \
      if ( (x) && *(x) != SDB_INVALID_HANDLE ) {      \
         sdbReleaseCursor ( *(x) ) ;                  \
         *(x) = SDB_INVALID_HANDLE ;                  \
      }                                               \
   } while (0)

#define SAFE_RELEASE_DOMAIN(x)                        \
   do {                                               \
      if ( (x) && *(x) != SDB_INVALID_HANDLE ) {      \
         sdbReleaseDomain ( *(x) ) ;                  \
         *(x) = SDB_INVALID_HANDLE ;                  \
      }                                               \
   } while (0)

#define SDB_JSVAL_IS_OBJECT(x) \
   (JSVAL_IS_NULL(x) || JSVAL_IS_VOID(x) || ! JSVAL_IS_PRIMITIVE(x))

#define NODE_NAME_SPLIT ':'
#define SDB_DEF_COORD_NAME "localhost"
#define SDB_DEF_COORD_PORT OSS_DFT_SVCPORT

#if defined (SDB_FMP)
extern CHAR FMP_COORD_SERVICE[OSS_MAX_PATHSIZE+1] ;
extern CHAR *FMP_COORD_HOST ;
extern CHAR g_UserName[ OSS_MAX_PATHSIZE + 1 ] ;
extern CHAR g_Password[ OSS_MAX_PATHSIZE + 1 ] ;
SDB_EXTERN_C_START
extern BOOLEAN g_disablePassEncode ;
SDB_EXTERN_C_END
#endif // SDB_FMP

#if defined (_WINDOWS)
#define TF_REL_PATH "..\\doc\\manual\\"
#else
#define TF_REL_PATH "../doc/manual/"
#endif

OSS_INLINE JSObject *SDB_JSVAL_TO_OBJECT( jsval x )
{
   if ( JSVAL_IS_NULL(x) || JSVAL_IS_VOID(x) )
      return NULL ;
   else
      return JSVAL_TO_OBJECT(x) ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_OBJ2BSON, "objToBson" )
static JSBool objToBson ( JSContext *cx , JSObject *obj , bson ** bs )
{
   PD_TRACE_ENTRY ( SDB_OBJ2BSON );
   char *      js          = NULL ;
   JSBool      ret         = JS_TRUE ;
   INT32 rc                = SDB_OK ;

   VERIFY ( cx && obj && bs ) ;

   {
   sptConvertor convertor( cx ) ;
   rc = convertor.toBson( obj, bs ) ;
   VERIFY( SDB_OK == rc ) ;
   }

/*
   global = JS_GetGlobalForObject ( cx , obj ) ;
   VERIFY ( global ) ;

   ret = JS_GetProperty( cx, global, "JSON", &objJson ) ;
   VERIFY ( ret ) ;

   arg = OBJECT_TO_JSVAL ( obj ) ;
   ret = JS_CallFunctionName ( cx , JSVAL_TO_OBJECT ( objJson ) ,
                               "stringify" , 1 , &arg , &strJson ) ;
   VERIFY ( ret ) ;

   js = JS_EncodeString ( cx , JSVAL_TO_STRING ( strJson ) ) ;
   VERIFY ( js ) ;

   *bs = bson_create() ;
   VERIFY ( *bs ) ;

   if ( FALSE == jsonToBson ( *bs , js ) )
   {
      ret = JS_FALSE ;
      goto error ;
   }
*/
done :
   SAFE_JS_FREE ( cx , js ) ;
   PD_TRACE_EXIT ( SDB_OBJ2BSON );
   return ret ;
error :
   SAFE_BSON_DISPOSE ( *bs ) ;
   TRY_REPORT ( cx, "objToBson: false" ) ;
   ret = JS_FALSE ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_BSON_DESTRUCTOR, "bson_destructor" )
static void bson_destructor ( JSContext *cx , JSObject *obj )
{
   PD_TRACE_ENTRY ( SDB_BSON_DESTRUCTOR );
    bson *record = (bson *) JS_GetPrivate ( cx , obj ) ;
    SAFE_BSON_DISPOSE ( record ) ;
    JS_SetPrivate ( cx , obj , NULL ) ;
    PD_TRACE_EXIT ( SDB_BSON_DESTRUCTOR );
}

static JSClass bson_class = {
   "Bson",                       // class name
   JSCLASS_HAS_PRIVATE,          // flags
   JS_PropertyStub,              // addProperty
   JS_PropertyStub,              // delProperty
   JS_PropertyStub,              // getProperty
   JS_StrictPropertyStub,        // setProperty
   JS_EnumerateStub,             // enumerate
   JS_ResolveStub,               // resolve
   JS_ConvertStub,               // convert
   bson_destructor,              // finalize
   JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
};

// PD_TRACE_DECLARE_FUNCTION ( SDB_BSON_CONSTRUCTOR, "bson_constructor" )
static JSBool bson_constructor ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_BSON_CONSTRUCTOR );
   JSBool ret = JS_TRUE ;
   REPORT ( JS_FALSE , "use of new Bson() is forbidden, you should use "
                       "other functions to produce a Bson object" ) ;

done :
   PD_TRACE_EXIT ( SDB_BSON_CONSTRUCTOR );
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_BSON_TO_JSON, "bson_to_json" )
static JSBool bson_to_json ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_BSON_TO_JSON );
   bson *      record   = NULL ;
   char *      buf      = NULL ;
   INT32       size     = 0 ;
   JSString *  json     = NULL ;
   JSBool      ret      = JS_TRUE ;

   record = (bson *) JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( record , "Bson.toJson(): no bson record" ) ;

   size = bson_sprint_length ( record ) ;
   VERIFY ( size > 0 ) ;

   buf = (char *) JS_malloc ( cx , size ) ;
   VERIFY ( buf ) ;

   VERIFY ( bsonToJson ( buf , size , record, FALSE, FALSE ) );

   json = JS_NewStringCopyN ( cx , buf , ossStrlen ( buf ) ) ;
   VERIFY ( json ) ;

   JS_SET_RVAL ( cx , vp , STRING_TO_JSVAL ( json ) ) ;

done :
   SAFE_JS_FREE ( cx , buf ) ;
   PD_TRACE_EXIT ( SDB_BSON_TO_JSON );
   return ret ;
error :
   TRY_REPORT ( cx , "Bson.toJson(): false" ) ;
   goto done ;
}

static JSFunctionSpec bson_functions[] = {
    JS_FS ( "toJson" , bson_to_json , 0 , 0 ) ,
    JS_FS_END
} ;


// PD_TRACE_DECLARE_FUNCTION ( SDB_GLOBAL_PRINT, "global_print" )
static JSBool global_print ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_GLOBAL_PRINT );
   JSBool      ret      = JS_TRUE ;
   JSString *  strVal   = NULL ;
   CHAR *      val      = NULL ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strVal ) ;
   REPORT ( ret , "print(): wrong arguments" ) ;

   val = (CHAR *) JS_EncodeString ( NULL , strVal ) ;
   if ( val )
   {
      ossPrintf ( "%s" , val ) ;
   }
   else
   {
#if defined (_DEBUG)
      PD_LOG ( PDWARNING ,
               "Failed to encode BSON record in javascript engine. "
               "It is possible the record contains invalid UTF-8 string" ) ;
#endif
   }

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   if ( val )
      free ( val ) ;
   PD_TRACE_EXIT ( SDB_GLOBAL_PRINT );
   return ret ;
error :
   TRY_REPORT ( cx , "print(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_TRACE_FMT, "trace_fmt" )
static JSBool trace_fmt ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_TRACE_FMT );
   JSBool                  ret          = JS_FALSE ;
   CHAR                   *pInputName   = NULL ;
   CHAR                   *pOutputName  = NULL ;
   INT32                   formatType   = 0 ;
   JSString               *strInput     = NULL ;
   JSString               *strOutput    = NULL ;
   INT32                   rc           = SDB_OK ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "iSS" ,
                               &formatType , &strInput , &strOutput ) ;
   REPORT ( ret, "traceFmt(): wrong arguments" ) ;

   pInputName = (CHAR*) JS_EncodeString ( cx, strInput ) ;
   VERIFY ( pInputName ) ;

   pOutputName = (CHAR*) JS_EncodeString ( cx, strOutput ) ;
   VERIFY ( pOutputName ) ;

   if ( PD_TRACE_FORMAT_TYPE_FLOW != formatType &&
        PD_TRACE_FORMAT_TYPE_FORMAT != formatType )
   {
      rc = SDB_INVALIDARG ;
      REPORT_RC ( JS_FALSE, "traceFmt(): bad format type", rc ) ;
   }

   rc = pdTraceCB::format ( pInputName, pOutputName,
                            (_pdTraceFormatType)formatType ) ;
   REPORT_RC ( SDB_OK == rc, "traceFmt()", rc ) ;
   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done :
   SAFE_JS_FREE ( cx, pInputName ) ;
   SAFE_JS_FREE ( cx, pOutputName ) ;
   PD_TRACE_EXIT ( SDB_TRACE_FMT );
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_GLOBAL_HELP, "global_help" )
static JSBool global_help ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_GLOBAL_HELP );
#if defined (SDB_SHELL)
   INT32 rc                                     = SDB_OK ;
   CHAR tfPath[ OSS_MAX_PATHSIZE + 1 ]          = { 0 } ;
   INT32 len                                    = 0 ;
#endif
   JSBool ret                                   = JS_TRUE ;
   JSString *strCate                            = NULL ;
   JSString *strCmd                             = NULL ;
   CHAR *cate                                    = NULL ;
   CHAR *cmd                                    = NULL ;

   ret = JS_ConvertArguments ( cx, argc, JS_ARGV ( cx , vp ),
                               "S/S", &strCate, &strCmd ) ;
   REPORT ( ret , "help(): wrong arguments" ) ;

   cate = (CHAR *) JS_EncodeString ( cx, strCate ) ;
   VERIFY ( cate ) ;
   if ( argc > 1 )
   {
      cmd = (CHAR *) JS_EncodeString ( cx, strCmd ) ;
      VERIFY ( cmd ) ;
   }

#if defined (SDB_SHELL)
   rc = getProgramPath( tfPath ) ;
   REPORT_RC ( SDB_OK == rc, "help()", rc ) ;
   len = ossStrlen(TF_REL_PATH) ;
   if ( ossStrlen( tfPath ) + len  > OSS_MAX_PATHSIZE )
   {
      rc = SDB_INVALIDARG ;
      REPORT_RC ( SDB_OK == rc, "help()", rc ) ;
   }
   ossStrncat ( tfPath, TF_REL_PATH, ossStrlen(TF_REL_PATH) ) ;
   rc = manHelp::getInstance( tfPath ).getFileHelp( cate, cmd ) ;
   REPORT_RC ( SDB_OK == rc, "help()", rc ) ;
#endif
   JS_SET_RVAL ( cx , vp, JSVAL_VOID ) ;

done :
   SAFE_JS_FREE ( cx, cate ) ;
   SAFE_JS_FREE ( cx, cmd ) ;

   PD_TRACE_EXIT ( SDB_GLOBAL_HELP );
   return ret ;
error :
   TRY_REPORT ( cx , "help(): false" ) ;
   goto done ;
}

static JSFunctionSpec global_functions[] = {
   JS_FS ( "print" , global_print , 1 , 0 ) ,
   JS_FS ( "traceFmt", trace_fmt, 3, 0 ) ,
   JS_FS ( "man", global_help, 1, 0 ),
   JS_FS_END
} ;


#if defined (SDB_CLIENT)
#include "../client/client.h"
#include "../client/client_internal.h"
#include "msg.h"

SDB_EXTERN_C_START
SDB_EXPORT INT32 __sdbGetReserveSpace1 ( sdbConnectionHandle cHandle,
                                         UINT64 *space ) ;
SDB_EXPORT INT32 __sdbSetReserveSpace1 ( sdbConnectionHandle cHandle,
                                         UINT64 space ) ;
SDB_EXTERN_C_END

JSBool get_cs_and_setproperty( JSContext *cx, jsval *vp,
                               sdbConnectionHandle *connection,
                               const CHAR *csName,
                               JSString *jsCsName ) ;
JSBool get_rg_and_setproperty( JSContext *cx, jsval *vp,
                               sdbConnectionHandle conn,
                               UINT32 id, const CHAR *name ) ;

JSBool get_node_and_setproperty( JSContext *cx, jsval *vp,
                                 jsval *valRG,
                                 const CHAR *pHostName,
                                 const CHAR *pServiceName ) ;

// PD_TRACE_DECLARE_FUNCTION ( SDB_CURSOR_DESTRUCTOR, "cursor_destructor" )
static void cursor_destructor ( JSContext *cx , JSObject *obj )
{
   PD_TRACE_ENTRY ( SDB_CURSOR_DESTRUCTOR );
   sdbCursorHandle *cursor = (sdbCursorHandle *) JS_GetPrivate ( cx , obj ) ;
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   JS_SetPrivate ( cx , obj , NULL ) ;
   PD_TRACE_EXIT ( SDB_CURSOR_DESTRUCTOR );
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_CURSOR_RESV, "cursor_resolve" )
static JSBool cursor_resolve ( JSContext *cx , JSObject *obj , jsid id ,
                               uintN flags , JSObject ** objp )
{
   PD_TRACE_ENTRY ( SDB_CURSOR_RESV );
   BOOLEAN  ret   = JS_TRUE ;
   jsval    val   = JSVAL_VOID ;
   jsval    valID = JSVAL_VOID ;

   ret = JS_IdToValue ( cx , id , &valID ) ;
   VERIFY ( ret ) ;

   if ( flags & JSRESOLVE_ASSIGNING )
      goto done ;

   if ( ! JSVAL_IS_INT ( valID ) )
      goto done ;

   ret = JS_CallFunctionName ( cx , obj , "arrayAccess" , 1 , &valID , &val ) ;
   VERIFY ( ret ) ;

   ret = JS_SetPropertyById ( cx , obj , id , &val ) ;
   VERIFY ( ret ) ;

   *objp = obj ;

done :
   PD_TRACE_EXIT ( SDB_CURSOR_RESV );
   return ret ;
error :
   goto done ;
}

static JSClass cursor_class = {
   "SdbCursor",                  // class name
   JSCLASS_HAS_PRIVATE | JSCLASS_NEW_RESOLVE ,          // flags
   JS_PropertyStub,              // addProperty
   JS_PropertyStub,              // delProperty
   JS_PropertyStub,              // getProperty
   JS_StrictPropertyStub,        // setProperty
   JS_EnumerateStub,             // enumerate
   (JSResolveOp) cursor_resolve ,               // resolve
   JS_ConvertStub,               // convert
   cursor_destructor,        // finalize
   JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
};

// PD_TRACE_DECLARE_FUNCTION ( SDB_CURSOR_CONSTRUCTOR, "cursor_constructor" )
static JSBool cursor_constructor ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_CURSOR_CONSTRUCTOR );
   JSBool ret = JS_TRUE ;

   REPORT ( JS_FALSE , "use of new SdbCursor() is forbidden, you should use "
                       "other functions to produce a SdbCursor object" ) ;

done :
   PD_TRACE_EXIT ( SDB_CURSOR_CONSTRUCTOR );
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_CURSOR_NEXT, "cursor_next" )
static JSBool cursor_next ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_CURSOR_NEXT );
   sdbCursorHandle * cursor   = NULL ;
   JSObject *        bsonObj  = NULL ;
   bson *            record   = NULL ;
   bson *            copy     = NULL ;
   INT32             rc       = SDB_OK ;
   JSBool            ret      = JS_TRUE ;

   cursor = (sdbCursorHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   if ( ! cursor )
   {
      JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
      goto done ;
   }

   record = bson_create() ;
   VERIFY ( record ) ;

   rc = sdbNext( *cursor , record ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc , "SdbCursor.next()" , rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      if ( 0 == ( ( (sdbCursorStruct*)*cursor )->_totalRead ) )
      {
         engine::sdbSetReadData( FALSE ) ;
      }
      else
      {
         engine::sdbSetReadData( TRUE ) ;
      }

      SAFE_RELEASE_CURSOR ( cursor ) ;
      SAFE_JS_FREE ( cx , cursor ) ;
      JS_SetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) , NULL ) ;
      JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
      goto done ;
   }

   bsonObj = JS_NewObject ( cx , &bson_class , 0 , 0 ) ;
   VERIFY ( bsonObj ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( bsonObj ) ) ;

   copy = bson_create() ;
   VERIFY ( copy ) ;

   VERIFY ( BSON_OK == bson_copy ( copy , record ) ) ;

   ret = JS_SetPrivate ( cx , bsonObj , copy ) ;
   VERIFY ( ret ) ;

done :
   SAFE_BSON_DISPOSE ( record ) ;
   PD_TRACE_EXIT ( SDB_CURSOR_NEXT );
   return ret ;
error :
   SAFE_BSON_DISPOSE ( copy ) ;
   TRY_REPORT ( cx , "SdbCursor.next(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_CURSOR_CURRENT, "cursor_current" )
static JSBool cursor_current ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_CURSOR_CURRENT );
   sdbCursorHandle * cursor   = NULL ;
   JSObject *        bsonObj  = NULL ;
   bson *            record   = NULL ;
   bson *            copy     = NULL ;
   INT32             rc       = SDB_OK ;
   JSBool            ret      = JS_TRUE ;

   cursor = (sdbCursorHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   if ( ! cursor )
   {
      JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
      goto done ;
   }

   record = bson_create() ;
   VERIFY ( record ) ;

   rc = sdbCurrent ( *cursor , record ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCursor.current()" , rc ) ;

   bsonObj = JS_NewObject ( cx , &bson_class , 0 , 0 ) ;
   VERIFY ( bsonObj ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( bsonObj ) ) ;

   copy = bson_create() ;
   VERIFY ( copy ) ;

   VERIFY ( BSON_OK == bson_copy ( copy , record ) ) ;

   ret = JS_SetPrivate ( cx , bsonObj , copy ) ;
   VERIFY ( ret ) ;

done :
   SAFE_BSON_DISPOSE ( record ) ;
   PD_TRACE_EXIT ( SDB_CURSOR_CURRENT );
   return ret ;
error :
   SAFE_BSON_DISPOSE ( copy ) ;
   TRY_REPORT ( cx , "SdbCursor.current(): false" ) ;
   goto done ;
}
/*
// PD_TRACE_DECLARE_FUNCTION ( SDB_CURSOR_UP_CURRENT, "cursor_update_current" )
static JSBool cursor_update_current ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_CURSOR_UP_CURRENT );
   sdbCursorHandle * cursor      = NULL ;
   JSObject *        objRule     = NULL ;
   bson *            bsonRule    = NULL ;
   INT32             rc          = SDB_OK ;
   JSBool            ret         = JS_TRUE ;
   jsval *           argv        = JS_ARGV ( cx , vp ) ;

   cursor = (sdbCursorHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( cursor , "SdbCursor.updateCurrent(): no cursor handle" ) ;

   if ( ! cursor )
   {
      JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
      goto done ;
   }

   REPORT ( argc >= 1 && !JSVAL_IS_PRIMITIVE ( argv[0] ) ,
         "SdbCursor.updateCurrent(): 1st argument must be valid update rule") ;

   objRule = JSVAL_TO_OBJECT ( argv[0] ) ;
   VERIFY ( objToBson ( cx , objRule , &bsonRule ) ) ;

   rc = sdbUpdateCurrent ( *cursor , bsonRule ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCursor.updateCurrent()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_BSON_DISPOSE ( bsonRule ) ;
   PD_TRACE_EXIT ( SDB_CURSOR_UP_CURRENT );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCursor.updateCurrent(): false" ) ;
   goto done ;
}
*/
/*
// PD_TRACE_DECLARE_FUNCTION ( SDB_CURSOR_DEL_CURR, "cursor_delete_current" )
static JSBool cursor_delete_current ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_CURSOR_DEL_CURR );
   sdbCursorHandle * cursor   = NULL ;
   INT32             rc       = SDB_OK ;
   JSBool            ret      = JS_TRUE ;

   cursor = (sdbCursorHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   if ( ! cursor )
   {
      JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
      goto done ;
   }

   rc = sdbDeleteCurrent ( *cursor ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCursor.deleteCurrent()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   PD_TRACE_EXIT ( SDB_CURSOR_DEL_CURR );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCursor.deleteCurrent(): false" ) ;
   goto done ;
}
*/

// PD_TRACE_DECLARE_FUNCTION ( SDB_CURSOR_CLOSE, "cursor_close" )
static JSBool cursor_close ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_CURSOR_CLOSE );
   sdbCursorHandle * cursor   = NULL ;
   JSBool            ret      = JS_TRUE ;
   INT32             rc       = SDB_OK ;

   cursor = (sdbCursorHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   if ( ! cursor )
   {
      JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
      goto done ;
   }

   rc = sdbCloseCursor( *cursor ) ;
   REPORT_RC ( SDB_OK == rc, "SdbCursor.close()", rc ) ;
   SAFE_RELEASE_CURSOR ( cursor ) ;
   JS_SetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ), NULL ) ;
   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done :
   PD_TRACE_EXIT ( SDB_CURSOR_CLOSE );
   return ret ;
error :
   ret = JS_FALSE ;
   TRY_REPORT ( cx , "SdbCursor.close(): false" ) ;
   goto done ;
}

static JSFunctionSpec cursor_functions[] = {
   JS_FS ( "next", cursor_next, 0, 0 ),
   JS_FS ( "current", cursor_current, 0, 0 ),
   JS_FS ( "close", cursor_close, 0, 0 ),
   JS_FS_END
} ;


/*
// PD_TRACE_DECLARE_FUNCTION ( SDB_COUNT_DESTRUCTOR, "count_destructor" )
static void count_destructor ( JSContext *cx , JSObject *obj )
{
   PD_TRACE_ENTRY ( SDB_COUNT_DESTRUCTOR );
   JS_SetPrivate ( cx , obj , NULL ) ;
   PD_TRACE_EXIT ( SDB_COUNT_DESTRUCTOR );
}
*/

// PD_TRACE_DECLARE_FUNCTION ( SDB_COUNT_RESV, "count_resolve" )
static JSBool count_resolve ( JSContext *cx , JSObject *obj , jsid id ,
                               uintN flags , JSObject ** objp )
{
   PD_TRACE_ENTRY ( SDB_COUNT_RESV );
   BOOLEAN  ret   = JS_TRUE ;
   jsval    val   = JSVAL_VOID ;
   jsval    valID = JSVAL_VOID ;

   ret = JS_IdToValue ( cx , id , &valID ) ;
   VERIFY ( ret ) ;

   if ( flags & JSRESOLVE_ASSIGNING )
      goto done ;

   if ( ! JSVAL_IS_INT ( valID ) )
      goto done ;

   ret = JS_CallFunctionName ( cx , obj , "_exec" , 1 , &valID , &val ) ;
   VERIFY ( ret ) ;

   ret = JS_SetPropertyById ( cx , obj , id , &val ) ;
   VERIFY ( ret ) ;

   *objp = obj ;
done :
   PD_TRACE_EXIT ( SDB_COUNT_RESV );
   return ret ;
error :
   goto done ;
}

static JSClass count_class = {
   "CLCount",                    // class name
   JSCLASS_HAS_PRIVATE | JSCLASS_NEW_RESOLVE ,          // flags
   JS_PropertyStub,              // addProperty
   JS_PropertyStub,              // delProperty
   JS_PropertyStub,              // getProperty
   JS_StrictPropertyStub,        // setProperty
   JS_EnumerateStub,             // enumerate
   (JSResolveOp) count_resolve , // resolve
   JS_ConvertStub,               // convert
   JS_FinalizeStub,              // finalize
   JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
};

// PD_TRACE_DECLARE_FUNCTION ( SDB_COUNT_CONSTRUCTOR, "count_constructor" )
static JSBool count_constructor ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COUNT_CONSTRUCTOR );
   JSObject * objCollection   = NULL ;
   JSObject *  objCondition   = NULL ;
   JSObject *  objHint        = NULL ;
   JSObject *  obj            = NULL ;
   jsval       val            = JSVAL_VOID ;
   JSBool      ret            = JS_TRUE ;
   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "o/oo" ,
                               &objCollection, &objCondition, &objHint ) ;
   REPORT ( ret , "new CLCount(): wrong arguments" ) ;
   VERIFY ( objCollection ) ;
   obj = JS_NewObject ( cx , &count_class , NULL , NULL ) ;
   VERIFY ( obj ) ;
   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( obj ) ) ;
   val = OBJECT_TO_JSVAL ( objCollection ) ;
   VERIFY ( JS_SetProperty ( cx, obj, "_collection", &val ) ) ;
   val = OBJECT_TO_JSVAL ( objCondition ) ;
   VERIFY ( JS_SetProperty ( cx, obj, "_condition", &val ) ) ;
   val = OBJECT_TO_JSVAL ( objHint ) ;
   VERIFY ( JS_SetProperty ( cx, obj, "_hint", &val ) ) ;
   val = JSVAL_NULL ;
   VERIFY ( JS_SetProperty ( cx, obj, "_count", &val ) ) ;
done :
   PD_TRACE_EXIT ( SDB_COUNT_CONSTRUCTOR );
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_DESTRUCTOR, "collection_destructor" )
static void collection_destructor ( JSContext *cx , JSObject *obj )
{
   PD_TRACE_ENTRY ( SDB_COLL_DESTRUCTOR );
   sdbCollectionHandle *collection = NULL ;
   collection = (sdbCollectionHandle *) JS_GetPrivate ( cx , obj ) ;
   SAFE_RELEASE_COLLECTION ( collection ) ;
   SAFE_JS_FREE ( cx , collection ) ;
   JS_SetPrivate ( cx , obj , NULL ) ;
   PD_TRACE_EXIT ( SDB_COLL_DESTRUCTOR );
}

static JSClass collection_class = {
   "SdbCollection" ,             // class name
   JSCLASS_HAS_PRIVATE ,         // flags
   JS_PropertyStub ,             // addProperty
   JS_PropertyStub ,             // delProperty
   JS_PropertyStub ,             // getProperty
   JS_StrictPropertyStub ,       // setProperty
   JS_EnumerateStub ,            // enumerate
   JS_ResolveStub ,              // resolve
   JS_ConvertStub ,              // convert
   collection_destructor ,       // finalize
   JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
};

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_CONSTRUCTOR, "collection_constructor" )
static JSBool collection_constructor ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_CONSTRUCTOR );
   JSBool ret = JS_TRUE ;

   REPORT ( JS_FALSE, "use of new SdbCollection() is forbidden, you should use "
                       "other functions to produce a SdbCollection object" ) ;

done :
   PD_TRACE_EXIT ( SDB_COLL_CONSTRUCTOR );
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_RAW_FND, "collection_raw_find" )
static JSBool collection_raw_find ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_RAW_FND );
   sdbCollectionHandle *collection  = NULL ;
   JSObject *objCond                = NULL ;
   JSObject *objSel                 = NULL ;
   JSObject *objOrder               = NULL ;
   JSObject *objHint                = NULL ;
   int32_t numToSkip                = 0 ;
   int32_t numToRet                 = -1 ;
   int32_t flags                    = 0 ;
   bson *bsonCond                   = NULL ;
   bson *bsonSel                    = NULL ;
   bson *bsonOrder                  = NULL ;
   bson *bsonHint                   = NULL ;
   sdbCursorHandle *cursor          = NULL ;
   JSObject *objCursor              = NULL ;
   INT32 rc                         = SDB_OK ;
   JSBool ret                       = JS_TRUE ;
   jsval *argv                      = JS_ARGV ( cx, vp ) ;

   collection  = (sdbCollectionHandle *)
                  JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( collection , "SdbCollection.rawFind(): no collection handle" ) ;

   if ( JSVAL_IS_VOID( argv[0]) || JSVAL_IS_NULL( argv[0] ) )
   {
   }
   else if ( JSVAL_IS_OBJECT( argv[0] ) )
   {
      objCond = JSVAL_TO_OBJECT ( argv[0] ) ;
      VERIFY ( objCond ) ;
      VERIFY ( objToBson( cx, objCond, &bsonCond ) ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.rawFind(): the 1st argument wrong" ) ;
   }
   if ( JSVAL_IS_VOID( argv[1]) || JSVAL_IS_NULL( argv[1] ) )
   {
   }
   else if ( JSVAL_IS_OBJECT( argv[1] ) )
   {
      objSel = JSVAL_TO_OBJECT ( argv[1] ) ;
      VERIFY ( objSel ) ;
      VERIFY ( objToBson( cx, objSel, &bsonSel ) ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.rawFind(): the 2nd argument wrong" ) ;
   }
   if ( JSVAL_IS_VOID( argv[2]) || JSVAL_IS_NULL( argv[2]) )
   {
   }
   else if ( JSVAL_IS_OBJECT( argv[2] ) )
   {
      objOrder = JSVAL_TO_OBJECT ( argv[2] ) ;
      VERIFY ( objOrder ) ;
      VERIFY ( objToBson( cx, objOrder, &bsonOrder ) ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.rawFind(): wrong argument in sort(<sort>)" ) ;
   }
   if ( JSVAL_IS_VOID( argv[3] ) || JSVAL_IS_NULL( argv[3] ) )
   {
   }
   else if ( JSVAL_IS_OBJECT( argv[3] ) )
   {
      objHint = JSVAL_TO_OBJECT ( argv[3] ) ;
      VERIFY ( objHint ) ;
      VERIFY ( objToBson( cx, objHint, &bsonHint ) ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.rawFind(): wrong argument in hint(<hint>)" ) ;
   }
   if ( JSVAL_IS_VOID( argv[4] ) || JSVAL_IS_NULL( argv[4] ) )
   {
   }
   else if ( JSVAL_IS_INT( argv[4] ) )
   {
     numToSkip = JSVAL_TO_INT ( argv[4] ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.rawFind(): wrong argument in skip(<num>)" ) ;
   }
   if ( JSVAL_IS_VOID( argv[5] ) || JSVAL_IS_NULL( argv[5] ) )
   {
   }
   else if ( JSVAL_IS_INT( argv[5] ) )
   {
      numToRet = JSVAL_TO_INT ( argv[5] ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.rawFind(): wrong argument in limit(<num>)" ) ;
   }
   if ( JSVAL_IS_VOID( argv[6] ) || JSVAL_IS_NULL( argv[6] ) )
   {
   }
   else if ( JSVAL_IS_INT( argv[6] ) )
   {
      flags = JSVAL_TO_INT ( argv[6] ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.rawFind(): wrong argument in flags(<num>)" ) ;
   }
/*
   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "/ooooii" ,
                               &objCond , &objSel , &objOrder , &objHint ,
                               &numToSkip , &numToRet ) ;
   REPORT ( ret , "SdbCollection.rawFind(): wrong arguments" ) ;

   if ( objCond )
   {

      ret = objToBson ( cx , objCond , &bsonCond ) ;
      VERIFY ( ret ) ;
   }

   if ( objSel )
   {

      ret = objToBson ( cx , objSel , &bsonSel ) ;
      VERIFY ( ret ) ;
   }

   if ( objOrder )
   {

      ret = objToBson ( cx , objOrder , &bsonOrder ) ;
      VERIFY ( ret ) ;
   }

   if ( objHint )
   {

      ret = objToBson ( cx , objHint , &bsonHint ) ;
      VERIFY ( ret ) ;
   }
*/
   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   rc = sdbQuery1( *collection , bsonCond , bsonSel , bsonOrder , bsonHint ,
                   numToSkip , numToRet , flags , cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc ,
               "SdbCollection.find()" , rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;
done :
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonSel ) ;
   SAFE_BSON_DISPOSE ( bsonOrder ) ;
   SAFE_BSON_DISPOSE ( bsonHint ) ;
   PD_TRACE_EXIT ( SDB_COLL_RAW_FND );
   return ret ;
error :
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "SdbCollection.find(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_INSERT, "collection_insert" )
static JSBool collection_insert ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_INSERT );
   sdbCollectionHandle *collection  = NULL ;
   JSObject *           objData     = NULL ;
   bson *               bsonData    = NULL ;
   JSBool               returnID    = JS_FALSE ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   bson_oid_t           *oid        = NULL ;
   JSString *           strID       = NULL ;
   bson_iterator        id ;
   char                 buf[25] ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection._insert(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "ob" , &objData , &returnID ) ;
   REPORT ( ret , "SdbCollection._insert(): wrong arguments" ) ;

   if ( JS_FALSE == objToBson ( cx , objData , &bsonData ) )
   {
      rc = SDB_INVALIDARG ;
      REPORT_RC ( JS_FALSE , "SdbCollection._insert()" , rc ) ;
   }

   if ( returnID )
   {
      rc = sdbInsert1 ( *collection , bsonData , &id ) ;
      REPORT_RC ( SDB_OK == rc , "SdbCollection._insert()" , rc ) ;

      oid = bson_iterator_oid ( &id ) ;
      VERIFY ( oid ) ;

      bson_oid_to_string ( oid , buf ) ;
      strID = JS_NewStringCopyN ( cx , buf , ossStrlen ( buf ) ) ;
      VERIFY ( strID ) ;

      JS_SET_RVAL ( cx , vp , STRING_TO_JSVAL ( strID ) ) ;
   }
   else
   {
      rc = sdbInsert ( *collection , bsonData ) ;
      REPORT_RC ( SDB_OK == rc , "SdbCollection._insert()" , rc ) ;

      JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
   }

done :
   SAFE_BSON_DISPOSE ( bsonData ) ;
   PD_TRACE_EXIT ( SDB_COLL_INSERT );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection._insert(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_UPDATE, "collection_update" )
static JSBool collection_update ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_UPDATE );
   sdbCollectionHandle *collection  = NULL ;
   JSObject *           objRule     = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objHint     = NULL ;
   bson *               bsonRule    = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonHint    = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   jsval *              argv        = JS_ARGV ( cx , vp ) ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.update(): no collection handle" ) ;

   REPORT ( argc >= 1 && !JSVAL_IS_PRIMITIVE ( argv[0] ) ,
            "SdbCollection.update(): 1st argument must be valid update rule") ;

   objRule = JSVAL_TO_OBJECT ( argv[0] ) ;
   VERIFY ( objToBson ( cx , objRule , &bsonRule ) ) ;

   if ( argc >= 2 && SDB_JSVAL_IS_OBJECT ( argv[1] ) )
   {
      objCond = SDB_JSVAL_TO_OBJECT ( argv[1] ) ;
      if ( objCond )
      {
         argv[1] = OBJECT_TO_JSVAL ( objCond ) ;
         VERIFY ( objToBson( cx , objCond , &bsonCond ) ) ;
      }
   }

   if ( argc >= 3 && SDB_JSVAL_IS_OBJECT ( argv[2] ) )
   {
      objHint = SDB_JSVAL_TO_OBJECT ( argv[2] ) ;
      if ( objHint )
      {
         argv[2] = OBJECT_TO_JSVAL ( objHint ) ;
         VERIFY ( objToBson ( cx , objHint , &bsonHint ) ) ;
      }
   }

   rc = sdbUpdate ( *collection , bsonRule , bsonCond , bsonHint ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCollection.update()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_BSON_DISPOSE ( bsonRule ) ;
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonHint ) ;
   PD_TRACE_EXIT ( SDB_COLL_UPDATE );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbColleciton.update(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_UPSERT, "collection_upsert" )
static JSBool collection_upsert ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_UPSERT );
   sdbCollectionHandle *collection  = NULL ;
   JSObject *           objRule     = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objHint     = NULL ;
   bson *               bsonRule    = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonHint    = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   jsval *              argv        = JS_ARGV ( cx , vp ) ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.upsert(): no collection handle" ) ;

   REPORT ( argc >= 1 && !JSVAL_IS_PRIMITIVE ( argv[0] ) ,
            "SdbCollection.upsert(): 1st argument must be valid update rule") ;

   objRule = JSVAL_TO_OBJECT ( argv[0] ) ;
   VERIFY ( objToBson ( cx , objRule , &bsonRule ) ) ;

   if ( argc >= 2 && SDB_JSVAL_IS_OBJECT ( argv[1] ) )
   {
      objCond = SDB_JSVAL_TO_OBJECT ( argv[1] ) ;
      if ( objCond )
      {
         argv[1] = OBJECT_TO_JSVAL ( objCond ) ;
         VERIFY ( objToBson( cx , objCond , &bsonCond ) ) ;
      }
   }

   if ( argc >= 3 && SDB_JSVAL_IS_OBJECT ( argv[2] ) )
   {
      objHint = SDB_JSVAL_TO_OBJECT ( argv[2] ) ;
      if ( objHint )
      {
         argv[2] = OBJECT_TO_JSVAL ( objHint ) ;
         VERIFY ( objToBson ( cx , objHint , &bsonHint ) ) ;
      }
   }

   rc = sdbUpsert ( *collection , bsonRule , bsonCond , bsonHint ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCollection.upsert()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_BSON_DISPOSE ( bsonRule ) ;
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonHint ) ;
   PD_TRACE_EXIT ( SDB_COLL_UPSERT );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbColleciton.upsert(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_REMOVE, "collection_remove" )
static JSBool collection_remove ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_REMOVE );
   sdbCollectionHandle *collection  = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objHint     = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonHint    = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.remove(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "/oo" , &objCond , &objHint ) ;
   REPORT ( ret , "SdbCollection.remove(): wrong arguments" ) ;

   if ( objCond )
   {
      ret = objToBson ( cx , objCond , &bsonCond ) ;
      VERIFY ( ret ) ;
   }

   if ( objHint )
   {
      ret = objToBson ( cx , objHint , &bsonHint ) ;
      VERIFY ( ret ) ;
   }

   rc = sdbDelete ( *collection , bsonCond , bsonHint ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCollection.remove()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonHint ) ;
   PD_TRACE_EXIT ( SDB_COLL_REMOVE );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection.remove(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_DELETE_LOB, "collection_delete_lob" )
static JSBool collection_delete_lob( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY( SDB_COLL_DELETE_LOB ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbCollectionHandle *collection = NULL ;
   JSString *jsOid = NULL ;
   CHAR *oidStr = NULL ;
   bson_oid_t oid ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.deleteLob(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &jsOid ) ;
   REPORT ( ret , "SdbCollection.deleteLob(): wrong arguments" ) ;

   oidStr = (CHAR *) JS_EncodeString ( cx , jsOid ) ;
   VERIFY( oidStr ) ;
   bson_oid_from_string( &oid, oidStr ) ;

   rc = sdbRemoveLob( *collection, &oid ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.deleteLob(): failed to delete lob", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done:
   SAFE_JS_FREE( cx, oidStr ) ; 
   PD_TRACE_EXIT( SDB_COLL_DELETE_LOB ) ;
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_LIST_LOBS, "collection_list_lobs" )
static JSBool collection_list_lobs( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY( SDB_COLL_LIST_LOBS ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbCollectionHandle *collection = NULL ;
   sdbCursorHandle *cursor = NULL ;
   JSObject *objCursor = NULL ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;

   REPORT ( 0 == argc , "SdbCollection.listLobs(): wrong arguments" ) ;

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;
   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   rc = sdbListLobs( *collection, cursor ) ;
   REPORT_RC ( SDB_OK == rc,
               "SdbCollection.listLobs()" , rc ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;
done:
   PD_TRACE_EXIT( SDB_COLL_LIST_LOBS ) ;
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "SdbCollection.listLobs(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_LIST_LOBPIECES, "collection_list_lob_pieces" )
static JSBool collection_list_lob_pieces( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY( SDB_COLL_LIST_LOBPIECES ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbCollectionHandle *collection = NULL ;
   sdbCursorHandle *cursor = NULL ;
   JSObject *objCursor = NULL ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;

   REPORT ( 0 == argc , "SdbCollection.listLobPieces(): wrong arguments" ) ;

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;
   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   rc = sdbListLobPieces( *collection, cursor ) ;
   REPORT_RC ( SDB_OK == rc,
               "SdbCollection.listLobPieces()" , rc ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;
done:
   PD_TRACE_EXIT( SDB_COLL_LIST_LOBPIECES ) ;
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "SdbCollection.listLobPieces(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_GET_LOB, "collection_get_lob" )
static JSBool collection_get_lob( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY( SDB_COLL_GET_LOB ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbCollectionHandle *collection = NULL ;
   JSString *jsPath = NULL ;
   JSString *jsOid = NULL ;
   JSBool replaceFile = JS_FALSE ;
   CHAR *path = NULL ;
   CHAR *oidStr = NULL ;
   OSSFILE file ;
   bson_oid_t oid ;
   sdbLobHandle lob = SDB_INVALID_HANDLE ;
   const UINT32 bufLen = 2 * 1024 * 1024 ;
   CHAR *buf = NULL ;
   INT32 mode = OSS_READWRITE|OSS_EXCLUSIVE ;
   SINT64 lobSize = 0 ;
   SINT64 readSize = 0 ;
   JSObject *rval = NULL ;
   bson *meta = NULL ;
   UINT64 createTime = 0 ;
   bson_timestamp_t t ;

   buf = ( CHAR * )SDB_OSS_MALLOC( bufLen ) ;
   REPORT_RC( NULL != buf, "SdbCollection.getLob(): failed to allocate mem", SDB_OOM ) ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.getLob(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "SS/b" , &jsOid, &jsPath, &replaceFile ) ;
   REPORT ( ret , "SdbCollection.getLob(): wrong arguments" ) ;

   oidStr = (CHAR *) JS_EncodeString ( cx , jsOid ) ;
   VERIFY( oidStr ) ;
   bson_oid_from_string( &oid, oidStr ) ;

   path = (CHAR *) JS_EncodeString ( cx , jsPath ) ;
   VERIFY( path ) ;

   if ( replaceFile )
   {
      mode |= OSS_REPLACE ;
   }
   else
   {
      mode |= OSS_CREATEONLY ;
   }
   
   rc = ossOpen( path, mode, OSS_DEFAULTFILE, file ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to open local file", rc ) ;

   rc = sdbOpenLob( *collection, &oid, SDB_LOB_READ, &lob ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to open lob", rc ) ;

   rc = sdbGetLobSize( lob, &lobSize ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to get size of lob", rc ) ;

   rc = sdbGetLobCreateTime( lob, &createTime ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to get create time of lob", rc ) ;

   while ( readSize < lobSize )
   {
      UINT32 read = 0 ;
      rc = sdbReadLob( lob, bufLen, buf, &read ) ;
      REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to read lob", rc ) ;
      readSize += read ;
      rc = ossWriteN( &file, buf, read ) ;
      REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to write local file", rc ) ;
      
   }

   rc = ossClose( file ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to close local file", rc ) ;

   rc = sdbCloseLob( &lob ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to close lob", rc ) ;

   meta = bson_create() ;
   VERIFY( meta ) ;

   rc = bson_append_long( meta, "LobSize", lobSize ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to create meta data", rc ) ;

   t.t = createTime / 1000 ;
   t.i = ( createTime - ( t.t * 1000 ) ) * 1000 ;
   rc = bson_append_timestamp( meta, "CreateTime", &t ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.getLob(): failed to create meta data", rc ) ;

   bson_finish( meta ) ;

   rval = JS_NewObject ( cx , &bson_class , 0 , 0 ) ;
   VERIFY ( rval ) ;

   ret = JS_SetPrivate ( cx , rval, meta ) ;
   VERIFY ( ret ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( rval ) ) ;

done:
   SAFE_JS_FREE( cx, path ) ;
   SAFE_JS_FREE( cx, oidStr ) ;
   if ( SDB_INVALID_HANDLE != lob )
   {
      sdbCloseLob( &lob ) ;
   }
   if ( file.isOpened() )
   {
      ossClose( file ) ;
   }

   SAFE_OSS_FREE( buf ) ;
   PD_TRACE_EXIT( SDB_COLL_GET_LOB ) ;
   return ret ;
error:
   if ( file.isOpened() )
   {
      ossClose( file ) ;
      ossDelete( path ) ;
   }

   SAFE_BSON_DISPOSE( meta ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_PUT_LOB, "collection_put_lob" )
static JSBool collection_put_lob( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY( SDB_COLL_PUT_LOB ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbCollectionHandle *collection = NULL ;
   JSString *jsPath = NULL ;
   JSString *jsOid = NULL ;
   CHAR *path = NULL ;
   CHAR *oidStr = NULL ;
   OSSFILE file ;
   bson_oid_t oid ;
   sdbLobHandle lob = SDB_INVALID_HANDLE ;
   SINT64 read = 0 ;
   const UINT32 bufLen = 2 * 1024 * 1024 ;
   CHAR *buf = ( CHAR * )SDB_OSS_MALLOC( bufLen ) ;
   REPORT_RC( NULL != buf, "SdbCollection.putLob(): failed to allocate mem", SDB_OOM ) ;
   ossMemset( buf, 0, bufLen ) ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.putLob(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S/S" , &jsPath, &jsOid ) ;
   REPORT ( ret , "SdbCollection.putLob(): wrong arguments" ) ;

   path = (CHAR *) JS_EncodeString ( cx , jsPath ) ;
   VERIFY( path ) ;

   if ( NULL != jsOid )
   {
      oidStr = (CHAR *) JS_EncodeString ( cx , jsOid ) ;
      VERIFY( path ) ;
   }

   if ( NULL != oidStr )
   {
      bson_oid_from_string( &oid, oidStr ) ;
   }
   else
   {
      bson_oid_gen( &oid ) ;
   }

   rc = ossOpen( path, OSS_READONLY|OSS_SHAREREAD, OSS_DEFAULTFILE, file ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.putLob(): failed to open file", rc ) ;

   rc = sdbOpenLob( *collection, &oid, SDB_LOB_CREATEONLY, &lob ) ;
   REPORT_RC( SDB_OK == rc, "SdbCollection.putLob(): failed to open lob", rc ) ;

   while ( SDB_OK == ( rc = ossReadN( &file, bufLen, buf, read ) ) )
   {
      rc = sdbWriteLob( lob, buf, read ) ;
      REPORT_RC( SDB_OK == rc, "SdbCollection.putLob(): failed to write lob", rc ) ;
      read = 0 ;
   }

   if ( SDB_EOF == rc )
   {
      rc = SDB_OK ;
      rc = sdbCloseLob( &lob ) ;
      REPORT_RC( SDB_OK == rc, "SdbCollection.putLob(): close lob with exception", rc ) ;
   }
   else
   {
      REPORT_RC( SDB_OK == rc, "SdbCollection.putLob(): failed to read local file", rc ) ;
   }

   if ( NULL == jsOid )
   {
      CHAR tmp[25] ;
      bson_oid_to_string( &oid, tmp ) ;
      jsOid = JS_NewStringCopyN( cx, tmp, 24 ) ;
   }
   JS_SET_RVAL ( cx , vp , STRING_TO_JSVAL( jsOid ) ) ;
done:
   SAFE_JS_FREE( cx, path ) ;
   SAFE_JS_FREE( cx, oidStr ) ;
   if ( SDB_INVALID_HANDLE != lob )
   {
      sdbCloseLob( &lob ) ;
   }
   if ( file.isOpened() )
   {
      ossClose( file ) ;
   }
   SAFE_OSS_FREE( buf ) ;
   PD_TRACE_EXIT( SDB_COLL_PUT_LOB ) ;
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_EXPLAIN, "collection_explain" )
static JSBool collection_explain( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY( SDB_COLL_EXPLAIN ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbCollectionHandle *collection = NULL ;
   JSObject *objExplain = NULL ;
   bson *explain = NULL ;
   INT32 skip = 0 ;
   INT32 limit = -1 ;
   JSObject *objCondition = NULL ;
   JSObject *objSelector = NULL ;
   JSObject *objSort = NULL ;
   JSObject *objHint = NULL ;

   bson *condition = NULL ;
   bson *selector = NULL ;
   bson *sort = NULL ;
   bson *hint = NULL ;

   sdbCursorHandle *cursor = NULL ;
   JSObject *objCursor = NULL ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.explain(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "ooooii/o" , &objCondition,
                               &objSelector, &objSort,
                               &objHint, &skip, &limit, &objExplain ) ;
   REPORT ( ret , "SdbCollection.explain(): wrong arguments" ) ;

   if ( NULL != objExplain )
   {
      ret = objToBson ( cx , objExplain , &explain ) ;
      VERIFY ( ret ) ;
   }

   if ( NULL != objCondition )
   {
      ret = objToBson ( cx , objCondition , &condition ) ;
      VERIFY ( ret ) ;
   }

   if ( NULL != objSelector )
   {
      ret = objToBson ( cx , objSelector , &selector ) ;
      VERIFY ( ret ) ;
   }

   if ( NULL != objSort )
   {
      ret = objToBson ( cx , objSort , &sort ) ;
      VERIFY ( ret ) ;
   }

   if ( NULL != objHint )
   {
      ret = objToBson ( cx , objHint, &hint ) ;
      VERIFY ( ret ) ;
   }

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;
   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   rc = sdbExplain( *collection, condition , selector , sort , hint ,
                   0, skip , limit , explain, cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc ,
               "SdbCollection.explain()" , rc ) ;
   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }
   else
   {
      VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;
   }
done:
   SAFE_BSON_DISPOSE( explain ) ;
   SAFE_BSON_DISPOSE( condition ) ;
   SAFE_BSON_DISPOSE( selector ) ;
   SAFE_BSON_DISPOSE( sort ) ;
   SAFE_BSON_DISPOSE( hint ) ;
   PD_TRACE_EXIT( SDB_COLL_EXPLAIN ) ;
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_COUNT, "collection_count" )
static JSBool collection_count ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_COUNT );
   sdbCollectionHandle *collection  = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objHint     = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonHint    = NULL ;
   JSBool               ret         = JS_TRUE ;
   INT32                rc          = SDB_OK ;
   INT64                count       = 0 ;
   jsval                valCount    = JSVAL_VOID ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.count(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "/oo" , &objCond, &objHint ) ;
   REPORT ( ret , "SdbCollection.count(): wrong arguments" ) ;

   if ( objCond )
   {
      ret = objToBson ( cx , objCond , &bsonCond ) ;
      VERIFY ( ret ) ;
   }
   if ( objHint )
   {
      ret = objToBson ( cx , objHint , &bsonHint ) ;
      VERIFY ( ret ) ;
   }

   rc = sdbGetCount1 ( *collection , bsonCond, bsonHint, &count ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCollection.count()" , rc ) ;

   ret = JS_NewNumberValue ( cx , count , &valCount ) ;
   VERIFY ( ret ) ;

   JS_SET_RVAL ( cx , vp , valCount ) ;

done :
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonHint ) ;
   PD_TRACE_EXIT ( SDB_COLL_COUNT );
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_SPLIT, "collection_split" )
static JSBool collection_split ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_SPLIT );
   sdbCollectionHandle *collection  = NULL ;
   JSBool               ret         = JS_TRUE ;
   INT32                rc          = SDB_OK ;
   JSString *           strSource   = NULL ;
   JSString *           strTarget   = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objEndCond  = NULL ;
   CHAR *               source      = NULL ;
   CHAR *               target      = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonEndCond = NULL ;
   FLOAT64              percent     = 0.0 ;
   jsval*               argv        = JS_ARGV( cx, vp ) ;


   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.split(): no collection handle" ) ;

   REPORT( argc >= 3, "SdbCollection.split(): wrong argument number" ) ;

   if ( JSVAL_IS_INT( argv[2] ) )
   {
      percent = (FLOAT64)JSVAL_TO_INT( argv[2] ) ;
      REPORT( argc == 3, "SdbCollection.split(): invalid argument" ) ;
   }
   else if ( JSVAL_IS_DOUBLE( argv[2] ) )
   {
      percent = (FLOAT64)JSVAL_TO_DOUBLE( argv[2] ) ;
      REPORT( argc == 3, "SdbCollection.split(): invalid argument" ) ;
   }
   else if ( SDB_JSVAL_IS_OBJECT( argv[2] ) )
   {
      objCond = SDB_JSVAL_TO_OBJECT( argv[2] ) ;
      ret = objToBson ( cx , objCond , &bsonCond ) ;
      VERIFY ( ret ) ;

      if ( 4 == argc )
      {
         objEndCond = SDB_JSVAL_TO_OBJECT( argv[3] ) ;
         ret = objToBson( cx, objEndCond, &bsonEndCond ) ;
         VERIFY ( ret ) ;
      }
   }
   else
   {
      REPORT( FALSE, "SdbCollection.split(): wrong argument[%d]", 3 ) ;
   }

   ret = JS_ConvertArguments ( cx , argc , argv , "SS/" ,
                               &strSource , &strTarget ) ;
   REPORT ( ret , "SdbCollection.split(): wrong arguments" ) ;

   source = (CHAR *) JS_EncodeString ( cx , strSource ) ;
   VERIFY ( source ) ;

   target = (CHAR *) JS_EncodeString ( cx, strTarget ) ;
   VERIFY ( target ) ;

   if ( bsonCond )
   {
      rc = sdbSplitCollection ( *collection , source, target,
                                bsonCond, bsonEndCond );
   }
   else
   {
      if ( percent <= 0.0 || percent > 100.0 )
      {
         REPORT( FALSE, "SdbCollection.split(): wrong argument(percent)" ) ;
      }
      rc = sdbSplitCollectionByPercent( *collection, source, target, percent ) ;
   }
   REPORT_RC ( SDB_OK == rc , "SdbCollection.split()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done :
   SAFE_JS_FREE ( cx, source ) ;
   SAFE_JS_FREE ( cx, target ) ;
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE( bsonEndCond ) ;
   PD_TRACE_EXIT ( SDB_COLL_SPLIT );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection.split(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_SPLIT_ASYNC, "collection_split_async" )
static JSBool collection_split_async ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_SPLIT_ASYNC );
   sdbCollectionHandle *collection  = NULL ;
   JSBool               ret         = JS_TRUE ;
   INT32                rc          = SDB_OK ;
   JSString *           strSource   = NULL ;
   JSString *           strTarget   = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objEndCond  = NULL ;
   CHAR *               source      = NULL ;
   CHAR *               target      = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonEndCond = NULL ;
   FLOAT64              percent     = 0.0 ;
   SINT64               taskID      = 0 ;
   jsval                valTaskID   = JSVAL_VOID ;
   jsval*               argv        = JS_ARGV( cx, vp ) ;


   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.splitAsync(): no collection handle" ) ;

   REPORT( argc >= 3, "SdbCollection.splitAsync(): wrong argument number" ) ;

   if ( JSVAL_IS_INT( argv[2] ) )
   {
      percent = (FLOAT64)JSVAL_TO_INT( argv[2] ) ;
      REPORT( argc == 3, "SdbCollection.splitAsync(): invalid argument" ) ;
   }
   else if ( JSVAL_IS_DOUBLE( argv[2] ) )
   {
      percent = (FLOAT64)JSVAL_TO_DOUBLE( argv[2] ) ;
      REPORT( argc == 3, "SdbCollection.splitAsync(): invalid argument" ) ;
   }
   else if ( SDB_JSVAL_IS_OBJECT( argv[2] ) )
   {
      objCond = SDB_JSVAL_TO_OBJECT( argv[2] ) ;
      ret = objToBson ( cx , objCond , &bsonCond ) ;
      VERIFY ( ret ) ;

      if ( 4 == argc )
      {
         objEndCond = SDB_JSVAL_TO_OBJECT( argv[3] ) ;
         ret = objToBson( cx, objEndCond, &bsonEndCond ) ;
         VERIFY ( ret ) ;
      }
   }
   else
   {
      REPORT( FALSE, "SdbCollection.splitAsync(): wrong argument[%d]", 3 ) ;
   }

   ret = JS_ConvertArguments ( cx , argc , argv , "SS/" ,
                               &strSource , &strTarget ) ;
   REPORT ( ret , "SdbCollection.splitAsync(): wrong arguments" ) ;

   source = (CHAR *) JS_EncodeString ( cx , strSource ) ;
   VERIFY ( source ) ;

   target = (CHAR *) JS_EncodeString ( cx, strTarget ) ;
   VERIFY ( target ) ;

   if ( bsonCond )
   {
      rc = sdbSplitCLAsync ( *collection , source, target,
                             bsonCond, bsonEndCond, &taskID );
   }
   else
   {
      if ( percent <= 0.0 || percent > 100.0 )
      {
         REPORT( FALSE, "SdbCollection.splitAsync(): wrong argument(percent)" ) ;
      }
      rc = sdbSplitCLByPercentAsync( *collection, source, target,
                                     percent, &taskID ) ;
   }
   REPORT_RC ( SDB_OK == rc , "SdbCollection.splitAsync()" , rc ) ;

   ret = JS_NewNumberValue ( cx, taskID, &valTaskID ) ;
   VERIFY ( ret ) ;

   JS_SET_RVAL ( cx , vp , valTaskID ) ;
done :
   SAFE_JS_FREE ( cx, source ) ;
   SAFE_JS_FREE ( cx, target ) ;
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE( bsonEndCond ) ;
   PD_TRACE_EXIT ( SDB_COLL_SPLIT_ASYNC );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection.splitAsync(): false" ) ;
   goto done ;
}

static JSBool collection_alter ( JSContext *cx , uintN argc , jsval *vp )
{
   sdbCollectionHandle *collection  = NULL ;
   JSBool               ret         = JS_TRUE ;
   INT32                rc          = SDB_OK ;
   JSObject *           objModify   = NULL ;
   bson *               bsonModify  = NULL ;


   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.alter(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "o" ,
                               &objModify ) ;
   REPORT ( ret , "SdbCollection.alter(): wrong arguments" ) ;

   ret = objToBson ( cx, objModify, &bsonModify ) ;
   VERIFY ( ret ) ;
   rc = sdbAlterCollection ( *collection, bsonModify ) ;
   REPORT_RC ( SDB_OK == rc, "SdbCollection.alter()", rc ) ;

   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done :
   SAFE_BSON_DISPOSE ( bsonModify ) ;
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection.alter(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_CRT_INX, "collection_create_index" )
static JSBool collection_create_index ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_CRT_INX );
   sdbCollectionHandle *collection  = NULL ;
   JSBool               ret         = JS_TRUE ;
   INT32                rc          = SDB_OK ;
   jsval *              argv        = JS_ARGV ( cx , vp ) ;
   JSObject *           objDef      = NULL ;
   bson *               bsonDef     = NULL ;
   JSString *           strName     = NULL ;
   CHAR *               name        = NULL ;
   JSBool               unique      = JS_FALSE ;
   JSBool               enforced    = JS_FALSE ;

   REPORT ( argc >= 2 ,
            "SdbCollection.createIndex(): need at least two arguments" ) ;

   REPORT ( ! JSVAL_IS_PRIMITIVE ( argv[1] ) ,
            "SdbCollection.createIndex(): 2nd argument is not valid object" ) ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.createIndex(): no collection handle" ) ;

   strName = JS_ValueToString ( cx , argv[0] ) ;
   VERIFY ( strName ) ;
   argv[0] = STRING_TO_JSVAL ( strName ) ;

   name = JS_EncodeString ( cx , strName ) ;
   VERIFY ( name ) ;

   objDef = JSVAL_TO_OBJECT ( argv[1] ) ;
   VERIFY ( objDef ) ;
   VERIFY ( objToBson ( cx , objDef , &bsonDef ) ) ;

   if ( argc >= 3 )
   {
      VERIFY ( JS_ValueToBoolean ( cx , argv[2] , &unique ) ) ;
   }
   if ( argc >= 4 )
   {
      VERIFY ( JS_ValueToBoolean ( cx, argv[3], &enforced ) ) ;
   }

   rc = sdbCreateIndex ( *collection , bsonDef , name , unique ? TRUE : FALSE,
                         enforced ? TRUE : FALSE );
   REPORT_RC ( SDB_OK == rc , "SdbCollection.createIndex()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_BSON_DISPOSE ( bsonDef ) ;
   SAFE_JS_FREE ( cx , name ) ;
   PD_TRACE_EXIT ( SDB_COLL_CRT_INX );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection.createIndex(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_GET_INX, "collection_get_indexes" )
static JSBool collection_get_indexes ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_GET_INX );
   sdbCollectionHandle *collection  = NULL ;
   sdbCursorHandle *    cursor      = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSObject *           objCursor   = NULL ;
   JSString *           strName     = NULL ;
   CHAR *               name        = NULL ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection._getIndexes(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "/S" , &strName ) ;
   REPORT ( ret , "SdbCollection._getIndexes(): invalid arguments" ) ;

   if ( strName )
   {
      name = (CHAR *) JS_EncodeString ( cx , strName ) ;
      VERIFY ( name ) ;
   }

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   rc = sdbGetIndexes ( *collection , name , cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc  ,
               "SdbCollection._getIndexes()" , rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;

done :
   SAFE_JS_FREE ( cx , name ) ;
   PD_TRACE_EXIT ( SDB_COLL_GET_INX );
   return ret ;
error :
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "SdbCollection._getIndexes(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_DROP_INX, "collection_drop_index" )
static JSBool collection_drop_index ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_DROP_INX );
   sdbCollectionHandle *collection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSString *           strName     = NULL ;
   CHAR *               name        = NULL ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.dropIndex(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strName ) ;
   REPORT ( ret , "SdbCollection.dropIndex(): invalid arguments" ) ;

   VERIFY ( strName ) ;
   name = (CHAR *) JS_EncodeString ( cx , strName ) ;
   VERIFY ( name ) ;

   rc = sdbDropIndex ( *collection , name ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCollection.dropIndex()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_JS_FREE ( cx , name ) ;
   PD_TRACE_EXIT ( SDB_COLL_DROP_INX );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection.dropIndex(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_BULK_INSERT, "collection_bulk_insert" )
static JSBool collection_bulk_insert ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_BULK_INSERT );
   sdbCollectionHandle *   collection  = NULL ;
   jsval *                 argv        = JS_ARGV ( cx , vp ) ;
   INT32                   rc          = SDB_OK ;
   JSBool                  ret         = JS_TRUE ;
   JSObject *              objArray    = NULL ;
   jsval                   valElem     = JSVAL_VOID ;
   JSObject *              objElem     = NULL ;
   jsuint                  len         = 0 ;
   jsuint                  i           = 0 ;
   bson **                 bsonArray   = NULL ;
   jsint                   insertFlags = 0 ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection._bulkInsert(): no collection handle" ) ;

   REPORT ( argc >= 1 && !JSVAL_IS_PRIMITIVE(argv[0])
               && JS_IsArrayObject ( cx , JSVAL_TO_OBJECT ( argv[0] ) ) ,
            "SdbCollection._bulkInsert(): 1st param should be array of objs" ) ;

   objArray = JSVAL_TO_OBJECT ( argv[0] ) ;
   VERIFY ( JS_GetArrayLength ( cx , objArray , &len ) ) ;
   VERIFY ( len > 0 ) ;

   bsonArray = (bson **) JS_malloc ( cx , len * sizeof (bson *) ) ;
   VERIFY ( bsonArray ) ;
   ossMemset ( bsonArray , 0 , len * sizeof (bson *) ) ;

   for ( i = 0 ; i < len ; i++ )
   {
      VERIFY ( JS_GetElement ( cx , objArray , i , &valElem ) ) ;
      objElem = JSVAL_TO_OBJECT ( valElem ) ;
      VERIFY ( objToBson ( cx , objElem , &bsonArray[i] ) ) ;
   }

   if ( argc >= 2 )
   {
      REPORT ( JSVAL_IS_INT ( argv[1] ) ,
               "SdbCollection._bulkInsert(): 2nd param should be bit flags" ) ;
      insertFlags = JSVAL_TO_INT ( argv[1] ) ;
   }

   rc = sdbBulkInsert ( *collection , insertFlags , bsonArray , len ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCollection._bulkInsert()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   if ( bsonArray )
   {
      for ( i = 0 ; i < len ; i++ )
         SAFE_BSON_DISPOSE ( bsonArray[i] ) ;
      SAFE_JS_FREE ( cx , bsonArray ) ;
   }
   PD_TRACE_EXIT ( SDB_COLL_BULK_INSERT );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection._bulkInsert(): false" ) ;
   goto done ;
}
/*
// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_RENM, "collection_rename" )
static JSBool collection_rename ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_RENM );
   sdbCollectionHandle *collection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSString *           strName     = NULL ;
   CHAR *               name        = NULL ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection._rename(): no collection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strName ) ;
   REPORT ( ret , "SdbCollection._rename(): invalid arguments" ) ;

   VERIFY ( strName ) ;
   name = (CHAR *) JS_EncodeString ( cx , strName ) ;
   VERIFY ( name ) ;

   rc = sdbRenameCollection ( *collection , name ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCollection._rename()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_JS_FREE ( cx , name ) ;
   PD_TRACE_EXIT ( SDB_COLL_RENM );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection._rename(): false" ) ;
   goto done ;
}*/

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_AGGR, "collection_aggr" )
static JSBool collection_aggr ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_AGGR );
   sdbCollectionHandle *collection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   jsval *              argv        = JS_ARGV ( cx , vp ) ;
   bson **              bsonArray   = NULL ;
   jsuint               i           = 0 ;
   JSObject *           objElem     = NULL ;
   sdbCursorHandle *    cursor      = NULL ;
   JSObject *           objCursor   = NULL ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.aggregate(): no collection handle" ) ;

   REPORT ( argc >= 1, "param can't be empty " );

   bsonArray = (bson **)JS_malloc( cx, argc * sizeof(bson *) );
   ossMemset( bsonArray, 0, argc * sizeof(bson *) );

   for ( i = 0 ; i < argc ; i++ )
   {
      if ( JSVAL_IS_OBJECT( argv[i] ) )
      {
         objElem = JSVAL_TO_OBJECT( argv[i] );
         if ( objElem )
         {
            VERIFY( objToBson( cx, objElem, &bsonArray[i] ) );
         }
      }
      else
      {
         REPORT ( FALSE , "SdbCollection.aggregation(): wrong arguments" ) ;
      }
      objElem = NULL;
   }

   cursor = (sdbCursorHandle *) JS_malloc ( cx, sizeof( sdbCursorHandle ) );
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   rc = sdbAggregate( *collection, bsonArray, argc, cursor );

   REPORT_RC( SDB_OK == rc || SDB_DMS_EOC == rc ,
            "SdbCollection.aggregate()", rc );
   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE( cx, cursor );
      cursor = NULL ;
   }
   objCursor = JS_NewObject( cx, &cursor_class, NULL , NULL );
   VERIFY( objCursor );

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objCursor ) );
   VERIFY( JS_SetPrivate( cx, objCursor, cursor ) );
done :
   if ( bsonArray )
   {
      for ( i = 0 ; i < argc ; i++ )
      {
         SAFE_BSON_DISPOSE( bsonArray[i] );
      }
      SAFE_JS_FREE( cx, bsonArray );
   }
   PD_TRACE_EXIT ( SDB_COLL_AGGR );
   return ret ;
error :
   SAFE_RELEASE_CURSOR( cursor );
   SAFE_JS_FREE( cx, cursor );
   TRY_REPORT ( cx , "SdbColleciton.aggregate(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_ATTACHCOLLECTION, "collection_attachCollection" )
static JSBool collection_attachCollection ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_ATTACHCOLLECTION );
   sdbCollectionHandle *collection  = NULL ;
   JSBool ret                       = JS_TRUE ;
   INT32 rc                         = SDB_OK ;
   JSString *JSStrName              = NULL ;
   CHAR *subCLName                  = NULL ;
   JSObject *objDef                 = NULL ;
   bson *bsonDef                    = NULL ;
   jsval *argv                      = JS_ARGV ( cx , vp ) ;

   collection = (sdbCollectionHandle *)
                 JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.attachCL(): no collection handle" ) ;
   REPORT ( 2 == argc ,
            "SdbCollection.attachCL(): need two arguments" ) ;
   if ( JSVAL_IS_STRING( argv[0] ) )
   {
      JSStrName = JS_ValueToString ( cx , argv[0] ) ;
      VERIFY ( JSStrName ) ;
      subCLName = JS_EncodeString ( cx , JSStrName ) ;
      VERIFY ( subCLName ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.attachCL(): wrong arguments" ) ;
   }
   if ( JSVAL_IS_OBJECT( argv[1] ) )
   {
      objDef = JSVAL_TO_OBJECT ( argv[1] ) ;
      VERIFY ( objDef ) ;
      VERIFY ( objToBson( cx, objDef, &bsonDef ) ) ;
   }
   else
   {
      REPORT ( FALSE , "SdbCollection.attachCL(): wrong arguments" ) ;
   }
   rc = sdbAttachCollection ( *collection , subCLName, bsonDef );
   REPORT_RC ( SDB_OK == rc , "SdbCollection.attachCL()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_JS_FREE ( cx , subCLName ) ;
   SAFE_BSON_DISPOSE ( bsonDef ) ;
   PD_TRACE_EXIT ( SDB_COLL_ATTACHCOLLECTION );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection.attachCL(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_COLL_DETACHCOLLECTION, "collection_detachCollection" )
static JSBool collection_detachCollection ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_COLL_DETACHCOLLECTION );
   sdbCollectionHandle *collection  = NULL ;
   JSBool               ret         = JS_TRUE ;
   INT32                rc          = SDB_OK ;
   jsval *              argv        = JS_ARGV ( cx , vp ) ;
   JSString *           strName     = NULL ;
   CHAR *               name        = NULL ;

   REPORT ( 1 == argc ,
            "SdbCollection.detachCL(): need one arguments" ) ;

   collection = (sdbCollectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( collection , "SdbCollection.detachCL(): no collection handle" ) ;

   strName = JS_ValueToString ( cx , argv[0] ) ;
   VERIFY ( strName ) ;
   argv[0] = STRING_TO_JSVAL ( strName ) ;

   name = JS_EncodeString ( cx , strName ) ;
   VERIFY ( name ) ;

   rc = sdbDetachCollection( *collection , name );
   REPORT_RC ( SDB_OK == rc , "SdbCollection.detachCL()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_JS_FREE ( cx , name ) ;
   PD_TRACE_EXIT ( SDB_COLL_DETACHCOLLECTION );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCollection.detachCL(): false" ) ;
   goto done ;
}

static JSFunctionSpec collection_functions[] = {
    JS_FS ( "rawFind" , collection_raw_find , 0 , 0 ) ,
    JS_FS ( "_insert" , collection_insert , 1 , 0 ) ,
    JS_FS ( "update" , collection_update , 1 , 0 ) ,
    JS_FS ( "upsert" , collection_upsert , 1 , 0 ) ,
    JS_FS ( "remove" , collection_remove , 0 , 0 ) ,
    JS_FS ( "_count" , collection_count , 0 , 0 ) ,
    JS_FS ( "createIndex" , collection_create_index , 2 , 0 ) ,
    JS_FS ( "_getIndexes" , collection_get_indexes , 1 , 0 ) ,
    JS_FS ( "dropIndex" , collection_drop_index , 1 , 0 ) ,
    JS_FS ( "_bulkInsert" , collection_bulk_insert , 1 , 0 ) ,
    JS_FS ( "split", collection_split, 3, 0 ),
    JS_FS ( "splitAsync", collection_split_async, 3, 0 ),
    JS_FS ( "aggregate" , collection_aggr , 1 , 0 ) ,
    JS_FS ( "alter", collection_alter, 1, 0 ),
    JS_FS ( "attachCL", collection_attachCollection, 2, 0 ) ,
    JS_FS ( "detachCL", collection_detachCollection, 1, 0 ) ,
    JS_FS ( "explain", collection_explain, 1, 0 ) ,
    JS_FS ( "putLob", collection_put_lob, 1, 0 ) ,
    JS_FS ( "getLob", collection_get_lob, 1, 0 ) ,
    JS_FS ( "deleteLob", collection_delete_lob, 1, 0 ) ,
    JS_FS ( "listLobs", collection_list_lobs, 1, 0 ) ,
    JS_FS ( "listLobPieces", collection_list_lob_pieces, 1, 0 ) ,
    JS_FS_END
} ;



// PD_TRACE_DECLARE_FUNCTION ( SDB_QUERY_RESV, "query_resolve" )
static JSBool query_resolve ( JSContext *cx , JSObject *obj , jsid id ,
                              uintN flags , JSObject ** objp )
{
   PD_TRACE_ENTRY ( SDB_QUERY_RESV );
   BOOLEAN  ret   = JS_TRUE ;
   jsval    val   = JSVAL_VOID ;
   jsval    valID = JSVAL_VOID ;

   ret = JS_IdToValue ( cx , id , &valID ) ;
   VERIFY ( ret ) ;

   if ( flags & JSRESOLVE_ASSIGNING )
      goto done ;

   if ( ! JSVAL_IS_INT ( valID ) )
      goto done ;

   ret = JS_CallFunctionName ( cx , obj , "arrayAccess" , 1 , &valID , &val ) ;
   VERIFY ( ret ) ;

   ret = JS_SetPropertyById ( cx , obj , id , &val ) ;
   VERIFY ( ret ) ;

   *objp = obj ;

done :
   PD_TRACE_EXIT ( SDB_QUERY_RESV );
   return ret ;
error :
   goto done ;
}

static JSClass query_class = {
   "SdbQuery" ,                                 // class name
   JSCLASS_NEW_RESOLVE |JSCLASS_HAS_PRIVATE ,   // flags
   JS_PropertyStub ,                            // addProperty
   JS_PropertyStub ,                            // delProperty
   JS_PropertyStub ,                            // getProperty
   JS_StrictPropertyStub ,                      // setProperty
   JS_EnumerateStub ,                           // enumerate
   (JSResolveOp) query_resolve ,                // resolve
   JS_ConvertStub ,                             // convert
   JS_FinalizeStub ,                            // finalize
   JSCLASS_NO_OPTIONAL_MEMBERS                  // optional members
};

// PD_TRACE_DECLARE_FUNCTION ( SDB_QUERY_CONSTRUCTOR, "query_constructor" )
static JSBool query_constructor ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_QUERY_CONSTRUCTOR );
   JSObject *objSort          = NULL ;
   JSObject *objHint          = NULL ;
   int32_t skip               = 0 ;
   int32_t limit              = -1 ;
   int32_t flags              = 0 ;
   JSObject *obj              = NULL ;
   jsval val                  = JSVAL_VOID ;
   JSBool ret                 = JS_TRUE ;
   jsval *argv                = JS_ARGV ( cx, vp ) ;

   obj = JS_NewObject ( cx , &query_class , NULL , NULL ) ;
   VERIFY ( obj ) ;
   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( obj ) ) ;
   if ( JSVAL_IS_OBJECT( argv[0] ) )
   {
      VERIFY ( JS_SetProperty ( cx , obj , "_collection" , &argv[0] ) ) ;
   }
   else
   {
      REPORT ( FALSE, "new SdbQuery(): failed to get collection handle" ) ;
   }
   if ( JSVAL_IS_OBJECT( argv[1] ) || JSVAL_IS_VOID( argv[1] ) ||
        JSVAL_IS_NULL( argv[1] ) )
   {
      VERIFY ( JS_SetProperty ( cx , obj , "_query" , &argv[1] ) ) ;
   }
   else
   {
      REPORT ( FALSE, "the 1st argument wrong" ) ;
   }
   if ( JSVAL_IS_OBJECT( argv[2] ) || JSVAL_IS_VOID( argv[2] ) ||
        JSVAL_IS_NULL( argv[2] ) )
   {
      VERIFY ( JS_SetProperty ( cx , obj , "_select" , &argv[2] ) ) ;
   }
   else
   {
      REPORT ( FALSE, "the 2nd argument wrong" ) ;
   }
   val = OBJECT_TO_JSVAL ( objSort ) ;
   VERIFY ( JS_SetProperty ( cx, obj, "_sort", &val ) ) ;
   val = OBJECT_TO_JSVAL ( objHint ) ;
   VERIFY ( JS_SetProperty ( cx , obj , "_hint" , &val ) ) ;
   val = INT_TO_JSVAL ( skip ) ;
   VERIFY ( JS_SetProperty ( cx , obj , "_skip" , &val ) ) ;
   val = INT_TO_JSVAL ( limit ) ;
   VERIFY ( JS_SetProperty ( cx , obj , "_limit" , &val ) ) ;
   val = INT_TO_JSVAL ( flags ) ;
   VERIFY ( JS_SetProperty ( cx , obj , "_flags" , &val ) ) ;
   val = JSVAL_NULL ;
   VERIFY ( JS_SetProperty ( cx , obj , "_cursor" , &val ) ) ;

done :
   PD_TRACE_EXIT ( SDB_QUERY_CONSTRUCTOR );
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RN_DESTRUCTOR, "rn_destructor" )
static void rn_destructor ( JSContext *cx, JSObject *obj )
{
   PD_TRACE_ENTRY ( SDB_RN_DESTRUCTOR );
   sdbNodeHandle *rn = (sdbNodeHandle *)
                              JS_GetPrivate ( cx, obj ) ;
   SAFE_RELEASE_RN ( rn ) ;
   SAFE_JS_FREE ( cx, rn ) ;
   JS_SetPrivate ( cx, obj, NULL ) ;
   PD_TRACE_EXIT ( SDB_RN_DESTRUCTOR );
}

static JSClass rn_class = {
    "SdbNode",        // class name
    JSCLASS_HAS_PRIVATE | JSCLASS_NEW_RESOLVE ,    // flags
    JS_PropertyStub,              // addProperty
    JS_PropertyStub,              // delProperty
    JS_PropertyStub,              // getProperty
    JS_StrictPropertyStub,        // setProperty
    JS_EnumerateStub,             // enumerate
    JS_ResolveStub,               // resolve
    JS_ConvertStub,               // convert
    rn_destructor,                // finalize
    JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
} ;

// PD_TRACE_DECLARE_FUNCTION ( SB_RG_DESTRUCTOR, "rg_destructor" )
static void rg_destructor ( JSContext *cx, JSObject *obj )
{
   PD_TRACE_ENTRY ( SB_RG_DESTRUCTOR );
   sdbReplicaGroupHandle *rg = (sdbReplicaGroupHandle *)
                               JS_GetPrivate ( cx, obj ) ;
   SAFE_RELEASE_RG ( rg ) ;
   SAFE_JS_FREE ( cx, rg ) ;
   JS_SetPrivate ( cx, obj, NULL ) ;
   PD_TRACE_EXIT ( SB_RG_DESTRUCTOR );
}

static JSClass rg_class = {
    "SdbReplicaGroup",        // class name
    JSCLASS_HAS_PRIVATE | JSCLASS_NEW_RESOLVE ,    // flags
    JS_PropertyStub,              // addProperty
    JS_PropertyStub,              // delProperty
    JS_PropertyStub,              // getProperty
    JS_StrictPropertyStub,        // setProperty
    JS_EnumerateStub,             // enumerate
    JS_ResolveStub,               // resolve
    JS_ConvertStub,               // convert
    rg_destructor,                // finalize
    JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
} ;

// PD_TRACE_DECLARE_FUNCTION ( SDB_RG_CONSTRUCTOR, "rg_constructor" )
static JSBool rg_constructor ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RG_CONSTRUCTOR );
   JSBool ret = JS_TRUE ;

   REPORT ( JS_FALSE, "use of new SdbReplicaGroup() is forbidden, you should use "
                      "other functions to produce a SdbReplicaGroup object" ) ;
done :
   PD_TRACE_EXIT ( SDB_RG_CONSTRUCTOR );
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RG_GET_MST, "rg_get_master" )
static JSBool rg_get_master ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RG_GET_MST );
   JSBool                  ret               = JS_TRUE ;
   jsval                   valRG             = JSVAL_VOID ;
   jsval                   valHostName       = JSVAL_VOID ;
   jsval                   valServiceName    = JSVAL_VOID ;
   jsval                   valNodeName       = JSVAL_VOID ;
   JSString              * strHostName       = NULL ;
   JSString              * strServiceName    = NULL ;
   JSString              * strNodeName       = NULL ;
   INT32                   rc                = SDB_OK ;
   sdbReplicaGroupHandle * rg                = NULL ;
   sdbNodeHandle  * rn                = NULL ;
   JSObject *              objRN             = NULL ;
   const CHAR *            hostName          = NULL ;
   const CHAR *            serviceName       = NULL ;
   const CHAR *            nodeName          = NULL ;
   INT32                   nodeID            = -1 ;
   jsval                   valNodeID         = JSVAL_VOID ;

   rg = (sdbReplicaGroupHandle *)JS_GetPrivate ( cx,
                                                 JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rg, "RG.getMaster(): no replica group handle" ) ;

   rn = (sdbNodeHandle *)
        JS_malloc ( cx, sizeof ( sdbNodeHandle ) ) ;
   VERIFY ( rn ) ;
   *rn = SDB_INVALID_HANDLE ;

   rc = sdbGetNodeMaster ( *rg, rn ) ;
   REPORT_RC ( SDB_OK == rc, "RG.getMaster()", rc ) ;

   rc = sdbGetNodeAddr ( *rn, &hostName, &serviceName,
                                &nodeName, &nodeID ) ;
   REPORT_RC ( SDB_OK == rc, "RG.getMaster()", rc ) ;

   objRN = JS_NewObject ( cx, &rn_class , 0 , 0 ) ;
   VERIFY ( objRN ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objRN ) ) ;
   VERIFY ( JS_SetPrivate ( cx , objRN , rn ) ) ;

   valRG = JS_THIS ( cx, vp ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_rg", &valRG ) ) ;

   strHostName = JS_NewStringCopyN ( cx , hostName , ossStrlen ( hostName ) ) ;
   VERIFY ( strHostName ) ;

   valHostName = STRING_TO_JSVAL ( strHostName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_hostname", &valHostName ) ) ;


   strServiceName = JS_NewStringCopyN ( cx , serviceName,
                                        ossStrlen ( serviceName ) ) ;
   VERIFY ( strServiceName ) ;

   valServiceName = STRING_TO_JSVAL ( strServiceName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_servicename", &valServiceName ) ) ;

   strNodeName = JS_NewStringCopyN ( cx, nodeName, ossStrlen ( nodeName ) ) ;
   VERIFY ( strNodeName ) ;

   valNodeName = STRING_TO_JSVAL ( strNodeName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_nodename", &valNodeName ) ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objRN ) ) ;

   valNodeID = INT_TO_JSVAL ( nodeID ) ;
   VERIFY ( JS_SetProperty ( cx , objRN , "_nodeid" , &valNodeID ) ) ;

done :
   PD_TRACE_EXIT ( SDB_RG_GET_MST );
   return ret ;
error :
   SAFE_RELEASE_RN ( rn ) ;
   SAFE_JS_FREE ( cx, rn ) ;
   TRY_REPORT ( cx , "SdbRL.getMaster(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RG_GET_SLAVE, "rg_get_slave" )
static JSBool rg_get_slave ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RG_GET_SLAVE );
   JSBool                  ret               = JS_TRUE ;
   jsval                   valRG             = JSVAL_VOID ;
   jsval                   valHostName       = JSVAL_VOID ;
   jsval                   valServiceName    = JSVAL_VOID ;
   jsval                   valNodeName       = JSVAL_VOID ;
   JSString              * strHostName       = NULL ;
   JSString              * strServiceName    = NULL ;
   JSString              * strNodeName       = NULL ;
   INT32                   rc                = SDB_OK ;
   sdbReplicaGroupHandle * rg                       = NULL ;
   sdbNodeHandle  * rn                       = NULL ;
   JSObject *              objRN             = NULL ;
   const CHAR *            hostName          = NULL ;
   const CHAR *            serviceName       = NULL ;
   const CHAR *            nodeName          = NULL ;
   INT32                   nodeID            = -1 ;
   jsval                   valNodeID         = JSVAL_VOID ;

   rg = (sdbReplicaGroupHandle *)JS_GetPrivate ( cx,
                                                 JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rg, "RG.getSlave(): no replica group handle" ) ;

   rn = (sdbNodeHandle *)
        JS_malloc ( cx, sizeof ( sdbNodeHandle ) ) ;
   VERIFY ( rn ) ;
   *rn = SDB_INVALID_HANDLE ;

   rc = sdbGetNodeSlave ( *rg, rn ) ;
   REPORT_RC ( SDB_OK == rc, "RG.getSlave()", rc ) ;

   rc = sdbGetNodeAddr ( *rn, &hostName, &serviceName,
                                &nodeName, &nodeID ) ;
   REPORT_RC ( SDB_OK == rc, "RG.getSlave()", rc ) ;

   objRN = JS_NewObject ( cx, &rn_class , 0 , 0 ) ;
   VERIFY ( objRN ) ;

   VERIFY ( JS_SetPrivate ( cx , objRN , rn ) ) ;

   valRG = JS_THIS ( cx, vp ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_rg", &valRG ) ) ;

   strHostName = JS_NewStringCopyN ( cx , hostName , ossStrlen ( hostName ) ) ;
   VERIFY ( strHostName ) ;

   valHostName = STRING_TO_JSVAL ( strHostName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_hostname", &valHostName ) ) ;


   strServiceName = JS_NewStringCopyN ( cx , serviceName,
                                        ossStrlen ( serviceName ) ) ;
   VERIFY ( strServiceName ) ;

   valServiceName = STRING_TO_JSVAL ( strServiceName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_servicename", &valServiceName ) ) ;

   strNodeName = JS_NewStringCopyN ( cx, nodeName, ossStrlen ( nodeName ) ) ;
   VERIFY ( strNodeName ) ;

   valNodeName = STRING_TO_JSVAL ( strNodeName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_nodename", &valNodeName ) ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objRN ) ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objRN ) ) ;

   valNodeID = INT_TO_JSVAL ( nodeID ) ;
   VERIFY ( JS_SetProperty ( cx , objRN , "_nodeid" , &valNodeID ) ) ;

done :
   PD_TRACE_EXIT ( SDB_RG_GET_SLAVE );
   return ret ;
error :
   SAFE_RELEASE_RN ( rn ) ;
   SAFE_JS_FREE ( cx, rn ) ;
   TRY_REPORT ( cx , "RG.getSlave(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RG_START, "rg_start" )
static JSBool rg_start ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RG_START );
   JSBool                 ret              = JS_TRUE ;
   INT32                  rc               = SDB_OK ;
   sdbReplicaGroupHandle *rg               = NULL ;

   rg = (sdbReplicaGroupHandle *) JS_GetPrivate ( cx,
         JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rg, "RG.start(): no replica group handle" ) ;

   rc = sdbStartReplicaGroup ( *rg ) ;
   REPORT_RC ( SDB_OK == rc, "RG.start()", rc ) ;

   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;

done :
   PD_TRACE_EXIT ( SDB_RG_START );
   return ret ;
error :
   TRY_REPORT ( cx, "RG.start(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RG_STOP, "rg_stop" )
static JSBool rg_stop ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RG_STOP );
   JSBool                 ret              = JS_TRUE ;
   INT32                  rc               = SDB_OK ;
   sdbReplicaGroupHandle *rg               = NULL ;

   rg = (sdbReplicaGroupHandle *) JS_GetPrivate ( cx,
         JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rg, "RG.stop(): no replica group handle" ) ;

   rc = sdbStopReplicaGroup ( *rg ) ;
   REPORT_RC ( SDB_OK == rc, "RG.stop()", rc ) ;

   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;

done :
   PD_TRACE_EXIT ( SDB_RG_STOP );
   return ret ;
error :
   TRY_REPORT ( cx, "RG.stop(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RG_CRT_NODE, "rg_create_node" )
static JSBool rg_create_node ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RG_CRT_NODE );
   sdbReplicaGroupHandle *rg                = NULL ;
   JSObject *             objConfig         = NULL ;
   JSString *             strHost           = NULL ;
   JSString *             strPort           = NULL ;
   JSString *             strDBPath         = NULL ;
   CHAR *                 host              = NULL ;
   CHAR *                 port              = NULL ;
   CHAR *                 dbPath            = NULL ;
   bson *                 bsonConfig        = NULL ;
   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;
   sdbNodeHandle  *rn                = NULL ;
   const CHAR *           hostName          = NULL ;
   const CHAR *           serviceName       = NULL ;
   const CHAR *           nodeName          = NULL ;
   jsval                  valRG             = JSVAL_VOID ;
   jsval                  valHostName       = JSVAL_VOID ;
   jsval                  valServiceName    = JSVAL_VOID ;
   jsval                  valNodeName       = JSVAL_VOID ;
   JSString *             strHostName       = NULL ;
   JSString *             strServiceName    = NULL ;
   JSString *             strNodeName       = NULL ;
   INT32                  nodeID            = -1 ;
   JSObject *             objRN             = NULL ;
   jsval                  valNodeID         = JSVAL_VOID ;

   rg  = (sdbReplicaGroupHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rg , "RG.createNode(): no replica group handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "SSS/o" ,
                               &strHost , &strPort , &strDBPath , &objConfig ) ;
   REPORT ( ret , "RG.createNode(): wrong arguments" ) ;

   if ( objConfig )
   {
      ret = objToBson ( cx , objConfig , &bsonConfig ) ;
      VERIFY ( ret ) ;
   }

   host = (CHAR *) JS_EncodeString ( cx , strHost ) ;
   VERIFY ( host ) ;

   port = (CHAR *) JS_EncodeString ( cx , strPort ) ;
   VERIFY ( port ) ;

   dbPath = (CHAR *) JS_EncodeString ( cx, strDBPath ) ;
   VERIFY ( dbPath ) ;

   rc = sdbCreateNode ( *rg, host, port, dbPath, bsonConfig ) ;
   REPORT_RC ( SDB_OK == rc, "RG.createNode()", rc ) ;

   rn = (sdbNodeHandle *)
        JS_malloc ( cx, sizeof ( sdbNodeHandle ) ) ;
   VERIFY ( rn ) ;
   *rn = SDB_INVALID_HANDLE ;

   rc = sdbGetNodeByHost ( *rg, host, port, rn ) ;
   REPORT_RC ( SDB_OK == rc, "RG.createNode()", rc ) ;

   rc = sdbGetNodeAddr ( *rn, &hostName, &serviceName,
                                &nodeName, &nodeID ) ;
   REPORT_RC ( SDB_OK == rc, "RG.createNode()", rc ) ;

   objRN = JS_NewObject ( cx, &rn_class, 0 , 0 ) ;
   VERIFY ( objRN ) ;

   VERIFY ( JS_SetPrivate ( cx, objRN, rn ) ) ;

   valRG = JS_THIS ( cx, vp ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_rg", &valRG ) ) ;

   strHostName = JS_NewStringCopyN ( cx, hostName, ossStrlen ( hostName ) ) ;
   VERIFY ( strHostName ) ;

   valHostName = STRING_TO_JSVAL ( strHostName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_hostname", &valHostName ) ) ;

   strServiceName = JS_NewStringCopyN ( cx, serviceName,
                                        ossStrlen ( serviceName ) ) ;
   VERIFY ( strServiceName ) ;

   valServiceName = STRING_TO_JSVAL ( strServiceName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_servicename", &valServiceName ) ) ;

   strNodeName = JS_NewStringCopyN ( cx, nodeName, ossStrlen ( nodeName ) ) ;
   VERIFY ( strNodeName ) ;

   valNodeName = STRING_TO_JSVAL ( strNodeName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_nodename", &valNodeName ) ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objRN ) ) ;

   valNodeID = INT_TO_JSVAL ( nodeID ) ;
   VERIFY ( JS_SetProperty ( cx , objRN , "_nodeid" , &valNodeID ) ) ;

done :
   SAFE_JS_FREE ( cx , host ) ;
   SAFE_JS_FREE ( cx , port ) ;
   SAFE_JS_FREE ( cx , dbPath ) ;
   SAFE_BSON_DISPOSE ( bsonConfig ) ;
   PD_TRACE_EXIT ( SDB_RG_CRT_NODE );
   return ret ;
error :
   SAFE_RELEASE_RN ( rn ) ;
   SAFE_JS_FREE ( cx, rn ) ;
   TRY_REPORT ( cx , "RG.createNode(): false" ) ;
   goto done ;

}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RG_RM_NODE, "rg_remove_node" )
static JSBool rg_remove_node ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY( SDB_RG_RM_NODE ) ;

   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;
   sdbReplicaGroupHandle *rg                = NULL ;
   JSObject *             objConfig         = NULL ;
   JSString *             strHost           = NULL ;
   JSString *             strPort           = NULL ;
   CHAR *                 host              = NULL ;
   CHAR *                 port              = NULL ;
   bson *                 bsonConfig        = NULL ;

   rg  = (sdbReplicaGroupHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rg , "RG.removeNode(): no replica group handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "SS/o" ,
                               &strHost , &strPort , &objConfig ) ;
   REPORT ( ret , "RG.createNode(): wrong arguments" ) ;

   if ( objConfig )
   {
      ret = objToBson ( cx , objConfig , &bsonConfig ) ;
      VERIFY ( ret ) ;
   }

   host = (CHAR *) JS_EncodeString ( cx , strHost ) ;
   VERIFY ( host ) ;

   port = (CHAR *) JS_EncodeString ( cx , strPort ) ;
   VERIFY ( port ) ;

   rc = sdbRemoveNode ( *rg, host, port, bsonConfig ) ;
   REPORT_RC ( SDB_OK == rc, "RG.removeNode()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;

done:
   SAFE_JS_FREE ( cx , host ) ;
   SAFE_JS_FREE ( cx , port ) ;
   SAFE_BSON_DISPOSE ( bsonConfig ) ;
   PD_TRACE_EXIT( SDB_RG_RM_NODE ) ;
   return ret ;
error:
   TRY_REPORT ( cx , "RG.removeNode(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_GET_NODE_AND_SETPROPERTY, "get_node_and_setproperty" )
JSBool get_node_and_setproperty( JSContext *cx, jsval *vp,
                                 jsval *valRG,
                                 sdbReplicaGroupHandle *rg,
                                 const CHAR *pHostName,
                                 const CHAR *pServiceName )
{
   PD_TRACE_ENTRY( SDB_GET_NODE_AND_SETPROPERTY ) ;
   JSBool ret = JS_TRUE ;
   INT32 rc = SDB_OK ;
   sdbNodeHandle *rn = NULL ;
   const CHAR *hostName = NULL ;
   const CHAR *serviceName = NULL ;
   const CHAR *nodeName = NULL ;
   INT32 nodeID = -1 ;
   JSObject *objRN = NULL ;
   jsval valHostName = JSVAL_VOID ;
   jsval valServiceName = JSVAL_VOID ;
   jsval valNodeName = JSVAL_VOID ;
   jsval valNodeID = JSVAL_VOID ;
   JSString *strHostName = NULL ;
   JSString *strServiceName = NULL ;
   JSString *strNodeName = NULL ;

   rn = (sdbNodeHandle *)
        JS_malloc ( cx, sizeof ( sdbNodeHandle ) ) ;
   VERIFY ( rn ) ;
   *rn = SDB_INVALID_HANDLE ;

   rc = sdbGetNodeByHost ( *rg, pHostName, pServiceName, rn ) ;
   REPORT_RC ( SDB_OK == rc, "get_node_and_setproperty()", rc ) ;

   rc = sdbGetNodeAddr ( *rn, &hostName, &serviceName,
                                &nodeName, &nodeID ) ;
   REPORT_RC ( SDB_OK == rc, "get_node_and_setproperty()", rc ) ;

   objRN = JS_NewObject ( cx, &rn_class , 0 , 0 ) ;
   VERIFY ( objRN ) ;

   VERIFY ( JS_SetPrivate ( cx , objRN , rn ) ) ;

   if ( NULL == valRG )
   {
      jsval valRG2 = JS_THIS ( cx, vp ) ;
      VERIFY ( JS_SetProperty ( cx, objRN, "_rg", &valRG2 ) ) ;
   }
   else
   {
      VERIFY ( JS_SetProperty ( cx, objRN, "_rg", valRG ) ) ;
   }

   strHostName = JS_NewStringCopyN ( cx , hostName , ossStrlen ( hostName ) ) ;
   VERIFY ( strHostName ) ;

   valHostName = STRING_TO_JSVAL ( strHostName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_hostname", &valHostName ) ) ;


   strServiceName = JS_NewStringCopyN ( cx , serviceName,
                                        ossStrlen ( serviceName ) ) ;
   VERIFY ( strServiceName ) ;

   valServiceName = STRING_TO_JSVAL ( strServiceName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_servicename", &valServiceName ) ) ;

   strNodeName = JS_NewStringCopyN ( cx, nodeName, ossStrlen ( nodeName ) ) ;
   VERIFY ( strNodeName ) ;

   valNodeName = STRING_TO_JSVAL ( strNodeName ) ;
   VERIFY ( JS_SetProperty ( cx, objRN, "_nodename", &valNodeName ) ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objRN ) ) ;

   valNodeID = INT_TO_JSVAL ( nodeID ) ;
   VERIFY ( JS_SetProperty ( cx , objRN , "_nodeid" , &valNodeID ) ) ;

done:
   PD_TRACE_EXIT( SDB_GET_NODE_AND_SETPROPERTY ) ;
   return ret ;
error:
   SAFE_RELEASE_RN ( rn ) ;
   SAFE_JS_FREE ( cx, rn ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RG_GET_NODE, "rg_get_node" )
static JSBool rg_get_node ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RG_GET_NODE );
   JSBool                  ret               = JS_TRUE ;
   JSString              * strHostName       = NULL ;
   JSString              * strServiceName    = NULL ;
   CHAR *                  pHostName         = NULL ;
   CHAR *                  pServiceName      = NULL ;
   CHAR *                  pNodeName         = NULL ;
   sdbReplicaGroupHandle  *rg                = NULL ;

   ret = JS_ConvertArguments ( cx, argc, JS_ARGV ( cx, vp ), "S/S",
                               &strHostName, &strServiceName ) ;
   REPORT ( ret, "RG.getNode(): wrong arguments" ) ;

   if ( !strServiceName )
   {
      INT32 serviceNameLen = 0 ;
      const CHAR *pSplit = NULL ;
      pNodeName = (CHAR*)JS_EncodeString ( cx, strHostName ) ;
      VERIFY ( pNodeName ) ;

      pSplit = ossStrchr ( pNodeName, NODE_NAME_SPLIT ) ;
      VERIFY ( pSplit ) ;

      serviceNameLen = ossStrlen ( pSplit+1 ) ;
      VERIFY ( serviceNameLen != 0 ) ;

      pHostName = (CHAR *) JS_malloc ( cx , pSplit-pNodeName+1 ) ;
      VERIFY ( pHostName ) ;

      ossStrncpy ( pHostName, pNodeName, pSplit-pNodeName ) ;
      pHostName[pSplit-pNodeName] = '\0' ;

      pServiceName = (CHAR *) JS_malloc ( cx, serviceNameLen+1 ) ;
      VERIFY ( pServiceName ) ;

      ossStrncpy ( pServiceName, pSplit+1, serviceNameLen ) ;
      pServiceName[serviceNameLen] = '\0' ;
   }
   else
   {
      pHostName = (CHAR*)JS_EncodeString ( cx, strHostName ) ;
      VERIFY ( pHostName ) ;

      pServiceName = (CHAR*)JS_EncodeString ( cx, strServiceName ) ;
      VERIFY ( pServiceName ) ;
   }

   rg = (sdbReplicaGroupHandle *)JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rg, "RG.getNode(): no replica group handle" ) ;
   ret = get_node_and_setproperty( cx, vp, NULL, rg,
                                   pHostName, pServiceName ) ;

done :
   SAFE_JS_FREE ( cx , pNodeName ) ;
   SAFE_JS_FREE ( cx , pHostName ) ;
   SAFE_JS_FREE ( cx , pServiceName ) ;
   PD_TRACE_EXIT ( SDB_RG_GET_NODE );
   return ret ;
error :
   TRY_REPORT ( cx , "RG.getNode(): false" ) ;
   goto done ;
}

static JSFunctionSpec rg_functions[] = {
   JS_FS ( "getMaster" , rg_get_master , 0 , 0 ) ,
   JS_FS ( "getSlave" , rg_get_slave , 0 , 0 ) ,
   JS_FS ( "start", rg_start, 0, 0 ),
   JS_FS ( "stop", rg_stop, 0, 0 ) ,
   JS_FS ( "createNode", rg_create_node, 0 , 0 ) ,
   JS_FS ( "removeNode", rg_remove_node, 0 , 0 ) ,
   JS_FS ( "getNode" , rg_get_node , 1 , 0 ) ,
   JS_FS_END
} ;


// PD_TRACE_DECLARE_FUNCTION ( SDB_CS_DESTRUCTOR, "cs_destructor" )
static void cs_destructor ( JSContext *cx , JSObject *obj )
{
   PD_TRACE_ENTRY ( SDB_CS_DESTRUCTOR );
   sdbCSHandle *cs = (sdbCSHandle *) JS_GetPrivate ( cx , obj ) ;
   SAFE_RELEASE_CS ( cs ) ;
   SAFE_JS_FREE ( cx , cs ) ;
   JS_SetPrivate ( cx , obj , NULL ) ;
   PD_TRACE_EXIT ( SDB_CS_DESTRUCTOR );
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_ISSPECCOLLNM, "isSpecialCollectionName" )
static JSBool isSpecialCollectionName ( const CHAR *name )
{
   PD_TRACE_ENTRY ( SDB_ISSPECCOLLNM );
   static CHAR *specialNames[] = { "getCL" ,
                                   "createCL" ,
                                   "dropCL" ,
                                   "toString" ,
                                   "help" } ;
   JSBool   in = JS_FALSE ;
   INT32    i  = 0 ;
   INT32    n  = sizeof ( specialNames ) / sizeof ( CHAR * ) ;

   SDB_ASSERT ( name , "invalid argument" ) ;

   if ( '_' == name[0] )
   {
      in = JS_TRUE ;
      goto done ;
   }

   for ( ; i < n ; i++ )
   {
      if ( ossStrcmp ( specialNames[i] , name ) == 0 )
      {
         in = JS_TRUE ;
         goto done ;
      }
   }
done :
   PD_TRACE_EXIT ( SDB_ISSPECCOLLNM );
   return in ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_CS_RESV, "cs_resolve" )
static JSBool cs_resolve ( JSContext *cx , JSObject *obj , jsid id ,
                           uintN flags , JSObject ** objp )
{
   PD_TRACE_ENTRY ( SDB_CS_RESV );
   JSBool                  ret            = JS_TRUE ;
   jsval                   valRes         = JSVAL_VOID ;
   jsval                   valID          = JSVAL_VOID ;
   CHAR *                  name           = NULL ;

   ret = JS_IdToValue ( cx , id , &valID ) ;
   VERIFY ( ret ) ;

   if ( flags & JSRESOLVE_ASSIGNING )
      goto done ;

   if ( ! JSVAL_IS_STRING ( valID ) )
      goto done ;

   name = (CHAR *) JS_EncodeString ( cx , JSVAL_TO_STRING ( valID ) ) ;
   VERIFY ( name ) ;

   if ( isSpecialCollectionName ( name ) ) goto done ;

   ret = JS_CallFunctionName ( cx , obj , "_resolveCL" ,
                               1 , &valID , &valRes ) ;
   VERIFY ( ret ) ;

   *objp = obj ;

done :
   SAFE_JS_FREE ( cx , name ) ;
   PD_TRACE_EXIT ( SDB_CS_RESV );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCS.resolve: false" ) ;
   goto done ;
}

static JSClass cs_class = {
   "SdbCS",         // class name
   JSCLASS_HAS_PRIVATE | JSCLASS_NEW_RESOLVE ,          // flags
   JS_PropertyStub,              // addProperty
   JS_PropertyStub,              // delProperty
   JS_PropertyStub,              // getProperty
   JS_StrictPropertyStub,        // setProperty
   JS_EnumerateStub,             // enumerate
   (JSResolveOp) cs_resolve ,               // resolve
   JS_ConvertStub,               // convert
   cs_destructor,            // finalize
   JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
};

static JSBool cs_constructor ( JSContext *cx , uintN argc , jsval *vp )
{
   JSBool ret = JS_TRUE ;

   REPORT ( JS_FALSE , "use of new SdbCS() is forbidden, you should use "
                       "other functions to produce a SdbCS object" ) ;

done :
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION( GET_CL_AND_SETPROPERTY, "get_cl_and_setproperty" )
static JSBool get_cl_and_setproperty( JSContext *cx, jsval *vp,
                                      jsval *vpcs,
                                      sdbCSHandle cs,
                                      const CHAR *clName,
                                      JSString *clStr )
{
   PD_TRACE_ENTRY( GET_CL_AND_SETPROPERTY ) ;
   JSBool ret = JS_TRUE ;
   INT32 rc = SDB_OK ;
   sdbCollectionHandle *collection = NULL ;
   JSObject *objCollection  = NULL ;
   jsval valCS = JSVAL_VOID ;
   jsval valCL = JSVAL_VOID ;
   jsval valName = JSVAL_VOID ;

   collection = (sdbCollectionHandle *)
      JS_malloc ( cx , sizeof ( sdbCollectionHandle ) ) ;
   VERIFY ( collection ) ;
   *collection = SDB_INVALID_HANDLE ;

   rc = sdbGetCollection1 ( cs , clName , collection ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCS.getCL()" , rc ) ;

   objCollection = JS_NewObject ( cx , &collection_class , 0 , 0 ) ;
   VERIFY ( objCollection ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCollection ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCollection , collection ) ) ;

   if ( NULL == vpcs )
   {
      valCS = JS_THIS ( cx , vp ) ;
      VERIFY ( JS_SetProperty ( cx , objCollection , "_cs" , &valCS ) ) ;
   }
   else
   {
      VERIFY ( JS_SetProperty ( cx , objCollection , "_cs" , vpcs ) ) ;
   }

   valName = STRING_TO_JSVAL ( clStr ) ;
   VERIFY ( JS_SetProperty ( cx , objCollection , "_name" , &valName ) ) ;

   valCL = OBJECT_TO_JSVAL ( objCollection ) ;
   VERIFY ( JS_SetProperty ( cx , JS_THIS_OBJECT ( cx , vp ) , clName , &valCL ) ) ;
done:
   PD_TRACE_EXIT( GET_CL_AND_SETPROPERTY ) ;
   return ret ;
error:
   SAFE_RELEASE_COLLECTION ( collection ) ;
   SAFE_JS_FREE ( cx , collection ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_CS_GET_CL, "cs_get_cl" )
static JSBool cs_get_cl ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_CS_GET_CL );
   JSBool                  ret            = JS_TRUE ;
   JSString *              strName        = NULL ;
   CHAR *                  name           = NULL ;
   sdbCSHandle *           cs             = NULL ;

   cs = (sdbCSHandle *) JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( cs , "SdbCS.getCL(): no collection space handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strName ) ;
   REPORT ( ret , "SdbCS.getCL(): invalid arguments" ) ;

   name = (CHAR *) JS_EncodeString ( cx , strName ) ;
   VERIFY ( name ) ;

   ret = get_cl_and_setproperty( cx, vp, NULL, *cs,
                                  name, strName ) ;
   VERIFY( ret ) ;
done :
   SAFE_JS_FREE ( cx , name ) ;
   PD_TRACE_EXIT ( SDB_CS_GET_CL );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCS.getCL(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SB_CS_CRT_CL, "cs_create_cl" )
static JSBool cs_create_cl ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SB_CS_CRT_CL );
   JSBool                  ret            = JS_TRUE ;
   JSString *              strCLName      = NULL ;
   CHAR *                  clName         = NULL ;
   INT32                   rc             = SDB_OK ;
   sdbCSHandle *           cs             = NULL ;
   sdbCollectionHandle *   collection     = NULL ;
   JSObject *              objOptions     = NULL ;
   bson *                  bsonOptions    = NULL ;
   JSObject *              objCL          = NULL ;
   jsval                   valName        = JSVAL_VOID ;
   jsval                   valCS          = JSVAL_VOID ;
   jsval                   valCL          = JSVAL_VOID ;

   cs = (sdbCSHandle *) JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( cs , "SdbCS.createCL(): no collection space handle" ) ;

   collection = (sdbCollectionHandle *) JS_malloc ( cx , sizeof ( sdbCollectionHandle ) ) ;
   VERIFY ( collection ) ;
   *collection = SDB_INVALID_HANDLE ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S/o" , &strCLName, &objOptions ) ;
   REPORT ( ret , "SdbCS.createCL(): invalid arguments" ) ;

   clName = (CHAR *) JS_EncodeString ( cx , strCLName ) ;
   VERIFY ( clName ) ;

   if ( objOptions )
   {
      VERIFY ( objToBson ( cx, objOptions, &bsonOptions ) ) ;
   }

   rc = sdbCreateCollection1 ( *cs , clName , bsonOptions , collection ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCS.createCL()" , rc ) ;

   rc = sdbGetCollection1 ( *cs , clName , collection ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCS.createCL()" , rc ) ;

   objCL = JS_NewObject ( cx , &collection_class , 0 , 0 ) ;
   VERIFY ( objCL ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCL ) ) ;

   valCS = JS_THIS ( cx, vp ) ;
   VERIFY ( JS_SetProperty ( cx , objCL , "_cs" , &valCS ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCL , collection ) ) ;

   valName = STRING_TO_JSVAL ( strCLName ) ;
   VERIFY ( JS_SetProperty ( cx , objCL , "_name" , &valName ) ) ;

   valCL = OBJECT_TO_JSVAL ( objCL ) ;
   VERIFY ( JS_SetProperty ( cx , JS_THIS_OBJECT ( cx , vp ) , clName, &valCL ) ) ;

done :
   SAFE_BSON_DISPOSE ( bsonOptions ) ;
   SAFE_JS_FREE ( cx , clName ) ;
   PD_TRACE_EXIT ( SB_CS_CRT_CL );
   return ret ;
error :
   SAFE_RELEASE_COLLECTION ( collection ) ;
   SAFE_JS_FREE ( cx, collection ) ;
   TRY_REPORT ( cx , "SdbCS.createCL(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_CS_DROP_CL, "cs_drop_cl" )
static JSBool cs_drop_cl ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_CS_DROP_CL );
   sdbCSHandle *  cs                = NULL ;
   JSString *     strCollectionName = NULL ;
   CHAR *         collectionName    = NULL ;
   INT32          rc                = SDB_OK ;
   JSBool         ret               = JS_TRUE ;
   JSBool         foundp            = JS_FALSE ;
   jsval          val               = JSVAL_VOID ;

   cs = (sdbCSHandle *) JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( cs , "SdbCS.dropCL(): no collection space handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strCollectionName ) ;
   REPORT ( ret , "SdbCS.dropCL(): wrong arguments" ) ;

   collectionName = (CHAR *) JS_EncodeString( cx , strCollectionName ) ;
   VERIFY ( collectionName ) ;

   if ( !JS_HasProperty ( cx , JS_THIS_OBJECT ( cx , vp ) , collectionName ,
                          &foundp ) )
   {
      ret = JS_FALSE ;
      goto error ;
   }

   rc = sdbDropCollection ( *cs , collectionName ) ;
   REPORT_RC ( SDB_OK == rc , "SdbCS.dropCL()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

   JS_DeleteProperty2 ( cx , JS_THIS_OBJECT ( cx , vp ) , collectionName ,
                        &val ) ;

done :
   SAFE_JS_FREE ( cx , collectionName ) ;
   PD_TRACE_EXIT ( SDB_CS_DROP_CL );
   return ret ;
error :
   TRY_REPORT ( cx , "SdbCS.dropCL(): false" ) ;
   goto done ;
}

static JSFunctionSpec cs_functions[] = {
   JS_FS ( "getCL" , cs_get_cl , 1 , 0 ) ,
   JS_FS ( "dropCL" , cs_drop_cl , 1 , 0 ) ,
   JS_FS ( "createCL" , cs_create_cl , 1 , 0 ) ,
   JS_FS_END
} ;

// PD_TRACE_DECLARE_FUNCTION ( SDB_DOMAIN_DESTRUCTOR, "domain_destructor" )
static void domain_destructor ( JSContext *cx, JSObject *obj )
{
   PD_TRACE_ENTRY ( SDB_DOMAIN_DESTRUCTOR );
   sdbDomainHandle *s = (sdbDomainHandle *)
         JS_GetPrivate ( cx, obj ) ;
   SAFE_RELEASE_DOMAIN ( s ) ;
   SAFE_JS_FREE( cx, s ) ;
   JS_SetPrivate ( cx, obj, NULL ) ;
   PD_TRACE_EXIT ( SDB_DOMAIN_DESTRUCTOR );
}
static JSClass domain_class = {
   "SdbDomain", // class name
   JSCLASS_HAS_PRIVATE | JSCLASS_NEW_RESOLVE ,   // flags
   JS_PropertyStub,              // addProperty
   JS_PropertyStub,              // delProperty
   JS_PropertyStub,              // getProperty
   JS_StrictPropertyStub,        // setProperty
   JS_EnumerateStub,             // enumerate
   JS_ResolveStub,               // resolve
   JS_ConvertStub,               // convert
   domain_destructor,            // finalize
   JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
} ;

static JSBool domain_constructor ( JSContext *cx, uintN argc, jsval *vp )
{
   JSBool ret = JS_TRUE ;
   REPORT ( JS_FALSE, "use of new SdbDomain() is orbidden, you should use "
                      "other functions to produce a SdbDomain object" ) ;
done :
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DOMAIN_ALTER, "domain_alter" )
static JSBool domain_alter( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY( SDB_DOMAIN_ALTER ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbDomainHandle *domain = NULL ;
   
   JSObject *argJSObj = NULL ;
   jsval *argv = NULL ;
   bson argObj ;
   bson_init( &argObj ) ;

   domain = ( sdbDomainHandle * )
            JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( domain, "Domain.alter(): no domain handle" ) ;

   REPORT ( argc >= 1,
            "Domain.alter(): need one argument" ) ;

   argv = JS_ARGV( cx, vp ) ;
   REPORT( JSVAL_IS_OBJECT( argv[0] ),
           "Domain.alter(): need a object argument") ;

   argJSObj = JSVAL_TO_OBJECT( argv[0] ) ;
   VERIFY( argJSObj ) ;

   {
   sptConvertor c( cx ) ;
   rc = c.toBson( argJSObj, &argObj ) ;
   VERIFY( SDB_OK == rc ) ;
   }

   rc = sdbAlterDomain( *domain, &argObj ) ;
   REPORT_RC ( SDB_OK == rc, "Domain.alter()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done:
   bson_destroy( &argObj ) ;
   PD_TRACE_EXIT( SDB_DOMAIN_ALTER ) ;
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DOMAIN_LIST_GROUP, "domain_list_group" )
static JSBool domain_list_group( JSContext *cx, uintN argc, jsval *vp )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY( SDB_DOMAIN_LIST_GROUP ) ;
   JSBool ret = JS_TRUE ;
   sdbDomainHandle *domain = NULL ;
   sdbCursorHandle *handle = NULL ;
   JSObject *objCursor = NULL ;

   domain = ( sdbDomainHandle * )
            JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( domain, "Domain.listGroups(): no domain handle" ) ;

   handle = (sdbCursorHandle *)
               JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( handle ) ;
   *handle  = SDB_INVALID_HANDLE ;

   rc = sdbListGroupsInDomain( *domain,
                               handle ) ;
   REPORT_RC ( SDB_OK == rc, "Domain.listGroups()", rc ) ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;
   VERIFY ( JS_SetPrivate ( cx , objCursor , handle ) ) ;
done:
   PD_TRACE_EXIT( SDB_DOMAIN_LIST_GROUP ) ;
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( handle ) ;
   SAFE_JS_FREE ( cx , handle ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DOMAIN_LIST_CS, "domain_list_cs" )
static JSBool domain_list_cs( JSContext *cx, uintN argc, jsval *vp )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY( SDB_DOMAIN_LIST_CS ) ;
   JSBool ret = JS_TRUE ;
   sdbDomainHandle *domain = NULL ;
   sdbCursorHandle *handle = NULL ;
   JSObject *objCursor = NULL ;

   domain = ( sdbDomainHandle * )
            JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( domain, "Domain.listCollectionSpaces(): no domain handle" ) ;

   handle = (sdbCursorHandle *)
               JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( handle ) ;
   *handle  = SDB_INVALID_HANDLE ;

   rc = sdbListCollectionSpacesInDomain( *domain,
                                         handle ) ;
   REPORT_RC ( SDB_OK == rc, "Domain.listCollectionSpaces()", rc ) ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;
   VERIFY ( JS_SetPrivate ( cx , objCursor , handle ) ) ;
done:
   PD_TRACE_EXIT( SDB_DOMAIN_LIST_CS ) ;
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( handle ) ;
   SAFE_JS_FREE ( cx , handle ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_DOMAIN_LIST_CL, "domain_list_cl" )
static JSBool domain_list_cl( JSContext *cx, uintN argc, jsval *vp )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY( SDB_DOMAIN_LIST_CL ) ;
   JSBool ret = JS_TRUE ;
   sdbDomainHandle *domain = NULL ;
   sdbCursorHandle *handle = NULL ;
   JSObject *objCursor = NULL ;

   domain = ( sdbDomainHandle * )
            JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( domain, "Domain.listCollections(): no domain handle" ) ;

   handle = (sdbCursorHandle *)
               JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( handle ) ;
   *handle  = SDB_INVALID_HANDLE ;

   rc = sdbListCollectionsInDomain( *domain,
                                    handle ) ;
   REPORT_RC ( SDB_OK == rc, "Domain.listCollections()", rc ) ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;
   VERIFY ( JS_SetPrivate ( cx , objCursor , handle ) ) ;
done:
   PD_TRACE_EXIT( SDB_DOMAIN_LIST_CS ) ;
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( handle ) ;
   SAFE_JS_FREE ( cx , handle ) ;
   goto done ;
}

static JSFunctionSpec domain_functions[] = {
   JS_FS( "alter", domain_alter, 0, 0 ),
   JS_FS( "listCollectionSpaces", domain_list_cs, 0, 0 ),
   JS_FS( "listCollections", domain_list_cl, 0, 0 ),
   JS_FS( "listGroups", domain_list_group, 0, 0 ),
   JS_FS_END
} ;


// PD_TRACE_DECLARE_FUNCTION ( SDB_DESTRUCTOR, "sdb_destructor" )
static void sdb_destructor ( JSContext *cx , JSObject *obj )
{
   PD_TRACE_ENTRY ( SDB_DESTRUCTOR );
   sdbConnectionHandle *connection = NULL ;
   connection = (sdbConnectionHandle *) JS_GetPrivate ( cx , obj ) ;
   if ( connection )
   {
      /*
      UINT64 addr = 0 ;
      __sdbGetReserveSpace1 ( *connection, &addr ) ;
      void *p = (void*)addr ;
      JS_RemoveValueRoot ( cx, (jsval*)p ) ;
      SAFE_JS_FREE ( cx, p ) ; */
      sdbDisconnect ( *connection ) ;
      SAFE_RELEASE_CONNECTION ( connection ) ;
      SAFE_JS_FREE ( cx , connection ) ;
      JS_SetPrivate ( cx , obj , NULL ) ;
   }
   PD_TRACE_EXIT ( SDB_DESTRUCTOR );
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_ISSPECSNM, "isSpecialCSName" )
static JSBool isSpecialCSName ( const CHAR *name )
{
   PD_TRACE_ENTRY ( SDB_ISSPECSNM );
   static CHAR *specialNames[] = { "listCollectionSpaces" ,
                                   "listCollections" ,
                                   "listReplicaGroups",
                                   "getCS" ,
                                   "getRG" ,
                                   "createCS" ,
                                   "createRG",
                                   "removeRG",
                                   "createCataRG",
                                   "dropCS" ,
                                   "toString" ,
                                   "snapshot" ,
                                   "resetSnapshot" ,
                                   "list",
                                   "startRG",
                                   "createUsr",
                                   "dropUsr",
                                   "exec",
                                   "execUpdate",
                                   "traceOn",
                                   "traceResume",
                                   "traceOff",
                                   "traceStatus",
                                   "transBegin",
                                   "transCommit",
                                   "transRollback",
                                   "close",
                                   "flushConfigure",
                                   "createProcedure",
                                   "removeProcedure",
                                   "listProcedures",
                                   "createDomain",
                                   "dropDomain",
                                   "getDomain",
                                   "listDomains",
                                   "eval",
                                   "backupOffline",
                                   "listBackup",
                                   "removeBackup",
                                   "listTasks",
                                   "waitTasks",
                                   "cancelTask",
                                   "setSessionAttr",
                                   "msg",
                                   "invalidateCache",
                                   "forceSession",
                                   "help",
                                   "getCatalogRG",
                                   "removeCatalogRG",
                                   "createCoordRG",
                                   "removeCoordRG",
                                   "getCoordRG"
   };
   JSBool   in = JS_FALSE ;
   INT32    i  = 0 ;
   INT32    n  = sizeof ( specialNames ) / sizeof ( CHAR * ) ;

   SDB_ASSERT ( name , "invalid argument" ) ;

   if ( '_' == name[0] )
   {
      in = JS_TRUE ;
      goto done ;
   }

   for ( ; i < n ; i++ )
   {
      if ( ossStrcmp ( specialNames[i] , name ) == 0 )
      {
         in = JS_TRUE ;
         goto done ;
      }
   }

done :
   PD_TRACE_EXIT ( SDB_ISSPECSNM );
   return in ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_RESV, "sdb_resolve" )
static JSBool sdb_resolve ( JSContext *cx , JSObject *obj , jsid id ,
                            uintN flags , JSObject ** objp )
{
   PD_TRACE_ENTRY ( SDB_SDB_RESV );
   JSBool                  ret         = JS_TRUE ;
   jsval                   valRes      = JSVAL_VOID ;
   jsval                   valID       = JSVAL_VOID ;
   CHAR *                  name        = NULL ;

   ret = JS_IdToValue ( cx , id , &valID ) ;
   VERIFY ( ret ) ;

   if ( flags & JSRESOLVE_ASSIGNING )
      goto done ;

   if ( ! JSVAL_IS_STRING ( valID ) )
      goto done ;

   name = (CHAR *) JS_EncodeString ( cx , JSVAL_TO_STRING ( valID ) ) ;
   VERIFY ( name ) ;

   if ( isSpecialCSName ( name ) ) goto done ;

   ret = JS_CallFunctionName ( cx, obj , "_resolveCS" , 1 , &valID , &valRes ) ;
   VERIFY ( ret ) ;

   *objp = obj ;

done :
   SAFE_JS_FREE ( cx , name ) ;
   PD_TRACE_EXIT ( SDB_SDB_RESV );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.resolve: false" ) ;
   goto done ;
}

static JSClass sdb_class = {
   "Sdb",                                // class name
   JSCLASS_HAS_PRIVATE | JSCLASS_NEW_RESOLVE,   // flags
   JS_PropertyStub,                      // addProperty
   JS_PropertyStub,                      // delProperty
   JS_PropertyStub,                      // getProperty
   JS_StrictPropertyStub,                // setProperty
   JS_EnumerateStub,                     // enumerate
   (JSResolveOp) sdb_resolve ,           // resolve
   JS_ConvertStub,                       // convert
   sdb_destructor,                       // finalize
   JSCLASS_NO_OPTIONAL_MEMBERS           // optional members
};

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CONSTRUCTOR, "sdb_constructor" )
static JSBool sdb_constructor ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_CONSTRUCTOR );
   JSString *           strHost     = NULL ;
   JSString *           strPort     = NULL ;
   JSString *           strName     = NULL ;
   JSString *           strPwd      = NULL ;
   sdbConnectionHandle *connection  = NULL ;
   JSObject *           obj         = NULL ;
   CHAR *               host        = NULL ;
   CHAR *               port        = NULL ;
   CHAR *               pwd         = NULL ;
   CHAR *               name        = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   jsval                val         = JSVAL_VOID ;
#if defined (SDB_FMP)
   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "/SS" , &strName , &strPwd ) ;
#else
   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "/SSSS" , &strHost , &strPort ,
                               &strName , &strPwd ) ;
#endif
   REPORT ( ret , "new Sdb(): wrong arguments" ) ;

#if !defined (SDB_FMP)
   if ( !strHost )
   {
      INT32 hostNameLen = 0 ;
      INT32 portNameLen = 0 ;
      CHAR portBuffer[OSS_MAX_SERVICENAME + 1] = {0};
      hostNameLen = ossStrlen(SDB_DEF_COORD_NAME) ;
      host = ( CHAR* ) JS_malloc ( cx, hostNameLen + 1 ) ;
      VERIFY ( host ) ;
      ossStrncpy ( host, SDB_DEF_COORD_NAME, hostNameLen ) ;
      host[hostNameLen] = '\0' ;
      portNameLen = sprintf( portBuffer, "%d", SDB_DEF_COORD_PORT ) ;
      VERIFY ( portNameLen != 0 ) ;
      port = ( CHAR* ) JS_malloc ( cx, portNameLen + 1 ) ;
      VERIFY ( port ) ;
      ossStrncpy ( port, portBuffer, portNameLen ) ;
      port[portNameLen] = '\0' ;
   }
   else if( ! strPort )
   {
      INT32 portNameLen = 0 ;
      const CHAR *pNodename = NULL ;
      const CHAR *pSplit = NULL ;
      CHAR portBuffer[OSS_MAX_SERVICENAME + 1] = {0};
      pNodename = ( CHAR* ) JS_EncodeString( cx, strHost ) ;
      VERIFY ( pNodename ) ;

      pSplit = ossStrchr ( pNodename, NODE_NAME_SPLIT ) ;
      if ( NULL != pSplit )
      {
         portNameLen = ossStrlen ( pSplit+1 ) ;
         VERIFY ( portNameLen != 0 ) ;
   
         host = ( CHAR* ) JS_malloc ( cx, pSplit-pNodename+1 ) ;
         VERIFY ( host ) ;
   
         ossStrncpy ( host, pNodename, pSplit-pNodename ) ;
         host[pSplit-pNodename] = '\0' ;
   
         port = ( CHAR* ) JS_malloc ( cx, portNameLen+1 );
         VERIFY ( port ) ;
   
         ossStrncpy ( port, pSplit+1, portNameLen );
         port[portNameLen] = '\0' ;
      }
      else
      {
         host = (CHAR *) JS_EncodeString ( cx , strHost ) ;
         VERIFY ( host ) ;
         portNameLen = sprintf( portBuffer, "%d", SDB_DEF_COORD_PORT ) ;
         VERIFY ( portNameLen != 0 ) ;
         port = ( CHAR* ) JS_malloc ( cx, portNameLen + 1 ) ;
         VERIFY ( port ) ;
         ossStrncpy ( port, portBuffer, portNameLen ) ;
         port[portNameLen] = '\0' ;
      }
   }
   else
   {
      host = (CHAR *) JS_EncodeString ( cx , strHost ) ;
      VERIFY ( host ) ;

      port = (CHAR *) JS_EncodeString ( cx , strPort ) ;
      VERIFY ( port ) ;
   }
#else
   strHost = JS_NewStringCopyN( cx, FMP_COORD_HOST,
                                ossStrlen(FMP_COORD_HOST) ) ;
   VERIFY( strHost ) ;
   strPort = JS_NewStringCopyN( cx, FMP_COORD_SERVICE,
                                ossStrlen(FMP_COORD_SERVICE ) ) ;
   VERIFY( strPort ) ;

   host = FMP_COORD_HOST ;
   port = FMP_COORD_SERVICE ;
#endif

   connection = (sdbConnectionHandle *)
      JS_malloc ( cx , sizeof ( sdbConnectionHandle ) ) ;
   VERIFY ( connection ) ;
   *connection = SDB_INVALID_HANDLE ;
   if ( strName && strPwd )
   {
      name = (CHAR *) JS_EncodeString ( cx , strName ) ;
      VERIFY ( name ) ;

      pwd = (CHAR *) JS_EncodeString ( cx , strPwd ) ;
      VERIFY ( pwd ) ;

#if defined( SDB_FMP )
      g_disablePassEncode = FALSE ;
#endif // SDB_FMP
      rc = sdbConnect ( host , port , name , pwd , connection ) ;
      REPORT_RC ( SDB_OK == rc , "new Sdb()" , rc ) ;
   }
   else if ( strName && ! strPwd )
   {
      REPORT ( JS_FALSE , "you should input your password to connect engine!" ) ;
   }
   else
   {
#if defined( SDB_FMP )
      g_disablePassEncode = TRUE ;
      rc = sdbConnect ( host , port , g_UserName, g_Password, connection ) ;
#else
      rc = sdbConnect ( host , port , "", "", connection ) ;
#endif // SDB_FMP
      REPORT_RC ( SDB_OK == rc , "new Sdb()" , rc ) ;
   }
   obj = JS_NewObject ( cx , &sdb_class, NULL, NULL ) ;
   VERIFY ( obj ) ;
   /*
   jsval *pv = (jsval*)JS_malloc ( cx, sizeof(jsval) ) ;
   VERIFY ( pv ) ;
   *pv = OBJECT_TO_JSVAL ( obj ) ;
   JS_SET_RVAL ( cx , vp , *pv ) ;
   JS_AddValueRoot ( cx, pv ) ;
   __sdbSetReserveSpace1 ( *connection, (UINT64)pv ) ;
   *pv = 0 ;
   */
   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL(obj) ) ;
   ret = JS_SetPrivate ( cx , obj , connection ) ;
   VERIFY ( ret ) ;
   if( !strHost || !strPort )
   {
      strPort = JS_NewStringCopyN( cx, port,
                                   ossStrlen(port) ) ;
      strHost = JS_NewStringCopyN( cx, host,
                                   ossStrlen(host) ) ;
   }
   val = STRING_TO_JSVAL ( strHost ) ;
   VERIFY ( JS_SetProperty ( cx , obj , "_host" , &val ) ) ;
   val = STRING_TO_JSVAL ( strPort ) ;
   VERIFY ( JS_SetProperty ( cx , obj , "_port" , &val ) ) ;

done :
#if !defined (SDB_FMP)
   SAFE_JS_FREE ( cx , host ) ;
   SAFE_JS_FREE ( cx , port ) ;
#endif
   SAFE_JS_FREE ( cx , name ) ;
   SAFE_JS_FREE ( cx , pwd ) ;
   PD_TRACE_EXIT ( SDB_SDB_CONSTRUCTOR );
   return ret ;
error :
   SAFE_RELEASE_CONNECTION ( connection ) ;
   SAFE_JS_FREE ( cx , connection ) ;
   TRY_REPORT ( cx , "new Sdb(): false" ) ;
   goto done ;
}


static JSBool rn_constructor ( JSContext *cx, uintN argc, jsval *vp )
{
   JSBool ret = JS_TRUE ;

   REPORT ( JS_FALSE, "use of new SdbNode() is forbidden, you should use "
                      "other functions to produce a SdbNode object" ) ;
done :
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RN_CONNECT, "rn_connect" )
static JSBool rn_connect ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RN_CONNECT );
   JSBool ret                              = JS_TRUE ;
   sdbNodeHandle *rn                       = NULL ;
   sdbConnectionHandle     *connection     = NULL ;
   const CHAR              *host           = NULL ;
   const CHAR              *port           = NULL ;
   INT32                    rc             = SDB_OK ;
   JSObject                *obj            = NULL ;
   jsval                    valHostName    = JSVAL_VOID ;
   jsval                    valServiceName = JSVAL_VOID ;
   JSString                *strHostName    = NULL ;
   JSString                *strServiceName = NULL ;

   rn = (sdbNodeHandle *)JS_GetPrivate ( cx,
                                                JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rn, "SdbNode.connect(): no node handle" ) ;

   rc = sdbGetNodeAddr ( *rn, &host, &port, NULL, NULL ) ;
   REPORT_RC ( SDB_OK == rc, "SdbNode.connect()", rc ) ;

   connection = (sdbConnectionHandle *)
      JS_malloc ( cx, sizeof ( sdbConnectionHandle ) ) ;
   VERIFY ( connection ) ;
   *connection = SDB_INVALID_HANDLE ;

   rc = sdbConnect ( host, port, "", "", connection ) ;
   REPORT_RC ( SDB_OK == rc, "SdbNode.connect()", rc ) ;

   obj = JS_NewObject ( cx, &sdb_class , 0 , 0 ) ;
   VERIFY ( obj ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( obj ) ) ;

   ret = JS_SetPrivate ( cx, obj, connection ) ;
   VERIFY ( ret ) ;

   strHostName = JS_NewStringCopyN ( cx , host, ossStrlen ( host ) ) ;
   VERIFY ( strHostName ) ;
   valHostName = STRING_TO_JSVAL ( strHostName ) ;
   VERIFY ( JS_SetProperty ( cx, obj, "_host", &valHostName ) ) ;

   strServiceName = JS_NewStringCopyN ( cx , port, ossStrlen ( port ) ) ;
   VERIFY ( strServiceName ) ;
   valServiceName = STRING_TO_JSVAL ( strServiceName ) ;
   VERIFY ( JS_SetProperty ( cx, obj, "_port", &valServiceName ) ) ;

done :
   PD_TRACE_EXIT ( SDB_RN_CONNECT );
   return ret ;
error :
   SAFE_RELEASE_CONNECTION ( connection ) ;
   SAFE_JS_FREE ( cx, connection ) ;
   TRY_REPORT ( cx, "SdbNode.connect(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RN_START, "rn_start" )
static JSBool rn_start ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RN_START );
   JSBool                 ret              = JS_TRUE ;
   INT32                  rc               = SDB_OK ;
   sdbNodeHandle  *rn               = NULL ;

   rn = (sdbNodeHandle *) JS_GetPrivate ( cx,
         JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rn, "SdbNode.start(): no node handle" ) ;

   rc = sdbStartNode ( *rn ) ;
   REPORT_RC ( SDB_OK == rc, "SdbNode.start()", rc ) ;

   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;

done :
   PD_TRACE_EXIT ( SDB_RN_START );
   return ret ;
error :
   TRY_REPORT ( cx, "SdbNode.start(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_RN_STOP, "rn_stop" )
static JSBool rn_stop ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_RN_STOP );
   JSBool                 ret              = JS_TRUE ;
   INT32                  rc               = SDB_OK ;
   sdbNodeHandle  *rn               = NULL ;

   rn = (sdbNodeHandle *) JS_GetPrivate ( cx,
         JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( rn, "SdbNode.stop(): no node handle" ) ;

   rc = sdbStopNode ( *rn ) ;
   REPORT_RC ( SDB_OK == rc, "SdbNode.stop()", rc ) ;

   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;

done :
   PD_TRACE_EXIT ( SDB_RN_STOP );
   return ret ;
error :
   TRY_REPORT ( cx, "SdbNode.stop(): false" ) ;
   goto done ;
}

static JSFunctionSpec rn_functions[] = {
   JS_FS ( "connect" , rn_connect, 0, 0 ) ,
   JS_FS ( "start", rn_start, 0, 0 ) ,
   JS_FS ( "stop", rn_stop, 0, 0 ) ,
   JS_FS_END
} ;

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CRT_RG, "sdb_create_rg" )
static JSBool sdb_create_rg ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_CRT_RG );
   sdbConnectionHandle   *connection = NULL ;
   sdbReplicaGroupHandle *rg         = NULL ;
   JSString *             strRGName  = NULL ;
   CHAR *                 rgName     = NULL ;
   INT32                  rc         = SDB_OK ;
   JSBool                 ret        = JS_TRUE ;
   JSObject *             objRG      = NULL ;
   jsval                  valConn    = JSVAL_VOID ;
   jsval                  valName    = JSVAL_VOID ;

   rg = (sdbReplicaGroupHandle *)JS_malloc ( cx,
         sizeof ( sdbReplicaGroupHandle ) ) ;
   VERIFY ( rg ) ;
   *rg = SDB_INVALID_HANDLE ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.createRG: no connection handle" ) ;

   ret = JS_ConvertArguments ( cx, argc, JS_ARGV ( cx, vp ),
                               "S", &strRGName ) ;
   REPORT ( ret, "Sdb.createRG(): wrong arguments" ) ;

   rgName = (CHAR*)JS_EncodeString ( cx, strRGName ) ;
   VERIFY ( rgName ) ;

   rc = sdbCreateReplicaGroup( *connection, rgName, rg ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.createRG()", rc ) ;

   objRG = JS_NewObject ( cx, &rg_class, 0 , 0 ) ;
   VERIFY ( objRG ) ;

   valConn = JS_THIS ( cx, vp ) ;
   VERIFY ( JS_SetProperty ( cx, objRG, "_conn", &valConn ) ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objRG ) ) ;

   VERIFY ( JS_SetPrivate ( cx, objRG, rg ) ) ;

   strRGName = JS_NewStringCopyN ( cx, rgName, ossStrlen ( rgName ) ) ;
   VERIFY ( strRGName ) ;
   valName = STRING_TO_JSVAL ( strRGName ) ;
   VERIFY ( JS_SetProperty ( cx, objRG, "_name", &valName ) ) ;

done :
   SAFE_JS_FREE ( cx, rgName ) ;
   PD_TRACE_EXIT ( SDB_SDB_CRT_RG );
   return ret ;
error :
   SAFE_RELEASE_RG ( rg ) ;
   SAFE_JS_FREE ( cx, rg ) ;
   TRY_REPORT ( cx, "Sdb.createRG(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CREATE_DOMAIN, "sdb_create_domain" )
static JSBool sdb_create_domain ( JSContext *cx, uintN argc, jsval *vp )
{
   INT32                  rc          = SDB_OK ;
   sdbConnectionHandle   *connection  = NULL ;
   JSBool                 ret         = JS_TRUE ;
   jsval                 *argv        = JS_ARGV ( cx , vp ) ;
   JSString              *domainName  = NULL ;
   JSObject              *domainObj   = NULL ;
   jsuint                 groupListL  = 0 ;
   JSObject              *objDomain   = NULL ;
   CHAR                  *name        = NULL ;
   sdbDomainHandle       *domain      = NULL ;
   jsval                  valDomainN  = JSVAL_VOID ;
   bson                   bsonDef ;
   bson                   options ;

   PD_TRACE_ENTRY ( SDB_SDB_CREATE_DOMAIN ) ;
   bson_init ( &bsonDef ) ;
   bson_init ( &options ) ;
   REPORT ( argc >= 1,
            "Sdb.createDomain(): need at least one argument" ) ;
   connection = (sdbConnectionHandle *)
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.createDomain(): no connection handle" ) ;

   domainName = JS_ValueToString ( cx, argv[0] ) ;
   VERIFY ( domainName ) ;
   argv[0] = STRING_TO_JSVAL ( domainName ) ;

   name = JS_EncodeString ( cx, domainName ) ;
   VERIFY ( name ) ;

   if ( argc >= 2 )
   {
      domainObj = JSVAL_TO_OBJECT ( argv[1] ) ;
      VERIFY ( domainObj ) ;
      REPORT ( JS_IsArrayObject ( cx, domainObj ),
               "Sdb.createDomain(): second argument must be array of groups" ) ;
      VERIFY ( BSON_OK ==
               bson_append_start_array ( &bsonDef, FIELD_NAME_GROUPS ) ) ;
      VERIFY ( JS_GetArrayLength ( cx, domainObj, &groupListL ) ) ;
      for ( UINT32 i = 0; i < groupListL; ++i )
      {
         jsval v ;
         CHAR  buffer [ OSS_MAX_GROUPNAME_SIZE ] = {0} ;
         ossMemset ( buffer, 0, sizeof(buffer) ) ;
         JSString * groupName = NULL ;
         CHAR     * strGroupName = NULL ;
         VERIFY ( JS_GetElement ( cx, domainObj, (jsint)i, &v ) ) ;
         groupName = JS_ValueToString ( cx, v ) ;
         VERIFY ( groupName ) ;
         strGroupName = JS_EncodeString ( cx, groupName ) ;
         VERIFY ( strGroupName ) ;
         ossSnprintf ( buffer, sizeof(buffer), "%d", i ) ;
         bson_append_string ( &bsonDef, buffer, strGroupName ) ;
         SAFE_JS_FREE ( cx, strGroupName ) ;
      }
      VERIFY ( BSON_OK == bson_append_finish_array ( &bsonDef ) ) ;
   }

   if ( argc >=3 )
   {
      if ( JSVAL_IS_OBJECT( argv[2]) )
      {
         sptConvertor convertor( cx ) ;
         bson_iterator itr ;
         JSObject *optionsObj = JSVAL_TO_OBJECT( argv[2] ) ;         
         VERIFY ( optionsObj ) ;
         rc = convertor.toBson( optionsObj, &options ) ;
         VERIFY( SDB_OK == rc ) ;
         bson_iterator_init( &itr, &options ) ;
         while ( bson_iterator_more( &itr ) )
         {
            bson_iterator_next( &itr ) ;
            bson_append_element( &bsonDef, NULL, &itr ) ;
         }
      }
   }

   domain = (sdbDomainHandle *) JS_malloc ( cx, sizeof(sdbDomainHandle) ) ;
   VERIFY ( domain ) ;
   *domain = SDB_INVALID_HANDLE ;

   bson_finish ( &bsonDef ) ;
   rc = sdbCreateDomain ( *connection, name, &bsonDef, domain ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.createDomain()", rc ) ;
   objDomain = JS_NewObject ( cx, &domain_class, 0, 0 ) ;
   VERIFY ( objDomain ) ;
   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objDomain ) ) ;
   VERIFY ( JS_SetPrivate ( cx, objDomain, domain ) ) ;
   valDomainN = STRING_TO_JSVAL ( domainName ) ;
   VERIFY ( JS_SetProperty ( cx, objDomain, "_domainname", &valDomainN ) ) ;
done :
   SAFE_JS_FREE ( cx, name ) ;
   bson_destroy ( &bsonDef ) ;
   bson_destroy ( &options ) ;
   PD_TRACE_EXIT ( SDB_SDB_CREATE_DOMAIN ) ;
   return ret ;
error :
   SAFE_RELEASE_DOMAIN ( domain ) ;
   SAFE_JS_FREE ( cx, domain ) ;
   TRY_REPORT ( cx, "Sdb.createDomain(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_DROP_DOMAIN, "sdb_drop_domain" )
static JSBool sdb_drop_domain ( JSContext *cx, uintN argc, jsval *vp )
{
   INT32                  rc          = SDB_OK ;
   sdbConnectionHandle   *connection  = NULL ;
   JSBool                 ret         = JS_TRUE ;
   JSString              *domainName  = NULL ;
   CHAR                  *name        = NULL ;
   PD_TRACE_ENTRY ( SDB_SDB_DROP_DOMAIN ) ;
   connection = (sdbConnectionHandle *)
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.dropDoamin(): no connection handle" ) ;
   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &domainName ) ;
   REPORT ( ret && domainName, "Sdb.dropDoamin(): wrong arguments" ) ;
   name = JS_EncodeString ( cx, domainName ) ;
   VERIFY ( name ) ;
   rc = sdbDropDomain ( *connection, name ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.dropDomain()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done :
   PD_TRACE_EXIT ( SDB_SDB_DROP_DOMAIN ) ;
   SAFE_JS_FREE ( cx, name ) ;
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_GET_DOMAIN, "sdb_get_domain" )
static JSBool sdb_get_domain ( JSContext *cx, uintN argc, jsval *vp )
{
   INT32                  rc          = SDB_OK ;
   sdbConnectionHandle   *connection  = NULL ;
   JSBool                 ret         = JS_TRUE ;
   JSString              *domainName  = NULL ;
   CHAR                  *name        = NULL ;
   sdbDomainHandle       *domain      = NULL ;
   JSObject              *objDomain   = NULL ;
   jsval                  valDomainN  = JSVAL_VOID ;
   PD_TRACE_ENTRY ( SDB_SDB_DROP_DOMAIN ) ;
   connection = (sdbConnectionHandle *)
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.getDoamin(): no connection handle" ) ;
   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &domainName ) ;
   REPORT ( ret && domainName, "Sdb.getDoamin(): wrong arguments" ) ;
   name = JS_EncodeString ( cx, domainName ) ;
   VERIFY ( name ) ;

   domain = ( sdbDomainHandle * )JS_malloc ( cx, sizeof(sdbDomainHandle) ) ;
   VERIFY ( domain ) ;

   rc = sdbGetDomain ( *connection, name, domain ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.getDomain()", rc ) ;
   objDomain = JS_NewObject ( cx, &domain_class, 0, 0 ) ;
   VERIFY ( objDomain ) ;
   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objDomain ) ) ;
   VERIFY ( JS_SetPrivate ( cx, objDomain, domain ) ) ;
   valDomainN = STRING_TO_JSVAL ( domainName ) ;
   VERIFY ( JS_SetProperty ( cx, objDomain, "_domainname", &valDomainN ) ) ;
done :
   PD_TRACE_EXIT ( SDB_SDB_DROP_DOMAIN ) ;
   SAFE_JS_FREE ( cx, name ) ;
   return ret ;
error :
   SAFE_RELEASE_DOMAIN ( domain ) ;
   SAFE_JS_FREE( cx, domain ) ;
   TRY_REPORT ( cx, "Sdb.getDomain(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_LIST_DOMAINS, "sdb_list_domains" )
static JSBool sdb_list_domains ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_LIST_DOMAINS );
   sdbConnectionHandle *connection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSObject *           objCursor   = NULL ;
   sdbCursorHandle *    cursor      = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objSel      = NULL ;
   JSObject *           objOrder    = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonSel     = NULL ;
   bson *               bsonOrder   = NULL ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.listDomains(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "/ooo" ,
                               &objCond , &objSel , &objOrder ) ;
   REPORT ( ret , "Sdb.listDomains(): invalid arguments" ) ;

   if ( objCond )
   {
      VERIFY ( objToBson ( cx , objCond , &bsonCond ) ) ;
   }

   if ( objSel )
   {
      VERIFY ( objToBson ( cx , objSel , &bsonSel ) ) ;
   }

   if ( objOrder )
   {
      VERIFY ( objToBson ( cx , objOrder , &bsonOrder ) ) ;
   }

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   rc = sdbListDomains( *connection, bsonCond, bsonSel,
                         bsonOrder, cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc  , "Sdb.listDomains()", rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;

done :
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonSel ) ;
   SAFE_BSON_DISPOSE ( bsonOrder ) ;
   PD_TRACE_EXIT ( SDB_SDB_LIST_DOMAINS );
   return ret ;
error :
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "Sdb.listDomains(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CRT_PROCEDURE, "sdb_crt_procedure" )
static JSBool sdb_crt_procedure( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_CRT_PROCEDURE ) ;
   INT32 rc = SDB_OK ;
   sdbConnectionHandle   *connection  = NULL ;
   JSBool                 ret         = JS_TRUE ;
   JSObject *             objData     = NULL ;
   jsval                  funcvar     = JSVAL_VOID ;
   JSString              *funcjsstr   = NULL ;
   CHAR                  *funcstr     = NULL ;

   connection = (sdbConnectionHandle *)
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.createProcedure(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "o" , &objData) ;
   REPORT ( ret && objData , "Sdb.createProcedure(): wrong arguments" ) ;
   ret = JS_ObjectIsFunction( cx, objData ) ;
   REPORT ( ret, "Sdb.createProcedure(): wrong arguments") ;

   funcvar = OBJECT_TO_JSVAL( objData ) ;
   funcjsstr = JS_ValueToString( cx, funcvar ) ;
   funcstr = ( CHAR *) JS_EncodeString ( cx , funcjsstr ) ;
   if ( NULL == funcstr )
   {
      rc = SDB_SYS ;
      goto error ;
   }

   rc = sdbCrtJSProcedure( *connection, funcstr ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.createProcedure()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done:
   SAFE_JS_FREE( cx, funcstr ) ;
   PD_TRACE_EXIT ( SDB_SDB_CRT_PROCEDURE ) ;
   return ret ;
error:
   TRY_REPORT ( cx, "Sdb.createProcedure(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_RM_PROCEDURE, "sdb_rm_procedure" )
static JSBool sdb_rm_procedure( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY( SDB_SDB_RM_PROCEDURE ) ;
   INT32 rc = SDB_OK ;
   sdbConnectionHandle   *connection = NULL ;
   JSBool                 ret        = JS_TRUE ;
   JSString              *func       = NULL ;
   CHAR                  *strFunc    = NULL ;

   connection = (sdbConnectionHandle *)
   JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.removeProcedure(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &func ) ;
   REPORT ( ret, "Sdb.removeProcedure(): wrong arguments" ) ;

   strFunc = ( CHAR *) JS_EncodeString ( cx, func ) ;
   VERIFY( strFunc ) ;

   rc = sdbRmProcedure( *connection, strFunc ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.removeProcedure()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;

done:
   SAFE_JS_FREE( cx, strFunc ) ;
   PD_TRACE_EXIT( SDB_SDB_RM_PROCEDURE ) ;
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_LIST_PROCEDURES, "sdb_list_procedures" )
static JSBool sdb_list_procedures( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY( SDB_SDB_LIST_PROCEDURES ) ;
   sdbConnectionHandle *connection = NULL ;
   JSBool ret = JS_TRUE ;
   JSObject *condition = NULL ;
   bson *bs = NULL;
   INT32 rc = SDB_OK ;
   sdbCursorHandle *handle = NULL ;
   JSObject *objCursor = NULL ;

   connection = (sdbConnectionHandle *)
   JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.listProcedures(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "/o" , &condition ) ;
   REPORT ( ret, "Sdb.listProceduers(): wrong arguments" ) ;

   if ( NULL != condition )
   {
      VERIFY( objToBson( cx, condition, &bs ) ) ;
   }

   handle = (sdbCursorHandle *)
               JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( handle ) ;
   *handle  = SDB_INVALID_HANDLE ;

   rc = sdbListProcedures( *connection, bs, handle ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.listProcedures()", rc ) ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , handle ) ) ;
done:
   SAFE_BSON_DISPOSE( bs ) ;
   PD_TRACE_EXIT( SDB_SDB_LIST_PROCEDURES ) ;
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( handle ) ;
   SAFE_JS_FREE ( cx , handle ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_EVAL, "sdb_eval" )
static JSBool sdb_eval( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_EVAL ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   jsval *argv = JS_ARGV( cx, vp ) ;
   JSString *jsFunc = NULL ;
   sdbConnectionHandle *connection  = NULL ;
   sdbReplicaGroupHandle *rg = NULL ;
   CHAR *code = NULL ;
   bson *next = NULL ;
   bson *subBson = NULL ;
   bson *copy = NULL ;
   bson errmsg  ;
   BOOLEAN needKill = FALSE ;
   SDB_SPD_RES_TYPE valueType = FMP_RES_TYPE_VOID ;
   sdbCursorHandle *cursor = NULL ;
   sdbCSHandle *cs = NULL ;
   JSObject *objCursor = NULL ;
   bson_init( &errmsg ) ;

   connection =  (sdbConnectionHandle*)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.eval(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc, argv,
                               "S" , &jsFunc ) ;
   if ( !ret )
   {
      rc = SDB_INVALIDARG ;
      REPORT ( SDB_OK == rc , "Sdb.eval(): invalid arguments" ) ;
   }

   code = ( CHAR *) JS_EncodeString ( cx, jsFunc ) ;
   VERIFY(code) ;

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   rc = sdbEvalJS( *connection, code, &valueType, cursor , &errmsg ) ;
   if ( SDB_OK != rc )
   {
      bson_iterator it ;
      const CHAR *pErrMsg = NULL ;
      bson_type iteType = bson_find( &it, &errmsg, FMP_ERR_MSG ) ;
      if ( BSON_STRING == iteType )
      {
         pErrMsg = bson_iterator_string( &it ) ;
      }
      else
      {
         bson_print( &errmsg ) ;
      }
      REPORT_RC_MSG ( SDB_OK == rc, "Sdb.eval(): failed to eval", rc,
                      pErrMsg ) ;
   }

   if ( FMP_RES_TYPE_VOID == valueType )
   {
      JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
      needKill = TRUE ;
   }
   else if ( FMP_RES_TYPE_STR == valueType ||
             FMP_RES_TYPE_NUMBER == valueType ||
             FMP_RES_TYPE_OBJ == valueType ||
             FMP_RES_TYPE_BOOL == valueType )
   {
      next = bson_create() ;
      VERIFY( next ) ;
      rc = sdbNext( *cursor, next ) ;
      REPORT ( SDB_OK == rc , "Sdb.eval(): failed to fetch" ) ;
      needKill = TRUE ;
      bson_iterator it ;
      bson_type iteType = bson_find( &it, next, FMP_RES_VALUE ) ;
      if ( BSON_EOO ==  iteType )
      {
         REPORT ( SDB_OK == rc , "Sdb.eval(): invalid bsonobj was fetched" ) ;
      }
      else if ( BSON_STRING == iteType )
      {
         const CHAR *str = bson_iterator_string( &it ) ;
         JSString *jsstr = JS_NewStringCopyN ( cx , str, ossStrlen ( str ) ) ;
         VERIFY( jsstr ) ;
         JS_SET_RVAL ( cx , vp, STRING_TO_JSVAL(jsstr) ) ;
      }
      else if ( BSON_BOOL == iteType )
      {
         JS_SET_RVAL( cx, vp, BOOLEAN_TO_JSVAL(bson_iterator_bool( &it ) )) ;
      }
      else if ( BSON_DOUBLE == iteType )
      {
         JS_SET_RVAL( cx, vp, DOUBLE_TO_JSVAL( bson_iterator_double( &it ) ) ) ;
      }
      else if ( BSON_INT == iteType )
      {
         JS_SET_RVAL( cx, vp, INT_TO_JSVAL(bson_iterator_int( &it )) ) ;
      }
      else if ( BSON_LONG == iteType )
      {
         JS_SET_RVAL( cx, vp, INT_TO_JSVAL(bson_iterator_int( &it )) ) ;
      }
      else if ( BSON_OBJECT == iteType )
      {
         JSObject *tmpObj = NULL ;
         tmpObj = JS_NewObject ( cx , &bson_class , 0 , 0 ) ;
         VERIFY ( tmpObj ) ;
         JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( tmpObj ) ) ;
         subBson = bson_create() ;
         VERIFY ( subBson ) ;
         bson_iterator_subobject( &it, subBson ) ;

         copy = bson_create() ;
         VERIFY ( copy ) ;
         VERIFY ( BSON_OK == bson_copy ( copy , subBson ) ) ;

         ret = JS_SetPrivate ( cx , tmpObj , copy ) ;
         VERIFY ( ret ) ;
      }
      else
      {
         REPORT ( SDB_OK == rc , "Sdb.eval(): invalid bsonobj was fetched" ) ;
      }
   }
   else if ( FMP_RES_TYPE_RECORDSET == valueType )
   {
      JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;
      VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;
   }
   else if ( FMP_RES_TYPE_CS == valueType ||
             FMP_RES_TYPE_CL == valueType ||
             FMP_RES_TYPE_RG == valueType ||
             FMP_RES_TYPE_RN == valueType )
   {
      const CHAR *name = NULL ;
      next = bson_create() ;
      VERIFY( next ) ;
      rc = sdbNext( *cursor, next ) ;
      REPORT ( SDB_OK == rc , "Sdb.eval(): failed to fetch" ) ;
      needKill = TRUE ;
      bson_iterator it ;
      bson_type iteType = bson_find( &it, next, FMP_RES_VALUE ) ;
      if ( BSON_STRING != iteType )
      {
         REPORT ( SDB_OK == rc , "Sdb.eval(): invalid bsonobj was fetched" ) ;
      }

      name = bson_iterator_string( &it ) ;

      if ( FMP_RES_TYPE_CS == valueType )
      {
         JSString *jsname = NULL ;
         jsname = JS_NewStringCopyN( cx, name, ossStrlen(name) ) ;
         VERIFY( jsname ) ;
         ret = get_cs_and_setproperty( cx, vp, connection,
                                       name, jsname ) ;
         VERIFY( ret ) ;
      }
      else if ( FMP_RES_TYPE_CL == valueType )
      {
         JSString *jsname = NULL ;
         JSString *jscsname = NULL ;
         CHAR *clName = NULL ;
         JSObject *csObj = NULL ;
         jsval valConn = JSVAL_VOID ;
         jsval valCSName = JSVAL_VOID ;
         jsval valCS = JSVAL_VOID ;

         CHAR *point = ossStrchr( (CHAR *)name, '.' ) ;
         if ( NULL == point ||
              (ossStrlen(name) + name - 1) <= point )
         {
            REPORT_RC( FALSE, "invalid fullname", SDB_SYS ) ;
         }

         clName = point + 1 ;
         *point = '\0' ;
         cs = (sdbCSHandle *)JS_malloc ( cx , sizeof ( sdbCSHandle ) ) ;
         VERIFY ( cs ) ;
         *cs = SDB_INVALID_HANDLE ;

         rc = sdbGetCollectionSpace ( *connection , name , cs ) ;
         REPORT_RC ( SDB_OK == rc , "sdbGetCollectionSpace" , rc ) ;

         csObj = JS_NewObject ( cx , &cs_class , 0 , 0 ) ;
         VERIFY ( csObj ) ;
         valConn = JS_THIS ( cx , vp ) ;
         VERIFY ( JS_SetProperty ( cx , csObj , "_conn" , &valConn ) ) ;
         VERIFY ( JS_SetPrivate ( cx , csObj , cs ) ) ;
         jscsname = JS_NewStringCopyN( cx, name, ossStrlen(name) );
         valCSName = STRING_TO_JSVAL ( jscsname ) ;
         VERIFY ( JS_SetProperty ( cx , csObj , "_name" , &valCSName ) ) ;
         valCS = OBJECT_TO_JSVAL ( csObj ) ;
         VERIFY ( JS_SetProperty ( cx, JS_THIS_OBJECT ( cx , vp ),
                  name, &valCS ) ) ;

         jsname = JS_NewStringCopyN( cx, clName, ossStrlen(clName) );
         VERIFY( jsname ) ;

         ret = get_cl_and_setproperty( cx, vp, &valCS, *cs,
                                       clName, jsname ) ;
         VERIFY( ret ) ;
      }
      else if ( FMP_RES_TYPE_RG == valueType )
      {
         ret = get_rg_and_setproperty( cx, vp, *connection,
                                       0, name ) ;
         VERIFY( ret ) ;
      }
      else
      {
         CHAR *point = ossStrchr( (CHAR *)name, ':') ;
         if ( NULL == point ||
              (ossStrlen(name) + name - 1) <= point )
         {
            REPORT_RC( FALSE, "invalid nodename", SDB_SYS ) ;
         }
         *point = '\0' ;

         rg = (sdbReplicaGroupHandle *)JS_malloc ( cx,
         sizeof ( sdbReplicaGroupHandle ) ) ;
         VERIFY ( rg ) ;
         *rg = SDB_INVALID_HANDLE ;

         rc = sdbGetReplicaGroup ( *connection, name, rg ) ;
         REPORT_RC( rc == SDB_OK, "failed to get replica group", rc ) ;

         {
         CHAR *point2 = ossStrchr( point + 1, ':') ;
         if ( NULL == point2 ||
              (ossStrlen(point + 1) + point /* + 1 - 1*/) <= point2 )
         {
            REPORT_RC( FALSE, "invalid nodename", SDB_SYS ) ;
         }
         *point2 = '\0' ;

         {
         JSString *jsRGName = NULL ;
         jsval valConn = JSVAL_VOID ;
         jsval valRGName = JSVAL_VOID ;
         jsval valRG = JSVAL_VOID ;
         JSObject* rgObj = JS_NewObject ( cx, &rg_class, 0, 0 ) ;
         VERIFY ( rgObj ) ;
         valConn = JS_THIS ( cx, vp ) ;
         VERIFY ( JS_SetProperty ( cx, rgObj, "_conn",
                  &valConn ) ) ;
         VERIFY ( JS_SetPrivate ( cx, rgObj, rg ) ) ;
         jsRGName = JS_NewStringCopyN ( cx , name , ossStrlen ( name ) ) ;
         VERIFY ( jsRGName ) ;
         valRGName = STRING_TO_JSVAL ( jsRGName ) ;
         VERIFY ( JS_SetProperty ( cx, rgObj, "_name", &valRGName) ) ;
         valRG = OBJECT_TO_JSVAL( rgObj ) ;
         ret = get_node_and_setproperty( cx, vp, &valRG,
                                         rg, point + 1, point2 + 1 ) ;
         VERIFY( ret ) ;
         }
         }
      }
   }
   else
   {
      REPORT ( FALSE , "Sdb.eval(): unknown type of res:%d", valueType ) ;
   }

done:

   if ( needKill  && NULL != cursor )
   {
      INT32 r = SDB_OK ;
      do
      {
         r = sdbNext( *cursor, next ) ;
      }while ( SDB_OK == r ) ;
   }
   SAFE_JS_FREE ( cx, code ) ;
   SAFE_BSON_DISPOSE( next ) ;
   SAFE_BSON_DISPOSE( subBson ) ;
   bson_destroy( &errmsg ) ;
   PD_TRACE_EXIT ( SDB_SDB_EVAL ) ;
   return ret ;
error:
   if ( NULL != cursor )
   {
      SAFE_RELEASE_CURSOR ( cursor ) ;
      SAFE_JS_FREE ( cx , cursor ) ;
   }
   SAFE_BSON_DISPOSE( copy ) ;
   SAFE_RELEASE_CS ( cs ) ;
   SAFE_JS_FREE ( cx, cs ) ;
   SAFE_RELEASE_RG ( rg ) ;
   SAFE_JS_FREE( cx, rg ) ;
   TRY_REPORT ( cx, "Sdb.eval(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_FLUSH_CONF, "sdb_flush_configure" )
static JSBool sdb_flush_configure( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_FLUSH_CONF ) ;
   INT32 rc = SDB_OK ;
   sdbConnectionHandle   *connection = NULL ;
   JSBool                 ret        = JS_TRUE ;
   JSObject *             objData     = NULL ;
   bson *                 bsonData    = NULL ;

   connection = (sdbConnectionHandle *)
   JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.flushConfigure: no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "o" , &objData) ;
   REPORT ( ret, "Sdb.flushConfigure(): wrong arguments" ) ;

   if ( JS_FALSE == objToBson ( cx , objData , &bsonData ) )
   {
      rc = SDB_INVALIDARG ;
      REPORT_RC ( JS_FALSE , "Sdb.flushConfigure: failed"
                  " to convert bson" , rc ) ;
   }

   rc = sdbFlushConfigure( *connection, bsonData ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.flushConfigure()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done:
   SAFE_BSON_DISPOSE ( bsonData ) ;
   PD_TRACE_EXIT ( SDB_SDB_FLUSH_CONF ) ;
   return ret ;
error:
   TRY_REPORT ( cx, "Sdb.flushConfigure(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_RM_RG, "rg_remove_rg" )
static JSBool ssdb_remove_rg ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_RM_RG );
   INT32 rc = SDB_OK ;
   sdbConnectionHandle   *connection = NULL ;
   JSString *             strRGName  = NULL ;
   CHAR *                 rgName     = NULL ;
   JSBool                 ret        = JS_TRUE ;

   connection = (sdbConnectionHandle *)
   JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.removeRG: no connection handle" ) ;

   ret = JS_ConvertArguments ( cx, argc, JS_ARGV ( cx, vp ),
                               "S", &strRGName ) ;
   REPORT ( ret, "Sdb.removeRG(): wrong arguments" ) ;

   rgName = (CHAR*)JS_EncodeString ( cx, strRGName ) ;
   VERIFY ( rgName ) ;

   rc = sdbRemoveReplicaGroup( *connection, rgName ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.removeRG()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done:
   SAFE_JS_FREE ( cx, rgName ) ;
   PD_TRACE_EXIT ( SDB_SDB_RM_RG );
   return ret ;
error:
   TRY_REPORT ( cx, "Sdb.removeRG(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CRT_CATA_RG, "sdb_create_cata_rg" )
static JSBool sdb_create_cata_rg ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_CRT_CATA_RG );
   sdbCollectionHandle   *connection        = NULL ;
   JSObject *             objConfig         = NULL ;
   JSString *             strHost           = NULL ;
   JSString *             strPort           = NULL ;
   JSString *             strDBPath         = NULL ;
   CHAR *                 host              = NULL ;
   CHAR *                 port              = NULL ;
   CHAR *                 dbPath            = NULL ;
   bson *                 bsonConfig        = NULL ;
   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;

   connection = (sdbConnectionHandle *)
               JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.createReplicaCataGroup: no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "SSS/o" ,
                               &strHost , &strPort , &strDBPath , &objConfig ) ;
   REPORT ( ret , "Sdb.createReplicaCataGroup(): wrong arguments" ) ;

   if ( objConfig )
   {
      ret = objToBson ( cx , objConfig , &bsonConfig ) ;
      VERIFY ( ret ) ;
   }

   host = (CHAR *) JS_EncodeString ( cx , strHost ) ;
   VERIFY ( host ) ;

   port = (CHAR *) JS_EncodeString ( cx , strPort ) ;
   VERIFY ( port ) ;

   dbPath = (CHAR *) JS_EncodeString ( cx, strDBPath ) ;
   VERIFY ( dbPath ) ;

   rc = sdbCreateReplicaCataGroup ( *connection, host,
                              port, dbPath, bsonConfig ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.createReplicaCataGroup()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_JS_FREE ( cx , host ) ;
   SAFE_JS_FREE ( cx , port ) ;
   SAFE_JS_FREE ( cx , dbPath ) ;
   SAFE_BSON_DISPOSE ( bsonConfig ) ;
   PD_TRACE_EXIT ( SDB_SDB_CRT_CATA_RG );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.createReplicaCataGroup(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CRT_CS, "sdb_create_cs" )
static JSBool sdb_create_cs ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_CRT_CS );
   sdbConnectionHandle *connection  = NULL ;
   sdbCSHandle *        cs          = NULL ;
   JSString *           strCSName   = NULL ;
   CHAR *               csName      = NULL ;
   int32_t              pageSize    = SDB_PAGESIZE_DEFAULT ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSObject *           objCS       = NULL ;
   JSObject *           objOptions     = NULL ;
   jsval                valConn     = JSVAL_VOID ;
   jsval                valName     = JSVAL_VOID ;
   jsval                valCS       = JSVAL_VOID ;
   jsval               *argv        = JS_ARGV( cx, vp ) ; 
   bson                 options ;

   bson_init( &options ) ;

   if ( 1 != argc && 2 != argc )
   {
      REPORT ( ret , "Sdb.createCS(): wrong arguments" ) ;
   }

   cs = (sdbCSHandle *) JS_malloc ( cx , sizeof ( sdbCSHandle ) ) ;
   VERIFY ( cs ) ;
   *cs = SDB_INVALID_HANDLE ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.createCS: no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strCSName ) ;
   REPORT ( ret , "Sdb.createCS(): wrong arguments" ) ;

   
   if ( 2 == argc )
   {
      if ( JSVAL_IS_INT( argv[1] ) )
      {
         pageSize = JSVAL_TO_INT( argv[1] ) ;
      }
      else if ( JSVAL_IS_OBJECT( argv[1] ) )
      {
         objOptions = JSVAL_TO_OBJECT( argv[1] ) ;
      }
      else
      {
         REPORT ( ret , "Sdb.createCS(): wrong arguments" ) ;
      }
   }

   if ( 0 == pageSize && NULL == objOptions && argc > 1 )
   {
      ret = FALSE ;
      REPORT ( ret , "Sdb.createCS(): wrong arguments" ) ;
   }

   if ( 0 == pageSize )
   {
      pageSize = SDB_PAGESIZE_DEFAULT ;
   }

   csName = (CHAR *) JS_EncodeString ( cx , strCSName ) ;
   VERIFY ( csName ) ;

   if ( NULL != objOptions )
   {
      sptConvertor c( cx ) ;
      rc = c.toBson( objOptions, &options ) ;
      VERIFY ( SDB_OK == rc ) ;
   }
   else if ( 0 != pageSize )
   {
      bson_append_int( &options, FIELD_NAME_PAGE_SIZE, pageSize ) ;
   }
   else
   {
      
   }

   bson_finish( &options ) ;
   rc = sdbCreateCollectionSpaceV2( *connection , csName ,&options, cs );
   REPORT_RC ( SDB_OK == rc , "Sdb.createCS()" , rc ) ;
   rc = sdbGetCollectionSpace ( *connection , csName , cs ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.createCS()" , rc ) ;
   objCS = JS_NewObject ( cx , &cs_class , 0 , 0 ) ;
   VERIFY ( objCS ) ;
   valConn = JS_THIS ( cx, vp ) ;
   VERIFY ( JS_SetProperty ( cx, objCS, "_conn", &valConn ) ) ;
   valName = STRING_TO_JSVAL ( strCSName ) ;
   VERIFY ( JS_SetProperty ( cx , objCS , "_name" , &valName ) ) ;
   VERIFY ( JS_SetPrivate ( cx , objCS , cs ) ) ;
   valCS = OBJECT_TO_JSVAL ( objCS ) ;
   VERIFY ( JS_SetProperty ( cx , JS_THIS_OBJECT ( cx , vp ) , csName, &valCS ) ) ;
   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCS ) ) ;

done :
   SAFE_JS_FREE ( cx , csName ) ;
   bson_destroy( &options ) ;
   PD_TRACE_EXIT ( SDB_SDB_CRT_CS );
   return ret ;
error :
   SAFE_RELEASE_CS ( cs ) ;
   SAFE_JS_FREE ( cx, cs ) ;
   TRY_REPORT ( cx , "Sdb.createCS(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION( SDB_GET_RG_AND_SETPROPERTY, "get_rg_and_setproperty" )
JSBool get_rg_and_setproperty( JSContext *cx, jsval *vp,
                               sdbConnectionHandle conn,
                               UINT32 id, const CHAR *name )
{
   PD_TRACE_ENTRY( SDB_GET_RG_AND_SETPROPERTY ) ;
   INT32 rc = SDB_OK ;
   sdbReplicaGroupHandle *rg = NULL ;
   JSBool ret = JS_TRUE ;
   CHAR *rgName = NULL ;
   jsval valName = JSVAL_VOID ;
   jsval valConn = JSVAL_VOID ;
   JSObject *objRG = NULL ;
   JSString *strRGName = NULL ;

   rg = (sdbReplicaGroupHandle *)JS_malloc ( cx,
         sizeof ( sdbReplicaGroupHandle ) ) ;
   VERIFY ( rg ) ;
   *rg = SDB_INVALID_HANDLE ;

   if ( NULL != name )
   {
      rc = sdbGetReplicaGroup ( conn, name, rg ) ;
      REPORT_RC ( SDB_OK == rc, "get_rg_and_setproperty()", rc ) ;
      rgName = (CHAR *)name ;
   }
   else
   {
      rc = sdbGetReplicaGroup1 ( conn, id, rg ) ;
      REPORT_RC ( SDB_OK == rc, "get_rg_and_setproperty()", rc ) ;
      rc = sdbGetReplicaGroupName ( *rg, &rgName ) ;
      REPORT_RC ( SDB_OK == rc, "get_rg_and_setproperty()", rc ) ;
   }

   objRG = JS_NewObject ( cx, &rg_class, 0, 0 ) ;
   VERIFY ( objRG ) ;
   valConn = JS_THIS ( cx, vp ) ;
   VERIFY ( JS_SetProperty ( cx, objRG, "_conn", &valConn ) ) ;

   JS_SET_RVAL ( cx, vp, OBJECT_TO_JSVAL ( objRG ) ) ;

   VERIFY ( JS_SetPrivate ( cx, objRG, rg ) ) ;

   strRGName = JS_NewStringCopyN ( cx , rgName , ossStrlen ( rgName ) ) ;
   VERIFY ( strRGName ) ;
   valName = STRING_TO_JSVAL ( strRGName ) ;
   VERIFY ( JS_SetProperty ( cx, objRG, "_name", &valName ) ) ;
done:
   PD_TRACE_EXIT( SDB_GET_RG_AND_SETPROPERTY ) ;
   return ret ;
error:
   SAFE_RELEASE_RG ( rg ) ;
   SAFE_JS_FREE ( cx, rg ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_GET_RG, "sdb_get_rg" )
static JSBool sdb_get_rg ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_GET_RG );
   sdbConnectionHandle   *connection  = NULL ;
   jsval *                argv        = JS_ARGV ( cx , vp ) ;
   UINT32                 rgID        = 0 ;
   JSBool                 ret         = JS_TRUE ;
   CHAR                  *rgName      = NULL ;
   BOOLEAN                rgNameAlloc = FALSE ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection , "Sdb.getRG: no connection handle" ) ;

   if ( argc != 1 )
   {
      REPORT ( FALSE, "Sdb.getRG(<name>|<id>): wrong arguments" ) ;
   }

   if ( JSVAL_IS_INT ( argv[0] ) )
   {
      rgID = (UINT32)JSVAL_TO_INT ( argv[0] ) ;
      ret = get_rg_and_setproperty( cx, vp, *connection,
                                    rgID, NULL ) ;
      VERIFY( ret ) ;
   }
   else if ( JSVAL_IS_STRING ( argv[0] ) )
   {
      rgName = (CHAR *) JS_EncodeString ( cx, JSVAL_TO_STRING ( argv[0] ) ) ;
      VERIFY ( rgName ) ;
      rgNameAlloc = TRUE ;
      ret = get_rg_and_setproperty( cx, vp, *connection,
                                    0, rgName ) ;
      VERIFY( ret ) ;
   }
   else
   {
      REPORT ( FALSE, "Sdb.getRG(<name>|<id>): wrong arguments" ) ;
   }
done :
   if ( rgNameAlloc )
      SAFE_JS_FREE ( cx, rgName ) ;
   PD_TRACE_EXIT ( SDB_SDB_GET_RG );
   return ret ;
error :
   TRY_REPORT ( cx, "Sdb.getRG(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION(GET_CS_AND_SETPROPERTY, "get_cs_and_setproperty" )
JSBool get_cs_and_setproperty( JSContext *cx, jsval *vp,
                               sdbConnectionHandle *connection,
                               const CHAR *csName,
                               JSString *jsCSName )
{
   PD_TRACE_ENTRY( GET_CS_AND_SETPROPERTY ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbCSHandle *cs = NULL ;
   JSObject *csObj = NULL ;
   jsval valConn = JSVAL_VOID ;
   jsval valName = JSVAL_VOID ;
   jsval valCS = JSVAL_VOID ;

   cs = (sdbCSHandle *) JS_malloc ( cx , sizeof ( sdbCSHandle ) ) ;
   VERIFY ( cs ) ;
   *cs = SDB_INVALID_HANDLE ;
   rc = sdbGetCollectionSpace ( *connection , csName , cs ) ;
   REPORT_RC ( SDB_OK == rc , "get_cs_and_setproperty" , rc ) ;
   csObj = JS_NewObject ( cx , &cs_class , 0 , 0 ) ;
   VERIFY ( csObj ) ;
   valConn = JS_THIS ( cx , vp ) ;
   VERIFY ( JS_SetProperty ( cx , csObj , "_conn" , &valConn ) ) ;
   valName = STRING_TO_JSVAL ( jsCSName ) ;
   VERIFY ( JS_SetProperty ( cx , csObj , "_name" , &valName ) ) ;
   VERIFY ( JS_SetPrivate ( cx , csObj , cs ) ) ;
   valCS = OBJECT_TO_JSVAL ( csObj ) ;
   VERIFY ( JS_SetProperty ( cx, JS_THIS_OBJECT ( cx , vp ),
            csName, &valCS ) ) ;
   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( csObj ) ) ;
done:
   PD_TRACE_EXIT( GET_CS_AND_SETPROPERTY ) ;
   return ret ;
error:
   SAFE_RELEASE_CS ( cs ) ;
   SAFE_JS_FREE ( cx , cs ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_GET_CS, "sdb_get_cs" )
static JSBool sdb_get_cs ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_GET_CS );
   sdbConnectionHandle *connection  = NULL ;
   JSString *           strCSName   = NULL ;
   CHAR *               csName      = NULL ;
   JSBool               ret         = JS_TRUE ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.getCS: no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strCSName ) ;
   REPORT ( ret , "Sdb.getCS(): wrong arguments" ) ;

   csName = (CHAR *) JS_EncodeString ( cx , strCSName ) ;
   VERIFY ( csName ) ;

   ret = get_cs_and_setproperty( cx, vp, connection,
                                 csName, strCSName ) ;
   VERIFY( ret ) ;

done :
   SAFE_JS_FREE ( cx , csName ) ;
   PD_TRACE_EXIT ( SDB_GET_CS );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.getCS(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_DROP_CS, "sdb_drop_cs" )
static JSBool sdb_drop_cs( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_DROP_CS );
   sdbConnectionHandle *connection  = NULL ;
   JSString *           strCSName   = NULL ;
   char *               csName      = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSBool               foundp      = JS_FALSE ;
   jsval                val         = JSVAL_VOID ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.dropCS(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strCSName ) ;
   REPORT ( ret , "Sdb.dropCS(): wrong arguments" ) ;

   csName = JS_EncodeString ( cx , strCSName ) ;
   VERIFY ( csName ) ;

   if ( !JS_HasProperty ( cx , JS_THIS_OBJECT(cx, vp) , csName , &foundp ))
   {
      ret = JS_FALSE ;
      goto error ;
   }

   rc = sdbDropCollectionSpace ( *connection , csName ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.dropCS()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

   JS_DeleteProperty2 ( cx , JS_THIS_OBJECT(cx, vp) , csName , &val ) ;

done :
   SAFE_JS_FREE ( cx , csName ) ;
   PD_TRACE_EXIT ( SDB_SDB_DROP_CS );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.dropCS(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_SNAPSHOT, "sdb_snapshot" )
static JSBool sdb_snapshot ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_SNAPSHOT );
   sdbConnectionHandle *connection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSObject *           objCursor   = NULL ;
   sdbCursorHandle *    cursor      = NULL ;
   int32_t              snapType    = SDB_SNAP_DATABASE ;
   JSObject *           objCond     = NULL ;
   JSObject *           objSel      = NULL ;
   JSObject *           objOrder    = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonSel     = NULL ;
   bson *               bsonOrder   = NULL ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.snapshot(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "i/ooo" ,
                               &snapType , &objCond , &objSel , &objOrder ) ;
   REPORT ( ret , "Sdb.snapshot(): invalid arguments" ) ;

   if ( objCond )
   {
      VERIFY ( objToBson ( cx , objCond , &bsonCond ) ) ;
   }

   if ( objSel )
   {
      VERIFY ( objToBson ( cx , objSel , &bsonSel ) ) ;
   }

   if ( objOrder )
   {
      VERIFY ( objToBson ( cx , objOrder , &bsonOrder ) ) ;
   }

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   rc = sdbGetSnapshot ( *connection , snapType , bsonCond , bsonSel ,
                         bsonOrder , cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc  , "Sdb.snapshot()" , rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;

done :
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonSel ) ;
   SAFE_BSON_DISPOSE ( bsonOrder ) ;
   PD_TRACE_EXIT ( SDB_SDB_SNAPSHOT );
   return ret ;
error :
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "Sdb.snapshot(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_RESET_SNAP, "sdb_reset_snapshot" )
static JSBool sdb_reset_snapshot ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_RESET_SNAP );
   sdbConnectionHandle *connection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSObject *           objCond     = NULL ;
   bson *               bsonCond    = NULL ;
   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.resetSnapshot(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "/o" ,
                               &objCond ) ;
   REPORT ( ret , "Sdb.resetSnapshot(): invalid arguments" ) ;

   if ( objCond )
   {
      VERIFY ( objToBson ( cx , objCond , &bsonCond ) ) ;
   }
   rc = sdbResetSnapshot ( *connection, bsonCond ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.resetSnapshot()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   PD_TRACE_EXIT ( SDB_SDB_RESET_SNAP );
   return ret ;
error :
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_LIST, "sdb_list" )
static JSBool sdb_list ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_LIST );
   sdbConnectionHandle *connection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSObject *           objCursor   = NULL ;
   sdbCursorHandle *    cursor      = NULL ;
   int32_t              listType    = SDB_LIST_CONTEXTS ;
   JSObject *           objCond     = NULL ;
   JSObject *           objSel      = NULL ;
   JSObject *           objOrder    = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonSel     = NULL ;
   bson *               bsonOrder   = NULL ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.list(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "i/ooo" ,
                               &listType , &objCond , &objSel , &objOrder ) ;
   REPORT ( ret , "Sdb.list(): invalid arguments" ) ;

   if ( objCond )
   {
      VERIFY ( objToBson ( cx , objCond , &bsonCond ) ) ;
   }

   if ( objSel )
   {
      VERIFY ( objToBson ( cx , objSel , &bsonSel ) ) ;
   }

   if ( objOrder )
   {
      VERIFY ( objToBson ( cx , objOrder , &bsonOrder ) ) ;
   }

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   rc = sdbGetList ( *connection , listType , bsonCond , bsonSel ,
                     bsonOrder , cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc  , "Sdb.list()" , rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;

done :
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonSel ) ;
   SAFE_BSON_DISPOSE ( bsonOrder ) ;
   PD_TRACE_EXIT ( SDB_SDB_LIST );
   return ret ;
error :
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "Sdb.list(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_START_RG, "sdb_start_rg" )
static JSBool sdb_start_rg ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_START_RG );
   JSBool                  ret        = JS_TRUE ;
   INT32                   rc         = SDB_OK ;
   CHAR *                  rgName     = NULL ;
   jsval *                 argv       = JS_ARGV ( cx, vp ) ;
   UINT32                  count      = 0 ;
   sdbReplicaGroupHandle * rg         = NULL ;
   sdbConnectionHandle *   connection = NULL ;

   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.startRG: no connection handle" ) ;

   rg = ( sdbReplicaGroupHandle * ) JS_malloc ( cx,
         sizeof ( sdbReplicaGroupHandle ) ) ;
   VERIFY ( rg ) ;
   *rg = SDB_INVALID_HANDLE ;

   for ( count=0; count<argc; ++count )
   {
      if ( JSVAL_IS_STRING ( argv[count] ) )
      {
         rgName = (CHAR *) JS_EncodeString ( cx,
               JSVAL_TO_STRING ( argv[count] ) ) ;
         VERIFY ( rgName ) ;

         rc = sdbGetReplicaGroup ( *connection, rgName, rg ) ;
         REPORT_RC ( SDB_OK == rc, "Sdb.startRG()", rc ) ;

         rc = sdbStartReplicaGroup ( *rg ) ;
         REPORT_RC ( SDB_OK == rc, "Sdb.startRG()", rc ) ;

         if ( rgName )
         {
            SAFE_JS_FREE ( cx, rgName ) ;
            rgName = NULL ;
         }
      }
      else
      {
         REPORT ( FALSE, "Sdb.startRG(): wrong arguments" ) ;
      }
   }

   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;

done:
   if ( rgName )
      SAFE_JS_FREE ( cx, rgName ) ;
   PD_TRACE_EXIT ( SDB_SDB_START_RG );
   return ret ;
error:
   SAFE_RELEASE_RG ( rg ) ;
   SAFE_JS_FREE ( cx, rg ) ;
   TRY_REPORT ( cx, "Sdb.startRG(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CRT_USER, "sdb_create_user" )
static JSBool sdb_create_user ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_CRT_USER );
   JSBool                  ret          = JS_FALSE ;
   INT32                   rc           = SDB_OK ;
   CHAR *                  usrName      = NULL ;
   JSString *              strUsrName   = NULL ;
   CHAR *                  usrPwd       = NULL ;
   JSString *              strUsrPwd    = NULL ;
   sdbConnectionHandle *   connection   = NULL ;

   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.createUsr(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "SS" , &strUsrName , &strUsrPwd ) ;
   REPORT ( ret , "Sdb.createUsr(): wrong arguments" ) ;

   if ( strUsrName )
   {
      usrName = (CHAR *) JS_EncodeString ( cx , strUsrName ) ;
      VERIFY ( usrName ) ;
      if ( strUsrPwd )
      {
         usrPwd = (CHAR *) JS_EncodeString ( cx , strUsrPwd ) ;
         VERIFY ( usrPwd ) ;
         rc = sdbCreateUsr( *connection , usrName , usrPwd ) ;
         REPORT_RC ( SDB_OK == rc , "Sdb.createUsr()" , rc ) ;
      }
      else
      {
         REPORT ( JS_FALSE , "please input password" ) ;
      }

   }
   else
   {
         REPORT ( JS_FALSE , "please input user name" ) ;
   }
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done:
   SAFE_JS_FREE ( cx ,  usrName ) ;
   SAFE_JS_FREE ( cx ,  usrPwd ) ;
   PD_TRACE_EXIT ( SDB_SDB_CRT_USER );
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_DROP_USER, "sdb_drop_user" )
static JSBool sdb_drop_user ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_DROP_USER );
   JSBool                  ret          = JS_FALSE ;
   INT32                   rc           = SDB_OK ;
   CHAR *                  usrName      = NULL ;
   JSString *              strUsrName   = NULL ;
   CHAR *                  usrPwd       = NULL ;
   JSString *              strUsrPwd    = NULL ;
   sdbConnectionHandle *   connection   = NULL ;

   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.createUsr(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "SS" , &strUsrName , &strUsrPwd ) ;
   REPORT ( ret , "Sdb.createUsr(): wrong arguments" ) ;

   if ( strUsrName )
   {
      usrName = (CHAR *) JS_EncodeString ( cx , strUsrName ) ;
      VERIFY ( usrName ) ;
      if ( strUsrPwd )
      {
         usrPwd = (CHAR *) JS_EncodeString ( cx , strUsrPwd ) ;
         VERIFY ( usrPwd ) ;
         rc = sdbRemoveUsr( *connection , usrName , usrPwd ) ;
         REPORT_RC ( SDB_OK == rc , "Sdb.createUsr()" , rc ) ;
      }
      else
      {
         REPORT ( JS_FALSE , "please input password" ) ;
      }

   }
   else
   {
         REPORT ( JS_FALSE , "please input user name" ) ;
   }
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done:
   SAFE_JS_FREE ( cx ,  usrName ) ;
   SAFE_JS_FREE ( cx ,  usrPwd ) ;
   PD_TRACE_EXIT ( SDB_SDB_DROP_USER );
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_EXEC, "sdb_exec" )
static JSBool sdb_exec ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_EXEC );
   JSBool                  ret          = JS_FALSE ;
   INT32                   rc           = SDB_OK ;
   CHAR *                  sql          = NULL ;
   JSString *              strSql       = NULL ;
   sdbConnectionHandle *   connection   = NULL ;
   sdbCursorHandle *       cursor       = NULL ;
   JSObject *              objCursor    = NULL ;

   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.exec(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strSql ) ;
   REPORT ( ret , "Sdb.exec(): wrong arguments" ) ;

   sql = (CHAR *) JS_EncodeString ( cx , strSql ) ;
   VERIFY ( sql ) ;

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   rc = sdbExec ( *connection , sql , cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc ,
               "Sdb.exec()" , rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;

done:
   SAFE_JS_FREE ( cx ,  sql ) ;
   PD_TRACE_EXIT ( SDB_SDB_EXEC );
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "Sdb.exec(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_EXECUP, "sdb_execUpdate" )
static JSBool sdb_execUpdate ( JSContext *cx , uintN argc , jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_EXECUP );
   JSBool                  ret          = JS_FALSE ;
   INT32                   rc           = SDB_OK ;
   CHAR *                  sql          = NULL ;
   JSString *              strSql       = NULL ;
   sdbConnectionHandle *   connection   = NULL ;

   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.execUpdate(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "S" , &strSql ) ;
   REPORT ( ret , "Sdb.execUpdate(): wrong arguments" ) ;

   sql = (CHAR *) JS_EncodeString ( cx , strSql ) ;
   VERIFY ( sql ) ;

   rc = sdbExecUpdate ( *connection , sql ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.execUpdate()" , rc ) ;

   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done:
   SAFE_JS_FREE ( cx ,  sql ) ;
   PD_TRACE_EXIT ( SDB_SDB_EXECUP );
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_TRACE_ON, "sdb_trace_on" )
static JSBool sdb_trace_on ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_TRACE_ON );
   JSBool                  ret          = JS_FALSE ;
   INT32                   rc           = SDB_OK ;
   int32_t                 bufferSize   = 0 ;
   jsval *                 argv         = JS_ARGV ( cx, vp ) ;
   sdbConnectionHandle *   connection   = NULL ;
   JSString               *strComp      = NULL;
   JSString               *strBreakPoint= NULL;
   CHAR                   *comp         = NULL;
   CHAR                   *breakPoint   = NULL;
   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.traceOn(): no connection handle" ) ;

   ret = JS_ConvertArguments( cx, argc, argv, "i/SS",
                        &bufferSize, &strComp, &strBreakPoint );
   REPORT( ret, "Sdb.traceOn(): invalid arguments");

   if ( argc >= 2 )
   {
      comp = (CHAR*)JS_EncodeString( cx, strComp ) ;
      VERIFY(comp) ;
   }

   if ( argc >= 3 )
   {
      breakPoint = (CHAR*)JS_EncodeString( cx, strBreakPoint ) ;
      VERIFY( breakPoint ) ;
   }

   rc = sdbTraceStart ( *connection, bufferSize, comp, breakPoint ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.traceOn()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done :
   PD_TRACE_EXIT ( SDB_SDB_TRACE_ON );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.traceOn(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_TRACE_RESUME, "sdb_trace_resume" )
static JSBool sdb_trace_resume ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_TRACE_RESUME );
   JSBool                  ret          = JS_FALSE ;
   INT32                   rc           = SDB_OK;
   sdbConnectionHandle *   connection   = NULL ;
   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;

   rc = sdbTraceResume ( *connection ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.traceResume()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done :
   PD_TRACE_EXIT ( SDB_SDB_TRACE_RESUME );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.traceResume(): false" ) ;
   goto done ;
}


// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_TRACE_OFF, "sdb_trace_off" )
static JSBool sdb_trace_off ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_TRACE_OFF );
   JSBool                  ret          = JS_FALSE ;
   INT32                   rc           = SDB_OK ;
   jsval *                 argv         = JS_ARGV ( cx, vp ) ;
   CHAR                   *pFileName    = NULL ;
   sdbConnectionHandle *   connection   = NULL ;

   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.traceOff(): no connection handle" ) ;

   if ( argc > 1 )
   {
      REPORT ( FALSE, "Sdb.traceOff([dump file]): wrong arguments" ) ;
   }
   if ( argc == 1 )
   {
      if ( JSVAL_IS_STRING(argv[0]) )
      {
         pFileName = (CHAR*)JS_EncodeString ( cx, JSVAL_TO_STRING(argv[0]) ) ;
         VERIFY ( pFileName ) ;
      }
      else
      {
         REPORT ( FALSE, "Sdb.traceOff([dump file]): wrong arguments" ) ;
      }
   }
   rc = sdbTraceStop ( *connection, pFileName ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.traceOff()", rc ) ;
   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done :
   SAFE_JS_FREE ( cx, pFileName ) ;
   PD_TRACE_EXIT ( SDB_SDB_TRACE_OFF );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.traceOff(): false" ) ;
   goto done ;
}

static JSBool sdb_trace_status ( JSContext *cx, uintN argc, jsval *vp )
{
   JSBool                  ret          = JS_TRUE ;
   INT32                   rc           = SDB_OK ;
   sdbConnectionHandle *   connection   = NULL ;
   sdbCursorHandle *       cursor       = NULL ;
   JSObject *              objCursor    = NULL ;

   connection = ( sdbConnectionHandle * )
         JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.exec(): no connection handle" ) ;

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;

   *cursor = SDB_INVALID_HANDLE ;

   rc = sdbTraceStatus ( *connection, cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc ,
               "Sdb.traceStatus()" , rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;

done:
   return ret ;
error:
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "Sdb.traceStatus(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_TRANS_BEGIN, "sdb_trans_begin" )
static JSBool sdb_trans_begin ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_TRANS_BEGIN );
   sdbCollectionHandle   *connection        = NULL ;
   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;

   connection = (sdbConnectionHandle *)
               JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.transBegin: no connection handle" ) ;

   rc = sdbTransactionBegin ( *connection ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.transBegin()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   PD_TRACE_EXIT ( SDB_SDB_TRANS_BEGIN );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.transBegin(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_TRANS_COMMIT, "sdb_trans_commit" )
static JSBool sdb_trans_commit ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_TRANS_COMMIT );
   sdbCollectionHandle   *connection        = NULL ;
   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;

   connection = (sdbConnectionHandle *)
               JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.transCommit: no connection handle" ) ;

   rc = sdbTransactionCommit ( *connection ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.transCommit()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done :
   PD_TRACE_EXIT ( SDB_SDB_TRANS_COMMIT );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.transCommit(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_TRANS_ROLLBACK, "sdb_trans_rollback" )
static JSBool sdb_trans_rollback ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_TRANS_ROLLBACK );
   sdbCollectionHandle   *connection        = NULL ;
   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;

   connection = (sdbConnectionHandle *)
               JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.transRollback: no connection handle" ) ;

   rc = sdbTransactionRollback ( *connection ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.transRollback()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done :
   PD_TRACE_EXIT ( SDB_SDB_TRANS_ROLLBACK );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.transRollback(): false" ) ;
   goto done ;
}


// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CLOSE, "sdb_close" )
static JSBool sdb_close ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_CLOSE );
   sdbCollectionHandle   *connection        = NULL ;
   JSBool                 ret               = JS_TRUE ;
   connection = (sdbConnectionHandle *)
               JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.close: no connection handle" ) ;

   sdbDisconnect(*connection ) ;
   /*
   UINT64 addr = 0 ;
   void *p = NULL ;
   __sdbGetReserveSpace1 ( *connection, &addr ) ;
   __sdbSetReserveSpace1 ( *connection, 0 ) ;
   p = (void*)addr ;
   JS_RemoveValueRoot ( cx, (jsval*)p ) ;
   SAFE_JS_FREE ( cx, p ) ;
   */

   ret = JS_SetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ), 0 ) ;
   VERIFY ( ret ) ;

   JS_SET_RVAL ( cx, vp, JSVAL_VOID ) ;
done :
   PD_TRACE_EXIT ( SDB_SDB_CLOSE );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.close(): false" ) ;
   ret = JS_FALSE;
   goto done ;
}


// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_BACKUP_OFFLINE, "sdb_backup_offline" )
static JSBool sdb_backup_offline ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_BACKUP_OFFLINE );
   sdbCollectionHandle   *connection        = NULL ;
   JSObject *             objConfig         = NULL ;
   bson *                 bsonConfig        = NULL ;
   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;

   connection = (sdbConnectionHandle *)
               JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.backupOffline: no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "/o" ,
                               &objConfig ) ;
   REPORT ( ret , "Sdb.backupOffline(): wrong arguments" ) ;

   if ( objConfig )
   {
      ret = objToBson ( cx , objConfig , &bsonConfig ) ;
      VERIFY ( ret ) ;
   }
   rc = sdbBackupOffline ( *connection, bsonConfig ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.backupOffline()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_BSON_DISPOSE ( bsonConfig ) ;
   PD_TRACE_EXIT ( SDB_SDB_BACKUP_OFFLINE );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.backupOffline(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_LIST_BACKUP, "sdb_list_backup" )
static JSBool sdb_list_backup ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_LIST_BACKUP );
   sdbConnectionHandle *connection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSObject *           objCursor   = NULL ;
   sdbCursorHandle *    cursor      = NULL ;
   JSObject *           objOpt     = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objSel      = NULL ;
   JSObject *           objOrder    = NULL ;
   bson *               bsonOpt    = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonSel     = NULL ;
   bson *               bsonOrder   = NULL ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.listBackup(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "/oooo" ,
                               &objOpt , &objCond , &objSel , &objOrder ) ;
   REPORT ( ret , "Sdb.listBackup(): invalid arguments" ) ;

   if ( objOpt )
   {
      VERIFY ( objToBson ( cx , objOpt , &bsonOpt ) ) ;
   }

   if ( objCond )
   {
      VERIFY ( objToBson ( cx , objCond , &bsonCond ) ) ;
   }

   if ( objSel )
   {
      VERIFY ( objToBson ( cx , objSel , &bsonSel ) ) ;
   }

   if ( objOrder )
   {
      VERIFY ( objToBson ( cx , objOrder , &bsonOrder ) ) ;
   }

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   rc = sdbListBackup( *connection, bsonOpt, bsonCond, bsonSel,
                     bsonOrder, cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc  , "Sdb.listBackup()", rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;

done :
   SAFE_BSON_DISPOSE ( bsonOpt ) ;
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonSel ) ;
   SAFE_BSON_DISPOSE ( bsonOrder ) ;
   PD_TRACE_EXIT ( SDB_SDB_LIST_BACKUP );
   return ret ;
error :
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "Sdb.listBackup(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_REMOVE_BACKUP, "sdb_remove_backup" )
static JSBool sdb_remove_backup ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_REMOVE_BACKUP );
   sdbCollectionHandle   *connection        = NULL ;
   JSObject *             objConfig         = NULL ;
   bson *                 bsonConfig        = NULL ;
   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;

   connection = (sdbConnectionHandle *)
               JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.removeBackup(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "/o" ,
                               &objConfig ) ;
   REPORT ( ret , "Sdb.removeBackup(): wrong arguments" ) ;

   if ( objConfig )
   {
      ret = objToBson ( cx , objConfig , &bsonConfig ) ;
      VERIFY ( ret ) ;
   }
   rc = sdbRemoveBackup ( *connection, bsonConfig ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.removeBackup()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_BSON_DISPOSE ( bsonConfig ) ;
   PD_TRACE_EXIT ( SDB_SDB_REMOVE_BACKUP );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.removeBackup(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_LIST_TASKS, "sdb_list_tasks" )
static JSBool sdb_list_tasks ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_LIST_TASKS );
   sdbConnectionHandle *connection  = NULL ;
   INT32                rc          = SDB_OK ;
   JSBool               ret         = JS_TRUE ;
   JSObject *           objCursor   = NULL ;
   sdbCursorHandle *    cursor      = NULL ;
   JSObject *           objCond     = NULL ;
   JSObject *           objSel      = NULL ;
   JSObject *           objOrder    = NULL ;
   JSObject *           objHint     = NULL ;
   bson *               bsonCond    = NULL ;
   bson *               bsonSel     = NULL ;
   bson *               bsonOrder   = NULL ;
   bson *               bsonHint    = NULL ;

   connection = (sdbConnectionHandle *)
      JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.listTasks(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) , "/oooo" ,
                               &objCond , &objSel , &objOrder, &objHint ) ;
   REPORT ( ret , "Sdb.listTasks(): invalid arguments" ) ;

   if ( objCond )
   {
      VERIFY ( objToBson ( cx , objCond , &bsonCond ) ) ;
   }

   if ( objSel )
   {
      VERIFY ( objToBson ( cx , objSel , &bsonSel ) ) ;
   }

   if ( objOrder )
   {
      VERIFY ( objToBson ( cx , objOrder , &bsonOrder ) ) ;
   }

   if ( objHint )
   {
      VERIFY ( objToBson ( cx , objHint , &bsonHint ) ) ;
   }

   cursor = (sdbCursorHandle *) JS_malloc ( cx , sizeof ( sdbCursorHandle ) ) ;
   VERIFY ( cursor ) ;
   *cursor = SDB_INVALID_HANDLE ;

   rc = sdbListTasks( *connection, bsonCond, bsonSel,
                     bsonOrder, bsonHint, cursor ) ;
   REPORT_RC ( SDB_OK == rc || SDB_DMS_EOC == rc  , "Sdb.listTasks()", rc ) ;

   if ( SDB_DMS_EOC == rc )
   {
      SAFE_JS_FREE ( cx , cursor ) ;
      cursor = NULL ;
   }

   objCursor = JS_NewObject ( cx , &cursor_class , NULL , NULL ) ;
   VERIFY ( objCursor ) ;

   JS_SET_RVAL ( cx , vp , OBJECT_TO_JSVAL ( objCursor ) ) ;

   VERIFY ( JS_SetPrivate ( cx , objCursor , cursor ) ) ;

done :
   SAFE_BSON_DISPOSE ( bsonCond ) ;
   SAFE_BSON_DISPOSE ( bsonSel ) ;
   SAFE_BSON_DISPOSE ( bsonOrder ) ;
   SAFE_BSON_DISPOSE ( bsonHint ) ;
   PD_TRACE_EXIT ( SDB_SDB_LIST_TASKS );
   return ret ;
error :
   SAFE_RELEASE_CURSOR ( cursor ) ;
   SAFE_JS_FREE ( cx , cursor ) ;
   TRY_REPORT ( cx , "Sdb.listBackup(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_WAIT_TASKS, "sdb_wait_tasks" )
static JSBool sdb_wait_tasks ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_WAIT_TASKS );
   sdbCollectionHandle   *connection        = NULL ;
   INT32                  rc                = SDB_OK ;
   JSBool                 ret               = JS_TRUE ;
   SINT64                *idArray           = NULL ;
   jsint                  len               = 0 ;
   jsint                  i                 = 0 ;
   jsval                 *argv              = JS_ARGV( cx, vp ) ;

   connection = (sdbConnectionHandle *)
                 JS_GetPrivate ( cx, JS_THIS_OBJECT ( cx, vp ) ) ;
   REPORT ( connection, "Sdb.waitTasks: no connection handle" ) ;
   REPORT ( argc >= 1, "Sdb.waitTasks() need at lease one task id for argument." ) ;
   len = argc ;
   idArray = (SINT64*) JS_malloc ( cx, len * sizeof( SINT64 ) ) ;
   for ( i = 0; i < len; i++ )
   {
      if ( JSVAL_IS_INT ( argv[i] ) )
      {
         idArray[i] = (SINT64)JSVAL_TO_INT ( argv[i] ) ;
      }
      else
      {
         REPORT( FALSE, "Sdb.waitTasks(): wrong argument[%d]", i ) ;
      }
   }
   rc = sdbWaitTasks ( *connection, idArray, len ) ;
   REPORT_RC ( SDB_OK == rc, "Sdb.waitTasks()", rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   SAFE_JS_FREE ( cx , idArray ) ;
   PD_TRACE_EXIT ( SDB_SDB_WAIT_TASKS );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.waitTasks(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_CANCEL_TASK, "sdb_cancel_task" )
static JSBool sdb_cancel_task ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_CANCEL_TASK );
   sdbConnectionHandle *connection  = NULL ;
   JSBool               ret         = JS_TRUE ;
   INT32                rc          = SDB_OK ;
   SINT64               taskID      = 0 ;
   JSBool               isAsync     = JS_FALSE ;
   jsval               *argv        = JS_ARGV ( cx , vp ) ;

   connection = (sdbConnectionHandle *)
                 JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.cancelTask(): no connection handle" ) ;
   REPORT ( argc >= 1 ,
            "Sdb.cancelTask(): need at least one arguments" ) ;

   if ( JSVAL_IS_INT ( argv[0] ) )
   {
      taskID = (SINT64)JSVAL_TO_INT ( argv[0] ) ;
   }
   else
   {
      REPORT( FALSE, "Sdb.cancelTask(): wrong argument[%d]", 0 ) ;
   }

   if ( argc >= 2 )
   {
      VERIFY ( JS_ValueToBoolean ( cx , argv[1] , &isAsync ) ) ;
   }
   rc = sdbCancelTask( *connection, taskID, isAsync ? TRUE : FALSE ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.cancelTask()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;

done :
   PD_TRACE_EXIT ( SDB_SDB_CANCEL_TASK );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.cancelTask(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_SET_SESSION_ATTR, "sdb_set_session_attr" )
static JSBool sdb_set_session_attr ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_SET_SESSION_ATTR );
   sdbConnectionHandle *connection  = NULL ;
   JSBool ret                       = JS_TRUE ;
   INT32 rc                         = SDB_OK ;
   JSObject *objOpts                = NULL ;
   bson *bsonOpts                   = NULL ;

   connection = (sdbConnectionHandle *)
                 JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.setSession(): no connection handle" ) ;
   ret = JS_ConvertArguments ( cx, argc, JS_ARGV( cx, vp ),
                               "o", &objOpts ) ;
   REPORT ( ret, "sdb.setSession(): wrong arguments" ) ;
/*
   REPORT ( argc >= 1 ,
            "Sdb.setSession(): need one argument" ) ;
*/
   if ( JS_FALSE == objToBson( cx, objOpts, &bsonOpts ) )
   {
      rc = SDB_INVALIDARG ;
      REPORT_RC ( JS_FALSE, "Sdb.setSession()", rc ) ;
   }
   rc = sdbSetSessionAttr( *connection, bsonOpts ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.setSession()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done :
   SAFE_BSON_DISPOSE ( bsonOpts ) ;
   PD_TRACE_EXIT ( SDB_SDB_SET_SESSION_ATTR );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.setSession(): false" ) ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION ( SDB_SDB_MSG, "sdb_msg" )
static JSBool sdb_msg ( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY ( SDB_SDB_MSG );
   sdbConnectionHandle *connection  = NULL ;
   CHAR *msg                        = NULL ;
   JSString *strMsg                 = NULL ;
   JSBool ret                       = JS_TRUE ;
   INT32 rc                         = SDB_OK ;

   connection = (sdbConnectionHandle *)
                 JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.msg(): no connection handle" ) ;
   ret = JS_ConvertArguments ( cx, argc, JS_ARGV( cx, vp ),
                               "S", &strMsg ) ;
   REPORT ( ret, "Sdb.msg():wrong arguments" ) ;
   msg = (CHAR *)JS_EncodeString( cx, strMsg ) ;
   VERIFY ( msg ) ;
   rc = _sdbMsg( *connection, msg ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.msg()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done :
   SAFE_JS_FREE ( cx, msg ) ;
   PD_TRACE_EXIT ( SDB_SDB_MSG );
   return ret ;
error :
   TRY_REPORT ( cx , "Sdb.msg(): false" ) ;
   ret = JS_FALSE ;
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION( SDB_SDB_INVALIDATE_CACHE, "sdb_invalidate_cache" )
static JSBool sdb_invalidate_cache( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY( SDB_SDB_INVALIDATE_CACHE ) ;
   INT32 rc = SDB_OK ;
   JSBool ret = JS_TRUE ;
   sdbConnectionHandle *connection = NULL ;
   JSObject *conditionObj = NULL ;
   bson *condition = NULL ;

   connection = (sdbConnectionHandle *)
                 JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.invalidateCataCache(): no connection handle" ) ;

   ret = JS_ConvertArguments ( cx , argc , JS_ARGV ( cx , vp ) ,
                               "/o" , &conditionObj ) ;
   REPORT ( ret , "Sdb.invalidateCataCache(): wrong arguments" ) ;

   if ( NULL != conditionObj )
   {
      ret = objToBson( cx, conditionObj, &condition ) ;
      REPORT ( ret , "Sdb.invalidateCataCache(): failed to convert object" ) ;
   }

   rc = sdbInvalidateCache( *connection, condition ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.invalidateCataCache()" , rc ) ;

   JS_SET_RVAL ( cx , vp , JSVAL_VOID ) ;
done:
   SAFE_BSON_DISPOSE( condition ) ;
   PD_TRACE_EXIT( SDB_SDB_INVALIDATE_CACHE ) ;
   return ret ;
error:
   goto done ;
}

// PD_TRACE_DECLARE_FUNCTION( SDB_SDB_FORCE_SESSION, "sdb_force_session" )
static JSBool sdb_force_session( JSContext *cx, uintN argc, jsval *vp )
{
   PD_TRACE_ENTRY( SDB_SDB_FORCE_SESSION ) ;
   INT32 rc = SDB_OK ;
   sdbConnectionHandle *connection = NULL ;
   SINT64 sessionID = -1 ;
   CHAR *sessionStr = NULL ;
   BOOLEAN ret = TRUE ;
   jsval *argv = JS_ARGV ( cx , vp ) ;

   connection = (sdbConnectionHandle *)
                 JS_GetPrivate ( cx , JS_THIS_OBJECT ( cx , vp ) ) ;
   REPORT ( connection , "Sdb.forceSession(): no connection handle" ) ;

   if ( 1 != argc )
   {
      REPORT ( ret , "Sdb.forceSession(): wrong arguments" ) ;
   }
   else if ( JSVAL_IS_INT(argv[0]) )
   {
      sessionID = JSVAL_TO_INT( argv[0] ) ;
   }
   else if ( JSVAL_IS_STRING(argv[0]) )
   {
      sessionStr = JS_EncodeString ( cx, JSVAL_TO_STRING ( argv[0] ) ) ;
      VERIFY( sessionStr ) ;
      sessionID = ossAtoll( sessionStr ) ;
   }
   else
   {
      ret = FALSE ;
      REPORT ( ret , "Sdb.forceSession(): wrong arguments" ) ;
   }

   if ( sessionID <= 0 )
   {
      ret = FALSE ;
      REPORT ( ret , "Sdb.forceSession(): wrong arguments" ) ;
   }

   rc = sdbForceSession( *connection, sessionID ) ;
   REPORT_RC ( SDB_OK == rc , "Sdb.forceSession()" , rc ) ;
   JS_SET_RVAL( cx , vp , JSVAL_VOID ) ;
done:
   SAFE_JS_FREE( cx, sessionStr ) ;
   PD_TRACE_EXIT( SDB_SDB_FORCE_SESSION ) ;
   return ret ;
error:
   ret = FALSE ;
   goto done ;
}

static JSFunctionSpec sdb_functions[] = {
   JS_FS ( "getCS" , sdb_get_cs , 1 , 0 ) ,
   JS_FS ( "getRG" , sdb_get_rg , 1 , 0 ) ,
   JS_FS ( "createCS" , sdb_create_cs , 1 , 0 ) ,
   JS_FS ( "createRG", sdb_create_rg , 1, 0 ),
   JS_FS ( "removeRG", ssdb_remove_rg , 1, 0 ),
   JS_FS ( "createCataRG", sdb_create_cata_rg , 1, 0 ),
   JS_FS ( "dropCS" , sdb_drop_cs , 1 , 0 ) ,
   JS_FS ( "snapshot" , sdb_snapshot , 1 , 0 ) ,
   JS_FS ( "resetSnapshot" , sdb_reset_snapshot , 0 , 0 ) ,
   JS_FS ( "list" , sdb_list , 1 , 0 ) ,
   JS_FS ( "startRG" , sdb_start_rg , 1 , 0 ) ,
   JS_FS ( "createUsr" , sdb_create_user , 2 , 0 ) ,
   JS_FS ( "dropUsr", sdb_drop_user , 2 , 0 ) ,
   JS_FS ( "exec", sdb_exec , 1 , 0 ) ,
   JS_FS ( "execUpdate", sdb_execUpdate , 1 , 0 ) ,
   JS_FS ( "traceOn", sdb_trace_on, 1, 0 ),
   JS_FS ( "traceResume", sdb_trace_resume, 0, 0 ),
   JS_FS ( "traceOff", sdb_trace_off, 1, 0 ),
   JS_FS ( "traceStatus", sdb_trace_status, 0, 0 ),
   JS_FS ( "transBegin", sdb_trans_begin , 1, 0 ),
   JS_FS ( "transCommit", sdb_trans_commit , 1, 0 ),
   JS_FS ( "transRollback", sdb_trans_rollback , 1, 0 ),
   JS_FS ( "flushConfigure", sdb_flush_configure, 0, 0 ),
   JS_FS ( "close", sdb_close, 1, 0 ),
   JS_FS ( "createProcedure", sdb_crt_procedure, 1, 0 ),
   JS_FS ( "removeProcedure", sdb_rm_procedure, 1, 0 ),
   JS_FS ( "listProcedures", sdb_list_procedures,1, 0),
   JS_FS ( "eval", sdb_eval, 0, 0 ),
   JS_FS ( "backupOffline", sdb_backup_offline, 0, 0 ),
   JS_FS ( "listBackup", sdb_list_backup, 0, 0 ),
   JS_FS ( "removeBackup", sdb_remove_backup, 0, 0 ),
   JS_FS ( "listTasks", sdb_list_tasks, 0, 0 ),
   JS_FS ( "waitTasks", sdb_wait_tasks, 1, 0 ),
   JS_FS ( "cancelTask", sdb_cancel_task, 1, 0 ),
   JS_FS ( "setSessionAttr", sdb_set_session_attr, 1, 0 ),
   JS_FS ( "msg", sdb_msg, 0, 0 ),
   JS_FS ( "createDomain", sdb_create_domain, 2, 0 ),
   JS_FS ( "dropDomain", sdb_drop_domain, 1, 0 ),
   JS_FS ( "getDomain", sdb_get_domain, 1, 0 ),
   JS_FS ( "listDomains", sdb_list_domains, 0, 0 ),
   JS_FS ( "invalidateCache", sdb_invalidate_cache, 0, 0 ),
   JS_FS ( "forceSession", sdb_force_session, 0, 0 ),
   JS_FS_END
} ;

JSBool jsobj_is_query( JSContext *cx, JSObject *obj )
{
   return JS_InstanceOf( cx, obj, &query_class, NULL ) ;
}

JSBool jsobj_is_cursor( JSContext *cx, JSObject *obj )
{
   return JS_InstanceOf( cx, obj, &cursor_class, NULL ) ;
}

JSBool jsobj_is_cs( JSContext *cx, JSObject *obj )
{
   return JS_InstanceOf( cx, obj, &cs_class, NULL ) ;
}

JSBool jsobj_is_cl( JSContext *cx, JSObject *obj )
{
   return JS_InstanceOf( cx, obj, &collection_class, NULL ) ;
}

JSBool jsobj_is_rn( JSContext *cx, JSObject *obj )
{
   return JS_InstanceOf( cx, obj, &rn_class, NULL ) ;
}

JSBool jsobj_is_rg( JSContext *cx, JSObject *obj )
{
   return JS_InstanceOf( cx, obj, &rg_class, NULL ) ;
}

JSBool jsobj_is_sdbobj( JSContext *cx, JSObject *obj )
{
   if ( JS_InstanceOf( cx, obj, &cursor_class, NULL ) )
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &collection_class, NULL ))
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &query_class, NULL ))
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &rn_class, NULL ))
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &rg_class, NULL ))
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &cs_class, NULL ) )
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &sdb_class, NULL ) )
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &domain_class, NULL ) )
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &count_class, NULL ) )
   {
      return TRUE ;
   }
   else if ( JS_InstanceOf( cx, obj, &bson_class, NULL ) )
   {
      return TRUE ;
   }
   else
   {
      return FALSE ;
   }
}

JSBool jsobj_is_bsonobj( JSContext *cx, JSObject *obj )
{
   return JS_InstanceOf( cx, obj, &bson_class, NULL ) ;
}

void *jsobj_get_cursor_private( JSContext *cx, JSObject *obj )
{
   return JS_GetPrivate( cx, obj ) ;
}

#elif defined(SDB_ENGINE)
#endif

JSBool InitDbClasses( JSContext *cx, JSObject *obj )
{

   JSBool ret = JS_TRUE ;

   VERIFY ( JS_DefineFunctions ( cx , obj , global_functions ) ) ;

   VERIFY ( JS_InitClass ( cx , obj , 0 , &bson_class ,
                           bson_constructor , 0 ,
                           0 , bson_functions , 0 , 0 ) ) ;

#if defined (SDB_CLIENT)
   VERIFY ( JS_InitClass ( cx , obj , 0 , &cursor_class ,
                           cursor_constructor , 0 ,
                           0 , cursor_functions , 0 , 0 ) ) ;

   VERIFY ( JS_InitClass ( cx , obj , 0 , &collection_class ,
                           collection_constructor , 0 ,
                           0 , collection_functions , 0 , 0 ) ) ;

   VERIFY ( JS_InitClass ( cx , obj , 0 , &query_class ,
                           query_constructor , 1 ,
                           0 , 0 , 0 , 0 ) ) ;

   VERIFY ( JS_InitClass ( cx , obj , 0 , &rn_class ,
                           rn_constructor , 0 ,
                           0 , rn_functions , 0 , 0 ) ) ;

   VERIFY ( JS_InitClass ( cx , obj , 0 , &rg_class ,
                           rg_constructor , 0 ,
                           0 , rg_functions , 0 , 0 ) ) ;

   VERIFY ( JS_InitClass ( cx , obj , 0 , &cs_class ,
                           cs_constructor , 0 ,
                           0 , cs_functions , 0 , 0 ) ) ;

   VERIFY ( JS_InitClass ( cx , obj , NULL , &sdb_class ,
                           sdb_constructor , 2 ,
                           0 , sdb_functions , 0 , 0 ) ) ;

   VERIFY ( JS_InitClass ( cx , obj , NULL , &count_class ,
                           count_constructor , 1 ,
                           0 , 0 , 0 , 0 ) ) ;

   VERIFY ( JS_InitClass ( cx, obj, NULL, &domain_class,
                           domain_constructor, 0,
                           0, domain_functions, 0, 0 ) ) ;
#elif defined (SDB_ENGINE)
#endif

done :
   return ret ;
error :
   goto done ;
}


