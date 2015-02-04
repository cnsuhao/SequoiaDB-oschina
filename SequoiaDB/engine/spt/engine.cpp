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

   Source File Name = engine.cpp

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

#include "spt.hpp"
#include "ossTypes.h"
#include "jsapi.h"
#include "ossUtil.h"
#include "pd.hpp"
#include "ossMem.hpp"
#include "pdTrace.hpp"
#include "sptTrace.hpp"
#include "sptApi.hpp"
#include "sptConvertorHelper.hpp"
#include "sptSPDef.hpp"
#include "sptCommon.hpp"

#define VERIFY(cond) if ( ! (cond) ) goto error

#include "js_in_cpp.hpp"

namespace engine {

   static ScriptEngine * globalEngine ;

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SE_GLBSE, "ScriptEngine::globalScriptEngine" )
   ScriptEngine * ScriptEngine::globalScriptEngine()
   {
      PD_TRACE_ENTRY ( SDB_SE_GLBSE );
      if ( ! globalEngine )
      {
         globalEngine = SDB_OSS_NEW ScriptEngine ;
         VERIFY( globalEngine );

         VERIFY( globalEngine->init() );
      }
   done :
      PD_TRACE_EXIT ( SDB_SE_GLBSE );
      return globalEngine ;
   error :
      SAFE_OSS_DELETE( globalEngine );
      globalEngine = NULL ;
      goto done ;
   }

   void ScriptEngine::purgeGlobalScriptEngine()
   {
      SAFE_OSS_DELETE( globalEngine );
      globalEngine = NULL ;
   }

   ScriptEngine::ScriptEngine() :
      _runtime( NULL )
   {
   }

   ScriptEngine::~ScriptEngine()
   {
      if ( _runtime ) JS_DestroyRuntime( _runtime );
      JS_ShutDown();
   }

   BOOLEAN ScriptEngine::init()
   {
      _runtime = JS_NewRuntime( 32 * 1024 * 1024 );
      return _runtime ? TRUE : FALSE ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SE_NEWSCOPE, "ScriptEngine::newScope" )
   Scope *ScriptEngine::newScope()
   {
      PD_TRACE_ENTRY ( SDB_SE_NEWSCOPE );
      Scope *scope = SDB_OSS_NEW Scope ;
      VERIFY( scope );

      VERIFY( scope->init() );

   done :
      PD_TRACE_EXIT ( SDB_SE_NEWSCOPE );
      return scope ;
   error :
      SAFE_OSS_DELETE( scope );
      scope = NULL ;
      goto done ;
   }


   static JSClass global_class = {
      "Global",                     // class name
      JSCLASS_GLOBAL_FLAGS,         // flags
      JS_PropertyStub,              // addProperty
      JS_PropertyStub,              // delProperty
      JS_PropertyStub,              // getProperty
      JS_StrictPropertyStub,        // setProperty
      JS_EnumerateStub,             // enumerate
      JS_ResolveStub,               // resolve
      JS_ConvertStub,               // convert
      JS_FinalizeStub,              // finalize
      JSCLASS_NO_OPTIONAL_MEMBERS   // optional members
   };

   Scope::Scope() :
      _context( NULL ),
      _global( NULL )
   {
   }

   Scope::~Scope()
   {
      if ( _context ) JS_DestroyContext( _context );
      _context = NULL ;
      _global = NULL ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SCOPE_INIT, "Scope::init" )
   BOOLEAN Scope::init()
   {
      BOOLEAN ret = FALSE ;
      PD_TRACE_ENTRY ( SDB_SCOPE_INIT );

      SDB_ASSERT( globalEngine, "Script engine has not been initialized" );
      SDB_ASSERT( ! _context && ! _global, "Can't init a scope twice" );

      _context = JS_NewContext( globalEngine->_runtime, 1024 * 1024 ) ;
      VERIFY( _context );

      JS_SetOptions( _context, JSOPTION_VAROBJFIX );
      JS_SetVersion( _context, JSVERSION_LATEST );
      JS_SetErrorReporter( _context, sdbReportError );

      _global = JS_NewCompartmentAndGlobalObject( _context, &global_class, NULL );
      VERIFY( _global );

      VERIFY( JS_InitStandardClasses( _context, _global ) );

      VERIFY( InitDbClasses( _context, _global ) ) ;

      VERIFY ( SDB_OK == evalInitScripts ( this ) ) ;

      ret = TRUE ;

   done :
      PD_TRACE_EXIT ( SDB_SCOPE_INIT );
      return ret ;
   error :
      goto done ;
   }

   INT32 Scope::getLastError()
   {
      return sdbGetErrno() ;
   }

   const CHAR* Scope::getLastErrMsg()
   {
      return sdbGetErrMsg() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SCOPE_EVALUATE, "Scope::evaluate" )
   INT32 Scope::evaluate ( const CHAR *code , UINT32 len,
                           const CHAR *filename, UINT32 lineno,
                           CHAR ** result, INT32 printFlag )
   {
      PD_TRACE_ENTRY ( SDB_SCOPE_EVALUATE );
      jsval        rval           = JSVAL_VOID ;
      jsval        exception      = JSVAL_VOID ;
      CHAR *       cstrException  = NULL ;
      CHAR *       cstr           = NULL ;
      INT32        rc             = SDB_OK ;

      SDB_ASSERT ( _context && _global, "this scope has not been initilized" ) ;
      SDB_ASSERT ( code , "Invalid arguments" ) ;

      sdbSetPrintError( ( printFlag & SPT_EVAL_FLAG_PRINT ) ? TRUE : FALSE ) ;
      sdbSetNeedClearErrorInfo( TRUE ) ;

      if ( ! JS_EvaluateScript ( _context , _global ,
                                 code , len > 0 ? len : ossStrlen ( code ) ,
                                 filename ? filename : "(default)" , lineno ,
                                 &rval ) )
      {
         rc = sdbGetErrno() ? sdbGetErrno() : SDB_SPT_EVAL_FAIL ; ;
         goto error ;
      }

      if ( sdbIsNeedClearErrorInfo() &&
           !JS_IsExceptionPending( _context ) )
      {
         sdbClearErrorInfo() ;
      }

      if ( !result )
      {
         goto done ;
      }

      if ( JSVAL_IS_VOID ( rval ) )
      {
#if defined (_LINUX)
         cstr = strdup ( "" ) ;
#elif defined (_WINDOWS)
         cstr = _strdup ( "" ) ;
#endif
      }
      else
      {
         cstr = convertJsvalToString ( _context , rval ) ;
      }

      if ( ! cstr )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      *result = ossStrdup ( cstr ) ;
      if ( !( result && result[0] != '\0') )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR , "memory allcation fail" ) ;
         goto error ;
      }

   done :
      SAFE_JS_FREE ( _context , cstr ) ;
      PD_TRACE_EXITRC ( SDB_SCOPE_EVALUATE, rc );
      return rc ;
   error :
      if ( JS_GetPendingException ( _context , &exception ) )
      {
         cstrException = convertJsvalToString ( _context , exception ) ;
         if ( cstrException )
         {
            if ( printFlag & SPT_EVAL_FLAG_PRINT )
            {
               ossPrintf ( "Uncaught exception: %s\n" , cstrException ) ;
            }
            SAFE_JS_FREE ( _context , cstrException ) ;
         }
         else
         {
            JS_ClearPendingException ( _context ) ;
         }
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_SCOPE_EVALUATE2, "Scope::evaluate2" )
   INT32 Scope::evaluate2 ( const CHAR *code, UINT32 len, UINT32 lineno,
                            jsval *rval, CHAR **errMsg,
                            INT32 printFlag )
   {
      PD_TRACE_ENTRY ( SDB_SCOPE_EVALUATE2 );
      INT32 rc = SDB_OK ;
      jsval exception = JSVAL_VOID ;
      CHAR *cstrException = NULL ;
      SDB_ASSERT ( _context && _global, "this scope has not been initilized" ) ;
      SDB_ASSERT ( code , "Invalid arguments" ) ;

      sdbSetPrintError( ( printFlag & SPT_EVAL_FLAG_PRINT ) ? TRUE : FALSE ) ;
      sdbSetNeedClearErrorInfo( TRUE ) ;

      if ( ! JS_EvaluateScript ( _context, _global, code, len, NULL,
                                 lineno, rval ) )
      {
         rc = sdbGetErrno() ? sdbGetErrno() : SDB_SPT_EVAL_FAIL ;
         goto error ;
      }

      if ( sdbIsNeedClearErrorInfo() &&
           !JS_IsExceptionPending( _context ) )
      {
         sdbClearErrorInfo() ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_SCOPE_EVALUATE2, rc );
      return rc ;
   error:
      if ( JS_IsExceptionPending( _context ) &&
           JS_GetPendingException ( _context , &exception ) )
      {
         cstrException = convertJsvalToString ( _context , exception ) ;
         if ( cstrException )
         {
            if ( printFlag & SPT_EVAL_FLAG_PRINT )
            {
               ossPrintf ( "Uncaught exception: %s\n" , cstrException ) ;
            }

            *errMsg = ossStrdup( cstrException ) ;
            SAFE_JS_FREE ( _context , cstrException ) ;
         }
         else
         {
            JS_ClearPendingException ( _context ) ;
         }
      }
      goto done ;
   }

} // namespace engine

