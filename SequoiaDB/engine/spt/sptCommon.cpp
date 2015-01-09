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

   Source File Name = sptCommon.cpp

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

#include "sptCommon.hpp"
#include "pd.hpp"
#include "ossUtil.h"
#include "ossMem.h"
#include <string>
#include <sstream>

using namespace std ;

namespace engine
{
   /*
      Global function
   */
   static OSS_THREAD_LOCAL CHAR *__errmsg__ = NULL ;
   static OSS_THREAD_LOCAL INT32 __errno__ = SDB_OK ;
   static OSS_THREAD_LOCAL BOOLEAN __printError__ = TRUE ;
   static OSS_THREAD_LOCAL BOOLEAN __hasReadData__ = FALSE ;

   static OSS_THREAD_LOCAL BOOLEAN __hasSetErrMsg__ = FALSE ;
   static OSS_THREAD_LOCAL BOOLEAN __hasSetErrNo__  = FALSE ;
   static OSS_THREAD_LOCAL BOOLEAN __needClearErrorInfo__ = FALSE ;

   const CHAR *sdbGetErrMsg()
   {
      return __errmsg__ ;
   }

   void sdbSetErrMsg( const CHAR *err )
   {
      static CHAR* s_emptyMsg = "" ;
      __hasSetErrMsg__        = FALSE ;
      if ( NULL != __errmsg__ && s_emptyMsg != __errmsg__ )
      {
         SDB_OSS_FREE( __errmsg__ ) ;
         __errmsg__ = s_emptyMsg ;
      }
      if ( err && 0 != *err )
      {
         __errmsg__ = ossStrdup( err ) ;
         __hasSetErrMsg__ = TRUE ;
      }
   }

   BOOLEAN sdbIsErrMsgEmpty()
   {
      if ( __errmsg__ && *__errmsg__ )
      {
         return FALSE ;
      }
      return TRUE ;
   }

   INT32 sdbGetErrno()
   {
      return __errno__ ;
   }

   void sdbSetErrno( INT32 errNum )
   {
      __errno__ = errNum ;
      __hasSetErrNo__ = errNum ? TRUE : FALSE ;
   }

   void sdbClearErrorInfo()
   {
      sdbSetErrno( SDB_OK ) ;
      sdbSetErrMsg( NULL ) ;
   }

   BOOLEAN sdbNeedPrintError()
   {
      return __printError__ ;
   }

   void sdbSetPrintError( BOOLEAN print )
   {
      __printError__ = print ;
   }

   void sdbSetReadData( BOOLEAN hasRead )
   {
      __hasReadData__ = hasRead ;
   }

   BOOLEAN sdbHasReadData()
   {
      return __hasReadData__ ;
   }

   void sdbSetNeedClearErrorInfo( BOOLEAN need )
   {
      __needClearErrorInfo__ = need ;
   }

   BOOLEAN sdbIsNeedClearErrorInfo()
   {
      return __needClearErrorInfo__ ;
   }

   void sdbReportError( JSContext *cx, const char *msg,
                        JSErrorReport *report )
   {
      return sdbReportError( report->filename, report->lineno, msg,
                             JSREPORT_IS_EXCEPTION( report->flags ) ) ;
   }

   void sdbReportError( const CHAR *filename, UINT32 lineno,
                        const CHAR *msg, BOOLEAN isException )
   {
      BOOLEAN add = FALSE ;

      if ( SDB_OK == sdbGetErrno() || !__hasSetErrNo__ )
      {
         const CHAR *p = NULL ;
         if ( isException && msg &&
              NULL != ( p = ossStrstr( msg, ":" ) ) &&
              0 != ossAtoi( p + 1 ) )
         {
            sdbSetErrno( ossAtoi( p + 1 ) ) ;
         }
         else
         {
            sdbSetErrno( SDB_SPT_EVAL_FAIL ) ;
         }
      }

      if ( ( sdbIsErrMsgEmpty() || !__hasSetErrMsg__ ) && msg )
      {
         if ( filename )
         {
            stringstream ss ;
            ss << filename << ":" << lineno << " " << msg ;
            sdbSetErrMsg( ss.str().c_str() ) ;
         }
         else
         {
            sdbSetErrMsg( msg ) ;
         }
         add = TRUE ;
      }

      if ( sdbNeedPrintError() )
      {
         ossPrintf( "%s:%d %s\n",
                    filename ? filename : "(nofile)" ,
                    lineno, msg ) ;

         if ( !add && !sdbIsErrMsgEmpty() &&
              NULL == ossStrstr( sdbGetErrMsg(), msg ) )
         {
            ossPrintf( "%s\n", sdbGetErrMsg() ) ;
         }
      }
      __hasSetErrMsg__ = FALSE ;
      __hasSetErrNo__  = FALSE ;
   }

}

