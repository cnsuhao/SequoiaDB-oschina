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

   Source File Name = sptCommon.hpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/28/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPTCOMMON_HPP__
#define SPTCOMMON_HPP__

#include "core.h"
#include "jsapi.h"

#define CMD_HELP           "help"
#define CMD_QUIT           "quit"
#define CMD_QUIT1          "quit;"
#define CMD_CLEAR          "clear"
#define CMD_CLEAR1         "clear;"
#define CMD_CLEARHISTORY   "history-c"
#define CMD_CLEARHISTORY1  "history-c;"

namespace engine
{
   /*
      Global function define
   */
   const CHAR *sdbGetErrMsg() ;
   void  sdbSetErrMsg( const CHAR *err ) ;
   BOOLEAN sdbIsErrMsgEmpty() ;

   INT32 sdbGetErrno() ;
   void  sdbSetErrno( INT32 errNum ) ;

   void  sdbClearErrorInfo() ;

   BOOLEAN  sdbNeedPrintError() ;
   void     sdbSetPrintError( BOOLEAN print ) ;

   void     sdbSetReadData( BOOLEAN hasRead ) ;
   BOOLEAN  sdbHasReadData() ;

   void     sdbSetNeedClearErrorInfo( BOOLEAN need ) ;
   BOOLEAN  sdbIsNeedClearErrorInfo() ;

   void     sdbReportError( JSContext *cx, const char *msg,
                            JSErrorReport *report ) ;

   void     sdbReportError( const CHAR *filename, UINT32 lineno,
                            const CHAR *msg, BOOLEAN isException ) ;

}

#endif //SPTCOMMON_HPP__

