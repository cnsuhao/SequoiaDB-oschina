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

   Source File Name = rtnCoordAuthDel.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/12/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnCoordAuthDel.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOAUTHDEL_EXECUTE, "rtnCoordAuthDel::execute" )
   INT32 rtnCoordAuthDel::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                   CHAR **ppResultBuffer, pmdEDUCB *cb,
                                   MsgOpReply &replyHeader,
                                   BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOAUTHDEL_EXECUTE ) ;
      rc = forward( pReceiveBuffer, packSize,
                      ppResultBuffer, cb, replyHeader,
                      MSG_AUTH_DELUSR_RES, FALSE ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOAUTHDEL_EXECUTE, rc ) ;
      return rc ;
   }
}
