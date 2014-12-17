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

   Source File Name = rtnCoordAuthCrt.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/12/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnCoordAuthCrt.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOAUTHCRT_EXECUTE, "rtnCoordAuthCrt::execute" )
   INT32 rtnCoordAuthCrt::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                   CHAR **ppResultBuffer, pmdEDUCB *cb,
                                   MsgOpReply &replyHeader,
                                   BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNCOAUTHCRT_EXECUTE ) ;
      rc = forward( pReceiveBuffer, packSize,
                      ppResultBuffer, cb, replyHeader,
                      MSG_AUTH_CRTUSR_RES, FALSE ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOAUTHCRT_EXECUTE, rc ) ;
      return rc ;
   }
}
