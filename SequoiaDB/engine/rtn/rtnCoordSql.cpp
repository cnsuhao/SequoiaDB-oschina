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

   Source File Name = rtnCoordSql.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/12/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnCoordSql.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "msgMessage.hpp"

namespace engine
{
   INT32 _rtnCoordSql::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                CHAR **ppResultBuffer, pmdEDUCB *cb,
                                MsgOpReply &replyHeader,
                                BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB();
      SQL_CB *sqlcb = krcb->getSqlCB() ;
      NodeID curNodeID = pmdGetNodeID() ;
      MsgHeader *header = (MsgHeader *)pReceiveBuffer;
      CHAR *sql = NULL ;
      SINT64 contextID = -1 ;
      rc = msgExtractSql( pReceiveBuffer, &sql ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract sql" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = sqlcb->exec( sql, cb, contextID ) ;
   done:
      msgBuildReplyMsgHeader( replyHeader,
                              sizeof(replyHeader),
                              header->opCode,
                              rc,
                              contextID, 0, 0,
                              curNodeID,
                              header->requestID ) ;
      return rc ;
   error:
      goto done ;
   }
}
