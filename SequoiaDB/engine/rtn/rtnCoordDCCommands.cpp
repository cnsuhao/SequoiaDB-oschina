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

   Source File Name = rtnCoordDCCommands.cpp

   Descriptive Name = Runtime Coord Common

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          02/11/15    XJH Init
   Last Changed =

*******************************************************************************/

#include "rtnCoordDCCommands.hpp"
#include "msgMessage.hpp"
#include "pmdCB.hpp"

using namespace bson ;

namespace engine
{

   /*
      rtnCoordAlterImage implement
   */
   INT32 rtnCoordAlterImage::execute( CHAR *pReceiveBuffer,
                                      SINT32 packSize,
                                      pmdEDUCB *cb,
                                      MsgOpReply &replyHeader,
                                      rtnContextBuf *buf )
   {
      INT32 rc = SDB_OK ;
      CoordCB *pCoord = pmdGetKRCB()->getCoordCB() ;
      netMultiRouteAgent *pRouteAgent = pCoord->getRouteAgent() ;
      CoordGroupList datagroups ;
      CoordGroupList sendgroups ;
      CoordGroupList allgroups ;
      const CHAR *pAction = NULL ;

      MsgHeader *pHeader               = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode        = MSG_BS_QUERY_RES ;
      replyHeader.header.requestID     = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID           = pHeader->TID;
      replyHeader.contextID            = -1;
      replyHeader.flags                = SDB_OK;
      replyHeader.numReturned          = 0;
      replyHeader.startFrom            = 0;

      MsgOpQuery *pAttachMsg           = (MsgOpQuery *)pReceiveBuffer ;
      pAttachMsg->header.routeID.value = 0 ;
      pAttachMsg->header.TID           = cb->getTID() ;
      pAttachMsg->header.opCode        = MSG_CAT_ALTER_IMAGE_REQ ;

      {
         CHAR *pQuery = NULL ;
         rc = msgExtractQuery( pReceiveBuffer, NULL, NULL, NULL, NULL,
                               &pQuery, NULL, NULL, NULL ) ;
         PD_RC_CHECK( rc, PDERROR, "Extract command[%s] msg failed, rc: %d",
                      COORD_CMD_ALTER_IMAGE, rc ) ;
         try
         {
            BSONObj objQuery( pQuery ) ;
            BSONElement eleAction = objQuery.getField( FIELD_NAME_ACTION ) ;
            if ( String != eleAction.type() )
            {
               PD_LOG( PDERROR, "The field[%s] is not valid in command[%s]'s "
                       "param[%s]", FIELD_NAME_ACTION, COORD_CMD_ALTER_IMAGE,
                       objQuery.toString().c_str() ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            pAction = eleAction.valuestr() ;
         }
         catch( std::exception &e )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "Parse command[%s]'s param occur exception: %s",
                    COORD_CMD_ALTER_IMAGE, e.what() ) ;
            goto error ;
         }
      }

      rc = executeOnCataGroup( pReceiveBuffer, pRouteAgent, cb,
                               NULL, &datagroups ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to execute %s:%s on catalog node, rc: %d",
                 COORD_CMD_ALTER_IMAGE, pAction, rc ) ;
         goto error ;
      }

      rc = rtnCoordGetAllGroupList( cb, allgroups, NULL, FALSE, TRUE ) ;
      if ( rc )
      {
         PD_LOG( PDWARNING, "Failed to update all group list, rc: %d", rc ) ;
         rc = SDB_OK ;
      }

      pAttachMsg->header.opCode        = MSG_BS_QUERY_REQ ;
      rc = executeOnDataGroup( &pAttachMsg->header, datagroups, sendgroups,
                               pRouteAgent, cb, TRUE ) ;
      if ( rc )
      {
         PD_LOG( PDWARNING, "Failed to execute %s:%s on data nodes, "
                 "rc: %d", COORD_CMD_ALTER_IMAGE, pAction, rc ) ;
         rc = SDB_OK ;
      }

   done:
      replyHeader.flags = rc ;
      return rc ;
   error:
      goto done ;
   }

}

