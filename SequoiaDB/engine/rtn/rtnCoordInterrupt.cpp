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

   Source File Name = rtnCoordInterrupt.cpp

   Descriptive Name = Runtime Coord Interrupt

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   interrupt processing on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoordInterrupt.hpp"
#include "coordSession.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "pmdCB.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINTERRUPT_EXECUTE, "rtnCoordInterrupt::execute" )
   INT32 rtnCoordInterrupt::execute( CHAR * pReceiveBuffer, SINT32 packSize,
                                     CHAR * * ppResultBuffer, pmdEDUCB * cb,
                                     MsgOpReply & replyHeader,
                                     BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOINTERRUPT_EXECUTE ) ;
      MsgHeader *pInterrupt = (MsgHeader *)pReceiveBuffer;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode = MSG_BS_INTERRUPTE;
      replyHeader.header.TID = pInterrupt->TID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.requestID = pInterrupt->requestID;
      replyHeader.flags = SDB_OK ;
      replyHeader.startFrom = 0;
      replyHeader.numReturned = 0;

      pmdKRCB *pKrcb = pmdGetKRCB();
      SDB_RTNCB *pRtncb = pKrcb->getRTNCB();

      CoordSession *pSession = cb->getCoordSession();
      if ( pSession )
      {
         ROUTE_SET routeSet;
         pSession->getAllSessionRoute( routeSet );
         SendInterrupt( cb, routeSet );
      }

      {
         SINT64 contextID = -1 ;
         while ( -1 != (contextID = cb->contextPeek() ))
            pRtncb->contextDelete( contextID, NULL ) ;
      }

      PD_TRACE_EXITRC ( SDB_RTNCOINTERRUPT_EXECUTE, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOINTERRUPT_SENDINTER, "rtnCoordInterrupt::SendInterrupt" )
   void rtnCoordInterrupt::SendInterrupt( pmdEDUCB *cb, ROUTE_SET &routeSet )
   {
      PD_TRACE_ENTRY ( SDB_RTNCOINTERRUPT_SENDINTER ) ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      netMultiRouteAgent *pRouteAgent = krcb->getCoordCB()->getRouteAgent();

      MsgHeader interruptMsg;
      interruptMsg.messageLength = sizeof( MsgHeader );
      interruptMsg.opCode = MSG_BS_INTERRUPTE;
      interruptMsg.TID = cb->getTID();
      interruptMsg.routeID.value = 0;

      rtnCoordSendRequestToNodesWithOutReply((void *)(&interruptMsg), routeSet,
                                             pRouteAgent );
      PD_TRACE_EXIT ( SDB_RTNCOINTERRUPT_SENDINTER ) ;
   }
}
