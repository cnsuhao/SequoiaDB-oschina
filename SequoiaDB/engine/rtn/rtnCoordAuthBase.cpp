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

   Source File Name = rtnCoordAuthBase.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/12/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnCoordAuthBase.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "msgMessage.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "rtnCoordCommon.hpp"
#include "msgAuth.hpp"

using namespace bson ;

namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLEARQ, "clearQ" )
   static void clearQ( REPLY_QUE &queue )
   {
      PD_TRACE_ENTRY ( SDB_CLEARQ ) ;
      while ( !queue.empty() )
      {
         MsgInternalReplyHeader *tmp = ( MsgInternalReplyHeader * )
                                       (queue.front()) ;
         queue.pop();
         SDB_OSS_FREE ( tmp );
      }
      PD_TRACE_EXIT ( SDB_CLEARQ ) ;
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOAUTHBASE_FORWARD, "rtnCoordAuthBase::forward" )
   INT32 rtnCoordAuthBase::forward( CHAR *pReceiveBuffer, SINT32 packSize,
                                    CHAR **ppResultBuffer, pmdEDUCB *cb,
                                    MsgOpReply &replyHeader, INT32 msgType,
                                    BOOLEAN sWhenNoPrimary )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_RTNCOAUTHBASE_FORWARD ) ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      CoordCB *pCoordcb                = pKrcb->getCoordCB();
      netMultiRouteAgent *pRouteAgent  = pCoordcb->getRouteAgent();
      MsgHeader *header = (MsgHeader *)pReceiveBuffer ;
      header->routeID.value = 0 ;
      header->TID = cb->getTID() ;
      CoordGroupInfoPtr cata ;
      REQUESTID_MAP nodes ;
      REPLY_QUE replyQue ;
      NodeID curNodeID = pmdGetNodeID() ;
      BOOLEAN hasRetry = FALSE ;

      BSONObj authObj ;
      BSONElement user, pass ;
      rc = extractAuthMsg( (MsgHeader*)pReceiveBuffer, authObj ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to extrace auth msg, "
                   "rc: %d", rc ) ;
      user = authObj.getField( SDB_AUTH_USER ) ;
      pass = authObj.getField( SDB_AUTH_PASSWD ) ;

   retry:
      rc = rtnCoordGetCatGroupInfo( cb, hasRetry ? TRUE : FALSE, cata ) ;
      PD_RC_CHECK ( rc, PDWARNING, "Failed to get catalog group info, "
                    "rc = %d", rc  ) ;

      nodes.clear() ;
      rc = rtnCoordSendRequestToPrimary( pReceiveBuffer,
                                         cata, nodes,
                                         pRouteAgent,
                                         MSG_ROUTE_CAT_SERVICE,
                                         cb ) ;
      if ( SDB_OK != rc )
      {
         if ( !hasRetry )
         {
            hasRetry = TRUE ;
            goto retry ;
         }

         if ( sWhenNoPrimary )
         {
            rc = rtnCoordSendRequestToOne( pReceiveBuffer, cata,
                                           nodes, pRouteAgent,
                                           MSG_ROUTE_CAT_SERVICE,
                                           cb ) ;
            PD_RC_CHECK ( rc, PDERROR, "Can not find a available cata node, "
                          "rc = %d", rc ) ;
         }
         else
         {
            PD_RC_CHECK ( rc, PDERROR, "Can not find the priamry, rc = %d",
                          rc ) ;
         }
      }

      rc = rtnCoordGetReply( cb, nodes, replyQue, msgType ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to get reply from catalog for auth, "
                    "rc = %d", rc ) ;

      if ( !replyQue.empty() )
      {
         MsgInternalReplyHeader *res = ( MsgInternalReplyHeader * )
                                       ( replyQue.front() ) ;
         if ( SDB_CLS_NOT_PRIMARY == res->res && !hasRetry )
         {
            clearQ( replyQue ) ;
            hasRetry = TRUE ;
            goto retry ;
         }
         else
         {
            rc = res->res ;
         }
      }
      else
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR, "Empty reply is received" ) ;
      }

      if ( SDB_OK != rc )
      {
         goto error ;
      }
      else
      {
         cb->setUserInfo( user.valuestrsafe(), pass.valuestrsafe() ) ;
      }

    done:
      msgBuildReplyMsgHeader( replyHeader,
                              sizeof(replyHeader),
                              header->opCode,
                              rc,
                              -1, 0, 0,
                              curNodeID,
                              header->requestID ) ;
      clearQ( replyQue ) ;
      PD_TRACE_EXITRC ( SDB_RTNCOAUTHBASE_FORWARD, rc ) ;
      return rc ;
   error:
      rtnCoordClearRequest( cb, nodes );
      goto done ;
   }

}

