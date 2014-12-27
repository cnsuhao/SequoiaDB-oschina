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

   Source File Name = rtnCoordAggregate.cpp

   Descriptive Name = Runtime Coord Aggregation

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   aggregation logic on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoordAggregate.hpp"
#include "msgMessage.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"

using namespace bson;
namespace engine
{
   INT32 rtnCoordAggregate::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                    CHAR **ppResultBuffer, pmdEDUCB *cb,
                                    MsgOpReply &replyHeader,
                                    BSONObj **ppErrorObj )
   {
      INT32 rc = SDB_OK;
      MsgHeader *pHeader = (MsgHeader *)pReceiveBuffer;
      CHAR *pCollectionName = NULL;
      CHAR *pObjs = NULL;
      INT32 count = 0;
      BSONObj objs;
      SINT64 contextID = -1;

      replyHeader.contextID = -1;
      replyHeader.flags = SDB_OK;
      replyHeader.numReturned = 0;
      replyHeader.startFrom = 0;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode = MSG_BS_AGGREGATE_RSP;
      replyHeader.header.requestID = pHeader->requestID;
      replyHeader.header.routeID.value = 0;
      replyHeader.header.TID = pHeader->TID;

      rc = msgExtractAggrRequest( pReceiveBuffer, &pCollectionName, &pObjs, count );
      PD_RC_CHECK( rc, PDERROR, "failed to parse aggregate request(rc=%d)", rc );

      try
      {
         objs = BSONObj( pObjs );
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( rc, PDERROR,
                     "failed to execute aggregate, received unexpecte error:%s",
                     e.what() );
      }
      rc = pmdGetKRCB()->getAggrCB()->build( objs, count, pCollectionName,
                                             cb, contextID );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to execute aggregation operation(rc=%d)",
                  rc );
      replyHeader.contextID = contextID;
   done:
      return rc;
   error:
      replyHeader.flags = rc;
      goto done;
   }
}
