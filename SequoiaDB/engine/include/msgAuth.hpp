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

   Source File Name = msgAuth.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/12/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef MSGAUTH_HPP_
#define MSGAUTH_HPP_
#include "core.hpp"
#include "msg.h"
#include "authDef.hpp"
#include "../bson/bson.h"

using namespace bson ;

namespace engine
{
   struct _MsgAuthReply
   {
      MsgInternalReplyHeader header ;
      _MsgAuthReply()
      {
         header.res = 0 ;
         header.header.messageLength = sizeof( _MsgAuthReply ) ;
         header.header.opCode = MSG_AUTH_VERIFY_RES ;
         header.header.routeID.value = MSG_INVALID_ROUTEID ;
         header.header.TID = 0 ;
         header.header.requestID = 0 ;
      }
   } ;
   typedef struct _MsgAuthReply MsgAuthReply ;

   struct _MsgAuthCrtReply
   {
      MsgInternalReplyHeader header ;
      _MsgAuthCrtReply()
      {
         header.res = 0 ;
         header.header.messageLength = sizeof(_MsgAuthCrtReply ) ;
         header.header.opCode = MSG_AUTH_CRTUSR_RES ;
         header.header.routeID.value = MSG_INVALID_ROUTEID ;
         header.header.TID = 0 ;
         header.header.requestID = 0 ;
      }
   } ;
   typedef struct _MsgAuthCrtReply MsgAuthCrtReply ;

   struct _MsgAuthDelReply
   {
      MsgInternalReplyHeader header ;
      _MsgAuthDelReply()
      {
         header.res = 0 ;
         header.header.messageLength = sizeof(_MsgAuthCrtReply ) ;
         header.header.opCode = MSG_AUTH_DELUSR_RES ;
         header.header.routeID.value = MSG_INVALID_ROUTEID ;
         header.header.TID = 0 ;
         header.header.requestID = 0 ;
      }
   } ;
   typedef struct _MsgAuthDelReply MsgAuthDelReply ;

   INT32 extractAuthMsg( MsgHeader *header, BSONObj &obj ) ;

   INT32 msgBuildAuthMsg( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *username,
                          const CHAR *password,
                          UINT64 reqID ) ;

}

#endif

