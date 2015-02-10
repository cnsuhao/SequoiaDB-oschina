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

   Source File Name = mongoReplyHelper.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          02/05/2015  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#include "mongoReplyHelper.hpp"
#include "sdbInterface.hpp"
#include "../../bson/bsonobjbuilder.h"
#include "../../bson/lib/nonce.h"
#include "msgBuffer.hpp"
namespace fap
{
   namespace mongo
   {
      INT32 buildIsMasterMsg( engine::IResource *resource, msgBuffer &msg )
      {
         INT32 rc = SDB_OK ;
         bson::BSONObjBuilder bob ;

         bob.append( "ismaster", FALSE ) ;
         bob.append("msg", "isdbgrid");
         bob.append( "maxBsonObjectSize", resource ) ;
         bob.append( "maxMessageSizeBytes", SDB_MAX_MSG_LENGTH ) ;
         bob.append( "maxWriteBatchSize", resource ) ;
         bob.append( "localTime", 100 ) ;
         bob.append( "maxWireVersion", 2 ) ;
         bob.append( "minWireVersion", 2 ) ;

         msg.write( bob.obj(), TRUE ) ;
      error:
         return rc ;
      done:
         goto error ;
      }

      INT32 buildGetNonceMsg( msgBuffer &msg )
      {
         bson::BSONObjBuilder bob ;
         static Nonce::Security security ;
         UINT64 nonce = security.getNonce() ;

         std::stringstream ss ;
         ss << std::hex << nonce ;
         bob.append( "nonce", ss.str() ) ;
         msg.write( bob.obj(), TRUE ) ;

         return SDB_OK ;
      }

      INT32 buildNotSupportMsg( msgBuffer &msg )
      {
         INT32 rc = SDB_OK ;


      error:
         return rc ;
      done:
         goto error ;
      }
   }
}
