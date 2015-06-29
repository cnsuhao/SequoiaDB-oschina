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
namespace fap
{
   namespace mongo
   {
      void buildIsMasterMsg( engine::IResource *resource,
                             engine::rtnContextBuf &buff )
      {
         bson::BSONObjBuilder bob ;
         bob.append( "ismaster", FALSE ) ;
         bob.append("msg", "isdbgrid");
         bob.append( "maxBsonObjectSize", 16777216 ) ;
         bob.append( "maxMessageSizeBytes", SDB_MAX_MSG_LENGTH ) ;
         bob.append( "maxWriteBatchSize", 16777216 ) ;
         bob.append( "localTime", 100 ) ;
         bob.append( "maxWireVersion", 2 ) ;
         bob.append( "minWireVersion", 2 ) ;
         buff = engine::rtnContextBuf( bob.obj() ) ;
      }

      void buildGetNonceMsg( engine::rtnContextBuf &buff )
      {
         bson::BSONObjBuilder bob ;
         static Nonce::Security security ;
         UINT64 nonce = security.getNonce() ;

         std::stringstream ss ;
         ss << std::hex << nonce ;
         bob.append( "nonce", ss.str() ) ;
         buff = engine::rtnContextBuf( bob.obj() ) ;
      }

      void buildGetLastErrorMsg( const bson::BSONObj &err,
                                 engine::rtnContextBuf &buff )
      {
         INT32 rc = SDB_OK ;
         bson::BSONObjBuilder bob ;
         rc = err.getIntField( OP_ERRNOFIELD ) ;
         bob.append( "ok", 1.0 ) ;
         bob.append( "code", rc ) ;
         bob.append( "errmsg", err.getStringField( OP_ERRDESP_FIELD) ) ;
         buff = engine::rtnContextBuf( bob.obj() ) ;
      }

      void buildNotSupportMsg( engine::rtnContextBuf &buff )
      {
         bson::BSONObjBuilder bob ;
         bob.append( "ok", 1.0 ) ;
         bob.append( "msg", "Sorry, the command has not support now" ) ;
         buff = engine::rtnContextBuf( bob.obj() ) ;
      }
   }
}
