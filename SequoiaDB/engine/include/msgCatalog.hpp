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

   Source File Name = msgCatalog.hpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/28/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef MSGCATALOG_HPP__
#define MSGCATALOG_HPP__

#pragma warning( disable: 4200 )

#include "core.hpp"
#include <map>
#include <string>
#include "../bson/bsonelement.h"
#include "oss.hpp"
#include "msg.hpp"
#include "netDef.hpp"
#include "dpsLogDef.hpp"
#include "clsDef.hpp"
using namespace std ;

#include "msgCatalogDef.h"

#define CAT_INVALID_VERSION               -1

#pragma pack(4)

namespace engine
{
   /************************************************
   * node register request message
   * data is a BsonObject:
   * {"Type":0, "Host":"vmsvr1",
   *    "Service":[{"Type":"0","Name":"cat"},
   *               {"Type":"1","Name":"repl"},
   *               {"Type":"2", "Name":"shard"}]}
   ************************************************/
   class _MsgCatRegisterMessage : public SDBObject
   {
   public :
      MsgHeader      header;
      BYTE           data[0];
      _MsgCatRegisterMessage()
      {
         header.messageLength = 0 ;
         header.TID = 0 ;
         header.opCode = MSG_CAT_REG_REQ ;
         header.requestID = 0 ;
         header.routeID.value = 0 ;
      }
   private :
      _MsgCatRegisterMessage ( _MsgCatRegisterMessage const & ) ;
      _MsgCatRegisterMessage& operator=(_MsgCatRegisterMessage const & ) ;
   } ;
   typedef class _MsgCatRegisterMessage MsgCatRegisterReq;

   /************************************************
   * response message of node register
   * the data is a BsonObject:
   * {"Type":0, "Host":"vmsvr1","GroupID":00000001,"NodeID":00000001,
   *    "Service":[{"Type":0,"Name":"cat"},
   *               {"Type":1,"Name":"repl"},
   *               {"Type":2, "Name":"shard"}]}
   ************************************************/
   class _MsgCatRegisterRsp : public SDBObject
   {
   public :
      _MsgInternalReplyHeader    header;
      BYTE                       data[0];
      _MsgCatRegisterRsp()
      {
         header.header.messageLength = 0 ;
         header.header.TID = 0 ;
         header.header.opCode = MSG_CAT_REG_RES ;
         header.header.requestID = 0 ;
         header.header.routeID.value = 0 ;
      }
   private :
      _MsgCatRegisterRsp ( _MsgCatRegisterRsp const & ) ;
      _MsgCatRegisterRsp& operator=( _MsgCatRegisterRsp const & ) ;
   } ;
   typedef class _MsgCatRegisterRsp MsgCatRegisterRsp;

   class _MsgCatGroupReq : public SDBObject
   {
   public :
      MsgHeader      header;
      MsgRouteID     id ;
      _MsgCatGroupReq()
      {
         header.messageLength = sizeof( _MsgCatGroupReq ) ;
         header.opCode = MSG_CAT_GRP_REQ ;
         header.requestID = 0 ;
         header.routeID.value = 0 ;
         header.TID = 0 ;
      }
   private :
      _MsgCatGroupReq ( _MsgCatGroupReq const & ) ;
      _MsgCatGroupReq& operator=( _MsgCatGroupReq const & ) ;
   } ;

   typedef class _MsgCatGroupReq MsgCatGroupReq ;

   class _MsgCatGroupRes : public SDBObject
   {
   public :
      MsgInternalReplyHeader header ;
      _MsgCatGroupRes()
      {
         header.header.opCode = MSG_CAT_GRP_RES ;
         header.header.TID = 0 ;
         header.header.requestID = 0 ;
         header.header.routeID.value = 0 ;
      }
   private :
      _MsgCatGroupRes ( _MsgCatGroupRes const & ) ;
      _MsgCatGroupRes& operator=( _MsgCatGroupRes const & ) ;
   } ;

   typedef class _MsgCatGroupRes MsgCatGroupRes ;

   INT32 msgParseCatGroupRes( const _MsgCatGroupRes *msg,
                              CLS_GROUP_VERSION &version,
                              string &groupName,
                              map<UINT64, _netRouteNode> &group,
                              UINT32 *pPrimary = NULL ) ;

   INT32 msgParseCatGroupObj( const CHAR* objdata,
                              CLS_GROUP_VERSION &version,
                              UINT32 &groupID,
                              string &groupName,
                              map<UINT64, _netRouteNode> &group,
                              UINT32 *pPrimary = NULL ) ;

   const CHAR* getShardServiceName ( bson::BSONElement &beService ) ;
   
   std::string getServiceName ( bson::BSONElement &beService, INT32 serviceType ) ;

   typedef _MsgCatGroupReq       MsgCatCatGroupReq ;
   typedef _MsgCatGroupRes       MsgCatCatGroupRes ;

   class _MsgCatPrimaryChange : public SDBObject
   {
   public :
      MsgHeader      header;
      MsgRouteID     newPrimary ;
      MsgRouteID     oldPrimary ;
      _MsgCatPrimaryChange()
      {
         header.messageLength = sizeof( _MsgCatPrimaryChange ) ;
         header.opCode = MSG_CAT_PAIMARY_CHANGE ;
         header.routeID.value = 0 ;
         header.requestID = 0 ;
         header.TID = 0 ;
         newPrimary.value = MSG_INVALID_ROUTEID ;
         oldPrimary.value = MSG_INVALID_ROUTEID ;
      }
   private :
      _MsgCatPrimaryChange ( _MsgCatPrimaryChange const & ) ;
      _MsgCatPrimaryChange& operator=( _MsgCatPrimaryChange const & ) ;
   } ;
   typedef class _MsgCatPrimaryChange MsgCatPrimaryChange ;

   class _MsgCatPrimaryChangeRes : public SDBObject
   {
   public :
      MsgInternalReplyHeader header ;
      _MsgCatPrimaryChangeRes()
      {
         header.header.messageLength = sizeof( _MsgCatPrimaryChangeRes ) ;
         header.header.opCode = MSG_CAT_PAIMARY_CHANGE_RES ;
         header.header.TID = 0 ;
         header.header.requestID = 0 ;
         header.header.routeID.value = 0 ;
      }
   private :
      _MsgCatPrimaryChangeRes ( _MsgCatPrimaryChangeRes const & ) ;
      _MsgCatPrimaryChangeRes& operator=( _MsgCatPrimaryChangeRes const & ) ;
   } ;
   typedef class _MsgCatPrimaryChangeRes MsgCatPrimaryChangeRes ;

   typedef MsgOpQuery MsgCatQueryCatReq;

   typedef MsgOpReply MsgCatQueryCatRsp;

   enum SDB_CAT_GROUP_STATUS
   {
      SDB_CAT_GRP_DEACTIVE = 0,
      SDB_CAT_GRP_ACTIVE
   } ;

   typedef MsgOpQuery MsgCatQueryTaskReq ;
   typedef MsgOpReply MsgCatQueryTaskRes ;

}
#pragma pack()

#endif // MSGCATALOG_HPP__
