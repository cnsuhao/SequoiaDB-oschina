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

   Source File Name = rtnCoordLobDispatcher.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/10/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_COORDMSGDISPATCHER_HPP_
#define RTN_COORDMSGDISPATCHER_HPP_

#include "coordDef.hpp"
#include "netMultiRouteAgent.hpp"

namespace engine
{
   class _rtnCoordLobDispatcher : public SDBObject
   {
   public:
      _rtnCoordLobDispatcher() ;
      virtual ~_rtnCoordLobDispatcher() ;

   public:
      struct msgOptions
      {
         BOOLEAN onlyPrimary ;
         BOOLEAN dispatchedByGroupID ;

         msgOptions()
         :onlyPrimary( FALSE ),
          dispatchedByGroupID( FALSE )
         {

         }

         msgOptions( BOOLEAN p1, BOOLEAN p2 )
         :onlyPrimary( p1 ),
          dispatchedByGroupID( p2 )
         {

         }

         void clear()
         {
            onlyPrimary = FALSE ;
            dispatchedByGroupID = FALSE ;
         }
      } ;
   public:
      INT32 wait4Reply( _pmdEDUCB *cb ) ;

      INT32 createMsg( INT32 opCode,
                       INT32 version,
                       const msgOptions &options,
                       const bson::BSONObj &obj ) ;

      _rtnCoordLobDispatcher &add( UINT32 groupID ,
                                   const void *data,
                                   UINT32 len ) ;

      _rtnCoordLobDispatcher &add( UINT32 groupID ) ;

      _rtnCoordLobDispatcher &add( MsgRouteID id ,
                                   const void *data,
                                   UINT32 len ) ;

      _rtnCoordLobDispatcher &add( const MsgRouteID &id ) ;

      void addDone() ;

      void setContextID( MsgRouteID id, SINT64 contextID ) ;

      void clear( BOOLEAN releaseReply = TRUE ) ;

      const std::vector<MsgOpReply *> &getReplyMsgs() const
      {
         return _replyBuf ;
      }

      const MsgOpReply *getFirstReply() const
      {
         return _replyBuf.empty() ? NULL : *( _replyBuf.begin() ) ;
      }

   private:
      INT32 _sendMsg( _pmdEDUCB *cb ) ;

      INT32 _getReplyWithRetry( _pmdEDUCB *cb ) ;
 
      INT32 _getReply( _pmdEDUCB *cb ) ;
   private:
      struct msgTuple
      {
         netIOVec bodies ;
         UINT32 totalLen ;
         SINT64 contextID ;

         msgTuple()
         :totalLen( 0 ),
          contextID( -1 )
         {

         }

         BOOLEAN isDone() const
         {
            return 0 < totalLen ;
         }
      } ;

   public:
      class cmp
      {
      public:
         BOOLEAN operator()( const MsgRouteID &l,
                             const MsgRouteID &r ) const
         {
            return l.value < r.value ;
         }
      } ;
   private:
      typedef std::map<MsgRouteID, msgTuple, cmp> MSG_TUPLES ;
   private:
      std::vector<MsgOpReply *> _replyBuf ;
      REQUESTID_MAP _sendMap ;
      MSG_TUPLES _tuples ;
      MsgOpLob _header ;
      bson::BSONObj _msgObj ;
      msgOptions _options ;
      UINT32 _retryTimes ;
      CHAR _alignBuf[sizeof( UINT32 )] ;
   } ;
   typedef class _rtnCoordLobDispatcher rtnCoordLobDispatcher ;
}

#endif

