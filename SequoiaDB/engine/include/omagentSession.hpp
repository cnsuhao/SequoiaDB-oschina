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

   Source File Name = omagentSession.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_SESSION_HPP_
#define OMAGENT_SESSION_HPP_

#include "omagent.hpp"
#include "pmdAsyncSession.hpp"
#include "netRouteAgent.hpp"

#include "../bson/bson.h"
using namespace bson ;

namespace engine
{

   class _omAgentNodeMgr ;

   /*
      _omaSession define
   */
   class _omaSession : public _pmdAsyncSession
   {
      DECLARE_OBJ_MSG_MAP()

      public:
         _omaSession ( UINT64 sessionID ) ;
         virtual ~_omaSession () ;

         virtual UINT64 identifyID() { return (UINT64)SDB_OK ; }
         virtual INT32 getServiceType() const { return SDB_OK ; }

         virtual SDB_SESSION_TYPE sessionType() const ;
         virtual EDU_TYPES eduType () const ;

         virtual void    onRecieve ( const NET_HANDLE netHandle,
                                     MsgHeader * msg ) ;
         virtual BOOLEAN timeout ( UINT32 interval ) ;

         virtual void    onTimer ( UINT64 timerID, UINT32 interval ) ;

      protected:
         virtual void   _onDetach () ;
         virtual void   _onAttach () ;
         virtual INT32  _defaultMsgFunc ( NET_HANDLE handle, MsgHeader* msg ) ;

      protected:
         INT32       _onNodeMgrReq( const NET_HANDLE &handle,
                                    MsgHeader *pMsg ) ;
         INT32       _onOMAgentReq( const NET_HANDLE &handle,
                                    MsgHeader *pMsg ) ;
         INT32       _onAuth( const NET_HANDLE &handle,
                              MsgHeader *pMsg ) ;

      protected:

         INT32 _reply ( MsgOpReply *header, const CHAR *pBody,
                        INT32 bodyLen ) ;
         INT32 _reply ( INT32 flags, MsgHeader *pSrcReqMsg ) ;

         INT32 _buildReplyHeader( MsgHeader *pMsg ) ;

      private:
         MsgOpReply       _replyHeader ;
         BSONObj          _errorInfo ;

         _omAgentNodeMgr      *_pNodeMgr ;

         ossTimestamp         _lastRecvTime ;

   } ;

   typedef _omaSession omaSession ;
}



#endif // OMAGENT_SESSION_HPP_
