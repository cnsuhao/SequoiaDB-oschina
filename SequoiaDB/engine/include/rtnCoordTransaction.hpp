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

   Source File Name = rtnCoordTransaction.hpp

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

#ifndef RTNCOORDTRANSACTION_HPP__
#define RTNCOORDTRANSACTION_HPP__

#include "rtnCoordOperator.hpp"

namespace engine
{
   class rtnCoordTransBegin : public rtnCoordOperator
   {
   public:
      virtual INT32 execute( CHAR * pReceiveBuffer, SINT32 packSize,
                           CHAR * * ppResultBuffer, pmdEDUCB * cb,
                           MsgOpReply & replyHeader,
                           BSONObj **ppErrorObj );
   };

   class rtnCoord2PhaseCommit : public rtnCoordOperator
   {
   public:
      virtual INT32 execute( CHAR * pReceiveBuffer, SINT32 packSize,
                           CHAR * * ppResultBuffer, pmdEDUCB * cb,
                           MsgOpReply & replyHeader,
                           BSONObj **ppErrorObj );

   private:
      virtual INT32 doPhase1( CHAR * pReceiveBuffer, SINT32 packSize,
                           CHAR * * ppResultBuffer, pmdEDUCB * cb,
                           MsgOpReply & replyHeader );

      virtual INT32 doPhase2( CHAR * pReceiveBuffer, SINT32 packSize,
                           CHAR * * ppResultBuffer, pmdEDUCB * cb,
                           MsgOpReply & replyHeader );

      virtual INT32 cancelOp( CHAR * pReceiveBuffer, SINT32 packSize,
                           CHAR * * ppResultBuffer, pmdEDUCB * cb,
                           MsgOpReply & replyHeader );

      virtual INT32 buildPhase1Msg( CHAR * pReceiveBuffer, CHAR **pMsg ) = 0;

      virtual INT32 buildPhase2Msg( CHAR * pReceiveBuffer, CHAR **pMsg ) = 0;

      virtual INT32 executeOnDataGroup ( CHAR *pMsg,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb ) = 0;
   };

   class rtnCoordTransCommit : public rtnCoord2PhaseCommit
   {
   public:
      virtual INT32 execute( CHAR * pReceiveBuffer, SINT32 packSize,
                           CHAR * * ppResultBuffer, pmdEDUCB * cb,
                           MsgOpReply & replyHeader,
                           BSONObj **ppErrorObj );

   private:
      virtual INT32 buildPhase1Msg( CHAR * pReceiveBuffer, CHAR **pMsg );

      virtual INT32 buildPhase2Msg( CHAR * pReceiveBuffer, CHAR **pMsg );

      virtual INT32 executeOnDataGroup ( CHAR *pMsg,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb );
   };

   class rtnCoordTransRollback : public rtnCoordOperator
   {
   public:
      virtual INT32 execute( CHAR * pReceiveBuffer, SINT32 packSize,
                           CHAR * * ppResultBuffer, pmdEDUCB * cb,
                           MsgOpReply & replyHeader,
                           BSONObj **ppErrorObj );

   private:
      virtual INT32 executeOnDataGroup ( CHAR *pMsg,
                                         netMultiRouteAgent *pRouteAgent,
                                         pmdEDUCB *cb );
   };
}

#endif
