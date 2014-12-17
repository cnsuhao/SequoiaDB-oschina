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

   Source File Name = rtnCoordLob.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/08/2014  YW  Initial Draft
   Last Changed =

*******************************************************************************/

#ifndef RTN_COORDLOB_HPP_
#define RTN_COORDLOB_HPP_

#include "rtnCoordOperator.hpp"

namespace engine
{
   class rtnCoordOpenLob : public rtnCoordOperator
   {
   public:
      rtnCoordOpenLob(){}
      virtual ~rtnCoordOpenLob(){}
   public:
      virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                             CHAR **ppResultBuffer, pmdEDUCB *cb,
                             MsgOpReply &replyHeader,
                             BSONObj** ppErrorObj ) ;
   } ;

   class rtnCoordWriteLob : public rtnCoordOperator
   {
   public:
      rtnCoordWriteLob(){}
      virtual ~rtnCoordWriteLob(){}
   public:
      virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                             CHAR **ppResultBuffer, pmdEDUCB *cb,
                             MsgOpReply &replyHeader,
                             BSONObj** ppErrorObj ) ;
   } ;

   class rtnCoordReadLob : public rtnCoordOperator
   {
   public:
      rtnCoordReadLob(){}
      virtual ~rtnCoordReadLob(){}
   public:
      virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                             CHAR **ppResultBuffer, pmdEDUCB *cb,
                             MsgOpReply &replyHeader,
                             BSONObj** ppErrorObj ) ;
   } ;

   class rtnCoordCloseLob : public rtnCoordOperator
   {
   public:
      rtnCoordCloseLob(){}
      virtual ~rtnCoordCloseLob(){}
   public:
      virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                             CHAR **ppResultBuffer, pmdEDUCB *cb,
                             MsgOpReply &replyHeader,
                             BSONObj** ppErrorObj ) ;
   } ;

   class rtnCoordRemoveLob : public rtnCoordOperator
   {
   public:
      rtnCoordRemoveLob(){}
      virtual ~rtnCoordRemoveLob(){}
   public:
      virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                             CHAR **ppResultBuffer, pmdEDUCB *cb,
                             MsgOpReply &replyHeader,
                             BSONObj** ppErrorObj ) ;
   } ;
}

#endif

