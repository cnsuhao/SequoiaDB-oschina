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

   Source File Name = mongoSession.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/27/2015  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef _SDB_MONGO_MSG_CONVERTER_HPP_
#define _SDB_MONGO_MSG_CONVERTER_HPP_

#include "util.hpp"
#include "dpsLogWrapper.hpp"
#include "rtnContextBuff.hpp"
#include "pmdSession.hpp"
#include "mongodef.hpp"

class mongoConverter ;

/*
   _mongoSession define
*/
class _mongoSession : public engine::pmdSession
{
public:
   _mongoSession( SOCKET fd, engine::IResource *resource ) ;
   virtual ~_mongoSession() ;

   virtual UINT64 identifyID() ;
   virtual INT32 getServiceType() const ;
   virtual engine::SDB_SESSION_TYPE sessionType() const ;

   virtual INT32 run() ;

protected:
   BOOLEAN _preProcessMsg( const mongoParser &parser,
                           engine::IResource *resource,
                           msgBuffer &msg ) ;
   INT32 _processMsg( const CHAR *pMsg, const INT32 len ) ;
   INT32 _onMsgBegin( MsgHeader *msg ) ;
   INT32 _onMsgEnd( INT32 result, MsgHeader *msg ) ;
   INT32 _reply( MsgOpReply *replyHeader, const CHAR *pBody, const INT32 len ) ;
   virtual void  _onAttach() ;
   virtual void  _onDetach() ;

private:
   void  resetBuffers() ;

private:
   mongoConverter         *_converter ;
   MsgOpReply              _replyHeader ;
   BOOLEAN                 _needReply ;
   engine::rtnContextBuf   _contextBuff ;
   BSONObj                 _errorInfo ;

   std::vector< msgBuffer* > _inBufferVec ;
   msgBuffer               _outBuffer ;
   engine::IResource      *_resource ;
} ;

typedef _mongoSession mongoSession ;

#endif // _SDB_MONGO_MSG_CONVERTER_HPP_
