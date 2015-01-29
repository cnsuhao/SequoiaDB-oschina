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

   Source File Name = aggrGroup.hpp

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
#ifndef _SDB_MONGO_DEFINITION_HPP_
#define _SDB_MONGO_DEFINITION_HPP_

#include "util.hpp"
#include "../../bson/bson.hpp"

enum mongoOption
{
   dbReply       = 1,
   dbUpdate      = 1000,
   dbInsert      = 2002,
   dbQuery       = 2004,
   dbGetMore     = 2005,
   dbDelete      = 2006,
   dbKillCursors = 2007,
} ;

enum insertOption
{
   INSERT_CONTINUE_ON_ERROR = 1 << 0,
} ;

enum removeOption
{
   REMOVE_JUSTONE   = 1 << 0,
   REMOVE_BROADCASE = 1 << 1,
} ;

enum updateOption
{
   UPDATE_UPSERT    = 1 << 0,
   UPDATE_MULTI     = 1 << 1,
   UPDATE_BROADCAST = 1 << 2,
} ;

enum queryOption
{
   QUERY_CURSOR_TAILABLE   = 1 << 1,
   QUERY_SLAVE_OK          = 1 << 2,
   QUERY_OPLOG_REPLAY      = 1 << 3,
   QUERY_NO_CURSOR_TIMEOUT = 1 << 4,
   QUERY_AWAIT_DATA        = 1 << 5,
   QUERY_EXHAUST           = 1 << 6,
   QUERY_PARTIAL_RESULTS   = 1 << 7,
   QUERY_ALL_SUPPORTED     = QUERY_CURSOR_TAILABLE | QUERY_SLAVE_OK |
                             QUERY_OPLOG_REPLAY | QUERY_NO_CURSOR_TIMEOUT |
                             QUERY_AWAIT_DATA | QUERY_EXHAUST |
   QUERY_PARTIAL_RESULTS,
} ;

enum ResultFlag
{
   RESULT_CURSOR_NOT_FOUND   = 1,
   RESULT_ERRSET             = 2,
   RESULT_SHARD_CONFIG_STALE = 4,
   RESULT_AWAIT_CAPABLE      = 8,
};

enum authState
{
   AUTH_NONE  = 0,
   AUTH_NONCE = 1,

   AUTH_FINISHED = 1 << 31,
} ;

struct mongoMsgHeader
{
   INT32 len ;
   INT32 id ;
   INT32 responseTo ;
   SINT16 opCode ;
   CHAR _flags ;
   CHAR _version ;
   INT32 reservedFlags ;
};

struct mongoMsgReply : mongoMsgHeader
{
   SINT64 cursorId;
   INT32 startingFrom;
   INT32 nReturned;
};

class mongoParser : public mongoMsgHeader
{
public:
   UINT32 nsLen ;
   UINT32 dbNameLen ;
   const CHAR *dbName ;

public:
   mongoParser() ;
   ~mongoParser() ;

   void init( const CHAR *in, const INT32 inLen ) ;
   void setEndian( BOOLEAN bigEndian ) ;

   void nextObj( bson::BSONObj &obj ) ;

   void skip( INT32 size )
   {
      _offset += size ;
   }

   void bsonReadBegin()
   {
      _nextObj = _dataStart + _offset ;
   }

   BOOLEAN more() const
   {
      return ( NULL != _nextObj ) ;
   }

   void reparse()
   {
      _offset = 0 ;
      _nextObj = NULL ;
      init( _dataStart, len ) ;
   }

   void readNumber( const UINT32 size, CHAR *out ) ;
   void extractMsg() ;

private:
   const CHAR *_dataStart ;
   const CHAR *_dataEnd ;
   const CHAR *_nextObj ;
   INT32 _offset ;
   BOOLEAN _bigEndian ;
} ;

#endif
