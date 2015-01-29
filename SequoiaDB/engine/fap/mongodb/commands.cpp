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
#include "commands.hpp"
#include "mongoConverter.hpp"
#include "msg.h"
#include "../../bson/bsonmisc.h"
#include "../../bson/bsonobjbuilder.h"

DECLARE_COMMAND_VAR( insert )
DECLARE_COMMAND_VAR( remove )
DECLARE_COMMAND_VAR( update )
DECLARE_COMMAND_VAR( query )
DECLARE_COMMAND_VAR( getMore )
DECLARE_COMMAND_VAR( killCursors )

DECLARE_COMMAND_VAR( create )
DECLARE_COMMAND_VAR( drop )
DECLARE_COMMAND_VAR( count )
DECLARE_COMMAND_VAR( aggregate )

DECLARE_COMMAND_VAR( createIndex )
DECLARE_COMMAND_VAR( dropIndexes )
DECLARE_COMMAND_VAR( getIndexes )

DECLARE_COMMAND_VAR( getLastError )

command::command( const CHAR *cmdName )
{
   commandMgr::instance()->addCommand( cmdName, this ) ;
   _cmdName = cmdName ;
}

commandMgr* commandMgr::instance()
{
   static commandMgr mgr ;
   return &mgr ;
}

INT32 insertCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc            = SDB_OK ;
   MsgHeader *header   = NULL ;
   MsgOpInsert *insert = NULL ;
   command *cmd        = NULL ;
   bson::BSONObj obj ;

   if ( ossStrstr( parser.dbName, ".system.indexes" ) )
   {
      cmd = commandMgr::instance()->findCommand( "createIndex" ) ;
      if ( NULL != cmd )
      {
         rc = SDB_OPTION_NOT_SUPPORT ;
         goto error ;
      }
      parser.reparse() ;
      sdbMsg.zero() ;

      cmd->convertRequest( parser, sdbMsg ) ;

   }

   sdbMsg.reverse( sizeof ( MsgOpInsert ) ) ;
   sdbMsg.advance( sizeof ( MsgOpInsert ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_INSERT_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   insert = ( MsgOpInsert * )&sdbMsg ;
   insert->version = 0 ;
   insert->w = 0 ;
   insert->padding = 0 ;
   insert->flags = 0 ;

   if ( parser.reservedFlags & INSERT_CONTINUE_ON_ERROR )
   {
      insert->flags |= FLG_INSERT_CONTONDUP ;
   }

   insert->nameLength = parser.dbNameLen ;
   sdbMsg.write( parser.dbName, parser.dbNameLen ) ;

   parser.skip( parser.dbNameLen + 1 ) ;
   while ( parser.more() )
   {
      parser.nextObj( obj ) ;
      sdbMsg.write( obj.objdata(), obj.objsize() ) ;
   }

   header->messageLength = sdbMsg.size() ;
done:
   return rc ;
error:
   goto done ;
}

INT32 removeCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc            = SDB_OK ;
   MsgHeader *header   = NULL ;
   MsgOpDelete *remove = NULL ;
   bson::BSONObj del ;
   bson::BSONObj hint ;

   sdbMsg.reverse( sizeof ( MsgOpDelete ) ) ;
   sdbMsg.advance( sizeof ( MsgOpDelete ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_DELETE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   remove = ( MsgOpDelete* )&sdbMsg ;
   remove->version = 0 ;
   remove->w = 0 ;
   remove->padding = 0 ;
   remove->flags = 0 ;

   if ( parser.reservedFlags & REMOVE_JUSTONE )
   {
      remove->flags |= FLG_DELETE_SINGLEREMOVE ;
   }

   remove->nameLength = parser.dbNameLen ;

   parser.skip( parser.dbNameLen + 1 ) ;
   if ( !parser.more() )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   parser.nextObj( del ) ;

   if ( parser.more() )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   hint = del.getObjectField( "$hint" ) ;
   hint = removeField( hint, "$hint" ) ;

   sdbMsg.write( parser.dbName, parser.dbNameLen ) ;
   sdbMsg.write( del.objdata(), del.objsize() ) ;
   sdbMsg.write( hint.objdata(), hint.objsize() ) ;

   header->messageLength = sdbMsg.size() ;
done:
   return rc ;
error:
   goto done ;
}

INT32 updateCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc            = SDB_OK ;
   INT32 updateFlags   = 0 ;
   MsgHeader *header   = NULL ;
   MsgOpUpdate *update = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj updater ;
   bson::BSONObj hint ;

   sdbMsg.reverse( sizeof ( MsgOpUpdate ) ) ;
   sdbMsg.advance( sizeof ( MsgOpUpdate ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_UPDATE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   update = ( MsgOpUpdate * )&sdbMsg ;
   update->version = 0 ;
   update->w = 0 ;
   update->padding = 0 ;
   update->flags = 0 ;

   update->nameLength = parser.dbNameLen ;

   parser.skip( parser.dbNameLen + 1 ) ;
   parser.readNumber( sizeof( INT32 ), (CHAR *)&updateFlags ) ;
   if ( updateFlags & UPDATE_UPSERT )
   {
      update->flags |= FLG_UPDATE_UPSERT ;
   }

   if ( updateFlags & UPDATE_MULTI )
   {
      update->flags |= FLG_UPDATE_MULTIUPDATE ;
   }

   if ( !parser.more() )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   parser.nextObj( cond ) ;

   if ( !parser.more() )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   parser.nextObj( updater ) ;

   hint = cond.getObjectField( "$hint" ) ;
   hint = removeField( hint, "$hint" ) ;

   sdbMsg.write( parser.dbName, parser.dbNameLen ) ;
   sdbMsg.write( cond.objdata(), cond.objsize() ) ;
   sdbMsg.write( updater.objdata(), updater.objsize() ) ;
   sdbMsg.write( hint.objdata(), hint.objsize() ) ;

   header->messageLength = sdbMsg.size() ;
done:
   return rc ;
error:
   goto done ;
}

INT32 queryCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc           = SDB_OK ;
   INT32 nToSkip      = 0 ;
   INT32 nToReturn    = 0 ;
   MsgHeader *header  = NULL ;
   MsgOpQuery *query  = NULL ;
   const CHAR *ptr    = NULL ;
   const CHAR *cmdStr = NULL ;
   command* cmd       = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj selector ;
   bson::BSONObj orderby ;
   bson::BSONObj hint ;
   bson::BSONObj fieldToReturn ;

   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )&sdbMsg ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   query->nameLength = parser.dbNameLen ;
   parser.skip( parser.dbNameLen + 1 ) ;

   if ( parser.reservedFlags & QUERY_CURSOR_TAILABLE )
   {
   }

   if ( parser.reservedFlags & QUERY_SLAVE_OK )
   {
      query->flags |= FLG_QUERY_SLAVEOK ;
   }

   if ( parser.reservedFlags & QUERY_OPLOG_REPLAY )
   {
      query->flags |= FLG_QUERY_OPLOGREPLAY ;
   }

   if ( parser.reservedFlags & QUERY_NO_CURSOR_TIMEOUT )
   {
      query->flags |= FLG_QUERY_NOCONTEXTTIMEOUT ;
   }

   if ( parser.reservedFlags & QUERY_AWAIT_DATA )
   {
      query->flags |= FLG_QUERY_AWAITDATA ;
   }

   if ( parser.reservedFlags & QUERY_EXHAUST )
   {
   }

   if ( parser.reservedFlags & QUERY_PARTIAL_RESULTS )
   {
      query->flags |= FLG_QUERY_PARTIALREAD ;
   }

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   query->numToSkip = nToSkip ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   query->numToReturn = nToReturn ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   ptr = ossStrstr( parser.dbName, ".$cmd" ) ;
   if ( NULL != ptr )
   {
      cmdStr = cond.firstElementFieldName() ;

      ptr = ossStrstr( cmdStr, "getlasterror" ) ;
      if ( NULL == ptr )
      {
         ptr = ossStrstr( cmdStr, "getLastError" ) ;
      }

      if ( NULL != ptr )
      {
         cmdStr = "getLastError" ;
      }

      cmd = commandMgr::instance()->findCommand( cmdStr ) ;
      if ( NULL == cmd )
      {
         rc = SDB_OPTION_NOT_SUPPORT ;
         goto error ;
      }
      parser.reparse() ;
      sdbMsg.zero() ;

      cmd->convertRequest( parser, sdbMsg ) ;

      return rc ;
   }

   orderby = cond.getObjectField( "orderby" ) ;
   orderby = removeField( orderby, "$hint" ) ;

   if ( !parser.more() )
   {
      parser.nextObj( fieldToReturn ) ;
   }

   sdbMsg.write( parser.dbName, parser.dbNameLen ) ;
   sdbMsg.write( cond.objdata(), cond.objsize() ) ;
   sdbMsg.write( fieldToReturn.objdata(), fieldToReturn.objsize() ) ;
   sdbMsg.write( orderby.objdata(), orderby.objsize() ) ;
   sdbMsg.write( hint.objdata(), hint.objsize() ) ;

   header->messageLength = sdbMsg.size() ;
done:
   return rc ;
error:
   goto done ;
}

INT32 getMoreCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc           = SDB_OK ;
   INT32 nToReturn    = 0 ;
   INT64 cursorid     = 0 ;
   MsgHeader *header  = NULL ;
   MsgOpGetMore *more = NULL ;

   sdbMsg.reverse( sizeof ( MsgOpGetMore ) ) ;
   sdbMsg.advance( sizeof ( MsgOpGetMore ) ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_GETMORE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   more = ( MsgOpGetMore * )&sdbMsg ;

   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   more->numToReturn = nToReturn ;
   parser.readNumber( sizeof( SINT64 ), ( CHAR * )&cursorid ) ;
   more->contextID = cursorid ;

   header->messageLength = sdbMsg.size() ;

   return rc ;
}

INT32 killCursorsCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   INT32 nContext    = 0 ;
   INT32 nToReturn   = 0 ;
   SINT64 cursorid   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpKillContexts *kill = NULL ;

   sdbMsg.reverse( sizeof ( MsgOpKillContexts ) ) ;
   sdbMsg.advance( sizeof ( MsgOpKillContexts ) - sizeof( SINT64 ) ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_KILL_CONTEXT_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   kill = ( MsgOpKillContexts * )&sdbMsg ;
   kill->ZERO = 0 ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nContext ) ;
   kill->numContexts = nToReturn ;

   while ( nToReturn > 0 )
   {
      parser.readNumber( sizeof( SINT64 ), ( CHAR * )&cursorid ) ;
      sdbMsg.write( ( CHAR * )&cursorid, sizeof( SINT64 ) ) ;
      --nToReturn ;
   }

   header->messageLength = sdbMsg.size() ;

   return rc ;
}


/*
CONVERT_ERROR getnonceCommand::parse( mongoParser &parser, fixedStream &sdbMsg )
{
   CONVERT_ERROR rc  = CON_OK ;
   MsgHeader *header = NULL ;

   sdbMsg.reverse( sizeof ( MsgAuthentication ) ) ;
   sdbMsg.advance( sizeof ( MsgAuthentication ) ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_AUTH_VERIFY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   header->messageLength = sdbMsg.size() ;
done:
   return rc ;
error:
   goto done ;
}

INT32 getnonceCommand::handle( fixedStream &sdbMsg, fixedStream &sdbResult )
{
   INT32 reservedFlags = RESULT_AWAIT_CAPABLE ;
   sdbResult.reverse( sizeof( MsgOpReply ) ) ;
   sdbResult.advance( sizeof( MsgOpReply ) ) ;
   MsgOpReply *replyTo = ( MsgOpReply *)&sdbResult ;
   MsgHeader  *header  = ( MsgHeader *)&sdbMsg ;
   
   header->messageLength = sizeof( MsgOpReply ) ;
   header->opCode = MSG_BS_QUERY_RES ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = header->requestID ;

   replyTo->contextID = 0 ;
   replyTo->flags = 0 ;
   replyTo->startFrom = 0 ;
   replyTo->numReturned = 1 ;

   return SDB_OK ;
}

CONVERT_ERROR getnonceCommand::convertResponse( fixedStream &sdbResult, fixedStream &result )
{
   result.zero() ;
   INT32 resultFlag = RESULT_AWAIT_CAPABLE ;

   mongoMsgHeader *mongoHeader = ( mongoMsgHeader * )&result ;
   MsgHeader *header = ( MsgHeader * )&sdbResult ;

   mongoHeader->opCode = dbReply ;
   mongoHeader->id = 0 ;
   mongoHeader->responseTo = header->requestID ;
   mongoHeader->_flags = 0 ;
   mongoHeader->_version = 0 ;

   result.advance( sizeof( mongoMsgHeader ) ) ;
   result.write( ( CHAR * )&resultFlag, sizeof( INT32 ) ) ;


   return CON_OK ;
}

CONVERT_ERROR authenticateCommand::parse( mongoParser &parser, fixedStream &sdbMsg )
{
   CONVERT_ERROR rc  = CON_OK ;
   INT32 nToSkip     = 0 ;
   INT32 nToReturn   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpQuery *auth  = NULL ;
   CHAR *ptr         = NULL ;
   bson::BSONObj cond ;
   bson::BSONObjBuilder obj ;

   sdbMsg.reverse( sizeof ( MsgAuthentication ) ) ;
   sdbMsg.advance( sizeof ( MsgAuthentication ) ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_AUTH_VERIFY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   parser.skip( parser.nsLen + 1 ) ;
   parser.skip( sizeof( INT32 ) ) ;
   parser.skip( sizeof( INT32 ) ) ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   header->messageLength = sdbMsg.size() ;
done:
   return rc ;
error:
   goto done ;
}

INT32 authenticateCommand::handle( fixedStream &sdbMsg, fixedStream &sdbResult )
{
   INT32 reservedFlags = RESULT_AWAIT_CAPABLE ;
   sdbResult.reverse( sizeof( MsgOpReply ) ) ;
   sdbResult.advance( sizeof( MsgOpReply ) ) ;
   MsgOpReply *replyTo = ( MsgOpReply *)&sdbResult ;
   MsgHeader  *header  = ( MsgHeader *)&sdbMsg ;

   header->messageLength = sizeof( MsgOpReply ) ;
   header->opCode = MSG_AUTH_VERIFY_RES ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = header->requestID ;

   replyTo->contextID = 0 ;
   replyTo->flags = 0 ;
   replyTo->startFrom = 0 ;
   replyTo->numReturned = 1 ;

   return SDB_OK ;
}

CONVERT_ERROR authenticateCommand::convertResponse( fixedStream &sdbResult, fixedStream &result )
{
   INT32 resultFlag = RESULT_AWAIT_CAPABLE ;
   bson::BSONObjBuilder obj ;

   result.zero() ;

   mongoMsgHeader *mongoHeader = ( mongoMsgHeader * )&result ;
   MsgHeader *header = ( MsgHeader * )&sdbResult ;

   mongoHeader->opCode = dbReply ;
   mongoHeader->id = 0 ;
   mongoHeader->responseTo = header->requestID ;
   mongoHeader->_flags = 0 ;
   mongoHeader->_version = 0 ;

   result.advance( sizeof( mongoMsgHeader ) ) ;
   result.write( ( CHAR * )&resultFlag, sizeof( INT32 ) ) ;


   return CON_OK ;
}
*/
INT32 createCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   INT32 nToSkip     = 0 ;
   INT32 nToReturn   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObj cond ;
   bson::BSONObjBuilder obj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$create collection" ;
   std::string fullname ;

   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_CAT_CREATE_COLLECTION_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )&sdbMsg ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   fullname = parser.dbName ;
   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   query->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   query->numToReturn = -1 ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   e = cond.getField( "create" ) ;
   if ( !e.eoo() )
   {
      fullname += "." ;
      fullname += e.valuestr() ;
   }
   sdbMsg.write( cmdStr.c_str(), cmdStr.length() ) ;
   obj.append( "Name", fullname.c_str() ) ;

   sdbMsg.write( obj.obj().objdata(), obj.obj().objsize() ) ;

   header->messageLength = sdbMsg.size() ;

   return rc ;
}

INT32 dropCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   INT32 nToSkip     = 0 ;
   INT32 nToReturn   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObj cond ;
   bson::BSONObjBuilder obj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$drop collection" ;
   std::string fullname ;

   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_CAT_DROP_COLLECTION_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )&sdbMsg ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   fullname = parser.dbName ;
   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   query->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   query->numToReturn = -1 ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   e = cond.getField( "create" ) ;
   if ( !e.eoo() )
   {
      fullname += "." ;
      fullname += e.valuestr() ;
   }

   sdbMsg.write( cmdStr.c_str(), cmdStr.length() ) ;
   obj.append( "Name", fullname.c_str() ) ;

   sdbMsg.write( obj.obj().objdata(), obj.obj().objsize() ) ;

   header->messageLength = sdbMsg.size() ;

   return rc ;
}


INT32 countCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   INT32 nToSkip     = 0 ;
   INT32 nToReturn   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj queryObj ;
   bson::BSONObj orderby ;
   bson::BSONObj fields ;
   bson::BSONObjBuilder obj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$get count" ;
   std::string fullname ;

   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )&sdbMsg ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   fullname = parser.dbName ;
   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   e = cond.getField( "count" ) ;
   if ( !e.eoo() )
   {
      fullname += "." ;
      fullname += e.valuestr() ;
   }

   query->nameLength = cmdStr.length() ;
   sdbMsg.write( cmdStr.c_str(), cmdStr.length() ) ;
   obj.append( "Name", fullname.c_str() ) ;

   queryObj = cond.getObjectField( "query" ) ;
   orderby = queryObj.getObjectField( "sort" ) ;
   queryObj = removeField( queryObj, "sort" ) ;

   nToReturn = cond.getIntField( "limit" ) ;
   nToSkip   = cond.getIntField( "skip" ) ;
   query->numToSkip = nToSkip ;
   query->numToReturn = nToReturn ;

   sdbMsg.write( queryObj.objdata(), queryObj.objsize() ) ;
   sdbMsg.write( fields.objdata(), fields.objsize() ) ;
   sdbMsg.write( orderby.objdata(), orderby.objsize() ) ;
   sdbMsg.write( obj.obj().objdata(), obj.obj().objsize() ) ;

   header->messageLength = sdbMsg.size() ;

   return rc ;
}

INT32 aggregateCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc             = SDB_OK ;
   INT32 nToSkip        = 0 ;
   INT32 nToReturn      = 0 ;
   MsgHeader *header    = NULL ;
   MsgOpAggregate *aggr = NULL ;
   bson::BSONObj cond ;
   bson::BSONElement e ;
   std::string fullname ;

   sdbMsg.reverse( sizeof ( MsgOpAggregate ) ) ;
   sdbMsg.advance( sizeof ( MsgOpAggregate ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_AGGREGATE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   aggr = ( MsgOpAggregate * )&sdbMsg ;
   aggr->version = 0 ;
   aggr->w = 0 ;
   aggr->padding = 0 ;
   aggr->flags = 0 ;

   fullname = parser.dbName ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   e = cond.getField( "aggregate" ) ;
   if ( !e.eoo() )
   {
      fullname += "." ;
      fullname += e.valuestr() ;
   }
   cond = removeField( cond, "aggregate" ) ;

   aggr->nameLength = fullname.length() ;
   sdbMsg.write( fullname.c_str(), fullname.length() ) ;


   sdbMsg.write( cond.objdata(), cond.objsize() ) ;
   header->messageLength = sdbMsg.size() ;

   return rc ;
}

INT32 createIndexCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc           = SDB_OK ;
   INT32 nToSkip      = 0 ;
   INT32 nToReturn    = 0 ;
   MsgHeader *header  = NULL ;
   MsgOpQuery *index  = NULL ;
   bson::BSONObj cond ;
   bson::BSONObjBuilder obj ;
   bson::BSONObjBuilder indexobj ;
   const std::string cmdStr = "$create index" ;

   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   index = ( MsgOpQuery * )&sdbMsg ;
   index->version = 0 ;
   index->w = 0 ;
   index->padding = 0 ;
   index->flags = 0 ;

   parser.skip( parser.nsLen + 1 ) ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   index->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   index->numToReturn = -1 ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   obj.append( "Collection", cond.getField( "ns" ) ) ;
   indexobj.append( "key", cond.getObjectField( "key" ) ) ;
   indexobj.append( "name", cond.getStringField( "name" ) ) ;
   indexobj.append("unique", cond.getBoolField( "unique" ) ) ;
   indexobj.append("enforce", false ) ;
   obj.append( "Index", indexobj.obj() ) ;

   index->nameLength = cmdStr.length() ;
   sdbMsg.write( cmdStr.c_str(), cmdStr.length() ) ;
   sdbMsg.write( obj.obj().objdata(), obj.obj().objsize() ) ;

   header->messageLength = sdbMsg.size() ;

   return rc ;
}

INT32 dropIndexesCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc              = SDB_OK ;
   INT32 nToSkip         = 0 ;
   INT32 nToReturn       = 0 ;
   const CHAR *ptr       = NULL ;
   MsgHeader *header     = NULL ;
   MsgOpQuery *dropIndex = NULL ;
   bson::BSONObj cond ;
   bson::BSONObjBuilder obj ;
   bson::BSONObjBuilder indexObj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$drop index" ;
   std::string fullname ;

   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   dropIndex = ( MsgOpQuery * )&sdbMsg ;
   dropIndex->version = 0 ;
   dropIndex->w = 0 ;
   dropIndex->padding = 0 ;
   dropIndex->flags = 0 ;

   ptr = ossStrstr( parser.dbName, "." ) ;
   if ( NULL == ptr )
   {
      rc = SDB_INVALIDARG ;
      return rc ;
   }

   fullname = std::string(parser.dbName).substr( 0, ptr - parser.dbName ) ;
   dropIndex->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   dropIndex->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   dropIndex->numToReturn = -1 ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   e = cond.getField( "deleteIndexes" ) ;
   if ( !e.eoo() )
   {
      fullname += "." ;
      fullname += e.valuestr() ;
   }

   indexObj.append( "", cond.getStringField( "index" ) ) ;
   obj.append( "Collection", fullname.c_str() ) ;
   obj.append( "Index", indexObj.obj() ) ;

   sdbMsg.write( cmdStr.c_str(), cmdStr.length() ) ;
   sdbMsg.write( obj.obj().objdata(), obj.obj().objsize() ) ;
   header->messageLength = sdbMsg.size() ;

   return rc ;
}

INT32 getIndexesCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc              = SDB_OK ;
   INT32 nToSkip         = 0 ;
   INT32 nToReturn       = 0 ;
   MsgHeader *header     = NULL ;
   MsgOpQuery *getIndex  = NULL ;
   bson::BSONObj cond ;
   bson::BSONObjBuilder obj ;
   bson::BSONObjBuilder indexObj ;
   const std::string cmdStr = "$get indexes" ;
   std::string fullname ;

   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )&sdbMsg ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   getIndex = ( MsgOpQuery * )&sdbMsg ;
   getIndex->version = 0 ;
   getIndex->w = 0 ;
   getIndex->padding = 0 ;
   getIndex->flags = 0 ;

   getIndex->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   getIndex->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   getIndex->numToReturn = -1 ;

   if ( !parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   indexObj.append( "indexDef.name", cond.getStringField( "index" ) ) ;
   obj.append( "Collection", cond.getStringField( "ns" ) ) ;
   obj.append( "Index", indexObj.obj() ) ;

   sdbMsg.write( cmdStr.c_str(), cmdStr.length() ) ;
   sdbMsg.write( obj.obj().objdata(), obj.obj().objsize() ) ;
   header->messageLength = sdbMsg.size() ;

   return rc ;
}

INT32 getLastErrorCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   return SDB_OK ;
}
