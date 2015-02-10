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

   Source File Name = commands.cpp

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

DECLARE_COMMAND_VAR( createCS )
DECLARE_COMMAND_VAR( create )
DECLARE_COMMAND_VAR( drop )
DECLARE_COMMAND_VAR( count )
DECLARE_COMMAND_VAR( aggregate )

DECLARE_COMMAND_VAR( createIndex )
DECLARE_COMMAND_VAR( dropIndexes )
DECLARE_COMMAND_VAR( getIndexes )

DECLARE_COMMAND_VAR( getLastError )
DECLARE_COMMAND_VAR( ismaster )

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

INT32 insertCommand::convertRequest( mongoParser &parser,
                                     std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc            = SDB_OK ;
   MsgHeader *header   = NULL ;
   MsgOpInsert *insert = NULL ;
   command *cmd        = NULL ;
   msgBuffer *sdbMsg   = NULL ;
   bson::BSONObj obj ;

   if ( parser.withIndex )
   {
      cmd = commandMgr::instance()->findCommand( "createIndex" ) ;
      if ( NULL != cmd )
      {
         rc = SDB_OPTION_NOT_SUPPORT ;
         goto error ;
      }

      rc = cmd->convertRequest( parser, sdbMsgs ) ;

      goto done ;
   }

   sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }
   parser.opType = OP_INSERT ;
   sdbMsg->reverse( sizeof ( MsgOpInsert ) ) ;
   sdbMsg->advance( sizeof ( MsgOpInsert ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_INSERT_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   insert = ( MsgOpInsert * )sdbMsg->data() ;
   insert->version = 0 ;
   insert->w = 0 ;
   insert->padding = 0 ;
   insert->flags = 0 ;

   if ( parser.reservedFlags & INSERT_CONTINUE_ON_ERROR )
   {
      insert->flags |= FLG_INSERT_CONTONDUP ;
   }

   insert->nameLength = parser.nsLen ;
   sdbMsg->write( parser.fullName, insert->nameLength) ;

   parser.skip( insert->nameLength + 1 ) ;
   while ( parser.more() )
   {
      parser.nextObj( obj ) ;
      sdbMsg->write( obj, TRUE ) ;
   }

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 removeCommand::convertRequest( mongoParser &parser,
                                     std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc            = SDB_OK ;
   MsgHeader *header   = NULL ;
   MsgOpDelete *remove = NULL ;
   bson::BSONObj del ;
   bson::BSONObj hint ;

   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_REMOVE ;
   sdbMsg->reverse( sizeof ( MsgOpDelete ) ) ;
   sdbMsg->advance( sizeof ( MsgOpDelete ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_DELETE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   remove = ( MsgOpDelete* )sdbMsg->data() ;
   remove->version = 0 ;
   remove->w = 0 ;
   remove->padding = 0 ;
   remove->flags = 0 ;

   if ( parser.reservedFlags & REMOVE_JUSTONE )
   {
      remove->flags |= FLG_DELETE_SINGLEREMOVE ;
   }

   remove->nameLength = parser.nsLen ;

   parser.skip( remove->nameLength + 1 ) ;
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

   sdbMsg->write( parser.fullName, remove->nameLength ) ;
   sdbMsg->write( del, TRUE ) ;
   sdbMsg->write( hint, TRUE ) ;

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 updateCommand::convertRequest( mongoParser &parser,
                                     std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc            = SDB_OK ;
   INT32 updateFlags   = 0 ;
   MsgHeader *header   = NULL ;
   MsgOpUpdate *update = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj updater ;
   bson::BSONObj hint ;
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_UPDATE ;
   sdbMsg->reverse( sizeof ( MsgOpUpdate ) ) ;
   sdbMsg->advance( sizeof ( MsgOpUpdate ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_UPDATE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   update = ( MsgOpUpdate * )sdbMsg->data() ;
   update->version = 0 ;
   update->w = 0 ;
   update->padding = 0 ;
   update->flags = 0 ;

   update->nameLength = parser.nsLen ;

   parser.skip( update->nameLength + 1 ) ;
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

   sdbMsg->write( parser.fullName, update->nameLength ) ;
   sdbMsg->write( cond, TRUE ) ;
   sdbMsg->write( updater, TRUE ) ;
   sdbMsg->write( hint, TRUE ) ;

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 queryCommand::convertRequest( mongoParser &parser,
                                    std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc           = SDB_OK ;
   INT32 nToSkip      = 0 ;
   INT32 nToReturn    = 0 ;
   MsgHeader *header  = NULL ;
   MsgOpQuery *query  = NULL ;
   const CHAR *cmdStr = NULL ;
   command* cmd       = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj selector ;
   bson::BSONObj orderby ;
   bson::BSONObj hint ;
   bson::BSONObj fieldToReturn ;
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_QUERY ;
   sdbMsg->reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg->advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg->data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   query->nameLength = parser.nsLen ;
   parser.skip( query->nameLength + 1 ) ;

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

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   if ( parser.withCmd )
   {
      cmdStr = cond.firstElementFieldName() ;
      cmd = commandMgr::instance()->findCommand( cmdStr ) ;
      if ( NULL == cmd )
      {
         rc = SDB_OPTION_NOT_SUPPORT ;
         goto error ;
      }
      parser.reparse() ;
      sdbMsg->zero() ;

      SDB_OSS_DEL sdbMsg ;
      sdbMsg = NULL ;

      rc = cmd->convertRequest( parser, sdbMsgs ) ;
      goto done ;
   }

   orderby = cond.getObjectField( "orderby" ) ;
   orderby = removeField( orderby, "$hint" ) ;

   if ( parser.more() )
   {
      parser.nextObj( fieldToReturn ) ;
   }

   sdbMsg->write( parser.fullName, query->nameLength ) ;
   sdbMsg->write( cond, TRUE ) ;
   sdbMsg->write( fieldToReturn, TRUE ) ;
   sdbMsg->write( orderby, TRUE ) ;
   sdbMsg->write( hint, TRUE ) ;

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 getMoreCommand::convertRequest( mongoParser &parser,
                                      std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc           = SDB_OK ;
   INT32 nToReturn    = 0 ;
   INT64 cursorid     = 0 ;
   MsgHeader *header  = NULL ;
   MsgOpGetMore *more = NULL ;
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_GETMORE ;
   sdbMsg->reverse( sizeof ( MsgOpGetMore ) ) ;
   sdbMsg->advance( sizeof ( MsgOpGetMore ) ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_GETMORE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   more = ( MsgOpGetMore * )sdbMsg->data() ;

   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   more->numToReturn = nToReturn ;
   parser.readNumber( sizeof( SINT64 ), ( CHAR * )&cursorid ) ;
   more->contextID = cursorid ;

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 killCursorsCommand::convertRequest( mongoParser &parser,
                                          std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc          = SDB_OK ;
   INT32 nContext    = 0 ;
   INT32 nToReturn   = 0 ;
   SINT64 cursorid   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpKillContexts *kill = NULL ;
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_KILLCURSORS ;
   sdbMsg->reverse( sizeof ( MsgOpKillContexts ) ) ;
   sdbMsg->advance( sizeof ( MsgOpKillContexts ) - sizeof( SINT64 ) ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_KILL_CONTEXT_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   kill = ( MsgOpKillContexts * )sdbMsg->data() ;
   kill->ZERO = 0 ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nContext ) ;
   kill->numContexts = nToReturn ;

   while ( nToReturn > 0 )
   {
      parser.readNumber( sizeof( SINT64 ), ( CHAR * )&cursorid ) ;
      sdbMsg->write( ( CHAR * )&cursorid, sizeof( SINT64 ) ) ;
      --nToReturn ;
   }

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}


/*
INT32 getnonceCommand::convertRequest( mongoParser &parser,
                                       std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc = SDB_OK ;
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_CMD_GETNONCE ;
   static Nonce::Security security ;
   UINT64 nonce = security.getNonce() ;

   bson::BSONObjBuilder obj ;
   std::stringstream ss ;
   ss << std::hex << nonce ;
   obj.append( "nonce", ss.str() ) ;
   sdbMsg->write( obj.obj(), TRUE ) ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}
*/

INT32 createCSCommand::convertRequest( mongoParser &parser,
                                       std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc          = SDB_OK ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObjBuilder obj ;
   const std::string cmdStr = "$create collectionspace" ;
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_CMD_CREATE ;
   sdbMsg->reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg->advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_CAT_CREATE_COLLECTION_SPACE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg->data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   query->nameLength = cmdStr.length() ;
   query->numToSkip = 0 ;
   query->numToReturn = -1 ;

   obj.append( "Name", parser.csName ) ;
   obj.append( "PageSize", 0 ) ;

   sdbMsg->write( cmdStr.c_str(), cmdStr.length() + 1 ) ;
   sdbMsg->write( obj.obj(), TRUE ) ;

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 createCommand::convertRequest( mongoParser &parser,
                                     std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc          = SDB_OK ;
   INT32 nToSkip     = 0 ;
   INT32 nToReturn   = 0 ;
   command *cmd      = NULL ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObj cond ;
   bson::BSONObjBuilder obj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$create collection" ;
   std::string fullname ;
   msgBuffer *sdbMsg = NULL ; 

   cmd = commandMgr::instance()->findCommand( "createCS" ) ;
   if ( NULL == cmd )
   {
      rc = SDB_OPTION_NOT_SUPPORT ;
      goto error ;
   }

   rc = cmd->convertRequest( parser, sdbMsgs ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   parser.opType = OP_CMD_CREATE ;
   sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   sdbMsg->reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg->advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_CAT_CREATE_COLLECTION_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg->data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   if ( parser.withCmd )
   {
      fullname = parser.csName ;
      parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
      query->numToSkip = 0 ;
      parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
      query->numToReturn = -1 ;

      if ( parser.more() )
      {
         parser.nextObj( cond ) ;
      }

      e = cond.getField( "create" ) ;
      if ( !e.eoo() && parser.withCmd )
      {
         fullname += "." ;
         fullname += e.valuestr() ;
      }
   }
   else
   {
      query->numToSkip = 0 ;
      query->numToReturn = -1 ;
      fullname = parser.fullName ;
   }

   sdbMsg->write( cmdStr.c_str(), cmdStr.length() + 1 ) ;
   obj.append( "Name", fullname.c_str() ) ;

   sdbMsg->write( obj.obj(), TRUE ) ;
   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 dropCommand::convertRequest( mongoParser &parser,
                                   std::vector<msgBuffer*> &sdbMsgs )
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
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_CMD_DROP ;
   sdbMsg->reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg->advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_CAT_DROP_COLLECTION_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg->data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   fullname = parser.csName ;
   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   query->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   query->numToReturn = -1 ;

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }
   fullname += "." ;
   fullname += cond.getStringField( "drop" ) ;

   sdbMsg->write( cmdStr.c_str(), cmdStr.length() ) ;
   obj.append( "Name", fullname.c_str() ) ;

   sdbMsg->write( obj.obj(), TRUE ) ;

   header->messageLength = sdbMsg->size() ;

   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}


INT32 countCommand::convertRequest( mongoParser &parser,
                                    std::vector<msgBuffer*> &sdbMsgs )
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
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }
   parser.opType = OP_CMD_COUNT ;
   sdbMsg->reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg->advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg->data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;

   fullname = parser.csName ;

   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   fullname += "." ;
   fullname += cond.getStringField( "count" ) ;

   query->nameLength = cmdStr.length() ;
   sdbMsg->write( cmdStr.c_str(), cmdStr.length() ) ;
   obj.append( "Name", fullname.c_str() ) ;

   queryObj = cond.getObjectField( "query" ) ;
   orderby = queryObj.getObjectField( "sort" ) ;
   queryObj = removeField( queryObj, "sort" ) ;

   nToReturn = cond.getIntField( "limit" ) ;
   nToSkip   = cond.getIntField( "skip" ) ;
   query->numToSkip = nToSkip ;
   query->numToReturn = nToReturn ;

   sdbMsg->write( queryObj, TRUE ) ;
   sdbMsg->write( fields, TRUE ) ;
   sdbMsg->write( orderby, TRUE ) ;
   sdbMsg->write( obj.obj(), TRUE ) ;

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 aggregateCommand::convertRequest( mongoParser &parser,
                                        std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc             = SDB_OK ;
   INT32 nToSkip        = 0 ;
   INT32 nToReturn      = 0 ;
   MsgHeader *header    = NULL ;
   MsgOpAggregate *aggr = NULL ;
   bson::BSONObj cond ;
   bson::BSONElement e ;
   std::string fullname ;
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_CMD_AGGREGATE ;
   sdbMsg->reverse( sizeof ( MsgOpAggregate ) ) ;
   sdbMsg->advance( sizeof ( MsgOpAggregate ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_AGGREGATE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   aggr = ( MsgOpAggregate * )sdbMsg->data() ;
   aggr->version = 0 ;
   aggr->w = 0 ;
   aggr->padding = 0 ;
   aggr->flags = 0 ;

   fullname = parser.csName ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   fullname += "." ;
   fullname += cond.getStringField( "aggregate" ) ;
   cond = removeField( cond, "aggregate" ) ;

   aggr->nameLength = fullname.length() ;
   sdbMsg->write( fullname.c_str(), fullname.length() ) ;


   sdbMsg->write( cond, TRUE ) ;
   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 createIndexCommand::convertRequest( mongoParser &parser,
                                          std::vector<msgBuffer*> &sdbMsgs )
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
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_ENSURE_INDEX ;
   sdbMsg->reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg->advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   index = ( MsgOpQuery * )sdbMsg->data() ;
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
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   parser.nextObj( cond ) ;
   obj.append( "Collection", cond.getField( "ns" ) ) ;
   indexobj.append( "key", cond.getObjectField( "key" ) ) ;
   indexobj.append( "name", cond.getStringField( "name" ) ) ;
   indexobj.append("unique", cond.getBoolField( "unique" ) ) ;
   indexobj.append("enforce", false ) ;
   obj.append( "Index", indexobj.obj() ) ;

   index->nameLength = cmdStr.length() ;
   sdbMsg->write( cmdStr.c_str(), cmdStr.length() ) ;
   sdbMsg->write( obj.obj(), TRUE ) ;

   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 dropIndexesCommand::convertRequest( mongoParser &parser,
                                          std::vector<msgBuffer*> &sdbMsgs )
{
   INT32 rc              = SDB_OK ;
   INT32 nToSkip         = 0 ;
   INT32 nToReturn       = 0 ;
   MsgHeader *header     = NULL ;
   MsgOpQuery *dropIndex = NULL ;
   bson::BSONObj cond ;
   bson::BSONObjBuilder obj ;
   bson::BSONObjBuilder indexObj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$drop index" ;
   std::string fullname ;
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_CMD_DROP_INDEX ;
   sdbMsg->reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg->advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   dropIndex = ( MsgOpQuery * )sdbMsg->data() ;
   dropIndex->version = 0 ;
   dropIndex->w = 0 ;
   dropIndex->padding = 0 ;
   dropIndex->flags = 0 ;

   fullname = parser.csName ;
   dropIndex->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   dropIndex->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   dropIndex->numToReturn = -1 ;

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   fullname += "." ;
   fullname += cond.getField( "deleteIndexes" ) ;

   indexObj.append( "", cond.getStringField( "index" ) ) ;
   obj.append( "Collection", fullname.c_str() ) ;
   obj.append( "Index", indexObj.obj() ) ;

   sdbMsg->write( cmdStr.c_str(), cmdStr.length() ) ;
   sdbMsg->write( obj.obj(), TRUE ) ;
   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 getIndexesCommand::convertRequest( mongoParser &parser,
                                         std::vector<msgBuffer*> &sdbMsgs )
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
   msgBuffer *sdbMsg = SDB_OSS_NEW msgBuffer() ;
   if ( NULL == sdbMsg )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   parser.opType = OP_CMD_GET_INDEX ;
   sdbMsg->reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg->advance( sizeof ( MsgOpQuery ) - 1 ) ;

   header = ( MsgHeader * )sdbMsg->data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   getIndex = ( MsgOpQuery * )sdbMsg->data() ;
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

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   indexObj.append( "indexDef.name", cond.getStringField( "index" ) ) ;
   obj.append( "Collection", cond.getStringField( "ns" ) ) ;
   obj.append( "Index", indexObj.obj() ) ;

   sdbMsg->write( cmdStr.c_str(), cmdStr.length() ) ;
   sdbMsg->write( obj.obj(), TRUE ) ;
   header->messageLength = sdbMsg->size() ;
   sdbMsgs.push_back( sdbMsg ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 getLastErrorCommand::convertRequest( mongoParser &parser,
                                           std::vector<msgBuffer*> &sdbMsgs )
{
   parser.opType = OP_CMD_GETLASTERROR ;
   return SDB_OK ;
}

INT32 ismasterCommand::convertRequest( mongoParser &parser,
                                       std::vector<msgBuffer*> &sdbMsgs )
{
   parser.opType = OP_CMD_ISMASTER ;
   return SDB_OK ;
}
