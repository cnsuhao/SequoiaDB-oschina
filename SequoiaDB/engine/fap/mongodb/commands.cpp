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
#include "msg.hpp"
#include "../../bson/bson.hpp"

DECLARE_COMMAND_VAR( insert )
DECLARE_COMMAND_VAR( delete )
DECLARE_COMMAND_VAR( update )
DECLARE_COMMAND_VAR( query )
DECLARE_COMMAND_VAR( getMore )
DECLARE_COMMAND_VAR( killCursors )

DECLARE_COMMAND_VAR( createCS )
DECLARE_COMMAND_VAR( create )
DECLARE_COMMAND_VAR( drop )
DECLARE_COMMAND_VAR( count )
DECLARE_COMMAND_VAR( aggregate )
DECLARE_COMMAND_VAR( dropDatabase )

DECLARE_COMMAND_VAR( createIndexes )
DECLARE_COMMAND_VAR( deleteIndexes )
DECLARE_COMMAND_VAR( listIndexes )

DECLARE_COMMAND_VAR( getlasterror )
DECLARE_COMMAND_VAR( ismaster )

DECLARE_COMMAND_VAR( ping )

command::command( const CHAR *cmdName, const CHAR* secondName )
{
   commandMgr* mgr = commandMgr::instance() ;
   if ( NULL != mgr )
   {
      mgr->addCommand( cmdName, this ) ;
      _cmdName = cmdName ;
      if ( NULL != secondName )
      {
         mgr->addCommand( secondName, this ) ;
      }
   }
}

commandMgr* commandMgr::instance()
{
   static commandMgr mgr ;
   return &mgr ;
}

namespace fap {
   static bson::BSONObj emptyObj ;

   void setQueryFlags( const INT32 reservedFlags, SINT32 &flags )
   {
      flags |= FLG_QUERY_WITH_RETURNDATA ;

      if ( reservedFlags & QUERY_CURSOR_TAILABLE )
      {
      }

      if ( reservedFlags & QUERY_SLAVE_OK )
      {
         flags |= FLG_QUERY_SLAVEOK ;
      }

      if ( reservedFlags & QUERY_OPLOG_REPLAY )
      {
         flags |= FLG_QUERY_OPLOGREPLAY ;
      }

      if ( reservedFlags & QUERY_NO_CURSOR_TIMEOUT )
      {
         flags |= FLG_QUERY_NOCONTEXTTIMEOUT ;
      }

      if ( reservedFlags & QUERY_AWAIT_DATA )
      {
         flags |= FLG_QUERY_AWAITDATA ;
      }

      if ( reservedFlags & QUERY_EXHAUST )
      {
      }

      if ( reservedFlags & QUERY_PARTIAL_RESULTS )
      {
         flags |= FLG_QUERY_PARTIALREAD ;
      }
   }

   bson::BSONObj getCondObj( const bson::BSONObj& all )
   {
      bson::BSONObj cond ;
      const char* cmdStr = all.firstElementFieldName() ;
      if ( all.hasField( "$query" ) )
      {
         cond = all.getObjectField( "$query" ) ;
      }
      else if ( all.hasField( "query" ) )
      {
         cond = all.getObjectField( "query" ) ;
      }
      else
      {
         cond = all ;
      }

      return cond ;
   }

}

INT32 insertCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc            = SDB_OK ;
   INT32 nToSkip       = 0 ;
   INT32 nToReturned   = 0 ;
   MsgHeader *header   = NULL ;
   MsgOpInsert *insert = NULL ;
   command *cmd        = NULL ;
   bson::BSONObj obj ;
   bson::BSONObj subObj ;
   bson::BSONElement e ;
   std::vector< bson::BSONElement > objList ;
   std::vector< bson::BSONElement >::const_iterator cit ;
   std::string fullname ;

   if ( parser.withIndex )
   {
      cmd = commandMgr::instance()->findCommand( "createIndexes" ) ;
      if ( NULL == cmd )
      {
         rc = SDB_OPTION_NOT_SUPPORT ;
         parser.opType = OP_CMD_NOT_SUPPORTED ;
         goto error ;
      }

      rc = cmd->convertRequest( parser, sdbMsg ) ;

      goto done ;
   }

   parser.opType = OP_INSERT ;
   sdbMsg.reverse( sizeof ( MsgOpInsert ) ) ;
   sdbMsg.advance( sizeof ( MsgOpInsert ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_INSERT_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   insert = ( MsgOpInsert * )sdbMsg.data() ;
   insert->version = 0 ;
   insert->w = 0 ;
   insert->padding = 0 ;
   insert->flags = 0 ;

   if ( parser.reservedFlags & INSERT_CONTINUE_ON_ERROR )
   {
      insert->flags |= FLG_INSERT_CONTONDUP ;
   }

   parser.skip( parser.nsLen + 1 ) ;
   if ( parser.withCmd )
   {
      parser.skip( sizeof( nToSkip ) + sizeof( nToReturned ) ) ;
      if ( !parser.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      parser.nextObj( obj ) ;
      if ( !obj.getBoolField( "ordered" ) )
      {
         insert->flags |= FLG_INSERT_CONTONDUP ;
      }
      fullname += parser.csName ;
      fullname += "." ;
      fullname += obj.getStringField( "insert" ) ;
      sdbMsg.write( fullname.c_str() , fullname.length() + 1, TRUE ) ;
      insert->nameLength = fullname.length() ;
      e = obj.getField( "documents" ) ;
      if ( bson::Array != e.type() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      {
         bson::BSONObjIterator i(e.Obj());
         while( i.more() )
         {
            bson::BSONElement eb = i.next();
            sdbMsg.write( eb.Obj(), TRUE ) ;
         }
      }
   }
   else
   {
      insert->nameLength = parser.nsLen ;
      sdbMsg.write( parser.fullName, insert->nameLength + 1, TRUE ) ;

      while ( parser.more() )
      {
         parser.nextObj( obj ) ;
         sdbMsg.write( obj, TRUE ) ;
      }
   }

   sdbMsg.doneLen() ;

done:
   return rc ;
error:
   goto done ;
}

INT32 deleteCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc            = SDB_OK ;
   INT32 nToSkip       = 0 ;
   INT32 nToReturned   = 0 ;
   MsgHeader *header   = NULL ;
   MsgOpDelete *remove = NULL ;
   bson::BSONObj del ;
   bson::BSONObj cond ;
   bson::BSONObj hint ;
   std::string fullname ;
   bson::BSONObj obj ;
   bson::BSONObj subObj ;
   bson::BSONElement e ;
   std::vector< bson::BSONElement > objList ;
   std::vector< bson::BSONElement >::const_iterator cit ;

   parser.opType = OP_REMOVE ;
   sdbMsg.reverse( sizeof ( MsgOpDelete ) ) ;
   sdbMsg.advance( sizeof ( MsgOpDelete ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_DELETE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   remove = ( MsgOpDelete* )sdbMsg.data() ;
   remove->version = 0 ;
   remove->w = 0 ;
   remove->padding = 0 ;
   remove->flags = 0 ;

   if ( parser.reservedFlags & REMOVE_JUSTONE )
   {
      remove->flags |= FLG_DELETE_SINGLEREMOVE ;
   }

   parser.skip( parser.nsLen + 1 ) ;
   if ( parser.withCmd )
   {
      parser.skip( sizeof( nToSkip ) + sizeof( nToReturned ) ) ;
      if ( !parser.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      parser.nextObj( obj ) ;
      fullname += parser.csName ;
      fullname += "." ;
      fullname += obj.getStringField( "delete" ) ;
      sdbMsg.write( fullname.c_str() , fullname.length() + 1, TRUE ) ;
      remove->nameLength = fullname.length() ;
      e = obj.getField( "deletes" ) ;
      if ( bson::Array != e.type() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      objList = e.Array() ;
      cit = objList.begin() ;
      while ( objList.end() != cit )
      {
         subObj = (*cit).Obj() ;
         del = subObj.getObjectField( "q" ) ;
         cond = fap::getCondObj( del ) ;
         hint = del.getObjectField( "$hint" ) ;
         sdbMsg.write( cond, TRUE ) ;
         sdbMsg.write( hint, TRUE ) ;
         ++cit ;
      }
   }
   else
   {
      remove->nameLength = parser.nsLen ;
      if ( !parser.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      parser.nextObj( cond ) ;

      if ( parser.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      cond = del.getObjectField( "$query" ) ;
      hint = del.getObjectField( "$hint" ) ;

      sdbMsg.write( parser.fullName, remove->nameLength + 1, TRUE ) ;
      sdbMsg.write( del, TRUE ) ;
      sdbMsg.write( hint, TRUE ) ;
   }

   sdbMsg.doneLen() ;

done:
   return rc ;
error:
   goto done ;
}

INT32 updateCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc            = SDB_OK ;
   INT32 nToSkip       = 0 ;
   INT32 nToReturned   = 0 ;
   INT32 updateFlags   = 0 ;
   MsgHeader *header   = NULL ;
   MsgOpUpdate *update = NULL ;
   bson::BSONObj all ;
   bson::BSONObj cond ;
   bson::BSONObj updator ;
   bson::BSONObj hint ;

   std::string fullname ;
   bson::BSONObj obj ;
   bson::BSONObj subObj ;
   bson::BSONElement e ;
   std::vector< bson::BSONElement > objList ;
   std::vector< bson::BSONElement >::const_iterator cit ;

   parser.opType = OP_UPDATE ;
   sdbMsg.reverse( sizeof ( MsgOpUpdate ) ) ;
   sdbMsg.advance( sizeof ( MsgOpUpdate ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_UPDATE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   update = ( MsgOpUpdate * )sdbMsg.data() ;
   update->version = 0 ;
   update->w = 0 ;
   update->padding = 0 ;
   update->flags = 0 ;

   parser.skip( parser.nsLen + 1 ) ;
   if ( parser.withCmd )
   {
      parser.skip( sizeof( nToSkip ) + sizeof( nToReturned ) ) ;
      if ( !parser.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      parser.nextObj( obj ) ;
      fullname += parser.csName ;
      fullname += "." ;
      fullname += obj.getStringField( "update" ) ;
      sdbMsg.write( fullname.c_str() , fullname.length() + 1, TRUE ) ;
      update->nameLength = fullname.length() ;
      e = obj.getField( "updates" ) ;
      if ( bson::Array != e.type() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      objList = e.Array() ;
      cit = objList.begin() ;
      while ( objList.end() != cit )
      {
         subObj = (*cit).Obj() ;
         all= subObj.getObjectField( "q" ) ;// u , multi upsert

         cond = fap::getCondObj( all ) ;

         updator = subObj.getObjectField( "u" ) ;

         hint = all.getObjectField( "$hint" ) ;
         BOOLEAN tmp = 0 ;
         tmp = subObj.getBoolField( "multi" ) ;
         if ( tmp )
         {
            update->flags |= FLG_UPDATE_MULTIUPDATE ;
         }
         tmp = subObj.getBoolField( "upsert" ) ;
         if ( tmp )
         {
            update->flags |= FLG_UPDATE_UPSERT ;
         }
         sdbMsg.write( cond, TRUE ) ;
         sdbMsg.write( updator, TRUE ) ;
         sdbMsg.write( hint, TRUE ) ;
         ++cit ;
      }
   }
   else
   {
      update->nameLength = parser.nsLen ;
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
      parser.nextObj( all ) ;

      if ( !parser.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      parser.nextObj( updator ) ;

      cond = fap::getCondObj( all ) ;
      hint = all.getObjectField( "$hint" ) ;

      sdbMsg.write( parser.fullName, update->nameLength + 1, TRUE ) ;
      sdbMsg.write( cond, TRUE ) ;
      sdbMsg.write( updator, TRUE ) ;
      sdbMsg.write( hint, TRUE ) ;
   }

   sdbMsg.doneLen() ;

done:
   return rc ;
error:
   goto done ;
}

INT32 queryCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc           = SDB_OK ;
   INT32 nToSkip      = 0 ;
   INT32 nToReturn    = -1 ;
   MsgHeader *header  = NULL ;
   MsgOpQuery *query  = NULL ;
   const CHAR *cmdStr = NULL ;
   command* cmd       = NULL ;
   bson::BSONObj all ;
   bson::BSONObj cond ;
   bson::BSONObj selector ;
   bson::BSONObj orderby ;
   bson::BSONObj hint ;
   bson::BSONObj fieldToReturn ;

   parser.opType = OP_QUERY ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg.data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, query->flags ) ;

   query->nameLength = parser.nsLen ;
   parser.skip( query->nameLength + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   query->numToSkip = nToSkip ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   query->numToReturn = (0 == nToReturn ? -1 : nToReturn) ;
   if ( 0 > nToReturn )
   {
      query->numToReturn = -nToReturn ;
   }

   if ( parser.more() )
   {
      parser.nextObj( all ) ;
   }

   if ( parser.withIndex )
   {
      cmd = commandMgr::instance()->findCommand( "listIndexes" ) ;
      if ( NULL == cmd )
      {
         rc = SDB_OPTION_NOT_SUPPORT ;
         parser.opType = OP_CMD_NOT_SUPPORTED ;
         goto error ;
      }
      parser.reparse() ;
      sdbMsg.zero() ;

      rc = cmd->convertRequest( parser, sdbMsg ) ;
      goto done ;
   }

   if ( parser.withCmd )
   {
      cmdStr = all.firstElementFieldName() ;
      cmd = commandMgr::instance()->findCommand( cmdStr ) ;
      if ( NULL == cmd )
      {
         rc = SDB_OPTION_NOT_SUPPORT ;
         parser.opType = OP_CMD_NOT_SUPPORTED ;
         parser.cmdName = cmdStr ;
         goto error ;
      }
      parser.reparse() ;
      sdbMsg.zero() ;

      rc = cmd->convertRequest( parser, sdbMsg ) ;
      goto done ;
   }

   cond = fap::getCondObj( all ) ;
   orderby = all.getObjectField( "orderby" ) ;
   hint    = cond.getObjectField( "$hint" ) ;

   if ( all.hasField( "limit" ) )
   {
      query->numToReturn = cond.getIntField( "limit" ) ;
   }

   if ( all.hasField( "skip" ) )
   {
      query->numToSkip   = cond.getIntField( "skip" ) ;
   }

   if ( cond.getBoolField("$explain") )
   {
      query->flags |= FLG_QUERY_EXPLAIN ;
   }

   if ( parser.more() )
   {
      parser.nextObj( fieldToReturn ) ;
   }

   sdbMsg.write( parser.fullName, query->nameLength + 1, TRUE ) ;
   sdbMsg.write( cond, TRUE ) ;
   sdbMsg.write( fieldToReturn, TRUE ) ;
   sdbMsg.write( orderby, TRUE ) ;
   sdbMsg.write( hint, TRUE ) ;

   sdbMsg.doneLen() ;

done:
   return rc ;
error:
   goto done ;
}

INT32 getMoreCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc           = SDB_OK ;
   INT32 nToReturn    = -1 ;
   INT64 cursorid     = 0 ;
   MsgHeader *header  = NULL ;
   MsgOpGetMore *more = NULL ;

   parser.opType = OP_GETMORE ;
   sdbMsg.reverse( sizeof ( MsgOpGetMore ) ) ;
   sdbMsg.advance( sizeof ( MsgOpGetMore ) ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_GETMORE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   more = ( MsgOpGetMore * )sdbMsg.data() ;

   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   more->numToReturn = (0 == nToReturn ? -1 : nToReturn) ;
   if ( 0 > nToReturn )
   {
      more->numToReturn = -nToReturn ;
   }
   parser.readNumber( sizeof( SINT64 ), ( CHAR * )&cursorid ) ;
   more->contextID = cursorid - 1;

   sdbMsg.doneLen() ;

   return rc ;
}

INT32 killCursorsCommand::convertRequest( mongoParser &parser,
                                          msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   INT32 nContext    = 0 ;
   INT32 nToReturn   = 0 ;
   SINT64 cursorid   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpKillContexts *kill = NULL ;

   parser.opType = OP_KILLCURSORS ;
   sdbMsg.reverse( sizeof ( MsgOpKillContexts ) ) ;
   sdbMsg.advance( sizeof ( MsgOpKillContexts ) - sizeof( SINT64 ) ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_KILL_CONTEXT_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   kill = ( MsgOpKillContexts * )sdbMsg.data() ;
   kill->ZERO = 0 ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nContext ) ;
   kill->numContexts = nToReturn ;

   while ( nToReturn > 0 )
   {
      parser.readNumber( sizeof( SINT64 ), ( CHAR * )&cursorid ) ;
      if ( cursorid != 0 )
      {
         cursorid -= 1 ;
         sdbMsg.write( ( CHAR * )&cursorid, sizeof( SINT64 ) ) ;
      }
      --nToReturn ;
   }

   sdbMsg.doneLen() ;

   return rc ;
}


/*
INT32 getnonceCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc = SDB_OK ;

   parser.opType = OP_CMD_GETNONCE ;
   static Nonce::Security security ;
   UINT64 nonce = security.getNonce() ;

   bson::BSONObjBuilder obj ;
   std::stringstream ss ;
   ss << std::hex << nonce ;
   obj.append( "nonce", ss.str() ) ;
   sdbMsg.write( obj.obj(), TRUE ) ;


done:
   return rc ;
error:
   goto done ;
}
*/

INT32 createCSCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObj obj ;
   const std::string cmdStr = "$create collectionspace" ;

   parser.opType = OP_CMD_CREATE_CS ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg.data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, query->flags ) ;

   query->nameLength = cmdStr.length() ;
   query->numToSkip = 0 ;
   query->numToReturn = -1 ;

   obj = BSON( "Name" << parser.csName << "PageSize" << 65536 ) ;

   sdbMsg.write( cmdStr.c_str(), query->nameLength + 1, TRUE ) ;
   sdbMsg.write( obj, TRUE ) ;

   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;

   sdbMsg.doneLen() ;

   return rc ;
}

INT32 createCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   INT32 nToSkip     = 0 ;
   INT32 nToReturn   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj obj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$create collection" ;
   std::string fullname ;


   parser.opType = OP_CMD_CREATE ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg.data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, query->flags ) ;

   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   if ( parser.withCmd )
   {
      fullname = parser.csName ;
      parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
      query->numToSkip = 0 ;
      parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
      query->numToReturn = (0 == nToReturn ? -1 : nToReturn) ;
      if ( 0 > nToReturn )
      {
         query->numToReturn = -nToReturn ;
      }

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

   sdbMsg.write( cmdStr.c_str(), query->nameLength + 1, TRUE ) ;
   obj = BSON( "Name" << fullname.c_str() ) ;

   sdbMsg.write( obj, TRUE ) ;

   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;

   sdbMsg.doneLen() ;

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
   bson::BSONObj obj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$drop collection" ;
   std::string fullname ;

   parser.opType = OP_CMD_DROP ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg.data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, query->flags ) ;

   fullname = parser.csName ;
   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   query->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   query->numToReturn = (0 == nToReturn ? -1 : nToReturn) ;
   if ( 0 > nToReturn )
   {
      query->numToReturn = -nToReturn ;
   }

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }
   fullname += "." ;
   fullname += cond.getStringField( "drop" ) ;

   sdbMsg.write( cmdStr.c_str(), query->nameLength + 1, TRUE ) ;
   obj = BSON( "Name" << fullname.c_str() ) ;

   sdbMsg.write( obj, TRUE ) ;

   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;

   sdbMsg.doneLen() ;

   return rc ;
}

INT32 countCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   INT32 nToSkip     = 0 ;
   INT32 nToReturn   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObj all ;
   bson::BSONObj queryObj ;
   bson::BSONObj orderby ;
   bson::BSONObj fields ;
   bson::BSONObj obj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$get count" ;
   std::string fullname ;

   parser.opType = OP_CMD_COUNT ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg.data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, query->flags ) ;

   fullname = parser.csName ;

   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   query->numToSkip = nToSkip ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   query->numToReturn = nToReturn ;

   if ( parser.more() )
   {
      parser.nextObj( all ) ;
   }

   fullname += "." ;
   fullname += all.getStringField( "count" ) ;

   query->nameLength = cmdStr.length() ;
   sdbMsg.write( cmdStr.c_str(), query->nameLength + 1, TRUE ) ;
   obj = BSON( "Collection" << fullname.c_str() ) ;

   queryObj = fap::getCondObj( all ) ;
   orderby = all.getObjectField( "sort" ) ;
   if ( all.hasField( "limit" ) )
   {
      query->numToReturn = all.getIntField( "limit" ) ;
   }

   if ( all.hasField( "skip" ) )
   {
      query->numToSkip = all.getIntField( "skip" ) ;
   }

   sdbMsg.write( queryObj, TRUE ) ;
   sdbMsg.write( fields, TRUE ) ;
   sdbMsg.write( orderby, TRUE ) ;
   sdbMsg.write( obj, TRUE ) ;

   sdbMsg.doneLen() ;

   return rc ;
}

INT32 aggregateCommand::convertRequest( mongoParser &parser,
                                        msgBuffer &sdbMsg )
{
   INT32 rc             = SDB_OK ;
   INT32 nToSkip        = 0 ;
   INT32 nToReturn      = 0 ;
   MsgHeader *header    = NULL ;
   MsgOpAggregate *aggr = NULL ;
   bson::BSONObj cond ;
   bson::BSONElement e ;
   std::string fullname ;

   parser.opType = OP_CMD_AGGREGATE ;
   sdbMsg.reverse( sizeof ( MsgOpAggregate ) ) ;
   sdbMsg.advance( sizeof ( MsgOpAggregate ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_AGGREGATE_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   aggr = ( MsgOpAggregate * )sdbMsg.data() ;
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

   aggr->nameLength = fullname.length() ;
   sdbMsg.write( fullname.c_str(), aggr->nameLength + 1, TRUE ) ;


   sdbMsg.write( cond, TRUE ) ;
   sdbMsg.doneLen() ;

   return rc ;
}

INT32 dropDatabaseCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   INT32 rc          = SDB_OK ;
   INT32 nToSkip     = 0 ;
   INT32 nToReturn   = 0 ;
   MsgHeader *header = NULL ;
   MsgOpQuery *query = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj obj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$drop collectionspace" ;

   parser.opType = OP_CMD_DROP_DATABASE ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   query = ( MsgOpQuery * )sdbMsg.data() ;
   query->version = 0 ;
   query->w = 0 ;
   query->padding = 0 ;
   query->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, query->flags ) ;

   query->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   query->nameLength = cmdStr.length() ;
   sdbMsg.write( cmdStr.c_str(), query->nameLength + 1, TRUE ) ;
   obj = BSON( "Name" << parser.csName ) ;

   query->numToSkip = nToSkip ;
   query->numToReturn = nToReturn ;

   sdbMsg.write( obj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;

   sdbMsg.doneLen() ;

   return rc ;
}

INT32 createIndexesCommand::convertRequest( mongoParser &parser,
                                          msgBuffer &sdbMsg )
{
   INT32 rc           = SDB_OK ;
   INT32 nToSkip      = 0 ;
   INT32 nToReturn    = 0 ;
   MsgHeader *header  = NULL ;
   MsgOpQuery *index  = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj subObj ;
   bson::BSONElement e ;
   std::vector< bson::BSONElement > objList ;
   std::vector< bson::BSONElement >::const_iterator cit ;
   bson::BSONObjBuilder obj ;
   bson::BSONObjBuilder indexobj ;
   const std::string cmdStr = "$create index" ;
   std::string fullname ;

   parser.opType = OP_ENSURE_INDEX ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   index = ( MsgOpQuery * )sdbMsg.data() ;
   index->version = 0 ;
   index->w = 0 ;
   index->padding = 0 ;
   index->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, index->flags ) ;

   fullname = parser.csName ;
   parser.skip( parser.nsLen + 1 ) ;

   if ( parser.withCmd )
   {
      parser.skip( sizeof( nToSkip ) + sizeof( nToReturn ) ) ;
      index->numToSkip = 0 ;
      index->numToReturn = nToReturn <= 0 ? -1 : nToReturn ;
      if ( !parser.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }//createIndexes

      parser.nextObj( cond ) ;

      index->nameLength = cmdStr.length() ;
      sdbMsg.write( cmdStr.c_str(), index->nameLength + 1, TRUE ) ;
      fullname += "." ;
      fullname += cond.getStringField( "createIndexes" ) ;
      obj.append( "Collection", fullname.c_str() ) ;
      e = cond.getField( "indexes" ) ;
      if ( bson::Array != e.type() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      objList = e.Array() ;
      cit = objList.begin() ;
      while ( objList.end() != cit )
      {
         subObj = (*cit).Obj() ;
         indexobj.append( "key", subObj.getObjectField( "key" ) ) ;
         indexobj.append( "name", subObj.getStringField( "name" ) ) ;
         indexobj.append("unique", subObj.getBoolField( "unique" ) ) ;
         obj.append( "Index", indexobj.obj() ) ;
         sdbMsg.write( obj.obj(), TRUE ) ;
         ++cit ;
      }
   }
   else
   {
      parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
      index->numToSkip = 0 ;
      parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
      index->numToReturn = (0 == nToReturn ? -1 : nToReturn) ;
      if ( 0 > nToReturn )
      {
         index->numToReturn = -nToReturn ;
      }
      index->nameLength = cmdStr.length() ;
      sdbMsg.write( cmdStr.c_str(), index->nameLength + 1, TRUE ) ;

      if ( !parser.more() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      parser.nextObj( cond ) ;
      e = cond.getField( "documents" ) ;
      if ( bson::Array != e.type() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      objList = e.Array() ;
      cit = objList.begin() ;
      while ( objList.end() != cit )
      {
         subObj = (*cit).Obj() ;
         obj.append( "Collection", subObj.getStringField( "ns" ) ) ;
         indexobj.append( "key", subObj.getObjectField( "key" ) ) ;
         indexobj.append( "name", subObj.getStringField( "name" ) ) ;
         indexobj.append("unique", subObj.getBoolField( "unique" ) ) ;
         obj.append( "Index", indexobj.obj() ) ;
         sdbMsg.write( obj.obj(), TRUE ) ;
         ++cit ;
      }
   }
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.doneLen() ;

done:
   return rc ;
error:
   goto done ;
}

INT32 deleteIndexesCommand::convertRequest( mongoParser &parser,
                                          msgBuffer &sdbMsg )
{
   INT32 rc              = SDB_OK ;
   INT32 nToSkip         = 0 ;
   INT32 nToReturn       = 0 ;
   MsgHeader *header     = NULL ;
   MsgOpQuery *dropIndex = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj obj ;
   bson::BSONObjBuilder indexObj ;
   bson::BSONElement e ;
   const std::string cmdStr = "$drop index" ;
   std::string fullname ;

   parser.opType = OP_CMD_DROP_INDEX ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   dropIndex = ( MsgOpQuery * )sdbMsg.data() ;
   dropIndex->version = 0 ;
   dropIndex->w = 0 ;
   dropIndex->padding = 0 ;
   dropIndex->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, dropIndex->flags ) ;

   fullname = parser.csName ;
   dropIndex->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   dropIndex->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   dropIndex->numToReturn = (0 == nToReturn ? -1 : nToReturn) ;
   if ( 0 > nToReturn )
   {
      dropIndex->numToReturn = -nToReturn ;
   }

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   {
      std::string clname = cond.getStringField( "deleteIndexes" ) ;
      if ( clname.empty() )
      {
         clname = cond.getStringField( "dropIndexes" ) ;
         if ( clname.empty() )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      fullname += "." ;
      fullname += clname ;
   }

   indexObj.append( "", cond.getStringField( "index" ) ) ;
   obj = BSON( "Collection" << fullname.c_str() << "Index" << indexObj.obj() );

   sdbMsg.write( cmdStr.c_str(), dropIndex->nameLength + 1, TRUE ) ;
   sdbMsg.write( obj, TRUE ) ;

   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;

   sdbMsg.doneLen() ;

done:
   return rc ;
error:
   goto done ;
}

INT32 listIndexesCommand::convertRequest( mongoParser &parser,
                                         msgBuffer &sdbMsg )
{
   INT32 rc              = SDB_OK ;
   INT32 nToSkip         = 0 ;
   INT32 nToReturn       = 0 ;
   MsgHeader *header     = NULL ;
   MsgOpQuery *getIndex  = NULL ;
   bson::BSONObj cond ;
   bson::BSONObj indexObj ;
   bson::BSONObjBuilder obj ;
   const std::string cmdStr = "$get indexes" ;
   std::string fullname ;

   parser.opType = OP_CMD_GET_INDEX ;
   sdbMsg.reverse( sizeof ( MsgOpQuery ) ) ;
   sdbMsg.advance( sizeof ( MsgOpQuery ) - 4 ) ;

   header = ( MsgHeader * )sdbMsg.data() ;
   header->opCode = MSG_BS_QUERY_REQ ;
   header->TID = 0 ;
   header->routeID.value = 0 ;
   header->requestID = parser.id ;

   getIndex = ( MsgOpQuery * )sdbMsg.data() ;
   getIndex->version = 0 ;
   getIndex->w = 0 ;
   getIndex->padding = 0 ;
   getIndex->flags = 0 ;
   fap::setQueryFlags( parser.reservedFlags, getIndex->flags ) ;

   getIndex->nameLength = cmdStr.length() ;
   parser.skip( parser.nsLen + 1 ) ;

   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToSkip ) ;
   getIndex->numToSkip = 0 ;
   parser.readNumber( sizeof( INT32 ), ( CHAR * )&nToReturn ) ;
   getIndex->numToReturn = (0 == nToReturn ? -1 : nToReturn) ;
   if ( 0 > nToReturn )
   {
      getIndex->numToReturn = -nToReturn ;
   }

   if ( parser.more() )
   {
      parser.nextObj( cond ) ;
   }

   if ( parser.withIndex )
   {
      if ( cond.hasField( "index" ) )
      {
         indexObj = BSON( "indexDef.name" << cond.getStringField( "index" )  ) ;
      }
      obj.append( "Collection", cond.getStringField( "ns" ) ) ;
   }
   else if ( parser.withCmd )
   {
      parser.opType = OP_CMD_NOT_SUPPORTED ;
      parser.cmdName = cond.firstElementFieldName() ;

      fullname = parser.csName ;
      fullname += "." ;
      fullname += cond.getStringField( "listIndexes" ) ;
      obj.append( "Collection", fullname.c_str() ) ;
   }

   sdbMsg.write( cmdStr.c_str(), getIndex->nameLength + 1, TRUE ) ;
   sdbMsg.write( indexObj, TRUE ) ;

   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( fap::emptyObj, TRUE ) ;
   sdbMsg.write( obj.obj(), TRUE ) ;

   sdbMsg.doneLen() ;

   return rc ;
}

INT32 getlasterrorCommand::convertRequest( mongoParser &parser,
                                           msgBuffer &sdbMsg )
{
   parser.opType = OP_CMD_GETLASTERROR ;
   return SDB_OK ;
}

INT32 ismasterCommand::convertRequest( mongoParser &parser,
                                       msgBuffer &sdbMsg )
{
   parser.opType = OP_CMD_ISMASTER ;
   return SDB_OK ;
}

INT32 pingCommand::convertRequest( mongoParser &parser, msgBuffer &sdbMsg )
{
   parser.opType = OP_CMD_PING ;
   return SDB_OK ;
}
