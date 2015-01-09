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

   Source File Name = qgmPlCommand.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains declare for QGM operators

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/09/2013  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "qgmPlCommand.hpp"
#include "rtn.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "msgMessage.hpp"
#include "qgmBuilder.hpp"
#include "rtnCoordTransaction.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"
#include <sstream>

using namespace bson ;

namespace engine
{
   _qgmPlCommand::_qgmPlCommand( INT32 type,
                                 const qgmDbAttr &fullName,
                                 const qgmField &indexName,
                                 const qgmOPFieldVec &indexColumns,
                                 const BSONObj &partition,
                                 BOOLEAN uniqIndex )
   :_qgmPlan( QGM_PLAN_TYPE_COMMAND, _qgmField() )
   {
      _commandType = type ;
      _fullName = fullName ;
      _indexName = indexName ;
      _indexColumns = indexColumns ;
      _uniqIndex = uniqIndex ;
      _contextID = -1 ;
      _initialized = TRUE ;
      if ( !partition.isEmpty() )
      {
         _partition = partition ;
      }
   }

   _qgmPlCommand::~_qgmPlCommand()
   {
      close() ;
   }

   void _qgmPlCommand::close()
   {
      _killContext() ;
      return ;
   }

   string _qgmPlCommand::toString() const
   {
      stringstream ss ;

      ss << "Type:" ;
      if ( SQL_GRAMMAR::CRTCS == _commandType )
      {
         ss << "create collectionspace" << '\n';
         ss << "Name:" << _fullName.toString() << '\n';
      }
      else if ( SQL_GRAMMAR::DROPCS == _commandType )
      {
         ss << "drop collectionspace" << '\n';
         ss << "Name:" << _fullName.toString() << '\n';
      }
      else if ( SQL_GRAMMAR::CRTCL == _commandType )
      {
         ss << "create collection" << '\n';
         ss << "Name:" << _fullName.toString() << '\n';
         if ( !_partition.isEmpty() )
         {
            ss << "Shardingkey:" << _partition.toString() << '\n';
         }
      }
      else if ( SQL_GRAMMAR::DROPCL == _commandType )
      {
         ss << "drop collection" ;
         ss << "Name:" << _fullName.toString() << '\n';
      }
      else if ( SQL_GRAMMAR::CRTINDEX == _commandType )
      {
         ss << "create index" ;
         ss << "Name:" << _fullName.toString() << '\n';
         ss << "Index:" << _indexName.toString() << '\n';
         ss << "Columns:" << qgmBuilder::buildOrderby( _indexColumns )
                               .toString() << '\n';
         if ( _uniqIndex )
         {
            ss <<  "Unique:true" << '\n';
         }
         else
         {
            ss <<  "Unique:false" << '\n';
         }
      }
      else if ( SQL_GRAMMAR::DROPINDEX == _commandType )
      {
         ss << "drop index" ;
         ss << "Name:" << _fullName.toString() << '\n';
         ss << "Index:" << _indexName.toString() << '\n';
      }
      else if ( SQL_GRAMMAR::LISTCL == _commandType )
      {
         ss << "list collections" << '\n';
      }
      else if ( SQL_GRAMMAR::LISTCS == _commandType )
      {
         ss << "list collectinospaces" << '\n';
      }
      else if ( SQL_GRAMMAR::BEGINTRAN == _commandType )
      {
         ss << "begin transaction" << '\n' ;
      }
      else if ( SQL_GRAMMAR::ROLLBACK == _commandType )
      {
         ss << "rollback\n" ;
      }
      else if ( SQL_GRAMMAR::COMMIT == _commandType )
      {
         ss <<"commit\n" ;
      }
      else
      {
      }

      return ss.str() ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLCOMMAND__EXEC, "_qgmPlCommand::_execute" )
   INT32 _qgmPlCommand::_execute( _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB__QGMPLCOMMAND__EXEC ) ;
      INT32 rc = SDB_OK ;
      SDB_ROLE role = pmdGetKRCB()->getDBRole();

      rc = SDB_ROLE_COORD == role?
           _executeOnCoord( eduCB ) : _executeOnData( eduCB ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMPLCOMMAND__EXEC, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLCOMMAND_EXECONCOORD, "_qgmPlCommand::_executeOnCoord" )
   INT32 _qgmPlCommand::_executeOnCoord( _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB__QGMPLCOMMAND_EXECONCOORD ) ;
      INT32 rc = SDB_OK ;

      CHAR *msg = NULL ;
      INT32 bufSize = 0 ;
      MsgOpReply dummyReply ;
      BSONObj *err = NULL ;

      if ( SQL_GRAMMAR::CRTCS == _commandType )
      {
         rtnCoordCMDCreateCollectionSpace coord ;
         BSONObj obj = BSON( FIELD_NAME_NAME << _fullName.toString() ) ;
         rc = msgBuildQueryMsg( &msg,
                                &bufSize,
                                string ( CMD_ADMIN_PREFIX
                                   CMD_NAME_CREATE_COLLECTIONSPACE ).c_str(),
                                0, 0, 0, -1, &obj ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                             dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DROPCS == _commandType )
      {
         rtnCoordCMDDropCollectionSpace coord ;
         BSONObj obj = BSON( FIELD_NAME_NAME << _fullName.toString() ) ;
         rc = msgBuildQueryMsg( &msg,
                                &bufSize,
                                string ( CMD_ADMIN_PREFIX
                                   CMD_NAME_DROP_COLLECTIONSPACE ).c_str(),
                                0, 0, 0, -1, &obj ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::CRTCL == _commandType )
      {
         rtnCoordCMDCreateCollection coord ;
         BSONObj obj ;
         if ( _partition.isEmpty() )
         {
            obj = BSON( FIELD_NAME_NAME << _fullName.toString() ) ;
         }
         else
         {
            obj = BSON( FIELD_NAME_NAME << _fullName.toString()
                        << FIELD_NAME_SHARDINGKEY
                        << _partition ) ;
         }
         rc = msgBuildQueryMsg( &msg,
                                &bufSize,
                                string ( CMD_ADMIN_PREFIX
                                   CMD_NAME_CREATE_COLLECTION ).c_str(),
                                0, 0, 0, -1, &obj ) ;

         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DROPCL == _commandType )
      {
         rtnCoordCMDDropCollection coord ;
         BSONObj obj = BSON( FIELD_NAME_NAME << _fullName.toString() ) ;
         rc = msgBuildQueryMsg( &msg,
                                &bufSize,
                                string ( CMD_ADMIN_PREFIX
                                   CMD_NAME_DROP_COLLECTION ).c_str(),
                                0, 0, 0, -1, &obj ) ;

         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::CRTINDEX == _commandType )
      {
         rtnCoordCMDCreateIndex coord ;
         BSONObjBuilder builder ;
         qgmOPFieldVec::const_iterator itr = _indexColumns.begin() ;
         for ( ; itr != _indexColumns.end(); itr++ )
         {
            builder.append( itr->value.attr().toString(),
                            SQL_GRAMMAR::ASC == itr->type?
                            1 : -1 ) ;
         }

         BSONObj index ;
         if ( !_uniqIndex )
         {
            index = BSON( "key"<<builder.obj()
                           <<"name"<<_indexName.toString()) ;
         }
         else
         {
            BSONObjBuilder indexBuilder ;
            indexBuilder.append( "key", builder.obj()) ;
            indexBuilder.append("name", _indexName.toString()) ;
            indexBuilder.appendBool( "unique", TRUE ) ;
            index = indexBuilder.obj() ;
         }

         BSONObj obj = BSON( FIELD_NAME_COLLECTION << _fullName.toString()
                             << FIELD_NAME_INDEX << index ) ;
         rc = msgBuildQueryMsg( &msg, &bufSize,
                                string ( CMD_ADMIN_PREFIX
                                         CMD_NAME_CREATE_INDEX ).c_str(),
                                0, 0, 0, -1, &obj ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DROPINDEX == _commandType )
      {
         rtnCoordCMDDropIndex coord ;
         BSONObj obj = BSON( FIELD_NAME_COLLECTION << _fullName.toString()
                             << FIELD_NAME_INDEX
                             << BSON("name" << _indexName.toString()
                                     << "key" << "" )) ;
         rc = msgBuildQueryMsg( &msg, &bufSize,
                                string ( CMD_ADMIN_PREFIX
                                         CMD_NAME_DROP_INDEX ).c_str(),
                                0, 0, 0, -1, &obj ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::LISTCS == _commandType )
      {
         rtnCoordCMDListCollectionSpace coord ;
         BSONObj obj ;
         rc = msgBuildQueryMsg( &msg, &bufSize,
                                string( CMD_ADMIN_PREFIX
                                      CMD_NAME_LIST_COLLECTIONSPACES).c_str(),
                                0, 0, 0, -1, &obj ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         _contextID = dummyReply.contextID ;
      }
      else if ( SQL_GRAMMAR::LISTCL == _commandType )
      {
         rtnCoordCMDListCollection coord ;
         BSONObj obj ;
         rc = msgBuildQueryMsg( &msg, &bufSize,
                                string( CMD_ADMIN_PREFIX
                                        CMD_NAME_LIST_COLLECTIONS).c_str(),
                                0, 0, 0, -1, &obj ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         _contextID = dummyReply.contextID ;
      }
      else if ( SQL_GRAMMAR::BEGINTRAN == _commandType )
      {
         rtnCoordTransBegin coord ;
         _MsgHeader transMsg ;
         transMsg.messageLength = sizeof( transMsg ) ;
         transMsg.opCode = MSG_BS_TRANS_BEGIN_REQ ;
         transMsg.TID = 0 ;
         transMsg.routeID.value = 0 ;

         rc = coord.execute( ( CHAR *)(&transMsg),
                              transMsg.messageLength,
                              NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::ROLLBACK == _commandType )
      {
         rtnCoordTransRollback coord ;
         rc = msgBuildTransRollbackMsg( &msg, &bufSize ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::COMMIT == _commandType )
      {
         rtnCoordTransCommit coord ;
         rc = msgBuildTransCommitMsg( &msg, &bufSize ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = coord.execute( msg, *(SINT32*)msg, NULL, eduCB,
                              dummyReply, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         PD_LOG( PDERROR, "invalid command type:%d", _commandType ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      if ( NULL != msg )
      {
         SDB_OSS_FREE( msg ) ;
         msg = NULL ;
      }
      PD_TRACE_EXITRC( SDB__QGMPLCOMMAND_EXECONCOORD, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLCOMMAND__EXECONDATA, "_qgmPlCommand::_executeOnData" )
   INT32 _qgmPlCommand::_executeOnData( _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB__QGMPLCOMMAND__EXECONDATA ) ;
      INT32 rc = SDB_OK ;
      pmdKRCB *pKrcb = pmdGetKRCB();
      SDB_DMSCB *dmsCB = pKrcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = pKrcb->getDPSCB() ;
      SDB_RTNCB *rtnCB = pKrcb->getRTNCB() ;
      BOOLEAN addInfo = eduCB->getType() == EDU_TYPE_SHARDAGENT ?
                        TRUE : FALSE ;

      if ( dpsCB && eduCB->isFromLocal() && !dpsCB->isLogLocal() )
      {
         dpsCB = NULL ;
      }

      if ( SQL_GRAMMAR::CRTCS == _commandType )
      {
         rc = rtnCreateCollectionSpaceCommand(
                                  _fullName.toString().c_str(),
                                  eduCB, dmsCB, dpsCB ) ;
      }
      else if ( SQL_GRAMMAR::DROPCS == _commandType )
      {
         rc = rtnDropCollectionSpaceCommand(
                                    _fullName.toString().c_str(),
                                    eduCB, dmsCB, dpsCB ) ;
      }
      else if ( SQL_GRAMMAR::CRTCL == _commandType )
      {
         if ( _partition.isEmpty() )
         {
            rc = rtnCreateCollectionCommand(
                        _fullName.toString().c_str(),
                        0, eduCB, dmsCB, dpsCB ) ;
         }
         else
         {
            rc = rtnCreateCollectionCommand(
                            _fullName.toString().c_str(),
                            _partition, 0,
                            eduCB, dmsCB, dpsCB ) ;
         }
      }
      else if ( SQL_GRAMMAR::DROPCL == _commandType )
      {
         rc = rtnDropCollectionCommand(
                         _fullName.toString().c_str(),
                         eduCB, dmsCB, dpsCB ) ;
      }
      else if ( SQL_GRAMMAR::CRTINDEX == _commandType )
      {
         BSONObjBuilder builder ;
         BSONObj index ;
         qgmOPFieldVec::const_iterator itr = _indexColumns.begin() ;
         for ( ; itr != _indexColumns.end(); itr++ )
         {
            builder.append( itr->value.attr().toString(),
                            SQL_GRAMMAR::ASC == itr->type?
                            1 : -1 ) ;
         }
         index = builder.obj() ;
         if ( !_uniqIndex )
         {
            rc = rtnCreateIndexCommand(
                        _fullName.toString().c_str(),
                        BSON("key"<<index<<"name"<<_indexName.toString()),
                        eduCB, dmsCB, dpsCB ) ;
         }
         else
         {
            BSONObjBuilder indexBuilder ;
            indexBuilder.append( "key", index) ;
            indexBuilder.append("name", _indexName.toString()) ;
            indexBuilder.appendBool( "unique", TRUE ) ;
            rc =  rtnCreateIndexCommand(
                        _fullName.toString().c_str(),
                        indexBuilder.obj(),
                        eduCB, dmsCB, dpsCB ) ;
         }
      }
      else if ( SQL_GRAMMAR::DROPINDEX == _commandType )
      {
         BSONObj identifier = BSON( "name" << _indexName.toString() ) ;
         BSONElement ele = identifier.firstElement() ;
         rc = rtnDropIndexCommand(
                          _fullName.toString().c_str(),
                          ele, eduCB, dmsCB, dpsCB ) ;
      }
      else if ( SQL_GRAMMAR::LISTCS == _commandType )
      {
         BSONObj empty ;
         rc = rtnListCommandEntry( CMD_LIST_COLLECTIONSPACES,
                                   empty, empty, empty, empty, 0, eduCB,
                                   0, -1, dmsCB, rtnCB,
                                   _contextID, addInfo ) ;
      }
      else if ( SQL_GRAMMAR::LISTCL == _commandType )
      {
         BSONObj empty ;
         rc = rtnListCommandEntry( CMD_LIST_COLLECTIONS,
                                   empty, empty, empty, empty, 0, eduCB,
                                   0, -1, dmsCB, rtnCB,
                                   _contextID, addInfo ) ;
      }
      else if ( SQL_GRAMMAR::BEGINTRAN == _commandType )
      {
         rc = rtnTransBegin( eduCB ) ;
      }
      else if ( SQL_GRAMMAR::ROLLBACK == _commandType )
      {
         rc = rtnTransRollback( eduCB, dpsCB ) ;
      }
      else if ( SQL_GRAMMAR::COMMIT == _commandType )
      {
         rc = rtnTransCommit( eduCB, dpsCB ) ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid command type:%d", _commandType ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPLCOMMAND__EXECONDATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLCOMMAND__FETCHNEXT, "_qgmPlCommand::_fetchNext" )
   INT32 _qgmPlCommand::_fetchNext( qgmFetchOut &next )
   {
      PD_TRACE_ENTRY( SDB__QGMPLCOMMAND__FETCHNEXT ) ;
      INT32 rc = SDB_OK ;

      rtnContextBuf buffObj ;
      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;

      rc = rtnGetMore( _contextID, 1, buffObj, _eduCB, rtnCB ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "Failed to getmore from non-coord, rc = %d",
                    rc) ;
         }
         goto error ;
      }

      try
      {
         next.obj = BSONObj( buffObj.data() ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexcepted err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMPLCOMMAND__FETCHNEXT, rc ) ;
      return rc ;
   error:
      if ( SDB_DMS_EOC == rc )
         _contextID = -1 ;
      goto done ;
   }

   void _qgmPlCommand::_killContext()
   {
      if ( -1 != _contextID )
      {
         SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
         rtnKillContexts( 1, &_contextID, _eduCB, rtnCB ) ;
         _contextID = -1 ;
      }
      return ;
   }
}

