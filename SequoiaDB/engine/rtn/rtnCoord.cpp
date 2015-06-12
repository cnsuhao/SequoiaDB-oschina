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

   Source File Name = rtnCoord.cpp

   Descriptive Name = Runtime Coord

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   command factory on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoord.hpp"
#include "rtnCoordCommands.hpp"
#include "rtnCoordImageCommands.hpp"
#include "rtnCoordOperator.hpp"
#include "rtnCoordAuth.hpp"
#include "rtnCoordAuthCrt.hpp"
#include "rtnCoordAuthDel.hpp"
#include "rtnCoordInsert.hpp"
#include "rtnCoordQuery.hpp"
#include "rtnCoordDelete.hpp"
#include "rtnCoordUpdate.hpp"
#include "rtnCoordInterrupt.hpp"
#include "rtnCoordTransaction.hpp"
#include "rtnCoordSql.hpp"
#include "rtnCoordAggregate.hpp"
#include "rtnCoordLob.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace engine
{


   /*
      MAP COMMANDS
   */
   RTN_COORD_CMD_BEGIN
   RTN_COORD_CMD_ADD( COORD_CMD_BACKUP_OFFLINE, rtnCoordBackupOffline )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_BACKUPS, rtnCoordListBackup )
   RTN_COORD_CMD_ADD( COORD_CMD_REMOVE_BACKUP, rtnCoordRemoveBackup )
   RTN_COORD_CMD_ADD( COORD_CMD_LISTGROUPS,  rtnCoordCMDListGroups )
   RTN_COORD_CMD_ADD( COORD_CMD_LISTCOLLECTIONSPACES, rtnCoordCMDListCollectionSpace )
   RTN_COORD_CMD_ADD( COORD_CMD_LISTCOLLECTIONS, rtnCoordCMDListCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATECOLLECTIONSPACE, rtnCoordCMDCreateCollectionSpace )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATECOLLECTION, rtnCoordCMDCreateCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_ALTERCOLLECTION, rtnCoordCMDAlterCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_DROPCOLLECTION, rtnCoordCMDDropCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_DROPCOLLECTIONSPACE, rtnCoordCMDDropCollectionSpace )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTDATABASE, rtnCoordCMDSnapshotDataBase )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSYSTEM, rtnCoordCMDSnapshotSystem )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSESSIONS, rtnCoordCMDSnapshotSessions )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSESSIONSCUR, rtnCoordCMDSnapshotSessionsCur )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCONTEXTS, rtnCoordCMDSnapshotContexts )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCONTEXTSCUR, rtnCoordCMDSnapshotContextsCur )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTRESET, rtnCoordCMDSnapshotReset )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCOLLECTIONS, rtnCoordCMDSnapshotCollections )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCOLLECTIONSPACES, rtnCoordCMDSnapshotSpaces )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCATALOG, rtnCoordCMDSnapshotCata )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTDBINTR, rtnCoordCMDSnapshotDBIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSYSINTR, rtnCoordCMDSnapshotSysIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCLINTR, rtnCoordCMDSnapshotClIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCSINTR, rtnCoordCMDSnapshotCsIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCTXINTR, rtnCoordCMDSnapshotCtxIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTCTXCURINTR, rtnCoordCMDSnapshotCtxCurIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSESSINTR, rtnCoordCMDSnapshotSessionIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_SNAPSHOTSESSCURINTR, rtnCoordCMDSnapshotSessionCurIntr )
   RTN_COORD_CMD_ADD( COORD_CMD_TESTCOLLECTIONSPACE, rtnCoordCMDTestCollectionSpace )
   RTN_COORD_CMD_ADD( COORD_CMD_TESTCOLLECTION, rtnCoordCMDTestCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATEGROUP, rtnCoordCMDCreateGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_REMOVEGROUP, rtnCoordCMDRemoveGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_ACTIVEGROUP, rtnCoordCMDActiveGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATENODE, rtnCoordCMDCreateNode )
   RTN_COORD_CMD_ADD( COORD_CMD_REMOVENODE, rtnCoordCMDRemoveNode )
   RTN_COORD_CMD_ADD( COORD_CMD_UPDATENODE, rtnCoordCMDUpdateNode )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATEINDEX, rtnCoordCMDCreateIndex )
   RTN_COORD_CMD_ADD( COORD_CMD_DROPINDEX, rtnCoordCMDDropIndex )
   RTN_COORD_CMD_ADD( COORD_CMD_STARTUPNODE, rtnCoordCMDStartupNode )
   RTN_COORD_CMD_ADD( COORD_CMD_SHUTDOWNNODE, rtnCoordCMDShutdownNode )
   RTN_COORD_CMD_ADD( COORD_CMD_SHUTDOWNGROUP, rtnCoordCMDShutdownGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_GETCOUNT, rtnCoordCMDGetCount )
   RTN_COORD_CMD_ADD( COORD_CMD_GETDATABLOCKS, rtnCoordCMDGetDatablocks )
   RTN_COORD_CMD_ADD( COORD_CMD_GETQUERYMETA, rtnCoordCMDGetQueryMeta )
   RTN_COORD_CMD_ADD( COORD_CMD_SPLIT, rtnCoordCMDSplit )
   RTN_COORD_CMD_ADD( COORD_CMD_WAITTASK, rtnCoordCmdWaitTask )
   RTN_COORD_CMD_ADD( COORD_CMD_GETINDEXES, rtnCoordCMDGetIndexes )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATECATAGROUP, rtnCoordCMDCreateCataGroup )
   RTN_COORD_CMD_ADD( COORD_CMD_TRACESTART, rtnCoordCMDTraceStart )
   RTN_COORD_CMD_ADD( COORD_CMD_TRACESTOP, rtnCoordCMDTraceStop )
   RTN_COORD_CMD_ADD( COORD_CMD_TRACERESUME, rtnCoordCMDTraceResume )
   RTN_COORD_CMD_ADD( COORD_CMD_TRACESTATUS, rtnCoordCMDTraceStatus )
   RTN_COORD_CMD_ADD( COORD_CMD_EXPCONFIG, rtnCoordCMDExpConfig )
   RTN_COORD_CMD_ADD( COORD_CMD_CRT_PROCEDURE, rtnCoordCMDCrtProcedure )
   RTN_COORD_CMD_ADD( COORD_CMD_EVAL, rtnCoordCMDEval )
   RTN_COORD_CMD_ADD( COORD_CMD_RM_PROCEDURE, rtnCoordCMDRmProcedure )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_PROCEDURES, rtnCoordCMDListProcedures )
   RTN_COORD_CMD_ADD( COORD_CMD_DEFAULT, rtnCoordDefaultCommand )
   RTN_COORD_CMD_ADD( COORD_CMD_LINK, rtnCoordCMDLinkCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_UNLINK, rtnCoordCMDUnlinkCollection )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_TASKS, rtnCoordCmdListTask )
   RTN_COORD_CMD_ADD( COORD_CMD_CANCEL_TASK, rtnCoordCmdCancelTask )
   RTN_COORD_CMD_ADD( COORD_CMD_SET_SESS_ATTR, rtnCoordCMDSetSessionAttr )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_DOMAINS, rtnCoordCMDListDomains )
   RTN_COORD_CMD_ADD( COORD_CMD_CREATE_DOMAIN, rtnCoordCMDCreateDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_DROP_DOMAIN, rtnCoordCMDDropDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_ALTER_DOMAIN, rtnCoordCMDAlterDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_CS_IN_DOMAIN, rtnCoordCMDListCSInDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_CL_IN_DOMAIN, rtnCoordCMDListCLInDomain )
   RTN_COORD_CMD_ADD( COORD_CMD_INVALIDATE_CACHE, rtnCoordCMDInvalidateCache )
   RTN_COORD_CMD_ADD( COORD_CMD_LIST_LOBS, rtnCoordCMDListLobs )
   RTN_COORD_CMD_ADD( COORD_CMD_ATTACH_IMAGE, rtnCoordAttachImage )
   RTN_COORD_CMD_ADD( COORD_CMD_REELECT, rtnCoordCMDReelection )
   RTN_COORD_CMD_END

   /*
      MAP OPERATIONS
   */
   RTN_COORD_OP_BEGIN
   RTN_COORD_OP_ADD( MSG_BS_INSERT_REQ, rtnCoordInsert )
   RTN_COORD_OP_ADD( MSG_BS_QUERY_REQ, rtnCoordQuery )
   RTN_COORD_OP_ADD( MSG_BS_DELETE_REQ, rtnCoordDelete )
   RTN_COORD_OP_ADD( MSG_BS_UPDATE_REQ, rtnCoordUpdate )
   RTN_COORD_OP_ADD( MSG_BS_AGGREGATE_REQ, rtnCoordAggregate )
   RTN_COORD_OP_ADD( MSG_BS_INTERRUPTE, rtnCoordInterrupt )
   RTN_COORD_OP_ADD( MSG_BS_KILL_CONTEXT_REQ, rtnCoordKillContext )
   RTN_COORD_OP_ADD( MSG_AUTH_VERIFY_REQ, rtnCoordAuth )
   RTN_COORD_OP_ADD( MSG_AUTH_CRTUSR_REQ, rtnCoordAuthCrt )
   RTN_COORD_OP_ADD( MSG_AUTH_DELUSR_REQ, rtnCoordAuthDel )
   RTN_COORD_OP_ADD( MSG_BS_TRANS_BEGIN_REQ, rtnCoordTransBegin )
   RTN_COORD_OP_ADD( MSG_BS_TRANS_COMMIT_REQ, rtnCoordTransCommit )
   RTN_COORD_OP_ADD( MSG_BS_TRANS_ROLLBACK_REQ, rtnCoordTransRollback )
   RTN_COORD_OP_ADD( MSG_BS_SQL_REQ, rtnCoordSql )
   RTN_COORD_OP_ADD( MSG_BS_MSG_REQ, rtnCoordMsg )
   RTN_COORD_OP_ADD( MSG_BS_LOB_OPEN_REQ, rtnCoordOpenLob )
   RTN_COORD_OP_ADD( MSG_BS_LOB_WRITE_REQ, rtnCoordWriteLob )
   RTN_COORD_OP_ADD( MSG_BS_LOB_READ_REQ, rtnCoordReadLob )
   RTN_COORD_OP_ADD( MSG_BS_LOB_CLOSE_REQ, rtnCoordCloseLob )
   RTN_COORD_OP_ADD( MSG_BS_LOB_REMOVE_REQ, rtnCoordRemoveLob )
   RTN_COORD_OP_ADD( MSG_NULL, rtnCoordOperatorDefault )
   RTN_COORD_OP_END

   rtnCoordProcesserFactory::rtnCoordProcesserFactory()
   {
      addCommand();
      addOperator();
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROFAC_RTNCOPROFAC, "rtnCoordProcesserFactory::~rtnCoordProcesserFactory" )
   rtnCoordProcesserFactory::~rtnCoordProcesserFactory()
   {
      PD_TRACE_ENTRY ( SDB_RTNCOPROFAC_RTNCOPROFAC ) ;
      COORD_CMD_MAP::iterator iter;
      iter = _cmdMap.begin();
      while ( iter != _cmdMap.end() )
      {
         SDB_OSS_DEL iter->second;
         _cmdMap.erase( iter++ );
      }
      _cmdMap.clear();

      COORD_OP_MAP::iterator iterOp;
      iterOp = _opMap.begin();
      while( iterOp != _opMap.end() )
      {
         SDB_OSS_DEL iterOp->second;
         _opMap.erase( iterOp++ );
      }
      PD_TRACE_EXIT ( SDB_RTNCOPROFAC_RTNCOPROFAC ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROFAC_GETOP, "rtnCoordProcesserFactory::getOperator" )
   rtnCoordOperator * rtnCoordProcesserFactory::getOperator( SINT32 opCode )
   {
      PD_TRACE_ENTRY ( SDB_RTNCOPROFAC_GETOP ) ;
      COORD_OP_MAP::iterator iter;
      iter = _opMap.find ( opCode );
      if ( _opMap.end() == iter )
      {
         iter = _opMap.find ( MSG_NULL );
      }
      PD_TRACE_EXIT ( SDB_RTNCOPROFAC_GETOP ) ;
      return iter->second;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROFAC_GETCOMPRO1, "rtnCoordProcesserFactory::getCommandProcesser" )
   rtnCoordCommand * rtnCoordProcesserFactory::getCommandProcesser(const MsgOpQuery *pQuery)
   {
      PD_TRACE_ENTRY ( SDB_RTNCOPROFAC_GETCOMPRO1 ) ;
      SDB_ASSERT ( pQuery, "pQuery can't be NULL" ) ;
      rtnCoordCommand *pProcesser = NULL;
      do
      {
         if ( MSG_BS_QUERY_REQ == pQuery->header.opCode )
         {
            if ( pQuery->nameLength > 0 )
            {
               COORD_CMD_MAP::iterator iter;
               iter = _cmdMap.find( pQuery->name );
               if ( iter != _cmdMap.end() )
               {
                  pProcesser = iter->second;
               }
            }
         }
         if ( NULL == pProcesser )
         {
            COORD_CMD_MAP::iterator iter;
            iter = _cmdMap.find( COORD_CMD_DEFAULT );
            if ( iter != _cmdMap.end() )
            {
               pProcesser = iter->second;
            }
         }
      }while ( FALSE );
      PD_TRACE_EXIT ( SDB_RTNCOPROFAC_GETCOMPRO1 ) ;
      return pProcesser;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROFAC_GETCOMPRO2, "rtnCoordProcesserFactory::getCommandProcesser" )
   rtnCoordCommand * rtnCoordProcesserFactory::getCommandProcesser(const char *pCmd)
   {
      PD_TRACE_ENTRY ( SDB_RTNCOPROFAC_GETCOMPRO2 ) ;
      SDB_ASSERT ( pCmd, "pCmd can't be NULL" ) ;
      rtnCoordCommand *pProcesser = NULL;
      do
      {
         COORD_CMD_MAP::iterator iter;
         iter = _cmdMap.find( pCmd );
         if ( iter != _cmdMap.end() )
         {
            pProcesser = iter->second;
         }
      }while ( FALSE );
      PD_TRACE_EXIT ( SDB_RTNCOPROFAC_GETCOMPRO2 ) ;
      return pProcesser;
   }
}
