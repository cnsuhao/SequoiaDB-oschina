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

   Source File Name = clsReplayer.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime code for insert
   request.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "clsReplayer.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "rtn.hpp"
#include "dpsOp2Record.hpp"
#include "clsReplBucket.hpp"
#include "pdTrace.hpp"
#include "clsTrace.hpp"
#include "rtnLob.hpp"

using namespace bson ;

namespace engine
{

   INT32 startIndexJob ( RTN_JOB_TYPE type,
                         const dpsLogRecordHeader *recordHeader,
                         _dpsLogWrapper *dpsCB ) ;

   _clsReplayer::_clsReplayer( BOOLEAN useDps )
   {
      _dmsCB = sdbGetDMSCB() ;
      _dpsCB = NULL ;
      if ( useDps )
      {
         _dpsCB = sdbGetDPSCB() ;
      }
      _monDBCB = pmdGetKRCB()->getMonDBCB () ;
   }

   _clsReplayer::~_clsReplayer()
   {

   }

   void _clsReplayer::enableDPS ()
   {
      _dpsCB = sdbGetDPSCB() ;
   }

   void _clsReplayer::disableDPS ()
   {
      _dpsCB = NULL ;
   }

   INT32 _clsReplayer::replayByBucket( dpsLogRecordHeader *recordHeader,
                                       pmdEDUCB *eduCB, clsBucket *pBucket )
   {
      INT32 rc = SDB_OK ;
      const CHAR *fullname = NULL ;
      BSONObj obj ;
      BSONElement idEle ;
      const bson::OID *oidPtr = NULL ;
      BOOLEAN paralla = FALSE ;
      UINT32 bucketID = ~0 ;

      SDB_ASSERT( recordHeader && pBucket, "Invalid param" ) ;

      switch( recordHeader->_type )
      {
         case LOG_TYPE_DATA_INSERT :
            paralla = FALSE ;
            rc = dpsRecord2Insert( (CHAR *)recordHeader, &fullname, obj ) ;
            break ;
         case LOG_TYPE_DATA_DELETE :
            paralla = FALSE ;
            rc = dpsRecord2Delete( (CHAR *)recordHeader, &fullname, obj ) ;
            break ;
         case LOG_TYPE_DATA_UPDATE :
         {
            BSONObj match ;     //old match
            BSONObj oldObj ;
            BSONObj newMatch ;
            BSONObj modifier ;   //new change obj
            rc = dpsRecord2Update( (CHAR *)recordHeader, &fullname,
                                   match, oldObj, newMatch, modifier ) ;
            if ( SDB_OK == rc &&
                 0 == match.woCompare( newMatch, BSONObj(), false ) )
            {
               obj = match ;
               paralla = TRUE ;
            }
            break ;
         }
         case LOG_TYPE_LOB_WRITE :
         {
            paralla = TRUE ;
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            rc = dpsRecord2LobW( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data, page ) ;
            oidPtr = oid ;
            break ;
         }
         case LOG_TYPE_LOB_UPDATE :
         {
            paralla = TRUE ;
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            UINT32 oldLen = 0 ;
            const CHAR *oldData = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            rc = dpsRecord2LobU( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data,
                                  oldLen, &oldData, page ) ;
            oidPtr = oid ;
            break ;
         }
         case LOG_TYPE_LOB_REMOVE :
         {
            paralla = TRUE ;
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            rc = dpsRecord2LobRm( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data, page ) ;
            oidPtr = oid ;
            break ;
         }
         case LOG_TYPE_DUMMY :
            bucketID = 0 ;
            break ;
         default :
            break ;
      }

      PD_RC_CHECK( rc, PDERROR, "Parse dps log[type: %d, lsn: %lld, len: %d]"
                   "falied, rc: %d", recordHeader->_type,
                   recordHeader->_lsn, recordHeader->_length, rc ) ;

      if ( LOG_TYPE_DATA_INSERT == recordHeader->_type ||
           LOG_TYPE_DATA_DELETE == recordHeader->_type )
      {
         dmsStorageUnit *su = NULL ;
         const CHAR *pShortName = NULL ;
         dmsStorageUnitID suID = DMS_INVALID_SUID ;
         rc = rtnResolveCollectionNameAndLock( fullname, _dmsCB, &su,
                                               &pShortName, suID ) ;
         if ( SDB_OK == rc )
         {
            dmsMBContext *mbContext = NULL ;
            rc = su->data()->getMBContext( &mbContext, pShortName, SHARED ) ;
            if ( SDB_OK == rc )
            {
               paralla = mbContext->mbStat()->_uniqueIdxNum <= 1 ?
                         TRUE : FALSE ;
               su->data()->releaseMBContext( mbContext ) ;
            }
            _dmsCB->suUnlock( suID, SHARED ) ;
         }
      }

      if ( paralla )
      {
         idEle = obj.getField( DMS_ID_KEY_NAME ) ;
         if ( !idEle.eoo() )
         {
            bucketID = pBucket->calcIndex( idEle.value(),
                                           idEle.valuesize() ) ;
         }
         else if ( NULL != oidPtr )
         {
            bucketID = pBucket->calcIndex( ( const CHAR * )( oidPtr->getData()),
                                           sizeof( *oidPtr ) ) ;
         }
      }

      if ( (UINT32)~0 != bucketID )
      {
         rc = pBucket->pushData( bucketID, (CHAR *)recordHeader,
                                 recordHeader->_length ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to push log to bucket, rc: %d",
                      rc ) ;
      }
      else
      {
         pBucket->waitQueEmpty() ;
         if ( !pBucket->_expectLSN.invalid() &&
              0 != pBucket->_expectLSN.compareOffset( recordHeader->_lsn ) )
         {
            PD_LOG( PDWARNING, "Expect lsn[%lld], real complete lsn[%lld]",
                    recordHeader->_lsn, pBucket->_expectLSN.offset ) ;
            rc = SDB_CLS_REPLAY_LOG_FAILED ;
            goto error ;
         }
         rc = replay( recordHeader, eduCB ) ;
         if ( SDB_OK == rc && !pBucket->_expectLSN.invalid() )
         {
            pBucket->_expectLSN.offset += recordHeader->_length ;
            pBucket->_expectLSN.version = recordHeader->_version ;
         }
      }

   done:
      if ( SDB_OK != rc )
      {
         dpsLogRecord record ;
         CHAR tmpBuff[4096] = {0} ;
         INT32 rcTmp = record.load( (const CHAR*)recordHeader ) ;
         if ( SDB_OK == rcTmp )
         {
            record.dump( tmpBuff, sizeof(tmpBuff)-1, DPS_DMP_OPT_FORMATTED ) ;
         }
         PD_LOG( PDERROR, "sync bucket: replay log [type:%d, lsn:%lld, "
                 "data: %s] failed, rc: %d", recordHeader->_type,
                 recordHeader->_lsn, tmpBuff, rc ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSREP_REPLAY, "_clsReplayer::replay" )
   INT32 _clsReplayer::replay( dpsLogRecordHeader *recordHeader,
                               pmdEDUCB *eduCB, BOOLEAN incCount )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSREP_REPLAY );
      SDB_ASSERT( NULL != recordHeader, "head should not be NULL" ) ;

      try
      {
      switch ( recordHeader->_type )
      {
         case LOG_TYPE_DATA_INSERT :
         {
            const CHAR *fullname = NULL ;
            BSONObj obj ;
            rc = dpsRecord2Insert( (CHAR *)recordHeader,
                                   &fullname,
                                   obj ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = rtnInsert( fullname, obj, 1, 0, eduCB, _dmsCB, _dpsCB, 1 ) ;
            if ( SDB_OK == rc && incCount )
            {
               _monDBCB->monOperationCountInc ( MON_INSERT_REPL ) ;
            }
            else if ( SDB_IXM_DUP_KEY == rc )
            {
               PD_LOG( PDINFO, "Record[%s] already exist when insert",
                       obj.toString().c_str() ) ;
               rc = SDB_OK ;
            }
            break ;
         }
         case LOG_TYPE_DATA_UPDATE :
         {
            BSONObj match ;     //old match
            BSONObj oldObj ;
            BSONObj newMatch ;
            BSONObj modifier ;   //new change obj
            const CHAR *fullname = NULL ;
            BSONObj hint = BSON(""<<IXM_ID_KEY_NAME);
            rc = dpsRecord2Update( (CHAR *)recordHeader,
                                   &fullname,
                                   match,
                                   oldObj,
                                   newMatch,
                                   modifier ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            if ( !modifier.isEmpty() )
            {
               rc = rtnUpdate( fullname, match, modifier,
                               hint, 0, eduCB, _dmsCB, _dpsCB, 1 ) ;
            }
            if ( SDB_OK == rc && incCount )
            {
               _monDBCB->monOperationCountInc ( MON_UPDATE_REPL ) ;
            }
            break ;
         }
         case LOG_TYPE_DATA_DELETE :
         {
            const CHAR *fullname = NULL ;
            BSONObj obj ;
            rc = dpsRecord2Delete( (CHAR *)recordHeader,
                                   &fullname,
                                   obj ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            else
            {
               BSONElement idEle = obj.getField( DMS_ID_KEY_NAME ) ;
               if ( idEle.eoo() )
               {
                  PD_LOG( PDWARNING, "replay: failed to parse "
                          "oid from bson:[%s]",obj.toString().c_str() ) ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
               {
                  BSONObjBuilder selectorBuilder ;
                  selectorBuilder.append( idEle ) ;
                  BSONObj selector = selectorBuilder.obj() ;
                  BSONObj hint = BSON(""<<IXM_ID_KEY_NAME) ;
                  rc = rtnDelete( fullname, selector, hint, 0, eduCB, _dmsCB,
                                  _dpsCB, 1 ) ;
               }
            }
            if ( SDB_OK == rc && incCount )
            {
               _monDBCB->monOperationCountInc ( MON_DELETE_REPL ) ;
            }
            break ;
         }
         case LOG_TYPE_CS_CRT :
         {
            const CHAR *cs = NULL ;
            INT32 pageSize = 0 ;
            INT32 lobPageSize = 0 ;
            rc = dpsRecord2CSCrt( (CHAR *)recordHeader,
                                  &cs, pageSize, lobPageSize ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = rtnCreateCollectionSpaceCommand( cs, eduCB, _dmsCB, _dpsCB,
                                                  pageSize, lobPageSize,
                                                  TRUE ) ;
            if ( SDB_DMS_CS_EXIST == rc )
            {
               PD_LOG( PDWARNING, "Collection space[%s] already exist when "
                       "create", cs ) ;
               rc = SDB_OK ;
            }
            break ;
         }
         case LOG_TYPE_CS_DELETE :
         {
            const CHAR *cs = NULL ;
            rc = dpsRecord2CSDel( (CHAR *)recordHeader,
                                  &cs ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            while ( TRUE )
            {
               rc = rtnDropCollectionSpaceCommand( cs, eduCB, _dmsCB, _dpsCB,
                                                   TRUE ) ;
               if ( SDB_LOCK_FAILED == rc )
               {
                  ossSleep ( 100 ) ;
                  continue ;
               }
               break ;
            }
            if ( SDB_DMS_CS_NOTEXIST == rc )
            {
               PD_LOG( PDWARNING, "Collection space[%s] not exist when "
                       "drop", cs ) ;
               rc = SDB_OK ;
            }
            break ;
         }
         case LOG_TYPE_CL_CRT :
         {
            const CHAR *cl = NULL ;
            UINT32 attribute = 0 ;
            rc = dpsRecord2CLCrt( (CHAR *)recordHeader,
                                  &cl,
                                  attribute ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = rtnCreateCollectionCommand( cl, attribute, eduCB, _dmsCB,
                                             _dpsCB, 0, TRUE ) ;
            if ( SDB_DMS_EXIST == rc )
            {
               PD_LOG( PDWARNING, "Collection [%s] already exist when "
                       "create", cl ) ;
               rc = SDB_OK ;
            }
            break ;
         }
         case LOG_TYPE_CL_DELETE :
         {
            const CHAR *cl = NULL ;
            rc = dpsRecord2CLDel( (CHAR *)recordHeader,
                                   &cl ) ;
            rc = rtnDropCollectionCommand( cl, eduCB, _dmsCB, _dpsCB ) ;
            if ( SDB_DMS_NOTEXIST == rc )
            {
               PD_LOG( PDWARNING, "Collection [%s] not exist when drop", cl ) ;
               rc = SDB_OK ;
            }
            break ;
         }
         case LOG_TYPE_IX_CRT :
         {
            startIndexJob ( RTN_JOB_CREATE_INDEX, recordHeader, _dpsCB ) ;
            break ;
         }
         case LOG_TYPE_IX_DELETE :
         {
            startIndexJob ( RTN_JOB_DROP_INDEX, recordHeader, _dpsCB ) ;
            break ;
         }
         case LOG_TYPE_CL_RENAME :
         {
            const CHAR *cs = NULL ;
            const CHAR *oldCl = NULL ;
            const CHAR *newCl = NULL ;
            dmsStorageUnitID suID = DMS_INVALID_CS ;
            dmsStorageUnit *su = NULL ;
            rc = dpsRecord2CLRename( (CHAR *)recordHeader,
                                      &cs, &oldCl, &newCl ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = rtnCollectionSpaceLock ( cs, _dmsCB, FALSE, &su, suID ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get collection space %s, rc: %d",
                       cs, rc ) ;
               goto error ;
            }
            rc = su->data()->renameCollection ( oldCl, newCl,
                                                eduCB, _dpsCB ) ;
            _dmsCB->suUnlock ( suID ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to rename %s to %s, rc: %d",
                       oldCl, newCl, rc ) ;
               goto error ;
            }
            break ;
         }
         case LOG_TYPE_CL_TRUNC :
         {
            const CHAR *clname = NULL ;
            rc = dpsRecord2CLTrunc( (const CHAR *)recordHeader, &clname ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = rtnTruncCollectionCommand( clname, eduCB, _dmsCB, _dpsCB ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "Failed to truncate collection[%s], rc: %d",
                       clname, rc ) ;
               goto error ;
            }
            break ;
         }
         case LOG_TYPE_INVALIDATE_CATA :
         {
            catAgent *pCatAgent = sdbGetShardCB()->getCataAgent() ;
            const CHAR *name = NULL ;
            rc = dpsRecord2InvalidCata( (CHAR *)recordHeader,
                                        &name ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            pCatAgent->lock_w() ;
            if ( ossStrchr( name, '.' ) )
            {
               pCatAgent->clear( name ) ;
            }
            else
            {
               pCatAgent->clearBySpaceName( name ) ;
            }
            pCatAgent->release_w() ;

            rc = SDB_OK ;
            break ;
         }
         case LOG_TYPE_LOB_WRITE :
         {
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            rc = dpsRecord2LobW( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data, page ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = rtnWriteLob( fullName, *oid, sequence,
                              offset, len, data, eduCB,
                              1, _dpsCB ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
               goto error ;
            }
            break ;
         }
         case LOG_TYPE_LOB_REMOVE :
         {
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            rc = dpsRecord2LobRm( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data, page ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = rtnRemoveLobPiece( fullName, *oid,
                                    sequence, eduCB,
                                    1, _dpsCB ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to remove lob:%d", rc ) ;
               goto error ;
            }
            break ;
         }
         case LOG_TYPE_LOB_UPDATE :
         {
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            UINT32 oldLen = 0 ;
            const CHAR *oldData = NULL ;
            rc = dpsRecord2LobU( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data,
                                  oldLen, &oldData, page ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = rtnUpdateLob( fullName, *oid, sequence,
                               offset, len, data, eduCB,
                               1, _dpsCB ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to update lob:%d", rc ) ;
               goto error ;
            }

            break ;
         }
         case LOG_TYPE_DUMMY :
         {
            rc = SDB_OK ;
            break ;
         }
         case LOG_TYPE_TS_COMMIT :
         {
            rc = SDB_OK ;
            break ;
         }
         default :
         {
            rc = SDB_CLS_REPLAY_LOG_FAILED ;
            PD_LOG( PDWARNING, "unexpected log type: %d",
                    recordHeader->_type ) ;
            break ;
         }
      }
      }
      catch ( std::exception &e )
      {
         rc = SDB_CLS_REPLAY_LOG_FAILED ;
         PD_LOG( PDERROR, "unexpected exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      if ( SDB_OK != rc )
      {
         dpsLogRecord record ;
         CHAR tmpBuff[4096] = {0} ;
         INT32 rcTmp = record.load( (const CHAR*)recordHeader ) ;
         if ( SDB_OK == rcTmp )
         {
            record.dump( tmpBuff, sizeof(tmpBuff)-1, DPS_DMP_OPT_FORMATTED ) ;
         }
         PD_LOG( PDERROR, "sync: replay log [type:%d, lsn:%lld, data: %s] "
                 "failed, rc: %d", recordHeader->_type, recordHeader->_lsn,
                 tmpBuff, rc ) ;
      }
      PD_TRACE_EXITRC ( SDB__CLSREP_REPLAY, rc );
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__CLSREP_ROLBCK, "_clsReplayer::rollback" )
   INT32 _clsReplayer::rollback( const dpsLogRecordHeader *recordHeader,
                                 _pmdEDUCB *eduCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSREP_ROLBCK );
      SDB_ASSERT( NULL != recordHeader, "head should not be NULL" ) ;

      try
      {
         switch ( recordHeader->_type )
         {
         case LOG_TYPE_DATA_INSERT :
         {
            BSONObj obj ;
            const CHAR *fullname = NULL ;
            rc = dpsRecord2Insert( (const CHAR *)recordHeader,
                                   &fullname, obj ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            {
               BSONElement idEle = obj.getField( DMS_ID_KEY_NAME ) ;
               if ( idEle.eoo() )
               {
                  PD_LOG( PDWARNING, "replay: failed to parse"
                          " oid from bson:[%s]",obj.toString().c_str() ) ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
               {
                  BSONObjBuilder selectorBuilder ;
                  selectorBuilder.append( idEle ) ;
                  BSONObj selector = selectorBuilder.obj() ;
                  BSONObj hint = BSON(""<<IXM_ID_KEY_NAME) ;
                  rc = rtnDelete( fullname, selector, hint, 0, eduCB, _dmsCB,
                                  _dpsCB, 1 ) ;
               }
            }
            break ;
         }
         case LOG_TYPE_DATA_UPDATE :
         {
            const CHAR *fullname = NULL ;
            BSONObj oldMatch ;
            BSONObj modifier ;  //old modifier
            BSONObj newMatch ;     //new matcher
            BSONObj newObj ;
            BSONObj hint = BSON(""<<IXM_ID_KEY_NAME) ;
            rc = dpsRecord2Update( (const CHAR *)recordHeader,
                                    &fullname,
                                    oldMatch,
                                    modifier,
                                    newMatch,
                                    newObj ) ;
            if ( !modifier.isEmpty() )
            {
               rc = rtnUpdate( fullname, newMatch, modifier,
                               hint, 0, eduCB, _dmsCB, _dpsCB, 1 ) ;
            }
            break ;
         }
         case LOG_TYPE_DATA_DELETE :
         {
            BSONObj obj ;
            const CHAR *fullname = NULL ;
            rc = dpsRecord2Delete( (const CHAR *)recordHeader,
                                   &fullname,
                                   obj ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = rtnInsert( fullname, obj, 1, 0, eduCB, _dmsCB, _dpsCB, 1 ) ;
            if ( SDB_IXM_DUP_KEY == rc )
            {
               PD_LOG( PDINFO, "Record[%s] exist when rollback delete",
                       obj.toString().c_str() ) ;
               rc = SDB_OK ;
            }
            break ;
         }
         case LOG_TYPE_CS_CRT :
         {
            const CHAR *cs = NULL ;
            INT32 pageSize = 0 ;
            INT32 lobPageSize = 0 ;
            rc = dpsRecord2CSCrt( (const CHAR *)recordHeader,
                                  &cs, pageSize, lobPageSize ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = rtnDropCollectionSpaceCommand( cs, eduCB, _dmsCB,
                                                _dpsCB, TRUE ) ;
            if ( SDB_DMS_CS_NOTEXIST == rc )
            {
               rc = SDB_OK ;
               PD_LOG( PDWARNING, "Collection space[%s] not exist when "
                       "rollback create cs", cs ) ;
            }
            break ;
         }
         case LOG_TYPE_CS_DELETE :
         {
            rc = SDB_CLS_REPLAY_LOG_FAILED ;
            goto error ;
         }
         case LOG_TYPE_CL_CRT :
         {
            const CHAR *fullname = NULL ;
            UINT32 attribute = 0 ;
            rc = dpsRecord2CLCrt( (const CHAR *)recordHeader,
                                  &fullname,
                                   attribute ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = rtnDropCollectionCommand( fullname, eduCB, _dmsCB, _dpsCB ) ;
            if ( SDB_DMS_NOTEXIST == rc )
            {
               rc = SDB_OK ;
               PD_LOG( PDWARNING, "Collection[%s] not exist when rollback "
                       "create cl", fullname ) ;
            }
            break ;
         }
         case LOG_TYPE_CL_DELETE :
         {
            rc = SDB_CLS_REPLAY_LOG_FAILED ;
            goto error ;
         }
         case LOG_TYPE_IX_CRT :
         {
            startIndexJob ( RTN_JOB_DROP_INDEX, recordHeader, _dpsCB ) ;
            break ;
         }
         case LOG_TYPE_IX_DELETE :
         {
            startIndexJob ( RTN_JOB_CREATE_INDEX, recordHeader, _dpsCB ) ;
            break ;
         }
         case LOG_TYPE_CL_RENAME :
         {
            dmsStorageUnitID suID = DMS_INVALID_CS ;
            dmsStorageUnit *su = NULL ;
            const CHAR *cs = NULL ;
            const CHAR *oldName = NULL ;
            const CHAR *newName = NULL ;
            rc = dpsRecord2CLRename( (const CHAR *)recordHeader,
                                     &cs,
                                     &oldName,
                                     &newName ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = rtnCollectionSpaceLock ( cs, _dmsCB, FALSE, &su, suID ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get collection space %s, rc: %d",
                       cs, rc ) ;
               goto error ;
            }
            rc = su->data()->renameCollection ( newName,
                                                oldName,
                                                eduCB,
                                                _dpsCB ) ;
            _dmsCB->suUnlock ( suID ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to rename %s to %s, rc: %d",
                       newName, oldName, rc ) ;
               goto error ;
            }
            break ;
         }
         case LOG_TYPE_CL_TRUNC :
         {
            rc = SDB_CLS_REPLAY_LOG_FAILED ;
            goto error ;
         }
         case LOG_TYPE_TS_COMMIT :
         case LOG_TYPE_DUMMY :
         case LOG_TYPE_INVALIDATE_CATA :
         {
            rc = SDB_OK ;
            break ;
         }
         case LOG_TYPE_LOB_WRITE :
         {
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            rc = dpsRecord2LobW( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data, page ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = rtnRemoveLobPiece( fullName, *oid, sequence,
                                    eduCB, 1, NULL ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to remove lob:%d", rc ) ;
               goto error ;
            }
            break ; 
         }
         case LOG_TYPE_LOB_UPDATE :
         {
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            UINT32 oldLen = 0 ;
            const CHAR *oldData = NULL ;
            rc = dpsRecord2LobU( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data,
                                  oldLen, &oldData, page ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = rtnUpdateLob( fullName, *oid, sequence,
                               offset, oldLen, oldData, eduCB,
                               1, NULL ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to update lob:%d", rc ) ;
               goto error ;
            } 
            break ; 
         }
         case LOG_TYPE_LOB_REMOVE :
         {
            const CHAR *fullName = NULL ;
            const bson::OID *oid = NULL ;
            UINT32 sequence = 0 ;
            UINT32 offset = 0 ;
            UINT32 len = 0 ;
            UINT32 hash = 0 ;
            const CHAR *data = NULL ;
            DMS_LOB_PAGEID page = DMS_LOB_INVALID_PAGEID ;
            rc = dpsRecord2LobRm( (CHAR *)recordHeader,
                                  &fullName, &oid,
                                  sequence, offset,
                                  len, hash, &data, page ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = rtnWriteLob( fullName, *oid, sequence,
                              offset, len, data, eduCB,
                              1, NULL ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
               goto error ;
            }
            break ;
         }
         default :
         {
            rc = SDB_CLS_REPLAY_LOG_FAILED ;
            PD_LOG( PDWARNING, "unexpected log type: %d",
                    recordHeader->_type ) ;
            break ;
         }
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_CLS_REPLAY_LOG_FAILED ;
         PD_LOG( PDERROR, "unexpected exception: %s", e.what() ) ;
         goto error ;
      }

      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "sync:rollback log[type:%d, lsn:%lld] failed, rc: %d",
                 recordHeader->_type, recordHeader->_lsn, rc ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSREP_ROLBCK, rc );
      return rc ;
   error:
      goto done ;
   }

   INT32 _clsReplayer::replayCrtCS( const CHAR *cs, INT32 pageSize,
                                    INT32 lobPageSize,
                                    _pmdEDUCB *eduCB,
                                    BOOLEAN delWhenExist )
   {
      SDB_ASSERT( NULL != cs, "cs should not be NULL" ) ;
      return rtnCreateCollectionSpaceCommand( cs, eduCB, _dmsCB,
                                              _dpsCB, pageSize,
                                              lobPageSize,
                                              TRUE,
                                              delWhenExist ) ;
   }

   INT32 _clsReplayer::replayCrtCollection( const CHAR *collection,
                                            UINT32 attributes,
                                            _pmdEDUCB *eduCB )
   {
      SDB_ASSERT( NULL != collection, "collection should not be NULL" ) ;
      return rtnCreateCollectionCommand( collection, attributes, eduCB,
                                         _dmsCB, _dpsCB, 0, TRUE ) ;
   }

   INT32 _clsReplayer::replayIXCrt( const CHAR *collection,
                                    BSONObj &index,
                                    _pmdEDUCB *eduCB )
   {
      SDB_ASSERT( NULL != collection, "collection should not be NULL" ) ;
      return rtnCreateIndexCommand( collection, index, eduCB,
                                    _dmsCB, _dpsCB, TRUE ) ;
   }

   INT32 _clsReplayer::replayInsert( const CHAR *collection,
                                     BSONObj &obj,
                                     _pmdEDUCB *eduCB )
   {
      SDB_ASSERT( NULL != collection, "collection should not be NULL" ) ;
      return rtnInsert( collection, obj, 1, FLG_INSERT_CONTONDUP, eduCB,
                        _dmsCB, _dpsCB ) ;
   }

   INT32 _clsReplayer::replayWriteLob( const CHAR *fullName,
                                       const bson::OID &oid,
                                       UINT32 sequence,
                                       UINT32 offset,
                                       UINT32 len,
                                       const CHAR *data,
                                       _pmdEDUCB *eduCB )
   {
      return rtnWriteLob( fullName, oid, sequence,
                          offset, len, data, eduCB,
                          1, _dpsCB ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_STARTINXJOB, "startIndexJob" )
   INT32 startIndexJob ( RTN_JOB_TYPE type,
                         const dpsLogRecordHeader *recordHeader,
                         _dpsLogWrapper *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_STARTINXJOB );
      const CHAR *fullname = NULL ;
      BSONObj index ;
      rtnIndexJob *indexJob = NULL ;
      clsCatalogSet *pCatSet = NULL ;
      BOOLEAN useSync = FALSE ;

      if ( LOG_TYPE_IX_CRT != recordHeader->_type &&
           LOG_TYPE_IX_DELETE != recordHeader->_type )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else if ( LOG_TYPE_IX_CRT == recordHeader->_type)
      {
         rc = dpsRecord2IXCrt( (CHAR *)recordHeader,
                               &fullname,
                               index ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         rc = dpsRecord2IXDel( (CHAR *)recordHeader,
                               &fullname,
                               index ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }

      sdbGetShardCB()->getAndLockCataSet( fullname, &pCatSet, TRUE ) ;
      if ( pCatSet && CLS_REPLSET_MAX_NODE_SIZE == pCatSet->getW() )
      {
         useSync = TRUE ;
      }
      sdbGetShardCB()->unlockCataSet( pCatSet ) ;

      indexJob = SDB_OSS_NEW rtnIndexJob( type, fullname, index, dpsCB ) ;
      if ( NULL == indexJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for indexJob" ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = indexJob->init () ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Index job[%s] init failed, rc = %d",
                  indexJob->name(), rc ) ;
         goto error ;
      }

      if ( FALSE == useSync )
      {
         /*rc = rtnGetJobMgr()->startJob( indexJob, RTN_JOB_MUTEX_STOP_RET, NULL ) ;
         if ( SDB_RTN_MUTEX_JOB_EXIST == rc )
         {
            rc = SDB_OK ;
         }*/
         rc = rtnGetJobMgr()->startJob( indexJob, RTN_JOB_MUTEX_STOP_CONT, NULL ) ;
      }
      else
      {
         indexJob->doit() ;
         SDB_OSS_DEL indexJob ;
         indexJob = NULL ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_STARTINXJOB, rc );
      return rc ;
   error:
      goto done ;
   }

}
