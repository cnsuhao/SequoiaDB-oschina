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

   Source File Name = rtnLob.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnLob.hpp"
#include "dmsStorageUnit.hpp"
#include "dmsLobDef.hpp"
#include "pd.hpp"
#include "rtnContextLob.hpp"
#include "rtnTrace.hpp"
#include "rtnLocalLobStream.hpp"

namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNOPENLOB, "rtnOpenLob" )
   INT32 rtnOpenLob( const BSONObj &lob,
                     SINT32 flags,
                     BOOLEAN isLocal,
                     _pmdEDUCB *cb,
                     SDB_DPSCB *dpsCB,
                     SINT16 w,
                     SINT64 &contextID,
                     BSONObj &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNOPENLOB ) ;
      rtnContextLob *lobContext = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;

      rc = rtnCB->contextNew( RTN_CONTEXT_LOB,
                              (rtnContext**)(&lobContext),
                              contextID, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob context:%d", rc ) ;
         goto error ;
      }

      SDB_ASSERT( NULL != lobContext, "can not be null" ) ;
      rc = lobContext->open( lob, isLocal, cb, dpsCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob context:%d", rc ) ;
         goto error ;
      }

      rc = lobContext->getLobMetaData( meta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get meta data:%d", rc ) ;
         goto error ;
      }

      if ( NULL != dpsCB && 1 < w )
      {
         dpsCB->completeOpr( cb, w ) ;
         cb->resetLsn () ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNOPENLOB, rc ) ;
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREADLOB, "rtnReadLob" )
   INT32 rtnReadLob( SINT64 contextID,
                     pmdEDUCB *cb,
                     UINT32 len,
                     SINT64 offset,
                     const CHAR **buf,
                     UINT32 &read )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNREADLOB ) ;
      rtnContextLob *lobContext = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      rtnContextBuf contextBuf ;
      rtnContext *context = rtnCB->contextFind ( contextID ) ;
      if ( NULL == context )
      {
         PD_LOG ( PDERROR, "Context %lld does not exist", contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }
      
      if ( !cb->contextFind ( contextID ) )
      {
         PD_LOG ( PDERROR, "Context %lld does not owned by current session",
                  contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( RTN_CONTEXT_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "it is not a lob context, invalid context type:%d"
                 ", contextID:%lld", context->getType(), contextID ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      lobContext = ( rtnContextLob * )context ;
      rc = lobContext->read( len, offset, cb ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_EOF != rc )
         {
            PD_LOG( PDERROR, "failed to read lob:%d", rc ) ;
         }

         goto error ;
      }

      rc = lobContext->getMore( -1, contextBuf, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get more from context:%d", rc ) ;
         goto error ;
      }

      *buf = contextBuf.data() ;
      read = contextBuf.size() ;
   done:
      PD_TRACE_EXITRC( SDB_RTNREADLOB, rc ) ;
      return rc ;
   error:
      if ( SDB_EOF != rc )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNWRITELOB, "rtnWriteLob" )
   INT32 rtnWriteLob( SINT64 contextID,
                      pmdEDUCB *cb,
                      UINT32 len,
                      const CHAR *buf )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNWRITELOB ) ;
      rtnContextLob *lobContext = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      rtnContext *context = rtnCB->contextFind ( contextID ) ;
      if ( NULL == context )
      {
         PD_LOG ( PDERROR, "Context %lld does not exist", contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( !cb->contextFind ( contextID ) )
      {
         PD_LOG ( PDERROR, "Context %lld does not owned by current session",
                  contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( RTN_CONTEXT_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "it is not a lob context, invalid context type:%d"
                 ", contextID:%lld", context->getType(), contextID ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      lobContext = ( rtnContextLob * )context ;
      rc = lobContext->write( len, buf, cb ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_EOF != rc )
         {
            PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
         }

         goto error ;
      }      
   done:
      PD_TRACE_EXITRC( SDB_RTNWRITELOB, rc ) ;
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCLOSELOB, "rtnCloseLob" )
   INT32 rtnCloseLob( SINT64 contextID,
                     pmdEDUCB *cb )
   {
      
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCLOSELOB ) ;
      rtnContextLob *lobContext = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      rtnContext *context = rtnCB->contextFind ( contextID ) ;
      if ( NULL == context )
      {
         goto done ;
      }

      if ( !cb->contextFind ( contextID ) )
      {
         PD_LOG ( PDERROR, "Context %lld does not owned by current session",
                  contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( RTN_CONTEXT_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "it is not a lob context, invalid context type:%d"
                 ", contextID:%lld", context->getType(), contextID ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      lobContext = ( rtnContextLob * )context ;
      rc = lobContext->close( cb ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_EOF != rc )
         {
            PD_LOG( PDERROR, "failed to close lob:%d", rc ) ;
         }

         goto error ;
      }
   done:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNCLOSELOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREMOVELOB, "rtnRemoveLob" )
   INT32 rtnRemoveLob( const BSONObj &meta,
                       SINT16 w,
                       _pmdEDUCB *cb,
                       SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNREMOVELOB ) ;
      _rtnLocalLobStream stream ;
      BSONElement fullName ;
      BSONElement oidEle ;
      bson::OID oid ;

      fullName = meta.getField( FIELD_NAME_COLLECTION ) ;
      if ( String != fullName.type() )
      {
         PD_LOG( PDERROR, "invalid type of full name:%s",
                 meta.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      oidEle = meta.getField( FIELD_NAME_LOB_OID ) ;
      if ( jstOID != oidEle.type() )
      {
         PD_LOG( PDERROR, "invalid type of full oid:%s",
                 meta.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      oid = oidEle.OID() ;

      rc = stream.open( fullName.valuestr(),
                        oid, SDB_LOB_MODE_REMOVE,
                        cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to remove lob:%s, rc:%d",
                 oid.str().c_str(), rc ) ;
         goto error ;
      }
      else
      {
      }

      rc = stream.truncate( 0, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "faield to truncate lob:%d", rc ) ;
         goto error ;
      }

      rc = stream.close( cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to remove lob:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNREMOVELOB, rc ) ;
      return rc ;
   error:
     goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNGETLOBMETADATA, "rtnGetLobMetaData" )
   INT32 rtnGetLobMetaData( SINT64 contextID,
                            pmdEDUCB *cb, 
                            BSONObj &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNGETLOBMETADATA ) ;
      rtnContextLob *lobContext = NULL ;
      SDB_RTNCB *rtnCB = sdbGetRTNCB() ;
      rtnContext *context = rtnCB->contextFind ( contextID ) ;
      if ( NULL == context )
      {
         PD_LOG ( PDERROR, "Context %lld does not exist", contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( !cb->contextFind ( contextID ) )
      {
         PD_LOG ( PDERROR, "Context %lld does not owned by current session",
                  contextID ) ;
         rc = SDB_RTN_CONTEXT_NOTEXIST ;
         goto error ;
      }

      if ( RTN_CONTEXT_LOB != context->getType() )
      {
         PD_LOG( PDERROR, "it is not a lob context, invalid context type:%d"
                 ", contextID:%lld", context->getType(), contextID ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      lobContext = ( rtnContextLob * )context ;
      rc = lobContext->getLobMetaData( meta ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_EOF != rc )
         {
            PD_LOG( PDERROR, "failed to get lob meta data:%d", rc ) ;
         }

         goto error ;
      }      
   done:
      PD_TRACE_EXITRC( SDB_RTNGETLOBMETADATA, rc ) ;
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPREPARE4WRITELOB, "rtnPrepage4WriteLob" )
   static INT32 rtnPrepage4WriteLob( const CHAR *fullName,
                                     _pmdEDUCB *cb,
                                     BOOLEAN acquireLock,
                                     _dmsStorageUnit *&su,
                                     _dmsMBContext *&mbContext )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNPREPARE4WRITELOB ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      dmsStorageUnitID suID = DMS_INVALID_CS ;
      const CHAR *clName = NULL ;
      BOOLEAN lockDms = FALSE ;
      _dmsStorageUnit *tmpSu = NULL ;
      _dmsMBContext *tmpMb = NULL ;

      rc = rtnResolveCollectionNameAndLock( fullName, dmsCB,
                                            &tmpSu, &clName, suID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to resolve collection:%s, rc:%d",
                 fullName, rc ) ;
         goto error ;
      }

      rc = tmpSu->data()->getMBContext( &tmpMb, clName, -1 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to resolve collection name:%s, rc:%d",
                 clName, rc ) ;
         goto error ;
      }

      rc = dmsCB->writable( cb ) ;
      if ( SDB_OK !=rc )
      {
         PD_LOG ( PDERROR, "database is not writable, rc = %d", rc ) ;
         goto error ;
      }
      lockDms = TRUE ;

      if ( acquireLock )
      {
         rc = tmpMb->mbLock( EXCLUSIVE ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to get exclusive lock:%d", rc ) ;
            goto error ;
         }
      }

      su = tmpSu ;
      mbContext = tmpMb ;
   done:
      PD_TRACE_EXITRC( SDB_RTNPREPARE4WRITELOB, rc ) ;
      return rc ;
   error:
      if ( NULL != tmpMb &&
           tmpMb->isMBLock() )
      {
         tmpMb->mbUnlock() ;
      }
       
      if ( NULL != tmpMb && NULL != tmpSu )
      {
         tmpSu->data()->releaseMBContext( tmpMb ) ;
         tmpMb = NULL ;
      }

      if ( NULL != tmpSu )
      {
         dmsCB->suUnlock ( tmpSu->CSID() ) ;
         tmpSu = NULL ;
      }

      if ( lockDms )
      {
         dmsCB->writeDown() ;
      }

      su = NULL ;
      mbContext = NULL ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNWRITELOBDONE, "rtnWriteLobDone" )
   static void rtnWriteLobDone( _pmdEDUCB *cb,
                                _dmsStorageUnit *&su,
                                _dmsMBContext *&mbContext )
   {
      PD_TRACE_ENTRY( SDB_RTNWRITELOBDONE ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      if ( mbContext->isMBLock() )
      {
         mbContext->mbUnlock() ;
      }
      su->data()->releaseMBContext( mbContext ) ;
      dmsCB->suUnlock ( su->CSID() ) ;
      dmsCB->writeDown() ;
      su = NULL ;
      mbContext = NULL ;
      PD_TRACE_EXIT( SDB_RTNWRITELOBDONE ) ;
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCREATELOB, "rtnCreateLob" )
   INT32 rtnCreateLob( const CHAR *fullName,
                       const bson::OID &oid,
                       pmdEDUCB *cb,
                       SINT16 w,
                       SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCREATELOB ) ;
      SDB_ASSERT( NULL != fullName && NULL != cb, "can not be null" ) ;
      _dmsStorageUnit *su = NULL ;
      _dmsMBContext *mbContext = NULL ;
      _dmsLobMeta meta ;
      BOOLEAN prepared = FALSE ;
   
      rc = rtnPrepage4WriteLob( fullName, cb, TRUE,
                                su, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to write lob:%d", rc ) ;
         goto error ;
      }
      prepared = TRUE ;

      rc = su->lob()->getLobMeta( oid, mbContext,
                                  cb, meta ) ;
      if ( SDB_FNE == rc )
      {
         rc = SDB_OK ;
      }
      else if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get meta data of lob:%d", rc ) ;
         goto error ;
      }
      else if ( meta.isDone() )
      {
         PD_LOG( PDERROR, "lob[%s] exists", oid.str().c_str() ) ;
         rc = SDB_FE ;
         goto error ;
      }
      else
      {
         PD_LOG( PDERROR, "lob[%s] is not available",
                 oid.str().c_str() ) ;
         rc = SDB_LOB_IS_NOT_AVAILABLE ;
         goto error ;
      }

      rc = su->lob()->writeLobMeta( oid, mbContext,
                                    cb, meta, TRUE,
                                    dpsCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to ensure meta data:%d", rc ) ;
         goto error ;
      }

      if ( NULL != dpsCB && 1 < w )
      {
         dpsCB->completeOpr( cb, w ) ;
         cb->resetLsn () ;
      }
   done:
      if ( prepared )
      {
         rtnWriteLobDone( cb, su, mbContext ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNCREATELOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNWRITELOB2, "rtnWriteLob" ) 
   INT32 rtnWriteLob( const CHAR *fullName,
                      const bson::OID &oid,
                      UINT32 sequence,
                      UINT32 offset,
                      UINT32 len,
                      const CHAR *data,
                      pmdEDUCB *cb,
                      SINT16 w,
                      SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNWRITELOB2 ) ;
      SDB_ASSERT( NULL != fullName && NULL != cb, "can not be null" ) ;
      _dmsStorageUnit *su = NULL ;
      _dmsMBContext *mbContext = NULL ;
      _dmsLobRecord record ;
      BOOLEAN prepared = FALSE ;

      rc = rtnPrepage4WriteLob( fullName, cb, FALSE,
                                su, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to write lob:%d", rc ) ;
         goto error ;
      }
      prepared = TRUE ;

      record.set( &oid, sequence, offset,
                  len, data ) ;
      rc = su->lob()->write( record, mbContext, cb,
                             dpsCB ) ;

      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
         goto error ;
      }

      if ( NULL != dpsCB && 1 < w )
      {
         dpsCB->completeOpr( cb, w ) ;
         cb->resetLsn () ;
      }   
   done:
      if ( prepared )
      {
         rtnWriteLobDone( cb, su, mbContext ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNWRITELOB2, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCLOSELOB2, "rtnCloseLob" )
   INT32 rtnCloseLob( const CHAR *fullName,
                      const bson::OID &oid,
                      const dmsLobMeta &meta,
                      pmdEDUCB *cb,
                      SINT16 w,
                      SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCLOSELOB2 ) ;
      SDB_ASSERT( NULL != fullName && NULL != cb, "can not be null" ) ;
      _dmsStorageUnit *su = NULL ;
      _dmsMBContext *mbContext = NULL ;
      BOOLEAN prepared = FALSE ;

      rc = rtnPrepage4WriteLob( fullName, cb, FALSE,
                                su, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to write lob:%d", rc ) ;
         goto error ;
      }
      prepared = TRUE ;

      rc = su->lob()->writeLobMeta( oid, mbContext, cb,
                                    meta, FALSE, dpsCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write meta data of lob:%d", rc ) ;
         goto error ;
      }

      if ( NULL != dpsCB && 1 < w )
      {
         dpsCB->completeOpr( cb, w ) ;
         cb->resetLsn () ;
      }
   done:
      if ( prepared )
      {
         rtnWriteLobDone( cb, su, mbContext ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNCLOSELOB2, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPREPARE4READLOB, "rtnPrepage4ReadLob" )
   static INT32 rtnPrepage4ReadLob( const CHAR *fullName,
                                    _dmsStorageUnit *&su,
                                    _dmsMBContext *&mbContext )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNPREPARE4READLOB ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      dmsStorageUnitID suID = DMS_INVALID_CS ;
      const CHAR *clName = NULL ;
      BOOLEAN lockDms = FALSE ;
      _dmsStorageUnit *tmpSu = NULL ;
      _dmsMBContext *tmpMb = NULL ;

      rc = rtnResolveCollectionNameAndLock( fullName, dmsCB,
                                            &tmpSu, &clName, suID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to resolve collection:%s, rc:%d",
                 fullName, rc ) ;
         goto error ;
      }

      rc = tmpSu->data()->getMBContext( &tmpMb, clName, -1 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to resolve collection name:%s",
                 clName ) ;
         goto error ;
      }

      su = tmpSu ;
      mbContext = tmpMb ;
   done:
      PD_TRACE_EXITRC( SDB_RTNPREPARE4READLOB, rc ) ;
      return rc ;
   error:
      if ( NULL != tmpMb &&
           tmpMb->isMBLock() )
      {
         tmpMb->mbUnlock() ;
      }

      if ( NULL != tmpMb && NULL != tmpSu )
      {
         tmpSu->data()->releaseMBContext( tmpMb ) ;
         tmpMb = NULL ;
      }

      if ( NULL != tmpSu )
      {
         dmsCB->suUnlock ( tmpSu->CSID() ) ;
         tmpSu = NULL ;
      }

      if ( lockDms )
      {
         dmsCB->writeDown() ;
      }

      su = NULL ;
      mbContext = NULL ;
      goto done ;
   }

   static void rtnReadLobDone( _pmdEDUCB *cb,
                               _dmsStorageUnit *&su,
                               _dmsMBContext *&mbContext )
   {
      PD_TRACE_ENTRY( SDB_RTNWRITELOBDONE ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      su->data()->releaseMBContext( mbContext ) ;
      dmsCB->suUnlock ( su->CSID() ) ;
      su = NULL ;
      mbContext = NULL ;
      PD_TRACE_EXIT( SDB_RTNWRITELOBDONE ) ;
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNGETLOBMETADATA2, "rtnGetLobMetaData" )
   INT32 rtnGetLobMetaData( const CHAR *fullName,
                            const bson::OID &oid,
                            pmdEDUCB *cb,
                            dmsLobMeta &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNGETLOBMETADATA2 ) ;
      _dmsStorageUnit *su = NULL ;
      _dmsMBContext *mbContext = NULL ;
      _dmsLobRecord record ;
      BOOLEAN prepared = FALSE ;

      rc = rtnPrepage4ReadLob( fullName, 
                               su, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to read lob:%d", rc ) ;
         goto error ;
      }
      prepared = TRUE ;

      rc = su->lob()->getLobMeta( oid, mbContext,
                                  cb, meta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read lob[%s] in collection[%s], rc:%d",
                 oid.str().c_str(), fullName, rc ) ;
         goto error ;
      }

      if ( !meta.isDone() )
      {
         PD_LOG( PDERROR, "lob[%s] is not available",
                 oid.str().c_str() ) ;
         rc = SDB_LOB_IS_NOT_AVAILABLE ;
         goto error ;
      }
   done:
      if ( prepared )
      {
         rtnReadLobDone( cb, su, mbContext ) ; 
      }
      PD_TRACE_EXITRC( SDB_RTNGETLOBMETADATA2, rc ) ;
      return rc ;
   error:
      meta.clear() ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREADLOB2, "rtnReadLob" )
   INT32 rtnReadLob( const CHAR *fullName,
                     const bson::OID &oid,
                     UINT32 sequence,
                     UINT32 offset,
                     UINT32 len,
                     pmdEDUCB *cb,
                     CHAR *data,
                     UINT32 &read )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNREADLOB2 ) ;
      _dmsStorageUnit *su = NULL ;
      _dmsMBContext *mbContext = NULL ;
      _dmsLobRecord record ;
      BOOLEAN prepared = FALSE ;
      const CHAR *np = NULL ;

      rc = rtnPrepage4ReadLob( fullName,
                               su, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to read lob:%d", rc ) ;
         goto error ;
      }
      prepared = TRUE ;

      record.set( &oid, sequence, offset, len, np ) ;
      rc = su->lob()->read( record, mbContext, cb,
                            data, read ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read lob:%d", rc ) ;
         goto error ;
      }
   done:
      if ( prepared )
      {
         rtnReadLobDone( cb, su, mbContext ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNREADLOB2, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREMOVELOBPIECE, "rtnRemoveLobPiece" )
   INT32 rtnRemoveLobPiece( const CHAR *fullName,
                            const bson::OID &oid,
                            UINT32 sequence,
                            pmdEDUCB *cb,
                            SINT16 w,
                            SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNREMOVELOBPIECE ) ;
      dmsStorageUnit *su = NULL ;
      _dmsMBContext *mbContext = NULL ;
      _dmsLobRecord record ;
      BOOLEAN prepared = FALSE ;

      rc = rtnPrepage4WriteLob( fullName, cb, FALSE,
                                su, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to write lob:%d", rc ) ;
         goto error ;
      }
      prepared = TRUE ;

      record.set( &oid, sequence, 0, 0, NULL ) ;
      rc = su->lob()->remove( record, mbContext, cb,
                              dpsCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to remove lob[%s],"
                 "sequence:%d, rc:%d", oid.str().c_str(),
                 sequence, rc ) ;
         goto error ;
      }

      if ( NULL != dpsCB && 1 < w )
      {
         dpsCB->completeOpr( cb, w ) ;
         cb->resetLsn () ;
      }
   done:
      if ( prepared )
      {
         rtnWriteLobDone( cb, su, mbContext ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNREMOVELOBPIECE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNQUERYANDINVALIDAGELOB, "rtnQueryAndInvalidateLob" )
   INT32 rtnQueryAndInvalidateLob( const CHAR *fullName,
                                   const bson::OID &oid,
                                   pmdEDUCB *cb,
                                   SINT16 w,
                                   SDB_DPSCB *dpsCB,
                                   dmsLobMeta &meta )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNQUERYANDINVALIDAGELOB ) ;
      BOOLEAN prepared = FALSE ;
      dmsStorageUnit *su = NULL ;
      _dmsMBContext *mbContext = NULL ;
      dmsLobMeta lobMeta ;

      rc = rtnPrepage4WriteLob( fullName, cb, TRUE,
                                su, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to write lob:%d", rc ) ;
         goto error ;
      }
      prepared = TRUE ;

      rc = su->lob()->getLobMeta( oid, mbContext, cb,
                                  lobMeta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get lob meta[%s], rc:%d",
                 oid.str().c_str(), rc ) ;
         goto error ;
      }

      if ( !lobMeta.isDone() )
      {
         rc = SDB_LOB_IS_NOT_AVAILABLE ;
         goto error ;
      }

      meta = lobMeta ;
      lobMeta._status = DMS_LOB_UNCOMPLETE ;

      rc = su->lob()->writeLobMeta( oid, mbContext, cb,
                                    lobMeta, FALSE, dpsCB ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to invalidate lob[%s], rc:%d",
                 oid.str().c_str(), rc ) ;
         goto error ;
      }

   done:
      if ( prepared )
      {
         rtnWriteLobDone( cb, su, mbContext ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNQUERYANDINVALIDAGELOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNUPDATELOB, "rtnUpdateLob" ) 
   INT32 rtnUpdateLob( const CHAR *fullName,
                       const bson::OID &oid,
                       UINT32 sequence,
                       UINT32 offset,
                       UINT32 len,
                       const CHAR *data,
                       pmdEDUCB *cb,
                       SINT16 w,
                       SDB_DPSCB *dpsCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNUPDATELOB ) ;
      SDB_ASSERT( NULL != fullName && NULL != cb, "can not be null" ) ;
      _dmsStorageUnit *su = NULL ;
      _dmsMBContext *mbContext = NULL ;
      _dmsLobRecord record ;
      BOOLEAN prepared = FALSE ;

      rc = rtnPrepage4WriteLob( fullName, cb, FALSE,
                                su, mbContext ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to prepare to write lob:%d", rc ) ;
         goto error ;
      }
      prepared = TRUE ;

      record.set( &oid, sequence, offset,
                  len, data ) ;

      rc = su->lob()->update( record, mbContext, cb,
                              dpsCB ) ;

      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to update lob:%d", rc ) ;
         goto error ;
      }

      if ( NULL != dpsCB && 1 < w )
      {
         dpsCB->completeOpr( cb, w ) ;
         cb->resetLsn () ;
      }
   done:
      if ( prepared )
      {
         rtnWriteLobDone( cb, su, mbContext ) ;
      }
      PD_TRACE_EXITRC( SDB_RTNUPDATELOB, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

