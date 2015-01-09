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

   Source File Name = rtnBackgroundJob.cpp

   Descriptive Name = Data Management Service Header

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          03/06/2013  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnBackgroundJob.hpp"
#include "rtn.hpp"
#include "ixm.hpp"
#include "dmsStorageUnit.hpp"
#include "dmsStorageLoadExtent.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace engine
{


   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNINDEXJOB__RTNINDEXJOB, "_rtnIndexJob::_rtnIndexJob" )
   _rtnIndexJob::_rtnIndexJob ( RTN_JOB_TYPE type, const CHAR *pCLName,
                                const BSONObj & indexObj, SDB_DPSCB * dpsCB)
   {
      PD_TRACE_ENTRY ( SDB__RTNINDEXJOB__RTNINDEXJOB ) ;
      _type = type ;
      ossMemcpy ( _clFullName, pCLName, DMS_COLLECTION_FULL_NAME_SZ ) ;
      _clFullName[DMS_COLLECTION_FULL_NAME_SZ] = 0 ;
      _indexObj = indexObj.copy() ;
      _dpsCB = dpsCB ;
      _dmsCB = pmdGetKRCB()->getDMSCB() ;
      PD_TRACE_EXIT ( SDB__RTNINDEXJOB__RTNINDEXJOB ) ;
   }

   _rtnIndexJob::~_rtnIndexJob ()
   {
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNINDEXJOB_INIT, "_rtnIndexJob::init ()" )
   INT32 _rtnIndexJob::init ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNINDEXJOB_INIT ) ;
      dmsStorageUnitID suID = DMS_INVALID_SUID ;

      switch ( _type )
      {
         case RTN_JOB_CREATE_INDEX :
            {
               _jobName = "CreateIndex-" ;
               _indexName = _indexObj.getStringField( IXM_NAME_FIELD ) ;
            }
            break ;
         case RTN_JOB_DROP_INDEX :
            {
               _jobName = "DropIndex-" ;
               _indexEle = _indexObj.getField( IXM_NAME_FIELD ) ;
               if ( _indexEle.eoo() )
               {
                  _indexEle = _indexObj.firstElement () ;
               }

               if ( jstOID == _indexEle.type() )
               {
                  OID oid ;
                  const CHAR *pCLShortName = NULL ;
                  dmsStorageUnit *su = NULL ;
                  dmsMBContext *mbContext = NULL ;
                  dmsExtentID idxExtent = DMS_INVALID_EXTENT ;

                  rc = rtnResolveCollectionNameAndLock ( _clFullName, _dmsCB,
                                                         &su, &pCLShortName,
                                                         suID ) ;
                  if ( SDB_OK != rc )
                  {
                     PD_LOG ( PDERROR, "Failed to resolve collection name %s",
                              _clFullName ) ;
                     goto error ;
                  }

                  rc = su->data()->getMBContext( &mbContext, pCLShortName,
                                                 SHARED ) ;
                  if ( SDB_OK != rc )
                  {
                     PD_LOG ( PDERROR, "Lock collection[%s] failed, rc = %d",
                              _clFullName, rc ) ;
                     goto error ;
                  }

                  _indexEle.Val( oid ) ;
                  rc = su->index()->getIndexCBExtent( mbContext, oid,
                                                      idxExtent ) ;
                  if ( SDB_OK != rc )
                  {
                     su->data()->releaseMBContext( mbContext ) ;
                     PD_LOG ( PDERROR, "Get collection[%s] indexCB extent "
                              "failed, rc = %d", _clFullName, rc ) ;
                     goto error ;
                  }

                  ixmIndexCB indexCB ( idxExtent, su->index(), NULL ) ;
                  _indexName = indexCB.getName() ;

                  su->data()->releaseMBContext( mbContext ) ;
                  _dmsCB->suUnlock( suID ) ;
                  suID = DMS_INVALID_SUID ;
               }
               else
               {
                  _indexName = _indexEle.str () ;
               }
            }
            break ;
         default :
            _jobName = "UnknowIndexJob" ;
            PD_LOG ( PDERROR, "Index job not support this type[%d]", _type ) ;
            rc = SDB_INVALIDARG ;
            break ;
      }

      if ( SDB_OK == rc )
      {
         _jobName += _clFullName ;
         _jobName += "[" ;
         _jobName += _indexName ;
         _jobName += "]" ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__RTNINDEXJOB_INIT, rc ) ;
      return rc ;
   error:
      if ( DMS_INVALID_SUID != suID )
      {
         _dmsCB->suUnlock( suID ) ;
      }
      goto done ;
   }

   const CHAR* _rtnIndexJob::getIndexName () const
   {
      return _indexName.c_str() ;
   }

   const CHAR* _rtnIndexJob::getCollectionName() const
   {
      return _clFullName ;
   }

   RTN_JOB_TYPE _rtnIndexJob::type () const
   {
      return _type ;
   }

   const CHAR* _rtnIndexJob::name () const
   {
      return _jobName.c_str() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNINDEXJOB_MUTEXON, "_rtnIndexJob::muteXOn" )
   BOOLEAN _rtnIndexJob::muteXOn ( const _rtnBaseJob * pOther )
   {
      PD_TRACE_ENTRY ( SDB__RTNINDEXJOB_MUTEXON ) ;
      BOOLEAN ret = FALSE;
      if ( RTN_JOB_CREATE_INDEX != pOther->type() &&
           RTN_JOB_DROP_INDEX != pOther->type() )
      {
         ret = FALSE ;
         goto done ;
      }

      {
         _rtnIndexJob *pIndexJob = ( _rtnIndexJob* )pOther ;

         if ( 0 == ossStrcmp( getIndexName(), pIndexJob->getIndexName() ) &&
              0 == ossStrcmp( getCollectionName(),
                              pIndexJob->getCollectionName() ) )
         {
            ret = TRUE ;
            goto done ;
         }
      }
   done :
      PD_TRACE_EXIT ( SDB__RTNINDEXJOB_MUTEXON ) ;
      return ret ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNINDEXJOB_DOIT , "_rtnIndexJob::doit" )
   INT32 _rtnIndexJob::doit ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNINDEXJOB_DOIT ) ;

      switch ( _type )
      {
         case RTN_JOB_CREATE_INDEX :
            rc = rtnCreateIndexCommand( _clFullName, _indexObj, eduCB(),
                                        _dmsCB, _dpsCB, TRUE ) ;
            break ;
         case RTN_JOB_DROP_INDEX :
            rc = rtnDropIndexCommand( _clFullName, _indexEle, eduCB(),
                                      _dmsCB, _dpsCB, TRUE ) ;
            break ;
         default :
            PD_LOG ( PDERROR, "Index job not support this type[%d]", _type ) ;
            rc = SDB_INVALIDARG ;
            break ;
      }

      PD_TRACE_EXITRC ( SDB__RTNINDEXJOB_DOIT, rc ) ;
      return rc ;
   }

   RTN_JOB_TYPE _rtnLoadJob::type () const
   {
      return RTN_JOB_LOAD ;
   }

   const CHAR* _rtnLoadJob::name () const
   {
      return _jobName.c_str() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNLOADJOB_MUTEXON, "_rtnLoadJob::muteXOn" )
   BOOLEAN _rtnLoadJob::muteXOn ( const _rtnBaseJob * pOther )
   {
      PD_TRACE_ENTRY ( SDB__RTNLOADJOB_MUTEXON ) ;
      BOOLEAN ret = FALSE;
      PD_TRACE_EXIT ( SDB__RTNLOADJOB_MUTEXON ) ;
      return ret ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNLOADJOB_DOIT , "_rtnLoadJob::doit" )
   INT32 _rtnLoadJob::doit ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNLOADJOB_DOIT ) ;
      dmsStorageUnitID  suID     = DMS_INVALID_CS ;
      dmsStorageUnit   *su       = NULL ;
      pmdKRCB          *krcb     = pmdGetKRCB () ;
      SDB_DMSCB        *dmsCB    = krcb->getDMSCB () ;
      pmdEDUMgr        *eduMgr   = krcb->getEDUMgr () ;
      pmdEDUCB         *eduCB    = eduMgr->getEDU() ;
      dmsStorageLoadOp dmsLoadExtent ;
      std::set<monCollectionSpace> csList ;
      std::set<monCollectionSpace>::iterator it ;

      if ( SDB_ROLE_STANDALONE != krcb->getDBRole() &&
           SDB_ROLE_DATA != krcb->getDBRole() )
      {
         goto done ;
      }

      dmsCB->dumpInfo ( csList ) ;

      for ( it = csList.begin(); it != csList.end(); ++it )
      {
         std::set<monCollection> clList ;
         std::set<monCollection>::iterator itCollection ;
         rc = rtnCollectionSpaceLock ( (*it)._name,
                                       dmsCB,
                                       FALSE,
                                       &su,
                                       suID ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to lock collection space, rc=%d", rc ) ;
            goto error ;
         }
   
         dmsLoadExtent.init ( su ) ;

         su->dumpInfo ( clList ) ;
         for ( itCollection = clList.begin();
               itCollection != clList.end();
               ++itCollection )
         {
            dmsMBContext *mbContext = NULL ;
            UINT16 collectionFlag = 0 ;
            const CHAR *pCLNameTemp = NULL ;
            const CHAR *pCLName = (*itCollection)._name ;

            if ( ( ossStrlen ( pCLName ) > DMS_COLLECTION_FULL_NAME_SZ ) ||
                    ( NULL == ( pCLNameTemp = ossStrrchr ( pCLName, '.' ))) )
            {
               PD_LOG ( PDERROR, "collection name is not valid: %s",
                        pCLName ) ;
               continue ;
            }

            rc = su->data()->getMBContext( &mbContext, pCLNameTemp + 1,
                                           EXCLUSIVE ) ;
            if ( rc )
            {
               PD_LOG( PDERROR, "Failed to lock collection: %s, rc: %d",
                       pCLName, rc ) ;
               continue ;
            }
            collectionFlag = mbContext->mb()->_flag ;


            if ( DMS_IS_MB_FLAG_LOAD_LOAD ( collectionFlag ) )
            {
               PD_LOG ( PDEVENT, "Start Rollback" ) ;
               rc = dmsLoadExtent.loadRollbackPhase ( mbContext ) ;
               if ( rc )
               {
                  su->data()->releaseMBContext( mbContext ) ;
                  PD_LOG ( PDERROR, "Failed to load Rollback Phase, rc=%d", rc ) ;
                  continue ;
               }
               dmsLoadExtent.clearFlagLoadLoad ( mbContext->mb() ) ;
            }
            if ( DMS_IS_MB_FLAG_LOAD_BUILD ( collectionFlag ) )
            {
               PD_LOG ( PDEVENT, "Start loadBuild" ) ;
               rc = dmsLoadExtent.loadBuildPhase ( mbContext,
                                                   eduCB ) ;
               if ( rc )
               {
                  su->data()->releaseMBContext( mbContext ) ;
                  PD_LOG ( PDERROR, "Failed to load build Phase, rc=%d", rc ) ;
                  continue ;
               }
               dmsLoadExtent.clearFlagLoadBuild ( mbContext->mb() ) ;
            }
            if ( DMS_IS_MB_LOAD ( collectionFlag ) )
            {
               PD_LOG ( PDEVENT, "Start clear load flag" ) ;
               dmsLoadExtent.clearFlagLoad ( mbContext->mb() ) ;
            }

            su->data()->releaseMBContext( mbContext ) ;
         }
         dmsCB->suUnlock ( suID ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__RTNLOADJOB_DOIT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNSTARTLOADJOB, "rtnStartLoadJob" )
   INT32 rtnStartLoadJob()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNSTARTLOADJOB );
      rtnLoadJob *loadJob = SDB_OSS_NEW rtnLoadJob() ;
      if ( NULL == loadJob )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for loadJob" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( loadJob, RTN_JOB_MUTEX_NONE, NULL ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to start load job, rc: %d", rc ) ;

   done :
      PD_TRACE_EXITRC ( SDB_RTNSTARTLOADJOB, rc );
      return rc ;
   error :
      goto done ;
   }

}

