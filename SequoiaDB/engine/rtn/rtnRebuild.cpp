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

   Source File Name = rtnRebuild.cpp

   Descriptive Name = Runtime Database Rebuild

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime code for database
   rebuild.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "rtn.hpp"
#include "dmsStorageUnit.hpp"
#include "dmsReorgUnit.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace engine
{

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNREBUILDDB, "rtnRebuildDB" )
   INT32 rtnRebuildDB ( pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNREBUILDDB ) ;

      pmdKRCB *krcb = pmdGetKRCB () ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB () ;
      SDB_RTNCB *rtnCB = krcb->getRTNCB () ;
      BSONObj dummyObj ;
      std::set<monCollectionSpace> csList ;
      std::set<monCollectionSpace>::iterator it ;
      BOOLEAN registeredRebuild = FALSE ;
      UINT64 beginTime = 0 ;
      UINT64 endTime = 0 ;

      PD_LOG ( PDEVENT, "Start rebuilding database" ) ;

      rc = dmsCB->registerRebuild ( cb ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to register rebuild" ) ;
         goto error ;
      }
      registeredRebuild = TRUE ;
      beginTime = (UINT64)time( NULL ) ;

      dmsCB->dumpInfo ( csList, TRUE ) ;

      for ( it = csList.begin(); it != csList.end(); ++it )
      {
         const CHAR *pCSName = (*it)._name ;
         std::set<monCollection> clList ;
         std::set<monCollection>::iterator itCollection ;
         dmsStorageUnitID suID ;
         if ( ossStrlen ( pCSName ) > DMS_COLLECTION_SPACE_NAME_SZ )
         {
            PD_LOG ( PDERROR, "collection space name is not valid: %s",
                     pCSName ) ;
            continue ;
         }
         if ( ossStrncmp ( pCSName, SDB_DMSTEMP_NAME,
                           DMS_COLLECTION_SPACE_NAME_SZ ) == 0 )
         {
            continue ;
         }
         PD_LOG ( PDEVENT, "Start rebuilding collection space %s", pCSName ) ;
         dmsStorageUnit *su = NULL;
         rc = dmsCB->nameToSUAndLock ( pCSName, suID, &su ) ;
         if ( rc != SDB_OK )
         {
            PD_LOG ( PDERROR, "Failed to lock collection space %s", pCSName ) ;
            continue ;
         }

         do
         {
            UINT64 tCLBegin = 0 ;
            UINT64 tCLEnd = 0 ;
            su->dumpInfo ( clList, TRUE ) ;
            for ( itCollection = clList.begin();
                  itCollection != clList.end();
                  ++itCollection )
            {
               dmsMBContext *mbContext = NULL ;
               UINT16 collectionFlag ;
               const CHAR *pCLNameTemp = NULL ;
               const CHAR *pCLName = (*itCollection)._name ;

               if ( ( ossStrlen ( pCLName ) > DMS_COLLECTION_FULL_NAME_SZ ) ||
                    ( NULL == ( pCLNameTemp = ossStrrchr ( pCLName, '.' ))) )
               {
                  PD_LOG ( PDERROR, "collection name is not valid: %s",
                           pCLName ) ;
                  continue ;
               }
               rc = su->data()->getMBContext( &mbContext, pCLNameTemp+1,
                                              EXCLUSIVE ) ;
               if ( rc )
               {
                  PD_LOG ( PDWARNING, "Failed to lock collection %s, rc = %d",
                           pCLName, rc ) ;
                  continue ;
               }
               collectionFlag = mbContext->mb()->_flag ;
               su->data()->releaseMBContext( mbContext ) ;

               tCLBegin = (UINT64)time( NULL ) ;
               PD_LOG ( PDEVENT, "Start rebuilding collection %s", pCLName ) ;

               if ( DMS_IS_MB_OFFLINE_REORG( collectionFlag ) ||
                    DMS_IS_MB_ONLINE_REORG ( collectionFlag ) )
               {
                  rc = rtnReorgRecover ( pCLName, cb, dmsCB, rtnCB ) ;
                  if ( rc )
                  {
                     PD_LOG ( PDERROR, "Failed to perform reorg recover: %s, "
                              "rc = %d", pCLName, rc ) ;
                  }
               }
               else
               {
                  rc = rtnReorgOffline ( pCLName, dummyObj, cb,
                                         dmsCB, rtnCB, TRUE ) ;
                  if ( rc )
                  {
                     PD_LOG ( PDERROR, "Failed to perform offline reorg: %s, "
                              "rc = %d", pCLName, rc ) ;
                  }
               }

               tCLEnd = (UINT64)time(NULL) ;
               PD_LOG ( PDEVENT, "Complete rebuilding collection %s, "
                        "result: %d, cost %s", pCLName, rc,
                        utilTimeSpanStr( tCLEnd-tCLBegin ).c_str() ) ;

               if ( SDB_APP_INTERRUPT == rc )
               {
                  break ;
               }
            } // for
         } while ( 0 ) ;

         dmsCB->suUnlock ( suID ) ;
         PD_LOG ( PDEVENT, "Complete rebuilding collection space %s, "
                  "result: %d", pCSName, rc ) ;

         if ( SDB_APP_INTERRUPT == rc )
         {
            if ( PMD_IS_DB_UP )
            {
               PMD_SHUTDOWN_DB( rc ) ;
            }
            break ;
         }
      } // end for

      endTime = (UINT64)time(NULL) ;
      PD_LOG ( PDEVENT, "Complete rebuild database, result: %d, cost %s",
               rc, utilTimeSpanStr( endTime-beginTime ).c_str() ) ;

   done :
      if ( registeredRebuild )
      {
         dmsCB->rebuildDown ( cb ) ;
      }
      PD_TRACE_EXITRC ( SDB_RTNREBUILDDB, rc ) ;
      return rc ;
   error :
      goto done ;
   }

}

