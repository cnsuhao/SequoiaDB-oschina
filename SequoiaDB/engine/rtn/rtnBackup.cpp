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

   Source File Name = rtnInsert.cpp

   Descriptive Name = Runtime Insert

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime code for insert
   request.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "barBkupLogger.hpp"
#include "rtn.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

using namespace bson ;

namespace engine
{

   static string _rtnMakeDateDirName()
   {
      CHAR tmpBuff[ 20 ] = {0} ;
      time_t tmpTime = time( NULL ) ;
      struct tm localTm ;
      ossLocalTime( tmpTime, localTm ) ;

      ossSnprintf( tmpBuff, sizeof(tmpBuff) - 1,
                   "%04d%02d%02d",
                   localTm.tm_year+1900,            // 1) Year (UINT32)
                   localTm.tm_mon+1,                // 2) Month (UINT32)
                   localTm.tm_mday                  // 3) Day (UINT32)
                  ) ;
      return (string)tmpBuff ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNBACKUP, "rtnBackup" )
   INT32 rtnBackup( pmdEDUCB *cb, const CHAR *path, const CHAR *backupName,
                    BOOLEAN ensureInc, BOOLEAN rewrite, const CHAR *desp,
                    const BSONObj &option )
   {
      INT32 rc  = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNBACKUP ) ;
      string bkpath ;
      INT32 maxDataFileSize = 0 ;

      barBKOfflineLogger logger ;

      BOOLEAN isSubDir        = FALSE ;
      BOOLEAN enableDateDir   = FALSE ;
      const CHAR *prefix      = NULL ;

      rc = rtnGetBooleanElement( option, FIELD_NAME_ISSUBDIR, isSubDir ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_ISSUBDIR, rc ) ;

      rc = rtnGetIntElement( option, FIELD_NAME_MAX_DATAFILE_SIZE,
                             maxDataFileSize ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_MAX_DATAFILE_SIZE, rc ) ;

      rc = rtnGetBooleanElement( option, FIELD_NAME_ENABLE_DATEDIR,
                                 enableDateDir ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_ENABLE_DATEDIR, rc ) ;

      rc = rtnGetStringElement( option, FIELD_NAME_PREFIX, &prefix ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_PREFIX, rc ) ;

      if ( maxDataFileSize < BAR_MIN_DATAFILE_SIZE ||
           maxDataFileSize > BAR_MAX_DATAFILE_SIZE )
      {
         maxDataFileSize = BAR_DFT_DATAFILE_SIZE ;
      }

      if ( isSubDir && path )
      {
         bkpath = rtnFullPathName( pmdGetOptionCB()->getBkupPath(), path ) ;
      }
      else if ( path && 0 != path[0] )
      {
         bkpath = path ;
      }
      else
      {
         bkpath = pmdGetOptionCB()->getBkupPath() ;
      }

      if ( enableDateDir )
      {
         bkpath = rtnFullPathName( bkpath, _rtnMakeDateDirName() ) ;
      }

      rc = logger.init( bkpath.c_str(), backupName, maxDataFileSize, prefix,
                        ensureInc ? BAR_BACKUP_OP_TYPE_INC :
                        BAR_BACKUP_OP_TYPE_FULL, rewrite, desp ) ;
      PD_RC_CHECK( rc, PDERROR, "Init off line backup logger failed, rc: %d",
                   rc ) ;

      rc = logger.backup( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Off line backup failed, rc: %d", rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_RTNBACKUP, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnDumpBackups( const BSONObj &hint, rtnContextDump *context )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB () ;
      const CHAR *pPath = NULL ;
      const CHAR *backupName = NULL ;
      BOOLEAN isSubDir        = FALSE ;
      const CHAR *prefix      = NULL ;
      string bkpath ;
      BOOLEAN detail = FALSE ;
      barBackupMgr bkMgr( krcb->getGroupName() ) ;
      vector < BSONObj >  vecBackup ;
      UINT32 index = 0 ;

      rc = rtnGetStringElement( hint, FIELD_NAME_PATH, &pPath ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_PATH, rc ) ;

      rc = rtnGetStringElement( hint, FIELD_NAME_NAME, &backupName ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_NAME, rc ) ;
      rc = rtnGetBooleanElement( hint, FIELD_NAME_DETAIL, detail ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_DETAIL, rc ) ;

      rc = rtnGetBooleanElement( hint, FIELD_NAME_ISSUBDIR, isSubDir ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_ISSUBDIR, rc ) ;

      rc = rtnGetStringElement( hint, FIELD_NAME_PREFIX, &prefix ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_PREFIX, rc ) ;

      if ( isSubDir && pPath )
      {
         bkpath = rtnFullPathName( pmdGetOptionCB()->getBkupPath(), pPath ) ;
      }
      else if ( pPath && 0 != pPath[0] )
      {
         bkpath = pPath ;
      }
      else
      {
         bkpath = pmdGetOptionCB()->getBkupPath() ;
      }

      rc = bkMgr.init( bkpath.c_str(), backupName, prefix ) ;
      PD_RC_CHECK( rc, PDWARNING, "Init backup manager failed, rc: %d", rc ) ;

      rc = bkMgr.list( vecBackup, detail ) ;
      PD_RC_CHECK( rc, PDWARNING, "List backup failed, rc: %d", rc ) ;

      while ( index < vecBackup.size() )
      {
         rc = context->monAppend( vecBackup[index] ) ;
         PD_RC_CHECK( rc, PDERROR, "Add to obj[%s] to context failed, "
                      "rc: %d", vecBackup[index].toString().c_str(), rc ) ;
         ++index ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 rtnRemoveBackup( pmdEDUCB *cb, const CHAR *path,
                          const CHAR *backupName,
                          const BSONObj &option )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB () ;

      BOOLEAN isSubDir        = FALSE ;
      const CHAR *prefix      = NULL ;
      string bkpath ;

      barBackupMgr bkMgr( krcb->getGroupName() ) ;

      rc = rtnGetBooleanElement( option, FIELD_NAME_ISSUBDIR, isSubDir ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_ISSUBDIR, rc ) ;

      rc = rtnGetStringElement( option, FIELD_NAME_PREFIX, &prefix ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
      }
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   FIELD_NAME_PREFIX, rc ) ;

      if ( isSubDir && path )
      {
         bkpath = rtnFullPathName( pmdGetOptionCB()->getBkupPath(), path ) ;
      }
      else if ( path && 0 != path[0] )
      {
         bkpath = path ;
      }
      else
      {
         bkpath = pmdGetOptionCB()->getBkupPath() ;
      }

      rc = bkMgr.init( bkpath.c_str(), backupName, prefix ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init backup manager, rc: %d", rc ) ;

      rc = bkMgr.drop() ;
      PD_RC_CHECK( rc, PDERROR, "Failed to drop backup[%s], rc: %d",
                   bkMgr.backupName(), rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN rtnIsInBackup ()
   {
      return DMS_STATE_BACKUP == pmdGetKRCB()->getDMSCB()->getCBState() ?
             TRUE : FALSE ;
   }

}

