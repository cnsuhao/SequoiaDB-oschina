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

   Source File Name = barBkupLogger.cpp

   Descriptive Name = backup and recovery

   When/how to use: this program may be used on backup or restore db data.
   You can specfiy some options from parameters.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "barBkupLogger.hpp"
#include "pd.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "utilCommon.hpp"
#include "rtn.hpp"
#include "dmsStorageUnit.hpp"
#include "monDump.hpp"
#include "clsReplayer.hpp"
#include "pdTrace.hpp"
#include "barTrace.hpp"

#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

namespace fs = boost::filesystem ;
using namespace bson ;


namespace engine
{

   /*
      back up extent meta fields define
   */
   #define BAR_SU_NAME                    "suName"
   #define BAR_SU_SEQUENCE                "sequence"
   #define BAR_SU_FILE_NAME               "suFileName"
   #define BAR_SU_FILE_OFFSET             "offset"
   #define BAR_SU_FILE_TYPE               "type"

   /*
      back up extent meta fields values
   */
   #define BAR_SU_FILE_TYPE_DATA          "data"
   #define BAR_SU_FILE_TYPE_INDEX         "index"
   #define BAR_SU_FILE_TYPE_LOBM          "lobm"
   #define BAR_SU_FILE_TYPE_LOBD          "lobd"

   #define BAR_MAX_EXTENT_DATA_SIZE       (16777216)           // 16MB
   #define BAR_THINCOPY_THRESHOLD_SIZE    (16)                 // MB
   #define BAR_THINCOPY_THRESHOLD_RATIO   (0.1)

   /*
      _barBaseLogger implement
   */
   _barBaseLogger::_barBaseLogger ()
   {
      _pDMSCB        = NULL ;
      _pDPSCB        = NULL ;
      _pTransCB      = NULL ;
      _pOptCB        = NULL ;
      _pClsCB        = NULL ;

      _pDataExtent   = NULL ;
   }

   _barBaseLogger::~_barBaseLogger ()
   {
      if ( _pDataExtent )
      {
         SDB_OSS_DEL _pDataExtent ;
         _pDataExtent = NULL ;
      }
      _pDMSCB        = NULL ;
      _pDPSCB        = NULL ;
      _pTransCB      = NULL ;
      _pOptCB        = NULL ;
      _pClsCB        = NULL ;
   }

   string _barBaseLogger::_replaceWildcard( const CHAR * source )
   {
      if ( NULL == source )
      {
         return "" ;
      }
      pmdKRCB *krcb = pmdGetKRCB() ;
      string destStr ;
      UINT32 index = 0 ;
      BOOLEAN isFormat = FALSE ;

      while ( source[index] )
      {
         if ( isFormat )
         {
            isFormat = FALSE ;

            if ( 'G' == source[index] || 'g' == source[index] )
            {
               destStr += krcb->getGroupName() ;
            }
            else if ( 'H' == source[index] || 'h' == source[index] )
            {
               destStr += krcb->getHostName() ;
            }
            else if ( 'S' == source[index] || 's' == source[index] )
            {
               destStr += pmdGetOptionCB()->getServiceAddr() ;
            }
            else
            {
               destStr += source[index] ;
            }
         }
         else if ( '%' == source[index] )
         {
            isFormat = TRUE ;
            ++index ;
            continue ;
         }
         else
         {
            destStr += source[index] ;
         }
         ++index ;
      }
      return destStr ;
   }

   string _barBaseLogger::getIncFileName( UINT32 sequence )
   {
      return getIncFileName( _path, _backupName, sequence ) ;
   }

   string _barBaseLogger::getIncFileName( const string &backupName,
                                          UINT32 sequence )
   {
      return getIncFileName( _path, backupName, sequence ) ;
   }

   string _barBaseLogger::getIncFileName( const string &path,
                                          const string &backupName,
                                          UINT32 sequence )
   {
      CHAR tmp[ 15 ] = {0} ;
      ossSnprintf( tmp, sizeof(tmp)-1, ".%u", sequence ) ;
      string fileName = rtnFullPathName( path, backupName ) ;
      fileName += BAR_BACKUP_META_FILE_EXT ;
      if ( sequence > 0 )
      {
         fileName += tmp ;
      }
      return fileName ;
   }

   string _barBaseLogger::getMainFileName()
   {
      string fileName = rtnFullPathName( _path, _backupName ) ;
      fileName += BAR_BACKUP_META_FILE_EXT ;
      return fileName ;
   }

   BOOLEAN _barBaseLogger::parseMainFile( const string &fileName,
                                          string &backupName )
   {
      const CHAR *pFileName = fileName.c_str() ;
      const CHAR *pExt = ossStrrchr( pFileName , '.' ) ;

      if ( !pExt || pExt == pFileName ||
           0 != ossStrcmp( pExt, BAR_BACKUP_META_FILE_EXT ) )
      {
         return FALSE ;
      }

      backupName = fileName.substr( 0, pExt - pFileName ) ;
      return TRUE ;
   }

   string _barBaseLogger::getDataFileName( UINT32 sequence )
   {
      return getDataFileName( _backupName, sequence ) ;
   }

   string _barBaseLogger::getDataFileName( const string &backupName,
                                           UINT32 sequence )
   {
      SDB_ASSERT( sequence > 0, "sequence must > 0" ) ;

      CHAR tmp[ 15 ] = {0} ;
      ossSnprintf( tmp, sizeof(tmp)-1, ".%u", sequence ) ;
      string fileName = rtnFullPathName( _path, backupName ) ;
      fileName += tmp ;
      return fileName ;
   }

   UINT32 _barBaseLogger::_ensureMetaFileSeq ()
   {
      return _ensureMetaFileSeq( _path, _backupName ) ;
   }

   UINT32 _barBaseLogger::_ensureMetaFileSeq( const string &backupName )
   {
      return _ensureMetaFileSeq( _path, backupName ) ;
   }

   UINT32 _barBaseLogger::_ensureMetaFileSeq( const string &path,
                                              const string &backupName )
   {
      string fileName ;
      UINT32 metaFileSeq = 0 ;

      while ( TRUE )
      {
         fileName = getIncFileName( path, backupName, metaFileSeq ) ;
         if ( SDB_OK != ossAccess( fileName.c_str(), 0 ) )
         {
            break ;
         }
         ++metaFileSeq ;
      }
      return metaFileSeq ;
   }

   INT32 _barBaseLogger::_initInner ( const CHAR *path, const CHAR *backupName,
                                      const CHAR *prefix )
   {
      INT32 rc = SDB_OK ;

      pmdKRCB *krcb = pmdGetKRCB() ;
      _pDMSCB = krcb->getDMSCB() ;
      _pDPSCB = krcb->getDPSCB() ;
      _pTransCB = krcb->getTransCB() ;
      _pOptCB = krcb->getOptionCB() ;
      _pClsCB = krcb->getClsCB() ;

      if ( !_pDataExtent )
      {
         _pDataExtent = SDB_OSS_NEW barBackupExtentHeader ;
         if ( !_pDataExtent )
         {
            PD_LOG( PDERROR, "Failed to alloc memory for extent header" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }

      if ( !path || 0 == ossStrlen( path ) )
      {
         PD_LOG( PDWARNING, "Path can't be empty" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _metaHeader.makeBeginTime() ;
      if ( !backupName || 0 == ossStrlen( backupName ) )
      {
         _backupName = _metaHeader._startTimeStr ;
      }
      else
      {
         _backupName = backupName ;
      }
      _path          = _replaceWildcard ( path ) ;

      if ( prefix )
      {
         string tmpName = _replaceWildcard( prefix ) ;
         tmpName += "_" ;
         tmpName += _backupName ;
         _backupName = tmpName ;
      }

      _metaHeader.setPath( _path.c_str() ) ;
      _metaHeader.setName( _backupName.c_str(), NULL ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBaseLogger::_read( OSSFILE & file, CHAR * buf, SINT64 len )
   {
      INT32 rc = SDB_OK ;
      SINT64 read = 0 ;
      SINT64 needRead = len ;
      SINT64 bufOffset = 0 ;

      while ( 0 < needRead )
      {
         rc = ossRead( &file, buf + bufOffset, needRead, &read );
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG( PDWARNING, "Failed to read data, rc: %d", rc ) ;
            goto error ;
         }
         needRead -= read ;
         bufOffset += read ;

         rc = SDB_OK ;
      }

   done:
      return rc ;
   error:
      goto done;
   }

   INT32 _barBaseLogger::_flush( OSSFILE &file, const CHAR *buf,
                                 SINT64 len )
   {
      INT32 rc = SDB_OK;
      SINT64 written = 0;
      SINT64 needWrite = len;
      SINT64 bufOffset = 0;

      while ( 0 < needWrite )
      {
         rc = ossWrite( &file, buf + bufOffset, needWrite, &written );
         if ( rc && SDB_INTERRUPT != rc )
         {
            PD_LOG( PDWARNING, "Failed to write data, rc: %d", rc ) ;
            goto error ;
         }
         needWrite -= written ;
         bufOffset += written ;

         rc = SDB_OK ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBaseLogger::_readMetaHeader( UINT32 incID,
                                          barBackupHeader *pHeader,
                                          BOOLEAN check,
                                          UINT64 secretValue )
   {
      INT32 rc = SDB_OK ;
      OSSFILE file ;
      BOOLEAN isOpened = FALSE ;
      string fileName = getIncFileName( incID ) ;

      rc = ossOpen( fileName.c_str(), OSS_READONLY, OSS_RU|OSS_WU|OSS_RG,
                    file ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to open file[%s], rc: %d",
                   fileName.c_str(), rc ) ;
      isOpened = TRUE ;

      rc = _read( file, (CHAR*)pHeader, BAR_BACKUP_HEADER_SIZE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to read header from file[%s], rc: %d",
                   fileName.c_str(), rc ) ;

      if ( FALSE == check )
      {
         goto done ;
      }

      if ( 0 != ossStrncmp( pHeader->_eyeCatcher, BAR_BACKUP_META_EYECATCHER,
                            BAR_BACKUP_HEADER_EYECATCHER_LEN ) )
      {
         PD_LOG( PDWARNING, "Invalid eyecatcher[%s]", pHeader->_eyeCatcher ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }
      else if ( 0 != ossStrncmp( pHeader->_name, backupName(),
                                 BAR_BACKUP_NAME_LEN ) )
      {
         PD_LOG( PDWARNING, "Invalid backup name[%s], should be: %s",
                 pHeader->_name, backupName() ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }
      else if ( 0 != secretValue && secretValue != pHeader->_secretValue )
      {
         PD_LOG( PDERROR, "File[%s] header's secret value[%lld] is not the "
                 "same with [%lld]", fileName.c_str(),
                 pHeader->_secretValue, secretValue ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }

   done:
      if ( isOpened )
      {
         ossClose( file ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBaseLogger::_readDataHeader( OSSFILE &file,
                                          const string &fileName,
                                          barBackupDataHeader *pHeader,
                                          BOOLEAN check, UINT64 secretValue,
                                          UINT32 sequence )
   {
      INT32 rc = SDB_OK ;

      rc = _read( file, (CHAR*)pHeader, BAR_BACKUPDATA_HEADER_SIZE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to read data header from file[%s], "
                   "rc: %d", fileName.c_str(), rc ) ;

      if ( FALSE == check )
      {
         goto done ;
      }

      if ( 0 != ossStrncmp( pHeader->_eyeCatcher, BAR_BACKUP_DATA_EYECATCHER,
                            BAR_BACKUP_HEADER_EYECATCHER_LEN ) )
      {
         PD_LOG( PDERROR, "Invalid eyecatcher[%s] in file[%s]",
                 pHeader->_eyeCatcher, fileName.c_str() ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }
      else if ( 0 != secretValue && secretValue != pHeader->_secretValue )
      {
         PD_LOG( PDERROR, "Secret value[%llu] is not expect[%llu] in file[%s]",
                 pHeader->_secretValue, secretValue, fileName.c_str() ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }
      else if ( 0 != sequence && sequence != pHeader->_sequence )
      {
         PD_LOG( PDERROR, "Sequence value[%d] is not expect[%d] in file[%s]",
                 pHeader->_sequence, sequence, fileName.c_str() ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBaseLogger::_readDataHeader( UINT32 sequence,
                                          barBackupDataHeader *pHeader,
                                          BOOLEAN check,
                                          UINT64 secretValue )
   {
      INT32 rc = SDB_OK ;
      string fileName = getDataFileName( sequence ) ;
      OSSFILE file ;
      BOOLEAN isOpened = FALSE ;

      rc = ossOpen( fileName.c_str(), OSS_READONLY, OSS_RU|OSS_WU|OSS_RG,
                    file ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to open file[%s], rc: %d",
                   fileName.c_str(), rc ) ;
      isOpened = TRUE ;

      rc = _readDataHeader( file, fileName, pHeader, check, secretValue,
                            sequence ) ;
      if ( rc )
      {
         goto error ;
      }

   done:
      if ( isOpened )
      {
         ossClose( file ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   /*
      _barBkupBaseLogger implement
   */
   _barBkupBaseLogger::_barBkupBaseLogger ()
   {
      _rewrite       = TRUE ;
      _metaFileSeq   = 0 ;
      _curFileSize   = 0 ;
      _isOpened      = FALSE ;
   }

   _barBkupBaseLogger::~_barBkupBaseLogger ()
   {
      _closeCurFile() ;
   }

   INT32 _barBkupBaseLogger::_closeCurFile ()
   {
      if ( _isOpened )
      {
         _isOpened = FALSE ;
         return ossClose( _curFile ) ;
      }
      return SDB_OK ;
   }

   INT32 _barBkupBaseLogger::init( const CHAR *path,
                                   const CHAR *backupName,
                                   INT32 maxDataFileSize,
                                   const CHAR *prefix,
                                   UINT32 opType,
                                   BOOLEAN rewrite,
                                   const CHAR *backupDesp )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;

      rc = _initInner( path, backupName, prefix ) ;
      PD_RC_CHECK( rc, PDERROR, "Init inner failed, rc: %d", rc ) ;

      if ( BAR_BACKUP_OP_TYPE_FULL != opType &&
           BAR_BACKUP_OP_TYPE_INC != opType )
      {
         PD_LOG( PDWARNING, "Invalid backup op type: %d", opType ) ;
         rc = SDB_INVALIDARG ;
      }

      _metaHeader._type                = _getBackupType() ;
      _metaHeader._opType              = opType ;
      _metaHeader._maxDataFileSize     = (UINT64)maxDataFileSize << 20 ;
      ossStrncpy( _metaHeader._groupName, krcb->getGroupName(),
                  BAR_BACKUP_GROUPNAME_LEN - 1 ) ;
      ossStrncpy( _metaHeader._hostName, krcb->getHostName(),
                  BAR_BACKUP_HOSTNAME_LEN - 1 ) ;
      ossStrncpy( _metaHeader._svcName, pmdGetOptionCB()->getServiceAddr(),
                  BAR_BACKUP_SVCNAME_LEN - 1 ) ;
      _metaHeader._nodeID              = pmdGetNodeID().value ;
      if ( backupDesp )
      {
         _metaHeader.setDesp( backupDesp ) ;
      }
      _rewrite                         = rewrite ;

      _metaFileSeq = _ensureMetaFileSeq () ;

      rc = _initCheckAndPrepare() ;
      PD_RC_CHECK( rc, PDWARNING, "Prepare check for backup failed, rc: %d",
                   rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBkupBaseLogger::backup ( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN hasPrepared = FALSE ;

      PD_LOG( PDEVENT, "Begin to backup[%s]...", backupName() ) ;

      rc = _prepareBackup( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Prepare for backup failed, rc: %d", rc ) ;

      hasPrepared = TRUE ;

      rc = _backupConfig() ;
      PD_RC_CHECK( rc, PDERROR, "Failed to backup config, rc: %d", rc ) ;

      rc = _doBackup ( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to do backup, rc: %d", rc ) ;

      rc = _writeMetaFile () ;
      PD_RC_CHECK( rc, PDERROR, "Failed to write meta file, rc: %d", rc ) ;

      rc = _afterBackup ( cb ) ;
      hasPrepared = FALSE ;
      PD_RC_CHECK( rc, PDERROR, "Failed to cleanup after backup, rc: %d", rc ) ;

      PD_LOG( PDEVENT, "Complete backup[%s]", backupName() ) ;

   done:
      return rc ;
   error:
      {
         INT32 tempRC = dropCurBackup() ;
         if ( tempRC )
         {
            PD_LOG( PDWARNING, "Rollback bakcup failed, rc: %d", tempRC ) ;
         }

         if ( hasPrepared )
         {
            tempRC = _afterBackup ( cb ) ;
            hasPrepared = FALSE ;
            if ( tempRC )
            {
               PD_LOG( PDWARNING, "Failed to cleanup after backup, rc: %d",
                       tempRC ) ;
            }
         }
      }
      goto done ;
   }

   INT32 _barBkupBaseLogger::_initCheckAndPrepare ()
   {
      INT32 rc = SDB_OK ;

      rc = ossMkdir( _metaHeader._path, OSS_CREATE|OSS_READWRITE ) ;
      if ( rc && SDB_FE != rc )
      {
         PD_LOG( PDERROR, "Create backup dir[%s] failed, rc: %d",
                 _metaHeader._path, rc ) ;
         goto error ;
      }
      rc = SDB_OK ;

      if ( BAR_BACKUP_OP_TYPE_INC == _metaHeader._opType &&
           0 == _metaFileSeq )
      {
         PD_LOG( PDERROR, "Full backup[%s] not exist", backupName() ) ;
         rc = SDB_BAR_BACKUP_NOTEXIST ;
         goto error ;
      }
      else if ( BAR_BACKUP_OP_TYPE_FULL == _metaHeader._opType &&
                _metaFileSeq != 0  )
      {
         if ( !_rewrite )
         {
            PD_LOG( PDERROR, "Backup[%s] already exist", backupName() ) ;
            rc = SDB_BAR_BACKUP_EXIST ;
         }
         else
         {
            rc = dropAllBackup () ;
            PD_RC_CHECK( rc, PDERROR, "Failed to drop backup[%s], rc: %d",
                         backupName(), rc ) ;
         }
      }

      if ( BAR_BACKUP_OP_TYPE_INC == _metaHeader._opType )
      {
         _metaHeader._secretValue = 0 ;

         rc = _updateFromMetaFile( _metaFileSeq - 1 ) ;
         PD_RC_CHECK( rc, PDERROR, "Update from meta file[%d] failed, rc: %d",
                      _metaFileSeq - 1, rc ) ;

         PD_LOG( PDDEBUG, "Backup info: lastFileSequence: %lld, "
                 "lastExtentID: %lld, beginLSN: %llu, secretValue: %llu",
                 _metaHeader._lastDataSequence, _metaHeader._lastExtentID,
                 _metaHeader._beginLSNOffset, _metaHeader._secretValue ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBkupBaseLogger::_updateFromMetaFile( UINT32 incID )
   {
      INT32 rc = SDB_OK ;
      barBackupHeader *pHeader = NULL ;

      pHeader = SDB_OSS_NEW barBackupHeader ;
      if ( !pHeader )
      {
         PD_LOG( PDERROR, "Failed to alloc memory for backup header" ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = _readMetaHeader( incID, pHeader, TRUE, _metaHeader._secretValue ) ;
      if ( rc )
      {
         goto error ;
      }

      _metaHeader._lastDataSequence = pHeader->_lastDataSequence ;
      _metaHeader._beginLSNOffset   = pHeader->_endLSNOffset ;
      _metaHeader._lastExtentID     = pHeader->_lastExtentID ;
      if ( 0 == _metaHeader._secretValue )
      {
         _metaHeader._secretValue      = pHeader->_secretValue ;
      }

   done:
      if ( pHeader )
      {
         SDB_OSS_DEL pHeader ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBkupBaseLogger::_writeData( const CHAR * buf, INT64 len,
                                         BOOLEAN isExtHeader )
   {
      INT32 rc = SDB_OK ;

      if ( isExtHeader )
      {
         barBackupExtentHeader *pHeader = (barBackupExtentHeader*)buf ;
         UINT32 dataLen = ( 0 == pHeader->_thinCopy ) ?
                          pHeader->_dataSize : 0 ;
         if ( _curFileSize + len + dataLen > _metaHeader._maxDataFileSize )
         {
            _closeCurFile() ;
         }
      }

      if ( !_isOpened )
      {
         rc = _openDataFile() ;
         PD_RC_CHECK( rc, PDERROR, "Failed to open data file, rc: %d", rc ) ;
      }

      rc = _flush( _curFile, buf, len ) ;
      if ( rc )
      {
         goto error ;
      }
      _curFileSize += len ;
      _metaHeader._dataSize += len ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBkupBaseLogger::_openDataFile ()
   {
      if ( _isOpened )
      {
         return SDB_OK ;
      }

      ++_metaHeader._lastDataSequence ;

      barBackupDataHeader *pHeader = NULL ;
      string fileName = getDataFileName( _metaHeader._lastDataSequence ) ;
      INT32 rc = ossOpen( fileName.c_str(), OSS_REPLACE | OSS_READWRITE,
                          OSS_RU | OSS_WU | OSS_RG, _curFile ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to open file[%s], rc: %d", fileName.c_str(),
                 rc ) ;
         goto error ;
      }
      _isOpened = TRUE ;

      pHeader = SDB_OSS_NEW barBackupDataHeader ;
      if ( !pHeader )
      {
         PD_LOG( PDERROR, "Failed to alloc memory for backup data header" ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      pHeader->_secretValue   = _metaHeader._secretValue ;
      pHeader->_sequence      = _metaHeader._lastDataSequence ;

      rc = _flush( _curFile, (const CHAR *)pHeader,
                   BAR_BACKUPDATA_HEADER_SIZE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to write header to file[%s], rc: %d",
                   fileName.c_str(), rc ) ;

      _curFileSize = BAR_BACKUPDATA_HEADER_SIZE ;
      _metaHeader._dataSize += BAR_BACKUPDATA_HEADER_SIZE ;
      _metaHeader._dataFileNum++ ;

   done:
      if ( pHeader )
      {
         SDB_OSS_DEL pHeader ;
      }
      return rc ;
   error:
      goto done ;
   }

   barBackupExtentHeader* _barBkupBaseLogger::_nextDataExtent( UINT32 dataType )
   {
      if ( !_pDataExtent )
      {
         return NULL ;
      }
      _pDataExtent->init() ;
      _pDataExtent->_dataType = dataType ;
      _pDataExtent->_extentID = ++_metaHeader._lastExtentID ;

      return _pDataExtent ;
   }

   INT32 _barBkupBaseLogger::_backupConfig ()
   {
      INT32 rc = SDB_OK ;
      BSONObj cfgData ;
      barBackupExtentHeader *pHeader = _nextDataExtent( BAR_DATA_TYPE_CONFIG ) ;
      CHAR tmpBuff[4] = {0} ;
      UINT32 tmpSize = 0 ;

      rc = _pOptCB->toBSON( cfgData ) ;
      PD_RC_CHECK( rc, PDERROR, "Config to bson failed, rc: %d", rc ) ;

      tmpSize = ossAlign4( (UINT32)cfgData.objsize() ) - cfgData.objsize() ;
      pHeader->_dataSize = ossAlign4( (UINT32)cfgData.objsize () ) ;

      rc = _writeData( (const CHAR*)pHeader, BAR_BACKUP_EXTENT_HEADER_SIZE,
                       TRUE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to write config extent header, rc: %d",
                   rc ) ;
      rc = _writeData( cfgData.objdata(), cfgData.objsize(), FALSE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to write config, rc: %d", rc ) ;

      if ( 0 != tmpSize )
      {
         rc = _writeData( (const CHAR*)tmpBuff, tmpSize, FALSE ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to write config align data, rc: %d",
                      rc ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBkupBaseLogger::_writeMetaFile ()
   {
      INT32 rc = SDB_OK ;
      string fileName ;

      rc = _onWriteMetaFile() ;
      PD_RC_CHECK( rc, PDERROR, "On write meta file failed, rc: %d", rc ) ;

      _closeCurFile () ;

      fileName = getIncFileName( _metaFileSeq ) ;
      rc = ossOpen( fileName.c_str(), OSS_REPLACE | OSS_READWRITE,
                    OSS_RU | OSS_WU | OSS_RG, _curFile ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to open file[%s], rc: %d",
                   fileName.c_str(), rc ) ;

      _isOpened = TRUE ;
      _metaHeader.makeEndTime() ;

      rc = _flush( _curFile, (const CHAR *)&_metaHeader,
                   BAR_BACKUP_HEADER_SIZE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to write backup header to meta "
                   "file[%s], rc: %d", fileName.c_str(), rc ) ;

      _closeCurFile () ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBkupBaseLogger::dropCurBackup ()
   {
      INT32 rc = SDB_OK ;
      string fileName ;

      _closeCurFile() ;

      while ( _metaHeader._lastDataSequence > 0 &&
              _metaHeader._dataFileNum > 0 )
      {
         fileName = getDataFileName( _metaHeader._lastDataSequence ) ;
         if ( SDB_OK == ossAccess( fileName.c_str() ) )
         {
            rc = ossDelete( fileName.c_str() ) ;
            PD_RC_CHECK( rc, PDWARNING, "Failed to delete file[%s], rc: %d",
                         fileName.c_str(), rc ) ;
         }

         --_metaHeader._dataFileNum ;
         --_metaHeader._lastDataSequence ;
      }

      fileName = getIncFileName( _metaFileSeq ) ;
      if ( SDB_OK == ossAccess( fileName.c_str() ) )
      {
         rc = ossDelete( fileName.c_str() ) ;
         PD_RC_CHECK( rc, PDWARNING, "Failed to delete file[%s], rc: %d",
                      fileName.c_str(), rc ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBkupBaseLogger::dropAllBackup ()
   {
      INT32 rc = SDB_OK ;
      barBackupMgr bkMgr ;

      rc = bkMgr.init( path(), backupName() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init backup mgr, rc: %d", rc ) ;

      rc = bkMgr.drop() ;
      PD_RC_CHECK( rc, PDERROR, "Failed to drop backup[%s], rc: %d",
                   backupName(), rc ) ;

      _metaFileSeq = 0 ;

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _barBKOfflineLogger implement
   */
   _barBKOfflineLogger::_barBKOfflineLogger ()
   {
      _curDataType = BAR_DATA_TYPE_RAW_DATA ;
      _curOffset   = 0 ;
      _curSequence = 0 ;
      _replStatus  = -1 ;
      _pExtentBuff = NULL ;
   }

   _barBKOfflineLogger::~_barBKOfflineLogger ()
   {
      if ( _pExtentBuff )
      {
         SDB_OSS_FREE( _pExtentBuff ) ;
         _pExtentBuff = NULL ;
      }
   }

   UINT32 _barBKOfflineLogger::_getBackupType () const
   {
      return BAR_BACKUP_TYPE_OFFLINE ;
   }

   INT32 _barBKOfflineLogger::_prepareBackup ( _pmdEDUCB *cb )
   {
      pmdKRCB *krcb = pmdGetKRCB() ;
      INT32 rc = SDB_OK ;
      BOOLEAN hasReg = FALSE ;
      DPS_LSN lsn ;

      if ( SDB_ROLE_STANDALONE != krcb->getDBRole() )
      {
         _replStatus = _pClsCB->getReplCB()->getStatus() ;
         _pClsCB->getReplCB()->setStatus( CLS_BS_BACKUPOFFLINE ) ;

         while ( TRUE )
         {
            if ( cb->isInterrupted() )
            {
               rc = SDB_APP_INTERRUPT ;
               goto error ;
            }
            if ( SDB_OK == _pClsCB->getReplCB()->getBucket()->waitEmpty(
                           OSS_ONE_SEC ) )
            {
               break ;
            }
         }
      }

      rc = _pDMSCB->registerBackup( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to register backup, rc: %d", rc ) ;
      hasReg = TRUE ;

      lsn = _pDPSCB->getStartLsn( FALSE ) ;
      if ( BAR_BACKUP_OP_TYPE_INC == _metaHeader._opType &&
           lsn.compareOffset( _metaHeader._beginLSNOffset ) > 0 )
      {
         PD_LOG( PDERROR, "LSN[%lld] is out of dps log, begin lsn[%u,%lld]",
                 _metaHeader._beginLSNOffset, lsn.version, lsn.offset ) ;
         rc = SDB_DPS_LSN_OUTOFRANGE ;
         goto error ;
      }
      else if ( BAR_BACKUP_OP_TYPE_FULL == _metaHeader._opType )
      {
         _metaHeader._beginLSNOffset = lsn.offset ;
      }

      _metaHeader._endLSNOffset   = _pDPSCB->expectLsn().offset ;
      _metaHeader._transLSNOffset = _pTransCB->getOldestBeginLsn() ;

   done:
      return rc ;
   error:
      if ( hasReg )
      {
         _pDMSCB->backupDown( cb ) ;
      }
      if ( -1 != _replStatus )
      {
         _pClsCB->getReplCB()->setStatus( (CLS_BS_STATUS)_replStatus ) ;
      }
      goto done ;
   }

   INT32 _barBKOfflineLogger::_onWriteMetaFile ()
   {
      return SDB_OK ;
   }

   INT32 _barBKOfflineLogger::_afterBackup ( _pmdEDUCB *cb )
   {
      _pDMSCB->backupDown( cb ) ;
      if ( -1 != _replStatus )
      {
         _pClsCB->getReplCB()->setStatus( (CLS_BS_STATUS)_replStatus ) ;
      }
      return SDB_OK ;
   }

   INT32 _barBKOfflineLogger::_doBackup ( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;

      if ( BAR_BACKUP_OP_TYPE_FULL == _metaHeader._opType )
      {
         dmsStorageUnit *su = NULL ;
         _SDB_DMSCB::CSCB_ITERATOR itr = _pDMSCB->begin() ;
         while ( itr != _pDMSCB->end() )
         {
            if ( NULL == *itr )
            {
               ++itr ;
               continue ;
            }

            su = (*itr)->_su ;

            if ( su->data()->isTempSU () )
            {
               ++itr ;
               continue ;
            }

            PD_LOG( PDEVENT, "Begin to backup storage: %s", su->CSName() ) ;

            _curSequence = su->CSSequence() ;

            _curDataType = BAR_DATA_TYPE_RAW_DATA ;
            _curOffset = 0 ;
            rc = _backupSU( su->data(), cb ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to backup storage unit[%s] data "
                         "su, rc: %d", su->CSName(), rc ) ;

            _curDataType = BAR_DATA_TYPE_RAW_IDX ;
            _curOffset = 0 ;
            rc = _backupSU ( su->index(), cb ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to backup storage unit[%s] "
                         "index su, rc: %d", su->CSName(), rc ) ;

            if ( su->lob()->isOpened() )
            {
               _curDataType = BAR_DATA_TYPE_RAW_LOBM ;
               _curOffset = 0 ;
               rc = _backupSU( su->lob(), cb ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to backup storage unit[%s] "
                            "lob meta su, rc: %d", su->CSName(), rc ) ;

               _curDataType = BAR_DATA_TYPE_RAW_LOBD ;
               _curOffset = 0 ;
               rc = _backupLobData( su->lob(), cb ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to backup storage unit[%s] "
                            "lob data su, rc: %d", su->CSName(), rc ) ;
            }

            PD_LOG( PDEVENT, "Complete backup storage: %s", su->CSName() ) ;

            _metaHeader._csNum++ ;
            ++itr ;
         }
      }

      PD_LOG( PDEVENT, "Begin to backup repl-log: %lld",
              _metaHeader._beginLSNOffset ) ;
      rc = _backupLog( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to backup log, rc: %d", rc ) ;

      PD_LOG( PDEVENT, "Complete backup repl-log: %lld",
              _metaHeader._endLSNOffset ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   BSONObj _barBKOfflineLogger::_makeExtentMeta( _dmsStorageBase *pSU )
   {
      BSONObjBuilder builder ;
      builder.append( BAR_SU_NAME, pSU->getSuName() ) ;
      builder.append( BAR_SU_FILE_NAME, pSU->getSuFileName() ) ;
      builder.append( BAR_SU_FILE_OFFSET, (long long)_curOffset ) ;
      builder.append( BAR_SU_SEQUENCE, (INT32)_curSequence ) ;
      if ( BAR_DATA_TYPE_RAW_DATA == _curDataType )
      {
         builder.append( BAR_SU_FILE_TYPE, BAR_SU_FILE_TYPE_DATA ) ;
      }
      else if ( BAR_DATA_TYPE_RAW_IDX == _curDataType )
      {
         builder.append( BAR_SU_FILE_TYPE, BAR_SU_FILE_TYPE_INDEX ) ;
      }
      else if ( BAR_DATA_TYPE_RAW_LOBM == _curDataType )
      {
         builder.append( BAR_SU_FILE_TYPE, BAR_SU_FILE_TYPE_LOBM ) ;
      }
      else
      {
         builder.append( BAR_SU_FILE_TYPE, BAR_SU_FILE_TYPE_LOBD ) ;
      }

      return builder.obj() ;
   }

   BSONObj _barBKOfflineLogger::_makeExtentMeta( _dmsStorageLob *pLobSU )
   {
      BSONObjBuilder builder ;
      builder.append( BAR_SU_NAME, pLobSU->getSuName() ) ;
      builder.append( BAR_SU_FILE_NAME,
                      pLobSU->getLobData()->getFileName().c_str() ) ;
      builder.append( BAR_SU_FILE_OFFSET, (long long)_curOffset ) ;
      builder.append( BAR_SU_SEQUENCE, (INT32)_curSequence ) ;
      if ( BAR_DATA_TYPE_RAW_DATA == _curDataType )
      {
         builder.append( BAR_SU_FILE_TYPE, BAR_SU_FILE_TYPE_DATA ) ;
      }
      else if ( BAR_DATA_TYPE_RAW_IDX == _curDataType )
      {
         builder.append( BAR_SU_FILE_TYPE, BAR_SU_FILE_TYPE_INDEX ) ;
      }
      else if ( BAR_DATA_TYPE_RAW_LOBM == _curDataType )
      {
         builder.append( BAR_SU_FILE_TYPE, BAR_SU_FILE_TYPE_LOBM ) ;
      }
      else
      {
         builder.append( BAR_SU_FILE_TYPE, BAR_SU_FILE_TYPE_LOBD ) ;
      }

      return builder.obj() ;
   }

   INT32 _barBKOfflineLogger::_nextThinCopyInfo( dmsStorageBase *pSU,
                                                 UINT32 startExtID,
                                                 UINT32 maxExtID,
                                                 UINT32 maxNum,
                                                 UINT32 &num,
                                                 BOOLEAN &used )
   {
      INT32 rc = SDB_OK ;
      const dmsSpaceManagementExtent *pSME = pSU->getSME() ;
      num   = 0 ;
      used  = FALSE ;
      CHAR  flag = DMS_SME_ALLOCATED ;

      if ( startExtID >= pSU->pageNum() )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "start extent id[%u] is more than total data "
                 "pages[%u]", startExtID, pSU->pageNum() ) ;
         goto error ;
      }

      if ( startExtID >= maxExtID || num >= maxNum )
      {
         goto done ;
      }

      if ( DMS_SME_ALLOCATED == pSME->getBitMask( startExtID ) )
      {
         used = TRUE ;
      }
      else
      {
         used = FALSE ;
      }
      ++num ;

      while ( num < maxNum && startExtID + num < maxExtID )
      {
         flag = pSME->getBitMask( startExtID + num ) ;

         if ( ( used && DMS_SME_ALLOCATED != flag ) ||
              ( !used && DMS_SME_ALLOCATED == flag ) )
         {
            break ;
         }
         ++num ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBKOfflineLogger::_backupSU( _dmsStorageBase *pSU,
                                         _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj metaObj ;
      barBackupExtentHeader *pHeader = NULL ;
      ossValuePtr ptr   = 0 ;
      UINT32 length     = 0 ;
      BOOLEAN thinCopy  = FALSE ;

      UINT32 segmentID = 0 ;
      UINT32 curExtentID = 0 ;
      UINT32 maxExtNum = BAR_MAX_EXTENT_DATA_SIZE >> pSU->pageSizeSquareRoot() ;

      pSU->syncMemToMmap() ;

      if ( pSU->dataSize() > ((UINT64)BAR_THINCOPY_THRESHOLD_SIZE << 20 ) )
      {
         FLOAT64 ratio = (FLOAT64)pSU->getSMEMgr()->totalFree() /
                         (FLOAT64)pSU->pageNum() ;
         if ( ratio >= BAR_THINCOPY_THRESHOLD_RATIO )
         {
            thinCopy = TRUE ;
         }
      }

      _ossMmapFile::CONST_ITR itr = pSU->begin() ;
      while ( itr != pSU->end() )
      {
         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }

         ptr = (*itr)._ptr ;
         length = (*itr)._length ;

         if ( segmentID < pSU->dataStartSegID() || !thinCopy )
         {
            while ( length > 0 )
            {
               pHeader = _nextDataExtent( _curDataType ) ;
               pHeader->_dataSize = length < BAR_MAX_EXTENT_DATA_SIZE ?
                                    length : BAR_MAX_EXTENT_DATA_SIZE ;
               metaObj = _makeExtentMeta( pSU ) ;
               pHeader->setMetaData( metaObj.objdata(), metaObj.objsize() ) ;

               rc = _writeData( (const CHAR *)pHeader,
                                BAR_BACKUP_EXTENT_HEADER_SIZE, TRUE ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to write extent header, "
                            "rc: %d", rc ) ;
               rc = _writeData( (const CHAR*)ptr, pHeader->_dataSize, FALSE ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to write data, rc: %d", rc ) ;

               length -= pHeader->_dataSize ;
               ptr += pHeader->_dataSize ;
               _curOffset += pHeader->_dataSize ;
            }
         }
         else
         {
            UINT32 num = 0 ;
            BOOLEAN used = FALSE ;
            UINT32 maxExtID = curExtentID + pSU->segmentPages() ;
            while ( curExtentID < maxExtID )
            {
               rc = _nextThinCopyInfo( pSU, curExtentID, maxExtID, maxExtNum,
                                       num, used ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to get next thin copy info, "
                            "rc: %d", rc ) ;

               pHeader = _nextDataExtent( _curDataType ) ;
               pHeader->_dataSize = (UINT64)num << pSU->pageSizeSquareRoot() ;
               pHeader->_thinCopy = used ? 0 : 1 ;
               metaObj = _makeExtentMeta( pSU ) ;
               pHeader->setMetaData( metaObj.objdata(), metaObj.objsize() ) ;

               rc = _writeData( (const CHAR *)pHeader,
                                BAR_BACKUP_EXTENT_HEADER_SIZE, TRUE ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to write extent header, "
                            "rc: %d", rc ) ;
               if ( used )
               {
                  rc = _writeData( (const CHAR*)ptr, pHeader->_dataSize,
                                   FALSE ) ;
                  PD_RC_CHECK( rc, PDERROR, "Failed to write data, rc: %d",
                               rc ) ;
               }

               ptr += pHeader->_dataSize ;
               _curOffset += pHeader->_dataSize ;
               curExtentID += num ;
            }
         }

         ++segmentID ;
         ++itr ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBKOfflineLogger::_backupLobData( _dmsStorageLob * pLobSU,
                                              _pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj metaObj ;
      barBackupExtentHeader *pHeader = NULL ;
      BOOLEAN thinCopy  = FALSE ;
      dmsStorageLobData *pLobData = pLobSU->getLobData() ;
      UINT64 metaLen    = pLobData->getFileSz() - pLobData->getDataSz() ;
      UINT32 readLen    = 0 ;

      UINT32 curExtentID = 0 ;
      UINT32 maxExtNum = BAR_MAX_EXTENT_DATA_SIZE >>
                         pLobData->pageSizeSquareRoot() ;

      if ( !_pExtentBuff )
      {
         _pExtentBuff = ( CHAR* )SDB_OSS_MALLOC( BAR_MAX_EXTENT_DATA_SIZE ) ;
         if ( !_pExtentBuff )
         {
            PD_LOG( PDERROR, "Alloc extent buff failed" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }

      if ( (UINT64)pLobData->getDataSz() >
           ((UINT64)BAR_THINCOPY_THRESHOLD_SIZE << 20 ) )
      {
         FLOAT64 ratio = (FLOAT64)pLobSU->getSMEMgr()->totalFree() /
                         (FLOAT64)pLobSU->pageNum() ;
         if ( ratio >= BAR_THINCOPY_THRESHOLD_RATIO )
         {
            thinCopy = TRUE ;
         }
      }

      while ( _curOffset < (UINT64)pLobData->getFileSz() )
      {
         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }

         if ( _curOffset < metaLen || !thinCopy )
         {
            while ( _curOffset < metaLen )
            {
               pHeader = _nextDataExtent( _curDataType ) ;
               pHeader->_dataSize = metaLen - _curOffset <
                                    BAR_MAX_EXTENT_DATA_SIZE ?
                                    metaLen - _curOffset :
                                    BAR_MAX_EXTENT_DATA_SIZE ;
               metaObj = _makeExtentMeta( pLobSU ) ;
               pHeader->setMetaData( metaObj.objdata(), metaObj.objsize() ) ;

               rc = pLobData->readRaw( _curOffset, pHeader->_dataSize,
                                       _pExtentBuff, readLen ) ;
               if ( rc )
               {
                  PD_LOG( PDERROR, "Read lob file[%s, offset: %lld, len: %lld] "
                          "failed, rc: %d", pLobData->getFileName().c_str(),
                          _curOffset, pHeader->_dataSize, rc ) ;
                  goto error ;
               }
               else if ( readLen != pHeader->_dataSize )
               {
                  rc = SDB_SYS ;
                  PD_LOG( PDERROR, "Read lob file[%s, offset: %lld, len: %lld] "
                          "failed[readLen: %d], rc: %d",
                          pLobData->getFileName().c_str(), _curOffset,
                          pHeader->_dataSize, readLen, rc ) ;
                  goto error ;
               }

               rc = _writeData( (const CHAR *)pHeader,
                                BAR_BACKUP_EXTENT_HEADER_SIZE, TRUE ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to write extent header, "
                            "rc: %d", rc ) ;
               rc = _writeData( _pExtentBuff, pHeader->_dataSize, FALSE ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to write data, rc: %d", rc ) ;

               _curOffset += pHeader->_dataSize ;
            }
         }
         else
         {
            UINT32 num = 0 ;
            BOOLEAN used = FALSE ;
            UINT32 maxExtID = curExtentID + pLobSU->segmentPages() ;
            while ( curExtentID < maxExtID )
            {
               rc = _nextThinCopyInfo( pLobSU, curExtentID, maxExtID,
                                       maxExtNum, num, used ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to get next thin copy info, "
                            "rc: %d", rc ) ;

               pHeader = _nextDataExtent( _curDataType ) ;
               pHeader->_dataSize = (UINT64)num << pLobData->pageSizeSquareRoot() ;
               pHeader->_thinCopy = used ? 0 : 1 ;
               metaObj = _makeExtentMeta( pLobSU ) ;
               pHeader->setMetaData( metaObj.objdata(), metaObj.objsize() ) ;

               rc = pLobData->readRaw( _curOffset, pHeader->_dataSize,
                                       _pExtentBuff, readLen ) ;
               if ( rc )
               {
                  PD_LOG( PDERROR, "Read lob file[%s, offset: %lld, len: %lld] "
                          "failed, rc: %d", pLobData->getFileName().c_str(),
                          _curOffset, pHeader->_dataSize, rc ) ;
                  goto error ;
               }
               else if ( readLen != pHeader->_dataSize )
               {
                  rc = SDB_SYS ;
                  PD_LOG( PDERROR, "Read lob file[%s, offset: %lld, len: %lld] "
                          "failed[readLen: %d], rc: %d",
                          pLobData->getFileName().c_str(), _curOffset,
                          pHeader->_dataSize, readLen, rc ) ;
                  goto error ;
               }

               rc = _writeData( (const CHAR *)pHeader,
                                BAR_BACKUP_EXTENT_HEADER_SIZE, TRUE ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to write extent header, "
                            "rc: %d", rc ) ;
               if ( used )
               {
                  rc = _writeData( _pExtentBuff, pHeader->_dataSize,
                                   FALSE ) ;
                  PD_RC_CHECK( rc, PDERROR, "Failed to write data, rc: %d",
                               rc ) ;
               }

               _curOffset += pHeader->_dataSize ;
               curExtentID += num ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBKOfflineLogger::_backupLog( pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      DPS_LSN lsn ;
      barBackupExtentHeader *pHeader = NULL ;
      dpsMessageBlock mb( BAR_MAX_EXTENT_DATA_SIZE ) ;

      if ( DPS_INVALID_LSN_OFFSET == _metaHeader._beginLSNOffset )
      {
         goto done ;
      }

      lsn.offset = _metaHeader._beginLSNOffset ;
      while ( lsn.compareOffset( _metaHeader._endLSNOffset ) < 0 )
      {
         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }

         mb.clear() ;

         while ( lsn.compareOffset( _metaHeader._endLSNOffset ) < 0 &&
                 mb.length() < BAR_MAX_EXTENT_DATA_SIZE )
         {
            rc = _pDPSCB->search( lsn, &mb ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to search lsn[%u,%lld], rc: %d",
                         lsn.version, lsn.offset, rc ) ;
            dpsLogRecordHeader *header = (dpsLogRecordHeader*)mb.readPtr() ;
            mb.readPtr( mb.length() ) ;
            lsn.offset += header->_length ;
            lsn.version = header->_version ;
         }

         pHeader = _nextDataExtent( BAR_DATA_TYPE_REPL_LOG ) ;
         pHeader->_dataSize = mb.length() ;

         rc = _writeData( (const CHAR*)pHeader, BAR_BACKUP_EXTENT_HEADER_SIZE,
                          TRUE );
         PD_RC_CHECK( rc, PDERROR, "Failed to write extent header, rc: %d",
                      rc ) ;
         rc = _writeData( mb.startPtr(), mb.length(), FALSE ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to write repl-log, rc: %d", rc ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _barRSBaseLogger implement
   */
   _barRSBaseLogger::_barRSBaseLogger ()
   {
      _metaFileSeq         = 0 ;
      _secretValue         = 0 ;
      _expectExtID         = 1 ;
      _curDataFileSeq      = 0 ;

      _isOpened            = FALSE ;
      _curOffset           = 0 ;
      _pBuff               = NULL ;
      _buffSize            = 0 ;
   }

   _barRSBaseLogger::~_barRSBaseLogger ()
   {
      _closeCurFile() ;
      if ( _pBuff )
      {
         SDB_OSS_FREE( _pBuff ) ;
         _pBuff = NULL ;
         _buffSize = 0 ;
      }
   }

   void _barRSBaseLogger::_reset ()
   {
      _closeCurFile() ;
      _expectExtID = 1 ;
      _curDataFileSeq = 0 ;
      _curOffset = 0 ;
   }

   INT32 _barRSBaseLogger::_allocBuff( UINT64 buffSize )
   {
      if ( buffSize <= _buffSize )
      {
         return SDB_OK ;
      }
      if ( _pBuff )
      {
         SDB_OSS_FREE( _pBuff ) ;
         _pBuff = NULL ;
         _buffSize = 0 ;
      }
      _pBuff = ( CHAR* )SDB_OSS_MALLOC( buffSize ) ;
      if ( !_pBuff )
      {
         PD_LOG( PDERROR, "Failed to alloc memory, size: %llu", buffSize ) ;
         return SDB_OOM ;
      }
      _buffSize = buffSize ;
      return SDB_OK ;
   }

   INT32 _barRSBaseLogger::_closeCurFile ()
   {
      if ( _isOpened )
      {
         _isOpened = FALSE ;
         return ossClose( _curFile ) ;
      }
      return SDB_OK ;
   }

   INT32 _barRSBaseLogger::_openDataFile ()
   {
      if ( _isOpened )
      {
         return SDB_OK ;
      }
      ++_curDataFileSeq ;
      barBackupDataHeader *pHeader = NULL ;
      string fileName = getDataFileName( _curDataFileSeq ) ;

      if ( 0 != _expectExtID )
      {
         std::cout << "Begin to restore data file: " << fileName.c_str()
                   << " ..." << std::endl ;
         PD_LOG( PDEVENT, "Begin to restore data file[%s]", fileName.c_str() ) ;
      }

      INT32 rc = ossOpen( fileName.c_str(), OSS_READONLY,
                          OSS_RU | OSS_WU | OSS_RG, _curFile ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to open file[%s], rc: %d", fileName.c_str(),
                 rc ) ;
         goto error ;
      }
      _isOpened = TRUE ;

      pHeader = SDB_OSS_NEW barBackupDataHeader ;
      if ( !pHeader )
      {
         PD_LOG( PDERROR, "Failed to alloc memory for backup data header" ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = _readDataHeader( _curFile, fileName, pHeader, TRUE, _secretValue,
                            _curDataFileSeq ) ;
      if ( rc )
      {
         goto error ;
      }
      _curOffset = BAR_BACKUP_HEADER_EYECATCHER_LEN ;

   done:
      if ( pHeader )
      {
         SDB_OSS_DEL pHeader ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSBaseLogger::init( const CHAR *path, const CHAR *backupName,
                                 const CHAR *prefix, INT32 incID )
   {
      INT32 rc = SDB_OK ;

      rc = _initInner( path, backupName, prefix ) ;
      PD_RC_CHECK( rc, PDWARNING, "Init inner failed, rc: %d", rc ) ;

      if ( !backupName || 0 == ossStrlen( backupName ) )
      {
         PD_LOG( PDWARNING, "Backup name can't be empty" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _metaFileSeq = _ensureMetaFileSeq() ;

      rc = _initCheckAndPrepare ( incID ) ;
      PD_RC_CHECK( rc, PDWARNING, "Prepare check for resotre failed, rc: %d",
                   rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSBaseLogger::_initCheckAndPrepare( INT32 incID )
   {
      INT32 rc = SDB_OK ;

      if ( 0 == _metaFileSeq )
      {
         rc = SDB_BAR_BACKUP_NOTEXIST ;
         PD_LOG( PDWARNING, "Full backup[%s] not exist", backupName() ) ;
         goto error ;
      }
      --_metaFileSeq ;

      if ( incID >= 0 )
      {
         if ( (UINT32)incID > _metaFileSeq )
         {
            rc = SDB_BAR_BACKUP_NOTEXIST ;
            PD_LOG( PDWARNING, "Increase backup[%s,%d] not exist",
                    backupName(), incID ) ;
            goto error ;
         }
         _metaFileSeq = (UINT32)incID ;
      }

      rc = _updateFromMetafile( _metaFileSeq ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to read meta file[%d], rc: %d",
                   _metaFileSeq, rc ) ;

      rc = _loadConf () ;
      PD_RC_CHECK( rc, PDERROR, "Failed to load config, rc: %d", rc ) ;

      _reset () ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSBaseLogger::_updateFromMetafile( UINT32 incID )
   {
      INT32 rc = SDB_OK ;

      rc = _readMetaHeader( incID, &_metaHeader, TRUE, _secretValue ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( 0 == _secretValue )
      {
         _secretValue = _metaHeader._secretValue ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSBaseLogger::_loadConf ()
   {
      INT32 rc = SDB_OK ;
      CHAR *pBuff = NULL ;
      barBackupExtentHeader *pExtHeader = NULL ;

      _curDataFileSeq = _metaHeader._lastDataSequence -
                        _metaHeader._dataFileNum ;
      _expectExtID = 0 ;

      rc = _readData( &pExtHeader, &pBuff ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to read config, rc: %d" ) ;

      if ( !pExtHeader || !pBuff )
      {
         PD_LOG( PDERROR, "Invalid backup data file and meta file" ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }
      else if ( BAR_DATA_TYPE_CONFIG != pExtHeader->_dataType )
      {
         PD_LOG( PDERROR, "Extent data type[%d] error",
                 pExtHeader->_dataType ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }

      try
      {
         _confObj = BSONObj( pBuff ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception when read config: %s", e.what() ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSBaseLogger::restore( pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN prepared = FALSE ;

      PD_LOG( PDEVENT, "Begin to restore[%s]...", backupName() ) ;

      rc = _prepareRestore( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to prepare for restore, rc: %d", rc ) ;

      prepared = TRUE ;

      rc = _restoreConfig () ;
      PD_RC_CHECK( rc, PDERROR, "Failed to restore config, rc: %d", rc ) ;

      rc = _doRestore( cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to do restore, rc: %d", rc ) ;

      rc = _afterRestore( cb ) ;
      prepared = FALSE ;
      PD_RC_CHECK( rc, PDERROR, "Failed to clean up after restore", rc ) ;

      PD_LOG( PDEVENT, "Complete restore[%s]", backupName() ) ;

   done:
      PMD_SHUTDOWN_DB( rc )
      return rc ;
   error:
      {
         INT32 tempRC = cleanDMSData () ;
         if ( tempRC )
         {
            PD_LOG( PDWARNING, "Rollback restore failed, rc: %d", tempRC ) ;
         }

         if ( prepared )
         {
            tempRC = _afterRestore( cb ) ;
            if ( tempRC )
            {
               PD_LOG( PDWARNING, "Failed to clean up after restore", tempRC ) ;
            }
         }
      }
      goto done ;
   }

   INT32 _barRSBaseLogger::cleanDMSData ()
   {
      INT32 rc = SDB_OK ;
      std::set<_monCollectionSpace> csList ;

      PD_LOG ( PDEVENT, "Clear dms data for restore" ) ;

      _pDMSCB->dumpInfo( csList, TRUE ) ;
      std::set<_monCollectionSpace>::const_iterator it = csList.begin() ;
      while ( it != csList.end() )
      {
         const _monCollectionSpace &cs = *it ;
         rc = rtnDropCollectionSpaceCommand ( cs._name, NULL, _pDMSCB, NULL,
                                              TRUE ) ;
         if ( SDB_OK != rc && SDB_DMS_CS_NOTEXIST != rc )
         {
            PD_LOG ( PDERROR, "Clear collectionspace[%s] failed[rc:%d]",
                     cs._name, rc ) ;
            break ;
         }
         PD_LOG ( PDDEBUG, "Clear collectionspace[%s] succeed", cs._name ) ;
         ++it ;
      }
      return rc ;
   }

   INT32 _barRSBaseLogger::_restoreConfig ()
   {
      return _pOptCB->reflush2File() ;
   }

   INT32 _barRSBaseLogger::_readData( barBackupExtentHeader **pExtHeader,
                                      CHAR **pBuff )
   {
      INT32 rc = SDB_OK ;
      *pExtHeader = NULL ;
      *pBuff = NULL ;

      if ( _expectExtID > _metaHeader._lastExtentID )
      {
         if ( _curDataFileSeq != _metaHeader._lastDataSequence )
         {
            PD_LOG( PDERROR, "Invalid backup file, expect extent id: %llu, "
                    "meta header last extent id: %llu, cur file seq: %d, meta "
                    "header last data seq: %d", _expectExtID,
                    _metaHeader._lastExtentID, _curDataFileSeq,
                    _metaHeader._lastDataSequence ) ;
            rc = SDB_BAR_DAMAGED_BK_FILE ;
            goto error ;
         }
         goto done ;
      }

   retry:
      if ( !_isOpened )
      {
         rc = _openDataFile() ;
         PD_RC_CHECK( rc, PDERROR, "Failed to open data file, rc: %d", rc ) ;
      }

      rc = _read( _curFile, (CHAR*)_pDataExtent,
                  BAR_BACKUP_EXTENT_HEADER_SIZE ) ;
      if ( SDB_EOF == rc )
      {
         if ( _curDataFileSeq < _metaHeader._lastDataSequence )
         {
            _closeCurFile() ;
            goto retry ;
         }
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to read data extent header, offset: "
                   "%llu, expect extent id: %llu, data seq: %d, last extent "
                   "id: %llu, rc: %d", _curOffset, _expectExtID,
                   _curDataFileSeq, _metaHeader._lastExtentID, rc ) ;

      _curOffset += BAR_BACKUP_EXTENT_HEADER_SIZE ;

      if ( 0 != ossStrncmp( _pDataExtent->_eyeCatcher, BAR_EXTENT_EYECATCH,
                           BAR_BACKUP_HEADER_EYECATCHER_LEN ) )
      {
         PD_LOG( PDERROR, "Extent eyecatcher[%s] is invalid, offset: %llu, "
                 "data seq: %d", _pDataExtent->_eyeCatcher, _curOffset,
                 _curDataFileSeq ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }
      if ( 0 != _expectExtID )
      {
         if ( _pDataExtent->_extentID != _expectExtID )
         {
            PD_LOG( PDERROR, "Extent id[%llu] is not expect[%llu], offset: "
                    "%llu, data seq: %d", _pDataExtent->_extentID, _expectExtID,
                    _curOffset, _curDataFileSeq ) ;
            rc = SDB_BAR_DAMAGED_BK_FILE ;
            goto error ;
         }
         ++_expectExtID ;
      }

      *pExtHeader = _pDataExtent ;

      if ( _pDataExtent->_dataSize > 0 )
      {
         rc = _allocBuff( _pDataExtent->_dataSize ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to alloc memory, rc: %d", rc ) ;

         if ( 0 == _pDataExtent->_thinCopy )
         {
            rc = _read( _curFile, _pBuff, _pDataExtent->_dataSize ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to read extent data, "
                         "offset: %llu, data seq: %d, rc: %d", _curOffset,
                         _curDataFileSeq, rc ) ;
            _curOffset += _pDataExtent->_dataSize ;
         }
         else
         {
            ossMemset( _pBuff, 0, _pDataExtent->_dataSize ) ;
         }
         *pBuff = _pBuff ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _barRSOfflineLogger implement
   */
   _barRSOfflineLogger::_barRSOfflineLogger ()
   {
      _curSUOffset      = 0 ;
      _curSUSequence    = 0 ;
      _openedSU         = FALSE ;
      _incDataFileBeginSeq = (UINT32)-1 ;
      _hasLoadDMS       = FALSE ;
   }

   _barRSOfflineLogger::~_barRSOfflineLogger ()
   {
      _closeSUFile () ;
   }

   INT32 _barRSOfflineLogger::_closeSUFile ()
   {
      if ( _openedSU )
      {
         ossClose( _curSUFile ) ;
         _openedSU      = FALSE ;
         _curSUName     = "" ;
         _curSUOffset   = 0 ;
         _curSUSequence = 0 ;
         _curSUFileName = "" ;
      }
      return SDB_OK ;
   }

   INT32 _barRSOfflineLogger::_parseExtentMeta( barBackupExtentHeader *pExtHeader,
                                                string &suName,
                                                string & suFileName,
                                                UINT32 & sequence,
                                                UINT64 & offset )
   {
      INT32 rc = SDB_OK ;

      if ( NULL == pExtHeader->getMetaData() )
      {
         PD_LOG( PDERROR, "Invalid raw data, meta data can't be empty" ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }

      try
      {
         BSONObj metaObj( pExtHeader->getMetaData() ) ;
         BSONElement ele = metaObj.getField( BAR_SU_NAME ) ;
         if ( ele.type() != String )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] error in [%s]",
                    BAR_SU_NAME, ele.type(), metaObj.toString().c_str() ) ;
            rc = SDB_BAR_DAMAGED_BK_FILE ;
            goto error ;
         }
         suName = ele.str() ;
         ele = metaObj.getField( BAR_SU_FILE_NAME ) ;
         if ( ele.type() != String )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] error in [%s]",
                    BAR_SU_FILE_NAME, ele.type(), metaObj.toString().c_str() ) ;
            rc = SDB_BAR_DAMAGED_BK_FILE ;
            goto error ;
         }
         suFileName = ele.str() ;
         ele = metaObj.getField( BAR_SU_FILE_OFFSET ) ;
         if ( ele.type() != NumberLong )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] error in [%s]",
                    BAR_SU_FILE_OFFSET, ele.type(), metaObj.toString().c_str() ) ;
            rc = SDB_BAR_DAMAGED_BK_FILE ;
            goto error ;
         }
         offset = (UINT64)ele.numberLong() ;
         ele = metaObj.getField( BAR_SU_SEQUENCE ) ;
         if ( ele.type() != NumberInt )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] error in [%s]",
                    BAR_SU_SEQUENCE, ele.type(), metaObj.toString().c_str() ) ;
            rc = SDB_BAR_DAMAGED_BK_FILE ;
            goto error ;
         }
         sequence = (UINT32)ele.numberInt() ;
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception when get meta obj: %s", e.what() ) ;
         rc = SDB_BAR_DAMAGED_BK_FILE ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSOfflineLogger::_prepareRestore( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      DPS_LSN_OFFSET beginLSN = DPS_INVALID_LSN_OFFSET ;

      if ( 0 != _metaFileSeq )
      {
         barBackupHeader mainMetaHeader ;
         rc = _readMetaHeader( 0, &mainMetaHeader, TRUE, _secretValue ) ;
         if ( rc )
         {
            goto error ;
         }
         beginLSN = mainMetaHeader._beginLSNOffset ;
         _incDataFileBeginSeq = mainMetaHeader._lastDataSequence + 1 ;
      }
      else
      {
         beginLSN = _metaHeader._beginLSNOffset ;
      }

      if ( DPS_INVALID_LSN_OFFSET != beginLSN )
      {
         rc = _pDPSCB->move( beginLSN, 1 ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to move dps to %lld, rc: %d",
                      beginLSN, rc ) ;
      }

      rc = cleanDMSData() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSOfflineLogger::_doRestore( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      barBackupExtentHeader *pExtHeader = NULL ;
      CHAR *pBuff = NULL ;
      BOOLEAN restoreDPS = FALSE ;
      BOOLEAN restoreInc = FALSE ;

      while ( TRUE )
      {
         if ( cb->isInterrupted() )
         {
            rc = SDB_APP_INTERRUPT ;
            goto error ;
         }
         rc = _readData( &pExtHeader, &pBuff ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to read data, rc: %d", rc ) ;

         if ( NULL == pExtHeader )
         {
            break ;
         }

         switch ( pExtHeader->_dataType )
         {
            case BAR_DATA_TYPE_CONFIG :
               rc = _processConfigData( pExtHeader, pBuff ) ;
               break ;
            case BAR_DATA_TYPE_RAW_DATA :
               rc = _processRawData( pExtHeader, pBuff ) ;
               break ;
            case BAR_DATA_TYPE_RAW_IDX :
               rc = _processRawIndex( pExtHeader, pBuff ) ;
               break ;
            case BAR_DATA_TYPE_RAW_LOBM :
               rc = _processRawLobM( pExtHeader, pBuff ) ;
               break ;
            case BAR_DATA_TYPE_RAW_LOBD :
               rc = _processRawLobD( pExtHeader, pBuff ) ;
               break ;
            case BAR_DATA_TYPE_REPL_LOG :
               if ( !restoreDPS )
               {
                  restoreDPS = TRUE ;
                  PD_LOG( PDEVENT, "Begin to restore dps logs..." ) ;
                  std::cout << "Begin to restore dps logs..." << std::endl ;
               }
               if ( !restoreInc && _curDataFileSeq >= _incDataFileBeginSeq )
               {
                  restoreInc = TRUE ;
                  _closeSUFile() ;

                  PD_LOG( PDEVENT, "Begin to load all collection spaces..." ) ;
                  std::cout << "Begin to load all collection spaces..."
                            << std::endl ;
                  rc = rtnLoadCollectionSpaces ( _pOptCB->getDbPath(),
                                                 _pOptCB->getIndexPath(),
                                                 _pOptCB->getLobPath(),
                                                 _pDMSCB ) ;
                  PD_RC_CHECK( rc, PDERROR, "Failed to load collection spaces, "
                               "rc: %d", rc ) ;
                  _hasLoadDMS = TRUE ;
               }
               rc = _processReplLog( pExtHeader, pBuff, restoreInc, cb ) ;
               break ;
            default :
               rc = SDB_SYS ;
               PD_LOG( PDERROR, "Unknow data type[%d]", pExtHeader->_dataType ) ;
               break ;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to process data extent[type:%d, "
                      "id:%lld], data file seq: %d, offset: %lld, rc: %d",
                      pExtHeader->_dataType, pExtHeader->_extentID,
                      _curDataFileSeq, _curOffset, rc ) ;
      }

   done:
      _closeSUFile() ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSOfflineLogger::_processConfigData( barBackupExtentHeader * pExtHeader,
                                                  const CHAR * pData )
   {
      return SDB_OK ;
   }

   INT32 _barRSOfflineLogger::_processRawData( barBackupExtentHeader * pExtHeader,
                                               const CHAR * pData )
   {
      return _writeSU( pExtHeader, pData ) ;
   }

   INT32 _barRSOfflineLogger::_processRawIndex( barBackupExtentHeader * pExtHeader,
                                                const CHAR * pData )
   {
      return _writeSU( pExtHeader, pData ) ;
   }

   INT32 _barRSOfflineLogger::_processRawLobM( barBackupExtentHeader * pExtHeader,
                                               const CHAR * pData )
   {
      return _writeSU( pExtHeader, pData ) ;
   }

   INT32 _barRSOfflineLogger::_processRawLobD( barBackupExtentHeader * pExtHeader,
                                               const CHAR * pData )
   {
      return _writeSU( pExtHeader, pData ) ;
   }

   INT32 _barRSOfflineLogger::_processReplLog( barBackupExtentHeader * pExtHeader,
                                               const CHAR * pData,
                                               BOOLEAN isIncData,
                                               _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      const CHAR *pLogIndex = pData ;
      DPS_LSN lsn ;
      _clsReplayer replayer ;

      while ( pLogIndex < pData + pExtHeader->_dataSize )
      {
         dpsLogRecordHeader *pHeader = (dpsLogRecordHeader *)pLogIndex ;
         lsn = _pDPSCB->expectLsn() ;

         if ( 0 != lsn.compareOffset( pHeader->_lsn ) )
         {
            PD_LOG( PDERROR, "Expect lsn[%lld] is not the same with cur "
                    "lsn[%lld]", lsn.offset, pHeader->_lsn ) ;
            rc = SDB_BAR_DAMAGED_BK_FILE ;
            goto error ;
         }

         if ( isIncData )
         {
            rc = replayer.replay( pHeader, cb ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to reply log[ lsn: %lld, type: "
                         "%d, len: %d ], rc: %d", pHeader->_lsn, pHeader->_type,
                         pHeader->_length, rc ) ;
         }
         rc = _pDPSCB->recordRow( pLogIndex, pHeader->_length ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to write lsn[%d,%lld], length: %d, "
                      "rc: %d", pHeader->_version, pHeader->_lsn,
                      pHeader->_length, rc ) ;
         if ( DPS_INVALID_LSN_OFFSET != _metaHeader._transLSNOffset )
         {
            dpsLogRecord record ;
            rc = record.load( pLogIndex ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to load logRecord, lsn[%d,%lld], "
                         "type: %d, rc: %d", pHeader->_version, pHeader->_lsn,
                         pHeader->_type, rc ) ;
            _pTransCB->saveTransInfoFromLog( record ) ;
         }

         pLogIndex += pHeader->_length ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSOfflineLogger::_writeSU( barBackupExtentHeader * pExtHeader,
                                        const CHAR * pData )
   {
      INT32 rc = SDB_OK ;
      string suName ;
      string suFileName ;
      UINT32 sequence = 0 ;
      UINT64 offset = 0 ;
      string path ;

      rc = _parseExtentMeta( pExtHeader, suName, suFileName, sequence,
                             offset ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( _openedSU && 0 != _curSUFileName.compare( suFileName ) )
      {
         _closeSUFile() ;
      }

      if ( BAR_DATA_TYPE_RAW_DATA == pExtHeader->_dataType )
      {
         path = _pOptCB->getDbPath() ;
      }
      else if ( BAR_DATA_TYPE_RAW_IDX == pExtHeader->_dataType )
      {
         path = _pOptCB->getIndexPath() ;
      }
      else
      {
         path = _pOptCB->getLobPath() ;
      }

      rc = _openSUFile( path, suName, suFileName, sequence  ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to open su file[%s], rc: %d",
                   suFileName.c_str(), rc ) ;

      if ( offset != _curSUOffset )
      {
         rc = ossSeek( &_curSUFile, offset, OSS_SEEK_SET ) ;
         PD_RC_CHECK( rc, PDERROR, "Seek file[%s] offset[%lld] failed, rc: %d",
                      _curSUFileName.c_str(), offset, rc ) ;
         _curSUOffset = offset ;
      }

      rc = _flush( _curSUFile, pData, pExtHeader->_dataSize ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to write data to file[%s], rc: %d",
                   _curSUFileName.c_str(), rc ) ;
      _curSUOffset += pExtHeader->_dataSize ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSOfflineLogger::_openSUFile( const string &path,
                                           const string & suName,
                                           const string & suFileName,
                                           UINT32 sequence )
   {
      INT32 rc = SDB_OK ;
      string pathName = rtnFullPathName( path, suFileName ) ;

      if ( _openedSU )
      {
         goto done ;
      }
      rc = ossOpen( pathName.c_str(), OSS_CREATE|OSS_READWRITE,
                    OSS_RU | OSS_WU | OSS_RG, _curSUFile ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to open su file[%s], rc: %d",
                   pathName.c_str(), rc ) ;

      _openedSU = TRUE ;
      _curSUName = suName ;
      _curSUFileName = suFileName ;
      _curSUSequence = sequence ;

      PD_LOG( PDEVENT, "Begin to restore su[%s]...", _curSUFileName.c_str() );
      std::cout << "Begin to restore su: " << _curSUFileName.c_str()
                << " ..." << std::endl ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barRSOfflineLogger::_afterRestore( pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;

      if ( DPS_INVALID_LSN_OFFSET != _metaHeader._transLSNOffset )
      {
         if ( !_hasLoadDMS )
         {
            PD_LOG( PDEVENT, "Begin to load all collection spaces..." ) ;
            std::cout << "Begin to load all collection spaces..." << std::endl ;

            rc = rtnLoadCollectionSpaces ( _pOptCB->getDbPath(),
                                           _pOptCB->getIndexPath(),
                                           _pOptCB->getLobPath(),
                                           _pDMSCB ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to load collection spaces, "
                         "rc: %d", rc ) ;
            _hasLoadDMS = TRUE ;
         }

         PD_LOG( PDEVENT, "Begin to rollback all trans..." ) ;
         std::cout << "Begin to rollback all trans..." << std::endl ;
         rc = rtnTransRollbackAll( cb ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to rollback all trans, rc: %d",
                      rc ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
      _barBackupMgr implement
   */
   _barBackupMgr::_barBackupMgr ( const string &checkGroupName,
                                  const string &checkHostName,
                                  const string &checkSvcName )
   {
      _checkGroupName = checkGroupName ;
      _checkHostName = checkHostName ;
      _checkSvcName = checkSvcName ;
      
   }

   _barBackupMgr::~_barBackupMgr ()
   {
   }

   UINT32 _barBackupMgr::count ()
   {
      return _backupInfo.size() ;
   }

   /*INT32 _barBackupMgr::_enumSpecBackup()
   {
      barBackupInfo info ;
      info._name = backupName () ;
      info._maxIncID = _ensureMetaFileSeq() ;

      if ( info._maxIncID > 0 )
      {
         _backupInfo.push_back( info ) ;
      }
      return SDB_OK ;
   }*/

   INT32 _barBackupMgr::_enumBackups ( const string &fullPath,
                                       const string &subPath )
   {
      INT32 rc = SDB_OK ;
      barBackupInfo info ;

      fs::path dbDir ( fullPath ) ;
      fs::directory_iterator end_iter ;

      if ( fs::exists ( dbDir ) && fs::is_directory ( dbDir ) )
      {
         for ( fs::directory_iterator dir_iter ( dbDir );
               dir_iter != end_iter; ++dir_iter )
         {
            if ( fs::is_regular_file ( dir_iter->status() ) )
            {
               const std::string fileName =
                  dir_iter->path().filename().string() ;
               if ( parseMainFile ( fileName, info._name ) &&
                    ( info._maxIncID = _ensureMetaFileSeq( fullPath,
                      info._name ) ) > 0 )
               {
                  if ( _backupName.empty() ||
                       0 == _backupName.compare( info._name ) )
                  {
                     info._relPath = subPath ;
                     _backupInfo.push_back( info ) ;
                  }
               }
            }
            else if ( fs::is_directory( dir_iter->path() ) )
            {
               string tmpSubPath = subPath ;
               if ( !tmpSubPath.empty() )
               {
                  tmpSubPath += OSS_FILE_SEP ;
               }
               tmpSubPath += dir_iter->path().leaf().string() ;
               _enumBackups( dir_iter->path().string(), tmpSubPath ) ;
            }
         }
      }
      else
      {
         PD_LOG( PDERROR, "Path[%s] not exist or invalid", path() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBackupMgr::init( const CHAR *path,
                              const CHAR *backupName,
                              const CHAR *prefix )
   {
      INT32 rc = SDB_OK ;
      _backupInfo.clear() ;

      rc = _initInner( path, backupName, prefix ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( !backupName || 0 == ossStrlen( backupName ) )
      {
         _backupName = "" ;
      }
      rc = _enumBackups( _path, "" ) ;

      PD_RC_CHECK( rc, PDERROR, "Enum backup failed, path: %s, name: %s, "
                   "rc: %d", _path.c_str(), _backupName.c_str(), rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBackupMgr::_backupToBSON( const barBackupInfo &info,
                                       vector< BSONObj > &vecBackup,
                                       BOOLEAN detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 incID = 0 ;
      UINT64 secretValue = 0 ;
      BOOLEAN hasError = FALSE ;
      BSONObj obj ;

      while ( incID < info._maxIncID )
      {
         rc = _readMetaHeader( incID, &_metaHeader, TRUE, secretValue ) ;
         if ( SDB_BAR_DAMAGED_BK_FILE == rc )
         {
            hasError = TRUE ;
            rc = SDB_OK ;
         }
         else
         {
            hasError = FALSE ;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to read meta header, rc: %d", rc ) ;

         if ( !_checkGroupName.empty() &&
              0 != ossStrncmp( _metaHeader._groupName, _checkGroupName.c_str(),
                               BAR_BACKUP_GROUPNAME_LEN - 1 ) )
         {
            goto done ;
         }
         if ( !_checkHostName.empty() &&
              0 != ossStrncmp( _metaHeader._hostName, _checkHostName.c_str(),
                               BAR_BACKUP_HOSTNAME_LEN - 1 ) )
         {
            goto done ;
         }
         if ( !_checkSvcName.empty() &&
              0 != ossStrncmp( _metaHeader._svcName, _checkSvcName.c_str(),
                               BAR_BACKUP_SVCNAME_LEN - 1 ) )
         {
            goto done ;
         }

         if ( 0 == secretValue )
         {
            secretValue = _metaHeader._secretValue ;
         }

         rc = _metaHeaderToBSON( &_metaHeader, info._relPath, hasError,
                                 obj, detail ) ;
         PD_RC_CHECK( rc, PDERROR, "Meta header to bson failed, rc: %d", rc ) ;

         vecBackup.push_back( obj ) ;

         ++incID ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBackupMgr::_metaHeaderToBSON( barBackupHeader *pHeader,
                                           const string &relPath,
                                           BOOLEAN hasError, BSONObj &obj,
                                           BOOLEAN detail )
   {
      BSONObjBuilder builder ;

      builder.append( FIELD_NAME_NAME, pHeader->_name ) ;
      if ( 0 != pHeader->_description[0] )
      {
         builder.append( FIELD_NAME_DESP, pHeader->_description ) ;
      }
      if ( !relPath.empty() )
      {
         builder.append( FIELD_NAME_PATH, relPath.c_str() ) ;
      }

      if ( SDB_ROLE_STANDALONE != pmdGetKRCB()->getDBRole() )
      {
         monAppendSystemInfo( builder, MON_MASK_NODE_NAME |
                                       MON_MASK_GROUP_NAME ) ;
      }
      else
      {
         if ( 0 != pHeader->_groupName[0] )
         {
            builder.append( FIELD_NAME_GROUPNAME, pHeader->_groupName ) ;
         }
         string nodeName = pHeader->_hostName ;
         if ( !nodeName.empty() )
         {
            nodeName += ":" ;
            nodeName += pHeader->_svcName ;
            builder.append( FIELD_NAME_NODE_NAME, nodeName ) ;
         }
      }

      builder.append( FIELD_NAME_ENSURE_INC,
                      pHeader->_opType == BAR_BACKUP_OP_TYPE_INC ?
                      true : false ) ;

      builder.append( "BeginLSNOffset", (INT64)pHeader->_beginLSNOffset ) ;
      builder.append( "EndLSNOffset", (INT64)pHeader->_endLSNOffset ) ;
      if ( DPS_INVALID_LSN_OFFSET != pHeader->_transLSNOffset )
      {
         builder.append( "TransLSNOffset", (INT64)pHeader->_transLSNOffset ) ;
      }
      builder.append( "StartTime", pHeader->_startTimeStr ) ;
      if ( detail )
      {
         builder.append( "EndTime", pHeader->_endTimeStr ) ;
         builder.append( "UseTime", (INT32)pHeader->_useTime ) ;
         builder.append( "CSNum", (INT32)pHeader->_csNum ) ;
         builder.append( "DataFileNum", (INT32)pHeader->_dataFileNum ) ;
         builder.append( "LastDataFileSeq", (INT32)pHeader->_lastDataSequence ) ;
         builder.append( "LastExtentID", (INT64)pHeader->_lastExtentID ) ;
         builder.append( "DataSize", (INT64)pHeader->_dataSize ) ;
      }

      builder.append( "HasError", hasError ? true : false ) ;
      obj = builder.obj () ;

      return SDB_OK ;
   }

   INT32 _barBackupMgr::list( vector < BSONObj > &vecBackup,
                              BOOLEAN detail )
   {
      INT32 rc = SDB_OK ;
      BSONObj backupObj ;
      string tmpPath = _path ;
      string tmpName = _backupName ;
      
      vecBackup.clear() ;

      vector< barBackupInfo >::iterator it = _backupInfo.begin() ;
      while ( it != _backupInfo.end() )
      {
         barBackupInfo &info = ( *it ) ;

         if ( info._relPath.empty() )
         {
            _path = tmpPath ;
         }
         else
         {
            _path = rtnFullPathName( tmpPath, info._relPath ) ;
         }
         _backupName = info._name ;

         rc = _backupToBSON( info, vecBackup, detail ) ;
         PD_RC_CHECK( rc, PDERROR, "Backup[%s] to bson failed, rc: %d",
                      (*it)._name.c_str(), rc ) ;
         ++it ;
      }

   done:
      _path = tmpPath ;
      _backupName = tmpName ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBackupMgr::_drop ( const string &backupName )
   {
      INT32 rc = SDB_OK ;
      string fileName ;

      UINT32 sequence = _findMaxSeq( backupName, TRUE ) ;

      while ( sequence > 0 )
      {
         fileName = getDataFileName( backupName, sequence ) ;
         if ( SDB_OK == ossAccess( fileName.c_str() ) )
         {
            rc = ossDelete( fileName.c_str() ) ;
            PD_RC_CHECK( rc, PDWARNING, "Failed to delete file[%s], rc: %d",
                         fileName.c_str(), rc ) ;
         }
         --sequence ;
      }

      sequence = _findMaxSeq( backupName, FALSE ) ;
      while ( sequence >= 0 )
      {
         fileName = getIncFileName( backupName, sequence ) ;
         if ( SDB_OK == ossAccess( fileName.c_str() ) )
         {
            rc = ossDelete( fileName.c_str() ) ;
            PD_RC_CHECK( rc, PDWARNING, "Failed to delete file[%s], rc: %d",
                         fileName.c_str(), rc ) ;
         }
         if ( 0 == sequence )
         {
            break ;
         }
         --sequence ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _barBackupMgr::drop ()
   {
      INT32 rc = SDB_OK ;
      string tmpPath = _path ;
      string tmpName = _backupName ;

      vector< barBackupInfo >::iterator it = _backupInfo.begin() ;
      while ( it != _backupInfo.end() )
      {
         barBackupInfo &info = ( *it ) ;

         vector< BSONObj > objList ;

         if ( info._relPath.empty() )
         {
            _path = tmpPath ;
         }
         else
         {
            _path = rtnFullPathName( tmpPath, info._relPath ) ;
         }
         _backupName = info._name ;

         rc = _backupToBSON( info, objList, FALSE ) ;
         PD_RC_CHECK( rc, PDWARNING, "back up to bson failed, rc: %d", rc ) ;

         if ( objList.size() == 0 )
         {
            ++it ;
            continue ;
         }

         rc = _drop ( info._name ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to drop backup[%s], rc: %d",
                      (*it)._name.c_str(), rc ) ;
         ++it ;
      }

   done:
      _path = tmpPath ;
      _backupName = tmpName ;
      return rc ;
   error:
      goto done ;
   }

   UINT32 _barBackupMgr::_findMaxSeq( const string &backupName,
                                      BOOLEAN isDataFile )
   {
      string fileName ;
      UINT32 step = 1000 ;
      UINT32 low = 1 ;
      UINT32 high = 1 ;
      UINT32 mid = 0 ;

      while ( TRUE )
      {
         if ( isDataFile )
         {
            fileName = getDataFileName( backupName, high ) ;
         }
         else
         {
            fileName = getIncFileName( backupName, high ) ;
         }
         if ( 0 == ossAccess( fileName.c_str() ) )
         {
            high += step ;
         }
         else
         {
            break ;
         }
      }

      mid = ( low + high ) / 2 ;
      while ( mid != low && mid != high )
      {
         if ( isDataFile )
         {
            fileName = getDataFileName( backupName, mid ) ;
         }
         else
         {
            fileName = getIncFileName( backupName, mid ) ;
         }

         if ( 0 == ossAccess( fileName.c_str() ) )
         {
            low = mid ;
         }
         else
         {
            high = mid ;
         }
         mid = ( low + high ) / 2 ;
      }

      return mid ;
   }

}

