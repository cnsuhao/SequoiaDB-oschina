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

   Source File Name = dmsStorageLobData.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          17/07/2014  YW Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DMS_STORAGELOBDATA_HPP_
#define DMS_STORAGELOBDATA_HPP_

#include "dmsLobDef.hpp"
#include "ossIO.hpp"
#include "dmsStorageBase.hpp"
#include "pmdEDU.hpp"

namespace engine
{
   /*
      _dmsStorageLobData define
   */
   class _dmsStorageLobData : public SDBObject
   {
   public:
      _dmsStorageLobData( const CHAR *fileName ) ;
      virtual ~_dmsStorageLobData() ;

   public:
      OSS_INLINE INT64 getFileSz() const
      {
         return _fileSz ;
      }

      OSS_INLINE INT64 getDataSz() const
      {
         return getFileSz() - sizeof( _dmsStorageUnitHeader ) ;
      }

      OSS_INLINE const std::string &getFileName() const
      {
         return _fileName ;
      }

      OSS_INLINE UINT32 pageSize() const { return _pageSz ; }
      OSS_INLINE UINT32 pageSizeSquareRoot() const { return _logarithmic ; }

      OSS_INLINE UINT32 getSegmentSize() const { return DMS_SEGMENT_SZ ; }
      OSS_INLINE UINT32 segmentPages() const { return _segmentPages ; }
      OSS_INLINE UINT32 segmentPagesSquareRoot() const { return _segmentPagesSquare ; }

      INT32 open( const CHAR *path,
                  BOOLEAN createNew,
                  BOOLEAN delWhenExist,
                  const dmsStorageInfo &info,
                  _pmdEDUCB *cb ) ;

      BOOLEAN isOpened() const ;

      INT32 close() ;

      INT32 remove() ;

      INT32 write( DMS_LOB_PAGEID page,
                   const CHAR *data,
                   UINT32 len,
                   UINT32 offset,
                   _pmdEDUCB *cb ) ;

      INT32 read( DMS_LOB_PAGEID page,
                  UINT32 len,
                  UINT32 offset,
                  _pmdEDUCB *cb,
                  CHAR *buf,
                  UINT32 &readLen ) ;

      INT32 extend( INT64 len ) ;

      INT32 readRaw( UINT64 offset,
                     UINT32 len,
                     CHAR *buf,
                     UINT32 &readLen ) ;

   private:
      INT32 _initFileHeader( const dmsStorageInfo &info,
                             _pmdEDUCB *cb ) ;

      INT32 _validateFile( const dmsStorageInfo &info,
                           _pmdEDUCB *cb ) ;

      INT32 _getFileHeader( _dmsStorageUnitHeader &header,
                            _pmdEDUCB *cb ) ;

      OSS_INLINE SINT64 getSeek( DMS_LOB_PAGEID page,
                                 UINT32 offset ) const
      {
         INT64 seek = page ;
         return ( seek << _logarithmic ) +
                sizeof( _dmsStorageUnitHeader ) + offset ;
      }

      INT32 _reopen() ;

      INT32 _extend( INT64 len ) ;

   private:
      std::string       _fileName ;
      CHAR              _fullPath[ OSS_MAX_PATHSIZE + 1 ] ;
      OSSFILE           _file ;
      INT64             _fileSz ;
      INT64             _lastSz ;
      UINT32            _pageSz ;
      UINT32            _logarithmic ;
      UINT32            _flags ; 
      UINT32            _segmentPages ;
      UINT32            _segmentPagesSquare ;

   } ;
   typedef class _dmsStorageLobData dmsStorageLobData ;
}

#endif // DMS_STORAGELOBDATA_HPP_

