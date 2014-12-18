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

   Source File Name = dmsStorageLob.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          17/07/2014  YW Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DMS_STORAGELOB_HPP_
#define DMS_STORAGELOB_HPP_

#include "dmsLobDef.hpp"
#include "dmsStorageLobData.hpp"
#include "dmsStorageData.hpp"
#include "ossLatch.hpp"

namespace engine
{
   #define DMS_BME_OFFSET        DMS_MME_OFFSET

   const UINT32 DMS_BUCKETS_NUM =   16777216 ; // 16MB
   #define DMS_BUCKETS_MODULO       16777215

   /*
      _dmsBucketsManagementExtent define
   */
   struct _dmsBucketsManagementExtent : public SDBObject
   {
      INT32 _buckets[DMS_BUCKETS_NUM] ;

      _dmsBucketsManagementExtent()
      {
         ossMemset( this, 0xff,
                    sizeof( _dmsBucketsManagementExtent ) ) ;
      }
   } ;
   typedef struct _dmsBucketsManagementExtent dmsBucketsManagementExtent ;

   #define DMS_BME_SZ (sizeof( dmsBucketsManagementExtent ))


   class _pmdEDUCB ;

   /*
      _dmsStorageLob define
   */
   class _dmsStorageLob : public _dmsStorageBase
   {
   public:
      _dmsStorageLob( const CHAR *lobmFileName,
                      const CHAR *lobdFileName,
                      dmsStorageInfo *info,
                      dmsStorageData *pDataSu ) ;
      virtual ~_dmsStorageLob() ;

      _dmsStorageLobData*  getLobData() { return &_data ; }

   public:
      INT32 open( const CHAR *path,
                  BOOLEAN createNew,
                  BOOLEAN rmWhenExist) ;

      void removeStorageFiles() ;

      BOOLEAN isOpened()const ;

      INT32 getLobMeta( const bson::OID &oid,
                        dmsMBContext *mbContext,
                        _pmdEDUCB *cb,
                        _dmsLobMeta &meta ) ;

      INT32 writeLobMeta( const bson::OID &oid,
                          dmsMBContext *mbContext,
                          _pmdEDUCB *cb,
                          const _dmsLobMeta &meta,
                          BOOLEAN isNew,
                          SDB_DPSCB *dpsCB ) ;

      INT32 write( const dmsLobRecord &record,
                   dmsMBContext *mbContext,
                   _pmdEDUCB *cb,
                   SDB_DPSCB *dpscb ) ;

      INT32 update( const dmsLobRecord &record,
                    dmsMBContext *mbContext,
                    _pmdEDUCB *cb,
                    SDB_DPSCB *dpscb ) ;

      INT32 remove( const dmsLobRecord &record,
                    dmsMBContext *mbContext,
                    _pmdEDUCB *cb,
                    SDB_DPSCB *dpscb ) ;

      INT32 read( const dmsLobRecord &record,
                  dmsMBContext *mbContext,
                  _pmdEDUCB *cb,
                  CHAR *buf,
                  UINT32 &len ) ;

      INT32 readPage( DMS_LOB_PAGEID &pos,
                      BOOLEAN onlyMetaPage,
                      _pmdEDUCB *cb,
                      dmsMBContext *mbContext,
                      dmsLobInfoOnPage &page ) ;

      INT32 truncate( dmsMBContext *mbContext,
                      _pmdEDUCB *cb,
                      SDB_DPSCB *dpscb ) ;
   protected:
      INT32  _openLob( const CHAR *path,
                       BOOLEAN createNew,
                       BOOLEAN rmWhenExist ) ;
      INT32 _delayOpen() ;

   private:
      virtual INT32  _onCreate( OSSFILE *file, UINT64 curOffSet ) ;
      virtual INT32  _onMapMeta( UINT64 curOffSet ) ;
      virtual UINT32 _getSegmentSize() const ;
      virtual UINT32 _extendThreshold() const ;
      virtual UINT64 _dataOffset() ;
      virtual INT32  _extendSegments( UINT32 numSeg ) ;
      virtual const CHAR* _getEyeCatcher() const ;
      virtual UINT32 _curVersion() const ;
      virtual INT32  _checkVersion( dmsStorageUnitHeader *pHeader ) ;
      virtual void   _onClosed() ;
      virtual void   _initHeaderPageSize( dmsStorageUnitHeader *pHeader,
                                          dmsStorageInfo *pInfo ) ;
      virtual INT32  _checkPageSize( dmsStorageUnitHeader *pHeader ) ;

   private:
      OSS_INLINE UINT32 _getBucket( UINT32 hash )
      {
         return hash & DMS_BUCKETS_MODULO ;
      }

      INT32 _push2Bucket( UINT32 bucket, DMS_LOB_PAGEID pageId,
                          _dmsLobDataMapBlk &blk) ;

      INT32 _find( const _dmsLobRecord &record,
                   UINT32 clID,
                   DMS_LOB_PAGEID &page,
                   _dmsLobDataMapBlk *&blk,
                   UINT32 *bucket = NULL ) ;

      INT32 _allocatePage( const dmsLobRecord &record,
                           dmsMBContext *mbContext,
                           DMS_LOB_PAGEID &page ) ;

      INT32 _fillPage( const dmsLobRecord &record,
                       DMS_LOB_PAGEID page,
                       dmsMBContext *mbContext ) ;

      INT32 _findPage( const dmsLobRecord &record,
                      dmsMBContext *mbContext,
                      DMS_LOB_PAGEID &page ) ;

      INT32 _releasePage( DMS_LOB_PAGEID page ) ;

      INT32 _removePage( DMS_LOB_PAGEID page,
                         const _dmsLobDataMapBlk *blk,
                         const UINT32 *bucket ) ;

   private:
      dmsBucketsManagementExtent    *_dmsBME ;
      UINT32                        _segmentSize ;
      _dmsStorageData               *_dmsData ;
      _dmsStorageLobData            _data ;
      CHAR                          _path[ OSS_MAX_PATHSIZE + 1 ] ;
      BOOLEAN                       _needDelayOpen ;
      ossSpinXLatch                 _delayOpenLatch ;

   } ;
   typedef class _dmsStorageLob dmsStorageLob ;

}

#endif // DMS_STORAGELOB_HPP_

