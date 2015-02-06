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

   Source File Name = dmsStorageData.hpp

   Descriptive Name = Data Management Service Storage Unit Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          14/08/2013  XJH Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef DMSSTORAGE_DATA_HPP_
#define DMSSTORAGE_DATA_HPP_

#include "dmsStorageBase.hpp"
#include "dpsLogWrapper.hpp"

#include <map>

using namespace bson ;

namespace engine
{

#pragma pack(1)
   /*
      _IDToInsert define
   */
   class _IDToInsert : public SDBObject
   {
   public :
      CHAR _type ;
      CHAR _id[4] ; // _id + '\0'
      OID _oid ;
      _IDToInsert ()
      {
         _type = (CHAR)jstOID ;
         _id[0] = '_' ;
         _id[1] = 'i' ;
         _id[2] = 'd' ;
         _id[3] = 0 ;
         SDB_ASSERT ( sizeof ( _IDToInsert) == 17,
                      "IDToInsert should be 17 bytes" ) ;
      }
   } ;
   typedef class _IDToInsert IDToInsert ;

   /*
      _idToInsert define
   */
   class _idToInsertEle : public BSONElement
   {
   public :
      _idToInsertEle( CHAR* x ) : BSONElement((CHAR*) ( x )){}
   } ;
   typedef class _idToInsertEle idToInsertEle ;

#pragma pack()

   /*
      MB FLAG(_flag) values :
   */
   #define DMS_MB_BASE_MASK                        0x000F
   #define DMS_MB_FLAG_FREE                        0x0000
   #define DMS_MB_FLAG_USED                        0x0001
   #define DMS_MB_FLAG_DROPED                      0x0002

   #define DMS_MB_OPR_TYPE_MASK                    0x00F0
   #define DMS_MB_FLAG_OFFLINE_REORG               0x0010
   #define DMS_MB_FLAG_ONLINE_REORG                0x0020
   #define DMS_MB_FLAG_LOAD                        0x0040

   #define DMS_MB_OPR_PHASE_MASK                   0x0F00

   #define DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY   0x0100
   #define DMS_MB_FLAG_OFFLINE_REORG_TRUNCATE      0x0200
   #define DMS_MB_FLAG_OFFLINE_REORG_COPY_BACK     0x0400
   #define DMS_MB_FLAG_OFFLINE_REORG_REBUILD       0x0800

   #define DMS_MB_FLAG_LOAD_LOAD                   0x0100
   #define DMS_MB_FLAG_LOAD_BUILD                  0x0200

   #define DMS_MB_BASE_FLAG(x)                     ((x)&DMS_MB_BASE_MASK)
   #define DMS_MB_OPR_FLAG(x)                      ((x)&DMS_MB_OPR_TYPE_MASK)
   #define DMS_MB_PHASE_FLAG(x)                    ((x)&DMS_MB_OPR_PHASE_MASK)

   #define DMS_IS_MB_FREE(x)        (DMS_MB_FLAG_FREE==(x))
   #define DMS_SET_MB_FREE(x)       do {(x)=DMS_MB_FLAG_FREE ;} while(0)
   #define DMS_IS_MB_INUSE(x)       (0!=((x)&DMS_MB_FLAG_USED))
   #define DMS_SET_MB_INUSE(x)      do {(x)|=DMS_MB_FLAG_USED ;} while(0)
   #define DMS_IS_MB_DROPPED(x)     (DMS_MB_FLAG_DROPED==(x))
   #define DMS_SET_MB_DROPPED(x)    do {(x)=DMS_MB_FLAG_DROPED ;} while(0)
   #define DMS_IS_MB_NORMAL(x)      (DMS_MB_FLAG_USED==(x))
   #define DMS_SET_MB_NORMAL(x)     do {(x)=DMS_MB_FLAG_USED ;} while(0)

   #define DMS_IS_MB_OFFLINE_REORG(x)  \
      ((0!=((x)&DMS_MB_FLAG_OFFLINE_REORG))&&(DMS_IS_MB_INUSE(x)))
   #define DMS_SET_MB_OFFLINE_REORG(x) \
      do {(x)=DMS_MB_FLAG_OFFLINE_REORG|DMS_MB_FLAG_USED;} while(0)
   #define DMS_IS_MB_ONLINE_REORG(x)   \
      ((0!=((x)&DMS_MB_FLAG_ONLINE_REORG))&&(DMS_IS_MB_INUSE(x)))
   #define DMS_SET_MB_ONLINE_REORG(x)  \
      do {(x)=DMS_MB_FLAG_ONLINE_REORG|DMS_MB_FLAG_USED;} while(0)
   #define DMS_IS_MB_OFFLINE_REORG_SHADOW_COPY(x)     \
      ((0!=((x)&DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY))&&\
      (DMS_IS_MB_OFFLINE_REORG(x)))
   #define DMS_SET_MB_OFFLINE_REORG_SHADOW_COPY(x)    \
      do {(x)=DMS_MB_FLAG_OFFLINE_REORG_SHADOW_COPY|DMS_MB_FLAG_OFFLINE_REORG|\
      DMS_MB_FLAG_USED;} while(0)
   #define DMS_IS_MB_OFFLINE_REORG_TRUNCATE(x)        \
      ((0!=((x)&DMS_MB_FLAG_OFFLINE_REORG_TRUNCATE))&&\
      (DMS_IS_MB_OFFLINE_REORG(x)))
   #define DMS_SET_MB_OFFLINE_REORG_TRUNCATE(x)       \
      do {(x)=DMS_MB_FLAG_OFFLINE_REORG_TRUNCATE|DMS_MB_FLAG_OFFLINE_REORG|\
      DMS_MB_FLAG_USED;} while(0)
   #define DMS_IS_MB_OFFLINE_REORG_COPY_BACK(x)       \
      ((0!=((x)&DMS_MB_FLAG_OFFLINE_REORG_COPY_BACK))&&\
      (DMS_IS_MB_OFFLINE_REORG(x)))
   #define DMS_SET_MB_OFFLINE_REORG_COPY_BACK(x)      \
      do {(x)=DMS_MB_FLAG_OFFLINE_REORG_COPY_BACK|DMS_MB_FLAG_OFFLINE_REORG|\
      DMS_MB_FLAG_USED;} while(0)
   #define DMS_IS_MB_OFFLINE_REORG_REBUILD(x)         \
      ((0!=((x)&DMS_MB_FLAG_OFFLINE_REORG_REBUILD))&&\
      (DMS_IS_MB_OFFLINE_REORG(x)))
   #define DMS_SET_MB_OFFLINE_REORG_REBUILD(x)        \
      do {(x)=DMS_MB_FLAG_OFFLINE_REORG_REBUILD|DMS_MB_FLAG_OFFLINE_REORG|\
      DMS_MB_FLAG_USED;} while(0)

   #define DMS_IS_MB_LOAD(x)                          \
      (0!=((x)&DMS_MB_FLAG_LOAD)&&(DMS_IS_MB_INUSE(x)))
   #define DMS_SET_MB_LOAD(x)                         \
      do {(x)=DMS_MB_FLAG_LOAD|DMS_MB_FLAG_USED;} while(0)
   #define DMS_IS_MB_FLAG_LOAD_LOAD(x)                \
      ((0!=((x)&DMS_MB_FLAG_LOAD_LOAD))&&(DMS_IS_MB_LOAD(x)))
   #define DMS_SET_MB_FLAG_LOAD_LOAD(x)               \
      do {(x)=DMS_MB_FLAG_LOAD_LOAD|DMS_MB_FLAG_LOAD|\
      DMS_MB_FLAG_USED;} while(0)
   #define DMS_IS_MB_FLAG_LOAD_BUILD(x)               \
      ((0!=((x)&DMS_MB_FLAG_LOAD_BUILD))&&(DMS_IS_MB_LOAD(x)))
   #define DMS_SET_MB_FLAG_LOAD_BUILD(x)              \
      do {(x)=DMS_MB_FLAG_LOAD_BUILD|DMS_MB_FLAG_LOAD|\
      DMS_MB_FLAG_USED;} while(0)

   /*
      DMS MB ATTRIBUTE DEFINE
   */
   #define DMS_MB_ATTR_COMPRESSED         0x00000001
   #define DMS_MB_ATTR_NOIDINDEX          0x00000002

#pragma pack(4)
   /*
      _dmsMetadataBlock defined
   */
   struct _dmsMetadataBlock
   {
      enum deleteListType
      {
         _32 = 0,
         _64,
         _128,
         _256,
         _512,
         _1k,
         _2k,
         _4k,
         _8k,
         _16k,
         _32k,
         _64k,
         _128k,
         _256k,
         _512k,
         _1m,
         _2m,
         _4m,
         _8m,
         _16m,
         _max
      } ;

      CHAR           _collectionName [ DMS_COLLECTION_NAME_SZ+1 ] ;
      UINT16         _flag ;
      UINT16         _blockID ;
      dmsExtentID    _firstExtentID ;
      dmsExtentID    _lastExtentID ;
      UINT32         _numIndexes ;
      dmsRecordID    _deleteList [_max] ;
      dmsExtentID    _indexExtent [DMS_COLLECTION_MAX_INDEX] ;
      UINT32         _logicalID ;
      UINT32         _indexHWCount ;
      UINT32         _attributes ;
      dmsExtentID    _loadFirstExtentID ;
      dmsExtentID    _loadLastExtentID ;
      dmsExtentID    _mbExExtentID ;
      UINT64         _totalRecords ;
      UINT32         _totalDataPages ;
      UINT32         _totalIndexPages ;
      UINT64         _totalDataFreeSpace ;
      UINT64         _totalIndexFreeSpace ;
      UINT32         _totalLobPages ;
      CHAR           _pad [ 404 ] ;

      void reset ( const CHAR *clName = NULL,
                   UINT16 mbID = DMS_INVALID_MBID,
                   UINT32 clLID = DMS_INVALID_CLID,
                   UINT32 attr = 0 )
      {
         INT32 i = 0 ;
         ossMemset( _collectionName, 0, sizeof( _collectionName ) ) ;
         if ( clName )
         {
            ossStrncpy( _collectionName, clName, DMS_COLLECTION_NAME_SZ ) ;
         }
         if ( DMS_INVALID_MBID != mbID )
         {
            DMS_SET_MB_INUSE( _flag ) ;
         }
         else
         {
            DMS_SET_MB_FREE( _flag ) ;
         }
         _blockID = mbID ;
         _firstExtentID = DMS_INVALID_EXTENT ;
         _lastExtentID  = DMS_INVALID_EXTENT ;
         _numIndexes    = 0 ;
         for ( i = 0 ; i < _max ; ++i )
         {
            _deleteList[i].reset() ;
         }
         for ( i = 0 ; i < DMS_COLLECTION_MAX_INDEX ; ++i )
         {
            _indexExtent[i] = DMS_INVALID_EXTENT ;
         }
         _logicalID = clLID ;
         _indexHWCount = 0 ;
         _attributes   = attr ;
         _loadFirstExtentID = DMS_INVALID_EXTENT ;
         _loadLastExtentID  = DMS_INVALID_EXTENT ;
         _mbExExtentID      = DMS_INVALID_EXTENT ;

         _totalRecords           = 0 ;
         _totalDataPages         = 0 ;
         _totalIndexPages        = 0 ;
         _totalDataFreeSpace     = 0 ;
         _totalIndexFreeSpace    = 0 ;
         _totalLobPages          = 0 ;

         ossMemset( _pad, 0, sizeof( _pad ) ) ;
      }
   } ;
   typedef _dmsMetadataBlock  dmsMetadataBlock ;
   typedef dmsMetadataBlock   dmsMB ;
   #define DMS_MB_SIZE                 (1024)

#pragma pack()

   /*
      Type to String functions
   */
   void  mbFlag2String ( UINT16 flag, CHAR *pBuffer, INT32 bufSize ) ;
   void  mbAttr2String ( UINT32 attributes, CHAR *pBuffer, INT32 bufSize ) ;

   /*
      _metadataBlockEx define
   */
   struct _metadataBlockEx
   {
      dmsMetaExtent        _header ;
      dmsExtentID          _array[1] ;

      INT32 getFirstExtentID( UINT32 segID, dmsExtentID &extID )
      {
         if ( segID >= _header._segNum )
         {
            return SDB_INVALIDARG ;
         }
         UINT32 index = segID << 1 ;
         extID = _array[index] ;
         return SDB_OK ;
      }
      INT32 getLastExtentID( UINT32 segID, dmsExtentID &extID )
      {
         if ( segID >= _header._segNum )
         {
            return SDB_INVALIDARG ;
         }
         UINT32 index = ( segID << 1 ) + 1 ;
         extID = _array[index] ;
         return SDB_OK ;
      }
      INT32 setFirstExtentID( UINT32 segID, dmsExtentID extID )
      {
         if ( segID >= _header._segNum )
         {
            return SDB_INVALIDARG ;
         }
         UINT32 index = segID << 1 ;
         _array[index] = extID ;
         return SDB_OK ;
      }
      INT32 setLastExtentID( UINT32 segID, dmsExtentID extID )
      {
         if ( segID >= _header._segNum )
         {
            return SDB_INVALIDARG ;
         }
         UINT32 index = ( segID << 1 ) + 1 ;
         _array[index] = extID ;
         return SDB_OK ;
      }
   } ;
   typedef _metadataBlockEx   dmsMBEx ;

   #define DMS_MME_SZ               (DMS_MME_SLOTS*DMS_MB_SIZE)
   /*
      _dmsMetadataManagementExtent defined
   */
   struct _dmsMetadataManagementExtent : public SDBObject
   {
      dmsMetadataBlock  _mbList [ DMS_MME_SLOTS ] ;

      _dmsMetadataManagementExtent ()
      {
         SDB_ASSERT( DMS_MME_SZ == sizeof( _dmsMetadataManagementExtent ),
                     "MME size error" ) ;
         ossMemset( this, 0, sizeof( _dmsMetadataManagementExtent ) ) ;
      }
   } ;
   typedef _dmsMetadataManagementExtent dmsMetadataManagementExtent ;

   /*
      _dmsMBStatInfo define
   */
   struct _dmsMBStatInfo
   {
      UINT64      _totalRecords ;
      UINT32      _totalDataPages ;
      UINT32      _totalIndexPages ;
      UINT64      _totalDataFreeSpace ;
      UINT64      _totalIndexFreeSpace ;
      UINT32      _totalLobPages ;
      UINT32      _uniqueIdxNum ;

      void reset()
      {
         _totalRecords           = 0 ;
         _totalDataPages         = 0 ;
         _totalIndexPages        = 0 ;
         _totalDataFreeSpace     = 0 ;
         _totalIndexFreeSpace    = 0 ;
         _totalLobPages          = 0 ;
         _uniqueIdxNum           = 0 ;
      }
      _dmsMBStatInfo ()
      {
         reset() ;
      }
   } ;
   typedef _dmsMBStatInfo dmsMBStatInfo ;

   class _dmsStorageData ;
   /*
      _dmsMBContext define
   */
   class _dmsMBContext : public _dmsContext
   {
      friend class _dmsStorageData ;
      private:
         _dmsMBContext() ;
         virtual ~_dmsMBContext() ;
         void _reset () ;

      public:
         virtual string toString () const ;
         virtual INT32  pause () ;
         virtual INT32  resume () ;

         OSS_INLINE INT32   mbLock( INT32 lockType ) ;
         OSS_INLINE INT32   mbUnlock() ;
         OSS_INLINE BOOLEAN isMBLock( INT32 lockType ) const ;
         OSS_INLINE BOOLEAN isMBLock() const ;
         OSS_INLINE BOOLEAN canResume() const ;

         OSS_INLINE  UINT16 mbID () const { return _mbID ; }
         OSS_INLINE  dmsMB* mb () { return _mb ; }
         OSS_INLINE  dmsMBStatInfo* mbStat() { return _mbStat ; }
         OSS_INLINE  UINT32 clLID () const { return _clLID ; }

      private:
         dmsMB             *_mb ;
         dmsMBStatInfo     *_mbStat ;
         ossSpinSLatch     *_latch ;
         UINT32            _clLID ;
         UINT16            _mbID ;
         INT32             _mbLockType ;
         INT32             _resumeType ;

   };
   typedef _dmsMBContext   dmsMBContext ;

   /*
      _dmsMBContext OSS_INLINE functions
   */
   OSS_INLINE INT32 _dmsMBContext::mbLock( INT32 lockType )
   {
      INT32 rc = SDB_OK ;
      if ( SHARED != lockType && EXCLUSIVE != lockType )
      {
         return SDB_INVALIDARG ;
      }
      if ( _mbLockType == lockType )
      {
         return SDB_OK ;
      }
      if ( -1 != _mbLockType && SDB_OK != ( rc = pause() ) )
      {
         return rc ;
      }
      if ( !DMS_IS_MB_INUSE(_mb->_flag) || _clLID != _mb->_logicalID )
      {
         return SDB_DMS_NOTEXIST ;
      }
      ossLatch( _latch, (OSS_LATCH_MODE)lockType ) ;
      if ( !DMS_IS_MB_INUSE(_mb->_flag) || _clLID != _mb->_logicalID )
      {
         ossUnlatch( _latch, (OSS_LATCH_MODE)lockType ) ;
         return SDB_DMS_NOTEXIST ;
      }
      _mbLockType = lockType ;
      _resumeType = -1 ;
      return SDB_OK ;
   }
   OSS_INLINE INT32 _dmsMBContext::mbUnlock()
   {
      if ( SHARED == _mbLockType || EXCLUSIVE == _mbLockType )
      {
         ossUnlatch( _latch, (OSS_LATCH_MODE)_mbLockType ) ;
         _resumeType = _mbLockType ;
         _mbLockType = -1 ;
      }
      return SDB_OK ;
   }
   OSS_INLINE BOOLEAN _dmsMBContext::isMBLock( INT32 lockType ) const
   {
      return lockType == _mbLockType ? TRUE : FALSE ;
   }
   OSS_INLINE BOOLEAN _dmsMBContext::isMBLock() const
   {
      if ( SHARED == _mbLockType || EXCLUSIVE == _mbLockType )
      {
         return TRUE ;
      }
      return FALSE ;
   }
   OSS_INLINE BOOLEAN _dmsMBContext::canResume() const
   {
      if ( SHARED == _resumeType || EXCLUSIVE == _resumeType )
      {
         return TRUE ;
      }
      return FALSE ;
   }

   #define DMS_MME_OFFSET                 ( DMS_SME_OFFSET + DMS_SME_SZ )
   #define DMS_DATASU_EYECATCHER          "SDBDATA"
   #define DMS_DATASU_CUR_VERSION         1
   #define DMS_CONTEXT_MAX_SIZE           (2000)
   #define DMS_RECORDS_PER_EXTENT_SQUARE  4     // value is 2^4=16
   #define DMS_RECORD_OVERFLOW_RATIO      1.2f

   /*
      DMS TRUNCATE TYPE DEFINE
   */
   enum DMS_TRUNC_TYPE
   {
      DMS_TRUNC_LOAD    = 1,
      DMS_TRUNC_ALL     = 2
   } ;

   class _dmsStorageIndex ;
   class _dmsStorageLob ;
   class _dmsStorageUnit ;
   class _pmdEDUCB ;
   class _mthModifier ;

   /*
      _dmsStorageData defined
   */
   class _dmsStorageData : public _dmsStorageBase
   {
      friend class _dmsStorageIndex ;
      friend class _dmsStorageUnit ;
      friend class _dmsStorageLob ;

      struct cmp_str
      {
         bool operator() (const char *a, const char *b)
         {
            return std::strcmp( a, b ) < 0 ;
         }
      } ;

      typedef std::map<const CHAR*, UINT16, cmp_str>        COLNAME_MAP ;
#if defined (_WINDOWS)
      typedef COLNAME_MAP::iterator                         COLNAME_MAP_IT ;
      typedef COLNAME_MAP::const_iterator                   COLNAME_MAP_CIT ;
#else
      typedef std::map<const CHAR*, UINT16>::iterator       COLNAME_MAP_IT ;
      typedef std::map<const CHAR*, UINT16>::const_iterator COLNAME_MAP_CIT ;
#endif

      public:
         _dmsStorageData ( const CHAR *pSuFileName, dmsStorageInfo *pInfo ) ;
         ~_dmsStorageData () ;

         void  syncMemToMmap () ;

         OSS_INLINE ossValuePtr   recordAddr ( const dmsRecordID &record ) ;

         UINT32 logicalID () const { return _logicalCSID ; }
         dmsStorageUnitID CSID () const { return _CSID ; }

         OSS_INLINE INT32  getMBContext( dmsMBContext **pContext, UINT16 mbID,
                                         UINT32 clLID, INT32 lockType = -1 );
         OSS_INLINE INT32  getMBContext( dmsMBContext **pContext,
                                         const CHAR* pName,
                                         INT32 lockType = -1 ) ;
         OSS_INLINE void   releaseMBContext( dmsMBContext *&pContext ) ;

         OSS_INLINE const dmsMBStatInfo* getMBStatInfo( UINT16 mbID ) const ;

         OSS_INLINE UINT32 getCollectionNum() ;

         INT32         addExtent2Meta( dmsExtentID extID, dmsExtent *extent,
                                       dmsMBContext *context ) ;

         OSS_INLINE void   updateCreateLobs( UINT32 createLobs ) ;

      public:

         INT32 addCollection ( const CHAR *pName,
                               UINT16 *collectionID,
                               UINT32 attributes = 0,
                               _pmdEDUCB * cb = NULL,
                               SDB_DPSCB *dpscb = NULL,
                               UINT16 initPages = 0,
                               BOOLEAN sysCollection = FALSE,
                               BOOLEAN noIDIndex = FALSE ) ;

         INT32 dropCollection ( const CHAR *pName,
                                _pmdEDUCB *cb,
                                SDB_DPSCB *dpscb,
                                BOOLEAN sysCollection = TRUE,
                                dmsMBContext *context = NULL ) ;

         INT32 truncateCollection ( const CHAR *pName,
                                    _pmdEDUCB *cb,
                                    SDB_DPSCB *dpscb,
                                    BOOLEAN sysCollection = TRUE,
                                    dmsMBContext *context = NULL,
                                    BOOLEAN needChangeCLID = TRUE ) ;

         INT32 truncateCollectionLoads( const CHAR *pName,
                                        dmsMBContext *context = NULL ) ;

         INT32 renameCollection ( const CHAR *oldName, const CHAR *newName,
                                  _pmdEDUCB *cb, SDB_DPSCB *dpscb,
                                  BOOLEAN sysCollection = FALSE ) ;

         INT32 findCollection ( const CHAR *pName, UINT16 &collectionID ) ;

         INT32 insertRecord ( dmsMBContext *context, const BSONObj &record,
                              _pmdEDUCB *cb, SDB_DPSCB *dpscb,
                              BOOLEAN mustOID = TRUE,
                              BOOLEAN canUnLock = TRUE ) ;

         INT32 deleteRecord ( dmsMBContext *context,
                              const dmsRecordID &recordID,
                              ossValuePtr deletedDataPtr,
                              _pmdEDUCB * cb,
                              SDB_DPSCB *dpscb ) ;

         INT32 updateRecord ( dmsMBContext *context,
                              const dmsRecordID &recordID,
                              ossValuePtr updatedDataPtr,
                              _pmdEDUCB *cb,
                              SDB_DPSCB *dpscb,
                              _mthModifier &modifier ) ;

         INT32 fetch ( dmsMBContext *context,
                       const dmsRecordID &recordID,
                       BSONObj &dataRecord,
                       _pmdEDUCB *cb,
                       BOOLEAN dataOwned = FALSE ) ;

      private:
         virtual UINT64 _dataOffset() ;
         virtual const CHAR* _getEyeCatcher() const ;
         virtual UINT32 _curVersion() const ;
         virtual INT32  _checkVersion( dmsStorageUnitHeader *pHeader ) ;
         virtual INT32  _onCreate( OSSFILE *file, UINT64 curOffSet ) ;
         virtual INT32  _onMapMeta( UINT64 curOffSet ) ;
         virtual void   _onClosed() ;

      protected:
         OSS_INLINE const CHAR*   _clFullName ( const CHAR *clName,
                                            CHAR *clFullName,
                                            UINT32 fullNameLen ) ;

         void                 _attach ( _dmsStorageIndex *pIndexSu ) ;
         void                 _detach () ;

         void                 _attachLob( _dmsStorageLob *pLobSu ) ;
         void                 _detachLob() ;

      private:
         void           _initializeMME () ;

         OSS_INLINE void    _collectionNameInsert ( const CHAR *pName,
                                                    UINT16 mbID ) ;
         OSS_INLINE UINT16  _collectionNameLookup ( const CHAR *pName ) ;
         OSS_INLINE void    _collectionNameRemove ( const CHAR *pName ) ;
         OSS_INLINE void    _collectionNameMapCleanup () ;

         INT32          _logDPS( SDB_DPSCB *dpsCB, dpsMergeInfo &info,
                                 _pmdEDUCB * cb, ossSLatch *pLatch,
                                 OSS_LATCH_MODE mode, BOOLEAN &locked,
                                 UINT32 clLID, dmsExtentID extLID ) ;

         INT32          _logDPS( SDB_DPSCB *dpsCB, dpsMergeInfo &info,
                                 _pmdEDUCB *cb, dmsMBContext *context,
                                 dmsExtentID extLID, BOOLEAN needUnLock ) ;

      private:
         INT32          _saveDeletedRecord ( dmsMB *mb,
                                             const dmsRecordID &recordID,
                                             INT32 recordSize = 0 ) ;
         INT32          _saveDeletedRecord ( dmsMB *mb, dmsExtent *extAddr,
                                             dmsOffset offset, INT32 recordSize,
                                             INT32 extentID ) ;
         void           _mapExtent2DelList ( dmsMB *mb, dmsExtent *extAddr,
                                             SINT32 extentID ) ;

         INT32          _allocateExtent ( dmsMBContext *context,
                                          UINT16 numPages,
                                          BOOLEAN map2DelList = TRUE,
                                          BOOLEAN add2LoadList = FALSE,
                                          dmsExtentID *allocExtID = NULL ) ;

         INT32          _freeExtent ( dmsExtentID extentID ) ;

         INT32          _reserveFromDeleteList ( dmsMBContext *context,
                                                 UINT32 requiredSize,
                                                 dmsRecordID &resultID,
                                                 _pmdEDUCB *cb ) ;

         INT32          _truncateCollection ( dmsMBContext *context ) ;

         INT32          _truncateCollectionLoads( dmsMBContext *context ) ;

         INT32          _extentInsertRecord ( dmsMBContext *context,
                                              ossValuePtr deletedRecordPtr,
                                              UINT32 dmsRecordSize,
                                              ossValuePtr ptr,
                                              INT32 len,
                                              INT32 extentID,
                                              BSONElement *extraOID,
                                              _pmdEDUCB *cb,
                                              BOOLEAN compressed,
                                              BOOLEAN addIntoList = TRUE ) ;

         INT32          _extentRemoveRecord ( dmsMB *mb,
                                              const dmsRecordID &recordID,
                                              INT32 recordSize,
                                              _pmdEDUCB *cb,
                                              BOOLEAN decCount = TRUE ) ;

         INT32          _extentUpdatedRecord ( dmsMBContext *context,
                                               const dmsRecordID &recordID,
                                               ossValuePtr recordDataPtr,
                                               dmsExtentID extLID,
                                               ossValuePtr ptr,
                                               INT32 len,
                                               _pmdEDUCB *cb ) ;

         OSS_INLINE UINT32  _getFactor () const ;

      private:
         dmsMetadataManagementExtent         *_dmsMME ;     // 4MB

         ossSpinSLatch                       _mblock [ DMS_MME_SLOTS ] ;
         dmsMBStatInfo                       _mbStatInfo [ DMS_MME_SLOTS ] ;
         ossSpinSLatch                       _metadataLatch ;
         COLNAME_MAP                         _collectionNameMap ;
         UINT32                              _logicalCSID ;
         dmsStorageUnitID                    _CSID ;

         vector<dmsMBContext*>               _vecContext ;
         ossSpinXLatch                       _latchContext ;

         _dmsStorageIndex                    *_pIdxSU ;
         _dmsStorageLob                      *_pLobSU ;

   };
   typedef _dmsStorageData dmsStorageData ;

   /*
      OSS_INLINE functions :
   */
   OSS_INLINE void _dmsStorageData::_collectionNameInsert( const CHAR * pName,
                                                           UINT16 mbID )
   {
      _collectionNameMap[ ossStrdup( pName ) ] = mbID ;
   }
   OSS_INLINE UINT16 _dmsStorageData::_collectionNameLookup( const CHAR * pName )
   {
      COLNAME_MAP_CIT it = _collectionNameMap.find( pName ) ;
      if ( _collectionNameMap.end() == it  )
      {
         return DMS_INVALID_MBID ;
      }
      return (*it).second ;
   }
   OSS_INLINE void _dmsStorageData::_collectionNameRemove( const CHAR * pName )
   {
      COLNAME_MAP_IT it = _collectionNameMap.find( pName ) ;
      if ( _collectionNameMap.end() != it )
      {
         const CHAR *tp = (*it).first ;
         _collectionNameMap.erase( it ) ;
         SDB_OSS_FREE( const_cast<CHAR *>(tp) ) ;
      }
   }
   OSS_INLINE void _dmsStorageData::_collectionNameMapCleanup ()
   {
      COLNAME_MAP_CIT it = _collectionNameMap.begin() ;

      for ( ; it != _collectionNameMap.end() ; ++it )
      {
         SDB_OSS_FREE( const_cast<CHAR *>(it->first) ) ;
      }
      _collectionNameMap.clear() ;
   }
   OSS_INLINE UINT32 _dmsStorageData::getCollectionNum()
   {
      ossScopedLock lock( &_metadataLatch, SHARED ) ;
      return (UINT32)_collectionNameMap.size() ;
   }
   OSS_INLINE ossValuePtr _dmsStorageData::recordAddr( const dmsRecordID &record )
   {
      ossValuePtr extPtr = extentAddr( record._extent ) ;
      if ( 0 != extPtr )
      {
         extPtr += record._offset ;
      }
      return extPtr ;
   }
   OSS_INLINE const CHAR* _dmsStorageData::_clFullName( const CHAR *clName,
                                                        CHAR * clFullName,
                                                        UINT32 fullNameLen )
   {
      SDB_ASSERT( fullNameLen > DMS_COLLECTION_FULL_NAME_SZ,
                  "Collection full name len error" ) ;
      ossStrncat( clFullName, getSuName(), DMS_COLLECTION_SPACE_NAME_SZ ) ;
      ossStrncat( clFullName, ".", 1 ) ;
      ossStrncat( clFullName, clName, DMS_COLLECTION_NAME_SZ ) ;
      clFullName[ DMS_COLLECTION_FULL_NAME_SZ ] = 0 ;

      return clFullName ;
   }
   OSS_INLINE void _dmsStorageData::updateCreateLobs( UINT32 createLobs )
   {
      if ( _dmsHeader && _dmsHeader->_createLobs != createLobs )
      {
         _dmsHeader->_createLobs = createLobs ;
      }
   }
   OSS_INLINE INT32 _dmsStorageData::getMBContext( dmsMBContext ** pContext,
                                                   UINT16 mbID, UINT32 clLID,
                                                   INT32 lockType )
   {
      if ( mbID >= DMS_MME_SLOTS )
      {
         return SDB_INVALIDARG ;
      }

      if ( (UINT32)DMS_INVALID_CLID == clLID )
      {
         _metadataLatch.get_shared() ;
         clLID = _dmsMME->_mbList[mbID]._logicalID ;
         _metadataLatch.release_shared() ;
      }

      _latchContext.get() ;
      if ( _vecContext.size () > 0 )
      {
         *pContext = _vecContext.back () ;
         _vecContext.pop_back () ;
      }
      else
      {
         *pContext = SDB_OSS_NEW dmsMBContext ;
      }
      _latchContext.release() ;

      if ( !(*pContext) )
      {
         return SDB_OOM ;
      }
      (*pContext)->_clLID = clLID ;
      (*pContext)->_mbID = mbID ;
      (*pContext)->_mb = &_dmsMME->_mbList[mbID] ;
      (*pContext)->_mbStat = &_mbStatInfo[mbID] ;
      (*pContext)->_latch = &_mblock[mbID] ;
      if ( SHARED == lockType || EXCLUSIVE == lockType )
      {
         return (*pContext)->mbLock( lockType ) ;
      }
      return SDB_OK ;
   }
   OSS_INLINE INT32 _dmsStorageData::getMBContext( dmsMBContext ** pContext,
                                                   const CHAR * pName,
                                                   INT32 lockType )
   {
      UINT16 mbID = DMS_INVALID_MBID ;
      UINT32 clLID = DMS_INVALID_CLID ;

      _metadataLatch.get_shared() ;
      mbID = _collectionNameLookup( pName ) ;
      if ( DMS_INVALID_MBID != mbID )
      {
         clLID = _dmsMME->_mbList[mbID]._logicalID ;
      }
      _metadataLatch.release_shared() ;

      if ( DMS_INVALID_MBID == mbID )
      {
         return SDB_DMS_NOTEXIST ;
      }
      return getMBContext( pContext, mbID, clLID, lockType ) ;
   }
   OSS_INLINE void _dmsStorageData::releaseMBContext( dmsMBContext *&pContext )
   {
      if ( !pContext )
      {
         return ;
      }
      pContext->mbUnlock() ;

      _latchContext.get() ;
      if ( _vecContext.size() < DMS_CONTEXT_MAX_SIZE )
      {
         pContext->_reset() ;
         _vecContext.push_back( pContext ) ;
      }
      else
      {
         SDB_OSS_DEL pContext ;
      }
      _latchContext.release() ;
      pContext = NULL ;
   }
   OSS_INLINE const dmsMBStatInfo* _dmsStorageData::getMBStatInfo( UINT16 mbID ) const
   {
      if ( mbID >= DMS_MME_SLOTS )
      {
         return NULL ;
      }
      return &_mbStatInfo[ mbID ] ;
   }
   OSS_INLINE UINT32 _dmsStorageData::_getFactor() const
   {
      return 16 + 14 - pageSizeSquareRoot() ;
   }

}

#endif //DMSSTORAGE_DATA_HPP_

