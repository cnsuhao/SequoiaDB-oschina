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

   Source File Name = dmsStorageIndex.hpp

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
#ifndef DMSSTORAGE_INDEX_HPP_
#define DMSSTORAGE_INDEX_HPP_

#include "dmsStorageBase.hpp"
#include "dpsLogWrapper.hpp"

using namespace bson ;

namespace engine
{

   class _dmsMBContext ;
   class _dmsStorageData ;
   class _pmdEDUCB ;
   class _ixmIndexCB ;
   class _dmsMBContext ;

   #define DMS_INDEXSU_EYECATCHER         "SDBIDX"
   #define DMS_INDEXSU_CUR_VERSION        1

   /*
      _dmsStorageIndex defined
   */
   class _dmsStorageIndex : public _dmsStorageBase
   {
      public:
         _dmsStorageIndex ( const CHAR *pSuFileName, dmsStorageInfo *pInfo,
                            _dmsStorageData *pDataSu ) ;
         ~_dmsStorageIndex () ;

      public:
         INT32    reserveExtent ( UINT16 mbID, dmsExtentID &extentID,
                                  dmsContext *context ) ;
         INT32    releaseExtent ( dmsExtentID extentID ) ;

         INT32    createIndex ( _dmsMBContext *context, const BSONObj &index,
                                _pmdEDUCB *cb, SDB_DPSCB *dpscb,
                                BOOLEAN isSys = FALSE ) ;

         INT32    dropIndex ( _dmsMBContext *context, OID &indexOID,
                              _pmdEDUCB *cb, SDB_DPSCB *dpscb,
                              BOOLEAN isSys = FALSE ) ;

         INT32    dropIndex ( _dmsMBContext *context, const CHAR *indexName,
                              _pmdEDUCB *cb, SDB_DPSCB *dpscb,
                              BOOLEAN isSys = FALSE ) ;

         INT32    dropAllIndexes( _dmsMBContext *context, _pmdEDUCB *cb,
                                  SDB_DPSCB *dpscb ) ;

         INT32    dropIndex ( _dmsMBContext *context, INT32 indexID,
                              dmsExtentID indexLID, _pmdEDUCB *cb,
                              SDB_DPSCB *dpscb, BOOLEAN isSys = FALSE ) ;

         INT32    rebuildIndexes ( _dmsMBContext *context, _pmdEDUCB *cb ) ;

         INT32    indexesInsert ( _dmsMBContext *context, dmsExtentID extLID,
                                  BSONObj &inputObj, const dmsRecordID &rid,
                                  _pmdEDUCB *cb ) ;

         INT32    indexesUpdate ( _dmsMBContext *context, dmsExtentID extLID,
                                  BSONObj &originalObj, BSONObj &newObj,
                                  const dmsRecordID &rid, _pmdEDUCB *cb,
                                  BOOLEAN isRollback ) ;

         INT32    indexesDelete ( _dmsMBContext *context, dmsExtentID extLID,
                                  BSONObj &inputObj, const dmsRecordID &rid,
                                  _pmdEDUCB *cb ) ;

         INT32    truncateIndexes ( _dmsMBContext *context ) ;

         INT32    getIndexCBExtent ( _dmsMBContext *context,
                                     const CHAR *indexName,
                                     dmsExtentID &indexExtent ) ;

         INT32    getIndexCBExtent ( _dmsMBContext *context,
                                     const OID &indexOID,
                                     dmsExtentID &indexExtent ) ;

         INT32    getIndexCBExtent ( _dmsMBContext *context,
                                     INT32 indexID,
                                     dmsExtentID &indexExtent ) ;

         void     addStatFreeSpace ( UINT16 mbID, UINT16 size ) ;
         void     decStatFreeSpace ( UINT16 mbID, UINT16 size ) ;

      private:

         INT32    _rebuildIndex ( _dmsMBContext *context, INT32 indexID,
                                  dmsExtentID indexLID, _pmdEDUCB * cb ) ;

         INT32    _indexInsert ( _dmsMBContext *context, _ixmIndexCB *indexCB,
                                 BSONObj &inputObj, const dmsRecordID &rid,
                                 _pmdEDUCB *cb, BOOLEAN dupAllowed,
                                 BOOLEAN dropDups ) ;

         INT32    _indexUpdate ( _dmsMBContext *context, _ixmIndexCB *indexCB,
                                 BSONObj &originalObj, BSONObj &newObj,
                                 const dmsRecordID &rid, _pmdEDUCB *cb,
                                 BOOLEAN isRollback ) ;

         INT32    _indexDelete ( _dmsMBContext *context, _ixmIndexCB *indexCB,
                                 BSONObj &inputObj, const dmsRecordID &rid,
                                 _pmdEDUCB *cb ) ;

      private:
         virtual UINT64 _dataOffset() ;
         virtual const CHAR* _getEyeCatcher() const ;
         virtual UINT32 _curVersion() const ;
         virtual INT32  _checkVersion( dmsStorageUnitHeader *pHeader ) ;
         virtual INT32  _onCreate( OSSFILE *file, UINT64 curOffSet ) ;
         virtual INT32  _onMapMeta( UINT64 curOffSet ) ;
         virtual void   _onOpened() ;
         virtual void   _onClosed() ;
         virtual BOOLEAN _keepInRam()const
         {
            return TRUE ;
         }

      protected:

      private:
         _dmsStorageData               *_pDataSu ;

   };
   typedef _dmsStorageIndex dmsStorageIndex ;

}

#endif //DMSSTORAGE_INDEX_HPP_

