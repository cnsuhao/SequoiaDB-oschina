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

   Source File Name = dmsScanner.hpp

   Descriptive Name = Data Management Service Storage Unit Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          22/08/2013  XJH Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef DMSSCANNER_HPP__
#define DMSSCANNER_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "dms.hpp"
#include "dmsExtent.hpp"
#include "ossUtil.hpp"
#include "ossMem.hpp"
#include "dmsStorageBase.hpp"
#include "../bson/bson.h"
#include "../bson/bsonobj.h"

using namespace bson ;

namespace engine
{

   class _dmsMBContext ;
   class _dmsStorageData ;
   class _mthMatcher ;
   class _rtnIXScanner ;
   class _pmdEDUCB ;
   class _monAppCB ;
   class dpsTransCB ;

   /*
      _dmsScanner define
   */
   class _dmsScanner : public SDBObject
   {
      public:
         _dmsScanner ( _dmsStorageData *su, _dmsMBContext *context,
                       _mthMatcher *match,
                       DMS_ACCESS_TYPE accessType = DMS_ACCESS_TYPE_FETCH ) ;
         virtual ~_dmsScanner () ;

      public:
         virtual INT32 advance ( dmsRecordID &recordID,
                                 ossValuePtr &recordDataPtr,
                                 _pmdEDUCB *cb,
                                 vector<INT64> *dollarList = NULL ) = 0 ;
         virtual void  stop () = 0 ;

      protected:
         _dmsStorageData         *_pSu ;
         _dmsMBContext           *_context ;
         _mthMatcher             *_match ;
         DMS_ACCESS_TYPE         _accessType ;

   } ;
   typedef _dmsScanner dmsScanner ;

   class _dmsTBScanner ;
   /*
      _dmsExtScanner define
   */
   class _dmsExtScanner : public _dmsScanner
   {
      friend class _dmsTBScanner ;
      public:
         _dmsExtScanner ( _dmsStorageData *su, _dmsMBContext *context,
                          _mthMatcher *match, dmsExtentID curExtentID,
                          DMS_ACCESS_TYPE accessType = DMS_ACCESS_TYPE_FETCH,
                          INT64 maxRecords = -1, INT64 skipNum = 0 ) ;
         virtual ~_dmsExtScanner () ;

         dmsExtent* curExtent () { return _extent ; }
         dmsExtentID nextExtentID () const ;
         INT32 stepToNextExtent() ;
         INT64 getMaxRecords() const { return _maxRecords ; }
         INT64 getSkipNum () const { return _skipNum ; }

      public:
         virtual INT32 advance ( dmsRecordID &recordID,
                                 ossValuePtr &recordDataPtr,
                                 _pmdEDUCB *cb,
                                 vector<INT64> *dollarList = NULL ) ;
         virtual void  stop () ;

      protected:
         INT32 _firstInit( _pmdEDUCB *cb ) ;

      private:
         INT64                _maxRecords ;
         INT64                _skipNum ;
         dmsExtent            *_extent ;
         dmsRecordID          _curRID ;
         ossValuePtr          _curRecordPtr ;
         dmsOffset            _next ;
         dmsRecordID          _ovfRID ;
         BOOLEAN              _firstRun ;
         _monAppCB            *_pMonAppCB ;
         dpsTransCB           *_pTransCB ;
         BOOLEAN              _recordXLock ;
         BOOLEAN              _needUnLock ;
         _pmdEDUCB            *_cb ;

   };
   typedef _dmsExtScanner dmsExtScanner ;

   /*
      _dmsTBScanner define
   */
   class _dmsTBScanner : public _dmsScanner
   {
      public:
         _dmsTBScanner ( _dmsStorageData *su, _dmsMBContext *context,
                         _mthMatcher *match,
                         DMS_ACCESS_TYPE accessType = DMS_ACCESS_TYPE_FETCH,
                         INT64 maxRecords = -1, INT64 skipNum = 0 ) ;
         ~_dmsTBScanner () ;

      public:
         virtual INT32 advance ( dmsRecordID &recordID,
                                 ossValuePtr &recordDataPtr,
                                 _pmdEDUCB *cb,
                                 vector<INT64> *dollarList = NULL ) ;
         virtual void  stop () ;

      protected:
         void  _resetExtScanner() ;
         INT32 _firstInit() ;

      private:
         dmsExtScanner              _extScanner ;
         dmsExtentID                _curExtentID ;
         BOOLEAN                    _firstRun ;

   };
   typedef _dmsTBScanner dmsTBScanner ;

   class _dmsIXScanner ;
   /*
      _dmsIXSecScanner define
   */
   class _dmsIXSecScanner : public _dmsScanner
   {
      friend class _dmsIXScanner ;
      public:
         _dmsIXSecScanner ( _dmsStorageData *su, _dmsMBContext *context,
                            _mthMatcher *match, _rtnIXScanner *scanner,
                            DMS_ACCESS_TYPE accessType = DMS_ACCESS_TYPE_FETCH,
                            INT64 maxRecords = -1, INT64 skipNum = 0 ) ;
         virtual ~_dmsIXSecScanner () ;

         void  enableIndexBlockScan( const BSONObj &startKey,
                                     const BSONObj &endKey,
                                     const dmsRecordID &startRID,
                                     const dmsRecordID &endRID,
                                     INT32 direction ) ;

         INT64 getMaxRecords() const { return _maxRecords ; }
         INT64 getSkipNum () const { return _skipNum ; }

         BOOLEAN eof () const { return _eof ; }

      public:
         virtual INT32 advance ( dmsRecordID &recordID,
                                 ossValuePtr &recordDataPtr,
                                 _pmdEDUCB *cb,
                                 vector<INT64> *dollarList = NULL ) ;
         virtual void  stop () ;

      protected:
         INT32 _firstInit( _pmdEDUCB *cb ) ;
         BSONObj* _getStartKey () ;
         BSONObj* _getEndKey () ;
         dmsRecordID* _getStartRID () ;
         dmsRecordID* _getEndRID () ;

      private:
         INT64                _maxRecords ;
         INT64                _skipNum ;
         dmsRecordID          _curRID ;
         ossValuePtr          _curRecordPtr ;
         dmsRecordID          _ovfRID ;
         BOOLEAN              _firstRun ;
         _monAppCB            *_pMonAppCB ;
         dpsTransCB           *_pTransCB ;
         BOOLEAN              _recordXLock ;
         BOOLEAN              _needUnLock ;
         _pmdEDUCB            *_cb ;
         _rtnIXScanner        *_scanner ;
         INT64                _onceRestNum ;
         BOOLEAN              _eof ;

         BSONObj              _startKey ;
         BSONObj              _endKey ;
         dmsRecordID          _startRID ;
         dmsRecordID          _endRID ;
         BOOLEAN              _indexBlockScan ;
         INT32                _blockScanDir ;
         BOOLEAN              _judgeStartKey ;
         BOOLEAN              _includeStartKey ;
         BOOLEAN              _includeEndKey ;

   } ;
   typedef _dmsIXSecScanner dmsIXSecScanner ;

   /*
      _dmsIXScanner define
   */
   class _dmsIXScanner : public _dmsScanner
   {
      public:
         _dmsIXScanner ( _dmsStorageData *su, _dmsMBContext *context,
                         _mthMatcher *match, _rtnIXScanner *scanner,
                         BOOLEAN ownedScanner = FALSE,
                         DMS_ACCESS_TYPE accessType = DMS_ACCESS_TYPE_FETCH,
                         INT64 maxRecords = -1, INT64 skipNum = 0 ) ;
         ~_dmsIXScanner () ;

         _rtnIXScanner* getScanner () { return _scanner ; }

      public:
         virtual INT32 advance ( dmsRecordID &recordID,
                                 ossValuePtr &recordDataPtr,
                                 _pmdEDUCB *cb,
                                 vector<INT64> *dollarList = NULL ) ;
         virtual void  stop () ;

      protected:
         void  _resetIXSecScanner() ;
         INT32 _firstInit() ;

      private:
         dmsIXSecScanner            _secScanner ;
         _rtnIXScanner              *_scanner ;
         BOOLEAN                    _firstRun ;
         BOOLEAN                    _eof ;
         BOOLEAN                    _ownedScanner ;

   } ;
   typedef _dmsIXScanner dmsIXScanner ;

   /*
      _dmsExtentItr define
   */
   class _dmsExtentItr : public SDBObject
   {
      public:
         _dmsExtentItr ( _dmsStorageData *su, _dmsMBContext *context,
                         DMS_ACCESS_TYPE accessType = DMS_ACCESS_TYPE_QUERY ) ;
         ~_dmsExtentItr () ;

      public:
         INT32    next ( dmsExtent **ppExtent, _pmdEDUCB *cb ) ;

      private:
         _dmsStorageData            *_pSu ;
         _dmsMBContext              *_context ;
         dmsExtent                  *_curExtent ;
         ossValuePtr                _curExtAddr ;
         DMS_ACCESS_TYPE            _accessType ;
         UINT32                     _extentCount ;

   } ;
   typedef _dmsExtentItr dmsExtentItr ;

}

#endif //DMSSCANNER_HPP__

