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

   Source File Name = rtnIXScanner.hpp

   Descriptive Name = RunTime Index Scanner Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains structure for index
   scanner, which is used to traverse index tree.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef RTNIXSCANNER_HPP__
#define RTNIXSCANNER_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "ixm.hpp"
#include "ixmExtent.hpp"
#include "rtnPredicate.hpp"
#include "pmdEDU.hpp"
#include "../bson/ordering.h"
#include "../bson/oid.h"
#include "dms.hpp"
#include "monCB.hpp"
#include <set>

namespace engine
{
   class _dmsStorageUnit ;

   #define RTN_IXSCANNER_DEDUPBUFSZ_DFT      (1024*1024)

   /*
      _rtnIXScanner define
   */
   class _rtnIXScanner : public SDBObject
   {
   private :
      ixmIndexCB *_indexCB ;
      rtnPredicateList *_predList ;
      rtnPredicateListIterator _listIterator ;
      Ordering _order ;
      _dmsStorageUnit *_su ;
      monContextCB *_pMonCtxCB ;
      _pmdEDUCB *_cb ;
      INT32 _direction ;

      ixmRecordID _curIndexRID ;
      BOOLEAN _init ;
      // need to store a duplicate buffer for dmsRecordID for each ixscan,
      // because each record may be refered by more than one index key (say
      // {c1:[1,2,3,4,5]}, there will be 5 index keys pointing to the same
      // record), so index scan may return the same record more than one time,
      // which should be avoided. Thus we keep a set ( maybe we can further
      // improve performance by customize data structure ) to record id that
      // already scanned.
      // note that a record id will not be changed during update because it will
      // use overflowed record with existing record head
      UINT64 _dedupBufferSize ;
      set<dmsRecordID> _dupBuffer ;

      // after the caller release X latch, another session may change the index
      // structure. So everytime before releasing X latch, the caller must store
      // the current obj, and then verify if it's still remaining the same after
      // next check. If the object doens't match the on-disk obj, something must
      // goes changed and we should relocate
      BSONObj _savedObj ;
      dmsRecordID _savedRID ;

      BSONObj _curKeyObj ;

      OID _indexOID ;
      dmsExtentID _indexCBExtent ;
      dmsExtentID _indexLID ;

   public :
      _rtnIXScanner ( ixmIndexCB *indexCB, rtnPredicateList *predList,
                      _dmsStorageUnit *su, _pmdEDUCB *cb ) ;
      ~_rtnIXScanner () ;

      void setMonCtxCB ( monContextCB *monCtxCB )
      {
         _pMonCtxCB = monCtxCB ;
      }

      BSONObj getSavedObj () const
      {
         return _savedObj ;
      }

      dmsRecordID getSavedRID () const
      {
         return _savedRID ;
      }

      ixmRecordID getCurIndexRID () const
      {
         return _curIndexRID ;
      }

      const BSONObj& getCurKeyObj () const
      {
         return _curKeyObj ;
      }

      INT32 getDirection () const
      {
         return _direction ;
      }

      rtnPredicateList* getPredicateList ()
      {
         return _predList ;
      }

      INT32 compareWithCurKeyObj ( const BSONObj &keyObj ) const
      {
         return _curKeyObj.woCompare( keyObj, _order, false ) * _direction ;
      }

      void reset()
      {
         _curIndexRID.reset() ;
         _listIterator.reset() ;
         _dupBuffer.clear() ;
         _init    = FALSE ;
      }
      // save the bson key for the current index rid, before releasing X latch
      // on the collection
      // we have to call resumeScan after getting X latch again just in case
      // other sessions changed tree structure
      INT32 pauseScan( BOOLEAN isReadOnly = TRUE ) ;
      // restoring the bson key and rid for the current index rid. This is done
      // by comparing the current key/rid pointed by _curIndexRID still same as
      // previous saved one. If so it means there's no change in this key, and
      // we can move on the from the current index rid. Otherwise we have to
      // locate the new position for the saved key+rid
      INT32 resumeScan( BOOLEAN isReadOnly = TRUE ) ;

      // return SDB_IXM_EOC for end of index
      // otherwise rid is set to dmsRecordID
      INT32 advance ( dmsRecordID &rid, BOOLEAN isReadOnly = TRUE ) ;
      INT32 relocateRID () ;
      INT32 relocateRID ( const BSONObj &keyObj, const dmsRecordID &rid ) ;
   } ;
   typedef class _rtnIXScanner rtnIXScanner ;

}

#endif //RTNIXSCANNER_HPP__

