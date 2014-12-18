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

   Source File Name = ixmExtent.hpp

   Descriptive Name = Index Management Extent Header

   When/how to use: this program may be used on binary and text-formatted
   versions of index management component. This file contains structure for
   index extent and its methods. The B Tree Insert/Delete/Update methods are
   also defined in this file.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef IXMEXTENT_HPP_
#define IXMEXTENT_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "dms.hpp"
#include "ixm.hpp"
#include "ixmKey.hpp"
#include "pd.hpp"
#include "pmdEDU.hpp"

namespace engine
{
   class ixmIndexInsertRequestImpl ;
   class _dmsStorageIndex ;

   /*
      _ixmKeyNode define
   */
   class _ixmKeyNode : public SDBObject
   {
   public :
      dmsExtentID _left ;
      dmsRecordID _rid ;
      UINT16      _keyOffset ;
      UINT16      _pad ;

      UINT16 keyDataOffset () const
      {
         return _keyOffset ;
      }
      void setKeyDataOffset ( UINT16 keyoff )
      {
         _keyOffset = keyoff ;
      }
      void setUnused()
      {
         _rid._offset |= 1 ;
      }
      void setUsed()
      {
         _rid._offset &= ~1 ;
      }
      BOOLEAN isUnused() const
      {
         return _rid._offset & 1 ;
      }
      BOOLEAN isUsed() const
      {
         return !isUnused() ;
      }
   } ;
   typedef class _ixmKeyNode ixmKeyNode ;

   /*
      IXM EXTENT HEAD EYE CATCHER DEFINE
   */
   #define IXM_EXTENT_EYECATCHER0      'I'
   #define IXM_EXTENT_EYECATCHER1      'E'

   /*
      IXM EXTENT HEAD VERSION DEFINE
   */
   #define IXM_EXTENT_VERSION_V0       0
   #define IXM_EXTENT_CURRENT_V        IXM_EXTENT_VERSION_V0

   /*
      _ixmExtentHead define
   */
   struct _ixmExtentHead : public SDBObject
   {
      CHAR        _eyeCatcher [2] ;
      UINT16      _totalKeyNodeNum ;
      UINT16      _mbID ;
      CHAR        _flag ;
      CHAR        _version ;
      dmsExtentID _parentExtentID ;
      ixmOffset   _beginFreeOffset ;
      UINT16      _totalFreeSize ;
      dmsExtentID _right ;
   } ;
   typedef struct _ixmExtentHead ixmExtentHead ;

   /*
      _ixmExtent define
   */
   class _ixmExtent : public SDBObject
   {
   protected:
      ixmExtentHead     *_extentHead ;
      dmsExtentID       _me ;
      _dmsStorageIndex  *_pIndexSu ;
      INT32             _pageSize ;

      INT32 _reorg (const Ordering &order, UINT16 &newPos) ;
      INT32 _reorg (const Ordering &order) ;
      INT32 _alloc ( INT32 requestSpace, UINT16 &beginOffset ) ;

      INT32 _splitPos ( UINT16 pos, UINT16 &splitPos ) ;
      INT32 _basicInsert ( UINT16 &pos, const dmsRecordID &rid,
                           const ixmKey &key, const Ordering &order ) ;
      INT32 _split ( UINT16 pos, const dmsRecordID &rid,
                     const ixmKey &key, const Ordering &order,
                     const dmsExtentID lchild, const dmsExtentID rchild,
                     ixmIndexCB *indexCB ) ;
      enum _ixmExtentValidateLevel
      {
         NONE = 0,
         MIN,
         MID,
         MAX
      } ;
      INT32 _validate ( _ixmExtentValidateLevel level,
                        const Ordering &order ) ;
      INT32 _pushBack ( const dmsRecordID &rid, const ixmKey &key,
                        const Ordering &order, const dmsExtentID left ) ;
      INT32 _fixParentPtrs ( UINT16 startPos, UINT16 stopPos ) ;
      void _assignRight ( const dmsExtentID right ) ;
      INT32 _truncate ( UINT16 totalNodes,UINT16 &newPos,const Ordering &order);
      INT32 _insert ( const dmsRecordID &rid, const ixmKey &key,
                      const Ordering &order, BOOLEAN dupAllowed,
                      dmsExtentID lchild, dmsExtentID rchild,
                      ixmIndexCB *indexCB ) ;
      INT32 _delKeyAtPos ( UINT16 pos ) ;
      INT32 _delKeyAtPos ( UINT16 pos, const Ordering &order,
                           ixmIndexCB *indexCB ) ;
      INT32 _mayBalanceWithNeighbors ( const Ordering &order,
                                       ixmIndexCB *indexCB,
                                       BOOLEAN &result ) ;
      INT32 _delExtent ( ixmIndexCB *indexCB ) ;
      INT32 _findChildExtent ( dmsExtentID childExtent, UINT16 &pos ) ;
      INT32 _deleteInternalKey ( UINT16 pos, const Ordering &order,
                                 ixmIndexCB *indexCB ) ;
      INT32 _setInternalKey ( UINT16 pos, const dmsRecordID &rid,
                              const ixmKey &key,
                              const Ordering &order, dmsExtentID lchild,
                              dmsExtentID rchild, ixmIndexCB *indexCB ) ;
      INT32 _doMergeChildren ( UINT16 pos, const Ordering &order,
                               ixmIndexCB *indexCB, BOOLEAN &result ) ;
      INT32 _locate ( const ixmKey &key, const dmsRecordID &rid,
                      const Ordering &order, ixmRecordID &indexrid,
                      BOOLEAN &found, INT32 direction,
                      const ixmIndexCB *indexCB ) ;

      INT32 _keyCmp ( const BSONObj &currentKey, const BSONObj &prevKey,
                      INT32 keepFieldsNum, BOOLEAN skipToNext,
                      const vector < const BSONElement *> &matchEle,
                      const vector < BOOLEAN > &matchInclusive,
                      const Ordering &o, INT32 direction ) ;

      INT32 _keyFind ( UINT16 low, UINT16 high, const BSONObj &prevKey,
                       INT32 keepFieldsNum, BOOLEAN skipToNext,
                       const vector < const BSONElement *> &matchEle,
                       const vector < BOOLEAN > &matchInclusive,
                       const Ordering &o,
                       INT32 direction,
                       ixmRecordID &bestIxmRID,
                       dmsExtentID &resultExtent, _pmdEDUCB *cb ) ;
   public:
      _ixmExtent ( CHAR *extentStart, UINT16 mbID,
                   dmsExtentID parent, dmsExtentID me,
                   _dmsStorageIndex *pIndexSu ) ;
      _ixmExtent ( dmsExtentID extentID, UINT16 mbID,
                   _dmsStorageIndex *pIndexSu );

      _ixmExtent ( CHAR *extentStart, dmsExtentID me,
                   _dmsStorageIndex *pIndexSu ) ;
      _ixmExtent ( dmsExtentID extentID,
                   _dmsStorageIndex *pIndexSu ) ;

      BOOLEAN verify () ;
      OSS_INLINE UINT16 getNumKeyNode ()
      {
         return _extentHead->_totalKeyNodeNum ;
      }
      OSS_INLINE const ixmKeyNode *getKeyNode ( UINT16 i )
      {
         if ( i>_extentHead->_totalKeyNodeNum )
            return NULL ;
         return (ixmKeyNode*)(((CHAR*)_extentHead) + sizeof(ixmExtentHead) +
                              sizeof(ixmKeyNode)*i) ;
      }
      OSS_INLINE CHAR *getKeyData ( UINT16 i )
      {
         if ( i>=_extentHead->_totalKeyNodeNum )
            return NULL ;
         return (CHAR*)_extentHead+getKeyNode(i)->_keyOffset ;
      }
      OSS_INLINE UINT16 getFreeSize()
      {
         return _extentHead->_totalFreeSize ;
      }
      OSS_INLINE UINT16 getTotalKeySize()
      {
         return (UINT16)(_pageSize-1) - _extentHead->_totalFreeSize -
                (sizeof(ixmExtentHead) +
                 _extentHead->_totalKeyNodeNum * sizeof(ixmKeyNode)) ;
      }
      OSS_INLINE BOOLEAN isRoot()
      {
         return DMS_INVALID_EXTENT == _extentHead->_parentExtentID ;
      }
      dmsExtentID getChildExtentID ( UINT16 i )
      {
         if ( i>_extentHead->_totalKeyNodeNum ) return DMS_INVALID_EXTENT ;
         return (i==_extentHead->_totalKeyNodeNum)?(_extentHead->_right):
                    (getKeyNode(i)->_left) ;
      }
      dmsRecordID getRID ( UINT16 i )
      {
         if ( i>=_extentHead->_totalKeyNodeNum ) return dmsRecordID() ;
         const ixmKeyNode *kn = getKeyNode(i) ;
         if ( kn->isUnused() ) return dmsRecordID() ;
         return kn->_rid ;
      }
      OSS_INLINE dmsExtentID getParent ()
      {
         return _extentHead->_parentExtentID ;
      }
      OSS_INLINE void setParent ( dmsExtentID extentID )
      {
         _extentHead->_parentExtentID = extentID ;
      }
      void setChildExtentID ( UINT16 i, dmsExtentID extentID ) ;
      OSS_INLINE ossValuePtr getChildExtentPtr ( UINT16 i ) ;
      void setCompact()
      {
         ((CHAR*)_extentHead)[(_pageSize-1)] = 1 ;
      }
      void unsetCompact()
      {
         ((CHAR*)_extentHead)[(_pageSize-1)] = 0 ;
      }
      BOOLEAN isCompact()
      {
         return 1==((CHAR*)_extentHead)[(_pageSize-1)] ;
      }
      INT32 insertStepOne ( ixmIndexInsertRequestImpl &insertRequest,
                            BOOLEAN dupAllowed ) ;
      INT32 find ( const ixmIndexCB *indexCB, const ixmKey &key,
                   const dmsRecordID &rid, const Ordering &order, UINT16 &pos,
                   BOOLEAN dupAllowed, BOOLEAN &found ) ;
      INT32 locate ( const BSONObj &key, const dmsRecordID &rid,
                     const Ordering &order, ixmRecordID &indexrid,
                     BOOLEAN &found, INT32 direction,
                     const ixmIndexCB *indexCB ) ;

      INT32 insertHere ( UINT16 pos, const dmsRecordID &rid, const ixmKey &key,
                         const Ordering &order, dmsExtentID lchild,
                         dmsExtentID rchild,
                         ixmIndexCB *indexCB ) ;

      INT32 unindex ( const ixmKey &key, const dmsRecordID &rid,
                      const Ordering &order, ixmIndexCB *indexCB,
                      BOOLEAN &result ) ;
      INT32 advance ( ixmRecordID &keyRID, INT32 direction ) ;
      INT32 exists ( const ixmKey &key, const Ordering &order,
                     const ixmIndexCB *indexCB, BOOLEAN &result ) ;
      dmsExtentID getRoot () ;
      INT32 findSingle ( const ixmKey &key, const Ordering &order,
                         dmsRecordID &rid, ixmIndexCB *indexCB ) ;
      INT32 insert ( const ixmKey &key, const dmsRecordID &rid,
                     const Ordering &order, BOOLEAN dupAllowed,
                     ixmIndexCB *indexCB ) ;
      void truncate ( ixmIndexCB *indexCB ) ;
      UINT64 count () ;

      BOOLEAN isStillValid( UINT16 mbID ) const ;

      /******************************************************/
      /*      index cursor usage functions                  */
      /******************************************************/
      INT32 keyLocate ( ixmRecordID &rid, const BSONObj &prevKey,
                        INT32 keepFieldsNum, BOOLEAN skipToNext,
                        const vector < const BSONElement *> &matchEle,
                        const vector < BOOLEAN > &matchInclusive,
                        const Ordering &o, INT32 direction,
                        _pmdEDUCB *cb ) ;
      INT32 keyAdvance ( ixmRecordID &rid, const BSONObj &prevKey,
                         INT32 keepFieldsNum, BOOLEAN skipToNext,
                         const vector < const BSONElement *> &matchEle,
                         const vector < BOOLEAN > &matchInclusive,
                         const Ordering &o, INT32 direction,
                         _pmdEDUCB *cb ) ;
      INT32 dumpIndexExtentIntoLog() ;
   } ;
   typedef class _ixmExtent ixmExtent ;
}

#endif //IXMEXTENT_HPP_

