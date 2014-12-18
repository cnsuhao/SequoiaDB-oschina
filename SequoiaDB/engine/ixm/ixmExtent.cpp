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

   Source File Name = ixmExtent.cpp

   Descriptive Name = Index Manager Extent

   When/how to use: this program may be used on binary and text-formatted
   versions of Index Manager component. This file contains functions for index
   extent implmenetation. This include B tree insert/update/delete.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "ixmExtent.hpp"
#include "dmsStorageIndex.hpp"
#include "ixmInsertRequest.hpp"
#include "pd.hpp"
#include "monCB.hpp"
#include "dmsStorageUnit.hpp"
#include "dmsDump.hpp"
#include "pdTrace.hpp"
#include "ixmTrace.hpp"

namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT1, "_ixmExtent::_ixmExtent" )
   _ixmExtent::_ixmExtent ( CHAR *extentStart, UINT16 mbID,
                            dmsExtentID parent, dmsExtentID me,
                            dmsStorageIndex *pIndexSu )
   :_extentHead((ixmExtentHead*)extentStart),
    _me(me),
    _pIndexSu(pIndexSu),
    _pageSize(pIndexSu->pageSize())
   {
      SDB_ASSERT ( extentStart, "extentStart can't be NULL" ) ;
      SDB_ASSERT ( _pIndexSu, "index su can't be NULL" ) ;
      PD_TRACE_ENTRY( SDB__IXMEXT1 ) ;

      _extentHead->_eyeCatcher [0] = IXM_EXTENT_EYECATCHER0 ;
      _extentHead->_eyeCatcher [1] = IXM_EXTENT_EYECATCHER1 ;
      _extentHead->_totalKeyNodeNum = 0 ;
      _extentHead->_mbID = mbID ;
      _extentHead->_version = IXM_EXTENT_CURRENT_V ;
      _extentHead->_parentExtentID = parent ;
      _extentHead->_beginFreeOffset = _pageSize-1 ;
      _extentHead->_right = DMS_INVALID_EXTENT ;
      _extentHead->_totalFreeSize = _extentHead->_beginFreeOffset -
                        (sizeof(ixmExtentHead) +
                        (_extentHead->_totalKeyNodeNum*sizeof(ixmKeyNode))) ;
      pIndexSu->addStatFreeSpace( mbID, _extentHead->_totalFreeSize ) ;

      PD_TRACE_EXIT ( SDB__IXMEXT1 );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT2, "_ixmExtent::_ixmExtent" )
   _ixmExtent::_ixmExtent ( dmsExtentID extentID, UINT16 mbID,
                            dmsStorageIndex *pIndexSu )
   {
      SDB_ASSERT ( pIndexSu, "index su can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB__IXMEXT2 ) ;
      _pIndexSu = pIndexSu ;
      _pageSize = _pIndexSu->pageSize() ;
      _extentHead = (ixmExtentHead*)_pIndexSu->extentAddr(extentID ) ;
      SDB_ASSERT(_extentHead, "extent can't be NULL" ) ;
      _me = extentID ;
      _extentHead->_eyeCatcher [0] = IXM_EXTENT_EYECATCHER0 ;
      _extentHead->_eyeCatcher [1] = IXM_EXTENT_EYECATCHER1 ;
      _extentHead->_totalKeyNodeNum = 0 ;
      _extentHead->_mbID = mbID ;
      _extentHead->_version = IXM_EXTENT_CURRENT_V ;
      _extentHead->_parentExtentID = DMS_INVALID_EXTENT ;
      _extentHead->_beginFreeOffset = _pageSize-1 ;
      _extentHead->_right = DMS_INVALID_EXTENT ;
      _extentHead->_totalFreeSize = _extentHead->_beginFreeOffset -
                        (sizeof(ixmExtentHead) +
                        (_extentHead->_totalKeyNodeNum*sizeof(ixmKeyNode))) ;
      pIndexSu->addStatFreeSpace( mbID, _extentHead->_totalFreeSize ) ;

      PD_TRACE_EXIT ( SDB__IXMEXT2 );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT3, "_ixmExtent::_ixmExtent" )
   _ixmExtent::_ixmExtent ( CHAR *extentStart, dmsExtentID me,
                            dmsStorageIndex *pIndexSu )
   {
      SDB_ASSERT ( extentStart, "extentStart can't be NULL" ) ;
      SDB_ASSERT ( pIndexSu, "index su can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB__IXMEXT3 );
      _extentHead = (ixmExtentHead*)extentStart ;
      _pIndexSu = pIndexSu ;
      _pageSize = _pIndexSu->pageSize() ;
      _me = me ;
      PD_TRACE_EXIT ( SDB__IXMEXT3 );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT4, "_ixmExtent::_ixmExtent" )
   _ixmExtent::_ixmExtent ( dmsExtentID extentID,
                            dmsStorageIndex *pIndexSu )
   {
      SDB_ASSERT ( pIndexSu, "index su can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB__IXMEXT4 );
      _pIndexSu = pIndexSu ;
      _pageSize = _pIndexSu->pageSize() ;
      _me = extentID ;
      _extentHead = (ixmExtentHead*)_pIndexSu->extentAddr(extentID) ;
      PD_TRACE_EXIT ( SDB__IXMEXT4 );
      SDB_ASSERT(_extentHead, "extent can't be NULL" ) ;
   }

   BOOLEAN _ixmExtent::verify ()
   {
      if ( !_extentHead )
      {
         PD_LOG ( PDERROR, "NULL index extent" ) ;
         return FALSE ;
      }
      if ( _extentHead->_eyeCatcher[0] != IXM_EXTENT_EYECATCHER0 ||
           _extentHead->_eyeCatcher[1] != IXM_EXTENT_EYECATCHER1 )
      {
         PD_LOG ( PDERROR, "Invalid index eye-catcher" ) ;
         return FALSE ;
      }
      if ( !(_extentHead->_flag & DMS_MB_FLAG_USED) )
      {
         PD_LOG ( PDERROR, "Unused extent" ) ;
         return FALSE ;
      }
      return TRUE ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_FIND, "_ixmExtent::find" )
   INT32 _ixmExtent::find ( const ixmIndexCB *indexCB, const ixmKey &key,
                            const dmsRecordID &rid, const Ordering &order,
                            UINT16 &pos, BOOLEAN dupAllowed, BOOLEAN &found )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_FIND );
      found = FALSE ;
      INT32 low = 0 ;
      INT32 high = _extentHead->_totalKeyNodeNum-1 ;
      INT32 middle = (low + high)/2 ;
      while ( low <= high )
      {
         PD_TRACE3 ( SDB__IXMEXT_FIND,
                     PD_PACK_INT ( low ),
                     PD_PACK_INT ( high ),
                     PD_PACK_INT ( middle ) ) ;
         CHAR *keyData = getKeyData (middle) ;
         if ( !keyData )
         {
            PD_LOG ( PDERROR, "Unable to locate key" ) ;
            dumpIndexExtentIntoLog () ;
            rc = SDB_SYS ;
            goto error ;
         }
         ixmKey keyDisk(keyData) ;
         INT32 result = key.woCompare ( keyDisk, order ) ;
         PD_TRACE1 ( SDB__IXMEXT_FIND, PD_PACK_INT ( result ) ) ;
         if ( 0 == result )
         {
            const ixmKeyNode *M = getKeyNode(middle) ;
            if ( !dupAllowed )
            {
               if ( M->isUsed() )
               {
                  rc = SDB_IXM_DUP_KEY ;
                  if ( 0 == rid.compare(M->_rid) )
                  {
                     pos = middle ;
                     found = TRUE ;
                     goto done ;
                  }
               }
            }
            result = rid.compare(M->_rid) ;
         }
         if ( result < 0 )
            high = middle -1 ;
         else if ( result > 0 )
            low = middle + 1 ;
         else
         {
            pos = middle ;
            found = TRUE ;
            goto done ;
         }
         middle = (low + high)/2 ;
      }
      pos = low ;
      PD_TRACE1 ( SDB__IXMEXT_FIND, PD_PACK_USHORT(pos) ) ;
      if ( pos != _extentHead->_totalKeyNodeNum )
      {
         {
            CHAR *keyData = getKeyData (pos) ;
            ixmKey keyDisk(keyData) ;
            if ( key.woCompare ( keyDisk, order ) > 0 )
            {
               PD_LOG ( PDERROR, "Internal logic error, key compare wrong" ) ;
               dumpIndexExtentIntoLog () ;
               rc = SDB_SYS ;
               goto error ;
            }
         }
         if ( pos > 0 )
         {
            CHAR *keyData = getKeyData (pos-1) ;
            ixmKey keyDisk(keyData) ;
            if ( keyDisk.woCompare ( key, order ) > 0 )
            {
               PD_LOG ( PDERROR, "Internal logic error, key compare wrong" ) ;
               dumpIndexExtentIntoLog () ;
               rc = SDB_SYS ;
               goto error ;
            }
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_FIND, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _ixmExtent::insert ( const ixmKey &key, const dmsRecordID &rid,
                              const Ordering &order, BOOLEAN dupAllowed,
                              ixmIndexCB *indexCB )
   {
      return _insert ( rid, key, order, dupAllowed, DMS_INVALID_EXTENT,
                       DMS_INVALID_EXTENT, indexCB ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_ISONE, "_ixmExtent::insertStepOne" )
   INT32 _ixmExtent::insertStepOne ( ixmIndexInsertRequestImpl &insertRequest,
                                    BOOLEAN dupAllowed )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_ISONE );
      BOOLEAN found = FALSE ;
      UINT16 pos = 0 ;
      ossValuePtr ptr = 0 ;
      if ( insertRequest._key.dataSize() >= IXM_KEY_MAX_SIZE )
      {
         PD_LOG ( PDERROR, "key size must be less than %d", IXM_KEY_MAX_SIZE ) ;
         rc = SDB_IXM_KEY_TOO_LARGE ;
         goto error ;
      }
      if ( insertRequest._key.dataSize() <= 0 )
      {
         PD_LOG ( PDERROR, "key size must be greater than 0" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = find ( insertRequest._indexCB, insertRequest._key,
                  insertRequest._recordRID, insertRequest._order, pos,
                  dupAllowed, found ) ;
      if ( rc )
      {
         if ( SDB_IXM_DUP_KEY == rc )
         {
            if ( TRUE == found )
            {
               rc = SDB_IXM_IDENTICAL_KEY ;
               PD_LOG ( PDERROR, "two keys are pointing to same record" ) ;
               goto error ;
            }
            if ( insertRequest._indexCB->enforced() ||
                 !insertRequest._key.isUndefined () )
            {
               PD_LOG ( PDINFO, "Duplicate key is detected" ) ;
               goto error ;
            }
            else
            {
               rc = SDB_OK ;
            }
         }
         if ( rc )
         {
            PD_LOG ( PDERROR, "Error happened during find, rc = %d", rc ) ;
            goto error ;
         }
      }
      if ( found )
      {
         const ixmKeyNode *kn = getKeyNode(pos) ;
         if ( kn->isUnused() )
         {
            insertRequest._extent = _extentHead ;
            insertRequest._extentID = _me ;
            insertRequest._pos = pos ;
            insertRequest._op = ixmIndexInsertRequest::SetUsed ;
            goto done ;
         }
         PD_LOG ( PDERROR, "Internal error, two keys are pointing to same "
                  "record" ) ;
         rc = SDB_IXM_IDENTICAL_KEY ;
         goto error ;
      }
      ptr = getChildExtentPtr ( pos ) ;
      if ( 0 == ptr )
      {
         insertRequest._extent = _extentHead ;
         insertRequest._extentID = _me ;
         insertRequest._pos = pos ;
         insertRequest._op = ixmIndexInsertRequest::InsertHere ;
         goto done ;
      }
      rc = _ixmExtent((CHAR*)ptr,getChildExtentID(pos),
                       _pIndexSu).insertStepOne ( insertRequest, dupAllowed ) ;
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_ISONE, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_INSERTHERE, "_ixmExtent::insertHere" )
   INT32 _ixmExtent::insertHere ( UINT16 pos, const dmsRecordID &rid,
                                  const ixmKey &key, const Ordering &order,
                                  dmsExtentID lchild, dmsExtentID rchild,
                                  ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_INSERTHERE );
      rc = _basicInsert ( pos, rid, key, order ) ;
      if ( rc )
      {
         if ( SDB_IXM_NOSPC == rc )
         {
            rc = _split ( pos, rid, key, order, lchild, rchild, indexCB ) ;
            goto done ;
         }
         PD_LOG ( PDERROR, "Failed to insert, rc = %d", rc ) ;
         goto error ;
      }
      {
         ixmKeyNode *kn = (ixmKeyNode*)getKeyNode(pos) ;
         if ( pos+1 == getNumKeyNode ())
         {
            if ( _extentHead->_right != lchild )
            {
               PD_LOG ( PDERROR, "index logic error[lchild:%d, rchild:%d, "
                        "pos:%u, _extentHead->_right:%d]", lchild, rchild, pos,
                        _extentHead->_right ) ;
               dumpIndexExtentIntoLog () ;
               rc = SDB_SYS ;
               goto error ;
            }
            kn->_left = _extentHead->_right ;

            _assignRight ( rchild ) ;
            /*if ( DMS_INVALID_EXTENT != rchild )
            {
               _ixmExtent ( rchild, _pIndexSu ).setParent ( _me ) ;
            }*/
         }
         else
         {
            kn->_left = lchild ;

            ixmKeyNode *kn1 = (ixmKeyNode*)getKeyNode(pos+1) ;
            if ( kn1->_left != lchild )
            {
               PD_LOG ( PDERROR, "index logic error[lchild:%d, rchild:%d, "
                        "pos:%u, kn1->_left:%d]", lchild, rchild, pos,
                        kn1->_left ) ;
               dumpIndexExtentIntoLog () ;
               rc = SDB_SYS ;
               goto error ;
            }

            kn1->_left = rchild ;
            if ( DMS_INVALID_EXTENT != rchild )
            {
               _ixmExtent ( rchild, _pIndexSu ).setParent ( _me ) ;
            }
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_INSERTHERE, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__BASICINS, "_ixmExtent::_basicInsert" )
   INT32 _ixmExtent::_basicInsert ( UINT16 &pos, const dmsRecordID &rid,
                                   const ixmKey &key, const Ordering &order )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__BASICINS );
      UINT16 bytesNeeded ;
      if ( pos > getNumKeyNode () )
      {
         PD_LOG ( PDERROR, "insert pos out of range" ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      bytesNeeded = key.dataSize() + sizeof(ixmKeyNode) ;
      if ( bytesNeeded > getFreeSize() )
      {
         dmsExtentID ch ;
         ch = getChildExtentID ( pos ) ;
         rc = _reorg (order, pos) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "index extent reorg failed with rc : %d", rc ) ;
            goto error ;
         }
         SDB_ASSERT ( pos <= getNumKeyNode(), "pos is out of range" ) ;
         if ( bytesNeeded > getFreeSize() )
         {
            rc = SDB_IXM_NOSPC ;
            goto error ;
         }
         if ( getChildExtentID ( pos ) != ch )
         {
            rc = SDB_IXM_REORG_DONE ;
            goto error ;
         }
      }
      ossMemmove ( (void*)getKeyNode(pos+1), (void*)getKeyNode(pos),
                   sizeof(ixmKeyNode)*(getNumKeyNode()-pos) ) ;
      _extentHead->_totalFreeSize -= sizeof(ixmKeyNode) ;
      _pIndexSu->decStatFreeSpace( _extentHead->_mbID, sizeof(ixmKeyNode) ) ;
      _extentHead->_totalKeyNodeNum ++ ;
      {
         INT32 datasize = key.dataSize() ;
         ixmKeyNode *kn = (ixmKeyNode*)getKeyNode(pos) ;
         kn->_left = DMS_INVALID_EXTENT ;
         kn->_rid = rid ;
         rc = _alloc ( datasize, kn->_keyOffset ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to allocate %d bytes in index",
                     key.dataSize()) ;
            goto error ;
         }
         ossMemcpy ( ((CHAR*)_extentHead)+kn->_keyOffset,
                      key.data(), datasize ) ;
      }
#if defined (_DEBUG)
      rc = _validate(MAX, order) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to validate the extent, rc = %d", rc ) ;
         goto error ;
      }
#endif
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__BASICINS, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__SPLIT, "_ixmExtent::_split" )
   INT32 _ixmExtent::_split ( UINT16 pos, const dmsRecordID &rid,
                             const ixmKey &key, const Ordering &order,
                             const dmsExtentID lchild, const dmsExtentID rchild,
                             ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__SPLIT );
      UINT16 splitPos, newPos ;
      SDB_ASSERT ( indexCB, "index control block can't be NULL" ) ;
      dmsExtentID newExtentID ;
      const ixmKeyNode *splitKey = NULL ;
      rc = _splitPos ( pos, splitPos ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get split position, rc = %d", rc ) ;
         goto error ;
      }
      PD_TRACE2 ( SDB__IXMEXT__SPLIT, PD_PACK_USHORT(pos),
                  PD_PACK_USHORT(splitPos) ) ;
      rc = indexCB->allocExtent ( newExtentID ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to allocate new extent for index, rc = %d",
                  rc ) ;
         goto error ;
      }
      {
         _ixmExtent newExtent(newExtentID, _extentHead->_mbID, _pIndexSu ) ;
         for ( UINT16 i = splitPos +1; i<getNumKeyNode(); i++)
         {
            const ixmKeyNode *kn = getKeyNode(i) ;
            rc = newExtent._pushBack ( kn->_rid,
                                    ixmKey(((CHAR*)_extentHead)+kn->_keyOffset),
                                    order, kn->_left ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to push back key %d to new extent, "
                        "rc = %d", (INT32)i, rc ) ;
               goto error ;
            }
         }
         newExtent._assignRight ( _extentHead->_right ) ;
         /*rc = newExtent._fixParentPtrs ( 0, newExtent.getNumKeyNode() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to fix parent pointers for the new "
                     "extent, rc = %d", rc ) ;
            goto error ;
         }*/
#if defined (_DEBUG)
         rc = newExtent._validate(MAX, order) ;
#else
         rc = newExtent._validate(MIN, order) ;
#endif
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to validate the new extent, rc = %d",
                     rc ) ;
            goto error ;
         }

         splitKey = getKeyNode(splitPos) ;
         _assignRight ( splitKey->_left ) ;
         if ( DMS_INVALID_EXTENT == getParent() )
         {
            dmsExtentID rootExtentID ;
            rc = indexCB->allocExtent ( rootExtentID ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to allocate new extent for index, "
                        "rc = %d", rc ) ;
               goto error ;
            }
            _ixmExtent rootExtent( rootExtentID, _extentHead->_mbID,
                                   _pIndexSu ) ;
            rc = rootExtent._pushBack ( splitKey->_rid,
                                        ixmKey(((CHAR*)_extentHead)+
                                        splitKey->_keyOffset), order, _me ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to promote split key into root, "
                        "rc = %d", rc ) ;
               goto error ;
            }
            rootExtent._assignRight ( newExtentID ) ;
#if defined (_DEBUG)
            rc = rootExtent._validate(MAX, order) ;
#else
            rc = rootExtent._validate(MIN, order) ;
#endif
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to validate the new root, rc = %d",
                        rc ) ;
               goto error ;
            }
            /*setParent ( rootExtentID ) ;*/
            /*newExtent.setParent ( rootExtentID ) ;*/
            indexCB->setRoot ( rootExtentID ) ;
         }
         else
         {
            newExtent.setParent ( getParent() ) ;
            _ixmExtent parentExtent(getParent(), _pIndexSu ) ;
            rc = parentExtent._insert(splitKey->_rid,
                       ixmKey(((CHAR*)_extentHead)+splitKey->_keyOffset),
                       order, TRUE, _me,
                       newExtentID, indexCB ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to promote into parent, rc = %d",
                        rc ) ;
               goto error ;
            }
         }
         newPos = pos ;
         rc = _truncate ( splitPos, newPos, order ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to truncate index extent, rc = %d",
                     rc ) ;
            goto error ;
         }
         PD_TRACE1 ( SDB__IXMEXT__SPLIT, PD_PACK_USHORT(newPos) ) ;
         if ( pos <= splitPos )
         {
            SDB_ASSERT ( 0xFFFF != newPos, "Invalid newPos" ) ;
            rc = insertHere ( newPos, rid, key, order, lchild, rchild,
                              indexCB ) ;
         }
         else
         {
            rc = newExtent.insertHere ( pos-splitPos-1, rid, key, order, lchild,
                                        rchild, indexCB ) ;
         }
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to insert into splitted page, rc = %d",
                     rc ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__SPLIT, rc );
      return rc ;
   error :
      goto done ;
   }
   INT32 _ixmExtent::_truncate ( UINT16 totalNodes, UINT16 &newPos,
                                 const Ordering &order )
   {
      if ( totalNodes < getNumKeyNode() )
      {
         _extentHead->_totalKeyNodeNum = totalNodes ;
         unsetCompact() ;
         return _reorg(order, newPos) ;
      }
      return SDB_OK ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__SPLITPOS, "_ixmExtent::_splitPos" )
   INT32 _ixmExtent::_splitPos ( UINT16 pos, UINT16 &splitPos )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__SPLITPOS );
      UINT16 rightSize = 0 ;
      UINT16 maxRightSize = 0 ;
      UINT16 totalKeySize = getTotalKeySize() ;
      PD_TRACE1 ( SDB__IXMEXT__SPLITPOS, PD_PACK_USHORT(totalKeySize) );
      splitPos = 1 ;
      if ( getNumKeyNode() <= 2 )
      {
         PD_LOG ( PDERROR, "Only %d elements in the index",
                  (INT32)getNumKeyNode() ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( pos == _extentHead->_totalKeyNodeNum )
      {
         maxRightSize = totalKeySize / 10 ;
      }
      else
      {
         maxRightSize = totalKeySize / 2 ;
      }
      for ( INT32 i = _extentHead->_totalKeyNodeNum-1; i>=0; --i )
      {
         rightSize += ixmKey(getKeyData(i)).dataSize() ;
         if ( rightSize > maxRightSize)
         {
            splitPos = i ;
            break ;
         }
      }
      if ( splitPos > getNumKeyNode()-2 )
      {
         splitPos = getNumKeyNode() - 2 ;
      }
      PD_TRACE1 ( SDB__IXMEXT__SPLITPOS, PD_PACK_USHORT( splitPos ) ) ;
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__SPLITPOS, rc );
      return rc ;
   error :
      goto done ;
   }
   INT32 _ixmExtent::_fixParentPtrs ( UINT16 startPos, UINT16 stopPos )
   {
      for ( UINT16 i = startPos; i < stopPos; i++ )
      {
         const ixmKeyNode *kn = getKeyNode ( i ) ;
         if ( DMS_INVALID_EXTENT != kn->_left )
         {
            _ixmExtent childExtent ( kn->_left, _pIndexSu ) ;
            childExtent.setParent ( _me ) ;
         }
      }
      return SDB_OK ;
   }
   void _ixmExtent::_assignRight ( const dmsExtentID right )
   {
      _extentHead->_right = right ;
      if ( DMS_INVALID_EXTENT != right )
      {
         _ixmExtent childExtent ( right, _pIndexSu ) ;
         childExtent.setParent ( _me ) ;
      }
   }

   void _ixmExtent::setChildExtentID ( UINT16 i, dmsExtentID extentID )
   {
      if ( i>_extentHead->_totalKeyNodeNum ) return ;
      else if ( i == _extentHead->_totalKeyNodeNum )
         _assignRight ( extentID ) ;
      else
      {
         ((ixmKeyNode*)getKeyNode(i))->_left = extentID ;
         if ( DMS_INVALID_EXTENT != extentID )
         {
            _ixmExtent childExtent ( extentID, _pIndexSu ) ;
            childExtent.setParent ( _me ) ;
         }
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__PSHBACK, "_ixmExtent::_pushBack" )
   INT32 _ixmExtent::_pushBack ( const dmsRecordID &rid, const ixmKey &key,
                                 const Ordering &order, const dmsExtentID left )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__PSHBACK );
      UINT16 bytesNeeded = key.dataSize() + sizeof(ixmKeyNode) ;
      ixmKeyNode *kn = NULL ;
      if ( bytesNeeded > _extentHead->_totalFreeSize )
      {
         PD_LOG ( PDERROR, "Bytes needed should never smaller than "
                  "_totalFreeSize" ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( getNumKeyNode() )
      {
         ixmKey lastkey ( getKeyData(getNumKeyNode()-1) ) ;
         if ( lastkey.woCompare(key, order) > 0 )
         {
            PD_LOG ( PDERROR, "New key smaller than the last key during "
                     "push" ) ;
            dumpIndexExtentIntoLog () ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
      _extentHead->_totalFreeSize -= sizeof(ixmKeyNode) ;
      _pIndexSu->decStatFreeSpace( _extentHead->_mbID, sizeof(ixmKeyNode) ) ;
      kn = (ixmKeyNode*)getKeyNode(_extentHead->_totalKeyNodeNum) ;
      _extentHead->_totalKeyNodeNum++ ;
      kn->_left = left ;
      if ( DMS_INVALID_EXTENT != kn->_left )
      {
         _ixmExtent childExtent ( kn->_left, _pIndexSu ) ;
         childExtent.setParent ( _me ) ;
      }
      kn->_rid = rid ;
      rc = _alloc ( key.dataSize(), kn->_keyOffset ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to allocate %d bytes in index",
                  key.dataSize()) ;
         goto error ;
      }
      ossMemcpy ( ((CHAR*)_extentHead)+kn->_keyOffset,
                  key.data(), key.dataSize()) ;
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__PSHBACK, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__VALIDATE, "_ixmExtent::_validate" )
   INT32 _ixmExtent::_validate ( _ixmExtentValidateLevel level,
                                 const Ordering &order )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__VALIDATE );
      if ( NONE == level )
         goto done ;
      if ( _extentHead->_eyeCatcher[0] != IXM_EXTENT_EYECATCHER0 ||
           _extentHead->_eyeCatcher[1] != IXM_EXTENT_EYECATCHER1 )
      {
         PD_LOG ( PDERROR, "Invalid index extent eye catcher" ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( _extentHead->_beginFreeOffset - sizeof(ixmExtentHead) -
           _extentHead->_totalKeyNodeNum*sizeof(ixmKeyNode) !=
           _extentHead->_totalFreeSize )
      {
         PD_LOG ( PDERROR, "Inconsistent free size" ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      if ( !(_extentHead->_flag & DMS_MB_FLAG_USED) )
      {
         PD_LOG ( PDERROR, "Invalid flag" ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( MID == level )
      {
         ixmKey k1 ( getKeyData(0) ) ;
         ixmKey k2 ( getKeyData(_extentHead->_totalKeyNodeNum-1) ) ;
         if ( k1.woCompare(k2, order) > 0 )
         {
            PD_LOG ( PDERROR, "First key is greater than the last" ) ;
            dumpIndexExtentIntoLog () ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
      else if ( MAX == level )
      {
         for ( UINT16 i = 0; i < _extentHead->_totalKeyNodeNum-1; i++ )
         {
            ixmKey k1 ( getKeyData(i)) ;
            ixmKey k2 ( getKeyData(i+1)) ;
            INT32 result = k1.woCompare(k2, order) ;
            if ( result > 0 )
            {
               PD_LOG ( PDERROR, "%d'th key is greater than its next",
                        (INT32)i ) ;
               dumpIndexExtentIntoLog () ;
               rc = SDB_SYS ;
               goto error ;
            }
            else if ( 0 == result )
            {
               dmsRecordID rid1 = getKeyNode(i)->_rid ;
               dmsRecordID rid2 = getKeyNode(i+1)->_rid ;
               if ( rid1.compare(rid2) >=0 )
               {
                  PD_LOG ( PDERROR, "%d'th key's RID is greater or equal to "
                           "the next", (INT32)i ) ;
                  dumpIndexExtentIntoLog () ;
                  rc = SDB_SYS ;
                  goto error ;
               }
            } //else if ( 0 == result )
         } //for (
      } //else if ( MAX == level )
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__VALIDATE, rc );
      return rc ;
   error :
      goto done ;
   }
   INT32 _ixmExtent::_reorg (const Ordering &order)
   {
      UINT16 dummy = 0xFFFF ;
      return _reorg ( order, dummy ) ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__REORG, "_ixmExtent::_reorg" )
   INT32 _ixmExtent::_reorg (const Ordering &order, UINT16 &newPos)
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__REORG );
      if ( isCompact() )
         return rc ;
      UINT16 beginFreeOffset = _pageSize-1 ;
      UINT16 totalKeyNodeNum = 0 ;
      UINT16 totalFreeSize = beginFreeOffset - sizeof(ixmExtentHead) ;
      CHAR   buffer[DMS_PAGE_SIZE_MAX] ;

      for ( UINT16 i = 0 ; i<_extentHead->_totalKeyNodeNum; i++ )
      {
         ixmKeyNode *kn = (ixmKeyNode*)getKeyNode(i) ;
         INT32 keyDataSize = 0 ;
         if ( newPos == i )
            newPos = totalKeyNodeNum ;
         if ( kn->isUnused() && DMS_INVALID_EXTENT == kn->_left )
            continue ;
         totalFreeSize -= sizeof(ixmKeyNode) ;
         ixmKey key ( ((CHAR*)_extentHead)+kn->_keyOffset) ;
         keyDataSize = key.dataSize() ;
         if ( (INT32)beginFreeOffset - keyDataSize < 0 ||
              (INT32)totalFreeSize - keyDataSize < 0 )
         {
            PD_LOG ( PDERROR, "key is too large" ) ;
            dumpIndexExtentIntoLog () ;
            rc = SDB_SYS ;
            goto error ;
         }
         beginFreeOffset -= keyDataSize ;
         totalFreeSize -= keyDataSize ;
         ossMemcpy ( &buffer[beginFreeOffset],
                     ((CHAR*)_extentHead)+kn->_keyOffset,
                      keyDataSize ) ;
         kn->_keyOffset = beginFreeOffset ;
         if ( totalKeyNodeNum != i )
         {
            ossMemcpy ( ((CHAR*)_extentHead) + sizeof(ixmExtentHead) +
                        totalKeyNodeNum*sizeof(ixmKeyNode),
                        ((CHAR*)_extentHead) + sizeof(ixmExtentHead) +
                        i*sizeof(ixmKeyNode),
                        sizeof(ixmKeyNode)) ;
         }
         ++totalKeyNodeNum ;
      }
      if ( _extentHead->_totalKeyNodeNum == newPos )
         newPos = totalKeyNodeNum ;
      else if ( _extentHead->_totalKeyNodeNum < newPos )
         newPos = 0xFFFF ;
	PD_TRACE1 ( SDB__IXMEXT__REORG, PD_PACK_USHORT( newPos ) ) ;
      _extentHead->_beginFreeOffset = beginFreeOffset ;
      _extentHead->_totalKeyNodeNum = totalKeyNodeNum ;
      _pIndexSu->decStatFreeSpace( _extentHead->_mbID,
                                   _extentHead->_totalFreeSize ) ;
      _extentHead->_totalFreeSize = totalFreeSize ;
      _pIndexSu->addStatFreeSpace( _extentHead->_mbID,
                                   _extentHead->_totalFreeSize ) ;
      ossMemcpy ( ((CHAR*)_extentHead)+beginFreeOffset,
                  &buffer[beginFreeOffset],
                  _pageSize - beginFreeOffset ) ;
#if defined (_DEBUG)
      rc = _validate(MAX, order) ;
#else
      rc = _validate(MIN, order) ;
#endif
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to validate the new extent, rc = %d", rc ) ;
         goto error ;
      }
      setCompact() ;
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__REORG, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _ixmExtent::_alloc ( INT32 requestSpace, UINT16 &beginOffset )
   {
      if ( requestSpace > (INT32)getFreeSize() )
         return SDB_IXM_NOSPC ;
      _extentHead->_beginFreeOffset -= requestSpace ;
      _extentHead->_totalFreeSize -= requestSpace ;
      _pIndexSu->decStatFreeSpace( _extentHead->_mbID, requestSpace ) ;
      beginOffset = _extentHead->_beginFreeOffset ;
      return SDB_OK ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__INSERT, "_ixmExtent::_insert" )
   INT32 _ixmExtent::_insert ( const dmsRecordID &rid, const ixmKey &key,
                               const Ordering &order, BOOLEAN dupAllowed,
                               dmsExtentID lchild, dmsExtentID rchild,
                               ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__INSERT );
      BOOLEAN found = FALSE ;
      UINT16 pos = 0 ;
      dmsExtentID ch = DMS_INVALID_EXTENT ;
      if ( key.dataSize() >= IXM_KEY_MAX_SIZE )
      {
         PD_LOG ( PDERROR, "key size must be less than %d", IXM_KEY_MAX_SIZE ) ;
         rc = SDB_IXM_KEY_TOO_LARGE ;
         goto error ;
      }
      if ( key.dataSize() <= 0 )
      {
         PD_LOG ( PDERROR, "key size must be greater than 0" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   retry :
      rc = find ( indexCB, key, rid, order, pos, dupAllowed, found ) ;
      if ( rc )
      {
         if ( SDB_IXM_DUP_KEY == rc )
         {
            if ( TRUE == found )
            {
               rc = SDB_IXM_IDENTICAL_KEY ;
               PD_LOG ( PDERROR, "two keys are pointing to same record" ) ;
               goto error ;
            }
            if ( indexCB->enforced() || !key.isUndefined () )
            {
               PD_LOG ( PDINFO, "Duplicate key is detected" ) ;
               goto error ;
            }
            else
            {
               rc = SDB_OK ;
            }
         }
         if ( rc )
         {
            PD_LOG ( PDERROR, "Error happened during find, rc = %d", rc ) ;
            goto error ;
         }
      }
      if ( found )
      {
         ixmKeyNode *kn = (ixmKeyNode*)getKeyNode ( pos ) ;
         if ( kn->isUnused() )
         {
            kn->setUsed() ;
            goto done ;
         }
         PD_LOG ( PDERROR, "same key + rid is already in index" ) ;
         rc = SDB_IXM_IDENTICAL_KEY ;
         goto error ;
      }
      ch = getChildExtentID ( pos ) ;
      if ( DMS_INVALID_EXTENT == ch || DMS_INVALID_EXTENT != rchild )
      {
         rc = insertHere ( pos, rid, key, order, lchild, rchild, indexCB ) ;
         if ( rc )
         {
            if ( SDB_IXM_REORG_DONE == rc )
            {
               rc = SDB_OK ;
               goto retry ;
            }
            PD_LOG ( PDERROR, "Failed to insert, rc = %d", rc ) ;
            goto error ;
         }
      }
      else
      {
         rc = _ixmExtent(ch, _pIndexSu)._insert( rid, key, order, dupAllowed, 
                                                 lchild, rchild, indexCB ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to insert, rc = %d", rc ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__INSERT, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_UNINDEX, "_ixmExtent::unindex" )
   INT32 _ixmExtent::unindex ( const ixmKey &key, const dmsRecordID &rid,
                               const Ordering &order, ixmIndexCB *indexCB,
                               BOOLEAN &result )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_UNINDEX );
      BOOLEAN found ;
      ixmRecordID indexrid ;
      result = FALSE ;

      rc = _locate ( key, rid, order, indexrid, found, 1, indexCB ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to locate key and rid" ) ;
         goto error ;
      }
      if ( found )
      {
         rc = ixmExtent( indexrid._extent, _pIndexSu)._delKeyAtPos (
                         indexrid._slot, order, indexCB ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "failed to delete key" ) ;
            goto error ;
         }
         result = TRUE ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_UNINDEX, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__DELKEYATPOS1, "_ixmExtent::_delKeyAtPos" )
   INT32 _ixmExtent::_delKeyAtPos ( UINT16 pos )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__DELKEYATPOS1 );
      ixmKeyNode *kn ;
      if ( pos >= getNumKeyNode() )
      {
         PD_LOG ( PDERROR, "pos out of range, pos=%d, totalKey=%d",
                  (INT32)pos, (INT32)getNumKeyNode() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      kn = (ixmKeyNode*)getKeyNode(pos) ;
      if ( DMS_INVALID_EXTENT != kn->_left )
      {
         PD_LOG ( PDERROR, "left pointer must be NULL" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      _extentHead->_totalFreeSize += sizeof(ixmKeyNode) ;
      _pIndexSu->addStatFreeSpace( _extentHead->_mbID, sizeof(ixmKeyNode) ) ;
      _extentHead->_totalKeyNodeNum -- ;
      ossMemmove ( (CHAR*)getKeyNode(pos), (CHAR*)getKeyNode(pos+1),
                   sizeof(ixmKeyNode)*(_extentHead->_totalKeyNodeNum-pos) ) ;
      unsetCompact() ;
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__DELKEYATPOS1, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__DELKEYATPOS2, "_ixmExtent::_delKeyAtPos" )
   INT32 _ixmExtent::_delKeyAtPos ( UINT16 pos, const Ordering &order,
                                    ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__DELKEYATPOS2 );
      dmsExtentID left ;
      BOOLEAN result ;
      if ( pos >= getNumKeyNode() )
      {
         PD_LOG ( PDERROR, "pos out of range, pos=%d, totalKey=%d",
                 (INT32)pos, (INT32)getNumKeyNode() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      left = getKeyNode ( pos )->_left ;
      if ( 1 == getNumKeyNode() )
      {
         if ( DMS_INVALID_EXTENT == left &&
              DMS_INVALID_EXTENT == _extentHead->_right )
         {
            rc = _delKeyAtPos ( pos ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to delete at pos %d", (INT32) pos ) ;
               goto error ;
            }
            if ( DMS_INVALID_EXTENT != getParent() )
            {
               rc = _mayBalanceWithNeighbors ( order, indexCB, result ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to balance with neighbors" ) ;
                  goto error ;
               }
               if ( !result )
               {
                  rc = _delExtent ( indexCB ) ;
                  if ( rc )
                  {
                     PD_LOG ( PDERROR, "Failed to delete extent for the "
                              "index" ) ;
                     goto error ;
                  }
               }
            }
            goto done ;
         }
         rc = _deleteInternalKey ( pos, order, indexCB ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to delete internal key" ) ;
            goto error ;
         }
         goto done ;
      }
      if ( DMS_INVALID_EXTENT == left )
      {
         rc = _delKeyAtPos ( pos ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to delete at pos %d", (INT32)pos ) ;
            goto error ;
         }
         rc = _mayBalanceWithNeighbors ( order, indexCB, result ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to balance with neighbors" ) ;
            goto error ;
         }
      }
      else
      {
         rc = _deleteInternalKey ( pos, order, indexCB ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to delete internal key" ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__DELKEYATPOS2, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__MAYBLCWITHNGB, "_ixmExtent::_mayBalanceWithNeighbors" )
   INT32 _ixmExtent::_mayBalanceWithNeighbors ( const Ordering &order,
                                                ixmIndexCB *indexCB,
                                                BOOLEAN &result )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__MAYBLCWITHNGB );
      result = FALSE ;
      UINT16 pos ;
      if ( DMS_INVALID_EXTENT == getParent() )
         return rc ;
      ixmExtent parent( getParent(), _pIndexSu ) ;
      rc = parent._findChildExtent ( _me, pos ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Unable to find the extent in it's parent" ) ;
         goto error ;
      }
      /*mayBalanceRight = (pos < parent.getNumKeyNode() &&
                         parent.getChildExtentID(pos+1) !=
                            DMS_INVALID_EXTENT ) ;
      mayBalanceLeft = (pos>0 && parent.getChildExtentID(pos-1) !=
                            DMS_INVALID_EXTENT ) ;*/
      /*
      if ( mayBalanceRight )
      {
         rc = parent._tryBalanceChildren ( pos, order, indexCB, result ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to try balance children" ) ;
            goto error ;
         }
         if ( result )
            goto done ;
      }
      if ( mayBalanceLeft )
      {
         rc = parent._tryBalanceChildren ( pos-1, order, indexCB, result ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to try balance children" ) ;
            goto error ;
         }
         if ( result )
            goto done ;
      }
      if ( mayBalanceRight )
      {
         rc = parent._doMergeChildren ( pos, order, indexCB, result ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to try balance children" ) ;
            goto error ;
         }
         goto done ;
      }
      if ( mayBalanceLeft )
      {
         rc = parent._doMergeChildren ( pos-1, order, indexCB, result ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to try balance children" ) ;
            goto error ;
         }
         goto done ;
      } */
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__MAYBLCWITHNGB, rc );
      return rc ;
   error :
      goto done ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__DELEXT, "_ixmExtent::_delExtent" )
   INT32 _ixmExtent::_delExtent ( ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__DELEXT );
      UINT16 pos ;
      if ( DMS_INVALID_EXTENT == getParent() )
         return rc ;
      ixmExtent parent( getParent(), _pIndexSu ) ;
      rc = parent._findChildExtent ( _me, pos ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Unable to find the extent in it's parent" ) ;
         goto error ;
      }
      parent.setChildExtentID ( pos, DMS_INVALID_EXTENT ) ;
      rc = indexCB->freeExtent ( _me ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Unable to free extent" ) ;
         goto error ;
      }
      _pIndexSu->decStatFreeSpace( _extentHead->_mbID,
                                   _extentHead->_totalFreeSize ) ;

   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__DELEXT, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__FNDCHLDEXT, "_ixmExtent::_findChildExtent " )
   INT32 _ixmExtent::_findChildExtent ( dmsExtentID childExtent, UINT16 &pos )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__FNDCHLDEXT );
      if ( _extentHead->_right == childExtent )
      {
         pos = getNumKeyNode() ;
         goto done ;
      }
      for ( UINT16 i =0 ; i<getNumKeyNode(); i++ )
      {
         if ( getChildExtentID (i) == childExtent )
         {
            pos = i ;
            goto done ;
         }
      }
      rc = SDB_IXM_KEY_NOTEXIST ;
   done :
      PD_TRACE1 ( SDB__IXMEXT__FNDCHLDEXT, PD_PACK_USHORT( pos ) );
      PD_TRACE_EXITRC ( SDB__IXMEXT__FNDCHLDEXT, rc );
      return rc ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__DELITNKEY, "_ixmExtent::_deleteInternalKey" )
   INT32 _ixmExtent::_deleteInternalKey ( UINT16 pos, const Ordering &order,
                                          ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__DELITNKEY );
      dmsExtentID lchild = getChildExtentID(pos) ;
      dmsExtentID rchild = getChildExtentID(pos+1) ;
      ixmRecordID nextIndexKey ;
      INT32 direction ;
      if ( DMS_INVALID_EXTENT == lchild && DMS_INVALID_EXTENT == rchild )
      {
         PD_LOG ( PDERROR, "both left/right child are NULL" ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      direction = (DMS_INVALID_EXTENT == lchild)?1:-1 ;
      nextIndexKey._extent = _me ;
      nextIndexKey._slot = pos ;
      rc = advance ( nextIndexKey, direction ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to find the next index key" ) ;
         goto error ;
      }
      if ( nextIndexKey.isNull() )
      {
         PD_LOG ( PDERROR, "advance key shouldn't be NULL" ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      {
         ixmExtent nextExtent ( nextIndexKey._extent, _pIndexSu ) ;
         if ( nextExtent.getChildExtentID ( nextIndexKey._slot ) !=
                    DMS_INVALID_EXTENT ||
              nextExtent.getChildExtentID ( nextIndexKey._slot+1 ) !=
                    DMS_INVALID_EXTENT )
         {
            ((ixmKeyNode*)getKeyNode(pos))->setUnused() ;
         }
         else
         {
            const ixmKeyNode *kn = nextExtent.getKeyNode
                  ( nextIndexKey._slot ) ;
            ixmKey nextKey ( nextExtent.getKeyData(nextIndexKey._slot)) ;
            if ( !kn )
            {
               PD_LOG ( PDERROR, "Failed to find key node" ) ;
               dumpIndexExtentIntoLog () ;
               rc = SDB_SYS ;
               goto error ;
            }
            rc = _setInternalKey ( pos, kn->_rid, nextKey, order,
                                   getChildExtentID ( pos ) ,
                                   getChildExtentID ( pos+1 ) ,
                                   indexCB ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "failed to set internal key" ) ;
               goto error ;
            }
            rc = nextExtent._delKeyAtPos ( nextIndexKey._slot, order,
                                           indexCB ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "failed to delete key" ) ;
               goto error ;
            }
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__DELITNKEY, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_ADVANCE, "_ixmExtent::advance" )
   INT32 _ixmExtent::advance ( ixmRecordID &keyRID, INT32 direction )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_ADVANCE );
      INT32 adj ;
      INT32 ko ;
      dmsExtentID nextDown ;
      dmsExtentID childExtent ;
      dmsExtentID parent ;

      if ( keyRID._slot >= getNumKeyNode() )
      {
         PD_LOG ( PDERROR, "key slot is out of range" ) ;
         rc = SDB_IXM_KEY_NOTEXIST ;
         goto error ;
      }
      adj = direction < 0 ? 1:0 ;
      ko = keyRID._slot + direction ;
      nextDown = getChildExtentID((UINT16)(ko+adj)) ;
      if ( DMS_INVALID_EXTENT != nextDown )
      {
         while ( TRUE )
         {
            ixmExtent childExtent(nextDown, _pIndexSu) ;
            keyRID._slot = direction>0?0:
                (childExtent.getNumKeyNode()-1) ;
            dmsExtentID child = childExtent.getChildExtentID(keyRID._slot+adj) ;
            if ( DMS_INVALID_EXTENT == child )
               break ;
            nextDown = child ;
         }
         keyRID._extent = nextDown ;
         goto done ;
      }
      if ( ko < getNumKeyNode() && ko >= 0 )
      {
         keyRID._slot = (UINT16)ko ;
         keyRID._extent = _me ;
         goto done ;
      }
      childExtent = _me ;
      parent = getParent() ;
      while ( TRUE )
      {
         if ( DMS_INVALID_EXTENT == parent )
            break ;
         ixmExtent parentExtent ( parent, _pIndexSu ) ;
         for ( UINT16 i=0; i<parentExtent.getNumKeyNode(); i++ )
         {
            if ( childExtent == parentExtent.getChildExtentID(i+adj) )
            {
               keyRID._slot = i ;
               keyRID._extent = parent ;
               goto done ;
            }
         }
         if ( direction > 0 && parentExtent._extentHead->_right != childExtent )
         {
            PD_LOG ( PDERROR,"Invalid tree structure" ) ;
            dumpIndexExtentIntoLog () ;
            rc = SDB_SYS ;
            goto error ;
         }
         childExtent = parent ;
         parent = parentExtent.getParent() ;
      }
      keyRID.reset() ;
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_ADVANCE, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__SETITNKEY, "_ixmExtent::_setInternalKey" )
   INT32 _ixmExtent::_setInternalKey (UINT16 pos, const dmsRecordID &rid,
                                      const ixmKey &key,
                                      const Ordering &order, dmsExtentID lchild,
                                      dmsExtentID rchild, ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__SETITNKEY );
      setChildExtentID ( pos, DMS_INVALID_EXTENT ) ;
      rc = _delKeyAtPos ( pos ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to delete key at pos" ) ;
         goto error ;
      }
      if ( getChildExtentID ( pos ) != rchild )
      {
         PD_LOG ( PDERROR, "rchild doesn't match" ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      setChildExtentID ( pos, lchild ) ;
      rc = insertHere ( pos, rid, key, order, lchild, rchild, indexCB ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to insert here" ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__SETITNKEY, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _ixmExtent::_doMergeChildren ( UINT16 pos, const Ordering &order,
                                        ixmIndexCB *indexCB, BOOLEAN &result )
   {
      INT32 rc = SDB_OK ;
      return rc ;
   }
   INT32 _ixmExtent::locate ( const BSONObj &key, const dmsRecordID &rid,
                              const Ordering &order, ixmRecordID &indexrid,
                              BOOLEAN &found, INT32 direction,
                              const ixmIndexCB *indexCB )
   {
      ixmKeyOwned ixkey ( key ) ;
      return _locate ( ixkey, rid, order, indexrid, found, direction, indexCB );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__LOCATE, "_ixmExtent::_locate" )
   INT32 _ixmExtent::_locate ( const ixmKey &key, const dmsRecordID &rid,
                               const Ordering &order, ixmRecordID &indexrid,
                               BOOLEAN &found, INT32 direction,
                               const ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__LOCATE );
      SDB_ASSERT ( 1 == direction || -1 == direction, "Invalid direction" ) ;
      UINT16 pos ;
      dmsExtentID childExtent ;
      rc = find ( indexCB, key, rid, order, pos, TRUE, found ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to find in locate" ) ;
         goto error ;
      }
      if ( found )
      {
         indexrid._extent = _me ;
         indexrid._slot = pos ;
         goto done ;
      }

      childExtent = getChildExtentID ( pos ) ;
      if ( DMS_INVALID_EXTENT != childExtent )
      {
         rc = ixmExtent(childExtent, _pIndexSu)._locate( key, rid, order,
                                                         indexrid, found,
                                                         direction, indexCB ) ;
         if ( rc )
         {
            goto error ;
         }
         if ( !indexrid.isNull() )
            goto done ;
      }
      if ( (direction<0 && 0==pos) || (direction>0 && getNumKeyNode()==pos) )
      {
         indexrid.reset() ;
      }
      else
      {
         indexrid._extent = _me ;
         indexrid._slot = direction<0?pos-1:pos ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__LOCATE, rc );
      return rc ;
   error :
      goto done ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_EXIST, "_ixmExtent::exists" )
   INT32 _ixmExtent::exists ( const ixmKey &key, const Ordering &order,
                             const ixmIndexCB *indexCB, BOOLEAN &result )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_EXIST );
      BOOLEAN found ;
      dmsRecordID dummyID ;
      ixmRecordID indexrid ;
      result = FALSE ;
      rc = _locate ( key, dummyID, order, indexrid, found, 1, indexCB );
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to locate key" ) ;
         goto error ;
      }
      while ( TRUE )
      {
         if ( indexrid.isNull() )
            break ;
         ixmExtent extent ( indexrid._extent, _pIndexSu ) ;
         const ixmKeyNode *kn = extent.getKeyNode(indexrid._slot) ;
         if ( kn->isUsed() )
         {
            result = ixmKey(extent.getKeyData(indexrid._slot)).woEqual(key) ;
            goto done ;
         }
         rc = extent.advance ( indexrid, 1 ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to advance" ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_EXIST, rc );
      return rc ;
   error :
      goto done ;
   }
#define IXM_GET_ROOT_MAX_LOOP 100
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_GETROOT, "_ixmExtent::getRoot" )
   dmsExtentID _ixmExtent::getRoot ()
   {
      PD_TRACE_ENTRY ( SDB__IXMEXT_GETROOT );
      dmsExtentID extentID = _me ;
      INT32 maxLoop = IXM_GET_ROOT_MAX_LOOP ;
      while ( DMS_INVALID_EXTENT != extentID &&
              maxLoop > 0 )
      {
         ixmExtent extent ( extentID, _pIndexSu ) ;
         if ( extent.isRoot() )
         {
            PD_TRACE_EXIT ( SDB__IXMEXT_GETROOT );
            return extentID ;
         }
         extentID = extent.getParent() ;
         maxLoop -- ;
      }
      PD_LOG ( PDERROR, "loop more than %d times to get root",
               IXM_GET_ROOT_MAX_LOOP ) ;
      PD_TRACE_EXIT ( SDB__IXMEXT_GETROOT );
      return DMS_INVALID_EXTENT ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_FNDSNG, "_ixmExtent::findSingle" )
   INT32 _ixmExtent::findSingle ( const ixmKey &key, const Ordering &order,
                                  dmsRecordID &rid, ixmIndexCB *indexCB )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_FNDSNG );
      BOOLEAN found ;
      dmsRecordID dummyID ;
      ixmRecordID indexrid ;
      rc = _locate ( key, dummyID, order, indexrid, found, 1, indexCB ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to locate key" ) ;
         goto error ;
      }
      while ( TRUE )
      {
         if ( indexrid.isNull() )
         {
            indexrid.reset() ;
            break ;
         }
         ixmExtent extent ( indexrid._extent, _pIndexSu ) ;
         const ixmKeyNode *kn = extent.getKeyNode(indexrid._slot) ;
         if ( kn->isUsed() )
         {
            if ( ixmKey(extent.getKeyData(indexrid._slot)).woCompare (
                        key, order ) != 0 )
            {
               rid.reset() ;
               goto done ;
            }
            rid = kn->_rid ;
            goto done ;
         }
         rc = extent.advance ( indexrid, 1 ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to advance" ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_FNDSNG, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_TRUNC, "_ixmExtent::truncate" )
   void _ixmExtent::truncate( ixmIndexCB *indexCB )
   {
      PD_TRACE_ENTRY ( SDB__IXMEXT_TRUNC );
      dmsExtentID childExtentID ;
      for ( INT32 i = (INT32)getNumKeyNode() ; i>=0; i-- )
      {
         childExtentID = getChildExtentID ((UINT16)i) ;
         if ( childExtentID != DMS_INVALID_EXTENT )
         {
            ixmExtent ( childExtentID, _pIndexSu ).truncate ( indexCB ) ;
            indexCB->freeExtent ( childExtentID ) ;
            setChildExtentID ( i, DMS_INVALID_EXTENT ) ;
         }
      }
      _pIndexSu->decStatFreeSpace( _extentHead->_mbID,
                                   _extentHead->_totalFreeSize ) ;
      _extentHead->_totalKeyNodeNum = 0 ;
      _extentHead->_beginFreeOffset = _pageSize-1 ;
      _extentHead->_totalFreeSize = _extentHead->_beginFreeOffset -
                       (sizeof(ixmExtentHead) +
                       (_extentHead->_totalKeyNodeNum*sizeof(ixmKeyNode))) ;
      _pIndexSu->addStatFreeSpace( _extentHead->_mbID,
                                   _extentHead->_totalFreeSize ) ;

      PD_TRACE_EXIT ( SDB__IXMEXT_TRUNC );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_IXMEXT_COUNT, "_ixmExtent::count" )
   UINT64 _ixmExtent::count ()
   {
      PD_TRACE_ENTRY ( SDB_IXMEXT_COUNT );
      UINT64 totalCount = 0 ;
      dmsExtentID childExtentID ;
      for ( INT32 i = (INT32)getNumKeyNode()-1; i>=0; i-- )
      {
         const ixmKeyNode *kn = getKeyNode(i) ;
         if ( kn->isUsed() )
         {
            totalCount ++ ;
         }
         childExtentID = getChildExtentID ((UINT16)i ) ;
         if ( childExtentID != DMS_INVALID_EXTENT )
            totalCount += ixmExtent(childExtentID, _pIndexSu).count() ;
      }
      if ( DMS_INVALID_EXTENT != _extentHead->_right )
         totalCount += ixmExtent(_extentHead->_right, _pIndexSu).count() ;
      PD_TRACE_EXIT ( SDB_IXMEXT_COUNT );
      return totalCount ;
   }

   BOOLEAN _ixmExtent::isStillValid( UINT16 mbID ) const
   {
      if ( IXM_EXTENT_EYECATCHER0 != _extentHead->_eyeCatcher[0] ||
           IXM_EXTENT_EYECATCHER1 != _extentHead->_eyeCatcher[1] )
      {
         return FALSE ;
      }
      else if ( _extentHead->_mbID != mbID )
      {
         return FALSE ;
      }
      else if ( DMS_EXTENT_FLAG_INUSE != _extentHead->_flag )
      {
         return FALSE ;
      }
      return TRUE ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_IXMEXT__KEYCMP, "_ixmExtent::_keyCmp" )
   INT32 _ixmExtent::_keyCmp ( const BSONObj &currentKey, const BSONObj &prevKey,
                               INT32 keepFieldsNum, BOOLEAN skipToNext,
                               const vector < const BSONElement *> &matchEle,
                               const vector < BOOLEAN > &matchInclusive,
                               const Ordering &o, INT32 direction )
   {
      PD_TRACE_ENTRY ( SDB_IXMEXT__KEYCMP );
      BSONObjIterator ll ( currentKey ) ;
      BSONObjIterator rr ( prevKey ) ;
      vector< const BSONElement *>::const_iterator eleItr = matchEle.begin() ;
      vector< BOOLEAN > ::const_iterator incItr = matchInclusive.begin() ;
      UINT32 mask = 1 ;
      INT32 retCode = 0 ;
      for ( INT32 i = 0 ; i < keepFieldsNum; ++i, mask<<=1 )
      {
         BSONElement curEle = ll.next() ;
         BSONElement prevEle = rr.next() ;
         ++eleItr ;
         ++incItr ;
         INT32 result = curEle.woCompare ( prevEle, FALSE ) ;
         if ( o.descending ( mask ))
            result = -result ;
         if ( result )
         {
            retCode = result ;
            goto done ;
         }
      }
      if ( skipToNext )
      {
         retCode = -direction ;
         goto done ;
      }
      for ( ; ll.more(); mask<<=1 )
      {
         BSONElement curEle = ll.next() ;
         BSONElement prevEle = **eleItr ;
         ++eleItr ;
         INT32 result = curEle.woCompare ( prevEle, FALSE ) ;
         if ( o.descending ( mask ))
            result = -result ;
         if ( result )
         {
            retCode = result ;
            goto done ;
         }
         if ( !*incItr )
         {
            retCode = -direction ;
            goto done ;
         }
         ++incItr ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_IXMEXT__KEYCMP, retCode );
      return retCode ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT__KEYFIND, "_ixmExtent::_keyFind" )
   INT32 _ixmExtent::_keyFind ( UINT16 l, UINT16 h, const BSONObj &prevKey,
                                INT32 keepFieldsNum, BOOLEAN skipToNext,
                                const vector < const BSONElement *> &matchEle,
                                const vector < BOOLEAN > &matchInclusive,
                                const Ordering &o, INT32 direction,
                                ixmRecordID &bestIxmRID,
                                dmsExtentID &resultExtent, _pmdEDUCB *cb )
   {
      SINT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT__KEYFIND );
      monAppCB * pMonAppCB = cb ? cb->getMonAppCB() : NULL ;
      SDB_ASSERT ( l <= h, "low must be less than high" ) ;
      DMS_MON_OP_COUNT_INC( pMonAppCB, MON_INDEX_READ, 1 ) ;
      while ( TRUE )
      {
         if ( l+1 >= h )
         {
            bestIxmRID._extent = _me ;
            bestIxmRID._slot = (direction>0)?h:l ;
            resultExtent = getChildExtentID(h) ;
            goto done ;
         }
         UINT16 m = (h+l)/2 ;
         CHAR *data = getKeyData ( m ) ;
         if ( !data )
         {
            PD_LOG ( PDERROR, "slot %d doesn't have matching key", m ) ;
            dumpIndexExtentIntoLog () ;
            rc = SDB_SYS ;
            goto error ;
         }
         INT32 r = _keyCmp ( ixmKey(data).toBson(), prevKey, keepFieldsNum,
                             skipToNext, matchEle, matchInclusive, o, direction);
         if ( r < 0 )
            l = m ;
         else if ( r > 0 )
            h = m ;
         else
         {
            if ( direction < 0 )
               l = m ;
            else
               h = m ;
         }
      } // while ( TRUE )
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT__KEYFIND, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_KEYLOCATE, "_ixmExtent::keyLocate" )
   INT32 _ixmExtent::keyLocate ( ixmRecordID &rid, const BSONObj &prevKey,
                                 INT32 keepFieldsNum, BOOLEAN skipToNext,
                                 const vector < const BSONElement *> &matchEle,
                                 const vector < BOOLEAN > &matchInclusive,
                                 const Ordering &o, INT32 direction,
                                 _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_KEYLOCATE );
      UINT16 l, h, z ;
      CHAR *data = NULL ;
      INT32 result ;
      dmsExtentID childExtentID ;
      SDB_ASSERT ( direction == 1 || direction == -1, "direction must be "
                   "either 1 or -1" ) ;
      if ( 0 == getNumKeyNode() )
      {
         rid.reset() ;
         goto done ;
      }

      l = 0 ;
      h = getNumKeyNode() - 1 ;
      z = (1-direction)/2*h ;
      data = getKeyData ( z ) ;
      if ( !data )
      {
         PD_LOG ( PDERROR, "slot %d doesn't have matching key", z ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      result = _keyCmp ( ixmKey(data).toBson(), prevKey, keepFieldsNum,
                         skipToNext, matchEle, matchInclusive, o, direction ) ;

      if ( direction * result >= 0 )
      {
         rid._extent = _me ;
         rid._slot   = z ;
         if ( direction > 0 )
         {
            childExtentID = getChildExtentID(0) ;
         }
         else
         {
            childExtentID = _extentHead->_right ;
         }
         if ( DMS_INVALID_EXTENT != childExtentID )
         {
            ixmExtent nextExtent ( childExtentID, _pIndexSu ) ;
            rc = nextExtent.keyLocate ( rid, prevKey, keepFieldsNum, skipToNext,
                                        matchEle, matchInclusive, o, direction,
                                        cb );
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to run keyLocate from extent %d",
                        childExtentID ) ;
               goto error ;
            }
         }
         goto done ;
      }

      data = getKeyData ( h-z ) ;
      if ( !data )
      {
         PD_LOG ( PDERROR, "slot %d doesn't have matching key", z ) ;
         dumpIndexExtentIntoLog () ;
         rc = SDB_SYS ;
         goto error ;
      }
      result = _keyCmp ( ixmKey(data).toBson(), prevKey, keepFieldsNum,
                         skipToNext, matchEle, matchInclusive, o, direction ) ;
      if ( direction * result < 0 )
      {
         if ( direction > 0 )
         {
            childExtentID = _extentHead->_right ;
         }
         else
         {
            childExtentID = getChildExtentID(0) ;
         }
         if ( DMS_INVALID_EXTENT != childExtentID )
         {
            ixmExtent nextExtent ( childExtentID, _pIndexSu ) ;
            rc = nextExtent.keyLocate ( rid, prevKey, keepFieldsNum, skipToNext,
                                        matchEle, matchInclusive, o, direction,
                                        cb );
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to run keyLocate from extent %d",
                        childExtentID ) ;
               goto error ;
            }
         }
         goto done ;
      }

      rc = _keyFind ( l, h, prevKey, keepFieldsNum, skipToNext, matchEle,
                      matchInclusive, o, direction, rid, childExtentID, cb ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to run keyFind from extent %d", _me ) ;
         goto error ;
      }
      if ( DMS_INVALID_EXTENT != childExtentID )
      {
         ixmExtent nextExtent ( childExtentID, _pIndexSu ) ;
         rc = nextExtent.keyLocate ( rid, prevKey, keepFieldsNum, skipToNext,
                                     matchEle, matchInclusive, o, direction,
                                     cb ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to run keyLocate from extent %d",
                     childExtentID ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_KEYLOCATE, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_KEYADVANCE, "_ixmExtent::keyAdvance" )
   INT32 _ixmExtent::keyAdvance ( ixmRecordID &rid, const BSONObj &prevKey,
                                 INT32 keepFieldsNum, BOOLEAN skipToNext,
                                 const vector < const BSONElement *> &matchEle,
                                 const vector < BOOLEAN > &matchInclusive,
                                 const Ordering &o, INT32 direction,
                                 _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_KEYADVANCE );
      UINT16 l, h ;
      BOOLEAN currentLevel ;
      dmsExtentID childExtentID, parentExtentID ;
      CHAR *data ;
      if ( direction > 0 )
      {
         l = rid.isNull()?(0):rid._slot ;
         h = getNumKeyNode() -1 ;
         data = getKeyData ( h ) ;
         if ( !data )
         {
            PD_LOG ( PDERROR, "slot %d doesn't have matching key", h ) ;
            dumpIndexExtentIntoLog () ;
            rc = SDB_SYS ;
            goto error ;
         }
         currentLevel = ( _keyCmp ( ixmKey(data).toBson(), prevKey,
                                    keepFieldsNum, skipToNext, matchEle,
                                    matchInclusive, o, direction) >= 0 ) ;
      }
      else
      {
         l = 0 ;
         h = rid.isNull()?(getNumKeyNode()-1):rid._slot ;
         data = getKeyData ( l ) ;
         if ( !data )
         {
            PD_LOG ( PDERROR, "slot %d doesn't have matching key", l ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         currentLevel = ( _keyCmp ( ixmKey(data).toBson(), prevKey,
                                    keepFieldsNum, skipToNext, matchEle,
                                    matchInclusive, o, direction) <= 0 ) ;
      }
      if ( currentLevel )
      {
         rc = _keyFind ( l, h, prevKey, keepFieldsNum, skipToNext,
                         matchEle, matchInclusive, o, direction,
                         rid, childExtentID, cb ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to keyFind in extent %d", _me ) ;
            goto error ;
         }
         if ( DMS_INVALID_EXTENT == childExtentID )
            goto done ;
         {
            ixmExtent childExtent ( childExtentID, _pIndexSu ) ;
            rc = childExtent.keyLocate ( rid, prevKey, keepFieldsNum,
                                         skipToNext, matchEle,
                                         matchInclusive, o, direction, cb ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to keyLocate in extent %d",
                        childExtentID ) ;
               goto error ;
            }
         }
      }
      else if ( (parentExtentID = getParent()) != DMS_INVALID_EXTENT )
      {
         rid.reset() ;
         ixmExtent parentExtent ( parentExtentID, _pIndexSu ) ;
         rc = parentExtent.keyAdvance ( rid, prevKey, keepFieldsNum,
                                        skipToNext, matchEle, matchInclusive,
                                        o, direction, cb ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to keyAdvance in extent %d",
                     parentExtentID ) ;
            goto error ;
         }
      }
      else
      {
         rid.reset() ;
         rc = keyLocate ( rid, prevKey, keepFieldsNum, skipToNext, matchEle,
                          matchInclusive, o, direction, cb ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to keyLocate in extent %d", _me ) ;
            goto error ;
         }
      }
   done :
      PD_TRACE_EXITRC ( SDB__IXMEXT_KEYADVANCE, rc );
      return rc ;
   error :
      goto done ;
   }
   ossValuePtr _ixmExtent::getChildExtentPtr ( UINT16 i )
   {
      return _pIndexSu->extentAddr(getChildExtentID(i)) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMEXT_DMPINXEXT2LOG, "_ixmExtent::dumpIndexExtentIntoLog" )
   INT32 _ixmExtent::dumpIndexExtentIntoLog ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMEXT_DMPINXEXT2LOG );
      INT32 indexExtentDumpBufferSize = 1024 * 1024 ;
      std::deque<dmsExtentID> childExtents ;
      CHAR *pBuffer = (CHAR*)SDB_OSS_MALLOC ( indexExtentDumpBufferSize ) ;
      PD_CHECK ( pBuffer, SDB_OOM, error, PDERROR,
                 "Failed to allocate memory for dump buffer" ) ;
      rc = dmsDump::dumpIndexExtent ( (CHAR*)_extentHead,
                                       _pageSize,
                                       pBuffer, indexExtentDumpBufferSize,
                                       NULL,
                                       DMS_SU_DMP_OPT_HEX |
                                       DMS_SU_DMP_OPT_HEX_WITH_ASCII |
                                       DMS_SU_DMP_OPT_HEX_PREFIX_AS_ADDR |
                                       DMS_SU_DMP_OPT_FORMATTED,
                                       childExtents,
                                       TRUE ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to dump index extent, rc = %d", rc ) ;
      PD_LOG ( PDERROR, "Index Page Dump:\n%s", pBuffer ) ;

   done :
      if ( pBuffer )
      {
         SDB_OSS_FREE ( pBuffer ) ;
      }
      PD_TRACE_EXITRC ( SDB__IXMEXT_DMPINXEXT2LOG, rc );
      return rc ;
   error :
      goto done ;
   }

}


