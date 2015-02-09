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

   Source File Name = rtnPredicate.hpp

   Descriptive Name = RunTime Access Plan Manager Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains structure for runtime
   predicate, which is records the predicates assigned by user.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef RTNPREDICATE_HPP__
#define RTNPREDICATE_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "../bson/bson.h"
#include <string>
#include <vector>
#include <map>
#include <set>
using namespace bson ;
using namespace std ;
namespace engine
{

   INT32 rtnKeyCompare ( const BSONElement &l, const BSONElement &r );

   class rtnKeyBoundary : public SDBObject
   {
   public :
      BSONElement _bound ;
      BOOLEAN _inclusive ;
      BOOLEAN operator==( const rtnKeyBoundary &r ) const
      {
         return _inclusive == r._inclusive &&
                _bound.woCompare ( r._bound ) == 0 ;
      }
      void flipInclusive ()
      {
         _inclusive = !_inclusive ;
      }
   } ;
   enum RTN_SSK_VALUE_POS
   {
      RTN_SSK_VALUE_POS_LT = -2,
      RTN_SSK_VALUE_POS_LET,
      RTN_SSK_VALUE_POS_WITHIN,
      RTN_SSK_VALUE_POS_GET,
      RTN_SSK_VALUE_POS_GT,
   } ;

   OSS_INLINE BOOLEAN rtnSSKPosAtEdge ( RTN_SSK_VALUE_POS pos )
   {
      return pos == RTN_SSK_VALUE_POS_LET ||
             pos == RTN_SSK_VALUE_POS_GET ;
   }

   enum RTN_SSK_RANGE_POS
   {
      RTN_SSK_RANGE_POS_LT = -3,
      RTN_SSK_RANGE_POS_LET,
      RTN_SSK_RANGE_POS_LI,
      RTN_SSK_RANGE_POS_CONTAIN,
      RTN_SSK_RANGE_POS_RI,
      RTN_SSK_RANGE_POS_RET,
      RTN_SSK_RANGE_POS_RT
   } ;

   OSS_INLINE BOOLEAN rtnSSKRangeAtEdge ( RTN_SSK_RANGE_POS pos )
   {
      return pos == RTN_SSK_RANGE_POS_LET ||
             pos == RTN_SSK_RANGE_POS_RET ;
   }

   class rtnStartStopKey : public SDBObject
   {
   public :
      rtnKeyBoundary _startKey ;
      rtnKeyBoundary _stopKey ;
      mutable INT32 _equality ;
      BOOLEAN isValid () const
      {
         INT32 result = _startKey._bound.woCompare ( _stopKey._bound, FALSE);
         return ( result<0 || (result == 0 && _startKey._inclusive &&
                               _stopKey._inclusive) ) ;
      }
      BOOLEAN isEquality () const
      {
         if ( -1 == _equality )
         {
            _equality = (_startKey._inclusive &&
                         _startKey == _stopKey)?
                        1:0 ;
         }
         return 1 == _equality ;
      }
      rtnStartStopKey()
      {
         _equality = -1 ;
      }
      rtnStartStopKey ( const BSONElement &e )
      {
         _startKey._bound = _stopKey._bound = e ;
         _startKey._inclusive = _stopKey._inclusive = TRUE ;
         _equality = 1 ;
      }
      rtnStartStopKey( INT32 imPossibleCondition )
      {
         _startKey._bound = _stopKey._bound = maxKey.firstElement() ;
         _startKey._inclusive = _stopKey._inclusive = FALSE ;
         _equality = 1 ;
      }
      string toString() const ;
      BSONObj toBson () const ;
      BOOLEAN fromBson ( BSONObj &ob ) ;
      void reset () ;
      RTN_SSK_VALUE_POS compare ( BSONElement &ele, INT32 dir ) const ;

      RTN_SSK_RANGE_POS compare ( rtnStartStopKey &key, INT32 dir ) const ;
   } ;

   class rtnPredicate : public SDBObject
   {
   private:
      BOOLEAN _isInitialized ;
      vector<BSONObj> _objData ;
      BSONObj addObj ( const BSONObj &o )
      {
         _objData.push_back(o) ;
         return o ;
      }
      void finishOperation ( const vector<rtnStartStopKey> &newkeys,
                             const rtnPredicate &other ) ;
   public:
      vector<rtnStartStopKey> _startStopKeys ;
      rtnPredicate ( )
      {
         rtnPredicate ( BSONObj().firstElement(), FALSE ) ;
      }
      rtnPredicate ( const BSONElement &e, BOOLEAN isNot ) ;
      ~rtnPredicate ()
      {
         _startStopKeys.clear() ;
      }
      const rtnPredicate &operator&= (const rtnPredicate &right) ;
      const rtnPredicate &operator|= (const rtnPredicate &right) ;
      const rtnPredicate &operator-= (const rtnPredicate &right) ;
      const rtnPredicate &operator= (const rtnPredicate &right) ;
      BOOLEAN operator<= ( const rtnPredicate &r ) const ;
      void reverse ( rtnPredicate &result ) const ;
      BOOLEAN isInit()
      {
         return _isInitialized ;
      }
      BSONElement min() const
      {
         if ( !isEmpty() )
         {
            return _startStopKeys[0]._startKey._bound ;
         }
         return BSONElement() ;
      }
      BSONElement max() const
      {
         if ( !isEmpty() )
         {
            return _startStopKeys[_startStopKeys.size()-1]._stopKey._bound ;
         }
         return BSONElement() ;
      }
      BOOLEAN minInclusive() const
      {
         if ( !isEmpty() )
         {
            return _startStopKeys[0]._startKey._inclusive ;
         }
         return FALSE ;
      }
      BOOLEAN maxInclusive() const
      {
         if ( !isEmpty() )
         {
            return _startStopKeys[_startStopKeys.size()-1]._stopKey._inclusive ;
         }
         return FALSE ;
      }
      BOOLEAN isEquality () const
      {
         return !isEmpty() && min().woCompare(max(), FALSE)== 0 &&
                 maxInclusive() && minInclusive() ;
      }
      BOOLEAN isEmpty() const
      {
         return _startStopKeys.empty() ;
      }
      BOOLEAN isGeneric() const
      {
         return !isEmpty() && _startStopKeys.size() == 1 &&
                 _startStopKeys[0]._startKey._inclusive &&
                 _startStopKeys[0]._stopKey._inclusive &&
                 _startStopKeys[0]._startKey._bound == minKey.firstElement() &&
                 _startStopKeys[0]._stopKey._bound == maxKey.firstElement() ;
      }
      string toString() const ;
   } ;

   class _rtnPredicateSet : public SDBObject
   {
   public:
      const rtnPredicate &predicate ( const CHAR *fieldName ) const ;
      const map<string, rtnPredicate> &predicates() const { return _predicates ; }
      INT32 matchLevelForIndex ( const BSONObj &keyPattern ) const ;
      INT32 addPredicate ( const CHAR *fieldName, const BSONElement &e,
                           BOOLEAN isNot ) ;
      void clear()
      {
         _predicates.clear() ;
      }
      string toString() const ;

   private:
      map<string, rtnPredicate> _predicates ;
   } ;
   typedef class _rtnPredicateSet rtnPredicateSet ;

   class _ixmIndexCB ;
   class _rtnPredicateListIterator ;
   class _rtnPredicateList : public SDBObject
   {
   public:
      _rtnPredicateList ( const rtnPredicateSet &predSet,
                          const _ixmIndexCB *indexCB,
                          int direction ) ;
      UINT32 size() { return _predicates.size(); }
      BSONObj startKey() const ;
      BSONObj endKey() const ;
      BSONObj obj() const ;
      BOOLEAN matchesKey ( const BSONObj &key ) const ;
      string toString() const ;
      INT32 getDirection() const
      {
         return _direction;
      }
      void setDirection ( INT32 dir )
      {
         _direction = dir ;
      }
   private :
      INT32 matchingLowElement ( const BSONElement &e, INT32 i,
                                 BOOLEAN direction,
                                 BOOLEAN &lowEquality ) const ;
      BOOLEAN matchesElement ( const BSONElement &e, INT32 i,
                               BOOLEAN direction ) const ;
      vector <rtnPredicate> _predicates ;
      INT32 _direction ;
      BSONObj _keyPattern ;
      friend class _rtnPredicateListIterator ;
   } ;
   typedef class _rtnPredicateList rtnPredicateList ;

   enum rtnPredicateCompareResult
   {
      MATCH = 0,
      LESS,
      GREATER
   } ;
   class _rtnPredicateListIterator : public SDBObject
   {
   private :
      const rtnPredicateList &_predList ;
      vector <const BSONElement *>_cmp ;
      vector <BOOLEAN> _inc ;
      vector <INT32>   _currentKey ;
      vector <INT32>   _prevKey ;
      BOOLEAN _after ;
   public :
      _rtnPredicateListIterator ( const rtnPredicateList &predList ) ;
      INT32 advance ( const BSONObj &curr ) ;
      const vector<const BSONElement *> &cmp() const { return _cmp ; }
      const vector<BOOLEAN> &inc() const { return _inc ; }
      void reset() ;
      BOOLEAN after() { return _after ; }
   private :
      rtnPredicateCompareResult
            validateCurrentStartStopKey ( INT32 keyIdx,
                                          const BSONElement &currElt,
                                          BOOLEAN reverse,
                                          BOOLEAN &hitUpperInclusive ) ;
      INT32 advanceToLowerBound ( INT32 i ) ;
      INT32 advancePast ( INT32 i ) ;
      INT32 advancePastZeroed ( INT32 i ) ;
   } ;
   typedef class _rtnPredicateListIterator rtnPredicateListIterator ;
}

#endif
