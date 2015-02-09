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

   Source File Name = rtnPredicate.cpp

   Descriptive Name = Runtime Predicate

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains Runtime code to generate
   predicate list from user's input.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnPredicate.hpp"
#include "mthMatcher.hpp"
#include "ixm.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include <sstream>
#include "../bson/util/builder.h"
namespace bson
{
   extern BSONObj staticNull ;
   extern BSONObj staticUndefined ;
   extern BSONObj minKey ;
   extern BSONObj maxKey ;
}
namespace engine
{
   class startStopKeyUnionBuilder : boost::noncopyable
   {
   public :
      startStopKeyUnionBuilder()
      {
         _init = FALSE ;
      }
      void next ( const rtnStartStopKey &n )
      {
         if ( !_init )
         {
            _tail = n ;
            _init = TRUE ;
            return ;
         }
         _merge(n) ;
      }
      void done()
      {
         if ( _init )
            _result.push_back ( _tail ) ;
      }
      const vector<rtnStartStopKey> &result() const
      {
         return _result ;
      }
   private:
      void _merge ( const rtnStartStopKey &n )
      {
         INT32 cmp = _tail._stopKey._bound.woCompare ( n._startKey._bound,
                                                       FALSE ) ;
         if ((cmp<0) || (cmp==0 && !_tail._stopKey._inclusive &&
                                   !n._startKey._inclusive ))
         {
            _result.push_back ( _tail ) ;
            _tail = n ;
            return ;
         }
         cmp = _tail._stopKey._bound.woCompare ( n._stopKey._bound, FALSE ) ;
         if ( (cmp<0) || (cmp==0 && !_tail._stopKey._inclusive &&
                                    !n._stopKey._inclusive ))
         {
            _tail._stopKey = n._stopKey ;
         }
      }
      BOOLEAN _init ;
      rtnStartStopKey _tail ;
      vector<rtnStartStopKey> _result ;
   } ;

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNSSKEY_TOSTRING, "rtnStartStopKey::toString" )
   string rtnStartStopKey::toString() const
   {
      PD_TRACE_ENTRY ( SDB_RTNSSKEY_TOSTRING ) ;
      StringBuilder buf ;
      buf << ( _startKey._inclusive ? "[":"(") << " " ;
      buf << _startKey._bound.toString( FALSE ) ;
      buf << ", " ;
      buf << _stopKey._bound.toString( FALSE ) ;
      buf << " " << (_stopKey._inclusive?"]":")") ;
      PD_TRACE_EXIT ( SDB_RTNSSKEY_TOSTRING ) ;
      return buf.str() ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNSSKEY_RESET, "rtnStartStopKey::reset" )
   void rtnStartStopKey::reset ()
   {
      PD_TRACE_ENTRY ( SDB_RTNSSKEY_RESET ) ;
      _startKey._bound = bson::minKey.firstElement() ;
      _startKey._inclusive = FALSE ;
      _stopKey._bound = bson::maxKey.firstElement() ;
      _stopKey._inclusive = FALSE ;
      PD_TRACE_EXIT ( SDB_RTNSSKEY_RESET ) ;
   }

#define RTN_START_STOP_KEY_START "a"
#define RTN_START_STOP_KEY_STOP  "o"
#define RTN_START_STOP_KEY_INCL  "i"
   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNSSKEY_FROMBSON, "rtnStartStopKey::fromBson" )
   BOOLEAN rtnStartStopKey::fromBson ( BSONObj &ob )
   {
      PD_TRACE_ENTRY ( SDB_RTNSSKEY_FROMBSON ) ;
      BOOLEAN ret = TRUE ;
      try
      {
         BSONElement startBE = ob.getField ( RTN_START_STOP_KEY_START ) ;
         BSONElement stopBE  = ob.getField ( RTN_START_STOP_KEY_STOP ) ;
         if ( startBE.type() != Object )
         {
            ret = FALSE ;
            goto done ;
         }
         if ( stopBE.type() != Object )
         {
            ret = FALSE ;
            goto done ;
         }
         BSONObj startKey = startBE.embeddedObject() ;
         _startKey._bound = startKey.firstElement () ;
         _startKey._inclusive = startKey.hasElement ( RTN_START_STOP_KEY_INCL );
         BSONObj stopKey = stopBE.embeddedObject() ;
         _stopKey._bound = stopKey.firstElement () ;
         _stopKey._inclusive = stopKey.hasElement ( RTN_START_STOP_KEY_INCL ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDWARNING, "Failed to extract rtnStartSTopKey from %s, %s",
                  ob.toString().c_str(), e.what() ) ;
         ret = FALSE ;
         goto done ;
      }
   done :
      PD_TRACE1 ( SDB_RTNSSKEY_FROMBSON, PD_PACK_INT ( ret ) ) ;
      PD_TRACE_EXIT ( SDB_RTNSSKEY_FROMBSON ) ;
      return ret ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNSSKEY_TOBSON, "rtnStartStopKey::toBson" )
   BSONObj rtnStartStopKey::toBson() const
   {
      PD_TRACE_ENTRY ( SDB_RTNSSKEY_TOBSON ) ;
      BSONObjBuilder ob ;
      {
         BSONObjBuilder startOB ;
         startOB.append ( _startKey._bound ) ;
         if ( _startKey._inclusive )
         {
            startOB.append ( RTN_START_STOP_KEY_INCL, TRUE ) ;
         }
         ob.append ( RTN_START_STOP_KEY_START,
                     startOB.obj () ) ;
      }
      {
         BSONObjBuilder stopOB ;
         stopOB.append ( _stopKey._bound ) ;
         if ( _stopKey._inclusive )
         {
            stopOB.append ( RTN_START_STOP_KEY_INCL, TRUE ) ;
         }
         ob.append ( RTN_START_STOP_KEY_STOP,
                     stopOB.obj () ) ;
      }
      PD_TRACE_EXIT ( SDB_RTNSSKEY_TOBSON ) ;
      return ob.obj () ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNKEYCOMPARE, "rtnKeyCompare" )
   INT32 rtnKeyCompare ( const BSONElement &l, const BSONElement &r )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_RTNKEYCOMPARE ) ;
      INT32 f = l.canonicalType() - r.canonicalType() ;
      if ( f != 0 )
      {
         rc = f ;
         goto done ;
      }
      rc = compareElementValues ( l, r ) ;
   done : 
      PD_TRACE_EXITRC ( SDB_RTNKEYCOMPARE, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNSSKEY_COMPARE1, "rtnStartStopKey::compare" )
   RTN_SSK_VALUE_POS rtnStartStopKey::compare ( BSONElement &ele,
                                                INT32 dir ) const
   {
      PD_TRACE_ENTRY ( SDB_RTNSSKEY_COMPARE1 ) ;
      INT32 posStart = 0 ;
      INT32 posStop  = 0 ;
      RTN_SSK_VALUE_POS pos ;
      SDB_ASSERT ( isValid (), "this object is not valid" ) ;
      if ( dir >= 0 )
         posStart = rtnKeyCompare ( ele, _startKey._bound ) ;
      else
         posStart = rtnKeyCompare ( ele, _stopKey._bound ) ;
      if ( posStart < 0 )
      {
         pos = RTN_SSK_VALUE_POS_LT ;
      }
      else if ( posStart == 0 )
      {
         if ( dir >= 0 )
            pos = _startKey._inclusive?
                    RTN_SSK_VALUE_POS_LET:RTN_SSK_VALUE_POS_LT ;
         else
            pos = _stopKey._inclusive?
                    RTN_SSK_VALUE_POS_LET:RTN_SSK_VALUE_POS_LT ;
      }
      if ( dir >= 0 )
         posStop  = rtnKeyCompare ( ele, _stopKey._bound ) ;
      else
         posStop = rtnKeyCompare ( ele, _startKey._bound ) ;
      if ( posStart > 0 && posStop < 0 )
      {
         pos = RTN_SSK_VALUE_POS_WITHIN ;
      }
      else if ( posStop == 0 )
      {
         if ( dir >= 0 )
            pos = _stopKey._inclusive?
                     RTN_SSK_VALUE_POS_GET:RTN_SSK_VALUE_POS_GT ;
         else
            pos = _startKey._inclusive?
                     RTN_SSK_VALUE_POS_GET:RTN_SSK_VALUE_POS_GT ;
      }
      else
      {
         pos = RTN_SSK_VALUE_POS_GT ;
      }
      if ( dir < 0 )
         pos = (RTN_SSK_VALUE_POS)(((INT32)pos) * -1) ;
      PD_TRACE_EXIT ( SDB_RTNSSKEY_COMPARE1 ) ;
      return pos ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNSSKEY_COMPARE2, "rtnStartStopKey::compare" )
   RTN_SSK_RANGE_POS rtnStartStopKey::compare ( rtnStartStopKey &key,
                                                INT32 dir ) const
   {
      PD_TRACE_ENTRY ( SDB_RTNSSKEY_COMPARE2 ) ;
      RTN_SSK_VALUE_POS posStart ;
      RTN_SSK_VALUE_POS posStop ;
      RTN_SSK_RANGE_POS pos = RTN_SSK_RANGE_POS_RT ;
      SDB_ASSERT ( key.isValid (), "key is not valid!" ) ;
      if ( dir >= 0 )
      {
         posStart = compare ( key._startKey._bound, 1 ) ;
         posStop = compare ( key._stopKey._bound, 1 ) ;
      }
      else
      {
         posStart = compare ( key._stopKey._bound, -1 ) ;
         posStop = compare ( key._startKey._bound, -1 ) ;
      }
      if ( RTN_SSK_VALUE_POS_LT == posStart )
      {
         if ( RTN_SSK_VALUE_POS_LT == posStop )
         {
            pos = RTN_SSK_RANGE_POS_LT ;
         }
         else if ( RTN_SSK_VALUE_POS_LET == posStop )
         {
            if ( dir >= 0 )
               pos = key._stopKey._inclusive?
                   RTN_SSK_RANGE_POS_LET:RTN_SSK_RANGE_POS_LT ;
            else
               pos = key._startKey._inclusive?
                   RTN_SSK_RANGE_POS_LET:RTN_SSK_RANGE_POS_LT ;
         }
         else if ( RTN_SSK_VALUE_POS_WITHIN == posStop )
         {
            pos = RTN_SSK_RANGE_POS_LI ;
         }
         pos = RTN_SSK_RANGE_POS_CONTAIN ;
      }
      else if ( RTN_SSK_VALUE_POS_LET == posStart )
      {
         if ( RTN_SSK_VALUE_POS_LET == posStop )
         {
            pos = RTN_SSK_RANGE_POS_LET ;
         }
         pos = RTN_SSK_RANGE_POS_CONTAIN ;
      }
      else if ( RTN_SSK_VALUE_POS_WITHIN == posStart )
      {
         if ( RTN_SSK_VALUE_POS_GT == posStop )
         {
            pos = RTN_SSK_RANGE_POS_RI ;
         }
         pos = RTN_SSK_RANGE_POS_CONTAIN ;
      }
      else if ( RTN_SSK_VALUE_POS_GET == posStart )
      {
         if ( dir >= 0 )
            pos = key._startKey._inclusive?
                   RTN_SSK_RANGE_POS_RET:RTN_SSK_RANGE_POS_RT ;
         else
            pos = key._stopKey._inclusive?
                   RTN_SSK_RANGE_POS_RET:RTN_SSK_RANGE_POS_RT ;
      }
      PD_TRACE_EXIT ( SDB_RTNSSKEY_COMPARE2 ) ;
      return pos ;
   }


   PD_TRACE_DECLARE_FUNCTION ( SDB_SIMAPLEREGEX1, "simpleRegex" )
   string simpleRegex ( const CHAR* regex,
                        const CHAR* flags,
                        BOOLEAN *purePrefix )
   {
      PD_TRACE_ENTRY ( SDB_SIMAPLEREGEX1 ) ;
      BOOLEAN extended = FALSE ;
      string r = "";
      stringstream ss;
      if (purePrefix)
         *purePrefix = false;

      BOOLEAN multilineOK;
      if ( regex[0] == '\\' && regex[1] == 'A')
      {
         multilineOK = TRUE ;
         regex += 2;
      }
      else if (regex[0] == '^')
      {
          multilineOK = FALSE ;
          regex += 1;
      }
      else
      {
          goto done ;
      }

      while (*flags)
      {
         switch (*(flags++))
         {
         case 'm': // multiline
            if (multilineOK)
               continue;
            else
               goto done ;
         case 'x': // extended
            extended = true;
            break;
         case 's':
            continue;
         default:
            goto done ; // cant use index
         }
      }

      while(*regex)
      {
         CHAR c = *(regex++);
         if ( c == '*' || c == '?' )
         {
            r = ss.str();
            r = r.substr( 0 , r.size() - 1 );
            goto done ; //breaking here fails with /^a?/
         }
         else if (c == '|')
         {
            r = string();
            goto done ;
         }
         else if (c == '\\')
         {
            c = *(regex++);
            if (c == 'Q')
            {
               while (*regex)
               {
                  c = (*regex++);
                  if (c == '\\' && (*regex == 'E'))
                  {
                     regex++; //skip the 'E'
                     break; // go back to start of outer loop
                  }
                  else
                  {
                     ss << c; // character should match itself
                  }
               }
            }
            else if ((c >= 'A' && c <= 'Z') ||
                     (c >= 'a' && c <= 'z') ||
                     (c >= '0' && c <= '0') ||
                     (c == '\0'))
            {
               r = ss.str();
               break;
            }
            else
            {
               ss << c;
            }
         } // else if (c == '\\')
         else if (strchr("^$.[()+{", c))
         {
            r = ss.str();
            break;
         }
         else if (extended && c == '#')
         {
            r = ss.str();
            break;
         }
         else if (extended && isspace(c))
         {
            continue;
         }
         else
         {
            ss << c;
         }
      }

      if ( r.empty() && *regex == 0 )
      {
         r = ss.str();
         if (purePrefix)
            *purePrefix = !r.empty();
      }
   done :
      PD_TRACE1 ( SDB_SIMAPLEREGEX1, PD_PACK_STRING ( r.c_str() ) ) ;
      PD_TRACE_EXIT ( SDB_SIMAPLEREGEX1 ) ;
      return r;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB_SIMAPLEREGEX2, "simpleRegex" )
   inline string simpleRegex(const BSONElement& e)
   {
      PD_TRACE_ENTRY ( SDB_SIMAPLEREGEX2 ) ;
      string r = "" ;
      switch(e.type())
      {
      case RegEx:
         r = simpleRegex(e.regex(), e.regexFlags(), NULL);
         break ;
      case Object:
      {
         BSONObj o = e.embeddedObject();
         r = simpleRegex(o["$regex"].valuestrsafe(),
                            o["$options"].valuestrsafe(),
                            NULL);
         break ;
      }
      default:
         break ;
      }
      PD_TRACE1 ( SDB_SIMAPLEREGEX2, PD_PACK_STRING ( r.c_str() ) ) ;
      PD_TRACE_EXIT ( SDB_SIMAPLEREGEX2 ) ;
      return r ;
   }

   string simpleRegexEnd( string regex )
   {
      ++regex[ regex.length() - 1 ];
      return regex;
   }


   rtnKeyBoundary maxKeyBound ( const rtnKeyBoundary &l,
                                const rtnKeyBoundary &r,
                                BOOLEAN startKey )
   {
      INT32 result = l._bound.woCompare ( r._bound, FALSE ) ;
      if ( result < 0  || (result == 0 &&
              (startKey?(!r._inclusive):(r._inclusive)) ) )
         return r ;
      return l ;
   }
   rtnKeyBoundary minKeyBound ( const rtnKeyBoundary &l,
                                const rtnKeyBoundary &r,
                                BOOLEAN stopKey )
   {
      INT32 result = l._bound.woCompare ( r._bound, FALSE ) ;
      if ( result > 0  || (result == 0 &&
              (stopKey?(!r._inclusive):(r._inclusive)) ))
         return r ;
      return l ;
   }
   // PD_TRACE_DECLARE_FUNCTION ( SDB_PREDOVERLAP, "predicatesOverlap" )
   BOOLEAN predicatesOverlap ( const rtnStartStopKey &l,
                               const rtnStartStopKey &r,
                               rtnStartStopKey &result )
   {
      PD_TRACE_ENTRY ( SDB_PREDOVERLAP ) ;
      result._startKey = maxKeyBound ( l._startKey, r._startKey, TRUE ) ;
      result._stopKey = minKeyBound ( l._stopKey, r._stopKey, TRUE ) ;
      PD_TRACE_EXIT ( SDB_PREDOVERLAP ) ;
      return result.isValid() ;
   }

   void rtnPredicate::finishOperation ( const vector<rtnStartStopKey> &newkeys,
                                        const rtnPredicate &other )
   {
      _startStopKeys = newkeys ;
      for ( vector<BSONObj>::const_iterator i = other._objData.begin() ;
            i != other._objData.end(); i++ )
      {
         _objData.push_back(*i) ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPRED_OPEQU, "rtnPredicate::operator&=" )
   const rtnPredicate &rtnPredicate::operator&=
                      (const rtnPredicate &right)
   {
      PD_TRACE_ENTRY ( SDB_RTNPRED_OPEQU ) ;
      vector<rtnStartStopKey> newKeySet ;
      vector<rtnStartStopKey>::const_iterator i = _startStopKeys.begin() ;
      vector<rtnStartStopKey>::const_iterator j =
                        right._startStopKeys.begin() ;
      while ( i != _startStopKeys.end() && j != right._startStopKeys.end())
      {
         rtnStartStopKey overlap ;
         if ( predicatesOverlap ( *i, *j, overlap ) )
         {
            newKeySet.push_back ( overlap ) ;
         }
         if ( i->_stopKey == minKeyBound ( i->_stopKey, j->_stopKey, TRUE ) )
         {
            ++i ;
         }
         else
         {
            ++j ;
         }
      }
      finishOperation ( newKeySet, right ) ;
      PD_TRACE_EXIT ( SDB_RTNPRED_OPEQU ) ;
      return *this ;
   }

   const rtnPredicate &rtnPredicate::operator=
                      (const rtnPredicate &right)
   {
      finishOperation(right._startStopKeys, right) ;
      _isInitialized = right._isInitialized ;
      return *this ;
   }

   PD_TRACE_DECLARE_FUNCTION (SDB_RTNPRED_OPOREQ, "rtnPredicate::operator|=" )
   const rtnPredicate &rtnPredicate::operator|=
                      (const rtnPredicate &right)
   {
      PD_TRACE_ENTRY ( SDB_RTNPRED_OPOREQ ) ;
      startStopKeyUnionBuilder b ;
      vector<rtnStartStopKey>::const_iterator i = _startStopKeys.begin() ;
      vector<rtnStartStopKey>::const_iterator j =
                        right._startStopKeys.begin() ;
      while ( i != _startStopKeys.end() && j != right._startStopKeys.end())
      {
         INT32 cmp = i->_startKey._bound.woCompare ( j->_startKey._bound,
                                                     FALSE ) ;
         if ( cmp < 0 || (cmp==0 && i->_startKey._inclusive ))
         {
            b.next(*i++) ;
         }
         else
         {
            b.next(*j++) ;
         }
      }
      while ( i!= _startStopKeys.end() )
      {
         b.next(*i++) ;
      }
      while ( j!= right._startStopKeys.end())
      {
         b.next(*j++) ;
      }
      b.done () ;
      finishOperation ( b.result(), right ) ;
      PD_TRACE_EXIT ( SDB_RTNPRED_OPOREQ ) ;
      return *this ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPRED_OPMINUSEQ, "rtnPredicate::operator-=" )
   const rtnPredicate &rtnPredicate::operator-=
                      (const rtnPredicate &right)
   {
      PD_TRACE_ENTRY ( SDB_RTNPRED_OPMINUSEQ ) ;
      vector<rtnStartStopKey> newKeySet ;
      vector<rtnStartStopKey>::iterator i = _startStopKeys.begin() ;
      vector<rtnStartStopKey>::const_iterator j =
                        right._startStopKeys.begin() ;
      while ( i != _startStopKeys.end() && j != right._startStopKeys.end())
      {
         INT32 cmp = i->_startKey._bound.woCompare ( j->_startKey._bound,
                                                     FALSE ) ;
         if ( cmp < 0 || ( cmp==0 && i->_startKey._inclusive &&
                                    !j->_startKey._inclusive ) )
         {
            INT32 cmp1 = i->_stopKey._bound.woCompare ( j->_startKey._bound,
                                                        FALSE ) ;
            newKeySet.push_back (*i) ;
            if ( cmp1 < 0 )
            {
               ++i ;
            }
            else if ( cmp1 == 0 )
            {
               if ( newKeySet.back()._stopKey._inclusive &&
                    j->_startKey._inclusive )
               {
                  newKeySet.back()._stopKey._inclusive = FALSE ;
               }
               ++i ;
            }
            else
            {
               newKeySet.back()._stopKey = j->_startKey ;
               newKeySet.back()._stopKey.flipInclusive() ;
               INT32 cmp2 = i->_stopKey._bound.woCompare (
                               j->_stopKey._bound, FALSE ) ;
               if ( cmp2 < 0 || ( cmp2 == 0 && (
                         !i->_stopKey._inclusive ||
                          j->_stopKey._inclusive )))
               {
                  ++i ;
               }
               else
               {
                  i->_startKey = j->_stopKey ;
                  i->_startKey.flipInclusive() ;
                  ++j ;
               }
            }
         }
         else
         {
            INT32 cmp1 = i->_startKey._bound.woCompare ( j->_stopKey._bound,
                                                         FALSE ) ;
            if ( cmp1 > 0 || (cmp1 == 0 && (!i->_stopKey._inclusive ||
                                            !j->_stopKey._inclusive )))
            {
               ++j ;
            }
            else
            {
               INT32 cmp2 = i->_stopKey._bound.woCompare (
                                                        j->_stopKey._bound,
                                                        FALSE ) ;
               if ( cmp2 < 0 ||
                    (cmp2 == 0 && ( !i->_stopKey._inclusive ||
                                     j->_stopKey._inclusive )))
               {
                  ++i ;
               }
               else
               {
                  i->_startKey = j->_stopKey ;
                  i->_startKey.flipInclusive() ;
                  ++j ;
               }
            }
         }
      } // while ( i != _startStopKeys.end() && j !=
      while ( i != _startStopKeys.end() )
      {
         newKeySet.push_back (*i) ;
         ++i ;
      }
      finishOperation ( newKeySet, right ) ;
      PD_TRACE_EXIT ( SDB_RTNPRED_OPMINUSEQ ) ;
      return *this ;
   }
   BOOLEAN rtnPredicate::operator<= ( const rtnPredicate &r ) const
   {
      rtnPredicate temp = *this ;
      temp -= r ;
      return temp.isEmpty() ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPRED_RTNPRED, "rtnPredicate::rtnPredicate" )
   rtnPredicate::rtnPredicate ( const BSONElement &e, BOOLEAN isNot )
   {
      PD_TRACE_ENTRY ( SDB_RTNPRED_RTNPRED ) ;
      _isInitialized = FALSE ;
      INT32 op = e.getGtLtOp() ;
      if ( ( !isNot && !e.eoo() && e.type() != RegEx && op == BSONObj::opIN ) )
      {
         set<BSONElement, element_lt> vals ;
         vector<rtnPredicate> regexes ;
         BSONObjIterator i ( e.embeddedObject() ) ;

         if ( !i.more() )
         {
            vals.insert ( e ) ;
         }
         while ( i.more() )
         {
            BSONElement ie = i.next() ;
            if ( ie.type() == Object &&
                 ie.embeddedObject().firstElement().getGtLtOp() !=
                       BSONObj::opELEM_MATCH )
            {
               pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                       "$eleMatch is not allowed within $in" ) ;
               return ;
            }
            if ( ie.type() == RegEx )
            {
               regexes.push_back ( rtnPredicate ( ie, FALSE ) ) ;
               if ( !regexes.back().isInit() )
               {
                  pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                          "Failed to create regex predicate" ) ;
                  return ;
               }
            }
            else
            {
               vals.insert ( ie ) ;
            }
         }
         for ( set<BSONElement,element_lt>::const_iterator i = vals.begin();
               i!=vals.end(); i++ )
         {
            _startStopKeys.push_back ( rtnStartStopKey ( *i ) ) ;
         }
         for ( vector<rtnPredicate>::const_iterator i = regexes.begin();
               i!=regexes.end(); i++ )
         {
            *this |= *i ;
         }
         _isInitialized = TRUE ;
         return ;
      }

      _startStopKeys.push_back ( rtnStartStopKey() ) ;
      rtnStartStopKey &initial = _startStopKeys[0] ;
      BSONElement &startKey = initial._startKey._bound ;
      BOOLEAN &startKeyInclusive = initial._startKey._inclusive ;
      BSONElement &stopKey = initial._stopKey._bound ;
      BOOLEAN &stopKeyInclusive = initial._stopKey._inclusive ;
      startKey = bson::minKey.firstElement() ;
      startKeyInclusive = TRUE ;
      stopKey = bson::maxKey.firstElement() ;
      stopKeyInclusive = TRUE ;

      if ( e.eoo() )
      {
         _isInitialized = TRUE ;
         return ;
      }

      BOOLEAN existsSpec = FALSE ;
      if ( BSONObj::opEXISTS == op )
      {
         existsSpec = e.trueValue() ;
      }
      else if ( BSONObj::opISNULL == op )
      {
         existsSpec = !e.trueValue() ;
      }
      if ( e.type() == RegEx ||
           (e.type() == Object && !e.embeddedObject()["$regex"].eoo() ))
      {
         if ( op != BSONObj::Equality && op != BSONObj::opREGEX )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "Invalid regular expression operator" ) ;
            return ;
         }
         if ( !isNot )
         {
            const string r = simpleRegex(e) ;
            if ( r.size() )
            {
               startKey = addObj(BSON(""<<r)).firstElement() ;
               stopKey = addObj(BSON(""<<simpleRegexEnd(r))).firstElement() ;
               stopKeyInclusive = FALSE ;
            }
            if ( e.type() == RegEx )
            {
               BSONElement re = addObj(BSON(""<<e)).firstElement() ;
               _startStopKeys.push_back ( rtnStartStopKey ( re ) ) ;
            }
            else
            {
               BSONObj orig = e.embeddedObject() ;
               BSONObjBuilder b ;
               b.appendRegex("", orig["$regex"].valuestrsafe(),
                                 orig["$options"].valuestrsafe()) ;
               BSONElement re = addObj(b.obj()).firstElement() ;
               _startStopKeys.push_back ( rtnStartStopKey ( re ) ) ;
            }
         }
         _isInitialized = TRUE ;
         return ;
      }

      if ( isNot )
      {
         switch ( op )
         {
         case BSONObj::opALL:
         case BSONObj::opMOD:
         case BSONObj::opTYPE:
            _isInitialized = TRUE ;
            return ;
         case BSONObj::Equality:
            op = BSONObj::NE ;
            break ;
         case BSONObj::NE:
            op = BSONObj::Equality ;
            break ;
         case BSONObj::LT:
            op = BSONObj::GTE ;
            break ;
         case BSONObj::LTE:
            op = BSONObj::GT ;
            break ;
         case BSONObj::GT:
            op = BSONObj::LTE ;
            break ;
         case BSONObj::GTE:
            op = BSONObj::LT ;
            break ;
         case BSONObj::opEXISTS:
         case BSONObj::opISNULL:
            existsSpec = !existsSpec ;
            break ;
         default:
            break ;
         }
      }
      switch(op)
      {
      case BSONObj::Equality:
         if ( e.type() == Array )
         {
            BSONObjIterator i ( e.embeddedObject()) ;
            while ( i.more() )
            {
               BSONElement x = i.next() ;
               if ( x.type() == Object &&
                    x.embeddedObject().firstElement().getGtLtOp() ==
                    BSONObj::opELEM_MATCH )
                  continue ;
               if ( x.type() != RegEx )
               {
                  startKey = stopKey = x ;
                  break ;
               }
            }
         }
         else
         {
            startKey = stopKey = e ;
         }
         break ;
      case BSONObj::NE:
      {
         _startStopKeys.push_back ( rtnStartStopKey() ) ;
         _startStopKeys[0]._stopKey._bound = e ;
         _startStopKeys[0]._stopKey._inclusive = FALSE ;
         _startStopKeys[1]._startKey._bound = e ;
         _startStopKeys[1]._startKey._inclusive = FALSE ;
         _startStopKeys[1]._stopKey._bound = maxKey.firstElement() ;
         _startStopKeys[1]._stopKey._inclusive = TRUE ;
         break ;
      }
      case BSONObj::LT:
         stopKeyInclusive = FALSE ;
      case BSONObj::LTE:
         stopKey = e ;
         break ;
      case BSONObj::GT:
         startKeyInclusive = FALSE ;
      case BSONObj::GTE:
         startKey = e ;
         break ;
      case BSONObj::opALL:
      {
         if ( e.type() != Array )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "Must be array type for opALL" ) ;
            return ;
         }
         BSONObjIterator i ( e.embeddedObject()) ;
         while ( i.more() )
         {
            BSONElement x = i.next() ;
            if ( x.type() == Object &&
                 x.embeddedObject().firstElement().getGtLtOp() ==
                 BSONObj::opELEM_MATCH )
               continue ;
            if ( x.type() != RegEx )
            {
               startKey = stopKey = x ;
               break ;
            }
         }
         break ;
      }
      case BSONObj::opMOD:
      {
         {
            BSONObjBuilder b ;
            b.appendMinForType ( "", NumberDouble ) ;
            startKey = addObj(b.obj()).firstElement() ;
         }
         {
            BSONObjBuilder b ;
            b.appendMaxForType ( "", NumberDouble ) ;
            stopKey = addObj(b.obj()).firstElement() ;
         }
         break ;
      }
      case BSONObj::opTYPE:
      {
         BSONType t = (BSONType) e.numberInt() ;
         {
            BSONObjBuilder b ;
            b.appendMinForType ( "", t ) ;
            startKey = addObj(b.obj()).firstElement() ;
         }
         {
            BSONObjBuilder b ;
            b.appendMaxForType ( "", t ) ;
            stopKey = addObj(b.obj()).firstElement() ;
         }
         break ;
      }
      case BSONObj::opREGEX:
      case BSONObj::opOPTIONS:
      case BSONObj::opELEM_MATCH:
         break ;
      case BSONObj::opEXISTS:
         if ( !existsSpec )
         {
            startKey = stopKey = bson::staticUndefined.firstElement() ;
         }
         break ;
      case BSONObj::opISNULL:
         if ( !existsSpec )
         {
            _startStopKeys.push_back ( rtnStartStopKey() ) ;
            _startStopKeys[0]._startKey._bound =
                  _startStopKeys[0]._stopKey._bound =
                  bson::staticUndefined.firstElement() ;
            _startStopKeys[1]._startKey._bound =
                  _startStopKeys[1]._stopKey._bound =
                  bson::staticNull.firstElement() ;
            _startStopKeys[0]._startKey._inclusive =
                  _startStopKeys[0]._stopKey._inclusive =
                  TRUE ;
            _startStopKeys[1]._startKey._inclusive =
                  _startStopKeys[1]._stopKey._inclusive =
                  TRUE ;
         }
         break ;
      default:
         break ;
      }
      _isInitialized = TRUE ;
      PD_TRACE_EXIT ( SDB_RTNPRED_RTNPRED ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPRED_REVERSE, "rtnPredicate::reverse" )
   void rtnPredicate::reverse ( rtnPredicate &result ) const
   {
      PD_TRACE_ENTRY ( SDB_RTNPRED_REVERSE ) ;
      result._objData = _objData ;
      result._startStopKeys.clear() ;
      for ( vector<rtnStartStopKey>::const_reverse_iterator i =
            _startStopKeys.rbegin();
            i != _startStopKeys.rend(); ++i )
      {
         rtnStartStopKey temp ;
         temp._startKey = i->_stopKey ;
         temp._stopKey = i->_startKey ;
         result._startStopKeys.push_back ( temp ) ;
      }
      PD_TRACE_EXIT ( SDB_RTNPRED_REVERSE ) ;
   }
   
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNPRED_TOSTRING, "rtnPredicate::toString()" )
   string rtnPredicate::toString() const
   {
      PD_TRACE_ENTRY ( SDB_RTNPRED_TOSTRING ) ;
      StringBuilder buf ;
      buf << "{ " ;
      for ( vector<rtnStartStopKey>::const_iterator i =
                       _startStopKeys.begin();
            i != _startStopKeys.end();
            i++ )
      {
         buf << i->toString() << " " ;
      }
      buf << " }" ;
      PD_TRACE_EXIT ( SDB_RTNPRED_TOSTRING ) ;
      return buf.str() ;
   }

   string _rtnPredicateSet::toString() const
   {
      StringBuilder buf ;
      buf << "[ " ;
      map<string, rtnPredicate>::const_iterator it = _predicates.begin() ;
      while ( it != _predicates.end() )
      {
         buf << it->second.toString() << " " ;
         ++it ;
      }
      buf << " ]" ;
      return buf.str() ;
   }

   static rtnPredicate *genericPredicate = NULL ;
   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPRED_PRED, "_rtnPredicateSet::predicate" )
   const rtnPredicate &_rtnPredicateSet::predicate (const CHAR *fieldName) const
   {
      PD_TRACE_ENTRY ( SDB__RTNPRED_PRED ) ;
      map<string, rtnPredicate>::const_iterator f = _predicates.find(fieldName);
      if ( _predicates.end() == f )
      {
         if ( !genericPredicate )
            genericPredicate = SDB_OSS_NEW rtnPredicate
                                     (BSONObj().firstElement(),FALSE);
         return *genericPredicate ;
      }
      PD_TRACE_EXIT ( SDB__RTNPRED_PRED ) ;
      return f->second ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDSET_MALEFORINDEX, "_rtnPredicateSet::matchLevelForIndex" )
   INT32 _rtnPredicateSet::matchLevelForIndex (const BSONObj &keyPattern) const
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDSET_MALEFORINDEX ) ;
      INT32 level = 0 ;
      BSONObjIterator i ( keyPattern ) ;
      while ( i.more() )
      {
         BSONElement e = i.next() ;
         if ( predicate(e.fieldName()).isGeneric() )
            return level ;
      }
      PD_TRACE_EXIT ( SDB__RTNPREDSET_MALEFORINDEX ) ;
      return level ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDSET_ADDPRED, "_rtnPredicateSet::addPredicate" )
   INT32 _rtnPredicateSet::addPredicate ( const CHAR *fieldName,
                                          const BSONElement &e,
                                          BOOLEAN isNot )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__RTNPREDSET_ADDPRED ) ;
      map<string, rtnPredicate>::const_iterator f ;
      rtnPredicate pred ( e, isNot ) ;
      if ( !pred.isInit() )
      {
         PD_LOG ( PDERROR, "Failed to add predicate %s: %s",
                  fieldName, e.toString().c_str()) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      f = _predicates.find(fieldName);
      if ( _predicates.end() == f )
      {
         if ( !genericPredicate )
            genericPredicate = SDB_OSS_NEW rtnPredicate
                                     (BSONObj().firstElement(),FALSE);
         _predicates[fieldName] = *genericPredicate ;
      }
      _predicates[fieldName] &= pred ;
   done :
      PD_TRACE_EXITRC ( SDB__RTNPREDSET_ADDPRED, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLIST__RTNPREDLIST, "_rtnPredicateList::_rtnPredicateList" )
   _rtnPredicateList::_rtnPredicateList ( const rtnPredicateSet &predSet,
                                          const _ixmIndexCB *indexCB,
                                          INT32 direction )
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLIST__RTNPREDLIST ) ;
      _direction = direction>0?1:-1 ;
      _keyPattern = indexCB->keyPattern() ;
      static rtnStartStopKey s_emptyStartStopKey( 0 ) ;
      BSONObjIterator i(_keyPattern) ;
      while ( i.more() )
      {
         BSONElement e = i.next() ;
         const rtnPredicate &pred = predSet.predicate ( e.fieldName() ) ;
         if ( pred.isEmpty() )
         {
            rtnPredicate emptyPred ;
            emptyPred._startStopKeys.push_back( s_emptyStartStopKey ) ;
            _predicates.push_back( emptyPred ) ;
            break ;
         }
         INT32 num = (INT32)e.number() ;
         BOOLEAN forward = ((num>=0?1:-1)*(direction>=0?1:-1)>0) ;
         if ( forward )
         {
            _predicates.push_back ( pred ) ;
         }
         else
         {
            _predicates.push_back ( rtnPredicate () ) ;
            pred.reverse ( _predicates.back() ) ;
         }
      }
      PD_TRACE_EXIT ( SDB__RTNPREDLIST__RTNPREDLIST ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLIST_STARTKEY, "_rtnPredicateList::startKey" )
   BSONObj _rtnPredicateList::startKey() const
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLIST_STARTKEY ) ;
      BSONObjBuilder b ;
      for ( vector<rtnPredicate>::const_iterator i = _predicates.begin() ;
            i != _predicates.end(); i++ )
      {
         const rtnStartStopKey &key = i->_startStopKeys.front() ;
         b.appendAs ( key._startKey._bound, "" ) ;
      }
      PD_TRACE_EXIT ( SDB__RTNPREDLIST_STARTKEY ) ;
      return b.obj() ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLIST_ENDKEY, "_rtnPredicateList::endKey" )
   BSONObj _rtnPredicateList::endKey() const
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLIST_ENDKEY ) ;
      BSONObjBuilder b ;
      for ( vector<rtnPredicate>::const_iterator i = _predicates.begin() ;
            i != _predicates.end(); i++ )
      {
         const rtnStartStopKey &key = i->_startStopKeys.back() ;
         b.appendAs ( key._stopKey._bound, "" ) ;
      }
      PD_TRACE_EXIT ( SDB__RTNPREDLIST_ENDKEY ) ;
      return b.obj() ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLIST_OBJ, "_rtnPredicateList::obj" )
   BSONObj _rtnPredicateList::obj() const
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLIST_OBJ ) ;
      BSONObjBuilder b ;
      for ( INT32 i = 0; i<(INT32)_predicates.size(); i++ )
      {
         BSONArrayBuilder a ( b.subarrayStart("") ) ;
         for ( vector<rtnStartStopKey>::const_iterator j =
               _predicates[i]._startStopKeys.begin() ;
               j != _predicates[i]._startStopKeys.end() ;
               j++ )
         {
            a << BSONArray ( BSON_ARRAY ( j->_startKey._bound <<
                       j->_stopKey._bound ).clientReadable() ) ;
         }
         a.done () ;
      }
      PD_TRACE_EXIT ( SDB__RTNPREDLIST_OBJ ) ;
      return b.obj() ;
   }
   string _rtnPredicateList::toString() const
   {
      return obj().toString(false, false) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLIST_MATLOWELE, "_rtnPredicateList::matchingLowElement" )
   INT32 _rtnPredicateList::matchingLowElement ( const BSONElement &e, INT32 i,
                                                BOOLEAN direction,
                                                BOOLEAN &lowEquality ) const
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLIST_MATLOWELE ) ;
      lowEquality = FALSE ;
      INT32 l = -1 ;
      INT32 h = _predicates[i]._startStopKeys.size() * 2 ;
      INT32 m = 0 ;
      while ( l + 1 < h )
      {
         m = ( l + h ) / 2 ;
         BSONElement toCmp ;
         BOOLEAN toCmpInclusive ;
         const rtnStartStopKey &startstopkey=_predicates[i]._startStopKeys[m/2];
         if ( 0 == m%2 )
         {
            toCmp = startstopkey._startKey._bound ;
            toCmpInclusive = startstopkey._startKey._inclusive ;
         }
         else
         {
            toCmp = startstopkey._stopKey._bound ;
            toCmpInclusive = startstopkey._stopKey._inclusive ;
         }
         INT32 result = toCmp.woCompare ( e, FALSE ) ;
         if ( !direction )
            result = -result ;
         if ( result < 0 )
            l = m ;
         else if ( result > 0 )
            h = m ;
         else
         {
            if ( 0 == m%2 )
               lowEquality = TRUE ;
            INT32 ret = m ;
            if ((0 == m%2 && !toCmpInclusive) ||
                (1 == m%2 && toCmpInclusive))
            {
               --ret ;
            }
            PD_TRACE1 ( SDB__RTNPREDLIST_MATLOWELE, PD_PACK_INT ( ret ) ) ;
            PD_TRACE_EXIT ( SDB__RTNPREDLIST_MATLOWELE ) ;
            return ret ;
         }
      }
      PD_TRACE1 ( SDB__RTNPREDLIST_MATLOWELE, PD_PACK_INT ( l ) ) ;
      PD_TRACE_EXIT ( SDB__RTNPREDLIST_MATLOWELE ) ;
      return l ;
   }
   BOOLEAN _rtnPredicateList::matchesElement ( const BSONElement &e, INT32 i,
                                               BOOLEAN direction ) const
   {
      BOOLEAN dummy ;
      return ( 0 == matchingLowElement ( e, i, direction, dummy )%2 ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLIST_MATKEY, "_rtnPredicateList::matchesKey" )
   BOOLEAN _rtnPredicateList::matchesKey ( const BSONObj &key ) const
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLIST_MATKEY ) ;
      BOOLEAN ret = TRUE ;
      BSONObjIterator j ( key ) ;
      BSONObjIterator k ( _keyPattern ) ;
      for ( INT32 l = 0; l < (INT32)_predicates.size(); ++l )
      {
         INT32 number = (INT32)k.next().number() ;
         BOOLEAN forward = (number>=0?1:-1)*(_direction>=0?1:-1)>0 ;
         if ( !matchesElement ( j.next(), l, forward))
         {
            ret = FALSE ;
            break ;
         }
      }
      PD_TRACE_EXIT ( SDB__RTNPREDLIST_MATKEY ) ;
      return ret ;
   }

   _rtnPredicateListIterator::_rtnPredicateListIterator ( const rtnPredicateList
                                                          &predList )
   :
   _predList(predList),
   _cmp ( _predList._predicates.size(), NULL ),
   _inc ( _predList._predicates.size(), FALSE ),
   _currentKey ( _predList._predicates.size(), -1 ),
   _prevKey ( _predList._predicates.size(), -1 )
   {
     reset() ;
     _after = FALSE ;
   }

   INT32 _rtnPredicateListIterator::advancePast ( INT32 i )
   {
      _after = TRUE ;
      return i ;
   }
   INT32 _rtnPredicateListIterator::advancePastZeroed ( INT32 i )
   {
      for ( INT32 j = i; j < (INT32)_currentKey.size(); ++j )
      {
         _currentKey[j] = 0 ;
      }
      _after = TRUE ;
      return i ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLISTITE_ADVTOLOBOU, "_rtnPredicateListIterator::advanceToLowerBound" )
   INT32 _rtnPredicateListIterator::advanceToLowerBound( INT32 i )
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLISTITE_ADVTOLOBOU ) ;
      _cmp[i] = &_predList._predicates[i]._startStopKeys[_currentKey[i]
                                                        ]._startKey._bound ;
      _inc[i] = _predList._predicates[i]._startStopKeys[_currentKey[i]
                                                       ]._startKey._inclusive ;
      for ( INT32 j = i+1; j < (INT32)_currentKey.size(); ++j )
      {
         _cmp[j] =
            &_predList._predicates[j]._startStopKeys.front()._startKey._bound ;
         _inc[j] =
          _predList._predicates[j]._startStopKeys.front()._startKey._inclusive ;
      }
      _after = FALSE ;
      PD_TRACE_EXIT ( SDB__RTNPREDLISTITE_ADVTOLOBOU ) ;
      return i ;
   }
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLISTITE_VALCURSSKEY, "_rtnPredicateListIterator::validateCurrentStartStopKey" )
   rtnPredicateCompareResult
     _rtnPredicateListIterator::validateCurrentStartStopKey ( INT32 keyIdx,
                                                const BSONElement &currElt,
                                                BOOLEAN reverse,
                                                BOOLEAN &hitUpperInclusive )
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLISTITE_VALCURSSKEY ) ;
      rtnPredicateCompareResult re = MATCH ;
      hitUpperInclusive = FALSE ;
      const rtnStartStopKey &key =_predList._predicates[keyIdx]._startStopKeys [
                                         _currentKey[keyIdx]] ;
      INT32 upperMatch = key._stopKey._bound.woCompare ( currElt, FALSE ) ;
      if ( reverse )
         upperMatch = -upperMatch ;
      if ( upperMatch == 0 && key._stopKey._inclusive )
      {
         hitUpperInclusive = TRUE ;
         re = MATCH ;
         goto done ;
      }
      if ( upperMatch <= 0 )
      {
         re = GREATER ;
         goto done ;
      }
      {
         INT32 lowerMatch = key._startKey._bound.woCompare ( currElt, FALSE ) ;
         if ( reverse )
            lowerMatch = -lowerMatch ;
         if ( lowerMatch == 0 && key._startKey._inclusive )
         {
            re = MATCH ;
            goto done ;
         }
         if ( lowerMatch >= 0 )
         {
            re = LESS ;
         }
      }
   done :
      PD_TRACE_EXIT ( SDB__RTNPREDLISTITE_VALCURSSKEY ) ;
      return re ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLISTITE_RESET, "_rtnPredicateListIterator::reset" )
   void _rtnPredicateListIterator::reset ()
   {
      PD_TRACE_ENTRY ( SDB__RTNPREDLISTITE_RESET ) ;
      for ( INT32 i = 0; i < (INT32)_currentKey.size(); ++i )
      {
         _cmp[i] =
            &_predList._predicates[i]._startStopKeys.front()._startKey._bound ;
         _inc[i] =
           _predList._predicates[i]._startStopKeys.front()._startKey._inclusive;
         _currentKey[i] = 0 ;
      }
      PD_TRACE_EXIT ( SDB__RTNPREDLISTITE_RESET ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__RTNPREDLISTITE_ADVANCE, "_rtnPredicateListIterator::advance" )
   INT32 _rtnPredicateListIterator::advance ( const BSONObj &curr )
   {
      INT32 rc = -1 ;
      PD_TRACE_ENTRY ( SDB__RTNPREDLISTITE_ADVANCE ) ;
      BSONObjIterator j ( curr ) ;
      BSONObjIterator o ( _predList._keyPattern ) ;
      INT32 latestNonEndPoint = -1 ;
      for ( INT32 i = 0; i < (INT32)_currentKey.size(); ++i )
      {
         if ( i>0 && !_predList._predicates[i-1]._startStopKeys[_currentKey[i-1]
                                                              ].isEquality() )
         {
            _currentKey[i] = -1 ;
         }
         BSONElement oo = o.next() ;
         BOOLEAN reverse = ((oo.number()<0)^(_predList._direction<0)) ;
         BSONElement jj = j.next() ;
         if ( -1 == _currentKey[i] )
         {
            BOOLEAN lowEquality ;
            INT32 l = _predList.matchingLowElement ( jj, i, !reverse,
                                                     lowEquality ) ;
            if ( 0 == l%2 )
            {
               _currentKey[i] = l/2 ;
               if ( ((INT32)_predList._predicates[i]._startStopKeys.size() >
                        _currentKey[i]+1) ||
                     (_predList._predicates[i]._startStopKeys.back().
                      _stopKey._bound.woCompare ( jj, FALSE ) != 0) )
               {
                  latestNonEndPoint = i ;
               }
               continue ;
            }
            else
            {
               if ( l ==
                 (INT32)_predList._predicates[i]._startStopKeys.size()*2-1 )
               {
                  if ( -1 == latestNonEndPoint )
                  {
                     rc = -2 ;
                     goto done ;
                  }
                  rc = advancePastZeroed ( latestNonEndPoint + 1 ) ;
                  goto done ;
               }
               _currentKey[i] = (l+1)/2 ;
               if ( lowEquality )
               {
                  rc = advancePast ( i+1 ) ;
                  goto done ;
               }
               rc = advanceToLowerBound(i) ;
               goto done ;
            }
         } // if ( -1 == _currentKey[i] )
         BOOLEAN eq = FALSE ;
         while ( _currentKey[i] <
                 (INT32)_predList._predicates[i]._startStopKeys.size())
         {
            rtnPredicateCompareResult compareResult =
               validateCurrentStartStopKey ( i, jj, reverse, eq ) ;
            if ( GREATER == compareResult ||
                 ( LESS == compareResult && _prevKey[i] == _currentKey[i] ) )
            {
               _currentKey[i]++ ;
               advancePastZeroed(i+1) ;
               continue ;
            }
            else if ( MATCH == compareResult )
               break ;
            else
            {
               rc = advanceToLowerBound(i) ;
               goto done ;
            }
         }
         INT32 diff = _predList._predicates[i]._startStopKeys.size() -
                      _currentKey[i] ;
         if ( diff > 1 || ( !eq && diff == 1 ) )
         {
            latestNonEndPoint = i ;
         }
         else if ( diff == 0 )
         {
            if ( -1 == latestNonEndPoint )
            {
               rc = -2 ;
               goto done ;
            }
            rc = advancePastZeroed ( latestNonEndPoint + 1 ) ;
            goto done ;
         }
      }
   done :
      for ( INT32 i = 0; i < (INT32)_currentKey.size(); ++i )
      {
         _prevKey[i] = _currentKey[i] ;
      }
      PD_TRACE_EXITRC ( SDB__RTNPREDLISTITE_ADVANCE, rc ) ;
      return rc ;
   }
}
