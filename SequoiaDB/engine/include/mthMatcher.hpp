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

   Source File Name = mthMatcher.hpp

   Descriptive Name = Method Matcher Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Method component. This file contains structure for matching
   operation, which is indicating whether a record matches a given matching
   rule.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef MTHMATCHER_HPP_
#define MTHMATCHER_HPP_
#include "core.hpp"
#include "oss.hpp"
#include "ossUtil.hpp"
#include "pd.hpp"
#include "rtnPredicate.hpp"
#include <vector>
#include <set>
#include <boost/shared_ptr.hpp>
#include <exception>
#include "../bson/bson.h"
#include "../bson/bsonobj.h"
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include "pcrecpp.h"
using namespace bson ;
using namespace pcrecpp ;
namespace engine
{
   struct element_lt
   {
      BOOLEAN operator()(const BSONElement& l, const BSONElement& r) const
      {
         INT32 x = (INT32) l.canonicalType() - (INT32) r.canonicalType() ;
         if ( x < 0 ) return TRUE ;
         else if ( x > 0 ) return FALSE ;
         return compareElementValues(l,r) < 0 ;
      }
   };
#define MTH_LOGIC_AND    0
#define MTH_LOGIC_OR     1
#define MTH_LOGIC_NOT    2
#define MTH_LOGIC_OTHER  3

#define MTH_OPERATOR_EYECATCHER '$'
#define MTH_OPERATOR_NOT        "not"
#define MTH_FIELDNAME_SEP       '.'

#define MTH_WEIGHT_EQUAL 100
#define MTH_WEIGHT_GT    150
#define MTH_WEIGHT_GTE   200
#define MTH_WEIGHT_LT    150
#define MTH_WEIGHT_LTE   200
#define MTH_WEIGHT_NE    500
#define MTH_WEIGHT_MOD   200
#define MTH_WEIGHT_TYPE  150
#define MTH_WEIGHT_IN    5000
#define MTH_WEIGHT_NIN   10000
#define MTH_WEIGHT_ALL   550
#define MTH_WEIGHT_SIZE  250
#define MTH_WEIGHT_EXISTS    250
#define MTH_WEIGHT_ELEMMATCH 450
#define MTH_WEIGHT_REGEX     1000000
#define MTH_WEIGHT_ISNULL 250

   class _mthMatcher : public SDBObject
   {
   private:
      class _REMatchElement : public SDBObject
      {
      private :
         OSS_INLINE BOOLEAN _isPureWords(const char* regex, 
                                         const char* flags)
         {
            BOOLEAN extended = FALSE;
            if(flags)
            {
               while (*flags)
               {
                  switch (*(flags++))
                  {
                  case 'm': // multiline
                  case 's':
                     continue;
                  case 'x': // extended
                     extended = TRUE;
                     continue;
                  default:
                     return FALSE ;
                  }
               }
            }
            if(regex)
            {
               while(*regex)
               {
                  CHAR c = *(regex++);
                  if( ossStrchr("|?*\\^$.[()+{", c) ||
                      ( ossStrchr("# ", c) && extended ))
                  {
                     return FALSE;
                  }
               } 
            }
            else
            {
               return FALSE;
            }
            return TRUE;
         }
         OSS_INLINE pcrecpp::RE_Options flags2options(const char* flags)
         {
            pcrecpp::RE_Options options;
            options.set_utf8(true);
            while ( flags && *flags )
            {
               if ( *flags == 'i' )
                  options.set_caseless(true);
               else if ( *flags == 'm' )
                  options.set_multiline(true);
               else if ( *flags == 'x' )
                  options.set_extended(true);
               else if ( *flags == 's' )
                  options.set_dotall(true);
               flags++;
            }
            return options;
         }
      public :
         const CHAR *_fieldName ;
         const CHAR *_regex ;
         const CHAR *_flags ;
         BOOLEAN _goSimpleMatch ;
         boost::shared_ptr<RE> _re ;
         _REMatchElement ( const BSONElement &e )
         {
            _fieldName = e.fieldName() ;
            _regex = e.regex() ;
            _flags = e.regexFlags() ;
            _goSimpleMatch = _isPureWords(_regex, _flags);
            _re.reset ( new RE(_regex, flags2options(_flags) )) ;
         }
         _REMatchElement ( const CHAR *fieldName, const CHAR *regex,
                           const CHAR *options )
         {
            _fieldName = fieldName ;
            _regex = regex ;
            _flags = options ;
            _goSimpleMatch = _isPureWords(_regex, _flags);
            _re.reset ( new RE(_regex, flags2options(_flags) )) ;
         }
      } ;
      typedef class _REMatchElement REMatchElement ;
      class _MatchElement : public SDBObject
      {
      public :
         BSONElement _toMatch ;
         BSONObj::MatchType _op ;
         set<BSONElement, element_lt> _myset ;
         vector<REMatchElement> _myregex ;
         BSONType _type ;
         INT32 _mod ;
         INT32 _modm ;

         _mthMatcher *_subMatcher ;

         _MatchElement ( const BSONElement &e, BSONObj::MatchType op ) ;
         _MatchElement ( const BSONElement &e, BSONObj::MatchType op,
                         const BSONObj &array ) :
         _subMatcher(NULL)
         {
            _toMatch = e ;
            _op = op ;
            BSONObjIterator it ( array ) ;
            while ( it.more() )
            {
               BSONElement ie = it.next() ;
               /*if ( ie.type() == Object )
               {
                  throw pdGeneralException (
                        "match array can't contain object, ignore" ) ;
               }
               else*/ if ( ie.type() == RegEx )
                  _myregex.push_back ( REMatchElement(ie) ) ;
               else
                  _myset.insert(ie) ;
            }
         }
         ~_MatchElement()
         {
            _myset.clear() ;
            _myregex.clear() ;
            if ( _subMatcher )
            {
               SDB_OSS_DEL _subMatcher ;
               _subMatcher = NULL ;
            }
         }
      } ;
      typedef class _MatchElement MatchElement ;
      class _LogicMatchElement : public SDBObject
      {
      public :
         UINT32 _weight ;
         INT32 _logicType ;
         BOOLEAN _isRegex ;
         BOOLEAN _isFieldCom ;
         BOOLEAN _matchAll ;
         vector<_LogicMatchElement *> _vlme ;
         _MatchElement *_me ;
         _REMatchElement *_rme ;
         _LogicMatchElement ()
         {
            _weight = 0 ;
            _logicType = MTH_LOGIC_AND ;
            _isRegex = FALSE ;
            _isFieldCom = FALSE ;
            _matchAll = FALSE ;
            _me = NULL ;
            _rme = NULL ;
         }
      } ;
      typedef class _LogicMatchElement LogicMatchElement ;
      BSONObj _matchPattern ;
      BOOLEAN _initialized ;
      BOOLEAN _matchesAll ;
      rtnPredicateSet _predicateSet ;
      LogicMatchElement *_rlme;
      vector<BSONObjBuilder*> _builderList ;

      BOOLEAN _totallyConverted ;

      INT32 _createLME ( LogicMatchElement *lme,
                         LogicMatchElement **clme,
                         INT32   logicType,
                         BOOLEAN isRegex = FALSE
                        ) ;
      enum _MTH_MATCHER_FIELD_TYPE
      {
         MTH_MATCHER_FIELD_NOT = 0,
         MTH_MATCHER_FIELD_EQU,
         MTH_MATCHER_FIELD_OTH
      } ;
      INT32 _parseElement ( const BSONElement &ele,
                            LogicMatchElement *lme,
                            BOOLEAN predicatable,
                            BOOLEAN isNot = FALSE
                          ) ;
      INT32 _addOperator ( const BSONElement &ele,
                           const BSONElement &embEle,
                           const CHAR *&regex,
                           const CHAR *&options,
                           LogicMatchElement *lme,
                           BOOLEAN predicatable,
                           BOOLEAN isNot,
                           INT32 fieldOp,
                           _MTH_MATCHER_FIELD_TYPE fieldCom
                         ) ;
      INT32 _injectElement ( const BSONElement &ele,
                             BSONObj::MatchType type,
                             LogicMatchElement *lme,
                             BOOLEAN isFieldCom = FALSE
                           ) ;
      INT32 _matches ( const CHAR *fieldName,
                       const BSONElement &toMatch,
                       const BSONObj &rootObj,
                       const BSONObj &obj,
                       BSONObj::MatchType op,
                       BOOLEAN isArray,
                       BOOLEAN isFieldCom,
                       const MatchElement &bm,
                       BOOLEAN isNot,
                       INT32 &result,
                       vector<INT64> *dollarList ) ;
      INT32 _traverseMatches ( LogicMatchElement *lme,
                              const BSONObj &obj,
                              BOOLEAN isNot,
                              INT32 &result,
                              vector<INT64> *dollarList ) ;
      BOOLEAN _REmatches ( LogicMatchElement *lme, const BSONObj &obj ) ;
      INT32 _valuesMatch ( const BSONElement &l,
                           const BSONElement &r,
                           BSONObj::MatchType op,
                           const MatchElement &bm,
                           vector<INT64> *dollarList
                         ) ;
      BOOLEAN _regexMatches ( const REMatchElement &me, const BSONElement& e ) ;
      void _deleteLME ( LogicMatchElement *lme ) ;
      void _setWeight ( LogicMatchElement *lme ) ;
      void _setWeight ( LogicMatchElement *lme, BSONObj::MatchType op ) ;
      static BOOLEAN compare ( LogicMatchElement *l1, LogicMatchElement *l2 ) ;
      void _sortLME ( LogicMatchElement *lme ) ;
      void _checkTotallyConverted( LogicMatchElement *lme ) ;
      void _countElement ( const BSONElement &ele, UINT32 &countOp ) ;
      BOOLEAN _checkValue( const BSONElement &ele, BOOLEAN isRoot = FALSE ) ;
      BOOLEAN _checkValueNonRec( const BSONElement &ele ) ;
      INT32 _loadPattern ( const BSONObj &matchPattern,
                           BOOLEAN predicatable ) ;
      INT32 _getDollarNumber ( const CHAR *pFieldName, INT32 &number ) ;

      INT32 _createBsonBuilder( BSONObjBuilder **builder ) ;

      friend class _mthMatcher::_MatchElement ;
   public:
      _mthMatcher ()
      {
         _rlme        = NULL ;
         _initialized = FALSE ;
         _matchesAll  = TRUE ;
         _totallyConverted = TRUE ;
      }
      ~_mthMatcher ()
      {
         clear() ;
      }
      void clear()
      {
         if ( _rlme )
         {
            _deleteLME ( _rlme ) ;
            _rlme = NULL ;
         }
         vector<BSONObjBuilder*>::iterator it ;
         for ( it = _builderList.begin(); it < _builderList.end(); it++ )
         {
            SDB_OSS_DEL (*it) ;
         }
         _builderList.clear() ;
         _predicateSet.clear() ;
         _matchesAll = TRUE ;
         _initialized = FALSE ;
      }
      INT32 loadPattern ( const BSONObj &matchPattern ) ;
      INT32 matches ( const BSONObj &matchTarget,
                      BOOLEAN &result,
                      vector<INT64> *dollarList = NULL ) ;
      BOOLEAN isInitialized () { return _initialized ; }
      BOOLEAN isMatchesAll() { return _matchesAll ; }
      const rtnPredicateSet &getPredicateSet ()
      {
         return _predicateSet ;
      }
      BSONObj &getMatchPattern ()
      {
         return _matchPattern ;
      }

      BOOLEAN totallyConverted() const
      {
         return _totallyConverted ;
      }

      void setMatchesAll( BOOLEAN matchesAll )
      {
         _matchesAll = matchesAll ;
      }
   } ;
   typedef class _mthMatcher mthMatcher ;
}

#endif
