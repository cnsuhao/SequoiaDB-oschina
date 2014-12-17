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

   Source File Name = mthMatcher.cpp

   Descriptive Name = Method Matcher

   When/how to use: this program may be used on binary and text-formatted
   versions of Method component. This file contains functions for matcher, which
   indicates whether a record matches a given matching rule.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "mthMatcher.hpp"
#include "ossUtil.hpp"
#include "pdTrace.hpp"
#include "mthTrace.hpp"

namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__MTHELEMT__MTHELEMT, "_mthMatcher::_MatchElement::_MatchElement" )
   _mthMatcher::_MatchElement::_MatchElement ( const BSONElement &e,
                                               BSONObj::MatchType op )
   :_subMatcher(NULL)
   {
      _toMatch = e ;
      _op = op ;
      if ( op == BSONObj::opTYPE )
      {
         _type = (BSONType)(e.numberInt());
      }
      else if ( op == BSONObj::opMOD )
      {
         if ( e.type() != Array )
         {
            PD_LOG ( PDERROR, "Array is expected for MOD" ) ;
            throw pdGeneralException ( "Array is expected for MOD" ) ;
         }
         BSONObj o = e.embeddedObject() ;
         if ( o.nFields() != 2 )
         {
            PD_LOG ( PDERROR, "Two elements are expceted for MOD" ) ;
            throw pdGeneralException (
                  "Two elements are expceted for MOD" );
         }
         _mod = o["0"].numberInt() ;
         _modm = o["1"].numberInt() ;
         if ( 0 == _mod )
         {
            PD_LOG ( PDERROR, "Modulo can't be 0" ) ;
            throw pdGeneralException ( "Modulo can't be 0" ) ;
         }
      }
      else if ( op == BSONObj::opELEM_MATCH )
      {
         //BSONElement m = e ;
         if ( e.type() != Object )
         {
            PD_LOG ( PDERROR, "eleMatch expects Object" ) ;
            throw pdGeneralException ( "eleMatch expects Object" ) ;
         }
         // for eleMatch, such like {a:{$eleMatch:{b:1}}}, this will
         // match {a:{b:1}}
         // or {a:{$eleMatch:{$and:[{b:1},{c:1}]}}}
         // this will match {a:{b:1,c:1}}
         // we do not support {a:{$eleMatch:{$lt:1}}} at the moment. The
         // object in eleMatch must be a full matching condition
         _subMatcher = SDB_OSS_NEW _mthMatcher () ;
         if ( !_subMatcher )
         {
            PD_LOG ( PDERROR, "Failed to allocate memory for sub "
                     "matcher" ) ;
            throw pdGeneralException ( "Failed to new submatcher" ) ;
         }
         INT32 rc = _subMatcher->_loadPattern ( e.embeddedObject(), FALSE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR,
                     "Failed to load pattern for submatcher, rc = %d",
                     rc ) ;
            throw pdGeneralException (
                  "Failed to load pattern for submatcher" ) ;
         }
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__PSELE, "_mthMatcher::_parseElement" )
   INT32 _mthMatcher::_parseElement ( const BSONElement &ele,
                                      LogicMatchElement *lme,
                                      BOOLEAN predicatable,
                                      BOOLEAN isNot
                                    )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__PSELE );
      const CHAR *regex = NULL ;
      const CHAR *options = NULL ;
      SDB_ASSERT ( ele.type() != Undefined, "Undefined element type" ) ;
      if ( isNot )
      {
         _totallyConverted = FALSE ;
      }

      // then check element type
      switch ( ele.type() )
      {
      case RegEx:
      {
         // for regular expresion case
         rc = _injectElement ( ele, BSONObj::opREGEX, lme ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed inject element, rc: %d", rc ) ;
            goto error ;
         }

         // if is in the $or { } and more than one element
         // need not add predicate
         if ( predicatable )
         {
            _predicateSet.addPredicate ( ele.fieldName(), ele, isNot ) ;
         }
         
         break ;

      }
      case Array:
      case Object:
      {
         BSONObjIterator j( ele.embeddedObject() ) ;
         BSONObjIterator *jt = NULL ;
         BSONElement eleTemp ;
         const CHAR *eFieldName = ele.fieldName() ;
         // if eFieldName()[0] is '$'
         if ( MTH_OPERATOR_EYECATCHER == eFieldName[0] )
         {
            // if eFieldName is "$and"
            if ( eFieldName[1] == 'a' && eFieldName[2] == 'n' &&
                 eFieldName[3] == 'd' && eFieldName[4] == 0 )
            {
               LogicMatchElement *clme ;
               // create LogicMatchElement
               rc = _createLME ( lme, &clme, MTH_LOGIC_AND ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed call create LogicMatcherElment, "
                           "rc: %d", rc ) ;
                  goto error ;
               }
               clme->_matchAll = lme->_matchAll ;
               while ( j.more () )
               {
                  eleTemp = j.next() ;
                  if ( Array == ele.type() )
                  {
                     if ( Object != eleTemp.type() )
                     {
                        rc = SDB_INVALIDARG ;
                        PD_LOG ( PDERROR, "Failed condition" ) ;
                        goto error ;
                     }
                     jt = new(std::nothrow)
                          BSONObjIterator(eleTemp.embeddedObject());
                     if ( !jt )
                     {
                        rc = SDB_OOM ;
                        PD_LOG ( PDERROR, "Failed to new BSONObj" ) ;
                        goto error ;
                     }
                     while ( jt->more() )
                     {
                        eleTemp = jt->next() ;
                        rc = _parseElement ( eleTemp,
                                             clme,
                                             predicatable,
                                             isNot
                                           ) ;
                        if ( rc )
                        {
                           delete jt ;
                           jt = NULL ;
                           PD_LOG ( PDERROR, "Failed call parseElement, "
                                    "rc: %d", rc ) ;
                           goto error ;
                        }
                     }
                     delete jt ;
                     jt = NULL ;
                  }
                  else
                  {
                     rc = SDB_INVALIDARG ;
                     PD_LOG ( PDERROR, "Failed to parse $and element" );
                     goto error ;
                  }
               }
               break ;
            }
            // if eFieldName is "$or"
            else if ( eFieldName[1] == 'o' && eFieldName[2] == 'r' &&
                      eFieldName[3] == 0 )
            {
               LogicMatchElement *clme ;
               LogicMatchElement *clme2 ;
               // create LogicMatchElement
               rc = _createLME ( lme, &clme2, MTH_LOGIC_OR ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed call create LogicMatcherElment, "
                           "rc: %d", rc ) ;
                  goto error ;
               }
               clme2->_matchAll = TRUE ;
               while ( j.more () )
               {
                  eleTemp = j.next() ;
                  if ( Array == ele.type() )
                  {
                     if ( Object != eleTemp.type() )
                     {
                        rc = SDB_INVALIDARG ;
                        PD_LOG ( PDERROR, "Failed condition" ) ;
                        goto error ;
                     }
                     jt = new(std::nothrow)
                          BSONObjIterator(eleTemp.embeddedObject());
                     if ( !jt )
                     {
                        rc = SDB_OOM ;
                        PD_LOG ( PDERROR, "Failed to new BSONObj" ) ;
                        goto error ;
                     }

                     rc = _createLME ( clme2, &clme, MTH_LOGIC_AND ) ;
                     if ( rc )
                     {
                        PD_LOG ( PDERROR, "Failed call create "
                                 "LogicMatcherElment, rc: %d", rc ) ;
                        goto error ;
                     }
                     clme->_matchAll = clme2->_matchAll ;

                     while ( jt->more() )
                     {
                        eleTemp = jt->next() ;
                        rc = _parseElement ( eleTemp,
                                             clme,
                                             FALSE,
                                             isNot
                                           ) ;
                        if ( rc )
                        {
                           delete jt ;
                           jt = NULL ;
                           PD_LOG ( PDERROR, "Failed call parseElement, rc: %d",
                                    rc ) ;
                           goto error ;
                        }
                     }
                     delete jt ;
                     jt = NULL ;
                  }
                  else
                  {
                     rc = SDB_INVALIDARG ;
                     PD_LOG ( PDERROR, "Failed to parse $or element, rc: %d",
                              rc );
                     goto error ;
                  }
               }
               break ;
            }
            // if eFieldName is "$not"
            else if ( eFieldName[1] == 'n' && eFieldName[2] == 'o' &&
                      eFieldName[3] == 't' && eFieldName[4] == 0 )
            {
               LogicMatchElement *clme ;
               LogicMatchElement *clme2 ;
               //UINT32 count = 0 ;
               //BOOLEAN pre = FALSE ;
               // create LogicMatchElement
               rc = _createLME ( lme, &clme2, MTH_LOGIC_NOT ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed call create LogicMatcherElment,"
                           "rc: %d", rc ) ;
                  goto error ;
               }
               clme2->_matchAll = TRUE ;
               // validate how many sub elements in the $not clause. If there's
               // only one element, we can push subobject to predicate.
               // Otherwise everything in the subelement can't be pushed
               //_countElement ( ele, count ) ;
               //if ( 1 == count )
               //   pre = TRUE ;
               while ( j.more () )
               {
                  eleTemp = j.next() ;
                  if ( Array == ele.type() )
                  {
                     if ( Object != eleTemp.type() )
                     {
                        rc = SDB_INVALIDARG ;
                        PD_LOG ( PDERROR, "Failed condition" ) ;
                        goto error ;
                     }
                     jt = new(std::nothrow)
                          BSONObjIterator(eleTemp.embeddedObject());
                     if ( !jt )
                     {
                        rc = SDB_OOM ;
                        PD_LOG ( PDERROR, "Failed to new BSONObj" ) ;
                        goto error ;
                     }

                     rc = _createLME ( clme2, &clme, MTH_LOGIC_AND ) ;
                     if ( rc )
                     {
                        PD_LOG ( PDERROR, "Failed call create "
                                 "LogicMatcherElment, rc: %d", rc ) ;
                        goto error ;
                     }
                     clme->_matchAll = clme2->_matchAll ;

                     while ( jt->more() )
                     {
                        eleTemp = jt->next() ;
                        rc = _parseElement ( eleTemp,
                                             clme,
                                             FALSE,
                                             TRUE
                                           ) ;
                        if ( rc )
                        {
                           delete jt ;
                           jt = NULL ;
                           PD_LOG ( PDERROR, "Failed call parseElement, rc: %d",
                                    rc ) ;
                           goto error ;
                        }
                     }
                     delete jt ;
                     jt = NULL ;
                  }
                  else
                  {
                     rc = SDB_INVALIDARG ;
                     PD_LOG ( PDERROR, "Failed to parse $not element, rc: %d",
                              rc );
                     goto error ;
                  }
               }
               break ;
            }
            // if is not "$and $or $not"
            // than it is $gt $gte $lt...
            // { $gt : { xxxxxxxx } }
            else
            {
               rc = SDB_INVALIDARG ;
               PD_LOG ( PDERROR, "operator can not in the first" ) ;
               goto error ;
            }
         }
         else if ( Array == ele.type() )
         {
            // for normal match
            if ( predicatable )
            {
               // key name push predicate must not command
               _predicateSet.addPredicate ( ele.fieldName(),
                                            ele,
                                            isNot ) ;
            }
            rc = _injectElement ( ele, BSONObj::Equality, lme ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed inject element, rc: %d", rc ) ;
               goto error ;
            }
            break ;
         }
         // key Name does not contain '$'
         // { a : { xxxxxx } }
         else
         {
            if ( _checkValueNonRec ( ele ) )
            {
               // for normal match
               if ( predicatable )
               {
                  // key name push predicate must not command
                  _predicateSet.addPredicate ( eFieldName,
                                               ele,
                                               isNot ) ;
               }
               rc = _injectElement ( ele, BSONObj::Equality, lme ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed inject element, rc: %d", rc ) ;
                  goto error ;
               }
            }
            else
            {
               while( j.more () )
               {
                  BSONElement embEle = j.next () ;
                  // first we get embedded field name
                  const CHAR *embFieldName = embEle.fieldName () ;

                  // { a : { $xx : xxxxxxx } }
                  if ( MTH_OPERATOR_EYECATCHER == embFieldName[0] &&
                       Object != embEle.type() && Array != embEle.type() )
                  {
                     _MTH_MATCHER_FIELD_TYPE fieldCom = MTH_MATCHER_FIELD_NOT ;
                     // { a : { $and : xxxx } }
                     if ( ( embFieldName[1] == 'a' && embFieldName[2] == 'n' &&
                            embFieldName[3] == 'd' && embFieldName[4] == 0 ) ||
                          ( embFieldName[1] == 'o' && embFieldName[2] == 'r' &&
                            embFieldName[3] == 0 ) ||
                          ( embFieldName[1] == 'n' && embFieldName[2] == 'o' &&
                            embFieldName[3] == 't' && embFieldName[4] == 0 )
                        )
                     {
                        rc = SDB_INVALIDARG ;
                        PD_LOG ( PDERROR, "$and $or $not must be stay in front "
                                 "of predicates" ) ;
                        goto error ;
                     }
                     if ( embFieldName[1] == 'f' && embFieldName[2] == 'i' &&
                          embFieldName[3] == 'e' && embFieldName[4] == 'l' &&
                          embFieldName[5] == 'd' && embFieldName[6] == 0 )
                     {
                        if ( String != embEle.type() )
                        {
                           rc = SDB_INVALIDARG ;
                           PD_LOG ( PDERROR, "Field must string" ) ;
                           goto error ;
                        }
                        fieldCom = MTH_MATCHER_FIELD_EQU ;
                        predicatable = FALSE ;
                     }                     
                     // $regex
                     else if ( embFieldName[1] == 'r' && embFieldName[2] == 'e' &&
                           embFieldName[3] == 'g' && embFieldName[4] == 'e' && 
                           embFieldName[5] == 'x'  && embFieldName[6] == 0 )
                     {
                        rc = _injectElement ( ele, BSONObj::opREGEX, lme ) ;
                        if ( rc )
                        {
                           PD_LOG ( PDERROR, "Failed inject element, rc: %d", rc ) ;
                           goto error ;
                        }

                        if ( predicatable )
                        {
                           _predicateSet.addPredicate ( ele.fieldName(), ele, isNot ) ;
                        }                        
                        
                        continue;
                     }
                     // $options
                     else if ( embFieldName[1] == 'o' && embFieldName[2] == 'p' &&
                           embFieldName[3] == 't' && embFieldName[4] == 'i' && 
                           embFieldName[5] == 'o' && embFieldName[6] == 'n' && 
                           embFieldName[7] == 's'  && embFieldName[8] == 0)
                     {
                        continue;
                     }
                     
                     if ( predicatable )
                     {
                        INT32 opTemp = embEle.getGtLtOp ( -1 ) ;
                        if ( ( Object != embEle.type() &&
                               Array != embEle.type() ) &&
                             ( BSONObj::opIN == opTemp ||
                               BSONObj::NIN == opTemp ||
                               BSONObj::opALL == opTemp ) )
                        {
                           rc = SDB_INVALIDARG ;
                           PD_LOG ( PDERROR, "Matching '$' command's value must"
                                    " object or array" ) ;
                           goto error ;
                        }
                        _predicateSet.addPredicate ( ele.fieldName(),
                                                     embEle,
                                                     isNot ) ;
                     }
                     rc = _addOperator( ele,
                                        embEle,
                                        regex,
                                        options,
                                        lme,
                                        predicatable,
                                        isNot,
                                        -1,
                                        fieldCom ) ;
                     if ( rc )
                     {
                        PD_LOG ( PDERROR, "Add operator[%s] failed, rc: %d",
                                 embEle.toString().c_str(), rc ) ;
                        goto error ;
                     }
                  }
                  // { a : { $xx : {...} } } or
                  // { a : { $xx : [...] } }
                  else if ( MTH_OPERATOR_EYECATCHER == embFieldName[0] &&
                           (Object == embEle.type() || Array == embEle.type()) )
                  {
                     // { a : { $xx : { $xx : ... } } }
                     if ( !_checkValue( embEle, TRUE ) )
                     {
                        BSONObjIterator k( embEle.embeddedObject() ) ;
                        while ( k.more() )
                        {
                           INT32 opTemp = -1 ;
                           _MTH_MATCHER_FIELD_TYPE fieldCom
                                                       = MTH_MATCHER_FIELD_NOT ;
                           BSONElement tElem = k.next () ;
                           const CHAR *tEleFieName = tElem.fieldName () ;
                           if ( MTH_OPERATOR_EYECATCHER == tEleFieName[0] &&
                                tEleFieName[1] == 'f' &&
                                tEleFieName[2] == 'i' &&
                                tEleFieName[3] == 'e' &&
                                tEleFieName[4] == 'l' &&
                                tEleFieName[5] == 'd' &&
                                tEleFieName[6] == 0 )
                           {
                              if ( String != tElem.type() )
                              {
                                 rc = SDB_INVALIDARG ;
                                 PD_LOG ( PDERROR, "Field must string" ) ;
                                 goto error ;
                              }
                              fieldCom = MTH_MATCHER_FIELD_OTH ;
                              predicatable = FALSE ;
                              opTemp = embEle.getGtLtOp ( -1 ) ;
                           }
                           else
                           {
                              if ( !_checkValue( embEle ) )
                              {
                                 rc = SDB_INVALIDARG ;
                                 PD_LOG ( PDERROR, "Matching syntax can not "
                                          "have more than one operator") ;
                                 goto error ;
                              }
                           }// if
                           rc = _addOperator( ele,
                                              tElem,
                                              regex,
                                              options,
                                              lme,
                                              FALSE,
                                              isNot,
                                              opTemp,
                                              fieldCom ) ;
                           if ( rc )
                           {
                              PD_LOG ( PDERROR, "Add operator[%s] failed, "
                                       "rc: %d", embEle.toString().c_str(),
                                       rc ) ;
                              goto error ;
                           }
                           opTemp = -1 ;
                        }// while
                     }
                     // { a : { $xx : { xx : ... } } }
                     else
                     {
                        if ( predicatable )
                        {
                           INT32 opTemp = embEle.getGtLtOp ( -1 ) ;
                           if ( ( Object != embEle.type() &&
                                  Array != embEle.type() ) &&
                                ( BSONObj::opIN == opTemp ||
                                  BSONObj::NIN == opTemp ||
                                  BSONObj::opALL == opTemp ) )
                           {
                              rc = SDB_INVALIDARG ;
                              PD_LOG ( PDERROR, "Matching '$' command's value "
                                       "must object or array") ;
                              goto error ;
                           }
                           _predicateSet.addPredicate ( ele.fieldName(),
                                                        embEle,
                                                        isNot ) ;
                        }
                        rc = _addOperator( ele,
                                           embEle,
                                           regex,
                                           options,
                                           lme,
                                           predicatable,
                                           isNot,
                                           -1,
                                           MTH_MATCHER_FIELD_NOT ) ;
                        if ( rc )
                        {
                           PD_LOG ( PDERROR, "Add operator[%s] failed, rc: %d",
                                    embEle.toString().c_str(), rc ) ;
                           goto error ;
                        }
                     }
                  }
                  // { a : { xxx : xxxxxx } }, quality condition
                  else
                  {
                     // { a : { b : ...{ $xxx : xxxxxx}... } }
                     if ( Object == embEle.type() || Array == embEle.type() )
                     {
                        BSONObjIterator k( embEle.embeddedObject() ) ;
                        while ( k.more() )
                        {
                           if ( !_checkValue( k.next() ) )
                           {
                              rc = SDB_INVALIDARG ;
                              PD_LOG ( PDERROR, "Operators should be back of "
                                       "the key" );
                              goto error ;
                           }
                        }
                     }
                     if ( predicatable )
                     {
                         _predicateSet.addPredicate ( ele.fieldName(),
                                                      embEle,
                                                      isNot ) ;
                     }
                     rc = _injectElement ( ele, BSONObj::Equality, lme ) ;
                     if ( rc )
                     {
                        PD_LOG ( PDERROR, "Failed inject element, rc: %d", rc) ;
                        goto error ;
                     }
                  }
               }
            }
            break ;
         }
      }
      //case Array:
         // for array match, deal it same way as normal match
      default:
      {
         const CHAR *eFieldName = ele.fieldName() ;
         //if eFieldName()[0] is '$'
         // { $gt : 0 }
         if ( MTH_OPERATOR_EYECATCHER == eFieldName[0] )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "operator can not in the head" ) ;
            goto error ;
         }
         // { a : 1 }
         else
         {
            // for normal match
            if ( predicatable )
            {
               // key name push predicate must not command
               _predicateSet.addPredicate ( ele.fieldName(),
                                            ele,
                                            isNot ) ;
            }
            rc = _injectElement ( ele, BSONObj::Equality, lme ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed inject element, rc: %d", rc ) ;
               goto error ;
            }
         }
         break ;
      } // default
      } // switch

   done :
      PD_TRACE_EXITRC ( SDB__MTHMACH__PSELE, rc );
      return rc ;
   error :
      PD_LOG ( PDERROR, "Failed, element: %s, rc: %d",
               ele.toString().c_str(), rc ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__ADDOP, "_mthMatcher::_addOperator" )
   INT32 _mthMatcher::_addOperator ( const BSONElement &ele,
                                     const BSONElement &embEle,
                                     const CHAR *&regex,
                                     const CHAR *&options,
                                     LogicMatchElement *lme,
                                     BOOLEAN predicatable,
                                     BOOLEAN isNot,
                                     INT32 fieldOp,
                                     _MTH_MATCHER_FIELD_TYPE fieldCom )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__ADDOP );
      BOOLEAN isFieldCom = FALSE ;
      INT32 op = -1 ;
      BSONElement e ;

      if ( fieldCom )
      {
         isFieldCom = TRUE ;
         op = fieldOp ;
      }
      if ( -1 == op )
      {
         op = embEle.getGtLtOp ( -1 ) ;
         if ( -1 == op )
         {
            PD_LOG ( PDERROR, "Failed to get type of element" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      // add the element into predicate set
      if ( predicatable )
      {
         _predicateSet.addPredicate ( ele.fieldName(), embEle, isNot ) ;
      }

      switch ( op )
      {
      // >, >=, <, <=, <>
      case BSONObj::GT:
      case BSONObj::GTE:
      case BSONObj::LT:
      case BSONObj::LTE:
      case BSONObj::NE:
      case BSONObj::opMOD:
      case BSONObj::opTYPE:
      case BSONObj::opELEM_MATCH:
      case BSONObj::opSIZE:
      case BSONObj::opEXISTS:
      case BSONObj::opISNULL:
      case BSONObj::Equality:
      {
         // create a new BSON object, with ele.fieldName as name, and embEle
         // as value
         // for example the input is
         // (1) {age:{$gt:20}}
         // (2) {$gt:20}
         // here ele is (1), embEle is (2)
         // fieldName here is $gt
         // type is number
         // then the new object will be
         // {age:20}

         // this pointer is stored in _builderList vector, and will be freed
         // in class destructor when the object is destroyed
         BSONObjBuilder *b = SDB_OSS_NEW BSONObjBuilder () ;
         if ( !b )
         {
            PD_LOG ( PDERROR, "Failed to allocate memory for BSONObjBuilder" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         try
         {
            _builderList.push_back ( b ) ;
         }
         catch( std::exception &e )
         {
            if ( b )
            {
               SDB_OSS_DEL b ;
               b = NULL ;
            }
            PD_LOG ( PDERROR, "Failed to push builder to builder list: %s",
                     e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
         b->appendAs ( embEle, ele.fieldName() ) ;
         rc = _injectElement ( b->done().firstElement(),
                               ( BSONObj::MatchType )op,
                               lme, isFieldCom ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed inject element, rc: %d", rc ) ;
            goto error ;
         }
         break ;
      }
      case BSONObj::opREGEX:
      {
         if ( RegEx == embEle.type () )
         {
            regex = embEle.regex () ;
            options = embEle.regexFlags () ;
         }
         else
         {
            regex = embEle.valuestrsafe ();
         }
         break ;
      }
      case BSONObj::opOPTIONS:
      {
         options = embEle.valuestrsafe ();
         break ;
      }
      case BSONObj::opALL:
      case BSONObj::opIN:
      case BSONObj::NIN:
      {
         if ( Array != embEle.type() )
         {
            PD_LOG ( PDERROR, "Need to be array" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         // add array into matchElements
         LogicMatchElement *clme = NULL ;
         rc = _createLME ( lme, &clme, MTH_LOGIC_OTHER ) ;
         if ( rc )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "Failed to create LogicMatchElement" ) ;
            goto error ;
         }
         //memory free is in the destructor
         clme->_me = SDB_OSS_NEW MatchElement( ele,
                                              (BSONObj::MatchType)op,
                                               embEle.embeddedObject()
                                              ) ;
         if ( !clme->_me )
         {
            PD_LOG ( PDERROR, "Failed to allocate memory for MatchElement" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         break ;
      }
      default:
      {
         PD_LOG ( PDERROR, "Operator %d is not supported yet", op ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }//default
      }// switch

   done :
      PD_TRACE_EXITRC ( SDB__MTHMACH__ADDOP, rc );
      return rc ;
   error :
      PD_LOG ( PDERROR, "Failed, element: %s, rc: %d",
               embEle.toString().c_str(), rc ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__INJELE, "_mthMatcher::_injectElement" )
   INT32 _mthMatcher::_injectElement ( const BSONElement &ele,
                                       BSONObj::MatchType type,
                                       LogicMatchElement *lme,
                                       BOOLEAN isFieldCom
                                     )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__INJELE );
      if ( ele.type() == MinKey || ele.type() == MaxKey ||
           ele.type() == Undefined )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      // push regular expression check to separate queue since it's expensive
      if ( BSONObj::opREGEX == type )
      {
         LogicMatchElement *clme ;
         rc = _createLME ( lme,
                           &clme,
                           MTH_LOGIC_OTHER,
                           TRUE
                         ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to create LogicMatchElement, rc: %d",
                     rc ) ;
            goto error ;
         }
         //set weight
         _setWeight ( clme, type ) ;
         //create regex Match Element, memory free is in the destructor

         switch ( ele.type() )
         {
         case RegEx:
         {
            clme->_rme = SDB_OSS_NEW REMatchElement ( ele ) ;
            break;
         }
         case Object:
         {
            const CHAR *fieldName = NULL;
            const CHAR *regex = NULL;
            const CHAR *options = NULL;

            fieldName = ele.fieldName();
            BSONObjIterator i( ele.embeddedObject() );
            while(i.more())
            {
               BSONElement embEle = i.next();

               const CHAR *embFieldName = embEle.fieldName () ;

               if ( MTH_OPERATOR_EYECATCHER == embFieldName[0] &&
                    String == embEle.type() )               
               {
                  if ( embFieldName[1] == 'r' && embFieldName[2] == 'e' &&
                       embFieldName[3] == 'g' && embFieldName[4] == 'e' && 
                       embFieldName[5] == 'x'  && embFieldName[6] == 0)
                  {
                     regex = embEle.valuestrsafe();
                  }
                  else if ( embFieldName[1] == 'o' && embFieldName[2] == 'p' &&
                       embFieldName[3] == 't' && embFieldName[4] == 'i' && 
                       embFieldName[5] == 'o' && embFieldName[6] == 'n' && 
                       embFieldName[7] == 's'  && embFieldName[8] == 0)
                  {
                     options = embEle.valuestrsafe();
                  }
                  else
                  {
                     continue;
                  }
               }
            }

            //options can be null
            if( fieldName == NULL || regex == NULL)
            {
               PD_LOG ( PDERROR, "Some regex elements missing." ) ;
               rc = SDB_INVALIDARG ;
               goto error ;               
            }
            
            clme->_rme = SDB_OSS_NEW REMatchElement ( fieldName, regex, options ) ;
            break;
         }
         default:
         {
            PD_LOG ( PDERROR, "Regex element type should be 'Object' or 'Regex'." ) ;            
            rc = SDB_INVALIDARG ;
            goto error ;
         }// end default
         }// end switch
         
         if ( !clme->_rme )
         {
            PD_LOG ( PDERROR, "Failed to allocate memory for REMatchElement" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }
      else
      {
         LogicMatchElement *clme ;
         rc = _createLME ( lme,
                           &clme,
                           MTH_LOGIC_OTHER
                         ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to create LogicMatchElement" ) ;
            goto error ;
         }
         //set weight
         _setWeight ( clme, type ) ;
         // one record's field and self field compare
         if ( isFieldCom )
         {
            clme->_isFieldCom = TRUE ;
         }
         //create Match Element,memory free is in the destructor
         clme->_me = SDB_OSS_NEW MatchElement ( ele, type ) ;
         if ( !clme->_me )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "Failed to allocate memory for MatchElement" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }

      if ( BSONObj::GT != type &&
           BSONObj::GTE != type &&
           BSONObj::LT != type &&
           BSONObj::LTE != type &&
           BSONObj::Equality != type )
      {
         _totallyConverted = FALSE ;
      }
   done :
      PD_TRACE_EXITRC ( SDB__MTHMACH__INJELE, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _mthMatcher::loadPattern ( const BSONObj &matchPattern )
   {
      return _loadPattern ( matchPattern, TRUE ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__LDPTTN, "_mthMatcher::_loadPattern" )
   INT32 _mthMatcher::_loadPattern ( const BSONObj &matchPattern,
                                     BOOLEAN predicatable )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__LDPTTN );
      _matchPattern = matchPattern.copy() ;
      BSONObjIterator i(_matchPattern) ;
      INT32 eleNum = 0 ;
      SDB_ASSERT ( !_initialized, "mthMatcher can't be initialized "
                   "multiple times" ) ;
      try
      {
         //create root object,memory free is in the destructor
         _rlme = SDB_OSS_NEW LogicMatchElement() ;
         if ( !_rlme )
         {
            PD_LOG ( PDERROR, "Failed to allocate memory for "
                     "LogicMatchElement" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         //root object logic type must and
         _rlme->_logicType = MTH_LOGIC_AND ;
         while ( i.more() )
         {
            rc = _parseElement( i.next(), _rlme, predicatable ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to parse match pattern %d, rc: %d",
                        eleNum, rc ) ;
               goto error ;
            }
            ++eleNum ;
            // if there is at least one predicate,
            // let's set _matchesAll to FALSE
            _matchesAll = FALSE ;
         }
         //set all weight
         _setWeight ( _rlme ) ;
         //sort the logic match element tree
         _sortLME ( _rlme ) ;
         if ( !_predicateSet.isValid() )
         {
            rc = SDB_RTN_INVALID_PREDICATES ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to call loadPattern: %s", e.what() ) ;
         goto error ;
      }
      _initialized = TRUE ;

   done :
      PD_TRACE_EXITRC ( SDB__MTHMACH__LDPTTN, rc );
      return rc ;
   error :
      goto done ;
   }

   OSS_INLINE BOOLEAN _mthMatcher::_regexMatches ( const REMatchElement &me,
                                                   const BSONElement& e
                                                 )
   {
      switch ( e.type() )
      {
      case String:
      case Symbol:
         if ( me._goSimpleMatch )
         {
            return ossStrstr( e.valuestr(), me._regex ) != NULL ;
         }
         else
         {
            return me._re->PartialMatch( e.valuestr() ) ;
         }
      case RegEx:
         return !ossStrcmp( me._regex, e.regex() ) &&
                !ossStrcmp( me._flags, e.regexFlags() ) ;
      default:
         return FALSE ;
      }
   }

   OSS_INLINE INT32 _mthMatcher::_valuesMatch ( const BSONElement &l,
                                                const BSONElement &r,
                                                BSONObj::MatchType op,
                                                const MatchElement &bm,
                                                vector<INT64> *dollarList )
   {
      SDB_ASSERT(BSONObj::NE != op, "op shouldn't be NE") ;
      SDB_ASSERT(BSONObj::NIN != op, "op shouldn't be NIN") ;

      if ( BSONObj::Equality == op )
         return l.valuesEqual(r) ;
      if ( BSONObj::opIN == op )
      {
         // for example {$in:[1,2,3]}
         // in the set we search for what we are looking for
         INT32 count = bm._myset.count(l) ;
         // if there's any match, we return the count
         if ( count )
            return count ;
         // if there's regular expression we want to match for IN clause
         if ( bm._myregex.size() != 0 )
         {
            vector<REMatchElement>::const_iterator i ;
            for ( i = bm._myregex.begin(); i != bm._myregex.end(); i++ )
               if ( _regexMatches ( *i, l ) )
                  return TRUE ;
         }
      }
      if ( BSONObj::opALL == op )
      {
         // if l is not array
         if ( Array != l.type() && Object != l.type() )
         {
            // r is  { $all : [ xxx , ...... ] }
            BSONObjIterator it( r.embeddedObject() ) ;
            // tempEle is [ xxx , ...... ]
            if ( !it.more() )
            {
               return FALSE ;
            }
            BSONElement tempEle = it.next() ;
            BSONObjIterator it2( tempEle.embeddedObject() ) ;
            BOOLEAN tempRc = FALSE ;
            while ( it2.more() )
            {
               // z is every element
               BSONElement z = it2.next() ;
               // compare l and z , l is notes
               if ( RegEx != z.type() )
               {
                  if ( !l.valuesEqual ( z ) )
                  {
                     return FALSE ;
                  }
                  else
                  {
                     tempRc = TRUE ;
                  }
               }
               else
               {
                  vector<REMatchElement>::const_iterator i ;
                  for ( i = bm._myregex.begin(); i != bm._myregex.end(); i++ )
                  {
                     if ( _regexMatches ( *i, l ))
                     {
                        tempRc = TRUE ;
                        break ;
                     }
                  }
               }
            }
            return tempRc ;
         }
         // { a : [ 1, 2, 3 ] }
         else if ( Array == l.type () )
         {
            // for example {$in:[1,2,3]}
            // in the set we search for what we are looking for
            set<BSONElement, element_lt> _myset ;
            BSONObjIterator it ( l.embeddedObject() ) ;
            while ( it.more() )
            {
               BSONElement ie = it.next() ;
               _myset.insert(ie) ;
            }
            INT32 count = _myset.count( r ) ;
            // if there's any match, we return the count
            if ( count )
               return count ;
            // if there's regular expression we want to match for IN clause
            if ( RegEx == r.type() && bm._myregex.size() != 0 )
            {
               vector<REMatchElement>::const_iterator i ;
               for ( i = bm._myregex.begin(); i != bm._myregex.end(); i++ )
               {
                  BSONObjIterator it ( l.embeddedObject() ) ;
                  while ( it.more() )
                  {
                     if ( _regexMatches ( *i, it.next() ) )
                        return TRUE ;
                  }
               }
               return FALSE ;
            }
         }
         return FALSE ;
      }
      if ( BSONObj::opSIZE == op )
      {
         if ( l.type() != Array )
            return FALSE ;
         INT32 count = 0 ;
         BSONObjIterator i ( l.embeddedObject() ) ;
         while ( i.moreWithEOO() )
         {
            BSONElement e = i.next() ;
            if ( e.eoo() )
               break ;
            count++ ;
         }
         return count == r.number() ;
      }
      if ( BSONObj::opMOD == op )
      {
         if ( !l.isNumber() )
            return FALSE ;
         return l.numberLong() % bm._mod == bm._modm ;
      }
      else if ( BSONObj::opTYPE == op )
      {
         return bm._type == l.type() ;
      }
      // sub matcher compare
      if ( BSONObj::opELEM_MATCH == op )
      {
         BOOLEAN result = FALSE ;
         if ( l.type() != Object &&
              l.type() != Array )
            return FALSE ;
         if ( bm._subMatcher->matches ( l.embeddedObject(),
                                        result,
                                        dollarList ) != SDB_OK )
            return FALSE ;
         return result ;
      }

      if ( l.canonicalType() != r.canonicalType() )
         return FALSE ;
      // compareElementValues is in bsonobj.cpp
      INT32 c = compareElementValues ( l, r ) ;
      // less than, we have -1
      if ( c<-1) c= -1 ;
      // greater than, we have 1
      if ( c>1)  c= 1 ;
      // mask is 100 for greater than result, or 0 for less than result, or 10
      // for equal
      INT32 z = 1<<(c+1) ;
      // if we are looking for LT is 1(001), LTE is 3 (011), GTE is 6 (110), GT
      // is 4 (100)
      // so the c & z (2 means >, 0 means <, 1 means =)
      // is able to get the result (3 bits represet "bigger than" "equal to"
      // "less than"), so 100 will match both GTE and GT, and 10 will match LTE
      // and GTE, and 1 will match LT and LTE
      return (op&z) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__DELLME, "_mthMatcher::_deleteLME" )
   void _mthMatcher::_deleteLME ( LogicMatchElement *lme )
   {
      SDB_ASSERT ( lme, "lme can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__DELLME );
      //delete "$and" and "$or" object,clear there vector
      if ( MTH_LOGIC_OTHER != lme->_logicType )
      {
         for ( UINT32 i = 0; i < lme->_vlme.size(); i++ )
         {
            LogicMatchElement *temp = lme->_vlme[i] ;
            if ( temp )
               _deleteLME ( temp ) ;
         }
         lme->_vlme.clear () ;
      }
      // delete the "$and" or "$or" leaf object
      else
      {
         if ( lme->_me )
         {
            SDB_OSS_DEL lme->_me ;
            lme->_me = NULL ;
         }
         if ( lme->_rme )
         {
            SDB_OSS_DEL lme->_rme ;
            lme->_rme = NULL ;
         }
      }
      SDB_OSS_DEL lme ;
      lme = NULL ;
      PD_TRACE_EXIT ( SDB__MTHMACH__DELLME );
   }

   //set leaf weight
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__SETWGH, "_mthMatcher::_setWeight" )
   void _mthMatcher::_setWeight ( LogicMatchElement *lme,
                                  BSONObj::MatchType op
                                )
   {
      UINT32 num = 0 ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__SETWGH );
      // if logic type is not MTH_LOGIC_OTHER, we will set weight in the last
      // else we must set weight
      if ( MTH_LOGIC_OTHER == lme->_logicType )
      {
         switch ( op )
         {
         case BSONObj::GT:
            num = MTH_WEIGHT_GT ;
            break ;
         case BSONObj::GTE:
            num = MTH_WEIGHT_GTE ;
            break ;
         case BSONObj::LT:
            num = MTH_WEIGHT_LT ;
            break ;
         case BSONObj::LTE:
            num = MTH_WEIGHT_LTE ;
            break ;
         case BSONObj::NE:
            num = MTH_WEIGHT_NE ;
            break ;
         case BSONObj::opMOD:
            num = MTH_WEIGHT_MOD ;
            break ;
         case BSONObj::opTYPE:
            num = MTH_WEIGHT_TYPE ;
            break ;
         case BSONObj::opELEM_MATCH:
            num = MTH_WEIGHT_ELEMMATCH ;
            break ;
         case BSONObj::opSIZE:
            num = MTH_WEIGHT_SIZE ;
            break ;
         case BSONObj::opEXISTS:
            num = MTH_WEIGHT_EXISTS ;
            break ;
         case BSONObj::opISNULL:
            num = MTH_WEIGHT_ISNULL ;
            break ;
         case BSONObj::opALL:
            num = MTH_WEIGHT_ALL ;
            break ;
         case BSONObj::opIN:
            num = MTH_WEIGHT_IN ;
            break ;
         case BSONObj::NIN:
            num = MTH_WEIGHT_NIN ;
            break ;
         case BSONObj::opREGEX:
            num = MTH_WEIGHT_REGEX ;
            break ;
         default :
            num = 0 ;
            break ;
         }
         lme->_weight = num ;
      }
      PD_TRACE_EXIT ( SDB__MTHMACH__SETWGH );
   }

   //set the tree weight
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__SETWGH2, "_mthMatcher::_setWeight" )
   void _mthMatcher::_setWeight ( LogicMatchElement *lme )
   {
      // in the last, we must set weight, but when the logic type is
      // MTH_LOGIC_OTHER, we need not set weight, because we have already set up
      PD_TRACE_ENTRY ( SDB__MTHMACH__SETWGH2 );
      if ( MTH_LOGIC_OTHER != lme->_logicType )
      {
         LogicMatchElement *temp;
         for ( UINT32 i = 0; i < lme->_vlme.size(); i++ )
         {
            temp = lme->_vlme[i] ;
            _setWeight ( temp ) ;
            lme->_weight += temp->_weight ;
         }
      }
      PD_TRACE_EXIT ( SDB__MTHMACH__SETWGH2 );
   }

   //sort compare function
   BOOLEAN _mthMatcher::compare ( LogicMatchElement *l1, LogicMatchElement *l2 )
   {
      return l1->_weight < l2->_weight ;
   }

   //sort the logic match element tree
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__SRLME, "_mthMatcher::_sortLME" )
   void _mthMatcher::_sortLME ( LogicMatchElement *lme )
   {
      PD_TRACE_ENTRY ( SDB__MTHMACH__SRLME );
      UINT32 vSize = lme->_vlme.size() ;
      // when logic type is MTH_LOGIC_OTHER, then it is leaf
      // if is leaf, we do not need to sort
      if ( MTH_LOGIC_OTHER != lme->_logicType )
      {
         LogicMatchElement *temp ;
         //we must take every leaf sorting
         for ( UINT32 i = 0; i < vSize; i++ )
         {
            temp = lme->_vlme[i] ;
            _sortLME ( temp ) ;
         }
         std::sort ( lme->_vlme.begin(), lme->_vlme.end(), compare ) ;
      }
      PD_TRACE_EXIT ( SDB__MTHMACH__SRLME );
   }

   //create logic match element object
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__CRTLME, "_mthMatcher::_createLME" )
   INT32 _mthMatcher::_createLME ( LogicMatchElement *lme,
                                   LogicMatchElement **clme,
                                   INT32   logicType,
                                   BOOLEAN isRegex
                                 )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__CRTLME );
      SDB_ASSERT ( lme && clme, "lme and clme can't be NULL" ) ;
      // create a new LogicMatchElement obj,memory free is in the destructor
      *clme = SDB_OSS_NEW LogicMatchElement () ;
      if ( !(*clme) )
      {
         PD_LOG ( PDERROR, "Failed to allocate memory for LogicMatchElement" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      try
      {
         //push the child object in the parent vector
         lme->_vlme.push_back ( *clme ) ;
      }
      catch ( std::exception &e )
      {
         SDB_OSS_DEL *clme ;
         *clme = NULL ;
         PD_LOG ( PDERROR, "Failed to push back LogicMatchElement into "
                  "vector: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      (*clme)->_logicType = logicType ;
      (*clme)->_isRegex = isRegex ;

      if ( MTH_LOGIC_OR == logicType )
      {
         _totallyConverted = FALSE ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__MTHMACH__CRTLME, rc );
      return rc ;
   error :
      goto done ;
   }

   BOOLEAN _mthMatcher::_checkValueNonRec( const BSONElement &ele )
   {
      const CHAR *eFieldName = NULL ;
      BSONElement temEle ;
      if ( Object == ele.type() || Array == ele.type() )
      {
            BSONObjIterator j( ele.embeddedObject() ) ;
            while ( j.more () )
            {
               temEle = j.next() ;
               eFieldName = temEle.fieldName() ;
               if ( MTH_OPERATOR_EYECATCHER == eFieldName[0] )
               {
                  return FALSE ;
               }
            }
            return TRUE ;
      }
      else
      {
         return TRUE ;
      }
   }

   BOOLEAN _mthMatcher::_checkValue( const BSONElement &ele, BOOLEAN isRoot )
   {
      if ( Object == ele.type() || Array == ele.type() )
      {
         //get key name
         const CHAR *eFieldName = ele.fieldName() ;
         // do not check subelement if it's elem_match
         if ( ele.getGtLtOp(-1) == BSONObj::opELEM_MATCH )
            return TRUE ;
         //eFieldName cannot be $xxx
         if ( MTH_OPERATOR_EYECATCHER != eFieldName[0] || isRoot )
         {
            BSONObjIterator j( ele.embeddedObject() ) ;
            while ( j.more () )
            {
               if ( !_checkValue( j.next(), FALSE ) )
                  return FALSE ;
            }
            return TRUE ;
         }
         return FALSE ;
      }
      // regex normal
      else
      {
         //get key name
         const CHAR *eFieldName = ele.fieldName() ;
         return MTH_OPERATOR_EYECATCHER != eFieldName[0] ;
      }
   }

   // get element number, note this is ONLY used by $not
   // { $not : { a : 1 , b : 2 } }  return 2
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__CNTELE, "_mthMatcher::_countElement" )
   void _mthMatcher::_countElement ( const BSONElement &ele, UINT32 &countOp )
   {
      PD_TRACE_ENTRY ( SDB__MTHMACH__CNTELE );
      if ( Array == ele.type() )
      {
         //get key name
         const CHAR *eFieldName = ele.fieldName() ;
         //eFieldName must is $xxx
         if ( MTH_OPERATOR_EYECATCHER == eFieldName[0] )
         {
            // is and or not
            if ( ( eFieldName[1] == 'a' && eFieldName[2] == 'n' &&
                   eFieldName[3] == 'd' && eFieldName[4] == 0 ) ||
                 ( eFieldName[1] == 'o' && eFieldName[2] == 'r' &&
                   eFieldName[3] == 0 ) ||
                 ( eFieldName[1] == 'n' && eFieldName[2] == 'o' &&
                   eFieldName[3] == 't' && eFieldName[4] == 0 )
               )
            {
               BSONObjIterator j( ele.embeddedObject() ) ;
               while ( j.more () )
               {
                  j.next() ;
                  ++countOp ;//_countElement ( j.next (), countOp ) ;
               }
            }// is and or not
         }// $
         else
         {
            //++countOp ;
         }
      }
      else
      {
         //++countOp ;
      }
      PD_TRACE_EXIT ( SDB__MTHMACH__CNTELE );
   }

   #define MTH_MATCH_STATIC_BUFFER_SZ 255
   #define NMATCH  -1
   #define NEXIST   0
   #define MATCH    1
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH_MATCHES, "_mthMatcher::_matches" )
   INT32 _mthMatcher::_matches ( const CHAR *fieldName,
                                 const BSONElement &toMatch,
                                 const BSONObj &rootObj,
                                 const BSONObj &obj,
                                 BSONObj::MatchType op,
                                 BOOLEAN isArray,
                                 BOOLEAN isFieldCom,
                                 const MatchElement &bm,
                                 BOOLEAN isNot,
                                 INT32 &result,
                                 vector<INT64> *dollarList )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH_MATCHES );
      CHAR *tempFieldName = NULL ;
      INT32 fieldNameLen = 0 ;
      CHAR *p = NULL ;
      const BSONElement *pElement = NULL ;
      BSONElement e ;
      BSONElement f ;
      // use 256 bytes static buffer for most scenarios
      CHAR staticBuffer [ MTH_MATCH_STATIC_BUFFER_SZ+1 ] = {0} ;
      tempFieldName = &staticBuffer[0] ;
      // get the field name length
      fieldNameLen = ossStrlen ( fieldName ) ;
      // if the length is bigger than static buffer, we duplicate the string
      if ( fieldNameLen > MTH_MATCH_STATIC_BUFFER_SZ )
      {
         // tempFieldName is freed in done
         tempFieldName = ossStrdup ( fieldName ) ;
         if ( !tempFieldName )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "Failed to duplicate field name %s", fieldName ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }
      // otherwise we just copy the string to static buffer
      else
      {
         ossStrncpy ( tempFieldName, fieldName, fieldNameLen ) ;
      }

      // special case for Not Equal, we treat it as Equality and reverse the
      // result
      if ( op == BSONObj::NE )
      {
         rc = _matches ( fieldName, toMatch, rootObj, obj, BSONObj::Equality,
                         FALSE, isFieldCom, bm, !isNot, result, dollarList ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to match NE, rc: %d", rc ) ;
            goto error ;
         }
         result = -result ;
         // for NE, if the field doesn't exist, we still consider it's a match
         if ( NEXIST == result )
            result = MATCH ;
         goto done ;
      }
      // same for Not In
      else if ( op == BSONObj::NIN )
      {
         rc = _matches ( fieldName, toMatch, rootObj, obj, BSONObj::opIN,
                         FALSE, isFieldCom, bm, !isNot, result, dollarList ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to match NIN, rc: %d", rc ) ;
            goto error ;
         }
         result = -result ;
         // for NIN, if the field doesn't exist, we still consider it's a match
         if ( NEXIST == result )
            result = MATCH ;
         goto done ;
      }

      // try to find "." from the copied string
      // xxxxxx
      p = ossStrnchr ( tempFieldName, MTH_FIELDNAME_SEP, fieldNameLen ) ;
      if ( p )
      {
         // xxx.xxx
         // if we can find ".", we replace it by end of string
         *p = '\0' ;
         // use first part as field name to find whether if the field exist
         BSONElement se = obj.getField(tempFieldName) ;
         if ( se.eoo() || (se.type() !=Object && se.type() != Array) )
         {
            // do something if we can't find the field, or it's not object nor
            // array
         }
         else
         {
            // if the field is object or array, we call _matches by recursive
            // and pass it the embedded object
            INT32 childFieldNameLen = 0 ;
            INT32 dollarNum = 0 ;
            CHAR *pChildFieldName = NULL ;
            if ( MTH_OPERATOR_EYECATCHER == *(p + 1) )
            {
               //rc = _getDollarNumber ( p + 1, dollarNum ) ;
               rc = ossStrToInt ( p + 2, &dollarNum ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to parse number, rc: %d", rc ) ;
                  goto error ;
               }
               if ( Array == se.type() )
               {
                  childFieldNameLen = ossStrlen ( (p + 1) ) ;
                  pChildFieldName = ossStrnchr ( p + 1,
                                                 MTH_FIELDNAME_SEP,
                                                 childFieldNameLen ) ;
                  // $xxx.xxx ... : xxx
                  if ( pChildFieldName )
                  {
                     BSONObjIterator it( se.embeddedObject() ) ;
                     while ( it.more() )
                     {
                        BSONElement z = it.next() ;
                        if ( Object == z.type() )
                        {
                           BSONObj eo = z.embeddedObject () ;
                           rc = _matches ( pChildFieldName + 1, toMatch,
                                           rootObj, eo, op,
                                           Array == z.type(), isFieldCom, bm,
                                           isNot, result, dollarList ) ;
                           if ( rc )
                           {
                              PD_LOG ( PDERROR, "Failed to match NE, rc: %d",
                              rc ) ;
                              goto error ;
                           }
                        }
                        else
                        {
                           if ( _valuesMatch(z, toMatch, op, bm, dollarList) )
                           {
                              result = MATCH ;
                           }
                        }
                        if ( ( !isNot && MATCH == result ) ||
                             ( isNot && NMATCH == result ) )
                        {
                           if ( dollarList )
                           {
                              INT64 temp = 0 ;
                              INT32 dollarNum2 = ossAtoi( z.fieldName() ) ;
                              temp = (((INT64)dollarNum)<<32)|
                                     (((INT64)dollarNum2)&0xFFFFFFFF) ;
                              dollarList->push_back ( temp );
                           }
                        }
                        if ( MATCH == result )
                        {
                           goto done ;
                        }
                     }
                     result = NMATCH ;
                     goto done ;
                  }
                  // $xxx : xxx
                  else
                  {
                     BSONObjIterator it( se.embeddedObject() ) ;
                     BOOLEAN isMatch = FALSE ;
                     while ( it.more() )
                     {
                        BSONElement z = it.next() ;
                        isMatch = _valuesMatch(z, toMatch, op, bm, dollarList) ;
                        if ( ( !isNot && isMatch ) ||
                             ( isNot && !isMatch ) )
                        {
                           if ( dollarList )
                           {
                              //number is z.fieldName()
                              INT64 temp = 0 ;
                              INT32 dollarNum2 = ossAtoi( z.fieldName() ) ;
                              temp = (((INT64)dollarNum)<<32)|
                                     (((INT64)dollarNum2)&0xFFFFFFFF) ;
                              dollarList->push_back ( temp );
                           }
                        }
                        if ( isMatch )
                        {
                           result = MATCH ;
                           goto done ;
                        }
                     }
                     result = NMATCH ;
                     goto done ;
                  }
               }
               else
               {
                  result = NMATCH ;
                  goto done ;
               }
            }
            else
            {
               // xxx.xxx.xxx
               BSONObj eo = se.embeddedObject () ;
               rc = _matches ( p+1, toMatch, rootObj, eo, op,
                               Array == se.type(), isFieldCom,
                               bm, isNot, result, dollarList ) ;
               goto done ;
            }
         }
      }

      // here we know we can't find ".", so let's see what's going on here

      // special handle for array, since we may have multiple values for a given
      // field. In this case any match will return success
      // for example {name: tom, like:{bookname: ['harry potter','dragon world',
      //                                          'lord of ring']}}
      // for a query {like.bookname:'lord of ring'}, first round it will match
      // 'like' and found bookname is Array type, and then it will call _matches
      // again and pass isArray = true
      // Now the input obj itself is array, so we need to make sure handling it
      // properly (note this only happen with nested BSON, the first level
      // _matches should never accept Array
      if ( isArray )
      {
         BSONObjIterator it ( obj ) ;
         BOOLEAN found = FALSE ;
         // by default we return Not Found
         result = NEXIST ;
         while ( it.moreWithEOO() )
         {
            BSONElement z = it.next() ;
            if ( ossStrncmp(z.fieldName(), fieldName, fieldNameLen) == 0 )
            {
               // if we can find the field and we are looking for EXISTS, then
               // we return the expected value ( true for exist(true), false for
               // exist(false)
               if ( BSONObj::opEXISTS == op )
               {
                  result = (toMatch.trueValue()?MATCH:NMATCH) ;
                  goto done ;
               }
               // if we can find the field and we need to compare whether if
               // it's null
               if ( BSONObj::opISNULL == op )
               {
                  result = toMatch.trueValue()?z.isNull():!z.isNull() ;
                  goto done ;
               }
               // if we are looking for some other operator, then let's do
               // compare the value
               if ( _valuesMatch(z, toMatch, op, bm, dollarList) )
               {
                  result = MATCH ;
                  goto done ;
               }
            }
            // here, either the field name doesn't match, or the value doesn't
            // match
            if ( z.type() == Object )
            {
               // if this is an object in array, then we need to pass the field
               // name into the object
               BSONObj eo = z.embeddedObject() ;
               INT32 cmp = 0 ;
               rc = _matches ( fieldName, toMatch, rootObj, eo, op,
                               FALSE, isFieldCom, bm, isNot, cmp, dollarList ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to run _matches, rc: %d", rc ) ;
                  goto error ;
               }
               if ( cmp > 0 )
               {
                  result = MATCH ;
                  goto done ;
               }
               // if we found the element but value is not we are looking for,
               // we shouldn't just return, we need to loop until the full array
               // to make sure there's nothing matches our request
               // in this case, if at the end of search we know we found
               // something but it's not what we need, we'll return -1.
               // otherwise if we never saw the field we'll return 0
               else if ( cmp < 0 )
                  found = TRUE ;
            }
         } // while ( it.moreWithEOO() )
         result = found ? NMATCH : NEXIST ;
         goto done ;
      }
      // if(p) mweans we found "." in the middle of field name
      if ( p )
      {
         result = NEXIST ;
         goto done ;
      }
      // here, fieldName should contain no "."
      e = obj.getField(fieldName) ;
      if ( isFieldCom )
      {
         f = rootObj.getFieldDotted( toMatch.valuestr() ) ;
         if ( f.eoo() )
         {
            result = NMATCH ;
            goto done ;
         }
         pElement = &f ;
      }
      else
      {
         pElement = &toMatch ;
      }
      // if we are looking for EXISTS
      if ( BSONObj::opEXISTS == op )
      {
         // if we don't find the field, then return NEXIST
         if ( e.eoo() )
         {
            result = NEXIST ;
            goto done ;
         }
         else
         {
            // otherwise, if we are looking for exist, then we return MATCH,
            // otherwise we return NMATCH
            result = (toMatch.trueValue()?MATCH:NMATCH) ;
            goto done ;
         }
      }
      // if we are looking for isnull
      else if ( BSONObj::opISNULL == op )
      {
         // if we don't find the field, then return NEXIST
         if ( e.eoo() )
         {
            result = NEXIST ;
            goto done ;
         }
         else
         {
            // otherwise, if we are looking for isnull, then we return MATCH if
            // it's NULL
            result = toMatch.trueValue()?
                        (e.isNull()?MATCH:NMATCH):
                        (e.isNull()?NMATCH:MATCH) ;
            goto done ;
         }
      }
      // in other cases, when it's not Array, or we are looking for array siz,
      // and _valuesMatch return true, then we return MATCH
      else if ( (Array != e.type() || BSONObj::opSIZE == op ) &&
                _valuesMatch(e, *pElement, op, bm, dollarList) )
      {
         result = MATCH ;
         goto done ;
      }
      // if we are dealing with array
      else if ( Array == e.type() && BSONObj::opSIZE != op )
      {
         // loop for all entities and see if we get any value match
         if ( BSONObj::opALL == op )
         {
            // "it" is compare rule here
            BSONObjIterator it( toMatch.embeddedObject() ) ;
            if ( it.more() )
            {
               BSONElement tempEle = it.next() ;
               BSONObjIterator it2( tempEle.embeddedObject() ) ;
               INT32 tempRc = 0 ;
               while ( it2.more() )
               {
                  BSONElement z = it2.next() ;
                  tempRc = _valuesMatch ( e, z, op, bm, dollarList ) ;
                  if ( !tempRc )
                  {
                     result = NMATCH ;
                     goto done ;
                  }
               }
               result = MATCH ;
               goto done ;
            }
            result = NMATCH ;
            goto done ;
         }
         if ( BSONObj::opTYPE == op )
         {
            if ( _valuesMatch ( e, toMatch, op, bm, dollarList ) )
            {
                  result = MATCH ;
                  goto done ;
            }
         }
         BSONObjIterator it( e.embeddedObject() ) ;
         while ( it.moreWithEOO() )
         {
            BSONElement z=it.next() ;
            if ( _valuesMatch ( z, toMatch, op, bm, dollarList ) )
            {
                  result = MATCH ;
                  goto done ;
            }
         }
         // are we looking for the entire array match?
         if ( BSONObj::Equality == op && e.woCompare(toMatch, false) == 0 )
         {
            result = MATCH ;
            goto done ;
         }
         if ( BSONObj::opIN == op && _valuesMatch ( e, toMatch, op,
                                                    bm, dollarList ) )
         {
            result = MATCH ;
            goto done ;
         }
      }
      else if ( e.eoo() )
      {
         result = NEXIST ;
         goto done ;
      }
      result = NMATCH ;

   done :
      // if tempFieldName is not NULL, and it's not point to static buffer, that
      // means we dynamically allocated memory so that we need to free it
      if ( tempFieldName && tempFieldName != &staticBuffer[0] )
         SDB_OSS_FREE ( tempFieldName ) ;
      PD_TRACE_EXITRC ( SDB__MTHMACH_MATCHES, rc );
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__REMACH, "_mthMatcher::_REmatches" )
   BOOLEAN _mthMatcher::_REmatches ( LogicMatchElement *lme,
                                     const BSONObj &obj )
   {
      PD_TRACE_ENTRY ( SDB__MTHMACH__REMACH );
      BOOLEAN result = FALSE;
      const REMatchElement &rme = *(lme->_rme) ;
      BSONElementSet s ;
      obj.getFieldsDotted ( rme._fieldName, s ) ;
      BOOLEAN match = FALSE ;
      for ( BSONElementSet::const_iterator i = s.begin(); i!=s.end(); ++i )
      {
         if ( _regexMatches ( rme, (*i) ) )
         {
            match = TRUE ;
            break ;
         }
      }
      // if any of the condition fail, or can't find the field, we return
      // false
      if ( match )
      {
         result = TRUE ;
      }
      PD_TRACE_EXIT ( SDB__MTHMACH__REMACH );
      return result ;
   }

   //this is matches main function
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__TVSMACH, "_mthMatcher::_traverseMatches" )
   INT32 _mthMatcher::_traverseMatches ( LogicMatchElement *lme,
                                         const BSONObj &obj,
                                         BOOLEAN isNot,
                                         INT32 &result,
                                         vector<INT64> *dollarList )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__TVSMACH );
      // r must be set to 1 here, because when there's no element in $and array,
      // r will be returned as default value
      INT32 r = 1 ;
      INT32 tempR = 0 ;
      BOOLEAN isMatchErr = FALSE ;
      LogicMatchElement *temp = NULL ;
      //when logic type is and or not, we must match its every sub leaf
      if ( MTH_LOGIC_AND == lme->_logicType )
      {
         for ( UINT32 i = 0; i < lme->_vlme.size(); i++ )
         {
            temp = lme->_vlme[i] ;
            rc = _traverseMatches ( temp, obj, isNot, r, dollarList ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to call _traverseMatches, rc: %d",
                        rc ) ;
               goto error ;
            }
            if ( !r )
            {
               if ( !lme->_matchAll || !dollarList )
               {
                  goto done ;
               }
               isMatchErr = TRUE ;
               tempR = r ;
            }
         }
         if ( lme->_matchAll && dollarList && isMatchErr )
         {
            r = tempR ;
         }
         goto done ;
      }
      else if ( MTH_LOGIC_OR == lme->_logicType )
      {
         for ( UINT32 i = 0; i < lme->_vlme.size(); i++ )
         {
            temp = lme->_vlme[i] ;
            rc = _traverseMatches ( temp, obj, isNot, r, dollarList ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to call _traverseMatches, rc: %d",
                        rc ) ;
               goto error ;
            }
            if ( r )
            {
               if ( !lme->_matchAll || !dollarList )
               {
                  goto done ;
               }
               isMatchErr = TRUE ;
               tempR = r ;
            }
         }
         if ( lme->_matchAll && dollarList && isMatchErr )
         {
            r = tempR ;
         }
         goto done ;
      }
      else if ( MTH_LOGIC_NOT == lme->_logicType )
      {
         for ( UINT32 i = 0; i < lme->_vlme.size(); i++ )
         {
            temp = lme->_vlme[i] ;
            rc = _traverseMatches ( temp, obj, !isNot, r, dollarList ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to call _traverseMatches, rc: %d",
                        rc ) ;
               goto error ;
            }
            r = !r ;
            if ( r )
            {
               if ( !lme->_matchAll || !dollarList )
               {
                  goto done ;
               }
               isMatchErr = TRUE ;
               tempR = r ;
            }
         }
         if ( lme->_matchAll && dollarList && isMatchErr )
         {
            r = tempR ;
         }
         goto done ;
      }
      // lme->_logicType == MTH_LOGIC_OTHER
      // when logic type is not and or not, we will begin to match
      else
      {
         // if is normal match
         if ( !lme->_isRegex )
         {
            const MatchElement &me = *(lme->_me) ;
            const BSONElement &be = me._toMatch ;
            rc = _matches ( be.fieldName(), be, obj, obj,
                            me._op, FALSE, lme->_isFieldCom,
                            me, isNot, r, dollarList ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to call _matcher, rc: %d", rc ) ;
               goto error ;
            }
            // NEXIST
            if ( NEXIST == r )
            {
               if ( BSONObj::opEXISTS == me._op )
               {
                  // when we can't find the element, special handling for $exist
                  // for example,if we have {name,{$exist,false}}, that means we
                  // should return true for all the records where "name" doesn't
                  // exist
                  /* so if we can't find the field and we are checking opEXISTS
                   * we */
                  // should reverse the requested value for result (true means
                  // failed,
                  // false means success)
                  r = -(be.trueValue()?MATCH:NMATCH);
               }
               else if ( BSONObj::opISNULL == me._op )
               {
                  // special handle for $isnull, true for match ( not exist )
                  // false for unmatch ( exists )
                  r = (be.trueValue()?MATCH:NMATCH) ;
               }
               // add more special conditions here when can't find the element
            }
            // if any of the condition fail, or can't find the field, we return
            // false
            if ( r <= 0 )
            {
               r = FALSE ;
               goto done ;
            }
         }
         // else, is regex match
         else
         {
            r = _REmatches ( lme, obj ) ;
            goto done ;
         }
      }

      done :
         result = r ;
         PD_TRACE_EXITRC ( SDB__MTHMACH__TVSMACH, rc );
         return rc ;
      error :
         goto done ;
   }

   // check whether a target BSON Object matches our search
   // this is match interface
   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH_MATCHES2, "_mthMatcher::matches" )
   INT32 _mthMatcher::matches ( const BSONObj &matchTarget,
                                BOOLEAN &result,
                                vector<INT64> *dollarList )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH_MATCHES2 );
      //map<INT32,INT32>::iterator it ;
      INT32 r = FALSE ;
      SDB_ASSERT( _initialized, "The matcher has not been initialized, please "
                 "call 'loadPattern' before using it" ) ;
      if ( _matchesAll )
      {
         r = TRUE ;
         goto done ;
      }
      try
      {
         rc = _traverseMatches ( _rlme, matchTarget, FALSE, r, dollarList ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to call _matcher" ) ;
            goto error ;
         }
      }
      catch (  std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to match: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      /*if ( dollerList )
      {
         for ( it = dollerList->begin(); it != dollerList->end(); ++it )
         {
            PD_LOG ( PDERROR, "number1: %d , number2: %d", it->first, it->second );
         }
      }*/
   done :
      result = r ;
      PD_TRACE_EXITRC ( SDB__MTHMACH_MATCHES2, rc );
      return rc ;
   error :
      goto done ;
   }

   /*PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH_GETDOLLARNUMBER,"_mthMatcher::_getDollarNumber" )
   INT32 _mthMatcher::_getDollarNumber ( const CHAR *pFieldName, INT32 &number )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH_GETDOLLARNUMBER );
      const CHAR *pField = pFieldName ;
      if ( !pFieldName ||
           !(*pFieldName) ||
           MTH_OPERATOR_EYECATCHER != *pFieldName )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ++pFieldName ;
      number = 0 ;
      while ( pFieldName &&
              *pFieldName )
      {
         if ( *pFieldName >= '0' &&
              *pFieldName <= '9' )
         {
            number *= 10 ;
            number += ( *pFieldName - '0' ) ;
            ++pFieldName ;
         }
         else if ( '.' == *pFieldName )
         {
            if ( pFieldName - pField <= 1 )
            {
               rc = SDB_INVALIDARG ;
            }
            goto done ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC ( SDB__MTHMACH_GETDOLLARNUMBER, rc );
      return rc ;
   error:
      goto done ;
   }*/
}

