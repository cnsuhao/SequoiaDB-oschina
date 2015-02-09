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
         if ( e.type() != Object )
         {
            PD_LOG ( PDERROR, "eleMatch expects Object" ) ;
            throw pdGeneralException ( "eleMatch expects Object" ) ;
         }
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

      switch ( ele.type() )
      {
      case RegEx:
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
         
         break ;

      }
      case Array:
      case Object:
      {
         BSONObjIterator j( ele.embeddedObject() ) ;
         BSONObjIterator *jt = NULL ;
         BSONElement eleTemp ;
         const CHAR *eFieldName = ele.fieldName() ;
         if ( MTH_OPERATOR_EYECATCHER == eFieldName[0] )
         {
            if ( eFieldName[1] == 'a' && eFieldName[2] == 'n' &&
                 eFieldName[3] == 'd' && eFieldName[4] == 0 )
            {
               LogicMatchElement *clme ;
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
            else if ( eFieldName[1] == 'o' && eFieldName[2] == 'r' &&
                      eFieldName[3] == 0 )
            {
               LogicMatchElement *clme ;
               LogicMatchElement *clme2 ;
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
            else if ( eFieldName[1] == 'n' && eFieldName[2] == 'o' &&
                      eFieldName[3] == 't' && eFieldName[4] == 0 )
            {
               LogicMatchElement *clme ;
               LogicMatchElement *clme2 ;
               rc = _createLME ( lme, &clme2, MTH_LOGIC_NOT ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed call create LogicMatcherElment,"
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
            else
            {
               rc = SDB_INVALIDARG ;
               PD_LOG ( PDERROR, "operator can not in the first" ) ;
               goto error ;
            }
         }
         else if ( Array == ele.type() )
         {
            if ( predicatable )
            {
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
         else
         {
            if ( _checkValueNonRec ( ele ) )
            {
               if ( predicatable )
               {
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
                  const CHAR *embFieldName = embEle.fieldName () ;

                  if ( MTH_OPERATOR_EYECATCHER == embFieldName[0] &&
                       Object != embEle.type() && Array != embEle.type() )
                  {
                     _MTH_MATCHER_FIELD_TYPE fieldCom = MTH_MATCHER_FIELD_NOT ;
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
                  else if ( MTH_OPERATOR_EYECATCHER == embFieldName[0] &&
                           (Object == embEle.type() || Array == embEle.type()) )
                  {
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
                  else
                  {
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
      default:
      {
         const CHAR *eFieldName = ele.fieldName() ;
         if ( MTH_OPERATOR_EYECATCHER == eFieldName[0] )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "operator can not in the head" ) ;
            goto error ;
         }
         else
         {
            if ( predicatable )
            {
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

   INT32 _mthMatcher::_createBsonBuilder( BSONObjBuilder **builder )
   {
      SDB_ASSERT( NULL != builder, "builder should not be null" ) ;

      INT32 rc = SDB_OK ;
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
         PD_LOG ( PDERROR, "Failed to push builder to builder list: %s",
                  e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      *builder = b ;

   done:
      return rc ;
   error:
      if ( b )
      {
         SDB_OSS_DEL b ;
         b = NULL ;
      }
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
      if ( predicatable )
      {
         _predicateSet.addPredicate ( ele.fieldName(), embEle, isNot ) ;
      }

      switch ( op )
      {
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

         BSONObjBuilder *b = NULL ;
         rc = _createBsonBuilder( &b ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR, "Failed to create bson builder:rc=%d", rc ) ;
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
         LogicMatchElement *clme = NULL ;
         rc = _createLME ( lme, &clme, MTH_LOGIC_OTHER ) ;
         if ( rc )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "Failed to create LogicMatchElement" ) ;
            goto error ;
         }
         if ( ele.isABSONObj() && ele.embeddedObject().nFields() > 1 )
         {
            BSONObj tmpObj ;
            BSONObjBuilder *tmpBuilder = NULL ;
            rc = _createBsonBuilder( &tmpBuilder ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG ( PDERROR, "Failed to create bson builder:rc=%d", rc ) ;
               goto error ;
            }

            tmpObj = BSON( embEle.fieldName() << embEle.embeddedObject() ) ;
            tmpBuilder->append( ele.fieldName(), tmpObj ) ;

            clme->_me = SDB_OSS_NEW MatchElement( 
                                             tmpBuilder->done().firstElement(),
                                             ( BSONObj::MatchType )op,
                                             embEle.embeddedObject() ) ;

         }
         else
         {
            clme->_me = SDB_OSS_NEW MatchElement( ele, ( BSONObj::MatchType )op,
                                                  embEle.embeddedObject() ) ;
         }
         
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
         _setWeight ( clme, type ) ;

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
         _setWeight ( clme, type ) ;
         if ( isFieldCom )
         {
            clme->_isFieldCom = TRUE ;
         }
         clme->_me = SDB_OSS_NEW MatchElement ( ele, type ) ;
         if ( !clme->_me )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "Failed to allocate memory for MatchElement" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
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
         _rlme = SDB_OSS_NEW LogicMatchElement() ;
         if ( !_rlme )
         {
            PD_LOG ( PDERROR, "Failed to allocate memory for "
                     "LogicMatchElement" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
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
            _matchesAll = FALSE ;
         }
         _setWeight ( _rlme ) ;
         _sortLME ( _rlme ) ;
         _checkTotallyConverted( _rlme ) ;
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
         INT32 count = bm._myset.count(l) ;
         if ( count )
            return count ;
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
         if ( Array != l.type() && Object != l.type() )
         {
            BSONObjIterator it( r.embeddedObject() ) ;
            if ( !it.more() )
            {
               return FALSE ;
            }
            BSONElement tempEle = it.next() ;
            BSONObjIterator it2( tempEle.embeddedObject() ) ;
            BOOLEAN tempRc = FALSE ;
            while ( it2.more() )
            {
               BSONElement z = it2.next() ;
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
         else if ( Array == l.type () )
         {
            set<BSONElement, element_lt> _myset ;
            BSONObjIterator it ( l.embeddedObject() ) ;
            while ( it.more() )
            {
               BSONElement ie = it.next() ;
               _myset.insert(ie) ;
            }
            INT32 count = _myset.count( r ) ;
            if ( count )
               return count ;
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
      INT32 c = compareElementValues ( l, r ) ;
      if ( c<-1) c= -1 ;
      if ( c>1)  c= 1 ;
      INT32 z = 1<<(c+1) ;
      return (op&z) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__DELLME, "_mthMatcher::_deleteLME" )
   void _mthMatcher::_deleteLME ( LogicMatchElement *lme )
   {
      SDB_ASSERT ( lme, "lme can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__DELLME );
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

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__SETWGH, "_mthMatcher::_setWeight" )
   void _mthMatcher::_setWeight ( LogicMatchElement *lme,
                                  BSONObj::MatchType op
                                )
   {
      UINT32 num = 0 ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__SETWGH );
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

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__SETWGH2, "_mthMatcher::_setWeight" )
   void _mthMatcher::_setWeight ( LogicMatchElement *lme )
   {
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

   BOOLEAN _mthMatcher::compare ( LogicMatchElement *l1, LogicMatchElement *l2 )
   {
      return l1->_weight < l2->_weight ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__SRLME, "_mthMatcher::_sortLME" )
   void _mthMatcher::_sortLME ( LogicMatchElement *lme )
   {
      PD_TRACE_ENTRY ( SDB__MTHMACH__SRLME );
      UINT32 vSize = lme->_vlme.size() ;
      if ( MTH_LOGIC_OTHER != lme->_logicType )
      {
         LogicMatchElement *temp ;
         for ( UINT32 i = 0; i < vSize; i++ )
         {
            temp = lme->_vlme[i] ;
            _sortLME ( temp ) ;
         }
         std::sort ( lme->_vlme.begin(), lme->_vlme.end(), compare ) ;
      }
      PD_TRACE_EXIT ( SDB__MTHMACH__SRLME );
   }

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
      *clme = SDB_OSS_NEW LogicMatchElement () ;
      if ( !(*clme) )
      {
         PD_LOG ( PDERROR, "Failed to allocate memory for LogicMatchElement" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      try
      {
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
         const CHAR *eFieldName = ele.fieldName() ;
         if ( ele.getGtLtOp(-1) == BSONObj::opELEM_MATCH )
            return TRUE ;
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
      else
      {
         const CHAR *eFieldName = ele.fieldName() ;
         return MTH_OPERATOR_EYECATCHER != eFieldName[0] ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__CNTELE, "_mthMatcher::_countElement" )
   void _mthMatcher::_countElement ( const BSONElement &ele, UINT32 &countOp )
   {
      PD_TRACE_ENTRY ( SDB__MTHMACH__CNTELE );
      if ( Array == ele.type() )
      {
         const CHAR *eFieldName = ele.fieldName() ;
         if ( MTH_OPERATOR_EYECATCHER == eFieldName[0] )
         {
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
         }
      }
      else
      {
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
      CHAR staticBuffer [ MTH_MATCH_STATIC_BUFFER_SZ+1 ] = {0} ;
      tempFieldName = &staticBuffer[0] ;
      fieldNameLen = ossStrlen ( fieldName ) ;
      if ( fieldNameLen > MTH_MATCH_STATIC_BUFFER_SZ )
      {
         tempFieldName = ossStrdup ( fieldName ) ;
         if ( !tempFieldName )
         {
            pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                    "Failed to duplicate field name %s", fieldName ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }
      else
      {
         ossStrncpy ( tempFieldName, fieldName, fieldNameLen ) ;
      }

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
         if ( NEXIST == result )
            result = MATCH ;
         goto done ;
      }
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
         if ( NEXIST == result )
            result = MATCH ;
         goto done ;
      }

      p = ossStrnchr ( tempFieldName, MTH_FIELDNAME_SEP, fieldNameLen ) ;
      if ( p )
      {
         *p = '\0' ;
         BSONElement se = obj.getField(tempFieldName) ;
         if ( se.eoo() || (se.type() !=Object && se.type() != Array) )
         {
         }
         else
         {
            INT32 childFieldNameLen = 0 ;
            INT32 dollarNum = 0 ;
            CHAR *pChildFieldName = NULL ;
            if ( MTH_OPERATOR_EYECATCHER == *(p + 1) )
            {
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
               BSONObj eo = se.embeddedObject () ;
               rc = _matches ( p+1, toMatch, rootObj, eo, op,
                               Array == se.type(), isFieldCom,
                               bm, isNot, result, dollarList ) ;
               goto done ;
            }
         }
      }


      if ( isArray )
      {
         BSONObjIterator it ( obj ) ;
         BOOLEAN found = FALSE ;
         result = NEXIST ;
         while ( it.moreWithEOO() )
         {
            BSONElement z = it.next() ;
            if ( ossStrncmp(z.fieldName(), fieldName, fieldNameLen) == 0 )
            {
               if ( BSONObj::opEXISTS == op )
               {
                  result = (toMatch.trueValue()?MATCH:NMATCH) ;
                  goto done ;
               }
               if ( BSONObj::opISNULL == op )
               {
                  result = toMatch.trueValue()?z.isNull():!z.isNull() ;
                  goto done ;
               }
               if ( _valuesMatch(z, toMatch, op, bm, dollarList) )
               {
                  result = MATCH ;
                  goto done ;
               }
            }
            if ( z.type() == Object )
            {
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
               else if ( cmp < 0 )
                  found = TRUE ;
            }
         } // while ( it.moreWithEOO() )
         result = found ? NMATCH : NEXIST ;
         goto done ;
      }
      if ( p )
      {
         result = NEXIST ;
         goto done ;
      }
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
      if ( BSONObj::opEXISTS == op )
      {
         if ( e.eoo() )
         {
            result = NEXIST ;
            goto done ;
         }
         else
         {
            result = (toMatch.trueValue()?MATCH:NMATCH) ;
            goto done ;
         }
      }
      else if ( BSONObj::opISNULL == op )
      {
         if ( e.eoo() )
         {
            result = NEXIST ;
            goto done ;
         }
         else
         {
            result = toMatch.trueValue()?
                        (e.isNull()?MATCH:NMATCH):
                        (e.isNull()?NMATCH:MATCH) ;
            goto done ;
         }
      }
      else if ( (Array != e.type() || BSONObj::opSIZE == op ) &&
                _valuesMatch(e, *pElement, op, bm, dollarList) )
      {
         result = MATCH ;
         goto done ;
      }
      else if ( Array == e.type() && BSONObj::opSIZE != op )
      {
         if ( BSONObj::opALL == op )
         {
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

         BSONObj eEmbObj = e.embeddedObject() ;
         if ( ( BSONObj::opIN == op ) && ( eEmbObj.nFields() == 0 )
              && ( bm._myset.size() == 0 ) )
         {
            result = MATCH ;
            goto done ;
         }

         BSONObjIterator it( eEmbObj ) ;
         while ( it.moreWithEOO() )
         {
            BSONElement z=it.next() ;
            if ( _valuesMatch ( z, toMatch, op, bm, dollarList ) )
            {
                  result = MATCH ;
                  goto done ;
            }
         }
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
      if ( match )
      {
         result = TRUE ;
      }
      PD_TRACE_EXIT ( SDB__MTHMACH__REMACH );
      return result ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__TVSMACH, "_mthMatcher::_traverseMatches" )
   INT32 _mthMatcher::_traverseMatches ( LogicMatchElement *lme,
                                         const BSONObj &obj,
                                         BOOLEAN isNot,
                                         INT32 &result,
                                         vector<INT64> *dollarList )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH__TVSMACH );
      INT32 r = 1 ;
      INT32 tempR = 0 ;
      BOOLEAN isMatchErr = FALSE ;
      LogicMatchElement *temp = NULL ;
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
      else
      {
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
            if ( NEXIST == r )
            {
               if ( BSONObj::opEXISTS == me._op )
               {
                  /* so if we can't find the field and we are checking opEXISTS
                   * we */
                  r = -(be.trueValue()?MATCH:NMATCH);
               }
               else if ( BSONObj::opISNULL == me._op )
               {
                  r = (be.trueValue()?MATCH:NMATCH) ;
               }
            }
            if ( r <= 0 )
            {
               r = FALSE ;
               goto done ;
            }
         }
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

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH_MATCHES2, "_mthMatcher::matches" )
   INT32 _mthMatcher::matches ( const BSONObj &matchTarget,
                                BOOLEAN &result,
                                vector<INT64> *dollarList )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHMACH_MATCHES2 );
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

   PD_TRACE_DECLARE_FUNCTION ( SDB__MTHMACH__CHKTOTALLYCONVERTED, "_mthMatcher::_checkTotallyConverted" )
   void _mthMatcher::_checkTotallyConverted( LogicMatchElement *ele )
   {
      const LogicMatchElement *lme = ele ;
      if ( MTH_LOGIC_OR == lme->_logicType ||
           MTH_LOGIC_NOT == lme->_logicType )
      {
         _totallyConverted = FALSE ;
      }
      else if ( MTH_LOGIC_OTHER == lme->_logicType )
      {
         const _MatchElement *me = lme->_me ;
         if ( lme->_isRegex ||
              ( BSONObj::Equality != me->_op &&
                BSONObj::GT != me->_op &&
                BSONObj::GTE != me->_op &&
                BSONObj::LT != me->_op &&
                BSONObj::LTE != me->_op ) )
         {
            _totallyConverted = FALSE ;
         }
         else if ( BSONObj::Equality == me->_op &&
                   Array == me->_toMatch.type() )
         {
            _totallyConverted = FALSE ;
         }
      }
      else
      {
         SDB_ASSERT( MTH_LOGIC_AND == lme->_logicType,
                     "logictype must be and" ) ;
         vector<_LogicMatchElement *>::const_iterator itr =
                                      lme->_vlme.begin() ;
         for ( ; itr != lme->_vlme.end(); ++itr )
         {
            _checkTotallyConverted( *itr ) ;
            if ( !_totallyConverted )
            {
               break ;
            }
         }
      }
      return ;
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

