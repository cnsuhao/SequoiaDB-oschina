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

   Source File Name = mthSelector.hpp

   Descriptive Name = Method Selector

   When/how to use: this program may be used on binary and text-formatted
   versions of Method component. This file contains functions for creating a new
   record based on old record and selecting rule.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include <algorithm>
#include "pd.hpp"
#include "mthSelector.hpp"
#include "pdTrace.hpp"
#include "mthTrace.hpp"
#include "mthCommon.hpp"
#include "../util/rawbson2json.h"
using namespace bson ;
using namespace std ;
namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSEL__ADDSEL, "_mthSelector::_addSelector" )
   INT32 _mthSelector::_addSelector ( const BSONElement &ele )
   {
      PD_TRACE_ENTRY ( SDB__MTHSEL__ADDSEL );
      SelectorElement me( ele ) ;
      _selectorElements.push_back( me ) ;
      PD_TRACE_EXIT ( SDB__MTHSEL__ADDSEL );
      return SDB_OK ;
   }

   BOOLEAN _selectorElementsSort ( const SelectorElement &l,
                                   const SelectorElement &r )
   {
      FieldCompareResult result = compareDottedFieldNames (
         l._toSelect.fieldName(), r._toSelect.fieldName() ) ;
      return ((result == RIGHT_SUBFIELD) || (result == LEFT_BEFORE)) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSEL_LDPATTERN, "_mthSelector::loadPattern" )
   INT32 _mthSelector::loadPattern ( const BSONObj &selectorPattern )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHSEL_LDPATTERN );
      _selectorPattern = selectorPattern.copy() ;
      BSONObjIterator i(_selectorPattern) ;
      INT32 eleNum = 0 ;
      while ( i.more() )
      {
         rc = _addSelector( i.next() ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to parse match pattern[%s, pos: %d], "
                     "rc: %d", selectorPattern.toString().c_str(), eleNum,
                     rc ) ;
            goto error ;
         }
         eleNum ++ ;
      }
      std::sort( _selectorElements.begin(), _selectorElements.end(),
                 _selectorElementsSort ) ;

      _initialized = TRUE ;

   done :
      PD_TRACE_EXITRC ( SDB__MTHSEL_LDPATTERN, rc );
      return rc ;
   error :
      goto done ;
   }

   BOOLEAN _mthSelector::_dupFieldName ( const BSONElement &l,
                                         const BSONElement &r )
   {
      return !l.eoo() && !r.eoo() && (l.rawdata() != r.rawdata()) &&
        ossStrncmp(l.fieldName(),r.fieldName(),ossStrlen(r.fieldName()))==0 ;
   }

   // when requested update want to change something that not exist in original
   // object, we need to append the original object in those cases
   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSEL__APPNEW, "_mthSelector::_appendNew" )
   template<class Builder>
   INT32 _mthSelector::_appendNew ( const CHAR *pFieldName, Builder& b,
                                    SINT32 *selectorIndex )
   {
      PD_TRACE_ENTRY ( SDB__MTHSEL__APPNEW );
      SelectorElement me = _selectorElements[(*selectorIndex)] ;
      b.appendAs ( me._toSelect, pFieldName ) ;

      // here we actually consume selector, then we add index
      _incSelectorIndex( selectorIndex ) ;

      PD_TRACE_EXIT ( SDB__MTHSEL__APPNEW );
      return SDB_OK ;
   }

   // Builder could be BSONObjBuilder or BSONArrayBuilder
   // _appendNewFromMods appends the current builder with the new field
   // root represent the current fieldName, me is the current selector element
   // b is the builder, onedownseen represent the all subobjects have been
   // processed in the current object, and selectorIndex is the pointer for
   // current selector
   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSEL__APPNEWFRMMODS, "_mthSelector::_appendNewFromMods" )
   template<class Builder>
   INT32 _mthSelector::_appendNewFromMods ( CHAR **ppRoot,
                                            INT32 &rootBufLen,
                                            INT32 rootLen,
                                            Builder &b,
                                            SINT32 *selectorIndex)
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHSEL__APPNEWFRMMODS );
      SelectorElement me = _selectorElements[(*selectorIndex)] ;
      INT32 newRootLen = rootLen ;

      // if the selected request does not exist in original one
      // first let's see if there's nested object in the request
      // ex. current root is user.name
      // however request is user.name.first.origin
      // in this case we'll have to create sub object 'first'

      // note fieldName is the FULL path "user.name.first.origin"
      // root is user.name.
      const CHAR *fieldName = me._toSelect.fieldName() ;
      // now temp is "first.origin"
      const CHAR *temp = fieldName + rootLen ;
      // find the "." starting from root length
      const CHAR *dot = ossStrchr ( temp,'.') ;

      if ( dot )
      {
         *(CHAR*)dot = 0 ;
      }
      const CHAR *pCurField = *ppRoot + newRootLen ;
      rc = mthAppendString( ppRoot, rootBufLen, newRootLen, temp, -1,
                            &newRootLen ) ;
      // Restore
      if ( dot )
      {
         *(CHAR*)dot = '.' ;
      }
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to append string, rc: %d", rc ) ;
         goto error ;
      }

      // given example
      // user.name.first.origin
      // |         ^    #
      // | represent fieldName
      // ^ represent temp
      // # represent dot
      // if there is sub object
      if ( dot )
      {
         // create object builder for nf ("first" field)
         BSONObjBuilder bb ( b.subobjStart( pCurField ) ) ;
         // create a es for empty object
         const BSONObj obj ;
         BSONObjIteratorSorted es(obj);

         // append '.'
         rc = mthAppendString ( ppRoot, rootBufLen, newRootLen, ".", 1,
                                &newRootLen ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to append string, rc: %d", rc ) ;
            goto error ;
         }

         // create an object for path "user.name.first."
         // bb is the new builder, es is iterator
         // selectorIndex is the index
         // note now root is nr.c_str() instead of original root
         rc = _buildNewObj ( ppRoot, rootBufLen, newRootLen, bb, es,
                             selectorIndex ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to build new object for %s, rc: %d",
                     me._toSelect.toString().c_str(), rc ) ;
            goto error ;
         }
      }
      // if we can't find ".", then we are not embedded BSON, let's just
      // create whatever object we asked
      // for example current root is "user.name."
      // and we want {$set: {user.name.firstname, "tao wang"}}
      // here temp will be firstname, and dot will be NULL
      else
      {
         // call _appendNew to append selected element into the current builder
         try
         {
            rc = _appendNew ( pCurField, b, selectorIndex ) ;
         }
         catch( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to append for %s: %s",
                     me._toSelect.toString().c_str(), e.what() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to append for %s, rc: %d",
                     me._toSelect.toString().c_str(), rc ) ;
            goto error ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB__MTHSEL__APPNEWFRMMODS, rc );
      return rc ;
   error :
      goto done ;
   }

   // Builder could be BSONObjBuilder or BSONArrayBuilder
   // This function is recursively called to build new object
   // The prerequisit is that _selectorElement is sorted, which supposed to
   // happen at end of loadPattern
   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSEL__BLDNEWOBJ, "_mthSelector::_buildNewObj" )
   template<class Builder>
   INT32 _mthSelector::_buildNewObj ( CHAR **ppRoot,
                                      INT32 &rootBufLen,
                                      INT32 rootLen,
                                      Builder &b,
                                      BSONObjIteratorSorted &es,
                                      SINT32 *selectorIndex )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHSEL__BLDNEWOBJ );
      // get the next element in the object
      BSONElement e = es.next() ;
      // previous element is set to empty
      BSONElement prevE ;
      INT32 newRootLen = rootLen ;

      // loop until we hit end of original object, or end of selector list
      while( !e.eoo() && (*selectorIndex) < (SINT32)_selectorElements.size() )
      {
         // if we get two elements with same field name, we don't need to
         // continue checking, simply ignore
         if ( _dupFieldName( prevE, e ) )
         {
            prevE = e ;
            e = es.next() ;
            continue ;
         }
         prevE = e ;

         // every time we build the current field, let's set root to original
         (*ppRoot)[rootLen] = '\0' ;
         newRootLen = rootLen ;

         // construct the full path of the current field name
         // say current root is "user.employee.", and this object contains
         // "name, age" fields, then first loop we get user.employee.name
         // second round get user.employee.age
         rc = mthAppendString ( ppRoot, rootBufLen, newRootLen,
                                e.fieldName(), -1, &newRootLen ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to append string, rc: %d", rc ) ;

         // compare the full field name with requested update field
         FieldCompareResult cmp = compareDottedFieldNames (
               _selectorElements[(*selectorIndex)]._toSelect.fieldName(),
               *ppRoot ) ;

         // compare the full path
         // we have few situations need to handle
         // 1) current field is a parent of requested field
         // for example, currentfield = user, requested field = user.name.test
         // this situation called LEFT_SUBFIELD

         // 2) current field is same as requested field
         // for example both current field and requests are user.name.test
         // this situation called SAME

         // 3) current field is not same as requested field, and alphabatically
         // current field is greater than requested field
         // for example current field is user.myname, requested fialed is
         // user.abc
         // this situation called LEFT_BEFORE

         // 4) current field is not same as requested field, and alphabatically
         // current field is smaller than requested field
         // for example current field is user.myname, requested field is
         // user.name
         // this situation called RIGHT_BEFORE

         // 5) requested field is a parent of current field
         // for example current field is user.name.test, requested field is user
         // howwever since we are doing merge, this situation should NEVER
         // HAPPEN!!
         switch ( cmp )
         {
         case LEFT_SUBFIELD:
            // ex, select request $set:{user.name,"taoewang"}
            // field: user
            // make sure the BSONElement is object or array
            // if the requested field already exist but it's not object nor
            // array, we should report error since we can't continue building
            // subfield
            if ( e.type() != Object && e.type() != Array )
            {
               PD_LOG ( ( _ignoreTypeError ? PDDEBUG : PDERROR ),
                        "not object or array: %s",
                        e.toString().c_str() ) ;
               if ( _ignoreTypeError )
               {
                  e = es.next() ;
                  prevE = e ; // ignore _dupFieldName check
                  continue ;
               }
               else
               {
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
            }

            // add "." at end
            rc = mthAppendString ( ppRoot, rootBufLen, newRootLen, ".", 1,
                                   &newRootLen ) ;
            PD_RC_CHECK ( rc, PDERROR, "Failed to append string, rc: %d", rc ) ;

            // insert into list
            //onedownseen.insert(e.fieldName()) ;
            // if we are dealing with object, then let's create a new object
            // builder starting from our current fieldName
            if ( e.type() == Object )
            {
               BSONObjBuilder bb(b.subobjStart(e.fieldName()));
               // get the object for the current element, and create sorted
               // iterator on it
               BSONObjIteratorSorted bis(e.Obj());

               // add fieldname into path and recursively call _buildNewObj
               // to create embedded object
               // root is original root + current field + .
               // bb is new object builder
               // bis is the sorted iterator
               // selectorIndex is the current selector we are working on
               rc = _buildNewObj ( ppRoot, rootBufLen, newRootLen, bb, bis,
                                   selectorIndex ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to build object: %s, rc: %d",
                           e.toString().c_str(), rc ) ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
               // call bb.done() to close the builder
               bb.done() ;
            }
            else
            {
               // if it's not object, then we must have array
               // now let's create BSONArrayBuilder
               BSONArrayBuilder ba(b.subarrayStart(e.fieldName()));
               //BSONArrayIteratorSorted bis(BSONArray(e.embeddedObject()));
               BSONObjIteratorSorted bis(e.embeddedObject());
               // add fieldname into path and recursively call _buildNewObj
               // to create embedded object
               // root is original root + current field + .
               // ba is new array builder
               // bis is the sorted iterator
               // selectorIndex is the current selector we are working on
               rc = _buildNewObj ( ppRoot, rootBufLen, newRootLen, ba, bis,
                                   selectorIndex ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to build array: %s, rc: %d",
                           e.toString().c_str(), rc );
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
               ba.done();
            }
            // process to the next element
            e = es.next() ;
            // note we shouldn't touch selectorIndex here, we should only
            // change it at the place actually consuming it
         break ;

         case LEFT_BEFORE:
            // if the selector request does not exist in original one
            // first let's see if there's nested object in the request
            // ex. current root is user. and our first element is "name"
            // however request is user.address
            // in this case we'll have to create sub object 'address' first

            // _appendNewFromMods appends the current builder with the new field
            // _selectorElement[selectorIndex] represents the current
            // SelectorElement, b is the builder, root is the string of current
            // root field, onedownseen is the set for all subobjects

            // first let's revert root to original
            (*ppRoot)[rootLen] = '\0' ;
            newRootLen = rootLen ;
            rc = _appendNewFromMods ( ppRoot, rootBufLen, newRootLen,
                                      b, selectorIndex) ;
            PD_RC_CHECK ( rc, PDERROR, "Failed to append for %s, rc: %d",
                          _selectorElements[(*selectorIndex)
                          ]._toSelect.toString().c_str(), rc ) ;
            // note we don't change e here because we just add the field
            // requested by selector into new object, the original e shoudln't
            // be changed.

            // we also don't change selectorIndex here since it should be
            // changed by the actual consumer function, not in this loop
            break ;

         case SAME:
            // in this situation, the requested field is the one we are
            // processing, so that we don't need to change object metadata,
            // let's just apply the change
            // e is the current element, b is the current builder, selectorIndex
            // is the current selector
            b.append( e ) ;
            _incSelectorIndex( selectorIndex ) ;
            break ;

         case RIGHT_BEFORE:
            // in this situation, the original field is alphabetically ahead of
            // requested field.
            // for example current field is user.name but requested field is
            // user.plan, then we simply add the field into new object
            // original object doesn't need to change

            // In the situation we are processing different object, for example
            // requested update field is user.newfield.test
            // current processing e is mydata.test
            // in this case, we should skip this element since it's not part of
            // select list
            e = es.next() ;
            break ;

         case RIGHT_SUBFIELD:
         default :
            //we should never reach this codepath
            PD_RC_CHECK ( SDB_SYS, PDERROR, "Reaching unexpected codepath, "
                          "selector: %s, root: %s, rc: %d",
                          _selectorElements[(*selectorIndex)
                          ]._toSelect.toString().c_str(), *ppRoot, rc ) ;
         }
      }
      // we break out the loop either hitting end of original object, or end of
      // the selector list
      // we are going to skip all leftover original elements

      // revert root to original
      while ( (*selectorIndex)<(SINT32)_selectorElements.size() )
      {
         (*ppRoot)[rootLen] = '\0' ;
         newRootLen = rootLen ;
         FieldCompareResult cmp = compareDottedFieldNames (
               _selectorElements[(*selectorIndex)]._toSelect.fieldName(),
               *ppRoot ) ;
         if ( LEFT_SUBFIELD == cmp )
         {
            rc = _appendNewFromMods ( ppRoot, rootBufLen, newRootLen,
                                      b, selectorIndex ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to append for %s, rc: %d",
                        _selectorElements[(*selectorIndex)
                        ]._toSelect.toString().c_str(), rc ) ;
               goto error ;
            }
         }
         else
         {
            goto done ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB__MTHSEL__BLDNEWOBJ, rc );
      return rc ;
   error :
      goto done ;
   }

   // 4 bytes size, 1 byte type, 1 byte 0, 4 bytes string length
   #define FIRST_ELEMENT_STARTING_POS     10
   #define MAX_SELECTOR_BUFFER_THRESHOLD  67108864 // 64MB

   // given a source BSON object and empty target, the returned target will
   // contains selected fields

   // since we are dealing with tons of BSON object conversion, this part should
   // ALWAYS protected by try{} catch{}
   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSEL_SELECT, "_mthSelector::select" )
   INT32 _mthSelector::select ( const BSONObj &source, BSONObj &target )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHSEL_SELECT );
      SDB_ASSERT(_initialized, "The selector has not been initialized, please "
                               "call 'loadPattern' before using it" ) ;
      SDB_ASSERT(target.isEmpty(), "target should be empty") ;

      // create a builder with 10% extra space for buffer
      BSONObjBuilder builder ( (INT32)(source.objsize()*1.1) );
      // create sorted iterator
      BSONObjIteratorSorted es(source) ;

      // index for select, should be less than _selectorElements.size()
      SINT32 selectorIndex = 0 ;

      CHAR *pBuffer = (CHAR*)SDB_OSS_MALLOC ( SDB_PAGE_SIZE ) ;
      INT32 bufferSize = SDB_PAGE_SIZE ;
      if ( !pBuffer )
      {
         PD_LOG ( PDERROR, "Failed to allocate buffer for select" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      pBuffer[0] = '\0' ;

      // create a new object based on the source
      // "" is empty root, builder is BSONObjBuilder
      // es is our iterator, and selectorIndex is the current selector we are
      // going to apply
      // when this call returns SDB_OK, we should call builder.obj() to create
      // BSONObject from the builder.
      rc = _buildNewObj ( &pBuffer, bufferSize, 0, builder, es,
                          &selectorIndex ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to select target, rc: %d", rc ) ;
         goto error ;
      }
      // if we ask for output in json format, let's just do obj()
      if ( !_stringOutput )
      {
         // now target owns the builder buffer, since obj() will decouple() the
         // buffer from builder, and assign holder to the new BSONObj
         target=builder.obj();
      }
      else
      {
         // if we are asked to output as csv, let's build a temp target obj
         BSONObj tempObj = builder.obj () ;
         BOOLEAN result = FALSE ;
         INT32 stringLength = 0 ;

         // in the first round, let's allocate memory
         if ( 0 == _stringOutputBufferSize )
         {
            rc = mthDoubleBufferSize ( &_stringOutputBuffer,
                                        _stringOutputBufferSize ) ;
            PD_RC_CHECK ( rc, PDERROR,
                          "Failed to append string, rc = %d", rc ) ;
         }
         _stringOutputBuffer[FIRST_ELEMENT_STARTING_POS-6] = String ;
         _stringOutputBuffer[FIRST_ELEMENT_STARTING_POS-5] = '\0' ;
         while ( _stringOutputBufferSize < MAX_SELECTOR_BUFFER_THRESHOLD )
         {
            result = rawbson2csv ( tempObj.objdata(),
                  &_stringOutputBuffer[FIRST_ELEMENT_STARTING_POS],
                   _stringOutputBufferSize-FIRST_ELEMENT_STARTING_POS ) ;
            if ( result )
            {
               break ;
            }
            else
            {
               rc = mthDoubleBufferSize ( &_stringOutputBuffer,
                                          _stringOutputBufferSize ) ;
               PD_RC_CHECK ( rc, PDERROR,
                             "Failed to double buffer, rc = %d", rc ) ;
            }
         }
         if ( _stringOutputBufferSize >= MAX_SELECTOR_BUFFER_THRESHOLD )
         {
            PD_LOG ( PDERROR,
                     "string output buffer size is greater than threshold" ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         stringLength =
               ossStrlen ( &_stringOutputBuffer[FIRST_ELEMENT_STARTING_POS] ) ;
         // assign object length, 1 for 0 at the end, 1 for the eoo
         *(INT32*)_stringOutputBuffer = FIRST_ELEMENT_STARTING_POS + 2 +
                                        stringLength ;
         _stringOutputBuffer[ *(INT32*)_stringOutputBuffer -1 ] = EOO ;
         // assign string length, 1 for 0 at the end
         *(INT32*)(&_stringOutputBuffer[FIRST_ELEMENT_STARTING_POS-4]) =
               stringLength + 1 ;
         // it should not cause memory leak even if there's previous owned
         // buffer because _stringOutputBuffer is owned by context, and we don't
         // touch holder in BSONObj, so smart pointer should still holding the
         // original buffer it owns
         target.init ( _stringOutputBuffer ) ;
      }

   done :
      if ( pBuffer )
      {
         SDB_OSS_FREE ( pBuffer ) ;
      }
      PD_TRACE_EXITRC ( SDB__MTHSEL_SELECT, rc );
      return rc ;
   error :
      goto done ;
   }

}

