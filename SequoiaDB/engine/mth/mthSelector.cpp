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

   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSEL__APPNEW, "_mthSelector::_appendNew" )
   template<class Builder>
   INT32 _mthSelector::_appendNew ( const CHAR *pFieldName, Builder& b,
                                    SINT32 *selectorIndex )
   {
      PD_TRACE_ENTRY ( SDB__MTHSEL__APPNEW );
      SelectorElement me = _selectorElements[(*selectorIndex)] ;
      b.appendAs ( me._toSelect, pFieldName ) ;

      _incSelectorIndex( selectorIndex ) ;

      PD_TRACE_EXIT ( SDB__MTHSEL__APPNEW );
      return SDB_OK ;
   }

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


      const CHAR *fieldName = me._toSelect.fieldName() ;
      const CHAR *temp = fieldName + rootLen ;
      const CHAR *dot = ossStrchr ( temp,'.') ;

      if ( dot )
      {
         *(CHAR*)dot = 0 ;
      }
      const CHAR *pCurField = *ppRoot + newRootLen ;
      rc = mthAppendString( ppRoot, rootBufLen, newRootLen, temp, -1,
                            &newRootLen ) ;
      if ( dot )
      {
         *(CHAR*)dot = '.' ;
      }
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to append string, rc: %d", rc ) ;
         goto error ;
      }

      if ( dot )
      {
         BSONObjBuilder bb ( b.subobjStart( pCurField ) ) ;
         const BSONObj obj ;
         BSONObjIteratorSorted es(obj);

         rc = mthAppendString ( ppRoot, rootBufLen, newRootLen, ".", 1,
                                &newRootLen ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to append string, rc: %d", rc ) ;
            goto error ;
         }

         rc = _buildNewObj ( ppRoot, rootBufLen, newRootLen, bb, es,
                             selectorIndex ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to build new object for %s, rc: %d",
                     me._toSelect.toString().c_str(), rc ) ;
            goto error ;
         }
      }
      else
      {
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
      BSONElement e = es.next() ;
      BSONElement prevE ;
      INT32 newRootLen = rootLen ;

      while( !e.eoo() && (*selectorIndex) < (SINT32)_selectorElements.size() )
      {
         if ( _dupFieldName( prevE, e ) )
         {
            prevE = e ;
            e = es.next() ;
            continue ;
         }
         prevE = e ;

         (*ppRoot)[rootLen] = '\0' ;
         newRootLen = rootLen ;

         rc = mthAppendString ( ppRoot, rootBufLen, newRootLen,
                                e.fieldName(), -1, &newRootLen ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to append string, rc: %d", rc ) ;

         FieldCompareResult cmp = compareDottedFieldNames (
               _selectorElements[(*selectorIndex)]._toSelect.fieldName(),
               *ppRoot ) ;





         switch ( cmp )
         {
         case LEFT_SUBFIELD:
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

            rc = mthAppendString ( ppRoot, rootBufLen, newRootLen, ".", 1,
                                   &newRootLen ) ;
            PD_RC_CHECK ( rc, PDERROR, "Failed to append string, rc: %d", rc ) ;

            if ( e.type() == Object )
            {
               BSONObjBuilder bb(b.subobjStart(e.fieldName()));
               BSONObjIteratorSorted bis(e.Obj());

               rc = _buildNewObj ( ppRoot, rootBufLen, newRootLen, bb, bis,
                                   selectorIndex ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to build object: %s, rc: %d",
                           e.toString().c_str(), rc ) ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
               bb.done() ;
            }
            else
            {
               BSONArrayBuilder ba(b.subarrayStart(e.fieldName()));
               BSONObjIteratorSorted bis(e.embeddedObject());
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
            e = es.next() ;
         break ;

         case LEFT_BEFORE:


            (*ppRoot)[rootLen] = '\0' ;
            newRootLen = rootLen ;
            rc = _appendNewFromMods ( ppRoot, rootBufLen, newRootLen,
                                      b, selectorIndex) ;
            PD_RC_CHECK ( rc, PDERROR, "Failed to append for %s, rc: %d",
                          _selectorElements[(*selectorIndex)
                          ]._toSelect.toString().c_str(), rc ) ;

            break ;

         case SAME:
            b.append( e ) ;
            _incSelectorIndex( selectorIndex ) ;
            break ;

         case RIGHT_BEFORE:

            e = es.next() ;
            break ;

         case RIGHT_SUBFIELD:
         default :
            PD_RC_CHECK ( SDB_SYS, PDERROR, "Reaching unexpected codepath, "
                          "selector: %s, root: %s, rc: %d",
                          _selectorElements[(*selectorIndex)
                          ]._toSelect.toString().c_str(), *ppRoot, rc ) ;
         }
      }

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

   #define FIRST_ELEMENT_STARTING_POS     10
   #define MAX_SELECTOR_BUFFER_THRESHOLD  67108864 // 64MB


   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSEL_SELECT, "_mthSelector::select" )
   INT32 _mthSelector::select ( const BSONObj &source, BSONObj &target )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHSEL_SELECT );
      SDB_ASSERT(_initialized, "The selector has not been initialized, please "
                               "call 'loadPattern' before using it" ) ;
      SDB_ASSERT(target.isEmpty(), "target should be empty") ;

      BSONObjBuilder builder ( (INT32)(source.objsize()*1.1) );
      BSONObjIteratorSorted es(source) ;

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

      rc = _buildNewObj ( &pBuffer, bufferSize, 0, builder, es,
                          &selectorIndex ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to select target, rc: %d", rc ) ;
         goto error ;
      }
      if ( !_stringOutput )
      {
         target=builder.obj();
      }
      else
      {
         BSONObj tempObj = builder.obj () ;
         BOOLEAN result = FALSE ;
         INT32 stringLength = 0 ;

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
         *(INT32*)_stringOutputBuffer = FIRST_ELEMENT_STARTING_POS + 2 +
                                        stringLength ;
         _stringOutputBuffer[ *(INT32*)_stringOutputBuffer -1 ] = EOO ;
         *(INT32*)(&_stringOutputBuffer[FIRST_ELEMENT_STARTING_POS-4]) =
               stringLength + 1 ;
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

