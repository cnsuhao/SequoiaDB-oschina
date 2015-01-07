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

   Descriptive Name = Method Selector Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Method component. This file contains structure for selecting
   operation, which is generating a new BSON record from a given record, based
   on the given selecting rule.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef MTHSELECTOR_HPP_
#define MTHSELECTOR_HPP_

#include "core.hpp"
#include "oss.hpp"
#include <vector>
#include "ossUtil.hpp"
#include "../bson/bson.h"
#include "../bson/bsonobj.h"
using namespace bson ;

namespace engine
{
   /*
      _SelectorElement define
   */
   class _SelectorElement : public SDBObject
   {
   public :
      BSONElement _toSelect ; // the element to select
      _SelectorElement ( const BSONElement &e )
      {
         _toSelect = e ;
      }
   } ;
   typedef _SelectorElement SelectorElement ;

   /*
      _mthSelector define
   */
   class _mthSelector : public SDBObject
   {
   private :
      BSONObj _selectorPattern ;
      BOOLEAN _initialized ;
      BOOLEAN _stringOutput ;
      CHAR   *_stringOutputBuffer ;
      INT32   _stringOutputBufferSize ;
      BOOLEAN _ignoreTypeError ;
      vector<SelectorElement> _selectorElements ;

      INT32 _addSelector ( const BSONElement &ele ) ;
      BOOLEAN _dupFieldName ( const BSONElement &l,
                              const BSONElement &r ) ;

      OSS_INLINE void _incSelectorIndex( INT32 *selectorIndex ) ;

      // when requested update want to change something that not exist in
      // original
      // object, we need to append the original object in those cases
      template<class Builder>
      INT32 _appendNew ( const CHAR *pFieldName, Builder& b,
                         SINT32 *selectorIndex ) ;

      // Builder could be BSONObjBuilder or BSONArrayBuilder
      // _appendNewFromMods appends the current builder with the new field
      // root represent the current fieldName, me is the current selector elemen
      // b is the builder, onedownseen represent the all subobjects have been
      // processed in the current object, and selectorIndex is the pointer for
      // current selector
      template<class Builder>
      INT32 _appendNewFromMods ( CHAR **ppRoot,
                                 INT32 &rootBufLen,
                                 INT32 rootLen,
                                 Builder &b,
                                 SINT32 *selectorIndex ) ;
      // Builder could be BSONObjBuilder or BSONArrayBuilder
      // This function is recursively called to build new object
      // The prerequisit is that _selectorElement is sorted, which supposed to
      // happen at end of loadPattern
      template<class Builder>
      INT32 _buildNewObj ( CHAR **ppRoot,
                           INT32 &rootBufLen,
                           INT32 rootLen,
                           Builder &b,
                           BSONObjIteratorSorted &es,
                           SINT32 *selectorIndex ) ;
   public :
      _mthSelector ()
      {
         _stringOutput = FALSE ;
         _initialized = FALSE ;
         _stringOutputBuffer = NULL ;
         _stringOutputBufferSize = 0 ;
         _ignoreTypeError = TRUE ;
      }
      ~_mthSelector()
      {
         _selectorElements.clear() ;
         if ( _stringOutputBuffer )
         {
            SDB_OSS_FREE ( _stringOutputBuffer ) ;
         }
      }

      INT32 loadPattern ( const BSONObj &selectorPattern ) ;

      INT32 select ( const BSONObj &source, BSONObj &target ) ;

      OSS_INLINE void setStringOutput ( BOOLEAN strOut )
      {
         _stringOutput = strOut ;
      }
      OSS_INLINE BOOLEAN getStringOutput () const
      {
         return _stringOutput ;
      }
      OSS_INLINE BOOLEAN isInitialized () { return _initialized ; }

   } ;
   typedef _mthSelector mthSelector ;

   /*
      OSS_INLINE function
   */
   OSS_INLINE void _mthSelector::_incSelectorIndex( INT32 *selectorIndex )
   {
      ++(*selectorIndex) ;
   }

}

#endif //MTHSELECTOR_HPP_
