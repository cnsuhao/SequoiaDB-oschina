/******************************************************************************

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

   Source File Name = mthSliceIterator.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "mthSliceIterator.hpp"

using namespace bson ;

namespace engine
{
   _mthSliceIterator::_mthSliceIterator( const bson::BSONObj &obj,
                                         INT32 begin,
                                         INT32 limit )
   :_obj( obj ),
    _where( 0 ),
    _limit( limit ),
    _itr( _obj )
   {
      INT32 total = obj.nFields() ;
      _where = begin < 0 ? begin + total : begin ;
      if ( _where < 0 )
      {
         _where = 0 ;
      }

      while ( 0 != _where )
      {
         if ( _itr.more() )
         {
            _itr.next() ;
            --_where ;
         }
         else
         {
            _limit = 0 ;
            break ;
         }
      }
   }

   _mthSliceIterator::~_mthSliceIterator()
   {

   }

   BOOLEAN _mthSliceIterator::more()
   {
      return _limit != 0 &&
             _itr.more() ;
   }

   bson::BSONElement _mthSliceIterator::next()
   {
      if ( more() )
      {
         if ( 0 < _limit )
         {
            --_limit ;
         }
         return _itr.next() ;
      }
      else
      {
         return BSONElement() ;
      }
   }
}

