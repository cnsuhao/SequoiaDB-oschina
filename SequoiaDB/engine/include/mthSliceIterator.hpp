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

   Source File Name = mthSliceIterator.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef MTH_SLICEITERATOR_HPP_
#define MTH_SLICEITERATOR_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "../bson/bson.hpp"

namespace engine
{
   class _mthSliceIterator : public SDBObject
   {
   public:
      _mthSliceIterator( const bson::BSONObj &obj,
                         INT32 begin = 0,
                         INT32 limit = -1 ) ;
      ~_mthSliceIterator() ;

   public:
      BOOLEAN more() ;
      bson::BSONElement next() ;

   private:
      bson::BSONObj _obj ;
      INT32 _where ;
      INT32 _limit ;
      bson::BSONObjIterator _itr ;     
   } ;
   typedef class _mthSliceIterator mthSliceIterator ;
}

#endif

