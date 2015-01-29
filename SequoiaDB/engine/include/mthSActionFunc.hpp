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

   Source File Name = mthSActionFunc.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef MTH_SACTIONFUNC_HPP_
#define MTH_SACTIONFUNC_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "../bson/bson.hpp"

namespace engine
{
   class _mthSAction ;
   typedef INT32 (*MTH_SACTION_BUILD)( const CHAR *,
                                       const bson::BSONElement &,
                                       _mthSAction *,
                                       bson::BSONObjBuilder & ) ;

   typedef INT32 (*MTH_SACTION_GET)( const CHAR *,
                                     const bson::BSONElement &,
                                     _mthSAction *,
                                     bson::BSONElement & ) ;

   INT32 mthIncludeBuild( const CHAR *,
                          const bson::BSONElement &,
                          _mthSAction *,
                          bson::BSONObjBuilder & ) ;

   INT32 mthIncludeGet( const CHAR *,
                        const bson::BSONElement &,
                        _mthSAction *,
                        bson::BSONElement & ) ;

   INT32 mthDefaultBuild( const CHAR *,
                          const bson::BSONElement &,
                          _mthSAction *,
                          bson::BSONObjBuilder & ) ;

   INT32 mthDefaultGet( const CHAR *,
                        const bson::BSONElement &,
                        _mthSAction *,
                        bson::BSONElement & ) ;

   INT32 mthSliceBuild( const CHAR *,
                        const bson::BSONElement &,
                        _mthSAction *,
                        bson::BSONObjBuilder & ) ;

   INT32 mthSliceGet( const CHAR *,
                      const bson::BSONElement &,
                      _mthSAction *,
                      bson::BSONElement & ) ;

   INT32 mthElemMatchBuild( const CHAR *,
                            const bson::BSONElement &,
                            _mthSAction *,
                            bson::BSONObjBuilder & ) ;

   INT32 mthElemMatchGet( const CHAR *,
                          const bson::BSONElement &,
                          _mthSAction *,
                          bson::BSONElement & ) ;

   INT32 mthElemMatchOneBuild( const CHAR *,
                               const bson::BSONElement &,
                               _mthSAction *,
                               bson::BSONObjBuilder & ) ;

   INT32 mthElemMatchOneGet( const CHAR *,
                             const bson::BSONElement &,
                             _mthSAction *,
                             bson::BSONElement & ) ;

}

#endif

