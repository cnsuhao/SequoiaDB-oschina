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

   Source File Name = mthCommon.hpp

   Descriptive Name = Method Common Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Method component.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/12/2013  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef MTHCOMMON_HPP__
#define MTHCOMMON_HPP__

#include "core.hpp"
#include <vector>

namespace engine
{

   INT32 mthAppendString ( CHAR **ppStr, INT32 &bufLen,
                           INT32 strLen, const CHAR *newStr,
                           INT32 newStrLen, INT32 *pMergedLen = NULL ) ;

   INT32 mthDoubleBufferSize ( CHAR **ppStr, INT32 &bufLen ) ;


   INT32 mthCheckFieldName( const CHAR *pField, INT32 &dollarNum ) ;

   BOOLEAN mthCheckUnknowDollar( const CHAR *pField,
                                 std::vector<INT64> *dollarList ) ;

   INT32 mthConvertSubElemToNumeric( const CHAR *desc,
                                     INT32 &number ) ;

}

#endif //MTHCOMMON_HPP__
