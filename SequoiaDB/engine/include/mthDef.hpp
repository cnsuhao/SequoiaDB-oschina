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

   Source File Name = mthDef.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef MTH_DEF_HPP_
#define MTH_DEF_HPP_

namespace engine
{
   #define MTH_S_PREFIX "$"
   #define MTH_S_INCLUDE        MTH_S_PREFIX"include"
   #define MTH_S_DEFAULT        MTH_S_PREFIX"default"
   #define MTH_S_SLICE          MTH_S_PREFIX"slice"
   #define MTH_S_ELEMMATCH      MTH_S_PREFIX"elemMatch"
   #define MTH_S_ELEMMATCHONE   MTH_S_PREFIX"elemMatchOne"
   #define MTH_S_ALIAS          MTH_S_PREFIX"alias"

   typedef UINT32 MTH_S_ATTRIBUTE ;

   /* attribute bit
   | 1bit                 | 1bit                 | 1bit                        |1bit                           |
   | 1:invalid, 0:invalid | 1:include, 0:exclude | 1:default 0:non-default     |1:projection, 0:non-projection |
   */

   #define MTH_S_ATTR_VALID_BIT          1
   #define MTH_S_ATTR_INCLUDE_BIT        2
   #define MTH_S_ATTR_DEFAULT_BIT        4
   #define MTH_S_ATTR_PROJECTION_BIT     8

   #define MTH_S_ATTR_NONE               0
   #define MTH_S_ATTR_EXCLUDE            ( MTH_S_ATTR_VALID_BIT )
   #define MTH_S_ATTR_INCLUDE            ( MTH_S_ATTR_VALID_BIT | MTH_S_ATTR_INCLUDE_BIT )
   #define MTH_S_ATTR_DEFAULT            ( MTH_S_ATTR_INCLUDE | MTH_S_ATTR_DEFAULT_BIT )
   #define MTH_S_ATTR_PROJECTION         ( MTH_S_ATTR_VALID_BIT | MTH_S_ATTR_PROJECTION_BIT )

   #define MTH_ATTR_IS_VALID( attribute ) \
           OSS_BIT_TEST( attribute, MTH_S_ATTR_VALID_BIT )

   #define MTH_ATTR_IS_INCLUDE( attribute ) \
           ( MTH_ATTR_IS_VALID(attribute) && OSS_BIT_TEST( attribute, MTH_S_ATTR_INCLUDE_BIT ))

   #define MTH_ATTR_IS_DEFAULT( attribute ) \
           ( MTH_ATTR_IS_VALID(attribute) && OSS_BIT_TEST( attribute, MTH_S_ATTR_DEFAULT_BIT ))

   #define MTH_ATTR_IS_PROJECTION( attribute ) \
           ( MTH_ATTR_IS_INCLUDE(attribute) && OSS_BIT_TEST(attribute, MTH_S_ATTR_PROJECTION_BIT))
}

#endif

