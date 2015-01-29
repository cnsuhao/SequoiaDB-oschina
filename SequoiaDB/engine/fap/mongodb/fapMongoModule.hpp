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

   Source File Name = aggrGroup.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/27/2015  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef _SDB_FAP_MONGO_MODULE_HPP_
#define _SDB_FAP_MONGO_MODULE_HPP_

#include "../fapModuleWrapper.hpp"
#include "pmdAccessProtocolBase.hpp"

#ifdef WIN32
#define MONGO_MODULE_NAME "fapmongo.dll"
#else
#define MONGO_MODULE_NAME "libfapmongo.so"
#endif // WIN32


#define MONGO_MODULE_PATH "./bin/fap/"

class _fapMongoModule : public engine::fapModuleWrapper
{
public:
   _fapMongoModule() ;
   virtual ~_fapMongoModule() ;

   virtual INT32 init() ;
   virtual INT32 active() ;
   virtual INT32 fini() ;
};

typedef _fapMongoModule fapMongoModule ;

#endif
