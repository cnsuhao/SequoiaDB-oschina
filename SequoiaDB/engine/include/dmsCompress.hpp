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

   Source File Name = dmsCompress.hpp

   Descriptive Name =

   When/how to use: str util

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          20/06/2014  XJH Initial Draft

   Last Changed =

******************************************************************************/

#ifndef DMSCOMPRESS_HPP__
#define DMSCOMPRESS_HPP__

#include "core.hpp"
#include "../bson/bson.h"

using namespace bson ;

namespace engine
{

   class _pmdEDUCB ;

   /*
      ppData: output data pointer, not need release
   */
   INT32 dmsCompress ( _pmdEDUCB *cb, const CHAR *pInputData,
                       INT32 inputSize, const CHAR **ppData,
                       INT32 *pDataSize ) ;

   INT32 dmsCompress ( _pmdEDUCB *cb, const BSONObj &obj,
                       const CHAR* pOIDPtr, INT32 oidLen,
                       const CHAR **ppData, INT32 *pDataSize ) ;

   /*
      ppData: output data pointer, not need release
   */
   INT32 dmsUncompress ( _pmdEDUCB *cb, const CHAR *pInputData,
                         INT32 inputSize, const CHAR **ppData,
                         INT32 *pDataSize ) ;

}

#endif // DMSCOMPRESS_HPP__

