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

   Source File Name = rtnDataLobStream.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          05/08/2014  YW Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_DATALOBSTREAM_HPP_
#define RTN_DATALOBSTREAM_HPP_

#include "rtnLobStream.hpp"

namespace engine
{
   class _rtnDataLobStream : public _rtnLobStream
   {
   public:
      _rtnDataLobStream() ;
      virtual ~_rtnDataLobStream() ;
   } ;
   typedef class _rtnDataLobStream rtnDataLobStream ;
}

#endif

