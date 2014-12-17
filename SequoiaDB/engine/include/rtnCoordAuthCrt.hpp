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

   Source File Name = rtnCoordAuthCrt.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/12/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTNCOORDAUTHCRT_HPP_
#define RTNCOORDAUTHCRT_HPP_

#include "rtnCoordAuthBase.hpp"

namespace engine
{
   class rtnCoordAuthCrt : public rtnCoordAuthBase
   {
   public:
      virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                             CHAR **ppResultBuffer, pmdEDUCB *cb,
                             MsgOpReply &replyHeader,
                             BSONObj **ppErrorObj ) ;
   } ;
}

#endif

