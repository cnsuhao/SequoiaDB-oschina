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

   Source File Name = rtnCoordImageCommands.hpp

   Descriptive Name = Runtime Coord Common

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          02/11/15    XJH Init
   Last Changed =

*******************************************************************************/

#ifndef RTNCOORD_IMAGE_COMMANDS_HPP__
#define RTNCOORD_IMAGE_COMMANDS_HPP__

#include "rtnCoordCommands.hpp"

namespace engine
{

   /*
      rtnCoordAttachImage define
   */
   class rtnCoordAttachImage : public rtnCoordCommand
   {
      public:
         virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                CHAR **ppResultBuffer,
                                pmdEDUCB *cb, MsgOpReply &replyHeader,
                                BSONObj **ppErrorObj ) ;
   } ;

}

#endif // RTNCOORD_IMAGE_COMMANDS_HPP__
