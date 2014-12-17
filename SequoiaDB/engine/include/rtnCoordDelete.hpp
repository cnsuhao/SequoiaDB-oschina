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

   Source File Name = rtnCoordDelete.hpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/28/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTNCOORDDELETE_HPP__
#define RTNCOORDDELETE_HPP__

#include "rtnCoordOperator.hpp"
#include "../bson/bson.h"

namespace engine
{
   class rtnCoordDelete : public rtnCoordTransOperator
   {
   public:
      virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                           CHAR **ppResultBuffer, pmdEDUCB *cb,
                           MsgOpReply &replyHeader, BSONObj **ppErrorObj );

   protected:
      
      INT32 deleteNormalCL( CoordCataInfoPtr cataInfo,
                           bson::BSONObj &boDelete,
                           MsgOpDelete *pDelMsg,
                           netMultiRouteAgent *pRouteAgent,
                           pmdEDUCB *cb,
                           CoordGroupList &sendGroupLst);

   private:
      INT32 getNodeGroups( const CoordCataInfoPtr &cataInfo,
                           bson::BSONObj &deleteObj,
                           CoordGroupList &sendGroupLst,
                           CoordGroupList &groupLst );
      INT32 deleteToDataNodeGroup( CHAR *pBuffer,
                                 CoordGroupList &groupLst,
                                 CoordGroupList &sendGroupLst,
                                 netMultiRouteAgent *pRouteAgent,
                                 pmdEDUCB *cb );
      INT32 buildOpMsg( const CoordCataInfoPtr &cataInfo,
                              const CoordSubCLlist &subCLList,
                              CHAR *pSrcMsg, CHAR *&pDstMsg,
                              INT32 &bufferSize );
   };
}

#endif

