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

   Source File Name = rtnCoordQuery.hpp

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

#ifndef RTNCOORDQUERY_HPP__
#define RTNCOORDQUERY_HPP__

#include "rtnCoordOperator.hpp"
#include "rtnContext.hpp"
#include "../bson/bson.h"

namespace engine
{
   class rtnCoordQuery : virtual public rtnCoordOperator
   {
   public:
      virtual INT32 execute( CHAR *pReceiveBuffer, SINT32 packSize,
                           CHAR **ppResultBuffer, pmdEDUCB *cb,
                           MsgOpReply &replyHeader,
                           BSONObj** ppErrorObj );
      INT32 queryToDataNodeGroup( CHAR *pBuffer,
                                 CoordGroupList &groupLst,
                                 CoordGroupList &sendGroupLst,
                                 netMultiRouteAgent *pRouteAgent,
                                 pmdEDUCB *cb,
                                 rtnContextCoord *pContext,
                                 BOOLEAN sendToPrimary = FALSE,
                                 std::set<INT32> *ignoreRCList = NULL );
      virtual INT32 executeQuery( CHAR *pSrc ,
                                 const bson::BSONObj &boQuery ,
                                 const bson::BSONObj &boSelector,
                                 const bson::BSONObj &boOrderBy ,
                                 const CHAR * pCollectionName ,
                                 netMultiRouteAgent *pRouteAgent ,
                                 pmdEDUCB *cb ,
                                 rtnContextCoord *&pContext );
   private:
      INT32 getNodeGroups( const CoordCataInfoPtr &cataInfo,
                           const bson::BSONObj &queryObj,
                           const CoordGroupList &sendGroupLst,
                           CoordGroupList &groupLst );
      INT32 queryOnMainCL( CoordGroupSubCLMap &groupSubCLMap,
                           MsgOpQuery *pSrc,
                           pmdEDUCB *cb,
                           netMultiRouteAgent *pRouteAgent,
                           CoordGroupList &sendGroupList,
                           rtnContextCoord *pContext );

      INT32 _buildNewMsg( const CHAR *msg,
                          const bson::BSONObj &newSelector,
                          CHAR *&newMsg ) ;
   };

}

#endif

