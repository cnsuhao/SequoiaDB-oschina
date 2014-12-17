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

   Source File Name = catSplit.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          19/07/2013  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef CAT_SPLIT_HPP__
#define CAT_SPLIT_HPP__

#include "core.hpp"
#include "pd.hpp"
#include "oss.hpp"
#include "ossErr.h"
#include "../bson/bson.h"
#include "catDef.hpp"
#include "pmd.hpp"
#include "clsTask.hpp"

using namespace bson ;

namespace engine
{

   INT32 catSplitPrepare( const BSONObj &splitInfo, const CHAR *clFullName,
                          clsCatalogSet *cataSet, INT32 &groupID,
                          pmdEDUCB *cb ) ;

   INT32 catSplitReady( const BSONObj &splitInfo, const CHAR *clFullName,
                        clsCatalogSet *cataSet, INT32 &groupID,
                        clsTaskMgr &taskMgr, pmdEDUCB *cb, INT16 w,
                        UINT64 *pTaskID = NULL ) ;

   INT32 catSplitStart( const BSONObj &splitInfo, pmdEDUCB *cb, INT16 w ) ;

   INT32 catSplitChgMeta( const BSONObj &splitInfo, const CHAR *clFullName,
                          clsCatalogSet *cataSet, pmdEDUCB *cb, INT16 w ) ;

   INT32 catSplitCleanup( const BSONObj &splitInfo, pmdEDUCB *cb, INT16 w ) ;

   INT32 catSplitFinish( const BSONObj &splitInfo, pmdEDUCB *cb, INT16 w ) ;

   INT32 catSplitCancel( const BSONObj &splitInfo, pmdEDUCB *cb,
                         INT32 &groupID, INT16 w ) ;

   INT32 catSplitCheckConflict( BSONObj &match, clsSplitTask &splitTask,
                                BOOLEAN &conflict, pmdEDUCB *cb ) ;

}


#endif //CAT_SPLIT_HPP__

