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

   Source File Name = qgmUtil.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

******************************************************************************/

#ifndef QGMUTIL_HPP_
#define QGMUTIL_HPP_

#include "core.hpp"
#include "qgmOptiTree.hpp"
#include "qgmPlan.hpp"
#include "qgmOptiAggregation.hpp"

namespace engine
{

   BOOLEAN qgmUtilFirstDot( const CHAR *str, UINT32 len, UINT32 &num ) ;

   BOOLEAN qgmUtilSame( const CHAR *src, UINT32 srcLen,
                        const CHAR *dst, UINT32 dstLen ) ;

   INT32 qgmFindFieldFromFunc( const CHAR *str, UINT32 size,
                               _qgmField &func,
                               vector<qgmOpField> &params,
                               _qgmPtrTable *table,
                               BOOLEAN needRele ) ;

   BOOLEAN isFromOne( const qgmOpField &left, const qgmOPFieldVec &right,
                      BOOLEAN useAlias = TRUE, UINT32 *pPos = NULL ) ;
   BOOLEAN isSameFrom( const qgmOPFieldVec &left, const qgmOPFieldVec &right ) ;

   BOOLEAN isFrom( const qgmDbAttr &left, const qgmOpField &right,
                   BOOLEAN useAlias = TRUE ) ;
   BOOLEAN isFromOne( const qgmDbAttr &left, const qgmOPFieldVec &right,
                      BOOLEAN useAlias = TRUE, UINT32 *pPos = NULL ) ;

   BSONObj qgmMerge( const BSONObj &left, const BSONObj &right ) ;

   string qgmPlanType( QGM_PLAN_TYPE type ) ;


   BOOLEAN isWildCard( const qgmOPFieldVec &fields ) ;
   void  replaceFieldRele( qgmOPFieldVec &fields, const qgmField &newRele ) ;
   void  replaceAttrRele( qgmDbAttrPtrVec &attrs,  const qgmField &newRele ) ;
   void  replaceAttrRele( qgmDbAttrVec &attrs, const qgmField &newRele ) ;
   void  replaceAggrRele( qgmAggrSelectorVec &aggrs, const qgmField &newRele ) ;

   void  clearFieldAlias( qgmOPFieldVec &fields ) ;

   INT32 downFieldsByFieldAlias( qgmOPFieldVec &fields,
                                 const qgmOPFieldPtrVec & fieldAlias,
                                 BOOLEAN needCopyAlias ) ;
   INT32 downAttrsByFieldAlias( qgmDbAttrPtrVec &attrs,
                                const qgmOPFieldPtrVec & fieldAlias ) ;
   INT32 downAttrsByFieldAlias( qgmDbAttrVec &attrs,
                                const qgmOPFieldPtrVec & fieldAlias ) ;
   INT32 downAAttrByFieldAlias( qgmDbAttr &attr,
                                const qgmOPFieldPtrVec & fieldAlias ) ;
   INT32 downAggrsByFieldAlias( qgmAggrSelectorVec &aggrs,
                                const qgmOPFieldPtrVec & fieldAlias ) ;

   INT32 upFieldsByFieldAlias( qgmOPFieldVec &fields,
                               const qgmOPFieldPtrVec & fieldAlias,
                               BOOLEAN needClearAlias ) ;
   INT32 upAttrsByFieldAlias( qgmDbAttrPtrVec &attrs,
                              const qgmOPFieldPtrVec & fieldAlias ) ;
   INT32 upAttrsByFieldAlias( qgmDbAttrVec &attrs,
                              const qgmOPFieldPtrVec & fieldAlias ) ;
   INT32 upAAttrByFieldAlias( qgmDbAttr &attr,
                              const qgmOPFieldPtrVec & fieldAlias ) ;
   INT32 upAggrsByFieldAlias( qgmAggrSelectorVec &aggrs,
                              const qgmOPFieldPtrVec & fieldAlias ) ;

}

#endif

