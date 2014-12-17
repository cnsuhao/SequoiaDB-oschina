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

   Source File Name = aggrMatcher.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/04/2013  JHL  Initial Draft

   Last Changed =

******************************************************************************/
#include "aggrMatcher.hpp"
#include "qgmOptiMthMatchSelect.hpp"
#include "aggrDef.hpp"

using namespace bson;
namespace engine
{
   INT32 aggrMatchParser::buildNode( const bson::BSONElement &elem,
                                       const CHAR *pCLName,
                                       qgmOptiTreeNode *&pNode,
                                       _qgmPtrTable *pTable,
                                       _qgmParamTable *pParamTable )
   {
      INT32 rc = SDB_OK;
      qgmOptiMthMatchSelect *pSelect
                     = SDB_OSS_NEW qgmOptiMthMatchSelect( pTable,
                                                         pParamTable );
      PD_CHECK( pSelect != NULL, SDB_OOM, error, PDERROR,
               "malloc failed!" );
      try
      {
         BSONObj obj;
         PD_CHECK( elem.type() == Object, SDB_INVALIDARG, error, PDERROR,
               "failed to parse the parameter:%s(type=%d, expectType=%d)",
               elem.fieldName(), elem.type(), Object );
         obj = elem.embeddedObject();
         PD_CHECK( !obj.isEmpty(), SDB_INVALIDARG, error, PDERROR,
                  "Parameter-object can't be empty!" );

         rc = pSelect->fromBson( obj );
         PD_RC_CHECK( rc, PDERROR, "failed to build the node(rc=%d)", rc );
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                  "failed to parse the Parameter-object, received unexpected error:%s",
                  e.what() );
      }
      {
         qgmOpField selectAll;
         selectAll.type = SQL_GRAMMAR::WILDCARD;
         pSelect->_selector.push_back( selectAll );
         pSelect->_limit = -1;
         pSelect->_skip = 0;
         pSelect->_type = QGM_OPTI_TYPE_MTHMCHSEL;
         pSelect->_hasFunc = FALSE;
         rc = pTable->getOwnField( AGGR_CL_DEFAULT_ALIAS, pSelect->_alias );
         PD_RC_CHECK( rc, PDERROR, "failed to get the field(%s)", AGGR_CL_DEFAULT_ALIAS );
         if ( pCLName != NULL )
         {
            qgmField clValAttr;
            qgmField clValRelegation;
            rc = pTable->getOwnField( pCLName, clValAttr );
            PD_RC_CHECK( rc, PDERROR, "failed to get the field(%s)", pCLName );
            rc = pTable->getOwnField( AGGR_CL_DEFAULT_ALIAS, pSelect->_collection.alias );
            PD_RC_CHECK( rc, PDERROR, "failed to get the field(%s)", AGGR_CL_DEFAULT_ALIAS );
            pSelect->_collection.value = qgmDbAttr( clValRelegation, clValAttr );
            pSelect->_collection.type = SQL_GRAMMAR::DBATTR;
         }
      }
      pNode = pSelect;
   done:
      return rc;
   error:
      SAFE_OSS_DELETE( pSelect );
      goto done;
   }
}
