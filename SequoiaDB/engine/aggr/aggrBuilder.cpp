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

   Source File Name = aggrBuilder.cpp

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

*******************************************************************************/
#include "aggrBuilder.hpp"
#include "qgmPlanContainer.hpp"
#include "aggrDef.hpp"
#include "optQgmOptimizer.hpp"
#include "qgmBuilder.hpp"
#include "rtnCB.hpp"
#include "pmd.hpp"
#include "aggrGroup.hpp"
#include "aggrMatcher.hpp"
#include "aggrSkip.hpp"
#include "aggrLimit.hpp"
#include "aggrSort.hpp"
#include "aggrProject.hpp"
#include "aggrTrace.h"


using namespace bson;

namespace engine
{
   AGGR_PARSER_BEGIN
   AGGR_PARSER_ADD( AGGR_GROUP_PARSER_NAME, aggrGroupParser )
   AGGR_PARSER_ADD( AGGR_MATCH_PARSER_NAME, aggrMatchParser )
   AGGR_PARSER_ADD( AGGR_SKIP_PARSER_NAME, aggrSkipParser )
   AGGR_PARSER_ADD( AGGR_LIMIT_PARSER_NAME, aggrLimitParser )
   AGGR_PARSER_ADD( AGGR_SORT_PARSER_NAME, aggrSortParser )
   AGGR_PARSER_ADD( AGGR_PROJECT_PARSER_NAME, aggrProjectParser )
   AGGR_PARSER_END

   // PD_TRACE_DECLARE_FUNCTION ( SDB_AGGRBUILDER_AGGRBUILDER, "aggrBuilder::aggrBuilder" )
   aggrBuilder::aggrBuilder()
   {
      PD_TRACE_ENTRY ( SDB_AGGRBUILDER_AGGRBUILDER ) ;
      addParser();
      PD_TRACE_EXIT ( SDB_AGGRBUILDER_AGGRBUILDER ) ;
   }

   aggrBuilder::~aggrBuilder()
   {
      AGGR_PARSER_MAP::iterator iterMap = _parserMap.begin();
      while( iterMap != _parserMap.end() )
      {
         SDB_OSS_DEL iterMap->second;
         ++iterMap;
      }
   }

   INT32 aggrBuilder::init ()
   {
      return SDB_OK ;
   }

   INT32 aggrBuilder::active ()
   {
      return SDB_OK ;
   }

   INT32 aggrBuilder::deactive ()
   {
      return SDB_OK ;
   }

   INT32 aggrBuilder::fini ()
   {
      return SDB_OK ;
   }

   INT32 aggrBuilder::build( BSONObj &objs, INT32 objNum,
                             const CHAR *pCLName, _pmdEDUCB *cb,
                             SINT64 &contextID )
   {
      INT32 rc = SDB_OK;
      BOOLEAN hasNew = FALSE;
      qgmOptiTreeNode *pOptiTree = NULL;
      qgmOptiTreeNode *pExtend = NULL;
      qgmPlanContainer *pContainer = NULL;

      PD_CHECK( objNum > 0, SDB_INVALIDARG, error, PDERROR,
               "input error, no object input!" );

      pContainer = SDB_OSS_NEW _qgmPlanContainer() ;
      PD_CHECK( pContainer != NULL, SDB_OOM, error, PDERROR,
               "malloc failed" );
      hasNew = TRUE;

      rc = buildTree( objs, objNum, pOptiTree, pContainer->ptrTable(),
                     pContainer->paramTable(), pCLName );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to build the opti tree(rc=%d)",
                  rc );

      rc = pOptiTree->extend( pExtend );
      PD_RC_CHECK( rc, PDERROR,
                  "extend failed(rc=%d)", rc );

      {
         qgmOptTree tree( pExtend );
         optQgmOptimizer optimizer;
         rc = optimizer.adjust( tree );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to adjust the tree(rc=%d)",
                     rc );
         pExtend = tree.getRoot();
      }

      {
         qgmBuilder builder( pContainer->ptrTable(),
                           pContainer->paramTable() );
         rc = builder.build( pExtend, pContainer->plan() );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to build the physical plan(rc=%d)",
                     rc );
         SDB_ASSERT( QGM_PLAN_TYPE_MAX != pContainer->type(),
                     "invalid container type!" );
      }

      rc = pContainer->execute( cb );
      PD_RC_CHECK( rc, PDERROR, "execute failed(rc=%d)", rc );

      SDB_ASSERT( QGM_PLAN_TYPE_RETURN == pContainer->type(),
                  "invalid container type!" );
      rc = createContext( pContainer, cb, contextID );
      PD_RC_CHECK( rc, PDERROR, "failed to create context(rc=%d)", rc );
   done:
      if ( NULL != pExtend )
      {
         SDB_OSS_DEL pExtend;
      }
      else
      {
         SAFE_OSS_DELETE( pOptiTree );
      }
      if ( hasNew
         && ( rc != SDB_OK || QGM_PLAN_TYPE_RETURN != pContainer->type()) )
      {
         SDB_OSS_DEL pContainer;
      }
      return rc;
   error:
      goto done;
   }

   INT32 aggrBuilder::buildTree( BSONObj &objs, INT32 objNum,
                                 _qgmOptiTreeNode *&root,
                                 _qgmPtrTable * pPtrTable,
                                 _qgmParamTable *pParamTable,
                                 const CHAR *pCollectionName )
   {
      INT32 rc = SDB_OK;
      INT32 i = 0;
      const CHAR *pCLNameTmp = pCollectionName;
      ossValuePtr pDataPos = 0;
      PD_CHECK( !objs.isEmpty(), SDB_INVALIDARG, error, PDERROR,
               "Parameter-object can't be empty!" );
      pDataPos = (ossValuePtr)objs.objdata();
      while ( i < objNum )
      {
         try
         {
            BSONObj paraObj ( (const CHAR*)pDataPos );
            BSONElement bePara = paraObj.firstElement();
            const CHAR *pAggrOp = bePara.fieldName();
            AGGR_PARSER_MAP::iterator iterMap
                                       = _parserMap.find( pAggrOp );
            PD_CHECK( iterMap != _parserMap.end(), SDB_INVALIDARG,
                     error, PDERROR,
                     "unknow aggregation-operator name(%s)",
                     pAggrOp );
            rc = iterMap->second->parse( bePara, root, pPtrTable,
                                       pParamTable, pCLNameTmp );
            PD_RC_CHECK( rc, PDERROR,
                        "failed to build the opti tree(rc=%d)", rc );
            pDataPos += ossAlignX( (ossValuePtr)paraObj.objsize(), 4 );
            i++;
            pCLNameTmp = NULL;
         }
         catch ( std::exception &e )
         {
            PD_LOG( PDERROR,
                  "Failed to build tree, received unexpected error:%s",
                  e.what() );
            rc = SDB_INVALIDARG;
            goto error;
         }
      }
   done:
      return rc;
   error:
      SAFE_OSS_DELETE( root );
      goto done;
   }

   INT32 aggrBuilder::createContext( _qgmPlanContainer *container,
                                    _pmdEDUCB *cb, SINT64 &contextID )
   {
      INT32 rc = SDB_OK ;
      contextID = -1 ;

      rtnContextQGM *context = NULL ;
      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
      rc = rtnCB->contextNew ( RTN_CONTEXT_QGM, (rtnContext**)&context,
                               contextID, cb ) ;
      if ( SDB_OK != rc )
      {
         context = NULL ;
         PD_LOG ( PDERROR, "Failed to create new context" ) ;
         goto error ;
      }
      rc = context->open( container ) ;
      PD_RC_CHECK( rc, PDERROR, "Open context failed, rc: %d", rc ) ;

   done:
      return rc ;
   error:
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
         contextID = -1 ;
      }
      goto done ;
   }

   /*
      get global aggr cb
   */
   aggrBuilder* sdbGetAggrCB ()
   {
      static aggrBuilder s_aggrCB ;
      return &s_aggrCB ;
   }
}

