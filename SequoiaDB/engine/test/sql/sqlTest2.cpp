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

*******************************************************************************/

#include "ossTypes.hpp"
#include <gtest/gtest.h>
#define BOOST_SPIRIT_DEBUG
#include "sqlGrammar.hpp"
#include "sqlUtil.hpp"
#include "qgmBuilder.hpp"
#include "optQgmOptimizer.hpp"
#include "optQgmStrategy.hpp"
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>
#include "qgmPlan.hpp"
#include "utilStr.hpp"
#include "qgmPlanContainer.hpp"

#include "pmd.hpp"

#include <stdio.h>
#include <string>
#include <iostream>


using namespace engine;
using namespace std;

void dump( _qgmPlan *node, INT32 indent )
{
   cout << string( indent * 4, ' ') << "|--" << node->toString() << endl ;
   for ( UINT32 i = 0; i < node->inputSize(); i++ )
   {
      dump( node->input(i), indent+1) ;
   }
}

void dump(_qgmPlan *root)
{
   dump( root, 0 ) ;
}


TEST(sqlTest, parse_1)
{
   INT32 rc = SDB_OK ;
   SQL_GRAMMAR grammar ;
   qgmOptiTreeNode *qgm = NULL ;
   CHAR *line = new CHAR[1024*1024];
   CHAR *dumpBuf = new CHAR[1024*1024*10] ;

   sdbEnablePD( NULL ) ;
   setPDLevel( PDDEBUG ) ;
   getQgmStrategyTable()->init() ;

   while ( TRUE )
   {
      _qgmPlanContainer container ;
      qgmBuilder builder( container.ptrTable(), container.paramTable() ) ;
      memset(line, 0, 1024*1024 );
      memset(dumpBuf, 0, 1024*1024*10 );
      cout << "sql>" ;
      cin.getline( line, 1024*1024 ) ;
      const CHAR *sql = NULL ;
      utilStrTrim( line, sql ) ;
      SQL_AST &ast = container.ast() ;
      ast = ast_parse( sql, grammar ) ;
      _qgmPlan *plan = NULL ;
      cout << "match:"<< ast.match << " full:" << ast.full << endl ;
      if ( ast.match )
         {
            sqlDumpAst( ast.trees ) ;
            cout << endl ;
            rc = builder.build( ast.trees, qgm ) ;
            if ( SDB_OK == rc )
            {
               qgm->dump() ;
               cout << "**************" << endl ;

               qgmOptiTreeNode *e = NULL ;
               rc = qgm->extend( e ) ;
               if ( SDB_OK != rc )
               {
                  cout << "rc = " << rc << endl ;
                  break ;
               }
               else
               {
                  cout << "After extent, tree dump:" << endl ;
                  e->dump() ;
                  qgm = e ;
               }

               qgmOptTree tree( qgm ) ;
               optQgmOptimizer optimizer ;
               rc = optimizer.adjust( tree ) ;
               if ( SDB_OK != rc )
               {
                  cout << "opt failed, rc = " << rc << endl ;
                  break ;
               }
               else
               {
                  cout << "After optimizer, tree dump:" << endl ;
                  qgm = tree.getRoot() ;
                  qgm->dump() ;
               }

               rc = builder.build( tree.getRoot(), plan ) ;
               if ( SDB_OK == rc )
               {
                  cout << endl << "plan tree:" << endl ;
                  container.plan() = plan ;
                  rc = qgmDump( &container, dumpBuf, 1024*1024*10) ;
                  if ( SDB_OK == rc )
                  cout << dumpBuf << endl ;
               }
               else
               {
                  cout << "rc:" << rc << endl ;
                  break ;
               }

               delete qgm ;
               qgm = NULL ;
            }
         }
      else
         cout << ast.stop << endl ;
   }

   delete []line ;
}

/*
TEST(sqlTest, abc1)
{
   boost::filesystem::path fullpath( boost::filesystem::initial_path()) ;
   string p("../abc/cba") ;
   fullpath = boost::filesystem::system_complete( boost::filesystem::path(p))  ;
   std::cout << fullpath.string() << endl ;
}
*/
