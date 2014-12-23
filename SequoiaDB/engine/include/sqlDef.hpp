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

   Source File Name = sqlDef.hpp

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

*******************************************************************************/

#ifndef SQLDEF_HPP_
#define SQLDEF_HPP_

#include "core.hpp"
#include "oss.hpp"

#include <boost/spirit/include/classic_ast.hpp>
#include <boost/spirit/include/classic_core.hpp>

#define BOOST_SPIRIT_THREADSAFE

#define BOOST_SPIRIT_GRAMMAR_STARTRULE_TYPE_LIMIT 20

using namespace BOOST_SPIRIT_CLASSIC_NS ;
using namespace boost::spirit ;
using namespace std ;

namespace engine
{
typedef const CHAR *  iteratorT ;
typedef tree_parse_info<iteratorT> SQL_AST ;
typedef tree_match<iteratorT>::container_t SQL_CONTAINER ;
typedef SQL_CONTAINER::const_iterator SQL_CON_ITR ;

#define SQL_RULE( ID ) \
        rule<ScannerT, parser_context<>, parser_tag<ID> >

#define SQL_BLANK (no_node_d[+blank_p])
#define SQL_BLANKORNO (no_node_d[*blank_p])

   struct _sqlGrammarID
   {
      const static INT32 SQL = 0 ;
      const static INT32 SELECT = 1 ;
      const static INT32 INSERT = 2 ;
      const static INT32 UPDATE = 3 ;
      const static INT32 DELETE_ = 4 ;
      const static INT32 CRTCS = 5 ;
      const static INT32 DROPCS = 6 ;
      const static INT32 CRTCL = 7 ;
      const static INT32 DROPCL = 8 ;
      const static INT32 CRTINDEX = 9 ;
      const static INT32 DROPINDEX = 10 ;
      const static INT32 LISTCS = 11 ;
      const static INT32 LISTCL = 12 ;
      const static INT32 WHERE = 13 ;
      const static INT32 SET = 14 ;
      const static INT32 VALUES = 15 ;
      const static INT32 ON = 16 ;
      const static INT32 UNIQUE = 17 ;
      const static INT32 DESC = 18 ;
      const static INT32 ASC = 19 ;
      const static INT32 INNERJOIN = 20 ;
      const static INT32 L_OUTERJOIN = 21 ;
      const static INT32 R_OUTERJOIN = 22 ;
      const static INT32 F_OUTERJOIN = 23 ;
      const static INT32 FROM = 24 ;
      const static INT32 ORDERBY = 25 ;
      const static INT32 HINT = 26 ;
      const static INT32 LIMIT = 27 ;
      const static INT32 OFFSET = 28 ;
      const static INT32 GROUPBY = 29 ;
      const static INT32 BEGINTRAN = 30 ;
      const static INT32 ROLLBACK = 31 ;
      const static INT32 COMMIT = 32 ;
      const static INT32 AS = 33 ;

      const static INT32 ET = 1000 ;
      const static INT32 NE = 1001 ;
      const static INT32 LT = 1002 ;
      const static INT32 GT = 1003 ;
      const static INT32 GTE = 1004 ;
      const static INT32 LTE = 1005 ;
      const static INT32 LBRACKETS = 1006 ;
      const static INT32 RBRACKETS = 1007 ;
      const static INT32 COMMA = 1009 ;
      const static INT32 WILDCARD = 1010 ;
      const static INT32 SUM = 1011 ;
      const static INT32 REDUCE = 1012 ;
      const static INT32 MULTIPLY = 1013 ;
      const static INT32 DIVIDE = 1014 ;

      const static INT32 DBATTR = 1500 ;
      const static INT32 DIGITAL = 1501 ;
      const static INT32 STR = 1502 ;
      const static INT32 FUNC = 1503 ;
      const static INT32 EXTERNALVAR = 1504 ;
      const static INT32 EXPRESSION = 1505 ;
      const static INT32 SQLMAX = 5000 ;

   } ;
   typedef struct _sqlGrammarID SQL_ID ;
}

#endif

