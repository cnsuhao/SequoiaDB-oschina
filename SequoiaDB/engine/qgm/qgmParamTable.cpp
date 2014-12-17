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

   Source File Name = qgmParamTable.cpp

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

#include "qgmParamTable.hpp"
#include "pd.hpp"
#include "utilStr.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"

using namespace std;

namespace engine
{
   _qgmParamTable::_qgmParamTable()
   {

   }

   _qgmParamTable::~_qgmParamTable()
   {
      _const.clear() ;
      _var.clear() ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPARAMTABLE_ADDCONST, "_qgmParamTable::addConst" )
   INT32 _qgmParamTable::addConst( const qgmOpField &value,
                                   const BSONElement *&out )
   {
      PD_TRACE_ENTRY( SDB__QGMPARAMTABLE_ADDCONST ) ;
      INT32 rc = SDB_OK ;

      BSONObjBuilder builder ;
      _qgmBsonPair bPair ;
      try
      {
      if ( SQL_GRAMMAR::DIGITAL == value.type )
      {
         BOOLEAN r = FALSE ;
         r = builder.appendAsNumber( "$const",
                                     value.value.toString() ) ;
         if ( !r )
         {
            PD_LOG( PDERROR, "failed to append as number:%s",
                    value.value.toString().c_str() ) ;
            rc = SDB_SYS ;
            SDB_ASSERT( FALSE, "impossible" ) ;
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::STR == value.type )
      {
         builder.append( "$const",
                         value.value.toString() ) ;
      }
      else if ( SQL_GRAMMAR::DATE == value.type )
      {
         bson::Date_t t ;
         UINT64 millis = 0 ;
         rc = utilStr2Date( value.value.toString().c_str(),
                            millis ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDDEBUG, "failed to parse to Date_t:%s",
                    value.value.toString().c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         t.millis = millis ;
         builder.appendDate( "$const", t ) ;
      }
      else
      {
         PD_LOG( PDERROR, "wrong type:%d", value.type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      bPair.obj = builder.obj() ;
      _const.push_back(  bPair ) ;
      _qgmBsonPair &p =  _const.at( _const.size() -1 ) ;
      p.ele = p.obj.firstElement() ;
      out = &(p.ele) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened :%s",
                 e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPARAMTABLE_ADDCONST, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPARAMTABLE_ADDCONST2, "_qgmParamTable::addConst2" )
   INT32 _qgmParamTable::addConst( const BSONObj &obj,
                                   const BSONElement *&out )
   {
      PD_TRACE_ENTRY( SDB__QGMPARAMTABLE_ADDCONST2 ) ;
      INT32 rc = SDB_OK ;
      _qgmBsonPair bPair ;
      try
      {
         bPair.obj = obj ;
         _const.push_back( bPair ) ;
         _qgmBsonPair &p =  _const.at( _const.size() -1 ) ;
         p.ele = p.obj.firstElement() ;
         out = &(p.ele) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
   
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPARAMTABLE_ADDCONST2, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPARATABLE_ADDVAR, "_qgmParamTable::addVar" )
   INT32 _qgmParamTable::addVar( const qgmDbAttr &key,
                                 const BSONElement *&out,
                                 BOOLEAN *pExisted )
   {
      PD_TRACE_ENTRY( SDB__QGMPARATABLE_ADDVAR ) ;
      INT32 rc = SDB_OK ;

      pair<QGM_VAR_TABLE::iterator, BOOLEAN> rInsert =
                        _var.insert( std::make_pair(key, BSONElement()) ) ;
      if ( !rInsert.second )
      {
         PD_LOG( PDDEBUG, "repeat name was found:%s",
                 key.toString().c_str() ) ;
         if ( pExisted )
         {
            *pExisted = TRUE ;
         }
      }

      out = &( rInsert.first->second ) ;
      PD_TRACE_EXITRC( SDB__QGMPARATABLE_ADDVAR, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPARAMTABLE_SETVAR, "_qgmParamTable::setVar" )
   INT32 _qgmParamTable::setVar( const varItem &item,
                                 const BSONObj &obj )
   {
      PD_TRACE_ENTRY( SDB__QGMPARAMTABLE_SETVAR ) ;
      INT32 rc = SDB_OK ;
      try
      {
         BSONElement ele = obj.getField( item._fieldName.attr().toString() ) ;
         if ( ele.eoo() )
         {
            PD_LOG( PDDEBUG, "key[%s] was not found in bson obj",
                    item._fieldName.toString().c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         {
         QGM_VAR_TABLE::iterator itr = _var.find( item._varName ) ;
         if ( _var.end() == itr )
         {
            PD_LOG( PDDEBUG, "key[%s] was not found in var table",
                    item._varName.toString().c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         else
         {
            itr->second = ele ;
         }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happpend: %s",
                 e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPARAMTABLE_SETVAR, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}
