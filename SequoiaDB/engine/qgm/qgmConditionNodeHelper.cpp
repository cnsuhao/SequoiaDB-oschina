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

   Source File Name = qgmConditionNodeHelper.cpp

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

#include "qgmConditionNodeHelper.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"

namespace engine
{
   _qgmConditionNodeHelper::_qgmConditionNodeHelper( _qgmConditionNode *root )
   :_root( root )
   {
   }

   _qgmConditionNodeHelper::~_qgmConditionNodeHelper()
   {
      _root = NULL ;
   }

   string _qgmConditionNodeHelper::toJson() const
   {
   /*   stringstream ss ;
      ss << "{" ;
      _toString( _root, ss ) ;
      ss << "}" ;*/
      return toBson().toString() ;
   }

   BSONObj _qgmConditionNodeHelper::toBson( BOOLEAN keepAlias ) const
   {
      BSONObj obj ;
      return SDB_OK == _crtBson( _root, obj, keepAlias ) ?
             obj : BSONObj() ;
   }

   INT32 _qgmConditionNodeHelper::getAllAttr( qgmDbAttrPtrVec &fields )
   {
      INT32 rc = SDB_OK ;
      PD_CHECK( NULL != _root,
                SDB_INVALIDARG,
                error, PDDEBUG,
                "root node is a NULL pointer." ) ;

      rc = _getAllAttr( _root, fields ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   void _qgmConditionNodeHelper::releaseNodes( qgmConditionNodePtrVec &nodes )
   {
      qgmConditionNodePtrVec::iterator itr = nodes.begin() ;
      for ( ; itr != nodes.end(); itr++ )
      {
         SAFE_OSS_DELETE( *itr ) ;
      }
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMCONDITIONNODEHELPER_MERGE, "_qgmConditionNodeHelper::merge" )
   INT32 _qgmConditionNodeHelper::merge( _qgmConditionNode *node )
   {
      PD_TRACE_ENTRY( SDB__QGMCONDITIONNODEHELPER_MERGE ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != node, "node can't be NULL" ) ;
      if ( NULL == node )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( NULL != _root )
      {
         _qgmConditionNode *newRoot = SDB_OSS_NEW
                                  _qgmConditionNode( SQL_GRAMMAR::AND ) ;
         if ( NULL == newRoot )
         {
            PD_LOG( PDERROR, "failed to allocate mem." ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         newRoot->left = _root ;
         newRoot->right = node ;
         _root = newRoot ;
      }
      else
      {
         _root = node ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMCONDITIONNODEHELPER_MERGE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMCONDITIONNODEHELPER_SEPARATE, "_qgmConditionNodeHelper::separate" )
   INT32 _qgmConditionNodeHelper::separate( qgmConditionNodePtrVec &nodes )
   {
      PD_TRACE_ENTRY( SDB__QGMCONDITIONNODEHELPER_SEPARATE ) ;
      INT32 rc = SDB_OK ;

      if ( NULL == _root )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _separate( _root, nodes ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      _root = NULL ;

   done:
      PD_TRACE_EXITRC( SDB__QGMCONDITIONNODEHELPER_SEPARATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _qgmConditionNodeHelper::merge( qgmConditionNodePtrVec &nodes )
   {
      INT32 rc = SDB_OK ;
      qgmConditionNodePtrVec::iterator itr = nodes.begin() ;
      for ( ; itr != nodes.end(); itr++ )
      {
         rc = merge( *itr ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }


   PD_TRACE_DECLARE_FUNCTION( SDB__QGMCONDITIONNODEHELPER_SEPARATE2, "_qgmConditionNodeHelper::_separate2" )
   INT32 _qgmConditionNodeHelper::_separate( _qgmConditionNode *predicate,
                                             qgmConditionNodePtrVec &nodes )
   {
      PD_TRACE_ENTRY( SDB__QGMCONDITIONNODEHELPER_SEPARATE2 ) ;
      SDB_ASSERT( NULL != predicate, "predicate can't be NULL" ) ;
      SDB_ASSERT( SQL_GRAMMAR::EG == predicate->type ||
                  SQL_GRAMMAR::NE == predicate->type ||
                  SQL_GRAMMAR::GT == predicate->type ||
                  SQL_GRAMMAR::LT == predicate->type ||
                  SQL_GRAMMAR::GTE == predicate->type ||
                  SQL_GRAMMAR::LTE == predicate->type ||
                  SQL_GRAMMAR::AND == predicate->type ||
                  SQL_GRAMMAR::OR == predicate->type ||
                  SQL_GRAMMAR::LIKE == predicate->type ||
                  SQL_GRAMMAR::INN == predicate->type ||
                  SQL_GRAMMAR::NOT == predicate->type ||
                  SQL_GRAMMAR::IS == predicate->type,
                  "Invalid predicate type" ) ;

      INT32 rc = SDB_OK ;
      if ( SQL_GRAMMAR::AND == predicate->type )
      {
         rc = _separate( predicate->left, nodes ) ;
         if ( SDB_OK == rc )
         {
            _qgmConditionNode *rightTmp = predicate->right ;
            predicate->dettach() ;
            SAFE_OSS_DELETE( predicate ) ;
            predicate = rightTmp ;
            rc = _separate( predicate, nodes ) ;
         }
         else
         {
            rc = _separate( predicate->right, nodes ) ;
            if ( SDB_OK == rc )
            {
               _qgmConditionNode *leftTmp = predicate->left ;
               predicate->dettach() ;
               SAFE_OSS_DELETE( predicate ) ;
               predicate = leftTmp ;
            }
            else
            {
               goto error ;
            }
         }
      }
      /*else if ( SQL_GRAMMAR::OR == predicate->type )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }*/
      else
      {
         nodes.push_back( predicate ) ;
         predicate = NULL ;
      }
      /*
      else if ( SQL_GRAMMAR::STR == predicate->right->type
                || SQL_GRAMMAR::DIGITAL == predicate->right->type )
      {
         nodes.push_back( predicate ) ;
         predicate = NULL ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }*/
   done:
      PD_TRACE_EXITRC( SDB__QGMCONDITIONNODEHELPER_SEPARATE2, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMCONDITIONNODEHELPER__CRTBSON, "_qgmConditionNodeHelper::_crtBson" )
   INT32 _qgmConditionNodeHelper::_crtBson( const _qgmConditionNode *node,
                                           BSONObj &obj,
                                           BOOLEAN keepAlias )const
   {
      PD_TRACE_ENTRY( SDB__QGMCONDITIONNODEHELPER__CRTBSON ) ;
      INT32 rc = SDB_OK ;

      if ( NULL == node )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( SQL_GRAMMAR::EG == node->type ||
           SQL_GRAMMAR::NE == node->type ||
           SQL_GRAMMAR::GT == node->type ||
           SQL_GRAMMAR::LT == node->type ||
           SQL_GRAMMAR::GTE == node->type ||
           SQL_GRAMMAR::LTE == node->type ||
           SQL_GRAMMAR::AND == node->type ||
           SQL_GRAMMAR::OR == node->type ||
           SQL_GRAMMAR::LIKE == node->type ||
           SQL_GRAMMAR::INN == node->type ||
           SQL_GRAMMAR::NOT == node->type ||
           SQL_GRAMMAR::IS == node->type )
      {
         BSONObjBuilder builder ;
         BSONObj left, right ;

         if ( SQL_GRAMMAR::AND == node->type
              || SQL_GRAMMAR::OR == node->type )
         {
            BSONArrayBuilder andBuilder ;
            rc = _crtBson( node->left, left, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = _crtBson( node->right, right, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            andBuilder.append( left ) ;
            andBuilder.append( right ) ;
            if ( SQL_GRAMMAR::AND == node->type )
            {
               builder.append( "$and", andBuilder.arr() ) ;
            }
            else
            {
               builder.append( "$or", andBuilder.arr() ) ;
            }
         }
         else if ( SQL_GRAMMAR::EG == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            builder.append( ssLeft.str(),
                            toBson( node->right, "$et", 3 ) ) ;
         }
         else if ( SQL_GRAMMAR::NE == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            builder.append( ssLeft.str(),
                            toBson( node->right, "$ne",3 ) ) ;
         }
         else if ( SQL_GRAMMAR::LT == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            builder.append( ssLeft.str(),
                            toBson( node->right, "$lt",3 ) ) ;
         }
         else if ( SQL_GRAMMAR::GT == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            builder.append( ssLeft.str(),
                            toBson( node->right, "$gt",3 ) ) ;
         }
         else if ( SQL_GRAMMAR::LTE == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            builder.append( ssLeft.str(),
                            toBson( node->right, "$lte",4 ) ) ;
         }
         else if ( SQL_GRAMMAR::GTE == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            builder.append( ssLeft.str(),
                            toBson( node->right, "$gte",4 ) ) ;
         }
         else if ( SQL_GRAMMAR::LIKE == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            builder.appendRegex( ssLeft.str(),
                            node->right->value.toString(), "s" ) ;
         }
         else if ( SQL_GRAMMAR::INN == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            builder.append( ssLeft.str(),
                            toBson( node->right, "$in",3 ) ) ;
         }
         else if ( SQL_GRAMMAR::NOT == node->type )
         {
            BSONArrayBuilder notBuilder ;
            rc = _crtBson( node->left, left, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            notBuilder.append( left ) ;
            builder.append( "$not", notBuilder.arr() ) ;
         }
         else if ( SQL_GRAMMAR::IS == node->type )
         {
            stringstream ssLeft ;
            rc = _toString( node->left, ssLeft, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            builder.append( ssLeft.str(),
                            toBson( node->right, "$et", 3 ) ) ;
         }
         else
         {
            PD_LOG( PDERROR, "invalid node type:%d", node->type ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         obj = builder.obj() ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", node->type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }


   done:
      PD_TRACE_EXITRC( SDB__QGMCONDITIONNODEHELPER__CRTBSON, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMCONDITIONNODEHELPER__TOBSON, "_qgmConditionNodeHelper::toBson" )
   BSONObj _qgmConditionNodeHelper::toBson( const _qgmConditionNode *node,
                                            const CHAR *key,
                                            UINT32 size )
   {
      PD_TRACE_ENTRY( SDB__QGMCONDITIONNODEHELPER__TOBSON ) ;
      BSONObjBuilder builder ;

      string str( key, size ) ;

      if ( SQL_GRAMMAR::STR == node->type
           ||SQL_GRAMMAR::DBATTR == node->type )
      {
         builder.append( str, node->value.toString() ) ;
      }
      else if ( SQL_GRAMMAR::DIGITAL == node->type )
      {
         builder.appendAsNumber( str, node->value.toString() ) ;
      }
      else if ( SQL_GRAMMAR::SQLMAX < node->type )
      {
         if ( NULL != node->var &&
              !node->var->eoo() )
         {
            builder.appendAs( *(node->var), str ) ;
         }
         else
         {
            builder.appendNull( str ) ;
         }
      }
      else if ( SQL_GRAMMAR::NULLL == node->type )
      {
         builder.appendNull( str ) ;
      }
      else
      {
      }
      PD_TRACE_EXIT( SDB__QGMCONDITIONNODEHELPER__TOBSON ) ;
      return builder.obj() ;
   }

   BSONObj _qgmConditionNodeHelper::_toBson( const _qgmConditionNode *node,
                                            const CHAR *key ) const
   {
      return toBson( node, key, ossStrlen(key) ) ;
   }

   INT32 _qgmConditionNodeHelper::_toString( const _qgmConditionNode *node,
                                             stringstream &ss,
                                             BOOLEAN keepAlias ) const
   {
      INT32 rc = SDB_OK ;

      if ( NULL == node )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( SQL_GRAMMAR::EG == node->type ||
           SQL_GRAMMAR::NE == node->type ||
           SQL_GRAMMAR::GT == node->type ||
           SQL_GRAMMAR::LT == node->type ||
           SQL_GRAMMAR::GTE == node->type ||
           SQL_GRAMMAR::LTE == node->type ||
           SQL_GRAMMAR::AND == node->type ||
           SQL_GRAMMAR::OR == node->type ||
           SQL_GRAMMAR::IS == node->type ||
           SQL_GRAMMAR::NOT == node->type )
      {
         stringstream left, right ;
         SDB_ASSERT( NULL != node->left && NULL != node->right,
                     "impossible" ) ;
         rc = _toString( node->left, left, keepAlias ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         if ( SQL_GRAMMAR::NOT != node->type )
         {
            rc = _toString( node->right, right, keepAlias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         if ( SQL_GRAMMAR::AND == node->type )
         {
            ss << "$and:[{" << left.str() << "},{" << right.str() << "}]" ;
         }
         else if ( SQL_GRAMMAR::OR == node->type )
         {
            ss << "$or:[{" << left.str() << "},{" << right.str() << "}]" ;
         }
         else if ( SQL_GRAMMAR::NOT == node->type )
         {
            ss << "$not:[{" << left.str() << "}]" ;
         }
         else if ( SQL_GRAMMAR::EG == node->type ||
                   SQL_GRAMMAR::IS == node->type )
         {
            ss << left.str() << "{$et" << ":" << right.str() << "}";
         }
         else if ( SQL_GRAMMAR::NE == node->type )
         {
            ss << left.str() << ":{$ne:" << right.str() << "}" ;
         }
         else if ( SQL_GRAMMAR::LT == node->type )
         {
            ss << left.str() << ":{$lt:" << right.str() << "}" ;
         }
         else if ( SQL_GRAMMAR::GT == node->type )
         {
            ss << left.str() << ":{$gt:" << right.str() << "}" ;
         }
         else if ( SQL_GRAMMAR::LTE == node->type )
         {
            ss << left.str() << ":{$lte" << right.str() << "}" ;
         }
         else if ( SQL_GRAMMAR::GTE == node->type )
         {
            ss << left.str() << ":{$gte" << right.str() << "}" ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DBATTR == node->type ||
                SQL_GRAMMAR::DIGITAL == node->type ||
                SQL_GRAMMAR::STR == node->type )
      {
         if ( keepAlias )
         {
            ss << node->value.toString() ;
         }
         else
         {
            ss << node->value.attr().toString() ;
         }
      }
      else if ( SQL_GRAMMAR::LIKE == node->type )
      {
         SDB_ASSERT( NULL != node->left && NULL != node->right,
                     "impossible" ) ;
         stringstream left ;
         rc = _toString( node->left, left, keepAlias ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         ss << left.str() << ":{$regex:" <<
               node->right->value.toString() <<
               ", $options:\"s\"";
      }
      else if ( SQL_GRAMMAR::SQLMAX < node->type )
      {
         ss << "$var" ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", node->type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMCONDITIONNODEHELPER__GETALLATTR, "_qgmConditionNodeHelper::_getAllAttr" )
   INT32 _qgmConditionNodeHelper::_getAllAttr( _qgmConditionNode *node,
                                            vector<qgmDbAttr*> &fields )
   {
      PD_TRACE_ENTRY( SDB__QGMCONDITIONNODEHELPER__GETALLATTR ) ;
      INT32 rc = SDB_OK ;
      PD_CHECK( NULL != node,
                SDB_INVALIDARG,
                error, PDERROR,
                "root node is a NULL pointer." ) ;
      if ( SQL_GRAMMAR::EG == node->type ||
           SQL_GRAMMAR::NE == node->type ||
           SQL_GRAMMAR::GT == node->type ||
           SQL_GRAMMAR::LT == node->type ||
           SQL_GRAMMAR::GTE == node->type ||
           SQL_GRAMMAR::LTE == node->type ||
           SQL_GRAMMAR::AND == node->type ||
           SQL_GRAMMAR::OR == node->type ||
           SQL_GRAMMAR::IS == node->type ||
           SQL_GRAMMAR::LIKE == node->type ||
           SQL_GRAMMAR::INN == node->type )
      {
         rc = _getAllAttr( node->left, fields ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         rc = _getAllAttr( node->right, fields ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::NOT == node->type )
      {
         rc = _getAllAttr( node->left, fields ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DBATTR == node->type )
      {
         fields.push_back( &(node->value) ) ;
      }
      else
      {
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMCONDITIONNODEHELPER__GETALLATTR, rc ) ;
      return rc ;
   error:
      goto done ;
   }

}
