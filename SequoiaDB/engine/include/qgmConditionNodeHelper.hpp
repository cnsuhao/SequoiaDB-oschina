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

   Source File Name = qgmConditionNodeHelper.hpp

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

#ifndef QGMCONDITIONNODEHELPER_HPP_
#define QGMCONDITIONNODEHELPER_HPP_

#include "qgmConditionNode.hpp"
#include <sstream>

using namespace std ;

namespace engine
{
   class _qgmConditionNodeHelper : public SDBObject
   {
   public:
      _qgmConditionNodeHelper( _qgmConditionNode *root ) ;
      virtual ~_qgmConditionNodeHelper() ;

      _qgmConditionNode* getRoot() { return _root ; }
      void setRoot( _qgmConditionNode *pRoot ) { _root = pRoot ; }

   public:
      static void releaseNodes( qgmConditionNodePtrVec &nodes ) ;


      static BSONObj toBson( const _qgmConditionNode *node,
                             const CHAR *begin,
                             UINT32 size ) ;
/*
      static BSONObj toBson( const _qgmConditionNode *node,
                             const CHAR *begin ) ;
*/

   public:
      string toJson() const ;

      BSONObj toBson( BOOLEAN keepAlias = TRUE ) const ;

      INT32 getAllAttr( qgmDbAttrPtrVec &fields ) ;

      INT32 separate( qgmConditionNodePtrVec &nodes ) ;

      INT32 merge( qgmConditionNodePtrVec &nodes ) ;

      INT32 merge( _qgmConditionNode *node ) ;


   private:
      INT32 _getAllAttr( _qgmConditionNode *node,
                         qgmDbAttrPtrVec &fields ) ;

      INT32 _toString( const _qgmConditionNode *,
                       stringstream &ss,
                       BOOLEAN keepAlias ) const ;

      INT32 _separate( _qgmConditionNode *predicate,
                       qgmConditionNodePtrVec &nodes ) ;

      INT32 _crtBson( const _qgmConditionNode *node,
                     BSONObj &obj,
                     BOOLEAN keepAlias ) const ;

      BSONObj _toBson( const _qgmConditionNode *node,
                       const CHAR *begin,
                       UINT32 size ) const ;

      BSONObj _toBson( const _qgmConditionNode *node,
                       const CHAR *begin ) const ;


   private:
      _qgmConditionNode *_root ;
   } ;
   typedef class _qgmConditionNodeHelper qgmConditionNodeHelper ;
}

#endif

