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

   Source File Name = aggrGroup.cpp

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
#include "aggrGroup.hpp"
#include "qgmDef.hpp"
#include "qgmOptiSelect.hpp"
#include "aggrDef.hpp"
#include "msgDef.h"
#include "qgmOptiTree.hpp"
#include "ossUtil.h"

using namespace bson;

namespace engine
{

   INT32 aggrGroupParser::buildNode( const BSONElement &elem,
                                       const CHAR *pCLName,
                                       qgmOptiTreeNode *&pNode,
                                       _qgmPtrTable *pTable,
                                       _qgmParamTable *pParamTable )
   {
      INT32 rc = SDB_OK;
      BOOLEAN hasFunc = FALSE;
      BSONObj obj;

      qgmOptiSelect *pSelect = SDB_OSS_NEW qgmOptiSelect( pTable, pParamTable );
      PD_CHECK( pSelect!=NULL, SDB_OOM, error, PDERROR,
               "malloc failed" );

      PD_CHECK( elem.type() == Object, SDB_INVALIDARG, error, PDERROR,
               "failed to parse the parameter:%s(type=%d, expectType=%d)",
               elem.fieldName(), elem.type(), Object );

      try
      {
         obj = elem.embeddedObject();
         {
            PD_CHECK( !obj.isEmpty(), SDB_INVALIDARG, error, PDERROR,
                     "Parameter-object can't be empty!" );
         }
         BSONObjIterator iter( obj );
         while ( iter.more() )
         {
            BSONElement beField = iter.next();
            const CHAR *pFieldName = beField.fieldName();
            if ( 0 == ossStrcmp( pFieldName, FIELD_NAME_GROUPBY_ID ))
            {
               rc = parseGroupbyField( beField, pSelect->_groupby, pTable, pCLName );
            }
            else
            {
               rc = parseSelectorField( beField, pCLName, pSelect->_selector,
                                       pTable, hasFunc );
            }
            PD_RC_CHECK( rc, PDERROR, "failed to parse the field:%s", pFieldName );
         }
         if ( pSelect->_selector.empty() )
         {
            qgmOpField selectAll;
            selectAll.type = SQL_GRAMMAR::WILDCARD;
            pSelect->_selector.push_back( selectAll );
         }
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                  "failed to parse the Parameter-object, received unexpected error:%s",
                  e.what() );
      }

      pSelect->_limit = -1;
      pSelect->_skip = 0;
      pSelect->_type = QGM_OPTI_TYPE_SELECT;
      pSelect->_hasFunc = hasFunc;
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
      
      pNode = pSelect;
   done:
      return rc;
   error:
      SAFE_OSS_DELETE( pSelect );
      goto done;
   }

   INT32 aggrGroupParser::parseGroupbyField( const bson::BSONElement &beId,
                                             qgmOPFieldVec &groupby,
                                             _qgmPtrTable *pTable,
                                             const CHAR *pCLName )
   {
      INT32 rc = SDB_OK;
      SDB_ASSERT( pTable != NULL , "_qgmPtrTable can't be NULL!") ;
      if ( beId.isNull() )
      {
         goto done;
      }
      try
      {
         switch ( beId.type() )
         {
            case String:
               {
                  const CHAR *pFieldName = beId.valuestr();
                  rc = addGroupByField( pFieldName, groupby, pTable, pCLName );
                  PD_RC_CHECK( rc, PDERROR,
                              "failed to parse _id(rc=%d)", rc );
                  break;
               }
            case Object:
               {
                  BSONObj idObj = beId.embeddedObject();
                  BSONObjIterator iter( idObj );
                  while( iter.more() )
                  {
                     BSONElement beField = iter.next();
                     PD_CHECK( beField.type()==String, SDB_INVALIDARG, error, PDERROR,
                              "failed to parse _id, sub-field type must be string!" );
                     const CHAR *pFieldName = beField.valuestr();
                     rc = addGroupByField( pFieldName, groupby, pTable, pCLName );
                     PD_RC_CHECK( rc, PDERROR,
                              "failed to parse _id(rc=%d)", rc );
                  }
                  break;
               }
            default:
               {
                  PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                              "the type of _id is invalid(type=%d)",
                              beId.type() );
               }
         }
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                  "failed to parse _id, received unexpected error:%s",
                  e.what() );
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 aggrGroupParser::addGroupByField( const CHAR *pFieldName,
                                          qgmOPFieldVec &groupby,
                                          _qgmPtrTable *pTable,
                                          const CHAR *pCLName )
   {
      INT32 rc = SDB_OK;
      qgmField gbValAttr;
      qgmField gbValRelegation;
      rc = pTable->getOwnField( AGGR_CL_DEFAULT_ALIAS,
                              gbValRelegation );
      PD_RC_CHECK( rc, PDERROR, "failed to get the field(%s)",
                  AGGR_CL_DEFAULT_ALIAS );

      PD_CHECK( AGGR_KEYWORD_PREFIX == pFieldName[0], SDB_INVALIDARG,
               error, PDERROR, "fieldname must begin with \"$\"!" );
      rc = pTable->getOwnField( &(pFieldName[1]), gbValAttr );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get the field(%s)",
                  &(pFieldName[1]) );
      {
         qgmDbAttr gbVal( gbValRelegation, gbValAttr );
         qgmField gbAlias;
         qgmOpField gbOpField;
         gbOpField.value = gbVal;
         gbOpField.alias = gbAlias;
         gbOpField.type = SQL_GRAMMAR::ASC;
         groupby.push_back( gbOpField );
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 aggrGroupParser::parseSelectorField( const BSONElement &beField,
                                             const CHAR *pCLName,
                                             qgmOPFieldVec &selectorVec,
                                             _qgmPtrTable *pTable,
                                             BOOLEAN &hasFunc )
   {
      INT32 rc = SDB_OK;
      CHAR *pFuncBuf = NULL;
      try
      {
         PD_CHECK( beField.type() == Object, SDB_INVALIDARG, error, PDERROR,
                  "failed to parse selector field, field type must be object!" );

         const CHAR *pAlias = beField.fieldName();
         BSONObj funcObj;
         funcObj = beField.embeddedObject();
         const CHAR *pFuncName = funcObj.firstElementFieldName();
         PD_CHECK( AGGR_KEYWORD_PREFIX == pFuncName[0], SDB_INVALIDARG, error, PDERROR,
                  "failed to parse selector field, function name must begin with \"$\"" );

         hasFunc = TRUE;

         qgmField slValAttr;
         qgmField slValRelegation;

         rc = parseInputFunc( funcObj, pCLName, slValAttr, pTable );
         PD_RC_CHECK( rc, PDERROR, "failed to parse selector field(rc=%d)" );

         qgmDbAttr slVal( slValRelegation, slValAttr );
         qgmField slAlias;
         rc = pTable->getOwnField( pAlias, slAlias );
         qgmOpField selector;
         selector.alias = slAlias;
         selector.value = slVal;
         selector.type = SQL_GRAMMAR::FUNC;
         selectorVec.push_back( selector );
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                  "failed to parse selector field, received unexpected error:%s",
                  e.what() );
      }
   done:
      SAFE_OSS_FREE( pFuncBuf );
      return rc;
   error:
      goto done;
   }

   INT32 aggrGroupParser::parseInputFunc( const BSONObj &funcObj,
                                          const CHAR *pCLName,
                                          qgmField &funcField,
                                          _qgmPtrTable *pTable )
   {
      INT32 rc = SDB_OK;
      CHAR *pFuncBuf = NULL;
      try
      {
         BSONElement beField = funcObj.firstElement();
         const CHAR *pFunc = NULL;
         const CHAR *pParam = NULL;
         UINT32 nameLen;
         UINT32 paramLen;
         UINT32 curPos = 0;
         const INT32 NUM_BUF_SIZE = 30;
         CHAR szTmp[NUM_BUF_SIZE] = {0};
         BOOLEAN isNumber = FALSE;
         UINT32 funcSize = 0;
         pFunc = beField.fieldName();
         PD_CHECK( AGGR_KEYWORD_PREFIX == pFunc[0], SDB_INVALIDARG, error, PDERROR,
                  "failed to parse function, function name must begin with \"$\"" );

         nameLen = ossStrlen(&(pFunc[1]));
         
         if ( pFunc[1] == 's' && pFunc[2] == 'u' && pFunc[3] == 'm'
               && pFunc[4] == '\0' && beField.isNumber() )
         {
            isNumber = TRUE;
            if ( beField.type() == NumberLong )
            {
               INT64 numPara = beField.numberLong();
               ossLltoa( numPara, szTmp, NUM_BUF_SIZE );
            }
            else
            {
               INT32 numPara = beField.number();
               ossItoa( numPara, szTmp, NUM_BUF_SIZE );
            }
         }
         else
         {
            PD_CHECK( beField.type()==String, SDB_INVALIDARG, error, PDERROR,
                     "failed to parse function, field-type must be string!" );
            pParam = funcObj.firstElement().valuestr();
            PD_CHECK( AGGR_KEYWORD_PREFIX == pParam[0] , SDB_INVALIDARG, error, PDERROR,
                     "failed to parse function, parameter must begin with \"$\"" );
         }

         paramLen = ossStrlen(AGGR_CL_DEFAULT_ALIAS) + 1;

         if ( isNumber )
         {
            paramLen += ossStrlen( szTmp );
         }
         else
         {
            paramLen += ossStrlen( &(pParam[1]) );
         }

         funcSize = nameLen + paramLen + 3;
         pFuncBuf = ( CHAR * )SDB_OSS_MALLOC( funcSize );
         PD_CHECK( pFuncBuf != NULL, SDB_OOM, error, PDERROR,
                  "malloc failed(size=%d)", funcSize );

         ossStrcpy( pFuncBuf, &(pFunc[1]) );
         pFuncBuf[ nameLen ]='(';
         curPos = nameLen + 1;

         ossStrcpy( ( pFuncBuf + curPos), AGGR_CL_DEFAULT_ALIAS );
         curPos += ossStrlen(AGGR_CL_DEFAULT_ALIAS);
         pFuncBuf[ curPos ] = '.';
         curPos += 1;

         if ( isNumber )
         {
            ossStrcpy( ( pFuncBuf + curPos ), szTmp );
            curPos += ossStrlen( szTmp );
         }
         else
         {
            ossStrcpy( ( pFuncBuf + curPos ), &(pParam[1]) );
            curPos += ossStrlen(&(pParam[1]));
         }
         pFuncBuf[ curPos ] = ')';
         curPos += 1;
         pFuncBuf[ curPos ] = 0;
         rc = pTable->getOwnField( pFuncBuf, funcField );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to get the field(%s)",
                     pFuncBuf );
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                  "failed to parse function, received unexpected error:%s",
                  e.what() );
      }
   done:
      SAFE_OSS_FREE( pFuncBuf );
      return rc;
   error:
      goto done;
   }
}
