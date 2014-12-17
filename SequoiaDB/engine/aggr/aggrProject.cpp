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

   Source File Name = aggrProject.hpp

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
#include "qgmOptiSelect.hpp"
#include "aggrDef.hpp"
#include "aggrProject.hpp"

using namespace bson;

namespace engine
{
   INT32 aggrProjectParser::buildNode( const BSONElement &elem,
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
            PD_CHECK( pFieldName[0] != AGGR_KEYWORD_PREFIX, SDB_INVALIDARG, error, PDERROR,
                     "failed to parse \"project\", field name can't begin with\"$\"!" );

            rc = parseSelectorField( beField, pCLName, pSelect->_selector,
                                    pTable, hasFunc );
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

   INT32 aggrProjectParser::parseSelectorField( const BSONElement &beField,
                                             const CHAR *pCLName,
                                             qgmOPFieldVec &selectorVec,
                                             _qgmPtrTable *pTable,
                                             BOOLEAN &hasFunc )
   {
      INT32 rc = SDB_OK;
      try
      {
         if ( beField.isNumber() )
         {
            if ( beField.number() == 0 )
            {
               goto done;
            }
            const CHAR *pAlias = beField.fieldName();
            const CHAR *pPara = beField.fieldName();
            rc = addField( pAlias, pPara, pCLName, selectorVec, pTable );
            PD_RC_CHECK( rc, PDERROR,
                        "failed to add the field(name=%s, rc=%d)",
                        pPara, rc );
         }
         else if ( beField.type() == String )
         {
            const CHAR *pAlias = beField.fieldName();
            const CHAR *pPara = beField.valuestr();
            PD_CHECK( AGGR_KEYWORD_PREFIX == pPara[0], SDB_INVALIDARG, error, PDERROR,
                     "failed to parse selector field(%s), parameter must begin with \"$\"",
                     pPara );
            rc = addField( pAlias, &(pPara[1]), pCLName, selectorVec, pTable );
            PD_RC_CHECK( rc, PDERROR,
                        "failed to add the field(alias-name=%s, para-name=%s, rc=%d)",
                        pAlias, pPara, rc );
         }
         else if ( beField.type() == Object )
         {
            const CHAR *pAlias = beField.fieldName();
            BSONObj fieldObj = beField.embeddedObject();
            rc = addObj( pAlias, fieldObj, pCLName, selectorVec, pTable );
            PD_RC_CHECK( rc, PDERROR,
                        "failed add the function(alis-name=%s, rc=%d)",
                        pAlias, rc );
            hasFunc = TRUE;
         }
         else
         {
            PD_RC_CHECK( SDB_INVALIDARG, PDERROR,
                        "failed to parse the field(%s), invalid field type!",
                        beField.fieldName() );
         }
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                  "failed to parse selector field, received unexpected error:%s",
                  e.what() );
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 aggrProjectParser::addFunc( const CHAR *pAlias, const bson::BSONObj &funcObj,
                                    const CHAR *pCLName, qgmOPFieldVec &selectorVec,
                                    _qgmPtrTable *pTable )
   {
      INT32 rc = SDB_OK;
      CHAR *pFuncBuf = NULL;
      try
      {
         const CHAR *pFuncName = funcObj.firstElementFieldName();
         PD_CHECK( AGGR_KEYWORD_PREFIX == pFuncName[0], SDB_INVALIDARG, error, PDERROR,
                  "failed to parse selector field(%s), function name must begin with \"$\"",
                  pFuncName );
         const CHAR *pFuncParam = funcObj.firstElement().valuestr();
         PD_CHECK( AGGR_KEYWORD_PREFIX == pFuncParam[0], SDB_INVALIDARG, error, PDERROR,
                  "failed to parse selector field(%s), parameter must begin with \"$\"",
                  pFuncParam );

         qgmField slValAttr;
         qgmField slValRelegation;

         UINT32 nameLen = ossStrlen(&(pFuncName[1]));
         UINT32 paramLen = ossStrlen(AGGR_CL_DEFAULT_ALIAS) + 1 + ossStrlen(&(pFuncParam[1]));
         UINT32 curPos = 0;
         pFuncBuf = ( CHAR * )SDB_OSS_MALLOC( nameLen + 3 + paramLen );
         PD_CHECK( pFuncBuf != NULL, SDB_OOM, error, PDERROR,
                  "malloc failed(size=%d)", (nameLen + 3 + paramLen) );
         ossStrcpy( pFuncBuf, &(pFuncName[1]) );
         pFuncBuf[ nameLen ]='(';
         curPos = nameLen + 1;

         ossStrcpy( ( pFuncBuf + curPos), AGGR_CL_DEFAULT_ALIAS );
         curPos += ossStrlen(AGGR_CL_DEFAULT_ALIAS);
         pFuncBuf[ curPos ] = '.';
         curPos += 1;

         ossStrcpy( ( pFuncBuf + curPos ), &(pFuncParam[1]) );
         curPos += ossStrlen(&(pFuncParam[1]));
         pFuncBuf[ curPos ] = ')';
          curPos += 1;
         pFuncBuf[ curPos ] = 0;
         rc = pTable->getOwnField( pFuncBuf, slValAttr );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to get the field(%s)",
                     pFuncBuf );
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
                  "failed to add function-field, received unexpected error:%s",
                  e.what() );
      }
   done:
      if ( pFuncBuf != NULL )
      {
         SDB_OSS_FREE( pFuncBuf );
      }
      return rc;
   error:
      goto done;
   }

   INT32 aggrProjectParser::addField( const CHAR *pAlias, const CHAR *pPara,
                                    const CHAR *pCLName, qgmOPFieldVec &selectorVec,
                                    _qgmPtrTable *pTable )
   {
      INT32 rc = SDB_OK;
      qgmField slValAttr;
      qgmField slValRelegation;
      qgmOpField selector;

      rc = pTable->getOwnField( AGGR_CL_DEFAULT_ALIAS, slValRelegation );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get the field(%s)",
                  AGGR_CL_DEFAULT_ALIAS );

      rc = pTable->getOwnField( pPara, slValAttr );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get the field(%s)",
                  pPara );

      rc = pTable->getOwnField( pAlias, selector.alias );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get the field(%s)",
                  pAlias );

      {
         qgmDbAttr slVal( slValRelegation, slValAttr );
         selector.value = slVal;
         selector.type = SQL_GRAMMAR::DBATTR;
         selectorVec.push_back( selector );
      }
   done:
      return rc;
   error:
      goto done;
   }

   INT32 aggrProjectParser::addObj( const CHAR *pAlias, const bson::BSONObj &Obj,
                                    const CHAR *pCLName, qgmOPFieldVec &selectorVec,
                                    _qgmPtrTable *pTable )
   {
      INT32 rc = SDB_OK;
      CHAR *pFuncBuf = NULL;
#define FUNC_NAME_BUILDOBJ    "buildObj"
      try
      {
         UINT32 nameLen = ossStrlen( FUNC_NAME_BUILDOBJ );
         UINT32 paramLen = 0;
         UINT32 fieldNum = 0;
         UINT32 curPos = 0;
         BSONObjIterator iter( Obj );
         while ( iter.more() )
         {
            BSONElement beField = iter.next();
            PD_CHECK( beField.isNumber(), SDB_INVALIDARG, error, PDERROR,
                     "field type must be number!" );
            if ( beField.number() == 0 )
            {
               continue;
            }
            ++fieldNum;
            paramLen += ossStrlen(AGGR_CL_DEFAULT_ALIAS) + 1
                        + ossStrlen( beField.fieldName() );
         }

         PD_CHECK( fieldNum > 0, SDB_INVALIDARG, error, PDERROR,
                  "Can't add empty obj!" );
         paramLen += ( fieldNum - 1 );
         pFuncBuf = ( CHAR * )SDB_OSS_MALLOC( nameLen + 3 + paramLen );
         PD_CHECK( pFuncBuf != NULL, SDB_OOM, error, PDERROR,
                  "malloc failed(size=%d)", ( nameLen + 3 + paramLen ));
         ossStrcpy( pFuncBuf, FUNC_NAME_BUILDOBJ );
         pFuncBuf[ nameLen ] = '(';
         curPos = nameLen + 1;

         iter = BSONObjIterator( Obj );
         while ( iter.more() )
         {
            BSONElement beField = iter.next();
            if ( beField.number() == 0 )
            {
               continue;
            }
            ossStrcpy( ( pFuncBuf + curPos ), AGGR_CL_DEFAULT_ALIAS );
            curPos += ossStrlen( AGGR_CL_DEFAULT_ALIAS );
            pFuncBuf[ curPos ] = '.';
            curPos += 1;
            ossStrcpy( ( pFuncBuf + curPos ), beField.fieldName() );
            curPos += ossStrlen( beField.fieldName() );
            if ( fieldNum > 1 )
            {
               pFuncBuf[ curPos ] = ',';
               curPos += 1;
            }
            --fieldNum;
         }
         pFuncBuf[ curPos ] = ')';
         curPos += 1;
         pFuncBuf[ curPos ] = 0;
         qgmField slValAttr;
         qgmField slValRelegation;
         rc = pTable->getOwnField( pFuncBuf, slValAttr );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to get the field(%s)",
                     pFuncBuf );
         qgmDbAttr slVal( slValRelegation, slValAttr );
         qgmField slAlias;
         rc = pTable->getOwnField( pAlias, slAlias );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to get the field(%s)",
                     pAlias );
         qgmOpField selector;
         selector.alias = slAlias;
         selector.value = slVal;
         selector.type = SQL_GRAMMAR::FUNC;
         selectorVec.push_back( selector );
      }
      catch ( std::exception &e )
      {
         PD_CHECK( SDB_INVALIDARG, SDB_INVALIDARG, error, PDERROR,
                  "failed to add function-field, received unexpected error:%s",
                  e.what() );
      }
   done:
      SDB_OSS_FREE( pFuncBuf );
      return rc;
   error:
      goto done;
   }
}
