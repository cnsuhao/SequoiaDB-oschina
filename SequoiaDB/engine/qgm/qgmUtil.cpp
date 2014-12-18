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

   Source File Name = qgmUtil.cpp

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

#include "qgmUtil.hpp"
#include "pd.hpp"
#include "ossUtil.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"

namespace engine
{
   PD_TRACE_DECLARE_FUNCTION( SDB__QGMUTILFIRSTDOT, "qgmUtilFirstDot" )
   BOOLEAN qgmUtilFirstDot( const CHAR *str, UINT32 len, UINT32 &num )
   {
      PD_TRACE_ENTRY( SDB__QGMUTILFIRSTDOT ) ;
      SDB_ASSERT( NULL != str, "impossible" ) ;
      BOOLEAN found = FALSE ;

      UINT32 tLen = 0 ;

      while ( tLen < len )
      {
         const CHAR *tmp = str + tLen ;
         if ( '.' == *tmp )
         {
            num = tLen + 1 ;
            found = TRUE ;
            break ;
         }
         ++tLen ;
      }
      PD_TRACE_EXIT( SDB__QGMUTILFIRSTDOT ) ;
      return found ;
   }

   BOOLEAN qgmUtilSame( const CHAR *src, UINT32 srcLen,
                        const CHAR *dst, UINT32 dstLen )
   {
      return srcLen == dstLen ?
             SDB_OK == ossStrncmp( src, dst, srcLen ) : FALSE ;
   }

   enum QGM_FIND_FUNC_STATUS
   {
      FUNC = 0,
      FIELD,
   } ;

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMFINDFIELDFROMFUNC, "qgmFindFieldFromFunc" )
   INT32 qgmFindFieldFromFunc( const CHAR *str, UINT32 strSize,
                               _qgmField &func,
                               vector< qgmOpField > &params,
                               _qgmPtrTable *table,
                               BOOLEAN needRele )
   {
      PD_TRACE_ENTRY( SDB__QGMFINDFIELDFROMFUNC ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != str, "impossible" ) ;

      const CHAR *tmp = NULL ;
      UINT32 read = 0 ;
      UINT32 size = 0 ;
      UINT32 fieldNameSize = 0 ;
      const CHAR *begin = str ;
      const CHAR *fieldName = str ;
      QGM_FIND_FUNC_STATUS status = FUNC;
      while ( read < strSize )
      {
         tmp = str + read ;

         if ( ' ' == *tmp || '\t' == *tmp )
         {
            ++read ;
            continue ;
         }
         else if ( FUNC == status )
         {
            if ( '(' == *tmp )
            {
               status = FIELD ;
               rc = table->getField( begin, size, func ) ;
               if ( SDB_OK != rc )
               {
                  goto error ;
               }
               size = 0 ;
               begin = NULL ;
            }
            else
            {
               ++size ;
            }
         }
         else
         {
            if ( ',' == *tmp )
            {
               qgmOpField field;
               if ( NULL == begin || NULL == fieldName )
               {
                  PD_LOG( PDERROR, "Aggr func param is null" ) ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
               rc = table->getAttr( begin, size, field.value ) ;
               if ( SDB_OK != rc )
               {
                  goto error ;
               }
               if ( needRele && field.value.relegation().empty() )
               {
                  goto error ;
               }

               field.alias = field.value.attr() ;

               params.push_back( field ) ;
               begin = NULL ;
               size = 0 ;
               fieldName = NULL ;
               fieldNameSize = 0 ;
            }
            else if ( ')' == *tmp )
            {
               if ( NULL != begin && NULL != fieldName )
               {
                  qgmOpField field ;
                  rc = table->getAttr( begin, size, field.value ) ;
                  if ( SDB_OK != rc )
                  {
                     goto error ;
                  }
                  if ( needRele && field.value.relegation().empty() )
                  {
                     goto error ;
                  }

                  field.alias = field.value.attr() ;
                  params.push_back( field ) ;
               }
               begin = NULL ;
               size = 0 ;
               fieldName = NULL ;
               fieldNameSize = 0 ;
            }
            else
            {
               if ( NULL == begin )
               {
                  begin = tmp ;
               }
               ++size ;
               if ( NULL == fieldName )
               {
                  fieldName = tmp;
               }
               ++fieldNameSize ;
               if ( '.' == *tmp )
               {
                  fieldName = NULL;
                  fieldNameSize = 0;
               }
            }
         }

         ++read ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMFINDFIELDFROMFUNC, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMISFROMONE, "isFromOne")
   BOOLEAN isFromOne( const qgmOpField & left, const qgmOPFieldVec & right,
                      BOOLEAN useAlias, UINT32 *pPos )
   {
      PD_TRACE_ENTRY( SDB__QGMISFROMONE ) ;
      UINT32 i = 0 ;
      while ( i < right.size() )
      {
         if ( left.isFrom( right[i], useAlias ) )
         {
            if ( pPos )
            {
               *pPos = i ;
            }
            return TRUE ;
         }
         ++i ;
      }
      PD_TRACE_EXIT( SDB__QGMISFROMONE) ;
      return FALSE ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMISSAMEFROM, "isSameFrom" )
   BOOLEAN isSameFrom( const qgmOPFieldVec & left, const qgmOPFieldVec & right )
   {
      PD_TRACE_ENTRY( SDB__QGMISSAMEFROM ) ;
      if ( left.size() != right.size() )
      {
         return FALSE ;
      }

      UINT32 i = 0 ;
      while ( i < left.size() )
      {
         qgmOpField field ;

         if ( ( !right[i].alias.empty() &&
                 left[i].value.attr() != right[i].alias ) ||
              ( right[i].alias.empty() &&
                left[i].value.attr() != right[i].value.attr() ) )
         {
            return FALSE ;
         }
         ++i ;
      }
      PD_TRACE_EXIT( SDB__QGMISSAMEFROM ) ;
      return TRUE ;
   }

   BOOLEAN isFrom( const qgmDbAttr &left, const qgmOpField &right,
                   BOOLEAN useAlias )
   {
      if ( ( useAlias && left.attr() == right.alias )
           || left == right.value || right.type == SQL_GRAMMAR::WILDCARD )
      {
         return TRUE ;
      }
      return FALSE ;
   }

   BOOLEAN isFromOne( const qgmDbAttr &left, const qgmOPFieldVec &right,
                      BOOLEAN useAlias, UINT32 *pPos )
   {
      UINT32 i = 0 ;
      while ( i < right.size() )
      {
         if ( isFrom( left, right[i], useAlias ) )
         {
            if ( pPos )
            {
               *pPos = i ;
            }
            return TRUE ;
         }
         ++i ;
      }
      return FALSE ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMMERGE, "qgmMerge" )
   BSONObj qgmMerge( const BSONObj &left, const BSONObj &right )
   {
      PD_TRACE_ENTRY( SDB__QGMMERGE ) ;
      BSONObj obj ;
      BSONObjBuilder builder ;
      try
      {
         BSONObjIterator itr1( left ) ;
         while ( itr1.more() )
         {
            builder.append( itr1.next() ) ;
         }

         BSONObjIterator itr2( right ) ;
         while ( itr2.more() )
         {
            builder.append( itr2.next() ) ;
         }

         obj = builder.obj() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s",
                 e.what() ) ;
      }

      PD_TRACE_EXIT( SDB__QGMMERGE ) ;
      return obj ;
   }

   string qgmPlanType( QGM_PLAN_TYPE type )
   {
      string t ;
      if ( QGM_PLAN_TYPE_RETURN == type )
      {
         t = "RETURN" ;
      }
      else if ( QGM_PLAN_TYPE_FILTER == type )
      {
         t = "FILTER" ;
      }
      else if ( QGM_PLAN_TYPE_SCAN == type )
      {
         t = "SCAN" ;
      }
      else if ( QGM_PLAN_TYPE_NLJOIN == type )
      {
         t = "NLJOIN" ;
      }
      else if ( QGM_PLAN_TYPE_INSERT == type )
      {
         t = "INSERT" ;
      }
      else if ( QGM_PLAN_TYPE_UPDATE == type )
      {
         t = "UPDATE" ;
      }
      else if ( QGM_PLAN_TYPE_AGGR == type )
      {
         t = "AGGREGATION" ;
      }
      else if ( QGM_PLAN_TYPE_SORT == type )
      {
         t = "SORT" ;
      }
      else if ( QGM_PLAN_TYPE_COMMAND == type )
      {
         t = "COMMAND" ;
      }
      else if ( QGM_PLAN_TYPE_DELETE == type )
      {
         t = "DELETE" ;
      }
      else
      {
      }

      return t ;
   }

   BOOLEAN isWildCard( const qgmOPFieldVec & fields )
   {
      if ( 1 == fields.size() && SQL_GRAMMAR::WILDCARD == fields[0].type )
      {
         return TRUE ;
      }
      return FALSE ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMREPLACEFIELDRELE, "replaceFieldRele" )
   void replaceFieldRele( qgmOPFieldVec &fields, const qgmField &newRele )
   {
      PD_TRACE_ENTRY( SDB__QGMREPLACEFIELDRELE ) ;
      qgmOPFieldVec::iterator it = fields.begin() ;
      while ( it != fields.end() )
      {
         qgmOpField &field = *it ;
         if ( SQL_GRAMMAR::WILDCARD != field.type )
         {
            field.value.relegation() = newRele ;
         }
         ++it ;
      }

      PD_TRACE_EXIT( SDB__QGMREPLACEFIELDRELE ) ;
      return ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMREPLACEATTRRELE, "replaceAttrRele" )
   void replaceAttrRele( qgmDbAttrPtrVec &attrs,  const qgmField &newRele )
   {
      PD_TRACE_ENTRY( SDB__QGMREPLACEATTRRELE ) ;
      qgmDbAttrPtrVec::iterator it = attrs.begin() ;
      while ( it != attrs.end() )
      {
         qgmDbAttr &attr = *(*it) ;
         attr.relegation() = newRele ;
         ++it ;
      }
      PD_TRACE_EXIT( SDB__QGMREPLACEATTRRELE ) ;
      return ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMREPLACEATTRRELE2, "replaceAttrRele2" )
   void replaceAttrRele( qgmDbAttrVec &attrs,  const qgmField &newRele )
   {
      PD_TRACE_ENTRY( SDB__QGMREPLACEATTRRELE2 ) ;
      qgmDbAttrVec::iterator it = attrs.begin() ;
      while ( it != attrs.end() )
      {
         qgmDbAttr &attr = *it ;
         attr.relegation() = newRele ;
         ++it ;
      }

      PD_TRACE_EXIT( SDB__QGMREPLACEATTRRELE2 ) ;
      return ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMREPLACEATTRRELE3, "replaceAggrRele3" )
   void replaceAggrRele( qgmAggrSelectorVec & aggrs, const qgmField & newRele )
   {
      PD_TRACE_ENTRY( SDB__QGMREPLACEATTRRELE3 ) ;
      qgmAggrSelectorVec::iterator it = aggrs.begin() ;
      while ( it != aggrs.end() )
      {
         qgmAggrSelector &selector = *it ;
         if ( SQL_GRAMMAR::FUNC == selector.value.type )
         {
            replaceFieldRele( selector.param, newRele ) ;
         }
         else if ( SQL_GRAMMAR::WILDCARD != selector.value.type )
         {
            selector.value.value.relegation() = newRele ;
         }
         ++it ;
      }

      PD_TRACE_EXIT( SDB__QGMREPLACEATTRRELE3 ) ;
      return ;
   }

   void clearFieldAlias( qgmOPFieldVec & fields )
   {
      UINT32 count = fields.size() ;
      UINT32 index = 0 ;
      while ( index < count )
      {
         fields[index].alias.clear() ;
         ++index ;
      }
   }

   static INT32 downAFieldByFieldAlias( qgmOpField &field,
                                        const qgmOPFieldPtrVec & fieldAlias,
                                        BOOLEAN needCopyAlias )
   {
      qgmOPFieldPtrVec::const_iterator cit = fieldAlias.begin() ;
      while ( cit != fieldAlias.end() )
      {
         if ( ( (*cit)->alias.empty() &&
                field.value.attr() == (*cit)->value.attr() ) ||
              ( !(*cit)->alias.empty() &&
                field.value.attr() == (*cit)->alias ) )
         {
            field.value = (*cit)->value ;
            if ( needCopyAlias && field.alias.empty() )
            {
               field.alias = (*cit)->alias ;
            }
            break ;
         }
         ++cit ;
      }

      return SDB_OK ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QDMDOWNFIELDSBYFIELDALIAS, "downFieldsByFieldAlias" )
   INT32 downFieldsByFieldAlias( qgmOPFieldVec & fields,
                                 const qgmOPFieldPtrVec & fieldAlias,
                                 BOOLEAN needCopyAlias )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__QDMDOWNFIELDSBYFIELDALIAS ) ;
      qgmOPFieldVec::iterator it = fields.begin() ;
      while ( it != fields.end() )
      {
         if ( SQL_GRAMMAR::WILDCARD != (*it).type )
         {
            downAFieldByFieldAlias( *it, fieldAlias, needCopyAlias ) ;
         }
         ++it ;
      }

      PD_TRACE_EXITRC( SDB__QDMDOWNFIELDSBYFIELDALIAS, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMDOWNATTRSBYFIELDALIAS, "downAttrsByFieldAlias" )
   INT32 downAttrsByFieldAlias( qgmDbAttrPtrVec & attrs,
                                const qgmOPFieldPtrVec & fieldAlias )
   {
      PD_TRACE_ENTRY( SDB__QGMDOWNATTRSBYFIELDALIAS ) ;
      INT32 rc = SDB_OK ;
      qgmDbAttrPtrVec::iterator it = attrs.begin() ;
      while ( it != attrs.end() )
      {
         qgmDbAttr &attr = *(*it) ;

         qgmOPFieldPtrVec::const_iterator cit = fieldAlias.begin() ;
         while ( cit != fieldAlias.end() )
         {
            if ( ( (*cit)->alias.empty() && attr.attr() == (*cit)->value.attr() )
                 || ( !(*cit)->alias.empty() && attr.attr() == (*cit)->alias ) )
            {
               attr = (*cit)->value ;
               break ;
            }
            ++cit ;
         }
         ++it ;
      }

      PD_TRACE_EXITRC( SDB__QGMDOWNATTRSBYFIELDALIAS, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMDOWNATTRSBYFIELDALIAS2, "downAttrsByFieldAlias" )
   INT32 downAttrsByFieldAlias( qgmDbAttrVec & attrs,
                                const qgmOPFieldPtrVec & fieldAlias )
   {
      PD_TRACE_ENTRY( SDB__QGMDOWNATTRSBYFIELDALIAS2 ) ;
      INT32 rc = SDB_OK ;
      qgmDbAttrVec::iterator it = attrs.begin() ;
      while ( it != attrs.end() )
      {
         qgmDbAttr &attr = *it ;

         qgmOPFieldPtrVec::const_iterator cit = fieldAlias.begin() ;
         while ( cit != fieldAlias.end() )
         {
            if ( ( (*cit)->alias.empty() && attr.attr() == (*cit)->value.attr() )
                 || ( !(*cit)->alias.empty() && attr.attr() == (*cit)->alias ) )
            {
               attr = (*cit)->value ;
               break ;
            }
            ++cit ;
         }
         ++it ;
      }

      PD_TRACE_EXITRC( SDB__QGMDOWNATTRSBYFIELDALIAS2, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMDOWNAATTRBYFIELDALIAS, "downAAttrByFieldAlias" )
   INT32 downAAttrByFieldAlias( qgmDbAttr & attr,
                                 const qgmOPFieldPtrVec & fieldAlias)
   {
      PD_TRACE_ENTRY( SDB__QGMDOWNAATTRBYFIELDALIAS ) ;
      INT32 rc = SDB_OK ;
      qgmOPFieldPtrVec::const_iterator cit = fieldAlias.begin() ;
      while ( cit != fieldAlias.end() )
      {
         if ( ( (*cit)->alias.empty() && attr.attr() == (*cit)->value.attr() )
              || ( !(*cit)->alias.empty() && attr.attr() == (*cit)->alias ) )
         {
            attr = (*cit)->value ;
            break ;
         }
         ++cit ;
      }
      PD_TRACE_EXITRC( SDB__QGMDOWNAATTRBYFIELDALIAS, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMDOWNAGGRSBYFIELDALIAS, "downAggrsByFieldAlias" )
   INT32 downAggrsByFieldAlias( qgmAggrSelectorVec & aggrs,
                                const qgmOPFieldPtrVec & fieldAlias )
   {
      PD_TRACE_ENTRY( SDB__QGMDOWNAGGRSBYFIELDALIAS ) ;
      INT32 rc = SDB_OK ;
      qgmAggrSelectorVec::iterator it = aggrs.begin() ;
      while ( it != aggrs.end() )
      {
         qgmAggrSelector &selector= *it ;

         if ( SQL_GRAMMAR::FUNC == selector.value.type )
         {
            vector<qgmOpField>::iterator iter = selector.param.begin();
            while ( iter != selector.param.end() )
            {
               downAAttrByFieldAlias( iter->value, fieldAlias );
               ++iter;
            }
         }
         else if ( SQL_GRAMMAR::WILDCARD != selector.value.type )
         {
            downAFieldByFieldAlias( selector.value, fieldAlias, TRUE ) ;
         }
         ++it ;
      }

      PD_TRACE_EXITRC( SDB__QGMDOWNAGGRSBYFIELDALIAS, rc ) ;
      return rc ;
   }

   static INT32 upAFieldByFieldAlias( qgmOpField &field,
                                      const qgmOPFieldPtrVec & fieldAlias,
                                      BOOLEAN needClearAlias )
   {
      if ( needClearAlias && field.alias.empty() )
      {
         return SDB_OK ;
      }

      qgmOPFieldPtrVec::const_iterator cit = fieldAlias.begin() ;
      while ( cit != fieldAlias.end() )
      {
         if ( ( field.alias.empty() && field.value == (*cit)->value ) ||
              ( !field.alias.empty() && field == *(*cit) ) )
         {
            if ( (*cit)->alias.empty() )
            {
               field.value.relegation().clear() ;
            }
            else
            {
               field.value.attr() = (*cit)->alias ;
            }

            if ( needClearAlias && field.value.attr() == field.alias )
            {
               field.alias.clear() ;
            }
            break ;
         }

         ++cit ;
      }

      return SDB_OK ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMUPFIELDSBYFIELDALIAS, "upFieldsByFieldAlias" )
   INT32 upFieldsByFieldAlias( qgmOPFieldVec & fields,
                               const qgmOPFieldPtrVec & fieldAlias,
                               BOOLEAN needClearAlias )
   {
      PD_TRACE_ENTRY( SDB__QGMUPFIELDSBYFIELDALIAS ) ;
      INT32 rc = SDB_OK ;
      qgmOPFieldVec::iterator it = fields.begin() ;
      while ( it != fields.end() )
      {
         if ( SQL_GRAMMAR::WILDCARD != (*it).type )
         {
            upAFieldByFieldAlias( *it, fieldAlias, needClearAlias ) ;
         }
         ++it ;
      }

      PD_TRACE_EXITRC( SDB__QGMUPFIELDSBYFIELDALIAS, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMUPATTRSBYFIELDALIAS, "upAttrsByFieldAlias" )
   INT32 upAttrsByFieldAlias( qgmDbAttrPtrVec & attrs,
                              const qgmOPFieldPtrVec & fieldAlias )
   {
      PD_TRACE_ENTRY( SDB__QGMUPATTRSBYFIELDALIAS ) ;
      INT32 rc = SDB_OK ;
      qgmDbAttrPtrVec::iterator it = attrs.begin() ;
      while ( it != attrs.end() )
      {
         qgmDbAttr &attr = *(*it) ;

         qgmOPFieldPtrVec::const_iterator cit = fieldAlias.begin() ;
         while ( cit != fieldAlias.end() )
         {
            if ( attr == (*cit)->value )
            {
               if ( (*cit)->alias.empty() )
               {
                  attr.relegation().clear() ;
               }
               else
               {
                  attr.attr() = (*cit)->alias ;
               }
               break ;
            }
            ++cit ;
         }
         ++it ;
      }

      PD_TRACE_EXITRC( SDB__QGMUPATTRSBYFIELDALIAS, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMUPATTRSBYFIELDALIAS2, "upAttrsByFieldAlias2" )
   INT32 upAttrsByFieldAlias( qgmDbAttrVec & attrs,
                              const qgmOPFieldPtrVec & fieldAlias )
   {
      PD_TRACE_ENTRY( SDB__QGMUPATTRSBYFIELDALIAS2 ) ;
      INT32 rc = SDB_OK ;
      qgmDbAttrVec::iterator it = attrs.begin() ;
      while ( it != attrs.end() )
      {
         qgmDbAttr &attr = *it ;

         qgmOPFieldPtrVec::const_iterator cit = fieldAlias.begin() ;
         while ( cit != fieldAlias.end() )
         {
            if ( attr == (*cit)->value )
            {
               if ( (*cit)->alias.empty() )
               {
                  attr.relegation().clear() ;
               }
               else
               {
                  attr.attr() = (*cit)->alias ;
               }
               break ;
            }
            ++cit ;
         }
         ++it ;
      }

      PD_TRACE_EXITRC( SDB__QGMUPATTRSBYFIELDALIAS2, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMUPAATTRBYFIELDALIAS, "upAAttrByFieldAlias" )
   INT32 upAAttrByFieldAlias( qgmDbAttr & attr,
                              const qgmOPFieldPtrVec & fieldAlias )
   {
      PD_TRACE_ENTRY( SDB__QGMUPAATTRBYFIELDALIAS ) ;
      INT32 rc = SDB_OK ;
      qgmOPFieldPtrVec::const_iterator cit = fieldAlias.begin() ;
      while ( cit != fieldAlias.end() )
      {
         if ( attr == (*cit)->value )
         {
            if ( (*cit)->alias.empty() )
            {
               attr.relegation().clear() ;
            }
            else
            {
               attr.attr() = (*cit)->alias ;
            }
            break ;
         }
         ++cit ;
      }

      PD_TRACE_EXITRC( SDB__QGMUPAATTRBYFIELDALIAS, rc ) ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMUPATTRSBYFIELDALIAS3, "upAggrsByFieldAlias3" )
   INT32 upAggrsByFieldAlias( qgmAggrSelectorVec & aggrs,
                              const qgmOPFieldPtrVec & fieldAlias )
   {
      PD_TRACE_ENTRY( SDB__QGMUPATTRSBYFIELDALIAS3 ) ;
      INT32 rc = SDB_OK ;
      qgmAggrSelectorVec::iterator it = aggrs.begin() ;
      while ( it != aggrs.end() )
      {
         qgmAggrSelector &selector = *it ;

         if ( SQL_GRAMMAR::FUNC == selector.value.type )
         {
            vector<qgmOpField>::iterator iter = selector.param.begin();
            while ( iter != selector.param.end() )
            {
               upAAttrByFieldAlias( iter->value, fieldAlias );
               ++iter;
            }
         }
         else if ( SQL_GRAMMAR::WILDCARD != selector.value.type )
         {
            upAFieldByFieldAlias( selector.value, fieldAlias, TRUE ) ;
         }

         ++it ;
      }

      PD_TRACE_EXITRC( SDB__QGMUPATTRSBYFIELDALIAS3, rc ) ;
      return rc ;
   }

}

