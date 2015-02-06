/******************************************************************************

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

   Source File Name = mthSActionParser.cpp

   Descriptive Name = mth selector action parser

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "mthSActionParser.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "mthTrace.hpp"
#include "mthIncludeParser.hpp"
#include "mthDefaultParser.hpp"
#include "mthSliceParser.hpp"
#include "mthElemMatchParser.hpp"
#include "mthElemMatchOneParser.hpp"

#define MTH_ADD_PARSER( parser )\
        do                                                                                    \
        {                                                                                     \
           p = NULL ;                                                                         \
           p = SDB_OSS_NEW parser ;                                                           \
           if ( NULL == p )                                                                   \
           {                                                                                  \
              PD_LOG( PDERROR, "failed to allocate mem." ) ;                                  \
              rc = SDB_OOM ;                                                                  \
              goto error ;                                                                    \
           }                                                                                  \
           if ( !_parsers.insert( std::make_pair( p->getActionName(), p ) ).second )          \
           {                                                                                  \
              PD_LOG( PDERROR, "duplicate action:%s", p->getActionName().c_str() ) ;          \
              rc = SDB_SYS ;                                                                  \
              goto error ;                                                                    \
           }                                                                                  \
           p = NULL ;                                                                         \
        } while( FALSE )

namespace engine
{
   static _mthSActionParser PARSER ;
   _mthSActionParser::_mthSActionParser()
   {
      _registerParsers() ;
   }

   _mthSActionParser::~_mthSActionParser()
   {
      PARSERS::iterator itr = _parsers.begin() ;
      for ( ; itr != _parsers.end(); ++itr )
      {
         SAFE_OSS_DELETE( itr->second ) ;
      }
      _parsers.clear() ;
   }

   const _mthSActionParser *_mthSActionParser::instance()
   {
      return &PARSER ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSACTIONPARSER_GETACTION, "_mthSActionParser::getAction" )
   INT32 _mthSActionParser::parse( const bson::BSONElement &e,
                                   _mthSAction &action ) const
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSACTIONPARSER_GETACTION ) ;
      SDB_ASSERT( !e.eoo(), "can not be invalid" ) ;
      const CHAR *fieldName = e.fieldName() ;
      action.clear() ;

      if ( '$' != *fieldName )
      {
         goto done ;
      }

      {
      PARSERS::const_iterator itr = _parsers.find( fieldName ) ;
      if ( _parsers.end() == itr )
      {
         PD_LOG( PDERROR, "can not find the parser of action[%s]",
                 fieldName ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = itr->second->parse( e, action ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to parse action:%d", rc ) ;
         goto error ;
      } 
      }

   done:
      PD_TRACE_EXITRC( SDB__MTHSACTIONPARSER_GETACTION, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSACTIONPARSER_BUILDDEFAULTVALUEACTION, "_mthSActionParser::buildDefaultValueAction" )
   INT32 _mthSActionParser::buildDefaultValueAction( const bson::BSONElement &e,
                                                     _mthSAction &action ) const
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSACTIONPARSER_BUILDDEFAULTVALUEACTION ) ;
      PARSERS::const_iterator itr = _parsers.find( MTH_S_DEFAULT ) ;
      if ( _parsers.end() == itr )
      {
         PD_LOG( PDERROR, "can not find the parser of action[%s]",
                 MTH_S_DEFAULT ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = itr->second->parse( e, action ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to parse action:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__MTHSACTIONPARSER_BUILDDEFAULTVALUEACTION, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSACTIONPARSER__BUILDSLICEACTION, "_mthSActionParser::buildSliceAction" )
   INT32 _mthSActionParser::buildSliceAction( INT32 begin,
                                              INT32 limit,
                                              _mthSAction &action ) const
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSACTIONPARSER__BUILDSLICEACTION ) ;
      action.setAttribute( MTH_S_ATTR_PROJECTION ) ;
      action.setFunc( &mthSliceBuild,
                      &mthSliceGet ) ;
      action.setName( MTH_S_SLICE ) ;
      action.setSlicePair( begin, limit ) ;
      PD_TRACE_EXITRC( SDB__MTHSACTIONPARSER__BUILDSLICEACTION, rc ) ;
      return rc ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSACTIONPARSER__REGISTERPARSERS, "_mthSActionParser::_registerParsers" )
   INT32 _mthSActionParser::_registerParsers()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSACTIONPARSER__REGISTERPARSERS ) ;


      parser *p = NULL ;

      MTH_ADD_PARSER( _mthIncludeParser ) ;

      MTH_ADD_PARSER( _mthDefaultParser ) ;

      MTH_ADD_PARSER( _mthSliceParser ) ;

      MTH_ADD_PARSER( _mthElemMatchParser ) ;

      MTH_ADD_PARSER( _mthElemMatchOneParser ) ;
   done:
      PD_TRACE_EXITRC( SDB__MTHSACTIONPARSER__REGISTERPARSERS, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( p ) ;
      goto done ;
   }
}

