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
   std::map<std::string, const _mthSActionParser::parser *> _mthSActionParser::_parsers ;
   INT32 _mthSActionParser::_initParsers = _mthSActionParser::_registerParsers() ;

   _mthSActionParser::_mthSActionParser()
   {

   }

   _mthSActionParser::~_mthSActionParser()
   {

   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSACTIONPARSER_GETACTION, "_mthSActionParser::getAction" )
   INT32 _mthSActionParser::parse( const bson::BSONElement &e,
                                   _mthSAction &action )
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
                                                     _mthSAction &action )
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

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSACTIONPARSER__REGISTERPARSERS, "_mthSActionParser::_registerParsers" )
   INT32 _mthSActionParser::_registerParsers()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSACTIONPARSER__REGISTERPARSERS ) ;


      parser *p = NULL ;

      MTH_ADD_PARSER( _mthIncludeParser ) ;

      MTH_ADD_PARSER( _mthDefaultParser ) ;
   done:
      PD_TRACE_EXITRC( SDB__MTHSACTIONPARSER__REGISTERPARSERS, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( p ) ;
      goto done ;
   }
}

