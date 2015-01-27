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

   Source File Name = mthSActionFunc.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "mthSActionFunc.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "mthTrace.hpp"
#include "mthSAction.hpp"

namespace engine
{
   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHINCLUDEBUILD, "mthIncludeBuild" )
   INT32 mthIncludeBuild( const CHAR *fieldName,
                          const bson::BSONElement &e,
                          _mthSAction *action,
                          bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHINCLUDEBUILD ) ;
      if ( !e.eoo() )
      {
         builder.append( e ) ;
      }
      PD_TRACE_EXITRC( SDB__MTHINCLUDEBUILD, rc ) ;
      return rc ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHINCLUDEGET, "mthIncludeGet" )
   INT32 mthIncludeGet( const CHAR *fieldName,
                        const bson::BSONElement &in,
                        _mthSAction *action,
                        bson::BSONElement &out )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHINCLUDEGET ) ;
      out = in ;
      PD_TRACE_EXITRC( SDB__MTHINCLUDEGET, rc ) ;
      return rc ;
   }
 
   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHDEFAULTBUILD, "mthDefaultBuild" )
   INT32 mthDefaultBuild( const CHAR *fieldName,
                          const bson::BSONElement &e,
                          _mthSAction *action,
                          bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHDEFAULTBUILD ) ;
      if ( e.eoo() )
      {
         builder.appendAs( action->getValue(), fieldName ) ;
      }
      else
      {
         builder.append( e ) ;
      }
      PD_TRACE_EXITRC( SDB__MTHDEFAULTBUILD, rc ) ;
      return rc ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHDEFAULTGET, "mthDefaultGet" )
   INT32 mthDefaultGet( const CHAR *fieldName,
                        const bson::BSONElement &in,
                        _mthSAction *action,
                        bson::BSONElement &out )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHDEFAULTGET ) ;
      if ( !in.eoo() )
      {
         out = in ;
         goto done ;
      }

      if ( action->getObj().isEmpty() )
      {
         bson::BSONObjBuilder builder ;
         builder.appendAs( action->getValue(), fieldName ) ;
         bson::BSONObj obj = builder.obj() ;
         action->setObj( builder.obj() ) ;
         action->setValue( obj.getField( fieldName ) ) ;
      }

      out = action->getValue() ;
   done:
      PD_TRACE_EXITRC( SDB__MTHDEFAULTGET, rc ) ;
      return rc ;
   }
}

