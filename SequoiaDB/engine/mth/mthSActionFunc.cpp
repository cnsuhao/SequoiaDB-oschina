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
#include "mthSliceIterator.hpp"
#include "mthElemMatchIterator.hpp"

using namespace bson ;

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
      SDB_ASSERT( NULL != action, "can not be null" ) ;
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
      SDB_ASSERT( NULL != action, "can not be null" ) ;
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
      SDB_ASSERT( NULL != action, "can not be null" ) ;
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
      SDB_ASSERT( NULL != action, "can not be null" ) ;
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

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSLICEBUILD, "mthSliceBuild" )
   INT32 mthSliceBuild( const CHAR *fieldName,
                        const bson::BSONElement &e,
                        _mthSAction *action,
                        bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSLICEBUILD ) ;
      SDB_ASSERT( NULL != action, "can not be null" ) ;
      INT32 begin = 0 ;
      INT32 limit = -1 ;

      if ( e.eoo() )
      {
         goto done ;
      }
      else if ( Array == e.type() )
      {
         action->getSlicePair( begin, limit ) ;
         _mthSliceIterator i( e.embeddedObject(),
                              begin, limit ) ;
         BSONArrayBuilder sliceBuilder( builder.subarrayStart( fieldName ) ) ;
         while ( i.more() )
         {
            sliceBuilder.append( i.next() ) ;
         }
         sliceBuilder.doneFast() ;
      }
      else
      {
         builder.append( e ) ;
      }
   done:
      PD_TRACE_EXITRC( SDB__MTHSLICEBUILD, rc ) ;
      return rc ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSLICEGET, "mthSliceGet" )
   INT32 mthSliceGet( const CHAR *fieldName,
                      const bson::BSONElement &in,
                      _mthSAction *action,
                      bson::BSONElement &out )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSLICEGET ) ;
      SDB_ASSERT( NULL != action, "can not be null" ) ;

      INT32 begin = 0 ;
      INT32 limit = -1 ;

      if ( Array != in.type() )
      {
         out = in ;
         goto done ; 
      }
      else if ( Array == in.type() )
      {
         BSONObjBuilder subBuilder ;
         action->getSlicePair( begin, limit ) ;
         _mthSliceIterator i( in.embeddedObject(),
                              begin, limit ) ;
         BSONArrayBuilder sliceBuilder( subBuilder.subarrayStart( fieldName ) ) ;
         while ( i.more() )
         {
            sliceBuilder.append( i.next() ) ;
         }
         sliceBuilder.doneFast() ;
         action->setObj( subBuilder.obj() ) ;
         out = action->getObj().getField( fieldName ) ;
      }
   done:
      PD_TRACE_EXITRC( SDB__MTHSLICEGET, rc ) ;
      return rc ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHELEMMATCHBUILDN, "mthElemMatchBuildN" )
   static INT32 mthElemMatchBuildN( const CHAR *fieldName,
                                    const bson::BSONElement &e,
                                    _mthSAction *action,
                                    bson::BSONObjBuilder &builder,
                                    INT32 n )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHELEMMATCHBUILDN ) ;
      if ( Array == e.type() )
      {
         BSONArrayBuilder arrayBuilder( builder.subarrayStart( fieldName ) ) ;
         _mthElemMatchIterator i( e.embeddedObject(),
                                  &( action->getMatcher() ),
                                  n ) ;
         do
         {
            BSONElement next ;
            rc = i.next( next ) ;
            if ( SDB_OK == rc )
            {
               arrayBuilder.append( next ) ;    
            }
            else if ( SDB_DMS_EOC == rc )
            {
               arrayBuilder.doneFast() ;
               rc = SDB_OK ;
               break ;
            }
            else
            {
               PD_LOG( PDERROR, "failed to get next element:%d", rc ) ;
               goto error ;
            }
         } while ( TRUE ) ;
      }
   done:
      PD_TRACE_EXITRC( SDB__MTHELEMMATCHBUILDN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHELEMMATCHGETN, "mthElemMatchGetN" )
   static INT32 mthElemMatchGetN( const CHAR *fieldName,
                                  const bson::BSONElement &in,
                                  _mthSAction *action,
                                  bson::BSONElement &out,
                                  INT32 n )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHELEMMATCHGETN ) ;
      if ( Array == in.type() )
      {
         BSONObjBuilder objBuilder ;
         BSONArrayBuilder arrayBuilder( objBuilder.subarrayStart( fieldName ) ) ;
         _mthElemMatchIterator i( in.embeddedObject(),
                                  &( action->getMatcher() ),
                                  n ) ;
         do
         {
            BSONElement next ;
            rc = i.next( next ) ;
            if ( SDB_OK == rc )
            {
               arrayBuilder.append( next ) ;
            }
            else if ( SDB_DMS_EOC == rc )
            {
               arrayBuilder.doneFast() ;
               rc = SDB_OK ;
               break ;
            }
            else
            {
               PD_LOG( PDERROR, "failed to get next element:%d", rc ) ;
               goto error ;
            }
         } while ( TRUE ) ;

         action->setObj( objBuilder.obj() ) ;
         out = action->getObj().getField( fieldName ) ;
      }
      else
      {
         out = BSONElement() ;
      }
   done:
      PD_TRACE_EXITRC( SDB__MTHELEMMATCHGETN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHELEMMATCHBUILD, "mthElemMatchBuild" )
   INT32 mthElemMatchBuild( const CHAR *fieldName,
                            const bson::BSONElement &e,
                            _mthSAction *action,
                            bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHELEMMATCHBUILD ) ;
      rc = mthElemMatchBuildN( fieldName, e, action, builder, -1 ) ;
      PD_TRACE_EXITRC( SDB__MTHELEMMATCHBUILD, rc ) ;
      return rc ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHELEMMATCHGET, "mthElemMatchGet" )
   INT32 mthElemMatchGet( const CHAR *fieldName,
                          const bson::BSONElement &in,
                          _mthSAction *action,
                          bson::BSONElement &out )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHELEMMATCHGET ) ;
      rc = mthElemMatchGetN( fieldName, in, action, out, -1 ) ;
      PD_TRACE_EXITRC( SDB__MTHELEMMATCHGET, rc ) ;
      return rc ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHELEMMATCHONEBUILD, "mthElemMatchOneBuild" )
   INT32 mthElemMatchOneBuild( const CHAR *fieldName,
                               const bson::BSONElement &e,
                               _mthSAction *action,
                               bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHELEMMATCHONEBUILD ) ;
      rc = mthElemMatchBuildN( fieldName, e, action, builder, 1 ) ;
      PD_TRACE_EXITRC( SDB__MTHELEMMATCHONEBUILD, rc ) ;
      return rc ;
   }

   ///PD_TRACE_DECLARE_FUNCTION ( SDB__MTHELEMMATCHONEGET, "mthElemMatchOneGet" )
   INT32 mthElemMatchOneGet( const CHAR *fieldName,
                             const bson::BSONElement &in,
                             _mthSAction *action,
                             bson::BSONElement &out )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHELEMMATCHONEGET ) ;
      rc = mthElemMatchGetN( fieldName, in, action, out, 1 ) ;
      PD_TRACE_EXITRC( SDB__MTHELEMMATCHONEGET, rc ) ;
      return rc ;
   }
}

