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

   Source File Name = qgmPlSplitBy.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains declare for QGM operators

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/09/2013  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "qgmPlSplitBy.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"
#include <sstream>

using namespace std ;

namespace engine
{
   _qgmPlSplitBy::_qgmPlSplitBy( const _qgmDbAttr &splitby,
                                 const _qgmField &alias )
   :_qgmPlan( QGM_PLAN_TYPE_SPLIT, alias ),
   _splitby(splitby),
   _itr(_fetch.obj),
   _fieldName(_splitby.attr().toString())
   {
      _initialized = TRUE ;
   }

   _qgmPlSplitBy::~_qgmPlSplitBy()
   {

   }

   INT32 _qgmPlSplitBy::_execute( _pmdEDUCB *eduCB )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( 1 == inputSize(), "impossible" ) ;
      rc = input( 0 )->execute( eduCB ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLSPLITBY__FETCHNEXT, "_qgmPlSplitBy::_fetchNext" )
   INT32 _qgmPlSplitBy::_fetchNext( qgmFetchOut &next )
   {
      PD_TRACE_ENTRY( SDB__QGMPLSPLITBY__FETCHNEXT ) ;
      INT32 rc = SDB_OK ;
      if ( _fetch.obj.isEmpty() )
      {
fetch:
         rc = input( 0 )->fetchNext( _fetch ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         SDB_ASSERT( NULL == _fetch.next, "impossible" ) ;
         {
         std::string fieldName = _splitby.attr().toString() ;
         BSONElement ele = _fetch.obj.getField( fieldName ) ;
         if ( Array != ele.type() )
         {
            PD_LOG( PDDEBUG, "element [%s] is not array", fieldName.c_str() ) ;
            if ( ele.eoo() )
            {
               BSONObjBuilder builder ;
               builder.appendElements( _fetch.obj ) ;
               builder.appendNull( fieldName ) ;
               next.obj = builder.obj() ;
               next.alias = _fetch.alias ;
            }
            else
            {
               next = _fetch ;
            }

            _clear() ;
            goto done ;
         }
         else
         {
            _itr = BSONObjIterator( ele.embeddedObject() ) ;
            if ( !_itr.more() )
            {
               PD_LOG( PDDEBUG, "element [%s] is a empty array",
                       ele.fieldName() ) ;
               BSONObjBuilder builder ;
               BSONObjIterator itr( _fetch.obj ) ;
               while ( itr.more() )
               {
                  BSONElement e = itr.next() ;
                  if ( 0 == ossStrcmp( ele.fieldName(),
                                       e.fieldName()) )
                  {
                     builder.appendNull( ele.fieldName()) ;
                  }
                  else
                  {
                     builder.append( e ) ;
                  }
               }
               next.obj = builder.obj() ;
               next.alias = _fetch.alias ;
               _clear() ;
               goto done ;
            }
         }
         }
      }

      SDB_ASSERT( NULL == _fetch.next, "impossible" ) ;
      if ( _itr.more() )
      {
         BSONObjBuilder builder ;
         BOOLEAN added = FALSE ;
         BSONElement splited = _itr.next() ;
         BSONObjIterator itr( _fetch.obj ) ;
         while ( itr.more() )
         {
            BSONElement other = itr.next() ;
            if ( !added &&
                 0 == _fieldName.compare(other.fieldName()))
            {
               builder.appendAs( splited, _fieldName ) ;
               added = TRUE ;
            }
            else
            {
               builder.append( other ) ;
            }
         }
         next.obj = builder.obj() ;
         next.alias = _fetch.alias ;
      }
      else
      {
         _clear() ;
         goto fetch ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMPLSPLITBY__FETCHNEXT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   void _qgmPlSplitBy::_clear()
   {
      _fetch.obj = BSONObj() ;
      _fetch.alias.clear() ;
   }

   string _qgmPlSplitBy::toString() const
   {
      stringstream ss ;
      ss << "split by [" << _splitby.toString() << "]" ;
      return ss.str();
   }
}
