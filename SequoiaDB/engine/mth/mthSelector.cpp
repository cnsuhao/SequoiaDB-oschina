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

   Source File Name = mthSelector.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "mthSelector.hpp"
#include "pd.hpp"
#include "mthTrace.hpp"
#include "pdTrace.hpp"
#include "mthCommon.hpp"
#include "../util/rawbson2json.h"

using namespace bson ;

#define FIRST_ELEMENT_STARTING_POS     10
#define MAX_SELECTOR_BUFFER_THRESHOLD  67108864 // 64MB

namespace engine
{
   _mthSelector::_mthSelector()
   :_init( FALSE ),
    _stringOutput( FALSE ),
    _stringOutputBufferSize( 0 ),
    _stringOutputBuffer( NULL )
   {

   }

   _mthSelector::~_mthSelector()
   {
      SAFE_OSS_FREE( _stringOutputBuffer ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSELECTOR_LOADPATTERN, "_mthSelector::loadPattern" )
   INT32 _mthSelector::loadPattern( const bson::BSONObj &pattern )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSELECTOR_LOADPATTERN ) ;

      rc = _matrix.load( pattern ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to parse pattern:%d", rc ) ;
         goto error ;
      }

      _init = TRUE ;
   done:
      PD_TRACE_EXITRC( SDB__MTHSELECTOR_LOADPATTERN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSELECTOR_SELECT, "_mthSelector::select" )
   INT32 _mthSelector::select( const bson::BSONObj &source,
                               bson::BSONObj &target )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSELECTOR_SELECT ) ;
      SDB_ASSERT(_init, "The selector has not been initialized, please "
                         "call 'loadPattern' before using it" ) ;
      BSONObj obj ;
      if ( !_init )
      {
         target = source.copy() ;
         goto done ;
      }

      rc = _matrix.select( source, obj ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to select columns:%d", rc ) ;
         goto error ;
      }

      if ( !_stringOutput )
      {
         target = obj ;
      }
      else
      {
         rc = _buildCSV( obj, target ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build csv:%d", rc ) ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__MTHSELECTOR_SELECT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSELECTOR_MOVE, "_mthSelector::move" )
   INT32 _mthSelector::move( _mthSelector &other )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSELECTOR_MOVE ) ;
      if ( !_init )
      {
         PD_LOG( PDERROR, "selector has not been initalized yet" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      other.clear() ;

      rc = other.loadPattern( getPattern() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to load pattern:%d", rc ) ;
         goto error ;
      }

      clear() ;
   done:
      PD_TRACE_EXITRC( SDB__MTHSELECTOR_MOVE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSELECTOR_CLEAR, "_mthSelector::clear" )
   void _mthSelector::clear()
   {
      PD_TRACE_ENTRY( SDB__MTHSELECTOR_CLEAR ) ;
      _matrix.clear() ;
      _init = FALSE ;
      _stringOutput = FALSE ;
      PD_TRACE_EXIT( SDB__MTHSELECTOR_CLEAR ) ;
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHSELECTOR__BUILDCSV, "_mthSelector::_buildCSV" )
   INT32 _mthSelector::_buildCSV( const bson::BSONObj &obj,
                                  bson::BSONObj &csv )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__MTHSELECTOR__BUILDCSV ) ;
      BOOLEAN result = FALSE ;
      INT32 stringLength = 0 ;

      if ( 0 == _stringOutputBufferSize )
      {
         rc = mthDoubleBufferSize ( &_stringOutputBuffer,
                                     _stringOutputBufferSize ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to append string, rc = %d", rc ) ;
      }
      _stringOutputBuffer[FIRST_ELEMENT_STARTING_POS-6] = String ;
      _stringOutputBuffer[FIRST_ELEMENT_STARTING_POS-5] = '\0' ;

      while ( _stringOutputBufferSize < MAX_SELECTOR_BUFFER_THRESHOLD )
      {
         result = rawbson2csv ( obj.objdata(),
               &_stringOutputBuffer[FIRST_ELEMENT_STARTING_POS],
                _stringOutputBufferSize-FIRST_ELEMENT_STARTING_POS ) ;
         if ( result )
         {
            break ;
         }
         else
         {
            rc = mthDoubleBufferSize ( &_stringOutputBuffer,
                                       _stringOutputBufferSize ) ;
            PD_RC_CHECK ( rc, PDERROR,
                          "Failed to double buffer, rc = %d", rc ) ;
         }
      }

      if ( _stringOutputBufferSize >= MAX_SELECTOR_BUFFER_THRESHOLD )
      {
         PD_LOG ( PDERROR,
                  "string output buffer size is greater than threshold" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      stringLength =
               ossStrlen ( &_stringOutputBuffer[FIRST_ELEMENT_STARTING_POS] ) ;
      *(INT32*)_stringOutputBuffer = FIRST_ELEMENT_STARTING_POS + 2 +
                                        stringLength ;
      _stringOutputBuffer[ *(INT32*)_stringOutputBuffer -1 ] = EOO ;
      *(INT32*)(&_stringOutputBuffer[FIRST_ELEMENT_STARTING_POS-4]) =
               stringLength + 1 ;
      csv.init ( _stringOutputBuffer ) ;
   done:
      PD_TRACE_EXITRC( SDB__MTHSELECTOR__BUILDCSV, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

