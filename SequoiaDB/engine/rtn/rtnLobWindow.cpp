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

   Source File Name = rtnLobWindow.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnLobWindow.hpp"
#include "rtnTrace.hpp"
#include "pdTrace.hpp"

namespace engine
{
const UINT32 RTN_MIN_READ_LEN = DMS_PAGE_SIZE512K ;
const UINT32 RTN_MAX_READ_LEN = DMS_PAGE_SIZE128K * 1024 ;
const UINT32 RTN_LOOP_WRITE_LEN = DMS_PAGE_SIZE512K * 4 ;

   _rtnLobWindow::_rtnLobWindow()
   :_pageSize( DMS_DO_NOT_CREATE_LOB ),
    _logarithmic( 0 ),
    _curOffset( 0 ),
    _pool( NULL ),
    _cachedSz( 0 ),
    _analysisCache( FALSE )
   {

   }

   _rtnLobWindow::~_rtnLobWindow()
   {
      if ( NULL != _pool )
      {
         SDB_OSS_FREE( _pool ) ;
         _pool = NULL ; 
      }
   }

   INT32 _rtnLobWindow::init( INT32 pageSize )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( DMS_DO_NOT_CREATE_LOB < pageSize,
                  "invalid arguments" ) ;
      SDB_ASSERT( _writeData.empty(), "impossible" ) ;

      if ( !ossIsPowerOf2( pageSize, &_logarithmic ) )
      {
         PD_LOG( PDERROR, "invalid page size:%d, it should be a power of 2",
                 pageSize ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _pool = ( CHAR * )SDB_OSS_MALLOC( pageSize ) ;
      if ( NULL == _pool )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      _pageSize = pageSize ;
   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBWINDOW_ADDOUTPUTDATA, "_rtnLobWindow::addOutputData" )
   INT32 _rtnLobWindow::prepare2Write( SINT64 offset, UINT32 len, const CHAR *data )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBWINDOW_ADDOUTPUTDATA ) ;
      SDB_ASSERT( 0 <= offset && NULL != data, "invalid arguments" ) ;
      SDB_ASSERT( _writeData.empty(), "the last write has not been done" ) ;
      
      if ( offset != _curOffset )
      {
         PD_LOG( PDERROR, "invalid offset:%lld, current offset:%lld"
                 ", we do not support seek write yet",
                 offset, _curOffset ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( 0 == _cachedSz )
      {
         _writeData.tuple.columns.offset = offset ;
         _writeData.tuple.columns.len = len ;
         _writeData.data = data ;
         _analysisCache = FALSE ;
      }
      else
      {
         SDB_ASSERT( _cachedSz < _pageSize, "impossible" ) ;
         INT32 mvSize = _pageSize - _cachedSz ;
         mvSize = ( UINT32 )mvSize <= len ? mvSize : len ;
         ossMemcpy( _pool + _cachedSz, data, mvSize ) ;
         _cachedSz += mvSize ;
         if ( 0 != len - mvSize )
         {
            _writeData.tuple.columns.offset = offset + mvSize ;
            _writeData.tuple.columns.len = len - mvSize ;
            _writeData.data = data + mvSize ;
         }

         _analysisCache = TRUE ;
      }
      _curOffset += len ;
   done:
      PD_TRACE_EXITRC( SDB_RTNLOBWINDOW_ADDOUTPUTDATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBWINDOW_GETNEXTWRITESEQUENCES, "_rtnLobWindow::getNextWriteSequences" )
   BOOLEAN _rtnLobWindow::getNextWriteSequences( RTN_LOB_TUPLES &tuples )
   {
      PD_TRACE_ENTRY( SDB_RTNLOBWINDOW_GETNEXTWRITESEQUENCES ) ;
      _rtnLobTuple tuple ;
      BOOLEAN hasNext = FALSE ;
      while ( _getNextWriteSequence( tuple ) )
      {
         tuples.push_back( tuple ) ;
         hasNext = TRUE ;
      }

      PD_TRACE_EXIT( SDB_RTNLOBWINDOW_GETNEXTWRITESEQUENCES ) ;
      return hasNext ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBWINDOW__GETNEXTWRITESEQUENCE, "_rtnLobWindow::_getNextWriteSequence" )
   BOOLEAN _rtnLobWindow::_getNextWriteSequence( _rtnLobTuple &tuple )
   {
      PD_TRACE_ENTRY( SDB_RTNLOBWINDOW__GETNEXTWRITESEQUENCE ) ;
      BOOLEAN hasNext = FALSE ;
      MsgLobTuple &t = tuple.tuple ;

      if ( !_analysisCache )
      {
         if ( (UINT32)_pageSize < _writeData.tuple.columns.len )
         {
            t.columns.len = _pageSize ;
            t.columns.sequence = RTN_LOB_GET_SEQUENCE(
                                         ( _writeData.tuple.columns.offset ),_logarithmic) ;
            t.columns.offset = 0 ;
            tuple.data = _writeData.data ;

            _writeData.tuple.columns.len -= _pageSize ;
            _writeData.tuple.columns.offset += _pageSize ;
            _writeData.data += _pageSize ;
            hasNext = TRUE ;
         }
         else if ( (UINT32)_pageSize == _writeData.tuple.columns.len )
         {
            t.columns.len = _pageSize ;
            t.columns.sequence = RTN_LOB_GET_SEQUENCE(
                                     ( _writeData.tuple.columns.offset ),_logarithmic) ;
            t.columns.offset = 0 ;
            tuple.data = _writeData.data ;

            hasNext = TRUE ;
            _writeData.clear() ;
         }
         else
         {
            goto done ;
         }
      }
      else if ( _pageSize == _cachedSz )
      {
         SINT64 offset = _writeData.empty() ?
                         _curOffset : _writeData.tuple.columns.offset ;
         t.columns.len = _cachedSz ;
         t.columns.sequence = RTN_LOB_GET_SEQUENCE(
                              ( offset - _cachedSz ),_logarithmic) ;
         t.columns.offset = 0 ;
         tuple.data = _pool ;

         hasNext = TRUE ;
         _analysisCache = FALSE ;
      }
      else
      {
         SDB_ASSERT( _writeData.empty(), "should be joined before" ) ;
      }
   done:
      PD_TRACE_EXIT( SDB_RTNLOBWINDOW__GETNEXTWRITESEQUENCE ) ;
      return hasNext ;
   }

   void _rtnLobWindow::cacheLastDataOrClearCache()
   {
      if ( _pageSize == _cachedSz )
      {
         _cachedSz = 0 ;
      }

      if ( !_writeData.empty() )
      {
         SDB_ASSERT( _writeData.tuple.columns.len < ( UINT32 )_pageSize, "impossible" ) ;
         SDB_ASSERT( 0 == _cachedSz || _cachedSz == _pageSize, "impossible" ) ;
         ossMemcpy( _pool, _writeData.data, _writeData.tuple.columns.len ) ;
         _cachedSz += _writeData.tuple.columns.len ;
         _writeData.clear() ;
      }

      _analysisCache = FALSE ;
      return ;
   }

   BOOLEAN _rtnLobWindow::getCachedData( _rtnLobTuple &tuple )
   {
      BOOLEAN hasNext = FALSE ;
      MsgLobTuple &t = tuple.tuple ;
      if ( 0 == _cachedSz )
      {
         goto done ;
      }

      t.columns.len = _cachedSz ;
      t.columns.sequence = RTN_LOB_GET_SEQUENCE(
                          _curOffset - _cachedSz, _logarithmic ) ;
      t.columns.offset = 0 ;
      tuple.data = _pool ;

      hasNext = TRUE ;
      _cachedSz = 0 ;
      _analysisCache = FALSE ; 
   done:
      return hasNext ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNLOBWINDOW_PREPARE2READ, "_rtnLobWindow::_rtnLobWindow::prepare2Read" )
   INT32 _rtnLobWindow::prepare2Read( SINT64 lobLen,
                                      SINT64 offset,
                                      UINT32 len,
                                      RTN_LOB_TUPLES &tuples )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNLOBWINDOW_PREPARE2READ ) ;
      SDB_ASSERT( offset < lobLen, "impossible" ) ;
      UINT32 totalRead = 0 ;
      SINT64 currentOffset = offset ;
      UINT32 maxLen = RTN_MAX_READ_LEN <= ( lobLen - offset ) ?
                      RTN_MAX_READ_LEN : ( lobLen - offset ) ;
      UINT32 needRead = len <= RTN_MIN_READ_LEN ?
                        RTN_MIN_READ_LEN : len ;
      tuples.clear() ;

      while ( currentOffset < lobLen &&
              totalRead < needRead &&
              totalRead < maxLen ) 
      {
         UINT32 offsetOfTuple =
                      RTN_LOB_GET_OFFSET_IN_SEQUENCE( currentOffset,
                                                      _pageSize ) ;
         UINT32 lenOfTuple = _pageSize - offsetOfTuple ;
         if ( ( lobLen - currentOffset ) < lenOfTuple )
         {
            lenOfTuple = lobLen - currentOffset ;
         }

         if ( 0 == lenOfTuple )
         {
            break ;
         }

         UINT32 sequence = RTN_LOB_GET_SEQUENCE( currentOffset,
                                                 _logarithmic ) ;

         currentOffset += lenOfTuple ;
         totalRead += lenOfTuple ;

         tuples.push_back( _rtnLobTuple( lenOfTuple,
                                         sequence,
                                         offsetOfTuple,
                                         NULL ) ) ;
      }

      PD_TRACE_EXITRC( SDB_RTNLOBWINDOW_PREPARE2READ, rc ) ;
      return rc ;
   }

   
}

