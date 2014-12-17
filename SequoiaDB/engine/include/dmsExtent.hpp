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

   Source File Name = dmsExtent.hpp

   Descriptive Name = Data Management Service Extent Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   data/index extent metadata.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef DMSEXTENT_HPP_
#define DMSEXTENT_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "dms.hpp"
#include "dmsRecord.hpp"
#include "ossUtil.hpp"
#include "../bson/bson.h"
#include "../bson/bsonobj.h"

using namespace bson ;

namespace engine
{

   /*
      Eyecatcher define
   */
   #define DMS_EXTENT_EYECATCHER0         'D'
   #define DMS_EXTENT_EYECATCHER1         'E'
   /*
      Flag define
   */
   #define DMS_EXTENT_FLAG_INUSE          0x01
   #define DMS_EXTENT_FLAG_FREED          0x02
   /*
      Version define
   */
   #define DMS_EXTENT_CURRENT_V           1

   /*
      _dmsExtent define
   */
   struct _dmsExtent : public SDBObject
   {
      CHAR        _eyeCatcher [2] ;
      UINT16      _blockSize ;   // num of pages, i.e. 4k to 128MB
      UINT16      _mbID ;        // 1 to 4096
      CHAR        _flag ;
      CHAR        _version ;
      dmsExtentID _logicID ;
      dmsExtentID _prevExtent ;
      dmsExtentID _nextExtent ;
      UINT32      _recCount ;
      dmsOffset   _firstRecordOffset ;
      dmsOffset   _lastRecordOffset ;
      INT32       _freeSpace ;

      void init( UINT16 numPages, UINT16 mbID, UINT32 totalSize )
      {
         _eyeCatcher[0]       = DMS_EXTENT_EYECATCHER0 ;
         _eyeCatcher[1]       = DMS_EXTENT_EYECATCHER1 ;
         _blockSize           = numPages ;
         _mbID                = mbID ;
         _flag                = DMS_EXTENT_FLAG_INUSE ;
         _version             = DMS_EXTENT_CURRENT_V ;
         _logicID             = DMS_INVALID_EXTENT ;
         _prevExtent          = DMS_INVALID_EXTENT ;
         _nextExtent          = DMS_INVALID_EXTENT ;
         _recCount            = 0 ;
         _firstRecordOffset   = DMS_INVALID_OFFSET ;
         _lastRecordOffset    = DMS_INVALID_OFFSET ;
         _freeSpace           = (INT32)( totalSize - sizeof(_dmsExtent) ) ;
      }
      BOOLEAN validate( UINT16 mbID = DMS_INVALID_MBID )
      {
         if ( DMS_EXTENT_EYECATCHER0 != _eyeCatcher[0] ||
              DMS_EXTENT_EYECATCHER1 != _eyeCatcher[1] ||
              DMS_EXTENT_FLAG_INUSE  != _flag )
         {
            return FALSE ;
         }
         else if ( DMS_INVALID_MBID != mbID && _mbID != mbID )
         {
            return FALSE ;
         }
         return TRUE ;
      }
   } ;
   typedef struct _dmsExtent           dmsExtent ;
   #define DMS_EXTENT_METADATA_SZ      sizeof(dmsExtent)


   /*
      Eyecatcher define
   */
   #define DMS_META_EXTENT_EYECATCHER0    'M'
   #define DMS_META_EXTENT_EYECATCHER1    'E'
   /*
      Version define
   */
   #define DMS_META_EXTENT_CURRENT_V      1

   /*
      _dmsMetaExtent define
   */
   struct _dmsMetaExtent : public SDBObject
   {
      CHAR        _eyeCatcher [2] ;
      UINT16      _blockSize ;   // num of pages, i.e. 4k to 128MB
      UINT16      _mbID ;        // 1 to 4096
      CHAR        _flag ;
      CHAR        _version ;
      UINT32      _segNum ;
      UINT32      _usedSegNum ;

      void init( UINT16 numPages, UINT16 mbID, UINT32 segNum )
      {
         _eyeCatcher[0]       = DMS_META_EXTENT_EYECATCHER0 ;
         _eyeCatcher[1]       = DMS_META_EXTENT_EYECATCHER1 ;
         _blockSize           = numPages ;
         _mbID                = mbID ;
         _flag                = DMS_EXTENT_FLAG_INUSE ;
         _version             = DMS_META_EXTENT_CURRENT_V ;
         _segNum              = segNum ;

         _usedSegNum          = 0 ;

         dmsExtentID *pArray = ( dmsExtentID* )( (CHAR*)this +
                                                 sizeof( _dmsMetaExtent ) ) ;
         for ( UINT32 i = 0 ; i < _segNum ; ++i )
         {
            pArray[i<<1]      = DMS_INVALID_EXTENT ;  // first
            pArray[(i<<1)+1]  = DMS_INVALID_EXTENT ;  // last
         }
      }
      void reset()
      {
         _usedSegNum          = 0 ;
         dmsExtentID *pArray = ( dmsExtentID* )( (CHAR*)this +
                                                 sizeof( _dmsMetaExtent ) ) ;
         for ( UINT32 i = 0 ; i < _segNum ; ++i )
         {
            pArray[i<<1]      = DMS_INVALID_EXTENT ;  // first
            pArray[(i<<1)+1]  = DMS_INVALID_EXTENT ;  // last
         }
      }
      BOOLEAN validate( UINT16 mbID = DMS_INVALID_MBID )
      {
         if ( DMS_META_EXTENT_EYECATCHER0 != _eyeCatcher[0] ||
              DMS_META_EXTENT_EYECATCHER1 != _eyeCatcher[1] ||
              DMS_EXTENT_FLAG_INUSE  != _flag )
         {
            return FALSE ;
         }
         else if ( DMS_INVALID_MBID != mbID && _mbID != mbID )
         {
            return FALSE ;
         }
         return TRUE ;
      }
   } ;
   typedef _dmsMetaExtent dmsMetaExtent ;
   #define DMS_METAEXTENT_HEADER_SZ    sizeof(dmsMetaExtent)

}

#endif //DMSEXTENT_HPP_

