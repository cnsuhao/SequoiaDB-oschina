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

   Source File Name = ossMmap.hpp

   Descriptive Name = Operating System Services Memory Map Header

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains structure of Memory Map File.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef OSSMMAP_HPP_
#define OSSMMAP_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossIO.hpp"
#include "ossLatch.hpp"
#include "ossUtil.hpp"
#include <vector>

using namespace std ;

class _ossMmapFile : public SDBObject
{
protected:
   class _ossMmapSegment : public SDBObject
   {
   public :
      ossValuePtr _ptr ;
      UINT32      _length ;
      UINT64      _offset ;
#if defined (_WINDOWS)
      HANDLE _maphandle ;
#endif
      _ossMmapSegment ( ossValuePtr ptr, UINT32 length, UINT64 offset )
      {
         _ptr = ptr ;
         _length = length ;
         _offset = offset ;
#if defined (_WINDOWS)
         _maphandle = INVALID_HANDLE_VALUE ;
#endif
      }
   } ;
   typedef _ossMmapSegment ossMmapSegment ;

   ossSpinSLatch _mutex ;
#define OSSMMAP_SLOCK ossScopedLock _mmaplock( &_mutex, SHARED ) ;
#define OSSMMAP_XLOCK ossScopedLock _mmaplock( &_mutex, EXCLUSIVE ) ;

   OSSFILE  _file ;
   BOOLEAN  _opened ;
   vector < ossMmapSegment > _segments ;
   CHAR     _fileName[ OSS_MAX_PATHSIZE + 1 ] ;

public:
   typedef vector<ossMmapSegment>::const_iterator CONST_ITR;

   OSS_INLINE CONST_ITR begin()
   {
      return _segments.begin();
   }

   OSS_INLINE CONST_ITR end()
   {
      return _segments.end();
   }

   OSS_INLINE UINT32 segmentSize()
   {
      return _segments.size();
   }

public:
   _ossMmapFile ()
   {
      _opened = FALSE ;
      ossMemset ( _fileName, 0, sizeof(_fileName) ) ;
   }
   ~_ossMmapFile ()
   {
      OSSMMAP_XLOCK
      if ( _opened )
      {
         ossClose ( _file ) ;
         _opened = FALSE ;
      }
   }
   INT32 open ( const CHAR *pFilename,
                UINT32 iMode = OSS_READWRITE|OSS_EXCLUSIVE|OSS_CREATE,
                UINT32 iPermission = OSS_RU|OSS_WU|OSS_RG ) ;
   void  close () ;
   INT32 map ( UINT64 offset, UINT32 length, void **pAddress ) ;
   INT32 flushAll ( BOOLEAN sync = FALSE ) ;
   INT32 flush ( UINT32 segmentID, BOOLEAN sync = FALSE ) ;
   INT32 unlink () ;
   INT32 size ( UINT64 &fileSize ) ;

} ;
typedef class _ossMmapFile ossMmapFile ;
#endif
