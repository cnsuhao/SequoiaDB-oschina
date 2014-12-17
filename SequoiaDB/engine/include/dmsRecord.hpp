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

   Source File Name = dmsRecord.hpp

   Descriptive Name = Data Management Service Record Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   data record, including normal record and deleted record.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef DMSRECORD_HPP_
#define DMSRECORD_HPP_

#include "dms.hpp"
#include "oss.hpp"

namespace engine
{

   #define DMS_RECORD_MAX_SZ           0x01000000


   #define DMS_RECORD_USER_MAX_SZ      (DMS_RECORD_MAX_SZ-4096)

   /*
      _dmsRecord defined
   */
   class _dmsRecord : public SDBObject
   {
   public:
      union
      {
         CHAR     _recordHead[4] ;     // 1 byte flag, 3 bytes length - 1
         UINT32   _flag_and_size ;
      }                 _head ;
      dmsOffset         _myOffset ;
      dmsOffset         _previousOffset ;
      dmsOffset         _nextOffset ;
   } ;
   typedef _dmsRecord dmsRecord ;
   #define DMS_RECORD_METADATA_SZ   sizeof(dmsRecord)

   /*
      Record Flag define:
   */
   #define DMS_RECORD_FLAG_NORMAL            0x00
   #define DMS_RECORD_FLAG_OVERFLOWF         0x01
   #define DMS_RECORD_FLAG_OVERFLOWT         0x02
   #define DMS_RECORD_FLAG_DELETED           0x04
   #define DMS_RECORD_FLAG_COMPRESSED        0x10
   #define DMS_RECORD_FLAG_DELETING          0x80

   #define DMS_RECORD_GETFLAG(record)        (*((CHAR*)(record)))
   #define DMS_RECORD_GETSTATE(record)       (*((CHAR*)(record))&0x0F)
   #define DMS_RECORD_GETATTR(record)        (*((CHAR*)(record))&0xF0)

#if defined (SDB_BIG_ENDIAN)
   #define DMS_RECORD_GETSIZE(record)  (((*((UINT32*)(record)))&0x00FFFFFF)+1)
#else
   #define DMS_RECORD_GETSIZE(record)  (((*((UINT32*)(record)))>>8)+1)
#endif

   #define DMS_RECORD_GETMYOFFSET(record)    \
      (((dmsRecord*)(record))->_myOffset)
   #define DMS_RECORD_GETPREVOFFSET(record)  \
      (((dmsRecord*)(record))->_previousOffset)
   #define DMS_RECORD_GETNEXTOFFSET(record)  \
      (((dmsRecord*)(record))->_nextOffset)

   #define DMS_RECORD_GETDATA(record)                                         \
      ( OSS_BIT_TEST(DMS_RECORD_GETATTR(record),DMS_RECORD_FLAG_COMPRESSED) ? \
       (CHAR*)((CHAR*)(record)+sizeof(INT32)+DMS_RECORD_METADATA_SZ) :        \
       (CHAR*)((CHAR*)(record)+DMS_RECORD_METADATA_SZ) )

   #define DMS_RECORD_GETOVF(record)         \
      *(dmsRecordID*)((char*)(record)+DMS_RECORD_METADATA_SZ)

   #define DMS_RECORD_GETDATALEN(record)     \
      *(INT32*)((CHAR*)(record)+DMS_RECORD_METADATA_SZ)

   #define DMS_RECORD_EXTRACTDATA(recordPtr,retPtr)                        \
      do {                                                                 \
            if ( OSS_BIT_TEST ( DMS_RECORD_GETATTR(recordPtr),             \
                                DMS_RECORD_FLAG_COMPRESSED ) )             \
            {                                                              \
               INT32 uncompLen = 0 ;                                       \
               rc = dmsUncompress( cb, DMS_RECORD_GETDATA(recordPtr),      \
                                   DMS_RECORD_GETDATALEN(recordPtr),       \
                                   (const CHAR**)&(retPtr), &uncompLen ) ; \
               PD_RC_CHECK ( rc, PDERROR,                                  \
                             "Failed to uncompress record, rc = %d", rc ); \
               PD_CHECK ( uncompLen == *(INT32*)(retPtr),                  \
                          SDB_CORRUPTED_RECORD, error, PDERROR,            \
                          "uncompressed length %d does not match real "    \
                          "len %d", uncompLen, *(INT32*)(retPtr) ) ;       \
            }                                                              \
            else                                                           \
            {                                                              \
               (retPtr) = (ossValuePtr)(DMS_RECORD_GETDATA(recordPtr)) ;   \
            }                                                              \
         } while ( FALSE )

   #define DMS_RECORD_SETFLAG(record,flag)   (*((CHAR*)(record))=(CHAR)(flag))
   #define DMS_RECORD_SETSTATE(record,state)  \
      (*((CHAR*)(record))=(CHAR)(state&0x0F)|(*((CHAR*)(record))&0xF0))
   #define DMS_RECORD_SETATTR(record,attr)   \
      (*((CHAR*)(record))=(CHAR)(attr&0xF0)|(*((CHAR*)(record))))
   #define DMS_RECORD_UNSETATTR(record,attr) \
      (*((CHAR*)(record))=(CHAR)(~(attr&0xF0))&(*((CHAR*)(record))))
   #define DMS_RECORD_RESETATTR(record)      DMS_RECORD_UNSETATTR(record,0xF0)

#if defined (SDB_BIG_ENDIAN)
   #define DMS_RECORD_SETSIZE(record,size)   \
      (*((UINT32*)(record))=(DMS_RECORD_GETFLAG(record)<<24)|\
      ((UINT32)((size)-1)&0x00FFFFFF))
#else
   #define DMS_RECORD_SETSIZE(record,size)      \
      (*((UINT32*)(record))=DMS_RECORD_GETFLAG(record)|\
      ((UINT32)((size)-1)<<8))
#endif

   #define DMS_RECORD_SETMYOFFSET(record,offset)   \
      (((dmsRecord*)(record))->_myOffset=(offset))
   #define DMS_RECORD_SETPREVOFFSET(record,offset) \
      (((dmsRecord*)(record))->_previousOffset=(offset))
   #define DMS_RECORD_SETNEXTOFFSET(record,offset) \
      (((dmsRecord*)(record))->_nextOffset=(offset))

   #define DMS_RECORD_SETDATA(record,ptr,len)                              \
     do {                                                                  \
        if ( OSS_BIT_TEST(DMS_RECORD_GETATTR(record),                      \
                          DMS_RECORD_FLAG_COMPRESSED) )                    \
        {                                                                  \
           *(INT32*)((CHAR*)(record)+DMS_RECORD_METADATA_SZ) = len ;       \
           ossMemcpy((void*)((CHAR*)(record)+DMS_RECORD_METADATA_SZ+       \
                      sizeof(INT32)), ((CHAR*)ptr),(len)) ;                \
        }                                                                  \
        else                                                               \
        {                                                                  \
           ossMemcpy((void*)((CHAR*)(record)+DMS_RECORD_METADATA_SZ),      \
                     ((CHAR*)ptr),(len)) ;                                 \
        }                                                                  \
     } while (FALSE)

   #define DMS_RECORD_SETDATA_OID(record,ptr,len,oid)                      \
      do {                                                                 \
           *(INT32*)((CHAR*)(record)+DMS_RECORD_METADATA_SZ) =             \
                     (len) + oid.size() ;                                  \
           ossMemcpy((void*)((CHAR*)(record)+DMS_RECORD_METADATA_SZ+       \
                     sizeof(INT32)), oid.rawdata(), oid.size()) ;          \
           ossMemcpy((void*)((CHAR*)(record)+DMS_RECORD_METADATA_SZ+       \
                     sizeof(INT32)+oid.size()),                            \
                     (CHAR*)((CHAR*)(ptr)+sizeof(INT32)),                  \
                     (len)-sizeof(INT32)) ;                                \
        } while ( FALSE )

   #define DMS_RECORD_SETOVF(record,rid)     \
      *((dmsRecordID*)((char*)(record)+DMS_RECORD_METADATA_SZ))=(rid)

   /*
      _dmsDeletedRecord defined
   */
   class _dmsDeletedRecord : public SDBObject
   {
   public :
      union
      {
         CHAR     _recordHead[4] ;     // 1 byte flag, 3 bytes length-1
         UINT32   _flag_and_size ;
      }                 _head ;
      dmsOffset         _myOffset ;
      dmsRecordID       _next ;
   } ;
   typedef _dmsDeletedRecord dmsDeletedRecord ;
   #define DMS_DELETEDRECORD_METADATA_SZ  sizeof(dmsDeletedRecord)

   #define DMS_DELETEDRECORD_GETFLAG(record)       (*((CHAR*)(record)))
   #define DMS_DELETEDRECORD_SETFLAG(record,flag)  \
      (*((CHAR*)(record))=(CHAR)(flag))

   #define DMS_DELETEDRECORD_GETSIZE(record)       DMS_RECORD_GETSIZE(record)
   #define DMS_DELETEDRECORD_SETSIZE(record,size)  \
      DMS_RECORD_SETSIZE(record,size)

   #define DMS_DELETEDRECORD_GETMYOFFSET(record)   \
      (((dmsDeletedRecord*)(record))->_myOffset)
   #define DMS_DELETEDRECORD_GETNEXTRID(record)    \
      (((dmsDeletedRecord*)(record))->_next)

   #define DMS_DELETEDRECORD_SETMYOFFSET(record,offset)  \
      (((dmsDeletedRecord*)(record))->_myOffset=(offset))
   #define DMS_DELETEDRECORD_SETNEXTRID(record,rid)      \
      (((dmsDeletedRecord*)(record))->_next=(rid))

   #define DMS_MIN_DELETEDRECORD_SZ    (DMS_DELETEDRECORD_METADATA_SZ+20)
   #define DMS_MIN_RECORD_SZ           DMS_MIN_DELETEDRECORD_SZ

}

#endif //DMSRECORD_HPP_

