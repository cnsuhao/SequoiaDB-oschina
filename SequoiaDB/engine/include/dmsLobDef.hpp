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

   Source File Name = dmsLobDef.hpp

   Descriptive Name = Data Management Service Storage(Hash) Unit Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          17/07/2014  YW Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DMS_LOBDEF_HPP_
#define DMS_LOBDEF_HPP_

#include "dms.hpp"
#include "ossUtil.hpp"
#include "pd.hpp"
#include "../bson/bson.hpp"

namespace engine
{
   #define DMS_LOB_OID_LEN                   12 
   #define DMS_LOB_DATA_MAP_BLK_LEN          DMS_PAGE_SIZE256B
   #define DMS_LOB_INVALID_PAGEID            DMS_INVALID_EXTENT

   typedef SINT32 DMS_LOB_PAGEID ;

   #define DMS_LOB_CUR_VERSION               1
   #define DMS_LOB_META_SEQUENCE             0

   #define DMS_LOB_COMPLETE                  1
   #define DMS_LOB_UNCOMPLETE                0

   #define RTN_LOB_GET_SEQUENCE( offset, log ) \
     (( (offset) >> (log))+1)

   #define RTN_LOB_GET_OFFSET_IN_SEQUENCE( offset, pagesize ) \
     ((offset) & ((pagesize)-1))

   #define RTN_LOB_GET_SEQUENCE_NUM( len, pagesize, num )\
     do\
     {\
       if ( 0 == ((len) & ((pagesize)-1)) )\
       {\
          num = (len) / (pagesize) + 1;\
       }\
       else\
       {\
          num = (len) / (pagesize) + 2 ;\
       }\
     } while ( FALSE )

   #define RTN_LOB_GET_OFFSET_OF_LOB( pageSz, sequence, offsetInSeq )\
      ( ( (SINT64)( sequence ) - 1 ) * (SINT64)( pageSz ) + (SINT64)( offsetInSeq ) )

   /*
      _dmsLobRecord define
   */
   struct _dmsLobRecord : public SDBObject
   {
      const bson::OID   *_oid ;
      UINT32            _sequence ;
      UINT32            _offset ;  /// offset in a page, not the offset of lob
      UINT32            _hash ;
      UINT32            _dataLen ;
      const CHAR        *_data ;

      _dmsLobRecord()
      :_oid( NULL ),
       _sequence( 0 ),
       _offset( 0 ),
       _hash( 0 ),
       _dataLen( 0 ),
       _data( NULL )
      {
      }

      void clear()
      {
         _oid = NULL ;
         _sequence = 0 ;
         _offset = 0 ;
         _hash = 0 ;
         _dataLen = 0 ;
         _data = NULL ;
         return ;
      }

      BOOLEAN empty() const
      {
         return NULL == _oid ;
      }

      void set( const bson::OID *oid,
                UINT32 sequence,
                UINT32 offset,
                UINT32 dataLen,
                const CHAR *data )
      {
         _oid = oid ;
         _sequence = sequence ;
         _offset = offset ;
         _hash = ossHash( ( const BYTE * )_oid->getData(), 12,
                          ( const BYTE * )( &_sequence ),
                          sizeof( _sequence ) ) ;
         _dataLen = dataLen ;
         _data = data ;
         return ;
      }
   } ;
   typedef struct _dmsLobRecord dmsLobRecord ;

   #pragma pack(1)

   /*
      _dmsLobMeta define
   */
   struct _dmsLobMeta : public SDBObject
   {
      SINT64      _lobLen ;
      UINT64      _createTime ;
      UINT8       _status ;
      CHAR        _pad[495] ;

      _dmsLobMeta()
      :_lobLen( 0 ),
       _createTime( 0 ),
       _status( DMS_LOB_UNCOMPLETE )
      {
         ossMemset( _pad, 0, sizeof( _pad ) ) ;
      }

      void clear()
      {
         _lobLen = 0 ;
         _createTime = 0 ;
         _status = DMS_LOB_UNCOMPLETE ;
         ossMemset( _pad, 0, sizeof( _pad ) ) ;
      }

      BOOLEAN isDone() const
      {
         return ( DMS_LOB_COMPLETE == _status ) ? TRUE : FALSE ;
      }
   } ;
   typedef struct _dmsLobMeta dmsLobMeta ;

   /*
      _dmsLobDataMapBlk define
   */
   struct _dmsLobDataMapBlk: public SDBObject
   {
      CHAR           _pad1[4] ;
      BYTE           _oid[DMS_LOB_OID_LEN] ;
      UINT32         _sequence ;
      UINT32         _dataLen ;
      SINT32         _prevPageInBucket ;
      SINT32         _nextPageInBucket ;
      UINT32         _clLogicalID ;
      UINT16         _mbID ;
      CHAR           _pad2[212];  /// sizeof( _dmsLobDataMapBlk ) == 256B

      _dmsLobDataMapBlk()
      :_sequence( 0 ),
       _dataLen( 0 ),
       _prevPageInBucket( DMS_LOB_INVALID_PAGEID ),
       _nextPageInBucket( DMS_LOB_INVALID_PAGEID ),
       _clLogicalID( DMS_INVALID_CLID ),
       _mbID( DMS_INVALID_MBID )
      {
         ossMemset( this, 0, sizeof( _pad1 ) + sizeof( _oid ) ) ;
         ossMemset( _pad2, 0, sizeof( _pad2 ) ) ;
         SDB_ASSERT( 256 == sizeof( _dmsLobDataMapBlk ),
                     "invalid blk" ) ;
      }

      BOOLEAN equals( const BYTE *oid, UINT32 sequence ) const
      {
         return ( _sequence == sequence &&
                  0 == ossMemcmp( _oid, oid, DMS_LOB_OID_LEN ) ) ?
                TRUE : FALSE ;
      }
   } ;
   typedef struct _dmsLobDataMapBlk dmsLobDataMapBlk ;

   #pragma pack()

   struct _dmsLobInfoOnPage
   {
      UINT32 _sequence ;
      UINT32 _len ;
      bson::OID _oid ;
   } ;
   typedef struct _dmsLobInfoOnPage dmsLobInfoOnPage ;
}

#endif // DMS_LOBDEF_HPP_

