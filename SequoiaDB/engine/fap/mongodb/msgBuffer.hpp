#ifndef _SDB_MSG_BUFFER_HPP_
#define _SDB_MSG_BUFFER_HPP_

#include "oss.hpp"
#include "ossUtil.hpp"
#include "ossMem.hpp"
#define MEMERY_BLOCK_SIZE 4096

class _msgBuffer : public SDBObject
{
public:
   _msgBuffer() : _data( NULL ), _size( 0 ), _capacity( 0 )
   {
      alloc( _data, MEMERY_BLOCK_SIZE ) ;
   }

   ~_msgBuffer()
   {
      if ( NULL != _data )
      {
         SDB_OSS_FREE( _data ) ;
         _data = NULL ;
      }

   }

   BOOLEAN empty() const
   {
      return 0 == _size ;
   }

   INT32 write( const CHAR *in, const UINT32 inLen ) ;

   INT32 read( CHAR* in, const UINT32 len ) ;

   INT32 advance( const UINT32 pos ) ;

   void zero()
   {
      ossMemset( _data, 0, _capacity ) ;
      _size = 0 ;
   }

   CHAR *data() const
   {
      return _data ;
   }

   const UINT32 size() const
   {
      return _size ;
   }

   const UINT32 capacity() const
   {
      return _capacity ;
   }

   void reverse( const UINT32 size )
   {
      if ( size < _capacity )
      {
         return ;
      }

      realloc( _data, size ) ;
   }

private:
   INT32 alloc( CHAR *&ptr, const UINT32 size ) ;
   INT32 realloc( CHAR *&ptr, const UINT32 size ) ;

private:
   CHAR  *_data ;
   UINT32 _size ;
   UINT32 _capacity ;
} ;

typedef _msgBuffer fixedStream ;

#endif
