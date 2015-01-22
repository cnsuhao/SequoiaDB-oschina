#include "msgBuffer.hpp"
#include "util.hpp"

INT32 _msgBuffer::alloc( CHAR *&ptr, const UINT32 size )
{
   INT32 rc = SDB_OK ;

   if( 0 ==  size )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   ptr = ( CHAR *) SDB_OSS_MALLOC( size ) ;
   if ( NULL == ptr )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   _capacity = size ;

done:
   return rc ;
error:
   goto done ;
}

INT32 _msgBuffer::realloc( CHAR *&ptr, const UINT32 size )
{
   INT32 rc = SDB_OK ;
   if ( 0 == size )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( size <= _capacity )
   {
      goto done ;
   }

   ptr = ( CHAR * )SDB_OSS_REALLOC( ptr, size + 1 ) ;
   if ( NULL == ptr )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   _capacity = size ;
   ossMemset( _data + _size, 0, _capacity - _size ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 _msgBuffer::write( const CHAR *in, const UINT32 inLen )
{
   INT32 rc   = SDB_OK ;
   INT32 size = 0 ;        // new size to realloc
   INT32 num  = 0 ;        // number of memery block
   if( inLen > _capacity - _size )
   {
      num = ( inLen + _size ) / MEMERY_BLOCK_SIZE + 1 ;
      size = num * MEMERY_BLOCK_SIZE ;

      rc = realloc( _data, size ) ;
      if( SDB_OK != rc )
      {
         goto error ;
      }

   }

   ossMemcpy( _data + _size, in, inLen ) ;
   _size += inLen ;

done:
   return rc ;
error:
   goto done ;
}

INT32 _msgBuffer::advance( const UINT32 pos )
{
   INT32 rc = SDB_OK ;

   if ( pos > _capacity )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   _size = pos ;

done:
   return rc ;
error:
   goto done ;
}

int main( int argc, char** argv)
{
   std::string str = "abcdef" ;
   fixedStream buffer ;
   for ( int i = 0; i < 10; ++i)
   {
      str += "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" ;
      buffer.write( str.c_str(), str.length() ) ;
   }

   std::cout << buffer.data() << std::endl ;

   return 0 ;
}

