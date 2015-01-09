#include "../ext/jstobs.h"
#include "ossTypes.hpp"
#include "iostream"

#define RECEIVE_BUFFER_SIZE 4095
#define COLLECTION_SPACE_MAX_SZ 127
#define COLLECTION_MAX_SZ 127
typedef UINT32_64 UintPtr ;
#define OSS_NEWLINE "\r\n"
#define OSS_PRIxPTR "%08lx"
#define OSS_PRIXPTR "%08lX"
#define OSS_INT8_MAX_HEX_STRING   "0xFF"
#define OSS_INT16_MAX_HEX_STRING  "0xFFFF"
#define OSS_INT32_MAX_HEX_STRING  "0xFFFFFFFF"
#define OSS_INT64_MAX_HEX_STRING  "0xFFFFFFFFFFFFFFFF"
#define OSS_INTPTR_MAX_HEX_STRING   OSS_INT32_MAX_HEX_STRING
#define OSS_HEXDUMP_SPLITER " : "
#define OSS_HEXDUMP_BYTES_PER_LINE 16
#define OSS_HEXDUMP_ADDRESS_SIZE  (   sizeof( OSS_INTPTR_MAX_HEX_STRING \
                                              OSS_HEXDUMP_SPLITER )     \
                                    - sizeof( '\0' ) )
#define OSS_HEXDUMP_HEX_LEN       (   ( OSS_HEXDUMP_BYTES_PER_LINE << 1 )   \
                                    + ( OSS_HEXDUMP_BYTES_PER_LINE >> 1 ) )
#define OSS_HEXDUMP_SPACES_IN_BETWEEN  2
#define OSS_HEXDUMP_START_OF_DATA_DISP (   OSS_HEXDUMP_HEX_LEN              \
                                         + OSS_HEXDUMP_SPACES_IN_BETWEEN )
#define OSS_HEXDUMP_LINEBUFFER_SIZE  (   OSS_HEXDUMP_ADDRESS_SIZE       \
                                       + OSS_HEXDUMP_START_OF_DATA_DISP \
                                       + OSS_HEXDUMP_BYTES_PER_LINE     \
                                       + sizeof(OSS_NEWLINE) )
#define OSS_HEXDUMP_NULL_PREFIX    ((CHAR *) NULL)

#define OSS_HEXDUMP_INCLUDE_ADDR    1
#define OSS_HEXDUMP_RAW_HEX_ONLY    2
#define OSS_HEXDUMP_PREFIX_AS_ADDR  4
#define ossStrncmp(x,y,z) strncmp(x,y,z)
#define ossStrcmp(x,y) strcmp(x,y)
#define ossStrncpy(x,y,z) strncpy(x,y,z)
#define ossStrncat(x,y,z) strncat(x,y,z)
#if defined(_LINUX)
#define ossStrdup(x) strdup(x)
#elif defined (_WINDOWS)
#define ossStrdup(x) _strdup(x)
#endif
#define ossMemcpy(x,y,z) memcpy(x,y,z)
#define ossMemmove(x,y,z) memmove(x,y,z)
#define ossMemset(x,y,z) memset(x,y,z)
#define ossMemcmp(x,y,z) memcmp(x,y,z)
#define ossStrlen(x) strlen(x)
#define ossStrrchr(x,y) strrchr(x,y)
#define ossStrchr(x,y) strchr(x,y)
#define ossAtoi(x) atoi(x)

char receiveBuffer [ RECEIVE_BUFFER_SIZE+1 ] ;
char *pOutBuffer = NULL ;
int outBufferSize = 0 ;

INT32 readInput ( CHAR *pPrompt, INT32 numIndent );
UINT32 ossHexDumpBuffer
(
   const void *   inPtr,
   UINT32         len,
   CHAR *         szOutBuf,
   UINT32         outBufSz,
   const void *   szPrefix,
   UINT32         flags
);

char *readLine ( char* p, int length )
{
   fgets ( p, length, stdin ) ;
   p[strlen(p)-1] = 0 ;
   return p ;
}

INT32 readInput ( CHAR *pPrompt, INT32 numIndent )
{
   memset ( receiveBuffer, 0, sizeof(receiveBuffer) ) ;
   for ( INT32 i = 0; i<numIndent; i++ )
   {
      printf("\t") ;
   }
   printf("%s> ", pPrompt ) ;
   readLine ( receiveBuffer, sizeof(receiveBuffer) ) ;
   while ( receiveBuffer[strlen(receiveBuffer)-1] == '\\' &&
           RECEIVE_BUFFER_SIZE - strlen(receiveBuffer) > 0 )
   {
      for ( INT32 i = 0; i<numIndent; i++ )
      {
         printf("\t") ;
      }
      printf ( "> " ) ;
      readLine ( &receiveBuffer[strlen(receiveBuffer)-1],
                 RECEIVE_BUFFER_SIZE - strlen(receiveBuffer) ) ;
   }
   if ( RECEIVE_BUFFER_SIZE == strlen(receiveBuffer) )
   {
      printf ( "Error: Max input length is %d bytes\n", RECEIVE_BUFFER_SIZE ) ;
      return SDB_INVALIDARG ;
   }
   return SDB_OK ;
}

int main(int argc,char *argv[])
{
   if( argc > 2 || argc < 2 )
   {
       std::cout << "Input false !\n" ; 
       return 0;
   }
   INT32 rc = SDB_OK ;
   CHAR BUFF  [ RECEIVE_BUFFER_SIZE ] ;

   bson *bs = bson_create () ;
   std::cout << argv[1] <<"\n" ; 
   if ( !jsonToBson ( bs, argv[1] ) )
   {
      bson_finish  ( bs ) ;
      bson_dispose ( bs ) ;
      std::cout << "json to bson false !\n" ; 
      return 0;
   }

   bson_finish ( bs ) ;

   ossHexDumpBuffer( bs->data, *(int*)bs->data, BUFF, sizeof(BUFF), NULL, 4 ) ;

   std::cout << BUFF <<"\n" ; 

   bson_dispose ( bs ) ;
   return 0;
}


size_t ossSnprintf(char* pBuffer, size_t iLength, const char* pFormat, ...)
{
   va_list ap;
   int n;
   va_start(ap, pFormat);
#if defined (_WINDOWS)
   n=_vsnprintf(pBuffer, iLength, pFormat, ap);
#else
   n=vsnprintf(pBuffer, iLength, pFormat, ap);
#endif
   va_end(ap);
   if((n<0) || (size_t)n>=iLength)
      n=iLength-1;
   pBuffer[n]='\0';
   return (size_t)n;
}



UINT32 ossHexDumpLine
(
   const void *   inPtr,
   UINT32         len,
   CHAR *         szOutBuf,
   UINT32         flags
)
{
   const char * cPtr = ( const char *)inPtr ;
   UINT32 curOff = 0 ;
   UINT32 bytesWritten = 0 ;
   UINT32 offInBuf = 0 ;
   UINT32 bytesRemain = OSS_HEXDUMP_LINEBUFFER_SIZE ;


   if ( inPtr && szOutBuf && ( len <= OSS_HEXDUMP_BYTES_PER_LINE ) )
   {
      bool padding = false ;

      szOutBuf[OSS_HEXDUMP_LINEBUFFER_SIZE - 1] = '\0' ;

      /* OSS_HEXDUMP_INCLUDE_ADDRESS */
      if ( flags & OSS_HEXDUMP_INCLUDE_ADDR )
      {
         offInBuf = ossSnprintf( szOutBuf, bytesRemain,
                                 "0x"OSS_PRIXPTR" : ", (UintPtr)cPtr) ;
         bytesRemain -= offInBuf ;
      }

      for ( UINT32 i = 0 ; i < len ; ++i )
      {
         bytesWritten = ossSnprintf( &szOutBuf[ offInBuf ], bytesRemain,
                                     "%02X", (unsigned char)cPtr[ i ] ) ;
         offInBuf += bytesWritten ;
         bytesRemain -= bytesWritten ;
        /* if ( bytesRemain )
         {
            szOutBuf[ offInBuf ] = ' ' ;
         }*/
         if ( padding && bytesRemain )
         {
         	  szOutBuf[ offInBuf ] = ' ' ;
            ++offInBuf ;
            bytesRemain -- ;
         }
         padding = ! padding ;
      }

      curOff = OSS_HEXDUMP_START_OF_DATA_DISP ;
      if ( flags & OSS_HEXDUMP_INCLUDE_ADDR )
      {
         curOff += OSS_HEXDUMP_ADDRESS_SIZE ;
      }

      if ( offInBuf < curOff )
      {
         ossMemset( &szOutBuf[ offInBuf ], ' ', ( curOff - offInBuf ) ) ;
      }

      if ( ! ( flags & OSS_HEXDUMP_RAW_HEX_ONLY ) )
      {
         for ( UINT32 i = 0 ; i < len ; i++, curOff++ )
         {
            /* Print character as is only if it is printable */
            if ( cPtr[i] >= ' ' && cPtr[i] <= '~' )
            {
               if ( curOff < OSS_HEXDUMP_LINEBUFFER_SIZE )
               {
                  szOutBuf[ curOff ] = cPtr[ i ] ;
               }
            }
            else
            {
               if ( curOff < OSS_HEXDUMP_LINEBUFFER_SIZE )
               {
                  szOutBuf[ curOff ] = '.' ;
               }
            }
         }
      }

      if ( curOff + sizeof(OSS_NEWLINE) <= OSS_HEXDUMP_LINEBUFFER_SIZE )
      {
         ossStrncpy( &szOutBuf[curOff], OSS_NEWLINE, sizeof( OSS_NEWLINE ) ) ;
         curOff += sizeof( OSS_NEWLINE ) - sizeof( '\0' ) ;
      }
      else
      {
         szOutBuf[OSS_HEXDUMP_LINEBUFFER_SIZE - 1] = '\0' ;
         curOff = OSS_HEXDUMP_LINEBUFFER_SIZE - 1 ;
      }
   }
   return curOff ;
}






UINT32 ossHexDumpBuffer
(
   const void *   inPtr,
   UINT32         len,
   CHAR *         szOutBuf,
   UINT32         outBufSz,
   const void *   szPrefix,
   UINT32         flags
)
{
   UINT32 bytesProcessed = 0 ;
   CHAR szLineBuf[OSS_HEXDUMP_LINEBUFFER_SIZE] = { 0 } ;
   unsigned char preLine[ OSS_HEXDUMP_BYTES_PER_LINE ] = { 0 } ;
   bool bIsDupLine = false ;
   bool bPrinted = false ;
   CHAR * curPos = szOutBuf ;
   UINT32 prefixLength = 0 ;
   UINT32 totalLines = ossAlignX(len, OSS_HEXDUMP_BYTES_PER_LINE) /
                          OSS_HEXDUMP_BYTES_PER_LINE ;
   const char * cPtr = (const char *)inPtr ;
   const char * addrPtr = (const char *)szPrefix ;
   char szAddrStr[ OSS_HEXDUMP_ADDRESS_SIZE + 1 ] = { 0 } ;

   /* sanity check */
   if ( !( inPtr && szOutBuf && outBufSz ) )
   {
      goto exit ;
   }

   if ( flags & OSS_HEXDUMP_PREFIX_AS_ADDR )
   {
      flags = ( ~ OSS_HEXDUMP_INCLUDE_ADDR ) & flags ;
      prefixLength = OSS_HEXDUMP_ADDRESS_SIZE ;
   }
   else if (szPrefix)
   {
      prefixLength = ossStrlen((const CHAR*)szPrefix) ;
   }

   for ( UINT32 i = 0 ;
         i < totalLines ;
         i++, cPtr += OSS_HEXDUMP_BYTES_PER_LINE,
              addrPtr += OSS_HEXDUMP_BYTES_PER_LINE  )
   {
      UINT32 curLen, curOff ;
      if ( i + 1 == totalLines )
      {
         curLen = len - i * OSS_HEXDUMP_BYTES_PER_LINE ;
      }
      else
      {
         curLen = OSS_HEXDUMP_BYTES_PER_LINE ;
      }

      if ( OSS_HEXDUMP_BYTES_PER_LINE == curLen )
      {
         if ( i > 0 )
         {
            bIsDupLine = ( 0 == ossMemcmp( preLine,
                                           cPtr,
                                           OSS_HEXDUMP_BYTES_PER_LINE ) ) ;
         }
         ossMemcpy( preLine, cPtr, OSS_HEXDUMP_BYTES_PER_LINE ) ;
      }

      if ( ! bIsDupLine )
      {
         curOff = ossHexDumpLine( cPtr, curLen, szLineBuf, flags ) ;
      }
      else
      {
         curOff = 0 ;
         szLineBuf[0]= '\0' ;
      }

      if ( outBufSz >= curOff + prefixLength + 1 )
      {
         bytesProcessed += curLen ;
         if ( ! bIsDupLine )
         {
            if ( flags & OSS_HEXDUMP_PREFIX_AS_ADDR )
            {
               ossSnprintf( szAddrStr, sizeof( szAddrStr ),
                            "0x"OSS_PRIXPTR OSS_HEXDUMP_SPLITER,
                            (UintPtr)addrPtr ) ;
               ossStrncpy(curPos, szAddrStr, prefixLength + 1) ;
               curPos += prefixLength ;
               outBufSz -= prefixLength ;
            }
            else
            {
               if ( prefixLength )
               {
                  /* copy prefix first */
                  ossStrncpy(curPos, (const CHAR*)szPrefix, prefixLength + 1) ;
                  curPos += prefixLength ;
                  outBufSz -= prefixLength ;
               }
            }
            bPrinted = false ;
         }
         else
         {
            if ( ! bPrinted )
            {
               ossStrncpy(curPos, "*"OSS_NEWLINE, sizeof( "*"OSS_NEWLINE )) ;
               curPos += sizeof( "*"OSS_NEWLINE ) - sizeof( '\0' ) ;
               outBufSz -= sizeof( "*"OSS_NEWLINE ) - sizeof( '\0' ) ;
               bPrinted = true ;
            }
         }
         ossStrncpy(curPos, szLineBuf, curOff + 1) ;
         outBufSz -= curOff ;
         curPos += curOff ;
      }
      else
      {
         break ;
      }
      if ( ( curPos ) && ( (int)( curPos - szOutBuf ) >= 0 ) )
      {
         *curPos = '\0' ;
      }
   }

exit :
   return bytesProcessed ;
}
