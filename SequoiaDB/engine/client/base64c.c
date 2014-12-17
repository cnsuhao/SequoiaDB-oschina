/*******************************************************************************
   Copyright (C) 2012-2014 SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*******************************************************************************/
#include "base64c.h"

static char B64[64] =
{
   'A','B','C','D','E','F','G','H',
   'I','J','K','L','M','N','O','P',
   'Q','R','S','T','U','V','W','X',
   'Y','Z',
   'a','b','c','d','e','f','g','h',
   'i','j','k','l','m','n','o','p',
   'q','r','s','t','u','v','w','x',
   'y','z',
   '0','1','2','3','4','5','6','7',
   '8','9','+','/'
} ;

int getEnBase64Size ( int size )
{
   int len = size ;
   int zeroize = len % 3 ;
   len = ( len + ( zeroize ? 3 - zeroize : 0 ) ) / 3 * 4 + 1 ;
   return len ;
}

int getDeBase64Size ( const char *s )
{
   int len = strlen ( s ) ;
   int zeroize = 0 ;
   if ( '=' == s [ len - 2 ] )
      zeroize = 2 ;
   else if ( '=' == s [ len - 1 ] )
      zeroize = 1 ;
   len = len / 4 * 3 - zeroize + 1 ;
   return len ;
}

int base64Encode ( const char *s, int in_size, char *_ret, int out_size )
{
   char c = 0x00 ;
   char t = 0x00 ;
   int vLen = 0 ;
   int len = in_size ;
   if ( out_size < getEnBase64Size ( in_size ) )
      return 0 ;
   while ( len > 0 )
   {
      /* This is not support chinese
      *_ret++ = B64[ ( s [ 0 ] >> 2 ) & 0x3F ] ;
      if ( len > 2 )
      {
         *_ret++ = B64[ ( ( s [ 0 ] & 3 ) << 4 ) | ( s [ 1 ] >> 4 ) ] ;
         *_ret++ = B64[ ( ( s [ 1 ] & 0xF ) << 2 ) | ( s [ 2 ] >> 6 ) ] ;
         *_ret++ = B64[ ( s [ 2 ] & 0x3F ) ] ;
      }*/
      /* This is support chinese */
      c = ( s [ 0 ] >> 2 ) ;
      c = c & 0x3F;
      *_ret++ = B64[ (int)c ] ;
      if ( len > 2 )
      {
         c = ( s [ 0 ] & 3 ) << 4 ;
         t = ( s [ 1 ] >> 4 ) & 0x0F ;
         *_ret++ = B64 [ ( c ) | ( t ) ] ;
         c = ( s [ 1 ] & 0xF ) << 2 ;
         t = ( s [ 2 ] >> 6 ) & 0x3 ;
         *_ret++ = B64 [ ( c ) | ( t ) ] ;
         c = s [ 2 ] & 0x3F ;
         *_ret++ = B64 [ (int)c ] ;
      }
      else
      {
         switch ( len )
         {
            case 1:
               *_ret++ = B64[ ( s [ 0 ] & 3 ) << 4 ] ;
               *_ret++ = '=' ;
               *_ret++ = '=' ;
               break ;
            case 2:
               *_ret++ = B64[ ( ( s [ 0 ] & 3 ) << 4 ) | ( s [ 1 ] >> 4 ) ] ;
               *_ret++ = B64[ ( ( s [ 1 ] & 0x0F ) << 2 ) | ( s [ 2 ] >> 6 ) ] ;
               *_ret++ = '=' ;
               break ;
         }
      }
      s += 3 ;
      len -= 3 ;
      vLen += 4 ;
   }
   *_ret = 0 ;
   return vLen ;
}

char getCharIndex ( char c )
{
   if ( ( c >= 'A' ) && ( c <= 'Z' ) )
   {
      return c - 'A' ;
   }
   else if ( ( c >= 'a' ) && ( c <= 'z' ) )
   {
      return c - 'a' + 26 ;
   }
   else if ( ( c >= '0' ) && ( c <= '9' ) )
   {
      return c - '0' + 52 ;
   }
   else if ( c == '+' )
   {
      return 62 ;
   }
   else if ( c == '/' )
   {
      return 63 ;
   }
   else if ( c == '=' )
   {
      return 0 ;
   }
   return 0 ;
}

int base64Decode ( const char *s, char *_ret, int out_size )
{
   static char lpCode [ 4 ] ;
   int vLen = 0 ;
   int len = strlen ( s ) ;

   /* base64 must be 4 bytes aligned */
   if ( ( len % 4 ) ||
        ( out_size < getDeBase64Size ( s ) ) )
      return 0 ;

/* this is error code
   while ( p < e )
   {
      memcpy ( unit, p, 4 ) ;
      if ( unit[3] == '=' )
         unit[3] = 0 ;
      if ( unit[2] == '=' )
         unit[2] = 0 ;
      p += 4 ;

      for ( i = 0 ; unit[0] != B64[i] && i < 64 ; i++ ) ;
      unit[0] = i==64 ? 0 : i ;
      for ( i = 0 ; unit[1] != B64[i] && i < 64 ; i++ ) ;
      unit[1] = i==64 ? 0 : i ;
      for ( i = 0 ; unit[2] != B64[i] && i < 64 ; i++ ) ;
      unit[2] = i==64 ? 0 : i ;
      for ( i = 0 ; unit[3] != B64[i] && i < 64 ; i++ ) ;
      unit[3] = i==64 ? 0 : i ;
      *r++ = (unit[0]<<2) | (unit[1]>>4) ;
      *r++ = (unit[1]<<4) | (unit[2]>>2) ;
      *r++ = (unit[2]<<6) | unit[3] ;
   }
   *r = 0 ;
   return _ret ;
*/
   while ( len > 2 )
   {
      lpCode [ 0 ] = getCharIndex ( s [ 0 ] ) ;
      lpCode [ 1 ] = getCharIndex ( s [ 1 ] ) ;
      lpCode [ 2 ] = getCharIndex ( s [ 2 ] ) ;
      lpCode [ 3 ] = getCharIndex ( s [ 3 ] ) ;

      *_ret++ = ( lpCode [ 0 ] << 2 ) | ( lpCode [ 1 ] >> 4 ) ;
      *_ret++ = ( lpCode [ 1 ] << 4 ) | ( lpCode [ 2 ] >> 2 ) ;
      *_ret++ = ( lpCode [ 2 ] << 6 ) | ( lpCode [ 3 ] ) ;

      s += 4 ;
      len -= 4 ;
      vLen += 3 ;
   }

   return vLen ;
}
