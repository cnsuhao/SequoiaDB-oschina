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

   Source File Name = rawbson2csv.c

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          12/1/2014  ly  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rawbson2csv.h"
#include "ossUtil.h"
#include "../client/bson/bson.h"
#include "../client/base64c.h"
#include "time.h"

#define TIME_FORMAT "%d-%d-%d-%d.%d.%d.%d"
#define DATE_FORMAT "%d-%d-%d"
#define INT32_LAST_YEAR 2038
#define RELATIVE_YEAR 1900
#define RELATIVE_MOD 12
#define RELATIVE_DAY 31
#define RELATIVE_HOUR 24
#define RELATIVE_MIN_SEC 60
#define CSV_STR_LEFTBRACKET    "("
#define CSV_STR_RIGHTBRACKET   ")"
#define CSV_STR_BACKSLASH      "/"

static void local_time ( time_t *Time, struct tm *TM )
{
   if ( !Time || !TM )
      return ;
#if defined (__linux__ )
   localtime_r( Time, TM ) ;
#elif defined (_WIN32)
   localtime_s( TM, Time ) ;
#endif
}

INT32 _appendString( CHAR delChar, const CHAR *pBuffer, INT32 size,
                     CHAR **ppCSVBuf, INT32 *pCSVSize )
{
   INT32 rc = SDB_OK ;
   INT32 isDoubleChar = FALSE ;
   INT32 i = 0 ;

   for ( i = 0; i < size; )
   {
      if ( ppCSVBuf && (*pCSVSize) == 0 )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      if ( isDoubleChar )
      {
         if ( ppCSVBuf )
         {
            *(*ppCSVBuf) = delChar ;
         }
         isDoubleChar = FALSE ;
      }
      else
      {
         if ( *(pBuffer + i) == delChar )
         {
            isDoubleChar = TRUE ;
         }
         if ( ppCSVBuf )
         {
            *(*ppCSVBuf) = *(pBuffer + i) ;
         }
         ++i ;
      }
      if ( ppCSVBuf )
      {
         ++(*ppCSVBuf) ;
         --(*pCSVSize) ;
      }
      else
      {
         ++(*pCSVSize) ;
      }
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 _appendObj( CHAR delChar, bson_iterator *pIt,
                  CHAR **ppCSVBuf, INT32 *pCSVSize )
{
   INT32 rc = SDB_OK ;
   INT32 size    = 0 ;
   INT32 objSize = 0 ;
   CHAR *pBuffer = NULL ;
   CHAR *pTempBuf = NULL ;

   size = bson_sprint_length_iterator ( pIt ) ;

   if ( ppCSVBuf && size > (*pCSVSize) )
   {
      rc = SDB_OOM ;
      goto error ;
   }

   pBuffer = (CHAR *)SDB_OSS_MALLOC( size ) ;
   if ( !pBuffer )
   {
      rc = SDB_OOM ;
      goto error ;
   }
   pTempBuf = pBuffer ;
   ossMemset( pTempBuf, 0, size ) ;

   objSize = size ;
   if( !bson_sprint_iterator ( &pTempBuf,
                               &size,
                               pIt, '"' ) )
   {
      rc = SDB_OOM ;
      goto error ;
   }
   objSize -= size ;
   rc = _appendString( delChar, pBuffer, objSize,
                       ppCSVBuf, pCSVSize ) ;
   if ( rc )
   {
      goto error ;
   }
done:
   SAFE_OSS_FREE( pBuffer ) ;
   return rc ;
error:
   goto done ;
}

INT32 _appendNonString( CHAR delChar, bson_iterator *pIt,
                        CHAR **ppCSVBuf, INT32 *pCSVSize )
{
   INT32 rc = SDB_OK ;
   INT32 size     = 0 ;

   size = bson_sprint_length_iterator( pIt ) ;

   if ( !ppCSVBuf )
   {
      (*pCSVSize) += size ;
      goto done ;
   }

   if ( size > (*pCSVSize) )
   {
      rc = SDB_OOM ;
      goto error ;
   }
   if ( !bson_sprint_iterator ( ppCSVBuf, pCSVSize,
                                pIt, delChar ) )
   {
      rc = SDB_OOM ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 _appendValue( CHAR delChar, bson_iterator *pIt,
                    CHAR **ppBuffer, INT32 *pCSVSize,
                    BOOLEAN includeBinary,
                    BOOLEAN includeRegex )
{
   INT32 rc = SDB_OK ;
   bson_type type = bson_iterator_type( pIt ) ;
   INT32 tempSize = 0 ;
   INT32 base64Size = 0 ;
   INT32 binType = 0 ;
   CHAR temp[128] = { 0 } ;
   const CHAR *pTemp = NULL ;
   CHAR *pBase64 = NULL ;
   bson_timestamp_t ts;
   time_t timer ;
   struct tm psr;

   if ( type == BSON_DOUBLE || type == BSON_BOOL ||
        type == BSON_NULL || type == BSON_INT ||
        type == BSON_LONG )
   {
      rc = _appendNonString( delChar, pIt, ppBuffer, pCSVSize ) ;
      if ( rc )
      {
         goto error ;
      }
   }
   else
   {
      rc = _appendString( delChar, &delChar, 1, ppBuffer, pCSVSize ) ;
      if ( rc )
      {
         goto error ;
      }
      if ( type == BSON_TIMESTAMP )
      {
         ts = bson_iterator_timestamp( pIt ) ;
         timer = (time_t)ts.t;
         local_time( &timer, &psr ) ;
         tempSize = ossSnprintf ( temp, 64,
                                  "%04d-%02d-%02d-%02d.%02d.%02d.%06d",
                                  psr.tm_year + 1900,
                                  psr.tm_mon + 1,
                                  psr.tm_mday,
                                  psr.tm_hour,
                                  psr.tm_min,
                                  psr.tm_sec,
                                  ts.i ) ;
         rc = _appendString( delChar, temp, tempSize, ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( type == BSON_DATE )
      {
         timer = bson_iterator_date( pIt ) / 1000 ;
         local_time( &timer, &psr ) ;
         tempSize = ossSnprintf ( temp, 64, "%04d-%02d-%02d",
                                  psr.tm_year + 1900,
                                  psr.tm_mon + 1,
                                  psr.tm_mday ) ;
         rc = _appendString( delChar, temp, tempSize, ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( type == BSON_UNDEFINED )
      {
         rc = _appendString( delChar, CSV_STR_UNDEFINED,
                             CSV_STR_UNDEFINED_SIZE,
                             ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( type == BSON_MINKEY )
      {
         rc = _appendString( delChar, CSV_STR_MINKEY,
                             CSV_STR_MINKEY_SIZE, ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( type == BSON_MAXKEY )
      {
         rc = _appendString( delChar, CSV_STR_MAXKEY,
                             CSV_STR_MAXKEY_SIZE, ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( type == BSON_CODE )
      {
         pTemp = bson_iterator_code( pIt ) ;
         rc = _appendString( delChar, pTemp, ossStrlen( pTemp ),
                             ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( type == BSON_STRING || type == BSON_SYMBOL )
      {
         pTemp = bson_iterator_string( pIt ) ;
         rc = _appendString( delChar, pTemp, ossStrlen( pTemp ),
                             ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( type == BSON_BINDATA )
      {
         if( TRUE == includeBinary )
         {
            rc = _appendString( delChar, CSV_STR_LEFTBRACKET, 1, ppBuffer, pCSVSize ) ;
            if ( rc )
            {
               goto error ;
            }
            binType = (INT32)bson_iterator_bin_type( pIt ) ;
            tempSize = ossSnprintf ( temp, 64, "%d", binType ) ;
            rc = _appendString( delChar, &temp, tempSize, ppBuffer, pCSVSize ) ;
            if ( rc )
            {
               goto error ;
            }
            rc = _appendString( delChar, CSV_STR_RIGHTBRACKET, 1, ppBuffer, pCSVSize ) ;
            if ( rc )
            {
               goto error ;
            }
         }
         pTemp = bson_iterator_bin_data( pIt ) ;
         tempSize = bson_iterator_bin_len ( pIt ) ;
         base64Size = getEnBase64Size ( tempSize ) ;
         pBase64 = (CHAR *)SDB_OSS_MALLOC( base64Size ) ;
         if( NULL == pBase64 )
         {
            rc = SDB_OOM ;
            goto error ;
         }
         ossMemset( pBase64, 0, base64Size ) ;
         if ( !base64Encode( pTemp, tempSize, pBase64, base64Size ) )
         {
            rc = SDB_OOM ;
            goto error ;
         }
         rc = _appendString( delChar, pBase64, base64Size - 1,
                             ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else if ( type == BSON_REGEX )
      {
         if( TRUE == includeRegex )
         {
            rc = _appendString( delChar, CSV_STR_BACKSLASH, 1,
                                ppBuffer, pCSVSize ) ;
            if ( rc )
            {
               goto error ;
            }
         }
         pTemp = bson_iterator_regex( pIt ) ;
         rc = _appendString( delChar, pTemp, ossStrlen( pTemp ),
                             ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
         if( TRUE == includeRegex )
         {
            rc = _appendString( delChar, CSV_STR_BACKSLASH, 1,
                                ppBuffer, pCSVSize ) ;
            if ( rc )
            {
               goto error ;
            }
            pTemp = bson_iterator_regex_opts( pIt ) ;
            rc = _appendString( delChar, pTemp, ossStrlen( pTemp ),
                                ppBuffer, pCSVSize ) ;
            if ( rc )
            {
               goto error ;
            }
         }
      }
      else if ( type == BSON_OID )
      {
         bson_oid_to_string( bson_iterator_oid( pIt ), temp ) ;
         rc = _appendString( delChar, temp, 24, ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      else
      {
         rc = _appendObj( delChar, pIt, ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      rc = _appendString( delChar, &delChar, 1, ppBuffer, pCSVSize ) ;
      if ( rc )
      {
         goto error ;
      }
   }
done:
   SAFE_OSS_FREE( pBase64 ) ;
   return rc ;
error:
   goto done ;
}

INT32 getCSVSize ( CHAR delChar, CHAR delField,
                   CHAR *pbson, INT32 *pCSVSize,
                   BOOLEAN includeBinary,
                   BOOLEAN includeRegex )
{
   INT32 rc = SDB_OK ;
   rc = bson2csv( delChar, delField, pbson, NULL, pCSVSize,
                  includeBinary,
                  includeRegex ) ;
   if ( rc )
   {
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 bson2csv( CHAR delChar, CHAR delField, CHAR *pbson,
                CHAR **ppBuffer, INT32 *pCSVSize,
                BOOLEAN includeBinary,
                BOOLEAN includeRegex )
{
   INT32 rc = SDB_OK ;
   BOOLEAN isFirst = TRUE ;
   bson_type fieldType ;
   bson_iterator it ;

   bson_iterator_from_buffer( &it, pbson ) ;


   while ( bson_iterator_next( &it ) )
   {
      fieldType = bson_iterator_type( &it ) ;
      if ( BSON_EOO == fieldType )
      {
         break ;
      }
      if ( isFirst )
      {
         isFirst = FALSE ;
      }
      else
      {
         rc = _appendString( delChar, &delField, 1, ppBuffer, pCSVSize ) ;
         if ( rc )
         {
            goto error ;
         }
      }
      if ( BSON_UNDEFINED == fieldType )
      {
         continue ;
      }
      rc = _appendValue( delChar, &it, ppBuffer, pCSVSize,
                         includeBinary,
                         includeRegex ) ;
      if ( rc )
      {
         goto error ;
      }
   }
done:
   return rc ;
error:
   goto done ;
}
