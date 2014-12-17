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

   Source File Name = csv2rawbson.cpp

   Descriptive Name = CSV To Raw BSON

   When/how to use: this program may be used on binary and text-formatted
   versions of UTIL component. This file contains declare of json2rawbson. Note
   this function should NEVER be directly called other than fromjson.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          05/04/2014  JWH Initial Draft

   Last Changed =

*******************************************************************************/

#include "csv2rawbson.hpp"
#include "ossUtil.h"
#include "pd.hpp"
#include "../client/bson/bson.h"
#include "time.h"
#include <math.h>
#include "../client/base64c.h"

#define CSV_STR_TABLE   '\t'
#define CSV_STR_CR      '\r'
#define CSV_STR_LF      '\n'
#define CSV_STR_COMMA   ','
#define CSV_STR_QUOTES  '"'
#define CSV_STR_SPACE   32
#define CSV_STR_SLASH   '\\'

#define TIME_FORMAT "%d-%d-%d-%d.%d.%d.%d"
#define DATE_FORMAT "%d-%d-%d"
#define INT32_LAST_YEAR 2038
#define RELATIVE_YEAR 1900
#define RELATIVE_MOD 12
#define RELATIVE_DAY 31
#define RELATIVE_HOUR 24
#define RELATIVE_MIN_SEC 60

#define TIME_MAX_NUM  2147356800
#define TIME_MIX_NUM -2209017600

const CHAR *_pCSVTYPESTR[] = {
   CSV_STR_INT,         CSV_STR_INTEGER,        CSV_STR_LONG,
   CSV_STR_BOOL,        CSV_STR_BOOLEAN,        CSV_STR_DOUBLE,
   CSV_STR_STRING,      CSV_STR_TIMESTAMP,      CSV_STR_DATE,
   CSV_STR_NULL
} ;

const INT32 _CSVTYPESTRSIZE[] = {
   CSV_STR_INT_SIZE,    CSV_STR_INTEGER_SIZE,   CSV_STR_LONG_SIZE,
   CSV_STR_BOOL_SIZE,   CSV_STR_BOOLEAN_SIZE,   CSV_STR_DOUBLE_SIZE,
   CSV_STR_STRING_SIZE, CSV_STR_TIMESTAMP_SIZE, CSV_STR_DATE_SIZE,
   CSV_STR_NULL_SIZE,   CSV_STR_TRUE_SIZE,      CSV_STR_FALSE_SIZE,
   CSV_STR_DEFAULT_SIZE
} ;

const INT32 _CSVTYPENUM[] = {
   0,    0,    1,
   2,    2,    3,
   4,    5,    6,
   7
} ;

CHAR *csvParser::_trimLeft ( CHAR *pCursor, INT32 &size )
{
   for ( INT32 i = 0; i < size; ++i )
   {
      switch( *pCursor )
      {
      case CSV_STR_TABLE:
      case CSV_STR_CR:
      case CSV_STR_LF:
      case CSV_STR_SPACE:
         ++pCursor ;
         break ;
      case 0:
      default:
         size -= i ;
         return pCursor ;
      }
   }
   return pCursor ;
}

CHAR *csvParser::_trimRight ( CHAR *pCursor, INT32 &size )
{
   for ( INT32 i = 1; i <= size; ++i )
   {
      switch( *( pCursor + ( size - i ) ) )
      {
      case CSV_STR_TABLE:
      case CSV_STR_CR:
      case CSV_STR_LF:
      case CSV_STR_SPACE:
         break ;
      case 0:
      default:
         size -= ( i - 1 ) ;
         return pCursor ;
      }
   }
   return pCursor ;
}

CHAR *csvParser::_trim ( CHAR *pCursor, INT32 &size )
{
   pCursor = _trimLeft( pCursor, size ) ;
   pCursor = _trimRight( pCursor, size ) ;
   return pCursor ;
}

/*
 * field has not type
*/
INT32 csvParser::_parseValue( _valueData &valueData, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   pBuffer = _trim( pBuffer, size ) ;

   //is string "xxxxxx"
   if ( _delChar == *pBuffer &&
        _delChar == *(pBuffer + size - 1) )
   {
      //++pBuffer ;
      //size -= 2 ;
      valueData.type = CSV_TYPE_STRING ;
      valueData.pVarString = pBuffer ;
      valueData.stringSize = size ;
      goto done ;
   }
   //is string xxxxx"
   else if ( _delChar != *pBuffer &&
             _delChar == *(pBuffer + size - 1) )
   {
      valueData.type = CSV_TYPE_STRING ;
      valueData.pVarString = pBuffer ;
      valueData.stringSize = size ;
      goto done ;
   }
   //is string "xxxxx
   else if ( _delChar == *pBuffer &&
             _delChar != *(pBuffer + size - 1) )
   {
      valueData.type = CSV_TYPE_STRING ;
      valueData.pVarString = pBuffer ;
      valueData.stringSize = size ;
      goto done ;
   }
   //not string  xxxxx
   else if ( _delChar != *pBuffer &&
             _delChar != *(pBuffer + size - 1) )
   {
      //is number?
      if ( size == CSV_STR_TRUE_SIZE &&
           ossStrncasecmp( pBuffer, CSV_STR_TRUE, CSV_STR_TRUE_SIZE ) == 0 )
      {
         valueData.type = CSV_TYPE_BOOL ;
         valueData.varBool = TRUE ;
         goto done ;
      }
      else if ( size == CSV_STR_NULL_SIZE &&
                ossStrncasecmp( pBuffer, CSV_STR_NULL,
                                CSV_STR_NULL_SIZE ) == 0 )
      {
         valueData.type = CSV_TYPE_NULL ;
         goto done ;
      }
      else if ( size == CSV_STR_FALSE_SIZE &&
                ossStrncasecmp( pBuffer, CSV_STR_FALSE,
                                CSV_STR_FALSE_SIZE ) == 0 )
      {
         valueData.type = CSV_TYPE_BOOL ;
         valueData.varBool = FALSE ;
         goto done ;
      }
      else
      {
         rc =  _parseNumber ( pBuffer, size,
                              valueData.type,
                              &valueData.varInt,
                              &valueData.varLong,
                              &valueData.varDouble ) ;
         if( rc )
         {
            goto error ;
         }
         if ( valueData.type == CSV_TYPE_STRING )
         {
            valueData.pVarString = pBuffer ;
            valueData.stringSize = size ;
         }
      }
   }
done:
   return rc ;
error:
   goto done ;
}

/*
 * field has type
*/
INT32 csvParser::_parseValue( _valueData &valueData,
                              _fieldData &fieldData,
                              CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   valueData.type = fieldData.type ;
   switch( fieldData.type )
   {
   case CSV_TYPE_INT:
      rc = _value2str( pBuffer, size,
                      &pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _string2int( valueData.varInt,
                        pBuffer,
                        size ) ;
      if ( rc )
      {
         rc = _string2bool( valueData.varBool,
                            pBuffer,
                            size ) ;
         if ( rc )
         {
            rc = SDB_OK ;
            if ( fieldData.hasDefVal )
            {
               valueData.varInt = fieldData.varInt ;
            }
            else
            {
               valueData.type = CSV_TYPE_NULL ;
            }
         }
         else
         {
            valueData.varInt = (INT32)valueData.varBool ;
         }
      }
      break ;
   case CSV_TYPE_LONG:
      rc = _value2str( pBuffer, size,
                      &pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _string2long( valueData.varLong,
                         pBuffer,
                         size ) ;
      if ( rc )
      {
         rc = _string2int( valueData.varInt,
                           pBuffer,
                           size ) ;
         if ( rc )
         {
            rc = _string2bool( valueData.varBool,
                               pBuffer,
                               size ) ;
            if ( rc )
            {
               rc = SDB_OK ;
               if ( fieldData.hasDefVal )
               {
                  valueData.varLong = fieldData.varLong ;
               }
               else
               {
                  valueData.type = CSV_TYPE_NULL ;
               }
            }
            else
            {
               valueData.varLong = (INT64)valueData.varBool ;
            }
         }
         else
         {
            valueData.varLong = (INT64)valueData.varInt ;
         }
      }
      break ;
   case CSV_TYPE_BOOL:
      rc = _value2str( pBuffer, size,
                      &pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _string2bool( valueData.varBool,
                         pBuffer,
                         size ) ;
      if ( rc )
      {
         rc = _string2int( valueData.varInt,
                           pBuffer,
                           size ) ;
         if ( rc )
         {
            rc = _string2long( valueData.varLong,
                               pBuffer,
                               size ) ;
            if ( rc )
            {
               rc = SDB_OK ;
               if ( fieldData.hasDefVal )
               {
                  valueData.varBool = fieldData.varBool ;
               }
               else
               {
                  valueData.type = CSV_TYPE_NULL ;
               }
            }
            else
            {
               if ( valueData.varLong != 0 )
               {
                  valueData.varBool = TRUE ;
               }
               else
               {
                  valueData.varBool = FALSE ;
               }
            }
         }
         else
         {
            if ( valueData.varInt != 0 )
            {
               valueData.varBool = TRUE ;
            }
            else
            {
               valueData.varBool = FALSE ;
            }
         }
      }
      break ;
   case CSV_TYPE_DOUBLE:
      rc = _value2str( pBuffer, size,
                      &pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _string2double( valueData.varDouble,
                           pBuffer,
                           size ) ;
      if ( rc )
      {
         rc = _string2int( valueData.varInt,
                           pBuffer,
                           size ) ;
         if ( rc )
         {
            rc = SDB_OK ;
            if ( fieldData.hasDefVal )
            {
               valueData.varDouble = fieldData.varDouble ;
            }
            else
            {
               valueData.type = CSV_TYPE_NULL ;
            }
         }
         else
         {
            valueData.varDouble = (FLOAT64)valueData.varInt ;
         }
      }
      break ;
   case CSV_TYPE_NULL:
      valueData.type = CSV_TYPE_NULL ;
      break ;
   case CSV_TYPE_STRING:
      rc = _value2str( pBuffer, size,
                      &pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      valueData.pVarString = pBuffer ;
      valueData.stringSize = size ;
      break ;
   case CSV_TYPE_TIMESTAMP:
      rc = _value2str( pBuffer, size,
                      &pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _string2timestamp( valueData.varTimestamp,
                              pBuffer,
                              size ) ;
      if ( rc )
      {
         rc = _string2timestamp2( valueData.varTimestamp,
                                  pBuffer,
                                  size ) ;
         if ( rc )
         {
            rc = SDB_OK ;
            if ( fieldData.hasDefVal )
            {
               valueData.varTimestamp = fieldData.varTimestamp ;
            }
            else
            {
               valueData.type = CSV_TYPE_NULL ;
            }
         }
      }
      break ;
   case CSV_TYPE_DATE:
      rc = _value2str( pBuffer, size,
                      &pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _string2date( valueData.varLong,
                         pBuffer,
                         size ) ;
      if ( rc )
      {
         rc = _string2date2( valueData.varLong,
                             pBuffer,
                             size ) ;
         if ( rc )
         {
            rc = SDB_OK ;
            if ( fieldData.hasDefVal )
            {
               valueData.varLong = fieldData.varLong ;
            }
            else
            {
               valueData.type = CSV_TYPE_NULL ;
            }
         }
      }
      break ;
   default:
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "unknow type %d", fieldData.type ) ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_parseNumber( CHAR *pBuffer, INT32 size,
                               CSV_TYPE &csvType,
                               INT32 *pVarInt,
                               INT64 *pVarLong,
                               FLOAT64 *pVarDouble )
{
   INT32 rc = SDB_OK ;
   CSV_TYPE type = CSV_TYPE_INT ;
   FLOAT64 n = 0 ;
   FLOAT64 sign = 1 ;
   FLOAT64 scale = 0 ;
   FLOAT64 subscale = 0 ;
   FLOAT64 signsubscale = 1 ;
   INT32 n1 = 0 ;
   INT64 n2 = 0 ;

   if ( 0 == size )
   {
      type = CSV_TYPE_NULL ;
      goto done ;
   }

   if ( *pBuffer != '+' && *pBuffer != '-' &&
        ( *pBuffer < '0' || *pBuffer >'9' ) )
   {
      type = CSV_TYPE_STRING ;
      goto done ;
   }

   /* Could use sscanf for this? */
   /* Has sign? */
   if ( '-' == *pBuffer )
   {
      sign = -1 ;
      --size ;
      ++pBuffer ;
   }
   else if ( '+' == *pBuffer )
   {
      sign = 1 ;
      --size ;
      ++pBuffer ;
   }

   while ( size > 0 && '0' == *pBuffer )
   {
      /* is zero */
      ++pBuffer ;
      --size ;
   }

   if ( size > 0 && *pBuffer >= '1' && *pBuffer <= '9' )
   {
      do
      {
         n  = ( n  * 10.0 ) + ( *pBuffer - '0' ) ;   
         n1 = ( n1 * 10 )   + ( *pBuffer - '0' ) ;
         n2 = ( n2 * 10 )   + ( *pBuffer - '0' ) ;
         --size ;
         ++pBuffer ;
         if ( (INT64)n1 != n2 )
         {
            type = CSV_TYPE_LONG ;
         }
      }
      while ( size > 0 && *pBuffer >= '0' && *pBuffer <= '9' ) ;
   }

   if ( size > 0 && *pBuffer == '.' &&
        pBuffer[1] >= '0' && pBuffer[1] <= '9' )
   {
      type = CSV_TYPE_DOUBLE ;
      --size ;
      ++pBuffer ;
      while ( size > 0 && *pBuffer >= '0' && *pBuffer <= '9' )
      {
         n = ( n ) + ( *pBuffer - '0' ) / pow( 10.0, ++scale ) ;
         --size ;
         ++pBuffer ;
      }
   }

   if ( size > 0 && ( *pBuffer == 'e' || *pBuffer == 'E' ) )
   {
      --size ;
      ++pBuffer ;
      if ( size > 0 && '+' == *pBuffer )
      {
         --size ;
         ++pBuffer ;
         signsubscale = 1 ;
      }
      else if ( size > 0 && '-' == *pBuffer )
      {
         type = CSV_TYPE_DOUBLE ;
         --size ;
         ++pBuffer;
         signsubscale = -1 ;
      }
      while ( size > 0 && *pBuffer >= '0' && *pBuffer <= '9' )
      {
         subscale = ( subscale * 10 ) + ( *pBuffer - '0' ) ;
         --size ;
         ++pBuffer ;
      }
   }

   if ( size == 0 )
   {
      if ( CSV_TYPE_DOUBLE == type )
      {
         n = sign * n * pow ( 10.0, ( subscale * signsubscale * 1.0 ) ) ;
      }
      else if ( CSV_TYPE_LONG == type )
      {
         if ( 0 != subscale )
         {
            n2 = (INT64)( sign * n2 * pow( 10.0, subscale * 1.00 ) ) ;
         }
         else
         {
            n2 = ( ( (INT64) sign ) * n2 ) ;
         }
      }
      else if ( CSV_TYPE_INT == type )
      {
          n1 = (INT32)( sign * n1 * pow( 10.0, subscale * 1.00 ) ) ;
          n2 = (INT64)( sign * n2 * pow( 10.0, subscale * 1.00 ) ) ;
          if ( (INT64)n1 != n2 )
          {
             type = CSV_TYPE_LONG ;
          }
      }
   }
   else
   {
      type = CSV_TYPE_STRING ;
   }
done:
   csvType = type ;
   if ( pVarInt )
   {
      (*pVarInt) = n1 ;
   }
   if ( pVarLong )
   {
      (*pVarLong) = n2 ;
   }
   if( pVarDouble )
   {
      (*pVarDouble) = n ;
   }
   return rc ;
}

CHAR *csvParser::_findSpace( CHAR *pBuffer, INT32 &size )
{
   while( size > 0 )
   {
      if ( (*pBuffer) == CSV_STR_SPACE || (*pBuffer) == CSV_STR_TABLE )
      {
         return pBuffer ;
      }
      ++pBuffer ;
      --size ;
   }
   return NULL ;
}

CHAR *csvParser::_skipSpace( CHAR *pBuffer, INT32 &size )
{
   while( size > 0 &&
          ( (*pBuffer) == CSV_STR_SPACE || (*pBuffer) == CSV_STR_TABLE ) )
   {
      ++pBuffer ;
      --size ;
   }
   return pBuffer ;
}

INT32 csvParser::_field2str( CHAR *pBuffer, INT32 size,
                             CHAR **ppOutBuf, INT32 &newSize )
{
   INT32 rc = SDB_OK ;
   CHAR *pNewBuffer = NULL ;

   if ( size == 0 )
   {
      *ppOutBuf = NULL ;
      newSize = 0 ;
      goto done ;
   }

   pNewBuffer = (CHAR *)SDB_OSS_MALLOC( size + 1 ) ;
   if ( !pNewBuffer )
   {
      PD_LOG ( PDERROR, "Failed to allocate memory for %d bytes",
               size ) ;
      rc = SDB_OOM ;
      goto error ;
   }
   ossMemset( pNewBuffer, 0, size + 1 ) ;

   if ( size > 1 &&
        ( ( _isHeaderline && pBuffer[0] == _delChar &&
            pBuffer[size-1] == _delChar ) ||
          ( !_isHeaderline && pBuffer[0] == CSV_STR_QUOTES &&
            pBuffer[size-1] == CSV_STR_QUOTES ) ) )
   {
      // "xxxx"
      size -= 2 ;
      ++pBuffer ;
      newSize = size ;
      for ( INT32 i = 0, k = 0; i < size; ++i, ++k )
      {
         if ( ( _isHeaderline && pBuffer[i] == _delChar ) ||
              ( !_isHeaderline && pBuffer[i] == CSV_STR_SLASH ) )
         {
            ++i ;
            --newSize ;
         }
         else
         {
            pNewBuffer[k] = pBuffer[i] ;
         }
      }
   }
   else
   {
      ossMemcpy( pNewBuffer, pBuffer, size ) ;
      newSize = size ;
   }

   *ppOutBuf = pNewBuffer ;

done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_value2str( CHAR *pBuffer, INT32 size,
                             CHAR **ppOutBuf, INT32 &newSize )
{
   INT32 rc = SDB_OK ;
   if ( size > 1 &&
        pBuffer[0] == _delChar && pBuffer[size-1] == _delChar )
   {
      // "xxxx"
      size -= 2 ;
      ++pBuffer ;
      for ( INT32 i = 0; i < size - 1; ++i )
      {
         if ( pBuffer[i] == _delChar )
         {
            if( pBuffer[i+1] == _delChar )
            {
               ossMemmove( pBuffer + i, pBuffer + i + 1, size - i - 1 ) ;
               --size ;
            }
            else
            {
               rc = SDB_INVALIDARG ;
               PD_LOG ( PDERROR, "CSV format error, only one side of \
the field appears delChar, rc = %d", rc ) ;
               goto error ;
            }
         }
      }
   }
   *ppOutBuf = pBuffer ;
   newSize = size ;
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_parseField( _fieldData &fieldData, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   INT32 fieldSize   = 0 ;
   INT32 typeSize    = 0 ;
   INT32 defaultSize = 0 ;
   INT32 valueSize   = 0 ;
   INT32 unreadSize  = 0 ;
   INT32 typeSum    = sizeof( _pCSVTYPESTR ) / sizeof( _pCSVTYPESTR[0] ) ;
   CHAR *pField   = NULL ;
   CHAR *pType    = NULL ;
   CHAR *pDefault = NULL ;
   CHAR *pValue   = NULL ;

   fieldData.pVarString = NULL ;
   fieldData.pField = NULL ;

   pField = pBuffer ;
   unreadSize = size ;
   pType = _findSpace( pBuffer, unreadSize ) ;
   if ( pType )
   {
      // field [space] xxx
      fieldSize = pType - pBuffer ;
      ( *pType ) = '\0' ;
      ++pType ;
      unreadSize = size - ( pType - pBuffer ) ;
      pType = _skipSpace( pType, unreadSize ) ;
      pDefault = _findSpace( pType, unreadSize ) ;
      if ( pDefault )
      {
         // field [space] type [space] xxx
         typeSize = pDefault - pType ;
         ( *pDefault ) = '\0' ;
         ++pDefault ;
         unreadSize = size - ( pDefault - pBuffer ) ;
         pDefault = _skipSpace( pDefault, unreadSize ) ;
         pValue = _findSpace( pDefault, unreadSize ) ;
         if ( pValue )
         {
            // field [space] type [space] xxxx [space] xxx
            defaultSize = pValue - pDefault ;
            ( *pValue ) = '\0' ;
            ++pValue ;
            unreadSize = size - ( pValue - pBuffer ) ;
            pValue = _skipSpace( pValue, unreadSize ) ;
            valueSize = unreadSize ;
            for ( INT32 i = 0; i < typeSum; ++i )
            {
               if ( typeSize == _CSVTYPESTRSIZE[ i ] &&
                    ossStrncasecmp( pType, _pCSVTYPESTR[ i ],
                                    _CSVTYPESTRSIZE[ i ] ) == 0 )
               {
                  if ( defaultSize == CSV_STR_DEFAULT_SIZE &&
                       ossStrncasecmp( pDefault, CSV_STR_DEFAULT,
                                       CSV_STR_DEFAULT_SIZE ) == 0 )
                  {
                     // field [space] type [space] default [space] xxx
                     fieldData.type = (CSV_TYPE)_CSVTYPENUM[ i ] ;
                     fieldData.hasDefVal = TRUE ;
                     switch( fieldData.type )
                     {
                     case CSV_TYPE_INT:
                        rc = _string2int( fieldData.varInt,
                                          pValue,
                                          valueSize ) ;
                        if ( rc )
                        {
                           rc = SDB_INVALIDARG ;
                           PD_LOG ( PDERROR,
                                    "The default value %.s is not of int type",
                                    valueSize, pValue ) ;
                           goto error ;
                        }
                        break ;
                     case CSV_TYPE_LONG:
                        rc = _string2long( fieldData.varLong,
                                           pValue,
                                           valueSize ) ;
                        if ( rc )
                        {
                           rc = SDB_INVALIDARG ;
                           PD_LOG ( PDERROR,
                                    "The default value %.s is not of long type",
                                    valueSize, pValue ) ;
                           goto error ;
                        }
                        break ;
                     case CSV_TYPE_BOOL:
                        rc = _string2bool( fieldData.varBool,
                                           pValue,
                                           valueSize ) ;
                        if ( rc )
                        {
                           rc = SDB_INVALIDARG ;
                           PD_LOG ( PDERROR,
                                    "The default value %.s is not of bool type",
                                    valueSize, pValue ) ;
                           goto error ;
                        }
                        break ;
                     case CSV_TYPE_DOUBLE:
                        rc = _string2double( fieldData.varDouble,
                                             pValue,
                                             valueSize ) ;
                        if ( rc )
                        {
                           rc = SDB_INVALIDARG ;
                           PD_LOG ( PDERROR,
                                    "The default value %.s is not of double type",
                                    valueSize, pValue ) ;
                           goto error ;
                        }
                        break ;
                     case CSV_TYPE_NULL:
                        rc = SDB_INVALIDARG ;
                        PD_LOG ( PDERROR,
                                 "The null type has no default value" ) ;
                        goto error ;
                     case CSV_TYPE_STRING:
                        rc = _field2str( pValue, valueSize,
                                        &pValue, valueSize ) ;
                        if ( rc )
                        {
                           goto error ;
                        }
                        fieldData.pVarString = pValue ;
                        fieldData.stringSize = valueSize ;
                        break ;
                     case CSV_TYPE_TIMESTAMP:
                        rc = _field2str( pValue, valueSize,
                                        &pValue, valueSize ) ;
                        if ( rc )
                        {
                           goto error ;
                        }
                        rc = _string2timestamp( fieldData.varTimestamp,
                                                pValue,
                                                valueSize ) ;
                        if ( rc )
                        {
                           rc = SDB_INVALIDARG ;
                           PD_LOG ( PDERROR,
                                    "The default value %.s is not of timestamp type",
                                    valueSize, pValue ) ;
                           goto error ;
                        }
                        break ;
                     case CSV_TYPE_DATE:
                        rc = _field2str( pValue, valueSize,
                                        &pValue, valueSize ) ;
                        if ( rc )
                        {
                           goto error ;
                        }
                        rc = _string2date( fieldData.varLong,
                                           pValue,
                                           valueSize ) ;
                        if ( rc )
                        {
                           rc = SDB_INVALIDARG ;
                           PD_LOG ( PDERROR,
                                    "The default value %.s is not of date type",
                                    valueSize, pValue ) ;
                           goto error ;
                        }
                        break ;
                     default:
                        rc = SDB_INVALIDARG ;
                        PD_LOG ( PDERROR, "unknow type %d", fieldData.type ) ;
                        goto error ;
                     }
                     goto finish ;
                  }
                  else
                  {
                     rc = SDB_INVALIDARG ;
                     PD_LOG ( PDERROR, "CSV header error, \
the format is:  field [type] [default <default value>]" ) ;
                     goto error ;
                  }
               }
            }
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "unknow field %s type %s", pField, pType ) ;
            goto error ;
         }
         else
         {
            // field [space] type [space] xxxxx
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "CSV header error, \
the format is:  field [type] [default <default value>]" ) ;
            goto error ;
         }
      }
      else
      {
         // field [space] type
         typeSize = size - ( pType - pBuffer ) ;
         for ( INT32 i = 0; i < typeSum; ++i )
         {
            if ( typeSize == _CSVTYPESTRSIZE[ i ] &&
                 ossStrncasecmp( pType, _pCSVTYPESTR[ i ],
                                 _CSVTYPESTRSIZE[ i ] ) == 0 )
            {
               fieldData.type = (CSV_TYPE)_CSVTYPENUM[ i ] ;
               fieldData.hasDefVal = FALSE ;
               goto finish ;
            }
         }
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "unknow type %s", pType ) ;
         goto error ;
      }
   }
   else
   {
      fieldSize = size ;
      fieldData.type = CSV_TYPE_AUTO ;
      goto finish ;
   }

finish:
   rc = _field2str( pField, fieldSize,
                   &pField, fieldSize ) ;
   if ( rc )
   {
      goto error ;
   }

done:
   fieldData.pField = pField ;
   return rc ;
error:
   SAFE_OSS_FREE( fieldData.pVarString ) ;
   SAFE_OSS_FREE( fieldData.pField ) ;
   goto done ;
}

INT32 csvParser::_string2int( INT32 &value, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   CSV_TYPE csvType = CSV_TYPE_INT ;
   rc = _parseNumber( pBuffer, size, csvType, &value ) ;
   if ( rc )
   {
      goto error ;
   }
   if ( csvType != CSV_TYPE_INT )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}
   
INT32 csvParser::_string2long( INT64 &value, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   CSV_TYPE csvType = CSV_TYPE_INT ;
   rc = _parseNumber( pBuffer, size, csvType, NULL, &value ) ;
   if ( rc )
   {
      goto error ;
   }
   if ( csvType != CSV_TYPE_LONG )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_string2bool( BOOLEAN &value, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   if ( size == CSV_STR_TRUE_SIZE &&
        ossStrncasecmp( pBuffer, CSV_STR_TRUE, CSV_STR_TRUE_SIZE ) == 0 )
   {
      value = TRUE ;
      goto done ;
   }
   else if ( size == CSV_STR_FALSE_SIZE &&
             ossStrncasecmp( pBuffer, CSV_STR_FALSE,
                             CSV_STR_FALSE_SIZE ) == 0 )
   {
      value = FALSE ;
      goto done ;
   }
   else
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_string2double( FLOAT64 &value, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   CSV_TYPE csvType = CSV_TYPE_INT ;
   rc = _parseNumber( pBuffer, size, csvType, NULL, NULL, &value ) ;
   if ( rc )
   {
      goto error ;
   }
   if ( csvType != CSV_TYPE_DOUBLE )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_string2timestamp( _csvTimestamp &value, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   struct tm t ;
   /* date and timestamp */
   INT32 year   = 0 ;
   INT32 month  = 0 ;
   INT32 day    = 0 ;
   INT32 hour   = 0 ;
   INT32 minute = 0 ;
   INT32 second = 0 ;
   INT32 micros = 0 ;
   time_t timep ;
   memset ( &t, 0, sizeof(t) ) ;
   /* for timestamp type, we provide yyyy-mm-dd-hh.mm.ss.uuuuuu */
   if ( !sscanf ( pBuffer,
                  TIME_FORMAT,
                  &year   ,
                  &month  ,
                  &day    ,
                  &hour   ,
                  &minute ,
                  &second ,
                  &micros ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   --month ;
   /* sanity check for years */
   if( year    >     INT32_LAST_YEAR   ||
       year    <     RELATIVE_YEAR     ||
       month   >=    RELATIVE_MOD      || //[0,11]
       month   <     0                 ||
       day     >     RELATIVE_DAY      || //[1,31]
       day     <=    0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( ( year   == INT32_LAST_YEAR &&
          month  >  0 ) ||
        ( year   == INT32_LAST_YEAR &&
          month  == 0 &&
          day    >= 18 ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if( hour    >=    RELATIVE_HOUR     || //[0,23]
       hour    <     0                 ||
       minute  >=    RELATIVE_MIN_SEC  || //[0,59]
       minute  <     0                 ||
       second  >=    RELATIVE_MIN_SEC  || //[0,59]
       second  <     0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   year -= RELATIVE_YEAR ;

   /* construct tm */
   t.tm_year  = year   ;
   t.tm_mon   = month  ;
   t.tm_mday  = day    ;
   t.tm_hour  = hour   ;
   t.tm_min   = minute ;
   t.tm_sec   = second ;

   /* create integer time representation */
   timep = mktime( &t ) ;
   if( !timep )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   value.t = (INT32)timep ;
   value.i = micros ;
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_string2timestamp2( _csvTimestamp &value,
                                     CHAR *pBuffer,
                                     INT32 size )
{
   INT32 rc = SDB_OK ;
   INT32 valueInt = 0 ;
   INT64 varLong = 0 ;
   INT64 temp1 = 0 ;
   INT64 temp2 = 0 ;

   rc = _string2long( varLong, pBuffer, size ) ;
   if ( rc )
   {
      rc = _string2int( valueInt, pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      varLong = (INT64)valueInt ;
   }
   temp1 = varLong / 1000 ;
   temp2 = varLong - ( temp1 * 1000 ) ;

   if ( varLong < TIME_MIX_NUM )
   {
      PD_LOG ( PDERROR, "The time stamp %lld is greater than %d000",
               varLong, TIME_MIX_NUM ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( ( temp1 > TIME_MAX_NUM ) ||
        ( ( temp1 == TIME_MAX_NUM ) && temp2 > 0 ) )
   {
      PD_LOG ( PDERROR, "The time stamp %lld is greater than %d000",
               varLong, TIME_MAX_NUM ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   value.t = (INT32)temp1 ;
   value.i = (INT32)temp2 ;
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_string2date( INT64 &value, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   struct tm t ;
   /* date and timestamp */
   INT32 year   = 0 ;
   INT32 month  = 0 ;
   INT32 day    = 0 ;
   time_t timep ;
   memset ( &t, 0, sizeof(t) ) ;
   /* for timestamp type, we provide yyyy-mm-dd-hh.mm.ss.uuuuuu */
   if ( !sscanf ( pBuffer,
                  DATE_FORMAT,
                  &year   ,
                  &month  ,
                  &day ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   --month ;
   /* sanity check for years */
   if( year    >     INT32_LAST_YEAR   ||
       year    <     RELATIVE_YEAR     ||
       month   >=    RELATIVE_MOD      || //[0,11]
       month   <     0                 ||
       day     >     RELATIVE_DAY      || //[1,31]
       day     <=    0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( ( year   == INT32_LAST_YEAR &&
          month  >  0 ) ||
        ( year   == INT32_LAST_YEAR &&
          month  == 0 &&
          day    >= 18 ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   year -= RELATIVE_YEAR ;

   /* construct tm */
   t.tm_year  = year   ;
   t.tm_mon   = month  ;
   t.tm_mday  = day    ;
   t.tm_hour  = 0 ;
   t.tm_min   = 0 ;
   t.tm_sec   = 0 ;

   /* create integer time representation */
   timep = mktime( &t ) ;
   if( !timep )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   value = ( ( (INT64)timep ) * 1000 ) ;
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_string2date2( INT64 &value, CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   INT32 valueInt = 0 ;

   rc = _string2long( value, pBuffer, size ) ;
   if ( rc )
   {
      rc = _string2int( valueInt, pBuffer, size ) ;
      if ( rc )
      {
         goto error ;
      }
      value = (INT64)valueInt ;
   }

   if ( value < TIME_MIX_NUM )
   {
      PD_LOG ( PDERROR, "The time stamp %lld is greater than %d",
               value, TIME_MIX_NUM ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( value > TIME_MAX_NUM )
   {
      PD_LOG ( PDERROR, "The time stamp %lld is greater than %d",
               value, TIME_MAX_NUM ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   value *= 1000 ;

done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_string2null( CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   if ( size == CSV_STR_NULL_SIZE &&
        ossStrncasecmp( pBuffer, CSV_STR_NULL,
                        CSV_STR_NULL_SIZE ) == 0 )
   {
      goto done ;
   }
   else
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

csvParser::csvParser() : _addField(FALSE),
                         _completion(FALSE),
                         _delChar(0),
                         _delField(0),
                         _delRecord(0)
                         
                         
{
}

csvParser::~csvParser()
{
   std::vector<_fieldData *>::iterator it ;
   _fieldData *pFieldData = NULL ;

   for( it = _vField.begin(); it != _vField.end(); ++it )
   {
      pFieldData = *it ;
      if ( pFieldData )
      {
         SAFE_OSS_FREE( pFieldData->pField ) ;
         if ( pFieldData->type == CSV_TYPE_STRING )
         {
            SAFE_OSS_FREE ( pFieldData->pVarString ) ;
         }
         SAFE_OSS_DELETE( pFieldData ) ;
      }
   }
}

INT32 csvParser::init( BOOLEAN autoAddField,
                       BOOLEAN autoCompletion,
                       BOOLEAN isHeaderline,
                       CHAR delChar,
                       CHAR delField,
                       CHAR delRecord )
{
   INT32 rc = SDB_OK ;

   /*if ( isHeaderline  )
   {
      if( ( delChar == CSV_STR_SPACE || delChar == CSV_STR_TABLE ) ||
          ( delField == CSV_STR_SPACE || delField == CSV_STR_TABLE ) ||
          ( delRecord == CSV_STR_SPACE || delRecord == CSV_STR_TABLE ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "when the fields are the first line of the file, \
can not specify delchar, delfield,delrecord as 0x20 or 0x09" ) ;
         goto error ;
      }
   }*/
   if ( delChar == delField )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delchar does not like delfield" ) ;
      goto error ;
   }
   if ( delChar == delRecord )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delchar does not like delrecord" ) ;
      goto error ;
   }
   if ( delField == delRecord )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delfield does not like delrecord" ) ;
      goto error ;
   }

   _addField     = autoAddField ;
   _completion   = autoCompletion ;
   _isHeaderline = isHeaderline ;
   _delChar      = delChar ;
   _delField     = delField ;
   _delRecord    = delRecord ;

done:
   return rc ;
error:
   goto done ;
}


INT32 csvParser::parseHeader( CHAR *pHeader, INT32 size )
{
   INT32   rc         = SDB_OK ;
   INT32   tempRc     = SDB_OK ;
   INT32   fieldSize  = 0 ;
   BOOLEAN isString   = FALSE;
   CHAR   *pCursor    = pHeader ;
   CHAR   *leftField  = pHeader ;
   _fieldData *pFieldData = NULL ;

   do
   {
      if ( 0 == size )
      {
         if ( !isString )
         {
            fieldSize = pCursor - leftField ;
            leftField = _trim( leftField, fieldSize ) ;
            if ( fieldSize == 0 )
            {
               if ( _isHeaderline )
               {
                  PD_LOG ( PDERROR, "The field can not be an empty string, \
if need the space string field, please use delchar" ) ;
               }
               else
               {
                  PD_LOG ( PDERROR, "The field can not be an empty string, \
if need the space string field, please use \"\"" ) ;
               }
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            else
            {
               pFieldData = SDB_OSS_NEW _fieldData() ;
               if ( !pFieldData )
               {
                  PD_LOG ( PDERROR, "Failed to allocate memory" ) ;
                  rc = SDB_OOM ;
                  goto error ;
               }
               leftField[ fieldSize ] = 0 ;
               rc = _parseField( (*pFieldData), leftField, fieldSize ) ;
               if ( rc )
               {
                  goto error ;
               }
               _vField.push_back ( pFieldData ) ;
            }
         }
         break ;
      }

      if ( ( _isHeaderline && _delChar == *pCursor ) ||
           ( !_isHeaderline && CSV_STR_QUOTES == *pCursor ) )
      {
         --size ;
         ++pCursor ;
         isString = !isString ;
      }
      else if ( !isString &&
                ( ( _isHeaderline && ( _delField == *pCursor ||
                                       _delRecord == *pCursor ) ) ||
                  ( !_isHeaderline && ( CSV_STR_COMMA == *pCursor ||
                                        CSV_STR_LF == *pCursor ) ) ) )
      {
         fieldSize = pCursor - leftField ;
         leftField = _trim( leftField, fieldSize ) ;
         if ( ( _isHeaderline && _delRecord == *pCursor ) ||
              ( !_isHeaderline && CSV_STR_LF == *pCursor ) )
         {
            tempRc = SDB_UTIL_CSV_FIELD_END ;
         }
         if ( fieldSize == 0 )
         {
            if ( _isHeaderline )
            {
               PD_LOG ( PDERROR, "The field can not be an empty string, \
if need the space string field, please use delchar" ) ;
            }
            else
            {
               PD_LOG ( PDERROR, "The field can not be an empty string, \
if need the space string field, please use \"" ) ;
            }
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         else
         {
            pFieldData = SDB_OSS_NEW _fieldData() ;
            if ( !pFieldData )
            {
               PD_LOG ( PDERROR, "Failed to allocate memory" ) ;
               rc = SDB_OOM ;
               goto error ;
            }
            leftField[ fieldSize ] = 0 ;
            rc = _parseField( (*pFieldData), leftField, fieldSize ) ;
            if ( rc )
            {
               goto error ;
            }
            _vField.push_back ( pFieldData ) ;
         }

         if ( tempRc == SDB_UTIL_CSV_FIELD_END )
         {
            break ;
         }
         else
         {
            --size ;
            ++pCursor ;
            leftField = pCursor ;
         }
      }
      else
      {
         --size ;
         ++pCursor ;
      }
   }while ( TRUE ) ;

done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_appendBson( void *bsonObj, CSV_TYPE csvType,
                              const CHAR *pKey, void *pValue, INT32 valueSize )
{
   INT32 rc = SDB_OK ;
   CHAR *pBuffer = NULL ;
   bson *pObj = (bson *)bsonObj ;
   switch( csvType )
   {
   case CSV_TYPE_INT:
      bson_append_int( pObj, pKey, *((INT32 *)pValue) ) ;
      break ;
   case CSV_TYPE_LONG:
      bson_append_long( pObj, pKey, *((INT64 *)pValue) ) ;
      break ;
   case CSV_TYPE_BOOL:
      bson_append_bool( pObj, pKey, *((BOOLEAN *)pValue) ) ;
      break ;
   case CSV_TYPE_DOUBLE:
      bson_append_double( pObj, pKey, *((FLOAT64 *)pValue) ) ;
      break ;
   case CSV_TYPE_STRING:
      pBuffer = (CHAR *)pValue ;
      rc = _value2str( pBuffer, valueSize,
                      &pBuffer, valueSize ) ;
      if ( rc )
      {
         goto error ;
      }
      bson_append_string_n( pObj, pKey, pBuffer, valueSize ) ;
      break ;
   case CSV_TYPE_TIMESTAMP:
      bson_append_timestamp( pObj, pKey, (bson_timestamp_t *)pValue ) ;
      break ;
   case CSV_TYPE_DATE:
      bson_append_date( pObj, pKey, *((bson_date_t *)pValue) ) ;
      break ;
   case CSV_TYPE_NULL:
      bson_append_null( pObj, pKey ) ;
      break ;
   default:
      break ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::_appendBson( void *bsonObj, _fieldData *pFieldData )
{
   INT32 rc = SDB_OK ;
   bson *pObj = (bson *)bsonObj ;
   switch( pFieldData->type )
   {
   case CSV_TYPE_INT:
      bson_append_int( pObj, pFieldData->pField, pFieldData->varInt ) ;
      break ;
   case CSV_TYPE_LONG:
      bson_append_long( pObj, pFieldData->pField, pFieldData->varLong ) ;
      break ;
   case CSV_TYPE_BOOL:
      bson_append_bool( pObj, pFieldData->pField, pFieldData->varBool ) ;
      break ;
   case CSV_TYPE_DOUBLE:
      bson_append_double( pObj, pFieldData->pField, pFieldData->varDouble ) ;
      break ;
   case CSV_TYPE_STRING:
      bson_append_string_n( pObj, pFieldData->pField,
                            pFieldData->pVarString, pFieldData->stringSize ) ;
      break ;
   case CSV_TYPE_TIMESTAMP:
      bson_append_timestamp2( pObj, pFieldData->pField,
                              (pFieldData->varTimestamp).t,
                              (pFieldData->varTimestamp).i ) ;
      break ;
   case CSV_TYPE_DATE:
      bson_append_date( pObj, pFieldData->pField, pFieldData->varLong ) ;
      break ;
   case CSV_TYPE_NULL:
      bson_append_null( pObj, pFieldData->pField ) ;
      break ;
   default:
      break ;
   }
   return rc ;
}

INT32 csvParser::_appendBson( void *bsonObj, const CHAR *pKey,
                              _valueData *pValueData )
{
   INT32 rc = SDB_OK ;
   bson *pObj = (bson *)bsonObj ;
   switch( pValueData->type )
   {
   case CSV_TYPE_INT:
      bson_append_int( pObj, pKey, pValueData->varInt ) ;
      break ;
   case CSV_TYPE_LONG:
      bson_append_long( pObj, pKey, pValueData->varLong ) ;
      break ;
   case CSV_TYPE_BOOL:
      bson_append_bool( pObj, pKey, pValueData->varBool ) ;
      break ;
   case CSV_TYPE_DOUBLE:
      bson_append_double( pObj, pKey, pValueData->varDouble ) ;
      break ;
   case CSV_TYPE_STRING:
      rc = _value2str( pValueData->pVarString, pValueData->stringSize,
                      &pValueData->pVarString, pValueData->stringSize ) ;
      if ( rc )
      {
         goto error ;
      }
      bson_append_string_n( pObj, pKey,
                            pValueData->pVarString, pValueData->stringSize ) ;
      break ;
   case CSV_TYPE_TIMESTAMP:
      bson_append_timestamp2( pObj, pKey,
                              (pValueData->varTimestamp).t,
                              (pValueData->varTimestamp).i ) ;
      break ;
   case CSV_TYPE_DATE:
      bson_append_date( pObj, pKey, pValueData->varLong ) ;
      break ;
   case CSV_TYPE_NULL:
      bson_append_null( pObj, pKey ) ;
      break ;
   default:
      break ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 csvParser::csv2bson( CHAR *pBuffer, INT32 size, CHAR **ppRawbson )
{
   INT32   rc           = SDB_OK ;
   INT32   bsonsize     = 0 ;
   INT32   fieldSize    = 0 ;
   INT32   fieldNum     = 0 ;
   INT32   autoFieldNum = 1 ;
   //field sum
   INT32   fieldSumNum  = _vField.size() ;
   BOOLEAN isString     = FALSE;
   CHAR   *pCursor      = pBuffer ;
   CHAR   *leftField    = pBuffer ;
   CHAR   *pBsonBuf     = NULL ;
   CHAR    fieldName[CSV_STR_FIELD_MAX_SIZE] ;
   _valueData valueData ;
   bson obj ;
   bson_init ( &obj ) ;

   do
   {
      if ( 0 == size )
      {
         if ( !isString )
         {
            fieldSize = pCursor - leftField ;
            leftField = _trim( leftField, fieldSize ) ;
            if ( fieldSize == 0 )
            {
               //NULL or default value
               if ( fieldSumNum <= fieldNum )
               {
                  if ( _addField )
                  {
                     ossMemset ( fieldName, 0, CSV_STR_FIELD_MAX_SIZE ) ;
                     ossSnprintf ( fieldName,
                                   CSV_STR_FIELD_MAX_SIZE,
                                   CSV_STR_FIELD "%d",
                                   autoFieldNum ) ;
                     rc = _appendBson( &obj, CSV_TYPE_NULL,
                                       fieldName, NULL, 0 ) ;
                     if ( rc )
                     {
                        goto error ;
                     }
                     ++autoFieldNum ;
                  }
               }
               else
               {
                  if( _vField.at(fieldNum)->hasDefVal )
                  {
                     rc = _appendBson( &obj, _vField.at(fieldNum) ) ;
                     if ( rc )
                     {
                        goto error ;
                     }
                  }
                  else
                  {
                     rc = _appendBson( &obj, CSV_TYPE_NULL,
                                       _vField.at(fieldNum)->pField, NULL, 0 ) ;
                     if ( rc )
                     {
                        goto error ;
                     }
                  }
               }
            }
            else
            {
               //NULL or default value
               if ( fieldSumNum <= fieldNum )
               {
                  if ( _addField )
                  {
                     ossMemset ( fieldName, 0, CSV_STR_FIELD_MAX_SIZE ) ;
                     ossSnprintf ( fieldName,
                                   CSV_STR_FIELD_MAX_SIZE,
                                   CSV_STR_FIELD "%d",
                                   autoFieldNum ) ;
                     rc = _parseValue( valueData, leftField, fieldSize ) ;
                     if ( rc )
                     {
                        goto error ;
                     }
                     rc = _appendBson( &obj, fieldName, &valueData ) ;
                     if ( rc )
                     {
                        goto error ;
                     }
                     ++autoFieldNum ;
                  }
               }
               else
               {
                  if ( _vField.at(fieldNum)->type == CSV_TYPE_AUTO )
                  {
                     rc = _parseValue( valueData, leftField, fieldSize ) ;
                     if ( rc )
                     {
                        goto error ;
                     }
                  }
                  else
                  {
                     rc = _parseValue( valueData, (*(_vField.at(fieldNum))),
                                       leftField, fieldSize ) ;
                     if ( rc )
                     {
                        goto error ;
                     }
                  }
                  rc = _appendBson( &obj, _vField.at(fieldNum)->pField,
                                    &valueData ) ;
                  if ( rc )
                  {
                     goto error ;
                  }
               }
            }
            ++fieldNum ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "CSV format error, only one side of \
the field appears delChar, rc = %d", rc ) ;
            goto error ;
         }
         break ;
      }

      if ( _delChar == *pCursor )
      {
         --size ;
         ++pCursor ;
         isString = !isString ;
      }
      else if ( !isString &&
                ( _delField == *pCursor || _delRecord == *pCursor ) )
      {
         fieldSize = pCursor - leftField ;
         leftField = _trim( leftField, fieldSize ) ;
         if ( fieldSize == 0 )
         {
            //NULL or default value
            if ( fieldSumNum <= fieldNum )
            {
               if ( _addField )
               {
                  ossMemset ( fieldName, 0, CSV_STR_FIELD_MAX_SIZE ) ;
                  ossSnprintf ( fieldName,
                                CSV_STR_FIELD_MAX_SIZE,
                                CSV_STR_FIELD "%d",
                                autoFieldNum ) ;
                  rc = _appendBson( &obj, CSV_TYPE_NULL, fieldName, NULL, 0 ) ;
                  if ( rc )
                  {
                     goto error ;
                  }
                  ++autoFieldNum ;
               }
            }
            else
            {
               if( _vField.at(fieldNum)->hasDefVal )
               {
                  rc = _appendBson( &obj, _vField.at(fieldNum) ) ;
                  if ( rc )
                  {
                     goto error ;
                  }
               }
               else
               {
                  rc = _appendBson( &obj, CSV_TYPE_NULL,
                                    _vField.at(fieldNum)->pField, NULL, 0 ) ;
                  if ( rc )
                  {
                     goto error ;
                  }
               }
            }
         }
         else
         {
            //NULL or default value
            if ( fieldSumNum <= fieldNum )
            {
               if ( _addField )
               {
                  ossMemset ( fieldName, 0, CSV_STR_FIELD_MAX_SIZE ) ;
                  ossSnprintf ( fieldName,
                                CSV_STR_FIELD_MAX_SIZE,
                                CSV_STR_FIELD "%d",
                                autoFieldNum ) ;
                  rc = _parseValue( valueData, leftField, fieldSize ) ;
                  if ( rc )
                  {
                     goto error ;
                  }
                  rc = _appendBson( &obj, fieldName, &valueData ) ;
                  if ( rc )
                  {
                     goto error ;
                  }
                  ++autoFieldNum ;
               }
            }
            else
            {
               if ( _vField.at(fieldNum)->type == CSV_TYPE_AUTO )
               {
                  rc = _parseValue( valueData, leftField, fieldSize ) ;
                  if ( rc )
                  {
                     goto error ;
                  }
               }
               else
               {
                  rc = _parseValue( valueData, (*(_vField.at(fieldNum))),
                                    leftField, fieldSize ) ;
                  if ( rc )
                  {
                     goto error ;
                  }
               }
               rc = _appendBson( &obj, _vField.at(fieldNum)->pField,
                                 &valueData ) ;
               if ( rc )
               {
                  goto error ;
               }
            }
         }
         ++fieldNum ;
         if ( _delRecord == *pCursor )
         {
            break ;
         }
         else
         {
            --size ;
            ++pCursor ;
            leftField = pCursor ;
         }
      }
      else
      {
         --size ;
         ++pCursor ;
      }
   }while ( TRUE ) ;

   if ( _completion )
   {
      for ( ; fieldNum < fieldSumNum; ++fieldNum )
      {
         if( _vField.at(fieldNum)->hasDefVal )
         {
            rc = _appendBson( &obj, _vField.at(fieldNum) ) ;
            if ( rc )
            {
               goto error ;
            }
         }
         else
         {
            rc = _appendBson( &obj, CSV_TYPE_NULL,
                              _vField.at(fieldNum)->pField, NULL, 0 ) ;
            if ( rc )
            {
               goto error ;
            }
         }
      }
   }

   bson_finish ( &obj ) ;
   bsonsize = *((INT32*)obj.data) ;
   if ( bsonsize < 0 )
   {
      PD_LOG ( PDERROR, "bson size error, %d bytes",
               bsonsize ) ;
      rc = SDB_OOM ;
      goto error ;
   }
   pBsonBuf = (CHAR*)SDB_OSS_MALLOC( bsonsize ) ;
   if ( !pBsonBuf )
   {
      PD_LOG ( PDERROR, "Failed to allocate memory for %d bytes",
               bsonsize + sizeof( unsigned ) ) ;
      rc = SDB_OOM ;
      goto error ;
   }
   memset ( pBsonBuf, 0, bsonsize ) ;
   memcpy ( pBsonBuf, obj.data, bsonsize ) ;
   *ppRawbson = pBsonBuf ;

done:
   bson_destroy ( &obj ) ;
   return rc ;
error:
   goto done ;
}

INT32 csvParser::csv2bson( CHAR *pBuffer, INT32 size, void *pbson )
{
   INT32 rc = SDB_OK ;
   CHAR *pBsonBuf = NULL ;
   bson *pObj = (bson *)pbson ;
   bson obj ;
   bson_init ( &obj ) ;
   rc = csv2bson( pBuffer, size, &pBsonBuf ) ;
   if ( rc )
   {
      goto error ;
   }
   obj.ownmem = 0 ;
   obj.data = NULL ;
   bson_init_finished_data ( &obj, pBsonBuf ) ;
   bson_copy ( pObj, &obj ) ;
   SAFE_OSS_FREE ( pBsonBuf ) ;
done:
   return rc ;
error:
   goto done ;
}