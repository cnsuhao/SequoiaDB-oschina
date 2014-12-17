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

   Source File Name = sptConvertor.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of Script component. This file contains structures for javascript
   engine wrapper

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/13/2013  YW Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptConvertor.hpp"
#include "pd.hpp"
#include "ossMem.hpp"
#include "utilStr.hpp"
#include "../client/base64c.h"
#include <boost/lexical_cast.hpp>

#define SPT_CONVERTOR_SPE_OBJSTART '$'
#define SPT_SPEOBJ_MINKEY "$minKey"
#define SPT_SPEOBJ_MAXKEY "$maxKey"
#define SPT_SPEOBJ_TIMESTAMP "$timestamp"
#define SPT_SPEOBJ_DATE "$date"
#define SPT_SPEOBJ_REGEX "$regex"
#define SPT_SPEOBJ_OPTION "$options"
#define SPT_SPEOBJ_BINARY "$binary"
#define SPT_SPEOBJ_TYPE "$type"
#define SPT_SPEOBJ_OID "$oid"

INT32 sptConvertor::toBson( JSObject *obj , bson **bs )
{
   INT32 rc = SDB_OK ;
   SDB_ASSERT( NULL != _cx && NULL != bs, "can not be NULL" ) ;

   *bs = bson_create() ;
   if ( NULL == *bs )
   {
      rc = SDB_OOM ;
      goto error ;
   }
   bson_init( *bs ) ;

   rc = _traverse( obj, *bs ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = bson_finish( *bs ) ;
   if ( rc )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

done:
   return rc ;
error:
   if ( NULL != *bs )
   {
      bson_dispose( *bs ) ;
      *bs = NULL ;
   }
   goto done ;
}

INT32 sptConvertor::toBson( JSObject *obj, bson *bs )
{
   INT32 rc = SDB_OK ;
   SDB_ASSERT( NULL != obj && NULL != bs, "can not be NULL" ) ;
   rc = _traverse( obj, bs ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = bson_finish( bs ) ;
   if ( rc )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 sptConvertor::_traverse( JSObject *obj , bson *bs )
{
   INT32 rc = SDB_OK ;
   JSIdArray *properties = NULL ;
   if ( NULL == obj )
   {
      goto done ;
   }

   properties = JS_Enumerate( _cx, obj ) ;
   if ( NULL == properties )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   for ( jsint i = 0; i < properties->length; i++ )
   {
      jsid id = properties->vector[i] ;
      jsval fieldName, fieldValue ;
      std::string name ;
      if ( !JS_IdToValue( _cx, id, &fieldName ))
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _toString( fieldName, name ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      if ( !JS_GetProperty( _cx, obj, name.c_str(), &fieldValue ))
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      _appendToBson( name, fieldValue, bs ) ;
   }
done:
   return rc ;
error:
   goto done ;
}

BOOLEAN sptConvertor::_addSpecialObj( JSObject *obj,
                                      const CHAR *key,
                                      bson *bs )
{
   BOOLEAN ret = TRUE ;
   INT32 rc = SDB_OK ;
   JSIdArray *properties = JS_Enumerate( _cx, obj ) ;
   if ( NULL == properties || 0 == properties->length )
   {
      goto error ;
   }

   {
   jsid id = properties->vector[0] ;
   jsval fieldName ;
   std::string name ;
   if ( !JS_IdToValue( _cx, id, &fieldName ))
   {
      goto error ;
   }

   rc = _toString( fieldName, name ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   if ( name.length() <= 1 )
   {
      goto error ;
   }

   if ( SPT_CONVERTOR_SPE_OBJSTART != name.at(0) )
   {
      goto error ;
   }

   if ( 0 == name.compare( SPT_SPEOBJ_MINKEY ) &&
        1 == properties->length )
   {
      jsval value ;
      if ( !_getProperty( obj, name.c_str(), JSTYPE_NUMBER, value ) )
      {
         goto error ;
      }

      bson_append_minkey( bs, key ) ;
   }
   else if ( 0 == name.compare(SPT_SPEOBJ_MAXKEY) &&
             1 == properties->length )
   {
      jsval value ;
      if ( !_getProperty( obj, name.c_str(), JSTYPE_NUMBER, value ) )
      {
         goto error ;
      }

      bson_append_maxkey( bs, key ) ;
   }
   else if ( 0 == name.compare( SPT_SPEOBJ_OID ) &&
             1 == properties->length )
   {
      std::string strValue ;
      jsval value ;
      if ( !_getProperty( obj, name.c_str(), JSTYPE_STRING, value ))
      {
         goto error ;
      }

      rc = _toString( value, strValue ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      if ( 24 != strValue.length() )
      {
         goto error ;
      }

      bson_oid_t oid ;
      bson_oid_from_string( &oid, strValue.c_str() ) ;
      bson_append_oid( bs, key, &oid ) ;
   }
   else if ( 0 == name.compare( SPT_SPEOBJ_TIMESTAMP ) &&
             1 == properties->length )
   {
      std::string strValue ;
      jsval value ;
      time_t tm ;
      UINT64 usec = 0 ;
      bson_timestamp_t btm ;
      if ( !_getProperty( obj, name.c_str(), JSTYPE_STRING, value ))
      {
         goto error ;
      }

      rc = _toString( value, strValue ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      if ( SDB_OK != engine::utilStr2TimeT( strValue.c_str(),
                                            tm,
                                            &usec ))
      {
         goto error ;
      }

      btm.t = tm;
      btm.i = usec ;
      bson_append_timestamp( bs, key, &btm ) ;
   }
   else if ( 0 == name.compare( SPT_SPEOBJ_DATE ) &&
             1 == properties->length )
   {
      std::string strValue ;
      jsval value ;
      UINT64 tm ;
      bson_date_t datet ;
      if ( !_getProperty( obj, name.c_str(), JSTYPE_STRING, value ))
      {
         goto error ;
      }

      rc = _toString( value, strValue ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      if ( SDB_OK != engine::utilStr2Date( strValue.c_str(),
                                           tm ) )
      {
         goto error ;
      }

      datet = tm ;
      bson_append_date( bs, key, datet ) ;
   }
   else if ( 0 == name.compare( SPT_SPEOBJ_REGEX ) &&
             2 == properties->length )
   {
      std::string optionName ;
      std::string strRegex, strOption ;
      jsval jsRegex, jsOption ;
      jsid optionid = properties->vector[1] ;
      jsval optionValName ;

      if ( !JS_IdToValue( _cx, optionid, &optionValName ))
      {
         goto error ;
      }

      rc = _toString( optionValName, optionName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      if ( 0 != optionName.compare( SPT_SPEOBJ_OPTION ) )
      {
         goto error ;
      }

      if ( !_getProperty( obj, name.c_str(),
                          JSTYPE_STRING, jsRegex ))
      {
         goto error ;
      }

      if ( !_getProperty( obj, optionName.c_str(),
                          JSTYPE_STRING, jsOption ))
      {
         goto error ;
      }

      rc = _toString( jsRegex, strRegex ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _toString( jsOption, strOption ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      bson_append_regex( bs, key, strRegex.c_str(), strOption.c_str() ) ;
   }
   else if ( 0 == name.compare( SPT_SPEOBJ_BINARY ) &&
             2 == properties->length )
   {
      std::string typeName ;
      std::string strBin, strType ;
      jsval jsBin, jsType ;
      jsid typeId = properties->vector[1] ;
      jsval typeValName ;
      CHAR *decode = NULL ;
      UINT32 decodeSize = 0 ;
      INT32 binType = 0 ;

      if ( !JS_IdToValue( _cx, typeId, &typeValName ))
      {
         goto error ;
      }

      rc = _toString( typeValName, typeName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      if ( 0 != typeName.compare( SPT_SPEOBJ_TYPE ) )
      {
         goto error ;
      }

      if ( !_getProperty( obj, name.c_str(),
                          JSTYPE_STRING, jsBin ))
      {
         goto error ;
      }

      if ( !_getProperty( obj, typeName.c_str(),
                          JSTYPE_STRING, jsType ))
      {
         goto error ;
      }

      rc = _toString( jsBin, strBin ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _toString( jsType, strType ) ;
      if ( SDB_OK != rc || strType.empty())
      {
         goto error ;
      }

      try
      {
         binType = boost::lexical_cast<INT32>( strType.c_str() ) ;
      }
      catch ( std::bad_cast &e )
      {
         PD_LOG( PDERROR, "bad type for binary:%s", strType.c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      decodeSize = getDeBase64Size( strBin.c_str() ) ;
      if ( decodeSize <= 1 )
      {
         PD_LOG( PDERROR, "invalid decode size:%d", decodeSize ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      decode = ( CHAR * )SDB_OSS_MALLOC( decodeSize ) ;
      if ( NULL == decode )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      if ( !base64Decode( strBin.c_str(), decode, decodeSize ) )
      {
         PD_LOG( PDERROR, "failed to decode base64 code" ) ;
         rc = SDB_INVALIDARG ;
         SDB_OSS_FREE( decode ) ;
         goto error ;
      }

      bson_append_binary( bs, key, binType,
                          decode, decodeSize - 1 ) ;
      SDB_OSS_FREE( decode ) ;

   }
   else
   {
      goto error ;
   }
   }

done:
   return ret ;
error:
   ret = FALSE ;
   goto done ;
}

INT32 sptConvertor::_appendToBson( const std::string &name,
                                   const jsval &val,
                                   bson *bs )
{
   INT32 rc = SDB_OK ;
   switch (JS_TypeOfValue( _cx, val ))
   {
      case JSTYPE_VOID :
      {
         bson_append_undefined( bs, name.c_str() ) ;
         break ;
      }
      case JSTYPE_NULL :
      {
         bson_append_null( bs, name.c_str() ) ;
         break ;
      }
      case JSTYPE_NUMBER :
      {
         if ( JSVAL_IS_INT( val ) )
         {
            INT32 iN = 0 ;
            rc = _toInt( val, iN ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            bson_append_int( bs, name.c_str(), iN ) ;
         }
         else
         {
            FLOAT64 fV = 0 ;
            rc = _toDouble( val, fV ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            bson_append_double( bs, name.c_str(), fV ) ;
         }
         break ;
      }
      case JSTYPE_STRING :
      {
         std::string str ;
         rc = _toString( val, str ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         bson_append_string( bs, name.c_str(), str.c_str() ) ;
         break ;
      }
      case JSTYPE_BOOLEAN :
      {
         BOOLEAN bL = TRUE ;
         rc = _toBoolean( val, bL ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         bson_append_bool( bs, name.c_str(), bL ) ;
         break ;
      }
      case JSTYPE_OBJECT :
      {
         if ( JSVAL_IS_NULL( val ) )
         {
            bson_append_null( bs, name.c_str() ) ;
         }
         else
         {
            JSObject *obj = JSVAL_TO_OBJECT( val ) ;
            if ( NULL == obj )
            {
               bson_append_null( bs, name.c_str() ) ;
            }
            else if ( !_addSpecialObj( obj, name.c_str(), bs ) )
            {
               bson *bsobj = NULL ;
               rc = toBson( obj, &bsobj ) ;
               if ( SDB_OK != rc )
               {
                  goto error ;
               }

               if ( JS_IsArrayObject( _cx, obj ) )
               {
                  bson_append_array( bs, name.c_str(), bsobj ) ;
               }
               else
               {
                  bson_append_bson( bs, name.c_str(), bsobj ) ;
               }

               bson_destroy( bsobj ) ;
            }
            else
            {
            }
         }
         break ;
      }
      case JSTYPE_FUNCTION :
      {
         std::string str ;
         rc = _toString( val, str ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         bson_append_code( bs, name.c_str(), str.c_str() ) ;
         break ;
      }
      default :
      {
         SDB_ASSERT( FALSE, "unexpected type" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   }
done:
   return rc ;
error:
   goto done ;
}

BOOLEAN sptConvertor::_getProperty( JSObject *obj,
                                    const CHAR *name,
                                    JSType type,
                                    jsval &val )
{
   if ( !JS_GetProperty( _cx, obj, name, &val ) )
   {
      return FALSE ;
   }
   else if ( type != JS_TypeOfValue( _cx, val ) )
   {
      return FALSE ;
   }
   else
   {
      return TRUE ;
   }
}

INT32 sptConvertor::toString( JSContext *cx,
                              const jsval &val,
                              std::string &str )
{
   INT32 rc = SDB_OK ;
   SDB_ASSERT( NULL != cx, "impossible" ) ;
   size_t len = 0 ;
   JSString *jsStr = JS_ValueToString( cx, val ) ;
   if ( NULL == jsStr )
   {
      goto done ;
   }
   len = JS_GetStringLength( jsStr ) ;
   if ( 0 == len )
   {
      goto done ;
   }
   else
   {
/*      size_t cLen = len * 6 + 1 ;
      const jschar *utf16 = JS_GetStringCharsZ( cx, jsStr ) ; ;
      utf8 = (CHAR *)SDB_OSS_MALLOC( cLen ) ;
      if ( NULL == utf8 )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      if ( !JS_EncodeCharacters( cx, utf16, len, utf8, &cLen ) )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      str.assign( utf8, cLen ) ;
*/

      CHAR *p = JS_EncodeString ( cx , jsStr ) ;
      if ( NULL != p )
      {
         str.assign( p ) ;
         free( p ) ;
      }
   }
done:
   return rc ;
}

INT32 sptConvertor::_toString( const jsval &val, std::string &str )
{
   return toString( _cx, val, str ) ;
}

INT32 sptConvertor::_toInt( const jsval &val, INT32 &iN )
{
   INT32 rc = SDB_OK ;
   int32 ip = 0 ;
   if ( !JS_ValueToInt32( _cx, val, &ip ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   iN = ip ;
done:
   return rc ;
error:
   goto done ;
}

INT32 sptConvertor::_toDouble( const jsval &val, FLOAT64 &fV )
{
   INT32 rc = SDB_OK ;
   jsdouble dp = 0 ;
   if ( !JS_ValueToNumber( _cx, val, &dp ))
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   fV = dp ;
done:
   return rc ;
error:
   goto done ;
}

INT32 sptConvertor::_toBoolean( const jsval &val, BOOLEAN &bL )
{
   INT32 rc = SDB_OK ;
   JSBool bp = TRUE ;
   if ( !JS_ValueToBoolean( _cx, val, &bp ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   bL = bp ;
done:
   return rc ;
error:
   goto done ;
}
