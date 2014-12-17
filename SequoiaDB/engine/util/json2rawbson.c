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

   Source File Name = json2rawbson.c

   Descriptive Name = JSON To Raw BSON

   When/how to use: this program may be used on binary and text-formatted
   versions of UTIL component. This file contains declare of json2rawbson. Note
   this function should NEVER be directly called other than fromjson.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "json2rawbson.h"
#if defined (SDB_ENGINE) || defined (SDB_CLIENT)
#include "../client/jstobs.h"
#else
#include "jstobs.h"
#endif
// This function should NEVER be directly called other than fromjson.cpp
// The reason to separate this function into different file is that
// client/jstobs.h uses bson structure, but fromjson.cpp uses bson namespace, so
// compiler will be confused if structure and namespace are the same. Thus we
// have to separate the interface to bson from interfrace to BSON
// This function will create a temporary bson structure and convert JSON to
// bson, and then allocate memory and copy the bson into the new memory block.
// This block is 4 bytes greater than bson size, for BSONObj::Holder obj, and
// this block will be sent to fromjson() function to convert into BSONObj.
CHAR * json2rawbson ( const CHAR *str )
{
   bson obj ;
   CHAR *p = NULL ;
   bson_init ( &obj ) ;
   if ( jsonToBson2 ( &obj, str, 0, 1 ) )
   {
      if ( obj.data )
      {
         INT32 bsonsize = *((INT32*)obj.data) ;
         p = (CHAR*)malloc ( bsonsize + sizeof(unsigned)) ;
         if ( p )
         {
            memset ( p, 0, bsonsize + sizeof(unsigned) ) ;
            memcpy ( &p[sizeof(unsigned)], obj.data, bsonsize ) ;
         }
      }
   }
   bson_destroy ( &obj ) ;
   return p ;
}
CHAR * json2rawcbson ( const CHAR *str )
{
   bson obj ;
   CHAR *p = NULL ;
   bson_init ( &obj ) ;
   if ( jsonToBson2 ( &obj, str, 0, 1 ) )
   {
      if ( obj.data )
      {
         INT32 bsonsize = *((INT32*)obj.data) ;
         if ( 0 < bsonsize )
         {
            p = (CHAR*)malloc ( bsonsize ) ;
            if ( p )
            {
               memset ( p, 0, bsonsize ) ;
               memcpy ( p, obj.data, bsonsize ) ;
            }
         }
      }
   }
   bson_destroy ( &obj ) ;
   return p ;
}
