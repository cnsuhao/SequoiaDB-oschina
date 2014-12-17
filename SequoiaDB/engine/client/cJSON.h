/*    Copyright 2012 SequoiaDB Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/*
  Copyright (c) 2009 Dave Gamble

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
*/

#ifndef cJSON__h
#define cJSON__h

#ifdef __cplusplus
extern "C"
{
#endif

#include "core.h"
#include <stdlib.h>

/* cJSON Types: */
#define cJSON_False 0
#define cJSON_True 1
#define cJSON_NULL 2
#define cJSON_Number 3
#define cJSON_String 4
#define cJSON_Array 5
#define cJSON_Object 6
#define cJSON_Timestamp 7
#define cJSON_Date 8
#define cJSON_Regex 9
#define cJSON_Oid 10
#define cJSON_Binary 11
#define cJSON_MinKey 12
#define cJSON_MaxKey 13
#define cJSON_Undefined 14

#define cJSON_IsReference 256

#define cJSON_INT32 0
#define cJSON_INT64 1
#define cJSON_DOUBLE 2

#define CJSON_OP_ADDTOSET  "$addtoset"
#define CJSON_OP_ALL       "$all"
#define CJSON_OP_AND       "$and"
#define CJSON_OP_BITAND    "$bitand"
#define CJSON_OP_BITOR     "$bitor"
#define CJSON_OP_BITNOT    "$bitnot"
#define CJSON_OP_BITXOR    "$bitxor"
#define CJSON_OP_BIT       "$bit"
#define CJSON_OP_ELEMAT    "$elemMatch"
#define CJSON_OP_EXISTS    "$exists"
#define CJSON_OP_ISNULL    "$isnull"
#define CJSON_OP_GTE       "$gte"
#define CJSON_OP_GT        "$gt"
#define CJSON_OP_INC       "$inc"
#define CJSON_OP_LTE       "$lte"
#define CJSON_OP_LT        "$lt"
#define CJSON_OP_IN        "$in"
#define CJSON_OP_ET        "$et"
#define CJSON_OP_MAXDIS    "$maxDistance"
#define CJSON_OP_MOD       "$mod"
#define CJSON_OP_NEAR      "$near"
#define CJSON_OP_NE        "$ne"
#define CJSON_OP_NIN       "$nin"
#define CJSON_OP_NOT       "$not"
#define CJSON_OP_OPTIONS   "$options"
#define CJSON_OP_OR        "$or"
#define CJSON_OP_POP       "$pop"
#define CJSON_OP_PULLALL   "$pull_all"
#define CJSON_OP_PULL      "$pull"
#define CJSON_OP_PUSHALL   "$push_all"
#define CJSON_OP_PUSH      "$push"
#define CJSON_OP_REGEX     "$regex"
#define CJSON_OP_RENAME    "$rename"
#define CJSON_OP_SET       "$set"
#define CJSON_OP_SIZE      "$size"
#define CJSON_OP_TYPE      "$type"
#define CJSON_OP_UNSET     "$unset"
#define CJSON_OP_WINTHIN   "$within"
#define CJSON_OP_FIELD     "$field"
#define CJSON_OP_SUM       "$sum"
#define CJSON_OP_PROJECT   "$project"
#define CJSON_OP_MATCH     "$match"
#define CJSON_OP_LIMIT     "$limit"
#define CJSON_OP_SKIP      "$skip"
#define CJSON_OP_GROUP     "$group"
#define CJSON_OP_FIRST     "$first"
#define CJSON_OP_LAST      "$last"
#define CJSON_OP_MAX       "$max"
#define CJSON_OP_MIN       "$min"
#define CJSON_OP_AVG       "$avg"
#define CJSON_OP_SORT      "$sort"
#define CJSON_OP_MERGEARRAYSET   "$mergearrayset"
#define CJSON_INNER_META   "$Meta"


/* The cJSON structure: */
typedef struct cJSON {
   struct cJSON *next,*prev;   /* next/prev allow you to walk array/object chains. Alternatively, use GetArraySize/GetArrayItem/GetObjectItem */
   struct cJSON *child;      /* An array or object item will have a child pointer pointing to a chain of the items in the array/object. */

   int type;               /* The type of the item, as above. */

   char *valuestring;         /* The item's string, if type==cJSON_String */
   char *valuestring2;         /* The item's string, if type==cJSON_String */
   int valueint;            /* The item's number, if type==cJSON_Number */
   double valuedouble;         /* The item's number, if type==cJSON_Number */
   long long valuelongint; /* The item's number, if type==cJSON_Number */
   int numType;

   char *string;            /* The item's name string, if this item is the child of, or is in the list of subitems of an object. */
} cJSON;

typedef struct cJSON_Hooks {
      void *(*malloc_fn)(size_t sz);
      void (*free_fn)(void *ptr);
} cJSON_Hooks;

/* Supply malloc, realloc and free functions to cJSON */
extern void cJSON_InitHooks(cJSON_Hooks* hooks);


/* Supply a block of JSON, and this returns a cJSON object you can interrogate. Call cJSON_Delete when finished. */
extern cJSON *cJSON_Parse(const char *value);
extern cJSON *cJSON_Parse2(const char *value,int isMongo,int isBatch);
/* Render a cJSON entity to text for transfer/storage. Free the char* when finished. */
extern char  *cJSON_Print(cJSON *item);
/* Render a cJSON entity to text for transfer/storage without any formatting. Free the char* when finished. */
extern char  *cJSON_PrintUnformatted(cJSON *item);
/* Delete a cJSON entity and all subentities. */
extern void   cJSON_Delete(cJSON *c);

/* Returns the number of items in an array (or object). */
extern int     cJSON_GetArraySize(cJSON *array);
/* Retrieve item number "item" from array "array". Returns NULL if unsuccessful. */
extern cJSON *cJSON_GetArrayItem(cJSON *array,int item);
/* Get item "string" from object. Case insensitive. */
extern cJSON *cJSON_GetObjectItem(cJSON *object,const char *string);

/* For analysing failed parses. This returns a pointer to the parse error. You'll probably need to look a few chars back to make sense of it. Defined when cJSON_Parse() returns 0. 0 when cJSON_Parse() succeeds. */
extern const char *cJSON_GetErrorPtr();
   
/* These calls create a cJSON item of the appropriate type. */
extern cJSON *cJSON_CreateNull();
extern cJSON *cJSON_CreateTrue();
extern cJSON *cJSON_CreateFalse();
extern cJSON *cJSON_CreateBool(int b);
extern cJSON *cJSON_CreateNumber(double num);
extern cJSON *cJSON_CreateString(const char *string);
extern cJSON *cJSON_CreateArray();
extern cJSON *cJSON_CreateObject();

/* These utilities create an Array of count items. */
extern cJSON *cJSON_CreateIntArray(int *numbers,int count);
extern cJSON *cJSON_CreateFloatArray(float *numbers,int count);
extern cJSON *cJSON_CreateDoubleArray(double *numbers,int count);
extern cJSON *cJSON_CreateStringArray(const char **strings,int count);

/* Append item to the specified array/object. */
extern void cJSON_AddItemToArray(cJSON *array, cJSON *item);
extern void   cJSON_AddItemToObject(cJSON *object,const char *string,cJSON *item);
/* Append reference to item to the specified array/object. Use this when you want to add an existing cJSON to a new cJSON, but don't want to corrupt your existing cJSON. */
extern void cJSON_AddItemReferenceToArray(cJSON *array, cJSON *item);
extern void   cJSON_AddItemReferenceToObject(cJSON *object,const char *string,cJSON *item);

/* Remove/Detatch items from Arrays/Objects. */
extern cJSON *cJSON_DetachItemFromArray(cJSON *array,int which);
extern void   cJSON_DeleteItemFromArray(cJSON *array,int which);
extern cJSON *cJSON_DetachItemFromObject(cJSON *object,const char *string);
extern void   cJSON_DeleteItemFromObject(cJSON *object,const char *string);
   
/* Update array items. */
extern void cJSON_ReplaceItemInArray(cJSON *array,int which,cJSON *newitem);
extern void cJSON_ReplaceItemInObject(cJSON *object,const char *string,cJSON *newitem);

int bson_Sum_Size(const char *json_str);

#define cJSON_AddNullToObject(object,name)   cJSON_AddItemToObject(object, name, cJSON_CreateNull())
#define cJSON_AddTrueToObject(object,name)   cJSON_AddItemToObject(object, name, cJSON_CreateTrue())
#define cJSON_AddFalseToObject(object,name)      cJSON_AddItemToObject(object, name, cJSON_CreateFalse())
#define cJSON_AddNumberToObject(object,name,n)   cJSON_AddItemToObject(object, name, cJSON_CreateNumber(n))
#define cJSON_AddStringToObject(object,name,s)   cJSON_AddItemToObject(object, name, cJSON_CreateString(s))

#ifdef __cplusplus
}
#endif

#endif
