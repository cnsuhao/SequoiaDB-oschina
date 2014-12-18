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

/* cJSON */
/* JSON parser in C. */

#include "cJSON.h"
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <float.h>
#include <limits.h>
#include <ctype.h>
#include <time.h>

/* #include "iostream.h" */

static const char *ep;
static int sum;

static const char *dollar_command_string[]= {
   CJSON_OP_ADDTOSET    , CJSON_OP_ALL    , CJSON_OP_AND     , CJSON_OP_BITAND ,
   CJSON_OP_BITOR       ,
   CJSON_OP_BITNOT      , CJSON_OP_BITXOR , CJSON_OP_BIT     , CJSON_OP_ELEMAT ,
   CJSON_OP_EXISTS      , CJSON_OP_GTE    , CJSON_OP_GT      , CJSON_OP_INC    ,
   CJSON_OP_LTE         , CJSON_OP_LT     , CJSON_OP_IN      , CJSON_OP_ET,
   CJSON_OP_MAXDIS ,
   CJSON_OP_MOD         , CJSON_OP_NEAR   , CJSON_OP_NE      , CJSON_OP_NIN    ,
   CJSON_OP_NOT         ,
   CJSON_OP_OPTIONS     , CJSON_OP_OR     , CJSON_OP_POP     , CJSON_OP_PULLALL,
   CJSON_OP_PULL        ,
   CJSON_OP_PUSHALL     , CJSON_OP_PUSH   , CJSON_OP_REGEX   , CJSON_OP_RENAME ,
   CJSON_OP_SET         , CJSON_OP_SIZE   , CJSON_OP_TYPE    , CJSON_OP_UNSET  ,
   CJSON_OP_WINTHIN     , CJSON_OP_FIELD  , CJSON_OP_SUM     , CJSON_OP_PROJECT,
   CJSON_OP_MATCH       , CJSON_OP_LIMIT  , CJSON_OP_SKIP    , CJSON_OP_GROUP  ,
   CJSON_OP_FIRST       , CJSON_OP_LAST   , CJSON_OP_MAX     , CJSON_OP_MIN    ,
   CJSON_OP_AVG         , CJSON_OP_SORT   , CJSON_OP_MERGEARRAYSET,
   CJSON_INNER_META     , CJSON_OP_ISNULL
} ;

static const char *parse_array_size(const char *value);
static const char *parse_object_size(const char *value);
static const char *parse_dollar_string(cJSON *item,const char *value) ;

const char *cJSON_GetErrorPtr() {return ep;}

static int cJSON_strcasecmp(const char *s1,const char *s2)
{
   if (!s1) return (s1==s2)?0:1;if (!s2) return 1;
   for(; tolower(*s1) == tolower(*s2); ++s1, ++s2)   if(*s1 == 0)   return 0;
   return tolower(*(const unsigned char *)s1) - tolower(*(const unsigned char *)s2);
}

static void *(*cJSON_malloc)(size_t sz) = malloc;
static void (*cJSON_free)(void *ptr) = free;

static char* cJSON_strdup(const char* str)
{
      size_t len;
      char* copy;

      len = strlen(str) + 1;
      if (!(copy = (char*)cJSON_malloc(len))) return 0;
      memcpy(copy,str,len);
      return copy;
}

void cJSON_InitHooks(cJSON_Hooks* hooks)
{
    if (!hooks) { /* Reset hooks */
        cJSON_malloc = malloc;
        cJSON_free = free;
        return;
    }

   cJSON_malloc = (hooks->malloc_fn)?hooks->malloc_fn:malloc;
   cJSON_free    = (hooks->free_fn)?hooks->free_fn:free;
}

/* Internal constructor. */
static cJSON *cJSON_New_Item()
{
   cJSON* node = (cJSON*)cJSON_malloc(sizeof(cJSON));
   if (node) memset(node,0,sizeof(cJSON));
   return node;
}

/* Delete a cJSON structure. */
void cJSON_Delete(cJSON *c)
{
   cJSON *next;
   while (c)
   {
      next=c->next;
      if (!(c->type&cJSON_IsReference) && c->child) cJSON_Delete(c->child);
      if (!(c->type&cJSON_IsReference) && c->valuestring) cJSON_free(c->valuestring);
      if (!(c->type&cJSON_IsReference) && c->valuestring2) cJSON_free(c->valuestring2);
      if (c->string) cJSON_free(c->string);
      cJSON_free(c);
      c=next;
   }
}

/* Parse the input text to generate a number, and populate the result into item. */
static const char *parse_number(cJSON *item,const char *num)
{
   double n=0,sign=1,scale=0;int subscale=0,signsubscale=1;
   int n1=0 ;
   long long n2 = 0 ;
   item->numType = cJSON_INT32 ;
   /* Could use sscanf for this? */
   if (*num=='-')
   {
      sign=-1 ;
      num++ ;   /* Has sign? */
   }
   else if (*num=='+')
   {
      sign=1 ;
      num++ ;
   }
   while (*num=='0') num++;         /* is zero */
   if (*num>='1' && *num<='9')
   {
      do
      {
         n=(n*10.0)+(*num -'0');   
         n1=(n1*10)+(*num -'0') ;
         n2=(n2*10)+(*num -'0') ;
         ++num ;
         if ( cJSON_INT32== item->numType &&
               (long long)n1!=n2 )
         {
            item->numType = cJSON_INT64 ;
         }
      } while (*num>='0' && *num<='9');   /* Number? */
   }
   if (*num=='.' && num[1]>='0' && num[1]<='9') 
   {
      item->numType = cJSON_DOUBLE ;
      num++;
      do
      {
         n=(n)+(*num++ -'0')/pow(10.0,++scale) ;
      }
      while (*num>='0' && *num<='9');
   }   /* Fractional part? */
   if (*num=='e' || *num=='E')      /* Exponent? */
   {
      num++;
      if (*num=='+')
         num++;
      else if (*num=='-')
      {
           item->numType = cJSON_DOUBLE ;
           signsubscale=-1 ;
           num++;      /* With sign? */
      }
       while (*num>='0' && *num<='9')
          subscale=(subscale*10)+(*num++ - '0');   /* Number? */
   }

  if ( cJSON_DOUBLE == item->numType )
  {
      n=sign*n*pow(10.0,(subscale*signsubscale*1.0));   /* number = +/- number.fraction * 10^+/- exponent */
   }
   else if ( cJSON_INT64 == item->numType )
   {
      if ( 0 != subscale )
      {
         n2=(long long)(sign*n2*pow(10.0,subscale*1.00));
      }
      else
      {
         n2=(((long long)sign)*n2) ;
      }
   }
   else if ( cJSON_INT32 == item->numType )
   {
       n1=(int)(sign*n1*pow(10.0,subscale*1.00));
       n2=(long long)(sign*n2*pow(10.0,subscale*1.00));
       if ( (long long)n1 != n2 )
       {
          item->numType = cJSON_INT64 ;
       }
   }
   item->valuedouble=n;
   item->valueint=n1;
   item->valuelongint=n2;
   item->type=cJSON_Number;
   return num;
}

/* Render the number nicely from the given item into a string. */
static char *print_number(cJSON *item)
{
   char *str;
   double d=item->valuedouble;
   if (fabs(((double)item->valueint)-d)<=DBL_EPSILON && d<=INT_MAX && d>=INT_MIN)
   {
      str=(char*)cJSON_malloc(21);   /* 2^64+1 can be represented in 21 chars. */
      if (str) sprintf(str,"%d",item->valueint);
   }
   else
   {
      str=(char*)cJSON_malloc(64);   /* This is a nice tradeoff. */
      if (str)
      {
         if (fabs(floor(d)-d)<=DBL_EPSILON)         sprintf(str,"%.0f",d);
         else if (fabs(d)<1.0e-6 || fabs(d)>1.0e9)   sprintf(str,"%e",d);
         else                              sprintf(str,"%f",d);
      }
   }
   return str;
}

/* Parse the input text into an unescaped cstring, and populate item. */
static const unsigned char firstByteMark[7] = { 0x00, 0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC };
static const char *parse_string(cJSON *item,const char *str,int isKey)
{
   const char *ptr;char *ptr2;char *out;int len=0;unsigned uc,uc2;
   int isString = 0 ;
   if (*str!='\"' && !isKey)
      return 0 ;
   if (*str=='\"')
   {
      isString = 1 ;
   }   /* never '\"' */
   if ( isString )
      ptr = str + 1 ;
   else
      ptr = str ;
   if ( *(ptr) == '$' && isKey )
   {
      ptr = parse_dollar_string ( item, ptr ) ;
      if ( ptr )
      {
         if ( isString )
            return (ptr+1) ;
         else
            return ptr ;
      }
      return 0 ;
   }
   if ( isString )
      ptr = str + 1 ;
   else
      ptr = str ;
   while ( *ptr && 
           ( !isString || *ptr != '\"' ) &&
           ( isString || (*ptr != 32 && *ptr != 9) ) &&
           ( isString || *ptr != ':' ) &&
           ++len )
   {
      if (*ptr++ == '\\')
         ptr++;   /* Skip escaped quotes. */
   }
   if ( *ptr!='\"' && isString) return 0 ;
   out=(char*)cJSON_malloc(len+1);   /* This is how long we need for the string, roughly. */
   if (!out) return 0;
   ptr2=out;
   if ( isString )
      ptr = str + 1 ;
   else
      ptr = str ;
   while ( *ptr && 
           ( !isString || *ptr != '\"' ) &&
           ( isString || (*ptr != 32 && *ptr != 9) ) &&
           ( isString || *ptr != ':' ) &&
           ++len )
   {

      if (*ptr!='\\') *ptr2++=*ptr++;
      else
      {
         ptr++;
         switch (*ptr)
         {
            case 'b': *ptr2++='\b';   break;
            case 'f': *ptr2++='\f';   break;
            case 'n': *ptr2++='\n';   break;
            case 'r': *ptr2++='\r';   break;
            case 't': *ptr2++='\t';   break;
            case 'u':    /* transcode utf16 to utf8. */
               sscanf(ptr+1,"%4x",&uc);ptr+=4;   /* get the unicode char. */

               if ((uc>=0xDC00 && uc<=0xDFFF) || uc==0)   break;   // check for invalid.

               if (uc>=0xD800 && uc<=0xDBFF)   // UTF16 surrogate pairs.
               {
                  if (ptr[1]!='\\' || ptr[2]!='u')   break;   // missing second-half of surrogate.
                  sscanf(ptr+3,"%4x",&uc2);ptr+=6;
                  if (uc2<0xDC00 || uc2>0xDFFF)      break;   // invalid second-half of surrogate.
                  uc=0x10000 | ((uc&0x3FF)<<10) | (uc2&0x3FF);
               }

               len=4;if (uc<0x80) len=1;else if (uc<0x800) len=2;else if (uc<0x10000) len=3; ptr2+=len;

               switch (len) {
                  case 4: *--ptr2 =((uc | 0x80) & 0xBF); uc >>= 6;
                  case 3: *--ptr2 =((uc | 0x80) & 0xBF); uc >>= 6;
                  case 2: *--ptr2 =((uc | 0x80) & 0xBF); uc >>= 6;
                  case 1: *--ptr2 =(uc | firstByteMark[len]);
               }
               ptr2+=len;
               break;
            default:  *ptr2++=*ptr; break;
         }
         ptr++;
      }
   }
   *ptr2=0;
   if (*ptr=='\"' && isString) ptr++;
   item->valuestring=out;
   item->type=cJSON_String;

   return ptr;
}

/* Render the cstring provided to an escaped version that can be printed. */
static char *print_string_ptr(const char *str)
{
   const char *ptr;char *ptr2,*out;int len=0;unsigned char token;

   if (!str) return cJSON_strdup("");
   ptr=str;while ((token=*ptr) && ++len) {if (strchr("\"\\\b\f\n\r\t",token)) len++; else if (token<32) len+=5;ptr++;}

   out=(char*)cJSON_malloc(len+3);

   if (!out) return 0;
   ptr2=out;ptr=str;
   *ptr2++='\"';
   while (*ptr)
   {

      if ((unsigned char)*ptr>31 && *ptr!='\"' && *ptr!='\\') *ptr2++=*ptr++;
      else
      {
         *ptr2++='\\';
         switch (token=*ptr++)
         {
            case '\\':   *ptr2++='\\';   break;
            case '\"':   *ptr2++='\"';   break;
            case '\b':   *ptr2++='b';   break;
            case '\f':   *ptr2++='f';   break;
            case '\n':   *ptr2++='n';   break;
            case '\r':   *ptr2++='r';   break;
            case '\t':   *ptr2++='t';   break;
            default: sprintf(ptr2,"u%04x",token);ptr2+=5;   break;   /* escape and print */
         }
      }
   }
   *ptr2++='\"';*ptr2++=0;
   return out;
}
/* Invote print_string_ptr (which is useful) on an item. */
static char *print_string(cJSON *item)   {return print_string_ptr(item->valuestring);}

/* Predeclare these prototypes. */
static const char *parse_value(cJSON *item,const char *value,int isKey,int isMongo);
static char *print_value(cJSON *item,int depth,int fmt);
static const char *parse_array(cJSON *item,const char *value,int isMongo);
static char *print_array(cJSON *item,int depth,int fmt);
static const char *parse_object(cJSON *item,const char *value,int isMongo);
static char *print_object(cJSON *item,int depth,int fmt);
static const char *parse_dollar_command(cJSON *item,const char *value,int cj_type);
static const char *parse_objectid(cJSON *item,const char *value);
static const char *parse_Mongo_Timestamp ( cJSON *item, const char *value ) ;
static void local_time ( time_t *Time, struct tm *TM )
{
   if ( !Time || !TM )
      return ;
#if defined (__linux__ )
   localtime_r( Time, TM ) ;
#elif defined (_WIN32)
   localtime_s( TM, Time ) ;
#endif
} ;
/* Utility to jump whitespace and cr/lf */
static const char *skip(const char *in) 
{
   while (in && *in && (unsigned char)*in<=32) 
      in++; 
   return in;
}

/* Parse an object - create a new root, and populate. */
cJSON *cJSON_Parse(const char *value)
{
   return cJSON_Parse2 ( value, 0, 0 ) ;
}

cJSON *cJSON_Parse2(const char *value,int isMongo,int isBatch)
{
   cJSON *c=cJSON_New_Item();
   ep=0;
   if (!c) return 0;       /* memory fail */
   value = skip ( value );
   if ( *value != '{' ) return 0;       /* memory fail */
   value = parse_value(c,skip(value),0, isMongo);
   if ( !value )
   {
      cJSON_Delete(c);
      return 0;
   }
   else
   {
      value = skip ( value );
      if ( *value && !isBatch )
         return 0 ;
   }
   return c;
}

/* Render a cJSON item/entity/structure to text. */
char *cJSON_Print(cJSON *item)            {return print_value(item,0,1);}
char *cJSON_PrintUnformatted(cJSON *item)   {return print_value(item,0,0);}

/* Parser core - when encountering text, process appropriately. */
static const char *parse_value(cJSON *item,const char *value,int isKey,int isMongo)
{
   if (!value)                  return 0;   /* Fail on null. */
   if (!strncmp(value,"null",4))
   {
      item->type=cJSON_NULL;
      return value+4;
   }
   if (!strncmp(value,"false",5))
   {
      item->type=cJSON_False;
      return value+5;
   }
   if (!strncmp(value,"true",4))
   {
      item->type=cJSON_True;
      item->valueint=1;
      return value+4;
   }
   if (!strncmp(value,"ObjectId",8))
   {
      item->type=cJSON_Oid;
      value += 8 ;
      return parse_objectid ( item, value ) ;
   }
   if (*value=='\"')
   {
      return parse_string(item,value,isKey);
   }
   if ( *value=='+' || *value=='-' || (*value>='0' && *value<='9'))
   {
      return parse_number(item,value);
   }
   if (*value=='{')
   {
      const char *value_temp = value + 1 ;
      const char *value_temp2 = value + 1 ;
      value_temp = skip ( value_temp ) ;
      if ( *value_temp == '\"' )
      {
         ++value_temp ;
      }
      if ( isMongo && *value_temp == 't' )
      {
         value_temp2 = parse_Mongo_Timestamp ( item, value ) ;
         if ( value_temp2 )
         {
            return value_temp2 ;
         }
      }
      if ( !strncmp ( value_temp, "$timestamp", 10 ) )
      {
         return parse_dollar_command ( item, value, cJSON_Timestamp ) ;
      }
      else if ( !strncmp ( value_temp, "$date", 5 ) )
      {
         return parse_dollar_command ( item, value, cJSON_Date ) ;
      }
      else if ( !strncmp ( value_temp, "$regex", 6 ) )
      {
         return parse_dollar_command ( item, value, cJSON_Regex ) ;
      }
      else if( !strncmp ( value_temp, "$oid", 4 ) )
      {
         return parse_dollar_command ( item, value, cJSON_Oid ) ;
      }
      else if( !strncmp ( value_temp, "$binary", 7 ) )
      {
         return parse_dollar_command ( item, value, cJSON_Binary ) ;
      }
      else if( !strncmp ( value_temp, "$maxKey", 7 ) )
      {
         return parse_dollar_command ( item, value, cJSON_MaxKey ) ;
      }
      else if( !strncmp ( value_temp, "$minKey", 7 ) )
      {
         return parse_dollar_command ( item, value, cJSON_MinKey ) ;
      }
      else if( !strncmp ( value_temp, "$undefined", 10 ) )
      {
         return parse_dollar_command ( item, value, cJSON_Undefined ) ;
      }
   }
   if (*value=='[')
   {
      return parse_array(item,value,isMongo);
   }
   if (*value=='{')
   {
      return parse_object(item,value,isMongo);
   }

   ep=value;
   return 0;   /* failure. */
}

static const char *parse_Mongo_Timestamp ( cJSON *item, const char *value )
{
   long long t = 0 ;
   int i = 0 ;
   int isStr = 0 ;
   struct tm psr ;
   char temp[64] ;
   time_t timer ;
   memset ( temp, 0, 64 ) ;
   if ( *value != '{' )
   {
      return 0 ;
   }
   value = skip ( value+1 ) ;
   if ( *value == '\"' )
   {
      isStr = 1 ;
      ++value ;
   }
   if ( *value != 't' )
   {
      return 0 ;
   }
   ++value ;
   if ( *value == '\"' && isStr )
   {
      ++value ;
   }
   else if ( *value != '\"' && !isStr )
   {
   }
   else
   {
      return 0 ;
   }
   value = skip ( value ) ;
   if ( *value != ':' )
   {
      return 0 ;
   }
   value = skip ( value+1 ) ;
   value = parse_value(item, value, 0, 1 ) ;
   if ( item->type != cJSON_Number ||
        ( item->numType != cJSON_INT32 && item->numType != cJSON_INT64 ) )
   {
      return 0 ;
   }
   if ( item->numType == cJSON_INT32 )
   {
      t = item->valueint/1000 ;
   }
   else
   {
      t = item->valuelongint/1000 ;
   }
   value = skip ( value ) ;
   if ( *value != ',' )
   {
      return 0 ;
   }
   value = skip ( value + 1 ) ;
   isStr = 0 ;
   if ( *value == '\"' )
   {
      isStr = 1 ;
      ++value ;
   }
   if ( *value != 'i' )
   {
      return 0 ;
   }
   ++value ;
   if ( *value == '\"' && isStr )
   {
      ++value ;
   }
   else if ( *value != '\"' && !isStr )
   {
   }
   else
   {
      return 0 ;
   }
   value = skip ( value ) ;
   if ( *value != ':' )
   {
      return 0 ;
   }
   value = skip ( value+1 ) ;
   value = parse_value(item, value, 0, 1 ) ;
   if ( item->type != cJSON_Number ||
        item->numType != cJSON_INT32 )
   {
      return 0 ;
   }
   value = skip ( value ) ;
   if ( *value != '}' )
   {
      return 0 ;
   }
   value = skip ( value+1 ) ;
   i = item->valueint ;
   value = skip ( value ) ;
   item->type = cJSON_Timestamp ;
   timer = (time_t)t ;
   local_time ( &timer, &psr ) ;
   sprintf( temp,
            "%04d-%02d-%02d-%02d.%02d.%02d.%06d",
            psr.tm_year + 1900,
            psr.tm_mon + 1,
            psr.tm_mday,
            psr.tm_hour,
            psr.tm_min,
            psr.tm_sec,
            i ) ;
    item->valuestring = (char*)cJSON_malloc( 64 ) ;
    memset ( item->valuestring, 0, 64 ) ;
    strncpy ( item->valuestring, temp, 64 ) ;
    return value ;
}

static const char *parse_objectid(cJSON *item,const char *value)
{
   const char *value_temp = NULL ;
   int len = 0;
   value = skip ( value ) ;
   if ( *value != '(' )return 0 ;
   value = skip ( value+1 ) ;
   if ( *value != '\"' )return 0 ;
   value_temp = ++value;
   while ( *value_temp != '\"' &&
           *value_temp != ' '  &&
           *value_temp != '}'  &&
           (unsigned char)*value_temp > 32 )
   {
      /* not an object! */
      if( !value_temp )
         return 0 ;
      ++len ;
      ++value_temp ;
   }
   if ( len != 24 )
      return 0 ;
   item->valuestring = (char*)cJSON_malloc( len + 1 ) ;
   strncpy ( item->valuestring, value, len ) ;
   item->valuestring [ len ] = 0 ;
   value = value_temp ;
   value = skip ( value+1 ) ;
   if ( *value != ')' )return 0 ;
   return value+1 ;
}

/* Render a value to text. */

static char *print_value(cJSON *item,int depth,int fmt)
{
   char *out=0;
   if (!item) return 0;
   switch ((item->type)&255)
   {
      case cJSON_NULL:   out=cJSON_strdup("null");   break;
      case cJSON_False:   out=cJSON_strdup("false");break;
      case cJSON_True:   out=cJSON_strdup("true"); break;
      case cJSON_Number:   out=print_number(item);break;
      case cJSON_String:   out=print_string(item);break;
      case cJSON_Array:   out=print_array(item,depth,fmt);break;
      case cJSON_Object:   out=print_object(item,depth,fmt);break;
   }
   return out;
}

/* Build one parse dollar string key from input text. */
static const char *parse_dollar_string ( cJSON *item, const char *value )
{
   int dollar_string_len = 0 ;
   if ( '$' == *value )
   {
      int k = 0 ;
      int typeNum = sizeof(dollar_command_string)/sizeof(dollar_command_string[0]);
      for( k = 0; k < typeNum; ++k )
      {
         dollar_string_len = strlen( dollar_command_string [ k ] ) ;
         if ( !strncmp ( value,
                         dollar_command_string [ k ],
                         dollar_string_len ) )
         {
            item->valuestring = (char*)cJSON_malloc( dollar_string_len + 1 ) ;
            strncpy ( item->valuestring,
                      dollar_command_string [ k ],
                      dollar_string_len ) ;

            item->valuestring [ dollar_string_len ] = 0 ;
            value = skip ( value + dollar_string_len ) ;
            return value ;
         }
      }
   }
   return 0 ;
}

/* Build one parse_dollar_command from input text. */
static const char *parse_first_command(cJSON *item,const char *value,int cj_type )
{
   int isNumber = 0 ;
   switch ( cj_type )
   {
   case cJSON_Timestamp:
   {
      /* not a dollar command! */
      if ( strncmp ( value, "$timestamp", 10 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 10 ) ;
      break ;
   }
   case cJSON_Date:
   {
      /* not a dollar command! */
      if ( strncmp ( value, "$date", 5 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 5 ) ;
      break ;
   }
   case cJSON_Regex:
   {
      /* not a dollar command! */
      if ( strncmp ( value, "$regex", 6 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 6 ) ;
      break ;
   }
   case cJSON_Oid:
   {
      /* not a dollar command! */
      if ( strncmp ( value, "$oid", 4 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 4 ) ;
      break ;
   }
   case cJSON_Binary:
   {
      /* not a dollar command! */
      if ( strncmp ( value, "$binary", 7 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 7 ) ;
      break ;
   }
   case cJSON_MinKey:
   {
      /* not a dollar command! */
      if ( strncmp ( value, "$minKey", 7 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 7 ) ;
      break ;
   }
   case cJSON_MaxKey:
   {
      /* not a dollar command! */
      if ( strncmp ( value, "$maxKey", 7 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 7 ) ;
      break ;
   }
   case cJSON_Undefined:
   {
      /* not a dollar command! */
      if ( strncmp ( value, "$undefined", 10 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 10 ) ;
      break ;
   }
   }
   if ( *value == '\"' )
      value = skip ( value + 1 ) ;
   else
      value = skip ( value ) ;
   /* not an object! */
   if ( *value != ':' )
   {
      ep = value ;
      return 0 ;
   }
   value = skip ( value + 1 ) ;
   if ( *value != '\"' )
   {
      if ( cj_type == cJSON_Date ||
           cj_type == cJSON_MinKey ||
           cj_type == cJSON_MaxKey ||
           cj_type == cJSON_Undefined )
      {
         isNumber = 1 ;
      }
      else
      {
         ep = value ;
         return 0 ;
      }
   }
   else
   {
      if ( cj_type == cJSON_MinKey ||
           cj_type == cJSON_MaxKey ||
           cj_type == cJSON_Undefined )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 1 ) ;
   }
   switch ( cj_type )
   {
   case cJSON_Timestamp:
   {
      const char *value_temp = value;
      int len = 0;
      while (  value_temp &&
               *value_temp != '\"' &&
               (unsigned char)*value_temp > 32 )
      {
         /* not an object! */
         if ( !((*value_temp >= '0' && *value_temp <= '9') ||
              *value_temp == '-' ||
              *value_temp == '.') )
            return 0 ;
         ++len ;
         ++value_temp ;
      }
      if( !value_temp )
         return 0 ;
      item->valuestring = (char*)cJSON_malloc( len + 1 ) ;
      strncpy ( item->valuestring, value, len ) ;
      item->valuestring [ len ] = 0 ;
      value = value_temp ;
      break ;
   }
   case cJSON_Date:
   {
      const char *value_temp = value;
      int len = 0;
      time_t timer ;
      if ( isNumber )
      {
         long long t = 0 ;
         int i = 0 ;
         struct tm psr ;
         char temp[64] ;
         memset ( temp, 0, 64 ) ;
         value_temp = parse_value ( item, value_temp, 0, 0 ) ;
         if ( !value_temp )
         {
            return 0 ;
         }
         if ( item->type != cJSON_Number )
         {
            return 0 ;
         }
         if ( item->numType == cJSON_INT32 )
         {
            t = item->valueint/1000 ;
            i = item->valueint - ( t * 1000 )  ;
         }
         else if ( item->numType == cJSON_INT64 )
         {
            t = item->valuelongint/1000 ;
            i = item->valuelongint - ( t * 1000 )  ;
         }
         else
         {
            return 0 ;
         }
         item->type = cJSON_Timestamp ;
         timer = (time_t)t ;
         local_time ( &timer, &psr ) ;
         sprintf( temp,
                  "%04d-%02d-%02d-%02d.%02d.%02d.%06d",
                  psr.tm_year + 1900,
                  psr.tm_mon + 1,
                  psr.tm_mday,
                  psr.tm_hour,
                  psr.tm_min,
                  psr.tm_sec,
                  i ) ;
         item->valuestring = (char*)cJSON_malloc( 64 ) ;
         memset ( item->valuestring, 0, 64 ) ;
         strncpy ( item->valuestring, temp, 64 ) ;
         value = value_temp ;
      }
      else
      {
         while ( value_temp &&
                 *value_temp != '\"' &&
                 (unsigned char)*value_temp > 32 )
         {
            /* not an object! */
            if ( !((*value_temp >= '0' && *value_temp <= '9') ||
                 *value_temp == '-' ) )
               return 0 ;
            ++len ;
            ++value_temp ;
         }
         if( !value_temp )
            return 0 ;
         item->valuestring = (char*)cJSON_malloc( len + 1 ) ;
         strncpy ( item->valuestring, value, len ) ;
         item->valuestring [ len ] = 0 ;
         value = value_temp ;
      }
      break ;
   }
   case cJSON_Oid:
   {
      const char *value_temp = value;
      int len = 0;
      while ( value_temp &&
              *value_temp != '\"' &&
              (unsigned char)*value_temp > 32 )
      {
         ++len ;
         ++value_temp ;
      }
      if( !value_temp )
         return 0 ;
      if ( len != 24 )
         return 0 ;
      item->valuestring = (char*)cJSON_malloc( len + 1 ) ;
      strncpy ( item->valuestring, value, len ) ;
      item->valuestring [ len ] = 0 ;
      value = value_temp ;
      break ;
   }
   case cJSON_Regex:
   {
      const char *value_temp = value;
      int len = 0;
      while ( value_temp &&
              *value_temp != '\"' &&
              (unsigned char)*value_temp > 32 )
      {
         ++len ;
         ++value_temp ;
      }
      /* not an object! */
      if( !value_temp )
         return 0 ;
      item->valuestring = (char*)cJSON_malloc( len + 1 ) ;
      strncpy ( item->valuestring, value, len ) ;
      item->valuestring [ len ] = 0 ;
      value = value_temp ;
      break ;
   }
   case cJSON_Binary:
   {
      const char *value_temp = value;
      int len = 0;
      while ( value_temp &&
              *value_temp != '\"' &&
              (unsigned char)*value_temp > 32 )
      {
         /* not an object! */
         if ( !((*value_temp >= '0' && *value_temp <= '9') ||
                (*value_temp >= 'a' && *value_temp <= 'z') ||
                (*value_temp >= 'A' && *value_temp <= 'Z') ||
                *value_temp == '+' ||
                *value_temp == '/' ||
                *value_temp == '=' ) )
             return 0 ;
         ++len ;
         ++value_temp ;
      }
      if( !value_temp )
         return 0 ;
      item->valuestring = (char*)cJSON_malloc( len + 1 ) ;
      strncpy ( item->valuestring, value, len ) ;
      item->valuestring [ len ] = 0 ;
      value = value_temp ;
      break ;
   }
   case cJSON_MaxKey:
   case cJSON_MinKey:
   case cJSON_Undefined:
   {
      if ( *value != '1' )
      {
         return 0 ;
      }
      return ++value ;
   }
   }
   if ( *value == ' ' || *value == '\"' )
      ++value ;
   value = skip ( value ) ;
   return value ;
}

static const char *parse_second_command(cJSON *item,const char *value,int cj_type)
{
   int isString = 0 ;
   switch ( cj_type )
   {
   case cJSON_Timestamp:
   case cJSON_Date:
   case cJSON_Oid:
   case cJSON_MaxKey:
   case cJSON_MinKey:
   case cJSON_Undefined:
      return value ;
   case cJSON_Regex:
   {
      /* not an object! */
      if (*value!=',')
      {
         ep = value ;
         item->valuestring2 = (char*)cJSON_malloc( 1 ) ;
         item->valuestring2 [ 0 ] = 0 ;
         return value ;
      }
      value = skip ( value + 1 ) ;
      /* not a json. */
      if (*value=='}')
      {
         ep = value ;
         return 0 ;
      }
      if ( *value == '\"' )
      {
         isString = 1 ;
         value = skip ( value + 1 ) ;
      }
      else
         value = skip ( value ) ;
      /* not a commond! */
      if (*value!='$')
      {
         ep = value ;
         return 0 ;
      }
      if ( strncmp ( value, "$options", 8 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 8 ) ;
      break ;
   }
   case cJSON_Binary:
   {
      /* not an object! */
      if (*value!=',')
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 1 ) ;
      /* not a json. */
      if (*value=='}')
      {
         ep = value ;
         return 0 ;
      }
      if ( *value == '\"' )
      {
         isString = 1 ;
         value = skip ( value + 1 ) ;
      }
      else
         value = skip ( value ) ;
      /* not a commond! */
      if (*value!='$')
      {
         ep = value ;
         return 0 ;
      }
      /* not a dollar command! */
      if ( strncmp ( value, "$type", 5 ) )
      {
         ep = value ;
         return 0 ;
      }
      value = skip ( value + 5 ) ;
      break ;
   }
   }
   if ( isString )
   {
      if ( *value == '\"' )
      {
         value = skip ( value + 1 ) ;
      }
      else
      {
         ep = value ;
         return 0 ;
      }
   }
   else
      value = skip ( value ) ;
   /* not an object! */
   if ( *value != ':' )
   {
      ep = value ;
      return 0 ;
   }
   value = skip ( value + 1 ) ;
   switch ( cj_type )
   {
   case cJSON_Binary:
   {
      if ( *value == '\"' )
      {
         ++value ;
      }
      else
      {
         return 0 ;
      }
      value = parse_number ( item, value ) ;
      if ( !value )
         return 0 ;
      if ( *value == '\"' )
         ++value ;
      value = skip ( value ) ;
      item->type = cj_type ;
      break ;
   }
   case cJSON_Regex:
   {
      const char *value_temp = NULL ;
      int len = 0;
      if ( *value == '\"' )
      {
         ++value ;
      }
      else
      {
         return 0 ;
      }
      value_temp = value;
      while ( value_temp &&
              *value_temp != '\"' &&
              (unsigned char)*value_temp > 32 )
      {
         if ( *value_temp != 'i' &&
              *value_temp != 'm' &&
              *value_temp != 'x' &&
              *value_temp != 's' )
              return 0 ;
         /* not an object! */
         if( !value_temp )
            return 0 ;
         ++len ;
         ++value_temp ;
      }
      if( !value_temp )
         return 0 ;
      item->valuestring2 = (char*)cJSON_malloc( len + 1 ) ;
      strncpy ( item->valuestring2, value, len ) ;
      item->valuestring2 [ len ] = 0 ;
      value = value_temp ;
      if ( *value == '\"' )
         ++value ;
      value = skip ( value ) ;
      break ;
   }
   }
   return value  ;
}
/* Build a parse_dollar_command from input text. */
static const char *parse_dollar_command(cJSON *item,const char *value,int cj_type)
{
   /* not an object! */
   if ( *value != '{' )
   {
      ep = value ;
      return 0 ;
   }
   value = skip ( value + 1 ) ;
   if (*value=='\"')
   {
      value = skip ( value + 1 ) ;
   }
   /* empty array. */
   if (*value=='}')
   {
      ep = value ;
      return 0 ;
   }
   /* not a dollar command! */
   if ( *value != '$' )
   {
      ep = value ;
      return 0 ;
   }
   item->type = cj_type ;
   value = parse_first_command ( item, value, cj_type ) ;
   if ( !value )
      return 0 ;

   value = parse_second_command ( item, value, cj_type ) ;
   if ( !value )
      return 0 ;

   value = skip ( value ) ;
   if ( *value == '}' )
      return value + 1 ;
   ep = value ;
   return 0 ;
}

/* Build an array from input text. */
static const char *parse_array(cJSON *item,const char *value,int isMongo)
{
   cJSON *child;
   if (*value!='[')   {ep=value;return 0;}   /* not an array! */

   item->type=cJSON_Array;
   value=skip(value+1);
   if (*value==']') return value+1;   /* empty array. */

   item->child=child=cJSON_New_Item();
   if (!item->child) return 0;       /* memory fail */
   value=skip(parse_value(child,skip(value),0,isMongo));   /* skip any spacing, get the value. */
   if (!value) return 0;

   while (*value==',')
   {
      cJSON *new_item;
      if (!(new_item=cJSON_New_Item())) return 0;    /* memory fail */
      child->next=new_item;new_item->prev=child;child=new_item;
      value=skip(parse_value(child,skip(value+1),0,isMongo));
      if (!value) return 0;   /* memory fail */
   }

   if (*value==']') return value+1;   /* end of array */
   ep=value;return 0;   /* malformed. */
}

/* Render an array to text */
static char *print_array(cJSON *item,int depth,int fmt)
{
   char **entries;
   char *out=0,*ptr,*ret;int len=5;
   cJSON *child=item->child;
   int numentries=0,i=0,fail=0;
   
   /* How many entries in the array? */
   while (child) numentries++,child=child->next;
   /* Allocate an array to hold the values for each */
   entries=(char**)cJSON_malloc(numentries*sizeof(char*));
   if (!entries) return 0;
   memset(entries,0,numentries*sizeof(char*));
   /* Retrieve all the results: */
   child=item->child;
   while (child && !fail)
   {
      ret=print_value(child,depth+1,fmt);
      entries[i++]=ret;
      if (ret) len+=strlen(ret)+2+(fmt?1:0); else fail=1;
      child=child->next;
   }
   
   /* If we didn't fail, try to malloc the output string */
   if (!fail) out=(char*)cJSON_malloc(len);
   /* If that fails, we fail. */
   if (!out) fail=1;

   /* Handle failure. */
   if (fail)
   {
      for (i=0;i<numentries;i++) if (entries[i]) cJSON_free(entries[i]);
      cJSON_free(entries);
      return 0;
   }
   
   /* Compose the output array. */
   *out='[';
   ptr=out+1;*ptr=0;
   for (i=0;i<numentries;i++)
   {
      strcpy(ptr,entries[i]);ptr+=strlen(entries[i]);
      if (i!=numentries-1) {*ptr++=',';if(fmt)*ptr++=' ';*ptr=0;}
      cJSON_free(entries[i]);
   }
   cJSON_free(entries);
   *ptr++=']';*ptr++=0;
   return out;   
}

/* Build an object from the text. */
static const char *parse_object(cJSON *item,const char *value,int isMongo)
{
   cJSON *child;
   if (*value!='{')   {ep=value;return 0;}   /* not an object! */
   
   item->type=cJSON_Object;
   value=skip(value+1);
   if (*value=='}') return value+1;   /* empty array. */
   
   item->child=child=cJSON_New_Item();
   if (!item->child) return 0;
   
   value=skip(parse_string(child,skip(value),1));
   if (!value) return 0;
   child->string=child->valuestring;child->valuestring=0;
   if (*value!=':') {ep=value;return 0;}   /* fail! */
   
   value=skip(parse_value(child,skip(value+1),0,isMongo));   /* skip any spacing, get the value. */
   if (!value) return 0;
   
   while (*value==',')
   {
      cJSON *new_item;
      if (!(new_item=cJSON_New_Item()))   return 0; /* memory fail */
      child->next=new_item;new_item->prev=child;child=new_item;
      
      value=skip(parse_string(child,skip(value+1),1));
      if (!value) return 0;
      child->string=child->valuestring;child->valuestring=0;
      if (*value!=':') {ep=value;return 0;}   /* fail! */

      value=skip(parse_value(child,skip(value+1),0,isMongo));   /* skip any spacing, get the value. */
      if (!value) return 0;
   }
   
   if (*value=='}') return value+1;   /* end of array */
   ep=value;return 0;   /* malformed. */
}

/* Render an object to text. */
static char *print_object(cJSON *item,int depth,int fmt)
{
   char **entries=0,**names=0;
   char *out=0,*ptr,*ret,*str;int len=7,i=0,j;
   cJSON *child=item->child;
   int numentries=0,fail=0;
   /* Count the number of entries. */
   while (child) numentries++,child=child->next;
   /* Allocate space for the names and the objects */
   entries=(char**)cJSON_malloc(numentries*sizeof(char*));
   if (!entries) return 0;
   names=(char**)cJSON_malloc(numentries*sizeof(char*));
   if (!names) {cJSON_free(entries);return 0;}
   memset(entries,0,sizeof(char*)*numentries);
   memset(names,0,sizeof(char*)*numentries);

   /* Collect all the results into our arrays: */
   child=item->child;depth++;if (fmt) len+=depth;
   while (child)
   {
      names[i]=str=print_string_ptr(child->string);
      entries[i++]=ret=print_value(child,depth,fmt);
      if (str && ret) len+=strlen(ret)+strlen(str)+2+(fmt?2+depth:0); else fail=1;
      child=child->next;
   }
   
   /* Try to allocate the output string */
   if (!fail) out=(char*)cJSON_malloc(len);
   if (!out) fail=1;

   /* Handle failure */
   if (fail)
   {
      for (i=0;i<numentries;i++) {if (names[i]) cJSON_free(names[i]);if (entries[i]) cJSON_free(entries[i]);}
      cJSON_free(names);cJSON_free(entries);
      return 0;
   }
   
   /* Compose the output: */
   *out='{';ptr=out+1;if (fmt)*ptr++='\n';*ptr=0;
   for (i=0;i<numentries;i++)
   {
      if (fmt) for (j=0;j<depth;j++) *ptr++='\t';
      strcpy(ptr,names[i]);ptr+=strlen(names[i]);
      *ptr++=':';if (fmt) *ptr++='\t';
      strcpy(ptr,entries[i]);ptr+=strlen(entries[i]);
      if (i!=numentries-1) *ptr++=',';
      if (fmt) *ptr++='\n';*ptr=0;
      cJSON_free(names[i]);cJSON_free(entries[i]);
   }
   
   cJSON_free(names);cJSON_free(entries);
   if (fmt) for (i=0;i<depth-1;i++) *ptr++='\t';
   *ptr++='}';*ptr++=0;
   return out;   
}

/* Get Array size/item / object item. */
int    cJSON_GetArraySize(cJSON *array)                     {cJSON *c=array->child;int i=0;while(c)i++,c=c->next;return i;}
cJSON *cJSON_GetArrayItem(cJSON *array,int item)            {cJSON *c=array->child;  while (c && item>0) item--,c=c->next; return c;}
cJSON *cJSON_GetObjectItem(cJSON *object,const char *string)   {cJSON *c=object->child; while (c && cJSON_strcasecmp(c->string,string)) c=c->next; return c;}

/* Utility for array list handling. */
static void suffix_object(cJSON *prev,cJSON *item) {prev->next=item;item->prev=prev;}
/* Utility for handling references. */
static cJSON *create_reference(cJSON *item) {cJSON *ref=cJSON_New_Item();if (!ref) return 0;memcpy(ref,item,sizeof(cJSON));ref->string=0;ref->type|=cJSON_IsReference;ref->next=ref->prev=0;return ref;}

/* Add item to array/object. */
void   cJSON_AddItemToArray(cJSON *array, cJSON *item)                  {cJSON *c=array->child;if (!item) return; if (!c) {array->child=item;} else {while (c && c->next) c=c->next; suffix_object(c,item);}}
void   cJSON_AddItemToObject(cJSON *object,const char *string,cJSON *item)   {if (!item) return; if (item->string) cJSON_free(item->string);item->string=cJSON_strdup(string);cJSON_AddItemToArray(object,item);}
void   cJSON_AddItemReferenceToArray(cJSON *array, cJSON *item)                  {cJSON_AddItemToArray(array,create_reference(item));}
void   cJSON_AddItemReferenceToObject(cJSON *object,const char *string,cJSON *item)   {cJSON_AddItemToObject(object,string,create_reference(item));}

cJSON *cJSON_DetachItemFromArray(cJSON *array,int which)         {cJSON *c=array->child;while (c && which>0) c=c->next,which--;if (!c) return 0;
   if (c->prev) c->prev->next=c->next;if (c->next) c->next->prev=c->prev;if (c==array->child) array->child=c->next;c->prev=c->next=0;return c;}
void   cJSON_DeleteItemFromArray(cJSON *array,int which)         {cJSON_Delete(cJSON_DetachItemFromArray(array,which));}
cJSON *cJSON_DetachItemFromObject(cJSON *object,const char *string) {int i=0;cJSON *c=object->child;while (c && cJSON_strcasecmp(c->string,string)) i++,c=c->next;if (c) return cJSON_DetachItemFromArray(object,i);return 0;}
void   cJSON_DeleteItemFromObject(cJSON *object,const char *string) {cJSON_Delete(cJSON_DetachItemFromObject(object,string));}

/* Replace array/object items with new ones. */
void   cJSON_ReplaceItemInArray(cJSON *array,int which,cJSON *newitem)      {cJSON *c=array->child;while (c && which>0) c=c->next,which--;if (!c) return;
   newitem->next=c->next;newitem->prev=c->prev;if (newitem->next) newitem->next->prev=newitem;
   if (c==array->child) array->child=newitem; else newitem->prev->next=newitem;c->next=c->prev=0;cJSON_Delete(c);}
void   cJSON_ReplaceItemInObject(cJSON *object,const char *string,cJSON *newitem){int i=0;cJSON *c=object->child;while(c && cJSON_strcasecmp(c->string,string))i++,c=c->next;if(c){newitem->string=cJSON_strdup(string);cJSON_ReplaceItemInArray(object,i,newitem);}}

/* Create basic types: */
cJSON *cJSON_CreateNull()                  {cJSON *item=cJSON_New_Item();if(item)item->type=cJSON_NULL;return item;}
cJSON *cJSON_CreateTrue()                  {cJSON *item=cJSON_New_Item();if(item)item->type=cJSON_True;return item;}
cJSON *cJSON_CreateFalse()                  {cJSON *item=cJSON_New_Item();if(item)item->type=cJSON_False;return item;}
cJSON *cJSON_CreateBool(int b)               {cJSON *item=cJSON_New_Item();if(item)item->type=b?cJSON_True:cJSON_False;return item;}
cJSON *cJSON_CreateNumber(double num)         {cJSON *item=cJSON_New_Item();if(item){item->type=cJSON_Number;item->valuedouble=num;item->valueint=(int)num;}return item;}
cJSON *cJSON_CreateString(const char *string)   {cJSON *item=cJSON_New_Item();if(item){item->type=cJSON_String;item->valuestring=cJSON_strdup(string);}return item;}
cJSON *cJSON_CreateArray()                  {cJSON *item=cJSON_New_Item();if(item)item->type=cJSON_Array;return item;}
cJSON *cJSON_CreateObject()                  {cJSON *item=cJSON_New_Item();if(item)item->type=cJSON_Object;return item;}

/* Create Arrays: */
cJSON *cJSON_CreateIntArray(int *numbers,int count)            {int i;cJSON *n=0,*p=0,*a=cJSON_CreateArray();for(i=0;a && i<count;i++){n=cJSON_CreateNumber(numbers[i]);if(!i)a->child=n;else suffix_object(p,n);p=n;}return a;}
cJSON *cJSON_CreateFloatArray(float *numbers,int count)         {int i;cJSON *n=0,*p=0,*a=cJSON_CreateArray();for(i=0;a && i<count;i++){n=cJSON_CreateNumber(numbers[i]);if(!i)a->child=n;else suffix_object(p,n);p=n;}return a;}
cJSON *cJSON_CreateDoubleArray(double *numbers,int count)      {int i;cJSON *n=0,*p=0,*a=cJSON_CreateArray();for(i=0;a && i<count;i++){n=cJSON_CreateNumber(numbers[i]);if(!i)a->child=n;else suffix_object(p,n);p=n;}return a;}
cJSON *cJSON_CreateStringArray(const char **strings,int count)   {int i;cJSON *n=0,*p=0,*a=cJSON_CreateArray();for(i=0;a && i<count;i++){n=cJSON_CreateString(strings[i]);if(!i)a->child=n;else suffix_object(p,n);p=n;}return a;}




static const char *parse_string_size(const char *str)
{
   const char *ptr=str+1;char *ptr2;char *out;int len=0;unsigned uc,uc2;
   if (*str!='\"'){ep=str;return 0;}   /* not a string! */
   
   while (*ptr!='\"' && *ptr && ++len) if (*ptr++ == '\\') ptr++;   /* Skip escaped quotes. */
    sum+=len;
   sum++;
   out=(char*)cJSON_malloc(len+1);   /* This is how long we need for the string, roughly. */
   if (!out) return 0;
   
   ptr=str+1;ptr2=out;

   while (*ptr!='\"' && *ptr)
   {
      
      if (*ptr!='\\') *ptr2++=*ptr++;
      else
      {
         ptr++;
         switch (*ptr)
         {
            case 'b': *ptr2++='\b';   break;
            case 'f': *ptr2++='\f';   break;
            case 'n': *ptr2++='\n';   break;
            case 'r': *ptr2++='\r';   break;
            case 't': *ptr2++='\t';   break;
            case 'u':    /* transcode utf16 to utf8. */
               sscanf(ptr+1,"%4x",&uc);ptr+=4;   /* get the unicode char. */

               if ((uc>=0xDC00 && uc<=0xDFFF) || uc==0)   break;   // check for invalid.

               if (uc>=0xD800 && uc<=0xDBFF)   // UTF16 surrogate pairs.
               {
                  if (ptr[1]!='\\' || ptr[2]!='u')   break;   // missing second-half of surrogate.
                  sscanf(ptr+3,"%4x",&uc2);ptr+=6;
                  if (uc2<0xDC00 || uc2>0xDFFF)      break;   // invalid second-half of surrogate.
                  uc=0x10000 | ((uc&0x3FF)<<10) | (uc2&0x3FF);
               }

               len=4;if (uc<0x80) len=1;else if (uc<0x800) len=2;else if (uc<0x10000) len=3; ptr2+=len;
               
               switch (len) {
                  case 4: *--ptr2 =((uc | 0x80) & 0xBF); uc >>= 6;
                  case 3: *--ptr2 =((uc | 0x80) & 0xBF); uc >>= 6;
                  case 2: *--ptr2 =((uc | 0x80) & 0xBF); uc >>= 6;
                  case 1: *--ptr2 =(uc | firstByteMark[len]);
               }
               ptr2+=len;
               break;
            default:  *ptr2++=*ptr; break;
         }
         ptr++;
      }
   }
   *ptr2=0;
   if (*ptr=='\"') 
      ptr++;
   return ptr;
}


static const char *parse_number_size(const char *num)
{
   double n=0,sign=1,scale=0;int subscale=0,signsubscale=1;

   /* Could use sscanf for this? */
   if (*num=='-') 
      sign=-1,num++;   /* Has sign? */
   if (*num=='0') 
      num++;         /* is zero */
   if (*num>='1' && *num<='9')   
   {
      /* Number? */
      do   
      {
         n=(n*10.0)+(*num++ -'0');   
      }
      while (*num>='0' && *num<='9');   
   }
   if (*num=='.' && num[1]>='0' && num[1]<='9') 
   {
      /* Fractional part? */
      sum+=8;
      num++;
      do   
      {
         n=(n*10.0)+(*num++ -'0'),scale--; 
      }
      while (*num>='0' && *num<='9');
   }   
   else
   {
      sum+=4;
   }
   if (*num=='e' || *num=='E')      /* Exponent? */
   {   
      num++;
      if (*num=='+') 
         num++;   
      else if (*num=='-') 
         signsubscale=-1,num++;      /* With sign? */

      while (*num>='0' && *num<='9') 
         subscale=(subscale*10)+(*num++ - '0');   /* Number? */
   }

   n=sign*n*pow(10.0,(scale+subscale*signsubscale));   /* number = +/- number.fraction * 10^+/- exponent */
   
   return num;
}

static const char *parse_value_size(const char *value)
{
   if (!value)                  
      return "";
   if (!strncmp(value,"null",4))   
   {
      sum+=4; 
      return value+4; 
   }
   if (!strncmp(value,"false",5))   
   {
      sum+=5;
      return value+5; 
   }
   if (!strncmp(value,"true",4))   
   {
      sum+=4; 
      return value+4; 
   }
   if (*value=='\"')            
   { 
      return parse_string_size(value); 
   }
   if (*value=='-' || (*value>='0' && *value<='9'))   
   { 
      return parse_number_size(value); 
   }
   if (*value=='[')            
   { 
      sum += 5; 
      return parse_array_size(value); 
   }
   if (*value=='{')            
   { 
      sum += 5; 
      return parse_object_size(value); 
   }

   ep=value;
   return "";   /* failure. */
}

static const char *parse_object_size(const char *value)
{
   if (*value!='{')   {ep=value;return 0;}   /* not an object! */
   
   value=skip(value+1);
   if (*value=='}') return value+1;   /* empty array. */
   
   sum++;

   value = skip( parse_string_size(skip(value)));
   if (!value) return 0;

   if (*value!=':') {ep=value;return 0;}   /* fail! */
   sum+=4;
   value=skip(parse_value_size(skip(value+1)));   /* skip any spacing, get the value. */
   if (!value) return 0;
   
   while (*value==',')
   {
      sum++;
      value=skip(parse_string_size(skip(value+1)));
      if (!value) return 0;

      if (*value!=':') {ep=value;return 0;}   /* fail! */
      sum+=4;
      value=skip(parse_value_size(skip(value+1)));   /* skip any spacing, get the value. */
      if (!value) return 0;
   }
   
   if (*value=='}') return value+1;   /* end of array */
   ep=value;return 0;   /* malformed. */
}

static const char *parse_array_size(const char *value)
{

   if (*value!='[')   {ep=value;return 0;}   /* not an array! */

   value=skip(value+1);
   if (*value==']') return value+1;   /* empty array. */

   value=skip(parse_value_size(skip(value)));   /* skip any spacing, get the value. */
   if (!value) return 0;
    sum+=3;
   while (*value==',')
   {
      value=skip(parse_value_size(skip(value+1)));
      if (!value) return 0;   /* memory fail */
      sum+=3;
   }

   if (*value==']') return value+1;   /* end of array */
   ep=value;return 0;   /* malformed. */
}



int bson_Sum_Size(const char *json_str)
{
   sum = 0;
    ep=0;
   parse_value_size(skip(json_str));
   return sum;
}
