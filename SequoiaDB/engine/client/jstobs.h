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

/** \file Jstobs.h
    \brief Json and Bson convert to each other.
*/
#include "core.h"
#include "time.h"
#include "bson/bson.h"

#define INT_NUM_SIZE 32
#define DOU_NUM_SIZE 32
#define BSON_TEMP_SIZE_6 6
#define BSON_TEMP_SIZE_32 32
#define BSON_TEMP_SIZE_64 64
#define BSON_TEMP_SIZE_128 128
#define BSON_TEMP_SIZE_256 256
#define BSON_TEMP_SIZE_512 512

#define CJSON_MD5_16 16
#define CJSON_MD5_32 32
#define CJSON_MD5_64 64
#define CJSON_UUID 36

#define INT32_LAST_YEAR 2038
#define RELATIVE_YEAR 1900
#define RELATIVE_MOD 12
#define RELATIVE_DAY 31
#define RELATIVE_HOUR 24
#define RELATIVE_MIN_SEC 60

#define TIME_FORMAT "%d-%d-%d-%d.%d.%d.%d"
#define TIME_OUTPUT_CSV_FORMAT "%04d-%02d-%02d-%02d.%02d.%02d.%06d"
#define TIME_OUTPUT_FORMAT "{ \"$timestamp\": \"" TIME_OUTPUT_CSV_FORMAT "\" }"
#define DATE_FORMAT "%d-%d-%d"
#define DATE_OUTPUT_CSV_FORMAT "%04d-%02d-%02d"
#define DATE_OUTPUT_FORMAT "{ \"$date\": \"" DATE_OUTPUT_CSV_FORMAT "\" }"

SDB_EXTERN_C_START

/** \fn 
*/

/** \fn BOOLEAN jsonToBson ( bson *bs, const CHAR *json_str )
    \brief Json converts to bson.
    \param [in] json_str The json string to convert 
    \param [out] bs The return bson object 
    \retval TRUE Operation Success
    \retval FALSE Operation Fail
*/
SDB_EXPORT BOOLEAN jsonToBson ( bson *bs, const CHAR *json_str ) ;
SDB_EXPORT BOOLEAN jsonToBson2 ( bson *bs,
                                 const CHAR *json_str,
                                 BOOLEAN isMongo,
                                 BOOLEAN isBatch ) ;
/** \fn BOOLEAN bsonToJson ( CHAR *buffer, INT32 bufsize, const bson *b,
                             BOOLEAN toCSV, BOOLEAN skipUndefined) 
    \brief Bson converts to json.
    \param [in] buffer the buffer to convert
    \param [in] bufsize the buffer's size
    \param [in] b The bson object to convert
    \param [in] toCSV bson to csv or not
    \param [in] skipUndefined to skip undefined filed or not
    \param [out] buffer The return json string 
    \retval TRUE Operation Success
    \retval FALSE Operation Fail
    \note Before calling this funtion,need to build up
             a buffer for the convertion result.
*/
SDB_EXPORT BOOLEAN bsonToJson ( CHAR *buffer, INT32 bufsize, const bson *b,
                                BOOLEAN toCSV, BOOLEAN skipUndefined ) ;

SDB_EXPORT BOOLEAN bsonElementToChar ( CHAR **buffer, INT32 *bufsize,
                                       bson_iterator *in ) ;

SDB_EXTERN_C_END
