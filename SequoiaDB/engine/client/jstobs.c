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

#include "cJSON.h"
#include "base64c.h"
#include "jstobs.h"

/*
 * check the remaining size
 * x : the remaining size
*/
#define CHECK_LEFT(x) \
{ \
   if ( (*x) <= 0 ) \
      return FALSE ; \
}
static char *intToString ( int value, char *string, int radix )
{
   char tmp[33] ;
   char *tp = tmp ;
   int i ;
   unsigned v ;
   int sign ;
   char *sp ;
   if ( radix > 36 || radix <= 1 )
      return NULL ;
   sign = (radix == 10 && value < 0 ) ;
   if ( sign )
      v = -value ;
   else
      v = (unsigned)value ;
   while ( v || tp == tmp )
   {
      i = v % radix ;
      v = v / radix ;
      if ( i < 10 )
         *tp++ = i + '0' ;
      else
         *tp++ = i + 'a' - 10 ;
   }
   sp = string ;
   if ( sign )
      *sp++ = '-' ;
   while ( tp > tmp )
      *sp++ = *--tp ;
   *sp = 0 ;
   return string ;
}

static const char onethousand_num[1000][4] = {
    "0",  "1",  "2",  "3",  "4",  "5",  "6",  "7",  "8",  "9",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99",

    "100", "101", "102", "103", "104", "105", "106", "107", "108", "109",
    "110", "111", "112", "113", "114", "115", "116", "117", "118", "119",
    "120", "121", "122", "123", "124", "125", "126", "127", "128", "129",
    "130", "131", "132", "133", "134", "135", "136", "137", "138", "139",
    "140", "141", "142", "143", "144", "145", "146", "147", "148", "149",
    "150", "151", "152", "153", "154", "155", "156", "157", "158", "159",
    "160", "161", "162", "163", "164", "165", "166", "167", "168", "169",
    "170", "171", "172", "173", "174", "175", "176", "177", "178", "179",
    "180", "181", "182", "183", "184", "185", "186", "187", "188", "189",
    "190", "191", "192", "193", "194", "195", "196", "197", "198", "199",

    "200", "201", "202", "203", "204", "205", "206", "207", "208", "209",
    "210", "211", "212", "213", "214", "215", "216", "217", "218", "219",
    "220", "221", "222", "223", "224", "225", "226", "227", "228", "229",
    "230", "231", "232", "233", "234", "235", "236", "237", "238", "239",
    "240", "241", "242", "243", "244", "245", "246", "247", "248", "249",
    "250", "251", "252", "253", "254", "255", "256", "257", "258", "259",
    "260", "261", "262", "263", "264", "265", "266", "267", "268", "269",
    "270", "271", "272", "273", "274", "275", "276", "277", "278", "279",
    "280", "281", "282", "283", "284", "285", "286", "287", "288", "289",
    "290", "291", "292", "293", "294", "295", "296", "297", "298", "299",

    "300", "301", "302", "303", "304", "305", "306", "307", "308", "309",
    "310", "311", "312", "313", "314", "315", "316", "317", "318", "319",
    "320", "321", "322", "323", "324", "325", "326", "327", "328", "329",
    "330", "331", "332", "333", "334", "335", "336", "337", "338", "339",
    "340", "341", "342", "343", "344", "345", "346", "347", "348", "349",
    "350", "351", "352", "353", "354", "355", "356", "357", "358", "359",
    "360", "361", "362", "363", "364", "365", "366", "367", "368", "369",
    "370", "371", "372", "373", "374", "375", "376", "377", "378", "379",
    "380", "381", "382", "383", "384", "385", "386", "387", "388", "389",
    "390", "391", "392", "393", "394", "395", "396", "397", "398", "399",

    "400", "401", "402", "403", "404", "405", "406", "407", "408", "409",
    "410", "411", "412", "413", "414", "415", "416", "417", "418", "419",
    "420", "421", "422", "423", "424", "425", "426", "427", "428", "429",
    "430", "431", "432", "433", "434", "435", "436", "437", "438", "439",
    "440", "441", "442", "443", "444", "445", "446", "447", "448", "449",
    "450", "451", "452", "453", "454", "455", "456", "457", "458", "459",
    "460", "461", "462", "463", "464", "465", "466", "467", "468", "469",
    "470", "471", "472", "473", "474", "475", "476", "477", "478", "479",
    "480", "481", "482", "483", "484", "485", "486", "487", "488", "489",
    "490", "491", "492", "493", "494", "495", "496", "497", "498", "499",

    "500", "501", "502", "503", "504", "505", "506", "507", "508", "509",
    "510", "511", "512", "513", "514", "515", "516", "517", "518", "519",
    "520", "521", "522", "523", "524", "525", "526", "527", "528", "529",
    "530", "531", "532", "533", "534", "535", "536", "537", "538", "539",
    "540", "541", "542", "543", "544", "545", "546", "547", "548", "549",
    "550", "551", "552", "553", "554", "555", "556", "557", "558", "559",
    "560", "561", "562", "563", "564", "565", "566", "567", "568", "569",
    "570", "571", "572", "573", "574", "575", "576", "577", "578", "579",
    "580", "581", "582", "583", "584", "585", "586", "587", "588", "589",
    "590", "591", "592", "593", "594", "595", "596", "597", "598", "599",

    "600", "601", "602", "603", "604", "605", "606", "607", "608", "609",
    "610", "611", "612", "613", "614", "615", "616", "617", "618", "619",
    "620", "621", "622", "623", "624", "625", "626", "627", "628", "629",
    "630", "631", "632", "633", "634", "635", "636", "637", "638", "639",
    "640", "641", "642", "643", "644", "645", "646", "647", "648", "649",
    "650", "651", "652", "653", "654", "655", "656", "657", "658", "659",
    "660", "661", "662", "663", "664", "665", "666", "667", "668", "669",
    "670", "671", "672", "673", "674", "675", "676", "677", "678", "679",
    "680", "681", "682", "683", "684", "685", "686", "687", "688", "689",
    "690", "691", "692", "693", "694", "695", "696", "697", "698", "699",

    "700", "701", "702", "703", "704", "705", "706", "707", "708", "709",
    "710", "711", "712", "713", "714", "715", "716", "717", "718", "719",
    "720", "721", "722", "723", "724", "725", "726", "727", "728", "729",
    "730", "731", "732", "733", "734", "735", "736", "737", "738", "739",
    "740", "741", "742", "743", "744", "745", "746", "747", "748", "749",
    "750", "751", "752", "753", "754", "755", "756", "757", "758", "759",
    "760", "761", "762", "763", "764", "765", "766", "767", "768", "769",
    "770", "771", "772", "773", "774", "775", "776", "777", "778", "779",
    "780", "781", "782", "783", "784", "785", "786", "787", "788", "789",
    "790", "791", "792", "793", "794", "795", "796", "797", "798", "799",

    "800", "801", "802", "803", "804", "805", "806", "807", "808", "809",
    "810", "811", "812", "813", "814", "815", "816", "817", "818", "819",
    "820", "821", "822", "823", "824", "825", "826", "827", "828", "829",
    "830", "831", "832", "833", "834", "835", "836", "837", "838", "839",
    "840", "841", "842", "843", "844", "845", "846", "847", "848", "849",
    "850", "851", "852", "853", "854", "855", "856", "857", "858", "859",
    "860", "861", "862", "863", "864", "865", "866", "867", "868", "869",
    "870", "871", "872", "873", "874", "875", "876", "877", "878", "879",
    "880", "881", "882", "883", "884", "885", "886", "887", "888", "889",
    "890", "891", "892", "893", "894", "895", "896", "897", "898", "899",

    "900", "901", "902", "903", "904", "905", "906", "907", "908", "909",
    "910", "911", "912", "913", "914", "915", "916", "917", "918", "919",
    "920", "921", "922", "923", "924", "925", "926", "927", "928", "929",
    "930", "931", "932", "933", "934", "935", "936", "937", "938", "939",
    "940", "941", "942", "943", "944", "945", "946", "947", "948", "949",
    "950", "951", "952", "953", "954", "955", "956", "957", "958", "959",
    "960", "961", "962", "963", "964", "965", "966", "967", "968", "969",
    "970", "971", "972", "973", "974", "975", "976", "977", "978", "979",
    "980", "981", "982", "983", "984", "985", "986", "987", "988", "989",
    "990", "991", "992", "993", "994", "995", "996", "997", "998", "999",
} ;

static void get_char_num ( CHAR *str, INT32 i, INT32 str_size )
{
   if( 1000 > i && 0 < i )
   {
      memcpy( str, onethousand_num[i], 4 );
   }
   else
   {
      memset ( str, 0, str_size ) ;
      
      intToString ( i, str, 10 ) ;
/*
#ifdef WIN32
      _snprintf ( str, str_size, "%d", i ) ;
#else
      snprintf ( str, str_size, "%d", i ) ;
#endif
*/
   }
}

/*
 * time_t convert tm
 * Time: the second time conversion
 * TM : the date structure
 * return : the data structure
*/
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

static INT32 strlen_a ( const CHAR *data )
{
   INT32 len = 0 ;
   if ( !data )
   {
      return 0 ;
   }
   while ( data && *data )
   {
      /*if ( data[0] == '\'' ||
           data[0] == '\"' ||
           data[0] == '\\' )*/
      if ( data[0] == '\"' ||
           data[0] == '\\' ||
           data[0] == '\b' ||
           data[0] == '\f' ||
           data[0] == '\n' ||
           data[0] == '\r' ||
           data[0] == '\t' )
      {
         ++len ;
      }
      ++len ;
      ++data ;  
   }
   return len ;
}

/*
 * copy data to pbuf
 * pbuf : output variable
 * left : the remaining size
 * data : input variable
 * return : void
*/
static void bsonConvertJsonRawConcat ( CHAR **pbuf, INT32 *left, const CHAR *data , BOOLEAN isString )
{
   UINT32 tempsize = 0 ;
   CHAR *pTempBuf = *pbuf ;
   if ( isString )
      tempsize = strlen_a ( data ) ;
   else
      tempsize = strlen ( data ) ;
   tempsize = tempsize > ( UINT32 )(*left) ? ( UINT32 )(*left) : tempsize ;
   if ( isString )
   {
      UINT32 i = 0 ;
      for ( i = 0; i < tempsize; ++i )
      {
         switch ( *data )
         {
         /*case '\'':
         {
           pTempBuf[i] = '\\' ;
           ++i ;
           pTempBuf[i] = '\'' ;
           break ;
         }*/
         case '\"':
         {
           pTempBuf[i] = '\\' ;
           ++i ;
           pTempBuf[i] = '\"' ;
           break ;
         }
         case '\\':
         {
           pTempBuf[i] = '\\' ;
           ++i ;
           pTempBuf[i] = '\\' ;
           break ;
         }
         case '\b':
         {
           pTempBuf[i] = '\\' ;
           ++i ;
           pTempBuf[i] = 'b' ;
           break ;
         }
         case '\f':
         {
           pTempBuf[i] = '\\' ;
           ++i ;
           pTempBuf[i] = 'f' ;
           break ;
         }
         case '\n':
         {
           pTempBuf[i] = '\\' ;
           ++i ;
           pTempBuf[i] = 'n' ;
           break ;
         }
         case '\r':
         {
           pTempBuf[i] = '\\' ;
           ++i ;
           pTempBuf[i] = 'r' ;
           break ;
         }
         case '\t':
         {
           pTempBuf[i] = '\\' ;
           ++i ;
           pTempBuf[i] = 't' ;
           break ;
         }
         default :
         {
            pTempBuf[i] = *data ;
            break ;
         }
         }
         ++data ;
      }
   }
   else
   {
      memcpy ( *pbuf, data, tempsize ) ;
   }
   *left -= tempsize ;
   *pbuf += tempsize ;
}

/*
 * Bson convert Json
 * pbuf : output variable
 * left : the pbuf size
 * data : input bson's data variable
 * isobj : determine the current status
 * return : the conversion result
*/
static BOOLEAN bsonConvertJson ( CHAR **pbuf,
                                 INT32 *left,
                                 const CHAR *data ,
                                 INT32 isobj,
                                 BOOLEAN toCSV,
                                 BOOLEAN skipUndefined )
{
   bson_iterator i ;
   const CHAR *key ;
   bson_timestamp_t ts ;
   CHAR oidhex [ 25 ] ;
   struct tm psr ;
   INT32 first = 1 ;
   if ( *left <= 0 || !pbuf || !data )
      return FALSE ;
   bson_iterator_from_buffer( &i, data ) ;
   if ( !toCSV )
   {
      if ( isobj )
      {
         bsonConvertJsonRawConcat ( pbuf, left, "{ ", FALSE ) ;
         CHECK_LEFT ( left )
      }
      else
      {
         bsonConvertJsonRawConcat ( pbuf, left, "[ ", FALSE ) ;
         CHECK_LEFT ( left )
      }
   }
   while ( bson_iterator_next( &i ) )
   {
      bson_type t = bson_iterator_type( &i ) ;
      /* if BSON_EOO == t ( which is 0 ), that means we hit end of object */
      if ( !t )
      {
         break ;
      }
      if ( skipUndefined && BSON_UNDEFINED == t )
      {
         continue ;
      }
      /* do NOT concat "," for first entrance */
      if ( !first )
      {
         bsonConvertJsonRawConcat ( pbuf, left, toCSV?"|":", ", FALSE ) ;
         CHECK_LEFT ( left )
      }
      else
         first = 0 ;
      /* get key string */
      key = bson_iterator_key( &i ) ;
      /* for object, we always display { " key" : "value" }, so we have to
       * display double quotes and key and comma */
      if ( isobj && !toCSV )
      {
         bsonConvertJsonRawConcat ( pbuf, left, "\"", FALSE ) ;
         CHECK_LEFT ( left )
         bsonConvertJsonRawConcat ( pbuf, left, key, TRUE ) ;
         CHECK_LEFT ( left )
         bsonConvertJsonRawConcat ( pbuf, left, "\"", FALSE ) ;
         CHECK_LEFT ( left )
         bsonConvertJsonRawConcat ( pbuf, left, ": ", FALSE ) ;
         CHECK_LEFT ( left )
      }
      /* then we check the data type */
      switch ( t )
      {
      case BSON_DOUBLE:
      {
         /* for double type, we use 64 bytes string for such big value */
         CHAR temp[ BSON_TEMP_SIZE_512 ] ;
         memset ( temp, 0, BSON_TEMP_SIZE_512 ) ;
#ifdef WIN32
         _snprintf ( temp,
                     BSON_TEMP_SIZE_512,
                     "%.16g", bson_iterator_double( &i ) ) ;
#else
         snprintf ( temp,
                    BSON_TEMP_SIZE_512,
                    "%.16g", bson_iterator_double( &i ) ) ;
#endif
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_STRING:
      case BSON_SYMBOL:
      {
         /* for string type, we output double quote and string data */
         const CHAR *temp = bson_iterator_string( &i ) ;
         if ( !toCSV )
         {
            bsonConvertJsonRawConcat ( pbuf, left, "\"", FALSE ) ;
            CHECK_LEFT ( left )
         }
         bsonConvertJsonRawConcat ( pbuf, left, temp, TRUE ) ;
         CHECK_LEFT ( left )
         if ( !toCSV )
         {
            bsonConvertJsonRawConcat ( pbuf, left, "\"", FALSE ) ;
            CHECK_LEFT ( left )
         }
         break ;
      }
      case BSON_OID:
      {
         /* for oid type, we always display { $oid : "<12 bytes string>" }. So
          * we have to display first part, then concat oidhex, then the last
          * part */
         bson_oid_to_string( bson_iterator_oid( &i ), oidhex );
         if ( !toCSV )
         {
            bsonConvertJsonRawConcat ( pbuf, left, "{ \"$oid\": \"", FALSE ) ;
            CHECK_LEFT ( left )
         }
         bsonConvertJsonRawConcat ( pbuf, left, oidhex, FALSE ) ;
         CHECK_LEFT ( left )
         if ( !toCSV )
         {
            bsonConvertJsonRawConcat ( pbuf, left, "\" }", FALSE ) ;
            CHECK_LEFT ( left )
         }
         break ;
      }
      case BSON_BOOL:
      {
         /* for boolean type, we display either true or false */
         bsonConvertJsonRawConcat ( pbuf, left,
                               (bson_iterator_bool( &i ) ? "true" : "false"), FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_DATE:
      {
         /* for date type, DATE_OUTPUT_FORMAT is the format we need to use, and
          * use snprintf to display */
         CHAR temp[ BSON_TEMP_SIZE_64 ] ;
         struct tm psr;
         time_t timer = bson_iterator_date( &i ) / 1000 ;
         memset ( temp, 0, BSON_TEMP_SIZE_64 ) ;
         local_time ( &timer, &psr ) ;
#ifdef WIN32
         _snprintf ( temp,
                     BSON_TEMP_SIZE_64,
                     toCSV?DATE_OUTPUT_CSV_FORMAT:DATE_OUTPUT_FORMAT,
                     psr.tm_year + RELATIVE_YEAR,
                     psr.tm_mon + 1,
                     psr.tm_mday ) ;
#else
         snprintf ( temp,
                    BSON_TEMP_SIZE_64,
                    toCSV?DATE_OUTPUT_CSV_FORMAT:DATE_OUTPUT_FORMAT,
                    psr.tm_year + RELATIVE_YEAR,
                    psr.tm_mon + 1,
                    psr.tm_mday ) ;
#endif
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_BINDATA:
      {
         CHAR bin_type ;
         CHAR *bin_data ;
         INT32 bin_size ;
         INT32 len = 0 ;
         CHAR *temp = NULL ;
         CHAR *out = NULL ;
         /* TODO: We have to remove malloc here later */
         /* for BINDATA type, user need to input base64 encoded string, which is
          * supposed to be 4 bytes aligned, and we use base64_decode to extract
          * and store in database. For display, we use base64_encode to encode
          * the data stored in database, and format it into base64 encoded
          * string */
         if ( toCSV )
         {
            break ;
         }
         bin_type = bson_iterator_bin_type( &i ) ;
         bin_data = (CHAR *)bson_iterator_bin_data( &i ) ;
         bin_size = bson_iterator_bin_len ( &i ) ;
         /* first we need to calculate how much space we need to put the new
          * data */
         len = getEnBase64Size ( bin_size ) ;
         /* and then we allocate memory for the display string, which includes
          * { $binary : xxxxx, $type : xxx }, so we have to put another 40
          * bytes */
         temp = (CHAR *)malloc( len + 48 ) ;
         if ( !temp )
         {
            return FALSE ;
         }
         memset ( temp, 0, len + 48 ) ;
         /* then we have to allocate another piece of memory for base64 encoding
          */
         out = (CHAR *)malloc( len + 1 ) ;
         if ( !out )
         {
            free( temp ) ;
            return FALSE ;
         }
         memset ( out, 0, len ) ;
         /* encode bin_data to out, with size len */
         if ( !base64Encode( bin_data, bin_size, out, len ) )
         {
            free ( temp ) ;
            free ( out ) ;
            return FALSE ;
         }
#ifdef WIN32
         _snprintf ( temp,
                     len + 48,
                     "{ \"$binary\": \"%s\", \"$type\" : \"%d\" }",
                     out, bin_type ) ;
#else
         snprintf ( temp,
                    len + 48,
                    "{ \"$binary\": \"%s\", \"$type\" : \"%d\" }",
                    out, bin_type ) ;
#endif
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         free( temp ) ;
         free( out ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_UNDEFINED:
      {
         const CHAR *temp = "{ \"$undefined\": 1 }" ;
         /* we don't know how to deal with undefined value at the moment, let's
          * just output it as UNDEFINED, we may change it later */
         if ( toCSV )
         {
            break ;
         }
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_NULL:
      {
         const CHAR *temp = "null" ;
         /* display "null" for null type */
         if ( toCSV )
         {
            break ;
         }
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_MINKEY:
      {
         const CHAR *temp = "{ \"$minKey\": 1 }" ;
         /* display "null" for null type */
         if ( toCSV )
         {
            break ;
         }
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_MAXKEY:
      {
         const CHAR *temp = "{ \"$maxKey\": 1 }" ;
         /* display "null" for null type */
         if ( toCSV )
         {
            break ;
         }
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_REGEX:
      {
         /* for regular expression type, we need to display both regex and
          * options. In raw data format we have 1 byte type, 4 byte length,
          * which includes both pattern and options, and then pattern string and
          * options string. */
         if ( toCSV )
         {
            break ;
         }
         bsonConvertJsonRawConcat ( pbuf, left, "{ \"$regex\": \"", FALSE ) ;
         CHECK_LEFT ( left )
         /* get pattern string */
         bsonConvertJsonRawConcat ( pbuf, left, bson_iterator_regex ( &i ), TRUE ) ;
         CHECK_LEFT ( left )
         /* bson_iterator_regex_opts get options by "p+strlen(p)+1", which means
          * we don't need to move iterator to next element. So we use
          * bson_iterator_regex_opts directly on &i */
         bsonConvertJsonRawConcat ( pbuf, left, "\", \"$options\": \"", FALSE ) ;
         CHECK_LEFT ( left )
         bsonConvertJsonRawConcat ( pbuf, left, bson_iterator_regex_opts ( &i ), FALSE ) ;
         CHECK_LEFT ( left )
         bsonConvertJsonRawConcat ( pbuf, left, "\" }", FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_CODE:
      {
         /* we don't know how to deal with code at the moment, let's just
          * display it as normal string */
         if ( toCSV )
         {
            break ;
         }
         bsonConvertJsonRawConcat ( pbuf, left, "\"", FALSE ) ;
         CHECK_LEFT ( left )
         bsonConvertJsonRawConcat ( pbuf, left, bson_iterator_code( &i ), TRUE ) ;
         CHECK_LEFT ( left )
         bsonConvertJsonRawConcat ( pbuf, left, "\"", FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_INT:
      {
         /* format integer. Instead of using snprintf, we call get_char_num(),
          * which uses static string to improve performance ( when value < 1000
          * ) */
         CHAR temp[ BSON_TEMP_SIZE_32 ] = {0} ;
         get_char_num ( temp, bson_iterator_int( &i ), BSON_TEMP_SIZE_32 ) ;
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_LONG:
      {
         /* for 64 bit integer, most likely it's more than 1000, so we always
          * snprintf */
         CHAR temp[ BSON_TEMP_SIZE_512 ] ;
         memset ( temp, 0, BSON_TEMP_SIZE_512 ) ;
#ifdef WIN32
         _snprintf ( temp,
                     BSON_TEMP_SIZE_512,
                     "%lld",
                     ( unsigned long long )bson_iterator_long( &i ) ) ;
#else
         snprintf ( temp,
                    BSON_TEMP_SIZE_512,
                    "%lld",
                    ( unsigned long long )bson_iterator_long( &i ) ) ;
#endif
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_TIMESTAMP:
      {
         /* for timestamp, it's yyyy-mm-dd-hh.mm.ss.uuuuuu */
         CHAR temp[ BSON_TEMP_SIZE_64 ] = {0} ;
         time_t timer ;
         memset ( temp, 0, BSON_TEMP_SIZE_64 ) ;
         ts = bson_iterator_timestamp( &i ) ;
         timer = (time_t)( ts.t ) ;
         local_time ( &timer, &psr ) ;
#ifdef WIN32
         _snprintf ( temp,
                     BSON_TEMP_SIZE_64,
                     toCSV?TIME_OUTPUT_CSV_FORMAT:TIME_OUTPUT_FORMAT,
                     psr.tm_year + RELATIVE_YEAR,
                     psr.tm_mon + 1,
                     psr.tm_mday,
                     psr.tm_hour,
                     psr.tm_min,
                     psr.tm_sec,
                     ts.i ) ;
#else
         snprintf ( temp,
                    BSON_TEMP_SIZE_64,
                    toCSV?TIME_OUTPUT_CSV_FORMAT:TIME_OUTPUT_FORMAT,
                    psr.tm_year + RELATIVE_YEAR,
                    psr.tm_mon + 1,
                    psr.tm_mday,
                    psr.tm_hour,
                    psr.tm_min,
                    psr.tm_sec,
                    ts.i ) ;
#endif
         bsonConvertJsonRawConcat ( pbuf, left, temp, FALSE ) ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_OBJECT:
      {
         /* for object type, we do recursive call with TRUE */
         if ( toCSV )
         {
            break ;
         }
         if ( !bsonConvertJson( pbuf, left, bson_iterator_value( &i ) ,
                                1, toCSV, skipUndefined ) )
            return  FALSE ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_ARRAY:
      {
         /* for array type, we do recursive call with FALSE */
         if ( toCSV )
         {
            break ;
         }
         if ( !bsonConvertJson( pbuf, left, bson_iterator_value( &i ),
                                0, toCSV, skipUndefined ) )
            return FALSE ;
         CHECK_LEFT ( left )
         break ;
      }
      default:
         return FALSE ;
      }
   }
   if ( !toCSV )
   {
      if ( isobj )
      {
         bsonConvertJsonRawConcat ( pbuf, left, " }", FALSE ) ;
         CHECK_LEFT ( left )
      }
      else
      {
         bsonConvertJsonRawConcat ( pbuf, left, " ]", FALSE ) ;
         CHECK_LEFT ( left )
      }
   }
   return TRUE ;
}
/*
 * Json converslion Bson
 * cj : the cJson object
 * bs : the bson object
 * isOjb : determine the current status
 * return : the conversion result
*/
/* This function takes input from cJSON library, which parses a string to linked
 * list. In this function we iterate all elements in list and construct BSON
 * accordingly */
static BOOLEAN jsonConvertBson ( cJSON *cj, bson *bs, BOOLEAN isObj )
{
   INT32 i = 0 ;
   /* loop until reach end of element */
   while ( cj )
   {
      switch ( cj->type )
      {
      case cJSON_Number:
      {
         /* for number type, it could be 64 bit float, 32 bit int, or 64 bit int
          */
         if ( cJSON_DOUBLE == cj->numType )
         {
            /* for 64 bit float */
            if ( isObj && cj->string )
               /* if it's obj type, we append with key and value */
               bson_append_double ( bs, cj->string, cj->valuedouble ) ;
            else
            {
               /* if it's array type, we append with array location and value */
               CHAR num[ INT_NUM_SIZE ] = {0} ;
               get_char_num ( num, i, INT_NUM_SIZE ) ;
               bson_append_double ( bs, num, cj->valuedouble ) ;
            }
         }
         else if ( cJSON_INT32 == cj->numType )
         {
            /* for 32 bit int */
            if ( isObj && cj->string )
               bson_append_int ( bs, cj->string, cj->valueint ) ;
            else
            {
               CHAR num[ INT_NUM_SIZE ] = {0} ;
               get_char_num ( num, i, INT_NUM_SIZE ) ;
               bson_append_int ( bs, num, cj->valueint ) ;
            }
         }
         else if ( cJSON_INT64 == cj->numType )
         {
            /* for 64 bit int */
            if ( isObj && cj->string )
               bson_append_long ( bs, cj->string, cj->valuelongint ) ;
            else
            {
               CHAR num[ INT_NUM_SIZE ] = {0} ;
               get_char_num ( num, i, INT_NUM_SIZE ) ;
               bson_append_long ( bs, num, cj->valuelongint ) ;
            }
         }
         break ;
      }
      case cJSON_NULL:
      {
         /* for null type */
         if ( isObj && cj->string )
            bson_append_null ( bs, cj->string ) ;
         else
         {
            CHAR num[ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            bson_append_null ( bs, num ) ;
         }
         break ;
      }
      case cJSON_True:
      case cJSON_False:
      {
         /* for boolean */
         if ( isObj && cj->string )
            bson_append_bool ( bs, cj->string, cj->type ) ;
         else
         {
            CHAR num[ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            bson_append_bool ( bs, num, cj->type ) ;
         }
         break ;
      }
      case cJSON_String:
      {
         /* string type */
         if ( isObj && cj->string )
            bson_append_string ( bs, cj->string, cj->valuestring ) ;
         else
         {
            CHAR num[ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            bson_append_string ( bs, num, cj->valuestring ) ;
         }
         break ;
      }
      case cJSON_Timestamp:
      case cJSON_Date:
      {
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
         if ( cJSON_Timestamp == cj->type )
         {
            /* for timestamp type, we provide yyyy-mm-dd-hh.mm.ss.uuuuuu */
            if ( !sscanf ( cj->valuestring,
                             TIME_FORMAT,
                             &year   ,
                             &month  ,
                             &day    ,
                             &hour   ,
                             &minute ,
                             &second ,
                             &micros ) )
            {
              return FALSE ;
            }
         }
         else
         {
            /* for date type, we provide yyyy-mm-dd */
            if ( !sscanf ( cj->valuestring,
                           DATE_FORMAT,
                           &year,
                           &month,
                           &day ) )
                  return FALSE ;
         }
         --month ;
         /* sanity check for years */
         if( year    >=    INT32_LAST_YEAR   ||
             month   >=    RELATIVE_MOD      || //[0,11]
             month   <     0                 ||
             day     >     RELATIVE_DAY      || //[1,31]
             day     <=    0 )
         {
            return FALSE ;
         }
         if( cJSON_Timestamp == cj->type && (
             hour    >=    RELATIVE_HOUR     || //[0,23]
             hour    <     0                 ||
             minute  >=    RELATIVE_MIN_SEC  || //[0,59]
             minute  <     0                 ||
             second  >=    RELATIVE_MIN_SEC  || //[0,59]
             second  <     0                )
           )
         {
            return FALSE ;
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
            return FALSE ;
         /* append timestamp or date accordingly */
         if ( cJSON_Timestamp == cj->type )
         {
            if ( isObj && cj->string )
               bson_append_timestamp2 ( bs,
                                        cj->string,
                                        (((INT32)timep) * 1),
                                        micros ) ;
            else
            {
               CHAR num[ INT_NUM_SIZE  ] = {0} ;
               get_char_num ( num, i, INT_NUM_SIZE ) ;
               bson_append_timestamp2 ( bs, num,
                                        ((INT32)timep * 1) , micros ) ;
            }
         }
         else
         {
            bson_date_t s = ((( bson_date_t )timep) * 1000 ) ;

            if ( isObj && cj->string )
               bson_append_date ( bs, cj->string, s ) ;
            else
            {
               CHAR num[ INT_NUM_SIZE ] = {0} ;
               get_char_num ( num, i, INT_NUM_SIZE ) ;
               bson_append_date ( bs, num, s ) ;
            }
         }
         break ;
      }
      case cJSON_Regex:
      {
         /* for regex type, we have both pattern and options */
         if ( isObj && cj->string )
            bson_append_regex( bs,
                               cj->string,
                               cj->valuestring,
                               cj->valuestring2 ) ;
         else
         {
            CHAR num[ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            bson_append_regex( bs, num , cj->valuestring, cj->valuestring2 ) ;
         }
         break ;
      }
      case cJSON_Oid:
      {
         /* for oid, we format input into oid and then append into bson */
         bson_oid_t bot ;
         bson_oid_from_string ( &bot, cj->valuestring ) ;
         if ( isObj && cj->string )
         {
            bson_append_oid( bs, cj->string , &bot ) ;
         }
         else
         {
            CHAR num [ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            bson_append_oid( bs, num, &bot ) ;
         }
         break ;
      }
      case cJSON_MinKey:
      {
         if ( isObj && cj->string )
         {
            bson_append_minkey( bs, cj->string ) ;
         }
         else
         {
            CHAR num [ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            bson_append_minkey( bs, num ) ;
         }
         break ;
      }
      case cJSON_MaxKey:
      {
         if ( isObj && cj->string )
         {
            bson_append_maxkey( bs, cj->string ) ;
         }
         else
         {
            CHAR num [ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            bson_append_maxkey( bs, num ) ;
         }
         break ;
      }
      case cJSON_Undefined:
      {
         if ( isObj && cj->string )
         {
            bson_append_undefined( bs, cj->string ) ;
         }
         else
         {
            CHAR num [ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            bson_append_undefined( bs, num ) ;
         }
         break ;
      }
      case cJSON_Binary:
      {
         /* for binary type, user input base64 encoded string, which should be
          * 4 bytes aligned, and then call base64_decode to extract into binary
          * and store in BSON object */
         if ( isObj && cj->string )
         {
            INT32 out_len = 0 ;
            /* first we calculate the expected size after extraction */
            INT32 len = getDeBase64Size ( cj->valuestring ) ;
            /* and allocate memory */
            CHAR *out = (CHAR *)malloc ( len ) ;
            if ( !out )
               return FALSE ;
            memset ( out, 0, len ) ;
            /* and then decode into the buffer we just allocated */
            if ( !base64Decode( cj->valuestring, out, len ) )
            {
               free ( out ) ;
               return FALSE ;
            }
            out_len = len - 1 ;
            if ( 5 == cj->valueint &&
               CJSON_MD5_16 != out_len &&
               CJSON_MD5_32 != out_len &&
               CJSON_MD5_64 != out_len )
               return FALSE ;
            if ( 3 == cj->valueint && CJSON_UUID != out_len )
               return FALSE ;
            /* and then append into bson */
            bson_append_binary( bs, cj->string , cj->valueint, out, out_len ) ;
            free( out ) ;
         }
         else
         {
            CHAR num[ INT_NUM_SIZE ] = {0} ;
            INT32 len = 0 ;
            CHAR *out = NULL ;
            INT32 out_len = 0 ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            len = getDeBase64Size ( cj->valuestring ) ;

            out = (CHAR *)malloc(len) ;
            if ( !out )
               return FALSE ;
            memset ( out, 0, len ) ;
            if ( !base64Decode( cj->valuestring, out, len ) )
            {
               free ( out ) ;
               return FALSE ;
            }
            out_len = len - 1 ;
            if ( 5 == cj->valueint &&
               CJSON_MD5_16 != out_len &&
               CJSON_MD5_32 != out_len &&
               CJSON_MD5_64 != out_len )
               return FALSE ;
            if ( 3 == cj->valueint && CJSON_UUID != out_len )
               return FALSE ;
            bson_append_binary( bs, num , cj->valueint, out, out_len ) ;
            free ( out ) ;
         }
         break ;
      }
      case cJSON_Object:
      {
         /* for object type, we call jsonConvertBson recursively, and provide
          * cj->child as object input */
         if ( isObj && cj->string )
         {
            if ( bson_append_start_object ( bs, cj->string ) )
               return FALSE ;
         }
         else
         {
            CHAR num[ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            if ( bson_append_start_object ( bs, num ) )
               return FALSE ;
         }
         if ( !jsonConvertBson ( cj->child, bs, TRUE ) )
            return FALSE ;
         bson_append_finish_object ( bs ) ;
         break ;
      }
      case cJSON_Array:
      {
         /* for array type, we call jsonConvertBson recursively, and provide
          * cj->child as array input */
         if ( isObj && cj->string )
         {
            if ( bson_append_start_array ( bs, cj->string ) )
               return FALSE ;
         }
         else
         {
            CHAR num[ INT_NUM_SIZE ] = {0} ;
            get_char_num ( num, i, INT_NUM_SIZE ) ;
            if ( bson_append_start_array ( bs, num ) )
               return FALSE ;
         }
         if ( !jsonConvertBson ( cj->child, bs, FALSE ) )
            return FALSE ;
         bson_append_finish_array ( bs ) ;
         break ;
      }
      }
      cj = cj->next ;
      ++i ;
   }
   return TRUE ;
}

/*
 * jscon convert bson interface
 * bs : bson object
 * json_str : json string
 * return : the conversion result
*/
/* THIS IS EXTERNAL FUNCTION TO CONVERT FROM JSON STRING INTO BSON OBJECT */
BOOLEAN jsonToBson2 ( bson *bs,
                      const CHAR *json_str,
                      BOOLEAN isMongo,
                      BOOLEAN isBatch )
{
   BOOLEAN flag = TRUE ;
   cJSON *cj = cJSON_Parse2 ( json_str, isMongo, isBatch ) ;
   if ( !cj || !bs )
   {
      cJSON_Delete ( cj ) ;
      return FALSE ;
   }
   bson_init( bs ) ;
   if ( cj->child )
      flag = jsonConvertBson ( cj->child, bs, TRUE ) ;
   else
      bs = bson_empty ( bs ) ;
   bson_finish ( bs ) ;
   cJSON_Delete ( cj ) ;

   return flag ;
}

BOOLEAN jsonToBson ( bson *bs, const CHAR *json_str )
{
   return jsonToBson2 ( bs, json_str, FALSE, FALSE ) ;
}
/*
 * bson convert json interface
 * buffer : output bson convert json string
 * bufsize : buffer's size
 * b : bson object
 * return : the conversion result
*/
/* THIS IS EXTERNAL FUNCTION TO CONVERT FROM BSON OBJECT INTO JSON STRING */
BOOLEAN bsonToJson ( CHAR *buffer, INT32 bufsize, const bson *b,
                     BOOLEAN toCSV, BOOLEAN skipUndefined )
{
    CHAR *pbuf = buffer ;
    BOOLEAN result = FALSE ;
    INT32 leftsize = bufsize ;
    if ( bufsize <= 0 || !buffer || !b )
       return FALSE ;
    result = bsonConvertJson ( &pbuf, &leftsize, b->data, 1, toCSV, skipUndefined ) ;
    if ( !result || !leftsize )
       return FALSE ;
    *pbuf = '\0' ;
    return TRUE ;
}

BOOLEAN bsonElementToChar ( CHAR **buffer, INT32 *bufsize, bson_iterator *in )
{
   bson_type t ;
   if ( !bufsize || !in )
   {
      return FALSE ;
   }
   t = bson_iterator_type( in ) ;
   switch ( t )
   {
   case BSON_OID:
   {
      CHAR temp[4096] = {0} ;
      *bufsize = 64 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      bson_oid_to_string ( bson_iterator_oid( in ), temp ) ;
      *bufsize = sprintf ( *buffer, "{\"$oid\":\"%s\"}", temp ) ;
      return TRUE ;
   }
   case BSON_STRING:
   {
      const CHAR *temp = bson_iterator_string( in ) ;
      *bufsize = strlen ( temp ) + 3 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "\"%s\"", temp ) ;
      return TRUE ;
   }
   case BSON_INT:
   {
      *bufsize = 33 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "%d", bson_iterator_int( in ) ) ;
      return TRUE ;
   }
   case BSON_LONG:
   {
      *bufsize = 33 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "%lld", ( unsigned long long )bson_iterator_long( in ) ) ;
      return TRUE ;
   }
   case BSON_DOUBLE:
   {
      *bufsize = 65 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "%f", bson_iterator_double( in ) ) ;
      return TRUE ;
   }
   case BSON_CODE:
   {
      *bufsize = 130 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "\"%s\"", bson_iterator_code( in ) ) ;
      return TRUE ;
   }
   case BSON_BOOL:
   {
      *bufsize = 6 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "%s", (bson_iterator_bool( in ) ? "true" : "false") ) ;
      return TRUE ;
   }
   case BSON_DATE:
   {
      time_t timer ;
      struct tm psr;
      *bufsize = 64 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      timer = bson_iterator_date( in );
      local_time ( &timer, &psr ) ;
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "{\"$date\":\"%04d-%02d-%02d\" }", psr.tm_year + 1900, psr.tm_mon, psr.tm_mday ) ;
      return TRUE ;
   }
   case BSON_BINDATA:
   {
      *bufsize = 15 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "\"BSON_BINDATA\"" ) ;
      return TRUE ;
   }
   case BSON_UNDEFINED:
   {
      *bufsize = 17 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "\"BSON_UNDEFINED\"" ) ;
      return TRUE ;
   }
   case BSON_NULL:
   {
      *bufsize = 5 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "null" ) ;
      return TRUE ;
   }
   case BSON_REGEX:
   {
      *bufsize = 131 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "\"%s\"", bson_iterator_regex( in ) ) ;
      return TRUE ;
   }
   case BSON_TIMESTAMP:
   {
      bson_timestamp_t ts ;
      time_t timer ;
      struct tm psr;
      *bufsize = 128 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      ts = bson_iterator_timestamp( in ) ;
      timer = (time_t)ts.t ;
      local_time ( &timer, &psr ) ;
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, TIME_OUTPUT_FORMAT, psr.tm_year + 1900,
                           psr.tm_mon+1, psr.tm_mday, psr.tm_hour, psr.tm_min,
                           psr.tm_sec, ts.i ) ;
      return TRUE ;
   }
   case BSON_SYMBOL:
   {
      const CHAR *temp = bson_iterator_string( in ) ;
      *bufsize = strlen ( temp ) + 3 ;
      *buffer = (CHAR *)malloc ( *bufsize ) ;
      if ( !(*buffer) )
      {
         return FALSE ;
      }
      memset ( *buffer, 0, *bufsize ) ;
      *bufsize = sprintf ( *buffer, "\"%s\"", temp ) ;
      return TRUE ;
   }
   case BSON_OBJECT:
   case BSON_ARRAY:
   default:
      return FALSE ;
   }
}
