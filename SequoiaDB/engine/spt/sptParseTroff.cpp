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

   Source File Name = sptParseTroff.cpp

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

#include "core.h"
#include "ossUtil.h"
#include "sptParseTroff.hpp"
#include "ossErr.h"
#include "ossMem.h"
#include "../mdocml/parseMandocCpp.hpp"
#include "ossTypes.h"
#include <algorithm>
#include <vector>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

#define MFILE_SUFFIX ".cli"

#define DB_CATEGORY         "db"
#define CS_CATEGORY         "cs"
#define CL_CATEGORY         "cl"
#define RG_CATEGORY         "rg"
#define NODE_CATEGORY       "node"
#define CURSOR_CATEGORY     "cursor"
#define CLCOUNT_CATEGORY    "count"
#define DOMAIN_CATEGORY     "domain"
#define OMA_CATEGORY        "oma"
#define QUERY_CATEGORY      "query"
#define QUERY_GEN_CATEGORY  "query_gen"
#define QUERY_COND_CATEGORY "query_cond"
#define QUERY_CURS_CATEGORY "query_curs"

#define READ_CHARACTOR_NUM 1024
#define CMD_NAME_LEN  255

#define READ_CUTLINE_BEGIN ".SH \"NAME\""
#define READ_CUTLINE_END  ".SH \"SYNOPSIS\""
#define READ_SYN_BEGIN READ_CUTLINE_END
#define READ_SYN_END ".SH \"CATEGORY\""
#define MARK1 "\\fB"
#define MARK2 "\\fR"
#define MARK3 "("

#define INDENT_WIDTH1  3
#define INDENT_WIDTH2  30
#define CONTENT_LEN    50
#define FORMAT_LEN     100

#define SPLIT_POINT1 '.'
#define SPLIT_POINT2 '_'

namespace fs = boost::filesystem ;

#define CATE_SIZE  13
const CHAR* CATE_ARR[ CATE_SIZE ] =
{
   DB_CATEGORY,
   CS_CATEGORY,
   CL_CATEGORY,
   RG_CATEGORY,
   NODE_CATEGORY,
   CURSOR_CATEGORY,
   CLCOUNT_CATEGORY,
   DOMAIN_CATEGORY,
   OMA_CATEGORY,
   QUERY_CATEGORY,
   QUERY_GEN_CATEGORY,
   QUERY_COND_CATEGORY,
   QUERY_CURS_CATEGORY
 } ;

static INT32 removeMark( CHAR *buffer, INT32 buffer_len, const CHAR *mark ) ;

static INT32 replaceSubStr( CHAR *buffer, INT32 buffer_len,
                            const CHAR *pStr1, const CHAR *pStr2 ) ;

static INT32 checkBuffer ( CHAR **ppBuffer, INT32 *bufferSize,
                           INT32 length ) ;

static BOOLEAN isCategory( const CHAR *pName, const CHAR** arr, INT32 size ) ;

static INT32 display( const CHAR *first, const CHAR *second, INT32 indent1,
                      INT32 indent2 ) ;

static INT32 splitCutline( const CHAR *cutline, vector<string> &vec ) ;

static INT32 getSplitPos( const CHAR *pCur, INT32 part_len, INT32 *ret ) ;



manHelp& manHelp::getInstance( const CHAR *path )
{
   static manHelp _instance( path ) ;
   return _instance ;
}

manHelp::manHelp( const CHAR *path )
{
   INT32 rc = SDB_OK;
   INT32 len = ossStrlen( path );
   ossMemcpy( _filePath, path , len );
   _filePath[len] = 0;
   troffFileNotEixt = FALSE ;
   rc = scanFile();
   if ( rc )
   {
      troffFileNotEixt = TRUE ;
   }
   _classify.insert( pair< string, ssmap_ref >( string(DB_CATEGORY),
                                                _db._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(CS_CATEGORY),
                                                _cs._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(CL_CATEGORY),
                                                _cl._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(RG_CATEGORY),
                                                _rg._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(NODE_CATEGORY),
                                                _node._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(CURSOR_CATEGORY),
                                                _cursor._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(CLCOUNT_CATEGORY),
                                                _clcount._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(DOMAIN_CATEGORY),
                                                _domain._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(OMA_CATEGORY),
                                                _oma._first ) ) ;
   _classify.insert( pair< string, ssmap_ref >( string(QUERY_CATEGORY),
                                                _query._first ) ) ;
}

manHelp::~manHelp() {}

INT32 manHelp::scanFile()
{
   INT32 rc = SDB_OK;
   INT32 tmp_buf_len = READ_CHARACTOR_NUM ;
   INT32 file_buf_len = READ_CHARACTOR_NUM ;
   CHAR *file_buffer = (CHAR *)SDB_OSS_MALLOC( file_buf_len ) ;
   CHAR *tmp_buffer = (CHAR *)SDB_OSS_MALLOC( tmp_buf_len + 1 ) ;
   typedef vector<fs::path> vec;
   vec v;

   const INT32 file_name_len = OSS_PROCESS_NAME_LEN ;
   fs::path p( _filePath );
   CHAR *pFileName = NULL ;

   pFileName = (CHAR *)SDB_OSS_MALLOC( file_name_len + 1 );
   if ( !pFileName )
   {
      ossPrintf( "Memory malloc failed(size = %d), %s:%d "OSS_NEWLINE,
                 file_name_len + 1,  __FILE__, __LINE__ ) ;
      rc = SDB_OOM ;
      goto error ;
   }


   if ( !fs::exists(p) || !fs::is_directory(p) )
   {
      rc = SDB_INVALIDARG;
      goto error;
   }
   std::copy( fs::directory_iterator(p), fs::directory_iterator(),
              std::back_inserter(v) );
   for ( vec::const_iterator it(v.begin()), it_end(v.end());
         it != it_end; it++ )
   {
      string fPath = (*it).string() ;
      const CHAR *pfPath = fPath.c_str() ;
      string fSuffix = fs::extension(*it);
      const CHAR *pfSuffix = fSuffix.c_str();
      if ( ossStrncmp( pfSuffix, MFILE_SUFFIX, ossStrlen(MFILE_SUFFIX) ) == 0 )
      {
         INT32 strLen = 0;
         string fileName ;
         string fileNameLower ;
         string categoryName ; // db, cs, cl ...
         string funcName ; // createCL, createCS ...
         const CHAR* pLeaf = NULL;
         CHAR *pSplit      = NULL ;
         CHAR *pSplit2     = NULL ;
         std::string leaf = (*it).leaf().string();
         pLeaf = leaf.c_str();
         strLen = ossStrlen( pLeaf ) - ossStrlen( MFILE_SUFFIX );
         if ( strLen > file_name_len )
         {
            ossPrintf( "Invalid arguments, %s:%d "OSS_NEWLINE,
                        __FILE__, __LINE__ ) ;
            rc = SDB_INVALIDARG ;
            goto error;
         }
         ossMemcpy( pFileName, pLeaf, strLen );
         *(pFileName + strLen) = 0;
         fileName =  pFileName ;
         fileNameLower = pFileName ;

         boost::to_lower( fileNameLower ) ;
         _nmap.insert( pair<string, string>(fileNameLower, fileName) ) ;
         pSplit = ossStrrchr( pFileName, SPLIT_POINT1 ) ;
         if ( !pSplit )
         {
            ossPrintf( "Invalid arguments, %s:%d "OSS_NEWLINE,
                        __FILE__, __LINE__ ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         *pSplit = '\0' ;
         funcName = ++pSplit ;
         if ( isCategory( pFileName, CATE_ARR, CATE_SIZE ) )
         {
            std::ifstream fin ;
            INT32 troff_file_len = 0 ;
            INT32 read_real_len = 0 ;
            INT32 cutline_len = 0 ;
            CHAR* r_beg = NULL ;
            CHAR* r_end = NULL ;
            CHAR* r_pos = NULL ;
            std::string cutline ;
            std::string synopsis ;
            fin.open( pfPath ) ;
            fin.seekg ( 0, fin.end ) ;
            troff_file_len = fin.tellg() ;
            fin.seekg ( 0, fin.beg ) ;
            rc = checkBuffer ( &file_buffer, &file_buf_len, troff_file_len ) ;
            if ( rc )
            {
               goto exit ;
            }
            fin.read ( file_buffer, troff_file_len ) ;

            r_beg = ossStrstr( file_buffer, READ_CUTLINE_BEGIN ) ;
            if ( !r_beg )
            {
               ossPrintf( "Failed to deal with file %s, for having no "
                          "tag \"NAME\""OSS_NEWLINE, pfPath ) ;
               rc = SDB_INVALIDARG ;
               goto exit ;
            }
            r_end = ossStrstr( file_buffer, READ_CUTLINE_END ) ;
            if ( !r_end )
            {
               ossPrintf( "Failed to deal with file %s, for having no "
                          "tag \"SYNOPSIS\""OSS_NEWLINE, pfPath ) ;
               rc = SDB_INVALIDARG ;
               goto exit ;
            }
            *r_end = '\0' ;
            r_pos =  ossStrstr( r_beg, pSplit ) ;
            if ( !r_pos )
            {
               ossPrintf ( "Failed to deal with file %s, for the content of "
                           "tag \"NAME\" having no short name %s"OSS_NEWLINE,
                           pfPath, pSplit ) ;
               rc = SDB_INVALIDARG ;
               goto exit ;
            }
            read_real_len = ossStrlen( r_pos ) ;
            rc = checkBuffer ( &tmp_buffer, &tmp_buf_len, read_real_len ) ;
            if ( rc )
            {
               ossPrintf( "Failed to check buffer, %s:%d "OSS_NEWLINE,
                           __FILE__, __LINE__ ) ;
               goto exit ;
            }
            cutline_len = read_real_len - ossStrlen(pSplit) ;
            ossMemcpy( tmp_buffer, r_pos + ossStrlen(pSplit), cutline_len ) ;
            tmp_buffer[ cutline_len ] = '\0' ;
            #if defined ( _WINDOWS )
            rc = replaceSubStr( tmp_buffer, tmp_buf_len, "\r\n", " " ) ;
            #else
            rc = replaceSubStr( tmp_buffer, tmp_buf_len, "\n", " " ) ;
            #endif
            rc = replaceSubStr( tmp_buffer, tmp_buf_len, "\f", " " ) ;
            cutline = tmp_buffer ;
            r_beg = r_end + 1 ;
            r_pos = NULL ;
            r_end = NULL ;
            r_end = ossStrstr( r_beg, READ_SYN_END ) ;
            if ( !r_end )
            {
               ossPrintf( "Failed to deal with file %s, for having no tag "
                          "\"CATEGORY\""OSS_NEWLINE, pfPath ) ;
               rc = SDB_INVALIDARG ;
               goto exit ;
            }
            *r_end = '\0' ;
            r_pos = ossStrstr ( r_beg, pSplit ) ;
            if ( !r_pos )
            {
               ossPrintf ( "Failed to deal with file %s, for the content of "
                           "tag \"SYNOPIS\" having no short name %s"OSS_NEWLINE,
                           pfPath, pSplit ) ;
               rc = SDB_INVALIDARG ;
               goto exit ;
            }
            read_real_len = ossStrlen( r_pos ) ;
            rc = checkBuffer ( &tmp_buffer, &tmp_buf_len, read_real_len ) ;
            if ( rc )
            {
               ossPrintf( "Failed to check buffer, %s:%d "OSS_NEWLINE,
                           __FILE__, __LINE__ ) ;
               goto exit ;
            }
            ossMemcpy( tmp_buffer, r_pos, read_real_len ) ;
            tmp_buffer[read_real_len] = '\0' ;
            rc = removeMark( tmp_buffer, read_real_len, MARK1 ) ;
            rc = removeMark( tmp_buffer, read_real_len, MARK2 ) ;
            #if defined ( _WINDOWS )
            rc = replaceSubStr( tmp_buffer, tmp_buf_len, "\r\n", " " ) ;
            #else
            rc = replaceSubStr( tmp_buffer, tmp_buf_len, "\n", " " ) ;
            #endif
            rc = replaceSubStr( tmp_buffer, tmp_buf_len, "\f", " " ) ;
            synopsis = tmp_buffer ;

            pSplit2 = ossStrrchr( pFileName, SPLIT_POINT2 ) ;
            if ( NULL == pSplit2 )
            {
               categoryName = pFileName ;
            }
            else
            {
               categoryName = string( pFileName,
                                 ossStrlen(pFileName) - ossStrlen(pSplit2) ) ;
            }
            if ( string(DB_CATEGORY) == categoryName )
            {
               _db._first.insert( pair<string, string>(funcName, pFileName) ) ;
               _db._second.insert( pair<string, string>(synopsis, cutline) ) ;
            }
            else if ( string(CS_CATEGORY) == categoryName )
            {
               _cs._first.insert( std::pair<string, string>(funcName, pFileName) ) ;
               _cs._second.insert( std::pair<string, string>(synopsis, cutline) ) ;
            }
            else if ( string(CL_CATEGORY) == categoryName )
            {
               _cl._first.insert( std::pair<string, string>(funcName, pFileName) ) ;
               _cl._second.insert( std::pair<string, string>(synopsis, cutline) ) ;
            }
            else if ( string(RG_CATEGORY) == categoryName )
            {
               _rg._first.insert( std::pair<string, string>(funcName, pFileName) ) ;
               _rg._second.insert( std::pair<string, string>(synopsis, cutline) ) ;
            }
            else if ( string(NODE_CATEGORY) == categoryName )
            {
               _node._first.insert( std::pair<string, string>(funcName, pFileName) ) ;
               _node._second.insert( std::pair<string, string>(synopsis, cutline) ) ;
            }
            else if ( string(CURSOR_CATEGORY) == categoryName )             
            {
               _cursor._first.insert( pair<string, string>(funcName, pFileName) ) ;
               _cursor._second.insert( pair<string, string>(synopsis, cutline) ) ;
            }
            else if ( string(CLCOUNT_CATEGORY) == categoryName )
            {
               _clcount._first.insert( pair<string, string>(funcName, pFileName) ) ;
               _clcount._second.insert( pair<string, string>(synopsis, cutline) ) ;
            }
            else if ( string(DOMAIN_CATEGORY) == categoryName )
            {
               _domain._first.insert( pair<string, string>(funcName, pFileName) ) ;
               _domain._second.insert( pair<string, string>(synopsis, cutline) ) ;
            }
            else if ( string(OMA_CATEGORY) == categoryName )
            {
               _oma._first.insert( pair<string, string>(funcName, pFileName) ) ;
               _oma._second.insert( pair<string,string>(synopsis, cutline) ) ;
            }
            else if ( string(QUERY_CATEGORY) == categoryName )
            {
               _query._first.insert( std::pair<string, string>(funcName,
                                                               pFileName) ) ;
               if ( ossMemcmp( pFileName, QUERY_GEN_CATEGORY,
                               ossStrlen( pFileName ) ) == 0 )
               {
                  _query_gen._second.insert( pair<string,string>(synopsis,
                                             cutline) ) ;
               }
               else if ( ossMemcmp( pFileName, QUERY_COND_CATEGORY,
                                    ossStrlen( pFileName ) ) == 0 )
               {
                  _query_cond._second.insert( pair<string,string>(synopsis,
                                              cutline) ) ;
               }
               else if ( ossMemcmp( pFileName, QUERY_CURS_CATEGORY,
                                    ossStrlen( pFileName ) ) == 0 )
               {
                  _query_curs._second.insert( pair<string,string>(synopsis,
                                              cutline) ) ;
               }
               else
               {
                  ossPrintf( "Failed to deal with file %s, for the wrong file "
                             "name %s"OSS_NEWLINE, pfPath, pFileName ) ;
                  rc = SDB_INVALIDARG ;
                  goto exit ;
               }
            }
            else
            {
               ossPrintf( "Failed to deal with file %s, for the wrong file "
                          "name %s"OSS_NEWLINE, pfPath, categoryName.c_str() ) ;
               rc = SDB_INVALIDARG ;
               goto exit ;
            }
          exit:
            fin.close();
            if ( rc )
            {
               goto error ;
            }
         }
         else
         {
            ossPrintf( "Failed to deal with file %s, for the wrong file "
                       "name %s"OSS_NEWLINE, pfPath, pFileName ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
   }
done :
   if ( NULL != file_buffer )
   {
      SDB_OSS_FREE( file_buffer ) ;
      file_buffer = NULL ;
   }
   if ( NULL != tmp_buffer )
   {
      SDB_OSS_FREE( tmp_buffer ) ;
      tmp_buffer = NULL ;
   }
   if ( NULL != pFileName )
   {
      SDB_OSS_FREE( pFileName ) ;
      pFileName = NULL ;
   }
   return rc;
error :
   goto done;
}

INT32 manHelp::getFileHelp( const CHAR* name )
{
   INT32 rc = SDB_OK;
   sset fuzzy_match ;
   string nameLower = name ;
   const CHAR *str = NULL;
   const CHAR *fname = NULL;
   const CHAR *nameL = NULL ;
   CHAR fPath[ OSS_MAX_PATHSIZE + 1 ] = { 0 };
   boost::to_lower( nameLower ) ;
   nameL = nameLower.c_str() ;
   if ( troffFileNotEixt )
   {
      ossPrintf( "Failed to scan troff file"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( name == NULL || ossStrcmp(name, "") == 0 )
   {
      ossPrintf( "Invalid arguments, %s:%d "OSS_NEWLINE, __FILE__,
                 __LINE__ ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   for ( ssmap::const_iterator it(_nmap.begin()), it_end(_nmap.end());
         it != it_end; it++ )
   {
      fname = (it->first).c_str() ;
      str = ossStrstr( fname, nameL );
      if ( str != NULL )
      {
         if ( ossStrncmp( fname, nameL, ossStrlen( nameL ) + 1 ) == 0 )
         {
            fuzzy_match.clear() ;
            fuzzy_match.insert( (it->second) ) ;
            break ;
         }
         fuzzy_match.insert( (it->second) );
      }
   }
   if ( fuzzy_match.size() == 0 )
   {
      ossPrintf( "No manual for method %s"OSS_NEWLINE, name );
   }
   else if ( fuzzy_match.size() > 1 )
   {
      ossPrintf( "%d methods related to \"%s\", please fill in the full "
                 "name: "OSS_NEWLINE, (INT32)fuzzy_match.size(), name );
      for ( sset::const_iterator it(fuzzy_match.begin()),
            it_end(fuzzy_match.end());
            it != it_end; it++ )
      {
         ossPrintf( "    %s"OSS_NEWLINE, (*it).c_str() );
      }
   }
   else // if we get one, parse it
   {
      sset::const_iterator it(fuzzy_match.begin());
      ossMemcpy( fPath, _filePath, ossStrlen(_filePath) );
      ossStrncat( fPath, (*it).c_str(), ossStrlen((*it).c_str()) );
      ossStrncat( fPath, MFILE_SUFFIX, ossStrlen(MFILE_SUFFIX) );
      rc = parseMandoc::getInstance().parse ( fPath ) ;
      if ( rc != SDB_OK )
      {
         ossPrintf( "Failed to parse troff file, %s:%d "OSS_NEWLINE,
                     __FILE__, __LINE__ ) ;
         goto error ;
      }
   }

done :
   return rc;
error :
   goto done;
}

INT32 manHelp::getFileHelp( const CHAR* category, const CHAR* cmd )
{
   INT32 rc = SDB_OK;
   if ( category == NULL )
   {
      ossPrintf( "Invalid arguments, %s:%d "OSS_NEWLINE, __FILE__, __LINE__ ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( NULL == cmd )
   {
      rc = displayMethod( category ) ;
      if ( rc )
      {
         ossPrintf( "Failed to display methods, %s:%d "OSS_NEWLINE,
                     __FILE__, __LINE__ ) ;
         goto error ;
      }
   }
   else
   {
      rc = displayManual( category, cmd ) ;
      if ( rc )
      {
         ossPrintf( "Failed to display manual, %s:%d "OSS_NEWLINE,
                     __FILE__, __LINE__ ) ;
         goto error ;
      }
   }

done :
   return rc;
error :
   goto done;
}

typedef std::map<string, string> ssmap ;


ssmap& manHelp::getCategoryMap( const CHAR *category )
{
    if ( ossMemcmp( category, DB_CATEGORY,
                    ossStrlen( category ) ) == 0 )
    {
       return _db._second ;
    }
    else if ( ossMemcmp( category, CS_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _cs._second ;
    }
    else if ( ossMemcmp( category, CL_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _cl._second ;
    }
    else if ( ossMemcmp( category, RG_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _rg._second ;
    }
    else if ( ossMemcmp( category, NODE_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _node._second ;
    }
    else if ( ossMemcmp( category, CURSOR_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _cursor._second ;
    }
    else if ( ossMemcmp( category, CLCOUNT_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _clcount._second ;
    }
    else if ( ossMemcmp( category, DOMAIN_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _domain._second ;
    }
    else if ( ossMemcmp( category, OMA_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _oma._second ;
    }
    else if ( ossMemcmp( category, QUERY_GEN_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _query_gen._second ;
    }
    else if ( ossMemcmp( category, QUERY_COND_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _query_cond._second ;
    }
    else if ( ossMemcmp( category, QUERY_CURS_CATEGORY,
                         ossStrlen( category ) ) == 0 )
    {
       return _query_curs._second ;
    }
    else
    {
       return _empty._second ;
    }
}

INT32 manHelp::displayMethod( const CHAR *category )
{
   INT32 rc = SDB_OK ;
   ssmap &cate = getCategoryMap( category ) ;
   if ( 0 == cate.size() )
   {
      goto done ;
   }
   else
   {
      const CHAR *p_first = NULL ;
      const CHAR *p_second = NULL ;
      ssmap::iterator it = cate.begin() ;
      for ( ; it != cate.end(); it++ )
      {
         p_first = (it->first).c_str() ;
         p_second = (it->second).c_str() ;
         rc =  display( p_first, p_second, INDENT_WIDTH1, INDENT_WIDTH2 ) ;
         if ( rc )
         {
            goto error ;
         }
      }
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 manHelp::displayManual( const CHAR *category, const CHAR *cmd )
{
   INT32 rc = SDB_OK ;
   sset fuzzy_match ;
   smmap::iterator it ;
   string str ;
   if ( !category || !cmd )
   {
      ossPrintf( "Invalid arguments, %s:%d "OSS_NEWLINE, __FILE__, __LINE__ ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   it = _classify.find( string(category) ) ;
   if ( it != _classify.end() )
   {
      ssmap::iterator itr = it->second.begin() ;
      for( ; itr != it->second.end(); itr++ )
      {
         const CHAR *pFunc = itr->first.c_str() ;
         const CHAR *pPos = ossStrstr( pFunc, cmd );
         if ( pPos != NULL )
         {
            string filename = itr->second + "." + itr->first ;
            if ( ossStrncmp( pFunc, cmd, ossStrlen( cmd ) + 1 ) == 0 )
            {
               fuzzy_match.clear() ;
               fuzzy_match.insert( filename ) ;
               break ;
            }
            fuzzy_match.insert( filename );
         }
      }
   }
   if ( 0 == fuzzy_match.size() )
   {
      if ( NULL == category || '\0' == category[0] )
         fuzzy_match.insert( string(cmd) ) ;
      else
         fuzzy_match.insert( string(category) + "." + string(cmd) ) ;
   }
   else if ( fuzzy_match.size() > 1 )
   {
      ossPrintf( "%d methods related to \"%s\" in \"%s\" category, please "
                 "fill in the full name: "OSS_NEWLINE,
                 (INT32)fuzzy_match.size(), cmd, category );
      for ( sset::const_iterator i = fuzzy_match.begin();
            i != fuzzy_match.end(); i++ )
      {
         ossPrintf( "    %s"OSS_NEWLINE, (*i).c_str() );
      }
      goto done ;
   }
   str = *(fuzzy_match.begin()) ;
   rc = getFileHelp ( str.c_str() ) ;
   if ( rc )
   {
      goto error ;
   }

done :
   return rc ;
error :
   goto done ;
}

INT32 removeMark( CHAR *buffer, INT32 buffer_len, const CHAR *mark )
{
   INT32 rc = SDB_OK ;
   INT32 strLen = 0 ;
   INT32 moveLen = 0 ;
   CHAR *pb = NULL ;
   CHAR *pe = NULL ;
   CHAR *pp = NULL ;
   if ( !buffer || !mark || buffer_len <= 0 )
   {
      ossPrintf( "Invalid arguments, %s:%d "OSS_NEWLINE, __FILE__, __LINE__ ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   buffer[buffer_len] = '\0' ;
   strLen = ossStrlen(mark) ;
   pp = pb = ossStrstr( buffer, mark ) ;
   if ( !pb )
    goto done ;
   while ( true )
   {
      pe = ossStrstr( pb + strLen, mark ) ;
      if ( !pe )
      {
         moveLen = ossStrlen( pb + strLen ) ;
         ossMemmove( pp, pb + strLen, moveLen + 1 ) ;
         break ;
      }
      else
      {
         moveLen = pe - (pb + strLen) ;
         ossMemmove( pp, pb + strLen, moveLen ) ;
         pp += moveLen ;
         pb = pe ;
      }
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 replaceSubStr( CHAR *buffer, INT32 buffer_len,
                     const CHAR *pStr1, const CHAR *pStr2 )
{
   INT32 rc = SDB_OK ;
   INT32 str1_len = ossStrlen( pStr1 ) ;
   INT32 str2_len = ossStrlen( pStr2 ) ;
   INT32 offset = 0 ;
   INT32 less_part_len = 0 ;
   CHAR buf[ READ_CHARACTOR_NUM + 1 ] = { 0 } ;
   CHAR *pos = NULL ;

   if ( !buffer || !pStr1 || !pStr2 ||
        buffer_len > READ_CHARACTOR_NUM + 1 ||
        str1_len  > READ_CHARACTOR_NUM ||
        str2_len > READ_CHARACTOR_NUM )
   {
      ossPrintf( "Invalid arguments, %s:%d "OSS_NEWLINE, __FILE__, __LINE__ ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   pos = ossStrstr( buffer, pStr1 ) ;
   while ( pos != NULL )
   {
      ossMemset( buf, 0, sizeof( buf ) ) ;
      offset = pos - buffer ;
      ossStrncpy( buf, buffer, offset ) ;
      if ( offset + str2_len > buffer_len - 1 )
      {
         ossPrintf( "Failed to replace sub str1 with str2 in buffer, "
                    "%s:%d "OSS_NEWLINE, __FILE__, __LINE__ ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossStrncat( buf, pStr2, str2_len ) ;
      less_part_len = ossStrlen( pos+str1_len ) ;
      if ( offset + str2_len + less_part_len > buffer_len - 1 )
      {
         ossPrintf( "Failed to replace sub str1 with str2 in buffer, "
                    "%s:%d "OSS_NEWLINE, __FILE__, __LINE__ ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      ossStrncat( buf, pos + str1_len,  less_part_len );
      ossStrncpy( buffer, buf, buffer_len -1 ) ;
      buffer[ buffer_len - 1 ] = 0 ;

      pos = ossStrstr( buffer, pStr1 ) ;
   }
done :
   return rc ;
error :
   goto done ;
}
INT32 checkBuffer ( CHAR **ppBuffer, INT32 *bufferSize,
                           INT32 length )
{
   INT32 rc = SDB_OK ;
   if ( length > *bufferSize )
   {
      CHAR *pOld = *ppBuffer ;
      INT32 newSize = length + 1 ;
      *ppBuffer = (CHAR*)SDB_OSS_REALLOC ( *ppBuffer, sizeof(CHAR)*(newSize)) ;
      if ( !*ppBuffer )
      {
         ossPrintf ( "Failed to allocate %d bytes send buffer"OSS_NEWLINE,
                     newSize ) ;
         rc = SDB_OOM ;
         *ppBuffer = pOld ;
         goto error ;
      }
      *bufferSize = newSize ;
   }
done :
   return rc ;
error :
   goto done ;
}

BOOLEAN isCategory( const CHAR *pName, const CHAR** arr, INT32 size )
{
   INT32 i = 0 ;
   BOOLEAN result = FALSE ;
   for ( ; i < size ; i++ )
   {
      if ( ossMemcmp( pName, arr[i], ossStrlen( pName ) ) == 0 )
      {
         result = TRUE ;
         break ;
      }
   }
   return result ;
}

INT32 display( const CHAR *first, const CHAR *second, INT32 indent1,
               INT32 indent2 )
{
   INT32 rc = SDB_OK ;
   vector<string> vec ;
   INT32 first_part_len = 0 ;
   INT32 idt1 = indent1 ;
   INT32 idt2 = 0 ;

   if ( !first || !second || indent1 < 0 || indent2 < 0 )
   {
      ossPrintf( "Invalid argument, %s:%d"OSS_NEWLINE,
                  __FILE__, __LINE__ ) ;

      rc = SDB_INVALIDARG ;
      goto error ;
   }

   first_part_len = indent1 + ossStrlen( first ) + 1 ;
   if ( first_part_len < indent2 )
   {
      idt2 = indent2 - first_part_len + 1 ;
      rc = splitCutline( second, vec ) ;
      if ( rc )
         goto error ;
      vector<string>::iterator it ;
      it = vec.begin() ;
      #if defined ( _WINDOWS )
      cout << setw(idt1) << " " << first ;
      #else
      cout << setw(idt1) << " " << first << setw(idt2) << " " << *it << endl ;
      idt2 = indent2 ;
      for( it++; it != vec.end(); it++ )
      {
         cout << setw(idt2) << " " << *it << endl ;
      }
      #endif
   }
   else
   {
      #if defined ( _WINDOWS )
      cout << setw(idt1) << " " << first ;
      #else
      cout << setw(idt1) << " " << first << endl ;
      idt2 = indent2 ;
      rc = splitCutline( second, vec ) ;
      if ( rc )
         goto error ;
      vector<string>::iterator it ;
      it = vec.begin() ;
      for ( ; it != vec.end(); it++ )
      {
         cout << setw(idt2) << " " << *it << endl ;
      }
      #endif
   }

done :
   return rc ;
error :
   goto done ;
}

INT32 getSplitPos( const CHAR *pCur, INT32 part_len, INT32 *ret )
{
   INT32 rc = SDB_OK ;
   const CHAR *p = NULL ;
   INT32 less_part_len = 0 ;
   if ( !pCur || part_len <= 0 )
   {
      ossPrintf( "Invalid argument, %s:%d"OSS_NEWLINE,
                  __FILE__, __LINE__ ) ;

      rc = SDB_INVALIDARG ;
      goto error ;
   }

   less_part_len = ossStrlen( pCur ) + 1 ;
   if ( less_part_len <= part_len )
   {
      *ret = 0 ;
      goto done ;
   }
   p = pCur + part_len ;
   while( ( *p != ' ' ) && ( p > pCur ) )
   {
      --p ;
   }
   if ( p == pCur )
      *ret = part_len ;
   else
      *ret = p - pCur ;
done :
   return rc ;
error :
   goto done ;
}

INT32 splitCutline( const CHAR *cutline, vector<string> &vec )
{
   INT32 rc = SDB_OK ;
   const CHAR *pb = NULL ;
   INT32 move_len = -1 ;
   INT32 part_len = CONTENT_LEN ;

   if ( !cutline )
   {
      ossPrintf( "Invalid argument, %s:%d"OSS_NEWLINE,
                  __FILE__, __LINE__ ) ;

      rc = SDB_INVALIDARG ;
      goto error ;
   }
   pb = cutline ;
   while ( TRUE )
   {
      rc = getSplitPos( pb, part_len, &move_len ) ;
      if ( rc )
         goto error ;
      if ( move_len == 0 )
      {
         string str = string( pb, ossStrlen(pb) ) ;
         vec.push_back( str ) ;
         break ;
      }
      vec.push_back( string ( pb, move_len ) ) ;
      pb = pb + move_len ;
      move_len = -1 ;
   }

done :
   return rc ;
error :
   goto done ;
}

