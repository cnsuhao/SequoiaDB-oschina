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

   Source File Name = sptParseTroff.hpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/28/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef SPTPARSETROFF_HPP__
#define SPTPARSETROFF_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "ossTypes.hpp"
#include "ossUtil.hpp"

#include <set>
#include <string>
#include <map>

using namespace std;

typedef std::set<string> sset ;
typedef std::map<string, string> ssmap ;
typedef std::map<string, string>& ssmap_ref ;
typedef std::map< string, ssmap_ref > smmap ;

struct Classified_info
{
   ssmap _first ; // save "funcName, fileName", use them in display manumal
   ssmap _second ; // save "synopsis, cutline", use them in diaplay functions
} ;

class manHelp : public SDBObject
{
public:
   static manHelp& getInstance( const CHAR *path ) ;
   INT32 getFileHelp( const CHAR *cmd ) ;
   INT32 getFileHelp( const CHAR *category, const CHAR *cmd ) ;
   
private:
   manHelp( const CHAR* path ) ;
   manHelp( const manHelp& ) ;
   manHelp& operator=( const manHelp& ) ;
   ~manHelp() ;
   INT32 scanFile() ;
   ssmap& getCategoryMap( const CHAR *category ) ;
   INT32 displayMethod( const CHAR *category ) ;
   INT32 displayManual( const CHAR *category, const CHAR *cmd ) ;

private:
   ssmap _nmap ;
   CHAR _filePath[ OSS_MAX_PATHSIZE + 1 ] ;
   Classified_info _empty ;
   Classified_info _db ;
   Classified_info _cs ;
   Classified_info _cl ;
   Classified_info _rg ;
   Classified_info _node ;
   Classified_info _cursor ;
   Classified_info _clcount ;
   Classified_info _domain ;
   Classified_info _oma ;
   Classified_info _query ;
   Classified_info _query_gen ; // general functions in sdbQuery
   Classified_info _query_cond ; // functions about condition in sdbQuery
   Classified_info _query_curs ; // functions about cursor in sdbQuery
   smmap _classify ; 
   BOOLEAN troffFileNotEixt ;
} ;

#endif // SPTPARSETROFF_HPP__
