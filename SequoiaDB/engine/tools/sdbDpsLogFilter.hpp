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

   Source File Name = sdbDpsLogFilter.hpp

   Descriptive Name = N/A

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains code logic for
   data insert/update/delete. This file does NOT include index logic.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/19/2014  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef _SDB_DPS_DUMP_LOG_FILTER_HPP_
#define _SDB_DPS_DUMP_LOG_FILTER_HPP_
   
#include "ossTypes.hpp"
#include "ossMem.hpp"
#include "ossIO.hpp"
#include "ossUtil.hpp"
#include "sdbDpsFilter.hpp"



class _dpsLogFilter : public SDBObject
{
public:
   _dpsLogFilter( const dpsCmdData* data ) ;

   virtual ~_dpsLogFilter() ;

   virtual INT32 doParse() ;

   void setFilter( iFilter *filter ) ;

   const CHAR* getSrcPath() const ;
   const CHAR* getDstPath() const ;

   static const INT32 getFileCount( const CHAR *path ) ;

   static BOOLEAN isFileExisted( const CHAR *path ) ;

   static BOOLEAN isDir( const CHAR *path ) ;

private:
   iFilter *_filter ;
   const dpsCmdData *_cmdData ;
} ;
typedef _dpsLogFilter dpsLogFilter ;
   
#endif
