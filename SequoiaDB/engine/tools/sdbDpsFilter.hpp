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

   Source File Name = sdbDpsFilter.hpp

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
#ifndef _SDB_DPS_DUMP_FILTER_HPP_
#define _SDB_DPS_DUMP_FILTER_HPP_

#include "ossUtil.hpp"
#include "ossIO.hpp"
#include <vector>
#include <list>

struct _dpsCmdData
{
   UINT16  type ;
   INT32   lsnAhead ;
   INT32   lsnBack ;
   INT32   lastCount ;
   BOOLEAN output ;
   UINT64  lsn ;
   CHAR    inputName[ OSS_MAX_PATHSIZE + 1 ] ;
   CHAR    srcPath  [ OSS_MAX_PATHSIZE + 1 ] ;
   CHAR    dstPath  [ OSS_MAX_PATHSIZE + 1 ] ;
} ;
typedef _dpsCmdData dpsCmdData ;

struct _dpsFileMeta
{
   UINT32 index ;
   UINT32 logID ;
   INT64  validSize ;
   INT64  restSize ;
   UINT64 firstLSN ;
   UINT64 lastLSN ;
   UINT64 expectLSN ;
} ;
typedef _dpsFileMeta dpsFileMeta ;

#define DPS_LOG_REACH_HEAD 1
#define DPS_LOG_INVALID_LSN 0xffffffffffffffff
#define LOG_BUFFER_FORMAT_MULTIPLIER 10

struct _dpsMetaData
{
   UINT32 fileBegin ;
   UINT32 fileWork ;
   UINT32 fileCount ;
   std::vector<dpsFileMeta> metaList ;
} ;
typedef _dpsMetaData dpsMetaData ;

enum SDB_DPS_LOG_FILTER_TYPE
{
   SDB_LOG_FILTER_INVALID = 0,
   SDB_LOG_FILTER_NONE,
   SDB_LOG_FILTER_TYPE,
   SDB_LOG_FILTER_NAME,
   SDB_LOG_FILTER_LSN,
   SDB_LOG_FILTER_META,
   SDB_LOG_FILTER_LAST,
} ;

class _iFilter
{
public:
   _iFilter( SDB_DPS_LOG_FILTER_TYPE type ) : _nextFilter( NULL ),
                                              _type( type )
   {}

   virtual ~_iFilter() 
   {
      _nextFilter = NULL ;
   }

   virtual INT32 doFilte( const dpsCmdData *data, OSSFILE& out,
                          const CHAR* logFilePath ) = 0 ;

   virtual BOOLEAN match( const dpsCmdData *data, CHAR *pRecord )
   {
      BOOLEAN rc = TRUE ;
      if( NULL != _nextFilter )
      {
         rc = _nextFilter->match( data, pRecord ) ;
         goto done;
      }
   done:
      return rc ;
   }

   void setNext( _iFilter *filter )
   {
      SDB_ASSERT( NULL == _nextFilter, "_nextFilter must be NULL" ) ;
      if( _nextFilter )
      {
         filter->setNext( _nextFilter ) ;
      }
      _nextFilter = filter ;
   }

   SDB_DPS_LOG_FILTER_TYPE getType() const
   {
      return _type ;
   }

protected:
   _iFilter *_nextFilter ;
   SDB_DPS_LOG_FILTER_TYPE _type ;

} ;
typedef _iFilter iFilter ;

#define DECLARE_FILTER( filterName, alias, typeIndex )                \
class filterName : public SDBObject, public iFilter                   \
{                                                                     \
public:                                                               \
   filterName() : iFilter( typeIndex ) {}                             \
   virtual  ~filterName() {}                                          \
                                                                      \
   virtual BOOLEAN match( const dpsCmdData *data, CHAR *pRecord ) ;   \
   virtual INT32 doFilte( const dpsCmdData *data, OSSFILE& out,       \
                          const CHAR *logFilePath ) ;                 \
                                                                      \
} ;                                                                   \
typedef filterName alias ;

#define FILTER_DEFINITION( TYPE, TYPE_INDEX ) \
DECLARE_FILTER( _dps##TYPE##Filter, dps##TYPE##Filter, TYPE_INDEX )

FILTER_DEFINITION( Type, SDB_LOG_FILTER_TYPE )
FILTER_DEFINITION( Name, SDB_LOG_FILTER_NAME )
FILTER_DEFINITION( Meta, SDB_LOG_FILTER_META )
FILTER_DEFINITION( Lsn,  SDB_LOG_FILTER_LSN  )
FILTER_DEFINITION( None, SDB_LOG_FILTER_NONE )
FILTER_DEFINITION( Last, SDB_LOG_FILTER_LAST )

class _dpsFilterFactory
{
public:
   static _dpsFilterFactory* getInstance() ;

   iFilter* createFilter( int type ) ;   

   void release( iFilter *filter ) ;

private:
   _dpsFilterFactory()  ;
   ~_dpsFilterFactory() ;
   
   std::list< iFilter *> _filterList ;
} ;
typedef _dpsFilterFactory dpsFilterFactory;

#endif
