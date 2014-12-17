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

   Source File Name = rtnQueryOptions.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   user command processing on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          27/05/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_QUERYOPTIONS_HPP_
#define RTN_QUERYOPTIONS_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "../bson/bson.hpp"

namespace engine
{
   class _rtnQueryOptions : public SDBObject
   {
   public:
      _rtnQueryOptions()
      :_fullName( NULL ),
       _fullNameBuf( NULL ),
       _skip( 0 ),
       _limit( -1 ),
       _flag( 0 ),
       _enablePrefetch( FALSE )
      {

      }

      _rtnQueryOptions( const CHAR *query,
                        const CHAR *selector,
                        const CHAR *orderBy,
                        const CHAR *hint,
                        const CHAR *fullName,
                        SINT64 skip,
                        SINT64 limit,
                        INT32 flag,
                        BOOLEAN enablePrefetch )
      :_query( query ),
       _selector( selector ),
       _orderBy( orderBy ),
       _hint( hint ),
       _fullName( fullName ),
       _fullNameBuf( NULL ),
       _skip( skip ),
       _limit( limit ),
       _flag( flag ),
       _enablePrefetch( enablePrefetch )
      {

      }

      _rtnQueryOptions( const bson::BSONObj &query,
                        const bson::BSONObj &selector,
                        const bson::BSONObj &orderBy,
                        const bson::BSONObj &hint,
                        const CHAR *fullName,
                        SINT64 skip,
                        SINT64 limit,
                        INT32 flag,
                        BOOLEAN enablePrefetch )
      :_query( query ),
       _selector( selector ),
       _orderBy( orderBy ),
       _hint( hint ),
       _fullName( fullName ),
       _fullNameBuf( NULL ),
       _skip( skip ),
       _limit( limit ),
       _flag( flag ),
       _enablePrefetch( enablePrefetch )
      {

      }

      virtual ~_rtnQueryOptions() ;

      INT32 getOwned() ;

   public:
      bson::BSONObj _query ;
      bson::BSONObj _selector ;
      bson::BSONObj _orderBy ;
      bson::BSONObj _hint ;
      const CHAR *_fullName ;
      CHAR *_fullNameBuf ;
      SINT64 _skip ;
      SINT64 _limit ;
      INT32 _flag ;
      BOOLEAN _enablePrefetch ;
   } ;
   typedef class _rtnQueryOptions rtnQueryOptions ;
}

#endif


