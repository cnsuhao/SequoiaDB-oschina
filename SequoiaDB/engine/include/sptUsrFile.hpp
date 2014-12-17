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

   Source File Name = sptUsrFile.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_USRFILE_HPP_
#define SPT_USRFILE_HPP_

#include "sptApi.hpp"
#include "ossIO.hpp"

namespace engine
{
   class _sptUsrFile : public SDBObject
   {
   JS_DECLARE_CLASS( _sptUsrFile )

   public:
      _sptUsrFile() ;
      virtual ~_sptUsrFile() ;

   public:
      INT32 construct( const _sptArguments &arg,
                       _sptReturnVal &rval,
                       bson::BSONObj &detail) ;

      INT32 read( const _sptArguments &arg,
                  _sptReturnVal &rval,
                  bson::BSONObj &detail ) ;

      INT32 write( const _sptArguments &arg,
                   _sptReturnVal &rval,
                   bson::BSONObj &detail ) ;

      INT32 seek( const _sptArguments &arg,
                  _sptReturnVal &rval,
                  bson::BSONObj &detail ) ;

      INT32 close( const _sptArguments &arg,
                   _sptReturnVal &rval,
                   bson::BSONObj &detail ) ;

      static INT32 remove( const _sptArguments &arg,
                           _sptReturnVal &rval,
                           bson::BSONObj &detail ) ;

      static INT32 exist( const _sptArguments &arg,
                          _sptReturnVal &rval,
                          bson::BSONObj &detail ) ;

      static INT32 copy( const _sptArguments &arg,
                          _sptReturnVal &rval,
                          bson::BSONObj &detail ) ;

      static INT32 move( const _sptArguments &arg,
                          _sptReturnVal &rval,
                          bson::BSONObj &detail ) ;

      static INT32 mkdir( const _sptArguments &arg,
                          _sptReturnVal &rval,
                          bson::BSONObj &detail ) ;

      INT32 toString( const _sptArguments &arg,
                      _sptReturnVal &rval,
                      bson::BSONObj &detail ) ;

      static INT32 help( const _sptArguments &arg,
                         _sptReturnVal &rval,
                         bson::BSONObj &detail ) ;

      INT32 destruct() ;

   private:
      OSSFILE _file ;
      string  _filename ;
   } ;
}

#endif
