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

   Source File Name = sptUsrCmd.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_USRCMD_HPP_
#define SPT_USRCMD_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "sptApi.hpp"

#include <string>

using namespace std ;

namespace engine
{
   class _ossCmdRunner ;

   class _sptUsrCmd : public SDBObject
   {
   JS_DECLARE_CLASS( _sptUsrCmd )

   public:
      _sptUsrCmd() ;
      virtual ~_sptUsrCmd() ;

   public:

      INT32 construct( const _sptArguments &arg,
                       _sptReturnVal &rval,
                       bson::BSONObj &detail ) ;

      INT32 destruct() ;

      INT32 toString( const _sptArguments &arg,
                      _sptReturnVal &rval,
                      bson::BSONObj &detail ) ;

      INT32 getLastRet( const _sptArguments &arg,
                        _sptReturnVal &rval,
                        bson::BSONObj &detail ) ;

      INT32 start( const _sptArguments &arg,
                   _sptReturnVal &rval,
                   bson::BSONObj &detail ) ;

      INT32 getLastOut( const _sptArguments &arg,
                        _sptReturnVal &rval,
                        bson::BSONObj &detail ) ;

      INT32 exec( const _sptArguments &arg,
                  _sptReturnVal &rval,
                  bson::BSONObj &detail ) ;

      static INT32 help( const _sptArguments &arg,
                         _sptReturnVal &rval,
                         bson::BSONObj &detail ) ;

   private:
      INT32 _setRVal( _ossCmdRunner *runner,
                      _sptReturnVal &rval,
                      BOOLEAN setToRVal,
                      bson::BSONObj &detail ) ;

   private:
      UINT32         _retCode ;
      string         _strOut ;

   } ;
   typedef class _sptUsrCmd sptUsrCmd ;
}

#endif

