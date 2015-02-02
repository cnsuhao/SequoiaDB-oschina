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

   Source File Name = sptUsrSdbTool.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          18/08/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_USR_SDBTOOL_HPP__
#define SPT_USR_SDBTOOL_HPP__

#include "sptApi.hpp"
#include <string>
#include <vector>

using namespace std ;

namespace engine
{

   struct _sdbToolListParam
   {
      vector< string >     _svcnames ;
      INT32                _typeFilter ;
      INT32                _modeFilter ;
      INT32                _roleFilter ;
   } ;

   /*
      _sptUsrSdbTool define
   */
   class _sptUsrSdbTool : public SDBObject
   {
   JS_DECLARE_CLASS( _sptUsrSdbTool )

   public:
      _sptUsrSdbTool() ;
      virtual ~_sptUsrSdbTool() ;

   public:

      static INT32 help( const _sptArguments &arg,
                         _sptReturnVal &rval,
                         bson::BSONObj &detail ) ;
      /*
         static functions
      */

      static INT32 listNodes( const _sptArguments &arg,
                              _sptReturnVal &rval,
                              bson::BSONObj &detail ) ;

      static INT32 getNodeConfig( const _sptArguments &arg,
                                  _sptReturnVal &rval,
                                  bson::BSONObj &detail ) ;

   protected:

   private:

   } ;

}

#endif // SPT_USR_SDBTOOL_HPP__

