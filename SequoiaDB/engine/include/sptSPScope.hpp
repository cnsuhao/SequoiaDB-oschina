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

   Source File Name = sptSPScope.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_SPSCOPE_HPP_
#define SPT_SPSCOPE_HPP_

#include "sptScope.hpp"
#include "jsapi.h"

namespace engine
{
   class _sptSPScope : public _sptScope
   {
   public:
      _sptSPScope() ;
      virtual ~_sptSPScope() ;

      virtual SPT_SCOPE_TYPE getType() const { return SPT_SCOPE_TYPE_SP ; }

      template<typename T>
      BOOLEAN isInstanceOf( JSContext *cx, JSObject *obj )
      {
         return T::__desc.isInstanceOf( cx, obj ) ;
      }

   public:
      virtual INT32 start() ;

      virtual void shutdown() ;

      JSContext *getContext()
      {
         return _context ;
      }

      JSObject *getGlobalObj()
      {
         return _global ;
      }

   public:
      virtual INT32 eval(const CHAR *code, UINT32 len,
                         const CHAR *filename,
                         UINT32 lineno,
                         INT32 flag,
                         bson::BSONObj &rval,
                         bson::BSONObj &detail ) ;

   private:
      virtual INT32 _loadUsrDefObj( _sptObjDesc *desc ) ;

      INT32 _loadUsrClass( _sptObjDesc *desc ) ;

      INT32 _loadGlobal( _sptObjDesc *desc ) ;

      INT32 _rval2obj( JSContext *cx,
                       const jsval &jsrval,
                       bson::BSONObj &rval ) ;

   private:
      JSRuntime *_runtime ;
      JSContext *_context ;
      JSObject *_global ;
      JSErrorReporter _errReporter ;
   } ;
   typedef class _sptSPScope sptSPScope ;
}

#endif

