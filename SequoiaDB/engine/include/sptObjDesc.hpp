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

   Source File Name = sptObjDesc.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_OBJDESC_HPP_
#define SPT_OBJDESC_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "sptFuncMap.hpp"
#include "jsapi.h"

namespace engine
{
   class _sptObjDesc : public SDBObject
   {
   public:
      _sptObjDesc()
      :_init(FALSE)
      {}

      virtual ~_sptObjDesc(){}
   public:
      const CHAR *getJSClassName() const
      {
         return _jsClassName.c_str() ;
      }

      const _sptFuncMap &getFuncMap()const
      {
         return _funcMap ;
      }

      const JSClass *getClassDef() const
      {
         return _init ? &_classDef : NULL ;
      }

      void setClassName( const CHAR *name )
      {
         _jsClassName.assign( name ) ;
      }

      void setClassDef( const JSClass &def )
      {
         _classDef = def ;
         _init = TRUE ;
      }

      BOOLEAN getIgnore() const
      {
         return _jsClassName.empty() ;
      }

      BOOLEAN isInstanceOf( JSContext *cx, JSObject *obj )
      {
         if ( !_init )
         {
            return FALSE ;
         }
         return JS_InstanceOf( cx, obj, &_classDef, NULL ) ;
      }

   protected:
      std::string _jsClassName ;
      _sptFuncMap _funcMap ;
      JSClass _classDef ;
      BOOLEAN _init ;
   } ;
   typedef class _sptObjDesc sptObjDesc ;
}

#endif

