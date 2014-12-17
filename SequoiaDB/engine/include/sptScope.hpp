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

   Source File Name = sptScope.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_SCOPE_HPP_
#define SPT_SCOPE_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "sptSPDef.hpp"
#include "../bson/bson.hpp"

namespace engine
{
   class _sptObjDesc ;

   enum SPT_SCOPE_TYPE
   {
      SPT_SCOPE_TYPE_SP = 0,
      SPT_SCOPE_TYPE_V8 = 1,
   } ;

   class _sptScope : public SDBObject
   {
   public:
      _sptScope() ;
      virtual ~_sptScope() ;

      virtual SPT_SCOPE_TYPE getType() const = 0 ;

      INT32  getLastError() ;
      const CHAR* getLastErrMsg() ;

   public:
      virtual INT32 start() = 0 ;

      virtual void shutdown() = 0 ;

      virtual INT32 eval( const CHAR *code, UINT32 len,
                          const CHAR *filename,
                          UINT32 lineno,
                          INT32 flag, // SPT_EVAL_FLAG_NONE/SPT_EVAL_FLAG_PRINT
                          bson::BSONObj &rval,
                          bson::BSONObj &detail ) = 0 ;
   public:
      template<typename T>
      INT32 loadUsrDefObj()
      {
         return loadUsrDefObj( &(T::__desc) ) ;
      }

      INT32 loadUsrDefObj( _sptObjDesc *desc ) ;

   private:
      virtual INT32 _loadUsrDefObj( _sptObjDesc *desc ) = 0 ;

   private:
      typedef std::map<std::string, const _sptObjDesc *> OBJ_DESCS ;

   protected:
      BOOLEAN _addDesc( const CHAR *name, const _sptObjDesc *desc )
      {
         return _descs.insert( std::make_pair( name, desc ) ).second ;
      }

      const _sptObjDesc *getDesc( const CHAR *name )
      {
         OBJ_DESCS::const_iterator itr = _descs.find( name ) ;
         return _descs.end() == itr ?
                NULL : itr->second ;
      }

   private:
      static OBJ_DESCS _descs ;
   } ;
   typedef class _sptScope sptScope ;

}

#endif

