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

   Source File Name = utilCommon.cpp

   Descriptive Name = Process MoDel Main

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for SequoiaDB,
   and all other process-initialization code.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "utilCommon.hpp"
#include "ossUtil.hpp"
#include "msg.h"
#include "ossLatch.hpp"
#include "pdTrace.hpp"
#include "utilTrace.hpp"

using namespace bson ;

namespace engine
{

   SDB_ROLE utilGetRoleEnum( const CHAR *role )
   {
      if ( NULL == role )
         return SDB_ROLE_MAX;
      else if ( *role == 0 || 0 == ossStrcmp( role, SDB_ROLE_STANDALONE_STR ) )
         return SDB_ROLE_STANDALONE ;
      else if ( 0 == ossStrcmp( role, SDB_ROLE_DATA_STR ) )
         return SDB_ROLE_DATA;
      else if ( 0 == ossStrcmp( role, SDB_ROLE_CATALOG_STR ) )
         return SDB_ROLE_CATALOG;
      else if ( 0 == ossStrcmp( role, SDB_ROLE_COORD_STR ) )
         return SDB_ROLE_COORD;
      else if ( 0 == ossStrcmp( role, SDB_ROLE_OM_STR ) )
         return SDB_ROLE_OM ;
      else if ( 0 == ossStrcmp( role, SDB_ROLE_OMA_STR ) )
         return SDB_ROLE_OMA ;
      else
         return SDB_ROLE_MAX;
   }

   const CHAR* utilDBRoleStr( SDB_ROLE dbrole )
   {
      switch ( dbrole )
      {
         case SDB_ROLE_DATA :
            return SDB_ROLE_DATA_STR ;
         case SDB_ROLE_COORD :
            return SDB_ROLE_COORD_STR ;
         case SDB_ROLE_CATALOG :
            return SDB_ROLE_CATALOG_STR ;
         case SDB_ROLE_STANDALONE :
            return SDB_ROLE_STANDALONE_STR ;
         case SDB_ROLE_OM :
            return SDB_ROLE_OM_STR ;
         case SDB_ROLE_OMA :
            return SDB_ROLE_OMA_STR ;
         default :
            break ;
      }
      return "" ;
   }

   const CHAR* utilDBRoleShortStr( SDB_ROLE dbrole )
   {
      switch ( dbrole )
      {
         case SDB_ROLE_DATA :
            return "D" ;
         case SDB_ROLE_COORD :
            return "S" ;
         case SDB_ROLE_CATALOG :
            return "C" ;
         default :
            break ;
      }
      return "" ;
   }

   SDB_ROLE utilShortStr2DBRole( const CHAR * role )
   {
      if ( NULL == role )
         return SDB_ROLE_MAX;
      if ( 0 == ossStrcmp( role, "D" ) )
         return SDB_ROLE_DATA;
      else if ( 0 == ossStrcmp( role, "C" ) )
         return SDB_ROLE_CATALOG;
      else if ( 0 == ossStrcmp( role, "S" ) )
         return SDB_ROLE_COORD;
      else
         return SDB_ROLE_MAX;
   }

   SDB_TYPE utilGetTypeEnum( const CHAR * type )
   {
      if ( NULL == type )
      {
         return SDB_TYPE_MAX ;
      }
      else if ( 0 == *type ||
                0 == ossStrcmp( type, SDB_TYPE_DB_STR ) )
      {
         return SDB_TYPE_DB ;
      }
      else if ( 0 == ossStrcmp( type, SDB_TYPE_OM_STR ) )
      {
         return SDB_TYPE_OM ;
      }
      else if ( 0 == ossStrcmp( type, SDB_TYPE_OMA_STR ) )
      {
         return SDB_TYPE_OMA ;
      }
      else
      {
         return SDB_TYPE_MAX ;
      }
   }

   const CHAR* utilDBTypeStr( SDB_TYPE type )
   {
      switch ( type )
      {
         case SDB_TYPE_DB :
            return SDB_TYPE_DB_STR ;
         case SDB_TYPE_OM :
            return SDB_TYPE_OM_STR ;
         case SDB_TYPE_OMA :
            return SDB_TYPE_OMA_STR ;
         default :
            break ;
      }
      return "Unknow" ;
   }

   SDB_TYPE utilRoleToType( SDB_ROLE role )
   {
      switch ( role )
      {
         case SDB_ROLE_DATA :
         case SDB_ROLE_COORD :
         case SDB_ROLE_CATALOG :
         case SDB_ROLE_STANDALONE :
            return SDB_TYPE_DB ;
         case SDB_ROLE_OM :
            return SDB_TYPE_OM ;
         case SDB_ROLE_OMA :
            return SDB_TYPE_OMA ;
         default :
            break ;
      }
      return SDB_TYPE_MAX ;
   }

   INT32 utilPrefReplStr2Enum( const CHAR * prefReplStr )
   {
      INT32 enumPrefRepl = PREFER_REPL_ANYONE ;

      if ( prefReplStr && *prefReplStr && !*(prefReplStr+1) )
      {
         CHAR ch = *prefReplStr ;
         if ( ch >= '1' && ch <= '7' )
         {
            enumPrefRepl = (INT32)( ch - '0' ) ;
         }
         else if ( 'M' == ch || 'm' == ch )
         {
            enumPrefRepl = PREFER_REPL_MASTER ;
         }
         else if ( 'S' == ch || 's' == ch )
         {
            enumPrefRepl = PREFER_REPL_SLAVE ;
         }
      }
      return enumPrefRepl ;
   }

   INT32 utilPrefReplEnum2Str( INT32 enumPrefRepl, CHAR * prefReplStr,
                               UINT32 len )
   {
      ossMemset( prefReplStr, 0, len ) ;

      if ( enumPrefRepl >= PREFER_REPL_NODE_1 &&
           enumPrefRepl <= PREFER_REPL_NODE_7 )
      {
         ossSnprintf( prefReplStr, len-1, "%d", enumPrefRepl ) ;
      }
      else if ( enumPrefRepl == PREFER_REPL_MASTER )
      {
         ossSnprintf( prefReplStr, len-1, "%s", "M" ) ;
      }
      else if ( enumPrefRepl == PREFER_REPL_SLAVE )
      {
         ossSnprintf( prefReplStr, len-1, "%s", "S" ) ;
      }
      else if ( enumPrefRepl == PREFER_REPL_ANYONE )
      {
         ossSnprintf( prefReplStr, len-1, "%s", "A" ) ;
      }
      return SDB_OK ;
   }

   BSONObj utilGetErrorBson( INT32 flags, const CHAR *detail )
   {
      static BSONObj _retObj [SDB_MAX_ERROR + SDB_MAX_WARNING + 1] ;
      static BOOLEAN _init = FALSE ;
      static ossSpinXLatch _lock ;

      if ( FALSE == _init )
      {
         _lock.get() ;
         if ( FALSE == _init )
         {
            for ( SINT32 i = -SDB_MAX_ERROR; i <= SDB_MAX_WARNING ; i ++ )
            {
               BSONObjBuilder berror ;
               berror.append ( OP_ERRNOFIELD, i ) ;
               berror.append ( OP_ERRDESP_FIELD, getErrDesp ( i ) ) ;
               _retObj[ i + SDB_MAX_ERROR ] = berror.obj() ;
            }
            _init = TRUE ;
         }
         _lock.release() ;
      }

      if ( flags < -SDB_MAX_ERROR || flags > SDB_MAX_WARNING )
      {
         PD_LOG ( PDERROR, "Error code error[rc:%d]", flags ) ;
         flags = SDB_SYS ;
      }

      if ( detail && *detail != 0 )
      {
         BSONObjBuilder bb ;
         bb.append ( OP_ERRNOFIELD, flags ) ;
         bb.append ( OP_ERRDESP_FIELD, getErrDesp ( flags ) ) ;
         bb.append ( OP_ERR_DETAIL, detail ) ;
         return bb.obj() ;
      }
      return _retObj[ SDB_MAX_ERROR + flags ] ;
   }

   struct _utilShellRCItem
   {
      UINT32      _src ;
      INT32       _rc ;
      UINT32      _end ;
   } ;
   typedef _utilShellRCItem utilShellRCItem ;

   #define MAP_SHELL_RC_ITEM( src, rc )   { src, rc, 0 },

   utilShellRCItem* utilGetShellRCMap()
   {
      static utilShellRCItem s_srcMap[] = {
         MAP_SHELL_RC_ITEM( SDB_SRC_SUC, SDB_OK )
         MAP_SHELL_RC_ITEM( SDB_SRC_IO, SDB_IO )
         MAP_SHELL_RC_ITEM( SDB_SRC_PERM, SDB_PERM )
         MAP_SHELL_RC_ITEM( SDB_SRC_OOM, SDB_OOM )
         MAP_SHELL_RC_ITEM( SDB_SRC_INTERRUPT, SDB_INTERRUPT )
         MAP_SHELL_RC_ITEM( SDB_SRC_SYS, SDB_SYS )
         MAP_SHELL_RC_ITEM( SDB_SRC_NOSPC, SDB_NOSPC )
         MAP_SHELL_RC_ITEM( SDB_SRC_TIMEOUT, SDB_TIMEOUT )
         MAP_SHELL_RC_ITEM( SDB_SRC_NETWORK, SDB_NETWORK )
         MAP_SHELL_RC_ITEM( SDB_SRC_INVALIDPATH, SDB_INVALIDPATH )
         MAP_SHELL_RC_ITEM( SDB_SRC_CANNOT_LISTEN, SDB_NET_CANNOT_LISTEN )
         MAP_SHELL_RC_ITEM( SDB_SRC_CAT_AUTH_FAILED, SDB_CAT_AUTH_FAILED )
         MAP_SHELL_RC_ITEM( SDB_SRC_INVALIDARG, SDB_INVALIDARG )
         { 0, 0, 1 }
      } ;
      return &s_srcMap[0] ;
   }

   UINT32 utilRC2ShellRC( INT32 rc )
   {
      utilShellRCItem *pEntry = utilGetShellRCMap() ;
      while( 0 == pEntry->_end )
      {
         if ( rc == pEntry->_rc )
         {
            return pEntry->_src ;
         }
         ++pEntry ;
      }
      if ( rc >= 0 )
      {
         return rc ;
      }
      return SDB_SRC_SYS ;
   }

   INT32 utilShellRC2RC( UINT32 src )
   {
      utilShellRCItem *pEntry = utilGetShellRCMap() ;
      while( 0 == pEntry->_end )
      {
         if ( src == pEntry->_src )
         {
            return pEntry->_rc ;
         }
         ++pEntry ;
      }
      if ( (INT32)src <= 0 )
      {
         return src ;
      }
      return SDB_SYS ;
   }

}

