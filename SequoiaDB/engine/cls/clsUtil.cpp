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

   Source File Name = clsUtil.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of clsication component. This file contains structure for
   clsication control block.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include <vector>
#include "clsUtil.hpp"
#include "pmd.hpp"
#include <iostream>
#include "pdTrace.hpp"
#include "clsTrace.hpp"

using namespace std ;

namespace engine
{

   void clsStrcpy( const CHAR *src, CHAR *dst, const UINT32 &dstLen )
   {
      UINT32 strLen = ossStrlen( src ) ;
      UINT32 cpLen = strLen < dstLen - 1 ?
                     strLen : dstLen - 1;
      ossMemcpy( dst, src, cpLen ) ;
      dst[cpLen] = '\0' ;
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CLSSYNCWIN, "clsSyncWindow" )
   CLS_SYNC_STATUS clsSyncWindow( const DPS_LSN &remoteLsn,
                                  const DPS_LSN &fileBeginLsn,
                                  const DPS_LSN &memBeginLSn,
                                  const DPS_LSN &endLsn )
   {
      PD_TRACE_ENTRY ( SDB_CLSSYNCWIN ) ;
      CLS_SYNC_STATUS status = CLS_SYNC_STATUS_NONE ;
      INT32 frc = fileBeginLsn.compare( remoteLsn ) ;
      INT32 mrc = memBeginLSn.compare( remoteLsn ) ;
      INT32 erc = endLsn.compare( remoteLsn ) ;


      if ( 0 < frc )
      {
         goto done ;
      }
      if ( frc <= 0 && 0 < mrc )
      {
         status = CLS_SYNC_STATUS_RC ;
         goto done ;
      }
      else if ( mrc <= 0 && 0 < erc )
      {
         status = CLS_SYNC_STATUS_PEER ;
         goto done ;
      }
      else if ( 0 == erc )
      {
         status = CLS_SYNC_STATUS_PEER ;
         goto done ;
      }
      else
      {
         goto done ;
      }
   done:
      PD_TRACE_EXIT ( SDB_CLSSYNCWIN ) ;
      return status ;
   }

   void clsJoin2Full( const CHAR *cs, const CHAR *collection,
                      CHAR *full )
   {
      UINT32 csLen = ossStrlen( cs ) ;
      UINT32 clLen = ossStrlen( collection ) ;
      ossMemcpy( full, cs, csLen ) ;
      full[csLen] = '.' ;
      ossMemcpy( full + csLen + 1, collection, clLen ) ;
      full[csLen+clLen+1] = '\0' ;
      return ;
   }

   #define CLS_SYNC_NONE_STR           "None"
   #define CLS_SYNC_KEEPNORMAL_STR     "KeepNormal"
   #define CLS_SYNC_KEEPALL_STR        "KeepAll"

   INT32 clsString2Strategy( const CHAR * str, INT32 &sty )
   {
      INT32 rc = SDB_OK ;
      if ( !str || !*str )
      {
         sty = CLS_SYNC_DTF_STRATEGY ;
      }
      else
      {
         UINT32 len = ossStrlen( str ) ;
         if ( 0 == ossStrncasecmp( str, CLS_SYNC_NONE_STR, len ) &&
              len == ossStrlen( CLS_SYNC_NONE_STR ) )
         {
            sty = CLS_SYNC_NONE ;
         }
         else if ( 0 == ossStrncasecmp( str, CLS_SYNC_KEEPNORMAL_STR, len ) &&
                   len == ossStrlen( CLS_SYNC_KEEPNORMAL_STR ) )
         {
            sty = CLS_SYNC_KEEPNORMAL ;
         }
         else if ( 0 == ossStrncasecmp( str, CLS_SYNC_KEEPALL_STR, len ) &&
                   len == ossStrlen( CLS_SYNC_KEEPALL_STR ) )
         {
            sty = CLS_SYNC_KEEPALL ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
         }
      }
      return rc ;
   }

   INT32 clsStrategy2String( INT32 sty, CHAR * str, UINT32 len )
   {
      INT32 rc = SDB_OK ;

      switch ( sty )
      {
         case CLS_SYNC_NONE :
            ossStrncpy( str, CLS_SYNC_NONE_STR, len - 1 ) ;
            break ;
         case CLS_SYNC_KEEPNORMAL :
            ossStrncpy( str, CLS_SYNC_KEEPNORMAL_STR, len -1 ) ;
            break ;
         case CLS_SYNC_KEEPALL :
            ossStrncpy( str, CLS_SYNC_KEEPALL_STR, len -1 ) ;
            break ;
         default :
            str[0] = 0 ;
            rc = SDB_INVALIDARG ;
      }
      str[ len -1 ] = 0 ;
      return rc ;
   }


}
