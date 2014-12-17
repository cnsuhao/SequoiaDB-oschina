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

   Source File Name = pmdCluster.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          30/11/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdEDUMgr.hpp"
#include "clsMgr.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"

namespace engine
{

   INT32 pmdClsNtyEntryPoint( pmdEDUCB * cb, void * arg )
   {
      INT32 rc = SDB_OK ;
      clsLSNNtyInfo lsnInfo ;
      EDUID myEDUID = cb->getID () ;
      pmdEDUMgr * eduMgr = cb->getEDUMgr() ;
      replCB *pReplCb = ( replCB* )arg ;
      ossQueue< clsLSNNtyInfo > *pNtyQue = pReplCb->getNtyQue() ;

      rc = eduMgr->activateEDU ( myEDUID ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to activate EDU" ) ;
         goto error ;
      }

      while ( !cb->isDisconnected() )
      {
         if ( !pNtyQue->timed_wait_and_pop( lsnInfo, OSS_ONE_SEC ) )
         {
            continue ;
         }
         cb->incEventCount() ;
         pReplCb->notify2Session( lsnInfo._csLID, lsnInfo._clLID,
                                  lsnInfo._extLID, lsnInfo._offset ) ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

}

