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

   Source File Name = pmdCBMgrEntryPoint.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          30/11/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdEDUMgr.hpp"
#include "pmdObjBase.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"

namespace engine
{

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDCBMGREP, "pmdCBMgrEntryPoint" )
   INT32 pmdCBMgrEntryPoint ( pmdEDUCB *cb, void *pData )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDCBMGREP );

      _pmdObjBase *pObj = ( _pmdObjBase* )pData ;
      pmdEDUMgr *pEDUMgr = cb->getEDUMgr() ;
      pmdEDUEvent eventData;

      pObj->attachCB( cb ) ;

      rc = pEDUMgr->activateEDU( cb->getID() ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Failed to active EDU" ) ;
         goto error ;
      }

      while ( !cb->isDisconnected() )
      {
         if ( cb->waitEvent( eventData, OSS_ONE_SEC ) )
         {
            if ( PMD_EDU_EVENT_TERM == eventData._eventType )
            {
               PD_LOG ( PDDEBUG, "EDU[%lld, %s] is terminated", cb->getID(),
                        getEDUName( cb->getType() ) ) ;
            }
            else if ( PMD_EDU_EVENT_MSG == eventData._eventType )
            {
               pObj->dispatchMsg( (NET_HANDLE)eventData._userData,
                                  (MsgHeader*)(eventData._Data) ) ;
            }
            else
            {
               pObj->dispatchEvent ( &eventData ) ;
            }

            pmdEduEventRelase( eventData, cb ) ;
            eventData.reset () ;
         }
      }

   done:
      pObj->detachCB( cb ) ;
      PD_TRACE_EXITRC ( SDB_PMDCBMGREP, rc );
      return rc ;
   error:
      goto done ;
   }

}

