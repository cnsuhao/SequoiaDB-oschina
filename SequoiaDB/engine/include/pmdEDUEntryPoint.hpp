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

   Source File Name = pmdEDUEntryPoint.hpp

   Descriptive Name = Process MoDel Engine Dispatchable Unit Event Header

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains structure for events that
   used as inter-EDU communications.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          23/06/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef PMD_EDU_ENTRYPOINT_HPP__
#define PMD_EDU_ENTRYPOINT_HPP__

#include "pmdEDU.hpp"

namespace engine
{

   /*
      PMD ENTRY POINTERS
   */

   /*
      @Common Entry Point For CB Manager
      @pData: must be the _pmdObjBase pointer
   */
   INT32 pmdCBMgrEntryPoint ( pmdEDUCB *cb, void *pData ) ;

   /*
      @Common Entry Point For Async Session
      @pData: must be the pmdAsyncSession pointer
   */
   INT32 pmdAsyncSessionAgentEntryPoint ( pmdEDUCB *cb, void *pData ) ;

   /*
      @Common Entry Point For Async Net
      @pData: must be the _netRouteAgent pointer
   */
   INT32 pmdAsyncNetEntryPoint ( pmdEDUCB *cb, void *pData ) ;
   INT32 pmdLocalAgentEntryPoint( pmdEDUCB *cb, void *arg ) ;
   INT32 pmdRestAgentEntryPoint ( pmdEDUCB *cb, void *pData ) ;
   INT32 pmdTcpListenerEntryPoint ( pmdEDUCB *cb, void *arg ) ;
   INT32 pmdHTTPListenerEntryPoint ( pmdEDUCB *cb, void *arg ) ;
   INT32 pmdRestSvcEntryPoint ( pmdEDUCB *cb, void *arg ) ;
   INT32 pmdLoggWEntryPoint ( pmdEDUCB *cb, void *arg ) ;
   INT32 pmdClsNtyEntryPoint( pmdEDUCB * cb, void * arg ) ;
   INT32 pmdCoordNetWorkEntryPoint ( pmdEDUCB *cb, void *pData );
   INT32 pmdPreLoaderEntryPoint ( pmdEDUCB *cb, void *pData ) ;
   INT32 pmdBackgroundJobEntryPoint ( pmdEDUCB *cb, void *pData ) ;
   INT32 pmdDpsTransRollbackEntryPoint( pmdEDUCB *cb, void *pData ) ;
   INT32 pmdPipeListenerEntryPoint ( pmdEDUCB *cb, void *arg ) ;
   INT32 pmdLoadWorkerEntryPoint ( pmdEDUCB *cb, void *pData ) ;
   INT32 pmdSyncClockEntryPoint( pmdEDUCB *cb, void *arg ) ;

   INT32 pmdFapListenerEntryPoint ( pmdEDUCB *cb, void *pData ) ;
   INT32 pmdFapAgentEntryPoint( pmdEDUCB *cb, void *arg ) ;

}

#endif // PMD_EDU_ENTRYPOINT_HPP__

