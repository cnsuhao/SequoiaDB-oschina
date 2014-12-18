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

   Source File Name = pmdAsyncSessionAgent.cpp

   Descriptive Name = Process MoDel Agent

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdAsyncSession.hpp"
#include "pmd.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"

#if defined ( SDB_ENGINE )
#include "rtn.hpp"
#endif // SDB_ENGINE

namespace engine
{

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDSYNCSESSIONAGENTEP, "pmdAsyncSessionAgentEntryPoint" )
   INT32 pmdAsyncSessionAgentEntryPoint ( pmdEDUCB * cb, void * pData )
   {
      PD_TRACE_ENTRY ( SDB_PMDSYNCSESSIONAGENTEP );
      _pmdAsyncSession * pSession = (_pmdAsyncSession*)pData ;
      pmdEDUEvent event ;
      pmdBuffInfo *pBuffInfo = NULL ;
      MsgHeader *pMsg = NULL ;
      INT32 timeDiff = 0 ;
#if defined ( SDB_ENGINE )
      pmdKRCB *krcb    = pmdGetKRCB() ;
      monDBCB *mondbcb = krcb->getMonDBCB () ;
#endif // SDB_ENGINE

      pSession->attachIn ( cb ) ;

      while ( !cb->isDisconnected() )
      {
         cb->resetInterrupt() ;
         cb->resetInfo( EDU_INFO_ERROR ) ;

         if ( cb->waitEvent( event, OSS_ONE_SEC ) )
         { 
            if ( PMD_EDU_EVENT_TERM == event._eventType )
            {
               PD_LOG ( PDDEBUG, "EDU[%lld, %s] is terminated", cb->getID(),
                        getEDUName( cb->getType() ) ) ;
            }
            else if ( PMD_EDU_EVENT_MSG == event._eventType )
            {
#if defined ( SDB_ENGINE )
               mondbcb->addReceiveNum () ;
#endif // SDB_ENGINE
               if ( 0 == event._userData )
               {
                  pBuffInfo = ( pmdBuffInfo* )( event._Data ) ;
                  pMsg = ( MsgHeader* )( pBuffInfo->pBuffer ) ;

                  timeDiff = (INT32)(time( NULL ) - pBuffInfo->addTime) ;
               }
               else
               {
                  pBuffInfo = NULL ;
                  pMsg = ( MsgHeader* )event._Data ;
                  timeDiff = 0 ;
               }

               if ( timeDiff > 2 )
               {
                  PD_LOG( PDINFO, "Session[%s] msg[opCode:[%d]%d, requestID: "
                          "%lld, TID: %d, Len: %d] stay over %d seconds",
                          pSession->sessionName(), IS_REPLY_TYPE(pMsg->opCode),
                          GET_REQUEST_TYPE(pMsg->opCode), pMsg->requestID,
                          pMsg->TID, pMsg->messageLength, timeDiff ) ;
               }

               pSession->dispatchMsg ( pSession->netHandle(), pMsg,
                                       &timeDiff ) ;

               if ( timeDiff > 20 )
               {
                  PD_LOG( PDINFO, "Session[%s] msg[opCode:[%d]%d, requestID: "
                          "%lld, TID: %d, Len: %d] processed over %d seconds",
                          pSession->sessionName(), IS_REPLY_TYPE(pMsg->opCode),
                          GET_REQUEST_TYPE(pMsg->opCode), pMsg->requestID,
                          pMsg->TID, pMsg->messageLength, timeDiff ) ;
               }

               if ( pBuffInfo )
               {
                  pBuffInfo->setFree () ;
               }
            }
            else
            {
               pSession->dispatch ( &event ) ;
            }

            pmdEduEventRelase( event, cb ) ;
            event.reset () ;
         }
         else
         {
            pSession->onTimer( 0, OSS_ONE_SEC ) ;
         }
      }

      pSession->attachOut () ;

      PD_TRACE_EXIT ( SDB_PMDSYNCSESSIONAGENTEP );
      return SDB_OK ;
   }

}

