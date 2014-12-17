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

   Source File Name = pmdRestSvc.cpp

   Descriptive Name = Process MoDel HTTP Listener ( REST requests )

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main entry point for HTTP
   Listener.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          14/04/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include "pd.hpp"
#include "pmd.hpp"
#include "pmdEDUMgr.hpp"
#include "ossSocket.hpp"
#include "pmdRestSession.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "pmdProcessor.hpp"

namespace engine
{

   /*
      rest service entry point
   */
   INT32 pmdRestSvcEntryPoint ( pmdEDUCB *cb, void *pData )
   {
      INT32 rc                = SDB_OK ;
      pmdKRCB *krcb           = pmdGetKRCB() ;
      monDBCB *mondbcb        = krcb->getMonDBCB () ;
      pmdEDUMgr *eduMgr       = cb->getEDUMgr() ;
      ossSocket *pListerner   = ( ossSocket* )pData ;
      EDUID agentEDU          = PMD_INVALID_EDUID ;

      if ( SDB_OK != ( rc = eduMgr->activateEDU ( cb )) )
      {
         goto error ;
      }

      while ( !cb->isDisconnected() )
      {
         SOCKET s ;
         rc = pListerner->accept ( &s, NULL, NULL ) ;
         // if we don't get anything for a period of time, let's loop
         if ( SDB_TIMEOUT == rc || SDB_TOO_MANY_OPEN_FD == rc  )
         {
            rc = SDB_OK ;
            continue ;
         }
         // if we receive error due to database down, we finish
         if ( rc && PMD_IS_DB_DOWN )
         {
            rc = SDB_OK ;
            goto done ;
         }
         else if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to accept rest socket, rc: %d",
                     rc ) ;
            if ( pListerner->isClosed() )
            {
               break ;
            }
            else
            {
               continue ;
            }
         }

         cb->incEventCount() ;
         ++mondbcb->numConnects ;

         // assign the socket to the arg
         void *pData = NULL ;
         *((SOCKET *) &pData) = s ;

         if ( !krcb->isActive() )
         {
            ossSocket newsock ( &s ) ;
            newsock.close () ;
            continue ;
         }

         // now we have a tcp socket for a new connection, let's get an agent
         // Note the new new socket sent passing to startEDU
         if ( SDB_ROLE_OM == pmdGetDBRole() || SDB_ROLE_DATA == pmdGetDBRole() 
              || SDB_ROLE_STANDALONE == pmdGetDBRole() 
              || SDB_ROLE_COORD == pmdGetDBRole() )
         {
            rc = eduMgr->startEDU ( EDU_TYPE_RESTAGENT, pData, &agentEDU ) ;
         }
         else
         {
            rc = eduMgr->startEDU ( EDU_TYPE_HTTPAGENT, pData, &agentEDU ) ;
         }

         if ( rc )
         {
            PD_LOG( ( rc == SDB_QUIESCED ? PDWARNING : PDERROR ),
                    "Failed to start edu, rc: %d", rc ) ;

            // close remote connection if we can't create new thread
            ossSocket newsock ( &s ) ;
            newsock.close () ;
            continue ;
         }
      } //while ( ! cb->isDisconnected() )

   done :
      return rc ;
   error :
      goto done ;
   }

   /*
      rest agent entry point
   */
   INT32 pmdRestAgentEntryPoint( pmdEDUCB *cb, void *pData )
   {
      INT32 rc = SDB_OK ;

      SOCKET s = *(( SOCKET *) &pData ) ;

      pmdRestSession restSession( s ) ;
      restSession.attach( cb ) ;
      if ( SDB_ROLE_OM == pmdGetDBRole() )
      {
         rc = restSession.run() ;
      }
      else if ( SDB_ROLE_STANDALONE == pmdGetDBRole() 
                || SDB_ROLE_DATA == pmdGetDBRole() )
      {
         _pmdDataProcessor processor ;
         restSession.attachProcessor( &processor ) ;
         processor.attachSession( &restSession ) ;
         rc = restSession.run1() ;
         processor.detachSession() ;
         restSession.detachProcessor() ;
      }
      else if ( SDB_ROLE_COORD == pmdGetDBRole() )
      {
         //TODO: _CoordProcessor
         //cb->setClientSock( &s ) ;
         _pmdCoordProcessor processor ;
         restSession.attachProcessor( &processor ) ;
         processor.attachSession( &restSession ) ;
         rc = restSession.run1() ;
         processor.detachSession() ;
         restSession.detachProcessor() ;
      }
      restSession.detach() ;

      return rc ;
   }

}

