#include <stdio.h>
#include "pd.hpp"
#include "ossMem.hpp"
#include "pmd.hpp"
#include "pmdEDUMgr.hpp"
#include "ossSocket.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "pmdProcessor.hpp"
#include "pmdAccessProtocolBase.hpp"

namespace engine {

   INT32 pmdProtocolListenerEntryPoint ( pmdEDUCB *cb, void *pData )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDTCPLSTNENTPNT ) ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      monDBCB *mondbcb = krcb->getMonDBCB () ;
      pmdEDUMgr * eduMgr = cb->getEDUMgr() ;
      EDUID agentEDU = PMD_INVALID_EDUID ;
      ossSocket *pListerner = ( ossSocket* )pData ;

      if ( SDB_OK != ( rc = eduMgr->activateEDU ( cb ) ) )
      {
         goto error ;
      }

      while ( ! cb->isDisconnected() )
      {
         SOCKET s ;
         rc = pListerner->accept ( &s, NULL, NULL ) ;
         if ( SDB_TIMEOUT == rc || SDB_TOO_MANY_OPEN_FD == rc )
         {
            rc = SDB_OK ;
            continue ;
         }
         if ( rc && PMD_IS_DB_DOWN )
         {
            rc = SDB_OK ;
            goto done ;
         }
         else if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to accept socket in TcpListener(rc=%d)",
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

         void *pData = NULL ;
         *((SOCKET *) &pData) = s ;

         if ( !krcb->isActive() )
         {
            ossSocket newsock ( &s ) ;
            newsock.close () ;
            continue ;
         }

         rc = eduMgr->startEDU ( EDU_TYPE_PROTOCOL, pData, &agentEDU ) ;
         if ( rc )
         {
            PD_LOG( ( rc == SDB_QUIESCED ? PDWARNING : PDERROR ),
               "Failed to start edu, rc: %d", rc ) ;

            ossSocket newsock ( &s ) ;
            newsock.close () ;
            continue ;
         }
      } //while ( ! cb->isDisconnected() )

      if ( SDB_OK != ( rc = eduMgr->waitEDU ( cb ) ) )
      {
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_PMDTCPLSTNENTPNT, rc );
      return rc;

   error :
      switch ( rc )
      {
      case SDB_SYS :
         PD_LOG ( PDSEVERE, "System error occured" ) ;
         break ;
      default :
         PD_LOG ( PDSEVERE, "Internal error" ) ;
         break ;
      }
      goto done ;
   }

   INT32 pmdProtocolEntryPoint( pmdEDUCB *cb, void *arg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDLOCALAGENTENTPNT );
      pmdSession *session = NULL ;
      SOCKET s = *(( SOCKET *) &arg ) ;
      IPmdAccessProtocol* protocol = NULL ;

      if ( pmdGetDBRole() == SDB_ROLE_COORD )
      {
         pmdCoordProcessor coordProcessor ;
         session = protocol->getSession( s, &coordProcessor ) ;
         if ( NULL == session )
         {
            PD_LOG( PDERROR, "Failed to get Session of protocol" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         session->attach( cb ) ;
         coordProcessor.attachSession( session ) ;
         rc = session->run() ;
         coordProcessor.detachSession() ;
         session->detach() ;
         protocol->releaseSession( session ) ;
      }
      else
      {
         pmdDataProcessor dataProcessor ;
         session = protocol->getSession( s, &dataProcessor ) ;
         if ( NULL == session )
         {
            PD_LOG( PDERROR, "Failed to get Session of protocol" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         session->attach( cb ) ;
         dataProcessor.attachSession( session ) ;
         rc = session->run() ;
         dataProcessor.detachSession() ;
         session->detach() ;
         protocol->releaseSession( session ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_PMDLOCALAGENTENTPNT, rc );
      return rc ;
   error:
      goto done ;
   }

}
