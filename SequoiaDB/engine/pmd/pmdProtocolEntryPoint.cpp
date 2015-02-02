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
#include "pmdModuleLoader.hpp"

namespace engine {

   INT32 pmdFapListenerEntryPoint ( pmdEDUCB *cb, void *pData )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDTCPLSTNENTPNT ) ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      monDBCB *mondbcb = krcb->getMonDBCB () ;
      pmdEDUMgr * eduMgr = cb->getEDUMgr() ;
      EDUID agentEDU = PMD_INVALID_EDUID ;

      pmdEDUParam* param = ( pmdEDUParam * )pData ;
      ossSocket *pListerner = (ossSocket *)(param->pSocket) ;
      IPmdAccessProtocol *protocol = param->protocol ;
      SDB_OSS_DEL param ;
      param = NULL ;

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

         pmdEDUParam *pParam = SDB_OSS_NEW pmdEDUParam() ;
         *(( SOCKET *)&pParam->pSocket) = s ;
         pParam->protocol = protocol ;

         if ( !krcb->isActive() )
         {
            ossSocket newsock ( &s ) ;
            newsock.close () ;
            continue ;
         }

         rc = eduMgr->startEDU ( EDU_TYPE_FAPAGENT, (void *)pParam, &agentEDU ) ;
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

   INT32 pmdFapAgentEntryPoint( pmdEDUCB *cb, void *arg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDLOCALAGENTENTPNT );
      pmdSession *session = NULL ;
      pmdEDUParam *pParam = ( pmdEDUParam * )arg ;
      SOCKET s = *((SOCKET *)&pParam->pSocket) ;
      IPmdAccessProtocol* protocol = pParam->protocol ;
      SDB_OSS_DEL pParam ;
      pParam = NULL ;

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
