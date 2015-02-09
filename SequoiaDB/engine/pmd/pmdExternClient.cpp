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

   Source File Name = pmdExternClient.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          12/11/2014  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/


#include "pmdExternClient.hpp"
#include "pmdEDU.hpp"

#if defined ( SDB_ENGINE )
   #include "clsMgr.hpp"
   #include "omManager.hpp"
   #include "msgAuth.hpp"
   #include "coordCB.hpp"
   #include "rtnCoord.hpp"
   #include "rtnCoordOperator.hpp"
#endif // SDB_ENGINE

#include "../bson/bson.h"

using namespace bson ;

namespace engine
{

   /*
      _pmdExternClient implement
   */
   _pmdExternClient::_pmdExternClient( ossSocket *pSocket )
   {
      SDB_ASSERT( pSocket, "Socket can't be NULL" ) ;

      _isAuthed      = FALSE ;
      _pSocket       = pSocket ;
      _pEDUCB        = NULL ;

      _localPort     = 0 ;
      _peerPort      = 0 ;
      ossMemset( _localIP, 0, sizeof( _localIP ) ) ;
      ossMemset( _peerIP, 0, sizeof( _peerIP ) ) ;
      ossMemset( _clientName, 0, sizeof( _clientName ) ) ;

      if ( pSocket )
      {
         _localPort = pSocket->getLocalPort() ;
         _peerPort = pSocket->getPeerPort() ;
         pSocket->getLocalAddress( _localIP, PMD_IPADDR_LEN ) ;
         pSocket->getPeerAddress( _peerIP, PMD_IPADDR_LEN ) ;
      }

      _makeName() ;
   }

   _pmdExternClient::~_pmdExternClient()
   {
      _pSocket       = NULL ;
      _pEDUCB        = NULL ;
   }

   SDB_CLIENT_TYPE _pmdExternClient::clientType() const
   {
      return SDB_CLIENT_EXTERN ;
   }

   const CHAR* _pmdExternClient::clientName() const
   {
      return _clientName ;
   }

   void _pmdExternClient::_makeName()
   {
      if ( 0 == _peerIP[ 0 ] )
      {
         ossStrcpy( _clientName, "noip-Extern" ) ;
      }
      else if ( _username.empty() )
      {
         ossSnprintf( _clientName, PMD_CLIENTNAME_LEN, "%s:%u-Extern",
                      _peerIP, _peerPort ) ;
      }
      else
      {
         ossSnprintf( _clientName, PMD_CLIENTNAME_LEN, "%s@%s:%u-Extern",
                      _username.c_str(), _peerIP, _peerPort ) ;
      }
   }

   INT32 _pmdExternClient::authenticate( MsgHeader *pMsg )
   {
#if defined ( SDB_ENGINE )
      INT32 rc = SDB_OK ;
      BSONObj authObj ;
      BSONElement user, pass ;
      rc = extractAuthMsg( pMsg, authObj ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Client[%s] extract auth msg failed, rc: %d",
                 clientName(), rc ) ;
         goto error ;
      }
      user = authObj.getField( SDB_AUTH_USER ) ;
      pass = authObj.getField( SDB_AUTH_PASSWD ) ;

      _isAuthed = FALSE ;

      if ( SDB_ROLE_STANDALONE == pmdGetDBRole() ) // not auth
      {
         _isAuthed = TRUE ;
         goto done ;
      }
      else if ( SDB_ROLE_OM == pmdGetDBRole() )
      {
         rc = sdbGetOMManager()->authenticate( authObj, _pEDUCB ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Client[%s] authenticate failed[user: %s, "
                    "passwd: %s], rc: %d", clientName(), user.valuestrsafe(),
                    pass.valuestrsafe(), rc ) ;
            goto error ;
         }
         _isAuthed = TRUE ;
      }
      else if ( SDB_ROLE_COORD == pmdGetDBRole() )
      {
         CHAR *pResult = NULL ;
         MsgOpReply replayHeader ;
         BSONObj *pErrObj = NULL ;

         CoordCB *pCoordcb = pmdGetKRCB()->getCoordCB();
         rtnCoordProcesserFactory *pProcesserFactory =
            pCoordcb->getProcesserFactory();
         rtnCoordOperator *pOperator = NULL ;
         pOperator = pProcesserFactory->getOperator( pMsg->opCode );
         rc = pOperator->execute( (CHAR*)pMsg, pMsg->messageLength,
                                  &pResult, _pEDUCB, replayHeader,
                                  &pErrObj ) ;
         SDB_ASSERT( NULL == pErrObj, "ErrObj must be NULL" ) ;
         SDB_ASSERT( NULL == pResult, "Result must be NULL" ) ;
         if ( MSG_AUTH_VERIFY_REQ == pMsg->opCode &&
              SDB_CAT_NO_ADDR_LIST == rc )
         {
            rc = SDB_OK ;
            _isAuthed = TRUE ;
         }
         else if ( rc )
         {
            PD_LOG( PDERROR, "Client[%s] authenticate failed[user: %s, "
                    "passwd: %s], rc: %d", clientName(),
                    user.valuestrsafe(), pass.valuestrsafe(), rc ) ;
            goto error ;
         }
         else
         {
            _isAuthed = TRUE ;
         }
      }
      else
      {
         MsgHeader *pAuthRes = NULL ;
         shardCB *pShard = sdbGetShardCB() ;
         BOOLEAN hasRetry = FALSE ;

         while ( TRUE )
         {
            rc = pShard->syncSend( pMsg, CATALOG_GROUPID, TRUE, &pAuthRes ) ;
            if ( SDB_OK != rc )
            {
               rc = pShard->syncSend( pMsg, CATALOG_GROUPID, FALSE,
                                      &pAuthRes ) ;
               PD_RC_CHECK( rc, PDERROR, "Client[%s] failed to send auth "
                            "req to catalog, rc: %d", clientName(), rc ) ;
            }
            if ( NULL == pAuthRes )
            {
               rc = SDB_SYS ;
               PD_LOG( PDERROR, "syncsend return ok but res is NULL" ) ;
               goto error ;
            }
            rc = (( MsgInternalReplyHeader *)pAuthRes)->res ;
            SDB_OSS_FREE( (BYTE*)pAuthRes ) ;
            pAuthRes = NULL ;

            if ( SDB_CLS_NOT_PRIMARY == rc && !hasRetry )
            {
               hasRetry = TRUE ;
               pShard->updateCatGroup( TRUE, CLS_SHARD_TIMEOUT ) ;
               continue ;
            }
            else if ( rc )
            {
               PD_LOG( PDERROR, "Client[%s] authenticate failed[user: %s, "
                       "passwd: %s], rc: %d", clientName(),
                       user.valuestrsafe(), pass.valuestrsafe(), rc ) ;
               goto error ;
            }
            else
            {
               _isAuthed = TRUE ;
            }
            break ;
         }
      }

   done:
      if ( SDB_OK == rc && _isAuthed )
      {
         _pEDUCB->setUserInfo( user.valuestrsafe(), pass.valuestrsafe() ) ;
         _username = user.valuestrsafe() ;
         _password = pass.valuestrsafe() ;
         _makeName() ;
      }
      return rc ;
   error:
      goto done ;
#else
   _isAuthed = TRUE ;
   return SDB_OK ;
#endif // SDB_ENGINE
   }

   INT32 _pmdExternClient::authenticate( const CHAR *username,
                                         const CHAR *password )
   {
      INT32 rc = SDB_OK ;
      CHAR *pBuffer = NULL ;
      INT32 buffSize = 0 ;
      
      rc = msgBuildAuthMsg( &pBuffer, &buffSize, username, password, 0 ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to build auth msg, rc: %d", rc ) ;
         goto error ;
      }
      rc = authenticate( (MsgHeader*)pBuffer ) ;
   done:
      if ( pBuffer )
      {
         SDB_OSS_FREE( pBuffer ) ;
         pBuffer = NULL ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdExternClient::disconnect()
   {
      if ( _pSocket && !_pSocket->isClosed() )
      {
         _pSocket->close() ;
      }
      _isAuthed = FALSE ;
      return SDB_OK ;
   }

   BOOLEAN _pmdExternClient::isAuthed() const
   {
      return _isAuthed ;
   }

   void _pmdExternClient::setAuthed( BOOLEAN authed )
   {
      _isAuthed = authed ;
   }

   BOOLEAN _pmdExternClient::isClosed() const
   {
      if ( _pSocket )
      {
         return _pSocket->isClosed() ;
      }
      return TRUE ;
   }

   BOOLEAN _pmdExternClient::isConnected() const
   {
      if ( _pSocket )
      {
         return _pSocket->isConnected() ;
      }
      return FALSE ;
   }

   UINT16 _pmdExternClient::getLocalPort() const
   {
      return _localPort ;
   }

   UINT16 _pmdExternClient::getPeerPort() const
   {
      return _peerPort ;
   }

   const CHAR* _pmdExternClient::getLocalIPAddr() const
   {
      return _localIP ;
   }

   const CHAR* _pmdExternClient::getPeerIPAddr() const
   {
      return _peerIP ;
   }

   const CHAR* _pmdExternClient::getUsername() const
   {
      return _username.c_str() ;
   }

   const CHAR* _pmdExternClient::getPassword() const
   {
      return _password.c_str() ;
   }

}


