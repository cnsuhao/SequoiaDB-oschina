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

   Source File Name = pmdSessionBase.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/04/2014  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef PMD_SESSION_BASE_HPP_
#define PMD_SESSION_BASE_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossSocket.hpp"
#include "sdbInterface.hpp"
#include "pmdProcessorBase.hpp"
#include "pmdExternClient.hpp"

#include <string>

namespace engine
{

   class _pmdEDUCB ;
   class _dpsLogWrapper ;
   class _pmdProcessor ;

   /*
      _pmdSession define
   */
   class _pmdSession : public _ISession
   {
      public:
         _pmdSession( SOCKET fd ) ;
         virtual ~_pmdSession() ;

         virtual void            clear() ;

         virtual const CHAR*     sessionName() const ;
         virtual IClient*        getClient() { return &_client ; }
         virtual IProcessor*     getProcessor() ;

         virtual INT32           run() = 0 ;

      public:
         UINT64      sessionID () const { return _eduID ; }
         EDUID       eduID () const { return _eduID ; }
         _pmdEDUCB*  eduCB () const { return _pEDUCB ; }
         ossSocket*  socket () { return &_socket ; }

         void        attach( _pmdEDUCB * cb ) ;
         void        detach() ;

         void        attachProcessor( _pmdProcessor *pProcessor ) ;
         void        detachProcessor() ;

         _dpsLogWrapper* getDPSCB() { return _pDPSCB ; }

         CHAR*       getBuff( INT32 len ) ;
         INT32       getBuffLen () const { return _buffLen ; }

         INT32       allocBuff( INT32 len, CHAR **ppBuff, INT32 &buffLen ) ;
         void        releaseBuff( CHAR *pBuff, INT32 buffLen ) ;
         INT32       reallocBuff( INT32 len, CHAR **ppBuff, INT32 &buffLen ) ;

         void        disconnect() ;
         INT32       sendData( const CHAR *pData, INT32 size,
                               INT32 timeout = -1,
                               BOOLEAN block = TRUE,
                               INT32 *pSentLen = NULL,
                               INT32 flags = 0 ) ;
         INT32       recvData( CHAR *pData, INT32 size,
                               INT32 timeout = -1,
                               BOOLEAN block = TRUE,
                               INT32 *pRecvLen = NULL,
                               INT32 flags = 0 ) ;
         INT32       sniffData( INT32 timeout = OSS_ONE_SEC ) ;

      protected:

         _pmdEDUCB                        *_pEDUCB ;
         EDUID                            _eduID ;
         ossSocket                        _socket ;
         std::string                      _sessionName ;
         pmdExternClient                  _client ;
         _pmdProcessor                    *_processor ;
         _dpsLogWrapper                   *_pDPSCB ;

      protected:
         CHAR                             *_pBuff ;
         INT32                            _buffLen ;

   } ;
   typedef _pmdSession pmdSession ;

   /*
      _pmdProcessor define
   */
   class _pmdProcessor : public _IProcessor
   {
      friend class _pmdSession ;
      public:
         _pmdProcessor() ;
         virtual ~_pmdProcessor() ;

         virtual ISession*             getSession() { return _pSession ; }

      protected:
         void     attachSession( pmdSession *pSession ) ;
         void     detachSession() ;

         _dpsLogWrapper*   getDPSCB() ;
         _IClient*         getClient() ;
         _pmdEDUCB*        eduCB() ;
         EDUID             eduID () const ;

      protected:
         pmdSession                 *_pSession ;

   } ;
   typedef _pmdProcessor pmdProcessor ;

}

#endif //PMD_SESSION_BASE_HPP_

