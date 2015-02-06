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

   Source File Name = pmdRestSession.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/14/2014  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef PMD_REST_SESSION_HPP_
#define PMD_REST_SESSION_HPP_

#include "pmdSessionBase.hpp"
#include "restDefine.hpp"
#include "ossLatch.hpp"
#include "ossAtomic.hpp"
#include "../omsvc/omCommandInterface.hpp"
#include "pmdProcessorBase.hpp"
#include <string>

using namespace bson ;

namespace engine
{

   class _dpsLogWrapper ;
   class _SDB_RTNCB ;
   class restAdaptor ;

   /*
      global define
   */
   #define SESSION_USER_NAME_LEN       ( 63 )

   /*
      _restSessionInfo define
   */
   struct _restSessionInfo : public SDBObject
   {
      struct _sessionAttr
      {
         UINT64            _sessionID ;      // host ip + seq
         UINT64            _loginTime ;
         CHAR              _userName[SESSION_USER_NAME_LEN+1] ;
      } _attr ;

      UINT64               _activeTime ;
      INT64                _timeoutCounter ; // ms
      BOOLEAN              _authOK ;
      ossAtomic32          _inNum ;
      ossSpinXLatch        _inLatch ;
      BOOLEAN              _isLock ;

      std::string          _id ;

      _pmdEDUCB::CATCH_MAP _catchMap ;

      _restSessionInfo()
      :_inNum( 0 )
      {
         _attr._sessionID     = 0 ;
         _attr._loginTime     = (UINT64)time(NULL) ;
         ossMemset( _attr._userName, 0, sizeof( _attr._userName ) ) ;
         _activeTime          = _attr._loginTime ;
         _timeoutCounter      = 0 ;
         _authOK              = FALSE ;
         _isLock              = FALSE ;
      }
      INT32 getAttrSize()
      {
         return (INT32)sizeof( _attr ) ;
      }
      BOOLEAN isValid() const
      {
         return _id.size() == 0 ? FALSE : TRUE ;
      }
      BOOLEAN isIn()
      {
         return !_inNum.compare( 0 ) ;
      }
      void invalidate()
      {
         _id     = "" ;
         _authOK = FALSE ;
      }
      void active()
      {
         _activeTime       = (UINT64)time( NULL ) ;
         _timeoutCounter   = 0 ;
      }
      BOOLEAN isTimeout( INT64 timeout ) const
      {
         if ( timeout > 0 && _timeoutCounter > timeout )
         {
            return TRUE ;
         }
         return FALSE ;
      }
      void onTimer( UINT32 interval )
      {
         _timeoutCounter += interval ;
      }
      void lock()
      {
         _inLatch.get() ;
         _isLock = TRUE ;
      }
      void unlock()
      {
         _isLock = FALSE ;
         _inLatch.release() ;
      }
      BOOLEAN isLock() const
      {
         return _isLock ;
      }

      void releaseMem() ;
      void pushMemToMap( _pmdEDUCB::CATCH_MAP &catchMap ) ;
      void makeMemFromMap( _pmdEDUCB::CATCH_MAP &catchMap ) ;

   } ;
   typedef _restSessionInfo restSessionInfo ;


   class RestToMSGTransfer ;

   /*
      _pmdRestSession define
   */
   class _pmdRestSession : public _pmdSession
   {
      public:
         _pmdRestSession( SOCKET fd ) ;
         virtual ~_pmdRestSession () ;

         virtual UINT64    identifyID() ;
         virtual INT32     getServiceType() const ;
         virtual SDB_SESSION_TYPE sessionType() const ;

         virtual INT32     run() ;


      public:
         httpConnection*   getRestConn() { return &_restConn ; }
         CHAR*             getFixBuff() ;
         INT32             getFixBuffSize () const ;

         BOOLEAN           isAuthOK() ;
         string            getLoginUserName() ;
         const CHAR*       getSessionID() ;

         void              doLogout () ;
         INT32             doLogin ( const string &username,
                                     UINT32 localIP ) ;

         INT32             _dealWithLoginReq( INT32 result ) ;

      protected:
         virtual void      _onAttach () ;
         virtual void      _onDetach () ;

         void              restoreSession() ;
         void              saveSession() ;

      protected:

         INT32             _processOMRestMsg( HTTP_PARSE_COMMON command,
                                              const CHAR *pFilePath ) ;

         INT32             _fetchOneContext( SINT64 &contextID, 
                                             rtnContextBuf &contextBuff ) ;
         INT32             _processMsg( HTTP_PARSE_COMMON command, 
                                        const CHAR *pFilePath ) ;
         INT32             _processBusinessMsg( restAdaptor *pAdaptor, 
                                                HTTP_PARSE_COMMON command, 
                                                const CHAR *pFilePath ) ;
         INT32             _translateMSG( restAdaptor *pAdaptor, 
                                          HTTP_PARSE_COMMON command, 
                                          const CHAR *pFilePath, 
                                          MsgHeader **msg ) ;

      private:
         omRestCommandBase *_createCommand( HTTP_PARSE_COMMON command,
                                            const CHAR *pFilePath ) ;
      protected:
         httpConnection    _restConn ;
         CHAR*             _pFixBuff ;

         restSessionInfo*  _pSessionInfo ;

         string            _wwwRootPath ;

         _SDB_RTNCB        *_pRTNCB ;

         RestToMSGTransfer *_pRestTransfer ;

   } ;
   typedef _pmdRestSession pmdRestSession ;


   #define REST_CMD_NAME_QUERY         "query"
   #define REST_CMD_NAME_INSERT        "insert"
   #define REST_CMD_NAME_UPDATE        "update"
   #define REST_CMD_NAME_DELETE        "delete"

   #define REST_KEY_NAME_FLAG          "Flag"
   #define REST_KEY_NAME_INSERTOR      "Insertor"
   #define REST_KEY_NAME_UPDATOR       "Updator"
   #define REST_KEY_NAME_DELETOR       "Deletor"
   
   class RestToMSGTransfer : public SDBObject
   {
      public:
         RestToMSGTransfer( pmdRestSession *session ) ;
         ~RestToMSGTransfer() ;
         
      public:
         INT32       trans( restAdaptor *pAdaptor, HTTP_PARSE_COMMON command, 
                            const CHAR *pFilePath, MsgHeader **msg ) ;
                            
      private:
         INT32       _convertCreateCS( restAdaptor *pAdaptor, 
                                       MsgHeader **msg ) ;
         INT32       _convertCreateCL( restAdaptor *pAdaptor, 
                                       MsgHeader **msg ) ;
         INT32       _convertDropCS( restAdaptor *pAdaptor, MsgHeader **msg ) ;
         INT32       _convertDropCL( restAdaptor *pAdaptor, MsgHeader **msg ) ;

         INT32       _convertQuery( restAdaptor *pAdaptor, MsgHeader **msg ) ;
         INT32       _convertInsert( restAdaptor *pAdaptor, MsgHeader **msg ) ;
         INT32       _convertUpdate( restAdaptor *pAdaptor, MsgHeader **msg ) ;
         INT32       _convertDelete( restAdaptor *pAdaptor, MsgHeader **msg ) ;

         INT32       _convertSplit( restAdaptor *pAdaptor, MsgHeader **msg ) ;
         INT32       _convertListGroups( restAdaptor *pAdaptor,
                                         MsgHeader **msg ) ;
         INT32       _convertAlterCollection( restAdaptor *pAdaptor,
                                              MsgHeader **msg ) ;
         INT32       _convertGetCount( restAdaptor *pAdaptor, 
                                       MsgHeader **msg ) ;

         INT32       _convertLogin( restAdaptor *pAdaptor, MsgHeader **msg ) ;

      private:
         pmdRestSession    *_restSession ;
   } ;

}

#endif //PMD_REST_SESSION_HPP_

