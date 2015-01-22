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

   Source File Name = catMainController.hpp

   Descriptive Name = Process MoDel Header

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains structure kernel control block,
   which is the most critical data structure in the engine process.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef CAT_MAIN_CONTROLLER_HPP__
#define CAT_MAIN_CONTROLLER_HPP__

#include "pmdObjBase.hpp"
#include "netMsgHandler.hpp"
#include "netTimer.hpp"
#include "msgCatalog.hpp"
#include "pmdEDU.hpp"
#include "pmd.hpp"
#include "ossEvent.hpp"
#include "catCatalogManager.hpp"
#include "catNodeManager.hpp"

#include <map>
#include <vector>

namespace engine
{

   class _SDB_DMSCB ;
   class _SDB_RTNCB ;
   class _authCB ;
   class sdbCatalogueCB ;

   /*
      catMainController define
   */
   class catMainController : public _pmdObjBase, public _netMsgHandler,
                             public _netTimeoutHandler
   {
   typedef std::map< SINT64, UINT64 >  CONTEXT_LIST ;
   typedef std::vector< pmdEDUEvent >  VEC_EVENT ;

   DECLARE_OBJ_MSG_MAP()

   public:
      catMainController() ;
      virtual ~catMainController() ;
      INT32 init() ;

      virtual void   attachCB( _pmdEDUCB *cb ) ;
      virtual void   detachCB( _pmdEDUCB *cb ) ;

      virtual void   onTimer ( UINT64 timerID, UINT32 interval ) ;

      ossEvent* getAttachEvent() { return &_attachEvent ; }
      ossEvent* getChangeEvent() { return &_changeEvent ; }

      BOOLEAN   delayCurOperation() ;
      BOOLEAN   isDelayed() const { return _isDelayed ; }

   public:
      INT32 handleMsg( const NET_HANDLE &handle,
                       const _MsgHeader *header,
                       const CHAR *msg ) ;
      void  handleClose( const NET_HANDLE &handle, _MsgRouteID id ) ;

      void  handleTimeout( const UINT32 &millisec, const UINT32 &id ) ;

   protected:
      virtual INT32 _defaultMsgFunc ( NET_HANDLE handle,
                                      MsgHeader* msg ) ;

      INT32 _processMsg( const NET_HANDLE &handle, MsgHeader *pMsg ) ;

      void  _dispatchDelayedOperation( BOOLEAN dispatch ) ;

   protected:
      INT32 _onActiveEvent( pmdEDUEvent *event ) ;
      INT32 _onDeactiveEvent( pmdEDUEvent *event ) ;

   protected :
      INT32 _processGetMoreMsg ( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 _processQueryDataGrp( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 _processQueryCollections( const NET_HANDLE &handle,
                                      MsgHeader *pMsg ) ;
      INT32 _processQueryCollectionSpaces ( const NET_HANDLE &handle,
                                            MsgHeader *pMsg ) ;
      INT32 _processQueryMsg( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 _processKillContext(const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 _processAuthenticate( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 _processAuthCrt( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 _processAuthDel( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 _processCheckRouteID( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 _processInterruptMsg( const NET_HANDLE &handle,
                                  MsgHeader *header ) ;
      INT32 _processDisconnectMsg( const NET_HANDLE &handle,
                                   MsgHeader *header ) ;
      INT32 _processQueryRequest ( const NET_HANDLE &handle,
                                   MsgHeader *pMsg,
                                   const CHAR *pCollectionName ) ;

   protected:
      INT32 _postMsg( const NET_HANDLE &handle, const MsgHeader *pHead ) ;
      INT32 _catBuildMsgEvent ( const NET_HANDLE &handle,
                                const MsgHeader *pMsg,
                                pmdEDUEvent &event ) ;
      INT32 _ensureMetadata() ;
      INT32 _createSysIndex ( const CHAR *pCollection,
                              const CHAR *pIndex,
                              pmdEDUCB *cb ) ;
      INT32 _createSysCollection ( const CHAR *pCollection,
                                   pmdEDUCB *cb ) ;
      void _addContext( const UINT32 &handle, UINT32 tid, INT64 contextID ) ;
      void _delContextByHandle( const UINT32 &handle ) ;
      void _delContext( const UINT32 &handle, UINT32 tid ) ;
      void _delContextByID( INT64 contextID, BOOLEAN rtnDel ) ;

   private :
      pmdEDUMgr         *_pEduMgr;
      sdbCatalogueCB    *_pCatCB;
      _SDB_DMSCB        *_pDmsCB;
      pmdEDUCB          *_pEDUCB;
      _SDB_RTNCB        *_pRtnCB;
      _authCB           *_pAuthCB;
      CONTEXT_LIST      _contextLst;

      ossEvent          _attachEvent ;
      BOOLEAN           _isActived ;

      ossEvent          _changeEvent ;

      VEC_EVENT         _vecEvent ;
      UINT32            _checkEventTimerID ;
      pmdEDUEvent       _lastDelayEvent ;
      BOOLEAN           _isDelayed ;

   } ;

}

#endif // CAT_MAIN_CONTROLLER_HPP__

