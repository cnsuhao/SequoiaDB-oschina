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

   Source File Name = catalogueCB.hpp

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

#ifndef CATALOGUECB_HPP_
#define CATALOGUECB_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "pmdDef.hpp"
#include "msg.hpp"
#include "netRouteAgent.hpp"
#include "msgCatalog.hpp"
#include "catMainController.hpp"
#include "catCatalogManager.hpp"
#include "catNodeManager.hpp"
#include "sdbInterface.hpp"
#include "catLevelLock.hpp"

namespace engine
{

   /*
      sdbCatalogueCB define
   */
   class sdbCatalogueCB : public _IControlBlock, public _IEventHander
   {
      friend class catMainController ;

      typedef std::map<UINT32, UINT32>    GRP_ID_MAP;
      typedef std::map<UINT16, UINT16>    NODE_ID_MAP;

      public:
         sdbCatalogueCB() ;
         virtual ~sdbCatalogueCB() ;

         virtual SDB_CB_TYPE cbType() const { return SDB_CB_CATALOGUE ; }
         virtual const CHAR* cbName() const { return "CATALOGUECB" ; }

         virtual INT32  init () ;
         virtual INT32  active () ;
         virtual INT32  deactive () ;
         virtual INT32  fini () ;
         virtual void   onConfigChange() ;

         virtual void   onRegistered( const MsgRouteID &nodeID ) ;
         virtual void   onPrimaryChange( BOOLEAN primary,
                                         SDB_EVENT_OCCUR_TYPE occurType ) ;
         virtual UINT32 getMask() const ;

         void     insertGroupID( UINT32 grpID, BOOLEAN isActive = TRUE ) ;
         void     removeGroupID( UINT32 grpID ) ;
         void     insertNodeID( UINT16 nodeID ) ;
         void     activeGroup( UINT32 groupID ) ;
         UINT32   AllocGroupID() ;
         INT32    getAGroupRand( INT32 &groupID) ;
         UINT16   AllocNodeID();
         void     releaseNodeID( UINT16 nodeID );
         UINT16   AllocCataNodeID();
         void     insertCataNodeID( UINT16 nodeID );
         void     releaseCataNodeID( UINT16 nodeID );

         INT16    majoritySize() ;

         UINT32   setTimer( UINT32 milliSec ) ;
         void     killTimer( UINT32 timerID ) ;

         BOOLEAN  delayCurOperation() ;
         BOOLEAN  isDelayed() const { return _catMainCtrl.isDelayed() ; }

         _netRouteAgent* netWork()
         {
            return _pNetWork;
         }
         catMainController* getMainController()
         {
            return &_catMainCtrl ;
         }
         catCatalogueManager* getCatlogueMgr()
         {
            return &_catlogueMgr ;
         }
         catNodeManager* getCatNodeMgr()
         {
            return &_catNodeMgr ;
         }
         catLevelLockMgr* getLevelLockMgr()
         {
            return &_levelLockMgr ;
         }

      private:
         _netRouteAgent       *_pNetWork ;
         _MsgRouteID          _routeID ;
         std::string          _strHostName ;
         std::string          _strCatServiceName ;
         NODE_ID_MAP          _nodeIdMap ;
         NODE_ID_MAP          _cataNodeIdMap ;
         GRP_ID_MAP           _grpIdMap ;
         GRP_ID_MAP           _deactiveGrpIdMap ;
         UINT16               _iCurNodeId ;
         UINT16               _curCataNodeId ;
         ossSpinSLatch        _GrpIDMutex ;
         UINT32               _iCurGrpId ;

         catMainController    _catMainCtrl ;
         catCatalogueManager  _catlogueMgr ;
         catNodeManager       _catNodeMgr ;
         catLevelLockMgr      _levelLockMgr ;
   };

   /*
      get global catalogue cb
   */
   sdbCatalogueCB* sdbGetCatalogueCB() ;

}

#endif // CATALOGUECB_HPP_

