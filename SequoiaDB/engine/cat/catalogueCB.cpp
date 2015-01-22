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

   Source File Name = catalogueCB.cpp

   Descriptive Name = Catalog Control Block

   When/how to use: this program may be used in catalog component for control
   block initialization and common functions.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "catalogueCB.hpp"
#include "msgCatalog.hpp"
#include "clsMgr.hpp"
#include "ossUtil.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "catTrace.hpp"
#include "pmd.hpp"
#include <stdlib.h>

namespace engine
{

   #define CAT_WAIT_EDU_ATTACH_TIMEOUT       ( 60 * OSS_ONE_SEC )

   sdbCatalogueCB::sdbCatalogueCB()
   {
      _routeID.value       = 0;
      _strHostName         = "";
      _strCatServiceName   = "";
      _pNetWork            = NULL;
      _iCurNodeId          = CAT_DATA_NODE_ID_BEGIN;
      _iCurGrpId           = CAT_DATA_GROUP_ID_BEGIN;
      _curCataNodeId       = CATA_NODE_ID_BEGIN;
   }

   sdbCatalogueCB::~sdbCatalogueCB()
   {
   }

   INT16 sdbCatalogueCB::majoritySize()
   {
      return (INT16)( sdbGetReplCB()->groupSize() / 2 + 1 ) ;
   }

   UINT32 sdbCatalogueCB::setTimer( UINT32 milliSec )
   {
      UINT32 id = NET_INVALID_TIMER_ID ;
      if ( _pNetWork )
      {
         _pNetWork->addTimer( milliSec, &_catMainCtrl, id ) ;
      }
      return id ;
   }

   void sdbCatalogueCB::killTimer( UINT32 timerID )
   {
      if ( _pNetWork )
      {
         _pNetWork->removeTimer( timerID ) ;
      }
   }

   BOOLEAN sdbCatalogueCB::delayCurOperation()
   {
      return _catMainCtrl.delayCurOperation() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_INIT, "sdbCatalogueCB::init" )
   INT32 sdbCatalogueCB::init()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGCB_INIT ) ;

      _routeID.columns.serviceID = MSG_ROUTE_CAT_SERVICE ;
      _strHostName = pmdGetKRCB()->getHostName() ;
      _strCatServiceName = pmdGetOptionCB()->catService() ;

      IControlBlock *pClsCB = pmdGetKRCB()->getCBByType( SDB_CB_CLS ) ;
      IEventHolder *pHolder = NULL ;
      if ( pClsCB && pClsCB->queryInterface( SDB_IF_EVT_HOLDER ) )
      {
         pHolder = (IEventHolder*)pClsCB->queryInterface( SDB_IF_EVT_HOLDER ) ;
         pHolder->regEventHandler( this ) ;
      }

      _pNetWork = SDB_OSS_NEW _netRouteAgent( &_catMainCtrl ) ;
      if ( !_pNetWork )
      {
         PD_LOG ( PDERROR, "Failed to allocate memory for netRouteAgent" ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = _catMainCtrl.init() ;
      PD_RC_CHECK( rc, PDERROR, "Init main controller failed, rc: %d", rc ) ;

      rc = _catlogueMgr.init() ;
      PD_RC_CHECK( rc, PDERROR, "Init catlogue manager failed, rc: %d", rc ) ;

      rc = _catNodeMgr.init() ;
      PD_RC_CHECK( rc, PDERROR, "Init cat node manager failed, rc: %d", rc ) ;

      PD_TRACE1 ( SDB_CATALOGCB_INIT,
                  PD_PACK_ULONG ( _routeID.value ) ) ;
      _pNetWork->setLocalID( _routeID );
      rc = _pNetWork->updateRoute( _routeID,
                                   _strHostName.c_str(),
                                   _strCatServiceName.c_str() );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to update route(routeID=%lld, host=%s, "
                  "service=%s, rc=%d)", _routeID.value, _strHostName.c_str(),
                  _strCatServiceName.c_str(), rc);
         goto error ;
      }
      rc = _pNetWork->listen( _routeID );
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to open listen-port(host=%s, service=%s, "
                  "rc=%d)", _strHostName.c_str(), _strCatServiceName.c_str(),
                  rc );
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATALOGCB_INIT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 sdbCatalogueCB::active ()
   {
      INT32 rc = SDB_OK ;
      pmdEDUMgr *pEDUMgr = pmdGetKRCB()->getEDUMgr() ;
      EDUID eduID = PMD_INVALID_EDUID ;

      _catMainCtrl.getAttachEvent()->reset() ;
      rc = pEDUMgr->startEDU ( EDU_TYPE_CATMGR,
                               (_pmdObjBase*)getMainController(),
                               &eduID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to start cat main controller edu, "
                   "rc: %d", rc ) ;
      pEDUMgr->regSystemEDU( EDU_TYPE_CATMGR, eduID ) ;
      rc = _catMainCtrl.getAttachEvent()->wait( CAT_WAIT_EDU_ATTACH_TIMEOUT ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to wait cat manager edu "
                   "attach, rc: %d", rc ) ;

      pEDUMgr->startEDU ( EDU_TYPE_CATNETWORK, (netRouteAgent*)netWork(),
                          &eduID ) ;
      pEDUMgr->regSystemEDU ( EDU_TYPE_CATNETWORK, eduID ) ;
      rc = pEDUMgr->waitUntil( EDU_TYPE_CATNETWORK, PMD_EDU_RUNNING ) ;
      PD_RC_CHECK( rc, PDERROR, "Wait CATNET active failed, rc: %d", rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 sdbCatalogueCB::deactive ()
   {
      if ( _pNetWork )
      {
         _pNetWork->closeListen() ;
      }

      if ( _pNetWork )
      {
         _pNetWork->stop() ;
      }

      return SDB_OK ;
   }

   INT32 sdbCatalogueCB::fini ()
   {
      IControlBlock *pClsCB = pmdGetKRCB()->getCBByType( SDB_CB_CLS ) ;
      IEventHolder *pHolder = NULL ;
      if ( pClsCB && pClsCB->queryInterface( SDB_IF_EVT_HOLDER ) )
      {
         pHolder = (IEventHolder*)pClsCB->queryInterface( SDB_IF_EVT_HOLDER ) ;
         pHolder->unregEventHandler( this ) ;
      }

      if ( _pNetWork != NULL )
      {
         SDB_OSS_DEL _pNetWork;
         _pNetWork = NULL;
      }
      return SDB_OK ;
   }

   void sdbCatalogueCB::onConfigChange ()
   {
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_INSERTGROUPID, "sdbCatalogueCB::insertGroupID" )
   void sdbCatalogueCB::insertGroupID( UINT32 grpID, BOOLEAN isActive )
   {
      PD_TRACE_ENTRY ( SDB_CATALOGCB_INSERTGROUPID ) ;
      PD_TRACE2 ( SDB_CATALOGCB_INSERTGROUPID,
                  PD_PACK_UINT ( grpID ),
                  PD_PACK_UINT ( isActive ) ) ;
      if ( grpID >= CAT_DATA_GROUP_ID_BEGIN )
      {
         ossScopedLock _lock(&_GrpIDMutex, EXCLUSIVE) ;
         if ( isActive )
         {
            _grpIdMap.insert( GRP_ID_MAP::value_type(grpID, grpID) );
         }
         else
         {
            _deactiveGrpIdMap.insert( GRP_ID_MAP::value_type(grpID, grpID) );
         }
         _iCurGrpId = _iCurGrpId > grpID ? _iCurGrpId : ++grpID ;
      }
      PD_TRACE_EXIT ( SDB_CATALOGCB_INSERTGROUPID ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_REMOVEGROUPID, "sdbCatalogueCB::removeGroupID" )
   void sdbCatalogueCB::removeGroupID( UINT32 grpID )
   {
      PD_TRACE_ENTRY ( SDB_CATALOGCB_REMOVEGROUPID ) ;
      PD_TRACE1 ( SDB_CATALOGCB_REMOVEGROUPID,
                  PD_PACK_UINT ( grpID ) ) ;
      if ( grpID >= CAT_DATA_GROUP_ID_BEGIN )
      {
         ossScopedLock _lock(&_GrpIDMutex, EXCLUSIVE) ;
         _grpIdMap.erase(grpID);
         _deactiveGrpIdMap.erase( grpID );
      }
      PD_TRACE_EXIT ( SDB_CATALOGCB_REMOVEGROUPID ) ;
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_ACTIVEGROUP, "sdbCatalogueCB::activeGroup" )
   void sdbCatalogueCB::activeGroup( UINT32 groupID )
   {
      PD_TRACE_ENTRY ( SDB_CATALOGCB_ACTIVEGROUP ) ;
      ossScopedLock _lock(&_GrpIDMutex, EXCLUSIVE) ;
      _deactiveGrpIdMap.erase( groupID );
      _grpIdMap.insert( GRP_ID_MAP::value_type(groupID, groupID) );
      PD_TRACE_EXIT ( SDB_CATALOGCB_ACTIVEGROUP ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_INSERTNODEID, "sdbCatalogueCB::insertNodeID" )
   void sdbCatalogueCB::insertNodeID( UINT16 nodeID )
   {
      PD_TRACE_ENTRY ( SDB_CATALOGCB_INSERTNODEID ) ;
      PD_TRACE1 ( SDB_CATALOGCB_INSERTNODEID, PD_PACK_USHORT ( nodeID ) ) ;
      if ( nodeID >= CAT_DATA_NODE_ID_BEGIN )
      {
         _nodeIdMap.insert( NODE_ID_MAP::value_type(nodeID, nodeID) );
         _iCurNodeId = _iCurNodeId > nodeID ? _iCurNodeId : ++nodeID ;
      }
      else
      {
         _cataNodeIdMap.insert( NODE_ID_MAP::value_type(nodeID, nodeID) );
         _curCataNodeId = _curCataNodeId > nodeID ? _curCataNodeId : ++nodeID ;
      }
      PD_TRACE_EXIT ( SDB_CATALOGCB_INSERTNODEID ) ;
   }

   void sdbCatalogueCB::insertCataNodeID( UINT16 nodeID )
   {
      _cataNodeIdMap.insert( NODE_ID_MAP::value_type(nodeID, nodeID) );
      _curCataNodeId = _curCataNodeId > nodeID ? _curCataNodeId : ++nodeID ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_GETAGROUPRAND, "sdbCatalogueCB::getAGroupRand" )
   INT32 sdbCatalogueCB::getAGroupRand( INT32 &groupID )
   {
      INT32 rc = SDB_CAT_NO_NODEGROUP_INFO ;
      PD_TRACE_ENTRY ( SDB_CATALOGCB_GETAGROUPRAND ) ;
      ossScopedLock _lock( &_GrpIDMutex, EXCLUSIVE ) ;
      UINT32 mapSize = _grpIdMap.size();
      PD_TRACE1 ( SDB_CATALOGCB_GETAGROUPRAND,
                  PD_PACK_UINT ( mapSize ) ) ;
      if ( mapSize > 0 )
      {
         UINT32 randNum = ossRand() % mapSize;
         UINT32 i = 0;
         GRP_ID_MAP::iterator iterMap = _grpIdMap.begin();
         for ( ; i < randNum && iterMap!=_grpIdMap.end(); i++ )
         {
            ++iterMap;
         }
         if ( iterMap != _grpIdMap.end() )
         {
            groupID = iterMap->first;
            rc = SDB_OK ;
         }
      }
      PD_TRACE_EXITRC ( SDB_CATALOGCB_GETAGROUPRAND, rc ) ;
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_ALLOCGROUPID, "sdbCatalogueCB::AllocGroupID" )
   UINT32 sdbCatalogueCB::AllocGroupID ()
   {
      INT32 i = 0;
      UINT32 id = 0 ;
      PD_TRACE_ENTRY ( SDB_CATALOGCB_ALLOCGROUPID ) ;
      ossScopedLock _lock(&_GrpIDMutex, EXCLUSIVE) ;
      while ( i++ < CAT_DATA_NODE_MAX_NUM )
      {
         if ( _iCurGrpId < CAT_DATA_GROUP_ID_BEGIN )
         {
            _iCurGrpId = CAT_DATA_GROUP_ID_BEGIN;
         }
         GRP_ID_MAP::const_iterator it = _grpIdMap.find( _iCurGrpId );
         if ( it != _grpIdMap.end() )
         {
            _iCurGrpId++;
            continue;
         }
         it = _deactiveGrpIdMap.find( _iCurGrpId );
         if ( it != _deactiveGrpIdMap.end() )
         {
            _iCurGrpId++;
            continue;
         }
         id = _iCurGrpId ;
         goto done ;
      }
      id = CAT_INVALID_GROUPID ;
   done :
      PD_TRACE1 ( SDB_CATALOGCB_ALLOCGROUPID, PD_PACK_UINT ( id ) ) ;
      PD_TRACE_EXIT ( SDB_CATALOGCB_ALLOCGROUPID ) ;
      return id ;
   }

   void sdbCatalogueCB::releaseNodeID( UINT16 nodeID )
   {
      _nodeIdMap.erase( nodeID );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_ALLOCCATANODEID, "sdbCatalogueCB::AllocCataNodeID" )
   UINT16 sdbCatalogueCB::AllocCataNodeID()
   {
      INT32 i = 0 ;
      UINT16 id = 0 ;
      PD_TRACE_ENTRY ( SDB_CATALOGCB_ALLOCCATANODEID ) ;
      while ( i++ < CATA_NODE_MAX_NUM )
      {
         if ( _curCataNodeId >= CAT_DATA_NODE_ID_BEGIN )
         {
            _curCataNodeId = CATA_NODE_ID_BEGIN;
         }
         NODE_ID_MAP::const_iterator it
                           = _cataNodeIdMap.find( _curCataNodeId );
         if ( _cataNodeIdMap.end() == it )
         {
            id = _curCataNodeId ;
            insertCataNodeID( _curCataNodeId );
            goto done ;
         }
         _curCataNodeId++;
      }
      id = CAT_INVALID_NODEID ;
   done :
      PD_TRACE1 ( SDB_CATALOGCB_ALLOCCATANODEID, PD_PACK_USHORT ( id ) ) ;
      PD_TRACE_EXIT ( SDB_CATALOGCB_ALLOCCATANODEID ) ;
      return id ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_ALLOCNODEID, "sdbCatalogueCB::AllocNodeID" )
   UINT16 sdbCatalogueCB::AllocNodeID()
   {
      INT32 i = 0;
      UINT16 id = 0 ;
      PD_TRACE_ENTRY ( SDB_CATALOGCB_ALLOCNODEID ) ;
      while ( i++ < CAT_DATA_NODE_MAX_NUM )
      {
         if ( _iCurNodeId < CAT_DATA_NODE_ID_BEGIN )
         {
            _iCurNodeId = CAT_DATA_NODE_ID_BEGIN;
         }
         NODE_ID_MAP::const_iterator it = _nodeIdMap.find( _iCurNodeId );
         if ( _nodeIdMap.end() == it )
         {
            id = _iCurNodeId ;
            insertNodeID( _iCurNodeId );
            goto done ;
         }
         _iCurNodeId++;
      }
      id = CAT_INVALID_NODEID ;
   done :
      PD_TRACE1 ( SDB_CATALOGCB_ALLOCNODEID, PD_PACK_USHORT ( id ) ) ;
      PD_TRACE_EXIT ( SDB_CATALOGCB_ALLOCNODEID ) ;
      return id;
   }

   UINT32 sdbCatalogueCB::getMask() const
   {
      return EVENT_MASK_ON_REGISTERED | EVENT_MASK_ON_PRIMARYCHG ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATALOGCB_UPDATEROUTEID, "sdbCatalogueCB::updateRouteID" )
   void sdbCatalogueCB::onRegistered ( const MsgRouteID &nodeID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_CATALOGCB_UPDATEROUTEID ) ;
      MsgRouteID id ;
      id.value = nodeID.value ;
      id.columns.serviceID = MSG_ROUTE_CAT_SERVICE ;
      PD_TRACE1 ( SDB_CATALOGCB_UPDATEROUTEID,
                  PD_PACK_ULONG ( id.value ) ) ;

      rc = _pNetWork->updateRoute( _routeID, id ) ;
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "Failed to update route(old=%d,new=%d host=%s, "
                  "service=%s, rc=%d)", _routeID.columns.nodeID,
                  id.columns.nodeID, _strHostName.c_str(),
                  _strCatServiceName.c_str(), rc );
      }
      _pNetWork->setLocalID( id ) ;
      _routeID.value = id.value ;
      PD_TRACE_EXIT ( SDB_CATALOGCB_UPDATEROUTEID ) ;
   }

   void sdbCatalogueCB::onPrimaryChange( BOOLEAN primary,
                                         SDB_EVENT_OCCUR_TYPE occurType )
   {
      pmdEDUMgr *pEDUMgr = pmdGetKRCB()->getEDUMgr() ;
      pmdEDUEventTypes eventType = primary ? PMD_EDU_EVENT_ACTIVE :
                                             PMD_EDU_EVENT_DEACTIVE ;

      if ( SDB_EVT_OCCUR_AFTER == occurType )
      {
         EDUID eduID = pEDUMgr->getSystemEDU( EDU_TYPE_CATMGR ) ;
         if ( PMD_INVALID_EDUID != eduID )
         {
            _catMainCtrl.getChangeEvent()->reset() ;
            if ( SDB_OK != pEDUMgr->postEDUPost( eduID, eventType ) )
            {
               _catMainCtrl.getChangeEvent()->signal() ;
            }
            _catMainCtrl.getChangeEvent()->wait( OSS_ONE_SEC * 120 ) ;
         }
      }
   }

   /*
      get global catalogue cb
   */
   sdbCatalogueCB* sdbGetCatalogueCB()
   {
      static sdbCatalogueCB s_catacb ;
      return &s_catacb ;
   }

}
