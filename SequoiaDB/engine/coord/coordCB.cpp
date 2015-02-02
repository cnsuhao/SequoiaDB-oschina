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

   Source File Name = coordCB.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "coordCB.hpp"
#include "pmd.hpp"
#include "ossTypes.h"
#include "pdTrace.hpp"
#include "coordTrace.hpp"
#include "coordDef.hpp"
#include "pmdStartup.hpp"

using namespace bson;
namespace engine
{

   /*
   note: _CoordGroupInfo implement
   */
   _CoordGroupInfo::_CoordGroupInfo ( UINT32 groupID )
   :_groupItem( groupID )
   {
   }

   _CoordGroupInfo::~_CoordGroupInfo ()
   {
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_COORDGI_FRMBSONOBJ, "CoordGroupInfo::fromBSONObj" )
   INT32 _CoordGroupInfo::fromBSONObj( const bson::BSONObj &boGroupInfo )
   {
      PD_TRACE_ENTRY ( SDB_COORDGI_FRMBSONOBJ ) ;

      INT32 rc = _groupItem.updateGroupItem( boGroupInfo ) ;
      PD_RC_CHECK( rc, PDERROR, "Update group info failed, rc: %d", rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_COORDGI_FRMBSONOBJ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   void _CoordGroupInfo::setPrimary( const MsgRouteID &ID )
   {
      ossScopedRWLock lock( &_primaryMutex, EXCLUSIVE ) ;
      _groupItem.updatePrimary( ID, TRUE ) ;
   }

   void _CoordGroupInfo::setSlave( const MsgRouteID & ID )
   {
      ossScopedRWLock lock( &_primaryMutex, EXCLUSIVE ) ;
      _groupItem.updatePrimary( ID, FALSE ) ;
   }

   INT32 CoordCataInfo::getMatchGroups( const bson::BSONObj &matcher,
                                       CoordGroupList &groupLst )
   {
      INT32 rc = SDB_OK ;
      UINT32 i = 0 ;
      UINT32 groupID = 0 ;
      VEC_GROUP_ID vecGroup;
      rc = _catlogSet.findGroupIDS( matcher, vecGroup );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get match groups(rc=%d)",
                  rc );
      SDB_ASSERT( vecGroup.size() != 0, "no match groups!" ) ;
      PD_CHECK( vecGroup.size() != 0, SDB_CAT_NO_MATCH_CATALOG, error,
               PDERROR, "couldn't find match group" );
      for ( i = 0; i < vecGroup.size(); i++ )
      {
         groupID = vecGroup[i] ;
         groupLst[ groupID ] = groupID ;
      }
   done:
      return rc;
   error:
      goto done;
   }

   /*
   note: _CoordCB implement
   */
   _CoordCB::_CoordCB()
   {
      _pNetWork = NULL ;
   }

   _CoordCB::~_CoordCB()
   {
   }

   INT32 _CoordCB::init ()
   {
      INT32 rc = SDB_OK ;
      CoordGroupInfo *pGroupInfo = NULL ;
      UINT32 catGID = CATALOG_GROUPID ;
      UINT16 catNID = CATA_NODE_ID_BEGIN + CLS_REPLSET_MAX_NODE_SIZE ;
      MsgRouteID id ;
      pmdOptionsCB *optCB = pmdGetOptionCB() ;
      vector< _pmdOptionsMgr::_pmdAddrPair > catAddrs = optCB->catAddrs() ;

      _pNetWork = SDB_OSS_NEW _netRouteAgent( &_multiRouteAgent ) ;
      if ( !_pNetWork )
      {
         PD_LOG( PDERROR, "Failed to alloc memory for net agent" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      _multiRouteAgent.setNetWork( _pNetWork ) ;

      pGroupInfo = SDB_OSS_NEW CoordGroupInfo( CAT_CATALOG_GROUPID ) ;
      if ( !pGroupInfo )
      {
         PD_LOG( PDERROR, "Failed to alloc memory for group info" ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      _catGroupInfo = CoordGroupInfoPtr( pGroupInfo ) ;

      for ( UINT32 i = 0 ; i < catAddrs.size() ; ++i )
      {
         if ( 0 == catAddrs[i]._host[ 0 ] )
         {
            break ;
         }
         id.columns.groupID = catGID ;
         id.columns.nodeID = catNID++ ;
         id.columns.serviceID = MSG_ROUTE_CAT_SERVICE ;
         addCatNodeAddr( id, catAddrs[i]._host, catAddrs[i]._service ) ;
      }

      pmdGetStartup().ok( TRUE ) ;
      pmdGetKRCB()->setGroupName( COORD_GROUPNAME ) ;
      {
         MsgRouteID id ;
         id.value = MSG_INVALID_ROUTEID ;
         id.columns.groupID = COORD_GROUPID ;
         pmdSetNodeID( id ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _CoordCB::active ()
   {
      INT32 rc = SDB_OK ;
      pmdEDUMgr* pEDUMgr = pmdGetKRCB()->getEDUMgr() ;
      EDUID eduID = PMD_INVALID_EDUID ;

      pmdSetPrimary( TRUE ) ;

      rc = pEDUMgr->startEDU ( EDU_TYPE_COORDNETWORK, (void*)netWork(),
                               &eduID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to start coord network edu, rc: %d",
                   rc ) ;
      pEDUMgr->regSystemEDU ( EDU_TYPE_COORDNETWORK, eduID ) ;
      rc = pEDUMgr->waitUntil( eduID , PMD_EDU_RUNNING ) ;
      PD_RC_CHECK( rc, PDERROR, "Wait CoordNet active failed, rc: %d", rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _CoordCB::deactive ()
   {
      if ( _pNetWork )
      {
         _pNetWork->stop() ;
      }
      return SDB_OK ;
   }

   INT32 _CoordCB::fini ()
   {
      if ( _pNetWork )
      {
         SDB_OSS_DEL _pNetWork ;
         _pNetWork = NULL ;
      }
      return SDB_OK ;
   }

   void _CoordCB::onConfigChange ()
   {
   }

   void _CoordCB::updateCatGroupInfo( CoordGroupInfoPtr &groupInfo )
   {
      ossScopedLock _lock(&_mutex, EXCLUSIVE) ;
      if ( _catGroupInfo->getGroupItem()->groupVersion() !=
           groupInfo->getGroupItem()->groupVersion() )
      {
         string oldCfg, newCfg ;
         pmdOptionsCB *optCB = pmdGetOptionCB() ;
         optCB->toString( oldCfg ) ;
         optCB->clearCatAddr() ;
         clearCatNodeAddrList() ;

         UINT32 pos = 0 ;
         MsgRouteID id ;
         string hostName ;
         string svcName ;
         while ( SDB_OK == groupInfo->getGroupItem()->getNodeInfo( pos, id,
                           hostName, svcName, MSG_ROUTE_CAT_SERVICE ) )
         {
            addCatNodeAddr( id, hostName.c_str(), svcName.c_str() ) ;
            optCB->setCatAddr( hostName.c_str(), svcName.c_str() ) ;
            ++pos ;
         }
         optCB->toString( newCfg ) ;
         if ( oldCfg != newCfg )
         {
            optCB->reflush2File() ;
         }
      }
      _catGroupInfo = groupInfo ;
   }

   void _CoordCB::getCatNodeAddrList ( CoordVecNodeInfo &catNodeLst )
   {
      catNodeLst = _cataNodeAddrList;
   }

   void _CoordCB::clearCatNodeAddrList()
   {
      _cataNodeAddrList.clear();
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_COORDCB_ADDCATNDADDR, "CoordCB::addCatNodeAddr" )
   INT32 _CoordCB::addCatNodeAddr( const _MsgRouteID &id,
                                   const CHAR *pHost,
                                   const CHAR *pService )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_COORDCB_ADDCATNDADDR ) ;
      CoordNodeInfo nodeInfo ;
      nodeInfo._id = id ;
      nodeInfo._id.columns.groupID = 0 ;
      ossStrncpy( nodeInfo._host, pHost, OSS_MAX_HOSTNAME );
      nodeInfo._host[OSS_MAX_HOSTNAME] = 0 ;
      nodeInfo._service[MSG_ROUTE_CAT_SERVICE] = pService ;

      _cataNodeAddrList.push_back( nodeInfo ) ;
      _multiRouteAgent.updateRoute( nodeInfo._id,
                                    nodeInfo._host,
                                    nodeInfo._service[MSG_ROUTE_CAT_SERVICE].c_str() );
      PD_TRACE_EXIT ( SDB_COORDCB_ADDCATNDADDR );
      return rc ;
   }

   INT32 _CoordCB::_addGroupName ( const std::string& name, UINT32 id )
   {
      INT32 rc = SDB_OK ;
      GROUP_NAME_MAP_IT it = _groupNameMap.find ( name ) ;
      if ( it != _groupNameMap.end() )
      {
         if ( it->second == id )
         {
            rc = SDB_OK ;
            goto done ;
         }
      }
      _groupNameMap[name] = id ;

   done :
      return rc ;
   }

   INT32 _CoordCB::_clearGroupName( UINT32 id )
   {
      GROUP_NAME_MAP_IT it = _groupNameMap.begin() ;
      while ( it != _groupNameMap.end() )
      {
         if ( it->second == id )
         {
            _groupNameMap.erase( it ) ;
            break ;
         }
         ++it ;
      }
      return SDB_OK ;
   }

   INT32 _CoordCB::groupID2Name ( UINT32 id, std::string &name )
   {
      ossScopedLock _lock( &_nodeGroupMutex, SHARED ) ;

      CoordGroupMap::iterator it = _nodeGroupInfo.find( id ) ;
      if ( it == _nodeGroupInfo.end() )
      {
         return SDB_COOR_NO_NODEGROUP_INFO ;
      }
      name = it->second->groupName() ;

      return SDB_OK ;
   }

   INT32 _CoordCB::groupName2ID ( const CHAR* name, UINT32 &id )
   {
      ossScopedLock _lock( &_nodeGroupMutex, SHARED ) ;

      GROUP_NAME_MAP::iterator it = _groupNameMap.find( name ) ;
      if ( it == _groupNameMap.end() )
      {
         return SDB_COOR_NO_NODEGROUP_INFO ;
      }
      id = it->second ;

      return SDB_OK ;
   }

   void _CoordCB::addGroupInfo ( CoordGroupInfoPtr &groupInfo )
   {
      ossScopedLock _lock( &_nodeGroupMutex, EXCLUSIVE ) ;

      _nodeGroupInfo[groupInfo->getGroupID()] = groupInfo ;

      _clearGroupName( groupInfo->getGroupID() ) ;

      _addGroupName( groupInfo->groupName(), groupInfo->getGroupID() ) ;
   }

   void _CoordCB::removeGroupInfo( UINT32 groupID )
   {
      ossScopedLock _lock(&_nodeGroupMutex, EXCLUSIVE) ;
      _nodeGroupInfo.erase( groupID ) ;

      _clearGroupName( groupID ) ;
   }

   INT32 _CoordCB::getGroupInfo ( UINT32 groupID,
                                  CoordGroupInfoPtr &groupInfo )
   {
      INT32 rc = SDB_OK;
      ossScopedLock _lock( &_nodeGroupMutex, SHARED ) ;
      CoordGroupMap::iterator iter = _nodeGroupInfo.find ( groupID );
      if ( _nodeGroupInfo.end() == iter )
      {
         rc = SDB_COOR_NO_NODEGROUP_INFO;
      }
      else
      {
         groupInfo = iter->second;
      }
      return rc;
   }

   INT32 _CoordCB::getGroupInfo ( const CHAR *groupName,
                                  CoordGroupInfoPtr &groupInfo )
   {
      UINT32 groupID = 0 ;
      INT32 rc = groupName2ID( groupName, groupID ) ;
      if ( SDB_OK == rc )
      {
         rc = getGroupInfo( groupID, groupInfo ) ;
      }
      return rc ;
   }

   void _CoordCB::updateCataInfo ( const std::string &collectionName,
                                   CoordCataInfoPtr &cataInfo )
   {
      ossScopedLock _lock( &_cataInfoMutex, EXCLUSIVE );
      _cataInfoMap[collectionName] = cataInfo ;
   }

   INT32 _CoordCB::getCataInfo ( const std::string &strCollectionName,
                                 CoordCataInfoPtr &cataInfo )
   {
      INT32 rc = SDB_CAT_NO_MATCH_CATALOG;
      ossScopedLock _lock( &_cataInfoMutex, SHARED );
      CoordCataMap::iterator iter
                           = _cataInfoMap.find( strCollectionName );
      if ( iter != _cataInfoMap.end() )
      {
         rc = SDB_OK;
         cataInfo = iter->second;
      }
      return rc;
   }

   void _CoordCB::delCataInfo ( const std::string &collectionName )
   {
      ossScopedLock _lock( &_cataInfoMutex, EXCLUSIVE );
      _cataInfoMap.erase( collectionName );
   }

   void _CoordCB::invalidateCataInfo()
   {
      ossScopedLock _lock( &_cataInfoMutex, EXCLUSIVE );
      _cataInfoMap.clear() ;
   }

   void _CoordCB::invalidateGroupInfo()
   {
      ossScopedLock _lock(&_nodeGroupMutex, EXCLUSIVE) ;
      _nodeGroupInfo.clear() ;
      _groupNameMap.clear() ;
   }

   /*
      get global coord cb
   */
   CoordCB* sdbGetCoordCB ()
   {
      static CoordCB s_coordCB ;
      return &s_coordCB ;
   }

}

