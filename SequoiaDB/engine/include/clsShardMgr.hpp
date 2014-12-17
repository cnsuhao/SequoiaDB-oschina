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

   Source File Name = clsShardMgr.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          22/11/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef CLS_SHD_MGR_HPP_
#define CLS_SHD_MGR_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "pmdObjBase.hpp"
#include "netRouteAgent.hpp"
#include "ossEvent.hpp"
#include "ossLatch.hpp"
#include "clsCatalogAgent.hpp"
#include "sdbInterface.hpp"
#include <map>

using namespace bson ;

namespace engine
{
   #define CLS_SHARD_TIMEOUT     (5*OSS_ONE_SEC)

   /*
      _catlogServerInfo define
   */
   struct _catlogServerInfo : public SDBObject
   {
      NodeID      nodeID ;
      std::string host ;
      std::string service ;
   } ;
   typedef std::vector<_catlogServerInfo>             VECCATLOG ;

   /*
      _clsEventItem define
   */
   class _clsEventItem : public SDBObject
   {
   public :
      BOOLEAN        send ;
      UINT32         waitNum ;
      ossEvent       event ;
      UINT64         requestID ;
      std::string    name ;
      UINT32         groupID ;
      INT32          sendNums ;

      _clsEventItem ()
      {
         send = FALSE ;
         waitNum = 0 ;
         requestID = 0 ;
         groupID = 0 ;
         sendNums = 0 ;
      }
   } ;
   typedef class _clsEventItem clsEventItem ;

   /*
      _clsCSEventItem define
   */
   class _clsCSEventItem : public SDBObject
   {
      public:
         std::string    csName ;
         ossEvent       event ;
         UINT32         pageSize ;
         UINT32         lobPageSize ;
         INT32          sendNums ;

         _clsCSEventItem()
         {
            pageSize = 0 ;
            sendNums = 0 ;
            lobPageSize = 0 ;
         }
   } ;
   typedef _clsCSEventItem clsCSEventItem ;

   /*
      _clsShardMgr define
   */
   class _clsShardMgr :  public _pmdObjBase
   {
      typedef std::map<std::string, clsEventItem*>       MAP_CAT_EVENT ;
      typedef MAP_CAT_EVENT::iterator                    MAP_CAT_EVENT_IT ;

      typedef std::map<UINT32, clsEventItem*>            MAP_NM_EVENT ;
      typedef MAP_NM_EVENT::iterator                     MAP_NM_EVENT_IT ;

      typedef std::map<UINT64, clsCSEventItem*>          MAP_CS_EVENT ;
      typedef MAP_CS_EVENT::iterator                     MAP_CS_EVENT_IT ;

      DECLARE_OBJ_MSG_MAP()

      public:
         _clsShardMgr( _netRouteAgent *rtAgent );
         virtual ~_clsShardMgr();

         INT32    initialize() ;
         INT32    active () ;
         INT32    deactive () ;
         INT32    final() ;
         void     onConfigChange() ;
         void     ntyPrimaryChange( BOOLEAN primary,
                                    SDB_EVENT_OCCUR_TYPE type ) ;

         virtual void   attachCB( _pmdEDUCB *cb ) ;
         virtual void   detachCB( _pmdEDUCB *cb ) ;

         virtual void     onTimer ( UINT32 timerID, UINT32 interval ) ;

         void setCatlogInfo ( const NodeID &id, const std::string& host,
                              const std::string& service ) ;
         void setNodeID ( const MsgRouteID& nodeID ) ;

         catAgent* getCataAgent () ;
         nodeMgrAgent* getNodeMgrAgent () ;

         INT32 getAndLockCataSet( const CHAR *name, clsCatalogSet **ppSet,
                                  BOOLEAN noWithUpdate = TRUE,
                                  INT64 waitMillSec = CLS_SHARD_TIMEOUT,
                                  BOOLEAN *pUpdated = NULL ) ;
         INT32 unlockCataSet( clsCatalogSet *catSet ) ;

         INT32 getAndLockGroupItem( UINT32 id, clsGroupItem **ppItem,
                                     BOOLEAN noWithUpdate = TRUE,
                                     INT64 waitMillSec = CLS_SHARD_TIMEOUT,
                                     BOOLEAN *pUpdated = NULL ) ;
         INT32 unlockGroupItem( clsGroupItem *item ) ;

         INT32 rGetCSPageSize( const CHAR *csName, UINT32 &pageSize,
                               UINT32 &lobPageSize,
                               INT64 waitMillSec = CLS_SHARD_TIMEOUT ) ;

      public:
         INT32  sendToCatlog ( MsgHeader * msg, INT32 *pSendNum = NULL ) ;
         INT32  syncSend( MsgHeader * msg, UINT32 groupID, BOOLEAN primary,
                          MsgHeader **ppRecvMsg,
                          INT64 millisec = CLS_SHARD_TIMEOUT ) ;
         INT32  updatePrimary ( const NodeID & id , BOOLEAN primary ) ;
         INT32  updateCatGroup ( BOOLEAN unsetPrimary = TRUE,
                                 INT64 millsec = 0 ) ;

         INT32 syncUpdateCatalog ( const CHAR *pCollectionName,
                                   INT64 millsec = CLS_SHARD_TIMEOUT ) ;
         INT32 syncUpdateGroupInfo ( UINT32 groupID,
                                     INT64 millsec = CLS_SHARD_TIMEOUT ) ;
         NodeID nodeID () const ;
         INT32 clearAllData () ;

         INT64 netIn() ;
         INT64 netOut() ;
         void resetMon() ;

      protected:

         INT32 _sendCataQueryReq( INT32 queryType, const BSONObj &query,
                                  UINT64 requestID, INT32 *pSendNum = NULL ) ;

         INT32 _sendCatalogReq ( const CHAR *pCollectionName,
                                 UINT64 requestID = 0,
                                 INT32 *pSendNum = NULL ) ;
         INT32 _sendGroupReq ( UINT32 groupID, UINT64 requestID = 0,
                               INT32 *pSendNum = NULL ) ;
         INT32 _sendCSInfoReq ( const CHAR *pCSName, UINT64 requestID = 0,
                                INT32 *pSendNum = NULL ) ;

         clsEventItem *_findCatSyncEvent ( const CHAR *pCollectionName,
                                           BOOLEAN bCreate = FALSE ) ;
         clsEventItem *_findCatSyncEvent ( UINT64 requestID ) ;

         clsEventItem *_findNMSyncEvent ( UINT32 groupID,
                                          BOOLEAN bCreate = FALSE ) ;
         clsEventItem *_findNMSyncEvent ( UINT64 requestID ) ;

         INT32 _findCatNodeID ( const VECCATLOG &catNodes,
                                const std::string &host,
                                const std::string &service,
                                NodeID &id ) ;

      protected:
         INT32 _onCatCatGroupRes ( NET_HANDLE handle, MsgHeader* msg ) ;
         INT32 _onCatalogReqMsg ( NET_HANDLE handle, MsgHeader* msg ) ;
         INT32 _onCatGroupRes ( NET_HANDLE handle, MsgHeader* msg ) ;
         INT32 _onQueryCSInfoRsp( NET_HANDLE handle, MsgHeader* msg ) ;

      private:
         _netRouteAgent                *_pNetRtAgent ;
         _clsCatalogAgent              *_pCatAgent ;
         _clsNodeMgrAgent              *_pNodeMgrAgent ;
         ossSpinXLatch                 _catLatch ;
         MAP_CAT_EVENT                 _mapSyncCatEvent ;
         MAP_NM_EVENT                  _mapSyncNMEvent ;
         MAP_CS_EVENT                  _mapSyncCSEvent ;
         UINT64                        _requestID ;

         VECCATLOG                     _vecCatlog ;
         INT32                         _primary ;
         UINT32                        _catVerion ;
         ossEvent                      _upCatEvent ;
         ossSpinSLatch                 _shardLatch ;

         MsgRouteID                    _nodeID ;

   } ;

   typedef _clsShardMgr shardCB ;
}

#endif //CLS_SHD_MGR_HPP_

