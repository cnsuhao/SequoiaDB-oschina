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

   Source File Name = clsReplicateSet.hpp

   Descriptive Name = Replication Control Block Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Replication component. This file contains structure for
   replication control block.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef CLSREPLICATESET_HPP_
#define CLSREPLICATESET_HPP_

#include "netRouteAgent.hpp"
#include "msgReplicator.hpp"
#include "msgCatalog.hpp"
#include "clsVoteMachine.hpp"
#include "msg.hpp"
#include "pmdObjBase.hpp"
#include "clsCatalogCaller.hpp"
#include "clsSyncManager.hpp"
#include "dms.hpp"
#include "clsReplBucket.hpp"
#include "dpsDef.hpp"
#include "ossQueue.hpp"
#include <vector>

using namespace std ;

namespace engine
{
   class _netRouteAgent ;
   class _clsMgr ;
   class _clsDataSrcBaseSession ;
   class _pmdEDUCB ;

   #define CLS_SYNCCTRL_THRESHOLD_SIZE          (10)

   /*
      _clsReplicateSet define
   */
   class _clsReplicateSet : public _pmdObjBase, public _dpsEventHandler
   {
      DECLARE_OBJ_MSG_MAP()

      public:
         _clsReplicateSet( _netRouteAgent *agent ) ;
         virtual ~_clsReplicateSet() ;

      public:
         OSS_INLINE BOOLEAN primaryIsMe()
         {
            return _vote.primaryIsMe() ;
         }

         OSS_INLINE BOOLEAN isFullSync ()
         {
            return CLS_BS_FULLSYNC == _replStatus ? TRUE : FALSE ;
         }

         OSS_INLINE CLS_BS_STATUS getStatus () const
         {
            return _replStatus ;
         }

         OSS_INLINE clsBucket* getBucket ()
         {
            return &_replBucket ;
         }

         OSS_INLINE void setLocalID( const MsgRouteID &id )
         {
            _info.local = id ;
         }

         OSS_INLINE void setFullSync( BOOLEAN fullSync )
         {
            if ( fullSync )
            {
               _replStatus = CLS_BS_FULLSYNC ;
            }
            else
            {
               _replStatus = CLS_BS_NORMAL ;
            }
         }

         OSS_INLINE void setStatus( CLS_BS_STATUS status )
         {
            _replStatus = status ;
         }

         OSS_INLINE const UINT32 ailves()
         {
            UINT32 num = 0 ;
            _info.mtx.lock_r() ;
            num = _info.aliveSize() ;
            _info.mtx.release_r() ;
            return num ;
         }

         OSS_INLINE UINT32 groupSize ()
         {
            UINT32 num = 0 ;
            _info.mtx.lock_r () ;
            num = _info.groupSize() ;
            _info.mtx.release_r  () ;
            return num ;
         }

         OSS_INLINE BOOLEAN isAlive ( NodeID node )
         {
            BOOLEAN bAlive = FALSE ;
            _info.mtx.lock_r() ;
            map<UINT64, _clsSharingStatus *>::iterator it =
               _info.alives.find ( node.value ) ;
            if ( it != _info.alives.end() )
            {
               bAlive = TRUE ;
            }
            _info.mtx.release_r() ;

            return bAlive ;
         }

         OSS_INLINE _clsSyncManager *syncMgr()
         {
            return &_sync ;
         }

         OSS_INLINE _clsVoteMachine* voteMachine()
         {
            return &_vote ;
         }

         OSS_INLINE INT32 sync( const DPS_LSN_OFFSET &offset,
                                _pmdEDUCB *eduCB,
                                UINT32 w = 1,
                                INT64 timeout = -1 )
         {
            if ( DPS_INVALID_LSN_OFFSET == offset || 1 >= w )
            {
               return SDB_OK ;
            }

            _clsSyncSession session ;
            session.endLsn = offset ;
            session.eduCB = eduCB ;
            eduCB->getEvent().reset() ;

            if ( w > CLS_REPLSET_MAX_NODE_SIZE )
            {
               w = CLS_REPLSET_MAX_NODE_SIZE ;
            }

            return _sync.sync( session, w, timeout ) ;
         }

         OSS_INLINE UINT32 getNtySessionNum ()
         {
            return _srcSessionNum ;
         }

         ossQueue< clsLSNNtyInfo >* getNtyQue() { return &_ntyQue ; }
         DPS_LSN_OFFSET getNtyLastOffset() const { return _ntyLastOffset ; }
         DPS_LSN_OFFSET getNtyProcessedOffset() const { return _ntyProcessedOffset ; }

         void notify2Session( UINT32 suLID, UINT32 clLID, dmsExtentID extLID,
                              const DPS_LSN_OFFSET &offset ) ;

         virtual void onWriteLog( DPS_LSN_OFFSET offset ) ;

         virtual void onPrepareLog( UINT32 csLID, UINT32 clLID,
                                    INT32 extLID, DPS_LSN_OFFSET offset ) ;

         virtual INT32 canAssignLogPage( UINT32 reqLen, pmdEDUCB *cb ) ;

         virtual INT32 onCompleteOpr( _pmdEDUCB *cb, INT32 w )
         {
            return sync( cb->getEndLsn(), cb, w ) ;
         }

      public:
         void  regSession ( _clsDataSrcBaseSession *pSession ) ;
         void  unregSession ( _clsDataSrcBaseSession *pSession ) ;

      public:
         INT32 initialize() ;
         INT32 active() ;
         INT32 deactive () ;
         INT32 final() ;
         void  onConfigChange() ;
         void  ntyPrimaryChange( BOOLEAN primary,
                                 SDB_EVENT_OCCUR_TYPE type ) ;

         virtual void  onTimer ( UINT64 timerID, UINT32 interval ) ;

         INT32 handleMsg( NET_HANDLE handle, MsgHeader* msg ) ;

         INT32 callCatalog( MsgHeader *header ) ;

         void getGroupInfo( _MsgRouteID &primary,
                            vector<_netRouteNode > &group ) ;

         _MsgRouteID getPrimary () ;

         void tearDown() ;

         INT64 netIn() ;
         INT64 netOut() ;
         void resetMon() ;

      private:
         INT32 _setGroupSet( const CLS_GROUP_VERSION &version,
                             map<UINT64, _netRouteNode> &nodes ) ;

         INT32 _alive( const _MsgRouteID &id ) ;

         INT32 _handleSharingBeat( const _MsgClsBeat *msg ) ;

         INT32 _handleSharingBeatRes( const _MsgClsBeatRes *msg ) ;

         INT32 _handleGroupRes( const _MsgCatGroupRes *msg ) ;

         void _sharingBeat() ;

         void _checkBreak( const UINT32 &millisec ) ;

         UINT32 _getThresholdTime( UINT64 diffSize ) ;

      private:
         _netRouteAgent          *_agent ;
         _clsGroupInfo           _info ;
         _clsVoteMachine         _vote ;
         _dpsLogWrapper          *_logger ;
         _clsSyncManager         _sync ;
         _clsCatalogCaller       _cata ;
         clsBucket               _replBucket ;
         _clsMgr                 *_clsCB ;
         UINT64                  _timerID ;
         UINT32                  _beatTime ;
         UINT32                  _downloadTime ;
         BOOLEAN                 _active ;
         CLS_BS_STATUS           _replStatus ;

         UINT32                  _srcSessionNum ;
         ossRWMutex              _vecLatch ;
         std::vector<_clsDataSrcBaseSession*> _vecSrcSessions ;

         ossQueue< clsLSNNtyInfo >  _ntyQue ;
         DPS_LSN_OFFSET             _ntyLastOffset ;
         DPS_LSN_OFFSET             _ntyProcessedOffset ;

         UINT64                  _totalLogSize ;
         UINT64                  _sizethreshold[ CLS_SYNCCTRL_THRESHOLD_SIZE ] ;
         UINT32                  _timeThreshold[ CLS_SYNCCTRL_THRESHOLD_SIZE ] ;
         BOOLEAN                 _inSyncCtrl ;

   } ;

   typedef class _clsReplicateSet clsReplicateSet ;
   typedef _clsReplicateSet replCB ;
}

#endif

