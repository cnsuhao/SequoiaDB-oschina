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

   Source File Name = clsDef.hpp

   Descriptive Name =

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

#ifndef CLSDEF_HPP_
#define CLSDEF_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "dpsLogDef.hpp"
#include "netDef.hpp"
#include "msgDef.h"
#include "ossMem.hpp"
#include "msg.h"
#include "pmdEDU.hpp"
#include "ossRWMutex.hpp"
#include "dms.hpp"

#include <map>

using namespace std ;

namespace engine
{
   typedef UINT32 CLS_GROUP_VERSION ;

   const UINT32 CLS_VOTE_CS_TIME = 3000 ;
   const UINT32 CLS_SHARING_BETA_INTERVAL = 2000 ;
   const UINT32 CLS_SYNC_MAX_LEN = 1024 * 1024 * 5 ;
   extern INT32  g_startShiftTime ;

   #define CLS_TID(sessionid)          ((UINT32)(sessionid & 0xFFFFFFFF))
   #define CLS_NODEID(sessionid)       ((UINT32)(sessionid >> 32))


   #define CLS_FS_NORES_TIMEOUT 10000
   #define CLS_DST_SESSION_NO_MSG_TIME          (300000)
   #define CLS_SRC_SESSION_NO_MSG_TIME          (10000)

   enum CLS_SYNC_STATUS
   {
      CLS_SYNC_STATUS_NONE = 0,
      CLS_SYNC_STATUS_PEER = 1,
      CLS_SYNC_STATUS_RC = 2,
   } ;

   enum CLS_BEAT_STATUS
   {
      CLS_BEAT_STATUS_BREAK = 0,
      CLS_BEAT_STATUS_ALIVE = 1,
   } ;

   const UINT32 CLS_BEAT_ID_OVERTURN_WINDOW = 2 ^ 31 ;

   enum CLS_GROUP_ROLE
   {
      CLS_GROUP_ROLE_SECONDARY = 0,
      CLS_GROUP_ROLE_PRIMARY =1,
   } ;

   enum CLS_ELECTION_STATUS
   {
      CLS_ELECTION_STATUS_SILENCE = 0,
      CLS_ELECTION_STATUS_SEC = 1,
      CLS_ELECTION_STATUS_VOTE = 2,
      CLS_ELECTION_STATUS_ANNOUNCE = 3,
      CLS_ELECTION_STATUS_PRIMARY = 4,
   } ;

   enum CLS_BS_STATUS
   {
      CLS_BS_CLOSED           = 0,
      CLS_BS_NORMAL,
      CLS_BS_FULLSYNC,
      CLS_BS_BACKUPOFFLINE,
   } ;

   enum CLS_NODE_SERVICE_STATUS
   {
      SERVICE_NORMAL          = 0,
      SERVICE_ABNORMAL,
      SERVICE_UNKNOWN
   } ;

   /*
      _clsGroupBeat define
   */
   class _clsGroupBeat : public SDBObject
   {
   public :
      DPS_LSN                 endLsn ;
      _MsgRouteID             identity ;
      UINT8                   weight ;
      CHAR                    pad[7] ;
      CLS_GROUP_VERSION       version ;
      CLS_GROUP_ROLE          role ;         // self role
      CLS_SYNC_STATUS         syncStatus ;
      UINT32                  beatID ;
      CLS_NODE_SERVICE_STATUS serviceStatus ;

      _clsGroupBeat(): version( 0 ),
                       role( CLS_GROUP_ROLE_SECONDARY ),
                       syncStatus( CLS_SYNC_STATUS_NONE ),
                       beatID( 0 ),
                       serviceStatus( SERVICE_UNKNOWN )
      {
         UINT64 *p = ( UINT64 *)(&weight) ;
         *p = 0 ;
      }

      BOOLEAN isValidID( const UINT32 &id )
      {
         if ( beatID < id )
         {
            return TRUE ;
         }
         else if ( (beatID - id) >
                    CLS_BEAT_ID_OVERTURN_WINDOW )
         {
            return TRUE ;
         }
         else
         {
            return FALSE ;
         }
      }
   } ;

   /*
      _clsSharingStatus define
   */
   class _clsSharingStatus : public SDBObject
   {
   public:
      _clsGroupBeat beat ;
      UINT32 timeout ;
      UINT32 breakTime ;
      _clsSharingStatus():timeout(0), breakTime( 0 )
      {
      }
   } ;

   /*
      _clsGroupInfo define
   */
   class _clsGroupInfo : public SDBObject
   {
   public :
      map<UINT64, _clsSharingStatus> info ;
      map<UINT64, _clsSharingStatus *> alives ;
      ossRWMutex mtx ;
      _MsgRouteID primary ;
      _MsgRouteID local ;
      UINT32 localBeatID ;
      CLS_GROUP_VERSION version ;
      _clsGroupInfo():localBeatID( 0 ),
                      version( 0 )
      {
         local.value = 0 ;
         primary.value = 0 ;
      }
      ~_clsGroupInfo()
      {
         alives.clear() ;
         info.clear() ;
      }

      UINT32 groupSize ()
      {
         return info.size() + 1 ;
      }

      UINT32 aliveSize ()
      {
         return alives.size() + 1 ;
      }

      BOOLEAN isAllNodeBeat()
      {
         map<UINT64, _clsSharingStatus>::iterator it = info.begin() ;
         while ( it != info.end() )
         {
            _clsSharingStatus &status = it->second ;
            if ( 0 == status.beat.beatID )
            {
               return FALSE ;
            }
            ++it ;
         }
         return TRUE ;
      }

      BOOLEAN isAllNodeAbnormal( UINT32 timeout )
      {
         map<UINT64, _clsSharingStatus>::iterator it = info.begin() ;
         while ( it != info.end() )
         {
            _clsSharingStatus &status = it->second ;
            if ( SERVICE_NORMAL == status.beat.serviceStatus )
            {
               return FALSE ;
            }
            else if ( SERVICE_UNKNOWN == status.beat.serviceStatus &&
                      ( 0 == timeout || status.breakTime < timeout ) )
            {
               return FALSE ;
            }
            ++it ;
         }
         return TRUE ;
      }

   } ;

   enum CLS_ELECTION_ROUND
   {
      CLS_ELECTION_ROUND_STAGE_ONE = 0,
      CLS_ELECTION_ROUND_STAGE_TWO = 1,
   } ;

   class _clsSyncSession : public SDBObject
   {
   public :
      DPS_LSN_OFFSET endLsn ;
      _pmdEDUCB *eduCB ;

      _clsSyncSession():endLsn(DPS_INVALID_LSN_OFFSET), eduCB( NULL)
      {}

      BOOLEAN operator<( const _clsSyncSession &session )
      {
         return endLsn < session.endLsn ;
      }

      BOOLEAN operator<=( const _clsSyncSession &session )
      {
         return endLsn <= session.endLsn ;
      }
   } ;


   #define CLS_SAME_SYNC_LSN_MAX_TIMES    (20)

   /*
      _clsSyncStatus define
   */
   class _clsSyncStatus : public SDBObject
   {
   public :
      DPS_LSN_OFFSET    offset ;
      _MsgRouteID       id ;
      BOOLEAN           valid ;
      UINT32            sameReqTimes ;

      _clsSyncStatus():offset(0)
      {
         id.value       = 0 ;
         valid          = TRUE ;
         sameReqTimes   = 0 ;
      }

      _clsSyncStatus& operator=( const _clsSyncStatus &right )
      {
         offset         = right.offset ;
         id.value       = right.id.value ;
         valid          = right.valid ;
         sameReqTimes   = right.sameReqTimes ;

         return *this ;
      }

      BOOLEAN isValid() const
      {
         if ( DPS_INVALID_LSN_OFFSET == offset ||
              !valid ||
              sameReqTimes > CLS_SAME_SYNC_LSN_MAX_TIMES )
         {
            return FALSE ;
         }
         return TRUE ;
      }
   } ;

   /*
      _clsLSNNtyInfo define
   */
   struct _clsLSNNtyInfo
   {
      UINT32               _csLID ;
      UINT32               _clLID ;
      dmsExtentID          _extLID ;
      DPS_LSN_OFFSET       _offset ;

      _clsLSNNtyInfo ()
      {
         _csLID = ~0 ;
         _clLID = ~0 ;
         _extLID = -1 ;
         _offset = 0 ;
      }
      _clsLSNNtyInfo( UINT32 csLID, UINT32 clLID, dmsExtentID extLID,
                      DPS_LSN_OFFSET offset )
      {
         _csLID      = csLID ;
         _clLID      = clLID ;
         _extLID     = extLID ;
         _offset     = offset ;
      }
   } ;
   typedef _clsLSNNtyInfo clsLSNNtyInfo ;

}

#endif // CLSDEF_HPP_

