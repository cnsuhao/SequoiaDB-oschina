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

   Source File Name = clsFSDstSession.hpp

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

#ifndef CLSFSDSTSESSION_HPP_
#define CLSFSDSTSESSION_HPP_

#include "clsDef.hpp"
#include "clsFSDef.hpp"
#include "msgReplicator.hpp"
#include "pmdAsyncSession.hpp"
#include "clsReplayer.hpp"
#include "clsSrcSelector.hpp"
#include <vector>
#include "../bson/bsonobj.h"
#include "clsTask.hpp"

using namespace std ;
using namespace bson ;

namespace engine
{
   class _clsSyncManager ;
   class _clsShardMgr ;

   class _clsDataDstBaseSession : public _pmdAsyncSession
   {
      DECLARE_OBJ_MSG_MAP()

      public:
         _clsDataDstBaseSession ( UINT64 sessionID, _netRouteAgent *agent ) ;
         virtual ~_clsDataDstBaseSession () ;

      public:
         virtual BOOLEAN timeout ( UINT32 interval ) ;
         virtual void    onTimer ( UINT64 timerID, UINT32 interval ) ;
         virtual void    onRecieve ( const NET_HANDLE netHandle,
                                     MsgHeader * msg ) ;

      protected:
         virtual void   _begin () = 0 ;
         virtual void   _end () = 0 ;

         virtual BSONObj _keyObjB () = 0 ;
         virtual BSONObj _keyObjE () = 0 ;
         virtual INT32   _needData () const = 0 ;
         virtual INT32   _dataSessionType () const = 0 ;
         virtual BOOLEAN _isReady () = 0 ;
         virtual void    _endLog () = 0 ;
         virtual INT32   _onNotify () = 0 ;

      protected:
         INT32 handleMetaRes( NET_HANDLE handle, MsgHeader* header ) ;
         INT32 handleIndexRes( NET_HANDLE handle, MsgHeader* header ) ;
         INT32 handleNotifyRes( NET_HANDLE handle, MsgHeader* header ) ;

      protected:
         void           _disconnect() ;
         void           _meta () ;
         void           _index() ;
         void           _notify( CLS_FS_NOTIFY_TYPE type ) ;
         BOOLEAN        _more( const MsgClsFSNotifyRes *msg,
                               const CHAR *&itr,
                               BOOLEAN isData = TRUE ) ;

         BOOLEAN        _more( const MsgClsFSNotifyRes *msg,
                               const CHAR *&itr,
                               const bson::OID *&oid,
                               const MsgLobTuple *&tuple,
                               const CHAR *&data ) ;

         INT32          _extractFullNames( const CHAR *names ) ;
         INT32          _extractMeta( const CHAR *objdata, string &cs,
                                      string &collection, UINT32 &pageSize,
                                      UINT32 &attributes, INT32 &lobPageSize ) ;
         INT32          _extractIndex( const CHAR *objdata, vector<BSONObj> &index,
                                       BOOLEAN &noMore ) ;

         UINT32         _addCollection ( const CHAR *pCollectionName ) ;
         UINT32         _removeCollection ( const CHAR *pCollectionName ) ;
         UINT32         _removeCS ( const CHAR *pCSName ) ;

      private:
         INT32 _replayDoc( const MsgClsFSNotifyRes *msg ) ;
         INT32 _replayLog( const MsgClsFSNotifyRes *msg ) ;
         INT32 _replayLob( const MsgClsFSNotifyRes *msg ) ;

      protected:
         vector<string>       _fullNames ;
         _clsReplayer         _replayer ;
         clsSrcSelector       _selector ;
         _netRouteAgent       *_agent ;
         SINT64               _packet ;
         CLS_FS_STATUS        _status ;
         UINT32               _current ;
         UINT32               _timeout ;
         UINT32               _recvTimeout ;
         BOOLEAN              _quit ;
         UINT64               _requestID ;
         DPS_LSN              _expectLSN ;
         BOOLEAN              _needMoreDoc ;  /// when we begin to get lob, we do not want to sync doc any more.
         struct pageSzTuple
         {
            INT32 pageSize ;
            INT32 lobPageSize ;
            pageSzTuple( INT32 ps, INT32 lps )
            :pageSize( ps ),
             lobPageSize( lps )
            {

            }

            pageSzTuple()
            :pageSize( 0 ),
             lobPageSize( 0 )
            {

            }
         } ;
         typedef std::map<string, pageSzTuple> CS_PS_TUPLES ;
         CS_PS_TUPLES _mapEmptyCS ;

   };

   class _clsFSDstSession : public _clsDataDstBaseSession
   {
   DECLARE_OBJ_MSG_MAP()
   public:
      _clsFSDstSession( UINT64 sessionID,
                        _netRouteAgent *agent ) ;
      virtual ~_clsFSDstSession() ;
      enum TRANS_SYNC_STEP
      {
         STEP_TS_BEGIN = 0,
         STEP_TS_ING,
         STEP_TS_END
      };
   public:
      virtual SDB_SESSION_TYPE sessionType() const ;
      virtual EDU_TYPES eduType () const ;

   public:
      INT32 handleBeginRes( NET_HANDLE handle, MsgHeader* header ) ;
      INT32 handleEndRes( NET_HANDLE handle, MsgHeader* header ) ;
      INT32 handleSyncTransRes( NET_HANDLE handle, MsgHeader* header ) ;

   protected:
      virtual void      _onDetach () ;
   protected:
      void              _pullTransLog ( DPS_LSN &begin ) ;
   protected:
      virtual void      _begin() ;
      virtual void      _end() ;
      virtual INT32     _needData () const ;
      virtual BSONObj   _keyObjB () ;
      virtual BSONObj   _keyObjE () ;
      virtual INT32     _dataSessionType () const ;
      virtual BOOLEAN   _isReady () ;
      virtual void      _endLog () ;
      virtual INT32     _onNotify () { return SDB_OK ; }

   private:
      TRANS_SYNC_STEP   _tsStep;

   } ;

   class _clsSplitDstSession : public _clsDataDstBaseSession
   {
      DECLARE_OBJ_MSG_MAP ()

      public:
         _clsSplitDstSession ( UINT64 sessionID, _netRouteAgent *agent,
                               void *data ) ;
         ~_clsSplitDstSession () ;

      enum SESSION_STEP
      {
         STEP_NONE      = 0,
         STEP_START ,         // start notify to catalog
         STEP_META ,          // when cleanup notify to catalog and catalog
         STEP_END_NTY ,       // notify the peer node to update catalog and
         STEP_END_LOG,        // get the last log
         STEP_FINISH,         // notify catalog get all data, will to clean
         STEP_CLEANUP ,       // notify the peer node to clean up data
         STEP_REMOVE,         // remove notify to catalog
         STEP_END       
      };

      public:
         virtual SDB_SESSION_TYPE sessionType() const ;
         virtual EDU_TYPES eduType () const ;
         virtual BOOLEAN canAttachMeta() const ;

      protected:
         INT32 handleTaskNotifyRes ( NET_HANDLE handle, MsgHeader* header ) ;
         INT32 handleBeginRes( NET_HANDLE handle, MsgHeader* header ) ;
         INT32 handleEndRes( NET_HANDLE handle, MsgHeader* header ) ;
         INT32 handleLEndRes ( NET_HANDLE handle, MsgHeader* header ) ;

      protected:
         virtual void      _begin () ;
         virtual void      _end ()  ;
         virtual INT32     _needData () const ;
         virtual BSONObj   _keyObjB () ;
         virtual BSONObj   _keyObjE () ;
         virtual void      _onAttach () ;
         virtual void      _onDetach () ;
         virtual INT32     _dataSessionType () const ;
         virtual BOOLEAN   _isReady () ;
         virtual void      _endLog () ;
         virtual INT32     _onNotify () ;

      private:
         void              _taskNotify ( INT32 msgType ) ;
         void              _lend () ;

      protected:
         _clsSplitTask           *_pTask ;
         BSONObj                 _taskObj ;
         _clsShardMgr            *_pShardMgr ;
         INT32                   _step ;
         INT32                   _needSyncData ;
         BOOLEAN                 _regTask ;
         UINT32                  _collectionW ;

   };
}

#endif

