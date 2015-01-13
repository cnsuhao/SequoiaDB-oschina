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

   Source File Name = rtnContext.hpp

   Descriptive Name = RunTime Context Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains structure for Runtime
   Context.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef RTNCONTEXT_HPP_
#define RTNCONTEXT_HPP_

#include "dms.hpp"
#include "pd.hpp"
#include "ossMem.hpp"
#include "ossLatch.hpp"
#include "ossRWMutex.hpp"
#include "mthMatcher.hpp"
#include "mthSelector.hpp"
#include "monCB.hpp"
#include "ixm.hpp"
#include "optAccessPlan.hpp"
#include "qgmPlanContainer.hpp"
#include "msg.h"
#include "ossAtomic.hpp"
#include "../bson/bsonobj.h"
#include "dmsCB.hpp"
#include "rtnQueryOptions.hpp"
#include "dmsLobDef.hpp"
#include "rtnLocalLobStream.hpp"
#include "rtnContextBuff.hpp"

#include <map>

using namespace bson ;

namespace engine
{

   class _pmdEDUCB ;
   class _dmsStorageUnit ;
   class _rtnIXScanner ;
   class _optAccessPlan ;
   class _SDB_DMSCB ;
   class _dmsMBContext ;
   class _rtnContextBase ;
   class netMultiRouteAgent ;

   /*
      _rtnPrefWatcher define
   */
   class _rtnPrefWatcher : public SDBObject
   {
      public:
         _rtnPrefWatcher () :_prefNum(0), _needWait(FALSE) {}
         ~_rtnPrefWatcher () {}
         void     reset ()
         {
            _needWait = _prefNum > 0 ? TRUE : FALSE ;
            _prefEvent.reset() ;
         }
         void     ntyBegin ()
         { 
            ++_prefNum ;
            _needWait = TRUE ;
         }
         void     ntyEnd ()
         {
            --_prefNum ;
            _prefEvent.signalAll() ;
         }
         INT32    waitDone( INT64 millisec = -1 )
         {
            if ( !_needWait && _prefNum <= 0 )
            {
               return 0 ;
            }
            INT32 rc = _prefEvent.wait( millisec, NULL ) ;
            if ( SDB_OK == rc )
            {
               return 1 ;
            }
            return rc ;
         }

      private:
         UINT32         _prefNum ;
         BOOLEAN        _needWait ;
         ossEvent       _prefEvent ;
   } ;
   typedef _rtnPrefWatcher rtnPrefWatcher ;

   /*
      RTN_CONTEXT_TYPE define
   */
   enum RTN_CONTEXT_TYPE
   {
      RTN_CONTEXT_DATA     = 1,
      RTN_CONTEXT_DUMP,
      RTN_CONTEXT_COORD,
      RTN_CONTEXT_QGM,
      RTN_CONTEXT_TEMP,
      RTN_CONTEXT_SP,
      RTN_CONTEXT_PARADATA,
      RTN_CONTEXT_MAINCL,
      RTN_CONTEXT_SORT,
      RTN_CONTEXT_QGMSORT,
      RTN_CONTEXT_DELCS,
      RTN_CONTEXT_DELCL,
      RTN_CONTEXT_DELMAINCL,
      RTN_CONTEXT_EXPLAIN,
      RTN_CONTEXT_LOB,
      RTN_CONTEXT_SHARD_OF_LOB,
      RTN_CONTEXT_LIST_LOB,
   } ;

   const CHAR *getContextTypeDesp( RTN_CONTEXT_TYPE type ) ;

   /*
      _rtnContextBase define
   */
   class _rtnContextBase : public SDBObject
   {
      friend class _rtnContextParaData ;
      public:
         _rtnContextBase ( INT64 contextID, UINT64 eduID ) ;
         virtual ~_rtnContextBase () ;
         string   toString() ;

         INT32    newMatcher () ;

         INT64    contextID () const { return _contextID ; }
         UINT64   eduID () const { return _eduID ; }

         monContextCB*     getMonCB () { return &_monCtxCB ; }
         ossRWMutex*       dataLock () { return &_dataLock ; }
         _mthSelector&     getSelector () { return _selector ; }
         _mthMatcher*      getMatcher () { return _matcher ; }

         INT32    append( const BSONObj &result ) ;
         INT32    appendObjs( const CHAR *pObjBuff,
                              INT32 len,
                              INT32 num,
                              BOOLEAN needAligned = TRUE ) ;

         virtual INT32    getMore( INT32 maxNumToReturn,
                                   rtnContextBuf &buffObj,
                                   _pmdEDUCB *cb ) ;

         OSS_INLINE BOOLEAN  isEmpty () const ;

         INT64    numRecords () const { return _bufferNumRecords ; }
         INT32    buffSize () const { return _resultBufferSize ; }
         INT64    totalRecords () const { return _totalRecords ; }
         OSS_INLINE INT32 freeSize () const ;
         INT32    buffEndOffset () const { return _bufferEndOffset ; }

         BOOLEAN  isOpened () const { return _isOpened ; }
         BOOLEAN  eof () const { return _hitEnd ; }

         INT32    getReference() const ;

      public:
         void     enablePrefetch ( _pmdEDUCB *cb,
                                   rtnPrefWatcher *pWatcher = NULL ) ;
         void     disablePrefetch ()
         {
            _prefetchID = 0 ;
            _pPrefWatcher = NULL ;
            _pMonAppCB = NULL ;
         }
         INT32    prefetchResult() const { return _prefetchRet ; }
         INT32    prefetch ( _pmdEDUCB *cb, UINT32 prefetchID ) ;
         void     waitForPrefetch() ;

      public:

         virtual RTN_CONTEXT_TYPE getType () const = 0 ;
         virtual _dmsStorageUnit* getSU () = 0 ;
         virtual _optAccessPlan*  getPlan () { return NULL ; }

      protected:
         void              _onDataEmpty () ;
         virtual INT32     _prepareData( _pmdEDUCB *cb ) = 0 ;
         virtual BOOLEAN   _canPrefetch () const { return FALSE ; }
         virtual void      _toString( stringstream &ss ) {}

      protected:
         INT32    _reallocBuffer ( SINT32 requiredSize ) ;
         OSS_INLINE void _empty () ;
         OSS_INLINE void _close () { _isOpened = FALSE ; }
         UINT32   _getWaitPrefetchNum () { return _waitPrefetchNum.peek() ; }
         BOOLEAN  _isInPrefetching () const { return _isInPrefetch ; }

      protected:
         monContextCB            _monCtxCB ;
         _mthSelector            _selector ;
         _mthMatcher             *_matcher ;
         BOOLEAN                 _ownedMatcher ;
         BOOLEAN                 _hitEnd ;
         BOOLEAN                 _isOpened ;

      private:
         INT64                   _contextID ;
         UINT64                  _eduID ;
         CHAR                   *_pResultBuffer ;
         INT32                   _resultBufferSize ;
         INT32                   _bufferCurrentOffset ;
         INT32                   _bufferEndOffset ;
         INT64                   _bufferNumRecords ;
         INT64                   _totalRecords ;
         ossRWMutex              _dataLock ;
         ossRWMutex              _prefetchLock ;
         UINT32                  _prefetchID ;
         ossAtomic32             _waitPrefetchNum ;
         BOOLEAN                 _isInPrefetch ;
         INT32                   _prefetchRet ;
         rtnPrefWatcher          *_pPrefWatcher ;
         _monAppCB               *_pMonAppCB ;
   } ;
   typedef _rtnContextBase rtnContextBase ;

   /*
      _rtnContextBase OSS_INLINE functions
   */
   OSS_INLINE BOOLEAN _rtnContextBase::isEmpty () const
   {
      return _bufferCurrentOffset >= _bufferEndOffset ;
   }
   OSS_INLINE void _rtnContextBase::_empty ()
   {
      _bufferCurrentOffset = 0 ;
      _bufferEndOffset     = 0 ;
      _totalRecords        = _totalRecords - _bufferNumRecords ;
      _bufferNumRecords    = 0 ;
   }
   OSS_INLINE INT32 _rtnContextBase::freeSize () const
   {
      return _resultBufferSize - ossAlign4((UINT32)_bufferEndOffset) ;
   }

   /*
      _rtnContextData define
   */
   class _rtnContextData : public _rtnContextBase
   {
      public:
         _rtnContextData ( INT64 contextID, UINT64 eduID ) ;
         virtual ~_rtnContextData () ;

         _rtnIXScanner*    getIXScanner () { return _scanner ; }
         optScanType       scanType () const { return _scanType ; }
         _dmsMBContext*    getMBContext () { return _mbContext ; }

         dmsExtentID       lastExtLID () const { return _lastExtLID ; }

         virtual INT32 open( _dmsStorageUnit *su, _dmsMBContext *mbContext,
                             _optAccessPlan *plan, _pmdEDUCB *cb,
                             const BSONObj &selector, INT64 numToReturn = -1,
                             INT64 numToSkip = 0,
                             const BSONObj *blockObj = NULL,
                             INT32 direction = 1 ) ;

         INT32 openTraversal( _dmsStorageUnit *su, _dmsMBContext *mbContext,
                              _optAccessPlan *plan, _rtnIXScanner *scanner,
                              _pmdEDUCB *cb, const BSONObj &selector,
                              INT64 numToReturn = -1, INT64 numToSkip = 0 ) ;

      public:
         virtual RTN_CONTEXT_TYPE getType () const ;
         virtual _dmsStorageUnit* getSU () { return _su ; }
         virtual _optAccessPlan*  getPlan () { return _plan ; }

      protected:
         virtual INT32     _prepareData( _pmdEDUCB *cb ) ;
         virtual BOOLEAN   _canPrefetch () const { return TRUE ; }
         virtual void      _toString( stringstream &ss ) ;

      protected:

         INT32    _prepareByTBScan( _pmdEDUCB *cb ) ;
         INT32    _prepareByIXScan( _pmdEDUCB *cb ) ;

         INT32    _parseSegments( const BSONObj &obj,
                                  std::vector< dmsExtentID > &segments ) ;
         INT32    _parseIndexBlocks( const BSONObj &obj,
                                    std::vector< BSONObj > &indexBlocks,
                                    std::vector< dmsRecordID > &indexRIDs ) ;
         INT32    _parseRID( const BSONElement &ele, dmsRecordID &rid ) ;

         INT32    _openTBScan ( _dmsStorageUnit *su, _dmsMBContext *mbContext,
                                _optAccessPlan *plan, _pmdEDUCB *cb,
                                const BSONObj *blockObj ) ;
         INT32    _openIXScan ( _dmsStorageUnit *su, _dmsMBContext *mbContext,
                                _optAccessPlan *plan, _pmdEDUCB *cb,
                                const BSONObj *blockObj,
                                INT32 direction ) ;

      protected:
         _SDB_DMSCB                 *_dmsCB ;
         _dmsStorageUnit            *_su ;
         _dmsMBContext              *_mbContext ;
         _optAccessPlan             *_plan ;
         optScanType                _scanType ;

         SINT64                     _numToReturn ;
         SINT64                     _numToSkip ;

         dmsExtentID                _extentID ;
         dmsExtentID                _lastExtLID ;
         BOOLEAN                    _segmentScan ;
         std::vector< dmsExtentID > _segments ;
         _rtnIXScanner              *_scanner ;
         std::vector< BSONObj >     _indexBlocks ;
         std::vector< dmsRecordID > _indexRIDs ;
         BOOLEAN                    _indexBlockScan ;
         INT32                      _direction ;

   } ;
   typedef _rtnContextData rtnContextData ;

   /*
      _rtnContextParaData define
   */
   class _rtnContextParaData : public _rtnContextData
   {
      public:
         _rtnContextParaData( INT64 contextID, UINT64 eduID ) ;
         virtual ~_rtnContextParaData () ;

         virtual INT32 open( _dmsStorageUnit *su, _dmsMBContext *mbContext,
                             _optAccessPlan *plan, _pmdEDUCB *cb,
                             const BSONObj &selector, INT64 numToReturn = -1,
                             INT64 numToSkip = 0,
                             const BSONObj *blockObj = NULL,
                             INT32 direction = 1 ) ;

      public:
         virtual RTN_CONTEXT_TYPE getType () const ;

      protected:
         virtual INT32     _prepareData( _pmdEDUCB *cb ) ;
         virtual BOOLEAN   _canPrefetch () const { return FALSE ; }

         const BSONObj* _nextBlockObj () ;
         INT32          _checkAndPrefetch () ;
         INT32          _getSubContextData( _pmdEDUCB *cb ) ;
         INT32          _openSubContext( const BSONObj *blockObj,
                                         const BSONObj &selector,
                                         _pmdEDUCB *cb,
                                         INT64 numToReturn ) ;
         void           _removeSubContext( rtnContextData *pContext ) ;
         INT32          _getSubCtxWithData ( rtnContextData **ppContext,
                                             _pmdEDUCB *cb ) ;

      protected:
         std::vector< _rtnContextData* >           _vecContext ;
         BOOLEAN                                   _isParalled ;
         BSONObj                                   _blockObj ;
         UINT32                                    _curIndex ;
         UINT32                                    _step ;
         rtnPrefWatcher                            _prefWather ;

   } ;
   typedef _rtnContextParaData rtnContextParaData ;

   /*
      _rtnContextTemp define
   */
   class _rtnContextTemp : public _rtnContextData
   {
      public:
         _rtnContextTemp ( INT64 contextID, UINT64 eduID ) ;
         virtual ~_rtnContextTemp ();

      public:
         virtual RTN_CONTEXT_TYPE getType () const ;

   } ;
   typedef _rtnContextTemp rtnContextTemp ;

   /*
      _rtnContextQGM define
   */
   class _rtnContextQGM : public _rtnContextBase
   {
      public:
         _rtnContextQGM ( INT64 contextID, UINT64 eduID ) ;
         virtual ~_rtnContextQGM () ;

      public:

         virtual RTN_CONTEXT_TYPE getType () const ;
         virtual _dmsStorageUnit* getSU () { return NULL ; }

         INT32 open( _qgmPlanContainer *accPlan ) ;

      protected:
         virtual INT32  _prepareData( _pmdEDUCB *cb ) ;

      private:
         _qgmPlanContainer          *_accPlan ;

   } ;
   typedef _rtnContextQGM rtnContextQGM ;

   /*
      _rtnContextDump define
   */
   class _rtnContextDump : public _rtnContextBase
   {
      public:
         _rtnContextDump ( INT64 contextID, UINT64 eduID ) ;
         virtual ~_rtnContextDump () ;

         INT32 open ( const BSONObj &selector, const BSONObj &matcher,
                      INT64 numToReturn = -1, INT64 numToSkip = 0 ) ;

         INT32 monAppend( const BSONObj &result ) ;

      public:

         virtual RTN_CONTEXT_TYPE getType () const ;
         virtual _dmsStorageUnit* getSU () { return NULL ; }

      protected:
         virtual INT32  _prepareData( _pmdEDUCB *cb ) ;

      private:
         SINT64                     _numToReturn ;
         SINT64                     _numToSkip ;

         BSONObj                    _orderby ;

   } ;
   typedef _rtnContextDump rtnContextDump ;

   /*
      _coordOrderKey define
   */
   class _coordOrderKey : public SDBObject
   {
      typedef std::vector< BSONElement >  OrderKeyList;
      typedef std::vector< BSONElement >  OrderKeyEleList;
      typedef std::vector< BSONObj >      OrderKeyObjList;
      public:
         _coordOrderKey( const _coordOrderKey &orderKey ) ;
         _coordOrderKey() ;

      public:
         BOOLEAN operator<( const _coordOrderKey &rhs ) const ;
         void clear() ;
         void setOrderBy( const BSONObj &orderBy ) ;
         INT32 generateKey( const BSONObj &record,
                           _ixmIndexKeyGen *keyGen ) ;

      private:
         BSONObj              _orderBy ;
         ixmHashValue         _hash ;
         BSONObj              _keyObj ;
         BSONElement          _arrEle ;
   } ;
   typedef _coordOrderKey coordOrderKey ;

   /*
      coordSubContext define
   */
   class _coordSubContext : public SDBObject
   {
      public:
         _coordSubContext ( MsgRouteID routeID,
                           SINT64 contextID,
                           _ixmIndexKeyGen *keyGen ) ;
         ~_coordSubContext () ;

      public:

         void     appendData ( MsgOpReply *pReply ) ;
         void     clearData () ;
         SINT64   getContextID() ;
         MsgRouteID getRouteID() ;
         CHAR*    front () ;
         INT32    pop() ;
         INT32    popN( SINT32 num ) ;
         INT32    popAll() ;
         SINT32   getRecordNum() ;
         UINT32   getRemainLen() ;
         INT32    getOrderKey( coordOrderKey &orderKey,
                              _ixmIndexKeyGen *keyGen ) ;
         void     setOrderBy( const BSONObj &orderBy ) ;

      private:
         _coordSubContext () ;
         _coordSubContext ( const _coordSubContext &srcContext ) ;

      private:
         MsgRouteID           _routeID ;
         SINT64               _contextID ;
         INT32                _curOffset ;
         MsgOpReply           *_pData ;
         coordOrderKey        _orderKey ;
         BSONObj              _orderBy ;
         BOOLEAN              _isOrderKeyChange ;
         SINT32               _recordNum ;
         _ixmIndexKeyGen      *_keyGen ;

   } ;
   typedef _coordSubContext coordSubContext ;

   typedef std::multimap< coordOrderKey, coordSubContext* > SUB_CONTEXT_MAP ;
   typedef std::map< UINT64, coordSubContext* >             EMPTY_CONTEXT_MAP ;
   typedef std::map<UINT64, MsgRouteID>                     PREPARE_NODES_MAP ;

   /*
      _rtnContextCoord define
   */
   class _rtnContextCoord : public _rtnContextBase
   {
      public:
         _rtnContextCoord ( INT64 contextID, UINT64 eduID,
                           BOOLEAN preRead = TRUE ) ;
         virtual ~_rtnContextCoord () ;

         INT32    addSubContext ( MsgRouteID routeID, SINT64 contextID ) ;
         INT32    addSubContext ( MsgOpReply *pReply, BOOLEAN &takeOver ) ;

         void     addSubDone( _pmdEDUCB *cb ) ;

         INT32    open( const BSONObj &orderBy,
                        const BSONObj &selector,
                        INT64 numToReturn = -1,
                        INT64 numToSkip = 0 ) ;
         INT32    reopen () ;

         void     killSubContexts( _pmdEDUCB *cb ) ;

      public:

         virtual RTN_CONTEXT_TYPE getType () const ;
         virtual _dmsStorageUnit* getSU () { return NULL ; }

         OSS_INLINE  BOOLEAN requireOrder () const ;

      protected:
         virtual INT32  _prepareData( _pmdEDUCB *cb ) ;

      private:
         INT32    _getSubData () ;
         INT32    _getSubDataNormal () ;
         INT32    _getSubDataByOrder () ;
         INT32    _appendSubData ( CHAR *pData ) ;

         void     _delPrepareContext( const MsgRouteID &routeID ) ;

         INT32    _send2EmptyNodes( _pmdEDUCB *cb ) ;
         INT32    _getPrepareNodesData( _pmdEDUCB *cb, BOOLEAN waitAll ) ;

      private:
         SINT64                     _numToReturn ;
         SINT64                     _numToSkip ;

         SUB_CONTEXT_MAP            _subContextMap ;
         EMPTY_CONTEXT_MAP          _emptyContextMap ;
         EMPTY_CONTEXT_MAP          _prepareContextMap ;
         PREPARE_NODES_MAP          _prepareNodeMap ;

         netMultiRouteAgent         *_netAgent ;

         coordOrderKey              _emptyKey ;
         BSONObj                    _orderBy ;
         BOOLEAN                    _preRead ;

         _ixmIndexKeyGen            *_keyGen ;
         mthSelector                _selector ;
   } ;
   typedef _rtnContextCoord rtnContextCoord ;

   /*
      _rtnContextCoord OSS_INLINE functions
   */
   OSS_INLINE BOOLEAN _rtnContextCoord::requireOrder () const
   {
      if ( _orderBy.isEmpty() ||
           _subContextMap.size() + _emptyContextMap.size() +
           _prepareContextMap.size() <= 1 )
      {
         return FALSE ;
      }
      return TRUE ;
   }

   typedef class _rtnContextBase rtnContext ;

   /*
      _rtnContextSP OSS_INLINE functions
   */
   class _spdSession ;

   class _rtnContextSP : public _rtnContextBase
   {
   public:
      _rtnContextSP( INT64 contextID, UINT64 eduID ) ;
      virtual ~_rtnContextSP() ;

   public:
      virtual RTN_CONTEXT_TYPE getType () const ;
      virtual _dmsStorageUnit* getSU () { return NULL ; }
      INT32 open( _spdSession *sp ) ;

   protected:
      virtual INT32  _prepareData( _pmdEDUCB *cb ) ;

   private:
      _spdSession *_sp ;
   } ;

   typedef class _rtnContextSP rtnContextSP ;


   /*
      _rtnSubCLBuf
   */
   class _rtnSubCLBuf : public SDBObject
   {
   public:
      _rtnSubCLBuf();
      _rtnSubCLBuf( BSONObj &orderBy,
                  _ixmIndexKeyGen *keyGen );
      virtual ~_rtnSubCLBuf();
      const CHAR *front();
      INT32 pop();
      INT32 popN( SINT32 num );
      INT32 popAll();
      INT32 recordNum();
      INT32 getOrderKey( coordOrderKey &orderKey );
      rtnContextBuf buffer();
      void setBuffer( rtnContextBuf &buffer );

   private:
      coordOrderKey        _orderKey;
      BOOLEAN              _isOrderKeyChange;
      rtnContextBuf        _buffer;
      INT32                _remainNum;
      _ixmIndexKeyGen      *_keyGen;
   };
   typedef class _rtnSubCLBuf rtnSubCLBuf;

   /*
      _rtnContextMainCL define
   */
   class _rtnContextMainCL : public _rtnContextBase
   {
   typedef std::map< SINT64, _rtnSubCLBuf >    SubCLBufList;
   public:
      _rtnContextMainCL( SINT64 contextID, UINT64 eduID ) ;
      ~_rtnContextMainCL();
      virtual RTN_CONTEXT_TYPE getType () const;
      virtual _dmsStorageUnit* getSU () { return NULL ; }

      INT32 open( const bson::BSONObj & orderBy,
                  INT64 numToReturn,
                  INT64 numToSkip,
                  BOOLEAN includeShardingOrder = FALSE );

      virtual INT32 getMore( INT32 maxNumToReturn, rtnContextBuf &buffObj,
                             _pmdEDUCB *cb ) ;

      INT32 addSubContext( SINT64 contextID );

      BOOLEAN requireOrder () const;

   protected:
      virtual INT32 _prepareData( _pmdEDUCB *cb );

   private:
      INT32 _prepareSubCTXData( SubCLBufList::iterator iterSubCTX,
                              _pmdEDUCB * cb,
                              INT32 maxNumToReturn = -1 );
      INT32 _prepareDataByOrder( _pmdEDUCB *cb );

   private:
      INT64             _numToReturn;
      INT64             _numToSkip;
      BSONObj           _orderBy;
      SubCLBufList      _subCLBufList;
      SubCLBufList      _emptyBufList;
      BOOLEAN           _includeShardingOrder;
      _ixmIndexKeyGen   *_keyGen;
   };
   typedef class _rtnContextMainCL rtnContextMainCL;


   class _qgmPlan ;

   class _rtnContextQgmSort : public _rtnContextBase
   {
   public:
      _rtnContextQgmSort( INT64 contextID, UINT64 eduID ) ;
      virtual ~_rtnContextQgmSort() ;

   public:
      virtual RTN_CONTEXT_TYPE getType () const ;
      virtual _dmsStorageUnit* getSU () { return NULL ; }

      INT32 open( _qgmPlan *qp ) ;

   protected:
      virtual INT32  _prepareData( _pmdEDUCB *cb ) ;

   private:
      _qgmPlan *_qp ;
   } ;
   typedef class _rtnContextQgmSort rtnContextQgmSort ;

   /*
      _rtnContextDelCS define
   */
   class _clsCatalogAgent;
   class dpsTransCB;
   class _rtnContextDelCS : public _rtnContextBase
   {
      enum delCSPhase
      {
         DELCSPHASE_0 = 0,
         DELCSPHASE_1,
         DELCSPHASE_2
      };
   public:
      _rtnContextDelCS( SINT64 contextID, UINT64 eduID ) ;
      ~_rtnContextDelCS();
      virtual RTN_CONTEXT_TYPE getType () const;
      virtual _dmsStorageUnit* getSU () { return NULL ; }

      INT32 open( const CHAR *pCollectionName,
                  _pmdEDUCB *cb );

      virtual INT32 getMore( INT32 maxNumToReturn, rtnContextBuf &buffObj,
                             _pmdEDUCB *cb );

   protected:
      virtual INT32 _prepareData( _pmdEDUCB *cb ){ return SDB_DMS_EOC; };

   private:
      INT32 _tryLock( const CHAR *pCollectionName,
                     _pmdEDUCB *cb );

      INT32 _releaseLock( _pmdEDUCB *cb );

      void _clean( _pmdEDUCB *cb );

   private:
      delCSPhase           _status;
      SDB_DMSCB            *_pDmsCB;
      SDB_DPSCB            *_pDpsCB;
      dpsTransCB           *_pTransCB;
      _clsCatalogAgent     *_pCatAgent;
      CHAR                 _name[ DMS_COLLECTION_SPACE_NAME_SZ + 1 ];
      UINT32               _gotLogSize;
      BOOLEAN              _gotDmsCBWrite;
      UINT32               _logicCSID;
   };
   typedef class _rtnContextDelCS rtnContextDelCS;

   /*
      _rtnContextDelCL define
   */
   class _rtnContextDelCL : public _rtnContextBase
   {
   public:
      _rtnContextDelCL( SINT64 contextID, UINT64 eduID );
      ~_rtnContextDelCL();
      virtual RTN_CONTEXT_TYPE getType () const;
      virtual _dmsStorageUnit* getSU () { return NULL ; }

      INT32 open( const CHAR *pCollectionName,
                  _pmdEDUCB *cb );

      virtual INT32 getMore( INT32 maxNumToReturn, rtnContextBuf &buffObj,
                             _pmdEDUCB *cb );

   protected:
      virtual INT32 _prepareData( _pmdEDUCB *cb ){ return SDB_DMS_EOC; };

   private:
      INT32 _tryLock( const CHAR *pCollectionName,
                      _pmdEDUCB *cb );

      INT32 _releaseLock( _pmdEDUCB *cb );

      void _clean( _pmdEDUCB *cb );

   private:
      SDB_DMSCB            *_pDmsCB;
      SDB_DPSCB            *_pDpsCB;
      _clsCatalogAgent     *_pCatAgent;
      dpsTransCB           *_pTransCB;
      std::string          _collectionName ;
      std::string          _clShortName ;
      BOOLEAN              _gotDmsCBWrite ;
      BOOLEAN              _hasLock ;
      BOOLEAN              _hasDropped ;

      _dmsStorageUnit      *_su ;
      _dmsMBContext        *_mbContext ;
   };
   typedef class _rtnContextDelCL rtnContextDelCL;

   /*
      _rtnContextDelMainCL define
   */
   class _SDB_RTNCB;
   class _rtnContextDelMainCL : public _rtnContextBase
   {
      typedef std::map< std::string, SINT64 > SUBCL_CONTEXT_LIST;
   public:
      _rtnContextDelMainCL( SINT64 contextID, UINT64 eduID );
      ~_rtnContextDelMainCL();
      virtual RTN_CONTEXT_TYPE getType () const;
      virtual _dmsStorageUnit* getSU () { return NULL ; }

      INT32 open( const CHAR *pCollectionName,
                  _pmdEDUCB *cb );

      virtual INT32 getMore( INT32 maxNumToReturn, rtnContextBuf &buffObj,
                             _pmdEDUCB *cb ) ;

   protected:
      virtual INT32 _prepareData( _pmdEDUCB *cb ){ return SDB_DMS_EOC; };

   private:
      void _clean( _pmdEDUCB *cb );

   private:
      _clsCatalogAgent           *_pCatAgent;
      _SDB_RTNCB                 *_pRtncb;
      CHAR                       _name[ DMS_COLLECTION_FULL_NAME_SZ + 1 ];
      SUBCL_CONTEXT_LIST         _subContextList;
      INT32                      _version;
   };
   typedef class _rtnContextDelMainCL rtnContextDelMainCL;


   class _rtnContextExplain : public _rtnContextBase
   {
   public:
      _rtnContextExplain( INT64 contextID, UINT64 eduID ) ;
      virtual ~_rtnContextExplain() ;

   public:
      virtual RTN_CONTEXT_TYPE getType() const { return RTN_CONTEXT_EXPLAIN ; }
      virtual _dmsStorageUnit* getSU () { return NULL ; }

      INT32 open( const _rtnQueryOptions &options,
                  const BSONObj &explainOptions ) ;

   protected:
      virtual INT32     _prepareData( _pmdEDUCB *cb ) ;
      virtual BOOLEAN   _canPrefetch () const { return FALSE ; }

   private:
      INT32 _prepareToExplain( _pmdEDUCB *cb ) ;

      INT32 _explainQuery( _pmdEDUCB *cb ) ;

      INT32 _commitResult( _pmdEDUCB *cb ) ;

      INT32 _getMonInfo( _pmdEDUCB *cb, BSONObj &info ) ;

   private:
      _rtnQueryOptions _options ;
      INT64 _queryContextID ;
      BOOLEAN _needRun ;

      BSONObj _beginMon ;
      ossTimestamp _beginTime ;

      BSONObj _endMon ;
      ossTimestamp _endTime ;
      INT64 _recordNum ;

      _pmdEDUCB *_cbOfQuery ;

      BSONObjBuilder _builder ;
      BOOLEAN _explained ;
      
   } ;
   typedef class _rtnContextExplain rtnContextExplain ;

}

#endif //RTNCONTEXT_HPP_

