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

   Source File Name = clsCatalogAgent.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          05/12/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef CLS_CATALOG_AGENT_HPP_
#define CLS_CATALOG_AGENT_HPP_

#include "core.hpp"
#include <string>
#include <map>
#include <vector>
#include "oss.hpp"
#include "clsBase.hpp"
#include "ossRWMutex.hpp"
#include "netDef.hpp"
#include "ixmIndexKey.hpp"
#include "../bson/bson.h"
#include "../bson/ordering.h"

using namespace bson ;

namespace engine
{
   #define SDB_CLS_NODE_INFO_EXPIRED_TIME       100000
   class _clsShardMgr ;
   class _clsCatalogAgent ;
   class _clsCatalogSet ;

   class _clsCataItemKey : public SDBObject
   {
      enum CLS_KEY_TYPE
      {
         CLS_KEY_BSON      = 1,
         CLS_KEY_NUM
      } ;

      union clsKeyData
      {
         const CHAR           *_pUpBoundData ;
         INT32                _number ;
      } ;

      public:
         _clsCataItemKey ( const CHAR *pUpBoundData,
                           const Ordering *ordering ) ;
         _clsCataItemKey ( INT32 number ) ;
         _clsCataItemKey ( const _clsCataItemKey &right ) ;
         ~_clsCataItemKey () ;

         bool operator< ( const _clsCataItemKey &right ) const ;
         bool operator== ( const _clsCataItemKey &right ) const ;
         bool operator!= ( const _clsCataItemKey &right ) const ;
         bool operator<= ( const _clsCataItemKey &right ) const ;
         bool operator> ( const _clsCataItemKey &right ) const ;
         bool operator>= ( const _clsCataItemKey &right ) const ;
         void operator= ( const _clsCataItemKey &right ) ;

         clsKeyData           _keyData ;
         INT32                _keyType ;
         const Ordering       *_ordering ;
   } ;
   typedef _clsCataItemKey clsCataItemKey ;

   class _clsCatalogItem : public SDBObject
   {
      public:
         _clsCatalogItem ( BOOLEAN saveName = TRUE,
                           BOOLEAN isSubCl = FALSE ) ;
         ~_clsCatalogItem () ;

      public:
         UINT32    getGroupID () const ;
         const string &getGroupName() const { return _groupName ;}
         BSONObj&  getLowBound () ;
         BSONObj&  getUpBound () ;

         clsCataItemKey getLowBoundKey ( const Ordering* ordering ) ;
         clsCataItemKey getUpBoundKey ( const Ordering* ordering ) ;

         INT32    updateItem ( const BSONObj &obj, BOOLEAN isSharding,
                               BOOLEAN isHash ) ;
         BSONObj  toBson () ;
         std::string getSubClName(){ return _subCLName; } ;
         BOOLEAN  isLast() const { return _isLast ; }

      private:
         BSONObj           _lowBound ;
         BSONObj           _upBound ;
         UINT32            _groupID ;
         BOOLEAN           _isHash ;
         BOOLEAN           _saveName ;
         string            _groupName ;
         string            _subCLName ;
         BOOLEAN           _isSubCl ;

         BOOLEAN           _isLast ;

         friend class _clsCatalogSet ;
   };
   typedef _clsCatalogItem clsCatalogItem ;

   typedef std::vector<UINT32>                           VEC_GROUP_ID ;

   class _clsCataOrder : public SDBObject
   {
      public:
         _clsCataOrder ( const Ordering &order ) ;
         ~_clsCataOrder () ;

         Ordering* getOrdering () ;

      private:
         Ordering          _ordering ;
   } ;
   typedef _clsCataOrder clsCataOrder ;

   class _clsCatalogSet : public SDBObject
   {
      friend class _clsCatalogAgent ;

      public:
      typedef std::map<clsCataItemKey, clsCatalogItem*>  MAP_CAT_ITEM ;
      typedef MAP_CAT_ITEM::iterator                     MAP_CAT_ITEM_IT ;
      typedef MAP_CAT_ITEM_IT                            POSITION ;

      public:
         _clsCatalogSet ( const CHAR * name, BOOLEAN saveName = TRUE ) ;
         ~_clsCatalogSet () ;

      public:
         INT32             getVersion () const ;
         UINT32            getW () const ;
         INT32             getHashPartition() const { return _partition ;}
         UINT32            getPartitionBit() const { return _square ; }
         bool              ensureShardingIndex() const { return _ensureShardingIndex ; }
         const CHAR        *name () const ;
         VEC_GROUP_ID      *getAllGroupID () ;
         UINT32            getAllGroupID ( VEC_GROUP_ID &vecGroup ) ;
         UINT32            groupCount () const ;
         Ordering*         getOrdering () ;
         BSONObj&          getShardingKey () ;
         BSONObj           OwnedShardingKey () ;
         BOOLEAN           isWholeRange () const ;
         BOOLEAN           isSharding () const ;
         BOOLEAN           isIncludeShardingKey( const bson::BSONObj &record ) const;

         INT32             genKeyObj ( const BSONObj &obj, BSONObj &keyObj ) ;
         INT32             findItem ( const BSONObj & obj, clsCatalogItem *& item ) ;
         INT32             findGroupID ( const BSONObj & obj, UINT32 &groupID ) ;
         INT32             findGroupID ( const bson::OID &oid,
                                         UINT32 sequence,
                                         UINT32 &groupID ) ;
         INT32             findGroupIDS ( const BSONObj &matcher, VEC_GROUP_ID &vecGroup );
         INT32             findSubCLName ( const BSONObj &obj, std::string &subCLName );
         INT32             findSubCLNames( const bson::BSONObj &matcher,
                                          std::vector< std::string > &subCLList );

         INT32             getGroupLowBound( UINT32 groupID, BSONObj &lowBound ) const;
         INT32             getGroupUpBound( UINT32 groupID, BSONObj &upBound ) const;

         INT32             updateCatSet ( const BSONObj & catSet,
                                          UINT32 groupID = 0 ) ;
         BSONObj           toCataInfoBson () ;
         INT32             split ( const BSONObj &splitKey,
                                   const BSONObj &splitEndKey,
                                   UINT32 groupID, const CHAR *groupName ) ;
         BOOLEAN           isObjInGroup ( const BSONObj &obj, UINT32 groupID ) ;
         BOOLEAN           isKeyInGroup ( const BSONObj &obj, UINT32 groupID ) ;
         BOOLEAN           isKeyOnBoundary ( const BSONObj &obj,
                                             UINT32* pGroupID = NULL ) ;

         BOOLEAN           isHashSharding() const ;
         BOOLEAN           isRangeSharding() const ;

         UINT32            getItemNum() ;
         POSITION          getFirstItem() ;
         clsCatalogItem*   getNextItem( POSITION &pos ) ;

         UINT32            getAttribute() const
         {
            return _attribute ;
         }

         BOOLEAN isMainCL();
         INT32 getSubCLList( std::vector<std::string> &subCLLst );
         BOOLEAN isContainSubCL( const std::string &subCLName );
         std::string getMainCLName();
         INT32 addSubCL ( const CHAR *subCLName, const BSONObj &lowBound,
                        const BSONObj &upBound );

         INT32 delSubCL ( const CHAR *subCLName );

      protected:
         _clsCatalogSet    *next () ;
         INT32             next ( _clsCatalogSet * next ) ;

         void              _addGroupID ( UINT32 groupID ) ;
         void              _clear () ;
         void              _deduplicate () ;
         BOOLEAN           _isObjAllMaxKey ( BSONObj &obj ) ;
         INT32             _findItem( const clsCataItemKey &findKey,
                                      clsCatalogItem *& item ) ;
         INT32             _hash( const BSONObj &key ) ;
         void              _addSubClName( const std::string strClName );
         void              _clearSubClName();

      private:
         INT32             _splitItem( clsCatalogItem *item,
                                       clsCataItemKey *beginKey,
                                       clsCataItemKey *endKey,
                                       const BSONObj &beginKeyObj,
                                       const BSONObj &endKeyObj,
                                       UINT32 groupID,
                                       const CHAR *groupName ) ;

         INT32             _removeItem( clsCatalogItem *item ) ;
         INT32             _addItem( clsCatalogItem *item ) ;
         void              _remakeGroupIDs() ;
         /*INT32             _getBoundByRecord( const BSONObj &record,
                           BSONObj &bound );*/

      private:
         INT32             _version ;
         UINT32            _w ;
         BSONObj           _shardingKey ;
         UINT16            _shardingType ;
         bool              _ensureShardingIndex ;
         std::string       _name ;

         _clsCatalogSet    *_next ;
         clsCatalogItem    *_lastItem ;
         BOOLEAN           _isWholeRange ;

         MAP_CAT_ITEM      _mapItems ;
         VEC_GROUP_ID      _vecGroupID ;
         UINT32            _groupCount ;
         clsCataOrder      *_pOrder ;
         ixmIndexKeyGen    *_pKeyGen ;
         INT32             _partition ;
         UINT32            _square ;

         BOOLEAN           _saveName ;
         UINT32            _attribute ;
         std::vector<std::string> _subCLList ;
         BOOLEAN           _isMainCL ;
         std::string       _mainCLName;

   };
   typedef class _clsCatalogSet clsCatalogSet ;

   class _clsCatalogAgent : public SDBObject
   {
      typedef std::map<UINT32, _clsCatalogSet*>       CAT_MAP ;
      typedef CAT_MAP::iterator                       CAT_MAP_IT ;

      public:
         _clsCatalogAgent () ;
         ~_clsCatalogAgent () ;

      public:
         INT32   catVersion () ;
         INT32   collectionVersion ( const CHAR* name ) ;
         INT32   collectionW ( const CHAR * name ) ;
         INT32   collectionInfo ( const CHAR * name , INT32 &version, UINT32 &w ) ;
         void    getAllNames( std::vector<string> &names ) ;

         _clsCatalogSet *collectionSet ( const CHAR * name ) ;

         INT32   updateCatalog ( INT32 version, UINT32 groupID,
                                 const CHAR* objdata, UINT32 length,
                                 _clsCatalogSet **ppSet = NULL ) ;
         INT32   clear ( const CHAR* name ) ;
         INT32   clearBySpaceName ( const CHAR* name ) ;
         INT32   clearAll () ;

         INT32   lock_r ( INT32 millisec = -1 ) ;
         INT32   lock_w ( INT32 millisec = -1 ) ;
         INT32   release_r () ;
         INT32   release_w () ;

      protected:
         _clsCatalogSet * _addCollectionSet ( const CHAR * name ) ;

      private:
         CAT_MAP                       _mapCatalog ;
         INT32                         _catVersion ;
         ossRWMutex                    _rwMutex ;
   };
   typedef _clsCatalogAgent catAgent ;

   typedef _netRouteNode clsNoteItem ;

   class _clsNodeMgrAgent ;
   typedef std::vector<clsNoteItem>            VEC_NODE_INFO ;
   typedef VEC_NODE_INFO::iterator             VEC_NODE_INFO_IT ;
   typedef VEC_NODE_INFO::const_iterator       VEC_NODE_INFO_CIT ;

   class _clsGroupItem : public SDBObject
   {
      friend class _clsNodeMgrAgent ;

      public:
         _clsGroupItem ( UINT32 groupID ) ;
         ~_clsGroupItem () ;

         INT32  updateGroupItem( const BSONObj &obj ) ;

      protected:
         void   setGroupInfo ( const std::string& name, UINT32 version,
                               UINT32 primary ) ;
         INT32  updateNodes ( std::map<UINT64, _netRouteNode>& nodes ) ;
      public:
         UINT32 nodeCount () ;

         UINT32 groupID () const { return _groupID ; }
         INT32  groupVersion () const { return _groupVersion ; }
         std::string groupName() const { return _groupName ; }

         const VEC_NODE_INFO* getNodes () ;
         clsNoteItem*         nodeItem ( UINT32 nodeID ) ;
         INT32                nodePos  ( UINT32 nodeID ) ;

         INT32 getNodeID ( UINT32 pos, MsgRouteID& id,
                           MSG_ROUTE_SERVICE_TYPE type=
                           MSG_ROUTE_SHARD_SERVCIE ) ;

         INT32 getNodeID ( const std::string& hostName,
                           const std::string& serviceName,
                           MsgRouteID& id,
                           MSG_ROUTE_SERVICE_TYPE type =
                           MSG_ROUTE_SHARD_SERVCIE ) ;

         INT32 getNodeInfo ( UINT32 pos, MsgRouteID& id,
                             std::string& hostName, std::string& serviceName,
                             MSG_ROUTE_SERVICE_TYPE type =
                             MSG_ROUTE_SHARD_SERVCIE ) ;
         INT32 getNodeInfo ( const MsgRouteID& id,
                             std::string& hostName, std::string& serviceName ) ;

         INT32 getNodeInfo ( UINT32 pos, SINT32 &status );

         MsgRouteID  primary ( MSG_ROUTE_SERVICE_TYPE type =
                               MSG_ROUTE_SHARD_SERVCIE ) const ;

         UINT32 getPrimaryPos();

         INT32  updatePrimary ( const MsgRouteID& nodeID, BOOLEAN primary ) ;

         void   cancelPrimary () ;

         void updateNodeStat( UINT16 nodeID, NET_NODE_STATUS status );

      protected:
         void   _clear () ;

      private:
         UINT32                        _groupID ;
         std::string                   _groupName ;
         UINT32                        _groupVersion ;
         VEC_NODE_INFO                 _vecNodes ;
         MsgRouteID                    _primaryNode ;
         UINT32                        _primaryPos;
         ossAtomic32                   _errTime;

   };
   typedef _clsGroupItem clsGroupItem ;

   class _clsNodeMgrAgent : public SDBObject
   {
      typedef std::map<UINT32, clsGroupItem*>   GROUP_MAP ;
      typedef GROUP_MAP::iterator               GROUP_MAP_IT ;

      typedef std::map<std::string, UINT32>     GROUP_NAME_MAP ;
      typedef GROUP_NAME_MAP::iterator          GROUP_NAME_MAP_IT ;

      public:
         _clsNodeMgrAgent () ;
         ~_clsNodeMgrAgent () ;
      public:
         INT32       groupCount () ;

         INT32       groupVersion ( UINT32 id ) ;
         INT32       groupID2Name ( UINT32 id, std::string &name ) ;
         INT32       groupName2ID ( const CHAR* name, UINT32 &id ) ;
         INT32       groupNodeCount ( UINT32 id ) ;
         INT32       groupPrimaryNode ( UINT32 id, MsgRouteID &primary,
                                        MSG_ROUTE_SERVICE_TYPE type =
                                        MSG_ROUTE_SHARD_SERVCIE ) ;
         INT32       cancelPrimary( UINT32 id ) ;

         clsGroupItem* groupItem ( UINT32 id ) ;
         clsGroupItem* groupItem ( const CHAR* name ) ;

         INT32       clearAll () ;
         INT32       clearGroup ( UINT32 id ) ;

         INT32       updateGroupInfo ( const CHAR* objdata, UINT32 length,
                                       UINT32 *pGroupID = NULL ) ;

         INT32   lock_r ( INT32 millisec = -1 ) ;
         INT32   lock_w ( INT32 millisec = -1 ) ;
         INT32   release_r () ;
         INT32   release_w () ;

      protected:
         clsGroupItem* _addGroupItem ( UINT32 id ) ;
         INT32         _addGroupName ( const std::string& name, UINT32 id ) ;
         INT32         _clearGroupName ( UINT32 id ) ;

      private:
         ossRWMutex                    _rwMutex ;
         GROUP_MAP                     _groupMap ;
         GROUP_NAME_MAP                _groupNameMap ;

   };
   typedef _clsNodeMgrAgent nodeMgrAgent ;


   INT32    clsPartition( const BSONObj &keyObj, UINT32 partitionBit ) ;
   INT32    clsPartition( const bson::OID &oid, UINT32 sequence, UINT32 partitionBit ) ;

}

#endif //CLS_CATALOG_AGENT_HPP_

