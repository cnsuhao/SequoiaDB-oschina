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

   Source File Name = coordDef.hpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/28/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef COORDDEF_HPP__
#define COORDDEF_HPP__

#include "clsCatalogAgent.hpp"

namespace engine
{
   typedef std::map< UINT32, UINT32 >     CoordGroupList ;
   typedef clsNoteItem                    CoordNodeInfo ;
   typedef VEC_NODE_INFO                  CoordVecNodeInfo ;

   class _CoordGroupInfo : public SDBObject
   {
   public:
      _CoordGroupInfo ( UINT32 groupID ) ;
      ~_CoordGroupInfo () ;

      INT32 fromBSONObj( const bson::BSONObj &boGroupInfo );

      void  setPrimary( const MsgRouteID &ID ) ;
      void  setSlave( const MsgRouteID &ID ) ;

      MsgRouteID getPrimary( MSG_ROUTE_SERVICE_TYPE type =
                             MSG_ROUTE_SHARD_SERVCIE )
      {
         ossScopedRWLock lock( &_primaryMutex, SHARED ) ;
         return _groupItem.primary( type ) ;
      }

      UINT32 getGroupID() const
      {
         return _groupItem.groupID() ;
      }

      string groupName() const
      {
         return _groupItem.groupName() ;
      }

      UINT32 getGroupSize()
      {
         return _groupItem.nodeCount() ;
      }

      clsGroupItem* getGroupItem()
      {
         return &_groupItem ;
      }

   private:
      clsGroupItem               _groupItem ;
      ossRWMutex                 _primaryMutex ;

   };
   typedef _CoordGroupInfo CoordGroupInfo ;

   typedef boost::shared_ptr<CoordGroupInfo>    CoordGroupInfoPtr;
   typedef std::map<UINT32, CoordGroupInfoPtr>  CoordGroupMap;
   typedef std::vector<std::string>             CoordSubCLlist;
   typedef std::map<UINT32, CoordSubCLlist>     CoordGroupSubCLMap;

   class _CoordCataInfo : public SDBObject
   {
   public:
      _CoordCataInfo( INT32 version, const char *pCollectionName )
      :_catlogSet ( pCollectionName, FALSE )
      {}

      ~_CoordCataInfo()
      {}

      void getGroupLst( CoordGroupList &groupLst )
      {
         groupLst = _groupLst ;
      }

      BOOLEAN isMainCL()
      {
         return _catlogSet.isMainCL();
      }

      INT32 getSubCLList( CoordSubCLlist &subCLLst )
      {
         return _catlogSet.getSubCLList( subCLLst );
      }

      BOOLEAN isContainSubCL( const std::string &subCLName )
      {
         return _catlogSet.isContainSubCL( subCLName );
      }

      INT32 getGroupByMatcher( const bson::BSONObj & matcher,
                              CoordGroupList &groupLst )
      {
         INT32 rc = SDB_OK;
         UINT32 i = 0;
         VEC_GROUP_ID vecGroup;
         rc = _catlogSet.findGroupIDS( matcher, vecGroup );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to find the match groups(rc=%d)",
                     rc );
         for ( ; i < vecGroup.size(); i++ )
         {
            groupLst[vecGroup[i]] = vecGroup[i];
         }
      done:
         return rc;
      error:
         goto done;
      }

      INT32 getGroupByRecord( const bson::BSONObj &recordObj, UINT32 &groupID )
      {
         return _catlogSet.findGroupID ( recordObj, groupID ) ;
      }

      INT32 getSubCLNameByRecord( const bson::BSONObj &recordObj, std::string &subCLName )
      {
         return _catlogSet.findSubCLName( recordObj, subCLName );
      }

      INT32 getMatchGroups( const bson::BSONObj &matcher,
                           CoordGroupList &groupLst );

      INT32 getMatchSubCLs( const bson::BSONObj &matcher,
                           CoordSubCLlist &subCLList )
      {
         return _catlogSet.findSubCLNames( matcher, subCLList );
      }

      INT32 getVersion()
      {
         return _catlogSet.getVersion() ;
      }
      INT32 fromBSONObj ( const bson::BSONObj &boRecord )
      {
         INT32 rc = _catlogSet.updateCatSet ( boRecord, 0 ) ;
         if ( SDB_OK == rc )
         {
            UINT32 groupID = 0 ;
            VEC_GROUP_ID *vecGroup = _catlogSet.getAllGroupID() ;
            for ( UINT32 index = 0 ; index < vecGroup->size() ;++index )
            {
               groupID = (*vecGroup)[index] ;
               _groupLst[groupID] = groupID ;
            }
         }
         return rc ;
      }

      void getShardingKey ( bson::BSONObj &shardingKey )
      {
         shardingKey = _catlogSet.getShardingKey() ;
      }

      BOOLEAN isIncludeShardingKey( const bson::BSONObj &record )
      {
         return _catlogSet.isIncludeShardingKey( record );
      }

      BOOLEAN isSharded ()
      {
         return _catlogSet.isSharding() ;
      }

      BOOLEAN isRangeSharded() const
      {
         return _catlogSet.isRangeSharding() ;
      }

      INT32 getGroupLowBound( UINT32 groupID, BSONObj &lowBound )
      {
         return _catlogSet.getGroupLowBound( groupID, lowBound ) ;
      }

      clsCatalogSet* getCatalogSet()
      {
         return &_catlogSet ;
      }

      INT32 getLobGroupID( const bson::OID &oid,
                           UINT32 sequence,
                           UINT32 &groupID )
      {
         return _catlogSet.findGroupID( oid, sequence, groupID ) ;
      }

   private:
      _CoordCataInfo()
      :_catlogSet( NULL, FALSE ) 
      {}  

   private:
      clsCatalogSet        _catlogSet ;
      CoordGroupList       _groupLst ;

   };
   typedef _CoordCataInfo CoordCataInfo ;

   typedef boost::shared_ptr< CoordCataInfo >         CoordCataInfoPtr ;
   typedef std::map< std::string, CoordCataInfoPtr >  CoordCataMap ;
}

#endif
