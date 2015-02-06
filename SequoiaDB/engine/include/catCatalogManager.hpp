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

   Source File Name = catCatalogManager.hpp

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

#ifndef CATCATALOGUEMANAGER_HPP_
#define CATCATALOGUEMANAGER_HPP_

#include "pmd.hpp"
#include "catSplit.hpp"

using namespace bson ;

namespace engine
{

   class _dpsLogWrapper ;
   class sdbCatalogueCB ;
   class _SDB_DMSCB ;

   enum CAT_ASSIGNGROUP_TYPE
   {
      ASSIGN_FOLLOW     = 1,
      ASSIGN_RANDOM     = 2
   } ;

   #define CAT_MASK_CLNAME          0x00000001
   #define CAT_MASK_SHDKEY          0x00000002
   #define CAT_MASK_REPLSIZE        0x00000004
   #define CAT_MASK_SHDIDX          0x00000008
   #define CAT_MASK_SHDTYPE         0x00000010
   #define CAT_MASK_SHDPARTITION    0x00000020
   #define CAT_MASK_COMPRESSED      0x00000040
   #define CAT_MASK_ISMAINCL        0x00000080
   #define CAT_MASK_AUTOASPLIT      0x00000100
   #define CAT_MASK_AUTOREBALAN     0x00000200
   #define CAT_MASK_AUTOINDEXID     0x00000400

   struct _catCollectionInfo
   {
      const CHAR  *_pCLName ;
      BSONObj     _shardingKey ;
      INT32       _replSize ;
      BOOLEAN     _enSureShardIndex ;
      const CHAR  *_pShardingType ;
      INT32       _shardPartition ;
      BOOLEAN     _isHash ;
      BOOLEAN     _isSharding ;
      BOOLEAN     _isCompressed ;
      BOOLEAN     _isMainCL;
      BOOLEAN     _autoSplit ;
      BOOLEAN     _autoRebalance ;
      const CHAR * _gpSpecified ;
      INT32       _version ;
      INT32       _assignType ;
      BOOLEAN     _autoIndexId ;
      
      std::vector<std::string>   _subCLList;

      _catCollectionInfo()
      {
         _pCLName             = NULL ;
         _replSize            = 1 ;
         _enSureShardIndex    = TRUE ;
         _pShardingType       = CAT_SHARDING_TYPE_RANGE ;
         _shardPartition      = CAT_SHARDING_PARTITION_DEFAULT ;
         _isHash              = FALSE ;
         _isSharding          = FALSE ;
         _isCompressed        = FALSE ;
         _isMainCL            = FALSE ;
         _autoSplit           = FALSE ;
         _autoRebalance       = FALSE ;
         _gpSpecified         = NULL ;
         _version             = 0 ;
         _assignType          = ASSIGN_RANDOM ;
         _autoIndexId         = TRUE ;
      }
   };
   typedef _catCollectionInfo catCollectionInfo ;

   struct _catCSInfo
   {
      const CHAR  *_pCSName ;
      INT32       _pageSize ;
      const CHAR  *_domainName ;
      INT32       _lobPageSize ;

      _catCSInfo()
      {
         _pCSName = NULL ;
         _pageSize = DMS_PAGE_SIZE_DFT ;
         _domainName = NULL ;
         _lobPageSize = DMS_DEFAULT_LOB_PAGE_SZ ;
      }

      BSONObj toBson()
      {
         BSONObjBuilder builder ;
         builder.append( CAT_COLLECTION_SPACE_NAME, _pCSName ) ;
         builder.append( CAT_PAGE_SIZE_NAME, _pageSize ) ;
         if ( _domainName )
         {
            builder.append( CAT_DOMAIN_NAME, _domainName ) ;
         }
         builder.append( CAT_LOB_PAGE_SZ_NAME, _lobPageSize ) ;
         return builder.obj() ;
      }
   } ;
   typedef _catCSInfo catCSInfo ;

   /*
      catCatalogueManager define
   */
   class catCatalogueManager : public SDBObject
   {
   public:
      catCatalogueManager() ;
      INT32 init() ;

      void  attachCB( _pmdEDUCB *cb ) ;
      void  detachCB( _pmdEDUCB *cb ) ;

      INT32 processMsg( const NET_HANDLE &handle, MsgHeader *pMsg ) ;

      INT32 active() ;
      INT32 deactive() ;

   protected:
      INT32 processCommandMsg( const NET_HANDLE &handle, MsgHeader *pMsg,
                               BOOLEAN writable ) ;

      INT32 processCmdCreateCL( const CHAR *pQuery,
                                CHAR **ppReplyBody,
                                UINT32 &replyBodyLen,
                                INT32 &returnNum ) ;
      INT32 processCmdCreateCS( const CHAR *pQuery,
                                CHAR **ppReplyBody,
                                UINT32 &replyBodyLen,
                                INT32 &returnNum ) ;
      INT32 processCmdSplit( const CHAR *pQuery,
                             INT32 opCode,
                             CHAR **ppReplyBody,
                             UINT32 &replyBodyLen,
                             INT32 &returnNum ) ;
      INT32 processCmdQuerySpaceInfo( const CHAR *pQuery,
                                      CHAR **ppReplyBody,
                                      UINT32 &replyBodyLen,
                                      INT32 &returnNum ) ;
      INT32 processCmdDropCollection ( const CHAR *pQuery,
                                       INT32 version = -1 ) ;
      INT32 processCmdDropCollectionSpace ( const CHAR *pQuery ) ;

      INT32 processQueryCatalogue ( const NET_HANDLE &handle,
                                    MsgHeader *pMsg ) ;
      INT32 processQueryTask ( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 processAlterCollection ( void *pMsg,
                                     CHAR **ppReplyBody,
                                     UINT32 &replyBodyLen,
                                     INT32 &returnNum ) ;
      INT32 processCmdCrtProcedures( void *pMsg ) ;
      INT32 processCmdRmProcedures( void *pMsg ) ;
      INT32 processCmdLinkCollection( const CHAR *pQuery,
                                    CHAR **ppReplyBody,
                                    UINT32 &replyBodyLen,
                                    INT32 &returnNum );
      INT32 processCmdUnlinkCollection( const CHAR *pQuery,
                                       CHAR **ppReplyBody,
                                       UINT32 &replyBodyLen,
                                       INT32 &returnNum );
      INT32 processCmdCreateDomain ( const CHAR *pQuery ) ;
      INT32 processCmdDropDomain ( const CHAR *pQuery ) ;
      INT32 processCmdAlterDomain ( const CHAR *pQuery ) ;

   protected:
      void  _fillRspHeader( MsgHeader *rspMsg, const MsgHeader *reqMsg ) ;
      INT32 _sendFailedRsp( NET_HANDLE handle, INT32 res, MsgHeader *reqMsg) ;

      INT32 _createCL( BSONObj & createObj, INT32 &groupID,
                       std::vector<UINT64> &taskIDs ) ;
      INT32 _createCS( BSONObj & createObj, INT32 &groupID ) ;

      INT32 _checkAndBuildCataRecord( const BSONObj &infoObj,
                                      UINT32 &fieldMask,
                                      catCollectionInfo &clInfo,
                                      BOOLEAN clNameIsNecessary = TRUE ) ;
      INT32 _checkCSObj( const BSONObj &infoObj,
                         catCSInfo &csInfo ) ;

      INT32 _checkGroupInDomain( const CHAR *groupName,
                                 const CHAR *domainName,
                                 BOOLEAN &existed,
                                 INT32 *pGroupID = NULL ) ;
      INT32 _assignGroup( vector< INT32 > *pGoups, INT32 &groupID ) ;

      INT32 _buildCatalogRecord( const catCollectionInfo &clInfo,
                                 UINT32 mask,
                                 INT32 groupID,
                                 const CHAR *groupName,
                                 BSONObj &catRecord ) ;

      INT32 _chooseGroupOfCl( const BSONObj &domainObj,
                              const BSONObj &csObj,
                              const catCollectionInfo &clInfo,
                              std::string &groupName,
                              INT32 &groupID,
                              std::map<string, INT32> &splitRange ) ;

      INT32 _autoHashSplit( const BSONObj &clObj, std::vector<UINT64> &taskIDs,
                            const CHAR *srcGroupName = NULL,
                            const map<string, INT32> *dstIDs = NULL ) ;

      INT32 _combineOptions( const BSONObj &domain,
                             const BSONObj &cs,
                             UINT32 &mask,
                             catCollectionInfo &options  ) ;

      BSONObj _crtSplitInfo( const CHAR *fullName,
                             const CHAR *src,
                             const CHAR *dst,
                             UINT32 begin,
                             UINT32 end ) ;

      INT32 _buildAlterObjWithMetaAndObj( _clsCatalogSet &catSet,
                                          UINT32 mask,
                                          catCollectionInfo &alterInfo,
                                          BSONObj &alterObj ) ;

      INT32 _getGroupsOfCollections( const std::vector<string> &clNames,
                                     BSONObj &groups  ) ;
   private:
      INT32 _buildInitBound ( UINT32 fieldNum,
                              const Ordering& order,
                              BSONObj& lowBound,
                              BSONObj& upBound ) ;

      INT32 _buildHashBound( BSONObj& lowBound,
                             BSONObj& upBound,
                             INT32 paritition ) ;

      INT16 _majoritySize() ;

      INT32 _buildAlterGroups( const BSONObj &domain,
                               const BSONElement &ele,
                               BSONObjBuilder &builder ) ;
   private:
      sdbCatalogueCB       *_pCatCB;
      _SDB_DMSCB           *_pDmsCB;
      _dpsLogWrapper       *_pDpsCB;
      pmdEDUCB             *_pEduCB;
      clsTaskMgr           _taskMgr ;

   } ;
}

#endif // CATCATALOGUEMANAGER_HPP_

