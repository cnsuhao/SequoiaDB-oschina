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

   Source File Name = catNodeManager.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =     XJH Opt

*******************************************************************************/
#ifndef CATNODEMANAGER_HPP__
#define CATNODEMANAGER_HPP__

#include "pmd.hpp"
#include "netDef.hpp"

using namespace bson ;

namespace engine
{
   class sdbCatalogueCB ;
   class _SDB_RTNCB ;
   class _dpsLogWrapper ;
   class _SDB_DMSCB ;

   /*
      catNodeManager define
   */
   class catNodeManager : public SDBObject
   {
   public:
      catNodeManager() ;
      virtual ~catNodeManager() ;
      INT32 init() ;

      void  attachCB( _pmdEDUCB *cb ) ;
      void  detachCB( _pmdEDUCB *cb ) ;

      INT32 processMsg( const NET_HANDLE &handle, MsgHeader *pMsg ) ;

      INT32 active() ;
      INT32 deactive() ;

   protected:
      INT32 processCommandMsg( const NET_HANDLE &handle, MsgHeader *pMsg,
                               BOOLEAN writable ) ;

      INT32 processCmdCreateGrp( const CHAR *pQuery ) ;
      INT32 processCmdCreateNode( const NET_HANDLE &handle,
                                  const CHAR *pQuery ) ;
      INT32 processCmdUpdateNode( const NET_HANDLE &handle,
                                  const CHAR *pQuery,
                                  const CHAR *pSelector ) ;
      INT32 processCmdDelNode( const NET_HANDLE &handle,
                               const CHAR *pQuery ) ;

      INT32 processGrpReq( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 processRegReq( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 processPrimaryChange( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 processRemoveGrp( const NET_HANDLE &handle, MsgHeader *pMsg ) ;
      INT32 processActiveGrp( const NET_HANDLE &handle, MsgHeader *pMsg );

      INT32 readCataConf();
      INT32 parseCatalogConf( CHAR *pData, const SINT64 sDataSize,
                              SINT64 &sParseBytes );
      INT32 parseLine( const CHAR *pLine, BSONObj &obj );
      INT32 generateGroupInfo( bson::BSONObj &boConf,
                               bson::BSONObj &boGroupInfo );
      INT32 saveGroupInfo ( bson::BSONObj &boGroupInfo, INT16 w );
      INT32 parseIDInfo( bson::BSONObj &obj );
      INT32 getNodeInfo( const bson::BSONObj &boReq,
                         bson::BSONObj &boNodeInfo );
      INT32 removeGrp( const CHAR *groupName ) ;
      INT32 activeGrp( const std::string &strGroupName,
                       UINT32 groupID,
                       bson::BSONObj &boGroupInfo );

      INT32 _count( const CHAR *collection, const BSONObj &matcher,
                    UINT64 &count ) ;

   protected:
      void  _fillRspHeader( MsgHeader *rspMsg, const MsgHeader *reqMsg ) ;
      INT32 _sendFailedRsp( NET_HANDLE handle, INT32 res, MsgHeader *reqMsg) ;

   private:
      INT32 _createGrp( const CHAR *groupName ) ;
      INT32 _createNode( const CHAR *pQuery, BOOLEAN isLoalConn ) ;
      INT32 _delNode( BSONObj &boDelNodeInfo, BOOLEAN isLoalConn ) ;
      INT32 _addNodeToGrp ( BSONObj &boGroupInfo, BSONObj &boNodeInfo,
                            UINT16 nodeID ) ;
      INT32 _updateNodeToGrp ( BSONObj &boGroupInfo, BSONObj &boNodeInfoNew,
                               UINT16 nodeID, BOOLEAN isLoalConn ) ;
      INT32 _getRemovedGroupsObj( const BSONObj &srcGroupsObj,
                                  UINT16 removeNode,
                                  BSONObj &removedObj,
                                  BSONArrayBuilder &newObjBuilder ) ;
      INT32 _getRemovedGroupsObj( const BSONObj &srcGroupsObj,
                                  const CHAR *hostName,
                                  const CHAR *serviceName,
                                  BSONArrayBuilder &newObjBuilder,
                                  INT32 *pRemoveNodeID = NULL ) ;
      INT32 _checkNodeInfo( BSONObj &boNodeInfo, INT32 nodeRole,
                            BSONObjBuilder *newObjBuilder = NULL ) ;
      string _getServiceName( UINT16 localPort, _MSG_ROUTE_SERVICE_TYPE type ) ;
      INT16 _majoritySize() ;

      INT32 _getNodeInfoByConf( BSONObj &boConf, BSONObjBuilder &bobNodeInfo ) ;
      INT32 _checkLocalHost( BOOLEAN &isValid ) ;

   private:
      typedef enum _SDB_CAT_MODULE_STATUS
      {
         SDB_CAT_MODULE_ACTIVE    =  0,
         SDB_CAT_MODULE_DEACTIVE
      }SDB_CAT_MODULE_STATUS;

   private:
      SDB_CAT_MODULE_STATUS      _status;
      _SDB_DMSCB                 *_pDmsCB;
      _dpsLogWrapper             *_pDpsCB;
      _SDB_RTNCB                 *_pRtnCB;
      sdbCatalogueCB             *_pCatCB;
      pmdEDUCB                   *_pEduCB;

   } ;
}

#endif // CATNODEMANAGER_HPP__

