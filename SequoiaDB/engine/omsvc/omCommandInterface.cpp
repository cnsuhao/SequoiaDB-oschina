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

   Source File Name = omCommandInterface.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/12/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omCommandInterface.hpp"
#include "omDef.hpp"
#include "rtn.hpp"
#include "msgMessage.hpp"


using namespace bson;

namespace engine
{
   omCommandInterafce::omCommandInterafce()
   {
   }

   omCommandInterafce::~omCommandInterafce()
   {
   }

   omRestCommandBase::omRestCommandBase()
   {
      _pKRCB  = pmdGetKRCB() ;
      _pDMDCB = _pKRCB->getDMSCB() ;
      _pRTNCB = _pKRCB->getRTNCB() ;
      _pDMSCB = _pKRCB->getDMSCB() ;
      _cb     = NULL ;
   }

   omRestCommandBase::~omRestCommandBase()
   {
   }

   INT32 omRestCommandBase::init( pmdEDUCB * cb )
   {
      _cb = cb ;

      return SDB_OK ;
   }

   bool omRestCommandBase::isFetchAgentResponse( UINT64 requestID )
   {
      return false ;
   }

   INT32 omRestCommandBase::doAgentResponse ( MsgHeader* pAgentResponse )
   {
      return SDB_OK ;
   }

   INT32 omRestCommandBase::_getBusinessInfo( string business, 
                                              BSONObj &businessInfo )
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;

      matcher = BSON( OM_BUSINESS_FIELD_NAME << business ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_BUSINESS, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d",
                     OM_CS_DEPLOY_CL_BUSINESS, rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      while ( TRUE )
      {
         BSONObjBuilder innerBuilder ;
         BSONObj tmp ;
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_BUSINESS, rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         businessInfo = result.copy() ;
         break ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omRestCommandBase::_getHostInfo( string hostName, 
                                           BSONObj &hostInfo )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      SINT64 contextID = -1 ;

      matcher = BSON( OM_HOST_FIELD_NAME << hostName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 0, 
                     _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "fail to query table:%s,rc=%d",
                     OM_CS_DEPLOY_CL_HOST, rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            contextID = -1 ;
            PD_LOG_MSG( PDERROR, "failed to get record from table:%s,rc=%d", 
                        OM_CS_DEPLOY_CL_HOST, rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }

         BSONObj record( buffObj.data() ) ;
         BSONObj filter = BSON( OM_HOST_FIELD_PASSWORD << "" ) ;
         BSONObj result = record.filterFieldsUndotted( filter, false ) ;
         hostInfo = result.copy() ;
         break ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   /**
   * get the host's disk info. the host is specific by the hostNameList
   * if hostNameList's size is 0, get all the cluster(clusterName )'s hosts 
     instead
   * @param clusterName the cluster's name
   * @param hostNameList the specific hosts
   * @param hostInfoList the result of  disk infos
   * @return SDB_OK if success; otherwise failure
   */
   INT32 omRestCommandBase::_fetchHostDiskInfo( const string &clusterName, 
                                            list<string> &hostNameList, 
                                            list<simpleHostDisk> &hostInfoList )
   {
      BSONObj matcher ;

      if ( hostNameList.size() > 0 )
      {
         BSONArrayBuilder arrayBuilder ;
         list<string>::iterator iterList = hostNameList.begin() ;
         while ( iterList != hostNameList.end() )
         {
            BSONObj tmp = BSON( OM_HOST_FIELD_NAME << *iterList ) ;
            arrayBuilder.append( tmp ) ;
            iterList++ ;
         }
         matcher = BSON( OM_HOST_FIELD_CLUSTERNAME << clusterName 
                         << "$or" << arrayBuilder.arr() ) ;
      }
      else
      {
         matcher = BSON( OM_HOST_FIELD_CLUSTERNAME << clusterName  ) ;
      }

      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_HOST, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         _errorDetail = string( "fail to query table:" ) 
                        + OM_CS_DEPLOY_CL_HOST ;
         PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               rc = SDB_OK ;
               break ;
            }

            contextID = -1 ;
            _errorDetail = string( "failed to get record from table:" )
                           + OM_CS_DEPLOY_CL_HOST ;
            PD_LOG( PDERROR, "%s,rc=%d", _errorDetail.c_str(), rc ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         BSONObj diskArray ;
         simpleHostDisk hostDisk ;
         hostDisk.hostName  = result.getStringField( OM_HOST_FIELD_NAME ) ;
         hostDisk.user      = result.getStringField( OM_HOST_FIELD_USER ) ;
         hostDisk.passwd    = result.getStringField( OM_HOST_FIELD_PASSWORD ) ;
         hostDisk.agentPort = result.getStringField( OM_HOST_FIELD_AGENT_PORT ) ;
         diskArray = result.getObjectField( OM_HOST_FIELD_DISK ) ;
         {
            BSONObjIterator iter( diskArray ) ;
            while ( iter.more() )
            {
               BSONElement ele = iter.next() ;
               BSONObj oneDisk = ele.embeddedObject() ;
               simpleDiskInfo diskInfo ;
               diskInfo.diskName  = oneDisk.getStringField( 
                                                    OM_HOST_FIELD_DISK_NAME ) ;
               diskInfo.mountPath = oneDisk.getStringField( 
                                                    OM_HOST_FIELD_DISK_MOUNT ) ;
               BSONElement eleNum ;
               eleNum = oneDisk.getField( OM_HOST_FIELD_DISK_SIZE ) ;
               diskInfo.totalSize = eleNum.numberLong() ;
               eleNum = oneDisk.getField( OM_HOST_FIELD_DISK_FREE_SIZE ) ;
               diskInfo.freeSize  = eleNum.numberLong() ;
               hostDisk.diskInfo.push_back( diskInfo ) ;
            }
         }

         hostInfoList.push_back( hostDisk ) ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omRestCommandBase::_deleteHost( const string &hostName )
   {
      INT32 rc          = SDB_OK ;
      BSONObj condition = BSON( OM_HOST_FIELD_NAME << hostName ) ;
      BSONObj hint ;

      rc = rtnDelete( OM_CS_DEPLOY_CL_HOST, condition, hint, 0, _cb );
      if ( rc )
      {
         PD_LOG_MSG( PDERROR, "failed to delete record from table:%s,"
                     "%s=%s,rc=%d", OM_CS_DEPLOY_CL_HOST, 
                     OM_HOST_FIELD_NAME, hostName.c_str(), rc ) ;
         _errorDetail = _cb->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRestCommandBase::_receiveFromAgent( pmdRemoteSession *remoteSession,
                                               SINT32 &flag,
                                               BSONObj &result )
   {
      VEC_SUB_SESSIONPTR subSessionVec ;
      INT32 rc           = SDB_OK ;
      MsgHeader *pRspMsg = NULL ;
      SINT64 contextID   = -1 ;
      SINT32 startFrom   = 0 ;
      SINT32 numReturned = 0 ;
      vector<BSONObj> objVec ;

      rc = remoteSession->waitReply( TRUE, &subSessionVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "wait reply failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( 1 != subSessionVec.size() )
      {
         rc = SDB_UNEXPECTED_RESULT ;
         PD_LOG( PDERROR, "unexpected session size:size=%d", 
                 subSessionVec.size() ) ;
         goto error ;
      }

      if ( subSessionVec[0]->isDisconnect() )
      {
         rc = SDB_UNEXPECTED_RESULT ;
         PD_LOG(PDERROR, "session disconnected:id=%s,rc=%d", 
                routeID2String(subSessionVec[0]->getNodeID()).c_str(), rc ) ;
         goto error ;
      }

      pRspMsg = subSessionVec[0]->getRspMsg() ;
      if ( NULL == pRspMsg )
      {
         rc = SDB_UNEXPECTED_RESULT ;
         PD_LOG( PDERROR, "receive null response:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgExtractReply( (CHAR *)pRspMsg, &flag, &contextID, &startFrom, 
                            &numReturned, objVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "extract reply failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( objVec.size() > 0 )
      {
         result = objVec[0] ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRestCommandBase::_getAllReplay( pmdRemoteSession *remoteSession, 
                                           VEC_SUB_SESSIONPTR *subSessionVec )
   {
      SDB_ASSERT( NULL != remoteSession, "remoteSession can't be null" ) ;

      pmdSubSessionItr itr( NULL ) ;
      INT32 rc = SDB_OK ;
      VEC_SUB_SESSIONPTR tmpSessionVec ;
      rc = remoteSession->waitReply( TRUE, &tmpSessionVec ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "wait replay failed:rc=%d", rc ) ;
         goto error ;
      }

      itr = remoteSession->getSubSessionItr( PMD_SSITR_ALL ) ;
      while ( itr.more() )
      {
         subSessionVec->push_back( itr.next() ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omRestCommandBase::_getClusterInfo( const string &clusterName, 
                                             BSONObj &clusterInfo )
   {
      BSONObjBuilder bsonBuilder ;
      BSONObj selector ;
      BSONObj matcher ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj result ;
      SINT64 contextID = -1 ;
      INT32 rc         = SDB_OK ;

      matcher = BSON( OM_CLUSTER_FIELD_NAME << clusterName ) ;
      rc = rtnQuery( OM_CS_DEPLOY_CL_CLUSTER, selector, matcher, order, hint, 
                     0, _cb, 0, -1, _pDMSCB, _pRTNCB, contextID );
      if ( rc )
      {
         PD_LOG( PDERROR, "failed to get record from table:%s,rc=%d", 
                    OM_CS_DEPLOY_CL_CLUSTER, rc ) ;
         _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
         goto error ;
      }

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, 1, buffObj, _cb, _pRTNCB ) ;
         if ( rc )
         {
            contextID = -1 ;
            PD_LOG( PDERROR, "failed to get record from table:%s,rc=%d", 
                    OM_CS_DEPLOY_CL_CLUSTER, rc ) ;
            _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
            goto error ;
         }

         BSONObj result( buffObj.data() ) ;
         clusterInfo = result.copy() ;
         break ;
      }
   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete ( contextID, _cb ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 omRestCommandBase::_checkHostBasicContent( BSONObj &oneHost )
   {
      INT32 rc = SDB_INVALIDARG ;
      BSONElement ele ;

      ele = oneHost.getField( OM_BSON_FIELD_HOST_IP ) ;
      if ( ele.type() != String )
      {
         PD_LOG_MSG( PDERROR, "field is not String type:field=%s,type=%d", 
                     OM_BSON_FIELD_HOST_IP, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_HOST_NAME ) ;
      if ( ele.type() != String )
      {
         PD_LOG_MSG( PDERROR, "field is not String type:field=%s,type=%d", 
                     OM_BSON_FIELD_HOST_NAME, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_OS ) ;
      if ( ele.type() != Object )
      {
         PD_LOG_MSG( PDERROR, "field is not Object type:field=%s,type=%d", 
                     OM_BSON_FIELD_OS, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_OMA ) ;
      if ( ele.type() != Object )
      {
         PD_LOG_MSG( PDERROR, "field is not Object type:field=%s,type=%d", 
                     OM_BSON_FIELD_OMA, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_CPU ) ;
      if ( ele.type() != Array )
      {
         PD_LOG_MSG( PDERROR, "field is not Array type:field=%s,type=%d", 
                     OM_BSON_FIELD_CPU, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_MEMORY ) ;
      if ( ele.type() != Object )
      {
         PD_LOG_MSG( PDERROR, "field is not Object type:field=%s,type=%d", 
                     OM_BSON_FIELD_MEMORY, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_NET ) ;
      if ( ele.type() != Array )
      {
         PD_LOG_MSG( PDERROR, "field is not Array type:field=%s,type=%d", 
                     OM_BSON_FIELD_NET, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_PORT ) ;
      if ( ele.type() != Array )
      {
         PD_LOG_MSG( PDERROR, "field is not Array type:field=%s,type=%d", 
                     OM_BSON_FIELD_PORT, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_SAFETY ) ;
      if ( ele.type() != Object )
      {
         PD_LOG_MSG( PDERROR, "field is not Object type:field=%s,type=%d", 
                     OM_BSON_FIELD_SAFETY, ele.type() ) ;
         goto error ;
      }

      ele = oneHost.getField( OM_BSON_FIELD_DISK ) ;
      if ( ele.type() != Array )
      {
         PD_LOG_MSG( PDERROR, "field is not Array type:field=%s,type=%d", 
                     OM_BSON_FIELD_DISK, ele.type() ) ;
         goto error ;
      }

      rc = SDB_OK ;
   done:
      return rc ;
   error:
      goto done ;
   }


   omAgentReqBase::omAgentReqBase( BSONObj &request )
                  :_request( request.copy() ), _response( BSONObj() )
   {
   }

   omAgentReqBase::~omAgentReqBase()
   {
   }

   void omAgentReqBase::getResponse( BSONObj &response )
   {
      _response = response ;
   }

}

