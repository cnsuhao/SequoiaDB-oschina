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

   Source File Name = rtnCoordOpenLob.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/08/2014  YW  Initial Draft
   Last Changed =

*******************************************************************************/

#include "rtnCoordLob.hpp"
#include "rtnLob.hpp"
#include "rtnTrace.hpp"
#include "msgMessage.hpp"
#include "rtnCoordLobStream.hpp"

namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOORDOPENLOB_EXECUTE, "rtnCoordOpenLob::execute" )
   INT32 rtnCoordOpenLob::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                   CHAR **ppResultBuffer, pmdEDUCB *cb,
                                   MsgOpReply &replyHeader,
                                   BSONObj** ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOORDOPENLOB_EXECUTE ) ;
      const MsgOpLob *header = NULL ;
      const MsgHeader *baseHeader = ( const MsgHeader * )pReceiveBuffer ;
      BSONObj obj ;
      BSONObj meta ;
      SINT64 contextID = -1 ;
      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode = MSG_BS_LOB_OPEN_RES ;
      replyHeader.header.requestID = baseHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID = baseHeader->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      rc = msgExtractOpenLobRequest( pReceiveBuffer, &header, obj ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract open msg:%d", rc ) ;
         goto error ;
      }

      rc = rtnOpenLob( obj, 0, FALSE, cb,
                       NULL, 0, contextID,
                       meta ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to open lob:%s, rc:%d",
                 obj.toString( FALSE, TRUE ).c_str(), rc ) ;
         goto error ;
      }

      replyHeader.contextID = contextID ;
      *ppResultBuffer = ( CHAR * )( meta.objdata() ) ;  /// TODO: ppResultBuffer should be const char **
      replyHeader.header.messageLength += meta.objsize() ;
   done:
      PD_TRACE_EXITRC( SDB_RTNCOORDOPENLOB_EXECUTE, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOORDWRITELOB_EXECUTE, "rtnCoordWriteLob::execute" )
   INT32 rtnCoordWriteLob::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                    CHAR **ppResultBuffer, pmdEDUCB *cb,
                                    MsgOpReply &replyHeader,
                                    BSONObj** ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOORDWRITELOB_EXECUTE ) ;
      const MsgOpLob *header = NULL ;
      const MsgHeader *baseHeader = ( const MsgHeader * )pReceiveBuffer ;
      BSONObj obj ;
      UINT32 len = 0 ;
      SINT64 offset = -1 ;
      const CHAR *data = NULL ;

      replyHeader.header.messageLength = sizeof( MsgOpReply );
      replyHeader.header.opCode = MSG_BS_LOB_WRITE_RES ;
      replyHeader.header.requestID = baseHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID = baseHeader->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      rc = msgExtractWriteLobRequest( pReceiveBuffer, &header, &len,
                                      &offset, &data ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract msg:%d", rc ) ;
         goto error ;
      }

      rc = rtnWriteLob( header->contextID, cb, len, data ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to write lob:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNCOORDWRITELOB_EXECUTE, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOORDREADLOB_EXECUTE, "rtnCoordReadLob::execute" )
   INT32 rtnCoordReadLob::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                   CHAR **ppResultBuffer, pmdEDUCB *cb,
                                   MsgOpReply &replyHeader,
                                   BSONObj** ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOORDREADLOB_EXECUTE ) ;
      const MsgOpLob *header = NULL ;
      const MsgHeader *baseHeader = ( const MsgHeader * )pReceiveBuffer ;
      BSONObj obj ;
      UINT32 len = 0 ;
      SINT64 offset = -1 ;
      const CHAR *data = NULL ;
      UINT32 readLen = 0 ;

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode = MSG_BS_LOB_READ_RES ;
      replyHeader.header.requestID = baseHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID = baseHeader->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      rc = msgExtractReadLobRequest( pReceiveBuffer, &header, &len,
                                     &offset ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract msg:%d", rc ) ;
         goto error ;
      }

      rc = rtnReadLob( header->contextID, cb, len,
                       offset, &data, readLen ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read lob:%d", rc ) ;
         goto error ;   
      }

      *ppResultBuffer = ( CHAR * )data ;
      replyHeader.header.messageLength += readLen ;
   done:
      PD_TRACE_EXITRC( SDB_RTNCOORDREADLOB_EXECUTE, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOORDCLOSELOB_EXECUTE, "rtnCoordCloseLob::execute" )
   INT32 rtnCoordCloseLob::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                    CHAR **ppResultBuffer, pmdEDUCB *cb,
                                    MsgOpReply &replyHeader,
                                    BSONObj** ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOORDCLOSELOB_EXECUTE ) ;
      const MsgOpLob *header = NULL ;
      const MsgHeader *baseHeader = ( const MsgHeader * )pReceiveBuffer ;

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode = MSG_BS_LOB_CLOSE_RES ;
      replyHeader.header.requestID = baseHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID = baseHeader->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      rc = msgExtractCloseLobRequest( pReceiveBuffer, &header ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract msg:%d", rc ) ;
         goto error ;
      } 

      rc = rtnCloseLob( header->contextID, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to close lob:%d", rc ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_RTNCOORDCLOSELOB_EXECUTE, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOORDREMOVELOB_EXECUTE, "rtnCoordRemoveLob::execute" )
   INT32 rtnCoordRemoveLob::execute( CHAR *pReceiveBuffer, SINT32 packSize,
                                     CHAR **ppResultBuffer, pmdEDUCB *cb,
                                     MsgOpReply &replyHeader,
                                     BSONObj** ppErrorObj )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_RTNCOORDREMOVELOB_EXECUTE ) ;
      const MsgOpLob *header = NULL ;
      const MsgHeader *baseHeader = ( const MsgHeader * )pReceiveBuffer ;
      BSONObj obj ;
      BSONElement ele ;
      const CHAR *fullName = NULL ;
      _rtnCoordLobStream stream ;

      replyHeader.header.messageLength = sizeof( MsgOpReply ) ;
      replyHeader.header.opCode = MSG_BS_LOB_REMOVE_RES ;
      replyHeader.header.requestID = baseHeader->requestID ;
      replyHeader.header.routeID.value = 0 ;
      replyHeader.header.TID = baseHeader->TID ;
      replyHeader.contextID = -1 ;
      replyHeader.flags = SDB_OK ;
      replyHeader.numReturned = 0 ;
      replyHeader.startFrom = 0 ;

      rc = msgExtractRemoveLobRequest( pReceiveBuffer, &header,
                                       obj ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract remove msg:%d", rc ) ;
         goto error ;
      }

      ele = obj.getField( FIELD_NAME_COLLECTION ) ;
      if ( String != ele.type() )
      {
         PD_LOG( PDERROR, "invalid type of field \"collection\":%s",
                 obj.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      fullName = ele.valuestr() ;

      ele = obj.getField( FIELD_NAME_LOB_OID ) ;
      if ( jstOID != ele.type() )
      {
         PD_LOG( PDERROR, "invalid type of field \"oid\":%s",
                 obj.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = stream.open( fullName,
                        ele.__oid(), SDB_LOB_MODE_REMOVE,
                        cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to remove lob:%s, rc:%d",
                 ele.__oid().str().c_str(), rc ) ;
         goto error ;
      }
      else
      {
      }

      rc = stream.truncate( 0, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "faield to truncate lob:%d", rc ) ;
         goto error ;
      }

   done:
      {
      INT32 rcTmp = SDB_OK ;
      rcTmp = stream.close( cb ) ;
      if ( SDB_OK != rcTmp )
      {
         PD_LOG( PDERROR, "failed to remove lob:%d", rcTmp ) ;
         rc = rc == SDB_OK ? rcTmp : rc ;
         replyHeader.flags = rc ; 
      }
      }
      PD_TRACE_EXITRC( SDB_RTNCOORDREMOVELOB_EXECUTE, rc ) ;
      return rc ;
   error:
      replyHeader.flags = rc ; 
      goto done ;
   }
}

