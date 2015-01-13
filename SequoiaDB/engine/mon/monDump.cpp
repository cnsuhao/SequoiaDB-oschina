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

   Source File Name = monDump.cpp

   Descriptive Name = Monitoring Dump

   When/how to use: this program may be used on binary and text-formatted
   versions of Monitoring component. This file contains functions for
   creating resultset for a given resource.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include <set>
#include <map>
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "pmdEDUMgr.hpp"
#include "dmsCB.hpp"
#include "monDump.hpp"
#include "monEDU.hpp"
#include "monDMS.hpp"
#include "pmdOptionsMgr.hpp"
#include "ossSocket.hpp"
#include "ossVer.hpp"
#include "dmsStorageUnit.hpp"
#include "dmsDump.hpp"
#include "barBkupLogger.hpp"

#include "pdTrace.hpp"
#include "monTrace.hpp"


using namespace bson ;

#define OSS_MAX_SESSIONNAME ( OSS_MAX_HOSTNAME+OSS_MAX_SERVICENAME+30 )

namespace engine
{

   #define OSS_MAX_FILE_SZ          ( 8796093022208ll )

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONGETNODENAME, "monGetNodeName" )
   static CHAR *monGetNodeName ( CHAR *nodeName,
                                 UINT32 size,
                                 const CHAR *hostName,
                                 const CHAR *serviceName )
   {
      CHAR *ret = NULL ;
      PD_TRACE_ENTRY ( SDB_MONGETNODENAME ) ;
      PD_TRACE4 ( SDB_MONGETNODENAME, PD_PACK_STRING ( nodeName ),
                  PD_PACK_UINT ( size ), PD_PACK_STRING ( hostName ),
                  PD_PACK_STRING ( serviceName ) ) ;
      INT32 hostSize = 0 ;
      if ( !nodeName )
      {
         goto done ;
      }
      hostSize = size < ossStrlen(hostName) ? size : ossStrlen(hostName) ;
      if ( !ossStrncpy ( nodeName, hostName, hostSize ) )
      {
         goto done ;
      }
      *( nodeName + hostSize ) = NODE_NAME_SERVICE_SEPCHAR ;
      ++hostSize ;
      size -= hostSize ;
      if ( !ossStrncpy ( nodeName + hostSize,
                         serviceName,
                         size < ossStrlen(serviceName) ?
                         size : ossStrlen(serviceName) ) )
      {
         goto done ;
      }
      ret = nodeName ;
   done :
      PD_TRACE1 ( SDB_MONGETNODENAME, PD_PACK_STRING ( ret ) ) ;
      PD_TRACE_EXIT ( SDB_MONGETNODENAME ) ;
      return ret ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONGETSESSIONNAME, "monGetSessionName" )
   static INT32 monGetSessionName( char *pSessName, UINT32 size, SINT64 sessionId )
   {
      INT32 rc = SDB_OK;
      UINT32 curPos = 0;
      PD_TRACE_ENTRY ( SDB_MONGETSESSIONNAME ) ;
      *(pSessName + size - 1) = 0;

      const CHAR* hostName = pmdGetKRCB()->getHostName();
      ossStrncpy(pSessName, hostName, size - 1);

      curPos = ossStrlen( pSessName );
      PD_CHECK( curPos < size - 1, SDB_INVALIDARG, error, PDERROR,
               "out off buffer!" );
      *(pSessName + curPos) = NODE_NAME_SERVICE_SEPCHAR;
      ++curPos;
      PD_CHECK( curPos < size - 1, SDB_INVALIDARG, error, PDERROR,
               "out off buffer!" ) ;
      ossStrncpy( pSessName + curPos, pmdGetOptionCB()->getServiceAddr(),
                  size - 1 - curPos ) ;
      curPos = ossStrlen( pSessName );
      PD_CHECK( curPos < size - 1, SDB_INVALIDARG, error, PDERROR,
               "out off buffer!" );
      *(pSessName + curPos) = NODE_NAME_SERVICE_SEPCHAR;
      ++curPos;
      PD_CHECK( curPos < size - 1, SDB_INVALIDARG, error, PDERROR,
               "out off buffer!" );
      ossSnprintf( pSessName + curPos, size - 1 - curPos,
                  OSS_LL_PRINT_FORMAT, sessionId );
   done :
      PD_TRACE_EXITRC ( SDB_MONGETSESSIONNAME, rc ) ;
      return rc;
   error :
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONAPPSESSIONNAME, "monAppendSessionName" )
   INT32 monAppendSessionName ( BSONObjBuilder &ob, INT64 sessionId )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB_MONAPPSESSIONNAME ) ;
      CHAR sessionName[OSS_MAX_SESSIONNAME + 1] = {0};
      rc = monGetSessionName( sessionName,
                              OSS_MAX_SESSIONNAME + 1,
                              sessionId );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to get session-name(rc=%d)",
                  rc );
      ob.append( FIELD_NAME_SESSIONID, sessionName );
   done:
      PD_TRACE_EXITRC ( SDB_MONAPPSESSIONNAME, rc ) ;
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONAPPENDSYSTEMINFO, "monAppendSystemInfo" )
   INT32 monAppendSystemInfo ( BSONObjBuilder &ob, UINT32 mask )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MONAPPENDSYSTEMINFO ) ;

      pmdKRCB *krcb     = pmdGetKRCB() ;
      replCB *pReplcb   = sdbGetReplCB() ;
      SDB_DPSCB *dpscb  = krcb->getDPSCB() ;
      shardCB *pShardCB = sdbGetShardCB() ;
      dpsTransCB *transCB = krcb->getTransCB() ;

      CHAR hostName [ OSS_MAX_HOSTNAME + 1 ]             = {0} ;
      const CHAR *serviceName       = pmdGetOptionCB()->getServiceAddr() ;
      const CHAR *groupName         = krcb->getGroupName() ;
      CHAR nodeName [ OSS_MAX_HOSTNAME + OSS_MAX_SERVICENAME + 1 + 1 ] = {0} ;
      UINT32 nodeNameSize = OSS_MAX_HOSTNAME + OSS_MAX_SERVICENAME + 1 ;

      ossSocket::getHostName ( hostName, OSS_MAX_HOSTNAME ) ;
      monGetNodeName( nodeName, nodeNameSize,hostName, serviceName ) ;

      PD_TRACE4 ( SDB_MONAPPENDSYSTEMINFO, PD_PACK_STRING ( hostName ),
                  PD_PACK_STRING ( serviceName ),
                  PD_PACK_STRING ( groupName ),
                  PD_PACK_STRING ( nodeName ) ) ;
      try
      {
         if ( MON_MASK_NODE_NAME & mask )
         {
            ob.append ( FIELD_NAME_NODE_NAME, nodeName ) ;
         }
         if ( MON_MASK_HOSTNAME & mask )
         {
            ob.append ( FIELD_NAME_HOST, hostName ) ;
         }
         if ( MON_MASK_SERVICE_NAME & mask )
         {
            ob.append ( FIELD_NAME_SERVICE_NAME, serviceName ) ;
         }

         if ( pReplcb )
         {
            if ( MON_MASK_GROUP_NAME & mask )
            {
               ob.append ( FIELD_NAME_GROUPNAME, groupName ) ;
            }
            if ( MON_MASK_IS_PRIMARY & mask )
            {
               ob.appendBool ( FIELD_NAME_IS_PRIMARY, pReplcb->primaryIsMe() ) ;
            }
            if ( MON_MASK_SERVICE_STATUS & mask )
            {
               ob.appendBool ( FIELD_NAME_SERVICE_STATUS,
                               !pReplcb->isFullSync() ) ;
            }
         }

         if ( dpscb && ( MON_MASK_LSN_INFO & mask ) )
         {
            DPS_LSN beginLSN = dpscb->getStartLsn() ;
            DPS_LSN dpsLSN = dpscb->getCurrentLsn() ;
            INT64 offset = (INT64)dpsLSN.offset ;
            PD_TRACE2 ( SDB_MONAPPENDSYSTEMINFO,
                        PD_PACK_RAW ( &dpsLSN, sizeof(DPS_LSN) ),
                        PD_PACK_LONG ( offset ) ) ;
            BSONObj bsonTemp = BSON ( FIELD_NAME_LSN_OFFSET << offset <<
                                      FIELD_NAME_LSN_VERSION <<
                                      dpsLSN.version ) ;
            BSONObj beginLsnObj = BSON( FIELD_NAME_LSN_OFFSET <<
                                        (INT64)beginLSN.offset <<
                                        FIELD_NAME_LSN_VERSION <<
                                        beginLSN.version ) ;
            ob.append ( FIELD_NAME_BEGIN_LSN, beginLsnObj ) ;
            ob.append ( FIELD_NAME_CURRENT_LSN, bsonTemp ) ;
         }

         if ( transCB && ( MON_MASK_TRANSINFO & mask ) )
         {
            BSONObj obj = BSON( FIELD_NAME_BEGIN_LSN <<
                                (INT64)transCB->getOldestBeginLsn() ) ;
            ob.append ( FIELD_NAME_TRANS_INFO, obj ) ;
         }

         if ( pShardCB && ( MON_MASK_NODEID & mask ) )
         {
            NodeID selfID = pShardCB->nodeID() ;
            BSONArray nodeArr = BSON_ARRAY( selfID.columns.groupID <<
                                            selfID.columns.nodeID ) ;
            ob.appendArray( FIELD_NAME_NODEID,  nodeArr ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDWARNING, "Failed to append hostname and servicename, %s",
                  e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_MONAPPENDSYSTEMINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONAPPENDVERSION, "monAppendVersion" )
   void monAppendVersion ( BSONObjBuilder &ob )
   {
      INT32 major        = 0 ;
      INT32 minor        = 0 ;
      INT32 release      = 0 ;
      const CHAR *pBuild = NULL ;
      PD_TRACE_ENTRY ( SDB_MONAPPENDVERSION ) ;
      ossGetVersion ( &major, &minor, &release, &pBuild ) ;
      PD_TRACE4 ( SDB_MONAPPENDVERSION,
                  PD_PACK_INT ( major ),
                  PD_PACK_INT ( minor ),
                  PD_PACK_INT ( release ),
                  PD_PACK_STRING ( pBuild ) ) ;
      BSONObjBuilder obVersion ;
      try
      {
         obVersion.append ( FIELD_NAME_MAJOR, major ) ;
         obVersion.append ( FIELD_NAME_MINOR, minor ) ;
         obVersion.append ( FIELD_NAME_RELEASE, release ) ;
         obVersion.append ( FIELD_NAME_BUILD, pBuild ) ;
         ob.append ( FIELD_NAME_VERSION, obVersion.obj () ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDWARNING, "Failed to append version information, %s",
                  e.what() ) ;
      }
      PD_TRACE_EXIT ( SDB_MONAPPENDVERSION ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPCONTEXTSFROMCB, "monDumpContextsFromCB" )
   INT32 monDumpContextsFromCB ( pmdEDUCB *cb, rtnContextDump *context,
                                 SDB_RTNCB *rtncb, BOOLEAN simple )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( cb, "educb can't be NULL" ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;
      SDB_ASSERT ( rtncb, "runtimecb can't be NULL" ) ;

      PD_TRACE_ENTRY ( SDB_MONDUMPCONTEXTSFROMCB ) ;
      PD_TRACE1 ( SDB_MONDUMPCONTEXTSFROMCB, PD_PACK_INT ( simple ) ) ;
      if ( simple )
      {
         std::set<SINT64>contextList ;
         cb->contextCopy ( contextList ) ;
         try
         {
            BSONObj obj ;
            BSONObjBuilder ob ;
            ob.append ( FIELD_NAME_SESSIONID, (SINT64)cb->getID() ) ;
            BSONArrayBuilder ba ;
            std::set<SINT64>::const_iterator it ;
            for ( it = contextList.begin(); it!= contextList.end(); it++ )
            {
               ba.append ((*it)) ;
            }
            ob.append ( FIELD_NAME_CONTEXTS, ba.arr() ) ;
            obj = ob.obj () ;
            rc = context->monAppend( obj ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to add object %s to context",
                        obj.toString().c_str() ) ;
               goto error ;
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to create BSON objects for context, %s",
                     e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
      else
      {
         std::set<monContextFull> contextList ;
         rtncb->monContextSnap ( cb->getID(), contextList ) ;
         try
         {
            BSONObj obj ;
            BSONObjBuilder ob ;
            BSONArrayBuilder ba ;
            ossTickConversionFactor factor ;

            monAppendSessionName( ob, (SINT64)cb->getID() );

            std::set<monContextFull>::const_iterator it ;
            for ( it = contextList.begin(); it!= contextList.end(); it++ )
            {
               BSONObjBuilder contextObjBuilder ;
               UINT32 seconds ;
               UINT32 microseconds ;
               CHAR   timestampStr[ OSS_TIMESTAMP_STRING_LEN + 1] = { 0 } ;
               ossTimestamp timestamp = (*it)._monContext._startTimestamp ;

               contextObjBuilder.append( FIELD_NAME_CONTEXTID, (*it)._contextID);
               contextObjBuilder.append( FIELD_NAME_TYPE, (*it)._typeDesp ) ;
               contextObjBuilder.append( FIELD_NAME_DESP, (*it)._info ) ;
               contextObjBuilder.append( FIELD_NAME_DATAREAD,
                                         (SINT64)(*it)._monContext.dataRead );
               contextObjBuilder.append( FIELD_NAME_INDEXREAD,
                                         (SINT64)(*it)._monContext.indexRead ) ;
               (*it)._monContext.queryTimeSpent.convertToTime ( factor,
                                                                seconds,
                                                                microseconds ) ;
               contextObjBuilder.append( FIELD_NAME_QUERYTIMESPENT,
                     (SINT64)(seconds*1000 + microseconds / 1000 ) ) ;
               ossTimestampToString( timestamp, timestampStr ) ;
               contextObjBuilder.append(FIELD_NAME_STARTTIMESTAMP, timestampStr ) ;
               ba.append ( contextObjBuilder.obj() ) ;
            }
            ob.append ( FIELD_NAME_CONTEXTS, ba.arr() ) ;
            obj = ob.obj () ;
            rc = context->monAppend( obj ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to add object %s to context",
                        obj.toString().c_str() ) ;
               goto error ;
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to create BSON objects for context: %s",
                     e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPCONTEXTSFROMCB, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPALLCONTEXTS, "monDumpAllContexts" )
   INT32 monDumpAllContexts ( SDB_RTNCB *rtncb,
                              rtnContextDump *context,
                              BOOLEAN simple )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( rtncb, "runtimecb can't be NULL" ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB_MONDUMPALLCONTEXTS ) ;
      PD_TRACE1 ( SDB_MONDUMPALLCONTEXTS, PD_PACK_INT ( simple ) ) ;
      if ( simple )
      {
         std::map<UINT64, std::set<SINT64> > contextList ;
         rtncb->contextDump ( contextList ) ;
         try
         {
            std::map<UINT64, std::set<SINT64> >::const_iterator it ;
            for ( it = contextList.begin(); it!=contextList.end(); it++ )
            {
               BSONObj obj ;
               BSONObjBuilder ob ;
               ob.append ( "SessionID", (SINT64)((*it).first)) ;
               BSONArrayBuilder ba ;
               std::set<SINT64> contexts = (*it).second ;
               std::set<SINT64>::const_iterator it1 ;
               for ( it1 = contexts.begin(); it1 != contexts.end(); it1 ++ )
               {
                  ba.append ((*it1)) ;
               }
               ob.append ( FIELD_NAME_CONTEXTS, ba.arr()) ;
               obj = ob.obj() ;
               rc = context->monAppend( obj ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to add object %s to context",
                           obj.toString().c_str() ) ;
                  goto error ;
               }
            }
         }
         catch(...)
         {
            PD_LOG ( PDERROR, "Failed to create BSON objects for contexts") ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
      else
      {
         std::map<UINT64, std::set<monContextFull> > contextList ;
         rtncb->monContextSnap ( contextList ) ;
         try
         {
            ossTickConversionFactor factor ;
            std::map<UINT64, std::set<monContextFull> >::const_iterator it ;
            for ( it = contextList.begin(); it!=contextList.end(); it++ )
            {
               BSONObj obj ;
               BSONObjBuilder ob ;
               BSONArrayBuilder ba ;
               std::set<monContextFull> cf = (*it).second ;
               monAppendSessionName( ob, (SINT64)((*it).first) );
               std::set<monContextFull>::const_iterator itr ;
               for ( itr = cf.begin(); itr != cf.end(); ++itr )
               {
                  BSONObjBuilder contextObjBuilder ;
                  UINT32 seconds ;
                  UINT32 microseconds ;
                  CHAR   timestampStr[ OSS_TIMESTAMP_STRING_LEN + 1] = { 0 } ;
                  ossTimestamp timestamp = (*itr)._monContext._startTimestamp ;

                  contextObjBuilder.append(FIELD_NAME_CONTEXTID, (*itr)._contextID) ;
                  contextObjBuilder.append( FIELD_NAME_TYPE, (*itr)._typeDesp ) ;
                  contextObjBuilder.append( FIELD_NAME_DESP, (*itr)._info ) ;
                  contextObjBuilder.append(FIELD_NAME_DATAREAD,
                        (SINT64)((*itr)._monContext.dataRead) ) ;
                  contextObjBuilder.append(FIELD_NAME_INDEXREAD,
                        (SINT64)((*itr)._monContext.indexRead) ) ;
                  (*itr)._monContext.queryTimeSpent.convertToTime ( factor,
                         seconds, microseconds ) ;
                  contextObjBuilder.append(FIELD_NAME_QUERYTIMESPENT,
                        (SINT64)(seconds*1000 + microseconds / 1000 ) ) ;
                  ossTimestampToString( timestamp, timestampStr ) ;
                  contextObjBuilder.append(FIELD_NAME_STARTTIMESTAMP, timestampStr ) ;
                  ba.append ( contextObjBuilder.obj() ) ;
               }
               ob.append ( FIELD_NAME_CONTEXTS, ba.arr() ) ;
               obj = ob.obj() ;
               rc = context->monAppend( obj ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to add object %s to context",
                           obj.toString().c_str() ) ;
                  goto error ;
               }
            }
         }
         catch(...)
         {
            PD_LOG ( PDERROR, "Failed to create BSON objects for contexts" ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPALLCONTEXTS, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   #define MON_CPU_USAGE_STR_SIZE 20
   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDBDUMP, "monDBDump" )
   INT32 monDBDump ( BSONObjBuilder &ob, monDBCB *mondbcb,
                     ossTickConversionFactor &factor,
                     ossTime userTime, ossTime sysTime )
   {
      INT32 rc = SDB_OK ;
      UINT32 seconds, microseconds ;
      CHAR   timestamp[ OSS_TIMESTAMP_STRING_LEN + 1] = { 0 } ;
      CHAR   CPUTime[ MON_CPU_USAGE_STR_SIZE ] = { 0 } ;

      PD_TRACE_ENTRY ( SDB_MONDBDUMP ) ;
      ob.append( FIELD_NAME_TOTALNUMCONNECTS, (SINT64)mondbcb->numConnects ) ;
      ob.append( FIELD_NAME_TOTALDATAREAD,    (SINT64)mondbcb->totalDataRead ) ;
      ob.append( FIELD_NAME_TOTALINDEXREAD,   (SINT64)mondbcb->totalIndexRead ) ;
      ob.append( FIELD_NAME_TOTALDATAWRITE,   (SINT64)mondbcb->totalDataWrite ) ;
      ob.append( FIELD_NAME_TOTALINDEXWRITE,  (SINT64)mondbcb->totalIndexWrite ) ;
      ob.append( FIELD_NAME_TOTALUPDATE,      (SINT64)mondbcb->totalUpdate ) ;
      ob.append( FIELD_NAME_TOTALDELETE,      (SINT64)mondbcb->totalDelete ) ;
      ob.append( FIELD_NAME_TOTALINSERT,      (SINT64)mondbcb->totalInsert ) ;
      ob.append( FIELD_NAME_REPLUPDATE,       (SINT64)mondbcb->replUpdate ) ;
      ob.append( FIELD_NAME_REPLDELETE,       (SINT64)mondbcb->replDelete ) ;
      ob.append( FIELD_NAME_REPLINSERT,       (SINT64)mondbcb->replInsert ) ;
      ob.append( FIELD_NAME_TOTALSELECT,      (SINT64)mondbcb->totalSelect ) ;
      ob.append( FIELD_NAME_TOTALREAD,        (SINT64)mondbcb->totalRead ) ;

      mondbcb->totalReadTime.convertToTime ( factor, seconds, microseconds ) ;
      ob.append ( FIELD_NAME_TOTALREADTIME,
                  (SINT64)(seconds*1000 + microseconds / 1000 ) ) ;
      mondbcb->totalWriteTime.convertToTime ( factor, seconds, microseconds ) ;
      ob.append ( FIELD_NAME_TOTALWRITETIME,
                  (SINT64)(seconds*1000 + microseconds / 1000 ) ) ;
      ossTimestampToString ( mondbcb->_activateTimestamp, timestamp ) ;
      ob.append ( FIELD_NAME_ACTIVETIMESTAMP, timestamp ) ;
      ossSnprintf( CPUTime, sizeof(CPUTime), "%u.%06u",
                   userTime.seconds, userTime.microsec ) ;
      ob.append( FIELD_NAME_USERCPU, CPUTime ) ;

      ossSnprintf( CPUTime, sizeof(CPUTime), "%u.%06u",
                    sysTime.seconds, sysTime.microsec ) ;
      ob.append( FIELD_NAME_SYSCPU, CPUTime ) ;
      PD_TRACE_EXITRC ( SDB_MONDBDUMP, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONSESSIONMONEDUFULL, "monSessionMonEDUFull" )
   INT32 monSessionMonEDUFull(  BSONObjBuilder &ob, monEDUFull &full,
                                ossTickConversionFactor &factor,
                                ossTime userTime, ossTime sysTime  )
   {
      INT32 rc = SDB_OK ;
      UINT32 seconds, microseconds ;
      CHAR   timestamp[ OSS_TIMESTAMP_STRING_LEN + 1] = { 0 } ;

      PD_TRACE_ENTRY ( SDB_MONSESSIONMONEDUFULL ) ;
      ob.append( FIELD_NAME_TOTALDATAREAD, (SINT64)full._monApplCB.totalDataRead ) ;
      ob.append( FIELD_NAME_TOTALINDEXREAD, (SINT64)full._monApplCB.totalIndexRead ) ;
      ob.append( FIELD_NAME_TOTALDATAWRITE, (SINT64)full._monApplCB.totalDataWrite ) ;
      ob.append( FIELD_NAME_TOTALINDEXWRITE, (SINT64)full._monApplCB.totalIndexWrite ) ;
      ob.append( FIELD_NAME_TOTALUPDATE, (SINT64)full._monApplCB.totalUpdate ) ;
      ob.append( FIELD_NAME_TOTALDELETE, (SINT64)full._monApplCB.totalDelete ) ;
      ob.append( FIELD_NAME_TOTALINSERT, (SINT64)full._monApplCB.totalInsert ) ;
      ob.append( FIELD_NAME_TOTALSELECT, (SINT64)full._monApplCB.totalSelect ) ;
      ob.append( FIELD_NAME_TOTALREAD, (SINT64)full._monApplCB.totalRead ) ;

      full._monApplCB.totalReadTime.convertToTime ( factor,
                                                    seconds,
                                                    microseconds ) ;
      ob.append( FIELD_NAME_TOTALREADTIME,
                 (SINT64)(seconds*1000 + microseconds / 1000 ) ) ;

      full._monApplCB.totalWriteTime.convertToTime ( factor,
                                                     seconds,
                                                     microseconds ) ;
      ob.append( FIELD_NAME_TOTALWRITETIME,
                 (SINT64)(seconds*1000 + microseconds / 1000 ) ) ;

      full._monApplCB._readTimeSpent.convertToTime ( factor,
                                                    seconds,
                                                    microseconds ) ;
      ob.append( FIELD_NAME_READTIMESPENT,
                 (SINT64)(seconds*1000 + microseconds / 1000 ) ) ;

      full._monApplCB._writeTimeSpent.convertToTime ( factor,
                                                    seconds,
                                                    microseconds ) ;
      ob.append( FIELD_NAME_WRITETIMESPENT,
                 (SINT64)(seconds*1000 + microseconds / 1000 ) ) ;

      ossTimestampToString( full._monApplCB._connectTimestamp, timestamp ) ;
      ob.append ( FIELD_NAME_CONNECTTIMESTAMP, timestamp ) ;

      monDumpLastOpInfo( ob, full._monApplCB ) ;

      double userCpu;
      userCpu = userTime.seconds + (double)userTime.microsec / 1000000 ;
      ob.append( FIELD_NAME_USERCPU, userCpu ) ;

      double sysCpu;
      sysCpu = sysTime.seconds + (double)sysTime.microsec / 1000000 ;
      ob.append( FIELD_NAME_SYSCPU, sysCpu ) ;

      PD_TRACE_EXITRC ( SDB_MONSESSIONMONEDUFULL, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPMONSYSTEM, "monDumpMonSystem" )
   INT32 monDumpMonSystem ( rtnContextDump *context, BOOLEAN addInfo )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB_MONDUMPMONSYSTEM ) ;

      INT64 cpuUser ;
      INT64 cpuSys ;
      INT64 cpuIdle ;
      INT64 cpuOther ;
      INT32 memLoadPercent  = 0 ;
      INT64 memTotalPhys    = 0 ;
      INT64 memAvailPhys    = 0 ;
      INT64 memTotalPF      = 0 ;
      INT64 memAvailPF      = 0 ;
      INT64 memTotalVirtual = 0 ;
      INT64 memAvailVirtual = 0 ;
      INT64 diskTotalBytes  = 0 ;
      INT64 diskFreeBytes   = 0 ;
      const CHAR *dbPath    = pmdGetOptionCB()->getDbPath () ;

      rc = ossGetCPUInfo ( cpuUser, cpuSys, cpuIdle, cpuOther ) ;
       if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get cpu info, rc = %d", rc ) ;
         goto error ;
      }
      rc = ossGetMemoryInfo ( memLoadPercent,
                              memTotalPhys, memAvailPhys,
                              memTotalPF, memAvailPF,
                              memTotalVirtual, memAvailVirtual ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get memory info, rc = %d", rc ) ;
         goto error ;
      }

      rc = ossGetDiskInfo ( dbPath, diskTotalBytes, diskFreeBytes ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get disk info, rc = %d", rc ) ;
         goto error ;
      }
      try
      {
         BSONObj obj ;
         BSONObjBuilder ob ;

         monAppendSystemInfo ( ob ) ;
         {
            BSONObjBuilder cpuOb ;
            cpuOb.append ( FIELD_NAME_USER, ((FLOAT64)cpuUser)/1000 ) ;
            cpuOb.append ( FIELD_NAME_SYS, ((FLOAT64)cpuSys)/1000 ) ;
            cpuOb.append ( FIELD_NAME_IDLE, ((FLOAT64)cpuIdle)/1000 ) ;
            cpuOb.append ( FIELD_NAME_OTHER, ((FLOAT64)cpuOther)/1000 ) ;
            ob.append ( FIELD_NAME_CPU, cpuOb.obj () ) ;
         }
         {
            BSONObjBuilder memOb ;
            memOb.append ( FIELD_NAME_LOADPERCENT, memLoadPercent ) ;
            memOb.append ( FIELD_NAME_TOTALRAM, memTotalPhys ) ;
            memOb.append ( FIELD_NAME_FREERAM, memAvailPhys ) ;
            memOb.append ( FIELD_NAME_TOTALSWAP, memTotalPF ) ;
            memOb.append ( FIELD_NAME_FREESWAP, memAvailPF ) ;
            memOb.append ( FIELD_NAME_TOTALVIRTUAL, memTotalVirtual ) ;
            memOb.append ( FIELD_NAME_FREEVIRTUAL, memAvailVirtual ) ;
            ob.append ( FIELD_NAME_MEMORY, memOb.obj () ) ;
         }
         {
            BSONObjBuilder diskOb ;
            INT32 loadPercent = 0 ;
            if ( diskTotalBytes != 0 )
            {
               loadPercent = 100 * ( diskTotalBytes - diskFreeBytes ) /
                             diskTotalBytes ;
               loadPercent = loadPercent>100? 100:loadPercent ;
               loadPercent = loadPercent<0? 0:loadPercent ;
            }
            else
               loadPercent = 0 ;
            diskOb.append ( FIELD_NAME_DATABASEPATH, dbPath ) ;
            diskOb.append ( FIELD_NAME_LOADPERCENT, loadPercent ) ;
            diskOb.append ( FIELD_NAME_TOTALSPACE, diskTotalBytes ) ;
            diskOb.append ( FIELD_NAME_FREESPACE, diskFreeBytes ) ;
            ob.append ( FIELD_NAME_DISK, diskOb.obj () ) ;
         }
         obj = ob.obj() ;
         rc = context->monAppend( obj ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add object %s to db snap",
                     obj.toString().c_str() ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to generate system snapshot: %s",
                  e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPMONSYSTEM, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPMONDBCB, "monDumpMonDBCB" )
   INT32 monDumpMonDBCB ( rtnContextDump *context, BOOLEAN addInfo )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB_MONDUMPMONDBCB ) ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      monDBCB *mondbcb = krcb->getMonDBCB () ;
      pmdEDUMgr *mgr = krcb->getEDUMgr() ;
      SDB_RTNCB *rtnCB = krcb->getRTNCB() ;
      SDB_ASSERT ( mgr, "EDU Mgr can't be NULL" ) ;
      ossTime userTime, sysTime ;
      INT64 diskTotalBytes ;
      INT64 diskFreeBytes ;
      const CHAR *dbPath = pmdGetOptionCB()->getDbPath () ;
      ossGetCPUUsage( userTime, sysTime ) ;
      ossGetDiskInfo ( dbPath, diskTotalBytes, diskFreeBytes ) ;
      try
      {
         BSONObj obj ;
         BSONObjBuilder ob ;

         monAppendSystemInfo ( ob ) ;
         monAppendVersion ( ob ) ;
         ossTickConversionFactor factor ;
         ob.append ( FIELD_NAME_CURRENTACTIVESESSIONS,
                     (SINT32)mgr->sizeRun() ) ;
         ob.append ( FIELD_NAME_CURRENTIDLESESSIONS,
                     (SINT32)mgr->sizeIdle () ) ;
         ob.append ( FIELD_NAME_CURRENTSYSTEMSESSIONS,
                     (SINT32)mgr->sizeSystem() ) ;
         ob.append ( FIELD_NAME_CURRENTCONTEXTS, (SINT32)rtnCB->contextNum() ) ;
         ob.append ( FIELD_NAME_RECEIVECOUNT,
                     (SINT32)mondbcb->getReceiveNum() ) ;
         ob.append ( FIELD_NAME_ROLE, krcb->getOptionCB()->dbroleStr() ) ;

         {
            BSONObjBuilder diskOb ;
            INT32 loadPercent = 0 ;
            if ( diskTotalBytes != 0 )
            {
               loadPercent = 100 * ( diskTotalBytes - diskFreeBytes ) /
                             diskTotalBytes ;
               loadPercent = loadPercent>100? 100:loadPercent ;
               loadPercent = loadPercent<0? 0:loadPercent ;
            }
            else
               loadPercent = 0 ;
            diskOb.append ( FIELD_NAME_DATABASEPATH, dbPath ) ;
            diskOb.append ( FIELD_NAME_LOADPERCENT, loadPercent ) ;
            diskOb.append ( FIELD_NAME_TOTALSPACE, diskTotalBytes ) ;
            diskOb.append ( FIELD_NAME_FREESPACE, diskFreeBytes ) ;
            ob.append ( FIELD_NAME_DISK, diskOb.obj () ) ;
         }
         monDBDump ( ob, mondbcb, factor, userTime, sysTime ) ;
         monDBDumpLogInfo( ob ) ;
         monDBDumpProcMemInfo( ob ) ;
         monDBDumpStorageInfo( ob ) ;
         monDBDumpNetInfo( ob ) ;

         obj = ob.obj () ;
         rc = context->monAppend( obj ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add object %s to db snap",
                     obj.toString().c_str() ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to create BSON objects for db snap: %s",
                  e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPMONDBCB, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPSESSIONFROMCB, "monDumpSessionFromCB" )
   INT32 monDumpSessionFromCB ( pmdEDUCB *cb, rtnContextDump *context,
                                BOOLEAN addInfo, BOOLEAN simple )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( cb, "educb can't be NULL" ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB_MONDUMPSESSIONFROMCB ) ;
      if ( simple )
      {
         monEDUSimple simple ;
         cb->dumpInfo ( simple ) ;
         try
         {
            BSONObj obj ;
            BSONObjBuilder ob ;
            ob.append ( FIELD_NAME_SESSIONID, (SINT64)simple._eduID ) ;
            ob.append ( FIELD_NAME_TID, simple._tid ) ;
            ob.append ( FIELD_NAME_STATUS, simple._eduStatus ) ;
            ob.append ( FIELD_NAME_TYPE, simple._eduType ) ;
            ob.append ( FIELD_NAME_EDUNAME, simple._eduName ) ;
            if ( addInfo )
            {
               monAppendSystemInfo( ob, MON_MASK_NODE_NAME ) ;
            }
            obj = ob.obj () ;
            rc = context->monAppend( obj ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to add object %s to session",
                        obj.toString().c_str() ) ;
               goto error ;
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to create BSON objects for session: %s",
                     e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
      else
      {
         ossTime userTime, sysTime ;
         ossGetCPUUsage( cb->getThreadHandle(), userTime, sysTime ) ;

         monEDUFull full ;
         cb->dumpInfo ( full ) ;
         try
         {
            BSONObj obj ;
            BSONObjBuilder ob ;
            ossTickConversionFactor factor ;

            monAppendSessionName( ob, (SINT64)full._eduID );
            ob.append ( FIELD_NAME_TID, full._tid ) ;
            ob.append ( FIELD_NAME_STATUS, full._eduStatus ) ;
            ob.append ( FIELD_NAME_TYPE, full._eduType ) ;
            ob.append ( FIELD_NAME_EDUNAME, full._eduName ) ;
            ob.append ( FIELD_NAME_QUEUE_SIZE, full._queueSize ) ;
            ob.append ( FIELD_NAME_PROCESS_EVENT_COUNT,
                        (SINT64)full._processEventCount ) ;
            BSONArrayBuilder ab ;
            std::set<SINT64>::const_iterator it ;
            for ( it = full._eduContextList.begin();
                  it != full._eduContextList.end() ;
                  ++it )
            {
               ab.append ( *it ) ;
            }
            ob.append ( FIELD_NAME_CONTEXTS, ab.arr() ) ;

            monSessionMonEDUFull( ob, full, factor, userTime, sysTime ) ;
            obj = ob.obj () ;
            rc = context->monAppend( obj ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to add object %s to session",
                        obj.toString().c_str() ) ;
               goto error ;
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to create BSON objects for session: %s",
                     e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPSESSIONFROMCB, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPALLSESSIONS, "monDumpAllSessions" )
   INT32 monDumpAllSessions ( pmdEDUCB *cb, rtnContextDump *context,
                              BOOLEAN addInfo, BOOLEAN simple )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( cb, "educb can't be NULL" ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;

      PD_TRACE_ENTRY ( SDB_MONDUMPALLSESSIONS ) ;
      pmdEDUMgr *mgr = cb->getEDUMgr() ;
      SDB_ASSERT ( mgr, "EDU Mgr can't be NULL" ) ;

      if ( simple )
      {
         std::set<monEDUSimple> sessionList ;
         mgr->dumpInfo ( sessionList ) ;
         try
         {
            std::set<monEDUSimple>::const_iterator it ;
            for ( it = sessionList.begin(); it!=sessionList.end(); it++ )
            {
               BSONObj obj ;
               BSONObjBuilder ob ;
               monEDUSimple simple = (*it) ;
               ob.append ( FIELD_NAME_SESSIONID, (SINT64)simple._eduID ) ;
               ob.append ( FIELD_NAME_TID, simple._tid ) ;
               ob.append ( FIELD_NAME_STATUS, simple._eduStatus ) ;
               ob.append ( FIELD_NAME_TYPE, simple._eduType ) ;
               ob.append ( FIELD_NAME_EDUNAME, simple._eduName ) ;
               if ( addInfo )
               {
                  monAppendSystemInfo( ob, MON_MASK_NODE_NAME ) ;
               }
               obj = ob.obj () ;
               rc = context->monAppend( obj ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to add object %s to session",
                           obj.toString().c_str() ) ;
                  goto error ;
               }
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to create BSON objects for sessions: %s",
                     e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
      else
      {
         std::set<monEDUFull> sessionList ;
         mgr->dumpInfo ( sessionList ) ;
         try
         {
            ossTickConversionFactor factor ;
            std::set<monEDUFull>::iterator it ;
            for ( it = sessionList.begin(); it!=sessionList.end(); it++ )
            {
               BSONObj obj ;
               BSONObjBuilder ob ;
               BSONArrayBuilder ab ;
               ossTime userTime, sysTime ;

               monAppendSessionName( ob, (SINT64)(*it)._eduID );
               ob.append( FIELD_NAME_TID, (*it)._tid ) ;
               ob.append( FIELD_NAME_STATUS, (*it)._eduStatus ) ;
               ob.append( FIELD_NAME_TYPE, (*it)._eduType ) ;
               ob.append( FIELD_NAME_EDUNAME, (*it)._eduName ) ;
               ob.append( FIELD_NAME_QUEUE_SIZE, (*it)._queueSize ) ;
               ob.append( FIELD_NAME_PROCESS_EVENT_COUNT,
                          (SINT64)(*it)._processEventCount ) ;

               std::set<SINT64>::const_iterator itr ;
               for ( itr = (*it)._eduContextList.begin();
                     itr != (*it)._eduContextList.end() ;
                     ++itr )
               {
                  ab.append( *itr ) ;
               }
               ob.append( FIELD_NAME_CONTEXTS, ab.arr() ) ;

               ossGetCPUUsage( (*it)._threadHdl, userTime, sysTime ) ;

               monSessionMonEDUFull( ob, (monEDUFull&)(*it),
                                     factor, userTime, sysTime ) ;

               obj = ob.obj() ;
               rc = context->monAppend( obj ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to add object %s to session",
                           obj.toString().c_str() ) ;
                  goto error ;
               }
            }
         }
         catch ( std::exception &e )
         {
            PD_LOG ( PDERROR, "Failed to create BSON objects for sessions: %s",
                     e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPALLSESSIONS, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_MONDMSCOLLECTIONFLAGTOSTRING, "monDMSCollectionFlagToString" )
   void monDMSCollectionFlagToString ( UINT16 flag, std::string &out )
   {
      PD_TRACE_ENTRY ( SDB_MONDMSCOLLECTIONFLAGTOSTRING ) ;
      PD_TRACE1 ( SDB_MONDMSCOLLECTIONFLAGTOSTRING, PD_PACK_USHORT(flag) ) ;
      if ( DMS_IS_MB_FREE(flag) )
      {
         out = "Free" ;
         goto done ;
      }
      if ( DMS_IS_MB_NORMAL(flag) )
      {
         out = "Normal" ;
         goto done ;
      }
      if ( DMS_IS_MB_DROPPED(flag) )
      {
         out = "Dropped" ;
         goto done ;
      }
      if ( DMS_IS_MB_OFFLINE_REORG_SHADOW_COPY(flag) )
      {
         out = "Offline Reorg Shadow Copy Phase" ;
         goto done ;
      }
      if ( DMS_IS_MB_OFFLINE_REORG_TRUNCATE(flag) )
      {
         out = "Offline Reorg Truncate Phase" ;
         goto done ;
      }
      if ( DMS_IS_MB_OFFLINE_REORG_COPY_BACK(flag) )
      {
         out = "Offline Reorg Copy Back Phase" ;
         goto done ;
      }
      if ( DMS_IS_MB_OFFLINE_REORG_REBUILD(flag) )
      {
         out = "Offline Reorg Rebuild Phase" ;
         goto done ;
      }
   done :
      PD_TRACE_EXIT ( SDB_MONDMSCOLLECTIONFLAGTOSTRING ) ;
      return ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPALLCOLLECTIONS, "monDumpAllCollections" )
   INT32 monDumpAllCollections( SDB_DMSCB *dmsCB, rtnContextDump *context,
                                BOOLEAN addInfo, BOOLEAN details,
                                BOOLEAN includeSys )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;

      PD_TRACE_ENTRY ( SDB_MONDUMPALLCOLLECTIONS ) ;
      std::set<monCollection> collectionList ;
      dmsCB->dumpInfo ( collectionList, includeSys ) ;
      try
      {
         std::set<monCollection>::const_iterator it ;
         for ( it = collectionList.begin(); it!=collectionList.end(); it++ )
         {
            BSONObj obj ;
            BSONObjBuilder ob ;
            BSONArrayBuilder ba ;
            monCollection collection = (*it) ;
            ob.append ( FIELD_NAME_NAME, collection._name ) ;
            std::set<detailedInfo>::const_iterator it1 ;
            if ( details )
            {
               for ( it1 = collection._details.begin();
                     it1 != collection._details.end();
                     it1++ )
               {
                  BSONObjBuilder ob1 ;
                  UINT16 flag = (*it1)._flag ;
                  std::string status = "" ;
                  ob1.append ( FIELD_NAME_ID,       (*it1)._blockID ) ;
                  ob1.append ( FIELD_NAME_LOGICAL_ID, (*it1)._logicID ) ;
                  ob1.append ( FIELD_NAME_SEQUENCE, (*it1)._sequence ) ;
                  ob1.append ( FIELD_NAME_INDEXES,  (*it1)._numIndexes ) ;
                  monDMSCollectionFlagToString ( flag, status ) ;
                  ob1.append ( FIELD_NAME_STATUS, status ) ;
                  ob1.append ( FIELD_NAME_TOTAL_RECORDS,
                               (long long)((*it1)._totalRecords )) ;
                  ob1.append ( FIELD_NAME_TOTAL_DATA_PAGES,
                               (*it1)._totalDataPages ) ;
                  ob1.append ( FIELD_NAME_TOTAL_INDEX_PAGES,
                               (*it1)._totalIndexPages ) ;
                  ob1.append ( FIELD_NAME_TOTAL_LOB_PAGES,
                               (*it1)._totalLobPages ) ;
                  ob1.append ( FIELD_NAME_TOTAL_DATA_FREESPACE,
                               (long long)((*it1)._totalDataFreeSpace )) ;
                  ob1.append ( FIELD_NAME_TOTAL_INDEX_FREESPACE,
                               (long long)((*it1)._totalIndexFreeSpace )) ;
                  if ( addInfo )
                  {
                     monAppendSystemInfo( ob1, MON_MASK_NODE_NAME ) ;
                     monAppendSystemInfo( ob1, MON_MASK_GROUP_NAME ) ;
                  }
                  ba.append (ob1.done()) ;
               }
               ob.append ( FIELD_NAME_DETAILS, ba.arr() ) ;
            }
            obj = ob.obj() ;
            rc = context->monAppend( obj ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to add object %s to collections",
                        obj.toString().c_str() ) ;
               goto error ;
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to create BSON objects for collections: %s",
                  e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPALLCOLLECTIONS, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPALLCOLLECTIONSPACES, "monDumpAllCollectionSpaces" )
   INT32 monDumpAllCollectionSpaces ( SDB_DMSCB *dmsCB, rtnContextDump *context,
                                      BOOLEAN addInfo, BOOLEAN details,
                                      BOOLEAN includeSys )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;

      INT64 dataCapSize    = 0 ;
      INT64 lobCapSize     = 0 ;

      PD_TRACE_ENTRY ( SDB_MONDUMPALLCOLLECTIONSPACES ) ;
      std::set<monCollectionSpace> csList ;
      dmsCB->dumpInfo ( csList, includeSys ) ;
      try
      {
         std::set<monCollectionSpace>::const_iterator it ;
         for ( it = csList.begin(); it!=csList.end(); it++ )
         {
            BSONObj obj ;
            BSONObjBuilder ob ;
            monCollectionSpace cs = (*it) ;
            if ( details )
            {
               BSONArrayBuilder ab ;
               if ( ossStrcmp ( cs._name, SDB_DMSTEMP_NAME ) != 0 )
               {
                  std::vector<CHAR*>::const_iterator it1 ;
                  for ( it1 = cs._collections.begin();
                        it1!= cs._collections.end();
                        it1++ )
                  {
                     ab.append (BSON ( FIELD_NAME_NAME << (*it1) ) ) ;
                  }
               }
               dataCapSize = (INT64)cs._pageSize * DMS_MAX_PG ;
               lobCapSize  = (INT64)cs._lobPageSize * DMS_MAX_PG ;
               if ( lobCapSize > OSS_MAX_FILE_SZ )
               {
                  lobCapSize = OSS_MAX_FILE_SZ ;
               }

               ob.append ( FIELD_NAME_COLLECTION, ab.arr() ) ;
               ob.append ( FIELD_NAME_PAGE_SIZE, cs._pageSize ) ;
               ob.append ( FIELD_NAME_LOB_PAGE_SIZE, cs._lobPageSize ) ;
               ob.append ( FIELD_NAME_MAX_CAPACITY_SIZE,
                           2 * dataCapSize + lobCapSize ) ;
               ob.append ( FIELD_NAME_MAX_DATA_CAP_SIZE, dataCapSize ) ;
               ob.append ( FIELD_NAME_MAX_INDEX_CAP_SIZE, dataCapSize ) ;
               ob.append ( FIELD_NAME_MAX_LOB_CAP_SIZE, lobCapSize ) ;
               ob.append ( FIELD_NAME_NUMCOLLECTIONS, cs._clNum ) ;
               ob.append ( FIELD_NAME_TOTAL_RECORDS, cs._totalRecordNum ) ;
               ob.append ( FIELD_NAME_TOTAL_SIZE, cs._totalSize ) ;
               ob.append ( FIELD_NAME_FREE_SIZE, cs._freeSize ) ;
               ob.append ( FIELD_NAME_TOTAL_DATA_SIZE, cs._totalDataSize ) ;
               ob.append ( FIELD_NAME_FREE_DATA_SIZE, cs._freeDataSize ) ;
               ob.append ( FIELD_NAME_TOTAL_IDX_SIZE, cs._totalIndexSize ) ;
               ob.append ( FIELD_NAME_FREE_IDX_SIZE, cs._freeIndexSize ) ;
               ob.append ( FIELD_NAME_TOTAL_LOB_SIZE, cs._totalLobSize ) ;
               ob.append ( FIELD_NAME_FREE_LOB_SIZE, cs._freeLobSize ) ;
            }
            ob.append ( FIELD_NAME_NAME, cs._name ) ;
            if ( addInfo )
            {
               monAppendSystemInfo( ob, MON_MASK_GROUP_NAME );
            }
            obj = ob.obj() ;
            rc = context->monAppend( obj ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to add object %s to collection spaces",
                        obj.toString().c_str() ) ;
               goto error ;
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to create BSON objects for collection "
                  "spaces: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPALLCOLLECTIONSPACES, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPALLSTORAGEUNITS, "monDumpAllStorageUnits" )
   INT32 monDumpAllStorageUnits ( SDB_DMSCB *dmsCB, rtnContextDump *context )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( dmsCB, "dmsCB can't be NULL" ) ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;

      PD_TRACE_ENTRY ( SDB_MONDUMPALLSTORAGEUNITS ) ;
      std::set<monStorageUnit> storageUnitList ;
      dmsCB->dumpInfo ( storageUnitList ) ;
      try
      {
         std::set<monStorageUnit>::const_iterator it ;
         for ( it = storageUnitList.begin(); it!=storageUnitList.end(); it++ )
         {
            BSONObj obj ;
            BSONObjBuilder ob ;
            monStorageUnit su = (*it) ;
            ob.append ( FIELD_NAME_NAME, su._name ) ;
            ob.append ( FIELD_NAME_ID, su._CSID ) ;
            ob.append ( FIELD_NAME_LOGICAL_ID, su._logicalCSID ) ;
            ob.append ( FIELD_NAME_PAGE_SIZE, su._pageSize ) ;
            ob.append ( FIELD_NAME_LOB_PAGE_SIZE, su._lobPageSize ) ;
            ob.append ( FIELD_NAME_SEQUENCE, su._sequence ) ;
            ob.append ( FIELD_NAME_NUMCOLLECTIONS, su._numCollections ) ;
            ob.append ( FIELD_NAME_COLLECTIONHWM, su._collectionHWM ) ;
            ob.append ( FIELD_NAME_SIZE, su._size ) ;
            obj = ob.obj() ;
            rc = context->monAppend( obj ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to add object %s to storage units",
                        obj.toString().c_str() ) ;
               goto error ;
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to create BSON objects for storage "
                  "units: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPALLSTORAGEUNITS, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPINDEXES, "monDumpIndexes" )
   INT32 monDumpIndexes( vector<monIndex> &indexes, rtnContextDump *context )
   {
      INT32 rc = SDB_OK ;
      string flagDesp ;
      SDB_ASSERT ( context, "context can't be NULL" ) ;

      PD_TRACE_ENTRY ( SDB_MONDUMPINDEXES ) ;
      try
      {
         std::vector<monIndex>::iterator it ;
         for ( it = indexes.begin(); it!=indexes.end(); ++it )
         {
            monIndex &indexItem = (*it) ;
            BSONObj &indexObj = indexItem._indexDef ;
            BSONObj obj ;
            BSONObjBuilder builder ;
            BSONObjBuilder ob (builder.subobjStart(IXM_FIELD_NAME_INDEX_DEF )) ;
            ob.append ( IXM_NAME_FIELD,
                        indexObj.getStringField(IXM_NAME_FIELD) ) ;
            OID oid ;
            indexObj.getField(DMS_ID_KEY_NAME).Val(oid) ;
            ob.append ( DMS_ID_KEY_NAME, oid ) ;
            ob.append ( IXM_KEY_FIELD,
                        indexObj.getObjectField(IXM_KEY_FIELD) ) ;
            BSONElement e = indexObj[IXM_V_FIELD] ;
            INT32 version = ( e.type() == NumberInt ) ? e._numberInt() : 0 ;
            ob.append ( IXM_V_FIELD, version ) ;
            ob.append ( IXM_UNIQUE_FIELD,
                        indexObj[IXM_UNIQUE_FIELD].trueValue() ) ;
            ob.append ( IXM_DROPDUP_FIELD,
                        indexObj.getBoolField(IXM_DROPDUP_FIELD) ) ;
            ob.append ( IXM_ENFORCED_FIELD,
                        indexObj.getBoolField(IXM_ENFORCED_FIELD) ) ;
            BSONObj range = indexObj.getObjectField( IXM_2DRANGE_FIELD ) ;
            if ( !range.isEmpty() )
            {
               ob.append( IXM_2DRANGE_FIELD, range ) ;
            }
            ob.done () ;

            flagDesp = getIndexFlagDesp(indexItem._indexFlag) ;
            builder.append (IXM_FIELD_NAME_INDEX_FLAG, flagDesp.c_str() ) ;
            if ( IXM_INDEX_FLAG_CREATING == indexItem._indexFlag )
            {
               builder.append ( IXM_FIELD_NAME_SCAN_EXTLID,
                                indexItem._scanExtLID ) ;
            }
            obj = builder.obj() ;
            rc = context->monAppend( obj ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to add object %s to collections",
                        obj.toString().c_str() ) ;
               goto error ;
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Failed to create BSON objects for collections: %s",
                  e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPINDEXES, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONRESETMON, "monResetMon" )
   void monResetMon ()
   {
      PD_TRACE_ENTRY ( SDB_MONRESETMON ) ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      monDBCB *mondbcb = krcb->getMonDBCB () ;
      pmdEDUMgr *mgr = krcb->getEDUMgr() ;
      mgr->resetMon () ;
      mondbcb->reset () ;
      PD_TRACE_EXIT ( SDB_MONRESETMON ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPTRACESTATUS, "monDumpTraceStatus" )
   INT32 monDumpTraceStatus ( rtnContextDump *context )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MONDUMPTRACESTATUS ) ;
      try
      {
         BSONObj obj ;
         BSONObjBuilder builder ;
         pdTraceCB *traceCB = sdbGetPDTraceCB() ;
         BOOLEAN traceStarted = traceCB->_traceStarted.peek() ;
         builder.appendBool ( FIELD_NAME_TRACESTARTED, traceStarted ) ;
         if ( traceStarted )
         {
            builder.appendBool ( FIELD_NAME_WRAPPED,
                                 traceCB->_currentSlot.peek() >
                                 traceCB->getSlotNum() ) ;
            builder.appendNumber ( FIELD_NAME_SIZE,
                                   (INT32)(traceCB->getSlotNum() *
                                           TRACE_SLOT_SIZE) ) ;
            BSONArrayBuilder arr ;
            for ( INT32 i = 0; i < _pdTraceComponentNum; ++i )
            {
               UINT32 mask = ((UINT32)1)<<i ;
               if ( mask & traceCB->getMask() )
               {
                  arr.append ( pdGetTraceComponent ( i ) ) ;
               }
            }
            builder.append ( FIELD_NAME_MASK, arr.arr() ) ;

            BSONArrayBuilder bpArr;
            const UINT64 *bpList = traceCB->getBPList () ;
            INT32 bpNum = traceCB->getBPNum () ;
            for ( INT32 i = 0; i < bpNum; ++i )
            {
               bpArr.append( pdGetTraceFunction( bpList[i] ) ) ;
            }
            builder.append( FIELD_NAME_BREAKPOINTS, bpArr.arr() );
         }
         obj = builder.obj() ;
         rc = context->monAppend( obj ) ;
         PD_RC_CHECK ( rc, PDERROR,
                       "Failed to add obj to context, rc = %d", rc ) ;
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK( SDB_SYS, PDERROR,
                      "Failed to create trace status dump: %s",
                      e.what() ) ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_MONDUMPTRACESTATUS, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   #define MAX_DATABLOCK_A_RECORD_NUM  (500)
   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPDATABLOCKS, "monDumpDatablocks" )
   INT32 monDumpDatablocks( std::vector<dmsExtentID> &datablocks,
                            rtnContextDump *context )
   {
      INT32 rc = SDB_OK ;
      INT32 datablockNum = 0 ;
      PD_TRACE_ENTRY ( SDB_MONDUMPDATABLOCKS ) ;
      while ( datablocks.size() > 0 )
      {
         try
         {
            datablockNum = 0 ;
            BSONObjBuilder builder ;
            BSONArrayBuilder blockArrBd ;
            BSONObj obj ;

            rc = monAppendSystemInfo( builder, MON_MASK_HOSTNAME|
                                      MON_MASK_SERVICE_NAME|MON_MASK_NODEID ) ;
            PD_RC_CHECK( rc, PDERROR, "Append system info failed, rc: %d",
                         rc ) ;

            builder.append( FIELD_NAME_SCANTYPE, VALUE_NAME_TBSCAN ) ;
            std::vector<dmsExtentID>::iterator it = datablocks.begin() ;
            while ( it != datablocks.end() &&
                    datablockNum < MAX_DATABLOCK_A_RECORD_NUM )
            {
               blockArrBd.append( *it ) ;
               it = datablocks.erase( it ) ;
               ++datablockNum ;
            }

            builder.appendArray( FIELD_NAME_DATABLOCKS, blockArrBd.arr() ) ;
            obj = builder.obj() ;
            rc = context->monAppend( obj ) ;
            PD_RC_CHECK( rc, PDERROR, "Add to obj[%s] to context failed, "
                         "rc: %d", obj.toString().c_str(), rc ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG( PDERROR, "Ocurr exception: %s", e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB_MONDUMPDATABLOCKS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   #define MAX_INDEXBLOCK_A_RECORD_NUM  (20)
   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDUMPINDEXBLOCKS, "monDumpIndexblocks" )
   INT32 monDumpIndexblocks( std::vector< BSONObj > &idxBlocks,
                             std::vector< dmsRecordID > &idxRIDs,
                             const CHAR *indexName,
                             dmsExtentID indexLID,
                             INT32 direction,
                             rtnContextDump * context )
   {
      INT32 rc = SDB_OK ;
      INT32 indexblockNum = 0 ;
      UINT32 indexPos = 0 ;
      PD_TRACE_ENTRY ( SDB_MONDUMPINDEXBLOCKS ) ;
      SDB_ASSERT( idxBlocks.size() == idxRIDs.size(), "size not same" ) ;

      if ( 1 != direction )
      {
         indexPos = idxBlocks.size() - 1 ;
      }

      while ( ( 1 == direction && indexPos + 1 < idxBlocks.size() ) ||
              ( -1 == direction && indexPos > 0 ) )
      {
         try
         {
            indexblockNum = 0 ;
            BSONObjBuilder builder ;
            BSONArrayBuilder blockArrBd ;
            BSONObj obj ;

            rc = monAppendSystemInfo( builder, MON_MASK_HOSTNAME|
                                      MON_MASK_SERVICE_NAME|MON_MASK_NODEID ) ;
            PD_RC_CHECK( rc, PDERROR, "Append system info failed, rc: %d",
                         rc ) ;

            builder.append( FIELD_NAME_SCANTYPE, VALUE_NAME_IXSCAN ) ;
            builder.append( FIELD_NAME_INDEXNAME, indexName ) ;
            builder.append( FIELD_NAME_INDEXLID, indexLID ) ;
            builder.append( FIELD_NAME_DIRECTION, direction ) ;
            while ( ( ( 1 == direction && indexPos + 1 < idxBlocks.size() ) ||
                      ( -1 == direction && indexPos > 0 ) ) &&
                    indexblockNum < MAX_INDEXBLOCK_A_RECORD_NUM )
            {
               blockArrBd.append( BSON( FIELD_NAME_STARTKEY <<
                                        idxBlocks[indexPos] <<
                                        FIELD_NAME_ENDKEY <<
                                        idxBlocks[indexPos+direction] <<
                                        FIELD_NAME_STARTRID <<
                                        BSON_ARRAY( idxRIDs[indexPos]._extent <<
                                                    idxRIDs[indexPos]._offset ) <<
                                        FIELD_NAME_ENDRID <<
                                        BSON_ARRAY( idxRIDs[indexPos+direction]._extent <<
                                                    idxRIDs[indexPos+direction]._offset )
                                        )
                                  ) ;
               indexPos += direction ;
               ++indexblockNum ;
            }

            builder.appendArray( FIELD_NAME_INDEXBLOCKS, blockArrBd.arr() ) ;
            obj = builder.obj() ;
            rc = context->monAppend( obj ) ;
            PD_RC_CHECK( rc, PDERROR, "Add to obj[%s] to context failed, "
                         "rc: %d", obj.toString().c_str(), rc ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG( PDERROR, "Ocurr exception: %s", e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB_MONDUMPINDEXBLOCKS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDBDUMPSTORINFO, "monDBDumpStorageInfo" )
   INT32 monDBDumpStorageInfo( BSONObjBuilder &ob )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MONDBDUMPSTORINFO ) ;
      try
      {
         INT64 totalMapped = 0 ;
         pmdGetKRCB()->getDMSCB()->dumpInfo( totalMapped ) ;
         ob.append( FIELD_NAME_TOTALMAPPED, totalMapped ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Ocurr exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_MONDBDUMPSTORINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDBDUMPPROCMEMINFO, "monDBDumpProcMemInfo" )
   INT32 monDBDumpProcMemInfo( BSONObjBuilder &ob )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MONDBDUMPPROCMEMINFO ) ;
      try
      {
         ossProcMemInfo memInfo ;
         rc = ossGetProcMemInfo( memInfo ) ;
         if ( SDB_OK == rc )
         {
            ob.append( FIELD_NAME_VSIZE, memInfo.vSize ) ;
            ob.append( FIELD_NAME_RSS, memInfo.rss ) ;
            ob.append( FIELD_NAME_FAULT, memInfo.fault ) ;
         }
         else
         {
            ob.append( FIELD_NAME_VSIZE, 0 ) ;
            ob.append( FIELD_NAME_RSS, 0 ) ;
            ob.append( FIELD_NAME_FAULT, 0 ) ;
            PD_RC_CHECK( rc, PDERROR,
                        "failed to dump memory info(rc=%d)",
                        rc ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Ocurr exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_MONDBDUMPPROCMEMINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDBDUMPNETINFO, "monDBDumpNetInfo" )
   INT32 monDBDumpNetInfo( BSONObjBuilder &ob )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MONDBDUMPNETINFO ) ;
      try
      {
         pmdKRCB *pKrcb = pmdGetKRCB() ;
         monDBCB *pdbCB = pKrcb->getMonDBCB() ;
         SDB_ROLE role = pKrcb->getDBRole() ;
         ob.append( FIELD_NAME_SVC_NETIN, pdbCB->svcNetIn() ) ;
         ob.append( FIELD_NAME_SVC_NETOUT, pdbCB->svcNetOut() ) ;
         if ( SDB_ROLE_DATA == role
            || SDB_ROLE_CATALOG == role )
         {
            shardCB *pShardCB = sdbGetShardCB() ;
            ob.append( FIELD_NAME_SHARD_NETIN, pShardCB->netIn() ) ;
            ob.append( FIELD_NAME_SHARD_NETOUT, pShardCB->netOut() ) ;

            replCB *pReplCB = sdbGetReplCB() ;
            ob.append( FIELD_NAME_REPL_NETIN, pReplCB->netIn() ) ;
            ob.append( FIELD_NAME_REPL_NETOUT, pReplCB->netOut() ) ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Ocurr exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_MONDBDUMPNETINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDBDUMPLOGINFO, "monDBDumpLogInfo" )
   INT32 monDBDumpLogInfo( BSONObjBuilder &ob )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MONDBDUMPLOGINFO ) ;
      try
      {
         ob.append( FIELD_NAME_FREELOGSPACE,
                  (INT64)(pmdGetKRCB()->getTransCB()->remainLogSpace()) );
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Ocurr exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_MONDBDUMPLOGINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_MONDBDUMPLASTOPINFO, "monDumpLastOpInfo" )
   INT32 monDumpLastOpInfo( BSONObjBuilder &ob, const monAppCB &moncb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MONDBDUMPLASTOPINFO ) ;
      try
      {
         switch( moncb._lastOpType )
         {
            case MSG_BS_QUERY_REQ:
               {
                  if ( MSG_NULL == moncb._cmdType )
                  {
                     ob.append( FIELD_NAME_LASTOPTYPE, "query" ) ;
                  }
                  else
                  {
                  }
                  break ;
               }
            case MSG_BS_GETMORE_REQ:
               {
                  ob.append( FIELD_NAME_LASTOPTYPE, "getmore" ) ;
                  break ;
               }
            case MSG_BS_DELETE_REQ:
               {
                  ob.append( FIELD_NAME_LASTOPTYPE, "delete" ) ;
                  break ;
               }
            case MSG_BS_INSERT_REQ:
               {
                  ob.append( FIELD_NAME_LASTOPTYPE, "insert" ) ;
                  break ;
               }
            case MSG_BS_UPDATE_REQ:
               {
                  ob.append( FIELD_NAME_LASTOPTYPE, "update" ) ;
                  break ;
               }
            default:
               {
                  ob.append( FIELD_NAME_LASTOPTYPE, "unknow" ) ;
                  break ;
               }
         }

         CHAR   timestamp[ OSS_TIMESTAMP_STRING_LEN + 1] = { 0 } ;
         if ( ( BOOLEAN )( moncb._lastOpBeginTime ) )
         {
            ossTimestamp Tm;
            moncb._lastOpBeginTime.convertToTimestamp( Tm ) ;
            ossTimestampToString( Tm, timestamp ) ;
         }
         else
         {
            ossStrcpy(timestamp, "--") ;
         }
         ob.append( FIELD_NAME_LASTOPBEGIN, timestamp ) ;

         if ( ( BOOLEAN )( moncb._lastOpEndTime ) )
         {
            ossTimestamp Tm;
            moncb._lastOpEndTime.convertToTimestamp( Tm ) ;
            ossTimestampToString( Tm, timestamp ) ;
         }
         else
         {
            ossStrcpy(timestamp, "--") ;
         }
         ob.append( FIELD_NAME_LASTOPEND, timestamp ) ;

         ob.append( FIELD_NAME_LASTOPINFO, moncb._lastOpDetail ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Ocurr exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_MONDBDUMPLASTOPINFO, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}

