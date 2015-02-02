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

   Source File Name = pmdEDUEntryPoint.cpp

   Descriptive Name = Process MoDel Engine Dispatchable Unit Event Header

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains structure for events that
   used as inter-EDU communications.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          23/06/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdEDUEntryPoint.hpp"
#include "pmd.hpp"

namespace engine
{

   pmdEntryPoint getEntryFuncByType ( EDU_TYPES type )
   {
      pmdEntryPoint rt = NULL ;
      static const _eduEntryInfo entry[] = {
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_SHARDAGENT, FALSE,
                                pmdAsyncSessionAgentEntryPoint,
                                "ShardAgent" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_AGENT, FALSE,
                                pmdLocalAgentEntryPoint,
                                "Agent" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_REPLAGENT, FALSE,
                                pmdAsyncSessionAgentEntryPoint,
                                "ReplAgent" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_RESTAGENT, FALSE,
                                pmdRestAgentEntryPoint,
                                "RestAgent" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_FAPAGENT, TRUE,
                                pmdFapAgentEntryPoint,
                                "FAPAgent" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_TCPLISTENER, TRUE,
                                pmdTcpListenerEntryPoint,
                                "TCPListener" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_RESTLISTENER, TRUE,
                                pmdRestSvcEntryPoint,
                                "RestListener" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_CLUSTER, TRUE,
                                pmdCBMgrEntryPoint,
                                "Cluster" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_CLUSTERSHARD, TRUE,
                                pmdCBMgrEntryPoint,
                                "ClusterShard" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_CLSLOGNTY, TRUE,
                                pmdClsNtyEntryPoint,
                                "ClusterLogNotify" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_REPR, TRUE,
                                pmdAsyncNetEntryPoint,
                                "ReplReader" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_LOGGW, TRUE,
                                pmdLoggWEntryPoint,
                                "LogWriter" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_SHARDR, TRUE,
                                pmdAsyncNetEntryPoint,
                                "ShardReader" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_PIPESLISTENER, TRUE,
                                pmdPipeListenerEntryPoint,
                                "PipeListener" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_BACKGROUND_JOB, FALSE,
                                pmdBackgroundJobEntryPoint,
                                "Task" ),

         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_CATMGR, TRUE,
                                pmdCBMgrEntryPoint,
                                "CatalogMgr" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_CATNETWORK, TRUE,
                                pmdAsyncNetEntryPoint,
                                "CatalogNetwork" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_COORDNETWORK, TRUE,
                                pmdCoordNetWorkEntryPoint,
                                "CoordNetwork" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_DPSROLLBACK, TRUE,
                                pmdDpsTransRollbackEntryPoint,
                                "DpsRollback"),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_LOADWORKER, FALSE,
                                pmdLoadWorkerEntryPoint,
                                "MigLoadWork" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_PREFETCHER, FALSE,
                                pmdPreLoaderEntryPoint,
                                "PreLoader" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_OMMGR, TRUE,
                                pmdCBMgrEntryPoint,
                                "OMManager" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_OMNET, TRUE,
                                pmdAsyncNetEntryPoint,
                                "OMNet" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_SYNCCLOCK, TRUE,
                                pmdSyncClockEntryPoint,
                                "SyncClockWorker" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_FAPLISTENER, TRUE,
                                pmdFapListenerEntryPoint,
                                "FAPListener" ),

         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_MAXIMUM, FALSE,
                                NULL,
                                "Unknow" )
      };

      static const UINT32 number = sizeof ( entry ) / sizeof ( _eduEntryInfo ) ;

      UINT32 index = 0 ;
      for ( ; index < number ; index ++ )
      {
         if ( entry[index].type == type )
         {
            rt = entry[index].entryFunc ;
            goto done ;
         }
      }

   done :
      return rt ;
   }

   /*
      ENTRY POINTER FUNCTIONS
   */
   INT32 pmdSyncClockEntryPoint( pmdEDUCB * cb, void * arg )
   {
      const UINT32 syncClockInterval = 10 ; // 10ms
      ossTick tmp ;
      pmdKRCB *pKrcb = pmdGetKRCB() ;

      pKrcb->getEDUMgr()->activateEDU( cb ) ;

      while ( !cb->isDisconnected() )
      {
         pKrcb->syncCurTime() ;
         ossSleep( syncClockInterval ) ;
      }
      return SDB_OK ;
   }

}


