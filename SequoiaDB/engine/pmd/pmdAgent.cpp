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

   Source File Name = pmdAgent.cpp

   Descriptive Name = Process MoDel Agent

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include <stdio.h>
#include "pd.hpp"
#include "pmdEDUMgr.hpp"
#include "pmdEDU.hpp"
#include "msgMessage.hpp"
#include "oss.hpp"
#include "ossSocket.hpp"
#include "ossMem.hpp"
#include "ossUtil.hpp"
#include "rtn.hpp"
#include "../bson/bson.h"
#include "pmd.hpp"
#include "restAdaptorold.hpp"
#include "rtnCoord.hpp"
#include "rtnCoordCommands.hpp"
#include "coordSession.hpp"
#include "pmdSession.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "pmdCB.hpp"
#include "pmdProcessor.hpp"

using namespace bson ;

namespace engine
{
   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDLOCALAGENTENTPNT, "pmdLocalAgentEntryPoint" )
   INT32 pmdLocalAgentEntryPoint( pmdEDUCB *cb, void *arg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDLOCALAGENTENTPNT );

      SOCKET s = *(( SOCKET *) &arg ) ;
      pmdLocalSession localSession( s ) ;
      localSession.attach( cb ) ;

      if ( pmdGetDBRole() == SDB_ROLE_COORD )
      {
         pmdCoordProcessor coordProcessor ;
         localSession.attachProcessor( &coordProcessor ) ;
         rc = localSession.run() ;
         localSession.detachProcessor() ;
      }
      else
      {
         pmdDataProcessor dataProcessor ;
         localSession.attachProcessor( &dataProcessor ) ;
         rc = localSession.run() ;
         localSession.detachProcessor() ;
      }

      localSession.detach() ;

      PD_TRACE_EXITRC ( SDB_PMDLOCALAGENTENTPNT, rc );
      return rc ;
   }

}

