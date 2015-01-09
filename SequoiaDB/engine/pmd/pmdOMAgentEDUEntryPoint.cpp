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

   Source File Name = pmdOMAgentEDUEntryPoint.cpp

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

namespace engine
{

   pmdEntryPoint getEntryFuncByType ( EDU_TYPES type )
   {
      pmdEntryPoint rt = NULL ;
      static const _eduEntryInfo entry[] = {
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_AGENT, FALSE,
                                pmdAsyncSessionAgentEntryPoint,
                                "Agent" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_BACKGROUND_JOB, FALSE,
                                pmdBackgroundJobEntryPoint,
                                "Task" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_OMMGR, TRUE,
                                pmdCBMgrEntryPoint,
                                "OMManager" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_OMNET, TRUE,
                                pmdAsyncNetEntryPoint,
                                "OMNet" ),
         ON_EDUTYPE_TO_ENTRY1 ( EDU_TYPE_PIPESLISTENER, TRUE,
                                pmdPipeListenerEntryPoint,
                                "PipeListener" ),

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

}


