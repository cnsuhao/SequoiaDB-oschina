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

   Source File Name = rtnCoord.cpp

   Descriptive Name = Runtime Coord

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   command factory on coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCoord.hpp"
#include "rtnCoordCommands.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"

namespace engine
{

   rtnCoordProcesserFactory::rtnCoordProcesserFactory()
   {
      addCommand();
      addOperator();
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROFAC_RTNCOPROFAC, "rtnCoordProcesserFactory::~rtnCoordProcesserFactory" )
   rtnCoordProcesserFactory::~rtnCoordProcesserFactory()
   {
      PD_TRACE_ENTRY ( SDB_RTNCOPROFAC_RTNCOPROFAC ) ;
      COORD_CMD_MAP::iterator iter;
      iter = _cmdMap.begin();
      while ( iter != _cmdMap.end() )
      {
         SDB_OSS_DEL iter->second;
         _cmdMap.erase( iter++ );
      }
      _cmdMap.clear();

      COORD_OP_MAP::iterator iterOp;
      iterOp = _opMap.begin();
      while( iterOp != _opMap.end() )
      {
         SDB_OSS_DEL iterOp->second;
         _opMap.erase( iterOp++ );
      }
      PD_TRACE_EXIT ( SDB_RTNCOPROFAC_RTNCOPROFAC ) ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROFAC_GETOP, "rtnCoordProcesserFactory::getOperator" )
   rtnCoordOperator * rtnCoordProcesserFactory::getOperator( SINT32 opCode )
   {
      PD_TRACE_ENTRY ( SDB_RTNCOPROFAC_GETOP ) ;
      COORD_OP_MAP::iterator iter;
      iter = _opMap.find ( opCode );
      if ( _opMap.end() == iter )
      {
         iter = _opMap.find ( MSG_NULL );
      }
      PD_TRACE_EXIT ( SDB_RTNCOPROFAC_GETOP ) ;
      return iter->second;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROFAC_GETCOMPRO1, "rtnCoordProcesserFactory::getCommandProcesser" )
   rtnCoordCommand * rtnCoordProcesserFactory::getCommandProcesser(const MsgOpQuery *pQuery)
   {
      PD_TRACE_ENTRY ( SDB_RTNCOPROFAC_GETCOMPRO1 ) ;
      SDB_ASSERT ( pQuery, "pQuery can't be NULL" ) ;
      rtnCoordCommand *pProcesser = NULL;
      do
      {
         if ( MSG_BS_QUERY_REQ == pQuery->header.opCode )
         {
            if ( pQuery->nameLength > 0 )
            {
               COORD_CMD_MAP::iterator iter;
               iter = _cmdMap.find( pQuery->name );
               if ( iter != _cmdMap.end() )
               {
                  pProcesser = iter->second;
               }
            }
         }
         if ( NULL == pProcesser )
         {
            COORD_CMD_MAP::iterator iter;
            iter = _cmdMap.find( COORD_CMD_DEFAULT );
            if ( iter != _cmdMap.end() )
            {
               pProcesser = iter->second;
            }
         }
      }while ( FALSE );
      PD_TRACE_EXIT ( SDB_RTNCOPROFAC_GETCOMPRO1 ) ;
      return pProcesser;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_RTNCOPROFAC_GETCOMPRO2, "rtnCoordProcesserFactory::getCommandProcesser" )
   rtnCoordCommand * rtnCoordProcesserFactory::getCommandProcesser(const char *pCmd)
   {
      PD_TRACE_ENTRY ( SDB_RTNCOPROFAC_GETCOMPRO2 ) ;
      SDB_ASSERT ( pCmd, "pCmd can't be NULL" ) ;
      rtnCoordCommand *pProcesser = NULL;
      do
      {
         COORD_CMD_MAP::iterator iter;
         iter = _cmdMap.find( pCmd );
         if ( iter != _cmdMap.end() )
         {
            pProcesser = iter->second;
         }
      }while ( FALSE );
      PD_TRACE_EXIT ( SDB_RTNCOPROFAC_GETCOMPRO2 ) ;
      return pProcesser;
   }
}
