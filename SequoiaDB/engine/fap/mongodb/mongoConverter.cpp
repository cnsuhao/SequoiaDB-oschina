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

   Source File Name = mongoConverter.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/27/2015  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#include "mongoConverter.hpp"
#include "mongodef.hpp"
#include "commands.hpp"

INT32 mongoConverter::convert( std::vector<msgBuffer*> &out )
{
   INT32 rc = SDB_OK ;
   _parser.init( _msgdata, _msglen ) ;

   if ( dbInsert == _parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "insert" ) ;
   }   
   else if ( dbDelete == _parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "delete" ) ;
   }
   else if ( dbUpdate == _parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "update" ) ;
   }
   else if ( dbQuery == _parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "query" ) ;
   }
   else if ( dbGetMore == _parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "getMore" ) ;
   }
   else if ( dbKillCursors == _parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "killCursors" ) ;
   }

   if ( NULL == _cmd )
   {
      goto error ;
   }

   rc = _cmd->convertRequest( _parser, out ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 mongoConverter::reConvert( msgBuffer *in, msgBuffer &out )
{
   return SDB_OK ;
}
