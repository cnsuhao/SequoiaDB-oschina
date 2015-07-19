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

   Source File Name = commands.hpp

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
#ifndef _SDB_MONGO_COMMANDS_HPP_
#define _SDB_MONGO_COMMANDS_HPP_

#include <map>
#include "util.hpp"
#include "mongodef.hpp"

class command
{
public:
   command( const CHAR *cmdName, const char* secondName = NULL ) ;
   virtual ~command() {} ;

   const CHAR *name() const
   {
      return _cmdName ;
   }

   virtual INT32 convertRequest( mongoParser &parser, msgBuffer &sdbMsgs )
   {
      return SDB_OK ;
   }

protected:
   const CHAR *_cmdName ;
} ;

class commandMgr
{
public:
   static commandMgr *instance() ;

   void addCommand( const std::string &name, command *cmd )
   {
      command *tmp = _cmdMap[name] ;
      if ( NULL != tmp )
      {
      }
      _cmdMap[name] = cmd ;
   }

   command *findCommand( const std::string &cmdName )
   {
      command *cmd = NULL ;
      std::map< std::string, command* >::iterator it = _cmdMap.find( cmdName ) ;
      if ( _cmdMap.end() != it )
      {
         cmd = it->second ;
      }

      return cmd ;
   }

public:
   commandMgr()
   {
      _cmdMap.clear() ;
   }

   ~commandMgr()
   {
      _cmdMap.clear() ;
   }

private:
   std::map< std::string, command *> _cmdMap ;
} ;

#define __DECLARE_COMMAND( cmd, secondName, cmdClass )               \
class cmdClass : public command                                      \
{                                                                    \
public:                                                              \
   cmdClass() : command( cmd, secondName ) {}                        \
   virtual INT32 convertRequest( mongoParser &parser,                \
                                 msgBuffer &sdbMsgs ) ;              \
} ;

#define __DECLARE_COMMAND_VAR( commandClass, var )                   \
        commandClass var ;


#define DECLARE_COMMAND( command )                                   \
        __DECLARE_COMMAND( #command, NULL, command##Command )

#define DECLARE_COMMAND_WITH_SECONDNAME( command, secondName )       \
        __DECLARE_COMMAND( #command, secondName, command##Command )

#define DECLARE_COMMAND_VAR( command )                               \
        __DECLARE_COMMAND_VAR( command##Command, command##Cmd )

DECLARE_COMMAND( insert )
DECLARE_COMMAND( delete )
DECLARE_COMMAND( update )
DECLARE_COMMAND( query )
DECLARE_COMMAND( getMore )
DECLARE_COMMAND( killCursors )

DECLARE_COMMAND( createCS )   // create collection space, NOT in mongodb
DECLARE_COMMAND( create )     // create collection
DECLARE_COMMAND( drop )       // drop   collection
DECLARE_COMMAND( count )
DECLARE_COMMAND( aggregate )
DECLARE_COMMAND( dropDatabase )

DECLARE_COMMAND( createIndexes )
DECLARE_COMMAND_WITH_SECONDNAME( deleteIndexes, "dropIndexes" )
DECLARE_COMMAND( listIndexes )

DECLARE_COMMAND( getlasterror )
DECLARE_COMMAND_WITH_SECONDNAME( ismaster, "isMaster" )

DECLARE_COMMAND( ping )

#endif
