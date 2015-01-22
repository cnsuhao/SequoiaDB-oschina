#ifndef _SDB_MONGO_COMMANDS_HPP_
#define _SDB_MONGO_COMMANDS_HPP_

#include <map>
#include "util.hpp"
#include "mongodef.hpp"

class command
{
public:
   command( const CHAR *cmdName ) ;
   virtual ~command() {} ;

   const CHAR *name() const
   {
      return _cmdName ;
   }

   virtual CONVERT_ERROR convertRequest( mongoParser &parser, fixedStream &sdbMsg )
   {
      return CON_OK ;
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

private:
   commandMgr()
   {
      _cmdMap.clear() ;
   }

   ~commandMgr()
   {
      std::map< std::string, command* >::iterator it = _cmdMap.begin() ;
      for ( ; it != _cmdMap.end(); ++it )
      {
         command *cmd = it->second ;
         delete cmd ;
         cmd = NULL ;
      }
   }

   std::map< std::string, command *> _cmdMap ;
} ;

#define __DECLARE_COMMAND( cmd, cmdClass )                        \
class cmdClass : public command                                   \
{                                                                 \
public:                                                           \
   cmdClass() : command( cmd ) {}                                 \
   virtual CONVERT_ERROR convertRequest( mongoParser &parser,     \
                                         fixedStream &sdbMsg ) ;  \
} ;

#define __DECLARE_COMMAND_VAR( commandClass, var )                \
        commandClass var ;


#define DECLARE_COMMAND( command )                                \
        __DECLARE_COMMAND( #command, command##Command )

#define DECLARE_COMMAND_VAR( command )                            \
        __DECLARE_COMMAND_VAR( command##Command, command##Cmd )



DECLARE_COMMAND( insert )
DECLARE_COMMAND( remove )
DECLARE_COMMAND( update )
DECLARE_COMMAND( query )
DECLARE_COMMAND( getMore )
DECLARE_COMMAND( killCursors )
DECLARE_COMMAND( reply )

DECLARE_COMMAND( getnonce )
DECLARE_COMMAND( authenticate )
DECLARE_COMMAND( create )
DECLARE_COMMAND( dropCollection )
DECLARE_COMMAND( drop )
DECLARE_COMMAND( count )
DECLARE_COMMAND( aggregate )

DECLARE_COMMAND( createIndex )
DECLARE_COMMAND( dropIndexes )
DECLARE_COMMAND( getIndexes )

DECLARE_COMMAND( getLastError )

#endif
