#include "mongoConverter.hpp"
#include "mongodef.hpp"
#include "commands.hpp"

CONVERT_ERROR mongoConverter::convert( fixedStream &out )
{
   CONVERT_ERROR rc = CON_OK ;
   parser.init( _msgdata, _msglen ) ;

   if ( dbInsert == parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "insert" ) ;
   }   
   else if ( dbDelete == parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "delete" ) ;
   }
   else if ( dbUpdate == parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "update" ) ;
   }
   else if ( dbQuery == parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "query" ) ;
   }
   else if ( dbGetMore == parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "getMore" ) ;
   }
   else if ( dbKillCursors == parser.opCode )
   {
      _cmd = commandMgr::instance()->findCommand( "killCursors" ) ;
   }

   if ( NULL == _cmd )
   {
      goto error ;
   }

   rc = _cmd->convertRequest( parser, out ) ;
   if ( CON_OK != rc )
   {
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

CONVERT_ERROR mongoConverter::reConvert( fixedStream *in, fixedStream &out )
{
   CONVERT_ERROR rc = CON_OK ;

done:
   return rc ;
error:
   goto done ;
}