#ifndef _SDB_MONGO_CONVERTER_HPP_
#define _SDB_MONGO_CONVERTER_HPP_

#include "util.hpp"
#include "oss.hpp"
#include "mongodef.hpp"
#include "commands.hpp"

class command ;

class mongoConverter : public baseConverter, public SDBObject
{
public:
   mongoConverter() : _cmd( NULL )
   {
      _bigEndian = checkBigEndian() ;
      parser.setEndian( _bigEndian ) ;
   }

   ~mongoConverter()
   {

   }

   BOOLEAN isBigEndian() const
   {
      return _bigEndian ;
   }

   BOOLEAN isGetLastError() const
   {
      const CHAR *ptr = NULL ;
      ptr = ossStrstr( _cmd->name(), "getLastError" ) ;
      if ( NULL == ptr )
      {
         ptr = ossStrstr( _cmd->name(), "getlasterror" ) ;
      }
      return NULL != ptr ;
   }

   void resetCommand()
   {
      _cmd = NULL ;
   }

   virtual CONVERT_ERROR convert( fixedStream &out ) ;
   virtual CONVERT_ERROR reConvert( fixedStream *in, fixedStream &out ) ;

private:
   BOOLEAN _bigEndian ;
   command *_cmd ;
   mongoParser parser ;
};
#endif
