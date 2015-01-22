#ifndef _SDB_MSG_CONVERTER_UTIL_HPP_
#define _SDB_MSG_CONVERTER_UTIL_HPP_

#include "iostream"
#include "msgBuffer.hpp"
#include "../../bson/bson.h"

enum CONVERT_ERROR
{
   CON_OK = 0,                ///< convert successfully
   CON_NONE_ORIGINAL,         ///< original data is empty
   CON_INVALIDARG,            ///< lack of data
   CON_COMMAND_UNSUPPORTED,   ///< command unsupported right now
   CON_OTHER_ERROR,           ///< unknown error
} ;

class _IConverter
{
public:
   virtual ~_IConverter() {}

   virtual CONVERT_ERROR convert( fixedStream &out ) = 0 ;

   virtual CONVERT_ERROR reConvert( fixedStream &in, fixedStream &out ) = 0 ;
};

typedef _IConverter IConverter ;
class _baseConverter/*, public IConverter*/
{
public:
   _baseConverter() : _msglen( 0 ), _msgdata( NULL )
   {}

   virtual ~_baseConverter()
   {
      if ( NULL != _msgdata )
      {
         _msgdata = NULL ;
      }
   }

   void loadFrom( const CHAR *msg, const INT32 len )
   {
      if ( NULL == msg )
      {
         return ;
      }

      _msgdata = const_cast< CHAR * >( msg ) ;
      _msglen  = len ;
   }

   virtual CONVERT_ERROR convert( fixedStream &out )
   {
      return CON_OK ;
   }

   virtual CONVERT_ERROR reConvert( fixedStream *in, fixedStream &out )
   {
      return CON_OK ;
   }

protected:
   INT32   _msglen ;
   CHAR   *_msgdata ;
} ;

typedef _baseConverter baseConverter ;
/*
#define __DECLARE_PROCESSOR( className )                                       \
class className : public baseConverter                                         \
{                                                                              \
public:                                                                        \
   className( const CHAR *msgdata,                                             \
              const INT32 msgLen )                                             \
      : baseConverter( sdbmsg, msgLen )                                        \
   {}                                                                          \
                                                                               \
   ~className() {}                                                             \
                                                                               \
   virtual CONVERT_ERROR convert( fixedStream &buffer ) ;                      \
   virtual CONVERT_ERROR reConvert( fixedStream &result, fixedStream &reply ) ;\
} ;
*/
inline BOOLEAN checkBigEndian()
{
   BOOLEAN bigEndian = FALSE ;
   union
   {
      unsigned int i ;
      unsigned char s[4] ;
   } c ;

   c.i = 0x12345678 ;
   if ( 0x12 == c.s[0] )
   {
      bigEndian = TRUE ;
   }

   return bigEndian ;
}

inline bson::BSONObj removeField( bson::BSONObj &obj, const string& name )
{
   bson::BSONObjBuilder b;
   bson::BSONObjIterator i(obj);
   while ( i.more() ) {
   bson::BSONElement e = i.next();
   const char *fname = e.fieldName();
   if ( name != fname )
      b.append(e);
   }
   return b.obj();
}
#endif
