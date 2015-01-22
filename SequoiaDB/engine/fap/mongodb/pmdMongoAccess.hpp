#ifndef _SDB_MONGO_ACCESS_HPP_
#define _SDB_MONGO_ACCESS_HPP_

#include "pmdAccessProtocolBase.hpp"
#include "sdbInterface.hpp"

#define ACCESS_FOR_MONGODB_CLIENT "server for mongodb client"

class pmdMongoAccess : public engine::IPmdAccessProtocol, public SDBObject
{
public:
   pmdMongoAccess() {}
   virtual ~pmdMongoAccess() {}

   virtual const CHAR *name() const
   {
      return ACCESS_FOR_MONGODB_CLIENT;
   }


public:
   virtual INT32 init( engine::IResource *pResource ) ;
   virtual INT32 active() ;
   virtual INT32 deactive() ;
   virtual INT32 fini() ;

   virtual const CHAR *getServiceName() const ;
   virtual engine::pmdSession *getSession( SOCKET fd,
                                           engine::IProcessor *pProcessor ) ;
   virtual void releaseSession( engine::pmdSession *pSession ) ; 

private:
   void _release() ;

private:
   engine::IResource *_resource ;
   CHAR _serviceName[ OSS_MAX_SERVICENAME + 1 ] ;

   static const INT32 PORT_OFFSET = 7 ;
};
#endif
