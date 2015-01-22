#include "pmdMongoAccess.hpp"
#include "ossUtil.h"
#include "pmdOptions.h"
#include "pmdMongoSession.hpp"

namespace engine {
   PMD_EXPORT_ACCESSPROTOCOL_DLL( pmdMongoAccess )
}

INT32 pmdMongoAccess::init( engine::IResource *pResource )
{
   INT32 rc        = SDB_OK ;
   UINT16 basePort = 0 ;
   if ( NULL != pResource )
   {
      _resource = pResource ;
   }

   ossMemset( (void *)_serviceName, 0, OSS_MAX_SERVICENAME + 1 ) ;
   if ( NULL != _resource )
   {
      basePort = _resource->getLocalPort() ;
      ossItoa( basePort + PORT_OFFSET, _serviceName, OSS_MAX_SERVICENAME ) ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 pmdMongoAccess::active()
{
   return SDB_OK ;
}

INT32 pmdMongoAccess::deactive()
{
   return SDB_OK ;
}

INT32 pmdMongoAccess::fini()
{
   return SDB_OK ;
}

const CHAR * pmdMongoAccess::getServiceName() const
{
   SDB_ASSERT( '\0' == _serviceName[0], "service name should not be empty" ) ;
   return _serviceName ;
}

engine::pmdSession * pmdMongoAccess::getSession( SOCKET fd,
                                                 engine::IProcessor *pProcessor )
{
   pmdMongoSession *session = NULL ;
   if ( NULL != pProcessor )
   {
      session = SDB_OSS_NEW pmdMongoSession( fd ) ;
      session->attachProcessor( pProcessor ) ;
   }

   return session ;
}

void pmdMongoAccess::releaseSession( engine::pmdSession *pSession )
{
   pmdMongoSession *session = dynamic_cast< pmdMongoSession *>( pSession ) ;
   if ( NULL == session )
   {
      session->detachProcessor() ;
      SDB_OSS_DEL session ;
      session = NULL ;
   }

   _release() ;
}

void pmdMongoAccess::_release()
{
   ossMemset( _serviceName, 0, OSS_MAX_SERVICENAME + 1 ) ;

   if ( NULL != _resource )
   {
      _resource = NULL ;
   }
}
