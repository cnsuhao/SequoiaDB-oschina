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

   Source File Name = aggrGroup.hpp

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
#include "pmdMongoAccess.hpp"
#include "ossUtil.hpp"
#include "pmdOptions.hpp"
#include "pmdMongoSession.hpp"

namespace engine {
   PMD_EXPORT_ACCESSPROTOCOL_DLL( pmdMongoAccess )
}

INT32 _pmdMongoAccess::init( engine::IResource *pResource )
{
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

   return SDB_OK ;
}

INT32 _pmdMongoAccess::active()
{
   return SDB_OK ;
}

INT32 _pmdMongoAccess::deactive()
{
   return SDB_OK ;
}

INT32 _pmdMongoAccess::fini()
{
   return SDB_OK ;
}

const CHAR * _pmdMongoAccess::getServiceName() const
{
   SDB_ASSERT( '\0' != _serviceName[0], "service name should not be empty" ) ;
   return _serviceName ;
}

engine::pmdSession * _pmdMongoAccess::getSession( SOCKET fd,
                                                 engine::IProcessor *pProcessor )
{
   pmdMongoSession *session = NULL ;
   if ( NULL != pProcessor )
   {
      session = SDB_OSS_NEW pmdMongoSession( fd ) ;
      session->attachProcessor( pProcessor ) ;
   }

   session->attachProcessor( pProcessor ) ;

   return session ;
}

void _pmdMongoAccess::releaseSession( engine::pmdSession *pSession )
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

void _pmdMongoAccess::_release()
{
   ossMemset( _serviceName, 0, OSS_MAX_SERVICENAME + 1 ) ;

   if ( NULL != _resource )
   {
      _resource = NULL ;
   }
}
