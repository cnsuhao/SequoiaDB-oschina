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

   Source File Name = pd.hpp

   Descriptive Name = Problem Determination Header

   When/how to use: this program may be used on binary and text-motionatted
   versions of PD component. This file contains declare of PD functions.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "msgCatalog.hpp"
#include "pd.hpp"
#include "../bson/bsonobj.h"
#include "pmd.hpp"
#include "pdTrace.hpp"
#include "msgTrace.hpp"


using namespace bson ;
namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB_MSGPASCATGRPRES, "msgParseCatGroupRes" )
   INT32 msgParseCatGroupRes( const _MsgCatGroupRes *msg,
                              CLS_GROUP_VERSION &version,
                              string &groupName,
                              map<UINT64, _netRouteNode> &group,
                              UINT32 *pPrimary )
   {
      SDB_ASSERT( NULL != msg, "data should not be NULL" ) ;
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MSGPASCATGRPRES );

      if ( SDB_OK != msg->header.res )
      {
         rc = msg->header.res ;
         goto done ;
      }
      {
         UINT32 groupID = 0 ;
         rc = msgParseCatGroupObj( (const CHAR*)msg + sizeof( _MsgCatGroupRes ),
                                     version, groupID, groupName,
                                     group, pPrimary ) ;
      }                               
   done :
      PD_TRACE_EXITRC ( SDB_MSGPASCATGRPRES, rc );
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_MSGPASCATGRPOBJ, "msgParseCatGroupObj" )
   INT32 msgParseCatGroupObj( const CHAR* objdata,
                              CLS_GROUP_VERSION &version,
                              UINT32 &groupID,
                              string &groupName,
                              map<UINT64, _netRouteNode> &group,
                              UINT32 *pPrimary )
   {
      SDB_ASSERT( NULL != objdata, "data should not be NULL" ) ;
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_MSGPASCATGRPOBJ );
      try
      {
      BSONObj obj( objdata ) ;
      PD_LOG( PDDEBUG, "bson: %s", obj.toString().c_str()) ;

      BSONElement ele = obj.getField( CAT_GROUPID_NAME ) ;
      if ( NumberInt != ele.type() )
      {
         PD_LOG( PDWARNING, "parse [%s] err",CAT_GROUPID_NAME ) ;
         goto error ;
      }
      groupID = ele.Int() ;

      ele = obj.getField( CAT_GROUPNAME_NAME ) ;
      if ( String != ele.type() )
      {
         PD_LOG ( PDWARNING, "parse [%s] err", CAT_GROUPNAME_NAME ) ;
         goto error ;
      }
      groupName = ele.str() ;

      ele = obj.getField( CAT_ROLE_NAME ) ;
      if ( NumberInt != ele.type() )
      {
         PD_LOG( PDWARNING, "parse [%s] err", CAT_ROLE_NAME ) ;
         goto error ;
      }

      ele = obj.getField( CAT_VERSION_NAME ) ;
      if ( ele.eoo() || NumberInt != ele.type() )
      {
         PD_LOG( PDWARNING, "parse [%s] err", CAT_VERSION_NAME ) ;
         goto error ;
      }
      version = ele.Int() ;

      if ( pPrimary )
      {
         *pPrimary = 0 ;
         ele = obj.getField ( CAT_PRIMARY_NAME ) ;
         if ( NumberInt == ele.type() )
         {
            *pPrimary = ele.numberInt() ;
         }
         else if ( !ele.eoo() )
         {
            PD_LOG( PDWARNING, "parse [%s] err", CAT_PRIMARY_NAME ) ;
            goto error ;
         }
      }

      ele = obj.getField( CAT_GROUP_NAME ) ;
      if ( !ele.eoo() && Array != ele.type() )
      {
         PD_LOG( PDWARNING, "parse [%s] err", CAT_GROUP_NAME ) ;
         goto error ;
      }

      if ( ele.isABSONObj() )
      {
      BSONObjIterator i(ele.embeddedObject() ) ;
      while ( i.more() )
      {
         BSONElement nextEle = i.next() ;
         if ( !nextEle.isABSONObj() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDWARNING, "parse [%s] err", CAT_GROUP_NAME ) ;
            break ;
         }
         BSONObj next = nextEle.embeddedObject() ;
         UINT16 nodeID = 0 ;
         _netRouteNode route ;
         BSONElement  node = next.getField( CAT_NODEID_NAME ) ;
         if ( node.eoo() || NumberInt != node.type() )
         {
            PD_LOG( PDWARNING, "parse [%s] err", CAT_NODEID_NAME ) ;
            goto error ;
         }
         nodeID = node.Int() ;
         node = next.getField( CAT_HOST_FIELD_NAME ) ;
         if ( node.eoo() || String != node.type() )
         {
            PD_LOG( PDWARNING, "parse [%s] err", CAT_HOST_FIELD_NAME ) ;
            goto error ;
         }
         {
         UINT32 len = node.String().size() < OSS_MAX_HOSTNAME ?
                      node.String().size() : OSS_MAX_HOSTNAME ;
         ossMemcpy( route._host, node.String().c_str(), len ) ;
         route._host[len] = '\0';
         }

         node = next.getField( CAT_SERVICE_FIELD_NAME ) ;
         if ( node.eoo() || Array != node.type() )
         {
            PD_LOG( PDWARNING, "parse [%s] err", CAT_SERVICE_FIELD_NAME ) ;
            goto error ;
         }
         if ( !node.isABSONObj() )
         {
            PD_LOG( PDWARNING, "parse [%s] err", CAT_SERVICE_FIELD_NAME ) ;
            goto error ;
         }

         {
         BSONObjIterator j(node.embeddedObject()) ;
         while ( j.more() )
         {
            BSONElement nextJ = j.next() ;
            if ( !nextJ.isABSONObj() )
            {
               PD_LOG( PDWARNING, "parse [%s] err", CAT_SERVICE_FIELD_NAME ) ;
               goto error ;
            }
            {
            BSONObj service = nextJ.embeddedObject() ;
            BSONElement serele ;
            UINT16 serID = 0 ;
            serele = service.getField( CAT_SERVICE_TYPE_FIELD_NAME ) ;
            if ( serele.eoo() || NumberInt != serele.type() )
            {
               PD_LOG( PDWARNING, "parse [%s] err", CAT_SERVICE_TYPE_FIELD_NAME ) ;
               goto error ;
            }
            if ( MSG_ROUTE_REPL_SERVICE != serele.Int() &&
                 MSG_ROUTE_SHARD_SERVCIE != serele.Int() &&
                 MSG_ROUTE_CAT_SERVICE != serele.Int() &&
                 MSG_ROUTE_LOCAL_SERVICE != serele.Int() )
            {
               PD_LOG( PDWARNING, "unknown service type: %d",
                       serele.Int() ) ;
               continue ;
            }
            serID = serele.Int() ;
            serele = service.getField( CAT_SERVICE_NAME_FIELD_NAME ) ;
            if ( serele.eoo() || String != serele.type() )
            {
               PD_LOG( PDWARNING, "parse [%s] err",
                       CAT_SERVICE_NAME_FIELD_NAME ) ;
               goto error ;
            }
            route._service[serID] = serele.String() ;
            }
         }
         route._id.columns.groupID = groupID ;
         route._id.columns.nodeID = nodeID ;
         route._id.columns.serviceID = MSG_ROUTE_REPL_SERVICE ;
         group.insert(make_pair(route._id.value,  route )) ;
         }
      }
      }
      }
      catch ( std::exception &e )
      {
         pdLog ( PDERROR, __FUNC__, __FILE__, __LINE__,
                 "unexpected exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_MSGPASCATGRPOBJ, rc );
      return rc ;
   error:
      rc = SDB_INVALIDARG ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_GETSVCNAME, "getServiceName" )
   std::string getServiceName ( bson::BSONElement &beService, INT32 serviceType )
   {
      PD_TRACE_ENTRY ( SDB_GETSVCNAME );
      std::string strName;
      try
      {
         if ( beService.type() != Array )
         {
            goto done ;
         }

         BSONObjIterator i( beService.embeddedObject() );
         while ( i.more() )
         {
            BSONElement beTmp = i.next();
            BSONObj boTmp = beTmp.embeddedObject();
            BSONElement beServiceType = boTmp.getField(CAT_SERVICE_TYPE_FIELD_NAME);
            if ( beServiceType.eoo() || !beServiceType.isNumber() )
            {
               goto done ;
            }
            if ( beServiceType.numberInt() == serviceType )
            {
               BSONElement beServiceName = boTmp.getField(CAT_SERVICE_NAME_FIELD_NAME);
               if ( beServiceName.type() != String )
               {
                  goto done ;
               }
               strName = beServiceName.str() ;
               goto done ;
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR,
                  "unexpected exception: %s", e.what() ) ;
      }
   done :
      PD_TRACE1 ( SDB_GETSVCNAME, PD_PACK_STRING(strName.c_str()) );
      PD_TRACE_EXIT ( SDB_GETSVCNAME );
      return strName ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB_GETSHDSVCNAME, "getShardServiceName" )
   const CHAR * getShardServiceName ( BSONElement &beService )
   {
      PD_TRACE_ENTRY ( SDB_GETSHDSVCNAME );
      const CHAR *ret = NULL ;
      try
      {
         if ( beService.type() != Array )
         {
            goto done ;
         }

         BSONObjIterator i( beService.embeddedObject() );
         while ( i.more() )
         {
            BSONElement beTmp = i.next();
            BSONObj boTmp = beTmp.embeddedObject();
            BSONElement beServiceType = boTmp.getField(CAT_SERVICE_TYPE_FIELD_NAME);
            if ( beServiceType.eoo() || !beServiceType.isNumber() )
            {
               goto done ;
            }
            if ( beServiceType.numberInt() == MSG_ROUTE_SHARD_SERVCIE )
            {
               BSONElement beServiceName = boTmp.getField(CAT_SERVICE_NAME_FIELD_NAME);
               if ( beServiceName.type() != String )
               {
                  goto done ;
               }
               ret = beServiceName.valuestr() ;
               goto done ;
            }
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR,
                  "unexpected exception: %s", e.what() ) ;
      }
   done :
      if ( NULL == ret )
         PD_TRACE1 ( SDB_GETSHDSVCNAME, PD_PACK_NULL );
      else
         PD_TRACE1 ( SDB_GETSHDSVCNAME, PD_PACK_STRING(ret) );
      PD_TRACE_EXIT ( SDB_GETSHDSVCNAME );
      return ret ;
   }//end of getShardServiceName()
}
