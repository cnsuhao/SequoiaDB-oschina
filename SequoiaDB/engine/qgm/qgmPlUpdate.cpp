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

   Source File Name = qgmPlUpdate.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains declare for QGM operators

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/09/2013  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "qgmPlUpdate.hpp"
#include "qgmConditionNodeHelper.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "rtn.hpp"
#include "rtnCoordUpdate.hpp"
#include "msgMessage.hpp"
#include "utilStr.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"

using namespace bson ;

namespace engine
{
   _qgmPlUpdate::_qgmPlUpdate( const _qgmDbAttr &collection,
                               const qgmDbAttrVec &columns,
                               const qgmOPFieldVec &values,
                               _qgmConditionNode *condition )
   :_qgmPlan( QGM_PLAN_TYPE_UPDATE, _qgmField() ),
    _collection( collection )
   {
      INT32 rc = SDB_OK ;
      if ( columns.size() != values.size() )
      {
         SDB_ASSERT( FALSE, "impossible" ) ;
         goto done ;
      }

      try
      {
         BSONObjBuilder builder, vbuilder ;

         qgmDbAttrVec::const_iterator it1 = columns.begin() ;
         qgmOPFieldVec::const_iterator it2 = values.begin() ;
         for ( ; it1 != columns.end(); it1++, it2++ )
         {
             if ( SQL_GRAMMAR::DIGITAL == it2->type )
             {
                if ( !vbuilder.appendAsNumber(  it1->toString(),
                                     it2->value.attr().toString()) )
                {
                   PD_LOG( PDERROR, "failed to append as number:%s",
                           it2->value.attr().toString().c_str() ) ;
                   goto done ;
                }
             }
             else if ( SQL_GRAMMAR::DATE == it2->type )
             {
                Date_t t ;
                time_t tm ;
                rc = utilStr2TimeT( it2->value.attr().toString().c_str(),
                                    tm ) ;
                if ( SDB_OK != rc )
                {
                   PD_LOG( PDDEBUG, "failed to parse to Date_t:%s",
                           it2->value.toString().c_str() ) ;
                   goto done ;
                }

                t = tm ;
                vbuilder.appendDate( it1->toString(), t ) ;
             }
             else
             {
                vbuilder.append( it1->toString(),
                                it2->value.attr().toString() ) ;
             }
         }

         builder.append( "$set", vbuilder.obj() ) ;
         _updater = builder.obj() ;

         if ( NULL != condition )
         {
            _qgmConditionNodeHelper tree( condition ) ;
            _condition = tree.toBson() ;
         }
      }
      catch ( std::exception &e )
      {
        PD_LOG( PDERROR, "unexcepted err happened:%s", e.what() ) ;
        goto done ;
      }

      _initialized = TRUE ;
   done:
      return ;
   }

   _qgmPlUpdate::~_qgmPlUpdate()
   {

   }

   // PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLUPDATE__EXEC, "_qgmPlUpdate::_execute" )
   INT32 _qgmPlUpdate::_execute( _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB__QGMPLUPDATE__EXEC ) ;
      INT32 rc = SDB_OK ;
      _SDB_KRCB *krcb = pmdGetKRCB() ;
      SDB_ROLE role = krcb->getDBRole() ;
      BSONObj hint ;
      BSONObj *err = NULL ;

      if ( SDB_ROLE_COORD == role )
      {
         rtnCoordUpdate update ;
         CHAR *msg = NULL ;
         INT32 size = 0 ;
         MsgOpReply dummy ;
         rc = msgBuildUpdateMsg( &msg, &size,
                                 _collection.toString().c_str(),
                                 0, 0,
                                 &_condition,
                                 &_updater,
                                 &hint ) ;

         if ( SDB_OK != rc )
         {
            SDB_OSS_FREE( msg ) ;
            msg = NULL ;
            goto error ;
         }

         rc = update.execute( msg, *((SINT32 *)msg),
                              NULL, eduCB, dummy, &err ) ;
         SDB_ASSERT( NULL == err, "impossible" ) ;
         SDB_OSS_FREE( msg ) ;
         msg = NULL ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
         SDB_DPSCB *dpsCB = krcb->getDPSCB() ;

         if ( dpsCB && eduCB->isFromLocal() && !dpsCB->isLogLocal() )
         {
             dpsCB = NULL ;
         }
         rc = rtnUpdate( _collection.toString().c_str(),
                         _condition,
                         _updater,
                         hint,
                         0, eduCB, dmsCB, dpsCB ) ;
         if( SDB_OK != rc )
         {
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPLUPDATE__EXEC, rc ) ;
      return rc ;
   error:
      goto done ;
   }
}
