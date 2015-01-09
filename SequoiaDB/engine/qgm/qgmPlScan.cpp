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

   Source File Name = qgmPlScan.cpp

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

#include "qgmPlScan.hpp"
#include "qgmConditionNodeHelper.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "netMultiRouteAgent.hpp"
#include "ossMem.hpp"
#include "msgMessage.hpp"
#include "qgmUtil.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"
#include <sstream>

namespace engine
{
   _qgmPlScan::_qgmPlScan( const qgmDbAttr &collection,
                           const qgmOPFieldVec &selector,
                           const BSONObj &orderby,
                           const BSONObj &hint,
                           INT64 numSkip,
                           INT64 numReturn,
                           const qgmField &alias,
                           _qgmConditionNode *condition )
   :_qgmPlan( QGM_PLAN_TYPE_SCAN, alias ),
   _invalidPredicate( FALSE ),
   _contextID( -1 ),
   _collection( collection ),
   _orderby( orderby ),
   _hint( hint ),
   _skip( numSkip ),
   _return( numReturn ),
   _dmsCB( NULL ),
   _rtnCB( NULL ),
   _conditionNode( NULL )
   {
      pmdKRCB *krcb = pmdGetKRCB() ;
      _dbRole = krcb->getDBRole() ;
      _dmsCB = krcb->getDMSCB() ;
      _rtnCB = krcb->getRTNCB() ;

      if ( NULL != condition )
      {
         _conditionNode = condition ;
      }

      _selector.load( selector ) ;
      _initialized = TRUE ;
   }

   _qgmPlScan::~_qgmPlScan()
   {
      _killContext() ;
      SAFE_OSS_DELETE( _conditionNode ) ;
   }

   void _qgmPlScan::close()
   {
      _killContext() ;
      return ;
   }

   string _qgmPlScan::toString() const
   {
      stringstream ss ;

      ss << "Type:" << qgmPlanType( _type ) << '\n';
      ss << "Collection:" << _collection.toString() << '\n' ;
      if ( !_alias.empty() )
      {
         ss << "Alias:" << _alias.toString() << '\n';
      }
      if ( !_selector.empty() )
      {
         ss << "Selector:"
            << _selector.toString() << '\n';
      }
      if ( NULL != _conditionNode )
      {
         qgmConditionNodeHelper tree( _conditionNode ) ;
         ss << "Condition:"
            << tree.toJson() << '\n';
      }
      if ( !_orderby.isEmpty() )
      {
         ss << "Sort:"
            << _orderby.toString() << '\n';
      }
      if ( !_hint.isEmpty() )
      {
         ss << "Hint:"
            << _hint.toString() << '\n';
      }
      if ( 0 != _skip )
      {
         ss << "Skip:"
            << _skip << '\n';
      }
      if ( -1 != _return )
      {
         ss << "Limit:"
            << _return << '\n';
      }
      return ss.str() ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLSCAN__EXEC, "_qgmPlScan::_execute" )
   INT32 _qgmPlScan::_execute( _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB__QGMPLSCAN__EXEC ) ;
      INT32 rc = SDB_OK ;

      SDB_ASSERT ( _input.size() == 0, "impossible" ) ;

      _invalidPredicate = FALSE ;
      _contextID = -1 ;

      _qgmConditionNodeHelper tree( _conditionNode ) ;
      _condition = tree.toBson( FALSE ) ;
      rc = SDB_ROLE_COORD == _dbRole ?
           _executeOnCoord( eduCB ) : _executeOnData( eduCB ) ;

      if ( SDB_RTN_INVALID_PREDICATES == rc )
      {
         rc = SDB_OK ;
         _invalidPredicate = TRUE ;
      }
      else if ( SDB_OK != rc )
      {
         goto error ;
      }
      else
      {
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMPLSCAN__EXEC, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLSCAN__EXECONDATA, "_qgmPlScan::_executeOnData" )
   INT32 _qgmPlScan::_executeOnData( _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB__QGMPLSCAN__EXECONDATA ) ;
      INT32 rc = SDB_OK ;

      BSONObj selector = _selector.selector() ;
      rc = rtnQuery ( _collection.toString().c_str(), selector, _condition,
                      _orderby, _hint, 0, eduCB, _skip, _return,
                      _dmsCB, _rtnCB, _contextID, NULL, FALSE ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPLSCAN__EXECONDATA, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLSCAN__EXECONCOORD, "_qgmPlScan::_executeOnCoord" )
   INT32 _qgmPlScan::_executeOnCoord( _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB__QGMPLSCAN__EXECONCOORD ) ;
      INT32 rc = SDB_OK ;
      INT32 bufSize = 0 ;
      CHAR *qMsg = NULL ;
      MsgOpReply dummyReply ;
      BSONObj *err = NULL ;
      BSONObj selector = _selector.selector() ;

      rc = msgBuildQueryMsg ( &qMsg, &bufSize,
                              _collection.toString().c_str(),0,
                              0, _skip, _return,
                              &_condition, &selector,
                              &_orderby, &_hint ) ;

      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _coordQuery.execute ( qMsg, *(SINT32*)qMsg, NULL, eduCB,
                                 dummyReply, &err ) ;
      SDB_ASSERT( NULL == err, "impossible" ) ;
      PD_RC_CHECK ( rc, PDERROR,
                    "Failed to execute coordQuery, rc = %d", rc ) ;

      _contextID = dummyReply.contextID ;

   done:
      if ( NULL != qMsg )
      {
         SDB_OSS_FREE( qMsg ) ;
         qMsg = NULL ;
      }
      PD_TRACE_EXITRC( SDB__QGMPLSCAN__EXECONCOORD, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLSCAN__FETCHNEXT, "_qgmPlScan::_fetchNext" )
   INT32 _qgmPlScan::_fetchNext ( qgmFetchOut &next )
   {
      PD_TRACE_ENTRY( SDB__QGMPLSCAN__FETCHNEXT ) ;
      INT32 rc = SDB_OK ;
      const CHAR *getMoreRes = NULL ;

      if ( _invalidPredicate )
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }

      rc = _fetch( getMoreRes ) ;

      if ( SDB_OK != rc && SDB_DMS_EOC != rc )
      {
         goto error ;
      }
      else if ( SDB_OK == rc )
      {
         try
         {
            if ( _selector.hasAlias() )
            {
               rc = _selector.select( BSONObj(getMoreRes), next.obj ) ;
               if ( SDB_OK != rc )
               {
                  goto error ;
               }
            }
            else
            {
               next.obj = BSONObj( getMoreRes ) ;
            }
         }
         catch ( std::exception &e )
         {
            PD_RC_CHECK ( SDB_SYS, PDERROR,
                          "unexpected err happened when fetching: %s",
                          e.what() ) ;
         }

         if ( !_merge )
         {
            next.alias = _alias ;
         }
      }
      else
      {
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMPLSCAN__FETCHNEXT, rc ) ;
      return rc ;
   error:
      if ( SDB_DMS_EOC == rc )
         _contextID = -1 ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLSCAN__FETCH, "_qgmPlScan::_fetch" )
   INT32 _qgmPlScan::_fetch( const CHAR *&result )
   {
      PD_TRACE_ENTRY( SDB__QGMPLSCAN__FETCH );
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( _contextID != -1,
                   "context id must be initialized" ) ;

      rtnContextBuf buffObj ;
      rc = rtnGetMore( _contextID, 1, buffObj, _eduCB, _rtnCB ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG( PDERROR, "Failed to getmore from context[%lld], rc = %d",
                    _contextID, rc ) ;
         }
         goto error ;
      }
      result = buffObj.data() ;

   done:
      PD_TRACE_EXITRC( SDB__QGMPLSCAN__FETCH, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   void _qgmPlScan::_killContext()
   {
      if ( -1 != _contextID && NULL != _eduCB )
      {
         rtnKillContexts( 1, &_contextID, _eduCB, _rtnCB ) ;
         _contextID = -1 ;
      }
   }
}

