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

   Source File Name = qgmPlInsert.cpp

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

#include "qgmPlInsert.hpp"
#include "pmd.hpp"
#include "pmdCB.hpp"
#include "rtnCoordInsert.hpp"
#include "rtn.hpp"
#include "msgMessage.hpp"
#include "qgmUtil.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"
#include "utilStr.hpp"
#include <sstream>

using namespace bson ;

namespace engine
{
   _qgmPlInsert::_qgmPlInsert( const qgmDbAttr &collection )
   :_qgmPlan( QGM_PLAN_TYPE_INSERT, _qgmField() ),
   _got( FALSE )
   {
      _fullName = collection.toString() ;
      _role = pmdGetKRCB()->getDBRole() ;
      _initialized = TRUE ;
   }

   _qgmPlInsert::~_qgmPlInsert()
   {
      _columns.clear() ;
      _values.clear() ;
   }

   string _qgmPlInsert::toString() const
   {
      stringstream ss ;
      ss << "Type:" << qgmPlanType( _type ) << '\n';
      ss << "Name:" << _fullName << '\n';
      if ( !_columns.empty() )
      {
         BSONObj obj ;
         _mergeObj( obj ) ;
         ss << "Record:" << obj.toString() << '\n';
      }

      return ss.str() ;
   }

   void _qgmPlInsert::addCV( const qgmOPFieldVec &columns,
                             const qgmOPFieldVec &values )
   {
      _columns = columns ;
      _values = values ;
      return ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLINSERT__MERGEOBJ, "_qgmPlInsert::_mergeObj" )
   INT32 _qgmPlInsert::_mergeObj( BSONObj &obj ) const
   {
      PD_TRACE_ENTRY( SDB__QGMPLINSERT__MERGEOBJ ) ;
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      if ( _columns.size() != _values.size() )
      {
         PD_LOG(PDERROR, "column's size does not suit value's size");
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      try
      {
         qgmOPFieldVec::const_iterator itr1 = _columns.begin() ;
         qgmOPFieldVec::const_iterator itr2 = _values.begin() ;
         for ( ; itr1 != _columns.end(); itr1++, itr2++ )
         {
            if ( SQL_GRAMMAR::DIGITAL == itr2->type )
            {
               builder.appendAsNumber( itr1->value.toString(),
                                       itr2->value.toString() ) ;
            }
            else if ( SQL_GRAMMAR::DATE == itr2->type )
            {
               Date_t t ;
               UINT64 millis = 0 ;
               rc = utilStr2Date( itr2->value.toString().c_str(),
                                  millis ) ;
               if ( SDB_OK != rc )
               {
                   PD_LOG( PDDEBUG, "failed to parse to Date_t:%s",
                           itr2->value.toString().c_str() ) ;
                   rc = SDB_INVALIDARG ;
                   goto error ;
               }

               t.millis = millis ;
               builder.appendDate( itr1->value.toString(), t ) ;
            }
            else
            {
               builder.append( itr1->value.toString(),
                               itr2->value.toString()) ;
            }
         }

         obj = builder.obj() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexcepted err happened: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMPLINSERT__MERGEOBJ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLINSERT__NEXTRECORD, "_qgmPlInsert::_nextRecord" )
   INT32 _qgmPlInsert::_nextRecord( _pmdEDUCB *eduCB, BSONObj &obj )
   {
      PD_TRACE_ENTRY( SDB__QGMPLINSERT__NEXTRECORD ) ;
      INT32 rc = SDB_OK ;

      if ( _directInsert() )
      {
         if ( !_got )
         {
            rc = _mergeObj( obj ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            _got = TRUE ;
         }
         else
         {
            rc = SDB_DMS_EOC ;
         }
      }
      else
      {
         if ( !_got )
         {
            rc = input( 0 )->execute( eduCB ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            _got = TRUE ;
         }

         qgmFetchOut fetch ;
         rc = input( 0 )->fetchNext( fetch ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         obj = fetch.obj ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMPLINSERT__NEXTRECORD, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMPLINSERT__EXEC, "_qgmPlInsert::_execute" )
   INT32 _qgmPlInsert::_execute( _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB__QGMPLINSERT__EXEC ) ;
      INT32 rc = SDB_OK ;
      pmdKRCB *pKrcb                   = pmdGetKRCB();
      rtnCoordInsert insert ;
      CHAR *pMsg    = NULL ;
      INT32 bufSize = 0 ;
      BSONObj obj ;
      MsgOpReply dummyReply ;
      SDB_DMSCB *dmsCB = pKrcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = pKrcb->getDPSCB() ;
      BSONObj *err = NULL ;

      if ( dpsCB && eduCB->isFromLocal() && !dpsCB->isLogLocal() )
      {
         dpsCB = NULL ;
      }

      while ( TRUE )
      {
         rc = _nextRecord( eduCB, obj ) ;
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_OK ;
            goto done ;
         }
         else if ( SDB_OK != rc )
         {
            goto error ;
         }
         else
         {
            if ( SDB_ROLE_COORD == _role )
            {
               rc = msgBuildInsertMsg ( &pMsg,
                                        &bufSize,
                                        _fullName.c_str(),
                                        0, 0,
                                        &obj ) ;
               if ( SDB_OK != rc )
               {
                  goto error ;
               }

               rc = insert.execute ( pMsg, *(SINT32*)pMsg, NULL, eduCB,
                                     dummyReply, &err ) ;
               SDB_ASSERT( NULL == err, "impossible" ) ;
               PD_RC_CHECK ( rc, PDERROR, "Failed to execute insert on coord, "
                             "rc = %d", rc ) ;
            }
            else
            {
               rc = rtnInsert ( _fullName.c_str(), obj, 1, 0, eduCB,
                                dmsCB, dpsCB ) ;
               PD_RC_CHECK ( rc, PDERROR,
                          "Failed to insert on non-coord, rc = %d", rc ) ;
            }
         }
      }
   done:
      if ( pMsg )
      {
         SDB_OSS_FREE ( pMsg ) ;
      }
      PD_TRACE_EXITRC( SDB__QGMPLINSERT__EXEC, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _qgmPlInsert::_fetchNext ( qgmFetchOut &next )
   {
      SDB_ASSERT( FALSE, "impossble" ) ;
      return SDB_SYS ;
   }
}
