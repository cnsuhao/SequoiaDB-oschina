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

   Source File Name = rtnDataSet.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains structure for Runtime
   Context.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          24/06/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnDataSet.hpp"
#include "rtn.hpp"

namespace engine
{
   _rtnDataSet::_rtnDataSet( const rtnQueryOptions &options,
                             _pmdEDUCB *cb )
   :_contextID( -1 ),
    _cb( cb ),
    _lastErr( SDB_OK ),
    _fetchFromContext( TRUE ),
    _rtnCB( NULL )
   {
      _rtnCB = sdbGetRTNCB() ;
      _lastErr = rtnQuery( options._fullName, options._query,
                           options._selector, options._orderBy,
                           options._hint, options._flag,
                           _cb, options._skip, options._limit,
                           sdbGetDMSCB(), _rtnCB,
                           _contextID, NULL, FALSE ) ;
   }

   _rtnDataSet::_rtnDataSet( SINT64 contextID,
                             _pmdEDUCB *cb )
   :_contextID( contextID ),
    _cb( cb ),
    _lastErr( SDB_OK ),
    _fetchFromContext( TRUE ),
    _rtnCB( NULL )
   {
      _rtnCB = sdbGetRTNCB() ;
   }

   _rtnDataSet::~_rtnDataSet()
   {
      if ( -1 != _contextID )
      {
         sdbGetRTNCB()->contextDelete( _contextID,
                                       _cb ) ;
         _contextID = -1 ;
      }
   }

   INT32 _rtnDataSet::next( BSONObj &obj )
   {
      INT32 rc = SDB_OK ;
      if ( SDB_OK != _lastErr )
      {
         goto error ;
      }

   retry:
      if ( _fetchFromContext )
      {
         rc = rtnGetMore( _contextID, -1, _contextBuf, _cb, _rtnCB ) ;
         if ( SDB_OK != rc )
         {
            if ( SDB_DMS_EOC != rc )
            {
               PD_LOG( PDERROR, "failed to get next object:%d", rc ) ;
            }
            else
            {
               _contextID = -1 ;
            }
            _lastErr = rc ;
            goto error ;
         }

         _fetchFromContext = FALSE ;
      }

      rc = _contextBuf.nextObj( obj ) ;
      if ( SDB_OK == rc )
      {
         goto done ;
      }
      else if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_OK ;
         _fetchFromContext = TRUE ;
         goto retry ;
      }
      else
      {
         _lastErr = rc ;
         PD_LOG( PDERROR, "failed to get next from buf:%d", rc ) ;
         goto error ;
      } 
   done:
      return rc ;
   error:
      goto done ;
   }
}

