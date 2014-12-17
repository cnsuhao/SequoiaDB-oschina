/******************************************************************************


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

   Source File Name = monCB.cpp

   Descriptive Name = Monitor Control Block

   When/how to use: this program may be used on binary and text-formatted
   versions of monitoring component. This file contains structure for
   database, application and context snapshot.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "monCB.hpp"
#include "pmd.hpp"
#include "clsMgr.hpp"
#include "clsReplicateSet.hpp"
#include "rtnCommand.hpp"

namespace engine
{
   void _monDBCB::reset()
   {
      numConnects     = 0 ;

      totalDataRead   = 0 ;
      totalIndexRead  = 0 ;
      totalDataWrite  = 0 ;
      totalIndexWrite = 0 ;

      totalUpdate     = 0 ;
      totalDelete     = 0 ;
      totalInsert     = 0 ;
      totalSelect     = 0 ;
      totalRead       = 0 ;

      replUpdate      = 0 ;
      replInsert      = 0 ;
      replDelete      = 0 ;

      totalReadTime.clear() ;
      totalWriteTime.clear() ;

      _svcNetIn.poke( 0 ) ;
      _svcNetOut.poke( 0 ) ;

      pmdKRCB *pKrcb = pmdGetKRCB() ;
      SDB_ROLE role = pKrcb->getDBRole() ;
      if ( SDB_ROLE_DATA == role || SDB_ROLE_CATALOG == role )
      {
         sdbGetShardCB()->resetMon() ;
         sdbGetReplCB()->resetMon() ;
      }
   }

   _monAppCB::_monAppCB()
   {
      reset () ;
      mondbcb = pmdGetKRCB()->getMonDBCB() ;
   }
   _monAppCB &_monAppCB::operator= ( const _monAppCB &rhs )
   {
      mondbcb                   = pmdGetKRCB()->getMonDBCB() ;
      totalDataRead             = rhs.totalDataRead ;
      totalIndexRead            = rhs.totalIndexRead ;
      totalDataWrite            = rhs.totalDataWrite ;
      totalIndexWrite           = rhs.totalIndexWrite ;

      totalUpdate               = rhs.totalUpdate ;
      totalDelete               = rhs.totalDelete ;
      totalInsert               = rhs.totalInsert ;
      totalSelect               = rhs.totalSelect ;
      totalRead                 = rhs.totalRead ;

      totalReadTime             = rhs.totalReadTime ;
      totalWriteTime            = rhs.totalWriteTime ;
      _connectTimestamp.time    = rhs._connectTimestamp.time;
      _connectTimestamp.microtm = rhs._connectTimestamp.microtm ;
      _connectTimeStampTick     = rhs._connectTimeStampTick ;

      _lastOpType = rhs._lastOpType ;
      _cmdType = rhs._cmdType ;
      _lastOpBeginTime = rhs._lastOpBeginTime ;
      _lastOpEndTime = rhs._lastOpEndTime ;
      _readTimeSpent = rhs._readTimeSpent ;
      _writeTimeSpent = rhs._writeTimeSpent ;
      ossMemcpy( _lastOpDetail, rhs._lastOpDetail,
               sizeof( _lastOpDetail ) ) ;

      return *this ;
   }
   _monAppCB &_monAppCB::operator+= ( const _monAppCB &rhs )
   {
      totalDataRead              += rhs.totalDataRead ;
      totalIndexRead             += rhs.totalIndexRead ;
      totalDataWrite             += rhs.totalDataWrite ;
      totalIndexWrite            += rhs.totalIndexWrite ;

      totalUpdate                += rhs.totalUpdate ;
      totalDelete                += rhs.totalDelete ;
      totalInsert                += rhs.totalInsert ;
      totalSelect                += rhs.totalSelect ;
      totalRead                  += rhs.totalRead ;

      totalReadTime              += rhs.totalReadTime ;
      totalWriteTime             += rhs.totalWriteTime ;
      _connectTimestamp.time     += rhs._connectTimestamp.time;
      _connectTimestamp.microtm  += rhs._connectTimestamp.microtm ;

      _readTimeSpent             += rhs._readTimeSpent ;
      _writeTimeSpent            += rhs._writeTimeSpent ;

      return *this ;
   }

   void _monAppCB::reset()
   {
      totalDataRead = 0 ;
      totalIndexRead = 0 ;
      totalDataWrite = 0 ;
      totalIndexWrite = 0 ;

      totalUpdate = 0 ;
      totalDelete = 0 ;
      totalInsert = 0 ;
      totalSelect = 0 ;
      totalRead  = 0 ;

      totalReadTime.clear() ;
      totalWriteTime.clear() ;
      _connectTimeStampTick.clear() ;
      _connectTimestamp.time = 0 ;
      _connectTimestamp.microtm = 0 ;

      _lastOpType = MSG_NULL ;
      _cmdType = CMD_UNKNOW ;
      _lastOpBeginTime.clear() ;
      _lastOpEndTime.clear() ;
      _readTimeSpent.clear() ;
      _writeTimeSpent.clear() ;
      ossMemset( _lastOpDetail, 0, sizeof(_lastOpDetail) ) ;
   }

   void _monAppCB::startOperator()
   {
      _lastOpBeginTime = pmdGetKRCB()->getCurTime() ;
      _lastOpEndTime.clear() ;
      _lastOpType = MSG_NULL ;
      _cmdType = CMD_UNKNOW ;
      ossMemset( _lastOpDetail, 0, sizeof(_lastOpDetail) ) ;
   }

   void _monAppCB::endOperator()
   {
      if ( (BOOLEAN)_lastOpBeginTime )
      {
         _lastOpEndTime = pmdGetKRCB()->getCurTime() ;
         ossTickDelta delta = _lastOpEndTime - _lastOpBeginTime ;
         opTimeSpentInc( delta ) ;
      }
   }

   void _monAppCB::setLastOpType( INT32 opType )
   {
      _lastOpType = opType ;
   }

   void _monAppCB::opTimeSpentInc( ossTickDelta delta )
   {
      switch ( _lastOpType )
      {
         case MSG_BS_QUERY_REQ :
            {
               if ( _cmdType != CMD_UNKNOW )
               {
                  break ;
               }
            }
         case MSG_BS_GETMORE_REQ :
            {
               _readTimeSpent += delta ;
               break ;
            }
         case MSG_BS_INSERT_REQ :
         case MSG_BS_UPDATE_REQ :
         case MSG_BS_DELETE_REQ :
            {
               _writeTimeSpent += delta ;
               break ;
            }
         default :
            break ;
      }
   }

   void _monAppCB::saveLastOpDetail( const CHAR *format, ... )
   {
      va_list argList ;
      UINT32 curLen = ossStrlen( _lastOpDetail ) ;
      if ( curLen >= sizeof( _lastOpDetail ) - 3 )
      {
         goto done ;
      }
      else if ( curLen > 0 )
      {
         _lastOpDetail[ curLen ] = ',' ;
         ++curLen ;
         _lastOpDetail[ curLen ] = ' ' ;
         ++curLen ;
         _lastOpDetail[ curLen ] = 0 ;
      }
      va_start( argList, format ) ;
      vsnprintf( _lastOpDetail + curLen,
               sizeof( _lastOpDetail ) - curLen,
               format, argList ) ;
      va_end( argList ) ;
   done:
      return ;
   }
}
