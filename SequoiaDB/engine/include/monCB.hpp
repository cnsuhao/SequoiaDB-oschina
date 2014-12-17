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

   Source File Name = monCB.hpp

   Descriptive Name = Monitor Control Block Header

   When/how to use: this program may be used on binary and text-formatted
   versions of monitoring component. This file contains structure for
   application and context snapshot.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef MONCB_HPP_
#define MONCB_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossUtil.hpp"
#include "ossAtomic.hpp"
#include "msg.h"

namespace engine
{
   #define MON_START_OP( _pMonAppCB_ )                      \
   {                                                        \
      if ( NULL != _pMonAppCB_ )                            \
      {                                                     \
         _pMonAppCB_->startOperator() ;                     \
      }                                                     \
   }

   #define MON_END_OP( _pMonAppCB_ )                        \
   {                                                        \
      if ( NULL != _pMonAppCB_ )                            \
      {                                                     \
         _pMonAppCB_->endOperator() ;                       \
      }                                                     \
   }

   #define MON_SET_OP_TYPE( _pMonAppCB_, opType )           \
   {                                                        \
      if ( NULL != _pMonAppCB_ )                            \
      {                                                     \
         _pMonAppCB_->setLastOpType( opType ) ;             \
      }                                                     \
   }

   #define MON_SAVE_OP_DETAIL( _pMonAppCB_, opType, format, ... )    \
   {                                                                 \
      if ( NULL != _pMonAppCB_ )                                     \
      {                                                              \
         _pMonAppCB_->setLastOpType( opType ) ;                      \
         _pMonAppCB_->saveLastOpDetail( format,                      \
                                       ##__VA_ARGS__ ) ;             \
      }                                                              \
   }

   #define MON_SAVE_CMD_DETAIL( _pMonAppCB_, cmdType, format, ... )  \
   {                                                                 \
      if ( NULL != _pMonAppCB_ )                                     \
      {                                                              \
         _pMonAppCB_->setLastOpType( MSG_BS_QUERY_REQ ) ;            \
         _pMonAppCB_->setLastCmdType( cmdType ) ;                    \
         _pMonAppCB_->saveLastOpDetail( format,                      \
                                       ##__VA_ARGS__ ) ;             \
      }                                                              \
   }


   struct _monConfigCB : public SDBObject
   {
      BOOLEAN timestampON ;
   } ;
   typedef struct _monConfigCB monConfigCB ;


   enum MON_OPERATION_TYPES
   {
      MON_COUNTER_OPERATION_NONE = 0,
      MON_DATA_READ,
      MON_INDEX_READ,
      MON_TEMP_READ,
      MON_DATA_WRITE,
      MON_INDEX_WRITE,
      MON_TEMP_WRITE,
      MON_UPDATE,
      MON_DELETE,
      MON_INSERT,
      MON_UPDATE_REPL,
      MON_DELETE_REPL,
      MON_INSERT_REPL,
      MON_SELECT,
      MON_READ,
      MON_COUNTER_OPERATION_MAX = MON_READ,

      MON_TIME_OPERATION_NONE = 0,
      MON_TOTAL_READ_TIME,
      MON_TOTAL_WRITE_TIME,
      MON_TIME_OPERATION_MAX = MON_TOTAL_WRITE_TIME
   } ;

   class _monDBCB : public SDBObject
   {
   public :
      UINT32 numConnects ;

      UINT64 totalDataRead ;
      UINT64 totalIndexRead ;
      UINT64 totalDataWrite ;
      UINT64 totalIndexWrite ;

      UINT64 totalUpdate ;
      UINT64 totalDelete ;
      UINT64 totalInsert ;
      UINT64 totalSelect ;  // total records into result set
      UINT64 totalRead ;    // total records readed from disk
      UINT32 receiveNum ;

      UINT64 replUpdate ;   // IUD caused by replica copy
      UINT64 replDelete ;
      UINT64 replInsert ;

      ossAtomicSigned64 _svcNetIn ;
      ossAtomicSigned64 _svcNetOut ;

      ossTickDelta totalReadTime ;
      ossTickDelta totalWriteTime ;
      ossTick      _activateTimeStampTick ;
      ossTimestamp _activateTimestamp ;

      UINT64 totalLogSize;

      void monOperationTimeInc( MON_OPERATION_TYPES op, ossTickDelta &delta )
      {
         switch ( op )
         {
            case MON_TOTAL_READ_TIME :
               totalReadTime += delta ;
               break ;

            case MON_TOTAL_WRITE_TIME :
               totalWriteTime += delta ;
               break ;

            default :
               break ;
         }
      }

      void monOperationCountInc( MON_OPERATION_TYPES op, UINT64 delta = 1 )
      {
         switch ( op )
         {
            case MON_DATA_READ :
               totalDataRead += delta ;
               break ;

            case MON_INDEX_READ :
               totalIndexRead += delta ;
               break ;

            case MON_DATA_WRITE :
               totalDataWrite += delta ;
               break ;

            case MON_INDEX_WRITE :
               totalIndexWrite += delta ;
               break ;

            case MON_UPDATE :
               totalUpdate += delta ;
               break ;

            case MON_DELETE :
               totalDelete += delta ;
               break ;

            case MON_INSERT :
               totalInsert += delta ;
               break ;

            case MON_UPDATE_REPL :
               replUpdate += delta ;
               break ;

            case MON_DELETE_REPL :
               replDelete += delta ;
               break ;

            case MON_INSERT_REPL :
               replInsert += delta ;
               break ;

            case MON_SELECT :
               totalSelect += delta ;
               break ;

            case MON_READ :
               totalRead += delta ;
               break ;

            default:
               break ;
         }
      }

      void reset();

      UINT32 getReceiveNum ()
      {
         return receiveNum ;
      }
      void addReceiveNum ()
      {
         ++receiveNum ;
      }

      void svcNetInAdd( INT32 sendSize )
      {
         _svcNetIn.add( sendSize ) ;
      }

      void svcNetOutAdd( INT32 recvSize )
      {
         _svcNetOut.add( recvSize ) ;
      }

      INT64 svcNetIn()
      {
         return _svcNetIn.peek() ;
      }

      INT64 svcNetOut()
      {
         return _svcNetOut.peek() ;
      }

      _monDBCB()
      :_svcNetIn(0),
      _svcNetOut(0)
      {
         reset() ;
         receiveNum = 0 ;
         _activateTimeStampTick.clear() ;
         _activateTimestamp.time = 0 ;
         _activateTimestamp.microtm = 0 ;
      }

      _monDBCB &operator= ( const _monDBCB &rhs )
      {
         numConnects               = rhs.numConnects ;

         totalDataRead             = rhs.totalDataRead ;
         totalIndexRead            = rhs.totalIndexRead ;
         totalDataWrite            = rhs.totalDataWrite ;
         totalIndexWrite           = rhs.totalIndexWrite ;

         totalUpdate               = rhs.totalUpdate ;
         totalDelete               = rhs.totalDelete ;
         totalInsert               = rhs.totalInsert ;
         totalSelect               = rhs.totalSelect ;
         totalRead                 = rhs.totalRead ;

         replUpdate                = rhs.replUpdate ;
         replDelete                = rhs.replDelete ;
         replInsert                = rhs.replInsert ;

         totalReadTime             = rhs.totalReadTime ;
         totalWriteTime            = rhs.totalWriteTime ;
         _activateTimestamp.time    = rhs._activateTimestamp.time;
         _activateTimestamp.microtm = rhs._activateTimestamp.microtm ;
         _activateTimeStampTick     = rhs._activateTimeStampTick ;

         return *this ;
      }

      void recordActivateTimestamp()
      {
         _activateTimeStampTick.sample() ;
         ossGetCurrentTime( _activateTimestamp ) ;
      }

   } ;
   typedef class _monDBCB  monDBCB ;

   class _monAppCB : public SDBObject
   {
   public :
      monDBCB *mondbcb ;
      UINT64 totalDataRead ;
      UINT64 totalIndexRead ;
      UINT64 totalDataWrite ;
      UINT64 totalIndexWrite ;

      UINT64 totalUpdate ;
      UINT64 totalDelete ;
      UINT64 totalInsert ;
      UINT64 totalSelect ;  // total records into result set
      UINT64 totalRead ;    // total records readed from disk

      ossTickDelta totalReadTime ;
      ossTickDelta totalWriteTime ;
      ossTick      _connectTimeStampTick ;
      ossTimestamp _connectTimestamp ;

      INT32 _lastOpType ;
      INT32 _cmdType ;
      ossTick _lastOpBeginTime ;
      ossTick _lastOpEndTime ;
      ossTickDelta _readTimeSpent ;
      ossTickDelta _writeTimeSpent ;
      CHAR _lastOpDetail[ 128 ] ;

      void monOperationTimeInc( MON_OPERATION_TYPES op, ossTickDelta &delta )
      {
         switch ( op )
         {
            case MON_TOTAL_READ_TIME :
               totalReadTime += delta ;
               break ;

            case MON_TOTAL_WRITE_TIME :
               totalWriteTime += delta ;
               break ;

            default :
               break ;
         }
         mondbcb->monOperationTimeInc ( op, delta ) ;
      }

      void monOperationCountInc( MON_OPERATION_TYPES op, UINT64 delta = 1 )
      {
         switch ( op )
         {
            case MON_DATA_READ :
               totalDataRead += delta ;
               break ;

            case MON_INDEX_READ :
               totalIndexRead += delta ;
               break ;

            case MON_DATA_WRITE :
               totalDataWrite += delta ;
               break ;

            case MON_INDEX_WRITE :
               totalIndexWrite += delta ;
               break ;

            case MON_UPDATE :
               totalUpdate += delta ;
               break ;

            case MON_DELETE :
               totalDelete += delta ;
               break ;

            case MON_INSERT :
               totalInsert += delta ;
               break ;

            case MON_SELECT :
               totalSelect += delta ;
               break ;

            case MON_READ :
               totalRead += delta ;
               break ;

            default:
               break ;
         }
         mondbcb->monOperationCountInc ( op, delta ) ;
      }

      void reset() ;

      _monAppCB() ;

      _monAppCB &operator= ( const _monAppCB &rhs ) ;
      _monAppCB &operator+= ( const _monAppCB &rhs ) ;

      void recordConnectTimestamp()
      {
         _connectTimeStampTick.sample() ;
         ossGetCurrentTime( _connectTimestamp ) ;
      }

      void startOperator() ;
      void endOperator() ;
      void setLastOpType( INT32 opType ) ;
      void setLastCmdType( INT32 cmdType ) ;
      void opTimeSpentInc( ossTickDelta delta );
      void saveLastOpDetail( const CHAR *format, ... ) ;

   } ;
   typedef class _monAppCB  monAppCB ;

   class _monContextCB : public SDBObject
   {
   public :
      UINT64 dataRead ;
      UINT64 indexRead ;
      ossTickDelta queryTimeSpent ;
      ossTimestamp _startTimestamp ;
      ossTick      _startTimestampTick ;

      void reset()
      {
         dataRead = 0 ;
         indexRead = 0 ;
         queryTimeSpent.clear() ;
         _startTimestamp.time = 0 ;
         _startTimestamp.microtm = 0 ;
         _startTimestampTick.clear() ;
      }

      _monContextCB()
      {
         reset() ;
      }

      _monContextCB &operator= ( const _monContextCB &rhs )
      {
         dataRead                = rhs.dataRead ;
         indexRead               = rhs.indexRead ;
         queryTimeSpent          = rhs.queryTimeSpent ;
         _startTimestamp.time    = rhs._startTimestamp.time ;
         _startTimestamp.microtm = rhs._startTimestamp.microtm ;
         _startTimestampTick     = rhs._startTimestampTick ;
         return *this ;
      }

      void recordStartTimestamp()
      {
         _startTimestampTick.sample() ;
         ossGetCurrentTime( _startTimestamp ) ;
      }

      void monOperationCountInc( MON_OPERATION_TYPES op, UINT64 delta = 1 )
      {
         switch ( op )
         {
            case MON_DATA_READ :
               dataRead += delta ;
               break ;

            case MON_INDEX_READ :
               indexRead += delta ;
               break ;

            default:
               break ;
         }
      }

      void monOperationTimeInc( MON_OPERATION_TYPES op, ossTickDelta &delta )
      {
         queryTimeSpent += delta ;
      }

   } ;
   typedef class _monContextCB  monContextCB ;
}
#endif
