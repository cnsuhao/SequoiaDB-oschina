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

   Source File Name = dpsLogWrapper.cpp

   Descriptive Name = Data Protection Service Log Wrapper

   When/how to use: this program may be used on binary and text-formatted
   versions of DPS component. This file contains code logic for log wrapper,
   which is also called DPS Control Block

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/01/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "dpsLogWrapper.hpp"
#include "dpsLogDef.hpp"
#include "dpsReplicaLogMgr.hpp"
#include "pd.hpp"
#include "dpsMergeBlock.hpp"
#include "dpsOp2Record.hpp"
#include "pdTrace.hpp"
#include "dpsTrace.hpp"
#include "dpsLogRecordDef.hpp"
#include "pmd.hpp"

namespace engine
{
   /*
      _dpsLogWrapper implement
   */
   _dpsLogWrapper::_dpsLogWrapper()
   {
      _initialized   = FALSE ;
      _dpslocal      = FALSE ;
      _pEventHandler = NULL ;
   }
   _dpsLogWrapper::~_dpsLogWrapper()
   {
   }

   INT32 _dpsLogWrapper::init ()
   {
      pmdOptionsCB *optCB = pmdGetKRCB()->getOptionCB() ;

      _dpslocal = optCB->isDpsLocal() ;
      _buf.setLogFileSz( optCB->getReplLogFileSz() ) ;
      _buf.setLogFileNum( optCB->getReplLogFileNum() ) ;

      INT32 rc = _buf.init( optCB->getReplLogPath(),
                            optCB->getReplLogBuffSize() ) ;
      if ( SDB_OK == rc )
      {
         _initialized = TRUE ;
      }

      return rc ;
   }

   INT32 _dpsLogWrapper::active ()
   {
      INT32 rc = SDB_OK ;
      pmdEDUMgr *pEDUMgr = pmdGetKRCB()->getEDUMgr() ;
      EDUID eduID = PMD_INVALID_EDUID ;

      rc = pEDUMgr->startEDU( EDU_TYPE_LOGGW, (void*)this, &eduID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Start dps log writer failed, rc: %d", rc ) ;
         goto error ;
      }
      pEDUMgr->regSystemEDU( EDU_TYPE_LOGGW, eduID ) ;
      rc = pEDUMgr->startEDU( EDU_TYPE_DPSROLLBACK, NULL, &eduID ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Start dps trans rollback failed, rc: %d", rc ) ;
         goto error ;
      }
      pEDUMgr->regSystemEDU( EDU_TYPE_DPSROLLBACK, eduID ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _dpsLogWrapper::deactive ()
   {
      return SDB_OK ;
   }

   INT32 _dpsLogWrapper::fini ()
   {
      return SDB_OK ;
   }

   void _dpsLogWrapper::writeData ( dpsMergeInfo & info )
   {
      _buf.writeData( info ) ;

      if ( _pEventHandler && info.isNeedNotify() )
      {
         DPS_LSN_OFFSET offset = DPS_INVALID_LSN_OFFSET ;
         pmdEDUCB *cb = info.getEDUCB() ;
         if ( info.hasDummy() )
         {
            offset = info.getDummyBlock().record().head()._lsn ;
            if ( cb )
            {
               cb->insertLsn( offset ) ;
            }
            _pEventHandler->onWriteLog( offset ) ;
         }
         offset = info.getMergeBlock().record().head()._lsn ;
         if ( cb )
         {
            cb->insertLsn( offset ) ;
         }
         _pEventHandler->onWriteLog( offset ) ;
      }
      info.resetInfoEx() ;
   }

   INT32 _dpsLogWrapper::completeOpr( _pmdEDUCB * cb, INT32 w )
   {
      INT32 rc = SDB_OK ;
      if ( _pEventHandler && w > 1 && cb && 0 != cb->getLsnCount() )
      {
         rc = _pEventHandler->onCompleteOpr( cb, w ) ;
         cb->resetLsn() ;
      }
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DPSLGWRAPP_RECDROW, "_dpsLogWrapper::recordRow" )
   INT32 _dpsLogWrapper::recordRow( const CHAR *row, UINT32 len )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__DPSLGWRAPP_RECDROW );
      if ( !_initialized )
      {
         goto done;
      }
      {
         SDB_ASSERT( NULL != row, "row should not be NULL!") ;
         _dpsMergeBlock block ;
         dpsLogRecord &record = block.record();
         dpsLogRecordHeader &header = record.head() ;
         ossMemcpy( &header, row, sizeof(dpsLogRecordHeader) );
         block.setRow( TRUE ) ;
         rc = record.push( DPS_LOG_ROW_ROWDATA,
                           header._length -  sizeof(dpsLogRecordHeader),
                           row + sizeof(dpsLogRecordHeader)) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "Failed to push row to record:%d", rc ) ;
            goto error;
         }

         rc = _buf.merge( block );
      }

   done :
      PD_TRACE_EXITRC ( SDB__DPSLGWRAPP_RECDROW, rc );
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN _dpsLogWrapper::isInRestore()
   {
      return _buf.isInRestore() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__DPSLGWRAPP_PREPARE, "prepare" )
   INT32 _dpsLogWrapper::prepare( dpsMergeInfo &info )
   {
      PD_TRACE_ENTRY( SDB__DPSLGWRAPP_PREPARE ) ;
      INT32 rc = SDB_OK ;
      if ( !_initialized )
      {
         goto done;
      }
      rc = _buf.preparePages( info ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to prepare pages, rc = %d", rc ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__DPSLGWRAPP_PREPARE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   /*
      get dps cb
   */
   SDB_DPSCB* sdbGetDPSCB()
   {
      static SDB_DPSCB s_dpscb ;
      return &s_dpscb ;
   }

}

