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

   Source File Name = pmd.cpp

   Descriptive Name = Process MoDel

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains kernel control block object.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include <string.h>
#include "core.hpp"
#include "pmd.hpp"
#include "pdTrace.hpp"

namespace engine
{

   /*
      _SDB_KRCB implement
   */
   _SDB_KRCB::_SDB_KRCB ()
   :_mainEDU( &_eduMgr, EDU_TYPE_AGENT )
   {
      ossMemset( _hostName, 0, sizeof( _hostName ) ) ;
      ossMemset( _groupName, 0, sizeof( _groupName ) ) ;

      for ( INT32 i = 0 ; i < SDB_CB_MAX ; ++i )
      {
         _arrayCBs[ i ] = NULL ;
         _arrayCBs[ i ] = NULL ;
      }
      _init             = FALSE ;
      _isActive         = FALSE ;

      /* <-- internal status, can't be modified by config file --> */
      setDBStatus ( PMD_DB_NORMAL ) ;
      /* <-- external status, can be changed by modifying config file --> */

      _role = SDB_ROLE_STANDALONE ;

      setGroupName ( "" );

#if defined ( SDB_ENGINE )
      _monCfgCB.timestampON = TRUE ;
      _monDBCB.recordActivateTimestamp () ;
#endif // SDB_ENGINE

      setBusinessOK( TRUE ) ;
      setExitCode( SDB_OK ) ;

      _optioncb.setConfigHandler( this ) ;
   }

   _SDB_KRCB::~_SDB_KRCB ()
   {
   }

   IParam* _SDB_KRCB::getParam()
   {
      return &_optioncb ;
   }

   IControlBlock* _SDB_KRCB::getCBByType( SDB_CB_TYPE type )
   {
      if ( (INT32)type < 0 || (INT32)type >= SDB_CB_MAX )
      {
         return NULL ;
      }
      return _arrayCBs[ type ] ;
   }

   void* _SDB_KRCB::getOrgPointByType( SDB_CB_TYPE type )
   {
      if ( (INT32)type < 0 || (INT32)type >= SDB_CB_MAX )
      {
         return NULL ;
      }
      return _arrayOrgs[ type ] ;
   }

   BOOLEAN _SDB_KRCB::isCBValue( SDB_CB_TYPE type ) const
   {
      if ( (INT32)type < 0 || (INT32)type >= SDB_CB_MAX )
      {
         return FALSE ;
      }
      return _arrayCBs[ type ] ? TRUE : FALSE ;
   }

   UINT16 _SDB_KRCB::getLocalPort() const
   {
      return _optioncb.getServicePort() ;
   }

   SDB_ROLE _SDB_KRCB::getDBRole() const
   {
      return _role ;
   }

   INT32 _SDB_KRCB::registerCB( IControlBlock *pCB, void *pOrg )
   {
      INT32 rc = SDB_OK ;

      SDB_ASSERT( pCB, "CB can't be NULL" ) ;
      SDB_ASSERT( FALSE == _init, "Registered cb must be done before "
                                  "KRCB initialization" ) ;

      if ( (INT32)( pCB->cbType () ) < 0 ||
           (INT32)( pCB->cbType () ) >= SDB_CB_MAX )
      {
         SDB_ASSERT ( FALSE, "CB registration should not be out of range" ) ;
         PD_LOG ( PDSEVERE, "Control Block type is not valid: %d",
                  pCB->cbType () ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      _arrayCBs[ pCB->cbType () ] = pCB ;
      _arrayOrgs[ pCB->cbType () ] = pOrg ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _SDB_KRCB::init ()
   {
      INT32 rc = SDB_OK ;
      INT32 index = 0 ;
      IControlBlock *pCB = NULL ;

      _mainEDU.setName( "Main" ) ;
      if ( NULL == pmdGetThreadEDUCB() )
      {
         pmdDeclareEDUCB( &_mainEDU ) ;
      }

      rc = ossGetHostName( _hostName, OSS_MAX_HOSTNAME ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get host name, rc: %d", rc ) ;

      _init = TRUE ;

      for ( index = 0 ; index < SDB_CB_MAX ; ++index )
      {
         pCB = _arrayCBs[ index ] ;
         if ( !pCB )
         {
            continue ;
         }
         if ( SDB_OK != ( rc = pCB->init() ) )
         {
            PD_LOG( PDERROR, "Init cb[Type: %d, Name: %s] failed, rc: %d",
                    pCB->cbType(), pCB->cbName(), rc ) ;
            goto error ;
         }
      }

      for ( index = 0 ; index < SDB_CB_MAX ; ++index )
      {
         pCB = _arrayCBs[ index ] ;
         if ( !pCB )
         {
            continue ;
         }
         if ( SDB_OK != ( rc = pCB->active() ) )
         {
            PD_LOG( PDERROR, "Active cb[Type: %d, Name: %s] failed, rc: %d",
                    pCB->cbType(), pCB->cbName(), rc ) ;
            goto error ;
         }
      }

      _isActive = TRUE ;

      _curTime.sample() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   void _SDB_KRCB::destroy ()
   {
      INT32 rc = SDB_OK ;
      INT32 index = 0 ;
      IControlBlock *pCB = NULL ;

      _isActive = FALSE ;

      _eduMgr.setQuiesced( TRUE ) ;

      for ( index = SDB_CB_MAX ; index > 0 ; --index )
      {
         pCB = _arrayCBs[ index - 1 ] ;
         if ( !pCB )
         {
            continue ;
         }
         if ( SDB_OK != ( rc = pCB->deactive() ) )
         {
            PD_LOG( PDERROR, "Deactive cb[Type: %d, Name: %s] failed, rc: %d",
                    pCB->cbType(), pCB->cbName(), rc ) ;
         }
      }

      _eduMgr.reset () ;

      for ( index = SDB_CB_MAX ; index > 0 ; --index )
      {
         pCB = _arrayCBs[ index - 1 ] ;
         if ( !pCB )
         {
            continue ;
         }
         if ( SDB_OK != ( rc = pCB->fini() ) )
         {
            PD_LOG( PDERROR, "Fini cb[Type: %d, Name: %s] failed, rc: %d",
                    pCB->cbType(), pCB->cbName(), rc ) ;
         }
      }

      pmdUndeclareEDUCB() ;
   }

   void _SDB_KRCB::onConfigChange ( UINT32 changeID )
   {
      INT32 index = 0 ;
      IControlBlock *pCB = NULL ;

      for ( index = 0 ; index < SDB_CB_MAX ; ++index )
      {
         pCB = _arrayCBs[ index ] ;
         if ( !pCB )
         {
            continue ;
         }
         pCB->onConfigChange() ;
      }
   }

   INT32 _SDB_KRCB::onConfigInit ()
   {
      _role = utilGetRoleEnum( _optioncb.krcbRole() ) ;
      pmdSetDBRole( _role ) ;

      if ( _optioncb.isTraceOn() && _optioncb.traceBuffSize() != 0 )
      {
         sdbGetPDTraceCB()->start ( (UINT64)_optioncb.traceBuffSize(),
                                    0xFFFFFFFF ) ;
      }

      ossEnableMemDebug( _optioncb.memDebugEnabled(),
                         _optioncb.memDebugSize() ) ;

      return _optioncb.makeAllDir() ;
   }

   ossTick _SDB_KRCB::getCurTime()
   {
      return _curTime ;
   }

   void _SDB_KRCB::syncCurTime()
   {
      _curTime.sample() ;
   }

   /*
    * kernel control block
    */
   pmdKRCB pmd_krcb ;
}

