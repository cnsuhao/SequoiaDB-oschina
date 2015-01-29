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

   Source File Name = pmdEDU.cpp

   Descriptive Name = Process MoDel Agent Engine Dispatchable Unit

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for EDU processing.
   EDU thread is a wrapper of all threads. It will call each entry function
   depends on the EDU type.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include <stdio.h>
#include "pd.hpp"
#include "ossEDU.hpp"
#include "ossMem.hpp"
#include "pmdEDU.hpp"
#include "pmdEDUMgr.hpp"
#include "pmd.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include <map>

#if defined ( SDB_ENGINE )
#include "rtnCB.hpp"
#endif // SDB_ENGINE

namespace engine
{
   const UINT32 EDU_MEM_ALIGMENT_SIZE  = 1024 ;
   const UINT32 EDU_MAX_CATCH_SIZE     = 16*1024*1024 ;

   static std::map<EDU_TYPES, std::string> mapEDUName ;
   static std::map<EDU_TYPES,EDU_TYPES>    mapEDUTypeSys ;

   /*
      TOOL FUNCTIONS
   */
   // PD_TRACE_DECLARE_FUNCTION ( SDB_REGEDUNAME, "registerEDUName" )
   INT32 registerEDUName ( EDU_TYPES type, const CHAR * name, BOOLEAN system )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_REGEDUNAME );
      std::map<EDU_TYPES, std::string>::iterator it =
         mapEDUName.find ( type ) ;
      if ( it != mapEDUName.end() )
      {
         PD_LOG ( PDERROR, "EDU type confict[type:%d, %s<->%s]", (INT32)type,
            it->second.c_str(), name ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      mapEDUName[type] = std::string( name ) ;

      if ( system )
      {
         mapEDUTypeSys[type] = type ;
      }
   done :
      PD_TRACE_EXIT ( SDB_REGEDUNAME );
      return rc ;
   error :
      goto done ;
   }

   const CHAR * getEDUStatusDesp ( EDU_STATUS status )
   {
      const CHAR *desp = "Unknown" ;

      switch ( status )
      {
         case PMD_EDU_CREATING :
            desp = "Creating" ;
            break ;
         case PMD_EDU_RUNNING :
            desp = "Running" ;
            break ;
         case PMD_EDU_WAITING :
            desp = "Waiting" ;
            break ;
         case PMD_EDU_IDLE :
            desp = "Idle" ;
            break ;
         case PMD_EDU_DESTROY :
            desp = "Destroying" ;
            break ;
         default :
            break ;
      }

      return desp ;
   }

   const CHAR * getEDUName( EDU_TYPES type )
   {
      std::map<EDU_TYPES, std::string>::iterator it =
         mapEDUName.find ( type ) ;
      if ( it != mapEDUName.end() )
      {
         return it->second.c_str() ;
      }

      return "Unknow" ;
   }

   BOOLEAN isSystemEDU ( EDU_TYPES type )
   {
      std::map<EDU_TYPES,EDU_TYPES>::iterator it = mapEDUTypeSys.find( type ) ;
      return it == mapEDUTypeSys.end() ? FALSE : TRUE ;
   }

   /*
      _pmdEDUCB implement
   */
   _pmdEDUCB::_pmdEDUCB( _pmdEDUMgr *mgr, EDU_TYPES type )
   : _tid( 0 ),
   _processEventCount( 0 ),
   _eduMgr( mgr ),
   _eduType( type ),
   _ctrlFlag( 0 ),
   _writingDB( FALSE ),
   _writingTime( 0 ),
   _threadHdl( 0 ),
#if defined (_LINUX)
   _threadID( 0 ),
#endif
   _pClientSock( NULL )
   {
      _Name[0]          = 0 ;
      _pSession         = NULL ;
      _pCompressBuff    = NULL ;
      _compressBuffLen  = 0 ;
      _pUncompressBuff  = NULL ;
      _uncompressBuffLen= 0 ;
      _totalCatchSize   = 0 ;
      _totalMemSize     = 0 ;
      _isDoRollback     = FALSE ;

#if defined ( SDB_ENGINE )
      _pCoordSession    = NULL ;
      _beginLsn         = 0 ;
      _endLsn           = 0 ;
      _lsnNumber        = 0 ;
      _relatedTransLSN  = DPS_INVALID_LSN_OFFSET ;
      _curTransLSN      = DPS_INVALID_LSN_OFFSET ;
      _curTransID       = DPS_INVALID_TRANS_ID ;
      _pTransNodeMap    = NULL ;
      _transRC          = SDB_OK ;

      _alignedMem       = NULL ;
      _alignedMemSize   = 0 ;

      _curRequestID     = 1 ;
      _dmsLockLevel     = 0 ;

      _monCfgCB = *( (monConfigCB*)(pmdGetKRCB()->getMonCB()) ) ;
#endif // SDB_ENGINE

      _pErrorBuff = (CHAR *)SDB_OSS_MALLOC( EDU_ERROR_BUFF_SIZE + 1 ) ;
      if ( _pErrorBuff )
      {
         ossMemset( _pErrorBuff, 0, EDU_ERROR_BUFF_SIZE + 1 ) ;
      }
   }

   _pmdEDUCB::~_pmdEDUCB ()
   {
      {
         ossScopedRWLock assist ( &_callInMutex, EXCLUSIVE ) ;
      }

      if ( _pErrorBuff )
      {
         SDB_OSS_FREE ( _pErrorBuff ) ;
         _pErrorBuff = NULL ;
      }
#if defined ( SDB_ENGINE )
      DpsTransCBLockList::iterator iterLst = _transLockLst.begin();
      while( iterLst != _transLockLst.end() )
      {
         if ( iterLst->second )
         {
            SDB_OSS_DEL iterLst->second ;
         }
         _transLockLst.erase( iterLst++ );
      }
      if ( _pTransNodeMap )
      {
         delete _pTransNodeMap;
         _pTransNodeMap = NULL;
      }
#endif // SDB_ENGINE

      clear() ;
   }

   void _pmdEDUCB::clear()
   {
      pmdEDUEvent data ;
      while ( _queue.try_pop( data ) )
      {
         pmdEduEventRelase( data, this ) ;
      }
      _processEventCount = 0 ;
      _Name[0] = 0 ;
      _userName = "" ;
      _passWord = "" ;

#if defined ( SDB_ENGINE )
      clearTransInfo() ;
      releaseAlignedMemory() ;
      resetLsn() ;
#endif // SDB_ENGINE

      if ( _pCompressBuff )
      {
         releaseBuff( _pCompressBuff ) ;
         _pCompressBuff = NULL ;
      }
      _compressBuffLen = 0 ;
      if ( _pUncompressBuff )
      {
         releaseBuff( _pUncompressBuff ) ;
         _pUncompressBuff = NULL ;
      }
      _uncompressBuffLen = 0 ;

      CATCH_MAP_IT it = _catchMap.begin() ;
      while ( it != _catchMap.end() )
      {
         SDB_OSS_FREE( it->second ) ;
         _totalCatchSize -= it->first ;
         _totalMemSize -= it->first ;
         ++it ;
      }
      _catchMap.clear() ;

      ALLOC_MAP_IT itAlloc = _allocMap.begin() ;
      while ( itAlloc != _allocMap.end() )
      {
         SDB_OSS_FREE( itAlloc->first ) ;
         _totalMemSize -= itAlloc->second ;
         ++itAlloc ;
      }
      _allocMap.clear() ;

      SDB_ASSERT( _totalCatchSize == 0 , "Catch size is error" ) ;
      SDB_ASSERT( _totalMemSize == 0, "Memory size is error" ) ;
   }

   string _pmdEDUCB::toString() const
   {
      stringstream ss ;
      ss << "ID: " << _eduID << ", Type: " << _eduType << "["
         << getEDUName( _eduType ) << "], TID: " << _tid ;

      if ( _pSession )
      {
         ss << ", Session: " << _pSession->sessionName() ;
      }
      return ss.str() ;
   }

   void _pmdEDUCB::attachSession( ISession *pSession )
   {
      _pSession = pSession ;
   }

   void _pmdEDUCB::detachSession()
   {
      _pSession = NULL ;
   }

   void _pmdEDUCB::setType ( EDU_TYPES type )
   {
      SDB_ASSERT ( PMD_EDU_IDLE == _status,
                   "Type can't be changed during active" ) ;
      _eduType = type ;
   }

   void _pmdEDUCB::interrupt ()
   {
      _ctrlFlag |= EDU_CTRL_INTERRUPTED ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_DISCONNECT, "_pmdEDUCB::disconnect" )
   void _pmdEDUCB::disconnect ()
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_DISCONNECT );
      ossScopedRWLock assist ( &_callInMutex, SHARED ) ;
      interrupt () ;
      _ctrlFlag |= EDU_CTRL_DISCONNECTED ;
      postEvent ( pmdEDUEvent ( PMD_EDU_EVENT_TERM ) ) ;
      PD_TRACE_EXIT ( SDB__PMDEDUCB_DISCONNECT );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_FORCE, "_pmdEDUCB::force" )
   void _pmdEDUCB::force ()
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_FORCE );
      ossScopedRWLock assist ( &_callInMutex, SHARED ) ;
      disconnect () ;
      _ctrlFlag |= EDU_CTRL_FORCED ;
      PD_TRACE_EXIT ( SDB__PMDEDUCB_FORCE );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_ISINT, "_pmdEDUCB::isInterrupted" )
   BOOLEAN _pmdEDUCB::isInterrupted ( BOOLEAN onlyFlag )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_ISINT );
      BOOLEAN ret = FALSE ;

      if ( !onlyFlag && _isDoRollback )
      {
         goto done;
      }
      if ( _ctrlFlag & EDU_CTRL_INTERRUPTED )
      {
         ret = TRUE ;
         goto done ;
      }
      else if ( !onlyFlag && _pClientSock )
      {
         if ( _pClientSock->isClosed() )
         {
            _ctrlFlag |= ( EDU_CTRL_INTERRUPTED | EDU_CTRL_DISCONNECTED ) ;
            ret = TRUE ;
         }
         else
         {
            INT32 receivedLen ;
            MsgHeader header ;
            INT32 rc = _pClientSock->recv( (CHAR*)&header , sizeof(header),
                                           receivedLen, 0, MSG_PEEK ) ;
            if ( ( rc >= (INT32)sizeof(header)
                   && MSG_BS_DISCONNECT == header.opCode )
                 || SDB_NETWORK_CLOSE == rc || SDB_NETWORK == rc )
            {
               _ctrlFlag |= ( EDU_CTRL_INTERRUPTED | EDU_CTRL_DISCONNECTED ) ;
               ret = TRUE ;
            }
            else if ( rc >= (INT32)sizeof(header) &&
                      ( MSG_BS_INTERRUPTE == header.opCode ||
                        MSG_BS_INTERRUPTE_SELF == header.opCode ) )
            {
               _ctrlFlag |= EDU_CTRL_INTERRUPTED ;
               ret = TRUE ;
            }
         }
      }
   done :
      PD_TRACE1 ( SDB__PMDEDUCB_ISINT, PD_PACK_INT(ret) );
      PD_TRACE_EXIT ( SDB__PMDEDUCB_ISINT );
      return ret ;
   }

   BOOLEAN _pmdEDUCB::isDisconnected ()
   {
      return ( _ctrlFlag & EDU_CTRL_DISCONNECTED ) ? TRUE : FALSE ;
   }

   BOOLEAN _pmdEDUCB::isForced ()
   {
      return ( _ctrlFlag & EDU_CTRL_FORCED ) ? TRUE : FALSE ;
   }

   void _pmdEDUCB::resetInterrupt ()
   {
      _ctrlFlag &= ~EDU_CTRL_INTERRUPTED ;
   }

   void _pmdEDUCB::resetDisconnect ()
   {
      resetInterrupt () ;
      _ctrlFlag &= ~EDU_CTRL_DISCONNECTED ;
   }

   void _pmdEDUCB::setUserInfo( const string & userName,
                                const string & password )
   {
      _userName = userName ;
      _passWord = password ;
   }

   void _pmdEDUCB::setClientInfo ( const CHAR *clientName, UINT16 clientPort )
   {
      ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
      ossSnprintf( _Name, PMD_EDU_NAME_LENGTH, "%s:%u",
                   clientName, clientPort ) ;
      _Name[PMD_EDU_NAME_LENGTH] = 0 ;
   }

   void _pmdEDUCB::setName ( const CHAR * name )
   {
      ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
      ossStrncpy ( _Name, name, PMD_EDU_NAME_LENGTH ) ;
      _Name[PMD_EDU_NAME_LENGTH] = 0 ;
   }

   CHAR *_pmdEDUCB::_getBuffInfo ( EDU_INFO_TYPE type, UINT32 & size )
   {
      CHAR *buff = NULL ;
      switch ( type )
      {
         case EDU_INFO_ERROR :
            buff = _pErrorBuff ;
            size = EDU_ERROR_BUFF_SIZE ;
            break ;
         default :
            break ;
      }

      return buff ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_PRINTINFO, "_pmdEDUCB::printInfo" )
   INT32 _pmdEDUCB::printInfo ( EDU_INFO_TYPE type, const CHAR * format, ... )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_PRINTINFO );
      if ( getInfo ( type ) )
      {
         goto done ;
      }

      {
      UINT32 buffSize = 0 ;
      CHAR *buff = _getBuffInfo ( type, buffSize ) ;

      if ( NULL == buff || buffSize == 0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      va_list ap ;
      va_start ( ap, format ) ;
      vsnprintf ( buff, buffSize, format, ap ) ;
      va_end ( ap ) ;

      buff[ buffSize ] = 0 ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__PMDEDUCB_PRINTINFO, rc );
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_GETINFO, "_pmdEDUCB::getInfo" )
   const CHAR *_pmdEDUCB::getInfo ( EDU_INFO_TYPE type )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_GETINFO );
      UINT32 buffSize = 0 ;
      CHAR *buff = _getBuffInfo ( type, buffSize ) ;
      if ( buff && buff[0] != 0 )
      {
         PD_TRACE_EXIT ( SDB__PMDEDUCB_GETINFO );
         return buff ;
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_GETINFO );
      return NULL ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_RESETINFO, "_pmdEDUCB::resetInfo" )
   void _pmdEDUCB::resetInfo ( EDU_INFO_TYPE type )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_RESETINFO );
      UINT32 buffSize = 0 ;
      CHAR *buff = _getBuffInfo ( type, buffSize ) ;
      if ( buff )
      {
         buff[0] = 0 ;
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_RESETINFO );
   }

   BOOLEAN _pmdEDUCB::_allocFromCatch( INT32 len, CHAR **ppBuff,
                                       INT32 &buffLen )
   {
      CATCH_MAP_IT it = _catchMap.lower_bound( len ) ;
      if ( it != _catchMap.end() )
      {
         *ppBuff = it->second ;
         buffLen = it->first ;
         _catchMap.erase( it ) ;
         _allocMap[ *ppBuff ] = buffLen ;
         _totalCatchSize -= buffLen ;
         return TRUE ;
      }
      return FALSE ;
   }

   INT32 _pmdEDUCB::allocBuff( INT32 len, CHAR **ppBuff, INT32 &buffLen )
   {
      INT32 rc = SDB_OK ;

      if ( _totalCatchSize >= len && _allocFromCatch( len, ppBuff, buffLen ) )
      {
         goto done ;
      }

      len = ossRoundUpToMultipleX( len, EDU_MEM_ALIGMENT_SIZE ) ;
      *ppBuff = ( CHAR* )SDB_OSS_MALLOC( len ) ;
      if( !*ppBuff )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "Edu[%s] malloc memory[size: %d] failed",
                 toString().c_str(), len ) ;
         goto error ;
      }
      buffLen = len ;

      _totalMemSize += buffLen ;
      _allocMap[ *ppBuff ] = buffLen ;

   done:
      return rc ;
   error:
      goto done ;
   }

   void _pmdEDUCB::releaseBuff( CHAR *pBuff )
   {
      ALLOC_MAP_IT itAlloc = _allocMap.find( pBuff ) ;
      if ( itAlloc == _allocMap.end() )
      {
         SDB_OSS_FREE( pBuff ) ;
         return ;
      }
      INT32 buffLen = itAlloc->second ;
      _allocMap.erase( itAlloc ) ;

      if ( (UINT32)buffLen > EDU_MAX_CATCH_SIZE )
      {
         SDB_OSS_FREE( pBuff ) ;
         _totalMemSize -= buffLen ;
      }
      else
      {
         _catchMap.insert( std::make_pair( buffLen, pBuff ) ) ;
         _totalCatchSize += buffLen ;

         while ( _totalCatchSize > EDU_MAX_CATCH_SIZE )
         {
            CATCH_MAP_IT it = _catchMap.begin() ;
            SDB_OSS_FREE( it->second ) ;
            _totalMemSize -= it->first ;
            _totalCatchSize -= it->first ;
            _catchMap.erase( it ) ;
         }
      }
   }

   INT32 _pmdEDUCB::reallocBuff( INT32 len, CHAR **ppBuff, INT32 &buffLen )
   {
      INT32 rc = SDB_OK ;
      CHAR *pOld = *ppBuff ;
      INT32 oldLen = buffLen ;

      ALLOC_MAP_IT itAlloc = _allocMap.find( *ppBuff ) ;
      if ( itAlloc != _allocMap.end() )
      {
         buffLen = itAlloc->second ;
         oldLen = buffLen ;
      }
      else if ( *ppBuff != NULL || buffLen != 0 )
      {
         PD_LOG( PDERROR, "EDU[%s] realloc input buffer error",
                 toString().c_str() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( buffLen >= len )
      {
         goto done ;
      }
      len = ossRoundUpToMultipleX( len, EDU_MEM_ALIGMENT_SIZE ) ;
      *ppBuff = ( CHAR* )SDB_OSS_REALLOC( *ppBuff, len ) ;
      if ( !*ppBuff )
      {
         PD_LOG( PDERROR, "Failed to realloc memory, size: %d", len ) ;
         goto error ;
      }

      buffLen = len ;

      _totalMemSize += ( len - oldLen ) ;

      _allocMap[ *ppBuff ] = buffLen ;

   done:
      return rc ;
   error:
      if ( pOld )
      {
         releaseBuff( pOld ) ;
         *ppBuff = NULL ;
         buffLen = 0 ;
      }
      goto done ;
   }

   void _pmdEDUCB::restoreBuffs( _pmdEDUCB::CATCH_MAP &catchMap )
   {
      CATCH_MAP_IT it = catchMap.begin() ;
      while ( it != catchMap.end() )
      {
         _catchMap.insert( std::make_pair( it->first, it->second ) ) ;
         _totalCatchSize += it->first ;
         _totalMemSize += it->first ;
         ++it ;
      }
      catchMap.clear() ;
   }

   void _pmdEDUCB::saveBuffs( _pmdEDUCB::CATCH_MAP &catchMap )
   {
      if ( _pCompressBuff )
      {
         releaseBuff( _pCompressBuff ) ;
         _pCompressBuff = NULL ;
      }
      _compressBuffLen = 0 ;
      if ( _pUncompressBuff )
      {
         releaseBuff( _pUncompressBuff ) ;
         _pUncompressBuff = NULL ;
      }
      _uncompressBuffLen = 0 ;

      CHAR *pBuff = NULL ;
      ALLOC_MAP_IT itAlloc = _allocMap.begin() ;
      while ( itAlloc != _allocMap.end() )
      {
         pBuff = itAlloc->first ;
         ++itAlloc ;
         releaseBuff( pBuff ) ;
      }
      _allocMap.clear() ;

      CATCH_MAP_IT it = _catchMap.begin() ;
      while ( it != _catchMap.end() )
      {
         _totalCatchSize -= it->first ;
         _totalMemSize -= it->first ;
         catchMap.insert( std::make_pair( it->first, it->second ) ) ;
         ++it ;
      }
      _catchMap.clear() ;
   }

   CHAR* _pmdEDUCB::getCompressBuff( INT32 len )
   {
      if ( _compressBuffLen < len )
      {
         if ( _pCompressBuff )
         {
            releaseBuff( _pCompressBuff ) ;
            _pCompressBuff = NULL ;
         }
         _compressBuffLen = 0 ;

         allocBuff( len, &_pCompressBuff, _compressBuffLen ) ;
      }

      return _pCompressBuff ;
   }

   CHAR* _pmdEDUCB::getUncompressBuff( INT32 len )
   {
      if ( _uncompressBuffLen < len )
      {
         if ( _pUncompressBuff )
         {
            releaseBuff( _pUncompressBuff ) ;
            _pUncompressBuff = NULL ;
         }
         _uncompressBuffLen = 0 ;

         allocBuff( len, &_pUncompressBuff, _uncompressBuffLen ) ;
      }

      return _pUncompressBuff ;
   }

#if defined ( SDB_ENGINE )
   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_CONTXTPEEK, "_pmdEDUCB::contextPeek" )
   SINT64 _pmdEDUCB::contextPeek ()
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_CONTXTPEEK );
      ossScopedLock _lock ( &_mutex, EXCLUSIVE ) ;
      SINT64 contextID = -1 ;
      std::set<SINT64>::const_iterator it ;
      if ( _contextList.empty() )
         goto done ;
      it = _contextList.begin() ;
      contextID = (*it) ;
      _contextList.erase(it) ;
   done :
      PD_TRACE1 ( SDB__PMDEDUCB_CONTXTPEEK, PD_PACK_LONG(contextID) );
      PD_TRACE_EXIT ( SDB__PMDEDUCB_CONTXTPEEK );
      return contextID ;
   }

   void _pmdEDUCB::clearTransInfo()
   {
      _curTransID = DPS_INVALID_TRANS_ID ;
      _relatedTransLSN = DPS_INVALID_LSN_OFFSET ;
      _curTransLSN = DPS_INVALID_LSN_OFFSET ;
      dpsTransCB *pTransCB = pmdGetKRCB()->getTransCB();
      if ( pTransCB )
      {
         pTransCB->transLockReleaseAll( this );
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB___PMDEDUCB_DUMPINFO, "_pmdEDUCB::dumpInfo" )
   void _pmdEDUCB::dumpInfo ( monEDUSimple &simple )
   {
      PD_TRACE_ENTRY ( SDB___PMDEDUCB_DUMPINFO );
      ossScopedLock _lock ( &_mutex, SHARED ) ;
      ossMemset ( &simple._eduStatus, 0, MON_EDU_STATUS_SZ ) ;
      ossMemset ( &simple._eduType, 0, MON_EDU_TYPE_SZ ) ;
      ossMemset ( &simple._eduName, 0, MON_EDU_NAME_SZ ) ;
      simple._eduID = _eduID ;
      simple._tid = _tid ;
      ossStrncpy ( simple._eduStatus, getEDUStatusDesp(_status),
                   MON_EDU_STATUS_SZ ) ;
      ossStrncpy ( simple._eduType, getEDUName (_eduType), MON_EDU_TYPE_SZ ) ;
      ossStrncpy ( simple._eduName, _Name, MON_EDU_NAME_SZ ) ;
      PD_TRACE_EXIT ( SDB___PMDEDUCB_DUMPINFO );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB___PMDEDUCB_DUMPINFO2, "_pmdEDUCB::dumpInfo" )
   void _pmdEDUCB::dumpInfo ( monEDUFull &full )
   {
      PD_TRACE_ENTRY ( SDB___PMDEDUCB_DUMPINFO2 );
      ossScopedLock _lock ( &_mutex, SHARED ) ;
      ossMemset ( &full._eduStatus, 0, MON_EDU_STATUS_SZ ) ;
      ossMemset ( &full._eduType, 0, MON_EDU_TYPE_SZ ) ;
      ossMemset ( &full._eduName, 0, MON_EDU_NAME_SZ ) ;
      full._eduID = _eduID ;
      full._tid = _tid ;
      full._processEventCount = _processEventCount ;
      full._queueSize = _queue.size() ;
      ossStrncpy ( full._eduStatus, getEDUStatusDesp(_status),
                   MON_EDU_STATUS_SZ ) ;
      ossStrncpy ( full._eduType, getEDUName (_eduType), MON_EDU_TYPE_SZ ) ;
      ossStrncpy ( full._eduName, _Name, MON_EDU_NAME_SZ ) ;

      full._monApplCB = _monApplCB ;
      full._threadHdl = _threadHdl ;

      full._eduContextList = _contextList ;
      PD_TRACE_EXIT ( SDB___PMDEDUCB_DUMPINFO2 );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_GETTRANSLOCK, "_pmdEDUCB::getTransLock" )
   dpsTransCBLockInfo *_pmdEDUCB::getTransLock( const dpsTransLockId &lockId )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_GETTRANSLOCK );
      dpsTransCBLockInfo *pLockInfo = NULL;
      DpsTransCBLockList::iterator iterLst = _transLockLst.find( lockId );
      if ( iterLst != _transLockLst.end() )
      {
         pLockInfo = iterLst->second ;
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_GETTRANSLOCK );
      return pLockInfo;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_ADDLOCKINFO, "_pmdEDUCB::addLockInfo" )
   void _pmdEDUCB::addLockInfo( const dpsTransLockId &lockId, DPS_TRANSLOCK_TYPE lockType )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_ADDLOCKINFO );
      dpsTransCBLockInfo *pLockInfo = NULL ;
      pLockInfo = SDB_OSS_NEW dpsTransCBLockInfo( lockType );
      if ( pLockInfo )
      {
         _transLockLst[ lockId ] = pLockInfo ;
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_ADDLOCKINFO );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_DELLOCKINFO, "_pmdEDUCB::delLockInfo" )
   void _pmdEDUCB::delLockInfo( const dpsTransLockId &lockId )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_DELLOCKINFO );
      DpsTransCBLockList::iterator iter
                        = _transLockLst.find( lockId );
      if ( iter != _transLockLst.end() )
      {
         SDB_OSS_DEL iter->second;
         _transLockLst.erase( iter );
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_DELLOCKINFO );
   }

   DpsTransCBLockList *_pmdEDUCB::getLockList()
   {
      return &_transLockLst;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_CREATETRANSACTION, "_pmdEDUCB::createTransaction" )
   INT32 _pmdEDUCB::createTransaction()
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_CREATETRANSACTION );
      INT32 rc = SDB_OK;
      if ( NULL == _pTransNodeMap )
      {
         _pTransNodeMap = new DpsTransNodeMap;
         setTransRC(SDB_OK);
      }
      if ( NULL == _pTransNodeMap )
      {
         rc = SDB_OOM;
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_CREATETRANSACTION );
      return rc;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_DELTRANSACTION, "_pmdEDUCB::delTransaction" )
   void _pmdEDUCB::delTransaction()
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_DELTRANSACTION );
      if ( _pTransNodeMap )
      {
         delete _pTransNodeMap;
         _pTransNodeMap = NULL;
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_DELTRANSACTION );
   }

   DpsTransNodeMap *_pmdEDUCB::getTransNodeLst()
   {
      return _pTransNodeMap;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_ADDTRANSNODE, "_pmdEDUCB::addTransNode" )
   void _pmdEDUCB::addTransNode( MsgRouteID &routeID )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_ADDTRANSNODE );
      if ( _pTransNodeMap )
      {
         (*_pTransNodeMap)[routeID.columns.groupID] = routeID;
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_ADDTRANSNODE );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_GETTRANSNODEROUTEID, "_pmdEDUCB::getTransNodeRouteID" )
   void _pmdEDUCB::getTransNodeRouteID( UINT32 groupID,
                                        MsgRouteID &routeID )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_GETTRANSNODEROUTEID );
      DpsTransNodeMap::iterator iterMap;
      routeID.value = 0;
      if ( _pTransNodeMap )
      {
         iterMap = _pTransNodeMap->find( groupID );
         if ( iterMap != _pTransNodeMap->end() )
         {
            routeID = iterMap->second;
         }
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_GETTRANSNODEROUTEID );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDEDUCB_ISTRANSNODE, "_pmdEDUCB::isTransNode" )
   BOOLEAN _pmdEDUCB::isTransNode( MsgRouteID &routeID )
   {
      PD_TRACE_ENTRY ( SDB__PMDEDUCB_ISTRANSNODE );
      BOOLEAN isTransNode = FALSE;
      DpsTransNodeMap::iterator iterMap;
      if ( _pTransNodeMap )
      {
         iterMap = _pTransNodeMap->find( routeID.columns.groupID );
         if (  iterMap != _pTransNodeMap->end() )
         {
            isTransNode = TRUE;
         }
      }
      PD_TRACE_EXIT ( SDB__PMDEDUCB_ISTRANSNODE );
      return isTransNode;
   }

   BOOLEAN _pmdEDUCB::isTransaction()
   {
      if ( _pTransNodeMap )
      {
         return TRUE;
      }
      return FALSE;
   }

   void *_pmdEDUCB::getAlignedMemory( UINT32 alignment, UINT32 size )
   {
      SDB_ASSERT( alignment == OSS_FILE_DIRECT_IO_ALIGNMENT,
                  "rewrite this function if u want to use new alignment" ) ;
      if ( _alignedMemSize < size )
      {
         if ( NULL != _alignedMem )
         {
            SDB_OSS_ORIGINAL_FREE( _alignedMem ) ;
            _alignedMemSize = 0 ;
            _alignedMem = NULL ; 
         }

         _alignedMem = ossAlignedAlloc( alignment, size ) ;
         if ( NULL != _alignedMem )
         {
            _alignedMemSize = size ;
         }
      }
      return _alignedMem ;
   }

   void _pmdEDUCB::releaseAlignedMemory()
   {
      if ( NULL != _alignedMem )
      {
         SDB_OSS_ORIGINAL_FREE( _alignedMem ) ;
         _alignedMem = NULL ;
         _alignedMemSize = 0 ;
      }

      return ;
   }
#endif // SDB_ENGINE

   /*
      edu entry point functions
   */
   INT32 pmdEDUEntryPointWrapper ( EDU_TYPES type, pmdEDUCB *cb, void *arg )
   {
#if defined (_WINDOWS)
      __try
      {
#endif
         return pmdEDUEntryPoint ( type, pmdDeclareEDUCB ( cb ), arg ) ;
#if defined (_WINDOWS)
      }
      __except ( engine::ossEDUExceptionFilter ( GetExceptionInformation() ) )
      {}
#endif
      return SDB_SYS ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDEDUENTPNT, "pmdEDUEntryPoint" )
   INT32 pmdEDUEntryPoint ( EDU_TYPES type, pmdEDUCB *cb, void *arg )
   {
      INT32       rc           = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_PMDEDUENTPNT );
#if defined ( SDB_ENGINE )
      pmdKRCB     *krcb        = pmdGetKRCB () ;
#endif
      EDUID       myEDUID      = cb->getID () ; // edu id for myself
      pmdEDUMgr  *eduMgr       = cb->getEDUMgr() ; // the manager class
      pmdEDUEvent event ;
      BOOLEAN     eduDestroyed = FALSE ;
      BOOLEAN     isForced     = FALSE ;
      CHAR        eduName[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;

#if defined (_WINDOWS)
      HANDLE      tHdl = NULL ;
      BOOLEAN     isHdlCreated = false ;

      if ( DuplicateHandle( GetCurrentProcess(), GetCurrentThread(),
                            GetCurrentProcess(), &tHdl, 0, false, 
                            DUPLICATE_SAME_ACCESS ) )
      {
         isHdlCreated = true ;
      }
#elif defined (_LINUX )
      OSSTID tHdl = ossGetCurrentThreadID() ;
      cb->setThreadID ( ossPThreadSelf() ) ;
#endif
      cb->setThreadHdl( tHdl ) ;
      cb->setTID ( ossGetCurrentThreadID() ) ;
      eduMgr->setEDU ( ossGetCurrentThreadID(), myEDUID ) ;

      PD_LOG ( PDEVENT, "Start thread[%d] for EDU[ID:%lld, type:%s]",
               ossGetCurrentThreadID(), myEDUID, getEDUName( type ) ) ;

      while ( !eduDestroyed )
      {
         type = cb->getType () ;
         if ( !cb->waitEvent ( event, OSS_ONE_SEC ) )
         {
            if ( cb->isForced () )
            {
               isForced = TRUE ;
            }
            else
            {
               continue ;
            }
         }

         if ( !isForced && PMD_EDU_EVENT_RESUME == event._eventType )
         {
            eduMgr->waitEDU ( cb->getID () ) ;
            pmdEntryPoint entryFunc = getEntryFuncByType ( cb->getType() ) ;
            if ( NULL == entryFunc )
            {
               PD_LOG ( PDERROR , "EDU[type=%d] entry point func is NULL",
                        cb->getType() ) ;
               PMD_SHUTDOWN_DB( SDB_SYS ) ;
               rc = SDB_SYS ;
            }
            else
            {
#if defined ( SDB_ENGINE )
               *(cb->getMonConfigCB() ) = *( (monConfigCB*)(krcb->getMonCB()) );
               cb->initMonAppCB() ;
#endif // SDB_ENGINE

               rc = entryFunc ( cb, event._Data ) ;

               ossStrncpy( eduName, cb->getName(), OSS_MAX_PATHSIZE ) ;
            }

            if ( PMD_IS_DB_UP )
            {
               if ( isSystemEDU( cb->getType() ) )
               {
                  PD_LOG ( PDSEVERE, "System EDU[ID:%lld, type:%s] exit "
                           "with %d", cb->getID(), getEDUName(cb->getType()),
                           rc ) ;
                  PMD_SHUTDOWN_DB( rc ) ;
               }
               else if ( SDB_OK != rc )
               {
                  PD_LOG ( PDWARNING, "EDU[ID:%lld, type:%s, Name:%s] exit "
                           "with %d", cb->getID(), getEDUName(cb->getType()),
                           cb->getName(), rc ) ;
               }
            }

            eduMgr->waitEDU ( cb->getID () ) ;
         }
         else if ( !isForced && PMD_EDU_EVENT_TERM != event._eventType )
         {
            PD_LOG ( PDERROR, "Recieve the error event[type=%d] in "
                     "EDU[ID:%lld, type:%s]", event._eventType, myEDUID,
                     getEDUName(cb->getType()) ) ;
            rc = SDB_SYS ;
         }
         else if ( !isForced && PMD_EDU_EVENT_TERM == event._eventType &&
                   cb->isForced () )
         {
            isForced = TRUE ;
         }

         if ( !isForced )
         {
            pmdEduEventRelase( event, cb ) ;
            event.reset () ;
         }


#if defined ( SDB_ENGINE )
         cb->resetMon() ;

         {
            SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
            SINT64 contextID = -1 ;
            while ( -1 != (contextID = cb->contextPeek() ) )
            {
               rtnCB->contextDelete( contextID, NULL ) ;
               PD_LOG ( PDWARNING, "EDU[%lld,%s] context[%d] leaked",
                        myEDUID, getEDUName(type), contextID ) ;
            }
         }

         SDB_ASSERT( 0 == cb->getDmsLockLevel(), "Dms lock level must be 0" ) ;
#endif // SDB_ENGINE

         cb->clear() ;
         rc = eduMgr->returnEDU ( cb->getID (), isForced, &eduDestroyed ) ;

         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR, "Invalid EDU Status for EDU[TID:%d, ID:%lld, "
                     "type:%s, Name: %s]", ossGetCurrentThreadID(), myEDUID,
                     getEDUName( type ), eduName ) ;
         }
         else if ( !eduDestroyed )
         {
            PD_LOG( PDINFO, "Push thread[%] for EDU[ID:%lld, Type:%s, "
                    "Name: %s] to thread pool", ossGetCurrentThreadID(),
                    myEDUID, getEDUName( type ), eduName ) ;
         }
      }
      pmdUndeclareEDUCB () ;
      PD_LOG ( PDEVENT, "Terminating thread[%d] for EDU[ID:%lld, Type:%s, "
               "Name: %s]", ossGetCurrentThreadID(), myEDUID,
               getEDUName( type ), eduName ) ;

   #if defined (_WINDOWS)
      if ( isHdlCreated )
      {
         CloseHandle( tHdl ) ;
      }
   #endif
      PD_TRACE_EXITRC ( SDB_PMDEDUENTPNT, rc );
      return rc ;
   }

   static OSS_THREAD_LOCAL _pmdEDUCB *__eduCB ;

   _pmdEDUCB *pmdGetThreadEDUCB ()
   {
      return __eduCB ;
   }

   _pmdEDUCB *pmdCreateThreadEDUCB( _pmdEDUMgr *mgr, EDU_TYPES type )
   {
      __eduCB = SDB_OSS_NEW _pmdEDUCB ( mgr, type ) ;
      return __eduCB ;
   }

   void pmdDeleteThreadEDUCB ()
   {
      if ( __eduCB )
      {
         SDB_OSS_DEL __eduCB ;
         __eduCB = NULL ;
      }
   }

   _pmdEDUCB *pmdDeclareEDUCB ( _pmdEDUCB *p )
   {
      __eduCB = p ;
      return __eduCB ;
   }

   void pmdUndeclareEDUCB ()
   {
      __eduCB = NULL ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDRECV, "pmdRecv" )
   INT32 pmdRecv ( CHAR *pBuffer, INT32 recvSize,
                   ossSocket *sock, pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( sock, "Socket is NULL" ) ;
      SDB_ASSERT ( cb, "cb is NULL" ) ;
      PD_TRACE_ENTRY ( SDB_PMDRECV );
      INT32 receivedSize = 0 ;
      INT32 totalReceivedSize = 0 ;
      while ( TRUE )
      {
         if ( cb->isForced () )
         {
            rc = SDB_APP_FORCED ;
            goto done ;
         }
         rc = sock->recv ( &pBuffer[totalReceivedSize],
                           recvSize-totalReceivedSize,
                           receivedSize ) ;
         totalReceivedSize += receivedSize ;
         if ( SDB_TIMEOUT == rc )
         {
            continue ;
         }
         goto done ;
      }
   done :
#if defined ( SDB_ENGINE )
      if ( totalReceivedSize > 0 )
      {
         pmdGetKRCB()->getMonDBCB()->svcNetInAdd( totalReceivedSize ) ;
      }
#endif // SDB_ENGINE
      PD_TRACE_EXITRC ( SDB_PMDRECV, rc );
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDSEND, "pmdSend" )
   INT32 pmdSend ( const CHAR *pBuffer, INT32 sendSize,
                   ossSocket *sock, pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( sock, "Socket is NULL" ) ;
      SDB_ASSERT ( cb, "cb is NULL" ) ;
      PD_TRACE_ENTRY ( SDB_PMDSEND );
      INT32 sentSize = 0 ;
      INT32 totalSentSize = 0 ;
      while ( true )
      {
         if ( cb->isForced () )
         {
            rc = SDB_APP_FORCED ;
            goto done ;
         }
         rc = sock->send ( &pBuffer[totalSentSize],
                           sendSize-totalSentSize,
                           sentSize ) ;
         totalSentSize += sentSize ;
         if ( SDB_TIMEOUT == rc )
            continue ;
         goto done ;
      }
   done :
#if defined ( SDB_ENGINE )
      if ( totalSentSize > 0 )
      {
         pmdGetKRCB()->getMonDBCB()->svcNetOutAdd( totalSentSize ) ;
      }
#endif // SDB_ENGINE
      PD_TRACE_EXITRC ( SDB_PMDSEND, rc );
      return rc ;
   }

   void pmdEduEventRelase( pmdEDUEvent &event, pmdEDUCB *cb )
   {
      if ( event._Data && event._dataMemType != PMD_EDU_MEM_NONE )
      {
         if ( PMD_EDU_MEM_ALLOC == event._dataMemType )
         {
            SDB_OSS_FREE( event._Data ) ;
         }
         else if ( PMD_EDU_MEM_SELF == event._dataMemType )
         {
            SDB_ASSERT( cb, "cb can't be NULL" ) ;
            if ( cb )
            {
               cb->releaseBuff( (CHAR *)event._Data ) ;
            }
         }
         event._Data = NULL ;
      }
   }

}

