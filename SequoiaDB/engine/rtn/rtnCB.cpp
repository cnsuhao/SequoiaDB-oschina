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

   Source File Name = rtnCB.cpp

   Descriptive Name = Runtime Control Block

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   control block.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "rtnCB.hpp"
#include "pdTrace.hpp"
#include "rtnTrace.hpp"
#include "rtnContextSort.hpp"
#include "rtnContextLob.hpp"
#include "rtnContextShdOfLob.hpp"
#include "rtnContextListLob.hpp"

using namespace std;
namespace engine
{

   _SDB_RTNCB::_SDB_RTNCB()
   {
      _contextHWM = 0 ;
   }

   _SDB_RTNCB::~_SDB_RTNCB()
   {
      std::map<SINT64, rtnContext *>::const_iterator it ;
      for ( it = _contextList.begin(); it != _contextList.end(); it++ )
      {
         SDB_OSS_DEL ((*it).second) ;
      }
      _contextList.clear() ;
   }

   INT32 _SDB_RTNCB::init ()
   {
      return SDB_OK ;
   }

   INT32 _SDB_RTNCB::active ()
   {
      return SDB_OK ;
   }

   INT32 _SDB_RTNCB::deactive ()
   {
      return SDB_OK ;
   }

   INT32 _SDB_RTNCB::fini ()
   {
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__SDB_RTNCB_CONTEXTDEL, "_SDB_RTNCB::contextDelete" )
   void _SDB_RTNCB::contextDelete ( SINT64 contextID, pmdEDUCB *cb )
   {
      PD_TRACE_ENTRY ( SDB__SDB_RTNCB_CONTEXTDEL ) ;

      rtnContext *pContext = NULL ;
      std::map<SINT64, rtnContext*>::iterator it ;

      if ( cb )
      {
         cb->contextDelete( contextID ) ;
      }

      {
         RTNCB_XLOCK
         it = _contextList.find( contextID ) ;
         if ( _contextList.end() != it )
         {
            pContext = it->second;
            _contextList.erase( it ) ;
         }
      }

      if ( pContext )
      {
         INT32 reference = pContext->getReference() ;
         pContext->waitForPrefetch() ;
         SDB_OSS_DEL pContext ;
         PD_LOG( PDDEBUG, "delete context(contextID=%lld, reference: %d)",
                 contextID, reference ) ;
      }

      PD_TRACE_EXIT ( SDB__SDB_RTNCB_CONTEXTDEL ) ;
      return ;
   }

   SINT32 _SDB_RTNCB::contextNew ( RTN_CONTEXT_TYPE type,
                                   rtnContext **context,
                                   SINT64 &contextID,
                                   _pmdEDUCB * pEDUCB )
   {
      SDB_ASSERT ( context, "context pointer can't be NULL" ) ;
      {
         RTNCB_XLOCK
         if ( _contextHWM+1 < 0 )
         {
            return SDB_SYS ;
         }

         switch ( type )
         {
            case RTN_CONTEXT_DATA :
               (*context) = SDB_OSS_NEW rtnContextData ( _contextHWM,
                                                         pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_DUMP :
               (*context) = SDB_OSS_NEW rtnContextDump ( _contextHWM,
                                                         pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_COORD :
               (*context) = SDB_OSS_NEW rtnContextCoord ( _contextHWM,
                                                          pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_QGM :
               (*context) = SDB_OSS_NEW rtnContextQGM ( _contextHWM,
                                                        pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_TEMP :
               (*context) = SDB_OSS_NEW rtnContextTemp ( _contextHWM,
                                                         pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_SP :
               (*context) = SDB_OSS_NEW rtnContextSP ( _contextHWM,
                                                       pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_PARADATA :
               (*context) = SDB_OSS_NEW rtnContextParaData( _contextHWM,
                                                            pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_MAINCL :
               (*context) = SDB_OSS_NEW rtnContextMainCL( _contextHWM,
                                                         pEDUCB->getID() );
               break;
            case RTN_CONTEXT_SORT :
               (*context) = SDB_OSS_NEW rtnContextSort( _contextHWM,
                                                        pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_QGMSORT :
               (*context) = SDB_OSS_NEW rtnContextQgmSort( _contextHWM,
                                                            pEDUCB->getID() ) ;
               break ;
            case RTN_CONTEXT_DELCS :
               (*context) = SDB_OSS_NEW rtnContextDelCS( _contextHWM,
                                                            pEDUCB->getID() ) ;
               break;
            case RTN_CONTEXT_DELCL :
               (*context) = SDB_OSS_NEW rtnContextDelCL( _contextHWM,
                                                            pEDUCB->getID() ) ;
               break;
            case RTN_CONTEXT_DELMAINCL :
               (*context) = SDB_OSS_NEW rtnContextDelMainCL( _contextHWM,
                                                            pEDUCB->getID() ) ;
               break;
            case RTN_CONTEXT_EXPLAIN :
                (*context) = SDB_OSS_NEW rtnContextExplain( _contextHWM,
                                                            pEDUCB->getID() ) ;
                break ;
            case RTN_CONTEXT_LOB :
                 (*context) = SDB_OSS_NEW rtnContextLob( _contextHWM,
                                                         pEDUCB->getID() ) ;
                break ;
            case RTN_CONTEXT_SHARD_OF_LOB :
                 (*context) = SDB_OSS_NEW rtnContextShdOfLob( _contextHWM,
                                                              pEDUCB->getID() ) ;
                break ;
            case RTN_CONTEXT_LIST_LOB:
                 (*context) = SDB_OSS_NEW rtnContextListLob( _contextHWM,
                                                             pEDUCB->getID() ) ;
                break ;
            default :
               PD_LOG( PDERROR, "Unknow context type: %d", type ) ;
               return SDB_SYS ;
         }

         if ( !(*context) )
         {
            return SDB_OOM ;
         }

         _contextList[_contextHWM] = *context ;
         pEDUCB->contextInsert( _contextHWM ) ;
         contextID = _contextHWM ;
         ++_contextHWM ;
      }
      PD_LOG ( PDDEBUG, "Create new context(contextID=%lld, type: %d[%s])",
               contextID, type, getContextTypeDesp(type) ) ;
      return SDB_OK ;
   }

   /*
      get global rtn cb
   */
   SDB_RTNCB* sdbGetRTNCB ()
   {
      static SDB_RTNCB s_rtnCB ;
      return &s_rtnCB ;
   }

}

