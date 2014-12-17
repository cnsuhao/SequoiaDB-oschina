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

   Source File Name = rtnCB.hpp

   Descriptive Name = RunTime Control Block Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains structure for Runtime
   Control Block.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef RTNCB_HPP_
#define RTNCB_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "rtnContext.hpp"
#include "ossLatch.hpp"
#include "pd.hpp"
#include "monEDU.hpp"
#include "dmsCB.hpp"
#include "pmdEDU.hpp"
#include "sdbInterface.hpp"
#include <map>
#include <set>

namespace engine
{
   /*
      _SDB_RTNCB define
   */
   class _SDB_RTNCB : public _IControlBlock
   {
   private :
   #ifdef RTNCB_XLOCK
   #undef RTNCB_XLOCK
   #endif
   #define RTNCB_XLOCK ossScopedLock _lock(&_mutex, EXCLUSIVE) ;
   #ifdef RTNCB_SLOCK
   #undef RTNCB_SLOCK
   #endif
   #define RTNCB_SLOCK ossScopedLock _lock(&_mutex, SHARED) ;
      ossSpinSLatch _mutex ;

      std::map<SINT64, rtnContext *> _contextList ;
      SINT64 _contextHWM ;

   public :
      _SDB_RTNCB() ;
      virtual ~_SDB_RTNCB() ;

      virtual SDB_CB_TYPE cbType() const { return SDB_CB_RTN ; }
      virtual const CHAR* cbName() const { return "RTNCB" ; }

      virtual INT32  init () ;
      virtual INT32  active () ;
      virtual INT32  deactive () ;
      virtual INT32  fini () ;

      SINT32 contextNew ( RTN_CONTEXT_TYPE type, rtnContext **context,
                          SINT64 &contextID, _pmdEDUCB * pEDUCB ) ;

      void contextDelete ( SINT64 contextID, _pmdEDUCB *cb ) ;

      OSS_INLINE rtnContext *contextFind ( SINT64 contextID )
      {
         RTNCB_SLOCK
         std::map<SINT64, rtnContext*>::const_iterator it ;
         if ( _contextList.end() == (it = _contextList.find(contextID)))
            return NULL ;
         return (*it).second ;
      }

      OSS_INLINE INT32 contextNum ()
      {
         RTNCB_SLOCK
         return _contextList.size() ;
      }

      OSS_INLINE void contextDump ( std::map<UINT64, std::set<SINT64> > &contextList )
      {
         UINT64 eduID = PMD_INVALID_EDUID ;
         INT64  contextID = -1  ;

         RTNCB_SLOCK
         std::map<SINT64, rtnContext*>::const_iterator it ;
         for ( it = _contextList.begin() ; it != _contextList.end(); ++it )
         {
            eduID = (*it).second->eduID() ;
            contextID = (*it).second->contextID() ;

            contextList[ eduID ].insert( contextID ) ;
         }
      }

      OSS_INLINE void monContextSnap ( std::map<UINT64, std::set<monContextFull> >
                                   &contextList )
      {
         UINT64 eduID = PMD_INVALID_EDUID ;
         INT64  contextID = -1  ;
         monContextCB *monCB = NULL ;

         RTNCB_SLOCK
         std::map<SINT64, rtnContext*>::const_iterator it ;
         for ( it = _contextList.begin() ; it != _contextList.end(); it++ )
         {
            eduID = (*it).second->eduID() ;
            contextID = (*it).second->contextID() ;
            monCB = (*it).second->getMonCB() ;

            monContextFull item( contextID, *monCB ) ;
            item._typeDesp = getContextTypeDesp( (*it).second->getType() ) ;
            item._info = (*it).second->toString() ;

            contextList[ eduID ].insert( item ) ;
         }
      }

      OSS_INLINE void monContextSnap( UINT64 eduID,
                                  std::set<monContextFull> &contextList )
      {
         INT64  contextID = -1  ;
         monContextCB *monCB = NULL ;

         RTNCB_SLOCK
         std::map<SINT64, rtnContext*>::const_iterator it ;
         for ( it = _contextList.begin() ; it != _contextList.end() ; it++ )
         {
            if ( (*it).second->eduID() == eduID )
            {
               contextID = (*it).second->contextID() ;
               monCB = (*it).second->getMonCB() ;

               monContextFull item( contextID, *monCB ) ;
               item._typeDesp = getContextTypeDesp( (*it).second->getType() ) ;
               item._info = (*it).second->toString() ;

               contextList.insert( item ) ;
            }
         }
      }

   } ;
   typedef class _SDB_RTNCB SDB_RTNCB ;

   /*
      get global rtn cb
   */
   SDB_RTNCB* sdbGetRTNCB () ;

}

#endif //RTNCB_HPP_

