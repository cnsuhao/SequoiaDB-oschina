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

   Source File Name = pmdEnv.hpp

   Descriptive Name = Process MoDel Main

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for SequoiaDB,
   and all other process-initialization code.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          22/04/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef PMDENV_HPP_
#define PMDENV_HPP_

#include "utilCommon.hpp"
#include "ossAtomic.hpp"

using namespace bson ;

namespace engine
{
   /*
      When recieve quit event or signal, will call
   */
   typedef  void (*PMD_ON_QUIT_FUNC)() ;

   /*
      pmd system info define
   */
   typedef struct _pmdSysInfo
   {
      SDB_ROLE                      _dbrole ;
      MsgRouteID                    _nodeID ;
      ossAtomic32                   _isPrimary ;
      SDB_TYPE                      _dbType ;

      BOOLEAN                       _quitFlag ;
      PMD_ON_QUIT_FUNC              _pQuitFunc ;

      _pmdSysInfo()
      :_isPrimary( 0 )
      {
         _dbrole        = SDB_ROLE_STANDALONE ;
         _nodeID.value  = MSG_INVALID_ROUTEID ;
         _quitFlag      = FALSE ;
         _dbType        = SDB_TYPE_DB ;
         _pQuitFunc     = NULL ;
      }
   } pmdSysInfo ;

   SDB_ROLE       pmdGetDBRole() ;
   void           pmdSetDBRole( SDB_ROLE role ) ;
   SDB_TYPE       pmdGetDBType() ;
   void           pmdSetDBType( SDB_TYPE type ) ;
   MsgRouteID     pmdGetNodeID() ;
   void           pmdSetNodeID( MsgRouteID id ) ;
   BOOLEAN        pmdIsPrimary() ;
   void           pmdSetPrimary( BOOLEAN primary ) ;

   void           pmdSetQuit() ;
   BOOLEAN        pmdIsQuitApp() ;

   pmdSysInfo*    pmdGetSysInfo () ;

   /*
      pmd trap functions
   */

   INT32    pmdEnableSignalEvent( const CHAR *filepath,
                                  PMD_ON_QUIT_FUNC pFunc,
                                  INT32 *pDelSig = NULL ) ;

   INT32&   pmdGetSigNum() ;

   /*
      Env define
   */
   #define  PMD_SIGNUM                 pmdGetSigNum()

}

#endif //PMDENV_HPP_

