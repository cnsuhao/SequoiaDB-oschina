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

   Source File Name = omagentSyncCmd.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_SYNC_CMD_HPP_
#define OMAGENT_SYNC_CMD_HPP_

#include "omagentCmdBase.hpp"

using namespace bson ;
using namespace std ;

namespace engine
{
   class _omaTaskMgr ;

   /******************************* scan host *********************************/
   /*
      _omaScanHost
   */
   class _omaScanHost : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER()
      public:
         _omaScanHost () ;
         ~_omaScanHost () ;
         virtual const CHAR* name () { return OMA_CMD_SCAN_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   /******************************* pre-check host ****************************/
   /*
      _omaPreCheckHost
   */
   class _omaPreCheckHost : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER()
      public:
         _omaPreCheckHost () ;
         ~_omaPreCheckHost () ;
         virtual const CHAR* name () { return OMA_CMD_PRE_CHECK_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
      private:
         INT32 _getProgPath( CHAR *path, INT32 len ) ;
   } ;

   /******************************* check host ********************************/
   /*
      _omaCheckHost
   */
   class _omaCheckHost : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaCheckHost () ;
         ~_omaCheckHost () ;
         virtual const CHAR* name () { return OMA_CMD_CHECK_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   /******************************* post-check host ***************************/
   /*
      _omaPostCheckHost
   */
   class _omaPostCheckHost : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaPostCheckHost () ;
         ~_omaPostCheckHost () ;
         virtual const CHAR* name () { return OMA_CMD_POST_CHECK_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   /******************************* query progress status ********************/
   /*
      _omaQueryTaskProgress
   */
/*
   class _omaQueryTaskProgress : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaQueryTaskProgress () ;
         ~_omaQueryTaskProgress () ;
         virtual const CHAR* name ()
         { 
            return OMA_CMD_QUERY_PROGRESS ;
         }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
         virtual INT32 doit ( BSONObj &retObj ) ;
      private:
         UINT64                             _taskID ;
         _omaTaskMgr*                       _taskMgr ;

   } ;
*/
   /***************************** update hosts table info ********************/
   /*
      _omaUpdateHostsInfo
   */
   class _omaUpdateHostsInfo : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaUpdateHostsInfo () ;
         ~_omaUpdateHostsInfo () ;
      
         virtual const CHAR * name ()
         {
            return OMA_CMD_UPDATE_HOSTS ;
         }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ; 

   /***************************** query host status ********************/
   /*
      _omaQueryHostStatus
   */
   class _omaQueryHostStatus : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaQueryHostStatus() ;
         ~_omaQueryHostStatus() ;

      public:
         virtual const CHAR* name () { return OMA_CMD_QUERY_HOST_STATUS ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   /***************************** handle task notify **************************/
   /*
      _omaHandleTaskNotify
   */
   class _omaHandleTaskNotify : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaHandleTaskNotify() ;
         ~_omaHandleTaskNotify() ;

      public:
         virtual const CHAR* name () { return OM_NOTIFY_TASK ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
         virtual INT32 doit ( BSONObj &retObj ) ;

      private:
         BSONObj   _taskIDObj ;
   } ;

   
} // namespace engine


#endif // OMAGENT_SYNC_CMD_HPP_
