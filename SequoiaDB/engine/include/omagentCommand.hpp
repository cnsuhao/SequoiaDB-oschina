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

   Source File Name = omagentCommand.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_COMMAND_HPP_
#define OMAGENT_COMMAND_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossTypes.h"
#include "ossUtil.h"
#include "../bson/bson.h"
#include "ossMem.h"
#include "ossSocket.hpp"
#include "omagentDef.hpp"
#include "omagentMsgDef.hpp"
#include "omagent.hpp"
#include "omagentTask.hpp"
#include "sptScope.hpp"
#include <map>
#include <string>

using namespace bson ;
using namespace std ;

namespace engine
{
   #define DECLARE_OACMD_AUTO_REGISTER()                       \
      public:                                                  \
         static _omaCommand *newThis () ;                      \

   #define IMPLEMENT_OACMD_AUTO_REGISTER(theClass)             \
      _omaCommand* theClass::newThis ()                        \
      {                                                        \
         return SDB_OSS_NEW theClass() ;                       \
      }                                                        \
      _omaCmdAssit theClass##Assit ( theClass::newThis ) ;     \

   /*
      _omaCommand
   */
   class _omaCommand : public SDBObject
   {
      public:
         _omaCommand () ;
         virtual ~_omaCommand () ;

         virtual BOOLEAN needCheckBusiness() const { return TRUE ; }

      public:
         virtual const CHAR * name () = 0 ;

         virtual INT32 prime () ; 

         virtual INT32 init ( const CHAR *pInstallInfo ) ;

         virtual INT32 doit ( BSONObj &retObj ) ;

         virtual INT32 final( BSONObj &rval, BSONObj &retObj ) ;

         virtual INT32 setJsFile ( const CHAR *fileName ) ;
         
         virtual INT32 addJsFile ( const CHAR *filename,
                                   const CHAR *bus = NULL,
                                   const CHAR *sys = NULL,
                                   const CHAR *env = NULL,
                                   const CHAR *other = NULL ) ;

         virtual INT32 getExcuteJsContent ( string &content ) ;

      protected:
         CHAR                            _jsFileName[ OSS_MAX_PATHSIZE + 1 ] ;
         CHAR                            _jsFileArgs[ JS_ARG_LEN + 1 ] ;
         CHAR                            *_fileBuff ;
         UINT32                          _buffSize ;
         UINT32                          _readSize ;
         vector<BSONObj>                 _hosts ;
         string                          _content ;
         vector< pair<string, string> >  _jsFiles ;
         _sptScope            *_scope ;
   } ;

   typedef _omaCommand* (*OA_NEW_FUNC) () ;

   /*
      _omaCmdAssit
   */
   class _omaCmdAssit : public SDBObject
   {
      public:
         _omaCmdAssit ( OA_NEW_FUNC ) ;
         virtual ~_omaCmdAssit () ;
   } ;

   struct _classComp
   {
      bool operator()( const CHAR *lhs, const CHAR *rhs ) const
      {
         return ossStrcmp( lhs, rhs ) < 0 ;
      }
   } ;

   typedef map<const CHAR*, OA_NEW_FUNC, _classComp>     MAP_OACMD ;
#if defined (_WINDOWS)
   typedef MAP_OACMD::iterator                           MAP_OACMD_IT ;
#else
   typedef map<const CHAR*, OA_NEW_FUNC>::iterator       MAP_OACMD_IT ;
#endif // _WINDOWS

   /*
      _omaCmdBuilder
   */
   class _omaCmdBuilder : public SDBObject
   {
      friend class _omaCmdAssit ;

      public:
         _omaCmdBuilder () ;
         ~_omaCmdBuilder () ;

      public:
         _omaCommand *create ( const CHAR *command ) ;

         void release ( _omaCommand *&pCommand ) ;

         INT32 _register ( const CHAR *name, OA_NEW_FUNC pFunc ) ;

         OA_NEW_FUNC _find ( const CHAR * name ) ;

      private:
         MAP_OACMD _cmdMap ;
   } ;

   /*
      get omagent command builder
   */
   _omaCmdBuilder* getOmaCmdBuilder() ;

   class _omaTaskMgr ;

   /******************************* scan host ********************************/
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


   /******************************* basic check *******************************/
   /*
      _omaBasicCheckHost
   */
   class _omaBasicCheckHost : public _omaScanHost
   {
      DECLARE_OACMD_AUTO_REGISTER()
      public:
         _omaBasicCheckHost () ;
         ~_omaBasicCheckHost () ;
         virtual const CHAR* name () { return OMA_CMD_BASIE_CHECK_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   /******************************* install remote agent **********************/
   /*
      _omaInstallRemoteAgent
   */
   class _omaInstallRemoteAgent : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER()
      public:
         _omaInstallRemoteAgent () ;
         ~_omaInstallRemoteAgent () ;
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

   /******************************* uninstall remote agent *******************/
   /*
      _omaUninstallRemoteAgent
   */
   class _omaUninstallRemoteAgent : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaUninstallRemoteAgent () ;
         ~_omaUninstallRemoteAgent () ;
         virtual const CHAR* name () { return OMA_CMD_POST_CHECK_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   /******************************* add host ********************************/
   /*
      _omaAddHost
   */
   class _omaAddHost : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER()
      public:
         _omaAddHost () ;
         ~_omaAddHost () ;
         virtual const CHAR * name () { return OMA_CMD_ADD_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
         virtual INT32 final( BSONObj &rval, BSONObj &retObj ) ;

      private:
         INT32 _getRollbackInfo( BSONObj &addHostResult,
                                 BSONObj &rollbackInfo ) ;
         INT32 _addHostRollback ( BSONObj &rollbackInfo,
                                  BSONObj &rollbackResult ) ;
         INT32 _buildErrDetail ( BSONObj &addHostResult,
                                 BSONObj &rollbackResult,
                                 CHAR *pBuf,
                                 INT32 bufSize ) ;
         INT32 _buildRetResult( BSONObj &obj, BSONObj &retObj ) ;

         BSONObj                          _addHostInfo ;
         INT32                            _transactionID ;
   } ;

   /******************************* add host ********************************/
   /*
      _omaAddHost
   */
   class _omaAddHost2 : public _omaCommand
   {
      public:
         _omaAddHost2 () ;
         ~_omaAddHost2 () ;
         virtual const CHAR * name () { return OMA_CMD_ADD_HOST ; }
         virtual INT32 init ( const CHAR *pAddHostInfo ) ;
         virtual INT32 doit ( BSONObj &retObj ) ;

      private:
         vector<AddHostInfo>        _addHostInfo ;
   } ;

   /******************************* remove host ********************************/
   /*
      _omaRemoveHost
   */
   class _omaRemoveHost : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER()
      public:
         _omaRemoveHost () ;
         ~_omaRemoveHost () ;
         virtual const CHAR * name () { return OMA_CMD_REMOVE_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   /******************************* install db business ***********************/
   /*
      _omaInsDBBus
   */
   class _omaInsDBBus : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaInsDBBus () ;
         ~_omaInsDBBus () ;

         virtual const CHAR* name () { return OMA_CMD_INSTALL_DB_BUSINESS ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
         virtual INT32 doit ( BSONObj &retObj ) ;
   } ;

   /******************************* uninstall db business *********************/
   /*
      _omaUninsDBBus
   */
   class _omaUninsDBBus : public _omaCommand
   {
      DECLARE_OACMD_AUTO_REGISTER ()
      public:
         _omaUninsDBBus () ;
         ~_omaUninsDBBus () ;

         virtual const CHAR* name () { return OMA_CMD_UNINSTALL_DB_BUSINESS ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;
         virtual INT32 doit ( BSONObj &retObj ) ;
      private:
         INT32 _getCataAddr( BSONObj &obj ) ;
         
         BSONObj                     _uninstallInfoObj ;
         vector<BSONObj>             _coord ;
         vector<BSONObj>             _catalog ;
         vector<BSONObj>             _data ;
         vector<BSONObj>             _standalone ;
         BOOLEAN                     _isStandalone ;
   } ;

   /******************************* query progress status ********************/
   /*
      _omaQueryTaskProgress
   */
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

      private:
   } ;

   class _omaCreateVirtualCoord : public _omaCommand
   {
      public:
         _omaCreateVirtualCoord () ;
         ~_omaCreateVirtualCoord () ;
         virtual const CHAR* name () { return OMA_CMD_CRRATE_VIRTUAL_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      public:
         INT32 createVirtualCoord ( BSONObj &retObj ) ;
   } ;

   class _omaRemoveVirtualCoord : public _omaCommand
   {
      public:
         _omaRemoveVirtualCoord ( const CHAR *vCoordSvcName ) ;
         ~_omaRemoveVirtualCoord () ;
         INT32 removeVirtualCoord ( BSONObj &retObj ) ;
         virtual const CHAR* name () { return OMA_CMD_REMOVE_VIRTUAL_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         CHAR _vCoordSvcName[OSS_MAX_SERVICENAME + 1] ;
   } ;

   class _omaAddHostRollbackInternal : public _omaCommand
   {
      public:
         _omaAddHostRollbackInternal() ;
         ~_omaAddHostRollbackInternal () ;
         virtual const CHAR* name () { return OMA_CMD_ROLLBACK_ADD_HOSTS ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   class _omaRunAddHost : public _omaCommand
   {
      public:
         _omaRunAddHost ( AddHostInfo &info ) ;
         ~_omaRunAddHost () ;
         virtual const CHAR * name () { return OMA_CMD_ADD_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         INT32 _getAddHostInfo( BSONObj &retObj ) ;
         
      private:
         AddHostInfo         _addHostInfo ;
   } ;

   class _omaRunRmHost : public _omaCommand
   {
      public:
         _omaRunRmHost( AddHostInfo &info ) ;
         ~_omaRunRmHost () ;
         virtual const CHAR* name () { return OMA_CMD_RM_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
      private:
         INT32 _getRmHostInfo( BSONObj &retObj ) ;
         
      private:
         AddHostInfo         _RmHostInfo ;
   } ;

   class _omaRunCheckAddHostInfo : public _omaCommand
   {
      public:
         _omaRunCheckAddHostInfo() ;
         ~_omaRunCheckAddHostInfo () ;
         virtual const CHAR* name () { return "" ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   class _omaRunCreateStandaloneJob : public _omaCommand
   {
      public:
         _omaRunCreateStandaloneJob ( string &vCoordSvcName,
                                      InstallInfo &info ) ;
         virtual ~_omaRunCreateStandaloneJob () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RUN_CREATE_STANDALONE ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         BSONObj                                        _installInfo ;
         InstallInfo                                    _info ;
         string                                         _vCoordSvcName ;
   } ;

   class _omaRunCreateCatalogJob : public _omaCommand
   {
      public:
         _omaRunCreateCatalogJob ( string &vCoordSvcName,
                                    InstallInfo &info ) ;
         virtual ~_omaRunCreateCatalogJob () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RUN_CREATE_CATALOG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         BSONObj                                        _installInfo ;
         InstallInfo                                    _info ;
         string                                         _vCoordSvcName ;
   } ;

   class _omaRunCreateCoordJob : public _omaCommand
   {
      public:
         _omaRunCreateCoordJob ( string &vCoordSvcName,
                                  InstallInfo &info ) ;
         virtual ~_omaRunCreateCoordJob () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RUN_CREATE_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         BSONObj                                        _installInfo ;
         InstallInfo                                    _info ;
         string                                         _vCoordSvcName ;
   } ;

   class _omaRunCreateDataNodeJob : public _omaCommand
   {
      public:
         _omaRunCreateDataNodeJob ( string &vCoordSvcName,
                                     InstallInfo &info ) ;
         virtual ~_omaRunCreateDataNodeJob () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RUN_CREATE_DATANODE ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         BSONObj                                        _installInfo ;
         InstallInfo                                    _info ;
         string                                         _vCoordSvcName ;
   } ;

   class _omaRunRollbackStandaloneJob : public _omaCommand
   {
      public:
         _omaRunRollbackStandaloneJob ( string &vCoordSvcName, 
                                        map< string, vector<InstalledNode> > &info
                                 ) ;
         ~_omaRunRollbackStandaloneJob () ;

      public:
         virtual const CHAR* name () { return OMA_ROLLBACK_STANDALONE ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         void _getInstallStandaloneInfo( BSONObj &obj ) ;
         
         map< string, vector< InstalledNode > >         &_info ;
         string                                         _vCoordSvcName ;
   } ;

   class _omaRunRollbackCoordJob : public _omaCommand
   {
      public:
         _omaRunRollbackCoordJob ( string &vCoordSvcName, 
                                   map< string, vector<InstalledNode> > &info
                                 ) ;
         ~_omaRunRollbackCoordJob () ;

      public:
         virtual const CHAR* name () { return OMA_JOB_ROLLBACK_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         map< string, vector< InstalledNode > >         &_info ;
         string                                         _vCoordSvcName ;
   } ;

   class _omaRunRollbackCatalogJob : public _omaCommand
   {
      public:
         _omaRunRollbackCatalogJob ( string &vCoordSvcName, 
                                     map< string, vector<InstalledNode> > &info
                                   ) ;
         ~_omaRunRollbackCatalogJob () ;

      public:
         virtual const CHAR* name () { return OMA_JOB_ROLLBACK_CATALOG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         map< string, vector< InstalledNode > >         &_info ;
         string                                         _vCoordSvcName ;
   } ;

   class _omaRunRollbackDataNodeJob : public _omaCommand
   {
      public:
         _omaRunRollbackDataNodeJob ( string &vCoordSvcNamem,
                                      map< string, vector<InstalledNode> > &info
                                    ) ;
         ~_omaRunRollbackDataNodeJob () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_DATA_RG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         void _getInstalledDataGroupInfo( BSONObj& obj ) ;         

         map< string, vector< InstalledNode > >         &_info ;
         string                                         _vCoordSvcName ;
   } ;

   class _omaRmStandalone : public _omaCommand
   {
      public:
         _omaRmStandalone () ;
         virtual ~_omaRmStandalone () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_STANDALONE ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;
/*
      private:
         BSONObj                                        _uninstallInfo ;
*/
   } ;

   class _omaRmCataRG : public _omaCommand
   {
      public:
         _omaRmCataRG ( string &vCoordSvcName ) ;
         virtual ~_omaRmCataRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_CATA_RG ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;

      private:
/*
         BSONObj                                        _uninstallInfo ;
*/
         string                                         _vCoordSvcName ;
   } ;

   class _omaRmCoordRG : public _omaCommand
   {
      public:
         _omaRmCoordRG ( string &vCoordSvcName ) ;
         virtual ~_omaRmCoordRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_COORD_RG ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;

      private:
/*
         BSONObj                                        _uninstallInfo ;
*/
         string                                         _vCoordSvcName ;
   } ;

   class _omaRmDataRG : public _omaCommand
   {
      public:
         _omaRmDataRG ( string &vCoordSvcName ) ;
         virtual ~_omaRmDataRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_DATA_RG ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;

      private:
/*
         BSONObj                                        _uninstallInfo ;
*/
         string                                         _vCoordSvcName ;
   } ;


} // namespace engine


#endif // OMAGENT_COMMAND_HPP_
