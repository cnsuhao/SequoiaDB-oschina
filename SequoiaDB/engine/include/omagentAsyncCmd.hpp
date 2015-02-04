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

   Source File Name = omagentAsyncCmd.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_ASYNC_CMD_HPP_
#define OMAGENT_ASYNC_CMD_HPP_

#include "omagentCmdBase.hpp"

using namespace bson ;
using namespace std ;

namespace engine
{
   class _omaTaskMgr ;

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
         INT32 _getAddHostInfo( BSONObj &retObj1, BSONObj &retObj2 ) ;
         
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
         virtual const CHAR* name () { return OMA_CMD_CHECK_ADD_HOST_INFO ; }
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
         virtual const CHAR* name () { return OMA_JOB_ROLLBACK_STANDALONE ; }
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


#endif // OMAGENT_ASYNC_CMD_HPP_