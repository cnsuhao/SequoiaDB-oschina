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


   class _omaCreateTmpCoord : public _omaCommand
   {
      public:
         _omaCreateTmpCoord () ;
         ~_omaCreateTmpCoord () ;
         virtual const CHAR* name () { return OMA_CMD_CRRATE_TMP_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      public:
         INT32 createTmpCoord ( BSONObj &retObj ) ;
   } ;

   class _omaRemoveTmpCoord : public _omaCommand
   {
      public:
         _omaRemoveTmpCoord ( const CHAR *tmpCoordSvcName ) ;
         ~_omaRemoveTmpCoord () ;
         virtual const CHAR* name () { return OMA_CMD_REMOVE_TMP_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
         
      public:
         INT32 removeTmpCoord ( BSONObj &retObj ) ;

      private:
         CHAR _tmpCoordSvcName[OSS_MAX_SERVICENAME + 1] ;
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

   class _omaInstallStandalone : public _omaCommand
   {
      public:
         _omaInstallStandalone ( INT64 taskID, InstDBInfo &info ) ;
         virtual ~_omaInstallStandalone () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_INSTALL_STANDALONE ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         INT64           _taskID ;
         InstDBInfo      _info ;
   } ;

   class _omaInstallCatalog : public _omaCommand
   {
      public:
         _omaInstallCatalog ( string &tmpCoordSvcName, InstDBInfo &info ) ;
         virtual ~_omaInstallCatalog () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_INSTALL_CATALOG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         BSONObj       _installInfo ;
         InstDBInfo    _info ;
         string        _tmpCoordSvcName ;
   } ;

   class _omaInstallCoord : public _omaCommand
   {
      public:
         _omaInstallCoord ( string &tmpCoordSvcName, InstDBInfo &info ) ;
         virtual ~_omaInstallCoord () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_INSTALL_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         BSONObj       _installInfo ;
         InstDBInfo    _info ;
         string        _tmpCoordSvcName ;
   } ;

   class _omaInstallDataNode : public _omaCommand
   {
      public:
         _omaInstallDataNode ( string &tmpCoordSvcName, InstDBInfo &info ) ;
         virtual ~_omaInstallDataNode () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_INSTALL_DATA_NODE ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         BSONObj       _installInfo ;
         InstDBInfo    _info ;
         string        _tmpCoordSvcName ;
   } ;

   class _omaRollbackStandalone : public _omaCommand
   {
      public:
         _omaRollbackStandalone( BSONObj &bus,
                                 BSONObj &sys,
                                 INT64 taskID ) ;
         ~_omaRollbackStandalone() ;

      public:
         virtual const CHAR* name() { return OMA_ROLLBACK_STANDALONE ; }
         virtual INT32 init( const CHAR *pInstallInfo ) ;

      private:
         BSONObj _bus ;
         BSONObj _sys ;
         INT64   _taskID ;
   } ;

   class _omaRunRollbackCoordJob : public _omaCommand
   {
      public:
         _omaRunRollbackCoordJob ( string &tmpCoordSvcName, 
                                   map< string, vector<InstalledNode> > &info
                                 ) ;
         ~_omaRunRollbackCoordJob () ;

      public:
         virtual const CHAR* name () { return OMA_ROLLBACK_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         map< string, vector< InstalledNode > >         &_info ;
         string                                         _tmpCoordSvcName ;
   } ;

   class _omaRunRollbackCatalogJob : public _omaCommand
   {
      public:
         _omaRunRollbackCatalogJob ( string &tmpCoordSvcName, 
                                     map< string, vector<InstalledNode> > &info
                                   ) ;
         ~_omaRunRollbackCatalogJob () ;

      public:
         virtual const CHAR* name () { return OMA_ROLLBACK_CATALOG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         map< string, vector< InstalledNode > >         &_info ;
         string                                         _tmpCoordSvcName ;
   } ;

   class _omaRunRollbackDataNodeJob : public _omaCommand
   {
      public:
         _omaRunRollbackDataNodeJob ( string &tmpCoordSvcNamem,
                                      map< string, vector<InstalledNode> > &info
                                    ) ;
         ~_omaRunRollbackDataNodeJob () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_DATA_RG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         void _getInstalledDataGroupInfo( BSONObj& obj ) ;         

         map< string, vector< InstalledNode > >         &_info ;
         string                                         _tmpCoordSvcName ;
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
         _omaRmCataRG ( string &tmpCoordSvcName ) ;
         virtual ~_omaRmCataRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_CATA_RG ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;

      private:
/*
         BSONObj                                        _uninstallInfo ;
*/
         string                                         _tmpCoordSvcName ;
   } ;

   class _omaRmCoordRG : public _omaCommand
   {
      public:
         _omaRmCoordRG ( string &tmpCoordSvcName ) ;
         virtual ~_omaRmCoordRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_COORD_RG ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;

      private:
/*
         BSONObj                                        _uninstallInfo ;
*/
         string                                         _tmpCoordSvcName ;
   } ;

   class _omaRmDataRG : public _omaCommand
   {
      public:
         _omaRmDataRG ( string &tmpCoordSvcName ) ;
         virtual ~_omaRmDataRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_DATA_RG ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;

      private:
/*
         BSONObj                                        _uninstallInfo ;
*/
         string                                         _tmpCoordSvcName ;
   } ;


} // namespace engine


#endif // OMAGENT_ASYNC_CMD_HPP_