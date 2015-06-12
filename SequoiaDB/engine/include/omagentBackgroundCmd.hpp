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

   Source File Name = omagentBackgroundCmd.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_BACKGROUD_CMD_HPP_
#define OMAGENT_BACKGROUD_CMD_HPP_

#include "omagentCmdBase.hpp"

using namespace bson ;
using namespace std ;

namespace engine
{
   /*
      _omaAddHost
   */
   class _omaAddHost : public _omaCommand
   {
      public:
         _omaAddHost ( AddHostInfo &info ) ;
         ~_omaAddHost () ;
         virtual const CHAR * name () { return OMA_CMD_ADD_HOST ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         INT32 _getAddHostInfo( BSONObj &retObj1, BSONObj &retObj2 ) ;
         
      private:
         AddHostInfo   _addHostInfo ;
   } ;

   /*
      _omaCheckAddHostInfo
   */
   class _omaCheckAddHostInfo : public _omaCommand
   {
      public:
         _omaCheckAddHostInfo() ;
         ~_omaCheckAddHostInfo () ;
         
      public:
         virtual const CHAR* name () { return OMA_CMD_CHECK_ADD_HOST_INFO ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;
   } ;

   /*
      _omaCreateTmpCoord
   */
   class _omaCreateTmpCoord : public _omaCommand
   {
      public:
         _omaCreateTmpCoord( INT64 taskID ) ;
         ~_omaCreateTmpCoord() ;
         virtual const CHAR* name() { return OMA_CMD_INSTALL_TMP_COORD ; }
         virtual INT32 init( const CHAR *pInstallInfo ) ;

      public:
         INT32 createTmpCoord( BSONObj &cfgObj, BSONObj &retObj ) ;

      private:
         INT64   _taskID ;
   } ;

   /*
      _omaRemoveTmpCoord
   */
   class _omaRemoveTmpCoord : public _omaCommand
   {
      public:
         _omaRemoveTmpCoord( INT64 taskID, string &tmpCoordSvcName ) ;
         ~_omaRemoveTmpCoord() ;
         virtual const CHAR* name() { return OMA_CMD_REMOVE_TMP_COORD ; }
         virtual INT32 init( const CHAR *pInstallInfo ) ;
         
      public:
         INT32 removeTmpCoord( BSONObj &retObj ) ;

      private:
         INT64  _taskID ;
         string _tmpCoordSvcName ;
   } ;

   /*
      install standalone
   */
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

   /*
      install catalog
   */
   class _omaInstallCatalog : public _omaCommand
   {
      public:
         _omaInstallCatalog ( INT64 taskID, string &tmpCoordSvcName,
                              InstDBInfo &info ) ;
         virtual ~_omaInstallCatalog () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_INSTALL_CATALOG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         INT64         _taskID ;
         string        _tmpCoordSvcName ;
         InstDBInfo    _info ;
   } ;

   /*
      install coord
   */
   class _omaInstallCoord : public _omaCommand
   {
      public:
         _omaInstallCoord ( INT64 taskID, string &tmpCoordSvcName,
                            InstDBInfo &info ) ;
         virtual ~_omaInstallCoord () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_INSTALL_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         INT64         _taskID ;
         string        _tmpCoordSvcName ;
         InstDBInfo    _info ;

   } ;

   /*
      install data node
   */
   class _omaInstallDataNode : public _omaCommand
   {
      public:
         _omaInstallDataNode ( INT64 taskID, string tmpCoordSvcName,
                               InstDBInfo &info ) ;
         virtual ~_omaInstallDataNode () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_INSTALL_DATA_NODE ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         INT64         _taskID ;
         string        _tmpCoordSvcName ;
         InstDBInfo    _info ;
   } ;

   /*
      rollback standalone
   */
   class _omaRollbackStandalone : public _omaCommand
   {
      public:
         _omaRollbackStandalone( BSONObj &bus, BSONObj &sys ) ;
         ~_omaRollbackStandalone() ;

      public:
         virtual const CHAR* name() { return OMA_ROLLBACK_STANDALONE ; }
         virtual INT32 init( const CHAR *pInstallInfo ) ;

      private:
         BSONObj   _bus ;
         BSONObj   _sys ;
   } ;

   /*
      rollback catalog
   */
   class _omaRollbackCatalog : public _omaCommand
   {
      public:
         _omaRollbackCatalog ( INT64 taskID,
                               string &tmpCoordSvcName ) ;
         ~_omaRollbackCatalog () ;

      public:
         virtual const CHAR* name () { return OMA_ROLLBACK_CATALOG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         INT64    _taskID ;
         string   _tmpCoordSvcName ;
   } ;

   /*
      rollback coord
   */
   class _omaRollbackCoord : public _omaCommand
   {
      public:
         _omaRollbackCoord ( INT64 taskID,
                             string &tmpCoordSvcName ) ;
         ~_omaRollbackCoord () ;

      public:
         virtual const CHAR* name () { return OMA_ROLLBACK_COORD ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         INT64    _taskID ;
         string   _tmpCoordSvcName ;
   } ;

   /*
      rollback data groups
   */
   class _omaRollbackDataRG : public _omaCommand
   {
      public:
         _omaRollbackDataRG (  INT64 taskID,
                               string &tmpCoordSvcNamem,
                               set<string> &info ) ;
         ~_omaRollbackDataRG () ;

      public:
         virtual const CHAR* name () { return OMA_ROLLBACK_DATA_RG ; }
         virtual INT32 init ( const CHAR *pInstallInfo ) ;

      private:
         void _getInstalledDataGroupInfo( BSONObj &obj ) ;         

      private:
         INT64         _taskID ;
         string        _tmpCoordSvcName ;
         set<string>   &_info ;
   } ;

   /*
      remove standalone 
   */
   class _omaRmStandalone : public _omaCommand
   {
      public:
         _omaRmStandalone ( BSONObj &bus, BSONObj &sys ) ;
         virtual ~_omaRmStandalone () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_STANDALONE ; }
         virtual INT32 init ( const CHAR *pUninstallInfo ) ;

      private:
         BSONObj   _bus ;
         BSONObj   _sys ;
   } ;

   /*
      remove catalog group 
   */
   class _omaRmCataRG : public _omaCommand
   {
      public:
         _omaRmCataRG ( INT64 taskID, string &tmpCoordSvcName, BSONObj &info ) ;
         virtual ~_omaRmCataRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_CATA_RG ; }
         virtual INT32 init ( const CHAR *pInfo ) ;

      private:
         INT64     _taskID ;
         string    _tmpCoordSvcName ;
         BSONObj   _info ;
   } ;

   /*
      remove coord group 
   */
   class _omaRmCoordRG : public _omaCommand
   {
      public:
         _omaRmCoordRG ( INT64 taskID, string &tmpCoordSvcName, BSONObj &info ) ;
         virtual ~_omaRmCoordRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_COORD_RG ; }
         virtual INT32 init ( const CHAR *pInfo ) ;

      private:
         INT64     _taskID ;
         string    _tmpCoordSvcName ;
         BSONObj   _info ;
   } ;

   /*
      remove data group 
   */
   class _omaRmDataRG : public _omaCommand
   {
      public:
         _omaRmDataRG ( INT64 taskID,
                        string &tmpCoordSvcNamem,
                        BSONObj &info ) ;
         virtual ~_omaRmDataRG () ;

      public:
         virtual const CHAR* name () { return OMA_CMD_RM_DATA_RG ; }
         virtual INT32 init ( const CHAR *pInfo ) ;

      private:
         INT64     _taskID ;
         string    _tmpCoordSvcName ;
         BSONObj   _info ;
   } ;


} // namespace engine


#endif // OMAGENT_BACKGROUD_CMD_HPP_