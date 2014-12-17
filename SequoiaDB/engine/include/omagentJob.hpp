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

   Source File Name = omagentJob.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_JOB_HPP_
#define OMAGENT_JOB_HPP_
#include "core.hpp"
#include "oss.hpp"
#include "ossUtil.hpp"
#include "pmd.hpp"
#include "omagent.hpp"
#include "omagentTask.hpp"
#include "omagentCommand.hpp"
#include "rtnBackgroundJob.hpp"
#include <string>
#include <vector>

using namespace bson ;

namespace engine
{
   class _omaInsDBBusTask ;

   /*
      add host job
   */
   class _omaAddHostJob : public _rtnBaseJob
   {
      public:
         _omaAddHostJob ( string jobName, _omaAddHostTask *pTask ) ;
         virtual ~_omaAddHostJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;

     private:
         string                   _jobName ;
         _omaAddHostTask*         _pTask ;
   } ; 


   /*
      rollback host job
   */
   class _omaRbHostJob : public _rtnBaseJob
   {
      public:
         _omaRbHostJob ( string jobName, _omaAddHostTask *pTask ) ;
         virtual ~_omaRbHostJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;

     private:
         string                             _jobName ;
         _omaAddHostTask*                   _pTask ;
   } ; 

   /*
      start add host task job
      it's just a backgroud thread to arrange
      a backgroud task to do the actual add host job
   */
   class _omaStartAddHostTaskJob : public _rtnBaseJob
   {
      public:
         _omaStartAddHostTaskJob(BSONObj& addHostInfo) ;
         virtual ~_omaStartAddHostTaskJob() ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;
         INT32                init() ;
         INT32                init2() ;

      private:
         INT32 _initAddHostsInfo( BSONObj &info ) ;

      private:
         // raw add host info
         BSONObj                     _addHostInfoObj ;
         // job name
         string                      _jobName ;
         // add host info
         vector<AddHostInfo>         _addHostInfo ;
         // task info
         INT64                       _taskID ;
         _omaAddHostTask*            _pTask ;
   } ;
 
   /*
      create standalone job
   */
   class _omaCreateStandaloneJob : public _rtnBaseJob
   {
      public:
         _omaCreateStandaloneJob ( _omaInsDBBusTask *pTask ) ;
         virtual ~_omaCreateStandaloneJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;

         OMA_JOB_STATUS getJobStatus ()
         {
            return _status ;
         }
         void setJobStatus ( OMA_JOB_STATUS status )
         {
            _status = status ;
            _pTask->updateInstallJobStatus( _name, status ) ;
         }
      private:

         INT32 _getInstallInfo( BSONObj &obj, InstallInfo &installInfo ) ;
         INT32 _updateInstallStatus( BOOLEAN isFinish,
                                     INT32 retRc,
                                     const CHAR *pErrMsg,
                                     const CHAR *pDesc,
                                     InstalledNode *pNode ) ;
      private:
         OMA_JOB_STATUS                      _status ;
         string                              _name ;
         _omaInsDBBusTask*                   _pTask ;
   } ;

   /*
      create catalog job
   */
   class _omaCreateCatalogJob : public _rtnBaseJob
   {
      public:
         _omaCreateCatalogJob ( _omaInsDBBusTask *pTask ) ;
         virtual ~_omaCreateCatalogJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;

         OMA_JOB_STATUS getJobStatus ()
         {
            return _status ;
         }
         void setJobStatus ( OMA_JOB_STATUS status )
         {
            _status = status ;
            _pTask->updateInstallJobStatus( _name, status ) ;
         }
      private:

         INT32 _getInstallInfo( BSONObj &obj, InstallInfo &installInfo ) ;
         INT32 _updateInstallStatus( BOOLEAN isFinish,
                                     INT32 retRc,
                                     const CHAR *pErrMsg,
                                     const CHAR *pDesc,
                                     InstalledNode *pNode ) ;
      private:
         OMA_JOB_STATUS                      _status ;
         string                              _name ;
         _omaInsDBBusTask*                   _pTask ;
   } ;

   /*
      create coord job
   */
   class _omaCreateCoordJob : public _rtnBaseJob
   {
      public:
         _omaCreateCoordJob ( _omaInsDBBusTask *pTask ) ;
         virtual ~_omaCreateCoordJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;

         OMA_JOB_STATUS getJobStatus ()
         {
            return _status ;
         }
         void setJobStatus ( OMA_JOB_STATUS status )
         {
            _status = status ;
            _pTask->updateInstallJobStatus( _name, status ) ;
         }
      private:

         INT32 _getInstallInfo( BSONObj &obj, InstallInfo &installInfo ) ;
         INT32 _updateInstallStatus( BOOLEAN isFinish,
                                     INT32 retRc,
                                     const CHAR *pErrMsg,
                                     const CHAR *pDesc,
                                     InstalledNode *pNode ) ;

      private:
         OMA_JOB_STATUS                      _status ;
         string                              _name ;
         _omaInsDBBusTask*                   _pTask ;
   } ;

   /*
      create data job
   */
   class _omaCreateDataJob : public _rtnBaseJob
   {
      public:
         _omaCreateDataJob ( const CHAR *pGroupName,
                             _omaInsDBBusTask *pTask ) ;
         virtual ~_omaCreateDataJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;

         OMA_JOB_STATUS getJobStatus ()
         {
            return _status ;
         }
         void setJobStatus ( OMA_JOB_STATUS status )
         {
            _status = status ;
            _pTask->updateInstallJobStatus( _name, status ) ;
         }
      private:

         INT32 _getInstallInfo( BSONObj &obj, InstallInfo &installInfo ) ;
         INT32 _updateInstallStatus( BOOLEAN isFinish,
                                     INT32 retRc,
                                     const CHAR *pErrMsg,
                                     const CHAR *pDesc,
                                     InstalledNode *pNode ) ;
     private:
         string                              _groupname ;
         OMA_JOB_STATUS                      _status ;
         string                              _name ;
         _omaInsDBBusTask*                   _pTask ;
   } ;

   /*
      start install db business task job
   */
   class _omaStartInsDBBusTaskJob : public _rtnBaseJob
   {
      public:
         _omaStartInsDBBusTaskJob ( BSONObj &installInfo ) ;
         virtual ~_omaStartInsDBBusTaskJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;
         INT32                init() ;
         INT32                init2() ;

      private:
         BOOLEAN                     _isStandalone ;
         // raw install info
         BSONObj                     _installInfoObj ;
         // install info after category
         vector<BSONObj>             _coord ;
         vector<BSONObj>             _catalog ;
         vector<BSONObj>             _data ;
         vector<BSONObj>             _standalone ;
         // task info
         INT64                       _taskID ;
         string                      _name ;
         _omaInsDBBusTask*           _pTask ;
   } ;

   /*
      start remove db business task job
   */
   class _omaStartRmDBBusTaskJob : public _rtnBaseJob
   {
      public:
         _omaStartRmDBBusTaskJob ( BSONObj &uninstallInfo ) ;
         virtual ~_omaStartRmDBBusTaskJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;
         INT32                init() ;
         INT32                init2() ;

      private:
         BOOLEAN                     _isStandalone ;
         // raw uninstall info
         BSONObj                     _uninstallInfoObj ;
         BSONObj                     _cataAddrInfo ;
         // uninstall info after category
         map<string, BSONObj>        _coord ;
         map<string, BSONObj>        _catalog ;
         map<string, BSONObj>        _data ;
         map<string, BSONObj>        _standalone ;
         // task info
         INT64                       _taskID ;
         string                      _name ;
         _omaRmDBBusTask*            _pTask ;
   } ;

   /*
      install db business task rollback internal job
   */
   class _omaInsDBBusTaskRbJob : public _rtnBaseJob
   {
      public:
         _omaInsDBBusTaskRbJob ( BOOLEAN isStandalone,
                                 string &vCoordSvcName,
                                 _omaInsDBBusTask *pTask ) ;
         virtual ~_omaInsDBBusTaskRbJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;
 
      private:
         INT32 _getRollbackInfo ( RollbackInfo &info ) ;
         INT32 _rollbackStandalone( string &vCoordSvcName,
                                    map< string, vector<InstalledNode> > &info ) ;
         INT32 _rollbackCoord( string &vCoordSvcName,
                               map< string, vector<InstalledNode> > &info ) ;
         INT32 _rollbackCatalog( string &vCoordSvcName,
                                 map< string, vector<InstalledNode> > &info ) ;
         INT32 _rollbackDataNode( string &vCoordSvcName, 
                                  map< string,vector<InstalledNode> > &info ) ;

         BOOLEAN                             _isStandalone ;
         string                              _vCoordSvcName ;
         string                              _name ;
         _omaInsDBBusTask*                   _pTask ;
   } ;

   /*
      create remove virtual coord job
   */
   class _omaRemoveVirtualCoordJob : public _rtnBaseJob
   {
      public:
         _omaRemoveVirtualCoordJob ( const CHAR *vCoordSvcName,
                                     _omaInsDBBusTask *pTask ) ;
         virtual ~_omaRemoveVirtualCoordJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR*  name () const ;
         virtual BOOLEAN      muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32        doit () ;

      private:
         string                      _vCoordSvcName ;
         _omaInsDBBusTask            *_pTask ;
   } ;


   // start add host job
   INT32 startAddHostJob( string jobName, _omaAddHostTask *pTask, EDUID *pEDUID ) ;

   // start rollback host job
   INT32 startRbHostJob( string jobName, _omaAddHostTask *pTask, EDUID *pEDUID ) ;

   // start add host task job
   INT32 startAddHostTaskJob ( const CHAR *pAddHostInfo, EDUID *pEDUID ) ;

   // start create standalone job
   INT32 startCreateStandaloneJob ( _omaInsDBBusTask *pTask,
                                    EDUID *pEDUID ) ;
   // start create catalog job
   INT32 startCreateCatalogJob ( _omaInsDBBusTask *pTask,
                                 EDUID *pEDUID ) ;
   // start create coord job
   INT32 startCreateCoordJob ( _omaInsDBBusTask *pTask,
                               EDUID *pEDUID ) ;
   // start create data job
   INT32 startCreateDataJob ( const CHAR *pGroupName,
                              _omaInsDBBusTask *pTask,
                              EDUID *pEDUID ) ;
   // start install db business task job
   INT32 startInsDBBusTaskJob ( const CHAR *pInstallInfo, EDUID *pEDUID ) ;
   // start remove db business task job
   INT32 startRmDBBusTaskJob ( const CHAR *pUninstallInfo, EDUID *pEDUID ) ;
   // start install db business task rollback job
   INT32 startInsDBBusTaskRbJob ( BOOLEAN isStandalone,
                                  string &vCoordSvcName,
                                  _omaInsDBBusTask *pTask,
                                  EDUID *pEDUID ) ;
   // start create remove virtual coord job
   INT32 startRemoveVirtualCoordJob ( const CHAR *vCoordSvcName,
                                      _omaInsDBBusTask *pTask,
                                      EDUID *pEDUID ) ;

}



#endif