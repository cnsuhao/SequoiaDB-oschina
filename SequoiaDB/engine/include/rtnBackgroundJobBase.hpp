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

   Source File Name = rtnBackgroundJobBase.hpp

   Descriptive Name = Data Management Service Header

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          03/06/2013  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_BACKGROUND_JOB_BASE_HPP_
#define RTN_BACKGROUND_JOB_BASE_HPP_

#include "ossLatch.hpp"
#include "pmdEDUMgr.hpp"
#include <map>

namespace engine
{
   class _rtnBaseJob ;

   enum RTN_JOB_TYPE
   {
      RTN_JOB_CREATE_INDEX       = 1,
      RTN_JOB_DROP_INDEX         = 2,
      RTN_JOB_CLEANUP            = 3,
      RTN_JOB_LOAD               = 4,
      RTN_JOB_PREFETCH           = 5,
      RTN_JOB_EXTENDSEGMENT      = 6,
      RTN_JOB_RESTORE            = 7,
      RTN_JOB_REPLSYNC           = 8,
      RTN_JOB_PAGECLEANER        = 9,
      RTN_JOB_ADDHOST            = 10, // add host
      RTN_JOB_RMHOST             = 11, // remove host
      RTN_JOB_CREATESTANDALONE   = 12, // create standalone
      RTN_JOB_CREATECATALOG      = 13, // create catalog
      RTN_JOB_CREATECOORD        = 14, // create coord
      RTN_JOB_CREATEDATA         = 15, // create data node
      RTN_JOB_STARTNODE          = 16, // start node
      RTN_JOB_CMSYNC             = 17, // cm and cmd sync info
      RTN_JOB_REMOVEVIRTUALCOORD = 18, // remove virtual coord
      RTN_JOB_STARTINSDBBUSTASK  = 19, // start install db business task
      RTN_JOB_STARTRMDBBUSTASK   = 20, // start remove db business task
      RTN_JOB_INSDBBUSTASKRB     = 21, // install db business task rollback
      RTN_JOB_STARTADDHOSTTASK   = 22, // start add host task

      RTN_JOB_OMAGENT            = 23,  // omagent job

      RTN_JOB_MAX
   } ;

   enum RTN_JOB_MUTEX_TYPE
   {
      RTN_JOB_MUTEX_NONE      = 0,     // not check mutex
      RTN_JOB_MUTEX_RET       = 1,     // when mutex, return self
      RTN_JOB_MUTEX_STOP_RET  = 2,     // when mutex, stop peer and return self
      RTN_JOB_MUTEX_STOP_CONT = 3,     // when mutex, stop peer and continue self
      RTN_JOB_MUTEX_REUSE     = 4      // when mutex, reuse peer
   } ;

   class _rtnJobMgr : public SDBObject
   {
      friend INT32 pmdBackgroundJobEntryPoint ( pmdEDUCB *cb, void *pData ) ;

      public:
         _rtnJobMgr ( pmdEDUMgr * eduMgr ) ;
         ~_rtnJobMgr () ;

      public:
         UINT32 jobsCount () ;
         _rtnBaseJob* findJob ( EDUID eduID, INT32 *pResult = NULL ) ;

         INT32 startJob ( _rtnBaseJob *pJob,
                          RTN_JOB_MUTEX_TYPE type = RTN_JOB_MUTEX_STOP_CONT ,
                          EDUID *pEDUID = NULL,
                          BOOLEAN returnResult = FALSE ) ;

      protected:
         INT32 _stopJob ( EDUID eduID ) ;
         INT32 _removeJob ( EDUID eduID, INT32 result = SDB_OK ) ;

      private:
         std::map<EDUID, _rtnBaseJob*>        _mapJobs ;
         std::map<EDUID, INT32>               _mapResult ;
         ossSpinSLatch                        _latch ;
         ossSpinSLatch                        _latchRemove ;
         pmdEDUMgr                            *_eduMgr ;
   } ;
   typedef _rtnJobMgr rtnJobMgr ;

   rtnJobMgr* rtnGetJobMgr () ;

   class _rtnBaseJob : public SDBObject
   {
      friend INT32 pmdBackgroundJobEntryPoint ( pmdEDUCB *cb, void *pData ) ;

      protected:
         INT32 attachIn ( pmdEDUCB *cb ) ;
         INT32 attachOut () ;

      public:
         _rtnBaseJob () ;
         virtual ~_rtnBaseJob () ;

         INT32 waitAttach () ;
         INT32 waitDetach () ;

         pmdEDUCB* eduCB() ;

      public:
         virtual RTN_JOB_TYPE type () const = 0 ;
         virtual const CHAR* name () const = 0 ;
         virtual BOOLEAN muteXOn ( const _rtnBaseJob *pOther ) = 0 ;
         virtual INT32 doit () = 0 ;

      private:
         ossSpinXLatch        _latchIn ;
         ossSpinXLatch        _latchOut ;
      protected:
         pmdEDUCB*            _pEDUCB ;

   } ;
   typedef _rtnBaseJob rtnBaseJob ;

}

#endif //RTN_BACKGROUND_JOB_BASE_HPP_

