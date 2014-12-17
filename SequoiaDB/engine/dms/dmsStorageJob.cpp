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

   Source File Name = dmsStorageJob.cpp

   Descriptive Name = Data Management Service Header

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/10/2013  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "dmsStorageJob.hpp"
#include "dmsStorageBase.hpp"

namespace engine
{

   /*
      _dmsExtendSegmentJob implement
   */

   _dmsExtendSegmentJob::_dmsExtendSegmentJob( dmsStorageBase * pSUBase )
   {
      SDB_ASSERT( pSUBase, "Storage base unit can't be NULL" ) ;
      _pSUBase = pSUBase ;
   }

   _dmsExtendSegmentJob::~_dmsExtendSegmentJob()
   {
      _pSUBase = NULL ;
   }

   RTN_JOB_TYPE _dmsExtendSegmentJob::type() const
   {
      return RTN_JOB_EXTENDSEGMENT ;
   }

   const CHAR* _dmsExtendSegmentJob::name () const
   {
      return "Job[ExtendSegment]" ;
   }

   BOOLEAN _dmsExtendSegmentJob::muteXOn( const _rtnBaseJob * pOther )
   {
      return FALSE ;
   }

   INT32 _dmsExtendSegmentJob::doit ()
   {
      return _pSUBase->_preExtendSegment() ;
   }

   INT32 startExtendSegmentJob( EDUID * pEDUID, _dmsStorageBase * pSUBase )
   {
      INT32 rc                      = SDB_OK ;
      dmsExtendSegmentJob * pJob    = NULL ;

      pJob = SDB_OSS_NEW dmsExtendSegmentJob ( pSUBase ) ;
      if ( !pJob )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "Allocate failed" ) ;
         goto error ;
      }
      rc = rtnGetJobMgr()->startJob( pJob, RTN_JOB_MUTEX_NONE, pEDUID ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

}


