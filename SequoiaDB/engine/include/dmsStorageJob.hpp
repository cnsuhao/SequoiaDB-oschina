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

   Source File Name = dmsStorageJob.hpp

   Descriptive Name = Data Management Service Header

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/10/2013  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DMS_STORAGE_JOB_HPP__
#define DMS_STORAGE_JOB_HPP__

#include "rtnBackgroundJob.hpp"

namespace engine
{

   class _dmsStorageBase ;

   /*
      _dmsExtendSegmentJob define
   */
   class _dmsExtendSegmentJob : public _rtnBaseJob
   {
      public:
         _dmsExtendSegmentJob ( _dmsStorageBase *pSUBase ) ;
         virtual ~_dmsExtendSegmentJob () ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR* name () const ;
         virtual BOOLEAN muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32 doit () ;

      private:
         _dmsStorageBase            *_pSUBase ;

   } ;
   typedef _dmsExtendSegmentJob  dmsExtendSegmentJob ;

   INT32 startExtendSegmentJob ( EDUID *pEDUID, _dmsStorageBase *pSUBase ) ;

}

#endif //DMS_STORAGE_JOB_HPP__

