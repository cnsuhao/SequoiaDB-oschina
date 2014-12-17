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

   Source File Name = rtnPageCleanerJob.hpp

   Descriptive Name = Page cleaner header. Page cleaner is a type of background
                      job that flush storage unit in period of time.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/04/2014  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_PAGECLEANER_JOB_HPP_
#define RTN_PAGECLEANER_JOB_HPP_

#include "rtnBackgroundJob.hpp"
#include "../bson/bsonobj.h"

using namespace bson ;

namespace engine
{
   /*
    * _rtnPageCleanerJob define
    */
   class _rtnPageCleanerJob : public _rtnBaseJob
   {
   public :
      _rtnPageCleanerJob ( INT32 periodTime = OSS_ONE_SEC ) ;
      virtual ~_rtnPageCleanerJob () ;
   public :
      virtual RTN_JOB_TYPE type () const ;
      virtual const CHAR* name() const ;
      virtual BOOLEAN muteXOn ( const _rtnBaseJob *pOther ) ;
      virtual INT32 doit () ;
   private :
      INT32   _periodTime ;
   } ;
   typedef _rtnPageCleanerJob rtnPageCleanerJob ;

   INT32 startPageCleanerJob ( EDUID *pEDUID,
                               INT32 periodTime = OSS_ONE_SEC ) ;
}

#endif // RTN_PAGECLEANER_JOB_HPP_
