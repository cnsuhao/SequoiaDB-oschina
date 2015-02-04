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

   Source File Name = omagentSubTask.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENTSUBTASK_HPP_
#define OMAGENTSUBTASK_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossUtil.hpp"
#include "ossLatch.hpp"
#include "../bson/bson.h"
#include "omagent.hpp"
#include "omagentTaskBase.hpp"
#include "omagentTask.hpp"
#include <map>
#include <vector>
#include <string>

using namespace std ;
using namespace bson ;

#define OMA_TASK_NAME_ADD_HOST_SUB   "add host sub task"

namespace engine
{
   /*
      add host sub task
   */
   class _omaAddHostSubTask : public _omaTask
   {
      public:
         _omaAddHostSubTask ( INT64 taskID ) ;
         virtual ~_omaAddHostSubTask () ;

/*
      public:
         virtual OMA_TASK_TYPE taskType () const { return _taskType ; }
         virtual const CHAR*   taskName () const { return _taskName.c_str() ; }
*/
      public:
         INT32 init( const BSONObj &info, void *ptr = NULL ) ;
         INT32 doit() ;

/*
      public:
         void setTaskStatus( OMA_TASK_STATUS status ) ;
*/

      private:

         _omaAddHostTask    *_pTask ;
         

   } ;
   typedef _omaAddHostSubTask omaAddHostSubTask ;
   
}


#endif  //  OMAGENTSUBTASK_HPP_