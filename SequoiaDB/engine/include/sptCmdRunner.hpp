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

   Source File Name = sptCmdRunner.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_CMDRUNNER_HPP_
#define SPT_CMDRUNNER_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "ossNPipe.hpp"
#include "ossProc.hpp"
#include "ossEvent.hpp"

#include <vector>
#include <map>
#include <string>

using namespace std ;

namespace engine
{
   class _sptCmdRunner : public SDBObject, public ossIExecHandle
   {
   public:
      _sptCmdRunner() ;
      virtual ~_sptCmdRunner() ;

   public:
      /*
         timeout is ms, ignore when isBackground = TRUE
      */
      INT32 exec( const CHAR *cmd, UINT32 &exit,
                  BOOLEAN isBackground = FALSE,
                  INT64 timeout = -1 ) ;

      INT32 done() ;

      INT32 read( string &out, BOOLEAN readEOF = TRUE ) ;

      OSSPID getPID() const { return _id ; }

   protected:
      virtual void  handleInOutPipe( OSSPID pid,
                                     OSSNPIPE * const npHandleStdin,
                                     OSSNPIPE * const npHandleStdout ) ;

      void  asyncRead() ;
      void  monitor() ;

      INT32 _readOut( string &out, BOOLEAN readEOF = TRUE ) ;

   private:
      OSSNPIPE       _out ;
      OSSPID         _id ;
      ossEvent       _event ;
      ossEvent       _monitorEvent ;
      BOOLEAN        _hasRead ;
      string         _outStr ;
      INT32          _readResult ;
      INT64          _timeout ;

   } ;
   typedef class _sptCmdRunner sptCmdRunner ;
}

#endif

