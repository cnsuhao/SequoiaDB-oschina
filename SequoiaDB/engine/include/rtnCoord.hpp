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

   Source File Name = rtnCoord.hpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/28/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTNCOORD_HPP__
#define RTNCOORD_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "pmdEDU.hpp"
#include "msg.hpp"
#include "netMultiRouteAgent.hpp"
#include <map>

namespace engine
{
   #define RTN_COORD_RSP_WAIT_TIME        1000 //1s
   #define RTN_COORD_RSP_WAIT_TIME_QUICK  10 //10ms
   #define RTN_COORD_GETMORE_MAX_SIZE     (16*1024)   //16KB
   #define RTN_FST_CMD_LIST               "list"
   #define RTN_FST_CMD_LIST_LEN           4
   #define RTN_SEC_CMD_CONTEXTS           "contexts"
   #define RTN_SEC_CMD_CONTEXTS_LEN       8
   #define RTN_SEC_CMD_GROUPS             "groups"
   #define RTN_SEC_CMD_GROUPS_LEN         6
   #define RTN_CMD_LIST_GROUPS            "$"RTN_FST_CMD_LIST" "RTN_SEC_CMD_GROUPS
   #define RTN_CMD_LIST_GROUPS_LEN        (1+RTN_FST_CMD_LIST_LEN+1+RTN_SEC_CMD_GROUPS_LEN)

   class rtnCoordCommand;
   class rtnCoordOperator;

   #define RTN_COORD_CMD_BEGIN  void rtnCoordProcesserFactory::addCommand(){
   #define RTN_COORD_CMD_END     }
   #define RTN_COORD_CMD_ADD( cmdName, cmdClass )  {\
       rtnCoordCommand *pObj = SDB_OSS_NEW cmdClass();\
       _cmdMap.insert ( COORD_CMD_MAP::value_type (cmdName, pObj ));}

   #define RTN_COORD_OP_BEGIN    void rtnCoordProcesserFactory::addOperator(){
   #define RTN_COORD_OP_END      }
   #define RTN_COORD_OP_ADD( opCode, opClass )  {\
      rtnCoordOperator *pObj = SDB_OSS_NEW opClass();\
      _opMap.insert ( COORD_OP_MAP::value_type ( opCode, pObj )); }

   class rtnCoordProcesserFactory : public SDBObject
   {
   typedef std::map<std::string, rtnCoordCommand *> COORD_CMD_MAP;
   typedef std::map<SINT32, rtnCoordOperator *> COORD_OP_MAP;
   public:
      rtnCoordProcesserFactory();
      ~rtnCoordProcesserFactory();
      rtnCoordCommand *getCommandProcesser(const MsgOpQuery *pQuery);
      rtnCoordCommand *getCommandProcesser(const char *pCmd);
      rtnCoordOperator *getOperator( SINT32 opCode );
   private:
      INT32 parseCommand(const CHAR *pMsg);
      void addCommand();
      void addOperator();
   private:
      COORD_CMD_MAP        _cmdMap;
      COORD_OP_MAP         _opMap;
   };

}

#endif
