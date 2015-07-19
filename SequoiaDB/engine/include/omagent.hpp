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

   Source File Name = omagent.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/30/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_HPP_
#define OMAGENT_HPP_

#include "core.hpp"
#include "../bson/bson.h"
#include "ossUtil.hpp"
#include "sptApi.hpp"
#include "omagentMsgDef.hpp"

using namespace std ;
using namespace bson ;

namespace engine
{

   /*
      add host
   */
   struct _AddHostCommon
   {
      string _sdbUser ;
      string _sdbPasswd ;
      string _userGroup ;
      string _installPacket ;
   } ;
   typedef struct _AddHostCommon AddHostCommon ;

   struct _AddHostItem
   {
      string _ip ;
      string _hostName ;
      string _user ;
      string _passwd ;
      string _sshPort ;
      string _agentService ;
      string _installPath ;
   } ;
   typedef struct _AddHostItem AddHostItem ;
   
   struct _AddHostResultInfo
   {
      string         _ip ;
      string         _hostName ;
      INT32          _status ;
      string         _statusDesc ;
      INT32          _errno ;
      string         _detail ;
      vector<string> _flow ;
   } ;
   typedef struct _AddHostResultInfo AddHostResultInfo ;
   typedef AddHostResultInfo RemoveHostResultInfo ;

   struct _AddHostInfo
   {
      INT32             _serialNum ;
      BOOLEAN           _flag ;   // whether the host has been handled or not
      INT64             _taskID ;
      AddHostCommon     _common ; // add host common field
      AddHostItem       _item ;   // add host info
   } ;
   typedef struct _AddHostInfo AddHostInfo ;

   /*
      remove host
   */
   struct _RemoveHostItem
   {
      string _ip ;
      string _hostName ;
      string _user ;
      string _passwd ;
      string _sshPort ;
      string _clusterName ;
      string _installPath ;
   } ;
   typedef struct _RemoveHostItem RemoveHostItem ;

   struct _RemoveHostInfo
   {
      INT32             _serialNum ;
      INT64             _taskID ;
      RemoveHostItem    _item ;
   } ;
   typedef struct _RemoveHostInfo RemoveHostInfo ;

   /*
      install db business host
   */
   struct _InstDBInfo
   {
      string _hostName ;
      string _svcName ;
      string _dbPath ;
      string _confPath ;
      string _dataGroupName ;
      string _sdbUser ;
      string _sdbPasswd ;
      string _sdbUserGroup ;
      string _user ;
      string _passwd ;
      string _sshPort ;
      BSONObj _conf ;
   } ;
   typedef struct _InstDBInfo InstDBInfo ;

   struct _InstDBResult
   {
      INT32          _errno ;
      string         _detail ;
      string         _hostName ;
      string         _svcName ;
      string         _role ;
      string         _groupName ;
      INT32          _status ;
      string         _statusDesc ;
      vector<string> _flow ;
   } ;
   typedef struct _InstDBResult InstDBResult ;
   typedef InstDBResult RemoveDBResult ;

   struct _InstDBBusInfo
   {
      INT32          _nodeSerialNum ;
      InstDBInfo     _instInfo ;
      InstDBResult   _instResult ;
   } ;
   typedef struct _InstDBBusInfo InstDBBusInfo ;


   /*
      remove db business
   */
   struct _RemoveDBInfo
   {
      string _hostName ;
      string _svcName ;
      string _role ;
      string _dataGroupName ;
      string _authUser ;
      string _authPasswd ;
   } ;
   typedef struct _RemoveDBInfo RemoveDBInfo ;

   struct _RemoveDBBusInfo
   {
      INT32            _nodeSerialNum ;
      RemoveDBInfo     _removeInfo ;
      RemoveDBResult   _removeResult ;
   } ;
   typedef _RemoveDBBusInfo RemoveDBBusInfo ;
   
   
}





#endif // OMAGENT_HPP_
