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

   Source File Name = sptUsrSystem.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_USRSYSTEM_HPP_
#define SPT_USRSYSTEM_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "sptApi.hpp"

#define SPT_USR_SYSTEM_DISTRIBUTOR        "Distributor"
#define SPT_USR_SYSTEM_RELASE             "Release"
#define SPT_USR_SYSTEM_DESP               "Description"
#define SPT_USR_SYSTEM_BIT                "Bit"
#define SPT_USR_SYSTEM_IP                 "Ip"
#define SPT_USR_SYSTEM_HOSTS              "Hosts"
#define SPT_USR_SYSTEM_HOSTNAME           "HostName"
#define SPT_USR_SYSTEM_FILESYSTEM         "Filesystem"
#define SPT_USR_SYSTEM_CORE               "Core"
#define SPT_USR_SYSTEM_INFO               "Info"
#define SPT_USR_SYSTEM_FREQ               "Freq"
#define SPT_USR_SYSTEM_CPUS               "Cpus"
#define SPT_USR_SYSTEM_USER               "User"
#define SPT_USR_SYSTEM_SYS                "Sys"
#define SPT_USR_SYSTEM_IDLE               "Idle"
#define SPT_USR_SYSTEM_OTHER              "Other"
#define SPT_USR_SYSTEM_SIZE               "Size"
#define SPT_USR_SYSTEM_FREE               "Free"
#define SPT_USR_SYSTEM_USED               "Used"
#define SPT_USR_SYSTEM_ISLOCAL            "IsLocal"
#define SPT_USR_SYSTEM_MOUNT              "Mount"
#define SPT_USR_SYSTEM_DISKS              "Disks"
#define SPT_USR_SYSTEM_NAME               "Name"
#define SPT_USR_SYSTEM_NETCARDS           "Netcards"
#define SPT_USR_SYSTEM_TARGET             "Target"
#define SPT_USR_SYSTEM_REACHABLE          "Reachable"
#define SPT_USR_SYSTEM_USABLE             "Usable"
#define SPT_USR_SYSTEM_UNIT               "Unit"
#define SPT_USR_SYSTEM_FSTYPE             "FsType"

#define SPT_USR_SYSTEM_RX_PACKETS         "RXPackets"
#define SPT_USR_SYSTEM_RX_BYTES           "RXBytes"
#define SPT_USR_SYSTEM_RX_ERRORS          "RXErrors"
#define SPT_USR_SYSTEM_RX_DROPS           "RXDrops"

#define SPT_USR_SYSTEM_TX_PACKETS         "TXPackets"
#define SPT_USR_SYSTEM_TX_BYTES           "TXBytes"
#define SPT_USR_SYSTEM_TX_ERRORS          "TXErrors"
#define SPT_USR_SYSTEM_TX_DROPS           "TXDrops"

#define SPT_USR_SYSTEM_CALENDAR_TIME      "CalendarTime"

namespace engine
{
   enum SPT_HOST_LINE_TYPE
   {
      LINE_HOST         = 1,
      LINE_UNKNONW,
   } ;

   struct _sptHostItem
   {
      INT32    _lineType ;
      string   _ip ;
      string   _com ;
      string   _host ;

      _sptHostItem()
      {
         _lineType = LINE_UNKNONW ;
      }

      string toString() const
      {
         if ( LINE_UNKNONW == _lineType )
         {
            return _ip ;
         }
         string space = "    " ;
         if ( _com.empty() )
         {
            return _ip + space + _host ;
         }
         return _ip + space + _com + space + _host ;
      }
   } ;
   typedef _sptHostItem sptHostItem ;

   typedef vector< sptHostItem >    VEC_HOST_ITEM ;

   /*
      _sptUsrSystem define
   */
   class _sptUsrSystem : public SDBObject
   {
   JS_DECLARE_CLASS( _sptUsrSystem )

   public:
      _sptUsrSystem() ;
      virtual ~_sptUsrSystem() ;

   public:
      static INT32 ping( const _sptArguments &arg,
                         _sptReturnVal &rval,
                         bson::BSONObj &detail ) ;

      static INT32 type( const _sptArguments &arg,
                         _sptReturnVal &rval,
                         bson::BSONObj &detail ) ;

      static INT32 getReleaseInfo( const _sptArguments &arg,
                                   _sptReturnVal &rval,
                                   bson::BSONObj &detail ) ;

      static INT32 getHostsMap( const _sptArguments &arg,
                                _sptReturnVal &rval,
                                bson::BSONObj &detail ) ;

      static INT32 getAHostMap( const _sptArguments &arg,
                                _sptReturnVal &rval,
                                bson::BSONObj &detail ) ;

      static INT32 addAHostMap( const _sptArguments &arg,
                                _sptReturnVal &rval,
                                bson::BSONObj &detail ) ;

      static INT32 delAHostMap( const _sptArguments &arg,
                                _sptReturnVal &rval,
                                bson::BSONObj &detail ) ;

      static INT32 getCpuInfo( const _sptArguments &arg,
                               _sptReturnVal &rval,
                               bson::BSONObj &detail ) ;

      static INT32 snapshotCpuInfo( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail ) ;

      static INT32 getMemInfo( const _sptArguments &arg,
                               _sptReturnVal &rval,
                               bson::BSONObj &detail ) ;

      static INT32 snapshotMemInfo( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail ) ;

      static INT32 getDiskInfo( const _sptArguments &arg,
                                _sptReturnVal &rval,
                                bson::BSONObj &detail ) ;

      static INT32 snapshotDiskInfo( const _sptArguments &arg,
                                     _sptReturnVal &rval,
                                     bson::BSONObj &detail ) ;

      static INT32 getNetcardInfo( const _sptArguments &arg,
                                   _sptReturnVal &rval,
                                   bson::BSONObj &detail ) ;

      static INT32 snapshotNetcardInfo( const _sptArguments &arg,
                                        _sptReturnVal &rval,
                                        bson::BSONObj &detail ) ;

      static INT32 getIpTablesInfo( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail ) ;

      static INT32 getHostName( const _sptArguments &arg,
                                _sptReturnVal &rval,
                                bson::BSONObj &detail ) ;

      static INT32 sniffPort ( const _sptArguments &arg,
                               _sptReturnVal &rval,
                               bson::BSONObj &detail ) ;

      static INT32 getPID ( const _sptArguments &arg,
                            _sptReturnVal &rval,
                            bson::BSONObj &detail ) ;

      static INT32 getTID ( const _sptArguments &arg,
                            _sptReturnVal &rval,
                            bson::BSONObj &detail ) ;

      static INT32 getEWD ( const _sptArguments &arg,
                            _sptReturnVal &rval,
                            bson::BSONObj &detail ) ;

      static INT32 help( const _sptArguments &arg,
                         _sptReturnVal &rval,
                         bson::BSONObj &detail ) ;

   private:
      static INT32 _extractReleaseInfo( const CHAR *buf,
                                        bson::BSONObjBuilder &builder ) ;

      static INT32 _parseHostsFile( VEC_HOST_ITEM &vecItems, string &err ) ;

      static INT32 _writeHostsFile( VEC_HOST_ITEM &vecItems, string &err ) ;

      static INT32 _extractHosts( const CHAR *buf,
                                  VEC_HOST_ITEM &vecItems ) ;

      static void  _buildHostsResult( VEC_HOST_ITEM &vecItems,
                                      bson::BSONObjBuilder &builder ) ;

      static INT32 _extractCpuInfo( const CHAR *buf,
                                    bson::BSONObjBuilder &builder ) ;

      static INT32 _extractMemInfo( const CHAR *buf,
                                    bson::BSONObjBuilder &builder ) ;

      static INT32 _getLinuxDiskInfo( const _sptArguments &arg,
                                      _sptReturnVal &rval,
                                      bson::BSONObj &detail ) ;

      static INT32 _extractLinuxDiskInfo( const CHAR *buf,
                                          _sptReturnVal &rval,
                                          bson::BSONObj &detail ) ;

      static INT32 _getWinDiskInfo( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail ) ;

      static INT32 _extractWinDiskInfo( const CHAR *buf,
                                        bson::BSONObjBuilder &builder ) ;

      static INT32 _extractNetcards( bson::BSONObjBuilder &builder ) ;

      static INT32 _snapshotNetcardInfo( bson::BSONObjBuilder &builder, 
                                         bson::BSONObj &detail ) ;
      static INT32 _extractNetCardSnapInfo( const CHAR *buf,
                                            bson::BSONObjBuilder &builder ) ;
   } ;
   typedef class _sptUsrSystem sptUsrSystem ;
}

#endif

