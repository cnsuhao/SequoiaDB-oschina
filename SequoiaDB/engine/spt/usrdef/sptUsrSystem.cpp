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

   Source File Name = sptUsrSystem.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptUsrSystem.hpp"
#include "ossCmdRunner.hpp"
#include "ossUtil.hpp"
#include "utilStr.hpp"
#include "ossSocket.hpp"
#include "ossIO.hpp"
#include "oss.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#if defined (_LINUX)
#include <net/if.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#else
#include <iphlpapi.h>
#pragma comment( lib, "IPHLPAPI.lib" )
#endif

using namespace bson ;

#define SPT_MB_SIZE     ( 1024*1024 )
#define SPT_DISK_SRC_FILE "/etc/mtab"

namespace engine
{
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, ping )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, type )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getReleaseInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getHostsMap )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getAHostMap )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, addAHostMap )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, delAHostMap )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getCpuInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, snapshotCpuInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getMemInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, snapshotMemInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getDiskInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, snapshotDiskInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getNetcardInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, snapshotNetcardInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getIpTablesInfo )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getHostName )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, sniffPort )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getPID )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getTID )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, getEWD )
   JS_STATIC_FUNC_DEFINE( _sptUsrSystem, help )

   JS_BEGIN_MAPPING( _sptUsrSystem, "System" )
      JS_ADD_STATIC_FUNC( "ping", ping )
      JS_ADD_STATIC_FUNC( "type", type )
      JS_ADD_STATIC_FUNC( "getReleaseInfo", getReleaseInfo )
      JS_ADD_STATIC_FUNC( "getHostsMap", getHostsMap )
      JS_ADD_STATIC_FUNC( "getAHostMap", getAHostMap )
      JS_ADD_STATIC_FUNC( "addAHostMap", addAHostMap )
      JS_ADD_STATIC_FUNC( "delAHostMap", delAHostMap )
      JS_ADD_STATIC_FUNC( "getCpuInfo", getCpuInfo )
      JS_ADD_STATIC_FUNC( "snapshotCpuInfo", snapshotCpuInfo )
      JS_ADD_STATIC_FUNC( "getMemInfo", getMemInfo )
      JS_ADD_STATIC_FUNC( "snapshotMemInfo", snapshotMemInfo )
      JS_ADD_STATIC_FUNC( "getDiskInfo", getDiskInfo )
      JS_ADD_STATIC_FUNC( "snapshotDiskInfo", snapshotDiskInfo )
      JS_ADD_STATIC_FUNC( "getNetcardInfo", getNetcardInfo )
      JS_ADD_STATIC_FUNC( "snapshotNetcardInfo", snapshotNetcardInfo )
      JS_ADD_STATIC_FUNC( "getIpTablesInfo", getIpTablesInfo )
      JS_ADD_STATIC_FUNC( "getHostName", getHostName )
      JS_ADD_STATIC_FUNC( "sniffPort", sniffPort )
      JS_ADD_STATIC_FUNC( "getPID", getPID )
      JS_ADD_STATIC_FUNC( "getTID", getTID )
      JS_ADD_STATIC_FUNC( "getEWD", getEWD )
      JS_ADD_STATIC_FUNC( "help", help )
   JS_MAPPING_END()

   INT32 _sptUsrSystem::ping( const _sptArguments &arg,
                              _sptReturnVal &rval,
                              bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      string host ;
      stringstream cmd ;
      _ossCmdRunner runner ;
      UINT32 exitCode = 0 ;
      rc = arg.getString( 0, host ) ;
      if ( SDB_OUT_OF_BOUND == rc )
      {
         detail = BSON( SPT_ERR << "hostname must be config" ) ;
      }
      else if ( rc )
      {
         detail = BSON( SPT_ERR << "hostname must be string" ) ;
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get hostname, rc: %d", rc ) ;

#if defined (_LINUX)
      cmd << "ping " << " -q -c 1 "  << "\"" << host << "\"" ;
#elif defined (_WINDOWS)
      cmd << "ping -n 2 -w 1000 " << "\"" << host << "\"" ;
#endif

      rc = runner.exec( cmd.str().c_str(), exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to exec cmd, rc:%d, exit:%d",
                 rc, exitCode ) ;
         if ( SDB_OK == rc )
         {
            rc = SDB_SYS ;
         }
         stringstream ss ;
         ss << "failed to exec cmd \"ping\",rc:"
            << rc
            << ",exit:"
            << exitCode ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      builder.append( SPT_USR_SYSTEM_TARGET, host ) ;
      builder.appendBool( SPT_USR_SYSTEM_REACHABLE, SDB_OK == exitCode ) ;
      rval.setBSONObj( "", builder.obj() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::type( const _sptArguments &arg,
                              _sptReturnVal &rval,
                              bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      if ( 0 < arg.argc() )
      {
         PD_LOG( PDERROR, "type() should have non arguments" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

#if defined (_LINUX)
      rval.setStringVal( "", "LINUX") ;
#elif defined (_WINDOWS)
      rval.setStringVal( "", "WINDOWS" ) ;
#endif
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::getReleaseInfo( const _sptArguments &arg,
                                        _sptReturnVal &rval,
                                        bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 exitCode = 0 ;
      _ossCmdRunner runner ;
      string outStr ;
      BSONObjBuilder builder ;

      if ( 0 < arg.argc() )
      {
         PD_LOG( PDERROR, "getReleaseInfo() should have non arguments" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

#if defined (_LINUX)
      rc = runner.exec( "lsb_release -a |grep -v \"LSB Version\"", exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
#elif defined (_WINDOWS)
      rc = SDB_SYS ;
#endif
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         rc = SDB_OK ;
         ossOSInfo info ;
         ossGetOSInfo( info ) ;

         builder.append( SPT_USR_SYSTEM_DISTRIBUTOR, info._distributor ) ;
         builder.append( SPT_USR_SYSTEM_RELASE, info._release ) ;
         builder.append( SPT_USR_SYSTEM_DESP, info._desp ) ;
         builder.append( SPT_USR_SYSTEM_BIT, info._bit ) ;

         rval.setBSONObj( "", builder.obj() ) ;
         goto done ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from cmd \"lsb_release -a\", rc:"
            << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = _extractReleaseInfo( outStr.c_str(), builder ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         stringstream ss ;
         ss << "failed to extract info from release info:"
            << outStr ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      outStr = "" ;
#if defined (_LINUX)
      rc = runner.exec( "uname -a", exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
#elif defined (_WINDOWS)
      rc = SDB_SYS ;
#endif
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         PD_LOG( PDERROR, "failed to exec cmd, rc:%d, exit:%d",
                 rc, exitCode ) ;
         if ( SDB_OK == rc )
         {
            rc = SDB_SYS ;
         }
         stringstream ss ;
         ss << "failed to exec cmd \"uname -a\", rc:"
            << rc
            << ",exit:"
            << exitCode ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from cmd \"uname -a\", rc:"
            << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      if ( NULL != ossStrstr( outStr.c_str(), "x86_64") )
      {
         builder.append( SPT_USR_SYSTEM_BIT, 64 ) ;
      }
      else
      {
         builder.append( SPT_USR_SYSTEM_BIT, 32 ) ;
      }
      rval.setBSONObj( "", builder.obj() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::_extractReleaseInfo( const CHAR *buf,
                                             bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      vector<string> splited ;
      boost::algorithm::split( splited, buf, boost::is_any_of("\n:") ) ;
      vector<string>::iterator itr = splited.begin() ;
      const string *distributor = NULL ;
      const string *release = NULL ;
      const string *desp = NULL ;
      for ( ; itr != splited.end(); itr++ )
      {
         if ( itr->empty() )
         {
            continue ;
         }
         boost::algorithm::trim( *itr ) ;
         if ( "Distributor ID" == *itr &&
              itr < splited.end() - 1 )
         {
            distributor = &( *( itr + 1 ) ) ;
         }
         else if ( "Release" == *itr &&
                   itr < splited.end() - 1 )
         {
            release = &( *( itr + 1 ) ) ;
         }
         else if ( "Description" == *itr &&
                   itr < splited.end() - 1 )
         {
            desp = &( *( itr + 1 ) ) ;
         }
      }

      if ( NULL == distributor ||
           NULL == release )
      {
         PD_LOG( PDERROR, "failed to split release info:%s",
                 buf )  ;
         rc = SDB_SYS ;
         goto error ;
      }

      builder.append( SPT_USR_SYSTEM_DISTRIBUTOR, *distributor ) ;
      builder.append( SPT_USR_SYSTEM_RELASE, *release ) ;
      builder.append( SPT_USR_SYSTEM_DESP, *desp ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::getHostsMap( const _sptArguments &arg,
                                     _sptReturnVal &rval,
                                     bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      string err ;
      VEC_HOST_ITEM vecItems ;

      if ( 0 < arg.argc() )
      {
         err = "getHostsMap() should have non arguments" ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _parseHostsFile( vecItems, err ) ;
      if ( rc )
      {
         goto error ;
      }

      _buildHostsResult( vecItems, builder ) ;
      rval.setBSONObj( "", builder.obj() ) ;

   done:
      return rc ;
   error:
      detail = BSON( SPT_ERR << err ) ;
      goto done ;
   }

   INT32 _sptUsrSystem::getAHostMap( const _sptArguments & arg,
                                     _sptReturnVal & rval,
                                     BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string hostname ;
      string err ;
      VEC_HOST_ITEM vecItems ;

      rc = arg.getString( 0, hostname ) ;
      if ( SDB_OUT_OF_BOUND == rc )
      {
         err = "hostname must config" ;
         goto error ;
      }
      else if ( rc )
      {
         err = "hostname must be string" ;
         goto error ;
      }
      else if ( hostname.empty() )
      {
         rc = SDB_INVALIDARG ;
         err = "hostname can't be empty" ;
         goto error ;
      }

      rc = _parseHostsFile( vecItems, err ) ;
      if ( rc )
      {
         goto error ;
      }
      else
      {
         VEC_HOST_ITEM::iterator it = vecItems.begin() ;
         while ( it != vecItems.end() )
         {
            sptHostItem &item = *it ;
            ++it ;
            if( LINE_HOST == item._lineType && hostname == item._host )
            {
               rval.setStringVal( "", item._ip.c_str() ) ;
               goto done ;
            }
         }
         err = "hostname not exist" ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return rc ;
   error:
      detail = BSON( SPT_ERR << err ) ;
      goto done ;
   }

   INT32 _sptUsrSystem::addAHostMap( const _sptArguments & arg,
                                     _sptReturnVal & rval,
                                     BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string hostname ;
      string ip ;
      INT32  isReplace = 1 ;
      string err ;
      VEC_HOST_ITEM vecItems ;

      rc = arg.getString( 0, hostname ) ;
      if ( rc == SDB_OUT_OF_BOUND )
      {
         err = "hostname must be config" ;
         goto error ;
      }
      else if ( rc )
      {
         err = "hostname must be string" ;
         goto error ;
      }
      else if ( hostname.empty() )
      {
         rc = SDB_INVALIDARG ;
         err = "hostname can't be empty" ;
         goto error ;
      }

      rc = arg.getString( 1, ip ) ;
      if ( rc == SDB_OUT_OF_BOUND )
      {
         err = "ip must be config" ;
         goto error ;
      }
      else if ( rc )
      {
         err = "ip must be string" ;
         goto error ;
      }
      else if ( ip.empty() )
      {
         rc = SDB_INVALIDARG ;
         err = "ip can't be empty" ;
         goto error ;
      }
      else if ( !isValidIPV4( ip.c_str() ) )
      {
         rc = SDB_INVALIDARG ;
         err = "ip is not ipv4" ;
         goto error ;
      }

      if ( arg.argc() > 2 )
      {
         rc = arg.getNative( 2, (void*)&isReplace, SPT_NATIVE_INT32 ) ;
         if ( rc )
         {
            err = "isReplace must be BOOLEAN" ;
            goto error ;
        }
      }

      rc = _parseHostsFile( vecItems, err ) ;
      if ( rc )
      {
         goto error ;
      }
      else
      {
         VEC_HOST_ITEM::iterator it = vecItems.begin() ;
         BOOLEAN hasMod = FALSE ;
         while ( it != vecItems.end() )
         {
            sptHostItem &item = *it ;
            ++it ;
            if( item._lineType == LINE_HOST && hostname == item._host )
            {
               if ( item._ip == ip )
               {
                  goto done ;
               }
               else if ( !isReplace )
               {
                  err = "hostname already exist" ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
               item._ip = ip ;
               hasMod = TRUE ;
            }
         }
         if ( !hasMod )
         {
            sptHostItem info ;
            info._lineType = LINE_HOST ;
            info._host = hostname ;
            info._ip = ip ;
            vecItems.push_back( info ) ;
         }
         rc = _writeHostsFile( vecItems, err ) ;
         if ( rc )
         {
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      detail = BSON( SPT_ERR << err ) ;
      goto done ;
   }

   INT32 _sptUsrSystem::delAHostMap( const _sptArguments & arg,
                                     _sptReturnVal & rval,
                                     BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string hostname ;
      string err ;
      VEC_HOST_ITEM vecItems ;

      rc = arg.getString( 0, hostname ) ;
      if ( rc == SDB_OUT_OF_BOUND )
      {
         err = "hostname must be config" ;
         goto error ;
      }
      else if ( rc )
      {
         err = "hostname must be string" ;
         goto error ;
      }
      else if ( hostname.empty() )
      {
         rc = SDB_INVALIDARG ;
         err = "hostname can't be empty" ;
         goto error ;
      }

      rc = _parseHostsFile( vecItems, err ) ;
      if ( rc )
      {
         goto error ;
      }
      else
      {
         VEC_HOST_ITEM::iterator it = vecItems.begin() ;
         BOOLEAN hasDel = FALSE ;
         while ( it != vecItems.end() )
         {
            sptHostItem &item = *it ;
            if( item._lineType == LINE_HOST && hostname == item._host )
            {
               it = vecItems.erase( it ) ;
               hasDel = TRUE ;
               continue ;
            }
            ++it ;
         }
         if ( hasDel )
         {
            rc = _writeHostsFile( vecItems, err ) ;
            if ( rc )
            {
               goto error ;
            }
         }
      }

   done:
      return rc ;
   error:
      detail = BSON( SPT_ERR << err ) ;
      goto done ;
   }

#if defined( _LINUX )
   #define HOSTS_FILE      "/etc/hosts"
#else
   #define HOSTS_FILE      "C:\\Windows\\System32\\drivers\\etc\\hosts"
#endif // _LINUX

   INT32 _sptUsrSystem::_parseHostsFile( VEC_HOST_ITEM & vecItems,
                                         string &err )
   {
      INT32 rc = SDB_OK ;
      OSSFILE file ;
      stringstream ss ;
      BOOLEAN isOpen = FALSE ;
      INT64 fileSize = 0 ;
      CHAR *pBuff = NULL ;
      INT64 hasRead = 0 ;

      rc = ossGetFileSizeByName( HOSTS_FILE, &fileSize ) ;
      if ( rc )
      {
         ss << "get file[" << HOSTS_FILE << "] size failed: " << rc ;
         goto error ;
      }
      pBuff = ( CHAR* )SDB_OSS_MALLOC( fileSize + 1 ) ;
      if ( !pBuff )
      {
         ss << "alloc memory[" << fileSize << "] failed" ;
         rc = SDB_OOM ;
         goto error ;
      }
      ossMemset( pBuff, 0, fileSize + 1 ) ;

      rc = ossOpen( HOSTS_FILE, OSS_READONLY|OSS_SHAREREAD, 0,
                    file ) ;
      if ( rc )
      {
         ss << "open file[" << HOSTS_FILE << "] failed: " << rc ;
         goto error ;
      }
      isOpen = TRUE ;

      rc = ossReadN( &file, fileSize, pBuff, hasRead ) ;
      if ( rc )
      {
         ss << "read file[" << HOSTS_FILE << "] failed: " << rc ;
         goto error ;
      }
      ossClose( file ) ;
      isOpen = FALSE ;

      rc = _extractHosts( pBuff, vecItems ) ;
      if ( rc )
      {
         ss << "extract hosts failed: " << rc ;
         goto error ;
      }

      if ( vecItems.size() > 0 )
      {
         VEC_HOST_ITEM::iterator itr = vecItems.end() - 1 ;
         sptHostItem &info = *itr ;
         if ( info.toString().empty() )
         {
            vecItems.erase( itr ) ;
         }
      }

   done:
      if ( isOpen )
      {
         ossClose( file ) ;
      }
      if ( pBuff )
      {
         SDB_OSS_FREE( pBuff ) ;
      }
      return rc ;
   error:
      err = ss.str() ;
      goto done ;
   }

   INT32 _sptUsrSystem::_writeHostsFile( VEC_HOST_ITEM & vecItems,
                                         string & err )
   {
      INT32 rc = SDB_OK ;
      std::string tmpFile = HOSTS_FILE ;
      tmpFile += ".tmp" ;
      OSSFILE file ;
      BOOLEAN isOpen = FALSE ;
      BOOLEAN isBak = FALSE ;
      stringstream ss ;

      if ( SDB_OK == ossAccess( tmpFile.c_str() ) )
      {
         ossDelete( tmpFile.c_str() ) ;
      }

      if ( SDB_OK == ossAccess( HOSTS_FILE ) )
      {
         if ( SDB_OK == ossRenamePath( HOSTS_FILE, tmpFile.c_str() ) )
         {
            isBak = TRUE ;
         }
      }

      rc = ossOpen ( HOSTS_FILE, OSS_READWRITE|OSS_SHAREWRITE|OSS_REPLACE,
                     OSS_RU|OSS_WU|OSS_RG|OSS_RO, file ) ;
      if ( rc )
      {
         ss << "open file[" <<  HOSTS_FILE << "] failed: " << rc ;
         goto error ;
      }
      isOpen = TRUE ;

      {
         VEC_HOST_ITEM::iterator it = vecItems.begin() ;
         UINT32 count = 0 ;
         while ( it != vecItems.end() )
         {
            ++count ;
            sptHostItem &item = *it ;
            ++it ;
            string text = item.toString() ;
            if ( !text.empty() || count < vecItems.size() )
            {
               text += OSS_NEWLINE ;
            }
            rc = ossWriteN( &file, text.c_str(), text.length() ) ;
            if ( rc )
            {
               ss << "write context[" << text << "] to file[" << HOSTS_FILE
                  << "] failed: " << rc ;
               goto error ;
            }
         }
      }

      if ( SDB_OK == ossAccess( tmpFile.c_str() ) )
      {
         ossDelete( tmpFile.c_str() ) ;
      }

   done:
      if ( isOpen )
      {
         ossClose( file ) ;
      }
      return rc ;
   error:
      if ( isBak )
      {
         if ( isOpen )
         {
            ossClose( file ) ;
            isOpen = FALSE ;
            ossDelete( HOSTS_FILE ) ;
         }
         ossRenamePath( tmpFile.c_str(), HOSTS_FILE ) ;
      }
      err = ss.str() ;
      goto done ;
   }

   INT32 _sptUsrSystem::_extractHosts( const CHAR *buf,
                                       VEC_HOST_ITEM &vecItems )
   {
      vector<string> splited ;
      boost::algorithm::split( splited, buf, boost::is_any_of("\r\n") ) ;
      if ( splited.empty() )
      {
         goto done ;
      }

      for ( vector<string>::iterator itr = splited.begin() ;
            itr != splited.end() ;
            itr++ )
      {
         sptHostItem item ;

         if ( itr->empty() )
         {
            vecItems.push_back( item ) ;
            continue ;
         }
         boost::algorithm::trim( *itr ) ;
         vector<string> columns ;
         boost::algorithm::split( columns, *itr, boost::is_any_of("\t ") ) ;

         for ( vector<string>::iterator itr2 = columns.begin();
               itr2 != columns.end();
               )
         {
            if ( itr2->empty() )
            {
               itr2 = columns.erase( itr2 ) ;
            }
            else
            {
               ++itr2 ;
            }
         }

         if ( 2 != columns.size() && 3 != columns.size() )
         {
            item._ip = *itr ;
            vecItems.push_back( item ) ;
            continue ;
         }

         if ( !isValidIPV4( columns.at( 0 ).c_str() ) )
         {
            item._ip = *itr ;
            vecItems.push_back( item ) ;
            continue ;
         }

         item._ip = columns[ 0 ] ;
         if ( columns.size() == 3 )
         {
            item._com = columns[ 1 ] ;
            item._host = columns[ 2 ] ;
         }
         else
         {
            item._host = columns[ 1 ] ;
         }
         item._lineType = LINE_HOST ;
         vecItems.push_back( item ) ;
      }

   done:
      return SDB_OK ;
   }

   void _sptUsrSystem::_buildHostsResult( VEC_HOST_ITEM & vecItems,
                                          BSONObjBuilder &builder )
   {
      BSONArrayBuilder arrBuilder ;
      VEC_HOST_ITEM::iterator it = vecItems.begin() ;
      while ( it != vecItems.end() )
      {
         sptHostItem &item = *it ;
         ++it ;

         if ( LINE_HOST != item._lineType )
         {
            continue ;
         }
         arrBuilder << BSON( SPT_USR_SYSTEM_IP << item._ip <<
                             SPT_USR_SYSTEM_HOSTNAME << item._host ) ;
      }
      builder.append( SPT_USR_SYSTEM_HOSTS, arrBuilder.arr() ) ;
   }

   static INT32 getCoreNum( UINT32 &num, std::string &error )
   {
      INT32 rc = SDB_OK ;
      _ossCmdRunner runner ;
      std::stringstream ss ;
      std::string outStr ;
      UINT32 exitCode = 0 ;
      const CHAR *cmd = "cat /proc/cpuinfo |grep 'processor' | wc -l" ;
      
      rc = runner.exec( cmd, exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         PD_LOG( PDERROR, "failed to exec cmd, rc:%d, exit:%d",
                 rc, exitCode ) ;
         if ( SDB_OK == rc )
         {
            rc = SDB_SYS ;
         }
         ss << "failed to exec cmd \" " << cmd << "\",rc:"
            << rc
            << ",exit:"
            << exitCode ;
         goto error ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         ss << "failed to read msg from cmd \"" << cmd << "\", rc:"
            << rc ;
         goto error ;
      }

      boost::algorithm::replace_last( outStr, "\n", "") ;
      try
      {
         num = boost::lexical_cast<UINT32>( outStr ) ;
      }
      catch ( std::exception &e )
      {
         ss << "unexpected error happened:" << e.what() ;
         rc = SDB_SYS ;
         goto error ; 
      }
   done:
      return rc ;
   error:
      error = ss.str() ;
      goto done ;
   }

   static INT32 getModelName( std::string &name, std::string &error )
   {
      INT32 rc = SDB_OK ;
      _ossCmdRunner runner ;
      std::stringstream ss ;
      std::string outStr ;
      UINT32 exitCode = 0 ;
      const CHAR *cmd = NULL ;
#if defined (_PPCLIN64)
      cmd = "cat /proc/cpuinfo |grep 'cpu' | cut -f2 -d: | uniq" ;
#else
      cmd = "cat /proc/cpuinfo |grep 'model name' | cut -f2 -d: | uniq" ;
#endif
      rc = runner.exec( cmd, exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         PD_LOG( PDERROR, "failed to exec cmd, rc:%d, exit:%d",
                 rc, exitCode ) ;
         if ( SDB_OK == rc )
         {
            rc = SDB_SYS ;
         }
         ss << "failed to exec cmd \" " << cmd << "\",rc:"
            << rc
            << ",exit:"
            << exitCode ;
         goto error ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         ss << "failed to read msg from cmd \"" << cmd << "\", rc:"
            << rc ;
         goto error ;
      }

     
      boost::algorithm::trim_left( outStr ) ;
      boost::algorithm::replace_last( outStr, "\n", "" ) ;
      name = outStr ;
   done:
      return rc ;
   error:
      error = ss.str() ;
      goto done ;
   }

   static INT32 getFreq( std::string &freq, std::string &error )
   {
      INT32 rc = SDB_OK ;
      _ossCmdRunner runner ;
      std::stringstream ss ;
      std::string outStr ;
      UINT32 exitCode = 0 ;
      FLOAT32 frequency = 0.0 ;
      const CHAR *cmd = NULL ;
#if defined (_PPCLIN64)
      cmd = "cat /proc/cpuinfo |grep 'clock' | cut -f2 -d: | uniq" ;
#else
      cmd = "cat /proc/cpuinfo |grep 'cpu MHz' | cut -f2 -d: | uniq" ;
#endif
      rc = runner.exec( cmd, exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         PD_LOG( PDERROR, "failed to exec cmd, rc:%d, exit:%d",
                 rc, exitCode ) ;
         if ( SDB_OK == rc )
         {
            rc = SDB_SYS ;
         }
         ss << "failed to exec cmd \" " << cmd << "\",rc:"
            << rc
            << ",exit:"
            << exitCode ;
         goto error ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         ss << "failed to read msg from cmd \"" << cmd << "\", rc:"
            << rc ;
         goto error ;
      }

      boost::algorithm::trim_left( outStr ) ;
      boost::algorithm::replace_last( outStr, "\n", "" ) ;
      boost::algorithm::replace_last( outStr, "MHz", "" ) ;
      try
      {
         frequency = boost::lexical_cast<FLOAT32>( outStr ) ;
         frequency /= 1000.0 ;
         outStr = boost::lexical_cast<string>( frequency ) ;
      }
      catch ( std::exception &e )
      {
         ss << "unexpected error happened:" << e.what() ;
         rc = SDB_SYS ;
         goto error ;
      }
      freq = outStr + "GHz" ;
   done:
      return rc ;
   error:
      error = ss.str() ;
      goto done ;
   }

#if defined (_LINUX)
   INT32 _sptUsrSystem::getCpuInfo( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      BSONArrayBuilder arrBuilder ;
      BSONObjBuilder cpuBuilder ;
      string error ;
      std::string model ;
      std::string freq ;
      UINT32 coreNum ;

      rc = getCoreNum( coreNum, error ) ;
      if ( SDB_OK != rc )
      {
         detail = BSON( SPT_ERR << error ) ;
         goto error ;
      }

      rc = getModelName( model, error ) ;
      if ( SDB_OK != rc )
      {
         detail = BSON( SPT_ERR << error ) ;
         goto error ;
      }

      rc = getFreq( freq, error ) ;
      if ( SDB_OK != rc )
      {
         detail = BSON( SPT_ERR << error ) ;
         goto error ;
      }

      cpuBuilder.append( SPT_USR_SYSTEM_CORE, coreNum ) ;
      cpuBuilder.append( SPT_USR_SYSTEM_INFO, model ) ;
      cpuBuilder.append( SPT_USR_SYSTEM_FREQ, freq ) ;

      arrBuilder << cpuBuilder.obj() ;
      builder.append( SPT_USR_SYSTEM_CPUS, arrBuilder.arr() ) ;

      {
      SINT64 user = 0 ;
      SINT64 sys = 0 ;
      SINT64 idle = 0 ;
      SINT64 other = 0 ;
      rc = ossGetCPUInfo( user, sys, idle, other ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      builder.appendNumber( SPT_USR_SYSTEM_USER, user ) ;
      builder.appendNumber( SPT_USR_SYSTEM_SYS, sys ) ;
      builder.appendNumber( SPT_USR_SYSTEM_IDLE, idle ) ;
      builder.appendNumber( SPT_USR_SYSTEM_OTHER, other ) ;
      }
      rval.setBSONObj( "", builder.obj() ) ; 
       
   done:
      return rc ;
   error:
      goto done ;
   }
#endif /// _LINUX

#if defined (_WINDOWS)
   INT32 _sptUsrSystem::getCpuInfo( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 exitCode = 0 ;
      _ossCmdRunner runner ;
      string outStr ;
      BSONObjBuilder builder ;
      const CHAR *cmd = "wmic CPU GET CurrentClockSpeed,Name,NumberOfCores" ;

      rc = runner.exec( cmd, exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         PD_LOG( PDERROR, "failed to exec cmd, rc:%d, exit:%d",
                 rc, exitCode ) ;
         if ( SDB_OK == rc )
         {
            rc = SDB_SYS ;
         }
         stringstream ss ;
         ss << "failed to exec cmd \" " << cmd << "\",rc:"
            << rc
            << ",exit:"
            << exitCode ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from cmd \"" << cmd << "\", rc:"
            << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = _extractCpuInfo( outStr.c_str(), builder ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract cpu info:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from buf:"
            << outStr ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      {
      SINT64 user = 0 ;
      SINT64 sys = 0 ;
      SINT64 idle = 0 ;
      SINT64 other = 0 ;
      rc = ossGetCPUInfo( user, sys, idle, other ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      builder.appendNumber( SPT_USR_SYSTEM_USER, user ) ;
      builder.appendNumber( SPT_USR_SYSTEM_SYS, sys ) ;
      builder.appendNumber( SPT_USR_SYSTEM_IDLE, idle ) ;
      builder.appendNumber( SPT_USR_SYSTEM_OTHER, other ) ;
      }
      rval.setBSONObj( "", builder.obj() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }
#endif /// _WINDOWS


   INT32 _sptUsrSystem::snapshotCpuInfo( const _sptArguments &arg,
                                         _sptReturnVal &rval,
                                         bson::BSONObj &detail )
   {
      INT32 rc     = SDB_OK ;
      SINT64 user  = 0 ;
      SINT64 sys   = 0 ;
      SINT64 idle  = 0 ;
      SINT64 other = 0 ;
      rc = ossGetCPUInfo( user, sys, idle, other ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get cpuinfo:%d", rc ) ;
         stringstream ss ;
         ss << "failed to get cpuinfo:rc="
            << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      {
         BSONObjBuilder builder ;
         builder.appendNumber( SPT_USR_SYSTEM_USER, user ) ;
         builder.appendNumber( SPT_USR_SYSTEM_SYS, sys ) ;
         builder.appendNumber( SPT_USR_SYSTEM_IDLE, idle ) ;
         builder.appendNumber( SPT_USR_SYSTEM_OTHER, other ) ;

         rval.setBSONObj( "", builder.obj() ) ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

#if defined (_LINUX)
   INT32 _sptUsrSystem::_extractCpuInfo( const CHAR *buf,
                                         bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      BSONArrayBuilder arrBuilder ;
      vector<string> splited ;
      UINT32 coreNum = 0 ;
      stringstream info ;
      string frequency = "unknown" ;

      boost::algorithm::split( splited, buf, boost::is_any_of("\r\n") ) ;
      for ( vector<string>::iterator itr = splited.begin();
            itr != splited.end();
            itr++ )
      {
         if ( itr->empty() )
         {
            continue ;
         }
         boost::algorithm::trim( *itr ) ;
         vector<string> columns ;
         boost::algorithm::split( columns, *itr, boost::is_any_of("\t ") ) ;

         for ( vector<string>::iterator itr2 = columns.begin();
               itr2 != columns.end();
               )
         {
            if ( itr2->empty() )
            {
               itr2 = columns.erase( itr2 ) ;
            }
            else
            {
               ++itr2 ;
            }
         }

         if ( columns.size() < 4 )
         {
            rc = SDB_SYS ;
            goto error ;
         }
         try
         {
            coreNum = boost::lexical_cast<UINT32>( columns.at( 0 ) ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG( PDERROR, "unexpected err happened:%s, content:%s",
                    e.what(), columns.at( 0 ).c_str() ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         for ( UINT32 i = 1; i < columns.size(); i++ )
         {
            if ( "@" == columns.at( i ) )
            {
               if ( i == columns.size() - 2 )
               {
                  frequency = columns.at( columns.size() - 1 ) ;
                  break ;
               }
            }

            info << columns.at( i ) << " " ;
         }
      }

   done:
      arrBuilder << BSON( SPT_USR_SYSTEM_CORE << coreNum
                          << SPT_USR_SYSTEM_INFO << info.str()
                          << SPT_USR_SYSTEM_FREQ << frequency ) ;
      
      builder.append( SPT_USR_SYSTEM_CPUS, arrBuilder.arr() ) ;
      rc = SDB_OK ;
      return rc ;
   error:
      goto done ;
   }
#else
   INT32 _sptUsrSystem::_extractCpuInfo( const CHAR *buf,
                                         bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      BSONArrayBuilder arrBuilder ;
      vector<string> splited ;
      INT32 lineCount = 0 ;
      boost::algorithm::split( splited, buf, boost::is_any_of("\r\n") ) ;
      for ( vector<string>::iterator itr = splited.begin();
            itr != splited.end();
            itr++ )
      {
         ++lineCount ;
         if ( 1 == lineCount || itr->empty() )
         {
            continue ;
         }
         boost::algorithm::trim( *itr ) ;
         vector<string> columns ;
         boost::algorithm::split( columns, *itr, boost::is_any_of("\t ") ) ;

         for ( vector<string>::iterator itr2 = columns.begin();
               itr2 != columns.end();
               )
         {
            if ( itr2->empty() )
            {
               itr2 = columns.erase( itr2 ) ;
            }
            else
            {
               ++itr2 ;
            }
         }

         if ( columns.size() < 3 )
         {
            rc = SDB_SYS ;
            goto error ;
         }
         UINT32 coreNum = 0 ;
         stringstream info ;

         try
         {
            coreNum = boost::lexical_cast<UINT32>(
               columns.at( columns.size() - 1 ) ) ;
         }
         catch ( std::exception &e )
         {
            PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         for ( UINT32 i = 1; i < columns.size() - 1 ; i++ )
         {
            info << columns.at( i ) << " " ;
         }

         arrBuilder << BSON( SPT_USR_SYSTEM_CORE << coreNum
                             << SPT_USR_SYSTEM_INFO << info.str()
                             << SPT_USR_SYSTEM_FREQ << columns[0] ) ;
      }

      builder.append( SPT_USR_SYSTEM_CPUS, arrBuilder.arr() ) ;
   done:
      return rc ;
   error:
      goto done ;
   }
#endif //_LINUX

   INT32 _sptUsrSystem::getMemInfo( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 exitCode = 0 ;
      _ossCmdRunner runner ;
      string outStr ;
      BSONObjBuilder builder ;

#if defined (_LINUX)
      rc = runner.exec( "free -m |grep Mem", exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
#elif defined (_WINDOWS)
      rc = SDB_SYS ;
#endif
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         INT32 loadPercent = 0 ;
         INT64 totalPhys = 0 ;
         INT64 availPhys = 0 ;
         INT64 totalPF = 0 ;
         INT64 availPF = 0 ;
         INT64 totalVirtual = 0 ;
         INT64 availVirtual = 0 ;
         rc = ossGetMemoryInfo( loadPercent, totalPhys, availPhys,
                                totalPF, availPF, totalVirtual,
                                availVirtual ) ;
         if ( rc )
         {
            stringstream ss ;
            ss << "ossGetMemoryInfo failed, rc:" << rc ;
            detail = BSON( SPT_ERR << ss.str() ) ;
            goto error ;
         }

         builder.append( SPT_USR_SYSTEM_SIZE, (INT32)(totalPhys/SPT_MB_SIZE) ) ;
         builder.append( SPT_USR_SYSTEM_USED,
                         (INT32)((totalPhys-availPhys)/SPT_MB_SIZE) ) ;
         builder.append( SPT_USR_SYSTEM_FREE,(INT32)(availPhys/SPT_MB_SIZE) ) ;
         builder.append( SPT_USR_SYSTEM_UNIT, "M" ) ;
         rval.setBSONObj( "", builder.obj() ) ;
         goto done ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from cmd \"free\", rc:"
            << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = _extractMemInfo( outStr.c_str(), builder ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract mem info:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from buf:"
            << outStr ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }
      rval.setBSONObj( "", builder.obj() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::snapshotMemInfo( const _sptArguments &arg,
                                         _sptReturnVal &rval,
                                         bson::BSONObj &detail )
   {
      return getMemInfo( arg, rval, detail ) ;
   }

   INT32 _sptUsrSystem::_extractMemInfo( const CHAR *buf,
                                         bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      vector<string> splited ;
      boost::algorithm::split( splited, buf, boost::is_any_of("\t ") ) ;

      for ( vector<string>::iterator itr = splited.begin();
            itr != splited.end();
          )
      {
         if ( itr->empty() )
         {
            itr = splited.erase( itr ) ;
         }
         else
         {
            ++itr ;
         }
      }
      if ( splited.size() < 4 )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      try
      {
         builder.append( SPT_USR_SYSTEM_SIZE,
                         boost::lexical_cast<UINT32>(splited.at( 1 ) ) ) ;
         builder.append( SPT_USR_SYSTEM_USED,
                         boost::lexical_cast<UINT32>(splited.at( 2 ) ) ) ;
         builder.append( SPT_USR_SYSTEM_FREE,
                         boost::lexical_cast<UINT32>(splited.at( 3) ) ) ;
         builder.append( SPT_USR_SYSTEM_UNIT, "M" ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::getDiskInfo( const _sptArguments &arg,
                                     _sptReturnVal &rval,
                                     bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
#if defined (_LINUX)
      rc = _getLinuxDiskInfo( arg, rval, detail ) ;
#else
      rc = _getWinDiskInfo( arg, rval, detail ) ;
#endif
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::_getLinuxDiskInfo( const _sptArguments &arg,
                                           _sptReturnVal &rval,
                                           bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      INT64 fileSz = 0 ;
      SINT64 read = 0 ;
      OSSFILE file ;
      stringstream ss ;
      stringstream filess ;
      const UINT32 bufSize = 256 ;
      CHAR buf[bufSize + 1] = { 0 } ; 

      rc = ossOpen( SPT_DISK_SRC_FILE,
                    OSS_READONLY | OSS_SHAREREAD,
                    OSS_DEFAULTFILE,
                    file ) ;
      if ( SDB_OK != rc )
      {
         ss << "failed to open file(/etc/mtab), rc:" << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      do
      {
         read = 0 ;
         ossMemset( buf, bufSize, '\0' ) ;
         rc = ossReadN( &file, bufSize, buf, read ) ;
         if ( SDB_OK != rc )
         {
            ss << "failed to read file(/etc/mtab), rc:" << rc ;
            detail = BSON( SPT_ERR << ss.str() ) ;
            goto error ;
         }

         filess << buf ;
         if ( read < bufSize )
         {
            break ;
         }
      } while ( TRUE ) ;

      rc = _extractLinuxDiskInfo( filess.str().c_str(), rval, detail ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      if ( file.isOpened() )
      {
         ossClose( file ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::_extractLinuxDiskInfo( const CHAR *buf,
                                               _sptReturnVal &rval,
                                               bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      BSONArrayBuilder arrBuilder ;
      vector<string> splited ;
      boost::algorithm::split( splited, buf, boost::is_any_of("\r\n") ) ;
      for ( vector<string>::iterator itr = splited.begin();
            itr != splited.end();
            itr++ )
      {
         BSONObjBuilder diskBuilder ;
         INT64 totalBytes = 0 ;
         INT64 freeBytes = 0 ;
         const CHAR *fs = NULL ;
         const CHAR *fsType = NULL ;
         const CHAR *mount = NULL ;

         vector<string> columns ;
         boost::algorithm::split( columns, *itr, boost::is_any_of("\t ") ) ;
         if ( 6 != columns.size() )
         {
            continue ;
         }

         fs = columns.at( 0 ).c_str() ;
         fsType = columns.at( 2 ).c_str() ;
         mount = columns.at( 1 ).c_str() ;
         rc = ossGetDiskInfo( mount, totalBytes, freeBytes ) ;
         if ( SDB_OK == rc )
         {
            diskBuilder.append( SPT_USR_SYSTEM_FILESYSTEM,
                                fs ) ;
            diskBuilder.append( SPT_USR_SYSTEM_FSTYPE, fsType ) ;
            diskBuilder.appendNumber( SPT_USR_SYSTEM_SIZE, totalBytes / ( 1024 * 1024 ) ) ;
            diskBuilder.appendNumber( SPT_USR_SYSTEM_USED, ( totalBytes - freeBytes ) / ( 1024 * 1024 ) ) ;
            diskBuilder.append( SPT_USR_SYSTEM_UNIT, "MB" ) ;
            diskBuilder.append( SPT_USR_SYSTEM_MOUNT, mount ) ;
            diskBuilder.appendBool( SPT_USR_SYSTEM_ISLOCAL,
                                    string::npos != columns.at( 0 ).find( "/dev/", 0, 5 ) ) ;
         }

         arrBuilder << diskBuilder.obj() ;
      }

      builder.append( SPT_USR_SYSTEM_DISKS, arrBuilder.arr() ) ;
      rval.setBSONObj( "", builder.obj() ) ;
      return rc ;
   }

   INT32 _sptUsrSystem::_getWinDiskInfo( const _sptArguments &arg,
                                         _sptReturnVal &rval,
                                         bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 exitCode = 0 ;
      _ossCmdRunner runner ;
      string outStr ;
      BSONObjBuilder builder ;

#define DISK_CMD  "wmic VOLUME get Capacity,DriveLetter,Caption,"\
                  "DriveType,FreeSpace,SystemVolume"

      rc = runner.exec( DISK_CMD, exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         PD_LOG( PDERROR, "failed to exec cmd, rc:%d, exit:%d",
                 rc, exitCode ) ;
         if ( SDB_OK == rc )
         {
            rc = SDB_SYS ;
         }
         stringstream ss ;
         ss << "failed to exec cmd \"" << DISK_CMD << "\",rc:"
            << rc
            << ",exit:"
            << exitCode ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from cmd \"df\", rc:"
            << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = _extractWinDiskInfo( outStr.c_str(), builder ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract disk info:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from buf:"
            << outStr ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }
      rval.setBSONObj( "", builder.obj() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::snapshotDiskInfo( const _sptArguments &arg,
                                          _sptReturnVal &rval,
                                          bson::BSONObj &detail )
   {
      return getDiskInfo( arg, rval, detail ) ;
   }

   INT32 _sptUsrSystem::_extractWinDiskInfo( const CHAR *buf,
                                             bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      BSONArrayBuilder arrBuilder ;
      string fileSystem ;
      string freeSpace ;
      string total ;
      string mount ;
      vector<string> splited ;
      INT32 lineCount = 0 ;
      boost::algorithm::split( splited, buf, boost::is_any_of("\r\n") ) ;
      for ( vector<string>::iterator itr = splited.begin();
            itr != splited.end();
            itr++ )
      {
         ++lineCount ;
         if ( 1 == lineCount || itr->empty() )
         {
            continue ;
         }

         vector<string> columns ;
         boost::algorithm::split( columns, *itr, boost::is_any_of("\t ") ) ;

         for ( vector<string>::iterator itr2 = columns.begin();
               itr2 != columns.end();
               )
         {
            if ( itr2->empty() )
            {
               itr2 = columns.erase( itr2 ) ;
            }
            else
            {
               ++itr2 ;
            }
         }

         if ( columns.size() < 6 || columns.at( 5 ) == "TRUE" ||
              columns.at( 3 ) != "3" )
         {
            continue ;
         }

         total = columns[ 0 ] ;
         fileSystem = columns[ 1 ] ;
         freeSpace = columns[ 4 ] ;
         mount = columns[ 2 ] ;

         SINT64 totalNum = 0 ;
         SINT64 usedNumber = 0 ;
         SINT64 avaNumber = 0 ;
         BSONObjBuilder lineBuilder ;
         try
         {
            avaNumber = boost::lexical_cast<SINT64>( freeSpace ) ;
            totalNum = boost::lexical_cast<SINT64>( total ) ;
            usedNumber = totalNum - avaNumber ;
            lineBuilder.append( SPT_USR_SYSTEM_FILESYSTEM,
                                fileSystem.c_str() ) ;
            lineBuilder.appendNumber( SPT_USR_SYSTEM_SIZE,
                                      (INT32)( totalNum / SPT_MB_SIZE ) ) ;
            lineBuilder.appendNumber( SPT_USR_SYSTEM_USED,
                                      (INT32)( usedNumber / SPT_MB_SIZE ) ) ;
            lineBuilder.append( SPT_USR_SYSTEM_UNIT, "M" ) ;
            lineBuilder.append( SPT_USR_SYSTEM_MOUNT, mount ) ;
            lineBuilder.appendBool( SPT_USR_SYSTEM_ISLOCAL, TRUE ) ;
            arrBuilder << lineBuilder.obj() ;
         }
         catch ( std::exception &e )
         {
            rc = SDB_SYS ;
            goto error ;
         }

         freeSpace.clear();
         total.clear() ;
         mount.clear() ;
         fileSystem.clear() ;
      } // end for

      builder.append( SPT_USR_SYSTEM_DISKS, arrBuilder.arr() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::getNetcardInfo( const _sptArguments &arg,
                                        _sptReturnVal &rval,
                                        bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder ;
      if ( 0 < arg.argc() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _extractNetcards( builder ) ;
      if ( SDB_OK != rc )
      {
         stringstream ss ;
         ss << "failed to get netcard info:" << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ; 
      }
      rval.setBSONObj( "", builder.obj() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::_extractNetcards( bson::BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      CHAR *pBuff = NULL ;
      BSONArrayBuilder arrBuilder ;

#if defined (_WINDOWS)
      PIP_ADAPTER_INFO pAdapterInfo = NULL ;
      DWORD dwRetVal = 0 ;
      ULONG ulOutbufLen = sizeof( PIP_ADAPTER_INFO ) ;

      pBuff = (CHAR*)SDB_OSS_MALLOC( ulOutbufLen ) ;
      if ( !pBuff )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      pAdapterInfo = (PIP_ADAPTER_INFO)pBuff ;

      dwRetVal = GetAdaptersInfo( pAdapterInfo, &ulOutbufLen ) ;
      if ( dwRetVal == ERROR_BUFFER_OVERFLOW )
      {
         SDB_OSS_FREE( pBuff ) ;
         pBuff = ( CHAR* )SDB_OSS_MALLOC( ulOutbufLen ) ;
         if ( !pBuff )
         {
            rc = SDB_OOM ;
            goto error ;
         }
         pAdapterInfo = (PIP_ADAPTER_INFO)pBuff ;
         dwRetVal = GetAdaptersInfo( pAdapterInfo, &ulOutbufLen ) ;
      }

      if ( dwRetVal != NO_ERROR )
      {
         rc = SDB_SYS ;
         goto error ;
      }
      else
      {
         PIP_ADAPTER_INFO pAdapter = pAdapterInfo ;
         while ( pAdapter )
         {
            stringstream ss ;
            ss << "eth" << pAdapter->Index ;
            arrBuilder << BSON( SPT_USR_SYSTEM_NAME << ss.str()
                                << SPT_USR_SYSTEM_IP <<
                                pAdapter->IpAddressList.IpAddress.String ) ;
            pAdapter = pAdapter->Next ;
         }
      }
#elif defined (_LINUX)
      struct ifconf ifc ;
      struct ifreq *ifreq = NULL ;
      INT32 sock = -1 ;

      pBuff = ( CHAR* )SDB_OSS_MALLOC( 1024 ) ;
      if ( !pBuff )
      {
         rc = SDB_OOM ;
         goto error ;
      }
      ifc.ifc_len = 1024 ;
      ifc.ifc_buf = pBuff;

      if ( (sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0 )
      {
         PD_LOG( PDERROR, "failed to init socket" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      if ( SDB_OK != ioctl( sock, SIOCGIFCONF, &ifc ) )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "failed to call ioctl" ) ;
         goto error ;
      }

      ifreq = ( struct ifreq * )pBuff ;
      for ( INT32 i = ifc.ifc_len / sizeof(struct ifreq);
            i > 0;
            --i )
      {
         arrBuilder << BSON( SPT_USR_SYSTEM_NAME << ifreq->ifr_name
                             << SPT_USR_SYSTEM_IP <<
                             inet_ntoa(((struct sockaddr_in*)&
                                         (ifreq->ifr_addr))->sin_addr) ) ;
         ++ifreq ;
      }
#endif
      builder.append( SPT_USR_SYSTEM_NETCARDS, arrBuilder.arr() ) ;
   done:
      if ( pBuff )
      {
         SDB_OSS_FREE( pBuff ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::getIpTablesInfo( const _sptArguments &arg,
                                         _sptReturnVal &rval,
                                         bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      if ( 0 < arg.argc() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      {
      BSONObj info = BSON( "FireWall" << "unknown" ) ;
      rval.setBSONObj( "", info ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   #if defined (_LINUX)
   INT32 _sptUsrSystem::_extractNetCardSnapInfo( const CHAR *buf,
                                                 bson::BSONObjBuilder &builder )
   {
      time_t myTime = time( NULL ) ;
      BSONArrayBuilder arrayBuilder ;
      INT32 rc = SDB_OK ;
      vector<string> vLines ;
      boost::algorithm::split( vLines, buf, boost::is_any_of("\n") ) ;
      vector<string>::iterator iterLine = vLines.begin() ;
      while ( iterLine != vLines.end() )
      {
         if ( !iterLine->empty() )
         {
            const CHAR *oneLine = iterLine->c_str() ;
            vector<string> vColumns ;
            boost::algorithm::split( vColumns, oneLine, 
                                     boost::is_any_of("\t ") ) ;
            vector<string>::iterator iterColumn = vColumns.begin() ;
            while ( iterColumn != vColumns.end() )
            {
               if ( iterColumn->empty() )
               {
                  vColumns.erase( iterColumn++ ) ;
               }
               else
               {
                  iterColumn++ ;
               }
            }

            if ( vColumns.size() < 9 )
            {
               rc = SDB_SYS ;
               goto error ;
            }
            try
            {
               BSONObjBuilder innerBuilder ;
               innerBuilder.append( SPT_USR_SYSTEM_NAME,
                             boost::lexical_cast<string>( vColumns.at( 0 ) ) ) ;
               innerBuilder.append( SPT_USR_SYSTEM_RX_BYTES,
                            ( long long )boost::lexical_cast<UINT64>( 
                                                         vColumns.at( 1 ) ) ) ;
               innerBuilder.append( SPT_USR_SYSTEM_RX_PACKETS,
                            ( long long )boost::lexical_cast<UINT64>( 
                                                         vColumns.at( 2 ) ) ) ;
               innerBuilder.append( SPT_USR_SYSTEM_RX_ERRORS,
                            ( long long )boost::lexical_cast<UINT64>( 
                                                         vColumns.at( 3 ) ) ) ;
               innerBuilder.append( SPT_USR_SYSTEM_RX_DROPS,
                            ( long long )boost::lexical_cast<UINT64>( 
                                                         vColumns.at( 4 ) ) ) ;
               innerBuilder.append( SPT_USR_SYSTEM_TX_BYTES,
                            ( long long )boost::lexical_cast<UINT64>( 
                                                         vColumns.at( 5 ) ) ) ;
               innerBuilder.append( SPT_USR_SYSTEM_TX_PACKETS,
                            ( long long )boost::lexical_cast<UINT64>(
                                                         vColumns.at( 6 ) ) ) ;
               innerBuilder.append( SPT_USR_SYSTEM_TX_ERRORS,
                            ( long long )boost::lexical_cast<UINT64>( 
                                                         vColumns.at( 7 ) ) ) ;
               innerBuilder.append( SPT_USR_SYSTEM_TX_DROPS,
                            ( long long )boost::lexical_cast<UINT64>( 
                                                         vColumns.at( 8 ) ) ) ;
               BSONObj obj = innerBuilder.obj() ;
               arrayBuilder.append( obj ) ;
            }
            catch ( std::exception &e )
            {
               PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
               rc = SDB_SYS ;
               goto error ;
            }
         }

         iterLine++ ;
      }

      try
      {
         builder.append( SPT_USR_SYSTEM_CALENDAR_TIME, (long long)myTime ) ;
         builder.append( SPT_USR_SYSTEM_NETCARDS, arrayBuilder.arr() ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }
#else
   INT32 _sptUsrSystem::_extractNetCardSnapInfo( const CHAR *buf,
                                                 bson::BSONObjBuilder &builder )
   {
      return SDB_INVALIDARG ;
   }
#endif

#if defined (_LINUX)
   INT32 _sptUsrSystem::_snapshotNetcardInfo( bson::BSONObjBuilder &builder, 
                                              bson::BSONObj &detail )
   {
      INT32 rc        = SDB_OK ;
      UINT32 exitCode = 0 ;
      _ossCmdRunner runner ;
      string outStr ;
      stringstream ss ;
      const CHAR *netFlowCMD = "cat /proc/net/dev | grep -v Receive |"
                               " grep -v bytes | sed 's/:/ /' |"
                               " awk '{print $1,$2,$3,$4,$5,$10,$11,$12,$13}'" ;

      rc = runner.exec( netFlowCMD, exitCode,
                        FALSE, -1, FALSE, NULL, TRUE ) ;
      if ( SDB_OK != rc || SDB_OK != exitCode )
      {
         PD_LOG( PDERROR, "failed to exec cmd, rc:%d, exit:%d",
                 rc, exitCode ) ;
         if ( SDB_OK == rc )
         {
            rc = SDB_SYS ;
         }
         ss << "failed to exec cmd \"" << netFlowCMD << "\",rc:"
            << rc << ",exit:" << exitCode ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = runner.read( outStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to read msg from cmd runner:%d", rc ) ;
         stringstream ss ;
         ss << "failed to read msg from cmd \"df\", rc:" << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = _extractNetCardSnapInfo( outStr.c_str(), builder ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to extract netcard snapshotinfo:%d", rc ) ;
         ss << "failed to extract netcard snapshotinfo from buf:" << outStr ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }
#else
   INT32 _sptUsrSystem::_snapshotNetcardInfo( bson::BSONObjBuilder &builder, 
                                              bson::BSONObj &detail )
   {
      INT32 rc              = SDB_OK ;
      UINT32 exitCode       = 0 ;
      PMIB_IFTABLE pTable   = NULL ;
      stringstream ss ;
      time_t myTime ;

      DWORD size = sizeof( MIB_IFTABLE ) ;
      pTable     = (PMIB_IFTABLE) SDB_OSS_MALLOC( size ) ; 
      if ( NULL == pTable )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "new MIB_IFTABLE failed:rc=%d", rc ) ;
         ss << "new MIB_IFTABLE failed:rc=" << rc ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      ULONG uRetCode = GetIfTable( pTable, &size, TRUE ) ;
      if ( uRetCode == ERROR_NOT_SUPPORTED )
      {
         PD_LOG ( PDERROR, "GetIfTable failed:rc=%u", uRetCode ) ;
         rc = SDB_INVALIDARG ;
         ss << "GetIfTable failed:rc=" << uRetCode ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      if ( uRetCode == ERROR_INSUFFICIENT_BUFFER )
      {
         SDB_OSS_FREE( pTable ) ;
         pTable = (PMIB_IFTABLE) SDB_OSS_MALLOC( size ) ;
         if ( NULL == pTable )
         {
            rc = SDB_OOM ;
            PD_LOG ( PDERROR, "new MIB_IFTABLE failed:rc=%d", rc ) ;
            ss << "new MIB_IFTABLE failed:rc=" << rc ;
            detail = BSON( SPT_ERR << ss.str() ) ;
            goto error ;
         }
      }

      myTime = time( NULL ) ;
      uRetCode = GetIfTable( pTable, &size, TRUE ) ;
      if ( NO_ERROR != uRetCode )
      {
         PD_LOG ( PDERROR, "GetIfTable failed:rc=%u", uRetCode ) ;
         rc = SDB_INVALIDARG ;
         ss << "GetIfTable failed:rc=" << uRetCode ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      try
      {
         BSONArrayBuilder arrayBuilder ;
         for ( UINT i = 0 ; i < pTable->dwNumEntries ; i++ )
         {
            MIB_IFROW Row = pTable->table[i];
            if ( IF_TYPE_ETHERNET_CSMACD != Row.dwType )
            {
               continue ;
            }

            BSONObjBuilder innerBuilder ;
            stringstream ss ;
            ss << "eth" << Row.dwIndex ;
            innerBuilder.append( SPT_USR_SYSTEM_NAME, ss.str() ) ;
            innerBuilder.append( SPT_USR_SYSTEM_RX_BYTES,
                                 ( long long )Row.dwInOctets ) ;
            innerBuilder.append( SPT_USR_SYSTEM_RX_PACKETS,
                          ( long long )
                                 ( Row.dwInUcastPkts + Row.dwInNUcastPkts ) ) ;
            innerBuilder.append( SPT_USR_SYSTEM_RX_ERRORS,
                                 ( long long )Row.dwInErrors ) ;
            innerBuilder.append( SPT_USR_SYSTEM_RX_DROPS,
                                 ( long long )Row.dwInDiscards ) ;
            innerBuilder.append( SPT_USR_SYSTEM_TX_BYTES,
                                 ( long long )Row.dwOutOctets ) ;
            innerBuilder.append( SPT_USR_SYSTEM_TX_PACKETS,
                          ( long long ) 
                                ( Row.dwOutUcastPkts + Row.dwOutNUcastPkts ) ) ;
            innerBuilder.append( SPT_USR_SYSTEM_TX_ERRORS,
                                 ( long long )Row.dwOutErrors ) ;
            innerBuilder.append( SPT_USR_SYSTEM_TX_DROPS,
                                 ( long long )Row.dwOutDiscards ) ;
            BSONObj obj = innerBuilder.obj() ;
            arrayBuilder.append( obj ) ;
         }

         builder.append( SPT_USR_SYSTEM_CALENDAR_TIME, (long long)myTime ) ;
         builder.append( SPT_USR_SYSTEM_NETCARDS, arrayBuilder.arr() ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      if ( NULL != pTable )
      {
         SDB_OSS_FREE( pTable ) ;
      }
      return rc ;
   error:
      goto done ;
   }

#endif

   INT32 _sptUsrSystem::snapshotNetcardInfo( const _sptArguments &arg,
                                             _sptReturnVal &rval,
                                             bson::BSONObj &detail )
   {
      bson::BSONObjBuilder builder ;
      INT32 rc = SDB_OK ;
      stringstream ss ;

      if ( 0 < arg.argc() )
      {
         PD_LOG ( PDERROR, "paramenter can't be greater then 0" ) ;
         rc = SDB_INVALIDARG ;
         ss << "paramenter can't be greater then 0" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = _snapshotNetcardInfo( builder, detail ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_snapshotNetcardInfo failed:rc=%d", rc ) ;
         goto error ;
      }

      rval.setBSONObj( "", builder.obj() ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::getHostName( const _sptArguments &arg,
                                     _sptReturnVal &rval,
                                     bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      CHAR hostName[ OSS_MAX_HOSTNAME + 1 ] = { 0 } ;
      if ( 0 < arg.argc() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = ossGetHostName( hostName, OSS_MAX_HOSTNAME ) ;
      if ( rc )
      {
         detail = BSON( SPT_ERR << "get hostname failed" ) ;
         goto error ;
      }

      rval.setStringVal( "", hostName ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrSystem::sniffPort ( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 port = 0 ;
      BOOLEAN result = FALSE ;
      stringstream ss ;
      BSONObjBuilder builder ;

      if ( 0 == arg.argc() )
      {
         rc = SDB_INVALIDARG ;
         ss << "not specified the port to sniff" ;
         goto error ;
      }
      rc = arg.getNative( 0, &port, SPT_NATIVE_INT32 ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "failed to get port argument: %d", rc ) ;
         ss << "port is not a number" ;
         goto error ;
      }
      {
      PD_LOG ( PDDEBUG, "sniff port is: %d", port ) ;
      _ossSocket sock( port, OSS_ONE_SEC ) ;
      rc = sock.initSocket() ;
      if ( rc )
      {
         PD_LOG ( PDWARNING, "failed to connect to port[%d], "
                  "rc: %d", port, rc ) ;
         ss << "failed to sniff port" ;
         goto error ;
      }
      rc = sock.bind_listen() ;
      if ( rc )
      {
         PD_LOG ( PDDEBUG, "port[%d] is busy, rc: %d", port, rc ) ;
         result = FALSE ;
         rc = SDB_OK ;
      }
      else
      {
         PD_LOG ( PDDEBUG, "port[%d] is usable", port ) ;
         result = TRUE ;
      }
      builder.appendBool( SPT_USR_SYSTEM_USABLE, result ) ;
      rval.setBSONObj( "", builder.obj() ) ;
      sock.close() ;
      }

   done:
      return rc ;
   error:
      detail = BSON( SPT_ERR << ss.str() ) ;
      goto done ;
   }

   INT32 _sptUsrSystem::getPID ( const _sptArguments &arg,
                                 _sptReturnVal &rval,
                                 bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 id = 0 ;
      stringstream ss ;
      BSONObjBuilder builder ;

      if ( 0 < arg.argc() )
      {
         rc = SDB_INVALIDARG ;
         ss << "No need arguments" ;
         goto error ;
      }
      id = ossGetCurrentProcessID() ;
      rval.setNativeVal( "", NumberInt, (const void *)(&id) ) ;
      
   done:
      return rc ;
   error:
      detail = BSON( SPT_ERR << ss.str() ) ;
      goto done ;
   }

   INT32 _sptUsrSystem::getTID ( const _sptArguments &arg,
                                 _sptReturnVal &rval,
                                 bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      UINT32 id = 0 ;
      stringstream ss ;
      BSONObjBuilder builder ;

      if ( 0 < arg.argc() )
      {
         rc = SDB_INVALIDARG ;
         ss << "No need arguments" ;
         goto error ;
      }

      id = (UINT32)ossGetCurrentThreadID() ;
      rval.setNativeVal( "", NumberInt, (const void *)(&id) ) ;

   done:
      return rc ;
   error:
      detail = BSON( SPT_ERR << ss.str() ) ;
      goto done ;
   }

   INT32 _sptUsrSystem::getEWD ( const _sptArguments &arg,
                                 _sptReturnVal &rval,
                                 bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      CHAR buf[ OSS_MAX_PATHSIZE + 1 ] = {0} ;
      stringstream ss ;
      BSONObjBuilder builder ;

      if ( 0 < arg.argc() )
      {
         rc = SDB_INVALIDARG ;
         ss << "No need arguments" ;
         goto error ;
      }

      rc = ossGetEWD( buf, OSS_MAX_PATHSIZE ) ;
      if ( rc )
      {
         ss << "Get current executable file's working directory failed" ;
         goto error ;
      }

      rval.setStringVal( "", buf ) ;

   done:
      return rc ;
   error:
      detail = BSON( SPT_ERR << ss.str() ) ;
      goto done ;
   }

   INT32 _sptUsrSystem::help( const _sptArguments & arg,
                              _sptReturnVal & rval,
                              BSONObj & detail )
   {
      stringstream ss ;
      ss << "System functions:" << endl
         << " System.ping( hostname )" << endl
         << " System.type()" << endl
         << " System.getReleaseInfo()" << endl
         << " System.getHostsMap()" << endl
         << " System.getAHostMap( hostname )" << endl
         << " System.addAHostMap( hostname, ip, [isReplace] )" << endl
         << " System.delAHostMap( hostname )" << endl
         << " System.getCpuInfo()" << endl
         << " System.snapshotCpuInfo()" << endl
         << " System.getMemInfo()" << endl
         << " System.snapshotMemInfo()" << endl
         << " System.getDiskInfo()" << endl
         << " System.snapshotDiskInfo()" << endl
         << " System.getNetcardInfo()" << endl
         << " System.snapshotNetcardInfo()" << endl
         << " System.getIpTablesInfo()" << endl
         << " System.getHostName()" << endl
         << " System.sniffPort( port )" << endl
         << " System.getPID()" << endl
         << " System.getTID()" << endl
         << " System.getEWD()" << endl;
      rval.setStringVal( "", ss.str().c_str() ) ;
      return SDB_OK ;
   }

}

