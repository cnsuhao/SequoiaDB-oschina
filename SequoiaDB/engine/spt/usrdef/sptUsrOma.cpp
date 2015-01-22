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

   Source File Name = sptUsrOma.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          18/08/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "sptUsrOma.hpp"
#include "omagentDef.hpp"
#include "ossUtil.hpp"
#include "utilStr.hpp"
#include "ossProc.hpp"
#include "ossIO.hpp"
#include "msgDef.h"
#include "pmdOptions.h"
#include "utilParam.hpp"
#include "../bson/bsonobj.h"

using namespace bson ;

namespace engine
{
   /*
      Function Define
   */
   JS_CONSTRUCT_FUNC_DEFINE( _sptUsrOma, construct )
   JS_DESTRUCT_FUNC_DEFINE( _sptUsrOma, destruct )
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, toString)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, createCoord)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, removeCoord)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, createData)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, removeData)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, createOM)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, removeOM)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, startNode)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, stopNode)
   JS_MEMBER_FUNC_DEFINE(_sptUsrOma, close)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, help)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, getOmaInstallInfo)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, getOmaInstallFile)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, getOmaConfigFile)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, getOmaConfigs)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, setOmaConfigs)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, getAOmaSvcName)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, addAOmaSvcName)
   JS_STATIC_FUNC_DEFINE(_sptUsrOma, delAOmaSvcName)

   /*
      Function Map
   */
   JS_BEGIN_MAPPING( _sptUsrOma, "Oma" )
      JS_ADD_CONSTRUCT_FUNC( construct )
      JS_ADD_DESTRUCT_FUNC(destruct)
      JS_ADD_MEMBER_FUNC("toString", toString)
      JS_ADD_MEMBER_FUNC("createCoord", createCoord)
      JS_ADD_MEMBER_FUNC("removeCoord", removeCoord)
      JS_ADD_MEMBER_FUNC("createData", createData)
      JS_ADD_MEMBER_FUNC("removeData", removeData)
      JS_ADD_MEMBER_FUNC("createOM", createOM)
      JS_ADD_MEMBER_FUNC("removeOM", removeOM)
      JS_ADD_MEMBER_FUNC("startNode", startNode)
      JS_ADD_MEMBER_FUNC("stopNode", stopNode)
      JS_ADD_MEMBER_FUNC("close", close)
      JS_ADD_STATIC_FUNC("help", help)
      JS_ADD_STATIC_FUNC("getOmaInstallInfo", getOmaInstallInfo)
      JS_ADD_STATIC_FUNC("getOmaInstallFile", getOmaInstallFile)
      JS_ADD_STATIC_FUNC("getOmaConfigFile", getOmaConfigFile)
      JS_ADD_STATIC_FUNC("getOmaConfigs", getOmaConfigs)
      JS_ADD_STATIC_FUNC("setOmaConfigs", setOmaConfigs)
      JS_ADD_STATIC_FUNC("getAOmaSvcName", getAOmaSvcName)
      JS_ADD_STATIC_FUNC("addAOmaSvcName", addAOmaSvcName)
      JS_ADD_STATIC_FUNC("delAOmaSvcName", delAOmaSvcName)
   JS_MAPPING_END()

   #define SPT_OMA_REL_PATH            SDBCM_CONF_DIR_NAME OSS_FILE_SEP
   #define SPT_OMA_REL_PATH_FILE       SPT_OMA_REL_PATH SDBCM_CFG_FILE_NAME

   /*
      define config
   */
   #define MAP_CONFIG_DESC( desc ) \
      desc.add_options() \
      ( SDBCM_RESTART_COUNT, po::value<INT32>(), "" ) \
      ( SDBCM_RESTART_INTERVAL, po::value<INT32>(), "" ) \
      ( SDBCM_AUTO_START, po::value<string>(), "" ) \
      ( SDBCM_DIALOG_LEVEL, po::value<INT32>(), "" ) \
      ( "*", po::value<string>(), "" )

   /*
      _sptUsrOma Implement
   */
   _sptUsrOma::_sptUsrOma()
   {
      CHAR tmpName[ 10 ] = { 0 } ;
      ossSnprintf( tmpName, sizeof( tmpName ) - 1, "%u", SDBCM_DFT_PORT ) ;
      _hostname = "localhost" ;
      _svcname = tmpName ;
   }

   _sptUsrOma::~_sptUsrOma()
   {
   }

   INT32 _sptUsrOma::construct( const _sptArguments & arg,
                                _sptReturnVal & rval,
                                BSONObj & detail )
   {
      INT32 rc = SDB_OK ;

      if ( arg.argc() >= 1 )
      {
         rc = arg.getString( 0, _hostname ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "hostname must be string" ) ;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to get hostname, rc: %d", rc ) ;
      }
      if ( arg.argc() >= 2 )
      {
         rc = arg.getString( 1, _svcname ) ;
         if ( rc )
         {
            UINT16 port = 0 ;
            rc = arg.getNative( 1, (void*)&port, SPT_NATIVE_INT16 ) ;
            if ( rc )
            {
               detail = BSON( SPT_ERR << "svcname must be string or int" ) ;
            }
            else if ( port <= 0 || port >= 65535 )
            {
               detail = BSON( SPT_ERR << "svcname must in range ( 0, 65535 )" ) ;
               rc = SDB_INVALIDARG ;
            }
            else
            {
               _svcname = boost::lexical_cast< string >( port ) ;
            }
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to get svcname, rc: %d", rc ) ;
      }

      rc = _assit.connect( _hostname.c_str(), _svcname.c_str() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to connect %s:%s, rc: %d",
                   _hostname.c_str(), _svcname.c_str(), rc ) ;

      rval.setUsrObjectVal( "", this, SPT_CLASS_DEF( this ) ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::toString( const _sptArguments & arg,
                               _sptReturnVal & rval,
                               BSONObj & detail )
   {
      string name = _hostname ;
      name += ":" ;
      name += _svcname ;
      rval.setStringVal( "", name.c_str() ) ;
      return SDB_OK ;
   }

   INT32 _sptUsrOma::help( const _sptArguments & arg,
                           _sptReturnVal & rval,
                           BSONObj & detail )
   {
      stringstream ss ;
      ss << "Oma functions:" << endl
         << " Oma.getOmaInstallFile()" << endl
         << " Oma.getOmaInstallInfo()" << endl
         << " Oma.getOmaConfigFile()" << endl
         << " Oma.getOmaConfigs( [confFile] )" << endl
         << " Oma.setOmaConfigs( obj, [confFile] )" << endl
         << " Oma.getAOmaSvcName( hostname, [confFile] )" << endl
         << " Oma.addAOmaSvcName( hostname, svcname, [isReplace], [confFile] )"
         << endl
         << " Oma.delAOmaSvcName( hostname, [confFile] )" << endl
         << endl
         << "var oma = new Oma( [hostname], [svcname] )" << endl
         << "   createCoord( svcname, dbpath, [config obj] )" << endl
         << "   removeCoord( svcname )" << endl
         << "   createData( svcname, dbpath, [config obj] )  -standalone" << endl
         << "   removeData( svcname )                       -standalone" << endl
         << "   createOM( svcname, dbpath, [config obj] )" << endl
         << "   removeOM( svcname )" << endl
         << "   startNode( svcname )" << endl
         << "   stopNode( svcname )" << endl
         << "   close()" << endl ;
      rval.setStringVal( "", ss.str().c_str() ) ;
      return SDB_OK ;
   }

   INT32 _sptUsrOma::destruct()
   {
      return _assit.disconnect() ;
   }

   INT32 _sptUsrOma::createCoord( const _sptArguments & arg,
                                  _sptReturnVal & rval,
                                  BSONObj & detail )
   {
      return _createNode( arg, rval, detail, SDB_ROLE_COORD_STR ) ;
   }

   INT32 _sptUsrOma::removeCoord( const _sptArguments & arg,
                                  _sptReturnVal & rval,
                                  BSONObj & detail )
   {
      return _removeNode( arg, rval, detail, SDB_ROLE_COORD_STR ) ;
   }

   INT32 _sptUsrOma::_createNode( const _sptArguments & arg,
                                  _sptReturnVal & rval,
                                  BSONObj & detail,
                                  const CHAR *pNodeStr )
   {
      INT32 rc = SDB_OK ;
      string svcname ;
      string dbpath ;
      BSONObj config ;

      rc = arg.getString( 0, svcname ) ;
      if ( SDB_OUT_OF_BOUND == rc )
      {
         detail = BSON( SPT_ERR << "svcname must be config" ) ;
      }
      else if ( rc )
      {
         UINT16 port = 0 ;
         rc = arg.getNative( 0, (void*)&port, SPT_NATIVE_INT16 ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "svcname must be string or int" ) ;
         }
         else if ( port <= 0 || port >= 65535 )
         {
            detail = BSON( SPT_ERR << "svcname must in range ( 0, 65535 )" ) ;
            rc = SDB_INVALIDARG ;
         }
         else
         {
            svcname = boost::lexical_cast< string >( port ) ;
         }
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get svcname, rc: %d", rc ) ;

      rc = arg.getString( 1, dbpath ) ;
      if ( SDB_OUT_OF_BOUND == rc )
      {
         detail = BSON( SPT_ERR << "dbpath must be config" ) ;
      }
      else if ( rc )
      {
         detail = BSON( SPT_ERR << "dbpath must be string" ) ;
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get dbpath, rc: %d", rc ) ;

      if ( arg.argc() >= 3 )
      {
         rc = arg.getBsonobj( 2, config ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "config must be object" ) ;
         }
         PD_RC_CHECK( rc, PDERROR, "Failed to get config, rc: %d", rc ) ;
      }

      {
         BSONObjBuilder builder ;
         BSONObjIterator it( config ) ;
         while ( it.more() )
         {
            BSONElement e = it.next() ;
            if ( 0 == ossStrcmp( PMD_OPTION_ROLE, e.fieldName() ) )
            {
               continue ;
            }
            builder.append( e ) ;
         }
         builder.append( PMD_OPTION_ROLE, pNodeStr ) ;
         config = builder.obj() ;
      }

      rc = _assit.createNode( svcname.c_str(), dbpath.c_str(),
                              config.objdata() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to create %s[%s], rc: %d",
                   pNodeStr, svcname.c_str(), rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::createData( const _sptArguments & arg,
                                 _sptReturnVal & rval,
                                 BSONObj & detail )
   {
      return _createNode( arg, rval, detail, SDB_ROLE_STANDALONE_STR ) ;
   }

   INT32 _sptUsrOma::_removeNode( const _sptArguments & arg,
                                  _sptReturnVal & rval,
                                  BSONObj & detail,
                                  const CHAR *pNodeStr )
   {
      INT32 rc = SDB_OK ;
      string svcname ;
      BSONObj config = BSON( PMD_OPTION_ROLE << pNodeStr ) ;

      rc = arg.getString( 0, svcname ) ;
      if ( SDB_OUT_OF_BOUND == rc )
      {
         detail = BSON( SPT_ERR << "svcname must be config" ) ;
      }
      else if ( rc )
      {
         UINT16 port = 0 ;
         rc = arg.getNative( 0, (void*)&port, SPT_NATIVE_INT16 ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "svcname must be string or int" ) ;
         }
         else if ( port <= 0 || port >= 65535 )
         {
            detail = BSON( SPT_ERR << "svcname must in range ( 0, 65535 )" ) ;
            rc = SDB_INVALIDARG ;
         }
         else
         {
            svcname = boost::lexical_cast< string >( port ) ;
         }
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get svcname, rc: %d", rc ) ;

      rc = _assit.removeNode( svcname.c_str(), config.objdata() ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to remove %s[%s], rc: %d",
                   pNodeStr, svcname.c_str(), rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::removeData( const _sptArguments & arg,
                                 _sptReturnVal & rval,
                                 BSONObj & detail )
   {
      return _removeNode( arg, rval, detail, SDB_ROLE_STANDALONE_STR ) ;
   }

   INT32 _sptUsrOma::createOM( const _sptArguments & arg,
                               _sptReturnVal & rval,
                               BSONObj & detail )
   {
      return _createNode( arg, rval, detail, SDB_ROLE_OM_STR ) ;
   }

   INT32 _sptUsrOma::removeOM( const _sptArguments & arg,
                               _sptReturnVal & rval,
                               BSONObj & detail )
   {
      return _removeNode( arg, rval, detail, SDB_ROLE_OM_STR ) ;
   }

   INT32 _sptUsrOma::startNode( const _sptArguments & arg,
                                _sptReturnVal & rval,
                                BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string svcname ;

      rc = arg.getString( 0, svcname ) ;
      if ( SDB_OUT_OF_BOUND == rc )
      {
         detail = BSON( SPT_ERR << "svcname must be config" ) ;
      }
      else if ( rc )
      {
         UINT16 port = 0 ;
         rc = arg.getNative( 0, (void*)&port, SPT_NATIVE_INT16 ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "svcname must be string or int" ) ;
         }
         else if ( port <= 0 || port >= 65535 )
         {
            detail = BSON( SPT_ERR << "svcname must in range ( 0, 65535 )" ) ;
            rc = SDB_INVALIDARG ;
         }
         else
         {
            svcname = boost::lexical_cast< string >( port ) ;
         }
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get svcname, rc: %d", rc ) ;

      rc = _assit.startNode( svcname.c_str() ) ;
      PD_RC_CHECK( rc, PDERROR, "Start node[%s] failed, rc: %d",
                   svcname.c_str(), rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::stopNode( const _sptArguments & arg,
                               _sptReturnVal & rval,
                               BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string svcname ;

      rc = arg.getString( 0, svcname ) ;
      if ( SDB_OUT_OF_BOUND == rc )
      {
         detail = BSON( SPT_ERR << "svcname must be config" ) ;
      }
      else if ( rc )
      {
         UINT16 port = 0 ;
         rc = arg.getNative( 0, (void*)&port, SPT_NATIVE_INT16 ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "svcname must be string or int" ) ;
         }
         else if ( port <= 0 || port >= 65535 )
         {
            detail = BSON( SPT_ERR << "svcname must in range ( 0, 65535 )" ) ;
            rc = SDB_INVALIDARG ;
         }
         else
         {
            svcname = boost::lexical_cast< string >( port ) ;
         }
      }
      PD_RC_CHECK( rc, PDERROR, "Failed to get svcname, rc: %d", rc ) ;

      rc = _assit.stopNode( svcname.c_str() ) ;
      PD_RC_CHECK( rc, PDERROR, "Stop node[%s] failed, rc: %d",
                   svcname.c_str(), rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::close( const _sptArguments & arg,
                            _sptReturnVal & rval,
                            BSONObj & detail )
   {
      return _assit.disconnect() ;
   }

   INT32 _sptUsrOma::getOmaInstallInfo( const _sptArguments & arg,
                                        _sptReturnVal & rval,
                                        BSONObj & detail )
   {
      utilInstallInfo info ;
      INT32 rc = utilGetInstallInfo( info ) ;
      if ( rc )
      {
         detail = BSON( SPT_ERR << "Install file is not exist" ) ;
         goto error ;
      }
      else
      {
         BSONObjBuilder builder ;
         builder.append( SDB_INSTALL_RUN_FILED, info._run ) ;
         builder.append( SDB_INSTALL_USER_FIELD, info._user ) ;
         builder.append( SDB_INSTALL_PATH_FIELD, info._path ) ;
         rval.setBSONObj( "", builder.obj() ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::getOmaInstallFile( const _sptArguments & arg,
                                        _sptReturnVal & rval,
                                        BSONObj & detail )
   {
      rval.setStringVal( "", SDB_INSTALL_FILE_NAME ) ;
      return SDB_OK ;
   }

   string _sptUsrOma::_getConfFile()
   {
      utilInstallInfo info ;
      CHAR confFile[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;

      if ( SDB_OK == utilGetInstallInfo( info ) )
      {
         if ( SDB_OK == utilBuildFullPath( info._path.c_str(),
                                           SPT_OMA_REL_PATH_FILE,
                                           OSS_MAX_PATHSIZE,
                                           confFile ) &&
              SDB_OK == ossAccess( confFile ) )
         {
            goto done ;
         }
      }

      ossGetEWD( confFile, OSS_MAX_PATHSIZE ) ;
      utilCatPath( confFile, OSS_MAX_PATHSIZE, SDBCM_CONF_PATH_FILE ) ;

   done:
      return confFile ;
   }

   INT32 _sptUsrOma::_getConfInfo( const string & confFile, BSONObj &conf,
                                   BSONObj & detail, BOOLEAN allowNotExist )
   {
      INT32 rc = SDB_OK ;
      po::options_description desc ;
      po ::variables_map vm ;

      MAP_CONFIG_DESC( desc ) ;

      rc = ossAccess( confFile.c_str() ) ;
      if ( rc )
      {
         if ( allowNotExist )
         {
            rc = SDB_OK ;
            goto done ;
         }
         stringstream ss ;
         ss << "conf file[" << confFile << "] is not exist" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

      rc = utilReadConfigureFile( confFile.c_str(), desc, vm ) ;
      if ( SDB_FNE == rc )
      {
         stringstream ss ;
         ss << "conf file[" << confFile << "] is not exist" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }
      else if ( SDB_PERM == rc )
      {
         stringstream ss ;
         ss << "conf file[" << confFile << "] permission error" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }
      else if ( rc )
      {
         stringstream ss ;
         ss << "read conf file[" << confFile << "] error" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }
      else
      {
         BSONObjBuilder builder ;
         po ::variables_map::iterator it = vm.begin() ;
         while ( it != vm.end() )
         {
            if ( SDBCM_RESTART_COUNT == it->first ||
                 SDBCM_RESTART_INTERVAL == it->first ||
                 SDBCM_DIALOG_LEVEL == it->first )
            {
               builder.append( it->first, it->second.as<INT32>() ) ;
            }
            else if ( SDBCM_AUTO_START == it->first )
            {
               BOOLEAN autoStart = TRUE ;
               ossStrToBoolean( it->second.as<string>().c_str(), &autoStart ) ;
               builder.appendBool( it->first, autoStart ) ;
            }
            else
            {
               builder.append( it->first, it->second.as<string>() ) ;
            }
            ++it ;
         }
         conf = builder.obj() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::getOmaConfigFile( const _sptArguments & arg,
                                       _sptReturnVal & rval,
                                       BSONObj & detail )
   {
      string confFile = _getConfFile() ;
      rval.setStringVal( "", confFile.c_str() ) ;
      return SDB_OK ;
   }

   INT32 _sptUsrOma::getOmaConfigs( const _sptArguments & arg,
                                    _sptReturnVal & rval,
                                    BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string confFile ;
      BSONObj conf ;

      if ( arg.argc() > 0 )
      {
         rc = arg.getString( 0, confFile ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "confFile must be string" ) ;
            goto error ;
         }
      }
      else
      {
         confFile = _getConfFile() ;
      }

      rc = _getConfInfo( confFile, conf, detail ) ;
      if ( rc )
      {
         goto error ;
      }
      rval.setBSONObj( "", conf ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::_confObj2Str( const BSONObj &conf, string &str,
                                   BSONObj &detail,
                                   const CHAR* pExcept )
   {
      INT32 rc = SDB_OK ;
      stringstream ss ;
      BSONObjIterator it ( conf ) ;
      while ( it.more() )
      {
         BSONElement e = it.next() ;

         if ( pExcept && 0 != pExcept[0] &&
              0 == ossStrcmp( pExcept, e.fieldName() ) )
         {
            continue ;
         }

         ss << e.fieldName() << "=" ;
         if ( e.type() == String )
         {
            ss << e.valuestr() ;
         }
         else if ( e.type() == NumberInt )
         {
            ss << e.numberInt() ;
         }
         else if ( e.type() == NumberLong )
         {
            ss << e.numberLong() ;
         }
         else if ( e.type() == NumberDouble )
         {
            ss << e.numberDouble() ;
         }
         else if ( e.type() == Bool )
         {
            ss << ( e.boolean() ? "TRUE" : "FALSE" ) ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
            stringstream errss ;
            errss << e.toString() << " is invalid config" ;
            detail = BSON( SPT_ERR << errss.str() ) ;
            goto error ;
         }
         ss << endl ;
      }
      str = ss.str() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::setOmaConfigs( const _sptArguments & arg,
                                    _sptReturnVal & rval,
                                    BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string confFile ;
      BSONObj conf ;
      string str ;

      rc = arg.getBsonobj( 0, conf ) ;
      if ( SDB_OUT_OF_BOUND == rc )
      {
         detail = BSON( SPT_ERR << "obj must be config" ) ;
         goto error ;
      }
      else if ( rc )
      {
         detail = BSON( SPT_ERR << "obj must be object" ) ;
         goto error ;
      }

      if ( arg.argc() > 1 )
      {
         rc = arg.getString( 1, confFile ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "confFile must be string" ) ;
            goto error ;
         }
      }
      else
      {
         confFile = _getConfFile() ;
      }

      rc = _confObj2Str( conf, str, detail ) ;
      if ( rc )
      {
         goto error ;
      }

      rc = utilWriteConfigFile( confFile.c_str(), str.c_str(), FALSE ) ;
      if ( rc )
      {
         stringstream ss ;
         ss << "write conf file[" << confFile << "] failed" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::getAOmaSvcName( const _sptArguments & arg,
                                     _sptReturnVal & rval,
                                     BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string hostname ;
      string confFile ;
      BSONObj confObj ;

      rc = arg.getString( 0, hostname ) ;
      if ( rc == SDB_OUT_OF_BOUND )
      {
         detail = BSON( SPT_ERR << "hostname must be config" ) ;
         goto error ;
      }
      else if ( rc )
      {
         detail = BSON( SPT_ERR << "hostname must be string" ) ;
         goto error ;
      }

      if ( arg.argc() > 1 )
      {
         rc = arg.getString( 1, confFile ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "confFile must be string" ) ;
            goto error ;
        }
      }
      else
      {
         confFile = _getConfFile() ;
      }

      rc = _getConfInfo( confFile, confObj, detail ) ;
      if ( rc )
      {
         goto error ;
      }
      else
      {
         const CHAR *p = ossStrstr( hostname.c_str(), SDBCM_CONF_PORT ) ;
         if ( !p || ossStrlen( p ) != ossStrlen( SDBCM_CONF_PORT ) )
         {
            hostname += SDBCM_CONF_PORT ;
         }
         BSONElement e = confObj.getField( hostname ) ;
         if ( e.eoo() )
         {
            e = confObj.getField( SDBCM_CONF_DFTPORT ) ;
         }

         if ( e.type() == String )
         {
            rval.setStringVal( "", e.valuestr() ) ;
         }
         else
         {
            stringstream ss ;
            ss << e.toString() << " is invalid" ;
            detail = BSON( SPT_ERR << ss.str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::addAOmaSvcName( const _sptArguments & arg,
                                     _sptReturnVal & rval,
                                     BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string hostname ;
      string svcname ;
      INT32 isReplace = TRUE ;
      string confFile ;
      BSONObj confObj ;
      string str ;

      rc = arg.getString( 0, hostname ) ;
      if ( rc == SDB_OUT_OF_BOUND )
      {
         detail = BSON( SPT_ERR << "hostname must be config" ) ;
         goto error ;
      }
      else if ( rc )
      {
         detail = BSON( SPT_ERR << "hostname must be string" ) ;
         goto error ;
      }
      else if ( hostname.empty() )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "hostname can't be empty" ) ;
         goto error ;
      }

      rc = arg.getString( 1, svcname ) ;
      if ( rc == SDB_OUT_OF_BOUND )
      {
         detail = BSON( SPT_ERR << "svcname must be config" ) ;
         goto error ;
      }
      else if ( rc )
      {
         detail = BSON( SPT_ERR << "svcname must be string" ) ;
         goto error ;
      }
      else if ( svcname.empty() )
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "svcname can't be empty" ) ;
         goto error ;
      }

      if ( arg.argc() > 2 )
      {
         rc = arg.getNative( 2, (void*)&isReplace, SPT_NATIVE_INT32 ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "isReplace must be BOOLEAN" ) ;
            goto error ;
        }
      }

      if ( arg.argc() > 3 )
      {
         rc = arg.getString( 3, confFile ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "confFile must be string" ) ;
            goto error ;
        }
      }
      else
      {
         confFile = _getConfFile() ;
      }

      rc = _getConfInfo( confFile, confObj, detail, TRUE ) ;
      if ( rc )
      {
         goto error ;
      }
      else
      {
         const CHAR *p = ossStrstr( hostname.c_str(), SDBCM_CONF_PORT ) ;
         if ( !p || ossStrlen( p ) != ossStrlen( SDBCM_CONF_PORT ) )
         {
            hostname += SDBCM_CONF_PORT ;
         }
         BSONElement e = confObj.getField( hostname ) ;
         BSONElement e1 = confObj.getField( SDBCM_CONF_DFTPORT ) ;

         if ( e.type() == String )
         {
            if ( 0 == ossStrcmp( e.valuestr(), svcname.c_str() ) )
            {
               goto done ;
            }
            else if ( !isReplace )
            {
               stringstream ss ;
               ss << hostname << " already exist" ;
               detail = BSON( SPT_ERR << ss.str() ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
         }
         else if ( e1.type() == String &&
                   0 == ossStrcmp( e1.valuestr(), svcname.c_str() ) )
         {
            goto done ;
         }
      }

      rc = _confObj2Str( confObj, str, detail, hostname.c_str() ) ;
      if ( rc )
      {
         goto error ;
      }
      str += hostname ;
      str += "=" ;
      str += svcname ;
      str += OSS_NEWLINE ;

      rc = utilWriteConfigFile( confFile.c_str(), str.c_str(), FALSE ) ;
      if ( rc )
      {
         stringstream ss ;
         ss << "write conf file[" << confFile << "] failed" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptUsrOma::delAOmaSvcName( const _sptArguments & arg,
                                     _sptReturnVal & rval,
                                     BSONObj & detail )
   {
      INT32 rc = SDB_OK ;
      string hostname ;
      string confFile ;
      BSONObj confObj ;
      string str ;

      rc = arg.getString( 0, hostname ) ;
      if ( rc == SDB_OUT_OF_BOUND )
      {
         detail = BSON( SPT_ERR << "hostname must be config" ) ;
         goto error ;
      }
      else if ( rc )
      {
         detail = BSON( SPT_ERR << "hostname must be string" ) ;
         goto error ;
      }

      if ( arg.argc() > 1 )
      {
         rc = arg.getString( 1, confFile ) ;
         if ( rc )
         {
            detail = BSON( SPT_ERR << "confFile must be string" ) ;
            goto error ;
        }
      }
      else
      {
         confFile = _getConfFile() ;
      }

      rc = _getConfInfo( confFile, confObj, detail, TRUE ) ;
      if ( rc )
      {
         goto error ;
      }
      else
      {
         const CHAR *p = ossStrstr( hostname.c_str(), SDBCM_CONF_PORT ) ;
         if ( !p || ossStrlen( p ) != ossStrlen( SDBCM_CONF_PORT ) )
         {
            hostname += SDBCM_CONF_PORT ;
         }
         BSONElement e = confObj.getField( hostname ) ;
         if ( e.eoo() )
         {
            goto done ;
         }
      }

      rc = _confObj2Str( confObj, str, detail, hostname.c_str() ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = utilWriteConfigFile( confFile.c_str(), str.c_str(), FALSE ) ;
      if ( rc )
      {
         stringstream ss ;
         ss << "write conf file[" << confFile << "] failed" ;
         detail = BSON( SPT_ERR << ss.str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

}


