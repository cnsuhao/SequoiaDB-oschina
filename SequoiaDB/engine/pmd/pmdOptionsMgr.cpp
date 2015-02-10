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

   Source File Name = pmdOptionsMgr.cpp

   Descriptive Name = Process MoDel Main

   When/how to use: this program may be used for managing sequoiadb
   configuration.It can be initialized from cmd and configure file.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdOptionsMgr.hpp"
#include "pd.hpp"
#include "utilCommon.hpp"
#include "utilStr.hpp"
#include "msg.hpp"
#include "msgCatalog.hpp"
#include "ossMem.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"
#include "ossIO.hpp"
#include "ossVer.hpp"
#include "dpsLogWrapper.hpp"

#include "rtnSortDef.hpp"
#include "clsUtil.hpp"

#include <vector>
#include <boost/algorithm/string.hpp>

using namespace bson ;

namespace engine
{

   #define JUDGE_RC( rc ) if ( SDB_OK != rc ) { goto error ; }

   #define PMD_OPTION_BRK_TIME_DEFAULT (5000)
   #define PMD_MAX_PREF_POOL           (0) // modify 200 to 0
   #define PMD_MAX_SUB_QUERY           (10)
   #define PMD_MIN_SORTBUF_SZ          (RTN_SORT_MIN_BUFSIZE)
   #define PMD_DEFAULT_SORTBUF_SZ      (256)
   #define PMD_DEFAULT_HJ_SZ           (128)
   #define PMD_MIN_HJ_SZ               (64)
   #define PMD_DEFAULT_MAX_REPLSYNC    (10)
   #define PMD_DFT_REPL_BUCKET_SIZE    (32)
   #define PMD_DFT_INDEX_SCAN_STEP     (100)
   #define PMD_DFT_START_SHIFT_TIME    (600)
   #define PMD_DFT_NUMPAGECLEAN        (0)
   #define PMD_MAX_NUMPAGECLEAN        (50)
   #define PMD_DFT_PAGECLEANINTERVAL   (10000)
   #define PMD_MIN_PAGECLEANINTERVAL   (1000)

   /*
      _pmdCfgExchange implement
   */
   _pmdCfgExchange::_pmdCfgExchange( const BSONObj &dataObj,
                                     BOOLEAN load,
                                     PMD_CFG_STEP step )
   :_cfgStep( step ), _isLoad( load ), _dataObj( dataObj )
   {
      _dataType   = PMD_CFG_DATA_BSON ;
      _pVMFile    = NULL ;
      _pVMCmd     = NULL ;
   }

   _pmdCfgExchange::_pmdCfgExchange( po::variables_map *pVMCmd,
                                     po::variables_map * pVMFile,
                                     BOOLEAN load,
                                     PMD_CFG_STEP step )
   :_cfgStep( step ), _isLoad( load ), _pVMFile( pVMFile ), _pVMCmd( pVMCmd )
   {
      _dataType   = PMD_CFG_DATA_CMD ;
   }

   _pmdCfgExchange::~_pmdCfgExchange()
   {
      _pVMFile    = NULL ;
      _pVMCmd     = NULL ;
   }

   INT32 _pmdCfgExchange::readInt( const CHAR * pFieldName, INT32 &value )
   {
      INT32 rc = SDB_OK ;

      if ( PMD_CFG_DATA_BSON == _dataType )
      {
         BSONElement ele = _dataObj.getField( pFieldName ) ;
         if ( ele.eoo() )
         {
            rc = SDB_FIELD_NOT_EXIST ;
         }
         else if ( !ele.isNumber() )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] is not number", pFieldName,
                    ele.type() ) ;
            rc = SDB_INVALIDARG ;
         }
         else
         {
            value = (INT32)ele.numberInt() ;
         }
      }
      else if ( PMD_CFG_DATA_CMD == _dataType )
      {
         if ( _pVMCmd && _pVMCmd->count( pFieldName ) )
         {
            value = (*_pVMCmd)[pFieldName].as<int>() ;
         }
         else if ( _pVMFile && _pVMFile->count( pFieldName ) )
         {
            value = (*_pVMFile)[pFieldName].as<int>() ;
         }
         else
         {
            rc = SDB_FIELD_NOT_EXIST ;
         }
      }
      else
      {
         rc = SDB_SYS ;
      }

      if ( SDB_OK == rc )
      {
         _mapKeyField[ pFieldName ] = pmdParamValue( value, TRUE ) ;
      }

      return rc ;
   }

   INT32 _pmdCfgExchange::readInt( const CHAR * pFieldName, INT32 &value,
                                   INT32 defaultValue )
   {
      INT32 rc = readInt( pFieldName, value ) ;
      if ( SDB_FIELD_NOT_EXIST == rc )
      {
         value = defaultValue ;
         rc = SDB_OK ;
         _mapKeyField[ pFieldName ] = pmdParamValue( value, TRUE ) ;
      }

      return rc ;
   }

   INT32 _pmdCfgExchange::readString( const CHAR *pFieldName, CHAR *pValue,
                                      UINT32 len )
   {
      INT32 rc = SDB_OK ;

      if ( PMD_CFG_DATA_BSON == _dataType )
      {
         BSONElement ele = _dataObj.getField( pFieldName ) ;
         if ( ele.eoo() )
         {
            rc = SDB_FIELD_NOT_EXIST ;
         }
         else if ( String != ele.type() )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] is not string", pFieldName,
                    ele.type() ) ;
            rc = SDB_INVALIDARG ;
         }
         else
         {
            ossStrncpy( pValue, ele.valuestr(), len ) ;
            pValue[ len - 1 ] = 0 ;
         }
      }
      else if ( PMD_CFG_DATA_CMD == _dataType )
      {
         string tmpValue ;
         if ( _pVMCmd && _pVMCmd->count( pFieldName ) )
         {
            tmpValue = (*_pVMCmd)[pFieldName].as<string>() ;
         }
         else if ( _pVMFile && _pVMFile->count( pFieldName ) )
         {
            tmpValue = (*_pVMFile)[pFieldName].as<string>() ;
         }
         else
         {
            rc = SDB_FIELD_NOT_EXIST ;
         }

         if ( SDB_OK == rc )
         {
            ossStrncpy( pValue, tmpValue.c_str(), len ) ;
            pValue[ len - 1 ] = 0 ;
         }
      }
      else
      {
         rc = SDB_SYS ;
      }

      if ( SDB_OK == rc )
      {
         _mapKeyField[ pFieldName ] = pmdParamValue( pValue, TRUE ) ;
      }

      return rc ;
   }

   INT32 _pmdCfgExchange::readString( const CHAR *pFieldName,
                                      CHAR *pValue, UINT32 len,
                                      const CHAR *pDefault )
   {
      INT32 rc = readString( pFieldName, pValue, len ) ;
      if ( SDB_FIELD_NOT_EXIST == rc && pDefault )
      {
         ossStrncpy( pValue, pDefault, len ) ;
         pValue[ len - 1 ] = 0 ;
         rc = SDB_OK ;

         _mapKeyField[ pFieldName ] = pmdParamValue( pValue, TRUE ) ;
      }
      return rc ;
   }

   INT32 _pmdCfgExchange::writeInt( const CHAR * pFieldName, INT32 value )
   {
      INT32 rc = SDB_OK ;

      if ( PMD_CFG_DATA_BSON == _dataType )
      {
         _dataBuilder.append( pFieldName, value ) ;
      }
      else if ( PMD_CFG_DATA_CMD == _dataType )
      {
         _strStream << pFieldName << " = " << value << OSS_NEWLINE ;
      }
      else
      {
         rc = SDB_SYS ;
      }
      return rc ;
   }

   INT32 _pmdCfgExchange::writeString( const CHAR *pFieldName,
                                       const CHAR *pValue )
   {
      INT32 rc = SDB_OK ;

      if ( PMD_CFG_DATA_BSON == _dataType )
      {
         _dataBuilder.append( pFieldName, pValue ) ;
      }
      else if ( PMD_CFG_DATA_CMD == _dataType )
      {
         _strStream << pFieldName << " = " << pValue << OSS_NEWLINE ;
      }
      else
      {
         rc = SDB_SYS ;
      }
      return rc ;
   }

   BOOLEAN _pmdCfgExchange::hasField( const CHAR * pFieldName )
   {
      if ( PMD_CFG_DATA_BSON == _dataType &&
           !_dataObj.getField( pFieldName ).eoo() )
      {
         return TRUE ;
      }
      else if ( PMD_CFG_DATA_CMD == _dataType )
      {
         if ( ( _pVMCmd && _pVMCmd->count( pFieldName ) ) ||
              ( _pVMFile && _pVMFile->count( pFieldName ) ) )
         {
            return TRUE ;
         }
      }

      return FALSE ;
   }

   const CHAR* _pmdCfgExchange::getData( UINT32 & dataLen, MAP_K2V &mapKeyValue )
   {
      MAP_K2V::iterator it ;
      if ( PMD_CFG_DATA_BSON == _dataType )
      {
         it = mapKeyValue.begin() ;
         while ( it != mapKeyValue.end() )
         {
            if ( FALSE == it->second._hasMapped )
            {
               _dataBuilder.append( it->first, it->second._value ) ;
            }
            ++it ;
         }
         _dataObj = _dataBuilder.obj() ;
         dataLen = _dataObj.objsize() ;
         return _dataObj.objdata() ;
      }
      else if ( PMD_CFG_DATA_CMD == _dataType )
      {
         it = mapKeyValue.begin() ;
         while ( it != mapKeyValue.end() )
         {
            if ( FALSE == it->second._hasMapped )
            {
               _strStream << it->first << " = " << it->second._value
                          << OSS_NEWLINE ;
            }
            ++it ;
         }
         _dataStr = _strStream.str() ;
         dataLen = _dataStr.size() ;
         return _dataStr.c_str() ;
      }
      return NULL ;
   }

   void _pmdCfgExchange::_makeKeyValueMap( po::variables_map *pVM )
   {
      po::variables_map::iterator it = pVM->begin() ;
      MAP_K2V::iterator itKV ;
      while ( it != pVM->end() )
      {
         itKV = _mapKeyField.find( it->first ) ;
         if ( itKV != _mapKeyField.end() && TRUE == itKV->second._hasMapped )
         {
            ++it ;
            continue ;
         }
         try
         {
            _mapKeyField[ it->first ] = pmdParamValue( it->second.as<string>(),
                                                       FALSE ) ;
            ++it ;
            continue ;
         }
         catch( std::exception )
         {
         }

         try
         {
            _mapKeyField[ it->first ] = pmdParamValue( it->second.as<int>(),
                                                       FALSE ) ;
            ++it ;
            continue ;
         }
         catch( std::exception )
         {
         }

         ++it ;
      }
   }

   MAP_K2V _pmdCfgExchange::getKVMap()
   {
      if ( PMD_CFG_DATA_CMD == _dataType )
      {
         if ( _pVMFile )
         {
            _makeKeyValueMap( _pVMFile ) ;
         }
         if ( _pVMCmd )
         {
            _makeKeyValueMap( _pVMCmd ) ;
         }
      }
      else
      {
         MAP_K2V::iterator itKV ;
         BSONObjIterator it( _dataObj ) ;
         while ( it.more() )
         {
            BSONElement e = it.next() ;

            itKV = _mapKeyField.find( e.fieldName() ) ;
            if ( itKV != _mapKeyField.end() &&
                 TRUE == itKV->second._hasMapped )
            {
               ++it ;
               continue ;
            }

            if ( String == e.type() )
            {
               _mapKeyField[ e.fieldName() ] = pmdParamValue( e.valuestrsafe(),
                                                              FALSE ) ;
            }
            else if ( e.isNumber() )
            {
               _mapKeyField[ e.fieldName() ] = pmdParamValue( e.numberInt(),
                                                              FALSE ) ;
            }
         }
      }

      return _mapKeyField ;
   }

   /*
      _pmdCfgRecord implement
   */
   _pmdCfgRecord::_pmdCfgRecord ()
   {
      _result = SDB_OK ;
      _changeID = 0 ;
      _pConfigHander = NULL ;
   }
   _pmdCfgRecord::~_pmdCfgRecord ()
   {
   }

   void _pmdCfgRecord::setConfigHandler( IConfigHandle * pConfigHandler )
   {
      _pConfigHander = pConfigHandler ;
   }

   IConfigHandle* _pmdCfgRecord::getConfigHandler() const
   {
      return _pConfigHander ;
   }

   INT32 _pmdCfgRecord::restore( const BSONObj & objData,
                                 po::variables_map *pVMCMD )
   {
      MAP_K2V mapKeyValue ;
      MAP_K2V::iterator it ;
      pmdCfgExchange ex( objData, TRUE, PMD_CFG_STEP_INIT ) ;
      INT32 rc = doDataExchange( &ex ) ;
      if ( rc )
      {
         goto error ;
      }
      else if ( pVMCMD )
      {
         pmdCfgExchange ex1( pVMCMD, NULL, TRUE, PMD_CFG_STEP_REINIT ) ;
         rc = doDataExchange( &ex1 ) ;
         if ( rc )
         {
            goto error ;
         }
         mapKeyValue = ex1.getKVMap() ;
      }
      rc = postLoaded() ;
      if ( rc )
      {
         goto error ;
      }

      _mapKeyValue = ex.getKVMap() ;
      it = mapKeyValue.begin() ;
      while ( it != mapKeyValue.end() )
      {
         _mapKeyValue[ it->first ] = it->second ;
         ++it ;
      }

      if ( getConfigHandler() )
      {
         rc = getConfigHandler()->onConfigInit() ;
         if ( rc )
         {
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdCfgRecord::change( const BSONObj &objData )
   {
      INT32 rc = SDB_OK ;
      BSONObj oldCfg ;
      MAP_K2V mapKeyField ;
      MAP_K2V::iterator it ;
      pmdCfgExchange ex( objData, TRUE, PMD_CFG_STEP_CHG ) ;

      rc = toBSON( oldCfg ) ;
      PD_RC_CHECK( rc, PDERROR, "Save old config failed, rc: %d", rc ) ;
      rc = doDataExchange( &ex ) ;
      if ( rc )
      {
         goto restore ;
      }
      rc = postLoaded() ;
      if ( rc )
      {
         goto restore ;
      }
      ++_changeID ;

      mapKeyField = ex.getKVMap() ;
      it = mapKeyField.begin() ;
      while ( it != mapKeyField.end() )
      {
         _mapKeyValue[ it->first ] = it->second ;
         ++it ;
      }

      if ( getConfigHandler() )
      {
         getConfigHandler()->onConfigChange( getChangeID() ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   restore:
      restore( oldCfg, NULL ) ;
      goto done ;
   }

   INT32 _pmdCfgRecord::init( po::variables_map *pVMFile,
                              po::variables_map *pVMCMD )
   {
      pmdCfgExchange ex( pVMCMD, pVMFile, TRUE, PMD_CFG_STEP_INIT ) ;
      INT32 rc = doDataExchange( &ex ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = postLoaded() ;
      if ( rc )
      {
         goto error ;
      }
      ++_changeID ;

      _mapKeyValue = ex.getKVMap() ;

      if ( getConfigHandler() )
      {
         rc = getConfigHandler()->onConfigInit() ;
         if ( rc )
         {
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdCfgRecord::toBSON( BSONObj & objData )
   {
      INT32 rc = preSaving() ;
      if ( SDB_OK == rc )
      {
         pmdCfgExchange ex( BSONObj(), FALSE, PMD_CFG_STEP_INIT ) ;
         rc = doDataExchange( &ex ) ;
         if ( SDB_OK == rc )
         {
            UINT32 dataLen = 0 ;
            try
            {
               objData = BSONObj( ex.getData( dataLen,
                                  _mapKeyValue ) ).getOwned() ;
            }
            catch( std::exception &e )
            {
               PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
               rc = SDB_SYS ;
            }
         }
      }
      return rc ;
   }

   INT32 _pmdCfgRecord::toString( string & str )
   {
      INT32 rc = preSaving() ;
      if ( SDB_OK == rc )
      {
         pmdCfgExchange ex( NULL, NULL, FALSE, PMD_CFG_STEP_INIT ) ;
         INT32 rc = doDataExchange( &ex ) ;
         if ( SDB_OK == rc )
         {
            UINT32 dataLen = 0 ;
            str = ex.getData( dataLen, _mapKeyValue ) ;
         }
      }
      return rc ;
   }

   INT32 _pmdCfgRecord::postLoaded()
   {
      return SDB_OK ;
   }

   INT32 _pmdCfgRecord::preSaving()
   {
      return SDB_OK ;
   }

   INT32 _pmdCfgRecord::parseAddressLine( const CHAR * pAddressLine,
                                          vector < _pmdCfgRecord::pmdAddrPair > & vecAddr,
                                          const CHAR * pItemSep,
                                          const CHAR * pInnerSep )
   {
      INT32 rc = SDB_OK ;
      vector<string> addrs ;
      pmdAddrPair addrItem ;

      if ( !pAddressLine || !pItemSep || !pInnerSep ||
           pItemSep[ 0 ] == 0 || pInnerSep[ 0 ] == 0 )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else if ( pAddressLine[ 0 ] == 0 )
      {
         goto done ;
      }

      boost::algorithm::split( addrs, pAddressLine,
                               boost::algorithm::is_any_of(
                               pItemSep ) ) ;
      if ( CLS_REPLSET_MAX_NODE_SIZE < addrs.size() )
      {
         std::cerr << "addr more than max member size" << endl ;
         goto error ;
      }

      for ( vector<string>::iterator itr = addrs.begin() ;
            itr != addrs.end() ;
            ++itr )
      {
         vector<string> pair ;
         string tmp = *itr ;
         boost::algorithm::trim( tmp ) ;
         boost::algorithm::split( pair, tmp,
                                  boost::algorithm::is_any_of(
                                  pInnerSep ) ) ;
         if ( pair.size() != 2 )
         {
            continue ;
         }
         UINT32 cpLen = pair.at(0).size() < OSS_MAX_HOSTNAME ?
                        pair.at(0).size() : OSS_MAX_HOSTNAME ;
         ossMemcpy( addrItem._host, pair.at(0).c_str(), cpLen ) ;
         addrItem._host[cpLen] = '\0' ;
         cpLen = pair.at(1).size() < OSS_MAX_SERVICENAME ?
                 pair.at(1).size() : OSS_MAX_SERVICENAME ;
         ossMemcpy( addrItem._service, pair.at(1).c_str(),cpLen ) ;
         addrItem._service[cpLen] = '\0' ;
         vecAddr.push_back( addrItem ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   string _pmdCfgRecord::makeAddressLine( vector < _pmdCfgRecord::pmdAddrPair > & vecAddr,
                                          CHAR chItemSep,
                                          CHAR chInnerSep )
   {
      UINT32 count = 0 ;
      stringstream ss ;
      for ( UINT32 i = 0; i < vecAddr.size() ; ++i )
      {
         pmdAddrPair &item = vecAddr[ i ] ;
         if ( '\0' != item._host[ 0 ] )
         {
            if ( 0 != count )
            {
               ss << chItemSep ;
            }
            ss << item._host << chInnerSep << item._service ;
            ++count ;
         }
         else
         {
            break ;
         }
      }
      return ss.str() ;
   }

   BOOLEAN _pmdCfgRecord::hasField( const CHAR * pFieldName )
   {
      if ( _mapKeyValue.find( pFieldName ) == _mapKeyValue.end() )
      {
         return FALSE ;
      }
      return TRUE ;
   }

   INT32 _pmdCfgRecord::getFieldInt( const CHAR * pFieldName,
                                     INT32 &value,
                                     INT32 *pDefault )
   {
      INT32 rc = SDB_OK ;

      if ( !hasField( pFieldName ) )
      {
         if ( pDefault )
         {
            value = *pDefault ;
         }
         else
         {
            rc = SDB_FIELD_NOT_EXIST ;
         }
      }
      else
      {
         value = ossAtoi( _mapKeyValue[ pFieldName ]._value.c_str() ) ;
      }

      return rc ;
   }

   INT32 _pmdCfgRecord::getFieldStr( const CHAR * pFieldName,
                                     CHAR *pValue, UINT32 len,
                                     const CHAR * pDefault )
   {
      INT32 rc = SDB_OK ;
      string tmpValue ;

      if ( !hasField( pFieldName ) )
      {
         if ( pDefault )
         {
            tmpValue = *pDefault ;
         }
         else
         {
            rc = SDB_FIELD_NOT_EXIST ;
         }
      }
      else
      {
         tmpValue = _mapKeyValue[ pFieldName ]._value ;
      }

      if ( SDB_OK == rc )
      {
         ossStrncpy( pValue, tmpValue.c_str(), len ) ;
         pValue[ len - 1 ] = 0 ;
      }

      return rc ;
   }

   INT32 _pmdCfgRecord::rdxString( pmdCfgExchange *pEX, const CHAR *pFieldName,
                                   CHAR *pValue, UINT32 len, BOOLEAN required,
                                   BOOLEAN allowRunChg,
                                   const CHAR *pDefaultValue,
                                   BOOLEAN hideParam )
   {
      if ( _result )
      {
         goto error ;
      }
      _curFieldName = pFieldName ;

      if ( pEX->isLoad() )
      {
         if ( PMD_CFG_STEP_REINIT == pEX->getCfgStep() &&
              !pEX->hasField( pFieldName ) )
         {
            goto done ;
         }
         else if ( PMD_CFG_STEP_CHG == pEX->getCfgStep() )
         {
            if ( FALSE == allowRunChg )
            {
               if ( pEX->hasField( pFieldName ) )
               {
                  _result = SDB_PERM ;
                  PD_LOG ( PDWARNING, "Field[%s] do not support changing in "
                           "runtime", pFieldName ) ;
                  goto error ;
               }
               goto done ;
            }
            if ( !pEX->hasField( pFieldName ) )
            {
               goto done ;
            }
         }

         if ( required )
         {
            _result = pEX->readString( pFieldName, pValue, len ) ;
         }
         else
         {
            _result = pEX->readString( pFieldName, pValue, len, pDefaultValue ) ;
         }
         if ( SDB_FIELD_NOT_EXIST == _result )
         {
            ossPrintf( "Error: Field[%s] not config\n", pFieldName ) ;
         }
         else if ( _result )
         {
            ossPrintf( "Error: Read field[%s] config failed, rc: %d\n",
                       pFieldName, _result ) ;
         }
      }
      else
      {
         if ( hideParam && 0 == ossStrcmp( pValue, pDefaultValue ) )
         {
            goto done ;
         }
         _result = pEX->writeString( pFieldName, pValue ) ;
         if ( _result )
         {
            PD_LOG( PDWARNING, "Write field[%s] failed, rc: %d",
                    pFieldName, _result ) ;
         }
      }

   done:
      return _result ;
   error:
      goto done ;
   }

   INT32 _pmdCfgRecord::rdxPath( pmdCfgExchange *pEX, const CHAR *pFieldName,
                                 CHAR *pValue, UINT32 len, BOOLEAN required,
                                 BOOLEAN allowRunChg,
                                 const CHAR *pDefaultValue,
                                 BOOLEAN hideParam )
   {
      _result = rdxString( pEX, pFieldName, pValue, len, required, allowRunChg,
                           pDefaultValue, hideParam ) ;
      if ( SDB_OK == _result && pEX->isLoad() && 0 != pValue[0] )
      {
         std::string strTmp = pValue ;
         if ( NULL == ossGetRealPath( strTmp.c_str(), pValue, len ) )
         {
            ossPrintf( "Error: Failed to get real path for %s:%s\n",
                       pFieldName, strTmp.c_str() ) ;
            _result = SDB_INVALIDARG ;
         }
         else
         {
            pValue[ len - 1 ] = 0 ;
         }
      }
      return _result ;
   }

   INT32 _pmdCfgRecord::rdxBooleanS( pmdCfgExchange *pEX,
                                     const CHAR *pFieldName,
                                     BOOLEAN &value,
                                     BOOLEAN required,
                                     BOOLEAN allowRunChg,
                                     BOOLEAN defaultValue,
                                     BOOLEAN hideParam )
   {
      CHAR szTmp[ PMD_MAX_ENUM_STR_LEN + 1 ] = {0} ;
      ossStrcpy( szTmp, value ? "TRUE" : "FALSE" ) ;
      _result = rdxString( pEX, pFieldName, szTmp, sizeof(szTmp), required,
                           allowRunChg, defaultValue ? "TRUE" : "FALSE",
                           hideParam ) ;
      if ( SDB_OK == _result && pEX->isLoad() )
      {
         ossStrToBoolean( szTmp, &value ) ;
      }
      return _result ;
   }

   INT32 _pmdCfgRecord::rdxInt( pmdCfgExchange *pEX, const CHAR *pFieldName,
                                INT32 &value, BOOLEAN required,
                                BOOLEAN allowRunChg, INT32 defaultValue,
                                BOOLEAN hideParam )
   {
      if ( _result )
      {
         goto error ;
      }
      _curFieldName = pFieldName ;

      if ( pEX->isLoad() )
      {
         if ( PMD_CFG_STEP_REINIT == pEX->getCfgStep() &&
              !pEX->hasField( pFieldName ) )
         {
            goto done ;
         }
         else if ( PMD_CFG_STEP_CHG == pEX->getCfgStep() )
         {
            if ( FALSE == allowRunChg )
            {
               if ( pEX->hasField( pFieldName ) )
               {
                  _result = SDB_PERM ;
                  PD_LOG ( PDWARNING, "Field[%s] do not support changing in "
                           "runtime", pFieldName ) ;
                  goto error ;
               }
               goto done ;
            }
            if ( !pEX->hasField( pFieldName ) )
            {
               goto done ;
            }
         }

         if ( required )
         {
            _result = pEX->readInt( pFieldName, value ) ;
         }
         else
         {
            _result = pEX->readInt( pFieldName, value, defaultValue ) ;
         }
         if ( SDB_FIELD_NOT_EXIST == _result )
         {
            ossPrintf( "Error: Field[%s] not config\n", pFieldName ) ;
         }
         else if ( _result )
         {
            ossPrintf( "Error: Read field[%s] config failed, rc: %d\n",
                       pFieldName, _result ) ;
         }
      }
      else
      {
         if ( hideParam && value == defaultValue )
         {
            goto done ;
         }
         _result = pEX->writeInt( pFieldName, value ) ;
         if ( _result )
         {
            PD_LOG( PDWARNING, "Write field[%s] failed, rc: %d",
                    pFieldName, _result ) ;
         }
      }

   done:
      return _result ;
   error:
      goto done ;
   }

   INT32 _pmdCfgRecord::rdxUInt( pmdCfgExchange *pEX, const CHAR *pFieldName,
                                 UINT32 &value, BOOLEAN required,
                                 BOOLEAN allowRunChg, UINT32 defaultValue,
                                 BOOLEAN hideParam )
   {
      INT32 tmpValue = (INT32)value ;
      _result = rdxInt( pEX, pFieldName, tmpValue, required, allowRunChg,
                        (INT32)defaultValue, hideParam ) ;
      if ( SDB_OK == _result && pEX->isLoad() )
      {
         value = (UINT32)tmpValue ;
      }
      return _result ;
   }

   INT32 _pmdCfgRecord::rdxShort( pmdCfgExchange *pEX, const CHAR *pFieldName,
                                  INT16 &value, BOOLEAN required,
                                  BOOLEAN allowRunChg, INT16 defaultValue,
                                  BOOLEAN hideParam )
   {
      INT32 tmpValue = (INT32)value ;
      _result = rdxInt( pEX, pFieldName, tmpValue, required, allowRunChg,
                        (INT32)defaultValue, hideParam ) ;
      if ( SDB_OK == _result && pEX->isLoad() )
      {
         value = (INT16)tmpValue ;
      }
      return _result ;
   }

   INT32 _pmdCfgRecord::rdxUShort( pmdCfgExchange *pEX, const CHAR *pFieldName,
                                   UINT16 &value, BOOLEAN required,
                                   BOOLEAN allowRunChg, UINT16 defaultValue,
                                   BOOLEAN hideParam )
   {
      INT32 tmpValue = (INT32)value ;
      _result = rdxInt( pEX, pFieldName, tmpValue, required, allowRunChg,
                        (INT32)defaultValue, hideParam ) ;
      if ( SDB_OK == _result && pEX->isLoad() )
      {
         value = (UINT16)tmpValue ;
      }
      return _result ;
   }

   INT32 _pmdCfgRecord::rdvMinMax( pmdCfgExchange *pEX, UINT32 &value,
                                   UINT32 minV, UINT32 maxV,
                                   BOOLEAN autoAdjust )
   {
      if ( _result )
      {
         goto error ;
      }

      if ( !pEX->isLoad() )
      {
         goto done ;
      }

      if ( value < minV )
      {
         ossPrintf( "Waring: Field[%s] value[%u] is less than min value[%u]\n",
                    _curFieldName.c_str(), value, minV ) ;
         if ( autoAdjust )
         {
            value = minV ;
         }
         else
         {
            _result = SDB_INVALIDARG ;
            goto error ;
         }
      }
      else if ( value > maxV )
      {
         ossPrintf( "Waring: Field[%s] value[%u] is more than max value[%u]\n",
                 _curFieldName.c_str(), value, maxV ) ;
         if ( autoAdjust )
         {
            value = maxV ;
         }
         else
         {
            _result = SDB_INVALIDARG ;
            goto error ;
         }
      }

   done:
      return _result ;
   error:
      goto done ;
   }

   INT32 _pmdCfgRecord::rdvMinMax( pmdCfgExchange *pEX, UINT16 &value,
                                   UINT16 minV, UINT16 maxV,
                                   BOOLEAN autoAdjust )
   {
      if ( _result )
      {
         goto error ;
      }

      if ( !pEX->isLoad() )
      {
         goto done ;
      }

      if ( value < minV )
      {
         ossPrintf( "Waring: Field[%s] value[%u] is less than min value[%u]\n",
                    _curFieldName.c_str(), value, minV ) ;
         if ( autoAdjust )
         {
            value = minV ;
         }
         else
         {
            _result = SDB_INVALIDARG ;
            goto error ;
         }
      }
      else if ( value > maxV )
      {
         ossPrintf( "Waring: Field[%s] value[%u] is more than max value[%u]\n",
                    _curFieldName.c_str(), value, maxV ) ;
         if ( autoAdjust )
         {
            value = maxV ;
         }
         else
         {
            _result = SDB_INVALIDARG ;
            goto error ;
         }
      }

   done:
      return _result ;
   error:
      goto done ;
   }

   INT32 _pmdCfgRecord::rdvMaxChar( pmdCfgExchange *pEX, CHAR *pValue,
                                    UINT32 maxChar, BOOLEAN autoAdjust )
   {
      UINT32 len = 0 ;

      if ( _result )
      {
         goto error ;
      }
      if ( !pEX->isLoad() )
      {
         goto done ;
      }

      len = ossStrlen( pValue ) ;
      if ( len > maxChar )
      {
         ossPrintf( "Waring: Field[%s] value[%s] length more than [%u]\n",
                    _curFieldName.c_str(), pValue, maxChar ) ;
         if ( autoAdjust )
         {
            pValue[ maxChar ] = 0 ;
         }
         else
         {
            _result = SDB_INVALIDARG ;
            goto error ;
         }
      }

   done:
      return _result ;
   error:
      goto done ;
   }

   INT32 _pmdCfgRecord::rdvNotEmpty( pmdCfgExchange * pEX, CHAR *pValue )
   {
      if ( _result )
      {
         goto error ;
      }
      if ( !pEX->isLoad() )
      {
         goto done ;
      }

      if ( ossStrlen( pValue ) == 0 )
      {
         ossPrintf( "Waring: Field[%s] is empty\n", _curFieldName.c_str() ) ;
         _result = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return _result ;
   error:
      goto done ;
   }

   /*
      _pmdOptionsMgr implement
   */
   _pmdOptionsMgr::_pmdOptionsMgr()
   {
      ossMemset( _krcbDbPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _krcbIndexPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _krcbDiagLogPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _krcbLogPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _krcbBkupPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _krcbWWWPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _krcbSvcName, 0, OSS_MAX_SERVICENAME + 1) ;
      ossMemset( _replServiceName, 0, OSS_MAX_SERVICENAME + 1) ;
      ossMemset( _catServiceName, 0, OSS_MAX_SERVICENAME + 1) ;
      ossMemset( _shardServiceName, 0, OSS_MAX_SERVICENAME + 1) ;
      ossMemset( _restServiceName, 0, OSS_MAX_SERVICENAME + 1) ;
      ossMemset( _omServiceName, 0, OSS_MAX_SERVICENAME + 1 ) ;
      ossMemset( _krcbRole, 0, PMD_MAX_ENUM_STR_LEN + 1) ;
      ossMemset( _syncStrategyStr, 0, PMD_MAX_ENUM_STR_LEN + 1) ;
      ossMemset( _prefReplStr, 0, PMD_MAX_ENUM_STR_LEN + 1 ) ;
      ossMemset( _catAddrLine, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _dmsTmpBlkPath, 0, OSS_MAX_PATHSIZE + 1 ) ;
      ossMemset( _krcbLobPath, 0, OSS_MAX_PATHSIZE + 1 ) ;

      _krcbMaxPool         = 0 ;
      _krcbDiagLvl         = (UINT16)PDWARNING ;
      _logFileSz           = 0 ;
      _logFileNum          = 0 ;
      _numPreLoaders       = 0 ;
      _maxPrefPool         = PMD_MAX_PREF_POOL ;
      _maxSubQuery         = PMD_MAX_SUB_QUERY ;
      _maxReplSync         = PMD_DEFAULT_MAX_REPLSYNC ;
      _syncStrategy        = CLS_SYNC_NONE ;
      _preferReplica       = PREFER_REPL_ANYONE ;
      _replBucketSize      = PMD_DFT_REPL_BUCKET_SIZE ;
      _memDebugEnabled     = FALSE ;
      _memDebugSize        = 0 ;
      _indexScanStep       = PMD_DFT_INDEX_SCAN_STEP ;
      _dpslocal            = FALSE ;
      _traceOn             = FALSE ;
      _traceBufSz          = TRACE_DFT_BUFFER_SIZE ;
      _transactionOn       = FALSE ;
      _sharingBreakTime    = PMD_OPTION_BRK_TIME_DEFAULT ;
      _startShiftTime      = PMD_DFT_START_SHIFT_TIME ;
      _logBuffSize         = DPS_DFT_LOG_BUF_SZ ;
      _sortBufSz           = PMD_DEFAULT_SORTBUF_SZ ;
      _hjBufSz             = PMD_DEFAULT_HJ_SZ ;
      _pagecleanNum        = PMD_DFT_NUMPAGECLEAN ;
      _pagecleanInterval   = PMD_DFT_PAGECLEANINTERVAL ;
      _dialogFileNum       = 0 ;
      _directIOInLob       = FALSE ;
      _sparseFile          = FALSE ;
      _weight              = 0 ; 

      ossMemset( _krcbConfPath, 0, sizeof( _krcbConfPath ) ) ;
      ossMemset( _krcbConfFile, 0, sizeof( _krcbConfFile ) ) ;
      ossMemset( _krcbCatFile, 0, sizeof( _krcbCatFile ) ) ;
      _krcbSvcPort         = OSS_DFT_SVCPORT ;
   }

   _pmdOptionsMgr::~_pmdOptionsMgr()
   {
   }

   INT32 _pmdOptionsMgr::doDataExchange( pmdCfgExchange * pEX )
   {
      resetResult () ;

      rdxPath( pEX, PMD_OPTION_CONFPATH , _krcbConfPath, sizeof(_krcbConfPath),
               FALSE, FALSE, PMD_CURRENT_PATH ) ;
      rdxPath( pEX, PMD_OPTION_DBPATH, _krcbDbPath, sizeof(_krcbDbPath),
               FALSE, FALSE, PMD_CURRENT_PATH ) ;
      rdvNotEmpty( pEX, _krcbDbPath ) ;
      rdxPath( pEX, PMD_OPTION_IDXPATH, _krcbIndexPath, sizeof(_krcbIndexPath),
               FALSE, FALSE, "" ) ;
      rdxPath( pEX, PMD_OPTION_DIAGLOGPATH, _krcbDiagLogPath,
               sizeof(_krcbDiagLogPath), FALSE, FALSE, "" ) ;
      rdxPath( pEX, PMD_OPTION_LOGPATH, _krcbLogPath, sizeof(_krcbLogPath),
               FALSE, FALSE, "" ) ;
      rdxPath( pEX, PMD_OPTION_BKUPPATH, _krcbBkupPath, sizeof(_krcbBkupPath),
               FALSE, FALSE, "" ) ;
      rdxPath( pEX, PMD_OPTION_WWWPATH, _krcbWWWPath, sizeof(_krcbWWWPath),
               FALSE, FALSE, "", TRUE ) ;

      rdxPath( pEX, PMD_OPTION_LOBPATH, _krcbLobPath, sizeof(_krcbLobPath),
               FALSE, FALSE, "" ) ;

      rdxUInt( pEX, PMD_OPTION_MAXPOOL, _krcbMaxPool, FALSE, TRUE, 0 ) ;
      rdxInt( pEX, PMD_OPTION_DIAGLOG_NUM, _dialogFileNum, FALSE, TRUE,
              PD_DFT_FILE_NUM ) ;
      rdxString( pEX, PMD_OPTION_SVCNAME, _krcbSvcName, sizeof(_krcbSvcName),
                 FALSE, FALSE,
                 boost::lexical_cast<string>(OSS_DFT_SVCPORT).c_str() ) ;
      rdvNotEmpty( pEX, _krcbSvcName ) ;
      rdxString( pEX, PMD_OPTION_REPLNAME, _replServiceName,
                 sizeof(_replServiceName), FALSE, FALSE, "" ) ;
      rdxString( pEX, PMD_OPTION_CATANAME, _catServiceName,
                 sizeof(_catServiceName), FALSE, FALSE, "" ) ;
      rdxString( pEX, PMD_OPTION_SHARDNAME, _shardServiceName,
                 sizeof(_shardServiceName), FALSE, FALSE, "" ) ;
      rdxString( pEX, PMD_OPTION_RESTNAME, _restServiceName,
                 sizeof(_restServiceName), FALSE, FALSE, "" ) ;
      rdxString( pEX, PMD_OPTION_OMNAME, _omServiceName,
                 sizeof(_omServiceName), FALSE, FALSE, "", TRUE ) ;
      rdxUShort( pEX, PMD_OPTION_DIAGLEVEL, _krcbDiagLvl, FALSE, TRUE,
                 (UINT16)PDWARNING ) ;
      rdvMinMax( pEX, _krcbDiagLvl, PDSEVERE, PDDEBUG, TRUE ) ;
      rdxString( pEX, PMD_OPTION_ROLE, _krcbRole, sizeof(_krcbRole),
                 FALSE, FALSE, SDB_ROLE_STANDALONE_STR ) ;
      rdvNotEmpty( pEX, _krcbRole ) ;
      rdxUInt( pEX, PMD_OPTION_LOGFILESZ, _logFileSz, FALSE, FALSE,
               PMD_DFT_LOG_FILE_SZ ) ;
      rdvMinMax( pEX, _logFileSz, PMD_MIN_LOG_FILE_SZ, PMD_MAX_LOG_FILE_SZ,
                 TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_LOGFILENUM, _logFileNum, FALSE, FALSE,
               PMD_DFT_LOG_FILE_NUM ) ;
      rdvMinMax( pEX, _logFileNum, 1, 60000, TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_LOGBUFFSIZE, _logBuffSize, FALSE, TRUE,
               DPS_DFT_LOG_BUF_SZ ) ;
      rdvMinMax( pEX, _logBuffSize, 512, 1024000, TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_NUMPRELOAD, _numPreLoaders, FALSE, TRUE, 0 ) ;
      rdvMinMax( pEX, _numPreLoaders, 0, 100, TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_MAX_PREF_POOL, _maxPrefPool, FALSE, TRUE,
               PMD_MAX_PREF_POOL ) ;
      rdvMinMax( pEX, _maxPrefPool, 0, 1000, TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_MAX_SUB_QUERY, _maxSubQuery, FALSE, TRUE,
               PMD_MAX_SUB_QUERY <= _maxPrefPool ?
               PMD_MAX_SUB_QUERY : _maxPrefPool ) ;
      rdvMinMax( pEX, _maxSubQuery, 0, _maxPrefPool, TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_MAX_REPL_SYNC, _maxReplSync, FALSE, TRUE,
               PMD_DEFAULT_MAX_REPLSYNC ) ;
      rdvMinMax( pEX, _maxReplSync, 0, 200, TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_REPL_BUCKET_SIZE, _replBucketSize, FALSE, FALSE,
               PMD_DFT_REPL_BUCKET_SIZE, TRUE ) ;
      rdvMinMax( pEX, _replBucketSize, 1, 4096, TRUE ) ;
      rdxString( pEX, PMD_OPTION_SYNC_STRATEGY, _syncStrategyStr,
                 sizeof( _syncStrategyStr ), FALSE, TRUE, "", FALSE ) ;
      rdxString( pEX, PMD_OPTION_PREFINST, _prefReplStr, sizeof(_prefReplStr),
                 FALSE, TRUE, "A" ) ;
      rdxBooleanS( pEX, PMD_OPTION_MEMDEBUG, _memDebugEnabled, FALSE, TRUE,
                   FALSE, TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_MEMDEBUGSIZE, _memDebugSize, FALSE, TRUE, 0,
               TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_INDEX_SCAN_STEP, _indexScanStep, FALSE, TRUE,
               PMD_DFT_INDEX_SCAN_STEP, TRUE ) ;
      rdvMinMax( pEX, _indexScanStep, 1, 10000, TRUE ) ;
      rdxBooleanS( pEX, PMD_OPTION_DPSLOCAL, _dpslocal, FALSE, TRUE, FALSE,
                   TRUE ) ;
      rdxBooleanS( pEX, PMD_OPTION_TRACEON, _traceOn, FALSE, TRUE, FALSE,
                   TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_TRACEBUFSZ, _traceBufSz, FALSE, TRUE,
               TRACE_DFT_BUFFER_SIZE, TRUE ) ;
      rdvMinMax( pEX, _traceBufSz, TRACE_MIN_BUFFER_SIZE,
                 TRACE_MAX_BUFFER_SIZE, TRUE ) ;
      rdxBooleanS( pEX, PMD_OPTION_TRANSACTIONON, _transactionOn, FALSE,
                   TRUE, FALSE ) ;
      rdxUInt( pEX, PMD_OPTION_SHARINGBRK, _sharingBreakTime, FALSE, TRUE,
               PMD_OPTION_BRK_TIME_DEFAULT, TRUE ) ;
      rdvMinMax( pEX, _sharingBreakTime, PMD_OPTION_BRK_TIME_DEFAULT,
                 300000, TRUE ) ;
      rdxUInt( pEX, PMD_OPTION_START_SHIFT_TIME, _startShiftTime, FALSE, TRUE,
               PMD_DFT_START_SHIFT_TIME, TRUE ) ;
      rdvMinMax( pEX, _startShiftTime, 0, 7200, TRUE ) ;
      rdxString( pEX, PMD_OPTION_CATALOG_ADDR, _catAddrLine,
                 sizeof(_catAddrLine), FALSE, TRUE, "" ) ;

      rdxPath( pEX, PMD_OPTION_DMS_TMPBLKPATH, _dmsTmpBlkPath,
               sizeof(_dmsTmpBlkPath), FALSE, FALSE, "" ) ;

      rdxUInt( pEX, PMD_OPTION_SORTBUF_SIZE, _sortBufSz,
               FALSE, TRUE, PMD_DEFAULT_SORTBUF_SZ ) ;
      rdvMinMax( pEX, _sortBufSz, PMD_MIN_SORTBUF_SZ,
                 -1, TRUE ) ;

      rdxUInt( pEX, PMD_OPTION_HJ_BUFSZ, _hjBufSz,
               FALSE, TRUE, PMD_DEFAULT_HJ_SZ ) ;
      rdvMinMax( pEX, _hjBufSz, PMD_MIN_HJ_SZ,
                 -1, TRUE ) ;

      rdxUInt( pEX, PMD_OPTION_NUMPAGECLEANERS, _pagecleanNum,
               FALSE, FALSE, PMD_DFT_NUMPAGECLEAN ) ;
      rdvMinMax( pEX, _pagecleanNum, 0, PMD_MAX_NUMPAGECLEAN, TRUE ) ;

      rdxUInt( pEX, PMD_OPTION_PAGECLEANINTERVAL, _pagecleanInterval,
               FALSE, TRUE, PMD_DFT_PAGECLEANINTERVAL ) ;
      rdvMinMax ( pEX, _pagecleanInterval, PMD_MIN_PAGECLEANINTERVAL,
                  -1, TRUE ) ;

      rdxBooleanS( pEX, PMD_OPTION_DIRECT_IO_IN_LOB, _directIOInLob,
                   FALSE, TRUE, FALSE, FALSE ) ;

      rdxBooleanS( pEX, PMD_OPTION_SPARSE_FILE, _sparseFile,
                   FALSE, TRUE, FALSE, FALSE ) ;

      rdxUInt( pEX, PMD_OPTION_WEIGHT, _weight,
               FALSE, TRUE, 10, FALSE ) ; 
      rdvMinMax( pEX, _weight, 1, 100, TRUE ) ;

      return getResult () ;
   }

   INT32 _pmdOptionsMgr::postLoaded ()
   {
      INT32 rc = SDB_OK ;
      SDB_ROLE dbRole = SDB_ROLE_STANDALONE ;

      ossGetPort( _krcbSvcName, _krcbSvcPort ) ;
      if ( 0 == _krcbSvcPort )
      {
         std::cerr << "Invalid svcname: " << _krcbSvcName << endl ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( _logBuffSize > _logFileNum * _logFileSz *
           ( 1048576 / DPS_DEFAULT_PAGE_SIZE ) )
      {
         std::cerr << "log buff size more than all log file size: "
                   << _logFileNum * _logFileSz << "MB" << std::endl ;
         _logBuffSize = _logFileNum * _logFileSz *
                        ( 1048576 / DPS_DEFAULT_PAGE_SIZE ) ;
      }

      if ( SDB_ROLE_MAX == ( dbRole = utilGetRoleEnum( _krcbRole ) ) )
      {
         std::cerr << "db role: " << _krcbRole << " error" << std::endl ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( !ossIsPowerOf2( _replBucketSize ) )
      {
         _replBucketSize = PMD_DFT_REPL_BUCKET_SIZE ;
      }

      if ( SDB_OK != clsString2Strategy( _syncStrategyStr, _syncStrategy ) )
      {
         std::cerr << PMD_OPTION_SYNC_STRATEGY << " value error, use default"
                   << endl ;
         _syncStrategy = CLS_SYNC_DTF_STRATEGY ;
      }
      _syncStrategyStr[0] = 0 ;

      _preferReplica = utilPrefReplStr2Enum( _prefReplStr ) ;

      if ( 0 == ossStrlen( _replServiceName ) )
      {
         string port = boost::lexical_cast<string>( _krcbSvcPort
                                                    + PMD_REPL_PORT ) ;
         ossMemcpy( _replServiceName, port.c_str(), port.size() ) ;
      }
      if ( 0 == ossStrlen( _shardServiceName ) )
      {
         string port = boost::lexical_cast<string>( _krcbSvcPort
                                                    + PMD_SHARD_PORT ) ;
         ossMemcpy( _shardServiceName, port.c_str(), port.size() ) ;
      }
      if ( 0 == ossStrlen( _catServiceName ) )
      {
         string port = boost::lexical_cast<string>( _krcbSvcPort
                                                    + PMD_CAT_PORT ) ;
         ossMemcpy( _catServiceName, port.c_str(), port.size() ) ;
      }
      if ( 0 == ossStrlen( _restServiceName ) )
      {
         string port = boost::lexical_cast<string>( _krcbSvcPort
                                                    + PMD_REST_PORT ) ;
         ossMemcpy( _restServiceName, port.c_str(), port.size() ) ;
      }
      if ( 0 == ossStrlen( _omServiceName ) )
      {
         string port = boost::lexical_cast<string>( _krcbSvcPort
                                                    + PMD_OM_PORT ) ;
         ossMemcpy( _omServiceName, port.c_str(), port.size() ) ;
      }

      if ( 0 == _krcbIndexPath[0] )
      {
         ossStrcpy( _krcbIndexPath, _krcbDbPath ) ;
      }
      if ( 0 == _krcbDiagLogPath[0] )
      {
         if ( SDB_OK != utilBuildFullPath( _krcbDbPath, PMD_OPTION_DIAG_PATH,
                                           OSS_MAX_PATHSIZE,
                                           _krcbDiagLogPath ) )
         {
            std::cerr << "diaglog path is too long!" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
      }
      if ( 0 == _krcbLogPath[0] )
      {
         if ( SDB_OK != utilBuildFullPath( _krcbDbPath, PMD_OPTION_LOG_PATH,
                                           OSS_MAX_PATHSIZE, _krcbLogPath ) )
         {
            std::cerr << "repicalog path is too long!" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
      }
      if ( 0 == _krcbBkupPath[0] )
      {
         if ( SDB_OK != utilBuildFullPath( _krcbDbPath, PMD_OPTION_BK_PATH,
                                           OSS_MAX_PATHSIZE, _krcbBkupPath ) )
         {
            std::cerr << "bakup path is too long!" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
      }
      if ( 0 == _krcbWWWPath[0] )
      {
         if ( !_exePath.empty() )
         {
            rc = utilBuildFullPath( _exePath.c_str(),
                                    ".."OSS_FILE_SEP PMD_OPTION_WWW_PATH_DIR,
                                    OSS_MAX_PATHSIZE, _krcbWWWPath ) ;
         }
         else
         {
            rc = utilBuildFullPath( PMD_CURRENT_PATH,
                                    PMD_OPTION_WWW_PATH_DIR,
                                    OSS_MAX_PATHSIZE, _krcbWWWPath ) ;
         }
         if ( SDB_OK != rc )
         {
            std::cerr << "www path is too long!" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
      }

      if ( 0 == _krcbLobPath[0] )
      {
         ossStrcpy( _krcbLobPath, _krcbDbPath ) ;
      }

      if ( 0 == _dmsTmpBlkPath[0] )
      {
         if ( SDB_OK != utilBuildFullPath( _krcbDbPath, PMD_OPTION_TMPBLK_PATH,
                                           OSS_MAX_PATHSIZE, _dmsTmpBlkPath ) )
         {
            std::cerr << "diaglog path is too long!" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
      }
      else
      {
         if ( 0 == ossStrcmp( _krcbDbPath, _dmsTmpBlkPath))
         {
            std::cerr << "tmp path and data path should not be the same" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
         if ( 0 == ossStrcmp( _krcbIndexPath, _dmsTmpBlkPath))
         {
            std::cerr << "tmp path and index path should not be the same" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
         if ( 0 == ossStrcmp( _krcbLogPath, _dmsTmpBlkPath))
         {
            std::cerr << "tmp path and log path should not be the same" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
         if ( 0 == ossStrcmp( _krcbBkupPath, _dmsTmpBlkPath))
         {
            std::cerr << "tmp path and bkup path should not be the same" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
         if ( 0 == ossStrcmp( _krcbDiagLogPath, _dmsTmpBlkPath))
         {
            std::cerr << "tmp path and diaglog path should not be the same" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
         if ( 0 == ossStrcmp( _krcbWWWPath, _dmsTmpBlkPath ) )
         {
            std::cerr << "tmp path and www path should not be the same" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
         if ( 0 == ossStrcmp( _krcbLobPath, _dmsTmpBlkPath ) )
         {
            std::cerr << "tmp path and lob path should not be the same" << endl ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
      }

      if ( _memDebugSize != 0 )
      {
         _memDebugSize = OSS_MIN ( _memDebugSize, SDB_MEMDEBUG_MAXGUARDSIZE ) ;
         _memDebugSize = OSS_MAX ( _memDebugSize, SDB_MEMDEBUG_MINGUARDSIZE ) ;
      }

      rc = parseAddressLine( _catAddrLine, _vecCat ) ;
      if ( rc )
      {
         goto error ;
      }

      if ( SDB_ROLE_STANDALONE == dbRole && _transactionOn )
      {
         _dpslocal = TRUE ;
      }

      if ( SDB_ROLE_CATALOG == dbRole || SDB_ROLE_OM == dbRole )
      {
         _numPreLoaders    = 0 ;
         _maxPrefPool      = 0 ;
         _maxSubQuery      = 0 ;
         _maxReplSync      = 0 ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdOptionsMgr::preSaving ()
   {
      string addr = makeAddressLine( _vecCat ) ;
      ossStrncpy( _catAddrLine, addr.c_str(), OSS_MAX_PATHSIZE ) ;
      _catAddrLine[ OSS_MAX_PATHSIZE ] = 0 ;

      clsStrategy2String( _syncStrategy, _syncStrategyStr,
                          sizeof( _syncStrategyStr ) ) ;

      utilPrefReplEnum2Str( _preferReplica, _prefReplStr,
                            sizeof(_prefReplStr) ) ;

      return SDB_OK ;
   }

   INT32 _pmdOptionsMgr::initFromFile( const CHAR * pConfigFile,
                                       BOOLEAN allowFileNotExist )
   {
      INT32 rc = SDB_OK ;
      po::options_description desc ( "Command options" ) ;
      po::variables_map vm ;

      PMD_ADD_PARAM_OPTIONS_BEGIN( desc )
         PMD_COMMANDS_OPTIONS
         PMD_HIDDEN_COMMANDS_OPTIONS
      PMD_ADD_PARAM_OPTIONS_END

      rc = utilReadConfigureFile( pConfigFile, desc, vm ) ;
      if ( rc )
      {
         if ( SDB_FNE != rc || !allowFileNotExist )
         {
            PD_LOG( PDERROR, "Failed to read config from file[%s], rc: %d",
                    pConfigFile, rc ) ;
            goto error ;
         }
      }

      rc = pmdCfgRecord::init( &vm, NULL ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Init config record failed, rc: %d", rc ) ;
         goto error ;
      }
      ossStrncpy( _krcbConfFile, pConfigFile, OSS_MAX_PATHSIZE ) ;
      if ( ossStrrchr( pConfigFile, OSS_FILE_SEP_CHAR ) )
      {
         const CHAR *pLastSep = ossStrrchr( pConfigFile, OSS_FILE_SEP_CHAR ) ;
         ossStrncpy( _krcbConfPath, pConfigFile,
                     OSS_MAX_PATHSIZE <= pLastSep - pConfigFile ?
                     OSS_MAX_PATHSIZE : pLastSep - pConfigFile ) ;
      }
      else
      {
         ossStrcpy( _krcbConfPath, PMD_CURRENT_PATH ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDOPTMGR_INIT, "_pmdOptionsMgr::init" )
   INT32 _pmdOptionsMgr::init( INT32 argc, CHAR **argv,
                               const std::string &exePath )
   {
      INT32 rc = SDB_OK;
      PD_TRACE_ENTRY ( SDB__PMDOPTMGR_INIT ) ;

      CHAR cfgTempPath[ OSS_MAX_PATHSIZE + 1 ] = {0} ;
      po::options_description all ( "Command options" ) ;
      po::options_description display ( "Command options (display)" ) ;
      po::variables_map vm ;
      po::variables_map vm2 ;

      PMD_ADD_PARAM_OPTIONS_BEGIN( all )
         PMD_COMMANDS_OPTIONS
         PMD_HIDDEN_COMMANDS_OPTIONS
         ( PMD_OPTION_HELPFULL, "help all configs" ) \
         ( "*", po::value<string>(), "" )
      PMD_ADD_PARAM_OPTIONS_END

      PMD_ADD_PARAM_OPTIONS_BEGIN( display )
         PMD_COMMANDS_OPTIONS
      PMD_ADD_PARAM_OPTIONS_END

      _exePath = exePath ;

      rc = utilReadCommandLine( argc, argv, all, vm );
      JUDGE_RC( rc )

      if ( vm.count( PMD_OPTION_HELP ) )
      {
         std::cout << display << std::endl ;
         rc = SDB_PMD_HELP_ONLY ;
         goto done ;
      }
      if ( vm.count( PMD_OPTION_HELPFULL ) )
      {
         std::cout << all << std::endl ;
         rc = SDB_PMD_HELP_ONLY ;
         goto done ;
      }
      if ( vm.count( PMD_OPTION_VERSION ) )
      {
         ossPrintVersion( "SequoiaDB version" ) ;
         rc = SDB_PMD_VERSION_ONLY ;
         goto done ;
      }

      if ( vm.count( PMD_OPTION_CONFPATH ) )
      {
         CHAR *p = ossGetRealPath( vm[PMD_OPTION_CONFPATH].as<string>().c_str(),
                                   cfgTempPath, OSS_MAX_PATHSIZE ) ;
         if ( NULL == p )
         {
            std::cerr << "ERROR: Failed to get real path for "
                      <<  vm[PMD_OPTION_CONFPATH].as<string>().c_str() << endl ;
            rc = SDB_INVALIDPATH ;
            goto error;
         }
      }
      else
      {
         CHAR *p = ossGetRealPath( PMD_CURRENT_PATH, cfgTempPath,
                                   OSS_MAX_PATHSIZE ) ;
         if ( NULL == p )
         {
            SDB_ASSERT( FALSE, "impossible" ) ;
            rc = SDB_INVALIDPATH ;
            goto error ;
         }
      }

      rc = utilBuildFullPath( cfgTempPath, PMD_DFT_CONF,
                              OSS_MAX_PATHSIZE, _krcbConfFile ) ;
      if ( rc )
      {
         std::cerr << "ERROR: Failed to make config file name: " << rc << endl ;
         goto error ;
      }
      rc = utilBuildFullPath( cfgTempPath, PMD_DFT_CAT,
                              OSS_MAX_PATHSIZE, _krcbCatFile ) ;
      if ( rc )
      {
         std::cerr << "ERROR: Failed to make cat file name: " << rc << endl ;
         goto error ;
      }

      rc = utilReadConfigureFile( _krcbConfFile, all, vm2 ) ;
      if ( SDB_OK != rc )
      {
         if ( vm.count( PMD_OPTION_CONFPATH ) )
         {
            std::cerr << "Read config file: " << _krcbConfFile
                      << " failed, rc: " << rc << std::endl ;
            goto error ;
         }
         else if ( SDB_FNE != rc )
         {
            std::cerr << "Read default config file: " << _krcbConfFile
                      << " failed, rc: " << rc << std::endl ;
            goto error ;
         }
         else
         {
            std::cout << "Using default config" << std::endl ;
            rc = SDB_OK ;
         }
      }

      rc = pmdCfgRecord::init( &vm2, &vm ) ;
      if ( rc )
      {
         goto error ;
      }
      else
      {
         ossStrcpy( _krcbConfPath, cfgTempPath ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__PMDOPTMGR_INIT, rc );
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdOptionsMgr::removeAllDir()
   {
      INT32 rc = SDB_OK ;

      if ( _dmsTmpBlkPath[ 0 ] != 0 && 0 == ossAccess( _dmsTmpBlkPath ) )
      {
         rc = ossDelete( _dmsTmpBlkPath ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove dir[%s] failed, rc: %d",
                      _dmsTmpBlkPath, rc ) ;
      }

      if ( _krcbBkupPath[ 0 ] != 0 && 0 == ossAccess( _krcbBkupPath ) )
      {
         rc = ossDelete( _krcbBkupPath ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove dir[%s] failed, rc: %d",
                      _krcbBkupPath, rc ) ;
     }

      if ( _krcbLogPath[ 0 ] != 0 && 0 == ossAccess( _krcbLogPath ) )
      {
         rc = ossDelete( _krcbLogPath ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove dir[%s] failed, rc: %d",
                      _krcbLogPath, rc ) ;
     }

      if ( _krcbDiagLogPath[ 0 ] != 0 && 0 == ossAccess( _krcbDiagLogPath ) )
      {
         rc = ossDelete( _krcbDiagLogPath ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove dir[%s] failed, rc: %d",
                      _krcbDiagLogPath, rc ) ;
     }

      if ( _krcbLobPath[ 0 ] != 0 && 0 == ossAccess( _krcbLobPath ) )
      {
         rc = ossDelete( _krcbLobPath ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove dir[%s] failed, rc: %d",
                      _krcbLobPath, rc ) ;
     }

      if ( _krcbIndexPath[ 0 ] != 0 && 0 == ossAccess( _krcbIndexPath ) )
      {
         rc = ossDelete( _krcbIndexPath ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove dir[%s] failed, rc: %d",
                      _krcbIndexPath, rc ) ;
     }

      if ( _krcbDbPath[ 0 ] != 0 && 0 == ossAccess( _krcbDbPath ) )
      {
         rc = ossDelete( _krcbDbPath ) ;
         PD_RC_CHECK( rc, PDERROR, "Remove dir[%s] failed, rc: %d",
                      _krcbDbPath, rc ) ;
     }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDOPTMGR__MKDIR, "_pmdOptionsMgr::makeAllDir" )
   INT32 _pmdOptionsMgr::makeAllDir()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDOPTMGR__MKDIR );

      rc = ossMkdir( _krcbDbPath, OSS_CREATE|OSS_READWRITE ) ;
      if ( rc && SDB_FE != rc )
      {
         std::cerr << "Failed to create database dir: " << _krcbDbPath <<
                      ", rc = " << rc << std::endl ;
         goto error ;
      }
      rc = ossMkdir( _krcbIndexPath, OSS_CREATE|OSS_READWRITE ) ;
      if ( rc && SDB_FE != rc )
      {
         std::cerr << "Failed to create index dir: " << _krcbIndexPath <<
                      ", rc = " << rc << std::endl ;
         goto error ;
      }
      rc = ossMkdir( _krcbDiagLogPath, OSS_CREATE|OSS_READWRITE ) ;
      if ( rc && SDB_FE != rc )
      {
         std::cerr << "Failed to create diaglog dir: " << _krcbDiagLogPath <<
                      ", rc = " << rc << std::endl ;
         goto error ;
      }

      rc = ossMkdir( _krcbLogPath, OSS_CREATE|OSS_READWRITE ) ;
      if ( rc && SDB_FE != rc )
      {
         std::cerr << "Failed to create log dir: " << _krcbLogPath <<
                      ", rc = " << rc << std::endl ;
         goto error ;
      }

      rc = ossMkdir( _krcbBkupPath, OSS_CREATE|OSS_READWRITE ) ;
      if ( rc && SDB_FE != rc )
      {
         PD_LOG ( PDERROR, "Failed to create backup dir: %s, rc = %d",
                  _krcbBkupPath, rc ) ;
         goto error ;
      }

      rc = ossMkdir( _dmsTmpBlkPath, OSS_CREATE|OSS_READWRITE ) ;
      if ( rc && SDB_FE != rc )
      {
         PD_LOG ( PDERROR, "Failed to create tmp dir: %s, rc = %d",
                  _krcbBkupPath, rc ) ;
         goto error ;
      }

      rc = ossMkdir( _krcbLobPath, OSS_CREATE|OSS_READWRITE ) ;
      if ( rc && SDB_FE != rc )
      {
         PD_LOG ( PDERROR, "Failed to create lob dir: %s, rc = %d",
                  _krcbLobPath, rc ) ;
         goto error ;
      }

      rc = SDB_OK ;
   done:
      PD_TRACE_EXITRC ( SDB__PMDOPTMGR__MKDIR, rc );
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDOPTMGR_REFLUSH2FILE, "_pmdOptionsMgr::reflush2file" )
   INT32 _pmdOptionsMgr::reflush2File()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY (SDB__PMDOPTMGR_REFLUSH2FILE ) ;
      std::string line ;
      CHAR conf[ OSS_MAX_PATHSIZE + 1 ] = {0} ;

      rc = pmdCfgRecord::toString( line ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Failed to get the line str:%d", rc ) ;
         goto error ;
      }

      rc = utilBuildFullPath( _krcbConfPath, PMD_DFT_CONF,
                              OSS_MAX_PATHSIZE, conf ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "Failed to build full path of configure file, "
                 "rc: %d", rc ) ;
         goto error;
      }

      rc = utilWriteConfigFile( conf, line.c_str(), FALSE ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to write config[%s], rc: %d",
                   conf, rc ) ;

   done:
      PD_TRACE_EXIT ( SDB__PMDOPTMGR_REFLUSH2FILE) ;
      return rc ;
   error:
      goto done ;
   }

   void _pmdOptionsMgr::clearCatAddr()
   {
      _vecCat.clear() ;
   }

   void _pmdOptionsMgr::setCatAddr( const CHAR *host,
                                    const CHAR *service )
   {
      BOOLEAN hasSet = FALSE ;

      for ( UINT32 i = 0 ; i < _vecCat.size() ; ++i )
      {
         if ( '\0' == _vecCat[i]._host[0] )
         {
            ossStrncpy( _vecCat[i]._host, host, OSS_MAX_HOSTNAME ) ;
            _vecCat[i]._host[ OSS_MAX_HOSTNAME ] = 0 ;
            ossStrncpy( _vecCat[i]._service, service, OSS_MAX_SERVICENAME ) ;
            _vecCat[i]._service[ OSS_MAX_SERVICENAME ] = 0 ;
            hasSet = TRUE ;
            break ;
         }
      }

      if ( !hasSet && _vecCat.size() < CATA_NODE_MAX_NUM )
      {
         pmdAddrPair addr ;
         ossStrncpy( addr._host, host, OSS_MAX_HOSTNAME ) ;
         addr._host[ OSS_MAX_HOSTNAME ] = 0 ;
         ossStrncpy( addr._service, service, OSS_MAX_SERVICENAME ) ;
         addr._service[ OSS_MAX_SERVICENAME ] = 0 ;
         _vecCat.push_back( addr ) ;
      }
   }

}


