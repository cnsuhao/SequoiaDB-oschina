
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

   Source File Name = omConfigGenerator.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          10/15/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/


#include "omConfigGenerator.hpp"
#include "omDef.hpp"
#include "pd.hpp"
#include "ossUtil.hpp"
#include "pmdOptions.hpp"
#include "pmdEDU.hpp"
#include "pmd.hpp"
#include "pmdOptionsMgr.hpp"

using namespace bson ;

namespace engine
{
   #define OM_GENERATOR_DOT               ","
   #define OM_GENERATOR_LINE              "-"


   #define OM_DG_NAME_PATTERN             "group"

   #define OM_DEPLOY_MOD_STANDALONE       "standalone"
   #define OM_DEPLOY_MOD_DISTRIBUTION     "distribution"

   #define OM_NODE_ROLE_STANDALONE        SDB_ROLE_STANDALONE_STR
   #define OM_NODE_ROLE_COORD             SDB_ROLE_COORD_STR
   #define OM_NODE_ROLE_CATALOG           SDB_ROLE_CATALOG_STR
   #define OM_NODE_ROLE_DATA              SDB_ROLE_DATA_STR

   #define OM_SVCNAME_STEP                (10)
   #define OM_PATH_LENGTH                 (256)
   #define OM_INT32_MAXVALUE_STR          "2147483647"

   #define OM_CONF_VALUE_INT_TYPE         "int"

   static string strPlus( const string &addend, INT32 augend ) ;
   static string strPlus( INT32 addend, INT32 augend ) ;
   static string strConnect( const string &left, INT32 right ) ;
   static string trimLeft( string &str, const string &trimer ) ;
   static string trimRight( string &str, const string &trimer ) ;
   static string trim( string &str ) ;
   static INT32 getValueAsString( const BSONObj &bsonTemplate, 
                                  const string &fieldName, string &value ) ;

   INT32 getValueAsString( const BSONObj &bsonTemplate, 
                           const string &fieldName, string &value )
   {
      INT32 rc = SDB_OK ;
      BSONElement element = bsonTemplate.getField( fieldName ) ;
      if ( element.eoo() )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( String == element.type() )
      {
         value = element.String() ;
      }
      else if ( NumberInt == element.type() )
      {
         CHAR tmp[20] ;
         ossSnprintf( tmp, sizeof(tmp), "%d", element.Int() ) ;
         value = string( tmp ) ;
      }
      else if ( NumberLong == element.type() )
      {
         CHAR tmp[40] ;
         ossSnprintf( tmp, sizeof(tmp), OSS_LL_PRINT_FORMAT, element.Long() ) ;
         value = string( tmp ) ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   string strPlus( const string &addend, INT32 augend )
   {
      INT32 total = ossAtoi( addend.c_str() ) + augend ;
      CHAR result[ OM_INT32_LENGTH + 1 ] ;
      ossItoa( total, result, OM_INT32_LENGTH ) ;

      return string( result ) ;
   }

   string strPlus( INT32 addend, INT32 augend )
   {
      INT32 total = addend + augend ;
      CHAR result[ OM_INT32_LENGTH + 1 ] ;
      ossItoa( total, result, OM_INT32_LENGTH ) ;

      return string( result ) ;
   }

   string strConnect( const string &left, INT32 right )
   {
      CHAR result[ OM_INT32_LENGTH + 1 ] ;
      ossItoa( right, result, OM_INT32_LENGTH ) ;

      return ( left + result ) ;
   }

   string trimLeft( string &str, const string &trimer )
   {
      str.erase( 0, str.find_first_not_of( trimer ) ) ;
      return str ;
   }

   string trimRight( string &str, const string &trimer )
   {
      string::size_type pos = str.find_last_not_of( trimer ) ;
      if ( pos == string::npos )
      {
         str.erase( 0 ) ;
      }
      else
      {
         str.erase( pos + 1 ) ;
      }

      return str ;
   }

   string trim( string &str )
   {
      trimLeft( str, " " ) ;
      trimRight( str, " " ) ;

      return str ;
   }

   nodeCounter::nodeCounter()
   {
   }

   nodeCounter::~nodeCounter()
   {
   }

   void nodeCounter::increaseNode( const string &role )
   {
      map<string, UINT32>::iterator iter = _mapCounter.find( role ) ;
      if ( iter == _mapCounter.end() )
      {
         _mapCounter[ role ] = 0 ;
      }

      _mapCounter[ role ] += 1 ;
   }

   INT32 nodeCounter::getNodeCount( const string &role )
   {
      map<string, UINT32>::iterator iter = _mapCounter.find( role ) ;
      if ( iter != _mapCounter.end() )
      {
         return iter->second ;
      }

      return 0 ;
   }

   INT32 nodeCounter::getNodeCount()
   {
      map<string, UINT32>::iterator iter = _mapCounter.begin() ;
      INT32 total = 0 ;
      while ( iter != _mapCounter.end() )
      {
         total += iter->second ;
         iter++ ;
      }

      return total ;
   }

   businessNodeCounter::businessNodeCounter( const string &businessName )
                       :_businessName( businessName )
   {
   }

   businessNodeCounter::~businessNodeCounter()
   {
   }

   void businessNodeCounter::addNode( const string &role )
   {
      _counter.increaseNode( role ) ;
   }


   diskNodeCounter::diskNodeCounter( const string &diskName )
                   :_diskName(diskName)
   {

   }

   diskNodeCounter::~diskNodeCounter()
   {
      map<string, businessNodeCounter*>::iterator iter ;
      iter = _mapBusinessCounter.begin() ;
      while ( iter != _mapBusinessCounter.end() )
      {
         SDB_OSS_DEL iter->second ;
         _mapBusinessCounter.erase( iter++ ) ;
      }

      _mapBusinessCounter.clear() ;
   }

   INT32 diskNodeCounter::addNode( const string &businessName, 
                                   const string &role )
   {
      INT32 rc = SDB_OK ;

      map<string, businessNodeCounter*>::iterator iter ;
      iter = _mapBusinessCounter.find( businessName ) ;
      if ( iter == _mapBusinessCounter.end() )
      {
         businessNodeCounter *bnc = SDB_OSS_NEW businessNodeCounter( 
                                                                businessName ) ;
         if ( NULL == bnc )
         {
            rc = SDB_OOM ;
            PD_LOG_MSG( PDERROR, "out of memory" ) ;      
            goto error ;
         }

         _mapBusinessCounter.insert( 
               map<string, businessNodeCounter*>::value_type( businessName, 
                                                              bnc ) ) ;
      }

      _mapBusinessCounter[businessName]->addNode( role ) ;
      _counter.increaseNode( role ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 diskNodeCounter::getNodeCount()
   {
      return _counter.getNodeCount() ;
   }

   INT32 diskNodeCounter::getNodeCount( const string &role )
   {
      return _counter.getNodeCount( role ) ;
   }

   hostNodeCounter::hostNodeCounter( const string &hostName )
                   :_hostName( hostName )
   {
   }

   hostNodeCounter::~hostNodeCounter()
   {
      map<string, diskNodeCounter*>::iterator iter ;
      iter = _mapDiskCounter.begin() ;
      while ( iter != _mapDiskCounter.end() )
      {
         SDB_OSS_DEL iter->second ;
         _mapDiskCounter.erase( iter++ ) ;
      }

      _mapDiskCounter.clear() ;
   }

   INT32 hostNodeCounter::addNode( const string &diskName,
                                   const string &businessName, 
                                   const string &role )
   {
      INT32 rc = SDB_OK ;
      map<string, diskNodeCounter *>::iterator iter ;
      iter = _mapDiskCounter.find( diskName ) ;
      if ( iter == _mapDiskCounter.end() )
      {
         diskNodeCounter *dnc = SDB_OSS_NEW diskNodeCounter( diskName ) ;
         if ( NULL == dnc )
         {
            rc = SDB_OOM ;
            PD_LOG_MSG( PDERROR, "out of memory" ) ;      
            goto error ;
         }

         _mapDiskCounter.insert( 
               map<string, diskNodeCounter*>::value_type( diskName, dnc ) ) ;
      }

      _mapDiskCounter[diskName]->addNode( businessName, role ) ;
      _counter.increaseNode( role ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 hostNodeCounter::getNodeCount( const string &role )
   {
      return _counter.getNodeCount( role ) ;
   }

   INT32 hostNodeCounter::getNodeCount()
   {
      return _counter.getNodeCount() ;
   }

   INT32 hostNodeCounter::getNodeCountInDisk( const string &diskName )
   {  
      INT32 count = 0 ;
      map<string, diskNodeCounter *>::iterator iter ;
      iter = _mapDiskCounter.find( diskName ) ;
      if ( iter != _mapDiskCounter.end() )
      {
         diskNodeCounter *p = iter->second ;
         count = p->getNodeCount() ;
      }

      return count ;
   }

   INT32 hostNodeCounter::getNodeCountInDisk( const string &diskName, 
                                              const string &role )
   {
      INT32 count = 0 ;
      map<string, diskNodeCounter *>::iterator iter ;
      iter = _mapDiskCounter.find( diskName ) ;
      if ( iter != _mapDiskCounter.end() )
      {
         diskNodeCounter *p = iter->second ;
         count = p->getNodeCount( role ) ;
      }

      return count ;
   }

   clusterNodeCounter::clusterNodeCounter()
                      :_availableGroupID( 1 )
   {
   }

   clusterNodeCounter::~clusterNodeCounter()
   {
      clear() ;
   }

   void clusterNodeCounter::clear()
   {
      map<string, hostNodeCounter*>::iterator iter ;
      iter = _mapHostNodeCounter.begin() ;
      while ( iter != _mapHostNodeCounter.end() )
      {
         SDB_OSS_DEL iter->second ;
         _mapHostNodeCounter.erase( iter++ ) ;
      }

      _availableGroupID = 1 ;
   }

   INT32 clusterNodeCounter::increaseGroupID()
   {
      return _availableGroupID++ ;
   }

   INT32 clusterNodeCounter::getCountInHost( const string &hostName, 
                                             const string &role )
   {
      INT32 count = 0 ;
      map<string, hostNodeCounter *>::iterator iter ;
      iter = _mapHostNodeCounter.find( hostName ) ;
      if ( iter != _mapHostNodeCounter.end() )
      {
         hostNodeCounter *p = iter->second ;
         count = p->getNodeCount( role ) ;
      }

      return count ;
   }

   INT32 clusterNodeCounter::getCountInHost( const string &hostName )
   {
      INT32 count = 0 ;
      map<string, hostNodeCounter *>::iterator iter ;
      iter = _mapHostNodeCounter.find( hostName ) ;
      if ( iter != _mapHostNodeCounter.end() )
      {
         hostNodeCounter *p = iter->second ;
         count = p->getNodeCount() ;
      }

      return count ;
   }

   INT32 clusterNodeCounter::getCountInDisk( const string &hostName, 
                                             const string &diskName,
                                             const string &role )
   {
      INT32 count = 0 ;
      map<string, hostNodeCounter *>::iterator iter ;
      iter = _mapHostNodeCounter.find( hostName ) ;
      if ( iter != _mapHostNodeCounter.end() )
      {
         hostNodeCounter *p = iter->second ;
         count = p->getNodeCountInDisk( diskName, role ) ;
      }

      return count ;
   }

   INT32 clusterNodeCounter::getCountInDisk( const string &hostName, 
                                             const string &diskName )
   {
      INT32 count = 0 ;
      map<string, hostNodeCounter *>::iterator iter ;
      iter = _mapHostNodeCounter.find( hostName ) ;
      if ( iter != _mapHostNodeCounter.end() )
      {
         hostNodeCounter *p = iter->second ;
         count = p->getNodeCountInDisk( diskName ) ;
      }

      return count ;
   }

   INT32 clusterNodeCounter::addNode( const string &hostName,
                                      const string &diskName,
                                      const string &businessName, 
                                      const string &role, 
                                      const string &groupName )
   {
      INT32 rc = SDB_OK ;
      map<string, hostNodeCounter *>::iterator iter ;
      iter = _mapHostNodeCounter.find( hostName ) ;
      if ( iter == _mapHostNodeCounter.end() )
      {
         hostNodeCounter *hnc = SDB_OSS_NEW hostNodeCounter( hostName ) ;
         if ( NULL == hnc )
         {
            rc = SDB_OOM ;
            PD_LOG_MSG( PDERROR, "out of memory" ) ;      
            goto error ;
         }

         _mapHostNodeCounter.insert( 
               map<string, hostNodeCounter*>::value_type( hostName, hnc ) ) ;
      }

      _mapHostNodeCounter[hostName]->addNode( diskName, businessName, role ) ;
      _counter.increaseNode( role ) ;

      if ( role == OM_NODE_ROLE_DATA )
      {
         string::size_type pos = 0 ;
         pos = groupName.find( OM_DG_NAME_PATTERN ) ;
         if ( pos != string::npos )
         {
            string groupID ;
            INT32 id ;
            string::size_type start = pos + ossStrlen( OM_DG_NAME_PATTERN ) ;
            groupID = groupName.substr( start ) ;
            id      = ossAtoi( groupID.c_str() ) ;
            if ( id >= _availableGroupID )
            {
               _availableGroupID = id + 1 ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }


   hostHardWare::hostHardWare( const string &hostName, propertyContainer *pc,
                               clusterNodeCounter *nodeCounter )
                :_hostName( hostName ), _propertyContainer( pc ),
                 _nodeCounter( nodeCounter )
   {
      _availableSvcName = 
                 _propertyContainer->getDefaultValue( OM_CONF_DETAIL_SVCNAME ) ;

      CHAR local[ OSS_MAX_HOSTNAME + 1 ] = "" ;
      ossGetHostName( local, OSS_MAX_HOSTNAME ) ;
      if ( _hostName.compare( local ) == 0 )
      {
         pmdKRCB *pKrcb       = pmdGetKRCB() ;
         _pmdOptionsMgr *pOpt = pKrcb->getOptionCB() ;
         INT32 svcPort        = pOpt->getServicePort() ;

         INT32 iTmpSvcName = ossAtoi( _availableSvcName.c_str() ) ;
         if ( iTmpSvcName <= svcPort )
         {
            _availableSvcName = strPlus( svcPort, OM_SVCNAME_STEP ) ;
         }
         else if ( iTmpSvcName < svcPort + OM_SVCNAME_STEP )
         {
            _availableSvcName = strPlus( iTmpSvcName, OM_SVCNAME_STEP ) ;
         }
      }
   }

   hostHardWare::~hostHardWare()
   {
      _mapDisk.clear() ;
      _resourceList.clear() ;
      _inUseDisk.clear() ;
      _propertyContainer = NULL ;
   }

   INT32 hostHardWare::addDisk( const string &diskName, 
                                const string &mountPath, UINT64 totalSize, 
                                UINT64 freeSize )
   {
      simpleDiskInfo disk ;
      disk.diskName  = diskName ;
      disk.mountPath = mountPath ;
      trimLeft( disk.mountPath, " " ) ;
      trimRight( disk.mountPath, " " ) ;
      trimRight( disk.mountPath, OSS_FILE_SEP ) ;
      if ( disk.mountPath == "" )
      {
         disk.mountPath = OSS_FILE_SEP ;
      }
      disk.totalSize = totalSize ;
      disk.freeSize  = freeSize ;

      _mapDisk[ diskName ] = disk ;

      return SDB_OK ;
   }

   string hostHardWare::getName()
   {
      return _hostName ;
   }

   INT32 hostHardWare::getDiskCount()
   {
      return _mapDisk.size() ;
   }

   simpleDiskInfo *hostHardWare::_getDiskInfo( const string &dbPath )
   {
      INT32 maxFitSize          = 0 ;
      simpleDiskInfo *pDiskInfo = NULL ;
      map<string, simpleDiskInfo>::iterator iter = _mapDisk.begin() ;
      while ( iter != _mapDisk.end() )
      {
         simpleDiskInfo *pTmpDisk = &iter->second ;
         string::size_type pathPos = dbPath.find( pTmpDisk->mountPath ) ;
         if ( pathPos != string::npos )
         {
            INT32 tmpFitSize = pTmpDisk->mountPath.length() ;
            if ( NULL == pDiskInfo )
            {
               pDiskInfo  = pTmpDisk ;
               maxFitSize = tmpFitSize ;
            }
            else
            {
               if ( maxFitSize < tmpFitSize )
               {
                  pDiskInfo  = pTmpDisk ;
                  maxFitSize = tmpFitSize ;
               }
            }
         }

         iter++ ;
      }

      return pDiskInfo ;
   }

   string hostHardWare::getDiskName( const string &dbPath )
   {
      simpleDiskInfo *pDiskInfo = _getDiskInfo( dbPath ) ;
      if ( NULL != pDiskInfo )
      {
         return pDiskInfo->diskName ;
      }

      return "" ;
   }

   string hostHardWare::getMountPath( const string &dbPath )
   {
      simpleDiskInfo *pDiskInfo = _getDiskInfo( dbPath ) ;
      if ( NULL != pDiskInfo )
      {
         return pDiskInfo->mountPath ;
      }

      return "" ;
   }

   INT32 hostHardWare::getFreeDiskCount()
   {
      return _mapDisk.size() - _inUseDisk.size() ;
   }

   INT32 hostHardWare::occupayResource( const string &path, 
                                        const string &svcName )
   {
      INT32 rc = SDB_OK ;
      string diskName = getDiskName( path ) ;
      if ( "" == diskName )
      {
         rc = SDB_DMS_RECORD_NOTEXIST ;
         PD_LOG_MSG( PDERROR, "path's disk is not exist:path=%s", 
                     path.c_str() ) ;
         goto error ;
      }

      {
         _inUseDisk.insert( diskName ) ;

         hostResource s ;
         s.path = path ;
         trimLeft( s.path, " " ) ;
         trimRight( s.path, " " ) ;
         trimRight( s.path, OSS_FILE_SEP ) ;
         s.svcName     = svcName ;
         s.replname    = strPlus( svcName, 1 ) ;
         s.shardname   = strPlus( svcName, 2 ) ;
         s.catalogname = strPlus( svcName, 3 ) ;
         s.httpname    = strPlus( svcName, 4 ) ;
         _resourceList.push_back( s ) ;

         INT32 iAvailable = ossAtoi( _availableSvcName.c_str() ) ;
         INT32 iSvcName   = ossAtoi( svcName.c_str() ) ;
         if ( iAvailable <= iSvcName )
         {
            _availableSvcName = strPlus( iSvcName, OM_SVCNAME_STEP ) ;
         }
         else if( iAvailable < iSvcName + OM_SVCNAME_STEP )
         {
            _availableSvcName = strPlus( iAvailable, OM_SVCNAME_STEP ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN hostHardWare::isDiskExist( const string &dbPath )
   {
      simpleDiskInfo *p = _getDiskInfo( dbPath ) ;
      return ( NULL != p ) ;
   }

   BOOLEAN hostHardWare::isPathOccupayed( const string &dbPath ) 
   {
      string tmpPath = dbPath ;
      trimLeft( tmpPath, " " ) ;
      trimRight( tmpPath, " " ) ;
      trimRight( tmpPath, OSS_FILE_SEP ) ;
      list<hostResource>::iterator iter = _resourceList.begin() ;
      while ( iter != _resourceList.end() )
      {
         if ( tmpPath == iter->path )
         {
            return TRUE ;
         }

         iter++ ;
      }

      return FALSE ;
   }

   BOOLEAN hostHardWare::isSvcNameOccupayed( const string &svcName ) 
   {
      string tmpName = svcName ;
      trimLeft( tmpName, " " ) ;
      trimRight( tmpName, " " ) ;
      list<hostResource>::iterator iter = _resourceList.begin() ;
      while ( iter != _resourceList.end() )
      {
         if ( tmpName == iter->svcName || tmpName == iter->replname ||
              tmpName == iter->shardname || tmpName == iter->catalogname ||
              tmpName == iter->httpname )
         {
            return TRUE ;
         }

         iter++ ;
      }

      return FALSE ;
   }

   simpleDiskInfo *hostHardWare::_getBestDisk( const string &role ) 
   {
      simpleDiskInfo *bestDisk = NULL ;
      map<string, simpleDiskInfo>::iterator iter = _mapDisk.begin() ;
      while ( iter != _mapDisk.end() )
      {
         if ( NULL == bestDisk )
         {
            bestDisk = &( iter->second ) ;
            iter++ ;
            continue ;
         }

         simpleDiskInfo *pTmp = &( iter->second ) ;

         INT32 bestRoleCount = _nodeCounter->getCountInDisk( _hostName, 
                                                             bestDisk->diskName, 
                                                             role ) ;
         INT32 tmpRoleCount  = _nodeCounter->getCountInDisk( _hostName,
                                                             pTmp->diskName ,
                                                             role ) ;
         if ( tmpRoleCount != bestRoleCount  )
         {
            if ( tmpRoleCount < bestRoleCount )
            {
               bestDisk = &( iter->second ) ;
            }

            iter++;
            continue ;
         }

         INT32 bestCount = _nodeCounter->getCountInDisk( _hostName, 
                                                         bestDisk->diskName ) ;
         INT32 tmpCount  = _nodeCounter->getCountInDisk( _hostName,
                                                         pTmp->diskName ) ;
         if ( tmpCount != bestCount  )
         {
            if ( tmpCount < bestCount )
            {
               bestDisk = &( iter->second ) ;
            }

            iter++;
            continue ;
         }

         iter++ ;
      }

      return bestDisk ;
   }

   INT32 hostHardWare::createNode( const string &role, 
                                   simpleDiskInfo **diskInfo, 
                                   string &svcName )
   {
      INT32 rc = SDB_OK ;
      *diskInfo = _getBestDisk( role ) ;
      if ( NULL == *diskInfo )
      {
         rc = SDB_DMS_RECORD_NOTEXIST ;
         PD_LOG( PDERROR, "get disk failed:role=%s", role.c_str() ) ;
         goto error ;
      }

      svcName = _availableSvcName ;

   done:
      return rc ;
   error:
      goto done ;
   }

   omCluster::omCluster()
   {
   }

   omCluster::~omCluster()
   {
      clear() ;
   }

   void omCluster::setPropertyContainer( propertyContainer *pc )
   {
      _propertyContainer = pc ;
   }

   /*
   host:
   { 
      "HostName":"host1", "ClusterName":"c1", 
      "Disk":
      [
         {
            "Name":"/dev/sdb", Size:"", Mount:"/test", Used:""
         }, ...
      ]
   }
   config:
   {
      [
         { "BusinessName":"b2","dbpath":"", svcname:"", "role":"", ... }
         , ...
      ]
   }
   */
   INT32 omCluster::addHost( const BSONObj &host, const BSONObj &config )
   {
      INT32 rc = SDB_OK ;
      string hostName = host.getStringField( OM_BSON_FIELD_HOST_NAME ) ;
      map<string, hostHardWare*>::iterator iter = _mapHost.find( hostName ) ;
      SDB_ASSERT( iter == _mapHost.end(), "" ) ;
      hostHardWare *pHostHW = SDB_OSS_NEW hostHardWare( hostName, 
                                                        _propertyContainer,
                                                        &_nodeCounter ) ;
      if ( NULL == pHostHW )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "new hostHardWare failed:rc=%d", rc ) ;
         goto error ;
      }

      _mapHost.insert( map<string, hostHardWare*>::value_type( hostName, 
                                                               pHostHW ) ) ;

      {
         BSONObj disks = host.getObjectField( OM_BSON_FIELD_DISK ) ;
         BSONObjIterator i( disks ) ;
         while ( i.more() )
         {
            string tmp ;
            BSONElement ele = i.next() ;
            if ( Object == ele.type() )
            {
               BSONObj oneDisk = ele.embeddedObject() ;
               string diskName ;
               string mountPath ;
               diskName  = oneDisk.getStringField( OM_BSON_FIELD_DISK_NAME ) ;
               mountPath = oneDisk.getStringField( OM_BSON_FIELD_DISK_MOUNT ) ;
               pHostHW->addDisk( diskName, mountPath, 0, 0 ) ;
            }
         }
      }

      {
         BSONObj nodes = host.getObjectField( OM_BSON_FIELD_CONFIG ) ;
         BSONObjIterator i( nodes ) ;
         while ( i.more() )
         {
            string tmp ;
            BSONElement ele = i.next() ;
            if ( Object == ele.type() )
            {
               BSONObj oneNode = ele.embeddedObject() ;
               string businessName ;
               string dbPath ;
               string role ;
               string svcName ;
               string groupName ;
               string diskName ;
               businessName = oneNode.getStringField( OM_BSON_BUSINESS_NAME ) ;
               dbPath       = oneNode.getStringField( OM_CONF_DETAIL_DBPATH ) ;
               role         = oneNode.getStringField( OM_CONF_DETAIL_ROLE ) ;
               svcName      = oneNode.getStringField( OM_CONF_DETAIL_SVCNAME ) ;
               groupName    = oneNode.getStringField( 
                                                OM_CONF_DETAIL_DATAGROUPNAME ) ;
               diskName     = pHostHW->getDiskName( dbPath ) ;
               SDB_ASSERT( diskName != "" ,"" ) ;

               pHostHW->occupayResource( dbPath, svcName ) ;
               rc = _nodeCounter.addNode( hostName, diskName, businessName, 
                                          role, groupName ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "add node failed:rc=%d", rc ) ;
                  goto error ;
               }
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omCluster::createNode( const string &businessType, 
                                const string &businessName,
                                const string &role, 
                                const string &groupName,
                                string &hostName, string &diskName, 
                                string &svcName, string &dbPath )
   {
      INT32 rc = SDB_OK ;
      simpleDiskInfo *diskInfo = NULL ;
      hostHardWare *host = _getBestHost( role ) ;
      if ( NULL == host )
      {
         rc = SDB_DMS_RECORD_NOTEXIST ;
         PD_LOG_MSG( PDERROR, 
                     "create node failed:host is zero or disk is zero" ) ;
         goto error ;
      }

      rc = host->createNode( role, &diskInfo, svcName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "create node failed:host=%s", 
                 host->getName().c_str() ) ;
         goto error ;
      }

      {
         CHAR tmpDbPath[OM_PATH_LENGTH + 1] = "";
         utilBuildFullPath( diskInfo->mountPath.c_str(), businessType.c_str(), 
                            OM_PATH_LENGTH, tmpDbPath ) ;
         utilCatPath( tmpDbPath, OM_PATH_LENGTH, OM_DBPATH_PREFIX_DATABASE ) ;
         utilCatPath( tmpDbPath, OM_PATH_LENGTH, role.c_str() ) ;
         utilCatPath( tmpDbPath, OM_PATH_LENGTH, svcName.c_str() ) ;

         dbPath   = tmpDbPath ;
      }

      hostName = host->getName() ;
      diskName = diskInfo->diskName ;

      host->occupayResource( dbPath, svcName ) ;
      rc = _nodeCounter.addNode( hostName, diskName, businessName, role, 
                                 groupName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "addNode failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omCluster::getHostNum()
   {
      return _mapHost.size() ;
   }

   INT32 omCluster::increaseGroupID()
   {
      return _nodeCounter.increaseGroupID() ;
   }

   /*
      get best host rule:
          rule1: the less the better which host contains specify role's count
          rule2: the more the better which host contains unused disk's count
          rule3: the less the better which host contains node's count
                 ( all the roles )
   */
   hostHardWare* omCluster::_getBestHost( const string &role )
   {
      map<string, hostHardWare*>::iterator iter ;
      hostHardWare *bestHost = NULL ;
      if ( _mapHost.size() == 0 )
      {
         PD_LOG( PDERROR, "host count is zero" ) ;
         goto error ;
      }

      iter = _mapHost.begin() ;
      while ( iter != _mapHost.end() )
      {
         hostHardWare *pTmpHost = iter->second ;

         if ( 0 == pTmpHost->getDiskCount() )
         {
            iter++ ;
            continue ;
         }

         if ( NULL == bestHost )
         {
            bestHost = pTmpHost ;
            iter++ ;
            continue ;
         }

         INT32 tmpRoleCount ;
         tmpRoleCount = _nodeCounter.getCountInHost( pTmpHost->getName(), 
                                                     role ) ;

         INT32 bestRoleCount ;
         bestRoleCount = _nodeCounter.getCountInHost( bestHost->getName(), 
                                                      role ) ;
         if ( tmpRoleCount != bestRoleCount )
         {
            if ( tmpRoleCount < bestRoleCount )
            {
               bestHost = pTmpHost ;
            }

            iter++ ;
            continue ;
         }

         INT32 tmpFreeDiskCount  = pTmpHost->getFreeDiskCount();
         INT32 bestFreeDiskCount = bestHost->getFreeDiskCount() ;
         if ( tmpFreeDiskCount != bestFreeDiskCount )
         {
            if ( tmpFreeDiskCount > bestFreeDiskCount )
            {
               bestHost = pTmpHost ;
            }

            iter++ ;
            continue ;
         }

         INT32 tmpNodeCount  = _nodeCounter.getCountInHost( 
                                                         pTmpHost->getName() ) ;
         INT32 bestNodeCount = _nodeCounter.getCountInHost( 
                                                         bestHost->getName() ) ;
         if ( tmpNodeCount != bestNodeCount )
         {
            if ( tmpNodeCount < bestNodeCount )
            {
               bestHost = pTmpHost ;
            }

            iter++ ;
            continue ;
         }

         iter++ ;
      }

   done:
      return bestHost ;
   error:
      goto done ;
   }

   INT32 omCluster::checkAndAddNode( const string &businessName,
                                     omNodeConf *node )
   {
      INT32 rc         = SDB_OK ;
      string role      = node->getRole() ;
      string svcName   = node->getSvcName() ;
      string dbPath    = node->getDbPath() ;
      string hostName  = node->getHostName() ;
      string groupName = node->getDataGroupName() ;

      map<string, hostHardWare*>::iterator iter = _mapHost.find( hostName ) ;
      if ( iter == _mapHost.end() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "host is not exist:hostName=%s", 
                     hostName.c_str() ) ;
         goto error ;
      }

      {
         string diskName ;
         hostHardWare *hw = iter->second ;
         if ( !hw->isDiskExist( dbPath ) )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "dbPath's disk is not exist:hostName=%s,"
                        "dbPath=%s", hostName.c_str(), dbPath.c_str() ) ;
            goto error ;
         }

         if ( hw->isPathOccupayed( dbPath ) )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "dbpath is exist:dbpath=%s", dbPath.c_str() ) ;
            goto error ;
         }

         if ( hw->isSvcNameOccupayed( svcName ) )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "svcname is exist:svcname=%s", 
                        svcName.c_str() ) ;
            goto error ;
         }

         hw->occupayResource( dbPath, svcName ) ;
         diskName = hw->getDiskName( dbPath ) ;

         _nodeCounter.addNode( hostName, diskName, businessName, role, 
                               groupName ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omCluster::clear()
   {
      map<string, hostHardWare*>::iterator iter = _mapHost.begin() ;
      while ( iter != _mapHost.end() )
      {
         hostHardWare *pHost = iter->second ;
         SDB_OSS_DEL pHost ;
         _mapHost.erase( iter++ ) ;
      }

      _propertyContainer = NULL ;
      _nodeCounter.clear() ;
   }

   omConfTemplate::omConfTemplate()
                  :_businessType( "" ), _businessName( "" ), _clusterName( "" ),
                   _deployMod( "" ), _replicaNum( -1 ), _dataNum( 0 ), 
                   _catalogNum( -1 ), _dataGroupNum( -1 ), _coordNum( -1 )
   {
   }

   omConfTemplate::~omConfTemplate()
   {
      clear() ;
   }

   /*
   bsonTemplate:
   {
      "ClusterName":"c1","BusinessType":"sequoiadb", "BusinessName":"b1",
      "DeployMod": "standalone", 
      "Property":[{"Name":"replicanum", "Type":"int", "Default":"1", 
                      "Valid":"1", "Display":"edit box", "Edit":"false", 
                      "Desc":"", "WebName":"" }
                      , ...
                 ] 
   }
   */
   INT32 omConfTemplate::init( const BSONObj &confTemplate )
   {
      INT32 rc = SDB_OK ;
      rc = getValueAsString( confTemplate, OM_BSON_BUSINESS_TYPE, 
                             _businessType ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "Template miss bson field[%s]", 
                     OM_BSON_BUSINESS_TYPE ) ;
         goto error ;
      }

      rc = getValueAsString( confTemplate, OM_BSON_BUSINESS_NAME, 
                             _businessName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "Template miss bson field[%s]", 
                     OM_BSON_BUSINESS_NAME ) ;
         goto error ;
      }

      rc = getValueAsString( confTemplate, OM_BSON_FIELD_CLUSTER_NAME, 
                             _clusterName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "Template miss bson field[%s]", 
                     OM_BSON_FIELD_CLUSTER_NAME ) ;
         goto error ;
      }

      rc = getValueAsString( confTemplate, OM_BSON_DEPLOY_MOD, _deployMod ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "Template miss bson field[%s]", 
                     OM_BSON_DEPLOY_MOD ) ;
         goto error ;
      }


      {
         BSONElement propertyElement ;
         propertyElement = confTemplate.getField( OM_BSON_PROPERTY_ARRAY ) ;
         if ( propertyElement.eoo() || Array != propertyElement.type() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "template's field is not Array:field=%s,"
                        "type=%d", OM_BSON_PROPERTY_ARRAY, 
                        propertyElement.type() ) ;
            goto error ;
         }

         BSONObjIterator i( propertyElement.embeddedObject() ) ;
         while ( i.more() )
         {
            BSONElement ele = i.next() ;
            if ( ele.type() == Object )
            {
               BSONObj oneProperty = ele.embeddedObject() ;
               rc = _setPropery( oneProperty ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "_setPropery failed:rc=%d", rc ) ;
                  goto error ;
               }
            }
         }
      }

      if ( !_isAllProperySet() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "miss template configur item" ) ;
         goto error ;
      }

      _dataNum = _dataGroupNum * _replicaNum ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omConfTemplate::_setPropery( BSONObj &property )
   {
      INT32 rc = SDB_OK ;
      string itemName ;
      string itemValue ;
      rc = getValueAsString( property, OM_BSON_PROPERTY_NAME, itemName ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "property miss bson field=%s", 
                     OM_BSON_PROPERTY_NAME ) ;
         goto error ;
      }

      rc = getValueAsString( property, OM_BSON_PROPERTY_VALUE, 
                             itemValue ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "property miss bson field=%s", 
                     OM_BSON_PROPERTY_VALUE ) ;
         goto error ;
      }

      if ( itemName.compare( OM_TEMPLATE_REPLICA_NUM ) == 0 )
      {
         _replicaNum = ossAtoi( itemValue.c_str() ) ;
      }
      else if ( itemName.compare( OM_TEMPLATE_DATAGROUP_NUM ) == 0 )
      {
         _dataGroupNum = ossAtoi( itemValue.c_str() ) ;
      }
      else if ( itemName.compare( OM_TEMPLATE_CATALOG_NUM ) == 0 )
      {
         _catalogNum = ossAtoi( itemValue.c_str() ) ;
      }
      else if ( itemName.compare( OM_TEMPLATE_COORD_NUM ) == 0 )
      {
         _coordNum = ossAtoi( itemValue.c_str() ) ;
      }

      {
         confProperty oneProperty ;
         rc = oneProperty.init( property ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "init property failed:rc=%d", rc ) ;
            goto error ;
         }

         if ( !oneProperty.isValid( itemValue ) )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "Template value is invalid:item=%s,value=%s,"
                        "valid=%s", itemName.c_str(), itemValue.c_str(), 
                        oneProperty.getValidString().c_str() ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omConfTemplate::clear()
   {
      _businessType = "" ;
      _businessName = "" ;
      _clusterName  = "" ;
      _deployMod    = "" ;
      _replicaNum   = -1 ;
      _dataNum      = -1 ;
      _dataGroupNum = -1 ;
      _catalogNum   = -1 ;
      _coordNum     = -1 ;

   }

   BOOLEAN omConfTemplate::_isAllProperySet()
   {
      if ( _replicaNum == -1 )
      {
         PD_LOG_MSG( PDERROR, "%s have not been set", 
                     OM_TEMPLATE_REPLICA_NUM ) ;
         return FALSE ;
      }
      else if ( _dataGroupNum == -1 )
      {
         PD_LOG_MSG( PDERROR, "%s have not been set", 
                     OM_TEMPLATE_DATAGROUP_NUM ) ;
         return FALSE ;
      }
      else if ( _catalogNum == -1 )
      {
         PD_LOG_MSG( PDERROR, "%s have not been set", 
                     OM_TEMPLATE_CATALOG_NUM ) ;
         return FALSE ;
      }
      else if ( _coordNum == -1 )
      {
         PD_LOG_MSG( PDERROR, "%s have not been set", OM_TEMPLATE_COORD_NUM ) ;
         return FALSE ;
      }

      return TRUE ;
   }

   string omConfTemplate::getBusinessType()
   {
      return _businessType ;
   }

   string omConfTemplate::getBusinessName()
   {
      return _businessName ;
   }

   string omConfTemplate::getClusterName()
   {
      return _clusterName ;
   }

   string omConfTemplate::getDeployMod()
   {
      return _deployMod ;
   }

   INT32 omConfTemplate::getReplicaNum()
   {
      return _replicaNum ;
   }

   INT32 omConfTemplate::getDataNum()
   {
      return _dataNum ;
   }

   INT32 omConfTemplate::getDataGroupNum()
   {
      return _dataGroupNum ;
   }

   INT32 omConfTemplate::getCatalogNum()
   {
      return _catalogNum ;
   }

   INT32 omConfTemplate::getCoordNum()
   {
      return _coordNum ;
   }

   void omConfTemplate::setCoordNum( INT32 coordNum )
   {
      _coordNum = coordNum ;
   }

   omNodeConf::omNodeConf()
   {
      _dbPath        = "" ;
      _svcName       = "" ;
      _role          = "" ;
      _dataGroupName = "" ;
      _hostName      = "" ;
      _diskName      = "" ;

      _additionalConfMap.clear() ;
   }

   omNodeConf::omNodeConf( const omNodeConf &right )
   {
      _dbPath        = right._dbPath ;
      _svcName       = right._svcName ;
      _role          = right._role ;
      _dataGroupName = right._dataGroupName ;
      _hostName      = right._hostName ;
      _diskName      = right._diskName ;

      _additionalConfMap = right._additionalConfMap ;
   }

   omNodeConf::~omNodeConf()
   {
      _additionalConfMap.clear() ;
   }

   void omNodeConf::setDbPath( const string &dbpath )
   {
      _dbPath = dbpath ;
   }

   void omNodeConf::setSvcName( const string &svcName )
   {
      _svcName = svcName ;
   }

   void omNodeConf::setRole( const string &role )
   {
      _role = role ;
   }

   void omNodeConf::setDataGroupName( const string &dataGroupName )
   {
      _dataGroupName = dataGroupName ;
   }

   void omNodeConf::setHostName( const string &hostName )
   {
      _hostName = hostName ;
   }

   void omNodeConf::setDiskName( const string &diskName )
   {
      _diskName = diskName ;
   }

   void omNodeConf::setAdditionalConf( const string &key, 
                                       const string &value )
   {
      _additionalConfMap[key] = value ;
   }

   string omNodeConf::getDbPath()
   {
      return _dbPath ;
   }

   string omNodeConf::getSvcName()
   {
      return _svcName ;
   }

   string omNodeConf::getRole()
   {
      return _role ;
   }

   string omNodeConf::getDataGroupName()
   {
      return _dataGroupName ;
   }

   string omNodeConf::getHostName()
   {
      return _hostName ;
   }

   string omNodeConf::getDiskName()
   {
      return _diskName ;
   }

   string omNodeConf::getAdditionlConf( const string &key )
   {
      string value = "" ;
      map<string, string>::iterator iter = _additionalConfMap.find( key ) ;
      if ( iter != _additionalConfMap.end() )
      {
         value = iter->second ;
      }

      return value ;
   }

   const map<string, string>* omNodeConf::getAdditionalMap()
   {
      return &_additionalConfMap ;
   }

   rangeValidator::rangeValidator( const string &type, const CHAR *value )
   {
      _isClosed   = TRUE ;
      _begin      = value ;
      _end        = value ;
      _isValidAll = FALSE ;
      _type       = type ;

      trim( _begin ) ;
      trim( _end ) ;

      if ( ( _begin.length() == 0 ) && ( _end.length() == 0 ) )
      {
         /* if range is empty, all the value is valid */
         _isValidAll = TRUE ;
      }
   }

   rangeValidator::rangeValidator( const string &type, const CHAR *begin, 
                                   const CHAR *end, BOOLEAN isClosed )
   {
      _isClosed   = isClosed ;
      _begin      = begin ;
      _end        = end ;
      _isValidAll = FALSE ;
      _type       = type ;

      trim( _begin ) ;
      trim( _end ) ;

      if ( ( _begin.length() == 0 ) && ( _end.length() == 0 ) )
      {
         /* if range is empty, all the value is valid */
         _isValidAll = TRUE ;
      }

      if ( _type.compare( OM_CONF_VALUE_INT_TYPE ) == 0 )
      {
         if ( _end.length() == 0 )
         {
            _end = OM_INT32_MAXVALUE_STR ;
         }
      }
   }

   rangeValidator::~rangeValidator()
   {
   }

   BOOLEAN rangeValidator::isValid( const string &value )
   {
      if ( _isValidAll )
      {
         return TRUE ;
      }

      INT32 compareEnd = _compare( value, _end ) ;
      if ( _isClosed )
      {
         if ( compareEnd == 0 )
         {
            return TRUE ;
         }
      }

      INT32 compareBegin = _compare( value, _begin ) ;
      if ( compareBegin >= 0 && compareEnd < 0 )
      {
         return TRUE ;
      }

      return FALSE ;
   }

   INT32 rangeValidator::_compare( string left, string right )
   {
      if ( _type == OM_CONF_VALUE_INT_TYPE )
      {
         INT32 leftInt  = ossAtoi( left.c_str() ) ;
         INT32 rightInt = ossAtoi( right.c_str() ) ;

         return ( leftInt - rightInt ) ;
      }

      return left.compare( right ) ;
   }

   string rangeValidator::getType() 
   {
      return _type ;
   }

   confValidator::confValidator()
   {
   }

   confValidator::~confValidator()
   {
      _clear() ;
   }

   rangeValidator *confValidator::_createrangeValidator( const string &value )
   {
      rangeValidator *rv       = NULL ;
      string::size_type posTmp = value.find( OM_GENERATOR_LINE ) ;
      if( string::npos != posTmp )
      {
         rv = SDB_OSS_NEW rangeValidator( _type,
                                          value.substr(0,posTmp).c_str(), 
                                          value.substr(posTmp+1).c_str() ) ;
      }
      else
      {
         rv = SDB_OSS_NEW rangeValidator( _type, value.c_str() ) ;
      }

      return rv ;
   }

   INT32 confValidator::init( const string &type, const string &validateStr )
   {
      _clear() ;

      string tmp ;
      INT32 rc = SDB_OK ;
      _type    = type ;

      rangeValidator *rv     = NULL ;
      string::size_type pos1 = 0 ;
      string::size_type pos2 = validateStr.find( OM_GENERATOR_DOT ) ;
      while( string::npos != pos2 )
      {
         rv = _createrangeValidator( validateStr.substr( pos1, pos2 - pos1 ) ) ;
         if ( NULL == rv )
         {
            rc = SDB_OOM ;
            goto error ;
         }

         _validatorList.push_back( rv ) ;

         pos1 = pos2 + 1 ;
         pos2 = validateStr.find( OM_GENERATOR_DOT, pos1 ) ;
      }

      tmp = validateStr.substr( pos1 ) ; 
      rv  = _createrangeValidator( tmp ) ;
      if ( NULL == rv )
      {
         rc = SDB_OOM ;
         goto error ;
      }

      _validatorList.push_back( rv ) ;

   done:
      return rc ;
   error:
      _clear() ;
      goto done ;
   }

   BOOLEAN confValidator::isValid( const string &value )
   {
      VALIDATORLIST_ITER iter = _validatorList.begin() ;
      while ( iter != _validatorList.end() )
      {
         if ( ( *iter )->isValid( value ) )
         {
            return TRUE ;
         }

         iter++ ;
      }

      return FALSE ;
   }

   string confValidator::getType()
   {
      return _type ;
   }

   void confValidator::_clear()
   {
      VALIDATORLIST_ITER iter = _validatorList.begin() ;
      while ( iter != _validatorList.end() )
      {
         rangeValidator *p = *iter ;
         SDB_OSS_DEL p ;
         iter++ ;
      }

      _validatorList.clear() ;
   }

   confProperty::confProperty()
   {
   }

   confProperty::~confProperty()
   {
   }

   /*
   propery:
      {
         "Name":"replicanum", "Type":"int", "Default":"1", "Valid":"1", 
         "Display":"edit box", "Edit":"false", "Desc":"", "WebName":"" 
      }
   */
   INT32 confProperty::init( const BSONObj &property )
   {
      INT32 rc = SDB_OK ;
      rc = getValueAsString( property, OM_BSON_PROPERTY_TYPE, _type ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "get field failed:field=%s,rc=%d", 
                     OM_BSON_PROPERTY_TYPE, rc ) ;
         goto error ;
      }

      rc = getValueAsString( property, OM_BSON_PROPERTY_NAME, _name ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "get field failed:field=%s,rc=%d", 
                     OM_BSON_PROPERTY_NAME, rc ) ;
         goto error ;
      }

      rc = getValueAsString( property, OM_BSON_PROPERTY_DEFAULT, 
                             _defaultValue ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "get field failed:field=%s,rc=%d", 
                     OM_BSON_PROPERTY_DEFAULT, rc ) ;
         goto error ;
      }

      rc = getValueAsString( property, OM_BSON_PROPERTY_VALID, _validateStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "get field failed:field=%s,rc=%d", 
                     OM_BSON_PROPERTY_VALID, rc ) ;
         goto error ;
      }

      rc = _confValidator.init( _type, _validateStr ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "init _confValidator failed:rc=%d", rc ) ;
         goto error ;
      }

      if ( !_confValidator.isValid( _defaultValue ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "%s's default value is invalid:value=%s,valid=%s", 
                     _name.c_str(), _defaultValue.c_str(), 
                     _validateStr.c_str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   string confProperty::getDefaultValue()
   {
      return _defaultValue ;
   }

   string confProperty::getItemName()
   {
      return _name ;
   }

   string confProperty::getType()
   {
      return _type ;
   }

   BOOLEAN confProperty::isValid( const string &value )
   {
      return _confValidator.isValid( value ) ;
   }

   string confProperty::getValidString()
   {
      return _validateStr ;
   }

   propertyContainer::propertyContainer()
                     :_dbPathProperty( NULL ), _roleProperty( NULL ), 
                     _svcNameProperty( NULL )
   {
   }

   propertyContainer::~propertyContainer()
   {
      clear() ;
   }

   void propertyContainer::clear()
   {
      map<string, confProperty*>::iterator iter ;
      iter = _additionalPropertyMap.begin() ;
      while ( iter != _additionalPropertyMap.end() )
      {
         confProperty *property = iter->second ;
         SDB_OSS_DEL property ;
         _additionalPropertyMap.erase( iter++ ) ;
      }

      if ( NULL != _dbPathProperty )
      {
         SDB_OSS_DEL _dbPathProperty ;
         _dbPathProperty = NULL ;
      }

      if ( NULL != _roleProperty )
      {
         SDB_OSS_DEL _roleProperty ;
         _roleProperty = NULL ;
      }

      if ( NULL != _svcNameProperty )
      {
         SDB_OSS_DEL _svcNameProperty ;
         _svcNameProperty = NULL ;
      }
   }

   INT32 propertyContainer::addProperty( const BSONObj &property )
   {
      INT32 rc = SDB_OK ;
      confProperty *pConfProperty = SDB_OSS_NEW confProperty() ;
      if ( NULL == pConfProperty )
      {
         rc = SDB_OOM ;
         PD_LOG_MSG( PDERROR, "new confProperty failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = pConfProperty->init( property ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "init confProperty failed:property=%s,rc=%d", 
                 property.toString(false, true ).c_str(), rc ) ;
         goto error ;
      }

      if ( OM_CONF_DETAIL_DBPATH == pConfProperty->getItemName() )
      {
         SDB_ASSERT( NULL == _dbPathProperty, "" ) ;
         _dbPathProperty = pConfProperty ;
      }
      else if ( OM_CONF_DETAIL_ROLE == pConfProperty->getItemName() )
      {
         SDB_ASSERT( NULL == _roleProperty, "" ) ;
         _roleProperty = pConfProperty ;
      }
      else if ( OM_CONF_DETAIL_SVCNAME == pConfProperty->getItemName() )
      {
         SDB_ASSERT( NULL == _svcNameProperty, "" ) ;
         _svcNameProperty = pConfProperty ;
      }
      else
      {
         map<string, confProperty*>::iterator iter ;
         iter = _additionalPropertyMap.find( pConfProperty->getItemName() ) ;
         if ( iter != _additionalPropertyMap.end() )
         {
            confProperty *pTmp = iter->second ;
            SDB_OSS_DEL pTmp ;
            _additionalPropertyMap.erase( iter ) ;
         }

         _additionalPropertyMap.insert( map<string, confProperty*>::value_type( 
                                                   pConfProperty->getItemName(),
                                                   pConfProperty ) ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 propertyContainer::checkValue( const string &name, 
                                        const string &value )
   {
      INT32 rc = SDB_OK ;
      confProperty *pProperty = _getConfProperty( name ) ;
      if ( NULL == pProperty )
      {
         rc = SDB_DMS_RECORD_NOTEXIST ;
         PD_LOG_MSG( PDERROR, "can't find the property:name=%s,rc=%d", 
                     name.c_str(), rc ) ;
         goto error ;
      }

      if ( !pProperty->isValid( value ) )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "property's value is invalid:name=%s,value=%s,"
                     "valid=%s", name.c_str(), value.c_str(), 
                     pProperty->getValidString().c_str() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   confProperty *propertyContainer::_getConfProperty( const string &name )
   {
      confProperty *property = NULL ;
      if ( OM_CONF_DETAIL_DBPATH == name )
      {
         property = _dbPathProperty ;
      }
      else if ( OM_CONF_DETAIL_ROLE == name )
      {
         property = _roleProperty ;
      }
      else if ( OM_CONF_DETAIL_SVCNAME == name )
      {
         property = _svcNameProperty ;
      }
      else
      {
         map<string, confProperty*>::iterator iter ;
         iter = _additionalPropertyMap.find( name ) ;
         if ( iter != _additionalPropertyMap.end() )
         {
            property = iter->second ;
         }
      }

      return property ;
   }

   INT32 propertyContainer::createSample( omNodeConf &sample )
   {
      confProperty *property = NULL ;
      map<string, confProperty*>::iterator iter ;
      iter = _additionalPropertyMap.begin() ;
      while ( iter != _additionalPropertyMap.end() )
      {
         property = iter->second ;
         sample.setAdditionalConf( property->getItemName(), 
                                   property->getDefaultValue() ) ;
         iter++ ;
      }

      return SDB_OK ;
   }

   string propertyContainer::getDefaultValue( const string &name )
   {
      confProperty *property = _getConfProperty( name ) ;
      if ( NULL != property )
      {
         return property->getDefaultValue() ;
      }

      return "" ;
   }

   BOOLEAN propertyContainer::isAllPropertySet()
   {
      if ( NULL == _dbPathProperty )
      {
         PD_LOG_MSG( PDERROR, "property [%s] have not been set", 
                     OM_CONF_DETAIL_DBPATH ) ;
         return FALSE ;
      }

      if ( NULL == _roleProperty )
      {
         PD_LOG_MSG( PDERROR, "property [%s] have not been set", 
                     OM_CONF_DETAIL_ROLE ) ;
         return FALSE ;
      }

      if ( NULL == _svcNameProperty )
      {
         PD_LOG_MSG( PDERROR, "property [%s] have not been set", 
                     OM_CONF_DETAIL_SVCNAME ) ;
         return FALSE ;
      }

      return TRUE ;
   }

   omBusinessConfigure::omBusinessConfigure()
   {
   }

   omBusinessConfigure::~omBusinessConfigure()
   {
      clear() ;
   }

   INT32 omBusinessConfigure::_setNodeConf( BSONObj &oneNode, 
                                            omNodeConf &nodeConf )
   {
      INT32 rc = SDB_OK ;
      BSONObjIterator itemIter( oneNode ) ;
      while ( itemIter.more() )
      {
         BSONElement itemEle = itemIter.next() ;
         string fieldName    = itemEle.fieldName() ;
         string value        = itemEle.String() ;
         if ( OM_BSON_FIELD_HOST_NAME == fieldName )
         {
            nodeConf.setHostName( value ) ;
         }
         else if ( OM_CONF_DETAIL_DATAGROUPNAME == fieldName )
         {
            nodeConf.setDataGroupName( value ) ;
         }
         else 
         {
            rc = _propertyContainer->checkValue( fieldName, value ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "check value failed:name=%s,value=%s", 
                       fieldName.c_str(), value.c_str() ) ;
               goto error ;
            }

            if ( OM_CONF_DETAIL_DBPATH == fieldName )
            {
               nodeConf.setDbPath( value ) ;
            }
            else if ( OM_CONF_DETAIL_SVCNAME == fieldName )
            {
               nodeConf.setSvcName( value ) ;
            }
            else if ( OM_CONF_DETAIL_ROLE == fieldName )
            {
               nodeConf.setRole( value ) ;
            }
            else 
            {
               nodeConf.setAdditionalConf( fieldName, value ) ;
            }
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   string omBusinessConfigure::getBusinessName()
   {
      return _businessName ;
   }

   /*
   business:
   {
      "BusinessType":"sequoiadb", "BusinessName":"b1", "DeployMod":"xx", 
      "ClusterName":"c1", 
      "Config":
      [
         {"HostName": "host1", "datagroupname": "", 
          "dbpath": "/home/db2/standalone/11830", "svcname": "11830", ...}
         ,...
      ]
   }
   */
   INT32 omBusinessConfigure::init( propertyContainer *pc, 
                                    const BSONObj &business )
   {
      _propertyContainer = pc ;
      _businessType = business.getStringField( OM_BSON_BUSINESS_TYPE ) ;
      _businessName = business.getStringField( OM_BSON_BUSINESS_NAME ) ;
      _deployMod    = business.getStringField( OM_BSON_DEPLOY_MOD ) ;
      _clusterName  = business.getStringField( OM_BSON_FIELD_CLUSTER_NAME ) ;

      INT32 rc = SDB_OK ;
      BSONElement configEle ;
      configEle = business.getField( OM_BSON_FIELD_CONFIG ) ;
      if ( configEle.eoo() || Array != configEle.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "business's field is not Array:field=%s,"
                     "type=%d", OM_BSON_FIELD_CONFIG, configEle.type() ) ;
         goto error ;
      }
      {
         BSONObjIterator i( configEle.embeddedObject() ) ;
         while ( i.more() )
         {
            BSONElement ele = i.next() ;
            if ( Object == ele.type() )
            {
               BSONObj oneNode = ele.embeddedObject() ;
               omNodeConf nodeConf ;
               rc = _setNodeConf( oneNode, nodeConf ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "set node conf failed:rc=%d", rc ) ;
                  goto error ;
               }
               _nodeList.push_back( nodeConf ) ;
            }
         }
      }

      rc = _innerCheck() ;
      if (SDB_OK != rc )
      {
         PD_LOG( PDERROR, "check business failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omBusinessConfigure::_innerCheck()
   {
      INT32 rc = SDB_OK ;
      if ( _nodeList.size() == 0 )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "node is zero!" ) ;
         goto error ;
      }

      if ( _deployMod != OM_DEPLOY_MOD_STANDALONE 
               && _deployMod != OM_DEPLOY_MOD_DISTRIBUTION )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "unreconigzed deploy mode:%s", 
                     _deployMod.c_str() ) ;
         goto error ;
      }

      if ( _deployMod == OM_DEPLOY_MOD_STANDALONE )
      {
         if ( _nodeList.size() != 1 )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "can't install more than one node in mode:%s",
                        OM_DEPLOY_MOD_STANDALONE ) ;
            goto error ;
         }

         list<omNodeConf>::iterator iter = _nodeList.begin() ;
         string role = iter->getRole() ;
         if ( role != OM_NODE_ROLE_STANDALONE )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "can't install node with role=%s in mode:%s", 
                        role.c_str(), _deployMod.c_str() ) ;
            goto error ;
         }
      }
      else
      {
         INT32 coordNum   = 0 ;
         INT32 catalogNum = 0 ;
         INT32 dataNum    = 0 ;
         list<omNodeConf>::iterator iter = _nodeList.begin() ;
         while( iter != _nodeList.end() )
         {
            string role = iter->getRole() ;
            if ( role == OM_NODE_ROLE_STANDALONE )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "can't install node with role=%s in mode:"
                           "mode=%s", role.c_str(), _deployMod.c_str() ) ;
               goto error ;
            }

            if ( role == OM_NODE_ROLE_CATALOG )
            {
               catalogNum++ ;
            }
            else if ( role == OM_NODE_ROLE_COORD )
            {
               coordNum++ ;
            }
            else if ( role == OM_NODE_ROLE_DATA )
            {
               dataNum++ ;
            }

            iter++ ;
         }

         if ( catalogNum == 0 )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "catalog number can't be zero" ) ;
            goto error ;
         }

         if ( coordNum == 0 )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "coord number can't be zero" ) ;
            goto error ;
         }

         if ( dataNum == 0 )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "data number can't be zero" ) ;
            goto error ;
         }
      }

      {
         string transaction = "" ;
         string hostName    = "" ;
         string svcName     = "" ;
         list<omNodeConf>::iterator iter = _nodeList.begin() ;
         while( iter != _nodeList.end() )
         {
            string tmp = iter->getAdditionlConf( OM_CONF_DETAIL_TRANSACTION ) ;
            if ( iter == _nodeList.begin() )
            {
               transaction = tmp ;
               hostName    = iter->getHostName() ;
               svcName     = iter->getSvcName() ;
            }
            else
            {
               if ( transaction != tmp )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG_MSG( PDERROR, "transaction exist conflict value:"
                              "host1=%s,svcname=%s,transaction=%s;host2=%s,"
                              "svcname=%s,transaction=%s", hostName.c_str(),
                              svcName.c_str(), transaction.c_str(), 
                              iter->getHostName().c_str(), 
                              iter->getSvcName().c_str(), tmp.c_str() ) ;
                  goto error ;
               }
            }
            iter++ ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   void omBusinessConfigure::getNodeList( list<omNodeConf> &nodeList )
   {
      nodeList = _nodeList ;
   }

   void omBusinessConfigure::clear()
   {
      _nodeList.clear() ;
      _businessType = "" ;
      _businessName = "" ;
      _deployMod    = "" ;
      _clusterName  = "" ;
      _propertyContainer = NULL ;
   }

   omConfigGenerator::omConfigGenerator()
   {
   }

   omConfigGenerator::~omConfigGenerator()
   {
   }

   /*
   bsonTemplate:
   {
      "ClusterName":"c1","BusinessType":"sequoiadb", "BusinessName":"b1",
      "DeployMod": "standalone", 
      "Property":[{"Name":"replicanum", "Type":"int", "Default":"1", 
                      "Valid":"1", "Display":"edit box", "Edit":"false", 
                      "Desc":"", "WebName":"" }
                      , ...
                 ] 
   }
   confProperties:
   {
      "Property":[{"Name":"dbpath", "Type":"path", "Default":"/opt/sequoiadb", 
                      "Valid":"1", "Display":"edit box", "Edit":"false", 
                      "Desc":"", "WebName":"" }
                      , ...
                 ] 
   }
   bsonHostInfo:
   { 
     "HostInfo":[
                   {
                      "HostName":"host1", "ClusterName":"c1", 
                      "Disk":{"Name":"/dev/sdb", Size:"", Mount:"", Used:""},
                      "Config":[{"BusinessName":"b2","dbpath":"", svcname:"", 
                                 "role":"", ... }, ...]
                   }
                    , ... 
                ]
   }
   */
   INT32 omConfigGenerator::generateSDBConfig( const BSONObj &bsonTemplate, 
                                                const BSONObj &confProperties, 
                                                const BSONObj &bsonHostInfo, 
                                                BSONObj &bsonConfig )
   {
      _cluster.clear() ;
      _propertyContainer.clear() ;
      _template.clear() ;

      INT32 rc = _template.init( bsonTemplate ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "init template failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = _parseProperties( confProperties ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "parse confProperties failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = _parseCluster( bsonHostInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "parse hostInfo failed:rc=%d", rc ) ;
         goto error ;
      }

      _propertyContainer.createSample( _nodeSample ) ;
      rc = _generate( bsonConfig ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "parse hostInfo failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
      goto done ;
   }

   /*
   newBusinessConf:
   {
      "BusinessType":"sequoiadb", "BusinessName":"b1", "DeployMod":"xx", 
      "ClusterName":"c1", 
      "Config":
      [
         {"HostName": "host1", "datagroupname": "", 
          "dbpath": "/home/db2/standalone/11830", "svcname": "11830", ...}
         ,...
      ]
   }
   confProperties:
   {
      "Property":[{"Name":"dbpath", "Type":"path", "Default":"/opt/sequoiadb", 
                      "Valid":"1", "Display":"edit box", "Edit":"false", 
                      "Desc":"", "WebName":"" }
                      , ...
                 ] 
   }
   bsonHostInfo:
   { 
     "HostInfo":[
                   {
                      "HostName":"host1", "ClusterName":"c1", 
                      "Disk":{"Name":"/dev/sdb", Size:"", Mount:"", Used:""},
                      "Config":[{"BusinessName":"b2","dbpath":"", svcname:"", 
                                 "role":"", ... }, ...]
                   }
                    , ... 
                ]
   }
   */
   INT32 omConfigGenerator::checkSDBConfig( BSONObj &newBusinessConf,
                                            const BSONObj &confProperties, 
                                            const BSONObj &bsonHostInfo )
   {
      _cluster.clear() ;
      _propertyContainer.clear() ;
      _template.clear() ;

      INT32 rc = SDB_OK ;
      rc = _parseProperties( confProperties ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "parse confProperties failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = _parseCluster( bsonHostInfo ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_parseCluster failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = _parseNewBusiness( newBusinessConf ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "_parseNewBusiness failed:rc=%d", rc ) ;
         goto error ;
      }


   done:
      return rc ;
   error:
      _errorDetail = pmdGetThreadEDUCB()->getInfo( EDU_INFO_ERROR ) ;
      goto done ;
   }

   /*
   newBusinessConf:
   {
      "BusinessType":"sequoiadb", "BusinessName":"b1", "DeployMod":"xx", 
      "ClusterName":"c1", 
      "Config":
      [
         {"HostName": "host1", "datagroupname": "", 
          "dbpath": "/home/db2/standalone/11830", "svcname": "11830", ...}
         ,...
      ]
   }
   */
   INT32 omConfigGenerator::_parseNewBusiness( const BSONObj &newBusinessConf )
   {
      INT32 rc = SDB_OK ;
      rc = _businessConf.init( &_propertyContainer, newBusinessConf ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "init business configure failed:rc=%d", rc ) ;
         goto error ;
      }

      {
         list<omNodeConf> nodeList ;
         _businessConf.getNodeList( nodeList ) ;
         list<omNodeConf>::iterator iter = nodeList.begin() ;
         while ( iter != nodeList.end() )
         {
            omNodeConf *pNodeConf = &( *iter ) ;
            rc = _cluster.checkAndAddNode( _businessConf.getBusinessName(), 
                                           pNodeConf ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "check node failed:rc=%d", rc ) ;
               goto error ;
            }
            iter++ ;
         }
      } 

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omConfigGenerator::_generateStandAlone( list<omNodeConf> &nodeList )
   {
      INT32 rc = SDB_OK ;
      omNodeConf node ;
      string svcName ;
      string dbPath ;
      string hostName ;
      string diskName ;
      string businessType = _template.getBusinessType() ;
      string businessName = _template.getBusinessName() ;
      rc = _cluster.createNode( businessType, businessName,
                                OM_NODE_ROLE_STANDALONE, "",
                                hostName, diskName, svcName, dbPath ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "createNode failed:businessName=%s,businessType=%s,"
                 "role=%s,rc=%d", businessType.c_str(), businessName.c_str(),
                 OM_NODE_ROLE_STANDALONE, rc ) ;
         goto error ;
      }

      node = _nodeSample ;
      node.setDataGroupName( "" ) ;
      node.setDbPath( dbPath ) ;
      node.setRole( OM_NODE_ROLE_STANDALONE ) ;
      node.setSvcName( svcName ) ;
      node.setDiskName( diskName ) ;
      node.setHostName( hostName ) ;
      nodeList.push_back( node ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omConfigGenerator::_generateCluster( list<omNodeConf> &nodeList )
   {
      INT32 rc = SDB_OK ;
      string businessType = _template.getBusinessType() ;
      string businessName = _template.getBusinessName() ;
      INT32 coordNum = _template.getCoordNum() ;
      if ( coordNum == 0 )
      {
         coordNum = _cluster.getHostNum() ;
         _template.setCoordNum( coordNum );
      }

      INT32 iCoordCount = 0 ;
      while ( iCoordCount < coordNum )
      {
         omNodeConf node ;
         string svcName ;
         string dbPath ;
         string hostName ;
         string diskName ;
         rc = _cluster.createNode( businessType, businessName,
                                   OM_NODE_ROLE_COORD, "",
                                   hostName, diskName, svcName, dbPath ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "createNode failed:businessName=%s,businessType=%s,"
                    "role=%s,rc=%d", businessType.c_str(), businessName.c_str(),
                    OM_NODE_ROLE_COORD, rc ) ;
            goto error ;
         }
         node = _nodeSample ;
         node.setDataGroupName( "" ) ;
         node.setDbPath( dbPath ) ;
         node.setRole( OM_NODE_ROLE_COORD ) ;
         node.setSvcName( svcName ) ;
         node.setDiskName( diskName ) ;
         node.setHostName( hostName ) ;
         nodeList.push_back( node ) ;

         iCoordCount++ ;
      }

      {
         INT32 catalogNum    = _template.getCatalogNum() ;
         INT32 iCatalogCount = 0 ;
         while ( iCatalogCount < catalogNum )
         {
            omNodeConf node ;
            string svcName ;
            string dbPath ;
            string hostName ;
            string diskName ;
            rc = _cluster.createNode( businessType,  businessName,
                                      OM_NODE_ROLE_CATALOG, "",
                                      hostName, diskName, svcName, dbPath ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "createNode failed:businessName=%s,businessType=%s,"
                       "role=%s,rc=%d", businessType.c_str(), businessName.c_str(),
                       OM_NODE_ROLE_CATALOG, rc ) ;
               goto error ;
            }
            node = _nodeSample ;
            node.setDataGroupName( "" ) ;
            node.setDbPath( dbPath ) ;
            node.setRole( OM_NODE_ROLE_CATALOG ) ;
            node.setSvcName( svcName ) ;
            node.setDiskName( diskName ) ;
            node.setHostName( hostName ) ;
            nodeList.push_back( node ) ;

            iCatalogCount++ ;
         }
      }

      {
         INT32 groupID      = _cluster.increaseGroupID() ;
         INT32 replicaNum   = _template.getReplicaNum() ;
         INT32 groupIDCycle = 0 ;
         INT32 dataCount    = 0 ;
         while ( dataCount < _template.getDataNum() )
         {
            omNodeConf node ;
            string svcName ;
            string dbPath ;
            string hostName ;
            string diskName ;
            string groupName = strConnect( OM_DG_NAME_PATTERN, groupID ) ;
            rc = _cluster.createNode( businessType,  businessName,
                                      OM_NODE_ROLE_DATA, groupName,
                                      hostName, diskName, svcName, dbPath ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "createNode failed:businessName=%s,businessType=%s,"
                       "role=%s,rc=%d", businessType.c_str(), businessName.c_str(),
                       OM_NODE_ROLE_DATA, rc ) ;
               goto error ;
            }

            node = _nodeSample ;
            node.setDataGroupName( groupName ) ;
            node.setDbPath( dbPath ) ;
            node.setRole( OM_NODE_ROLE_DATA ) ;
            node.setSvcName( svcName ) ;
            node.setDiskName( diskName ) ;
            node.setHostName( hostName ) ;
            nodeList.push_back( node ) ;

            dataCount++ ;
            groupIDCycle++ ;
            if ( groupIDCycle >= replicaNum )
            {
               groupID      = _cluster.increaseGroupID() ;
               groupIDCycle = 0 ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 omConfigGenerator::_generate( BSONObj &bsonConfig )
   {
      INT32 rc = SDB_OK ;
      list<omNodeConf> nodeList ;
      string deployMod = _template.getDeployMod() ;
      if ( ossStrcasecmp( deployMod.c_str(), OM_DEPLOY_MOD_STANDALONE ) == 0 )
      {
         rc = _generateStandAlone( nodeList ) ;
      }
      else if ( ossStrcasecmp( deployMod.c_str(), 
                               OM_DEPLOY_MOD_DISTRIBUTION ) == 0 )
      {
         rc = _generateCluster( nodeList ) ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "unrecognized deploy mod:type=%s", 
                     deployMod.c_str() ) ;
         goto error ;
      }

      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "generate config failed:rc=%d", rc ) ;
         goto error ;
      }

      {
         BSONArrayBuilder arrBuilder ;
         list<omNodeConf>::iterator iter = nodeList.begin() ;
         while( iter != nodeList.end() )
         {
            BSONObjBuilder builder ;
            builder.append( OM_BSON_FIELD_HOST_NAME, iter->getHostName() ) ;
            builder.append( OM_CONF_DETAIL_EX_DG_NAME, iter->getDataGroupName() ) ;
            builder.append( OM_CONF_DETAIL_DBPATH, iter->getDbPath() ) ;
            builder.append( OM_CONF_DETAIL_SVCNAME, iter->getSvcName() ) ;
            builder.append( OM_CONF_DETAIL_ROLE, iter->getRole() ) ;

            const map<string, string>* pAdditionalMap = iter->getAdditionalMap() ;
            map<string, string>::const_iterator additionalIter ;
            additionalIter = pAdditionalMap->begin() ;
            while( additionalIter != pAdditionalMap->end() )
            {
               builder.append( additionalIter->first, additionalIter->second ) ;
               additionalIter++ ;
            }

            arrBuilder.append( builder.obj() ) ;
            iter++ ;
         }

         BSONObjBuilder confBuilder ;
         confBuilder.append( OM_BSON_FIELD_CONFIG, arrBuilder.arr() ) ;
         confBuilder.append( OM_BSON_BUSINESS_NAME, 
                             _template.getBusinessName() ) ;
         confBuilder.append( OM_BSON_BUSINESS_TYPE, 
                             _template.getBusinessType() ) ;
         confBuilder.append( OM_BSON_DEPLOY_MOD, _template.getDeployMod() ) ;
         bsonConfig = confBuilder.obj() ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
   bsonHostInfo:
   { 
     "HostInfo":[
                   {
                      "HostName":"host1", "ClusterName":"c1", 
                      "Disk":{"Name":"/dev/sdb", Size:"", Mount:"", Used:""},
                      "Config":[{"BusinessName":"b2","dbpath":"", svcname:"", 
                                 "role":"", ... }, ...]
                   }
                    , ... 
                ]
   }
   */
   INT32 omConfigGenerator::_parseCluster( const BSONObj &bsonHostInfo )
   {
      INT32 rc = SDB_OK ;
      _cluster.setPropertyContainer( &_propertyContainer ) ;

      BSONObj confFilter = BSON( OM_BSON_FIELD_CONFIG << "" ) ;
      BSONElement clusterEle = bsonHostInfo.getField( 
                                                     OM_BSON_FIELD_HOST_INFO ) ;
      if ( clusterEle.eoo() || Array != clusterEle.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "hostInfo is not Array:field=%s,type=%d", 
                     OM_BSON_FIELD_HOST_INFO, clusterEle.type() ) ;
         goto error ;
      }
      {
         BSONObjIterator iter( clusterEle.embeddedObject() ) ;
         while ( iter.more() )
         {
            BSONObj oneHostConf ;
            BSONElement ele = iter.next() ;
            if ( Object == ele.type() )
            {
               BSONObj oneHostConf = ele.embeddedObject() ;
               BSONObj config  = oneHostConf.filterFieldsUndotted( confFilter, 
                                                                   true ) ;
               BSONObj oneHost = oneHostConf.filterFieldsUndotted( confFilter, 
                                                                   false ) ;
               rc = _cluster.addHost( oneHost, config ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "add host failed:rc=%d", rc ) ;
                  goto error ;
               }
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   /*
   confProperties:
   {
      "Property":[{"Name":"dbpath", "Type":"path", "Default":"/opt/sequoiadb", 
                      "Valid":"1", "Display":"edit box", "Edit":"false", 
                      "Desc":"", "WebName":"" }
                      , ...
                 ] 
   }
   */
   INT32 omConfigGenerator::_parseProperties( const BSONObj &confProperties )
   {
      INT32 rc = SDB_OK ;
      BSONElement propertyEle ;
      propertyEle = confProperties.getField( OM_BSON_PROPERTY_ARRAY ) ;
      if ( propertyEle.eoo() || Array != propertyEle.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "confProperties's field is not Array:field=%s,"
                     "type=%d", OM_BSON_PROPERTY_ARRAY, propertyEle.type() ) ;
         goto error ;
      }
      {
         BSONObjIterator i( propertyEle.embeddedObject() ) ;
         while ( i.more() )
         {
            BSONElement ele = i.next() ;
            if ( Object == ele.type() )
            {
               BSONObj oneProperty = ele.embeddedObject() ;
               rc = _propertyContainer.addProperty( oneProperty ) ;
               if ( SDB_OK != rc )
               {
                  PD_LOG( PDERROR, "addProperty failed:rc=%d", rc ) ;
                  goto error ;
               }
            }
         }

         if ( !_propertyContainer.isAllPropertySet() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG( PDERROR, "miss property configure" ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   string omConfigGenerator::getErrorDetail()
   {
      return _errorDetail ;
   }

}



