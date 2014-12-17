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

   Source File Name = omConfigGenerator.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/03/2014  LYB Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef OM_CONFIG_GENERATOR_HPP__
#define OM_CONFIG_GENERATOR_HPP__

#include "core.hpp"
#include "oss.hpp"
#include <map>
#include "../bson/bson.h"
#include "omCommandInterface.hpp"

using namespace bson;

namespace engine
{
   class nodeCounter
   {
      public:
         nodeCounter() ;
         ~nodeCounter() ;

      public:
         void                 increaseNode( const string &role ) ;
         INT32                getNodeCount( const string &role ) ;
         INT32                getNodeCount() ;


      private:
         map<string, UINT32>  _mapCounter ;
   } ;

   class businessNodeCounter : public SDBObject
   {
      public:
         businessNodeCounter( const string &businessName ) ;
         ~businessNodeCounter() ;

      public:
         void                 addNode( const string &role ) ;

      private:
         string               _businessName ;
         nodeCounter          _counter ;
   } ;

   class diskNodeCounter : public SDBObject
   {
      public:
         diskNodeCounter( const string &diskName ) ;
         ~diskNodeCounter() ;

      public:
         INT32                addNode( const string &businessName, 
                                       const string &role ) ;
         INT32                getNodeCount() ;
         INT32                getNodeCount( const string &role ) ;

      private:
         string               _diskName ;
         map<string, businessNodeCounter*> _mapBusinessCounter ;
         nodeCounter          _counter ;
   } ;

   class hostNodeCounter : public SDBObject
   {
      public:
         hostNodeCounter( const string &hostName ) ;
         ~hostNodeCounter() ;

      public:
         INT32                addNode( const string &diskName,
                                       const string &businessName, 
                                       const string &role ) ;
         INT32                getNodeCount( const string &role ) ;

         INT32                getNodeCount() ;

         INT32                getNodeCountInDisk( const string &diskName ) ;
         INT32                getNodeCountInDisk( const string &diskName, 
                                                  const string &role ) ;

      private:
         string               _hostName ;
         map<string, diskNodeCounter *>  _mapDiskCounter ;
         nodeCounter          _counter ;
   } ;

   class clusterNodeCounter
   {
      public:
         clusterNodeCounter() ;
         ~clusterNodeCounter() ;

      public:
         INT32                addNode( const string &hostName,
                                       const string &diskName,
                                       const string &businessName, 
                                       const string &role, 
                                       const string &groupName ) ;

         INT32                getCountInHost( const string &hostName, 
                                              const string &role ) ;
         INT32                getCountInHost( const string &hostName ) ;

         INT32                getCountInDisk( const string &hostName,
                                              const string &diskName,
                                              const string &role ) ;
         INT32                getCountInDisk( const string &hostName,
                                              const string &diskName ) ;

         INT32                increaseGroupID() ;
         void                 clear() ;

      private:
         map<string, hostNodeCounter *>  _mapHostNodeCounter ;
         nodeCounter          _counter ;
         INT32                _availableGroupID ;
   } ;

   class rangeValidator : public SDBObject
   {
      public:
         rangeValidator( const string &type, const CHAR *value ) ;
         rangeValidator( const string &type, const CHAR *begin, const CHAR *end, 
                         BOOLEAN isClosed = TRUE ) ;
         ~rangeValidator() ;

      public:
         BOOLEAN        isValid( const string &value ) ;
         string         getType() ;

      private:
         INT32          _compare( string left, string right ) ;
      private:
         string         _type ;
         BOOLEAN        _isClosed ;
         BOOLEAN        _isValidAll ;
         string         _begin ;
         string         _end ;
   } ;

   class confValidator : public SDBObject
   {
      public:
         confValidator() ;
         ~confValidator() ;

      public:
         INT32          init( const string &type, const string &validateStr ) ;
         BOOLEAN        isValid( const string &value ) ;
         string         getType() ;

      private:
         void           _clear() ;
         rangeValidator *_createrangeValidator( const string &value ) ;

      private:
         string                 _type ;
         list<rangeValidator *> _validatorList ;
         typedef list<rangeValidator *>::iterator VALIDATORLIST_ITER ;
   } ;

   class confProperty : public SDBObject
   {
      public:
         confProperty() ;
         ~confProperty() ;

      public:
         INT32          init( const BSONObj &property ) ;
         string         getDefaultValue() ;
         string         getItemName() ;
         string         getType() ;
         BOOLEAN        isValid( const string &value ) ;
         string         getValidString() ;
         string         getErrorDetail() ;

      private:
         string         _name ;
         string         _type ;
         string         _defaultValue ;
         string         _validateStr ;
         confValidator  _confValidator ;
   } ;

   class omNodeConf : public SDBObject
   {
      public:
         omNodeConf() ;
         omNodeConf( const omNodeConf &right ) ;
         ~omNodeConf() ;

      public:
         void              setDbPath( const string &dbpath ) ;
         void              setSvcName( const string &svcName ) ;
         void              setRole( const string &role ) ;
         void              setDataGroupName( const string &dataGroupName ) ;
         void              setHostName( const string &hostName ) ;
         void              setDiskName( const string &diskName ) ;
         void              setAdditionalConf( const string &key, 
                                              const string &value ) ;

         string            getDbPath() ;
         string            getSvcName() ;
         string            getRole() ;
         string            getDataGroupName() ;
         string            getHostName() ;
         string            getDiskName() ;
         string            getAdditionlConf( const string &key ) ;
         const map<string, string> * getAdditionalMap() ;


      private:
         string            _dbPath ;
         string            _svcName ;
         string            _role ;

         map<string, string> _additionalConfMap ;

         string            _dataGroupName ;
         string            _hostName ;
         string            _diskName ;
   } ;

   class propertyContainer : public SDBObject
   {
      public:
         propertyContainer() ;
         ~propertyContainer() ;

      public:
         INT32          addProperty( const BSONObj &property ) ;
         INT32          checkValue( const string &name, const string &value ) ;
         BOOLEAN        isAllPropertySet() ;
         void           clear() ;

         INT32          createSample( omNodeConf &sample ) ;

         string         getDefaultValue( const string &name ) ;

      private:
         confProperty*  _getConfProperty( const string &name ) ;

      private:
         confProperty                  *_dbPathProperty ;
         confProperty                  *_roleProperty ;
         confProperty                  *_svcNameProperty ;
         map<string, confProperty*>    _additionalPropertyMap ;
   } ;

   struct hostResource
   {
      string path ;
      string svcName ;
      string replname ;
      string shardname ;
      string catalogname ;
      string httpname ;
   } ;

   class hostHardWare : public SDBObject
   {
      public:
         hostHardWare( const string &hostName, propertyContainer *pc, 
                       clusterNodeCounter *nodeCounter ) ;
         ~hostHardWare() ;

      public:
         INT32                addDisk( const string &diskName, 
                                       const string &mountPath,
                                       UINT64 totalSize, UINT64 freeSize ) ;
         string               getName() ;
         INT32                occupayResource( const string &path, 
                                               const string &svcName ) ;

         string               getDiskName( const string &dbPath ) ;
         string               getMountPath( const string &dbPath ) ;

         INT32                getDiskCount() ;

         INT32                getFreeDiskCount() ;

         INT32                createNode( const string &role, 
                                          simpleDiskInfo **diskInfo, 
                                          string &svcName ) ;

         BOOLEAN              isDiskExist( const string &dbPath ) ;
         BOOLEAN              isPathOccupayed( const string &dbPath ) ;
         BOOLEAN              isSvcNameOccupayed( const string &svcName ) ;
      private:
         simpleDiskInfo*      _getDiskInfo( const string &dbPath ) ;
         simpleDiskInfo*      _getBestDisk( const string &role ) ;

      private:
         string                      _hostName ;
         map<string, simpleDiskInfo> _mapDisk ;
         set<string>                 _inUseDisk ;
         list<hostResource>          _resourceList ;
         propertyContainer           *_propertyContainer ;
         clusterNodeCounter          *_nodeCounter ;
         string                      _availableSvcName ;
   } ;

   class omCluster 
   {
      public:
         omCluster() ;
         ~omCluster() ;

      public:
         void                 setPropertyContainer( propertyContainer *pc ) ;
         INT32                addHost( const BSONObj &host, 
                                       const BSONObj &config ) ;
         INT32                createNode( const string &businessType,
                                          const string &businessName,
                                          const string &role,
                                          const string &groupName,
                                          string &hostName, string &diskName, 
                                          string &svcName, string &dbPath ) ;
         INT32                getHostNum() ;

         INT32                increaseGroupID() ;

         void                 clear() ;

         INT32                checkAndAddNode( const string &businessName,
                                               omNodeConf *node ) ;

      private:
         hostHardWare*        _getBestHost( const string &role ) ;

      private:
         map<string, hostHardWare*> _mapHost ;
         clusterNodeCounter         _nodeCounter ;
         propertyContainer          *_propertyContainer ;
   } ;

   class omConfTemplate : public SDBObject
   {
      public:
         omConfTemplate() ;
         ~omConfTemplate() ;

      public:
         INT32             init( const BSONObj &bsonTemplate ) ;
         string            getBusinessType() ;
         string            getBusinessName() ;
         string            getClusterName() ;
         string            getDeployMod() ;
         INT32             getReplicaNum() ;
         INT32             getDataNum() ;
         INT32             getDataGroupNum() ;
         INT32             getCatalogNum() ;
         INT32             getCoordNum() ;

         void              setCoordNum( INT32 coordNum ) ;

      public:
         void              clear() ;

      private:
         BOOLEAN           _isAllProperySet() ;
         INT32             _setPropery( BSONObj &property ) ;

      private:
         string            _businessType ;
         string            _businessName ;
         string            _clusterName ;
         string            _deployMod ;
         INT32             _replicaNum ;
         INT32             _dataNum ;
         INT32             _catalogNum ;
         INT32             _dataGroupNum ;
         INT32             _coordNum ;
   } ;

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
   class omBusinessConfigure : public SDBObject
   {
      public:
         omBusinessConfigure() ;
         ~omBusinessConfigure() ;

      public:
         INT32             init( propertyContainer *pc,
                                 const BSONObj &business ) ;
         void              clear() ;

         void              getNodeList( list<omNodeConf> &nodeList ) ;

         string            getBusinessName() ;
      private:
         INT32             _innerCheck() ;
         INT32             _setNodeConf( BSONObj &oneNode, 
                                         omNodeConf &nodeConf ) ;
      private:
         list<omNodeConf>  _nodeList ;
         string            _businessType ;
         string            _businessName ;
         string            _deployMod ;
         string            _clusterName ;
         propertyContainer *_propertyContainer ;
   } ;

   class omConfigGenerator : public SDBObject
   {
      public:
         omConfigGenerator() ;
         virtual ~omConfigGenerator() ;

      public:
         INT32       generateSDBConfig( const BSONObj &bsonTemplate, 
                                        const BSONObj &confProperties, 
                                        const BSONObj &bsonHostInfo, 
                                        BSONObj &bsonConfig ) ;

         INT32       checkSDBConfig( BSONObj &bsonConfValue,
                                     const BSONObj &bsonAllconf, 
                                     const BSONObj &bsonHostInfo ) ;

         string      getErrorDetail() ;

      private:
         INT32       _parseProperties( const BSONObj &confProperties ) ;
         INT32       _parseCluster( const BSONObj &bsonHostInfo ) ;

         INT32       _generate( BSONObj &bsonConfig ) ;
         INT32       _generateCluster( list<omNodeConf> &nodeList ) ;
         INT32       _generateStandAlone( list<omNodeConf> &nodeList ) ;

         INT32       _parseNewBusiness( const BSONObj &newBusinessConf ) ;

      private:
         omConfTemplate    _template ;
         propertyContainer _propertyContainer ;
         omNodeConf        _nodeSample ;
         omCluster         _cluster ;
         string            _errorDetail ;

         omBusinessConfigure _businessConf ;
   } ;
}


#endif /*OM_CONFIG_GENERATOR_HPP__*/


