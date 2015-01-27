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

   Source File Name = clsCommand.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          05/27/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "clsCommand.hpp"
#include "pd.hpp"
#include "pmd.hpp"
#include "clsMgr.hpp"
#include "pdTrace.hpp"
#include "clsTrace.hpp"
#include "rtn.hpp"

using namespace bson ;

namespace engine
{

   IMPLEMENT_CMD_AUTO_REGISTER(_rtnSplit)
   _rtnSplit::_rtnSplit ()
   {
      ossMemset ( _szCollection, 0, sizeof(_szCollection) ) ;
      ossMemset ( _szTargetName, 0, sizeof(_szTargetName) ) ;
      ossMemset ( _szSourceName, 0, sizeof(_szSourceName) ) ;
      _percent = 0.0 ;
   }

   INT32 _rtnSplit::spaceService ()
   {
      return CMD_SPACE_SERVICE_SHARD ;
   }

   _rtnSplit::~_rtnSplit ()
   {
   }

   const CHAR *_rtnSplit::name ()
   {
      return NAME_SPLIT ;
   }

   RTN_COMMAND_TYPE _rtnSplit::type ()
   {
      return CMD_SPLIT ;
   }

   BOOLEAN _rtnSplit::writable ()
   {
      return TRUE ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSPLIT_INIT, "_rtnSplit::init" )
   INT32 _rtnSplit::init ( INT32 flags, INT64 numToSkip, INT64 numToReturn,
                           const CHAR * pMatcherBuff,
                           const CHAR * pSelectBuff,
                           const CHAR * pOrderByBuff,
                           const CHAR * pHintBuff )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSPLIT_INIT ) ;
      const CHAR *pCollectionName = NULL ;
      const CHAR *pTargetName     = NULL ;
      const CHAR *pSourceName     = NULL ;

      try
      {
         BSONObj boRequest ( pMatcherBuff ) ;
         BSONElement beName       = boRequest.getField ( CAT_COLLECTION_NAME ) ;
         BSONElement beTarget     = boRequest.getField ( CAT_TARGET_NAME ) ;
         BSONElement beSplitKey   = boRequest.getField ( CAT_SPLITVALUE_NAME ) ;
         BSONElement beSource     = boRequest.getField ( CAT_SOURCE_NAME ) ;
         BSONElement bePercent    = boRequest.getField ( CAT_SPLITPERCENT_NAME ) ;

         PD_CHECK ( !beName.eoo() && beName.type() == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "Invalid collection name: %s", beName.toString().c_str() ) ;
         pCollectionName = beName.valuestr() ;
         PD_CHECK ( ossStrlen ( pCollectionName ) <
                       DMS_COLLECTION_SPACE_NAME_SZ +
                       DMS_COLLECTION_NAME_SZ + 1,
                    SDB_INVALIDARG, error, PDERROR,
                    "Collection name is too long: %s", pCollectionName ) ;
         ossStrncpy ( _szCollection, pCollectionName,
                         DMS_COLLECTION_SPACE_NAME_SZ +
                          DMS_COLLECTION_NAME_SZ + 1 ) ;
         PD_CHECK ( !beTarget.eoo() && beTarget.type() == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "Invalid target group name: %s",
                    beTarget.toString().c_str() ) ;
         pTargetName = beTarget.valuestr() ;
         PD_CHECK ( ossStrlen ( pTargetName ) < OP_MAXNAMELENGTH,
                    SDB_INVALIDARG, error, PDERROR,
                    "target group name is too long: %s",
                    pTargetName ) ;
         ossStrncpy ( _szTargetName, pTargetName, OP_MAXNAMELENGTH ) ;
         PD_CHECK ( !beSource.eoo() && beSource.type() == String,
                    SDB_INVALIDARG, error, PDERROR,
                    "Invalid source group name: %s",
                    beSource.toString().c_str() ) ;
         pSourceName = beSource.valuestr() ;
         PD_CHECK ( ossStrlen ( pSourceName ) < OP_MAXNAMELENGTH,
                    SDB_INVALIDARG, error, PDERROR,
                    "source group name is too long: %s",
                    pSourceName ) ;
         ossStrncpy ( _szSourceName, pSourceName, OP_MAXNAMELENGTH ) ;
         PD_CHECK ( !beSplitKey.eoo() && beSplitKey.type() == Object,
                    SDB_INVALIDARG, error, PDERROR,
                    "Invalid split key: %s",
                    beSplitKey.toString().c_str() ) ;
         _splitKey = beSplitKey.embeddedObject () ;
         _percent = bePercent.numberDouble() ;
      }
      catch ( std::exception &e )
      {
         PD_RC_CHECK ( SDB_SYS, PDERROR,
                       "Exception handled when parsing split request: %s",
                       e.what() ) ;
      }
      PD_TRACE4 ( SDB__CLSSPLIT_INIT,
                  PD_PACK_STRING ( pCollectionName ),
                  PD_PACK_STRING ( pTargetName ),
                  PD_PACK_STRING ( pSourceName ),
                  PD_PACK_STRING ( _splitKey.toString().c_str() ) ) ;

   done:
      PD_TRACE_EXITRC ( SDB__CLSSPLIT_INIT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSSPLIT_DOIT, "_rtnSplit::doit" )
   INT32 _rtnSplit::doit ( _pmdEDUCB * cb, _SDB_DMSCB * dmsCB,
                           _SDB_RTNCB * rtnCB, _dpsLogWrapper * dpsCB,
                           INT16 w, INT64 * pContextID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSSPLIT_DOIT ) ;
      BSONObjBuilder matchBuilder ;

      matchBuilder.append( CAT_TASKTYPE_NAME, CLS_TASK_SPLIT ) ;
      matchBuilder.append( CAT_COLLECTION_NAME, _szCollection ) ;
      matchBuilder.append( CAT_TARGET_NAME, _szTargetName ) ;
      matchBuilder.append( CAT_SOURCE_NAME, _szSourceName ) ;
      if ( _splitKey.isEmpty() )
      {
         matchBuilder.append( CAT_SPLITPERCENT_NAME, _percent  ) ;
      }
      else
      {
         matchBuilder.append( CAT_SPLITVALUE_NAME, _splitKey ) ;
      }
      BSONObj match = matchBuilder.obj() ;

      rc = sdbGetClsCB()->startTaskCheck ( match ) ;
      PD_TRACE_EXITRC ( SDB__CLSSPLIT_DOIT, rc ) ;
      return rc ;
   }

   IMPLEMENT_CMD_AUTO_REGISTER(_rtnCancelTask)
   INT32 _rtnCancelTask::spaceNode ()
   {
      return CMD_SPACE_NODE_ALL & ( ~CMD_SPACE_NODE_STANDALONE ) ;
   }

   INT32 _rtnCancelTask::init( INT32 flags, INT64 numToSkip,
                               INT64 numToReturn,
                               const CHAR * pMatcherBuff,
                               const CHAR * pSelectBuff,
                               const CHAR * pOrderByBuff,
                               const CHAR * pHintBuff )
   {
      INT32 rc = SDB_OK ;

      try
      {
         BSONObj matcher( pMatcherBuff ) ;
         BSONElement ele = matcher.getField( CAT_TASKID_NAME ) ;
         if ( ele.eoo() || !ele.isNumber() )
         {
            PD_LOG( PDERROR, "Field[%s] type[%d] is error", CAT_TASKID_NAME,
                    ele.type() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         _taskID = (UINT64)ele.numberLong() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnCancelTask::doit( pmdEDUCB * cb, SDB_DMSCB * dmsCB,
                               SDB_RTNCB * rtnCB, SDB_DPSCB * dpsCB,
                               INT16 w, INT64 *pContextID )
   {
      PD_LOG( PDEVENT, "Stop task[ID=%lld]", _taskID ) ;
      return sdbGetClsCB()->stopTask( _taskID ) ;
   }

   IMPLEMENT_CMD_AUTO_REGISTER(_rtnLinkCollection)
   _rtnLinkCollection::_rtnLinkCollection()
   {
      _collectionName = NULL ;
   }

   _rtnLinkCollection::~_rtnLinkCollection()
   {
   }

   INT32 _rtnLinkCollection::spaceService ()
   {
      return CMD_SPACE_SERVICE_SHARD ;
   }

   const CHAR * _rtnLinkCollection::name ()
   {
      return NAME_LINK_COLLECTION;
   }

   RTN_COMMAND_TYPE _rtnLinkCollection::type ()
   {
      return CMD_LINK_COLLECTION ;
   }

   const CHAR *_rtnLinkCollection::collectionFullName ()
   {
      return _collectionName ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSLINKCL_INIT, "_rtnLinkCollection::init" )
   INT32 _rtnLinkCollection::init ( INT32 flags, INT64 numToSkip,
                                    INT64 numToReturn,
                                    const CHAR * pMatcherBuff,
                                    const CHAR * pSelectBuff,
                                    const CHAR * pOrderByBuff,
                                    const CHAR * pHintBuff )
   {
      PD_TRACE_ENTRY ( SDB__CLSLINKCL_INIT ) ;
      INT32 rc = SDB_OK;
      try
      {
         BSONObj arg ( pMatcherBuff ) ;
         rc = rtnGetStringElement ( arg, FIELD_NAME_NAME,
                                    &_collectionName ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR, "Failed to get string [collection] " ) ;
            goto error ;
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSLINKCL_INIT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnLinkCollection::doit ( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                                    _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                                    INT16 w, INT64 *pContextID )
   {
      sdbGetClsCB()->invalidateCata( _collectionName ) ;
      return SDB_OK ;
   }

   IMPLEMENT_CMD_AUTO_REGISTER(_rtnUnlinkCollection)
   _rtnUnlinkCollection::_rtnUnlinkCollection()
   {
      _collectionName = NULL ;
   }

   _rtnUnlinkCollection::~_rtnUnlinkCollection()
   {
   }

   INT32 _rtnUnlinkCollection::spaceService ()
   {
      return CMD_SPACE_SERVICE_SHARD ;
   }

   const CHAR * _rtnUnlinkCollection::name ()
   {
      return NAME_UNLINK_COLLECTION ;
   }

   RTN_COMMAND_TYPE _rtnUnlinkCollection::type ()
   {
      return CMD_UNLINK_COLLECTION ;
   }

   const CHAR *_rtnUnlinkCollection::collectionFullName ()
   {
      return _collectionName ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSUNLINKCL_INIT, "_rtnUnlinkCollection::init" )
   INT32 _rtnUnlinkCollection::init ( INT32 flags, INT64 numToSkip,
                                      INT64 numToReturn,
                                      const CHAR * pMatcherBuff,
                                      const CHAR * pSelectBuff,
                                      const CHAR * pOrderByBuff,
                                      const CHAR * pHintBuff)
   {
      PD_TRACE_ENTRY ( SDB__CLSUNLINKCL_INIT ) ;
      INT32 rc = SDB_OK;
      try
      {
         BSONObj arg ( pMatcherBuff ) ;
         rc = rtnGetStringElement ( arg, FIELD_NAME_NAME,
                                    &_collectionName ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG ( PDERROR, "Failed to get string [collection] " ) ;
            goto error ;
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSUNLINKCL_INIT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnUnlinkCollection::doit ( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                                      _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                                      INT16 w, INT64 *pContextID )
   {
      sdbGetClsCB()->invalidateCata( _collectionName ) ;
      return SDB_OK ;
   }

   IMPLEMENT_CMD_AUTO_REGISTER(_rtnAlterCollection)
   _rtnAlterCollection::_rtnAlterCollection()
   {

   }

   _rtnAlterCollection::~_rtnAlterCollection()
   {

   }

   INT32 _rtnAlterCollection::spaceService()
   {
      return CMD_SPACE_SERVICE_SHARD ;
   }

   
   INT32 _rtnAlterCollection::init ( INT32 flags,
                                     INT64 numToSkip,
                                     INT64 numToReturn,
                                     const CHAR *pMatcherBuff,
                                     const CHAR *pSelectBuff,
                                     const CHAR *pOrderByBuff,
                                     const CHAR *pHintBuff )
   {
      INT32 rc = SDB_OK ;
      try
      {
         _alterObj = BSONObj( pMatcherBuff ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexpected err happened:%s",
                 e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      {
      BSONElement options ;
      BSONElement clName = _alterObj.getField( FIELD_NAME_NAME ) ;
      if ( String != clName.type() )
      {
         PD_LOG( PDERROR, "invalid alter object:%s",
                 _alterObj.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      options = _alterObj.getField( FIELD_NAME_OPTIONS ) ;
      if ( Object != options.type() )
      {
         PD_LOG( PDERROR, "invalid alter object:%s",
                 _alterObj.toString( FALSE, TRUE ).c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   const CHAR *_rtnAlterCollection::collectionFullName()
   {
      return _alterObj.getField( FIELD_NAME_NAME ).valuestrsafe() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNALTERCL_DOIT, "_rtnAlterCollection::doit" )
   INT32 _rtnAlterCollection::doit( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                                    _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                                    INT16 w, INT64 *pContextID )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNALTERCL_DOIT ) ;
      BSONObj idxDef ;
      BSONObj options ;
      BSONElement ensureIndex  ;
      BSONElement shardingKey ;
      options = _alterObj.getField( FIELD_NAME_OPTIONS ).embeddedObject() ;
      shardingKey = options.getField( FIELD_NAME_SHARDINGKEY ) ;

      if ( Object != shardingKey.type() )
      {
         PD_LOG( PDDEBUG, "no sharding key in the alter object, do noting." ) ;
         goto done ;
      }

      ensureIndex = options.getField( FIELD_NAME_ENSURE_SHDINDEX ) ;
      if ( Bool == ensureIndex.type() &&
           !ensureIndex.Bool() )
      {
         PD_LOG( PDDEBUG, "ensureShardingIndex is false, do nothing." ) ;
         goto done ;
      }

      idxDef = BSON( IXM_FIELD_NAME_KEY << shardingKey.embeddedObject()
                     << IXM_FIELD_NAME_NAME << IXM_SHARD_KEY_NAME
                     << "v"<<0 ) ;

      rc = rtnCreateIndexCommand( collectionFullName(),
                                  idxDef,
                                  cb, dmsCB, dpsCB, TRUE ) ;
      if ( SDB_IXM_REDEF == rc )
      {
         rc = SDB_OK ;
         goto done ;
      }
      else if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to create sharding key index:%d", rc ) ;
         goto error ;
      }
      else
      {
        /*
         catAgent *catAgent = sdbGetShardCB()->getCataAgent() ;
         catAgent->lock_w() ;
         catAgent->clear( collectionFullName() ) ;
         catAgent->release_w() ;
         sdbGetClsCB()->invalidateCata( collectionFullName() ) ;*/
      }
   done:
      PD_TRACE_EXITRC( SDB__RTNALTERCL_DOIT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   IMPLEMENT_CMD_AUTO_REGISTER(_rtnInvalidateCache)
   _rtnInvalidateCache::_rtnInvalidateCache()
   {

   }

   _rtnInvalidateCache::~_rtnInvalidateCache()
   {

   }

   INT32 _rtnInvalidateCache::spaceNode()
   {
      return CMD_SPACE_NODE_DATA | CMD_SPACE_NODE_CATA  ;
   }

   INT32 _rtnInvalidateCache::init ( INT32 flags,
                                     INT64 numToSkip,
                                     INT64 numToReturn,
                                     const CHAR *pMatcherBuff,
                                     const CHAR *pSelectBuff,
                                     const CHAR *pOrderByBuff,
                                     const CHAR *pHintBuff )
   {
      return SDB_OK ;
   }

   INT32 _rtnInvalidateCache::doit ( _pmdEDUCB *cb,
                                     SDB_DMSCB *dmsCB,
                                     _SDB_RTNCB *rtnCB,
                                     _dpsLogWrapper *dpsCB,
                                     INT16 w,
                                     INT64 *pContextID )
   {
      sdbGetShardCB()->getCataAgent()->clearAll() ;
      sdbGetShardCB()->getNodeMgrAgent()->clearAll() ;
      return  SDB_OK ;
   }

}

