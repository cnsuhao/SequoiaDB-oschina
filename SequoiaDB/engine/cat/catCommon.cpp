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

   Source File Name = catCommon.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          10/07/2013  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmd.hpp"
#include "pmdCB.hpp"
#include "catCommon.hpp"
#include "pdTrace.hpp"
#include "catTrace.hpp"
#include "rtn.hpp"
#include "fmpDef.hpp"
#include "clsCatalogAgent.hpp"

#include "../bson/lib/md5.hpp"

using namespace bson ;

namespace engine
{

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGROUPNAMEVALIDATE, "catGroupNameValidate" )
   INT32 catGroupNameValidate ( const CHAR *pName, BOOLEAN isSys )
   {
      INT32 rc = SDB_INVALIDARG ;
      PD_TRACE_ENTRY ( SDB_CATGROUPNAMEVALIDATE ) ;

      if ( !pName || pName[0] == '\0' )
      {
         PD_LOG ( PDWARNING, "group name can't be empty" ) ;
         goto error ;
      }
      PD_TRACE2 ( SDB_CATGROUPNAMEVALIDATE, PD_PACK_STRING ( pName ),
                  PD_PACK_UINT ( isSys ) ) ;
      if ( ossStrlen ( pName ) > OSS_MAX_GROUPNAME_SIZE )
      {
         PD_LOG ( PDWARNING, "group name %s is too long",
                  pName ) ;
         goto error ;
      }
      if ( !isSys &&
           ( ossStrncmp ( pName, "SYS", ossStrlen ( "SYS" ) ) == 0 ||
             ossStrncmp ( pName, "$", ossStrlen ( "$" ) ) == 0 ) )
      {
         PD_LOG ( PDWARNING, "group name should not start with SYS nor $: %s",
                  pName ) ;
         goto error ;
      }
      if ( ossStrchr ( pName, '.' ) != NULL )
      {
         PD_LOG ( PDWARNING, "group name should not contain dot(.): %s",
                  pName ) ;
         goto error ;
      }

      rc = SDB_OK ;

   done :
      PD_TRACE_EXITRC ( SDB_CATGROUPNAMEVALIDATE, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   INT32 catDomainNameValidate( const CHAR * pName )
   {
      return catGroupNameValidate( pName, FALSE ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATDOMAINOPTIONSEXTRACT, "catDomainOptionsExtract" )
   INT32 catDomainOptionsExtract( const BSONObj &options,
                                  pmdEDUCB *cb,
                                  BSONObjBuilder *builder )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB_CATDOMAINOPTIONSEXTRACT ) ;

      std::set <std::string> groupNameList ;
      INT32 expectedOptSize = 0 ;
      BSONElement beGroupList = options.getField ( CAT_GROUPS_NAME ) ;
      if ( !beGroupList.eoo() && beGroupList.type() != Array )
      {
         PD_LOG ( PDERROR, "group list must be array" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      if ( beGroupList.type() == Array )
      {
         BSONArrayBuilder gpInfoBuilder ;
         BSONObjIterator it ( beGroupList.embeddedObject() ) ;
         while ( it.more() )
         {
            BSONObj groupInfo ;
            BSONObjBuilder oneGroup ;
            BSONElement gpID ;
            BSONElement gpName ;
            BSONElement beGroupElement = it.next () ;
            if ( beGroupElement.type() != String )
            {
               PD_LOG ( PDERROR, "Each element in group list must be string" ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }

            rc = catGetGroupObj( beGroupElement.valuestr(),
                                 TRUE,
                                 groupInfo,
                                 cb ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get group info of [%s]",
                       beGroupElement.valuestr() ) ;
               goto error ;
            }

            if ( !groupNameList.insert ( beGroupElement.valuestr() ).second )
            {
               PD_LOG ( PDERROR, "Duplicate group name: %s",
                        beGroupElement.valuestr() ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }

            gpName = groupInfo.getField( CAT_GROUPNAME_NAME ) ;
            SDB_ASSERT( !gpName.eoo(), "can not be eoo" ) ;
            if ( !gpName.eoo() && NULL != builder )
            {
               oneGroup.append( gpName ) ;
            }

            gpID = groupInfo.getField( CAT_GROUPID_NAME ) ;
            SDB_ASSERT( !gpID.eoo(), "can not be eoo" ) ;
            if ( !gpID.eoo() && NULL != builder )
            {
               oneGroup.append( gpID ) ;
            }

            if ( NULL != builder )
            {
               gpInfoBuilder << oneGroup.obj() ;
            }
         }

         if ( NULL != builder )
         {
            builder->append( CAT_GROUPS_NAME, gpInfoBuilder.arr() ) ;
         }
         ++ expectedOptSize ;
      }

      {
      BSONElement autoSplit = options.getField( CAT_DOMAIN_AUTO_SPLIT ) ;
      if ( !autoSplit.eoo() && autoSplit.isBoolean() )
      {
         if ( NULL != builder )
         {
            builder->append( autoSplit ) ;
         }
         ++expectedOptSize ;
      }
      }

      {
      BSONElement autoRebalance = options.getField( CAT_DOMAIN_AUTO_REBALANCE ) ;
      if ( !autoRebalance.eoo() && autoRebalance.isBoolean() )
      {
         if ( NULL != builder )
         {
            builder->append( autoRebalance ) ;
         }
         ++expectedOptSize ;
      }
      } 
      
      if ( options.nFields() != expectedOptSize )
      {
         PD_LOG ( PDERROR, "Actual input doesn't match expected opt size, "
                  "there could be one or more invalid arguments in options" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB_CATDOMAINOPTIONSEXTRACT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATRESOLVECOLLECTIONNAME, "catResolveCollectionName" )
   INT32 catResolveCollectionName ( const CHAR *pInput, UINT32 inputLen,
                                    CHAR *pSpaceName, UINT32 spaceNameSize,
                                    CHAR *pCollectionName,
                                    UINT32 collectionNameSize )
   {
      INT32 rc = SDB_OK ;
      UINT32 curPos = 0 ;
      UINT32 i = 0 ;
      PD_TRACE_ENTRY ( SDB_CATRESOLVECOLLECTIONNAME ) ;
      while ( pInput[curPos] != '.' )
      {
         if ( curPos >= inputLen || i >= spaceNameSize )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         pSpaceName[ i++ ] = pInput[ curPos++ ] ;
      }
      pSpaceName[i] = '\0' ;

      i = 0 ;
      ++curPos ;
      while ( curPos < inputLen )
      {
         if ( i >= collectionNameSize )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         pCollectionName[ i++ ] = pInput[ curPos++ ] ;
      }
      pCollectionName[i] = '\0' ;

   done:
      PD_TRACE_EXITRC ( SDB_CATRESOLVECOLLECTIONNAME, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATQUERYANDGETMORE, "catQueryAndGetMore" )
   INT32 catQueryAndGetMore ( MsgOpReply **ppReply,
                              const CHAR *collectionName,
                              BSONObj &selector,
                              BSONObj &matcher,
                              BSONObj &orderBy,
                              BSONObj &hint,
                              SINT32 flags,
                              pmdEDUCB *cb,
                              SINT64 numToSkip,
                              SINT64 numToReturn )
   {
      INT32 rc               = SDB_OK ;
      SINT64 contextID       = -1 ;
      pmdKRCB *pKRCB          = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB        = pKRCB->getDMSCB() ;
      SDB_RTNCB *rtnCB        = pKRCB->getRTNCB() ;
      SINT32 replySize       = sizeof(MsgOpReply) ;
      SINT32 replyBufferSize = 0 ;

      PD_TRACE_ENTRY ( SDB_CATQUERYANDGETMORE ) ;
      rc = rtnReallocBuffer ( (CHAR**)ppReply, &replyBufferSize, replySize,
                              SDB_PAGE_SIZE ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to realloc buffer, rc = %d", rc ) ;
      ossMemset ( *ppReply, 0, replySize ) ;

      rc = rtnQuery ( collectionName, selector, matcher, orderBy, hint, flags,
                      cb, numToSkip, numToReturn, dmsCB, rtnCB, contextID ) ;
      PD_RC_CHECK ( rc, PDERROR, "Failed to perform query, rc = %d", rc ) ;

      while ( TRUE )
      {
         rtnContextBuf buffObj ;
         rc = rtnGetMore ( contextID, -1, buffObj, cb, rtnCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC == rc )
            {
               contextID = -1 ;
               rc = SDB_OK ;
               break ;
            }
            PD_LOG( PDERROR, "Failed to retreive record, rc = %d", rc ) ;
            goto error ;
         }

         replySize = ossRoundUpToMultipleX ( replySize, 4 ) ;
         rc = rtnReallocBuffer ( (CHAR**)ppReply, &replyBufferSize,
                                 replySize + buffObj.size(), SDB_PAGE_SIZE ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to realloc buffer, rc = %d", rc ) ;

         ossMemcpy ( &((CHAR*)(*ppReply))[replySize], buffObj.data(),
                     buffObj.size() ) ;
         (*ppReply)->numReturned += buffObj.recordNum() ;
         replySize               += buffObj.size() ;
      }
      (*ppReply)->header.messageLength = replySize ;
      (*ppReply)->flags                = SDB_OK ;
      (*ppReply)->contextID            = -1 ;

   done :
      if ( -1 != contextID )
      {
         rtnCB->contextDelete ( contextID, cb ) ;
      }
      PD_TRACE_EXITRC ( SDB_CATQUERYANDGETMORE, rc ) ;
      return rc ;
   error :
      if ( *ppReply )
      {
         SDB_OSS_FREE( (CHAR*)(*ppReply) ) ;
         *ppReply = NULL ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETONEOBJ, "catGetOneObj" )
   INT32 catGetOneObj( const CHAR * collectionName,
                       const BSONObj & selector,
                       const BSONObj & matcher,
                       const BSONObj & hint,
                       pmdEDUCB * cb,
                       BSONObj & obj )
   {
      INT32 rc                = SDB_OK ;
      SINT64 contextID        = -1 ;
      pmdKRCB *pKRCB          = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB        = pKRCB->getDMSCB() ;
      SDB_RTNCB *rtnCB        = pKRCB->getRTNCB() ;
      BSONObj dummyObj ;

      rtnContextBuf buffObj ;

      PD_TRACE_ENTRY ( SDB_CATGETONEOBJ ) ;
      rc = rtnQuery( collectionName, selector, matcher, dummyObj, hint,
                     0, cb, 0, 1, dmsCB, rtnCB, contextID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to query from %s, rc: %d",
                   collectionName, rc ) ;

      rc = rtnGetMore( contextID, 1, buffObj, cb, rtnCB ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            contextID = -1 ;
         }
         goto error ;
      }

      try
      {
         BSONObj resultObj( buffObj.data() ) ;
         obj = resultObj.copy() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      if ( -1 != contextID )
      {
         buffObj.release() ;
         rtnCB->contextDelete( contextID, cb ) ;
      }
      PD_TRACE_EXITRC ( SDB_CATGETONEOBJ, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETGROUPOBJ, "catGetGroupObj" )
   INT32 catGetGroupObj( const CHAR * groupName, BOOLEAN dataGroupOnly,
                         BSONObj & obj, pmdEDUCB *cb  )
   {
      INT32 rc           = SDB_OK;

      PD_TRACE_ENTRY ( SDB_CATGETGROUPOBJ ) ;
      BSONObj dummyObj ;
      BSONObj boMatcher ;
      boMatcher = BSON( CAT_GROUPNAME_NAME << groupName ) ;

      rc = catGetOneObj( CAT_NODE_INFO_COLLECTION, dummyObj, boMatcher,
                         dummyObj, cb, obj ) ;
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_CLS_GRP_NOT_EXIST ;
      }
      else if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 boMatcher.toString().c_str(), CAT_NODE_INFO_COLLECTION, rc ) ;
         goto error ;
      }
      else if ( dataGroupOnly )
      {
         BSONElement e = obj.getField( CAT_ROLE_NAME ) ;
         if ( !e.isNumber() || SDB_ROLE_DATA != e.numberInt() )
         {
            rc = SDB_CAT_IS_NOT_DATAGROUP ;
            goto error ;
         }
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATGETGROUPOBJ, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETGROUPOBJ1, "catGetGroupObj" )
   INT32 catGetGroupObj( UINT32 groupID, BSONObj &obj, pmdEDUCB *cb )
   {
      INT32 rc           = SDB_OK;

      PD_TRACE_ENTRY ( SDB_CATGETGROUPOBJ1 ) ;
      BSONObj dummyObj ;
      BSONObj boMatcher = BSON( CAT_GROUPID_NAME << groupID );

      rc = catGetOneObj( CAT_NODE_INFO_COLLECTION, dummyObj, boMatcher,
                         dummyObj, cb, obj ) ;
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_CLS_GRP_NOT_EXIST ;
      }
      else if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 boMatcher.toString().c_str(), CAT_NODE_INFO_COLLECTION, rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATGETGROUPOBJ1, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETGROUPOBJ2, "catGetGroupObj" )
   INT32 catGetGroupObj( UINT16 nodeID, BSONObj &obj, pmdEDUCB *cb )
   {
      INT32 rc           = SDB_OK;

      PD_TRACE_ENTRY ( SDB_CATGETGROUPOBJ2 ) ;
      BSONObj dummyObj ;
      BSONObj boMatcher = BSON(
            FIELD_NAME_GROUP"."FIELD_NAME_NODEID << nodeID );

      rc = catGetOneObj( CAT_NODE_INFO_COLLECTION, dummyObj, boMatcher,
                         dummyObj, cb, obj ) ;
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_CLS_NODE_NOT_EXIST ;
      }
      else if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 boMatcher.toString().c_str(), CAT_NODE_INFO_COLLECTION, rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATGETGROUPOBJ2, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGROUPCHECK, "catGroupCheck" )
   INT32 catGroupCheck( const CHAR * groupName, BOOLEAN & exist, pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATGROUPCHECK ) ;
      BSONObj boGroupInfo ;
      rc = catGetGroupObj( groupName, FALSE, boGroupInfo, cb ) ;
      if ( SDB_OK == rc )
      {
         exist = TRUE ;
      }
      else if ( SDB_CLS_GRP_NOT_EXIST == rc )
      {
         rc = SDB_OK;
         exist = FALSE;
      }
      PD_TRACE_EXITRC ( SDB_CATGROUPCHECK, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATSERVICECHECK, "catServiceCheck" )
   INT32 catServiceCheck( const CHAR * hostName, const CHAR * serviceName,
                          BOOLEAN & exist, pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATSERVICECHECK ) ;
      BSONObj groupInfo ;
      BSONObj dummyObj ;
      BSONObj match = BSON( FIELD_NAME_GROUP << BSON( "$elemMatch" <<
                            BSON( FIELD_NAME_SERVICE"."FIELD_NAME_NAME <<
                                  serviceName << FIELD_NAME_HOST <<
                                  hostName )) ) ;

      rc = catGetOneObj( CAT_NODE_INFO_COLLECTION, dummyObj, match,
                         dummyObj, cb, groupInfo ) ;
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_OK ;
         exist = FALSE ;
      }
      else if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 match.toString().c_str(), CAT_NODE_INFO_COLLECTION, rc ) ;
      }
      else
      {
         exist = TRUE ;
      }
      PD_TRACE_EXITRC ( SDB_CATSERVICECHECK, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGROUPID2NAME, "catGroupID2Name" )
   INT32 catGroupID2Name( INT32 groupID, string & groupName, pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj groupObj ;
      const CHAR *name = NULL ;

      PD_TRACE_ENTRY ( SDB_CATGROUPID2NAME ) ;
      rc = catGetGroupObj( (UINT32)groupID, groupObj, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get group obj by id[%d], rc: %d",
                   groupID, rc ) ;

      rc = rtnGetStringElement( groupObj, CAT_GROUPNAME_NAME, &name ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_GROUPNAME_NAME, rc ) ;

      groupName = name ;

   done:
      PD_TRACE_EXITRC ( SDB_CATGROUPID2NAME, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGROUPNAME2ID, "catGroupName2ID" )
   INT32 catGroupName2ID( const CHAR * groupName, INT32 & groupID,
                          pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj groupObj ;

      PD_TRACE_ENTRY ( SDB_CATGROUPNAME2ID ) ;
      rc = catGetGroupObj( groupName, FALSE, groupObj, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get group obj by name[%s], rc: %d",
                   groupName, rc ) ;

      rc = rtnGetIntElement( groupObj, CAT_GROUPID_NAME, groupID ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                   CAT_GROUPID_NAME, rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATGROUPNAME2ID, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 catGroupCount( INT64 & count, pmdEDUCB * cb )
   {
      INT32 rc                = SDB_OK ;
      SDB_DMSCB *dmsCB        = pmdGetKRCB()->getDMSCB() ;
      dmsStorageUnit *su      = NULL ;
      dmsStorageUnitID suID   = DMS_INVALID_CS ;
      const CHAR *pCollectionShortName = NULL ;

      rc = rtnResolveCollectionNameAndLock ( CAT_NODE_INFO_COLLECTION,
                                             dmsCB, &su,
                                             &pCollectionShortName, suID ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to resolve collection name %s, rc: %d",
                  CAT_NODE_INFO_COLLECTION, rc ) ;
         goto error ;
      }

      rc = su->countCollection( pCollectionShortName, count, cb, NULL ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to count collection: %s, rc: %d",
                 pCollectionShortName, rc ) ;
         goto error ;
      }

   done:
      if ( DMS_INVALID_CS != suID )
      {
         dmsCB->suUnlock( suID ) ;
      }
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETDOMAINOBJ, "catGetDomainObj" )
   INT32 catGetDomainObj( const CHAR * domainName, BSONObj & obj, pmdEDUCB * cb )
   {
      INT32 rc           = SDB_OK;

      PD_TRACE_ENTRY ( SDB_CATGETDOMAINOBJ ) ;
      BSONObj dummyObj ;
      BSONObj boMatcher = BSON( CAT_DOMAINNAME_NAME << domainName );

      rc = catGetOneObj( CAT_DOMAIN_COLLECTION, dummyObj, boMatcher,
                         dummyObj, cb, obj ) ;
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_CAT_DOMAIN_NOT_EXIST ;
      }
      else if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 boMatcher.toString().c_str(), CAT_DOMAIN_COLLECTION, rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATGETDOMAINOBJ, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATDOMAINCHECK, "catDomainCheck" )
   INT32 catDomainCheck( const CHAR * domainName, BOOLEAN & exist,
                         pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj obj ;

      PD_TRACE_ENTRY ( SDB_CATDOMAINCHECK ) ;
      rc = catGetDomainObj( domainName, obj, cb ) ;
      if ( SDB_OK == rc )
      {
         exist = TRUE ;
      }
      else if ( SDB_CAT_DOMAIN_NOT_EXIST == rc )
      {
         rc = SDB_OK ;
         exist = FALSE ;
      }
      PD_TRACE_EXITRC ( SDB_CATDOMAINCHECK, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETDOMAINGROUPS, "catGetDomainGroups" )
   INT32 catGetDomainGroups( const BSONObj & domain,
                             map < string, INT32 > & groups )
   {
      INT32 rc = SDB_OK ;
      const CHAR *groupName = NULL ;
      INT32 groupID = CAT_INVALID_GROUPID ;

      PD_TRACE_ENTRY ( SDB_CATGETDOMAINGROUPS ) ;
      BSONElement beGroups = domain.getField( CAT_GROUPS_NAME ) ;
      if ( beGroups.eoo() )
      {
         rc = SDB_CAT_NO_GROUP_IN_DOMAIN ;
         goto error ;
      }

      if ( Array != beGroups.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "Domin group type error, Domain info: %s",
                 domain.toString().c_str() ) ;
         goto error ;
      }

      {
         BSONObjIterator i( beGroups.embeddedObject() ) ;
         while ( i.more() )
         {
            BSONElement ele = i.next() ;
            PD_CHECK( Object == ele.type(), SDB_INVALIDARG, error, PDERROR,
                      "Domain group ele type is not object, Domain info: %s",
                      domain.toString().c_str() ) ;

            BSONObj boGroup = ele.embeddedObject() ;
            rc = rtnGetStringElement( boGroup, CAT_GROUPNAME_NAME,
                                      &groupName ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                         CAT_GROUPNAME_NAME, rc ) ;
            rc = rtnGetIntElement( boGroup, CAT_GROUPID_NAME, groupID ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                         CAT_GROUPID_NAME, rc ) ;

            groups[ groupName ] = groupID ;
         }
      }

      if ( groups.empty() )
      {
         rc = SDB_CAT_NO_GROUP_IN_DOMAIN ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATGETDOMAINGROUPS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETDOMAINGROUPS1, "catGetDomainGroups" )
   INT32 catGetDomainGroups( const BSONObj &domain,
                             vector< INT32 > &groupIDs )
   {
      INT32 rc = SDB_OK ;
      INT32 groupID = CAT_INVALID_GROUPID ;

      PD_TRACE_ENTRY ( SDB_CATGETDOMAINGROUPS1 ) ;
      BSONElement beGroups = domain.getField( CAT_GROUPS_NAME ) ;
      if ( beGroups.eoo() )
      {
         rc = SDB_CAT_NO_GROUP_IN_DOMAIN ;
         goto error ;
      }

      if ( Array != beGroups.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "Domin group type error, Domain info: %s",
                 domain.toString().c_str() ) ;
         goto error ;
      }

      {
         BSONObjIterator i( beGroups.embeddedObject() ) ;
         while ( i.more() )
         {
            BSONElement ele = i.next() ;
            PD_CHECK( ele.isABSONObj(), SDB_INVALIDARG, error, PDERROR,
                      "Domain group ele type is not object, Domain info: %s",
                      domain.toString().c_str() ) ;

            BSONObj boGroup = ele.embeddedObject() ;
            rc = rtnGetIntElement( boGroup, CAT_GROUPID_NAME, groupID ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to get field[%s], rc: %d",
                         CAT_GROUPID_NAME, rc ) ;

            groupIDs.push_back( groupID ) ;
         }
      }

      if ( groupIDs.empty() )
      {
         rc = SDB_CAT_NO_GROUP_IN_DOMAIN ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATGETDOMAINGROUPS1, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATADDGRP2DOMAIN, "catAddGroup2Domain" )
   INT32 catAddGroup2Domain( const CHAR *domainName, const CHAR *groupName,
                             INT32 groupID, pmdEDUCB *cb,
                             _SDB_DMSCB *dmsCB, _dpsLogWrapper *dpsCB,
                             INT16 w )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATADDGRP2DOMAIN ) ;
      BSONObj boMatcher = BSON( CAT_DOMAINNAME_NAME << domainName ) ;

      BSONObjBuilder updateBuild ;
      BSONObjBuilder sub( updateBuild.subobjStart("$addtoset") ) ;

      BSONObj newGroupObj = BSON( CAT_GROUPNAME_NAME << groupName <<
                                  CAT_GROUPID_NAME << groupID ) ;
      BSONObjBuilder sub2( sub.subarrayStart( CAT_GROUPS_NAME ) ) ;
      sub2.append( "0", newGroupObj ) ;
      sub2.done() ;

      sub.done() ;
      BSONObj updator = updateBuild.obj() ;
      BSONObj hint ;

      rc = rtnUpdate( CAT_DOMAIN_COLLECTION, boMatcher, updator,
                      hint, 0, cb, dmsCB, dpsCB, w ) ;

      PD_RC_CHECK( rc, PDERROR, "Failed to update collection: %s, match: %s, "
                   "updator: %s, rc: %d", CAT_DOMAIN_COLLECTION,
                   boMatcher.toString().c_str(),
                   updator.toString().c_str(), rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATADDGRP2DOMAIN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATDELGRPFROMDOMAIN, "catDelGroupFromDomain" )
   INT32 catDelGroupFromDomain( const CHAR *domainName, const CHAR *groupName,
                                INT32 groupID, pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                                _dpsLogWrapper *dpsCB, INT16 w )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATDELGRPFROMDOMAIN ) ;
      BSONObj modifier = BSON( "$pull" << BSON( CAT_GROUPS_NAME <<
                               BSON( CAT_GROUPNAME_NAME << groupName <<
                                     CAT_GROUPID_NAME << groupID ) ) ) ;
      BSONObj modifier2 = BSON( "$pull" << BSON( CAT_GROUPS_NAME <<
                                BSON( CAT_GROUPID_NAME << groupID <<
                                      CAT_GROUPNAME_NAME << groupName ) ) ) ;
      BSONObj matcher ;
      BSONObj dummy ;

      if ( domainName && domainName[0] != 0 )
      {
         matcher = BSON( CAT_DOMAINNAME_NAME << domainName ) ;
      }
      else
      {
         matcher = BSON( CAT_GROUPS_NAME"."CAT_GROUPID_NAME << groupID ) ;
      }

      rc = rtnUpdate( CAT_DOMAIN_COLLECTION, matcher, modifier,
                      dummy, 0, cb, dmsCB, dpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to update collection: %s, match: %s, "
                   "updator: %s, rc: %d", CAT_COLLECTION_SPACE_COLLECTION,
                   matcher.toString().c_str(), modifier.toString().c_str(),
                   rc ) ;

      rc = rtnUpdate( CAT_DOMAIN_COLLECTION, matcher, modifier2,
                      dummy, 0, cb, dmsCB, dpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to update collection: %s, match: %s, "
                   "updator: %s, rc: %d", CAT_COLLECTION_SPACE_COLLECTION,
                   matcher.toString().c_str(), modifier2.toString().c_str(),
                   rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATDELGRPFROMDOMAIN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CAATADDCL2CS, "catAddCL2CS" )
   INT32 catAddCL2CS( const CHAR * csName, const CHAR * clName,
                      pmdEDUCB * cb, SDB_DMSCB * dmsCB, SDB_DPSCB * dpsCB,
                      INT16 w )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CAATADDCL2CS ) ;
      BSONObj boMatcher = BSON( CAT_COLLECTION_SPACE_NAME << csName ) ;

      BSONObjBuilder updateBuild ;
      BSONObjBuilder sub( updateBuild.subobjStart("$addtoset") ) ;

      BSONObj newCLObj = BSON( CAT_COLLECTION_NAME << clName ) ;
      BSONObjBuilder sub1( sub.subarrayStart( CAT_COLLECTION ) ) ;
      sub1.append( "0", newCLObj ) ;
      sub1.done() ;

      sub.done() ;
      BSONObj updator = updateBuild.obj() ;
      BSONObj hint ;

      rc = rtnUpdate( CAT_COLLECTION_SPACE_COLLECTION, boMatcher, updator,
                      hint, 0, cb, dmsCB, dpsCB, w ) ;

      PD_RC_CHECK( rc, PDERROR, "Failed to update collection: %s, match: %s, "
                   "updator: %s, rc: %d", CAT_COLLECTION_SPACE_COLLECTION,
                   boMatcher.toString().c_str(),
                   updator.toString().c_str(), rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CAATADDCL2CS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATDELCLFROMCS, "catDelCLFromCS" )
   INT32 catDelCLFromCS( const CHAR * csName, const CHAR * clName,
                         pmdEDUCB * cb, SDB_DMSCB * dmsCB, SDB_DPSCB * dpsCB,
                         INT16 w )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATDELCLFROMCS ) ;
      BSONObj modifier = BSON( "$pull" << BSON( CAT_COLLECTION <<
                               BSON( CAT_COLLECTION_NAME << clName ) ) ) ;
      BSONObj matcher = BSON( CAT_COLLECTION_SPACE_NAME << csName ) ;
      BSONObj dummy ;

      rc = rtnUpdate( CAT_COLLECTION_SPACE_COLLECTION, matcher, modifier,
                      dummy, 0, cb, dmsCB, dpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to update collection: %s, match: %s, "
                   "updator: %s, rc: %d", CAT_COLLECTION_SPACE_COLLECTION,
                   matcher.toString().c_str(), modifier.toString().c_str(),
                   rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATDELCLFROMCS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATRESTORECS, "catRestoreCS" )
   INT32 catRestoreCS( const CHAR * csName, const BSONObj & oldInfo,
                       pmdEDUCB * cb, SDB_DMSCB * dmsCB,
                       SDB_DPSCB * dpsCB, INT16 w )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATRESTORECS ) ;
      BSONObj boMatcher = BSON( CAT_COLLECTION_SPACE_NAME << csName ) ;
      BSONObj updator   = BSON( "$set" << oldInfo ) ;
      BSONObj hint ;

      rc = rtnUpdate( CAT_COLLECTION_SPACE_COLLECTION, boMatcher, updator,
                      hint, 0, cb, dmsCB, dpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to update collection: %s, match: %s, "
                   "updator: %s, rc: %d", CAT_COLLECTION_SPACE_COLLECTION,
                   boMatcher.toString().c_str(),
                   updator.toString().c_str(), rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATRESTORECS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATCHECKSPACEEXIST, "catCheckSpaceExist" )
   INT32 catCheckSpaceExist( const char *pSpaceName, BOOLEAN &isExist,
                             BSONObj &obj, pmdEDUCB *cb )
   {
      INT32 rc           = SDB_OK ;
      isExist            = FALSE ;

      PD_TRACE_ENTRY ( SDB_CATCHECKSPACEEXIST ) ;
      BSONObj matcher = BSON( CAT_COLLECTION_SPACE_NAME << pSpaceName ) ;
      BSONObj dummyObj ;

      rc = catGetOneObj( CAT_COLLECTION_SPACE_COLLECTION, dummyObj, matcher,
                         dummyObj, cb, obj ) ;
      if ( SDB_DMS_EOC == rc )
      {
         isExist = FALSE ;
         rc = SDB_OK ;
      }
      else if ( SDB_OK == rc )
      {
         isExist = TRUE ;
      }
      else
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 matcher.toString().c_str(), CAT_COLLECTION_SPACE_COLLECTION,
                 rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATCHECKSPACEEXIST, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATREMOVECL, "catRemoveCL" )
   INT32 catRemoveCL( const CHAR * clFullName, pmdEDUCB * cb,
                      SDB_DMSCB * dmsCB, SDB_DPSCB * dpsCB, INT16 w )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATREMOVECL ) ;
      BSONObj boMatcher = BSON( CAT_CATALOGNAME_NAME << clFullName ) ;
      BSONObj dummyObj ;

      rc = rtnDelete( CAT_COLLECTION_INFO_COLLECTION, boMatcher, dummyObj,
                      0, cb, dmsCB, dpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to del record from collection: %s, "
                   "match: %s, rc: %d", CAT_COLLECTION_INFO_COLLECTION,
                   boMatcher.toString().c_str(), rc ) ;
   done:
      PD_TRACE_EXITRC ( SDB_CATREMOVECL, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATCHECKCOLLECTIONEXIST, "catCheckCollectionExist" )
   INT32 catCheckCollectionExist( const char *pCollectionName,
                                  BOOLEAN &isExist,
                                  BSONObj &obj,
                                  pmdEDUCB *cb )
   {
      INT32 rc           = SDB_OK ;
      isExist            = FALSE ;

      PD_TRACE_ENTRY ( SDB_CATCHECKCOLLECTIONEXIST ) ;
      BSONObj matcher = BSON( CAT_CATALOGNAME_NAME << pCollectionName ) ;
      BSONObj dummyObj ;

      rc = catGetOneObj( CAT_COLLECTION_INFO_COLLECTION, dummyObj, matcher,
                         dummyObj, cb, obj ) ;
      if ( SDB_DMS_EOC == rc )
      {
         isExist = FALSE ;
         rc = SDB_OK ;
      }
      else if ( SDB_OK == rc )
      {
         isExist = TRUE ;
      }
      else
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 matcher.toString().c_str(), CAT_COLLECTION_INFO_COLLECTION,
                 rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB_CATCHECKCOLLECTIONEXIST, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATUPDATECATALOG, "catUpdateCatalog" )
   INT32 catUpdateCatalog( const CHAR * clFullName, const BSONObj & cataInfo,
                           pmdEDUCB * cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = krcb->getDPSCB() ;

      PD_TRACE_ENTRY ( SDB_CATUPDATECATALOG ) ;
      BSONObj dummy ;
      BSONObj match = BSON( CAT_CATALOGNAME_NAME << clFullName ) ;
      BSONObj updator = BSON( "$inc" << BSON( CAT_VERSION_NAME << 1 ) <<
                              "$set" << cataInfo ) ;

      rc = rtnUpdate( CAT_COLLECTION_INFO_COLLECTION, match, updator, dummy, 0,
                      cb, dmsCB, dpsCB, w ) ;
      PD_RC_CHECK( rc, PDSEVERE, "Failed to update collection[%s] catalog info"
                   "[%s], rc: %d", clFullName, cataInfo.toString().c_str(),
                   rc ) ;
   done:
      PD_TRACE_EXITRC ( SDB_CATUPDATECATALOG, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 catGetCSGroupsFromCLs( const CHAR *csName, pmdEDUCB *cb,
                                vector< INT32 > &groups )
   {
      INT32 rc = SDB_OK ;
      BSONObj matcher ;
      BSONObj dummyObj ;
      SDB_DMSCB *dmsCB = pmdGetKRCB()->getDMSCB() ;
      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
      INT64 contextID = -1 ;
      std::set< INT32 > groupSet ;
      std::set< INT32 >::iterator itSet ;
      BSONObjBuilder builder ;
      std::stringstream ss ;

      ss << "^" << csName << "\\." ;
      builder.appendRegex( CAT_COLLECTION_NAME, ss.str() ) ;
      matcher = builder.obj() ;

      rc = rtnQuery( CAT_COLLECTION_INFO_COLLECTION, dummyObj, matcher,
                     dummyObj, dummyObj, 0, cb, 0, -1, dmsCB, rtnCB,
                     contextID ) ;
      PD_RC_CHECK( rc, PDERROR, "Query collection[%s] failed, matcher: %s, "
                   "rc: %d", CAT_COLLECTION_INFO_COLLECTION,
                   matcher.toString().c_str(), rc ) ;

      while ( TRUE )
      {
         BSONObj obj ;
         rtnContextBuf contextBuf ;
         rc = rtnGetMore( contextID, 1, contextBuf, cb, rtnCB ) ;
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_OK ;
            break ;
         }
         PD_RC_CHECK( rc, PDERROR, "Get more failed, rc: %d", rc ) ;

         try
         {
            obj = BSONObj( contextBuf.data() ) ;
            BSONElement eleCataInfo = obj.getField( CAT_CATALOGINFO_NAME ) ;
            if ( Array != eleCataInfo.type() )
            {
               continue ;
            }
            BSONObjIterator itr( eleCataInfo.embeddedObject() ) ;
            while( itr.more() )
            {
               BSONElement e = itr.next() ;
               if ( Object != e.type() )
               {
                  continue ;
               }
               BSONObj cataItemObj = e.embeddedObject() ;
               BSONElement eleGrpID = cataItemObj.getField( CAT_GROUPID_NAME ) ;
               if ( eleGrpID.isNumber() )
               {
                  groupSet.insert( eleGrpID.numberInt() ) ;
               }
            }
         }
         catch( std::exception &e )
         {
            PD_LOG( PDERROR, "Get group id from obj[%s] occur exception: %s",
                    obj.toString().c_str(), e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

      for ( UINT32 i = 0 ; i < groups.size() ; ++i )
      {
         groupSet.insert( groups[ i ] ) ;
      }
      groups.clear() ;

      itSet = groupSet.begin() ;
      while ( itSet != groupSet.end() )
      {
         groups.push_back( *itSet ) ;
         ++itSet ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATADDTASK, "catAddTask" )
   INT32 catAddTask( BSONObj & taskObj, pmdEDUCB * cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = krcb->getDPSCB() ;

      PD_TRACE_ENTRY ( SDB_CATADDTASK ) ;
      rc = rtnInsert( CAT_TASK_INFO_COLLECTION, taskObj, 1, 0, cb, dmsCB,
                      dpsCB, w ) ;

      if ( rc )
      {
         if ( SDB_IXM_DUP_KEY == rc )
         {
            rc = SDB_TASK_EXIST ;
         }
         else
         {
            PD_LOG( PDERROR, "Failed insert obj[%s] to collection[%s]",
                    taskObj.toString().c_str(), CAT_TASK_INFO_COLLECTION ) ;
         }
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATADDTASK, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETTASK, "catGetTask" )
   INT32 catGetTask( UINT64 taskID, BSONObj & obj, pmdEDUCB * cb )
   {
      INT32 rc           = SDB_OK;

      PD_TRACE_ENTRY ( SDB_CATGETTASK ) ;
      BSONObj dummyObj ;
      BSONObj boMatcher = BSON( CAT_TASKID_NAME << (INT64)taskID ) ;

      rc = catGetOneObj( CAT_TASK_INFO_COLLECTION, dummyObj, boMatcher,
                         dummyObj, cb, obj ) ;
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_CAT_TASK_NOTFOUND ;
         goto error ;
      }
      else if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 boMatcher.toString().c_str(), CAT_TASK_INFO_COLLECTION, rc ) ;
         goto error ;
      }
   done :
      PD_TRACE_EXITRC ( SDB_CATGETTASK, rc ) ;
      return rc;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETTASKSTATUS, "catGetTaskStatus" )
   INT32 catGetTaskStatus( UINT64 taskID, INT32 & status, pmdEDUCB * cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj taskObj ;
      PD_TRACE_ENTRY ( SDB_CATGETTASKSTATUS ) ;
      rc = catGetTask( taskID, taskObj, cb ) ;
      PD_RC_CHECK( rc, PDWARNING, "Get task[%lld] failed, rc: %d", taskID, rc ) ;

      rc = rtnGetIntElement( taskObj, CAT_STATUS_NAME, status ) ;
      PD_RC_CHECK( rc, PDWARNING, "Failed to get field[%s], rc: %d",
                   CAT_STATUS_NAME, rc ) ;
   done:
      PD_TRACE_EXITRC ( SDB_CATGETTASKSTATUS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATGETMAXTASKID, "catGetMaxTaskID" )
   INT64 catGetMaxTaskID( pmdEDUCB * cb )
   {
      INT64 taskID            = CLS_INVALID_TASKID ;
      INT32 rc                = SDB_OK ;
      SINT64 contextID        = -1 ;
      pmdKRCB *pKRCB          = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB        = pKRCB->getDMSCB() ;
      SDB_RTNCB *rtnCB        = pKRCB->getRTNCB() ;

      PD_TRACE_ENTRY ( SDB_CATGETMAXTASKID ) ;
      BSONObj dummyObj ;
      BSONObj orderby = BSON( CAT_TASKID_NAME << -1 ) ;

      rtnContextBuf buffObj ;

      rc = rtnQuery( CAT_TASK_INFO_COLLECTION, dummyObj, dummyObj, orderby,
                     dummyObj, 0, cb, 0, 1, dmsCB, rtnCB, contextID ) ;
      PD_RC_CHECK( rc, PDWARNING, "Failed to query from %s, rc: %d",
                   CAT_TASK_INFO_COLLECTION, rc ) ;

      rc = rtnGetMore( contextID, 1, buffObj, cb, rtnCB ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            contextID = -1 ;
         }
         goto error ;
      }

      try
      {
         BSONObj resultObj( buffObj.data() ) ;
         BSONElement ele = resultObj.getField( CAT_TASKID_NAME ) ;
         if ( !ele.isNumber() )
         {
            PD_LOG( PDWARNING, "Failed to get field[%s], type: %d",
                    CAT_TASKID_NAME, ele.type() ) ;
            goto error ;
         }
         taskID = (INT64)ele.numberLong() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         goto error ;
      }

   done:
      if ( -1 != contextID )
      {
         buffObj.release() ;
         rtnCB->contextDelete( contextID, cb ) ;
      }
      PD_TRACE_EXITRC ( SDB_CATGETMAXTASKID, rc ) ;
      return taskID ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATUPDATETASKSTATUS, "catUpdateTaskStatus" )
   INT32 catUpdateTaskStatus( UINT64 taskID, INT32 status, pmdEDUCB * cb,
                              INT16 w )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = krcb->getDPSCB() ;

      PD_TRACE_ENTRY ( SDB_CATUPDATETASKSTATUS ) ;
      BSONObj taskObj ;
      BSONObj dummy ;
      BSONObj match = BSON( CAT_TASKID_NAME << (INT64)taskID ) ;
      BSONObj updator = BSON( "$set" << BSON( CAT_STATUS_NAME << status ) ) ;

      rc = catGetTask( taskID, taskObj, cb ) ;
      PD_RC_CHECK( rc, PDERROR, "Get task[%lld] failed, rc: %d", taskID, rc ) ;

      rc = rtnUpdate( CAT_TASK_INFO_COLLECTION, match, updator, dummy, 0,
                      cb, dmsCB, dpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Update task[%lld] status to [%d] failed, "
                   "rc: %d", taskID, status, rc ) ;
   done:
      PD_TRACE_EXITRC ( SDB_CATUPDATETASKSTATUS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATREMOVETASK, "catRemoveTask" )
   INT32 catRemoveTask( UINT64 taskID, pmdEDUCB *cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = krcb->getDPSCB() ;

      PD_TRACE_ENTRY ( SDB_CATREMOVETASK ) ;
      BSONObj taskObj ;
      BSONObj match = BSON( CAT_TASKID_NAME << (INT64)taskID ) ;
      BSONObj dummy ;

      rc = catGetTask( taskID, taskObj, cb ) ;
      if ( rc )
      {
         goto error ;
      }

      rc = rtnDelete( CAT_TASK_INFO_COLLECTION, match, dummy, 0, cb, dmsCB,
                      dpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Delete task[%s] failed, rc: %d",
                   taskObj.toString().c_str(), rc ) ;

   done:
      PD_TRACE_EXITRC ( SDB_CATREMOVETASK, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATREMOVETASK1, "catRemoveTask" )
   INT32 catRemoveTask( BSONObj &match, pmdEDUCB *cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      pmdKRCB *krcb = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = krcb->getDPSCB() ;
      BSONObj taskObj ;
      BSONObj dummyObj ;

      PD_TRACE_ENTRY ( SDB_CATREMOVETASK1 ) ;

      rc = catGetOneObj( CAT_TASK_INFO_COLLECTION, dummyObj, match,
                         dummyObj, cb, taskObj ) ;
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_CAT_TASK_NOTFOUND ;
         goto error ;
      }
      else if ( rc )
      {
         PD_LOG( PDERROR, "Failed to get obj(%s) from %s, rc: %d",
                 match.toString().c_str(), CAT_TASK_INFO_COLLECTION, rc ) ;
         goto error ;
      }

      rc = rtnDelete( CAT_TASK_INFO_COLLECTION, match, dummyObj, 0, cb,
                      dmsCB, dpsCB, w ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to remove task from collection[%s], "
                   "rc: %d, del cond: %s", rc , match.toString().c_str() ) ;
   done:
      PD_TRACE_EXITRC ( SDB_CATREMOVETASK1, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 catGetCSGroupsFromTasks( const CHAR *csName, pmdEDUCB *cb,
                                  vector< INT32 > &groups )
   {
      INT32 rc = SDB_OK ;
      BSONObj matcher ;
      BSONObj dummyObj ;
      SDB_DMSCB *dmsCB = pmdGetKRCB()->getDMSCB() ;
      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
      INT64 contextID = -1 ;
      std::set< INT32 > groupSet ;
      std::set< INT32 >::iterator itSet ;
      BSONObjBuilder builder ;
      std::stringstream ss ;

      ss << "^" << csName << "\\." ;
      builder.appendRegex( CAT_COLLECTION_NAME, ss.str() ) ;
      matcher = builder.obj() ;

      rc = rtnQuery( CAT_TASK_INFO_COLLECTION, dummyObj, matcher, dummyObj,
                     dummyObj, 0, cb, 0, -1, dmsCB, rtnCB, contextID ) ;
      PD_RC_CHECK( rc, PDERROR, "Query collection[%s] failed, matcher: %s, "
                   "rc: %d", CAT_TASK_INFO_COLLECTION,
                   matcher.toString().c_str(), rc ) ;

      while ( TRUE )
      {
         BSONObj obj ;
         rtnContextBuf contextBuf ;
         rc = rtnGetMore( contextID, 1, contextBuf, cb, rtnCB ) ;
         if ( SDB_DMS_EOC == rc )
         {
            rc = SDB_OK ;
            break ;
         }
         PD_RC_CHECK( rc, PDERROR, "Get more failed, rc: %d", rc ) ;

         try
         {
            obj = BSONObj( contextBuf.data() ) ;
            BSONElement ele = obj.getField( CAT_TARGETID_NAME ) ;
            groupSet.insert( ele.numberInt() ) ;
         }
         catch( std::exception &e )
         {
            PD_LOG( PDERROR, "Get group id from obj[%s] occur exception: %s",
                    obj.toString().c_str(), e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

      for ( UINT32 i = 0 ; i < groups.size() ; ++i )
      {
         groupSet.insert( groups[ i ] ) ;
      }
      groups.clear() ;

      itSet = groupSet.begin() ;
      while ( itSet != groupSet.end() )
      {
         groups.push_back( *itSet ) ;
         ++itSet ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catGetBucketVersion( const CHAR *pCLName, pmdEDUCB *cb )
   {
      INT32 version = CAT_VERSION_BEGIN ;
      UINT32 bucketID = catCalcBucketID( pCLName, ossStrlen( pCLName ) ) ;
      BSONObj dummy ;
      BSONObj mather = BSON( FIELD_NAME_BUCKETID << bucketID ) ;
      BSONObj result ;

      INT32 rc = catGetOneObj( CAT_HISTORY_COLLECTION, dummy, mather,
                               dummy, cb, result ) ;
      if ( SDB_OK == rc )
      {
         version = (INT32)result.getField( FIELD_NAME_VERSION ).numberInt() ;
         ++version ;
      }
      return version ;
   }

   INT32 catSaveBucketVersion( const CHAR *pCLName, INT32 version,
                               pmdEDUCB *cb, INT16 w )
   {
      INT32 rc = SDB_OK ;
      const INT32 reverseVer = 0x00FFFFFF ;
      UINT32 bucketID = catCalcBucketID( pCLName, ossStrlen( pCLName ) ) ;
      BSONObj dummy ;
      BSONObj mather = BSON( FIELD_NAME_BUCKETID << bucketID ) ;
      BSONObj result ;

      pmdKRCB *krcb = pmdGetKRCB() ;
      SDB_DMSCB *dmsCB = krcb->getDMSCB() ;
      SDB_DPSCB *dpsCB = krcb->getDPSCB() ;

      rc = catGetOneObj( CAT_HISTORY_COLLECTION, dummy, mather,
                         dummy, cb, result ) ;
      if ( SDB_DMS_EOC == rc )
      {
#if defined ( _DEBUG )
         BSONObj obj = BSON( FIELD_NAME_BUCKETID << bucketID <<
                             FIELD_NAME_NAME << pCLName <<
                             FIELD_NAME_VERSION << version ) ;
#else
         BSONObj obj = BSON( FIELD_NAME_BUCKETID << bucketID <<
                             FIELD_NAME_VERSION << version ) ;
#endif // _DEBUG

         rc = rtnInsert( CAT_HISTORY_COLLECTION, obj, 1, 0, cb, dmsCB,
                         dpsCB, w ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to insert record[%s] to "
                      "collection[%s], rc: %d", obj.toString().c_str(),
                      CAT_HISTORY_COLLECTION, rc ) ;
      }
      else if ( SDB_OK == rc )
      {
         INT32 verTmp = (INT32)result.getField( FIELD_NAME_VERSION
                                               ).numberInt() ;
         if ( version < verTmp && verTmp - version < reverseVer )
         {
            goto done ;
         }
         else
         {
#if defined ( _DEBUG )
            BSONObj updator = BSON( "$set" << BSON( FIELD_NAME_VERSION <<
                                                    version <<
                                                    FIELD_NAME_NAME <<
                                                    pCLName ) ) ;
#else
            BSONObj updator = BSON( "$set" << BSON( FIELD_NAME_VERSION <<
                                                    version ) ) ;
#endif // _DEBUG
            rc = rtnUpdate( CAT_HISTORY_COLLECTION, mather, updator,
                            BSONObj(), 0, cb, dmsCB, dpsCB, w, NULL ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to update record[%s] to "
                         "collection[%s], rc: %d", updator.toString().c_str(),
                         CAT_HISTORY_COLLECTION, rc ) ;
         }
      }
      else
      {
         PD_LOG( PDERROR, "Failed to get record from collection[%s], rc: %d",
                 CAT_HISTORY_COLLECTION, rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATREMOVECLEX, "catRemoveCLEx" )
   INT32 catRemoveCLEx( const CHAR * clFullName, pmdEDUCB * cb,
                        SDB_DMSCB * dmsCB, SDB_DPSCB * dpsCB, INT16 w,
                        BOOLEAN delSubCL, INT32 version )
   {
      INT32 rc = SDB_OK ;
      CHAR szCLName[ DMS_COLLECTION_NAME_SZ + 1 ] = {0} ;
      CHAR szCSName[ DMS_COLLECTION_SPACE_NAME_SZ + 1 ] = {0} ;
      BOOLEAN isExist                  = FALSE ;
      BSONObj clObj;

      PD_TRACE_ENTRY ( SDB_CATREMOVECLEX ) ;
      clsCatalogSet cataInfo( clFullName );

      rc = catCheckCollectionExist( clFullName, isExist, clObj, cb );
      PD_RC_CHECK ( rc, PDERROR, "Failed to drop collection %s from catalog, "
                    "check collection failed(rc = %d)", clFullName, rc ) ;
      PD_CHECK( isExist, SDB_DMS_NOTEXIST, error, PDERROR,
               "collection %s is not exist" ,clFullName);

      rc = cataInfo.updateCatSet( clObj );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to parse catalog info(rc=%d)",
                  rc );
      PD_CHECK( -1 == version || cataInfo.getVersion() == version,
               SDB_CLS_COORD_NODE_CAT_VER_OLD, error, PDERROR,
               "failed to dropCL, coord version old(curVer:%d, coordVer:%d)",
               cataInfo.getVersion(), version );

      rc = catResolveCollectionName( clFullName, ossStrlen(clFullName),
                                     szCSName, DMS_COLLECTION_SPACE_NAME_SZ,
                                     szCLName, DMS_COLLECTION_NAME_SZ ) ;
      PD_RC_CHECK( rc, PDWARNING, "Resolve collection name[%s] failed, rc: %d",
                   clFullName, rc ) ;

      try
      {
         BSONObj matcher = BSON( CAT_COLLECTION_NAME << clFullName ) ;

         rc = catRemoveTask( matcher, cb, w ) ;
         if ( rc && SDB_CAT_TASK_NOTFOUND != rc )
         {
            goto error ;
         }
         rc = SDB_OK ;

         rc = catRemoveCL( clFullName, cb, dmsCB, dpsCB, w ) ;
         if ( rc )
         {
            goto error ;
         }

         catSaveBucketVersion( clFullName, cataInfo.getVersion(), cb, w ) ;

         rc = catDelCLFromCS( szCSName, szCLName, cb, dmsCB, dpsCB, w ) ;
         if ( rc )
         {
            goto error ;
         }

         if ( !cataInfo.getMainCLName().empty() )
         {
            BOOLEAN isMainExist = FALSE;
            BSONObj mainCLObj;
            clsCatalogSet mainCataInfo( cataInfo.getMainCLName().c_str() );
            rc = catCheckCollectionExist( cataInfo.getMainCLName().c_str(),
                                          isMainExist, mainCLObj, cb );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to get main-collection info(rc=%d)",
                         rc );
            if (!isMainExist )
            {
               goto done;
            }
            rc = mainCataInfo.updateCatSet( mainCLObj );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to parse catalog-info of main-collection(%s)",
                         cataInfo.getMainCLName().c_str() );
            if ( !mainCataInfo.isMainCL() )
            {
               PD_LOG( PDWARNING, "main-collection have been changed" );
               goto done;
            }

            rc = mainCataInfo.delSubCL( clFullName );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to delete the sub-collection(rc=%d)",
                         rc );
            {
            BSONObj newMainCLObj = mainCataInfo.toCataInfoBson();
            rc = catUpdateCatalog( cataInfo.getMainCLName().c_str(),
                                   newMainCLObj, cb, w );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to update the catalog of main-collection(%s)",
                         cataInfo.getMainCLName().c_str() );
            }
         }
         else if ( cataInfo.isMainCL() )
         {
            std::vector< std::string > subCLLst;
            std::vector< std::string >::iterator iterLst;
            rc = cataInfo.getSubCLList( subCLLst );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to get sub-collection list(rc=%d)" );
            iterLst = subCLLst.begin();
            while( iterLst != subCLLst.end() )
            {
               std::vector<UINT32>  groupList;
               rc = catUnlinkCL( clFullName, iterLst->c_str(), cb,
                                 dmsCB, dpsCB, w, groupList ) ;
               if ( SDB_DMS_NOTEXIST == rc )
               {
                  rc = SDB_OK;
                  ++iterLst;
                  continue;
               }
               PD_RC_CHECK( rc, PDERROR,
                            "Failed to unlink the sub-collection(%s) "
                            "from main-collection(%s)(rc=%d)",
                            clFullName, iterLst->c_str(), rc );
               if ( delSubCL )
               {
                  rc = catRemoveCLEx( iterLst->c_str(), cb, dmsCB, dpsCB, w ) ;
                  PD_CHECK( SDB_OK == rc || SDB_DMS_NOTEXIST == rc, rc, error,
                            PDERROR,
                            "Failed to remove the sub-collection(%s)(rc=%d)",
                            iterLst->c_str(), rc ) ;
                  rc = SDB_OK ;
               }
               ++iterLst ;
            }
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_CATREMOVECLEX, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATREMOVECSEX, "catRemoveCSEx" )
   INT32 catRemoveCSEx( const CHAR * csName, pmdEDUCB * cb, SDB_DMSCB * dmsCB,
                        SDB_DPSCB * dpsCB, INT16 w )
   {
      INT32 rc = SDB_OK ;
      BSONObj boSpace ;
      BOOLEAN exist = FALSE ;

      PD_TRACE_ENTRY ( SDB_CATREMOVECSEX ) ;
      rc = catCheckSpaceExist( csName, exist, boSpace, cb ) ;
      PD_RC_CHECK( rc, PDWARNING, "Failed to check space exist, rc: %d", rc ) ;
      PD_CHECK( exist, SDB_DMS_CS_NOTEXIST, error, PDWARNING,
                "Collection space[%s] does not exist", csName ) ;

      try
      {
         BSONObj matcher = BSON( CAT_COLLECTION_SPACE_NAME << csName ) ;
         BSONObj dummy ;

         BSONElement ele = boSpace.getField( CAT_COLLECTION ) ;
         if ( Array == ele.type() )
         {
            string clFullName ;
            BSONObj boTmp ;
            const CHAR *pCLName = NULL ;

            BSONObjIterator i ( ele.embeddedObject() ) ;
            while ( i.more() )
            {
               BSONElement beTmp = i.next() ;
               PD_CHECK( Object == beTmp.type(), SDB_CAT_CORRUPTION, error,
                         PDERROR, "Invalid collection record field type: %d",
                         beTmp.type() ) ;
               boTmp = beTmp.embeddedObject() ;
               rc = rtnGetStringElement( boTmp, CAT_COLLECTION_NAME, &pCLName ) ;
               PD_CHECK( SDB_OK == rc, SDB_CAT_CORRUPTION, error, PDERROR,
                         "Get field[%s] failed, rc: %d", CAT_COLLECTION_NAME,
                         rc ) ;

               clFullName = csName ;
               clFullName += "." ;
               clFullName += pCLName ;

               rc = catRemoveCLEx( clFullName.c_str(), cb, dmsCB, dpsCB, w ) ;
               PD_RC_CHECK( rc, PDWARNING, "Failed to remove collection[%s], "
                            "rc: %d", clFullName.c_str(), rc ) ;
            }
         }
         else if ( !ele.eoo() )
         {
            PD_LOG( PDERROR, "Invalid collection field[%s] type: %d",
                    CAT_COLLECTION, ele.type() ) ;
            rc = SDB_CAT_CORRUPTION ;
            goto error ;
         }

         rc = rtnDelete( CAT_COLLECTION_SPACE_COLLECTION, matcher, dummy,
                         0, cb, dmsCB, dpsCB, w ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to delete collection space[%s] item"
                      ", rc: %d", csName, rc ) ;
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CATREMOVECSEX, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATPRASEFUNC, "catPraseFunc" )
   INT32 catPraseFunc( const BSONObj &func, BSONObj &parsed )
   {
      INT32 rc = SDB_OK ;

      PD_TRACE_ENTRY ( SDB_CATPRASEFUNC ) ;
      BSONElement fValue = func.getField( FMP_FUNC_VALUE ) ;
      BSONElement fType = func.getField( FMP_FUNC_TYPE ) ;
      if ( fValue.eoo() || fType.eoo() )
      {
         PD_LOG( PDERROR, "failed to find specific element from func:%s",
                 func.toString().c_str() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else if ( Code != fValue.type() || NumberInt != fType.type())
      {
         PD_LOG( PDERROR, "invalid type of func element:%d, %d",
                 fValue.type(), fType.type() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
      {
         BSONObjBuilder builder ;
         const CHAR *nameBegin = NULL ;
         BOOLEAN appendBegun = FALSE ;
         std::string name ;
         const CHAR *fStr = ossStrstr(fValue.valuestr(),
                                      FMP_FUNCTION_DEF) ;
         if ( NULL == fStr )
         {
            PD_LOG( PDERROR, "can not find \"function\" in funcelement" ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         nameBegin = fStr + ossStrlen( FMP_FUNCTION_DEF ) ;
         while ( '\0' != *nameBegin )
         {
            if ( '(' == *nameBegin )
            {
               break ;
            }
            else if ( ' ' == *nameBegin && appendBegun )
            {
               break ;
            }
            else if ( ' ' != *nameBegin )
            {
               name.append( 1, *nameBegin ) ;
               appendBegun = TRUE ;
               ++nameBegin ;
            }
            else
            {
               ++nameBegin ;
            }
         }

         if ( name.empty() )
         {
            PD_LOG( PDERROR, "can not find func name" ) ;
            rc = SDB_INVALIDARG ;
         }

         builder.append( FMP_FUNC_NAME, name ) ;
         builder.append( fValue ) ;
         builder.append( fType ) ;
         parsed = builder.obj() ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CATPRASEFUNC, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATLINKCL, "catLinkCL" )
   INT32 catLinkCL( const CHAR *mainCLName, const CHAR *subCLName,
                     BSONObj &boLowBound, BSONObj &boUpBound,
                     pmdEDUCB *cb, SDB_DMSCB * dmsCB, SDB_DPSCB * dpsCB,
                     INT16 w, std::vector<UINT32>  &groupList )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN isSubExist = FALSE;
      BOOLEAN isMainExist = FALSE;
      BSONObj subCLObj;
      BSONObj mainCLObj;
      BOOLEAN hasUpdateSubCL = FALSE;
      PD_TRACE_ENTRY ( SDB_CATLINKCL ) ;
      clsCatalogSet cataInfo( mainCLName );

      try
      {
         rc = catCheckCollectionExist( subCLName, isSubExist, subCLObj, cb );
         PD_RC_CHECK(rc, PDERROR,
                     "failed to get sub-collection info(rc=%d)",
                     rc );
         PD_CHECK( isSubExist, SDB_DMS_NOTEXIST, error, PDERROR,
                  "sub-collection is not exist!" );
         {
         BSONElement beMainCLName = subCLObj.getField( CAT_MAINCL_NAME );
         if ( beMainCLName.type() == String )
         {
            std::string strMainCLName = beMainCLName.str();
            /*PD_CHECK( 0 == ossStrcmp( strMainCLName.c_str(), mainCLName ),
                     SDB_RELINK_SUB_CL, error, PDERROR,
                     "duplicate link sub-collection(%s), "
                     "the original main-collection is %s",
                     subCLName, strMainCLName.c_str() );
            hasUpdateSubCL = TRUE;*/
            PD_CHECK( strMainCLName.empty(),
                     SDB_RELINK_SUB_CL, error, PDERROR,
                     "duplicate attach collection partition(%s), "
                     "its partitioned-collection is %s",
                     subCLName, strMainCLName.c_str() );
         }
         }

         {
         BSONElement beIsMainCL = subCLObj.getField( CAT_IS_MAINCL );
         PD_CHECK( !beIsMainCL.booleanSafe(), SDB_INVALID_SUB_CL, error, PDERROR,
                  "sub-collection could not be a main-collection!" );
         }

         {
         BSONElement beCataInfo = subCLObj.getField( CAT_CATALOGINFO_NAME );
         BSONObj boCataInfo;
         PD_CHECK( beCataInfo.type() == Array, SDB_INVALIDARG, error, PDERROR,
                  "invalid sub-collecton, failed to get the field(%s)",
                  CAT_CATALOGINFO_NAME );
         boCataInfo = beCataInfo.embeddedObject();
         BSONObjIterator iterArr( boCataInfo );
         while ( iterArr.more() )
         {
            BSONElement beTmp = iterArr.next();
            PD_CHECK( beTmp.type() == Object, SDB_INVALIDARG, error, PDERROR,
                     "invalid catalog info(%s)", subCLName );
            BSONObj boTmp = beTmp.embeddedObject();
            BSONElement beGroupId = boTmp.getField( CAT_GROUPID_NAME );
            PD_CHECK( beGroupId.isNumber(), SDB_INVALIDARG, error, PDERROR,
                     "failed to get the field(%s)", CAT_GROUPID_NAME );
            groupList.push_back( beGroupId.numberInt() );
         }
         PD_CHECK( groupList.size() != 0, SDB_SYS, error, PDERROR,
                  "the collection(%s) has no group-info!", subCLName );
         }

         rc = catCheckCollectionExist( mainCLName, isMainExist, mainCLObj, cb );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to get partitioned-collection info(rc=%d)",
                     rc );
         PD_CHECK( isMainExist, SDB_DMS_NOTEXIST, error, PDERROR,
                   "partitioned-collection does not exist!" );
         rc = cataInfo.updateCatSet( mainCLObj );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to parse catalog-info of main-collection(%s)",
                     mainCLName );
         PD_CHECK( cataInfo.isMainCL(), SDB_INVALID_MAIN_CL, error, PDERROR,
                  "source collection must be partitioned-collection!" );
         SDB_ASSERT( cataInfo.isRangeSharding(),
                     "main-collection must be range-sharding!" ) ;

         rc = cataInfo.addSubCL( subCLName, boLowBound, boUpBound );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to add sub-collection(rc=%d)",
                     rc );

         if ( !hasUpdateSubCL )
         {
            BSONObjBuilder subClBuilder;
            subClBuilder.appendElements( subCLObj );
            subClBuilder.append( CAT_MAINCL_NAME, mainCLName );
            BSONObj newSubCLObj = subClBuilder.done();
            rc = catUpdateCatalog( subCLName, newSubCLObj, cb, w );
            PD_RC_CHECK( rc, PDERROR,
                        "failed to update the catalog of sub-collection(%s)",
                        subCLName );
            hasUpdateSubCL = TRUE;
         }

         {
         BSONObj newMainCLObj = cataInfo.toCataInfoBson();
         rc = catUpdateCatalog( mainCLName, newMainCLObj, cb, w );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to update the catalog of main-collection(%s)",
                     mainCLName );
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CATLINKCL, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_CATUNLINKCL, "catUnlinkCL" )
   INT32 catUnlinkCL( const CHAR *mainCLName, const CHAR *subCLName,
                     pmdEDUCB *cb, SDB_DMSCB * dmsCB, SDB_DPSCB * dpsCB,
                     INT16 w, std::vector<UINT32>  &groupList )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN isSubExist = FALSE;
      BOOLEAN isMainExist = FALSE;
      BSONObj subCLObj;
      BSONObj mainCLObj;
      BOOLEAN needUpdateSubCL = FALSE;

      PD_TRACE_ENTRY ( SDB_CATUNLINKCL ) ;
      clsCatalogSet cataInfo( mainCLName );
      try
      {
         rc = catCheckCollectionExist( subCLName, isSubExist, subCLObj, cb );
         PD_RC_CHECK( rc, PDERROR,
                      "Failed to get sub-collection info(rc=%d)",
                      rc );
         PD_CHECK( isSubExist, SDB_DMS_NOTEXIST, error, PDERROR,
                   "Sub-collection is not exist!" );
         {
         BSONElement beMainCLName = subCLObj.getField( CAT_MAINCL_NAME );
         if ( beMainCLName.type() == String )
         {
            std::string strMainCLName = beMainCLName.str();
            PD_CHECK( 0 == ossStrcmp( strMainCLName.c_str(), mainCLName ),
                      SDB_INVALIDARG, error, PDERROR,
                      "Failed to unlink sub-collection(%s), "
                      "the original main-collection is %s not %s",
                      subCLName, strMainCLName.c_str(), mainCLName );
            needUpdateSubCL = TRUE;
         }
         }

         {
         BSONElement beCataInfo = subCLObj.getField( CAT_CATALOGINFO_NAME );
         BSONObj boCataInfo;
         PD_CHECK( beCataInfo.type() == Array, SDB_INVALIDARG, error, PDERROR,
                   "Invalid sub-collecton, failed to get the field(%s)",
                   CAT_CATALOGINFO_NAME );
         boCataInfo = beCataInfo.embeddedObject();
         BSONObjIterator iterArr( boCataInfo );
         while ( iterArr.more() )
         {
            BSONElement beTmp = iterArr.next();
            PD_CHECK( beTmp.type() == Object, SDB_INVALIDARG, error, PDERROR,
                      "Invalid catalog info(%s)", subCLName );
            BSONObj boTmp = beTmp.embeddedObject();
            BSONElement beGroupId = boTmp.getField( CAT_GROUPID_NAME );
            PD_CHECK( beGroupId.isNumber(), SDB_INVALIDARG, error, PDERROR,
                      "Failed to get the field(%s)", CAT_GROUPID_NAME );
            groupList.push_back( beGroupId.numberInt() );
         }
         PD_CHECK( groupList.size() != 0, SDB_SYS, error, PDERROR,
                   "The collection(%s) has no group-info!", subCLName );
         }

         rc = catCheckCollectionExist( mainCLName, isMainExist, mainCLObj, cb );
         PD_RC_CHECK( rc, PDERROR,
                     "failed to get main-collection info(rc=%d)",
                     rc );
         if ( isMainExist )
         {
            rc = cataInfo.updateCatSet( mainCLObj );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to parse catalog-info of main-collection(%s)",
                         mainCLName );
            PD_CHECK( cataInfo.isMainCL(), SDB_INVALID_MAIN_CL, error, PDERROR,
                      "Source collection must be main-collection!" );

            rc = cataInfo.delSubCL( subCLName );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to delete the sub-collection(rc=%d)",
                         rc );
         }

         if ( needUpdateSubCL )
         {
            BSONObj emptyObj ;
            BSONObj match = BSON( CAT_CATALOGNAME_NAME << subCLName );
            BSONObj updator = BSON( "$inc" << BSON( CAT_VERSION_NAME << 1 ) <<
                                    "$unset" << BSON( CAT_MAINCL_NAME << "" ) );
            rc = rtnUpdate( CAT_COLLECTION_INFO_COLLECTION, match, updator,
                            emptyObj, 0, cb, dmsCB, dpsCB, w );
            PD_RC_CHECK( rc, PDERROR,
                         "Failed to update the catalog of sub-collection(%s)",
                         subCLName ) ;
            needUpdateSubCL = FALSE ;
         }

         if ( isMainExist )
         {
            BSONObj newMainCLObj = cataInfo.toCataInfoBson();
            rc = catUpdateCatalog( mainCLName, newMainCLObj, cb, w );
            PD_RC_CHECK( rc, PDERROR,
                        "failed to update the catalog of main-collection(%s)",
                        mainCLName );
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_CATUNLINKCL, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 catTestAndCreateCL( const CHAR *pCLFullName, pmdEDUCB *cb,
                             _SDB_DMSCB *dmsCB, _dpsLogWrapper *dpsCB,
                             BOOLEAN sys )
   {
      INT32 rc = SDB_OK ;

      rc = rtnTestCollectionCommand( pCLFullName, dmsCB ) ;
      if ( SDB_DMS_CS_NOTEXIST == rc || SDB_DMS_NOTEXIST == rc )
      {
         rc = rtnCreateCollectionCommand( pCLFullName, 0, cb, dmsCB, dpsCB,
                                          FLG_CREATE_WHEN_NOT_EXIST, sys ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to create collection[%s], rc: %d",
                      pCLFullName, rc ) ;
      }
      else if ( rc )
      {
         PD_LOG( PDERROR, "Test collection[%s] failed, rc: %d", pCLFullName,
                 rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 catTestAndCreateIndex( const CHAR *pCLFullName,
                                const BSONObj &indexDef,
                                pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                                _dpsLogWrapper *dpsCB, BOOLEAN sys )
   {
      INT32 rc          = SDB_OK ;
      BOOLEAN isSame    = FALSE ;

      try
      {
         rc = rtnTestIndex( pCLFullName, "", dmsCB, &indexDef, &isSame ) ;
         if ( SDB_IXM_NOTEXIST == rc || ( SDB_OK == rc && isSame == FALSE ) )
         {
            if ( SDB_OK == rc && isSame == FALSE )
            {
               BSONElement ele = indexDef.getField( IXM_NAME_FIELD ) ;
               rc = rtnDropIndexCommand( pCLFullName, ele, cb, dmsCB,
                                         dpsCB, sys ) ;
               PD_RC_CHECK( rc, PDERROR, "Failed to drop index[%s] for "
                            "collection[%s], rc: %d", ele.valuestr(),
                            pCLFullName, rc ) ;
            }
            rc = rtnCreateIndexCommand( pCLFullName, indexDef, cb, dmsCB,
                                        dpsCB, sys ) ;
            PD_RC_CHECK( rc, PDERROR, "Failed to create index[%s] for "
                         "collection[%s], rc: %d", indexDef.toString().c_str(),
                         pCLFullName, rc ) ;
         }
         else if ( rc )
         {
            PD_LOG( PDERROR, "Test index[%s] for collection[%s] failed, "
                    "rc: %d", indexDef.toString().c_str(), pCLFullName, rc ) ;
            goto error ;
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   UINT32 catCalcBucketID( const CHAR *pData, UINT32 length,
                           UINT32 bucketSize )
   {
      md5::md5digest digest ;
      md5::md5( pData, length, digest ) ;
      UINT32 hashValue = 0 ;
      UINT32 i = 0 ;
      while ( i++ < 4 )
      {
         hashValue |= ( (UINT32)digest[i-1] << ( 32 - 8 * i ) ) ;
      }
      return ( hashValue % bucketSize ) ;
   }

}


