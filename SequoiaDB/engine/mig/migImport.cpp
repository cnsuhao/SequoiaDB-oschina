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

   Source File Name = migImport.cpp

   Descriptive Name = Migration Import Implementation

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains implementation for import
   operation

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          05/06/2013  HTL  Initial Draft

   Last Changed =

*******************************************************************************/

#include "migImport.hpp"
#include "ossMem.hpp"
#include "ossUtil.hpp"
#include "ossIO.hpp"
#include "pdTrace.hpp"
#include "migTrace.hpp"
#include "../util/json2rawbson.h"
#include "../util/text.h"
#include "../client/jstobs.h"
#include "msgDef.h"
#include "msg.h"

migImport::migImport() : _pMigArg(NULL),
                         _pParser(NULL),
                         _ppBsonArray(NULL),
                         _gConnection(0),
                         _gCollectionSpace(0),
                         _gCollection(0)
{
}

migImport::~migImport()
{
   SAFE_OSS_DELETE ( _pParser ) ;
   if ( _gConnection )
   {
      sdbDisconnect ( _gConnection ) ;
   }
   if ( _gCollection )
   {
      sdbReleaseCollection ( _gCollection ) ;
   }
   if ( _gCollectionSpace )
   {
      sdbReleaseCS ( _gCollectionSpace ) ;
   }
   if ( _gConnection )
   {
      sdbDisconnect( _gConnection ) ;
      sdbReleaseConnection ( _gConnection ) ;
   }
   if ( _ppBsonArray )
   {
      for ( INT32 i = 0; i < _pMigArg->insertNum; ++i )
      {
         if ( _ppBsonArray[i] )
         {
            delete _ppBsonArray[i] ;
         }
      }
      delete _ppBsonArray ;
   }
}

INT32 migImport::_connectDB()
{
   INT32 rc = SDB_OK ;
   rc = sdbConnect ( _pMigArg->pHostname, _pMigArg->pSvcname,
                     _pMigArg->pUser, _pMigArg->pPassword,
                     &_gConnection ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to connect to database %s:%s, rc = %d",
               _pMigArg->pHostname, _pMigArg->pSvcname, rc ) ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migImport::_getCS( CHAR *pCSName )
{
   INT32 rc = SDB_OK ;
   rc = sdbGetCollectionSpace ( _gConnection, pCSName,
                                &_gCollectionSpace ) ;
   if ( SDB_DMS_CS_NOTEXIST == rc )
   {
      ossPrintf( "Collection space %s does not exist"OSS_NEWLINE, pCSName ) ;
      PD_LOG ( PDERROR, "Collection space %s does not exist",
               pCSName ) ;
      goto error ;
   }
   else if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to get collection space %s, rc = %d",
               pCSName, rc ) ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migImport::_getCL( CHAR *pCLName )
{
   INT32 rc = SDB_OK ;
   rc = sdbGetCollection1 ( _gCollectionSpace, pCLName,
                            &_gCollection ) ;
   if ( SDB_DMS_NOTEXIST == rc )
   {
      ossPrintf( "Collection %s does not exist"OSS_NEWLINE, pCLName ) ;
      PD_LOG ( PDERROR, "Collection %s does not exist"OSS_NEWLINE,
               pCLName ) ;
      goto error ;
   }
   else if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to get collection %s, rc = %d",
               pCLName, rc ) ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

PD_TRACE_DECLARE_FUNCTION ( SDB__MIGIMPORT__IMPRCD2, "migImport::_importRecord" )
INT32 migImport::_importRecord ( bson **bsonObj )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB__MIGIMPORT__IMPRCD2 ) ;
   SDB_ASSERT ( bsonObj, "bsonObj can't be NULL" ) ;

   rc = sdbInsert ( _gCollection, *bsonObj ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to insert record, rc = %d", rc ) ;
      goto error ;
   }
done :
   PD_TRACE_EXITRC ( SDB__MIGIMPORT__IMPRCD2, rc );
   return rc ;
error :
   goto done ;
}

PD_TRACE_DECLARE_FUNCTION ( SDB__MIGIMPORT__IMPRCD, "migImport::_importRecord" )
INT32 migImport::_importRecord ( bson **bsonObj, UINT32 bsonNum )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB__MIGIMPORT__IMPRCD ) ;
   SDB_ASSERT ( bsonObj, "bsonObj can't be NULL" ) ;
   rc = sdbBulkInsert ( _gCollection,
                        FLG_INSERT_CONTONDUP,
                        bsonObj,
                        bsonNum ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to insert record, rc = %d", rc ) ;
      goto error ;
   }
done :
   PD_TRACE_EXITRC ( SDB__MIGIMPORT__IMPRCD, rc );
   return rc ;
error :
   goto done ;
}

PD_TRACE_DECLARE_FUNCTION ( SDB__MIGIMPORT__GETRCD, "_migCSVParser::_getRecord" )
INT32 migImport::_getRecord ( bson &record )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB__MIGIMPORT__GETRCD ) ;
   UINT32  startOffset = 0 ;
   UINT32  size        = 0 ;
   bson obj ;
   bson *pObj = &record ;
   CHAR *pBuffer = _pParser->getBuffer() ;
   CHAR *pBuffer2 = NULL ;

   rc = _pParser->getNextRecord ( startOffset, size ) ;
   if ( rc )
   {
      if ( rc == SDB_EOF )
      {
         if ( 0 == size )
         {
            goto done ;
         }
      }
      else
      {
         PD_LOG ( PDERROR, "Failed to _parser getNextRecord, rc=%d", rc ) ;
         goto error ;
      }
   }
   if ( !isValidUTF8WSize ( pBuffer + startOffset, size ) &&
        !_pMigArg->force )
   {
      rc = SDB_MIG_DATA_NON_UTF ;
      PD_LOG ( PDERROR, "It is not utf-8 file, rc=%d", rc ) ;
      goto error ;
   }
   if ( _pMigArg->type == MIGIMPRT_CSV )
   {
      rc = _csvParser.csv2bson( pBuffer + startOffset, size, pObj ) ;
      if ( rc )
      {
         rc = SDB_UTIL_PARSE_CSV_INVALID ;
         PD_LOG ( PDERROR, "Failed to convert Bson, rc=%d", rc ) ;
         goto error ;
      }
   }
   else
   {
      pBuffer2 = json2rawcbson ( pBuffer + startOffset ) ;
      if ( !pBuffer2 )
      {
         rc = SDB_UTIL_PARSE_JSON_INVALID ;
         PD_LOG ( PDERROR, "Failed to convert Bson, rc=%d", rc ) ;
         goto error ;
      }
      obj.ownmem = 0 ;
      obj.data = NULL ;
      bson_init_finished_data ( &obj, pBuffer2 ) ;
      bson_copy ( pObj, &obj ) ;
      free ( pBuffer2 ) ;
   }
done :
   PD_TRACE_EXITRC ( SDB__MIGIMPORT__GETRCD, rc );
   return rc ;
error :
   goto done ;
}

PD_TRACE_DECLARE_FUNCTION ( SDB__MIGIMPORT__INIT, "migImport::init" )
INT32 migImport::init ( migImprtArg *pMigArg )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB__MIGIMPORT__INIT );
   SDB_ASSERT ( pMigArg, "pMigArg can't be NULL" ) ;
   SDB_ASSERT ( pMigArg->pFile, "file name can't be NULL" ) ;
   CHAR *pBuffer = NULL ;
   UINT32  startOffset = 0 ;
   UINT32  fieldsSize  = 0 ;
   _utilParserParamet parserPara ;

   _pMigArg = pMigArg ;

   rc = _connectDB() ;
   if ( rc )
   {
      goto error ;
   }
   rc = _getCS( _pMigArg->pCSName ) ;
   if ( rc )
   {
      goto error ;
   }
   rc = _getCL( _pMigArg->pCLName ) ;
   if ( rc )
   {
      goto error ;
   }

   if ( _pMigArg->type == MIGIMPRT_CSV )
   {
      _pParser = SDB_OSS_NEW _utilCSVParser() ;
   }
   else if ( _pMigArg->type == MIGIMPRT_JSON )
   {
      _pParser = SDB_OSS_NEW _utilJSONParser() ;
   }
   if ( !_pParser )
   {
      rc = SDB_OOM ;
      PD_LOG ( PDERROR, "memory error" ) ;
      goto error ;
   }
   _pParser->setDel ( _pMigArg->delChar,
                      _pMigArg->delField,
                      _pMigArg->delRecord ) ;

   parserPara.accessModel  = UTIL_GET_IO ;
   parserPara.fileName     = _pMigArg->pFile ;
   parserPara.bufferSize   = 33554432 ;
   parserPara.blockNum     = 32768 ;
   parserPara.linePriority = _pMigArg->linePriority ;
   rc = _pParser->initialize( &parserPara ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to _parser initialize, rc=%d", rc ) ;
      goto error ;
   }

   if ( _pMigArg->type == MIGIMPRT_CSV )
   {
      rc = _csvParser.init( _pMigArg->autoAddField,
                            _pMigArg->autoCompletion,
                            _pMigArg->pFields ? FALSE : _pMigArg->isHeaderline,
                            _pMigArg->delChar,
                            _pMigArg->delField,
                            _pMigArg->delRecord ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to csv parser initialize, rc=%d", rc ) ;
         goto error ;
      }
   
      if ( _pMigArg->isHeaderline )
      {
         rc = _pParser->getNextRecord ( startOffset, fieldsSize ) ;
         if ( rc )
         {
            if ( rc == SDB_EOF )
            {
               if ( 0 == fieldsSize )
               {
                  goto done ;
               }
            }
            else
            {
               PD_LOG ( PDERROR, "Failed to _parser getNextRecord, rc=%d", rc ) ;
               goto error ;
            }
         }
      }
      
      if ( _pMigArg->pFields )
      {
         fieldsSize = ossStrlen( _pMigArg->pFields ) ;
         rc = _csvParser.parseHeader( _pMigArg->pFields, fieldsSize ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to parse csv header, rc=%d", rc ) ;
            goto error ;
         }
      }
      else
      {
         pBuffer = _pParser->getBuffer() ;
         rc = _csvParser.parseHeader( pBuffer + startOffset, fieldsSize ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to parse csv header, rc=%d", rc ) ;
            goto error ;
         }
      }
   }

   if ( _pMigArg->insertNum > 1 )
   {
      _ppBsonArray = new(std::nothrow) bson* [ _pMigArg->insertNum ] ;
      if ( !_ppBsonArray )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "Failed to malloc memory" ) ;
         goto error ;
      }

      for ( INT32 i = 0; i < _pMigArg->insertNum; ++i )
      {
         _ppBsonArray[i] = NULL ;
      }
   
      for ( INT32 i = 0; i < _pMigArg->insertNum; ++i )
      {
         _ppBsonArray[i] = new(std::nothrow) bson() ;
         if ( !_ppBsonArray[i] )
         {
            rc = SDB_OOM ;
            PD_LOG ( PDERROR, "Failed to malloc memory" ) ;
            goto error ;
         }
      }
   }

done :
   PD_TRACE_EXITRC ( SDB__MIGIMPORT__INIT, rc );
   return rc ;
error :
   goto done ;
}

PD_TRACE_DECLARE_FUNCTION ( SDB__MIGIMPORT___RUN, "migImport::_run" )
INT32 migImport::_run ( INT32 &total, INT32 &succeed )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB__MIGIMPORT___RUN );
   INT32 count = 0 ;
   INT32 succ  = 0 ;
   INT32 bsonObjNum = 0 ;
   INT32 maxInsert  = _pMigArg->insertNum ;
   INT32 sumSize = 0 ;
   INT32 bsonSize = 0 ;
   bson *tempObj = NULL ;

   while ( TRUE )
   {
      tempObj = _ppBsonArray[ bsonObjNum ] ;
      bson_init ( tempObj ) ;
      rc = _getRecord ( (*tempObj) ) ;
      if ( rc )
      {
         bson_destroy ( tempObj ) ;
         if ( rc == SDB_EOF )
         {
            if ( bsonObjNum > 0 )
            {
               rc = _importRecord ( _ppBsonArray, bsonObjNum ) ;
               for ( INT32 i = 0; i < bsonObjNum; ++i )
               {
                  tempObj = _ppBsonArray[ i ] ;
                  bson_destroy ( tempObj ) ;
               }
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to import record in %d", count ) ;
               }
               else
               {
                  succ += bsonObjNum ;
               }
               bsonObjNum = 0 ;
               if ( rc && _pMigArg->errorStop )
               {
                  goto error ;
               }
            }
            PD_LOG ( PDEVENT, "Import Successfully" ) ;
            rc = SDB_OK ;
            break ;
         }
         else if( SDB_MIG_DATA_NON_UTF == rc ||
                  SDB_UTIL_PARSE_CSV_INVALID == rc ||
                  SDB_UTIL_PARSE_JSON_INVALID == rc )
         {
            ++count ;
            if( SDB_MIG_DATA_NON_UTF == rc )
            {
               PD_LOG ( PDERROR, "Data encoding is not utf8, in the %d line",
                        count ) ;
            }
            else
            {
               PD_LOG ( PDERROR, "Data format error, in the %d line", count ) ;
            }
            if ( _pMigArg->errorStop )
            {
               goto error ;
            }
            else
            {
               continue ;
            }
         }
         else
         {
            ++count ;
            PD_LOG ( PDERROR, "Failed to get record, rc=%d", rc ) ;
            goto error ;
         }
      }
      bsonSize = tempObj->dataSize ;
      if ( bsonSize <= 5 )
      {
         bson_destroy ( tempObj ) ;
         ++count ;
         continue ;
      }
      if ( (UINT32)( sumSize + bsonSize ) >=
            ( SDB_MAX_MSG_LENGTH - sizeof( MsgOpInsert ) ) )
      {
         sumSize = bsonSize ;
         rc = _importRecord ( _ppBsonArray, bsonObjNum ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to import %d record", bsonObjNum ) ;
         }
         else
         {
            succ += bsonObjNum ;
         }
         for ( INT32 i = 0; i < bsonObjNum; ++i )
         {
            tempObj = _ppBsonArray[ i ] ;
            bson_destroy ( tempObj ) ;
         }
         if ( rc && _pMigArg->errorStop )
         {
            goto error ;
         }
         tempObj = _ppBsonArray[ 0 ] ;
         _ppBsonArray[ 0 ] = _ppBsonArray[ bsonObjNum ] ;
         _ppBsonArray[ bsonObjNum ] = tempObj ;
         bsonObjNum = 0 ;
      }
      else
      {
         sumSize += bsonSize ;
      }
      ++count ;
      ++bsonObjNum ;
      if ( maxInsert == bsonObjNum )
      {
         sumSize = 0 ;
         rc = _importRecord ( _ppBsonArray, bsonObjNum ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to import %d record", bsonObjNum ) ;
         }
         else
         {
            succ += bsonObjNum ;
         }
         for ( INT32 i = 0; i < bsonObjNum; ++i )
         {
            tempObj = _ppBsonArray[ i ] ;
            bson_destroy ( tempObj ) ;
         }
         if ( rc && _pMigArg->errorStop )
         {
            goto error ;
         }
         bsonObjNum = 0 ;
      }
   }

done :
   total = count ;
   succeed = succ ;
   PD_TRACE_EXITRC ( SDB__MIGIMPORT___RUN, rc );
   return rc ;
error :
   goto done ;
}

PD_TRACE_DECLARE_FUNCTION ( SDB__MIGIMPORT__RUN, "migImport::run" )
INT32 migImport::run ( INT32 &total, INT32 &succeed )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB__MIGIMPORT__RUN );
   INT32 count = 0 ;
   INT32 succ = 0 ;
   bson bsonObj ;
   bson *obj = &bsonObj ; ;

   if ( _pMigArg->insertNum > 1 )
   {
      rc = _run ( count, succ ) ;
      goto done ;
   }

   while ( TRUE )
   {
      bson_init ( obj ) ;
      rc = _getRecord ( (*obj) ) ;
      if ( rc )
      {
         bson_destroy ( obj ) ;
         if ( rc == SDB_EOF )
         {
            rc = SDB_OK ;
            break ;
         }
         else if( SDB_MIG_DATA_NON_UTF == rc ||
                  SDB_UTIL_PARSE_CSV_INVALID == rc ||
                  SDB_UTIL_PARSE_JSON_INVALID == rc )
         {
            ++count ;
            if( SDB_MIG_DATA_NON_UTF == rc )
            {
               PD_LOG ( PDERROR, "Data encoding is not utf8, in the %d line",
                        count ) ;
            }
            else
            {
               PD_LOG ( PDERROR, "Data format error, in the %d line", count ) ;
            }
            if ( _pMigArg->errorStop )
            {
               goto error ;
            }
            else
            {
               continue ;
            }
         }
         else
         {
            ++count ;
            PD_LOG ( PDERROR, "Failed to get record, rc=%d", rc ) ;
            goto error ;
         }
      }
      if ( bsonObj.dataSize <= 5 )
      {
         bson_destroy ( obj ) ;
         continue ;
      }
      ++count ;
      rc = _importRecord ( &obj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to import record in the %d row", count ) ;
      }
      else
      {
         ++succ ;
      }
      bson_destroy ( obj ) ;
      if ( rc && _pMigArg->errorStop )
      {
         goto error ;
      }
   }
done :
   total = count ;
   succeed = succ ;
   PD_TRACE_EXITRC ( SDB__MIGIMPORT__RUN, rc );
   return rc ;
error :
   goto done ;
}
