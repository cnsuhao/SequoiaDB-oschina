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

   Source File Name = migExport.cpp

   Descriptive Name = Migration Export Header

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains implementation for export
   operation

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft
          05/22/2014  JWH
   Last Changed =

*******************************************************************************/

#include "migExport.hpp"
#include "ossMem.hpp"
#include "ossUtil.hpp"
#include "ossIO.hpp"
#include "pdTrace.hpp"
#include "migTrace.hpp"
#include "msgDef.h"
#include "msg.h"
#include "jstobs.h"

#define MIG_STR_POINT "."
#define MIG_STR_SPACE 32
#define MIG_STR_TABLE '\t'

migExport::migExport () : _gConnection(0),
                          _gCollectionSpace(0),
                          _gCollection(0),
                          _gCSList(0),
                          _gCLList(0),
                          _gCursor(0),
                          _bufferSize(0),
                          _isOpen(FALSE),
                          _pMigArg(NULL),
                          _pBuffer(NULL)
{
}

migExport::~migExport ()
{
   if ( _isOpen )
   {
      ossClose ( _file ) ;
   }
   if ( _gCursor )
   {
      sdbCloseCursor( _gCursor ) ;
      sdbReleaseCursor( _gCursor ) ;
   }
   if ( _gCSList )
   {
      sdbCloseCursor( _gCSList ) ;
      sdbReleaseCursor( _gCSList ) ;
   }
   if ( _gCLList )
   {
      sdbCloseCursor( _gCLList ) ;
      sdbReleaseCursor( _gCLList ) ;
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
      sdbDisconnect ( _gConnection ) ;
      sdbReleaseConnection( _gConnection ) ;
   }
   SAFE_OSS_FREE( _pBuffer ) ;
}

INT32 migExport::_connectDB()
{
   INT32 rc = SDB_OK ;
   rc = sdbConnect ( _pMigArg->pHostname, _pMigArg->pSvcname,
                     _pMigArg->pUser, _pMigArg->pPassword,
                     &_gConnection ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to connect database %s:%s, rc = %d",
               _pMigArg->pHostname, _pMigArg->pSvcname, rc ) ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::_getCSList()
{
   INT32 rc = SDB_OK ;
   rc = sdbListCollectionSpaces( _gConnection, &_gCSList ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to get collection space list, rc = %d", rc ) ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::_getCLList()
{
   INT32 rc = SDB_OK ;
   rc = sdbListCollections( _gConnection, &_gCLList ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to get collection list, rc = %d", rc ) ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::_getCS( const CHAR *pCSName )
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

INT32 migExport::_getCL( const CHAR *pCLName )
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

INT32 migExport::_writeData( CHAR *pBuffer, INT32 size )
{
   INT32 rc = SDB_OK ;
   SINT64 sumSize = (SINT64)size ;
   SINT64 writedSize = 0 ;
   SINT64 curWriteSize = 0 ;

   while ( sumSize > 0 )
   {
      rc = ossWrite ( &_file, pBuffer + writedSize,
                      sumSize, &curWriteSize ) ;
      if ( rc && SDB_INTERRUPT != rc )
      {
         PD_LOG ( PDERROR, "Failed to write to file, rc = %d", rc ) ;
         goto error ;
      }
      sumSize -= curWriteSize ;
      writedSize += curWriteSize ;
      rc = SDB_OK ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::_writeSubField( fieldResolve *pFieldRe, BOOLEAN isFirst )
{
   INT32 rc = SDB_OK ;
   INT32 fieldSize = 0 ;
   if ( !isFirst )
   {
      rc = _writeData( MIG_STR_POINT, 1 ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to write data, rc=%d", rc ) ;
         goto error ;
      }
   }
   fieldSize = ossStrlen( pFieldRe->pField ) ;
   rc = _writeData( pFieldRe->pField, fieldSize ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to write data, rc=%d", rc ) ;
      goto error ;
   }
   if ( pFieldRe->pSubField )
   {
      rc = _writeSubField( pFieldRe->pSubField, FALSE ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to call _writeSubField, rc=%d", rc ) ;
         goto error ;
      }
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::_writeInclude()
{
   INT32 rc = SDB_OK ;
   INT32 fieldsNum = 0 ;
   fieldResolve *pFieldRe = NULL ;

   if ( _pMigArg->type == MIGEXPRT_CSV &&
        _pMigArg->include == TRUE )
   {
      fieldsNum = _decodeBson._vFields.size() ;
      for ( INT32 i = 0; i < fieldsNum; ++i )
      {
         pFieldRe = _decodeBson._vFields.at( i ) ;
         rc = _writeSubField( pFieldRe, TRUE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to call _writeSubField, rc=%d", rc ) ;
            goto error ;
         }
         if ( i + 1 == fieldsNum )
         {
            rc = _writeData( &_pMigArg->delRecord, 1 ) ;
         }
         else
         {
            rc = _writeData( &_pMigArg->delField, 1 ) ;
         }
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to write data, rc=%d", rc ) ;
            goto error ;
         }
      }
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::_query()
{
   INT32 rc = SDB_OK ;
   bson condition ;
   bson sort ;
   bson_init ( &condition ) ;
   bson_init ( &sort ) ;
   if( _pMigArg->pFiter )
   {
      if ( !jsonToBson2 ( &condition, _pMigArg->pFiter, 0, 1 ) )
      {
         rc = SDB_INVALIDARG ;
         ossPrintf ( "fiter format error"OSS_NEWLINE ) ;
         PD_LOG ( PDERROR, "fiter format error" ) ;
         goto error ;
      }
   }
   else
   {
      bson_empty( &condition ) ;
   }
   if( _pMigArg->pSort )
   {
      if ( !jsonToBson2 ( &sort, _pMigArg->pSort, 0, 1 ) )
      {
         rc = SDB_INVALIDARG ;
         ossPrintf ( "sort format error"OSS_NEWLINE ) ;
         PD_LOG ( PDERROR, "sort format error" ) ;
         goto error ;
      }
   }
   else
   {
      bson_empty( &sort ) ;
   }
   rc = sdbQuery ( _gCollection, &condition, NULL, &sort, NULL, 0, -1,
                   &_gCursor ) ;
   if ( rc )
   {
      if ( SDB_DMS_EOC != rc )
      {
         PD_LOG ( PDERROR, "Failed to query collection, rc = %d", rc ) ;
         goto error ;
      }
      else
      {
         goto done ;
      }
   }
done:
   bson_destroy ( &sort ) ;
   bson_destroy ( &condition ) ;
   return rc ;
error:
   goto done ;
}

INT32 migExport::_reallocBuffer( CHAR **ppBuffer, INT32 size, INT32 newSize )
{
   INT32 rc = SDB_OK ;
   CHAR *pTemp = NULL ;
   if ( newSize > size )
   {
      pTemp = (CHAR *)SDB_OSS_REALLOC( *ppBuffer, newSize ) ;
      if ( !pTemp )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "out of memory, rc=%d", rc ) ;
         goto error ;
      }
      *ppBuffer = pTemp ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::_writeRecord( bson *pbson )
{
   INT32  rc = SDB_OK ;
   INT32  bufferSize = 0 ;
   INT32  tempSize = 0 ;
   CHAR *pTemp = NULL ;

   if ( _pMigArg->type == MIGEXPRT_CSV )
   {
      rc = _decodeBson.parseCSVSize( pbson->data, &bufferSize ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get csv size, rc=%d", rc ) ;
         goto error ;
      }
      if ( _bufferSize < bufferSize )
      {
         rc = _reallocBuffer( &_pBuffer, _bufferSize, bufferSize ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to realloc memory, rc=%d", rc ) ;
            goto error ;
         }
         _bufferSize = bufferSize ;
      }
      pTemp = _pBuffer ;
      tempSize = _bufferSize ;
      rc = _decodeBson.bsonCovertCSV( pbson->data, &pTemp, &tempSize ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to convert bson to csv, rc=%d", rc ) ;
         goto error ;
      }
      bufferSize = _bufferSize - tempSize ;
   }
   else if ( _pMigArg->type == MIGEXPRT_JSON )
   {
      rc = _decodeBson.parseJSONSize( pbson->data, &bufferSize ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get csv size, rc=%d", rc ) ;
         goto error ;
      }
      if ( _bufferSize < bufferSize )
      {
         rc = _reallocBuffer( &_pBuffer, _bufferSize, bufferSize ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to realloc memory, rc=%d", rc ) ;
            goto error ;
         }
         _bufferSize = bufferSize ;
      }
      ossMemset( _pBuffer, 0, _bufferSize ) ;
      pTemp = _pBuffer ;
      tempSize = _bufferSize ;
      rc = _decodeBson.bsonCovertJson( pbson->data, &pTemp, &tempSize ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to convert bson to csv, rc=%d", rc ) ;
         goto error ;
      }
      bufferSize = ossStrlen( _pBuffer ) ;
   }

   if ( bufferSize < 0 )
   {
      rc = SDB_SYS ;
      PD_LOG ( PDERROR, "csv size can not be less than 0, rc=%d", rc ) ;
      goto error ;
   }

   rc = _writeData( _pBuffer, bufferSize ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to write data, rc=%d", rc ) ;
      goto error ;
   }
   rc = _writeData( &_pMigArg->delRecord, 1 ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to write data, rc=%d", rc ) ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::_exportCL( const CHAR *pCSName,
                            const CHAR *pCLName,
                            INT32 &total )
{
   INT32 rc = SDB_OK ;
   INT32 clTotal = 0 ;
   bson obj ;

   bson_init( &obj ) ;
   _gCollection = 0 ;
   _gCollectionSpace = 0 ;

   rc = _getCS( pCSName ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to get collection space, rc = %d",
               rc ) ;
      goto error ;
   }
   rc = _getCL( pCLName ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to get collection, rc = %d",
               rc ) ;
      goto error ;
   }
   rc = _query() ;
   if ( rc )
   {
      if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_OK ;
         goto done ;
      }
      else
      {
         PD_LOG ( PDERROR, "Failed to get record, rc = %d",
                  rc ) ;
         goto error ;
      }
   }
   while ( TRUE )
   {
      rc = sdbNext( _gCursor, &obj ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG ( PDERROR, "Failed to get collection list, rc = %d",
                     rc ) ;
            goto error ;
         }
         else
         {
            rc = SDB_OK ;
            goto done ;
         }
      }
      rc = _writeRecord( &obj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to write record to file, rc = %d",
                  rc ) ;
         goto error ;
      }
      bson_destroy ( &obj ) ;
      ++clTotal ;
   }
done:
   total += clTotal ;
   bson_destroy ( &obj ) ;
   if ( _gCollection )
   {
      sdbReleaseCollection ( _gCollection ) ;
   }
   if ( _gCollectionSpace )
   {
      sdbReleaseCS ( _gCollectionSpace ) ;
   }
   _gCollection = 0 ;
   _gCollectionSpace = 0 ;
   PD_LOG ( PDEVENT, "%s.%s export record %d in file",pCSName, pCLName, clTotal ) ;
   return rc ;
error:
   goto done ;
}

INT32 migExport::_run( const CHAR *pCSName, const CHAR *pCLName, INT32 &total )
{
   INT32 rc = SDB_OK ;
   const CHAR *pTemp = NULL ;
   bson obj ;
   bson_iterator it ;
   bson_type type ;

   bson_init( &obj ) ;
   if ( ( pCSName == NULL && pCLName == NULL ) ||
        ( pCSName == NULL && pCLName != NULL ) )
   {
      rc = _getCSList() ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get collection space list, rc = %d",
                  rc ) ;
         goto error ;
      }
      while( TRUE )
      {
         rc = sdbNext( _gCSList, &obj ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC != rc )
            {
               PD_LOG ( PDERROR, "Failed to get collection space list, rc = %d",
                        rc ) ;
               goto error ;
            }
            else
            {
               rc = SDB_OK ;
               goto done ;
            }
         }
         type = bson_find( &it, &obj, "Name" ) ;
         if ( type != BSON_STRING )
         {
            rc = SDB_SYS ;
            PD_LOG ( PDERROR, "List collection space does not string, rc = %d",
                     rc ) ;
            goto error ;
         }
         pTemp = bson_iterator_string( &it ) ;
         rc = _run( pTemp, pCLName, total ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Faild to call _run, rc = %d", rc ) ;
            goto error ;
         }
      }
   }
   else if ( pCSName != NULL && pCLName == NULL )
   {
      rc = _getCLList() ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to get collection list, rc = %d",
                  rc ) ;
         goto error ;
      }
      while ( TRUE )
      {
         rc = sdbNext( _gCLList, &obj ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC != rc )
            {
               PD_LOG ( PDERROR, "Failed to get collection list, rc = %d",
                        rc ) ;
               goto error ;
            }
            else
            {
               rc = SDB_OK ;
               goto done ;
            }
         }
         type = bson_find( &it, &obj, "Name" ) ;
         if ( type != BSON_STRING )
         {
            rc = SDB_SYS ;
            PD_LOG ( PDERROR, "List collection does not string, rc = %d",
                     rc ) ;
            goto error ;
         }
         pTemp = bson_iterator_string( &it ) ;
         rc = _run( pCSName, pTemp, total ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Faild to call _run, rc = %d", rc ) ;
            goto error ;
         }
      }
   }
   else
   {
      rc = _exportCL( pCSName, pCLName, total ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Faild to call _export, rc = %d", rc ) ;
         goto error ;
      }
   }
done:
   bson_destroy ( &obj ) ;
   return rc ;
error:
   goto done ;
}

INT32 migExport::init( migExprtArg *pMigArg )
{
   INT32 rc = SDB_OK ;

   _pMigArg = pMigArg ;

   rc = _connectDB() ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to connect database, rc=%d", rc ) ;
      goto error ;
   }

   rc = ossOpen ( _pMigArg->pFile,
                  OSS_REPLACE | OSS_WRITEONLY | OSS_EXCLUSIVE,
                  OSS_RU | OSS_WU | OSS_RG,
                  _file ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to open file %s, rc = %d",
               _pMigArg->pFile, rc ) ;
      goto error ;
   }
   _isOpen = TRUE ;

   if ( _pMigArg->delChar == _pMigArg->delField )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delchar does not like delfield" ) ;
      goto error ;
   }
   else if ( MIG_STR_SPACE == _pMigArg->delChar )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delchar can not be a tab" ) ;
      goto error ;
   }
   else if ( MIG_STR_TABLE == _pMigArg->delChar )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delchar can not be a space" ) ;
      goto error ;
   }

   if ( _pMigArg->delField == _pMigArg->delRecord )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delfield does not like delrecord" ) ;
      goto error ;
   }
   else if ( MIG_STR_TABLE == _pMigArg->delField )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delfield can not be a tab" ) ;
      goto error ;
   }
   else if ( MIG_STR_SPACE == _pMigArg->delField )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delfield can not be a space" ) ;
      goto error ;
   }

   if ( _pMigArg->delRecord == _pMigArg->delChar )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delrecord does not like delchar" ) ;
      goto error ;
   }
   else if ( MIG_STR_TABLE == _pMigArg->delRecord )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delrecord can not be a tab" ) ;
      goto error ;
   }
   else if ( MIG_STR_SPACE == _pMigArg->delRecord )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "delrecord can not be a space" ) ;
      goto error ;
   }

   rc = _decodeBson.init( _pMigArg->delChar, _pMigArg->delField,
                          _pMigArg->includeBinary, _pMigArg->includeRegex ) ;
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to call init, rc=%d", rc ) ;
      goto error ;
   }

   if ( _pMigArg->pFields )
   {
      rc = _decodeBson.parseFields( _pMigArg->pFields,
                                    ossStrlen( _pMigArg->pFields ) ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to parse fields, rc=%d", rc ) ;
         goto error ;
      }
      rc = _writeInclude() ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to call _writeInclude, rc=%d", rc ) ;
         goto error ;
      }
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 migExport::run( INT32 &total )
{
   INT32 rc = SDB_OK ;
   rc = _run( _pMigArg->pCSName, _pMigArg->pCLName, total ) ;
   if ( rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}
