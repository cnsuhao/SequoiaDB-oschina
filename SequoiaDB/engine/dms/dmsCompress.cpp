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

   Source File Name = dmsCompress.cpp

   Descriptive Name =

   When/how to use: str util

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          20/06/2014  XJH Initial Draft

   Last Changed =

******************************************************************************/

#include "dmsCompress.hpp"
#include "pmdEDU.hpp"
#include "dmsRecord.hpp"
#include <../snappy/snappy.h>

using namespace bson ;

namespace engine
{

   INT32 dmsCompress ( _pmdEDUCB *cb, const CHAR *pInputData,
                       INT32 inputSize, const CHAR **ppData,
                       INT32 *pDataSize )
   {
      INT32 rc = SDB_OK ;
      CHAR *pBuff = NULL ;

      SDB_ASSERT ( pInputData && ppData && pDataSize,
                   "Data pointer and size pointer can't be NULL" ) ;

      size_t maxCompressedLen = snappy::MaxCompressedLength ( inputSize ) ;

      pBuff = cb->getCompressBuff( maxCompressedLen ) ;
      if ( !pBuff )
      {
         PD_LOG( PDERROR, "Failed to alloc compress buff, size: %d",
                 maxCompressedLen ) ;
         rc = SDB_OK ;
         goto error ;
      }

      snappy::RawCompress ( pInputData, (size_t)inputSize,
                            pBuff, &maxCompressedLen ) ;

      if ( ppData )
      {
         *ppData = pBuff ;
      }
      if ( pDataSize )
      {
         *pDataSize = (INT32)maxCompressedLen ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

   INT32 dmsCompress ( _pmdEDUCB *cb, const BSONObj &obj,
                       const CHAR* pOIDPtr, INT32 oidLen,
                       const CHAR **ppData, INT32 *pDataSize )
   {
      INT32 rc = SDB_OK ;
      CHAR *pTmpBuff = NULL ;

      if ( oidLen && pOIDPtr )
      {
         INT32 tmpBuffLen = 0 ;
         const CHAR *pObjData = NULL ;

         INT32 requestedSize = obj.objsize() + oidLen + DMS_RECORD_METADATA_SZ ;
         rc = cb->allocBuff( requestedSize, &pTmpBuff, tmpBuffLen ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to alloc tmp buffer, size: %d",
                    requestedSize ) ;
            goto error ;
         }
         pObjData = pTmpBuff + DMS_RECORD_METADATA_SZ ;

         DMS_RECORD_SETDATA_OID ( pTmpBuff, obj.objdata(), obj.objsize(),
                                  BSONElement(pOIDPtr) ) ;

         rc = dmsCompress ( cb, pObjData , BSONObj(pObjData).objsize(),
                            ppData, pDataSize ) ;
      }
      else
      {
         rc = dmsCompress( cb, obj.objdata(), obj.objsize(),
                           ppData, pDataSize ) ;
      }

   done :
      if ( pTmpBuff )
      {
         cb->releaseBuff( pTmpBuff ) ;
      }
      return rc ;
   error :
      goto done ;
   }

   INT32 dmsUncompress ( _pmdEDUCB *cb, const CHAR *pInputData,
                         INT32 inputSize, const CHAR **ppData,
                         INT32 *pDataSize )
   {
      INT32 rc = SDB_OK ;
      bool  result = true ;
      CHAR *pBuff = NULL ;

      SDB_ASSERT ( pInputData && ppData && pDataSize,
                   "Data pointer and size pointer can't be NULL" ) ;

      size_t maxUncompressedLen = 0 ;
      result = snappy::GetUncompressedLength ( pInputData, (size_t)inputSize,
                                               &maxUncompressedLen ) ;
      if ( !result )
      {
         PD_LOG( PDERROR, "Failed to get uncompressed length" ) ;
         rc = SDB_CORRUPTED_RECORD ;
         goto error ;
      }

      pBuff = cb->getUncompressBuff( maxUncompressedLen ) ;
      if ( !pBuff )
      {
         PD_LOG( PDERROR, "Failed to allocate uncompression buff, size: %d",
                 maxUncompressedLen ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      result = snappy::RawUncompress ( pInputData, (size_t)inputSize,
                                       pBuff ) ;
      if ( !result )
      {
         PD_LOG( PDERROR, "Failed to uncompress record" ) ;
         rc = SDB_CORRUPTED_RECORD ;
         goto error ;
      }

      if ( ppData )
      {
         *ppData = pBuff ;
      }
      if ( pDataSize )
      {
         *pDataSize = maxUncompressedLen ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

}


