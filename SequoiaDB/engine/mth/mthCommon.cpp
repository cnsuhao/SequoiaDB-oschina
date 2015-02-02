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

   Source File Name = mthCommon.cpp

   Descriptive Name = Method Common

   When/how to use: this program may be used on binary and text-formatted
   versions of Method component. This file contains common functions for mth

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/12/2013  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include "mthCommon.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "mthTrace.hpp"

namespace engine
{
   INT32 mthAppendString ( CHAR **ppStr, INT32 &bufLen,
                           INT32 strLen, const CHAR *newStr,
                           INT32 newStrLen, INT32 *pMergedLen )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( ppStr && newStr, "str or newStr can't be NULL" ) ;
      if ( !*ppStr )
      {
         strLen = 0 ;
      }
      else if ( strLen <= 0 )
      {
         strLen = ossStrlen ( *ppStr ) ;
      }
      if ( newStrLen <= 0 )
      {
         newStrLen = ossStrlen ( newStr ) ;
      }
      if ( strLen + newStrLen >= bufLen )
      {
         CHAR *pOldStr = *ppStr ;
         INT32 newSize = ossRoundUpToMultipleX ( strLen + newStrLen,
                                                 SDB_PAGE_SIZE ) ;
         if ( newSize < 0 )
         {
            PD_LOG ( PDERROR, "new buffer overflow, size: %d", newSize ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         *ppStr = (CHAR*)SDB_OSS_REALLOC ( *ppStr, sizeof(CHAR)*(newSize) ) ;
         if ( !*ppStr )
         {
            PD_LOG ( PDERROR, "Failed to allocate %d bytes buffer", newSize ) ;
            rc = SDB_OOM ;
            *ppStr = pOldStr ;
            goto error ;
         }
         bufLen = newSize ;
      }
      if ( *ppStr && newStr )
      {
         ossMemcpy ( &(*ppStr)[strLen], newStr, newStrLen ) ;
         (*ppStr)[strLen+newStrLen] = '\0' ;

         if ( pMergedLen )
         {
            *pMergedLen = strLen + newStrLen ;
         }
      }
      else
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done :
      return rc ;
   error :
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__MTHDOUBLEBUFFERSIZE, "mthDoubleBufferSize" )
   INT32 mthDoubleBufferSize ( CHAR **ppStr, INT32 &bufLen )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__MTHDOUBLEBUFFERSIZE ) ;
      SDB_ASSERT ( ppStr, "ppStr can't be NULL" ) ;
      CHAR *pOldStr = *ppStr ;
      INT32 newSize = ossRoundUpToMultipleX ( 2*bufLen,
                                              SDB_PAGE_SIZE ) ;
      if ( newSize < 0 )
      {
         PD_LOG ( PDERROR, "new buffer overflow" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      if ( 0 == newSize )
      {
         newSize = SDB_PAGE_SIZE ;
      }
      *ppStr = (CHAR*)SDB_OSS_REALLOC ( *ppStr, sizeof(CHAR)*(newSize) ) ;
      if ( !*ppStr )
      {
         PD_LOG ( PDERROR, "Failed to allocate %d bytes buffer", newSize ) ;
         rc = SDB_OOM ;
         *ppStr = pOldStr ;
         goto error ;
      }
      bufLen = newSize ;

   done :
      PD_TRACE_EXITRC ( SDB__MTHDOUBLEBUFFERSIZE, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   INT32 mthCheckFieldName( const CHAR *pField, INT32 &dollarNum )
   {
      INT32 rc = SDB_OK ;
      const CHAR *pTmp = pField ;
      const CHAR *pDot = NULL ;
      INT32 number = 0 ;
      dollarNum = 0 ;

      while ( pTmp && *pTmp )
      {
         pDot = ossStrchr( pTmp, '.' ) ;

         if ( '$' == *pTmp )
         {
            if ( pDot )
            {
               *(CHAR*)pDot = 0 ;
            }
            rc = ossStrToInt( pTmp + 1, &number ) ;
            if ( pDot )
            {
               *(CHAR*)pDot = '.' ;
            }
            if ( rc )
            {
               goto error ;
            }
            ++dollarNum ;
         }
         pTmp = pDot ? pDot + 1 : NULL ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   BOOLEAN mthCheckUnknowDollar( const CHAR *pField,
                                 std::vector<INT64> *dollarList )
   {
      INT32 rc = SDB_OK ;
      const CHAR *pTmp = pField ;
      const CHAR *pDot = NULL ;
      INT32 number = 0 ;
      BOOLEAN hasUnknowDollar = FALSE ;

      while ( pTmp && *pTmp )
      {
         pDot = ossStrchr( pTmp, '.' ) ;

         if ( '$' == *pTmp )
         {
            if ( pDot )
            {
               *(CHAR*)pDot = 0 ;
            }
            rc = ossStrToInt( pTmp + 1, &number ) ;
            if ( pDot )
            {
               *(CHAR*)pDot = '.' ;
            }
            if ( rc )
            {
               goto error ;
            }

            if ( dollarList )
            {
               std::vector<INT64>::iterator it = dollarList->begin() ;
               for ( ; it != dollarList->end() ; ++it )
               {
                  if ( number == (((*it)>>32)&0xFFFFFFFF) )
                  {
                     break ;
                  }
               }
               if ( it == dollarList->end() )
               {
                  goto error ;
               }
            }
         }
         pTmp = pDot ? pDot + 1 : NULL ;
      }

   done:
      return hasUnknowDollar ? FALSE : TRUE ;
   error:
      hasUnknowDollar = TRUE ;
      goto done ;
   }

   INT32 mthConvertSubElemToNumeric( const CHAR *desc,
                                     INT32 &n )
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != desc && '$' == *desc, "must be a $" ) ;
      const CHAR *p = desc ;
      const UINT32 maxLen = 10 ;
      CHAR number[maxLen + 1] = { 0 } ;
      UINT32 numberLen = 0 ;
      if ( '[' == *( p + 1 ) )
      {
         p += 2 ;
         while ( '\0' != *p )
         {
            if ( '0' <= *p && *p <= '9' )
            {
               if ( numberLen == maxLen )
               {
                  PD_LOG( PDERROR, "number is too long" ) ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
               number[numberLen++] = *p++ ;
            }
            else if ( ']' == *p )
            {
               break ;
            }
            else
            {
               PD_LOG( PDERROR, "argument should be a numeric" ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
         }

         if ( 0 == numberLen )
         {
            PD_LOG( PDERROR, "invalid action in selector:%s", desc ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         number[numberLen] = '\0' ;
         n = ossAtoi( number ) ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid action:%s", desc ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
     return rc ;
   error:
     goto done ;
   }
}

