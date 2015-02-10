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

   Source File Name = mongodef.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/27/2015  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#include "mongodef.hpp"

mongoParser::mongoParser()
   : withCmd( FALSE ),
     withIndex( FALSE ),
     nsLen( 0 ),
     opType( OP_INVALID ),
     _dataStart( NULL ),
     _dataEnd( NULL ),
     _nextObj( NULL ),
     _offset( 0 )
{
   ossMemset( csName, 0, CS_NAME_SIZE + 1 ) ;
   ossMemset( fullName, 0, CL_FULL_NAME_SIZE + 1 ) ;
}

mongoParser::~mongoParser( )
{
   reset() ;
}

void mongoParser::reset()
{
   ossMemset( csName, 0, CS_NAME_SIZE + 1 ) ;
   ossMemset( fullName, 0, CL_FULL_NAME_SIZE + 1 ) ;
   withCmd    = FALSE ;
   withIndex  = FALSE ;
   nsLen      = 0 ;
   opType     = OP_INVALID ;
   _dataStart = NULL ;
   _dataEnd   = NULL ;
   _nextObj   = NULL ;
   _offset    = 0 ;
}

void mongoParser::nextObj( bson::BSONObj &obj )
{
   obj = bson::BSONObj( _nextObj ) ;
   _nextObj += obj.objsize() ;
   _offset += obj.objsize() ;
   if ( _nextObj >= _dataEnd )
   {
      _nextObj = NULL ;
   }
}

void mongoParser::readNumber( const UINT32 size, CHAR *out )
{
   INT32 limit = size - 1 ;
   const CHAR *start = _dataStart + _offset ;
   if ( _bigEndian )
   {
      for( UINT32 i = 0; i < size; ++i )
      {
         *(out + i) = *(start +  limit - i) ;
      }
   }
   else
   {
      ossMemcpy( out, start, size ) ;
   }

   _offset += size ;
}

void mongoParser::extractMsg()
{
   const CHAR *ptr    = NULL ;
   const CHAR *dbName = NULL ;

   readNumber( sizeof( INT32 ), ( CHAR * )&len ) ;

   readNumber( sizeof( INT32 ), ( CHAR * )&id ) ;

   responseTo = 0 ;
   skip( 4 ) ;

   readNumber( sizeof( SINT16 ), ( CHAR * )&opCode ) ;

   _flags = 0 ;
   _version = 0 ;
   skip( 2 ) ;

   readNumber( sizeof( INT32 ), ( CHAR * )&reservedFlags ) ;

   dbName = _dataStart + _offset ;
   while( *( dbName + nsLen ) )
   {
      ++nsLen ;
   }

   ptr = ossStrstr( dbName, ".$cmd" ) ;
   if ( NULL != ptr )
   {
      withCmd = TRUE ;
      ossMemcpy( csName, dbName, ptr - dbName ) ;
   }
   else
   {
      ptr = ossStrstr(dbName, ".system.indexes" ) ;
      if ( NULL != ptr )
      {
         withIndex = TRUE ;
         ossMemcpy( csName, dbName, ptr - dbName ) ;
      }
      else
      {
         ossMemcpy( fullName, dbName, ossStrlen( dbName ) ) ;
         ptr = ossStrstr( dbName, "." ) ;
         if ( NULL != ptr )
         {
            ossMemcpy( csName, dbName, ptr - dbName ) ;
         }
      }
   }
}

void mongoParser::setEndian( BOOLEAN bigEndian )
{
   _bigEndian = bigEndian ;
}

void mongoParser::init( const CHAR *in, const INT32 inLen )
{
   reset() ;

   len = inLen ;
   _dataStart = in ;
   _dataEnd = _dataStart + inLen ;

   extractMsg() ;
}
