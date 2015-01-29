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

   Source File Name = aggrGroup.hpp

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
   : nsLen( 0 ),
     dbNameLen( 0 ),
     dbName( NULL ),
     _dataStart( NULL ),
     _dataEnd( NULL ),
     _nextObj( NULL ),
     _offset( 0 )
{
}

mongoParser::~mongoParser( )
{
   dbName   = NULL ;
   _dataEnd = NULL ;
   _nextObj = NULL ;
   _offset  = 0 ;
}

void mongoParser::nextObj( bson::BSONObj &obj )
{
   if ( NULL != _nextObj )
   {
      obj = bson::BSONObj( _nextObj ) ;
      _nextObj += obj.objsize() ;
      _offset += obj.objsize() ;

      if ( _nextObj >= _dataEnd )
      {
         _nextObj = NULL ;
      }
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
   const CHAR *ptr = NULL ;

   readNumber( sizeof( INT32 ), ( CHAR * )&len ) ;

   readNumber( sizeof( INT32 ), ( CHAR * )&id ) ;

   responseTo = 0 ;

   readNumber( sizeof( SINT16 ), ( CHAR * )&opCode ) ;

   _flags = 0 ;
   _version = 0 ;

   readNumber( sizeof( INT32 ), ( CHAR * )&reservedFlags ) ;

   dbName = _dataStart + _offset ;
   while( *( dbName + nsLen ) )
   {
      ++nsLen ;
   }
   _offset += nsLen + 1 ;

   ptr = ossStrstr( dbName, ".$cmd" ) ;
   if ( NULL == ptr )
   {
      ptr = ossStrstr(dbName, ".system.indexes" ) ;
   }

   if ( NULL != ptr )
   {
      dbNameLen = ( UINT32 )( ptr - dbName ) ;
   }
   else
   {
      dbNameLen = nsLen ;
   }
}

void mongoParser::setEndian( BOOLEAN bigEndian )
{
   _bigEndian = bigEndian ;
}

void mongoParser::init( const CHAR *in, const INT32 inLen )
{
   len = inLen ;
   _dataStart = in ;
   _dataEnd = _dataStart + inLen ;

   extractMsg() ;
}
