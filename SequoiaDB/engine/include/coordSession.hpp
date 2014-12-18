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

   Source File Name = coordSession.hpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/28/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef COORDSESSION_HPP__
#define COORDSESSION_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "pmdEDU.hpp"
#include "netMultiRouteAgent.hpp"

namespace engine
{
   typedef struct _subSessionInfo
   {
      MsgRouteID  routeID ;
      BOOLEAN     isConnected ;

      _subSessionInfo()
      {
         routeID.value  = MSG_INVALID_ROUTEID ;
         isConnected    = FALSE ;
      }
   } subSessionInfo ;

   typedef std::map<UINT64, subSessionInfo>     COORD_SUBSESSION_MAP ;
   typedef std::map<UINT32, MsgRouteID>         COORD_LASTNODE_MAP ;


   class CoordSession : public SDBObject
   {
   public:
      CoordSession( pmdEDUCB *pEduCB );
      ~CoordSession(){}

   public:
      INT32     addSubSession( const MsgRouteID &routeID );
      void     addSubSessionWithoutCheck( const MsgRouteID &routeID );
      BOOLEAN  delSubSession( const MsgRouteID &routeID );
      INT32    disConnect( const MsgRouteID &routeID );
      void     addLastNode( const MsgRouteID &routeID );
      MsgRouteID getLastNode( UINT32 groupID );
      void     removeLastNode( UINT32 groupID ) ;
      void     getAllSessionRoute( ROUTE_SET &routeMap );
      void     postEvent ( pmdEDUEvent const &data );
      BOOLEAN  isSubsessionConnected( const MsgRouteID &routeID );
      void     addRequest( const UINT64 reqID, const MsgRouteID &routeID );
      void     delRequest( const UINT64 reqID );
      void     clearRequest();
      BOOLEAN  isValidResponse( const UINT64 reqID );
      BOOLEAN  isValidResponse( const MsgRouteID &routeID, const UINT64 reqID );
      void     setPreferReplType( INT32 type );
      INT32    getPreferReplType();

   private:
      CoordSession(){}
      CoordSession( CoordSession &coordSession ){}
      INT32 checkRemoteRouteID( const MsgRouteID &routeID );

   private:
      pmdEDUCB                   *_pEduCB;
      COORD_SUBSESSION_MAP       _subSessionMap;
      COORD_LASTNODE_MAP         _lastNodeMap;
      ossSpinXLatch              _mutex ;
      REQUESTID_MAP              _requestMap;
      INT32                      _preferReplType;
   } ;
}

#endif

