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

   Source File Name = rtnLob.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_LOB_HPP_
#define RTN_LOB_HPP_

#include "rtn.hpp"
#include "dmsLobDef.hpp"

namespace engine
{
   INT32 rtnOpenLob( const BSONObj &lob,
                     SINT32 flags,
                     BOOLEAN isLocal,
                     _pmdEDUCB *cb,
                     SDB_DPSCB *dpsCB,
                     SINT16 w,
                     SINT64 &contextID,
                     bson::BSONObj &meta ) ;

   INT32 rtnWriteLob( SINT64 contextID,
                      pmdEDUCB *cb,
                      UINT32 len,
                      const CHAR *buf ) ;

   INT32 rtnReadLob( SINT64 contextID,
                     pmdEDUCB *cb,
                     UINT32 len,
                     SINT64 offset,
                     const CHAR **buf,
                     UINT32 &read ) ;

   INT32 rtnCloseLob( SINT64 contextID,
                      pmdEDUCB *cb ) ;

   INT32 rtnRemoveLob( const BSONObj &lob,
                       SINT16 w,
                       _pmdEDUCB *cb,
                       SDB_DPSCB *dpsCB ) ;

   INT32 rtnGetLobMetaData( SINT64 contextID,
                            pmdEDUCB *cb,
                            BSONObj &meta ) ;

   INT32 rtnCreateLob( const CHAR *fullName,
                       const bson::OID &oid,
                       pmdEDUCB *cb,
                       SINT16 w,
                       SDB_DPSCB *dpsCB ) ;

   INT32 rtnGetLobMetaData( const CHAR *fullName,
                            const bson::OID &oid,
                            pmdEDUCB *cb,
                            dmsLobMeta &meta ) ;

   INT32 rtnWriteLob( const CHAR *fullName,
                      const bson::OID &oid,
                      UINT32 sequence,
                      UINT32 offset,
                      UINT32 len,
                      const CHAR *data,
                      pmdEDUCB *cb,
                      SINT16 w,
                      SDB_DPSCB *dpsCB ) ;

   INT32 rtnUpdateLob( const CHAR *fullName,
                       const bson::OID &oid,
                       UINT32 sequence,
                       UINT32 offset,
                       UINT32 len,
                       const CHAR *data,
                       pmdEDUCB *cb,
                       SINT16 w,
                       SDB_DPSCB *dpsCB ) ;

   INT32 rtnReadLob( const CHAR *fullName,
                     const bson::OID &oid,
                     UINT32 sequence,
                     UINT32 offset,
                     UINT32 len,
                     pmdEDUCB *cb,
                     CHAR *data,
                     UINT32 &read ) ;
                      

   INT32 rtnCloseLob( const CHAR *fullName,
                      const bson::OID &oid,
                      const dmsLobMeta &meta,
                      pmdEDUCB *cb,
                      SINT16 w,
                      SDB_DPSCB *dpsCB ) ;

   INT32 rtnRemoveLobPiece( const CHAR *fullName,
                            const bson::OID &oid,
                            UINT32 sequence,
                            pmdEDUCB *cb,
                            SINT16 w,
                            SDB_DPSCB *dpsCB ) ;

   INT32 rtnQueryAndInvalidateLob( const CHAR *fullName,
                                   const bson::OID &oid,
                                   pmdEDUCB *cb,
                                   SINT16 w,
                                   SDB_DPSCB *dpsCB,
                                   dmsLobMeta &meta ) ;


}

#endif

