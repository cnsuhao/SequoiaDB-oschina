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

   Source File Name = clsCommand.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          05/27/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef CLS_COMMAND_HPP_
#define CLS_COMMAND_HPP_

#include "rtnCommand.hpp"

using namespace bson ;

namespace engine
{

   class _rtnSplit : public _rtnCommand
   {
      DECLARE_CMD_AUTO_REGISTER()

      public:
         _rtnSplit () ;
         ~_rtnSplit () ;

         virtual INT32 spaceService () ;

      public :
         virtual const CHAR * name () ;
         virtual RTN_COMMAND_TYPE type () ;
         virtual BOOLEAN      writable () ;
         virtual INT32 init ( INT32 flags, INT64 numToSkip, INT64 numToReturn,
                              const CHAR *pMatcherBuff,
                              const CHAR *pSelectBuff,
                              const CHAR *pOrderByBuff,
                              const CHAR *pHintBuff ) ;
         virtual INT32 doit ( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                              _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                              INT16 w = 1, INT64 *pContextID = NULL ) ;
      protected:
         CHAR _szCollection [ DMS_COLLECTION_SPACE_NAME_SZ +
                              DMS_COLLECTION_NAME_SZ + 2 ] ;
         CHAR _szTargetName [ OP_MAXNAMELENGTH + 1 ] ;
         CHAR _szSourceName [ OP_MAXNAMELENGTH + 1 ] ;
         FLOAT64 _percent  ;

         bson::BSONObj _splitKey ;
   } ;

   class _rtnCancelTask : public _rtnCommand
   {
      DECLARE_CMD_AUTO_REGISTER()
      public:
         _rtnCancelTask () { _taskID = 0 ; }
         ~_rtnCancelTask () {}
         virtual INT32 spaceNode () ;
         virtual BOOLEAN      writable () { return TRUE ; }
         virtual const CHAR * name () { return NAME_CANCEL_TASK ; }
         virtual RTN_COMMAND_TYPE type () { return CMD_CANCEL_TASK ; }

         virtual INT32 init ( INT32 flags, INT64 numToSkip, INT64 numToReturn,
                              const CHAR *pMatcherBuff,
                              const CHAR *pSelectBuff,
                              const CHAR *pOrderByBuff,
                              const CHAR *pHintBuff ) ;
         virtual INT32 doit ( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                              _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                              INT16 w = 1, INT64 *pContextID = NULL ) ;

      protected:
         UINT64            _taskID ;

   } ;

   class _rtnLinkCollection : public _rtnCommand
   {
      DECLARE_CMD_AUTO_REGISTER()

      public:
         _rtnLinkCollection () ;
         ~_rtnLinkCollection () ;

         virtual INT32 spaceService () ;
         virtual const CHAR * name () ;
         virtual RTN_COMMAND_TYPE type () ;
         virtual const CHAR * collectionFullName () ;
         virtual BOOLEAN      writable () { return TRUE ; }

         virtual INT32 init ( INT32 flags, INT64 numToSkip, INT64 numToReturn, 
                              const CHAR *pMatcherBuff,
                              const CHAR *pSelectBuff,
                              const CHAR *pOrderByBuff,
                              const CHAR *pHintBuff ) ;
         virtual INT32 doit ( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                              _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                              INT16 w = 1, INT64 *pContextID = NULL  ) ;
      protected:
         const CHAR           *_collectionName ;
   };

   class _rtnUnlinkCollection : public _rtnCommand
   {
      DECLARE_CMD_AUTO_REGISTER()

      public:
         _rtnUnlinkCollection () ;
         ~_rtnUnlinkCollection () ;

         virtual INT32 spaceService () ;
         virtual const CHAR * name () ;
         virtual RTN_COMMAND_TYPE type () ;
         virtual const CHAR * collectionFullName () ;
         virtual BOOLEAN      writable () { return TRUE ; }

         virtual INT32 init ( INT32 flags, INT64 numToSkip, INT64 numToReturn, 
                              const CHAR *pMatcherBuff,
                              const CHAR *pSelectBuff,
                              const CHAR *pOrderByBuff,
                              const CHAR *pHintBuff ) ;
         virtual INT32 doit ( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                              _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                              INT16 w = 1, INT64 *pContextID = NULL  ) ;
      protected:
         const CHAR           *_collectionName ;
   } ;

   class _rtnAlterCollection : public _rtnCommand
   {
      DECLARE_CMD_AUTO_REGISTER()
   public:
      _rtnAlterCollection() ;
      virtual ~_rtnAlterCollection() ;

   public:
      virtual const CHAR * name () { return NAME_ALTER_COLLECTION ; }
      virtual RTN_COMMAND_TYPE type () { return CMD_ALTER_COLLECTION ; }
      virtual INT32 spaceService() ;
      virtual const CHAR *collectionFullName() ;
      virtual BOOLEAN writable () { return TRUE ; }
      virtual INT32 init ( INT32 flags, INT64 numToSkip, INT64 numToReturn,
                              const CHAR *pMatcherBuff,
                              const CHAR *pSelectBuff,
                              const CHAR *pOrderByBuff,
                              const CHAR *pHintBuff ) ;
      virtual INT32 doit ( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                           _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                           INT16 w = 1, INT64 *pContextID = NULL  ) ;
   private:
      BSONObj _alterObj ;
   };

   class _rtnInvalidateCache : public _rtnCommand
   {
      DECLARE_CMD_AUTO_REGISTER()
   public:
      _rtnInvalidateCache() ;
      virtual ~_rtnInvalidateCache() ;

   public:
      virtual const CHAR *name() { return NAME_INVALIDATE_CACHE ; }
      virtual RTN_COMMAND_TYPE type() { return CMD_INVALIDATE_CACHE ; }
      virtual INT32 spaceNode () ;
      virtual INT32 init ( INT32 flags, INT64 numToSkip, INT64 numToReturn,
                           const CHAR *pMatcherBuff,
                           const CHAR *pSelectBuff,
                           const CHAR *pOrderByBuff,
                           const CHAR *pHintBuff ) ;
      virtual INT32 doit ( _pmdEDUCB *cb, _SDB_DMSCB *dmsCB,
                           _SDB_RTNCB *rtnCB, _dpsLogWrapper *dpsCB,
                           INT16 w = 1, INT64 *pContextID = NULL ) ;
   } ;

}


#endif //CLS_COMMAND_HPP_

