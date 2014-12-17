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

   Source File Name = dmsDump.hpp

   Descriptive Name = Data Management Service Storage Unit Header

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          26/08/2013  XJH Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef DMSDUMP_HPP__
#define DMSDUMP_HPP__

#include "core.hpp"
#include "oss.hpp"
#include "dms.hpp"
#include "dmsExtent.hpp"
#include "dmsRecord.hpp"
#include "ossUtil.hpp"
#include "ossMem.hpp"
#include "dmsStorageData.hpp"
#include "dmsStorageIndex.hpp"
#include "../bson/bson.h"
#include "../bson/bsonobj.h"

#include <deque>

using namespace bson ;
using namespace std ;

namespace engine
{

   #define DMS_SU_DMP_OPT_HEX                ((UINT32)(0x00000001))
   #define DMS_SU_DMP_OPT_HEX_WITH_ASCII     ((UINT32)(0x00000002))
   #define DMS_SU_DMP_OPT_HEX_PREFIX_AS_ADDR ((UINT32)(0x00000004))
   #define DMS_SU_DMP_OPT_FORMATTED          ((UINT32)(0x00000008))

   /*
      Tool functions
   */
   const CHAR* getIndexFlagDesp ( UINT16 indexFlag ) ;
   string getIndexTypeDesp ( UINT16 type ) ;

   class _pmdEDUCB ;

   /*
      _dmsDump define
   */
   class _dmsDump : public SDBObject
   {
      public:
         _dmsDump () {}
         ~_dmsDump () {}

      public:
         static UINT32 dumpHeader ( void * inBuf,
                                    UINT32 inSize,
                                    CHAR * outBuf,
                                    UINT32 outSize,
                                    CHAR * addrPrefix,
                                    UINT32 options,
                                    SINT32 &pageSize,
                                    INT32 &pageNum ) ;

         static UINT32 dumpSME ( void * inBuf,
                                 UINT32 inSize,
                                 CHAR * outBuf,
                                 UINT32 outSize,
                                 UINT32 pageNum ) ;

         static UINT32 dumpMME ( void * inBuf,
                                 UINT32 inSize,
                                 CHAR * outBuf,
                                 UINT32 outSize,
                                 CHAR * addrPrefix,
                                 UINT32 options,
                                 const CHAR *collectionName,
                                 std::vector<UINT16> &collections ) ;

         static UINT32 dumpMB( void * inBuf,
                               UINT32 inSize,
                               CHAR * outBuf,
                               UINT32 outSize,
                               CHAR * addrPrefix,
                               UINT32 options,
                               const CHAR *collectionName,
                               std::vector<UINT16> &collections ) ;

         static UINT32 dumpMBEx( void * inBuf,
                                 UINT32 inSize,
                                 CHAR * outBuf,
                                 UINT32 outSize,
                                 CHAR * addrPrefix,
                                 UINT32 options,
                                 dmsExtentID extID ) ;

         static UINT32 dumpRawPage ( void * inBuf,
                                     UINT32 inSize,
                                     CHAR * outBuf,
                                     UINT32 outSize ) ;

         static UINT32 dumpDataExtent ( _pmdEDUCB *cb,
                                        void * inBuf,
                                        UINT32 inSize,
                                        CHAR * outBuf,
                                        UINT32 outSize,
                                        CHAR * addrPrefix,
                                        UINT32 options,
                                        dmsExtentID &nextExtent,
                                        set<dmsRecordID> *ridList = NULL,
                                        BOOLEAN dumpRecord = FALSE ) ;

         static UINT32 dumpExtentHeader ( void * inBuf,
                                          UINT32 inSize,
                                          CHAR * outBuf,
                                          UINT32 outSize ) ;

         static UINT32 dumpDataExtentHeader ( void * inBuf,
                                              UINT32 inSize,
                                              CHAR * outBuf,
                                              UINT32 outSize ) ;

         static UINT32 dumpMetaExtentHeader( void * inBuf,
                                             UINT32 inSize,
                                             CHAR * outBuf,
                                             UINT32 outSize ) ;

         static UINT32 dumpDataRecord ( _pmdEDUCB *cb,
                                        void * inBuf,
                                        UINT32 inSize,
                                        CHAR * outBuf,
                                        UINT32 outSize,
                                        dmsOffset &nextRecord,
                                        set<dmsRecordID> *ridList = NULL ) ;

         static UINT32 dumpIndexExtent ( void * inBuf,
                                         UINT32 inSize,
                                         CHAR * outBuf,
                                         UINT32 outSize,
                                         CHAR * addrPrefix,
                                         UINT32 options,
                                         deque<dmsExtentID> &childExtents,
                                         BOOLEAN dumpIndexKey = FALSE ) ;

         static UINT32 dumpIndexExtentHeader ( void * inBuf,
                                               UINT32 inSize,
                                               CHAR * outBuf,
                                               UINT32 outSize ) ;

         static UINT32 dumpIndexRecord ( void * inBuf,
                                         UINT32 inSize,
                                         CHAR * outBuf,
                                         UINT32 outSize,
                                         UINT32 keyOffset ) ;

         static UINT32 dumpIndexCBExtentHeader ( void * inBuf,
                                                 UINT32 inSize,
                                                 CHAR * outBuf,
                                                 UINT32 outSize ) ;

         static UINT32 dumpIndexCBExtent (  void * inBuf,
                                            UINT32 inSize,
                                            CHAR * outBuf,
                                            UINT32 outSize,
                                            CHAR * addrPrefix,
                                            UINT32 options,
                                            dmsExtentID &root ) ;

   } ;
   typedef _dmsDump dmsDump ;

}

#endif //DMSDUMP_HPP__

