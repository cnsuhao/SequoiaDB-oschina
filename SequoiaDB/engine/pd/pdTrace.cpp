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

   Source File Name = pdTrace.cpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          12/1/2014  ly  Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include "oss.hpp"
#include "ossUtil.hpp"
#include "pdTrace.hpp"
#if defined (SDB_ENGINE)
#include "pmd.hpp"
#include "pmdDef.hpp"
#include "pmdEDU.hpp"
#include "pmdEDUMgr.hpp"
#endif
#include "ossPrimitiveFileOp.hpp"
using namespace engine ;

ossSpinXLatch gPDTraceMutex;
static BOOLEAN pdTraceMask ( UINT64 funcCode, pdTraceCB *cb )
{
   SDB_ASSERT ( cb, "trace cb can't be NULL" ) ;
   UINT32 component = (UINT32)(funcCode>>32) ;
   return ( component&cb->_componentMask ) != 0 ;
}
#if defined (SDB_ENGINE)
#define PD_TRACE_PAUSE_DFT_WAIT 100
void _pdTraceCB::pause ( UINT64 funcCode )
{
   pmdEDUCB *educb    = NULL ;
   pmdEDUEvent event;

   for ( UINT32 i = 0; i < _numBP; ++i )
   {
      if ( _bpList[i] == funcCode )
      {
         educb    = pmdGetKRCB()->getEDUMgr()->getEDU();
         addPausedEDU ( educb ) ;

         educb->waitEvent( engine::PMD_EDU_EVENT_BP_RESUME, event, -1 ) ;
         break ;
      } // if ( _bpList[i] == funcCode )
   } // for ( i = 0; i < _numBP; ++i )
}
#endif

void pdTraceFunc ( UINT64 funcCode, INT32 type,
                   const CHAR* file, UINT32 line,
                   pdTraceArgTuple *tuple )
{
   if ( sdbGetPDTraceCB()->_traceStarted.compare(FALSE) )
      return ;
   if ( !pdTraceMask ( funcCode, sdbGetPDTraceCB() ) )
      return ;
   INT32 numSlots = 0 ;
   pdTraceRecord record ;
   void *pBuffer = NULL ;
   UINT32 code = (UINT32)funcCode&0xFFFFFFFF;
   ossMemcpy ( record._eyeCatcher, TRACE_EYE_CATCHER,
               TRACE_EYE_CATCHER_SIZE ) ;
   record._recordSize = sizeof(record) ;
   record._functionID = code ;
   record._flag       = type ;
   record._tid        = ossGetCurrentThreadID () ;
   record._line       = line ;
   record._numArgs    = 0 ;
   ossGetCurrentTime ( record._timestamp ) ;

   for ( INT32 i = 0; i < PD_TRACE_MAX_ARG_NUM; ++i )
   {
      if ( PD_TRACE_ARGTYPE_NONE != tuple[i].x )
      {
         ++record._numArgs ;
         record._recordSize += tuple[i].z +
                               sizeof(tuple[i].x) +
                               sizeof(tuple[i].z);
      }
   }
   numSlots = (INT32)ceil( (FLOAT64)record._recordSize /
                           (FLOAT64)TRACE_SLOT_SIZE ) ;
   pBuffer = sdbGetPDTraceCB()->reserveSlots ( numSlots ) ;
   if ( !pBuffer )
      goto done ;
   sdbGetPDTraceCB()->startWrite () ;
   pBuffer = sdbGetPDTraceCB()->fillIn ( pBuffer, &record, sizeof(record) ) ;
   for ( INT32 i = 0; i < PD_TRACE_MAX_ARG_NUM; ++i )
   {
      if ( PD_TRACE_ARGTYPE_NONE != tuple[i].x )
      {
         pBuffer = sdbGetPDTraceCB()->fillIn ( pBuffer, &tuple[i].x,
                                               sizeof(tuple[i].x) ) ;
         pBuffer = sdbGetPDTraceCB()->fillIn ( pBuffer, &tuple[i].z,
                                               sizeof(tuple[i].z) ) ;
         pBuffer = sdbGetPDTraceCB()->fillIn ( pBuffer, tuple[i].y,
                                               tuple[i].z-sizeof(pdTraceArgument) ) ;
      }
   }
   sdbGetPDTraceCB()->finishWrite () ;
#if defined (SDB_ENGINE)
   if ( sdbGetPDTraceCB()->_numBP )
      sdbGetPDTraceCB()->pause ( code ) ;
#endif
done :
   return ;
}

_pdTraceCB::_pdTraceCB()
:_traceStarted(FALSE),
 _currentSlot(0),
 _currentWriter(0),
 _componentMask(0xFFFFFFFF),
 _totalChunks(0),
 _totalSlots(0),
 _pBuffer(NULL)
{
   _headerSize = sizeof(_pdTraceCB) ;
   ossMemcpy ( _eyeCatcher, TRACECB_EYE_CATCHER,
               TRACE_EYE_CATCHER_SIZE ) ;
}

_pdTraceCB::~_pdTraceCB()
{
   reset () ;
}

void* _pdTraceCB::reserveOneSlot ()
{
   INT32 slot = (INT32)_currentSlot.inc () ;
   slot %= _totalSlots ;
   return &_pBuffer[slot*TRACE_SLOT_SIZE] ;
}

void* _pdTraceCB::reserveSlots ( UINT32 numSlots )
{
   INT32 slot = (INT32)_currentSlot.add ( numSlots ) ;
   slot %= _totalSlots ;
   return &_pBuffer[slot*TRACE_SLOT_SIZE] ;
}

void* _pdTraceCB::fillIn ( void *pPos, const void *pInput, INT32 size )
{
   INT32 traceBufferSize = _totalSlots*TRACE_SLOT_SIZE ;
   CHAR *pRetAddr        = NULL ;
   ossValuePtr posStart  = 0 ;
   ossValuePtr posEnd    = 0 ;
   SDB_ASSERT ( pPos && pInput, "pos and input can't be NULL" ) ;
   SDB_ASSERT ( pPos >= _pBuffer, "pos can't be smaller than buffer" ) ;
   if ( size < 0 || size >= TRACE_RECORD_MAX_SIZE )
   {
      pRetAddr = (CHAR*)pPos ;
      goto done ;
   }
   posStart = (ossValuePtr)_pBuffer +
                 (((ossValuePtr)pPos-(ossValuePtr)_pBuffer) %
                 traceBufferSize ) ;
   posEnd = (ossValuePtr)_pBuffer +
                 (((ossValuePtr)pPos-(ossValuePtr)_pBuffer + size) %
                 traceBufferSize ) ;
   if ( posStart > posEnd )
   {
      INT32 diff = posEnd - (ossValuePtr)_pBuffer ;
      ossMemcpy ( pPos, pInput, size-diff ) ;
      ossMemcpy ( _pBuffer, (CHAR*)pInput+size-diff, diff ) ;
      pRetAddr = _pBuffer + diff ;
   }
   else
   {
      ossMemcpy ( pPos, pInput, size ) ;
      pRetAddr = (CHAR*)((ossValuePtr)pPos + size) ;
   }
done :
   return pRetAddr ;
}

void _pdTraceCB::startWrite ()
{
   _currentWriter.inc() ;
}

void _pdTraceCB::finishWrite ()
{
   _currentWriter.dec() ;
}

void _pdTraceCB::setMask ( UINT32 mask )
{
   _componentMask = mask ;
}

UINT32 _pdTraceCB::getMask ()
{
   return _componentMask ;
}

UINT32 _pdTraceCB::getSlotNum ()
{
   return _totalSlots ;
}

UINT32 _pdTraceCB::getChunkNum ()
{
   return _totalChunks ;
}

INT32 _pdTraceCB::start ( UINT64 size )
{
   return start ( size, 0xFFFFFFFF, NULL ) ;
}

INT32 _pdTraceCB::start ( UINT64 size, UINT32 mask )
{
   return start ( size, mask, NULL ) ;
}

INT32 _pdTraceCB::start ( UINT64 size, UINT32 mask,
                          std::vector<UINT64> *funcCode )
{
   INT32 rc = SDB_OK ;
   gPDTraceMutex.get() ;
   if ( _traceStarted.compare ( TRUE ) )
   {
      PD_LOG ( PDWARNING, "Trace is already started" ) ;
      rc = SDB_PD_TRACE_IS_STARTED ;
      goto error ;
   }
   reset () ;
#if defined (SDB_ENGINE)
   if ( funcCode )
   {
      for ( UINT32 i = 0; i < funcCode->size(); ++i )
      {
         rc = addBreakPoint ( (*funcCode)[i] ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add break point, rc = %d", rc ) ;
            removeAllBreakPoint() ;
            goto error ;
         }
      }
   }
#endif
   PD_LOG ( PDEVENT, "Trace starts, buffersize = %llu, mask = 0x%x",
            size, mask ) ;
   size           = ossRoundUpToMultipleX ( size, TRACE_CHUNK_SIZE ) ;
   size           = OSS_MAX ( size, TRACE_MIN_BUFFER_SIZE ) ;
   size           = OSS_MIN ( size, TRACE_MAX_BUFFER_SIZE ) ;
   _pBuffer       = (CHAR*)SDB_OSS_MALLOC ( (size_t)size ) ;
   if ( _pBuffer )
   {
      _totalChunks   = (UINT32)(size / TRACE_CHUNK_SIZE) ;
      _totalSlots    = _totalChunks * TRACE_SLOTS_PER_CHUNK ;
      _componentMask = mask ;
      _traceStarted.init ( TRUE ) ;
   }
   else
   {
      PD_LOG ( PDERROR,
               "Failed to allocate memory for trace buffer: %lld bytes",
               size ) ;
   }
done :
   gPDTraceMutex.release() ;
   return rc ;
error :
   goto done ;
}

void _pdTraceCB::stop ()
{
   PD_LOG ( PDEVENT, "Trace stops" ) ;
   _traceStarted.compareAndSwap ( TRUE, FALSE ) ;
}

INT32 _pdTraceCB::dump ( const CHAR *pFileName )
{
   INT32 rc = SDB_OK ;
   ossPrimitiveFileOp file ;
   gPDTraceMutex.get () ;
   PD_CHECK ( _traceStarted.compare ( FALSE ), SDB_PD_TRACE_IS_STARTED,
              error, PDWARNING,
              "Trace must be stopped before dumping" ) ;
   PD_CHECK ( _pBuffer, SDB_PD_TRACE_HAS_NO_BUFFER, error, PDWARNING,
              "Trace buffer does not exist, trace must be captured "
              "before dump" ) ;
   while ( !_currentWriter.compare(0) ) ;
   PD_LOG ( PDEVENT, "Trace dumps to %s", pFileName ) ;
   rc = file.Open ( pFileName,
                    OSS_PRIMITIVE_FILE_OP_WRITE_ONLY |
                    OSS_PRIMITIVE_FILE_OP_OPEN_ALWAYS |
                    OSS_PRIMITIVE_FILE_OP_OPEN_TRUNC ) ;
   PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
              "Failed to dump trace to file %s, errno=%d",
              pFileName, rc ) ;

   rc = file.Write ( this, sizeof(_pdTraceCB) ) ;
   PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
              "Failed to write trace to file %s, errno=%d",
              pFileName, rc ) ;
   for ( UINT32 i = 0; i < _totalChunks; ++i )
   {
      rc = file.Write ( &_pBuffer[i*TRACE_CHUNK_SIZE],
                        TRACE_CHUNK_SIZE ) ;
      PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                 "Failed to write trace to file %s, errno=%d",
                 pFileName, rc ) ;
   }
done :
   file.Close () ;
   gPDTraceMutex.release () ;
   return rc ;
error :
   goto done ;
}

class _pdTraceFormatSession : public SDBObject
{
public :
   UINT32 _tid ;
   std::vector<UINT32> _sequenceNum ;
   std::vector<UINT32> _slotNum ;
} ;

class _pdTraceFormatSystem : public SDBObject
{
public :
   std::map<UINT32, _pdTraceFormatSession *> _threadLists ;
   ~_pdTraceFormatSystem ()
   {
      clear () ;
   }
   void clear ()
   {
      std::map<UINT32, _pdTraceFormatSession *>::iterator it ;
      for ( it = _threadLists.begin() ;
            it != _threadLists.end() ;
            ++it )
      {
         SDB_OSS_DEL ( (*it).second ) ;
      }
      _threadLists.clear() ;
   }
} ;

_pdTraceFormatSystem gTraceFormatSystem ;

static INT32 pdTraceFormatProcessSlot ( ossPrimitiveFileOp *file,
                                        ossPrimitiveFileOp *out,
                                        UINT32 totalSlots,
                                        _pdTraceFormatType formatType,
                                        UINT32 &sequenceNum,
                                        INT32 &numIndent,
                                        UINT32 &slot,    // input and output
                                        UINT32 headerSize )
{
   INT32 rc = SDB_OK ;
   ossPrimitiveFileOp::offsetType offset ;
   CHAR tempBuf [ TRACE_RECORD_MAX_SIZE ] ;
   SDB_ASSERT ( file, "file can't be NULL" ) ;
   INT32 readSize = 0 ;
   INT32 numSlots = 0 ;
   pdTraceRecord *record = NULL ;
   UINT32 slotRead = 0 ;
   offset.offset = headerSize + slot*TRACE_SLOT_SIZE ;
   file->seekToOffset ( offset ) ;
   rc = file->Read ( TRACE_SLOT_SIZE, tempBuf, &readSize ) ;
   slotRead = slot ;
   slot = (slot+1)%totalSlots ;
   PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
              "Failed to read from trace file, errno=%d", rc ) ;
   PD_CHECK ( readSize == TRACE_SLOT_SIZE, SDB_PD_TRACE_FILE_INVALID,
              error, PDERROR,
              "Unable to read full slot for %u", slotRead ) ;
   record = (pdTraceRecord*)&tempBuf ;
   if ( ossMemcmp ( record->_eyeCatcher,
                    TRACE_EYE_CATCHER,
                    TRACE_EYE_CATCHER_SIZE ) != 0 )
   {
      goto done ;
   }
   PD_CHECK ( record->_recordSize <= TRACE_RECORD_MAX_SIZE,
              SDB_PD_TRACE_FILE_INVALID, error, PDERROR,
              "Too big trace record in slot %u", slotRead ) ;
   numSlots = (INT32)ceil( (FLOAT64)record->_recordSize /
                           (FLOAT64)TRACE_SLOT_SIZE ) ;

   if ( formatType == PD_TRACE_FORMAT_TYPE_FLOW_PREPARE )
   {
      UINT32 tid = record->_tid ;
      std::map<UINT32, _pdTraceFormatSession *>::iterator it ;
      if ( ( it = gTraceFormatSystem._threadLists.find ( tid ) ) ==
           gTraceFormatSystem._threadLists.end() )
      {
         _pdTraceFormatSession *formatSession = new _pdTraceFormatSession() ;
         PD_CHECK ( formatSession, SDB_OOM, error, PDERROR,
                    "Failed to allocate memory for _pdTraceFormatSession" ) ;
         formatSession->_tid = tid ;
         gTraceFormatSystem._threadLists[tid] = formatSession ;
         it = gTraceFormatSystem._threadLists.find ( tid ) ;
         SDB_ASSERT ( it != gTraceFormatSystem._threadLists.end(),
                      "we must be able to find the session" ) ;
      }
      (*it).second->_sequenceNum.push_back ( sequenceNum ) ;
      (*it).second->_slotNum.push_back ( slotRead ) ;
      slot = ( slotRead + numSlots ) % totalSlots ;
      sequenceNum ++ ;
   }
   else if ( formatType == PD_TRACE_FORMAT_TYPE_FLOW )
   {
      SDB_ASSERT ( out, "out can't be NULL" ) ;
      CHAR timestamp[64] ;
      for ( INT32 i = 1; i < numSlots; ++i )
      {
         offset.offset = headerSize + slot*TRACE_SLOT_SIZE ;
         file->seekToOffset ( offset ) ;

         rc = file->Read ( TRACE_SLOT_SIZE,
                           &tempBuf[(i)*TRACE_SLOT_SIZE],
                           &readSize ) ;
         slotRead = slot ;
         slot = (slot+1)%totalSlots ;
         PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                    "Failed to read from trace file, errno=%d", rc ) ;
         PD_CHECK ( readSize == TRACE_SLOT_SIZE, SDB_PD_TRACE_FILE_INVALID,
                    error, PDERROR,
                    "Unable to read full slot for %u", slotRead ) ;
      }
      rc = out->fWrite ( "%u: ", sequenceNum ) ;
      PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                 "Failed to write into trace file, errno = %d", rc ) ;

      if ( record->_flag == PD_TRACE_RECORD_FLAG_EXIT )
      {
         numIndent -- ;
         if ( numIndent < 0 )
            numIndent = 0 ;
      }
      for ( INT32 i = 0; i < numIndent; ++i )
      {
         rc = out->Write ( "| ", 2 ) ;
         PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                    "Failed to write into trace file, errno = %d", rc ) ;
      }

      rc = out->fWrite ( "%s",
                         pdGetTraceFunction(record->_functionID),
                         record->_line ) ;
      PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                 "Failed to write into trace file, errno = %d", rc ) ;

      if ( record->_flag == PD_TRACE_RECORD_FLAG_ENTRY )
      {
         rc = out->fWrite ( " Entry" ) ;
         PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                    "Failed to write into trace file, errno = %d", rc ) ;

         numIndent ++ ;
      }
      else if ( record->_flag == PD_TRACE_RECORD_FLAG_EXIT )
      {
         SDB_ASSERT ( sizeof(pdTraceRecord) + sizeof(pdTraceArgument) +
                      sizeof(UINT32) <= TRACE_SLOT_SIZE,
                      "trace header with single rc is greater than slot size" ) ;
         rc = out->fWrite ( " Exit" ) ;
         PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                    "Failed to write into trace file, errno = %d", rc ) ;
         if ( record->_numArgs == 1 )
         {
            pdTraceArgument *arg =
                  (pdTraceArgument*)&tempBuf[sizeof(pdTraceRecord)] ;
            if ( PD_TRACE_ARGTYPE_INT == arg->_argumentType )
            {
               INT32 retCode = *(INT32*)(((CHAR*)arg)+sizeof(pdTraceArgument));
               if ( SDB_OK != retCode )
               {
                  rc = out->fWrite ( "[retCode=%d]", retCode ) ;
                  PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                             "Failed to write into trace file, errno = %d",
                             rc ) ;
               }
            } // if ( PD_TRACE_ARGTYPE_INT == arg->_argumentType )
         } // if ( record->_numArgs == 1 )
      } // else if ( record->_flag == PD_TRACE_RECORD_FLAG_EXIT )
      ossTimestampToString ( record->_timestamp, timestamp ) ;
      rc = out->fWrite ( "(%u): %s"OSS_NEWLINE, record->_line, timestamp ) ;
      PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                 "Failed to write into trace file, errno = %d", rc ) ;
   }
   else if ( formatType == PD_TRACE_FORMAT_TYPE_FORMAT )
   {
      SDB_ASSERT ( out, "out can't be NULL" ) ;
      CHAR timestamp[64] ;
      CHAR *pArgs ;
      for ( INT32 i = 1; i < numSlots; ++i )
      {
         offset.offset = headerSize + slot*TRACE_SLOT_SIZE ;
         file->seekToOffset ( offset ) ;
         rc = file->Read ( TRACE_SLOT_SIZE,
                           &tempBuf[(i)*TRACE_SLOT_SIZE],
                           &readSize ) ;
         slotRead = slot ;
         slot = (slot+1)%totalSlots ;
         PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                    "Failed to read from trace file, errno=%d", rc ) ;
         PD_CHECK ( readSize == TRACE_SLOT_SIZE, SDB_PD_TRACE_FILE_INVALID,
                    error, PDERROR,
                    "Unable to read full slot for %u", slotRead ) ;
      }
      rc = out->fWrite ( "%u: ", sequenceNum ) ;
      PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                 "Failed to write into trace file, errno = %d", rc ) ;
      sequenceNum ++ ;
      ossTimestampToString ( record->_timestamp, timestamp ) ;
      rc = out->fWrite ( "%s(%u): %s"OSS_NEWLINE,
                         pdGetTraceFunction(record->_functionID),
                         record->_line, timestamp ) ;
      PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                 "Failed to write into trace file, errno = %d", rc ) ;
      rc = out->fWrite ( "tid: %u, numArgs: %u"OSS_NEWLINE,
                         record->_tid, record->_numArgs ) ;
      PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                 "Failed to write into trace file, errno = %d", rc ) ;

      pArgs = &tempBuf[sizeof(pdTraceRecord)] ;
      for ( UINT32 i = 0; i < record->_numArgs; i++ )
      {
         if ( pArgs - &tempBuf[0] >= TRACE_RECORD_MAX_SIZE ||
              pArgs - &tempBuf[0] < (INT32)sizeof(pdTraceRecord) )
         {
            PD_RC_CHECK ( SDB_PD_TRACE_FILE_INVALID, PDERROR,
                          "Invalid argument offset" ) ;
         }
         pdTraceArgument *arg = (pdTraceArgument*)pArgs ;
         pArgs += arg->_argumentSize ;
         rc = out->fWrite ( "\targ%d:"OSS_NEWLINE, i ) ;
         PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                    "Failed to write into trace file, errno = %d", rc ) ;
         switch ( arg->_argumentType )
         {
         case PD_TRACE_ARGTYPE_NULL :
            rc = out->fWrite ( "\t\tNULL"OSS_NEWLINE ) ;
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_CHAR :
            rc = out->fWrite ( "\t\t%c"OSS_NEWLINE,
                               (*(CHAR*)(((CHAR*)arg)+
                               sizeof(pdTraceArgument))));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_BYTE :
            rc = out->fWrite ( "\t\t0x%x"OSS_NEWLINE,
                               (UINT32)(*(CHAR*)(((CHAR*)arg)+
                               sizeof(pdTraceArgument))));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_SHORT :
            rc = out->fWrite ( "\t\t%d"OSS_NEWLINE,
                               (INT32)(*(INT16*)(((CHAR*)arg)+
                               sizeof(pdTraceArgument))));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_USHORT :
            rc = out->fWrite ( "\t\t%u"OSS_NEWLINE,
                               (UINT32)(*(UINT16*)(((CHAR*)arg)+
                               sizeof(pdTraceArgument))));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_INT :
            rc = out->fWrite ( "\t\t%d"OSS_NEWLINE,
                               *(INT32*)(((CHAR*)arg)+sizeof(pdTraceArgument)));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_UINT :
            rc = out->fWrite ( "\t\t%u"OSS_NEWLINE,
                               *(UINT32*)(((CHAR*)arg)+sizeof(pdTraceArgument)));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_LONG :
            rc = out->fWrite ( "\t\t%lld"OSS_NEWLINE,
                               *(INT64*)(((CHAR*)arg)+sizeof(pdTraceArgument)));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_ULONG :
            rc = out->fWrite ( "\t\t%llu"OSS_NEWLINE,
                               *(UINT64*)(((CHAR*)arg)+sizeof(pdTraceArgument)));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_FLOAT :
            rc = out->fWrite ( "\t\t%f"OSS_NEWLINE,
                               *(FLOAT32*)(((CHAR*)arg)+
                                            sizeof(pdTraceArgument)));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_DOUBLE :
            rc = out->fWrite ( "\t\t%f"OSS_NEWLINE,
                               *(FLOAT64*)(((CHAR*)arg)+
                                            sizeof(pdTraceArgument)));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_STRING :
            rc = out->fWrite ( "\t\t%s"OSS_NEWLINE,
                               (((CHAR*)arg)+
                               sizeof(pdTraceArgument)));
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         case PD_TRACE_ARGTYPE_RAW :
         {
            INT32 rawSize = arg->_argumentSize - sizeof(pdTraceArgument) ;
            UINT32 outSize = 10*rawSize ;
            CHAR *pTempBuffer = (CHAR*)SDB_OSS_MALLOC ( outSize ) ;
            PD_CHECK ( pTempBuffer, SDB_OOM, error, PDERROR,
                       "Failed to allocate memory for temp buffer" ) ;
            ossHexDumpBuffer ( (((CHAR*)arg)+
                               sizeof(pdTraceArgument)),
                               rawSize,
                               pTempBuffer,
                               outSize,
                               NULL,
                               OSS_HEXDUMP_INCLUDE_ADDR ) ;
            rc = out->fWrite ( "%s"OSS_NEWLINE, pTempBuffer ) ;
            SDB_OSS_FREE ( pTempBuffer ) ;
            PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                       "Failed to write into trace file, errno = %d", rc ) ;
            break ;
         }
         case PD_TRACE_ARGTYPE_NONE :
         default :
            break ;
         }
      }
      rc = out->fWrite ( OSS_NEWLINE ) ;
      PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                 "Failed to write into trace file, errno = %d", rc ) ;
   }
   else
   {
      PD_RC_CHECK ( SDB_SYS, PDERROR, "Unexpected format type: %d",
                    formatType ) ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 _pdTraceCB::format ( const CHAR *pInputFileName,
                           const CHAR *pOutputFileName,
                           _pdTraceFormatType type )
{
   INT32 rc               = SDB_OK ;
   INT32 byteRead         = 0 ;
   UINT64 fileSize        = 0 ;
   _pdTraceCB traceCB ;
   CHAR *pFormatBuffer    = NULL ;
   BOOLEAN wrap           = FALSE ;
   ossPrimitiveFileOp::offsetType offset ;
   ossPrimitiveFileOp file ;
   ossPrimitiveFileOp out ;
   INT32  numIndent = 0 ;
   rc = file.Open ( pInputFileName,
                    OSS_PRIMITIVE_FILE_OP_READ_ONLY |
                    OSS_PRIMITIVE_FILE_OP_OPEN_ALWAYS ) ;
   PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
              "Failed to dump trace to file %s, errno=%d",
              pInputFileName, rc ) ;
   rc = file.getSize ( &offset ) ;
   PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
              "Failed to get trace file size for %s, errno=%d",
              pInputFileName, rc ) ;
   fileSize = offset.offset ;
   rc = out.Open ( pOutputFileName,
                   OSS_PRIMITIVE_FILE_OP_WRITE_ONLY |
                   OSS_PRIMITIVE_FILE_OP_OPEN_ALWAYS |
                   OSS_PRIMITIVE_FILE_OP_OPEN_TRUNC ) ;
   PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
              "Failed to dump trace to file %s, errno=%d",
              pOutputFileName, rc ) ;
   rc = file.Read ( sizeof(_pdTraceCB), &traceCB, &byteRead ) ;
   traceCB._pBuffer = NULL ;
   PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
              "Failed to write trace to file %s, errno=%d",
              pInputFileName, rc ) ;
   PD_CHECK ( byteRead == sizeof(_pdTraceCB),
              SDB_PD_TRACE_FILE_INVALID, error, PDWARNING,
              "Unable to read header" ) ;
   PD_CHECK ( fileSize == traceCB._totalChunks*TRACE_CHUNK_SIZE +
                 traceCB._headerSize,
              SDB_PD_TRACE_FILE_INVALID, error, PDWARNING,
              "Trace file is not valid, real size: %lld, expected %lld",
              fileSize, traceCB._totalChunks*TRACE_CHUNK_SIZE +
                 traceCB._headerSize  ) ;
   PD_CHECK ( ossMemcmp ( traceCB._eyeCatcher, TRACECB_EYE_CATCHER,
                          TRACE_EYE_CATCHER_SIZE ) == 0,
              SDB_PD_TRACE_FILE_INVALID, error, PDWARNING,
              "Invalid eye catcher" ) ;
   if ( sizeof(_pdTraceCB) < traceCB._headerSize )
   {
      ossPrimitiveFileOp::offsetType off ;
      off.offset = traceCB._headerSize ;
      file.seekToOffset ( off ) ;
   }
   else if ( sizeof(_pdTraceCB) > traceCB._headerSize )
   {
      PD_LOG ( PDWARNING,
               "Trace file header is not valid, should be greater "
               "or equal to %lld bytes, but actual %lld bytes",
               sizeof(_pdTraceCB), traceCB._headerSize ) ;
      goto error ;
   }
   if ( traceCB._currentSlot.peek() >= traceCB._totalSlots )
   {
      wrap = TRUE ;
   }
   traceCB._currentSlot.init ( traceCB._currentSlot.peek() %
                               traceCB._totalSlots ) ;
   if ( wrap )
   {
      UINT32 sequenceNum = 0 ;
      BOOLEAN wrapped = FALSE ;
      UINT32 prev = 0 ;
      UINT32 i = ( traceCB._currentSlot.peek() + 1 ) % traceCB._totalSlots ;
      while ( !wrapped || i < traceCB._currentSlot.peek() )
      {
         prev = i ;
         rc = pdTraceFormatProcessSlot ( &file, &out, traceCB._totalSlots,
              type == PD_TRACE_FORMAT_TYPE_FORMAT ?
              PD_TRACE_FORMAT_TYPE_FORMAT:
              PD_TRACE_FORMAT_TYPE_FLOW_PREPARE,
              sequenceNum, numIndent, i, traceCB._headerSize ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to format slot %d, rc = %d",
                       i, rc ) ;
         if ( i < prev )
         {
            wrapped = TRUE ;
         }
      }
   }
   else
   {
      UINT32 sequenceNum = 0 ;
      for ( UINT32 i = 0; i < traceCB._currentSlot.peek(); )
      {
         rc = pdTraceFormatProcessSlot ( &file, &out, traceCB._totalSlots,
              type == PD_TRACE_FORMAT_TYPE_FORMAT ?
              PD_TRACE_FORMAT_TYPE_FORMAT:
              PD_TRACE_FORMAT_TYPE_FLOW_PREPARE,
              sequenceNum, numIndent, i, traceCB._headerSize ) ;
         PD_RC_CHECK ( rc, PDERROR, "Failed to format slot %d, rc = %d",
                       i, rc ) ;
      }
   }
   if ( type == PD_TRACE_FORMAT_TYPE_FLOW )
   {
      std::map<UINT32, _pdTraceFormatSession *>::iterator it ;
      for ( it = gTraceFormatSystem._threadLists.begin() ;
            it != gTraceFormatSystem._threadLists.end() ;
            ++it )
      {
         numIndent = 0 ;
         rc = out.fWrite ( "tid: %u"OSS_NEWLINE, (*it).second->_tid ) ;
         PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                    "Failed to write into trace file, errno = %d", rc ) ;

         std::vector<UINT32>::iterator slotit ;
         std::vector<UINT32>::iterator seqit ;
         slotit = (*it).second->_slotNum.begin() ;
         seqit  = (*it).second->_sequenceNum.begin() ;
         PD_CHECK ( (*it).second->_slotNum.size() ==
                    (*it).second->_sequenceNum.size(),
                    SDB_SYS, error, PDERROR,
                    "slot num and sequence num must be same" ) ;
         for ( ; slotit != (*it).second->_slotNum.end();
               ++slotit, ++seqit )
         {
            rc = pdTraceFormatProcessSlot ( &file, &out, traceCB._totalSlots,
                    PD_TRACE_FORMAT_TYPE_FLOW,
                    *seqit, numIndent, *slotit, traceCB._headerSize ) ;
            PD_RC_CHECK ( rc, PDERROR, "Failed to format slot %d, rc = %d",
                          *slotit, rc ) ;
         }
         rc = out.fWrite ( OSS_NEWLINE ) ;
         PD_CHECK ( 0 == rc, SDB_IO, error, PDERROR,
                    "Failed to write into trace file, errno = %d", rc ) ;
      }
   } // if ( type == PD_TRACE_FORMAT_TYPE_FLOW )
done :
   file.Close () ;
   out.Close () ;
   if ( pFormatBuffer )
   {
      SDB_OSS_FREE ( pFormatBuffer ) ;
      pFormatBuffer = NULL ;
   }
   return rc ;
error :
   goto done ;
}

void _pdTraceCB::destroy ()
{
   gPDTraceMutex.get() ;
   reset () ;
   gPDTraceMutex.release() ;
}

void _pdTraceCB::reset ()
{
   _traceStarted.compareAndSwap ( TRUE, FALSE ) ;
   while ( !_currentWriter.compare(0) ) ;
   if ( _pBuffer )
   {
      SDB_OSS_FREE ( _pBuffer ) ;
      _pBuffer = NULL ;
   }
   _traceStarted.init ( FALSE ) ;
   _currentSlot.init ( 0 ) ;
   _currentWriter.init ( 0 ) ;
   _componentMask = 0xFFFFFFFF ;
   _totalChunks   = 0 ;
   _totalSlots    = 0 ;
   ossMemcpy ( _eyeCatcher, TRACECB_EYE_CATCHER,
               TRACE_EYE_CATCHER_SIZE ) ;
#if defined (SDB_ENGINE)
   removeAllBreakPoint() ;
   if ( !_pmdEDUCBList.empty() )
   {
      ossSleepsecs ( 1 ) ;
      resumePausedEDUs () ;
   }
#endif
}

#if defined (SDB_ENGINE)
INT32 _pdTraceCB::addBreakPoint( UINT64 functionCode )
{
   INT32 rc = SDB_OK ;
   for ( UINT32 i = 0; i < _numBP; ++i )
   {
      if ( functionCode == _bpList[i] )
         goto done ;
   }
   if ( _numBP >= PD_TRACE_MAX_BP_NUM )
   {
      rc = SDB_TOO_MANY_TRACE_BP ;
      goto error ;
   }
   _bpList[_numBP] = functionCode ;
   ++_numBP ;
done :
   return rc ;
error :
   goto done ;
}

void _pdTraceCB::removeAllBreakPoint()
{
   _numBP = 0 ;
   ossMemset ( &_bpList[0], 0, sizeof(_bpList) ) ;
}

void _pdTraceCB::addPausedEDU ( engine::_pmdEDUCB *cb )
{
   _pmdEDUCBLatch.get() ;
   _pmdEDUCBList.push_back ( cb ) ;
   _pmdEDUCBLatch.release () ;
}
void _pdTraceCB::resumePausedEDUs ()
{
   if ( PMD_IS_DB_DOWN )
      return ;
   _pmdEDUCBLatch.get() ;
   pmdEDUEvent event ( PMD_EDU_EVENT_BP_RESUME ) ;
   while ( !_pmdEDUCBList.empty() )
   {
      _pmdEDUCBList.front()->postEvent ( event ) ;
      _pmdEDUCBList.pop_front () ;
   }
   _pmdEDUCBLatch.release () ;
}

#endif // SDB_ENGINE

/*
   get global pdtrace cb
*/
pdTraceCB* sdbGetPDTraceCB ()
{
   static pdTraceCB s_pdTraceCB ;
   return &s_pdTraceCB ;
}

