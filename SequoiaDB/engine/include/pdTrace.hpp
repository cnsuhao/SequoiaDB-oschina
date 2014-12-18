/*******************************************************************************

   Copyright (C) 2012-2014 SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   Source File Name = pd.hpp

   Descriptive Name = Problem Determination Header

   When/how to use: this program may be used on binary and text-formatted
   versions of PD component. This file contains declare of PD functions.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef PDTRACE_HPP__
#define PDTRACE_HPP__

#include "core.hpp"
#include "ossUtil.hpp"
#include "ossLatch.hpp"
#include "ossAtomic.hpp"
#include "oss.hpp"
#include "pdTrace.h"
#include <list>
#include <vector>

#define PD_TRACE_MAX_BP_NUM         10
/*
 * Slots and chunks
 *
 * The trace buffer is divided into slots and chunks, slots are the smallest
 * element in the buffer. Multiple slots ( TRACE_SLOTS_PER_CHUNK ) are grouped
 * into a single chunk, and multiple chunks are grouped into the entire trace
 * buffer
 * for Performance reason, we should never write a single slot into file,
 * instead we should flush file chunk by chunk.
 *
 * Each log record start from one slot, and may sit in one slot or cross
 * multiple slots.
 */
#define TRACE_CHUNK_SIZE      131072 /* bytes */
#define TRACE_SLOT_SIZE       64     /* bytes */
#define TRACE_SLOTS_PER_CHUNK (TRACE_CHUNK_SIZE/TRACE_SLOT_SIZE)

#define TRACE_RECORD_MAX_SIZE TRACE_CHUNK_SIZE

/*
 * Trace buffer should always be power of 2 and multiple of chunk size
 * Even thou there's no physical limitation of upper limit of buffer size, but
 * we should still limit it to a practical number, let's say 1GB
 */
#define TRACE_MIN_BUFFER_SIZE (4*TRACE_CHUNK_SIZE)  /* bytes */
#define TRACE_MAX_BUFFER_SIZE ( 1*1024*1024*1024 )  /* bytes */
#define TRACE_DFT_BUFFER_SIZE ( 256*1024*1024 )     /* bytes */

/*
 * Put \n ( newline ) as eye catcher. We put this one in trace file header,
 * so that if the trace file was FTP'd using ASCII mode between UNIX machine and
 * PC, this eye catcher will
 * be corrupted so that we can easily detect whether a trace file is valid or
 * not
 */
#define TRACE_EYE_CATCHER_SIZE     8
#if defined (_LINUX)
#define TRACE_EYE_CATCHER          "TRACE\n  "
#define TRACECB_EYE_CATCHER        "@TRACE\n "
#elif defined (_WINDOWS)
#define TRACE_EYE_CATCHER          "TRACE\r\n "
#define TRACECB_EYE_CATCHER        "@TRACE\r\n"
#endif

const INT32 _pdTraceComponentNum = 28 ;

#define PD_TRACE_COMPONENT_AUTH    0x00000001
#define PD_TRACE_COMPONENT_BPS     0x00000002
#define PD_TRACE_COMPONENT_CAT     0x00000004
#define PD_TRACE_COMPONENT_CLS     0x00000008
#define PD_TRACE_COMPONENT_DPS     0x00000010
#define PD_TRACE_COMPONENT_MIG     0x00000020
#define PD_TRACE_COMPONENT_MSG     0x00000040
#define PD_TRACE_COMPONENT_NET     0x00000080
#define PD_TRACE_COMPONENT_OSS     0x00000100
#define PD_TRACE_COMPONENT_PD      0x00000200
#define PD_TRACE_COMPONENT_RTN     0x00000400
#define PD_TRACE_COMPONENT_SQL     0x00000800
#define PD_TRACE_COMPONENT_TOOL    0x00001000
#define PD_TRACE_COMPONENT_BAR     0x00002000
#define PD_TRACE_COMPONENT_CLIENT  0x00004000
#define PD_TRACE_COMPONENT_COORD   0x00008000
#define PD_TRACE_COMPONENT_DMS     0x00010000
#define PD_TRACE_COMPONENT_IXM     0x00020000
#define PD_TRACE_COMPONENT_MON     0x00040000
#define PD_TRACE_COMPONENT_MTH     0x00080000
#define PD_TRACE_COMPONENT_OPT     0x00100000
#define PD_TRACE_COMPONENT_PMD     0x00200000
#define PD_TRACE_COMPONENT_REST    0x00400000
#define PD_TRACE_COMPONENT_SPT     0x00800000
#define PD_TRACE_COMPONENT_UTIL    0x01000000
#define PD_TRACE_COMPONENT_AGGR    0x02000000
#define PD_TRACE_COMPONENT_SPD     0x04000000
#define PD_TRACE_COMPONENT_QGM     0x08000000

#define PD_TRACE_DECLARE_FUNCTION(x,y) \

enum _pdTraceFormatType
{
   PD_TRACE_FORMAT_TYPE_FLOW = 0,
   PD_TRACE_FORMAT_TYPE_FORMAT,
   PD_TRACE_FORMAT_TYPE_FLOW_PREPARE
} ;

enum _pdTraceArgumentType
{
   PD_TRACE_ARGTYPE_NONE = 0,
   PD_TRACE_ARGTYPE_NULL,
   PD_TRACE_ARGTYPE_CHAR,
   PD_TRACE_ARGTYPE_BYTE,
   PD_TRACE_ARGTYPE_SHORT,
   PD_TRACE_ARGTYPE_USHORT,
   PD_TRACE_ARGTYPE_INT,
   PD_TRACE_ARGTYPE_UINT,
   PD_TRACE_ARGTYPE_LONG,
   PD_TRACE_ARGTYPE_ULONG,
   PD_TRACE_ARGTYPE_FLOAT,
   PD_TRACE_ARGTYPE_DOUBLE,
   PD_TRACE_ARGTYPE_STRING,
   PD_TRACE_ARGTYPE_RAW
} ;

class _pdTraceArgument : public SDBObject
{
public :
   _pdTraceArgumentType _argumentType ;
   UINT32               _argumentSize ;
} ;
typedef class _pdTraceArgument pdTraceArgument ;

#define PD_TRACE_RECORD_FLAG_NORMAL 0
#define PD_TRACE_RECORD_FLAG_ENTRY  1
#define PD_TRACE_RECORD_FLAG_EXIT   2

class _pdTraceRecord : public SDBObject
{
public :
   CHAR          _eyeCatcher [ TRACE_EYE_CATCHER_SIZE ] ;
   UINT32        _recordSize ;
   UINT32        _functionID ;
   UINT32        _flag ;
   UINT32        _tid ;
   UINT32        _line ;
   UINT32        _numArgs ;
   ossTimestamp  _timestamp ;
} ;
typedef class _pdTraceRecord pdTraceRecord ;

namespace engine
{
   class _pmdEDUCB ;
}
class _pdTraceCB : public SDBObject
{
public :
   CHAR          _eyeCatcher [ TRACE_EYE_CATCHER_SIZE ] ;
   UINT32        _headerSize ;
   ossAtomic32   _traceStarted ;
   ossAtomic64   _currentSlot ;
   ossAtomic32   _currentWriter ;
   UINT32        _componentMask ;
   UINT32        _totalChunks ;
   UINT32        _totalSlots ;
   CHAR         *_pBuffer ;

#if defined (SDB_ENGINE)
   UINT32   _numBP ;
   UINT64   _bpList [ PD_TRACE_MAX_BP_NUM ] ;
   ossSpinXLatch          _pmdEDUCBLatch ;
   std::list<engine::_pmdEDUCB*>  _pmdEDUCBList ;
#endif

   void         *reserveOneSlot () ;
   void         *reserveSlots ( UINT32 numSlots ) ;
   void         *fillIn ( void *pPos, const void *pInput, INT32 size ) ;
   void          startWrite () ;
   void          finishWrite () ;
   void          setMask ( UINT32 mask ) ;
   UINT32        getMask () ;
   UINT32        getSlotNum () ;
   UINT32        getChunkNum () ;
   INT32         start ( UINT64 size, UINT32 mask,
                         std::vector<UINT64> *funcCode ) ;
   INT32         start ( UINT64 size, UINT32 mask ) ;
   INT32         start ( UINT64 size ) ; // size for trace buffer size on bytes
   void          stop () ; // stop trace but keep memory available
   static INT32  format ( const CHAR *pInputFileName,
                          const CHAR *pOutputFileName,
                          _pdTraceFormatType type ) ;
   INT32         dump ( const CHAR *pFileName ) ;
   void          destroy () ; // stop trace and destroy memory
   void          reset () ;
#if defined (SDB_ENGINE)
   INT32 addBreakPoint( UINT64 functionCode );
   void removeAllBreakPoint();
   void addPausedEDU ( engine::_pmdEDUCB *cb ) ;
   void resumePausedEDUs () ;
   void pause ( UINT64 funcCode ) ;
   const UINT64 *getBPList ()
   {
      return _bpList ;
   }
   INT32 getBPNum ()
   {
      return _numBP ;
   }
#endif // SDB_ENGINE
   _pdTraceCB () ;
   ~_pdTraceCB () ;

} ;
typedef class _pdTraceCB pdTraceCB ;

struct _pdTraceArgTuple
{
   _pdTraceArgumentType x ;
   const void *y ;
   INT32 z ;
   _pdTraceArgTuple ( _pdTraceArgumentType a,
                      const void *b, INT32 c )
   {
      x = a ;
      y = b ;
      z = c+sizeof(pdTraceArgument) ;
   }
   _pdTraceArgTuple () {}
   _pdTraceArgTuple &operator=(const _pdTraceArgTuple &right)
   {
      this->x = right.x ;
      this->y = right.y ;
      this->z = right.z ;
      return *this ;
   }
} ;
typedef struct _pdTraceArgTuple pdTraceArgTuple ;

#define PD_PACK_NONE      _pdTraceArgTuple ( PD_TRACE_ARGTYPE_NONE, NULL, 0 )
#define PD_PACK_NULL      _pdTraceArgTuple ( PD_TRACE_ARGTYPE_NULL, NULL, 0 )
#define PD_PACK_CHAR(x)   _pdTraceArgTuple ( PD_TRACE_ARGTYPE_CHAR, &x, 1 )
#define PD_PACK_BYTE(x)   _pdTraceArgTuple ( PD_TRACE_ARGTYPE_BYTE, &x, 1 )
#define PD_PACK_SHORT(x)  _pdTraceArgTuple ( PD_TRACE_ARGTYPE_SHORT, &x, 2 )
#define PD_PACK_USHORT(x) _pdTraceArgTuple ( PD_TRACE_ARGTYPE_USHORT, &x, 2 )
#define PD_PACK_INT(x)    _pdTraceArgTuple ( PD_TRACE_ARGTYPE_INT, &x, 4 )
#define PD_PACK_UINT(x)   _pdTraceArgTuple ( PD_TRACE_ARGTYPE_UINT, &x, 4 )
#define PD_PACK_LONG(x)   _pdTraceArgTuple ( PD_TRACE_ARGTYPE_LONG, &x, 8 )
#define PD_PACK_ULONG(x)  _pdTraceArgTuple ( PD_TRACE_ARGTYPE_ULONG, &x, 8 )
#define PD_PACK_FLOAT(x)  _pdTraceArgTuple ( PD_TRACE_ARGTYPE_FLOAT, &x, 4 )
#define PD_PACK_DOUBLE(x) _pdTraceArgTuple ( PD_TRACE_ARGTYPE_DOUBLE, &x, 8 )
#define PD_PACK_STRING(x) _pdTraceArgTuple ( PD_TRACE_ARGTYPE_STRING, x, ossStrlen(x)+1 )
#define PD_PACK_RAW(x,y)  _pdTraceArgTuple ( PD_TRACE_ARGTYPE_RAW, x, y )

#define PD_TRACE_MAX_ARG_NUM 9

#define PD_TRACE_ENTRY(funcCode)                                    \
   do {                                                             \
      if ( sdbGetPDTraceCB()->_traceStarted.compare(TRUE) )  \
      {                                                             \
         pdTraceArgTuple argTuple[PD_TRACE_MAX_ARG_NUM] ;           \
         ossMemset ( &argTuple[0], 0, sizeof(argTuple) ) ;          \
         pdTraceFunc ( funcCode, PD_TRACE_RECORD_FLAG_ENTRY,        \
                       __FILE__, __LINE__, &argTuple[0] ) ;         \
      }                                                             \
   } while ( FALSE )

#define PD_TRACE_EXIT(funcCode)                                     \
   do {                                                             \
      if ( sdbGetPDTraceCB()->_traceStarted.compare(TRUE) )  \
      {                                                             \
         pdTraceArgTuple argTuple[PD_TRACE_MAX_ARG_NUM] ;           \
         ossMemset ( &argTuple[0], 0, sizeof(argTuple) ) ;          \
         pdTraceFunc ( funcCode, PD_TRACE_RECORD_FLAG_EXIT,         \
                       __FILE__, __LINE__, &argTuple[0] ) ;         \
      }                                                             \
   } while ( FALSE )

#define PD_TRACE_EXITRC(funcCode,rc)                                \
   do {                                                             \
      if ( sdbGetPDTraceCB()->_traceStarted.compare(TRUE) )  \
      {                                                             \
         pdTraceArgTuple argTuple[PD_TRACE_MAX_ARG_NUM] ;           \
         ossMemset ( &argTuple[0], 0, sizeof(argTuple) ) ;          \
         argTuple[0] = PD_PACK_INT(rc) ;                            \
         pdTraceFunc ( funcCode, PD_TRACE_RECORD_FLAG_EXIT,         \
                       __FILE__, __LINE__, &argTuple[0] ) ;         \
      }                                                             \
   } while ( FALSE )

#define PD_TRACE_FUNC(funcCode,file,line,pack0,pack1,pack2,pack3,pack4,pack5,pack6,pack7,pack8) \
   do {                                                             \
      if ( sdbGetPDTraceCB()->_traceStarted.compare(TRUE) )  \
      {                                                             \
         pdTraceArgTuple argTuple[PD_TRACE_MAX_ARG_NUM] ;           \
         argTuple[0] = pack0 ;                                      \
         argTuple[1] = pack1 ;                                      \
         argTuple[2] = pack2 ;                                      \
         argTuple[3] = pack3 ;                                      \
         argTuple[4] = pack4 ;                                      \
         argTuple[5] = pack5 ;                                      \
         argTuple[6] = pack6 ;                                      \
         argTuple[7] = pack7 ;                                      \
         argTuple[8] = pack8 ;                                      \
         pdTraceFunc ( funcCode, PD_TRACE_RECORD_FLAG_NORMAL,       \
                       file, line,                                  \
                       &argTuple[0] ) ;                             \
      }                                                             \
   } while ( FALSE )

#define PD_TRACE0(funcCode)                               \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE1(funcCode,pack0)                         \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE2(funcCode,pack0,pack1)                   \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      pack1,                              \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE3(funcCode,pack0,pack1,pack2)             \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      pack1,                              \
                      pack2,                              \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE4(funcCode,pack0,pack1,pack2,pack3)       \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      pack1,                              \
                      pack2,                              \
                      pack3,                              \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE5(funcCode,pack0,pack1,pack2,pack3,pack4) \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      pack1,                              \
                      pack2,                              \
                      pack3,                              \
                      pack4,                              \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE6(funcCode,pack0,pack1,pack2,pack3,pack4,pack5) \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      pack1,                              \
                      pack2,                              \
                      pack3,                              \
                      pack4,                              \
                      pack5,                              \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE7(funcCode,pack0,pack1,pack2,pack3,pack4,pack5,pack6) \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      pack1,                              \
                      pack2,                              \
                      pack3,                              \
                      pack4,                              \
                      pack5,                              \
                      pack6,                              \
                      PD_PACK_NONE,                       \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE8(funcCode,pack0,pack1,pack2,pack3,pack4,pack5,pack6,pack7) \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      pack1,                              \
                      pack2,                              \
                      pack3,                              \
                      pack4,                              \
                      pack5,                              \
                      pack6,                              \
                      pack7,                              \
                      PD_PACK_NONE ) ;                    \
   } while ( FALSE )

#define PD_TRACE9(funcCode,pack0,pack1,pack2,pack3,pack4,pack5,pack6,pack7,pack8) \
   do {                                                   \
      PD_TRACE_FUNC ( funcCode, __FILE__, __LINE__,       \
                      pack0,                              \
                      pack1,                              \
                      pack2,                              \
                      pack3,                              \
                      pack4,                              \
                      pack5,                              \
                      pack6,                              \
                      pack7,                              \
                      pack8 ) ;                           \
   } while ( FALSE )

const CHAR *pdGetTraceFunction ( UINT64 id ) ;
const CHAR *pdGetTraceComponent ( UINT32 id ) ;
void pdTraceFunc ( UINT64 funcCode, INT32 type,
                   const CHAR* file, UINT32 line,
                   pdTraceArgTuple *tuple ) ;

/*
   get global pdtrace cb
*/
pdTraceCB* sdbGetPDTraceCB () ;

#endif // PDTRACE_HPP__
