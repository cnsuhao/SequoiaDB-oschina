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

   Source File Name = ossLatch.hpp

   Descriptive Name = Operating System Services Latch Header

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains declares for latch operations.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef OSS_SPINLOCK_HPP_
#define OSS_SPINLOCK_HPP_

#include "core.hpp"
#include <boost/thread/shared_mutex.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include "oss.hpp"
#include "pd.hpp"
#if defined (_WINDOWS)
#include <WinBase.h>
#include <boost/thread/mutex.hpp>
#else
#include <unistd.h>
#include <pthread.h>
#endif

#include "ossAtomicBase.hpp"
typedef SINT32 ossLockType ;
typedef volatile ossLockType ossLock ;
#define OSS_LOCK_LOCKED   1
#define OSS_LOCK_UNLOCKED 0

#define ossLockPeek(pLock) *(volatile const ossLock *)pLock
/*
static OSS_INLINE ossLockType ossLockPeek( volatile const ossLock * const pLock )
{
   return *pLock ;
}*/

#define ossLockGetStatus(pLock) ossAtomicFetch32( ( volatile SINT32 * )pLock )
/*
static OSS_INLINE ossLockType ossLockGetStatus(volatile const ossLock * const pLock)
{
   return ossAtomicFetch32( ( volatile SINT32 * )pLock ) ;
}*/

#define ossLockTestGet(pLock) ossCompareAndSwap32( pLock, OSS_LOCK_UNLOCKED, \
                                                   OSS_LOCK_LOCKED )
/*
static OSS_INLINE BOOLEAN ossLockTestGet( volatile ossLock * const  pLock )
{
   return ( ossCompareAndSwap32( pLock, OSS_LOCK_UNLOCKED, OSS_LOCK_LOCKED ) ) ;
}*/

#define ossLockInit(pLock) *(volatile ossLock *)pLock=OSS_LOCK_UNLOCKED
/*
static OSS_INLINE void ossLockInit( volatile ossLock * const pLock )
{
   *pLock = OSS_LOCK_UNLOCKED ;
}*/

#define ossLockRelease(pLock) \
        {\
           ossCompilerFence() ;\
           ossAtomicExchange32((volatile ossLock *)pLock, OSS_LOCK_UNLOCKED );\
        }
/*
static OSS_INLINE void ossLockRelease( volatile ossLock * const pLock )
{
   ossCompilerFence() ;
   ossAtomicExchange32( pLock, OSS_LOCK_UNLOCKED ) ;
}*/

#define ossWait(x) \
        {\
           UINT32 __i = 0 ;\
           do\
           {\
              ossYield() ;\
              __i++;\
           }while(__i<x);\
        }
/*
static OSS_INLINE void ossWait( UINT32 x )
{
   UINT32 i = 0 ;
   do
   {
      ossYield() ;
      i++ ;
   } while ( i < x ) ;
}*/

#define ossLockGet(pLock) \
        {\
           while(!ossLockTestGet((volatile ossLock*)pLock))\
           {\
              ossWait( 1 );\
           }\
        }
/*
static OSS_INLINE void ossLockGet( volatile ossLock * const pLock )
{
   while( ! ossLockTestGet( pLock ) )
   {
   }
}*/

#if defined (_LINUX)
static OSS_INLINE void ossLockGet8 ( volatile CHAR * const pLock )
{
   if ( !*pLock && !ossAtomicExchange8 ( pLock, OSS_LOCK_LOCKED ) )
      return ;
   for ( int i=0; i<1000; i++ )
   {
      if ( !ossAtomicExchange8 ( pLock, OSS_LOCK_LOCKED ) )
         return ;
      ossYield () ;
   }
   while ( ossAtomicExchange8 ( pLock, OSS_LOCK_LOCKED ) )
      usleep ( 1000 ) ;
}

static OSS_INLINE void ossLockRelease8 ( volatile CHAR * const pLock )
{
   ossCompilerFence() ;
   ossAtomicExchange8( pLock, OSS_LOCK_UNLOCKED ) ;
}
#endif
/*
 * Interface for read/write lock
 */
class ossSLatch : public SDBObject
{
public:
   virtual ~ossSLatch (){} ;
   /*
    * Exclusive lock
    * Wait until lock is get
    */
   virtual void get () = 0 ;
   /*
    * Unlock exclusive lock
    */
   virtual void release () = 0 ;
   /*
    * shared lock
    * Wait until lock is get
    */
   virtual void get_shared () = 0 ;
   /*
    * unlock shared lock
    */
   virtual void release_shared () = 0 ;
   /*
    * get exclusive lock if possible
    * return TRUE if exc lock is get
    * return FALSE if it can't get
    */
   virtual BOOLEAN try_get () = 0 ;
   /*
    * get shared lock if possible
    * return TRUE if shared lock is get
    * return FALSE if it can't get
    */
   virtual BOOLEAN try_get_shared () = 0 ;
} ;

/*
 * Interface for simple (exclusive) lock
 */
class ossXLatch : public SDBObject
{
public :
   virtual ~ossXLatch (){} ;
   /*
    * lock
    * Wait until lock is get
    */
   virtual void get ()= 0 ;
   /*
    * Unlock lock
    */
   virtual void release () = 0 ;
   /*
    * get lock if possible
    * return TRUE if lock is get
    * return FALSE if it can't get
    */
   virtual BOOLEAN try_get() = 0 ;
} ;

class _ossAtomicXLatch : public ossXLatch
{
private :
   ossLock lock ;
public :
   _ossAtomicXLatch ()
   {
      ossLockInit( &lock ) ;
   }
   ~_ossAtomicXLatch ()
   {
   }

   BOOLEAN try_get ()
   {
      return ( ossLockTestGet( &lock ) ) ;
   }

   void get()
   {
         ossLockGet( &lock ) ;
   }

   void release()
   {
      ossLockRelease( &lock ) ;
   }
} ;
typedef class _ossAtomicXLatch ossAtomicXLatch ;

class _ossSpinXLatch : public ossXLatch
{
#if defined(_WIN32)
private :
   boost::mutex _lock ;
public:
   _ossSpinXLatch ()
   {
   }
   ~_ossSpinXLatch ()
   {
   }
   void get ()
   {
      _lock.lock () ;
   }
   void release ()
   {
      _lock.unlock () ;
   }
   BOOLEAN try_get ()
   {
      return (BOOLEAN) _lock.try_lock () ;
   }
/*#elif defined (__USE_XOPEN2K)
private :
   pthread_spinlock_t _lock ;
public :
   _ossSpinXLatch ()
   {
      pthread_spin_init ( &_lock , 0 ) ;
   }
   ~_ossSpinXLatch ()
   {
      pthread_spin_destroy ( &_lock ) ;
   }
   void get()
   {
      if ( pthread_spin_trylock ( &_lock ) == 0 )
         return ;
      for ( int i=0; i<1000; i++ )
      {
         if ( pthread_spin_trylock ( &_lock ) == 0 )
            return ;
         ossYield() ;
      }
      for ( int i=0; i<1000; i++ )
      {
         if ( pthread_spin_trylock ( &_lock ) == 0 )
            return ;
         pthread_yield () ;
      }
      while ( pthread_spin_trylock( &_lock ) != 0 )
         usleep ( 1000 ) ;
   }
   void release ()
   {
      pthread_spin_unlock ( &_lock ) ;
   }
   BOOLEAN try_get ()
   {
      return pthread_spin_trylock ( &_lock ) == 0 ;
   }
#elif  defined (__GCC_HAVE_SYNC_COMPARE_AND_SWAP_4)
private :
   volatile int _lock;
public :
   _ossSpinXLatch ()
   {
      _lock = 0 ;
   }
   ~_ossSpinXLatch (){}
   void get ()
   {
      if( !_lock && !__sync_lock_test_and_set ( &_lock, 1 ) )
         return ;
      for ( int i=0; i<1000; i++ )
      {
         if ( !__sync_lock_test_and_set ( &_lock, 1 ) )
            return ;
         ossYield () ;
      }
      while ( __sync_lock_test_and_set(&_lock, 1) )
         usleep ( 1000 ) ;
   }
   void release()
   {
      __sync_lock_release ( &_lock ) ;
   }
   BOOLEAN try_get()
   {
      return ( !_lock && !__sync_lock_test_and_set ( &_lock, 1 ) ) ;
   }*/
#else
private :
   pthread_mutex_t _lock ;
public :
   _ossSpinXLatch ()
   {
      SDB_ASSERT ( pthread_mutex_init ( &_lock, 0 ) == 0,
                   "Failed to init mutex" ) ;
   }
   ~_ossSpinXLatch()
   {
      SDB_ASSERT ( pthread_mutex_destroy ( &_lock ) == 0,
                   "Failed to destroy mutex" ) ;
   }
   void get ()
   {
      SDB_ASSERT ( pthread_mutex_lock ( &_lock ) == 0,
                   "Failed to lock mutex" ) ;
   }
   void release ()
   {
      SDB_ASSERT ( pthread_mutex_unlock ( &_lock ) == 0,
                   "Failed to unlock mutex" ) ;
   }
   BOOLEAN try_get ()
   {
      return ( pthread_mutex_trylock ( &_lock ) == 0 ) ;
   }
#endif
} ;
typedef class _ossSpinXLatch ossSpinXLatch ;

class _ossSpinSLatch : public ossSLatch
{
#if defined (_WINDOWS) && defined (USE_SRW)
private :
   SRWLOCK _lock ;
public:
   _ossSpinSLatch ()
   {
      InitializeSRWLock ( &_lock ) ;
   }
   ~_ossSpinSLatch() {} ;
   void get ()
   {
      AcquireSRWLockExclusive ( &_lock ) ;
   }
   void release ()
   {
      ReleaseSRWLockExclusive ( &_lock ) ;
   }
   void get_shared()
   {
      AcquireSRWLockShared ( &_lock ) ;
   }
   void release_shared ()
   {
      ReleaseSRWLockShared ( &_lock ) ;
   }
   BOOLEAN try_lock_shared ()
   {
      return TryAcquireSRWLockShared ( &_lock ) ;
   }
   BOOLEAN try_lock ()
   {
      return TryAcquireSRWLockExclusive ( &_lock ) ;
   }
#else
private :
   boost::shared_mutex _m;
public :
   void get ()
   {
      try
      {
         _m.lock() ;
      }
      catch(...)
      {
         SDB_ASSERT ( FALSE, "SLatch get failed" ) ;
      }
   }
   void release ()
   {
      try
      {
         _m.unlock() ;
      }
      catch(...)
      {
         SDB_ASSERT ( FALSE, "SLatch release failed" ) ;
      }
   }
   void get_shared ()
   {
      try
      {
         _m.lock_shared () ;
      }
      catch(...)
      {
         SDB_ASSERT ( FALSE, "SLatch get shared failed" ) ;
      }
   }
   void release_shared ()
   {
      try
      {
         _m.unlock_shared() ;
      }
      catch(...)
      {
         SDB_ASSERT ( FALSE, "SLatch release shared failed" ) ;
      }
   }
   BOOLEAN try_get ()
   {
      try
      {
         return _m.timed_lock ( boost::posix_time::milliseconds ( 0 ) ) ;
      }
      catch(...)
      {
         SDB_ASSERT ( FALSE, "SLatch try get failed" ) ;
      }
      return FALSE ;
   }
   BOOLEAN try_get_shared()
   {
      try
      {
         return _m.timed_lock_shared ( boost::posix_time::milliseconds ( 0 ) ) ;
      }
      catch(...)
      {
         SDB_ASSERT ( FALSE, "SLatch try get shared failed" ) ;
      }
      return FALSE ;
   }
#endif
} ;
typedef class _ossSpinSLatch ossSpinSLatch ;

enum OSS_LATCH_MODE
{
   SHARED ,
   EXCLUSIVE
} ;
void ossLatch ( ossSLatch *latch, OSS_LATCH_MODE mode ) ;
void ossLatch ( ossXLatch *latch ) ;
void ossUnlatch ( ossSLatch *latch, OSS_LATCH_MODE mode ) ;
void ossUnlatch ( ossXLatch *latch ) ;
BOOLEAN ossTestAndLatch ( ossSLatch *latch, OSS_LATCH_MODE mode ) ;
BOOLEAN ossTestAndLatch ( ossXLatch *latch ) ;

class _ossScopedLock
{
private :
   ossSLatch *_slatch ;
   ossXLatch *_xlatch ;
   OSS_LATCH_MODE _mode ;
public :
   _ossScopedLock ( ossSLatch *latch ) :
         _slatch ( NULL ), _xlatch ( NULL ), _mode ( EXCLUSIVE )
   {
      if ( latch )
      {
         _slatch = latch ;
         _mode = EXCLUSIVE ;
         _xlatch = NULL ;
         _slatch->get () ;
      }
   }
   _ossScopedLock ( ossSLatch *latch, OSS_LATCH_MODE mode) :
         _slatch ( NULL ), _xlatch ( NULL ), _mode ( EXCLUSIVE )
   {
      if ( latch )
      {
         _slatch = latch ;
         _mode = mode ;
         _xlatch = NULL ;
         if ( mode == EXCLUSIVE )
            _slatch->get () ;
         else
            _slatch->get_shared () ;
      }
   }
   _ossScopedLock ( ossXLatch *latch ) :
         _slatch ( NULL ), _xlatch ( NULL ), _mode ( EXCLUSIVE )
   {
      if ( latch )
      {
         _xlatch = latch ;
         _slatch = NULL ;
         _xlatch->get () ;
      }
   }
   ~_ossScopedLock ()
   {
      if ( _slatch )
         ( _mode == EXCLUSIVE ) ? _slatch->release() :
                                  _slatch->release_shared() ;
      else if ( _xlatch )
         _xlatch->release () ;
   }
} ;
typedef class _ossScopedLock ossScopedLock;
#endif
