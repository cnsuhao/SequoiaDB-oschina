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
#ifndef PD_HPP_
#define PD_HPP_

#include "core.h"
#include "oss.h"
#include <string>
#include <stdlib.h>

#define PD_DFT_FILE_NUM             (20)
#define PD_DFT_FILE_SZ              (100)
#define PD_DFT_DIAGLOG              "sdbdiag.log"

#define PD_LOG_STRINGMAX            ( 4096 )

#ifdef _DEBUG
#define SDB_ASSERT(cond,str)  \
   do { \
      if( !(cond) ) { pdassert(str,__FUNC__,__FILE__,__LINE__) ; } \
      } while ( 0 )
#else
#define SDB_ASSERT(cond,str)  do{ if( !(cond)) {} } while ( 0 )
#endif // _DEBUG

#define SDB_VALIDATE_GOTOERROR(cond, ret, str) \
   do { \
      if( !(cond) ) { \
         pdLog(PDERROR, __FUNC__, __FILE__, __LINE__, str) ; \
         rc=ret ; \
         goto error ; \
         } \
      } while ( 0 )

#define PD_LOG(level, fmt, ...) \
   do { \
      if ( getPDLevel() >= level ) \
      { \
         pdLog(level, __FUNC__, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
      } \
   }while (0)

#define PD_LOG_MSG(level, fmt, ...) \
   do { \
      if ( level <= PDERROR ) \
      { \
         _pmdEDUCB *cb = pmdGetThreadEDUCB() ; \
         if ( cb ) \
         { \
            cb->printInfo ( EDU_INFO_ERROR, fmt, ##__VA_ARGS__ ) ; \
         } \
      } \
      if ( getPDLevel() >= level ) \
      { \
         pdLog(level, __FUNC__, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
      } \
   } while ( 0 )

#define PD_CHECK(cond, retCode, gotoLabel, level, fmt, ...) \
   do {                                                     \
      if ( !(cond) )                                        \
      {                                                     \
         rc = (retCode) ;                                   \
         PD_LOG ( (level), fmt, ##__VA_ARGS__) ;            \
         goto gotoLabel ;                                   \
      }                                                     \
   } while ( 0 )                                            \


#define PD_RC_CHECK(rc, level, fmt, ...)                    \
   do {                                                     \
      PD_CHECK ( (SDB_OK == (rc)), (rc), error, (level),    \
                 fmt, ##__VA_ARGS__) ;                      \
   } while ( 0 )                                            \


enum PDLEVEL
{
   PDSEVERE = 0,
   PDERROR,
   PDEVENT,
   PDWARNING,
   PDINFO,
   PDDEBUG
} ;

PDLEVEL& getPDLevel() ;
PDLEVEL  setPDLevel( PDLEVEL newLevel ) ;
const CHAR* getDialogName() ;
const CHAR* getDialogPath() ;
const CHAR* getPDLevelDesp ( PDLEVEL level ) ;

void sdbEnablePD( const CHAR *pdPathOrFile,
                  INT32 fileMaxNum = PD_DFT_FILE_NUM,
                  UINT32 fileMaxSize = PD_DFT_FILE_SZ ) ;
void sdbDisablePD() ;

BOOLEAN sdbIsPDEnabled() ;


#ifdef _DEBUG
void pdassert( const CHAR* string, const CHAR* func,
               const CHAR* file, UINT32 line) ;
void pdcheck( const CHAR* string, const CHAR* func,
              const CHAR* file, UINT32 line) ;
#else
#define pdassert(str1,str2,str3,str4)
#define pdcheck(str1,str2,str3,str4)
#endif

/*
   _pdGeneralException define
*/
struct _pdGeneralException : public std::exception
{
   std::string s ;
   _pdGeneralException ( std::string ss ) : s (ss) {}
   ~_pdGeneralException() throw() {}
   const CHAR *what() const throw()
   {
      return s.c_str() ;
   }
} ;
typedef struct _pdGeneralException pdGeneralException ;

/*
   pdLog function define
*/
void pdLog( PDLEVEL level, const CHAR* func, const CHAR* file,
            UINT32 line, const CHAR* format, ...) ;

void pdLog( PDLEVEL level, const CHAR* func, const CHAR* file,
            UINT32 line, std::string message ) ;

#endif // PD_HPP_

