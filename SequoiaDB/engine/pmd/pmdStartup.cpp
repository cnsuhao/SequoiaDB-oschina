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

   Source File Name = pmdStartup.cpp

   Descriptive Name = Data Management Service Header

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          26/12/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdStartup.hpp"
#include "ossUtil.hpp"
#include "pdTrace.hpp"
#include "pmdTrace.hpp"

namespace engine
{

   #define PMD_STARTUP_START_CHAR         "SEQUOIADB:STARTUP"
   #define PMD_STARTUP_STOP_CHAR          "SEQUOIADB:STOPOFF"
   #define PMD_STARTUP_START_CHAR_LEN     ossStrlen(PMD_STARTUP_START_CHAR)
   #define PMD_STARTUP_STOP_CHAR_LEN      ossStrlen(PMD_STARTUP_STOP_CHAR)

   _pmdStartup::_pmdStartup () :
   _ok(FALSE),
   _startType(SDB_START_NORMAL),
   _fileOpened ( FALSE ),
   _fileLocked ( FALSE )
   {
   }

   _pmdStartup::~_pmdStartup ()
   {
   }

   void _pmdStartup::ok ( BOOLEAN bOK )
   {
      _ok = bOK ;
   }

   BOOLEAN _pmdStartup::isOK () const
   {
      return _ok ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDSTARTUP_INIT, "_pmdStartup::init" )
   INT32 _pmdStartup::init ( const CHAR *pPath, BOOLEAN onlyCheck )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__PMDSTARTUP_INIT );
      _fileOpened = FALSE ;
      SINT64 written = 0 ;
      UINT32 mode = OSS_READWRITE ;
      const UINT32 maxTryLockTime = 2 ;
      UINT32 lockTime = 0 ;

      _fileName = pPath ;
      _fileName += OSS_FILE_SEP ;
      _fileName += PMD_STARTUP_FILE_NAME ;

      PD_TRACE1 ( SDB__PMDSTARTUP_INIT, PD_PACK_STRING ( _fileName.c_str() ) ) ;
      rc = ossAccess ( _fileName.c_str() ) ;
      if ( SDB_FNE == rc )
      {
         _startType = SDB_START_NORMAL ;
         _ok = TRUE ;
         mode |= OSS_REPLACE ;
         rc = SDB_OK ;

         if ( onlyCheck )
         {
            goto done ;
         }
      }
      else if ( SDB_PERM == rc )
      {
         PD_LOG ( PDSEVERE, "Permission denied when creating startup file" ) ;
         goto error ;
      }
      else if ( rc )
      {
         PD_LOG ( PDSEVERE, "Failed to access startup file, rc = %d", rc ) ;
         goto error ;
      }
      else
      {
         _ok = FALSE ;
      }

      rc = ossOpen ( _fileName.c_str(), mode, OSS_RU|OSS_WU|OSS_RG, _file ) ;
      if ( SDB_OK != rc )
      {
#if defined (_WINDOWS)
         if ( SDB_PERM == rc )
         {
            PD_LOG ( PDERROR, "Failed to open startup file due to perm "
                     "error, please check if the directory is granted "
                     "with the right permission, or if there is another "
                     "instance is running with the directory" ) ;
         }
         else
#endif
         {
            PD_LOG ( PDERROR, "Failed to create startup file, rc = %d", rc ) ;
         }
         goto error ;
      }
      _fileOpened = TRUE ;

   retry:
      rc = ossLockFile ( &_file, OSS_LOCK_EX ) ;
      if ( SDB_PERM == rc )
      {
         if ( onlyCheck )
         {
            rc = SDB_OK ;
            _startType = SDB_START_NORMAL ;
            goto done ;
         }

         if ( lockTime++ < maxTryLockTime )
         {
            goto retry ;
         }
         PD_LOG ( PDERROR, "The startup file is already locked, most likely "
                  "there is another instance running in the directory" ) ;
         goto error ;
      }
      else if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to lock startup file, rc = %d", rc ) ;
         goto error ;
      }
      _fileLocked = TRUE ;

      if ( !_ok )
      {
         CHAR text[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
         rc = ossSeekAndRead( &_file, 0, text, PMD_STARTUP_STOP_CHAR_LEN,
                              &written ) ;
         if ( SDB_OK == rc && 0 == ossStrncmp( text ,
              PMD_STARTUP_STOP_CHAR, PMD_STARTUP_STOP_CHAR_LEN ) )
         {
            _startType = SDB_START_NORMAL ;
         }
         else
         {
            _startType = SDB_START_CRASH ;
         }
         PD_TRACE1 ( SDB__PMDSTARTUP_INIT,
                     PD_PACK_INT ( _startType ) ) ;
         rc = SDB_OK ;

         if ( onlyCheck )
         {
            goto done ;
         }
      }

      rc = ossSeekAndWrite ( &_file, 0, PMD_STARTUP_START_CHAR,
                             PMD_STARTUP_START_CHAR_LEN, &written ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      ossFsync( &_file ) ;

      if ( SDB_START_NORMAL != _startType )
      {
         PD_LOG ( PDEVENT, "Start up from crash" ) ;
      }
      else
      {
         PD_LOG ( PDEVENT, "Start up from normal" ) ;
      }

   done:
      if ( onlyCheck && _fileOpened )
      {
         if ( _fileLocked )
         {
            ossLockFile ( &_file, OSS_LOCK_UN ) ;
            _fileLocked = FALSE ;
         }
         ossClose( _file ) ;
         _fileOpened = FALSE ;
      }
      PD_TRACE_EXITRC ( SDB__PMDSTARTUP_INIT, rc );
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__PMDSTARTUP_FINAL, "_pmdStartup::final" )
   INT32 _pmdStartup::final ()
   {
      PD_TRACE_ENTRY ( SDB__PMDSTARTUP_FINAL ) ;
      if ( _fileOpened && _fileLocked )
      {
         INT64 write = 0 ;
         INT32 rc = ossSeekAndWrite( &_file, 0, PMD_STARTUP_STOP_CHAR,
                                     PMD_STARTUP_STOP_CHAR_LEN, &write ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to write startup file stop char, rc: %d",
                    rc ) ;
         }
      }
      if ( _fileLocked )
      {
         ossLockFile ( &_file, OSS_LOCK_UN ) ;
      }
      if ( _fileOpened )
      {
         ossClose( _file ) ;
      }
      if ( _ok )
      {
         ossDelete ( _fileName.c_str() ) ;
      }
      PD_TRACE_EXIT ( SDB__PMDSTARTUP_FINAL ) ;
      return SDB_OK ;
   }

   pmdStartup & pmdGetStartup ()
   {
      static pmdStartup _startUp ;
      return _startUp ;
   }

}


