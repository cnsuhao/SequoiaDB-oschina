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

   Source File Name = ossPath.h

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

#ifndef OSS_PATH_H
#define OSS_PATH_H

#include "ossTypes.h"

#if defined (_WINDOWS)
#define OSS_PATH_SEP_CHAR '\\'
#define OSS_PATH_SEP_SET "\\"
#else
#define OSS_PATH_SEP_CHAR '/'
#define OSS_PATH_SEP_SET "/"
#endif

/**
 * locate an executable by providing the path of another executable in the
 * same directory and its own name.
 * On Windows, refPath=C:\mypath\a.exe, exeName=b.exe, then path will contain
 * C:\mypath\b.exe
 * On Linux, refPath=/home/a, exeName=b, then path will contain /home/b
 */
INT32 ossLocateExecutable ( const CHAR * refPath ,
                            const CHAR * exeName ,
                            CHAR * buf ,
                            UINT32 bufSize ) ;
#endif // OSS_PATH_H

