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

*******************************************************************************/
/*
@description: check whether target host has installed valid db packet or not,
              then record the result to a file in /tmp/omatmp/tmp
@modify list:
   2015-1-6 Zhaobo Tan  Init
@parameter
   install_path[string]: the path of where we expect the sequoiadb programs in
@return void
*/

var FILE_NAME_ADD_HOST_PRE_CHECK = "addHostPreCheck.js" ;
var rc                 = SDB_OK ;
var errMsg             = "" ;

var result_file        = OMA_FILE_TEMP_ADD_HOST_CHECK ;
var expect_programs    = null ;

/* *****************************************************************************
@discretion: init
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _init()
{
   if ( SYS_LINUX == SYS_TYPE )
   {
      expect_programs = [ "sequoiadb", "sdb", "sdbcm", "sdbcmd", "sdbcmart", "sdbcmtop" ] ;
   }
   else
   {
      // TODO: windows
   }
   PD_LOG2( LOG_NONE, arguments, PDEVENT, FILE_NAME_ADD_HOST_PRE_CHECK,
            "Begin to check whether target host had been add before execute add host command" ) ;
}

/* *****************************************************************************
@discretion: final
@author: Tanzhaobo
@parameter void
@return void
***************************************************************************** */
function _final()
{
   PD_LOG2( LOG_NONE, arguments, PDEVENT, FILE_NAME_ADD_HOST_PRE_CHECK,
            "Finish checking whether target host had been add before execute add host command" ) ;
}

/* *****************************************************************************
@discretion: get installed db packet's md5 code
@author: Tanzhaobo
@parameter void
@return
   md5code[string]:
***************************************************************************** */
function _getMD5()
{
   var md5code = "" ;
   var obj = null ;

   if ( SYS_LINUX == SYS_TYPE )
   {
      try
      {
         if ( !File.exist( OMA_FILE_INSTALL_INFO ) )
            exception_handle( SDB_SYS, "Install db packet's result file not exist" ) ;
         // TODO: need to rename
         obj = eval( "(" + Oma.getOmaConfigs( OMA_FILE_INSTALL_INFO ) + ")" ) ;
         md5code = obj[MD5] ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         errMsg = sprintf( "Failed to get install packet's md5 in host[?]",
                           System.getHostName() ) ;
         rc = GETLASTERROR() ;
         PD_LOG2( LOG_NONE, arguments, PDERROR, FILE_NAME_ADD_HOST_PRE_CHECK,
                  sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
         exception_handle( rc, errMsg ) ;
      }
   }
   else
   {
      // TODO:
   }

   return md5code ;
}

/* *****************************************************************************
@discretion: judge whether sequoiadb programs exist or not
@author: Tanzhaobo
@parameter
@return
   [bool]: true for exist, false for not
***************************************************************************** */
function _isProgramExist()
{
   var path = "" ;
   var program_name = "" ;
   
   try
   {
      // check
      if ( SYS_LINUX == SYS_TYPE )
      {
         // get program's path
         path = adaptPath( install_path ) + 'bin/' ;
         for( var i =1; i < expect_programs.length; i++ )
         {
            program_name = path + expect_programs[i] ;
            if( !File.exist( program_name ) )
            {
               PD_LOG2( LOG_NONE, arguments, PDWARNING, FILE_NAME_ADD_HOST_PRE_CHECK,
                        sprintf( "Program [?] does not exist in target host, need to install db packet", program_name) ) ;
               return false ;
            }
         }

         return true ;
      }
      else
      {
         // TODO: windows
      }
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = "Failed to judge whether program exist or not" ;
      rc = GETLASTERROR() ;
      PD_LOG2( LOG_NONE, arguments, PDERROR, FILE_NAME_ADD_HOST_PRE_CHECK,
               sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;

      return false ;
   }
}

function main()
{
   _init() ;
   var str  = "" ;
   var md5  = "" ;
   var flag = false ;
   var file = null ;
   // get md5
   try
   {
      md5 = _getMD5() ; 
   }
   catch( e )
   {
      md5 = "" ;
   }
   // judge programs exist or not
   flag = _isProgramExist() ;

   // write the result to file
   try
   {
      if ( File.exist( result_file ) )
         File.remove( result_file ) ;
      file = new File( result_file ) ;
      str = MD5 + "=" + md5 + OMA_NEW_LINE ;
      file.write( str ) ;
      str = ISPROGRAMEXIST + "=" + flag ;
      file.write( str ) ;
      file.close() ;
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = sprintf( "Failed to write pre-check result to file[?] in host[?]",
                        result_file, System.getHostName() ) ;
      PD_LOG2( LOG_NONE, arguments, PDERROR, FILE_NAME_ADD_HOST_PRE_CHECK,
               sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
      exception_handle( rc, errMsg ) ;
   }
   
   _final() ;
}

// execute
   main() ;

