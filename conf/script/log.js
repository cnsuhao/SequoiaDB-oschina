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
@description: Log for om js files
@modify list:
   2015-1-8 Zhaobo Tan  Init
*/
var LOG_NEW_LINE      = "" ;

var LOG_FILE_PATH     = "" ;
var LOG_FILE_NAME     = "sdbcm_js.log" ;
var JS_LOG_FILE       = "" ;

var LOG_NONE          = -1 ;
var LOG_GENERIC       = -2 ;

var PDSEVERE          = 0 ;
var PDERROR           = 1 ;
var PDEVENT           = 2 ;
var PDWARNING         = 3 ;
var PDINFO            = 4 ;
var PDDEBUG           = 5 ;

var JS_LOG_LEVEL      = PDWARNING ;
var OS_TYPE_IN_JS_LOG = "LINUX" ;

try
{
   // get os type
   OS_TYPE_IN_JS_LOG = System.type() ;
   if ( undefined == OS_TYPE_IN_JS_LOG )
      OS_TYPE_IN_JS_LOG = "LINUX" ;
   // get diaglog level
   var obj = eval( '(' + Oma.getOmaConfigs() + ')' ) ;
   JS_LOG_LEVEL = obj["DiagLevel"] ;
   if ( undefined == JS_LOG_LEVEL )
      JS_LOG_LEVEL = PDWARNING ;
}
catch( e )
{
   OS_TYPE_IN_JS_LOG = "LINUX" ;
   JS_LOG_LEVEL = PDWARNING ;
}

if( "LINUX" == OS_TYPE_IN_JS_LOG )
{
   LOG_NEW_LINE  = "\n" ;
   LOG_FILE_PATH = "../web/log/" ;
   JS_LOG_FILE   = LOG_FILE_PATH + LOG_FILE_NAME ;
}
else
{
   LOG_NEW_LINE = "\r\n" ;
   LOG_FILE_PATH = "..\\web\\log\\" ;
   JS_LOG_FILE   = LOG_FILE_PATH + LOG_FILE_NAME ;
}

// get function name and line number
/* *****************************************************************************
@discretion: get the function name or line number when PD_LOG is invoked
@author: Tanzhaobo
@parameter
   type[string]: "line" for get line number, the others for get function name
@return the function name or line number of js file when PD_LOG is invoked
@deprecated: it's not a useful way to get function name or line number in js file
***************************************************************************** */
function _getFuncOrLine( type )
{
   var retStr = "" ;
   var stackArr = [] ;
   var str = "" ;
   var pos = -1 ;
   try
   {
      throw new Error( "Get function name or lineno" ) ;
   }
   catch( e )
   {
      try
      {
         stackArr = (e.stack).split( LOG_NEW_LINE ) ;
         if ( stackArr.length > 2 )
         {
            str = stackArr[2] ;
            if ( "line" == type )
            {
               try
               {
                  pos = str.lastIndexOf( ".js:" ) ;
                  if ( -1 != pos )
                  {
                     str = str.substr( pos + ".js:".length ) ;
                     //retStr = str ;
                     retStr = parseInt( str ) ;
                  }
               }
               catch( e )
               {}
            }
            else
            {
               try
               {
                  pos = str.indexOf("@") ;
                  if( -1 != pos )
                  {
                     var strArr = str.split( "@" ) ;
                     retStr = strArr[0] ;
                  }
               }
               catch( e )
               {}
            }
         }
      }
      catch( e )
      {
      }
   }
   return retStr ;
}

/* *****************************************************************************
@discretion: get the function name when PD_LOG is invoked
@author: Tanzhaobo
@parameter void
@return the function name of js file when PD_LOG is invoked
@deprecated: it's not a useful way to get function name in js file
***************************************************************************** */
function getFunc()
{
   return  _getFuncOrLine( "func" ) ;
}

/* *****************************************************************************
@discretion: get the line number when PD_LOG is invoked
@author: Tanzhaobo
@parameter void
@return the line number of js file when PD_LOG is invoked
@deprecated: it's not a useful way to get line number in js file
***************************************************************************** */
function getLine()
{
   return _getFuncOrLine( "line" ) - 1 ;
}

// format log information
/* *****************************************************************************
@discretion: generate some spacings
@author: Tanzhaobo
@parameter
   num[number]:
@return
   retStr[string]: a string made up by spacings
***************************************************************************** */
function _genSpace( num )
{
   var retStr = "" ;
   for ( var i = 0; i < num; i++ )
      retStr += " " ;
   return retStr ;
}

/* *****************************************************************************
@discretion: format one line of the log information 
@author: Tanzhaobo
@parameter
   inputStr[string]: one line of content to be formatted
   key[string]:
   indent[number]:
@return the formative log information
***************************************************************************** */
function _formatOneLine( inputStr, key, indent )
{
   var retStr = ( "string" == typeof(inputStr) ) ? inputStr : "" ;
   var index = inputStr.indexOf( key ) ;
   if ( -1 == index )
      return inputStr ;
   var arr = inputStr.split( key ) ;
   if ( indent > index )
   {
      var space = _genSpace( indent - index - 1 ) ;
      retStr = arr[0] + space + key + arr[1] + LOG_NEW_LINE ;
   }
   else
   {
      retStr = arr[0] + " " + key + arr[1] + LOG_NEW_LINE ; 
   }
   return retStr ;
} 

/* *****************************************************************************
@discretion: format the log information 
@author: Tanzhaobo
@parameter
   contentStr[string]: the content to be formatted
   keyArr[array]:
   indent[number]:
@return the formative log information
***************************************************************************** */
function _formatLogInfo( contentStr, keyArr, indent )
{
   var retStr = "" ;
   var i = 0 ;
   var arr = contentStr.split( LOG_NEW_LINE ) ;
   for( i = 0; i < keyArr.length; i++ )
      retStr += _formatOneLine( arr[i], keyArr[i], 42 ) ; 
   for( ; i < arr.length; i++ )
      retStr += arr[i] + LOG_NEW_LINE ;

   return retStr ;
}

/* *****************************************************************************
@discretion: get the log file
@author: Tanzhaobo
@parameter
   type[number]:
@return the log file
***************************************************************************** */
function _getJsLogFile( type )
{  
   switch( type )
   {
   case LOG_GENERIC:
      return JS_LOG_FILE ;
   default:
      if ( "number" == typeof(type) && 0 <= type )
      {
         return LOG_FILE_PATH + LOG_FILE_NAME ;
      }
      else
      {
         return JS_LOG_FILE ;
      }
   }
}

/* *****************************************************************************
@discretion: write the log message to the log file
@author: Tanzhaobo
@parameter
   type[number]: 
   infoStr[string]: the log message to write
@return void
***************************************************************************** */
function _write2File( type, infoStr )
{
   if ( LOG_NONE == type )
   {
      print( infoStr ) ;
      return ;
   }
   
   var file = null ;
   var logFile = _getJsLogFile( type ) ;
   try
   {   
      file = new File( logFile ) ;
   }
   catch( e )
   {
      var cmd = null ;
      var logFileFullName = "" ;
      var currentPath = "" ;
      var errMsg = "" ;
      var pos = -1 ;
      try
      {
         cmd = new Cmd() ;
         if ( "LINUX" == OS_TYPE_IN_JS_LOG )
         {
            currentPath = cmd.run( "pwd" ) ;
            // remove "\n" in the end of currentPath
            pos = currentPath.indexOf( "\n" ) ;
            if ( -1 != pos )
               currentPath = currentPath.substring(0, pos) ;
            logFileFullName = currentPath + "/" + logFile ;
         }
         else
         {
            // TODO: windows
            logFileFullName = "" ;
         }
      }
      catch( e )
      {
      }
      errMsg = "Failed to open log file[" + logFileFullName + "], rc: "
               + getLastError() + ", detail: " + getLastErrMsg() ;
      setLastErrMsg( errMsg ) ;
      setLastError( SDB_SYS ) ;
      throw SDB_SYS ;
   }
   file.seek( 0, "e" ) ;
   file.write( infoStr ) ;
   file.close() ;
}

/* *****************************************************************************
@discretion: get the log level description
@author: Tanzhaobo
@parameter
   level[number]: 0-5, the log level
@return the description of the log level
***************************************************************************** */
function _getPDLevelDesp( level )
{
   if ( "number" != typeof( level ) || level < PDSEVERE || level > PDDEBUG )
      return "UNKNOWN" ;
   switch( level )
   {
   case PDSEVERE:
      return "SEVERE" ;
   case PDERROR:
      return "ERROR" ;
   case PDEVENT:
      return "EVENT" ;
   case PDWARNING:
      return "WARNING" ;
   case PDINFO:
      return "INFO" ;
   case PDDEBUG:
      return "DEBUG" ;
   default:
      return "UNKNOWN" ;
   }
}

/* *****************************************************************************
@discretion: set log level
@author: Tanzhaobo
@parameter
   level[number]:
@return void
***************************************************************************** */
function setLogLevel( level )
{
   JS_LOG_LEVEL = level ;
}

/* *****************************************************************************
@discretion: get current log file's level
@author: Tanzhaobo
@parameter void
@return
   level[number]: the level of current log file
***************************************************************************** */
function getLogLevel()
{
   return JS_LOG_LEVEL ;
}

/* *****************************************************************************
@discretion: write the log
@author: Tanzhaobo
@parameter
   type[number]: the log type, -1 for none, -2 for general log,
      when type > 0, it is task type
   argsObj[object]: the arguments object of the function which invoked PD_LOG
   level[number]: 0-5, the log level
   func[string]: which function PD_LOG2 is invoked 
   line[number]: which line PD_LOG2 is invoked
   file[string]: which file PD_LOG is invoked
   message[string]: the log message
@return void
***************************************************************************** */
function PD_LOG3( type, argsObj, level, func, line, file, message )
{
   if ( "number" == typeof( level ) && level > JS_LOG_LEVEL )
      return ;
   var strArr = [ "Level", "TID", "Line" ] ;
   var formatStr = "?Level:?" + LOG_NEW_LINE +
                   "PID:?TID:?" + LOG_NEW_LINE +
                   "Function:?Line:?" + LOG_NEW_LINE +
                   "File:?" + LOG_NEW_LINE +
                   "Message:" + LOG_NEW_LINE + "?" + LOG_NEW_LINE ;
   var funcName = (argsObj.callee.toString().replace(/function\s?/mi, "").split("("))[0] ;
   var logInfo = sprintf( formatStr, genTimeStamp(), _getPDLevelDesp(level),
                          System.getPID(), System.getTID(),
                          funcName, "NULL", file, message ) ;
   var infoStr = _formatLogInfo( logInfo, strArr, 42 ) ;
   _write2File( type, infoStr ) ;
}

/* *****************************************************************************
@discretion: write the log
@author: Tanzhaobo
@parameter
   type[number]:
   argsObj[object]: the arguments object of the function which invoked PD_LOG
   level[number]: 0-5, the log level
   file[string]: which file PD_LOG is invoked
   message[string]: the log message
@return void
***************************************************************************** */
function PD_LOG2( type, argsObj, level, file, message )
{
   return PD_LOG3( type, argsObj, level, "", 0, file, message ) ;
}

/* *****************************************************************************
@discretion: write the log
@author: Tanzhaobo
@parameter
   argsObj[object]: the arguments object of the function which invoked PD_LOG
   level[number]: 0-5, the log level
   file[string]: which file PD_LOG is invoked
   message[string]: the log message
@return void
***************************************************************************** */
function PD_LOG( argsObj, level, file, message )
{
   return PD_LOG3( LOG_GENERIC, argsObj, level, "", 0, file, message ) ;
}

/* *****************************************************************************
@discretion: set the log's level to PDDEBUG temporary and then write the log
@author: Tanzhaobo
@parameter
   argsObj[object]: the arguments object of the function which invoked PD_LOG
   level[number]: 0-5, the log level
   file[string]: which file PD_LOG is invoked
   message[string]: the log message
@return void
***************************************************************************** */
function PD_LOG_DEBUG( argsObj, level, file, message )
{
   var oldLevel = getLogLevel() ;
   setLogLevel( PDDEBUG ) ;
   PD_LOG( argsObj, level, file, message ) ;
   setLogLevel( oldLevel ) ;
}

/* *****************************************************************************
@discretion: set the log's level to PDDEBUG temporary and then write the log
@author: Tanzhaobo
@parameter
   type[number]:
   argsObj[object]: the arguments object of the function which invoked PD_LOG
   level[number]: 0-5, the log level
   file[string]: which file PD_LOG is invoked
   message[string]: the log message
@return void
***************************************************************************** */
function PD_LOG_DEBUG2( type, argsObj, level, file, message )
{
   var oldLevel = getLogLevel() ;
   setLogLevel( PDDEBUG ) ;
   PD_LOG2( type, argsObj, level, file, message ) ;
   setLogLevel( oldLevel ) ;
}