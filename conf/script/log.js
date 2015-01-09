var OMA_NEW_LINE = "" ;
var JS_LOG_FILE = "" ;
var PDSEVERE = 0 ;
var PDERROR = 1 ;
var PDEVENT = 2 ;
var PDWARNING = 3 ;
var PDINFO = 4 ;
var PDDEBUG = 5 ;
var JS_LOG_LEVEL = PDERROR ;
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
      JS_LOG_LEVEL = PDERROR ;
}
catch( e )
{
   OS_TYPE_IN_JS_LOG = "LINUX" ;
   JS_LOG_LEVEL = PDERROR ;
}

if( "LINUX" == OS_TYPE_IN_JS_LOG )
{
   OMA_NEW_LINE = "\n" ;
   JS_LOG_FILE = "../conf/log/sdbcm_js.log" ;
}
else
{
   OMA_NEW_LINE = "\r\n" ;
   JS_LOG_FILE = "..\\conf\\log\\sdbcm_js.log" ;
}

// get func and line
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
         stackArr = (e.stack).split( OMA_NEW_LINE ) ;
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

function getFunc()
{
   return  _getFuncOrLine( "func" ) ;
}

function getLine()
{
   return _getFuncOrLine( "line" ) - 1 ;
}

// gen log format
function _genSpace( num )
{
   var retStr = "" ;
   for ( var i = 0; i < num; i++ )
      retStr += " " ;
   return retStr ;
}

function _genOneLine( inputStr, key, indent )
{
   var retStr = ( "string" == typeof(inputStr) ) ? inputStr : "" ;
   var index = inputStr.indexOf( key ) ;
   if ( -1 == index )
      return inputStr ;
   var arr = inputStr.split( key ) ;
   if ( indent > index )
   {
      var space = _genSpace( indent - index - 1 ) ;
      retStr = arr[0] + space + key + arr[1] + OMA_NEW_LINE ;
   }
   else
   {
      retStr = arr[0] + " " + key + arr[1] + OMA_NEW_LINE ; 
   }
   return retStr ;
} 

function _genLogInfo( contentStr, keyArr, indent )
{
   var retStr = "" ;
   var i = 0 ;
   var arr = contentStr.split( OMA_NEW_LINE ) ;
   for( i = 0; i < keyArr.length; i++ )
      retStr += _genOneLine( arr[i], keyArr[i], 42 ) ; 
   for( ; i < arr.length; i++ )
      retStr += arr[i] + OMA_NEW_LINE ;

   return retStr ;
}

function getJsLogFile()
{
   return JS_LOG_FILE ;
}

function _write2File( infoStr )
{
   var logFile = getJsLogFile() ;
   var file = new File( logFile ) ;
   file.seek( 0, "e" ) ;
   file.write( infoStr ) ;
   file.close() ;
}

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

function PD_LOG( argsObj, level, func, line, file, message )
{
   if ( "number" == typeof( level ) && level > JS_LOG_LEVEL )
      return ;
   var strArr = [ "Level", "TID", "Line" ] ;
   var formatStr = "?Level:?" + OMA_NEW_LINE +
                   "PID:?TID:?" + OMA_NEW_LINE +
                   "Function:?Line:?" + OMA_NEW_LINE +
                   "File:?" + OMA_NEW_LINE +
                   "Message:" + OMA_NEW_LINE + "?" + OMA_NEW_LINE ;
   var funcName = (argsObj.callee.toString().replace(/function\s?/mi, "").split("("))[0] ;
   var logInfo = sprintf( formatStr, genTimeStamp(), _getPDLevelDesp(level),
                          "NULL", "NULL", funcName, "NULL", file, message ) ;
   var infoStr = _genLogInfo( logInfo, strArr, 42 ) ;
   _write2File( infoStr ) ;
}

/*
function main()
{
   PD_LOG ( arguments, PDERROR, 0, 0, "log.js", "Hello world!" ) ;
}

main() ;
*/
