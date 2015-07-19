//--------------------------------- 通用变量 ---------------------------------//

//判断是否正在发包
var _isSending = false ;

//当前网页的文件名(不带后缀)
var _cursorFileName = '' ;

//语言
var _language = 'zh-CN' ;

//语言包
var _languagePack = {} ;

//任务列表
var _taskList = [] ;

//--------------------------------- 通用函数 ---------------------------------//

/*
 * 把普通字符转换成html字符,给html显示
 */
function htmlEncode( str )
{
	var s = "" ;
	str = str + '' ;
	if( str.length == 0 ) return "" ;
	s = str.replace( /&/g, "&amp;" ) ;
	s = s.replace( /</g, "&lt;" ) ;
	s = s.replace( />/g, "&gt;" ) ;
	s = s.replace( / {2,}/g, function( str ){
		str = str.replace( / /g, "&nbsp;" ) ;
		return str ;
	} ) ;
	s = s.replace( /\'/g, "&#39;" ) ;
	s = s.replace( /\"/g, "&quot;" ) ;
	s = s.replace( /\n/g, "<br>" ) ;
	return s ;
}

/*
 * 把html字符转换成普通字符
 */
function htmlDecode( str )
{
	var s = "" ;
	str = str + '' ;
	if( str.length ==0 ) return "" ;
	s = str.replace( /&amp;/g, "&" ) ;
	s = s.replace( /&lt;/g, "<" ) ;
	s = s.replace( /&gt;/g, ">" ) ;
	s = s.replace( /&nbsp;/g, " " ) ;
	s = s.replace( /&#39;/g, "\'" ) ;
	s = s.replace( /&quot;/g, "\"" ) ;
	s = s.replace( /<br>/g, "\n" ) ;
	return s ; 
}

//页面跳转
function gotoPage( address )
{
	var timers = null ;
	timers = setInterval( function(){
		if ( _isSending === false )
		{
			window.location.href = address ;
			clearInterval( timers ) ;
			timers = null ;
		}
	}, 100 ) ;
}

//在模态框底部显示错误
function showModalError( nodeName, errMsg )
{
	sdbjs.fun.setCSS( nodeName, { 'display': 'block' } ) ;
	sdbjs.parts.alertBox.update( nodeName, htmlEncode( errMsg ), 'danger' ) ;
}

//显示状态栏
function showFootStatus( type, text )
{
	var node = sdbjs.fun.getNode( 'ststusAlert', 'alertBox' ) ;
	sdbjs.parts.alertBox.update( 'ststusAlert', htmlEncode( text ), type ) ;
	$( node['obj'] ).stop() ;
	$( node['obj'] ).slideDown(300).delay(5000).slideUp(300) ;
}

//显示错误信息
function showProcessError( text )
{
	sdbjs.parts.alertBox.update( 'processErrorAlert', htmlEncode( text ), 'danger' ) ;
	sdbjs.parts.modalBox.show( 'processError' ) ;
}

/*
 * 通过ajax Post消息
 */
function ajaxSendMsg( data, async, before, success, error, complete, isJson )
{
	isJson =  ( ( typeof( isJson ) === 'undefined' || isJson === true ) ? true : false ) ;
	_isSending = true ;
	$.ajax( { 'type': 'POST', 'async': async, 'url': '/', 'data': data, 'success': function( json, textStatus, jqXHR ){
		if( json === '' )
		{
			showProcessError( _languagePack['error']['system']['networkErr'] ) ;//'网络异常'
		}
		else
		{
			if( isJson === true )
			{
				var jsonArr = sdbjs.fun.parseJson( json ) ;
				if( jsonArr[0]['errno'] === 0 )
				{
					if( typeof( success ) == 'function' )
					{
						jsonArr.splice( 0, 1 ) ;
						success( jsonArr, textStatus, jqXHR )
					}
				}
				else if( jsonArr[0]['errno'] === -62 )
				{
					//session id 不存在
					gotoPage( 'login.html' )
				}
				else
				{
					if( typeof( error ) === 'function' )
					{
						error( jsonArr[0], textStatus, jqXHR ) ;
					}
				}
			}
			else
			{
				if( typeof( success ) == 'function' )
				{
					success( json, textStatus, jqXHR )
				}
			}
		}
	}, 'error': function( XMLHttpRequest, textStatus, errorThrown ) {
		showProcessError( _languagePack['error']['system']['networkErr'] ) ;//网络异常
	}, 'complete': function ( XMLHttpRequest, textStatus ) {
		if( typeof( complete ) == 'function' )
		{
			complete( XMLHttpRequest, textStatus ) ;
		}
		_isSending = false ;
	}, 'beforeSend': function( XMLHttpRequest ){
		if( typeof( before ) == 'function' )
		{
			before( XMLHttpRequest ) ;
		}
	} } ) ;
}

/*
 * 检查字符串
 * 参数1 字符串
 * 参数2 最小长度
 * 参数3 最大长度
 */
function checkString( str, s_min, s_max )
{
	if ( typeof( str ) == 'string' )
	{
		var len = str.length ;
		if ( len < s_min || len > s_max )
		{
			return false ;
		}
		return true ;
	}
	else
	{
		return false ;
	}
}

/*
 * 检查命名
 * 参数1 字符串
*/
function checkStrName( str )
{
	if ( !checkString( str, 1, 255 ) )
	{
		return false ;
	}
	var len = str.length ;
	for( var i = 0; i < len; ++i )
	{
		var char = str.charAt( i ) ;
		if ( ( char < 'a' || char > 'z' ) &&
	     	  ( char < 'A' || char > 'Z' ) &&
			  ( char < '0' || char > '9' ) &&
		       char !== '_' )
		{
			return false ;
		}
	}
	return true ;
}

//判断是否整数
function isIntNumber( value )
{
	var type = "^-?\\d+$" ; 
	var re = new RegExp( type ) ;
	if ( value.match( re ) == null )
	{
		return false ;
	}
	return true ;
}

//检测整数
function checkInt( num, n_min, n_max )
{
	var number = 0 ;
	if ( typeof( num ) === 'string' )
	{
		if( !isIntNumber( num ) )
		{
			return false ;
		}
		number = parseInt( num ) ;
	}
	else if ( typeof( num ) === 'number' )
	{
		if ( parseInt( num ) !== num || num === NaN )
		{
			return false ;
		}
		number = parseInt( num ) ;
	}
	else
	{
		return false ;
	}
	if ( n_min != '' && number < parseInt( n_min ) )
	{
		return false ;
	}
	if ( n_max != '' && number > parseInt( n_max ) )
	{
		return false ;
	}
	return true ;
}

//检测端口
function checkPort( str )
{
	var len = str.length ;
	if ( len <= 0 )
	{
		return false ;
	}
	if ( str.charAt( 0 ) == '0' )
	{
		return false ;
	}
	for ( var i = 0; i < len; ++i )
	{
		var char = str.charAt( i ) ;
		if ( char < '0' || char > '9' )
		{
			return false ;
		}
	}
	var port = parseInt( str ) ;
	if ( port <= 0 || port > 65535 )
	{
		return false ;
	}
	return true ;
}

/*
 * 创建输入框
 * 参数1 输入框类型
 * 参数2 值的约束
 * 参数3 默认值
 * 参数4 是否可以修改
 */
function createHtmlInput( inputType, valid, defaultValue, Edit )
{
	var obj = null ;
	if( typeof( Edit ) === 'string' )
	{
		Edit = ( Edit === 'true' ? true : false ) ;
	}
	if( inputType === 'edit box' )
	{
		obj = $( '<input>' ).addClass( 'form-control' ).attr( 'type', 'text' ).val( defaultValue ) ;
		if( Edit === false )
		{
			$( obj ).attr( 'disabled', 'disabled' ) ;
		}
	}
	else if( inputType === 'select box' )
	{
		obj = $( '<select></select>' ).addClass( 'form-control' ) ;
		if( Edit === false )
		{
			$( obj ).attr( 'disabled', 'disabled' ) ;
		}
		var selectList = valid.split(',') ;
		$.each( selectList, function( index, validValue ){
			var options = $( '<option></option>' ).val( validValue ).text( validValue ) ;
			if( validValue === defaultValue )
			{
				options.attr( 'selected', 'selected' ) ;
			}
			obj.append( options ) ;
		} ) ;
	}
	else if( inputType === 'text box' )
	{
		obj = $( '<textarea></textarea>' ).addClass( 'form-control' ).attr( 'rows', '4' ).val( defaultValue ) ;
		if( Edit === false )
		{
			$( obj ).attr( 'disabled', 'disabled' ) ;
		}
	}
	return obj ;
}

/*
 * 输入框值判断
 * 参数1 输入框类型
 * 参数2 值的类型
 * 参数3 值的约束
 * 参数4 值的名称
 * 参数5 值
 */
function checkInputValue( inputType, valueType, valid, key, value )
{
	var returnValue = '' ;
	if( inputType === 'edit box' || inputType === 'text box' )
	{
		if( valueType === 'int' )
		{
			if( valid !== '' && valid.indexOf('-') !== -1 )
			{
				var splitValue = valid.split( '-' ) ;
				var minValue = splitValue[0] ;
				var maxValue = splitValue[1] ;
				if( !checkInt( value, minValue, maxValue ) )
				{
					if( !isIntNumber( value ) )
					{
						returnValue = sdbjs.fun.sprintf( _languagePack['error']['web']['input'][0], key, minValue ) ;//'? 请输入一个整数'
					}
					else
					{
						if( minValue !== '' && value < parseInt( minValue ) )
						{
							returnValue = sdbjs.fun.sprintf( _languagePack['error']['web']['input'][1], key, minValue ) ;//'? 最小值是 ?'
						}
						if( maxValue !== '' && value > parseInt( maxValue ) )
						{
							returnValue =  sdbjs.fun.sprintf( _languagePack['error']['web']['input'][2], key, maxValue ) ;//'? 最大值是 ?'
						}
					}
				}
			}
		}
		else if( valueType === 'port' )
		{
			if( !checkPort( value ) )
			{
				returnValue = sdbjs.fun.sprintf( _languagePack['error']['web']['input'][3], key ) ;//'? 端口错误'
			}
		}
	}
	else if( inputType === 'select box' )
	{
		if( valid !== '' )
		{
			if( value === null )
			{
				returnValue =  sdbjs.fun.sprintf( _languagePack['error']['web']['input'][4], key ) ;//'? 必须选择一个值'
			}
		}
	}
	return returnValue ;
}

//保留两位小数
function twoDecimalPlaces( num )
{
	return ( Math.round( num * 100 ) / 100 ) ;
}


//用于容量自动转换合适的单位
function sizeConvert( num )
{
	var rn = '0 MB' ;
	if ( num < 1 && num > 0 )
	{
		rn = ( num * 1024 ) + ' KB' ;
	}
	if( num >= 1 && num < 1024 )
	{
		rn = num+ ' MB' ;
	}
	if( num >= 1024 && num < 1048576 )
	{
		rn = twoDecimalPlaces( num / 1024 ) + ' GB' ;
	}
	else if ( num >= 1048576 && num < 1073741824 )
	{
		rn = twoDecimalPlaces( num / 1048576 ) + ' TB' ;
	}
	else if ( num >= 1073741824 )
	{
		rn = twoDecimalPlaces( num / 1073741824 ) + ' PB' ;
	}
	return rn ;
}

//获取语言包
function getPageLanguage( fileName )
{
	var url = './language/' + _language + '/' + fileName ;
	$.ajax( { 'type': 'GET', 'async': false, 'url': url, 'success': function( json, textStatus, jqXHR ){
		var jsonObj = sdbjs.fun.parseJson( json ) ;
		jsonObj = jsonObj[0] ;
		$.extend( _languagePack, jsonObj ) ;
	}, 'error': function( XMLHttpRequest, textStatus, errorThrown ) {
		showProcessError( 'Error: Network error.' ) ;
	} } ) ;
}

//--------------------------------- 预加载 -----------------------------------//

$(window).resize(function(){
	$( document.body ).css( 'overflow', 'hidden' ) ;
	sdbjs.fun.setRootNode( $( '#root' ), 'variable', 'variable' ) ;
	$( document.body ).css( 'overflow', 'visible' ) ;
	sdbjs.fun.allNodeRepaint() ;
} ) ;

/*
 * 通用预创建
 */
$(document).ready(function(){
	//检测非登录页面
	if( _cursorFileName !== 'login' )
	{
		if( sdbjs.fun.getData( 'SdbUser' ) === null )
		{
			gotoPage( 'login.html' ) ;
			return;
		}
		if( sdbjs.fun.getData( 'SdbSessionID' ) === null )
		{
			gotoPage( 'login.html' ) ;
			return;
		}
	}
	//判断浏览器语言
	_language = sdbjs.fun.getData( 'SdbLanguage' ) ;
	if( _language === null )
	{
		_language = sdbjs.fun.getLanguage() ;
		sdbjs.fun.saveData( 'SdbLanguage', _language ) ;
	}
	//获取语言包
	getPageLanguage( _cursorFileName ) ;
	getPageLanguage( 'error' ) ;
	getPageLanguage( 'html' ) ;
} ) ;

//获取网页名
var path = window.location.pathname ;
if( path == '/' )
{
	_cursorFileName = 'index' ;
}
else
{
	var fileName = path.substr( path.lastIndexOf( '/' ) + 1 ) ;
	_cursorFileName = fileName.substr( 0, fileName.indexOf( '.' ) ) ;
}