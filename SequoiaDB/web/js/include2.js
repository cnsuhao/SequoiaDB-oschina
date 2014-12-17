//--------------------------------- 通用变量 ---------------------------------//

//判断是否正在发包
var isSending = false ;

//--------------------------------- 通用函数 ---------------------------------//

/*
 * 在底部显示信息
 * 参数1 错误类型 warning, danger, info
 */
function showInfoFromFoot( type, text )
{
	$( '#foot_alert_msg' ).stop() ;
	$( '#foot_alert_msg' ).hide() ;
	$( '#foot_alert_msg' ).removeClass().addClass( 'alert alert-' + type ).text( text ).slideDown(300).delay(5000).slideUp(300) ;
}

/*
 * 网页跳转
 */
function gotoPage( address )
{
	var timers = null ;
	function tempGoPage()
	{
		if ( isSending == false )
		{
			window.location.href = address ;
			clearInterval( timers ) ;
			timers = null ;
		}
	}
	timers = setInterval( tempGoPage, 100 ) ;
}

/*
 * 设置登录用户
 */
function setUser()
{
	$( '#username' ).text( sdbjs.fun.limitString( $.cookie( 'SdbUser' ), 20 ) ) ;
	if( $( '#username' ).parent().parent().innerWidth() < 95 )
	{
		$( '#username' ).parent().parent().css( 'margin-right', 95 - $( '#username' ).parent().parent().innerWidth() ) ;
	}
}

/*
 * 登录Om
 * 参数1 用户名
 * 参数2 密码
 */
function loginOM( user, pwd )
{
	var jsonArr ;
	var timestamp = parseInt( ( new Date().getTime() ) / 1000 ) ;
	var order = {'cmd' : 'login', 'user': user, 'passwd': pwd, 'Timestamp': timestamp } ;
	$.ajax( { type: 'POST', async: false, url: './', data: order, success: function( json, textStatus, xhr ){
		jsonArr = sdbjs.fun.parseJson( json ) ;
		if( jsonArr[0]['errno'] == 0 )
		{
			var id = xhr.getResponseHeader('SessionID') ;  
			$.cookie( 'SdbSessionID', id );
		}
	} } ) ;
	return jsonArr ;
}

/*
 * 登出Om
 */
function logoutOM()
{
	var jsonArr ;
	var order = {'cmd' : 'logout' } ;
	$.removeCookie( 'SdbSessionID' ) ;
	$.removeCookie( 'SdbUser' ) ;
	$.removeCookie( 'SdbPasswd' ) ;
	$.ajax( { type: 'POST', async: false, url: './', data: order, success: function( json, textStatus, xhr ){
		gotoPage( 'login.html' ) ;
	} } ) ;
}

/*
 * 通用错误处理
 * 返回值 如果true，则需要重发消息; 如果false，则不需要。
 */
function errorProcess( errno, detail )
{
	var rc = false ;
	if( errno == -179 )
	{
		//SessionID丢失或超时
		var user = $.cookie( 'SdbUser' ) ;
		var pwd  = $.cookie( 'SdbPasswd' ) ;
		if( user == undefined || pwd == undefined )
		{
			gotoPage( 'login.html' ) ;
		}
		else
		{
			var jsonArr = loginOM( user, pwd ) ;
			if( jsonArr[0]['errno'] == 0 )
			{
				rc = true ;
			}
		}
	}
	else
	{
		$( '.modal' ).each(function(index, element) {
         $( this ).hide() ;
      });
		$( '.dropdown-menu' ).each(function(index, element) {
         $( this ).hide() ;
      });
		$( sdbjs.private.htmlScreen2 ).hide() ;
		$( '#errorProcessModal > .modal-body' ).html( '<div class="alert alert-danger">' + detail + '</div>' ) ;
		sdbjs.fun.openModal( 'errorProcessModal' ) ;
		sdbjs.fun.moveModal( 'errorProcessModal' ) ;
	}
	return rc ;
}

/*
 * 通过ajax Post消息
 */
function ajaxSendMsg( data, async, success, error, complete )
{
	isSending = true ;
	$.ajax( { 'type': 'POST', 'async': async, 'url': './', 'data': data, 'success': function( json ){
		isSending = false ;
		if( json == '' )
		{
			errorProcess( -15, '网络异常，请检测网络是否连接正常。' ) ;
		}
		else
		{
			var jsonArr = sdbjs.fun.parseJson( json ) ;
			if( jsonArr[0]['errno'] == 0 )
			{
				if( typeof( success ) == 'function' )
				{
					success( jsonArr ) ;
				}
			}
			else
			{
				if( typeof( error ) == 'function' )
				{
					if( error( jsonArr ) == true )
					{
						//重发
						isSending = true ;
						$.ajax( { 'type': 'POST', 'async': async, 'url': './', 'data': data, 'success': function( json ){
							isSending = false ;
							var jsonArr = sdbjs.fun.parseJson( json ) ;
							if( jsonArr[0]['errno'] == 0 )
							{
								if( typeof( success ) == 'function' )
								{
									success( jsonArr ) ;
								}
							}
							else
							{
								if( typeof( error ) == 'function' )
								{
									error( jsonArr ) ;
								}
							}
						}, 'error': function( XMLHttpRequest, textStatus, errorThrown ) {
							isSending = false ;
							errorProcess( -15, '网络异常，请检测网络是否连接正常。' ) ;
						} } ) ;
					}
				}
			}
		}
	}, 'error': function( XMLHttpRequest, textStatus, errorThrown ) {
		isSending = false ;
		errorProcess( -15, '网络异常，请检测网络是否连接正常。' ) ;
	}, 'complete': function (XMLHttpRequest, textStatus) {
		if( typeof( complete ) == 'function' )
		{
			complete() ;
		}
	} } ) ;
}

function openChangePwdModal()
{
	$( '#changePwdErr' ).text( '' ) ;
	$( '#changePwdModal > .modal-body .form-control' ).each(function(index, element) {
      $( this ).val( '' ) ;
   });
	sdbjs.fun.openModal( 'changePwdModal' ) ;
	sdbjs.fun.moveModal( 'changePwdModal' ) ;
}

/*
 * 修改用户密码
 */
function changePwd()
{
	var value = [] ;
	$( '#changePwdModal > .modal-body .form-control' ).each(function(index, element) {
      value.push( $( this ).val() ) ;
   });
	if( value[1] != value[2] )
	{
		$( '#changePwdErr' ).text( '新密码两次输入不一样，请重新输入。' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( value[1], 6, 16 ) )
	{
		$( '#changePwdErr' ).text( '新密码由6-16个字符组成，区分大小写。' ) ;
		return;
	}
	if( $.cookie( 'SdbUser' ) == undefined )
	{
		$( '#changePwdErr' ).text( '用户名丢失。' ) ;
		return;
	}
	var newPwd = $.md5( value[1] ) ;
	var timestamp = parseInt( ( new Date().getTime() ) / 1000 ) ;
	var order = { 'cmd': 'change passwd', 'User': $.cookie( 'SdbUser' ), 'Passwd': $.md5( value[0] ), 'Newpasswd': newPwd, 'Timestamp': timestamp } ;
	ajaxSendMsg( order, true, function( jsonArr ){
		$.cookie( 'SdbPasswd', newPwd ) ;
		sdbjs.fun.closeModal( 'changePwdModal' ) ;
		showInfoFromFoot( 'info', '密码修改成功' ) ;
	}, function( jsonArr ){
		$( '#changePwdErr' ).text( jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 数据路径转义
 * 参数1 路径字符串，例子 /opt/sequoiadb/[role]/[svcname]/[groupname]/[hostname] 可用的特殊命令就是 [role] [svcname] [groupname] [hostname]
 * 参数2 主机名
 * 参数3 端口
 * 参数4 角色
 * 参数5 分区组名
 */
function dbpathEscape( str, hostname, svcname, role, groupname )
{
	var newPath = '' ;
	while( true )
	{
		var leftNum = str.indexOf( '[' ) ;
		var rightNum = -1 ;
		if( leftNum >= 0 )
		{
			newPath += str.substring( 0, leftNum ) ;
			str = str.substring( leftNum ) ;
			rightNum = str.indexOf( ']' ) ;
			if( rightNum >= 0 )
			{
				var order = str.substring( 1, rightNum ) ;
				if( order == 'hostname' )
				{
					newPath += hostname + '' ;
				}
				else if( order == 'svcname' )
				{
					newPath += svcname + '' ;
				}
				else if( order == 'role' )
				{
					newPath += role + '' ;
				}
				else if( order == 'groupname' )
				{
					newPath += groupname + '' ;
				}
				else
				{
					newPath += str.substring( 0, rightNum + 1 ) ;
				}
				str = str.substring( rightNum + 1 ) ;
			}
			else
			{
				newPath += str ;
				break ;
			}
		}
		else
		{
			newPath += str ;
			break ;
		}
	}
	return newPath ;
}


/*
 * 端口转义
 * 参数1 端口字符串，例子 '11810[+10]'
 * 参数2 第几个节点 最小值 1
 */
function portEscape( str, num )
{
	var newPort = null ;
	str = str + '' ;
	if( str == '' )
	{
		return str ;
	}
	if( str.indexOf( '[' ) > 0 )
	{
		var portStr = str.substring( 0, str.indexOf( '[' ) ) ;
		var escapeStr = str.substring( str.indexOf( '[' ) ) ;
		var n = 1 ;
		if( escapeStr.charAt(0) == '[' && escapeStr.charAt(escapeStr.length - 1) == ']' )
		{
			if( escapeStr.charAt(1) == '+' )
			{
				n = 1 ;
			}
			else if( escapeStr.charAt(1) == '-' )
			{
				n = -1 ;
			}
			else
			{
				return null ;
			}
			var tempNum = parseInt( escapeStr.substring( 2, escapeStr.length - 1 ) ) * num * n ;
			newPort = '' + ( parseInt( portStr ) + tempNum ) ;
		}
		else
		{
			return null ;
		}
	}
	else
	{
		newPort = str ;
	}
	if( sdbjs.fun.checkPort( newPort ) )
	{
		return newPort ;
	}
	else
	{
		return null ;
	}
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
	var returnValue = [ true, '' ] ;
	if( inputType == 'edit box' || inputType == 'text box' )
	{
		if( valueType == 'int' )
		{
			if( valid != '' && valid.indexOf('-') != -1 )
			{
				var splitValue = valid.split( '-' ) ;
				var minValue = splitValue[0] ;
				var maxValue = splitValue[1] ;
				if( !sdbjs.fun.checkInt( value, minValue, maxValue ) )
				{
					returnValue[0] = false ;
					returnValue[1] = key ;
					if( minValue != '' )
					{
						returnValue[1] += ' 最小值是 ' + minValue ;
					}
					if( maxValue != '' )
					{
						returnValue[1] +=  ' 最大值是 ' + maxValue ;
					}
				}
			}
			else
			{
				if( !sdbjs.fun.checkInt( value, '', '' ) )
				{
					returnValue[0] = false ;
					returnValue[1] = key + '不是整数' ;
				}
			}
		}
		else if( valueType == 'port' )
		{
			if( !sdbjs.fun.checkPort( value ) )
			{
				returnValue[0] = false ;
				returnValue[1] = key + ' 端口错误' ;
			}
		}
	}
	else
	{
		returnValue[0] = true ;
		returnValue[1] = '' ;
	}
	return returnValue ;
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
	var str = '' ;
	if( typeof( Edit ) == 'string' )
	{
		Edit = ( Edit == 'true' ? true : false ) ;
	}
	if( inputType == 'edit box' )
	{
		if( Edit != false )
		{
			str = '<input class="form-control" type="text" value="' + defaultValue + '">' ;
		}
		else
		{
			str = '<input class="form-control" type="text" value="' + defaultValue + '" disabled="disabled">' ;
		}
	}
	else if( inputType == 'select box' )
	{
		var option = '' ;
		if( Edit != false )
		{
			option = '<select class="form-control">' ;
		}
		else
		{
			option = '<select class="form-control" disabled="disabled">' ;
		}
		var selectList = valid.split(',') ;
		var listLen = selectList.length ;
		for( var l = 0; l < listLen; ++l )
		{
			if( selectList[l] == defaultValue )
			{
				option += '<option value="' + selectList[l] + '" selected="selected">' + sdbjs.fun.htmlEncode( selectList[l] ) + '</option>' ;
			}
			else
			{
				option += '<option value="' + selectList[l] + '">' + sdbjs.fun.htmlEncode( selectList[l] ) + '</option>' ;
			}
		}
		option += '<select>' ;
		str = option ;
	}
	else if( inputType == 'text box' )
	{
		if( Edit != false )
		{
			str = '<textarea class="form-control" rows="4">' + defaultValue + '</textarea>' ;
		}
		else
		{
			str = '<textarea class="form-control" rows="4" disabled="disabled">' + defaultValue + '</textarea>' ;
		}
	}
	return str ;
}

/*
 * 加载值到输入框
 * 参数1 输入框对象
 * 参数2 输入框类型
 * 参数3 值
 */
function loadValue2Input( inputObj, inputType, value )
{
	if( inputType == 'edit box' || inputType == 'text box' )
	{
		$( inputObj ).val( value ) ;
	}
	else if ( inputType == 'select box' )
	{
		var input_box_obj = $( inputObj ).get(0) ;
		var select_len = input_box_obj.options.length ;
		for( var k = 0; k < select_len; ++k )
		{
			if( input_box_obj.options[k].value == value )
			{
				input_box_obj.options[k].selected = true;
				break;
			}
		}
	}
}

//--------------------------------- 预加载 -----------------------------------//

/*
 * 设置每次请求的会话ID
 */
$( document ).ajaxSend(function(event, jqXHR, ajaxOptions) {
	var id = $.cookie( 'SdbSessionID' ) ;
	if( id != null )
	{
		jqXHR.setRequestHeader( 'SessionID', id ) ;
	}
});

/*
 * 通用预创建
 */
$(document).ready(function(){
	//创建修改密码
	sdbjs.parts.modalBox.create( 'changePwdModal', {}, { 'header': true, 'body': true, 'foot': true } ).appendTo( $( 'body' ) ) ;
	$( '#changePwdModal > .modal-title' ).append( '修改密码' ) ;
	$( '#changePwdModal > .modal-body' ).append( '<table class="noborder-table"><tr><td width="70">当前密码：</td><td><input id="passwd" type="password" class="form-control"></td></tr><tr><td width="70">新密码：</td><td><input id="passwd" type="password" class="form-control"></td></tr><tr><td width="70">确认密码：</td><td><input id="passwd" type="password" class="form-control"></td></tr><tr><td colspan="2" id="changePwdErr" style="color:#F00"></td></tr></table>' ) ;
	$( '#changePwdModal > .modal-foot' ).append( '<button class="btn btn-primary" onclick="changePwd()">确定</button>&nbsp;<button class="btn btn-primary" data-type="close-modal" data-target="changePwdModal">关闭</button>' ) ;
	//创建通用错误提示窗口
	sdbjs.parts.modalBox.create( 'errorProcessModal', {}, { 'header': true, 'body': true, 'foot': true } ).appendTo( $( 'body' ) ) ;
	$( '#errorProcessModal > .modal-title' ).append( '错误' ) ;
	$( '#errorProcessModal > .modal-foot' ).append( '<button class="btn btn-primary" data-type="close-modal" data-target="errorProcessModal">关闭</button>' ) ;
} ) ;

var ___picOption = {
	title: {
		text: '集群CPU',
		subtext: 'percent'
	},
	animation: false,
	addDataAnimation: false,
	toolbox: {
		show: false,
		feature: {
			saveAsImage: {show: true}
		}
	},
	xAxis : [
		{
			type: 'category',
			boundaryGap: false,
			data: [
				 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
				10,11,12,13,14,15,16,17,18,19,
				20,21,22,23,24,25,26,27,28,29
			]
		}
	],
	yAxis : [
		{
			type: 'value',
			axisLabel: {
				formatter: '{value} %'
			}
		}
	],
	series : [
		{
			type: 'line',
			data: [
				0,0,0,0,0,0,0,0,0,0,
				0,0,0,0,0,0,0,0,0,0,
				0,0,0,0,0,0,0,0,0,0
			],
			itemStyle: {
				normal: {
					lineStyle: {
					color: '#2E76CA',
					shadowColor: 'rgba(0,0,0,0.4)',
					shadowBlur: 5,
					shadowOffsetX: 3,
					shadowOffsetY: 3
					}
				}
			}
		}
	]
};