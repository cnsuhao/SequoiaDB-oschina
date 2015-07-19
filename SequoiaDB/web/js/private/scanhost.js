
//全局主机列表
var _hostList = [];

//集群名
var _clusterName = null ;

//部署模式
var _deployModel = null ;

//清空扫描主机的输入框内容
function clearInput()
{
	$( '#hostSearchAddress' ).val( '' ) ;
	$( '#hostSearchPasswd' ).val( '' ) ;
}

/*
 * 解析ip ip段 hostname  hostname段
 * 返回数组  [ { 'address': 'xxx', 'type': 'host' }, { 'address': 'xxx', 'type': 'ip' } ]
 */
function parseAddress( address )
{
	//数字字符串自动补零
	function pad(num, n)
	{
		var len = num.toString().length;
		while(len < n)
		{
			num = "0" + num;
			len++;
		}
		return num;
	}
	var link_search = [] ;
	var splitAddress = address.split( /[,\s;]/ ) ;
	var splitLen = splitAddress.length ;
	
	for( var strNum = 0; strNum < splitLen; ++strNum )
	{
		var str = $.trim( splitAddress[ strNum ] ) ;
		var ip_search = [] ;
		var host_search = [] ;
		var matches = new Array() ;
		//识别主机字符串，扫描主机
		var reg = new RegExp(/^(((2[0-4]\d|25[0-5]|[01]?\d\d?)|(\[[ ]*(2[0-4]\d|25[0-5]|[01]?\d\d?)[ ]*\-[ ]*(2[0-4]\d|25[0-5]|[01]?\d\d?)[ ]*\]))\.){3}((2[0-4]\d|25[0-5]|[01]?\d\d?)|(\[(2[0-4]\d|25[0-5]|[01]?\d\d?)\-(2[0-4]\d|25[0-5]|[01]?\d\d?)\]))$/) ;
		if ( ( matches = reg.exec( str ) ) != null )
		{
			//ip区间
			var ip_arr = str.split(".") ;
			for ( ipsub in ip_arr )
			{
				reg = new RegExp(/^((2[0-4]\d|25[0-5]|[01]?\d\d?)|(\[[ ]*(2[0-4]\d|25[0-5]|[01]?\d\d?)[ ]*\-[ ]*(2[0-4]\d|25[0-5]|[01]?\d\d?)[ ]*\]))$/) ;
				if ( ( matches = reg.exec( ip_arr[ipsub] ) ) != null )
				{
					//匹配每个数值
					if ( ( matches[4] == undefined || matches[5] == undefined ) ||
						  ( matches[4] === '' || matches[5] === '' ) )
					{
						//这是一个数字 192
						ip_search.push( matches[0] ) ;
					}
					else
					{
						//这是一个区间 [1-10]
						ip_search.push( new Array( matches[4], matches[5] ) ) ;
					}
				}
			}
		}
		else
		{
			//主机名
			reg = new RegExp(/^((.*)(\[[ ]*(\d+)[ ]*\-[ ]*(\d+)[ ]*\])(.*))$/) ;
			if ( ( matches = reg.exec( str ) ) != null )
			{
				host_search.push( matches[2] ) ;
				host_search.push( matches[4] ) ;
				host_search.push( matches[5] ) ;
				host_search.push( matches[6] ) ;
			}
			else
			{
				host_search = str ;
			}
		}
	
		if ( ip_search.length > 0 )
		{
			//遍历数组，把IP段转成每个IP存入数组
			for( var i = ( sdbjs.fun.isArray( ip_search[0] ) ? parseInt(ip_search[0][0]) : 0 ), i_end = ( sdbjs.fun.isArray( ip_search[0] ) ? parseInt(ip_search[0][1]) : 0 ); i <= i_end; ++i )
			{
				for( var j = ( sdbjs.fun.isArray( ip_search[1] ) ? parseInt(ip_search[1][0]) : 0 ), j_end = ( sdbjs.fun.isArray( ip_search[1] ) ? parseInt(ip_search[1][1]) : 0 ); j <= j_end; ++j )
				{
					for( var k = ( sdbjs.fun.isArray( ip_search[2] ) ? parseInt(ip_search[2][0]) : 0 ), k_end = ( sdbjs.fun.isArray( ip_search[2] ) ? parseInt(ip_search[2][1]) : 0 ); k <= k_end; ++k )
					{
						for( var l = ( sdbjs.fun.isArray( ip_search[3] ) ? parseInt(ip_search[3][0]) : 0 ), l_end = ( sdbjs.fun.isArray( ip_search[3] ) ? parseInt(ip_search[3][1]) : 0 ); l <= l_end; ++l )
						{
							link_search.push( { 'IP': (( sdbjs.fun.isArray( ip_search[0] ) ? i : ip_search[0] )+'.'+( sdbjs.fun.isArray( ip_search[1] ) ? j : ip_search[1] )+'.'+( sdbjs.fun.isArray( ip_search[2] ) ? k : ip_search[2] )+'.'+( sdbjs.fun.isArray( ip_search[3] ) ? l : ip_search[3] )) } ) ;
						}
					}
				}
			}
		}
	
	
		if ( host_search.length > 0 )
		{
			//转换hostname
			if ( sdbjs.fun.isArray( host_search ) )
			{
				var str_start = host_search[0] ;
				var str_end   = host_search[3] ;
				var strlen_num = host_search[1].length ;
				var strlen_temp  = parseInt(host_search[1]).toString().length ;
				var need_add_zero = false ;
				if ( strlen_num > strlen_temp )
				{
					need_add_zero = true ;
				}
				for ( var i = parseInt(host_search[1]), i_end = parseInt(host_search[2]); i <= i_end ; ++i )
				{
					if ( need_add_zero && i.toString().length <= strlen_num )
					{
						link_search.push( { 'HostName': str_start + pad(i,strlen_num) + str_end } ) ;
					}
					else
					{
						link_search.push( { 'HostName': str_start + i + str_end } ) ;
					}
				}
			}
			else
			{
				link_search.push( { 'HostName': host_search } ) ;
			}
		}
	}
	return link_search ;
}

//解析主机列表
function parseHostString( hostStr )
{
	var i = 0 ;
	var tempHostList = [] ;
	var hostList = [] ;
	var addressList = hostStr.split( ',' ) ;
	$.each( addressList, function( index, address ){
		var temp = parseAddress( address ) ;
		$.each( temp, function( index2, hostInfo ){
			tempHostList.push( hostInfo ) ;
			++i ;
			if( i === 5 )
			{
				hostList.push( tempHostList ) ;
				tempHostList = [] ;
				i = 0 ;
			}
		} ) ;
	} ) ;
	if( tempHostList.length > 0 )
	{
		hostList.push( tempHostList ) ;
	}
	return hostList ;
}

function checkHostIsExist( hostName, IP, user, pwd, ssh, proxy )
{
	// -1 不存在， 如果存在，返回下标
	var rc = -1 ;
	$.each( _hostList, function( index, hostInfo ){
		if( ( hostInfo['HostInfo'][0]['HostName'] !== '' && hostName !== '' &&
				hostInfo['HostInfo'][0]['HostName'] === hostName ) ||
			 ( hostInfo['HostInfo'][0]['IP'] !== '' && IP !== '' &&
				hostInfo['HostInfo'][0]['IP'] === IP ) )
		{
			if( ( hostInfo['HostInfo'][0]['HostName'] !== '' && hostName !== '' &&
				   hostInfo['HostInfo'][0]['HostName'] === hostName ) &&
				 ( hostInfo['HostInfo'][0]['IP'] !== '' && IP !== '' &&
				   hostInfo['HostInfo'][0]['IP'] !== IP ) )
			{
				rc = -2 ;
				return true ;
			}
			if( ( hostInfo['HostInfo'][0]['HostName'] !== '' && hostName !== '' &&
				   hostInfo['HostInfo'][0]['HostName'] !== hostName ) &&
				 ( hostInfo['HostInfo'][0]['IP'] !== '' && IP !== '' &&
				   hostInfo['HostInfo'][0]['IP'] === IP ) )
			{
				rc = -2 ;
				return true ;
			}
			rc = index ;
			_hostList[index]['HostInfo'][0]['HostName'] = hostName ;
			_hostList[index]['HostInfo'][0]['IP'] = IP ;
			_hostList[index]['User'] = user ;
			_hostList[index]['Passwd'] = pwd ;
			_hostList[index]['SshPort'] = ssh ;
			_hostList[index]['AgentService'] = proxy ;
			return false ;
		}
	} ) ;
	if( rc === -1 || rc == -2 )
	{
		_hostList.push( { 'HostInfo': [ { 'HostName': hostName, 'IP': IP } ], 'User': user, 'Passwd': pwd, 'SshPort': ssh, 'AgentService': proxy } ) ;
	}
	return rc ;
}

//分段扫描主机列表
function scanHostList( key, hostListArr, user, pwd, ssh, proxy )
{
	var clusterName = _clusterName ;
	var len = hostListArr.length ;
	if( key === 0 )
	{
		sdbjs.parts.loadingBox.show( 'loading' ) ;
	}
	sdbjs.parts.loadingBox.update( 'loading', parseInt( key * 100 / len ) + '%' ) ;
	if( key < len )
	{
		var hostList = hostListArr[key] ;
		restScanHost( true, function( jsonArr, textStatus, jqXHR ){
			$.each( jsonArr, function( index, hostInfo ){
				var status = _languagePack['error']['system']['scanErr'][0] ;//'连接成功'
				var input = '<input type="checkbox" checked="checked">' ;
				//var input = '<div class="checked" data-toggle="checkBox"></div>' ;
				if( hostInfo['errno'] !== 0 )
				{
					if( hostInfo['Status'] === 'ping' )
					{
						status = _languagePack['error']['system']['scanErr'][1] ;//'网络连接错误'
					}
					else if( hostInfo['Status'] === 'ssh' )
					{
						status = _languagePack['error']['system']['scanErr'][2] ;//'SSH连接错误'
					}
					else if( hostInfo['Status'] === 'getinfo' )
					{
						status = _languagePack['error']['system']['scanErr'][3] ;//'获取主机信息失败' ;
					}
					else
					{
						status = hostInfo['detail'] ;
					}
					input = '<input type="checkbox" disabled="disabled">' ;
					//input = '<div class="disunchecked" data-toggle="checkBox"></div>' ;
				}
				var rc = checkHostIsExist( hostInfo['HostName'], hostInfo['IP'], user, pwd, ssh, proxy ) ;
				if( rc === -1 )
				{
					//如果不存在，添加到列表
					sdbjs.parts.gridBox.addBody( 'hostSearchGrid', [ { 'text': input, 'width': '5%' },
																					 { 'text': htmlEncode( hostInfo['HostName'] ), 'width': '15%' },
																					 { 'text': htmlEncode( hostInfo['IP'] ), 'width': '15%' },
																					 { 'text': htmlEncode( user ), 'width': '10%' },
																					 { 'text': htmlEncode( '***' ), 'width': '10%' },
																					 { 'text': htmlEncode( ssh ), 'width': '8%' },
																					 { 'text': htmlEncode( proxy ), 'width': '8%' },
																					 { 'text': htmlEncode( status ), 'width': '15%' } ]  ) ;
					 var line = _hostList.length - 1 ;
					 sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 1, function( tdObj ){
						  $( tdObj ).css( 'cursor', 'pointer' ) ;
						  sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + line + ')' ) ;
					 } ) ;
					 sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 2, function( tdObj ){
						  $( tdObj ).css( 'cursor', 'pointer' ) ;
						  sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + line + ')' ) ;
					 } ) ;
					 sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 3, function( tdObj ){
						  $( tdObj ).css( 'cursor', 'pointer' ) ;
						  sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + line + ')' ) ;
					 } ) ;
					 sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 4, function( tdObj ){
						  $( tdObj ).css( 'cursor', 'pointer' ) ;
						  sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + line + ')' ) ;
					 } ) ;
					 sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 5, function( tdObj ){
						  $( tdObj ).css( 'cursor', 'pointer' ) ;
						  sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + line + ')' ) ;
					 } ) ;
					 sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 6, function( tdObj ){
						  $( tdObj ).css( 'cursor', 'pointer' ) ;
						  sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + line + ')' ) ;
					 } ) ;
				}
				else if( rc === -2 )
				{
					//该主机已经在列表中
					status = _languagePack['error']['system']['scanErr'][4] ;
					input = '<input type="checkbox" disabled="disabled">' ;
					//不存在，但是HostName 或者 IP 有重复
					sdbjs.parts.gridBox.addBody( 'hostSearchGrid', [ { 'text': input, 'width': '5%' },
																					 { 'text': htmlEncode( hostInfo['HostName'] ), 'width': '15%' },
																					 { 'text': htmlEncode( hostInfo['IP'] ), 'width': '15%' },
																					 { 'text': htmlEncode( user ), 'width': '10%' },
																					 { 'text': htmlEncode( '***' ), 'width': '10%' },
																					 { 'text': htmlEncode( ssh ), 'width': '8%' },
																					 { 'text': htmlEncode( proxy ), 'width': '8%' },
																					 { 'text': htmlEncode( status ), 'width': '15%' } ]  ) ;
				}
				else
				{
					//如果存在，更新列表
					sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', rc, 0, input ) ;
					sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', rc, 1, htmlEncode( hostInfo['HostName'] ) ) ;
					sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', rc, 2, htmlEncode( hostInfo['IP'] ) ) ;
					sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', rc, 3, htmlEncode( user ) ) ;
					sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', rc, 4, function( tdObj ){
						$( tdObj ).html( htmlEncode( '***' ) ) ;
						sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + rc + ')' ) ;
					} ) ;
					sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', rc, 5, function( tdObj ){
						$( tdObj ).html( htmlEncode( ssh ) ) ;
						sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + rc + ')' ) ;
					} ) ;
					sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', rc, 6, function( tdObj ){
						$( tdObj ).html( htmlEncode( proxy ) ) ;
						sdbjs.fun.addClick( tdObj, 'modifyHostPara(' + rc + ')' ) ;
					} ) ;
					sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', rc, 7, htmlEncode( status ) ) ;
				}
			} ) ;
			++key ;
			scanHostList( key, hostListArr, user, pwd, ssh, proxy ) ;
		}, function( json ){
			sdbjs.parts.loadingBox.hide( 'loading' ) ;
			showProcessError( json['description'] ) ;
		}, null, clusterName, hostList, user, pwd, ssh, proxy ) ;
	}
	else
	{
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
	}
}

//重新扫描
function reScanHost( line )
{
	var user = $( '#hostSearchUser' ).val() ;
	var pwd = '' ;
	var ssh = '' ;
	var proxy = '' ;

	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 4, function( tdObj ){
		pwd = $( tdObj ).children( 'input' ).val() ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 5, function( tdObj ){
		ssh = $( tdObj ).children( 'input' ).val() ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 6, function( tdObj ){
		proxy = $( tdObj ).children( 'input' ).val() ;
	} ) ;
	
	if( !checkString( user, 1, 255 ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][1] ) ;//'用户名的长度必须1-255范围内。'
		return;
	}
	if( !checkString( pwd, 1, 255 ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][2] ) ;//'密码的长度在 1 - 255 范围。'
		return;
	}
	if( !checkPort( ssh ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][3] ) ;//'SSH端口格式错误。'
		return;
	}
	if( !checkPort( proxy ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][4] ) ;//'代理端口格式错误。'
		return;
	}
	
	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 4, function( tdObj ){
		$( tdObj ).children( 'input' ).css( 'z-index', '1' ) ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 5, function( tdObj ){
		$( tdObj ).children( 'input' ).css( 'z-index', '1' ) ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 6, function( tdObj ){
		$( tdObj ).children( 'input' ).css( 'z-index', '1' ) ;
	} ) ;
	
	//隐藏透明遮罩
	sdbjs.parts.screenBox.hide( 'modifyScreen' ) ;
	
	var hostListArr = [ [ {} ] ] ;
	
	if( _hostList[line]['HostInfo'][0]['HostName'] !== '' )
	{
		hostListArr[0][0]['HostName'] = _hostList[line]['HostInfo'][0]['HostName'] ;
	}
	else if( _hostList[line]['HostInfo'][0]['IP'] !== '' )
	{
		hostListArr[0][0]['IP'] = _hostList[line]['HostInfo'][0]['IP'] ;
	}
	
	scanHostList( 0, hostListArr, user, pwd, ssh, proxy ) ;
}

//修改主机参数
function modifyHostPara( line )
{
	//给遮罩添加事件
	var node = sdbjs.fun.getNode( 'modifyScreen', 'screenBox' ) ;
	node['data'] = line ;
	sdbjs.fun.addClick( node['obj'], 'reScanHost(' + line + ')' ) ;

	var pwd = _hostList[line]['Passwd'] ;
	var ssh = _hostList[line]['SshPort'] ;
	var proxy = _hostList[line]['AgentService'] ;
	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 4, function( tdObj ){
		sdbjs.fun.addClick( tdObj, '' ) ;
		$( tdObj ).html( '<input class="form-control" style="z-index:1002;position:relative;" type="password" value="' + htmlEncode( pwd ) + '">' ) ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 5, function( tdObj ){
		sdbjs.fun.addClick( tdObj, '' ) ;
		$( tdObj ).html( '<input class="form-control" style="z-index:1002;position:relative;" type="text" value="' + htmlEncode( ssh ) + '">' ) ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', line, 6, function( tdObj ){
		sdbjs.fun.addClick( tdObj, '' ) ;
		$( tdObj ).html( '<input class="form-control" style="z-index:1002;position:relative;" type="text" value="' + htmlEncode( proxy ) + '">' ) ;
	} ) ;
	//显示透明遮罩
	sdbjs.parts.screenBox.show( 'modifyScreen' ) ;
}

//扫描主机
function searchHostList()
{
	var hostString = $( '#hostSearchAddress' ).val() ;
	var user = $( '#hostSearchUser' ).val() ;
	var pwd = $( '#hostSearchPasswd' ).val() ;
	var ssh = $( '#hostSearchSSH' ).val() ;
	var proxy = $( '#hostSearchProxy' ).val() ;
	
	if( !checkString( hostString, 1, 1000 ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][0] ) ;//'地址的长度必须1-255范围内。'
		return;
	}
	if( !checkString( user, 1, 255 ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][1] ) ;//'用户名的长度必须1-255范围内。'
		return;
	}
	if( !checkString( pwd, 1, 255 ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][2] ) ;//'密码的长度在 1 - 255 范围。'
		return;
	}
	if( !checkPort( ssh ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][3] ) ;//'SSH端口格式错误。'
		return;
	}
	if( !checkPort( proxy ) )
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][4] ) ;//'代理端口格式错误。'
		return;
	}

	var hostListArr = parseHostString( hostString ) ;

	scanHostList( 0, hostListArr, user, pwd, ssh, proxy ) ;
}

$( document ).keydown( function(e){
	// 回车
	if( e.which === 13 )
	{
		var node = sdbjs.fun.getNode( 'modifyScreen', 'screenBox' ) ;
		if( !( $( node['obj'] ).is( ':hidden' ) ) )
		{
			var line = node['data'] ;
			reScanHost( line ) ;
		}
		else
		{
			var id = $( document.activeElement ).attr( 'id' ) ;
			if( id === 'hostSearchUser' || id === 'hostSearchPasswd' || id === 'hostSearchSSH' || id === 'hostSearchProxy' )
			{
				searchHostList() ;
			}
		}
	}
	// esc
	else if ( e.which === 27 )
	{
		var id = $( document.activeElement ).attr( 'id' ) ;
		if( id === 'hostSearchAddress' || id === 'hostSearchUser' || id === 'hostSearchPasswd' || id === 'hostSearchSSH' || id === 'hostSearchProxy' )
		{
			clearInput() ;
		}
	}
} ) ;

//返回
function returnPage()
{
	gotoPage( 'index.html' ) ;
}

// 下一步
function nextPage()
{
	function checkHostSearch()
	{
		var checkedHostLis = [] ;
		var len = _hostList.length ;
		for( var i = 0; i < len; ++i )
		{
			sdbjs.parts.gridBox.updateBody( 'hostSearchGrid', i, 0, function( tdObj ){
				//var className = $( tdObj ).children( 'div[data-toggle="checkBox"]' ).attr( 'class' ) ;
				//if( className === 'checked' || className === 'dischecked' )
				if( $( tdObj ).children( 'input' ).get(0).checked === true )
				{
					var hostname = _hostList[i]['HostInfo'][0]['HostName'] ;
					var ip       = _hostList[i]['HostInfo'][0]['IP'] ;
					var user     = _hostList[i]['User'] ;
					var pwd      = _hostList[i]['Passwd'] ;
					var ssh      = _hostList[i]['SshPort'] ;
					var proxy    = _hostList[i]['AgentService'] ;
					checkedHostLis.push( { 'HostName': hostname, 'IP': ip, 'User': user, 'Passwd': pwd, 'SshPort': ssh, 'AgentService': proxy } ) ;
				}
			} ) ;
		}
		return checkedHostLis ;
	}
	var checkedHostLis = checkHostSearch() ;
	if( checkedHostLis.length > 0 )
	{
		sdbjs.fun.delData( 'SdbHostConf' ) ;
		sdbjs.fun.saveData( 'SdbHostList', JSON.stringify( checkedHostLis ) ) ;
		if( _deployModel === 'AddHost' || _deployModel === 'Deploy' )
		{
			gotoPage( 'addhost.html' ) ;
		}
	}
	else
	{
		showFootStatus( 'danger', _languagePack['error']['web']['scanhost'][5] ) ;//您还没有选择主机，请至少选择一台主机。
	}
}

//加载主机列表
function loadHostList()
{
	var checkedHostLis = sdbjs.fun.getData( 'SdbHostList' ) ;
	if( checkedHostLis !== null )
	{
		checkedHostLis = JSON.parse( checkedHostLis ) ;
		$.each( checkedHostLis, function( index, value ){
			var hostname = value['HostName'] ;
			var user     = value['User'] ;
			var pwd      = value['Passwd'] ;
			var ssh      = value['SshPort'] ;
			var proxy    = value['AgentService'] ;
			var hostListArr = [ [ { 'HostName': hostname } ] ] ;
			scanHostList( 0, hostListArr, user, pwd, ssh, proxy ) ;
		} ) ;
	}
}

function createDynamicHtml()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	loadHostList() ;
	sdbjs.parts.loadingBox.hide( 'loading' ) ;
}

function createHtml()
{
	createPublicHtml() ;
	
	/* 创建一个全透明遮罩 */
	sdbjs.parts.screenBox.create( $( document.body ), 'modifyScreen' ) ;
	sdbjs.parts.screenBox.update( 'modifyScreen', 'unalpha' ) ;

	/* 分页 */
	sdbjs.parts.tabPageBox.create( 'top2', 'tab' ) ;
	sdbjs.fun.setCSS( 'tab', { 'padding-top': 5 } ) ;
	//'扫描主机'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/zoom.png"> ' + htmlEncode( _languagePack['public']['tabPage'][2] ), true, null ) ;
	if( _deployModel === 'AddHost' )
	{
		//'添加主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][3] ), false, null );
		//'安装主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][4] ), false, null );
	}
	if( _deployModel === 'Deploy' )
	{
		//'添加主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][3] ), false, null );
		//'安装主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][4] ), false, null );
		//'配置业务'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cube.png"> ' + htmlEncode( _languagePack['public']['tabPage'][5] ), false, null ) ;
		//'修改业务'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/doc_lines_stright.png"> ' + htmlEncode( _languagePack['public']['tabPage'][6] ), false, null );
		//'安装业务'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][7] ), false, null );
	}
	
	/* 左边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-left', 460, 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-left', { 'float': 'left', 'padding': '10px' } ) ;
	
	/* 左边 状态栏 */
	sdbjs.parts.panelBox.create( 'middle-left', 'hostSearchBar', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'hostSearchBar', { 'overflow': 'auto' } ) ;
	//'扫描主机'
	sdbjs.parts.panelBox.update( 'hostSearchBar', htmlEncode( _languagePack['scanhost']['leftPanel']['title'] ), function( panelBody ){
		sdbjs.parts.alertBox.create( panelBody['name'], 'hostSearchTips' ) ;
		sdbjs.fun.setCSS( 'hostSearchTips', { 'margin-bottom': 10 } ) ;
		//'提示：使用IP段或host段可以批量添加主机。详情请点击''帮助'
		sdbjs.parts.alertBox.update( 'hostSearchTips', sdbjs.fun.sprintf( '?<a href="#" data-toggle="modalBox" data-target="hostSearchHelp">?</a>', htmlEncode( _languagePack['scanhost']['leftPanel']['tip'][0] ), htmlEncode( _languagePack['scanhost']['leftPanel']['tip'][1] ) ), 'info' ) ;
		sdbjs.parts.tableBox.create( panelBody['name'], 'hostSearchTable' ) ;
		sdbjs.parts.tableBox.update( 'hostSearchTable', 'loosen' ) ;
		//'地址：'
		sdbjs.parts.tableBox.addBody( 'hostSearchTable', [ { 'text': htmlEncode( _languagePack['scanhost']['leftPanel']['input'][0] ), 'width': 80 },
																			{ 'text': '<textarea class="form-control" id="hostSearchAddress" rows="3"></textarea>', 'width': 314 } ] ) ;
		//'用户名：'
		sdbjs.parts.tableBox.addBody( 'hostSearchTable', [ { 'text': htmlEncode( _languagePack['scanhost']['leftPanel']['input'][1] ), 'width': 80 },
																			{ 'text': '<input class="form-control" type="text" id="hostSearchUser" value="root" disabled>' } ] ) ;
		//'密码：'
		sdbjs.parts.tableBox.addBody( 'hostSearchTable', [ { 'text': htmlEncode( _languagePack['scanhost']['leftPanel']['input'][2] ), 'width': 80 },
																			{ 'text': '<input class="form-control" type="password" id="hostSearchPasswd">' } ] ) ;
		//'SSH端口：'
		sdbjs.parts.tableBox.addBody( 'hostSearchTable', [ { 'text': htmlEncode( _languagePack['scanhost']['leftPanel']['input'][3] ), 'width': 80 },
																			{ 'text': '<input class="form-control" type="text" id="hostSearchSSH" value="22">' } ] ) ;
		//'代理端口：'
		sdbjs.parts.tableBox.addBody( 'hostSearchTable', [ { 'text': htmlEncode( _languagePack['scanhost']['leftPanel']['input'][4] ), 'width': 80 },
																			{ 'text': '<input class="form-control" type="text" id="hostSearchProxy" value="11790">' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostSearchTable', [ { 'colspan': 2, 'text': function( tdObj ){
			$( tdObj ).css( 'text-align', 'right' ) ;
			//'清空'
			$( tdObj ).append( '<button class="btn btn-primary" onClick="clearInput()">' + htmlEncode( _languagePack['scanhost']['leftPanel']['button'][0] ) + '</button>' ) ;
			$( tdObj ).append( '&nbsp;' ) ;
			//'扫描'
			$( tdObj ).append( '<button class="btn btn-primary" onclick="searchHostList()">' + htmlEncode( _languagePack['scanhost']['leftPanel']['button'][1] ) + '</button>' ) ;
		} } ] ) ;
	} ) ;
	
	/* 右边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-right', 'variable', 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-right', { 'float': 'left', 'padding': '10px', 'padding-left': 0 } ) ;
	
	/* 右边 主机列表 */
	sdbjs.parts.panelBox.create( 'middle-right', 'hostListBar', 'auto', 'variable' ) ;
	//'主机列表' 
	sdbjs.parts.panelBox.update( 'hostListBar', htmlEncode( _languagePack['scanhost']['rightPanel']['title'] ), function( panelBody ){
		sdbjs.parts.gridBox.create( panelBody['name'], 'hostSearchGrid', 'auto', 'variable' ) ;
		//'主机名' '地址' '用户名' '密码' 'SSH端口' '代理端口' '状态'
		sdbjs.parts.gridBox.addTitle( 'hostSearchGrid', [{ 'text': '', 'width': '5%' },
																		 { 'text': htmlEncode( _languagePack['scanhost']['rightPanel']['grid'][0] ), 'width': '15%' },
																		 { 'text': htmlEncode( _languagePack['scanhost']['rightPanel']['grid'][1] ), 'width': '15%' },
																		 { 'text': htmlEncode( _languagePack['scanhost']['rightPanel']['grid'][2] ), 'width': '10%' },
																		 { 'text': htmlEncode( _languagePack['scanhost']['rightPanel']['grid'][3] ), 'width': '10%' },
																		 { 'text': htmlEncode( _languagePack['scanhost']['rightPanel']['grid'][4] ), 'width': '8%' },
																		 { 'text': htmlEncode( _languagePack['scanhost']['rightPanel']['grid'][5] ), 'width': '8%' },
																		 { 'text': htmlEncode( _languagePack['scanhost']['rightPanel']['grid'][6] ), 'width': '15%' } ]  ) ;
	} ) ;
	
	/* ** */
	sdbjs.parts.divBox.create( 'middle', 'middle-clear', 0, 0 ) ;
	sdbjs.fun.setClass( 'middle-clear', 'clear-float' ) ;
	
	/* 创建帮助的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'hostSearchHelp' ) ;
	//'帮助'
	sdbjs.parts.modalBox.update( 'hostSearchHelp', htmlEncode( _languagePack['scanhost']['help']['title'] ), function( bodyObj ){
		//'请输入集群主机名称或IP地址，然后单击' '扫描' '。您还可以指定主机名称和IP地址范围'
		$( bodyObj ).append( sdbjs.fun.sprintf( '<p>?<b>?</b>?：</p>', htmlEncode( _languagePack['scanhost']['help']['body']['context_1'][0] ), htmlEncode( _languagePack['scanhost']['help']['body']['context_1'][1] ), htmlEncode( _languagePack['scanhost']['help']['body']['context_1'][2] ) ) ) ;
		sdbjs.parts.tableBox.create( bodyObj, 'hostSearchHelpTable' ) ;
		sdbjs.parts.tableBox.update( 'hostSearchHelpTable', 'loosen border' ) ;
		//'使用此延展范围' '要指定这些主机'
		sdbjs.parts.tableBox.addBody( 'hostSearchHelpTable', [ { 'text': '<b>' + htmlEncode( _languagePack['scanhost']['help']['body']['table']['title'][0] ) + '</b>' }, { 'text': '<b>' + htmlEncode( _languagePack['scanhost']['help']['body']['table']['title'][1] ) + '</b>' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostSearchHelpTable', [ { 'text': htmlEncode( '10.1.1.[1-4]' ) }, { 'text': htmlEncode( '10.1.1.1, 10.1.1.2, 10.1.1.3, 10.1.1.4' ) } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostSearchHelpTable', [ { 'text': htmlEncode( 'pc[1-4]host' ) }, { 'text': htmlEncode( 'pc1host, pc2host, pc3host, pc4host' ) } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostSearchHelpTable', [ { 'text': htmlEncode( 'pc[098-101]host' ) }, { 'text': htmlEncode( 'pc098host, pc099host, pc100host, pc101host' ) } ] ) ;
		//'您可以添加多个地址和地址范围，但要注意主机名称和IP是必须唯一的。'
		$( bodyObj ).append( '<p>' + htmlEncode( _languagePack['scanhost']['help']['body']['context_2'][0] ) + '</p>' ) ;
		//'扫描结果将包括所有扫描的地址，但只有运行SSH服务的主机的扫描才会被选择包含在集群中。如果是用户名、密码或SSH填写不正确，可以通过单击列表中需要修改的那一行进行修改。'
		$( bodyObj ).append( '<p>' + htmlEncode( _languagePack['scanhost']['help']['body']['context_2'][1] ) + '</p>' ) ;
		//'注意' '如果您不知道所有主机的地址，可输入更大的地址范围。但是范围越大，扫描时间越长。'
		$( bodyObj ).append( sdbjs.fun.sprintf( '<p><b>?</b>:?</p>', htmlEncode( _languagePack['scanhost']['help']['body']['context_2'][2] ), htmlEncode( _languagePack['scanhost']['help']['body']['context_2'][3] ) ) ) ;

	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'hostSearchClose' ) ;
		sdbjs.parts.buttonBox.update( 'hostSearchClose', function( buttonObj ){
			//'关闭'
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'hostSearchHelp' ) ;
		}, 'primary' ) ;
	} ) ;

	//返回 下一步
	sdbjs.parts.buttonBox.create( 'operate', 'deployReturn' ) ;
	sdbjs.parts.buttonBox.update( 'deployReturn', function( buttonObj ){
		//'返回'
		$( buttonObj ).text( _languagePack['public']['button']['return'] ) ;
		sdbjs.fun.addClick( buttonObj, 'returnPage()' ) ;
	}, 'primary' ) ;
	var operateNode = sdbjs.fun.getNode( 'operate', 'divBox' ) ;
	$( operateNode['obj'] ).append( '&nbsp;' ) ;
	sdbjs.parts.buttonBox.create( 'operate', 'deployNext' ) ;
	sdbjs.parts.buttonBox.update( 'deployNext', function( buttonObj ){
		//'下一步'
		$( buttonObj ).text( _languagePack['public']['button']['next'] ) ;
		sdbjs.fun.addClick( buttonObj, 'nextPage()' ) ;
	}, 'primary' ) ;

	$( '#hostSearchAddress' ).focus() ;
}

function checkReady()
{
	var rc = true ;
	_clusterName = sdbjs.fun.getData( 'SdbClusterName' ) ;
	if( _clusterName === null )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	_deployModel = sdbjs.fun.getData( 'SdbDeployModel' ) ;
	if( _deployModel === null || ( _deployModel !== 'AddHost' && _deployModel !== 'Deploy' ) )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	return rc ;
}

$(document).ready(function(){
	if( checkReady() )
	{
		sdbjs.fun.saveData( 'SdbStep', 'scanhost' ) ;
		createHtml() ;
		createDynamicHtml() ;
	}
} ) ;