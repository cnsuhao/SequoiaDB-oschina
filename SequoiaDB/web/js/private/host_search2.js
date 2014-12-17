//--------------------------------- 页面全局变量 -----------------------------------//

//主机列表
var _hostlist = [] ;
//主机配置参数列表
var _hostConf = [] ;
//全局安装路径
var _installPath = '' ;
//回车执行的函数
var _enterFunction = '' ;

//-------------------------------- 预加载 --------------------------------------//

$( document ).keydown( function(e){
	if( e.which == 13 )
	{
		eval( _enterFunction ) ;
	}
} ) ;

//--------------------------------- 页面函数 -----------------------------------//

/*
 * 搜索左边列表的主机
 */
function searchTabList()
{
	var value = $.trim( $( '#searchHostList' ).val() ) ;
	if( value == '' )
	{
		$( '#hostSwitchList' ).children().show() ;
	}
	else
	{
		$( '#hostSwitchList' ).children().show() ;
		
		$( '#hostSwitchList > li' ).each(function(index, element) {
			if( $( this ).children(':eq(1)').children(':eq(0)').text().indexOf( value ) == -1 && $( this ).children(':eq(1)').children(':eq(1)').text().indexOf( value ) == -1 )
			{
				$( this ).hide() ;
			}
			else
			{
				$( this ).show() ;
			}
		});
	}
}

/*
 * 获取cluster全局安装路径
 */
function getAllInstallPath()
{
	var clusterName = $.cookie( 'SdbClusterName' ) ;
	var order = { 'cmd': 'query cluster' } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		var len = jsonArr.length ;
		for( var i = 1; i < len; ++i )
		{
			if( jsonArr[i]['ClusterName'] == clusterName )
			{
				_installPath = jsonArr[1]['InstallPath'] ;
			}
		}
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 检测主机扫描至少有一台打勾
 */
function checkHostTable()
{
	var hasChecked = false ;
	$( '#hostGridList > .grid-body > .grid-tr' ).each(function(i) {
      $( this ).children( ':first-child' ).children( 'input' ).each(function() {
			_hostlist[i]['checked'] = ( $( this ).get(0).checked ) ;
			if( _hostlist[i]['checked'] == true )
			{
				hasChecked = true ;
			}
      });
   });
	return hasChecked ;
}

/*
 * 判断地址是否已经存在
 */
function addressIsExist( host, ip )
{
	var len = _hostlist.length ;
	for( var i = 0; i < len; ++i )
	{
		if( ( host != '' && host == _hostlist[ i ][ 'HostName' ] ) ||
		    ( ip != '' && ip == _hostlist[ i ][ 'IP' ] ) )
		{
			return true ;
		}
	}
	return false ;
}

/*
 * 添加主机到列表
 */
function addHostList()
{
	var value = [] ;
	$( '#scanTable .form-control' ).each(function() {
      value.push( $( this ).val() ) ;
   });
	var addressList = $.trim( value[0] ) ;
	var user = $.trim( value[1] ) ;
	var pwd = value[2] ;
	var ssh = $.trim( value[3] ) ;
	var agent = $.trim( value[4] ) ;
	
	if( !sdbjs.fun.checkString( addressList, 1, 255 ) )
	{
		showInfoFromFoot( 'danger', '地址字符长度必须1-255范围内!' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( user, 1, 255 ) )
	{
		showInfoFromFoot( 'danger', '用户名字符长度必须1-255范围内!' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( pwd, 1, 255 ) )
	{
		showInfoFromFoot( 'danger', '密码字符长度必须1-255范围内!' ) ;
		return;
	}
	if( !sdbjs.fun.checkPort( ssh ) )
	{
		showInfoFromFoot( 'danger', 'SSH端口格式错误!' ) ;
		return;
	}
	if( !sdbjs.fun.checkPort( agent ) )
	{
		showInfoFromFoot( 'danger', '代理端口格式错误!' ) ;
		return;
	}
	
	var list_obj = sdbjs.fun.getHostList( addressList );
	var clusterName = $.cookie( 'SdbClusterName' ) ;
	var order = '' ;
	var list_hosts_arr = list_obj ;
	var list_hosts_num = list_obj.length ;
	var list_hosts_index = 0 ;
	
	function getTheHostLink()
	{
		var tempHostInfo = [] ;
		for( var i = 0; i < 5 && list_hosts_index < list_hosts_num; ++i, ++list_hosts_index )
		{
			var temp = null ;
			if( list_hosts_arr[list_hosts_index]['type'] == 'ip' )
			{
				temp = { "IP": list_hosts_arr[list_hosts_index]['address'] } ;
			}
			else
			{
				temp = { "HostName": list_hosts_arr[list_hosts_index]['address'] } ;
			}
			tempHostInfo.push( temp ) ;
		}
		order = { 'cmd': 'scan host', 'HostInfo': JSON.stringify( { 'ClusterName': clusterName, 'HostInfo': tempHostInfo, 'User': user, 'Passwd': pwd, 'SshPort': ssh, 'AgentPort': agent } ) } ;
		ajaxSendMsg( order, true, function( jsonArr ){
			var hostInfoLen = jsonArr.length ;
			for( var i = 1; i < hostInfoLen; ++i )
			{
				var hostinfo = jsonArr[i] ;
				var temp = { 'HostName': ( hostinfo['HostName'] == null ? '' : hostinfo['HostName'] ), 'IP': ( hostinfo['IP'] == null ? '' : hostinfo['IP'] ), 'User': user, 'Passwd': pwd, 'SshPort': ssh, 'AgentPort': agent } ;
				var inputStr = '<input type="checkbox" checked="checked">' ;
				var desc = '' ;
				if( hostinfo['Ping'] == false )
				{
					desc = '网络连接错误' ;
					inputStr = '<input type="checkbox" disabled="disabled">' ;
				}
				else if( hostinfo['Ssh'] == false )
				{
					desc = 'SSH连接错误' ;
					inputStr = '<input type="checkbox" disabled="disabled">' ;
				}
				else if( hostinfo['errno'] != 0 )
				{
					desc = hostinfo['detail'] ;
					inputStr = '<input type="checkbox" disabled="disabled">' ;
				}
				else
				{
					desc = '连接成功' ;
				}
				var lineNum = _hostlist.length ;
				data_json = { 'cell': [ { 'text': inputStr },
												{ 'text': sdbjs.fun.htmlEncode( ( hostinfo['HostName'] == null ? '' : hostinfo['HostName'] ) ), 'event': 'openEditHostGrid(' + lineNum + ')' },
												{ 'text': sdbjs.fun.htmlEncode( hostinfo['IP'] == null ? '' : hostinfo['IP'] ), 'event': 'openEditHostGrid(' + lineNum + ')' },
												{ 'text': sdbjs.fun.htmlEncode( user ), 'event': 'openEditHostGrid(' + lineNum + ')' },
												{ 'text': sdbjs.fun.htmlEncode( '***' ), 'event': 'openEditHostGrid(' + lineNum + ')' },
												{ 'text': sdbjs.fun.htmlEncode( ssh ),	'event': 'openEditHostGrid(' + lineNum + ')' },
												{ 'text': sdbjs.fun.htmlEncode( agent ), 'event': 'openEditHostGrid(' + lineNum + ')' },
												{ 'text': sdbjs.fun.htmlEncode( desc ) } ] } ;
				if( !addressIsExist( temp['HostName'], temp['IP'] ) )
				{
					_hostlist.push( temp ) ;
					sdbjs.parts.gridBox.add( 'hostGridList', data_json ) ;
				}
			}
		}, function( jsonArr ){
			return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
		}, function(){
			if( list_hosts_index < list_hosts_num )
			{
				sdbjs.fun.setLoading( 'loading', parseInt(list_hosts_index/list_hosts_num*100) ) ;
				getTheHostLink() ;
			}
			else
			{
				sdbjs.fun.closeLoading( 'loading' ) ;
				$( '#scanTable .form-control' ).each(function(index) {
					if( index == 0 || index == 2 )
					{
						$( this ).val( '' ) ;
					}
				});
			}
		} ) ;
	}
	if( list_hosts_num > 0 )
	{
		sdbjs.fun.openLoading( 'loading' ) ;
		getTheHostLink() ;
	}
	else
	{
		showInfoFromFoot( 'danger', '地址格式错误!' ) ;
	}
}

/*
 * 提交主机列表
 */
function submitHostList()
{
	var clusterName = $.cookie( 'SdbClusterName' ) ;
	var sumHost = _hostConf.length ;
	var tempPoraryHosts = [] ;
	for( var i = 0; i < sumHost; ++i )
	{
		if( _hostConf[i]['checked'] != false && ( ( typeof( _hostConf[i]['errno'] ) == 'number' && _hostConf[i]['errno'] == 0 ) || ( _hostConf[i]['errno'] == undefined ) ) )
		{
			var tempHostInfo = {} ;
			tempHostInfo['HostName']  = _hostConf[i]['HostName'] ;
			tempHostInfo['IP']        = _hostConf[i]['IP'] ;
			tempHostInfo['User']      = _hostConf[i]['User'] ;
			tempHostInfo['Passwd']    = _hostConf[i]['Passwd'] ;
			tempHostInfo['SshPort']   = _hostConf[i]['SshPort'] ;
			tempHostInfo['AgentPort'] = _hostConf[i]['AgentPort'] ;
			tempHostInfo['CPU']       = _hostConf[i]['CPU'] ;
			tempHostInfo['Memory']    = _hostConf[i]['Memory'] ;
			tempHostInfo['Net']       = _hostConf[i]['Net'] ;
			tempHostInfo['Port']      = _hostConf[i]['Port'] ;
			tempHostInfo['Service']   = _hostConf[i]['Service'] ;
			tempHostInfo['OM']        = _hostConf[i]['OM'] ;
			tempHostInfo['Safety']    = _hostConf[i]['Safety'] ;
			tempHostInfo['OS']        = _hostConf[i]['OS'] ;
			tempHostInfo['Disk']      = [] ;
			var tempHostDiskLen = _hostConf[i]['Disk'].length ;
			for( var k = 0; k < tempHostDiskLen; ++k )
			{
				if( _hostConf[i]['Disk'][k]['CanUse'] == true && _hostConf[i]['Disk'][k]['checked'] != false )
				{
					var tempHostDisk = {} ;
					tempHostDisk['Name']    = _hostConf[i]['Disk'][k]['Name'] ;
					tempHostDisk['Mount']   = _hostConf[i]['Disk'][k]['Mount'] ;
					tempHostDisk['Size']    = _hostConf[i]['Disk'][k]['Size'] ;
					tempHostDisk['Free']    = _hostConf[i]['Disk'][k]['Free'] ;
					tempHostDisk['IsLocal'] = _hostConf[i]['Disk'][k]['IsLocal'] ;
					tempHostInfo['Disk'].push( tempHostDisk ) ;
				}
			}
			if( _hostConf[i]['InstallPath'] != undefined )
			{
				tempHostInfo['InstallPath'] = _hostConf[i]['InstallPath'] ;
			}
			tempPoraryHosts.push( tempHostInfo ) ;
		}
	}
	sdbjs.fun.openLoading( 'loading' ) ;
	sdbjs.fun.timeLoading( 'loading', tempPoraryHosts.length * 5 ) ;
	var order = { 'cmd': 'add host', 'HostInfo': JSON.stringify( { 'ClusterName': clusterName, 'HostInfo': tempPoraryHosts, 'User': '-', 'Passwd': '-', 'SshPort': '-', 'AgentPort': '-' } ) } ;
	ajaxSendMsg( order, true, function( jsonArr ){
		var SdbGuideOrder = $.cookie( 'SdbGuideOrder' ) ;
		if( SdbGuideOrder == 'Deployment' )
		{
			gotoPage( __Deployment[1] ) ;
		}
		else if( SdbGuideOrder == 'AddHost' )
		{
			var comeback = $.cookie( 'SdbComeback' ) ;
			if( comeback == undefined )
			{
				gotoPage( "index.html" ) ;
			}
			else
			{
				gotoPage( comeback ) ;
			}
		}
		else
		{
			gotoPage( "index.html" ) ;
		}
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	}, function(){
		sdbjs.fun.closeLoading( 'loading' ) ;
	} ) ;
}

/*
 * 切换显示
 */
function switchDisplay()
{
	$( '#leftPanel_1' ).toggle() ;
	$( '#rightPanel_1' ).toggle() ;
	$( '#leftPanel_2' ).toggle() ;
	$( '#rightPanel_2' ).toggle() ;
	if( $( '#leftPanel_1' ).is( ':hidden' ) )
	{
		$( '#__goBack' ).get(0).onclick = Function( 'switchDisplay()' ) ;
		$( '#__goOn' ).get(0).onclick = Function( 'submitHostList()' ) ;
		$( '#tab_box li' ).eq(0).removeClass() ;
		$( '#tab_box li' ).eq(1).addClass( 'active' ) ;
		sdbjs.fun.gridRevise( 'hostDiskList' ) ;
		sdbjs.fun.gridRevise( 'hostCPUList' ) ;
		sdbjs.fun.gridRevise( 'hostNetList' ) ;
		sdbjs.fun.gridRevise( 'hostPortList' ) ;
		sdbjs.fun.gridRevise( 'hostServiceList' ) ;
		sdbjs.fun.gridRevise( 'hostFirewallList' ) ;
	}
	else
	{
		$( '#__goBack' ).get(0).onclick = Function( 'gotoPage("index.html")' ) ;
		$( '#__goOn' ).get(0).onclick = Function( 'checkHostInfo()' ) ;
		$( '#tab_box li' ).eq(0).addClass( 'active' ) ;
		$( '#tab_box li' ).eq(1).removeClass() ;
		sdbjs.fun.gridRevise( 'hostGridList' ) ;
	}
}

/*
 * 选择主机
 */
function switchHost( num )
{
	sdbjs.parts.tabList.active( 'hostSwitchList', num ) ;
	$( '#switchHostButton' ).text( '取消选择主机' ).get(0).onclick = Function( 'unswitchHost(' + num + ')' ) ;
	_hostConf[num]['checked'] = true ;
}

/*
 * 取消选择主机
 */
function unswitchHost( num )
{
	sdbjs.parts.tabList.disable( 'hostSwitchList', num ) ;
	$( '#switchHostButton' ).text( '选择主机' ).get(0).onclick = Function( 'switchHost(' + num + ')' ) ;
	_hostConf[num]['checked'] = false ;
}

/*
 * 保存安装路径
 * 参数1 第几个主机
 */
function saveInstallPath()
{
	var hostNum = $( '#hostParaTable > tbody > tr:eq(4) > td:eq(1) > input' ).data( 'hostNum' ) ;
	_hostConf[hostNum]['InstallPath'] = $( '#hostParaTable > tbody > tr:eq(4) > td:eq(1) > input' ).val() ;
}

/*
 * 选择磁盘
 * 参数1 对象
 * 参数2 第几个主机
 * 参数3 第几个磁盘
 */
function switchDisk( checkObj, hostNum, diskNum )
{
	var num = parseInt( $( '#hostSwitchList > li:eq(' + hostNum + ') > div:eq(0) > span:eq(0)' ).text() ) ;
	if( checkObj.checked )
	{
		++num ;
	}
	else
	{
		--num ;
	}
	$( '#hostSwitchList > li:eq(' + hostNum + ') > div:eq(0) > span:eq(0)' ).text( num ).data( 'text', '已选择' + num + '个磁盘' ) ;
	_hostConf[hostNum]['Disk'][diskNum]['checked'] = checkObj.checked ;
}

/*
 * 显示主机信息
 */
function showHostData( num )
{
	if( sdbjs.parts.tabList.getStatus( 'hostSwitchList', num ) == 'active' )
	{
		return;
	}
	else if( sdbjs.parts.tabList.getStatus( 'hostSwitchList', num ) == 'unActive' || sdbjs.parts.tabList.getStatus( 'hostSwitchList', num ) == 'disabled' )
	{
		$( '#hostSwitchList > li' ).each(function(index, element) {
         if( !$( this ).hasClass( 'tag-list-row-off' ) )
			{
				sdbjs.parts.tabList.unActive( 'hostSwitchList', index ) ;
			}
      });
		if( sdbjs.parts.tabList.getStatus( 'hostSwitchList', num ) != 'disabled' )
		{
			sdbjs.parts.tabList.active( 'hostSwitchList', num ) ;
		}
	}
	$( '#hostDiskList > .grid-body' ).children().remove() ;
	$( '#hostCPUList > .grid-body' ).children().remove() ;
	$( '#hostNetList > .grid-body' ).children().remove() ;
	$( '#hostPortList > .grid-body' ).children().remove() ;
	$( '#hostServiceList > .grid-body' ).children().remove() ;
	$( '#hostFirewallList > .grid-body' ).children().remove() ;
	var hostdatas = _hostConf[ num ] ;
	var memorySize = parseInt( hostdatas['Memory']['Size'] ) ;
	var memoryUsed = memorySize - parseInt( hostdatas['Memory']['Free'] ) ;
	var memoryPercent = parseInt( memoryUsed / memorySize * 100 ) ;
	var memoryColor = '' ;
	if( memoryPercent < 70 )
	{
		memoryColor = 'green' ;
	}
	else if( memoryPercent < 90 )
	{
		memoryColor = 'orange' ;
	}
	else
	{
		memoryColor = 'red' ;
	}
	$( '#hostParaTable > tbody > tr:eq(0) > td:eq(1)' ).text( hostdatas['HostName'] ) ;
	$( '#hostParaTable > tbody > tr:eq(1) > td:eq(1)' ).text( hostdatas['IP'] ) ;
	$( '#hostParaTable > tbody > tr:eq(2) > td:eq(1)' ).text( hostdatas['OS']['Distributor'] + ' ' + hostdatas['OS']['Release'] + ' x' + hostdatas['OS']['Bit'] ) ;
	if( $( '#hostSwitchList' ).children( ':eq(' + num + ')' ).hasClass( 'tag-list-row-off' ) )
	{
		$( '#switchHostButton' ).text( '选择主机' ).get(0).onclick = Function( 'switchHost(' + num + ')' ) ;
	}
	else
	{
		$( '#switchHostButton' ).text( '取消选择主机' ).get(0).onclick = Function( 'unswitchHost(' + num + ')' ) ;
	}
	$( '#hostParaTable > tbody > tr:eq(4) > td:eq(1) > input' ).val( hostdatas['InstallPath'] == undefined ? _installPath : hostdatas['InstallPath'] ) ;
	$( '#hostParaTable > tbody > tr:eq(4) > td:eq(1) > input' ).on( 'input propertychange', saveInstallPath ).data( 'hostNum', num ) ; ;
	var hostDisk = hostdatas['Disk'] ;
	var diskWarning = 0 ;
	var diskLen = hostDisk.length ;
	for( var i = 0; i < diskLen; ++i )
	{
		var used   = parseInt( hostDisk[i]['Size'] ) - parseInt( hostDisk[i]['Free'] ) ;
		var total  = parseInt( hostDisk[i]['Size'] ) ;
		var percent = parseInt( used / total * 100 ) ;
		var color   = '' ;
		if( percent < 70 )
		{
			color = 'green' ;
		}
		else if( percent < 90 )
		{
			color = 'orange' ;
		}
		else
		{
			color = 'red' ;
		}
		var progress_str = '<div class="progress2"><span class="reading">' + used + 'MB&nbsp;/&nbsp;' + total + 'MB</span><span class="bar ' + color + '" style="width:' + percent + '%;"></span></div>' ;
		var inputStr = '' ;
		if( hostDisk[i]['CanUse'] == false )
		{
			inputStr = '<input type="checkbox" disabled="disabled">' ;
			++diskWarning ;
		}
		else
		{
			if( hostDisk[i]['checked'] != false )
			{
				inputStr = '<input type="checkbox" checked="checked" onclick="switchDisk(this,' + num + ',' + i + ')">' ;
			}
			else
			{
				inputStr = '<input type="checkbox" onclick="switchDisk(this,' + num + ',' + i + ')">' ;
			}
		}
		var data_json = { 'cell': [ { 'text': inputStr },
											 { 'text': hostDisk[i]['Name'] },
											 { 'text': hostDisk[i]['Mount'] },
											 { 'text': ( hostDisk[i]['IsLocal'] ? 'true' : 'false' ) },
											 { 'text': progress_str } ] } ;
		sdbjs.parts.gridBox.add( 'hostDiskList', data_json ) ;
	}
	var diskHeaderStr = '磁盘' ;
	if( diskWarning > 0 )
	{
		diskHeaderStr += '&nbsp;<span class="badge badge-warning">' + diskWarning + '</span></span>' ;
	}
	$( '#rightPanel_2 > .panel-header:eq(2)' ).html( diskHeaderStr ) ;
	
	var hostPort = hostdatas['Port'] ;
	var portLen = hostPort.length ;
	for( var i = 0; i < portLen; ++i )
	{
		var data_json = { 'cell': [ { 'text': hostPort[i]['Port'] },
											 { 'text': ( hostPort[i]['Status'] ? '已占用' : '可用' ) } ] } ;
		sdbjs.parts.gridBox.add( 'hostPortList', data_json ) ;
	}
	
	var hostCpu = hostdatas['CPU'] ;
	var cpuLen = hostCpu.length ;
	for( var i = 0; i < cpuLen; ++i )
	{
		var data_json = { 'cell': [ { 'text': hostCpu[i]['ID'] },
											 { 'text': hostCpu[i]['Model'] },
											 { 'text': hostCpu[i]['Core'] },
											 { 'text': hostCpu[i]['Freq'] } ] } ;
		sdbjs.parts.gridBox.add( 'hostCPUList', data_json ) ;
	}
	
	var hostNet = hostdatas['Net'] ;
	var netLen = hostNet.length ;
	for( var i = 0; i < netLen; ++i )
	{
		var data_json = { 'cell': [ { 'text': hostNet[i]['Name'] },
											 { 'text': hostNet[i]['Model'] },
											 { 'text': hostNet[i]['Bandwidth'] },
											 { 'text': hostNet[i]['IP'] } ] } ;
		sdbjs.parts.gridBox.add( 'hostNetList', data_json ) ;
	}
	
	var hostService = hostdatas['Service'] ;
	var serviceLen = hostService.length ;
	for( var i = 0; i < serviceLen; ++i )
	{
		var data_json = { 'cell': [ { 'text': hostService[i]['Name'] },
											 { 'text': ( hostService[i]['IsRunning'] ? 'true' : 'false' ) },
											 { 'text': hostService[i]['Version'] } ] } ;
		sdbjs.parts.gridBox.add( 'hostServiceList', data_json ) ;
	}
	
	var hostSafety = hostdatas['Safety'] ;
	{
		var data_json = { 'cell': [ { 'text': hostSafety['Name'] },
											 { 'text': ( hostSafety['IsRunning'] ? 'true' : 'false' ) },
											 { 'text': hostSafety['Context'] } ] } ;
		sdbjs.parts.gridBox.add( 'hostFirewallList', data_json ) ;
	}
	sdbjs.parts.progressBox.update( 'memoryProgress', memoryPercent, memoryColor, memoryUsed + 'MB / ' + memorySize + 'MB' ) ;
}

/*
 * 保存编辑的表格
 */
function saveEditHostGrid( num )
{
	var user  = $.trim( $( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(3)' ).children( 'input' ).val() ) ;
	var pwd   = $( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(4)' ).children( 'input' ).val() ;
	var ssh   = $.trim( $( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(5)' ).children( 'input' ).val() ) ;
	var agent = $.trim( $( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(6)' ).children( 'input' ).val() ) ;
	
	if( !sdbjs.fun.checkString( user, 1, 255 ) )
	{
		showInfoFromFoot( 'danger', '用户名字符长度必须1-255范围内!' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( pwd, 1, 255 ) )
	{
		showInfoFromFoot( 'danger', '密码字符长度必须1-255范围内!' ) ;
		return;
	}
	if( !sdbjs.fun.checkPort( ssh ) )
	{
		showInfoFromFoot( 'danger', 'SSH端口格式错误!' ) ;
		return;
	}
	if( !sdbjs.fun.checkPort( agent ) )
	{
		showInfoFromFoot( 'danger', '代理端口格式错误!' ) ;
		return;
	}
	
	_enterFunction = '' ;
	
	_hostlist[ num ]['User'] = user ;
	_hostlist[ num ]['Passwd'] = pwd ;
	_hostlist[ num ]['SshPort'] = ssh ;
	_hostlist[ num ]['AgentPort'] = agent ;
	
	var order ;
	var desc = '' ;
	var inputStr = '<input type="checkbox" checked="checked">' ;
	var clusterName = $.cookie( 'SdbClusterName' ) ;
	if( _hostlist[ num ]['IP'] != '' )
	{
		order = { 'cmd': 'scan host', 'HostInfo': JSON.stringify( { 'ClusterName': clusterName, 'HostInfo': [ { 'IP': _hostlist[ num ]['IP'] } ], 'User': user, 'Passwd': pwd, 'SshPort': ssh, 'AgentPort': agent } ) } ;
	}
	else
	{
		order = { 'cmd': 'scan host', 'HostInfo': JSON.stringify( { 'ClusterName': clusterName, 'HostInfo': [ { 'HostName': _hostlist[ num ]['HostName'] } ], 'User': user, 'Passwd': pwd, 'SshPort': ssh, 'AgentPort': agent } ) } ;
	}
	sdbjs.fun.openLoading( 'loading' ) ;
	sdbjs.fun.timeLoading( 'loading', 2 ) ;
	ajaxSendMsg( order, true, function( jsonArr ){
		var hostinfo = jsonArr[1] ;
		var temp = { 'HostName': ( hostinfo['HostName'] == null ? '' : hostinfo['HostName'] ), 'IP': ( hostinfo['IP'] == null ? '' : hostinfo['IP'] ), 'User': user, 'Passwd': pwd, 'SshPort': ssh, 'AgentPort': agent } ;
		if( hostinfo['Ping'] == false )
		{
			desc = '网络连接错误' ;
			inputStr = '<input type="checkbox" disabled="disabled">' ;
		}
		else if( hostinfo['Ssh'] == false )
		{
			desc = 'SSH连接错误' ;
			inputStr = '<input type="checkbox" disabled="disabled">' ;
		}
		else if( hostinfo['errno'] != 0 )
		{
			desc = hostinfo['detail'] ;
			inputStr = '<input type="checkbox" disabled="disabled">' ;
		}
		else
		{
			desc = '连接成功' ;
		}
		_hostlist[ num ]['HostName'] = temp['HostName'] ;
		_hostlist[ num ]['IP'] = temp['IP'] ;
		
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	}, function(){
		$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(0)' ).html( inputStr ) ;
		$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(1)' ).css( { 'z-index': 'auto', 'position': 'static' } ).text( _hostlist[ num ]['HostName'] ).get(0).onclick = Function( 'openEditHostGrid(' + num + ')' ) ;
		$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(2)' ).css( { 'z-index': 'auto', 'position': 'static' } ).text( _hostlist[ num ]['IP'] ).get(0).onclick = Function( 'openEditHostGrid(' + num + ')' ) ;
		$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(3)' ).css( { 'z-index': 'auto', 'position': 'static' } ).text( _hostlist[ num ]['User'] ).get(0).onclick = Function( 'openEditHostGrid(' + num + ')' ) ;
		$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(4)' ).css( { 'z-index': 'auto', 'position': 'static' } ).text( '***' ).get(0).onclick = Function( 'openEditHostGrid(' + num + ')' ) ;
		$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(5)' ).css( { 'z-index': 'auto', 'position': 'static' } ).text( _hostlist[ num ]['SshPort'] ).get(0).onclick = Function( 'openEditHostGrid(' + num + ')' ) ;
		$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(6)' ).css( { 'z-index': 'auto', 'position': 'static' } ).text( _hostlist[ num ]['AgentPort'] ).get(0).onclick = Function( 'openEditHostGrid(' + num + ')' ) ;
		$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(7)' ).text( desc ) ;
		sdbjs.fun.gridRevise( 'hostGridList' ) ;
		$( '#inputScreen' ).hide().get(0).onclick = Function( '' ) ;
		sdbjs.fun.closeLoading( 'loading' ) ;
	} ) ;
}

/*
 * 打开编辑表格
 */
function openEditHostGrid( num )
{
	_enterFunction = 'saveEditHostGrid(' + num + ')' ;
	$( '#inputScreen' ).show().get(0).onclick = Function( 'saveEditHostGrid(' + num + ')' ) ;
	$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(3)' ).css( { 'z-index': '701', 'position': 'relative' } ).html( '<input class="form-control" value="' + _hostlist[ num ]['User'] + '">' ).get(0).onclick = Function( '' ) ;
	$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(4)' ).css( { 'z-index': '701', 'position': 'relative' } ).html( '<input class="form-control" value="' + _hostlist[ num ]['Passwd'] + '">' ).get(0).onclick = Function( '' ) ;
	$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(5)' ).css( { 'z-index': '701', 'position': 'relative' } ).html( '<input class="form-control" value="' + _hostlist[ num ]['SshPort'] + '">' ).get(0).onclick = Function( '' ) ;
	$( '#hostGridList > .grid-body > .grid-tr:eq(' + num + ')' ).children( '.grid-td:eq(6)' ).css( { 'z-index': '701', 'position': 'relative' } ).html( '<input class="form-control" value="' + _hostlist[ num ]['AgentPort'] + '">' ).get(0).onclick = Function( '' ) ;
	sdbjs.fun.gridRevise( 'hostGridList' ) ;
}

/*
 * 进行主机检测
 */
function checkHostInfo()
{
	if( checkHostTable() )
	{
		switchDisplay() ;
		$( '#hostSwitchList' ).children().remove() ;
		_hostConf = [] ;
		var clusterName = $.cookie( 'SdbClusterName' ) ;
		var sumHost = _hostlist.length ;
		for( var i = 0; i < sumHost; ++i )
		{
			if( _hostlist[i]['checked'] == true )
			{
				var tempHostInfo = {} ;
				tempHostInfo['HostName']  = _hostlist[i]['HostName'] ;
				tempHostInfo['IP']        = _hostlist[i]['IP'] ;
				tempHostInfo['User']      = _hostlist[i]['User'] ;
				tempHostInfo['Passwd']    = _hostlist[i]['Passwd'] ;
				tempHostInfo['SshPort']   = _hostlist[i]['SshPort'] ;
				tempHostInfo['AgentPort'] = _hostlist[i]['AgentPort'] ;
				_hostConf.push( tempHostInfo ) ;
			}
		}
		sdbjs.fun.openLoading( 'loading' ) ;
		sdbjs.fun.timeLoading( 'loading', sumHost * 3 ) ;
		var order = { 'cmd': 'check host', 'HostInfo': JSON.stringify( { 'ClusterName': clusterName, 'HostInfo': _hostConf, 'User': '-', 'Passwd': '-', 'SshPort': '-', 'AgentPort': '-'} ) } ;
		ajaxSendMsg( order, true, function( jsonArr ){
			var hostListLen = _hostConf.length ;
			var host_len = jsonArr.length ;
			var errorDetail = [] ;
			for( var k = 0; k < hostListLen; ++k )
			{
				for( var i = 1; i < host_len; ++i )
				{
					if( _hostConf[k]['HostName'] == jsonArr[i]['HostName'] || _hostConf[k]['IP'] == jsonArr[i]['IP'] )
					{
						errorDetail[k] = '' ;
						if( !( typeof( jsonArr[i]['errno'] ) == 'number' && jsonArr[i]['errno'] != 0 ) )
						{
							_hostConf[k]['CPU']     = jsonArr[i]['CPU'] ;
							_hostConf[k]['Net']     = jsonArr[i]['Net'] ;
							_hostConf[k]['Memory']  = jsonArr[i]['Memory'] ;
							_hostConf[k]['Port']    = jsonArr[i]['Port'] ;
							_hostConf[k]['Service'] = jsonArr[i]['Service'] ;
							_hostConf[k]['OM']      = jsonArr[i]['OM'] ;
							_hostConf[k]['Safety']  = jsonArr[i]['Safety'] ;
							_hostConf[k]['OS']      = jsonArr[i]['OS'] ;
							_hostConf[k]['Disk']    = jsonArr[i]['Disk'] ;
						}
						else
						{
							errorDetail[k] = jsonArr[i]['detail'] ;
						}
						_hostConf[k]['errno'] = jsonArr[i]['errno'] ;
						break ;
					}
				}
			}
			var tab_data = [] ;
			var firstActive = -1 ;
			for( var i = 0; i < hostListLen; ++i )
			{
				var warningNum = 0 ;
				var useNum = 0 ;
				if( !( typeof( _hostConf[i]['errno'] ) == 'number' && _hostConf[i]['errno'] != 0 ) )
				{
					var disksLen = _hostConf[i]['Disk'].length ;
					for( var k = 0; k < disksLen; ++k )
					{
						if( _hostConf[i]['Disk'][k]['CanUse'] == false )
						{
							_hostConf[i]['Disk'][k]['checked'] = false ;
							++warningNum ;
						}
						else if ( _hostConf[i]['Disk'][k]['IsLocal'] == true )
						{
							_hostConf[i]['Disk'][k]['checked'] = true ;
							++useNum ;
						}
						else
						{
							_hostConf[i]['Disk'][k]['checked'] = false ;
						}
					}
					var leftListStatus = '<span class="badge badge-info" data-type="tooltip">' + useNum + '</span>' ;
					if( warningNum > 0 )
					{
						leftListStatus = leftListStatus + '&nbsp;<span class="badge badge-warning" data-type="tooltip">' + warningNum + '</span>' ;
					}
					sdbjs.parts.tabList.add( 'hostSwitchList', [ { 'name': _hostConf[i]['HostName'], 'sname': _hostConf[i]['IP'], 'event': 'showHostData(' + i + ')', 'text': leftListStatus } ] ) ;
					sdbjs.fun.addOnEvent( $( '#hostSwitchList > li:eq(' + i + ') > div:eq(0) > span:eq(0)' ).data( 'text', '已选择 ' + useNum + ' 个磁盘' ) ) ;
					if( warningNum > 0 )
					{
						sdbjs.fun.addOnEvent( $( '#hostSwitchList > li:eq(' + i + ') > div:eq(0) > span:eq(1)' ).data( 'text', '有 ' + warningNum + ' 个磁盘剩余容量不足' ) ) ;
					}
					if( firstActive < 0 )
					{
						firstActive = i ;
					}
				}
				else
				{
					sdbjs.parts.tabList.add( 'hostSwitchList', [ { 'name': _hostConf[i]['HostName'], 'sname': _hostConf[i]['IP'], 'text': '<span class="badge badge-danger" data-type="tooltip">error</span>' } ] ) ;
					sdbjs.fun.addOnEvent( $( '#hostSwitchList > li:eq(' + i + ') > div:eq(0) > span:eq(0)' ).data( 'text', errorDetail[i] ) ) ;
					sdbjs.parts.tabList.disable( 'hostSwitchList', i ) ;
				}
			}
			$( '#searchHostList' ).on( 'input propertychange', function(){ searchTabList() } ) ;
			if( firstActive >= 0 )
			{
				showHostData( firstActive ) ;
			}
		}, function( jsonArr ){
			return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
		}, function(){
			sdbjs.fun.closeLoading( 'loading' ) ;
		} ) ;
	}
	else
	{
		showInfoFromFoot( 'danger', '没有选择机器!' ) ;
	}
}

$(document).ready(function()
{
	if( $.cookie( 'SdbUser' ) == undefined || $.cookie( 'SdbPasswd' ) == undefined || $.cookie( 'SdbClusterName' ) == undefined || $.cookie( 'SdbGuideOrder' ) == undefined )
	{
		gotoPage( 'index.html' ) ;
		return;
	}
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlBody' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	if( navigator.cookieEnabled == false )
	{
		$( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ).html( '<div id="cookieBox" style="padding-top: 317px;"><div class="alert alert-danger" id="cookie_alert_box" style="width: 800px; margin-left: auto; margin-right: auto;">您的浏览器禁止使用Cookie,系统将不能正常使用，请设置浏览器启用Cookie，并且刷新或重新打开浏览器。</div></div>' ) ;
		sdbjs.fun.autoCorrect( { 'id': 'cookieBox', 'style': { 'paddingTop': 'parseInt( ( sdbjs.public.height - 131 ) / 2 - 25 )' } } ) ;
	}
	else
	{
		$( '<div></div>' ).attr( 'id', 'inputScreen' ).addClass( 'mask-screen mask-screen-not-alpha' ).css( 'z-index', 700 ).appendTo( $( 'body' ) ) ;
		//------------ 导航和底部 -------------//
		var SdbGuideOrder = $.cookie( 'SdbGuideOrder' ) ;
		if( SdbGuideOrder == 'Deployment' )
		{
			$( '#__goBack' ).get(0).onclick = Function( 'gotoPage("index.html")' ) ;
			$( '#__goOn' ).get(0).onclick = Function( 'checkHostInfo()' ) ;
		}
		else if( SdbGuideOrder == 'AddHost' )
		{
			$( '#__goBack' ).get(0).onclick = Function( 'gotoPage("index.html")' ) ;
			$( '#__goOn' ).get(0).onclick = Function( 'checkHostInfo()' ) ;
		}
		else
		{
			gotoPage( "index.html" ) ;
		}
		var guideLen = __processPic[SdbGuideOrder].length ;
		for( var i = 0; i < guideLen; ++i )
		{
			if( i == 0 )
			{
				$( '#tab_box' ).append( '<li class="active">' + __processPic[SdbGuideOrder][i] + '</li>' ) ;
			}
			else
			{
				$( '#tab_box' ).append( '<li>' + __processPic[SdbGuideOrder][i] + '</li>' ) ;
			}
		}
		
		sdbjs.fun.autoCorrect( { 'obj': $( '#bodyTran' ).children( ':first-child' ).children( ':eq(0)' ), 'style': { 'width': 'parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#bodyTran' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		
		//------------ 扫描主机 -------------//
		sdbjs.fun.autoCorrect( { 'id': 'leftPanel_1', 'style': { 'height': 'sdbjs.public.height - 151' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'rightPanel_1', 'style': { 'height': 'sdbjs.public.height - 151' } } ) ;
		sdbjs.parts.gridBox.create( 'hostGridList', {},  [ [ '', '主机名', '地址', '用户名', '密码', 'SSH端口', '代理端口', '状态' ] ], [ 8, 30, 25, 20, 20, 15, 15, 20 ] ).appendTo( $( '#rightPanel_1 > .panel-body' ) ) ;
		sdbjs.fun.autoCorrect( { 'id': 'hostGridList', 'style': { 'maxHeight': 'sdbjs.public.height - 211' } } ) ;
		sdbjs.fun.gridRevise( 'hostGridList' ) ;
		sdbjs.parts.loadingBox.create( 'loading' ) ;
		
		//------------ 添加主机 -------------//
		sdbjs.fun.autoCorrect( { 'id': 'leftPanel_2', 'style': { 'height': 'sdbjs.public.height - 151' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'rightPanel_2', 'style': { 'height': 'sdbjs.public.height - 151' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'hostSwitchList', 'style': { 'maxHeight': 'sdbjs.public.height - 261' } } ) ;
		sdbjs.parts.gridBox.create( 'hostDiskList', { 'maxHeight': 237, 'minHeight': 28, 'position': 'relative' },  [ [ '', '磁盘', '路径', '本地设备', '容量' ] ], [ 5, 10, 25, 20, 30 ] ).appendTo( $( '#rightPanel_2 > .panel-body' ).eq(2) ) ;
		sdbjs.parts.gridBox.create( 'hostCPUList', { 'maxHeight': 206, 'minHeight': 28, 'position': 'relative' },  [ [ 'ID', '类型', '核心数', '主频' ] ], [ 3, 2, 1, 1 ] ).appendTo( $( '#rightPanel_2 > .panel-body' ).eq(3) ) ;
		sdbjs.parts.gridBox.create( 'hostNetList', { 'maxHeight': 206, 'minHeight': 28, 'position': 'relative' },  [ [ 'ID', '类型', '速率', 'IP' ] ], [ 1, 1, 1, 1 ] ).appendTo( $( '#rightPanel_2 > .panel-body' ).eq(4) ) ;
		sdbjs.parts.gridBox.create( 'hostPortList', { 'maxHeight': 206, 'minHeight': 28, 'position': 'relative' },  [ [ '端口', '状态' ] ], [ 1, 1 ] ).appendTo( $( '#rightPanel_2 > .panel-body' ).eq(5) ) ;
		sdbjs.parts.gridBox.create( 'hostServiceList', { 'maxHeight': 206, 'minHeight': 28, 'position': 'relative' },  [ [ '服务名', '是否运行', '版本' ] ], [ 1, 1, 1 ] ).appendTo( $( '#rightPanel_2 > .panel-body' ).eq(6) ) ;
		sdbjs.parts.gridBox.create( 'hostFirewallList', { 'maxHeight': 206, 'minHeight': 28, 'position': 'relative' },  [ [ 'Name', '是否运行', '描述' ] ], [ 1, 1, 1 ] ).appendTo( $( '#rightPanel_2 > .panel-body' ).eq(7) ) ;
		
		getAllInstallPath() ;
		
		var browser = sdbjs.fun.getBrowserInfo() ;
		if( browser[0] == 'ie' && browser[1] <= 7 )
		{
			$( '#searchHostList' ).removeAttr( 'style' ) ;
		}
		setUser() ;
	}
	
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ), 'style': { 'height': 'sdbjs.public.height - 131' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - 428' } } ) ;
	
	sdbjs.fun.endOfCreate() ;
	
	$( '#scanTable .form-control' ).first().get(0).focus() ;
});