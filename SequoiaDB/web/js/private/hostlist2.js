//--------------------------------- 页面全局变量 -----------------------------------//

//主机列表
var _hostList = [] ;

//--------------------------------- 页面函数 -----------------------------------//

/*
 * 跳转添加主机
 */
function gotoHostSearch()
{
	$.cookie( 'SdbComeback', 'hostlist.html' ) ;
	$.cookie( 'SdbGuideOrder', 'AddHost' ) ;
	gotoPage( 'host_search.html' ) ;
}

/*
 * 全选主机
 */
function checkAllHost()
{
	$( '#hostListGrid > .grid-body > .grid-tr' ).each(function(index, element) {
      $( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked = true ;
   });
}

/*
 * 反选主机
 */
function inverseCheckAllHost()
{
	$( '#hostListGrid > .grid-body > .grid-tr' ).each(function(index, element) {
      if( $( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked )
		{
			$( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked = false ;
		}
		else
		{
			$( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked = true ;
		}
   });
}

/*
 * 删除主机
 */
function removeHost()
{
	sdbjs.fun.closeModal( 'removeHostModal' ) ;
	sdbjs.fun.openLoading( 'loading' ) ;
	var deleteHostList = [] ;
	var deleteResult = [] ;
	$( '#hostListGrid > .grid-body > .grid-tr' ).each(function(index, element) {
      if( $( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked )
		{
			deleteHostList.push( _hostList[index]['HostName'] ) ;
			deleteResult.push( { 'errno': 0, 'detail': '' } ) ;
		}
   });
	
	var len = deleteHostList.length ;
	if( len > 0 )
	{
		sdbjs.fun.timeLoading( 'loading', len * 2 ) ;
		var i = 0 ;
		function removeHostSub()
		{
			var order = { 'cmd': 'remove host', 'HostName': deleteHostList[i] } ;
			ajaxSendMsg( order, true, function( jsonArr ){
				deleteResult[i]['errno'] = 0 ;
				deleteResult[i]['detail'] = '删除成功' ;
			}, function( jsonArr ){
				if( jsonArr[0]['errno'] == -179 )
				{
					return true ;
				}
				else
				{
					deleteResult[i]['errno'] = jsonArr[0]['errno'] ;
					deleteResult[i]['detail'] = jsonArr[0]['detail'] ;
				}
			}, function(){
				if( i + 1 == len )
				{
					sdbjs.fun.closeLoading( 'loading' ) ;
					var imgStr = '' ;
					for( var k = 0; k < len; ++k )
					{
						if( deleteResult[k]['errno'] == 0 )
						{
							imgStr = '<img class="icon" src="./images/tick.png">' ;
						}
						else
						{
							imgStr = '<img class="icon" src="./images/delete.png">' ;
						}
						sdbjs.parts.gridBox.add( 'removehostListGrid', { 'cell': [ { 'text': imgStr }, { 'text': deleteHostList[k] }, { 'text': deleteResult[k]['detail'] } ] } ) ;
					}
					sdbjs.fun.openModal( 'removeHostResultModal' ) ;
					sdbjs.fun.gridRevise( 'removehostListGrid' ) ;
					sdbjs.fun.moveModal( 'removeHostResultModal' ) ;
				}
				else
				{
					++i ;
					removeHostSub() ;
				}
			} ) ;
		}
		removeHostSub() ;
	}
	else
	{
		sdbjs.fun.closeLoading( 'loading' ) ;
	}
}

/*
 * 打开删除主机模态框
 */
function openRemoveHostModal()
{
	sdbjs.fun.openModal( 'removeHostModal' ) ;
}

/*
 * 获取主机列表
 */
function getHostList()
{
	function getHostConfig()
	{
		var len = _hostList.length ;
		for( var i = 0; i < len; ++i )
		{
			var order = { 'cmd': 'query host', 'HostName': _hostList[i]['HostName'] } ;
			ajaxSendMsg( order, false, function( jsonArr ){
				_hostList[i]['OM'] = jsonArr[1]['OM'] ;
				_hostList[i]['OS'] = jsonArr[1]['OS'] ;
				_hostList[i]['Memory'] = jsonArr[1]['Memory'] ;
				_hostList[i]['InstallPath'] = jsonArr[1]['InstallPath'] ;
				_hostList[i]['AgentPort'] = jsonArr[1]['AgentPort'] ;
				_hostList[i]['CPU'] = 0 ;
				var cpuLen = jsonArr[1]['CPU'].length ;
				for( var k = 0; k < cpuLen; ++k )
				{
					_hostList[i]['CPU'] += jsonArr[1]['CPU'][k]['Core'] ;
				}
			} ) ;
		}
	}
	function getHostStatus()
	{
		var len = _hostList.length ;
		for( var i = 0; i < len; ++i )
		{
			var order = { 'cmd': 'query host status', 'HostInfo': JSON.stringify( { 'HostInfo': [ { 'HostName': _hostList[i]['HostName'] } ] } ) } ;
			ajaxSendMsg( order, false, function( jsonArr ){
				if( jsonArr[1]['HostInfo'][0]['errno'] == undefined || jsonArr[1]['HostInfo'][0]['errno'] == 0 )
				{
					_hostList[i]['errno'] = 0 ;
				}
				else
				{
					_hostList[i]['errno'] = jsonArr[1]['HostInfo'][0]['errno'] ;
				}
			} ) ;
		}
	}
	var clusterName = $.cookie( 'SdbClusterName' ) ;
	var order = { 'cmd': 'list host', 'ClusterName': clusterName } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		var len = jsonArr.length ;
		for( var i = 1; i < len; ++i )
		{
			_hostList.push( jsonArr[i] ) ;
		}
		getHostConfig() ;
		getHostStatus() ;
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 创建主机列表
 */
function createHostList()
{
	var hostLen = _hostList.length ;
	var i = 0 ;
	function createHostListT()
	{
		var imgStr = '' ;
		if( _hostList[i]['errno'] == 0 )
		{
			imgStr = '<img src="./images/tick.png">' ;
		}
		else
		{
			imgStr = '<img src="./images/delete.png">' ;
		}
		sdbjs.parts.gridBox.add( 'hostListGrid', { 'cell': [ { 'text': '<input type="checkbox">' }, { 'text': imgStr }, { 'text': sdbjs.fun.htmlEncode( _hostList[i]['HostName'] ) }, { 'text': sdbjs.fun.htmlEncode( _hostList[i]['IP'] ) }, { 'text': sdbjs.fun.htmlEncode( _hostList[i]['InstallPath'] ) }, { 'text': sdbjs.fun.htmlEncode( _hostList[i]['OM']['Version'] ) }, { 'text': sdbjs.fun.htmlEncode( _hostList[i]['AgentPort'] ) }, { 'text': sdbjs.fun.htmlEncode( _hostList[i]['OS']['Distributor'] + ' ' + _hostList[i]['OS']['Release'] + ' x' + _hostList[i]['OS']['Bit'] ) }, { 'text': sdbjs.fun.htmlEncode( _hostList[i]['CPU'] ) }, { 'text': sdbjs.fun.htmlEncode( _hostList[i]['Memory']['Size'] + 'MB' ) } ] } ) ;
				
		++i ;
		sdbjs.fun.setLoading( 'loading', parseInt( i / hostLen * 100 ) ) ;
		if( i < hostLen )
		{
			setTimeout( createHostListT, 1 ) ;
		}
		else
		{
			sdbjs.fun.closeLoading( 'loading' ) ;
		}
	}
	if( hostLen > 0 )
	{
		sdbjs.fun.openLoading( 'loading' ) ;
		setTimeout( createHostListT, 0 ) ;
	}
}

$(document).ready(function()
{
	if( $.cookie( 'SdbUser' ) == undefined || $.cookie( 'SdbPasswd' ) == undefined || $.cookie( 'SdbClusterName' ) == undefined )
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
		sdbjs.parts.loadingBox.create( 'loading' ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#bodyTran' ).children( ':first-child' ).children( ':eq(0)' ), 'style': { 'width': 'parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#bodyTran' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		
		sdbjs.fun.autoCorrect( { 'obj': $( '#hostListPanel' ), 'style': { 'height': 'sdbjs.public.height - 155' } } ) ;
		
		sdbjs.parts.gridBox.create( 'hostListGrid', {}, [ [ '', '状态', '主机', 'IP', '安装路径', 'OM版本', '代理端口', '系统', 'CPU核心数', '内存' ] ], [ 2, 3, 5, 5, 5, 5, 5, 5, 5, 5 ] ).appendTo( $( '#hostListPanel > .panel-body > div:eq(1)' ) ) ;
		$( '#hostListPanel > .panel-header' ).text( '集群：' + $.cookie( 'SdbClusterName' ) ) ;
		sdbjs.fun.gridRevise( 'hostListGrid' ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#hostListGrid' ), 'style': { 'maxHeight': 'sdbjs.public.height - 280' } } ) ;
		
		getHostList() ;
		createHostList() ;
		setUser() ;
		
		sdbjs.parts.gridBox.create( 'removehostListGrid', {}, [ [ '', '主机', '结果' ] ], [ 1, 5, 5 ] ).appendTo( $( '#removeHostResultModal > .modal-body' ) ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#removehostListGrid' ), 'style': { 'max-height': 'parseInt(sdbjs.public.height * 0.8) - 130' } } ) ;
	}
	
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ), 'style': { 'height': 'sdbjs.public.height - 131' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - 428' } } ) ;
	
	sdbjs.fun.endOfCreate() ;
});