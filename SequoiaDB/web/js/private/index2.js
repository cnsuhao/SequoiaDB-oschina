//--------------------------------- 页面函数 -----------------------------------//

//图对象
var rightPic = [] ;
//上次选择的clusterName
var oldCluster = null ;
//当前选择的clusterName
var cursorCluster = null ;
//主机列表
var hostList = [] ;
//主机的旧值
var oldHostConf = {} ;

//----------- 创建集群 ----------//

/*
 * 发送创建集群命令
 */
function createCluster()
{
	var value = [] ;
	$( '#createCluster > .modal-body input' ).each(function(index, element) {
      value.push( $( this ).val() ) ;
   });
	
	value[0] = $.trim( value[0] ) ;
	value[1] = $.trim( value[1] ) ;
	value[3] = $.trim( value[3] ) ;
	value[4] = $.trim( value[4] ) ;
	
	if( !sdbjs.fun.checkStrName( value[0] ) )
	{
		$( '#createClusterAdvanced' ).children( ':eq(1)' ).text( '集群名必须以下划线或英文字母开头，只含有下划线英文字母数字，长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( value[1], 1, 255 ) )
	{
		$( '#createClusterAdvanced' ).children( ':eq(1)' ).text( '用户名长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( value[2], 1, 255 ) )
	{
		$( '#createClusterAdvanced' ).children( ':eq(1)' ).text( '密码长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkStrName( value[3] ) )
	{
		$( '#createClusterAdvanced' ).children( ':eq(1)' ).text( '用户组必须以下划线或英文字母开头，只含有下划线英文字母数字，长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( value[4], 1, 1024 ) )
	{
		$( '#createClusterAdvanced' ).children( ':eq(1)' ).text( '安装路径长度在 1 - 1024 范围。' ) ;
		return;
	}
	var order = { 'cmd': 'create cluster', 'ClusterInfo': JSON.stringify( { 'ClusterName': value[0], 'SdbUser': value[1], 'SdbPasswd': value[2], 'SdbUserGroup': value[3], 'InstallPath': value[4] } ) } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		gotoPage( 'index.html' ) ;
	}, function( jsonArr ){
		if( jsonArr[0]['errno'] == -179 )
		{
			var user = $.cookie( 'SdbUser' ) ;
			var pwd  = $.cookie( 'SdbPasswd' ) ;
			if( user != undefined && pwd != undefined )
			{
				loginOM( user, pwd ) ;
				return true ;
			}
		}
		else
		{
			$( '#createClusterAdvanced' ).children( ':eq(1)' ).text( jsonArr[0]['detail'] ) ;
		}
		return false ;
	} ) ;
}

/*
 * 打开创建集群的模态框
 */
function openCreateClusterModal()
{
	$( '#createClusterAdvanced' ).children( ':eq(1)' ).text( '' ) ;
	sdbjs.fun.openModal( 'createCluster' ) ;
}

/*
 * 跳转到添加业务
 */
function gotoBusiness()
{
	var value = [] ;
	$( '#addbusinessModal > .modal-body .form-control' ).each(function(index, element) {
      value.push( $( this ).val() ) ;
   });
	if( !sdbjs.fun.checkStrName( value[0] ) )
	{
		$( '#addbusinessModal > .modal-body' ).children( ':eq(1)' ).text( '业务名必须以下划线或英文字母开头，只含有下划线英文字母数字，长度在 1 - 255 范围。' ) ;
		return;
	}
	$.cookie( 'SdbGuideOrder', 'AddBusiness' ) ;
	$.cookie( 'SdbClusterName', $( '#addbusinessModal' ).data( 'clusterName' ) ) ;
	$.cookie( 'SdbBusinessName', value[0] ) ;
	$.cookie( 'SdbBusinessType', value[1] ) ;
	gotoPage( 'business_sdb.html' ) ;
}

/*
 * 打开添加业务
 */
function openAddBusinessModal( clusterName )
{
	$( '#addbusinessModal' ).data( 'clusterName', clusterName ) ;
	$( '#addbusinessModal > .modal-body' ).children( ':eq(1)' ).text( '' ) ;
	sdbjs.fun.openModal( 'addbusinessModal' ) ;
}

//----------- 部署引导 ----------//

/*
 * 发送创建集群命令
 */
function guideStart()
{
	var value = [] ;
	$( '#deploymentGuide > .modal-body .form-control' ).each(function(index, element) {
      value.push( $( this ).val() ) ;
   });
	
	value[0] = $.trim( value[0] ) ;
	value[1] = $.trim( value[1] ) ;
	value[3] = $.trim( value[3] ) ;
	value[5] = $.trim( value[5] ) ;
	value[6] = $.trim( value[6] ) ;
	
	if( !sdbjs.fun.checkStrName( value[0] ) )
	{
		$( '#deploymentGuideAdvanced' ).children( ':eq(1)' ).text( '集群名必须以下划线或英文字母开头，只含有下划线英文字母数字，长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkStrName( value[1] ) )
	{
		$( '#deploymentGuideAdvanced' ).children( ':eq(1)' ).text( '业务名必须以下划线或英文字母开头，只含有下划线英文字母数字，长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( value[3], 1, 255 ) )
	{
		$( '#deploymentGuideAdvanced' ).children( ':eq(1)' ).text( '用户名长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( value[4], 1, 255 ) )
	{
		$( '#deploymentGuideAdvanced' ).children( ':eq(1)' ).text( '密码长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkStrName( value[5] ) )
	{
		$( '#deploymentGuideAdvanced' ).children( ':eq(1)' ).text( '用户组必须以下划线或英文字母开头，只含有下划线英文字母数字，长度在 1 - 255 范围。' ) ;
		return;
	}
	if( !sdbjs.fun.checkString( value[6], 1, 1024 ) )
	{
		$( '#deploymentGuideAdvanced' ).children( ':eq(1)' ).text( '安装路径长度在 1 - 1024 范围。' ) ;
		return;
	}
	var order = { 'cmd': 'create cluster', 'ClusterInfo': JSON.stringify( { 'ClusterName': value[0], 'SdbUser': value[3], 'SdbPasswd': value[4], 'SdbUserGroup': value[5], 'InstallPath': value[6] } ) } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		$.cookie( 'SdbGuideOrder', 'Deployment' ) ;
		$.cookie( 'SdbClusterName', value[0] ) ;
		$.cookie( 'SdbBusinessName', value[1] ) ;
		$.cookie( 'SdbBusinessType', value[2] ) ;
		gotoPage( 'host_search.html' ) ;
	}, function( jsonArr ){
		if( jsonArr[0]['errno'] == -179 )
		{
			var user = $.cookie( 'SdbUser' ) ;
			var pwd  = $.cookie( 'SdbPasswd' ) ;
			if( user != undefined && pwd != undefined )
			{
				loginOM( user, pwd ) ;
				return true ;
			}
		}
		else
		{
			$( '#deploymentGuideAdvanced' ).children( ':eq(1)' ).text( jsonArr[0]['detail'] ) ;
		}
		return false ;
	} ) ;
}

/*
 * 删除集群
 */
function removeCluster( clusterName )
{
	var order = { 'cmd': 'remove cluster', 'ClusterName': clusterName } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		gotoPage('index.html') ;
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	}) ;
}

/*
 * 打开创建集群的模态框
 */
function openDeploymentGuideModal()
{
	$( '#deploymentGuideAdvanced' ).children( ':eq(1)' ).text( '' ) ;
	sdbjs.fun.openModal( 'deploymentGuide' ) ;
}

/*
 * 添加主机
 */
function gotoAddHost( clusterName )
{
	$.cookie( 'SdbGuideOrder', 'AddHost' ) ;
	$.cookie( 'SdbClusterName', clusterName ) ;
	gotoPage( 'host_search.html' ) ;
}

/*
 * 主机列表
 */
function gotoHostList( clusterName )
{
	$.cookie( 'SdbClusterName', clusterName ) ;
	gotoPage( 'hostlist.html' ) ;
}

/*
 * 业务列表
 */
function gotoBusinessList( clusterName )
{
	$.cookie( 'SdbClusterName', clusterName ) ;
	gotoPage( 'businesslist.html' ) ;
}

//--------------------------------- 页面加载 -----------------------------------//

/*
 * 创建集群的业务列表
 */
function createBusiness( clusterName, tableID )
{
	var order = { 'cmd': 'list business', 'ClusterName': clusterName } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		var len = jsonArr.length ;
		for( var i = 1; i < len; ++i )
		{
			ajaxSendMsg( { 'cmd': 'query business', 'BusinessName': jsonArr[i]['BusinessName'] }, false, function( jsonArr ){} ) ;
			sdbjs.parts.tableBox.add( tableID, [ { 'cell': [ { 'text': '<div style="text-indent:2em;">' + sdbjs.fun.htmlEncode( jsonArr[i]['BusinessName'] ) + '</div>' }, { 'text': '' } ] } ] ) ;
		}
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 设置右边图的cluster
 */
function setCursorCluster( clusterName )
{
	cursorCluster = clusterName ;
}

/*
 * 获取主机数量
 */
function updateRightPic( clusterName )
{
	var hostLen = 0 ;
	var order = { 'cmd': 'list host', 'ClusterName': clusterName } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		hostLen = jsonArr.length - 1 ;
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
	return hostLen ;
}

/*
 * 创建集群列表, 并且创建左边面板的集群列表
 */
function createClusterList()
{
	var order = { 'cmd': 'query cluster' } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		var clusterInfo = jsonArr ;
		var clusterLen = clusterInfo.length ;
		for( var i = 1; i < clusterLen; ++i )
		{
			var clusterJson = clusterInfo[i] ;
			if( i == 1 )
			{
				cursorCluster = clusterJson['ClusterName'] ;
			}
			sdbjs.parts.foldBox.create( 'clusterFold_' + i, {}, sdbjs.fun.htmlEncode( clusterJson['ClusterName'] ), '', ( i == 1 ), true, 'cluster' ).appendTo( $( '#leftPanel > .panel-body' ) ) ;
			sdbjs.parts.dropDownBox.create( 'clusterDropDown_' + i, { 'float': 'right' }, '', [ { 'text': '添加主机', 'event': 'gotoAddHost("' + clusterJson['ClusterName'] + '")' }, { 'text': '主机列表', 'event': 'gotoHostList("' + clusterJson['ClusterName'] + '")' }, { 'text': '' }, { 'text': '添加业务', 'event': 'openAddBusinessModal("' + clusterJson['ClusterName'] + '")' }, { 'text': '业务列表', 'event': 'gotoBusinessList("' + clusterJson['ClusterName'] + '")' }, { 'text': '' }, { 'text': '删除集群', 'event': 'removeCluster("' + clusterJson['ClusterName'] + '")' } ], 'lg' ).prependTo( $( '#clusterFold_' + i + ' > .fold-header' ) ) ;
			$( '#clusterFold_' + i + ' > .fold-header > .fold-point' ).get(0).onclick = Function( 'setCursorCluster("' + clusterJson['ClusterName'] + '")' ) ;
			sdbjs.parts.tableBox.create( 'clusterTable_' + i, { 'marginTop': '5px' }, 'simple' ).appendTo( $( '#clusterFold_' + i + ' > .fold-body' ) ) ;
			sdbjs.parts.tableBox.add( 'clusterTable_' + i, [ { 'cell': [ { 'text': '<span style="cursor:pointer;" onclick="gotoBusinessList(\'' + clusterJson['ClusterName'] + '\')"><b>业务</b></span>', 'style': { 'width': '60%' } }, { 'text': '' } ] } ] ) ;
			createBusiness( clusterJson['ClusterName'], 'clusterTable_' + i ) ;
			var hostNum = updateRightPic( clusterJson['ClusterName'] ) ;
			sdbjs.parts.tableBox.add( 'clusterTable_' + i, [ { 'cell': [ { 'text': '<span style="cursor:pointer;" onclick="gotoHostList(\'' + clusterJson['ClusterName'] + '\')"><b>主机</b></span>&nbsp;<span class="badge badge-info" data-type="tooltip" data-text="<p>共 ' + hostNum + ' 台主机</p>">' + hostNum + '</span>' }, { 'text': '<span class="badge badge-warning" data-name="hostStatus" data-type="tooltip"></span>&nbsp;<span class="badge badge-danger" data-name="hostStatus" data-type="tooltip"></span>' } ] } ] ) ;
		}
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 获取业务类型列表
 */
function getBusinessType()
{
	var order = { 'cmd': 'query business type' } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		var businessList = jsonArr[1] ;
		var businessListLen = businessList['BusinessList'].length ;
		var selectList = [] ;
		var descList = [] ;
		for( var i = 0; i < businessListLen; ++i )
		{
			if( i == 0 ){ selectList.push( { 'key': sdbjs.fun.htmlEncode( businessList['BusinessList'][i]['BusinessType'] ), 'value': businessList['BusinessList'][i]['BusinessType'], 'selected': true } ) }
			else{ selectList.push( { 'key': sdbjs.fun.htmlEncode( businessList['BusinessList'][i]['BusinessType'] ), 'value': businessList['BusinessList'][i]['BusinessType'] } ) }
			descList.push( businessList['BusinessList'][i]['BusinessDesc'] ) ;
		}
		sdbjs.parts.selectBox.create( 'businessType', {} ).change( function(){
			$( '#deploymentGuide > .modal-body td:eq(8)' ).text( descList[ this.selectedIndex ] ) ;
		} ).appendTo( $( '#deploymentGuide > .modal-body > div:eq(0) > table td:eq(7)' ) ) ;
		sdbjs.parts.selectBox.add( 'businessType', selectList ) ;
		$( '#deploymentGuide > .modal-body > div:eq(0) > table td:eq(8)' ).text( descList[0] ) ;
		
		sdbjs.parts.selectBox.create( 'businessType2', {} ).change( function(){
			$( '#addbusinessModal > .modal-body td:eq(5)' ).text( descList[ this.selectedIndex ] ) ;
		} ).appendTo( $( '#addbusinessModal > .modal-body > div:eq(0) > table td:eq(4)' ) ) ;
		sdbjs.parts.selectBox.add( 'businessType2', selectList ) ;
		$( '#addbusinessModal > .modal-body > div:eq(0) > table td:eq(5)' ).text( descList[0] ) ;
		
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 画图
 */
function createRightPic()
{
	var conf = [] ;
	conf[0] = $.extend( true, {}, ___picOption ) ;
	conf[1] = $.extend( true, {}, ___picOption ) ;
	conf[2] = $.extend( true, {}, ___picOption ) ;
	conf[3] = $.extend( true, {}, ___picOption ) ;
	conf[4] = $.extend( true, {}, ___picOption ) ;
	conf[5] = $.extend( true, {}, ___picOption ) ;
	conf[6] = $.extend( true, {}, ___picOption ) ;
	
	conf[0]['title']['text'] = '集群CPU' ;
	conf[0]['title']['subtext'] = 'percent' ;
	conf[0]['yAxis'][0]['axisLabel']['formatter'] = '{value} %' ;
	
	conf[1]['title']['text'] = '集群Memory' ;
	conf[1]['title']['subtext'] = 'percent' ;
	conf[1]['yAxis'][0]['axisLabel']['formatter'] = '{value} %' ;
	
	conf[2]['title']['text'] = '集群Disk' ;
	conf[2]['title']['subtext'] = 'percent' ;
	conf[2]['yAxis'][0]['axisLabel']['formatter'] = '{value} %' ;
	
	conf[3]['title']['text'] = '集群Network In' ;
	conf[3]['title']['subtext'] = 'Kb/s' ;
	conf[3]['yAxis'][0]['axisLabel']['formatter'] = '{value} Kb/s' ;
	
	conf[4]['title']['text'] = '集群Network Out' ;
	conf[4]['title']['subtext'] = 'Kb/s' ;
	conf[4]['yAxis'][0]['axisLabel']['formatter'] = '{value} Kb/s' ;
	
	conf[5]['title']['text'] = '集群Network PIn' ;
	conf[5]['title']['subtext'] = '数据包' ;
	conf[5]['yAxis'][0]['axisLabel']['formatter'] = '{value}' ;
	
	conf[6]['title']['text'] = '集群Network POut' ;
	conf[6]['title']['subtext'] = '数据包' ;
	conf[6]['yAxis'][0]['axisLabel']['formatter'] = '{value}' ;
	
	function updateRightPic()
	{
		if( cursorCluster != oldCluster )
		{
			$( '#rightPanel > .panel-header' ).text( cursorCluster + ' 实时监控' ) ;
			oldCluster = cursorCluster ;
			var picLen = rightPic.length ;
			for( var i = 0; i < picLen; ++i )
			{
				rightPic[i].setOption( conf[i] ) ;
			}
			oldHostConf = [] ;
			hostList = [] ;
			var order = { 'cmd': 'list host', 'ClusterName': cursorCluster } ;
			ajaxSendMsg( order, false, function( jsonArr ){
				var len = jsonArr.length ;
				for( var i = 1; i < len; ++i )
				{
					hostList.push( { 'HostName': jsonArr[i]['HostName'] } ) ;
					oldHostConf.push( { 'CPU': { 'Idle': [ 0, 0 ], 'Sum': [ 0, 0 ] }, 'Net': { 'RXBytes': [ 0, 0 ], 'TXBytes': [ 0, 0 ], 'RXPackets': [ 0, 0 ], 'TXPackets': [ 0, 0 ], 'CalendarTime': 0 } } )
				}
			}, function( jsonArr ){
				return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
			} ) ;
			
			
		}
		
		if( hostList.length > 0 )
		{
			var order = { 'cmd': 'query host status', 'HostInfo': JSON.stringify( { 'HostInfo': hostList } ) } ;
			ajaxSendMsg( order, true, function( jsonArr ){
				var hostNumLen = jsonArr[1]['HostInfo'].length ;
				var cpuPercent = 0 ;
				var memoryPercent = 0 ;
				var diskPercent = 0 ;
				var netRXBytes = 0 ;
				var netTXBytes = 0 ;
				var netRXPackets = 0 ;
				var netTXPackets = 0 ;
				var cpuList = [] ;
				var memoryList = [] ;
				var diskList = [] ;
				var RXBytesList = [] ;
				var TXBytesList = [] ;
				var RXPacketsList = [] ;
				var TXPacketsList = [] ;
				var errNum = 0 ;
				
				for( var i = 0; i < hostNumLen; ++i )
				{
					var hostInfoT = jsonArr[1]['HostInfo'][i] ;
					if( hostInfoT['errno'] == undefined || hostInfoT['errno'] == 0 )
					{
						//计算内存
						var memorySize = hostInfoT['Memory']['Size'] ;
						var memoryUsed = hostInfoT['Memory']['Used'] ;
						var tempMemory = ( memoryUsed * 100 / memorySize ) ;
						memoryList.push( tempMemory ) ;
						//计算硬盘
						var diskSize = 0 ;
						var diskFree = 0 ;
						var tempDisk = 0 ;
						{
							var hostDiskInfoT = hostInfoT['Disk'] ;
							var hostDiskLen = hostDiskInfoT.length ;
							for( var k = 0; k < hostDiskLen; ++k )
							{
								diskSize += hostDiskInfoT[k]['Size'] ;
								diskFree += hostDiskInfoT[k]['Free'] ;
							}
						}
						tempDisk = ( ( diskSize - diskFree ) * 100 / diskSize ) ;
						diskList.push( tempDisk ) ;
						//计算cpu
						var cpuIdle = [ 0, 0 ] ;
						var cpuSum = [ 0, 0 ] ;
						var tempCpu = 0 ;
						cpuIdle[0] += hostInfoT['CPU']['Idle']['Megabit'] ;
						cpuIdle[1] += hostInfoT['CPU']['Idle']['Unit'] ;
						cpuSum[0] += hostInfoT['CPU']['Idle']['Megabit'] + hostInfoT['CPU']['Sys']['Megabit'] + hostInfoT['CPU']['Other']['Megabit'] + hostInfoT['CPU']['User']['Megabit'] ;
						cpuSum[1] += hostInfoT['CPU']['Idle']['Unit'] + hostInfoT['CPU']['Sys']['Unit'] + hostInfoT['CPU']['Other']['Unit'] + hostInfoT['CPU']['User']['Unit'] ;
						tempCpu = ( ( 1 - ( ( cpuIdle[0] - oldHostConf[i]['CPU']['Idle'][0] ) * 1024 + ( cpuIdle[1] - oldHostConf[i]['CPU']['Idle'][1] ) / 1024 ) / ( ( cpuSum[0] - oldHostConf[i]['CPU']['Sum'][0] ) * 1024 + ( cpuSum[1] - oldHostConf[i]['CPU']['Sum'][1] ) / 1024 ) ) * 100 ) ;
						oldHostConf[i]['CPU']['Idle'][0] = cpuIdle[0] ;
						oldHostConf[i]['CPU']['Idle'][1] = cpuIdle[1] ;
						oldHostConf[i]['CPU']['Sum'][0] = cpuSum[0] ;
						oldHostConf[i]['CPU']['Sum'][1] = cpuSum[1] ;
						cpuList.push( tempCpu ) ;
						//计算流量和数据包
						var RXBytes = [ 0, 0 ] ;
						var TXBytes = [ 0, 0 ] ;
						var RXPackets = [ 0, 0 ] ;
						var TXPackets = [ 0, 0 ] ;
						var tempRXBytes = 0 ;
						var tempTXBytes = 0 ;
						var tempRXPackets = 0 ;
						var tempTXPackets = 0 ;
						var CalendarTime = hostInfoT['Net']['CalendarTime'] ;
						if( CalendarTime > oldHostConf[i]['Net']['CalendarTime'] )
						{
							{
								var hostNetInfoT = hostInfoT['Net']['Net'] ;
								var hostNetLen = hostNetInfoT.length ;
								for( var k = 0; k < hostNetLen; ++k )
								{
									RXBytes[0] += hostNetInfoT[k]['RXBytes']['Megabit'] ;
									RXBytes[1] += hostNetInfoT[k]['RXBytes']['Unit'] ;
									TXBytes[0] += hostNetInfoT[k]['TXBytes']['Megabit'] ;
									TXBytes[1] += hostNetInfoT[k]['TXBytes']['Unit'] ;
									RXPackets[0] += hostNetInfoT[k]['RXPackets']['Megabit'] ;
									RXPackets[1] += hostNetInfoT[k]['RXPackets']['Unit'] ;
									TXPackets[0] += hostNetInfoT[k]['TXPackets']['Megabit'] ;
									TXPackets[1] += hostNetInfoT[k]['TXPackets']['Unit'] ;
								}
							}
							tempRXBytes = ( ( ( RXBytes[0] - oldHostConf[i]['Net']['RXBytes'][0] ) * 1024 + ( RXBytes[1] - oldHostConf[i]['Net']['RXBytes'][1] ) / 1024 ) / ( CalendarTime - oldHostConf[i]['Net']['CalendarTime'] ) ) ;
							tempTXBytes = ( ( ( TXBytes[0] - oldHostConf[i]['Net']['TXBytes'][0] ) * 1024 + ( TXBytes[1] - oldHostConf[i]['Net']['TXBytes'][1] ) / 1024 ) / ( CalendarTime - oldHostConf[i]['Net']['CalendarTime'] ) ) ;
							tempRXPackets = ( ( ( RXPackets[0] - oldHostConf[i]['Net']['RXPackets'][0] ) * 1048576 + ( RXPackets[1] - oldHostConf[i]['Net']['RXPackets'][1] ) ) / ( CalendarTime - oldHostConf[i]['Net']['CalendarTime'] ) ) ;
							tempTXPackets = ( ( ( TXPackets[0] - oldHostConf[i]['Net']['TXPackets'][0] ) * 1048576 + ( TXPackets[1] - oldHostConf[i]['Net']['TXPackets'][1] ) ) / ( CalendarTime - oldHostConf[i]['Net']['CalendarTime'] ) ) ;
							oldHostConf[i]['Net']['RXBytes'][0] = RXBytes[0] ;
							oldHostConf[i]['Net']['RXBytes'][1] = RXBytes[1] ;
							oldHostConf[i]['Net']['TXBytes'][0] = TXBytes[0] ;
							oldHostConf[i]['Net']['TXBytes'][1] = TXBytes[1] ;
							oldHostConf[i]['Net']['RXPackets'][0] = RXPackets[0] ;
							oldHostConf[i]['Net']['RXPackets'][1] = RXPackets[1] ;
							oldHostConf[i]['Net']['TXPackets'][0] = TXPackets[0] ;
							oldHostConf[i]['Net']['TXPackets'][1] = TXPackets[1] ;
							oldHostConf[i]['Net']['CalendarTime'] = CalendarTime ;
						}
						RXBytesList.push( tempRXBytes ) ;
						TXBytesList.push( tempTXBytes ) ;
						RXPacketsList.push( tempRXPackets ) ;
						TXPacketsList.push( tempTXPackets ) ;
					}
					else
					{
						++errNum ;
					}
				}

				if( errNum > 0 )
				{
					$( '#leftPanel > .panel-body > .fold[data-name="cluster"] > .fold-body:not(:hidden) span[data-name="hostStatus"]:eq(1)' ).text( errNum ).show().data( 'text', '<p>有' + errNum + '台主机故障</p>' ) ;
				}
				else
				{
					$( '#leftPanel > .panel-body > .fold[data-name="cluster"] > .fold-body:not(:hidden) span[data-name="hostStatus"]:eq(1)' ).hide() ;
				}
				
				var memoryWarning = 0 ;
				var diskWarning = 0 ;
				var cpuWarning = 0 ;
				var warningNum = 0 ;
				var warningStr = '' ;
				for( var i = 0; i < memoryList.length; ++i )
				{
					if( memoryList[i] >= 90 )
					{
						++memoryWarning ;
					}
					if( diskList[i] >= 90 )
					{
						++diskWarning ;
					}
					if( cpuList[i] >= 90 )
					{
						++cpuWarning ;
					}
					memoryPercent += memoryList[i] ;
					diskPercent += diskList[i] ;
					cpuPercent += cpuList[i] ;
					netRXBytes += RXBytesList[i] ;
					netTXBytes += TXBytesList[i] ;
					netRXPackets += RXPacketsList[i] ;
					netTXPackets += TXPacketsList[i] ;
				}
				warningNum = memoryWarning + diskWarning + cpuWarning ;
				if( memoryWarning > 0 )
				{
					warningStr += '<p>有 ' + memoryWarning + ' 台主机的内存达到90%以上</p>' ;
				}
				if( diskWarning > 0 )
				{
					warningStr += '<p>有 ' + diskWarning + ' 台主机的硬盘达到90%以上</p>' ;
				}
				if( cpuWarning > 0 )
				{
					warningStr += '<p>有 ' + cpuWarning + ' 台主机的CPU达到90%以上</p>' ;
				}
				if( warningNum > 0 )
				{
					$( '#leftPanel > .panel-body > .fold[data-name="cluster"] > .fold-body:not(:hidden) span[data-name="hostStatus"]:eq(0)' ).text( warningNum ).show().data( 'text', warningStr ) ;
				}
				else
				{
					$( '#leftPanel > .panel-body > .fold[data-name="cluster"] > .fold-body:not(:hidden) span[data-name="hostStatus"]:eq(0)' ).hide() ;
				}
				memoryPercent = ( memoryPercent / memoryList.length ) ;
				diskPercent = ( diskPercent / memoryList.length ) ;
				cpuPercent = ( cpuPercent / memoryList.length ) ;
				if( isNaN( cpuPercent ) || cpuPercent < 0 ||
					 isNaN( netRXBytes ) || netRXBytes < 0 ||
					 isNaN( netTXBytes ) || netTXBytes < 0 ||
					 isNaN( netRXPackets ) || netRXPackets < 0 ||
					 isNaN( netTXPackets ) || netTXPackets < 0 )
				{
					cpuPercent = 0 ;
					netRXBytes = 0 ;
					netTXBytes = 0 ;
					netRXPackets = 0 ;
					netTXPackets = 0 ;
					var picLen = rightPic.length ;
					for( var i = 0; i < picLen; ++i )
					{
						rightPic[i].setOption( conf[i] ) ;
					}
				}
				rightPic[0].addData( [ [ 0, cpuPercent, true, false ] ] ) ;
				rightPic[1].addData( [ [ 0, memoryPercent, true, false ] ] ) ;
				rightPic[2].addData( [ [ 0, diskPercent, true, false ] ] ) ;
				rightPic[3].addData( [ [ 0, netRXBytes, true, false ] ] ) ;
				rightPic[4].addData( [ [ 0, netTXBytes, true, false ] ] ) ;
				rightPic[5].addData( [ [ 0, netRXPackets, true, false ] ] ) ;
				rightPic[6].addData( [ [ 0, netTXPackets, true, false ] ] ) ;
				
				oldHostConf['CPU'] = { 'Idle': cpuIdle, 'Sum': cpuSum } ;
				oldHostConf['Net'] = { 'RXBytes': RXBytes, 'TXBytes': TXBytes, 'RXPackets': RXPackets, 'TXPackets': TXPackets, 'CalendarTime': CalendarTime } ;
	
				setTimeout( updateRightPic, 1000 ) ;
	
			}, function( jsonArr ){
				return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
			} ) ;
		}
		else
		{
			$( '#leftPanel > .panel-body > .fold[data-name="cluster"] > .fold-body:not(:hidden) span[data-name="hostStatus"]:eq(0)' ).hide() ;
			$( '#leftPanel > .panel-body > .fold[data-name="cluster"] > .fold-body:not(:hidden) span[data-name="hostStatus"]:eq(1)' ).hide() ;
			setTimeout( updateRightPic, 1000 ) ;
		}
	}
	if( cursorCluster != null )
	{
		rightPic[0] = echarts.init( $( '#rightPanel > .panel-body > div:eq(0)' ).get(0) ) ; 
		rightPic[0].setOption( conf[0] ) ;
		
		rightPic[1] = echarts.init( $( '#rightPanel > .panel-body > div:eq(1)' ).get(0) ) ; 
		rightPic[1].setOption( conf[1] ) ;
		
		rightPic[2] = echarts.init( $( '#rightPanel > .panel-body > div:eq(2)' ).get(0) ) ; 
		rightPic[2].setOption( conf[2] ) ;
		
		rightPic[3] = echarts.init( $( '#rightPanel > .panel-body > div:eq(3)' ).get(0) ) ; 
		rightPic[3].setOption( conf[3] ) ;
		
		rightPic[4] = echarts.init( $( '#rightPanel > .panel-body > div:eq(4)' ).get(0) ) ; 
		rightPic[4].setOption( conf[4] ) ;
		
		rightPic[5] = echarts.init( $( '#rightPanel > .panel-body > div:eq(5)' ).get(0) ) ; 
		rightPic[5].setOption( conf[5] ) ;
		
		rightPic[6] = echarts.init( $( '#rightPanel > .panel-body > div:eq(6)' ).get(0) ) ; 
		rightPic[6].setOption( conf[6] ) ;
		
		updateRightPic() ;
	}
}

/*
 * 加载
 */
$(document).ready(function()
{
	if( $.cookie( 'SdbUser' ) == undefined || $.cookie( 'SdbPasswd' ) == undefined )
	{
		gotoPage( 'login.html' ) ;
		return ;
	}
	$.removeCookie( 'SdbBusinessName' ) ;
	$.removeCookie( 'SdbBusinessType' ) ;
	$.removeCookie( 'SdbGuideOrder' ) ;
	$.removeCookie( 'SdbBusinessConfig' ) ;
	$.removeCookie( 'SdbClusterName' ) ;
	$.removeCookie( 'SdbComeback' ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlBody' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	if( navigator.cookieEnabled == false )
	{
		$( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ).html( '<div id="cookieBox" style="padding-top: 317px;"><div class="alert alert-danger" id="cookie_alert_box" style="width: 800px; margin-left: auto; margin-right: auto;">您的浏览器禁止使用Cookie,系统将不能正常使用，请设置浏览器启用Cookie，并且刷新或重新打开浏览器。</div></div>' ) ;
		sdbjs.fun.autoCorrect( { 'id': 'cookieBox', 'style': { 'paddingTop': 'parseInt( ( sdbjs.public.height - 131 ) / 2 - 25 )' } } ) ;
	}
	else
	{
		sdbjs.fun.autoCorrect( { 'obj': $( '#bodyTran' ).children( ':first-child' ).children( ':eq(0)' ), 'style': { 'width': 'parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#bodyTran' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'leftPanel', 'style': { 'height': 'sdbjs.public.height - 151' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'rightPanel', 'style': { 'height': 'sdbjs.public.height - 151' } } ) ;
		
		createClusterList() ;
		
		sdbjs.parts.foldBox.create( 'omFold', {}, 'SMS', '<table class="simple-table"><tr><td width="60%"><b>节点</b></td><td></td></tr></table>', true, false, 'om' ).appendTo( $( '#leftPanel > .panel-body' ) ) ;
		
		getBusinessType() ;
		
		createRightPic() ;
		
		setUser() ;
	}
	
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ), 'style': { 'height': 'sdbjs.public.height - 131' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - 428' } } ) ;
	
	sdbjs.fun.endOfCreate() ;
	
	$( '#createClusterAdvanced > .fold-header > .fold-point' ).on( 'click', function(){ sdbjs.fun.moveModal( 'createCluster' ) } ) ;
	$( '#deploymentGuideAdvanced > .fold-header > .fold-point' ).on( 'click', function(){ sdbjs.fun.moveModal( 'deploymentGuide' ) } ) ;
	showInfoFromFoot( 'info', '欢迎使用SequoiaDB管理系统!' ) ;
});