/* 全局变量 */

//集群列表
var _clusterList = null ;

//业务列表
var _businessList = null ;

//当前画图的集群ID
var _cursorClusterID = null ;

//上一个画图的集群ID
var _oldClusterID = null ;

//主机列表
var _hostList = [] ;

//主机的旧值
var _oldHostConf = {} ;

//是否创建画图的框
var _isCreatePicDiv = false ;

//画图的对象
var _rightPic = [] ;

//要删除的集群ID
var _removeClusterID = null ;

//画图的模板
var _picOption = {
	title: {
		text: '',
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

/* 函数 */

//打开创建集群弹窗
function openCreateClusterModal()
{
	sdbjs.fun.setCSS( 'createClusterAlert', { 'display': 'none' } ) ;
	sdbjs.parts.modalBox.show( 'createCluster' ) ;
}

//创建集群
function createCluster()
{
	var clusterName = $( '#clusterName_c' ).val() ;
	var desc = $( '#desc_c' ).val() ;
	var user = $( '#userName_c' ).val() ;
	var pwd = $( '#passwd_c' ).val() ;
	var group = $( '#userGroup_c' ).val() ;
	var installPath = $( '#installPath_c' ).val() ;

	if( !checkStrName( clusterName ) )
	{
		sdbjs.fun.setCSS( 'createClusterAlert', { 'display': 'block' } ) ;
		sdbjs.parts.alertBox.update( 'createClusterAlert', htmlEncode( _languagePack['error']['web']['create'][0] ), 'danger' ) ;//'Error: 集群名格式错误，集群名只能由数字字母下划线组成，并且长度在 1 - 255 个字符内。'
		return;
	}
	if ( !checkString( user, 1, 1024 ) )
	{
		sdbjs.fun.setCSS( 'createClusterAlert', { 'display': 'block' } ) ;
		sdbjs.parts.alertBox.update( 'createClusterAlert', htmlEncode( _languagePack['error']['web']['create'][1] ), 'danger' ) ;//'Error: 用户名格式错误，用户名长度在 1 - 1024 个字符内。'
		return;
	}
	if ( !checkString( pwd, 1, 1024 ) )
	{
		sdbjs.fun.setCSS( 'createClusterAlert', { 'display': 'block' } ) ;
		sdbjs.parts.alertBox.update( 'createClusterAlert', htmlEncode( _languagePack['error']['web']['create'][2] ), 'danger' ) ;//'Error: 密码格式错误，密码长度在 1 - 1024 个字符内。'
		return;
	}
	if ( !checkString( group, 1, 1024 ) )
	{
		sdbjs.fun.setCSS( 'createClusterAlert', { 'display': 'block' } ) ;
		sdbjs.parts.alertBox.update( 'createClusterAlert', htmlEncode( _languagePack['error']['web']['create'][3] ), 'danger' ) ;//'Error: 用户组格式错误，用户组长度在 1 - 1024 个字符内。'
		return;
	}
	if ( !checkString( installPath, 1, 4096 ) )
	{
		sdbjs.fun.setCSS( 'createClusterAlert', { 'display': 'block' } ) ;
		sdbjs.parts.alertBox.update( 'createClusterAlert', htmlEncode( _languagePack['error']['web']['create'][4] ), 'danger' ) ;//'Error: 安装路径格式错误，安装路径长度在 1 - 4096 个字符内。'
		return;
	}
	sdbjs.parts.modalBox.hide( 'createCluster' ) ;
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restCreateCluster( true, function( jsonArr, textStatus, jqXHR ){
		gotoPage( 'index.html' ) ;
	}, function( json ){
		sdbjs.fun.setCSS( 'createClusterAlert', { 'display': 'block' } ) ;
		sdbjs.parts.alertBox.update( 'createClusterAlert', htmlEncode( json['detail'] ), 'danger' ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
		sdbjs.parts.modalBox.show( 'createCluster' ) ;
	}, clusterName, desc, user, pwd, group, installPath ) ;
}

//添加主机
function addHosts( clusterName )
{
	sdbjs.fun.saveData( 'SdbDeployModel', 'AddHost' ) ;
	sdbjs.fun.saveData( 'SdbClusterName', clusterName ) ;
	sdbjs.fun.delData( 'SdbHostList' ) ;
	gotoPage( 'scanhost.html' ) ;
}

//打开添加业务模态框
function openAddBusinessModal( clusterID )
{
	sdbjs.fun.setCSS( 'addBusinessFootAlert', { 'display': 'none' } ) ;
	sdbjs.parts.buttonBox.update( 'addBusinessOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'addBusiness(' + clusterID + ')' ) ;
	sdbjs.parts.modalBox.show( 'addBusiness' ) ;
}

//添加业务
function addBusiness( clusterID )
{
	var rc = true ;
	var businessName = $( '#businessName_a' ).val() ;
	var businessType = $( '#businessType_a' ).val() ;
	
	if( !checkStrName( businessName ) )
	{
		showModalError( 'addBusinessFootAlert', _languagePack['error']['web']['create'][5] ) ;//'业务名格式错误，业务名只能由数字字母下划线组成，并且长度在 1 - 255 个字符内.'
		return;
	}
	restListClusterBusiness( false, function( jsonArr, textStatus, jqXHR ){
		$.each( jsonArr, function( index, businessInfo ){
			if( businessName === businessInfo['BusinessName'] )
			{
				rc = false ;
				return false ;
			}
		} ) ;
	}, function( json ){
		showProcessError( json['detail'] ) ;
	}, null, _clusterList[clusterID]['ClusterName'] ) ;
	if( rc === false )
	{
		showModalError( 'addBusinessFootAlert', _languagePack['error']['web']['create'][6] ) ;//'业务名已经存在.'
		return ;
	}
	if( _clusterList[clusterID]['HostNum'] <= 0 )
	{
		showModalError( 'addBusinessFootAlert', _languagePack['error']['web']['create'][7] ) ;//'该集群还没有主机，无法添加业务.'
		return ;
	}
	sdbjs.fun.saveData( 'SdbBusinessName', businessName ) ;
	sdbjs.fun.saveData( 'SdbBusinessType', businessType ) ;
	sdbjs.fun.saveData( 'SdbDeployModel', 'AddBusiness' ) ;
	sdbjs.fun.saveData( 'SdbClusterName', _clusterList[clusterID]['ClusterName'] ) ;
	if( businessType === 'sequoiadb' )
	{
		gotoPage( 'confsdb.html' ) ;
	}
}

//部署引导
function deployGuid()
{
	var clusterName = $( '#clusterName_d' ).val() ;
	var desc = $( '#desc_d' ).val() ;
	var businessName = $( '#businessName_d' ).val() ;
	var businessType = $( '#businessType_d' ).val() ;
	var user = $( '#userName_d' ).val() ;
	var pwd = $( '#passwd_d' ).val() ;
	var group = $( '#userGroup_d' ).val() ;
	var installPath = $( '#installPath_d' ).val() ;

	if( !checkStrName( clusterName ) )
	{
		showModalError( 'deployGuidAlert', _languagePack['error']['web']['create'][0] ) ;//'集群名格式错误，集群名只能由数字字母下划线组成，并且长度在 1 - 255 个字符内.'
		return;
	}
	if( !checkStrName( businessName ) )
	{
		showModalError( 'deployGuidAlert', _languagePack['error']['web']['create'][5] ) ;//'业务名格式错误，业务名只能由数字字母下划线组成，并且长度在 1 - 255 个字符内.'
		return;
	}
	if ( !checkString( user, 1, 1024 ) )
	{
		showModalError( 'deployGuidAlert', _languagePack['error']['web']['create'][1] ) ;//'用户名格式错误，用户名长度在 1 - 1024 个字符内.'
		return;
	}
	if ( !checkString( pwd, 1, 1024 ) )
	{
		showModalError( 'deployGuidAlert', _languagePack['error']['web']['create'][2] ) ;//'密码格式错误，密码长度在 1 - 1024 个字符内.'
		return;
	}
	if ( !checkString( group, 1, 1024 ) )
	{
		showModalError( 'deployGuidAlert', _languagePack['error']['web']['create'][3] ) ;//'用户组格式错误，用户组长度在 1 - 1024 个字符内.'
		return;
	}
	if ( !checkString( installPath, 1, 4096 ) )
	{
		showModalError( 'deployGuidAlert', _languagePack['error']['web']['create'][4] ) ;//'安装路径格式错误，安装路径长度在 1 - 4096 个字符内.'
		return;
	}
	sdbjs.parts.modalBox.hide( 'deployGuid' ) ;
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restCreateCluster( true, function( jsonArr, textStatus, jqXHR ){
		sdbjs.fun.saveData( 'SdbBusinessName', businessName ) ;
		sdbjs.fun.saveData( 'SdbBusinessType', businessType ) ;
		sdbjs.fun.saveData( 'SdbDeployModel', 'Deploy' ) ;
		sdbjs.fun.saveData( 'SdbClusterName', clusterName ) ;
		gotoPage( 'scanhost.html' ) ;
	}, function( json ){
		showModalError( 'deployGuidAlert', json['detail'] ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
		sdbjs.parts.modalBox.show( 'deployGuid' ) ;
	}, clusterName, desc, user, pwd, group, installPath ) ;
}

//查看主机列表
function gotoHostList( clusterID )
{
	sdbjs.fun.saveData( 'SdbClusterName', _clusterList[clusterID]['ClusterName'] ) ;
	gotoPage( 'hostlist.html' ) ;
}

//查看业务列表
function gotoBusinessList( clusterID )
{
	sdbjs.fun.saveData( 'SdbClusterName', _clusterList[clusterID]['ClusterName'] ) ;
	gotoPage( 'businesslist.html' ) ;
}

function gotoTaskPage( taskID, taskType )
{
	sdbjs.fun.saveData( 'SdbTaskID', taskID ) ;
	if( taskType === 0 )
	{
		sdbjs.fun.saveData( 'SdbDeployModel', 'taskAddHost' ) ;
		gotoPage( 'installhost.html' ) ;
	}
	else if( taskType === 1 )
	{
		sdbjs.fun.saveData( 'SdbDeployModel', 'taskRemoveHost' ) ;
		sdbjs.fun.saveData( 'SdbClusterName', '' ) ;
		gotoPage( 'uninsthost.html' ) ;
	}
	else if( taskType === 2 )
	{
		sdbjs.fun.saveData( 'SdbDeployModel', 'taskAddSdb' ) ;
		gotoPage( 'installsdb.html' ) ;
	}
	else if( taskType === 3 )
	{
		sdbjs.fun.saveData( 'SdbDeployModel', 'taskRemoveSdb' ) ;
		gotoPage( 'uninstsdb.html' ) ;
	}
}

//获取正在进行的任务
function getRunTask()
{
	var taskList = [] ;
	function getRunTaskStatus( isFirst )
	{
		var taskArr = [] ;
		var stopNum = 0 ;
		$.each( taskList, function( index, value ){
			taskArr.push( { 'TaskID': value['TaskID'] } ) ;
			if( value['stop'] === true )
			{
				++stopNum ;
			}
		} ) ;
		if ( stopNum < taskList.length || isFirst === true )
		{
			restGetTaskStatus( true, function( jsonArr, textStatus, jqXHR ){
				$.each( jsonArr, function( index, value ){
					sdbjs.parts.progressBox.update( 'pr_task_' + index, value['Progress'], 'green', value['Progress'] + '%' ) ;
					sdbjs.parts.tableBox.updateBody( 'taskListTable', index + 1, 3, function( obj ){
						$( obj ).text( value['StatusDesc'] ) ;
					} ) ;
					if( value['StatusDesc'] === 'FINISH' )
					{
						taskList[index]['stop'] = true ;
					}
				} ) ;
			}, null, null, taskArr ) ;
			setTimeout( getRunTaskStatus, 1000 ) ;
		}
	}
	
	restListTasks( true, function( jsonArr, textStatus, jqXHR ){
		var runTaskList = jsonArr ;
		if( runTaskList.length > 0 )
		{
			$.each( runTaskList, function( index, value ){
				sdbjs.parts.tableBox.addBody( 'taskListTable', [{ 'text': function( obj ){
																					$( obj ).css( 'cursor', 'pointer' ) ;
																					sdbjs.fun.addClick( obj, 'gotoTaskPage(' + value['TaskID'] + ',' + value['Type'] + ')' ) ;
																					$( obj ).text( value['TaskID'] ) ;
																				} },
																				{ 'text': function( obj ){
																					$( obj ).css( 'cursor', 'pointer' ) ;
																					sdbjs.fun.addClick( obj, 'gotoTaskPage(' + value['TaskID'] + ',' + value['Type'] + ')' ) ;
																					$( obj ).text( value['TypeDesc'] ) ;
																				} },
																				{ 'text': function( obj ){
																					$( obj ).css( 'cursor', 'pointer' ) ;
																					sdbjs.fun.addClick( obj, 'gotoTaskPage(' + value['TaskID'] + ',' + value['Type'] + ')' ) ;
																					sdbjs.parts.progressBox.create( obj, 'pr_task_' + index ) ;
																					sdbjs.parts.progressBox.update( 'pr_task_' + index, 0, 'green', '0%' ) ;
																				} },
																				{ 'text': function( obj ){
																					$( obj ).css( 'cursor', 'pointer' ) ;
																					sdbjs.fun.addClick( obj, 'gotoTaskPage(' + value['TaskID'] + ',' + value['Type'] + ')' ) ;
																				} } ] ) ;
				taskList.push( { 'TaskID': value['TaskID'], 'stop': false } ) ;
			} ) ;
			getRunTaskStatus( true ) ;
		}
	} ) ;
}

//画图
function createRightPic()
{
	var conf = [] ;
	conf[0] = $.extend( true, {}, _picOption ) ;
	conf[1] = $.extend( true, {}, _picOption ) ;
	conf[2] = $.extend( true, {}, _picOption ) ;
	conf[3] = $.extend( true, {}, _picOption ) ;
	conf[4] = $.extend( true, {}, _picOption ) ;
	conf[5] = $.extend( true, {}, _picOption ) ;
	conf[6] = $.extend( true, {}, _picOption ) ;
	
	conf[0]['title']['text'] = _languagePack['index']['rightPanel']['pic'][0]['title'] ;
	conf[0]['title']['subtext'] = _languagePack['index']['rightPanel']['pic'][0]['subTitle'] ;
	conf[0]['yAxis'][0]['axisLabel']['formatter'] = '{value} %' ;
	
	conf[1]['title']['text'] = _languagePack['index']['rightPanel']['pic'][1]['title'] ;
	conf[1]['title']['subtext'] = _languagePack['index']['rightPanel']['pic'][1]['subTitle'] ;
	conf[1]['yAxis'][0]['axisLabel']['formatter'] = '{value} %' ;
	
	conf[2]['title']['text'] = _languagePack['index']['rightPanel']['pic'][2]['title'] ;
	conf[2]['title']['subtext'] = _languagePack['index']['rightPanel']['pic'][2]['subTitle'] ;
	conf[2]['yAxis'][0]['axisLabel']['formatter'] = '{value} %' ;
	
	conf[3]['title']['text'] = _languagePack['index']['rightPanel']['pic'][3]['title'] ;
	conf[3]['title']['subtext'] = _languagePack['index']['rightPanel']['pic'][3]['subTitle'] ;
	conf[3]['yAxis'][0]['axisLabel']['formatter'] = '{value} Kb/s' ;
	
	conf[4]['title']['text'] = _languagePack['index']['rightPanel']['pic'][4]['title'] ;
	conf[4]['title']['subtext'] = _languagePack['index']['rightPanel']['pic'][4]['subTitle'] ;
	conf[4]['yAxis'][0]['axisLabel']['formatter'] = '{value} Kb/s' ;
	
	conf[5]['title']['text'] = _languagePack['index']['rightPanel']['pic'][5]['title'] ;
	conf[5]['title']['subtext'] = _languagePack['index']['rightPanel']['pic'][5]['subTitle'] ;
	conf[5]['yAxis'][0]['axisLabel']['formatter'] = '{value}' ;
	
	conf[6]['title']['text'] = _languagePack['index']['rightPanel']['pic'][6]['title'] ;
	conf[6]['title']['subtext'] = _languagePack['index']['rightPanel']['pic'][6]['subTitle'] ;
	conf[6]['yAxis'][0]['axisLabel']['formatter'] = '{value}' ;
	
	//更新右边的图表
	function updateRightPic()
	{
		var tmpCurClusterID = _cursorClusterID ;
		//更换了集群
		if( tmpCurClusterID !== _oldClusterID || _hostList.length === 0 )
		{
			//设置右边的标题
			sdbjs.parts.panelBox.update( 'chartBar', htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['rightPanel']['title'], _clusterList[tmpCurClusterID]['ClusterName'] ) ), null ) ;
			_oldClusterID = tmpCurClusterID ;
			var picLen = _rightPic.length ;
			for( var i = 0; i < picLen; ++i )
			{
				_rightPic[i].setOption( conf[i] ) ;
			}
			_oldHostConf = [] ;
			_hostList = [] ;
			//获取集群主机列表
			restListHost( false, function( jsonArr, textStatus, jqXHR ){
				var len = jsonArr.length ;
				sdbjs.parts.badgeBox.update( 'host_badge_' + tmpCurClusterID, htmlEncode( len ), 'info' ) ;
				sdbjs.fun.setLabel( 'host_badge_' + tmpCurClusterID, htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['leftPanel']['body']['badgeTips'][0], len ) ) ) ;//'一共 ? 台主机'
				$.each( jsonArr, function( index, hostInfo ){
					_hostList.push( { 'HostName': hostInfo['HostName'] } ) ;
					_oldHostConf.push( { 'CPU': { 'Idle': [ 0, 0 ], 'Sum': [ 0, 0 ] }, 'Net': { 'RXBytes': [ 0, 0 ], 'TXBytes': [ 0, 0 ], 'RXPackets': [ 0, 0 ], 'TXPackets': [ 0, 0 ], 'CalendarTime': 0 } } ) ;
				} ) ;
			}, function( json ){
				showProcessError( json['detail'] ) ;
			}, null, _clusterList[tmpCurClusterID]['ClusterName'] ) ;
			
			restListClusterBusiness( false, function( jsonArr, textStatus, jqXHR ){
				var len = jsonArr.length ;
				sdbjs.parts.badgeBox.update( 'business_badge_' + tmpCurClusterID, htmlEncode( len ), 'info' ) ;
				//'一共 ? 个业务'
				sdbjs.fun.setLabel( 'business_badge_' + tmpCurClusterID, htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['leftPanel']['body']['badgeTips'][1], len ) ) ) ;
			}, function( json ){
				showProcessError( json['detail'] ) ;
			}, null, _clusterList[tmpCurClusterID]['ClusterName'] ) ;
		}
		
		if( _hostList.length > 0 )
		{
			restQueryHostStatus( true, function( jsonArr, textStatus, jqXHR ){
				var hostNumLen = jsonArr[0]['HostInfo'].length ;
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
					var hostInfoT = jsonArr[0]['HostInfo'][i] ;
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
						tempCpu = ( ( 1 - ( ( cpuIdle[0] - _oldHostConf[i]['CPU']['Idle'][0] ) * 1024 + ( cpuIdle[1] - _oldHostConf[i]['CPU']['Idle'][1] ) / 1024 ) / ( ( cpuSum[0] - _oldHostConf[i]['CPU']['Sum'][0] ) * 1024 + ( cpuSum[1] - _oldHostConf[i]['CPU']['Sum'][1] ) / 1024 ) ) * 100 ) ;
						_oldHostConf[i]['CPU']['Idle'][0] = cpuIdle[0] ;
						_oldHostConf[i]['CPU']['Idle'][1] = cpuIdle[1] ;
						_oldHostConf[i]['CPU']['Sum'][0] = cpuSum[0] ;
						_oldHostConf[i]['CPU']['Sum'][1] = cpuSum[1] ;
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
						if( CalendarTime > _oldHostConf[i]['Net']['CalendarTime'] )
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
							tempRXBytes = ( ( ( RXBytes[0] - _oldHostConf[i]['Net']['RXBytes'][0] ) * 1024 + ( RXBytes[1] - _oldHostConf[i]['Net']['RXBytes'][1] ) / 1024 ) / ( CalendarTime - _oldHostConf[i]['Net']['CalendarTime'] ) ) ;
							tempTXBytes = ( ( ( TXBytes[0] - _oldHostConf[i]['Net']['TXBytes'][0] ) * 1024 + ( TXBytes[1] - _oldHostConf[i]['Net']['TXBytes'][1] ) / 1024 ) / ( CalendarTime - _oldHostConf[i]['Net']['CalendarTime'] ) ) ;
							tempRXPackets = ( ( ( RXPackets[0] - _oldHostConf[i]['Net']['RXPackets'][0] ) * 1048576 + ( RXPackets[1] - _oldHostConf[i]['Net']['RXPackets'][1] ) ) / ( CalendarTime - _oldHostConf[i]['Net']['CalendarTime'] ) ) ;
							tempTXPackets = ( ( ( TXPackets[0] - _oldHostConf[i]['Net']['TXPackets'][0] ) * 1048576 + ( TXPackets[1] - _oldHostConf[i]['Net']['TXPackets'][1] ) ) / ( CalendarTime - _oldHostConf[i]['Net']['CalendarTime'] ) ) ;
							_oldHostConf[i]['Net']['RXBytes'][0] = RXBytes[0] ;
							_oldHostConf[i]['Net']['RXBytes'][1] = RXBytes[1] ;
							_oldHostConf[i]['Net']['TXBytes'][0] = TXBytes[0] ;
							_oldHostConf[i]['Net']['TXBytes'][1] = TXBytes[1] ;
							_oldHostConf[i]['Net']['RXPackets'][0] = RXPackets[0] ;
							_oldHostConf[i]['Net']['RXPackets'][1] = RXPackets[1] ;
							_oldHostConf[i]['Net']['TXPackets'][0] = TXPackets[0] ;
							_oldHostConf[i]['Net']['TXPackets'][1] = TXPackets[1] ;
							_oldHostConf[i]['Net']['CalendarTime'] = CalendarTime ;
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
					sdbjs.fun.setCSS( 'danger_badge_' + tmpCurClusterID, { 'display': '' } ) ;
					sdbjs.parts.badgeBox.update( 'danger_badge_' + tmpCurClusterID, htmlEncode( errNum ), 'danger' ) ;
					//有 ? 台主机故障
					sdbjs.fun.setLabel( 'danger_badge_' + tmpCurClusterID, htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['leftPanel']['body']['badgeTips'][5], errNum ) ) ) ;
				}
				else
				{
					sdbjs.fun.setCSS( 'danger_badge_' + tmpCurClusterID, { 'display': 'none' } ) ;
				}

				var memoryWarning = 0 ;
				var diskWarning = 0 ;
				var cpuWarning = 0 ;
				var warningNum = 0 ;
				if( errNum < hostNumLen )
				{
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
						//有 ? 台主机的内存达到90%以上
						warningStr += '<p>' + htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['leftPanel']['body']['badgeTips'][2], memoryWarning ) ) + '</p>' ;
					}
					if( diskWarning > 0 )
					{
						//有 ? 台主机的硬盘达到90%以上
						warningStr += '<p>' + htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['leftPanel']['body']['badgeTips'][3], diskWarning ) ) + '</p>' ;
					}
					if( cpuWarning > 0 )
					{
						//有 ? 台主机的cpu达到90%以上
						warningStr += '<p>' + htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['leftPanel']['body']['badgeTips'][4], cpuWarning ) ) + '</p>' ;
					}
					if( warningNum > 0 )
					{
						sdbjs.parts.badgeBox.update( 'warning_badge_' + tmpCurClusterID, htmlEncode( warningNum ), 'warning' ) ;
						sdbjs.fun.setLabel( 'warning_badge_' + tmpCurClusterID, warningStr ) ;
						sdbjs.fun.setCSS( 'warning_badge_' + tmpCurClusterID, { 'display': '' } ) ;
					}
					else
					{
						sdbjs.fun.setCSS( 'warning_badge_' + tmpCurClusterID, { 'display': 'none' } ) ;
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
						var picLen = _rightPic.length ;
						for( var i = 0; i < picLen; ++i )
						{
							_rightPic[i].setOption( conf[i] ) ;
						}
					}
				}
				_rightPic[0].addData( [ [ 0, cpuPercent, true, false ] ] ) ;
				_rightPic[1].addData( [ [ 0, memoryPercent, true, false ] ] ) ;
				_rightPic[2].addData( [ [ 0, diskPercent, true, false ] ] ) ;
				_rightPic[3].addData( [ [ 0, netRXBytes, true, false ] ] ) ;
				_rightPic[4].addData( [ [ 0, netTXBytes, true, false ] ] ) ;
				_rightPic[5].addData( [ [ 0, netRXPackets, true, false ] ] ) ;
				_rightPic[6].addData( [ [ 0, netTXPackets, true, false ] ] ) ;
				
				_oldHostConf['CPU'] = { 'Idle': cpuIdle, 'Sum': cpuSum } ;
				_oldHostConf['Net'] = { 'RXBytes': RXBytes, 'TXBytes': TXBytes, 'RXPackets': RXPackets, 'TXPackets': TXPackets, 'CalendarTime': CalendarTime } ;

				if( tmpCurClusterID === _oldClusterID )
				{
					setTimeout( updateRightPic, 1000 ) ;
				}
				else
				{
					setTimeout( updateRightPic, 0 ) ;
				}
	
			}, function( jsonArr ){
				showProcessError( jsonArr['detail'] ) ;
			}, null, _hostList ) ;
		}
		else
		{
			setTimeout( updateRightPic, 1000 ) ;
		}
	}
	if( _cursorClusterID === null )
	{
		setTimeout( createRightPic, 100 ) ;
	}
	else
	{
		_rightPic[0].setOption( conf[0] ) ;
		_rightPic[1].setOption( conf[1] ) ;
		_rightPic[2].setOption( conf[2] ) ;
		_rightPic[3].setOption( conf[3] ) ;
		_rightPic[4].setOption( conf[4] ) ;
		_rightPic[5].setOption( conf[5] ) ;
		_rightPic[6].setOption( conf[6] ) ;
		
		updateRightPic() ;
	}
}

//设置当前的集群
function setCursorCluster( clusterID )
{
	_cursorClusterID = clusterID ;
}

//打开删除集群的模态框
function openRemoveCluster( clusterID )
{
	_removeClusterID = clusterID ;
	sdbjs.parts.modalBox.show( 'isRemoveCluster' ) ;
}

//删除集群
function removeCluster()
{
	var clusterName = _clusterList[ _removeClusterID ]['ClusterName'] ;
	sdbjs.parts.modalBox.hide( 'isRemoveCluster' ) ;
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restRemoveCluster( true, function( jsonArr, textStatus, jqXHR ){
		gotoPage( 'index.html' ) ;
	}, function( json ){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
		showProcessError( json['detail'] ) ;
	}, null, clusterName ) ;
}

//---------------------------- 初始化 ------------------------------

//加载集群列表
function loadClusterList()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restQueryCluster( true, function( jsonArr, textStatus, jqXHR ){
		_clusterList = jsonArr ;
		if( _clusterList.length > 0 )
		{
			sdbjs.parts.panelBox.update( 'chartBar', null, function( panelBody ){
				sdbjs.fun.setHtml( panelBody, '' ) ;
				sdbjs.parts.divBox.create( panelBody['obj'], 'pic_0' ) ;
				sdbjs.parts.divBox.create( panelBody['obj'], 'pic_1' ) ;
				sdbjs.parts.divBox.create( panelBody['obj'], 'pic_2' ) ;
				sdbjs.parts.divBox.create( panelBody['obj'], 'pic_3' ) ;
				sdbjs.parts.divBox.create( panelBody['obj'], 'pic_4' ) ;
				sdbjs.parts.divBox.create( panelBody['obj'], 'pic_5' ) ;
				sdbjs.parts.divBox.create( panelBody['obj'], 'pic_6' ) ;
				sdbjs.parts.divBox.create( panelBody['obj'], 'pic_clear' ) ;
				sdbjs.fun.setCSS( 'pic_0', { 'float': 'left', 'width': 300, height: 200 } ) ;
				sdbjs.fun.setCSS( 'pic_1', { 'float': 'left', 'width': 300, height: 200 } ) ;
				sdbjs.fun.setCSS( 'pic_2', { 'float': 'left', 'width': 300, height: 200 } ) ;
				sdbjs.fun.setCSS( 'pic_3', { 'float': 'left', 'width': 300, height: 200 } ) ;
				sdbjs.fun.setCSS( 'pic_4', { 'float': 'left', 'width': 300, height: 200 } ) ;
				sdbjs.fun.setCSS( 'pic_5', { 'float': 'left', 'width': 300, height: 200 } ) ;
				sdbjs.fun.setCSS( 'pic_6', { 'float': 'left', 'width': 300, height: 200 } ) ;
				sdbjs.fun.setClass( 'pic_clear', 'clear-float' ) ;
				
				sdbjs.parts.divBox.update( 'pic_0', function( obj ){
					_rightPic[0] = echarts.init( $( obj ).get(0) ) ;
				} ) ;
				sdbjs.parts.divBox.update( 'pic_1', function( obj ){
					_rightPic[1] = echarts.init( $( obj ).get(0) ) ;
				} ) ;
				sdbjs.parts.divBox.update( 'pic_2', function( obj ){
					_rightPic[2] = echarts.init( $( obj ).get(0) ) ;
				} ) ;
				sdbjs.parts.divBox.update( 'pic_3', function( obj ){
					_rightPic[3] = echarts.init( $( obj ).get(0) ) ;
				} ) ;
				sdbjs.parts.divBox.update( 'pic_4', function( obj ){
					_rightPic[4] = echarts.init( $( obj ).get(0) ) ;
				} ) ;
				sdbjs.parts.divBox.update( 'pic_5', function( obj ){
					_rightPic[5] = echarts.init( $( obj ).get(0) ) ;
				} ) ;
				sdbjs.parts.divBox.update( 'pic_6', function( obj ){
					_rightPic[6] = echarts.init( $( obj ).get(0) ) ;
				} ) ;
			} ) ;
		}
		$.each( _clusterList, function( index, clusterInfo ){
			var foldNodeName  = 'cluster_fold_'  + index ;
			var dropNodeName  = 'cluster_drop_'  + index ;
			var tableNodeName = 'cluster_table_' + index ;
			var businessBadgeName = 'business_badge_' + index ;
			var hostBadgeName = 'host_badge_' + index ;
			var warningBadgeName = 'warning_badge_' + index ;
			var dangerBadgeName = 'danger_badge_' + index ;
			var clusterName = clusterInfo['ClusterName'] ;
			
			sdbjs.parts.foldBox.create( 'foldBox_top', foldNodeName ) ;
			sdbjs.parts.foldBox.setDomain( foldNodeName, 'cluster' ) ;
			sdbjs.parts.foldBox.update( foldNodeName, function( headObj ){
				sdbjs.parts.dropDownBox.create( headObj, dropNodeName ) ;
				sdbjs.parts.dropDownBox.update( dropNodeName, '', 'btn-lg' ) ;
				sdbjs.parts.dropDownBox.add( dropNodeName, htmlEncode( _languagePack['index']['clusterOperation'][0] ), true, 'addHosts("' + clusterName + '")' ) ;//'添加主机'
				sdbjs.parts.dropDownBox.add( dropNodeName, htmlEncode( _languagePack['index']['clusterOperation'][1] ), true, 'gotoHostList(' + index + ')' ) ;//'主机列表'
				sdbjs.parts.dropDownBox.add( dropNodeName, '', true ) ;
				sdbjs.parts.dropDownBox.add( dropNodeName, htmlEncode( _languagePack['index']['clusterOperation'][2] ), true, 'openAddBusinessModal(' + index + ')' ) ;//'添加业务'
				sdbjs.parts.dropDownBox.add( dropNodeName, htmlEncode( _languagePack['index']['clusterOperation'][3] ), true, 'gotoBusinessList(' + index + ')' ) ;//业务列表
				sdbjs.parts.dropDownBox.add( dropNodeName, '', true ) ;
				sdbjs.parts.dropDownBox.add( dropNodeName, htmlEncode( _languagePack['index']['clusterOperation'][4] ), true, 'openRemoveCluster(' + index + ')' ) ;//'删除集群'
				sdbjs.fun.setClass( dropNodeName, 'pull-right' ) ;
				sdbjs.fun.setCSS( dropNodeName, { 'z-index': 10 } ) ;
				$( headObj ).append( '<span class="caret caret-right"></span> ' ) ;
				$( headObj ).append( htmlEncode( clusterName ) ) ;
				$( headObj ).append( '<div class="clear-float"></div> ' ) ;
			}, function( bodyObj ){
				$( bodyObj ).css( 'margin-top', 5 ) ;
				sdbjs.parts.tableBox.create( bodyObj, tableNodeName ) ;
				sdbjs.parts.tableBox.update(  tableNodeName, 'loosen border' ) ;
				sdbjs.parts.tableBox.addBody( tableNodeName, [ { 'text': '<span style="font-weight:bold;cursor:pointer;" onclick="gotoBusinessList(' + index + ')">' + htmlEncode( _languagePack['index']['leftPanel']['body']['table'][0] ) + '</span>&nbsp;', 'width': '60%' }, { 'text': '', 'width': '40%' } ] ) ;//业务
				sdbjs.parts.tableBox.updateBody( tableNodeName, 0, 0, function( tdObj ){
					sdbjs.parts.badgeBox.create( tdObj, businessBadgeName ) ;
					sdbjs.parts.badgeBox.update( businessBadgeName, htmlEncode( 0 ), 'info' ) ;
					sdbjs.fun.setLabel( businessBadgeName, htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['leftPanel']['body']['badgeTips'][1], 0 ) ) ) ;//'一共 ? 个业务'
				} ) ;
				sdbjs.parts.tableBox.addBody( tableNodeName, [ { 'text': '<span style="font-weight:bold;cursor:pointer;" onclick="gotoHostList(' + index + ')">' + htmlEncode( _languagePack['index']['leftPanel']['body']['table'][1] ) + '</span>&nbsp;', 'width': '60%' }, { 'text': '', 'width': '40%' } ] ) ;//主机
				sdbjs.parts.tableBox.updateBody( tableNodeName, 1, 0, function( tdObj ){
					sdbjs.parts.badgeBox.create( tdObj, hostBadgeName ) ;
					sdbjs.parts.badgeBox.update( hostBadgeName, htmlEncode( 0 ), 'info' ) ;
					sdbjs.fun.setLabel( hostBadgeName, htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['leftPanel']['body']['badgeTips'][0], 0 ) ) ) ;//'一共 ? 台主机'
				} ) ;
				sdbjs.parts.tableBox.updateBody( tableNodeName, 1, 1, function( tdObj ){
					sdbjs.parts.badgeBox.create( tdObj, warningBadgeName ) ;
					sdbjs.fun.setCSS( warningBadgeName, { 'display': 'none' } ) ;
					$( tdObj ).append( '&nbsp;' )
					sdbjs.parts.badgeBox.create( tdObj, dangerBadgeName ) ;
					sdbjs.fun.setCSS( dangerBadgeName, { 'display': 'none' } ) ;
				} ) ;
			}, 'setCursorCluster(' + index + ')' ) ;
			if( index === 0 )
			{
				sdbjs.parts.foldBox.show( 'cluster_fold_' + index ) ;
			}
		} ) ;
	}, function( json ){
		showProcessError( json['detail'] ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
	} ) ;
}

//加载业务类型
function loadBusinessType()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restQueryBusinessType( true, function( jsonArr, textStatus, jqXHR ){
		sdbjs.parts.tableBox.updateBody( 'deployGuidTable', 2, 1, function( obj ){
			var selectObj_1 = $( '#businessType_d' ) ;
			var selectObj_2 = $( '#businessType_a' ) ;
			_businessList = jsonArr ;
			$.each( _businessList, function( index, businessInfo ){
				selectObj_1.append( '<option value="' + htmlEncode( businessInfo['BusinessType'] ) + '"' + ( index === 0 ? ' select' : '' ) + '>' + htmlEncode( businessInfo['BusinessType'] ) + '</option>' ) ;
				selectObj_2.append( '<option value="' + htmlEncode( businessInfo['BusinessType'] ) + '"' + ( index === 0 ? ' select' : '' ) + '>' + htmlEncode( businessInfo['BusinessType'] ) + '</option>' )
				if( index === 0 )
				{
					sdbjs.parts.tableBox.updateBody( 'deployGuidTable', 3, 2, htmlEncode( businessInfo['BusinessDesc'] ) ) ;
					sdbjs.parts.tableBox.updateBody( 'addBusinessTable', 1, 2, htmlEncode( businessInfo['BusinessDesc'] ) ) ;
				}
			} ) ;
			selectObj_1.change( function(){
				var index = this.selectedIndex ;
				sdbjs.parts.tableBox.updateBody( 'deployGuidTable', 3, 2, htmlEncode( _businessList[index]['BusinessDesc'] ) ) ;
			} ) ;
			selectObj_2.change( function(){
				var index = this.selectedIndex ;
				sdbjs.parts.tableBox.updateBody( 'addBusinessTable', 1, 2, htmlEncode( _businessList[index]['BusinessDesc'] ) ) ;
			} ) ;
			sdbjs.parts.loadingBox.hide( 'loading' ) ;
		} ) ;
	}, function( json ){
		showProcessError( json['detail'] ) ;
	} ) ;
}

//检测部署状态
function checkDeploy()
{
	function setTips( target, pageName )
	{
		//'Info: 系统检测到您在?时中断，是否要回到该操作中继续？'
		sdbjs.parts.alertBox.update( 'isGoOnDeployAlert', htmlEncode( sdbjs.fun.sprintf( _languagePack['tip']['web']['index'][0], pageName ) ), 'info' ) ;
		sdbjs.parts.buttonBox.update( 'isGoOnDeployOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'gotoPage("' + target + '")' ) ;
		sdbjs.parts.modalBox.show( 'isGoOnDeploy' ) ;
	}
	var rc = false ;
	//刚刚登录 检查是否在部署中途
	if( sdbjs.fun.getData( 'SdbIsLogin' ) === 'true' )
	{
		sdbjs.fun.delData( 'SdbIsLogin' ) ;
		var deployModel = sdbjs.fun.getData( 'SdbDeployModel' ) ;
		var step = sdbjs.fun.getData( 'SdbStep' ) ;
		if ( deployModel === 'AddHost' || deployModel === 'Deploy' )
		{
			if( step === 'scanhost' && sdbjs.fun.hasData( 'SdbClusterName' ) )
			{
				rc = true ;
				setTips( step + '.html', _languagePack['public']['tabPage'][2] ) ;
			}
			else if( step === 'addhost' && sdbjs.fun.hasData( 'SdbHostList' ) && sdbjs.fun.hasData( 'SdbClusterName' ) )
			{
				rc = true ;
				setTips( step + '.html', _languagePack['public']['tabPage'][3] ) ;
			}
			else if( step === 'installhost' && sdbjs.fun.hasData( 'SdbTaskID' ) )
			{
				rc = true ;
				setTips( step + '.html', _languagePack['public']['tabPage'][4] ) ;
			}
		}
		else if ( deployModel === 'AddBusiness' || deployModel === 'Deploy' )
		{
			if ( sdbjs.fun.getData( 'SdbBusinessType' ) === 'sequoiadb' )
			{
				if( step === 'confsdb' && sdbjs.fun.hasData( 'SdbClusterName' ) && sdbjs.fun.hasData( 'SdbBusinessName' ) )
				{
					rc = true ;
					setTips( step + '.html', _languagePack['public']['tabPage'][5] ) ;
				}
				else if( step === 'modsdbd' && sdbjs.fun.hasData( 'SdbBusinessConfig' ) )
				{
					rc = true ;
					setTips( step + '.html', _languagePack['public']['tabPage'][6] ) ;
				}
				else if( step === 'modsdbs' && sdbjs.fun.hasData( 'SdbBusinessConfig' ) )
				{
					rc = true ;
					setTips( step + '.html', _languagePack['public']['tabPage'][6] ) ;
				}
				else if( step === 'installsdb' && sdbjs.fun.hasData( 'SdbTaskID' ) && sdbjs.fun.hasData( 'SdbBusinessConfig' ) )
				{
					rc = true ;
					setTips( step + '.html', _languagePack['public']['tabPage'][7] ) ;
				}
			}
		}
		else if ( deployModel === 'taskRemoveHost' )
		{
			if( step === 'uninsthost' )
			{
				rc = true ;
				setTips( step + '.html', _languagePack['public']['tabPage'][9] ) ;
			}
		}
		else if ( deployModel === 'taskRemoveSdb' )
		{
			if( step === 'uninstsdb' )
			{
				rc = true ;
				setTips( step + '.html', _languagePack['public']['tabPage'][8] ) ;
			}
		}
	}
	else
	{
		sdbjs.fun.delData( 'SdbDeployModel' ) ;
	}
	return rc ;
}

//检测是否第一次使用
function checkFirstUse()
{
	/*var isFirst = sdbjs.fun.getData( 'SdbIsFirst' ) ;
	if( _clusterList.length === 0 && isFirst === null )
	{
		sdbjs.fun.saveData( 'SdbIsFirst', 'true' ) ;
		
	}*/
}

function createDynamicHtml()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	loadClusterList() ;
	loadBusinessType() ;
	sdbjs.parts.loadingBox.hide( 'loading' ) ;
	createRightPic() ;
	getRunTask() ;
	if( checkDeploy() === false )
	{
		//checkFirstUse() ;
	}
	else
	{
		sdbjs.fun.saveData( 'SdbIsFirst', 'true' ) ;
	}
}

function createHtml()
{
	createPublicHtml() ;
	
	/* 分页 */
	sdbjs.parts.tabPageBox.create( 'top2', 'tab' ) ;
	sdbjs.fun.setCSS( 'tab', { 'padding-top': 5 } ) ;
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/home.png"> ' + htmlEncode( _languagePack['public']['tabPage'][1] ), true, null ) ;
	
	/* 左边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-left', 460, 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-left', { 'float': 'left', 'padding': '10px' } ) ;
	
	/* 左边 状态栏 */
	sdbjs.parts.panelBox.create( 'middle-left', 'ststusBar', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'ststusBar', { 'overflow': 'auto' } ) ;
	sdbjs.parts.panelBox.update( 'ststusBar', function( panelTitle ){
		sdbjs.parts.dropDownBox.create( panelTitle['obj'], 'cluster_dropDown' ) ;
		sdbjs.parts.dropDownBox.update( 'cluster_dropDown', '<img class="icon" src="./images/smallicon/blacks/16x16/align_just.png">', 'btn-lg' ) ;
		//'创建集群'
		sdbjs.parts.dropDownBox.add( 'cluster_dropDown', htmlEncode( _languagePack['index']['clusterOperation'][5] ), true, function( obj ){
			sdbjs.fun.addClick( obj, 'openCreateClusterModal()' ) ;
		} ) ;
		sdbjs.fun.setClass( 'cluster_dropDown', 'pull-right' ) ;
		$( panelTitle['obj'] ).append( htmlEncode( _languagePack['index']['leftPanel']['title'] ) ) ;//'状态' 
	}, function( panelBody ){
		sdbjs.parts.divBox.create( panelBody['obj'], 'foldBox_top' ) ;
		sdbjs.parts.divBox.create( panelBody['obj'], 'foldBox_bottom' ) ;
		sdbjs.parts.foldBox.create( 'foldBox_bottom', 'SAC' ) ;
		sdbjs.parts.foldBox.update( 'SAC', 'SAC', function( bodyObj ){
			sdbjs.parts.tableBox.create( bodyObj, 'SACTable' ) ;
			sdbjs.parts.tableBox.update( 'SACTable', 'loosen border' ) ;
			//'节点'
			sdbjs.parts.tableBox.addBody( 'SACTable', [ { 'text': '<b>' + htmlEncode( _languagePack['index']['leftPanel']['SAC'][0] ) + '</b>', 'width': '60%' }, { 'text': '', 'width': '40%' } ] ) ;
		} ) ;
		sdbjs.parts.foldBox.show( 'SAC' ) ;
	} ) ;
	
	/* 右边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-right', 'variable', 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-right', { 'float': 'left', 'padding': '10px', 'padding-left': 0 } ) ;
	
	/* 右边 图表 */
	sdbjs.parts.panelBox.create( 'middle-right', 'chartBar', 'auto', 'variable' ) ;
	//'监控信息'
	sdbjs.parts.panelBox.update( 'chartBar', htmlEncode( sdbjs.fun.sprintf( _languagePack['index']['rightPanel']['title'], '' ) ), function( panelBody ){
		
		sdbjs.fun.setCSS( panelBody['name'], { 'overflow-Y': 'auto', 'position': 'relative' } ) ;
		panelBody['obj'].text( _languagePack['index']['rightPanel']['body'] ) ;
	} ) ;
	
	/* ** */
	sdbjs.parts.divBox.create( 'middle', 'middle-clear', 0, 0 ) ;
	sdbjs.fun.setClass( 'middle-clear', 'clear-float' ) ;

	/* 部署引导的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'deployGuid' ) ;
	//'部署引导'
	sdbjs.parts.modalBox.update( 'deployGuid', htmlEncode( _languagePack['index']['modal']['deploy']['title'] ), function( bodyObj ){
		sdbjs.parts.tableBox.create( bodyObj, 'deployGuidTable' ) ;
		sdbjs.parts.tableBox.update( 'deployGuidTable', 'loosen' ) ;
		//'集群名:'  '部署的集群名'
		sdbjs.parts.tableBox.addBody( 'deployGuidTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][0]['name'] ), 'width': 100 },
																			{ 'text': '<input class="form-control" type="text" id="clusterName_d" value="myCluster">' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][0]['desc'] ) } ] ) ;
		//'描述：' '集群的描述'
		sdbjs.parts.tableBox.addBody( 'deployGuidTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][1]['name'] ), 'width': 100 },
																			{ 'text': '<input class="form-control" type="text" id="desc_d">' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][1]['desc'] ) } ] ) ;
		//'业务名：' '安装的业务名'
		sdbjs.parts.tableBox.addBody( 'deployGuidTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][2]['name'] ), 'width': 100 },
																			{ 'text': '<input class="form-control" type="text" id="businessName_d" value="myModule">' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][2]['desc'] ) } ] ) ;
		//'业务类型：'
		sdbjs.parts.tableBox.addBody( 'deployGuidTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][3]['name'] ), 'width': 100 },
																			{ 'text': '<select class="form-control" id="businessType_d"></select>' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][3]['desc'] ) } ] ) ;
		//'用户名：' '运行业务的用户名'
		sdbjs.parts.tableBox.addBody( 'deployGuidTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][4]['name'] ), 'width': 100 },
																			{ 'text': '<input class="form-control" type="text" id="userName_d" value="sdbadmin">' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][4]['desc'] ) } ] ) ;
		//'密码：' '用户密码'
		sdbjs.parts.tableBox.addBody( 'deployGuidTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][5]['name'] ), 'width': 100 },
																			{ 'text': '<input class="form-control" type="text" id="passwd_d" value="sdbadmin">' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][5]['desc'] ) } ] ) ;
		//'用户组：' '用户所属的组'
		sdbjs.parts.tableBox.addBody( 'deployGuidTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][6]['name'] ), 'width': 100 },
																			{ 'text': '<input class="form-control" type="text" id="userGroup_d" value="sdbadmin_group">' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][6]['desc'] ) } ] ) ;
		//'安装路径：' '业务默认的安装路径'
		sdbjs.parts.tableBox.addBody( 'deployGuidTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][7]['name']), 'width': 100 },
																			{ 'text': '<input class="form-control" type="text" id="installPath_d" value="/opt/sequoiadb/">' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['deploy']['body'][7]['desc'] ) } ] ) ;
	}, function( footObj ){
		sdbjs.parts.tableBox.create( footObj, 'deployGuidFootTable' ) ;
		sdbjs.parts.tableBox.addBody( 'deployGuidFootTable', [{ 'text': function( tdObj ){
																				sdbjs.parts.alertBox.create( tdObj, 'deployGuidAlert' ) ;
																				sdbjs.fun.setCSS( 'deployGuidAlert', { 'display': 'none', 'padding': '8px', 'text-align': 'left' } ) ;
																			} },
																			{ 'text': function( tdObj ){
																				sdbjs.parts.buttonBox.create( tdObj, 'deployGuidOK' ) ;
																				$( tdObj ).append( '&nbsp;' ) ;
																				sdbjs.parts.buttonBox.create( tdObj, 'deployGuidClose' ) ;
																				//确定
																				sdbjs.parts.buttonBox.update( 'deployGuidOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'deployGuid()' ) ;
																				//关闭
																				sdbjs.parts.buttonBox.update( 'deployGuidClose', function( buttonObj ){
																					$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'deployGuid' ) ;
																				}, 'primary' ) ;
																			}, 'width': 120  } ] ) ;
	} ) ;
	
	/* 创建集群的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'createCluster' ) ;
	//'创建集群'
	sdbjs.parts.modalBox.update( 'createCluster', htmlEncode( _languagePack['index']['modal']['createCluster']['title'] ), function( bodyObj ){
		sdbjs.parts.tableBox.create( bodyObj, 'createClusterTable' ) ;
		sdbjs.parts.tableBox.update( 'createClusterTable', 'loosen' ) ;
		//集群名 '部署的集群名'
		sdbjs.parts.tableBox.addBody( 'createClusterTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][0]['name'] ), 'width': 100 },
																				{ 'text': '<input class="form-control" type="text" id="clusterName_c" value="myCluster">' },
																				{ 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][0]['desc'] ) } ] ) ;
		//'描述：' '集群的描述'
		sdbjs.parts.tableBox.addBody( 'createClusterTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][1]['name'] ), 'width': 100 },
																				{ 'text': '<input class="form-control" type="text" id="desc_c">' },
																				{ 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][1]['desc'] ) } ] ) ;
		//'用户名：' '运行业务的用户名'
		sdbjs.parts.tableBox.addBody( 'createClusterTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][2]['name'] ), 'width': 100 },
																				{ 'text': '<input class="form-control" type="text" id="userName_c" value="sdbadmin">' },
																				{ 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][2]['desc'] ) } ] ) ;
		//'密码：' '用户密码'
		sdbjs.parts.tableBox.addBody( 'createClusterTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][3]['name'] ), 'width': 100 },
																			  	{ 'text': '<input class="form-control" type="text" id="passwd_c" value="sdbadmin">' },
																				{ 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][3]['desc'] ) } ] ) ;
		//'用户组：' '用户所属的组'
		sdbjs.parts.tableBox.addBody( 'createClusterTable', [ { 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][4]['name'] ), 'width': 100 },
																				{ 'text': '<input class="form-control" type="text" id="userGroup_c" value="sdbadmin_group">' },
																				{ 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][4]['desc'] ) } ] ) ;
		//'安装路径：' '业务默认的安装路径'
		sdbjs.parts.tableBox.addBody( 'createClusterTable', [ { 'text': htmlEncode(  _languagePack['index']['modal']['createCluster']['body'][5]['name'] ), 'width': 100 },
																				{ 'text': '<input class="form-control" type="text" id="installPath_c" value="/opt/sequoiadb/">' },
																				{ 'text': htmlEncode( _languagePack['index']['modal']['createCluster']['body'][5]['desc'] ) } ] ) ;
	}, function( footObj ){
		sdbjs.parts.tableBox.create( footObj, 'createClusterFootTable' ) ;
		sdbjs.parts.tableBox.addBody( 'createClusterFootTable', [{ 'text': function( tdObj ){
																				sdbjs.parts.alertBox.create( tdObj, 'createClusterAlert' ) ;
																				sdbjs.fun.setCSS( 'createClusterAlert', { 'display': 'none', 'padding': '8px', 'text-align': 'left' } ) ;
																			} },
																			{ 'text': function( tdObj ){
																				sdbjs.parts.buttonBox.create( tdObj, 'createClusterOK' ) ;
																				$( tdObj ).append( '&nbsp;' ) ;
																				sdbjs.parts.buttonBox.create( tdObj, 'createClusterClose' ) ;
																				//'确定'
																				sdbjs.parts.buttonBox.update( 'createClusterOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'createCluster()' ) ;
																				//'关闭'
																				sdbjs.parts.buttonBox.update( 'createClusterClose', function( buttonObj ){
																					$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'createCluster' ) ;
																				}, 'primary' ) ;
																			}, 'width': 120  } ] ) ;
		
	} ) ;
	
	/* 添加业务的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'addBusiness' ) ;
	sdbjs.parts.modalBox.update( 'addBusiness', htmlEncode( _languagePack['index']['modal']['addBusiness']['title'] ), function( bodyObj ){
		sdbjs.parts.tableBox.create( bodyObj, 'addBusinessTable' ) ;
		sdbjs.parts.tableBox.update( 'addBusinessTable', 'loosen' ) ;
		//'业务名：' '安装的业务名'
		sdbjs.parts.tableBox.addBody( 'addBusinessTable', [{ 'text': htmlEncode( _languagePack['index']['modal']['addBusiness']['body'][0]['name'] ), 'width': 100 },
																			{ 'text': '<input class="form-control" type="text" id="businessName_a" value="myModule">' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['addBusiness']['body'][0]['desc'] ) } ] ) ;
		//'业务类型：'
		sdbjs.parts.tableBox.addBody( 'addBusinessTable', [{ 'text': htmlEncode( _languagePack['index']['modal']['addBusiness']['body'][1]['name'] ), 'width': 100 },
																			{ 'text': '<select class="form-control" id="businessType_a"></select>' },
																			{ 'text': htmlEncode( _languagePack['index']['modal']['addBusiness']['body'][1]['desc'] ) } ] ) ;
	}, function( footObj ){
		sdbjs.parts.tableBox.create( footObj, 'addBusinessFootTable' ) ;
		sdbjs.parts.tableBox.addBody( 'addBusinessFootTable', [{ 'text': function( tdObj ){
																				sdbjs.parts.alertBox.create( tdObj, 'addBusinessFootAlert' ) ;
																				sdbjs.fun.setCSS( 'addBusinessFootAlert', { 'display': 'none', 'padding': '8px', 'text-align': 'left' } ) ;
																			} },
																			{ 'text': function( tdObj ){
																				sdbjs.parts.buttonBox.create( tdObj, 'addBusinessOK' ) ;
																				$( tdObj ).append( '&nbsp;' ) ;
																				sdbjs.parts.buttonBox.create( tdObj, 'addBusinessClose' ) ;
																				//'确定'
																				sdbjs.parts.buttonBox.update( 'addBusinessOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, '' ) ;
																				//'关闭'
																				sdbjs.parts.buttonBox.update( 'addBusinessClose', function( buttonObj ){
																					$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'addBusiness' ) ;
																				}, 'primary' ) ;
																			}, 'width': 120  } ] ) ;
		
	} ) ;
	
	/* 确认是否要删除集群 */
	sdbjs.parts.modalBox.create( $( document.body ), 'isRemoveCluster' ) ;
	sdbjs.parts.modalBox.update( 'isRemoveCluster', htmlEncode( _languagePack['index']['modal']['isRemoveCluster']['title'] ), function( bodyObj ){
		sdbjs.parts.alertBox.create( bodyObj, 'isRemoveClusterAlert' ) ;
		sdbjs.parts.alertBox.update( 'isRemoveClusterAlert', htmlEncode( _languagePack['tip']['web']['index'][1] ), 'warning' )
	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'isRemoveClusterOK' ) ;
		$( footObj ).append( '&nbsp;' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'isRemoveClusterClose' ) ;
		sdbjs.parts.buttonBox.update( 'isRemoveClusterOK', htmlEncode( _languagePack['public']['button']['ok']), 'primary', null, 'removeCluster()' ) ;
		sdbjs.parts.buttonBox.update( 'isRemoveClusterClose', function( buttonObj ){
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'isRemoveCluster' ) ;
		}, 'primary' ) ;
	} ) ;
	
	/* 确认是否要继续部署 */
	sdbjs.parts.modalBox.create( $( document.body ), 'isGoOnDeploy' ) ;
	sdbjs.parts.modalBox.update( 'isGoOnDeploy', htmlEncode( _languagePack['index']['modal']['isGoOnDeploy']['title'] ), function( bodyObj ){
		sdbjs.parts.alertBox.create( bodyObj, 'isGoOnDeployAlert' ) ;
		sdbjs.parts.alertBox.update( 'isGoOnDeployAlert', '', 'Info' ) ;
	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'isGoOnDeployOK' ) ;
		$( footObj ).append( '&nbsp;' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'isGoOnDeployClose' ) ;
		sdbjs.parts.buttonBox.update( 'isGoOnDeployOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, '' ) ;
		sdbjs.parts.buttonBox.update( 'isGoOnDeployClose', function( buttonObj ){
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'isGoOnDeploy' ) ;
		}, 'primary' ) ;
	} ) ;
}

$(document).ready(function(){
	createHtml() ;
	createDynamicHtml() ;
} ) ;