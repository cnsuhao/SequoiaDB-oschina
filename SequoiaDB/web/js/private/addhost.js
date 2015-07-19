////部署模式
var _deployModel = null ;

//集群名
var _clusterName = null ;

//安装路径
var _installPath = null ;

//主机列表
var _hostList = [] ;

//主机配置
var _hostConf = [] ;

//编辑主机列表
function editHostList( buttonObj )
{
	//'编辑'
	if( $( buttonObj ).text() === _languagePack['addhost']['leftPanel']['button'][0] )
	{
		$.each( _hostConf, function(index){
			
			sdbjs.parts.tabList.update( 'hostTabList', index, function( liObj ){
				$( liObj ).css( 'cursor', 'default' ) ;
				sdbjs.fun.addClick( liObj, '' ) ;
			} ) ;
			
			//只有没有错误的主机才需要激活
			if( typeof( _hostConf[index]['errno'] ) === 'undefined' ||  _hostConf[index]['errno'] === 0 )
			{
				var checkDiskNum = 0 ;
				$.each( _hostConf[index]['Disk'], function(index,diskInfo){
					if( diskInfo['isUse'] === true )
					{
						++checkDiskNum ;
					}
				} ) ;
				//必须至少1个磁盘选择才可以激活
				if( checkDiskNum > 0 )
				{
					sdbjs.parts.tableBox.updateBody( 'tabListTable_' + index, 0, 0, function( tdObj ){
						$( tdObj ).children( 'div' ).show() ;
					} ) ;
				}
			}
		} ) ;
		//'完成'
		$( buttonObj ).text( _languagePack['addhost']['leftPanel']['button'][1] ) ;
	}
	else
	{
		$.each( _hostConf, function(index,hostInfo){
			if( typeof( hostInfo['errno'] ) === 'undefined' || hostInfo['errno'] === 0 )
			{
				sdbjs.parts.tabList.update( 'hostTabList', index, function( liObj ){
					$( liObj ).css( 'cursor', 'pointer' ) ;
					sdbjs.fun.addClick( liObj, 'accessHostConf(' + index + ')' ) ;
				} ) ;
			}
			
			sdbjs.parts.tableBox.updateBody( 'tabListTable_' + index, 0, 0, function( tdObj ){
				$( tdObj ).children( 'div' ).hide() ;
			} ) ;
		} ) ;
		//'编辑'
		$( buttonObj ).text( _languagePack['addhost']['leftPanel']['button'][0] ) ;
	}
}

//激活主机
function activeHost( index, isCaller )
{
	_hostConf[index]['isUse'] = true ;
	sdbjs.fun.saveData( 'SdbHostConf', JSON.stringify( _hostConf ) ) ;
	sdbjs.parts.tabList.unDisable( 'hostTabList', index ) ;
	sdbjs.parts.tableBox.updateBody( 'tabListTable_' + index, 0, 0, function( tdObj ){
		sdbjs.fun.addClick( $( tdObj ).children( 'div[data-toggle="checkBox"]' ).get(0), 'disableHost(' + index + ')' ) ;
		if( isCaller === true )
		{
			$( tdObj ).children( 'div[data-toggle="checkBox"]' ).removeClass().addClass( 'checked' ) ;
		}
	} ) ;
}

//禁用主机
function disableHost( index, isCaller )
{
	_hostConf[index]['isUse'] = false ;
	sdbjs.fun.saveData( 'SdbHostConf', JSON.stringify( _hostConf ) ) ;
	sdbjs.parts.tabList.disable( 'hostTabList', index ) ;
	sdbjs.parts.tableBox.updateBody( 'tabListTable_' + index, 0, 0, function( tdObj ){
		sdbjs.fun.addClick( $( tdObj ).children( 'div[data-toggle="checkBox"]' ).get(0), 'activeHost(' + index + ')' ) ;
		if( isCaller === true )
		{
			$( tdObj ).children( 'div[data-toggle="checkBox"]' ).removeClass().addClass( 'unchecked' ) ;
		}
	} ) ;
}

//启动主机
function approval( index )
{
	_hostConf[index]['isUse'] = false ;
	sdbjs.fun.saveData( 'SdbHostConf', JSON.stringify( _hostConf ) ) ;
	sdbjs.parts.tableBox.updateBody( 'tabListTable_' + index, 0, 0, function( tdObj ){
		sdbjs.fun.addClick( $( tdObj ).children( 'div[data-toggle="checkBox"]' ).get(0), 'activeHost(' + index + ')' ) ;
		$( tdObj ).children( 'div[data-toggle="checkBox"]' ).removeClass().addClass( 'unchecked' ) ;
	} ) ;
}

//停用主机
function banHost( index )
{
	_hostConf[index]['isUse'] = false ;
	sdbjs.fun.saveData( 'SdbHostConf', JSON.stringify( _hostConf ) ) ;
	sdbjs.parts.tabList.disable( 'hostTabList', index ) ;
	sdbjs.parts.tableBox.updateBody( 'tabListTable_' + index, 0, 0, function( tdObj ){
		sdbjs.fun.addClick( $( tdObj ).children( 'div[data-toggle="checkBox"]' ).get(0), '' ) ;
		$( tdObj ).children( 'div[data-toggle="checkBox"]' ).removeClass().addClass( 'disunchecked' ) ;
	} ) ;
}

//激活或者取消磁盘
function changeDisk( obj, hostNum, diskNum )
{
	var checkDiskNum = 0 ;
	//存储到全局变量
	_hostConf[hostNum]['Disk'][diskNum]['isUse'] = obj.checked ;
	$.each( _hostConf[hostNum]['Disk'], function(index,diskInfo){
		if( diskInfo['isUse'] === true )
		{
			++checkDiskNum ;
		}
	} ) ;
	//设置显示已选择的磁盘数量
	sdbjs.parts.tableBox.updateBody( 'tabListTable_' + hostNum, 0, 2, function( tdObj ){
		$( tdObj ).children( '.badge-info' ).text( checkDiskNum ) ;
		//'已选择 ? 个磁盘'
		sdbjs.fun.setLabel( $( tdObj ).children( '.badge-info' ), htmlEncode( sdbjs.fun.sprintf( _languagePack['addhost']['leftPanel']['label'][0], checkDiskNum ) ) ) ;
	} ) ;
	//判断磁盘数量
	if( checkDiskNum > 0 )
	{
		approval( hostNum ) ;
		activeHost( hostNum, true ) ;
	}
	else
	{
		banHost( hostNum ) ;
	}
}

//设置安装路径
function saveInstallPath()
{
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 4, 1, function( tdObj ){
		var obj = $( tdObj ).children( 'input' ) ;
		var index = $( obj ).data( 'index' ) ;
		_hostConf[index]['InstallPath'] = $( obj ).val() ;
		sdbjs.fun.saveData( 'SdbHostConf', JSON.stringify( _hostConf ) ) ;
	} ) ;
}

//访问一个主机配置
function accessHostConf( index )
{
	//初始化主机列表延时的状态
	$.each( _hostConf, function(index2){
		sdbjs.parts.tabList.unActive( 'hostTabList', index2 ) ;
	} ) ;
	var hostConf = _hostConf[index] ;
	sdbjs.parts.tabList.active( 'hostTabList', index ) ;

	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 0, 1, htmlEncode( hostConf['HostName'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 1, 1, htmlEncode( hostConf['IP'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 2, 1, htmlEncode( hostConf['OS']['Distributor'] + ' ' + hostConf['OS']['Release'] + ' x' + hostConf['OS']['Bit'] ) ) ;
	//设置内存
	{
		var useMemory = hostConf['Memory']['Size'] - hostConf['Memory']['Free'] ;
		var percent = parseInt( useMemory * 100 / hostConf['Memory']['Size'] ) ;
		var color = 'green' ;
		if( percent > 60 && percent < 80 )
		{
			color = 'orange' ;
		}
		else if( percent >= 80 )
		{
			color = 'red' ;
		}
		sdbjs.parts.progressBox.update( 'hostMemory', percent, color, htmlEncode( useMemory + 'MB / ' + hostConf['Memory']['Size'] + 'MB' ) ) ;
	}
	//安装路径
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 4, 1, function( tdObj ){
		if( typeof( hostConf['InstallPath'] ) === 'undefined' )
		{
			hostConf['InstallPath'] = _installPath ;
		}
		$( tdObj ).children( 'input' ).data( 'index', index ).val( hostConf['InstallPath'] ) ;
	} ) ;
	//OM
	sdbjs.parts.tableBox.updateBody( 'omAgentTable', 0, 1, htmlEncode( hostConf['OMA']['Version'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'omAgentTable', 1, 1, htmlEncode( hostConf['OMA']['SdbUser'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'omAgentTable', 2, 1, htmlEncode( hostConf['OMA']['Path'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'omAgentTable', 3, 1, htmlEncode( hostConf['OMA']['Service'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'omAgentTable', 4, 1, htmlEncode( hostConf['OMA']['Release'] ) ) ;
	//磁盘
	sdbjs.parts.gridBox.emptyBody( 'hostDiskGrid' ) ;
	$.each( hostConf['Disk'], function(index2,hostDisk){
		if( hostDisk['Size'] === 0 || hostDisk['Name'] === 'none' )
		{
			//return true ;
		}
		var useDisk = hostDisk['Size'] - hostDisk['Free'] ;
		var percentDisk = parseInt( useDisk * 100 / hostDisk['Size'] ) ;
		var inputBox = '<input type="checkbox" checked="checked" onclick="changeDisk(this,' + index + ',' + index2 + ')">' ;
		var progress = null ;
		var color = 'green' ;
		if( percentDisk > 60 && percentDisk < 80 )
		{
			color = 'orange' ;
		}
		else if( percentDisk >= 80 )
		{
			color = 'red' ;
		}
		progress = '<div class="progress"><span class="reading">' + useDisk + 'MB / ' + hostDisk['Size'] + 'MB</span><span class="bar ' + color + '" style="width:' + percentDisk + '%;"></span></div>' ;
		if( ( typeof( _hostConf[index]['Disk'][index2]['isUse'] ) === 'undefined' && hostDisk['IsLocal'] === false ) || _hostConf[index]['Disk'][index2]['isUse'] === false )
		{
			inputBox = '<input type="checkbox" onclick="changeDisk(this,' + index + ',' + index2 + ')">' ;
		}
		if( hostDisk['CanUse'] === false )
		{
			inputBox = '<input type="checkbox" disabled="disabled">' ;
		}
		sdbjs.parts.gridBox.addBody( 'hostDiskGrid', [{ 'text': inputBox, 'width': '10%' },
																	 { 'text': htmlEncode( hostDisk['Name'] ), 'width': '15%' },
																	 { 'text': htmlEncode( hostDisk['Mount'] ), 'width': '25%' },
																	 //'是' '否'
																	 { 'text': htmlEncode( hostDisk['IsLocal'] === false ? _languagePack['addhost']['rightPanel']['disk']['other'][0] : _languagePack['addhost']['rightPanel']['disk']['other'][1] ), 'width': '15%' },
																	 { 'text': progress, 'width': '35%' } ] ) ;
	} ) ;
	sdbjs.fun.setCSS( 'hostDiskGrid~body', { 'max-height': 300 } ) ;
	//处理器
	sdbjs.parts.gridBox.emptyBody( 'hostCPUGrid' ) ;
	$.each( hostConf['CPU'], function(index2,hostCPU){
		sdbjs.parts.gridBox.addBody( 'hostCPUGrid', [{ 'text': htmlEncode( hostCPU['ID'] ), 'width': '15%' },
																	{ 'text': htmlEncode( hostCPU['Model'] ), 'width': '50%' },
																	{ 'text': htmlEncode( hostCPU['Core'] ), 'width': '15%' },
																	{ 'text': htmlEncode( hostCPU['Freq'] ), 'width': '20%' } ] ) ;
	} ) ;
	sdbjs.fun.setCSS( 'hostCPUGrid~body', { 'max-height': 200 } ) ;
	//网卡
	sdbjs.parts.gridBox.emptyBody( 'hostNetGrid' ) ;
	$.each( hostConf['Net'], function(index2,hostNet){
		sdbjs.parts.gridBox.addBody( 'hostNetGrid', [{ 'text': htmlEncode( hostNet['Name'] ), 'width': '15%' },
																	{ 'text': htmlEncode( hostNet['Model'] ), 'width': '35%' },
																	{ 'text': htmlEncode( hostNet['Bandwidth'] ), 'width': '15%' },
																	{ 'text': htmlEncode( hostNet['IP'] ), 'width': '35%' } ] ) ;
	} ) ;
	sdbjs.fun.setCSS( 'hostNetGrid~body', { 'max-height': 200 } ) ;
	//端口
	sdbjs.parts.gridBox.emptyBody( 'hostPortGrid' ) ;
	$.each( hostConf['Port'], function(index2,hostPort){
		sdbjs.parts.gridBox.addBody( 'hostPortGrid', [{ 'text': htmlEncode( hostPort['Port'] ), 'width': '50%' },
																	 //'未占用' '已占用'
																	 { 'text': htmlEncode( hostPort['CanUse'] === true ? _languagePack['addhost']['rightPanel']['port']['other'][0] : _languagePack['addhost']['rightPanel']['port']['other'][1] ), 'width': '50%' } ] ) ;
	} ) ;
	sdbjs.fun.setCSS( 'hostPortGrid~body', { 'max-height': 200 } ) ;
	//服务
	sdbjs.parts.gridBox.emptyBody( 'hostServiceGrid' ) ;
	$.each( hostConf['Service'], function(index2,hostService){
		sdbjs.parts.gridBox.addBody( 'hostServiceGrid', [{ 'text': htmlEncode( hostService['Name'] ), 'width': '33%' },
																		 //'正在运行' '未运行'
																		 { 'text': htmlEncode( hostService['IsRunning'] === true ? _languagePack['addhost']['rightPanel']['service']['other'][0] : _languagePack['addhost']['rightPanel']['service']['other'][1] ), 'width': '33%' },
																		 { 'text': htmlEncode( hostService['Version'] ) } ] ) ;
	} ) ;
	sdbjs.fun.setCSS( 'hostServiceGrid~body', { 'max-height': 200 } ) ;
}

//返回
function returnPage()
{
	gotoPage( 'scanhost.html' ) ;
}


// 下一步
function nextPage()
{
	var tempPoraryHosts = [] ;
	$.each( _hostConf, function( index, hostInfo ){
		//判断是不是错误的主机
		if( ( typeof( hostInfo['errno'] ) === 'undefined' || hostInfo['errno'] === 0 ) && hostInfo['isUse'] === true )
		{
			var tempHostInfo = {} ;
			tempHostInfo['HostName']		= hostInfo['HostName'] ;
			tempHostInfo['IP']				= hostInfo['IP'] ;
			tempHostInfo['User']				= hostInfo['User'] ;
			tempHostInfo['Passwd']			= hostInfo['Passwd'] ;
			tempHostInfo['SshPort']			= hostInfo['SshPort'] ;
			tempHostInfo['AgentService']	= hostInfo['AgentService'] ;
			tempHostInfo['CPU']				= hostInfo['CPU'] ;
			tempHostInfo['Memory']			= hostInfo['Memory'] ;
			tempHostInfo['Net']				= hostInfo['Net'] ;
			tempHostInfo['Port']				= hostInfo['Port'] ;
			tempHostInfo['Service']			= hostInfo['Service'] ;
			tempHostInfo['OMA']				= hostInfo['OMA'] ;
			tempHostInfo['Safety']			= hostInfo['Safety'] ;
			tempHostInfo['OS']				= hostInfo['OS'] ;
			tempHostInfo['InstallPath']	= hostInfo['InstallPath'] ;
			tempHostInfo['Disk']				= [] ;
			$.each( hostInfo['Disk'], function( index2, hostDisk ){
				if( hostDisk['isUse'] === true )
				{
					var tempHostDisk = {} ;
					tempHostDisk['Name']    = hostDisk['Name'] ;
					tempHostDisk['Mount']   = hostDisk['Mount'] ;
					tempHostDisk['Size']    = hostDisk['Size'] ;
					tempHostDisk['Free']    = hostDisk['Free'] ;
					tempHostDisk['IsLocal'] = hostDisk['IsLocal'] ;
					tempHostInfo['Disk'].push( tempHostDisk ) ;
				}
			} ) ;
			tempPoraryHosts.push( tempHostInfo ) ;
		}
	} ) ;
	if( tempPoraryHosts.length > 0 )
	{
		sdbjs.parts.loadingBox.show( 'loading' ) ;
		restAddHosts( true, function( jsonArr, textStatus, jqXHR ){
			var taskID = jsonArr[0]['TaskID'] ;
			sdbjs.fun.saveData( 'SdbTaskID', taskID ) ;
			gotoPage( 'installhost.html' ) ;
		}, function( json ){
			sdbjs.parts.loadingBox.hide( 'loading' ) ;
			showProcessError( json['detail'] ) ;
		}, null, _clusterName, tempPoraryHosts ) ;
	}
	else
	{
		//'所有主机都是禁用状态，无法添加主机。'
		showFootStatus( 'danger', _languagePack['error']['web']['addhost'][0] ) ;
	}
}

//过滤主机
function filterHosts( inputObj )
{
	var str = $( inputObj ).val()
	$.each( _hostConf, function(index,hostInfo){
		if( hostInfo['HostName'].indexOf( str ) !== -1 || hostInfo['IP'].indexOf( str ) !== -1 )
		{
			sdbjs.parts.tabList.show( 'hostTabList', index ) ;
		}
		else
		{
			sdbjs.parts.tabList.hide( 'hostTabList', index ) ;
		}
	} ) ;
}

//创建主机列表
function createHostList()
{
	var isActive = false ;
	$.each( _hostConf, function(index,hostInfo){
		if( typeof( hostInfo['errno'] ) === 'undefined' || hostInfo['errno'] === 0 )
		{
			if( typeof( hostInfo['InstallPath'] ) === 'undefined' )
			{
				if( hostInfo['OMA']['Path'] !== ''  )
				{
					hostInfo['InstallPath'] = hostInfo['OMA']['Path'] ;
				}
				else
				{
					hostInfo['InstallPath'] = _installPath ;
				}
			}
		}
		//把hostname和ip匹配到返回的数据中
		$.each( _hostList, function(index2,hostInfoTemp){
			if( hostInfoTemp['HostName'] === hostInfo['HostName'] || hostInfoTemp['IP'] === hostInfo['IP'] )
			{
				hostInfo['HostName'] = hostInfoTemp['HostName'] ;
				hostInfo['IP'] = hostInfoTemp['IP'] ;
				hostInfo['User'] = hostInfoTemp['User'] ;
				hostInfo['Passwd'] = hostInfoTemp['Passwd'] ;
				hostInfo['SshPort'] = hostInfoTemp['SshPort'] ;
				hostInfo['AgentService'] = hostInfoTemp['AgentService'] ;
				return false ;
			}
		} ) ;
		var isError = false ;
		var canUseDisk = 0 ;
		var unUseDisk = 0 ;
		var hostStatusTemp = null ;
		var hostStatusOperate = null ;
		//判断是不是错误的主机
		if( typeof( hostInfo['errno'] ) === 'undefined' || hostInfo['errno'] === 0 )
		{
			$.each( hostInfo['Disk'], function(index2,hostDisk){
				if( hostDisk['Size'] === 0 || hostDisk['Name'] === 'none' )
				{
					//return true ;
				}
				if( hostDisk['CanUse'] === true && hostDisk['IsLocal'] === true && ( typeof( hostDisk['isUse'] ) === 'undefined' || hostDisk['isUse'] === true ) )
				{
					hostDisk['isUse'] = true ;
					++canUseDisk ;
				}
				else
				{
					hostDisk['isUse'] = false ;
					if ( hostDisk['CanUse'] === false )
					{
						++unUseDisk ;
					}
				}
			} ) ;
			hostStatusTemp = '<span class="badge badge-info">' + canUseDisk + '</span>' ;
			if( unUseDisk > 0 )
			{
				if( hostInfo['OMA']['Version'] !== '' ||
					 hostInfo['OMA']['SdbUser'] !== '' ||
					 hostInfo['OMA']['Path']    !== '' ||
					 hostInfo['OMA']['Service'] !== '' ||
					 hostInfo['OMA']['Release'] !== '' )
				{
					hostStatusTemp += '&nbsp;<span class="badge badge-warning">' + ( unUseDisk + 1 ) + '</span>' ;
				}
				else
				{
					hostStatusTemp += '&nbsp;<span class="badge badge-warning">' + unUseDisk + '</span>' ;
				}
			}
			if( canUseDisk > 0 )
			{
				hostStatusOperate = '<div class="checked" data-toggle="checkBox" onClick="disableHost(' + index + ',false)"></div>' ;
			}
			else
			{
				hostStatusOperate = '<div class="disunchecked" data-toggle="checkBox"></div>' ;
			}
		}
		else if( typeof( hostInfo['errno'] ) !== 'undefined' && hostInfo['errno'] !== 0 )
		{
			isError = true ;
			hostStatusTemp = '<span class="badge badge-danger">Error</span>' ;
			hostStatusOperate = '<div class="disunchecked" data-toggle="checkBox"></div>' ;
		}
		sdbjs.parts.tabList.add( 'hostTabList', function( liObj ){
			$( liObj ).css( 'zoom', 1 ) ;
			sdbjs.parts.tableBox.create( liObj, 'tabListTable_' + index ) ;
			sdbjs.parts.tableBox.update( 'tabListTable_' + index, 'compact' ) ;
			sdbjs.parts.tableBox.addBody( 'tabListTable_' + index, [{ 'text': hostStatusOperate },
																					  { 'text': '<div style="padding:2px 2px 2px 0;">' + htmlEncode( hostInfo['HostName'] ) + '</div><div style="color:#666">' + htmlEncode( hostInfo['IP'] ) + '</div>' },
																					  { 'text': hostStatusTemp } ] ) ;
			sdbjs.parts.tableBox.updateBody( 'tabListTable_' + index, 0, 2, function( tdObj ){
				$( tdObj ).css( 'text-align', 'right' ) ;
				if( isError === false )
				{
					var warningStr = '' ;
					//'已选择 ? 个磁盘'
					sdbjs.fun.setLabel( $( tdObj ).children( '.badge-info' ), htmlEncode( sdbjs.fun.sprintf( _languagePack['addhost']['leftPanel']['label'][0], canUseDisk ) ) ) ;
					if( unUseDisk > 0 )
					{
						//'有 ? 个磁盘剩余容量不足'
						warningStr += '<p>' + htmlEncode( sdbjs.fun.sprintf( _languagePack['addhost']['leftPanel']['label'][1], unUseDisk ) ) + '</p>' ;
					}
					if( hostInfo['OMA']['Version'] !== '' ||
						 hostInfo['OMA']['SdbUser'] !== '' ||
						 hostInfo['OMA']['Path']    !== '' ||
						 hostInfo['OMA']['Service'] !== '' ||
						 hostInfo['OMA']['Release'] !== '' )
					{
						//有OM agent信息
						warningStr += '<p>' + htmlEncode( _languagePack['addhost']['leftPanel']['label'][2] ) + '</p>' ;
						if( hostInfo['isUse'] !== true )
						{
							disableHost( index, true ) ;
						}
					}
					if( warningStr !== '' )
					{
						sdbjs.fun.setLabel( $( tdObj ).children( '.badge-warning' ), warningStr ) ;
					}
				}
				else
				{
					sdbjs.fun.setLabel( $( tdObj ).children( '.badge-danger' ), htmlEncode( hostInfo['detail'] ) );
				}
			} ) ;
			if( isError === false )
			{
				$( liObj ).css( 'cursor', 'pointer' ) ;
				sdbjs.fun.addClick( liObj, 'accessHostConf(' + index + ')' ) ;
			}
		} ) ;
		if( typeof( hostInfo['isUse'] ) !== 'undefined'  && hostInfo['isUse'] === false )
		{
			hostInfo['isUse'] = false ;
			if( canUseDisk > 0 )
			{
				disableHost( index, true ) ;
			}
			else
			{
				disableHost( index, false ) ;
				banHost( index ) ;
			}
		}
		else
		{
			hostInfo['isUse'] = !isError ;
		}
		if( isError === false && isActive === false )
		{
			isActive = true ;
			accessHostConf( index ) ;
		}
		if( canUseDisk <= 0 )
		{
			hostInfo['isUse'] = false ;
			sdbjs.parts.tabList.disable( 'hostTabList', index ) ;
		}
		
	} ) ;
}

//加载主机列表
function loadHostList()
{
	if( _hostList !== null )
	{
		sdbjs.parts.loadingBox.show( 'loading' ) ;
		_hostList = JSON.parse( _hostList ) ;
		restCheckHost( true, function( jsonArr, textStatus, jqXHR ){
			_hostConf = jsonArr ;
			createHostList() ;
		}, function( json ){
			showProcessError( json['detail'] ) ;
		}, function(){
			sdbjs.parts.loadingBox.hide( 'loading' ) ;
		}, _clusterName, _hostList ) ;
	}
}

//加载cluster全局安装路径
function loadInstallPath()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restGetInstallPath( false, function( jsonArr, textStatus, jqXHR ){
		_installPath = jsonArr[0]['InstallPath'] ;
	}, function( json ){
		showProcessError( json['detail'] ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
	}, _clusterName ) ;
}

function createDynamicHtml()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	loadInstallPath() ;
	if( _hostConf.length <= 0 )
	{
		loadHostList() ;
	}
	else
	{
		createHostList() ;
	}
	sdbjs.parts.loadingBox.hide( 'loading' ) ;
}

function createHtml()
{
	createPublicHtml() ;

	/* 分页 */
	sdbjs.parts.tabPageBox.create( 'top2', 'tab' ) ;
	sdbjs.fun.setCSS( 'tab', { 'padding-top': 5 } ) ;

	//扫描主机
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/zoom.png"> ' + htmlEncode( _languagePack['public']['tabPage'][2] ), false, null ) ;
	//添加主机
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][3] ), true, null ) ;
	//安装主机
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][4] ), false, null );
	if( _deployModel === 'Deploy' )
	{
		//配置业务
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cube.png"> ' + htmlEncode( _languagePack['public']['tabPage'][5] ), false, null ) ;
		//修改业务
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/doc_lines_stright.png"> ' + htmlEncode( _languagePack['public']['tabPage'][6] ), false, null );
		//安装业务
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][7] ), false, null );
	}
	
	/* 左边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-left', 460, 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-left', { 'float': 'left', 'padding': '10px' } ) ;
	
	/* 左边 状态栏 */
	sdbjs.parts.panelBox.create( 'middle-left', 'hostListBar', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'hostListBar', { 'overflow': 'auto' } ) ;
	//'主机列表'
	sdbjs.parts.panelBox.update( 'hostListBar', htmlEncode( _languagePack['addhost']['leftPanel']['title'] ), function( panelBody ){
		sdbjs.parts.divBox.create( panelBody['name'], 'editDiv' ) ;
		sdbjs.fun.setCSS( 'editDiv', { 'padding-bottom': '5px', 'height': '35px' } ) ;
		sdbjs.parts.divBox.update( 'editDiv', function( obj ){
			var browser = sdbjs.fun.getBrowserInfo() ;
			//'编辑'
			//$( obj ).append( '<button style="float:left;" class="btn btn-lg btn-default" id="editButton" onClick="editHostList(this)">' + htmlEncode( _languagePack['addhost']['leftPanel']['button'][0] ) + '</button>' ) ;
			$( obj ).append( '<input style="float:left;width:375px;border-radius:5px 0 0 5px;border-right:0;" class="form-control" type="search">' ) ;
			$( obj ).children( 'input' ).on( 'input propertychange', function(){
				filterHosts( this ) ;
			} ) ;
			$( obj ).append( '<button style="float:left;border-radius:0 5px 5px 0;border-left:0;background: transparent url(./images/smallicon/blacks/16x16/zoom2.png) no-repeat center;height:26px;width:25px;" class="btn btn-lg btn-default"></button>' ) ;
			$( obj ).append( '<div class="clear-float"></div>' ) ;
		} ) ;
		sdbjs.parts.tabList.create( panelBody['name'], 'hostTabList' ) ;
	} ) ;

	/* 右边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-right', 'variable', 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-right', { 'float': 'left', 'padding': '10px', 'padding-left': 0 } ) ;
	
	/* 右边 主机列表 */
	sdbjs.parts.panelBox.create( 'middle-right', 'hostInfoBar', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'hostInfoBar', { 'overflow': 'auto', 'position': 'relative' } ) ;
	//'主机信息'
	sdbjs.parts.panelBox.update( 'hostInfoBar', htmlEncode( _languagePack['addhost']['rightPanel']['title'] ), function( panelBody ){
		
		sdbjs.parts.tableBox.create( panelBody['name'], 'hostInfoTable' ) ;
		sdbjs.fun.setCSS( 'hostInfoTable', { 'color': '#666' } ) ;
		sdbjs.parts.tableBox.update ( 'hostInfoTable', 'loosen simple' ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'HostName' ) + '</b>', 'width': 100 }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'IP' ) + '</b>', 'width': 100 }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'OS' ) + '</b>', 'width': 100 }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'Memory' ) + '</b>', 'width': 100 }, { 'text': function( tdObj ){
			sdbjs.parts.progressBox.create( tdObj, 'hostMemory' ) ;
		} } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'Install Path' ) + '</b>', 'width': 100 }, { 'text': '<input class="form-control" type="text">' } ] ) ;
		sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 4, 1, function( tdObj ){
			$( tdObj ).children( 'input' ).change( saveInstallPath ) ;
		} ) ;
	
		$( panelBody['obj'] ).append( '<div style="padding:10px 0 5px 0;font-weight:bold;font-size:16px;">' + htmlEncode( 'SAC Agent' ) + '</div>' ) ;
		sdbjs.parts.tableBox.create( panelBody['name'], 'omAgentTable' ) ;
		sdbjs.fun.setCSS( 'omAgentTable', { 'color': '#666' } ) ;
		sdbjs.parts.tableBox.update ( 'omAgentTable', 'loosen simple' ) ;
		sdbjs.parts.tableBox.addBody( 'omAgentTable', [ { 'text': '<b>' + htmlEncode( 'Version' ) + '</b>', 'width': 100 }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'omAgentTable', [ { 'text': '<b>' + htmlEncode( 'User' ) + '</b>', 'width': 100 }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'omAgentTable', [ { 'text': '<b>' + htmlEncode( 'Path' ) + '</b>', 'width': 100 }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'omAgentTable', [ { 'text': '<b>' + htmlEncode( 'Service' ) + '</b>', 'width': 100 }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'omAgentTable', [ { 'text': '<b>' + htmlEncode( 'Release' ) + '</b>', 'width': 100 }, { 'text': '' } ] ) ;
		//'磁盘'
		$( panelBody['obj'] ).append( '<div style="padding:15px 0 5px 0;font-weight:bold;font-size:16px;">' + htmlEncode( _languagePack['addhost']['rightPanel']['disk']['title'] ) + '</div>' ) ;
		//'磁盘' '路径' '网络映射' '容量'
		sdbjs.parts.gridBox.create( panelBody['name'], 'hostDiskGrid' ) ;
		sdbjs.parts.gridBox.addTitle( 'hostDiskGrid', [{ 'text': '', 'width': '10%' },
																	  { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['disk']['grid'][0] ), 'width': '15%' },
																	  { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['disk']['grid'][1] ), 'width': '25%' },
																	  { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['disk']['grid'][2] ), 'width': '15%' },
																	  { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['disk']['grid'][3] ), 'width': '35%' } ]  ) ;
		//'处理器'
		$( panelBody['obj'] ).append( '<div style="padding:15px 0 5px 0;font-weight:bold;font-size:16px;">' + htmlEncode( _languagePack['addhost']['rightPanel']['cpu']['title'] ) + '</div>' ) ;
		sdbjs.parts.gridBox.create( panelBody['name'], 'hostCPUGrid' ) ;
		//'ID' '类型' '核心数量' '主频'
		sdbjs.parts.gridBox.addTitle( 'hostCPUGrid', [{ 'text': htmlEncode( _languagePack['addhost']['rightPanel']['cpu']['grid'][0] ), 'width': '15%' },
																	 { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['cpu']['grid'][1] ), 'width': '50%' },
																	 { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['cpu']['grid'][2] ), 'width': '15%' },
																	 { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['cpu']['grid'][3] ), 'width': '20%' } ]  ) ;
		//'网卡'
		$( panelBody['obj'] ).append( '<div style="padding:15px 0 5px 0;font-weight:bold;font-size:16px;">' + htmlEncode( _languagePack['addhost']['rightPanel']['net']['title'] ) + '</div>' ) ;
		sdbjs.parts.gridBox.create( panelBody['name'], 'hostNetGrid' ) ;
		//'接口' '类型' '速率' '地址'
		sdbjs.parts.gridBox.addTitle( 'hostNetGrid', [{ 'text': htmlEncode( _languagePack['addhost']['rightPanel']['net']['grid'][0] ), 'width': '15%' },
																	 { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['net']['grid'][1] ), 'width': '35%' },
																	 { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['net']['grid'][2] ), 'width': '15%' },
																	 { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['net']['grid'][3] ), 'width': '35%' } ]  ) ;
		//'端口'
		$( panelBody['obj'] ).append( '<div style="padding:15px 0 5px 0;font-weight:bold;font-size:16px;">' + htmlEncode( _languagePack['addhost']['rightPanel']['port']['title'] ) + '</div>' ) ;
		sdbjs.parts.gridBox.create( panelBody['name'], 'hostPortGrid' ) ;
		//'端口' '状态'
		sdbjs.parts.gridBox.addTitle( 'hostPortGrid', [{ 'text': htmlEncode( _languagePack['addhost']['rightPanel']['port']['grid'][0] ), 'width': '50%' },
																	  { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['port']['grid'][1] ), 'width': '50%' } ]  ) ;
		//'服务'
		$( panelBody['obj'] ).append( '<div style="padding:15px 0 5px 0;font-weight:bold;font-size:16px;">' + htmlEncode( _languagePack['addhost']['rightPanel']['service']['title'] ) + '</div>' ) ;
		//'服务名' '状态' '版本'
		sdbjs.parts.gridBox.create( panelBody['name'], 'hostServiceGrid' ) ;
		sdbjs.parts.gridBox.addTitle( 'hostServiceGrid', [{ 'text': htmlEncode( _languagePack['addhost']['rightPanel']['service']['grid'][0] ), 'width': '33%' },
																		  { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['service']['grid'][1] ), 'width': '33%' },
																		  { 'text': htmlEncode( _languagePack['addhost']['rightPanel']['service']['grid'][2] ) } ] ) ;
	} ) ;
	
	/* ** */
	sdbjs.parts.divBox.create( 'middle', 'middle-clear', 0, 0 ) ;
	sdbjs.fun.setClass( 'middle-clear', 'clear-float' ) ;

	//返回 下一步
	sdbjs.parts.buttonBox.create( 'operate', 'deployReturn' ) ;
	sdbjs.parts.buttonBox.update( 'deployReturn', function( buttonObj ){
		$( buttonObj ).text( _languagePack['public']['button']['return'] ) ;
		sdbjs.fun.addClick( buttonObj, 'returnPage()' ) ;
	}, 'primary' ) ;
	var operateNode = sdbjs.fun.getNode( 'operate', 'divBox' ) ;
	$( operateNode['obj'] ).append( '&nbsp;' ) ;
	sdbjs.parts.buttonBox.create( 'operate', 'deployNext' ) ;
	sdbjs.parts.buttonBox.update( 'deployNext', function( buttonObj ){
		$( buttonObj ).text( _languagePack['public']['button']['next'] ) ;
		sdbjs.fun.addClick( buttonObj, 'nextPage()' ) ;
	}, 'primary' ) ;
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
	_hostConf = sdbjs.fun.getData( 'SdbHostConf' ) ;
	if( _hostConf === null )
	{
		_hostConf = [] ;
		_hostList = sdbjs.fun.getData( 'SdbHostList' ) ;
		if( _hostList === null )
		{
			rc = false ;
			gotoPage( 'index.html' ) ;
		}
	}
	else
	{
		_hostConf = JSON.parse( _hostConf ) ;
	}
	return rc ;
}

$(document).ready(function(){
	if( checkReady() )
	{
		sdbjs.fun.saveData( 'SdbStep', 'addhost' ) ;
		createHtml() ;
		createDynamicHtml() ;
	}
} ) ;