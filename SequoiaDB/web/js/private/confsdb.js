
//集群名
var _clusterName = null ;

//业务名
var _businessName = null ;

//部署模式
var _deployModel = null ;

//业务类型
var _businessType = null ;

//业务模板
var _businessTemplate = null ;

//业务配置
var _businessConfig = null ;

//主机列表详细信息
var _hostsInfo = null ;

//饼状图的对象
var _rightPiePic = null ;

$(window).resize(function(){
	if( _rightPiePic !== null )
	{
		_rightPiePic.resize() ;
	}
} ) ;

//获取容量预计
function getPredictCapacity()
{
	var tempHostInfo = [] ;
	$.each( _hostsInfo, function( index, hostInfo ){
		if( hostInfo['isUse'] === true )
		{
			tempHostInfo.push( { 'HostName': hostInfo['HostName'] } ) ;
		}
	} ) ;
	if( tempHostInfo.length === _hostsInfo.length )
	{
		sdbjs.fun.free( tempHostInfo ) ;
		tempHostInfo = null ;
	}
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restGetPredictCapacity( true, function( jsonArr, textStatus, jqXHR ){
		var totalSize = jsonArr[0]['TotalSize'] ;
		var validSize = jsonArr[0]['ValidSize'] ;
		var invalidSize = totalSize - validSize ;
		var percent = jsonArr[0]['RedundancyRate'] * 100 ;
		//'容量信息' '可用容量', '冗余容量' '总容量: '
		var option = {'title': { 'text': _languagePack['confsdb']['rightPanel'][0]['pie']['title'], 'x': 'center' },
						  'tooltip': { 'trigger': 'item', 'formatter': '{a} <br/>{b} : {c}MB ( {d}% )' },
						  'legend': { 'orient': 'vertical', 'x': 'left', 'data': [ _languagePack['confsdb']['rightPanel'][0]['pie']['data'][0], _languagePack['confsdb']['rightPanel'][0]['pie']['data'][1] ] },
						  'calculable': true,
						  'series': [ {'name': _languagePack['confsdb']['rightPanel'][0]['pie']['data'][2] + totalSize + 'MB',
											'type': 'pie',
											'radius': '100px',
											'center': ['200px', '160px'],
											'data': [{ 'value': validSize, 'name': _languagePack['confsdb']['rightPanel'][0]['pie']['data'][0] },
														{ 'value': invalidSize, 'name': _languagePack['confsdb']['rightPanel'][0]['pie']['data'][1] } ] } ] } ;
		if( _rightPiePic !== null )
		{
			_rightPiePic.clear() ;
		}
		_rightPiePic.setOption( option ) ;
		sdbjs.parts.tableBox.updateBody( 'PredictTable', 0, 1, htmlEncode( sizeConvert( totalSize ) ) ) ;
		sdbjs.parts.tableBox.updateBody( 'PredictTable', 1, 1, htmlEncode( sizeConvert( validSize ) ) ) ;
		sdbjs.parts.tableBox.updateBody( 'PredictTable', 2, 1, htmlEncode( sizeConvert( invalidSize ) ) ) ;
		sdbjs.parts.tableBox.updateBody( 'PredictTable', 3, 1, htmlEncode( percent + '%' ) ) ;
	}, function( json ){
		showProcessError( json['detail'] ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
	}, _clusterName, _businessConfig['Property'], tempHostInfo ) ;
}

//计算总预览
function getHostNodePreview( DeployMod )
{
	var replicaNum = 0 ;
	var groupNum = 0 ;
	var catalogNum = 0 ;
	var coordNum = 0 ;
	var hostsNum = 0 ;
	var diskNum = 0 ;
	var nodeSpread = 0 ;
	var sumNode = 0 ;

	$.each( _businessConfig['Property'], function( index, property ){
		if( property['Name'] === 'replicanum' )
		{
			replicaNum = parseInt( property['Value'] ) ;
		}
		if( property['Name'] === 'datagroupnum' )
		{
			groupNum = parseInt( property['Value'] ) ;
		}
		if( property['Name'] === 'catalognum' )
		{
			catalogNum = parseInt( property['Value'] ) ;
		}
		if( property['Name'] === 'coordnum' )
		{
			coordNum = parseInt( property['Value'] ) ;
		}
	} ) ;
		
	$.each( _hostsInfo, function( index, hostInfo ){
		if( hostInfo['isUse'] === true )
		{
			diskNum += hostInfo['Disk'].length ;
			++hostsNum ;
		}
	} ) ;

	if( coordNum === 0 )
	{
		coordNum = hostsNum ;
	}
	
	if( DeployMod === 'distribution' )
	{
		sumNode = replicaNum * groupNum + catalogNum + coordNum ;
	}
	else if( DeployMod === 'standalone' )
	{
		sumNode = 1 ;
	}
	nodeSpread = twoDecimalPlaces( sumNode / diskNum ) ;
	nodeSpread = nodeSpread < 1 ? 1 : nodeSpread ;
	
	sdbjs.parts.tableBox.updateBody( 'otherTable', 0, 1, htmlEncode( hostsNum ) ) ;
	sdbjs.parts.tableBox.updateBody( 'otherTable', 1, 1, htmlEncode( sumNode ) ) ;
	sdbjs.parts.tableBox.updateBody( 'otherTable', 2, 1, htmlEncode( diskNum ) ) ;
	sdbjs.parts.tableBox.updateBody( 'otherTable', 3, 1, htmlEncode( nodeSpread ) ) ;
}

//预览
function previewInfo()
{
	var rc = true ;
	var selectObj = sdbjs.fun.getNode( 'businessSelect', 'selectBox' ) ;
	selectObj = selectObj['obj'] ;
	var modNum = selectObj.get(0).selectedIndex ;
	var templateProperty = _businessTemplate[modNum]['Property'] ;
	$.each( _businessConfig['Property'], function( index, property ){
		sdbjs.parts.gridBox.updateBody( 'businessConfGrid', index, 1, function( tdObj ){
			var value = $( tdObj ).children( '.form-control' ).val() ;
			var rs = checkInputValue(templateProperty[index]['Display'],
											 templateProperty[index]['Type'],
											 templateProperty[index]['Valid'],
											 templateProperty[index]['WebName'],
											 value ) ;
			if( rs !== '' )
			{
				rc = false ;
				showFootStatus( 'danger', rs ) ;
				return ;
			}
			property['Value'] = value ;
		} ) ;
		if( rc === false )
		{
			return false ;
		}
	} ) ;
	if( rc === true )
	{
		var selectNum = 0 ;
		$.each( _hostsInfo, function( index, hostInfo ){
			sdbjs.parts.gridBox.updateBody( 'hostsListGrid', index, 0, function( tdObj ){
				hostInfo['isUse'] = $( tdObj ).children( 'input' ).get(0).checked ;
				if( hostInfo['isUse'] === true )
				{
					++selectNum ;
				}
			} ) ;
		} ) ;
		if( selectNum === 0 )
		{
			//'Error：安装业务至少需要一台主机。'
			showFootStatus( 'danger', _languagePack['error']['web']['confsdb'][0] ) ;
		}
		else
		{
			getHostNodePreview( _businessConfig['DeployMod'] ) ;
			getPredictCapacity() ;
		}
	}
}

//访问一个主机配置
function accessBusinessConf( index )
{
	var businessTemplate = _businessTemplate[index] ;
	//存储当前配置
	if( typeof( _businessConfig['Property'] ) !== 'undefined' )
	{
		sdbjs.fun.free( _businessConfig['Property'] ) ;
	}
	_businessConfig['DeployMod']		= _businessTemplate[index]['DeployMod'] ;
	_businessConfig['BusinessType']	= _businessTemplate[index]['BusinessType'] ;
	_businessConfig['Property']		= [] ;
	//清空表格
	sdbjs.parts.gridBox.emptyBody( 'businessConfGrid' ) ;
	$.each( businessTemplate['Property'], function( proNum, property ){
		//创建参数表格
		sdbjs.parts.gridBox.addBody( 'businessConfGrid', [{ 'text': htmlEncode( property['WebName'] ), 'width': '20%' },
																		  { 'text': function( tdObj ){
																			  var newEle = createHtmlInput(property['Display'],
																													 property['Valid'],
																													 property['Default'],
																													 property['Edit'] ) ;
																			  $( tdObj ).append( newEle ) ;
																		  }, 'width': '40%' },
																		  { 'text': htmlEncode( property['Desc'] ), 'width': '40%' } ]  ) ;
		//存储当前配置
		_businessConfig['Property'].push( { 'Name': property['Name'], 'Value': property['Default'] } ) ;
	} ) ;
}

//切换
function switchPreviewHost()
{
	//'选择主机'
	if( $( '#selectHostBtn' ).text() === _languagePack['confsdb']['leftPanel']['button'][0] )
	{
		sdbjs.fun.setCSS( 'hostsManageDiv', { 'display': 'block' } ) ;
		sdbjs.fun.setCSS( 'businessPreviewDiv', { 'display': 'none' } ) ;
		//'指定参加安装业务的主机'
		sdbjs.parts.panelBox.update( 'PredictBar', htmlEncode( _languagePack['confsdb']['rightPanel'][1]['title'] ) ) ;
		//'业务预览'
		$( '#selectHostBtn' ).text( _languagePack['confsdb']['leftPanel']['button'][2] ) ;
		$( '#previewDataBtn' ).hide() ;
	}
	//'业务预览'
	else if( $( '#selectHostBtn' ).text() === _languagePack['confsdb']['leftPanel']['button'][2] )
	{
		sdbjs.fun.setCSS( 'hostsManageDiv', { 'display': 'none' } ) ;
		sdbjs.fun.setCSS( 'businessPreviewDiv', { 'display': 'block' } ) ;
		//'业务预览'
		sdbjs.parts.panelBox.update( 'PredictBar', htmlEncode( _languagePack['confsdb']['rightPanel'][0]['title'] ) ) ;
		//'选择主机'
		$( '#selectHostBtn' ).text( _languagePack['confsdb']['leftPanel']['button'][0] ) ;
		$( '#previewDataBtn' ).show() ;
		previewInfo() ;
	}
}

//查看主机详细信息
function viewHostInfo( index )
{
	var hostInfo = _hostsInfo[index] ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 0, 1, htmlEncode( hostInfo['HostName'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 1, 1, htmlEncode( hostInfo['IP'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 2, 1, htmlEncode( hostInfo['OS']['Distributor'] + ' ' + hostInfo['OS']['Release'] + ' x' + hostInfo['OS']['Bit'] ) ) ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 3, 1, htmlEncode( sizeConvert( hostInfo['Memory']['Size'] ) ) ) ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 4, 1, htmlEncode( hostInfo['Disk'].length ) ) ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 5, 1, htmlEncode( hostInfo['InstallPath'] ) ) ;
	sdbjs.parts.modalBox.show( 'hostInfo' ) ;
}

//全选主机
function selectAllHost()
{
	$.each( _hostsInfo, function( index ){
		sdbjs.parts.gridBox.updateBody( 'hostsListGrid', index, 0, function( tdObj ){
			$( tdObj ).children( 'input' ).get(0).checked = true ;
		} ) ;
	} ) ;
}

//反选主机
function unSelectAllHost()
{
	$.each( _hostsInfo, function( index ){
		sdbjs.parts.gridBox.updateBody( 'hostsListGrid', index, 0, function( tdObj ){
			var inputObj = $( tdObj ).children( 'input' ).get(0) ;
			inputObj.checked = !inputObj.checked ;
		} ) ;
	} ) ;
}

//返回
function returnPage()
{
	gotoPage( 'index.html' ) ;
}

//下一步
function nextPage()
{
	var rc = true ;
	var selectObj = sdbjs.fun.getNode( 'businessSelect', 'selectBox' ) ;
	selectObj = selectObj['obj'] ;
	var modNum = selectObj.get(0).selectedIndex ;
	var templateProperty = _businessTemplate[modNum]['Property'] ;
	$.each( _businessConfig['Property'], function( index, property ){
		sdbjs.parts.gridBox.updateBody( 'businessConfGrid', index, 1, function( tdObj ){
			var value = $( tdObj ).children( '.form-control' ).val() ;
			var rs = checkInputValue(templateProperty[index]['Display'],
											 templateProperty[index]['Type'],
											 templateProperty[index]['Valid'],
											 templateProperty[index]['WebName'],
											 value ) ;
			if( rs !== '' )
			{
				rc = false ;
				showFootStatus( 'danger', rs ) ;
				return ;
			}
			property['Value'] = value ;
		} ) ;
		if( rc === false )
		{
			//退出循环
			return false ;
		}
	} ) ;
	if( rc === true )
	{
		var selectNum = 0 ;
		$.each( _hostsInfo, function( index, hostInfo ){
			sdbjs.parts.gridBox.updateBody( 'hostsListGrid', index, 0, function( tdObj ){
				hostInfo['isUse'] = $( tdObj ).children( 'input' ).get(0).checked ;
				if( hostInfo['isUse'] === true )
				{
					++selectNum ;
				}
			} ) ;
		} ) ;
		if( selectNum === 0 )
		{
			//'Error：安装业务至少需要一台主机。'
			showFootStatus( 'danger', _languagePack['error']['web']['confsdb'][0] ) ;
		}
		else
		{
			var tempHostInfo = [] ;
			$.each( _hostsInfo, function( index, hostInfo ){
				if( hostInfo['isUse'] === true )
				{
					tempHostInfo.push( { 'HostName': hostInfo['HostName'] } ) ;
				}
			} ) ;
			_businessConfig['HostInfo'] = tempHostInfo ;
			sdbjs.fun.saveData( 'SdbBusinessConfig', JSON.stringify( _businessConfig ) ) ;
			
			if ( _businessConfig['DeployMod'] === 'distribution' )
			{
				sdbjs.fun.delData( 'SdbConfigInfo' ) ;
				gotoPage( 'modsdbd.html' ) ;
			}
			else if ( _businessConfig['DeployMod'] === 'standalone' )
			{
				gotoPage( 'modsdbs.html' ) ;
			}
		}
	}
}

//加载主机列表和信息
function loadHostInfo()
{
	restGetClusterHostsInfo( false, function( jsonArr, textStatus, jqXHR ){
		_hostsInfo = jsonArr ;
		if( _hostsInfo.length === 0 )
		{
			//报错
			showProcessError( '请先安装主机，再安装业务。' ) ;
			return;
		}
		$.each( _hostsInfo, function( index, hostInfo ){
			sdbjs.parts.gridBox.addBody( 'hostsListGrid', [{ 'text': '<input type="checkbox" checked="checked">', 'width': '10%' },
																		  { 'text': htmlEncode( hostInfo['HostName'] ), 'width': '45%' },
																		  { 'text': htmlEncode( hostInfo['IP'] ), 'width': '45%' } ]  ) ;
			sdbjs.parts.gridBox.updateBody( 'hostsListGrid', index, 1, function( tdObj ){
				$( tdObj ).css( 'cursor', 'pointer' ) ;
				sdbjs.fun.addClick( tdObj, 'viewHostInfo(' + index + ')' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'hostsListGrid', index, 2, function( tdObj ){
				$( tdObj ).css( 'cursor', 'pointer' ) ;
				sdbjs.fun.addClick( tdObj, 'viewHostInfo(' + index + ')' ) ;
			} ) ;

		} ) ;
	}, function( json ){
		showProcessError( json['detail'] ) ;
	}, null, _clusterName ) ;
}

//加载业务类型
function loadBusinessTemplate()
{
	var businessType = _businessType ;
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restGetBusinessTemplate( true, function( jsonArr, textStatus, jqXHR ){
		_businessTemplate = jsonArr ;
		$.each( _businessTemplate, function(index,template){
			sdbjs.parts.selectBox.add( 'businessSelect', template['WebName'], htmlEncode( template['DeployMod'] ), ( index === 0 ) ) ;
		}) ;
		accessBusinessConf( 0 ) ;
		previewInfo() ;
	}, function( json ){
		showProcessError( json['detail'] ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
	}, businessType ) ;
}

//初始化业务配置
function initBusinessConfig()
{
	_businessConfig = {} ;
	_businessConfig['ClusterName']	= _clusterName ;
	_businessConfig['BusinessName']	= _businessName ;
}

function createDynamicHtml()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	loadHostInfo() ;
	if( _hostsInfo.length > 0 )
	{
		initBusinessConfig() ;
		loadBusinessTemplate() ;
	}
	sdbjs.parts.loadingBox.hide( 'loading' ) ;
}

function createHtml()
{
	createPublicHtml() ;
	
	/* 分页 */
	sdbjs.parts.tabPageBox.create( 'top2', 'tab' ) ;
	sdbjs.fun.setCSS( 'tab', { 'padding-top': 5 } ) ;
	
	if( _deployModel === 'Deploy' )
	{
		//'扫描主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/zoom.png"> ' + htmlEncode( _languagePack['public']['tabPage'][2] ), false, null ) ;
		//'添加主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][3] ), false, null );
		//'安装主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][4] ), false, null );
	}
	//'配置业务'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cube.png"> ' + htmlEncode( _languagePack['public']['tabPage'][5] ), true, null ) ;
	//'修改业务'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/doc_lines_stright.png"> ' + htmlEncode( _languagePack['public']['tabPage'][6] ), false, null );
	//'安装业务'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][7] ), false, null );
	
	/* 左边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-left', 500, 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-left', { 'float': 'left', 'padding': '10px' } ) ;
	
	/* 左边 状态栏 */
	sdbjs.parts.panelBox.create( 'middle-left', 'businessConfBar', 'auto', 'variable' ) ;
	//'业务参数'
	sdbjs.parts.panelBox.update( 'businessConfBar', htmlEncode( _languagePack['confsdb']['leftPanel']['title'] ), function( panelBody ){
		
		sdbjs.parts.divBox.create( panelBody['name'], 'businessTopDiv', 'auto', 40 ) ;
		sdbjs.parts.divBox.update( 'businessTopDiv', function( divObj ){
			sdbjs.parts.selectBox.create( divObj, 'businessSelect' ) ;
			sdbjs.fun.setCSS( 'businessSelect', { 'width': 240 } ) ;
			sdbjs.fun.getNode( 'businessSelect', 'selectBox' )['obj'].change( function(){
				accessBusinessConf( this.selectedIndex ) ;
			} ) ;
			$( divObj ).append( '&nbsp;' ) ;
			//选择主机
			$( divObj ).append( '<button class="btn btn-mg btn-default" style="vertical-align:top;" onclick="switchPreviewHost()" id="selectHostBtn">' + _languagePack['confsdb']['leftPanel']['button'][0] + '</button>' ) ;
			$( divObj ).append( '&nbsp;' ) ;
			//预览数据
			$( divObj ).append( '<button class="btn btn-mg btn-default" style="vertical-align:top;" onclick="previewInfo()" id="previewDataBtn">' + _languagePack['confsdb']['leftPanel']['button'][1] + '</button>' ) ;
		} ) ;
		
		sdbjs.parts.divBox.create( panelBody['name'], 'businessBottomDiv', 'auto', 'variable' ) ;
		sdbjs.parts.gridBox.create( 'businessBottomDiv', 'businessConfGrid', 'auto', 'variable' ) ;
		//'属性' '值' '说明'
		sdbjs.parts.gridBox.addTitle( 'businessConfGrid', [{ 'text': htmlEncode( _languagePack['confsdb']['leftPanel']['grid'][0] ), 'width': '20%' },
																			{ 'text': htmlEncode( _languagePack['confsdb']['leftPanel']['grid'][1] ), 'width': '40%' },
																			{ 'text': htmlEncode( _languagePack['confsdb']['leftPanel']['grid'][2] ), 'width': '40%' } ]  ) ;
	} ) ;
	
	/* 右边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-right', 'variable', 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-right', { 'float': 'left', 'padding': '10px', 'padding-left': 0 } ) ;
	
	/* 右边 图表 */
	sdbjs.parts.panelBox.create( 'middle-right', 'PredictBar', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'PredictBar', { 'overflow': 'auto', 'position': 'relative' } ) ;
	//'业务预览'
	sdbjs.parts.panelBox.update( 'PredictBar', htmlEncode( _languagePack['confsdb']['rightPanel'][0]['title'] ), function( panelBody ){
		
		sdbjs.parts.divBox.create( panelBody['name'], 'hostsManageDiv', 'auto', 'variable' ) ;
		sdbjs.fun.setCSS( 'hostsManageDiv', { 'display': 'none' } ) ;
		
		sdbjs.parts.divBox.create( 'hostsManageDiv', 'hostsOperationDiv', 'auto', 40 ) ;
		sdbjs.parts.divBox.update( 'hostsOperationDiv', function( divObj ){
			sdbjs.parts.dropDownBox.create( divObj, 'hostOperationDrop' ) ;
			//'选择操作'
			sdbjs.parts.dropDownBox.update( 'hostOperationDrop', htmlEncode( _languagePack['confsdb']['rightPanel'][1]['dropDown']['button'] ), 'btn-mg' ) ;
			//'全选'
			sdbjs.parts.dropDownBox.add( 'hostOperationDrop', htmlEncode( _languagePack['confsdb']['rightPanel'][1]['dropDown']['menu'][0] ), true, 'selectAllHost()' ) ;
			//'反选'
			sdbjs.parts.dropDownBox.add( 'hostOperationDrop', htmlEncode( _languagePack['confsdb']['rightPanel'][1]['dropDown']['menu'][1] ), true, 'unSelectAllHost()' ) ;
		} ) ;
		
		sdbjs.parts.divBox.create( 'hostsManageDiv', 'hostsListDiv', 'auto', 'variable' ) ;
		sdbjs.parts.gridBox.create( 'hostsListDiv', 'hostsListGrid', 'auto', 'variable' ) ;
		sdbjs.parts.gridBox.addTitle( 'hostsListGrid', [{ 'text': '', 'width': '10%' },
																		//'主机名'
																		{ 'text': htmlEncode( _languagePack['confsdb']['rightPanel'][1]['grid'][0] ), 'width': '45%' },
																		//'IP'
																		{ 'text': htmlEncode( _languagePack['confsdb']['rightPanel'][1]['grid'][1] ), 'width': '45%' } ]  ) ;
		
		
		sdbjs.parts.divBox.create( panelBody['name'], 'businessPreviewDiv', 'auto', 'variable' ) ;
		
		sdbjs.parts.tableBox.create( 'businessPreviewDiv', 'otherTable' ) ;
		sdbjs.fun.setCSS( 'otherTable', { 'color': '#666' } ) ;
		sdbjs.parts.tableBox.update ( 'otherTable', 'loosen simple' ) ;
		//'主机数'
		sdbjs.parts.tableBox.addBody( 'otherTable', [ { 'text': '<b>' + htmlEncode( _languagePack['confsdb']['rightPanel'][0]['table'][0] ) + '</b>', 'width': 200 }, { 'text': '' } ] ) ;
		//'总节点数'
		sdbjs.parts.tableBox.addBody( 'otherTable', [ { 'text': '<b>' + htmlEncode( _languagePack['confsdb']['rightPanel'][0]['table'][1] ) + '</b>' }, { 'text': '' } ] ) ;
		//'总磁盘数'
		sdbjs.parts.tableBox.addBody( 'otherTable', [ { 'text': '<b>' + htmlEncode( _languagePack['confsdb']['rightPanel'][0]['table'][2] ) + '</b>' }, { 'text': '' } ] ) ;
		//'节点分布率'
		sdbjs.parts.tableBox.addBody( 'otherTable', [ { 'text': '<b>' + htmlEncode( _languagePack['confsdb']['rightPanel'][0]['table'][3] ) + '</b>' }, { 'text': '' } ] ) ;
		
		sdbjs.parts.tableBox.create( 'businessPreviewDiv', 'PredictTable' ) ;
		sdbjs.fun.setCSS( 'PredictTable', { 'color': '#666', 'margin-top': 15 } ) ;
		sdbjs.parts.tableBox.update ( 'PredictTable', 'loosen simple' ) ;
		//'总容量'
		sdbjs.parts.tableBox.addBody( 'PredictTable', [ { 'text': '<b>' + htmlEncode( _languagePack['confsdb']['rightPanel'][0]['table'][4] ) + '</b>', 'width': 200 }, { 'text': '' } ] ) ;
		//'可用容量'
		sdbjs.parts.tableBox.addBody( 'PredictTable', [ { 'text': '<b>' + htmlEncode( _languagePack['confsdb']['rightPanel'][0]['table'][5] ) + '</b>' }, { 'text': '' } ] ) ;
		//'冗余容量'
		sdbjs.parts.tableBox.addBody( 'PredictTable', [ { 'text': '<b>' + htmlEncode( _languagePack['confsdb']['rightPanel'][0]['table'][6] ) + '</b>' }, { 'text': '' } ] ) ;
		//'冗余度'
		sdbjs.parts.tableBox.addBody( 'PredictTable', [ { 'text': '<b>' + htmlEncode( _languagePack['confsdb']['rightPanel'][0]['table'][7] ) + '</b>' }, { 'text': '' } ] ) ;
		
		sdbjs.parts.divBox.create( 'businessPreviewDiv', 'PicDiv', 450, 300 ) ;
		sdbjs.fun.setCSS( 'PicDiv', { 'margin-top': 20 } ) ;
		sdbjs.parts.divBox.update( 'PicDiv', function( divObj ){
			//饼状图
			_rightPiePic = echarts.init( $( divObj ).get(0) ) ;
		} ) ;
		
	} ) ;
	
	/* ** */
	sdbjs.parts.divBox.create( 'middle', 'middle-clear', 0, 0 ) ;
	sdbjs.fun.setClass( 'middle-clear', 'clear-float' ) ;
	
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
	
	
	/* 查看主机详细信息的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'hostInfo' ) ;
	//'主机信息'
	sdbjs.parts.modalBox.update( 'hostInfo', htmlEncode( _languagePack['confsdb']['hostModal']['title'] ), function( bodyObj ){
		sdbjs.parts.tableBox.create( bodyObj, 'hostInfoTable' ) ;
		sdbjs.parts.tableBox.update ( 'hostInfoTable', 'loosen simple' ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'HostName' ) + '</b>', 'width': 150 }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'IP' ) + '</b>' }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'OS' ) + '</b>' }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'Memory' ) + '</b>' }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'Disk Number' ) + '</b>' }, { 'text': '' } ] ) ;
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': '<b>' + htmlEncode( 'Install Path' ) + '</b>' }, { 'text': '' } ] ) ;
	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'hostInfoClose' ) ;
		sdbjs.parts.buttonBox.update( 'hostInfoClose', function( buttonObj ){
			//'关闭'
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'hostInfo' ) ;
		}, 'primary' ) ;
	} ) ;
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
	_businessName = sdbjs.fun.getData( 'SdbBusinessName' ) ;
	if( _businessName === null )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	_deployModel = sdbjs.fun.getData( 'SdbDeployModel' ) ;
	if( _deployModel === null || ( _deployModel !== 'AddBusiness' && _deployModel !== 'Deploy' ) )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	_businessType = sdbjs.fun.getData( 'SdbBusinessType' ) ;
	if( _businessType === null || _businessType !== 'sequoiadb' )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	return rc ;
}

$(document).ready(function(){
	if( checkReady() )
	{
		sdbjs.fun.saveData( 'SdbStep', 'confsdb' ) ;
		createHtml() ;
		createDynamicHtml() ;
	}
} ) ;