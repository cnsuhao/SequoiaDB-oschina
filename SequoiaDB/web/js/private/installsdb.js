//部署模式
var _deployModel = null ;

//部署类型
var _deployType = null ;

//任务ID
var _taskID = null ;

//主机状态
var _hostStatus = [] ;

//判断日志弹出是否放大
var _isLogModalZooomin = false ;

//打开日志弹窗
function openLogModal()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restGetLog( true, function( text ){
		sdbjs.parts.wellBox.update( 'logWell', htmlEncode( text ) ) ;	
		sdbjs.fun.setCSS( 'logModal', { 'width': 760 } ) ;
		sdbjs.fun.setCSS( 'logWell', { 'max-height': 350 } ) ;
		sdbjs.parts.modalBox.show( 'logModal' ) ;
		_isLogModalZooomin = false ;
	}, function(){
		showProcessError( _languagePack['error']['system']['networkErr'] ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
	}, _taskID ) ;
}

//放大日志弹窗
function zoominLogModal()
{
	_isLogModalZooomin = true ;
	var width = sdbjs.fun.getWindowWidth() ;
	var height = sdbjs.fun.getWindowHeight() ;
	sdbjs.fun.setCSS( 'logModal', { 'width': width - 100 } ) ;
	sdbjs.fun.setCSS( 'logWell', { 'max-height': height - 180 } ) ;
	sdbjs.parts.modalBox.redraw( 'logModal', width, height ) ;
}

$( window ).resize(function(){
	if( !sdbjs.parts.modalBox.isHidden( 'logModal' ) && _isLogModalZooomin === true )
	{
		var width = sdbjs.fun.getWindowWidth() ;
		var height = sdbjs.fun.getWindowHeight() ;
		sdbjs.fun.setCSS( 'logModal', { 'width': width - 100 } ) ;
		sdbjs.fun.setCSS( 'logWell', { 'max-height': height - 180 } ) ;
		sdbjs.parts.modalBox.redraw( 'logModal' ) ;
	}
} ) ;

//更新任务信息
function updateTaskInfo( taskInfo, isFirst )
{
	function typeToStr( type, errno )
	{
		var str = null ;
		if( errno === 0 )
		{
			if( type === 0 )
			{
				//'正在初始化任务'
				str = _languagePack['installsdb']['rightPanel']['taskStatus'][0] ;
			}
			else if( type === 1 )
			{
				//'正在执行任务'
				str = _languagePack['installsdb']['rightPanel']['taskStatus'][1] ;
			}
			else if( type === 2 )
			{
				//'正在回滚任务'
				str = _languagePack['installsdb']['rightPanel']['taskStatus'][2] ;
			}
			else if( type === 3 )
			{
				//'正在取消任务'
				str = _languagePack['installsdb']['rightPanel']['taskStatus'][3] ;
			}
			else if( type === 4 )
			{
				//'任务完成'
				str = _languagePack['installsdb']['rightPanel']['taskStatus'][4] ;
			}
			else
			{
				//'未知状态'
				str = _languagePack['installsdb']['rightPanel']['taskStatus'][5] ;
			}
		}
		else
		{
			//'任务失败'
			str = _languagePack['installsdb']['rightPanel']['taskStatus'][6] ;
		}
		return str ;
	}
	function typeToStr2( type, errno )
	{
		var str = null ;
		if( errno === 0 )
		{
			if( type === 0 )
			{
				//'正在初始化安装'
				str = _languagePack['installsdb']['rightPanel']['hostStatus'][0] ;
			}
			else if( type === 1 )
			{
				//'正在安装'
				str = _languagePack['installsdb']['rightPanel']['hostStatus'][1] ;
			}
			else if( type === 2 )
			{
				//'正在回滚安装'
				str = _languagePack['installsdb']['rightPanel']['hostStatus'][2] ;
			}
			else if( type === 3 )
			{
				//'正在取消安装'
				str = _languagePack['installsdb']['rightPanel']['hostStatus'][3] ;
			}
			else if( type === 4 )
			{
				//'安装完成'
				str = _languagePack['installsdb']['rightPanel']['hostStatus'][4] ;
			}
			else
			{
				//'未知状态'
				str = _languagePack['installsdb']['rightPanel']['hostStatus'][5] ;
			}
		}
		else
		{
			//'安装失败'
			str = _languagePack['installsdb']['rightPanel']['hostStatus'][6] ;
		}
		return str ;
	}
	var errHost = 0 ;
	var color = 'green' ;
	var sumPro = taskInfo['ResultInfo'].length ;
	var remainTime = parseInt( 3 * ( 1 - taskInfo['Progress'] / 100 ) * sumPro ) ;
	var taskStatus = typeToStr( taskInfo['Status'], taskInfo['errno'] ) ;
	if( isFirst === true )
	{
		sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 0, 1, htmlEncode( taskInfo['TaskID'] ) ) ;
		sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 1, 1, htmlEncode( taskInfo['Info']['ClusterName'] ) ) ;
		sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 2, 1, htmlEncode( taskInfo['TypeDesc'] ) ) ;
		sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 3, 1, htmlEncode( sumPro ) ) ;
		$.each( taskInfo['ResultInfo'], function( index, resultInfo ){
			_hostStatus.push( false ) ;
			sdbjs.parts.gridBox.addBody( 'hostListGrid', [{ 'text': function( tdObj ){ 
				$( tdObj ).attr( 'align', 'center').append( '<img width="22" height="22" src="./images/loading.gif">' ) ;
			}, 'width': '8%' },
																		 { 'text': htmlEncode( resultInfo['HostName'] ), 'width': '20%' },
																		 { 'text': htmlEncode( resultInfo['svcname'] ), 'width': '10%' },
																		 { 'text': htmlEncode( resultInfo['role'] ), 'width': '10%' },
																		 { 'text': htmlEncode( resultInfo['datagroupname'] ), 'width': '15%' },
																		 { 'text': '', 'width': '25%' },
																		 { 'text': '', 'width': '12%' } ] ) ;
		} ) ;
	}
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 4, 1, htmlEncode( taskStatus ) ) ;
	sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 5, 1, htmlEncode( taskInfo['Progress'] + '%' ) ) ;
	if( remainTime > 0 )
	{
		//'分钟'
		sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 6, 1, htmlEncode( remainTime + _languagePack['installsdb']['leftPanel']['time'] ) ) ;
	}
	else
	{
		sdbjs.parts.tableBox.updateBody( 'hostInfoTable', 6, 1, '' ) ;
	}
	$.each( taskInfo['ResultInfo'], function( index, resultInfo ){
		if( _hostStatus[index] === true && resultInfo['errno'] !== 0 )
		{
			_hostStatus[index] = false ;
		}
		//if( _hostStatus[index] === false )
		{
			var statusStr = '' ;
			if( resultInfo['Flow'] !== null && resultInfo['Flow'].length > 0 )
			{
				statusStr = resultInfo['Flow'][ resultInfo['Flow'].length - 1 ] ;
			}
			if( resultInfo['Status'] === 4 )
			{
				_hostStatus[index] = true ;
				sdbjs.parts.gridBox.updateBody( 'hostListGrid', index, 0, '<img src="./images/tick.png">' ) ;
			}
			if( resultInfo['errno'] !== 0 )
			{
				_hostStatus[index] = true ;
				sdbjs.parts.gridBox.updateBody( 'hostListGrid', index, 0, '<img src="./images/delete.png">' ) ;
			}
			sdbjs.parts.gridBox.updateBody( 'hostListGrid', index, 5, htmlEncode( statusStr ) ) ;
			sdbjs.parts.gridBox.updateBody( 'hostListGrid', index, 6, htmlEncode( typeToStr2( resultInfo['Status'], resultInfo['errno'] ) ) ) ;
		}
		if( resultInfo['errno'] !== 0 )
		{
			++errHost ;
		}
	} ) ;
	if( errHost > 0 && errHost < sumPro )
	{
		color = 'orange' ;
	}
	if( taskInfo['errno'] !== 0 )
	{
		color = 'red' ;
	}
	if( taskInfo['errno'] !== 0 )
	{
		color = 'red' ;
		$.each( taskInfo['ResultInfo'], function( index, resultInfo ){
				_hostStatus[index] = true ;
				sdbjs.parts.gridBox.updateBody( 'hostListGrid', index, 0, '<img src="./images/delete.png">' ) ;
		} ) ;
	}
	sdbjs.parts.progressBox2.update( 'Progress', color, taskInfo['Progress'] ) ;
	if( taskInfo['Status'] === 4 )
	{
		if( _deployModel !== 'taskAddSdb' && taskInfo['errno'] !== 0 )
		{
			sdbjs.parts.buttonBox.update( 'deployReturn', function( buttonObj ){
				$( buttonObj ).show() ;
			}, 'primary' ) ;
		}
		sdbjs.parts.buttonBox.update( 'deployNext', function( buttonObj ){
			$( buttonObj ).show() ;
		}, 'primary' ) ;
		if( taskInfo['errno'] === 0 )
		{
			sdbjs.fun.delData( 'SdbHostList' ) ;
			sdbjs.fun.delData( 'SdbClusterName' ) ;
			sdbjs.fun.delData( 'SdbConfigInfo' ) ;
		}
		else
		{
			showProcessError( taskInfo['detail'] ) ;
		}
	}
	return ( taskInfo['Status'] === 4 ) ;
}

//查询任务信息
function queryTaskInfo( isFirst )
{
	restQueryTask( false, function( jsonArr, textStatus, jqXHR ){
		if ( !updateTaskInfo( jsonArr[0], isFirst ) )
		{
			setTimeout( function(){ queryTaskInfo() ; }, 1000 ) ;
		}
	}, function( json ){
		showProcessError( json['detail'] ) ;
	}, null, parseInt( _taskID ) ) ;
}

//返回
function returnPage()
{
	if( _deployType === 'standalone' )
	{
		gotoPage( 'modsdbs.html' ) ;
	}
	else if( _deployType === 'distribution' )
	{
		gotoPage( 'modsdbd.html' ) ;
	}
	else
	{
		gotoPage( 'index.html' ) ;
	}
}


// 下一步
function nextPage()
{
	gotoPage( 'index.html' ) ;
}


function createHtml()
{
	createPublicHtml() ;

	/* 分页 */
	sdbjs.parts.tabPageBox.create( 'top2', 'tab' ) ;
	sdbjs.fun.setCSS( 'tab', { 'padding-top': 5 } ) ;

	if ( _deployModel !== 'taskAddSdb' )
	{
		if( _deployModel === 'Deploy' )
		{
			//'扫描主机'
			sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/zoom.png"> ' + htmlEncode( _languagePack['public']['tabPage'][2] ), false, null ) ;
			//'添加主机'
			sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][3] ), false, null ) ;
			//'安装主机'
			sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][4] ), false, null );
		}

		//'配置业务'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cube.png"> ' + htmlEncode( _languagePack['public']['tabPage'][5] ), false, null ) ;
		//'修改业务'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/doc_lines_stright.png"> ' + htmlEncode( _languagePack['public']['tabPage'][6] ), false, null );
	}
	else
	{
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/home.png"> ' + htmlEncode( _languagePack['public']['tabPage'][1] ), false, 'gotoPage("index.html")' ) ;
	}
	//'安装业务'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][7] ), true, null );
	
	
	/* 左边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-left', 460, 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-left', { 'float': 'left', 'padding': '10px' } ) ;
	
	/* 左边 状态栏 */
	sdbjs.parts.panelBox.create( 'middle-left', 'hostInfoBar', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'hostInfoBar', { 'overflow': 'auto' } ) ;
	//'任务信息'
	sdbjs.parts.panelBox.update( 'hostInfoBar', htmlEncode( _languagePack['installsdb']['leftPanel']['title'] ), function( panelBody ){
		sdbjs.parts.tableBox.create( panelBody['name'], 'hostInfoTable' ) ;
		sdbjs.parts.tableBox.update( 'hostInfoTable', 'loosen border' ) ;
		//'任务ID'
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': htmlEncode( _languagePack['installsdb']['leftPanel']['taskInfo'][0] ), width: 150 }, { 'text': '' } ] ) ;
		//'所属集群'
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': htmlEncode( _languagePack['installsdb']['leftPanel']['taskInfo'][1] ) }, { 'text': '' } ] ) ;
		//'任务类型'
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': htmlEncode( _languagePack['installsdb']['leftPanel']['taskInfo'][2] ) }, { 'text': '' } ] ) ;
		//'项目数'
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': htmlEncode( _languagePack['installsdb']['leftPanel']['taskInfo'][3] ) }, { 'text': '' } ] ) ;
		//'任务状态'
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': htmlEncode( _languagePack['installsdb']['leftPanel']['taskInfo'][4] ) }, { 'text': '' } ] ) ;
		//'安装进度'
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': htmlEncode( _languagePack['installsdb']['leftPanel']['taskInfo'][5] ) }, { 'text': '' } ] ) ;
		//'预计剩余时间'
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': htmlEncode( _languagePack['installsdb']['leftPanel']['taskInfo'][6] ) }, { 'text': '' } ] ) ;
		
		//'日志'
		sdbjs.parts.tableBox.addBody( 'hostInfoTable', [ { 'text': htmlEncode( _languagePack['installsdb']['leftPanel']['taskInfo'][7] ) }, { 'text': '<button class="btn btn-default btn-lg" onclick="openLogModal()">' + htmlEncode( _languagePack['installsdb']['leftPanel']['logButton'] ) + '</button>' } ] ) ;
	} ) ;

	/* 右边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-right', 'variable', 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-right', { 'float': 'left', 'padding': '10px', 'padding-left': 0 } ) ;
	
	/* 右边 主机列表 */
	sdbjs.parts.panelBox.create( 'middle-right', 'installInfoBar', 'auto', 'variable' ) ;
	//'安装进度'
	sdbjs.parts.panelBox.update( 'installInfoBar', htmlEncode( _languagePack['installsdb']['rightPanel']['title'] ), function( panelBody ){
		sdbjs.parts.progressBox2.create( panelBody['name'], 'Progress' ) ;
		sdbjs.parts.divBox.create( panelBody['name'], 'hostListDiv', 'auto', 'variable' ) ;
		sdbjs.fun.setCSS( 'hostListDiv', { 'padding-top': 10 } ) ;
		sdbjs.parts.gridBox.create( 'hostListDiv', 'hostListGrid', 'auto', 'variable' ) ;
		//'主机名' '端口' '安装状态' '安装进度'
		sdbjs.parts.gridBox.addTitle( 'hostListGrid', [{ 'text': '', 'width': '8%' },
																	  { 'text': htmlEncode( _languagePack['installsdb']['rightPanel']['hostGrid'][0] ), 'width': '20%' },
																	  { 'text': htmlEncode( _languagePack['installsdb']['rightPanel']['hostGrid'][1] ), 'width': '10%' },
																	  { 'text': htmlEncode( _languagePack['installsdb']['rightPanel']['hostGrid'][2] ), 'width': '10%' },
																	  { 'text': htmlEncode( _languagePack['installsdb']['rightPanel']['hostGrid'][3] ), 'width': '15%' },
																	  { 'text': htmlEncode( _languagePack['installsdb']['rightPanel']['hostGrid'][4] ), 'width': '25%' },
																	  { 'text': htmlEncode( _languagePack['installsdb']['rightPanel']['hostGrid'][5] ), 'width': '12%' } ] ) ;
		
	} ) ;
	
	/* ** */
	sdbjs.parts.divBox.create( 'middle', 'middle-clear', 0, 0 ) ;
	sdbjs.fun.setClass( 'middle-clear', 'clear-float' ) ;

	if( _deployModel !== 'taskAddSdb' )
	{
		//返回 下一步
		sdbjs.parts.buttonBox.create( 'operate', 'deployReturn' ) ;
		sdbjs.parts.buttonBox.update( 'deployReturn', function( buttonObj ){
			//'返回'
			$( buttonObj ).text( _languagePack['public']['button']['return'] ).hide() ;
			sdbjs.fun.addClick( buttonObj, 'returnPage()' ) ;
		}, 'primary' ) ;
		var operateNode = sdbjs.fun.getNode( 'operate', 'divBox' ) ;
		$( operateNode['obj'] ).append( '&nbsp;' ) ;
	}
	sdbjs.parts.buttonBox.create( 'operate', 'deployNext' ) ;
	sdbjs.parts.buttonBox.update( 'deployNext', function( buttonObj ){
		//'完成'
		$( buttonObj ).text( _languagePack['public']['button']['complete'] ) ;
		$( buttonObj ).hide() ;
		sdbjs.fun.addClick( buttonObj, 'nextPage()' ) ;
	}, 'primary' ) ;

	//日志的弹窗
	sdbjs.parts.modalBox.create( $( document.body ), 'logModal' ) ;
	//'日志'
	sdbjs.parts.modalBox.update( 'logModal', htmlEncode( _languagePack['installsdb']['logModal']['title'] ), function( bodyObj ){
		sdbjs.parts.wellBox.create( bodyObj, 'logWell' ) ;
		sdbjs.fun.setCSS( 'logWell', { 'max-height': 350, 'overflow': 'auto', 'font-family': 'Courier', 'word-break': 'break-all' } ) ;
	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'logModalZoomin' ) ;
		sdbjs.parts.buttonBox.update( 'logModalZoomin', function( buttonObj ){
			//'放大'
			$( buttonObj ).text( _languagePack['public']['button']['zoomin'] ) ;
		}, 'primary' ) ;
		sdbjs.fun.addClick( 'logModalZoomin', 'zoominLogModal()' )
		$( footObj ).append( '&nbsp;' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'logModalClose' ) ;
		sdbjs.parts.buttonBox.update( 'logModalClose', function( buttonObj ){
			//'关闭'
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'logModal' ) ;
		}, 'primary' ) ;
	} ) ;
}

function checkReady()
{
	var rc = true ;
	_taskID = sdbjs.fun.getData( 'SdbTaskID' ) ;
	if( _taskID === null )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	_deployModel = sdbjs.fun.getData( 'SdbDeployModel' ) ;
	if( _deployModel === null || ( _deployModel !== 'AddBusiness' && _deployModel !== 'Deploy' && _deployModel !== 'taskAddSdb' ) )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	if ( _deployModel !== 'taskAddSdb' )
	{
		businessConfig = sdbjs.fun.getData( 'SdbBusinessConfig' ) ;
		if( businessConfig === null )
		{
			rc = false ;
			gotoPage( 'index.html' ) ;
		}
		else
		{
			businessConfig = JSON.parse( businessConfig ) ;
			_deployType = businessConfig['DeployMod'] ;
		}
	}
	return rc ;
}

$(document).ready(function(){
	if( checkReady() === true )
	{
		sdbjs.fun.saveData( 'SdbStep', 'installsdb' ) ;
		createHtml() ;
		queryTaskInfo( true ) ;
	}
} ) ;