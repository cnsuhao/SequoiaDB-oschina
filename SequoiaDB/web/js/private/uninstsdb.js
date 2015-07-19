//任务时间
var _taskID = null ;

//主机状态
var _nodeStatus = [] ;

//部署模式
var _deployModel = null ;

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
				str = _languagePack['uninstsdb']['rightPanel']['taskStatus'][0] ;
			}
			else if( type === 1 )
			{
				//'正在执行任务'
				str = _languagePack['uninstsdb']['rightPanel']['taskStatus'][1] ;
			}
			else if( type === 2 )
			{
				//'正在回滚任务'
				str = _languagePack['uninstsdb']['rightPanel']['taskStatus'][2] ;
			}
			else if( type === 3 )
			{
				//'正在取消任务'
				str = _languagePack['uninstsdb']['rightPanel']['taskStatus'][3] ;
			}
			else if( type === 4 )
			{
				//'任务完成'
				str = _languagePack['uninstsdb']['rightPanel']['taskStatus'][4] ;
			}
			else
			{
				//'未知状态'
				str = _languagePack['uninstsdb']['rightPanel']['taskStatus'][5] ;
			}
		}
		else
		{
			//'任务失败'
			str = _languagePack['uninstsdb']['rightPanel']['taskStatus'][6] ;
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
				//'正在初始化卸载'
				str = _languagePack['uninstsdb']['rightPanel']['nodeStatus'][0] ;
			}
			else if( type === 1 )
			{
				//'正在卸载'
				str = _languagePack['uninstsdb']['rightPanel']['nodeStatus'][1] ;
			}
			else if( type === 2 )
			{
				//'正在回滚卸载'
				str = _languagePack['uninstsdb']['rightPanel']['nodeStatus'][2] ;
			}
			else if( type === 3 )
			{
				//'正在取消卸载'
				str = _languagePack['uninstsdb']['rightPanel']['nodeStatus'][3] ;
			}
			else if( type === 4 )
			{
				//'卸载完成'
				str = _languagePack['uninstsdb']['rightPanel']['nodeStatus'][4] ;
			}
			else
			{
				//'未知状态'
				str = _languagePack['uninstsdb']['rightPanel']['nodeStatus'][5] ;
			}
		}
		else
		{
			//'卸载失败'
			str = _languagePack['uninstsdb']['rightPanel']['nodeStatus'][6] ;
		}
		return str ;
	}
	var errNode = 0 ;
	var color = 'green' ;
	var sumPro = taskInfo['ResultInfo'].length ;
	var remainTime = parseInt( ( 1 - taskInfo['Progress'] / 100 ) * sumPro ) ;
	var taskStatus = typeToStr( taskInfo['Status'], taskInfo['errno'] ) ;
	if( isFirst === true )
	{
		sdbjs.parts.tableBox.updateBody( 'nodeInfoTable', 0, 1, htmlEncode( taskInfo['TaskID'] ) ) ;
		sdbjs.parts.tableBox.updateBody( 'nodeInfoTable', 1, 1, htmlEncode( taskInfo['Info']['ClusterName'] ) ) ;
		sdbjs.parts.tableBox.updateBody( 'nodeInfoTable', 2, 1, htmlEncode( taskInfo['TypeDesc'] ) ) ;
		sdbjs.parts.tableBox.updateBody( 'nodeInfoTable', 3, 1, htmlEncode( sumPro ) ) ;
		$.each( taskInfo['ResultInfo'], function( index, resultInfo ){
			_nodeStatus.push( false ) ;
			sdbjs.parts.gridBox.addBody( 'nodeListGrid', [{ 'text': function( tdObj ){ 
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
	sdbjs.parts.tableBox.updateBody( 'nodeInfoTable', 4, 1, htmlEncode( taskStatus ) ) ;
	sdbjs.parts.tableBox.updateBody( 'nodeInfoTable', 5, 1, htmlEncode( taskInfo['Progress'] + '%' ) ) ;
	if( remainTime > 0 )
	{
		//'分钟'
		sdbjs.parts.tableBox.updateBody( 'nodeInfoTable', 6, 1, htmlEncode( remainTime + _languagePack['uninstsdb']['leftPanel']['time'] ) ) ;
	}
	else
	{
		sdbjs.parts.tableBox.updateBody( 'nodeInfoTable', 6, 1, '' ) ;
	}
	$.each( taskInfo['ResultInfo'], function( index, resultInfo ){
		if( _nodeStatus[index] === false )
		{
			var statusStr = '' ;
			if( resultInfo['Flow'] !== null && resultInfo['Flow'].length > 0 )
			{
				statusStr = resultInfo['Flow'][ resultInfo['Flow'].length - 1 ] ;
			}
			if( resultInfo['Status'] === 4 )
			{
				_nodeStatus[index] = true ;
				sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 0, '<img src="./images/tick.png">' ) ;
			}
			if( resultInfo['errno'] !== 0 )
			{
				_nodeStatus[index] = true ;
				sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 0, '<img src="./images/delete.png">' ) ;
			}
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 5, htmlEncode( statusStr ) ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 6, htmlEncode( typeToStr2( resultInfo['Status'], resultInfo['errno'] ) ) ) ;
		}
		if( resultInfo['errno'] !== 0 )
		{
			++errNode ;
		}
	} ) ;
	if( errNode > 0 && errNode < sumPro )
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
				_nodeStatus[index] = true ;
				sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 0, '<img src="./images/delete.png">' ) ;
		} ) ;
	}
	sdbjs.parts.progressBox2.update( 'Progress', color, taskInfo['Progress'] ) ;
	if( taskInfo['Status'] === 4 )
	{
		sdbjs.parts.buttonBox.update( 'deployNext', function( buttonObj ){
			$( buttonObj ).show() ;
		}, 'primary' ) ;
		if( taskInfo['errno'] !== 0 )
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


// 下一步
function nextPage()
{
	gotoPage( 'businesslist.html' ) ;
}


function createHtml()
{
	createPublicHtml() ;

	/* 分页 */
	sdbjs.parts.tabPageBox.create( 'top2', 'tab' ) ;
	sdbjs.fun.setCSS( 'tab', { 'padding-top': 5 } ) ;

	if( _deployModel === 'taskRemoveSdb' )
	{
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/home.png"> ' + htmlEncode( _languagePack['public']['tabPage'][1] ), false, 'gotoPage("index.html")' ) ;
	}
	//'卸载业务'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/trash.png"> ' + htmlEncode( _languagePack['public']['tabPage'][8] ), true, null );
	
	
	/* 左边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-left', 460, 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-left', { 'float': 'left', 'padding': '10px' } ) ;
	
	/* 左边 状态栏 */
	sdbjs.parts.panelBox.create( 'middle-left', 'nodeInfoBar', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'nodeInfoBar', { 'overflow': 'auto' } ) ;
	//'任务信息'
	sdbjs.parts.panelBox.update( 'nodeInfoBar', htmlEncode( _languagePack['uninstsdb']['leftPanel']['title'] ), function( panelBody ){
		sdbjs.parts.tableBox.create( panelBody['name'], 'nodeInfoTable' ) ;
		sdbjs.parts.tableBox.update( 'nodeInfoTable', 'loosen border' ) ;
		//'任务ID'
		sdbjs.parts.tableBox.addBody( 'nodeInfoTable', [ { 'text': htmlEncode( _languagePack['uninstsdb']['leftPanel']['taskInfo'][0] ), width: 150 }, { 'text': '' } ] ) ;
		//'所属集群'
		sdbjs.parts.tableBox.addBody( 'nodeInfoTable', [ { 'text': htmlEncode( _languagePack['uninstsdb']['leftPanel']['taskInfo'][1] ) }, { 'text': '' } ] ) ;
		//'任务类型'
		sdbjs.parts.tableBox.addBody( 'nodeInfoTable', [ { 'text': htmlEncode( _languagePack['uninstsdb']['leftPanel']['taskInfo'][2] ) }, { 'text': '' } ] ) ;
		//'项目数'
		sdbjs.parts.tableBox.addBody( 'nodeInfoTable', [ { 'text': htmlEncode( _languagePack['uninstsdb']['leftPanel']['taskInfo'][3] ) }, { 'text': '' } ] ) ;
		//'任务状态'
		sdbjs.parts.tableBox.addBody( 'nodeInfoTable', [ { 'text': htmlEncode( _languagePack['uninstsdb']['leftPanel']['taskInfo'][4] ) }, { 'text': '' } ] ) ;
		//'卸载进度'
		sdbjs.parts.tableBox.addBody( 'nodeInfoTable', [ { 'text': htmlEncode( _languagePack['uninstsdb']['leftPanel']['taskInfo'][5] ) }, { 'text': '' } ] ) ;
		//'预计剩余时间'
		sdbjs.parts.tableBox.addBody( 'nodeInfoTable', [ { 'text': htmlEncode( _languagePack['uninstsdb']['leftPanel']['taskInfo'][6] ) }, { 'text': '' } ] ) ;
		
		//'日志'
		sdbjs.parts.tableBox.addBody( 'nodeInfoTable', [ { 'text': htmlEncode( _languagePack['uninstsdb']['leftPanel']['taskInfo'][7] ) }, { 'text': '<button class="btn btn-default btn-lg" onclick="openLogModal()">' + htmlEncode( _languagePack['uninstsdb']['leftPanel']['logButton'] ) + '</button>' } ] ) ;
	} ) ;

	/* 右边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-right', 'variable', 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-right', { 'float': 'left', 'padding': '10px', 'padding-left': 0 } ) ;
	
	/* 右边 主机列表 */
	sdbjs.parts.panelBox.create( 'middle-right', 'installInfoBar', 'auto', 'variable' ) ;
	//'卸载进度'
	sdbjs.parts.panelBox.update( 'installInfoBar', htmlEncode( _languagePack['uninstsdb']['rightPanel']['title'] ), function( panelBody ){
		sdbjs.parts.progressBox2.create( panelBody['name'], 'Progress' ) ;
		sdbjs.parts.divBox.create( panelBody['name'], 'nodeListDiv', 'auto', 'variable' ) ;
		sdbjs.fun.setCSS( 'nodeListDiv', { 'padding-top': 10 } ) ;
		sdbjs.parts.gridBox.create( 'nodeListDiv', 'nodeListGrid', 'auto', 'variable' ) ;
		//'主机名' '端口' '卸载状态' '卸载进度' '日志'
		sdbjs.parts.gridBox.addTitle( 'nodeListGrid', [{ 'text': '', 'width': '8%' },
																	  { 'text': htmlEncode( _languagePack['uninstsdb']['rightPanel']['nodeGrid'][0] ), 'width': '20%' },
																	  { 'text': htmlEncode( _languagePack['uninstsdb']['rightPanel']['nodeGrid'][1] ), 'width': '10%' },
																	  { 'text': htmlEncode( _languagePack['uninstsdb']['rightPanel']['nodeGrid'][2] ), 'width': '10%' },
																	  { 'text': htmlEncode( _languagePack['uninstsdb']['rightPanel']['nodeGrid'][3] ), 'width': '15%' },
																	  { 'text': htmlEncode( _languagePack['uninstsdb']['rightPanel']['nodeGrid'][4] ), 'width': '25%' },
																	  { 'text': htmlEncode( _languagePack['uninstsdb']['rightPanel']['nodeGrid'][5] ), 'width': '12%' } ] ) ;
		
	} ) ;
	
	/* ** */
	sdbjs.parts.divBox.create( 'middle', 'middle-clear', 0, 0 ) ;
	sdbjs.fun.setClass( 'middle-clear', 'clear-float' ) ;

	//下一步
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
	sdbjs.parts.modalBox.update( 'logModal', htmlEncode( _languagePack['uninstsdb']['logModal']['title'] ), function( bodyObj ){
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
	return rc ;
}

$(document).ready(function(){
	if( checkReady() === true )
	{
		sdbjs.fun.saveData( 'SdbStep', 'uninstsdb' ) ;
		createHtml() ;
		queryTaskInfo( true ) ;
	}
} ) ;