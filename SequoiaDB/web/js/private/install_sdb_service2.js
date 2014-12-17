//--------------------------------- 页面全局变量 -----------------------------------//

//任务信息
var progressInfo = null ;
//获取数据定时器
var progressTimer = 1 ;
//是否已经绘图
var isCreateGrid = false ;
//任务id
var taskID = 0 ;
//日志记录
var diag = [] ;
//上次日志记录
var oldDiag = [] ;
//判断是否完成
var isFinish = [] ;

//--------------------------------- 页面函数 -----------------------------------//

/*
 * 打开日志信息
 */
function openDiagModal( num )
{
	sdbjs.fun.openModal( 'addBusinessDiag' ) ;
	$( '#addBusinessDiag > .modal-body > div' ).html( diag[num] ) ;
}

/*
 * 创建业务安装进度
 */
function createProgress()
{
	var progressLen = progressInfo['Progress'].length ;
	$( '#installPanel > .panel-body > div:eq(1) > span:eq(0)' ).text( progressLen ) ;
	for( var i = 0; i < progressLen; ++i )
	{
		isFinish[i] = false ;
		oldDiag[i] = progressInfo['Progress'][i]['Desc'] ;
		if( progressInfo['Progress'][i]['Desc'] != '' )
		{
			diag[i] = sdbjs.fun.getDateFormat('yyyy-MM-dd.hh.mm.ss') + '<br>' + sdbjs.fun.htmlEncode( progressInfo['Progress'][i]['Desc'] ) + '<br><br>' ;
		}
		else
		{
			diag[i] = '' ;
		}
		sdbjs.parts.gridBox.add( 'progressListGrid', { 'cell': [ { 'text': progressInfo['Progress'][i]['Name'] }, { 'text': progressInfo['Progress'][i]['TotalCount'] }, { 'text': '<div class="progress2"><span class="reading">0%</span><span class="bar green"></span></div>' }, { 'text': progressInfo['Progress'][i]['Desc'] }, { 'text': '<a href="#" onclick="openDiagModal(' + i + ')">日志</a>' } ] } ) ;
	}
	isCreateGrid = true ;
	sdbjs.fun.gridRevise( 'progressListGrid' ) ;
}

/*
 * 重试一次添加业务
 */
function tryAgain()
{
	gotoPage( 'modify_business_sdb.html' ) ;
}

/*
 * 更新业务安装进度
 */
function updateProgress()
{
	var progressLen = progressInfo['Progress'].length ;
	var success = 0 ;
	var sumPercent = 0 ;
	var percent = 0 ;
	for( var i = 0; i < progressLen; ++i )
	{
		percent = parseInt( progressInfo['Progress'][i]['InstalledCount'] * 100 / progressInfo['Progress'][i]['TotalCount'] ) ;
		sumPercent += percent ;
		$( '#progressListGrid > .grid-body > .grid-tr:eq(' + i + ') > .grid-td:eq(2) > div > span:eq(0)' ).text( percent + '%' ) ;
		$( '#progressListGrid > .grid-body > .grid-tr:eq(' + i + ') > .grid-td:eq(2) > div > span:eq(1)' ).css( 'width', percent + '%' ) ;
		if( progressInfo['Progress'][i]['InstalledCount'] == progressInfo['Progress'][i]['TotalCount'] )
		{
			++success ;
			if( isFinish[i] == false )
			{
				isFinish[i] = true ;
				$( '#progressListGrid > .grid-body > .grid-tr:eq(' + i + ') > .grid-td:eq(3)' ).html( '<img src="images/tick.png">' ) ;
			}
		}
		else
		{
			$( '#progressListGrid > .grid-body > .grid-tr:eq(' + i + ') > .grid-td:eq(3)' ).text( progressInfo['Progress'][i]['Desc'] ) ;
		}
		if( oldDiag[i] != progressInfo['Progress'][i]['Desc'] && progressInfo['Progress'][i]['Desc'] != '' )
		{
			oldDiag[i] = progressInfo['Progress'][i]['Desc'] ;
			diag[i] += sdbjs.fun.getDateFormat('yyyy-MM-dd.hh.mm.ss') + '<br>' + sdbjs.fun.htmlEncode( progressInfo['Progress'][i]['Desc'] ) + '<br><br>' ;
		}
	}
	$( '#installPanel > .panel-body > div:eq(1) > span:eq(1)' ).text( success ) ;
	sumPercent = parseInt( sumPercent / progressLen ) ;
	$( '#installPanel > .panel-body > div:eq(0) > div' ).css( 'width', sumPercent + '%' ) ;
	
	if( progressInfo['Status'] == 'install' )
	{
		$( '#installPanel > .panel-header > span:eq(1)' ).text( '正在安装' ) ;
	}
	else if( progressInfo['Status'] == 'rollback' )
	{
		$( '#installPanel > .panel-header > span:eq(1)' ).text( '正在回滚' ) ;
	}
	
	if( progressInfo['IsFinish'] == true || progressInfo['IsEnable'] == false )
	{
		$( '.modal' ).each(function(index, element) {
         $( this ).hide() ;
      });
		var comeback = ( $.cookie( 'SdbComeback' ) == undefined ? 'index.html' : $.cookie( 'SdbComeback' ) ) ;
		if( progressInfo['IsEnable'] == false )
		{
			$( '#installPanel > .panel-header > span:eq(1)' ).text( '安装错误，回滚失败' ) ;
			sdbjs.fun.openModal( 'installFinishModal' ) ;
			$( '#installFinishModal > .modal-title' ).text( '安装结果' ) ;
			$( '#installFinishModal > .modal-body > div' ).text( '业务安装错误，回滚失败，错误原因：' + progressInfo['ErrMsg'] + '。' ) ;
			sdbjs.fun.moveModal( 'installFinishModal' ) ;
			$( '#installPanel > .panel-body > div:eq(0) > div' ).css( 'background-color', '#ae3027' ) ;
		}
		else
		{
			if( progressInfo['Status'] == 'rollback' )
			{
				$( '#installPanel > .panel-header > span:eq(1)' ).text( '安装错误，回滚成功' ) ;
				sdbjs.fun.openModal( 'installFinishModal' ) ;
				$( '#installFinishModal > .modal-title' ).text( '安装结果' ) ;
				$( '#installFinishModal > .modal-body > div' ).html( '业务安装错误，回滚成功，错误原因：' + sdbjs.fun.htmlEncode( progressInfo['ErrMsg'] ) + '。需要重新安装业务请<a href="#" onclick="tryAgain()">点击</a>。' ) ;
				sdbjs.fun.moveModal( 'installFinishModal' ) ;
				$( '#installPanel > .panel-body > div:eq(0) > div' ).css( 'background-color', '#ae3027' ) ;
			}
			else
			{
				$( '#installPanel > .panel-header > span:eq(1)' ).text( '安装完成' ) ;
				sdbjs.fun.openModal( 'installFinishModal' ) ;
				$( '#installFinishModal > .modal-title' ).text( '安装结果' ) ;
				$( '#installFinishModal > .modal-body > div' ).text( '业务安装完成， 系统将会在10秒后返回。或点击下方按钮马上返回。' ) ;
				sdbjs.fun.moveModal( 'installFinishModal' ) ;
				setTimeout( 'gotoPage("' + comeback + '")', 10000 ) ;
			}
		}
		progressTimer = null ;
		$( '#installPanel > .panel-body > div:eq(0) > div' ).css( 'width', '100%' ) ;
		$( '#installFinishModal > .modal-foot > button' ).get(0).onclick = Function( 'gotoPage("' + comeback + '")' ) ; ;
	}
	sdbjs.fun.gridRevise( 'progressListGrid' ) ;
}

function getTask()
{
	var order = { 'cmd': 'query progress', 'TaskID': taskID } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		progressInfo = jsonArr[1] ;
		if( progressInfo['Progress'].length > 0 && isCreateGrid == false )
		{
			createProgress() ;
		}
		else
		{
			updateProgress() ;
		}
		progressTimer = 1 ;
	}, function( jsonArr ){
		progressTimer = null ;
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	}, function(){
		if( progressTimer != null )
		{
			setTimeout( getTask, 1000 ) ;
		}
	} ) ;
}

$(document).ready(function()
{
	if( $.cookie( 'SdbUser' ) == undefined || $.cookie( 'SdbPasswd' ) == undefined || $.cookie( 'SdbTaskID' ) == undefined )
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
		var SdbGuideOrder = $.cookie( 'SdbGuideOrder' ) ;
		if( SdbGuideOrder != 'Deployment' && SdbGuideOrder != 'AddBusiness' )
		{
			gotoPage( "index.html" ) ;
		}
		var guideLen = __processPic[SdbGuideOrder].length ;
		for( var i = 0; i < guideLen; ++i )
		{
			if( ( SdbGuideOrder == 'Deployment' && i == 4 ) || ( SdbGuideOrder == 'AddBusiness' && i == 2 ) )
			{
				$( '#tab_box' ).append( '<li class="active">' + __processPic[SdbGuideOrder][i] + '</li>' ) ;
			}
			else
			{
				$( '#tab_box' ).append( '<li>' + __processPic[SdbGuideOrder][i] + '</li>' ) ;
			}
		}
		taskID = $.cookie( 'SdbTaskID' ) ;
		sdbjs.parts.loadingBox.create( 'loading' ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#bodyTran' ).children( ':first-child' ).children( ':eq(0)' ), 'style': { 'width': 'parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#bodyTran' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		
		sdbjs.fun.autoCorrect( { 'obj': $( '#addBusinessDiag > .modal-body > div' ), 'style': { 'height': 'parseInt(sdbjs.public.height * 3 / 5)' } } ) ;
		
		sdbjs.fun.autoCorrect( { 'obj': $( '#installPanel' ), 'style': { 'height': 'sdbjs.public.height - 155', 'width': 'parseInt(sdbjs.public.width*0.8)' } } ) ;
		
		sdbjs.parts.gridBox.create( 'progressListGrid', {}, [ [ '项目', '节点数', '进度', '状态', '日志' ] ], [ 15, 10, 20, 20, 10 ] ).appendTo( $( '#installPanel > .panel-body' ) ) ;
		
		sdbjs.fun.gridRevise( 'progressListGrid' ) ;
		
		sdbjs.fun.autoCorrect( { 'obj': $( '#progressListGrid' ), 'style': { 'maxHeight': 'sdbjs.public.height - 255' } } ) ;
		
		$( '#installPanel > .panel-header > span:eq(0)' ).text( sdbjs.fun.limitString( $.cookie( 'SdbBusinessName' ), 20 ) ) ;
		
		$( '#installPanel > .panel-header > span:eq(1)' ).text( '正在初始化' ) ;
		
		getTask() ;
		setUser() ;
	}
	
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ), 'style': { 'height': 'sdbjs.public.height - 131' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - 428' } } ) ;
	
	sdbjs.fun.endOfCreate() ;
});