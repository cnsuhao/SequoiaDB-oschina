//--------------------------------- 页面全局变量 -----------------------------------//

//业务模板
var _businessTemplate = null ;
//业务配置
var _businessConfig = null ;

//--------------------------------- 页面函数 -----------------------------------//

/*
 * 选择主机
 */
function switchHostGrid( line )
{
	if( $( '#choiceHostGrid > .grid-body > .grid-tr' ).eq( line ).data( 'isChoice' ) )
	{
		$( '#choiceHostGrid > .grid-body > .grid-tr' ).eq( line ).data( 'isChoice', false ).children().each(function(index, element) {
			if ( line % 2 != 0 )
			{
				$( this ).css( { 'backgroundColor': '#F5F5F5', 'color': '#000' } ) ;
			}
			else
			{
				$( this ).css( { 'backgroundColor': '#FFF', 'color': '#000' } ) ;
			}
		});
		return false ;
	}
	else
	{
		$( '#choiceHostGrid > .grid-body > .grid-tr' ).eq( line ).data( 'isChoice', true ).children().each(function(index, element) {
			$( this ).css( { 'backgroundColor': '#2E76CA', 'color': '#FFF' } ) ;
		});
		return true ;
	}
}

/*
 * 全选主机
 */
function checkAllHost()
{
	var sum_host_num = 0 ;
	$( '#choiceHostGrid > .grid-body > .grid-tr' ).each(function(index, element) {
		$( this ).data( 'isChoice', true ).children().each(function(index, element) {
			$( this ).css( { 'backgroundColor': '#2E76CA', 'color': '#FFF' } ) ;
		});
		++sum_host_num ;
	} ) ;
	$( '#choiceHostInfo' ).text( '共 ' + sum_host_num + ' 台主机，已选择 ' + sum_host_num + ' 台主机' ) ;
}

/*
 * 反选主机
 */
function inverseCheckAllHost()
{
	var check_line_num = 0 ;
	var sum_host_num = 0 ;
	$( '#choiceHostGrid > .grid-body > .grid-tr' ).each(function(index, element) {
		if( switchHostGrid( index ) )
		{
			++check_line_num ;
		}
		++sum_host_num ;
	} ) ;
	$( '#choiceHostInfo' ).text( '共 ' + sum_host_num + ' 台主机，已选择 ' + check_line_num + ' 台主机' ) ;
}

/*
 * 点击选择主机列表的事件
 */
function onCheckedHostGrid( line )
{
	switchHostGrid( line ) ;
	var hostInfo = [] ;
	var sum_host_num = 0 ;
	var check_line_num = 0 ;
	$( '#choiceHostGrid > .grid-body > .grid-tr' ).each(function(index, element) {
		if( $( this ).data( 'isChoice' ) )
		{
			++check_line_num ;
			hostInfo.push( { 'HostName': $( this ).data( 'HostName' ) } ) ;
		}
		++sum_host_num ;
	} ) ;

	if( check_line_num > 0 )
	{
		_businessConfig['HostInfo'] = hostInfo ;
	}
	else
	{
		delete _businessConfig['HostInfo'] ;
	}
	$.cookie( 'SdbBusinessConfig', JSON.stringify( _businessConfig ) ) ;
	$( '#choiceHostInfo' ).text( '共 ' + sum_host_num + ' 台主机，已选择 ' + check_line_num + ' 台主机' ) ;
}

/*
 * 打开主机选择模态框
 */
function openChoiceHostModal()
{
	sdbjs.fun.openModal( 'choiceHostModal' ) ;
	sdbjs.fun.gridRevise( 'choiceHostGrid' ) ;
}

/*
 * 获取业务模板
 */
function getBusinessTemplate()
{
	var rc = false ;
	var jsonArr = {} ;
	var businessType = $.cookie( 'SdbBusinessType' ) ;
	var order = { 'cmd': 'query business template', 'BusinessType': businessType } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		_businessTemplate = jsonArr ;
		var clusterName = $.cookie( 'SdbClusterName' ) ;
		var businessName = $.cookie( 'SdbBusinessName' ) ;
		_businessConfig = {} ;
		_businessConfig['ClusterName']  = clusterName ;
		_businessConfig['BusinessName'] = businessName ;
		_businessConfig['DeployMod']  = _businessTemplate[1]['DeployMod'] ;
		_businessConfig['BusinessType'] = _businessTemplate[1]['BusinessType'] ;
		_businessConfig['Property']     = [] ;
		var propertyLen = _businessTemplate[1]['Property'].length ;
		for( var i = 0; i < propertyLen; ++i )
		{
			_businessConfig['Property'].push( { 'Name': _businessTemplate[1]['Property'][i]['Name'], 'Value': _businessTemplate[1]['Property'][i]['Default'] } ) ;
		}
		$.cookie( 'SdbBusinessConfig', JSON.stringify( _businessConfig ) ) ;
		rc = true ;
	}, function( jsonArr ){
		rc = false ;
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
	return rc ;
}

/*
 * 展示模式参数表
 * 参数1 第几个模式 从1开始
 */
function switchModGrid( num )
{
	$( '#deployModGrid > .grid-body' ).children().remove() ;
	var tempTemplate = _businessTemplate[ num ] ;
	var len = tempTemplate['Property'].length ;
	for( var k = 0; k < len; ++k )
	{
		var para = tempTemplate['Property'][k] ;
		var data_json = { 'cell': [ { 'text': sdbjs.fun.htmlEncode( para['WebName'] ) },
											 { 'text': '' },
											 { 'text': sdbjs.fun.htmlEncode( para['Desc'] ) } ] } ;
		var str = createHtmlInput( para['Display'], para['Valid'], para['Default'], para['Edit'] ) ;
		if( str == '' ){ continue }
		else{ data_json['cell'][1]['text'] = str }
		sdbjs.parts.gridBox.add( 'deployModGrid', data_json ) ;
	}
}

/*
 * 创建业务
 */
function createBusiness()
{
	var select_data = [] ;
	var isfirst = true ;
	var templateLen = _businessTemplate.length ;
	for( var i = 1; i < templateLen; ++i )
	{
		var tempTemplate = _businessTemplate[i] ;
		if( isfirst == true )
		{
			select_data.push( { 'key': sdbjs.fun.htmlEncode( tempTemplate['WebName'] ), 'value': tempTemplate['DeployMod'], 'selected': true } ) ;
			isfirst = false ;
		}
		else
		{
			select_data.push( { 'key': sdbjs.fun.htmlEncode( tempTemplate['WebName'] ), 'value': tempTemplate['DeployMod'] } ) ;
		}
	}
	sdbjs.parts.selectBox.add( 'deployModBoxSelect', select_data ) ;
	$( '#deployModBoxSelect' ).change( function(){
		switchModGrid( this.selectedIndex + 1 ) ;
	} ) ;
	switchModGrid( 1 ) ;
}

/*
 * 创建高级选择的主机列表
 */
function createModalGrid()
{
	var clusterName = $.cookie( 'SdbClusterName' ) ;
	var order = { 'cmd': 'list host', 'ClusterName': clusterName } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		var len = jsonArr.length ;
		for( var i = 1; i < len; ++i )
		{
			var data_json = { 'cell': [ { 'text': sdbjs.fun.htmlEncode( jsonArr[i]['HostName'] ) },
												 { 'text': sdbjs.fun.htmlEncode( jsonArr[i]['IP'] ) } ],
									'event': 'onCheckedHostGrid( ' + ( i - 1 ) + ' )' } ;
			sdbjs.parts.gridBox.add( 'choiceHostGrid', data_json ) ;
			$( '#choiceHostGrid > .grid-body > .grid-tr:last' ).data( 'HostName', jsonArr[i]['HostName'] ) ;
			$( '#choiceHostGrid > .grid-body > .grid-tr:last' ).data( 'isChoice', false ) ;
		}
		$( '#choiceHostInfo' ).text( '共 ' + ( len - 1 ) + ' 台主机，已选择 0 台主机' ) ;
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 检测配置是否正确
 */
function checkBusinessConfig()
{
	var selectNum = $( '#deployModBoxSelect' ).get(0).selectedIndex + 1 ;
	var inputArray = $( '#deployModGrid > .grid-body .form-control' ) ;
	var property = _businessTemplate[selectNum]['Property'] ;
	var propertyLen = property.length ;
	for( var k = 0; k < propertyLen; ++k )
	{
		var inputValue = $( inputArray ).eq(k).val() ;
		if( property[k]['Display'] == 'edit box' )
		{
			inputValue = $.trim( inputValue ) ;
		}
		var returnValue = checkInputValue( property[k]['Display'], property[k]['Type'], property[k]['Valid'], property[k]['WebName'], inputValue ) ;
		if( returnValue[0] == false )
		{
			showInfoFromFoot( 'danger', returnValue[1] ) ;
			return;
		}
	}

	_businessConfig['DeployMod'] = _businessTemplate[selectNum]['DeployMod'] ;
	_businessConfig['Property'] = [] ;
	var propertyLen = _businessTemplate[selectNum]['Property'].length ;
	for( var k = 0; k < propertyLen; ++k )
	{
		if( property[k]['Display'] == 'edit box' )
		{
			_businessConfig['Property'].push( { 'Name': _businessTemplate[selectNum]['Property'][k]['Name'], 'Value': $.trim( $( inputArray ).eq(k).val() ) } ) ;
		}
		else
		{
			_businessConfig['Property'].push( { 'Name': _businessTemplate[selectNum]['Property'][k]['Name'], 'Value': $( inputArray ).eq(k).val() } ) ;
		}
	}
	$.cookie( 'SdbBusinessConfig', JSON.stringify( _businessConfig ) ) ;
	gotoPage( __Deployment[2] ) ;
}

$(document).ready(function()
{
	if( $.cookie( 'SdbUser' ) == undefined || $.cookie( 'SdbPasswd' ) == undefined || $.cookie( 'SdbClusterName' ) == undefined || $.cookie( 'SdbBusinessName' ) == undefined || $.cookie( 'SdbBusinessType' ) == undefined || $.cookie( 'SdbGuideOrder' ) == undefined )
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
		//------------ 导航和底部 -------------//
		var SdbGuideOrder = $.cookie( 'SdbGuideOrder' ) ;
		if( SdbGuideOrder == 'Deployment' )
		{
			$( '#__goBack' ).get(0).onclick = Function( 'gotoPage("' + __Deployment[0] + '")' ) ;
			$( '#__goOn' ).get(0).onclick = Function( 'checkBusinessConfig()' ) ;
		}
		else if( SdbGuideOrder == 'AddBusiness' )
		{
			$( '#__goBack' ).get(0).onclick = Function( 'gotoPage("index.html")' ) ;
			$( '#__goOn' ).get(0).onclick = Function( 'checkBusinessConfig()' ) ;
		}
		else
		{
			gotoPage( "index.html" ) ;
		}
		var guideLen = __processPic[SdbGuideOrder].length ;
		for( var i = 0; i < guideLen; ++i )
		{
			if( ( SdbGuideOrder == 'Deployment' && i == 2 ) || ( SdbGuideOrder == 'AddBusiness' && i == 0 ) )
			{
				$( '#tab_box' ).append( '<li class="active">' + __processPic[SdbGuideOrder][i] + '</li>' ) ;
			}
			else
			{
				$( '#tab_box' ).append( '<li>' + __processPic[SdbGuideOrder][i] + '</li>' ) ;
			}
		}

		sdbjs.fun.autoCorrect( { 'obj': $( '#deployModBox' ), 'style': { 'width': 'parseInt( sdbjs.public.width * 0.8 )' } } ) ;		
		sdbjs.parts.gridBox.create( 'deployModGrid', {},  [ [ '属性', '值', '说明' ] ], [ 4, 7, 7 ] ).appendTo( $( '#deployModBox > .panel-body' ) ) ;
		sdbjs.fun.gridRevise( 'deployModGrid' ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#deployModGrid' ), 'style': { 'maxHeight': 'sdbjs.public.height - 201' } } ) ;
		
		sdbjs.parts.gridBox.create( 'choiceHostGrid', {},  [ [ '主机', 'IP' ] ], [ 1, 1 ] ).appendTo( $( '#choiceHostModal > .modal-body' ) ) ;
		
		setUser() ;
		
		if( getBusinessTemplate() )
		{
			createBusiness() ;
			createModalGrid() ;
		}
	}
	
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ), 'style': { 'height': 'sdbjs.public.height - 131' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - 428' } } ) ;
	
	sdbjs.fun.endOfCreate() ;
});