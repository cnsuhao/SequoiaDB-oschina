//--------------------------------- 页面全局变量 -----------------------------------//

//业务列表
var _businessList = [] ;

//--------------------------------- 页面函数 -----------------------------------//

/*
 * 跳转到添加业务
 */
function gotoBusiness()
{
	var value = [] ;
	$( '#addbusinessModal > .modal-body .form-control' ).each(function(index, element) {
      value.push( $( this ).val() ) ;
   });
	value[0] = $.trim( value[0] ) ;
	if( !sdbjs.fun.checkStrName( value[0] ) )
	{
		$( '#addbusinessModal > .modal-body' ).children( ':eq(1)' ).text( '业务名必须以下划线或英文字母开头，只含有下划线英文字母数字，长度在 1 - 255 范围。' ) ;
		return;
	}
	$.cookie( 'SdbGuideOrder', 'AddBusiness' ) ;
	$.cookie( 'SdbComeback', 'businesslist.html' ) ;
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
 * 删除业务
 */
function removeBusiness()
{
	var businessName = $( '#removeBusinessModal' ).data( 'businessName' ) ;
	var order = { 'cmd': 'remove business', 'BusinessName': businessName } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		$.cookie( 'SdbBusinessName', businessName ) ;
		$.cookie( 'SdbTaskID', jsonArr[1]['TaskID'] ) ;
		$.cookie( 'SdbComeback', 'businesslist.html' ) ;
		gotoPage( 'uninstallbusiness.html' ) ;
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 打开删除业务模态框
 */
function openRemoveBusinessModal( businessName )
{
	$( '#removeBusinessModal' ).data( 'businessName', businessName ) ;
	sdbjs.fun.openModal( 'removeBusinessModal' ) ;
}

/*
 * 获取业务列表
 */
function getBusinessList()
{
	function getBusinessConfig()
	{
		var len = _businessList.length ;
		for( var i = 0; i < len; ++i )
		{
			var order = { 'cmd': 'query business', 'BusinessName': _businessList[i]['BusinessName'] } ;
			ajaxSendMsg( order, false, function( jsonArr ){
				_businessList[i]['BusinessName'] = jsonArr[1]['BusinessName'] ;
				_businessList[i]['BusinessType'] = jsonArr[1]['BusinessType'] ;
				_businessList[i]['DeployMod'] = jsonArr[1]['DeployMod'] ;
			} ) ;
		}
	}
	var clusterName = $.cookie( 'SdbClusterName' ) ;
	var order = { 'cmd': 'list business', 'ClusterName': clusterName } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		var len = jsonArr.length ;
		for( var i = 1; i < len; ++i )
		{
			_businessList.push( jsonArr[i] ) ;
		}
		getBusinessConfig() ;
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 创建业务列表
 */
function createBusinessList()
{
	var businessLen = _businessList.length ;
	var i = 0 ;
	function createBusinessListT()
	{
		sdbjs.parts.gridBox.add( 'businessListGrid', { 'cell': [ { 'text': sdbjs.fun.htmlEncode( _businessList[i]['BusinessName'] ) }, { 'text': sdbjs.fun.htmlEncode( _businessList[i]['BusinessType'] ) }, { 'text': sdbjs.fun.htmlEncode( _businessList[i]['DeployMod'] ) }, { 'text': '<button class="btn btn-lg btn-default" onclick="openRemoveBusinessModal(\'' + _businessList[i]['BusinessName'] + '\')">删除业务</button>' } ] } ) ;
				
		++i ;
		sdbjs.fun.setLoading( 'loading', parseInt( i / businessLen * 100 ) ) ;
		if( i < businessLen )
		{
			setTimeout( createBusinessListT, 1 ) ;
		}
		else
		{
			sdbjs.fun.closeLoading( 'loading' ) ;
		}
	}
	if( businessLen > 0 )
	{
		sdbjs.fun.openLoading( 'loading' ) ;
		setTimeout( createBusinessListT, 0 ) ;
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
		
		sdbjs.fun.autoCorrect( { 'obj': $( '#businessListPanel' ), 'style': { 'height': 'sdbjs.public.height - 155' } } ) ;
		
		sdbjs.parts.gridBox.create( 'businessListGrid', {}, [ [ '业务名', '业务类型', '部署模式', '操作' ] ], [ 5, 5, 5, 3 ] ).appendTo( $( '#businessListPanel > .panel-body > div:eq(1)' ) ) ;
		$( '#businessListPanel > .panel-header' ).text( '集群：' + $.cookie( 'SdbClusterName' ) ) ;
		sdbjs.fun.gridRevise( 'businessListGrid' ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#businessListGrid' ), 'style': { 'maxHeight': 'sdbjs.public.height - 280' } } ) ;
		
		getBusinessType() ;
		getBusinessList() ;
		createBusinessList() ;
		setUser() ;
		
		sdbjs.parts.gridBox.create( 'removeBusinessListGrid', {}, [ [ '', '主机', '结果' ] ], [ 1, 5, 5 ] ).appendTo( $( '#removeBusinessResultModal > .modal-body' ) ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#removeBusinessListGrid' ), 'style': { 'max-height': 'parseInt(sdbjs.public.height * 0.8) - 130' } } ) ;
	}
	
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ), 'style': { 'height': 'sdbjs.public.height - 131' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - 428' } } ) ;
	
	sdbjs.fun.endOfCreate() ;
});