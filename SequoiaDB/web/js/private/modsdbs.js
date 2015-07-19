//部署模式
var _deployModel = null ;

//业务配置
var _businessConfig = null ;

//业务参数
var _businessPara = null ;

//---------------------------------- node ----------------------------------

//保存节点参数
function saveNodePara( index )
{
	sdbjs.parts.gridBox.repigment( 'modNodeConfGrid', true ) ;
	sdbjs.parts.gridBox.repigment( 'modNodeConfGrid2', true ) ;
	function nodeParaError( obj, level, errMsg )
	{
		$( obj ).parent().parent().css( 'background-color', '#f2dede' ) ;
		showFootStatus( 'danger', errMsg ) ;
		if( level === '0' )
		{
			sdbjs.parts.navTabBox.show( 'modNodeConfTab', 0, function(){
				var offsetHeight = $( obj ).parent().parent().offset().top - $( obj ).parent().parent().parent().offset().top ;
				var gridBodyNode = sdbjs.fun.getNode( 'modNodeConfGrid~body' ) ;
				$( gridBodyNode['obj'] ).get(0).scrollTop = offsetHeight ;
			} ) ;
		}
		else
		{
			sdbjs.parts.navTabBox.show( 'modNodeConfTab', 1, function(){
				var offsetHeight = $( obj ).parent().parent().offset().top - $( obj ).parent().parent().parent().offset().top ;
				var gridBodyNode = sdbjs.fun.getNode( 'modNodeConfGrid2~body' ) ;
				$( gridBodyNode['obj'] ).get(0).scrollTop = offsetHeight ;
			} ) ;
		}
	}
	//参数1是节点的下标，参数2是选择的节点的第几个
	function saveOndeNode( tempValueList )
	{
		//保存节点
		$.each( tempValueList, function( nodeConfKey, nodeConfValue ){
			_businessPara['Config'][0][ nodeConfKey ] = nodeConfValue ;
		} ) ;
	}
	var rc = true ;
	//加载输入框的数据
	var valueList = {} ;
	$.each( _businessPara['Property'], function( paraID, property ){
		if( property['Display'] !== 'hidden' )
		{
			var value = $( '#' + property['Name'] + '_np' ).val() ;
			var rs = checkInputValue( property['Display'], property['Type'], property['Valid'], property['WebName'], value ) ;
			if( rs !== '' )
			{
				//报错
				nodeParaError( $( '#' + property['Name'] + '_np' ), property['Level'], rs ) ;
				rc = false ;
				return false ;
			}
			valueList[ property['Name'] ] = $( '#' + property['Name'] + '_np' ).val() ;
		}
	} ) ;

	if( rc === false )
	{
		return false ;
	}
	saveOndeNode( valueList ) ;
	return true ;
}

//重绘节点参数模态框
function redrawNodePataModal()
{
	$( document.body ).css( 'overflow', 'hidden' ) ;
	
	var gridNode = sdbjs.fun.getNode( 'modNodeConfGrid' ) ;
	var maxHeight = parseInt( $( gridNode['obj'] ).parent().parent().css( 'max-height' ) ) - 62 ;
	try{
		sdbjs.fun.setCSS( 'modNodeConfGrid~body', { 'max-height': maxHeight } ) ;
		sdbjs.fun.setCSS( 'modNodeConfGrid2~body', { 'max-height': maxHeight } ) ;
		sdbjs.parts.modalBox.redraw( 'modNodeConf' ) ;
	}catch(e){}
	$( document.body ).css( 'overflow', 'visible' ) ;
	//滚动条回到顶部
	var gridBodyNode = sdbjs.fun.getNode( 'modNodeConfGrid~body' ) ;
	$( gridBodyNode['obj'] ).get(0).scrollTop = 0 ;
	var gridBodyNode2 = sdbjs.fun.getNode( 'modNodeConfGrid2~body' ) ;
	$( gridBodyNode2['obj'] ).get(0).scrollTop = 0 ;
}

//--------------------- 初始化 -------------------

//返回
function returnPage()
{
	gotoPage( 'confsdb.html' ) ;
}

//下一步
function nextPage()
{
	if( saveNodePara() )
	{
		var tempBusinessPara = {} ;
		tempBusinessPara['ClusterName'] = _businessPara['ClusterName'] ;
		tempBusinessPara['BusinessType'] = _businessPara['BusinessType'] ;
		tempBusinessPara['BusinessName'] = _businessPara['BusinessName'] ;
		tempBusinessPara['DeployMod'] = _businessPara['DeployMod'] ;
		tempBusinessPara['Config'] = _businessPara['Config'] ;

		sdbjs.parts.loadingBox.show( 'loading' ) ;
		restAddBusiness( true, function( jsonArr, textStatus, jqXHR ){
			var taskID = jsonArr[0]['TaskID'] ;
			sdbjs.fun.saveData( 'SdbTaskID', taskID ) ;
			gotoPage( 'installsdb.html' ) ;
		}, function( json ){
			sdbjs.parts.loadingBox.hide( 'loading' ) ;
			showProcessError( json['detail'] ) ;
		}, null, tempBusinessPara ) ;
	}
}

//加载业务配置
function loadBusinessConf()
{
	var rc = false ;
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restGetBusinessConfig( false, function( jsonArr, textStatus, jqXHR ){
		_businessPara = jsonArr[0] ;
		rc = true ;
	}, function( json ){
		showProcessError( json['detail'] ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
	}, _businessConfig ) ;
	return rc ;
}

//加载配置参数列表
function loadNodePara()
{
	//主机名
	sdbjs.parts.gridBox.addBody( 'modNodeConfGrid', [{ 'text': htmlEncode( _languagePack['modsdbs']['panel']['nodeConf']['hostName'] ), 'width': '20%' },
																	 { 'text': htmlEncode( _businessPara['Config'][0]['HostName'] ), 'width': '40%' },
																	 { 'text': htmlEncode( '' ), 'width': '40%' } ]  ) ;
	
	//遍历所有配置项
	$.each( _businessPara['Property'], function( index, property ){
		var newInputObj = createHtmlInput( property['Display'], property['Valid'], _businessPara['Config'][0][property['Name']], property['Edit'] ) ;
		if( newInputObj !== null )
		{
			var nodeName = '' ;
			$( newInputObj ).attr( 'id', property['Name'] + '_np' ) ;
			if( property['Level'] == 0 )
			{
				nodeName = 'modNodeConfGrid' ;
			}
			else
			{
				nodeName = 'modNodeConfGrid2' ;
			}
			sdbjs.parts.gridBox.addBody( nodeName, [{ 'text': htmlEncode( property['WebName'] ), 'width': '20%' },
																 { 'text': function( tdObj ){
																	 $( tdObj ).append( newInputObj ) ;
																 }, 'width': '40%' },
																 { 'text': htmlEncode( property['Desc'] ), 'width': '40%' } ]  ) ;
		}
	} ) ;
	
	redrawNodePataModal() ;
}

function createDynamicHtml()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	if( loadBusinessConf() === true )
	{
		loadNodePara() ;
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
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/zoom.png"> ' + htmlEncode( _languagePack['public']['tabPage'][2] ), false, null ) ;
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][3] ), false, null );
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][4] ), false, null );
	}
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/home.png"> ' + htmlEncode( _languagePack['public']['tabPage'][5] ), false, null ) ;
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][6] ), true, null );
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][7] ), false, null );
	
	/* 外框 */
	sdbjs.parts.divBox.create( 'middle', 'middleDiv', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'middleDiv', { 'padding': '10px' } ) ;
	
	/* 参数框 */
	sdbjs.parts.panelBox.create( 'middleDiv', 'groupListBar', 'auto', 'variable' ) ;
	sdbjs.parts.panelBox.update( 'groupListBar', function( panelTitle ){
		sdbjs.parts.divBox.create( panelTitle['name'], 'businessNameDiv' ) ;
		sdbjs.fun.setCSS( 'businessNameDiv', { 'text-overflow': 'ellipsis', 'overflow': 'hidden', 'white-space': 'nowrap' } ) ;
		sdbjs.parts.divBox.update( 'businessNameDiv', function( divObj ){
			//业务
			$( divObj ).text( _languagePack['modsdbs']['title'] + _businessConfig['BusinessName'] ) ;
		} ) ;
		
	}, function( panelBody ){
		sdbjs.parts.navTabBox.create( panelBody['name'], 'modNodeConfTab', 'auto', 'variable' ) ;
		//普通
		sdbjs.parts.navTabBox.add( 'modNodeConfTab', _languagePack['modsdbs']['tab'][0], function( divObj ){
			sdbjs.parts.gridBox.create( divObj, 'modNodeConfGrid', 'auto', 'variable' ) ;
			// 属性 值 说明
			sdbjs.parts.gridBox.addTitle( 'modNodeConfGrid', [{ 'text': htmlEncode( _languagePack['modsdbs']['panel']['nodeConf']['title'][0] ), 'width': '20%' },
																			  { 'text': htmlEncode( _languagePack['modsdbs']['panel']['nodeConf']['title'][1] ), 'width': '40%' },
																			  { 'text': htmlEncode( _languagePack['modsdbs']['panel']['nodeConf']['title'][2] ), 'width': '40%' } ]  ) ;
		}, 'redrawNodePataModal()' ) ;
		//高级
		sdbjs.parts.navTabBox.add( 'modNodeConfTab', _languagePack['modsdbs']['tab'][1], function( divObj ){
			sdbjs.parts.gridBox.create( divObj, 'modNodeConfGrid2' ) ;
			// 属性 值 说明
			sdbjs.parts.gridBox.addTitle( 'modNodeConfGrid2', [{ 'text': htmlEncode( _languagePack['modsdbs']['panel']['nodeConf']['title'][0] ), 'width': '20%' },
																				{ 'text': htmlEncode( _languagePack['modsdbs']['panel']['nodeConf']['title'][1] ), 'width': '40%' },
																				{ 'text': htmlEncode( _languagePack['modsdbs']['panel']['nodeConf']['title'][2] ), 'width': '40%' } ]  ) ;
		}, 'redrawNodePataModal()' ) ;
	} ) ;
	
	/* ** */
	sdbjs.parts.divBox.create( 'middle', 'middle-clear', 0, 0 ) ;
	sdbjs.fun.setClass( 'middle-clear', 'clear-float' ) ;

	//返回 下一步
	sdbjs.parts.buttonBox.create( 'operate', 'deployReturn' ) ;
	sdbjs.parts.buttonBox.update( 'deployReturn', function( buttonObj ){
		$( buttonObj ).text( _languagePack['public']['button']['return']) ;
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
	_businessConfig = sdbjs.fun.getData( 'SdbBusinessConfig' ) ;
	if( _businessConfig === null )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	else
	{
		_businessConfig = JSON.parse( _businessConfig ) ;
	}
	if( _businessConfig['DeployMod'] !== 'standalone' )
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
	return rc ;
}

$(document).ready(function(){
	if( checkReady() === true )
	{
		sdbjs.fun.saveData( 'SdbStep', 'modsdbs' ) ;
		createHtml() ;
		createDynamicHtml() ;
	}
} ) ;

$(window).resize(function(){
	redrawNodePataModal() ;
} ) ;