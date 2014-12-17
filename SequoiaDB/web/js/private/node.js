window.onresize = function()
{
	var htmlbody = getPartsList( 'htmlbody' ) ;
	if( htmlbody != null )
	{
		var width, height, body_width, body_height ;
		setHtmlBodyStyle( 1 ) ;
		width  = get_window_width() ;
		height = get_window_height() ;
		setHtmlBodyStyle( 0 ) ;
		
		setHtml( width, height ) ;
		
		body_width  = width ;
		body_height	= height - 31 - 66 - 34 ;
		
		var tab_box = getPartsList( 'tab_box' ) ;
		tab_box.set( { 'style': { 'width': ( width - 10 ) + 'px'} } ) ;
		
		var panel_box_1 = getPartsList( 'panel_box_1' ) ;
		panel_box_1.set( { 'style': { 'height': body_height + 'px' } } ) ;
		
		var context_width   = width ;
		var context_width_1 = parseInt( context_width / 2 ) ;
		var context_width_2 = context_width - context_width_1 ;
		var context_tra = getPartsList( 'context_tra' ) ;
		context_tra.set( { 'style': { 'width': width + 'px' , 'height': ( body_height - 46 ) + 'px' } }, [ context_width_1 + 'px', context_width_2 + 'px' ] ) ;
		
		var ver_height   = body_height - 46 ;
		var ver_height_1 = 140 ;
		var ver_height_2 = parseInt( ( ver_height - ver_height_1 ) / 2 ) ;
		var ver_height_3 = ver_height - ver_height_1 - ver_height_2 ;
		
		var panel_box_3_1 = getPartsList( 'panel_box_3_1' ) ;
		set_element_style( panel_box_3_1.element[1], { 'maxHeight': ( ver_height_2 - 52 ) + 'px' } ) ;
		
		var grid_box_1	= getPartsList( 'grid_01' ) ;
		grid_box_1.set( { 'style' : { 'maxHeight' : ( ver_height_3 - 52 ) + 'px' } } ) ;
		grid_box_1.setTitle( [ 10, 30, 30 ] ) ;
		grid_box_1.setAllTrStyle() ;
		
		var panel_box_5 = getPartsList( 'panel_box_5' ) ;
		set_element_style( panel_box_5.element[2], { 'height': ver_height - 52 + 'px' } ) ;
		
		var pic_tra = getPartsList( 'pic_tra' ) ;
		pic_tra.set( { 'style': { 'width': context_width_2 - 40 + 'px' , 'height': 200 + 'px' } }, [ '350px', '350px', '350px', '350px', '350px' ] ) ;
	}
}

function getRunStatus( assemblyObj, runStatusBox )
{
	var fold_box, panel_box ;
	for( var i = 1; i <= 8; ++i )
	{
		fold_box	= assemblyObj.fold() ;
		fold_box.create( { 'id': 'temp_fold_box_' + i, 'style': { 'margin': '0' } }, { 'text': '06-21.10:25:00', 'open': false } ) ;
		set_element_style( fold_box.element[1], { 'fontSize': '14px', 'fontWeight': 'normal', 'padding': '5px', 'color': '#9D2522', 'backgroundColor': '#FDDCDC' } ) ;
		set_element_style( fold_box.element[3], { 'text': '错误' } ) ;
		panel_box = assemblyObj.panel() ;
		panel_box.create( { 'id': 'temp_panel_box_' + i, 'style': { 'border': '0', 'boxShadow': '0 0 0 rgba(0, 0, 0, 0)' } } ) ;
		set_element_style( panel_box.element[1], { 'text': '将如下行：</br>catalog addr (hostname1:servicename1,hostname2:servicename2,...)</br>catalogaddr=修改</br>catalog addr (hostname1:servicename1,hostname2:servicename2,...)</br>catalogaddr=sdbserver1:11803,sdbserver2:11803,sdbserver3:11803  该参数为Catalog服务地址和端口', 'wordWrap': 'break-word', 'wordBreak': 'break-all' } ) ;
		addChildEle( fold_box.element[2], panel_box ) ;
		addChildEle( runStatusBox, fold_box ) ;
	}
}

function getFileSystem( assemblyObj, gridBox )
{
	for( var i = 0; i < 8; ++i )
	{
		var value   = Math.floor( Math.random() * 10000 ) ;
		var percent = parseInt( value / 100 ) ;
		var color   = '' ;
		var progress_box = assemblyObj.progressBox() ;
		if( percent < 70 )
		{
			color = 'green' ;
		}
		else if( percent < 90 )
		{
			color = 'orange' ;
		}
		else
		{
			color = 'red' ;
		}
		progress_box.create( { 'id': 'temp_progress_box_' + i }, { 'width': percent + '%', 'color': color, 'text': value + 'MB / 10000MB' } ) ;
		var data_json = { 'rows': [ { 'context': 'log' },
									 		 { 'context': '/opt/sequoiadb/date/11840/' },
											 { 'context': get_assembly_code(progress_box.element) } ],
								'isclick': false,
								'fun': '' } ;
		gridBox.add_row( data_json ) ;
	}
}

function createGroupCPU( obj )
{
	var timeTicket = null ;
	var conf = ___option ;
	var myChart = echarts.init( obj ); 
	
	// 为echarts对象加载数据 
	myChart.setOption( conf ); 
	
	timeTicket = setInterval( function() {
		myChart.addData(
			[
				[
					0,
					Math.round(Math.random() * 100), // 新增数据
					true,     // 新增数据是否从队列头部插入
					false     // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
				]
			]
		) ;
	}, 1000 ) ;
}

function createGroupMemory( obj )
{
	var timeTicket = null ;
	var conf = ___option ;
	var myChart = echarts.init( obj ); 
	
	conf['title']['text'] = '集群内存' ;
	
	// 为echarts对象加载数据 
	myChart.setOption( conf ); 
	
	timeTicket = setInterval( function() {
		myChart.addData(
			[
				[
					0,
					Math.round(Math.random() * 100), // 新增数据
					true,     // 新增数据是否从队列头部插入
					false     // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
				]
			]
		) ;
	}, 1000 ) ;
}

function createGroupDisk( obj )
{
	var timeTicket = null ;
	var conf = ___option ;
	var myChart = echarts.init( obj ); 
	
	conf['title']['text'] = '集群磁盘' ;
	
	// 为echarts对象加载数据 
	myChart.setOption( conf ); 
	
	timeTicket = setInterval( function() {
		myChart.addData(
			[
				[
					0,
					Math.round(Math.random() * 100), // 新增数据
					true,     // 新增数据是否从队列头部插入
					false     // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
				]
			]
		) ;
	}, 1000 ) ;
}

function createGroupIO( obj )
{
	var timeTicket = null ;
	var conf = ___option ;
	var myChart = echarts.init( obj ); 
	
	conf['title']['text'] = '集群磁盘IO' ;
	
	// 为echarts对象加载数据 
	myChart.setOption( conf ); 
	
	timeTicket = setInterval( function() {
		myChart.addData(
			[
				[
					0,
					Math.round(Math.random() * 100), // 新增数据
					true,     // 新增数据是否从队列头部插入
					false     // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
				]
			]
		) ;
	}, 1000 ) ;
}

function createGroupNetIO( obj )
{
	var timeTicket = null ;
	var conf = ___option ;
	var myChart = echarts.init( obj ); 
	
	conf['title']['text'] = '集群网络IO' ;
	
	// 为echarts对象加载数据 
	myChart.setOption( conf ); 
	
	timeTicket = setInterval( function() {
		myChart.addData(
			[
				[
					0,
					Math.round(Math.random() * 100), // 新增数据
					true,     // 新增数据是否从队列头部插入
					false     // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
				]
			]
		) ;
	}, 1000 ) ;
}


function getPic( frameObj, rightBox )
{
	//创建右边图表组合
	set_element_style( rightBox.element[1], { 'border': '1px solid #DDD', 'margin': '0 10px 10px 0' } ) ;
	set_element_style( rightBox.element[2], { 'border': '1px solid #DDD', 'margin': '0 10px 10px 0' } ) ;
	set_element_style( rightBox.element[3], { 'border': '1px solid #DDD', 'margin': '0 10px 10px 0' } ) ;
	set_element_style( rightBox.element[4], { 'border': '1px solid #DDD', 'margin': '0 10px 10px 0' } ) ;
	set_element_style( rightBox.element[5], { 'border': '1px solid #DDD', 'margin': '0 10px 10px 0' } ) ;
	
	createGroupCPU( rightBox.element[1] ) ;
	createGroupMemory( rightBox.element[2] ) ;
	createGroupDisk( rightBox.element[3] ) ;
	createGroupIO( rightBox.element[4] ) ;
	createGroupNetIO( rightBox.element[5] ) ;
}

window.onload=function()
{
	var width, height, body_width, body_height ;
	setHtmlBodyStyle( 1 ) ;
	width  = get_window_width() ;
	height = get_window_height() ;
	setHtmlBodyStyle( 0 ) ;

	//创建行为对象
	var eleObj			= createEle() ;
	//创建框架对象
	var frameObj		= eleObj.frame() ;
	//创建部件对象
	var assemblyObj	= eleObj.assembly() ;
	
	createHtml( width, height, eleObj, frameObj, assemblyObj ) ;
	
	body_width	= width ;
	body_height	= height - 31 - 66 - 34 ;
	
	var body_ver = getPartsList( 'body_ver' ) ;
	
	//创建图标
	var icon_box_1 = assemblyObj.iconBox() ;
	icon_box_1.create( { 'id': 'icon_box_1', 'src': './images/smallicon/blacks/16x16/home.png' } ) ;
	var icon_box_2 = assemblyObj.iconBox() ;
	icon_box_2.create( { 'id': 'icon_box_2', 'src': './images/smallicon/blacks/16x16/cog.png' } ) ;
	var icon_box_3 = assemblyObj.iconBox() ;
	icon_box_3.create( { 'id': 'icon_box_3', 'src': './images/smallicon/blacks/16x16/picture.png' } ) ;
	var icon_box_4 = assemblyObj.iconBox() ;
	icon_box_4.create( { 'id': 'icon_box_4', 'src': './images/smallicon/blacks/16x16/comp.png' } ) ;
	var icon_box_5 = assemblyObj.iconBox() ;
	icon_box_5.create( { 'id': 'icon_box_5', 'src': './images/smallicon/blacks/16x16/share.png' } ) ;
	
	//创建标签页
	var tab_data = { 'tab': [ { 'name' : get_assembly_code(icon_box_1.element) + ' 状态', 'status': 'active' }, { 'name': get_assembly_code(icon_box_2.element) + ' 配置' }, { 'name': get_assembly_code(icon_box_3.element) + ' 图表库' } ] } ;
	var tab_box = assemblyObj.tabPage() ;
	tab_box.create( { 'id': 'tab_box', 'style': { 'width': ( width - 10 ) + 'px', 'paddingLeft': '10px', 'paddingTop': '5px', 'fontWeight': 'bold' } }, tab_data ) ;
	
	//创建面板
	var panel_box_1 = assemblyObj.panel() ;
	panel_box_1.create2( { 'id': 'panel_box_1', 'style': { 'height': body_height + 'px', 'border': '0' } } ) ;
	set_element_style( panel_box_1.element[1], { 'text': 'Node: host_01', 'fontSize': '18px', 'borderBottom': '1px solid #DDDDDD', 'backgroundColor': '#DBFFCE', 'color': '#607890' } ) ;
	set_element_style( panel_box_1.element[2], { 'padding': '0' } ) ;
	
	//创建中间内容框架
	var context_width   = width ;
	var context_width_1 = parseInt( context_width / 2 ) ;
	var context_width_2 = context_width - context_width_1 ;
	var context_tra = frameObj.transverse() ;
	context_tra.create( { 'id': 'context_tra', 'style': { 'width': context_width + 'px' , 'height': ( body_height - 46 ) + 'px', 'padding': '0' } }, [ context_width_1 + 'px', context_width_2 + 'px' ] ) ;
	
	var ver_height   = body_height - 46 ;
	var ver_height_1 = 140 ;
	var ver_height_2 = parseInt( ( ver_height - ver_height_1 ) / 2 ) ;
	var ver_height_3 = ver_height - ver_height_1 - ver_height_2 ;
	//创建面板
	var panel_box_2 = assemblyObj.panel() ;
	panel_box_2.create2( { 'id': 'panel_box_2', 'style': { 'border': '0', 'boxShadow': '0 0 0 rgba(0, 0, 0, 0)' } } ) ;
	set_element_style( panel_box_2.element[1], { 'text': '详细信息', 'fontSize': '18px' } ) ;
	set_element_style( panel_box_2.element[2], { 'padding': '0 10px 0 10px' } ) ;
	var panel_box_3 = assemblyObj.panel() ;
	panel_box_3.create2( { 'id': 'panel_box_3', 'style': { 'border': '0', 'boxShadow': '0 0 0 rgba(0, 0, 0, 0)' } } ) ;
	set_element_style( panel_box_3.element[1], { 'text': '运行状况', 'fontSize': '18px' } ) ;
	set_element_style( panel_box_3.element[2], { 'padding': '0 10px 0 10px' } ) ;
	var panel_box_3_1 = assemblyObj.panel() ;
	panel_box_3_1.create( { 'id': 'panel_box_3_1' } ) ;
	set_element_style( panel_box_3_1.element[1], { 'overflowY': 'auto', 'maxHeight': ( ver_height_2 - 52 ) + 'px', 'padding': '0' } ) ;
	addChildEle( panel_box_3.element[2], panel_box_3_1 ) ;
	var panel_box_4 = assemblyObj.panel() ;
	panel_box_4.create2( { 'id': 'panel_box_4', 'style': { 'border': '0', 'boxShadow': '0 0 0 rgba(0, 0, 0, 0)' } } ) ;
	set_element_style( panel_box_4.element[1], { 'text': '文件系统', 'fontSize': '18px' } ) ;
	set_element_style( panel_box_4.element[2], { 'padding': '0 10px 0 10px' } ) ;
	//表格
	var grid_box_1	= assemblyObj.gridBox() ;
	grid_box_1.create( { 'id' : 'grid_01', 'style' : { 'margin': '0', 'maxHeight' : ( ver_height_3 - 52 ) + 'px' }, 'title' : [ '文件', '路径', '使用情况' ], 'position': 'relative' } ) ;
	addChildEle( panel_box_4.element[2], grid_box_1 ) ;
	
	var panel_box_5 = assemblyObj.panel() ;
	panel_box_5.create2( { 'id': 'panel_box_5', 'style': { 'border': '0', 'boxShadow': '0 0 0 rgba(0, 0, 0, 0)' } } ) ;
	set_element_style( panel_box_5.element[1], { 'text': '图表', 'fontSize': '18px' } ) ;
	set_element_style( panel_box_5.element[2], { 'height': ver_height - 52 + 'px', 'overflowY': 'auto', 'position': 'relative' } ) ;
	
	//创建简易表格
	var simpleTable_box_1 = assemblyObj.simpleTable() ;
	simpleTable_box_1.create( { 'id': 'simpleTable_box_1', 'style': { 'borderWidth': '0px' } } ) ;
	var cols = simpleTable_box_1.insert( { 'cols': [ 1, 2, 1, 2 ] } ) ;
	set_element_style( cols[0], { 'borderRightWidth': '0px', 'color': '#595959', 'text': 'Host' } ) ;
	set_element_style( cols[1], { 'borderLeftWidth':  '0px', 'borderRightWidth': '0px', 'color': '#000', 'fontWeight': 'bold', 'text': 'host_01' } ) ;
	set_element_style( cols[2], { 'borderLeftWidth':  '0px', 'borderRightWidth': '0px', 'color': '#595959', 'text': 'Port' } ) ;
	set_element_style( cols[3], { 'borderLeftWidth':  '0px', 'color': '#000', 'fontWeight': 'bold', 'text': '11820' } ) ;
	cols = simpleTable_box_1.insert( { 'cols': [ 1, 2, 1, 2 ] } ) ;
	set_element_style( cols[0], { 'borderRightWidth': '0px', 'color': '#595959', 'text': 'Version' } ) ;
	set_element_style( cols[1], { 'borderLeftWidth':  '0px', 'borderRightWidth': '0px', 'color': '#000', 'fontWeight': 'bold', 'text': '1.8' } ) ;
	set_element_style( cols[2], { 'borderLeftWidth':  '0px', 'borderRightWidth': '0px', 'color': '#595959', 'text': 'Role' } ) ;
	set_element_style( cols[3], { 'borderLeftWidth':  '0px', 'color': '#000', 'fontWeight': 'bold', 'text': 'Data' } ) ;
	cols = simpleTable_box_1.insert( { 'cols': [ 1, 2, 1, 2 ] } ) ;
	set_element_style( cols[0], { 'borderRightWidth': '0px', 'color': '#595959', 'text': 'Cluster' } ) ;
	set_element_style( cols[1], { 'borderLeftWidth':  '0px', 'borderRightWidth': '0px', 'color': '#000', 'fontWeight': 'bold', 'text': 'cluster_1' } ) ;
	set_element_style( cols[2], { 'borderLeftWidth':  '0px', 'borderRightWidth': '0px', 'color': '#595959', 'text': 'Group' } ) ;
	set_element_style( cols[3], { 'borderLeftWidth':  '0px', 'color': '#000', 'fontWeight': 'bold', 'text': 'group_1' } ) ;
	cols = simpleTable_box_1.insert( { 'cols': [ 1, 2, 1, 2 ] } ) ;
	set_element_style( cols[0], { 'borderRightWidth': '0px', 'color': '#595959', 'text': 'PrimaryNode' } ) ;
	set_element_style( cols[1], { 'borderLeftWidth':  '0px', 'borderRightWidth': '0px', 'color': '#000', 'fontWeight': 'bold', 'text': 'true' } ) ;
	set_element_style( cols[2], { 'borderLeftWidth':  '0px', 'borderRightWidth': '0px', 'color': '#595959' } ) ;
	set_element_style( cols[3], { 'borderLeftWidth':  '0px', 'color': '#000', 'fontWeight': 'bold' } ) ;

	var pic_tra = frameObj.transverse() ;
	pic_tra.create( { 'id': 'pic_tra', 'style': { 'width': ( context_width_2 - 40 ) + 'px' , 'height': 200 + 'px' } }, [ '350px', '350px', '350px', '350px', '350px' ] ) ;

	addChildEle( panel_box_2.element[2], simpleTable_box_1 ) ;
	addChildEle( context_tra.element[1], panel_box_2 ) ;
	addChildEle( context_tra.element[1], panel_box_3 ) ;
	addChildEle( context_tra.element[1], panel_box_4 ) ;
	addChildEle( context_tra.element[2], panel_box_5 ) ;
	addChildEle( panel_box_1.element[2], context_tra ) ;
	addChildEle( panel_box_5.element[2], pic_tra ) ;
	addChildEle( body_ver.element[1], tab_box ) ;
	addChildEle( body_ver.element[3], panel_box_1 ) ;
	
	grid_box_1.setTitle( [ 10, 30, 30 ] ) ;
	getRunStatus( assemblyObj, panel_box_3_1.element[1] ) ;
	getFileSystem( assemblyObj, grid_box_1 ) ;
	getPic( frameObj, pic_tra ) ;
}