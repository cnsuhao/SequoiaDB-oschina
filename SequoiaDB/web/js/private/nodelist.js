window.onresize = function()
{
	var htmlbody = getPartsList( 'htmlbody' ) ;
	if( htmlbody != null )
	{
		var width, height, body_width, body_height, context_width_1, context_width_2 ;
		setHtmlBodyStyle( 1 ) ;
		width  = get_window_width() ;
		height = get_window_height() ;
		setHtmlBodyStyle( 0 ) ;
		
		setHtml( width, height ) ;
		
		body_width  = width ;
		body_height	= height - 31 - 66 - 34 ;
		
		var tab_box = getPartsList( 'tab_box' ) ;
		tab_box.set( { 'style': { 'width': ( width - 10 ) + 'px'} } ) ;
		
		var context_tra = getPartsList( 'context_tra' ) ;
		context_width_1 = parseInt( body_width / 3 ) ;
		context_width_2 = body_width - context_width_1 ;
		//创建中间内容框架
		context_tra.set( { 'style': { 'width': width + 'px' , 'height': body_height + 'px' } }, [ width + 'px' ] ) ;
		//创建面板
		var panel_box_1 = getPartsList( 'panel_box_1' ) ;
		panel_box_1.set( { 'style': { 'height': ( body_height - 20 ) + 'px', 'margin': '10px' } } ) ;
		//创建垂直框架
		var nodes_ver = getPartsList( 'nodes_ver' ) ;
		nodes_ver.set( {}, [ '60px', ( body_height - 100 ) + 'px' ] ) ;
		//表格
		var grid_box_1	= getPartsList( 'grid_01' ) ;
		grid_box_1.setTitle( [ 15, 20, 15, 15, 20, 15, 15 ] ) ;
		grid_box_1.set( { 'style' : { 'margin': '0', 'maxHeight' : ( body_height - 150 ) + 'px' } } ) ;
		grid_box_1.setAllTrStyle() ;
	}
}

function getNodeList( assemblyObj )
{
	var constRole = [ 'coord', 'catalog', 'date', 'date' ] ;
	var constStatus = [ '正常', '运行状况不良' ]
	var nodeList = [] ;
	var versionList = {} ;
	var groupList = {} ;
	var isfirst = true ;
	
	var grid_box_1	= getPartsList( 'grid_01' ) ;
	for( var i = 1, k = 1; i <= 20; ++i )
	{
		var status = 0 ;
		if( i % 5 == 0 )
		{
			status = 1 ;
		}
		nodeList.push( { 'host': 'host' + i, 'port': '118' + ( k + 1 ) + '0', 'version': '1.' + ( k + 5 ), 'group': 'group_' + k, 'role': constRole[k - 1], 'nodeStatus': constStatus[status], 'groupStatus': '正常' } ) ;
		
		versionList['1.' + ( k + 5 )] = 1 ;
		groupList['group_' + k] = 1 ;
		
		if( i % 5 == 0 )
		{
			++k ;
		}
	}
	
	//创建输入框
	var input_box_1 = assemblyObj.inputBox() ;
	input_box_1.create( { 'id': 's_host', 'value': '' } ) ;
	var input_box_2 = assemblyObj.inputBox() ;
	input_box_2.create( { 'id': 's_port', 'value': '' } ) ;
	
	//创建下拉框
	//版本
	var select_options = [ { 'name': '全部', 'selected': true } ] ;
	for( var key in versionList )
	{
		select_options.push( { 'name': key }  ) ;
	}
	var select_box_1 = assemblyObj.selectBox() ;
	select_box_1.create( { 'id': 'select_box_1', 'options': select_options } ) ;
	//分区组
	var select_options = [ { 'name': '全部', 'selected': true } ] ;
	for( var key in groupList )
	{
		select_options.push( { 'name': key }  ) ;
	}
	var select_box_2 = assemblyObj.selectBox() ;
	select_box_2.create( { 'id': 'select_box_2', 'options': select_options } ) ;
	//角色
	var select_box_3 = assemblyObj.selectBox() ;
	select_box_3.create( { 'id': 'select_box_3', 'options': [ { 'name': '全部', 'selected': true }, { 'name': 'coord' }, { 'name': 'catalog' }, { 'name': 'date' } ] } ) ;
	//节点状态
	var select_box_4 = assemblyObj.selectBox() ;
	select_box_4.create( { 'id': 'select_box_4', 'options': [ { 'name': '全部', 'selected': true }, { 'name': '正常' }, { 'name': '运行状况不良' } ] } ) ;
	//分区组状态
	var select_box_5 = assemblyObj.selectBox() ;
	select_box_5.create( { 'id': 'select_box_5', 'options': [ { 'name': '全部', 'selected': true }, { 'name': '正常' }, { 'name': '运行状况不良' } ] } ) ;
	
	//添加标题
	grid_box_1.addTitle( [ select_box_4.element, input_box_1.element, input_box_2.element, select_box_1.element, select_box_3.element, select_box_2.element, select_box_5.element ] ) ;
	grid_box_1.setTitle( [ 15, 20, 15, 15, 20, 15, 15 ] ) ;
	
	var len = nodeList.length ;
	for( var i = 0; i < len; ++i )
	{
		var data_json = { 'rows': [ { 'context': nodeList[i]['nodeStatus'] },
									 		 { 'context': nodeList[i]['host'] },
											 { 'context': nodeList[i]['port'] },
											 { 'context': nodeList[i]['version'] },
											 { 'context': nodeList[i]['role'] },
											 { 'context': nodeList[i]['group'] },
											 { 'context': nodeList[i]['groupStatus'] } ],
								'isclick': false,
								'fun': '' } ;
		grid_box_1.add_row( data_json ) ;
	}
}

window.onload=function()
{
	var width, height, body_width, body_height, context_width_1, context_width_2 ;
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
	icon_box_2.create( { 'id': 'icon_box_2', 'src': './images/smallicon/blacks/16x16/picture.png' } ) ;
	var icon_box_3 = assemblyObj.iconBox() ;
	icon_box_3.create( { 'id': 'icon_box_3', 'src': './images/smallicon/blacks/16x16/list_bullets.png' } ) ;
	var icon_box_4 = assemblyObj.iconBox() ;
	icon_box_4.create( { 'id': 'icon_box_4', 'src': './images/smallicon/blacks/16x16/comp.png' } ) ;
	var icon_box_5 = assemblyObj.iconBox() ;
	icon_box_5.create( { 'id': 'icon_box_5', 'src': './images/smallicon/blacks/16x16/share.png' } ) ;
	
	//创建标签页
	var tab_data = { 'tab': [ { 'name' : get_assembly_code(icon_box_1.element) + ' 状态', 'status': 'active' }, { 'name': get_assembly_code(icon_box_2.element) + ' 图表库' } ] } ;
	var tab_box = assemblyObj.tabPage() ;
	tab_box.create( { 'id': 'tab_box', 'style': { 'width': ( width - 10 ) + 'px', 'paddingLeft': '10px', 'paddingTop': '5px', 'fontWeight': 'bold' } }, tab_data ) ;
	
	//创建中间内容框架
	var context_tra = frameObj.transverse() ;
	context_tra.create( { 'id': 'context_tra', 'style': { 'width': width + 'px' , 'height': body_height + 'px', 'padding': '0' } }, [ width + 'px' ] ) ;
	
	//创建面板
	var panel_box_1 = assemblyObj.panel() ;
	panel_box_1.create2( { 'id': 'panel_box_1', 'style': { 'height': ( body_height - 20 ) + 'px', 'margin': '10px' } } ) ;
	set_element_style( panel_box_1.element[0], { 'backgroundColor': '#FBFBFB' } ) ;
	set_element_style( panel_box_1.element[1], { 'text': 'Cluster: cluster1' } ) ;
	
	//创建垂直框架
	var nodes_ver = frameObj.verticalBox() ;
	nodes_ver.create( { 'id': 'nodes_ver' }, [ '60px', ( body_height - 100 ) + 'px' ] ) ;
	
	//下拉菜单
	var dropDown_box_1 = assemblyObj.dropDown() ;
	dropDown_box_1.create( { 'id': 'dropDown_box_1', 'size': 'big' }, { 'name': get_assembly_code(icon_box_3.element) + ' 已选定的操作', 'list': [ { 'name': '启动节点' }, { 'name': '停止节点' }, { 'name': '' }, { 'name': '删除节点' }, { 'name': '' }, { 'name': '修改节点配置' } ] } ) ;
	var dropDown_box_2 = assemblyObj.dropDown() ;
	dropDown_box_2.create( { 'id': 'dropDown_box_2', 'size': 'big' }, { 'name': '删除分区组', 'list': [ { 'name': 'g1' }, { 'name': 'g2' }, { 'name': 'g3' } ] } ) ;
	
	//按钮
	var button_box_1 = assemblyObj.buttonBox() ;
	button_box_1.create( { 'id': 'button_box_1', 'type': 'default' }, { 'name': get_assembly_code(icon_box_4.element) + ' 添加主机', 'event': '' } ) ;
	var button_box_2 = assemblyObj.buttonBox() ;
	button_box_2.create( { 'id': 'button_box_2', 'type': 'default' }, { 'name': get_assembly_code(icon_box_5.element) + ' 添加分区组', 'event': '' } ) ;
	
	//创建简易表格
	var simpleTable_box_1 = assemblyObj.simpleTable() ;
	simpleTable_box_1.create( { 'id': 'simpleTable_box_1', 'style': { 'borderWidth': '0px' } } ) ;
	var cols = simpleTable_box_1.insert( { 'cols': [ 1, 1, 1, 1, 1 ], 'style': { 'borderWidth': '0px' } } ) ;
	set_element_style( cols[0], { 'width': '140px' } ) ;
	set_element_style( cols[1], { 'width': '100px' } ) ;
	set_element_style( cols[2], { 'width': '110px' } ) ;
	set_element_style( cols[3], { 'width': '120px' } ) ;
	addChildEle( cols[0], dropDown_box_1 ) ;
	addChildEle( cols[1], button_box_1 ) ;
	addChildEle( cols[2], button_box_2 ) ;
	addChildEle( cols[3], dropDown_box_2 ) ;
	
	//表格
	var grid_box_1	= assemblyObj.gridBox() ;
	grid_box_1.create( { 'id' : 'grid_01', 'style' : { 'margin': '0', 'maxHeight' : ( body_height - 150 ) + 'px' }, 'title' : [ '节点状态', '主机', '端口', '版本', '角色', '分区组', '分区组状态' ] } ) ;
	
	addChildEle( nodes_ver.element[1], simpleTable_box_1 ) ;
	addChildEle( nodes_ver.element[2], grid_box_1 ) ;
	addChildEle( panel_box_1.element[2], nodes_ver ) ;
	addChildEle( context_tra.element[1], panel_box_1 ) ;
	addChildEle( body_ver.element[1], tab_box ) ;
	addChildEle( body_ver.element[3], context_tra ) ;
	
	grid_box_1.setTitle( [ 15, 20, 15, 15, 20, 15, 15 ] ) ;
	
	getNodeList( assemblyObj ) ;
}