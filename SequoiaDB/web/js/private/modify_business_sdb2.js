//--------------------------------- 页面全局变量 -----------------------------------//

//业务配置参数表
var _configProperty = [] ;
//业务配置
var _businessConfig = {} ;
//主机列表
var _hostList = [] ;

//--------------------------------- 页面函数 -----------------------------------//

/*
 * 获取自动生成的配置
 */
function getBusinessConfig()
{
	var returnValue = null ;
	var businessConfig = $.cookie( 'SdbBusinessConfig' ) ;
	var order = { 'cmd': 'config business', 'TemplateInfo': businessConfig } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		returnValue = jsonArr[1] ;
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
	return returnValue ;
}

//---------------------- 集群 --------------------------//

/*
 * 全选节点
 */
function checkAllNode()
{
	$( '#nodeGrid > .grid-body > .grid-tr' ).each(function(index, element) {
      $( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked = true ;
   });
}

/*
 * 反选节点
 */
function inverseCheckAllNode()
{
	$( '#nodeGrid > .grid-body > .grid-tr' ).each(function(index, element) {
      if( $( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked )
		{
			$( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked = false ;
		}
		else
		{
			$( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked = true ;
		}
   });
}

/*
 * 收集分区组信息
 */
function collectGroup()
{
	var group = { 'coord': 0, 'catalog': 0, 'data': {}, 'nodeNum': 0, 'groupNum': 2 } ;
	var config = _businessConfig['Config'] ;
	var len = config.length ;
	for( var i = 0; i < len; ++i )
	{
		if( config[i]['role'] != 'data' )
		{
			++group[ config[i]['role'] ] ;
		}
		else
		{
			if ( group['data'][ config[i]['datagroupname'] ] == undefined )
			{
				group['data'][ config[i]['datagroupname'] ] = 0 ;
				++group[ 'groupNum' ] ;
			}
			++group['data'][ config[i]['datagroupname'] ] ;
		}
		++group[ 'nodeNum' ] ;
	}
	return group ;
}

/*
 * 根据条件搜索节点表格
 */
function searchNodeGrid()
{
	var value = [] ;
	$( '#nodeGrid > .grid-title > .grid-tr:last > .grid-th > .form-control' ).each(function(index, element) {
      value.push( $.trim( $( this ).val() ) ) ;
   });
	
	if( value[0] == '' && value[1] == '' && value[2] == '' && value[3] == '全部' && value[4] == '全部' )
	{
		$( '#nodeGrid > .grid-body > .grid-tr' ).each(function(index, element) {
         $( this ).show() ;
      });
	}
	else
	{
		$( '#nodeGrid > .grid-body > .grid-tr' ).each(function(index, element) {
			var tds = [] ;
			var isFirst = true ;
         $( this ).children( '.grid-td' ).each(function(index, element) {
				if( isFirst )
				{
					isFirst = false ;
				}
				else
				{
            	tds.push( $( this ).text() )
				}
         });
			if ( tds[0].indexOf( value[0] ) == -1 ||
			     tds[1].indexOf( value[1] ) == -1 ||
				  tds[2].indexOf( value[2] ) == -1 ||
				  ( value[3] != '全部' && tds[3] != value[3] ) ||
				  ( value[4] != '全部' && tds[4] != value[4] ) )
			{
				$( this ).hide() ;
			}
			else
			{
				$( this ).show() ;
			}
      });
	}
}


/*
 * 设置选择的分区组
 */
function checkedGroup( role, groupName, liDivObj )
{
	var len = $( '#groupSwitchList' ).children().length ;
	for( var i = 0; i < len; ++i )
	{
		sdbjs.parts.tabList.unActive( 'groupSwitchList', i ) ;
	}
	sdbjs.parts.tabList.active( 'groupSwitchList', $( liDivObj ).parent().index() ) ;
	
	var role_select_obj = $( '#roleSelect' ).get(0) ;
	var group_select_obj = $( '#groupSelect' ).get(0) ;
	var roleCheckedStr = '' ;
	var groupCheckedStr = '' ;
	if( role == '' && groupName == '' )
	{
		roleCheckedStr = '全部' ;
		groupCheckedStr = '全部' ;
	}
	else if( role == 'coord' )
	{
		roleCheckedStr = 'coord' ;
		groupCheckedStr = '全部' ;
	}
	else if( role == 'catalog' )
	{
		roleCheckedStr = 'catalog' ;
		groupCheckedStr = '全部' ;
	}
	else if( role == '' )
	{
		roleCheckedStr = '全部' ;
		groupCheckedStr = groupName ;
	}
	if( roleCheckedStr != '' )
	{
		var len = role_select_obj.options.length ;
		for( var i = 0; i < len; ++i )
		{
			if( role_select_obj.options[i].value == roleCheckedStr )
			{
				role_select_obj.options[i].selected = true;
				break;
			}
		}
	}
	if( groupCheckedStr != '' )
	{
		var len = group_select_obj.options.length ;
		for( var i = 0; i < len; ++i )
		{
			if( group_select_obj.options[i].value == groupCheckedStr )
			{
				group_select_obj.options[i].selected = true;
				break;
			}
		}
	}
	searchNodeGrid() ;
}

/*
 * 显示或隐藏高级选项
 */
function switchAdvaOptions( buttonObj )
{
	if( $( buttonObj ).text() == '高级选项' )
	{
		$( buttonObj ).text( '隐藏高级选项' ) ;
		$( '#nodeConfigGrid > .grid-body > .grid-tr' ).each(function(index, element) {
			$( this ).show() ;
		});
	}
	else
	{
		$( buttonObj ).text( '高级选项' ) ;
		$( '#nodeConfigGrid > .grid-body > .grid-tr' ).each(function(index, element) {
			if( _configProperty[ index ]['Level'] != 0 )
			{
				$( this ).hide() ;
			}
		});
	}
	sdbjs.fun.moveModal( 'nodeConfigModal' ) ;
}

/*
 * 保存节点配置
 * 参数1 第几个节点，如果 < 0 ， 则作为批量保存
 */
function saveNodeConfig( line )
{
	//恢复警告颜色
	$( '#nodeConfigGrid > .grid-body > .grid-tr' ).each(function(index, element) {
		if ( index % 2 != 0 )
		{
			$( this ).children( '.grid-td' ).css( { 'backgroundColor': '#F5F5F5' } ) ;
		}
		else
		{
			$( this ).children( '.grid-td' ).css( { 'backgroundColor': '#FFF' } ) ;
		}
	});

	//获取配置输入框的值
	var valueList = [] ;
	$( '#nodeConfigGrid > .grid-body > .grid-tr > .grid-td > .form-control' ).each(function(index, element) {
		valueList.push( $( this ).val() ) ;
	});
		
	//单节点配置保存
	if( line >= 0 )
	{
		//检测输入值
		var configLen = _configProperty.length ;
		var returnValue ;
		for( var i = 0; i < configLen; ++i )
		{
			if( _configProperty[i]['Display'] == 'edit box' )
			{
				valueList[i] = $.trim( valueList[i] ) ;
			}
			returnValue = checkInputValue( _configProperty[i]['Display'], _configProperty[i]['Type'], _configProperty[i]['Valid'], _configProperty[i]['WebName'], valueList[i] ) ;
			if( returnValue[0] == false )
			{
				$( '#nodeConfigGrid > .grid-body > .grid-tr' ).eq( i ).children( '.grid-td' ).css( { 'background-color': '#f2dede' } ) ;
				var distance = $( '#nodeConfigGrid > .grid-body > .grid-tr' ).eq( i ).offset().top - ( $( '#nodeConfigGrid' ).offset().top + $( '#nodeConfigGrid > .grid-title' ).height() ) ;
				var scrollHeight = $( '#nodeConfigGrid' ).scrollTop() ;
				$( '#nodeConfigGrid' ).animate( { 'scrollTop': ( scrollHeight + distance ) } ) ;
				break;
			}
		}
		if( returnValue[0] == false )
		{
			$( '#configError' ).text( returnValue[1] ) ;
		}
		else
		{
			//保存
			var valueListLen = valueList.length ;
			for( var i = 0; i < valueListLen; ++i )
			{
				_businessConfig['Config'][line][ _configProperty[i]['Name'] ] = valueList[i] ;
			}
			$( '#nodeGrid > .grid-body > .grid-tr' ).eq( line ).children( '.grid-td' ).eq(2).text( _businessConfig['Config'][line]['svcname'] ) ;
			$( '#nodeGrid > .grid-body > .grid-tr' ).eq( line ).children( '.grid-td' ).eq(3).text( _businessConfig['Config'][line]['dbpath'] ) ;
			sdbjs.fun.closeModal( 'nodeConfigModal' ) ;
			sdbjs.fun.gridRevise( 'nodeGrid' ) ;
		}
	}
	else
	{
		//检测输入值
		var configLen = _configProperty.length ;
		var returnValue ;
		for( var i = 0; i < configLen; ++i )
		{
			if( _configProperty[i]['Display'] == 'edit box' )
			{
				valueList[i] = $.trim( valueList[i] ) ;
			}
			//数据路径和服务名不做检查
			if( _configProperty[i]['Name'] != 'dbpath' && _configProperty[i]['Name'] != 'svcname' )
			{
				returnValue = checkInputValue( _configProperty[i]['Display'], _configProperty[i]['Type'], _configProperty[i]['Valid'], _configProperty[i]['WebName'], valueList[i] ) ;
				if( returnValue[0] == false )
				{
					$( '#nodeConfigGrid > .grid-body > .grid-tr' ).eq( i ).children( '.grid-td' ).css( { 'background-color': '#f2dede' } ) ;
					var distance = $( '#nodeConfigGrid > .grid-body > .grid-tr' ).eq( i ).offset().top - ( $( '#nodeConfigGrid' ).offset().top + $( '#nodeConfigGrid > .grid-title' ).height() ) ;
					var scrollHeight = $( '#nodeConfigGrid' ).scrollTop() ;
					$( '#nodeConfigGrid' ).animate( { 'scrollTop': ( scrollHeight + distance ) } ) ;
					break;
				}
			}
		}
		
		if( returnValue[0] == false )
		{
			$( '#configError' ).text( returnValue[1] ) ;
		}
		else
		{
			//找出打勾的节点
			var nodeList = [] ;
			$( '#nodeGrid > .grid-body > .grid-tr' ).each(function(index, element) {
				nodeList.push( $( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked ) ;
			});
			//保存数据
			var valueListLen = valueList.length ;
			var nodeListLen = nodeList.length ;
			for( var k = 0, l = 0; k < nodeListLen; ++k )
			{
				if( nodeList[k] == true )
				{
					var newValueList = [] ;
					var newPort = '' ;
					newValueList = $.extend( true, [], valueList ) ;
					//检查端口
					for( var i = 0; i < valueListLen; ++i )
					{
						if( _configProperty[i]['Name'] == 'svcname' )
						{
							newValueList[i] = portEscape( valueList[i], l ) ;
							newPort = newValueList[i] ;
							if( newValueList[i] == null )
							{
								$( '#configError' ).html( sdbjs.fun.htmlEncode( _configProperty[i]['WebName'] ) + ' 端口错误，使用方法请点击页面左侧<b>[提示]</b>里面的<b>[帮助]</b>' ) ;
								return ;
							}
							break ;
						}
					}
					//检测数据路径
					for( var i = 0; i < valueListLen; ++i )
					{
						if( _configProperty[i]['Name'] == 'dbpath' )
						{
							newValueList[i] = dbpathEscape( valueList[i],
																	  _businessConfig['Config'][k]['HostName'],
																	  ( newPort == '' ? _businessConfig['Config'][k]['svcname'] : newPort ),
																	  _businessConfig['Config'][k]['role'],
																	  _businessConfig['Config'][k]['datagroupname'] ) ;
							if( newValueList[i] == null )
							{
								$( '#configError' ).html( sdbjs.fun.htmlEncode( _configProperty[i]['WebName'] ) + ' 数据路径错误，使用方法请点击页面左侧<b>[提示]</b>里面的<b>[帮助]</b>' ) ;
								return ;
							}
							break ;
						}
					}
					for( var i = 0; i < valueListLen; ++i )
					{
						if( newValueList[i] != '' )
						{
							_businessConfig['Config'][k][ _configProperty[i]['Name'] ] = newValueList[i] ;
						}
					}
					$( '#nodeGrid > .grid-body > .grid-tr' ).eq( k ).children( '.grid-td' ).eq(2).text( _businessConfig['Config'][k]['svcname'] ) ;
					$( '#nodeGrid > .grid-body > .grid-tr' ).eq( k ).children( '.grid-td' ).eq(3).text( _businessConfig['Config'][k]['dbpath'] ) ;
					++l ;
				}
			}
			sdbjs.fun.closeModal( 'nodeConfigModal' ) ;
			sdbjs.fun.gridRevise( 'nodeGrid' ) ;
		}
	}
}

/*
 * 打开节点配置模态框，并且加载节点配置
 * 参数1 第几个节点，如果 -1 ， 则作为批量打开, 如果 -2, 则用默认配置打开
 */
function openNodeConfigModal( line )
{
	$( '#configError' ).text( '' ) ;
	$( '#nodeConfigGrid > .grid-body > .grid-tr' ).each(function(index, element) {
		if ( index % 2 != 0 )
		{
			$( this ).children( '.grid-td' ).css( { 'backgroundColor': '#F5F5F5' } ) ;
		}
		else
		{
			$( this ).children( '.grid-td' ).css( { 'backgroundColor': '#FFF' } ) ;
		}
	});
	//单节点配置
	if( line >= 0 )
	{
		sdbjs.fun.openModal( 'nodeConfigModal' ) ;
		sdbjs.fun.gridRevise( 'nodeConfigGrid' ) ;
		sdbjs.fun.moveModal( 'nodeConfigModal' ) ;
		$( '#nodeConfigGrid > .grid-body > .grid-tr > .grid-td > .form-control' ).each(function(index, element) {
			loadValue2Input( $( this ), _configProperty[index]['Display'], _businessConfig['Config'][line][ _configProperty[index]['Name'] ] ) ;
		});
		$( '#nodeConfigModal > .modal-title > span' ).text( _businessConfig['Config'][line]['HostName'] + ':' + _businessConfig['Config'][line]['svcname'] + '的配置' ) ;
		$( '#saveConfigButton' ).get(0).onclick = Function( 'saveNodeConfig(' + line + ')' ) ;
	}
	//批量配置
	else if( line == -1 )
	{
		var firstNodeNum = -1 ;
		var nodeList = [] ;
		var propertyList = [] ;
		//找出打勾的节点
		$( '#nodeGrid > .grid-body > .grid-tr' ).each(function(index, element) {
         nodeList.push( $( this ).children( '.grid-td:first' ).children( 'input' ).get(0).checked ) ;
      });
		//找出相同的参数
		var hostLen = _businessConfig['Config'].length ;
		var propertyLen = _configProperty.length ;
		//循环参数
		for( var k = 0; k < propertyLen; ++k )
		{
			var tempProperty = null ;
			propertyList.push( true ) ;
			//循环节点
			for( var i = 0; i < hostLen; ++i )
			{
				//找出打勾的节点
				if( nodeList[i] == true )
				{
					//记录第一个打勾的节点下标
					if( firstNodeNum == -1 )
					{
						firstNodeNum = i ;
					}
					//取得第一个节点参数的值
					if( tempProperty == null )
					{
						tempProperty = _businessConfig['Config'][i][ _configProperty[k]['Name'] ] ;
					}
					else
					{
						//对比每一个节点的值，如果不一样，则给false
						if( tempProperty != _businessConfig['Config'][i][ _configProperty[k]['Name'] ] )
						{
							propertyList[ propertyList.length - 1 ] = false ;
							break ;
						}
					}
				}
			}
		}
		if( firstNodeNum == -1 )
		{
			//没有一个打勾的节点，报警告
			showInfoFromFoot( 'warning', '至少请选择一个节点！' ) ;
		}
		else
		{
			sdbjs.fun.openModal( 'nodeConfigModal' ) ;
			sdbjs.fun.gridRevise( 'nodeConfigGrid' ) ;
			sdbjs.fun.moveModal( 'nodeConfigModal' ) ;
			//开始加载相同值的参数
			$( '#nodeConfigGrid > .grid-body > .grid-tr > .grid-td > .form-control' ).each(function(index, element) {
				if( propertyList[ index ] == true )
				{
					loadValue2Input( $( this ), _configProperty[index]['Display'], _businessConfig['Config'][ firstNodeNum ][ _configProperty[index]['Name'] ] ) ;
				}
				else
				{
					if ( _configProperty[index]['Display'] == 'select box' )
					{
						loadValue2Input( $( this ), _configProperty[index]['Display'], _configProperty[index]['Default'] ) ;
					}
					else
					{
						loadValue2Input( $( this ), _configProperty[index]['Display'], '' ) ;
					}
				}
			});
			$( '#nodeConfigModal > .modal-title > span' ).text( '批量修改配置' ) ;
			$( '#saveConfigButton' ).get(0).onclick = Function( 'saveNodeConfig(' + line + ')' ) ;
		}
	}
	//默认配置
	else if( line == -2 )
	{
		sdbjs.fun.openModal( 'nodeConfigModal' ) ;
		sdbjs.fun.gridRevise( 'nodeConfigGrid' ) ;
		sdbjs.fun.moveModal( 'nodeConfigModal' ) ;
		$( '#nodeConfigGrid > .grid-body > .grid-tr > .grid-td > .form-control' ).each(function(index, element) {
			loadValue2Input( $( this ), _configProperty[index]['Display'], _configProperty[index]['Default'] ) ;
		});
	}
}

/*
 * 打开添加分区组模态框
 */
function openAddGroupModal()
{
	sdbjs.fun.openModal( 'addGroupModal' ) ;
	sdbjs.fun.moveModal( 'addGroupModal' ) ;
	$( '#addGroupError' ).text( '' ) ;
	$( '#addGroupModal > .modal-body .form-control' ).val( '' ) ;
}

/*
 * 添加分区组
 */
function addGroup()
{
	var groupName = $.trim( $( '#addGroupModal > .modal-body .form-control' ).val() ) ;
	if ( sdbjs.fun.checkStrName( groupName ) )
	{
		var isExists = false ;
		var i = 0 ;
		$( '#groupSwitchList > li' ).each(function(index, element) {
			if( i > 2 )
			{
         	if ( groupName == $( this ).children( 'div' ).eq(1).children( 'div:first' ).text() )
				{
					isExists = true ;
					$( '#addGroupError' ).text( '分区组已经存在。' ) ;
				}
			}
			++i ;
      });
		if( isExists == false )
		{
			var groupNum = $( '#groupSwitchList > li' ).length ;
			sdbjs.parts.tabList.add( 'groupSwitchList', [ { 'name': sdbjs.fun.htmlEncode( groupName ), 'sname': '节点数：0', 'text': '', 'event': 'checkedGroup("","' + groupName + '",this)' } ] ) ;
			sdbjs.parts.dropDownBox.create( 'dataDropDown' + groupNum, {}, '', [ { 'text': '添加节点', 'event': 'openAddNodeModal("data","' + groupName + '",' + groupNum + ')' }, { 'text': '删除节点', 'event': 'openRemoveNodeModal("data","' + groupName + '",' + groupNum + ')' }, { 'text': '' }, { 'text': '删除分区组', 'event': 'openRemoveGroupModal("' + groupName + '")' } ], 'lg' ).appendTo( $( '#groupSwitchList > li:last > div' ).eq(0) ) ;
			sdbjs.fun.addOnEvent( $( '#dataDropDown' + groupNum + ' > button' ) ) ;
			sdbjs.fun.addOnEvent( $( '#dataDropDown' + groupNum + '_menu' ) ) ;
			sdbjs.parts.selectBox.add( 'groupSelect', [ { 'key': sdbjs.fun.htmlEncode( groupName ), 'value': groupName } ] ) ;
			sdbjs.fun.closeModal( 'addGroupModal' ) ;
			$( '#dataDropDown' + groupNum + '_menu > li:eq(1)' ).removeClass().addClass( 'drop-disabled' ) ;
		}
	}
	else
	{
		$( '#addGroupError' ).text( '分区组名必须以下划线或英文字母开头，只含有下划线英文字母数字，长度在 1 - 255 范围。' ) ;
	}
}

/*
 * 打开删除分区组模态框
 */
function openRemoveGroupModal( groupName )
{
	$( '#removeGroupModal' ).data( 'groupName', groupName ) ;
	sdbjs.fun.openModal( 'removeGroupModal' ) ;
	sdbjs.fun.moveModal( 'removeGroupModal' ) ;
}

/*
 * 删除分区组
 */
function removeGroup()
{
	//删除左边列表
	var groupName = $( '#removeGroupModal' ).data( 'groupName' ) ;
	$( '#groupSwitchList > li' ).each(function(index, element) {
      if( $( this ).children( 'div:eq(1)' ).children( 'div:first' ).text() == groupName )
		{
			$( this ).remove() ;
			return;
		}
   });
	//删除下拉菜单
	$( '#groupSelect > option' ).each(function(index, element) {
      if( $( this ).text() == groupName )
		{
			$( this ).remove() ;
			return;
		}
   });
	//删除表格
	$( '#nodeGrid > .grid-body > .grid-tr' ).each(function(index, element) {
      if( $( this ).children( '.grid-td' ).eq(5).text() == groupName )
		{
			$( this ).remove() ;
		}
   });
	//删除后台数据
	while( true )
	{
		var hasGroup = false ;
		var len = _businessConfig['Config'].length ;
		for( var i = 0; i < len; ++i )
		{
			if( _businessConfig['Config'][i]['datagroupname'] == groupName )
			{
				hasGroup = true ;
				_businessConfig['Config'].splice( i, 1 ) ;
				break ;
			}
		}
		if( hasGroup == false )
		{
			break ;
		}
	}
	sdbjs.fun.closeModal( 'removeGroupModal' ) ;
}

/*
 * 添加节点
 */
function addNode()
{
	var hostName = _hostList[ $( '#addHostSelect' ).val() ] ;
	var role = $( '#addNodeModal' ).data( 'role' ) ;
	var groupName = $( '#addNodeModal' ).data( 'groupName' ) ;
	var index = $( '#addNodeModal' ).data( 'listNum' ) ;
	
	//恢复警告颜色
	$( '#nodeConfigGrid > .grid-body > .grid-tr' ).each(function(index, element) {
		if ( index % 2 != 0 )
		{
			$( this ).children( '.grid-td' ).css( { 'backgroundColor': '#F5F5F5' } ) ;
		}
		else
		{
			$( this ).children( '.grid-td' ).css( { 'backgroundColor': '#FFF' } ) ;
		}
	});

	//获取配置输入框的值
	var valueList = [] ;
	$( '#nodeConfigGrid > .grid-body > .grid-tr > .grid-td > .form-control' ).each(function(index, element) {
		valueList.push( $( this ).val() ) ;
	});
		
	//检测输入值
	var configLen = _configProperty.length ;
	var returnValue ;
	for( var i = 0; i < configLen; ++i )
	{
		if( _configProperty[i]['Display'] == 'edit box' )
		{
			valueList[i] = $.trim( valueList[i] ) ;
		}
		returnValue = checkInputValue( _configProperty[i]['Display'], _configProperty[i]['Type'], _configProperty[i]['Valid'], _configProperty[i]['WebName'], valueList[i] ) ;
		if( returnValue[0] == false )
		{
			$( '#nodeConfigGrid > .grid-body > .grid-tr' ).eq( i ).children( '.grid-td' ).css( { 'background-color': '#f2dede' } ) ;
			var distance = $( '#nodeConfigGrid > .grid-body > .grid-tr' ).eq( i ).offset().top - ( $( '#nodeConfigGrid' ).offset().top + $( '#nodeConfigGrid > .grid-title' ).height() ) ;
			var scrollHeight = $( '#nodeConfigGrid' ).scrollTop() ;
			$( '#nodeConfigGrid' ).animate( { 'scrollTop': ( scrollHeight + distance ) } ) ;
			break;
		}
	}
	if( returnValue[0] == false )
	{
		$( '#configError' ).text( returnValue[1] ) ;
	}
	else
	{
		//保存
		var newNodeConfig = {} ;
		var valueListLen = valueList.length ;
		for( var i = 0; i < valueListLen; ++i )
		{
			newNodeConfig[ _configProperty[i]['Name'] ] = valueList[i] ;
		}
		newNodeConfig['HostName'] = hostName ;
		newNodeConfig['role'] = role ;
		newNodeConfig['datagroupname'] = groupName ;
		_businessConfig['Config'].push( newNodeConfig ) ;
		var line = _businessConfig['Config'].length - 1 ;
		sdbjs.parts.gridBox.add( 'nodeGrid', { 'cell': [ { 'text': '<input type="checkbox">' },
																		 { 'text': sdbjs.fun.htmlEncode( newNodeConfig['HostName'] ), 'event': 'openNodeConfigModal(' + line + ')' },
																		 { 'text': sdbjs.fun.htmlEncode( newNodeConfig['svcname'] ), 'event': 'openNodeConfigModal(' + line + ')' },
																		 { 'text': sdbjs.fun.htmlEncode( newNodeConfig['dbpath'] ), 'event': 'openNodeConfigModal(' + line + ')' },
																		 { 'text': sdbjs.fun.htmlEncode( newNodeConfig['role'] ), 'event': 'openNodeConfigModal(' + line + ')' },
																		 { 'text': sdbjs.fun.htmlEncode( newNodeConfig['datagroupname'] ), 'event': 'openNodeConfigModal(' + line + ')' } ] } ) ;
		sdbjs.fun.closeModal( 'nodeConfigModal' ) ;
		sdbjs.fun.gridRevise( 'nodeGrid' ) ;
		var nodeSum = 0 ;
		for( var i = 0; i <= line; ++i )
		{
			if( _businessConfig['Config'][i]['role'] == role && _businessConfig['Config'][i]['datagroupname'] == groupName )
			{
				++nodeSum ;
			}
		}
		if( nodeSum > 0 )
		{
			var htmlId = $( '#groupSwitchList > li:eq(' + index + ') > div:eq(0) > div:eq(0)' ).attr( 'id' ) + '_menu' ;
			$( '#' + htmlId + ' > li:eq(1)' ).removeClass().addClass( 'event' ) ;
		}
		$( '#groupSwitchList > li:first > div:eq(1) > div:eq(1)' ).text( '总节点数：' + ( line + 1 ) ) ;
		$( '#groupSwitchList > li:eq(' + index + ') > div:eq(1) > div:eq(1)' ).text( '节点数：' + nodeSum ) ;
		if( ( nodeSum == 7 && role != 'coord' ) || ( role == 'coord' && nodeSum == 993 ) )
		{
			var htmlId = $( '#groupSwitchList > li:eq(' + index + ') > div:eq(0) > div:eq(0)' ).attr( 'id' ) + '_menu' ;
			$( '#' + htmlId + ' > li:eq(0)' ).removeClass().addClass( 'drop-disabled' ) ;
		}
	}
}

/*
 * 添加节点配置
 */
function addNodeConfig()
{
	sdbjs.fun.closeModal( 'addNodeModal' ) ;
	var selectObj = $( '#addNodeModal > .modal-body > table select:eq(1)' ).get(0) ;
	if( selectObj.selectedIndex == 0 )
	{
		openNodeConfigModal( -2 ) ;
	}
	else
	{
		openNodeConfigModal( $( '#addNodeSelect' ).val() ) ;
	}
	$( '#nodeConfigModal > .modal-title > span' ).text( '添加新节点的配置' ) ;
	$( '#saveConfigButton' ).get(0).onclick = Function( 'addNode()' ) ;
}

/*
 * 添加节点模态框的配置方式
 */
function addNodeConfigModel()
{
	var selectObj = $( '#addNodeModal > .modal-body > table select:eq(1)' ).get(0) ;
	if( selectObj.selectedIndex == 0 )
	{
		$( '#addNodeModal > .modal-body > table tr:eq(2)' ).hide() ;
	}
	else
	{
		$( '#addNodeModal > .modal-body > table tr:eq(2)' ).show() ;
	}
}

/*
 * 打开添加节点模态框
 */
function openAddNodeModal( role, groupName, index )
{
	var nodeSum = 0 ;
	var nodeLen = _businessConfig['Config'].length ;
	for( var i = 0; i < nodeLen; ++i )
	{
		if( _businessConfig['Config'][i]['role'] == role && _businessConfig['Config'][i]['datagroupname'] == groupName )
		{
			++nodeSum ;
		}
	}
	if( ( nodeSum == 7 && role != 'coord' ) || ( nodeSum == 993 && role == 'coord' ) )
	{
		return;
	}
	$( '#addHostSelect' ).children().remove() ;
	$( '#addNodeSelect' ).children().remove() ;
	$( '#addNodeModal > .modal-body > table tr:eq(2)' ).hide() ;
	$( '#addNodeModal > .modal-body > table select:eq(1)' ).get(0).options[0].selected = true ;
	var len = _hostList.length ;
	for( var i = 0; i < len; ++i )
	{
		sdbjs.parts.selectBox.add( 'addHostSelect', [ { 'key': sdbjs.fun.htmlEncode( _hostList[i] ), 'value': i } ] ) ;
	}
	len = _businessConfig['Config'].length ;
	for( var i = 0; i < len; ++i )
	{
		sdbjs.parts.selectBox.add( 'addNodeSelect', [ { 'key': sdbjs.fun.htmlEncode( _businessConfig['Config'][i]['HostName'] + ':' + _businessConfig['Config'][i]['svcname'] ), 'value': i } ] ) ;
	}
	$( '#addNodeModal' ).data( 'role', role ) ;
	$( '#addNodeModal' ).data( 'groupName', groupName ) ;
	$( '#addNodeModal' ).data( 'listNum', index ) ;
	sdbjs.fun.openModal( 'addNodeModal' ) ;
	sdbjs.fun.moveModal( 'addNodeModal' ) ;
}

/*
 * 打开删除节点模态框
 */
function openRemoveNodeModal( role, groupName, index )
{
	$( '#removeNodeSelect' ).children().remove() ;
	var nodeSum = 0 ;
	var len = _businessConfig['Config'].length ;
	for( var i = 0; i < len; ++i )
	{
		if( _businessConfig['Config'][i]['role'] == role && _businessConfig['Config'][i]['datagroupname'] == groupName )
		{
			sdbjs.parts.selectBox.add( 'removeNodeSelect', [ { 'key': sdbjs.fun.htmlEncode( _businessConfig['Config'][i]['HostName'] + ':' + _businessConfig['Config'][i]['svcname'] ), 'value': i } ] ) ;
			++nodeSum ;
		}
	}
	if( nodeSum == 0 )
	{
		return;
	}
	$( '#removeNodeModal' ).data( 'role', role ) ;
	$( '#removeNodeModal' ).data( 'groupName', groupName ) ;
	$( '#removeNodeModal' ).data( 'listNum', index ) ;
	sdbjs.fun.openModal( 'removeNodeModal' ) ;
	sdbjs.fun.moveModal( 'removeNodeModal' ) ;
}

/*
 * 删除节点
 */
function removeNode()
{
	$( '#nodeGrid > .grid-body > .grid-tr' ).eq( $( '#removeNodeSelect' ).val() ).remove() ;
	_businessConfig['Config'].splice( $( '#removeNodeSelect' ).val(), 1 ) ;
	var role = $( '#removeNodeModal' ).data( 'role' ) ;
	var groupName = $( '#removeNodeModal' ).data( 'groupName' ) ;
	var index = $( '#removeNodeModal' ).data( 'listNum' ) ;
	var nodeLen = _businessConfig['Config'].length ;
	var nodeSum = 0 ;
	for( var i = 0; i < nodeLen; ++i )
	{
		if( _businessConfig['Config'][i]['role'] == role && _businessConfig['Config'][i]['datagroupname'] == groupName )
		{
			++nodeSum ;
		}
	}
	if( nodeSum == 0 )
	{
		var htmlId = $( '#groupSwitchList > li:eq(' + index + ') > div:eq(0) > div:eq(0)' ).attr( 'id' ) + '_menu' ;
		$( '#' + htmlId + ' > li:eq(1)' ).removeClass().addClass( 'drop-disabled' ) ;
	}
	$( '#groupSwitchList > li:first > div:eq(1) > div:eq(1)' ).text( '总节点数：' + nodeLen ) ;
	$( '#groupSwitchList > li:eq(' + index + ') > div:eq(1) > div:eq(1)' ).text( '节点数：' + nodeSum ) ;
	if( ( role != 'coord' && nodeSum < 7 ) || ( role == 'coord' && nodeSum < 993 ) )
	{
		var htmlId = $( '#groupSwitchList > li:eq(' + index + ') > div:eq(0) > div:eq(0)' ).attr( 'id' ) + '_menu' ;
		$( '#' + htmlId + ' > li:eq(0)' ).removeClass().addClass( 'event' ) ;
	}
	sdbjs.fun.closeModal( 'removeNodeModal' ) ;
}


/*
 * 获取主机列表
 */
function getHostList()
{
	var clusterName = $.cookie( 'SdbClusterName' ) ;
	var order = { 'cmd': 'list host', 'ClusterName': clusterName } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		var len = jsonArr.length ;
		for( var i = 1; i < len; ++i )
		{
			_hostList.push( jsonArr[i]['HostName'] ) ;
		}
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 开始创建业务
 */
function addBusiness()
{
	var businessConf = JSON.stringify( _businessConfig ) ;
	var order = { 'cmd': 'add business', 'ConfigInfo': businessConf } ;
	ajaxSendMsg( order, false, function( jsonArr ){
		$.cookie( 'SdbTaskID', jsonArr[1]['TaskID'] ) ;
		var SdbGuideOrder = $.cookie( 'SdbGuideOrder' ) ;
		if( SdbGuideOrder == 'Deployment' )
		{
			gotoPage( __Deployment[3] ) ;
		}
		else if( SdbGuideOrder == 'AddBusiness' )
		{
			gotoPage( __AddBuiness[2] ) ;
		}
		else
		{
			gotoPage( "index.html" ) ;
		}
	}, function( jsonArr ){
		return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
	} ) ;
}

/*
 * 创建节点列表
 */
function nodeStartCreate()
{
	var nodeLen = _businessConfig['Config'].length ;
	var i = 0 ;
	sdbjs.fun.openLoading( 'loading' ) ;
	function nodeStartCreateT()
	{
		sdbjs.parts.gridBox.add( 'nodeGrid', { 'cell': [ { 'text': '<input type="checkbox">' },
																		 { 'text': sdbjs.fun.htmlEncode( _businessConfig['Config'][i]['HostName'] ), 'event': 'openNodeConfigModal(' + i + ')' },
																		 { 'text': sdbjs.fun.htmlEncode( _businessConfig['Config'][i]['svcname'] ), 'event': 'openNodeConfigModal(' + i + ')' },
																		 { 'text': sdbjs.fun.htmlEncode( _businessConfig['Config'][i]['dbpath'] ), 'event': 'openNodeConfigModal(' + i + ')' },
																		 { 'text': sdbjs.fun.htmlEncode( _businessConfig['Config'][i]['role'] ), 'event': 'openNodeConfigModal(' + i + ')' },
																		 { 'text': sdbjs.fun.htmlEncode( _businessConfig['Config'][i]['datagroupname'] ), 'event': 'openNodeConfigModal(' + i + ')' } ] } ) ;
		++i ;
		sdbjs.fun.setLoading( 'loading', parseInt(i/nodeLen*100) ) ;
		if( i < nodeLen )
		{
			setTimeout( nodeStartCreateT, 1 ) ;
		}
		else
		{
			sdbjs.fun.closeLoading( 'loading' ) ;
		}
	}
	setTimeout( nodeStartCreateT, 0 ) ;
}

//------------------------- 单节点 -------------------------------------

/*
 * 显示或隐藏高级选项
 */
function switchAdvaOptionsStandalone( buttonObj )
{
	if( $( buttonObj ).text() == '高级选项' )
	{
		$( buttonObj ).text( '隐藏高级选项' ) ;
		$( '#nodeConfigGrid > .grid-body > .grid-tr' ).each(function(index, element) {
			$( this ).show() ;
		});
	}
	else
	{
		$( buttonObj ).text( '高级选项' ) ;
		$( '#nodeConfigGrid > .grid-body > .grid-tr' ).each(function(index, element) {
			if( index != 0 && _configProperty[ index ]['Level'] != 0 )
			{
				$( this ).hide() ;
			}
		});
	}
}

function addStandalone()
{
	//获取配置输入框的值
	var valueList = [] ;
	$( '#nodeConfigGrid > .grid-body > .grid-tr > .grid-td > .form-control' ).each(function(index, element) {
		valueList.push( $( this ).val() ) ;
	});

	//检测输入值
	var configLen = _configProperty.length ;
	var returnValue ;
	for( var i = 0; i < configLen; ++i )
	{
		if( _configProperty[i]['Display'] == 'edit box' )
		{
			valueList[ i + 1 ] = $.trim( valueList[ i + 1 ] ) ;
		}
		returnValue = checkInputValue( _configProperty[i]['Display'], _configProperty[i]['Type'], _configProperty[i]['Valid'], _configProperty[i]['WebName'], valueList[ i + 1 ] ) ;
		if( returnValue[0] == false )
		{
			$( '#nodeConfigGrid > .grid-body > .grid-tr' ).eq( i + 1 ).children( '.grid-td' ).css( { 'background-color': '#f2dede' } ) ;
			var distance = $( '#nodeConfigGrid > .grid-body > .grid-tr' ).eq( i + 1 ).offset().top - ( $( '#nodeConfigGrid' ).offset().top + $( '#nodeConfigGrid > .grid-title' ).height() ) ;
			var scrollHeight = $( '#nodeConfigGrid' ).scrollTop() ;
			$( '#nodeConfigGrid' ).animate( { 'scrollTop': ( scrollHeight + distance ) } ) ;
			break;
		}
	}
	if( returnValue[0] == false )
	{
		showInfoFromFoot( 'danger', returnValue[1] ) ;
	}
	else
	{
		//保存
		_businessConfig['Config'][0]['HostName'] = valueList[0] ;
		for( var i = 0; i < configLen; ++i )
		{
			_businessConfig['Config'][0][ _configProperty[i]['Name'] ] = valueList[ i + 1 ] ;
		}
		
		var order = { 'cmd': 'add business', 'ConfigInfo': JSON.stringify( _businessConfig ) } ;
		ajaxSendMsg( order, false, function( jsonArr ){
			$.cookie( 'SdbTaskID', jsonArr[1]['TaskID'] ) ;
			var SdbGuideOrder = $.cookie( 'SdbGuideOrder' ) ;
			if( SdbGuideOrder == 'Deployment' )
			{
				gotoPage( __Deployment[3] ) ;
			}
			else if( SdbGuideOrder == 'AddBusiness' )
			{
				gotoPage( __AddBuiness[2] ) ;
			}
			else
			{
				gotoPage( "index.html" ) ;
			}
		}, function( jsonArr ){
			return errorProcess( jsonArr[0]['errno'], jsonArr[0]['detail'] ) ;
		} ) ;
	}
}

//------------------------- 通用 ---------------------------------------

/*
 * 创建业务修改
 */
function createBusiness()
{
	function createDistribution()
	{
		$( '#distributionBox' ).show() ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#distributionBox' ).children( ':first-child' ).children( ':eq(0)' ), 'style': { 'width': 'parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#distributionBox' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - parseInt( sdbjs.public.width / 3 )', 'height': 'sdbjs.public.height - 131' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'leftPanel', 'style': { 'height': 'sdbjs.public.height - 151' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'rightPanel', 'style': { 'height': 'sdbjs.public.height - 151' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'groupSwitchList', 'style': { 'maxHeight': 'sdbjs.public.height - 228' } } ) ;
		
		sdbjs.parts.gridBox.create( 'nodeConfigGrid', {}, [ [ '属性', '值', '描述' ] ], [ 2, 4, 4 ] ).prependTo( $( '#nodeConfigModal > .modal-body' ) ) ;
		var len = _configProperty.length ;
		for( var k = 0; k < len; ++k )
		{
			var data_json = { 'cell': [ { 'text': sdbjs.fun.htmlEncode( _configProperty[k]['WebName'] ) },
												 { 'text': '' },
												 { 'text': sdbjs.fun.htmlEncode( _configProperty[k]['Desc'] ) } ] } ;
			var str = createHtmlInput( _configProperty[k]['Display'], _configProperty[k]['Valid'], '' ) ;
			if( str == '' ){ continue }
			else{ data_json['cell'][1]['text'] = str }
			sdbjs.parts.gridBox.add( 'nodeConfigGrid', data_json ) ;
			if( _configProperty[k]['Level'] != 0 )
			{
				$( '#nodeConfigGrid > .grid-body > .grid-tr' ).last().hide() ;
			}
		}
		
		sdbjs.fun.autoCorrect( { 'id': 'nodeConfigGrid', 'style': { 'maxHeight': 'sdbjs.public.height - 250' } } ) ;
		
		var nodeGridTitle = 	[ [ '', '主机名', '端口', '数据路径', '角色', '分区组' ], [ '', '<input class="form-control" type="text">', '<input class="form-control" type="text">', '<input class="form-control" type="text">', '<select id="roleSelect" class="form-control"><option value="全部" selected="selected">全部</option><option value="coord">coord</option><option value="catalog">catalog</option><option value="data">data</option></select>', '<select id="groupSelect" class="form-control"><option value="全部" selected="selected">全部</option></select>' ] ] ;
		sdbjs.parts.gridBox.create( 'nodeGrid', {}, nodeGridTitle, [ 4, 15, 10, 20, 10, 15 ] ).appendTo( $( '#rightPanel > .panel-body' ) ) ;
		sdbjs.fun.gridRevise( 'nodeGrid' ) ;
		sdbjs.fun.autoCorrect( { 'id': 'nodeGrid', 'style': { 'maxHeight': 'sdbjs.public.height - 227' } } ) ;
		
		$( '#nodeGrid > .grid-title > .grid-tr:last > .grid-th > .form-control' ).each(function(index, element) {
			$( this ).on( 'input propertychange', function(){ searchNodeGrid() } ) ;
		});

		var browser = sdbjs.fun.getBrowserInfo() ;
		if( browser[0] == 'ie' && browser[1] <= 7 )
		{
			$( '#searchGroupList' ).removeAttr( 'style' ) ;
		}
		
		nodeStartCreate() ;
		
		var groupInfo = collectGroup() ;
		sdbjs.parts.tabList.add( 'groupSwitchList', [ { 'name': '业务：' + sdbjs.fun.htmlEncode( _businessConfig['BusinessName'] ), 'sname': '总节点数：' + groupInfo['nodeNum'], 'text': '', 'event': 'checkedGroup("","",this)' } ] ) ;
		sdbjs.parts.dropDownBox.create( 'businessDropDown0', {}, '<img class="icon" src="./images/smallicon/blacks/16x16/list_bullets.png">', [ { 'text': '添加分区组', 'event': 'openAddGroupModal()' } ], 'lg' ).appendTo( $( '#groupSwitchList > li:last > div' ).eq(0) ) ;
		sdbjs.parts.tabList.add( 'groupSwitchList', [ { 'name': '协调组', 'sname': '节点数：' + groupInfo['coord'], 'text': '', 'event': 'checkedGroup("coord","",this)' } ] ) ;
		sdbjs.parts.dropDownBox.create( 'coordDropDown1', {}, '', [ { 'text': '添加节点', 'event': 'openAddNodeModal("coord","",1)' }, { 'text': '删除节点', 'event': 'openRemoveNodeModal("coord","",1)' } ], 'lg' ).appendTo( $( '#groupSwitchList > li:last > div' ).eq(0) ) ;
		if( groupInfo['coord'] == 993 )
		{
			$( '#coordDropDown1_menu > li:eq(0)' ).removeClass().addClass( 'drop-disabled' ) ;
		}
		if( groupInfo['coord'] == 0 )
		{
			$( '#coordDropDown1_menu > li:eq(1)' ).removeClass().addClass( 'drop-disabled' ) ;
		}
		sdbjs.parts.tabList.add( 'groupSwitchList', [ { 'name': '编目组', 'sname': '节点数：' + groupInfo['catalog'], 'text': '', 'event': 'checkedGroup("catalog","",this)' } ] ) ;
		sdbjs.parts.dropDownBox.create( 'catalogDropDown2', {}, '', [ { 'text': '添加节点', 'event': 'openAddNodeModal("catalog","",2)' }, { 'text': '删除节点', 'event': 'openRemoveNodeModal("catalog","",2)' } ], 'lg' ).appendTo( $( '#groupSwitchList > li:last > div' ).eq(0) ) ;
		if( groupInfo['catalog'] == 7 )
		{
			$( '#catalogDropDown2_menu > li:eq(0)' ).removeClass().addClass( 'drop-disabled' ) ;
		}
		if( groupInfo['catalog'] == 0 )
		{
			$( '#catalogDropDown2_menu > li:eq(1)' ).removeClass().addClass( 'drop-disabled' ) ;
		}
		var k = 3 ;
		for( var key in groupInfo['data'] )
		{
			sdbjs.parts.selectBox.add( 'groupSelect', [ { 'key': sdbjs.fun.htmlEncode( key ), 'value': key } ] ) ;
			sdbjs.parts.tabList.add( 'groupSwitchList', [ { 'name': sdbjs.fun.htmlEncode( key ), 'sname': '节点数：' + groupInfo['data'][key], 'text': '', 'event': 'checkedGroup("","' + key + '",this)' } ] ) ;
			sdbjs.parts.dropDownBox.create( 'dataDropDown' + k, {}, '', [ { 'text': '添加节点', 'event': 'openAddNodeModal("data","' + key + '",' + k + ')' }, { 'text': '删除节点', 'event': 'openRemoveNodeModal("data","' + key + '",' + k + ')' }, { 'text': '' }, { 'text': '删除分区组', 'event': 'openRemoveGroupModal("' + key + '")' } ], 'lg' ).appendTo( $( '#groupSwitchList > li:last > div' ).eq(0) ) ;
			if( groupInfo['data'][key] == 7 )
			{
				$( '#' + 'dataDropDown' + k + '_menu > li:eq(0)' ).removeClass().addClass( 'drop-disabled' ) ;
			}
			if( groupInfo['data'][key] == 0 )
			{
				$( '#' + 'dataDropDown' + k + '_menu > li:eq(1)' ).removeClass().addClass( 'drop-disabled' ) ;
			}
			++k ;
		}
		
		$( '#addNodeModal > .modal-body > table select:eq(1)' ).on( 'input propertychange', function(){ addNodeConfigModel() } ) ;
		
		sdbjs.parts.tabList.active( 'groupSwitchList', 0 ) ;
	}
	
	function createStandalone()
	{
		$( '#standaloneBox' ).show() ;
		sdbjs.fun.autoCorrect( { 'obj': $( '#standaloneBox > .modal' ), 'style': { 'height': 'sdbjs.public.height - 155', 'width': 'parseInt(sdbjs.public.width * 0.9)' } } ) ;
		sdbjs.fun.autoCorrect( { 'id': 'standaloneBox', 'style': { 'maxHeight': 'sdbjs.public.height - 230' } } ) ;
		sdbjs.parts.gridBox.create( 'nodeConfigGrid', {}, [ [ '属性', '值', '描述' ] ], [ 2, 4, 4 ] ).prependTo( $( '#standaloneBox > .modal > .modal-body' ) ) ;
		var hostStrList = '' ;
		var hostLen = _hostList.length ;
		for( var i = 0; i < hostLen; ++i )
		{
			if( i == 0 )
			{
				hostStrList += _hostList[i] ;
			}
			else
			{
				hostStrList += ',' + _hostList[i] ;
			}
		}
		var data_json = { 'cell': [ { 'text': sdbjs.fun.htmlEncode( '主机名' ) },
											 { 'text': '<input type="text" class="form-control" disabled="disabled" value="' + _businessConfig['Config'][0]['HostName'] + '" />' },//createHtmlInput( 'select box', hostStrList, _businessConfig['Config'][0]['HostName'] ) },
											 { 'text': sdbjs.fun.htmlEncode( '选择主机' ) } ] } ;
		sdbjs.parts.gridBox.add( 'nodeConfigGrid', data_json ) ;
		var len = _configProperty.length ;
		for( var k = 0; k < len; ++k )
		{
			var data_json = { 'cell': [ { 'text': sdbjs.fun.htmlEncode( _configProperty[k]['WebName'] ) },
												 { 'text': '' },
												 { 'text': sdbjs.fun.htmlEncode( _configProperty[k]['Desc'] ) } ] } ;
			var str = createHtmlInput( _configProperty[k]['Display'], _configProperty[k]['Valid'], _businessConfig['Config'][0][ _configProperty[k]['Name'] ] ) ;
			if( str == '' ){ continue }
			else{ data_json['cell'][1]['text'] = str }
			sdbjs.parts.gridBox.add( 'nodeConfigGrid', data_json ) ;
			if( _configProperty[k]['Level'] != 0 )
			{
				$( '#nodeConfigGrid > .grid-body > .grid-tr' ).last().hide() ;
			}
		}
		sdbjs.fun.autoCorrect( { 'id': 'nodeConfigGrid', 'style': { 'maxHeight': 'sdbjs.public.height - 230' } } ) ;
		sdbjs.fun.gridRevise( 'nodeConfigGrid' ) ;
		$( '#__goOn' ).get(0).onclick = Function( 'addStandalone()' ) ;
	}
	
	var tempBusinessConfig = getBusinessConfig() ;
	if( tempBusinessConfig != null )
	{
		//解析业务配置信息
		var len = tempBusinessConfig['Property'].length ;
		for( var i = 0; i < len; ++i )
		{
			if( tempBusinessConfig['Property'][i]['Display'] != 'hidden' )
			{
				_configProperty.push( tempBusinessConfig['Property'][i] ) ;
			}
		}
		_businessConfig['ClusterName']  = tempBusinessConfig['ClusterName'] ;
		_businessConfig['BusinessName'] = tempBusinessConfig['BusinessName'] ;
		_businessConfig['BusinessType'] = tempBusinessConfig['BusinessType'] ;
		_businessConfig['DeployMod']    = tempBusinessConfig['DeployMod'] ;
		_businessConfig['Config']       = tempBusinessConfig['Config'] ;
		getHostList() ;
		if( _businessConfig['DeployMod'] == 'standalone' )
		{
			createStandalone() ;
		}
		else if( _businessConfig['DeployMod'] == 'distribution' )
		{
			createDistribution() ;
		}
	}
}

//---------------------------------------------------------------------

$(document).ready(function()
{
	if( $.cookie( 'SdbUser' ) == undefined || $.cookie( 'SdbPasswd' ) == undefined || $.cookie( 'SdbClusterName' ) == undefined || $.cookie( 'SdbBusinessConfig' ) == undefined || $.cookie( 'SdbGuideOrder' ) == undefined )
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
			$( '#__goBack' ).get(0).onclick = Function( 'gotoPage("' + __Deployment[1] + '")' ) ;
			$( '#__goOn' ).get(0).onclick = Function( 'addBusiness()' ) ;
		}
		else if( SdbGuideOrder == 'AddBusiness' )
		{
			$( '#__goBack' ).get(0).onclick = Function( 'gotoPage("' + __AddBuiness[0] + '")' ) ;
			$( '#__goOn' ).get(0).onclick = Function( 'addBusiness()' ) ;
		}
		else
		{
			gotoPage( "index.html" ) ;
		}
		var guideLen = __processPic[SdbGuideOrder].length ;
		for( var i = 0; i < guideLen; ++i )
		{
			if( ( SdbGuideOrder == 'Deployment' && i == 3 ) || ( SdbGuideOrder == 'AddBusiness' && i == 1 ) )
			{
				$( '#tab_box' ).append( '<li class="active">' + __processPic[SdbGuideOrder][i] + '</li>' ) ;
			}
			else
			{
				$( '#tab_box' ).append( '<li>' + __processPic[SdbGuideOrder][i] + '</li>' ) ;
			}
		}
		sdbjs.parts.loadingBox.create( 'loading' ) ;
		createBusiness() ;
		setUser() ;
	}

	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ), 'style': { 'height': 'sdbjs.public.height - 131' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - 428' } } ) ;
	sdbjs.fun.endOfCreate() ;
});