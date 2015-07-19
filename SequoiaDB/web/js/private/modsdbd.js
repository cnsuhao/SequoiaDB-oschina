//部署模式
var _deployModel = null ;

//业务配置
var _businessConfig = null ;

//业务参数
var _businessPara = null ;

//分区组列表
var _groupList = [] ;

//当前所选择的分区组编号
var _selectGroupNum = -1 ;

//从路径去除 role svcname groupname hostname
function selectDBPath( dbpath, role, svcname, groupname, hostname )
{
	if( 'role' === groupname || 'role' === hostname || 'svcname' === groupname || 'svcname' === hostname || 'groupname' === groupname || 'groupname' === hostname || 'hostname' === groupname || 'hostname' === hostname )
	{
		return dbpath ;
	}
	var replaceTemp = '' ;
	function filterSlash( str )
	{
		var len = str.length ;
		var replaceTemp2 = replaceTemp ;
		replaceTemp2 = '/' + replaceTemp2 ;
		if( str.charAt( len - 1 ) === '/' )
		{
			replaceTemp2 = replaceTemp2 + '/' ;
		}
		return replaceTemp2 ;
	}
	if( role !== null && role !== '' )
	{
		var reg = new RegExp( '/' + role + '/|/' + role + '$', 'g' ) ;
		replaceTemp = '[role]' ;
		dbpath = dbpath.replace( reg, filterSlash ) ;
	}
	if( svcname !== null && svcname !== '' )
	{
		var reg = new RegExp( '/' + svcname + '/|/' + svcname + '$', 'g' ) ;
		replaceTemp = '[svcname]' ;
		dbpath = dbpath.replace( reg, filterSlash ) ;
	}
	if( groupname !== null && groupname !== '' )
	{
		var reg = new RegExp( '/' + groupname + '/|/' + groupname + '$', 'g' ) ;
		replaceTemp = '[groupname]' ;
		dbpath = dbpath.replace( reg, filterSlash ) ;
	}
	if( hostname !== null && hostname !== '' )
	{
		var reg = new RegExp( '/' + hostname + '/|/' + hostname + '$', 'g' ) ;
		replaceTemp = '[hostname]' ;
		dbpath = dbpath.replace( reg, filterSlash ) ;
	}
	return dbpath ;
}

/*
 * 数据路径转义
 * 参数1 路径字符串，例子 /opt/sequoiadb/[role]/[svcname]/[groupname]/[hostname] 可用的特殊命令就是 [role] [svcname] [groupname] [hostname]
 * 参数2 主机名
 * 参数3 端口
 * 参数4 角色
 * 参数5 分区组名
 */
function dbpathEscape( str, hostname, svcname, role, groupname )
{
	var newPath = '' ;
	while( true )
	{
		var leftNum = str.indexOf( '[' ) ;
		var rightNum = -1 ;
		if( leftNum >= 0 )
		{
			newPath += str.substring( 0, leftNum ) ;
			str = str.substring( leftNum ) ;
			rightNum = str.indexOf( ']' ) ;
			if( rightNum >= 0 )
			{
				var order = str.substring( 1, rightNum ) ;
				if( order == 'hostname' )
				{
					newPath += hostname + '' ;
				}
				else if( order == 'svcname' )
				{
					newPath += svcname + '' ;
				}
				else if( order == 'role' )
				{
					newPath += role + '' ;
				}
				else if( order == 'groupname' )
				{
					newPath += groupname + '' ;
				}
				else
				{
					newPath += str.substring( 0, rightNum + 1 ) ;
				}
				str = str.substring( rightNum + 1 ) ;
			}
			else
			{
				newPath += str ;
				break ;
			}
		}
		else
		{
			newPath += str ;
			break ;
		}
	}
	return newPath ;
}

/*
 * 端口转义
 * 参数1 端口字符串，例子 '11810[+10]'
 * 参数2 第几个节点 最小值 0
 */
function portEscape( str, num )
{
	var newPort = null ;
	str = str + '' ;
	if( str == '' )
	{
		return str ;
	}
	if( str.indexOf( '[' ) > 0 )
	{
		var portStr = str.substring( 0, str.indexOf( '[' ) ) ;
		var escapeStr = str.substring( str.indexOf( '[' ) ) ;
		var n = 1 ;
		if( escapeStr.charAt(0) == '[' && escapeStr.charAt(escapeStr.length - 1) == ']' )
		{
			if( escapeStr.charAt(1) == '+' )
			{
				n = 1 ;
			}
			else if( escapeStr.charAt(1) == '-' )
			{
				n = -1 ;
			}
			else
			{
				return null ;
			}
			var tempNum = parseInt( escapeStr.substring( 2, escapeStr.length - 1 ) ) * num * n ;
			newPort = '' + ( parseInt( portStr ) + tempNum ) ;
		}
		else
		{
			return null ;
		}
	}
	else
	{
		newPort = str ;
	}
	if( checkPort( newPort ) )
	{
		return newPort ;
	}
	else
	{
		return null ;
	}
}

/*
 * 判断端口规则是否合法
 */
function checkPortRule( str, nodeNum )
{
	if( portEscape( str, 0 ) === null || portEscape( str, nodeNum - 1 ) === null )
	{
		return false ;
	}
	else
	{
		return true ;
	}
}

//设置过滤
function filterNodeList()
{
	var hostNameF = $( '#hostnamefilter' ).val() ;
	var svcnameF =	$( '#svcnamefilter' ).val() ;
	var dbpathF = $( '#dbpathfilter' ).val() ;
	var selectNode_1 = sdbjs.fun.getNode( 'roleSelect', 'selectBox' ) ;
	var selectNode_2 = sdbjs.fun.getNode( 'groupSelect', 'selectBox' ) ;
	var roleF = $( selectNode_1['obj'] ).val() ;
	var groupF = $( selectNode_2['obj'] ).val() ;

	$.each( _businessPara['Config'], function( index, nodeInfo ){
		if(( hostNameF !== '' && nodeInfo['HostName'].indexOf( hostNameF ) === -1 ) ||
			( svcnameF !== '' && nodeInfo['svcname'].indexOf( svcnameF ) === -1 ) ||
			( dbpathF !== '' && nodeInfo['dbpath'].indexOf( dbpathF ) === -1 ) ||
			( roleF !== 'all' && nodeInfo['role'].indexOf( roleF ) === -1 ) ||
			( groupF !== 'all' && nodeInfo['datagroupname'].indexOf( groupF ) === -1 ) )
		{
			sdbjs.parts.gridBox.hideBody( 'nodeListGrid', index ) ;
		}
		else
		{
			sdbjs.parts.gridBox.showBody( 'nodeListGrid', index ) ;
		}
	} ) ;
	sdbjs.parts.gridBox.repigment( 'nodeListGrid' ) ;
}

//---------------------------------- Group ----------------------------------

//指定分区组
function selectGroup( num, role, group )
{
	function filterGroup( role2, group2 )
	{
		var selectNode_1 = sdbjs.fun.getNode( 'roleSelect', 'selectBox' ) ;
		var selectNode_2 = sdbjs.fun.getNode( 'groupSelect', 'selectBox' ) ;
		$( '#hostnamefilter' ).val( '' ) ;
		$( '#svcnamefilter' ).val( '' ) ;
		$( '#dbpathfilter' ).val( '' ) ;
		if( role2 === 'coord' || role2 === 'catalog' )
		{
			$( selectNode_1['obj'] ).val( role2 ) ;
			$( selectNode_2['obj'] ).val( 'all' ) ;
		}
		else if ( role2 === 'data' )
		{
			$( selectNode_1['obj'] ).val( 'data' ) ;
			$( selectNode_2['obj'] ).val( group2 ) ;
		}
		else
		{
			$( selectNode_1['obj'] ).val( role2 ) ;
			$( selectNode_2['obj'] ).val( group2 ) ;
		}
		filterNodeList() ;
	}
	
	if( _selectGroupNum === num )
	{
		sdbjs.parts.tabList.unActive( 'groupTabList', _selectGroupNum ) ;
		_selectGroupNum = -1 ;
		filterGroup( 'all', 'all' ) ;
	}
	else
	{
		sdbjs.parts.tabList.unActive( 'groupTabList', _selectGroupNum ) ;
		sdbjs.parts.tabList.active( 'groupTabList', num ) ;
		_selectGroupNum = num ;
		filterGroup( role, group ) ;
	}
}

//重新计算分区组的节点数
function reSumGroupNodeNum( role, groupName, line )
{
	var nodeNum = 0 ;
	//计算
	$.each( _businessPara['Config'], function( index, nodeInfo ){
		if( nodeInfo['role'] === role && nodeInfo['datagroupname'] === groupName )
		{
			++nodeNum ;
		}
	} ) ;
	sdbjs.parts.tableBox.updateBody( 'tabListTable_' + line, 0, 0, function( tdObj ){
		//'节点数：'
		$( tdObj ).children( 'div' ).eq(1).text( _languagePack['modsdbd']['leftPanel']['groupList']['nodeNum'] + nodeNum ) ;
	} ) ;
	
	if( nodeNum > 0 )
	{
		if( role === 'data' )
		{
			//'删除分区组'
			sdbjs.parts.dropDownBox.updateMenu( 'groupDrop_' + line, 3, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][2] ), false, '' ) ;
		}
		//'删除节点'
		sdbjs.parts.dropDownBox.updateMenu( 'groupDrop_' + line, 1, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][1] ), true,
														'openRemoveNodeModal("' + role + '","' + groupName + '",' + line + ')' ) ;
	}
	else
	{
		if( role === 'data' )
		{
			//'删除分区组'
			sdbjs.parts.dropDownBox.updateMenu( 'groupDrop_' + line, 3, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][2] ), true, 'removeGroup("' + groupName + '",' + line + ')' );
		}
		//'删除节点'
		sdbjs.parts.dropDownBox.updateMenu( 'groupDrop_' + line, 1, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][1] ), false, '' ) ;
	}
}

//删除分区组
function removeGroup( groupName, line )
{
	var rc = true ;
	//只能删除数据组，看看组是否节点数为0
	$.each( _businessPara['Config'], function( index, nodeInfo ){
		if( nodeInfo['role'] === 'data' && nodeInfo['datagroupname'] === groupName )
		{
			rc = false ;
			return false ;
		}
	} ) ;
	if( rc === false )
	{
		showFootStatus( 'danger', sdbjs.fun.sprintf( _languagePack['error']['web']['modsdbd'][0], groupName ) ) ;
	}
	else
	{
		//删除分区组的下拉菜单
		sdbjs.parts.selectBox.remove( 'groupSelect', groupName, function( num ){
			return ( num > 0 ) ;
		} ) ;
		sdbjs.parts.tabList.remove( 'groupTabList', line ) ;
	}
}

//在左边列表添加一个分区组
function addGroupList( groupName, nodeNum )
{
	var rc = true ;
	var index = _groupList.length ;
	//看看有没有重复
	if( index > 1 )
	{
		$.each( _groupList, function( i, tempGroupName ){
			if( i > 1 && tempGroupName === groupName )
			{
				rc = false ;
				return false ;
			}
		} ) ;
	}
	if( rc === true )
	{
		if( index > 1 )
		{
			sdbjs.parts.selectBox.add( 'groupSelect', groupName, groupName ) ;
		}
		_groupList.push( groupName ) ;
		sdbjs.parts.tabList.add( 'groupTabList', function( liObj ){
			$( liObj ).css( 'zoom', 1 ) ;
			sdbjs.parts.tableBox.create( liObj, 'tabListTable_' + index ) ;
			sdbjs.parts.tableBox.update( 'tabListTable_' + index, 'compact' ) ;
			sdbjs.parts.tableBox.addBody( 'tabListTable_' + index, [{ 'text': function( tdObj ){
				var newDiv_1 = $( '<div></div>' ).css( { 'padding': '2px 2px 2px 0', 'font-weight': 'bold' } ).text( groupName ) ;
				//'节点数：'
				var newDiv_2 = $( '<div></div>' ).css( 'color', '#666' ).text( _languagePack['modsdbd']['leftPanel']['groupList']['nodeNum'] + nodeNum ) ;
				$( tdObj ).append( newDiv_1 ).append( newDiv_2 ).css( 'cursor', 'pointer' ) ;
				if( index > 1 )
				{
					sdbjs.fun.addClick( tdObj, 'selectGroup(' + index + ',"data","' + groupName + '")' ) ;
				}
				else
				{
					sdbjs.fun.addClick( tdObj, 'selectGroup(' + index + ',"' + groupName + '")' ) ;
				}
			} },
																					  { 'text': function( tdObj ){
				sdbjs.parts.dropDownBox.create( tdObj, 'groupDrop_' + index ) ;
				sdbjs.parts.dropDownBox.update( 'groupDrop_' + index, '', 'btn-lg' ) ;
				if( index > 1 )
				{
					//'添加节点'
					sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][0] ), true, 'openCreateNodeModal("data","' + groupName + '",' + index + ')' ) ;
					//要有节点才可以删除
					if( nodeNum > 0 )
					{
						//'删除节点'
						sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][1] ), true, 'openRemoveNodeModal("data","' + groupName + '",' + index + ')' ) ;
					}
					else
					{
						//'删除节点'
						sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][1] ), false ) ;
					}
				}
				else
				{
					//'添加节点'
					sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][0] ), true, 'openCreateNodeModal("' + groupName + '","",' + index + ')' ) ;
					//要有节点才可以删除
					if( nodeNum > 0 )
					{
						//'删除节点'
						sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][1] ), true, 'openRemoveNodeModal("' + groupName + '","",' + index + ')' ) ;
					}
					else
					{
						//'删除节点'
						sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][1] ), false, '' ) ;
					}
				}

				if( index > 1 )
				{
					sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, '' ) ;
					//有节点不能删除分区组
					if( nodeNum > 0 )
					{
						//'删除分区组'
						sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][2] ), false, '' );
					}
					else
					{
						//'删除分区组'
						sdbjs.parts.dropDownBox.add( 'groupDrop_' + index, htmlEncode( _languagePack['modsdbd']['leftPanel']['groupList']['drop'][2] ), true, 'removeGroup("' + groupName + '",' + index + ')' );
					}
				}
			}, width: 20 } ] ) ;
		} ) ;
	}
	return rc ;
}

//打开创建分区组模态框
function openCreateGroupModal()
{
	$( '#createGroupName' ).val( '' ) ;
	sdbjs.fun.setCSS( 'createGroupAlert', { 'display': 'none' } ) ;
	sdbjs.parts.modalBox.show( 'createGroup' ) ;
	$( '#createGroupName' ).get(0).focus() ;
}

//模态框创建分区组
function createGroup()
{
	var groupName = $( '#createGroupName' ).val() ;
	if( !checkStrName( groupName ) )
	{
		sdbjs.fun.setCSS( 'createGroupAlert', { 'display': 'block' } ) ;
		//'Error: 分区组名格式错误，分区组名只能由数字字母下划线组成，并且长度在 1 - 255 个字符内。'
		sdbjs.parts.alertBox.update( 'createGroupAlert', htmlEncode( _languagePack['error']['web']['modsdbd'][1] ), 'danger' ) ;
		return;
	}
	if( !addGroupList( groupName, 0 ) )
	{
		sdbjs.fun.setCSS( 'createGroupAlert', { 'display': 'block' } ) ;
		//'Error: 分区组已经存在。'
		sdbjs.parts.alertBox.update( 'createGroupAlert', htmlEncode( _languagePack['error']['web']['modsdbd'][2] ), 'danger' ) ;
	}
	else
	{
		sdbjs.parts.modalBox.hide( 'createGroup' ) ;
	}
}

//---------------------------------- node ----------------------------------

//删除节点
function removeOneNode( role, groupname, line )
{
	var nodeID = sdbjs.parts.selectBox.get( 'nodeRemoveSelect' ) ;
	//删除节点表
	sdbjs.parts.gridBox.removeBody( 'nodeListGrid', nodeID ) ;
	sdbjs.parts.gridBox.repigment( 'nodeListGrid', false ) ;
	//删除后台数据
	_businessPara['Config'].splice( nodeID, 1 ) ;
	sdbjs.fun.saveData( 'SdbConfigInfo', JSON.stringify( _businessPara ) ) ;
	//更新数据
	reSumGroupNodeNum( role, groupname, line ) ;
	sdbjs.parts.modalBox.hide( 'removeNode' ) ;
	
	//把该节点后面的节点修改索引
	var len = _businessPara['Config'].length ;
	for( var i = nodeID; i < len; ++i )
	{
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', i, 1, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + i + ')' ) ;
		} ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', i, 2, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + i + ')' ) ;
		} ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', i, 3, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + i + ')' ) ;
		} ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', i, 4, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + i + ')' ) ;
		} ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', i, 5, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + i + ')' ) ;
		} ) ;
	}
}

//打开删除节点模态框
function openRemoveNodeModal( role, groupname, line )
{
	var selectObj = sdbjs.fun.getNode( 'nodeRemoveSelect', 'selectBox' ) ;
	//初始化
	sdbjs.parts.selectBox.set( 'nodeRemoveSelect', '0' ) ;
	//清空
	sdbjs.parts.selectBox.empty( 'nodeRemoveSelect' ) ;
	//读取所有节点
	$.each( _businessPara['Config'], function( index, nodeInfo ){
		if( nodeInfo['role'] === role && ( ( role === 'data' && nodeInfo['datagroupname'] === groupname ) || ( role === 'coord' || role === 'catalog' ) ) )
		{
			sdbjs.parts.selectBox.add( 'nodeRemoveSelect', nodeInfo['HostName'] + ':' + nodeInfo['svcname'], index ) ;
		}
	} ) ;
	//'确定'
	sdbjs.parts.buttonBox.update( 'removeNodeOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'removeOneNode("' + role + '","' + groupname + '",' + line + ')' ) ;
	sdbjs.parts.modalBox.show( 'removeNode' ) ;
	$( selectObj['obj'] ).get(0).focus() ;
}

//创建节点
function createNewNode( hostName, role, groupname, line, nodeID, isLoadData )
{
	//刷新
	sdbjs.parts.gridBox.repigment( 'modNodeConfGrid', true ) ;
	sdbjs.parts.gridBox.repigment( 'modNodeConfGrid2', true ) ;
	function nodeParaError( obj, level, errMsg )
	{
		$( obj ).parent().parent().css( 'background-color', '#f2dede' ) ;
		sdbjs.fun.setCSS( 'modNodeFootTableAlert', { 'display': 'block' } ) ;
		sdbjs.parts.alertBox.update( 'modNodeFootTableAlert', htmlEncode( errMsg ), 'danger' ) ;
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
	var rc = true ;
	var valueList = {} ;
	//加载输入框的数据
	$.each( _businessPara['Property'], function( paraID, property ){
		if( property['Display'] !== 'hidden' )
		{
			var value = $( '#' + property['Name'] + '_np' ).val() ;
			if( property['Name'] !== 'svcname' )
			{
				var rs = checkInputValue( property['Display'], property['Type'], property['Valid'], property['WebName'], value ) ;
				if( rs !== '' )
				{
					//报错
					nodeParaError( $( '#' + property['Name'] + '_np' ), property['Level'], rs ) ;
					rc = false ;
					return false ;
				}
			}
			valueList[ property['Name'] ] = $( '#' + property['Name'] + '_np' ).val() ;
		}
	} ) ;
	
	if( rc === false )
	{
		return;
	}
	
	//转换端口
	valueList['svcname'] = portEscape( valueList['svcname'], 0 ) ;
	if( valueList['svcname'] === null || !checkPort( valueList['svcname'] ) )
	{
		//报错，端口格式错误 '服务名格式错误'
		nodeParaError( $( '#svcname_np' ), '0', _languagePack['error']['web']['modsdbd'][3] ) ;
		return;
	}
	//转换路径
	valueList['dbpath'] = dbpathEscape( valueList['dbpath'], hostName, valueList['svcname'], role, groupname ) ;
	
	valueList['HostName'] = hostName ;
	valueList['role'] = role ;
	valueList['datagroupname'] = groupname ;
	
	
	sdbjs.parts.gridBox.addBody( 'nodeListGrid', [{ 'text': '<input style="display:none;" type="checkbox">', 'width': '0%' },
																 { 'text': htmlEncode( valueList['HostName'] ), 'width': '20%' },
																 { 'text': htmlEncode( valueList['svcname'] ), 'width': '15%' },
																 { 'text': htmlEncode( valueList['dbpath'] ), 'width': '30%' },
																 { 'text': htmlEncode( valueList['role'] ), 'width': '15%' },
																 { 'text': htmlEncode( valueList['datagroupname'] ), 'width': '20%' } ] ) ;
	sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 1, function( tdObj ){
		$( tdObj ).css( 'cursor', 'pointer' ) ;
		sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + nodeID + ')' ) ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 2, function( tdObj ){
		$( tdObj ).css( 'cursor', 'pointer' ) ;
		sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + nodeID + ')' ) ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 3, function( tdObj ){
		$( tdObj ).css( 'cursor', 'pointer' ) ;
		sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + nodeID + ')' ) ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 4, function( tdObj ){
		$( tdObj ).css( 'cursor', 'pointer' ) ;
		sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + nodeID + ')' ) ;
	} ) ;
	sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 5, function( tdObj ){
		$( tdObj ).css( 'cursor', 'pointer' ) ;
		sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + nodeID + ')' ) ;
	} ) ;
	_businessPara['Config'].push( valueList ) ;
	sdbjs.fun.saveData( 'SdbConfigInfo', JSON.stringify( _businessPara ) ) ;
	if( isLoadData === false )
	{
		reSumGroupNodeNum( role, groupname, line ) ;
	}
	sdbjs.parts.modalBox.hide( 'modNodeConf' ) ;
}

//打开创建节点的模态框
function openCreateNodeModal( role, groupname, line )
{
	var selectObj = sdbjs.fun.getNode( 'nodeModelSelect', 'selectBox' ) ;
	//初始化
	$( selectObj['obj'] ).val( '0' ) ;
	//隐藏
	sdbjs.parts.tableBox.updateBody( 'createNodeTable', 2, 0, function( tdObj ){
		$( tdObj ).hide() ;
	} ) ;
	sdbjs.parts.tableBox.updateBody( 'createNodeTable', 2, 1, function( tdObj ){
		$( tdObj ).hide() ;
	} ) ;
	//清空
	sdbjs.parts.selectBox.empty( 'nodeHostSelect' ) ;
	sdbjs.parts.selectBox.empty( 'nodeCopySelect' ) ;
	//读取所有主机
	$.each( _businessConfig['HostInfo'], function( index, hostInfo ){
		sdbjs.parts.selectBox.add( 'nodeHostSelect', hostInfo['HostName'], hostInfo['HostName'] ) ;
	} ) ;
	//读取所有节点
	$.each( _businessPara['Config'], function( index, nodeInfo ){
		sdbjs.parts.selectBox.add( 'nodeCopySelect', nodeInfo['HostName'] + ':' + nodeInfo['svcname'], index ) ;
	} ) ;
	//'确定'
	sdbjs.parts.buttonBox.update( 'createNodeOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'setCreateNodeModel("' + role + '","' + groupname + '",' + line + ')' ) ;
	sdbjs.parts.modalBox.show( 'createNode' ) ;
	$( selectObj['obj'] ).get(0).focus() ;
}

//设置创建节点模式
function setCreateNodeModel( role, groupname, line )
{
	var selectObj_1 = sdbjs.fun.getNode( 'nodeModelSelect', 'selectBox' ) ;
	var selectObj_2 = sdbjs.fun.getNode( 'nodeCopySelect', 'selectBox' ) ;
	var selectObj_3 = sdbjs.fun.getNode( 'nodeHostSelect', 'selectBox' ) ;
	var model = $( selectObj_1['obj'] ).val() ;
	var nodeID = $( selectObj_2['obj'] ).val() ;
	var HostName = $( selectObj_3['obj'] ).val() ;
	var nodeLen = _businessPara['Config'].length ;

	sdbjs.parts.modalBox.hide( 'createNode' ) ;
	if( model === '0' )
	{
		//默认值
		openNodeParaModal( '3', 0 ) ;
	}
	else if( model === '1' )
	{
		//没有值
		openNodeParaModal( '2', 0 ) ;
	}
	else if( model === '2' )
	{
		//复制节点
		openNodeParaModal( '0', parseInt( nodeID ) ) ;
	}
	//'修改新节点配置'
	sdbjs.parts.modalBox.update( 'modNodeConf', htmlEncode( _languagePack['modsdbd']['createNodeModal']['title2'] ) ) ;
	//'确定'
	sdbjs.parts.buttonBox.update('modNodeConfOK',
										  htmlEncode( _languagePack['public']['button']['ok'] ),
										  'primary',
										  null,
										  'createNewNode("' + HostName + '","' + role + '","' + groupname + '",' + line + ',' + nodeLen + ', false)' ) ;
}

//编辑节点列表
function editNodeList( btnObj )
{
	//'编辑'
	if( $( btnObj ).text() === _languagePack['modsdbd']['rightPanel']['button']['button'][0] )
	{
		//'完成'
		$( btnObj ).text( _languagePack['modsdbd']['rightPanel']['button']['button'][1] ) ;
		
		sdbjs.parts.gridBox.updateTitle( 'nodeListGrid', 0, 0, function( tdObj ){
			$( tdObj ).children( 'input' ).show( 200 ) ;
		} ) ;
		sdbjs.parts.gridBox.updateTitle( 'nodeListGrid', 1, 0, function( tdObj ){
			$( tdObj ).children( 'input' ).show( 200 ) ;
		} ) ;
		
		$.each( _businessPara['Config'], function( index, nodeInfo ){
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 0, function( tdObj ){
				$( tdObj ).children( 'input' ).get(0).checked = false ;
				$( tdObj ).children( 'input' ).show( 200 ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 1, function( tdObj ){
				$( tdObj ).css( 'cursor', 'default' ) ;
				sdbjs.fun.addClick( tdObj, '' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 2, function( tdObj ){
				$( tdObj ).css( 'cursor', 'default' ) ;
				sdbjs.fun.addClick( tdObj, '' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 3, function( tdObj ){
				$( tdObj ).css( 'cursor', 'default' ) ;
				sdbjs.fun.addClick( tdObj, '' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 4, function( tdObj ){
				$( tdObj ).css( 'cursor', 'default' ) ;
				sdbjs.fun.addClick( tdObj, '' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 5, function( tdObj ){
				$( tdObj ).css( 'cursor', 'default' ) ;
				sdbjs.fun.addClick( tdObj, '' ) ;
			} ) ;
		} ) ;
	}
	else
	{
		//'编辑'
		$( btnObj ).text( _languagePack['modsdbd']['rightPanel']['button']['button'][0] ) ;
		
		sdbjs.parts.gridBox.updateTitle( 'nodeListGrid', 0, 0, function( tdObj ){
			$( tdObj ).children( 'input' ).hide( 200 ) ;
		} ) ;
		sdbjs.parts.gridBox.updateTitle( 'nodeListGrid', 1, 0, function( tdObj ){
			$( tdObj ).children( 'input' ).hide( 200 ) ;
		} ) ;
		
		$.each( _businessPara['Config'], function( index, nodeInfo ){
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 0, function( tdObj ){
				$( tdObj ).children( 'input' ).hide( 200 ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 1, function( tdObj ){
				$( tdObj ).css( 'cursor', 'pointer' ) ;
				sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 2, function( tdObj ){
				$( tdObj ).css( 'cursor', 'pointer' ) ;
				sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 3, function( tdObj ){
				$( tdObj ).css( 'cursor', 'default' ) ;
				sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 4, function( tdObj ){
				$( tdObj ).css( 'cursor', 'pointer' ) ;
				sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
			} ) ;
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 5, function( tdObj ){
				$( tdObj ).css( 'cursor', 'pointer' ) ;
				sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
			} ) ;
		} ) ;
	}
}

//全选
function selectAll()
{
	//'编辑'
	if( $( '#editButton' ).text() === _languagePack['modsdbd']['rightPanel']['button']['button'][0] )
	{
		editNodeList( $( '#editButton' ) ) ;
	}
	$.each( _businessPara['Config'], function( index, nodeInfo ){
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 0, function( tdObj ){
			if( !$( tdObj ).is( ':hidden' ) )
			{
				$( tdObj ).children( 'input' ).get(0).checked = true ;
			}
		} ) ;
	} ) ;
}

//反选
function unSelectAll()
{
	//'编辑'
	if( $( '#editButton' ).text() === _languagePack['modsdbd']['rightPanel']['button']['button'][0] )
	{
		editNodeList( $( '#editButton' ) ) ;
	}
	else
	{
		$.each( _businessPara['Config'], function( index, nodeInfo ){
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 0, function( tdObj ){
				if( !$( tdObj ).is( ':hidden' ) )
				{
					$( tdObj ).children( 'input' ).get(0).checked = !$( tdObj ).children( 'input' ).get(0).checked ;
				}
			} ) ;
		} ) ;
	}
}

//保存节点参数
function saveNodePara( type, index )
{
	sdbjs.parts.gridBox.repigment( 'modNodeConfGrid', true ) ;
	sdbjs.parts.gridBox.repigment( 'modNodeConfGrid2', true ) ;
	function nodeParaError( obj, level, errMsg )
	{
		$( obj ).parent().parent().css( 'background-color', '#f2dede' ) ;
		sdbjs.fun.setCSS( 'modNodeFootTableAlert', { 'display': 'block' } ) ;
		sdbjs.parts.alertBox.update( 'modNodeFootTableAlert', htmlEncode( errMsg ), 'danger' ) ;
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
	function saveOndeNode( tempValueList, nodeID, id, saveType )
	{
		if( saveType === '0' || ( saveType !== '0' && tempValueList['svcname'] !== '' ) )
		{
			//转换端口
			tempValueList['svcname'] = portEscape( tempValueList['svcname'], id ) ;
			if( tempValueList['svcname'] === null || !checkPort( tempValueList['svcname'] ) )
			{
				//报错，端口格式错误 '服务名格式错误'
				nodeParaError( $( '#svcname_np' ), '0', _languagePack['error']['web']['modsdbd'][3] ) ;
				return ;
			}
			//转换路径
			tempValueList['dbpath'] = dbpathEscape(tempValueList['dbpath'],
																_businessPara['Config'][nodeID]['HostName'],
																tempValueList['svcname'],
																_businessPara['Config'][nodeID]['role'],
																_businessPara['Config'][nodeID]['datagroupname'] ) ;
		}
		else
		{
			//转换路径
			tempValueList['dbpath'] = dbpathEscape(tempValueList['dbpath'],
																_businessPara['Config'][nodeID]['HostName'],
																_businessPara['Config'][nodeID]['svcname'],
																_businessPara['Config'][nodeID]['role'],
																_businessPara['Config'][nodeID]['datagroupname'] ) ;
		}
		
		//保存节点
		$.each( tempValueList, function( nodeConfKey, nodeConfValue ){
			if( saveType === '0' || ( saveType !== '0' && nodeConfValue !== '' ) )
			{
				_businessPara['Config'][nodeID][ nodeConfKey ] = nodeConfValue ;
				sdbjs.fun.saveData( 'SdbConfigInfo', JSON.stringify( _businessPara ) ) ;
			}
		} ) ;
		//写入到表格
		if( saveType === '0' || ( saveType !== '0' && _businessPara['Config'][nodeID]['svcname'] !== '' ) )
		{
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 2, function( tdObj ){
				$( tdObj ).text( _businessPara['Config'][nodeID]['svcname'] ) ;
			} ) ;
		}
		if( saveType === '0' || ( saveType !== '0' && _businessPara['Config'][nodeID]['dbpath'] !== '' ) )
		{
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 3, function( tdObj ){
				$( tdObj ).text( _businessPara['Config'][nodeID]['dbpath'] ) ;
			} ) ;
		}
		if( saveType === '0' || ( saveType !== '0' && _businessPara['Config'][nodeID]['role'] !== '' ) )
		{
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 4, function( tdObj ){
				$( tdObj ).text( _businessPara['Config'][nodeID]['role'] ) ;
			} ) ;
		}
		if( saveType === '0' || ( saveType !== '0' && _businessPara['Config'][nodeID]['datagroupname'] !== '' ) )
		{
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 5, function( tdObj ){
				$( tdObj ).text( _businessPara['Config'][nodeID]['datagroupname'] ) ;
			} ) ;
		}
	}
	var rc = true ;
	//加载输入框的数据
	var valueList = {} ;
	$.each( _businessPara['Property'], function( paraID, property ){
		if( property['Display'] !== 'hidden' )
		{
			var value = $( '#' + property['Name'] + '_np' ).val() ;
			if( property['Name'] !== 'svcname' )
			{
				var rs = checkInputValue( property['Display'], property['Type'], property['Valid'], property['WebName'], value ) ;
				if( rs !== '' )
				{
					//报错
					nodeParaError( $( '#' + property['Name'] + '_np' ), property['Level'], rs ) ;
					rc = false ;
					return false ;
				}
			}
			valueList[ property['Name'] ] = $( '#' + property['Name'] + '_np' ).val() ;
		}
	} ) ;

	if( rc === false )
	{
		return;
	}

	if( type === '0' )
	{
		if( !checkPortRule( valueList['svcname'], 0 ) )
		{
			//报错，端口格式错误 '服务名格式错误'
			nodeParaError( $( '#svcname_np' ), '0', _languagePack['error']['web']['modsdbd'][3] ) ;
			return ;
		}
		saveOndeNode( valueList, index, 0, type ) ;
	}
	else
	{
		var nodesNum = 0 ;
		//求总的选择主机数
		$.each( _businessPara['Config'], function( nodeID, nodeInfo ){
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 0, function( tdObj ){
				if( !$( tdObj ).is( ':hidden' ) && $( tdObj ).children( 'input' ).get(0).checked === true )
				{
					++nodesNum ;
				}
			} ) ;
		} ) ;
		if( !checkPortRule( valueList['svcname'], nodesNum ) )
		{
			//报错，端口格式错误 '服务名格式错误'
			nodeParaError( $( '#svcname_np' ), '0', _languagePack['error']['web']['modsdbd'][3] ) ;
			return ;
		}
		nodesNum = 0 ;
		//找出选择的主机
		$.each( _businessPara['Config'], function( nodeID, nodeInfo ){
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 0, function( tdObj ){
				if( !$( tdObj ).is( ':hidden' ) && $( tdObj ).children( 'input' ).get(0).checked === true )
				{
					var newValueList = $.extend( true, {}, valueList ) ;
					saveOndeNode( newValueList, nodeID, nodesNum, type ) ;
					++nodesNum ;
				}
			} ) ;
		} ) ;
		editNodeList( $( '#editButton' ) ) ;
	}
	sdbjs.parts.modalBox.hide( 'modNodeConf' ) ;
}

//重绘节点参数模态框
function redrawNodePataModal()
{
	$( document.body ).css( 'overflow', 'hidden' ) ;
	try{
		sdbjs.fun.setCSS( 'modNodeConfTab~body', { 'max-height': 400 } ) ;
		sdbjs.fun.setCSS( 'modNodeConfGrid~body', { 'max-height': 350 } ) ;
		sdbjs.fun.setCSS( 'modNodeConfGrid2~body', { 'max-height': 350 } ) ;
		sdbjs.parts.modalBox.redraw( 'modNodeConf' ) ;
	}catch(e){}
	$( document.body ).css( 'overflow', 'visible' ) ;
	//滚动条回到顶部
	var gridBodyNode = sdbjs.fun.getNode( 'modNodeConfGrid~body' ) ;
	$( gridBodyNode['obj'] ).get(0).scrollTop = 0 ;
	var gridBodyNode2 = sdbjs.fun.getNode( 'modNodeConfGrid2~body' ) ;
	$( gridBodyNode2['obj'] ).get(0).scrollTop = 0 ;
}

//打开修改节点参数项的模态框
function openNodeParaModal( type, index )
{
	sdbjs.parts.gridBox.repigment( 'modNodeConfGrid', true ) ;
	sdbjs.parts.gridBox.repigment( 'modNodeConfGrid2', true ) ;
	sdbjs.fun.setCSS( 'modNodeFootTableAlert', { 'display': 'none' } ) ;
	//'确定'
	sdbjs.parts.buttonBox.update( 'modNodeConfOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'saveNodePara("' + type + '",' + index + ')' ) ;
	if( type === '0' )
	{
		//加载该节点的参数值
		$.each( _businessPara['Config'][index], function( key, value ){
			if( key === 'dbpath' )
			{
				var tempDBPath = selectDBPath( value,
														_businessPara['Config'][index]['role'],
														_businessPara['Config'][index]['svcname'],
														_businessPara['Config'][index]['datagroupname'],
														_businessPara['Config'][index]['HostName'] ) ;
				$( '#' + key + '_np' ).val( tempDBPath ) ;
			}
			else
			{
				$( '#' + key + '_np' ).val( value ) ;
			}
		} ) ;
		sdbjs.parts.modalBox.update( 'modNodeConf', '<div style="width:715px;text-overflow:ellipsis;overflow:hidden;white-space:nowrap;">' + htmlEncode( _businessPara['Config'][index]['role'] + '  ' + _businessPara['Config'][index]['HostName'] + ':' + _businessPara['Config'][index]['svcname'] ) + '</div>' ) ;
	}
	else if( type === '2' )
	{
		//不加载任何参数
		$.each( _businessPara['Property'], function( paraID, value ){
			$( '#' + value['Name'] + '_np' ).val( '' ) ;
		} ) ;
	}
	else if( type === '3' )
	{
		//加载默认参数
		$.each( _businessPara['Property'], function( paraID, value ){
			$( '#' + value['Name'] + '_np' ).val( value['Default'] ) ;
		} ) ;
	}
	else if( type === '1' )
	{
		//加载多个节点参数值
		//看看是否已经展开多选
		//'编辑'
		if( $( '#editButton' ).text() === _languagePack['modsdbd']['rightPanel']['button']['button'][0] )
		{
			editNodeList( $( '#editButton' ) ) ;
			//'Tip：批量修改节点配置至少选择一个节点。'
			showFootStatus( 'info', _languagePack['tip']['web']['modsdbd'][0] ) ;
			return;
		}
		//已经展开
		//修改标题
		//'批量修改节点配置'
		sdbjs.parts.modalBox.update( 'modNodeConf', htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['title2'] ) ) ;
		//查看有多少个节点选择，并且找出选择节点的相同值
		var checkedNodeNum = 0 ;
		var tempPara = {} ;
		var svcname = null ;
		var offsetSymobl = null ;
		var offsetNum = null ;
		$.each( _businessPara['Config'], function( nodeID, nodeInfo ){
			sdbjs.parts.gridBox.updateBody( 'nodeListGrid', nodeID, 0, function( tdObj ){
				if( !$( tdObj ).is( ':hidden' ) && $( tdObj ).children( 'input' ).get(0).checked === true )
				{
					++checkedNodeNum ;
					$.each( _businessPara['Property'], function( paraID, paraInfo ){
						//看看这个参数是否已经存在
						if( typeof( tempPara[ paraInfo['Name'] ] ) === 'undefined' )
						{
							//不存在则创建
							tempPara[ paraInfo['Name'] ] = nodeInfo[ paraInfo['Name'] ] ;
							//特殊，svcname，看看是否递增或者递减
							if( paraInfo['Name'] === 'svcname' )
							{
								svcname = parseInt( nodeInfo[ paraInfo['Name'] ] ) ;
							}
							else if( paraInfo['Name'] === 'dbpath' )
							{
								tempPara[ paraInfo['Name'] ] = selectDBPath(nodeInfo[ paraInfo['Name'] ],
																						  nodeInfo['role'],
																						  nodeInfo['svcname'],
																						  nodeInfo['datagroupname'],
																						  nodeInfo['HostName'] ) ;
							}
						}
						else
						{
							//特殊，svcname，看看是否递增或者递减
							if( paraInfo['Name'] === 'svcname' )
							{
								if( svcname !== null )
								{
									//获取当前的svcname和偏移符号和偏移值
									var tempSvcname = parseInt( nodeInfo[ paraInfo['Name'] ] ) ;
									var tempOffsetSymobl = ( svcname <= tempSvcname ? 1 : -1 ) ;
									var tempOffsetNum = ( tempSvcname - svcname ) * tempOffsetSymobl ;
									svcname = tempSvcname ;
									if( offsetSymobl === null || offsetNum === null )
									{
										offsetSymobl = tempOffsetSymobl ;
										offsetNum = tempOffsetNum ;
									}
									else
									{
										if( offsetSymobl !== tempOffsetSymobl || offsetNum !== tempOffsetNum )
										{
											svcname = null ;
										}
									}
								}
							}
							else if( paraInfo['Name'] === 'dbpath' )
							{
								var tempDBPath = selectDBPath(nodeInfo[ paraInfo['Name'] ],
																		nodeInfo['role'],
																		nodeInfo['svcname'],
																		nodeInfo['datagroupname'],
																		nodeInfo['HostName'] ) ;
								//存在则做判断
								if( tempPara[ paraInfo['Name'] ] !== tempDBPath )
								{
									tempPara[ paraInfo['Name'] ] = '' ;
								}
							}
							else
							{
								//存在则做判断
								if( tempPara[ paraInfo['Name'] ] !== nodeInfo[ paraInfo['Name'] ] )
								{
									if( paraInfo['Display'] === 'select box' )
									{
										tempPara[ paraInfo['Name'] ] = paraInfo['Default'] ;
									}
									else
									{
										tempPara[ paraInfo['Name'] ] = '' ;
									}
								}
							}
						}
					} ) ;
				}
			} ) ;
		} ) ;
		
		if( checkedNodeNum === 0 )
		{
			//'Tip：批量修改节点配置至少选择一个节点。'
			showFootStatus( 'info', _languagePack['tip']['web']['modsdbd'][0] ) ;
			return;
		}
		
		if( svcname !== null )
		{
			if( tempPara['svcname'] === ( svcname + '' ) )
			{
				//没有步进
				tempPara['svcname'] = tempPara['svcname'] ;
			}
			else
			{
				//有步进
				var rule = '[' ;
				if( offsetSymobl === 1 )
				{
					rule += '+'
				}
				else
				{
					rule += '-'
				}
				rule += ( offsetNum + ']' ) ;
				tempPara['svcname'] = tempPara['svcname'] + rule ;
			}
		}
		else
		{
			tempPara['svcname'] = '' ;
		}
		
		$.each( tempPara, function( key, value ){
			$( '#' + key + '_np' ).val( value ) ;
		} ) ;
	}
	//显示模态框
	sdbjs.parts.modalBox.show( 'modNodeConf' ) ;
	//回到第一项
	sdbjs.parts.navTabBox.show( 'modNodeConfTab', 0, 'redrawNodePataModal()' ) ;
	redrawNodePataModal() ;
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
	return ;
}

//加载业务配置
function loadBusinessConf()
{
	var rc = true ;
	if( _businessPara === null )
	{
		sdbjs.parts.loadingBox.show( 'loading' ) ;
		restGetBusinessConfig( false, function( jsonArr, textStatus, jqXHR ){
			_businessPara = jsonArr[0] ;
		}, function( json ){
			rc = false ;
			showProcessError( json['detail'] ) ;
		}, function(){
			sdbjs.parts.loadingBox.hide( 'loading' ) ;
		}, _businessConfig ) ;
	}
	return rc ;
}

//加载分区组和节点
function loadGroupNode()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	var coordNum = 0 ;
	var catalogNum = 0 ;
	var tempGroupList = {} ;
	//遍历所有节点
	$.each( _businessPara['Config'], function( index, nodeInfo ){
		if( nodeInfo['role'] === 'coord' )
		{
			++coordNum ;
		}
		else if( nodeInfo['role'] === 'catalog' )
		{
			++catalogNum ;
		}
		else
		{
			if( typeof( tempGroupList[ nodeInfo['datagroupname'] ] ) === 'undefined' )
			{
				tempGroupList[ nodeInfo['datagroupname'] ] = 1 ;
			}
			else
			{
				++tempGroupList[ nodeInfo['datagroupname'] ] ;
			}
		}
		//创建节点
		sdbjs.parts.gridBox.addBody( 'nodeListGrid', [{ 'text': '<input style="display:none;" type="checkbox">', 'width': '0%' },
																	 { 'text': htmlEncode( nodeInfo['HostName'] ), 'width': '20%' },
																	 { 'text': htmlEncode( nodeInfo['svcname'] ), 'width': '15%' },
																	 { 'text': htmlEncode( nodeInfo['dbpath'] ), 'width': '30%' },
																	 { 'text': htmlEncode( nodeInfo['role'] ), 'width': '15%' },
																	 { 'text': htmlEncode( nodeInfo['datagroupname'] ), 'width': '20%' } ] ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 1, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
		} ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 2, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
		} ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 3, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
		} ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 4, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
		} ) ;
		sdbjs.parts.gridBox.updateBody( 'nodeListGrid', index, 5, function( tdObj ){
			$( tdObj ).css( 'cursor', 'pointer' ) ;
			sdbjs.fun.addClick( tdObj, 'openNodeParaModal("0",' + index + ')' ) ;
		} ) ;
	} ) ;
	//创建coord
	addGroupList( 'coord', coordNum ) ;
	//创建catalog
	addGroupList( 'catalog', catalogNum ) ;
	//创建data分区组
	$.each( tempGroupList, function( groupName, nodeNum ){
		addGroupList( groupName, nodeNum ) ;
	} ) ;
	sdbjs.parts.loadingBox.hide( 'loading' ) ;
}

//加载配置参数列表
function loadNodePara()
{
	//遍历所有配置项
	$.each( _businessPara['Property'], function( index, property ){
		var newInputObj = createHtmlInput( property['Display'], property['Valid'], property['Default'], property['Edit'] ) ;
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
}

function createDynamicHtml()
{
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	if( loadBusinessConf() )
	{
		loadGroupNode() ;
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
		//'扫描主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/zoom.png"> ' + htmlEncode( _languagePack['public']['tabPage'][2] ), false, null ) ;
		//'添加主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/layers_1.png"> ' + htmlEncode( _languagePack['public']['tabPage'][3] ), false, null );
		//'安装主机'
		sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][4] ), false, null );
	}
	//'配置业务'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cube.png"> ' + htmlEncode( _languagePack['public']['tabPage'][5] ), false, null ) ;
	//'修改业务'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/doc_lines_stright.png"> ' + htmlEncode( _languagePack['public']['tabPage'][6] ), true, null );
	//'安装业务'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/cog.png"> ' + htmlEncode( _languagePack['public']['tabPage'][7] ), false, null );
	
	/* 左边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-left', 460, 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-left', { 'float': 'left', 'padding': '10px' } ) ;
	
	/* 左边 状态栏 */
	sdbjs.parts.panelBox.create( 'middle-left', 'groupListBar', 'auto', 'variable' ) ;
	sdbjs.parts.panelBox.update( 'groupListBar', function( panelTitle ){
		sdbjs.parts.dropDownBox.create( panelTitle['obj'], 'addGroupDropDown' ) ;
		sdbjs.parts.dropDownBox.update( 'addGroupDropDown', '<img class="icon" src="./images/smallicon/blacks/16x16/align_just.png">', 'btn-lg' ) ;
		//'创建分区组'
		sdbjs.parts.dropDownBox.add( 'addGroupDropDown', htmlEncode( _languagePack['modsdbd']['leftPanel']['titleDrop'][0] ), true, function( obj ){
			sdbjs.fun.addClick( obj, 'openCreateGroupModal()' ) ;
		} ) ;
		sdbjs.fun.setClass( 'addGroupDropDown', 'pull-right' ) ;
		sdbjs.parts.divBox.create( panelTitle['name'], 'businessNameDiv' ) ;
		sdbjs.fun.setCSS( 'businessNameDiv', { 'width': 370, 'text-overflow': 'ellipsis', 'overflow': 'hidden', 'white-space': 'nowrap' } ) ;
		sdbjs.parts.divBox.update( 'businessNameDiv', function( divObj ){
			//'业务：'
			$( divObj ).text( _languagePack['modsdbd']['leftPanel']['title'] + _businessConfig['BusinessName'] ) ;
		} ) ;
		
	}, function( panelBody ){
		
		sdbjs.parts.divBox.create( panelBody['name'], 'groupTopDiv', 'auto', 50 ) ;
		sdbjs.parts.alertBox.create( 'groupTopDiv', 'nodeConfTips' ) ;
		sdbjs.fun.setCSS( 'nodeConfTips', { 'padding': 10, 'margin-bottom': 5 } ) ;
		//'提示：批量配置节点信息说明。详情请点击' '帮助'
		sdbjs.parts.alertBox.update( 'nodeConfTips', sdbjs.fun.sprintf( '?<a href="#" data-toggle="modalBox" data-target="nodeConfHelp">?</a>', htmlEncode( _languagePack['modsdbd']['leftPanel']['tip'][0] ), htmlEncode( _languagePack['modsdbd']['leftPanel']['tip'][1] ) ), 'info' ) ;
		
		sdbjs.parts.divBox.create( panelBody['name'], 'groupBottomDiv', 'auto', 'variable' ) ;
		sdbjs.parts.tabList.create( 'groupBottomDiv', 'groupTabList', 'auto', 'variable' ) ;
	} ) ;

	/* 右边框架 */
	sdbjs.parts.divBox.create( 'middle', 'middle-right', 'variable', 'variable' ) ;
	sdbjs.fun.setCSS( 'middle-right', { 'float': 'left', 'padding': '10px', 'padding-left': 0 } ) ;
	
	/* 右边 主机列表 */
	sdbjs.parts.panelBox.create( 'middle-right', 'nodeInfoBar', 'auto', 'variable' ) ;
	sdbjs.fun.setCSS( 'nodeInfoBar', { 'overflow': 'auto', 'position': 'relative' } ) ;
	//'节点列表'
	sdbjs.parts.panelBox.update( 'nodeInfoBar', htmlEncode( _languagePack['modsdbd']['rightPanel']['title'] ), function( panelBody ){
		sdbjs.parts.divBox.create( panelBody['name'], 'nodeTopDiv', 'auto', 47 ) ;
		sdbjs.parts.divBox.update( 'nodeTopDiv', function( divObj ){
			//编辑
			$( divObj ).append( '<button class="btn btn-default" onclick="editNodeList(this)" id="editButton">' + _languagePack['modsdbd']['rightPanel']['button']['button'][0] + '</button>' ) ;
			$( divObj ).append( '&nbsp;&nbsp;' ) ;
			sdbjs.parts.dropDownBox.create( divObj, 'nodeSelect' ) ;
			//'选择操作'
			sdbjs.parts.dropDownBox.update( 'nodeSelect', htmlEncode( _languagePack['modsdbd']['rightPanel']['button']['dropDown'][0]['button'] ) ) ;
			//'全选'
			sdbjs.parts.dropDownBox.add( 'nodeSelect', htmlEncode( _languagePack['modsdbd']['rightPanel']['button']['dropDown'][0]['menu'][0] ), true, 'selectAll()' ) ;
			//'反选'
			sdbjs.parts.dropDownBox.add( 'nodeSelect', htmlEncode( _languagePack['modsdbd']['rightPanel']['button']['dropDown'][0]['menu'][1] ), true, 'unSelectAll()' ) ;
			$( divObj ).append( '&nbsp;&nbsp;' ) ;
			sdbjs.parts.dropDownBox.create( divObj, 'nodeOperation' ) ;
			//'已选定操作'
			sdbjs.parts.dropDownBox.update( 'nodeOperation', htmlEncode( _languagePack['modsdbd']['rightPanel']['button']['dropDown'][1]['button'] ) ) ;
			//'批量修改节点配置'
			sdbjs.parts.dropDownBox.add( 'nodeOperation', htmlEncode( _languagePack['modsdbd']['rightPanel']['button']['dropDown'][1]['menu'][0] ), true, 'openNodeParaModal("1",0)' ) ;
		} ) ;
		sdbjs.parts.divBox.create( panelBody['name'], 'nodeBottomDiv', 'auto', 'variable' ) ;
		sdbjs.parts.gridBox.create( 'nodeBottomDiv', 'nodeListGrid', 'auto', 'variable' ) ;
		sdbjs.parts.gridBox.addTitle( 'nodeListGrid', [{ 'text': function( tdObj ){
			//'主机名' '端口' '数据路径' '角色' '分区组'
			$( tdObj ).css( 'height', 19 ).html( '<input style="display:none;visibility:hidden;" type="checkbox">' ) ;
		}, 'width': '0%' },
																	  { 'text': htmlEncode( _languagePack['modsdbd']['rightPanel']['grid'][0] ), 'width': '20%' },
																	  { 'text': htmlEncode( _languagePack['modsdbd']['rightPanel']['grid'][1] ), 'width': '15%' },
																	  { 'text': htmlEncode( _languagePack['modsdbd']['rightPanel']['grid'][2] ), 'width': '30%' },
																	  { 'text': htmlEncode( _languagePack['modsdbd']['rightPanel']['grid'][3] ), 'width': '15%' },
																	  { 'text': htmlEncode( _languagePack['modsdbd']['rightPanel']['grid'][4] ), 'width': '20%' } ]  ) ;
		sdbjs.parts.gridBox.addTitle( 'nodeListGrid', [{ 'text': '<input style="display:none;visibility:hidden;" type="checkbox">', 'width': '0%' },
																	  { 'text': '<input class="form-control" type="search" id="hostnamefilter">', 'width': '20%' },
																	  { 'text': '<input class="form-control" type="search" id="svcnamefilter">', 'width': '15%' },
																	  { 'text': '<input class="form-control" type="search" id="dbpathfilter">', 'width': '30%' },
																	  { 'text': function( tdObj ){
																		  sdbjs.parts.selectBox.create( tdObj, 'roleSelect' ) ;
																		  //'全部'
																		  sdbjs.parts.selectBox.add( 'roleSelect', _languagePack['modsdbd']['rightPanel']['select'][0], 'all' ) ;
																		  sdbjs.parts.selectBox.add( 'roleSelect', 'coord', 'coord' ) ;
																		  sdbjs.parts.selectBox.add( 'roleSelect', 'catalog', 'catalog' ) ;
																		  sdbjs.parts.selectBox.add( 'roleSelect', 'data', 'data' ) ;
																	  }, 'width': '15%' },
																	  { 'text': function( tdObj ){
																		  sdbjs.parts.selectBox.create( tdObj, 'groupSelect' ) ;
																		  //'全部'
																		  sdbjs.parts.selectBox.add( 'groupSelect', _languagePack['modsdbd']['rightPanel']['select'][1], 'all' ) ;
																	  }, 'width': '20%' } ]  ) ;
		$( '#hostnamefilter' ).on( 'input propertychange', function(){
			filterNodeList() ;
		} ) ;
		$( '#svcnamefilter' ).on( 'input propertychange', function(){
			filterNodeList() ;
		} ) ;
		$( '#dbpathfilter' ).on( 'input propertychange', function(){
			filterNodeList() ;
		} ) ;
		var selectNode_1 = sdbjs.fun.getNode( 'roleSelect', 'selectBox' ) ;
		var selectNode_2 = sdbjs.fun.getNode( 'groupSelect', 'selectBox' ) ;
		$( selectNode_1['obj'] ).change( function(){
			filterNodeList() ;
		} ) ;
		$( selectNode_2['obj'] ).change( function(){
			filterNodeList() ;
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
	
	//创建分区组的弹窗
	sdbjs.parts.modalBox.create( $( document.body ), 'createGroup' ) ;
	//'创建分区组'
	sdbjs.parts.modalBox.update( 'createGroup', htmlEncode( _languagePack['modsdbd']['createGroupModal']['title'] ), function( bodyObj ){
		sdbjs.parts.tableBox.create( bodyObj, 'createGroupTable' ) ;
		sdbjs.parts.tableBox.update( 'createGroupTable', 'loosen' ) ;
		//'分区组名：' '创建的分区组名'
		sdbjs.parts.tableBox.addBody( 'createGroupTable', [{ 'text': htmlEncode( _languagePack['modsdbd']['createGroupModal']['table'][0] ), 'width': 150 },
																			{ 'text': '<input class="form-control" type="text" id="createGroupName">' },
																			{ 'text': htmlEncode( _languagePack['modsdbd']['createGroupModal']['table'][1] ) } ] ) ;
	}, function( footObj ){
		sdbjs.parts.tableBox.create( footObj, 'createGroupFootTable' ) ;
		sdbjs.parts.tableBox.addBody( 'createGroupFootTable', [{ 'text': function( tdObj ){
																					sdbjs.parts.alertBox.create( tdObj, 'createGroupAlert' ) ;
																					sdbjs.fun.setCSS( 'createGroupAlert', { 'display': 'none', 'padding': '8px', 'text-align': 'left' } ) ;
																				} },
																				{ 'text': function( tdObj ){
																					$( tdObj ).css( 'text-align', 'right' ) ;
																					sdbjs.parts.buttonBox.create( tdObj, 'createGroupOK' ) ;
																					$( tdObj ).append( '&nbsp;' ) ;
																					sdbjs.parts.buttonBox.create( tdObj, 'createGroupClose' ) ;
																					//'确定'
																					sdbjs.parts.buttonBox.update( 'createGroupOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'createGroup()' ) ;
																					//'关闭'
																					sdbjs.parts.buttonBox.update( 'createGroupClose', function( buttonObj ){
																						$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'createGroup' ) ;
																					}, 'primary' ) ;
																				}, 'width': 120  } ] ) ;
	} ) ;
	
	//创建节点的弹窗
	sdbjs.parts.modalBox.create( $( document.body ), 'createNode' ) ;
	//'添加节点'
	sdbjs.parts.modalBox.update( 'createNode', htmlEncode( _languagePack['modsdbd']['createNodeModal']['title'] ), function( bodyObj ){
		sdbjs.parts.tableBox.create( bodyObj, 'createNodeTable' ) ;
		sdbjs.parts.tableBox.update( 'createNodeTable', 'loosen' ) ;
		//'创建节点模式：'
		//'默认配置创建' '无配置创建' '复制节点配置创建'
		sdbjs.parts.tableBox.addBody( 'createNodeTable', [{ 'text': htmlEncode( _languagePack['modsdbd']['createNodeModal']['table'][0] ), 'width': 180 },
																		  { 'text': function( tdObj ){
																			  sdbjs.parts.selectBox.create( tdObj, 'nodeModelSelect' ) ;
																			  sdbjs.parts.selectBox.add( 'nodeModelSelect', _languagePack['modsdbd']['createNodeModal']['select'][0], '0' ) ;
																			  sdbjs.parts.selectBox.add( 'nodeModelSelect', _languagePack['modsdbd']['createNodeModal']['select'][1], '1' ) ;
																			  sdbjs.parts.selectBox.add( 'nodeModelSelect', _languagePack['modsdbd']['createNodeModal']['select'][2], '2' ) ;
																		  } } ] ) ;
		//'节点的主机名：'
		sdbjs.parts.tableBox.addBody( 'createNodeTable', [{ 'text': htmlEncode( _languagePack['modsdbd']['createNodeModal']['table'][1] ), 'width': 100 },
																		  { 'text': function( tdObj ){
																			  sdbjs.parts.selectBox.create( tdObj, 'nodeHostSelect' ) ;
																		  } } ] ) ;
		//'复制的节点：'
		sdbjs.parts.tableBox.addBody( 'createNodeTable', [{ 'text': function( tdObj ){
																			  $( tdObj ).text( _languagePack['modsdbd']['createNodeModal']['table'][2] ).hide() ;
																		  }, 'width': 100 },
																		  { 'text': function( tdObj ){
																			  sdbjs.parts.selectBox.create( tdObj, 'nodeCopySelect' ) ;
																			  $( tdObj ).hide() ;
																		  } } ] ) ;
		var selectObj = sdbjs.fun.getNode( 'nodeModelSelect', 'selectBox' ) ;
		$( selectObj['obj'] ).change( function(){
			if( $( this ).val() === '2' )
			{
				sdbjs.parts.tableBox.updateBody( 'createNodeTable', 2, 0, function( tdObj ){
					$( tdObj ).show() ;
				} ) ;
				sdbjs.parts.tableBox.updateBody( 'createNodeTable', 2, 1, function( tdObj ){
					$( tdObj ).show() ;
				} ) ;
			}
			else
			{
				sdbjs.parts.tableBox.updateBody( 'createNodeTable', 2, 0, function( tdObj ){
					$( tdObj ).hide() ;
				} ) ;
				sdbjs.parts.tableBox.updateBody( 'createNodeTable', 2, 1, function( tdObj ){
					$( tdObj ).hide() ;
				} ) ;
			}
		} ) ;
	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'createNodeOK' ) ;
		$( footObj ).append( '&nbsp;' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'createNodeClose' ) ;
		//'确定'
		sdbjs.parts.buttonBox.update( 'createNodeOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, '' ) ;
		sdbjs.parts.buttonBox.update( 'createNodeClose', function( buttonObj ){
			//'关闭'
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'createNode' ) ;
		}, 'primary' ) ;
	} ) ;
	
	//删除节点的弹窗
	sdbjs.parts.modalBox.create( $( document.body ), 'removeNode' ) ;
	//'删除节点'
	sdbjs.parts.modalBox.update( 'removeNode', htmlEncode( _languagePack['modsdbd']['removeNodeModal']['title'] ), function( bodyObj ){
		sdbjs.parts.tableBox.create( bodyObj, 'removeNodeTable' ) ;
		sdbjs.parts.tableBox.update( 'removeNodeTable', 'loosen' ) ;
		//'删除的节点：'
		sdbjs.parts.tableBox.addBody( 'removeNodeTable', [{ 'text': htmlEncode( _languagePack['modsdbd']['removeNodeModal']['table'][0] ), 'width': 100 },
																		  { 'text': function( tdObj ){
																			  sdbjs.parts.selectBox.create( tdObj, 'nodeRemoveSelect' ) ;
																		  } } ] ) ;
	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'removeNodeOK' ) ;
		$( footObj ).append( '&nbsp;' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'removeNodeClose' ) ;
		//'确定'
		sdbjs.parts.buttonBox.update( 'removeNodeOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, '' ) ;
		sdbjs.parts.buttonBox.update( 'removeNodeClose', function( buttonObj ){
			//'关闭'
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'removeNode' ) ;
		}, 'primary' ) ;
	} ) ;

	//修改节点配置弹窗
	sdbjs.parts.modalBox.create( $( document.body ), 'modNodeConf' ) ;
	//'修改节点'
	sdbjs.parts.modalBox.update( 'modNodeConf', htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['title'] ), function( bodyObj ){
		sdbjs.parts.navTabBox.create( bodyObj, 'modNodeConfTab' ) ;
		//'普通'
		sdbjs.parts.navTabBox.add( 'modNodeConfTab', htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['nav'][0] ), function( divObj ){
			sdbjs.parts.gridBox.create( divObj, 'modNodeConfGrid' ) ;
			//'属性' '值' '说明'
			sdbjs.parts.gridBox.addTitle( 'modNodeConfGrid', [{ 'text': htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['grid'][0] ), 'width': '20%' },
																			  { 'text': htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['grid'][1] ), 'width': '40%' },
																			  { 'text': htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['grid'][2] ), 'width': '40%' } ]  ) ;
		}, 'redrawNodePataModal()' ) ;
		//'高级'
		sdbjs.parts.navTabBox.add( 'modNodeConfTab', htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['nav'][1] ), function( divObj ){
			sdbjs.parts.gridBox.create( divObj, 'modNodeConfGrid2' ) ;
			//'属性' '值' '说明'
			sdbjs.parts.gridBox.addTitle( 'modNodeConfGrid2', [{ 'text': htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['grid'][0] ), 'width': '20%' },
																				{ 'text': htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['grid'][1] ), 'width': '40%' },
																				{ 'text': htmlEncode( _languagePack['modsdbd']['modNodeConfModal']['grid'][2] ), 'width': '40%' } ]  ) ;
		}, 'redrawNodePataModal()' ) ;
	}, function( footObj ){
		
		sdbjs.parts.tableBox.create( footObj, 'modNodeFootTable' ) ;
		sdbjs.parts.tableBox.addBody( 'modNodeFootTable', [{ 'text': function( tdObj ){
																				sdbjs.parts.alertBox.create( tdObj, 'modNodeFootTableAlert' ) ;
																				sdbjs.fun.setCSS( 'modNodeFootTableAlert', { 'display': 'none', 'padding': '8px', 'text-align': 'left' } ) ;
																			} },
																			{ 'text': function( tdObj ){
																				$( tdObj ).css( 'text-align', 'right' ) ;
																				sdbjs.parts.buttonBox.create( tdObj, 'modNodeConfOK' ) ;
																				$( tdObj ).append( '&nbsp;' ) ;
																				sdbjs.parts.buttonBox.create( tdObj, 'modNodeConfClose' ) ;
																				//'确定'
																				sdbjs.parts.buttonBox.update( 'modNodeConfOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, '' ) ;
																				sdbjs.parts.buttonBox.update( 'modNodeConfClose', function( buttonObj ){
																					//'关闭'
																					$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'modNodeConf' ) ;
																				}, 'primary' ) ;
																			}, 'width': 120  } ] ) ;
	} ) ;
	
	/* 创建帮助的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'nodeConfHelp' ) ;
	//'帮助'
	sdbjs.parts.modalBox.update( 'nodeConfHelp', htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['title'] ), function( bodyObj ){
		//'关于?[已选定操作]-[修改节点配置]?。您可以使用特殊规则来?批量修改?节点的?服务名?和?数据路径?:'
		$( bodyObj ).append( sdbjs.fun.sprintf( htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][0] ), '<b>', '</b>', '<b>', '</b>', '<b>', '</b>', '<b>', '</b>' ) ) ;
		sdbjs.parts.tableBox.create( bodyObj, 'nodeConfHelpTable_1' ) ;
		sdbjs.fun.setCSS( 'nodeConfHelpTable_1', { 'margin-top': 10, 'line-height': '160%' } ) ;
		sdbjs.parts.tableBox.update( 'nodeConfHelpTable_1', 'loosen border' ) ;
		//'服务名规则' '规则：服务名[+步进] 或 服务名[-步进]。'
		sdbjs.parts.tableBox.addBody( 'nodeConfHelpTable_1', [ { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][0][0] ), 'colspan': 2 }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][0][1] ) } ] ) ;
		sdbjs.parts.tableBox.addBody( 'nodeConfHelpTable_1', [ { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][1][0] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][1][1] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][1][2] ) } ] ) ;
		sdbjs.parts.tableBox.addBody( 'nodeConfHelpTable_1', [ { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][2][0] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][2][1] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][2][2] ) } ] ) ;
		sdbjs.parts.tableBox.addBody( 'nodeConfHelpTable_1', [ { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][3][0] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][3][1] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][3][2] ) } ] ) ;
		sdbjs.parts.tableBox.addBody( 'nodeConfHelpTable_1', [ { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][4][0] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][4][1] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][1][4][2] ) } ] ) ;
		
		sdbjs.parts.tableBox.create( bodyObj, 'nodeConfHelpTable_2' ) ;
		sdbjs.fun.setCSS( 'nodeConfHelpTable_2', { 'margin-top': 10, 'line-height': '160%' } ) ;
		sdbjs.parts.tableBox.update( 'nodeConfHelpTable_2', 'loosen border' ) ;
		sdbjs.parts.tableBox.addBody( 'nodeConfHelpTable_2', [ { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][2][0][0] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][2][0][1] ) } ] ) ;
		sdbjs.parts.tableBox.addBody( 'nodeConfHelpTable_2', [ { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][2][1][0] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][2][1][1] ) } ] ) ;
		sdbjs.parts.tableBox.addBody( 'nodeConfHelpTable_2', [ { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][2][2][0] ) }, { 'text': htmlEncode( _languagePack['modsdbd']['nodeConfHelpModal']['body'][2][2][1] ) } ] ) ;

	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'hostSearchClose' ) ;
		sdbjs.parts.buttonBox.update( 'hostSearchClose', function( buttonObj ){
			//'关闭'
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'nodeConfHelp' ) ;
		}, 'primary' ) ;
	} ) ;
	
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
	if( _businessConfig['DeployMod'] !== 'distribution' )
	{
		rc = false ;
		gotoPage( 'index.html' ) ;
	}
	_businessPara = sdbjs.fun.getData( 'SdbConfigInfo' ) ;
	if( _businessPara !== null )
	{
		_businessPara = JSON.parse( _businessPara ) ;
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
		sdbjs.fun.saveData( 'SdbStep', 'modsdbd' ) ;
		createHtml() ;
		createDynamicHtml() ;
	}
} ) ;

$(window).resize(function(){
	redrawNodePataModal() ;
} ) ;