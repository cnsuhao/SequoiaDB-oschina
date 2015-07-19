/*! sdbjs v3.0 | (c) 2014 sdbjs, Inc. */
if( typeof( sdbjs ) !== 'undefined' ){ throw new Error('Sdbjs注册失败') }

var sdbjs = new Object() ;

/************************************** 公有变量 ****************************************/


// ============================ 函数列表 ============================ //

sdbjs.fun = new Object() ;

// ------------ 功能函数 ------------ //

//删除对象, 帮助内存回收
sdbjs.fun.free = null ;

//解析多个json
sdbjs.fun.parseJson = null ;

//获取浏览器类型和版本
sdbjs.fun.getBrowserInfo = null ;

//获取该浏览器是否兼容系统
sdbjs.fun.compatible = null ;

//设置浏览器存储方式
sdbjs.fun.setBrowserStorage = null ;

//获取浏览器可见宽度
sdbjs.fun.getWindowWidth = null ;

//获取浏览器可见高度
sdbjs.fun.getWindowHeight = null ;

//解析一个数值
sdbjs.fun.parseNumber = null ;

//格式化字符串
sdbjs.fun.sprintf = null ;

//判断是否是数组
sdbjs.fun.isArray = null ;

//获取浏览器语言
sdbjs.fun.getLanguage = null ;

//保存数据到本地
sdbjs.fun.saveData = null ;

//获取本地数据
sdbjs.fun.getData = null ;

//判断存在本地数据
sdbjs.fun.hasData = null ;

//删除本地数据
sdbjs.fun.delData = null ;

//设置小标签
sdbjs.fun.setLabel = null ;

//显示遮罩
sdbjs.fun.showScreen = null ;

//隐藏遮罩
sdbjs.fun.hideScreen = null ;

// ------------ 节点函数 ------------ //

//设置根节点
sdbjs.fun.setRootNode = null ;

//创建节点
sdbjs.fun.createNode = null ;

//获取节点
sdbjs.fun.getNode = null ;

//注册节点
sdbjs.fun.registerNode = null ;

//把节点append到dom
sdbjs.fun.appendNode = null ;

//添加节点
sdbjs.fun.addNode = null ;

//删除节点
sdbjs.fun.removeNode = null ;

//设置节点宽度和高度
sdbjs.fun.setNode = null ;

//重绘节点的子节点宽高
sdbjs.fun.nodeRepaint = null ;

//重绘该节点的父节点宽高
sdbjs.fun.nodeParentRepaint = null ;

//所有节点宽高
sdbjs.fun.allNodeRepaint = null ;

// ------------ dom函数 ------------ //

//设置css
sdbjs.fun.setCSS = null ;

//设置class
sdbjs.fun.setClass = null ;

//添加class
sdbjs.fun.addClass = null ;

//设置html
sdbjs.fun.setHtml = null ;

//为对象创建一个点击事件
sdbjs.fun.addClick = null ;

//点击事件处理
sdbjs.fun.clickEvent = null ;

// ============================ 组件列表 ============================ //

sdbjs.parts = new Object() ;

//div框架
sdbjs.parts.divBox = new Object() ;
sdbjs.parts.divBox.create = null ;
sdbjs.parts.divBox.update = null ;

//标签列表
sdbjs.parts.tabList = new Object() ;
sdbjs.parts.tabList.create = null ;
sdbjs.parts.tabList.add = null ;
sdbjs.parts.tabList.remove = null ;
sdbjs.parts.tabList.update = null ;
sdbjs.parts.tabList.active = null ;
sdbjs.parts.tabList.disable = null ;
sdbjs.parts.tabList.unActive = null ;
sdbjs.parts.tabList.unDisable  = null ;
sdbjs.parts.tabList.getStatus = null ;
sdbjs.parts.tabList.show = null ;
sdbjs.parts.tabList.hide = null ;

//小进度条(带文字)
sdbjs.parts.progressBox = new Object() ;
sdbjs.parts.progressBox.create = null ;
sdbjs.parts.progressBox.update = null ;

//大进度条(无文字)
sdbjs.parts.progressBox2 = new Object() ;
sdbjs.parts.progressBox2.create = null ;
sdbjs.parts.progressBox2.update = null ;

//遮罩
sdbjs.parts.screenBox = new Object() ;
sdbjs.parts.screenBox.create = null ;
sdbjs.parts.screenBox.update = null ;
sdbjs.parts.screenBox.show = null ;
sdbjs.parts.screenBox.hide = null ;

//模态框
sdbjs.parts.modalBox = new Object() ;
sdbjs.parts.modalBox.create = null ;
sdbjs.parts.modalBox.update = null ;
sdbjs.parts.modalBox.show = null ;
sdbjs.parts.modalBox.redraw = null ;
sdbjs.parts.modalBox.hide = null ;
sdbjs.parts.modalBox.isHidden = null ;
sdbjs.parts.modalBox.toggle = null ;

//徽章
sdbjs.parts.badgeBox = new Object() ;
sdbjs.parts.badgeBox.create = null ;
sdbjs.parts.badgeBox.update = null ;

//按钮
sdbjs.parts.buttonBox = new Object() ;
sdbjs.parts.buttonBox.create = null ;
sdbjs.parts.buttonBox.update = null ;

//下拉菜单
sdbjs.parts.dropDownBox = new Object() ;
sdbjs.parts.dropDownBox.create = null ;
sdbjs.parts.dropDownBox.update = null ;
sdbjs.parts.dropDownBox.add = null ;
sdbjs.parts.dropDownBox.updateMenu = null ;

//网格
sdbjs.parts.gridBox = new Object() ;
sdbjs.parts.gridBox.create = null ;
sdbjs.parts.gridBox.addTitle = null ;
sdbjs.parts.gridBox.updateTitle = null ;
sdbjs.parts.gridBox.addBody = null ;
sdbjs.parts.gridBox.updateBody = null ;
sdbjs.parts.gridBox.removeBody = null ;
sdbjs.parts.gridBox.emptyBody = null ;
sdbjs.parts.gridBox.showBody = null ;
sdbjs.parts.gridBox.showAllBody = null ;
sdbjs.parts.gridBox.hideBody = null ;
sdbjs.parts.gridBox.hideAllBody = null ;
sdbjs.parts.gridBox.repigment = null ;

//表格
sdbjs.parts.tableBox = new Object() ;
sdbjs.parts.tableBox.create = null ;
sdbjs.parts.tableBox.update = null ;
sdbjs.parts.tableBox.addHead = null ;
sdbjs.parts.tableBox.addBody = null ;
sdbjs.parts.tableBox.updateHead = null ;
sdbjs.parts.tableBox.updateBody = null ;

//折叠栏
sdbjs.parts.foldBox = new Object() ;
sdbjs.parts.foldBox.create = null ;
sdbjs.parts.foldBox.update = null ;
sdbjs.parts.foldBox.setDomain = null ;
sdbjs.parts.foldBox.toggle = null ;
sdbjs.parts.foldBox.show = null ;
sdbjs.parts.foldBox.hide = null ;

//面板
sdbjs.parts.panelBox = new Object() ;
sdbjs.parts.panelBox.create = null ;
sdbjs.parts.panelBox.update = null ;

//分页栏
sdbjs.parts.tabPageBox = new Object() ;
sdbjs.parts.tabPageBox.create = null ;
sdbjs.parts.tabPageBox.add = null ;

//下拉选择框
sdbjs.parts.selectBox = new Object() ;
sdbjs.parts.selectBox.create = null ;
sdbjs.parts.selectBox.add = null ;
sdbjs.parts.selectBox.get = null ;
sdbjs.parts.selectBox.set = null ;
sdbjs.parts.selectBox.remove = null ;
sdbjs.parts.selectBox.empty = null ;

//well面板
sdbjs.parts.wellBox = new Object() ;
sdbjs.parts.wellBox.create = null ;
sdbjs.parts.wellBox.update = null ;

//提示框
sdbjs.parts.alertBox = new Object() ;
sdbjs.parts.alertBox.create = null ;
sdbjs.parts.alertBox.update = null ;

//导航
sdbjs.parts.navBox = new Object() ;
sdbjs.parts.navBox.create = null ;
sdbjs.parts.navBox.addColum = null ;
sdbjs.parts.navBox.addColum2 = null ;
sdbjs.parts.navBox.addMenu = null ;

//加载中的图
sdbjs.parts.loadingBox = new Object() ;
sdbjs.parts.loadingBox.create = null ;
sdbjs.parts.loadingBox.update = null ;
sdbjs.parts.loadingBox.show = null ;
sdbjs.parts.loadingBox.hide = null ;

//标签页
sdbjs.parts.navTabBox = new Object() ;
sdbjs.parts.navTabBox.create = null ;
sdbjs.parts.navTabBox.add = null ;
sdbjs.parts.navTabBox.show = null ;

//通知框
sdbjs.parts.noticeBox = new Object() ;
sdbjs.parts.noticeBox.create = null ;
sdbjs.parts.noticeBox.update = null ;
sdbjs.parts.noticeBox.show = null ;
sdbjs.parts.noticeBox.hide = null ;

/************************************** 私有变量 ****************************************/

sdbjs.private = new Object() ;

//树结构，方便父子节点之间查找
sdbjs.private.tree = { 'flag': 'node', 'name': 'root', 'type': 'root', 'obj': $( document.body ),'child': null, 'data': null, 'widthType': 'auto', 'heightType': 'auto', width: 0, height: 0, 'widthVariable': false, 'heightVariable': false } ;

//列表结构，方便逐个节点名查找
sdbjs.private.array = { 'root': sdbjs.private.tree } ;

//本地存储方式
sdbjs.private.storageType = 'localStorage' ;

//userData的存储
sdbjs.private.userdata = null ;

//当前导航显示信息
sdbjs.private.navBox = null ;

//当前下拉菜单
sdbjs.private.dropDownBox = null ;

//折叠栏 控制锁
sdbjs.private.foldBox = {} ;

//遮罩对象
sdbjs.private.htmlScreen = null ;
//遮罩叠加
sdbjs.private.htmlScreenNum = 0 ;

//小标签
sdbjs.private.smallLabel = null ;

//弹窗的节点名
sdbjs.private.modalBox = null ;

//加载
sdbjs.private.loadingBox = 0 ;

/************************************** 功能函数 ****************************************/

/*
 * 删除对象, 帮助内存回收
 */
sdbjs.fun.free = function( obj )
{
	if ( typeof( obj ) === "object" )
	{
		for ( var key in obj )
		{
			if ( typeof( obj[key] ) === "object" && typeof( obj[key]['innerHTML'] ) === 'undefined' )
			{
				sdbjs.fun.free( obj[key] ) ;
			}
			obj[key] = null ;
		}
	}
	return null ;
}

/*
 * 解析json字符串
 * 返回数组json对象
 */
sdbjs.fun.parseJson = function( str )
{
	var json_array = [] ;
	var i = 0, len = str.length ;
	var char, level, isEsc, isString, start, end, subStr, json ;
	while( i < len )
	{
		while( i < len ){	char = str.charAt( i ) ;	if( char === '{' ){	break ;	}	++i ;	}
		level = 0, isEsc = false, isString = false, start = i ;
		while( i < len )
		{
			char = str.charAt( i ) ;
			if( isEsc ){	isEsc = false ;	}
			else
			{
				if( ( char === '{' || char === '[' ) && isString === false ){	++level ;	}
				else if( ( char === '}' || char === ']' ) && isString === false )
				{
					--level ;
					if( level === 0 )
					{
						++i ;
						end = i ;
						subStr = str.substring( start, end ) ;
						//try{	json = eval( '(' + subStr + ')' ) ;	json_array.push( json ) ;	}catch(e){}
						json = JSON.parse( subStr ) ;
						json_array.push( json ) ;
						break ;
					}
				}
				else if( char === '"' ){	isString = !isString ;	}
				else if( char === '\\' ){	isEsc = true ;	}
			}
			++i ;
		}
	}
	return json_array ;
}


/*
 * 获取浏览器类型和版本
 */
sdbjs.fun.getBrowserInfo = function()
{
	var agent = window.navigator.userAgent.toLowerCase() ;
	var regStr_ie = /msie [\d.]+;/gi ;
	var regStr_ff = /firefox\/[\d.]+/gi
	var regStr_chrome = /chrome\/[\d.]+/gi ;
	var regStr_saf = /safari\/[\d.]+/gi ;
	var temp = '' ;
	var info = [] ;
	if(agent.indexOf("msie") > 0)
	{
		temp = agent.match( regStr_ie ) ;
		info.push( 'ie' ) ;
	}
	else if(agent.indexOf("firefox") > 0)
	{
		temp = agent.match( regStr_ff ) ;
		info.push( 'firefox' ) ;
	}
	else if(agent.indexOf("chrome") > 0)
	{
		temp = agent.match( regStr_chrome ) ;
		info.push( 'chrome' ) ;
	}
	else if(agent.indexOf("safari") > 0 && agent.indexOf("chrome") < 0)
	{
		temp = agent.match( regStr_saf ) ;
		info.push( 'safari' ) ;
	}
	else
	{
		if(agent.indexOf("trident") > 0 && agent.indexOf("rv") > 0)
		{
			info.push( 'ie' ) ;
			temp = '11' ;
		}
		else
		{
			temp = '0' ;
			info.push( 'unknow' ) ;
		}
	}
	verinfo = ( temp + '' ).replace(/[^0-9.]/ig, '' ) ;
	info.push( parseInt( verinfo ) ) ;
	return info ;
}

/*
 * 获取该浏览器是否兼容系统
 */
sdbjs.fun.compatible = function()
{
	var rc = true ;
	var browser = sdbjs.fun.getBrowserInfo() ;
	if( browser[0] === 'ie' && browser[1] < 7 )
	{
		rc = false ;
	}
	else if( browser[0] === 'firefox' && browser[1] < 22 )
	{
		rc = false ;
	}
	else if( browser[0] === 'chrome' && browser[1] < 17 )
	{
		rc = false ;
	}
	else if( browser[0] === 'safari' && browser[1] < 6 )
	{
		rc = false ;
	}
	var rj = { 'rc': rc, 'browser': browser[0], 'version': browser[1] } ;
	browser = sdbjs.fun.free( browser ) ;
	return rj ;
}

/*
 * 判断浏览器可以使用什么存储方式
 */
sdbjs.fun.setBrowserStorage = function()
{
	var browser = sdbjs.fun.getBrowserInfo() ;
	if( browser[0] === 'ie' && browser[1] <= 7 )
	{
		sdbjs.private.storageType = 'userData' ;
		var obj = document.createElement( 'input' ) ;
		obj.type = 'hidden' ;
		obj.style.display = 'none' ;
		obj.addBehavior( '#default#userData' ) ;
		$( document.body ).append( $( obj ) ) ;
		sdbjs.private.userdata = obj ;
	}
	else
	{
		if( window.localStorage )
		{
			sdbjs.private.storageType = 'localStorage' ;
		}
		else
		{
			if( navigator.cookieEnabled === true )
			{
				sdbjs.private.storageType = 'cookie' ;
			}
			else
			{
				sdbjs.private.storageType = '' ;
			}
		}
	}
	browser = sdbjs.fun.free( browser ) ;
	return sdbjs.private.storageType ;
}

/*
 * 获取浏览器可见宽度
 */
sdbjs.fun.getWindowWidth = function()
{
	var width = $( window ).width() ;
	if ( width < 970 )
	{
		if ( width === 0 )
		{
			throw new Error('获取浏览器宽度失败') ;
		}
		width = 970 ;
	}
	return width ;
}

/*
 * 获取浏览器可见高度
 */
sdbjs.fun.getWindowHeight = function()
{
	var height = $( window ).height() ;
	if ( height < 600 )
	{
		if ( height === 0 )
		{
			throw new Error('获取浏览器高度失败') ;
		}
		height = 600 ;
	}
	return height ;
}


/*
 * 保存数据到本地
 */
sdbjs.fun.saveData = function( key, value )
{
	if( sdbjs.private.storageType === 'userData' )
	{
		var saveTime = new Date() ;
		saveTime.setDate( saveTime.getDate() + 365 ) ;
		sdbjs.private.userdata.expires = saveTime.toUTCString() ;
		sdbjs.private.userdata.load( 'sdbjs' ) ;
		sdbjs.private.userdata.setAttribute( key, value ) ;
		sdbjs.private.userdata.save( 'sdbjs' ) ;
	}
	else if ( sdbjs.private.storageType === 'localStorage' )
	{
		window.localStorage.setItem( key, value ) ;
	}
	else if ( sdbjs.private.storageType === 'cookie' )
	{
		var saveTime = new Date() ;
		saveTime.setDate( saveTime.getDate() + 365 ) ;
		saveTime = saveTime.toUTCString() ;
		$.cookie( key, value, { 'expires': saveTime } ) ;
	}
}

/*
 * 获取本地数据
 */
sdbjs.fun.getData = function( key )
{
	var value = null ;
	if( sdbjs.private.storageType === 'userData' )
	{
		sdbjs.private.userdata.load( 'sdbjs' );
		value = sdbjs.private.userdata.getAttribute( key ) ;
	}
	else if ( sdbjs.private.storageType === 'localStorage' )
	{
		value = window.localStorage.getItem( key ) ;
	}
	else if ( sdbjs.private.storageType === 'cookie' )
	{
		value = $.cookie( key ) ;
	}
	return value ;
}

/*
 * 判断本地数据是否存在
 */
sdbjs.fun.hasData = function( key )
{
	var value = null ;
	if( sdbjs.private.storageType === 'userData' )
	{
		sdbjs.private.userdata.load( 'sdbjs' );
		value = sdbjs.private.userdata.getAttribute( key ) ;
	}
	else if ( sdbjs.private.storageType === 'localStorage' )
	{
		value = window.localStorage.getItem( key ) ;
	}
	else if ( sdbjs.private.storageType === 'cookie' )
	{
		value = $.cookie( key ) ;
	}
	return ( value !== null ) ;
}

/*
 * 删除本地数据
 */
sdbjs.fun.delData = function( key )
{
	if( sdbjs.private.storageType === 'userData' )
	{
		sdbjs.private.userdata.load( 'sdbjs' );
		sdbjs.private.userdata.removeAttribute( key ) ;
		sdbjs.private.userdata.save( 'sdbjs' ) ;
	}
	else if ( sdbjs.private.storageType === 'localStorage' )
	{
		window.localStorage.removeItem( key ) ;
	}
	else if ( sdbjs.private.storageType === 'cookie' )
	{
		$.removeCookie( key ) ;
	}
}

/*
 * 设置小标签
 */
sdbjs.fun.setLabel = function( nodeName, text )
{
	if( typeof( text ) === 'string' )
	{
		if( typeof( nodeName ) === 'object' )
		{
			var obj = nodeName ;
			$( obj ).attr( 'data-mouse', 'labelBox' ).data( 'desc', text ) ;
		}
		else if( typeof( nodeName ) === 'string' )
		{
			var obj = sdbjs.fun.getNode( nodeName ) ;
			$( obj['obj'] ).attr( 'data-mouse', 'labelBox' ).data( 'desc', text ) ;
		}
	}
}

/*
 * 显示遮罩
 */
sdbjs.fun.showScreen = function()
{
	++sdbjs.private.htmlScreenNum ;
	$( sdbjs.private.htmlScreen ).show() ;
}

/*
 * 隐藏遮罩
 */
sdbjs.fun.hideScreen = function()
{
	--sdbjs.private.htmlScreenNum ;
	if( sdbjs.private.htmlScreenNum === 0 )
	{
		$( sdbjs.private.htmlScreen ).hide() ;
	}
}

/* ================================================================== */

/*
 * 为对象创建一个点击事件
 */
sdbjs.fun.addClick = function( obj, functionStr )
{
	if( typeof( functionStr ) === 'string' )
	{
		if( typeof( obj ) === 'object' )
		{
			$( obj ).get(0).onclick = Function( functionStr ) ;
		}
		else if( typeof( obj ) === 'string' )
		{
			var nodeObj = sdbjs.fun.getNode( obj ) ;
			$( nodeObj['obj'] ).get(0).onclick = Function( functionStr ) ;
		}
	}
}

/*
 * 解析数值
 */
sdbjs.fun.parseNumber = function( number, type, minValue, maxValue )
{
	var value = null ;
	var temp = null ;
	if( typeof( type ) === 'undefined' || type === 'int' )
	{
		value = parseInt( number ) ;
	}
	else if( type === 'float' )
	{
		value = parseFloat( number ) ;
	}
	if( typeof( minValue ) !== 'undefined' )
	{
		if( type === 'int' )
		{
			temp = parseInt( minValue ) ;
		}
		else if( type === 'float' )
		{
			temp = parseFloat( minValue ) ;
		}
		if( temp > value )
		{
			value = temp ;
		}
	}
	if( typeof( maxValue ) !== 'undefined' )
	{
		if( type === 'int' )
		{
			temp = parseInt( maxValue ) ;
		}
		else if( type === 'float' )
		{
			temp = parseFloat( maxValue ) ;
		}
		if( temp < value )
		{
			value = temp ;
		}
	}
	return value ;
}

/*
 * 把格式化的字符串写入一个变量中。
 */
sdbjs.fun.sprintf = function( format )
{
	var len = arguments.length;
	var strLen = format.length ;
	var newStr = '' ;
	for( var i = 0, k = 1; i < strLen; ++i )
	{
		var char = format.charAt( i ) ;
		if( char == '\\' && ( i + 1 < strLen ) && format.charAt( i + 1 ) == '?' )
		{
			newStr += '?' ;
			++i ;
		}
		else if( char == '?' && k < len )
		{
			newStr += ( '' + arguments[k] ) ;
			++k ;
		}
		else
		{
			newStr += char ;
		}
	}
	return newStr ;
}

/*
 * 判断是否为数组
 */
sdbjs.fun.isArray = function( obj )
{
	return Object.prototype.toString.call( obj ) === '[object Array]' ;
}

/*
 * 获取浏览器语言
 * 返回值列表		中文 zh-CN, 英文 en, 法语 fr, 德语 de, 日语 ja, 西班牙语 es, 意大利 it
 */
sdbjs.fun.getLanguage = function()
{
	var language = navigator.userLanguage ;
	if( typeof( language ) === 'undefined' )
	{
		language = navigator.language ;
	}
	language = language.substr( 0, 2 ) ;
	if( language === 'zh' )
	{
		language = 'zh-CN' ;
	}
	if( typeof( language ) === 'undefined' || language === 'undefined' )
	{
		language = 'en' ;
	}
	return language ;
}

/************************************** 节点函数 ****************************************/

/*
 * 设置根节点
 */
sdbjs.fun.setRootNode = function( nodeObj, width, height )
{
	var widthType = '' ;
	var heightType = '' ;
	if( typeof( width ) === 'undefined' || width === 'auto' )
	{
		widthType = 'auto' ;
		width = 0 ;
	}
	else if ( width === 'variable' )
	{
		widthType = 'variable' ;
		width = sdbjs.fun.getWindowWidth() ;
		$( nodeObj ).width( width ) ;
	}
	else if( typeof( width ) === 'number' )
	{
		widthType = 'fixe' ;
		$( nodeObj ).width( width ) ;
	}

	if( typeof( height ) === 'undefined' || height === 'auto' )
	{
		heightType = 'auto' ;
		height = 0 ;
	}
	else if ( height === 'variable' )
	{
		heightType = 'variable' ;
		height = sdbjs.fun.getWindowHeight() ;
		$( nodeObj ).height( height ) ;
	}
	else if( typeof( height ) === 'number' )
	{
		heightType = 'fixe' ;
		$( nodeObj ).height( height ) ;
	}
	sdbjs.private.tree['obj'] = nodeObj ;
	sdbjs.private.tree['widthType'] = widthType ;
	sdbjs.private.tree['width'] = width ;
	sdbjs.private.tree['heightType'] = heightType ;
	sdbjs.private.tree['height'] = height ;
}

/*
 * 创建节点
 */
sdbjs.fun.createNode = function( nodeName, nodeType, nodeObj, width, height )
{
	var widthType = '' ;
	var heightType = '' ;
	if( typeof( sdbjs.private.array[nodeName] ) !== 'undefined' )
	{
		throw new Error( '节点已经存在 ' + nodeName ) ;
	}
	
	if( typeof( width ) === 'undefined' || width === 'auto' )
	{
		widthType = 'auto' ;
		width = 0 ;
	}
	else if ( width === 'variable' )
	{
		widthType = 'variable' ;
		width = 0 ;
	}
	else if( typeof( width ) === 'number' )
	{
		widthType = 'fixe' ;
		$( nodeObj ).width( width ) ;
	}

	if( typeof( height ) === 'undefined' || height === 'auto' )
	{
		heightType = 'auto' ;
		height = 0 ;
	}
	else if ( height === 'variable' )
	{
		heightType = 'variable' ;
		height = 0 ;
	}
	else if( typeof( height ) === 'number' )
	{
		heightType = 'fixe' ;
		$( nodeObj ).height( height ) ;
	}

	return {
		'flag':			'node',
		'name':			nodeName,
		'type':			nodeType,
		'obj':			nodeObj,
		'parent':		null,
		'child':			null,
		'parts':			[],
		'status':		null,
		'data':			null,
		'widthType':	widthType,
		'heightType':	heightType,
		'width':			width,
		'height':		height,
		'widthVariable':	false,
		'heightVariable':	false
	} ;
}

/*
 * 获取节点
 */
sdbjs.fun.getNode = function( nodeName, type )
{	
	var node = sdbjs.private.array[nodeName] ;
	if( typeof( node ) === 'undefined' )
	{
		throw new Error( '不存在的节点 ' + nodeName ) ;
	}
	if( typeof( type ) !== 'undefined' )
	{
		if( node['type'] !== type )
		{
			throw new Error( '组件类型错误 ' + nodeName + ' 类型是 ' + node['type'] + ', 不是 ' + type ) ;
		}
	}
	return node ;
}

/*
 * 注册节点
 */
sdbjs.fun.registerNode = function( parentName, node )
{
	if( typeof( parentName ) === 'string' )
	{
		var parentNode = sdbjs.fun.getNode( parentName ) ;
		if( parentNode['type'] === 'progressBox' )	{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'progressBox2' )	{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'modalBox' )		{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'badgeBox' )		{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'buttonBox' )		{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'dropDownBox' )	{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'tableBox' )		{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'foldBox' )			{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'tabPageBox' )		{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'selectBox' )		{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'alertBox' )		{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'navBox' )			{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		if( parentNode['type'] === 'loadingBox' )		{ throw new Error( '父节点' + parentName + '类型是' + parentNode['type'] + ', 不能在它上面添加其他组件' ) ; }
		node['parent'] = parentNode ;
		if( parentNode['child'] === null )
		{
			parentNode['child'] = [] ;
		}
		parentNode['child'].push( node ) ;
		sdbjs.private.array[node['name']] = node ;
	}
	else
	{
		sdbjs.private.array[node['name']] = node ;
	}
}

/*
 * 把节点append到dom
 */
sdbjs.fun.appendNode = function( parentName, nodeName )
{
	if( typeof( parentName ) === 'string' )
	{
		var parentNode = sdbjs.fun.getNode( parentName ) ;
		var node = sdbjs.fun.getNode( nodeName ) ;
		$( parentNode['obj'] ).append( node['obj'] ) ;
		if( node['widthType'] === 'variable' )
		{
			parentNode['widthVariable'] = true ;
		}
		if( node['heightType'] === 'variable' )
		{
			parentNode['heightVariable'] = true ;
		}
	}
	else
	{
		var node = sdbjs.fun.getNode( nodeName ) ;
		$( parentName ).append( node['obj'] ) ;
	}
}

/*
 * 添加节点
 */
sdbjs.fun.addNode = function( parentName, nodeName, nodeType, nodeObj, width, height )
{
	var node = sdbjs.fun.createNode( nodeName, nodeType, nodeObj, width, height ) ;
	sdbjs.fun.registerNode( parentName, node ) ;
	sdbjs.fun.appendNode( parentName, nodeName ) ;
	sdbjs.fun.nodeParentRepaint( nodeName ) ;
	return node ;
}

/*
 * 删除节点
 */
sdbjs.fun.removeNode = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName ) ;
	if( node['type'] === 'gridBox' )
	{
		sdbjs.fun.removeNode( nodeName + '~header' ) ;
		sdbjs.fun.removeNode( nodeName + '~body' ) ;
	}
	if( node['type'] === 'panelBox' )
	{
		sdbjs.fun.removeNode( nodeName + '~title' ) ;
		sdbjs.fun.removeNode( nodeName + '~body' ) ;
	}
	var parentNode = node['parent'] ;
	if( parentNode !== null )
	{
		$.each( parentNode['child'], function( index, childNode ){
			if( childNode['name'] === nodeName )
			{
				parentNode['child'].splice( index, 1 ) ;
				return;
			}
		} ) ;
		node['parent'] = null ;
	}
	delete sdbjs.private.array[ nodeName ] ;
	if( node['type'] === 'dropDownBox' )
	{
		$( node['parts']['menu'] ).empty().remove() ;
	}
	$( node['obj'] ).empty().remove() ;
	if( parentNode !== null )
	{
		sdbjs.fun.nodeRepaint( parentNode ) ;
	}
}

/*
 * 设置节点宽度和高度
 */
sdbjs.fun.setNode = function( nodeName, width, height )
{
	var widthType = '' ;
	var heightType = '' ;
	if( typeof( width ) === 'undefined' || width === 'auto' )
	{
		widthType = 'auto' ;
		width = 0 ;
	}
	else if ( width === 'variable' )
	{
		widthType = 'variable' ;
		width = 0 ;
	}
	else if( typeof( width ) === 'number' )
	{
		widthType = 'fixe' ;
		$( nodeObj ).width( width ) ;
	}

	if( typeof( height ) === 'undefined' || height === 'auto' )
	{
		heightType = 'auto' ;
		height = 0 ;
	}
	else if ( height === 'variable' )
	{
		heightType = 'variable' ;
		height = 0 ;
	}
	else if( typeof( height ) === 'number' )
	{
		heightType = 'fixe' ;
		$( nodeObj ).height( height ) ;
	}
	var node = sdbjs.fun.getNode( nodeName ) ;
	node['widthType']  = widthType ;
	node['heightType'] = heightType ;
	node['width']      = width ;
	node['height']     = height ;
}

/*
 * 重绘节点的所有子节点宽高
 */
sdbjs.fun.nodeRepaint = function( node )
{
	if( node['widthVariable'] === true )
	{
		var variableNode = null ;
		var pWidth = $( node['obj'] ).width() ;
		var sWidth = 0 ;
		$.each( node['child'], function( index, childNode ){
			if( childNode['widthType'] === 'fixe' )
			{
				sWidth += childNode['width'] ;
			}
			else if ( childNode['widthType'] === 'variable' )
			{
				if( variableNode === null )
				{
					variableNode = childNode ;
				}
				else
				{
					throw new Error( variableNode['name'] + '节点的width已经是variable类型，' + childNode['name'] + '的width不是设置为variable类型' ) ;
				}
			}
			else
			{
				throw new Error( '有兄弟节点的width是variable类型，' + childNode['name'] + '的width必须是fix类型' ) ;
			}
		} ) ;
		variableNode['width'] = pWidth - sWidth ;
		sdbjs.fun.setCSS( variableNode['name'], { 'width': pWidth - sWidth } ) ;
	}
	if( node['heightVariable'] === true )
	{
		var variableNode = null ;
		var pHeight = $( node['obj'] ).height() ;
		var sHeight = 0 ;
		$.each( node['child'], function( index, childNode ){
			if( childNode['heightType'] === 'fixe' )
			{
				sHeight += childNode['height'] ;
			}
			else if ( childNode['heightType'] === 'variable' )
			{
				if( variableNode === null )
				{
					if ( $( childNode['obj'] ).css( 'float' ) === 'left' || $( childNode['obj'] ).css( 'float' ) === 'right' )
					{
						sdbjs.fun.setCSS( childNode['name'], { 'height': pHeight } ) ;
					}
					else if ( $( childNode['obj'] ).css( 'display' ) === 'none' )
					{
						sdbjs.fun.setCSS( childNode['name'], { 'height': pHeight } ) ;
					}
					else
					{
						variableNode = childNode ;
					}
				}
				else if ( $( childNode['obj'] ).css( 'display' ) === 'none' )
				{
					sdbjs.fun.setCSS( childNode['name'], { 'height': pHeight } ) ;
				}
				else
				{
					throw new Error( variableNode['name'] + '节点的height已经是variable类型，' + childNode['name'] + '的height不是设置为variable类型' ) ;
				}
			}
			else if ( childNode['heightType'] === 'auto' )
			{
				sHeight += $( childNode['obj'] ).outerHeight() ;
				//throw new Error( '有兄弟节点的height是variable类型，' + childNode['name'] + '的heifht必须是fix类型' ) ;
			}
		} ) ;
		if( variableNode !== null )
		{
			variableNode['height'] = pHeight - sHeight ;
			if( variableNode['type'] === 'gridBoxBody' || variableNode['type'] === 'tabList' || variableNode['type'] === 'navTabBoxBody' )
			{
				sdbjs.fun.setCSS( variableNode['name'], { 'max-height': pHeight - sHeight } ) ;
				$( variableNode['obj'] ).css( { 'height': 'auto' } ) ;
			}
			else
			{
				sdbjs.fun.setCSS( variableNode['name'], { 'height': pHeight - sHeight } ) ;
			}
		}
	}
}

/*
 * 重绘节点的所有兄弟宽高
 */
sdbjs.fun.nodeParentRepaint = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName ) ;
	if( node['parent'] !== null )
	{
		sdbjs.fun.nodeRepaint( node['parent'] ) ;
	}
}

/*
 * 所有节点重绘
 */
sdbjs.fun.allNodeRepaint = function( node )
{
	if ( typeof( node ) === 'undefined' )
	{
		node = sdbjs.private.tree ;
	}
	sdbjs.fun.nodeRepaint( node ) ;
	if( node['child'] !== null )
	{
		$.each( node['child'], function( index, childNode ){
			sdbjs.fun.allNodeRepaint( childNode ) ;
		} ) ;
	}
}

/************************************** dom函数 ****************************************/

/*
 * 设置css
 */
sdbjs.fun.setCSS = function( nodeName, css )
{
	var node = sdbjs.fun.getNode( nodeName ) ;
	$( node['obj'] ).css( css ) ;
	if( node['widthType'] === 'fixe' )
	{
		var nodeWidth = sdbjs.fun.parseNumber( $( node['obj'] ).outerWidth() ) ;
		if( nodeWidth !== node['width'] )
		{
			var width = sdbjs.fun.parseNumber( $( node['obj'] ).width() ) - ( nodeWidth - node['width'] ) ;
			$( node['obj'] ).width( width ) ;
		}
	}
	else if( node['widthType'] === 'variable' )
	{
		var nodeWidth = sdbjs.fun.parseNumber( $( node['obj'] ).outerWidth() ) ;
		if( nodeWidth !== node['width'] )
		{
			var width = sdbjs.fun.parseNumber( $( node['obj'] ).width() ) * 2 - nodeWidth ;
			$( node['obj'] ).width( width ) ;
		}
	}

	if( node['heightType'] === 'fixe' )
	{
		var nodeHeight = sdbjs.fun.parseNumber( $( node['obj'] ).outerHeight() ) ;
		if( nodeHeight !== node['height'] )
		{
			var height = sdbjs.fun.parseNumber( $( node['obj'] ).height() ) - ( nodeHeight - node['height'] ) ;
			$( node['obj'] ).height( height ) ;
		}
	}
	else if ( node['heightType'] === 'variable' )
	{
		if( node['type'] !== 'gridBoxBody' && node['type'] !== 'tabList' && node['type'] !== 'navTabBoxBody' )
		{
			var nodeHeight = sdbjs.fun.parseNumber( $( node['obj'] ).outerHeight() ) ;
			if( nodeHeight !== node['height'] )
			{
				var height = sdbjs.fun.parseNumber( $( node['obj'] ).height() )  * 2 - nodeHeight ;
				$( node['obj'] ).height( height ) ;
			}
		}
	}
}

/*
 * 设置class
 */
sdbjs.fun.setClass = function( dest, className )
{
	var obj = null ;
	if( typeof( dest ) === 'string' )
	{
		if( dest.charAt( 0 ) === '#' )
		{
			obj = $( dest ) ;
		}
		else
		{
			obj = sdbjs.fun.getNode( dest ) ;
			obj = obj['obj'] ;
		}
	}
	else
	{
		if( dest['flag'] === 'node' )
		{
			obj = dest['obj'] ;
		}
		else
		{
			obj = $( dest ) ;
		}
	}
	$( obj ).removeClass().addClass( className ) ;
}

/*
 * 添加class
 */
sdbjs.fun.addClass = function( dest, className )
{
	var obj = null ;
	if( typeof( dest ) === 'string' )
	{
		if( dest.charAt( 0 ) === '#' )
		{
			obj = $( dest ) ;
		}
		else
		{
			obj = sdbjs.fun.getNode( dest ) ;
			obj = obj['obj'] ;
		}
	}
	else
	{
		if( dest['flag'] === 'node' )
		{
			obj = dest['obj'] ;
		}
		else
		{
			obj = $( dest ) ;
		}
	}
	$( obj ).addClass( className ) ;
}

/*
 * 设置html
 */
sdbjs.fun.setHtml = function( dest, html )
{
	var obj = null ;
	if( typeof( dest ) === 'string' )
	{
		if( dest.charAt( 0 ) === '#' )
		{
			obj = $( dest ) ;
		}
		else
		{
			obj = sdbjs.fun.getNode( dest ) ;
			obj = obj['obj'] ;
		}
	}
	else
	{
		if( dest['flag'] === 'node' )
		{
			obj = dest['obj'] ;
		}
		else
		{
			obj = $( dest ) ;
		}
	}
	$( obj ).html( html ) ;
}

/**************************************** 组件 ****************************************/

/********************* div **************************/

/*
 * 创建
*/
sdbjs.parts.divBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ) ;
	sdbjs.fun.addNode( parentName, nodeName, 'divBox', newObj, width, height ) ;
}

/*
 * 更新
 */
sdbjs.parts.divBox.update = function( nodeName, text )
{
	var node = sdbjs.fun.getNode( nodeName, 'divBox' ) ;
	if( typeof( text ) === 'string' )
	{
		$( node['obj'] ).append( text ) ;
	}
	else if( typeof( text ) === 'function' )
	{
		text( node['obj'] ) ;
	}
}

/********************* 标签列表 **************************/

/*
 * 创建
*/
sdbjs.parts.tabList.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<ul></ul>' ).addClass( 'tab-list' ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'tabList', newObj, width, height ) ;
}

/*
 * 添加
 */
sdbjs.parts.tabList.add = function( nodeName, text, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	var li   = $( '<li></li>'   ).addClass( 'plain' ).appendTo( node['obj'] ) ;
	var div  = $( '<div></div>' ).appendTo( li ) ;
	if( typeof( text ) === 'string' )
	{
		$( div ).html( text ) ;
	}
	else if( typeof( text ) === 'function' )
	{
		text( div ) ;
	}
	if( typeof( fun ) === 'function' )
	{
		fun( li ) ;
	}
	else
	{
		sdbjs.fun.addClick( li, fun ) ;
	}
	node['parts'].push( li ) ;
	sdbjs.fun.allNodeRepaint( node ) ;
}

/*
 * 删除
 */
sdbjs.parts.tabList.remove = function( nodeName, line )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	$( node['parts'][line] ).empty().remove() ;
	node['parts'][line].splice( line, 1 ) ;
}

/*
 * 更新
 */
sdbjs.parts.tabList.update = function( nodeName, line, text, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	if( typeof( text ) === 'string' )
	{
		$( node['parts'][line] ).children( 'div' ).html( text ) ;
	}
	else if( typeof( text ) === 'function' )
	{
		text( $( node['parts'][line] ).children( 'div' ) ) ;
	}
	if( typeof( fun ) === 'function' )
	{
		fun( node['parts'][line] ) ;
	}
	else
	{
		sdbjs.fun.addClick( node['parts'][line], fun ) ;
	}
}

/*
 * 设置为激活样式
 */
sdbjs.parts.tabList.active = function( nodeName, num )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	$( node['parts'][num] ).addClass( 'active' ) ;
}

/*
 * 设置为禁用样式
 */
sdbjs.parts.tabList.disable = function( nodeName, num )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	$( node['parts'][num] ).addClass( 'off' ) ;
}

/*
 * 设置为普通样式
 */
sdbjs.parts.tabList.unActive = function( nodeName, num )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	$( node['parts'][num] ).removeClass( 'active' ) ;
}

/*
 * 设置为可以用
 */
sdbjs.parts.tabList.unDisable = function( nodeName, num )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	$( node['parts'][num] ).removeClass( 'off' ) ;
}

/*
 * 获取当前状态
 */
sdbjs.parts.tabList.getStatus = function( nodeName, num )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	var status = 'plain' ;
	if( $( node['parts'][num] ).hasClass( 'active' ) )
	{
		status += ' active' ;
	}
	if( $( node['parts'][num] ).hasClass( 'off' ) )
	{
		status += ' off' ;
	}
	return status ;
}

/*
 * 显示
 */
sdbjs.parts.tabList.show = function( nodeName, num )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	$( node['parts'][num] ).show() ;
}

/*
 * 隐藏
 */
sdbjs.parts.tabList.hide = function( nodeName, num )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabList' ) ;
	$( node['parts'][num] ).hide() ;
}

/********************* 小进度条(带文字) **************************/

/*
 * 创建
 */
sdbjs.parts.progressBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'progress' ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'progressBox', newObj, width, height ) ;
	node['parts'].push( $( '<span></span>' ).addClass( 'reading' ).html( '' ).appendTo( node['obj'] ) ) ;
	node['parts'].push( $( '<span></span>' ).addClass( 'bar green' ).css( 'width', '0%' ).appendTo( node['obj'] ) ) ;
}

/*
 * 更新进度条
 */
sdbjs.parts.progressBox.update = function( nodeName, percent, color, text )
{
	var node = sdbjs.fun.getNode( nodeName, 'progressBox' ) ;
	percent = sdbjs.fun.parseNumber( percent, 'int', 0, 100 ) ;
	$( node['parts'][0] ).html( text ) ;
	$( node['parts'][1] ).removeClass().addClass( 'bar ' + color ).css( 'width', percent + '%' ) ;
}

/********************* 大进度条(无文字) **************************/

/*
 * 创建
 */
sdbjs.parts.progressBox2.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'progress2' ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'progressBox2', newObj, width, height ) ;
	node['parts'].push( $( '<div></div>' ).addClass( 'bar blue' ).css( { 'width': '0%' } ).appendTo( node['obj'] ) ) ;
}

/*
 * 更新进度条
 */
sdbjs.parts.progressBox2.update = function( nodeName, color, percent )
{
	var node = sdbjs.fun.getNode( nodeName, 'progressBox2' ) ;
	percent = sdbjs.fun.parseNumber( percent, 'int', 0, 100 ) ;
	$( node['parts'][0] ).css( 'width', percent + '%' ) ;
	if( typeof( color ) === 'string' )
	{
		$( node['parts'][0] ).removeClass( 'green orange red blue' ).addClass( color ) ;
	}
}

/********************* 遮罩 **************************/

/*
 * 创建
 */
sdbjs.parts.screenBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'mask-screen alpha' ) ;
	sdbjs.fun.addNode( parentName, nodeName, 'screenBox', newObj, width, height ) ;
}

/*
 * 更新遮罩类型
 */
sdbjs.parts.screenBox.update = function( nodeName, type )
{
	var node = sdbjs.fun.getNode( nodeName, 'screenBox' ) ;
	$( node['obj'] ).removeClass().addClass( 'mask-screen ' + type ) ;
}

/*
 * 显示
 */
sdbjs.parts.screenBox.show = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'screenBox' ) ;
	$( node['obj'] ).show() ;
}

/*
 * 隐藏
 */
sdbjs.parts.screenBox.hide = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'screenBox' ) ;
	$( node['obj'] ).hide() ;
}

/********************* 模态框 **************************/

/*
 * 创建
 */
sdbjs.parts.modalBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'modal' ) ;
	var header = $( '<div></div>' ).addClass( 'header' ).appendTo( newObj ) ;
	var button = $( '<button></button>' ).html( '&times;' ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', nodeName ).addClass( 'close' ).appendTo( header ) ;
	var title = $( '<div></div>' ).appendTo( header ) ;
	var clear = $( '<div></div>' ).addClass( 'clear-float' ).appendTo( header ) ;
	var mbody = $( '<div></div>' ).addClass( 'body' ).appendTo( newObj ) ;
	var foot = $( '<div></div>' ).addClass( 'foot' ).appendTo( newObj ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'modalBox', newObj, width, height ) ;
	node['parts'].push( { 'header': header, 'title': title, 'body': mbody, 'foot': foot } ) ;
}

/*
 * 更新内容
 */
sdbjs.parts.modalBox.update = function( nodeName, title, mbody, foot )
{
	var node = sdbjs.fun.getNode( nodeName, 'modalBox' ) ;
	if( typeof( title ) === 'string' )
	{
		$( node['parts'][0]['title'] ).html( title ) ;
	}
	else if( typeof( title ) === 'function' )
	{
		title( node['parts'][0]['title'] ) ;
	}
	
	if( typeof( mbody ) === 'string' )
	{
		$( node['parts'][0]['body'] ).html( mbody ) ;
	}
	else if( typeof( mbody ) === 'function' )
	{
		mbody( node['parts'][0]['body'] ) ;
	}

	if( typeof( foot ) === 'string' )
	{
		$( node['parts'][0]['foot'] ).html( foot ) ;
	}
	else if( typeof( foot ) === 'function' )
	{
		foot( node['parts'][0]['foot'] ) ;
	}
}

/*
 * 调整位置
 */
sdbjs.parts.modalBox.redraw = function( nodeName, width, height )
{
	var node = sdbjs.fun.getNode( nodeName, 'modalBox' ) ;
	width  = typeof( width ) === 'undefined'  ? sdbjs.fun.getWindowWidth()  : width ;
	height = typeof( height ) === 'undefined' ? sdbjs.fun.getWindowHeight() : height ;
	
	$( node['parts'][0]['body'] ).css( { 'height': 'auto' } ) ;
	
	var top = 0 ;
	var left = parseInt( width * 0.5 ) - parseInt( $( node['obj'] ).width() * 0.5 ) ;
	var modalHeight = $( node['obj'] ).outerHeight() ;
	
	if( modalHeight > height )
	{
		top = 10 ;
		var modalBodyHeight = $( node['parts'][0]['body'] ).height() - 40 - ( modalHeight - height ) ;
		$( node['parts'][0]['body'] ).css( { 'height': modalBodyHeight } ) ;
	}
	else
	{
		top = parseInt( ( height - modalHeight ) * 0.5 ) ;
	}

	$( node['obj'] ).css( { 'left': left, 'top': top } ) ;
}

/*
 * 显示
 */
sdbjs.parts.modalBox.show = function ( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'modalBox' ) ;
	sdbjs.fun.showScreen() ;
	$( node['obj'] ).show() ;
	sdbjs.parts.modalBox.redraw( nodeName ) ;
	sdbjs.private.modalBox = nodeName ;
}

/*
 * 隐藏
 */
sdbjs.parts.modalBox.hide = function ( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'modalBox' ) ;
	$( node['obj'] ).hide() ;
	sdbjs.fun.hideScreen() ;
	sdbjs.private.modalBox = null ;
}

/*
 * 是否隐藏
 */
sdbjs.parts.modalBox.isHidden = function ( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'modalBox' ) ;
	if( $( node['obj'] ).is( ':hidden' ) )
	{
		return true ;
	}
	else
	{
		return false ;
	}
}

/*
 * 切换
 */
sdbjs.parts.modalBox.toggle = function ( nodeName )
{
	if( sdbjs.parts.modalBox.isHidden( nodeName ) )
	{
		sdbjs.parts.modalBox.show( nodeName ) ;
	}
	else
	{
		sdbjs.parts.modalBox.hide( nodeName ) ;
	}
}

/********************* 徽章 **************************/

/*
 * 创建
 */
sdbjs.parts.badgeBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<span></span>' ).addClass( 'badge badge-info' ) ;
	sdbjs.fun.addNode( parentName, nodeName, 'badgeBox', newObj, width, height ) ;
}

/*
 * 更新
 */
sdbjs.parts.badgeBox.update = function( nodeName, text, type )
{
	var node = sdbjs.fun.getNode( nodeName, 'badgeBox' ) ;
	$( node['obj'] ).removeClass().addClass( 'badge badge-' + type ).html( text ) ;
}

/********************* 按钮 **************************/

/*
 * 创建
 */
sdbjs.parts.buttonBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<button></button>' ).addClass( 'btn btn-default' ) ;
	sdbjs.fun.addNode( parentName, nodeName, 'buttonBox', newObj, width, height ) ;
}

/*
 * 更新
 */
sdbjs.parts.buttonBox.update = function( nodeName, text, type, size, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'buttonBox' ) ;
	var strclass = 'btn' ;
	if ( typeof( size ) === 'string' ){ strclass = strclass + ' btn-' + size ; }
	if ( typeof( type ) === 'string' ){ strclass = strclass + ' btn-' + type ; }else{ strclass = strclass + ' btn-default' ; }
	if ( typeof( fun ) === 'function' )
	{
		fun( node['obj'] ) ;
	}
	else
	{
		sdbjs.fun.addClick( node['obj'], fun ) ;
	}
	if( typeof( text ) === 'string' )
	{
		$( node['obj'] ).removeClass().addClass( strclass ).html( text ) ; ;
	}
	else if( typeof( text ) === 'function' )
	{
		$( node['obj'] ).removeClass().addClass( strclass ) ;
		text( node['obj'] ) ;
	}
}

/********************* 下拉菜单 **************************/

/*
 * 创建
 */
sdbjs.parts.dropDownBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'dropdown' ) ;
	var button = $( '<button></button>' ).addClass( 'btn btn-default' ).attr( 'data-toggle', 'dropDownBox' ).attr( 'data-target', nodeName ).html( '<span class="caret caret-bottom"></span>' ).appendTo( newObj ) ;
	var clear  = $( '<div></div>' ).addClass( 'clear-float' ).appendTo( newObj ) ;
	var menu   = $( '<ul></ul>' ).addClass( 'dropdown-menu' ).appendTo( $( 'body' ) ) ;
	var node   = sdbjs.fun.addNode( parentName, nodeName, 'dropDownBox', newObj, width, height ) ;
	node['parts'].push( { 'button': button, 'menu': menu, 'list': [] } ) ;
}

/*
 * 更新
 */
sdbjs.parts.dropDownBox.update = function( nodeName, buttonText, size, type )
{
	var node = sdbjs.fun.getNode( nodeName, 'dropDownBox' ) ;
	$( node['parts'][0]['button'] ).html( buttonText + '<span class="caret caret-bottom"></span>' ) ;
	if( typeof( type ) !== 'undefined' )
	{
		$( node['parts'][0]['button'] ).removeClass( 'btn-default btn-plain btn-primary btn-success btn-warnings btn-danger' ) ;
		if( typeof( type ) === 'string' )
		{
			$( node['parts'][0]['button'] ).addClass( type ) ;
		}
	}
	if( typeof( size ) === 'string' )
	{
		$( node['parts'][0]['button'] ).removeClass( 'btn-lg btn-mg' ).addClass( 'btn ' + size ) ;
	}
}

/*
 * 添加
 */
sdbjs.parts.dropDownBox.add = function( nodeName, text, status, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'dropDownBox' ) ;
	var li = $( '<li></li>' ) ;
	if ( status === false ){ $( li ).addClass( 'drop-disabled' ); }
	if ( text === '' ){ $( li ).removeClass().addClass( 'divider' ) }else{ $( li ).html( text ); }
	if ( typeof( fun ) === 'function' )
	{
		$( li ).removeClass().addClass( 'event' );
		fun( li ) ;
	}
	else
	{
		if( typeof( fun ) === 'string' )
		{
			if( fun !== '' )
			{
				$( li ).removeClass().addClass( 'event' );
			}
			sdbjs.fun.addClick( li, fun ) ;
		}
	}
	$( node['parts'][0]['menu'] ).append( li ) ;
	node['parts'][0]['list'].push( li ) ;
}

/*
 * 更新
 */
sdbjs.parts.dropDownBox.updateMenu = function( nodeName, line, text, status, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'dropDownBox' ) ;
	var li = node['parts'][0]['list'][line] ;
	if ( status === false ){ $( li ).addClass( 'drop-disabled' ); }
	if ( text === '' ){ $( li ).removeClass().addClass( 'divider' ) }else{ $( li ).html( text ); }
	if ( typeof( fun ) === 'function' )
	{
		$( li ).removeClass().addClass( 'event' );
		fun( li ) ;
	}
	else
	{
		if( typeof( fun ) === 'string' )
		{
			if( fun !== '' )
			{
				$( li ).removeClass().addClass( 'event' );
			}
			sdbjs.fun.addClick( li, fun ) ;
		}
	}
}

/********************* 网格 **************************/

/*
 * 创建
 */
sdbjs.parts.gridBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'grid' ) ;
	var gHead  = $( '<div></div>' ).addClass( 'header' ) ;
	var gBody  = $( '<div></div>' ).addClass( 'body' ) ;
	var zBody  = $( '<div></div>' ).appendTo( gBody ) ;
	var hTable = $( '<table></table>' ).appendTo( gHead ) ;
	var bTable = $( '<table></table>' ).appendTo( zBody ) ;
	var hTableBody = $( '<tbody></tbody>' ).appendTo( hTable ) ;
	var bTableBody = $( '<tbody></tbody>' ).appendTo( bTable ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'gridBox', newObj, width, height ) ;
	var gHeadNode = sdbjs.fun.addNode( nodeName, nodeName + '~header', 'gridBoxHeader', gHead ) ;
	var gBodyNode = sdbjs.fun.addNode( nodeName, nodeName + '~body', 'gridBoxBody', gBody, 'auto', 'variable' ) ;
	node['parts'].push( { 'gHead': gHeadNode, 'head': hTableBody, 'list': [] } ) ;
	node['parts'].push( { 'gBody': gBodyNode, 'body': bTableBody, 'list': [] } ) ;
}

/*
 * 添加标题 [ { text: '', colspan: 2, width: '' } ] 
 */
sdbjs.parts.gridBox.addTitle = function( nodeName, gHeader )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	var row = $( '<tr></tr>' ) ;
	var list = [] ;
	var len = gHeader.length ;
	for( var i = 0; i < len; ++i )
	{
		var cell = $( '<td></td>' ).appendTo( row ) ;
		if( typeof( gHeader[i]['colspan'] ) !== 'undefined' )
		{
			$( cell ).attr( 'colspan', parseInt( gHeader[i]['colspan'] ) ) ;
		}
		if( typeof( gHeader[i]['width'] ) !== 'undefined' )
		{
			$( cell ).css( 'width', gHeader[i]['width'] ) ;
		}
		if( typeof( gHeader[i]['text'] ) === 'string' )
		{
			cell.html( gHeader[i]['text'] ) ;
		}
		else if( typeof( gHeader[i]['text'] ) === 'function' )
		{
			gHeader[i]['text']( cell ) ;
		}
		list.push( cell ) ;
	}
	$( node['parts'][0]['head'] ).append( row ) ;
	node['parts'][0]['list'].push( list ) ;
	sdbjs.fun.allNodeRepaint( node ) ;
}

/*
 * 更新标题
 */
sdbjs.parts.gridBox.updateTitle = function( nodeName, line, column, text )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	if( typeof( text ) === 'string' )
	{
		$( node['parts'][0]['list'][line][column] ).html( text ) ;
	}
	else if( typeof( text ) === 'function' )
	{
		text( node['parts'][0]['list'][line][column] ) ;
	}
}

/*
 * 添加内容 [ { text: '', colspan: 2 } ] 
 */
sdbjs.parts.gridBox.addBody = function( nodeName, gBody )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	var index = node['parts'][1]['list'].length ;
	var row = $( '<tr></tr>' ) ;
	var list = [] ;
	var len = gBody.length ;
	for( var i = 0; i < len; ++i )
	{
		var cell = $( '<td></td>' ).appendTo( row ) ;
		if( typeof( gBody[i]['colspan'] ) !== 'undefined' )
		{
			$( cell ).attr( 'colspan', parseInt( gBody[i]['colspan'] ) ) ;
		}
		if( typeof( gBody[i]['width'] ) !== 'undefined' )
		{
			$( cell ).css( 'width', gBody[i]['width'] ) ;
		}
		if( typeof( gBody[i]['text'] ) === 'string' )
		{
			cell.html( gBody[i]['text'] ) ;
		}
		else if( typeof( gBody[i]['text'] ) === 'function' )
		{
			gBody[i]['text']( cell ) ;
		}
		list.push( cell ) ;
	}
	$( node['parts'][1]['body'] ).append( row ) ;
	if( index % 2 === 1 )
	{
		$( row).css( 'background-color', '#EEF7FB' ) ;
	}
	node['parts'][1]['list'].push( list ) ;
	sdbjs.fun.allNodeRepaint( node ) ;
}

/*
 * 更新内容
 */
sdbjs.parts.gridBox.updateBody = function( nodeName, line, column, text )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	if( typeof( text ) === 'string' )
	{
		$( node['parts'][1]['list'][line][column] ).html( text ) ;
	}
	else if( typeof( text ) === 'function' )
	{
		text( node['parts'][1]['list'][line][column] ) ;
	}
}

/*
 * 删除一行
 */
sdbjs.parts.gridBox.removeBody = function( nodeName, line )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	$( node['parts'][1]['list'][line][0] ).parent().empty().remove() ;
	sdbjs.fun.free( node['parts'][1]['list'][line] ) ;
	node['parts'][1]['list'].splice( line, 1 ) ;
}

/*
 * 删除所有
 */
sdbjs.parts.gridBox.emptyBody = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	var len = node['parts'][1]['list'].length ;
	for( var i = len - 1; i >= 0; --i )
	{
		sdbjs.parts.gridBox.removeBody( nodeName, i ) ;
	}
}

/*
 * 显示一行
 */
sdbjs.parts.gridBox.showBody = function( nodeName, line )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	$.each( node['parts'][1]['list'][line], function( index, td ){
		$( td ).show() ;
	} ) ;
}

/*
 * 显示所有行
 */
sdbjs.parts.gridBox.showAllBody = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	$.each( node['parts'][1]['list'], function( line ){
		sdbjs.parts.gridBox.showBody( nodeName, line ) ;
	} ) ;
}

/*
 * 隐藏一行
 */
sdbjs.parts.gridBox.hideBody = function( nodeName, line )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	$.each( node['parts'][1]['list'][line], function( index, td ){
		$( td ).hide() ;
	} ) ;
}

/*
 * 隐藏所有行
 */
sdbjs.parts.gridBox.hideAllBody = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	$.each( node['parts'][1]['list'], function( line ){
		sdbjs.parts.gridBox.hideBody( nodeName, line ) ;
	} ) ;
}

/*
 * 重新绘制颜色
 */
sdbjs.parts.gridBox.repigment = function( nodeName, useHidden )
{
	var node = sdbjs.fun.getNode( nodeName, 'gridBox' ) ;
	var i = 0 ;
	$.each( node['parts'][1]['list'], function( line, tdArr ){
		var isRepigment = ( ( useHidden !== true && $( tdArr[0] ).is( ':hidden' ) === false ) || useHidden === true ) ;
		if( isRepigment === true )
		{
			if( i % 2 === 1 )
			{
				$( tdArr[0] ).parent().css( 'background-color', '#EEF7FB' ) ;
			}
			else
			{
				$( tdArr[0] ).parent().css( 'background-color', '#FFFFFF' ) ;
			}
			++i ;
		}
	} ) ;
}

/********************* 表格 **************************/

/*
 * 创建
 */
sdbjs.parts.tableBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<table></table>' ).addClass( 'table' ) ;
	var thead  = $( '<thead></thead>' ).appendTo( newObj ) ;
	var tbody  = $( '<tbody></tbody>' ).appendTo( newObj ) ;
	var node   = sdbjs.fun.addNode( parentName, nodeName, 'tableBox', newObj, width, height ) ;
	node['parts'].push( { 'thead': thead, 'list': [] } ) ;
	node['parts'].push( { 'tbody': tbody, 'list': [] } ) ;
}

/*
 * 设置表格类型 tableType compact(紧凑) loosen(宽松) simple(外边框表格) border(内外边框表格)
 */
sdbjs.parts.tableBox.update = function( nodeName, tableType )
{
	var node = sdbjs.fun.getNode( nodeName, 'tableBox' ) ;
	$( node['obj'] ).removeClass().addClass( 'table ' + tableType ) ;
}

/*
 * 添加头 [ { text: '', colspan: 2, width: 30 } ] 
 */
sdbjs.parts.tableBox.addHead = function( nodeName, thead )
{
	var node = sdbjs.fun.getNode( nodeName, 'tableBox' ) ;
	var row = $( '<tr></tr>' ) ;
	var list = [] ;
	var len = thead.length ;
	for( var i = 0; i < len; ++i )
	{
		var cell = $( '<th></th>' ).html( thead[i]['text'] ).appendTo( row ) ;
		if( typeof( thead[i]['colspan'] ) !== 'undefined' )
		{
			$( cell ).attr( 'colspan', parseInt( thead[i]['colspan'] ) ) ;
		}
		if( typeof( thead[i]['width'] ) !== 'undefined' )
		{
			$( cell ).attr( 'width', parseInt( thead[i]['width'] ) ) ;
		}
		list.push( cell ) ;
	}
	$( node['parts'][0]['thead'] ).append( row ) ;
	node['parts'][0]['list'].push( list ) ;
}

/*
 * 添加body [ { text: '', colspan: 2, width: 30 } ] 
 */
sdbjs.parts.tableBox.addBody = function( nodeName, tbody )
{
	var node = sdbjs.fun.getNode( nodeName, 'tableBox' ) ;
	var row = $( '<tr></tr>' ) ;
	var list = [] ;
	var len = tbody.length ;
	for( var i = 0; i < len; ++i )
	{
		var cell = $( '<td></td>' ).appendTo( row ) ;
		if( typeof( tbody[i]['colspan'] ) !== 'undefined' )
		{
			$( cell ).attr( 'colspan', parseInt( tbody[i]['colspan'] ) ) ;
		}
		if( typeof( tbody[i]['width'] ) !== 'undefined' )
		{
			$( cell ).css( 'width', tbody[i]['width'] ) ;
		}
		if( typeof( tbody[i]['text'] ) === 'string' )
		{
			cell.html( tbody[i]['text'] ) ;
		}
		else if( typeof( tbody[i]['text'] ) === 'function' )
		{
			tbody[i]['text']( cell ) ;
		}
		list.push( cell ) ;
	}
	$( node['parts'][1]['tbody'] ).append( row ) ;
	node['parts'][1]['list'].push( list ) ;
}

/*
 * 更新表格头
 */
sdbjs.parts.tableBox.updateHead = function( nodeName, line, column, text )
{
	var node = sdbjs.fun.getNode( nodeName, 'tableBox' ) ;
	if( typeof( text ) === 'string' )
	{
		$( node['parts'][0]['list'][line][column] ).html( text ) ;
	}
	else if ( typeof( text ) === 'function' )
	{
		text( node['parts'][0]['list'][line][column] ) ;
	}
}

/*
 * 更新表格body
 */
sdbjs.parts.tableBox.updateBody = function( nodeName, line, column, text )
{
	var node = sdbjs.fun.getNode( nodeName, 'tableBox' ) ;
	if( typeof( text ) === 'string' )
	{
		$( node['parts'][1]['list'][line][column] ).html( text ) ;
	}
	else if ( typeof( text ) === 'function' )
	{
		text( node['parts'][1]['list'][line][column] ) ;
	}
}

/********************* 折叠栏 **************************/

/*
 * 创建
 */
sdbjs.parts.foldBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'fold' ) ;
	var header = $( '<div></div>' ).addClass( 'header' ).appendTo( newObj ) ;
	var fbody  = $( '<div></div>' ).addClass( 'body' ).appendTo( newObj ) ;
	var title  = $( '<span></span>' ).addClass( 'point' ).attr( 'data-toggle', 'foldBox' ).attr( 'data-target', nodeName ).appendTo( header ) ;
	var node   = sdbjs.fun.addNode( parentName, nodeName, 'foldBox', newObj, width, height ) ;
	node['parts'].push( { 'header': header, 'title': title, 'body': fbody } ) ;
}

/*
 * 更新
 */
sdbjs.parts.foldBox.update = function( nodeName, title, fbody, showfun, hidefun )
{
	var node = sdbjs.fun.getNode( nodeName, 'foldBox' ) ;
	if( typeof( title ) === 'string' )
	{
		$( node['parts'][0]['title'] ).append( '<span class="caret caret-right"></span> ' ) ;
		$( node['parts'][0]['title'] ).append( title ) ;
		if( typeof( showfun ) === 'string' )
		{
			$(node['parts'][0]['title'] ).attr( 'data-showevent', showfun ) ;
		}
		if( typeof( hidefun ) === 'string' )
		{
			$(node['parts'][0]['title'] ).attr( 'data-hideevent', hidefun ) ;
		}
	}
	else if( typeof( title ) === 'function' )
	{
		title( node['parts'][0]['title'] ) ;
		if( typeof( showfun ) === 'string' )
		{
			$(node['parts'][0]['title'] ).attr( 'data-showevent', showfun ) ;
		}
		if( typeof( hidefun ) === 'string' )
		{
			$(node['parts'][0]['title'] ).attr( 'data-hideevent', hidefun ) ;
		}
	}

	if( typeof( fbody ) === 'string' )
	{
		$( node['parts'][0]['body'] ).html( fbody ) ;
	}
	else if ( typeof( fbody ) === 'function' )
	{
		fbody( node['parts'][0]['body'] ) ;
	}
}

/*
 * 设置域
 */
sdbjs.parts.foldBox.setDomain = function( nodeName, domainName )
{
	var node = sdbjs.fun.getNode( nodeName, 'foldBox' ) ;
	node['data'] = domainName ;
}

/*
 * 切换
 */
sdbjs.parts.foldBox.toggle = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'foldBox' ) ;
	if( $( node['parts'][0]['body'] ).is( ':hidden' ) )
	{
		sdbjs.parts.foldBox.show( nodeName ) ;
	}
	else
	{
		sdbjs.parts.foldBox.hide( nodeName ) ;
	}
}

/*
 * 显示
 */
sdbjs.parts.foldBox.show = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'foldBox' ) ;
	$( node['parts'][0]['body'] ).show() ;
	$( node['parts'][0]['title'] ).children( '.caret' ).removeClass( 'caret-right' ).addClass( 'caret-bottom' ) ;
	if( node['data'] !== null )
	{
		var domainName = node['data'] ;
		if( typeof( sdbjs.private.foldBox[ domainName ] ) !== 'undefined' )
		{
			var tempNodeName = sdbjs.private.foldBox[ domainName ] ;
			if( tempNodeName !== nodeName )
			{
				sdbjs.parts.foldBox.hide( tempNodeName ) ;
			}
		}
		sdbjs.private.foldBox[ domainName ] = nodeName ;
	}
	var fun = $( node['parts'][0]['title']).attr( 'data-showevent' ) ;
	if( typeof( fun ) === 'string' )
	{
		eval( fun ) ;
	}
}

/*
 * 隐藏
 */
sdbjs.parts.foldBox.hide = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'foldBox' ) ;
	$( node['parts'][0]['body'] ).hide() ;
	$( node['parts'][0]['title'] ).children( '.caret' ).removeClass( 'caret-bottom' ).addClass( 'caret-right' ) ;
	var fun = $( node['parts'][0]['title']).attr( 'data-hideevent' ) ;
	if( typeof( fun ) === 'string' )
	{
		eval( fun ) ;
	}
}

/********************* 面板 **************************/

/*
 * 创建
 */
sdbjs.parts.panelBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'panel' ) ;
	var title  = $( '<div></div>' ).addClass( 'title' ) ;
	var pbody  = $( '<div></div>' ).addClass( 'body' ) ;
	var node   = sdbjs.fun.addNode( parentName, nodeName, 'panelBox', newObj, width, height ) ;
	var titleNode = sdbjs.fun.addNode( nodeName, nodeName + '~title', 'panelBoxTitle', title ) ;
	var bodyNode = sdbjs.fun.addNode( nodeName, nodeName + '~body', 'panelBoxBody', pbody, 'auto', 'variable' ) ;
	node['parts'].push( { 'title': titleNode, 'body': bodyNode } ) ;
}

/*
 * 更新
 */
sdbjs.parts.panelBox.update = function( nodeName, title, pbody )
{
	var node = sdbjs.fun.getNode( nodeName, 'panelBox' ) ;
	if( typeof( title ) === 'string' )
	{
		$( node['parts'][0]['title']['obj'] ).html( title ) ;
		sdbjs.fun.allNodeRepaint( node ) ;
	}
	else if( typeof( title ) === 'function' )
	{
		title( node['parts'][0]['title'] ) ;
		sdbjs.fun.allNodeRepaint( node ) ;
	}
	
	if( typeof( pbody ) === 'string' )
	{
		$( node['parts'][0]['body']['obj'] ).html( pbody ) ;
		sdbjs.fun.allNodeRepaint( node ) ;
	}
	else if( typeof( pbody ) === 'function' )
	{
		pbody( node['parts'][0]['body'] ) ;
		sdbjs.fun.allNodeRepaint( node ) ;
	}
}

/********************* 分页栏 **************************/

/*
 * 创建
 */
sdbjs.parts.tabPageBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<ul></ul>' ).addClass( 'tab-page' ) ;
	sdbjs.fun.addNode( parentName, nodeName, 'tabPageBox', newObj, width, height ) ;
}

/*
 * 添加
 */
sdbjs.parts.tabPageBox.add = function( nodeName, text, isActive, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'tabPageBox' ) ;
	var li = $( '<li></li>' ).html( text ).appendTo( node['obj'] ) ;
	if ( isActive === true ){ $( li ).addClass( 'active' ) }
	if ( typeof( fun ) === 'function' )
	{
		fun( li ) ;
	}
	else
	{
		sdbjs.fun.addClick( li, fun ) ;
	}
	node['parts'].push( li ) ;
}

/********************* 下拉选择框 **************************/

/*
 * 创建
 */
sdbjs.parts.selectBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<select></select>' ).addClass( 'form-control' ) ;
	sdbjs.fun.addNode( parentName, nodeName, 'selectBox', newObj, width, height ) ;
}

/*
 * 添加
 */
sdbjs.parts.selectBox.add = function( nodeName, htmlText, value, selected )
{
	var node = sdbjs.fun.getNode( nodeName, 'selectBox' ) ;
	var options = $( '<option></option>' ).val( value ).text( htmlText ) ;
	if( selected === true )
	{
		options.attr( 'selected', 'selected' ) ;
	}
	$( node['obj'] ).append( options ) ;
}

/*
 * 取值
 */
sdbjs.parts.selectBox.get = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'selectBox' ) ;
	return $( node['obj'] ).val() ;
}

/*
 * 设值
 */
sdbjs.parts.selectBox.set = function( nodeName, value )
{
	var node = sdbjs.fun.getNode( nodeName, 'selectBox' ) ;
	$( node['obj'] ).val( value ) ;
}

/*
 * 删除一个
 */
sdbjs.parts.selectBox.remove = function( nodeName, htmlText, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'selectBox' ) ;
	$( node['obj'] ).children( 'option' ).each( function( index ){
		if( $( this ).text() === htmlText )
		{
			if( typeof( fun ) === 'string' )
			{
				var rc = eval( fun ) ;
				if( rc === true )
				{
					$( this ).empty().remove() ;
					return false ;
				}
			}
			else if( typeof( fun ) === 'function' )
			{
				var rc = fun( index, $( this ) ) ;
				if( rc === true )
				{
					$( this ).empty().remove() ;
					return false ;
				}
			}
			else
			{
				$( this ).empty().remove() ;
				return false ;
			}
		}
	} ) ;
}

/*
 * 删除所有
 */
sdbjs.parts.selectBox.empty = function( nodeName, htmlText, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'selectBox' ) ;
	$( node['obj'] ).empty() ;
}

/********************* well面板 **************************/

/*
 * 创建
 */
sdbjs.parts.wellBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'well' ) ;
	sdbjs.fun.addNode( parentName, nodeName, 'wellBox', newObj, width, height ) ;
}

/*
 * 更新
 */
sdbjs.parts.wellBox.update = function( nodeName, text )
{
	var node = sdbjs.fun.getNode( nodeName, 'wellBox' ) ;
	if( typeof( text ) === 'string' )
	{
		$( node['obj'] ).html( text ) ;
	}
	else if( typeof( text ) === 'function' )
	{
		text( node['obj'] ) ;
	}
}

/********************* 提示框 **************************/

/*
 * 创建
 */
sdbjs.parts.alertBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'alert alert-success' ) ;
	sdbjs.fun.addNode( parentName, nodeName, 'alertBox', newObj, width, height ) ;
}

/*
 * 更新
 */
sdbjs.parts.alertBox.update = function( nodeName, text, type )
{
	var node = sdbjs.fun.getNode( nodeName, 'alertBox' ) ;
	if( typeof( type ) === 'string' ){ $( node['obj'] ).removeClass().addClass( 'alert alert-' + type ) }
	if( typeof( text ) === 'string' )
	{
		$( node['obj'] ).html( text ) ;
	}
	else if ( typeof( text ) === 'function' )
	{
		text( node['obj'] ) ;
	}
}

/********************* 导航 **************************/

/*
 * 创建
 */
sdbjs.parts.navBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'nav' ) ;
	var colum = $( '<div></div>' ).addClass( 'colum' ).appendTo( newObj ) ;
	var menu = $( '<div></div>' ).addClass( 'menu' ).appendTo( newObj ) ;
	var ul = $( '<ul></ul>' ).appendTo( colum ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'navBox', newObj, width, height ) ;
	node['parts'].push( { 'columUL': ul, 'list': [] } ) ;
	node['parts'].push( { 'menu': menu, 'list': [] } ) ;
}

/*
 * 添加栏目
 */
sdbjs.parts.navBox.addColum = function( nodeName, text, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'navBox' ) ;
	var index = node['parts'][1]['list'].length ;
	var li = $( '<li></li>' ).attr( 'data-toggle', 'navBox' ).attr( 'data-target', nodeName ).attr( 'data-index', index ) ;
	var a  = $( '<a></a>' ).attr( 'href', '#' ).html( text ).appendTo( li ) ;
	var ul = $( '<ul></ul>' ) ;
	if( typeof( fun ) === 'function' )
	{
		fun( a ) ;
	}
	else
	{
		sdbjs.fun.addClick( a, fun ) ;
	}
	$( node['parts'][0]['columUL'] ).append( li ) ;
	$( node['parts'][1]['menu'] ).append( ul ) ;
	node['parts'][0]['list'].push( a ) ;
	node['parts'][1]['list'].push( { 'menu-ul': ul, 'list': [] } ) ;
}

/*
 * 添加栏目右侧
 */
sdbjs.parts.navBox.addColum2 = function( nodeName, text, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'navBox' ) ;
	var index = node['parts'][1]['list'].length ;
	var li = $( '<li></li>' ).css( 'float', 'right' ).attr( 'data-toggle', 'navBox' ).attr( 'data-target', nodeName ).attr( 'data-index', index ) ;
	var a  = $( '<a></a>' ).attr( 'href', '#' ).html( text ).appendTo( li ) ;
	var ul = $( '<ul></ul>' ) ;
	if( typeof( fun ) === 'function' )
	{
		fun( a ) ;
	}
	else
	{
		sdbjs.fun.addClick( a, fun ) ;
	}
	$( node['parts'][0]['columUL'] ).append( li ) ;
	$( node['parts'][1]['menu'] ).append( ul ) ;
	node['parts'][0]['list'].push( a ) ;
	node['parts'][1]['list'].push( { 'menu-ul': ul, 'list': [] } ) ;
}

/*
 * 添加下拉表 number 第几个栏目， MenuArray [ { 'text': 'xxx', 'fun': '' } ]
 */
sdbjs.parts.navBox.addMenu = function( nodeName, number, MenuArray )
{
	var node = sdbjs.fun.getNode( nodeName, 'navBox' ) ;
	for( var key in MenuArray )
	{
		var li = $( '<li></li>' ) ;
		var a = $( '<a></a>' ).attr( 'href', '#' ).html( MenuArray[key]['text'] ).appendTo( li ) ;
		if( typeof( MenuArray[key]['fun'] ) === 'function' )
		{
			MenuArray[key]['fun']( a ) ;
		}
		else
		{
			sdbjs.fun.addClick( a, MenuArray[key]['fun'] ) ;
		}
		$( node['parts'][1]['list'][number]['menu-ul'] ).append( li ) ;
		node['parts'][1]['list'][number]['list'].push( a ) ;
	}
}

/********************* 加载中的图 **************************/

/*
 * 创建
 */
sdbjs.parts.loadingBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'loading' ) ;
	var img = $('<img>' ).addClass( 'img' ).attr( 'src', './images/onloading.gif' ).appendTo( newObj ) ;
	var font = $( '<div></div>' ).addClass( 'font' ).html( '0%' ).appendTo( newObj ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'loadingBox', newObj, width, height ) ;
	node['parts'].push( font ) ;
}

/*
 * 更新
 */
sdbjs.parts.loadingBox.update = function( nodeName, text )
{
	var node = sdbjs.fun.getNode( nodeName, 'loadingBox' ) ;
	$( node['parts'][0] ).html( text ) ;
}

/*
 * 显示
 */
sdbjs.parts.loadingBox.show = function( nodeName )
{
	var node = sdbjs.fun.getNode( nodeName, 'loadingBox' ) ;
	sdbjs.fun.showScreen() ;
	$( node['obj'] ).show() ;
	++sdbjs.private.loadingBox ;
}

/*
 * 隐藏
 */
sdbjs.parts.loadingBox.hide = function( nodeName )
{
	--sdbjs.private.loadingBox ;
	if( sdbjs.private.loadingBox === 0 )
	{
		var node = sdbjs.fun.getNode( nodeName, 'loadingBox' ) ;
		$( node['obj'] ).hide() ;
	}
	sdbjs.fun.hideScreen() ;
}

/********************* 标签页 **************************/

/*
 * 创建
 */
sdbjs.parts.navTabBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'nav-tabs' ) ;
	var list = $( '<div></div>' ).addClass( 'title' ) ;
	var ul = $( '<ul></ul>' ).appendTo( list ) ;
	var body = $( '<div></div>' ).addClass( 'body' ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'navTabBox', newObj, width, height ) ;
	var titleNode = sdbjs.fun.addNode( nodeName, nodeName + '~title', 'navTabBoxTitle', list, 'auto', 40 ) ;
	var bodyNode = sdbjs.fun.addNode( nodeName, nodeName + '~body', 'navTabBoxBody', body, 'auto', 'variable' ) ;
	node['parts'].push( { 'ul': ul, 'title': titleNode, 'body': bodyNode, 'ulList': [], 'BodyList': [], 'active': 0 } ) ;
}

/*
 * 添加
 */
sdbjs.parts.navTabBox.add = function( nodeName, liName, text, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'navTabBox' ) ;
	var li = $( '<li></li>' ).attr( 'data-toggle', 'navTabBox' ).attr( 'data-target', nodeName ) ;
	var div = $( '<div></div>' ) ;
	var index = node['parts'][0]['ulList'].length ;
	if( index === 0 )
	{
		li.addClass( 'active' ) ;
	}
	
	li.attr( 'data-index', index ) ;
	
	if( typeof( liName ) === 'string' )
	{
		li.html( liName ) ;
	}
	else if( typeof( liName ) === 'function' )
	{
		liName( li ) ;
	}
	
	if( typeof( text ) === 'string' )
	{
		div.html( text ) ;
	}
	else if( typeof( text ) === 'function' )
	{
		text( div ) ;
	}
	
	if( typeof( fun ) === 'string' )
	{
		li.attr( 'data-event', fun ) ;
	}
	
	$( node['parts'][0]['ul'] ).append( li ) ;
	$( node['parts'][0]['body']['obj'] ).append( div ) ;
	node['parts'][0]['ulList'].push( li ) ;
	node['parts'][0]['BodyList'].push( div ) ;
}

/*
 * 显示
 */
sdbjs.parts.navTabBox.show = function( nodeName, num, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'navTabBox' ) ;
	var activeIndex = node['parts'][0]['active'] ;
	
	num = parseInt( num ) ;
	if( activeIndex !== num )
	{
		$( node['parts'][0]['ulList'][activeIndex] ).removeClass( 'active' ) ;
		$( node['parts'][0]['BodyList'][activeIndex] ).hide( 450 ) ;
		$( node['parts'][0]['ulList'][num] ).addClass( 'active' ) ;
		if( typeof( fun ) === 'function' )
		{
			$( node['parts'][0]['BodyList'][num] ).show( 500, fun ) ;
		}
		else if( typeof( fun ) === 'string' )
		{
			$( node['parts'][0]['BodyList'][num] ).show( 500, function(){
				try{
					eval( fun ) ;
				}catch(e){}
			} ) ;
		}
		node['parts'][0]['active'] = num ;
	}
	else
	{
		if( typeof( fun ) === 'function' )
		{
			fun() ;
		}
		else if( typeof( fun ) === 'string' )
		{
			try{
				eval( fun ) ;
			}catch(e){}
		}
	}
}

/********************* 通知框 **************************/

/*
 * 创建
 */
sdbjs.parts.noticeBox.create = function( parentName, nodeName, width, height )
{
	var newObj = $( '<div></div>' ).addClass( 'notice' ) ;
	var header = $( '<div></div>' ).addClass( 'header' ).appendTo( newObj ) ;
	var close  = $( '<button></button>' ).html( '&times;' ).attr( 'data-toggle', 'noticeBox' ).attr( 'data-target', nodeName ).addClass( 'close' ).appendTo( header ) ;
	var title  = $( '<div></div>' ).appendTo( header ) ;
	var body = $( '<div></div>' ).addClass( 'body' ).appendTo( newObj ) ;
	var node = sdbjs.fun.addNode( parentName, nodeName, 'noticeBox', newObj, width, height ) ;
	node['parts'].push( { 'header': header, 'title': title, 'body': body } ) ;
}

/*
 * 更新
 */
sdbjs.parts.noticeBox.update = function( nodeName, title, body )
{
	var node = sdbjs.fun.getNode( nodeName, 'noticeBox' ) ;
	if( typeof( title ) === 'string' )
	{
		$( node['parts'][0]['title'] ).html( title ) ;
	}
	else if( typeof( title ) === 'function' )
	{
		title( node['parts'][0]['title'] ) ;
	}
	
	if( typeof( body ) === 'string' )
	{
		$( node['parts'][0]['body'] ).html( body ) ;
	}
	else if( typeof( body ) === 'function' )
	{
		body( node['parts'][0]['body'] ) ;
	}
}

/*
 * 显示
 */
sdbjs.parts.noticeBox.show = function( nodeName, times, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'noticeBox' ) ;
	if( typeof( times ) === 'number' && times > 0 )
	{
		setTimeout( function(){
			$( node['obj'] ).show( 0, fun ) ;
		}, times ) ;
	}
	else
	{
		$( node['obj'] ).show( 0, fun ) ;
	}
}

/*
 * 隐藏
 */
sdbjs.parts.noticeBox.hide = function( nodeName, times, fun )
{
	var node = sdbjs.fun.getNode( nodeName, 'noticeBox' ) ;
	if( typeof( times ) === 'number' && times > 0 )
	{
		setTimeout( function(){
			$( node['obj'] ).hide( 0, fun ) ;
		}, times ) ;
	}
	else
	{
		$( node['obj'] ).hide( 0, fun ) ;
	}
}

/**************************************** 事件处理 ****************************************/

/* 点击事件处理 */
sdbjs.fun.clickEvent = function( domObj, isFirst )
{
	if( domObj === document || domObj === document.body )
	{
		return;
	}
	var isEvent = false ;
	var toggle = $( domObj ).attr( 'data-toggle' ) ;
	
	if( isFirst === true )
	{
		//导航隐藏
		if( sdbjs.private.navBox !== null )
		{
			var index = sdbjs.private.navBox['index'] ;
			var node = sdbjs.private.navBox['node'] ;
			$( node['parts'][1]['list'][index]['menu-ul'] ).hide();
			sdbjs.private.navBox = null ;
		}
		//下拉菜单
		if( sdbjs.private.dropDownBox !== null )
		{
			var node = sdbjs.private.dropDownBox['node'] ;
			$( node['parts'][0]['menu'] ).hide() ;
			sdbjs.private.dropDownBox = null ;
		}
		isFirst = false ;
	}
	
	if( typeof( toggle ) !== 'undefined' )
	{
		if( toggle === 'navBox' )
		{
			isEvent = true ;
			var sdbWidth  = sdbjs.fun.getWindowWidth() ;
			var target = $( domObj ).attr( 'data-target' ) ;
			var index = $( domObj ).attr( 'data-index' ) ;
			var node = sdbjs.fun.getNode( target, 'navBox' ) ;
			if( node['parts'][1]['list'][index]['list'].length > 0 )
			{
				var menuLeft = $( domObj ).offset().left ;
				var menuWidth = $( node['parts'][1]['list'][index]['menu-ul'] ).outerWidth() ;
				if( menuLeft + menuWidth > sdbWidth )
				{
					menuLeft = sdbWidth - menuWidth ;
				}
				$( node['parts'][1]['list'][index]['menu-ul'] ).css( 'margin-left', menuLeft ) ;
				$( node['parts'][1]['list'][index]['menu-ul'] ).show();
				sdbjs.private.navBox = { 'node': node, 'index': index } ;
			}
		}
		else if( toggle === 'dropDownBox' )
		{
			isEvent = true ;
			var target = $( domObj ).attr( 'data-target' ) ;
			var node = sdbjs.fun.getNode( target, 'dropDownBox' ) ;
			$( node['parts'][0]['menu'] ).css( { 'left': $( domObj ).offset().left, 'top': $( domObj ).offset().top + $( domObj ).height() } ) ;
			$( node['parts'][0]['menu'] ).show() ;
			sdbjs.private.dropDownBox = { 'node': node } ;
		}
		else if ( toggle === 'foldBox' )
		{
			isEvent = true ;
			var target = $( domObj ).attr( 'data-target' ) ;
			sdbjs.parts.foldBox.toggle( target ) ;
		}
		else if ( toggle === 'modalBox' )
		{
			isEvent = true ;
			var target = $( domObj ).attr( 'data-target' ) ;
			sdbjs.parts.modalBox.toggle( target ) ;
		}
		else if ( toggle === 'navTabBox' )
		{
			isEvent = true ;
			var target = $( domObj ).attr( 'data-target' ) ;
			var index = $( domObj ).attr( 'data-index' ) ;
			var fun = $( domObj ).attr( 'data-event' ) ;
			sdbjs.parts.navTabBox.show( target, index, fun ) ;
		}
		else if ( toggle === 'checkBox' )
		{
			var className = $( domObj ).attr( 'class' ) ;
			if( className === 'unchecked' )
			{
				$( domObj ).removeClass().addClass( 'checked' ) ;
			}
			else if( className === 'checked' )
			{
				$( domObj ).removeClass().addClass( 'unchecked' ) ;
			}
		}
	}
	if( domObj.parentNode !== document.body && isEvent === false )
	{
		sdbjs.fun.clickEvent( domObj.parentNode ) ;
	}
}

/* 鼠标事件处理 */
sdbjs.fun.overEvent = function( domObj, isFirst )
{
	if( domObj === document || domObj === document.body )
	{
		return;
	}
	var isEvent = false ;
	var toggle = $( domObj ).attr( 'data-mouse' ) ;
	
	if( isFirst === true )
	{
		isFirst = false ;
	}
	
	if( typeof( toggle ) !== 'undefined' )
	{
		if( toggle === 'labelBox' )
		{
			isEvent = true ;
			var desc = $( domObj ).data( 'desc' ) ;
			if( typeof( desc ) === 'string' )
			{
				var left = 0 ;
				var top = 0 ;
				var buttonL = $( domObj ).offset().left ;
				var buttonT = $( domObj ).offset().top ;
				var buttonW = $( domObj ).outerWidth() ;
				var buttonH = $( domObj ).outerHeight() ;
				
				$( sdbjs.private.smallLabel ).removeClass().addClass( 'tooltip' ).show().children( '.inner' ).html( desc ) ;
				
				var tooltipW = $( sdbjs.private.smallLabel ).outerWidth() ;
				var tooltipH = $( sdbjs.private.smallLabel ).outerHeight() ;
				
				var className = '' ;
				
				var sdbWidth  = sdbjs.fun.getWindowWidth() ;
				var sdbHeight = sdbjs.fun.getWindowHeight() ;
				
				if( buttonL + buttonW + tooltipW < sdbWidth && className === '' )
				{
					className = 'tooltip right' ;
					left = buttonL + buttonW ;
					if( buttonT + parseInt( buttonH * 0.5 ) + tooltipH - 14 < sdbHeight )
					{
						top = buttonT + parseInt( buttonH * 0.5 ) - 14 ;
					}
					else if ( tooltipH - 18 - parseInt( buttonH * 0.5 ) < buttonT )
					{
						className += ' right-bottom' ;
						top = buttonT + parseInt( buttonH * 0.5 ) - tooltipH + 18 ;
					}
					else
					{
						className = '' ;
					}
				}
				else if( tooltipW + 6 < buttonL && className === '' )
				{
					className = 'tooltip left' ;
					left = buttonL - tooltipW - 9 ;
					if( buttonT + parseInt( buttonH * 0.5 ) + tooltipH - 14 < sdbHeight )
					{
						top = buttonT + parseInt( buttonH * 0.5 ) - 14 ;
					}
					else if ( tooltipH - 18 - parseInt( buttonH * 0.5 ) < buttonT )
					{
						className += ' left-bottom' ;
						top = buttonT + parseInt( buttonH * 0.5 ) - tooltipH + 18 ;
					}
					else
					{
						className = '' ;
					}
				}
				else if( tooltipW - 27 < buttonL && className === '' )
				{
					className = 'tooltip top top-right' ;
					left = buttonL - tooltipW + 27 ;
					if( tooltipH + 7 < buttonT )
					{
						top = buttonT - tooltipH - 10 ;
					}
					else if ( tooltipH + 7 + buttonT + buttonH < sdbHeight )
					{
						className = 'tooltip bottom bottom-right' ;
						top = buttonT + buttonH ;
					}
					else
					{
						className = '' ;
					}
				}
				else if ( buttonL + buttonW + tooltipW - 27 < sdbWidth && className === '' )
				{
					className = 'tooltip top top-left' ;
					left = buttonL + buttonW - 27 ;
					if( tooltipH + 7 < buttonT )
					{
						top = buttonT - tooltipH - 10 ;
					}
					else if ( tooltipH + 7 + buttonT + buttonH < sdbHeight )
					{
						className = 'tooltip bottom bottom-left' ;
						top = buttonT + buttonH ;
					}
					else
					{
						className = '' ;
					}
				}
				else if ( parseInt( tooltipW * 0.5 ) < buttonL && 
							 parseInt( tooltipW * 0.5 ) + buttonL + parseInt( buttonW * 0.5 ) < sdbWidth && 
							 className === '' )
				{
					className = 'tooltip' ;
					if( tooltipH + 10 < buttonT )
					{
						className += ' top' ;
						top = buttonT - tooltipH - 10 ;
						left = buttonL + parseInt( buttonW * 0.5 ) - parseInt( tooltipW * 0.5 ) ;
					}
					else if ( buttonT + buttonH + tooltipH + 10 < sdbHeight )
					{
						className += ' bottom' ;
						top = buttonT + buttonH ;
						left = buttonL + parseInt( buttonW * 0.5 ) - parseInt( tooltipW * 0.5 ) ;
					}
					else
					{
						className = '' ;
					}
				}
				if( className === '' )
				{
					throw new Error( '该组件无法在页面正常显示' ) ;
				}
				$( sdbjs.private.smallLabel ).removeClass().addClass( className ).css( { 'left': left, 'top': top } ) ;
			}
		}
	}
	if( domObj.parentNode !== document.body && isEvent === false )
	{
		sdbjs.fun.overEvent( domObj.parentNode ) ;
	}
}

/* 鼠标事件处理 */
sdbjs.fun.outEvent = function( domObj, isFirst )
{
	if( domObj === document || domObj === document.body )
	{
		return;
	}
	var isEvent = false ;
	var toggle = $( domObj ).attr( 'data-mouse' ) ;
	
	if( isFirst === true )
	{
		isFirst = false ;
	}
	
	if( typeof( toggle ) !== 'undefined' )
	{
		if( toggle === 'labelBox' )
		{
			isEvent = true ;
			$( sdbjs.private.smallLabel ).hide() ;
		}
	}
	if( domObj.parentNode !== document.body && isEvent === false )
	{
		sdbjs.fun.outEvent( domObj.parentNode ) ;
	}
}

/**************************************** 初始化 ****************************************/

/* 监听点击事件 */
$(document).on( 'click mouseover mouseout', function( jEvent ){
	var domObj = jEvent.target ;
	var type = jEvent.type ;
	if( type === 'click' )
	{
		sdbjs.fun.clickEvent( domObj, true ) ;
	}
	else if( type === 'mouseover' )
	{
		sdbjs.fun.overEvent( domObj, true ) ;
	}
	else if( type === 'mouseout' )
	{
		sdbjs.fun.outEvent( domObj, true ) ;
	}
} ) ;

/* 监听按键事件 */
$( document ).keydown( function(e){
	// esc
	if( e.which === 27 )
	{
		if( sdbjs.private.modalBox !== null )
		{
			sdbjs.parts.modalBox.hide( sdbjs.private.modalBox ) ;
		}
	}
} ) ;

$( window ).resize(function(){
	if( sdbjs.private.modalBox !== null )
	{
		sdbjs.parts.modalBox.redraw( sdbjs.private.modalBox ) ;
	}
} ) ;

/* 预加载 */
$(document).ready(function(){
	//判断浏览器是否能用
	var rj = sdbjs.fun.compatible() ;
	if( rj['rc'] === true )
	{
		//设置浏览器存储方式
		var saveType = sdbjs.fun.setBrowserStorage() ;
		if( saveType === '' )
		{
			alert( 'Error：系统不支持任何存储方式，请开启Cookie' ) ;
		}
	}
	else
	{
		alert( 'Error：系统不支持该浏览器' ) ;
	}
	//创建遮罩
	sdbjs.private.htmlScreen = $( '<div></div>' ).addClass( 'mask-screen alpha' ).appendTo( $( document.body ) ) ;
	//创建小标签
	sdbjs.private.smallLabel = $( '<div></div>' ).addClass( 'tooltip' ).html( '<div class="arrow"></div><div class="inner"></div>' ).appendTo( $( 'body' ) ) ;
	$( sdbjs.private.smallLabel ).on( 'mouseover', function(){
		$( this ).show() ;
	} ) ;
	$( sdbjs.private.smallLabel ).on( 'mouseout', function(){
		$( this ).hide() ;
	} ) ;
} ) ;

