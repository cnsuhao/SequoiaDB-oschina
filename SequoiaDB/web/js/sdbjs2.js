/*! sdbjs v2.0 | (c) 2014 sdbjs, Inc. */
if( typeof( sdbjs ) !== 'undefined' ){ throw new Error('Sdbjs注册失败') }

var sdbjs = new Object() ;

//-------------------------- private 变量 ------------------------//

sdbjs.private = new Object() ;

//半透明遮罩对象
sdbjs.private.htmlScreen = null ;
//透明遮罩对象
sdbjs.private.htmlScreen2 = null ;
//网格列表
sdbjs.private.gridList = {} ;
//自动修正宽高队列
sdbjs.private.autoList = [] ;
//当前打开的下拉菜单对象
sdbjs.private.cursorDropDown = null ;
//小标签对象
sdbjs.private.smallLabel = null ;

//-------------------------- public 变量 ------------------------//

sdbjs.public = new Object() ;

//浏览器宽度
sdbjs.public.width = 0 ;
//浏览器高度
sdbjs.public.height = 0 ;

//-------------------------- private 函数 ------------------------//


//-------------------------- public 函数 ------------------------//

sdbjs.fun = new Object() ;

/*
 * 添加自动修正队列
 * 参数1 属性   例子 { 'obj': 对象, 'id': id, 'style': { 'width': 'parseInt( sdbjs.public.width * 2 )', 'height': 'parseInt( sdbjs.public.height * 3 )' } }
 */
sdbjs.fun.autoCorrect = function( data )
{
	sdbjs.private.autoList.push( data ) ;
	for( var key in data['style'] )
	{
		var value = eval( data['style'][key] ) ;
		if( data['obj'] != undefined )
		{
			$( data['obj'] ).css( key, value ) ;
		}
		else
		{
			$( '#' + data['id'] ).css( key, value ) ;
		}
	}
}

/*
 * 获取格式化的当前时间  
 * 格式 YYYY/yyyy/YY/yy 表示年份  
 * MM/M 月份  
 * W/w 星期  
 * dd/DD/d/D 日期  
 * hh/HH/h/H 时间  
 * mm/m 分钟  
 * ss/SS/s/S 秒  
 */
sdbjs.fun.getDateFormat = function( formatStr )
{
	var date = new Date() ;
	var str = formatStr;   
	var Week = ['日','一','二','三','四','五','六'];
	str=str.replace(/yyyy|YYYY/,date.getFullYear());
	str=str.replace(/yy|YY/,(date.getYear() % 100)>9?(date.getYear() % 100).toString():'0' + (date.getYear() % 100));
	str=str.replace(/MM/,date.getMonth()>9?date.getMonth().toString():'0' + date.getMonth());
	str=str.replace(/M/g,date.getMonth());
	str=str.replace(/w|W/g,Week[date.getDay()]);
	str=str.replace(/dd|DD/,date.getDate()>9?date.getDate().toString():'0' + date.getDate());
	str=str.replace(/d|D/g,date.getDate());
	str=str.replace(/hh|HH/,date.getHours()>9?date.getHours().toString():'0' + date.getHours());
	str=str.replace(/h|H/g,date.getHours());
	str=str.replace(/mm/,date.getMinutes()>9?date.getMinutes().toString():'0' + date.getMinutes());
	str=str.replace(/m/g,date.getMinutes());
	str=str.replace(/ss|SS/,date.getSeconds()>9?date.getSeconds().toString():'0' + date.getSeconds());
	str=str.replace(/s|S/g,date.getSeconds());
	return str;
}

/*
 * 限制字符串长度
 * 参数1 字符串
 * 参数2 最大长度
 * 参数3 超出部分的字符数，算入最大长度，默认 ...
 */
sdbjs.fun.limitString = function( str, s_max, delstr )
{
	var delstrLen = 0 ;
	if( str.length <= s_max )
	{
		return str ;
	}
	if( delstr == undefined )
	{
		delstr = '...' ;
	}
	delstrLen = delstr.length ;
	s_max -= delstrLen ;
	if( s_max < 0 )
	{
		s_max = 0 ;
	}
	return str.substr( 0, s_max ) + delstr ;
}

/*
 * 检查字符串
 * 参数1 字符串
 * 参数2 最小长度
 * 参数3 最大长度
 */
sdbjs.fun.checkString = function( str, s_min, s_max )
{
	if ( typeof( str ) == 'string' )
	{
		var len = str.length ;
		if ( len < s_min || len > s_max )
		{
			return false ;
		}
		return true ;
	}
	else
	{
		return false ;
	}
}

/*
 * 检查命名
 * 参数1 字符串
*/
sdbjs.fun.checkStrName = function( str )
{
	if ( !sdbjs.fun.checkString( str, 1, 255 ) )
	{
		return false ;
	}
	var first = str.charAt( 0 ) ;
	if ( ( first < 'a' || first > 'z' ) &&
	     ( first < 'A' || first > 'Z' ) &&
		    first != '_' )
	{
		return false ;
	}
	var len = str.length ;
	for( var i = 1; i < len; ++i )
	{
		var char = str.charAt( i ) ;
		if ( ( char < 'a' || char > 'z' ) &&
	     	  ( char < 'A' || char > 'Z' ) &&
			  ( char < '0' || char > '9' ) &&
		       char != '_' )
		{
			return false ;
		}
	}
	return true ;
}

/*
 * 检查整数
 * 参数1 字符串或整数
 * 参数2 最小值
 * 参数3 最大值
 */
sdbjs.fun.checkInt = function( num, n_min, n_max )
{
	var number = 0 ;
	if ( typeof( num ) == 'string' )
	{
		var type = "^-?\\d+$" ; 
		var re = new RegExp( type ) ;
		if ( num.match( re ) == null )
		{
			return false ;
		}
		number = parseInt( num ) ;
	}
	else if ( typeof( num ) == 'number' )
	{
		if ( parseInt( num ) != num  )
		{
			return false ;
		}
		number = parseInt( num ) ;
	}
	else
	{
		return false ;
	}
	if ( n_min != '' && number < parseInt( n_min ) )
	{
		return false ;
	}
	if ( n_max != '' && number > parseInt( n_max ) )
	{
		return false ;
	}
	return true ;
}

/*
 * 检查端口
 * 参数1 字符串
*/
sdbjs.fun.checkPort = function( str )
{
	var len = str.length ;
	if ( len <= 0 )
	{
		return false ;
	}
	if ( str.charAt( 0 ) == '0' )
	{
		return false ;
	}
	for ( var i = 0; i < len; ++i )
	{
		var char = str.charAt( i ) ;
		if ( char < '0' || char > '9' )
		{
			return false ;
		}
	}
	var port = parseInt( str ) ;
	if ( port <= 0 || port > 65535 )
	{
		return false ;
	}
	return true ;
}

/*
 * 把普通字符转换成html字符
 */
sdbjs.fun.htmlEncode = function( str )
{
	var s = "" ;
	str = str + '' ;
	if( str.length == 0 ) return "" ;
	s = str.replace( /&/g, "&amp;" ) ;
	s = s.replace( /</g, "&lt;" ) ;
	s = s.replace( />/g, "&gt;" ) ;
	s = s.replace( / /g, "&nbsp;" ) ;
	s = s.replace( /\'/g, "&#39;" ) ;
	s = s.replace( /\"/g, "&quot;" ) ;
	s = s.replace( /\n/g, "<br>" ) ;
	return s ;
}

/*
 * 把html字符转换成普通字符
 */
sdbjs.fun.htmlDecode = function( str )
{
	var s = "" ;
	str = str + '' ;
	if( str.length ==0 ) return "" ;
	s = str.replace( /&amp;/g, "&" ) ;
	s = s.replace( /&lt;/g, "<" ) ;
	s = s.replace( /&gt;/g, ">" ) ;
	s = s.replace( /&nbsp;/g, " " ) ;
	s = s.replace( /&#39;/g, "\'" ) ;
	s = s.replace( /&quot;/g, "\"" ) ;
	s = s.replace( /<br>/g, "\n" ) ;
	return s ; 
}

/*
 * 获取浏览器可见宽度
 */
sdbjs.fun.getWindowWidth = function()
{
	var width = $( window ).width() ;
	if ( width < 970 )
	{
		if ( width == 0 )
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
	if ( height < 500 )
	{
		if ( height == 0 )
		{
			throw new Error('获取浏览器高度失败') ;
		}
		height = 500 ;
	}
	return height ;
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
		temp = '0' ;
		info.push( 'unknow' ) ;
	}
	verinfo = ( temp + '' ).replace(/[^0-9.]/ig, '' ) ;
	info.push( verinfo ) ;
	return info ;
}

/*
 * 网格修正宽度
 */
sdbjs.fun.gridRevise = function( id )
{
	var width = $( '#' + id ).width() - 17  ;
	var scale = $( '#' + id ).data( 'scale' ) ;
	var len = scale.length ;
	var cellWidth = [] ;
	var tempSum = 0 ;
	for ( var i = 0; i < len; ++i )
	{
		var tempNum = 0 ;
		if( i + 1 == len ){ tempNum = width - tempSum ; cellWidth.push( tempNum - 21 ) ; }
		else{ tempNum = parseInt( width * scale[i] / 100 ) ; tempSum += tempNum ; cellWidth.push( tempNum - 20 ) ; }
	}
	$( '#' + id + ' > .grid-title' ).css( 'width', width ) ;
	//修正标题
	var titleLen = sdbjs.private.gridList[id]['title'].length ;
	for( var i = 0; i < titleLen; ++i )
	{
		var titleThLen = sdbjs.private.gridList[id]['title'][i]['ths'].length ;
		for( var k = 0; k < titleThLen; ++k )
		{
			$( sdbjs.private.gridList[id]['title'][i]['ths'][k] ).css( { 'width': function(index){ return cellWidth[ k ] }, 'height': 'auto' } ) ;
		}
		for( var k = 0; k < titleThLen; ++k )
		{
			$( sdbjs.private.gridList[id]['title'][i]['ths'][k] ).css( 'height', $( sdbjs.private.gridList[id]['title'][i]['tr'] ).height() - 10 ) ;
		}
	}
	//修正内容
	var bodyLen = sdbjs.private.gridList[id]['body'].length ;
	for( var i = 0; i < bodyLen; ++i )
	{
		var titleTdLen = sdbjs.private.gridList[id]['body'][i]['tds'].length ;
		for( var k = 0; k < titleTdLen; ++k )
		{
			$( sdbjs.private.gridList[id]['body'][i]['tds'][k] ).css( { 'width': function(index){ return cellWidth[ k ] }, 'height': 'auto' } ) ;
		}
		for( var k = 0; k < titleTdLen; ++k )
		{
			$( sdbjs.private.gridList[id]['body'][i]['tds'][k] ).css( 'height', $( sdbjs.private.gridList[id]['body'][i]['tr'] ).height() - 12 ) ;
		}
	}
	//修正内容离开标题
	$( '#' + id + ' > .grid-body' ).css( { 'marginTop': function(){
		var sumTitleHeight = 0 ;
		for( var i = 0; i < titleLen; ++i )
		{
			sumTitleHeight += $( sdbjs.private.gridList[id]['title'][i]['tr'] ).height() ;
		}
		return sumTitleHeight ;
	} } ) ;
}

/*
 * 打开模态框
 * 参数1 id
 */
sdbjs.fun.openModal = function( id )
{
	$( sdbjs.private.htmlScreen ).data( 'id', id ).show() ;
	$( '#' + id ).show().css( { 'left': parseInt( ( sdbjs.public.width - $( '#' + id ).outerWidth() ) * 0.5 ),
										 'top':  parseInt( ( sdbjs.public.height - $( '#' + id ).outerHeight() ) * 0.5 ) } ) ;
}

/*
 * 关闭模态框
 * 参数1 id
 */
sdbjs.fun.closeModal = function( id )
{
	if( $( sdbjs.private.htmlScreen ).data( 'id' ) == id )
	{
		$( sdbjs.private.htmlScreen ).hide() ;
	}
	$( '#' + id ).hide() ;
}

/*
 * 修正模态框的位置
 */
sdbjs.fun.moveModal = function( id )
{
	$( '#' + id ).css( { 'left': parseInt( ( sdbjs.public.width - $( '#' + id  ).outerWidth() ) * 0.5 ),
		                  'top':  parseInt( ( sdbjs.public.height - $( '#' + id  ).outerHeight() ) * 0.5 ) } ) ;
}

/*
 * 打开全局加载
 * 参数1 id
 */
sdbjs.fun.openLoading = function( id )
{
	$( '#' + id + ' > .loading-font' ).text( '0%' ) ;
	$( sdbjs.private.htmlScreen ).data( 'id', id ).show() ;
	$( '#' + id ).show() ;
}

/*
 * 关闭全局加载
 * 参数1 id
 */
sdbjs.fun.closeLoading = function( id )
{
	if( $( sdbjs.private.htmlScreen ).data( 'id' ) == id )
	{
		$( sdbjs.private.htmlScreen ).hide() ;
	}
	$( '#' + id ).hide() ;
}

/*
 * 设置加载的进度
 * 参数1 id
 * 参数2 百分比
 */
sdbjs.fun.setLoading = function( id, percent )
{
	$( '#' + id + ' > .loading-font' ).text( percent + '%' ) ;
}

/*
 * 设置加载的总时间
 * 参数1 id
 * 参数2 秒
 */
sdbjs.fun.timeLoading = function( id, seconds )
{
	var step = seconds * 10 ;
	var percent = 0 ;
	var int = setInterval( function(){
		percent += 1 ;
		if( percent == 100 || $( '#' + id ).is( ':hidden' ) )
		{
			clearInterval( int ) ;
		}
		else
		{
			sdbjs.fun.setLoading( id, percent ) ;
		}
	}, 100 ) ;
}

/*
 * 为指定的对象创建事件
 */
sdbjs.fun.addOnEvent = function( obj )
{
	var type = $( obj ).attr( 'data-type' ) ;
	if( type == 'close-modal' )
	{
		$( obj ).on( 'click', function(){
			var id = $( this ).attr( 'data-target' ) ;
			sdbjs.fun.closeModal( id ) ;
		} ) ;
	}
	else if( type == 'open-modal' )
	{
		$( obj ).on( 'click', function(){
			var id = $( this ).attr( 'data-target' ) ;
			sdbjs.fun.openModal( id ) ;
		} ) ;
	}
	else if( type == 'switch-dropDown' )
	{
		$( obj ).on( 'click', function(){
			var id = $( this ).attr( 'data-target' ) ;
			if( $( '#' + id + '_menu' ).is( ':hidden' ) )
			{
				$( sdbjs.private.htmlScreen2 ).show() ;
				sdbjs.private.cursorDropDown = obj ;
			}
			else
			{
				$( sdbjs.private.htmlScreen2 ).hide() ;
				sdbjs.private.cursorDropDown = null ;
			}
			var left = $( this ).offset().left ;
			var top = $( this ).offset().top + $( this ).height() ;
			if( $( '#' + id + '_menu' ).outerHeight() + top > sdbjs.public.height )
			{
				top = $( this ).offset().top - $( '#' + id + '_menu' ).outerHeight() ;
			}
			$( '#' + id + '_menu' ).css( { 'left': left, 'top': top } ).toggle() ;
		} ) ;
	}
	else if( type == 'toggle-fold' )
	{
		$( obj ).on( 'click', function(){
			var id = $( this ).attr( 'data-target' ) ;
			if( $( '#' + id + ' > .fold-body' ).is( ':hidden' ) )
			{
				if( $( this ).attr( 'data-model' ) == 'mutex' )
				{
					$( ".fold[data-name='" + $( '#' + id ).attr( 'data-name' ) + "'] > .fold-body" ).hide() ;
					$( ".fold[data-name='" + $( '#' + id ).attr( 'data-name' ) + "'] > .fold-header > span > .caret" ).removeClass().addClass( 'caret caret-right' ) ;
				}
				$( '#' + id + ' > .fold-body' ).show() ;
				if( $( '#' + id + ' > .fold-body' ).offset().top > $( '#' + id + ' > .fold-header' ).offset().top )
				{
					$( '#' + id + ' > .fold-header > span > .caret' ).removeClass().addClass( 'caret caret-bottom' ) ;
				}
				else
				{
					$( '#' + id + ' > .fold-header > span > .caret' ).removeClass().addClass( 'caret caret-top' ) ;
				}
			}
			else
			{
				$( '#' + id + ' > .fold-header > span > .caret' ).removeClass().addClass( 'caret caret-right' ) ;
				$( '#' + id + ' > .fold-body' ).hide() ;
			}
		} ) ;
	}
	else if( type == 'tooltip' )
	{
		$( obj ).on( 'mouseover', function(){
			var text = $( this ).data( 'text' ) ;
			if( text == undefined )
			{
				text = $( this ).attr( 'data-text' ) ;
			}
			if( text != undefined )
			{
				var left = 0 ;
				var top = 0 ;
				var buttonL = $( this ).offset().left ;
				var buttonT = $( this ).offset().top ;
				var buttonW = $( this ).outerWidth() ;
				var buttonH = $( this ).outerHeight() ;
				
				$( sdbjs.private.smallLabel ).removeClass().addClass( 'tooltip' ).show().children( ':eq(1)' ).html( text ) ;
				
				var tooltipW = $( sdbjs.private.smallLabel ).outerWidth() ;
				var tooltipH = $( sdbjs.private.smallLabel ).outerHeight() ;
				
				var className = '' ;
				
				if( buttonL + buttonW + tooltipW < sdbjs.public.width && className == '' )
				{
					className = 'tooltip right' ;
					left = buttonL + buttonW ;
					if( buttonT + parseInt( buttonH * 0.5 ) + tooltipH - 14 < sdbjs.public.height )
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
				if( tooltipW + 6 < buttonL && className == '' )
				{
					className = 'tooltip left' ;
					left = buttonL - tooltipW - 9 ;
					if( buttonT + parseInt( buttonH * 0.5 ) + tooltipH - 14 < sdbjs.public.height )
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
				if( tooltipW - 27 < buttonL && className == '' )
				{
					className = 'tooltip top top-right' ;
					left = buttonL - tooltipW + 27 ;
					if( tooltipH + 7 < buttonT )
					{
						top = buttonT - tooltipH - 10 ;
					}
					else if ( tooltipH + 7 + buttonT + buttonH < sdbjs.public.height )
					{
						className = 'tooltip bottom bottom-right' ;
						top = buttonT + buttonH ;
					}
					else
					{
						className = '' ;
					}
				}
				if ( buttonL + buttonW + tooltipW - 27 < sdbjs.public.width && className == '' )
				{
					className = 'tooltip top top-left' ;
					left = buttonL + buttonW - 27 ;
					if( tooltipH + 7 < buttonT )
					{
						top = buttonT - tooltipH - 10 ;
					}
					else if ( tooltipH + 7 + buttonT + buttonH < sdbjs.public.height )
					{
						className = 'tooltip bottom bottom-left' ;
						top = buttonT + buttonH ;
					}
					else
					{
						className = '' ;
					}
				}
				if ( parseInt( tooltipW * 0.5 ) < buttonL && parseInt( tooltipW * 0.5 ) + buttonL + parseInt( buttonW * 0.5 ) < sdbjs.public.width && className == '' )
				{
					className = 'tooltip' ;
					if( tooltipH + 10 < buttonT )
					{
						className += ' top' ;
						top = buttonT - tooltipH - 10 ;
						left = buttonL + parseInt( buttonW * 0.5 ) - parseInt( tooltipW * 0.5 ) ;
					}
					else if ( buttonT + buttonH + tooltipH + 10 < sdbjs.public.height )
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
				if( className == '' )
				{
					throw new Error( '该组件无法在页面正常显示' ) ;
				}
				$( sdbjs.private.smallLabel ).removeClass().addClass( className ).css( { 'left': left, 'top': top } ) ;
			}
		} ) ;
		$( obj ).on( 'mouseout', function(){
			$( sdbjs.private.smallLabel ).hide() ;
		} ) ;
	}
	else
	{
		if( $( obj ).hasClass( 'dropdown-menu' ) )
		{
			$( obj ).children( 'li' ).on( 'click', function(){
				$( sdbjs.private.htmlScreen2 ).hide() ;
				$( this ).parent().hide() ;
			} ) ;
		}
	}
}

/*
 * 创建完成后调用
 */
sdbjs.fun.endOfCreate = function()
{
	$( "[data-type='close-modal']" ).each(function(index, element) {
      sdbjs.fun.addOnEvent( $( this ) ) ;
   });
	
	$( "[data-type='open-modal']" ).each(function(index, element) {
      sdbjs.fun.addOnEvent( $( this ) ) ;
   });
	
	$( "[data-type='switch-dropDown']" ).each(function(index, element) {
      sdbjs.fun.addOnEvent( $( this ) ) ;
   });

	$( ".dropdown-menu" ).each(function(index, element) {
      sdbjs.fun.addOnEvent( $( this ) ) ;
   });

	$( sdbjs.private.htmlScreen2 ).on( 'click', function(){
		$( ".dropdown-menu:not(:hidden)" ).hide() ;
		$( this ).hide() ;
	} ) ;
	
	$( "[data-type='toggle-fold']" ).each(function(index, element) {
      sdbjs.fun.addOnEvent( $( this ) ) ;
   });
	
	$( "[data-type='tooltip']" ).each(function(index, element) {
      sdbjs.fun.addOnEvent( $( this ) ) ;
   });
}

/*
 * 解析json字符串
 * 返回数组json对象
 */
sdbjs.fun.parseJson = function( str )
{
	var json_array = new Array() ;
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
						try{	json = eval( '(' + subStr + ')' ) ;	json_array.push( json ) ;	}catch(e){}
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
 * 判断是不是数组
 * 参数1 对象
*/
sdbjs.fun.isArray = function( object )
{
	 return  object && typeof object==='object' &&    
            typeof object.length==='number' &&  
            typeof object.splice==='function' &&     
            !(object.propertyIsEnumerable('length'));
}

/*
 * 解析ip ip段 hostname  hostname段
 * 返回数组  [ { 'address': 'xxx', 'type': 'host' }, { 'address': 'xxx', 'type': 'ip' } ]
 */
sdbjs.fun.getHostList = function( address )
{
	//数字字符串自动补零
	function pad(num, n)
	{
		var len = num.toString().length;
		while(len < n)
		{
			num = "0" + num;
			len++;
		}
		return num;
	}
	var link_search = [] ;
	var splitAddress = address.split( /[,\s;]/ ) ;
	var splitLen = splitAddress.length ;
	for( var strNum = 0; strNum < splitLen; ++strNum )
	{
		var str = $.trim( splitAddress[ strNum ] ) ;
		var ip_search = [] ;
		var host_search = [] ;
		var matches = new Array() ;
		//识别主机字符串，扫描主机
		var reg = new RegExp(/^(((2[0-4]\d|25[0-5]|[01]?\d\d?)|(\[[ ]*(2[0-4]\d|25[0-5]|[01]?\d\d?)[ ]*\-[ ]*(2[0-4]\d|25[0-5]|[01]?\d\d?)[ ]*\]))\.){3}((2[0-4]\d|25[0-5]|[01]?\d\d?)|(\[(2[0-4]\d|25[0-5]|[01]?\d\d?)\-(2[0-4]\d|25[0-5]|[01]?\d\d?)\]))$/) ;
		if ( ( matches = reg.exec( str ) ) != null )
		{
			//ip区间
			var ip_arr = str.split(".") ;
			for ( ipsub in ip_arr )
			{
				reg = new RegExp(/^((2[0-4]\d|25[0-5]|[01]?\d\d?)|(\[[ ]*(2[0-4]\d|25[0-5]|[01]?\d\d?)[ ]*\-[ ]*(2[0-4]\d|25[0-5]|[01]?\d\d?)[ ]*\]))$/) ;
				if ( ( matches = reg.exec( ip_arr[ipsub] ) ) != null )
				{
					//匹配每个数值
					if ( ( matches[4] == undefined || matches[5] == undefined ) ||
						  ( matches[4] === '' || matches[5] === '' ) )
					{
						//这是一个数字 192
						ip_search.push( matches[0] ) ;
					}
					else
					{
						//这是一个区间 [1-10]
						ip_search.push( new Array( matches[4], matches[5] ) ) ;
					}
				}
			}
		}
		else
		{
			//主机名
			reg = new RegExp(/^((.*)(\[[ ]*(\d+)[ ]*\-[ ]*(\d+)[ ]*\])(.*))$/) ;
			if ( ( matches = reg.exec( str ) ) != null )
			{
				host_search.push( matches[2] ) ;
				host_search.push( matches[4] ) ;
				host_search.push( matches[5] ) ;
				host_search.push( matches[6] ) ;
			}
			else
			{
				host_search = str ;
			}
		}
	
		if ( ip_search.length > 0 )
		{
			//遍历数组，把IP段转成每个IP存入数组
			for( var i = ( sdbjs.fun.isArray( ip_search[0] ) ? parseInt(ip_search[0][0]) : 0 ), i_end = ( sdbjs.fun.isArray( ip_search[0] ) ? parseInt(ip_search[0][1]) : 0 ); i <= i_end; ++i )
			{
				for( var j = ( sdbjs.fun.isArray( ip_search[1] ) ? parseInt(ip_search[1][0]) : 0 ), j_end = ( sdbjs.fun.isArray( ip_search[1] ) ? parseInt(ip_search[1][1]) : 0 ); j <= j_end; ++j )
				{
					for( var k = ( sdbjs.fun.isArray( ip_search[2] ) ? parseInt(ip_search[2][0]) : 0 ), k_end = ( sdbjs.fun.isArray( ip_search[2] ) ? parseInt(ip_search[2][1]) : 0 ); k <= k_end; ++k )
					{
						for( var l = ( sdbjs.fun.isArray( ip_search[3] ) ? parseInt(ip_search[3][0]) : 0 ), l_end = ( sdbjs.fun.isArray( ip_search[3] ) ? parseInt(ip_search[3][1]) : 0 ); l <= l_end; ++l )
						{
							link_search.push( { 'address': (( sdbjs.fun.isArray( ip_search[0] ) ? i : ip_search[0] )+'.'+( sdbjs.fun.isArray( ip_search[1] ) ? j : ip_search[1] )+'.'+( sdbjs.fun.isArray( ip_search[2] ) ? k : ip_search[2] )+'.'+( sdbjs.fun.isArray( ip_search[3] ) ? l : ip_search[3] )), 'type': 'ip' } ) ;
						}
					}
				}
			}
		}
	
	
		if ( host_search.length > 0 )
		{
			//转换hostname
			if ( sdbjs.fun.isArray( host_search ) )
			{
				var str_start = host_search[0] ;
				var str_end   = host_search[3] ;
				var strlen_num = host_search[1].length ;
				var strlen_temp  = parseInt(host_search[1]).toString().length ;
				var need_add_zero = false ;
				if ( strlen_num > strlen_temp )
				{
					need_add_zero = true ;
				}
				for ( var i = parseInt(host_search[1]), i_end = parseInt(host_search[2]); i <= i_end ; ++i )
				{
					if ( need_add_zero && i.toString().length <= strlen_num )
					{
						link_search.push( { 'address': str_start + pad(i,strlen_num) + str_end, 'type': 'host' } ) ;
					}
					else
					{
						link_search.push( { 'address': str_start + i + str_end, 'type': 'host' } ) ;
					}
				}
			}
			else
			{
				link_search.push( { 'address': host_search, 'type': 'host' } ) ;
			}
		}
	}
	return link_search ;
}

//---------------------------------------- public 组件 ----------------------------------------//

sdbjs.parts = new Object() ;

//------------------------------ 标签列表 --------------------------------//

sdbjs.parts.tabList = new Object() ;

/*
 * 创建
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
*/
sdbjs.parts.tabList.create = function( id, style )
{
	return $( '<ul></ul>' ).addClass( 'tag-list' ).css( style ).attr( 'id', id ) ;
}

/*
 * 添加
 * 参数1 id
 * 参数2 数据		例子 [ { 'name': '11', 'sname': '22', 'text': '11', 'event': '' }, { ... }, ... ]
 */
sdbjs.parts.tabList.add = function( id, data )
{
	var obj = $( '#' + id ) ;
	var len = data.length ;
	for( var i = 0; i < len; ++i )
	{
		var row   = $( '<li></li>' ).addClass( 'tag-list-row' ) ;
		var txt   = $( '<div></div>' ).addClass( 'tag-list-row-txt' ) ;
		var txt_1 = $( '<div></div>' ).addClass( 'tag-list-row-txt-major' ).text( data[i]['name'] ).appendTo( txt ) ;
		var txt_2 = $( '<div></div>' ).addClass( 'tag-list-row-txt-less' ).text( data[i]['sname'] ).appendTo( txt ) ;
		var right = $( '<div></div>' ).css( { 'paddingTop': '6px', 'float': 'right' } ) ;
		var clear = $( '<div></div>' ).addClass( 'clear-float' ) ;
		if( data[i]['text'] != undefined ){ $( right ).append( data[i]['text'] ).appendTo( row ) }
		if( data[i]['event'] != undefined ){ $( txt ).get(0).onclick = Function( data[i]['event'] ) }
		$( txt ).appendTo( row ) ;
		$( clear ).appendTo( row ) ;
		$( obj ).append( row ) ;
	}
}

/*
 * 设置为选择样式
 * 参数1 id
 * 参数2 第几个
 */
sdbjs.parts.tabList.active = function( id, num )
{
	$( '#' + id + ' > li:eq(' + num + ')' ).removeClass().addClass( 'tag-list-row-active' ).children().eq(1).each(function(index, element) {
      $( this ).children().eq(0).removeClass().addClass( 'tag-list-row-txt-major-active' ) ;
		$( this ).children().eq(1).removeClass().addClass( 'tag-list-row-txt-less-active' ) ;
   });
}

/*
 * 设置为禁用样式
 * 参数1 id
 * 参数2 第几个
 */
sdbjs.parts.tabList.disable = function( id, num )
{
	$( '#' + id + ' > li:eq(' + num + ')' ).removeClass().addClass( 'tag-list-row-off' ).children().eq(1).each(function(index, element) {
      $( this ).children().eq(0).removeClass().addClass( 'tag-list-row-txt-major-off' ) ;
		$( this ).children().eq(1).removeClass().addClass( 'tag-list-row-txt-less-off' ) ;
   });
}

/*
 * 设置为普通样式
 * 参数1 id
 * 参数2 第几个
 */
sdbjs.parts.tabList.unActive = function( id, num )
{
	$( '#' + id + ' > li:eq(' + num + ')' ).removeClass().addClass( 'tag-list-row' ).children().eq(1).each(function(index, element) {
      $( this ).children().eq(0).removeClass().addClass( 'tag-list-row-txt-major' ) ;
		$( this ).children().eq(1).removeClass().addClass( 'tag-list-row-txt-less' ) ;
   });
}

/*
 * 获取当前样式
 * 参数1 id
 * 参数2 第几个
 */
sdbjs.parts.tabList.getStatus = function( id, num )
{
	if( $( '#' + id ).children( ':eq(' + num + ')' ).hasClass( 'tag-list-row' ) )
	{
		return 'unActive' ;
	}
	else if( $( '#' + id ).children( ':eq(' + num + ')' ).hasClass( 'tag-list-row-off' ) )
	{
		return 'disabled' ;
	}
	else if( $( '#' + id ).children( ':eq(' + num + ')' ).hasClass( 'tag-list-row-active' ) )
	{
		return 'active' ;
	}
	else
	{
		return 'unknow' ;
	}
}


//------------------------------ 进度条(有字) --------------------------------//

sdbjs.parts.progressBox = new Object() ;

/*
 * 创建
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
 * 参数3 进度		0 - 100
 * 参数4 颜色		green, orange, red
 * 参数5 文字
 */
sdbjs.parts.progressBox.create = function( id, style, percent, color, text )
{
	var progress = $( '<div></div>' ).addClass( 'progress2' ).css( style ).attr( 'id', id ) ;
	$( '<span></span>' ).addClass( 'reading' ).text( text ).appendTo( $( progress ) ) ;
	$( '<span></span>' ).addClass( 'bar ' + color ).css( 'width', percent + '%' ).appendTo( $( progress ) ) ;
	return progress ;
}
	
sdbjs.parts.progressBox.update = function( id, percent, color, text )
{
	if( text != undefined )
	{
		$( '#' + id + ' > span:first-child' ).text( text ) ;
	}
	if( color != undefined )
	{
		$( '#' + id + ' > span:last-child' ).removeClass().addClass( 'bar ' + color ) ;
	}
	if( percent != undefined )
	{
		$( '#' + id + ' > span:last-child' ).css( { 'width': percent + '%' } ) ;
	}
}

//------------------------------ 进度条(没有字) --------------------------------//

sdbjs.parts.progressBox2 = new Object() ;

/*
 * 创建
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
 * 参数3 进度		0 - 100
 */
sdbjs.parts.progressBox2.create = function( id, style, percent )
{
	var progress = $( '<div></div>' ).addClass( 'progress' ).css( style ).attr( 'id', id ) ;
	var progress_bar = $( '<div></div>' ).addClass( 'progress-bar' ).css( { 'width': percent + '%' } ).appendTo( $( progress ) ) ;
	return progress ;
}

sdbjs.parts.progressBox2.update = function( id, percent )
{
	$( '#' + id + ':first-child' ).css( { 'width': percent + '%' } ) ;
}

//------------------------------ 模态框(没有字) --------------------------------//

sdbjs.parts.modalBox = new Object() ;

/*
 * 创建
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
 * 参数3 部件		{ 'header': true, 'body': true, 'foot': true }
 */
sdbjs.parts.modalBox.create = function( id, style, parts )
{
	var modalbox, modalheader, modalbtn, modalclear, modalbody, modalfoot ;
	var modalbox = $( '<div></div>' ).addClass( 'modal' ).css( style ).attr( 'id', id ) ;
	if( parts['header'] == true )
	{
		modalheader = $( '<div></div>' ).addClass( 'modal-title' ).appendTo( $( modalbox ) ) ;
		modalbtn   = $( '<button></button>' ).append( '&times;' ).addClass( 'close-buttun' ).attr( { 'data-type': 'close-modal', 'data-target': id } ).appendTo( $( modalheader ) ) ;
		modalclear = $( '<div></div>' ).addClass( 'clear-float' ).appendTo( $( modalheader ) ) ;
	}
	if( parts['body'] == true )
	{
		modalbody = $( '<div></div>' ).addClass( 'modal-body' ).appendTo( $( modalbox ) ) ;
	}
	if( parts['foot'] == true )
	{
		modalfoot = $( '<div></div>' ).addClass( 'modal-foot' ).appendTo( $( modalbox ) ) ;
	}
	return modalbox ;
}

//------------------------------ 徽章 --------------------------------//

sdbjs.parts.badgeBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
 * 参数3 内容
 * 参数4 类型		warning, danger
*/
sdbjs.parts.badgeBox.create = function( id, style, text, type )
{
	return $( '<span></span>' ).addClass( 'badge badge-' + type ).css( style ).attr( 'id', id ).text( text ) ;
}

//------------------------------ 按钮 --------------------------------//

sdbjs.parts.buttonBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
 * 参数3 内容
 * 参数4 类型		primary, warning, danger, success
 * 参数5 大小		lg, mg
*/
sdbjs.parts.buttonBox.create = function( id, style, text, type, size )
{
	var strclass = 'btn' ;
	if ( size != undefined ){ strclass = strclass + ' btn-' + size }
	if ( type != undefined ){ strclass = strclass + ' btn-' + attribute_json['type'] }else{ strclass = strclass + ' btn-default' }
	return $( '<button></button>' ).addClass( strclass ).text( text ).css( style ).attr( 'id', id ) ;
}

//------------------------------ 下拉菜单 --------------------------------//

sdbjs.parts.dropDownBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style		例子 { 'padding': '0 0 0 0' }
 * 参数3 内容
 * 参数4 列表			[ { 'text': '123', status: true, event: '' }, { 'text': '' } ]
 * 参数5 大小			lg, mg
*/
sdbjs.parts.dropDownBox.create = function( id, style, text, list, size )
{
	var buttonClass = 'btn btn-default' ;
	if( size != undefined ){ buttonClass += ' btn-' + size }
	var obj = $( '<div></div>' ).addClass( 'dropdown' ).attr( 'id', id ).css( style ) ;
	$( '<button></button>' ).addClass( buttonClass ).attr( { 'data-type': 'switch-dropDown', 'data-target': id } ).append( text + '<span class="caret caret-bottom"></span>' ).appendTo( $( obj ) ) ;
	$( '<div></div>' ).addClass( 'clear-float' ).appendTo( $( obj ) ) ;
	var ul = $( '<ul></ul>' ).addClass( 'dropdown-menu' ).attr( 'id', id + '_menu' ).appendTo( $( 'body' ) ) ;
	var len = list.length ;
	for( var i = 0; i < len; ++i )
	{
		var li_json = list[i] ;
		var li = $( '<li></li>' ) ;
		if ( li_json['status'] == false ){ $( li ).addClass( 'drop-disabled' ) }
		if ( li_json['text'] == '' ){ $( li ).removeClass().addClass( 'divider' ) }else{ $( li ).text( li_json['text'] ) }
		if ( li_json['event'] != undefined ){ $( li ).addClass( 'event' ).get(0).onclick = Function( li_json['event'] ) }
		$( li ).appendTo( $( ul ) ) ;
	}
	return obj ;
}

//------------------------------ 简单表格 --------------------------------//

sdbjs.parts.tableBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style		例子 { 'padding': '0 0 0 0' }
 * 参数3 类型			empty, simple, noborder, border
*/
sdbjs.parts.tableBox.create = function( id, style, type )
{
	return $( '<table></table>' ).addClass( type + '-table' ).css( style ).attr( 'id', id ).append( '<tbody></tbody>' ) ;
}

/*
 * 参数1 id
 * 参数2 列表	例子 [ { style: { 'width': '20px' }, cell: [ { 'text': 'hello', style: {...}, colspan: 2, { 'text': 'hello2', style: {...} } ] }, [ ... ] ]
*/
sdbjs.parts.tableBox.add = function( id, list )
{
	var rowLen = list.length
	for( var i = 0; i < rowLen; ++i )
	{
		var row = $( '<tr></tr>' ).appendTo( $( '#' + id ) ) ;
		if( list[i]['style'] != undefined ){ $( row ).css( list[i]['style'] ) }
		var cellLen = list[i]['cell'].length ;
		for( var k = 0; k < cellLen; ++k )
		{
			var cell = $( '<td></td>' ).append( list[i]['cell'][k]['text'] ).appendTo( $( row ) ) ;
			if( list[i]['cell'][k]['style'] != undefined ){ $( cell ).css( list[i]['cell'][k]['style'] ) }
			if( list[i]['cell'][k]['colspan'] != undefined )
			{
				$( cell ).attr( 'colspan', parseInt( list[i]['cell'][k]['colspan'] ) ) ;
			}
		}
	}
}

//------------------------------ 折叠栏 --------------------------------//

sdbjs.parts.foldBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style			例子 { 'padding': '0 0 0 0' }
 * 参数3 标题	
 * 参数4 内容
 * 参数5 默认是否展开	true, false
 * 参数6 是否唯一展开
 * 参数7 排斥ID			如果是唯一展开，填写excludeID，如果有其他相同的excludeID，将会互为排斥
*/
sdbjs.parts.foldBox.create = function( id, style, title, text, isOpen, isOnly, excludeID )
{
	var obj        = $( '<div></div>' ).addClass( 'fold' ).attr( { 'id': id, 'data-name': excludeID } ).css( style ) ;
	var foldHeader = $( '<div></div>' ).addClass( 'fold-header' ).appendTo( obj ) ;
	var foldBody = $( '<div></div>' ).addClass( 'fold-body' ).append( text ).appendTo( obj ) ;
	var titleObj   = $( '<span></span>' ).addClass( 'fold-point' ).attr( { 'data-type': 'toggle-fold', 'data-target': id, 'data-model': ( isOnly ? 'mutex' : 'share' ) } ).appendTo( foldHeader ) ;
	if ( isOpen == true )
	{
		$( titleObj ).append( '<span class="caret caret-bottom"></span> ' + title ) ;
		$( foldBody ).show() ;
	}
	else
	{
		$( titleObj ).append( '<span class="caret caret-right"></span> ' + title ) ;
		$( foldBody ).hide() ;
	}
	return obj ;
}

//------------------------------ 面板 --------------------------------//

sdbjs.parts.panelBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style			例子 { 'padding': '0 0 0 0' }
 * 参数3 标题	
 * 参数4 内容
 * 参数5 是否有标题		true, false; 如果设置false, 参数3 无效
*/
sdbjs.parts.panelBox.create = function( id, style, title, text, hasHeader )
{
	var obj = $( '<div></div>' ).addClass( 'panel' ).attr( 'id', id ).css( style ) ;
	if( hasHeader != false )
	{
		$( '<div></div>' ).addClass( 'panel-header' ).append( title ).appendTo( $( obj ) ) ;
	}
	$( '<div></div>' ).addClass( 'panel-body' ).append( text ).appendTo( $( obj ) ) ;
	return obj ;
}

//------------------------------ 分页栏 --------------------------------//

sdbjs.parts.tabPageBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style		例子 { 'padding': '0 0 0 0' }
 * 参数3 列表			例子 [ { 'text': '123', 'event': 'alert(123)' }, { 'text': '456', 'status': 'active' } ] }
*/
sdbjs.parts.tabPageBox.create = function( id, style, list )
{
	var ul = $( '<ul></ul>' ).addClass( 'tab-page' ).attr( 'id', id ).css( style ) ;
	var listLen = list.length ;
	for( var i = 0; i < listLen; ++i )
	{
		var li = $( '<li></li>' ).append( list[i]['text'] ).appendTo( $( ul ) ) ;
		if ( list[i]['status'] != undefined && list[i]['status'] == 'active' ){ $( li ).addClass( 'active' ) }
		if ( list[i]['event'] != undefined ) { $( li ).get(0).onclick = Function( list[i]['event'] ) } ;
	}
	return ul ;
}

//------------------------------ 下拉框 --------------------------------//

sdbjs.parts.selectBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style		例子 { 'padding': '0 0 0 0' }
*/
sdbjs.parts.selectBox.create = function( id, style )
{
	var obj = $( '<select></select>' ).addClass( 'form-control' ).attr( 'id', id ).css( style ) ;
	return obj ;
}

/*
 * 参数1 id
 * 参数2 列表		例子 [ { 'key': '123', 'value': '123', 'selected': true },  { 'key': '456', 'value': '456' } ] 
 */
sdbjs.parts.selectBox.add = function( id, list )
{
	var obj = $( '#' + id ) ;
	var listLen = list.length ;
	for( var i = 0; i < listLen; ++i )
	{
		$( obj ).append( '<option value="' + list[i]['value'] + '">' + list[i]['key'] + '</option>' ) ;
		if ( list[i]['selected'] == true )
		{
			$( obj ).children( ':last-child' ).attr( 'selected', 'selected' ) ;
		}
	}
}

//------------------------------ 输入框 --------------------------------//

sdbjs.parts.inputBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style			例子 { 'padding': '0 0 0 0' }
 * 参数3 type			textarea, text, button, checkbox, file, hidden, image, password, radio
 * 参数4 text      	输入框的内容
 * 参数5 placeholder	输入提示，只有在 type = text | textarea 有效
 * 参数6 row				行数，只有在 type = text | textarea 有效
*/
sdbjs.parts.inputBox.create = function( id, style, type, text, isDisabled, placeholder, row )
{
	var obj = null ;
	if ( type != 'textarea' ){ obj = $( '<input></input>' ).attr( 'type', type ) }else{ obj = $( '<textarea></textarea>' ); if( row != undefined){ $( obj ).attr( 'row', row ) } }
	if( type != 'checkbox' ){ $( obj ).addClass( 'form-control' ) }
	if ( placeholder != undefined ){ $( obj ).attr( 'placeholder', placeholder ) }
	if ( isDisabled == true ){ $( obj ).attr( 'disabled', 'disabled' ) }
	$( obj ).attr( 'id', id ).css( style ).text( text ) ;
	return obj ;
}

//------------------------------ well面板 --------------------------------//

sdbjs.parts.wellBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style			例子 { 'padding': '0 0 0 0' }
 * 参数4 text      	内容
*/
sdbjs.parts.wellBox.create = function( id, style, type, text )
{
	return $( '<div></div>' ).addClass( 'well' ).attr( 'id', id ).css( style ).append( text ) ;
}

//------------------------------ 提示框 --------------------------------//

sdbjs.parts.alertBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style			例子 { 'padding': '0 0 0 0' }
 * 参数3 type			success, info, warning, danger
 * 参数4 text      	内容
*/
sdbjs.parts.alertBox.create = function( id, style, type, text )
{
	return $( '<div></div>' ).addClass( 'alert alert-' + type ).attr( 'id', id ).css( style ).append( text ) ;
}

//------------------------------ 导航菜单条 --------------------------------//

sdbjs.parts.navAppBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style			例子 { 'padding': '0 0 0 0' }
 * 参数3 list			例子 [ { 'text': '系统', 'sub': [ { 'text': '连接管理', 'event': 'alert(123)' }  ] }, { 'text': '编辑', 'sub': [ { 'text': '全选', 'event': 'alert(123)' }  ] } ]
*/
sdbjs.parts.navAppBox.create = function( id, style, list )
{
	var nav_box  = $( '<div></div>' ).addClass( 'nav-app-box' ).attr( 'id', id ).css( style ) ;
	var nav_body = $( '<div></div>' ).addClass( 'nav-app-body' ).appendTo( $( nav_box ) ) ;
	var nav_app  = $( '<div></div>' ).addClass( 'nav-app' ).appendTo( $( nav_body ) ) ;
	var listLen = list.length ;
	for( var i = 0; i < listLen; ++i )
	{
		if ( list[i]['sub'] != undefined )
		{
			var nav_li_div = $( '<div></div>' ).addClass( 'nav-li' ).append( '<div>' + list[i]['text'] + '<span class="caret caret-bottom"></span></div>' ).appendTo( $( nav_app ) ) ;
			var nav_sub_ul = $( '<ul></ul>' ).appendTo( $( nav_li_div ) ) ;
			var subLen = list[i]['sub'].length ;
			for( var k = 0; k < subLen; ++k )
			{
				var nav_sub_li_div = $( '<li></li>' ).text( list[i]['sub'][k]['text'] ).appendTo( $( nav_sub_ul ) ) ;
				if ( list[i]['sub'][k]['event'] != undefined ) { $( nav_sub_li_div ).get(0).onclick = Function( list[i]['sub'][k]['event'] ) } ;
			}
		}
		else
		{
			var nav_li_div = $( '<div></div>' ).addClass( 'nav-li' ).append( list[i]['text'] ).appendTo( $( nav_app ) ) ;
			if ( list[i]['event'] != undefined ) { $( nav_li_div ).get(0).onclick = Function( list[i]['event'] ) } ;
		}
	}
	return nav_box ;
}

//------------------------------ 网格 --------------------------------//

sdbjs.parts.gridBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style			例子 { 'padding': '0 0 0 0' }
 * 参数3 title			例子 [ [ 'haha', hehe', 'hello ], [ 'haha', hehe', 'hello ] ]
 * 参数4 宽度比例			[ 10, 20, 20 ] 总共50, 所以宽度是 20%, 40%, 40%
*/
sdbjs.parts.gridBox.create = function( id, style, title, scale )
{
	var tempGrid = { 'title': [], 'body': [] } ;
	var newScale = [] ;
	var sumNum = 0 ;
	var len = scale.length ;
	for ( var i = 0; i < len; ++i )
	{
		sumNum += parseInt( scale[i] ) ;
	}
	var tempSum = 0 ;
	for ( var i = 0; i < len; ++i )
	{
		var tempNum = 0 ;
		if( i + 1 == len )
		{
			tempNum = 100 - tempSum ;
			newScale.push( tempNum ) ;
		}
		else
		{
			tempNum = parseInt( parseInt( scale[i] ) / sumNum * 100 ) ;
			tempSum += tempNum ;
			newScale.push( tempNum ) ;
		}
	}
	var grid_box   = $( '<div></div>' ).addClass( 'grid-box' ).attr( 'id', id ).css( style ).data( 'scale', newScale ) ;
	var grid_title = $( '<div></div>' ).addClass( 'grid-title' ).appendTo( $( grid_box ) ) ;
	var trLen = title.length ;
	for ( var i = 0; i < trLen; ++i )
	{
		var grid_title_tr    = $( '<div></div>' ).addClass( 'grid-tr' ).appendTo( $( grid_title ) ) ;
		var grid_title_clear = $( '<div></div>' ).addClass( 'grid-clear' ).appendTo( $( grid_title ) ) ;
		var tempTitle = { 'tr': grid_title_tr, 'ths': [] } ;
		var thLen = title[i].length ;
		for ( var k = 0; k < thLen; ++k )
		{
			var grid_title_th = $( '<div></div>' ).addClass( 'grid-th' ).append( title[i][k] ).appendTo( $( grid_title_tr ) ) ;
			if( k + 1 == thLen ){ $( grid_title_th ).css( 'borderRight', '1px solid #CCC' ) }
			tempTitle['ths'].push( grid_title_th ) ;
		}
		tempGrid['title'].push( tempTitle ) ;
	}
	$( '<div></div>' ).addClass( 'grid-body' ).appendTo( $( grid_box ) ) ;
	sdbjs.private.gridList[id] = tempGrid ;
	return grid_box ;
}

/*
 * 参数1 id
 * 参数2 列表		{ 'cell': [ { 'text': '123', 'event': '' }, { 'text': '3456' } ], 'event': '' }
*/
sdbjs.parts.gridBox.add = function( id, list )
{
	var gridTitle  = $( '#' + id + ' > .grid-title' ).children( ':first-child' ) ;
	var gridBody   = $( '#' + id + ' > .grid-body' ) ;
	var gridTrLen  = $( gridBody ).children( '.grid-tr' ).length ;
	var grid_tr    = $( '<div></div>' ).addClass( 'grid-tr' ).appendTo( $( gridBody ) ) ;
	var grid_clear = $( '<div></div>' ).addClass( 'grid-clear' ).appendTo( $( gridBody ) ) ;
	var tempBody   = { 'tr': grid_tr, 'tds': [] } ;
	var tdLen = list['cell'].length ;
	for ( var i = 0; i < tdLen; ++i )
	{
		var grid_td = $( '<div></div>' ).addClass( 'grid-td' ).css( 'width', $( gridTitle ).children( ':eq(' + i + ')' ).css( 'width' ) ).append( list['cell'][i]['text'] ).appendTo( $( grid_tr ) ) ;
		if( list['cell'][i]['event'] != undefined ){ $( grid_td ).css( 'cursor', 'pointer' ).get(0).onclick = Function( list['cell'][i]['event'] ) }
		if( i + 1 == tdLen ){ $( grid_td ).css( 'borderRight', '1px solid #CCC' ) }
		if ( gridTrLen % 2 != 0 ){ $( grid_td ).css( 'backgroundColor', '#F5F5F5' ) }
		tempBody['tds'].push( grid_td ) ;
	}
	if( list['event'] != undefined ){ $( grid_tr ).css( 'cursor', 'pointer' ).get(0).onclick = Function( list['event'] ) }
	$( gridBody ).children( '.grid-tr' ).each(function() { $( this ).children().each(function(index, element) {
      $( this ).css( { 'height': $( this ).parent().height() - 12 } ) ;
   } ) } ) ;
	sdbjs.private.gridList[id]['body'].push( tempBody ) ;
}

//------------------------------ 全局加载(需要heartcode-canvasloader.js) --------------------------------//

sdbjs.parts.loadingBox = new Object() ;

/*
 * 参数1 id
*/
sdbjs.parts.loadingBox.create = function( id )
{
	$('<div></div>' ).addClass( 'loading-img' ).attr( 'id', id ).appendTo( $( 'body' ) ) ;
	var chartObj = new CanvasLoader( id ) ;
	chartObj.setShape('spiral') ;
	chartObj.setColor('#dedede');
	chartObj.setDiameter( 80 ) ;
	chartObj.setDensity( 160 ) ;
	chartObj.setRange( 0.9 ) ;
	if( !$.support.leadingWhitespace ){ chartObj.setSpeed( 1 ) }else{ chartObj.setSpeed( 4 ) }
	chartObj.setFPS( 40 ) ;
	chartObj.show() ;
	$( '<div></div>' ).addClass( 'loading-font' ).text( '0%' ).appendTo( $( '#' + id ) ) ;
}

//----------------------------------------------------------- 框架 -------------------------------------------------------//

sdbjs.frame = new Object() ;

//------------------------------ 网页总框架 --------------------------------//

sdbjs.frame.htmlBody = new Object() ;

/*
 * 参数1 id
 * 参数2 style			例子 { 'padding': '0 0 0 0' }
*/
sdbjs.frame.htmlBody.create = function( id, style )
{
	return $( '<div></div>' ).addClass( 'htmlbody' ).attr( 'id', id ).css( style ) ;
}

//------------------------------ 关联组合 --------------------------------//

sdbjs.frame.inlineBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
 * 参数3 列表		例子 [ [ '100px', 'auto' ], [ 'auto', '90%' ] ] ] [ 宽度, 高度 ]
*/
sdbjs.frame.inlineBox.create = function( id, style, list )
{
	var obj = $( '<div></div>' ).addClass( 'inline-box' ).attr( 'id', id ).css( style ) ;
	var len = list.length ;
	for ( var i = 0; i < len; ++i )
	{
		$( '<div></div>' ).addClass( 'inline-block' ).css( { 'width': list[i][0], 'height': list[i][1] } ).appendTo( $( obj ) ) ;
	}
	return obj ;
}

//------------------------------ 垂直组合 --------------------------------//

sdbjs.frame.verticalBox = new Object() ;

/*
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
 * 参数3 高度列表	   例子 [ '10%', '20px', '30px' ]
*/
sdbjs.frame.verticalBox.create = function( id, style, list )
{
	var obj = $( '<div></div>' ).addClass( 'vertical-box' ).attr( 'id', id ).css( style ) ;
	var vertical_body	= $( '<div></div>' ).addClass( 'vertical-body' ).appendTo( $( obj ) ) ;
	var len = list.length ;
	for( var i = 0; i < len; ++i )
	{
		$( '<div></div>' ).addClass( 'vertical-li' ).css( 'height', list[i] ).appendTo( $( vertical_body ) ) ;
	}
	return obj ;
}

//------------------------------ 并排组合 --------------------------------//

sdbjs.frame.transverse = new Object() ;

/*
 * 参数1 id
 * 参数2 style	例子 { 'padding': '0 0 0 0' }
 * 参数3 宽度列表	   例子 [ '10%', '20px', '30px' ]
*/
sdbjs.frame.transverse.create = function( id, style, list )
{
	var transverse_box	= $( '<div></div>' ).addClass( 'transverse-box' ).attr( 'id', id ).css( style ) ;
	var transverse_body	= $( '<div></div>' ).addClass( 'transverse-body' ).appendTo( $( transverse_box ) ) ;
	var clear_float		= $( '<div></div>' ).addClass( 'clear-float' ).appendTo( $( transverse_box ) ) ;
	var box_width, box_height ;
	try{ box_width  = parseInt( style['width'] ) }catch(e){ throw "style['width']	 必须填写!" ; }
	try{ box_height = parseInt( style['height'] ) }catch(e){ throw "style['height'] 必须填写!" ; }
	var len = list.length ;
	for( var i = 0; i < len; ++i )
	{
		$( '<div></div>' ).addClass( 'transverse-col' ).css( { 'width': list[i], 'height': box_height } ).appendTo( $( transverse_body ) ) ;
	}
	return transverse_box ;
}

//-------------------------- 脚本预加载 ------------------------//

/*
 * 获取浏览器宽度和高度
 */
$(document).ready(function(){
	$( document.body ).css( 'overflow', 'hidden' ) ;
	sdbjs.public.width = sdbjs.fun.getWindowWidth() ;
	sdbjs.public.height = sdbjs.fun.getWindowHeight() ;
	$( document.body ).css( 'overflow', 'visible' ) ;
});

$(window).resize(function(){
	$( document.body ).css( 'overflow', 'hidden' ) ;
	sdbjs.public.width = sdbjs.fun.getWindowWidth() ;
	sdbjs.public.height = sdbjs.fun.getWindowHeight() ;
	$( document.body ).css( 'overflow', 'visible' ) ;
});

/*
 * 自动创建的组件
 */
$(document).ready(function(){
	//创建遮罩
	sdbjs.private.htmlScreen = $( '<div></div>' ).addClass( 'mask-screen mask-screen-alpha' ).appendTo( $( 'body' ) ) ;
	//创建遮罩
	sdbjs.private.htmlScreen2 = $( '<div></div>' ).addClass( 'mask-screen mask-screen-not-alpha' ).css( 'z-index', 9998 ).appendTo( $( 'body' ) ) ;
	//创建小标签
	sdbjs.private.smallLabel = $( '<div></div>' ).addClass( 'tooltip' ).html( '<div class="tooltip-arrow"></div><div class="tooltip-inner"></div>' ).appendTo( $( 'body' ) ) ;
	$( sdbjs.private.smallLabel ).on( 'mouseover', function(){
		$( this ).show() ;
	} ) ;
	$( sdbjs.private.smallLabel ).on( 'mouseout', function(){
		$( this ).hide() ;
	} ) ;
});

/*
 * 自动修正
 */
$(window).resize(function(){
	//修正指定的组件宽度和高度
	var len = sdbjs.private.autoList.length ;
	for( var i = 0; i < len; ++i )
	{
		for( var key in sdbjs.private.autoList[i]['style'] )
		{
			var value = eval( sdbjs.private.autoList[i]['style'][key] ) ;
			if( sdbjs.private.autoList[i]['obj'] != undefined )
			{
				$( sdbjs.private.autoList[i]['obj'] ).css( key, value ) ;
			}
			else
			{
				$( '#' + sdbjs.private.autoList[i]['id'] ).css( key, value ) ;
			}
		}
	}
	//修正模态框的位置
	$( '.modal:not(:hidden)' ).each(function() {
      $( this ).css( { 'left': parseInt( ( sdbjs.public.width - $( this ).outerWidth() ) * 0.5 ),
		                 'top':  parseInt( ( sdbjs.public.height - $( this ).outerHeight() ) * 0.5 ) } ) ;
   });
	
	$( '.grid-box:not(:hidden)' ).each(function() {
		sdbjs.fun.gridRevise( $( this ).attr( 'id' ) ) ;
	});
	//下拉菜单弹出部分的位置
	if( sdbjs.private.cursorDropDown != null )
	{
		var id = $( sdbjs.private.cursorDropDown ).attr( 'data-target' ) ;
		if( !( $( '#' + id + '_menu' ).is( ':hidden' ) ) )
		{
			var left = $( sdbjs.private.cursorDropDown ).offset().left ;
			var top = $( sdbjs.private.cursorDropDown ).offset().top + $( sdbjs.private.cursorDropDown ).height() ;
			if( $( '#' + id + '_menu' ).outerHeight() + top > sdbjs.public.height )
			{
				top = $( sdbjs.private.cursorDropDown ).offset().top - $( '#' + id + '_menu' ).outerHeight() ;
			}
		}
		$( '#' + id + '_menu' ).css( { 'left': left, 'top': top } ) ;
	} ;
});