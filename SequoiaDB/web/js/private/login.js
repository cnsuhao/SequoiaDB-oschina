
function login()
{
	var user = $( '#userName' ).val() ;
	var pwd = $( '#passwd').val() ;
	pwd = $.md5( pwd ) ;
	restLogin( true, function( jsonArr, textStatus, jqXHR ){
		var id = jqXHR.getResponseHeader( 'SdbSessionID' ) ;
		sdbjs.fun.saveData( 'SdbSessionID', id ) ;
		sdbjs.fun.saveData( 'SdbUser', user ) ;
		sdbjs.fun.saveData( 'SdbIsLogin', 'true' ) ;
		gotoPage( 'index.html' ) ;
	}, function( json ){
		showFootStatus( 'danger', json['detail'] ) ;
	}, null, user, pwd ) ;
}

$( document ).keydown( function(e){
	// 13 回车
	if( e.which === 13 )
	{
		login() ;
	}
} ) ;

function createHtml()
{
	createPublicHtml() ;
	sdbjs.fun.setCSS( 'middle', { 'padding-top': '10%' } ) ;

	/* 分页 */
	sdbjs.parts.tabPageBox.create( 'top2', 'tab' ) ;
	sdbjs.fun.setCSS( 'tab', { 'padding-top': 5 } ) ;
	//'登录'
	sdbjs.parts.tabPageBox.add( 'tab', '<img width="14" src="./images/smallicon/blacks/16x16/user.png"> ' + htmlEncode( _languagePack['public']['tabPage'][0] ), true, null ) ;

	/* well面板 */
	sdbjs.parts.wellBox.create( 'middle', 'center', 600, 'auto' ) ;
	sdbjs.fun.setCSS( 'center', { 'margin': '0 auto 0 auto' } ) ;
	
	/* 表格 */
	sdbjs.parts.tableBox.create( 'center', 'table' ) ;
	sdbjs.parts.tableBox.update( 'table', 'loosen' ) ;
	//登录
	sdbjs.parts.tableBox.addBody( 'table', [ { 'text': '<b>' + htmlEncode( _languagePack['login']['table']['title'] ) + '</b>', 'colspan': 2 } ]  ) ;
	//用户名
	sdbjs.parts.tableBox.addBody( 'table', [ { 'text': htmlEncode( _languagePack['login']['table']['body'][0] ), 'width': 70 }, { 'text': '<input id="userName" type="text" class="form-control" value="admin">' } ]  ) ;
	//密码
	sdbjs.parts.tableBox.addBody( 'table', [ { 'text': htmlEncode( _languagePack['login']['table']['body'][1] ), 'width': 70 }, { 'text': '<input id="passwd" type="password" class="form-control">' } ]  ) ;
	//确定
	sdbjs.parts.tableBox.addBody( 'table', [ { 'text': '<button class="btn btn-primary pull-right" onclick="login()">' + htmlEncode( _languagePack['public']['button']['ok'] ) + '</button>', 'colspan': 2 } ]  ) ;
}

$(document).ready(function(){
	createHtml() ;
	$( '#passwd').focus() ;
} ) ;