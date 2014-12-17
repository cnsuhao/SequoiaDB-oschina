/*
 * 登录
 */
function login()
{
	var user = $.trim( $( '#userName' ).val() ) ;
	var pwd = $( '#passwd').val() ;
	pwd = $.md5( pwd ) ;
	var jsonArr = loginOM( user, pwd ) ;
	if( jsonArr[0]['errno'] == 0 )
	{
		$.cookie( 'SdbUser', user ) ;
		$.cookie( 'SdbPasswd', pwd ) ;
		gotoPage( 'index.html' ) ;
	}
	else
	{
		$.removeCookie( 'SdbUser' ) ;
		$.removeCookie( 'SdbPasswd' ) ;
		showInfoFromFoot( 'danger', jsonArr[0]['detail'] ) ;
	}
}

$( document ).keydown( function(e){
	if( e.which == 13 )
	{
		login() ;
	}
} ) ;

$(document).ready(function()
{
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlBody' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	if( navigator.cookieEnabled == false )
	{
		$( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ).html( '<div id="cookieBox" style="padding-top: 317px;"><div class="alert alert-danger" id="cookie_alert_box" style="width: 800px; margin-left: auto; margin-right: auto;">您的浏览器禁止使用Cookie,系统将不能正常使用，请设置浏览器启用Cookie，并且刷新或重新打开浏览器。</div></div>' ) ;
		sdbjs.fun.autoCorrect( { 'id': 'cookieBox', 'style': { 'paddingTop': 'parseInt( ( sdbjs.public.height - 131 ) / 2 - 25 )' } } ) ;
	}
	else
	{
		sdbjs.fun.autoCorrect( { 'id': 'userBox', 'style': { 'paddingTop': 'parseInt( ( sdbjs.public.height - 400 ) / 2 - 25 )' } } ) ;
		$( '#passwd' ).focus() ;
	}
	
	sdbjs.fun.autoCorrect( { 'obj': $( '#htmlVer' ).children( ':first-child' ).children( ':eq(2)' ), 'style': { 'height': 'sdbjs.public.height - 131' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ), 'style': { 'width': 'sdbjs.public.width' } } ) ;
	sdbjs.fun.autoCorrect( { 'obj': $( '#footTra' ).children( ':first-child' ).children( ':eq(1)' ), 'style': { 'width': 'sdbjs.public.width - 428' } } ) ;
	
	sdbjs.fun.endOfCreate() ;
});