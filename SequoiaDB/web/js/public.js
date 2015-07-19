//--------------------------------- 通用变量 ---------------------------------//

//语言列表
var _languageList = { '中文简体': 'zh-CN', 'English': 'en' } ;


//打开添加业务模态框
function openDeployGuidModal()
{
	sdbjs.fun.setCSS( 'deployGuidAlert', { 'display': 'none' } ) ;
	sdbjs.parts.modalBox.show( 'deployGuid' ) ;
}

//登出
function logout()
{
	sdbjs.fun.delData( 'SdbUser' ) ;
	sdbjs.fun.delData( 'SdbSessionID' ) ;
	sdbjs.fun.delData( 'SdbDeployModel' ) ;
	gotoPage( 'login.html' ) ;
}

//修改密码
function changeUserPassword()
{
	var user   = sdbjs.fun.getData( 'SdbUser' ) ;
	var pwd    = $( '#passwd_change' ).val() ;
	var newPwd = $( '#passwd_change_new' ).val() ;
	
	if ( !checkString( pwd, 1, 1024 ) )
	{
		//'密码格式错误，密码长度在 1 - 1024 个字符内。'
		showModalError( 'changePwdAlert', _languagePack['error']['web']['create'][8] ) ;
		return;
	}
	if ( !checkString( newPwd, 1, 1024 ) )
	{
		//'新密码格式错误，密码长度在 1 - 1024 个字符内。'
		showModalError( 'changePwdAlert', _languagePack['error']['web']['create'][9] ) ;
		return;
	}
	
	sdbjs.parts.modalBox.hide( 'changePwd' ) ;
	sdbjs.parts.loadingBox.show( 'loading' ) ;
	restChangePasswd( true, function( jsonArr, textStatus, jqXHR ){
		//'密码修改成功。'
		showFootStatus( 'info', _languagePack['info']['web']['create'][0] ) ;
	}, function( json ){
		showModalError( 'changePwdAlert', json['detail'] ) ;
		sdbjs.parts.modalBox.show( 'changePwd' ) ;
	}, function(){
		sdbjs.parts.loadingBox.hide( 'loading' ) ;
	}, user, pwd, newPwd ) ;
}

//修改语言
function changeLanguage()
{
	_language = sdbjs.parts.selectBox.get( 'languageSelect' ) ;
	sdbjs.fun.saveData( 'SdbLanguage', _language ) ;
	gotoPage( _cursorFileName + '.html' ) ;
}

//打开修改密码模态框
function openChangePasswdModal()
{
	$( '#passwd_change' ).val( '' ) ;
	$( '#passwd_change_new' ).val( '' ) ;
	sdbjs.fun.setCSS( 'changePwdAlert', { 'display': 'none' } ) ;
	sdbjs.parts.modalBox.show( 'changePwd' ) ;
}

//打开语言模态框
function openLanguageModal()
{
	sdbjs.parts.selectBox.set( 'languageSelect', _language ) ;
	sdbjs.parts.modalBox.show( 'languageModal' ) ;
}

//创建基础网页
function createPublicHtml()
{
	$( document.body ).css( 'overflow', 'hidden' ) ;
	sdbjs.fun.setRootNode( $( '#root' ), 'variable', 'variable' ) ;
	$( document.body ).css( 'overflow', 'visible' ) ;
	
	sdbjs.parts.loadingBox.create( $( document.body ), 'loading' ) ;
	sdbjs.parts.loadingBox.update( 'loading', 'loading' ) ;
	
	/* 构建页面框架 */
	//顶部1
	sdbjs.parts.divBox.create( 'root', 'top1', 'auto', 40 ) ;
	//顶部2
	sdbjs.parts.divBox.create( 'root', 'top2', 'auto', 34 ) ;
	//中间内容
	sdbjs.parts.divBox.create( 'root', 'middle', 'auto', 'variable' ) ;
	//底部
	sdbjs.parts.divBox.create( 'root', 'foot', 'auto', 65 ) ;
	sdbjs.fun.setCSS( 'foot', { 'border-top': '1px solid #DDD' } ) ;

	/* 导航 */
	sdbjs.parts.navBox.create( 'top1', 'nav' ) ;
	sdbjs.parts.navBox.addColum( 'nav', htmlEncode( _languagePack['public']['nav'][0]['text'] ), 'gotoPage("index.html")' ) ;//首页

	var navNum = 1 ;
	if( _cursorFileName === 'index' )
	{
		//部署引导
		sdbjs.parts.navBox.addColum( 'nav', htmlEncode( _languagePack['public']['nav'][1]['text'] ), function( obj ){
			sdbjs.fun.addClick( obj, 'openDeployGuidModal()' ) ;
		} ) ;
		++navNum ;
	}
	
	//帮助
	sdbjs.parts.navBox.addColum( 'nav', htmlEncode( _languagePack['public']['nav'][2]['text'] ) ) ;
	//关于SAC系统
	sdbjs.parts.navBox.addMenu( 'nav', navNum, [ { 'text': htmlEncode( _languagePack['public']['nav'][2]['child'][1] ), 'fun': function( obj ){
		$( obj ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'aboutSACModal' ) ;
	} } ] ) ;
	++navNum ;

	if( _cursorFileName !== 'login' )
	{
		//用户
		sdbjs.parts.navBox.addColum2( 'nav', '<img width="14" style="vertical-align:middle;" src="./images/smallicon/white/16x16/user.png"> ' + htmlEncode( sdbjs.fun.getData( 'SdbUser' ) ) ) ;
		sdbjs.parts.navBox.addMenu( 'nav', navNum, [ { 'text': htmlEncode( _languagePack['public']['nav'][3]['child'][0] ), 'fun': 'openChangePasswdModal()' } ] ) ;//修改密码
		sdbjs.parts.navBox.addMenu( 'nav', navNum, [ { 'text': htmlEncode( _languagePack['public']['nav'][3]['child'][1] ), 'fun': 'logout()' } ] ) ;//注销
		++navNum ;
	}
	
	if( _cursorFileName === 'index' )
	{
		//任务
		sdbjs.parts.navBox.addColum2( 'nav', '<img width="14" style="vertical-align:middle;" src="./images/smallicon/white/16x16/spechbubble.png"> ' + htmlEncode( _languagePack['public']['nav'][4]['text'] ), function( obj ){
			$( obj ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'taskList' ) ;
		} ) ;
		++navNum ;
	}
	
	//语言
	sdbjs.parts.navBox.addColum2( 'nav', '<img width="14" style="vertical-align:middle;" src="./images/smallicon/white/16x16/globe_2.png"> ' + htmlEncode( _languagePack['public']['nav'][2]['child'][0] ), 'openLanguageModal()' ) ;

	if( _cursorFileName !== 'login' )
	{
		/* 修改密码的弹窗 */
		sdbjs.parts.modalBox.create( $( document.body ), 'changePwd' ) ;
		//'修改密码'
		sdbjs.parts.modalBox.update( 'changePwd', htmlEncode( _languagePack['public']['modal']['changePwd']['title'] ), function( bodyObj ){
			sdbjs.parts.tableBox.create( bodyObj, 'changePwdTable' ) ;
			sdbjs.parts.tableBox.update( 'changePwdTable', 'loosen' ) ;
			//'用户名：'
			sdbjs.parts.tableBox.addBody( 'changePwdTable', [{ 'text': htmlEncode( _languagePack['public']['modal']['changePwd']['table']['title'][0] ), 'width': 100 },
																			 { 'text': htmlEncode( sdbjs.fun.getData( 'SdbUser' ) ) } ] ) ;
			//'密码：'
			sdbjs.parts.tableBox.addBody( 'changePwdTable', [{ 'text': htmlEncode( _languagePack['public']['modal']['changePwd']['table']['title'][1] ), 'width': 100 },
																			 { 'text': '<input class="form-control" type="password" id="passwd_change">' } ] ) ;
			//'新密码：'
			sdbjs.parts.tableBox.addBody( 'changePwdTable', [{ 'text': htmlEncode( _languagePack['public']['modal']['changePwd']['table']['title'][2] ), 'width': 100 },
																			 { 'text': '<input class="form-control" type="password" id="passwd_change_new">' } ] ) ;

		}, function( footObj ){
			sdbjs.parts.tableBox.create( footObj, 'changePwdFootTable' ) ;
			sdbjs.parts.tableBox.addBody( 'changePwdFootTable', [{ 'text': function( tdObj ){
																						sdbjs.parts.alertBox.create( tdObj, 'changePwdAlert' ) ;
																						sdbjs.fun.setCSS( 'changePwdAlert', { 'display': 'none', 'padding': '8px', 'text-align': 'left' } ) ; } },
																				  { 'text': function( tdObj ){
																						sdbjs.parts.buttonBox.create( tdObj, 'changePwdOK' ) ;
																						$( tdObj ).append( '&nbsp;' ) ;
																						sdbjs.parts.buttonBox.create( tdObj, 'changePwdClose' ) ;
																						//'确定'
																						sdbjs.parts.buttonBox.update( 'changePwdOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'changeUserPassword()' ) ;
																						//'关闭'
																						sdbjs.parts.buttonBox.update( 'changePwdClose', function( buttonObj ){
																							$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'changePwd' ) ;
																						}, 'primary' ) ; }, 'width': 120  } ] ) ;

		} ) ;
	}

	/* logo */
	sdbjs.parts.divBox.create( 'foot', 'logo', 228, 'auto' ) ;
	sdbjs.fun.setCSS( 'logo', { 'float': 'left' } ) ;
	if( _language === 'en' )
	{
		sdbjs.fun.setHtml( 'logo', '<img src="images/logo2.png">' ) ;
	}
	else
	{
		sdbjs.fun.setHtml( 'logo', '<img src="images/logo.png">' ) ;
	}
	
	/* 状态栏 */
	sdbjs.parts.divBox.create( 'foot', 'status', 'variable', 64 ) ;
	sdbjs.fun.setCSS( 'status', { 'float': 'left' } ) ;
	sdbjs.parts.alertBox.create( 'status', 'ststusAlert' ) ;
	sdbjs.fun.setCSS( 'ststusAlert', { 'display': 'none', 'margin-left': 10, 'margin-top': 5 } ) ;
	
	/* 操作区 */
	sdbjs.parts.divBox.create( 'foot', 'operate', 200, 'auto' ) ;
	sdbjs.fun.setCSS( 'operate', { 'float': 'left', 'padding': '10px 0 0 10px' } ) ;
	
	/* ** */
	sdbjs.parts.divBox.create( 'foot', 'foot-clear', 0, 'auto' ) ;
	sdbjs.fun.setClass( 'foot-clear', 'clear-float' ) ;
	
	/* 创建通用错误的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'processError' ) ;
	sdbjs.parts.modalBox.update( 'processError', htmlEncode( _languagePack['error']['system']['errModalTitle'] ), function( bodyObj ){
		sdbjs.parts.alertBox.create( bodyObj, 'processErrorAlert' ) ;
	}, function( footObj ){
		$( footObj ).css( 'text-align', 'right' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'processErrorClose' ) ;
		sdbjs.parts.buttonBox.update( 'processErrorClose', function( buttonObj ){
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'processError' ) ;
		}, 'primary' ) ;
	} ) ;

	/* 关于SAC的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'aboutSACModal' ) ;
	//关于SAC
	sdbjs.parts.modalBox.update( 'aboutSACModal', htmlEncode( _languagePack['public']['modal']['aboutSAC']['title'] ), function( bodyObj ){
		$( bodyObj ).append( '<img src="images/logo2.png">' ) ;
		//Version: 1.12
		$( bodyObj ).append( '<div style="margin-top:15px;">' + htmlEncode( _languagePack['public']['modal']['aboutSAC']['context'][0] ) + '</div>' ) ;
		//SAC（SequoiaDB Administration Center）提供针对SequoiaDB数据库的图形化监控服务。
		$( bodyObj ).append( '<div style="margin-top:15px;">' + htmlEncode( _languagePack['public']['modal']['aboutSAC']['context'][1] ) + '</div>' ) ;
	}, function( footObj ){
		sdbjs.parts.buttonBox.create( footObj, 'aboutSACModalClose' ) ;
		sdbjs.parts.buttonBox.update( 'aboutSACModalClose', function( buttonObj ){
			//'关闭'
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'aboutSACModal' ) ;
		}, 'primary' ) ;
	} ) ;
	
	if( _cursorFileName === 'index' )
	{
		/* 任务的弹窗 */
		sdbjs.parts.modalBox.create( $( document.body ), 'taskList' ) ;
		//'任务列表'
		sdbjs.parts.modalBox.update( 'taskList', htmlEncode( _languagePack['public']['modal']['task']['title'] ), function( bodyObj ){
			$( bodyObj ).css( { 'max-height': 600, 'overflow': 'auto' } ) ;
			sdbjs.parts.tableBox.create( bodyObj, 'taskListTable' ) ;
			sdbjs.parts.tableBox.update( 'taskListTable', 'loosen simple' ) ;
			//ID 类型 进度 状态
			sdbjs.parts.tableBox.addBody( 'taskListTable', [{ 'text': htmlEncode( _languagePack['public']['modal']['task']['table']['title'][0] ), 'width': 50 },
																			{ 'text': htmlEncode( _languagePack['public']['modal']['task']['table']['title'][1] ), 'width': 150 },
																			{ 'text': htmlEncode( _languagePack['public']['modal']['task']['table']['title'][2] ) },
																			{ 'text': htmlEncode( _languagePack['public']['modal']['task']['table']['title'][3] ), 'width': 160 } ] ) ;
		}, function( footObj ){
			$( footObj ).css( 'text-align', 'right' ) ;
			sdbjs.parts.buttonBox.create( footObj, 'taskListClose' ) ;
			sdbjs.parts.buttonBox.update( 'taskListClose', function( buttonObj ){
				//'关闭'
				$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'taskList' ) ;
			}, 'primary' ) ;
		} ) ;
	}
	
	/* 选择语言的弹窗 */
	sdbjs.parts.modalBox.create( $( document.body ), 'languageModal' ) ;
	//语言
	sdbjs.parts.modalBox.update( 'languageModal', htmlEncode( _languagePack['public']['modal']['language']['title'] ), function( bodyObj ){
		sdbjs.parts.tableBox.create( bodyObj, 'languageTable' ) ;
		sdbjs.parts.tableBox.update( 'languageTable', 'loosen' ) ;
		//'语言：'
		sdbjs.parts.tableBox.addBody( 'languageTable', [{ 'text': htmlEncode( _languagePack['public']['modal']['language']['table']['title'][0] ), 'width': 100 },
																		{ 'text': function( obj ){
																			sdbjs.parts.selectBox.create( obj, 'languageSelect' ) ;
																			$.each( _languageList, function( key, value ){
																				sdbjs.parts.selectBox.add( 'languageSelect', key, value, ( value === _language ) ) ;
																			} ) ;
																		} } ] ) ;
	}, function( footObj ){
		sdbjs.parts.buttonBox.create( footObj, 'languageOK' ) ;
		$( footObj ).append( '&nbsp;' ) ;
		sdbjs.parts.buttonBox.create( footObj, 'languageClose' ) ;
		//'确定'
		sdbjs.parts.buttonBox.update( 'languageOK', htmlEncode( _languagePack['public']['button']['ok'] ), 'primary', null, 'changeLanguage()' ) ;
		//'关闭'
		sdbjs.parts.buttonBox.update( 'languageClose', function( buttonObj ){
			$( buttonObj ).text( _languagePack['public']['button']['close'] ).attr( 'data-toggle', 'modalBox' ).attr( 'data-target', 'languageModal' ) ;
		}, 'primary' ) ;
	} ) ;
}