//--------------------------------- 通用变量 ---------------------------------//

/*
 * 设置每次请求的会话ID
 */
function restBeforeSend( jqXHR )
{
	var id = sdbjs.fun.getData( 'SdbSessionID' ) ;
	if( id !== null )
	{
		jqXHR.setRequestHeader( 'SdbSessionID', id ) ;
	}
	var language = sdbjs.fun.getData( 'SdbLanguage' )
	if( language !== null )
	{
		jqXHR.setRequestHeader( 'SdbLanguage', language ) ;
	}
}

//--------------------------------- 通用函数 ---------------------------------//

//登录
function restLogin( async, success, error, complete, user, pwd )
{
	var timestamp = parseInt( ( new Date().getTime() ) / 1000 ) ;
	var data = { 'cmd' : 'login', 'user': user, 'passwd': pwd, 'Timestamp': timestamp } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//查询Cluster
function restQueryCluster( async, success, error, complete )
{
	var data = { 'cmd': 'query cluster' } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//创建cluster
function restCreateCluster( async, success, error, complete, clusterName, desc, user, pwd, group, installPath )
{
	var data = { 'cmd': 'create cluster', 'ClusterInfo': JSON.stringify( {'ClusterName': clusterName,
																								 'Desc': desc,
																								 'SdbUser': user,
																								 'SdbPasswd': pwd,
																								 'SdbUserGroup': group,
																								 'InstallPath': installPath } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//枚举集群下的所有主机
function restListHost( async, success, error, complete, clusterName )
{
	var data = { 'cmd': 'list hosts', 'ClusterName': clusterName } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//枚举集群下的所有业务
function restListClusterBusiness( async, success, error, complete, clusterName )
{
	var data = { 'cmd': 'list businesses', 'ClusterName': clusterName } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//枚举主机下的所有业务
function restListHostBusiness( async, success, error, complete, hostName )
{
	var data = { 'cmd': 'list businesses', 'HostName': hostName } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//枚举业务类型
function restQueryBusinessType( async, success, error, complete )
{
	var data = { 'cmd': 'list business type' } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//扫描主机
function restScanHost( async, success, error, complete, clusterName, hostList, user, pwd, ssh, agent )
{
	var data = { 'cmd': 'scan host', 'HostInfo': JSON.stringify( { 'ClusterName': clusterName, 'HostInfo': hostList, 'User': user, 'Passwd': pwd, 'SshPort': ssh, 'AgentService': agent } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//检查主机
function restCheckHost( async, success, error, complete, clusterName, hostList )
{
	var data = { 'cmd': 'check host', 'HostInfo': JSON.stringify( { 'ClusterName': clusterName, 'HostInfo': hostList, 'User': 'root', 'Passwd': '-', 'SshPort': '-', 'AgentService': '-' } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//获取全局安装路径
function restGetInstallPath( async, success, error, complete, clusterName )
{
	var installPath = null ;
	var data = { 'cmd': 'query cluster' } ;
	ajaxSendMsg( data, async, restBeforeSend, function( jsonArr, textStatus, jqXHR ){
		$.each( jsonArr, function( index, clusterInfo ){
			if( clusterInfo['ClusterName'] === clusterName )
			{
				installPath = clusterInfo['InstallPath'] ;
				return ;
			}
		} ) ;
		success( [ { 'ClusterName': clusterName, 'InstallPath': installPath } ], textStatus, jqXHR ) ;
	}, error, complete ) ;
}

//获取业务模板
function restGetBusinessTemplate( async, success, error, complete, businessType )
{
	var data = { 'cmd': 'get business template', 'BusinessType': businessType } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//获取容量预计
function restGetPredictCapacity( async, success, error, complete, clusterName, templateInfo, hostInfo )
{
	var data = null ;
	if( hostInfo !== null )
	{
		data = { 'cmd': 'predict capacity', 'TemplateInfo': JSON.stringify( { 'ClusterName': clusterName, 'Property': templateInfo, 'HostInfo': hostInfo } ) } ;
	}
	else
	{
		data = { 'cmd': 'predict capacity', 'TemplateInfo': JSON.stringify( { 'ClusterName': clusterName, 'Property': templateInfo } ) } ;
	}
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//获取集群下的所有主机详细信息
function restGetClusterHostsInfo( async, success, error, complete, clusterName )
{
	var data = { 'cmd': 'query host', 'filter': JSON.stringify( { 'ClusterName': clusterName } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//获取集群下的所有业务信息
function restGetClusterBusinessInfo( async, success, error, complete, clusterName )
{
	var data = { 'cmd': 'query business', 'filter': JSON.stringify( { 'ClusterName': clusterName } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//获取业务配置
function restGetBusinessConfig( async, success, error, complete, businessConfig )
{
	var data = { 'cmd': 'get business config', 'TemplateInfo': JSON.stringify( businessConfig ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//添加主机
function restAddHosts( async, success, error, complete, clusterName, hostInfo )
{
	var newHostInfo = { 'ClusterName': clusterName, 'HostInfo': hostInfo, 'User': '-', 'Passwd': '-', 'SshPort': '-', 'AgentService': '-' } ;
	var data = { 'cmd': 'add host', 'HostInfo': JSON.stringify( newHostInfo ) } ; ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//查询进度
function restQueryTask( async, success, error, complete, taskID )
{
	var data = { 'cmd': 'query task', 'filter': JSON.stringify( { 'TaskID': taskID } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//安装业务
function restAddBusiness( async, success, error, complete, configInfo )
{
	var data = { 'cmd': 'add business', 'ConfigInfo': JSON.stringify( configInfo ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//卸载业务
function restRemoveBusiness( async, success, error, complete, businessName )
{
	var data = { 'cmd': 'remove business', 'BusinessName': businessName } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//获取主机状态信息
function restQueryHostStatus( async, success, error, complete, hostList )
{
	var data = { 'cmd': 'query host status', 'HostInfo': JSON.stringify( { 'HostInfo': hostList } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//获取日志
function restGetLog( async, success, error, complete, taskID )
{
	var data = { 'cmd': 'get log', 'name': './task/' + taskID + '.log' } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete, false ) ;
}

//删除主机
function restRemoveHost( async, success, error, complete, hostList )
{
	var data = { 'cmd': 'remove host', 'HostInfo': JSON.stringify( { 'HostInfo': hostList } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//删除集群
function restRemoveCluster( async, success, error, complete, clusterName )
{
	var data = { 'cmd': 'remove cluster', 'ClusterName': clusterName } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//修改密码
function restChangePasswd( async, success, error, complete, user, passwd, newPasswd )
{
	var timestamp = parseInt( ( new Date().getTime() ) / 1000 ) ;
	var data = { 'cmd': 'change passwd', 'User': user, 'Passwd': $.md5( passwd ), 'Newpasswd': $.md5( newPasswd ), 'Timestamp': timestamp } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//获取正在进行的任务列表
function restListTasks( async, success, error, complete )
{
	var data = { 'cmd': 'list tasks', 'filter': JSON.stringify( { 'Status': { '$ne': 4 } } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//查询多个任务简略进度
function restGetTaskStatus( async, success, error, complete, taskArrID )
{
	var data = { 'cmd': 'query task', 'filter': JSON.stringify( { '$or': taskArrID } ), 'selector': JSON.stringify( { 'Progress': 1, 'StatusDesc': 1 } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//设置业务鉴权
function restSetBusinessAuth( async, success, error, complete, businessName, user, pwd )
{
	var data = { 'cmd': 'set business authority', 'BusinessName': businessName, 'User': user, 'Passwd': pwd } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//删除业务鉴权
function restRemoveBusinessAuth( async, success, error, complete, businessName )
{
	var data = { 'cmd': 'remove business authority', 'BusinessName': businessName } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}

//查询业务鉴权
function restQueryBusinessAuth( async, success, error, complete, businessName )
{
	var data = { 'cmd': 'query business authority', 'filter': JSON.stringify( { 'BusinessName': businessName } ) } ;
	ajaxSendMsg( data, async, restBeforeSend, success, error, complete ) ;
}