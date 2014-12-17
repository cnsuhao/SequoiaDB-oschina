<?php
$isfirst = true ;
$cscl_list = '{"name":"数据库","child":{"name":"集合空间","child":[' ;
$db -> install ( "{ install : false }" ) ;
$cursor = $db -> getSnapshot ( SDB_SNAP_COLLECTIONSPACE ) ;
if ( !empty ( $cursor ) )
{
	while ( $arr = $cursor -> getNext() )
	{
		if ( !$isfirst )
		{
			$cscl_list .= "," ;
		}
		else
		{
			$isfirst = false ;
		}
		$cscl_list .= $arr ;
	}
}
$db -> install ( "{ install : true }" ) ;
$cscl_list .= ']}}' ;
$smarty -> assign( "cscl_list", $cscl_list ) ;

?>