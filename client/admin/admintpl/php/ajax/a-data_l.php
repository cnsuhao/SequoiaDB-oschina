<?php
$isfirst = true ;
//$cscl_list = '{"name":"数据库","child":{"name":"集合空间","child":[' ;
//$db -> install ( "{ install : false }" ) ;

$array_1 = array() ;
$array_2 = array() ;

$cursor = $db -> getList ( SDB_LIST_COLLECTIONSPACES ) ;
if ( !empty ( $cursor ) )
{
	while ( $arr = $cursor -> getNext() )
	{
		array_push( $array_1, $arr ) ;
	}
}
//$db -> install ( "{ install : true }" ) ;
//$cscl_list .= ']}}' ;

$cursor = $db -> getSnapshot ( SDB_LIST_COLLECTIONSPACES ) ;
if ( !empty ( $cursor ) )
{
	while ( $arr = $cursor -> getNext() )
	{
		array_push( $array_2, $arr ) ;
	}
}

$cscl_list = arrayMerges( $array_1, $array_2 ) ;
$cscl_list = json_encode( array( 'name' => '数据库', 'child' => array( 'name' => '集合空间', 'child' => $cscl_list ) ), true ) ;

$smarty -> assign( "cscl_list", $cscl_list ) ;

function arrayMerges( $a, $b )
{
   foreach( $a as $key => $value )
   {
      foreach( $b as $key2 => $value2 )
      {
         if( $value2['Name'] == $value['Name'] )
         {
            $a[$key] = array_merge( $a[$key], $b[$key2] ) ;
         }
      }
   }
   return $a ;
}

?>