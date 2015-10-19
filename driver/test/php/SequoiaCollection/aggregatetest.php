<?php
class select_drop_Collection_option_test extends PHPUnit_Framework_TestCase
{
	public function testconnect()
	{
		$sdb = new Sequoiadb() ;
		$array = $sdb->connect( "localhost:50000" ) ;
		$this->assertEquals( 0, $array['errno'] ) ;
		return $sdb ;
	}
	/**
	* @depends testconnect
	*/
	public function test_whether_cluster( SequoiaDB $sdb )
	{
	   $isCluster = TRUE;
	   $snapshot = $sdb->getSnapshot( SDB_SNAP_DATABASE ) ;
	   $array = $sdb->getError() ;
	   $this->assertEquals( 0, $array['errno'] ) ;
      $record = $snapshot -> getNext() ;
      if( array_key_exists( "Role", $record) )
      {
         $isCluster = false ;
      }
      else
      {
         $isCluster = true ;
      }
      return $isCluster ;
	}
	/**
	* @depends testconnect
	* @depends test_whether_cluster
	*/
	public function testselectCS()
	{
	   $args_array = func_get_args() ;
	   $sdb = $args_array[0] ;
	   $isCluster = $args_array[1] ;
   	$cs = $sdb->selectCS( "cs_test" ) ;
   	$this->assertNotEmpty( $cs ) ;
		return $cs ;
	}
	/**
	* @depends testselectCS
	* @depends test_whether_cluster
	*/
	public function testselectCL()
	{
	   $args_array = func_get_args() ;
	   $cs = $args_array[0] ;
	   $isCluster = $args_array[1] ;
	   if( $isCluster )
	   {
   		$cl = $cs->selectCollection( 'cl_test', '{ReplSize:0}' ) ;
   		$this->assertNotEmpty( $cl ) ;
   		return $cl ;
	   }
		//return $cl ;
	}
	/**
	* @depends testconnect
	* @depends testselectCL
	* @depends test_whether_cluster
	*/
	public function testupdateCL()
	{
	   $args_array = func_get_args() ;
	   $sdb = $args_array[0] ;
	   $cl = $args_array[1] ;
	   $isCluster = $args_array[2] ;
	   if( $isCluster )
	   {
   		$array = $cl->insert( "{ age:1 }" ) ;
   		$this->assertEquals( 0, $array["errno"] ) ;
   		
   		$find_cursor = $cl->aggregate('{$project:{age:1}}') ;
   		$array = $sdb->getError() ;
   		$this->assertEquals( 0, $array["errno"] ) ;
   		
   		$array_find = $find_cursor->getNext() ;
   		$array = $sdb->getError() ;
   		$this->assertEquals( 0, $array["errno"] ) ;
   	  $this->assertEquals( 1, $array_find["age"] ) ;
   	 	
   	  $array = $cl->update( '{"$inc":{age:1}}' );
   		$this->assertEquals( 0, $array["errno"] ) ;
   		
   		$find_cursor = $cl->aggregate('{$match:{age:2}},{$project:{age:1}}') ;
   		$array = $sdb->getError() ;
   		$this->assertEquals( 0, $array["errno"] ) ;
   		 
   		$array_find = $find_cursor->getNext() ;
   		$array = $sdb->getError() ;
   		$this->assertEquals( 0, $array["errno"] ) ;
   				
   		$this->assertEquals( 2, $array_find["age"] ) ;
   	 	  
   		print_r( $array_find ) ;
   		return $cl ;
	   }
	}
	/**
	* @depends testselectCS
	*/
	public function testdrop( SequoiaCS $cs )
	{
		$array = $cs->drop() ;
		$this->assertEquals( 0, $array["errno"] ) ;
	}
	protected function onNotSuccessfulTest( Exception $e )
	{
		$sdb = new Sequoiadb() ;
		$array = $sdb->connect( "localhost:50000" ) ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$sdb->dropCollectionSpace( "cs_test" ) ;
		fwrite( STDOUT, __METHOD__ . "\n" ) ;
		throw $e ;
	}
}
?>