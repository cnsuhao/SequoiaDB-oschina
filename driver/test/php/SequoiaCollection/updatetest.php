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
	public function testselectCS( SequoiaDB $sdb )
	{
		$cs = $sdb->selectCS( "cs_test" ) ;
		$this->assertNotEmpty( $cs ) ;
		return $cs ;
	}
	/**
	* @depends testselectCS
	*/
	public function testselectCL( SequoiaCS $cs )
	{
		$cl = $cs->selectCollection( 'cl_test', '{ReplSize:0}' ) ;
		$this->assertNotEmpty( $cl ) ;
		return $cl ;
	}
	/**
	* @depends testconnect
	* @depends testselectCL
	*/
	public function testupdateCL()
	{
	   $args_array = func_get_args() ;
	   $sdb = $args_array[0] ;
	   $cl = $args_array[1] ;
		$array = $cl->insert( "{ age:1 }" ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		
		$find_cursor = $cl->find() ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array["errno"] ) ;
		
		$array_find = $find_cursor->getNext() ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array["errno"] ) ;
	  $this->assertEquals( 1, $array_find["age"] ) ;
	 	
	  $array = $cl->update( '{"$inc":{age:1}}' );
		$this->assertEquals( 0, $array["errno"] ) ;
		
		$find_cursor = $cl->find() ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array["errno"] ) ;
		 
		$array_find = $find_cursor->getNext() ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array["errno"] ) ;
				
		$this->assertEquals( 2, $array_find["age"] ) ;
	  
		print_r( $array_find ) ;
		return $cl ;
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