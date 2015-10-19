<?php
class select_drop_Collection_option_test extends PHPUnit_Framework_TestCase
{
	public function testselectCS()
	{
		$sdb = new Sequoiadb() ;
		$array = $sdb->connect( "localhost:50000" ) ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$cs = $sdb->selectCS( "cs_test" ) ;
		$this->assertNotEmpty( $cs ) ;
		return $cs ;
	}
	/**
	* @depends testselectCS
	*/
	public function testselectCL( SequoiaCS $cs )
	{
		$cl = $cs->selectCollection( "cl_test" ) ;
		$this->assertNotEmpty( $cl ) ;
		return $cl ;
	}
	/**
	* @depends testselectCL
	*/
	public function testinsert_remove()
	{
	   $args_array = func_get_args() ;
	   $cl = $args_array[0] ;
		$array = $cl->insert( "{ age:1 }" ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$array = $cl->remove( "{ age:1 }" ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$this->assertEquals( 0, $cl->count() ) ;
		
		$insert_data = array( "age" => 2 ) ;
		$array = $cl->insert( $insert_data ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$array = $cl->remove( $insert_data ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$this->assertEquals( 0, $cl->count() ) ;
		
		$array = $cl->insert( "{ age:1 }" ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$insert_data = array( "age" => 2 ) ;
		$array = $cl->insert( $insert_data ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$array = $cl->remove() ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$this->assertEquals( 0, $cl->count() ) ;
		
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
}
?>