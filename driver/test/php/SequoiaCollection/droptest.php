<?php
class droptest extends PHPUnit_Framework_TestCase
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
	public function testdropCL()
	{
	   $args_array = func_get_args() ;
	   $cl = $args_array[0] ;
		$array = $cl->drop() ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$array = $cl->insert( '{a:1}' ) ;
		$this->assertEquals( -23, $array["errno"] ) ;
	}
	/**
	* @depends testselectCS
	*/
	public function testdropCS( SequoiaCS $cs )
	{
		$array = $cs->drop() ;
		$this->assertEquals( 0, $array["errno"] ) ;
	}
}
?>