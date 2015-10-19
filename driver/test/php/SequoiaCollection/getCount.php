<?php
class getCounttest extends PHPUnit_Framework_TestCase
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
	public function testCount()
	{
	   $args_array = func_get_args() ;
	   $cl = $args_array[0] ;
		$i = 1 ;
		for( ; $i <= 500 ; $i++ )
		{
			$cl->insert( '{a:1}' ) ;
		}
		$i = $i - 1 ;
		$this->assertEquals( $i, $cl->count() ) ;
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