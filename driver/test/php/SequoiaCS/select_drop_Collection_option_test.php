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
	public function testselectCL(SequoiaCS $cs)
	{
		$cl = $cs->selectCollection( "cl_test", '{ ShardingKey:{id:1}, ShardingType:"range", ReplSize:0, Compressed:true }' ) ;
		$this->assertNotEmpty( $cl ) ;
		
		$array_option = array(
			"ShardingKey" => array( "id" => 1),
			"ShardingType" => "range",
			"ReplSize" => 0,
			"Compressed" => true ) ;
		$cl2 = $cs->selectCollection( "cl_test2", $array_option) ;
		$this->assertNotEmpty( $cl2 ) ;
		return $cs ;
	}
	/**
	* @depends testselectCL
	*/
	public function testdropCollection(SequoiaCS $cs)
	{
		$array = $cs->dropCollection( "cl_test" ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$array = $cs->dropCollection( "cl_test2" );
		$this->assertEquals( 0, $array["errno"] ) ;
		return $cs ;
		
	}
	/**
	* @depends testdropCollection
	*/
	public function testdrop(SequoiaCS $cs)
	{
		$array = $cs->drop() ;
		$this->assertEquals( 0, $array["errno"] ) ;
	}
}
?>