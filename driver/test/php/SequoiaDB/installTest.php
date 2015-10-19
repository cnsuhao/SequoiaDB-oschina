<?php
class installTest extends PHPUnit_Framework_TestCase
{
	public function testconnect()
	{
		$sdb = new Sequoiadb() ;
		$array = $sdb->connect("localhost:50000") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		return $sdb ;
	}
	/**
	* @depends testconnect
	*/
	public function testinstall(SequoiaDB $sdb)
	{
		$array_install = array( "install" => false ) ;
		$sdb->install( $array_install ) ;
		$sdb->selectCS( "cs_test" ) ;
		$str = $sdb->getError() ;
		$this->assertEquals( '{"errno":0}', $str ) ;
		
		$array_install = array( "install" => true ) ;
		$sdb->install( $array_install ) ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$sdb->install( '{ "install":false }' ) ;
		$str = $sdb->dropCollectionSpace( "cs_test" ) ;
		$this->assertEquals( '{"errno":0}', $str ) ;
		
		$sdb->install( '{ "install":true }' ) ;
		$array = $sdb->dropCollectionSpace( "cs_test" ) ;
		$this->assertEquals( -34, $array["errno"] ) ;
		$sdb->install( '{ "install":false }' ) ;
		$str = $sdb->getError() ;
		$this->assertEquals( '{"errno":-34}', $str ) ;
		
	}
}
?>