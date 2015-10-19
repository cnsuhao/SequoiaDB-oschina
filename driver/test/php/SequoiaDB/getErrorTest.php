<?php
class _constructTest extends PHPUnit_Framework_TestCase
{
	public function testgetError()
	{
		$sdb = new Sequoiadb() ;
		$sdb->connect( "localhost" ) ;
		$array = $sdb->getError();
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$sdb->install( '{ "install":false }' ) ;
		$str = $sdb->getError();
		$this->assertEquals( '{"errno":0}', $str ) ;
	}
}
?>