<?php
class _constructTest extends PHPUnit_Framework_TestCase
{
	public function testclose()
	{
		$sdb=new Sequoiadb();
		$sdb->connect("localhost:50000");
		$array=$sdb->getError();
		$this->assertEquals(0,$array['errno']);
		$sdb->close();
		$sdb->selectCS("cs_test");
		$array=$sdb->getError();
		$this->assertNotEquals(0,$array['errno']);
	}
}
?>