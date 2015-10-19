<?php
class select_drop_CSTest extends PHPUnit_Framework_TestCase
{
	public function testselect()
	{
		$sdb=new Sequoiadb();
		$array=$sdb->connect("localhost:50000");
		$this->assertEquals(0,$array['errno']);
		$cs = $sdb->selectCS("cs_test");
		$this->assertNotEmpty($cs);
		return $sdb;
	}
	/**
	* @depends testselect
	*/
	public function testdrop(SequoiaDB $sdb)
	{
		$array=$sdb->dropCollectionSpace("cs_test");
		$this->assertEquals(0,$array['errno']);
	}
}
?>