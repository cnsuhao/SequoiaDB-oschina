<?php
class dropTest extends PHPUnit_Framework_TestCase
{
	public function testselect()
	{
		$sdb=new Sequoiadb();
		$array=$sdb->connect("localhost:50000");
		$this->assertEquals(0,$array['errno']);
		$cs = $sdb->selectCS("cs_test");
		$this->assertNotEmpty($cs);
		return $cs;
	}
	/**
	* @depends testselect
	*/
	public function testdrop(SequoiaCS $cs)
	{
		$array=$cs->drop("fgfdgdgd");
		$this->assertEquals(0,$array['errno']);
	}
}
?>