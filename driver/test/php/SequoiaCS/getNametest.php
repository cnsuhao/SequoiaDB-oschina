<?php
class getNameTest extends PHPUnit_Framework_TestCase
{
	public function testselectCS()
	{
		$sdb=new Sequoiadb();
		$array=$sdb->connect("localhost:50000");
		$this->assertEquals(0,$array['errno']);
		$cs = $sdb->selectCS("cs_test");
		$this->assertNotEmpty($cs);
		return $cs;
	}
	/**
	* @depends testselectCS
	*/
	public function testgetName(SequoiaCS $cs)
	{
		$csName=$cs->getName();
		$this->assertEquals("cs_test",$csName);
		return $cs;
	}
	/**
	* @depends testgetName
	*/
	public function testdrop(SequoiaCS $cs)
	{
		$array=$cs->drop();
		$this->assertEquals(0,$array["errno"]);
	}
}
?>