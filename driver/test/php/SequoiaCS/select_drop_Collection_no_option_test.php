<?php
class select_drop_Collection_no_option_test extends PHPUnit_Framework_TestCase
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
	public function testselectCL(SequoiaCS $cs)
	{
		$cl=$cs->selectCollection("cl_test");
		$this->assertNotEmpty($cl);
		return $cs;
	}
	/**
	* @depends testselectCL
	*/
	public function testdropCollection(SequoiaCS $cs)
	{
		$array=$cs->dropCollection("cl_test");
		$this->assertEquals(0,$array["errno"]);
		return $cs;
		
	}
	/**
	* @depends testdropCollection
	*/
	public function testdrop(SequoiaCS $cs)
	{
		$array=$cs->drop();
		$this->assertEquals(0,$array["errno"]);
	}
}
?>