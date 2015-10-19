<?php
class _constructTest extends PHPUnit_Framework_TestCase
{
	public function testconstruct()
	{
		$sdb=new Sequoiadb("localhost:50000");
		$this->assertNotEmpty($sdb);
	}
	public function testconstruct2()
	{
		$sdb2=new Sequoiadb("localhost:50000","root","sdbadmin");
		$this->assertNotEmpty($sdb2);
	}
}
?>