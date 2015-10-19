<?php
class _constructTest extends PHPUnit_Framework_TestCase
{
	public function testconnect()
	{
		$sdb=new Sequoiadb();
		$array=$sdb->connect("localhost:50000");
		
		$this->assertEquals(0,$array['errno']);
	}
	public function testconnect2()
	{
		$sdb2=new Sequoiadb();
		$array2=$sdb2->connect("localhost:50000","root","sdbadmin");
		
		$this->assertEquals(0,$array2['errno']);
	}
}
?>