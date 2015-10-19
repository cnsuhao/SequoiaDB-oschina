<?php
class exec_and_execUpdateCSTest extends PHPUnit_Framework_TestCase
{
	public function testconnect()
	{
		$sdb = new Sequoiadb() ;
		$array = $sdb->connect( "localhost:50000" ) ;
		$this->assertEquals( 0, $array['errno'] ) ;
		return $sdb ;
	}
	/**
	* @depends testconnect
	*/
	public function testexecUpdate( SequoiaDB $sdb )
	{
		$array = $sdb->execUpdateSQL("create collectionspace cs_test") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$array = $sdb->execUpdateSQL("drop collectionspace cs_test") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$array = $sdb->execUpdateSQL("create collectionspace cs_test") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$array = $sdb->execUpdateSQL("create collection cs_test.cl_test") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$array = $sdb->execUpdateSQL("drop collection cs_test.cl_test") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$array = $sdb->execUpdateSQL("create collection cs_test.cl_test") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$array = $sdb->execUpdateSQL("create index test_index on cs_test.cl_test (age)") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$array = $sdb->execUpdateSQL("drop index test_index on cs_test.cl_test") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		for( $i = 0; $i < 100; ++$i)
		{
			$array = $sdb->execUpdateSQL("insert into cs_test.cl_test(age,name) values(20,\"Tom\")") ;
			$this->assertEquals( 0, $array['errno'] ) ;
		}
		$array = $sdb->execUpdateSQL("update cs_test.cl_test set age=25") ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		return $sdb;
	}
	/**
	* @depends testexecUpdate
	*/
	public function testexec(SequoiaDB $sdb)
	{
		$sdb->execSQL("list collections") ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$sdb->execSQL("select * from cs_test.cl_test") ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$sdb->execSQL("list collectionspaces") ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$sdb->execSQL("select age,count(name) as 员工总数 from cs_test.cl_test group by age ") ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$sdb->execSQL("select * from cs_test.cl_test order by age desc") ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$sdb->execSQL("select * from cs_test.cl_test limit 5") ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array['errno'] ) ;
		
		$sdb->execSQL("select * from cs_test.cl_test offset 5") ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array['errno'] ) ;
	}
	/**
	* @depends testexecUpdate
	*/
	public function testdrop(SequoiaDB $sdb)
	{
		$array = $sdb->execUpdateSQL("drop collectionspace cs_test") ;
		$this->assertEquals( 0, $array['errno'] ) ;
	}
	protected function onNotSuccessfulTest( Exception $e )
	{
		$sdb = new Sequoiadb() ;
		$array = $sdb->connect( "localhost:50000" ) ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$array = $sdb->execUpdateSQL("drop collectionspace cs_test") ;
		fwrite( STDOUT, __METHOD__ . "\n" ) ;
		throw $e ;
	}
}
?>