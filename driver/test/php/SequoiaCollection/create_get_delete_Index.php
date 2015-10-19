<?php
class create_get_delete_Indextest extends PHPUnit_Framework_TestCase
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
	public function testselectCS( SequoiaDB $sdb )
	{
		$cs = $sdb->selectCS( "cs_test" ) ;
		$this->assertNotEmpty( $cs ) ;
		return $cs ;
	}
	/**
	* @depends testselectCS
	*/
	public function testselectCL( SequoiaCS $cs )
	{
		$cl = $cs->selectCollection( 'cl_test', '{ReplSize:0}' ) ;
		$this->assertNotEmpty( $cl ) ;
		return $cl ;
	}
	/**
	* @depends testselectCL
	*/
	public function testcreateIndex()
	{
	   $args_array = func_get_args() ;
	   $cl = $args_array[0] ;
		$array = $cl->createIndex( '{name:1, age:-1}', "testIndex", false ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$array = $cl->createIndex( '{name2:1}', "testIndex2", true ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
	
		$array = $cl->insert( '{name:1}' ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$array = $cl->insert( '{name:1}' ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
			
		$array = $cl->insert( '{name2:1}' ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		$array = $cl->insert( '{name2:1}' ) ;
		$this->assertNotEquals( 0, $array["errno"] ) ;
		
		return $cl ;
		
	}
	/**
	* @depends testconnect
	* @depends testcreateIndex
	*/
	public function testgetIndex()
	{	
	   $args_array = func_get_args() ;
	   $sdb = $args_array[0] ;
	   $cl = $args_array[1] ;
		$cursor = $cl->getIndex() ;
		$array = $sdb->getError() ;
		$this->assertEquals( 0, $array["errno"] ) ;
		
		$array_index = $cursor->getNext() ; // ignore
		
		$array_index = $cursor->getNext() ;
		
		$this->assertEquals( "testIndex", $array_index["IndexDef"]["name"] ) ;
		
		$array_index = $cursor->getNext() ;
		$this->assertEquals( "testIndex2", $array_index["IndexDef"]["name"] ) ;
		
		$i = 0 ;
		$cursor = $cl->getIndex() ;
		while( $cursor->getNext() ) 
		{
			$i++ ;
		}
		$this->assertEquals( 3, $i ) ;
		return $cl ;
	}
	
	/**
	* @depends testcreateIndex
	*/
	public function testdeleteIndex()
	{
	   $args_array = func_get_args() ;
	   $cl = $args_array[0] ;
		$array = $cl->deleteIndex( "testIndex" ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
		
		$array = $cl->deleteIndex( "testIndex2" ) ;
		$this->assertEquals( 0, $array["errno"] ) ;
	}
	/**
	* @depends testselectCS
	*/
	public function testdropCS( SequoiaCS $cs )
	{
		$array = $cs->drop() ;
		$this->assertEquals( 0, $array["errno"] ) ;
	}
	protected function onNotSuccessfulTest( Exception $e )
	{
		$sdb = new Sequoiadb() ;
		$array = $sdb->connect( "localhost:50000" ) ;
		$this->assertEquals( 0, $array['errno'] ) ;
		$sdb->dropCollectionSpace( "cs_test" ) ;
		fwrite( STDOUT, __METHOD__ . "\n" ) ;
		throw $e ;
	}
}
?>