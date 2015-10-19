<?php
class split_hash_test extends PHPUnit_Framework_TestCase
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
	public function test_whether_cluster( SequoiaDB $sdb )
	{
	   $isCluster = TRUE;
	   $snapshot = $sdb->getSnapshot( SDB_SNAP_DATABASE ) ;
	   $array = $sdb->getError() ;
	   $this->assertEquals( 0, $array['errno'] ) ;
      $record = $snapshot -> getNext() ;
      if( array_key_exists( "Role", $record) )
      {
         $isCluster = false ;
      }
      else
      {
         $isCluster = true ;
      }
      return $isCluster ;
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
	* @depends test_whether_cluster
	*/
	public function testselectCL()
	{
	   $args_array = func_get_args() ;
	   $cs = $args_array[0] ;
	   $isCluster = $args_array[1] ;
	   if( $isCluster )
	   {
   		$cl = $cs->selectCollection( 'cl_test', '{ ShardingKey:{age:1}, ShardingType:"hash", ReplSize:0, Compressed:true }' ) ;
   		$this->assertNotEmpty( $cl ) ;
   		return $cl ;
	   }
	}
	/**
	* @depends testselectCL
	* @depends test_whether_cluster
	*/
	public function testgetSourceGroupName( )
	{
	   $args_array = func_get_args() ;
	   $cl = $args_array[0] ;
	   $isCluster = $args_array[1] ;
	   if( $isCluster )
	   {
   		$cata = new Sequoiadb() ;
   		$array = $cata->connect( 'localhost:30000' ) ;
   		$this->assertEquals( 0, $array['errno'] ) ;
   		
   		$SYSCAT = $cata->selectCS( 'SYSCAT' ) ;
   		$array = $cata->getError() ;
   		$this->assertEquals( 0, $array['errno'] ) ;
   		
   		$SYSCOLLECTIONS = $SYSCAT->selectCollection( 'SYSCOLLECTIONS' ) ;
   		$array = $cata->getError() ;
   		$this->assertEquals( 0, $array['errno'] ) ;
   		
   		$array_find = $SYSCOLLECTIONS->find( ) ;
   		$array = $cata->getError() ;
   		$this->assertEquals( 0, $array['errno'] ) ;
   		$SourceGroupName = '' ;
   		while( $cursor = $array_find->getNext()  )
   		{
   			$array = $cata->getError() ;
   			$this->assertEquals( 0, $array['errno'] ) ;
   			if( $cursor['Name'] == 'cs_test.cl_test' )
   			{
   				$SourceGroupName = $cursor['CataInfo'][0]['GroupName'] ;
   				echo $SourceGroupName ;
   				break ;
   			}
   		}
   		if( '' == $SourceGroupName )
   		{
   			echo "output array still have problem\n";
   			$cata->install( '{install:false}' ) ;
   			$array_find = $SYSCOLLECTIONS->find( ) ;
   			while( $cursor_str = $array_find->getNext()  )
   			{
   				$str = $cata->getError() ;
   				$this->assertEquals( '{"errno":0}', $str ) ;
   				if( preg_match( "/\"Name\": \"(.+)\", \"Version\"/", $cursor_str, $matches) )
   				{
   					print_r( $matches ) ;
   					if( $matches[1] == 'cs_test.cl_test' )
   					{
   						if( preg_match( "/\"GroupName\": \"(.+?)\"/", $cursor_str, $matches) )
   						{
   							var_dump( $matches ) ;
   							$SourceGroupName = $matches[1] ;
   							echo $SourceGroupName ;
   							break ;
   						}
   					}
   				}
   			}
   		}
   		return $SourceGroupName ;
	   }
	}
	/**
	* @depends testconnect
	* @depends testgetSourceGroupName
	* @depends test_whether_cluster
	*/
	public function testgetOtherDataGroups()
	{
		$args_array = func_get_args() ;
		//print_r( $args_array ) ;
		$sdb = $args_array[0] ;
		$SourceGroupName = $args_array[1] ;
		$isCluster = $args_array[2] ;
		if( $isCluster )
		{
   		$this->assertNotEquals( '', $SourceGroupName ) ;
   		
   		$GroupsCursor = $sdb->getList( SDB_LIST_GROUPS ) ;
   		$array = $sdb->getError();
   		$this->assertEquals( 0, $array['errno'] ) ;
   		
   		$OtherDataGroups = array() ;
   		$RoleGroupNumbers = 0 ;
   		while( $cursor = $GroupsCursor->getNext() )
   		{
   			$array = $sdb->getError();
   			$this->assertEquals( 0, $array['errno'] ) ;
   			if( $cursor['Role' ] == 0 )
   			{
   				if( $cursor['GroupName'] != $SourceGroupName )
   				{
   					$OtherDataGroups[$RoleGroupNumbers] = $cursor['GroupName'] ;
   					++$RoleGroupNumbers ;
   				}
   			}
   		}
   		print_r( $OtherDataGroups ) ;
   		return $OtherDataGroups;
	   }
	}
	/**
	* @depends testgetSourceGroupName
	* @depends test_whether_cluster
	*/
	public function testgetPartition( )
	{
		$args_array = func_get_args() ;
		$isCluster = $args_array[1] ;
		if( $isCluster )
		{
   		$cata = new Sequoiadb() ;
   		$array = $cata->connect( 'localhost:30000' ) ;
   		$this->assertEquals( 0, $array['errno'] ) ;
   		
   		$SYSCAT = $cata->selectCS( 'SYSCAT' ) ;
   		$array = $cata->getError() ;
   		$this->assertEquals( 0, $array['errno'] ) ;
   		
   		$SYSCOLLECTIONS = $SYSCAT->selectCollection( 'SYSCOLLECTIONS' ) ;
   		$array = $cata->getError() ;
   		$this->assertEquals( 0, $array['errno'] ) ;
   		
   		$array_find = $SYSCOLLECTIONS->find( ) ;
   		$array = $cata->getError() ;
   		$this->assertEquals( 0, $array['errno'] ) ;
   		$Partition = '' ;
   		while( $cursor = $array_find->getNext()  )
   		{
   			$array = $cata->getError() ;
   			$this->assertEquals( 0, $array['errno'] ) ;
   			//print_r( $cursor ) ;
   			if( $cursor['Name'] == 'cs_test.cl_test' )
   			{
   				$Partition = $cursor['Partition'] ;
   				break ;
   			}
   		}
   		if( '' == $Partition )
   		{
   			echo "output array still have problem\n";
   			$cata->install( '{install:false}' ) ;
   			$array_find = $SYSCOLLECTIONS->find( ) ;
   			while( $cursor_str = $array_find->getNext()  )
   			{
   				$str = $cata->getError() ;
   				$this->assertEquals( '{"errno":0}', $str ) ;
      				if( preg_match( "/\"Name\": \"(.+)\", \"Version\"/", $cursor_str, $matches) )
   				{
   					print_r( $matches ) ;
   					if( $matches[1] == 'cs_test.cl_test' )
   					{
   						if( preg_match( "/\"Partition\": (.+?),/", $cursor_str, $matches) )
   						{
   							var_dump( $matches ) ;
   							$Partition = $matches[1] ;
   							echo $Partition ;
   							break ;
   						}
   					}
   				}
   			}
   		}
   		return $Partition ;     
	   }
	}
	/**
	* @depends testconnect
	* @depends testselectCL
	* @depends testgetSourceGroupName
	* @depends testgetOtherDataGroups
	* @depends testgetPartition
	* @depends test_whether_cluster
	*/
	public function test_splitCL_hash()
	{
		$args_array = func_get_args() ;
		print_r( $args_array ) ;
		$sdb = $args_array[0] ;
		$cl = $args_array[1] ;
		$sourceGroupName = $args_array[2] ;
		$otherDataGroups = $args_array[3] ;
		$isCluster = $args_array[5] ;
		
		if( $isCluster )
		{
   		$this->assertNotEquals( 1, count( $otherDataGroups ) );
	
   		$Partition = $args_array[4] ;
   		$this->assertNotEquals( 0, $Partition );
   		$Partition_PerGroup = $Partition / ( count( $otherDataGroups ) + 1 ) ;
   		for( $i = 0; $i < count( $otherDataGroups ) ; ++$i )
   		{
   			$str_start = '{Partition:'.round( ( $Partition_PerGroup * $i ) ).'}' ;
   			echo $str_start ;
   			$str_end = '{Partition:'.round( ( $Partition_PerGroup * ( $i + 1 ) ) ).'}' ;
   			echo $str_end ;
   			$cl->split( $sourceGroupName,  $otherDataGroups[$i], $str_start, $str_end ) ;
   			$array = $sdb->getError() ;
   			$this->assertEquals( 0, $array['errno'] ) ;
   		}
   		return $cl ;
	   }
	}
	/**
	* @depends testconnect
	* @depends test_splitCL_hash
	* @depends test_whether_cluster
	*/
	public function test_aftersplit_insert()
	{
	   $args_array = func_get_args() ;
	   $sdb = $args_array[0] ;
	   $cl = $args_array[1] ;
	   $isCluster = $args_array[2] ;
	   if( $isCluster )
	   {
   		for( $i = 0 ; $i <3000 ; ++$i )
   		{
   			$array = $cl->insert( '{age:'.$i.'}' ) ;
   			$this->assertEquals( 0, $array['errno'] ) ;
   		}
	   }
	}
	/**
	* @depends testselectCS
	*/
	public function testdrop( SequoiaCS $cs )
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