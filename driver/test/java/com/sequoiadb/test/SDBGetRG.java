package com.sequoiadb.test;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sequoiadb.base.Node;
import com.sequoiadb.base.ReplicaGroup;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;


public class SDBGetRG {
	private static Sequoiadb sdb ;
	private static ReplicaGroup rg ;
	private static Node node ;
	private static int groupID ;
	private static String groupName ;
	private static String Name ;
	private static boolean isCluster = true;
	
	@BeforeClass
	public static void setConnBeforeClass() throws Exception{
		isCluster = Constants.isCluster();
		if(!isCluster)
			return;
		sdb = new Sequoiadb(Constants.COOR_NODE_CONN,"","");
		rg = sdb.getReplicaGroup(1000);
		Name = rg.getGroupName();
	}
	
	@AfterClass
	public static void DropConnAfterClass() throws Exception {
		if(!isCluster)
			return;
		sdb.disconnect();
	}
	
	@Before
	public void setUp() throws Exception {
		if(!isCluster)
			return;
		groupID = Constants.GROUPID ;
		groupName = Name ;
	}

	@After
	public void tearDown() throws Exception {
		if(!isCluster)
			return;
	}
	
	@Test
	public void getReplicaGroupById(){
		if(!isCluster)
			return;
		rg = sdb.getReplicaGroup(groupID);
		int id = rg.getId();
	    assertEquals(groupID, id);
	}
	
	@Test
	public void getReplicaGroupByName(){
		if(!isCluster)
			return;
		groupName = Constants.CATALOGRGNAME ;
		rg = sdb.getReplicaGroup(groupName);
		String name = rg.getGroupName();
	    assertEquals(groupName, name);
	}
	
	@Test
	public void getCataReplicaGroupById(){
		if(!isCluster)
			return;
        groupID = 1 ;
		rg = sdb.getReplicaGroup(groupID);
		int id = rg.getId();
	    assertEquals(groupID, id);
	    boolean f = rg.isCatalog();
	    assertTrue( f );
	}
	
	@Test
	public void getCataReplicaGroupByName(){
		if(!isCluster)
			return;
		groupName = Constants.CATALOGRGNAME ;
		rg = sdb.getReplicaGroup(groupName);
		String name = rg.getGroupName();
	    assertEquals(groupName, name);
	    boolean f = rg.isCatalog();
	    assertTrue( f );
	}
	
	@Test
	public void getCataReplicaGroupByName1(){
		if(!isCluster)
			return;
		try {
			groupName = "SYSCatalogGroupForTest" ;
			rg = sdb.createReplicaGroup(groupName);
		}catch(BaseException e){
			assertEquals(new BaseException("SDB_INVALIDARG").getErrorCode(),
					     e.getErrorCode());
			return ;
		}
		assertTrue(false);
	}
	
}
