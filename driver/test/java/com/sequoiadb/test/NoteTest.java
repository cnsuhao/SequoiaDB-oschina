package com.sequoiadb.test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Node;
import com.sequoiadb.base.Node.NodeStatus;
import com.sequoiadb.base.ReplicaGroup;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;


public class NoteTest {
	
	private static Sequoiadb sdb ;
	private static ReplicaGroup rg = null;
	private static Node node = null;
	private static final int PORT = 51000;
	private static boolean isCluster = true ;
	
	@BeforeClass
	public static void setConnBeforeClass() throws Exception{
		sdb = new Sequoiadb(Constants.COOR_NODE_CONN,"","");
		isCluster = Constants.isCluster();
	}
	
	@AfterClass
	public static void DropConnAfterClass() throws Exception {
		sdb.disconnect();
	}
	
	@Before
	public void setUp() throws Exception {
		if (!isCluster)
			return ;
		rg = sdb.getReplicaGroup(Constants.GROUPNAME);
		node = rg.getNode(Constants.HOST, PORT);
	}
	@After
	public void tearDown() throws Exception {
		if (!isCluster)
			return ;
	}

	@Test
	public void test()
	{
		if (!isCluster)
			return ;
		assertTrue( 0 == 0 ) ;
	}
	
	@Ignore
	@Test
	public void traverseClassNode(){
		if (!isCluster)
			return ;
		int id = 0 ;
		id = node.getNodeId();
		assertTrue(id != 0);
		ReplicaGroup s = null;
		s = node.getReplicaGroup();
		assertTrue(s != null);
		Sequoiadb connect = null; 
		DBCursor cursor = null;
		connect = node.connect();
		cursor = connect.getList(4, null, null, null);
		assertTrue(connect != null);
		assertTrue(cursor != null);
		connect.disconnect();
		try{
			cursor = connect.getList(4, null, null, null);
		}catch(BaseException e){
			assertTrue(e.getErrorType().equals("SDB_NETWORK"));
		}
		Sequoiadb ddb = null;
		ddb = node.getSdb();
		assertTrue(ddb != null);
		String hostName = null;
		hostName = node.getHostName();
		assertTrue(hostName != null);
		int port = 0;
		port = node.getPort();
		assertTrue(port == PORT);
		String nodeName = null;
		nodeName = node.getNodeName();
		System.out.println(nodeName);
		NodeStatus status = NodeStatus.SDB_NODE_UNKNOWN;
		status = node.getStatus();
		assertTrue(status != NodeStatus.SDB_NODE_UNKNOWN);
	}

}
