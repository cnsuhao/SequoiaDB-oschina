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
import com.sequoiadb.base.ReplicaGroup;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.net.ConfigOptions;


public class SdbConnect {
	
	private static Sequoiadb sdb ;
	private static CollectionSpace cs ;
	private static DBCollection cl ;
	private static ReplicaGroup rg ;
	private static Node node ;
	private static DBCursor cursor ;
	
	@BeforeClass
	public static void setConnBeforeClass() throws Exception{
		sdb = new Sequoiadb(Constants.COOR_NODE_CONN,"","");
	}
	
	@AfterClass
	public static void DropConnAfterClass() throws Exception {
		sdb.disconnect();
	}
	
	@Before
	public void setUp() throws Exception {
		if(sdb.isCollectionSpaceExist(Constants.TEST_CS_NAME_1)){
			sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
			cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
		}
		else
			cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
		BSONObject conf = new BasicBSONObject();
		conf.put("ReplSize", 0);
		cl = cs.createCollection(Constants.TEST_CL_NAME_1, conf);
	}

	@After
	public void tearDown() throws Exception {
		sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
	}
	
	@Test
	public void sdbConnect() {
		List<String> list = new ArrayList<String>();
		list.add("192.168.20.35:12340");
		list.add("192.168.20.36:12340");
		list.add("123:123");
		list.add("");
		list.add(":12340");
		list.add("192.168.20.40");
		list.add("localhost:50000");
		list.add("localhost:11810");
		list.add("localhost:12340");
		list.add(Constants.COOR_NODE_CONN);

		ConfigOptions options = new ConfigOptions();
		options.setMaxAutoConnectRetryTime(0);
		options.setConnectTimeout(1);
		long begin = 0;
		long end = 0;
		begin = System.currentTimeMillis();
		Sequoiadb sdb1 = new Sequoiadb(list, "", "", options);
		end = System.currentTimeMillis();
		System.out.println("Takes " + (end - begin));
		options.setConnectTimeout(2000);
		sdb1.changeConnectionOptions(options);
		DBCursor cursor = sdb1.getList(4, null, null, null);
		assertTrue(cursor != null);
		sdb1.disconnect();
	}
	
	
}
