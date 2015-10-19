package com.sequoiadb.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;


public class CLBulkInsert {
	
	private static Sequoiadb sdb ;
	private static CollectionSpace cs ;
	private static DBCollection cl ;
	private static DBCollection cl1 ;
	private static DBCursor cursor ;
	
	@BeforeClass
	public static void setConnBeforeClass() throws Exception{
		sdb = new Sequoiadb(Constants.COOR_NODE_CONN,"","");
		if(sdb.isCollectionSpaceExist(Constants.TEST_CS_NAME_1)){
			sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
			cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
		}
		else
			cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
		BSONObject conf = new BasicBSONObject();
		conf.put("ReplSize", 0);
		cl = cs.createCollection(Constants.TEST_CL_NAME_1, conf);
		System.out.println("OK");
	}
	
	@AfterClass
	public static void DropConnAfterClass() throws Exception {
		sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
		sdb.disconnect();
	}
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void bulkInsert(){
		List<BSONObject>list = ConstantsInsert.createRecordList(100000);
		cl.bulkInsert(list, DBCollection.FLG_INSERT_CONTONDUP);
		cursor = cl.query();
		int i =0 ;
	    while(cursor.hasNext()){
	    	cursor.getNext();
	    	i++;
	    }
	    assertEquals(100000,i);
	}
	
	@Test
	public void bulkInsert_cl_with_conpress(){
		cl1 = cs.createCollection(Constants.TEST_CL_NAME_2,
		        new BasicBSONObject("Compressed",true).append("ReplSize", 0));
		List<BSONObject>list = ConstantsInsert.createRecordList(100000);
		cl1.bulkInsert(list, DBCollection.FLG_INSERT_CONTONDUP);
		cursor = cl1.query();
		int i =0 ;
	    while(cursor.hasNext()){
	    	cursor.getNext();
	    	i++;
	    }
	    assertEquals(100000,i);
	}
}
