package com.sequoiadb.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.util.JSON;
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


public class CLGetQueryMeta {
	
	private static Sequoiadb sdb ;
	private static CollectionSpace cs ;
	private static DBCollection cl ;
	private static DBCursor cursor ;
	private static DBCursor datacursor ;
	private static long RECORDNUM = 100000;
	
	@BeforeClass
	public static void setConnBeforeClass() throws Exception{
		sdb = new Sequoiadb(Constants.COOR_NODE_CONN,"","");
		ConstantsInsert.insertVastRecord(Constants._HOST, Constants._SERVER,
				                         Constants.TEST_CS_NAME_1, Constants.TEST_CL_NAME_1,
				                         Constants.SDB_PAGESIZE_4K, RECORDNUM);
		cs = sdb.getCollectionSpace(Constants.TEST_CS_NAME_1);
		cl = cs.getCollection(Constants.TEST_CL_NAME_1);
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
	public void getQueryMeta(){
		try{
         BSONObject sessionAttrObj = new BasicBSONObject();
         sessionAttrObj.put("PreferedInstance","M");
         sdb.setSessionAttr(sessionAttrObj);
			BSONObject empty = new BasicBSONObject();
			BSONObject subobj = new BasicBSONObject();
			subobj.put("$gt", 1);
			subobj.put("$lt", 99);
			BSONObject condition = new BasicBSONObject();
			condition.put("age", subobj);
	        System.out.println("condition is: "+condition.toString());
			BSONObject select = new BasicBSONObject();
			select.put("", "ageIndex");
			BSONObject orderBy = new BasicBSONObject();
			orderBy.put("Indexblocks", 1);
			cursor = cl.getQueryMeta(condition, orderBy, empty, 0, 0, 0);
		    long i = 0 ;
		    while(cursor.hasNext()){
		    	System.out.println(cursor.getNext().toString());
			    BSONObject temp = cursor.getCurrent();
			    BSONObject hint = new BasicBSONObject();
			    hint.put("Indexblocks", temp.get("Indexblocks"));
			    System.out.println("hint is: "+hint.toString());
			    datacursor = cl.query(null, null, null, hint, 0, 0, 0);
			    while( datacursor.hasNext()){
			    	i++ ;
			    	datacursor.getNext();
			    }
		    }
		    System.out.println("total record is: "+i);
		    assertEquals(RECORDNUM,i);
		} catch (BaseException e) {
			e.printStackTrace();
		}
	}
}
