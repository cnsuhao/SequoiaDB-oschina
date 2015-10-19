package com.sequoiadb.test;

import static org.junit.Assert.*;

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


public class SdbConditionAndSelectorQuery {
	private static Sequoiadb sdb ;
	private static CollectionSpace cs ;
	private static DBCollection cl ;
	
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
		List<BSONObject>list = ConstantsInsert.createRecordList(100);
		cl.bulkInsert(list, DBCollection.FLG_INSERT_CONTONDUP);
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
	public void testSdbConditionAndSelectorQuery(){
		BSONObject query = new BasicBSONObject();
		BSONObject con = new BasicBSONObject();
		BSONObject sel = new BasicBSONObject();
		BSONObject orderBy = new BasicBSONObject();
		BSONObject hint = new BasicBSONObject();
		
		con.put("$gte",0);
		con.put("$lte",20);
		
		query.put("Id", con);
		
		sel.put("Id", "");
		sel.put("str", "");
		
		orderBy.put("Id",-1);
		
		hint.put("", "Id");
		
		DBCursor cursor = cl.query(query,sel,orderBy,hint);
		int i = 20 ;
		while(cursor.hasNext()){
			if(!((cursor.getNext().get("Id").toString()).equals(Integer.toString(i)))){
				assertTrue(false);
				break;
			}
			i--;
		}
	}
	@Test
	public void testGetCount(){
		BSONObject query = new BasicBSONObject();
		BSONObject con = new BasicBSONObject();
		
		con.put("$gte", 0);
		con.put("$lte", 9);
		
		query.put("Id", con);
		
		long count = cl.getCount(query);
		assertTrue(count == 10);
	}
	
	@Test
	public void testDelete(){
		BSONObject con = new BasicBSONObject();
		con.put("Id", 0) ;
		cl.delete(con);
		DBCursor cursor = cl.query(con,null,null,null);
		assertNull(cursor.getNext());
	}
	@Test
	public void testDeleteByHint(){
		BSONObject con = new BasicBSONObject();
		BSONObject hint = new BasicBSONObject();
		con.put("Id", 1) ;
		hint.put("", "Id");
		cl.delete(con,hint);
		DBCursor cursor = cl.query(con,null,null,hint);
		assertNull(cursor.getNext());
	}
}
