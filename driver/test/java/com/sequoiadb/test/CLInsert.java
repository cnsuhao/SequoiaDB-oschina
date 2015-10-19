package com.sequoiadb.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
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
import com.sequoiadb.testdata.SDBTestHelper;


public class CLInsert {
	private static Sequoiadb sdb ;
	private static CollectionSpace cs ;
	private static DBCollection cl ;
	private static DBCursor cursor ;
	private static long i = 0;
	
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
	public void insertOneRecord(){
		System.out.println("begin to test insertOneRecord ...");

		BSONObject obj = new BasicBSONObject();
		BSONObject obj1 = new BasicBSONObject();
		ObjectId id = new ObjectId();;
		obj.put("_id",id) ;
		obj.put("Id", 10);
		obj.put("姓名", "汤姆");
		obj.put("年龄", 30);
		obj.put("Age", 30);

		obj1.put("0", "123456");
		obj1.put("1", "654321");

		obj.put("电话", obj1);
		obj.put("boolean1",true);
		obj.put("boolean2",false);
		obj.put("nullobj",null);
		obj.put("intnum",999999999);
		
		BSONObject temp = new BasicBSONObject();
		DBCursor cur = sdb.getSnapshot(3, temp, temp, temp);
		long time1 = SDBTestHelper.getTotalBySnapShotKey(cur, "TotalInsert");
		System.out.println("before insert, current session total insert num is " + time1);
		long count = 0;
		count = cl.getCount();
		System.out.println("before insert, the count is: " + count);
		cl.insert(obj) ;
		long count1 = 0;
		count1 = cl.getCount();
	    System.out.println("after insert, the count is: " + count1);
		BSONObject empty = new BasicBSONObject();
		cur = sdb.getSnapshot(3, empty, empty, empty);
		long time2 = SDBTestHelper.getTotalBySnapShotKey(cur, "TotalInsert");
		System.out.println("after insert, current session total insert num is " + time2);
		assertEquals(1, time2-time1);
		
        BSONObject tmp = new BasicBSONObject();
        DBCursor tmpCursor = cl.query(tmp, null, null, null);
        while(tmpCursor.hasNext()){
            BSONObject temp1 = tmpCursor.getNext();
            System.out.println(temp1.toString());
        }
		
		BSONObject query = new BasicBSONObject();
		query.put("Id", 10);
		query.put("姓名", "汤姆");
		query.put("年龄", 30);
		query.put("Age", 30);
		query.put("电话.0", "123456");
		query.put("电话.1", "654321");
		query.put("boolean1",true);
		query.put("boolean2",false);
		query.put("nullobj",null);
		query.put("intnum",999999999);
		cursor = cl.query(query, null, null, null);
	    while(cursor.hasNext()){
	    	BSONObject temp1 = cursor.getNext();
	    	System.out.println(temp1.toString());
	    	i++;
	    }
	    long count2 = cl.getCount();
	    System.out.println("after cl.query(), i is: " + i);
	    System.out.println("after cl.query(), the count is: " + count2);
	    assertEquals(1,i);
	}
}
