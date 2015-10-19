package com.sequoiadb.test;

import static org.junit.Assert.assertEquals;



import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.testdata.SDBTestHelper;

public class CLSelectQuery {
   private static Sequoiadb sdb;
   private static CollectionSpace cs;
   private static DBCollection cl;

   @BeforeClass
   public static void setConnBeforeClass() throws Exception {

   }

   @AfterClass
   public static void DropConAfterClass() throws Exception {

   }

   @Before
   public void setUp() throws Exception {
      sdb = new Sequoiadb(Constants.COOR_NODE_CONN, "", "");
      if (sdb.isCollectionSpaceExist(Constants.TEST_CS_NAME_1)) {
         sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
         cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
      }
      else
         cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
      BSONObject conf = new BasicBSONObject();
      conf.put("ReplSize", 0);
      cl = cs.createCollection(Constants.TEST_CL_NAME_1, conf);
      BSONObject obj = new BasicBSONObject();
      obj.put( "商品编号", 1000001 );
      BSONObject arr = new BasicBSONList();
      BSONObject subObj1 = new BasicBSONObject();
      BSONObject subObj2 = new BasicBSONObject();
      subObj1.put( "中文名", "电子笔" );
      subObj1.put( "英文名", "Mobile Digital Pen" );
      arr.put( "0", subObj1 );
      arr.put( "1", subObj2 );
      obj.put( "商品名", arr );
      obj.put( "品牌名称", "Mvpen" );
      BSONObject arr1 = new BasicBSONList();
      arr1.put( "0", "红");
      arr1.put( "1", "橙");
      arr1.put( "2", "青");
      arr1.put( "3", "蓝");
      arr1.put( "4", "紫");
      arr1.put( "5", "黑");
      arr1.put( "6", "白");
      obj.put( "颜色", arr1 );
      BSONObject subObj3 = new BasicBSONObject();
      subObj3.put( "宽度", 86.59 );
      BSONObject subObj4 = new BasicBSONObject();
      subObj4.put( "高度", 11.21 );
      BSONObject subObj5 = new BasicBSONObject();
      subObj5.put( "深度", 23.29 );
      BSONObject arr2 = new BasicBSONList();
      arr2.put( "0", subObj3 );
      arr2.put( "1", subObj4 );
      arr2.put( "2", subObj5 );
      obj.put( "规格参数", arr2 );
      BSONObject subObj6 = new BasicBSONObject();
      subObj6.put( "国家", "中国" );
      subObj6.put( "省份", "广东" );
      subObj6.put( "城市", "深圳" );
      BSONObject subObj7 = new BasicBSONObject();
      subObj7.put( "国家", "中国" );
      subObj7.put( "省份", "浙江" );
      subObj7.put( "城市", "温州" );
      BSONObject subObj8 = new BasicBSONObject();
      subObj8.put( "国家", "中国" );
      subObj8.put( "省份", "广东" );
      subObj8.put( "城市", "珠海" );
      BSONObject arr3 = new BasicBSONList();
      arr3.put( "0", subObj6 );
      arr3.put( "1", subObj7 );
      arr3.put( "2", subObj8 );
      obj.put( "产地", arr3 );
      obj.put( "包装类型", "盒装");
      cl.insert( obj );
   }

   @After
   public void tearDown() throws Exception {
       sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
       sdb.disconnect();
   }

   @Test
   public void testQuery() {
      DBCursor cursor = cl.query();
      int i = 0;
      BSONObject obj = null;
      while (cursor.hasNextRaw()) {
         byte[] bytes = cursor.getNextRaw();
         obj = SDBTestHelper.byteArrayToBSONObject(bytes);
         System.out.println(obj.toString());
         i++;
      }
      assertEquals(1,i);
   }
   
   @Test
   public void testQueryElementMatch() {
	  BSONObject matchObj = null;
	  BSONObject orderObj = null;
	  BSONObject hintObj = null;
	  BSONObject selectSubObj1 = new BasicBSONObject();
	  selectSubObj1.put("省份", "广东");
	  BSONObject selectSubObj2 = new BasicBSONObject();
	  selectSubObj2.put("$elemMatch", selectSubObj1 );
	  BSONObject selectObj = new BasicBSONObject();
	  selectObj.put("产地", selectSubObj2);
	  System.out.println(selectObj.toString());
      DBCursor cursor = cl.query(matchObj, selectObj, orderObj, hintObj);
      int i = 0;
      BSONObject obj = null;
      while (cursor.hasNextRaw()) {
         byte[] bytes = cursor.getNextRaw();
         obj = SDBTestHelper.byteArrayToBSONObject(bytes);
         System.out.println(obj.toString());
         i++;
      }
      assertEquals(1,i);
   }
   
   @Test
   public void testQueryElementMatchOne() {
	  BSONObject matchObj = null;
	  BSONObject orderObj = null;
	  BSONObject hintObj = null;
	  BSONObject selectSubObj1 = new BasicBSONObject();
	  selectSubObj1.put("省份", "广东");
	  BSONObject selectSubObj2 = new BasicBSONObject();
	  selectSubObj2.put("$elemMatchOne", selectSubObj1 );
	  BSONObject selectObj = new BasicBSONObject();
	  selectObj.put("产地", selectSubObj2);
	  System.out.println(selectObj.toString());
      DBCursor cursor = cl.query(matchObj, selectObj, orderObj, hintObj);
      int i = 0;
      BSONObject obj = null;
      while (cursor.hasNextRaw()) {
         byte[] bytes = cursor.getNextRaw();
         obj = SDBTestHelper.byteArrayToBSONObject(bytes);
         System.out.println(obj.toString());
         i++;
      }
      assertEquals(1,i);
   }
   
   @Test
   public void testQuerySlice() {
	  BSONObject matchObj = null;
	  BSONObject orderObj = null;
	  BSONObject hintObj = null;
	  BSONObject selectSubArr1 = new BasicBSONList();
	  selectSubArr1.put("0", 4);
	  selectSubArr1.put("1", 1);
	  BSONObject selectSubObj1 = new BasicBSONObject();
	  selectSubObj1.put("$slice", selectSubArr1);
	  BSONObject selectObj = new BasicBSONObject();
	  selectObj.put("颜色", selectSubObj1);
	  System.out.println(selectObj.toString());
      DBCursor cursor = cl.query(matchObj, selectObj, orderObj, hintObj);
      int i = 0;
      BSONObject obj = null;
      while (cursor.hasNextRaw()) {
         byte[] bytes = cursor.getNextRaw();
         obj = SDBTestHelper.byteArrayToBSONObject(bytes);
         System.out.println(obj.toString());
         System.out.println(obj.get("颜色").toString());
         i++;
      }
      assertEquals(1,i);
   }
   
   @Test
   public void testQueryDefault() {
	  BSONObject matchObj = null;
	  BSONObject orderObj = null;
	  BSONObject hintObj = null;
	  BSONObject selectSubObj1 = new BasicBSONObject();
	  selectSubObj1.put("$default", "defaultValue");
	  BSONObject selectObj = new BasicBSONObject();
	  selectObj.put("规格参数.高度", selectSubObj1);
	  System.out.println(selectObj.toString());
      DBCursor cursor = cl.query(matchObj, selectObj, orderObj, hintObj);
      int i = 0;
      BSONObject obj = null;
      while (cursor.hasNextRaw()) {
         byte[] bytes = cursor.getNextRaw();
         obj = SDBTestHelper.byteArrayToBSONObject(bytes);
         System.out.println(obj.toString());
         i++;
      }
      assertEquals(1,i);
   }
   
   @Test
   public void testQueryInclude() {
	  BSONObject matchObj = null;
	  BSONObject orderObj = null;
	  BSONObject hintObj = null;
	  BSONObject selectSubObj1 = new BasicBSONObject();
	  selectSubObj1.put("$include", 0);
	  BSONObject selectObj = new BasicBSONObject();
	  selectObj.put("产地", selectSubObj1);
	  System.out.println(selectObj.toString());
      DBCursor cursor = cl.query(matchObj, selectObj, orderObj, hintObj);
      int i = 0;
      BSONObject obj = null;
      while (cursor.hasNextRaw()) {
         byte[] bytes = cursor.getNextRaw();
         obj = SDBTestHelper.byteArrayToBSONObject(bytes);
         System.out.println(obj.toString());
         i++;
      }
      assertEquals(1,i);
   }
}

