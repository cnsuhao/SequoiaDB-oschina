package com.sequoiadb.test;

import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.Test;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;

/*启动多个线程向集合表中插入数据*/
public class MultiThreadInsert implements Runnable {
	Sequoiadb sdb;
	CollectionSpace cs;
	DBCollection cl;
	int num = 10;

	public MultiThreadInsert() {
		sdb = new Sequoiadb(Constants.COOR_NODE_CONN, "", "");
		if(sdb.isCollectionSpaceExist(Constants.TEST_CS_NAME_1)){
			cs = sdb.getCollectionSpace(Constants.TEST_CS_NAME_1);
			if(cs.isCollectionExist(Constants.TEST_CL_NAME_1))
				cl = cs.getCollection(Constants.TEST_CL_NAME_1);
			else
				cl = cs.createCollection(Constants.TEST_CL_NAME_1);
		}else {
			cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
			cl = cs.createCollection(Constants.TEST_CL_NAME_1);
		}
		
	}
	
	@Override
	public void run() {
		List<BSONObject> list = null;
		list = new ArrayList<BSONObject>();
			for (int j = 0; j < num; j++) {
				BSONObject obj = new BasicBSONObject();
				obj.put("ThreadID", Thread.currentThread().getId());
				obj.put("NO", Thread.currentThread().getId() + "_" + String.valueOf(j));
				list.add(obj);
			}
		cl.bulkInsert(list, DBCollection.FLG_INSERT_CONTONDUP);
	}
}
