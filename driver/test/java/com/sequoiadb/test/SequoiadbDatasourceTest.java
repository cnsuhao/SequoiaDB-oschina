package com.sequoiadb.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
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
import com.sequoiadb.base.DBLob;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.base.SequoiadbConstants;
import com.sequoiadb.base.SequoiadbDatasource;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.testdata.SDBTestHelper;

public class SequoiadbDatasourceTest {
    private SequoiadbDatasource ds;
    
    @BeforeClass
    public static void setConnBeforeClass() throws Exception {

    }

    @AfterClass
    public static void DropConnAfterClass() throws Exception {

    }

    @Before
    public void setUp() throws Exception {
        List<String> coords = new ArrayList<String>() ;
        coords.add(Constants.COOR_NODE_CONN);

        try{
            ds  = new SequoiadbDatasource(coords, "", "", null, null);
        }
        catch( Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
    }
    
    /*
     * connect one
     * */
    @Test
    public void testConnectOne() throws BaseException, InterruptedException {
        Sequoiadb sdb = ds.getConnection();
        CollectionSpace cs ;
        if (sdb.isCollectionSpaceExist(Constants.TEST_CS_NAME_1)) {
            sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
            cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
        }
        else {
            cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
        }
        BSONObject conf = new BasicBSONObject();
        conf.put("ReplSize", 0);
        DBCollection cl = cs.createCollection(Constants.TEST_CL_NAME_1, conf);
        
        BSONObject obj = new BasicBSONObject();
        obj.put("Id", 10);
        obj.put("Age", 30);
        
        cl.insert(obj);
        
        DBCursor cursor = cl.query();
        int i = 0;
        while (cursor.hasNext()) {
            BSONObject record = cursor.getNext();
            System.out.print(record);
            i++;
        }
        assertEquals(1, i);
        
        sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
    }
}
