package com.sequoiadb.testdata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;

import com.sequoiadb.base.*;
import com.sequoiadb.exception.BaseException;

public class SDBTestHelper {
    public static long getTotalBySnapShotKey(DBCursor snapshotCur, String key) {
        long total = 0;
        while (snapshotCur.hasNext()) {
            BSONObject result = snapshotCur.getNext();
            total += (Long) result.get(key);
        }

        return total;
    }

    public static int waitIndexCreateFinish(DBCollection cl, String indexName,
            int count) {
        int i = 0;
        DBCursor cursor = cl.getIndex(indexName);
        while ((cursor == null || !cursor.hasNext())) {
            if (i > count) {
                throw new BaseException(
                        ("wait index create failed:" + indexName));
            }
            i++;
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            cursor = cl.getIndex(indexName);
        }

        return 0;
    }

    public static int waitIndexDropFinish(DBCollection cl, String indexName,
            int count) {
        int i = 0;
        DBCursor cursor = cl.getIndex(indexName);
        while ((cursor != null && cursor.hasNext())) {
            if (i > count) {
                throw new BaseException(("wait index drop failed:" + indexName));
            }
            i++;
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            cursor = cl.getIndex(indexName);
        }

        return 0;
    }

    public static BSONObject byteArrayToBSONObject(byte[] array)
            throws BaseException {
        if (array == null || array.length == 0)
            return null;

        BSONDecoder d = new BasicBSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        try {
            d.decode(new ByteArrayInputStream(array, 0, array.length), cb);
            BSONObject o1 = (BSONObject) cb.get();
            return o1;
        }
        catch (IOException e) {
            throw new BaseException("SDB_INVALIDARG", e);
        }
    }

    public static void displayByteBuffer(ByteBuffer buffer) {
        displayByteArray(buffer.array());
    }

    public static void displayByteArray(byte[] array) {
        for (int i = 0; i < array.length; i++) {
            int tmp = array[i] & 0xFF;
            if (tmp < 0x10){
                System.out.print("0");
            }
            System.out.print(Integer.toHexString(tmp).toUpperCase());
            System.out.print(" ");
        }
        
        System.out.println();
    }
    
    public static Date millisToDate( long millis ) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        return calendar.getTime();
    }
    
    public static void println(String msg) {
        String fileName;
        int lineNum;
        fileName = Thread.currentThread().getStackTrace()[2].getFileName();
        lineNum  = Thread.currentThread().getStackTrace()[2].getLineNumber();
        System.out.println( getCurrentData() + " " + fileName + ":" + lineNum 
                + " - " + msg);
    }
    
    public static String getCurrentData() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }
}
