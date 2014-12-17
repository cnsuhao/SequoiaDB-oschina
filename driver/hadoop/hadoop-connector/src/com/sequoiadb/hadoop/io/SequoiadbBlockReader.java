package com.sequoiadb.hadoop.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.util.JSON;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.hadoop.split.SdbBlockSplit;
import com.sequoiadb.hadoop.util.SequoiadbConfigUtil;

/**
 * 
 * 
 * @className：SequoiadbReader
 * 
 * @author： gaoshengjie
 * 
 * @createtime:2013年12月10日 下午4:33:34
 * 
 * @changetime:TODO
 * 
 * @version 1.0.0
 * 
 */
public class SequoiadbBlockReader extends RecordReader<Object, BSONWritable> {
	private static final Log log = LogFactory.getLog(SequoiadbBlockReader.class);
    private BSONObject current;
    private final SdbBlockSplit sdbBlockSplit;
    private final Sequoiadb sequoiadb;
    private final DBCursor cursor;
    private long seen = 0;
    private long total;
	
      public SequoiadbBlockReader(InputSplit inputSplit, Configuration conf){
    	if(inputSplit==null||!(inputSplit instanceof SdbBlockSplit)){
    		throw new IllegalArgumentException("the inputsplit is not SdbBlockSplit" );
    	}
    	String collectionName = SequoiadbConfigUtil.getInCollectionName(conf);
		String collectionSpaceName = SequoiadbConfigUtil.getInCollectionSpaceName(conf);
		String queryStr = SequoiadbConfigUtil.getQueryString(conf);
		String selectorStr = SequoiadbConfigUtil.getSelectorString(conf);
		
    	this.sdbBlockSplit=(SdbBlockSplit)inputSplit;
    	   	
    	if(sdbBlockSplit.getSdbAddr()==null){
    		throw new IllegalArgumentException(" the SdbBlockSplit.sdbaddr is null");
    	}
    	
    	this.sequoiadb = new Sequoiadb(this.sdbBlockSplit.getSdbAddr().getHost(), this.sdbBlockSplit.getSdbAddr().getPort(),null,null);

    	CollectionSpace collectionSpace=sequoiadb.getCollectionSpace(collectionSpaceName);
    	if(collectionSpace==null){
    		throw new IllegalArgumentException(" the CS not exists");
    	}
    	
    	DBCollection dbCollection=collectionSpace.getCollection(collectionName);
    	if(dbCollection==null){
    		throw new IllegalArgumentException(" the CL not exists");
    	}	  	
    	
    	BSONObject hint=new BasicBSONObject();
    	
    	BSONObject meta=new BasicBSONObject();
    	BasicBSONList blocks=new BasicBSONList();
    	blocks.add(this.sdbBlockSplit.getDataBlockId());
    	meta.put("Datablocks",blocks);
    	meta.put("ScanType",this.sdbBlockSplit.getScanType());
    	hint.put("$Meta", meta);
    	BSONObject queryBson = null;
    	BSONObject selectorBson = null;
    	BSONObject orderbyBson = null;

    	log.info("queryStr = " + queryStr);
    	log.info("selectorStr = " + selectorStr);
    	if ( queryStr != null && !queryStr.equalsIgnoreCase("null") ){
    		queryBson = (BSONObject) JSON.parse( queryStr );
    		log.info( "queryBson = " + queryBson.toString() );
    	}
    	if ( selectorStr != null && !selectorStr.equalsIgnoreCase("null") ){
    		
    		selectorBson = (BSONObject) JSON.parse( selectorStr );
    		selectorBson.put("_id", null);
    		log.info( "selectorBson = " + selectorBson.toString() );
    	}
    	this.cursor=dbCollection.query( queryBson, 
    			                        selectorBson,
    			                        orderbyBson,
    			                        hint,
    			                        0,
    			                        -1);	
    }
    
	@Override
	public Object getCurrentKey() throws IOException, InterruptedException {
		return this.current.get("_id");
	}

	@Override
	public BSONWritable getCurrentValue() throws IOException,
			InterruptedException {
		return new BSONWritable(this.current);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if(this.cursor.hasNext()){
			return 0f;
		}else{
			return 1f;
		}
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		log.debug("begin to read the inputsplit");
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
            if ( !this.cursor.hasNext() ){
            	log.debug("this inputsplit read over");
                return false;
            }
            this.current = this.cursor.getNext();
            this.seen++;
            return true;
	}

	@Override
	public void close() throws IOException {
		if(cursor!=null){
			cursor.close();
		}
		if(this.sequoiadb!=null){
			this.sequoiadb.disconnect();
		}
	}

}

