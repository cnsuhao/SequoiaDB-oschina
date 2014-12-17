package com.sequoiadb.hadoop.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.hadoop.util.SdbConnAddr;
import com.sequoiadb.hadoop.util.SequoiadbConfigUtil;

/**
 * 
 * 
 * @className：SdbSplit
 * 
 * @author： gaoshengjie
 * 
 * @createtime:2013年12月11日 上午10:46:36
 * 
 * @changetime:TODO
 * 
 * @version 1.0.0
 * 
 */
public class SdbSplitFactory {
	private static final Log log = LogFactory.getLog(SdbSplitFactory.class);

	/**
	 * 
	 * @content:depend the method of scan ,return different split;
	 * 
	 * @param jobContext
	 * @return
	 * 
	 * @exception
	 * @since 1.0.0
	 */
	public static List<InputSplit> getSplits(JobContext jobContext) {
		System.out.println(jobContext.getClass().getName());

		Configuration conf = jobContext.getConfiguration();

		String urls = SequoiadbConfigUtil.getInputURL(conf);
		System.out.println(urls);
		SdbConnAddr[] sdbConnAddrs = SequoiadbConfigUtil.getAddrList(urls);

		String collectionName = SequoiadbConfigUtil.getInCollectionName(conf);
		String collectionSpaceName = SequoiadbConfigUtil.getInCollectionSpaceName(conf);
		
		log.debug("test sdbConnAddrs whether it can connect or not ");
		Sequoiadb sdb = null;
		BaseException lastException = null;
		boolean flag = false;
		for (int i = 0; i < sdbConnAddrs.length; i++) {
			try {
				sdb = new Sequoiadb(sdbConnAddrs[i].getHost(),
						sdbConnAddrs[i].getPort(), null, null);
				sdb.setSessionAttr(new BasicBSONObject("PreferedInstanc","S"));
				break;
			} catch (BaseException e) {
				lastException = e;
			}
		}
		if (sdb == null) {
			throw lastException;
		}

		log.debug("start get data blocks");

		if (collectionSpaceName == null || collectionName == null) {
			throw new IllegalArgumentException(
					"collectionSpaceName and collectionName must have value");
		}
		DBCollection collection = null;
		try {
			collection = sdb.getCollectionSpace(collectionSpaceName)
					.getCollection(collectionName);
		} catch (Exception e) {
			throw new Error("can't get DBCollection ,may be something wrong ");
		}

		DBCursor cursor = collection.getQueryMeta(null, null, null, 0, -1, 0);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		while (cursor.hasNext()) {
			BSONObject obj = cursor.getNext();
			log.info(" record:" + obj.toString());

			String hostname = (String) obj.get("HostName");
			int port = Integer.parseInt((String) obj.get("ServiceName"));
			String scanType = (String) obj.get("ScanType");

			if ("ixscan".equals(scanType)) {
				String indexName = (String) obj.get("IndexName");
				int indexLID = (Integer) obj.get("IndexLID");
				int direction = (Integer) obj.get("Direction");

				BasicBSONList indexBlockList = (BasicBSONList) obj
						.get("Indexblocks");

				for (Object objBlock : indexBlockList) {
					if (objBlock instanceof BSONObject) {
						BSONObject indexBlock = (BSONObject) objBlock;
						BSONObject startKey = (BSONObject) indexBlock
								.get("StartKey");
						BSONObject endKey = (BSONObject) indexBlock
								.get("EndKey");
					}
				}

			}

			if ("tbscan".equals(scanType)) {
				BasicBSONList blockList = (BasicBSONList) obj.get("Datablocks");
				int i = 0;
				for (Object objBlock : blockList) {
					if (objBlock instanceof Integer) {
						Integer blockId = (Integer) objBlock;
						splits.add(new SdbBlockSplit(new SdbConnAddr(hostname,
								port), scanType, blockId));
					}
				}
			}

		}
		return splits;
	}

}
