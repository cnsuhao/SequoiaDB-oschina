/**
 *      Copyright (C) 2012 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
/**
 * @package com.sequoiadb.base;
 * @brief SequoiaDB Driver for Java
 * @author Jacky Zhang
 */
package com.sequoiadb.base;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.sequoiadb.base.SequoiadbConstants.Operation;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.exception.SDBErrorLookup;
import com.sequoiadb.net.IConnection;
import com.sequoiadb.util.SDBMessageHelper;

/**
 * @class CollectionSpace
 * @brief Database operation interfaces of collection space.
 */
public class CollectionSpace {
	private String name;
	private Sequoiadb sequoiadb;

	/**
	 * @fn String getName()
	 * @brief Return the name of current collection space.
	 * @return The collection space name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @fn Sequoiadb getSequoiadb()
	 * @brief Return the Sequoiadb instance of current collection space belong to.
	 * @return Sequoiadb object
	 */
	public Sequoiadb getSequoiadb() {
		return sequoiadb;
	}

	/**
	 * @fn CollectionSpace(Sequoiadb sequoiadb, String name)
	 * @brief Constructor
	 * @param sequoiadb
	 *            Sequoiadb handle
	 * @param name
	 *            Collection space name
	 */
	CollectionSpace(Sequoiadb sequoiadb, String name) {
		this.name = name;
		this.sequoiadb = sequoiadb;
	}

	/**
	 * @fn DBCollection getCollection(String collectionName)
	 * @brief Get the named collection
	 * @param collectionName
	 *            The collection name
	 * @return The DBCollection handle or null for not exist
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCollection getCollection(String collectionName)
			throws BaseException {
		if (isCollectionExist(collectionName)) {
			return new DBCollection(sequoiadb, this, collectionName.trim());
		} else {
			return null;
		}
	}

	/**
	 * @fn boolean isCollectionExist(String colName)
	 * @brief Verify the existence of collection in current collection space
	 * @param colName
	 *            The collection name
	 * @return True if collection existed or False if not existed
	 * @throws com.sequoiadb.exception.BaseException 
	 */
	public boolean isCollectionExist(String colName) throws BaseException {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.TEST_CMD + " "
				+ SequoiadbConstants.COLLECTION;
		String collectionFullName = name + "." + colName;
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		SDBMessage rtn = adminCommand(commandString, obj, null, null, null);
		int flags = rtn.getFlags();
		if ( flags == 0 )
			return true;
		else if ( flags == new BaseException("SDB_DMS_NOTEXIST").getErrorCode())
			return false;
	    else
			throw new BaseException(flags);
	}

	/**
	 * @fn List<String> getCollectionNames()
	 * @brief Get all the collection names of current collection space
	 * @return A list of collection names or null
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public List<String> getCollectionNames() throws BaseException {
		List<String> colNames = sequoiadb.getCollectionNames();
		if (colNames == null || colNames.size() == 0)
			return null;
		List<String> collectionNames = new ArrayList<String>();
		for (String col : colNames) {
			if (col.startsWith(name + ".")) {
				collectionNames.add(col);
			}
		}
		return collectionNames;
	}

	/**
	 * @fn void createCollection(String collectionName)
	 * @brief Create the named collection in current collection space
	 * @param collectionName
	 *            The collection name
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCollection createCollection(String collectionName)
			throws BaseException {
		if (isCollectionExist(collectionName)) {
			throw new BaseException("SDB_DMS_EXIST", collectionName);
		}
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.CREATE_CMD + " "
				+ SequoiadbConstants.COLLECTION;
		String collectionFullName = name + "." + collectionName;
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		SDBMessage rtn = adminCommand(commandString, obj, null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, collectionName);
		}
		return getCollection(collectionName);
	}

	/**
	 * @fn DBCollection createCollection(String collectionName, BSONObject
	 *     options)
	 * @brief Create collection by options
	 * @param collectionName
	 *           The collection name
	 * @param options
	 *           The options for creating collection, including
     *           "ShardingKey", "ReplSize", "IsMainCL" and "Compressed" informations,
     *           no options, if null
	 * @return the created DBCollection
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCollection createCollection(String collectionName,
			BSONObject options) {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.CREATE_CMD + " "
				+ SequoiadbConstants.COLLECTION;
		String collectionFullName = name + "." + collectionName;
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		if (options != null)
			obj.putAll(options);
		SDBMessage rtn = adminCommand(commandString, obj, null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, collectionFullName, options);
		}
		return getCollection(collectionName);
	}

	/**
	 * @fn void drop()
	 * @brief Drop current collectionSpace
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 * @deprecated the method will be deprecated in version 2.x, use dropCollectionSpace instead
	 * @see com.sequoiadb.base.Sequoiadb.dropCollectionSpace
	 */
	public void drop() throws BaseException {
		sequoiadb.dropCollectionSpace(this.name);
	}

	/**
	 * @fn void dropCollection(String collectionName)
	 * @brief Remove the named collection of current collection space
	 * @param collectionName
	 *            The collection name
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void dropCollection(String collectionName) throws BaseException {
		if (!isCollectionExist(collectionName)) {
			throw new BaseException("SDB_DMS_NOTEXIST", collectionName);
		}
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.DROP_CMD + " "
				+ SequoiadbConstants.COLLECTION;
		String collectionFullName = name + "." + collectionName;
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		SDBMessage rtn = adminCommand(commandString, obj, null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, collectionName);
		}
	}

	private SDBMessage adminCommand(String commandString, BSONObject arg1,
			BSONObject arg2, BSONObject arg3, BSONObject arg4)
			throws BaseException {
		IConnection connection = sequoiadb.getConnection();
		BSONObject dummyObj = new BasicBSONObject();
		SDBMessage sdbMessage = new SDBMessage();
		sdbMessage.setMatcher(arg1);
		sdbMessage.setCollectionFullName(commandString);
		sdbMessage.setFlags(0);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		sdbMessage.setRequestID(sequoiadb.getNextRequstID());
		sdbMessage.setSkipRowsCount(-1);
		sdbMessage.setReturnRowsCount(-1);
		sdbMessage.setSelector(dummyObj);
		sdbMessage.setOrderBy(dummyObj);
		sdbMessage.setHint(dummyObj);
		sdbMessage.setOperationCode(Operation.OP_QUERY);

		byte[] request = SDBMessageHelper.buildQueryRequest(sdbMessage, sequoiadb.endianConvert);
		connection.sendMessage(request);
		
		ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
		
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		SDBMessageHelper.checkMessage(sdbMessage, rtnSDBMessage);
		return rtnSDBMessage;
	}
}
