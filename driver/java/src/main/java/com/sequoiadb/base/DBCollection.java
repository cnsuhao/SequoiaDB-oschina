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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.mina.core.buffer.IoBuffer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.bson.util.JSON;

import com.sequoiadb.base.SequoiadbConstants.Operation;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.net.IConnection;
import com.sequoiadb.util.Helper;
import com.sequoiadb.util.SDBMessageHelper;

/**
 * @class DBCollection
 * @brief Database operation interfaces of collection.
 */
public class DBCollection {
	private String name;
	private Sequoiadb sequoiadb;
	private CollectionSpace collectionSpace;
	private IConnection connection;
	private String csName;
	private String collectionFullName;
	private Set<String> mainKeys;
    private boolean ensureOID;

	private IoBuffer insert_buffer;
	private static final int DEF_BUFFER_LENGTH = 64*1024;
	private static final int DEF_BULK_BUFFER_LENGTH = 2*1024*1024; 
	
	/**
	 * @memberof FLG_INSERT_CONTONDUP 0x00000001
	 * @brief this flags represent that bulkInsert will continue when
	 *        Duplicate key exist.(the duplicate record will be ignored)
	 */
	public final static int FLG_INSERT_CONTONDUP = 0x00000001;
	
	public IConnection getConnection() {
		return connection;
	}
	
	public void setConnection(IConnection connection) {
		this.connection = connection;
	}

	/**
	 * @fn String getName()
	 * @brief Return the name of current collection
	 * @return The collection name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @fn String getFullName()
	 * @brief Get the full name of specified collection in current collection
	 *        space
	 * @return The full name of specified collection
	 */
	public String getFullName() {
		return collectionFullName;
	}

	/**
	 * @fn String getCSName()
	 * @brief Get the full name of specified collection in current collection
	 *        space
	 * @return The full name of specified collection
	 */
	public String getCSName() {
		return csName;
	}

	/**
	 * @fn Sequoiadb getSequoiadb()
	 * @brief Return the Sequoiadb handle of current collection
	 * @return Sequoiadb object
	 */
	public Sequoiadb getSequoiadb() {
		return sequoiadb;
	}

	/**
	 * @fn CollectionSpace getCollectionSpace()
	 * @brief Return the Collection Space handle of current collection
	 * @return CollectionSpace object
	 */
	public CollectionSpace getCollectionSpace() {
		return collectionSpace;
	}

	/**
	 * @fn void setMainKeys(String[] keys)
	 * @brief Set the main keys used in save(). if no main keys are set, use the
	 * 		  default main key "_id". 
	 * @param keys
	 * 		  the main keys specified by user.
	 * @exception com.sequoiadb.Exception.BaseException when keys is null
	 * @note 
	 *        every time invokes the method,
	 *        it will remove the main keys set in last time 
	 */
	public void setMainKeys(String[] keys) throws BaseException {
		if (keys == null)
			throw new BaseException("SDB_INVALIDARG", (Object[])keys);
		mainKeys.clear();
		if (keys.length == 0)
			return;
		for(String k : keys)
			mainKeys.add(k);
	}
	
	/**
	 * @fn DBCollection(Sequoiadb sequoiadb, CollectionSpace cs, String name)
	 * @brief Constructor
	 * @param sequoiadb
	 *            Sequoiadb object
	 * @param cs
	 *            CollectionSpace object
	 * @param name
	 *            Collection name
	 */
	DBCollection(Sequoiadb sequoiadb, CollectionSpace cs, String name) {
		this.name = name;
		this.sequoiadb = sequoiadb;
		this.collectionSpace = cs;
		this.csName = cs.getName();
		this.collectionFullName = csName + "." + name;
		this.connection = sequoiadb.getConnection();
		this.insert_buffer = null;
		this.mainKeys = new HashSet<String>();
        this.ensureOID=true;
	}

	/**
	 * @fn Object insert(BSONObject insertor)
	 * @brief Insert a document into current collection, if the document
	 *        does not contain field "_id", it will be added.
	 * @param insertor
	 *            The Bson object of insertor, can't be null
	 * @return Object the value of the filed "_id"
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Object insert(BSONObject insertor) throws BaseException {
		if (insertor == null)
			throw new BaseException("SDB_INVALIDARG");
		
		if (this.insert_buffer == null) {
			this.insert_buffer = IoBuffer.allocate(DEF_BUFFER_LENGTH);
			this.insert_buffer.setAutoExpand(true);
			if (sequoiadb.endianConvert) {
				insert_buffer.order(ByteOrder.LITTLE_ENDIAN);
			} else {
				insert_buffer.order(ByteOrder.BIG_ENDIAN);
			}
		} else {
			insert_buffer.clear();
		}
		
		Object tmp = insertor.get(SequoiadbConstants.OID);
		if (tmp == null) {
			ObjectId objId = ObjectId.get();
			insertor.put(SequoiadbConstants.OID, objId);
			tmp = objId;
		}
		
		int message_length = SDBMessageHelper.buildInsertRequest(insert_buffer, 
		        sequoiadb.getNextRequstID(), collectionFullName, insertor);

		connection.sendMessage(insert_buffer.array(), message_length);
		
		ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		if ( rtnSDBMessage.getOperationCode() != Operation.OP_INSERT_RES) {
		    throw new BaseException("SDB_UNKNOWN_MESSAGE", 
		            rtnSDBMessage.getOperationCode());
		}
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, insertor.toString());
		}
		return tmp;
	}

	/**
	 * @fn Object insert(String insertor)
	 * @brief Insert a document into current collection, if the document
	 *        does not contain field "_id", it will be added.
	 * @param insertor
	 *            The string of insertor
	 * @return Object the value of the filed "_id"
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Object insert(String insertor) throws BaseException {
		BSONObject in = null;
		if (insertor != null)
			in = (BSONObject) JSON.parse(insertor);
		return insert(in);
	}

	/**
     * @fn <T> void save(T type, Boolean ignoreNullValue)
     * @brief Insert an object into current collection
     * @param type
     *            The object of insertor, can't be null
     * @param ignoreNullValue
     *            true:if type's inner value is null, it will not save to collection;
     *            false:if type's inner value is null, it will save to collection too.
     * @exception com.sequoiadb.exception.BaseException 
     *            1.when the type is not support, throw BaseException with the type "SDB_INVALIDARG"
     *            2.when offer main keys by setMainKeys(), and try to update "_id" field,
     *              it may get a BaseException with the type of "SDB_IXM_DUP_KEY" 
     * @note when save include update shardingKey field, the shardingKey modify action is not take effect, but the other
     *       field update is take effect. Because of current version is not support update shardingKey field.
     * @see com.sequoiadb.base.DBCollection.setMainKeys
     */
    public <T> void save(T type, Boolean ignoreNullValue) throws BaseException
    {
        BSONObject obj;
        try
        {
            obj = BasicBSONObject.typeToBson(type, ignoreNullValue);
        }
        catch (Exception e)
        {
            throw new BaseException("SDB_INVALIDARG", type, e);
        }
        BSONObject matcher = new BasicBSONObject();
        BSONObject modifer = new BasicBSONObject(); 
        if (mainKeys.isEmpty())
        {
            Object id = obj.get(SequoiadbConstants.OID);
            if (id == null || (id instanceof ObjectId && ((ObjectId) id).isNew()))
            {
                if (id != null && id instanceof ObjectId)
                    ((ObjectId) id).notNew();
                insert(obj);
            }
            else
            {
                matcher.put(SequoiadbConstants.OID, id);
                modifer.put("$set", obj);
                upsert(matcher, modifer, null);
            }
        }
        else
        { // if user specify main keys, use these main keys
            Iterator<String> it = mainKeys.iterator();
            while (it.hasNext())
            {
                String key = it.next();
                if (obj.containsField(key))
                    matcher.put(key, obj.get(key));
                else
                    matcher.put(key, null);
            }
                modifer.put("$set", obj);
                upsert(matcher, modifer, null);
        }
    }
	/**
	 * @fn <T> void save(T type)
	 * @brief Insert an object into current collection
	 * @param type
	 *            The object of insertor, can't be null
	 * @exception com.sequoiadb.exception.BaseException 
	 * 			  1.when the type is not support, throw BaseException with the type "SDB_INVALIDARG"
	 *            2.when offer main keys by setMainKeys(), and try to update "_id" field,
	 *              it may get a BaseException with the type of "SDB_IXM_DUP_KEY" 
	 * @note when save include update shardingKey field, the shardingKey modify action is not take effect, but the other
	 *       field update is take effect. Because of current version is not support update shardingKey field.
	 * @see com.sequoiadb.base.DBCollection.setMainKeys
	 */
	public <T> void save(T type) throws BaseException
	{
		save(type, false);
	}
	
	/**
     * @fn <T> void save(List<T> type, Boolean ignoreNullValue)
     * @brief Insert an object into current collection
     * @param type
     *            The List instance of insertor, can't be null or empty
     * @param ignoreNullValue
     *            true:if type's inner value is null, it will not save to collection;
     *            false:if type's inner value is null, it will save to collection too.
     * @exception com.sequoiadb.exception.BaseException 
     *            1.while the input argument is null or the List instance is empty
     *            2.while the type is not support, throw BaseException with the type "SDB_INVALIDARG"
     *            3.while offer main keys by setMainKeys(), and try to update "_id" field,
     *              it may get a BaseException with the type of "SDB_IXM_DUP_KEY" when the "_id" field you
     *              want to update to had been existing in database 
     * @note when save include update shardingKey field, the shardingKey modify action is not take effect, but the other
     *       field update is take effect. Because of current version is not support update shardingKey field.
     * @see com.sequoiadb.base.DBCollection.setMainKeys
     */
    public <T> void save(List<T> type, Boolean ignoreNullValue) throws BaseException {
        if (type == null || type.size() == 0)
            throw new BaseException("SDB_INVALIDARG", type);
        List<BSONObject> objs = new ArrayList<BSONObject>();
        try {
            Iterator<T> it = type.iterator(); 
            while (it != null && it.hasNext()) {
                objs.add(BasicBSONObject.typeToBson(it.next(), ignoreNullValue));
            }
        } catch (Exception e) {
            throw new BaseException("SDB_INVALIDARG", type, e);
        }
        BSONObject matcher = new BasicBSONObject();
        BSONObject modifer = new BasicBSONObject();
        BSONObject obj = null;
        Iterator<BSONObject> ite = objs.iterator();
        if (mainKeys.isEmpty()) {
            while(ite != null && ite.hasNext()) {
                obj = ite.next();
                Object id = obj.get(SequoiadbConstants.OID);
                if (id == null || (id instanceof ObjectId && ((ObjectId) id).isNew())) {
                    if (id != null && id instanceof ObjectId)
                        ((ObjectId) id).notNew();
                    insert(obj);
                } else {
                    matcher.put(SequoiadbConstants.OID, id);
                    modifer.put("$set", obj);
                    upsert(matcher, modifer, null);
                }
            }
        } else { // if user specify main keys, use these main keys
            while (ite != null && ite.hasNext()) {
                obj = ite.next();
                Iterator<String> i = mainKeys.iterator();
                while (i.hasNext()) {
                    String key = i.next();
                    if (obj.containsField(key))
                        matcher.put(key, obj.get(key));
                    else
                        matcher.put(key, null);
                }
                modifer.put("$set", obj);
                upsert(matcher, modifer, null);
            }
        }
    }
	
	/**
	 * @fn <T> void save(List<T> type)
	 * @brief Insert an object into current collection
	 * @param type
	 *            The List instance of insertor, can't be null or empty
	 * @exception com.sequoiadb.exception.BaseException 
	 *            1.while the input argument is null or the List instance is empty
	 *            2.while the type is not support, throw BaseException with the type "SDB_INVALIDARG"
	 *            3.while offer main keys by setMainKeys(), and try to update "_id" field,
	 *              it may get a BaseException with the type of "SDB_IXM_DUP_KEY" when the "_id" field you
	 *              want to update to had been existing in database 
	 * @note when save include update shardingKey field, the shardingKey modify action is not take effect, but the other
	 *       field update is take effect. Because of current version is not support update shardingKey field.
	 * @see com.sequoiadb.base.DBCollection.setMainKeys
	 */
	public <T> void save(List<T> type) throws BaseException {
		save(type, false);
	}
	
	public void ensureOID(boolean flag) {
		ensureOID=flag;
	}
	
	public boolean isOIDEnsured() {
		return ensureOID;
	}
	
	/**
	 * @fn void bulkInsert(List<BSONObject> insertor, int flag)
	 * @brief Insert a bulk of bson objects into current collection
	 * @param insertor
	 *            The Bson object of insertor list, can't be null
	 * @param flag  
	 *            available value is FLG_INSERT_CONTONDUP or 0.  
	 *            if flag = FLG_INSERT_CONTONDUP, bulkInsert will continue when Duplicate 
	 *            key exist.(the duplicate record will be ignored); 
     *            if flag = 0, bulkInsert will interrupt when Duplicate key exist.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void bulkInsert(List<BSONObject> insertor, int flag)
			throws BaseException {

		if (flag != 0 && flag != FLG_INSERT_CONTONDUP)
			throw new BaseException("SDB_INVALIDARG");
		if (insertor == null || insertor.size() == 0)
			throw new BaseException("SDB_INVALIDARG");

		if (this.insert_buffer == null) {
			this.insert_buffer = IoBuffer.allocate(DEF_BULK_BUFFER_LENGTH);
			this.insert_buffer.setAutoExpand(true);
			if (sequoiadb.endianConvert) {
				insert_buffer.order(ByteOrder.LITTLE_ENDIAN);
			} else {
				insert_buffer.order(ByteOrder.BIG_ENDIAN);
			}
		} else {
			insert_buffer.clear();
		}

		int messageLength = SDBMessageHelper.buildBulkInsertRequest(
				insert_buffer, sequoiadb.getNextRequstID(), collectionFullName, 
				insertor, flag, this.ensureOID);

		connection.sendMessage(insert_buffer.array(), messageLength);
		
		ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		if ( rtnSDBMessage.getOperationCode() != Operation.OP_INSERT_RES) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", 
                    rtnSDBMessage.getOperationCode());
        }
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0)
			throw new BaseException(flags, insertor);
		/*
		int capacity = insert_buffer.capacity();
		if(capacity >= 8 * DEF_BULK_BUFFER_LENGTH) {
			insert_buffer.position(0);
			insert_buffer.limit(capacity/2);
		}
		else {
			insert_buffer.position(0);
			insert_buffer.limit(DEF_BULK_BUFFER_LENGTH);
		}
		insert_buffer.shrink();
		*/
	}

	/**
	 * @fn void delete(BSONObject matcher)
	 * @brief Delete the matching BSONObject of current collection
	 * @param matcher
	 *            The matching condition
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void delete(BSONObject matcher) throws BaseException {
		delete(matcher, null);
	}

	/**
	 * @fn void delete(String matcher)
	 * @brief Delete the matching of current collection
	 * @param matcher
	 *            The matching condition
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void delete(String matcher) throws BaseException {
		BSONObject ma = null;
		if (matcher != null)
			ma = (BSONObject) JSON.parse(matcher);
		delete(ma, null);
	}

	/**
	 * @fn void delete(String matcher, String hint)
	 * @brief Delete the matching bson's string of current collection
	 * @param matcher
	 *            The matching condition
	 * @param hint
	 *            Hint
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void delete(String matcher, String hint) throws BaseException {
		BSONObject ma = null;
		BSONObject hi = null;
		if (matcher != null)
			ma = (BSONObject) JSON.parse(matcher);
		if (hint != null)
			hi = (BSONObject) JSON.parse(hint);
		delete(ma, hi);
	}

	/**
	 * @fn void delete(BSONObject matcher, BSONObject hint)
	 * @brief Delete the matching BSONObject of current collection
	 * @param matcher
	 *            The matching condition
	 * @param hint
	 *            Hint
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void delete(BSONObject matcher, BSONObject hint)
			throws BaseException {
		BSONObject dummy = new BasicBSONObject();
		if (matcher == null)
			matcher = dummy;
		if (hint == null)
			hint = dummy;
		SDBMessage sdbMessage = new SDBMessage();

		sdbMessage.setVersion(1);
		sdbMessage.setW((short) 0);
		sdbMessage.setPadding((short) 0);
		sdbMessage.setFlags(0);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		sdbMessage.setCollectionFullName(collectionFullName);
		sdbMessage.setRequestID(sequoiadb.getNextRequstID());
		sdbMessage.setMatcher(matcher);
		sdbMessage.setHint(hint);
		sdbMessage.setOperationCode(Operation.OP_DELETE);

		byte[] request = SDBMessageHelper.buildDeleteRequest(sdbMessage,
				sequoiadb.endianConvert);
		connection.sendMessage(request);
		
		ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		SDBMessageHelper.checkMessage(sdbMessage, rtnSDBMessage);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, matcher, hint);
		}
	}

	/**
	 * @fn void update(DBQuery query)
	 * @brief Update the document of current collection
	 * @param query
	 *            DBQuery with matching condition, updating rule and hint
	 * @exception com.sequoiadb.exception.BaseException
	 * @note when save include update shardingKey field, the shardingKey modify action is not take effect, but the other
	 *       field update is take effect.
	 *       Because of current version is not support update shardingKey field.
	 */
	public void update(DBQuery query) throws BaseException {
		_update(0, query.getMatcher(), query.getModifier(), query.getHint());
	}

	/**
	 * @fn void update(BSONObject matcher, BSONObject modifier, BSONObject hint)
	 * @brief Update the BSONObject of current collection
	 * @param matcher
	 *            The matching condition
	 * @param modifier
	 *            The updating rule
	 * @param hint
	 *            Hint
	 * @exception com.sequoiadb.exception.BaseException
	 * @note when save include update shardingKey field, the shardingKey modify action is not take effect, but the other
	 *       field update is take effect.
	 *       Because of current version is not support update shardingKey field.
	 */
	public void update(BSONObject matcher, BSONObject modifier, BSONObject hint)
			throws BaseException {
		_update(0, matcher, modifier, hint);
	}

	/**
	 * @fn void update(String matcher, String modifier, String hint)
	 * @brief Update the BSONObject of current collection
	 * @param matcher
	 *            The matching condition
	 * @param modifier
	 *            The updating rule
	 * @param hint
	 *            Hint
	 * @exception com.sequoiadb.exception.BaseException
	 * @note when save include update shardingKey field, the shardingKey modify action is not take effect, but the other
	 *       field update is take effect.
	 *       Because of current version is not support update shardingKey field.
	 */
	public void update(String matcher, String modifier, String hint)
			throws BaseException {
		BSONObject ma = null;
		BSONObject mo = null;
		BSONObject hi = null;
		if (matcher != null)
			ma = (BSONObject) JSON.parse(matcher);
		if (modifier != null)
			mo = (BSONObject) JSON.parse(modifier);
		if (hint != null)
			hi = (BSONObject) JSON.parse(hint);
		_update(0, ma, mo, hi);
	}

	/**
	 * @fn void upsert(BSONObject matcher, BSONObject modifier, BSONObject hint)
	 * @brief Update the BSONObject of current collection, insert if no matching
	 * @param matcher
	 *            The matching condition
	 * @param modifier
	 *            The updating rule
	 * @param hint
	 *            Hint
	 * @exception com.sequoiadb.exception.BaseException
	 * @note when save include update shardingKey field, the shardingKey modify action is not take effect, but the other
	 *       field update is take effect.
	 *       Because of current version is not support update shardingKey field.
	 */
	public void upsert(BSONObject matcher, BSONObject modifier, BSONObject hint)
			throws BaseException {
		_update(SequoiadbConstants.FLG_UPDATE_UPSERT, matcher, modifier, hint);
	}
	
	/**
     * @fn DBCursor explain(BSONObject matcher, BSONObject selector,
            BSONObject orderBy, BSONObject hint, long skipRows, long returnRows,
            int flag, BSONObject options)
     * @brief Get explain of current collection.
     * @param matcher 
     *            the matching rule, return all the documents if null
     * @param selector
     *            the selective rule, return the whole document if null
     * @param orderBy
     *            the ordered rule, never sort if null
     * @param hint
     *            the hint, automatically match the optimal hint if null
     * @param skipRows
     *            skip the first numToSkip documents, never skip if this parameter is 0
     * @param returnRows
     *            only return returnRows documents, return all if this parameter is -1
     * @param flag the flag is used to choose the way to query, the optional options are as below:
     *<ul>
     * <li>DBQuery.FLG_QUERY_STRINGOUT
     * <li>DBQuery.FLG_QUERY_FORCE_HINT 
     * <li>DBQuery.FLG_QUERY_PARALLED
     * <li>DBQuery.FLG_QUERY_WITH_RETURNDATA
     * <li>DBQuery.FLG_QUERY_EXPLAIN
     *</ul>
     * @param options The rules of query explain, the options are as below:
     *<ul>
     *<li>Run     : Whether execute query explain or not, true for excuting query explain then get
     *              the data and time information; false for not excuting query explain but get the
     *              query explain information only. e.g. {Run:true}
     *</ul>
     * @return a DBCursor instance of the result
     * @exception com.sequoiadb.exception.BaseException
     */
    public DBCursor explain(BSONObject matcher, BSONObject selector,
            BSONObject orderBy, BSONObject hint, long skipRows, long returnRows,
            int flag, BSONObject options) throws BaseException {
        
        flag |= DBQuery.FLG_QUERY_EXPLAIN;
        BSONObject innerHint = new BasicBSONObject();
        if ( null != hint ){
            innerHint.put(SequoiadbConstants.FIELD_NAME_HINT, hint);
        }
        
        if ( null != options){
            innerHint.put(SequoiadbConstants.FIELD_NAME_OPTIONS, options);
        }
        
        return query(matcher, selector, orderBy, innerHint, skipRows, 
                returnRows, flag);
    }

	/**
	 * @fn DBCursor query()
	 * @brief Get all documents of current collection.
	 * @return a DBCursor instance of the result
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor query() throws BaseException {
		return query("", "", "", "", 0, -1);
	}

	/**
	 * @fn DBCursor query(DBQuery matcher)
	 * @brief Get the matching documents in current collection. 
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @return a DBCursor instance of the result or null if no any matched document
	 * @exception com.sequoiadb.exception.BaseException
	 * @see com.sequoiadb.base.DBQuery
	 */
	public DBCursor query(DBQuery matcher) throws BaseException {
		if (matcher == null)
			return query();
		return query(matcher.getMatcher(), matcher.getSelector(),
				matcher.getOrderBy(), matcher.getHint(), matcher.getSkipRowsCount(),
				matcher.getReturnRowsCount(), matcher.getFlag());
	}

	/**
	 * @fn DBCursor query(BSONObject matcher, BSONObject selector, BSONObject
	 *     orderBy, BSONObject hint)
	 * @brief Get the matching documents in current collection. 
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @param hint
	 *            the hint, automatically match the optimal hint if null
	 * @return a DBCursor instance of the result or null if no any matched document
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor query(BSONObject matcher, BSONObject selector,
			BSONObject orderBy, BSONObject hint) throws BaseException {
		return query(matcher, selector, orderBy, hint, 0, -1, 0);
	}

	/**
	 * @fn DBCursor query(BSONObject matcher, BSONObject selector, BSONObject
	 *     orderBy, BSONObject hint, int flag)
	 * @brief Get the matching documents in current collection. 
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @param hint
	 *            the hint, automatically match the optimal hint if null
	 * @param flag 
	 *            the flag is used to choose the way to query, the optional options are as below:  
     * <ul>
     * <li>BDQuery.FLG_QUERY_STRINGOUT
     * <li>BDQuery.FLG_QUERY_FORCE_HINT 
     * <li>BDQuery.FLG_QUERY_PARALLED
     * </ul>  
	 * @return a DBCursor instance of the result or null if no any matched document
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor query(BSONObject matcher, BSONObject selector,
			BSONObject orderBy, BSONObject hint, int flag) throws BaseException {
		return query(matcher, selector, orderBy, hint, 0, -1, flag);
	}
	
	/**
	 * @fn DBCursor query(String matcher, String selector, String orderBy, String
	 *     hint)
	 * @brief Get the matching documents in current collection. 
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @param hint
	 *            the hint, automatically match the optimal hint if null
	 * @return a DBCursor instance of the result or null if no any matched document
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor query(String matcher, String selector, String orderBy,
			String hint) throws BaseException {
		return query(matcher, selector, orderBy, hint, 0);
	}
	/**
	 * @fn DBCursor query(String matcher, String selector, String orderBy, String
	 *     hint, int flag)
	 * @brief Get the matching documents in current collection. 
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @param hint
	 *            the hint, automatically match the optimal hint if null
	 * @param flag 
	 *            the flag is used to choose the way to query, the optional options are as below:  
     * <ul>
     * <li>BDQuery.FLG_QUERY_STRINGOUT
     * <li>BDQuery.FLG_QUERY_FORCE_HINT 
     * <li>BDQuery.FLG_QUERY_PARALLED
     * </ul>  
	 * @return a DBCursor instance of the result or null if no any matched document
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor query(String matcher, String selector, String orderBy,
			String hint, int flag) throws BaseException {
		BSONObject ma = null;
		BSONObject se = null;
		BSONObject or = null;
		BSONObject hi = null;
		if (matcher != null)
			ma = (BSONObject) JSON.parse(matcher);
		if (selector != null)
			se = (BSONObject) JSON.parse(selector);
		if (orderBy != null && !orderBy.equals(""))
			or = (BSONObject) JSON.parse(orderBy);
		if (hint != null)
			hi = (BSONObject) JSON.parse(hint);
		return query(ma, se, or, hi, 0, -1, flag);
	}

	/**
	 * @fn DBCursor query(String matcher, String selector, String orderBy,
			String hint, long skipRows, long returnRows)
	 * @brief Get the matching documents in current collection. 
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @param hint
	 *            the hint, automatically match the optimal hint if null
	 * @param skipRows
	 *            skip the first numToSkip documents, never skip if this parameter is 0
	 * @param returnRows
	 *            only return returnRows documents, return all if this parameter is -1
	 * @return a DBCursor instance of the result or null if no any matched document
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor query(String matcher, String selector, String orderBy,
			String hint, long skipRows, long returnRows) throws BaseException {
		BSONObject ma = null;
		BSONObject se = null;
		BSONObject or = null;
		BSONObject hi = null;
		if (matcher != null)
			ma = (BSONObject) JSON.parse(matcher);
		if (selector != null)
			se = (BSONObject) JSON.parse(selector);
		if (orderBy != null)
			or = (BSONObject) JSON.parse(orderBy);
		if (hint != null)
			hi = (BSONObject) JSON.parse(hint);
		return query(ma, se, or, hi, skipRows, returnRows, 0);
	}
	
	
	/**
	 * @fn DBCursor query(BSONObject matcher, BSONObject selector, BSONObject
	 *     orderBy, BSONObject hint, long skipRows, long returnRows)
	 * @brief Get the matching documents in current collection. 
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @param hint
	 *            the hint, automatically match the optimal hint if null
	 * @param skipRows
	 *            skip the first numToSkip documents, never skip if this parameter is 0
	 * @param returnRows
	 *            only return returnRows documents, return all if this parameter is -1
	 * @return a DBCursor instance of the result or null if no any matched document
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor query(BSONObject matcher, BSONObject selector,
			BSONObject orderBy, BSONObject hint, long skipRows, long returnRows) throws BaseException {
		return query(matcher, selector, orderBy, hint, skipRows, returnRows, 0);
	}

	/**
	 * @fn DBCursor query(BSONObject matcher, BSONObject selector,
	 *		              BSONObject orderBy, BSONObject hint,
	 *		              long skipRows, long returnRows,
	 *		              int flag)
	 * @brief Get the matching documents in current collection. 
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @param hint
	 *            the hint, automatically match the optimal hint if null
	 * @param skipRows
	 *            skip the first numToSkip documents, never skip if this parameter is 0
	 * @param returnRows
	 *            only return returnRows documents, return all if this parameter is -1
	 * @param flag the flag is used to choose the way to query, the optional options are as below:  
     * <ul>
     * <li>DBQuery.FLG_QUERY_STRINGOUT
     * <li>DBQuery.FLG_QUERY_FORCE_HINT 
     * <li>DBQuery.FLG_QUERY_PARALLED
     * <li>DBQuery.FLG_QUERY_WITH_RETURNDATA
     * <li>DBQuery.FLG_QUERY_EXPLAIN
     * </ul>  
	 * @return a DBCursor instance of the result or null if no any matched document
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor query(BSONObject matcher, BSONObject selector,
			              BSONObject orderBy, BSONObject hint,
			              long skipRows, long returnRows,
			              int flag) throws BaseException {
		BSONObject dummy = new BasicBSONObject();
		if (matcher == null)
			matcher = dummy;
		if (selector == null)
			selector = dummy;
		if (orderBy == null)
			orderBy = dummy;
		if (hint == null)
			hint = dummy;
		if (returnRows == 0)
			returnRows = -1;
		if ( returnRows == 1) {
		    flag = flag | DBQuery.FLG_QUERY_WITH_RETURNDATA;
		}
		SDBMessage rtnSDBMessage = adminCommand(collectionFullName, matcher,
				selector, orderBy, hint, skipRows, returnRows, flag);
		DBCursor cursor = null;
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC) {
				return null;
			} else {
				throw new BaseException(flags, matcher, selector, orderBy, hint,
						skipRows, returnRows);
			}
		}
		cursor = new DBCursor(rtnSDBMessage, this);
		return cursor;
	}

	/**
	 * @fn BSONObject queryOne(BSONObject matcher, BSONObject selector, BSONObject
	 *     orderBy, BSONObject hint, int flag)
	 * @brief Returns one matched document from current collection.
     * @param matcher 
     *            the matching rule, return all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @param hint
	 *            the hint, automatically match the optimal hint if null
	 * @param flag the flag is used to choose the way to query, the optional options are as below:  
     * <ul>
     * <li>BDQuery.FLG_QUERY_STRINGOUT
     * <li>BDQuery.FLG_QUERY_FORCE_HINT 
     * <li>BDQuery.FLG_QUERY_PARALLED
     * </ul>
	 * @return the matched document or null if no such document
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public BSONObject queryOne(BSONObject matcher, BSONObject selector,
			                   BSONObject orderBy, BSONObject hint,
			                   int flag) throws BaseException {
	    flag = flag | DBQuery.FLG_QUERY_WITH_RETURNDATA;
		DBCursor cursor = null;
		try {
			cursor = query(matcher, selector, orderBy, hint, 0, 1, flag);
		} catch (BaseException e) {
			throw e;
		}
		return cursor.getNext();
	}
	
	/**
	 * @fn BSONObject queryOne()
	 * @brief Returns one document from current collection.
	 * @return the document or null if no any document in current collection
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public BSONObject queryOne() throws BaseException {
		BSONObject empty = new BasicBSONObject();
		return queryOne(empty, empty, empty, empty, 0);
	}
	
	/**
	 * @fn DBCursor getIndexes()
	 * @brief Get all the indexes of current collection
	 * @return DBCursor of indexes
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor getIndexes() throws BaseException {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.GET_INXES;
		BSONObject dummyObj = new BasicBSONObject();
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);

		SDBMessage rtn = adminCommand(commandString, dummyObj, dummyObj,
				dummyObj, obj, -1, -1, 0);
		int flags = rtn.getFlags();
		DBCursor cursor = null;
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC) {
				return cursor;
			} else {
				throw new BaseException(flags);
			}
		}
		cursor = new DBCursor(rtn, sequoiadb);
		return cursor;
	}

	/**
	 * @fn DBCursor getIndex(String name)
	 * @brief Get all of or one of the indexes in current collection
	 * @param name
	 *            The index name, returns all of the indexes if this parameter
	 *            is null
	 * @return dbCursor of indexes
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor getIndex(String name) throws BaseException {
		if (name == null)
			return getIndexes();
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.GET_INXES;
		BSONObject dummyObj = new BasicBSONObject();
		BSONObject condition = new BasicBSONObject();
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
		condition.put(SequoiadbConstants.IXM_INDEXDEF + "."
				+ SequoiadbConstants.IXM_NAME, name);

		SDBMessage rtn = adminCommand(commandString, condition, dummyObj,
				dummyObj, obj, -1, -1, 0);
		int flags = rtn.getFlags();
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC) {
				return null;
			} else {
				throw new BaseException(flags);
			}
		}
		return new DBCursor(rtn, this);
	}

	/**
	 * @fn void createIndex(String name, BSONObject key, boolean isUnique,
	 *     boolean enforced)
	 * @brief Create a index with name and key
	 * @param name
	 *            The index name
	 * @param key
	 *            The index key, like: {"key":1/-1}, ASC(1)/DESC(-1)
	 * @param isUnique
	 *            Whether the index elements are unique or not
	 * @param enforced
	 *            Whether the index is enforced unique This element is
	 *            meaningful when isUnique is set to true
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void createIndex(String name, BSONObject key, boolean isUnique,
			boolean enforced) throws BaseException {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.CREATE_INX;
		BSONObject obj = new BasicBSONObject();
		BSONObject dummyObj = new BasicBSONObject();
		BSONObject createObj = new BasicBSONObject();
		obj.put(SequoiadbConstants.IXM_KEY, key);
		obj.put(SequoiadbConstants.IXM_NAME, name);
		obj.put(SequoiadbConstants.IXM_UNIQUE, isUnique);
		obj.put(SequoiadbConstants.IXM_ENFORCED, enforced);
		createObj.put(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
		createObj.put(SequoiadbConstants.FIELD_INDEX, obj);

		SDBMessage rtn = adminCommand(commandString, createObj, dummyObj,
				dummyObj, dummyObj, -1, -1, 0);

		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, name, key, isUnique);
		}
	}

	/**
	 * @fn void createIndex(String name, String key, boolean isUnique, boolean
	 *     enforced)
	 * @brief Create a index with name and key
	 * @param name
	 *            The index name
	 * @param key
	 *            The index key, like: {"key":1/-1}, ASC(1)/DESC(-1)
	 * @param isUnique
	 *            Whether the index elements are unique or not
	 * @param enforced
	 *            Whether the index is enforced unique This element is
	 *            meaningful when isUnique is set to true
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void createIndex(String name, String key, boolean isUnique,
			boolean enforced) throws BaseException {
		BSONObject k = null;
		if (key != null)
			k = (BSONObject) JSON.parse(key);
		createIndex(name, k, isUnique, enforced);
	}

	/**
	 * @fn void dropIndex(String name)
	 * @brief Remove the named index of current collection
	 * @param name
	 *            The index name
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void dropIndex(String name) throws BaseException {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.DROP_INX;
		BSONObject dummyObj = new BasicBSONObject();
		BSONObject dropObj = new BasicBSONObject();
		BSONObject index = new BasicBSONObject();
		index.put("", name);
		dropObj.put(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
		dropObj.put(SequoiadbConstants.FIELD_INDEX, index);

		SDBMessage rtn = adminCommand(commandString, dropObj, dummyObj,
				dummyObj, dummyObj, -1, -1, 0);

		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, name);
		}
	}
	
	/**
	 * @fn long getCount()
	 * @brief Get the amount of documents in current collection.
	 * @return the amount of matching documents
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public long getCount() throws BaseException {
		return getCount("");
	}
	
	/**
	 * @fn long getCount(String matcher)
	 * @brief Get the amount of matching documnets in current collection.
	 * @param matcher
	 *            the matching rule
	 * @return the amount of matching documents
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public long getCount(String matcher) throws BaseException {
		BSONObject con = null;
		if (matcher != null)
			con = (BSONObject) JSON.parse(matcher);
		return getCount(con);
	}
	
	/**
	 * @fn long getCount(BSONObject matcher)
	 * @brief Get the amount of matching documents in current collection.
	 * @param matcher
	 *            The matching rule
	 * @return the amount of matching documents
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public long getCount(BSONObject matcher) throws BaseException {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.GET_COUNT;
		BSONObject dummyObj = new BasicBSONObject();
		BSONObject newobj = new BasicBSONObject();
		newobj.put(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
		SDBMessage rtnSDBMessage = adminCommand(commandString, matcher,
				dummyObj, dummyObj, newobj, -1, -1, 0);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0)
			throw new BaseException(flags, matcher);

		List<BSONObject> rtn = getMoreCommand(rtnSDBMessage);
		return Long.valueOf(rtn.get(0).get(SequoiadbConstants.FIELD_TOTAL)
				.toString());
	}
	
	/**
	 * @fn long getCount(BSONObject condition, BSONObject hint)
	 * @brief Get the count of matching BSONObject in current collection
	 * @param condition
	 *            The matching rule
	 * @param hint
	 *            The hint, automatically match the optimal hint if null
	 * @return The count of matching BSONObjects
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public long getCount(BSONObject condition, BSONObject hint) throws BaseException {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.GET_COUNT;
		BSONObject dummyObj = new BasicBSONObject();
		BSONObject newobj = new BasicBSONObject();
		newobj.put(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
		if (null != hint){
			try{
				newobj.put(SequoiadbConstants.FIELD_NAME_HINT, hint);
			}catch(Exception e){
				throw new BaseException("SDB_SYS", e);
			}
		}
		SDBMessage rtnSDBMessage = adminCommand(commandString, condition,
				dummyObj, dummyObj, newobj, -1, -1, 0);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0)
			throw new BaseException(flags, condition, hint);

		List<BSONObject> rtn = getMoreCommand(rtnSDBMessage);
		return Long.valueOf(rtn.get(0).get(SequoiadbConstants.FIELD_TOTAL)
				.toString());
	}

	/*
	 * @fn void rename(String newName)
	 * @brief rename the current collection by new name
	 * @param newName
	 *            The new name of current DBCollection
	 * @exception com.sequoiadb.exception.BaseException
	 */
	/*
	public void rename(String newName) throws BaseException {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.RENAME_COLLECTION;
		BSONObject dummyObj = new BasicBSONObject();
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_COLLECTIONSPACE, csName);
		obj.put(SequoiadbConstants.FIELD_NAME_OLDNAME, this.name);
		obj.put(SequoiadbConstants.FIELD_NAME_NEWNAME, newName);
		SDBMessage rtnSDBMessage = adminCommand(commandString, obj, dummyObj,
				dummyObj, dummyObj, -1, -1);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, newName);
		}
		this.name = newName;
		this.collectionFullName = csName + "." + this.name;
	}*/

	/**
	 * @fn void split(String sourceGroupName, String destGroupName,
	 *                BSONObject splitCondition, BSONObject splitEndCondition)
	 * @brief Split the specified collection from source group to target group by range.
	 * @param sourceGroupName
	 *            the source group name
	 * @param destGroupName
	 *            the destination group name
     * @param splitCondition
	 *            the split condition
     * @param splitEndCondition
	 *            the split end condition or null
	 *            eg:If we create a collection with the option {ShardingKey:{"age":1},ShardingType:"Hash",Partition:2^10},
     *               we can fill {age:30} as the splitCondition, and fill {age:60} as the splitEndCondition. when split, 
     *               the target group will get the records whose age's hash value are in [30,60). If splitEndCondition is null,
     *               they are in [30,max).
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void split(String sourceGroupName, String destGroupName,
			BSONObject splitCondition, BSONObject splitEndCondition) throws BaseException {
		if((null == sourceGroupName || sourceGroupName.equals("")) ||
		   (null == destGroupName || destGroupName.equals("")) ||
		    null == splitCondition){
			 throw new BaseException("SDB_INVALIDARG", sourceGroupName, 
			         destGroupName, splitCondition);
		  }
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		obj.put(SequoiadbConstants.FIELD_NAME_SOURCE, sourceGroupName);
		obj.put(SequoiadbConstants.FIELD_NAME_TARGET, destGroupName);
		obj.put(SequoiadbConstants.FIELD_NAME_SPLITQUERY, splitCondition);
		if(null != splitEndCondition)
			obj.put(SequoiadbConstants.FIELD_NAME_SPLITENDQUERY, splitEndCondition);
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.CMD_NAME_SPLIT;
		BSONObject dummy = new BasicBSONObject();
		SDBMessage rtnSDBMessage = adminCommand(commandString, obj, dummy,
				dummy, dummy, -1, -1, 0);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, sourceGroupName, destGroupName,
					splitCondition, splitEndCondition);
		}
	}

	/**
	 * @fn void split(String sourceGroupName, String destGroupName, double percent)
	 * @brief Split the specified collection from source group to target group by percent.
	 * @param sourceGroupName
	 *            the source group name
	 * @param destGroupName
	 *            the destination group name
     * @param percent
	 *            the split percent, Range:(0,100]
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void split(String sourceGroupName, String destGroupName,
			double percent) throws BaseException {
		if((null == sourceGroupName || sourceGroupName.equals("")) ||
		   (null == destGroupName || destGroupName.equals("")) ||
		   (percent <= 0.0 || percent > 100.0)){
			 throw new BaseException("SDB_INVALIDARG", sourceGroupName,
			         destGroupName, percent);
		  }
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		obj.put(SequoiadbConstants.FIELD_NAME_SOURCE, sourceGroupName);
		obj.put(SequoiadbConstants.FIELD_NAME_TARGET, destGroupName);
		obj.put(SequoiadbConstants.FIELD_NAME_SPLITPERCENT, percent);
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.CMD_NAME_SPLIT;
		BSONObject dummy = new BasicBSONObject();
		SDBMessage rtnSDBMessage = adminCommand(commandString, obj, dummy,
				dummy, dummy, -1, -1, 0);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, sourceGroupName, destGroupName,
					percent);
		}
	}
	
	/**
	 * @fn long splitAsync(String sourceGroupName, String destGroupName,
	 *                     BSONObject splitCondition, BSONObject splitEndCondition)
	 * @brief Split the specified collection from source group to target group by range asynchronously.
	 * @param sourceGroupName
	 *            the source group name
	 * @param destGroupName
	 *            the destination group name
     * @param splitCondition
	 *            the split condition
     * @param splitEndCondition
	 *            the split end condition or null
	 *            eg:If we create a collection with the option {ShardingKey:{"age":1},ShardingType:"Hash",Partition:2^10},
     *               we can fill {age:30} as the splitCondition, and fill {age:60} as the splitEndCondition. when split, 
     *               the targe group will get the records whose age's hash values are in [30,60). If splitEndCondition is null,
     *               they are in [30,max).
     * @return return the task id, we can use the return id to manage the sharding which is run backgroup.
	 * @exception com.sequoiadb.exception.BaseException
	 * @see listTask, cancelTask
	 */
	public long splitAsync(String sourceGroupName,
			               String destGroupName,
			               BSONObject splitCondition,
			               BSONObject splitEndCondition) throws BaseException {
		if((null == sourceGroupName || sourceGroupName.equals("")) ||
		   (null == destGroupName || destGroupName.equals("")) ||
		    null == splitCondition){
			 throw new BaseException("SDB_INVALIDARG", sourceGroupName,
					 destGroupName, splitCondition, splitEndCondition);
		  }
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		obj.put(SequoiadbConstants.FIELD_NAME_SOURCE, sourceGroupName);
		obj.put(SequoiadbConstants.FIELD_NAME_TARGET, destGroupName);
		obj.put(SequoiadbConstants.FIELD_NAME_SPLITQUERY, splitCondition);
		if(null != splitEndCondition)
			obj.put(SequoiadbConstants.FIELD_NAME_SPLITENDQUERY, splitEndCondition);
		obj.put(SequoiadbConstants.FIELD_NAME_ASYNC, true);
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.CMD_NAME_SPLIT;
		BSONObject dummy = new BasicBSONObject();
		SDBMessage rtnSDBMessage = adminCommand(commandString, obj, dummy,
				dummy, dummy, -1, -1, 0);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, sourceGroupName, destGroupName,
					splitCondition, splitEndCondition);
		}
		DBCursor cursor = new DBCursor(rtnSDBMessage, this);
		if (!cursor.hasNext())
			throw new BaseException("SDB_CAT_TASK_NOTFOUND");
		BSONObject result = cursor.getNext();
		boolean flag = result.containsField(SequoiadbConstants.FIELD_NAME_TASKID);
		if (!flag)
			throw new BaseException("SDB_CAT_TASK_NOTFOUND");
		long taskid = (Long)result.get(SequoiadbConstants.FIELD_NAME_TASKID);
		return taskid;
	}

	/**
	 * @fn long splitAsync(String sourceGroupName, String destGroupName, double percent)
	 * @brief Split the specified collection from source group to target group by percent asynchronously.
	 * @param sourceGroupName
	 *            the source group name
	 * @param destGroupName
	 *            the destination group name
     * @param percent
	 *            the split percent, Range:(0,100]
	 * @return return the task id, we can use the return id to manage the sharding which is run backgroup.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public long splitAsync(String sourceGroupName, String destGroupName,
			               double percent) throws BaseException {
		if((null == sourceGroupName || sourceGroupName.equals("")) ||
		   (null == destGroupName || destGroupName.equals("")) ||
		   (percent <= 0.0 || percent > 100.0)){
			 throw new BaseException("SDB_INVALIDARG", sourceGroupName, destGroupName,
		                             percent);
		  }
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		obj.put(SequoiadbConstants.FIELD_NAME_SOURCE, sourceGroupName);
		obj.put(SequoiadbConstants.FIELD_NAME_TARGET, destGroupName);
		obj.put(SequoiadbConstants.FIELD_NAME_SPLITPERCENT, percent);
		obj.put(SequoiadbConstants.FIELD_NAME_ASYNC, true);
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.CMD_NAME_SPLIT;
		BSONObject dummy = new BasicBSONObject();
		SDBMessage rtnSDBMessage = adminCommand(commandString, obj, dummy,
				dummy, dummy, 0, -1, 0);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, sourceGroupName, destGroupName,
					                percent);
		}
		DBCursor cursor = new DBCursor(rtnSDBMessage, this);
		if (!cursor.hasNext())
			throw new BaseException("SDB_CAT_TASK_NOTFOUND");
		BSONObject result = cursor.getNext();
		boolean flag = result.containsField(SequoiadbConstants.FIELD_NAME_TASKID);
		if (!flag)
			throw new BaseException("SDB_CAT_TASK_NOTFOUND");
		long taskid = (Long)result.get(SequoiadbConstants.FIELD_NAME_TASKID);
		return taskid;
	}
	
	/**
	 * @fn DBCursor aggregate(List<BSONObject> obj)
	 * @brief Execute aggregate operation in current collection
	 * @param obj
	 *            The Bson object of rule list, can't be null
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor aggregate(List<BSONObject> obj)
			throws BaseException {

		if (obj == null || obj.size() == 0)
			throw new BaseException("SDB_INVALIDARG");

		if (this.insert_buffer == null) {
			this.insert_buffer = IoBuffer.allocate(DEF_BUFFER_LENGTH);
			this.insert_buffer.setAutoExpand(true);
			if (sequoiadb.endianConvert) {
				insert_buffer.order(ByteOrder.LITTLE_ENDIAN);
			} else {
				insert_buffer.order(ByteOrder.BIG_ENDIAN);
			}
		} else {
			insert_buffer.clear();
		}

		int messageLength = SDBMessageHelper.buildAggrRequest(insert_buffer, 
		        sequoiadb.getNextRequstID(), collectionFullName, obj);
		
		connection.sendMessage(insert_buffer.array(), messageLength);
		
		ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		if ( rtnSDBMessage.getOperationCode() != Operation.OP_AGGREGATE_RES) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", 
                    rtnSDBMessage.getOperationCode());
        }
		DBCursor cursor = null;
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC) {
				return cursor;
			} else {
				throw new BaseException(flags, obj);
			}
		}
		cursor = new DBCursor(rtnSDBMessage, this);
		return cursor;
	}
	
	/**
	 * @fn DBCursor getQueryMeta(BSONObject query, BSONObject
	 *     orderBy, BSONObject hint, long skipRows, long returnRows)
	 * @brief Get index blocks' or data blocks' infomation for concurrent query
	 * @param query
	 *            The matching condition
	 * @param orderBy
	 *            The ordered rule
	 * @param hint
	 *            One of the indexs of current collection,
	 *            using default index to query if not provided
     *            eg:{"":"ageIndex"}
	 * @param skipRows
	 *            The rows to be skipped
	 * @param returnRows
	 *            The rows to return
	 * @param flag
	 *            The flag to use which form for record data
	 *            0: bson stream
	 *            1: binary data stream, form: col1|col2|col3
	 * @return DBCursor of datas
	 * @exception com.sequoiadb.exception.BaseException
     * 
	 */
	public DBCursor getQueryMeta(BSONObject query,BSONObject orderBy,
			                     BSONObject hint,long skipRows, 
			                     long returnRows, int flag) throws BaseException {
		BSONObject dummy = new BasicBSONObject();
		if (query == null)
			query = dummy;
		if (orderBy == null)
			orderBy = dummy;
		if (hint == null)
			hint = dummy;
		if (returnRows == 0)
			returnRows = -1;
	    BSONObject hint1 = new BasicBSONObject();
	    hint1.put("Collection", this.collectionFullName);
		String command = SequoiadbConstants.ADMIN_PROMPT+SequoiadbConstants.GET_QUERYMETA;
		SDBMessage rtnSDBMessage = adminCommand(command, query, hint, orderBy, hint1,
				                                skipRows, returnRows, flag);
		DBCursor cursor = null;
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC) {
				return cursor;
			} else {
				throw new BaseException(flags, query, hint, orderBy, hint1,
						skipRows, returnRows);
			}
		}
		cursor = new DBCursor(rtnSDBMessage, this);
		return cursor;
	}
	
	/**
	 * @fn void attachCollection( String subClFullName,BSONObject options )
	 * @brief Attach the specified collection.
	 * @param subClFullName
	 *            The full name of the subcollection
	 * @param options
	 *            The low boudary and up boudary
	 *            eg: {"LowBound":{a:1},"UpBound":{a:100}}
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void attachCollection( String subClFullName,BSONObject options ) throws BaseException {
		if ( null == subClFullName || subClFullName.equals("") ||
		     null == options ||
		     null == collectionFullName || collectionFullName.equals("") ) {
			throw new BaseException("SDB_INVALIDARG", subClFullName,
			        options, collectionFullName);
		}
		String command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CMD_NAME_ATTACH_CL;
		BSONObject newobj = new BasicBSONObject();
		newobj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		newobj.put(SequoiadbConstants.FIELD_NAME_SUBCLNAME, subClFullName);
		for (String key : options.keySet()){
			newobj.put(key, options.get(key));
		}
		SDBMessage rtnSDBMessage = adminCommand ( command, newobj, null, null, null,
				      			                  0, -1, 0 );
		int flags = rtnSDBMessage.getFlags();
		if (0 != flags){
			throw new BaseException(flags, subClFullName, options);
		}
	}
	
	/**
	 * @fn void detachCollection( String subClFullName )
	 * @brief Dettach the specified collection.
	 * @param subClFullName
	 *            The full name of the subcollection
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void detachCollection( String subClFullName ) throws BaseException {
		if ( null == subClFullName || subClFullName.equals("") ||
		     null == collectionFullName || collectionFullName.equals("") ) {
			throw new BaseException("SDB_INVALIDARG", subClFullName);
		}
		String command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CMD_NAME_DETACH_CL;
		BSONObject newobj = new BasicBSONObject();
		newobj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		newobj.put(SequoiadbConstants.FIELD_NAME_SUBCLNAME, subClFullName);
		SDBMessage rtnSDBMessage = adminCommand ( command, newobj, null, null, null,
				      			                  0, -1, 0 );
		int flags = rtnSDBMessage.getFlags();
		if (0 != flags){
			throw new BaseException(flags, subClFullName);
		}
	}
	
	/**
	 * @fn void alterCollection ( BSONObject options )
     * @brief Alter the attributes of current collection.
     * @param options The options for altering current collection are as below:
     *<ul>
     *<li>ReplSize     : Assign how many replica nodes need to be synchronized when a write request(insert, update, etc) is executed
     *<li>ShardingKey   : Assign the sharding key
     *<li>ShardingType        : Assign the sharding type
     *<li>Partition        : When the ShardingType is "hash", need to assign Partition, it's the bucket number for hash, the range is [2^3,2^20].
     *                       e.g. {RepliSize:0, ShardingKey:{a:1}, ShardingType:"hash", Partition:1024}
     *</ul>
     * @note Can't alter attributes about split in partition collection; After altering a collection to
     *       be a partition collection, need to split this collection manually
	 * @exception com.sequoiadb.exception.BaseException
	 */	
	public void alterCollection( BSONObject options ) throws BaseException {
		if ( null == options ) {
			throw new BaseException("SDB_INVALIDARG", options);
		}
		String command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CMD_NAME_ALTER_COLLECTION;
		BSONObject newobj = new BasicBSONObject();
		newobj.put(SequoiadbConstants.FIELD_NAME_NAME, collectionFullName);
		newobj.put(SequoiadbConstants.FIELD_NAME_OPTIONS, options);
		SDBMessage rtnSDBMessage = adminCommand ( command, newobj, null, null, null,
				      			                  0, -1, 0 );
		int flags = rtnSDBMessage.getFlags();
		if (0 != flags){
			throw new BaseException(flags, options);
		}
	}
	
	private SDBMessage adminCommand(String commandString, BSONObject query,
			BSONObject selector, BSONObject orderBy, BSONObject hint,
			long skipRows, long returnRows, int flag) throws BaseException {
		SDBMessage sdbMessage = new SDBMessage();
		BSONObject dummy = new BasicBSONObject();
		if (query == null)
			query = dummy;
		if (selector == null)
			selector = dummy;
		if (orderBy == null)
			orderBy = dummy;
		if (hint == null)
			hint = dummy;

		sdbMessage.setCollectionFullName(commandString);

		sdbMessage.setVersion(1);
		sdbMessage.setW((short) 0);
		sdbMessage.setPadding((short) 0);
		sdbMessage.setFlags(flag);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		sdbMessage.setRequestID(sequoiadb.getNextRequstID());
		sdbMessage.setSkipRowsCount(skipRows);
		sdbMessage.setReturnRowsCount(returnRows);
		sdbMessage.setMatcher(query);
		sdbMessage.setSelector(selector);
		sdbMessage.setOrderBy(orderBy);
		sdbMessage.setHint(hint);
		sdbMessage.setOperationCode(Operation.OP_QUERY);

		byte[] request = SDBMessageHelper.buildQueryRequest(sdbMessage,
				sequoiadb.endianConvert);
		connection.sendMessage(request); 
		
		ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		SDBMessageHelper.checkMessage(sdbMessage, rtnSDBMessage);

		return rtnSDBMessage;
	}
	
	private List<BSONObject> getMoreCommand(SDBMessage rtnSDBMessage)
			throws BaseException {
		long reqId = rtnSDBMessage.getRequestID();
		List<Long> contextIds = rtnSDBMessage.getContextIDList();
		List<BSONObject> fullList = new ArrayList<BSONObject>();
		boolean hasMore = true;
		while (hasMore) {
			SDBMessage sdbMessage = new SDBMessage();
			sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
			sdbMessage.setContextIDList(contextIds);
			sdbMessage.setRequestID(reqId);
			sdbMessage.setNumReturned(-1);
			sdbMessage.setOperationCode(Operation.OP_GETMORE);

			byte[] request = SDBMessageHelper.buildGetMoreRequest(sdbMessage,
					sequoiadb.endianConvert);
			connection.sendMessage(request);
			
			ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
			rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
			SDBMessageHelper.checkMessage(sdbMessage, rtnSDBMessage);

			int flags = rtnSDBMessage.getFlags();
			if (flags != 0) {
				if (flags == SequoiadbConstants.SDB_DMS_EOC) {
					hasMore = false;
				} else {
					throw new BaseException(flags);
				}
			} else {
				reqId = rtnSDBMessage.getRequestID();
				List<BSONObject> objList = rtnSDBMessage.getObjectList();
				fullList.addAll(objList);
			}
		}

		return fullList;
	}

	private void _update(int flag, BSONObject matcher, BSONObject modifier,
			BSONObject hint) throws BaseException {
		BSONObject dummy = new BasicBSONObject();
		if (matcher == null)
			matcher = dummy;
		if (modifier == null)
			modifier = dummy;
		if (hint == null)
			hint = dummy;
		SDBMessage sdbMessage = new SDBMessage();
		sdbMessage.setVersion(1);
		sdbMessage.setW((short) 0);
		sdbMessage.setPadding((short) 0);
		sdbMessage.setFlags(flag);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		sdbMessage.setCollectionFullName(collectionFullName);
		sdbMessage.setRequestID(sequoiadb.getNextRequstID());
		sdbMessage.setMatcher(matcher);
		sdbMessage.setModifier(modifier);
		sdbMessage.setHint(hint);
		sdbMessage.setOperationCode(Operation.OP_UPDATE);

		byte[] request = SDBMessageHelper.buildUpdateRequest(sdbMessage,
				sequoiadb.endianConvert);
		connection.sendMessage(request);
		
		ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		SDBMessageHelper.checkMessage(sdbMessage, rtnSDBMessage);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0)
			throw new BaseException(flags, matcher, modifier, hint);
	}
	
	/**
     * @fn DBCursor listLobs()
     * @brief Get all of the lobs in current collection
     * @return DBCursor of lobs
     * @exception com.sequoiadb.exception.BaseException
     */
    public DBCursor listLobs() throws BaseException {
        String commandString = SequoiadbConstants.ADMIN_PROMPT
                + SequoiadbConstants.LIST_LOBS;
        BSONObject dummyObj = new BasicBSONObject();
        BSONObject obj = new BasicBSONObject();
        obj.put(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);

        SDBMessage rtn = adminCommand(commandString, obj, dummyObj, dummyObj,
                dummyObj, -1, -1, 0);
        int flags = rtn.getFlags();
        DBCursor cursor = null;
        if (flags != 0) {
            if (flags == SequoiadbConstants.SDB_DMS_EOC) {
                return cursor;
            } else {
                throw new BaseException(flags);
            }
        }
        cursor = new DBCursor(rtn, sequoiadb);
        return cursor;
    }
    
    /**
     * @fn          DBLob createLob()
     * @brief       create a lob
     * @return      DBLob object
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public DBLob createLob() throws BaseException {
        return createLob( null );
    }
    
    /**
     * @fn          DBLob createLob( ObjectId id )
     * @brief       create a lob with a given id
     * @param       id   the lob's id. if id is null, it will be generated in 
     *                   this function
     * @return      DBLob object
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public DBLob createLob( ObjectId id ) throws BaseException {
        DBLobConcrete lob = new DBLobConcrete( this );
        lob.open( id, DBLobConcrete.SDB_LOB_CREATEONLY );
        return lob;
    }
    
    /**
     * @fn          DBLob openLob( ObjectId id )
     * @brief       open an exist lob with id
     * @param       id   the lob's id. 
     * @return      DBLob object
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public DBLob openLob( ObjectId id ) throws BaseException {
        DBLobConcrete lob = new DBLobConcrete( this );
        lob.open( id, DBLobConcrete.SDB_LOB_READ );
        return lob;
    }
    
    /**
     * @fn          removeLob( ObjectId id )
     * @brief       remove an exist lob
     * @param       id   the lob's id. 
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public void removeLob( ObjectId lobID ) throws BaseException {
        BSONObject removeObj = new BasicBSONObject();
        removeObj.put( SequoiadbConstants.FIELD_COLLECTION, 
                collectionFullName );
        removeObj.put( SequoiadbConstants.FIELD_NAME_LOB_OID, lobID );

        byte[] request = SDBMessageHelper.generateRemoveLobRequest(removeObj,
                sequoiadb.getNextRequstID(), sequoiadb.endianConvert );
        connection.sendMessage( request, request.length );
        ByteBuffer res = connection.receiveMessage(sequoiadb.endianConvert);
        
        SDBMessage resMessage = SDBMessageHelper.msgExtractLobRemoveReply( res );
        if ( resMessage.getOperationCode() != Operation.MSG_BS_LOB_REMOVE_RES) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", 
                    resMessage.getOperationCode());
        }
        int flag = resMessage.getFlags();
        if ( 0 != flag ) {
            throw new BaseException( flag, removeObj );
        }
    }
}
