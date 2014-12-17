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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.util.JSON;

import com.sequoiadb.base.SequoiadbConstants.PreferInstanceType;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.net.ConfigOptions;
import com.sequoiadb.net.ConnectionTCPImpl;
import com.sequoiadb.net.IConnection;
import com.sequoiadb.net.ServerAddress;
import com.sequoiadb.util.SDBMessageHelper;

/**
 * @class Sequoiadb
 * @brief Database operation interfaces of admin.
 */
public class Sequoiadb {
	private ServerAddress serverAddress;
	private IConnection connection;
	private String userName;
	private String password;
	boolean endianConvert;

	public final static int SDB_PAGESIZE_4K = 4096;
	public final static int SDB_PAGESIZE_8K = 8192;
	public final static int SDB_PAGESIZE_16K = 16384;
	public final static int SDB_PAGESIZE_32K = 32768;
	public final static int SDB_PAGESIZE_64K = 65536;
	/** 0 means using database's default pagesize, it 64k now */
	public final static int SDB_PAGESIZE_DEFAULT = 0;

	public final static int SDB_LIST_CONTEXTS = 0;
	public final static int SDB_LIST_CONTEXTS_CURRENT = 1;
	public final static int SDB_LIST_SESSIONS = 2;
	public final static int SDB_LIST_SESSIONS_CURRENT = 3;
	public final static int SDB_LIST_COLLECTIONS = 4;
	public final static int SDB_LIST_COLLECTIONSPACES = 5;
	public final static int SDB_LIST_STORAGEUNITS = 6;
	public final static int SDB_LIST_GROUPS = 7;
	public final static int SDB_LIST_STOREPROCEDURES = 8;
	public final static int SDB_LIST_DOMAINS = 9;
    public final static int SDB_LIST_TASKS = 10;
	public final static int SDB_LIST_CS_IN_DOMAIN = 11;
    public final static int SDB_LIST_CL_IN_DOMAIN = 12;

	public final static int SDB_SNAP_CONTEXTS = 0;
	public final static int SDB_SNAP_CONTEXTS_CURRENT = 1;
	public final static int SDB_SNAP_SESSIONS = 2;
	public final static int SDB_SNAP_SESSIONS_CURRENT = 3;
	public final static int SDB_SNAP_COLLECTIONS = 4;
	public final static int SDB_SNAP_COLLECTIONSPACES = 5;
	public final static int SDB_SNAP_DATABASE = 6;
	public final static int SDB_SNAP_SYSTEM = 7;
	public final static int SDB_SNAP_CATALOG = 8;
	
	public final static int FMP_FUNC_TYPE_INVALID = -1;
	public final static int FMP_FUNC_TYPE_JS = 0;
	public final static int FMP_FUNC_TYPE_C = 1;
	public final static int FMP_FUNC_TYPE_JAVA = 2;
	
	public final static String CATALOG_GROUP_NAME = "SYSCatalogGroup";

	/**
	 * @fn IConnection getConnection()
	 * @brief Get the current connection to remote server.
	 * @return IConnection
	 */
	public IConnection getConnection() {
		return connection;
	}

	/**
	 * @fn ServerAddress getServerAddress()
	 * @brief Get the address of remote server.
	 * @return ServerAddress
	 */
	public ServerAddress getServerAddress() {
		return serverAddress;
	}

	/**
	 * @fn void setServerAddress(ServerAddress serverAddress)
	 * @brief Set the address of remote server.
	 * @param serverAddress
	 *            the serverAddress object of remote server
	 */
	public void setServerAddress(ServerAddress serverAddress) {
		this.serverAddress = serverAddress;
	}

	/**
	 * @fn boolean isEndianConvert()
	 * @brief Judge the endian of the physical computer
	 * @return Big-Endian for true while Little-Endian for false
	 */
	public boolean isEndianConvert() {
		return endianConvert;
	}
	
	/**
	 * @fn Sequoiadb(String username, String password)
	 * @brief Constructor. The server address is "127.0.0.1 : 11810".
	 * @param username the user's name of the account
	 * @param password the password of the account
	 * @exception com.sequoiadb.exception.BaseException
	 *            "SDB_NETWORK" means network error,
	 *            "SDB_INVALIDARG" means wrong address or the address don't map to the hosts table
	 */
	public Sequoiadb(String username, String password) throws BaseException
	{
		serverAddress = new ServerAddress();
		ConfigOptions opts = new ConfigOptions();
		initConnection(opts);
		this.userName = username;
		this.password = password;
		auth();
	}
	
	/**
	 * @fn Sequoiadb(String connString, String username, String password)
	 * @brief Constructor.
	 * @param connString
	 *            remote server address "IP : Port" or "IP"(port is 50000)
	 * @param username the user's name of the account
	 * @param password the password of the account
	 * @exception com.sequoiadb.exception.BaseException
	 *            "SDB_NETWORK" means network error,
	 *            "SDB_INVALIDARG" means wrong address or the address don't map to the hosts table
	 */
	public Sequoiadb(String connString, String username, String password)
			throws BaseException
	{
		/*
		try
		{
			serverAddress = new ServerAddress(connString);
			ConfigOptions opts = new ConfigOptions();
			initConnection(opts);
		}
		catch (UnknownHostException e)
		{
			throw new BaseException("SDB_NETWORK", connString);
		}
		this.userName = username;
		this.password = password;
		auth();
		*/
		this(connString, username, password, null);
	}

	/**
	 * @fn Sequoiadb(String connString, String username,
	 *               String password, ConfigOptions options)
	 * @brief Constructor.
	 * @param connString
	 *            remote server address "IP : Port" or "IP"(port is 11810)
	 * @param username the user's name of the account
	 * @param password the password of the account
	 * @param options the options for connection
	 * @exception com.sequoiadb.exception.BaseException 
	 *            "SDB_NETWORK" means network error,
	 *            "SDB_INVALIDARG" means wrong address or the address don't map to the hosts table
	 */
	public Sequoiadb(String connString, String username, String password,
			         ConfigOptions options) throws BaseException
    {
		ConfigOptions opts = options;
		if (null == options)
			opts = new ConfigOptions();
		try
		{
			serverAddress = new ServerAddress(connString);
			initConnection(opts);
		}
		catch (UnknownHostException e)
		{
			throw new BaseException("SDB_NETWORK", connString, e);
		}
		this.userName = username;
		this.password = password;
		auth();
	}
	
	/**
	 * @fn Sequoiadb(List<String> connStrings, String username, String password,
	 *	             ConfigOptions options)
	 * @brief Constructor, use a random valid address to connect to database.
	 * @param connStrings The array of the coord's address
	 * @param username the user's name of the account
	 * @param password the password  of the account
	 * @param options the options for connection
	 * @exception com.sequoiadb.exception.BaseException
	 *            "SDB_NETWORK" means network error,
	 *            "SDB_INVALIDARG" means wrong address or the address don't map to the hosts table in local computer
	 */
	public Sequoiadb(List<String> connStrings, String username, String password,
			         ConfigOptions options) throws BaseException
	{
		ConfigOptions opts = options;
		if (options == null)
			opts = new ConfigOptions();
		int size = connStrings.size();
		if (0 == size)
		{
			throw new BaseException("SDB_INVALIDARG", "Address list is empty");
		}
		Random random = new Random();
		int count = random.nextInt(size);
		int mark = count;
		do
		{
			count = ++count % size;
			String str = connStrings.get(count);
			try
			{
				try
				{
					serverAddress = new ServerAddress(str);
					initConnection(opts);
				}
				catch (UnknownHostException e)
				{
					throw new BaseException("SDB_NETWORK", str);
				}
				this.userName = username;
				this.password = password;
				auth();
			}
			catch (BaseException e)
			{
				if ( mark == count)
				{
					throw new BaseException("SDB_NET_CANNOT_CONNECT");
				}
				continue;
			}
			break;
		} while (mark != count);
	}
	
	/**
	 * @fn Sequoiadb(String addr, int port, String username, String password)
	 * @brief Constructor.
	 * @param addr the address of coord
	 * @param port the port of coord
	 * @param username the user's name of the account
	 * @param password the password  of the account
	 * @exception com.sequoiadb.exception.BaseException
	 *            "SDB_NETWORK" means network error,
	 *            "SDB_INVALIDARG" means wrong address or the address don't map to the hosts table
	 */
	public Sequoiadb(String addr, int port, String username, String password)
			throws BaseException
    {
		/*
		try {
			serverAddress = new ServerAddress(addr, port);
			ConfigOptions opts = new ConfigOptions();
			initConnection(opts);
		} catch (UnknownHostException e) {
			throw new BaseException("SDB_NETWORK", addr, port);
		}
		this.userName = username;
		this.password = password;
		auth();
		*/
		this(addr, port, username, password, null);
	}

	/**
	 * @fn Sequoiadb(String addr, int port, String username,
	 *               String password, ConfigOptions options)
	 * @brief Constructor.
	 * @param addr the address of coord
	 * @param port the port of coord
	 * @param username the user's name of the account
	 * @param password the password of the account
	 * @exception com.sequoiadb.exception.BaseException
	 *            "SDB_NETWORK" means network error,
	 *            "SDB_INVALIDARG" means wrong address or the address don't map to the hosts table
	 */
	public Sequoiadb(String addr, int port, 
			         String username, String password,
			         ConfigOptions options) throws BaseException
    {
		ConfigOptions opts = options;
		if (options == null)
			opts = new ConfigOptions();
		try
		{
			serverAddress = new ServerAddress(addr, port);
			initConnection(opts);
		}
		catch (UnknownHostException e)
		{
			throw new BaseException("SDB_NETWORK", addr, port, e);
		}
		this.userName = username;
		this.password = password;
		auth();
	}
	
	/**
	 * @fn auth()
	 * @brief authentication
	 */
	private void auth()
	{
		endianConvert = requestSysInfo();
		byte[] request = SDBMessageHelper.buildAuthMsg(userName, password, 0,
				(byte) 0, endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtn = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtn.getFlags();
		if (flags != 0)
		{
			connection.close();
			throw new BaseException(flags, userName, password);
		}
	}

	/**
	 * @fn void createUser(String username, String password)
	 * @brief Add an user in current database.
	 * @param username
	 *            The connection user name
	 * @param password
	 *            The connection password
	 */
	public void createUser(String username, String password) throws BaseException {
		if(username == null || password == null) {
			throw new BaseException("SDB_INVALIDARG");
		}
		byte[] request = SDBMessageHelper.buildAuthMsg(username, password,
				(long) 0, (byte) 1, endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtn = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, username, password);
		}
	}

	/**
	 * @fn void removeUser(String username, String password)
	 * @brief Remove the spacified user from current database.
	 * @param username
	 *            The connection user name
	 * @param password
	 *            The connection password
	 */
	public void removeUser(String username, String password) throws BaseException {
		byte[] request = SDBMessageHelper.buildAuthMsg(username, password, (long)0,
				(byte) 2, endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtn = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, username, password);
		}
	}

	/**
	 * @fn void disconnect()
	 * @brief Disconnect from the remote server.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void disconnect() throws BaseException {
		byte[] request = SDBMessageHelper.buildDisconnectRequest(endianConvert);
		releaseResource();
		connection.sendMessage(request);
		connection.close();
	}

	/**
	 * @fn boolean isClosed()
	 * @brief Judge wether the connection is connected or not.
	 * @return if the connection is connected, return true
	 */
	private boolean isClosed(){
		if (connection == null)
			return true;
		return connection.isClosed();
	}
	
	/**
	 * @fn boolean isValid()
	 * @brief Judge wether the connection is valid or not.
	 * @return if the connection is valid, return true
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public boolean isValid() throws BaseException{
		if ( connection == null || connection.isClosed() )
			return false;
		try{
			sendKillContextMsg();
		}catch(BaseException e){
			return false;
		}
		return true;
	}
	
	/**
	 * @fn void changeConnectionOptions(ConfigOptions opts)
	 * @brief Change the connection options.
	 * @param opts
	 *            The connection options
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void changeConnectionOptions(ConfigOptions opts)
			throws BaseException {
		connection.changeConfigOptions(opts);
		auth();
	}

	/**
	 * @fn void createCollectionSpace(String collectionSpaceName)
	 * @brief Create the named collection space with default SDB_PAGESIZE_4K.
	 * @param csName
	 *            The collection space name
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public CollectionSpace createCollectionSpace(String csName)
			throws BaseException {
		return createCollectionSpace(csName, SDB_PAGESIZE_DEFAULT);
	}

	/**
	 * @fn CollectionSpace createCollectionSpace(String collectionSpaceName, int pageSize)
	 * @brief Create collection space.
	 * @param csName The name of collection space
	 * @param pageSize The Page Size as below:
	 * <ul>
	 * <li> SDB_PAGESIZE_4K
	 * <li> SDB_PAGESIZE_8K
	 * <li> SDB_PAGESIZE_16K
	 * <li> SDB_PAGESIZE_32K
	 * <li> SDB_PAGESIZE_64K
	 * <li> SDB_PAGESIZE_DEFAULT
	 * </ul>
	 * @return the newly created collection space object
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public CollectionSpace createCollectionSpace(String csName, int pageSize)
			throws BaseException {
/*
		if (isCollectionSpaceExist(csName))
			throw new BaseException("SDB_DMS_CS_EXIST", csName);
		if (pageSize != SDB_PAGESIZE_4K && pageSize != SDB_PAGESIZE_8K
				&& pageSize != SDB_PAGESIZE_16K && pageSize != SDB_PAGESIZE_32K
				&& pageSize != SDB_PAGESIZE_64K && pageSize != SDB_PAGESIZE_DEFAULT) {
			throw new BaseException("SDB_INVALIDARG", pageSize);
		}
		SDBMessage rtnSDBMessage = createCS(csName, pageSize);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0)
			throw new BaseException(flags);
		return getCollectionSpace(csName);
*/
		BSONObject options = new BasicBSONObject();
		options.put("PageSize", pageSize);
		return createCollectionSpace(csName, options);
	}

	/**
	 * @fn CollectionSpace createCollectionSpace(String csName, BSONObject options)
	 * @brief Create collection space.
	 * @param csName The name of collection space
	 * @param options Contains configuration informations for create collection space. The options are as below:
	 * <ul>
	 * <li>PageSize    : Assign how large the page size is for the collection created in this collection space, default to be 64K 
	 * <li>Domain    : Assign which domain does current collection space belong to, it will belongs to the system domain if not assign this option
	 * </ul>
	 * @return the newly created collection space object
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public CollectionSpace createCollectionSpace(String csName, BSONObject options)
			throws BaseException {
		if (isCollectionSpaceExist(csName))
			throw new BaseException("SDB_DMS_CS_EXIST", csName);
		SDBMessage rtnSDBMessage = createCS(csName, options);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0)
			throw new BaseException(flags);
		return getCollectionSpace(csName);
	}
	
	/**
	 * @fn void dropCollectionSpace(String collectionSpaceName)
	 * @brief Remove the named collection space.
	 * @param csName
	 *            The collection space name
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void dropCollectionSpace(String csName) throws BaseException {
		if (!isCollectionSpaceExist(csName)) {
			throw new BaseException("SDB_DMS_CS_NOTEXIST", csName);
		}
		BSONObject matcher = new BasicBSONObject();
		matcher.put(SequoiadbConstants.FIELD_NAME_NAME, csName);
		String commandString = SequoiadbConstants.DROP_CMD + " "
				+ SequoiadbConstants.COLSPACE;
		SDBMessage rtn = adminCommand(commandString, 0, 0, -1, -1, matcher,
				null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}

	/**
	 * @fn CollectionSpace getCollectionSpace(String csName)
	 * @brief Get the named collection space.
	 * @param csName
	 *            The collection space name.
	 * @return The CollecionSpace handle.
	 * @note If the collection space not exit, throw BaseException "SDB_DMS_CS_NOTEXIST".
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public CollectionSpace getCollectionSpace(String csName)
			throws BaseException {
		if (isCollectionSpaceExist(csName)) {
			return new CollectionSpace(this, csName.trim());
		} else {
			throw new BaseException("SDB_DMS_CS_NOTEXIST", csName);
		}
	}

	/**
	 * @fn boolean isCollectionSpaceExist(String csName)
	 * @brief Verify the existence of collection space.
	 * @param csName
	 *            The collecion space name
	 * @return True if existed or False if not existed
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public boolean isCollectionSpaceExist(String csName) throws BaseException {
		String commandString = SequoiadbConstants.TEST_CMD + " "
				+ SequoiadbConstants.COLSPACE;
		BSONObject matcher = new BasicBSONObject();
		matcher.put(SequoiadbConstants.FIELD_NAME_NAME, csName);
		SDBMessage rtn = adminCommand(commandString, 0, 0, -1, -1, matcher,
				null, null, null);
		int flags = rtn.getFlags();
		if (flags == 0)
			return true;
		else if (flags == new BaseException("SDB_DMS_CS_NOTEXIST")
				.getErrorCode())
			return false;
		else
			throw new BaseException(flags, csName);
	}

	/**
	 * @fn DBCursor listCollectionSpaces()
	 * @brief Get all the collecionspaces.
	 * @return cursor of all collecionspace names
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor listCollectionSpaces() throws BaseException {
		return getList(SDB_LIST_COLLECTIONSPACES, 0, 0, -1, -1, null, null,
				null, null);
	}
	
	/**
	 * @fn ArrayList<String> getCollectionSpaceNames()
	 * @brief Get all the collecion space names
	 * @return A list of all collecion space names
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public ArrayList<String> getCollectionSpaceNames() throws BaseException {
		DBCursor cursor = getList(SDB_LIST_COLLECTIONSPACES, 0, 0, -1, -1,
				null, null, null, null);
		if (cursor == null)
			return null;
		ArrayList<String> colList = new ArrayList<String>();
		while (cursor.hasNext()) {
			colList.add(cursor.getNext().get("Name").toString());
		}
		return colList;
	}
	
	/**
	 * @fn DBCursor listCollections()
	 * @brief Get all the collections
	 * @return dbCursor of all collecions
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor listCollections() throws BaseException {
		return getList(SDB_LIST_COLLECTIONS, 0, 0, 0, -1, null, null, null,
				null);
	}

	/**
	 * @fn ArrayList<String> getCollectionNames()
	 * @brief Get all the collection names
	 * @return A list of all collecion names
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public ArrayList<String> getCollectionNames() throws BaseException {
		DBCursor cursor = getList(SDB_LIST_COLLECTIONS, 0, 0, 0, -1, null,
				null, null, null);
		if (cursor == null)
			return null;
		ArrayList<String> colList = new ArrayList<String>();
		while (cursor.hasNext()) {
			colList.add(cursor.getNext().get("Name").toString());
		}
		return colList;
	}

	/**
	 * @fn List<BSONObject> getStorageUnits()
	 * @brief Get all the storage units
	 * @return A list of all storage units
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public ArrayList<String> getStorageUnits() throws BaseException {
		DBCursor cursor = getList(SDB_LIST_STORAGEUNITS, 0, 0, -1, -1, null,
				null, null, null);
		ArrayList<String> colList = new ArrayList<String>();
		while (cursor.hasNext()) {
			colList.add(cursor.getNext().get("Name").toString());
		}
		return colList;
	}

	/**
	 * @fn void resetSnapshot()
	 * @brief Reset the snapshot.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void resetSnapshot() throws BaseException {
		String commandString = SequoiadbConstants.SNAP_CMD + " "
				+ SequoiadbConstants.RESET;
		SDBMessage rtn = adminCommand(commandString, 0, 0, -1, -1, null, null,
				null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}

	/**
	 * @fn DBCursor getList(int listType, BSONObject query, BSONObject selector,
			BSONObject orderBy)
     * @brief Get the informations of specified type.
     * @param listType The list type as below:
     *<dl>
     *<dt>Sequoiadb.SDB_LIST_CONTEXTS   : Get all contexts list
     *<dt>Sequoiadb.SDB_LIST_CONTEXTS_CURRENT        : Get contexts list for the current session
     *<dt>Sequoiadb.SDB_LIST_SESSIONS        : Get all sessions list
     *<dt>Sequoiadb.SDB_LIST_SESSIONS_CURRENT        : Get the current session
     *<dt>Sequoiadb.SDB_LIST_COLLECTIONS        : Get all collections list
     *<dt>Sequoiadb.SDB_LIST_COLLECTIONSPACES        : Get all collection spaces list
     *<dt>Sequoiadb.SDB_LIST_STORAGEUNITS        : Get storage units list
     *<dt>Sequoiadb.SDB_LIST_GROUPS        : Get replica group list ( only applicable in sharding env )
     *<dt>Sequoiadb.SDB_LIST_STOREPROCEDURES           : Get stored procedure list ( only applicable in sharding env )
     *<dt>Sequoiadb.SDB_LIST_DOMAINS        : Get all the domains list ( only applicable in sharding env )
     *<dt>Sequoiadb.SDB_LIST_TASKS        : Get all the running split tasks ( only applicable in sharding env )
     *</dl>
     * @param query The matching rule, match all the documents if null.
     * @param selector The selective rule, return the whole document if null.
     * @param orderBy The ordered rule, never sort if null.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor getList(int listType, BSONObject query, BSONObject selector, BSONObject orderBy) throws BaseException {
		return getList(listType, 0, 0, 0, -1, query, selector, orderBy, null);
	}

	/**
	 * @fn void flushConfigure(BSONObject param)
	 * @brief Flush the options to configuration file
	 * @param param
	 *            The param of flush, pass {"Global":true} or {"Global":false}
	 *            In cluster environment, passing {"Global":true} will flush data's and catalog's configuration file,
	 *            while passing {"Global":false} will flush coord's configuration file
	 *            In stand-alone environment, both them have the same behaviour
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void flushConfigure(BSONObject param) throws BaseException {
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_EXPORT_CONFIG,0,0,0,-1,param,null,null,null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}

	/**
	 * @fn void execUpdate(String sql)
	 * @brief Execute sql in database.
	 * @param sql the SQL command.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void execUpdate(String sql) throws BaseException {
		SDBMessage sdb = new SDBMessage();
		sdb.setRequestID(0);
		sdb.setNodeID(SequoiadbConstants.ZERO_NODEID);
		byte[] request = SDBMessageHelper.buildSqlMsg(sdb, sql, endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtn = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, sql);
		}
	}

	/**
	 * @fn DBCursor exec(String sql)
	 * @brief Execute sql in database.
	 * @param sql the SQL command
	 * @return the DBCursor of the result
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor exec(String sql) throws BaseException {
		SDBMessage sdb = new SDBMessage();
		sdb.setRequestID(0);
		sdb.setNodeID(SequoiadbConstants.ZERO_NODEID);
		byte[] request = SDBMessageHelper.buildSqlMsg(sdb, sql, endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtn = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtn.getFlags();
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC)
				return null;
			else {
				throw new BaseException(flags, sql);
			}
		}
		return new DBCursor(rtn, this);
	}

	/**
	 * @fn DBCursor getSnapshot(int snapType, String matcher, String selector,
	 *     String orderBy)
	 * @brief Get snapshot of the database.
     * @param snapType The snapshot types are as below:
     * <dl>
     * <dt>Sequoiadb.SDB_SNAP_CONTEXTS   : Get all contexts' snapshot
     * <dt>Sequoiadb.SDB_SNAP_CONTEXTS_CURRENT        : Get the current context's snapshot
     * <dt>Sequoiadb.SDB_SNAP_SESSIONS        : Get all sessions' snapshot
     * <dt>Sequoiadb.SDB_SNAP_SESSIONS_CURRENT        : Get the current session's snapshot
     * <dt>Sequoiadb.SDB_SNAP_COLLECTIONS        : Get the collections' snapshot
     * <dt>Sequoiadb.SDB_SNAP_COLLECTIONSPACES        : Get the collection spaces' snapshot
     * <dt>Sequoiadb.SDB_SNAP_DATABASE        : Get database's snapshot
     * <dt>Sequoiadb.SDB_SNAP_SYSTEM        : Get system's snapshot
     * <dt>Sequoiadb.SDB_SNAP_CATALOG        : Get catalog's snapshot
     * <dt>Sequoiadb.SDB_LIST_GROUPS        : Get replica group list ( only applicable in sharding env )
     * <dt>Sequoiadb.SDB_LIST_STOREPROCEDURES           : Get stored procedure list ( only applicable in sharding env )
     * </dl>
	 * @param matcher
	 *            the matching rule, match all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @return the DBCursor instance of the result
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor getSnapshot(int snapType, String matcher, String selector,
			String orderBy) throws BaseException {
		BSONObject ma = null;
		BSONObject se = null;
		BSONObject or = null;
		if (matcher != null)
			ma = (BSONObject) JSON.parse(matcher);
		if (selector != null)
			se = (BSONObject) JSON.parse(selector);
		if (orderBy != null)
			or = (BSONObject) JSON.parse(orderBy);

		return getSnapshot(snapType, ma, se, or);
	}

	/**
	 * @fn DBCursor getSnapshot(int snapType, BSONObject matcher, BSONObject
	 *     selector, BSONObject orderBy)
	 * @brief Get snapshot of the database.
     * @param snapType The snapshot types are as below:
     * <dl>
     * <dt>Sequoiadb.SDB_SNAP_CONTEXTS   : Get all contexts' snapshot
     * <dt>Sequoiadb.SDB_SNAP_CONTEXTS_CURRENT        : Get the current context's snapshot
     * <dt>Sequoiadb.SDB_SNAP_SESSIONS        : Get all sessions' snapshot
     * <dt>Sequoiadb.SDB_SNAP_SESSIONS_CURRENT        : Get the current session's snapshot
     * <dt>Sequoiadb.SDB_SNAP_COLLECTIONS        : Get the collections' snapshot
     * <dt>Sequoiadb.SDB_SNAP_COLLECTIONSPACES        : Get the collection spaces' snapshot
     * <dt>Sequoiadb.SDB_SNAP_DATABASE        : Get database's snapshot
     * <dt>Sequoiadb.SDB_SNAP_SYSTEM        : Get system's snapshot
     * <dt>Sequoiadb.SDB_SNAP_CATALOG        : Get catalog's snapshot
     * <dt>Sequoiadb.SDB_LIST_GROUPS        : Get replica group list ( only applicable in sharding env )
     * <dt>Sequoiadb.SDB_LIST_STOREPROCEDURES           : Get stored procedure list ( only applicable in sharding env )
     * </dl>
	 * @param matcher
	 *            the matching rule, match all the documents if null
	 * @param selector
	 *            the selective rule, return the whole document if null
	 * @param orderBy
	 *            the ordered rule, never sort if null
	 * @return the DBCursor instance of the result
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor getSnapshot(int snapType, BSONObject matcher,
			BSONObject selector, BSONObject orderBy) throws BaseException {
		String command = SequoiadbConstants.SNAP_CMD;
		switch (snapType) {
		case SDB_SNAP_CONTEXTS:
			command += " " + SequoiadbConstants.CONTEXTS;
			break;
		case SDB_SNAP_CONTEXTS_CURRENT:
			command += " " + SequoiadbConstants.CONTEXTS_CUR;
			break;
		case SDB_SNAP_SESSIONS:
			command += " " + SequoiadbConstants.SESSIONS;
			break;
		case SDB_SNAP_SESSIONS_CURRENT:
			command += " " + SequoiadbConstants.SESSIONS_CUR;
			break;
		case SDB_SNAP_COLLECTIONS:
			command += " " + SequoiadbConstants.COLLECTIONS;
			break;
		case SDB_SNAP_COLLECTIONSPACES:
			command += " " + SequoiadbConstants.COLSPACES;
			break;
		case SDB_SNAP_DATABASE:
			command += " " + SequoiadbConstants.DATABASE;
			break;
		case SDB_SNAP_SYSTEM:
			command += " " + SequoiadbConstants.SYSTEM;
			break;
		case SDB_SNAP_CATALOG:
			command += " " + SequoiadbConstants.CATA;
			break;
		default:
			throw new BaseException("SDB_INVALIDARG");
		}

		SDBMessage rtn = adminCommand(command, 0, 0, -1, -1, matcher, selector,
				orderBy, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC) {
				return null;
			} else {
				throw new BaseException(flags, matcher, selector, orderBy);
			}
		}
		return new DBCursor(rtn, this);

	}
	
	/**
	 * @fn void beginTransaction()
	 * @brief Begin the transaction.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void beginTransaction() throws BaseException {
		byte[] request = SDBMessageHelper.buildTransactionRequest(SequoiadbConstants.Operation.TRANS_BEGIN_REQ, endianConvert);
		connection.sendMessage(request);
		
		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtn = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtn.getFlags();
		if(flags != 0)
			throw new BaseException(flags);
	}
	
	/**
	 * @fn void commit()
	 * @brief Commit the transaction.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void commit() throws BaseException {
		byte[] request = SDBMessageHelper.buildTransactionRequest(
				SequoiadbConstants.Operation.TRANS_COMMIT_REQ, endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtn = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtn.getFlags();
		if(flags != 0)
			throw new BaseException(flags);
	}
	
	/**
	 * @fn void rollback()
	 * @brief Rollback the transaction.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void rollback() throws BaseException {
		byte[] request = SDBMessageHelper.buildTransactionRequest(
				SequoiadbConstants.Operation.TRANS_ROLLBACK_REQ, endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtn = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtn.getFlags();
		if(flags != 0)
			throw new BaseException(flags);
	}

	/**
	 * @fn void crtJSProcedure ( String code )
     * @brief Create a store procedure.
     * @param code The code of store procedure
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void crtJSProcedure ( String code ) throws BaseException
	{
		if ( null == code || code.equals("") ){
			throw new BaseException("SDB_INVALIDARG", code);
		}
		BSONObject newobj = new BasicBSONObject();
		Code codeObj = new Code( code );
		newobj.put(SequoiadbConstants.FIELD_NAME_FUNC, codeObj);
		newobj.put(SequoiadbConstants.FMP_FUNC_TYPE, FMP_FUNC_TYPE_JS);
		
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_CRT_PROCEDURE,
				                      0,0,0,-1,newobj,
				                      null,null,null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}
	
	/**
	 * @fn void rmProcedure ( String name )
     * @brief Remove a store procedure.
     * @param name The name of store procedure to be removed
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void rmProcedure ( String name ) throws BaseException
	{
		if ( null == name || name.equals("") ) {
			throw new BaseException("SDB_INVALIDARG", name);
		}
		BSONObject newobj = new BasicBSONObject();
		newobj.put( SequoiadbConstants.FIELD_NAME_FUNC, name );
		
		SDBMessage rtn = adminCommand( SequoiadbConstants.CMD_NAME_RM_PROCEDURE,
				                       0, 0, 0, -1, newobj,
				                       null, null, null );
		int flags = rtn.getFlags();
		if (flags != 0)
			throw new BaseException(flags);
	}
	
	/**
	 * @fn void listProcedures ( BSONObject condition )
     * @brief List the store procedures.
     * @param condition The condition of list eg: {"name":"sum"}
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor listProcedures ( BSONObject condition ) throws BaseException
	{
		return getList(SDB_LIST_STOREPROCEDURES, 0, 0, 0,
				       -1, condition, null, null, null);
	}
	
	/**
	 * @fn Sequoiadb.SptEvalResult evalJS ( String code )
     * @brief Eval javascript code.
     * @param code The javasript code
     * @return The result of the eval operation, including the return value type,
     *         the return data and the error message. If succeed to eval, error message is null,
     *         and we can extract the eval result from the return cursor and return type,
     *         if not, the return cursor and the return type are null, we can extract
     *         the error mssage for more detail. 
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Sequoiadb.SptEvalResult evalJS ( String code ) throws BaseException
	{
		if ( code == null || code.equals("") ){
			throw new BaseException("SDB_INVALIDARG");
		}
		SptEvalResult evalResult = new Sequoiadb.SptEvalResult();
		BSONObject newObj = new BasicBSONObject();
		Code codeObj = new Code( code );
		newObj.put(SequoiadbConstants.FIELD_NAME_FUNC, codeObj);
		newObj.put(SequoiadbConstants.FMP_FUNC_TYPE, FMP_FUNC_TYPE_JS);
		
		SDBMessage rtn = adminCommandEval(SequoiadbConstants.CMD_NAME_EVAL,
				                      0,0,0,-1,newObj,
				                      null,null,null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			List<BSONObject> objList = rtn.getObjectList();
			if (objList.size() > 0){
				evalResult.errmsg = rtn.getObjectList().get(0);
			}
			return evalResult;
		}else{
			long typeValue = rtn.getNumReturned();
			evalResult.returnType = Sequoiadb.SptReturnType.getTypeByValue((int)typeValue);
			evalResult.cursor = new DBCursor(rtn, this);
			return evalResult;
		}
	}
	
	/**
	 * @fn void backupOffline ( BSONObject options )
     * @brief Backup the whole database or specifed replica group.
     * @param options Contains a series of backup configuration infomations. 
     *        Backup the whole cluster if null. The "options" contains 5 options as below. 
     *        All the elements in options are optional. 
     *        eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", 
     *             "Name":"backupName", "Description":description, "EnsureInc":true, "OverWrite":true}
     *<ul>
     *<li>GroupID     : The id(s) of replica group(s) which to be backuped
     *<li>GroupName   : The name(s) of replica group(s) which to be backuped
     *<li>Name        : The name for the backup
     *<li>Path        : The backup path, if not assign, use the backup path assigned in the configuration file,
     *                  the path support to use wildcard(%g/%G:group name, %h/%H:host name, %s/%S:service name).
     *                  e.g.  {Path:"/opt/sequoiadb/backup/%g"}
     *<li>isSubDir    : Whether the path specified by paramer "Path" is a subdirectory of
     *                  the path specified in the configuration file, default to be false
     *<li>Prefix      : The prefix of name for the backup, default to be null. e.g. {Prefix:"%g_bk_"}
     *<li>EnableDateDir : Whether turn on the feature which will create subdirectory named to
     *                    current date like "YYYY-MM-DD" automatically, default to be false             
     *<li>Description : The description for the backup
     *<li>EnsureInc   : Whether turn on increment synchronization, default to be false
     *<li>OverWrite   : Whether overwrite the old backup file with the same name, default to be false
     *</ul>
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void backupOffline ( BSONObject options ) throws BaseException
	{	
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_BACKUP_OFFLINE,
				                      0,0,0,-1,options,
				                      null,null,null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}
	
	/**
	 * @fn DBCursor listBackup ( BSONObject options, BSONObject matcher,
			                     BSONObject selector, BSONObject orderBy )
     * @brief List the backups.
     * @param options Contains configuration infomations for remove backups, list all the backups in the default backup path if null.
     *        The "options" contains 3 options as below. All the elements in options are optional. 
     *        eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}
     * <ul>
     * <li>GroupName   : Assign the backups of specifed replica groups to be list
     * <li>Path        : Assign the backups in specifed path to be list, if not assign, use the backup path asigned in the configuration file
     * <li>Name        : Assign the backups with specifed name to be list
     * </ul>
     * @param matcher The matching rule, return all the documents if null
     * @param selector The selective rule, return the whole document if null
     * @param orderBy The ordered rule, never sort if null
     * @return the DBCursor of the backup or null while having no backup infonation. 
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor listBackup ( BSONObject options, BSONObject matcher,
			                     BSONObject selector, BSONObject orderBy) throws BaseException
	{
		if ( null != options ){
			for ( String key : options.keySet() ){
				if ( key.equals(SequoiadbConstants.FIELD_NAME_GROUPNAME)
						|| key.equals(SequoiadbConstants.FIELD_NAME_NAME)
						|| key.equals(SequoiadbConstants.FIELD_NAME_PATH) ){
					continue ;
				}
				else{
					throw new BaseException("SDB_INVALIDARG", key);
				}
			}
		}
		
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_LIST_BACKUP,
				                      0,0,0,-1,matcher,
				                      selector,orderBy,options);
		DBCursor cursor = null;
		int flags = rtn.getFlags();
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC) {
				return cursor;
			} else {
				throw new BaseException(flags, matcher, selector, orderBy, options);
			}
		}
		cursor = new DBCursor(rtn, this);
		return cursor;
	}

	/**
	 * @fn void removeBackup ( BSONObject options )
     * @brief Remove the backups.
     * @param options Contains configuration infomations for remove backups, remove all the backups in the default backup path if null.
     *                The "options" contains 3 options as below. All the elements in options are optional.
     *                eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}
     *<ul>
     *<li>GroupName   : Assign the backups of specifed replica grouops to be remove
     *<li>Path        : Assign the backups in specifed path to be remove, if not assign, use the backup path asigned in the configuration file
     *<li>Name        : Assign the backups with specifed name to be remove
     *</ul>
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void removeBackup ( BSONObject options ) throws BaseException
	{
		if ( null != options ){
			for ( String key : options.keySet() ){
				if ( key.equals(SequoiadbConstants.FIELD_NAME_GROUPNAME)
						|| key.equals(SequoiadbConstants.FIELD_NAME_NAME)
						|| key.equals(SequoiadbConstants.FIELD_NAME_PATH) ){
					continue ;
				}
				else{
					throw new BaseException("SDB_INVALIDARG");
				}
			}
		}
		
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_REMOVE_BACKUP,
				                      0,0,0,-1,options,
				                      null,null,null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}
	
	/**
	 * @fn DBCursor listTasks ( BSONObject matcher, BSONObject selector,
	 *		                    BSONObject orderBy, BSONObject hint )
     * @brief List the tasks.
     * @param matcher The matching rule, return all the documents if null
     * @param selector The selective rule, return the whole document if null
     * @param orderBy The ordered rule, never sort if null
     * @param hint The hint, automatically match the optimal hint if null
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor listTasks ( BSONObject matcher, BSONObject selector,
			                BSONObject orderBy, BSONObject hint ) throws BaseException {
	
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_LIST_TASK,
				                      0, 0, 0, -1, matcher,
				                      selector, orderBy, hint);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, matcher,
                                    selector, orderBy, hint);
		}
		DBCursor cursor = null;
		cursor = new DBCursor(rtn, this);
		return cursor;
	}
	
	/**
	 * @fn DBCursor waitTasks ( BSONObject condition, BSONObject selector,
	 *		                    BSONObject orderBy, BSONObject hint )
     * @brief Wait the tasks to finish.
     * @param taskIDs The array of task id
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void waitTasks ( long[] taskIDs ) throws BaseException {
		if ( taskIDs == null || taskIDs.length == 0)
			throw new BaseException("SDB_INVALIDARG", taskIDs);
		BSONObject newObj = new BasicBSONObject();
		BSONObject subObj = new BasicBSONObject();
		BSONObject list = new BasicBSONList();
		for(int i = 0; i < taskIDs.length; i++){
			list.put(Integer.toString(i), taskIDs[i]);
		}
		subObj.put("$in", list);
		newObj.put(SequoiadbConstants.FIELD_NAME_TASKID, subObj);
	
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_WAITTASK,
				                      0, 0, 0, -1, newObj,
				                      null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}
	
	/**
	 * @fn DBCursor cancelTask ( long taskID, boolean isAsync )
     * @brief Cancel the specified task.
     * @param taskID The task id
     * @param isAsync The operation "cancel task" is async or not,
     *                "true" for async, "false" for sync. Default sync.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void cancelTask ( long taskID, boolean isAsync ) throws BaseException {
		if ( taskID <= 0)
			throw new BaseException("SDB_INVALIDARG", taskID, isAsync);
		BSONObject newObj = new BasicBSONObject();
		newObj.put(SequoiadbConstants.FIELD_NAME_TASKID, taskID);
		newObj.put(SequoiadbConstants.FIELD_NAME_ASYNC, isAsync);
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_CANCEL_TASK,
				                      0, 0, 0, -1, newObj,
				                      null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}
	
	/**
	 * @fn void setSessionAttr( BSONObject options )
     * @brief Set the attributes of the current session.
     * @param options  The configuration options for the current session.The options are as below:
     * <ul>
     * <li>PreferedInstance   : indicate which instance to respond read request in current session.
     *                        eg:{"PreferedInstance":"m"/"M"/"s"/"S"/"a"/"A"/1-7}, prefer to choose "read and write instance"/"read only instance"/"anyone instance"/instance1-insatance7,
     *                        default to be {"PreferedInstance":"A"}, means would like to choose anyone instance to respond read request such as query.
     * </li>
     * </ul>
     * @note 1.Option "PreferedInstance" is used to choose which instance for querying in current session.When a new session is built,
     *         it works with default attribute {"PreferedInstance":"A"}. And it will keep the preferred instance for querying in current session
     *         until the session is closed or the data node which belongs to this instance is shut down.
     *       2.If a replica group only has 3 data notes, and we offer a configuraion option {"PreferedInstance":5},
     *         in most cases, it will choose the instance which node 2 is in, the formula is (5-1)%3+1. But, if the selected instance is a "read and write instance",
     *         it will choose next instance.
     *         when offer {"PreferedInstance":1-7}, it will choose "read only instance" first.
     *      
     * @code
     *	Sequoiadb sdb = new Sequoiadb("ubuntu-dev1", 11810, "", ""); // when build object sdb, it means we start session 1
     *  sdb.setSessionAttr(new BasicBSONObject("PreferedInstance", 3)); // choose No.3 instance(assume it exist and it's not a r/w instance) for querying
     *  CollectionSpace cs = sdb.getCollectionSpace("foo");
     *  DBCollection cl = cs.getCollection("bar");
     *  cl.query(); // it will choose No.3 instance to query data in session 1
     *  
     *  Sequoiadb sdb1 = new Sequoiadb("ubuntu-dev2", 11810, "", ""); // build another Sequoiadb object, and we start session 2
     *  sdb1.setSessionAttr(new BasicBSONObject("PreferedInstance", "M")); // choose r/w instance for querying in session 2
     *  CollectionSpace cs1 = sdb.getCollectionSpace("foo");
     *  DBCollection cl1 = cs1.getCollection("bar");
     *  cl1.query(); // it will choose r/w instance to query data in session 2
     *  cl.query(); // it will choose No.3 instance to query data in session 1
     *  
     *  sdb.disconnect(); // close session 1
     *  
     *  Sequoiadb sdb = new Sequoiadb("ubuntu-dev1", 11810, "", ""); // start session 3
     *  CollectionSpace cs = sdb.getCollectionSpace("foo");
     *  DBCollection cl = cs.getCollection("bar");
     *  cl.query(); // it will choose any instance to query data in session 3. Assuming it choise No.4 instance, when we qurey next time,
     *              // unless the node which belongs to NO.4 instance had shut down, it would choose No.4 instance again.
     *  cl.query(); // choose No.4 instance to query again
     * @endcode
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void setSessionAttr( BSONObject options ) throws BaseException {
		if ( null == options || 
		     !options.containsField(SequoiadbConstants.FIELD_NAME_PREFERED_INSTANCE) )
			throw new BaseException( "SDB_INVALIDARG", options );
		BSONObject newObj = new BasicBSONObject();
		Object value = options.get( SequoiadbConstants.FIELD_NAME_PREFERED_INSTANCE );
		int v = PreferInstanceType.INS_SLAVE.getCode();
		if ( value instanceof Integer ) {
			v = (Integer)value;
			if ( v < 1 || v > 7 )
				throw new BaseException( "SDB_INVALIDARG", options );
		} else if ( value instanceof String ) {
			if ( value.equals("M") || value.equals("m") )
				v = PreferInstanceType.INS_MASTER.getCode();
			else if ( value.equals("S") || value.equals("s") )
				v = PreferInstanceType.INS_SLAVE.getCode();
			else if ( value.equals("A") || value.equals("a") )
				v = PreferInstanceType.INS_ANYONE.getCode();
			else
				throw new BaseException( "SDB_INVALIDARG", options );
		} else {
			throw new BaseException( "SDB_INVALIDARG", options );
		}
		newObj.put( SequoiadbConstants.FIELD_NAME_PREFERED_INSTANCE, v );
		SDBMessage rtn = adminCommand( SequoiadbConstants.CMD_NAME_SETSESS_ATTR,
				                       0, 0, 0, -1, newObj,
				                       null, null, null);
		int flags = rtn.getFlags();
		if ( flags != 0 ) {
			throw new BaseException( flags );
		}
	}
	
	/**
	 * @fn void closeAllCursors()
	 * @brief Close all the cursors created in current connection, we can't use those cursors to get
     *        data again.
     * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void closeAllCursors() throws BaseException {
		byte[] request = SDBMessageHelper.buildTransactionRequest(
				SequoiadbConstants.Operation.MSG_BS_INTERRUPTE, endianConvert);
		connection.sendMessage(request);
	}	
	
	/**
	 * @fn DBCursor listReplicaGroups()
	 * @brief List all the replica group.
	 * @return cursor of all collecionspace names
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor listReplicaGroups() throws BaseException {
		return getList(SDB_LIST_GROUPS, 0, 0, -1, -1, null, null,
				null, null);
	}
	
	/**
	 * @fn boolean isDomainExist(String domainName)
	 * @brief Verify the existence of domain.
	 * @param domainName the name of domain
	 * @return True if existed or False if not existed
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public boolean isDomainExist(String domainName) throws BaseException {
		if (null == domainName || domainName.equals(""))
			throw new BaseException("SDB_INVALIDARG", domainName);
		BSONObject matcher = new BasicBSONObject();
		matcher.put(SequoiadbConstants.FIELD_NAME_NAME, domainName);
		DBCursor cursor = getList(SDB_LIST_DOMAINS, matcher, null, null);
		if (null != cursor && cursor.hasNext())
			return true;
		else
			return false;
	}

	/**
	 * @fn Domain createDomain(String domainName, BSONObject options)
	 * @brief Create a domain.
	 * @param domainName The name of the creating domain
	 * @param options The options for the domain. The options are as below:
	 * <ul>
	 * <li>Groups    : the list of the replica groups' names which the domain is going to contain.
     *                 eg: { "Groups": [ "group1", "group2", "group3" ] }
     *                 If this argument is not included, the domain will contain all replica groups in the cluster. 
	 * <li>AutoSplit    : If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
     *                    the data of this collection will be split(hash split) into all the groups in this domain automatically.
     *                    However, it won't automatically split data into those groups which were add into this domain later.
     *                    eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
	 * </ul>
	 * @return the newly created collection space object
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Domain createDomain(String domainName, BSONObject options) throws BaseException {
		if (null == domainName || domainName.equals(""))
			throw new BaseException("SDB_INVALIDARG", domainName, options);
		if (isDomainExist(domainName))
			throw new BaseException("SDB_CAT_DOMAIN_EXIST", domainName);
		
		BSONObject newObj = new BasicBSONObject();
		newObj.put(SequoiadbConstants.FIELD_NAME_NAME, domainName);
		if (null != options)
		{
		   newObj.put(SequoiadbConstants.FIELD_NAME_OPTIONS, options);
		}
		SDBMessage rtn = adminCommand( SequoiadbConstants.CMD_NAME_CREATE_DOMAIN,
				                       0, 0, 0, -1, newObj,
				                       null, null, null);
		int flags = rtn.getFlags();
		if ( flags != 0 ) {
			throw new BaseException( flags );
		}
		return new Domain(this, domainName);
		
	}
	
	/**
	 * @fn void dropDomain(String domainName)
	 * @brief Drop a domain.
	 * @param domainName the name of the domain
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void dropDomain(String domainName) throws BaseException {
		if (null == domainName || domainName.equals(""))
			throw new BaseException("SDB_INVALIDARG", domainName);
		
		BSONObject newObj = new BasicBSONObject();
		newObj.put(SequoiadbConstants.FIELD_NAME_NAME, domainName);
		SDBMessage rtn = adminCommand( SequoiadbConstants.CMD_NAME_DROP_DOMAIN,
				                       0, 0, 0, -1, newObj,
				                       null, null, null);
		int flags = rtn.getFlags();
		if ( flags != 0 ) {
			throw new BaseException( flags );
		}
	}
	
	/**
	 * @fn Domain getDomain(String domainName)
	 * @brief Get the specified domain.
	 * @param domainName the name of the domain
	 * @return the Domain instance
	 * @exception com.sequoiadb.exception.BaseException
	 *            If the domain not exit, throw BaseException with the error type "SDB_CAT_DOMAIN_NOT_EXIST"
	 */
	public Domain getDomain(String domainName)
			throws BaseException {
		if (isDomainExist(domainName)) {
			return new Domain(this, domainName);
		} else {
			throw new BaseException("SDB_CAT_DOMAIN_NOT_EXIST", domainName);
		}
	}
	
	/**
	 * @fn DBCursor listDomains(BSONObject matcher, BSONObject selector,
                                BSONObject orderBy, BSONObject hint)
	 * @brief List domains.
	 * @param matcher the matching rule, return all the documents if null
	 * @param selector the selective rule, return the whole document if null
	 * @param orderBy the ordered rule, never sort if null
	 * @param hint the hint, automatically match the optimal hint if null
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public DBCursor listDomains(BSONObject matcher, BSONObject selector,
                                BSONObject orderBy, BSONObject hint) throws BaseException {
		 	return getList(SDB_LIST_DOMAINS, 0, 0, 0, -1, matcher, selector, orderBy, hint);
	}
	
	/**
	 * @fn ArrayList<String> getReplicaGroupNames()
	 * @brief Get all the replica groups' name.
	 * @return A list of all the replica groups' names.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public ArrayList<String> getReplicaGroupNames() throws BaseException {
		DBCursor cursor = getList(SDB_LIST_GROUPS, 0, 0, -1, -1,
				null, null, null, null);
		if (cursor == null)
			return null;
		ArrayList<String> colList = new ArrayList<String>();
		while (cursor.hasNext()) {
			colList.add(cursor.getNext().get("GroupName").toString());
		}
		return colList;
	}
	
	/**
	 * @fn List<String> getReplicaGroupsInfo()
	 * @brief Get the infomations of the replica groups.
	 * @return A list of informations of the replica groups.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public ArrayList<String> getReplicaGroupsInfo() throws BaseException {
		DBCursor cursor = getList(SDB_LIST_GROUPS, 0, 0, -1, -1, null, null,
				null, null);
		if (cursor == null)
			return null;
		ArrayList<String> colList = new ArrayList<String>();
		while (cursor.hasNext()) {
			colList.add(cursor.getNext().toString());
		}
		return colList;
	}
	
	/**
	 * @fn ReplicaGroup getReplicaGroup(String rgName)
	 * @brief Get replica group by name.
	 * @param rgName
	 *            replica group's name
	 * @return A replica group object or null for not exit.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public ReplicaGroup getReplicaGroup(String rgName)
			throws BaseException {
		BSONObject rg = getDetailByName(rgName);
		if (rg == null)
			return null;
		return new ReplicaGroup(this, rgName);
	}

	/**
	 * @fn ReplicaGroup getReplicaGroup(int rgId)
	 * @brief Get replica group by id.
	 * @param rgId
	 *            replica group id
	 * @return A replica group object or null for not exit.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public ReplicaGroup getReplicaGroup(int rgId) throws BaseException{
		BSONObject rg = getDetailById(rgId);
		if (rg == null)
			return null;
		return new ReplicaGroup(this, rgId);
	}

	/**
	 * @fn ReplicaGroup createReplicaGroup(String rgName)
	 * @brief Create replica group by name.
	 * @param rgName
	 *            replica group's name
	 * @return A replica group object.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public ReplicaGroup createReplicaGroup(String rgName)
			throws BaseException {
		BSONObject rg = new BasicBSONObject();
		rg.put(SequoiadbConstants.FIELD_NAME_GROUPNAME, rgName);
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_CREATE_GROUP,
				0, 0, -1, -1, rg, null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, rgName);
		}
		return getReplicaGroup(rgName);
	}

	/**
	 * @fn void removeReplicaGroup(String rgName)
	 * @brief Remove replica group by name.
	 * @param rgName
	 *            replica group's name
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void removeReplicaGroup(String rgName)
			throws BaseException {
		BSONObject rg = new BasicBSONObject();
		rg.put(SequoiadbConstants.FIELD_NAME_GROUPNAME, rgName);
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_REMOVE_GROUP,
				0, 0, -1, -1, rg, null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, rgName);
		}
	}

	/**
	 * @fn void activateReplicaGroup(String rgName)
	 * @brief Active replica group by name.
	 * @param rgName
	 *            replica group name
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void activateReplicaGroup(String rgName)
			throws BaseException {
		BSONObject rg = new BasicBSONObject();
		rg.put(SequoiadbConstants.FIELD_NAME_GROUPNAME, rgName);
		SDBMessage rtn = adminCommand(SequoiadbConstants.CMD_NAME_ACTIVE_GROUP,
				0, 0, -1, -1, rg, null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, rgName);
		}
	}

	/**
	 * @fn void createReplicaCataGroup(String hostName, int port, String dbPath,
	 *     BSONObject configuration)
	 * @brief Create the replica Catalog group with the given options.
	 * @param hostName
	 *            The host name
	 * @param port
	 *            The port
	 * @param dbpath
	 *            The database path
	 * @param configure
	 *            The configure options
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void createReplicaCataGroup(String hostName, int port,
			String dbPath, Map<String, String> configure) {
		String commandString = SequoiadbConstants.CMD_NAME_CREATE_CATA_GROUP;
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_HOST, hostName);
		obj.put(SequoiadbConstants.PMD_OPTION_SVCNAME, Integer.toString(port));
		obj.put(SequoiadbConstants.PMD_OPTION_DBPATH, dbPath);
		if (configure != null) {
			for (String key : configure.keySet()) {
				if (key.equals(SequoiadbConstants.FIELD_NAME_HOST)
						|| key.equals(SequoiadbConstants.PMD_OPTION_SVCNAME)
						|| key.equals(SequoiadbConstants.PMD_OPTION_DBPATH)) {
					continue;
				}
				obj.put(key, configure.get(key).toString());
			}
		}
		SDBMessage rtn = adminCommand(commandString, 0, 0, -1, -1, obj, null,
				null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}

	DBCursor getList(int listType, int flag, long reqID, long skipNum,
			long returnNum, BSONObject query, BSONObject selector,
			BSONObject order, BSONObject hint) throws BaseException {
		String command = "";
		switch (listType) {
		case SDB_LIST_CONTEXTS:
			command = SequoiadbConstants.CMD_NAME_LIST_CONTEXTS;
			break;
		case SDB_LIST_CONTEXTS_CURRENT:
			command = SequoiadbConstants.CMD_NAME_LIST_CONTEXTS_CURRENT;
			break;
		case SDB_LIST_SESSIONS:
			command = SequoiadbConstants.CMD_NAME_LIST_SESSIONS;
			break;
		case SDB_LIST_SESSIONS_CURRENT:
			command = SequoiadbConstants.CMD_NAME_LIST_SESSIONS_CURRENT;
			break;
		case SDB_LIST_COLLECTIONS:
			command = SequoiadbConstants.CMD_NAME_LIST_COLLECTIONS;
			break;
		case SDB_LIST_COLLECTIONSPACES:
			command = SequoiadbConstants.CMD_NAME_LIST_COLLECTIONSPACES;
			break;
		case SDB_LIST_STORAGEUNITS:
			command = SequoiadbConstants.CMD_NAME_LIST_STORAGEUNITS;
			break;
		case SDB_LIST_GROUPS:
			command = SequoiadbConstants.CMD_NAME_LIST_GROUPS;
			break;
		case SDB_LIST_STOREPROCEDURES:
			command = SequoiadbConstants.CMD_NAME_LIST_PROCEDURES;
			break;
		case SDB_LIST_DOMAINS:
			command = SequoiadbConstants.CMD_NAME_LIST_DOMAINS;
			break;
		case SDB_LIST_TASKS:
			command = SequoiadbConstants.CMD_NAME_LIST_TASKS;
			break;
		case SDB_LIST_CS_IN_DOMAIN:
			command = SequoiadbConstants.CMD_NAME_LIST_CS_IN_DOMAIN;
			break;
		case SDB_LIST_CL_IN_DOMAIN:
			command = SequoiadbConstants.CMD_NAME_LIST_CL_IN_DOMAIN;
			break;
		default:
			throw new BaseException("SDB_INVALIDARG");
		}

		SDBMessage rtn = adminCommand(command, flag, reqID, skipNum, returnNum,
				query, selector, order, hint);
		int flags = rtn.getFlags();
		if (flags != 0) {
			if (flags == SequoiadbConstants.SDB_DMS_EOC) {
				return null;
			} else {
				throw new BaseException(flags, query, selector, order, hint);
			}
		}
		return new DBCursor(rtn, this);

	}

	String getUserName() {
		return userName;
	}

	String getPassword() {
		return password;
	}

	BSONObject getDetailByName(String name) throws BaseException {
		BSONObject condition = new BasicBSONObject();
		condition.put(SequoiadbConstants.FIELD_NAME_GROUPNAME, name);
		DBCursor shardsCursor = getList(Sequoiadb.SDB_LIST_GROUPS, 0, 0, -1,
				-1, condition, null, null, null);
		if (shardsCursor == null || !shardsCursor.hasNext())
			return null;
		return shardsCursor.getNext();
	}

	BSONObject getDetailById(int id) throws BaseException {
		BSONObject condition = new BasicBSONObject();
		condition.put(SequoiadbConstants.FIELD_NAME_GROUPID, id);
		DBCursor shardsCursor = getList(Sequoiadb.SDB_LIST_GROUPS, 0, 0, -1,
				-1, condition, null, null, null);
		if (shardsCursor == null || !shardsCursor.hasNext())
			return null;
		return shardsCursor.getNext();
	}

	void releaseResource(){
		connection.shrinkBuffer();
	}
	
	private void initConnection(ConfigOptions options) throws BaseException {
		if (options == null)
			throw new BaseException("SDB_INVALIDARG");
		connection = new ConnectionTCPImpl(serverAddress, options);
		connection.initialize();
	}

    SDBMessage adminCommand(String commandString, int flag, long reqID,
			long skipNum, long returnNum, BSONObject query,
			BSONObject selector, BSONObject order, BSONObject hint)
			throws BaseException {
		BSONObject dummyObj = new BasicBSONObject();
		SDBMessage sdbMessage = new SDBMessage();

		if (query == null)
			sdbMessage.setMatcher(dummyObj);
		else
			sdbMessage.setMatcher(query);
		if (selector == null)
			sdbMessage.setSelector(dummyObj);
		else
			sdbMessage.setSelector(selector);
		if (order == null)
			sdbMessage.setOrderBy(dummyObj);
		else
			sdbMessage.setOrderBy(order);
		if (hint == null)
			sdbMessage.setHint(dummyObj);
		else
			sdbMessage.setHint(hint);

		sdbMessage.setCollectionFullName(SequoiadbConstants.ADMIN_PROMPT
				+ commandString);

		sdbMessage.setVersion(1);
		sdbMessage.setW((short) 0);
		sdbMessage.setPadding((short) 0);
		sdbMessage.setFlags(flag);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		sdbMessage.setRequestID(reqID);
		sdbMessage.setSkipRowsCount(skipNum);
		sdbMessage.setReturnRowsCount(returnNum);

		byte[] request = SDBMessageHelper.buildQueryRequest(sdbMessage,
				endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);

		return rtnSDBMessage;
	}

	private SDBMessage adminCommandEval(String commandString, int flag, long reqID,
			long skipNum, long returnNum, BSONObject query,
			BSONObject selector, BSONObject order, BSONObject hint)
			throws BaseException {
		BSONObject dummyObj = new BasicBSONObject();
		SDBMessage sdbMessage = new SDBMessage();

		if (query == null)
			sdbMessage.setMatcher(dummyObj);
		else
			sdbMessage.setMatcher(query);
		if (selector == null)
			sdbMessage.setSelector(dummyObj);
		else
			sdbMessage.setSelector(selector);
		if (order == null)
			sdbMessage.setOrderBy(dummyObj);
		else
			sdbMessage.setOrderBy(order);
		if (hint == null)
			sdbMessage.setHint(dummyObj);
		else
			sdbMessage.setHint(hint);

		sdbMessage.setCollectionFullName(SequoiadbConstants.ADMIN_PROMPT
				+ commandString);

		sdbMessage.setVersion(1);
		sdbMessage.setW((short) 0);
		sdbMessage.setPadding((short) 0);
		sdbMessage.setFlags(flag);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		sdbMessage.setRequestID(reqID);
		sdbMessage.setSkipRowsCount(skipNum);
		sdbMessage.setReturnRowsCount(returnNum);

		byte[] request = SDBMessageHelper.buildQueryRequest(sdbMessage,
				endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractEvalReply(byteBuffer);

		return rtnSDBMessage;
	}
	
	private SDBMessage createCS(String csName, BSONObject options)
			throws BaseException {
		String commandString = SequoiadbConstants.ADMIN_PROMPT
				+ SequoiadbConstants.CREATE_CMD + " "
				+ SequoiadbConstants.COLSPACE;
		BSONObject cObj = new BasicBSONObject();
		BSONObject dummyObj = new BasicBSONObject();
		SDBMessage sdbMessage = new SDBMessage();

		cObj.put(SequoiadbConstants.FIELD_NAME_NAME, csName);
		if (null != options)
		    cObj.putAll(options);
		sdbMessage.setMatcher(cObj);
		sdbMessage.setCollectionFullName(commandString);

		sdbMessage.setVersion(0);
		sdbMessage.setW((short) 0);
		sdbMessage.setPadding((short) 0);
		sdbMessage.setFlags(0);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		sdbMessage.setRequestID(0);
		sdbMessage.setSkipRowsCount(-1);
		sdbMessage.setReturnRowsCount(-1);
		sdbMessage.setSelector(dummyObj);
		sdbMessage.setOrderBy(dummyObj);
		sdbMessage.setHint(dummyObj);

		byte[] request = SDBMessageHelper.buildQueryRequest(sdbMessage,
				endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		if (endianConvert) {
			byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
		} else {
			byteBuffer.order(ByteOrder.BIG_ENDIAN);
		}
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);

		return rtnSDBMessage;
	}
	
	private boolean requestSysInfo() {
		byte[] request = SDBMessageHelper.buildSysInfoRequest();
		connection.sendMessage(request);
		boolean endianConvert = SDBMessageHelper
				.msgExtractSysInfoReply(connection.receiveSysInfoMsg(128));
		return endianConvert;
	}

	private void sendKillContextMsg() {
		if (connection == null )
			throw new BaseException("SDB_NETWORK");
		long[] contextIds = new long[] { -1 };
		byte[] request = SDBMessageHelper.buildKillCursorMsg(0, contextIds,
				endianConvert);
		connection.sendMessage(request);

		ByteBuffer byteBuffer = connection.receiveMessage(endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		int flags = rtnSDBMessage.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}
	
	/**
	 * @class SptEvalResult
	 * @brief Class for executing stored procedure result. 
	 */
	public static class SptEvalResult {
		private SptReturnType returnType;
		private BSONObject errmsg;
		private DBCursor cursor;
		
		/**
		 * @fn SptEvalResult ()
	     * @brief Constructor.
		 */
		public SptEvalResult(){
			returnType = null;
			errmsg = null;
			cursor = null;
		}
		
		/**
		 * @fn setReturnType ()
	     * @brief Set return type.
		 */
		public void setReturnType(SptReturnType returnType){
			this.returnType = returnType;  
		}
		
		/**
		 * @fn SptReturnType getReturnType ()
	     * @brief Get return type.
		 */
		public SptReturnType getReturnType(){
			return returnType;
		}
		
		/**
		 * @fn setErrMsg ()
	     * @brief Set error type.
		 */
		public void setErrMsg(BSONObject errmsg){
			this.errmsg = errmsg;
		}
		
		/**
		 * @fn BSONObject getErrMsg ()
	     * @brief Get error type.
		 */
		public BSONObject getErrMsg(){
			return errmsg;
		}
		
		/**
		 * @fn setCursor ()
	     * @brief Set result cursor.
		 */
		public void setCursor(DBCursor cursor){
			this.cursor = cursor;
		}
		
		/**
		 * @fn DBCursor getCursor ()
	     * @brief Get result cursor.
		 */
		public DBCursor getCursor(){
			return cursor;
		}
	}

	public enum SptReturnType {
		TYPE_VOID(0),
		TYPE_STR(1),
		TYPE_NUMBER(2),
		TYPE_OBJ(3),
		TYPE_BOOL(4),
		TYPE_RECORDSET(5),
		TYPE_CS(6),
		TYPE_CL(7),
		TYPE_RG(8),
		TYPE_RN(9);
	   
		private int typeValue;
	   
		private SptReturnType(int typeValue) {
			this.typeValue = typeValue;
		}
	   
		public int getTypeValue() {
			return typeValue;
		}
	   
		public static SptReturnType getTypeByValue(int typeValue) {
			SptReturnType retType = null;
			for (SptReturnType rt : values()) {
				if (rt.getTypeValue() == typeValue) {
					retType = rt;
					break;
				}
			}
			return retType;
		}
	}
}
