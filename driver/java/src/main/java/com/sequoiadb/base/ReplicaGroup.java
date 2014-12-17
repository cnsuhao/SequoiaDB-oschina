package com.sequoiadb.base;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import com.sequoiadb.exception.BaseException;
import com.sequoiadb.net.IConnection;
import com.sequoiadb.util.SDBMessageHelper;

/**
 * @class ReplicaGroup
 * @brief Database operation interfaces of replica group.
 */
public class ReplicaGroup {
	private String name;
	private int id;
	private Sequoiadb sequoiadb;
	private boolean isCataRG;


	/**
	 * @fn Sequoiadb getSequoiadb()
	 * @brief Get current replica group's Sequoiadb.
	 * @return the current replica group's Sequoiadb
	 */
	public Sequoiadb getSequoiadb() {
		return sequoiadb;
	}

	/**
	 * @fn int getId()
	 * @brief Get current replica group's id.
	 * @return the current replica group's id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @fn String getGroupName()
	 * @brief Get current replica group's name.
	 * @return the current replica group's name
	 */
	public String getGroupName() {
		return name;
	}

	ReplicaGroup(Sequoiadb sdb, int id) {
		this.sequoiadb = sdb;
		this.id = id;
		BSONObject group = sdb.getDetailById(id);
		this.name = group.get(SequoiadbConstants.FIELD_NAME_GROUPNAME)
				.toString();
		this.isCataRG = name.equals(Sequoiadb.CATALOG_GROUP_NAME);
	}
	
	ReplicaGroup(Sequoiadb sdb, String name) {
		this.sequoiadb = sdb;
		this.name = name;
		BSONObject group = sdb.getDetailByName(name);
		this.isCataRG = (name == Sequoiadb.CATALOG_GROUP_NAME);
		this.id = Integer.parseInt(group.get(
				SequoiadbConstants.FIELD_NAME_GROUPID).toString());
	}
	
	/**
	 * @fn int getNodeNum(Node.NodeStatus status)
	 * @brief Get the amount of the nodes with the specified status.
	 * @param status
	 * 			Node.NodeStatus
	 * @return the amount of the nodes with the specified status
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public int getNodeNum(Node.NodeStatus status) throws BaseException {
		BSONObject group = sequoiadb.getDetailById(id);
		try {
			Object obj = group.get(SequoiadbConstants.FIELD_NAME_GROUP);
			if (obj == null)
				return 0;
			BasicBSONList list = (BasicBSONList) obj;
			return list.size();
		} catch (BaseException e) {
			throw e;
		} catch (Exception e) {
			throw new BaseException("SDB_SYS", e);
		}
	}

	/**
	 * @fn BSONObject getDetail()
	 * @brief Get detail info of current replicaGoup
	 * @return the detail info
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public BSONObject getDetail() throws BaseException {
		return sequoiadb.getDetailById(id);
	}

	/**
	 * @fn Node getMaster()
	 * @brief Get the master node of current replica group.
	 * @return the master node
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Node getMaster() throws BaseException {
		BSONObject group = sequoiadb.getDetailById(id);
		BSONObject primaryData = null;
		Object nodeId = null;
		Object primaryNodeObj = group
				.get(SequoiadbConstants.FIELD_NAME_PRIMARY);
		if (primaryNodeObj == null)
			throw new BaseException("SDB_CLS_NODE_NOT_EXIST");
		Object groupInfoObj = group.get(SequoiadbConstants.FIELD_NAME_GROUP);
		if (groupInfoObj == null)
			return null;
		BasicBSONList nodeInfos = (BasicBSONList) groupInfoObj;
		for (Object nodeInfoObj : nodeInfos) {
			BSONObject nodeInfo = (BSONObject) nodeInfoObj;
			nodeId = nodeInfo.get(SequoiadbConstants.FIELD_NAME_NODEID);
			if (nodeId == null)
				throw new BaseException("SDB_SYS");
			if (nodeId.equals(primaryNodeObj)) {
				primaryData = nodeInfo;
				break;
			}
		}
		if (primaryData != null) {
			nodeId = primaryData.get(SequoiadbConstants.FIELD_NAME_NODEID);
			String hostName = primaryData.get(
					SequoiadbConstants.FIELD_NAME_HOST).toString();
			int port = getNodePort(primaryData);
			return new Node(hostName, port, Integer.parseInt(nodeId
					.toString()), this);
		}
		return null;
	}

	/**
	 * @fn Node getSlave()
	 * @brief Get the random slave of current replica group.
	 * @return the slave node
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Node getSlave() throws BaseException {
		BSONObject group = sequoiadb.getDetailById(id);
		if (group == null)
			return null;
		List<BSONObject> slaves = new ArrayList<BSONObject>();
		BSONObject primaryData = null;
		Object primaryNodeObj = group
				.get(SequoiadbConstants.FIELD_NAME_PRIMARY);
		if (primaryNodeObj == null)
			throw new BaseException("SDB_CLS_NODE_NOT_EXIST");
		Object groupInfoObj = group.get(SequoiadbConstants.FIELD_NAME_GROUP);
		if (groupInfoObj == null)
			return null;
		BasicBSONList nodeInfos = (BasicBSONList) groupInfoObj;
		for (Object nodeInfoObj : nodeInfos) {
			BSONObject nodeInfo = (BSONObject) nodeInfoObj;
			Object nodeId = nodeInfo.get(SequoiadbConstants.FIELD_NAME_NODEID);
			if (nodeId == null)
				throw new BaseException("SDB_SYS");
			if (nodeId.equals(primaryNodeObj)) {
				primaryData = nodeInfo;
			} else {
				slaves.add(nodeInfo);
			}
		}
		if (slaves.size() != 0) {
			Random rand = new Random();
			BSONObject randNode = slaves.get(rand.nextInt(slaves.size()));
			int nodeId = Integer.parseInt(randNode.get(
					SequoiadbConstants.FIELD_NAME_NODEID).toString());
			String hostName = randNode.get(SequoiadbConstants.FIELD_NAME_HOST)
					.toString();
			int port = getNodePort(randNode);
			return new Node(hostName, port, nodeId, this);
		} else if (primaryData != null) {
			int nodeId = Integer.parseInt(primaryData.get(
					SequoiadbConstants.FIELD_NAME_NODEID).toString());
			String hostName = primaryData.get(
					SequoiadbConstants.FIELD_NAME_HOST).toString();
			int port = getNodePort(primaryData);
			return new Node(hostName, port, nodeId, this);
		} else {
			return null;
		}
	}

	/**
	 * @fn Node getNode(String nodeName)
	 * @brief Get node by node's name (IP:PORT).
	 * @param nodeName
	 * 			The name of the node
	 * @return the specified node
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Node getNode(String nodeName) throws BaseException {
		String[] temp = nodeName.split(":");
		if (temp.length != 2) {
			throw new BaseException("SDB_INVALIDARG", nodeName);
		}
		BSONObject group = sequoiadb.getDetailById(id);
		if (group == null)
			return null;
		try {
			Object nodeId = null;
			Object hostName = null;
			int port = -1;
			Object list = group.get(SequoiadbConstants.FIELD_NAME_GROUP);
			if (list == null)
				return null;
			BasicBSONList nodeInfos = (BasicBSONList) list;
			if (nodeInfos.size() == 0)
				return null;
			for (Object nodeInfoObj : nodeInfos) {
				BSONObject nodeInfo = (BSONObject) nodeInfoObj;
				nodeId = nodeInfo.get(SequoiadbConstants.FIELD_NAME_NODEID);
				hostName = nodeInfo.get(SequoiadbConstants.FIELD_NAME_HOST);
				port = getNodePort(nodeInfo);
				if (nodeId == null || hostName == null)
					throw new BaseException("SDB_SYS");
				String hostName2 = InetAddress.getByName(temp[0]).toString()
						.split("/")[1];
				hostName = InetAddress.getByName(hostName.toString())
						.toString().split("/")[1];
				if (hostName.equals(hostName2)
						&& port == Integer.parseInt((temp[1]))) {
					return new Node(hostName2, port,
							Integer.parseInt(nodeId.toString()), this);
				}
			}
		} catch (Exception e) {
			throw new BaseException("SDB_SYS", nodeName);
		}
		return null;
	}

	/**
	 * @fn Node getNode(String hostName, int port)
	 * @brief Get node by hostName and port.
	 * @param hostName
	 * 			host name
	 * @param port
	 * 			port
	 * @return the Node object
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Node getNode(String hostName, int port) throws BaseException {
		BSONObject group = sequoiadb.getDetailById(id);
		try {
			Object list = group.get(SequoiadbConstants.FIELD_NAME_GROUP);
			if (list == null)
				return null;
			BasicBSONList nodeInfos = (BasicBSONList) (list);
			if (nodeInfos.size() == 0)
				return null;
			Object nodeIdObj = null;
			int nodeId = -1;
			BSONObject nodeInfo = null;
			int nodePort = -1;
			for (Object obj : nodeInfos) {
				nodeInfo = (BSONObject) obj;
				nodeIdObj = nodeInfo.get(SequoiadbConstants.FIELD_NAME_NODEID);
				if (nodeIdObj == null)
					throw new BaseException("SDB_SYS");
				nodeId = Integer.parseInt(nodeIdObj.toString());
				hostName = InetAddress.getByName(hostName).toString()
						.split("/")[1];
				String hostName2 = InetAddress
						.getByName(
								nodeInfo.get(SequoiadbConstants.FIELD_NAME_HOST)
										.toString()).toString().split("/")[1];
				if (hostName2.equals(hostName)) {
					nodePort = getNodePort(nodeInfo);
					if (nodePort == port)
						return new Node(hostName, port, nodeId, this);
				}
			}
		} catch (BaseException e) {
			throw e;
		} catch (Exception e) {
			throw new BaseException("SDB_SYS", hostName, port);
		}
		return null;
	}

	/**
	 * @fn Node createNode(String hostName, int port, String dbPath,
			Map<String, String> configure)
	 * @brief Create node.
	 * @param hostName
	 * 			host name
	 * @param port
	 * 			port
	 * @param dbPath
	 * 			the path for node
	 * @param configure
	 * 			configuration for this operation
	 * @return the created Node object
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Node createNode(String hostName, int port, String dbPath,
			Map<String, String> configure) throws BaseException {
		BSONObject config = new BasicBSONObject();
		config.put(SequoiadbConstants.FIELD_NAME_GROUPNAME, name);
		config.put(SequoiadbConstants.FIELD_NAME_HOST, hostName);
		config.put(SequoiadbConstants.PMD_OPTION_SVCNAME,
				Integer.toString(port));
		config.put(SequoiadbConstants.PMD_OPTION_DBPATH, dbPath);
		if (configure != null)
			for (String key : configure.keySet()) {
				if (key.equals(SequoiadbConstants.FIELD_NAME_GROUPNAME)
						|| key.equals(SequoiadbConstants.FIELD_NAME_HOST)
						|| key.equals(SequoiadbConstants.PMD_OPTION_SVCNAME))
					continue;
				config.put(key, configure.get(key));
			}
		SDBMessage rtn = adminCommand(SequoiadbConstants.CREATE_CMD,
				SequoiadbConstants.NODE, config);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, hostName, port, dbPath, configure);
		}
		return getNode(hostName, port);
	}

	/**
	 * @fn void removeNode(String hostName, int port,
	                       BSONObject configure)
	 * @brief Remove node.
	 * @param hostName
	 * 			host name
	 * @param port
	 * 			port
	 * @param configure
	 * 			configuration for this operation
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void removeNode(String hostName, int port,
			               BSONObject configure) throws BaseException {
		BSONObject config = new BasicBSONObject();
		config.put(SequoiadbConstants.FIELD_NAME_GROUPNAME, name);
		config.put(SequoiadbConstants.FIELD_NAME_HOST, hostName);
		config.put(SequoiadbConstants.PMD_OPTION_SVCNAME,
				Integer.toString(port));
		if (configure != null)
			for (String key : configure.keySet()) {
				if (key.equals(SequoiadbConstants.FIELD_NAME_GROUPNAME)
						|| key.equals(SequoiadbConstants.FIELD_NAME_HOST)
						|| key.equals(SequoiadbConstants.PMD_OPTION_SVCNAME))
					continue;
				config.put(key, configure.get(key));
			}
		SDBMessage rtn = adminCommand(SequoiadbConstants.REMOVE_CMD,
				SequoiadbConstants.NODE, config);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(hostName, port, configure);
		}
	}

	/**
	 * @fn void start()
	 * @brief Start current replica group.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void start() throws BaseException {
		BSONObject groupName = new BasicBSONObject();
		groupName.put(SequoiadbConstants.FIELD_NAME_GROUPNAME, this.name);
		SDBMessage rtn = adminCommand(SequoiadbConstants.ACTIVE_CMD,
				SequoiadbConstants.GROUP, groupName);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, this.name);
		}
	}
	
	/**
	 * @fn void stop()
	 * @brief Stop current replica group.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void stop() throws BaseException {
		BSONObject groupName = new BasicBSONObject();
		groupName.put(SequoiadbConstants.FIELD_NAME_GROUPNAME, this.name);
		SDBMessage rtn = adminCommand(SequoiadbConstants.SHUTDOWN_CMD,
				SequoiadbConstants.GROUP, groupName);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags, this.name);
		}
	}
	
	/**
	 * @fn boolean isCatalog()
	 * @brief Judge whether current replicaGroup is catalog replica group or not.
	 * @return true is while false is not
	 */
	public boolean isCatalog() {
		return isCataRG;
	}

	private int getNodePort(BSONObject node) {
		Object services = node.get(SequoiadbConstants.FIELD_NAME_GROUPSERVICE);
		if (services == null)
			throw new BaseException("SDB_SYS", node);
		BasicBSONList serviceInfos = (BasicBSONList) services;
		if (serviceInfos.size() == 0)
			throw new BaseException("SDB_CLS_NODE_NOT_EXIST");
		int port = -1;
		for (Object obj : serviceInfos) {
			BSONObject service = (BSONObject) obj;
			if (service.get(SequoiadbConstants.FIELD_NAME_SERVICETYPE)
					.toString().equals("0")) {
				port = Integer.parseInt(service.get(
						SequoiadbConstants.FIELD_NAME_SERVICENAME).toString());
				break;
			}
		}
		if (port == -1)
			throw new BaseException("SDB_SYS", node);
		return port;
	}

	private SDBMessage adminCommand(String cmdType, String contextType,
			BSONObject query) throws BaseException {
		IConnection connection = sequoiadb.getConnection();
		// Admin command request
		// long reqId = 0;
		BSONObject dummyObj = new BasicBSONObject();
		SDBMessage sdbMessage = new SDBMessage();
		String commandString = SequoiadbConstants.ADMIN_PROMPT + cmdType + " "
				+ contextType;
		if (query != null)
			sdbMessage.setMatcher(query);
		else
			sdbMessage.setMatcher(dummyObj);
		sdbMessage.setCollectionFullName(commandString);
		sdbMessage.setFlags(0);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		// sdbMessage.setResponseTo(reqId);
		// reqId++;
		sdbMessage.setRequestID(0);
		sdbMessage.setSkipRowsCount(-1);
		sdbMessage.setReturnRowsCount(-1);
		sdbMessage.setSelector(dummyObj);
		sdbMessage.setOrderBy(dummyObj);
		sdbMessage.setHint(dummyObj);

		byte[] request = SDBMessageHelper.buildQueryRequest(sdbMessage, sequoiadb.endianConvert);
		connection.sendMessage(request);
		
		ByteBuffer byteBuffer = connection.receiveMessage(sequoiadb.endianConvert);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		return rtnSDBMessage;
	}
}
