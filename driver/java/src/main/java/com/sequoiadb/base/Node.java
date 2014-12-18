package com.sequoiadb.base;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.sequoiadb.exception.BaseException;
import com.sequoiadb.util.SDBMessageHelper;

/**
 * @class Node
 * @brief Database operation interfaces of node.This class takes the place of class "replicaNode".
 * @note We use concept "node" instead of "replica node",
 *       and change the class name "ReplicaNode" to "Node".
 */
public class Node {
	private String hostName;
	private int port;
	private String nodeName;
	private int id;
	private ReplicaGroup rg;
	private Sequoiadb ddb;
	private NodeStatus status;

	Node(String hostName, int port, int nodeId, ReplicaGroup rg) {
		this.rg = rg;
		this.hostName = hostName;
		this.port = port;
		this.nodeName = hostName + SequoiadbConstants.NODE_NAME_SEP + port;
		this.id = nodeId;
	}
	
	/*!
	 * enum Node::NodeStatus
	 */
	public static enum NodeStatus {
		SDB_NODE_ALL(1),
		SDB_NODE_ACTIVE(2),
		SDB_NODE_INACTIVE(3),
		SDB_NODE_UNKNOWN(4);
		private final int key;
		private NodeStatus(int key){
			this.key = key;
		}
		public int getKey() {
			return key;
		}
		public static NodeStatus getByKey(int key) {
			NodeStatus nodeStatus = NodeStatus.SDB_NODE_ALL;
			for (NodeStatus status : NodeStatus.values()) {
				if (status.getKey() == key) {
					nodeStatus = status;
					break;
				}
			}
			return nodeStatus;
		}
	}

	/**
	 * @fn int getNodeId()
	 * @brief Get current node's id.
	 * @return Current node's id.
	 */
	public int getNodeId() {
		return id;
	}

	/**
	 * @fn ReplicaGroup getReplicaGroup()
	 * @brief Get current node's parent replica group.
	 * @return Current node's parent replica group.
	 */
	public ReplicaGroup getReplicaGroup() {
		return rg;
	}

	/**
	 * @fn void disconnect()
	 * @brief Disconnect from current node.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void disconnect() throws BaseException {
		ddb.disconnect();
	}

	/**
	 * @fn Sequoiadb connect ()
	 * @brief Connect to current node with the same username and password.
	 * @return The Sequoiadb object of current node.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Sequoiadb connect() throws BaseException {
		ddb = new Sequoiadb(hostName, port, rg.getSequoiadb().getUserName(),
				rg.getSequoiadb().getPassword());
		return ddb;
	}

	/**
	 * @fn Sequoiadb connect(String username, String password)
	 * @brief Connect to current node with username and password.
	 * @param username
	 * 			user name
	 * @param password
	 * 			pass word
	 * @return The Sequoiadb object of current node.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Sequoiadb connect(String username, String password) throws BaseException {
		ddb = new Sequoiadb(hostName, port, username, password);
		return ddb;
	}

	/**
	 * @fn Sequoiadb getSdb()
	 * @brief Get the Sequoiadb of current node.
	 * @return The Sequoiadb object of current node.
	 */
	public Sequoiadb getSdb() {
		return ddb;
	}

	/**
	 * @fn String getHostName()
	 * @brief Get the hostname of current node.
	 * @return Hostname of current node.
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * @fn int getPort()
	 * @brief Get the port of current node.
	 * @return The port of current node.
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @fn String getNodeName()
	 * @brief Get the name of current node.
	 * @return The name of current node.
	 */
	public String getNodeName() {
		return nodeName;
	}

	/**
	 * @fn NodeStatus getStatus()
	 * @brief Get the status of current node.
	 * @return The status of current node.
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public NodeStatus getStatus() throws BaseException {
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_GROUPID, rg.getId());
		obj.put(SequoiadbConstants.FIELD_NAME_NODEID, id);
		String commandString = SequoiadbConstants.SNAP_CMD + " "
				+ SequoiadbConstants.DATABASE;
		SDBMessage rtn = adminCommand(commandString, obj);
		int flags = rtn.getFlags();
		if (flags != 0) {
			if (flags == new BaseException("SDB_NET_CANNOT_CONNECT")
					.getErrorCode()) {
				status = NodeStatus.SDB_NODE_INACTIVE;
				return status;
			} else {
				throw new BaseException(flags);
			}
		}
		status = NodeStatus.SDB_NODE_ACTIVE;
		return status;
	}

	/**
	 * @fn void start()
	 * @brief Start current node in database.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void start() throws BaseException {
		startStop(true);
	}

	/**
	 * @fn void stop()
	 * @brief Stop current node in database.
	 * @return void
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void stop() throws BaseException {
		startStop(false);
	}

	private void startStop(boolean status) {
		BSONObject config = new BasicBSONObject();
		config.put(SequoiadbConstants.FIELD_NAME_HOST, hostName);
		config.put(SequoiadbConstants.PMD_OPTION_SVCNAME,
				Integer.toString(port));
		SDBMessage rtn = adminCommand(
				status ? SequoiadbConstants.CMD_NAME_STARTUP_NODE
						: SequoiadbConstants.CMD_NAME_SHUTDOWN_NODE, config);
		int flags = rtn.getFlags();
		if(flags != 0) {
			throw new BaseException(flags, hostName, port);
		}
	}

	private SDBMessage adminCommand(String commandString, BSONObject obj)
			throws BaseException {
		BSONObject dummyObj = new BasicBSONObject();
		SDBMessage sdbMessage = new SDBMessage();
		sdbMessage.setMatcher(obj);
		sdbMessage.setCollectionFullName(SequoiadbConstants.ADMIN_PROMPT + commandString);

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

		boolean endianConver = this.rg.getSequoiadb().endianConvert;
		byte[] request = SDBMessageHelper.buildQueryRequest(sdbMessage,
				endianConver);
		this.rg.getSequoiadb().getConnection().sendMessage(request);

		ByteBuffer byteBuffer = this.rg.getSequoiadb().getConnection().receiveMessage(endianConver);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);

		return rtnSDBMessage;
	}

}
