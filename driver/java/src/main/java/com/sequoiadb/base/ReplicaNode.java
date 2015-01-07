package com.sequoiadb.base;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.sequoiadb.base.SequoiadbConstants.Operation;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.util.SDBMessageHelper;

/**
 * @class ReplicaNode
 * @brief Database operation interfaces of replica node.This class will be deprecated in version 2.x.
 *        Use class Node instead of it.
 * @note We use concept "node" instead of "replica node",
 *       and change the class name "ReplicaNode" to "Node".
 * @deprecated 
 * @see Node
 */
public class ReplicaNode {
	private String hostName;
	private int port;
	private String nodeName;
	private int id;
	private ReplicaGroup group;
	private Sequoiadb sdb;
	private NodeStatus status;

	/*!
	 * enum ReplicaNode::NodeStatus
	 */
	public static enum NodeStatus {
		SDB_NODE_ALL(1), SDB_NODE_ACTIVE(2), SDB_NODE_INACTIVE(3), SDB_NODE_UNKNOWN(4);
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
	 * @brief get current replicaNode's id
	 * @return current replicaNode's id
	 */
	public int getNodeId() {
		return id;
	}

	/**
	 * @fn ReplicaGroup getGroup()
	 * @brief get current replicaNode's parent replicaGroup
	 * @return current replicaNode's parent replicaGroup
	 */
	public ReplicaGroup getGroup() {
		return group;
	}

	ReplicaNode(String hostName, int port, int nodeId, ReplicaGroup group) {
		this.group = group;
		this.hostName = hostName;
		this.port = port;
		this.nodeName = hostName + SequoiadbConstants.NODE_NAME_SEP + port;
		this.id = nodeId;
	}

	/**
	 * @fn void disconnect()
	 * @brief disconnect from current replicaNode
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void disconnect() throws BaseException {
		sdb.disconnect();
	}

	/**
	 * @fn Sequoiadb connect ()
	 * @brief Connect to current replicaNode with the same username and password
	 * @return the Sequoiadb of current replicaNode
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Sequoiadb connect() {
		sdb = new Sequoiadb(hostName, port, group.getSequoiadb().getUserName(),
				group.getSequoiadb().getPassword());
		return sdb;
	}

	/**
	 * @fn Sequoiadb connect(String username, String password)
	 * @brief Connect to current replicaNode with username and password
	 * @param username
	 * 			String
	 * @param password
	 * 			String
	 * @return the Sequoiadb of current replicaNode
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public Sequoiadb connect(String username, String password) {
		sdb = new Sequoiadb(hostName, port, username, password);
		return sdb;
	}

	/**
	 * @fn Sequoiadb getSdb()
	 * @brief Get the Sequoiadb of current replicaNode
	 * @return the Sequoiadb of current replicaNode
	 */
	public Sequoiadb getSdb() {
		return sdb;
	}

	/**
	 * @fn String getHostName()
	 * @brief Get the hostname of current replicaNode
	 * @return hostname of current replicaNode
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * @fn int getPort()
	 * @brief Get the port of current replicaNode
	 * @return port of current replicaNode
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @fn String getNodeName()
	 * @brief Get the name of current replicaNode
	 * @return name of current replicaNode
	 */
	public String getNodeName() {
		return nodeName;
	}

	/**
	 * @fn NodeStatus getStatus()
	 * @brief Get the status of current replicaNode
	 * @return the status of current replicaNode
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public NodeStatus getStatus() {
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.FIELD_NAME_GROUPID, group.getId());
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
	 * @brief Start current replicaNode in db
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void start() {
		startStop(true);
	}

	/**
	 * @fn void stop()
	 * @brief Stop current replicaNode in db
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public void stop() {
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
		// Admin command request
		// int reqId = 0;
		BSONObject dummyObj = new BasicBSONObject();
		SDBMessage sdbMessage = new SDBMessage();
		sdbMessage.setMatcher(obj);
		sdbMessage.setCollectionFullName(SequoiadbConstants.ADMIN_PROMPT + commandString);

		sdbMessage.setVersion(0);
		sdbMessage.setW((short) 0);
		sdbMessage.setPadding((short) 0);
		sdbMessage.setFlags(0);
		sdbMessage.setNodeID(SequoiadbConstants.ZERO_NODEID);
		// sdbMessage.setResponseTo(reqId);
		// reqId++;
		sdbMessage.setRequestID(this.getGroup().getSequoiadb().getNextRequstID());
		sdbMessage.setSkipRowsCount(-1);
		sdbMessage.setReturnRowsCount(-1);
		sdbMessage.setSelector(dummyObj);
		sdbMessage.setOrderBy(dummyObj);
		sdbMessage.setHint(dummyObj);
		sdbMessage.setOperationCode(Operation.OP_QUERY);

		boolean endianConver = this.group.getSequoiadb().endianConvert;
		byte[] request = SDBMessageHelper.buildQueryRequest(sdbMessage,
				endianConver);
		this.group.getSequoiadb().getConnection().sendMessage(request);

		ByteBuffer byteBuffer = this.group.getSequoiadb().getConnection().receiveMessage(endianConver);
		SDBMessage rtnSDBMessage = SDBMessageHelper.msgExtractReply(byteBuffer);
		SDBMessageHelper.checkMessage(sdbMessage, rtnSDBMessage);

		return rtnSDBMessage;
	}

}
