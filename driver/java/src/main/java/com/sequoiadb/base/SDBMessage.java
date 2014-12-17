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
package com.sequoiadb.base;

import java.util.List;

import org.bson.BSONObject;

import com.sequoiadb.base.SequoiadbConstants.Operation;

/**
 * @author Jacky Zhang
 * 
 */
public class SDBMessage {
	private int requestLength;
	private long requestID;
	private int responseTo;
	private Operation operationCode;
	private int version;
	private short w;
	private short padding;
	private int flags;
	private int collectionFullNameLength;
	private String collectionFullName;
	private BSONObject matcher;
	private BSONObject selector;
	private BSONObject orderBy;
	private BSONObject hint;
	private BSONObject insertor;
	private BSONObject modifier;
	private List<BSONObject> objectList;
	private List<byte[]> rawList;
	private byte[] nodeID = new byte[12];
	private long skipRowsCount;
	private long returnRowsCount;
	private int startFrom;
	private int numReturned;
	private int killCount;
	private List<Long> contextIDList;
	private String messageText;
	private int rc;
	
	private int lobLen;
    private long lobOffset;
    private int lobSequence;
    byte[] lobBuff;
	
    public int getLobLen() {
        return lobLen;
    }

    public void setLobLen(int lobLen) {
        this.lobLen = lobLen;
    }

    public long getLobOffset() {
        return lobOffset;
    }

    public void setLobOffset(long lobOffset) {
        this.lobOffset = lobOffset;
    }

    public int getLobSequence() {
        return lobSequence;
    }

    public void setLobSequence(int lobSequence) {
        this.lobSequence = lobSequence;
    }

    public byte[] getLobBuff() {
        return lobBuff;
    }

    public void setLobBuff(byte[] lobBuff) {
        this.lobBuff = lobBuff;
    }


	
	public SDBMessage() {
	}

	public byte[] getNodeID() {
		return nodeID;
	}

	public void setNodeID(byte[] nodeID) {
		this.nodeID = nodeID;
	}

	public BSONObject getSelector() {
		return selector;
	}

	public void setSelector(BSONObject selector) {
		this.selector = selector;
	}

	public BSONObject getModifier() {
		return modifier;
	}

	public void setModifier(BSONObject modifier) {
		this.modifier = modifier;
	}

	public BSONObject getInsertor() {
		return insertor;
	}

	public void setInsertor(BSONObject insertor) {
		this.insertor = insertor;
	}

	public BSONObject getMatcher() {
		return matcher;
	}

	public void setMatcher(BSONObject matcher) {
		this.matcher = matcher;
	}

	public BSONObject getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(BSONObject orderBy) {
		this.orderBy = orderBy;
	}

	public BSONObject getHint() {
		return hint;
	}

	public void setHint(BSONObject hint) {
		this.hint = hint;
	}

	public int getStartFrom() {
		return startFrom;
	}

	public void setStartFrom(int startFrom) {
		this.startFrom = startFrom;
	}

	public int getNumReturned() {
		return numReturned;
	}

	public void setNumReturned(int numReturned) {
		this.numReturned = numReturned;
	}

	public int getRc() {
		return rc;
	}

	public void setRc(int rc) {
		this.rc = rc;
	}

	public int getRequestLength() {
		return requestLength;
	}

	public void setRequestLength(int requestLength) {
		this.requestLength = requestLength;
	}

	public long getRequestID() {
		return requestID;
	}

	public void setRequestID(long requestID) {
		this.requestID = requestID;
	}

	public int getResponseTo() {
		return responseTo;
	}

	public void setResponseTo(int responseTo) {
		this.responseTo = responseTo;
	}

	public Operation getOperationCode() {
		return operationCode;
	}

	public void setOperationCode(Operation operationCode) {
		this.operationCode = operationCode;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public short getW() {
		return w;
	}

	public void setW(short w) {
		this.w = w;
	}

	public short getPadding() {
		return padding;
	}

	public void setPadding(short padding) {
		this.padding = padding;
	}
	
	public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	public int getCollectionFullNameLength() {
		return collectionFullNameLength;
	}

	public void setCollectionFullNameLength(int collectionFullNameLength) {
		this.collectionFullNameLength = collectionFullNameLength;
	}

	public String getCollectionFullName() {
		return collectionFullName;
	}

	public void setCollectionFullName(String collectionFullName) {
		this.collectionFullName = collectionFullName;
	}

	public List<BSONObject> getObjectList() {
		return objectList;
	}
	
	public List<byte[]> getObjectListRaw() {
		return rawList;
	}

	public void setObjectList(List<BSONObject> objectList) {
		this.objectList = objectList;
	}
	
	public void setObjectListRaw(List<byte[]> rawList) {
		this.rawList = rawList;
	}

	public long getSkipRowsCount() {
		return skipRowsCount;
	}

	public void setSkipRowsCount(long skipRowsCount) {
		this.skipRowsCount = skipRowsCount;
	}

	public long getReturnRowsCount() {
		return returnRowsCount;
	}

	public void setReturnRowsCount(long returnRowsCount) {
		this.returnRowsCount = returnRowsCount;
	}

	public int getKillCount() {
		return killCount;
	}

	public void setKillCount(int killCount) {
		this.killCount = killCount;
	}

	public List<Long> getContextIDList() {
		return contextIDList;
	}

	public void setContextIDList(List<Long> contextIDList) {
		this.contextIDList = contextIDList;
	}

	public String getMessageText() {
		return messageText;
	}

	public void setMessageText(String messageText) {
		this.messageText = messageText;
	}

}
