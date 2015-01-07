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
package com.sequoiadb.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.mina.core.buffer.IoBuffer;
import org.bson.BSON;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;

import com.google.gson.Gson;
import com.google.gson.internal.Primitives;
import com.sequoiadb.base.SDBMessage;
import com.sequoiadb.base.SequoiadbConstants;
import com.sequoiadb.base.SequoiadbConstants.Operation;
import com.sequoiadb.exception.BaseException;

/**
 * @author Jacky Zhang
 * 
 */
public class SDBMessageHelper {

	// msg.h - struct _MsgSysInfoRequest
	private final static int MESSAGE_SYSINFOREQUEST_LENGTH = 12;

	// msg.h - struct _MsgHeader
	private final static int MESSAGE_HEADER_LENGTH = 28;

	// msg.h - struct _MsgOpQuery
	private final static int MESSAGE_OPQUERY_LENGTH = 61;

	// msg.h - struct _MsgOpInsert
	public final static int MESSAGE_OPINSERT_LENGTH = 45;

	// msg.h - struct _MsgOpDelete
	private final static int MESSAGE_OPDELETE_LENGTH = 45;

	// msg.h - struct _MsgOpUpdate
	private final static int MESSAGE_OPUPDATE_LENGTH = 45;

	// msg.h - struct _MsgOpGetMore
	private final static int MESSAGE_OPGETMORE_LENGTH = 40;

	// msg.h - struct _MsgOpKillContexts
	private final static int MESSAGE_OPKILLCONTEXT_LENGTH = 36;
	
	// msg.h - struct _MsgOpLob
	public final static int MESSAGE_OPLOB_LENGTH = 52;
	
	// msg.h - struct _MsgLobTuple
    public final static int MESSAGE_LOBTUPLE_LENGTH = 16;
	
	private final static Byte BTYE_FILL = 0;

	public static byte[] buildSysInfoRequest() {
		ByteBuffer buf = ByteBuffer.allocate(MESSAGE_SYSINFOREQUEST_LENGTH);
		buf.putInt(SequoiadbConstants.MSG_SYSTEM_INFO_LENGTH);
		buf.putInt(SequoiadbConstants.MSG_SYSTEM_INFO_EYECATCHER);
		buf.putInt(MESSAGE_SYSINFOREQUEST_LENGTH);
		return buf.array();
	}

	public static byte[] buildQueryRequest(SDBMessage sdbMessage,
			boolean endianConvert) throws BaseException {
		try {
			String collectionName = sdbMessage.getCollectionFullName();
			int version = sdbMessage.getVersion();
			short w = sdbMessage.getW();
			short padding = sdbMessage.getPadding();
			int flags = sdbMessage.getFlags();
			long requestID = sdbMessage.getRequestID();
			int opCode = sdbMessage.getOperationCode().getOperationCode();
			// int responseTo = sdbMessage.getResponseTo();
			long skipRowsCount = sdbMessage.getSkipRowsCount();
			long returnRowsCount = sdbMessage.getReturnRowsCount();
			byte[] collByteArray = collectionName.getBytes("UTF-8");
			int collectionNameLength = collByteArray.length;
	
			byte[] query = bsonObjectToByteArray(sdbMessage.getMatcher());
			byte[] fieldSelector = bsonObjectToByteArray(sdbMessage.getSelector());
			byte[] orderBy = bsonObjectToByteArray(sdbMessage.getOrderBy());
			byte[] hint = bsonObjectToByteArray(sdbMessage.getHint());
			byte[] nodeID = sdbMessage.getNodeID();
	
			if (!endianConvert) {
				bsonEndianConvert(query, 0, query.length, true);
				bsonEndianConvert(fieldSelector, 0, fieldSelector.length, true);
				bsonEndianConvert(orderBy, 0, orderBy.length, true);
				bsonEndianConvert(hint, 0, hint.length, true);
			}
	
			int messageLength = Helper.roundToMultipleXLength(
					MESSAGE_OPQUERY_LENGTH + collectionNameLength, 4)
					+ Helper.roundToMultipleXLength(query.length, 4)
					+ Helper.roundToMultipleXLength(fieldSelector.length, 4)
					+ Helper.roundToMultipleXLength(orderBy.length, 4)
					+ Helper.roundToMultipleXLength(hint.length, 4);
	
			List<byte[]> fieldList = new ArrayList<byte[]>();
			// Header
			fieldList.add(assembleHeader(messageLength, requestID, nodeID,
					opCode, endianConvert));
	
			ByteBuffer buf = ByteBuffer.allocate(32);
			if (endianConvert) {
				buf.order(ByteOrder.LITTLE_ENDIAN);
			} else {
				buf.order(ByteOrder.BIG_ENDIAN);
			}
	
			buf.putInt(version);
			buf.putShort(w);
			buf.putShort(padding);
			buf.putInt(flags);
			buf.putInt(collectionNameLength);
			buf.putLong(skipRowsCount);
			buf.putLong(returnRowsCount);
	
			// Flags, collection name length, skip rows, return rows
			fieldList.add(buf.array());
	
			// Collection name plus '\0'
			byte[] newCollectionName = new byte[collectionNameLength + 1];
			for (int i = 0; i < collectionNameLength; i++) {
				newCollectionName[i] = collByteArray[i];
			}
	
			fieldList.add(Helper.roundToMultipleX(newCollectionName, 4));
	
			// query object
			fieldList.add(Helper.roundToMultipleX(query, 4));
	
			// Selector Object
			fieldList.add(Helper.roundToMultipleX(fieldSelector, 4));
	
			// OrderBy Object
			fieldList.add(Helper.roundToMultipleX(orderBy, 4));
	
			// hint Object
			fieldList.add(Helper.roundToMultipleX(hint, 4));
	
			// Concatenate everything
			byte[] msgInByteArray = Helper.concatByteArray(fieldList);
	
			return msgInByteArray;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BaseException("SDB_INVALIDARG", e);
		}
	}

	// type: 0(verify), 1(create), 2(delete)
	public static byte[] buildAuthMsg(String userName, String password,
			long reqID, byte type, boolean endianConvert) {
		String md5 = null;
		if (password != null)
			md5 = getMD5FromStr(password);
		BSONObject obj = new BasicBSONObject();
		obj.put(SequoiadbConstants.SDB_AUTH_USER, userName);
		obj.put(SequoiadbConstants.SDB_AUTH_PASSWD, md5);
		byte[] info = bsonObjectToByteArray(obj);
		if (!endianConvert) {
			bsonEndianConvert(info, 0, info.length, true);
		}
		int messageLength = MESSAGE_HEADER_LENGTH
				+ Helper.roundToMultipleXLength(info.length, 4);
		List<byte[]> fieldList = new ArrayList<byte[]>();
		// Header
		int opCode = 0;
		switch (type) {
		case 0:
			opCode = Operation.MSG_AUTH_VERIFY.getOperationCode();
			break;
		case 1:
			opCode = Operation.MSG_AUTH_CRTUSR.getOperationCode();
			break;
		case 2:
			opCode = Operation.MSG_AUTH_DELUSR.getOperationCode();
			break;
		default:
			throw new BaseException("SDB_INVALIDARG");
		}
		fieldList.add(assembleHeader(messageLength, reqID,
				SequoiadbConstants.ZERO_NODEID, opCode, endianConvert));
		fieldList.add(Helper.roundToMultipleX(info, 4));
		// Concatenate everything
		byte[] msgInByteArray = Helper.concatByteArray(fieldList);

		return msgInByteArray;
	}

	public static byte[] buildTransactionRequest(Operation opCode, long reqID,
			boolean endianConvert) {
		int messageLength = MESSAGE_HEADER_LENGTH;
		byte[] msg = assembleHeader(messageLength, reqID,
				SequoiadbConstants.ZERO_NODEID, opCode.getOperationCode(),
				endianConvert);
		return msg;
	}

	public static int buildBulkInsertRequest(IoBuffer bulk_buffer, long reqID,
			String collectionFullName, List<BSONObject> insertor, int flag)
			throws BaseException {
		try {
			int messageLength = SDBMessageHelper.MESSAGE_OPINSERT_LENGTH - 1;
	
			int startPos = bulk_buffer.position();
	
			bulk_buffer.putInt(-1); // messageLength
			bulk_buffer.putInt(Operation.OP_INSERT.getOperationCode()); // operationCode
			bulk_buffer.put(SequoiadbConstants.ZERO_NODEID); // NodeID
			bulk_buffer.putLong(reqID); // requestID
	
			bulk_buffer.putInt(0); // version
			bulk_buffer.putShort((short) 0); // W
			bulk_buffer.putShort((short) 0); // Padding
			bulk_buffer.putInt(flag); // flag
			byte[] collByteArray = collectionFullName.getBytes("UTF-8");
			bulk_buffer.putInt(collByteArray.length); // CollectionName
	
			byte[] newCollectionName = new byte[collByteArray.length + 1];
			for (int i = 0; i < collByteArray.length; i++) {
				newCollectionName[i] = collByteArray[i];
			}
	
			messageLength += Helper.roundToMultipleXLength(
					newCollectionName.length, 4);
	
			bulk_buffer.put(Helper.roundToMultipleX(newCollectionName, 4));
			for (int i = 0; i < insertor.size(); i++) {
				int record_length = bsonObjectToByteBuffer(bulk_buffer,
						insertor.get(i));
	
				int length = Helper.roundToMultipleXLength(record_length, 4);
				int j = 0;
				while (record_length + j < length) {
					bulk_buffer.put(BTYE_FILL);
					j++;
				}
	
				messageLength += length;
			}
	
			bulk_buffer.position(startPos); // set the real messageLength
			bulk_buffer.putInt(messageLength);
	
			return messageLength;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BaseException("SDB_INVALIDARG", e);
		}
	}

	public static int buildInsertRequest(IoBuffer bulk_buffer, long reqID,
			String collectionFullName, BSONObject insertor)
			throws BaseException {
		try {
			int messageLength = SDBMessageHelper.MESSAGE_OPINSERT_LENGTH - 1;
	
			int startPos = bulk_buffer.position();
	
			bulk_buffer.putInt(-1); // messageLength
			bulk_buffer.putInt(Operation.OP_INSERT.getOperationCode()); // operationCode
			bulk_buffer.put(SequoiadbConstants.ZERO_NODEID); // NodeID
			bulk_buffer.putLong(reqID); // requestID
	
			bulk_buffer.putInt(0); // version
			bulk_buffer.putShort((short) 0); // W
			bulk_buffer.putShort((short) 0); // Padding
			bulk_buffer.putInt(0); // flag
			byte[] collByteArray = collectionFullName.getBytes("UTF-8");
			bulk_buffer.putInt(collByteArray.length); // CollectionName
	
			byte[] newCollectionName = new byte[collByteArray.length + 1];
			for (int i = 0; i < collByteArray.length; i++) {
				newCollectionName[i] = collByteArray[i];
			}
	
			messageLength += Helper.roundToMultipleXLength(
					newCollectionName.length, 4);
	
			bulk_buffer.put(Helper.roundToMultipleX(newCollectionName, 4));
	
			int record_length = bsonObjectToByteBuffer(bulk_buffer, insertor);
	
			int length = Helper.roundToMultipleXLength(record_length, 4);
			int j = 0;
			while (record_length + j < length) {
				bulk_buffer.put(BTYE_FILL);
				j++;
			}
	
			messageLength += length;
	
			bulk_buffer.position(startPos);
			bulk_buffer.putInt(messageLength);
	
			return messageLength;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BaseException("SDB_INVALIDARG", e);
		}
	}
	
	public static byte[] buildDeleteRequest(SDBMessage sdbMessage,
			boolean endianConvert) throws BaseException {
		try {
			String collectionName = sdbMessage.getCollectionFullName();
			int version = sdbMessage.getVersion();
			short w = sdbMessage.getW();
			short padding = sdbMessage.getPadding();
			int flags = sdbMessage.getFlags();
			long requestID = sdbMessage.getRequestID();
			byte[] nodeID = sdbMessage.getNodeID();
			int opCode = sdbMessage.getOperationCode().getOperationCode();
	
			// int responseTo = sdbMessage.getResponseTo();
			byte[] collByteArray = collectionName.getBytes("UTF-8");
			int collectionNameLength = collByteArray.length;
	
			byte[] matcher = bsonObjectToByteArray(sdbMessage.getMatcher());
			byte[] hint = bsonObjectToByteArray(sdbMessage.getHint());
	
			if (!endianConvert) {
				bsonEndianConvert(matcher, 0, matcher.length, true);
				bsonEndianConvert(hint, 0, hint.length, true);
			}
	
			int messageLength = Helper.roundToMultipleXLength(
					MESSAGE_OPDELETE_LENGTH + collectionNameLength, 4)
					+ Helper.roundToMultipleXLength(matcher.length, 4)
					+ Helper.roundToMultipleXLength(hint.length, 4);
	
			List<byte[]> fieldList = new ArrayList<byte[]>();
			// Header
			fieldList.add(assembleHeader(messageLength, requestID, nodeID,
					opCode, endianConvert));
	
			ByteBuffer buf = ByteBuffer.allocate(16);
			if (endianConvert) {
				buf.order(ByteOrder.LITTLE_ENDIAN);
			} else {
				buf.order(ByteOrder.BIG_ENDIAN);
			}
	
			buf.putInt(version);
			buf.putShort(w);
			buf.putShort(padding);
			buf.putInt(flags);
			buf.putInt(collectionNameLength);
	
			// Flags, collection name length
			fieldList.add(buf.array());
	
			// Collection name plus '\0'
			byte[] newCollectionName = new byte[collectionNameLength + 1];
	
			for (int i = 0; i < collectionNameLength; i++) {
				newCollectionName[i] = collByteArray[i];
			}
	
			fieldList.add(Helper.roundToMultipleX(newCollectionName, 4));
	
			// Matcher object
			fieldList.add(Helper.roundToMultipleX(matcher, 4));
	
			// Hint object
			fieldList.add(Helper.roundToMultipleX(hint, 4));
	
			// Concatenate everything
			byte[] msgInByteArray = Helper.concatByteArray(fieldList);
	
			return msgInByteArray;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BaseException("SDB_INVALIDARG", e);
		}
	}

	public static byte[] buildUpdateRequest(SDBMessage sdbMessage,
			boolean endianConvert) throws BaseException {
		try {
			String collectionName = sdbMessage.getCollectionFullName();
			int version = sdbMessage.getVersion();
			short w = sdbMessage.getW();
			short padding = sdbMessage.getPadding();
			int flags = sdbMessage.getFlags();
			long requestID = sdbMessage.getRequestID();
			byte[] nodeID = sdbMessage.getNodeID();
			int opCode = sdbMessage.getOperationCode().getOperationCode();
	
			// int responseTo = sdbMessage.getResponseTo();
			byte[] collByteArray = collectionName.getBytes("UTF-8");
			int collectionNameLength = collByteArray.length;
	
			byte[] matcher = bsonObjectToByteArray(sdbMessage.getMatcher());
			byte[] hint = bsonObjectToByteArray(sdbMessage.getHint());
			byte[] modifier = bsonObjectToByteArray(sdbMessage.getModifier());
	
			if (!endianConvert) {
				bsonEndianConvert(matcher, 0, matcher.length, true);
				bsonEndianConvert(hint, 0, hint.length, true);
				bsonEndianConvert(modifier, 0, modifier.length, true);
			}
	
			int messageLength = Helper.roundToMultipleXLength(
					MESSAGE_OPUPDATE_LENGTH + collectionNameLength, 4)
					+ Helper.roundToMultipleXLength(matcher.length, 4)
					+ Helper.roundToMultipleXLength(modifier.length, 4)
					+ Helper.roundToMultipleXLength(hint.length, 4);
	
			List<byte[]> fieldList = new ArrayList<byte[]>();
			// Header
			fieldList.add(assembleHeader(messageLength, requestID, nodeID,
			        opCode, endianConvert));
	
			ByteBuffer buf = ByteBuffer.allocate(16);
			if (endianConvert) {
				buf.order(ByteOrder.LITTLE_ENDIAN);
			} else {
				buf.order(ByteOrder.BIG_ENDIAN);
			}
	
			buf.putInt(version);
			buf.putShort(w);
			buf.putShort(padding);
			buf.putInt(flags);
			buf.putInt(collectionNameLength);
	
			// Flags, collection name length
			fieldList.add(buf.array());
	
			// Collection name plus '\0'
			byte[] newCollectionName = new byte[collectionNameLength + 1];
			for (int i = 0; i < collectionNameLength; i++) {
				newCollectionName[i] = collByteArray[i];
			}
	
			fieldList.add(Helper.roundToMultipleX(newCollectionName, 4));
	
			// Matcher object
			fieldList.add(Helper.roundToMultipleX(matcher, 4));
	
			// Modifier object
			fieldList.add(Helper.roundToMultipleX(modifier, 4));
	
			// Hint object
			fieldList.add(Helper.roundToMultipleX(hint, 4));
	
			// Concatenate everything
			byte[] msgInByteArray = Helper.concatByteArray(fieldList);
	
			return msgInByteArray;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BaseException("SDB_INVALIDARG", e);
		}
	}

	public static byte[] buildDisconnectRequest(boolean endianConvert)
			throws BaseException {
		long requestID = 0;
		byte[] nodeID = SequoiadbConstants.ZERO_NODEID;
		int messageLength = Helper.roundToMultipleXLength(
				MESSAGE_HEADER_LENGTH, 4);
		List<byte[]> fieldList = new ArrayList<byte[]>();
		fieldList.add(assembleHeader(messageLength, requestID, nodeID,
				Operation.OP_DISCONNECT.getOperationCode(), endianConvert));
		byte[] msgInByteArray = Helper.concatByteArray(fieldList);

		return msgInByteArray;
	}

	public static byte[] buildSqlMsg(SDBMessage sdb, String sql,
			boolean endianConvert) throws BaseException {
		try {
			long requestID = sdb.getRequestID();
			byte[] nodeID = sdb.getNodeID();
			byte[] sqlBytes = sql.getBytes("UTF-8");
			int sqlLength = sqlBytes.length;
			byte[] newSqlBytes = new byte[sqlLength + 1];
			int messageLength = Helper.roundToMultipleXLength(
					(MESSAGE_HEADER_LENGTH + sqlLength + 1), 4);
			List<byte[]> fieldList = new ArrayList<byte[]>();
			fieldList.add(assembleHeader(messageLength, requestID, nodeID,
					Operation.MSG_BS_SQL_REQ.getOperationCode(), endianConvert));
			for (int i = 0; i < sqlLength; i++) {
				newSqlBytes[i] = sqlBytes[i];
			}
			fieldList.add(Helper.roundToMultipleX(newSqlBytes, 4));
			byte[] msgInByteArray = Helper.concatByteArray(fieldList);
			return msgInByteArray;
		} catch (java.io.UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new BaseException("SDB_INVALIDARG", e);
		}
	}
	
	/*
	 * public static int buildAggrRequest(ByteBuffer bulk_buffer, String
	 * collectionFullName, List<BSONObject> insertor) throws BaseException {
	 * return buildBulkInsertRequest(bulk_buffer,collectionFullName,insertor,0);
	 * }
	 */
	public static int buildAggrRequest(IoBuffer bulk_buffer, long reqID,
			                           String collectionFullName,
			                           List<BSONObject> insertor)
			throws BaseException {
		try {
			int messageLength = SDBMessageHelper.MESSAGE_OPINSERT_LENGTH - 1;
			int startPos = bulk_buffer.position();
			
			bulk_buffer.putInt(-1); // messageLength
			bulk_buffer.putInt(Operation.OP_AGGREGATE.getOperationCode()); // operationCode
			bulk_buffer.put(SequoiadbConstants.ZERO_NODEID); // NodeID
			bulk_buffer.putLong(reqID); // requestID
	
			bulk_buffer.putInt(0); // version
			bulk_buffer.putShort((short) 0); // W
			bulk_buffer.putShort((short) 0); // Padding
			bulk_buffer.putInt(0); // flag
			byte[] collByteArray = collectionFullName.getBytes("UTF-8");
			bulk_buffer.putInt(collByteArray.length); // CollectionName
	
			byte[] newCollectionName = new byte[collByteArray.length + 1];
	
			for (int i = 0; i < collByteArray.length; i++) {
				newCollectionName[i] = collByteArray[i];
			}
	
			messageLength += Helper.roundToMultipleXLength(
					newCollectionName.length, 4);
	
			bulk_buffer.put(Helper.roundToMultipleX(newCollectionName, 4));
	
			for (int i = 0; i < insertor.size(); i++) {
				int record_length = bsonObjectToByteBuffer(bulk_buffer,
						insertor.get(i));
	
				int length = Helper.roundToMultipleXLength(record_length, 4);
				int j = 0;
				while (record_length + j < length) {
					bulk_buffer.put(BTYE_FILL);
					j++;
				}
				messageLength += length;
			}
	        // set the real messageLength
			bulk_buffer.position(startPos);
			bulk_buffer.putInt(messageLength);
	
			return messageLength;
		} catch (java.io.UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new BaseException("SDB_INVALIDARG", e);
		}
	}

	private static byte[] assembleHeader(int messageLength, long requestID,
			byte[] nodeID, int operationCode, boolean endianConvert) {
		ByteBuffer buf = ByteBuffer.allocate(MESSAGE_HEADER_LENGTH);
		if (endianConvert) {
			buf.order(ByteOrder.LITTLE_ENDIAN);
		} else {
			buf.order(ByteOrder.BIG_ENDIAN);
		}

		buf.putInt(messageLength);
		buf.putInt(operationCode);
		buf.put(nodeID);
		buf.putLong(requestID);

		return buf.array();
	}

	public static SDBMessage msgExtractReply(ByteBuffer byteBuffer)
			throws BaseException {

		SDBMessage sdbMessage = new SDBMessage();

		int MessageLength = byteBuffer.getInt();

		if (MessageLength < MESSAGE_HEADER_LENGTH) {
			throw new BaseException("SDB_INVALIDSIZE", MessageLength);
		}

		// Request message length
		sdbMessage.setRequestLength(MessageLength);

		// Action code
		sdbMessage.setOperationCode(Operation.getByValue(byteBuffer.getInt()));

		// nodeID
		byte[] nodeID = new byte[12];
		byteBuffer.get(nodeID, 0, 12);
		sdbMessage.setNodeID(nodeID);

		// Request id
		sdbMessage.setRequestID(byteBuffer.getLong());

		// context id
		List<Long> contextIDList = new ArrayList<Long>();
		contextIDList.add(byteBuffer.getLong());
		sdbMessage.setContextIDList(contextIDList);

		// flags
		sdbMessage.setFlags(byteBuffer.getInt());

		// Start from
		sdbMessage.setStartFrom(byteBuffer.getInt());

		// Return record rows
		int numReturned = byteBuffer.getInt();
		sdbMessage.setNumReturned(numReturned);

		if (numReturned > 0) {
			List<BSONObject> objList = extractBSONObject(byteBuffer);
			sdbMessage.setObjectList(objList);
		} else {
			sdbMessage.setObjectList(null);
		}

		return sdbMessage;
	}
	
	public static void addLobMsgHeader( ByteBuffer buff, int totalLen, 
	        int opCode, byte[] nodeID, long requestID ) {
        
        //MsgHeader.messageLength
        buff.putInt( totalLen );
        //MsgHeader.opCode
        buff.putInt( opCode );
        //MsgHeader.TID + MsgHeader.routeID
        buff.put( nodeID );
        //MsgHeader.requestID
        buff.putLong(requestID);
    }
	
	public static void addLobOpMsg( ByteBuffer buff, int version, short w, 
            short padding, int flags, long contextID, int bsonLen ) {
        
        //_MsgOpLob.version
        buff.putInt( version );
        //_MsgOpLob.w
        buff.putShort( w );
        //_MsgOpLob.padding
        buff.putShort( padding );
        //_MsgOpLob.flags
        buff.putInt( flags );
        //_MsgOpLob.contextID
        buff.putLong( contextID );
        //_MsgOpLob.bsonLen
        buff.putInt( bsonLen );
    }
	
	public static byte[] generateRemoveLobRequest( BSONObject removeObj,
	        long reqID, boolean endianConvert) {
	    byte bRevemoObj[] = bsonObjectToByteArray( removeObj );
        int totalLen = MESSAGE_OPLOB_LENGTH 
                        + Helper.roundToMultipleXLength( bRevemoObj.length, 4 );
        
        // convert the openLob's buff
        if ( !endianConvert ) {
            bsonEndianConvert( bRevemoObj, 0, bRevemoObj.length, true );
        }
        
        // add _MsgOpLob into buff with convert(db.endianConvert)
        ByteBuffer buff = ByteBuffer.allocate( 
                                SDBMessageHelper.MESSAGE_OPLOB_LENGTH );
        if ( endianConvert ) {
            buff.order( ByteOrder.LITTLE_ENDIAN );
        } else {
            buff.order( ByteOrder.BIG_ENDIAN );
        }
        
        //*******************MsgHeader*******************
        addLobMsgHeader( buff, totalLen, 
                Operation.MSG_BS_LOB_REMOVE_REQ.getOperationCode(), 
                SequoiadbConstants.ZERO_NODEID, reqID );
        
        //*******************_MsgOpLob**********************
        addLobOpMsg( buff, SequoiadbConstants.DEFAULT_VERSION, 
                   SequoiadbConstants.DEFAULT_W, (short)0, 
                   SequoiadbConstants.DEFAULT_FLAGS, 
                   SequoiadbConstants.DEFAULT_CONTEXTID, bRevemoObj.length );
        
        List<byte[]> buffList = new ArrayList<byte[]>();
        buffList.add( buff.array() );
        buffList.add( Helper.roundToMultipleX( bRevemoObj, 4 ) );

        return Helper.concatByteArray( buffList );
	}
	
	public static SDBMessage msgExtractLobOpenReply(ByteBuffer byteBuffer)
            throws BaseException {

        SDBMessage sdbMessage = new SDBMessage();

        int MessageLength = byteBuffer.getInt();

        if (MessageLength < MESSAGE_HEADER_LENGTH) {
            throw new BaseException("SDB_INVALIDSIZE", MessageLength);
        }

        // Request message length
        sdbMessage.setRequestLength(MessageLength);

        // Action code
        sdbMessage.setOperationCode(Operation.getByValue(byteBuffer.getInt()));

        // nodeID
        byte[] nodeID = new byte[12];
        byteBuffer.get(nodeID, 0, 12);
        sdbMessage.setNodeID(nodeID);

        // Request id
        sdbMessage.setRequestID(byteBuffer.getLong());

        // context id
        List<Long> contextIDList = new ArrayList<Long>();
        contextIDList.add(byteBuffer.getLong());
        sdbMessage.setContextIDList(contextIDList);

        // flags
        sdbMessage.setFlags(byteBuffer.getInt());

        // Start from
        sdbMessage.setStartFrom(byteBuffer.getInt());

        // Return record rows
        int numReturned = byteBuffer.getInt();
        sdbMessage.setNumReturned(numReturned);

        List<BSONObject> objList = extractBSONObject(byteBuffer);
        sdbMessage.setObjectList(objList);

        return sdbMessage;
    }
	
	public static SDBMessage msgExtractLobRemoveReply(ByteBuffer byteBuffer)
            throws BaseException {
	    return msgExtractLobOpenReply(byteBuffer);
    }
	
	public static SDBMessage msgExtractLobReadReply(ByteBuffer byteBuffer)
            throws BaseException {

        SDBMessage sdbMessage = new SDBMessage();

        int MessageLength = byteBuffer.getInt();

        if (MessageLength < MESSAGE_HEADER_LENGTH) {
            throw new BaseException("SDB_INVALIDSIZE", MessageLength);
        }

        // Request message length
        sdbMessage.setRequestLength(MessageLength);

        // Action code
        sdbMessage.setOperationCode(Operation.getByValue(byteBuffer.getInt()));

        // nodeID
        byte[] nodeID = new byte[12];
        byteBuffer.get(nodeID, 0, 12);
        sdbMessage.setNodeID(nodeID);

        // Request id
        sdbMessage.setRequestID(byteBuffer.getLong());

        // context id
        List<Long> contextIDList = new ArrayList<Long>();
        contextIDList.add(byteBuffer.getLong());
        sdbMessage.setContextIDList(contextIDList);

        // flags
        sdbMessage.setFlags(byteBuffer.getInt());

        // Start from
        sdbMessage.setStartFrom(byteBuffer.getInt());

        // Return record rows
        int numReturned = byteBuffer.getInt();
        sdbMessage.setNumReturned(numReturned);

        sdbMessage.setObjectList(null);
        
        if ( sdbMessage.getFlags() == 0 ) {
            // _MsgLobTuple
            sdbMessage.setLobLen(byteBuffer.getInt());
            sdbMessage.setLobSequence(byteBuffer.getInt());
            sdbMessage.setLobOffset(byteBuffer.getLong());
            
            byte[] buff   = new byte[sdbMessage.getLobLen()];
            byteBuffer.get(buff);
            sdbMessage.setLobBuff(buff);
        }

        return sdbMessage;
    }

	public static SDBMessage msgExtractEvalReply(ByteBuffer byteBuffer)
			throws BaseException {

		SDBMessage sdbMessage = new SDBMessage();

		int MessageLength = byteBuffer.getInt();

		if (MessageLength < MESSAGE_HEADER_LENGTH) {
			throw new BaseException("SDB_INVALIDSIZE", MessageLength);
		}

		// Request message length
		sdbMessage.setRequestLength(MessageLength);

		// Action code
		sdbMessage.setOperationCode(Operation.getByValue(byteBuffer.getInt()));

		// nodeID
		byte[] nodeID = new byte[12];
		byteBuffer.get(nodeID, 0, 12);
		sdbMessage.setNodeID(nodeID);

		// Request id
		sdbMessage.setRequestID(byteBuffer.getLong());

		// context id
		List<Long> contextIDList = new ArrayList<Long>();
		contextIDList.add(byteBuffer.getLong());
		sdbMessage.setContextIDList(contextIDList);

		// flags
		sdbMessage.setFlags(byteBuffer.getInt());

		// Start from
		sdbMessage.setStartFrom(byteBuffer.getInt());

		// Not the return record rows, it is the return type of eval result
		int returnType = byteBuffer.getInt();
		sdbMessage.setNumReturned(returnType);
		// Get the extract the error message
		if ((sdbMessage.getFlags() != 0)) {
			List<BSONObject> objList = extractBSONObject(byteBuffer);
			sdbMessage.setObjectList(objList);
		} else {
			sdbMessage.setObjectList(null);
		}

		return sdbMessage;
	}
	
	public static SDBMessage msgExtractReplyRaw(ByteBuffer byteBuffer)
			throws BaseException {

		SDBMessage sdbMessage = new SDBMessage();

		int MessageLength = byteBuffer.getInt();
		// Request message length
		sdbMessage.setRequestLength(MessageLength);

		// Action code
		sdbMessage.setOperationCode(Operation.getByValue(byteBuffer.getInt()));

		// nodeID
		byte[] nodeID = new byte[12];
		byteBuffer.get(nodeID, 0, 12);
		sdbMessage.setNodeID(nodeID);

		// Request id
		sdbMessage.setRequestID(byteBuffer.getLong());

		// context id
		List<Long> contextIDList = new ArrayList<Long>();
		contextIDList.add(byteBuffer.getLong());
		sdbMessage.setContextIDList(contextIDList);

		// flags
		sdbMessage.setFlags(byteBuffer.getInt());

		// Start from
		sdbMessage.setStartFrom(byteBuffer.getInt());

		// Return record rows
		int numReturned = byteBuffer.getInt();
		sdbMessage.setNumReturned(numReturned);

		if (numReturned > 0) {
			List<byte[]> objList = extractRawObject(byteBuffer);
			sdbMessage.setObjectListRaw(objList);
		} else
			sdbMessage.setObjectListRaw(null);

		return sdbMessage;
	}

	@SuppressWarnings("unused")
	public static byte[] buildGetMoreRequest(SDBMessage sdbMessage,
			boolean endianConvert) throws BaseException {
		long requestID = sdbMessage.getRequestID();
		long contextId = sdbMessage.getContextIDList().get(0);
		int numReturned = sdbMessage.getNumReturned();
		byte[] nodeID = sdbMessage.getNodeID();
		int opCode = sdbMessage.getOperationCode().getOperationCode();
		// int responseTo = sdbMessage.getResponseTo();

		int messageLength = MESSAGE_OPGETMORE_LENGTH;

		List<byte[]> fieldList = new ArrayList<byte[]>();
		// Header
		fieldList.add(assembleHeader(messageLength, requestID,
				SequoiadbConstants.ZERO_NODEID, opCode, endianConvert));
		ByteBuffer buf = ByteBuffer.allocate(12);
		if (endianConvert) {
			buf.order(ByteOrder.LITTLE_ENDIAN);
		} else {
			buf.order(ByteOrder.BIG_ENDIAN);
		}
		buf.putLong(contextId);
		buf.putInt(numReturned);
		fieldList.add(buf.array());

		// Concatenate everything
		byte[] msgInByteArray = Helper.concatByteArray(fieldList);

		return msgInByteArray;
	}

	@SuppressWarnings("unused")
	public static byte[] buildKillCursorMsg(long reqId, long[] contextIds,
			boolean endianConvert) {
		int messageLength = MESSAGE_OPKILLCONTEXT_LENGTH + 8
				* contextIds.length;

		List<byte[]> fieldList = new ArrayList<byte[]>();
		// Header
		fieldList.add(assembleHeader(messageLength, reqId,
				SequoiadbConstants.ZERO_NODEID,
				Operation.OP_KILL_CONTEXT.getOperationCode(), endianConvert));
		ByteBuffer buf = ByteBuffer.allocate(8 + 8 * contextIds.length);
		if (SequoiadbConstants.SYSTEM_ENDIAN == SequoiadbConstants.LITTLE_ENDIAN) {
			buf.order(ByteOrder.LITTLE_ENDIAN);
		} else {
			buf.order(ByteOrder.BIG_ENDIAN);
		}
		buf.putInt(0);
		buf.putInt(contextIds.length);
		for (long temp : contextIds)
			buf.putLong(temp);

		fieldList.add(buf.array());
		// Concatenate everything
		byte[] msgInByteArray = Helper.concatByteArray(fieldList);
		return msgInByteArray;
	}

	private static List<byte[]> extractRawObject(ByteBuffer byteBuffer)
			throws BaseException {
		List<byte[]> rawList = new ArrayList<byte[]>();

		int nextBsonPos = byteBuffer.position();
		while (nextBsonPos < byteBuffer.limit()) {
			byteBuffer.position(nextBsonPos);

			int startPos = byteBuffer.position();
			int objLen = byteBuffer.getInt();
			byteBuffer.position(startPos);

			int objAllotLen = Helper.roundToMultipleXLength(objLen, 4);

			if (byteBuffer.order() == ByteOrder.BIG_ENDIAN) {
				bsonEndianConvert(byteBuffer.array(), byteBuffer.position(),
						objLen, false);
			} 
			
			byte[] bsonObj = new byte[objLen];
			byteBuffer.get(bsonObj, 0, objLen);
			rawList.add(bsonObj);
			
			nextBsonPos = byteBuffer.position() + (objAllotLen - objLen);
		}

		return rawList;
	}

	private static List<BSONObject> extractBSONObject(ByteBuffer byteBuffer)
			throws BaseException {

		List<BSONObject> objList = new ArrayList<BSONObject>();
		int nextBsonPos = byteBuffer.position();
		while (nextBsonPos < byteBuffer.limit()) {
			byteBuffer.position(nextBsonPos);
			
			int startPos = byteBuffer.position();
			int objLen = byteBuffer.getInt();
			byteBuffer.position(startPos);

			int objAllotLen = Helper.roundToMultipleXLength(objLen, 4);

			if (byteBuffer.order() == ByteOrder.BIG_ENDIAN) {
				bsonEndianConvert(byteBuffer.array(), byteBuffer.position(),
						objLen, false);
			}
			
			objList.add(byteArrayToBSONObject(byteBuffer));

			nextBsonPos = byteBuffer.position() + objAllotLen;
		}

		return objList;
	}

	public static int bsonObjectToByteBuffer(IoBuffer byteBuffer,
			BSONObject obj) {
		BSONEncoder e = new BasicBSONEncoder();
		ByteOutputBuffer1 buf = new ByteOutputBuffer1(byteBuffer);

		e.set(buf);
		int length = e.putObject(obj);
		e.done();

		return length;
	}

	public static byte[] bsonObjectToByteArray(BSONObject obj) {
		BSONEncoder e = new BasicBSONEncoder();
		OutputBuffer buf = new BasicOutputBuffer();

		e.set(buf);
		e.putObject(obj);
		e.done();

		return buf.toByteArray();
	}

	@SuppressWarnings("unused")
	public static BSONObject byteArrayToBSONObject(ByteBuffer byteBuffer)
			throws BaseException {
		if (byteBuffer == null || !byteBuffer.hasRemaining())
			return null;

		BSONDecoder d = new BasicBSONDecoder();
		BSONCallback cb = new BasicBSONCallback();
		try {
			// TODO: need process BIGEND

			int s = d.decode(new ByteArrayInputStream(byteBuffer.array(),
					byteBuffer.position(), byteBuffer.remaining()), cb);
			BSONObject o1 = (BSONObject) cb.get();
			return o1;
		} catch (IOException e) {
			throw new BaseException("SDB_INVALIDARG", e);
		}
	}

	public static BSONObject fromObject(Object object) throws BaseException {
		Gson gson = new Gson();
		String jString = gson.toJson(object);

		return fromJson(jString);
	}

	public static <T> T fromBson(BSONObject bObj, Class<T> classOfT) {
		bObj.removeField("_id");
		Gson gson = new Gson();
		Object object = gson.fromJson(bObj.toString(), (Type) classOfT);
		return Primitives.wrap(classOfT).cast(object);
	}

	public static BSONObject fromJson(String jsonString) throws BaseException {

		String fullString = "{\"bsonMap\":" + jsonString + "}";

		Gson gson = new Gson();
		ConvertHelpObject obj = gson.fromJson(fullString,
				ConvertHelpObject.class);

		LinkedHashMap<String, Object> bsonMap = obj.getBsonMap();

		BSONObject o1 = new BasicBSONObject();
		o1.putAll(bsonMap);

		return o1;
	}

	private class ConvertHelpObject {
		private LinkedHashMap<String, Object> bsonMap;

		public LinkedHashMap<String, Object> getBsonMap() {
			return bsonMap;
		}

		@SuppressWarnings("unused")
		public void setBsonMap(LinkedHashMap<String, Object> bsonMap) {
			this.bsonMap = bsonMap;
		}
	}

	public static byte[] appendInsertMsg(byte[] msg, BSONObject append,
			boolean endianConvert) {
		List<byte[]> tmp = Helper.splitByteArray(msg, 4);
		byte[] msgLength = tmp.get(0);
		byte[] remaining = tmp.get(1);
		byte[] insertor = bsonObjectToByteArray(append);

		if (!endianConvert)
			bsonEndianConvert(insertor, 0, insertor.length, true);

		int length = Helper.byteToInt(msgLength, endianConvert);
		int messageLength = length
				+ Helper.roundToMultipleXLength(insertor.length, 4);

		ByteBuffer buf = ByteBuffer.allocate(messageLength);
		if (endianConvert) {
			buf.order(ByteOrder.LITTLE_ENDIAN);
		} else {
			buf.order(ByteOrder.BIG_ENDIAN);
		}

		buf.putInt(messageLength);
		buf.put(remaining);
		buf.put(Helper.roundToMultipleX(insertor, 4));

		return buf.array();
	}

	private static String getMD5FromStr(String inStr) {
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
		char[] charArray = inStr.toCharArray();
		byte[] byteArray = new byte[charArray.length];

		for (int i = 0; i < charArray.length; i++) {
			byteArray[i] = (byte) charArray[i];
		}

		byte[] md5Bytes = md5.digest(byteArray);

		StringBuffer hexValue = new StringBuffer();

		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16)
				hexValue.append("0");
			hexValue.append(Integer.toHexString(val));
		}

		return hexValue.toString();

	}

	public static boolean msgExtractSysInfoReply(byte[] msg) {
		List<byte[]> tmp = Helper.splitByteArray(msg,
				MESSAGE_SYSINFOREQUEST_LENGTH);
		byte[] header = tmp.get(0);
		// byte[] remaining = tmp.get(1);

		return extractSysInfoHeader(header);
	}

	private static boolean extractSysInfoHeader(byte[] header) {
		boolean endianConvert = false;
                byte[] headsByte = new byte[4];
                System.arraycopy(header, 4, headsByte, 0, 4);
		int eyeCatcher = Helper.byteToInt(headsByte);
		if (eyeCatcher == SequoiadbConstants.MSG_SYSTEM_INFO_EYECATCHER)
			endianConvert = false;
		else if (eyeCatcher == SequoiadbConstants.MSG_SYSTEM_INFO_EYECATCHER_REVERT)
			endianConvert = true;
		else
			throw new BaseException("SDB_INVALIDARG", eyeCatcher);

		return endianConvert;
	}


	
	// little endian => big endian
	public static void bsonEndianConvert(byte[] inBytes, int offset,
			int objSize, boolean l2r) {
		int begin = offset;
		arrayReverse(inBytes, offset, 4);
		offset += 4;
		byte type;
		while (offset < inBytes.length) {
			// get bson element type
			type = inBytes[offset];
			// move offset to next to skip type
			++offset;
			if (type == BSON.EOO)
				break;
			// skip element name: '...\0'
			offset += getStrLength(inBytes, offset) + 1;
			switch (type) {
			case BSON.NUMBER:
				arrayReverse(inBytes, offset, 8);
				offset += 8;
				break;
			case BSON.STRING:
			case BSON.CODE:
			case BSON.SYMBOL: {
				// the first 4 bytes indicate the length of the string
				// the length of the string is the real length plus 1('\0')
				int length = Helper.byteToInt(inBytes, offset);
				arrayReverse(inBytes, offset, 4);
				int newLength = Helper.byteToInt(inBytes, offset);
				offset += (l2r ? newLength : length) + 4;
				break;
			}
			case BSON.OBJECT:
			case BSON.ARRAY: {
				int length = getBsonLength(inBytes, offset, l2r);
				bsonEndianConvert(inBytes, offset, length, l2r);
				offset += length;
				break;
			}
			case BSON.BINARY: {
				int length = Helper.byteToInt(inBytes, offset);
				arrayReverse(inBytes, offset, 4);
				int newLength = Helper.byteToInt(inBytes, offset);
				offset += (l2r ? newLength : length) + 5;
				break;
			}
			case BSON.UNDEFINED:
			case BSON.NULL:
			case BSON.MAXKEY:
			case BSON.MINKEY:
				break;
			case BSON.OID:
				offset += 12;
				break;
			case BSON.BOOLEAN:
				offset += 1;
				break;
			case BSON.DATE:
				arrayReverse(inBytes, offset, 8);
				offset += 8;
				break;
			case BSON.REGEX:
				// string regex
				offset += getStrLength(inBytes, offset) + 1;
				// string option
				offset += getStrLength(inBytes, offset) + 1;
				break;
			case BSON.REF: {
				offset += 12;
				// 4 bytes length + string + 12 bytes
				int length = Helper.byteToInt(inBytes, offset);
				arrayReverse(inBytes, offset, 4);
				int newLength = Helper.byteToInt(inBytes, offset);
				offset += (l2r ? newLength : length) + 4;
				offset += 12;
				break;
			}
			case BSON.CODE_W_SCOPE: {
				// 4 bytes
				arrayReverse(inBytes, offset, 4);
				offset += 4;
				// string
				int length = Helper.byteToInt(inBytes, offset);
				arrayReverse(inBytes, offset, 4);
				int newLength = Helper.byteToInt(inBytes, offset);
				offset += (l2r ? newLength : length) + 4;
				// then object
				int objLength = getBsonLength(inBytes, offset, l2r);
				bsonEndianConvert(inBytes, offset, objLength, l2r);
				offset += objLength;
				break;
			}
			case BSON.NUMBER_INT:
				arrayReverse(inBytes, offset, 4);
				offset += 4;
				break;
			case BSON.TIMESTAMP:
				// 2 4-bytes
				arrayReverse(inBytes, offset, 4);
				offset += 4;
				arrayReverse(inBytes, offset, 4);
				offset += 4;
				break;
			case BSON.NUMBER_LONG:
				arrayReverse(inBytes, offset, 8);
				offset += 8;
				break;
			}
		}
		if (offset - begin != objSize)
			throw new BaseException("SDB_INVALIDSIZE");

	}

	private static int getBsonLength(byte[] inBytes, int offset,
			boolean endianConvert) {
		byte[] tmp = new byte[4];
		for (int i = 0; i < 4; i++)
			tmp[i] = inBytes[offset + i];

		return Helper.byteToInt(tmp, endianConvert);
	}

	private static void arrayReverse(byte[] array, int begin, int length) {
		int i = begin;
		int j = begin + length - 1;
		byte tmp;
		while (i < j) {
			tmp = array[i];
			array[i] = array[j];
			array[j] = tmp;
			++i;
			--j;
		}
	}

	private static int getStrLength(byte[] array, int begin) {
		int length = 0;
		while (array[begin] != '\0') {
			++length;
			++begin;
		}
		return length;
	}

    public static void checkMessage(SDBMessage req, SDBMessage res) {
        checkMsgOpCode(req.getOperationCode(), res.getOperationCode());
        //checkMsgReqID(req.getRequestID(), res.getRequestID());
    }
    
    public static void checkMsgOpCode(Operation reqOpCode, Operation resOpCode) {
        int reqCode = reqOpCode.getOperationCode();
        int resCode = resOpCode.getOperationCode();
        if ((reqCode|Operation.RES_FLAG) != resCode) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", 
                    ("request=" + reqOpCode + " response=" + resOpCode));
        }
    }
    
    public static void checkMsgReqID(long reqID, long resID) {
        if (reqID != resID) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", "reqID is different"
                    + "reqID=" + reqID + " resID=" + resID);
        }
    }

}
