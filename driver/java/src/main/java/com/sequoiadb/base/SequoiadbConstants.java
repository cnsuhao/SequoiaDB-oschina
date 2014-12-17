/**
 *      Copyright (C) 2012 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE= -2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.sequoiadb.base;

import org.bson.BSONObject;

import com.sequoiadb.exception.BaseException;

/**
 * @author Jacky Zhang
 * 
 */
public class SequoiadbConstants {
	public final static String OP_ERRNOFIELD = "errno";
	public final static int OP_MAXNAMELENGTH = 255;

	public final static int COLLECTION_SPACE_MAX_SZ = 127;
	public final static int COLLECTION_MAX_SZ = 127;
	public final static int MAX_CS_SIZE = 127;
	
	public final static int MAX_MSG_LENGTH = 512 * 1024 * 1024;

	public static final String UNKNOWN_TYPE = "UNKNOWN";
	public static final String UNKNOWN_DESC = "Unknown Error";
	public static final int UNKNOWN_CODE = 1;

	public static final String ADMIN_PROMPT = "$";
	public static final String LIST_CMD = "list";
	public static final String CREATE_CMD = "create";
	public static final String REMOVE_CMD = "remove";
	public static final String DROP_CMD = "drop";
	public static final String SNAP_CMD = "snapshot";
	public static final String TEST_CMD = "test";
	public static final String ACTIVE_CMD = "active";
	public static final String SHUTDOWN_CMD = "shutdown";
	public final static String COLSPACES = "collectionspaces";
	public final static String COLLECTIONS = "collections";
	public final static String COLSPACE = "collectionspace";
	public final static String NODE = "node";
	public final static String NODE_NAME_SEP = ":";
	public final static String CONTEXTS = "contexts";
	public final static String CONTEXTS_CUR = "contexts current";
	public final static String SESSIONS = "sessions";
	public final static String SESSIONS_CUR = "sessions current";
	public final static String STOREUNITS = "storageunits";
	public final static String COLLECTION = "collection";
	public final static String CREATE_INX = "create index";
	public final static String DROP_INX = "drop index";
	public final static String GET_INXES = "get indexes";
	public final static String GET_COUNT = "get count";
	public final static String GET_DATABLOCKS = "get datablocks";
	public final static String GET_QUERYMETA  = "get querymeta";
	public final static String DATABASE = "database";
	public final static String SYSTEM = "system";
	public final static String CATA = "catalog";
	public final static String RESET = "reset";
	public final static String RENAME_COLLECTION = "rename collection";
	public final static String GROUP = "group";
	public final static String GROUPS = "groups";
	public final static String LIST_LOBS = "list lobs";

	public final static String SDB_AUTH_USER = "User";
	public final static String SDB_AUTH_PASSWD = "Passwd";

	public final static String CMD_NAME_LIST_CONTEXTS = "list contexts";
	public final static String CMD_NAME_LIST_CONTEXTS_CURRENT = "list contexts current";
	public final static String CMD_NAME_LIST_SESSIONS = "list sessions";
	public final static String CMD_NAME_LIST_SESSIONS_CURRENT = "list sessions current";
	public final static String CMD_NAME_LIST_COLLECTIONS = "list collections";
	public final static String CMD_NAME_LIST_COLLECTIONSPACES = "list collectionspaces";
	public final static String CMD_NAME_LIST_STORAGEUNITS = "list storageunits";
	public final static String CMD_NAME_LIST_GROUPS = "list groups";
	public final static String CMD_NAME_LIST_PROCEDURES = "list procedures";
	public final static String CMD_NAME_LIST_DOMAINS = "list domains";
	public final static String CMD_NAME_LIST_TASKS = "list tasks";
	public final static String CMD_NAME_LIST_CS_IN_DOMAIN = "list collectionspaces in domain";
	public final static String CMD_NAME_LIST_CL_IN_DOMAIN = "list collections in domain";
	public final static String CMD_NAME_CREATE_GROUP = "create group";
	public final static String CMD_NAME_REMOVE_GROUP = "remove group";
	public final static String CMD_NAME_REMOVE_NODE = "remove node";
	public final static String CMD_NAME_ACTIVE_GROUP = "active group";
	public final static String CMD_NAME_STARTUP_NODE = "startup node";
	public final static String CMD_NAME_SHUTDOWN_NODE = "shutdown node";
	public final static String CMD_NAME_SPLIT = "split";
	public final static String CMD_NAME_CREATE_CATA_GROUP = "create catalog group";
	public final static String CMD_NAME_EXPORT_CONFIG = "export configuration";
	public final static String CMD_NAME_BACKUP_OFFLINE = "backup offline";
	public final static String CMD_NAME_LIST_BACKUP = "list backups";
	public final static String CMD_NAME_REMOVE_BACKUP = "remove backup";
	public final static String CMD_NAME_CRT_PROCEDURE = "create procedure";
	public final static String CMD_NAME_RM_PROCEDURE = "remove procedure";
	public final static String CMD_NAME_EVAL = "eval";
	public final static String CMD_NAME_ATTACH_CL = "link collection";
	public final static String CMD_NAME_DETACH_CL = "unlink collection";
	public final static String CMD_NAME_SETSESS_ATTR = "set session attribute";
	public final static String CMD_NAME_LIST_TASK = "list tasks";             
	public final static String CMD_NAME_WAITTASK = "wait task";
	public final static String CMD_NAME_CANCEL_TASK = "cancel task";
	public final static String CMD_NAME_CREATE_DOMAIN = "create domain";
	public final static String CMD_NAME_DROP_DOMAIN = "drop domain";
	public final static String CMD_NAME_ALTER_DOMAIN = "alter domain";
	public final static String CMD_NAME_ADD_DOMAIN_GROUP = "add domain group";
	public final static String CMD_NAME_REMOVE_DOMAIN_GROUP = "remove domain group";
	public final static String CMD_NAME_ALTER_COLLECTION = "alter collection";
	
	public final static String FIELD_NAME_NAME = "Name";
	public final static String FIELD_NAME_OLDNAME = "OldName";
	public final static String FIELD_NAME_NEWNAME = "NewName";
	public final static String FIELD_NAME_PAGE_SIZE = "PageSize";
	public final static String FIELD_NAME_HOST = "HostName";
	public final static String FIELD_NAME_COLLECTIONSPACE = "CollectionSpace";
	public final static String FIELD_NAME_GROUPNAME = "GroupName";
	public final static String FIELD_NAME_GROUPSERVICE = "Service";
	public final static String FIELD_NAME_GROUP = "Group";
	public final static String FIELD_NAME_NODEID = "NodeID";
	public final static String FIELD_NAME_GROUPID = "GroupID";
	public final static String FIELD_NAME_PRIMARY = "PrimaryNode";
	public final static String FIELD_NAME_SERVICENAME = "Name";
	public final static String FIELD_NAME_SERVICETYPE = "Type";
	public final static String FIELD_NAME_SOURCE = "Source";
	public final static String FIELD_NAME_TARGET = "Target";
	public final static String FIELD_NAME_SPLITQUERY = "SplitQuery";
	public final static String FIELD_NAME_SPLITENDQUERY = "SplitEndQuery";
	public final static String FIELD_NAME_SPLITPERCENT = "SplitPercent";
	public final static String FIELD_NAME_CATALOGSHARDINGKEY = "ShardingKey";
	public final static String FIELD_NAME_PATH = "Path";
	public final static String FIELD_NAME_DESP = "Description";
	public final static String FIELD_NAME_ENSURE_INC = "EnsureInc";
	public final static String FIELD_NAME_OVERWRITE = "OverWrite";
	public final static String FIELD_NAME_FUNC = "func";
	public final static String FIELD_NAME_FUNCTYPE = FIELD_NAME_FUNC;
	public final static String FIELD_NAME_SUBCLNAME = "SubCLName";
	public final static String FIELD_NAME_HINT = "Hint";
	public final static String FIELD_NAME_ASYNC = "Async";
	public final static String FIELD_NAME_TASKTYPE = "TaskType";
	public final static String FIELD_NAME_TASKID = "TaskID";
	public final static String FIELD_NAME_OPTIONS = "Options";
	public final static String FIELD_NAME_DOMAIN = "Domain";
	
	public final static String FIELD_NAME_LOB_OPEN_MODE = "Mode";
	public final static String FIELD_NAME_LOB_OID = "Oid";
	public final static String FIELD_NAME_LOB_SIZE = "Size";
    public final static String FIELD_NAME_LOB_CREATTIME = "CreateTime";
	
	public final static String FMP_FUNC_TYPE = "funcType";
	public final static String FIELD_COLLECTION = "Collection";
	public final static String FIELD_TOTAL = "Total";
	public final static String FIELD_INDEX = "Index";
	public final static String FIELD_NAME_PREFERED_INSTANCE = "PreferedInstance";

	public final static String IXM_NAME = "name";
	public final static String IXM_KEY = "key";
	public final static String IXM_UNIQUE = "unique";
	public final static String IXM_ENFORCED = "enforced";
	public final static String IXM_INDEXDEF = "IndexDef";

	public final static String PMD_OPTION_SVCNAME = "svcname";
	public final static String PMD_OPTION_DBPATH = "dbpath";

	public final static String OID = "_id";

	public final static int FLG_UPDATE_UPSERT = 0x00000001;
	public final static int FLG_REPLY_CONTEXTSORNOTFOUND = 0x00000001;
	public final static int FLG_REPLY_SHARDCONFSTALE = 0x00000004;
	public final static int MSG_SYSTEM_INFO_LENGTH = 0xFFFFFFFF;
	public final static int MSG_SYSTEM_INFO_EYECATCHER = 0xFFFEFDFC;
	public final static int MSG_SYSTEM_INFO_EYECATCHER_REVERT = 0xFCFDFEFF;
	

    public final static int SDB_EOF = 
                                new BaseException("SDB_EOF").getErrorCode();
	public final static int SDB_DMS_EOC = new BaseException("SDB_DMS_EOC")
			.getErrorCode();

	public final static String LITTLE_ENDIAN = "LITTLE_ENDIAN";
	public final static String BIG_ENDIAN = "BIG_ENDIAN";
	public final static String SYSTEM_ENDIAN = LITTLE_ENDIAN;

	public final static byte[] ZERO_NODEID = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0 };
	
	public final static int DEFAULT_VERSION    = 0;
	public final static short DEFAULT_W        = 1;
	public final static int DEFAULT_FLAGS      = 0;
	public final static long DEFAULT_CONTEXTID = -1;

	public enum Operation {
		OP_REPLY(1), OP_MSG(1000), OP_UPDATE(2001), OP_INSERT(2002),
		RESERVED(2003), OP_QUERY(2004), OP_GETMORE(2005), OP_DELETE(2006),
		OP_KILL_CONTEXT(2007), OP_DISCONNECT(2008), MSG_BS_INTERRUPTE(2009),
		OP_AGGREGATE(2019), MSG_AUTH_VERIFY(7000), MSG_AUTH_CRTUSR(7001),
		MSG_AUTH_DELUSR(7002), TRANS_BEGIN_REQ(2010), TRANS_COMMIT_REQ(2011),
		TRANS_ROLLBACK_REQ(2012), MSG_BS_LOB_OPEN_REQ(8001), 
		MSG_BS_LOB_WRITE_REQ(8002), MSG_BS_LOB_READ_REQ(8003),
		MSG_BS_LOB_REMOVE_REQ(8004), MSG_BS_LOB_UPDATE_REQ(8005),
		MSG_BS_LOB_CLOSE_REQ(8006);

		private int operationCode;

		private Operation(int code) {
			operationCode = code;
		}

		public int getOperationCode() {
			return operationCode;
		}

		public static Operation getByValue(int inVal) {
			Operation rtnOper = Operation.RESERVED;
			for (Operation oper : Operation.values()) {
				if (oper.getOperationCode() == inVal) {
					rtnOper = oper;
					break;
				}
			}
			return rtnOper;
		}
	}
	
	public enum PreferInstanceType {
		INS_TYPE_MIN(0),
		INS_NODE_1(1),
		INS_NODE_2(2),
		INS_NODE_3(3),
		INS_NODE_4(4),
		INS_NODE_5(5),
		INS_NODE_6(6),
		INS_NODE_7(7),
		INS_MASTER(8),
		INS_SLAVE(9),
		INS_ANYONE(10),
		INS_TYPE_MAX(11);
		
		private int typeCode;
		private PreferInstanceType(int typeCode) {
			this.typeCode = typeCode;
		}
		
		public int getCode() {
			return typeCode;
		}
	}
	
//	public enum SptReturnType {
//	   TYPE_VOID(0),
//	   TYPE_STR(1),
//	   TYPE_NUMBER(2),
//	   TYPE_OBJ(3),
//	   TYPE_BOOL(4),
//	   TYPE_RECORDSET(5),
//	   TYPE_CS(6),
//	   TYPE_CL(7),
//	   TYPE_RG(8),
//	   TYPE_RN(9);
//	   
//	   private int typeCode;
//	   
//	   private SptReturnType(int typeCode) {
//		   this.typeCode = typeCode;
//	   }
//	   
//	   public int getTypeCode() {
//		   return typeCode;
//	   }
//	   
//	   public static SptReturnType getByValue(int codeType) {
//		   SptReturnType retType = null;
//		   for (SptReturnType rt : values()) {
//			   if (rt.getTypeCode() == codeType) {
//				   retType = rt;
//				   break;
//			   }
//		   }
//		   return retType;
//	   }
//	}
	
}
