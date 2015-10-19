package com.sequoiadb.test;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;

public class Constants {

/* use in ci */
	public final static int SERVER                         = 50000;
  	public final static String HOST                        = "localhost"; // change this for you own mechine (rhel ubutu-dev1)
  	public final static String BACKUPPATH                  = "/opt/sequoiadb/database/test/backup";
  	
  	public final static String DATAPATH1                   = "/opt/sequoiadb/database/test/data1";
	public final static String DATAPATH2                   = "/opt/sequoiadb/database/test/data2";
	public final static String DATAPATH3                   = "/opt/sequoiadb/database/test/data3";
	public final static String DATAPATH4                   = "/opt/sequoiadb/database/test/data4";
	  
/* use in my own machine(tanzhaobo) */  	
	
	public final static String _HOST                       = HOST;
	public final static int _SERVER                        = SERVER;
	public final static int SERVER1                        = 58000;
	public final static int SERVER2                        = 58100;
	public final static int SERVER3                        = 58200;
	public final static String COOR_NODE_CONN              = HOST+":"+ SERVER;
	public final static String DATA_NODE_CONN              = HOST+":"+"60000";
	public final static String COOR_HOST_NAME              = HOST;
	public final static String CATALOG_HOST_NAME           = HOST;
	public final static int COOR_SERVER_PORT               = 50000;
	public final static int CATALOG_SERVER_PORT            = 60000;
	public final static int TEST_RN_PORT_1                 = 60100;
	public final static int TEST_RN_PORT_2                 = 60200;
	
	public final static String _DATAPATH1                  = DATAPATH1;
	public final static String _DATAPATH2                  = DATAPATH2;
	public final static String _DATAPATH3                  = DATAPATH3;

	
	public final static String LOGIN_USER_NAME             = "";
	public final static String LOGIN_PASS_WORD             = "";
	public final static String USER_NAME                   = "user";
	public final static String PASS_WORD                   = "pass";
	
	
	public final static String TEST_CS_NAME_1              = "testfoo";
	public final static String TEST_CS_NAME_2              = "testCS2";
	public final static String TEST_CS_NAME_3              = "SAMPLE";
	public final static String TEST_CS_NAME_11             = "集合空间";
	
	public final static String TEST_CL_NAME_1              = "testbar";
	public final static String TEST_CL_NAME_2              = "testCL2";
	public final static String TEST_CL_NAME_3              = "employee";
	public final static String TEST_MAINCL                 = "mainCL";
	public final static String TEST_CL_NAME_11             = "集合";
	public final static String TEST_CL_FULL_NAME1          = "testfoo.testbar";
	public final static String TEST_CL_FULL_NAME2          = "testCS2.testCL2";
	public final static String TEST_CL_FULL_NAME11         = "集合空间.集合";
	public final static String TEST_CL_FULL_NAME3          = "SAMPLE.employee";
	public final static String TEST_CL_NEW_NAME            = "testCLNewName";
	public final static String TEST_SQL_CL                 = "testJavaSqlCL";
	public final static int SDB_PAGESIZE_4K                = 4*1024;
	public final static int SDB_PAGESIZE_8K                = 8*1024;
	public final static int SDB_PAGESIZE_16K               = 16*1024;
	public final static int SDB_PAGESIZE_32K               = 32*1024;
	public final static int SDB_PAGESIZE_64K               = 64*1024;
	
	public final static String GROUPNAME                   = "group1";
	public final static String GROUPNAME1                  = "testgroup1";
	public final static String GROUPNAME2                  = "testgroup2";
	public final static String GROUPNAME3                  = "testgroup3";
	public final static String TEST_RG_NAME_SRC            = "testRGSrc";
	public final static String TEST_RG_NAME_DEST           = "testRGDest";
	public final static String TEST_RG_NAME                = "javaTestRG1";
	public final static String CATALOGRGNAME                 = "SYSCatalogGroup";
	public final static int GROUPID                        = 1000;
	
	public final static String TEST_RN_HOSTNAME_SPLIT      = "vmsvr2-ubun-x64-2";
	public final static String TEST_RN_PORT_SPLIT          = "50100";
	public final static String TEST_RN_HOSTNAME_1          = "vmsvr2-ubun-x64-2";
	public final static String TEST_RN_DBPATH_1            = "/home/users/zhaorongsheng/sequoiadb/60100";
	public final static String TEST_RN_HOSTNAME_2          = "vmsvr2-ubun-x64-2";
	public final static String TEST_RN_DBPATH_2            = "/home/users/zhaorongsheng/sequoiadb/60200";
	public final static String TEST_INDEX_NAME             = "testIndexName";
	
	public final static String BACKUPNAME                  = "backup_in_java_test";
	public final static String BACKUPGROUPNAME             = GROUPNAME;

	public final static String TEST_DOMAIN_NAME            = "domain_java";
	
	public final static int GETDATABLOCKS_INSERT_NUM       = 1000;

	
	
	private static Sequoiadb sdb;
	
	public static boolean isCluster(){
	 sdb = new Sequoiadb(HOST+":"+SERVER, "", "");
	 try{
		 BSONObject empty = new BasicBSONObject();
		 sdb.getList(7, empty, empty, empty);
	 }catch (BaseException e){
		 if(e.getErrorType().equals("SDB_RTN_COORD_ONLY"));
		 return false;
	 }
	 return true;
	}
	

}
