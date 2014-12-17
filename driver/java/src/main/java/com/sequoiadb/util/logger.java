package com.sequoiadb.util;

import java.io.FileOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedOutputStream;
import java.util.*;
import java.util.Enumeration;

public class logger {
	private static final int LOG_LEVEL_ERROR = 2;
	private static final int LOG_LEVEL_FATAL = 2;
	private static final int LOG_LEVEL_WARN = 3;
	private static final int LOG_LEVEL_INFO = 4;
	private static final int LOG_LEVEL_DEBUG = 5;
	private static logger instance;
	private static boolean Iswritelog=false;
	private String logpath;
	private int loglevel;
	private Map<Long,Object> thrid2outputstream = new HashMap<Long, Object>();
	
	public static synchronized logger getInstance(){
		if (instance == null){
			instance = new logger();
		}
		return instance;
	}
	
	private logger(){
		InputStream in = null;
		try{
		    String filepath = System.getProperty("user.dir")+"/sdbdriver.properties";
		    System.out.println(filepath);
		    File file = new File(filepath);
		    if (file.exists())
		    {
		       in = new BufferedInputStream(new FileInputStream(filepath));
		       Iswritelog = true;
		    }
		    else
		    {
		       return;
		    }
		}catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}
		if (in != null){
			Properties properties = null;
			try{
				properties = new Properties();
				properties.load(in);
			}catch(IOException e){
				e.printStackTrace();
				return;
			}
			
			Enumeration em = properties.keys();
			while (em.hasMoreElements()) {
				String item = (String) em.nextElement();
				String itemvalue = (String) properties.get(item);
			  if (item.trim().equals("logpath")){
						logpath = itemvalue.trim();
						}
				else if (item.trim().equals("loglevel")){
						loglevel= Integer.parseInt(itemvalue.trim());
					}
				}	
			if (logpath != null){
			   File file = new File(logpath);
			   if (!file.exists()){
				   file.mkdir();
			   }
			}
		}
	}
	
	public BufferedOutputStream open(){
		if (!Iswritelog){
		   return null;
		}
		BufferedOutputStream fout = null;
		try{
			if (thrid2outputstream.containsKey(Thread.currentThread().getId())){
				fout = (BufferedOutputStream)thrid2outputstream.get(Thread.currentThread().getId());
				return fout;
			}
		
			String filenameSuffix="." + Long.toString(Thread.currentThread().getId());
			String filename = logpath + "/sdbdriver";
			filename += filenameSuffix;
			System.out.println(filename);
			File file = new File(filename);
			if (!file.exists()){
		     file.createNewFile();
			}
			fout = new BufferedOutputStream(new FileOutputStream(file, true));
			thrid2outputstream.put(Thread.currentThread().getId(), fout);
			
		}catch(IOException e){
			e.printStackTrace();
			return null;
		}
		return fout;
	}
	
	public void info(String msg){
		if (loglevel < LOG_LEVEL_INFO){
			return;
		}
	}
	
	public void warn(String msg){
		if (loglevel < LOG_LEVEL_WARN){
			return;
		}
		
	}
	
	public void fatal(String msg){
		if (loglevel < LOG_LEVEL_FATAL){
			return;
		}	
	}
	
	public void error(String msg){
		if (loglevel < LOG_LEVEL_ERROR){
			return;
		}
		
	}
	
	public void debug(int level, String msg){
		if (!Iswritelog){
		   return;
		}
		if (loglevel < LOG_LEVEL_DEBUG){
			return;
		}
		BufferedOutputStream fout = open();
		try{
			for (int i = 0; i < level; ++i){
				fout.write("-".getBytes());
			}
			fout.write(msg.getBytes());
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public void save(){
		if (!Iswritelog){
		   return;
		}
		if (loglevel < LOG_LEVEL_DEBUG){
			return;
		}
		BufferedOutputStream fout = open();
		try{
			fout.close();
			thrid2outputstream.remove(Thread.currentThread().getId());
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public String getStackMsg(){
		StackTraceElement[] stackArray = Thread.currentThread().getStackTrace();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < stackArray.length; ++i){
			StackTraceElement element = stackArray[i];
			sb.append(element.toString() + "\n");
		}
		
		return sb.toString();
	}
}

