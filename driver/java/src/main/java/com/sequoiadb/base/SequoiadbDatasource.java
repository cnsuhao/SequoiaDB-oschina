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
 * @brief SequoiaDB DataSource
 * @author tanzhaobo
 */
package com.sequoiadb.base;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import com.sequoiadb.exception.BaseException;
import com.sequoiadb.net.ConfigOptions;

/**
 * @class SequoiadbDatasource
 * @brief SequoiaDB DataSource
 */
public class SequoiadbDatasource
{
	// the idle queue
	private volatile LinkedList<Sequoiadb> idle_sequoiadbs = new LinkedList<Sequoiadb>();
	// the busy queue
	private volatile  LinkedList<Sequoiadb> used_sequoiadbs = new LinkedList<Sequoiadb>(); 
	// the normal coord addresses
	private volatile ArrayList<String> normal_urls = new ArrayList<String>();
	// the abnormal coord addresses
	private volatile ArrayList<String> abnormal_urls = new ArrayList<String>();
	private String username = null;
	private String password = null;
	// the configuration for network
	private ConfigOptions nwOpt = null;
	// the configuration for datasource
	private SequoiadbOption dsOpt = null;
	// a timer for clean idle connection
	private Timer timer = new Timer(true);
	// a timer for get back the useful coord address
	private Timer timer2 = new Timer(true);
	private final double MULTIPLE = 1.2;
	private final double MULTIPLE2 = 0.8;
	private Random rand = new Random(47);
	
	/**
	 * @fn int getIdleConnNum()
	 * @brief Get the current idle connection amount.
	 */
	public synchronized int getIdleConnNum()
	{
		return idle_sequoiadbs.size();
	}
	
	/**
	 * @fn int getUsedConnNum()
	 * @brief Get the current used connection amount.
	 */
	public synchronized int getUsedConnNum()
	{
		return used_sequoiadbs.size();
	}
	
	/**
	 * @fn int getNormalAddrNum()
	 * @brief Get the current normal address amount.
	 */
	public synchronized int getNormalAddrNum()
	{
		return normal_urls.size();
	}
	
	/**
	 * @fn int getAbnormalAddrNum()
	 * @brief Get the current abnormal address amount.
	 */
	public synchronized int getAbnormalAddrNum()
	{
		return abnormal_urls.size();
	}
	
	/**
	 * @fn SequoiadbDatasource(ArrayList<String> urls, String username, String password,
	 *		                   ConfigOptions nwOpt, SequoiadbOption dsOpt)
	 * @brief constructor.
	 * @param urls the addresses of coords, can't be null or empty,
	 *        e.g."ubuntu1:11810","ubuntu2:11810",...
	 * @param username the username for logging sequoiadb
	 * @param password the password for logging sequoiadb
	 * @param nwOpt the options for connection
	 * @param dsOpt the options for datasource  
	 * @note When offer several addresses for datasource to use, if 
	 *       some of them are not available(invalid address, network error, coord shutdown,
	 *       catalog replica group is not available), we will put these addresses
	 *       into a queue, and check them periodically. If some of them is valid again,
	 *       get them back for use. When datasource get a unavailable address to connect,
	 *       the default timeout time is 100ms, and default retry time is 0, use nwOpt can change them.
	 * @see ConfigOptions
	 * @see SequoiadbOption
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public SequoiadbDatasource(ArrayList<String> urls, String username, String password,
			ConfigOptions nwOpt, SequoiadbOption dsOpt) throws BaseException
	{
		if (null == urls || 0 == urls.size())
			throw new BaseException("SDB_INVALIDARG", urls);

		ConfigOptions temp = new ConfigOptions();
		temp.setConnectTimeout(100);
		temp.setMaxAutoConnectRetryTime(0);
		this.username = (null == username) ? "" : username;
		this.password = (null == password) ? "" : password;
		this.nwOpt = (null == nwOpt) ? temp : nwOpt;
		this.dsOpt = (null == dsOpt) ? new SequoiadbOption() : dsOpt;

		this.normal_urls.addAll(urls);

		try
		{
			init(this.dsOpt);
		}
		catch(BaseException e)
		{
			throw e;
		}
		// after the timer start for a while, it goes to clean periodically 
		timer.schedule(new CleanConnectionTask(this), dsOpt.getRecheckCyclePeriod(),
				       dsOpt.getRecheckCyclePeriod());
		// after the timer start 10 minutes, it goes to get back the useful coord address periodically 
		timer2.schedule(new RecaptureCoordAddrTask(this), dsOpt.getRecaptureConnPeriod(),
				        dsOpt.getRecaptureConnPeriod());
	}

	/**
	 * @fn SequoiadbDatasource(String url, String username, String password,
	 *		                   SequoiadbOption dsOpt)
	 * @brief Constructor.
	 * @param url the address of coord, can't be null, e.g."ubuntu1:11810"
	 * @param username the username of sequoiadb
	 * @param password the password of sequoiadb
	 * @param dsOpt the option of datasource
	 * @exception com.sequoiadb.exception.BaseException
	 */
	public SequoiadbDatasource(String url, String username, String password,
			SequoiadbOption dsOpt) throws BaseException
	{
		if (null == url)
			throw new BaseException("SDB_INVALIDARG", url);
		this.normal_urls.add(url);

		ConfigOptions temp = new ConfigOptions();
		temp.setConnectTimeout(100);
		temp.setMaxAutoConnectRetryTime(0);
		this.username = (null == username) ? "" : username;
		this.password = (null == password) ? "" : password;
		this.nwOpt = (null == nwOpt) ? temp : nwOpt;
		this.dsOpt = (null == dsOpt) ? new SequoiadbOption() : dsOpt;
		
		try
		{
			init(this.dsOpt);
		}
		catch(BaseException e)
		{
			throw e;
		}
		// after the timer start 500ms, it goes to clean periodically 
		timer.schedule(new CleanConnectionTask(this), dsOpt.getRecheckCyclePeriod(), dsOpt.getRecheckCyclePeriod());
	}	
	
	/**
	 * @fn void init(SequoiadbOption dsOpt)
	 * @brief Initialize datasource.
	 * @param dsOpt the configuration for datasource
	 * @exception com.sequoiadb.Exception.BaseException              
	 */
	private void init(SequoiadbOption dsOpt) throws BaseException
	{
		// check option
		if (dsOpt.getMaxConnectionNum() < 0)
			throw new BaseException("SDB_INVALIDARG",
					                "maxConnectionNum is negative: " + dsOpt.getMaxConnectionNum());
		// if no need to init, return directly
		if (dsOpt.getMaxConnectionNum() == 0)
			return ;
		// check option
		if (dsOpt.getMaxConnectionNum() < dsOpt.getInitConnectionNum())
			throw new BaseException("SDB_INVALIDARG", "maxConnectionNum is less than initConnectionNum, maxConnectionNum is " +
					dsOpt.getMaxConnectionNum() + ", initConnectionNum is " + dsOpt.getInitConnectionNum());
		if (dsOpt.getInitConnectionNum() < 0)
			throw new BaseException("SDB_INVALIDARG", 
					            "initConnectionNum is negative: " + dsOpt.getInitConnectionNum());
		// increase some connection
		increaseConn(dsOpt.getInitConnectionNum());
	}

	/**
	 * @fn String getCoordAddr()
	 * @brief Get coord url.
	 * @return one of the coord addresses
	 * @exception com.sequoiadb.Exception.BaseException              
	 */
	private synchronized String getCoordAddr() throws BaseException
	{
		if (1 < normal_urls.size())
			return normal_urls.get(rand.nextInt(normal_urls.size()));
		else if (1 == normal_urls.size())
			return normal_urls.get(0);
		else
			throw new BaseException("SDB_INVALIDARG", "no available address");
	}
	
	/**
	 * @fn String getBackCoordAddr()
	 * @brief Get back all the available coord addresses periodically.
	 * @exception com.sequoiadb.Exception.BaseException              
	 */
    synchronized void getBackCoordAddr()
	{
		Sequoiadb sdb = null;
		String addr = null;
		
		if (0 == abnormal_urls.size())
			return;
		for (int i = 0; i < abnormal_urls.size(); i++ )
		{
			addr = abnormal_urls.get(i);
			try
			{
				sdb = new Sequoiadb(addr, this.username, this.password, this.nwOpt);
				if (sdb.isValid()) // it seem no need to check here?
				{
					normal_urls.add(addr);
					abnormal_urls.remove(addr);
					i--;
				}
			}
			catch(BaseException e)
			{
			}
		}
	}
	
	/**
	 * @fn Sequoiadb getConnDrt()
	 * @brief Get a connection directly.
	 * @exception com.sequoiadb.Exception.BaseException              
	 */
    private synchronized Sequoiadb getConnDrt() throws BaseException
	{
    	Sequoiadb sdb = null;
		String addr = getCoordAddr();
		
		while (normal_urls.size() > 0)
		{
			try
			{
				sdb = new Sequoiadb(addr, this.username, this.password, this.nwOpt);
				break;
			}
			catch (BaseException e)
			{
				String errType = e.getErrorType();
				if (errType.equals("SDB_NETWORK") || errType.equals("SDB_INVALIDARG") ||
					errType.equals("SDB_NET_CANNOT_CONNECT"))
				{
					abnormal_urls.add(addr);
					normal_urls.remove(addr);
					addr = getCoordAddr();
					continue;
				}
				else
					throw e;
			}
		}

		// check whether we succeed to get connection or not
		if (null == sdb)
			throw new BaseException("SDB_INVALIDARG");
		return sdb;
	}
    
	/**
	 * @fn Sequoiadb getConnection()
	 * @brief  Get the connection from current datasource.
	 * @return Sequoiadb the connection for use
	 * @exception com.sequoiadb.Exception.BaseException
	 *            when datasource run out, throws BaseException with the type of "SDB_DRIVER_DS_RUNOUT"
	 * @exception InterruptedException
	 */
	public synchronized Sequoiadb getConnection() throws BaseException, InterruptedException
	{
		// when we don't want to use datasource, return sequiadb instance directly
		if (dsOpt.getMaxConnectionNum() == 0)
			return getConnDrt();
		// otherwise
		Sequoiadb sdb = null;
		if ((idle_sequoiadbs.size() > 0) && (used_sequoiadbs.size() < dsOpt.getMaxConnectionNum())) 
		{
			// get connection from idle queue if no connection, it return null
			sdb = idle_sequoiadbs.poll();
			// get a valid instance for return
			while((null != sdb) && (!sdb.isValid()))
			{
				sdb = idle_sequoiadbs.poll();
			}
			// if no valid instance in idle queue, let't create one for return
			if (null == sdb)
				sdb = getConnDrt();
			// add to busy queue
			used_sequoiadbs.add(sdb);
		}
		else
		{
			// if we run out all the connection, we need to wait for a moment
			if (used_sequoiadbs.size() >= dsOpt.getMaxConnectionNum())
			{
				wait(dsOpt.getTimeout());
				// when wake up, check again
				if (used_sequoiadbs.size() >= dsOpt.getMaxConnectionNum())
					throw new BaseException("SDB_DRIVER_DS_RUNOUT");
			}
			sdb = getConnDrt();
			used_sequoiadbs.add(sdb);
			// create a thread to increase connection
			Thread t = new Thread(new CreateConnectionTask(this));
			t.start();
		}
		return sdb;
	}
	
	/**
	 * @fn void close(Sequoiadb sdb)
	 * @brief Put the connection back to the datasource.
	 *        In the case of the connecton is not belong to current datasource or
	 *        the connecton is outdate(by default, if a connection has not be used for 10 minutes),
	 *        this API will disconnect directly.
	 * @param sdb the connection get from current datasource, can't be null 
	 * @exception com.sequoiadb.Exception.BaseException
	 *        if param sdb is null, throws BaseException with the type "SDB_INVALIDARG"
	 */
	public synchronized void close(Sequoiadb sdb) throws BaseException
	{
		if (null == sdb)
			throw new BaseException("SDB_INVALIDARG", sdb);
		// check option
		if (dsOpt.getAbandonTime() <= 0)
			throw new BaseException("SDB_INVALIDARG", "abandonTime is negative: " + dsOpt.getAbandonTime());
		if (dsOpt.getRecheckCyclePeriod() <= 0)
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is negative: " + dsOpt.getRecheckCyclePeriod());
		if (dsOpt.getRecheckCyclePeriod() >= dsOpt.getAbandonTime())
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is not less than abandonTime, recheckCyclePeriod is " +
					dsOpt.getRecheckCyclePeriod() + ", abandonTime is " + dsOpt.getAbandonTime());
		// if the busy queue contain this instance
		if (used_sequoiadbs.contains(sdb)) 
		{
			// remove it from busy queue
			used_sequoiadbs.remove(sdb);
			// when datasource not be used, abandon the connection directly
			if (dsOpt.getMaxConnectionNum() == 0)
			{
				sdb.disconnect();
				return;
			}
			// judge put it in idle queue or not
			long lastTime = sdb.getConnection().getLastUseTime();
			long currentTime = System.currentTimeMillis();
			// check the time keep in instance, don't let it go back to pool
			// when it timeout
			if (currentTime - lastTime + MULTIPLE * dsOpt.getRecheckCyclePeriod() >= dsOpt.getAbandonTime())
			{
				sdb.disconnect();
				return;
			}
			else
			{
				// close all cursor
				sdb.closeAllCursors();
				// release the heap memory or other resource holds in the instance
				sdb.releaseResource();
				// put it back to idle queue
				idle_sequoiadbs.add(sdb);
				notify();	
			}
		}
		// else, abandon it directly
		else
		{
			sdb.disconnect();
			return;
		}
	}
	
	/**
	 * @fn void increaseConnetions()
	 * @bref Add another 20 Sequoiadb objects to the datasource. 
	 */
	synchronized void increaseConnetions() throws BaseException
	{
		// when datasource is not used, return directly
		if (dsOpt.getMaxConnectionNum() == 0)
			return;
		// if the number of connection in the pool is less than SequoiadbOption::maxIdeNum
		// we are going to increase
		if (idle_sequoiadbs.size() >= dsOpt.getMaxIdeNum())
			return;
		if (dsOpt.getDeltaIncCount() < 0)
				throw new BaseException("SDB_INVALIDARG", 
						                "deltaIncCount is negative: " + dsOpt.getDeltaIncCount());
		if (dsOpt.getMaxConnectionNum() < dsOpt.getDeltaIncCount())
			throw new BaseException("SDB_INVALIDARG",
					                "deltaIncCount is greater than maxConnectionNum, deltaIncCount is " +
					dsOpt.getDeltaIncCount() + ", maxConnectionNum is " + dsOpt.getMaxConnectionNum());
		if (used_sequoiadbs.size() < dsOpt.getMaxConnectionNum() - dsOpt.getDeltaIncCount())
			increaseConn(dsOpt.getDeltaIncCount());
		else
			increaseConn(dsOpt.getMaxConnectionNum() - used_sequoiadbs.size());
	}
	
	/**
	 * @fn void cleanAbandonConnection()
	 * @brief  clean the tiemout connection periodically
	 */
	synchronized void cleanAbandonConnection() throws BaseException
	{
		// when no need to clean
		if ((0 == idle_sequoiadbs.size()) && (0 == used_sequoiadbs.size()))
			return ;
		// check option
		if (dsOpt.getAbandonTime() <= 0)
			throw new BaseException("SDB_INVALIDARG", "abandonTime is negative: " + dsOpt.getAbandonTime());
		if (dsOpt.getRecheckCyclePeriod() <= 0)
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is negative: " + dsOpt.getRecheckCyclePeriod());
		if (dsOpt.getRecheckCyclePeriod() >= dsOpt.getAbandonTime())
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is not less than abandonTime, recheckCyclePeriod is " +
					dsOpt.getRecheckCyclePeriod() + ", abandonTime is " + dsOpt.getAbandonTime());
		
		long lastTime = 0;
		long currentTime = System.currentTimeMillis();
		// remove the timeout connection in idle connection queue
		ListIterator<Sequoiadb> list1 = idle_sequoiadbs.listIterator(0);
		while(list1.hasNext())
		{
			Sequoiadb db = list1.next();
			lastTime = db.getConnection().getLastUseTime(); 
			if (currentTime - lastTime + MULTIPLE * dsOpt.getRecheckCyclePeriod() >= dsOpt.getAbandonTime())
			{
				db.disconnect () ;
				list1.remove();
			}
		}
		// remove the needless connection in idle connection queue
		for (int i = 0; i < idle_sequoiadbs.size() - dsOpt.getMaxIdeNum(); i++)
		{
			Sequoiadb db = idle_sequoiadbs.poll();
			db.disconnect();
			i--;
		}

	}
	
	/**
	 * @fn void increaseConn(int num)
	 * @brief Increase connections.
	 */
	private synchronized void increaseConn(int num)
	{
		if (num < 0)
			throw new BaseException("SDB_INVALIDARG");
		for (int i = 0; i < num; i++)
		{
			String coordAddr = getCoordAddr();
			Sequoiadb sdb = null;
			try
			{
				sdb = new Sequoiadb(coordAddr, username, password, this.nwOpt);
			}
			catch(BaseException e)
			{
				String errType = e.getErrorType();
				if (errType.equals("SDB_NETWORK") || errType.equals("SDB_INVALIDARG") ||
					errType.equals("SDB_NET_CANNOT_CONNECT"))
				{
					abnormal_urls.add(coordAddr);
					normal_urls.remove(coordAddr);
					i--;
					continue;
				}
				else
					throw e;
			}
			idle_sequoiadbs.add(sdb);
		}
	}
}

class CleanConnectionTask extends TimerTask
{
	private SequoiadbDatasource datasource;
	
	public CleanConnectionTask(SequoiadbDatasource ds)
	{
		datasource = ds;
	}
	@Override
	public void run()
	{
		datasource.cleanAbandonConnection();
	}
}

class CreateConnectionTask implements Runnable
{
	private SequoiadbDatasource datasource;
	
	public CreateConnectionTask(SequoiadbDatasource ds)
	{
		datasource = ds;
	}
	@Override
	public void run()
	{
		datasource.increaseConnetions();
	}
}

class RecaptureCoordAddrTask extends TimerTask
{
	private SequoiadbDatasource datasource;
	public RecaptureCoordAddrTask(SequoiadbDatasource ds){
		datasource = ds;
	}
	@Override
	public void run()
	{
		datasource.getBackCoordAddr();
	}
}
