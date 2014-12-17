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
 * @brief  this option of SequoiadbDatasource 
 * @author gaosj
 */
package com.sequoiadb.base;

import com.sequoiadb.exception.BaseException;

/**
 * @class SequoiadbOption
 * @brief the option of SequoiadbDatasource
 */
public class SequoiadbOption
{
	// max connction count, if maxConnectionNum == 0,
	// it means the datasource doesn't work
	private int maxConnectionNum = 500;
	// while maxConnectionNum != 0, increase 10 connection to pool
    private int deltaIncCount = 10 ;
    // initialize 10 connection in pool
	private int initConnectionNum = 10;
	private int maxIdeNum = 10;
	// when busy queue is full, request can't get connection,
	// they will wait 5s and wake up
	private int timeout = 5 * 1000;
	// clean the abandon connection period
    private int recheckCyclePeriod = 1 * 60 * 1000;
    // check and get back the useful coord address period 
    private int recaptureConnPeriod = 10 * 60 * 1000;
    // when the last service time of a connection longer than this,
    // we will abandon it.
    private int abandonTime = 10 * 60 * 1000;

    
	/**
	 * @fn void setMaxConnectionNum(int maxConnectionNum)
	 * @brief Set the max number of connection. when maxConnectionNum is 0,
	 *        the datasource doesn't work. when get connecton, it build a connection and return it
	 *        directly. when a connection go back to pool, it disconnect the connection directly and
	 *        would not put it in pool. 
	 * @param  maxConnectionNum default to be 500 
     * @exception com.sequoiadb.exception.BaseException when maxConnectionNum is negative
	 */
	public void setMaxConnectionNum(int maxConnectionNum) throws BaseException
	{
		if (maxConnectionNum < 0)
			throw new BaseException("SDB_INVALIDARG", "maxConnectionNum is negative: " + maxConnectionNum);
		this.maxConnectionNum = maxConnectionNum;
	}
	/**
	 * @fn void getMaxConnectionNum()
	 * @brief Get the max number of connection.
	 */
	public int getMaxConnectionNum()
	{
		return maxConnectionNum;
	}
	   
	/**
	 * @fn void setDeltaIncCount(int deltaIncCount)
	 * @brief Set the number of new connections to open once running out the
	 *        connection pool.
	 * @param deltaIncCount default to be 10
     * @exception com.sequoiadb.exception.BaseException when deltaIncCount is negative
	 */
	public void setDeltaIncCount ( int deltaIncCount ) throws BaseException
	{
		if (deltaIncCount < 0)
			throw new BaseException("SDB_INVALIDARG", "deltaIncCount is negative: " + deltaIncCount);
		this.deltaIncCount = deltaIncCount ;
	}
	/**
	 * @fn void getDeltaIncCount()
	 * @brief Get the number of new connections to open once running out the
	 *        connection pool.
	 * @setDeltaIncCount
	 */
	public int getDeltaIncCount ()
	{
		return deltaIncCount ;
	}
	
	/**
	 * @fn void setInitConnectionNum(int initConnectionNum)
	 * @brief Set the initial number of connection.
	 * @param  initConnectionNum default to be 10
     * @exception com.sequoiadb.exception.BaseException when initConnectionNum is negative
	 */
	public void setInitConnectionNum(int initConnectionNum) throws BaseException
	{
		if (initConnectionNum < 0)
			throw new BaseException("SDB_INVALIDARG", "initConnectionNum is negative: " + initConnectionNum);
		this.initConnectionNum = initConnectionNum;
	}
	/**
	 * @fn void getInitConnectionNum()
	 * @brief Get the initial number of connection.
	 */
	public int getInitConnectionNum()
	{
		return initConnectionNum;
	}
	
	/**
	 * @fn void setMaxIdeNum(int maxIdeNum)
	 * @brief Set the max number of the free connection left in datasource
	 *        after periodically cleaning.
	 * @param maxIdeNum default to be 10
     * @exception com.sequoiadb.exception.BaseException when maxIdeNum is negative
	 */
	public void setMaxIdeNum(int maxIdeNum) throws BaseException
	{
		if (maxIdeNum < 0)
			throw new BaseException("SDB_INVALIDARG", "maxIdeNum is negative: " + maxIdeNum);
		this.maxIdeNum = maxIdeNum;
	}
	/**
	 * @fn void getMaxIdeNum()
	 * @brief Get the max number of the free connection.
	 * @see setMaxIdeNum
	 */
	public int getMaxIdeNum()
	{
		return maxIdeNum;
	}
	
   /**
    * @fn void setRecheckCyclePeriod(int recheckCyclePeriod)
    * @brief Set the recheck cycle in milliseconds. In each cycle
    *        datasource cleans all the discardable connection,
    *        and keep the number of valid connection not more than maxIdeNum. 
    * @param recheckCyclePeriod recheckCyclePeriod should be less than abandonTime. Default to be 1 * 60 * 1000ms
    * @exception com.sequoiadb.exception.BaseException when recheckCyclePeriod is negative
    * @note recheckCyclePeriod is used to initalize datasource, it will be use in the Constructor of datasource to initalize
    * a Timer instance, once it is set, we can not change the recheck cycle of the Timer instance by setRecheckCyclePeriod().
    * It's better to set abandonTime greater than recheckCyclePeriod twice over.
    */
	public void setRecheckCyclePeriod ( int recheckCyclePeriod ) throws BaseException
	{
		if (recheckCyclePeriod < 0)
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is negative: " + recheckCyclePeriod);
		this.recheckCyclePeriod = recheckCyclePeriod ;
	}
   /**
    * @fn void getRecheckCyclePeriod(int recheckCyclePeriod)
    * @brief get the recheck cycle
    * @see setRecheckCyclePeriod
    */
	public int getRecheckCyclePeriod ()
	{
	   return recheckCyclePeriod ;
	}

   /**
    * @fn void setTimeout(int timeout)
    * @brief Set the wait time in milliseconds. If the number of connection reaches
    *        maxConnectionNum, datasource can't offer connection immediately, the
    *        requests will be blocked to wait for a moment. When timeout, and there is
    *        still no available connection, datasource throws exception  
    * @param timeout default to be 5 * 1000ms
    * @exception com.sequoiadb.exception.BaseException when timeout is negative
    */
	public void setTimeout(int timeout) throws BaseException
	{
		if (timeout < 0)
			throw new BaseException("SDB_INVALIDARG", "timeout is negative: " + timeout);
		if (timeout == 0)
			timeout = 1;
		this.timeout = timeout;
	}
	/**
	 * @fn void getTimeout()
	 * @brief get the wait time. 
	 * @see setTimeout
	 */
	public int getTimeout()
	{
		return timeout;
	}

	/**
	 * @fn void setRecaptureConnPeriod(int recaptureConnPeriod)
	 * @brief Set the time in milliseconds for get back the useful address.
	 *        When offer several addresses for datasource to connect, if 
	 *        some of them are not available(invalid address, network error, coord shutdown,
	 *        catalog replica group is not available), we will put these addresses
	 *        into a queue, and check them periodically. If some of them is valid again,
	 *        get them back for use; 
	 * @param abandonTime default to be 10 * 60 * 1000ms
	 * @exception com.sequoiadb.exception.BaseException when abandonTime is negative
	 * @note It's better to set abandonTime greater than recheckCyclePeriod twice over.
	 */
	public void setRecaptureConnPeriod(int recaptureConnPeriod) throws BaseException
	{
		if (recaptureConnPeriod <= 0)
			throw new BaseException("SDB_INVALIDARG", "recaptureConnPeriod is negative or 0: "
		                            + recaptureConnPeriod);
		this.recaptureConnPeriod = recaptureConnPeriod;
	}
	/**
	 * @fn void getRecaptureConnPeriod()
	 * @brief Get the setup time for abandoning discardable connection.
	 * @setAbandonTime
	 */
	public int getRecaptureConnPeriod()
	{
		return recaptureConnPeriod;
	}
	
	/**
	 * @fn void setAbandonTime(int abandonTime)
	 * @brief Set the time in milliseconds for abandoning discardable connection.
	 *        If a connection has not be used for a long time(longer than abandonTime),
	 *        datasource would not let it come back to pool. And it will clean this kind of
	 *        connections in the pool periodically. 
	 * @param abandonTime default to be 10 * 60 * 1000ms
	 * @exception com.sequoiadb.exception.BaseException when abandonTime is negative
	 * @note It's better to set abandonTime greater than recheckCyclePeriod twice over.
	 */
	public void setAbandonTime(int abandonTime) throws BaseException
	{
		if (abandonTime <= 0)
			throw new BaseException("SDB_INVALIDARG", "abandonTime is negative or 0: " + abandonTime);
		this.abandonTime = abandonTime;
	}
	/**
	 * @fn void getAbandonTime()
	 * @brief Get the setup time for abandoning discardable connection.
	 * @setAbandonTime
	 */
	public int getAbandonTime()
	{
		return abandonTime;
	}

}
