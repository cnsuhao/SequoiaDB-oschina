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
package com.sequoiadb.net;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.sequoiadb.util.Helper;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.util.logger;

/**
 * @author Jacky Zhang
 * 
 */
public class ConnectionTCPImpl implements IConnection {

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private ConfigOptions options;
	private ServerAddress hostAddress;
	private boolean endianConvert;
	private byte[] receive_buffer;
	final static private int DEF_BUFFER_LENGTH = 64 * 1024;
	private int REAL_BUFFER_LENGTH ;
	private long lastUseTime;

	public void setEndianConvert(boolean endianConvert) {
		this.endianConvert = endianConvert;
	}

	public boolean isEndianConvert() {
		return endianConvert;
	}

	public long getLastUseTime(){
		return lastUseTime;
	}
	public ConnectionTCPImpl(ServerAddress addr, ConfigOptions options) {
		this.hostAddress = addr;
		this.options = options;
		endianConvert = false;
		receive_buffer = new byte[DEF_BUFFER_LENGTH];
		REAL_BUFFER_LENGTH = DEF_BUFFER_LENGTH ;
	}

	private void connect() throws BaseException {
		logger.getInstance().debug(0, "enter connect\n");
		String objectidentity = Integer.toString(hashCode());
		logger.getInstance().debug(0, "objidentity:" + objectidentity +"\n");
		if (clientSocket != null) {
			return;
		}

		long sleepTime = 100;
		long maxAutoConnectRetryTime = options.getMaxAutoConnectRetryTime();
		long start = System.currentTimeMillis();
		while (true) {
			BaseException lastError = null;
			InetSocketAddress addr = hostAddress.getHostAddress();
			try {
				clientSocket = new Socket();
				clientSocket.connect(addr, options.getConnectTimeout());
				clientSocket.setTcpNoDelay(!options.getUseNagle());
				clientSocket.setKeepAlive(options.getSocketKeepAlive());
				clientSocket.setSoTimeout(options.getSocketTimeout());
				input = new BufferedInputStream(clientSocket.getInputStream());
				output = clientSocket.getOutputStream();
				logger.getInstance().debug(0, "leave connect\n");
				return;
			} catch (IOException ioe) {
				lastError = new BaseException("SDB_NETWORK", ioe);
				close();
			}
			long executedTime = System.currentTimeMillis() - start;
			if (executedTime >= maxAutoConnectRetryTime)
				throw lastError;

			if (sleepTime + executedTime > maxAutoConnectRetryTime)
				sleepTime = maxAutoConnectRetryTime - executedTime;

			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
			}
			sleepTime *= 2;
		}
	}

	public void close() {
		logger.getInstance().debug(0, "enter close\n");
		String trace = logger.getInstance().getStackMsg(); 
		logger.getInstance().debug(0, trace);
		if (clientSocket != null) {
			try {
				clientSocket.close();
			} catch (Exception e) {
			} finally {
				receive_buffer = null;
				REAL_BUFFER_LENGTH = 0;
				input = null;
				output = null;
				clientSocket = null;
			}
		}
		logger.getInstance().debug(0, "leave close\n");
		logger.getInstance().save();
	}

	public boolean isClosed() {
		if (clientSocket == null)
			return true;
		return clientSocket.isClosed();
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sequoiadb.net.IConnection#changeConfigOptions(com.sequoiadb.net.
	 * ConfigOptions)
	 */
	public void changeConfigOptions(ConfigOptions opts) throws BaseException {
		logger.getInstance().debug(0, "enter changeConfigOptions\n");
		this.options = opts;
		close();
		connect();
		logger.getInstance().debug(0, "leave changeConfigOptions\n");
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sequoiadb.net.IConnection#receiveMessage()
	 */
	public ByteBuffer receiveMessage(boolean endianConvert) throws BaseException {
		lastUseTime = System.currentTimeMillis();
		logger.getInstance().debug(0, "enter receiveMessage\n");
		try {
			if (REAL_BUFFER_LENGTH < DEF_BUFFER_LENGTH) {
				receive_buffer = new byte[DEF_BUFFER_LENGTH];
				REAL_BUFFER_LENGTH = DEF_BUFFER_LENGTH;
			}
			input.mark(4);
			int rtn = 0;
			while (rtn < 4){
				int retSize = input.read(receive_buffer, rtn, 4 - rtn);
				if (retSize == -1) {
					close();
					throw new BaseException("SDB_NETWORK"); 
				}
				rtn += retSize;
			}
			/*
			int rtn = input.read(MESSAGE_BUFFER, 0, 4);
			if (rtn != 4) {
				close();
				throw new BaseException("SDB_NETWORK");
			}
			*/
			int msgSize = Helper.byteToInt(receive_buffer, endianConvert);
			if (msgSize > REAL_BUFFER_LENGTH)
			{
				receive_buffer = new byte[msgSize];
				REAL_BUFFER_LENGTH = msgSize;
			}
			input.reset();
			rtn = 0;
			int retSize = 0;
			while (rtn < msgSize) {
				retSize = input.read(receive_buffer, rtn, msgSize - rtn);
				if (-1 == retSize) {
					close();
					throw new BaseException("SDB_NETWORK");
				}
				rtn += retSize;
			}
			
			if (rtn != msgSize) {
				close();
				throw new BaseException("SDB_NETWORK");
			}
/*
			if (rtn != msgSize) {
				StringBuffer bbf = new StringBuffer();
				for (byte by : MESSAGE_BUFFER) {
					bbf.append(String.format("%02x", by));
				}
				close();
				throw new BaseException("SDB_INVALIDARG");
			}
*/			
 			ByteBuffer byteBuffer = ByteBuffer.wrap(receive_buffer, 0 , msgSize );
			if (endianConvert) {
				byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			} else {
				byteBuffer.order(ByteOrder.BIG_ENDIAN);
			}
			logger.getInstance().debug(0, "leave receiveMessage\n");
			return byteBuffer;
			
		} catch (IOException e) {
			throw new BaseException("SDB_NETWORK");
		} catch(NullPointerException e) {
			logger.getInstance().error("objidentity:" + Integer.toString(hashCode()) +"\n");
			logger.getInstance().error("thread id:" + Long.toString(Thread.currentThread().getId()) +"\n");
			throw new BaseException("SDB_NETWORK");
		}
	}

	public byte[] receiveSysInfoMsg(int msgSize) throws BaseException {
		logger.getInstance().debug(0, "enter receiveSysInfoMsg\n");
		byte[] buf = new byte[msgSize];
		
		try {
			   int rtn = 0;
			   int retSize = 0;
               while (rtn < msgSize) {
            	   retSize = input.read(buf, rtn, msgSize - rtn);
            	   if (-1 == retSize) {
                       close();
                       throw new BaseException("SDB_NETWORK");
            	   }
                   rtn += retSize;
               }

               if (rtn != msgSize) {
            	   StringBuffer bbf = new StringBuffer();
            	   for (byte by : buf) {
            		   bbf.append(String.format("%02x", by));
            	   }
                   close();
                   throw new BaseException("SDB_INVALIDARG");
               }
			} catch (IOException e) {
				throw new BaseException("SDB_NETWORK");
			}catch(NullPointerException e){
				logger.getInstance().error("objidentity:" + Integer.toString(hashCode()) +"\n");
				logger.getInstance().error("thread id:" + Long.toString(Thread.currentThread().getId()) +"\n");
				throw new BaseException("SDB_NETWORK");
			}
		logger.getInstance().debug(0, "leave receiveSysInfoMsg\n");
		return buf;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sequoiadb.net.IConnection#initialize()
	 */
	public void initialize() throws BaseException {
		connect();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sequoiadb.net.IConnection#sendMessage(byte[], int)
	 */
	public void sendMessage(byte[] msg) throws BaseException {
		logger.getInstance().debug(0, "enter sendMessage\n");
		try {
			if(output != null) {
				output.write(msg);
			}
		} catch (IOException e) {
			throw new BaseException("SDB_NETWORK", e);
		}
		logger.getInstance().debug(0, "leave sendMessage\n");
	}
	
	public void sendMessage(byte[] msg, int length) throws BaseException {
	    logger.getInstance().debug(0, "enter sendMessage2\n");
        try {
            if (output != null) {
                output.write(msg, 0, length);
            }
            else{
            	throw new BaseException("SDB_NETWORK");
            }
        } catch (IOException e) {
            	throw new BaseException("SDB_NETWORK", e);
        }
		logger.getInstance().debug(0, "leave sendMessage2\n");
	}
	
	public void shrinkBuffer() {
		if (REAL_BUFFER_LENGTH != DEF_BUFFER_LENGTH) {
			receive_buffer = new byte[DEF_BUFFER_LENGTH];
			REAL_BUFFER_LENGTH = DEF_BUFFER_LENGTH ;
		}
	}
}
