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

import java.nio.ByteBuffer;

import com.sequoiadb.exception.BaseException;
/**
 * @author Jacky Zhang
 * 
 */
public interface IConnection {
	public void initialize() throws BaseException;
	public void close();
	public boolean isClosed();
	
	public void setEndianConvert(boolean endianConvert);
	public boolean isEndianConvert();
	public long getLastUseTime();
	public void changeConfigOptions(ConfigOptions opts) throws BaseException;

	public void sendMessage(byte[] msg) throws BaseException;
	
	public void sendMessage(byte[] msg, int length) throws BaseException;

	public byte[] receiveSysInfoMsg(int msgSize) throws BaseException;
	
	public ByteBuffer receiveMessage(boolean endianConvert) throws BaseException;
	
	public void shrinkBuffer();


}
