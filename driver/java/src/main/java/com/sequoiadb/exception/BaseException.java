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
package com.sequoiadb.exception;

import com.sequoiadb.base.SequoiadbConstants;

/**
 * @author Jacky Zhang
 * 
 */
public class BaseException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6115487863398926195L;

	private SDBError error;
	private String infos = "";

	/**
	 * @param errorType
	 * @throws Exception
	 */
	public BaseException(String errorType, Object... info) {
		error = new SDBError();
		error.setErrorType(errorType);

		if (info != null) {
			for (Object obj : info)
				infos += (obj + " ");
		} else {
			infos = "no more exception info";
		}
		try {
			error.setErrorCode(SDBErrorLookup.getErrorCodeByType(errorType));
			error.setErrorDescription("errorType:" + errorType + "," 
			        + SDBErrorLookup.getErrorDescriptionByType(errorType) 
			        + "\n Exception Detail:" + infos);
		} catch (Exception e) {
			error.setErrorCode(0);
			error.setErrorDescription(SequoiadbConstants.UNKNOWN_DESC + "\n Exception Detail:"
					+ infos);
		}
	}

	/**
	 * 
	 * @param errorCode
	 * @throws Exception
	 */
	public BaseException(int errorCode, Object... info) {
		error = new SDBError();
		error.setErrorCode(errorCode);
		if (info != null) {
			for (Object obj : info)
				infos += (obj + " ");
		} else {
			infos = "no more exception info";
		}
		try {
			error.setErrorType(SDBErrorLookup.getErrorTypeByCode(errorCode));
			error.setErrorDescription("errorCode:" + errorCode + "," 
			        + SDBErrorLookup.getErrorDescriptionByCode(errorCode) 
			        + "\n Exception Detail:" + infos);
		} catch (Exception e) {
			error.setErrorType(SequoiadbConstants.UNKNOWN_TYPE);
			error.setErrorDescription(SequoiadbConstants.UNKNOWN_DESC + "\n Exception Detail:"
					+ infos);
		}
	}

	@Override
	public String getMessage() {
		return error.getErrorDescription();
	}

	public String getErrorType() {
		return error.getErrorType();
	}

	public int getErrorCode() {
		return error.getErrorCode();
	}
}
