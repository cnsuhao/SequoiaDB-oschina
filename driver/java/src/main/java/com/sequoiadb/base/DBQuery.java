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
 * @brief SequoiaDB Driver for Java
 * @author Jacky Zhang
 */
package com.sequoiadb.base;

import org.bson.BSONObject;

/**
 * @class DBQuery
 * @brief Database operation rules.
 */
public class DBQuery {
	private BSONObject matcher;
	private BSONObject selector;
	private BSONObject orderBy;
	private BSONObject hint;
	private BSONObject modifier;
	private Long skipRowsCount;
	private Long returnRowsCount;
	private int flag;

	/**
	 * @memberof FLG_QUERY_STRINGOUT 0x00000001
	 * @brief Normally, query return bson stream, 
	 *        when this flag is added, query return binary data stream
	 */
	public static final int FLG_QUERY_STRINGOUT = 0x00000001;
	
	/**
	 * @memberof FLG_INSERT_CONTONDUP 0x00000080
	 * @brief Force to use specified hint to query,
	 *        if database have no index assigned by the hint, fail to query
	 */
	public static final int FLG_QUERY_FORCE_HINT = 0x00000080;
	
	/**
	 * @memberof FLG_QUERY_PARALLED 0x00000100
	 * @brief Enable paralled sub query
	 */
	public static final int FLG_QUERY_PARALLED = 0x00000100;
	
	/**
     * @memberof FLG_QUERY_WITH_RETURNDATA 0x00000200
     * @brief return data in query response
     */
	public static final int FLG_QUERY_WITH_RETURNDATA = 0x00000200;
	
	/**
	 * @memberof FLG_QUERY_EXPLAIN 0x00000400
	 * @brief explain query
	 */
	public static final int FLG_QUERY_EXPLAIN  = 0x00000400;
	
	public DBQuery() {
		matcher = null;
		selector = null;
		orderBy = null;
		hint = null;
		modifier = null;
		skipRowsCount = 0L;
		returnRowsCount = -1L;
		flag = 0;
	}
	
	/**
	 * @fn BSONObject getModifier()
	 * @brief Get modified rule
	 * @return The modified rule BSONObject
	 */
	public BSONObject getModifier() {
		return modifier;
	}

	/**
	 * @fn void setModifier(BSONObject modifier)
	 * @brief Set modified rule
	 * @param Modifier The modified rule BSONObject
	 */
	public void setModifier(BSONObject modifier) {
		this.modifier = modifier;
	}

	/**
	 * @fn BSONObject getSelector()
	 * @brief Get selective rule
	 * @return The selective rule BSONObject
	 */
	public BSONObject getSelector() {
		return selector;
	}

	/**
	 * @fn void setSelector(BSONObject selector)
	 * @brief Set selective rule
	 * @param Selector The selective rule BSONObject
	 */
	public void setSelector(BSONObject selector) {
		this.selector = selector;
	}

	/**
	 * @fn BSONObject getMatcher()
	 * @brief Get matching rule
	 * @return The matching rule BSONObject
	 */
	public BSONObject getMatcher() {
		return matcher;
	}

	/**
	 * @fn void setMatcher(BSONObject matcher)
	 * @brief Set matching rule
	 * @param Matcher The matching rule BSONObject
	 */
	public void setMatcher(BSONObject matcher) {
		this.matcher = matcher;
	}

	/**
	 * @fn BSONObject getOrderBy()
	 * @brief Get ordered rule
	 * @return The ordered rule BSONObject
	 */
	public BSONObject getOrderBy() {
		return orderBy;
	}

	/**
	 * @fn void setOrderBy(BSONObject orderBy)
	 * @brief Set ordered rule
	 * @param OrderBy The ordered rule BSONObject
	 */
	public void setOrderBy(BSONObject orderBy) {
		this.orderBy = orderBy;
	}

	/**
	 * @fn BSONObject getHint()
	 * @brief Get sepecified access plan
	 * @return The sepecified access plan BSONObject
	 */
	public BSONObject getHint() {
		return hint;
	}

	/**
	 * @fn void setHint(BSONObject hint)
	 * @brief Set sepecified access plan
	 * @param Hint The sepecified access plan BSONObject
	 */
	public void setHint(BSONObject hint) {
		this.hint = hint;
	}

	/**
	 * @fn Long getSkipRowsCount()
	 * @brief Get the count of BSONObjects to skip
	 * @return The count of BSONObjects to skip
	 */
	public Long getSkipRowsCount() {
		return skipRowsCount;
	}

	/**
	 * @fn void setSkipRowsCount(Long skipRowsCount)
	 * @brief Set the count of BSONObjects to skip
	 * @param SkipRowsCount The count of BSONObjects to skip
	 */
	public void setSkipRowsCount(Long skipRowsCount) {
		this.skipRowsCount = skipRowsCount;
	}

	/**
	 * @fn Long getReturnRowsCount()
	 * @brief Get the count of BSONObjects to return
	 * @return The count of BSONObjects to return
	 */
	public Long getReturnRowsCount() {
		return returnRowsCount;
	}

	/**
	 * @fn void setReturnRowsCount(Long returnRowsCount)
	 * @brief Set the count of BSONObjects to return
	 * @param ReturnRowsCount The count of BSONObjects to return
	 */
	public void setReturnRowsCount(Long returnRowsCount) {
		this.returnRowsCount = returnRowsCount;
	}

	/**
	 * @fn int getFlag()
	 * @brief Get the query
	 * @return The query flag
	 * @see com.sequoiadb.base.DBCollection.query
	 */
	public int getFlag() {
		return flag;
	}

	/**
	 * @fn void setFlag(int flag)
	 * @brief Set the query flag
	 * @param The query flag as below:
	 *  	  DBQuery.FLG_QUERY_STRINGOUT
     *        DBQuery.FLG_QUERY_FORCE_HINT
     *        DBQuery.LG_QUERY_PARALLED  
     * @see com.sequoiadb.base.DBCollection.query
	 */
	public void setFlag(int flag) {
		this.flag = flag;
	}
	
}
