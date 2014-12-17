// BasicBSONList.java

/**
 *      Copyright (C) 2008 10gen Inc.
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

package org.bson.types;

import org.bson.*;
import org.bson.util.StringRangeSet;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Utility class to allow array <code>DBObject</code>s to be created.
 * <p>
 * Note: MongoDB will also create arrays from <code>java.util.List</code>s.
 * </p>
 * <p>
 * <blockquote>
 * 
 * <pre>
 * DBObject obj = new BasicBSONList();
 * obj.put(&quot;0&quot;, value1);
 * obj.put(&quot;4&quot;, value2);
 * obj.put(2, value3);
 * </pre>
 * 
 * </blockquote> This simulates the array [ value1, null, value3, null, value2 ]
 * by creating the <code>DBObject</code>
 * <code>{ "0" : value1, "1" : null, "2" : value3, "3" : null, "4" : value2 }</code>
 * .
 * </p>
 * <p>
 * BasicBSONList only supports numeric keys. Passing strings that cannot be
 * converted to ints will cause an IllegalArgumentException. <blockquote>
 * 
 * <pre>
 * BasicBSONList list = new BasicBSONList();
 * list.put(&quot;1&quot;, &quot;bar&quot;); // ok
 * list.put(&quot;1E1&quot;, &quot;bar&quot;); // throws exception
 * </pre>
 * 
 * </blockquote>
 * </p>
 */
public class BasicBSONList extends ArrayList<Object> implements BSONObject {

	private static final long serialVersionUID = -4415279469780082174L;

	public BasicBSONList() {
	}

	/**
	 * Puts a value at an index. For interface compatibility. Must be passed a
	 * String that is parsable to an int.
	 * 
	 * @param key
	 *            the index at which to insert the value
	 * @param v
	 *            the value to insert
	 * @return the value
	 * @throws IllegalArgumentException
	 *             if <code>key</code> cannot be parsed into an <code>int</code>
	 */
	public Object put(String key, Object v) {
		return put(_getInt(key), v);
	}

	/**
	 * Puts a value at an index. This will fill any unset indexes less than
	 * <code>index</code> with <code>null</code>.
	 * 
	 * @param key
	 *            the index at which to insert the value
	 * @param v
	 *            the value to insert
	 * @return the value
	 */
	public Object put(int key, Object v) {
		while (key >= size())
			add(null);
		set(key, v);
		return v;
	}

	@SuppressWarnings("unchecked")
	public void putAll(Map m) {
		for (Map.Entry entry : (Set<Map.Entry>) m.entrySet()) {
			put(entry.getKey().toString(), entry.getValue());
		}
	}

	public void putAll(BSONObject o) {
		for (String k : o.keySet()) {
			put(k, o.get(k));
		}
	}

	/**
	 * Gets a value at an index. For interface compatibility. Must be passed a
	 * String that is parsable to an int.
	 * 
	 * @param key
	 *            the index
	 * @return the value, if found, or null
	 * @throws IllegalArgumentException
	 *             if <code>key</code> cannot be parsed into an <code>int</code>
	 */
	public Object get(String key) {
		int i = _getInt(key);
		if (i < 0)
			return null;
		if (i >= size())
			return null;
		return get(i);
	}

	public Object removeField(String key) {
		int i = _getInt(key);
		if (i < 0)
			return null;
		if (i >= size())
			return null;
		return remove(i);
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public boolean containsKey(String key) {
		return containsField(key);
	}

	public boolean containsField(String key) {
		int i = _getInt(key, false);
		if (i < 0)
			return false;
		return i >= 0 && i < size();
	}

	public Set<String> keySet() {
		return new StringRangeSet(size());
	}

	@SuppressWarnings("unchecked")
	public Map toMap() {
		Map m = new HashMap();
		Iterator i = this.keySet().iterator();
		while (i.hasNext()) {
			Object s = i.next();
			m.put(s, this.get(String.valueOf(s)));
		}
		return m;
	}

	int _getInt(String s) {
		return _getInt(s, true);
	}

	int _getInt(String s, boolean err) {
		try {
			return Integer.parseInt(s);
		} catch (Exception e) {
			if (err)
				throw new IllegalArgumentException(
						"BasicBSONList can only work with numeric keys, not: ["
								+ s + "]");
			return -1;
		}
	}

	/**
	 * Returns an instance of the class "type" only for BasicBsonObject
	 * 
	 * @param type
	 * @return the instance of the class
	 * @throws Exception
	 */
	//@Override
	public <T> T as(Class<T> type) throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
     * 
     */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	//@Override
	public <T> T as(Class<T> type, Type eleType) throws Exception {

		if (!Collection.class.isAssignableFrom(type)) {
			throw new IllegalArgumentException(
					"Current version only support as to subclass of java.util.Collection.");
		}

		Collection colletion = null;

		if (List.class.isAssignableFrom(type)) {
			colletion = new LinkedList();
		} else if (Set.class.isAssignableFrom(type)) {
			colletion = new TreeSet();
		} else if (Queue.class.isAssignableFrom(type)) {
			colletion = new LinkedList();
		} else {
			throw new IllegalArgumentException(
					"Current version not support type:" + type.getName());
		}

		Object eleObj = null;
		for (String key : this.keySet()) {
			eleObj = this.get(key);

			if (eleObj == null) {
				continue;
			} else if (BSON.IsBasicType(eleObj)) {
				colletion.add(eleObj);
			} else if (eleObj instanceof BasicBSONObject) {

				BSONObject comlexObj = (BSONObject) eleObj;
				colletion.add(comlexObj.as((Class<?>) eleType));
			} else if (eleObj instanceof BasicBSONList) {

				Type nestedEleType = null;
				if (eleType != null && eleType instanceof ParameterizedType) {
					nestedEleType = ((ParameterizedType) eleType)
							.getActualTypeArguments()[0];
				} else {
					throw new IllegalArgumentException(
							"Current version only support parameterized type field. unknow type="
									+ eleType.toString());
				}

				BSONObject comlexObj = (BSONObject) eleObj;
				colletion.add(comlexObj.as((Class<?>)(((ParameterizedType) eleType).getRawType()), nestedEleType));
			} else {
				throw new IllegalArgumentException(
						"Current version not support type:"
								+ eleObj.getClass().getName());
			}
		}

		return (T) colletion;
	}
}
