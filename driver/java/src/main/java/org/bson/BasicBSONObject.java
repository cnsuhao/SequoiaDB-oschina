
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

package org.bson;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

import org.bson.types.BSONTimestamp;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.bson.util.JSON;


/**
 * A simple implementation of <code>DBObject</code>. A <code>DBObject</code> can
 * be created as follows, using this class: <blockquote>
 * 
 * <pre>
 * DBObject obj = new BasicBSONObject();
 * obj.put(&quot;foo&quot;, &quot;bar&quot;);
 * </pre>
 * 
 * </blockquote>
 */
public class BasicBSONObject implements BSONObject {

	private static final long serialVersionUID = -4415279469780082174L;
	private Map<String, Object> _objectMap = null ;
	
	 /**
     * Creates an empty object.
     * 
     * @param sort
     *            true:  key will be sorted
     *            false: key won't be sorted. 
     */
    public BasicBSONObject(boolean sort) {
        if (sort) {
            _objectMap = new TreeMap<String, Object>() ;
        }
        else {
            _objectMap = new LinkedHashMap<String, Object>() ;
        }
    }

	/**
	 * Creates an empty object. by default, key won't be sorted
	 */
	public BasicBSONObject() {
	    this(false);
	}
	


	public BasicBSONObject(int size) {
		this(false);
	}

	public boolean isEmpty() {
		return _objectMap.size() == 0;
	}

	/**
	 * Convenience CTOR
	 * 
	 * @param key
	 *            key under which to store
	 * @param value
	 *            value to stor
	 */
	public BasicBSONObject(String key, Object value) {
	    this(false);
		put(key, value);
	}

	/**
	 * Creates a DBObject from a map.
	 * 
	 * @param m
	 *            map to convert
	 */
	@SuppressWarnings({"unchecked"})
	public BasicBSONObject(Map m) {
	    _objectMap = new LinkedHashMap<String, Object>(m) ;
	}

	/**
	 * Converts a DBObject to a map.
	 * 
	 * @return the DBObject
	 */
	public Map toMap() {
		return new LinkedHashMap<String, Object>(_objectMap);
	}

	/**
	 * Deletes a field from this object.
	 * 
	 * @param key
	 *            the field name to remove
	 * @return the object removed
	 */
	public Object removeField(String key) {
		return _objectMap.remove(key);
	}

	/**
	 * Checks if this object contains a given field
	 * 
	 * @param field
	 *            field name
	 * @return if the field exists
	 */
	public boolean containsField(String field) {
		return _objectMap.containsKey(field);
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public boolean containsKey(String key) {
		return containsField(key);
	}

	/**
	 * Gets a value from this object
	 * 
	 * @param key
	 *            field name
	 * @return the value
	 */
	public Object get(String key) {
		return _objectMap.get(key);
	}

	/**
	 * Returns the value of a field as an <code>int</code>.
	 * 
	 * @param key
	 *            the field to look for
	 * @return the field value (or default)
	 */
	public int getInt(String key) {
		Object o = get(key);
		if (o == null)
			throw new NullPointerException("no value for: " + key);

		return BSON.toInt(o);
	}

	/**
	 * Returns the value of a field as an <code>int</code>.
	 * 
	 * @param key
	 *            the field to look for
	 * @param def
	 *            the default to return
	 * @return the field value (or default)
	 */
	public int getInt(String key, int def) {
		Object foo = get(key);
		if (foo == null)
			return def;

		return BSON.toInt(foo);
	}

	/**
	 * Returns the value of a field as a <code>long</code>.
	 * 
	 * @param key
	 *            the field to return
	 * @return the field value
	 */
	public long getLong(String key) {
		Object foo = get(key);
		return ((Number) foo).longValue();
	}

	/**
	 * Returns the value of a field as an <code>long</code>.
	 * 
	 * @param key
	 *            the field to look for
	 * @param def
	 *            the default to return
	 * @return the field value (or default)
	 */
	public long getLong(String key, long def) {
		Object foo = get(key);
		if (foo == null)
			return def;

		return ((Number) foo).longValue();
	}

	/**
	 * Returns the value of a field as a <code>double</code>.
	 * 
	 * @param key
	 *            the field to return
	 * @return the field value
	 */
	public double getDouble(String key) {
		Object foo = get(key);
		return ((Number) foo).doubleValue();
	}

	/**
	 * Returns the value of a field as an <code>double</code>.
	 * 
	 * @param key
	 *            the field to look for
	 * @param def
	 *            the default to return
	 * @return the field value (or default)
	 */
	public double getDouble(String key, double def) {
		Object foo = get(key);
		if (foo == null)
			return def;

		return ((Number) foo).doubleValue();
	}

	/**
	 * Returns the value of a field as a string
	 * 
	 * @param key
	 *            the field to look up
	 * @return the value of the field, converted to a string
	 */
	public String getString(String key) {
		Object foo = get(key);
		if (foo == null)
			return null;
		return foo.toString();
	}

	/**
	 * Returns the value of a field as a string
	 * 
	 * @param key
	 *            the field to look up
	 * @param def
	 *            the default to return
	 * @return the value of the field, converted to a string
	 */
	public String getString(String key, final String def) {
		Object foo = get(key);
		if (foo == null)
			return def;

		return foo.toString();
	}

	/**
	 * Returns the value of a field as a boolean.
	 * 
	 * @param key
	 *            the field to look up
	 * @return the value of the field, or false if field does not exist
	 */
	public boolean getBoolean(String key) {
		return getBoolean(key, false);
	}

	/**
	 * Returns the value of a field as a boolean
	 * 
	 * @param key
	 *            the field to look up
	 * @param def
	 *            the default value in case the field is not found
	 * @return the value of the field, converted to a string
	 */
	public boolean getBoolean(String key, boolean def) {
		Object foo = get(key);
		if (foo == null)
			return def;
		if (foo instanceof Number)
			return ((Number) foo).intValue() > 0;
		if (foo instanceof Boolean)
			return ((Boolean) foo).booleanValue();
		throw new IllegalArgumentException("can't coerce to bool:"
				+ foo.getClass());
	}

	/**
	 * Returns the object id or null if not set.
	 * 
	 * @param field
	 *            The field to return
	 * @return The field object value or null if not found (or if null :-^).
	 */
	public ObjectId getObjectId(final String field) {
		return (ObjectId) get(field);
	}

	/**
	 * Returns the object id or def if not set.
	 * 
	 * @param field
	 *            The field to return
	 * @param def
	 *            the default value in case the field is not found
	 * @return The field object value or def if not set.
	 */
	public ObjectId getObjectId(final String field, final ObjectId def) {
		final Object foo = get(field);
		return (foo != null) ? (ObjectId) foo : def;
	}

	/**
	 * Returns the date or null if not set.
	 * 
	 * @param field
	 *            The field to return
	 * @return The field object value or null if not found.
	 */
	public Date getDate(final String field) {
		return (Date) get(field);
	}

	/**
	 * Returns the date or def if not set.
	 * 
	 * @param field
	 *            The field to return
	 * @param def
	 *            the default value in case the field is not found
	 * @return The field object value or def if not set.
	 */
	public Date getDate(final String field, final Date def) {
		final Object foo = get(field);
		return (foo != null) ? (Date) foo : def;
	}

	/**
	 * Add a key/value pair to this object
	 * 
	 * @param key
	 *            the field name
	 * @param val
	 *            the field value
	 * @return the <code>val</code> parameter
	 */
	public Object put(String key, Object val) {
		return _objectMap.put(key, val);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
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
	 * Add a key/value pair to this object
	 * 
	 * @param key
	 *            the field name
	 * @param val
	 *            the field value
	 * @return <code>this</code>
	 */
	public BasicBSONObject append(String key, Object val) {
		put(key, val);

		return this;
	}

	/**
	 * Returns a JSON serialization of this object
	 * 
	 * @return JSON serialization
	 */
	public String toString() {
		return JSON.serialize(this);
	}

	public boolean equals(Object o) {
		if (!(o instanceof BSONObject))
			return false;

		BSONObject other = (BSONObject) o;
		if (!keySet().equals(other.keySet()))
			return false;

		for (String key : keySet()) {
			Object a = get(key);
			Object b = other.get(key);

			if (a == null) {
				if (b != null)
					return false;
			}
			if (b == null) {
				if (a != null)
					return false;
			} else if (a instanceof Number && b instanceof Number) {
				if (((Number) a).doubleValue() != ((Number) b).doubleValue())
					return false;
			} else if (a instanceof Pattern && b instanceof Pattern) {
				Pattern p1 = (Pattern) a;
				Pattern p2 = (Pattern) b;
				if (!p1.pattern().equals(p2.pattern())
						|| p1.flags() != p2.flags())
					return false;
			} else {
				if (!a.equals(b))
					return false;
			}
		}
		return true;
	}

	@SuppressWarnings({"rawtypes"})
	public boolean BasicTypeWrite(Object object, Object field, Method method)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		Class<?> paramType = method.getParameterTypes()[0];
		boolean result = true;
                boolean numberCompare = false ;
		if (paramType.isPrimitive()) {

			if (paramType.getName().equals("int")) {
				method.invoke(object, ((Number) field).intValue());
			} else if (paramType.getName().equals("long")) {
				method.invoke(object, ((Number) field).longValue());
			} else if (paramType.getName().equals("byte")) {
				method.invoke(object, ((Number) field).byteValue());
			} else if (paramType.getName().equals("double")) {
				method.invoke(object, ((Number) field).doubleValue());
			} else if (paramType.getName().equals("float")) {
				method.invoke(object, ((Number) field).floatValue());
			} else if (paramType.getName().equals("short")) {
				method.invoke(object, ((Number) field).shortValue());
			} else if (paramType.getName().equals("char")) {
				method.invoke(object, ((Character) field).charValue());
			} else if(paramType.getName().equals("boolean")) {
				method.invoke(object, ((Boolean) field).booleanValue());//TODO
			}else{
				result = false;
			}

			return result;
		}

                if ( ( paramType.getName().equals("java.lang.Integer") ||
                       paramType.getName().equals("java.lang.Long") ||
                       paramType.getName().equals("java.lang.Float") ||
                       paramType.getName().equals("java.lang.Double") ) &&
                     ( field.getClass().getName().equals("java.lang.Integer") ||
                       field.getClass().getName().equals("java.lang.Long") ||
                       field.getClass().getName().equals("java.lang.Float") ||
                       field.getClass().getName().equals("java.lang.Double") ) )
                {
                   numberCompare = true ;
                }
		if (!numberCompare && !paramType.isInstance(field)) {
			throw new IllegalArgumentException("The method: "
					+ method.getName() + " Expected parameter type:"
					+ paramType.getName()
					+ " does not match with the actual type:"
					+ field.getClass().getName());
	        }	

		result = true;
		if (String.class.isAssignableFrom(paramType)) {
			method.invoke(object, (String) field);
		} else if (Date.class.isAssignableFrom(paramType)) {
			method.invoke(object, (Date) field);
		} else if (Integer.class.isAssignableFrom(paramType)) {
			method.invoke(object, new Integer(((Number)field).intValue()));
		} else if (Long.class.isAssignableFrom(paramType)) {
			method.invoke(object, new Long(((Number)field).longValue()));
		} else if (Double.class.isAssignableFrom(paramType)) {
			method.invoke(object, new Double(((Number)field).doubleValue()));
		} else if (Float.class.isAssignableFrom(paramType)) {
			method.invoke(object, new Float(((Number)field).floatValue()));
		} else if (Character.class.isAssignableFrom(paramType)) {
			method.invoke(object, (Character) field);
		} else if (ObjectId.class.isAssignableFrom(paramType)) {
			method.invoke(object, (ObjectId) field);
		} else if (Boolean.class.isAssignableFrom(paramType)) {
			method.invoke(object, (Boolean) field);
		} else if (Pattern.class.isAssignableFrom(paramType)) {
			method.invoke(object, (Pattern) field);
		} else if (byte[].class.isAssignableFrom(paramType)) {
			method.invoke(object, (byte[]) field);
		} else if (Binary.class.isAssignableFrom(paramType)) {
			method.invoke(object, (Binary) field);
		} else if (UUID.class.isAssignableFrom(paramType)) {
			method.invoke(object, (UUID) field);
		} else if (Symbol.class.isAssignableFrom(paramType)) {
			method.invoke(object, (Symbol) field);
		} else if (BSONTimestamp.class.isAssignableFrom(paramType)) {
			method.invoke(object, (BSONTimestamp) field);
		} else if (CodeWScope.class.isAssignableFrom(paramType)) {
			method.invoke(object, (CodeWScope) field);
		} else if (Code.class.isAssignableFrom(paramType)) {
			method.invoke(object, (Code) field);
		} else if (MinKey.class.isAssignableFrom(paramType))
			method.invoke(object, (MinKey) field);
		else if (MaxKey.class.isAssignableFrom(paramType)) {
			method.invoke(object, (MaxKey) field);
		} else {
			result = false;
		}
		return result;
	}

	/**
	 * Returns an instance of the class "type" only for BasicBsonObject
	 * 
	 * @param type
	 * @return the instance of the class
	 * @throws Exception
	 */
	public <T> T as(Class<T> type) throws Exception {
		return as(type, null);
	}

	@SuppressWarnings({"unchecked"}) 
	public <T> T as(Class<T> type, Type eleType) throws Exception {
		boolean hasConsturctor = false;
		T result = null;
		for (Constructor<?> con : type.getConstructors()) {
			if (con.getParameterTypes().length == 0) {
				result = (T) con.newInstance();
				hasConsturctor = true;
				break;
			}
		}
		if (hasConsturctor == false) {
			throw new Exception("Class " + type.getName()
					+ " does not exist an default constructor method");
		}

		if (BSON.IsBasicType(result)) {
			throw new IllegalArgumentException(
					"Not support as to basici type. type=" + type.getName());
		} else if (Collection.class.isAssignableFrom(type)
				|| Map.class.isAssignableFrom(type) || type.isArray()) {
			throw new IllegalArgumentException(
					"Not support as to Collection/Map/Array type. type="
							+ type.getName());
		} else {
			BeanInfo bi = Introspector.getBeanInfo(type);
			PropertyDescriptor[] props = bi.getPropertyDescriptors();

			Object field = null;
			for (PropertyDescriptor p : props) {
				if (this.containsField(p.getName())) {
					Method writeMethod = p.getWriteMethod();

					if (writeMethod == null) {
						throw new IllegalArgumentException("The property:"
								+ type.getName() + "." + p.getName()
								+ " have not set method.");
					}

					
					field = this.get(p.getName());	
					

					
					if (field == null) {
						continue;
					}else if(p.getPropertyType().equals(java.util.Map.class)){  //TODO
							Field mapField=type.getDeclaredField(p.getName());
							Type generictype=mapField.getGenericType();
							Type valueType=null;
							if(generictype instanceof ParameterizedType){
								Type[] types=((ParameterizedType)generictype).getActualTypeArguments();
								valueType=types[1];
							}
							Map map=((BSONObject) field).toMap();							
							Map realMap=new HashMap();
							Set<Map.Entry<?,?>> set=map.entrySet();
							Iterator<Map.Entry<?,?>> iterator=set.iterator();
							while(iterator.hasNext()){
								Map.Entry<?,?> entry=iterator.next();
								String  key =entry.getKey().toString();
								if(((Class)valueType).isPrimitive()||((Class)valueType).equals(java.lang.String.class)){
									realMap.put(key,((BSONObject) field).get(key));
								}else{
									realMap.put(key,((BSONObject)((BSONObject) field).get(key)).as((Class)valueType));
								}
							}
							writeMethod.invoke(result,realMap);
					}else if (field instanceof BasicBSONObject) { // bson <=>
						writeMethod.invoke(result,
								((BSONObject) field).as(p.getPropertyType()));
					} else if (field instanceof BasicBSONList) { // bsonlist <=>

						Field f = type.getDeclaredField(p.getName());
						if (f == null)
                                                     continue;
						Type _type = f.getGenericType();

						Type fileType = null;
						if (_type != null && _type instanceof ParameterizedType) {
							fileType = ((ParameterizedType) _type)
									.getActualTypeArguments()[0];
						} else {
							throw new IllegalArgumentException(
									"Current version only support parameterized type Collection(List/Set/Queue) field. unknow type="
											+ eleType.toString());
						}

						writeMethod.invoke(result, ((BSONObject) field).as(
								p.getPropertyType(), fileType));
					} else if (BasicTypeWrite(result, field, writeMethod)) {
						continue;
					} else {
						continue;
					}
				}
			}
		}
		return result;
	}
	

	
	@SuppressWarnings({"rawtypes"})
	public static BSONObject typeToBson(Object object)
			throws IntrospectionException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {

		BSONObject result = null;
		if (object == null) {
			result = null;
		} else if (BSON.IsBasicType(object)) {
			throw new IllegalArgumentException(
					"Current version is not support basice type to bson in the top level.");
		} else if (object instanceof List) {
			BSONObject listObj = new BasicBSONList();
			List list = (List) object;
			int index = 0;
			for (Object obj : list) {
				if (BSON.IsBasicType(obj)) {
					listObj.put(Integer.toString(index), obj);
				} else {
					listObj.put(Integer.toString(index), typeToBson(obj));
				}
				++index;
			}
			result = listObj;
		} else if (object instanceof Map) {
			BSONObject mapObj=new BasicBSONObject();
			Map map=(Map)object;
			Set<Map.Entry<?,?>> set=map.entrySet();
			Iterator<Map.Entry<?,?>> iterator=set.iterator();
			while(iterator.hasNext()){
				Map.Entry<?,?> entry=iterator.next();
				String  key =entry.getKey().toString();
				Object value=entry.getValue();
				if(BSON.IsBasicType(value)){
					mapObj.put(key, value);
				}else{
					mapObj.put(key, typeToBson(value));
				}				
			}
			result = mapObj;
		}else if(object.getClass().isArray()){
			throw new IllegalArgumentException(
					"Current version is not support Map/Array type field.");
		}else if (object instanceof BSONObject) {
			result = (BSONObject) object;
		} else if (object.getClass().getName() == "java.lang.Class") {
			throw new IllegalArgumentException(
					"Current version is not support java.lang.Class type field.");
		} else { // User define type.
			result = new BasicBSONObject();
			Class<?> cl = object.getClass();

			BeanInfo bi = Introspector.getBeanInfo(cl);
			PropertyDescriptor[] props = bi.getPropertyDescriptors();
			for (PropertyDescriptor p : props) {
				Class<?> type = p.getPropertyType();
				Object propObj = p.getReadMethod().invoke(object);
				if (BSON.IsBasicType(propObj)) {
					result.put(p.getName(), propObj);
				} else if (type.getName() == "java.lang.Class") {
					continue;
				} else {
					result.put(p.getName(), typeToBson(propObj));
				}
			}
		}

		return result;
	}

    @Override
    public Set<String> keySet() {
        return _objectMap.keySet();
    }
    
    public Set<Entry<String, Object>> entrySet() {
        return _objectMap.entrySet();
    }

}
