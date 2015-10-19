package com.sequoiadb.testdata;

import java.util.HashMap;
import java.util.Map;

public class HaveMapPropClass {
	private Map<String, String> mapProp = null;
	private Map<String,User> userMap=null;
	
	public HaveMapPropClass() {
		mapProp = new HashMap<String, String>();
	}

	public Map<String, String> getMapProp() {
		return mapProp;
	}

	public void setMapProp(Map<String, String> value) {
		this.mapProp = value;
	}
	
	public Map<String,User> getUserMap(){
		return this.userMap;
	}
	
	public void setUserMap(Map<String,User> userMap){
		this.userMap=userMap;
	}

	@Override
	public String toString() {
		return "HaveMapPropClass [mapProp=" + mapProp + ", userMap=" + userMap
				+ "]";
	}
	
}
