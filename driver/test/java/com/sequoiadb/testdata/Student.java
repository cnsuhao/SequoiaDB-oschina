package com.sequoiadb.testdata;

public class Student {
	public int age;
	public String name;
	
	public void setAge(int age){
		this.age=age;
	}
	
	public int   getAge(){
		return this.age;
	}
	
	public void  setName(String name){
		this.name=name;
	}
	
	public String getName(){
		return this.name;
	}

	public Student(int age, String name) {
		super();
		this.age = age;
		this.name = name;
	}

	public Student() {

	}

	@Override
	public String toString() {
		return "Student [age=" + age + ", name=" + name + "]";
	}	
	
}
