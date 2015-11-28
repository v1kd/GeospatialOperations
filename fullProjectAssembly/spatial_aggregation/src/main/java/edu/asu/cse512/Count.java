package edu.asu.cse512;

import java.io.Serializable;

public class Count implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Count() {
		
	}
	
	public Count(int id, int count) {
		this.id = id;
		this.count = count;
	}
	
	private int id;
	
	private int count;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.id + "," + this.count;
	}
}
