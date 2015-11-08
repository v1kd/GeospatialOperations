package edu.asu.cse512.beans;

import java.io.Serializable;


public class Point implements Serializable {
	
	private static final long serialVersionUID = 1L;

	public Point() {		
	}

	public Point(double x, double y) {
		this.x = x;
		this.y = y;
	}
	
	private double x;
	
	private double y;

	@Override
	public String toString() {
		return this.x + "," + this.y;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(x);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(y);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Point other = (Point) obj;
		if (Double.doubleToLongBits(x) != Double.doubleToLongBits(other.x))
			return false;
		if (Double.doubleToLongBits(y) != Double.doubleToLongBits(other.y))
			return false;
		return true;
	}
	
	public boolean lt(Object obj) {

		Point other = (Point) obj;
		if (x < other.x)
			return true;
		else if (x == other.x && y < other.y)
			return true;
		
		return false;
	}
	
	public boolean gt(Object obj) {

		Point other = (Point) obj;
		if (x > other.x)
			return true;
		else if (x == other.x && y > other.y)
			return true;
		
		return false;
	}
	
}
