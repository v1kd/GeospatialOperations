package edu.asu.cse512;

import java.io.Serializable;

public class Point implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private int id;
	private double x1;
	private double y1;
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public double getX1() {
		return x1;
	}
	
	public void setX1(double x1) {
		this.x1 = x1;
	}
	
	public double getY1() {
		return y1;
	}
	
	public void setY1(double y1) {
		this.y1 = y1;
	}
	
	public Point(int id, double x1, double y1) {
		this.id = id;
		this.x1 = x1;
		this.y1 = y1;
	}
	
	public Boolean isIn(Rectangle rectangle) {		
		return (x1 >= rectangle.getX1() && x1 <= rectangle.getX2()
				&& y1 >= rectangle.getY1() && y1 <= rectangle.getY2());
	}
}
