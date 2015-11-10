package edu.asu.cse512;

import java.io.Serializable;

public class Rectangle implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private int id;
	private double x1;
	private double x2;
	private double y1;
	private double y2;
	
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
	
	public double getX2() {
		return x2;
	}
	
	public void setX2(double x2) {
		this.x2 = x2;
	}
	
	public double getY1() {
		return y1;
	}
	
	public void setY1(double y1) {
		this.y1 = y1;
	}
	
	public double getY2() {
		return y2;
	}
	
	public void setY2(double y2) {
		this.y2 = y2;
	}
	
	public Rectangle(int id, double x1, double x2, double y1, double y2) {
		this.id = id;
		this.x1 = Math.min(x1, x2);
		this.x2 = Math.max(x1, x2);
		this.y1 = Math.min(y1, y2);
		this.y2 = Math.max(y1, y2);
	}
	
	public Boolean has(Point point) {		
		return point.getX1() >= x1 && point.getX1() <= x2
				&& point.getY1() >= y1 && point.getY1() <= y2;
	}
	
	public Boolean has(Rectangle rectangle) {
		double rectangleX1 = rectangle.getX1();
		double rectangleY1 = rectangle.getY1();
		double rectangleX2 = rectangle.getX2();
		double rectangleY2 = rectangle.getY2();
		
//		System.out.println("x1 = " + x1 + " rec.x1 = " + rectangle.getX1());
//		System.out.println("x2 = " + x2 + " rec.x2 = " + rectangle.getX2());
//		System.out.println("y1 = " + y1 + " rec.y1 = " + rectangle.getY1());
//		System.out.println("y2 = " + y2 + " rec.y2 = " + rectangle.getY1());
		
		return ((((x1 >= rectangleX1 && x1 <= rectangleX2) 
						|| (x2 >= rectangleX1 && x2 <= rectangleX2))
				&& ((y1 >= rectangleY1 && y1 <= rectangleY2)
						|| (y2 >= rectangleY1 && y2 <= rectangleY2)))
				|| (((rectangleX1 >= x1 && rectangleX1 <= x2)
						|| (rectangleX2 >= x1 && rectangleX2 <= x2))
				&&((rectangleY1 >= y1 && rectangleY1 <= y2)
						|| (rectangleY2 >= y1 && rectangleY2 <= y2))));
						
//		return ((x1 >= rectangle.getX1() && x1 <= rectangle.getX2()
//				&& y1 >= rectangle.getY1() && y1 <= rectangle.getY2())
//				|| (x2 >= rectangle.getX1() && x2 <= rectangle.getX2()
//				    && y2 >= rectangle.getY1() && y2 <= rectangle.getY2())
//				|| (x1 >= rectangle.getX1() && x1 <= rectangle.getX2()
//				   && y2 >= rectangle.getY1() && y2 <= rectangle.getY2())
//				|| (x2 >= rectangle.getX1() && x2 <= rectangle.getX2()
//				    && y1 >= rectangle.getY1() && y1 <= rectangle.getY2())
//				|| (rectangle.getX1() >= x1 && rectangle.getX1() <= x2
//						&& rectangle.getY1() >= y1 && rectangle.getY1() <= y2)
//				|| (rectangle.getX2() >= x1 && rectangle.getX2() <= x2
//						&& rectangle.getY2() >= y1 && rectangle.getY2() <= y2)
//				|| (rectangle.getX1() >= x1 && rectangle.getX1() <= x2
//						&& rectangle.getY2() >= y1 && rectangle.getY2() <= y2)
//				|| (rectangle.getX2() >= x1 && rectangle.getX2() <= x2
//						&& rectangle.getY1() >= y1 && rectangle.getY1() <= y2));
	}
}