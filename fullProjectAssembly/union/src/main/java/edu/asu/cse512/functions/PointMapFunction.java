package edu.asu.cse512.functions;

import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;

import edu.asu.cse512.beans.Point;

public class PointMapFunction implements Function<Coordinate, Point> {

	private static final long serialVersionUID = 1L;

	public Point call(Coordinate c) throws Exception {
		return new Point(c.x, c.y);
	}

}
