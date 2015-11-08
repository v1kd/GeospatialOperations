package edu.asu.cse512.functions;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import edu.asu.cse512.beans.Point;

public class PointPairFunction implements PairFunction<Point, Point, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Tuple2<Point, Boolean> call(Point point) throws Exception {
		return new Tuple2<Point, Boolean>(point, Boolean.TRUE);
	}

}
