package edu.asu.cse512.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import edu.asu.cse512.beans.Point;

public class TupleToPointMapFunction implements Function<Tuple2<Point,Boolean>, Point> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Point call(Tuple2<Point, Boolean> t) throws Exception {
		return t._1;
	}
}


