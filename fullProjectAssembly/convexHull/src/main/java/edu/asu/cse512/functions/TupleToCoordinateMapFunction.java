package edu.asu.cse512.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import com.vividsolutions.jts.geom.Coordinate;

public class TupleToCoordinateMapFunction implements Function<Tuple2<Coordinate,Boolean>, Coordinate> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Coordinate call(Tuple2<Coordinate, Boolean> t) throws Exception {
		return t._1;
	}
}


