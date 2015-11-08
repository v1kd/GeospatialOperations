package edu.asu.cse512.functions;

import org.apache.spark.api.java.function.Function2;

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateReduceFunction implements
		Function2<Coordinate, Coordinate, Coordinate> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private CoordinateType type;
	
	public CoordinateReduceFunction(CoordinateType type) {
		this.type = type;
	}

	public Coordinate call(Coordinate one, Coordinate two) throws Exception {

		Coordinate c = one;
		switch (this.type) {
		case MIN_X: // minimum x
			c = one.x < two.x ? one : two;
			break;
			
		case MAX_X: // maximum x
			c = one.x > two.x ? one : two;
			break;
			
		case MIN_Y: // minimum y
			c = one.y < two.y ? one : two;
			break;
			
		case MAX_Y:// maximum y
			c = one.y > two.y ? one : two;
			break;
			
		default:
			break;
		}
		
		return c;
	}

}
