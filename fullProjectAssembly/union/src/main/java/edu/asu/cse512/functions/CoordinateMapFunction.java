package edu.asu.cse512.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * @author hdworker
 *
 */
public class CoordinateMapFunction implements FlatMapFunction<Iterator<Geometry>, Coordinate> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Iterable<Coordinate> call(Iterator<Geometry> polygons) throws Exception {
		
		ArrayList<Coordinate> coordinatList = new ArrayList<Coordinate>();
		Geometry g;
		Coordinate[] coordinates = new Coordinate[] {};
		
		while (polygons.hasNext()) {
			g = polygons.next();
			coordinates = g.getCoordinates();
			coordinatList.addAll(Arrays.asList(coordinates));
		}

		return coordinatList;
	}

}
