package edu.asu.cse512.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;


/**
 * @author hdworker
 *
 */
public class GeometryFileMapFunction implements FlatMapFunction<Iterator<String>, Geometry> {
	
	private GeometryFactory geometryFactory = new GeometryFactory();

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<String> lines) throws Exception {
		
		List<Geometry> polygons = new ArrayList<Geometry>();
		
		String line;
		String[] strOrdinates;
		Polygon p;
		
		double x1;
		double y1;
		
		double x2;
		double y2;
		
		while (lines.hasNext()) {
			
			line = lines.next();
			
			line = line != null ? line.trim() : null;
			
			if (null == line || line.length() == 0)
				continue;
			
			
			strOrdinates = line.split(",");
			
			if (null == strOrdinates || strOrdinates.length != 4)
				continue;
			
			try {
				x1 = Double.parseDouble(strOrdinates[0]);
				y1 = Double.parseDouble(strOrdinates[1]);
				
				x2 = Double.parseDouble(strOrdinates[2]);
				y2 = Double.parseDouble(strOrdinates[3]);
			} catch(Exception e) {
				// invalid nums
				continue;
			}
			
			// create a new polygon
			p = geometryFactory.createPolygon(new Coordinate[] { new Coordinate(x1, y1),
					new Coordinate(x1, y2), new Coordinate(x2, y2),
					new Coordinate(x2, y1), new Coordinate(x1, y1) });
			
			polygons.add(p);
		}
		
		return polygons;
	}

}
