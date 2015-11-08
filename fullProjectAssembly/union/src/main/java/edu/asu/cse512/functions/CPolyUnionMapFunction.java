package edu.asu.cse512.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

/**
 * @author hdworker
 *
 */
public class CPolyUnionMapFunction implements FlatMapFunction<Iterator<Geometry>, Geometry> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<Geometry> polygons) throws Exception {
		
		ArrayList<Geometry> gList = new ArrayList<Geometry>();

		while (polygons.hasNext())
			gList.add(polygons.next());
		
		if (gList.isEmpty())
			return gList;
	

		CascadedPolygonUnion u = new CascadedPolygonUnion(gList);
		Geometry unionG = u.union();

		return Arrays.asList(new Geometry[] { unionG });
	}

}
