package edu.asu.cse512.functions;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateComparator implements Comparator<Coordinate>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public int compare(Coordinate o1, Coordinate o2) {
		if (o1.x < o2.x)
			return -1;
		else if(o1.x==o2.x && o1.y<o2.y)
			return -1;
		// no need of checking for equals as the points are distinct
		return 1;
	}

}
