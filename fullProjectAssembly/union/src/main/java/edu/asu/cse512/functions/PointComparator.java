package edu.asu.cse512.functions;

import java.io.Serializable;
import java.util.Comparator;

import edu.asu.cse512.beans.Point;

public class PointComparator implements Comparator<Point>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public int compare(Point o1, Point o2) {
		if (o1.lt(o2))
			return -1;
		// no need of checking for equals as the points are distinct
		return 1;
	}

}
