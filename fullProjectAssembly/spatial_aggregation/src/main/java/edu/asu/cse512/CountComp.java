package edu.asu.cse512;

import java.io.Serializable;
import java.util.Comparator;

public class CountComp implements Comparator<Count>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public int compare(Count o1, Count o2) {
		if (o1.getId() > o2.getId())
			return 1;
		
		return -1;
	}

}
