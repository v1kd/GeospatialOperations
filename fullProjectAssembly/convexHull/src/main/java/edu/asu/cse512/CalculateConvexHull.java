package edu.asu.cse512;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;


import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import geospatial1.operation1.functions.*;
//This calcualtes the convex hulls locally
class hull implements FlatMapFunction<Iterator<Coordinate>, Coordinate>, Serializable
{
	//Iterate over the coordinates to calcualte the convex hull
	public Iterable<Coordinate> call(Iterator<Coordinate> crd)
	{
		List<Coordinate> Coordinates = new ArrayList<Coordinate>();
		GeometryFactory gf = new GeometryFactory();
		try{
			while(crd.hasNext())
			{
				//Read the coordinate
				Coordinate temp = crd.next();
				if(temp!=null){
					Coordinates.add(temp);
				}
			}}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		// Create new ConvexHull object with list of coordinates and Geometryfactory object as input
		ConvexHull ch = new ConvexHull(Coordinates.toArray(new Coordinate[Coordinates.size()]), gf);
		
		// Perform Convex Hull operation
		Geometry g=ch.getConvexHull();
		
		//Convert Geometry to coordinates
		Coordinate[] c= g.getCoordinates();
		
		//Convert the coordinates array to arraylist
		List<Coordinate> a = Arrays.asList(c);
		return a;
	}
}


//Algorithm to explain convex_hull operation
public class CalculateConvexHull 
{    
	public static Function<String, Coordinate> parseData = new Function<String, Coordinate>() {
	
		public Coordinate call(String s) throws Exception {
			String[] parts = s.split(",");
			Coordinate coordinate = null; 
			if(parts.length == 2) {
			   coordinate = new Coordinate(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
			}
			return coordinate;
		}
	};
	
	public static Function<Coordinate, String> parseOutputData = new Function<Coordinate, String>() {
		
		public String call(Coordinate c) throws Exception {
			String answer  = "";
			Coordinate coordinate = null; 
			if(c!=null) {
			   answer = String.valueOf(c.x)+","+String.valueOf(c.y);
			}
			return answer;
		}
	};
	
	private static List<Coordinate> sortList(List<Coordinate> cr){
		Collections.sort(cr,new Comparator<Coordinate>() {

	    	public int compare(Coordinate o1, Coordinate o2) {
	    		if(o1==null || o2==null){
	    			return 0;
	    		}
	    		else if(o1.x!=o2.x){
	    			return Double.compare(o1.x, o2.x);
	    		}else{
	    			return Double.compare(o1.y, o2.y);
	    		}
	    	}
    	});

		return cr;
	}
	
    public static void main( String[] args ) throws FileNotFoundException
    {
    	if (args.length <= 0) {
    		System.out.println("We require input file path, output file path and number of partitions argument to proceed further.");
    		System.out.println("Usage: java ConvexHull <input file path> <output file path> <noOfPartitions>");
    		System.exit(0);
		}

    	String inputFile = args[0];
    	//setting up default partition if not given in argument
    	int noOfPartitions = 3;
    	if(args.length == 3) {
    		noOfPartitions = Integer.parseInt(args[2]);
    	}
    	
    	//delete if output file already exist
		try{
    		File file = new File(args[1]);
    		if(file.delete()){
    			System.out.println(file.getName() + " is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    	
    	SparkConf conf = new SparkConf().setAppName("convexHull").setMaster("local");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Read file as RDD
    	JavaRDD<String> inputData = sc.textFile(inputFile, noOfPartitions);
//    	JavaRDD<Coordinate> coordinates = inputData.mapPartitions(parseData);
    	
    	// Map each String in the file as a coordinate object
    	JavaRDD<Coordinate> coordinates = inputData.map(parseData);//.repartition(noOfPartitions);
    	
    	// Map to a tuple to sort the Points
    	JavaPairRDD<Coordinate, Boolean> pointTupleRDD = coordinates.mapToPair(new CoordinatePairFunction());

		// Sort the points
		JavaPairRDD<Coordinate, Boolean> sortedPointTupleRDD = pointTupleRDD.sortByKey(new CoordinateComparator());

		// Map to points RDD
		JavaRDD<Coordinate> finalSortedPointRDD = sortedPointTupleRDD.map(new TupleToCoordinateMapFunction());

    	// Convert sorted collection to RDD
		
    	
    	// Perform Convex hull operation on individual partition
    	JavaRDD<Coordinate> localHull = finalSortedPointRDD.mapPartitions(new hull());
    	
    	// Repartition to 1 partition in order to apply 'convex hull' on all the Coordinate objects obtained from individual partitions
    	JavaRDD<Coordinate> calculatedHull = localHull.coalesce(1).cache();
    	
    	// Perform Convex hull operation
    	JavaRDD<Coordinate> globalHull = calculatedHull.mapPartitions(new hull()).distinct();
    	
    	
		// Map to a tuple to sort the Points
		JavaPairRDD<Coordinate, Boolean> coordinateTupleRDD = globalHull.mapToPair(new CoordinatePairFunction());

		// Sort the points
		JavaPairRDD<Coordinate, Boolean> sortedCoordinateTupleRDD = coordinateTupleRDD.sortByKey(new CoordinateComparator());

		// Map to points RDD
		JavaRDD<Coordinate> finalSortedCoordinateRDD = sortedCoordinateTupleRDD.map(new TupleToCoordinateMapFunction());

		JavaRDD<String> outputData = finalSortedCoordinateRDD.map(parseOutputData);
		// Write to a file
		outputData.saveAsTextFile(args[1]);
    	
        sc.close();
    }

}
