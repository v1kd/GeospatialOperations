package geospatial1.operation1;

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

public class fpair {
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
	
	private static List<Coordinate> sortList(List<Coordinate> cr){
		Collections.sort(cr,new Comparator<Coordinate>() {

	    	public int compare(Coordinate o1, Coordinate o2) {
	    		if(o1==null || o2==null){
	    			return 0;
	    		}
	    		else if(o1.y!=o2.y){
	    			return Double.compare(o1.y, o2.y);
	    		}else{
	    			return Double.compare(o1.x, o2.x);
	    		}
	    	}
    	});

		return cr;
	}
	
	public static void main( String[] args ) throws FileNotFoundException
    {
    	
		if (args.length <= 0) {
    		System.out.println("We require input file path, output file path and number of partitions argument to proceed further.");
    		System.out.println("Usage: java fpair <input file path> <output file path> <noOfPartitions>");
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

		
    	SparkConf conf = new SparkConf().setAppName("FPair").setMaster("local");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Read file as RDD
    	JavaRDD<String> inputData = sc.textFile(inputFile, noOfPartitions);
    	//JavaRDD<Coordinate> coordinates = inputData.mapPartitions(parseData);
    	
    	// Map each String in the file as a coordinate object
    	JavaRDD<Coordinate> coordinates = inputData.map(parseData);//.repartition(noOfPartitions);
    	
    	// Sort coordinates before giving it to calculate Convex Hull, this is required to get optimized result.
    	List<Coordinate> cr = coordinates.collect();
    	
    	// sort coordinates before mapping
    	cr = sortList(cr);
    	// Convert sorted collection to RDD
    	JavaRDD<Coordinate> sortedRDD = sc.parallelize(cr);

    	// Perform Convex hull operation on individual partition
    	JavaRDD<Coordinate> localHull = sortedRDD.mapPartitions(new hull());
    	
    	// Repartition to 1 partition in order to apply 'convex hull' on all the Coordinate objects obtained from individual partitions
    	JavaRDD<Coordinate> calculatedHull = localHull.coalesce(1).cache();
    	
    	// Perform Convex hull operation
    	JavaRDD<Coordinate> globalHull = calculatedHull.mapPartitions(new hull());
    	List<Coordinate> convexHullList = null;
		try {
			// Collect list of coordinates
			convexHullList = globalHull.collect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		double maxDist = 0;
		int size = convexHullList.size();
		int a = 0,b = 1;
		for(int i = 0; i < size - 1; i++){
			for(int j = i+1; j < size; j++){
				double distance = Math.pow(Math.pow((convexHullList.get(i).x - convexHullList.get(j).x),2) + Math.pow((convexHullList.get(i).y - convexHullList.get(j).y),2),0.5);
				if(distance > maxDist){
					maxDist = distance;
					a = i; b = j;
				}
			}	
		}
		try{
			PrintWriter output = new PrintWriter(args[1]); //give this in input
			output.println(convexHullList.get(a).x + ", " + convexHullList.get(a).y);
			output.println(convexHullList.get(b).x + ", " + convexHullList.get(b).y);
			output.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}
    }
}

