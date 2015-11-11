package edu.asu.cse512;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.asu.cse512.functions.CoordinateComparator;
import edu.asu.cse512.functions.CoordinatePairFunction;
import edu.asu.cse512.functions.TupleToCoordinateMapFunction;

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


class Pair implements Serializable {
	public Coordinate A;
	public Coordinate B;
	double distanceLength;

	Pair() {

	}

	Pair(Coordinate A, Coordinate B) {
		this.A = A;
		this.B = B;
		distanceLength = findDistance(A, B);
	}

	@Override
	public String toString() {
		return "Pair [A=" + A + ", B=" + B + ", distanceLength=" + distanceLength + "]";
	}

	public double findDistance(Coordinate A1, Coordinate A2) {
		double dist;
		dist = Math.sqrt(Math.pow((A1.x - A2.x), 2) + Math.pow((A1.y - A2.y), 2));
		return dist;
	}
}



//Algorithm to explain convex_hull operation

public class FarthestPair {
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
    		System.out.println("Usage: java FarthestPair <input file path> <output file path> <noOfPartitions>");
    		System.exit(0);
		}

    	String inputFile = args[0];
		
    	SparkConf conf = new SparkConf().setAppName("Group6-FarthestPair");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	// Read file as RDD
    	JavaRDD<String> inputData = sc.textFile(inputFile);
    	//JavaRDD<Coordinate> coordinates = inputData.mapPartitions(parseData);
    	
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
    	
    	JavaPairRDD<Coordinate, Coordinate> allCoordinateTuples = globalHull.cartesian(globalHull);
		System.out.println("Total cart: " + allCoordinateTuples.collect().size());
		
		
		JavaRDD<Pair> pairsRDD = allCoordinateTuples.map(new Function<Tuple2<Coordinate, Coordinate>, Pair>() {

			public Pair call(Tuple2<Coordinate, Coordinate> tuple) throws Exception {
				// TODO Auto-generated method stub
				Coordinate pointA = tuple._1();
				Coordinate pointB = tuple._2();
				Pair a = new Pair(pointA, pointB);
				return a;
			}
		});

		JavaRDD<Pair> pairs = allCoordinateTuples.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Coordinate, Coordinate>>, Pair>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Iterable<Pair> call(Iterator<Tuple2<Coordinate, Coordinate>> tuples) throws Exception {
						// TODO Auto-generated method stub
						List<Pair> pairsFromTuples = new ArrayList<Pair>();
						// Pair singlePair = new Pair();
						Tuple2<Coordinate, Coordinate> tuple;
						while (tuples.hasNext()) {
							tuple = tuples.next();

							// singlePair.A = tuples.next()._1;
							// singlePair.B = tuples.next()._2;
							Pair singlePair = new Pair(tuple._1(), tuple._2());
							pairsFromTuples.add(singlePair);
						}
						return pairsFromTuples;
					}
				});

		JavaRDD<Integer> x = pairsRDD.mapPartitions(new FlatMapFunction<Iterator<Pair>, Integer>() {

			public Iterable<Integer> call(Iterator<Pair> arg0) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Integer> x = new ArrayList<Integer>();
				x.add(1);
				return x;
			}
		});

		System.out.println("Num of partitions: " + x.collect());

		JavaRDD<Integer> y = pairs.mapPartitions(new FlatMapFunction<Iterator<Pair>, Integer>() {

			public Iterable<Integer> call(Iterator<Pair> arg0) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Integer> x = new ArrayList<Integer>();
				x.add(1);
				return x;
			}
		});

		System.out.println("Num of partitions charan: " + y.collect());

		Pair minDistPair = pairs.reduce(new Function2<Pair, Pair, Pair>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Pair call(Pair a, Pair b) throws Exception {
				// TODO Auto-generated method stub

				return (a.distanceLength > b.distanceLength ? a : b);
			}

		});

		// System.out.println(minDistPair);

		Coordinate closestpointA = minDistPair.A;
		Coordinate closestpointB = minDistPair.B;

		List<Coordinate> closestPoints = new ArrayList<Coordinate>();
		closestPoints.add(closestpointA);
		closestPoints.add(closestpointB);

		JavaRDD<Coordinate> closestRDD = sc.parallelize(closestPoints);
		
		
		// Map to a tuple to sort the Points
		JavaPairRDD<Coordinate, Boolean> coordinateTupleRDD = closestRDD.mapToPair(new CoordinatePairFunction());

		// Sort the points
		JavaPairRDD<Coordinate, Boolean> sortedCoordinateTupleRDD = coordinateTupleRDD.sortByKey(new CoordinateComparator());

		// Map to points RDD
		JavaRDD<Coordinate> finalSortedCoordinateRDD = sortedCoordinateTupleRDD.map(new TupleToCoordinateMapFunction());


		JavaRDD<String> outputData = finalSortedCoordinateRDD.map(parseOutputData);
		//closestRDD.saveAsTextFile(outputfilepath);
		outputData.saveAsTextFile(args[1]);
		

		// Output your result, you need to sort your result!!!
		// And,Don't add a additional clean up step delete the new generated
		// file...
		sc.close();
    }
}

