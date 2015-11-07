package edu.asu.cse512;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Array;
import scala.Tuple2;

/**
 * Hello world!
 *
 */

class Point implements Serializable
{
	public double x;
	public double y;
	
	Point()
	{
		
	}
	Point(double x,double y)
	{
		this.x = x;
		this.y = y;
	}
	@Override
	public String toString() {
		return "Point [x=" + x + ", y=" + y + "]";
	}
	
	
}

class Pair implements Serializable
{
	public Point A;
	public Point B;
	double distanceLength;
	
	Pair()
	{
		
	}
	Pair(Point A, Point B)
	{
		this.A = A;
		this.B = B;
		distanceLength = findDistance(A,B);
	}
	

	@Override
	public String toString() {
		return "Pair [A=" + A + ", B=" + B + ", distanceLength=" + distanceLength + "]";
	}
	public double findDistance(Point A1,Point A2)
	{
		double dist ;
		dist = Math.sqrt( Math.pow((A1.x - A2.x),2) + Math.pow((A1.y - A2.y),2) );
		return dist;
	}
}

public class ClosestPair 
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
    public static void main( String[] args ) throws FileNotFoundException, UnsupportedEncodingException
    {
        //Initialize, need to remove existing in output file location.
    	int partitions = 4;
    	String appName = "Closest Pair";
    	String master="local";  //"spark://192.168.0.73:7077";

    	SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> lines =sc.textFile("/home/test1/CSE512Project_Related/x", partitions);
        
        JavaRDD<Point> points = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Point>() {
        	
        	/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<Point> call(Iterator<String> line)
        	{
        		String sep =",";
        		String eachline;
        		String[] xyvals;
        		Point eachPoint;
        		List<Point> pointsFromInputFile = new ArrayList<Point>();
        		while (line.hasNext())
        		{
        			eachline = line.next();
        			xyvals = eachline.split(sep);
        			eachPoint = new Point(Double.parseDouble(xyvals[0]), Double.parseDouble(xyvals[1]));
        			pointsFromInputFile.add(eachPoint);      			
        		}
				return pointsFromInputFile;
        	}
		});

        //points.repartition(1);
        System.out.println(points.collect().size());
        
//        JavaRDD<Point> pointSplit1 = points.filter(new Function<Point,Boolean>(){
//        	public Boolean call(Point p)
//        	{
//        		return null;
//        	}
//        });
//        
//        JavaRDD<Point> pointSplit2 = points.filter(new Function<Point,Boolean>(){
//        	public Boolean call(Point p)
//        	{
//        		return null;
//        	}
//        });
        
        
        JavaPairRDD<Point, Point> allPointTuples = points.cartesian(points);
        
        System.out.println("Total cart: " + allPointTuples.collect().size());
        
        JavaPairRDD<Point, Point> distinctPointTuples = allPointTuples.filter(new Function<Tuple2<Point,Point>,Boolean>(){
        	/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Point,Point> tuple) throws Exception {
        		return !((tuple._1.x == tuple._2.x) && (tuple._1.y == tuple._2.y));
        	}
        });
        
        System.out.println("Dist cart: " + distinctPointTuples.collect().size());
        
        JavaRDD<Pair> pairsRDD = distinctPointTuples.map(new Function<Tuple2<Point,Point>, Pair>() {

			public Pair call(Tuple2<Point, Point> tuple) throws Exception {
				// TODO Auto-generated method stub
				Point pointA = tuple._1();
				Point pointB = tuple._2();
				Pair a = new Pair(pointA, pointB);
				return a;
			}
		});
       
        
        JavaRDD<Pair> pairs = distinctPointTuples.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Point,Point>>, Pair>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<Pair> call(Iterator<Tuple2<Point, Point>> tuples) throws Exception {
				// TODO Auto-generated method stub
				List<Pair> pairsFromTuples = new ArrayList<Pair>();
//				Pair singlePair = new Pair();
				Tuple2<Point,Point> tuple;
				while (tuples.hasNext())
				{
					tuple = tuples.next();
					
//					singlePair.A = tuples.next()._1;
//					singlePair.B = tuples.next()._2;
					Pair singlePair = new Pair(tuple._1(),tuple._2());
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

        Pair minDistPair = pairs.reduce(new Function2<Pair,Pair,Pair>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Pair call(Pair a, Pair b) throws Exception {
				// TODO Auto-generated method stub
				
				return (a.distanceLength<b.distanceLength?a:b);
			}
        	
        });
        
        System.out.println(minDistPair);
        		
    	//Implement 
    	
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    }
}
