package edu.asu.cse512;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Array;

/**
 * Hello world!
 *
 */

class Point
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
}

class Pair
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
    public static void main( String[] args )
    {
        //Initialize, need to remove existing in output file location.
    	int partitions = 2;
    	String appName = "Closest Pair";
    	String master="spark://192.168.0.73:7077";  //"spark://192.168.0.73:7077";

    	SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> lines =sc.textFile("hdfs://master:54310/abc.txt", partitions);
        
        JavaRDD<Point> points = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Point>() {
        	
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
        System.out.println(points.collect());
        //JavaRDD<Pair> pairs = points.cartesian(other)
        
    	//Implement 
    	
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    }
}
