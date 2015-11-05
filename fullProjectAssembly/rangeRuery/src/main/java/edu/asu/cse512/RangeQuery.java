package edu.asu.cse512;

import org.apache.spark.api.java.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class RangeQuery 
{
	private static Function<String, Point> extractPoints = new Function<String, Point>() {

		private static final long serialVersionUID = 1L;

		public Point call(String arg0) throws Exception {
			List<String> list = Arrays.asList(arg0.split(","));
			int id = Integer.parseInt(list.get(0));
			double x1 = Double.parseDouble(list.get(1));
			double y1 = Double.parseDouble(list.get(2));
			
			Point point = new Point(id, x1, y1);
			return point;
		}		
	};
	
	private static Function<String, Rectangle> extractQueryWindow = new Function<String, Rectangle>() {

		private static final long serialVersionUID = 1L;

		public Rectangle call(String arg0) throws Exception {
			List<String> list = Arrays.asList(arg0.split(","));
			double x1 = Double.parseDouble(list.get(0));
			double y1 = Double.parseDouble(list.get(1));
			double x2 = Double.parseDouble(list.get(2));
			double y2 = Double.parseDouble(list.get(3));
			
			Rectangle rectangle = new Rectangle(x1, x2, y1, y2);
			return rectangle;
		}
	};

    public static void main(String[] args)
    {
    	String input1 = "RangeQueryTestData.csv";
    	String input2 = "input2.csv";
    	String output = "output";
    	
    	rangeQuery(input1, input2, output);
    }

	public static void rangeQuery(String input1, String input2, String output) {
    	SparkConf conf = new SparkConf().setAppName("RangeQuery").setMaster("local");
    	JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		JavaRDD<String> file1 = sparkContext.textFile(input1);
		JavaRDD<String> file2 = sparkContext.textFile(input2);
		
		JavaRDD<Point> points = file1.map(extractPoints);
		JavaRDD<Rectangle> queryWindow = file2.map(extractQueryWindow);
		
		final Broadcast<Rectangle> broadcastQueryWindow = sparkContext.broadcast(queryWindow.first());
		
		JavaRDD<Integer> validPoints = points
				.filter(new Function<Point, Boolean>() {
					
						private static final long serialVersionUID = 1L;

						public Boolean call(Point point) throws Exception {
							return broadcastQueryWindow.value().Has(point);
						}
				})
				.map(new Function<Point, Integer>() {
					
					private static final long serialVersionUID = 1L;

					public Integer call(Point arg0) throws Exception {
						return arg0.getId();
					}
				})
				.sortBy(new Function<Integer, Integer>(){
					
					private static final long serialVersionUID = 1L;

					public Integer call(Integer arg0) throws Exception {
						return arg0;
					}
			
				}, true, 1);
	
		validPoints.coalesce(1).saveAsTextFile(output);
		sparkContext.close();
	}
}
