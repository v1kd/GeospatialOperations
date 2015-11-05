package edu.asu.cse512;

import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Join 
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
	
	private static Function<String, Rectangle> extractRectangles = new Function<String, Rectangle>() {

		private static final long serialVersionUID = 1L;

		public Rectangle call(String arg0) throws Exception {
			List<String> list = Arrays.asList(arg0.split(","));
			int id = Integer.parseInt(list.get(0));
			double x1 = Double.parseDouble(list.get(1));
			double y1 = Double.parseDouble(list.get(2));
			double x2 = Double.parseDouble(list.get(3));
			double y2 = Double.parseDouble(list.get(4));
			
			Rectangle rectangle = new Rectangle(id, x1, x2, y1, y2);
			return rectangle;
		}
	};

    public static void main( String[] args ) {
    	String input1 = "JoinQueryTestData.csv";
    	String input2 = "input2.csv";
    	String output = "outputLocation";
    	String input1type = "rectangle";
    	
    	spatialJoinQuery(input1, input2, output, input1type);
    }

	public static void spatialJoinQuery(
			String input1,
			String input2,
			String output,
			String input1type) {
    	SparkConf conf = new SparkConf().setAppName("SpatialJoinQuery").setMaster("local");
    	JavaSparkContext sparkContext = new JavaSparkContext(conf);
    	
		JavaRDD<String> file1 = sparkContext.textFile(input1);
		JavaRDD<String> file2 = sparkContext.textFile(input2);
		
		if(input1type.equals("point")) {
			JavaRDD<Point> points = file1.map(extractPoints);
			JavaRDD<Rectangle> bigRectangles = file2.map(extractRectangles); 
			
			final Broadcast<List<Point>> broadcastPoints = sparkContext.broadcast(points.collect());			
			
			JavaRDD<Tuple2<Integer, ArrayList<Integer>>> validPolygons = bigRectangles
					.map(new Function<Rectangle, Tuple2<Integer, ArrayList<Integer>>>() {
					
						private static final long serialVersionUID = 1L;

						public Tuple2<Integer, ArrayList<Integer>> call(Rectangle rectangle) throws Exception {	
							ArrayList<Point> points = (ArrayList<Point>) broadcastPoints.getValue();
							Integer rectangleId = rectangle.getId();
							ArrayList<Integer> pointIds = new ArrayList<Integer>();
							
							for (int i = 0; i< points.size(); i++) {
//								System.out.println("Lets see how many times here " + i);
								if(points.get(i).IsIn(rectangle)) {
									pointIds.add(points.get(i).getId());
								}
							}
							
							Tuple2<Integer, ArrayList<Integer>> tupleOfAidBid = new Tuple2<Integer, ArrayList<Integer>>(rectangleId, pointIds);
							Collections.sort(tupleOfAidBid._2);
							return tupleOfAidBid;
						}
					})
					.sortBy(new Function<Tuple2<Integer, ArrayList<Integer>>, Integer>(){
						
						private static final long serialVersionUID = 1L;

						public Integer call(Tuple2<Integer, ArrayList<Integer>> arg0) throws Exception {
							return arg0._1;
						}
				
					}, true, 1);
			 
			validPolygons.coalesce(1).saveAsTextFile(output);	
		} else if(input1type.equals("rectangle")) {
			JavaRDD<Rectangle> rectangles = file1.map(extractRectangles);
			JavaRDD<Rectangle> bigRectangles = file2.map(extractRectangles); 
			
			final Broadcast<List<Rectangle>> broadcastRectangles = sparkContext.broadcast(rectangles.collect());			
			
			JavaRDD<Tuple2<Integer, ArrayList<Integer>>> validPolygons = bigRectangles
					.map(new Function<Rectangle, Tuple2<Integer, ArrayList<Integer>>>() {
					
						private static final long serialVersionUID = 1L;

						public Tuple2<Integer, ArrayList<Integer>> call(Rectangle rectangle) throws Exception {	
							ArrayList<Rectangle> rectangles = (ArrayList<Rectangle>) broadcastRectangles.getValue();
							Integer rectangleId = rectangle.getId();
							ArrayList<Integer> smallRectangleIds = new ArrayList<Integer>();
							
							for (int i = 0; i< rectangles.size(); i++) {
//								System.out.println("Lets see how many times here " + i);
								if(rectangles.get(i).IsIn(rectangle)) {
									smallRectangleIds.add(rectangles.get(i).getId());
								}
							}
							
							Tuple2<Integer, ArrayList<Integer>> tupleOfAidBid = new Tuple2<Integer, ArrayList<Integer>>(rectangleId, smallRectangleIds);
							Collections.sort(tupleOfAidBid._2);
							return tupleOfAidBid;
						}
					})
					.sortBy(new Function<Tuple2<Integer, ArrayList<Integer>>, Integer>(){
						
						private static final long serialVersionUID = 1L;

						public Integer call(Tuple2<Integer, ArrayList<Integer>> arg0) throws Exception {
							return arg0._1;
						}
				
					}, true, 1);
			
			validPolygons.coalesce(1).saveAsTextFile(output);
		}
		sparkContext.close();
	}
}
