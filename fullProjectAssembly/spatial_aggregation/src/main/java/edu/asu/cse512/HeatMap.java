package edu.asu.cse512;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * @author hdworker
 *
 */
public class HeatMap {
	// helper function to extract the coordinates from input file and define
	// points
	private static Function<String, Point> extractPoints = new Function<String, Point>() {

		private static final long serialVersionUID = 1L;

		public Point call(String arg0) throws Exception {
			// System.out.println(arg0);
			List<String> list = Arrays.asList(arg0.split(","));
			int id = Integer.parseInt(list.get(0).trim());
			double x1 = Double.parseDouble(list.get(1).trim());
			double y1 = Double.parseDouble(list.get(2).trim());

			Point point = new Point(id, x1, y1);
			return point;
		}
	};

	// helper function to extract the coordinates from input file and define
	// rectangles
	private static Function<String, Rectangle> extractRectangles = new Function<String, Rectangle>() {

		private static final long serialVersionUID = 1L;

		public Rectangle call(String arg0) throws Exception {
			List<String> list = Arrays.asList(arg0.split(","));
			int id = Integer.parseInt(list.get(0));
			double x1 = Double.parseDouble(list.get(1).trim());
			double y1 = Double.parseDouble(list.get(2).trim());
			double x2 = Double.parseDouble(list.get(3).trim());
			double y2 = Double.parseDouble(list.get(4).trim());

			Rectangle rectangle = new Rectangle(id, x1, x2, y1, y2);
			return rectangle;
		}
	};

	// helper function to convert tuple to string i.e. to remove opening '(' and
	// closing ')' from the output
	public static Function<Tuple2<Integer, String>, String> arrangeOutput = new Function<Tuple2<Integer, String>, String>() {

		private static final long serialVersionUID = 1L;

		public String call(Tuple2<Integer, String> arg0) throws Exception {
			return arg0._1.toString() + "," + arg0._2;
		}
	};

	// helper function to retrieve Bids from the tuple2 for sorting
	public static Function<Tuple2<Integer, String>, Integer> retrieveBid = new Function<Tuple2<Integer, String>, Integer>() {

		private static final long serialVersionUID = 1L;

		public Integer call(Tuple2<Integer, String> arg0) throws Exception {
			return arg0._1;
		}
	};

	public static void main(String[] args) {
		String input1 = args[0];
		String input2 = args[1];
		String output = args[2];
		String input1type = args[3];

		spatialJoinQuery(input1, input2, output, input1type);
	}

	/**
	 * @param inputFile
	 *            Input file either rectangle or point
	 * @param rectangleInputFile
	 *            Rectangle query window
	 * @param output
	 *            output file
	 * @param type
	 *            rectangle or point
	 */
	public static void spatialJoinQuery(String inputFile,
			String rectangleInputFile, String output, String type) {

		SparkConf conf = new SparkConf().setAppName("Group6-SpatialJoinQuery");
		// remove this
		// conf.setMaster("local");
		// remove end
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		// making Java RDDs from input files
		JavaRDD<String> file1 = sparkContext.textFile(inputFile);
		JavaRDD<String> file2 = sparkContext.textFile(rectangleInputFile);

		// checking whether type of input1 is point or rectangle and proceeding
		// accordingly
		if (type.equals("point")) {
			// mapping input1 to Point
			JavaRDD<Point> points = file1.map(extractPoints);

			// mapping input2 to Rectangle
			JavaRDD<Rectangle> bTypeRectangles = file2.map(extractRectangles);

			// broadcasting input1 so that every worker has a copy of them
			final Broadcast<List<Point>> broadcastPoints = sparkContext
					.broadcast(points.collect());

			JavaPairRDD<Count, Boolean> countPairRDD = bTypeRectangles
					.mapToPair(new PairFunction<Rectangle, Count, Boolean>() {

						private static final long serialVersionUID = 1L;

						public Tuple2<Count, Boolean> call(Rectangle rect)
								throws Exception {
							ArrayList<Point> points = (ArrayList<Point>) broadcastPoints
									.getValue();
							int count = 0;

							for (Point p : points)
								if (p.isIn(rect))
									count++;

							return new Tuple2<Count, Boolean>(new Count(rect
									.getId(), count), true);
							// return null;
						}
					});

			// sort by key
			JavaPairRDD<Count, Boolean> sortedCountPairRDD = countPairRDD
					.sortByKey(new CountComp());

			// map to Count
			JavaRDD<Count> countRDD = sortedCountPairRDD
					.map(new Function<Tuple2<Count, Boolean>, Count>() {
						private static final long serialVersionUID = 1L;

						public Count call(Tuple2<Count, Boolean> t)
								throws Exception {
							return t._1;
						}
					});

			// save as RDD
			countRDD.saveAsTextFile(output);

		} else if (type.equals("rectangle")) {
			// mapping input1 to Rectangle
			JavaRDD<Rectangle> rectanglesInput1 = file1.map(extractRectangles);

			// mapping input2 to Rectangle
			JavaRDD<Rectangle> bTypeRectangles = file2.map(extractRectangles);

			// broadcasting input1 so that every worker has a copy of them
			final Broadcast<List<Rectangle>> broadcastRectangles = sparkContext
					.broadcast(rectanglesInput1.collect());

			// mapping every bTypeRectangle with aTypeInput to check for join
			JavaRDD<Tuple2<Integer, String>> validPolygons = bTypeRectangles
					.map(new Function<Rectangle, Tuple2<Integer, String>>() {

						private static final long serialVersionUID = 1L;

						public Tuple2<Integer, String> call(Rectangle rectangle)
								throws Exception {

							// getting all the bTypeRectangles in an array list
							ArrayList<Rectangle> rectangles = (ArrayList<Rectangle>) broadcastRectangles
									.getValue();

							Integer rectangleId = rectangle.getId();
							ArrayList<Integer> rectangleIDs = new ArrayList<Integer>();

							// checking if bTyperectangles contain
							// aTypeRectangles and adding their IDs if true
							for (int i = 0; i < rectangles.size(); i++) {
								if (rectangles.get(i).has(rectangle)) {
									rectangleIDs.add(rectangles.get(i).getId());
								}
							}

							// making a tuple of AtypeIDandBTypeRectangleIDs
							Tuple2<Integer, ArrayList<Integer>> tupleOfAidBid = new Tuple2<Integer, ArrayList<Integer>>(
									rectangleId, rectangleIDs);

							// Sorting aTypeIDs in ascending order
							Collections.sort(tupleOfAidBid._2);

							// arranging the IDs as per requirement document
							StringBuilder aidList = new StringBuilder();
							for (int i = 0; i < tupleOfAidBid._2.size(); i++) {

								aidList.append(tupleOfAidBid._2.get(i) + ",");
							}
							int i;
							if (0 < (i = aidList.length())) {
								// deleting final ','
								aidList.deleteCharAt(i - 1);
							} else {
								// adding null as per requirement documentation
								aidList.append("NULL");
							}

							Tuple2<Integer, String> BidAid = new Tuple2<Integer, String>(
									rectangleId, aidList.toString());
							return BidAid;
						}
					}).sortBy(retrieveBid, true, 1); // sorting in ascending
														// order

			// arranging the result as per requirement document
			JavaRDD<String> result = validPolygons.map(arrangeOutput);

			// saving the result in a hdfs file
			result.coalesce(1).saveAsTextFile(output);
		}
		sparkContext.close();
	}
}
