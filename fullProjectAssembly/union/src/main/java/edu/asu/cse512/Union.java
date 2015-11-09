package edu.asu.cse512;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import edu.asu.cse512.beans.Point;
import edu.asu.cse512.functions.CPolyUnionMapFunction;
import edu.asu.cse512.functions.CoordinateMapFunction;
import edu.asu.cse512.functions.CoordinateReduceFunction;
import edu.asu.cse512.functions.CoordinateType;
import edu.asu.cse512.functions.GeometryFileMapFunction;
import edu.asu.cse512.functions.PointComparator;
import edu.asu.cse512.functions.PointMapFunction;
import edu.asu.cse512.functions.PointPairFunction;
import edu.asu.cse512.functions.TupleToPointMapFunction;

/**
 * @author hdworker
 *
 */
public class Union {

	public Union(String appName, String master, String inputFile,
			String outputFile) {
		this.master = master;
		this.appName = appName;
		this.inputFile = inputFile;
		this.outputFile = outputFile;
	}

	private String inputFile;

	private String outputFile;

	private String appName;

	private String master;

	private SparkConf conf;

	private JavaSparkContext context;

	private FileType fileType;

	/*
	 * Main function, take two parameter as input, output
	 * 
	 * @param inputLocation
	 * 
	 * @param outputLocation
	 */
	public static void main(String[] args) {
		// Initialize, need to remove existing in output file location.

		// Implement

		// Output your result, you need to sort your result!!!
		// And,Don't add a additional clean up step delete the new generated
		// file...
		System.out.println("Union started");

		String appName = "Union";
		// String master = "local";
		String master = "spark://192.168.0.69:7077";

		String in;
		String out;

		if (!validateInputs(args)) {
			// Invalid inputs
			System.out.println("Invalid arguments");
			System.exit(1);
		}

		in = args[0].trim();
		out = args[1].trim();

		Union union = new Union(appName, master, in, out);
		// set the type of files to be read and stored
		union.setFileType(FileType.TEXT);

		try {
			union.geometryUnion();
		} catch (Exception e) {
			// Exception
			System.out.println("Exception occured: " + e.getMessage());
			// e.printStackTrace();
		} finally {
			union.close();
		}

	}

	private void init() throws Exception {
		conf = new SparkConf().setAppName(appName);
		context = new JavaSparkContext(conf);

		// set file type
		if (this.fileType == null)
			this.fileType = FileType.HDFS;
	}

	/**
	 * Union operation
	 */
	public void geometryUnion() throws Exception {
		// initialize apache spark and java context
		init();

		// read the input file
		JavaRDD<String> inputFileRDD = this.context.textFile(inputFile);

		// convert the input file to collections of Geometry shapes
		JavaRDD<Geometry> polygonsRDD = inputFileRDD
				.mapPartitions(new GeometryFileMapFunction());

		// Cascade union polygons
		JavaRDD<Geometry> cascadedPolygonRDD = polygonsRDD
				.mapPartitions(new CPolyUnionMapFunction());

		// Merge all partitions and execute the cascaded polygon union
		// it has only one polygon object
		JavaRDD<Geometry> finalPolygonRDD = cascadedPolygonRDD.coalesce(1)
				.mapPartitions(new CPolyUnionMapFunction());

		// finalPolygonRDD.cache();

		// Get the coordinates
		JavaRDD<Coordinate> coordinatesRDD = finalPolygonRDD
				.mapPartitions(new CoordinateMapFunction());

		// Cache the coordinate list
		// coordinatesRDD.cache();

		// final list of coordinates
		JavaRDD<Coordinate> finalCoordinatesRDD;

		// Get the first polygon
		Geometry finalPolygon = finalPolygonRDD.first();

		int numGeoms = finalPolygon.getNumGeometries();

		// If number of polygons are more than 1 in the geometry
		if (numGeoms > 1) {
			// has multiple polygons

			Coordinate minX = coordinatesRDD
					.reduce(new CoordinateReduceFunction(CoordinateType.MIN_X));

			Coordinate maxX = coordinatesRDD
					.reduce(new CoordinateReduceFunction(CoordinateType.MAX_X));

			Coordinate minY = coordinatesRDD
					.reduce(new CoordinateReduceFunction(CoordinateType.MIN_Y));

			Coordinate maxY = coordinatesRDD
					.reduce(new CoordinateReduceFunction(CoordinateType.MAX_Y));

			// new polygon with extreme points
			finalCoordinatesRDD = this.context.parallelize(
					Arrays.asList(new Coordinate[] {
							new Coordinate(minX.x, minY.y),
							new Coordinate(maxX.x, minY.y),
							new Coordinate(maxX.x, maxY.y),
							new Coordinate(minX.x, maxY.y) }), 1);
		} else {
			finalCoordinatesRDD = coordinatesRDD;
		}

		// Get distinct points in the coordinates
		JavaRDD<Point> finalPointsRDD = finalCoordinatesRDD.map(
				new PointMapFunction()).distinct();

		// Map to a tuple to sort the Points
		JavaPairRDD<Point, Boolean> pointTupleRDD = finalPointsRDD
				.mapToPair(new PointPairFunction());

		// Sort the points
		JavaPairRDD<Point, Boolean> sortedPointTupleRDD = pointTupleRDD
				.sortByKey(new PointComparator());

		// Map to points RDD
		JavaRDD<Point> finalSortedPointRDD = sortedPointTupleRDD
				.map(new TupleToPointMapFunction());

		// Write to a file
		finalSortedPointRDD.saveAsTextFile(this.outputFile);

	}

	public void close() {
		try {
			if (this.context != null)
				this.context.close();
		} catch (Exception e) {
			// continue
		}
	}

	/**
	 * @param args
	 * @return valid
	 */
	private static boolean validateInputs(String args[]) {
		return (null != args && args.length == 2);
	}

	public String getInputFile() {
		return inputFile;
	}

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(String master) {
		this.master = master;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}

}

enum FileType {
	HDFS, TEXT
}
