package de.farberg.spark.examples.streaming;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;
import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;

import scala.Tuple2;

public class TimHortonsFuellstand {
	private static final String host = "localhost";

	@SuppressWarnings("resource")
	public static void main(String[] args) throws FileNotFoundException, InterruptedException {
		AtomicBoolean b = new AtomicBoolean(false);
		int maxDistanceToTimmy = 50000;

		ServerSocketSource<String> dataSource = new ServerSocketSource<>(() -> {
			b.set(!b.get());
			return b.get() ? "49.48893, 8.46726" : "49.48893, 7.46726";
		}, () -> 500);

		List<LatLng> timHortonsList = Lists.newArrayList(new LatLng(43.9447432, -78.8960708), new LatLng(49.4093582, 8.6947240),
				new LatLng(49.4895910, 8.4672360));

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

		JavaRDD<LatLng> timHortons = sc.parallelize(timHortonsList);

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> positionOfCarsAsText = ssc.socketTextStream(host, dataSource.getLocalPort(),
				StorageLevels.MEMORY_AND_DISK_SER);

		// String -> LatLng: 49.48893, 8.46726 --> new LatLng(49.48893, 8.46726)
		JavaDStream<LatLng> carPositions = positionOfCarsAsText.map(line -> {
			String[] split = line.split(",");
			return new LatLng(Double.parseDouble(split[0]), Double.parseDouble(split[1]));
		});

		// Cartesian Produkt carPositions x timHortons (um eine Distanztabelle aufzubauen)
		JavaDStream<Tuple2<LatLng, LatLng>> distanceMatrix = carPositions.<Tuple2<LatLng, LatLng>> transform(rdd -> {
			JavaPairRDD<LatLng, LatLng> cartesian = rdd.cartesian(timHortons);
			JavaRDD<Tuple2<LatLng, LatLng>> rdd2 = cartesian.rdd().toJavaRDD();
			return rdd2;
		});

		// Ergänze jeden Eintrag um die Distanz
		JavaDStream<Tuple2<Tuple2<LatLng, LatLng>, Double>> distances = distanceMatrix.map(coordinates -> {
			double distance = LatLngTool.distance(coordinates._1, coordinates._2, LengthUnit.METER);
			return new Tuple2<Tuple2<LatLng, LatLng>, Double>(coordinates, distance);
		});

		// Filtere die raus, deren Distanz zu groß ist
		JavaDStream<Tuple2<Tuple2<LatLng, LatLng>, Double>> closeToTimmy = distances
				.filter(dist -> dist._2.doubleValue() < maxDistanceToTimmy);

		// Map Timmy -> 1
		JavaPairDStream<String, Integer> timmyTo1 = closeToTimmy
				.mapToPair(dist -> new Tuple2<String, Integer>(dist._1._2.toString(), 1));

		// Count per Key
		JavaPairDStream<String, Integer> countPerTimmy = timmyTo1.reduceByKey((x, y) -> x + y);

		countPerTimmy.print();

		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		sc.close();

	}

}
