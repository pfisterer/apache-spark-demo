package de.farberg.spark.examples.streaming;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scala.Tuple2;
import scala.Tuple4;

public class Demo {
	private static final String host = "localhost";

	static class AvgHelper implements Serializable {
		private static final long serialVersionUID = 1L;
		int sum;
		int count;
	}

	static Map<Integer, Double> roomTemps = Maps.newHashMap();

	public static void main(String[] args) throws FileNotFoundException, InterruptedException {

		// Create a server socket data source that sends string values every 100mss
		ServerSocketSource<String> dataSource = new ServerSocketSource<>(() -> "1,2,3,4", () -> 100);

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(),
				StorageLevels.MEMORY_AND_DISK_SER);

		// "personid, room, temp, roomTemp"
		@SuppressWarnings("resource")
		JavaDStream<Tuple4<Integer, Integer, Double, Double>> mappedValues = lines.map(line -> {
			String[] split = line.split(",");
			return new Tuple4<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Double.parseDouble(split[2]),
					Double.parseDouble(split[3]));
		});

		JavaPairDStream<Integer, Double> pair = mappedValues.mapToPair(entry -> new Tuple2<>(entry._2(), entry._3()));
		JavaPairDStream<Integer, Double> reduceByKey = pair.reduceByKey((a, b) -> b);
		reduceByKey.foreachRDD(d -> {
			d.foreach(value -> {
				roomTemps.put(value._1(), value._2());
			});

		});

		JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(x.split(" ")).iterator());

		JavaPairDStream<String, Integer> wordCounts = words
				.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
				.reduceByKey((i1, i2) -> i1 + i2);

		wordCounts.print();
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		dataSource.stop();
	}

}
