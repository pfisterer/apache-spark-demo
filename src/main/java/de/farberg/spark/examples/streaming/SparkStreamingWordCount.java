package de.farberg.spark.examples.streaming;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class SparkStreamingWordCount {
	// TODO: Alles umbauem zu SparkContext wie in einer Klasse schon geschehen

	private static final String host = "localhost";

	public static void main(String[] args) throws FileNotFoundException, InterruptedException {

		// Create a server socket data source that sends string values every 100mss
		ServerSocketSource<String> dataSource = new ServerSocketSource<>(() -> "Das ist ein ein ein ein Test", () -> 100);

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[*]");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(),
				StorageLevels.MEMORY_AND_DISK_SER);

		JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(x.split(" ")).iterator());

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey(
				(i1, i2) -> i1 + i2);

		wordCounts.print();
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		dataSource.stop();
	}

}
