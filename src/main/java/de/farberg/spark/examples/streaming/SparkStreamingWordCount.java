package de.farberg.spark.examples.streaming;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class SparkStreamingWordCount {
	private static final String host = "localhost";
	private static final int port = 9999;

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		// Listen on a server socket and on connection send some \n-delimited text to the client
		new Thread(() -> {
			try {
				ServerSocket serverSocket = new ServerSocket(port);

				while (true) {
					Socket clientSocket = serverSocket.accept();
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

					for (; true;) {
						out.println("Das ist ein Test");
						out.flush();
						Thread.sleep(100);
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, port, StorageLevels.MEMORY_AND_DISK_SER);

		JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(x.split(" ")));

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1)).reduceByKey(
				(i1, i2) -> i1 + i2);

		wordCounts.print();
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
	}

}
