package de.farberg.spark.examples.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class NasaLogFile {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> sentences = sc.textFile("src/main/resources/NASA_access_log_Jul95.gz");

		JavaRDD<String> visitors = sentences.map(visitor -> visitor.split(" ")[0]);

		JavaPairRDD<String, Integer> wordMap = visitors.mapToPair(visitor -> new Tuple2<>(visitor, 1));

		JavaPairRDD<String, Integer> visitorCount = wordMap.reduceByKey((a, b) -> a + b);

		visitorCount.foreach(visitor -> System.out.println(visitor._1 + ": " + visitor._2));

		sc.close();
	}
}
