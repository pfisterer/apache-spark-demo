package de.farberg.spark.examples.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class CountHDFSExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Simple Application");
				
		JavaSparkContext sc = new JavaSparkContext(conf);

		//JavaRDD<String> sentences = sc.textFile("webhdfs://141.72.191.110:30704/daten/yarn.cmd");
		JavaRDD<String> sentences = sc.textFile("hdfs://hadoop-test-hadoop-hdfs-nn:9000/daten/yarn.cmd");
		JavaRDD<String> words = sentences.flatMap(sentence -> Lists.newArrayList(sentence.split(" ")).iterator());
		JavaPairRDD<String, Integer> wordMap = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> reduceByKey = wordMap.reduceByKey((a, b) -> a + b);

		
		//System.out.println(reduceByKey.collect());
		reduceByKey.saveAsTextFile("hdfs://hadoop-test-hadoop-hdfs-nn:9000/daten/demo.spark-out");
		
		sc.close();
	}
}
