package de.farberg.spark.examples.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class FlatMapExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> sentences = sc.parallelize(Lists.newArrayList("Das ist ein Satz", "Das ist auch ein Satz"));

		JavaRDD<String> words = 
					sentences.flatMap(sentence -> Lists.newArrayList(sentence.split(" ")).iterator());
		
		JavaPairRDD<String, Integer> wordMap = 
					words.mapToPair(word -> new Tuple2<>(word, 1));
		
		JavaPairRDD<String, Integer> reduceByKey = 
					wordMap.reduceByKey((a, b) -> a + b);

		System.out.println("sentences: " + sentences.collect());
		System.out.println("words: " + words.collect());
		System.out.println("wordMap: " + wordMap.collect());
		System.out.println("reduceByKey: " + reduceByKey.collect());

		sc.close();
	}
}
