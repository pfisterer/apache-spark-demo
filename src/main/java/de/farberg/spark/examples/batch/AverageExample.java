package de.farberg.spark.examples.batch;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

public class AverageExample {

	static class AvgHelper implements Serializable {
		private static final long serialVersionUID = 1L;
		int sum;
		int count;
	}

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> numbers = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));

		@SuppressWarnings("resource")
		AvgHelper aggregate = numbers.aggregate(new AvgHelper(), (a, x) -> {
			a.sum += x;
			a.count += 1;
			return a;
		}, (a, b) -> {
			a.sum += b.sum;
			a.count += b.count;
			return a;
		});

		System.out.println(aggregate.sum / aggregate.count);

		sc.close();
	}
}
