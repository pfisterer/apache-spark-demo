package de.farberg.spark.examples.sql;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkSqlStreamingDemo {

	public static void main(String[] args) throws AnalysisException {

		SparkSession spark = SparkSession.builder().master("local[*]").appName("demo").getOrCreate();
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		SQLContext sqlContext = spark.sqlContext();

		List<ElectricConsumer> data = Arrays.asList(new ElectricConsumer(10, 49, 2, true),
				new ElectricConsumer(11, 47, 3, false));

		JavaRDD<ElectricConsumer> distData = sc.parallelize(data);

		Dataset<Row> dataFrame = sqlContext.createDataFrame(distData, ElectricConsumer.class);
		dataFrame.createTempView("consumers");

		Dataset<Row> activeConsumersDataFrame = sqlContext
				.sql("SELECT posLat, posLon, kwH, switchedOn FROM consumers WHERE switchedOn = true");

		List<String> activeConsumers = activeConsumersDataFrame
				.javaRDD()
				.map(row -> "posLon: " + row.getDouble(1) + ", posLat: " + row.getDouble(0) + ", kwh: " + row.getInt(2))
				.collect();

		System.out.println("------------------------------------------------------------");
		for (String consumer : activeConsumers)
			System.out.println(consumer);
		System.out.println("------------------------------------------------------------");
	}

}
