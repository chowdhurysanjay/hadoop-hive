package com.happiestminds.demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

@SuppressWarnings("deprecation")
public class HiveWithSpark {
	public static void main(String[] args) {
		try {
			final SparkSession session = SparkSession.builder().appName("HiveWithSpark").enableHiveSupport()
					.getOrCreate();
			HiveContext hiveContext = new HiveContext(session);
			Dataset<Row> dataset = hiveContext
					.sql("SELECT department,gender,count(SSN) from demo.employee GROUP BY department,gender");
			JavaRDD<Row> rdd = dataset.javaRDD();
			rdd.saveAsTextFile(args[0].toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}