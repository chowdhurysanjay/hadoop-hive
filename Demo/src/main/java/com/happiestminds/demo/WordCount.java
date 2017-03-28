package com.happiestminds.demo;

import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * WordCount example in Spark 2.0
 * 
 * @author Sanjay.chowdhury
 *
 */
public class WordCount {
	public static void main(String[] args) {
		try {
			final SparkSession session = SparkSession.builder().appName("Word Count").getOrCreate();
			final SparkContext sparkContext = session.sparkContext();
			final String inputPath = args[0].toString();
			final String outputPath = args[1].toString();

			JavaRDD<String> textFile = sparkContext.textFile(inputPath, 1).toJavaRDD();
			JavaPairRDD<String, Integer> wordCount = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
					.mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((a, b) -> a + b);
			wordCount.saveAsTextFile(outputPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}