package com.happiestminds.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {
	public static void main(String[] args) {
		final SparkSession session = SparkSession.builder().appName("HiveWithSpark").enableHiveSupport().getOrCreate();
		SQLContext sqlContext = session.sqlContext();
		Dataset<Row> customer = sqlContext.read().option("header", "true").csv(args[0].toString());
		Dataset<Row> product = sqlContext.read().option("header", "true").csv(args[1].toString());
		Dataset<Row> sales = sqlContext.read().option("header", "true").csv(args[2].toString());

		Row customerHeader = customer.first();
		Row productHeader = product.first();
		Row salesHeader = sales.first();

		System.out.println(salesHeader.toString());

		Dataset<Row> custRefined = customer.filter(row -> row.getString(0) != customerHeader.getString(0));
		Dataset<Row> prodRefined = product.filter(row -> row.getString(0) != productHeader.getString(0));
		Dataset<Row> salesRefined = sales.filter(row -> row.getString(0) != salesHeader.getString(0));

		Dataset<Row> datasetSales = salesRefined
				.withColumn("unit_sales_tmp", salesRefined.col("unit_sales").cast("Int")).drop("unit_sales");
		Dataset<Row> groupedData = datasetSales.groupBy("customer_id","product_id").sum("unit_sales_tmp");
		
		custRefined.createOrReplaceTempView("Customer");
		prodRefined.createOrReplaceTempView("Product");
		groupedData.createOrReplaceTempView("Unit_Count");
		
		Dataset<Row> joinedDataset = sqlContext.sql("SELECT concat(c.fname,' ',c.lname) AS Name, "
				+ "p.product_name AS Product_Name,uc.`sum(unit_sales_tmp)` AS Unit_Purchased FROM Customer c "
				+ "JOIN Unit_Count uc ON (c.customer_id=uc.customer_id)"
				+ "JOIN Product p ON (uc.product_id=p.product_id)");
		
		joinedDataset.javaRDD().saveAsTextFile(args[3]);
	}
}
