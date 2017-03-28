package com.happiestminds.demo;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.google.protobuf.ServiceException;

import scala.Tuple2;

/**
 * 
 * @author Sanjay.chowdhury
 *
 */
public class HBaseDataLoader implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final String TABLE_NAME = "test_table";

	private Dataset<Row> getJoinedRDD(SQLContext sqlContext, String[] args) {
		// Loading CSV files
		Dataset<Row> customer = sqlContext.read().option("header", "true").csv(args[0].toString());
		Dataset<Row> product = sqlContext.read().option("header", "true").csv(args[1].toString());
		Dataset<Row> sales = sqlContext.read().option("header", "true").csv(args[2].toString());

		// Removing the header lines
		Row customerHeader = customer.first();
		Row productHeader = product.first();
		Row salesHeader = sales.first();
		Dataset<Row> custRefined = customer.filter(row -> row.getString(0) != customerHeader.getString(0));
		Dataset<Row> prodRefined = product.filter(row -> row.getString(0) != productHeader.getString(0));
		Dataset<Row> salesRefined = sales.filter(row -> row.getString(0) != salesHeader.getString(0));

		// Generating the unit purchased count for each customer based on the
		// product purchased
		Dataset<Row> datasetSales = salesRefined
				.withColumn("unit_sales_tmp", salesRefined.col("unit_sales").cast("Int")).drop("unit_sales");
		Dataset<Row> groupedData = datasetSales.groupBy("customer_id", "product_id").sum("unit_sales_tmp");

		// Registering the datasets as temp views
		custRefined.createOrReplaceTempView("Customer");
		prodRefined.createOrReplaceTempView("Product");
		groupedData.createOrReplaceTempView("Unit_Count");

		// Performing join query and generating the final output
		Dataset<Row> joinedDataset = sqlContext.sql("SELECT concat(c.fname,' ',c.lname) AS Name, "
				+ "p.product_name AS Product_Name,uc.`sum(unit_sales_tmp)` AS Unit_Purchased FROM Customer c "
				+ "JOIN Unit_Count uc ON (c.customer_id=uc.customer_id)"
				+ "JOIN Product p ON (uc.product_id=p.product_id)");
		String[] columns = new String[1];
		columns[0] = "Name";
		return joinedDataset.dropDuplicates(columns);
	}

	private Configuration getConfiguration() {
		final Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "ip-172-31-46-225.us-west-2.compute.internal");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.rootdir", "hdfs://ip-172-31-46-225.us-west-2.compute.internal:8020/user/hbase");
		conf.set("zookeeper.znode.parent", "/user/hbase");
		return conf;
	}

	@SuppressWarnings("unused")
	private HTable getHbaseTable() throws IOException {
		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		TableName tableName = TableName.valueOf(TABLE_NAME);
		HTable hTable = (HTable) connection.getTable(tableName);
		return hTable;
	}

	/*
	 * private Job getJobInstance(Configuration conf) throws IOException { Job
	 * job = Job.getInstance(conf);
	 * job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
	 * job.setOutputKeyClass(ImmutableBytesWritable.class);
	 * job.setOutputValueClass(Put.class);
	 * job.setOutputFormatClass(TableOutputFormat.class); return job; }
	 */

	public static void main(String[] args) throws IOException, ServiceException {
		final SparkSession session = SparkSession.builder().appName("SparkHbase").getOrCreate();
		final SQLContext sqlContext = session.sqlContext();
		final HBaseDataLoader dataLoader = new HBaseDataLoader();
		// final Configuration configuration = dataLoader.getConfiguration();
		// final Job job = dataLoader.getJobInstance(configuration);
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "ip-172-31-46-225.us-west-2.compute.internal");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.rootdir", "hdfs://ip-172-31-46-225.us-west-2.compute.internal:8020/user/hbase");
		conf.set("zookeeper.znode.parent", "/user/hbase");
		Job job = Job.getInstance(conf);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);

		Dataset<Row> joinedDS = dataLoader.getJoinedRDD(sqlContext, args);

		JavaPairRDD<ImmutableBytesWritable, Put> rddToStore = joinedDS.javaRDD()
				.flatMapToPair(new PairFlatMapFunction<Row, ImmutableBytesWritable, Put>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<ImmutableBytesWritable, Put>> call(Row row) throws Exception {
						byte[] columnFamily = "description".getBytes();
						byte[] qualifire1 = "prodId".getBytes();
						byte[] qualifire2 = "unit_count".getBytes();
						String rowKey = row.getString(0);

						ImmutableBytesWritable hKey = new ImmutableBytesWritable();
						hKey.set(rowKey.getBytes());
						List<Tuple2<ImmutableBytesWritable, Put>> kvList = new ArrayList<Tuple2<ImmutableBytesWritable, Put>>();

						Put hValue1 = new Put(hKey.get());
						hValue1.addColumn(columnFamily, qualifire1, row.getString(1).getBytes());
						kvList.add(new Tuple2<ImmutableBytesWritable, Put>(hKey, hValue1));

						Put hValue2 = new Put(hKey.get());
						byte[] value = row.get(2).toString().getBytes();
						hValue2.addColumn(columnFamily, qualifire2, value);
						kvList.add(new Tuple2<ImmutableBytesWritable, Put>(hKey, hValue2));
						return kvList.iterator();
					}
				});
		rddToStore.saveAsNewAPIHadoopDataset(job.getConfiguration());

		/*
		 * rddToStore.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable,
		 * Put>>() { private static final long serialVersionUID = 1L;
		 * 
		 * @Override public void call(Tuple2<ImmutableBytesWritable, Put> tuple)
		 * throws Exception { HTable hTable = dataLoader.getHbaseTable(); Put
		 * hVPut = tuple._2(); hTable.put(hVPut); } });
		 */
	}
}