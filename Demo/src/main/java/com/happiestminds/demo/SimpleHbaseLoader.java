package com.happiestminds.demo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class SimpleHbaseLoader {
	public static void main(String[] args) {
		try {
			String tableName = "myTable";
			byte[] rowKey = "10011".getBytes();
			byte[] columnFamily = "description".getBytes();
			byte[] qualifier = "age".getBytes();
			byte[] value = "20".getBytes();

			TableName hTable = TableName.valueOf(tableName);

			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "ip-172-31-46-225.us-west-2.compute.internal");
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			Connection connection = ConnectionFactory.createConnection(conf);
			HTable table = (HTable) connection.getTable(hTable);

			Scan scan = new Scan();
			scan.addColumn(columnFamily, qualifier);
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				String colValue = new String(iterator.next().value().toString());
				System.out.println("Result Found: " + colValue);
			}
			scanner.close();

			ImmutableBytesWritable hKey = new ImmutableBytesWritable();
			hKey.set(rowKey);

			Put hValue = new Put(hKey.get());
			hValue.addColumn(columnFamily, qualifier, value);

			table.put(hValue);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
