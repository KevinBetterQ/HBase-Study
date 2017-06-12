package com.hbase.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class StudentHbase {

	/**
	 * @param args
	 */
	static Configuration conf = HBaseConfiguration.create();	
	//通过HBaseAdmin HTableDescriptor来创建一个新表
	public static void create(String tableName, String[] columnFamily) throws Exception{
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(tableName);
		if(admin.tableExists(tn)){
			System.out.println("Table exist");
			System.exit(0);
		}
		else {
			HTableDescriptor table = new HTableDescriptor(tn);
			for(int i=0;i<columnFamily.length;i++){
				table.addFamily(new HColumnDescriptor(columnFamily[i]).setCompactionCompressionType(Algorithm.NONE));
			}
			
			admin.createTable(table);
			System.out.println("Table create success");
		}
	}
	
	//添加一条数据，通过HTable Put为已存在的表添加数据
	public static void put(String tableName,String row,String columnFamily,String column,String data) throws IOException{
		Connection con = ConnectionFactory.createConnection(conf);
		TableName tn = TableName.valueOf(tableName);
		Table table = con.getTable(tn);
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
		table.put(put);
		System.out.println(row+"  put success");
	}
	
	//获取tableName表里列为row的结果集
	public static void get(String tableName,String row) throws IOException{
		Connection con = ConnectionFactory.createConnection(conf);
		Table table = con.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		System.out.println("get "+ result);	
	}
	
	//通过HTable Scan来获取tableName表的所有数据信息
	public static void scan (String tableName) throws IOException{
		Connection con = ConnectionFactory.createConnection(conf);
		Table table = con.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result s : resultScanner){
			System.out.println("Scan "+ s);
		}
	}
	
	public static boolean delete(String tableName) throws Exception{
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(tableName);
		if(admin.tableExists(tn)){
			try {
				admin.disableTable(tn);
				admin.deleteTable(tn);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
	
	public static void readfile(String filename){
		File file = new File(filename);
		BufferedReader reader = null;
		String temString = null;
		
		try {
			reader = new BufferedReader(new FileReader(file));
			
				while((temString = reader.readLine() )!=null){
					String[] aa = temString.split("\t");
						StudentHbase.put("student", aa[0], "studentno", "", aa[1]);
						StudentHbase.put("student", aa[0], "name", "lastname", aa[2]);
						StudentHbase.put("student", aa[0], "name", "firstname", aa[3]);
						StudentHbase.put("student", aa[0], "gender", "", aa[4]);
						StudentHbase.put("student", aa[0], "birthday", "year", aa[5]);
						StudentHbase.put("student", aa[0], "birthday", "month", aa[6]);
						StudentHbase.put("student", aa[0], "birthday", "day", aa[7]);
						StudentHbase.put("student", aa[0], "class", "profession", aa[8]);
						StudentHbase.put("student", aa[0], "class", "number", aa[9]);
						StudentHbase.put("student", aa[0], "address", "province", aa[10]);
						StudentHbase.put("student", aa[0], "address", "city", aa[11]);
						StudentHbase.put("student", aa[0], "address", "addr", aa[12]);
						StudentHbase.put("student", aa[0], "scores", "sub_a", aa[13]);
						StudentHbase.put("student", aa[0], "scores", "sub_b", aa[14]);
				
				}
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public static void main(String[] args) {
		
		String tableName = "student";
		String[] columnFamily ={ "studentno","name", "gender", "birthday", "class", "address", "scores"};
		
		try {
			StudentHbase.create(tableName, columnFamily);
			//StudentHbase.put(tableName, "2", "studentno", "", "data2");
			//StudentHbase.get(tableName, "row1");
			//StudentHbase.scan(tableName);
			
//			if(HBaseExample.delete(tableName)==true){
//				System.out.println("delete table "+ tableName+"success");
//			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
		readfile("/home/hadoop/hbasefile.txt");
		
	}
}