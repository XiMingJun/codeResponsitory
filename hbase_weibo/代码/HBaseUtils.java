package hbaseDemo.com.zhiyou.bd23;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseUtils {

	private static Configuration configuration = HBaseConfiguration.create();
	private static Connection connection;
	private static Admin admin;
	
	static {
		//初始化连接
		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("连接HBase失败");
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 获取连接
	 * */
	public static Connection getConnection() {
		if (connection == null) {
			try {
				connection = ConnectionFactory.createConnection(configuration);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("连接HBase失败");
				e.printStackTrace();
			}  
		}
		return connection;
	}
	
	/**
	 * 关闭连接
	 * */
	public static void closeConnection() {
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 获取表
	 * */
	public static Table getTable(String tableName) {
		try {
			return getConnection().getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("获取表失败哦");
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * 获取admin
	 * */
	public static Admin getAdmin() {
		if (admin == null) {
			try {
				admin = getConnection().getAdmin();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("获取admin失败");
				e.printStackTrace();
			}
		}
		return admin;
	}
	
	public static void printResultScanner(ResultScanner resultScanner) throws Exception{
		Result result = resultScanner.next();
		if (result == null) {
			System.out.println("没有查到相关数据");
		}
		while(result!=null){
			//每循环一次处理一行数据
			//把当前条的数据打印展现出来
			while(result.advance()){
				//每循环一次处理一个单元格
				Cell cell = result.current();
				byte[] row = CellUtil.cloneRow(cell);
				byte[] family = Arrays.copyOfRange(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyOffset()+cell.getFamilyLength());
				byte[] qualify = CellUtil.cloneQualifier(cell);
				long timestamp = cell.getTimestamp();
				byte[] value = CellUtil.cloneValue(cell);
				Type type = cell.getType();
				System.out.println("rowkey:"+Bytes.toString(row)+
						"|family:"+Bytes.toString(family)+
						"|qualify:"+Bytes.toString(qualify)+
						"|timestamp:"+timestamp+
						"|value:"+Bytes.toString(value)+
						"|vtype:"+type.name()
				);
			}
			//取一下一条数据
			result = resultScanner.next();
		}
		
		//listCells()
//		List<Cell> lists = result.listCells();
//		for(Cell cell:lists){
//			
//		}
	}
	
	public static void printResult(Result result){
		while(result.advance()){
			//每循环一次处理一个单元格
			Cell cell = result.current();
			byte[] row = CellUtil.cloneRow(cell);
			byte[] family = Arrays.copyOfRange(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyOffset()+cell.getFamilyLength());
			byte[] qualify = CellUtil.cloneQualifier(cell);
			long timestamp = cell.getTimestamp();
			byte[] value = CellUtil.cloneValue(cell);
			Type type = cell.getType();
			System.out.println("rowkey:"+Bytes.toString(row)+
					",family:"+Bytes.toString(family)+
					",qualify:"+Bytes.toString(qualify)+
					",timestamp:"+timestamp+
					",value:"+Bytes.toString(value)+
					",vtype:"+type.name()
			);
		}
	}
	
}
