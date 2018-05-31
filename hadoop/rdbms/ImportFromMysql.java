package com.zhiyou.bd23.rdbms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//mysql 导出数据到 hdfs
public class ImportFromMysql {
	//1定义类型和数据库里面的数据映射
	public static class ImportFromMysqlDBWritable implements Writable, DBWritable{
		private String c1;
		private String c2;
		private String c3;
		private String c4;
		private String c5;
		private String c6;
		private String c7;
		private String c8;
		private String c9;
		private String c10;
		private String c11;
		private String c12;
		
		public String getC1() {
			return c1;
		}
		public void setC1(String c1) {
			this.c1 = c1;
		}
		public String getC2() {
			return c2;
		}
		public void setC2(String c2) {
			this.c2 = c2;
		}
		public String getC3() {
			return c3;
		}
		public void setC3(String c3) {
			this.c3 = c3;
		}
		public String getC4() {
			return c4;
		}
		public void setC4(String c4) {
			this.c4 = c4;
		}
		public String getC5() {
			return c5;
		}
		public void setC5(String c5) {
			this.c5 = c5;
		}
		public String getC6() {
			return c6;
		}
		public void setC6(String c6) {
			this.c6 = c6;
		}
		public String getC7() {
			return c7;
		}
		public void setC7(String c7) {
			this.c7 = c7;
		}
		public String getC8() {
			return c8;
		}
		public void setC8(String c8) {
			this.c8 = c8;
		}
		public String getC9() {
			return c9;
		}
		public void setC9(String c9) {
			this.c9 = c9;
		}
		public String getC10() {
			return c10;
		}
		public void setC10(String c10) {
			this.c10 = c10;
		}
		public String getC11() {
			return c11;
		}
		public void setC11(String c11) {
			this.c11 = c11;
		}
		public String getC12() {
			return c12;
		}
		public void setC12(String c12) {
			this.c12 = c12;
		}
		
		@Override
		public String toString() {
			return c1 + "|" + c2 + "|" + c3 + "|" + c4 + "|" + c5
					+ "|" + c6 + "|" + c7 + "|" + c8 + "|" + c9 + "|" + c10 + "|" + c11
					+ "|" + c12;
		}
		//DBWritable的序列化，把内存中的数据写入到mysql
		//insert into aaa values(?,?,?,?,?,?,?,?,?,?,?,?)
		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.c1);
			statement.setString(2, this.c2);
			statement.setString(3, this.c3);
			statement.setString(4, this.c4);
			statement.setString(5, this.c5);
			statement.setString(6, this.c6);
			statement.setString(7, this.c7);
			statement.setString(8, this.c8);
			statement.setString(9, this.c9);
			statement.setString(10, this.c10);
			statement.setString(11, this.c11);
			statement.setString(12, this.c12);
		}
		//DBWritable的反序列化，把数据从mysql读取到内存中来
		public void readFields(ResultSet resultSet) throws SQLException {
			this.c1 = resultSet.getString("c1");
			this.c2 = resultSet.getString("c2");
			this.c3 = resultSet.getString("c3");
			this.c4 = resultSet.getString("c4");
			this.c5 = resultSet.getString("c5");
			this.c6 = resultSet.getString("c6");
			this.c7 = resultSet.getString("c7");
			this.c8 = resultSet.getString("c8");
			this.c9 = resultSet.getString("c9");
			this.c10 = resultSet.getString("c10");
			this.c11 = resultSet.getString("c11");
			this.c12 = resultSet.getString("c12");
		}
		//Writable的序列化过程
		public void write(DataOutput out) throws IOException {
			out.writeUTF(c1);
			out.writeUTF(c2);
			out.writeUTF(c3);
			out.writeUTF(c4);
			out.writeUTF(c5);
			out.writeUTF(c6);
			out.writeUTF(c7);
			out.writeUTF(c8);
			out.writeUTF(c9);
			out.writeUTF(c10);
			out.writeUTF(c11);
			out.writeUTF(c12);
		}
		//Writable的反序列化过程
		public void readFields(DataInput in) throws IOException {
			this.c1 = in.readUTF();
			this.c2 = in.readUTF();
			this.c3 = in.readUTF();
			this.c4 = in.readUTF();
			this.c5 = in.readUTF();
			this.c6 = in.readUTF();
			this.c7 = in.readUTF();
			this.c8 = in.readUTF();
			this.c9 = in.readUTF();
			this.c10 = in.readUTF();
			this.c11 = in.readUTF();
			this.c12 = in.readUTF();
		}
	}
	//2在map里从mysql取出数据
	public static class ImportFromMysqlMap extends Mapper<LongWritable, ImportFromMysqlDBWritable, Text, NullWritable>{
		private Text outputKey = new Text();
		private NullWritable outputValue = NullWritable.get();
		@Override
		protected void map(LongWritable key, ImportFromMysqlDBWritable value,
				Mapper<LongWritable, ImportFromMysqlDBWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			outputKey.set(value.toString());
			context.write(outputKey, outputValue);
		}
	}
	//不需要reduce
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//配置mysql连接
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test3", "root", "wang201314");
		Job job = Job.getInstance(conf);
		job.setJarByClass(ImportFromMysql.class);
		job.setJobName("从mysql中导入数据到hdfs上");
		
		job.setMapperClass(ImportFromMysqlMap.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//配置mysql输入数据内容
		DBInputFormat.setInput(job, ImportFromMysqlDBWritable.class, "aaa", "", "c1", "*");
		
		Path outputDir = new Path("/frommysqlaaa");
		outputDir.getFileSystem(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
