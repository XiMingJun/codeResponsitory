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
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//hdfs导出数据到mysql

public class ExportToMysql {
	//自定义dbwritable的实现类完成java类和数据库表的映射
//	CREATE TABLE department(
//	department_id INT
//	,department_name VARCHAR(45)
//	)
	public static class DepartmentDBWritable implements Writable, DBWritable{
		private Integer departmentId;
		private String departmentName;
		public Integer getDepartmentId() {
			return departmentId;
		}
		public void setDepartmentId(Integer departmentId) {
			this.departmentId = departmentId;
		}
		public String getDepartmentName() {
			return departmentName;
		}
		public void setDepartmentName(String departmentName) {
			this.departmentName = departmentName;
		}

		public void write(PreparedStatement statement) throws SQLException {
			statement.setInt(1, this.departmentId);
			statement.setString(2, this.departmentName);
		}

		public void readFields(ResultSet resultSet) throws SQLException {
			this.departmentId = resultSet.getInt("department_id");
			this.departmentName = resultSet.getString("department_name");
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(this.departmentId);
			out.writeUTF(this.departmentName);
		}

		public void readFields(DataInput in) throws IOException {
			this.departmentId = in.readInt();
			this.departmentName = in.readUTF();
		}
	}
	//map把要存储到数据库的数据以DepartmentDBWritable的类型放在map的输出key上即可把数据导出到数据库中
	public static class ExportToMysqlMap extends Mapper<LongWritable, Text, DepartmentDBWritable, NullWritable>{
		private DepartmentDBWritable outputKey = new DepartmentDBWritable();
		private NullWritable outputValue = NullWritable.get();
		private String[] infos;
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, DepartmentDBWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			infos = value.toString().split("\\|");
			outputKey.setDepartmentId(Integer.valueOf(infos[0]));
			outputKey.setDepartmentName(infos[1]);
			context.write(outputKey, outputValue);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		DBConfiguration.configureDB(configuration, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test3", "root", "wang201314");
		Job job = Job.getInstance(configuration);
		job.setJarByClass(ExportToMysql.class);
		job.setJobName("导出数据到mysql中");
		job.setMapperClass(ExportToMysqlMap.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(DepartmentDBWritable.class);
		job.setOutputValueClass(NullWritable.class);
		Path input = new Path("/orderdata/departments");
		FileInputFormat.addInputPath(job, input);
		DBOutputFormat.setOutput(job, "department", "department_id", "department_name");
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
