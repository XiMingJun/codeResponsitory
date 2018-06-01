package com.zhiyou.bd23.skewdata;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProvinceUsers {
	public static class ProvinceUsersMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private String[] infos;
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//去除首行字段名称
			if(key.get()==0){
				return;
			}
			infos = StringUtils.split(value.toString(), ";");
			if(infos!=null && infos.length==5){
				outputKey.set(infos[2]+"|"+infos[4]);//provinceType|province_name
				outputValue.set(infos[3]);
				context.write(outputKey, outputValue);
			}
		}
	}
	public static class ProvinceUsersReducer extends Reducer<Text, Text, Text, Text>{
		private Text outputValue = new Text();
		private int sumNum;//总人口数
		private int maleNum;//男性人员个数
		private int fmaleNum;//女性的个数
		private double maleRate;//男性比例
		private double fmaleRate;//女性比例
		private double vsRate;//男女对比比例
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			sumNum=0;
			maleNum=0;
			fmaleNum=0;
			maleRate=0;
			fmaleRate=0;
			vsRate=0;
			for(Text value:values){
				sumNum += 1;
				if(value.toString().equals("male")){
					maleNum += 1;
				}else{
					fmaleNum += 1;
				}
			}
			maleRate = maleNum*1.00/sumNum;
			fmaleRate = fmaleNum*1.00/sumNum;
			vsRate = maleNum*1.00/fmaleNum;
			key.set(key.toString() + "|" + sumNum+"|"+maleNum+"|"+fmaleNum+"|"+maleRate+"|"+fmaleRate+"|"+vsRate);
			context.write(key, outputValue);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(ProvinceUsers.class);
		job.setJobName("倾斜数据");
		job.setMapperClass(ProvinceUsersMap.class);
		job.setReducerClass(ProvinceUsersReducer.class);
		job.setNumReduceTasks(3);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path input = new Path("/province_user.txt");
		Path outputDir = new Path("/skewdataoutput");
		outputDir.getFileSystem(configuration).delete(outputDir, true);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
