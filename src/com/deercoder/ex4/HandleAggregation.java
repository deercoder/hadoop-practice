package com.deercoder.ex4;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * This Program is used to build the relationship between different tables We
 * can learn how to use MapReduce to handle them in just one table
 * 
 * 
 * city.txt:
 * City (ID, Name, CountryCode, District, population)
 * 
 * countrylanguage.txt:
 * CountryLanguage (CountryCode, Language, IsOfficial, Percentage)
 * 
 * Used the joint key `CountryCode` to generate the city lists.
 * 
 * @author Chang Liu
 *
 */
public class HandleAggregation extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		
		private IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] list = line.split(",");
			String district = list[3];
			context.write(new Text(district), one);
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			IntWritable sum = new IntWritable(0);
			for (IntWritable val:values) {
				sum = new IntWritable(val.get() + sum.get());
			}
			context.write(key, sum);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(HandleAggregation.class);
		job.setJobName("HandleAggregation");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("data/city.txt"));
		FileOutputFormat.setOutputPath(job, new Path("ex4"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new HandleAggregation(), args);
		System.exit(ret);
	}

}