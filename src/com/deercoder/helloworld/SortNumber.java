package com.deercoder.helloworld;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class SortNumber extends Configured implements Tool {

	/**
	 * Map function is to convert the input into IntWritable type, and then
	 * output as the key Since MapReduce will automatically sort them in order
	 * 
	 * @author Chang Liu<chang_liu@student.uml.edu>
	 *
	 */
	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

		private static IntWritable data = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (!line.isEmpty()) {
				data.set(Integer.parseInt(line));
				context.write(data, new IntWritable(1));
			}
		}
	}

	/**
	 * Reducer has four parameters, they are the inputkey, input value,
	 * outputkey, output value type Here, for Reduce class, the input is
	 * mapper's output, which is <Text, Intwritable> type the output, is the
	 * pair of result, which means each word and its count, type is also <Text,
	 * IntWritable>
	 * 
	 * @author Chang Liu<chang_liu@student.uml.edu>
	 *
	 */
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private static IntWritable linenum = new IntWritable(1);

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(linenum, key);
				linenum = new IntWritable(linenum.get() + 1);
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(SortNumber.class);
		job.setJobName("SortNumber");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("num*"));
		FileOutputFormat.setOutputPath(job, new Path("sortNumber"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SortNumber(), args);
		System.exit(ret);
	}

}