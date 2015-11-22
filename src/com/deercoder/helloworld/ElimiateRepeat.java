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

public class ElimiateRepeat extends Configured implements Tool {

	/**
	 * Mapper has four parameters, they are the inputkey, input value, outputkey, output value type
	 * Here, for Map class, the input is the index(longwritable), and the whole text content for this list
	 * the output, is the pair of <word, num>, so it is Text Type and IntWritable
	 * @author Chang Liu<chang_liu@student.uml.edu>
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\n");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	/**
	 * Reducer has four parameters, they are the inputkey, input value, outputkey, output value type
	 * Here, for Reduce class, the input is mapper's output, which is <Text, Intwritable> type
	 * the output, is the pair of result, which means each word and its count, type is also <Text, IntWritable>
	 * @author Chang Liu<chang_liu@student.uml.edu>
	 *
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, new IntWritable(0));
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(ElimiateRepeat.class);
		job.setJobName("ElimiateRepeat");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("file0*"));
		FileOutputFormat.setOutputPath(job, new Path("elimiateRepeat"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new ElimiateRepeat(), args);
		System.exit(ret);
	}

}