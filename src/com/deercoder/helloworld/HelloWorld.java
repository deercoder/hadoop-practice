package com.deercoder.helloworld;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class HelloWorld {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static int count = 0;
		
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			count += 1;
			// get the input string from the file
			String line = value.toString();
			System.out.println("line " + count +  ": " + line);
			// treat each string and split by space by default
			StringTokenizer tokenizer = new StringTokenizer(line);
			// get words and do iteratively
			while (tokenizer.hasMoreTokens()) {
				// for each word, eject (word, 1) as one item, for example ("Hello", 1)
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}	
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			
			int sum = 0;
			// continuously get the items and merge the count value
			while (values.hasNext()) {
				// here next means the second value, which is "1" for an item
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}	
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(HelloWorld.class);
		conf.setJobName("wordcount");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path("file0*"));
		FileOutputFormat.setOutputPath(conf, new Path("input"));
		
		JobClient.runJob(conf);
		
	}
	
	
	
	
	
}
