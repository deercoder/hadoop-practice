package com.deercoder.score;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.*;

public class HandlerData {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private String strPopulation = "";
		private String strID = "";
		private String strCode = "";
		private String strDistrict = "";
		
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String line = value.toString();
			// for debugging, print out each line
			System.out.println(line);
			
			// split by space
			StringTokenizer tokenizerLine = new StringTokenizer(line, ",");
			
			// treat each line
			while (tokenizerLine.hasMoreTokens()) {
				
				// get the content of each line
				String eachLine = tokenizerLine.nextToken();
				System.out.println("each line: " + eachLine);		
				strID = tokenizerLine.nextToken();
				if(tokenizerLine.hasMoreTokens()) {
					strCode = tokenizerLine.nextToken();
					
					if (tokenizerLine.hasMoreTokens()) {
						strDistrict = tokenizerLine.nextToken();
						
						if (tokenizerLine.hasMoreTokens()) {
							strPopulation = tokenizerLine.nextToken();

						}
					}
				}
				
				
				Text name = new Text(strID);
				int scoreInt = Integer.parseInt(strPopulation);
				output.collect(name, new IntWritable(scoreInt));
			}

			
		}	
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			int count = 0;
			
			while (values.hasNext()) {
				sum += values.next().get();
				count++;
			}
			
			int average = (int) sum / count;
			output.collect(key, new IntWritable(average));
			
		}	
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(HandlerData.class);
		conf.setJobName("cityOps");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path("city.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("input"));
		
		JobClient.runJob(conf);
		
	}
	
	
	
	
	
}
