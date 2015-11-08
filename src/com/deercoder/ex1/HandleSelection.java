package com.deercoder.ex1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.*;

public class HandleSelection {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private String strID = "";
		private String strName = "";
		private String strCode = "";
		private String strDistrict = "";
		private String strPopulation = "";

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();

			// for debugging, print out each line
			System.out.println(line);

			// split by comma
			StringTokenizer tokenizerLine = new StringTokenizer(line, ",");

			// split each line by the delim
			while (tokenizerLine.hasMoreTokens()) {

				// get the first element. NOTE: after tokenize the first token
				// is ID
				String strID = tokenizerLine.nextToken();

				// for debugging
				System.out.println("City ID: " + strID);

				// parse every item(cityID, name, code, district, population)
				// from each line
				strName = tokenizerLine.nextToken();

				System.out.println("strName = " + strName);

				if (tokenizerLine.hasMoreTokens()) {

					// after Name, it parse the code
					strCode = tokenizerLine.nextToken();
					System.out.println("strCode = " + strCode);

					if (tokenizerLine.hasMoreTokens()) {

						// after country code, it parse the district
						strDistrict = tokenizerLine.nextToken();
						System.out.println("strDistrict = " + strDistrict);

						if (tokenizerLine.hasMoreTokens()) {

							// at last, parse the population
							strPopulation = tokenizerLine.nextToken();
							System.out.println("strPopulation = " + strPopulation);

						}
					}
				}

				Text name = new Text(strName);
				long scoreInt = Integer.parseInt(strPopulation);
				if (scoreInt > 300000) {
					output.collect(name, new IntWritable());
				}
			}

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {

			output.collect(key, new IntWritable());

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(HandleSelection.class);
		conf.setJobName("cityOps");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("city.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("ex1"));

		JobClient.runJob(conf);

	}

}
