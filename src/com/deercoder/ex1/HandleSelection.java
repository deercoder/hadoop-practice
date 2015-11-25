package com.deercoder.ex1;

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
 * This Program is used to parse the city.txt and select the city whose
 * population is larger than 300,000
 * 
 * city.txt: City (ID, Name, CountryCode, District, population)
 * 
 * 
 * Use map() to emit that items and reduce() to merge cities and save result
 * 
 * @author Chang Liu
 *
 */
public class HandleSelection extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// first judge which table it's from(city or language)
			String line = value.toString();
			String[] list = line.split(",");

			// parse the line correctly with exact item size(id, name, countrycode, district, population)
			if (list.length == 5) {
				// get the city
				String city = list[1];
				// get population in numeric form
				long population = Integer.parseInt(list[4]);

				// emit the population that larger than 300,000
				if (population > 300000) {
					context.write(new Text(city), new Text(""));
				}
			} else {
				System.out.println("Error when parsing the city.txt");
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// emit all the items that has unique key, which is just the city name
			context.write(new Text(key), new Text(""));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(HandleSelection.class);
		job.setJobName("HandleSelection");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("data/city.txt"));
		FileOutputFormat.setOutputPath(job, new Path("ex1"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new HandleSelection(), args);
		System.exit(ret);
	}

}