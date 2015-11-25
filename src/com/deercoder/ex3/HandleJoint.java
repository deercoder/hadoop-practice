package com.deercoder.ex3;

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
 * city.txt: City (ID, Name, CountryCode, District, population)
 * 
 * countrylanguage.txt: CountryLanguage (CountryCode, Language, IsOfficial,
 * Percentage)
 * 
 * Used the joint key `CountryCode` to generate the city lists.
 * 
 * @author Chang Liu
 *
 */
public class HandleJoint extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// first judge which table it's from(city or language)
			String line = value.toString();
			String[] list = line.split(",");

			if (list.length > 4) { // country table, has more items
				String countryCode = list[0];
				String countryName = list[1];
				context.write(new Text(countryCode), new Text(countryName));
				System.out.println("#DEBUG: " + list[0] + " " + list[1]);
			} else { // country language
				String countryCode = list[0];
				String language = list[1];
				if (language.compareTo("English") == 0) {
					System.out.println("##DEBUG: " + countryCode + " " + language);
					context.write(new Text(countryCode), new Text(""));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator ite = values.iterator();
			int counter = 0;
			String realcountry = "";
			while (ite.hasNext()) {
				String line = ite.next().toString();
				//System.out.println("******:" + line);
				if (!line.isEmpty()) {
					realcountry = line;
				}
				counter++;
			}
			/**
			 * For those has more than 1 counter, it means having 1) country with English 2) country and name
			 * If counter is less than 1, it means only emits the 2) country with name, that is not offically English
			 * 
			 * Note: these country name may be the same, like `Virgin Island` from British and US. but they're
			 * different, we divide them by the key here(as Code), but their name is actually the same.
			 */
			if (counter > 1) {
				context.write(new Text(realcountry), new Text(""));
			}
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(HandleJoint.class);
		job.setJobName("HandleJoint");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("data/country.txt"));
		FileInputFormat.addInputPath(job, new Path("data/countrylanguage.txt"));
		FileOutputFormat.setOutputPath(job, new Path("ex3"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new HandleJoint(), args);
		System.exit(ret);
	}

}