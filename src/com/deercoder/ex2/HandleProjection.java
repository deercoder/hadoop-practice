package com.deercoder.ex2;

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
public class HandleProjection extends Configured implements Tool {

	/**
	 * Mapper has four parameters, they are the inputkey, input value,
	 * outputkey, output value type Here, for Map class, the input is the
	 * index(longwritable), and the whole text content for this list the output,
	 * is the pair of <city, district>, so both of them are Text
	 * 
	 * @author Chang Liu<chang_liu@student.uml.edu>
	 *
	 */
	public static class Map extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// first judge which table it's from(city or language)
			String line = value.toString();
			String[] list = line.split(",");

			// parse the line correctly with exact item size(id, name,
			// countrycode, district, population)
			if (list.length == 5) {
				// get the city
				String city = list[1];
				// get population in numeric form
				String district = list[3];

				// emit the population that larger than 300,000
				context.write(new Text(city), new Text(district));

			} else {
				System.out.println("Error when parsing the city.txt");
			}
		}
	}

	/**
	 * Reducer has four parameters, they are the inputkey, input value,
	 * outputkey, output value type Here, for Reduce class, the input is
	 * mapper's output, which is <Text, Text> type the output, is the pair of
	 * result, which means city and its districts, type is also <Text, Text>
	 * 
	 * @author Chang Liu<chang_liu@student.uml.edu>
	 *
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// combine all the districts that have the same city name
			Iterator ite = values.iterator();
			String total = "";
			while (ite.hasNext()) {
				String dis = ite.next().toString();
				total += dis;
			}
			context.write(new Text(key), new Text(total));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(HandleProjection.class);
		job.setJobName("HandleProjection");

		/**
		 * Set the final output format, here it's city with its district, both
		 * are Text format
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/**
		 * Set map and reduce class for event processing
		 */
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		/**
		 * Set the input of the Hadoop job, here all of them are text files
		 */
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		/**
		 * Set the input and output directory/path of the input files
		 */
		FileInputFormat.setInputPaths(job, new Path("data/city.txt"));
		FileOutputFormat.setOutputPath(job, new Path("ex2"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	/**
	 * Start main function for executing the projection of the file
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new HandleProjection(), args);
		System.exit(ret);
	}

}