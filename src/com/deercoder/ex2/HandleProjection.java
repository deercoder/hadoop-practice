package com.deercoder.ex2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

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
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private String strID = "";
		private String strName = "";
		private String strCode = "";
		private String strDistrict = "";
		private String strPopulation = "";

		/**
		 * The parameters of the map function, is the input parameter of Mapper,
		 * which is index and its content
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

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
				Text district = new Text(strDistrict);

				context.write(name, district);
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

		/**
		 * FOR reduce, connect all the districts from the SAME city
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String whole = "";
			for (Text val : values) {
				whole += val.toString();
			}
			/**
			 * Generate the output per list
			 */
			context.write(key, new Text(whole));
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(HandleProjection.class);
		job.setJobName("Projection");

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
		FileInputFormat.setInputPaths(job, new Path("city.txt*"));
		FileOutputFormat.setOutputPath(job, new Path("ex2_output"));

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