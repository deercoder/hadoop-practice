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

/**
 * This Program is used to build the relationship between different tables We
 * can learn how to use MapReduce to handle them in just one table
 * 
 * 
 * factory:
 * factoryname addressID
 * 
 * address:
 * addressID city
 * 
 * Used the joint key `addressID` to genereate the pair(factoryname, city).
 * 
 * @author Chang Liu
 *
 */
public class MultiTableJoint extends Configured implements Tool {

	private static int time = 0;

	public static class Map extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// first judge which table the item is from, and then split them
			// with tags
			String line = value.toString();
			int i = 0;

			// ignore the first line of the input
			if (line.contains("factoryname") == true || line.contains("addressID") == true) {
				return;
			} else {
				while (line.charAt(i) >= '9' || line.charAt(i) <= '0') {
					i++;
				}

				// left table, because first char is not number
				if (line.charAt(0) >= '9' || line.charAt(0) <= '0') {
					int j = i - 1;
					while (line.charAt(j) != ' ') {
						j--;
					}
					String[] values = { line.substring(0, j), line.substring(i) };
					context.write(new Text(values[1]), new Text("1+" + values[0]));
				}
				// right table, as first char is number
				else {
					int j = i + 1;
					while (line.charAt(j) != ' ') {
						j++;
					}
					String[] values = { line.substring(0, i + 1), line.substring(j) };
					context.write(new Text(values[0]), new Text("2+" + values[1]));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// first parse the output of the map(), then output
			if (time == 0) {
				context.write(new Text("factoryname"), new Text("addressname"));
				time++;
			}

			int factorynum = 0;
			String factory[] = new String[10];
			int addressnum = 0;
			String address[] = new String[10];
			Iterator ite = values.iterator();

			while (ite.hasNext()) {
				String record = ite.next().toString();
				int len = record.length();
				int i = 2;
				char type = record.charAt(0);
				String factoryname = new String();
				String addressname = new String();

				// left table
				if (type == '1') {
					factory[factorynum] = record.substring(2);
					factorynum++;
				} else {
					address[addressnum] = record.substring(2);
					addressnum++;
				}

				if (factorynum != 0 && addressnum != 0) {
					for (int m = 0; m < factorynum; m++) {
						for (int n = 0; n < addressnum; n++) {
							context.write(new Text(factory[m]), new Text(address[n]));
						}
					}

				}
			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(MultiTableJoint.class);
		job.setJobName("MultiTableJoint");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("multitable/*"));
		FileOutputFormat.setOutputPath(job, new Path("multiJoint"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MultiTableJoint(), args);
		System.exit(ret);
	}

}