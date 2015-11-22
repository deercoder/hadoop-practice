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
 * This Program is used to build the relationship within one single table
 * We can learn how to use MapReduce to handle them in just one table
 * @author Chang Liu
 *
 */
public class SingleJoint extends Configured implements Tool {

	public static int time = 0;
	
	// map will divide the input as parent and child, then emit in order for the right table
	// reverse-order emit one time as the left table, note that the output of the value should
	// have the left or right marker to generate the overall output(otherwise it will in wrong relationship)
	
	public static class Map extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String childname  = new String();
			String parentname = new String();
			String relationtype = new String();
			String line = value.toString();
			
			int i = 0;
			while(line.charAt(i) != ' ') {
				i++;
			}
			
			String[]  values = {line.substring(0,i), line.substring(i+1)};
			if (values[0].compareTo("child") != 0) { // not the first tag line
				childname = values[0];
				parentname = values[1];
				
				// right table (parent, child)
				relationtype = "1";		// left  or right table
				context.write(new Text(values[1]), new Text(relationtype + "+" + childname + "+" + parentname));
				
				// left table (child, parent)
				relationtype = "2";
				context.write(new Text(values[0]), new Text(relationtype + "+" + childname + "+" + parentname));
				System.out.println("aabbcc"); //for debugging, it prints!!!
			}
			
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("ooooooooooooooo");
			System.out.println(values.toString());
			if (time == 0) { // output the header "grandchild grandparent"
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			
			int grandchildnum = 0;
			String grandchild[] = new String[10];
			int grandparentnum = 0;
			String grandparent[]  = new String[10];
			Iterator ite = values.iterator();
			
			while (ite.hasNext()) {
				String record = ite.next().toString();
				int len = record.length();
				int i = 2;
				if (len == 0) {
					continue;
				}
				
				char relationtype = record.charAt(0);
				String childname = new String();
				String parentname = new String();
				
				// get value-list's value of the child
				while(record.charAt(i) != '+') {
					childname = childname + record.charAt(i);
					i++;
				}
				i = i + 1;
				
				// get value-list's value of the parent
				while (i < len) {
					parentname = parentname + record.charAt(i);
					i++;
				}
				
				// now, get the type, by it we can know it's grandchild or grandparent
				if (relationtype == '1') {
					grandchild[grandchildnum] = childname;
					grandchildnum++;
					System.out.println("aaaa");
				}
				else {
					grandparent[grandparentnum] = parentname;
					grandparentnum++;
					System.out.println("bbbb");					
				}	
			}
			
			// for grandchild and grandparent array, get dikaEr product
			if (grandparentnum != 0 && grandchildnum != 0) {
				for (int m = 0; m < grandchildnum; m++) {
					for (int n = 0; n < grandparentnum; n++) {
						System.out.println("1: " + grandparent[n]);
						context.write(new Text(grandchild[m]), new Text(grandparent[n]));
					}
				}
			}
			
			
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(SingleJoint.class);
		job.setJobName("SingleJoint");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("child"));
		FileOutputFormat.setOutputPath(job, new Path("singleJoint"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SingleJoint(), args);
		System.exit(ret);
	}

}