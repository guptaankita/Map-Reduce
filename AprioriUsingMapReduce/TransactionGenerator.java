package com.trial;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class TransactionGenerator {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		
		
		String[] tokens = null;
		// map function
		@Override
		public void map(Object key, Text line, Context context)
				throws IOException, InterruptedException {
			tokens = line.toString().split("\t");
			Text user = new Text(tokens[0]);
			Text songId = new Text(tokens[1]);
			context.write(user, songId);
		}

	} // end of mapper

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		HashMap<String, Long> map;
		long count;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			map = new HashMap<String, Long>();
			count = 1;
			super.setup(context);
		}
		
		/*
		 * Used to accumulate songs and its counts from each mapper.
		 * */
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			map.put(key.toString(), count);
			count++;
			List<String> trans = new ArrayList<String>();
			Iterator<Text> it = values.iterator();
			while(it.hasNext())
			{
				trans.add(it.next().toString());
			}
			Collections.sort(trans);
			String result = StringUtils.join(trans, ",");
			try {
				context.write(key, new Text(count + "\t" + result));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception {
		run(args);
	}

	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
	
		// create a new Job
		Job job = new Job(conf, "Apriori");
		job.setJarByClass(TransactionGenerator.class);
		
		// set the intermediate key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the mapper and the reducer
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// one reducer only, this MR will create the partition file
		// sets the input format for the job and the output format
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Submit the job to the cluster and wait for it to finish.
		System.exit((job.waitForCompletion(true))?0:1);
	}
}